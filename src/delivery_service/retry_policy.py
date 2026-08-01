"""Retry and circuit-breaker policies for the idempotent route lookup."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from time import monotonic
from typing import TypeVar

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)


T = TypeVar("T")


class RetryableRouteStatus(httpx.HTTPStatusError):
    """A temporary provider response for which an idempotent GET may be repeated."""


RETRYABLE_ROUTE_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    RetryableRouteStatus,
)


def build_route_retrying(
    *,
    attempts: int = 3,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> AsyncRetrying:
    """Create a fresh bounded controller for one logical route lookup."""

    if attempts < 1:
        raise ValueError("attempts must be positive")
    common = {
        "retry": retry_if_exception_type(RETRYABLE_ROUTE_ERRORS),
        "stop": stop_after_attempt(attempts),
        # Full jitter: Tenacity chooses a random delay in the exponentially
        # growing interval instead of synchronising every caller.
        "wait": wait_random_exponential(multiplier=0.1, max=1.0),
    }
    if sleep is None:
        return AsyncRetrying(**common, reraise=True)
    return AsyncRetrying(**common, sleep=sleep, reraise=True)


async def run_with_route_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    retry_factory: Callable[[], AsyncRetrying] = build_route_retrying,
) -> T:
    """Run one idempotent operation without swallowing cancellation."""

    async for attempt in retry_factory():
        with attempt:
            return await operation()
    raise AssertionError("retry controller ended without result")


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """The dependency is known to be unhealthy, so no request was sent."""


@dataclass(frozen=True, slots=True)
class CircuitPermit:
    half_open_probe: bool = False


class CircuitBreaker:
    """Concurrency-safe CLOSED/OPEN/HALF_OPEN state machine for one process."""

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be positive")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be positive")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._clock = clock
        self._lock = asyncio.Lock()
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._probe_in_flight = False

    @property
    def state(self) -> CircuitState:
        return self._state

    async def acquire(self) -> CircuitPermit:
        """Allow a normal call or reserve the single half-open probe."""

        async with self._lock:
            if self._state is CircuitState.CLOSED:
                return CircuitPermit()

            assert self._opened_at is not None
            if self._clock() - self._opened_at < self.recovery_timeout:
                raise CircuitOpenError("route service circuit is open")

            self._state = CircuitState.HALF_OPEN
            if self._probe_in_flight:
                raise CircuitOpenError("route service recovery probe is already running")
            self._probe_in_flight = True
            return CircuitPermit(half_open_probe=True)

    async def record_success(self, permit: CircuitPermit) -> None:
        async with self._lock:
            self._state = CircuitState.CLOSED
            self._consecutive_failures = 0
            self._opened_at = None
            if permit.half_open_probe:
                self._probe_in_flight = False

    async def record_failure(self, permit: CircuitPermit) -> None:
        """Record one failed logical operation, not every retry attempt."""

        async with self._lock:
            if permit.half_open_probe:
                self._probe_in_flight = False
                self._open_now()
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.failure_threshold:
                self._open_now()

    async def abandon(self, permit: CircuitPermit) -> None:
        """Release a cancelled half-open probe without leaving the circuit stuck."""

        async with self._lock:
            if permit.half_open_probe:
                self._probe_in_flight = False
                self._open_now()

    def _open_now(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()
