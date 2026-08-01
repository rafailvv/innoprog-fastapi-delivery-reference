from __future__ import annotations

import hashlib
import hmac
import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

import httpx
from opentelemetry.trace import SpanKind
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, ValidationError
from tenacity import AsyncRetrying

from delivery_service.retry_policy import (
    CircuitBreaker,
    CircuitOpenError,
    RetryableRouteStatus,
    build_route_retrying,
    run_with_route_retry,
)
from delivery_service.observability import traced_operation


class RouteServiceUnavailable(RuntimeError):
    pass


class InvalidRouteResponse(RuntimeError):
    pass


class RouteRequestRejected(RuntimeError):
    """The provider understood the request but cannot build this route."""


@dataclass(frozen=True, slots=True)
class RouteQuery:
    origin: str
    destination: str

    def __post_init__(self) -> None:
        if not self.origin.strip() or not self.destination.strip():
            raise ValueError("origin and destination must not be blank")


@dataclass(frozen=True, slots=True)
class RouteEstimate:
    distance_m: int
    duration_s: int
    provider_ref: str

    @property
    def minutes(self) -> int:
        """Rounded-up duration kept as a convenient application-level view."""

        return (self.duration_s + 59) // 60


class RouteEstimator(Protocol):
    async def estimate(self, query: RouteQuery) -> RouteEstimate: ...


class ProviderRoutePayload(BaseModel):
    """Untrusted provider DTO; it never leaves the HTTP adapter."""

    model_config = ConfigDict(extra="forbid", strict=True)

    route_id: StrictStr = Field(min_length=1, max_length=128)
    distance_meters: StrictInt = Field(gt=0, le=2_000_000)
    duration_seconds: StrictInt = Field(gt=0, le=172_800)


class RouteServiceAdapter:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        retry_factory: Callable[[], AsyncRetrying] = build_route_retrying,
        total_timeout_seconds: float = 3.0,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self._client = client
        self._retry_factory = retry_factory
        self._total_timeout_seconds = total_timeout_seconds
        self._circuit_breaker = circuit_breaker or CircuitBreaker()

    async def estimate(self, query: RouteQuery) -> RouteEstimate:
        try:
            permit = await self._circuit_breaker.acquire()
        except CircuitOpenError as exc:
            raise RouteServiceUnavailable("route service is temporarily disabled") from exc

        async def one_attempt() -> RouteEstimate:
            return await self._estimate_once(query)

        try:
            async with asyncio.timeout(self._total_timeout_seconds):
                result = await run_with_route_retry(
                    one_attempt,
                    retry_factory=self._retry_factory,
                )
        except asyncio.CancelledError:
            await self._circuit_breaker.abandon(permit)
            raise
        except TimeoutError as exc:
            await self._circuit_breaker.record_failure(permit)
            raise RouteServiceUnavailable("route lookup deadline exceeded") from exc
        except httpx.TimeoutException as exc:
            await self._circuit_breaker.record_failure(permit)
            raise RouteServiceUnavailable("route service timed out") from exc
        except httpx.RequestError as exc:
            await self._circuit_breaker.record_failure(permit)
            raise RouteServiceUnavailable("route service is unreachable") from exc
        except InvalidRouteResponse:
            await self._circuit_breaker.record_failure(permit)
            raise
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status in {400, 404, 422}:
                # The provider is healthy and gave a deterministic answer.
                await self._circuit_breaker.record_success(permit)
                raise RouteRequestRejected("route cannot be calculated") from exc
            await self._circuit_breaker.record_failure(permit)
            raise RouteServiceUnavailable("route service returned an error") from exc
        else:
            await self._circuit_breaker.record_success(permit)
            return result

    async def _estimate_once(self, query: RouteQuery) -> RouteEstimate:
        with traced_operation(
            "route-service GET /routes/estimate",
            kind=SpanKind.CLIENT,
            attributes={
                "delivery.operation": "route.estimate",
                "http.request.method": "GET",
                "server.address": "route-service",
            },
        ):
            response = await self._client.get(
                "/routes/estimate",
                params={"origin": query.origin, "destination": query.destination},
            )
        if response.status_code in {429, 502, 503, 504}:
            raise RetryableRouteStatus(
                "route provider returned a temporary status",
                request=response.request,
                response=response,
            )
        response.raise_for_status()
        try:
            remote = ProviderRoutePayload.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise InvalidRouteResponse("route service returned an invalid payload") from exc
        return RouteEstimate(
            distance_m=remote.distance_meters,
            duration_s=remote.duration_seconds,
            provider_ref=remote.route_id,
        )


def verify_webhook_signature(
    payload: bytes, signature: str, secret: bytes, *, timestamp: str
) -> bool:
    """Verify HMAC-SHA256 over the exact timestamp text and raw body bytes."""

    if len(signature) != 64:
        return False
    try:
        supplied = bytes.fromhex(signature)
        timestamp_bytes = timestamp.encode("ascii")
    except (ValueError, UnicodeEncodeError):
        return False
    signed_payload = b"delivery.payment.v1\n" + timestamp_bytes + b"\n" + payload
    expected = hmac.digest(secret, signed_payload, hashlib.sha256)
    return hmac.compare_digest(expected, supplied)
