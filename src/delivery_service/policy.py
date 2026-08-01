from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import ipaddress
import math
from uuid import UUID, uuid4


@dataclass(frozen=True, slots=True)
class AuditEvent:
    event_id: UUID
    actor_id: str | None
    subject_fingerprint: str
    action: str
    resource_id: str | None
    outcome: str
    reason_code: str
    client_network_key: str
    correlation_id: str
    occurred_at: datetime


class AuditTrail:
    """Small append-only reference adapter.

    Production deployments write the same safe event contract to a durable
    store with append-only permissions and retention controls.  The tuple
    exposed here prevents callers from mutating already recorded events; it
    does not pretend that process memory is durable storage.
    """

    def __init__(self) -> None:
        self._events: list[AuditEvent] = []

    @property
    def events(self) -> tuple[AuditEvent, ...]:
        return tuple(self._events)

    def clear(self) -> None:
        self._events.clear()

    async def record(
        self,
        *,
        actor_id: str | None,
        subject_fingerprint: str,
        action: str,
        resource_id: str | None,
        outcome: str,
        reason_code: str,
        client_network_key: str,
        correlation_id: str,
    ) -> None:
        self._events.append(AuditEvent(
            event_id=uuid4(),
            actor_id=actor_id,
            subject_fingerprint=subject_fingerprint,
            action=action,
            resource_id=resource_id,
            outcome=outcome,
            reason_code=reason_code,
            client_network_key=client_network_key,
            correlation_id=correlation_id,
            occurred_at=datetime.now(UTC),
        ))


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: int = 0


class SlidingWindowRateLimiter:
    def __init__(self, *, limit: int, window: timedelta) -> None:
        self.limit = limit
        self.window = window
        self._requests: dict[str, deque[datetime]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def check_many(
        self, keys: tuple[str, ...], *, now: datetime | None = None,
    ) -> RateLimitDecision:
        """Atomically consume one slot from every independent budget."""
        if not keys or len(set(keys)) != len(keys):
            raise ValueError("rate-limit keys must be non-empty and unique")
        current = now or datetime.now(UTC)
        threshold = current - self.window
        async with self._lock:
            buckets = [self._requests[key] for key in keys]
            for requests in buckets:
                while requests and requests[0] <= threshold:
                    requests.popleft()

            blocked = [requests for requests in buckets if len(requests) >= self.limit]
            if blocked:
                retry_after = max(
                    math.ceil((requests[0] + self.window - current).total_seconds())
                    for requests in blocked
                )
                return RateLimitDecision(False, max(1, retry_after))

            for requests in buckets:
                requests.append(current)
            return RateLimitDecision(True)

    async def check(
        self, key: str, *, now: datetime | None = None,
    ) -> RateLimitDecision:
        return await self.check_many((key,), now=now)

    async def allow(self, key: str, *, now: datetime | None = None) -> bool:
        """Compatibility wrapper retained for earlier course milestones."""
        return (await self.check(key, now=now)).allowed

    def clear(self) -> None:
        self._requests.clear()


def normalize_login_subject(raw: str) -> str:
    """Bound cheap untrusted input before keys, logs or Argon2 work."""
    normalized = raw.strip().casefold()
    if not 3 <= len(normalized) <= 160:
        raise ValueError("invalid login identifier")
    if any(ord(character) < 32 or ord(character) == 127 for character in normalized):
        raise ValueError("invalid login identifier")
    return normalized


def subject_fingerprint(subject: str) -> str:
    """Bound the storage key; this digest is not claimed to hide the subject."""
    return hashlib.sha256(subject.encode("utf-8")).hexdigest()


def login_limit_keys(*, client_network_key: str, subject: str) -> tuple[str, str]:
    return (
        f"login:network:{client_network_key}",
        f"login:subject:{subject_fingerprint(subject)}",
    )


def trusted_client_network_key(host: str | None) -> str:
    """Canonicalize the address already restored by the trusted proxy chain."""
    if not host:
        return "unknown"
    try:
        return ipaddress.ip_address(host).compressed
    except ValueError:
        # TestClient and local transports use symbolic peer names.  Never fall
        # back to a client-controlled forwarding header here.
        return "unknown"


audit_trail = AuditTrail()
login_rate_limiter = SlidingWindowRateLimiter(limit=5, window=timedelta(minutes=1))
