from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from opentelemetry.trace import SpanKind

from delivery_service.observability import traced_operation


RELEASE_IF_OWNER = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class Cache(Protocol):
    async def get(self, key: str) -> str | None: ...
    async def set(self, key: str, value: str, *, ttl: timedelta) -> None: ...
    async def delete(self, key: str) -> None: ...


@dataclass(slots=True)
class CacheEntry:
    value: str
    expires_at: datetime


class InMemoryTTLCache:
    def __init__(self) -> None:
        self._entries: dict[str, CacheEntry] = {}

    async def get(self, key: str) -> str | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at <= datetime.now(UTC):
            self._entries.pop(key, None)
            return None
        return entry.value

    async def set(self, key: str, value: str, *, ttl: timedelta) -> None:
        if ttl <= timedelta(0):
            raise ValueError("cache TTL must be positive")
        self._entries[key] = CacheEntry(value, datetime.now(UTC) + ttl)

    async def delete(self, key: str) -> None:
        self._entries.pop(key, None)


class RedisCache:
    def __init__(self, client: Redis) -> None:
        self.client = client

    async def get(self, key: str) -> str | None:
        with traced_operation(
            "redis GET", kind=SpanKind.CLIENT,
            attributes={"db.system": "redis", "cache.operation": "GET"},
        ):
            value = await self.client.get(key)
        return value.decode() if isinstance(value, bytes) else value

    async def set(self, key: str, value: str, *, ttl: timedelta) -> None:
        with traced_operation(
            "redis SET", kind=SpanKind.CLIENT,
            attributes={"db.system": "redis", "cache.operation": "SET"},
        ):
            await self.client.set(key, value, ex=max(1, int(ttl.total_seconds())))

    async def delete(self, key: str) -> None:
        with traced_operation(
            "redis DEL", kind=SpanKind.CLIENT,
            attributes={"db.system": "redis", "cache.operation": "DEL"},
        ):
            await self.client.delete(key)


def build_redis_client(
    url: str,
    *,
    connect_timeout_seconds: float,
    socket_timeout_seconds: float,
    max_connections: int,
) -> Redis:
    """Create one bounded asyncio pool for the whole application process."""
    return Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=connect_timeout_seconds,
        socket_timeout=socket_timeout_seconds,
        max_connections=max_connections,
        health_check_interval=30,
    )


class DeliveryZoneRead(BaseModel):
    """Public, versioned snapshot that is safe to rebuild from PostgreSQL."""

    model_config = ConfigDict(extra="forbid")

    id: UUID
    name: str = Field(min_length=1, max_length=120)
    version: int = Field(ge=1)


class DeliveryZoneCacheAside:
    """Cache-aside query whose correctness never depends on Redis.

    The loader represents the PostgreSQL source of truth.  Redis errors and
    invalid payloads become cache misses; loader errors still propagate.
    """

    def __init__(
        self,
        cache: Cache,
        *,
        ttl: timedelta = timedelta(seconds=60),
    ) -> None:
        if ttl <= timedelta(0):
            raise ValueError("cache TTL must be positive")
        self.cache = cache
        self.ttl = ttl

    @staticmethod
    def key(zone_id: UUID) -> str:
        return f"delivery:cache:v1:zone:{zone_id}"

    async def get(
        self,
        zone_id: UUID,
        load_from_postgres: Callable[[UUID], Awaitable[DeliveryZoneRead]],
    ) -> DeliveryZoneRead:
        key = self.key(zone_id)
        try:
            cached = await self.cache.get(key)
        except RedisError:
            cached = None

        if cached is not None:
            try:
                return DeliveryZoneRead.model_validate_json(cached)
            except ValidationError:
                await self._delete_best_effort(key)

        zone = await load_from_postgres(zone_id)
        try:
            await self.cache.set(
                key,
                zone.model_dump_json(),
                ttl=self.ttl,
            )
        except RedisError:
            # Cache availability affects latency, not the query result.
            pass
        return zone

    async def invalidate(self, zone_id: UUID) -> None:
        """Call only after the source-of-truth transaction committed.

        The zone.changed outbox consumer repeats this idempotent invalidation
        if the process dies after commit and before this best-effort DEL.
        """
        await self._delete_best_effort(self.key(zone_id))

    async def _delete_best_effort(self, key: str) -> None:
        try:
            await self.cache.delete(key)
        except RedisError:
            pass


@dataclass(slots=True)
class RedisLease:
    """Short best-effort lease for rebuildable work such as cache warming.

    A lease is deliberately not a business lock: its TTL can expire while a
    paused worker still runs.  Durable effects must remain protected by a
    PostgreSQL constraint/conditional write or an idempotent consumer.
    """

    client: Redis
    key: str
    token: str

    @classmethod
    async def acquire(
        cls,
        client: Redis,
        key: str,
        *,
        ttl_ms: int,
    ) -> RedisLease | None:
        if ttl_ms <= 0:
            raise ValueError("lease TTL must be positive")
        token = secrets.token_urlsafe(24)
        acquired = await client.set(key, token, nx=True, px=ttl_ms)
        return cls(client=client, key=key, token=token) if acquired else None

    async def release(self) -> bool:
        deleted = await self.client.eval(RELEASE_IF_OWNER, 1, self.key, self.token)
        return bool(deleted)
