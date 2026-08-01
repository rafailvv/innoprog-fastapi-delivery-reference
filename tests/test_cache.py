from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from delivery_service.cache import (
    DeliveryZoneCacheAside,
    DeliveryZoneRead,
    InMemoryTTLCache,
)


class UnavailableCache:
    async def get(self, _key: str) -> str | None:
        raise RedisConnectionError("redis unavailable")

    async def set(self, _key: str, _value: str, *, ttl: timedelta) -> None:
        raise RedisConnectionError("redis unavailable")

    async def delete(self, _key: str) -> None:
        raise RedisConnectionError("redis unavailable")


@pytest.mark.asyncio
async def test_cache_aside_hit_avoids_second_source_read() -> None:
    zone = DeliveryZoneRead(id=uuid4(), name="Центр", version=1)
    reads = 0

    async def load(zone_id):
        nonlocal reads
        reads += 1
        assert zone_id == zone.id
        return zone

    cache = InMemoryTTLCache()
    reader = DeliveryZoneCacheAside(cache, ttl=timedelta(seconds=60))

    assert await reader.get(zone.id, load) == zone
    assert await reader.get(zone.id, load) == zone
    assert reads == 1
    assert await cache.get(reader.key(zone.id)) == zone.model_dump_json()


@pytest.mark.asyncio
async def test_invalid_cache_payload_is_deleted_and_rebuilt() -> None:
    zone = DeliveryZoneRead(id=uuid4(), name="Север", version=3)
    cache = InMemoryTTLCache()
    reader = DeliveryZoneCacheAside(cache)
    await cache.set(reader.key(zone.id), '{"version":"broken"}', ttl=timedelta(minutes=1))

    async def load(_zone_id):
        return zone

    assert await reader.get(zone.id, load) == zone
    assert await cache.get(reader.key(zone.id)) == zone.model_dump_json()


@pytest.mark.asyncio
async def test_redis_outage_changes_latency_not_correctness() -> None:
    zone = DeliveryZoneRead(id=uuid4(), name="Юг", version=2)
    calls = 0

    async def load(_zone_id):
        nonlocal calls
        calls += 1
        return zone

    reader = DeliveryZoneCacheAside(UnavailableCache())
    assert await reader.get(zone.id, load) == zone
    assert calls == 1
    await reader.invalidate(zone.id)  # A failed best-effort DEL does not fail the use case.


@pytest.mark.asyncio
async def test_post_commit_invalidation_forces_reload_of_new_version() -> None:
    zone_id = uuid4()
    source = DeliveryZoneRead(id=zone_id, name="Старая зона", version=1)
    calls = 0

    async def load(_zone_id):
        nonlocal calls
        calls += 1
        return source

    reader = DeliveryZoneCacheAside(InMemoryTTLCache())
    assert (await reader.get(zone_id, load)).version == 1

    # This assignment models a committed PostgreSQL transaction.  Only after
    # that durable change do the HTTP path and the zone.changed consumer DEL.
    source = DeliveryZoneRead(id=zone_id, name="Новая зона", version=2)
    await reader.invalidate(zone_id)

    refreshed = await reader.get(zone_id, load)
    assert refreshed.version == 2
    assert calls == 2


@pytest.mark.asyncio
async def test_non_positive_cache_ttl_is_rejected() -> None:
    with pytest.raises(ValueError, match="TTL must be positive"):
        DeliveryZoneCacheAside(InMemoryTTLCache(), ttl=timedelta(0))
