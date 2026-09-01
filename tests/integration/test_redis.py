from __future__ import annotations

import asyncio
import os
from datetime import timedelta

import pytest
from redis.asyncio import Redis

from delivery_service.cache import RedisCache, RedisLease


@pytest.mark.asyncio
async def test_real_redis_cache_value_expires() -> None:
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TEST_REDIS_URL is required for the Redis integration test")

    client = Redis.from_url(redis_url, socket_connect_timeout=1, socket_timeout=1)
    cache = RedisCache(client)
    key = "delivery:test:cache:ttl"
    try:
        await client.delete(key)
        await cache.set(key, '{"zone":"center"}', ttl=timedelta(seconds=1))
        assert await cache.get(key) == '{"zone":"center"}'
        assert 0 < await client.pttl(key) <= 1_000
        await asyncio.sleep(1.1)
        assert await cache.get(key) is None
    finally:
        await client.delete(key)
        await client.aclose()


@pytest.mark.asyncio
async def test_real_redis_ttl_and_owner_token_release() -> None:
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TEST_REDIS_URL is required for the Redis integration test")

    client = Redis.from_url(redis_url, socket_connect_timeout=1, socket_timeout=1)
    key = "delivery:test:lease:owner-change"
    try:
        await client.delete(key)
        old = await RedisLease.acquire(client, key, ttl_ms=80)
        assert old is not None
        assert await client.pttl(key) > 0

        await asyncio.sleep(0.12)
        new = await RedisLease.acquire(client, key, ttl_ms=1_000)
        assert new is not None

        assert await old.release() is False
        assert await client.get(key) == new.token.encode()
        assert await new.release() is True
        assert await client.exists(key) == 0
    finally:
        await client.delete(key)
        await client.aclose()
