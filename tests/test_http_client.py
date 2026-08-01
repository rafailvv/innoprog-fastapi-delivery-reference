from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI

from delivery_service.config import Settings
from delivery_service.http_client import build_route_http_client
from delivery_service.main import lifespan


@pytest.mark.asyncio
async def test_route_client_applies_all_timeout_budgets_and_reuses_instance() -> None:
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"minutes": 18})

    settings = Settings(
        route_service_url="https://routes.test",
        route_connect_timeout_seconds=1.1,
        route_pool_timeout_seconds=0.6,
        route_timeout_seconds=2.2,
    )
    client = build_route_http_client(
        settings,
        transport=httpx.MockTransport(handler),
    )
    identity = id(client)
    try:
        first = await client.get("/routes/estimate")
        second = await client.get("/routes/estimate")
        assert first.json() == second.json() == {"minutes": 18}
        assert id(client) == identity
        assert len(seen) == 2
        assert seen[0].extensions["timeout"] == {
            "connect": 1.1,
            "read": 2.2,
            "write": 2.2,
            "pool": 0.6,
        }
    finally:
        await client.aclose()
    assert client.is_closed
    assert seen[0].headers["authorization"] == "Bearer local-route-key-change-me"
    assert seen[0].headers["accept"] == "application/json"


@pytest.mark.asyncio
async def test_application_lifespan_owns_and_closes_route_client() -> None:
    app = FastAPI()
    async with lifespan(app):
        client = app.state.http_client
        assert not client.is_closed
        assert app.state.http_client is client
    assert client.is_closed
