from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
import pytest

from delivery_service.main import app, get_order_repository
from delivery_service.security import create_token
from tests.asgi_client import asgi_client
from tests.fakes import FakeOrderRepository
from tests.override_tools import dependency_override


def _auth(customer_id: str) -> dict[str, str]:
    token = create_token(
        f"customer:{customer_id}",
        token_type="access",
        lifetime=timedelta(minutes=5),
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_asgi_transport_does_not_start_lifespan_automatically() -> None:
    app.state.ready = False
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        live = await client.get("/health/live")
        ready = await client.get("/health/ready")

    assert live.status_code == 200
    assert live.json() == {"status": "ok"}
    assert ready.status_code == 503
    assert ready.json()["error_code"] == "service_unavailable"


@pytest.mark.asyncio
async def test_async_client_runs_lifespan_and_observes_cleanup() -> None:
    async with asgi_client(app) as client:
        response = await client.get("/health/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}
        assert app.state.ready is True
        assert app.state.http_client.is_closed is False

    assert app.state.ready is False
    assert app.state.http_client.is_closed is True


@pytest.mark.asyncio
async def test_async_api_happy_path_validation_and_side_effects() -> None:
    customer_id = str(uuid4())
    request_id = f"async-api-{uuid4()}"
    headers = {
        **_auth(customer_id),
        "Idempotency-Key": f"async-api-{uuid4()}",
        "X-Request-ID": request_id,
    }
    payload = {
        "customer_id": customer_id,
        "pickup_address": "Nevsky prospect 1",
        "destination_address": "Liteyny prospect 10",
        "weight_grams": 750,
    }
    fake = FakeOrderRepository()

    with dependency_override(app, get_order_repository, lambda: fake):
        async with asgi_client(app) as client:
            created = await client.post(
                "/api/v1/orders", json=payload, headers=headers,
            )
            rejected = await client.post(
                "/api/v1/orders",
                json={**payload, "weight_grams": 0},
                headers={
                    **headers,
                    "Idempotency-Key": f"async-invalid-{uuid4()}",
                },
            )

    assert created.status_code == 201
    assert created.headers["location"].endswith(created.json()["id"])
    assert created.headers["x-request-id"] == request_id
    assert created.json()["status"] == "created"
    assert len(fake.added) == 1
    assert fake.created_events == [fake.added[0].id]

    assert rejected.status_code == 422
    assert rejected.json()["error_code"] == "request_invalid"
    assert rejected.headers["x-request-id"] == request_id
    assert len(fake.added) == 1
    assert get_order_repository not in app.dependency_overrides


@pytest.mark.asyncio
async def test_raise_app_exceptions_selects_debug_or_http_contract() -> None:
    broken = FastAPI()

    @broken.get("/boom")
    async def boom() -> None:
        raise RuntimeError("private diagnostic")

    async with AsyncClient(
        transport=ASGITransport(app=broken, raise_app_exceptions=True),
        base_url="http://testserver",
    ) as debugging_client:
        with pytest.raises(RuntimeError, match="private diagnostic"):
            await debugging_client.get("/boom")

    async with AsyncClient(
        transport=ASGITransport(app=broken, raise_app_exceptions=False),
        base_url="http://testserver",
    ) as contract_client:
        response = await contract_client.get("/boom")

    assert response.status_code == 500
    assert "private diagnostic" not in response.text
