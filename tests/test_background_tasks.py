from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.background import BackgroundTasks

from delivery_service.main import app, preview_cache
from delivery_service.security import create_token


def auth(subject: str) -> dict[str, str]:
    token = create_token(subject, token_type="access", lifetime=timedelta(minutes=5))
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_authorized_request_rebuilds_disposable_preview() -> None:
    customer_id = uuid4()
    headers = {
        "Idempotency-Key": f"background-{uuid4()}",
        **auth(f"customer:{customer_id}"),
    }
    payload = {
        "customer_id": str(customer_id),
        "pickup_address": "Nevsky prospect 1",
        "destination_address": "Liteyny prospect 10",
        "weight_grams": 750,
    }
    preview_cache.clear()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        created = await client.post("/api/v1/orders", json=payload, headers=headers)
        order = created.json()
        accepted = await client.post(
            f"/api/v1/orders/{order['id']}/preview/rebuild", headers=headers
        )

    assert accepted.status_code == 202
    assert accepted.json() == {"status": "accepted"}
    assert preview_cache.version_for(UUID(order["id"])) == order["version"]


@pytest.mark.asyncio
async def test_other_customer_cannot_schedule_preview() -> None:
    owner_id = uuid4()
    owner_headers = {
        "Idempotency-Key": f"background-{uuid4()}",
        **auth(f"customer:{owner_id}"),
    }
    payload = {
        "customer_id": str(owner_id),
        "pickup_address": "Nevsky prospect 1",
        "destination_address": "Liteyny prospect 10",
        "weight_grams": 750,
    }
    preview_cache.clear()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        created = await client.post("/api/v1/orders", json=payload, headers=owner_headers)
        denied = await client.post(
            f"/api/v1/orders/{created.json()['id']}/preview/rebuild",
            headers=auth(f"customer:{uuid4()}"),
        )

    assert denied.status_code == 403
    assert preview_cache.values == {}


@pytest.mark.asyncio
async def test_background_tasks_stop_after_first_failure() -> None:
    observed: list[str] = []
    tasks = BackgroundTasks()

    async def fail_first() -> None:
        observed.append("first")
        raise RuntimeError("preview failed")

    async def should_not_run() -> None:
        observed.append("second")

    tasks.add_task(fail_first)
    tasks.add_task(should_not_run)

    with pytest.raises(RuntimeError, match="preview failed"):
        await tasks()
    assert observed == ["first"]
