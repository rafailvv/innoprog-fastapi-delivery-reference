from __future__ import annotations

import re
from uuid import uuid4

from httpx import ASGITransport, AsyncClient
import pytest

from delivery_service.main import app
from delivery_service.observability import (
    IN_FLIGHT,
    LATENCY_P95_SECONDS,
    REQUEST_DURATION_BUCKETS,
    SERVICE_LEVEL_OBJECTIVE,
    metric_method,
)
from delivery_service.security import create_token
from datetime import timedelta


def _auth(customer_id: str) -> dict[str, str]:
    token = create_token(
        f"customer:{customer_id}", token_type="access", lifetime=timedelta(minutes=5)
    )
    return {"Authorization": f"Bearer {token}"}


def test_unexpected_http_method_is_collapsed_to_a_bounded_label() -> None:
    assert metric_method("GET") == "GET"
    assert metric_method("PROPFIND") == "OTHER"
    assert metric_method("attacker-method-123") == "OTHER"


@pytest.mark.asyncio
async def test_red_metrics_use_route_templates_and_bounded_labels() -> None:
    customer_id = str(uuid4())
    headers = {
        **_auth(customer_id),
        "Idempotency-Key": f"metrics-{uuid4()}",
    }
    payload = {
        "customer_id": customer_id,
        "pickup_address": "Kazan, Baumana 1",
        "destination_address": "Kazan, Kremlevskaya 18",
        "weight_grams": 750,
    }
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        created = await client.post("/api/v1/orders", json=payload, headers=headers)
        order_id = created.json()["id"]
        fetched = await client.get(f"/api/v1/orders/{order_id}", headers=headers)
        metrics = await client.get("/metrics")

    assert created.status_code == 201 and fetched.status_code == 200
    assert 'route="/api/v1/orders/{order_id}"' in metrics.text
    assert order_id not in metrics.text
    assert customer_id not in metrics.text
    assert 'operation="create_order",outcome="success"' in metrics.text
    assert 'operation="get_order",outcome="success"' in metrics.text


@pytest.mark.asyncio
async def test_histogram_contains_the_exact_latency_slo_boundary() -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        await client.get("/health/live")
        metrics = await client.get("/metrics")

    assert SERVICE_LEVEL_OBJECTIVE == 0.995
    assert LATENCY_P95_SECONDS == 0.5
    assert LATENCY_P95_SECONDS in REQUEST_DURATION_BUCKETS
    assert re.search(
        r'delivery_http_request_seconds_bucket\{[^}]*le="0\.5"[^}]*\}', metrics.text
    )


@pytest.mark.asyncio
async def test_client_errors_are_visible_but_not_misclassified_as_server_errors() -> None:
    customer_id = str(uuid4())
    headers = {
        **_auth(customer_id),
        "Idempotency-Key": f"invalid-metrics-{uuid4()}",
    }
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        rejected = await client.post(
            "/api/v1/orders",
            json={
                "customer_id": customer_id,
                "pickup_address": "Kazan, Baumana 1",
                "destination_address": "Kazan, Kremlevskaya 18",
                "weight_grams": 0,
            },
            headers=headers,
        )
        metrics = await client.get("/metrics")

    assert rejected.status_code == 422
    assert 'operation="create_order",outcome="client_error"' in metrics.text


@pytest.mark.asyncio
async def test_in_flight_gauge_is_released_when_application_raises() -> None:
    async def failing_app(_scope, _receive, _send) -> None:
        raise RuntimeError("planned failure")

    from delivery_service.observability import TelemetryMiddleware

    gauge = IN_FLIGHT.labels("GET")
    before = gauge._value.get()
    async with AsyncClient(
        transport=ASGITransport(app=TelemetryMiddleware(failing_app)),
        base_url="http://testserver",
    ) as client:
        with pytest.raises(RuntimeError, match="planned"):
            await client.get("/failure")
    assert gauge._value.get() == before
