from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from delivery_service.main import app, repository
from delivery_service.security import create_token
from delivery_service.service import OrderService
from delivery_service.tracking import (
    InMemoryTrackingSnapshotRepository,
    TrackingEvent,
    TrackingHub,
    order_event_stream,
)


def token(subject: str) -> str:
    return create_token(subject, token_type="access", lifetime=timedelta(minutes=5))


def create_order(client: TestClient) -> tuple[dict[str, object], str, dict[str, str]]:
    customer_id = uuid4()
    access_token = token(f"customer:{customer_id}")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Idempotency-Key": f"tracking-{uuid4()}",
    }
    response = client.post(
        "/api/v1/orders",
        headers=headers,
        json={
            "customer_id": str(customer_id),
            "pickup_address": "Nevsky prospect 1",
            "destination_address": "Liteyny prospect 10",
            "weight_grams": 750,
        },
    )
    assert response.status_code == 201
    return response.json(), access_token, headers


def test_websocket_owner_receives_snapshot_and_disconnect_cleans_subscription() -> None:
    with TestClient(app) as client:
        order, access_token, _headers = create_order(client)
        order_id = UUID(str(order["id"]))
        hub = app.state.tracking
        assert hub.subscriber_count(order_id) == 0

        with client.websocket_connect(
            f"/api/v1/orders/{order_id}/tracking",
            subprotocols=[f"bearer.{access_token}"],
        ) as websocket:
            snapshot = websocket.receive_json()
            assert snapshot == {
                "order_id": str(order_id),
                "status": "created",
                "sequence": 0,
                "latitude": None,
                "longitude": None,
                "recorded_at": None,
            }
            assert hub.subscriber_count(order_id) == 1

        assert hub.subscriber_count(order_id) == 0


def test_websocket_rejects_foreign_customer_before_subscription() -> None:
    with TestClient(app) as client:
        order, _access_token, _headers = create_order(client)
        order_id = UUID(str(order["id"]))
        foreign = token(f"customer:{uuid4()}")

        with pytest.raises(WebSocketDisconnect) as rejected:
            with client.websocket_connect(
                f"/api/v1/orders/{order_id}/tracking",
                subprotocols=[f"bearer.{foreign}"],
            ):
                pass

        assert rejected.value.code == 4403
        assert app.state.tracking.subscriber_count(order_id) == 0


def test_sse_snapshot_has_framing_and_security_headers() -> None:
    with TestClient(app) as client:
        order, _access_token, headers = create_order(client)
        response = client.get(
            f"/api/v1/orders/{order['id']}/tracking/events?follow=false",
            headers={**headers, "Last-Event-ID": "0"},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"
    assert "event: snapshot\n" in response.text
    assert "id: 0\n" in response.text
    assert response.text.endswith("\n\n")


@pytest.mark.asyncio
async def test_sse_stream_skips_seen_sequence_and_unsubscribes_on_close() -> None:
    hub = TrackingHub(queue_size=2)
    order_id = uuid4()
    stream = order_event_stream(
        hub=hub,
        snapshot=TrackingEvent(order_id, "created", 1),
        last_sequence=2,
        follow=True,
    )
    snapshot = await anext(stream)
    assert b"event: snapshot" in snapshot
    assert hub.subscriber_count(order_id) == 1

    next_chunk = asyncio.create_task(anext(stream))
    await hub.publish(TrackingEvent(order_id, "assigned", 2))
    await hub.publish(TrackingEvent(order_id, "picked_up", 3))
    update = await asyncio.wait_for(next_chunk, timeout=1)
    assert b"id: 3\n" in update
    assert b'"status":"picked_up"' in update

    await stream.aclose()
    assert hub.subscriber_count(order_id) == 0


@pytest.mark.asyncio
async def test_tracking_snapshot_is_idempotent_and_monotonic() -> None:
    snapshots = InMemoryTrackingSnapshotRepository()
    order_id = uuid4()
    tenant_id = uuid4()
    first_event = uuid4()
    first, changed = await snapshots.record(
        order_id=order_id,
        tenant_id=tenant_id,
        status="assigned",
        latitude=59.9343,
        longitude=30.3351,
        client_event_id=first_event,
        recorded_at=datetime.now(UTC),
    )
    replay, replay_changed = await snapshots.record(
        order_id=order_id,
        tenant_id=tenant_id,
        status="assigned",
        latitude=0,
        longitude=0,
        client_event_id=first_event,
        recorded_at=datetime.now(UTC),
    )
    second, second_changed = await snapshots.record(
        order_id=order_id,
        tenant_id=tenant_id,
        status="assigned",
        latitude=59.94,
        longitude=30.34,
        client_event_id=uuid4(),
        recorded_at=datetime.now(UTC),
    )

    assert changed and second_changed and not replay_changed
    assert replay == first and first.sequence == 1 and second.sequence == 2
    assert len(snapshots.outbox) == 2


def test_assigned_courier_records_durable_position_and_foreign_actor_is_denied() -> None:
    with TestClient(app) as client:
        order, _customer_token, _headers = create_order(client)
        courier_id = uuid4()
        courier_token = token(f"courier:{courier_id}")
        courier_headers = {"Authorization": f"Bearer {courier_token}"}
        assigned = asyncio.run(OrderService(repository).assign(UUID(str(order["id"])), courier_id))
        assert assigned.courier_id == courier_id
        event_id = uuid4()
        payload = {
            "client_event_id": str(event_id),
            "latitude": 59.9343,
            "longitude": 30.3351,
        }
        first = client.post(
            f"/api/v1/courier/orders/{order['id']}/tracking",
            headers=courier_headers,
            json=payload,
        )
        replay = client.post(
            f"/api/v1/courier/orders/{order['id']}/tracking",
            headers=courier_headers,
            json=payload,
        )
        foreign = client.post(
            f"/api/v1/courier/orders/{order['id']}/tracking",
            headers={"Authorization": f"Bearer {token(f'courier:{uuid4()}')}"},
            json={**payload, "client_event_id": str(uuid4())},
        )

    assert first.status_code == replay.status_code == 201
    assert first.json()["sequence"] == replay.json()["sequence"] == 1
    assert foreign.status_code == 403
