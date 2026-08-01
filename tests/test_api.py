from httpx import ASGITransport, AsyncClient
import hashlib
import hmac
import pytest
import time
from uuid import uuid4

from delivery_service.main import app
from delivery_service.security import create_token
from datetime import timedelta


def auth(subject: str) -> dict[str, str]:
    token = create_token(subject, token_type="access", lifetime=timedelta(minutes=5))
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_liveness() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_traceparent_is_propagated_with_a_new_span() -> None:
    trace_id = "0123456789abcdef0123456789abcdef"
    parent = f"00-{trace_id}-0123456789abcdef-01"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/health/live", headers={"traceparent": parent})
    returned = response.headers["traceparent"]
    assert returned.startswith(f"00-{trace_id}-") and returned != parent


@pytest.mark.asyncio
async def test_create_order_is_idempotent() -> None:
    payload = {
        "customer_id": "11111111-1111-4111-8111-111111111111",
        "pickup_address": "Nevsky prospect 1",
        "destination_address": "Liteyny prospect 10",
        "weight_grams": 750,
    }
    headers = {"Idempotency-Key": "api-test-idempotency", **auth(f"customer:{payload['customer_id']}")}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        first = await client.post("/api/v1/orders", json=payload, headers=headers)
        second = await client.post("/api/v1/orders", json=payload, headers=headers)
    assert first.status_code == second.status_code == 201
    assert first.json()["id"] == second.json()["id"]


@pytest.mark.asyncio
async def test_create_returns_location_and_get_is_safe() -> None:
    customer_id = str(uuid4())
    payload = {
        "customer_id": customer_id,
        "pickup_address": "Nevsky prospect 1",
        "destination_address": "Liteyny prospect 10",
        "weight_grams": 750,
    }
    headers = {
        "Idempotency-Key": f"http-semantics-{uuid4()}",
        **auth(f"customer:{customer_id}"),
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        created = await client.post("/api/v1/orders", json=payload, headers=headers)
        location = created.headers["Location"]
        first = await client.get(location, headers=headers)
        second = await client.get(location, headers=headers)

    assert created.status_code == 201
    assert location.endswith(created.json()["id"])
    assert first.status_code == second.status_code == 200
    assert first.json()["status"] == second.json()["status"]
    assert first.json()["version"] == second.json()["version"]


@pytest.mark.asyncio
async def test_response_metadata_is_consistent_on_success_and_validation_error() -> None:
    customer_id = str(uuid4())
    request_id = f"lesson-16-{uuid4()}"
    headers = {
        "Idempotency-Key": f"lesson-16-{uuid4()}",
        "X-Request-ID": request_id,
        **auth(f"customer:{customer_id}"),
    }
    valid = {
        "customer_id": customer_id,
        "pickup_address": "Nevsky prospect 1",
        "destination_address": "Liteyny prospect 10",
        "weight_grams": 750,
    }
    invalid = {**valid, "weight_grams": 0}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        created = await client.post("/api/v1/orders", json=valid, headers=headers)
        rejected = await client.post("/api/v1/orders", json=invalid, headers=headers)

    assert created.status_code == 201
    assert created.headers["location"].endswith(created.json()["id"])
    assert created.headers["x-request-id"] == request_id
    assert rejected.status_code == 422
    assert rejected.headers["x-request-id"] == request_id
    assert created.headers["traceparent"].startswith("00-")
    assert rejected.headers["traceparent"].startswith("00-")


def test_create_order_openapi_declares_created_response() -> None:
    operation = app.openapi()["paths"]["/api/v1/orders"]["post"]
    assert "201" in operation["responses"]


@pytest.mark.asyncio
async def test_proof_upload_uses_form_metadata_content_detection_and_safe_download() -> None:
    customer_id = str(uuid4())
    headers = {
        "Idempotency-Key": f"proof-{uuid4()}",
        **auth(f"customer:{customer_id}"),
    }
    payload = {
        "customer_id": customer_id,
        "pickup_address": "Nevsky prospect 1",
        "destination_address": "Liteyny prospect 10",
        "weight_grams": 750,
    }
    png = b"\x89PNG\r\n\x1a\nnot-a-decoded-image-yet"

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        created = await client.post("/api/v1/orders", json=payload, headers=headers)
        order_id = created.json()["id"]
        accepted = await client.post(
            f"/api/v1/orders/{order_id}/proofs",
            headers=headers,
            data={"proof_kind": "delivery"},
            files={"file": ("proof.png", png, "application/octet-stream")},
        )
        downloaded = await client.get(accepted.headers["location"], headers=headers)
        presigned = await client.get(accepted.json()["download_url"])
        signed_url = accepted.json()["download_url"]
        tampered_signature = signed_url[:-1] + ("0" if signed_url[-1] != "0" else "1")
        tampered = await client.get(tampered_signature)
        spoofed = await client.post(
            f"/api/v1/orders/{order_id}/proofs",
            headers=headers,
            data={"proof_kind": "delivery"},
            files={"file": ("proof.png", b"MZ executable", "image/png")},
        )

    assert accepted.status_code == 201
    assert accepted.json()["proof_kind"] == "delivery"
    assert accepted.json()["media_type"] == "image/png"
    assert downloaded.status_code == 200 and downloaded.content == png
    assert presigned.status_code == 200 and presigned.content == png
    assert presigned.headers["cache-control"] == "private, no-store"
    assert tampered.status_code == 403
    assert downloaded.headers["content-disposition"].startswith('attachment; filename="proof-')
    assert downloaded.headers["x-content-type-options"] == "nosniff"
    assert spoofed.status_code == 415 and spoofed.json()["error_code"] == "invalid_proof"


@pytest.mark.asyncio
async def test_order_export_uses_current_state_and_attachment_headers() -> None:
    customer_id = str(uuid4())
    headers = {
        "Idempotency-Key": f"export-{uuid4()}",
        **auth(f"customer:{customer_id}"),
    }
    payload = {
        "customer_id": customer_id,
        "pickup_address": "Nevsky prospect 1",
        "destination_address": "Liteyny prospect 10",
        "weight_grams": 750,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        created = await client.post("/api/v1/orders", json=payload, headers=headers)
        cancelled = await client.post(
            f"/api/v1/orders/{created.json()['id']}/transitions",
            json={"status": "cancelled"},
            headers={**headers, "X-Expected-Version": "1"},
        )
        exported = await client.get(
            f"/api/v1/exports/{created.json()['id']}", headers=headers,
        )

    assert cancelled.status_code == 200
    assert exported.status_code == 200
    assert exported.text.endswith(",cancelled\n")
    assert exported.headers["content-disposition"].startswith('attachment; filename="order-')


@pytest.mark.asyncio
async def test_customer_can_cancel_once_but_cannot_repeat_terminal_transition() -> None:
    customer_id = "22222222-2222-4222-8222-222222222222"
    headers = {
        "Idempotency-Key": "api-test-transition",
        **auth(f"customer:{customer_id}"),
    }
    payload = {
        "customer_id": customer_id,
        "pickup_address": "First street 10",
        "destination_address": "Second street 20",
        "weight_grams": 500,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        created = await client.post("/api/v1/orders", json=payload, headers=headers)
        transition_url = f"/api/v1/orders/{created.json()['id']}/transitions"
        cancelled = await client.post(
            transition_url, json={"status": "cancelled"},
            headers={**headers, "X-Expected-Version": "1"},
        )
        repeated = await client.post(
            transition_url, json={"status": "cancelled"},
            headers={**headers, "X-Expected-Version": "2"},
        )
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    assert repeated.status_code == 409 and repeated.json()["error_code"] == "invalid_transition"


@pytest.mark.asyncio
async def test_payment_webhook_requires_signature_and_rejects_replay() -> None:
    event_id = uuid4()
    order_id = uuid4()
    payload = (
        '{"id":"%s","type":"payment.completed","data":'
        '{"order_id":"%s","payment_id":"pay-42"}}'
        % (event_id, order_id)
    ).encode()
    timestamp = int(time.time())
    signature = hmac.new(
        b"local-webhook-secret-change-me-1234",
        b"delivery.payment.v1\n" + str(timestamp).encode("ascii") + b"\n" + payload,
        hashlib.sha256,
    ).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "X-Event-ID": str(event_id),
        "X-Webhook-Timestamp": str(timestamp),
        "X-Webhook-Signature": signature,
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        accepted = await client.post("/api/v1/webhooks/payment", content=payload, headers=headers)
        replay = await client.post("/api/v1/webhooks/payment", content=payload, headers=headers)
        invalid = await client.post(
            "/api/v1/webhooks/payment", content=payload,
            headers={**headers, "X-Event-ID": str(uuid4()), "X-Webhook-Signature": "0" * 64},
        )
    assert accepted.status_code == 202
    assert accepted.json() == {"status": "accepted"}
    assert replay.status_code == 202
    assert replay.json() == {"status": "duplicate"}
    assert invalid.status_code == 401


@pytest.mark.asyncio
async def test_payment_webhook_rejects_stale_tampered_and_conflicting_event() -> None:
    event_id = uuid4()
    order_id = uuid4()
    secret = b"local-webhook-secret-change-me-1234"

    def signed(body: bytes, timestamp: int) -> dict[str, str]:
        signature = hmac.new(
            secret,
            b"delivery.payment.v1\n" + str(timestamp).encode() + b"\n" + body,
            hashlib.sha256,
        ).hexdigest()
        return {
            "Content-Type": "application/json",
            "X-Event-ID": str(event_id),
            "X-Webhook-Timestamp": str(timestamp),
            "X-Webhook-Signature": signature,
        }

    original = (
        '{"id":"%s","type":"payment.completed","data":'
        '{"order_id":"%s","payment_id":"pay-original"}}'
        % (event_id, order_id)
    ).encode()
    changed = original.replace(b"pay-original", b"pay-changed")
    now = int(time.time())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        tampered = await client.post(
            "/api/v1/webhooks/payment",
            content=changed,
            headers=signed(original, now),
        )
        stale = await client.post(
            "/api/v1/webhooks/payment",
            content=original,
            headers=signed(original, now - 301),
        )
        accepted = await client.post(
            "/api/v1/webhooks/payment", content=original, headers=signed(original, now)
        )
        conflict = await client.post(
            "/api/v1/webhooks/payment", content=changed, headers=signed(changed, now)
        )
    assert tampered.status_code == 401
    assert stale.status_code == 401
    assert accepted.status_code == 202
    assert conflict.status_code == 409
