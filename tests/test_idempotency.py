from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from delivery_service.main import app
from delivery_service.repository import IdempotencyConflict, InMemoryOrderRepository
from delivery_service.commands import CreateOrderCommand
from delivery_service.security import create_token
from delivery_service.service import OrderService
from datetime import timedelta


def command(customer_id, *, weight_grams: int = 500) -> CreateOrderCommand:
    return CreateOrderCommand(
        customer_id=customer_id,
        pickup_address="A street 10",
        destination_address="B street 20",
        weight_grams=weight_grams,
    )


@pytest.mark.asyncio
async def test_same_scope_and_fingerprint_replays_one_effect() -> None:
    repository = InMemoryOrderRepository()
    service = OrderService(repository)
    customer_id = uuid4()

    first = await service.create(command(customer_id), idempotency_key="checkout-42")
    second = await service.create(command(customer_id), idempotency_key="checkout-42")

    assert second == first
    assert len(repository._orders) == 1
    assert len(repository.outbox) == 1


@pytest.mark.asyncio
async def test_retry_replays_original_response_after_resource_changed() -> None:
    # A response snapshot is distinct from loading the resource in its latest state.
    repository = InMemoryOrderRepository()
    service = OrderService(repository)
    customer_id = uuid4()

    created = await service.create(command(customer_id), idempotency_key="checkout-snapshot")
    changed = created.update_details(weight_grams=975)
    await repository.save(changed, expected_version=created.version)

    replayed = await service.create(command(customer_id), idempotency_key="checkout-snapshot")

    assert replayed == created
    assert replayed.version == 1
    assert replayed.weight_grams == 500
    assert await repository.get(created.id) == changed
    assert len(repository.outbox) == 1


@pytest.mark.asyncio
async def test_reusing_key_with_another_payload_is_a_conflict() -> None:
    repository = InMemoryOrderRepository()
    service = OrderService(repository)
    customer_id = uuid4()
    await service.create(command(customer_id, weight_grams=500), idempotency_key="checkout-42")

    with pytest.raises(IdempotencyConflict):
        await service.create(
            command(customer_id, weight_grams=900), idempotency_key="checkout-42"
        )

    assert len(repository._orders) == 1
    assert len(repository.outbox) == 1


@pytest.mark.asyncio
async def test_the_same_raw_key_is_scoped_to_customer_and_operation() -> None:
    repository = InMemoryOrderRepository()
    service = OrderService(repository)

    first = await service.create(command(uuid4()), idempotency_key="mobile-retry")
    second = await service.create(command(uuid4()), idempotency_key="mobile-retry")

    assert first.id != second.id
    assert len(repository._orders) == 2


def test_openapi_documents_version_operation_location_and_conflict() -> None:
    schema = app.openapi()
    operation = schema["paths"]["/api/v1/orders"]["post"]

    assert schema["openapi"].startswith("3.")
    assert schema["info"]["version"] == "1.0.0"
    assert operation["operationId"] == "createOrderV1"
    assert operation["responses"]["201"]["headers"]["Location"]
    assert "409" in operation["responses"]


def test_http_rejects_mismatched_replay_and_oversized_key() -> None:
    customer_id = uuid4()
    token = create_token(
        f"customer:{customer_id}", token_type="access", lifetime=timedelta(minutes=5)
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Idempotency-Key": "lesson-18-fingerprint",
    }
    first_payload = command(customer_id, weight_grams=500).canonical_payload()
    second_payload = command(customer_id, weight_grams=900).canonical_payload()

    with TestClient(app) as client:
        first = client.post("/api/v1/orders", headers=headers, json=first_payload)
        conflict = client.post("/api/v1/orders", headers=headers, json=second_payload)
        oversized = client.post(
            "/api/v1/orders",
            headers={**headers, "Idempotency-Key": "x" * 129},
            json=first_payload,
        )
        invalid_characters = client.post(
            "/api/v1/orders",
            headers={**headers, "Idempotency-Key": "spaces are not allowed"},
            json=first_payload,
        )

    assert first.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json()["error_code"] == "idempotency_conflict"
    assert oversized.status_code == 422
    assert invalid_characters.status_code == 422
