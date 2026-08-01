from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from delivery_service.domain import DeliveryOrder, OrderStatus
from delivery_service.main import app
from delivery_service.repository import InMemoryOrderRepository
from delivery_service.security import create_token
from datetime import timedelta


def make_order(*, customer_id: UUID, status: OrderStatus = OrderStatus.CREATED) -> DeliveryOrder:
    order = DeliveryOrder.create(
        customer_id=customer_id,
        pickup_address="A street 10",
        destination_address="B street 20",
        weight_grams=500,
    )
    return replace(order, status=status)


@pytest.mark.asyncio
async def test_visibility_and_filtering_happen_before_pagination() -> None:
    repository = InMemoryOrderRepository()
    owner = uuid4()
    foreign = make_order(customer_id=uuid4())
    own_cancelled = make_order(customer_id=owner, status=OrderStatus.CANCELLED)
    own_created = make_order(customer_id=owner)
    for order in (foreign, own_created, own_cancelled):
        await repository.add(order)

    page = await repository.list_visible(
        role="customer",
        actor_id=owner,
        limit=1,
        offset=0,
        sort="created_at",
        status_filter="cancelled",
    )

    assert page == [own_cancelled]


@pytest.mark.asyncio
async def test_each_sort_has_a_unique_tie_breaker() -> None:
    repository = InMemoryOrderRepository()
    owner = uuid4()
    moment = datetime(2026, 1, 15, tzinfo=UTC)
    orders = [
        replace(make_order(customer_id=owner), created_at=moment)
        for _ in range(3)
    ]
    for order in reversed(orders):
        await repository.add(order)

    by_created = await repository.list_visible(
        role="customer", actor_id=owner, limit=10, offset=0,
        sort="created_at", status_filter=None,
    )
    by_status = await repository.list_visible(
        role="customer", actor_id=owner, limit=10, offset=0,
        sort="status", status_filter=None,
    )

    expected = sorted(orders, key=lambda order: order.id)
    assert by_created == expected
    assert by_status == expected


@pytest.mark.asyncio
async def test_repository_rejects_an_unknown_sort_field() -> None:
    repository = InMemoryOrderRepository()
    with pytest.raises(ValueError, match="unsupported sort field"):
        await repository.list_visible(
            role="dispatcher", actor_id=uuid4(), limit=20, offset=0,
            sort="created_at; drop table delivery_order", status_filter=None,
        )


@pytest.mark.asyncio
async def test_tenant_filter_is_applied_before_pagination() -> None:
    repository = InMemoryOrderRepository()
    actor_id = uuid4()
    own_tenant = uuid4()
    foreign = DeliveryOrder.create(
        tenant_id=uuid4(), customer_id=actor_id,
        pickup_address="A street 10", destination_address="B street 20",
        weight_grams=500,
    )
    own = DeliveryOrder.create(
        tenant_id=own_tenant, customer_id=actor_id,
        pickup_address="C street 30", destination_address="D street 40",
        weight_grams=500,
    )
    await repository.add(foreign)
    await repository.add(own)

    page = await repository.list_visible(
        role="customer", actor_id=actor_id, tenant_id=own_tenant,
        limit=1, offset=0, sort="created_at", status_filter=None,
    )

    assert page == [own]


def test_http_query_limits_and_sort_allowlist_are_enforced() -> None:
    token = create_token(
        f"customer:{uuid4()}", token_type="access", lifetime=timedelta(minutes=5)
    )
    headers = {"Authorization": f"Bearer {token}"}
    with TestClient(app) as client:
        too_large = client.get("/api/v1/orders?limit=101", headers=headers)
        negative_offset = client.get("/api/v1/orders?offset=-1", headers=headers)
        unknown_sort = client.get("/api/v1/orders?sort=customer_id", headers=headers)

    assert too_large.status_code == 422
    assert negative_offset.status_code == 422
    assert unknown_sort.status_code == 422
