from __future__ import annotations

from uuid import UUID

import pytest

from delivery_service.domain import OrderStatus
from delivery_service.commands import CreateOrderCommand
from delivery_service.service import OrderNotFound, OrderService
from tests.fakes import FakeOrderRepository


def create_command(customer_id: UUID) -> CreateOrderCommand:
    return CreateOrderCommand(
        customer_id=customer_id,
        pickup_address="Казань, Баумана, 1",
        destination_address="Казань, Кремлёвская, 18",
        weight_grams=750,
    )


@pytest.fixture
def fake_repository() -> FakeOrderRepository:
    return FakeOrderRepository()


@pytest.fixture
def service(fake_repository, fixed_clock) -> OrderService:
    return OrderService(fake_repository, clock=fixed_clock)


@pytest.mark.asyncio
async def test_create_persists_order_and_records_one_event(
    service: OrderService,
    fake_repository: FakeOrderRepository,
    fixed_now,
) -> None:
    order = await service.create(create_command(UUID(int=101)))

    assert order.created_at == fixed_now
    assert order.status is OrderStatus.CREATED
    assert await fake_repository.all_orders() == [order]
    assert fake_repository.added == [order]
    assert fake_repository.created_events == [order.id]


@pytest.mark.asyncio
async def test_assign_saves_next_version_with_expected_snapshot(
    service: OrderService,
    fake_repository: FakeOrderRepository,
) -> None:
    created = await service.create(create_command(UUID(int=102)))

    assigned = await service.assign(created.id, UUID(int=501))

    assert assigned.status is OrderStatus.ASSIGNED
    assert assigned.courier_id == UUID(int=501)
    assert assigned.version == created.version + 1
    assert fake_repository.saved == [(assigned, created.version)]
    assert len(fake_repository.transition_history) == 1
    assert fake_repository.transition_history[0].from_status is OrderStatus.CREATED
    assert fake_repository.transition_history[0].to_status is OrderStatus.ASSIGNED


@pytest.mark.asyncio
async def test_missing_order_does_not_attempt_save(
    service: OrderService,
    fake_repository: FakeOrderRepository,
) -> None:
    with pytest.raises(OrderNotFound, match=str(UUID(int=404))):
        await service.assign(UUID(int=404), UUID(int=501))

    assert fake_repository.saved == []


@pytest.mark.asyncio
async def test_idempotent_retry_does_not_duplicate_effects(
    service: OrderService,
    fake_repository: FakeOrderRepository,
) -> None:
    command = create_command(UUID(int=103))

    first = await service.create(command, idempotency_key="mobile-attempt-1")
    replay = await service.create(command, idempotency_key="mobile-attempt-1")

    assert replay == first
    assert fake_repository.added == [first]
    assert fake_repository.created_events == [first.id]
