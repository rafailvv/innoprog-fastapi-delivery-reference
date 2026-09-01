from __future__ import annotations

from dataclasses import replace
from uuid import UUID

import pytest

from delivery_service.commands import CreateOrderCommand, TransitionOrderCommand
from delivery_service.domain import (
    ALLOWED_TRANSITIONS,
    DeliveryOrder,
    InvalidTransition,
    OrderStatus,
)
from delivery_service.ports import ConcurrentUpdate
from delivery_service.repository import InMemoryOrderRepository
from delivery_service.service import OrderService


def _order(status: OrderStatus, *, version: int = 4) -> DeliveryOrder:
    return replace(
        DeliveryOrder.create(
            customer_id=UUID(int=6801),
            pickup_address="Казань, Баумана, 1",
            destination_address="Казань, Кремлёвская, 18",
            weight_grams=800,
        ),
        id=UUID(int=6802),
        tenant_id=UUID(int=6803),
        status=status,
        version=version,
        courier_id=UUID(int=6804) if status not in {OrderStatus.CREATED, OrderStatus.CANCELLED} else None,
    )


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (current, target)
        for current, targets in ALLOWED_TRANSITIONS.items()
        for target in targets
    ],
)
def test_every_declared_state_machine_edge_advances_exactly_one_version(
    current: OrderStatus, target: OrderStatus,
) -> None:
    before = _order(current)

    after = before.transition(target)

    assert after.status is target
    assert after.version == before.version + 1
    assert before.status is current and before.version == 4


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (current, target)
        for current in OrderStatus
        for target in OrderStatus
        if target not in ALLOWED_TRANSITIONS[current]
    ],
)
def test_every_absent_edge_is_rejected_without_mutating_the_order(
    current: OrderStatus, target: OrderStatus,
) -> None:
    before = _order(current)

    with pytest.raises(InvalidTransition) as captured:
        before.transition(target)

    assert captured.value.current is current
    assert captured.value.target is target
    assert before.status is current and before.version == 4


def _command(*, target: OrderStatus, expected_version: int = 1) -> TransitionOrderCommand:
    return TransitionOrderCommand(
        target=target,
        expected_version=expected_version,
        actor_id=UUID(int=6805),
        actor_role="customer",
        correlation_id="lesson-68-transition",
    )


@pytest.mark.asyncio
async def test_transition_persists_state_history_and_outbox_as_one_application_result(
    fixed_clock,
) -> None:
    repository = InMemoryOrderRepository()
    service = OrderService(repository, clock=fixed_clock)
    created = await service.create(CreateOrderCommand(
        customer_id=UUID(int=6805),
        pickup_address="Казань, Баумана, 1",
        destination_address="Казань, Кремлёвская, 18",
        weight_grams=800,
    ))

    cancelled = await service.transition(
        created.id, _command(target=OrderStatus.CANCELLED),
    )

    assert cancelled.status is OrderStatus.CANCELLED
    assert cancelled.version == 2
    assert await repository.get(created.id) == cancelled
    assert len(repository.transition_history) == 1
    event = repository.transition_history[0]
    assert (event.from_status, event.to_status) == (
        OrderStatus.CREATED, OrderStatus.CANCELLED,
    )
    assert event.order_version == 2
    assert event.actor_id == UUID(int=6805)
    assert event.actor_role == "customer"
    assert event.correlation_id == "lesson-68-transition"
    assert repository.outbox[-1] == event.integration_payload()
    assert "pickup_address" not in repr(event.integration_payload())
    assert "authorization" not in repr(event.integration_payload()).casefold()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "error"),
    [
        (_command(target=OrderStatus.DELIVERED), InvalidTransition),
        (_command(target=OrderStatus.CANCELLED, expected_version=99), ConcurrentUpdate),
    ],
)
async def test_rejected_transition_has_no_state_history_or_outbox_effect(
    fixed_clock, command: TransitionOrderCommand, error: type[Exception],
) -> None:
    repository = InMemoryOrderRepository()
    service = OrderService(repository, clock=fixed_clock)
    created = await service.create(CreateOrderCommand(
        customer_id=UUID(int=6805),
        pickup_address="Казань, Баумана, 1",
        destination_address="Казань, Кремлёвская, 18",
        weight_grams=800,
    ))
    before_outbox = list(repository.outbox)

    with pytest.raises(error):
        await service.transition(created.id, command)

    assert await repository.get(created.id) == created
    assert repository.transition_history == []
    assert repository.outbox == before_outbox
