from uuid import UUID

import pytest

from delivery_service.commands import CreateOrderCommand
from delivery_service.ports import CourierAlreadyBusy
from delivery_service.repository import InMemoryOrderRepository
from delivery_service.service import OrderService


@pytest.mark.asyncio
async def test_claim_records_one_transition_and_outbox_event() -> None:
    repository = InMemoryOrderRepository()
    service = OrderService(repository)
    tenant_id = UUID("69000000-0000-0000-0000-000000000001")
    courier_id = UUID("69000000-0000-0000-0000-000000000002")
    await service.create(
        CreateOrderCommand(
            customer_id=UUID("69000000-0000-0000-0000-000000000003"),
            pickup_address="Queue street 1",
            destination_address="Courier avenue 2",
            weight_grams=500,
        ),
        tenant_id=tenant_id,
    )

    claimed = await service.claim_next(
        courier_id=courier_id,
        tenant_id=tenant_id,
        correlation_id="lesson-69-claim",
    )

    assert claimed is not None
    assert claimed.status.value == "assigned"
    assert claimed.courier_id == courier_id
    assert claimed.version == 2
    assert len(repository.transition_history) == 1
    event = repository.transition_history[0]
    assert (event.from_status.value, event.to_status.value) == ("created", "assigned")
    assert event.correlation_id == "lesson-69-claim"
    assert repository.outbox[-1]["event_id"] == str(event.event_id)


@pytest.mark.asyncio
async def test_busy_courier_cannot_claim_second_order_or_create_effects() -> None:
    repository = InMemoryOrderRepository()
    service = OrderService(repository)
    tenant_id = UUID("69000000-0000-0000-0000-000000000011")
    courier_id = UUID("69000000-0000-0000-0000-000000000012")
    for suffix in (13, 14):
        await service.create(
            CreateOrderCommand(
                customer_id=UUID(f"69000000-0000-0000-0000-{suffix:012d}"),
                pickup_address=f"Queue street {suffix}",
                destination_address=f"Courier avenue {suffix}",
                weight_grams=500,
            ),
            tenant_id=tenant_id,
        )

    first = await service.claim_next(courier_id=courier_id, tenant_id=tenant_id)
    history_before = tuple(repository.transition_history)
    outbox_before = tuple(repository.outbox)

    with pytest.raises(CourierAlreadyBusy):
        await service.claim_next(courier_id=courier_id, tenant_id=tenant_id)

    assert first is not None
    assert tuple(repository.transition_history) == history_before
    assert tuple(repository.outbox) == outbox_before
    visible = list(repository._orders.values())
    assert sum(order.status.value == "assigned" for order in visible) == 1
    assert sum(order.status.value == "created" for order in visible) == 1
