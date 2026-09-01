from uuid import uuid4

import pytest

from delivery_service.domain import DeliveryOrder, InvalidTransition, OrderStatus


def test_order_state_machine() -> None:
    order = DeliveryOrder.create(
        customer_id=uuid4(),
        pickup_address="First street 1",
        destination_address="Second street 2",
        weight_grams=500,
    )
    assigned = order.assign(uuid4())
    assert assigned.status is OrderStatus.ASSIGNED
    assert assigned.version == 2

    with pytest.raises(InvalidTransition):
        order.transition(OrderStatus.DELIVERED)


@pytest.mark.parametrize("weight_grams", (0, -1, 100_001))
def test_order_domain_rejects_weight_outside_business_range(weight_grams: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 100000"):
        DeliveryOrder.create(
            customer_id=uuid4(),
            pickup_address="First street 1",
            destination_address="Second street 2",
            weight_grams=weight_grams,
        )


@pytest.mark.parametrize("weight_grams", (1, 100_000))
def test_order_domain_accepts_weight_boundaries(weight_grams: int) -> None:
    order = DeliveryOrder.create(
        customer_id=uuid4(),
        pickup_address="First street 1",
        destination_address="Second street 2",
        weight_grams=weight_grams,
    )
    assert order.weight_grams == weight_grams
