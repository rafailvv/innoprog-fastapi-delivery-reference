from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from delivery_service.schemas import DeliveryWindow, OrderCreate, OrderUpdate


def test_order_create_normalizes_address_spaces() -> None:
    command = OrderCreate(
        customer_id=uuid4(),
        pickup_address="  Невский   проспект, 1 ",
        destination_address="Литейный проспект, 2",
        weight_grams=750,
    )
    assert command.pickup_address == "Невский проспект, 1"


def test_order_create_rejects_equivalent_normalized_addresses() -> None:
    with pytest.raises(ValueError):
        OrderCreate(
            customer_id=uuid4(),
            pickup_address="Невский  проспект, 1",
            destination_address=" невский проспект, 1 ",
            weight_grams=750,
        )


def test_order_create_rejects_server_owned_fields() -> None:
    with pytest.raises(ValueError):
        OrderCreate.model_validate({
            "customer_id": str(uuid4()),
            "pickup_address": "Невский проспект, 1",
            "destination_address": "Литейный проспект, 2",
            "weight_grams": 750,
            "status": "delivered",
        })


def test_patch_distinguishes_missing_and_explicit_null() -> None:
    missing = OrderUpdate().model_dump(exclude_unset=True)
    explicit = OrderUpdate(destination_address=None).model_dump(exclude_unset=True)
    assert missing == {}
    assert explicit == {"destination_address": None}


@pytest.mark.parametrize(
    "duration",
    [timedelta(0), timedelta(minutes=-1), timedelta(hours=4, seconds=1)],
)
def test_delivery_window_rejects_invalid_duration(duration: timedelta) -> None:
    starts_at = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        DeliveryWindow(starts_at=starts_at, ends_at=starts_at + duration)


def test_delivery_window_accepts_four_hours() -> None:
    starts_at = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    window = DeliveryWindow(starts_at=starts_at, ends_at=starts_at + timedelta(hours=4))
    assert window.ends_at - window.starts_at == timedelta(hours=4)
