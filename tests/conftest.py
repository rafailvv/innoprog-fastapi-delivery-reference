from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from itertools import count
from uuid import UUID

import pytest

from delivery_service.domain import DeliveryOrder
from delivery_service.repository import InMemoryOrderRepository


@dataclass(frozen=True, slots=True)
class FixedClock:
    instant: datetime

    def now(self) -> datetime:
        return self.instant


OrderFactory = Callable[..., DeliveryOrder]


@pytest.fixture
def fixed_now() -> datetime:
    """Stable time owned by one test instead of the wall clock."""
    return datetime(2026, 1, 15, 10, 0, tzinfo=UTC)


@pytest.fixture
def fixed_clock(fixed_now: datetime) -> FixedClock:
    return FixedClock(fixed_now)


@pytest.fixture
def order_factory(fixed_now: datetime) -> OrderFactory:
    """Create fresh, readable orders without shared mutable fixture state."""
    sequence = count(1)

    def build(**changes: object) -> DeliveryOrder:
        number = next(sequence)
        weight_grams = int(changes.pop("weight_grams", 750))
        order = DeliveryOrder.create(
            customer_id=changes.pop("customer_id", UUID(int=1_000 + number)),
            tenant_id=changes.pop("tenant_id", UUID(int=2_000 + number)),
            pickup_address=str(changes.pop("pickup_address", "Казань, Баумана, 1")),
            destination_address=str(
                changes.pop("destination_address", "Казань, Кремлёвская, 18")
            ),
            weight_grams=weight_grams,
        )
        return replace(
            order,
            id=changes.pop("id", UUID(int=number)),
            created_at=changes.pop("created_at", fixed_now),
            **changes,
        )

    return build


@pytest.fixture
def repository() -> InMemoryOrderRepository:
    """A new mutable adapter for every test item."""
    return InMemoryOrderRepository()
