from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from delivery_service.access import DEFAULT_TENANT_ID


class OrderStatus(StrEnum):
    CREATED = "created"
    ASSIGNED = "assigned"
    PICKED_UP = "picked_up"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


ALLOWED_TRANSITIONS = {
    OrderStatus.CREATED: frozenset({OrderStatus.ASSIGNED, OrderStatus.CANCELLED}),
    OrderStatus.ASSIGNED: frozenset({OrderStatus.PICKED_UP, OrderStatus.CANCELLED}),
    OrderStatus.PICKED_UP: frozenset({OrderStatus.DELIVERED}),
    OrderStatus.DELIVERED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
}


class InvalidTransition(ValueError):
    def __init__(self, current: OrderStatus, target: OrderStatus) -> None:
        super().__init__(f"{current.value} -> {target.value} is forbidden")
        self.current = current
        self.target = target


class OrderStateMachine:
    @staticmethod
    def allows(current: OrderStatus, target: OrderStatus) -> bool:
        return target in ALLOWED_TRANSITIONS[current]


@dataclass(frozen=True, slots=True)
class DeliveryOrder:
    id: UUID
    tenant_id: UUID
    customer_id: UUID
    pickup_address: str
    destination_address: str
    weight_grams: int
    status: OrderStatus
    courier_id: UUID | None
    version: int
    created_at: datetime

    @classmethod
    def create(
        cls,
        *,
        customer_id: UUID,
        tenant_id: UUID = DEFAULT_TENANT_ID,
        pickup_address: str,
        destination_address: str,
        weight_grams: int,
        created_at: datetime | None = None,
    ) -> "DeliveryOrder":
        if not 0 < weight_grams <= 100_000:
            raise ValueError("weight_grams must be between 1 and 100000")
        return cls(
            id=uuid4(),
            tenant_id=tenant_id,
            customer_id=customer_id,
            pickup_address=pickup_address,
            destination_address=destination_address,
            weight_grams=weight_grams,
            status=OrderStatus.CREATED,
            courier_id=None,
            version=1,
            created_at=created_at or datetime.now(UTC),
        )

    def transition(self, target: OrderStatus) -> "DeliveryOrder":
        if not OrderStateMachine.allows(self.status, target):
            raise InvalidTransition(self.status, target)
        return replace(self, status=target, version=self.version + 1)

    def assign(self, courier_id: UUID) -> "DeliveryOrder":
        assigned = self.transition(OrderStatus.ASSIGNED)
        return replace(assigned, courier_id=courier_id)

    def update_details(self, **changes) -> "DeliveryOrder":
        allowed = {"pickup_address", "destination_address", "weight_grams"}
        if not changes.keys() <= allowed:
            raise ValueError("unsupported order field")
        updated = replace(self, **changes, version=self.version + 1)
        if updated.pickup_address.casefold() == updated.destination_address.casefold():
            raise ValueError("pickup and destination addresses must differ")
        if not 0 < updated.weight_grams <= 100_000:
            raise ValueError("weight_grams must be between 1 and 100000")
        return updated
