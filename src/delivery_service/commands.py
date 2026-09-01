"""Framework-independent input models for application use cases."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from delivery_service.domain import OrderStatus


@dataclass(frozen=True, slots=True)
class CreateOrderCommand:
    customer_id: UUID
    pickup_address: str
    destination_address: str
    weight_grams: int

    def canonical_payload(self) -> dict[str, str | int]:
        return {
            "customer_id": str(self.customer_id),
            "pickup_address": self.pickup_address,
            "destination_address": self.destination_address,
            "weight_grams": self.weight_grams,
        }


@dataclass(frozen=True, slots=True)
class UpdateOrderCommand:
    pickup_address: str | None = None
    destination_address: str | None = None
    weight_grams: int | None = None

    def changes(self) -> dict[str, str | int]:
        return {
            name: value
            for name, value in (
                ("pickup_address", self.pickup_address),
                ("destination_address", self.destination_address),
                ("weight_grams", self.weight_grams),
            )
            if value is not None
        }


@dataclass(frozen=True, slots=True)
class TransitionOrderCommand:
    """Framework-free intent plus the audit context owned by the caller."""

    target: OrderStatus
    expected_version: int
    actor_id: UUID
    actor_role: str
    correlation_id: str
