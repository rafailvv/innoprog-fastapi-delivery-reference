"""Domain facts and their explicit integration-event representation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from delivery_service.domain import DeliveryOrder, OrderStatus


@dataclass(frozen=True, slots=True)
class OrderCreated:
    """A past-tense domain fact produced by the create-order use case."""

    event_id: UUID
    order_id: UUID
    customer_id: UUID
    tenant_id: UUID
    occurred_at: datetime
    order_version: int

    @classmethod
    def from_order(cls, order: DeliveryOrder) -> "OrderCreated":
        return cls(
            event_id=uuid4(),
            order_id=order.id,
            customer_id=order.customer_id,
            tenant_id=order.tenant_id,
            occurred_at=order.created_at,
            order_version=order.version,
        )

    def integration_payload(self) -> dict[str, object]:
        """Create a stable DTO instead of serializing an ORM/domain object."""

        return {
            "schema_version": 1,
            "event_id": str(self.event_id),
            "event_type": "order.created",
            "aggregate_id": str(self.order_id),
            "aggregate_version": self.order_version,
            "tenant_id": str(self.tenant_id),
            "customer_id": str(self.customer_id),
            "occurred_at": self.occurred_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OrderStatusChanged:
    """Immutable fact shared by audit history and the integration outbox."""

    event_id: UUID
    order_id: UUID
    tenant_id: UUID
    from_status: OrderStatus
    to_status: OrderStatus
    order_version: int
    actor_id: UUID
    actor_role: str
    occurred_at: datetime
    correlation_id: str

    @classmethod
    def from_transition(
        cls,
        *,
        before: DeliveryOrder,
        after: DeliveryOrder,
        actor_id: UUID,
        actor_role: str,
        occurred_at: datetime,
        correlation_id: str,
    ) -> "OrderStatusChanged":
        if before.id != after.id or after.version != before.version + 1:
            raise ValueError("transition event requires one aggregate version step")
        if before.status is after.status:
            raise ValueError("transition event requires a changed status")
        return cls(
            event_id=uuid4(),
            order_id=after.id,
            tenant_id=after.tenant_id,
            from_status=before.status,
            to_status=after.status,
            order_version=after.version,
            actor_id=actor_id,
            actor_role=actor_role,
            occurred_at=occurred_at,
            correlation_id=correlation_id,
        )

    def integration_payload(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "event_id": str(self.event_id),
            "event_type": "order.status_changed",
            "aggregate_id": str(self.order_id),
            "aggregate_version": self.order_version,
            "tenant_id": str(self.tenant_id),
            "from_status": self.from_status.value,
            "to_status": self.to_status.value,
            "actor_id": str(self.actor_id),
            "actor_role": self.actor_role,
            "occurred_at": self.occurred_at.isoformat(),
            "correlation_id": self.correlation_id,
        }
