from __future__ import annotations

from uuid import UUID

from delivery_service.domain import DeliveryOrder
from delivery_service.events import OrderCreated
from delivery_service.repository import InMemoryOrderRepository


class FakeOrderRepository(InMemoryOrderRepository):
    """Stateful test implementation of the repository port.

    It preserves repository semantics and records only interactions that are
    part of the application-service contract.
    """

    def __init__(self) -> None:
        super().__init__()
        self.added: list[DeliveryOrder] = []
        self.saved: list[tuple[DeliveryOrder, int]] = []
        self.created_events: list[UUID] = []

    async def add(self, order: DeliveryOrder) -> None:
        self.added.append(order)
        await super().add(order)

    async def save(self, order: DeliveryOrder, *, expected_version: int) -> None:
        self.saved.append((order, expected_version))
        await super().save(order, expected_version=expected_version)

    async def record_created_event(self, event: OrderCreated) -> None:
        self.created_events.append(event.order_id)
        await super().record_created_event(event)

    async def all_orders(self) -> list[DeliveryOrder]:
        return await self.list(limit=100, offset=0)
