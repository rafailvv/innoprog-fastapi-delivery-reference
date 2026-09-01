from __future__ import annotations

import hashlib
import json
from uuid import UUID

from delivery_service.commands import (
    CreateOrderCommand,
    TransitionOrderCommand,
    UpdateOrderCommand,
)
from delivery_service.domain import DeliveryOrder
from delivery_service.events import OrderCreated, OrderStatusChanged
from delivery_service.ports import ConcurrentUpdate
from delivery_service.ports import OrderRepository
from delivery_service.access import DEFAULT_TENANT_ID
from delivery_service.clock import Clock, SystemClock


class OrderNotFound(LookupError):
    pass


class OrderService:
    def __init__(self, repository: OrderRepository, clock: Clock | None = None) -> None:
        self.repository = repository
        self.clock = clock or SystemClock()

    async def create(
        self,
        command: CreateOrderCommand,
        *,
        idempotency_key: str | None = None,
        tenant_id: UUID = DEFAULT_TENANT_ID,
    ) -> DeliveryOrder:
        scope = None
        request_fingerprint = None
        if idempotency_key:
            scope = f"create_order:{tenant_id}:{command.customer_id}:{idempotency_key}"
            canonical_request = json.dumps(
                command.canonical_payload(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            request_fingerprint = hashlib.sha256(canonical_request).hexdigest()
            existing = await self.repository.idempotent_order(scope, request_fingerprint)
            if existing is not None:
                return existing
        order = DeliveryOrder.create(
            tenant_id=tenant_id,
            created_at=self.clock.now(),
            customer_id=command.customer_id,
            pickup_address=command.pickup_address,
            destination_address=command.destination_address,
            weight_grams=command.weight_grams,
        )
        await self.repository.add(order)
        await self.repository.record_created_event(OrderCreated.from_order(order))
        if scope is not None and request_fingerprint is not None:
            await self.repository.remember_idempotency(scope, request_fingerprint, order)
        return order

    async def get(self, order_id: UUID) -> DeliveryOrder:
        order = await self.repository.get(order_id)
        if order is None:
            raise OrderNotFound(str(order_id))
        return order

    async def list(self, *, limit: int, offset: int) -> list[DeliveryOrder]:
        return list(await self.repository.list(limit=limit, offset=offset))

    async def list_visible(
        self, *, role: str, actor_id: UUID, limit: int, offset: int,
        sort: str, status_filter: str | None, tenant_id: UUID = DEFAULT_TENANT_ID,
    ) -> list[DeliveryOrder]:
        return list(await self.repository.list_visible(
            role=role, actor_id=actor_id, tenant_id=tenant_id, limit=limit, offset=offset,
            sort=sort, status_filter=status_filter,
        ))

    async def assign(self, order_id: UUID, courier_id: UUID) -> DeliveryOrder:
        current = await self.get(order_id)
        updated = current.assign(courier_id)
        event = OrderStatusChanged.from_transition(
            before=current,
            after=updated,
            actor_id=courier_id,
            actor_role="courier",
            occurred_at=self.clock.now(),
            correlation_id="assignment",
        )
        await self.repository.save_transition(
            updated, expected_version=current.version, event=event,
        )
        return updated

    async def claim_next(
        self, *, courier_id: UUID, tenant_id: UUID = DEFAULT_TENANT_ID,
        correlation_id: str = "queue-claim",
    ) -> DeliveryOrder | None:
        """Claim one queue item through the repository's atomic transaction adapter."""

        return await self.repository.claim_next_created_order(
            courier_id=courier_id,
            tenant_id=tenant_id,
            occurred_at=self.clock.now(),
            correlation_id=correlation_id,
        )

    async def update(
        self, order_id: UUID, command: UpdateOrderCommand, *, expected_version: int
    ) -> DeliveryOrder:
        current = await self.get(order_id)
        if current.version != expected_version:
            raise ConcurrentUpdate("optimistic version conflict")
        updated = current.update_details(**command.changes())
        await self.repository.save(updated, expected_version=expected_version)
        return updated

    async def transition(
        self, order_id: UUID, command: TransitionOrderCommand
    ) -> DeliveryOrder:
        current = await self.get(order_id)
        if current.version != command.expected_version:
            raise ConcurrentUpdate("optimistic version conflict")
        updated = current.transition(command.target)
        event = OrderStatusChanged.from_transition(
            before=current,
            after=updated,
            actor_id=command.actor_id,
            actor_role=command.actor_role,
            occurred_at=self.clock.now(),
            correlation_id=command.correlation_id,
        )
        await self.repository.save_transition(
            updated,
            expected_version=command.expected_version,
            event=event,
        )
        return updated
