"""Ports owned by the application layer.

This module intentionally has no SQLAlchemy, FastAPI, Redis or HTTPX imports.
Infrastructure adapters implement these contracts from the outside.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Protocol
from uuid import UUID

from delivery_service.access import DEFAULT_TENANT_ID
from delivery_service.domain import DeliveryOrder
from delivery_service.events import OrderCreated, OrderStatusChanged


class ConcurrentUpdate(RuntimeError):
    """The adapter rejected a stale aggregate version."""


class IdempotencyConflict(RuntimeError):
    """The same scoped key was reused for a different command payload."""


class CourierAlreadyBusy(RuntimeError):
    """The database rejected a second active assignment for one courier."""


class OrderRepository(Protocol):
    async def add(self, order: DeliveryOrder) -> None: ...
    async def get(self, order_id: UUID) -> DeliveryOrder | None: ...
    async def list(self, *, limit: int, offset: int) -> Iterable[DeliveryOrder]: ...
    async def list_visible(
        self, *, role: str, actor_id: UUID, limit: int, offset: int,
        sort: str, status_filter: str | None, tenant_id: UUID = DEFAULT_TENANT_ID,
    ) -> Iterable[DeliveryOrder]: ...
    async def save(self, order: DeliveryOrder, *, expected_version: int) -> None: ...
    async def save_transition(
        self,
        order: DeliveryOrder,
        *,
        expected_version: int,
        event: OrderStatusChanged,
    ) -> None: ...
    async def claim_next_created_order(
        self, *, courier_id: UUID, tenant_id: UUID = DEFAULT_TENANT_ID,
        occurred_at: datetime | None = None, correlation_id: str = "queue-claim",
    ) -> DeliveryOrder | None: ...
    async def idempotent_order(
        self, scope: str, request_fingerprint: str,
    ) -> DeliveryOrder | None: ...
    async def remember_idempotency(
        self, scope: str, request_fingerprint: str, order: DeliveryOrder,
    ) -> None: ...
    async def record_created_event(self, event: OrderCreated) -> None: ...
