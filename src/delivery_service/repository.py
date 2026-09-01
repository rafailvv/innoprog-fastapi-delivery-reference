from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from delivery_service.domain import DeliveryOrder
from delivery_service.events import OrderCreated, OrderStatusChanged
from delivery_service.access import DEFAULT_TENANT_ID
from delivery_service.ports import CourierAlreadyBusy, ConcurrentUpdate, IdempotencyConflict
from delivery_service.ports import OrderRepository


class InMemoryOrderRepository:
    def __init__(self) -> None:
        self._orders: dict[UUID, DeliveryOrder] = {}
        # Keep the response snapshot, not a pointer to the mutable resource.
        # A retry must replay the result of the original command even if the
        # order has changed since that command completed.
        self._idempotency: dict[str, tuple[str, DeliveryOrder]] = {}
        self.outbox: list[dict[str, object]] = []
        self.transition_history: list[OrderStatusChanged] = []

    async def add(self, order: DeliveryOrder) -> None:
        self._orders[order.id] = order

    async def get(self, order_id: UUID) -> DeliveryOrder | None:
        return self._orders.get(order_id)

    async def list(self, *, limit: int, offset: int) -> list[DeliveryOrder]:
        return list(self._orders.values())[offset : offset + limit]

    async def list_visible(
        self, *, role: str, actor_id: UUID, limit: int, offset: int,
        sort: str, status_filter: str | None, tenant_id: UUID = DEFAULT_TENANT_ID,
    ) -> list[DeliveryOrder]:
        if sort not in {"created_at", "status"}:
            raise ValueError("unsupported sort field")
        visible = [
            order for order in self._orders.values()
            if order.tenant_id == tenant_id and (
                role == "dispatcher"
                or (role == "customer" and order.customer_id == actor_id)
                or (role == "courier" and order.courier_id == actor_id)
            )
        ]
        if status_filter is not None:
            visible = [order for order in visible if order.status.value == status_filter]
        visible.sort(
            key=(lambda order: (order.status.value, order.id)) if sort == "status" else (
                lambda order: (order.created_at, order.id)
            )
        )
        return visible[offset : offset + limit]

    async def save(self, order: DeliveryOrder, *, expected_version: int) -> None:
        current = self._orders.get(order.id)
        if current is None or current.version != expected_version:
            raise ConcurrentUpdate("optimistic version conflict")
        if order.version != expected_version + 1:
            raise ValueError("domain state must advance version exactly once")
        self._orders[order.id] = order

    async def save_transition(
        self,
        order: DeliveryOrder,
        *,
        expected_version: int,
        event: OrderStatusChanged,
    ) -> None:
        if event.order_id != order.id or event.order_version != order.version:
            raise ValueError("transition event does not describe the persisted order")
        await self.save(order, expected_version=expected_version)
        self.transition_history.append(event)
        self.outbox.append(event.integration_payload())

    async def claim_next_created_order(
        self, *, courier_id: UUID, tenant_id: UUID = DEFAULT_TENANT_ID,
        occurred_at: datetime | None = None, correlation_id: str = "queue-claim",
    ) -> DeliveryOrder | None:
        if any(
            order.tenant_id == tenant_id and order.courier_id == courier_id
            and order.status.value in {"assigned", "picked_up"}
            for order in self._orders.values()
        ):
            raise CourierAlreadyBusy("courier already has an active assignment")
        candidates = sorted(
            (
                order for order in self._orders.values()
                if order.tenant_id == tenant_id and order.status.value == "created"
            ),
            key=lambda order: (order.created_at, order.id),
        )
        if not candidates:
            return None
        claimed = candidates[0].assign(courier_id)
        self._orders[claimed.id] = claimed
        event = OrderStatusChanged.from_transition(
            before=candidates[0], after=claimed, actor_id=courier_id,
            actor_role="courier", occurred_at=occurred_at or datetime.now(UTC),
            correlation_id=correlation_id,
        )
        self.transition_history.append(event)
        self.outbox.append(event.integration_payload())
        return claimed

    async def idempotent_order(
        self, scope: str, request_fingerprint: str,
    ) -> DeliveryOrder | None:
        record = self._idempotency.get(scope)
        if record is None:
            return None
        saved_fingerprint, response_snapshot = record
        if saved_fingerprint != request_fingerprint:
            raise IdempotencyConflict("idempotency key was already used for another request")
        return response_snapshot

    async def remember_idempotency(
        self, scope: str, request_fingerprint: str, order: DeliveryOrder,
    ) -> None:
        self._idempotency[scope] = (request_fingerprint, order)

    async def record_created_event(self, event: OrderCreated) -> None:
        self.outbox.append(event.integration_payload())


class SqlAlchemyOrderRepository:
    """PostgreSQL adapter; the session and transaction are owned by a dependency."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @staticmethod
    def _domain(row) -> DeliveryOrder:
        from delivery_service.domain import OrderStatus

        assignment = next((item for item in row.assignments if item.active), None)
        return DeliveryOrder(
            id=row.id,
            tenant_id=row.tenant_id,
            customer_id=row.customer_id,
            pickup_address=row.pickup_address,
            destination_address=row.destination_address,
            weight_grams=row.weight_grams,
            status=OrderStatus(row.status),
            courier_id=assignment.courier_id if assignment else None,
            version=row.version,
            created_at=row.created_at,
        )

    @staticmethod
    def _response_snapshot(order: DeliveryOrder) -> dict[str, object]:
        """Serialize the exact create response stored for a safe replay."""
        return {
            "id": str(order.id),
            "tenant_id": str(order.tenant_id),
            "customer_id": str(order.customer_id),
            "pickup_address": order.pickup_address,
            "destination_address": order.destination_address,
            "weight_grams": order.weight_grams,
            "status": order.status.value,
            "courier_id": str(order.courier_id) if order.courier_id else None,
            "version": order.version,
            "created_at": order.created_at.isoformat(),
        }

    @staticmethod
    def _domain_from_snapshot(payload: dict[str, object]) -> DeliveryOrder:
        from datetime import datetime
        from delivery_service.domain import OrderStatus

        courier_id = payload.get("courier_id")
        return DeliveryOrder(
            id=UUID(str(payload["id"])),
            tenant_id=UUID(str(payload["tenant_id"])),
            customer_id=UUID(str(payload["customer_id"])),
            pickup_address=str(payload["pickup_address"]),
            destination_address=str(payload["destination_address"]),
            weight_grams=int(payload["weight_grams"]),
            status=OrderStatus(str(payload["status"])),
            courier_id=UUID(str(courier_id)) if courier_id else None,
            version=int(payload["version"]),
            created_at=datetime.fromisoformat(str(payload["created_at"])),
        )

    async def add(self, order: DeliveryOrder) -> None:
        from delivery_service.db import OrderRow

        self.session.add(OrderRow(
            id=order.id,
            tenant_id=order.tenant_id,
            customer_id=order.customer_id,
            pickup_address=order.pickup_address,
            destination_address=order.destination_address,
            weight_grams=order.weight_grams,
            status=order.status.value,
            version=order.version,
            created_at=order.created_at,
        ))
        await self.session.flush()

    async def get(self, order_id: UUID) -> DeliveryOrder | None:
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload
        from delivery_service.db import OrderRow

        row = await self.session.scalar(
            select(OrderRow)
            .options(selectinload(OrderRow.assignments))
            .where(OrderRow.id == order_id)
        )
        return self._domain(row) if row else None

    async def list(self, *, limit: int, offset: int) -> list[DeliveryOrder]:
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload
        from delivery_service.db import OrderRow

        rows = (await self.session.scalars(
            select(OrderRow)
            .options(selectinload(OrderRow.assignments))
            .order_by(OrderRow.created_at, OrderRow.id)
            .limit(limit)
            .offset(offset)
        )).all()
        return [self._domain(row) for row in rows]

    async def list_visible(
        self, *, role: str, actor_id: UUID, limit: int, offset: int,
        sort: str, status_filter: str | None, tenant_id: UUID = DEFAULT_TENANT_ID,
    ) -> list[DeliveryOrder]:
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload
        from delivery_service.db import AssignmentRow, OrderRow

        orderings = {
            "created_at": (OrderRow.created_at, OrderRow.id),
            "status": (OrderRow.status, OrderRow.id),
        }
        if sort not in orderings:
            raise ValueError("unsupported sort field")

        statement = (
            select(OrderRow)
            .options(selectinload(OrderRow.assignments))
            .where(OrderRow.tenant_id == tenant_id)
        )
        if role == "customer":
            statement = statement.where(OrderRow.customer_id == actor_id)
        elif role == "courier":
            statement = statement.where(
                OrderRow.assignments.any(AssignmentRow.courier_id == actor_id)
            )
        elif role != "dispatcher":
            return []
        if status_filter is not None:
            statement = statement.where(OrderRow.status == status_filter)
        rows = (await self.session.scalars(
            statement.order_by(*orderings[sort]).limit(limit).offset(offset)
        )).all()
        return [self._domain(row) for row in rows]

    async def save(self, order: DeliveryOrder, *, expected_version: int) -> None:
        from sqlalchemy import select, update
        from delivery_service.db import AssignmentRow, OrderRow

        if order.version != expected_version + 1:
            raise ValueError("domain state must advance version exactly once")
        result = await self.session.execute(
            update(OrderRow)
            .where(OrderRow.id == order.id, OrderRow.version == expected_version)
            .values(
                pickup_address=order.pickup_address,
                destination_address=order.destination_address,
                weight_grams=order.weight_grams,
                status=order.status.value,
                version=OrderRow.version + 1,
            )
            .returning(OrderRow.version)
        )
        persisted_version = result.scalar_one_or_none()
        if persisted_version is None:
            raise ConcurrentUpdate("optimistic version conflict")
        if persisted_version != order.version:
            raise RuntimeError("persisted version differs from domain state")
        if order.courier_id is not None:
            active_assignment = await self.session.scalar(
                select(AssignmentRow.id).where(
                    AssignmentRow.order_id == order.id,
                    AssignmentRow.active.is_(True),
                ).limit(1)
            )
            if active_assignment is None:
                self.session.add(AssignmentRow(
                    order_id=order.id, courier_id=order.courier_id,
                ))
        await self.session.flush()

    async def save_transition(
        self,
        order: DeliveryOrder,
        *,
        expected_version: int,
        event: OrderStatusChanged,
    ) -> None:
        from delivery_service.db import OutboxRow, TransitionHistoryRow

        if event.order_id != order.id or event.order_version != order.version:
            raise ValueError("transition event does not describe the persisted order")
        await self.save(order, expected_version=expected_version)
        self.session.add(TransitionHistoryRow(
            event_id=event.event_id,
            tenant_id=event.tenant_id,
            order_id=event.order_id,
            from_status=event.from_status.value,
            to_status=event.to_status.value,
            order_version=event.order_version,
            actor_id=event.actor_id,
            actor_role=event.actor_role,
            occurred_at=event.occurred_at,
            correlation_id=event.correlation_id,
        ))
        self.session.add(OutboxRow(
            id=event.event_id,
            event_type="order.status_changed",
            payload=event.integration_payload(),
            created_at=event.occurred_at,
        ))
        await self.session.flush()

    async def claim_next_created_order(
        self, *, courier_id: UUID, tenant_id: UUID = DEFAULT_TENANT_ID,
        occurred_at: datetime | None = None, correlation_id: str = "queue-claim",
    ) -> DeliveryOrder | None:
        """Claim one queue row; the caller owns commit or rollback.

        SKIP LOCKED coordinates independent worker processes through
        PostgreSQL.  Changing status and creating the assignment before the
        outer commit makes the claim durable when the row lock is released.
        """
        from sqlalchemy import select
        from delivery_service.db import (
            AssignmentRow, OrderRow, OutboxRow, TransitionHistoryRow,
        )
        from delivery_service.domain import DeliveryOrder, OrderStatus

        row = await self.session.scalar(
            select(OrderRow)
            .where(
                OrderRow.tenant_id == tenant_id,
                OrderRow.status == OrderStatus.CREATED.value,
            )
            .order_by(OrderRow.created_at, OrderRow.id)
            .limit(1)
            .with_for_update(skip_locked=True, of=OrderRow)
        )
        if row is None:
            return None

        before = DeliveryOrder(
            id=row.id, tenant_id=row.tenant_id, customer_id=row.customer_id,
            pickup_address=row.pickup_address,
            destination_address=row.destination_address,
            weight_grams=row.weight_grams, status=OrderStatus.CREATED,
            courier_id=None, version=row.version, created_at=row.created_at,
        )
        claimed = before.assign(courier_id)
        event = OrderStatusChanged.from_transition(
            before=before, after=claimed, actor_id=courier_id,
            actor_role="courier", occurred_at=occurred_at or datetime.now(UTC),
            correlation_id=correlation_id,
        )
        row.status = claimed.status.value
        row.version = claimed.version
        self.session.add(AssignmentRow(
            order_id=row.id,
            courier_id=courier_id,
            active=True,
        ))
        self.session.add(TransitionHistoryRow(
            event_id=event.event_id, tenant_id=event.tenant_id,
            order_id=event.order_id, from_status=event.from_status.value,
            to_status=event.to_status.value, order_version=event.order_version,
            actor_id=event.actor_id, actor_role=event.actor_role,
            occurred_at=event.occurred_at, correlation_id=event.correlation_id,
        ))
        self.session.add(OutboxRow(
            id=event.event_id, event_type="order.status_changed",
            payload=event.integration_payload(), created_at=event.occurred_at,
        ))
        try:
            await self.session.flush()
        except IntegrityError as error:
            original = error.orig
            cause = getattr(original, "__cause__", None)
            constraint = (
                getattr(original, "constraint_name", None)
                or getattr(cause, "constraint_name", None)
                or getattr(getattr(original, "diag", None), "constraint_name", None)
            )
            if constraint == "uq_active_assignment_courier":
                raise CourierAlreadyBusy(
                    "courier already has an active assignment"
                ) from error
            raise
        return claimed

    async def idempotent_order(
        self, scope: str, request_fingerprint: str,
    ) -> DeliveryOrder | None:
        from sqlalchemy import select, text
        from delivery_service.db import IdempotencyRow

        # Serialize only commands with the same key for the duration of the
        # surrounding transaction.  A concurrent retry waits, then observes
        # the record committed by the first request instead of creating a
        # second order or failing on the unique constraint.
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
            {"scope": scope},
        )
        record = await self.session.scalar(
            select(IdempotencyRow).where(IdempotencyRow.key == scope)
        )
        if record is not None and record.response.get("request_fingerprint") != request_fingerprint:
            raise IdempotencyConflict("idempotency key was already used for another request")
        if record is None:
            return None
        response = record.response.get("response")
        if not isinstance(response, dict):
            raise RuntimeError("idempotency record has no response snapshot")
        return self._domain_from_snapshot(response)

    async def remember_idempotency(
        self, scope: str, request_fingerprint: str, order: DeliveryOrder,
    ) -> None:
        from datetime import UTC, datetime
        from delivery_service.db import IdempotencyRow

        self.session.add(IdempotencyRow(
            key=scope,
            operation="create_order",
            resource_id=order.id,
            response={
                "request_fingerprint": request_fingerprint,
                "status_code": 201,
                "response": self._response_snapshot(order),
            },
            created_at=datetime.now(UTC),
        ))
        await self.session.flush()

    async def record_created_event(self, event: OrderCreated) -> None:
        from delivery_service.db import OutboxRow

        self.session.add(OutboxRow(
            id=event.event_id,
            event_type="order.created",
            payload=event.integration_payload(),
            created_at=event.occurred_at,
        ))
        await self.session.flush()
