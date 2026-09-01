from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Protocol
from uuid import UUID


@dataclass(frozen=True, slots=True)
class TrackingEvent:
    order_id: UUID
    status: str
    sequence: int
    latitude: float | None = None
    longitude: float | None = None
    recorded_at: datetime | None = None

    def payload(self) -> dict[str, object]:
        return {
            "order_id": str(self.order_id),
            "status": self.status,
            "sequence": self.sequence,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "recorded_at": self.recorded_at.isoformat() if self.recorded_at else None,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> "TrackingEvent":
        recorded_at = payload.get("recorded_at")
        return cls(
            order_id=UUID(str(payload["order_id"])),
            status=str(payload["status"]),
            sequence=int(payload["sequence"]),
            latitude=float(payload["latitude"]) if payload.get("latitude") is not None else None,
            longitude=float(payload["longitude"]) if payload.get("longitude") is not None else None,
            recorded_at=datetime.fromisoformat(str(recorded_at)) if recorded_at else None,
        )


@dataclass(frozen=True, slots=True)
class TrackingSnapshot:
    order_id: UUID
    tenant_id: UUID
    status: str
    sequence: int
    latitude: float
    longitude: float
    recorded_at: datetime
    client_event_id: UUID

    def event(self) -> TrackingEvent:
        return TrackingEvent(
            order_id=self.order_id,
            status=self.status,
            sequence=self.sequence,
            latitude=self.latitude,
            longitude=self.longitude,
            recorded_at=self.recorded_at,
        )


class TrackingSnapshotRepository(Protocol):
    async def get(self, order_id: UUID) -> TrackingSnapshot | None: ...

    async def record(
        self, *, order_id: UUID, tenant_id: UUID, status: str,
        latitude: float, longitude: float, client_event_id: UUID,
        recorded_at: datetime,
    ) -> tuple[TrackingSnapshot, bool]: ...


class InMemoryTrackingSnapshotRepository:
    """Deterministic test adapter with the same idempotency contract as PostgreSQL."""

    def __init__(self) -> None:
        self._snapshots: dict[UUID, TrackingSnapshot] = {}
        self._event_orders: dict[UUID, UUID] = {}
        self._lock = asyncio.Lock()
        self.outbox: list[dict[str, object]] = []

    async def get(self, order_id: UUID) -> TrackingSnapshot | None:
        return self._snapshots.get(order_id)

    async def record(
        self, *, order_id: UUID, tenant_id: UUID, status: str,
        latitude: float, longitude: float, client_event_id: UUID,
        recorded_at: datetime,
    ) -> tuple[TrackingSnapshot, bool]:
        async with self._lock:
            previous_order = self._event_orders.get(client_event_id)
            if previous_order is not None and previous_order != order_id:
                raise ValueError("client_event_id belongs to another order")
            current = self._snapshots.get(order_id)
            if current is not None and current.client_event_id == client_event_id:
                return current, False
            snapshot = TrackingSnapshot(
                order_id=order_id,
                tenant_id=tenant_id,
                status=status,
                sequence=1 if current is None else current.sequence + 1,
                latitude=latitude,
                longitude=longitude,
                recorded_at=recorded_at,
                client_event_id=client_event_id,
            )
            self._snapshots[order_id] = snapshot
            self._event_orders[client_event_id] = order_id
            self.outbox.append({
                "event_id": str(client_event_id),
                "event_type": "tracking.position_updated",
                "payload": snapshot.event().payload(),
            })
            return snapshot, True


class SqlAlchemyTrackingSnapshotRepository:
    def __init__(self, session) -> None:
        self.session = session

    @staticmethod
    def _snapshot(row) -> TrackingSnapshot:
        return TrackingSnapshot(
            order_id=row.order_id,
            tenant_id=row.tenant_id,
            status=row.order_status,
            sequence=row.sequence,
            latitude=row.latitude,
            longitude=row.longitude,
            recorded_at=row.recorded_at,
            client_event_id=row.client_event_id,
        )

    async def get(self, order_id: UUID) -> TrackingSnapshot | None:
        from sqlalchemy import select
        from delivery_service.db import TrackingSnapshotRow

        row = await self.session.scalar(
            select(TrackingSnapshotRow).where(TrackingSnapshotRow.order_id == order_id)
        )
        return self._snapshot(row) if row is not None else None

    async def record(
        self, *, order_id: UUID, tenant_id: UUID, status: str,
        latitude: float, longitude: float, client_event_id: UUID,
        recorded_at: datetime,
    ) -> tuple[TrackingSnapshot, bool]:
        from sqlalchemy import select
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from delivery_service.db import OutboxRow, TrackingSnapshotRow

        statement = pg_insert(TrackingSnapshotRow).values(
            order_id=order_id,
            tenant_id=tenant_id,
            order_status=status,
            latitude=latitude,
            longitude=longitude,
            sequence=1,
            recorded_at=recorded_at,
            client_event_id=client_event_id,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[TrackingSnapshotRow.order_id],
            set_={
                "order_status": statement.excluded.order_status,
                "latitude": statement.excluded.latitude,
                "longitude": statement.excluded.longitude,
                "sequence": TrackingSnapshotRow.sequence + 1,
                "recorded_at": statement.excluded.recorded_at,
                "client_event_id": statement.excluded.client_event_id,
            },
            where=(
                (TrackingSnapshotRow.tenant_id == statement.excluded.tenant_id)
                & (TrackingSnapshotRow.client_event_id != statement.excluded.client_event_id)
            ),
        ).returning(TrackingSnapshotRow)
        row = (await self.session.execute(statement)).scalar_one_or_none()
        if row is None:
            row = await self.session.scalar(
                select(TrackingSnapshotRow).where(
                    TrackingSnapshotRow.order_id == order_id,
                    TrackingSnapshotRow.tenant_id == tenant_id,
                    TrackingSnapshotRow.client_event_id == client_event_id,
                )
            )
            if row is None:
                raise ValueError("tracking event conflicts with tenant or order")
            return self._snapshot(row), False
        snapshot = self._snapshot(row)
        self.session.add(OutboxRow(
            id=client_event_id,
            event_type="tracking.position_updated",
            payload=snapshot.event().payload(),
            created_at=recorded_at,
        ))
        await self.session.flush()
        return snapshot, True


class BackpressurePolicy:
    DROP_OLDEST = "drop_oldest"


class TrackingHub:
    """Bounded per-subscriber queues make slow-consumer behavior explicit."""

    def __init__(self, *, queue_size: int) -> None:
        self.queue_size = queue_size
        self.backpressure_policy = BackpressurePolicy.DROP_OLDEST
        self._subscribers: dict[UUID, set[asyncio.Queue[TrackingEvent]]] = defaultdict(set)
        self.dropped_updates = 0

    def subscribe(self, order_id: UUID) -> asyncio.Queue[TrackingEvent]:
        queue: asyncio.Queue[TrackingEvent] = asyncio.Queue(maxsize=self.queue_size)
        self._subscribers[order_id].add(queue)
        return queue

    def unsubscribe(self, order_id: UUID, queue: asyncio.Queue[TrackingEvent]) -> None:
        subscribers = self._subscribers.get(order_id)
        if subscribers is None:
            return
        subscribers.discard(queue)
        if not subscribers:
            self._subscribers.pop(order_id, None)

    def subscriber_count(self, order_id: UUID) -> int:
        return len(self._subscribers.get(order_id, ()))

    async def publish(self, event: TrackingEvent) -> None:
        for queue in tuple(self._subscribers.get(event.order_id, ())):
            if queue.full():
                queue.get_nowait()
                self.dropped_updates += 1
            queue.put_nowait(event)


def encode_sse(*, event: str, event_id: int, data: dict[str, Any]) -> bytes:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"id: {event_id}\nevent: {event}\ndata: {payload}\n\n".encode("utf-8")


async def order_event_stream(
    *, hub: TrackingHub, snapshot: TrackingEvent, last_sequence: int, follow: bool,
) -> AsyncIterator[bytes]:
    """Subscribe before snapshot delivery so an update cannot fall into a gap."""
    queue = hub.subscribe(snapshot.order_id)
    floor = max(last_sequence, snapshot.sequence)
    try:
        yield encode_sse(event="snapshot", event_id=snapshot.sequence, data=snapshot.payload())
        if not follow:
            return
        while True:
            update = await queue.get()
            if update.sequence <= floor:
                continue
            floor = update.sequence
            yield encode_sse(
                event="tracking-position", event_id=update.sequence, data=update.payload(),
            )
    finally:
        hub.unsubscribe(snapshot.order_id, queue)
