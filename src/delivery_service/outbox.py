from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    id: UUID
    event_type: str
    payload: dict[str, object]


class EventPublisher(Protocol):
    """Transport port: return only after the broker confirms publication."""

    async def publish(self, event: OutboxEvent) -> None: ...


def transitional_outbox_pending_clause():
    """Keep the new relay compatible with the pre-rename rollback image."""
    from delivery_service.db import OutboxRow

    return and_(
        OutboxRow.published_at.is_(None),
        OutboxRow.legacy_processed_at.is_(None),
    )


def mark_outbox_published(row, moment: datetime) -> None:
    """Dual-write until the previous image leaves the rollback window."""
    row.published_at = moment
    row.legacy_processed_at = moment


async def publish_pending_outbox(
    session: AsyncSession,
    publisher: EventPublisher,
    *,
    published_at: datetime,
    batch_size: int = 50,
) -> list[UUID]:
    """Publish one bounded, locked batch and mark only confirmed messages.

    The caller owns the transaction. Holding row locks during broker I/O is a
    deliberate, simple course implementation: it avoids concurrent publication
    of the same row, while ``SKIP LOCKED`` lets another relay claim other rows.
    A crash after broker confirmation but before commit can still redeliver the
    message, so consumers must remain idempotent.
    """

    from delivery_service.db import OutboxRow

    if not session.in_transaction():
        raise RuntimeError("publish_pending_outbox requires an active transaction")
    if not 1 <= batch_size <= 200:
        raise ValueError("batch_size must be between 1 and 200")

    rows = list((await session.scalars(
        select(OutboxRow)
        .where(transitional_outbox_pending_clause())
        .order_by(OutboxRow.created_at, OutboxRow.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True, of=OutboxRow)
    )).all())

    published_ids: list[UUID] = []
    for row in rows:
        await publisher.publish(OutboxEvent(row.id, row.event_type, row.payload))
        mark_outbox_published(row, published_at)
        published_ids.append(row.id)
    await session.flush()
    return published_ids


class InMemoryOutboxConsumerFake:
    """A unit-test fake; production deduplication lives in PostgreSQL below."""
    def __init__(self) -> None:
        self.processed: set[UUID] = set()

    async def handle(self, event: OutboxEvent) -> bool:
        if event.id in self.processed:
            return False
        await self.apply(event)
        self.processed.add(event.id)
        return True

    async def apply(self, event: OutboxEvent) -> None:
        """Adapter boundary for a notification or another durable side effect."""
        if not event.event_type:
            raise ValueError("event_type is required")


class WebhookEventConflict(RuntimeError):
    """One provider event ID was reused with a different signed payload."""


async def accept_payment_webhook_once(
    session: AsyncSession,
    *,
    event_id: UUID,
    event_payload: dict[str, object],
) -> bool:
    """Persist an incoming event claim and outgoing work in one transaction."""

    from delivery_service.db import ConsumerInboxRow, OutboxRow

    claimed = await session.scalar(
        insert(ConsumerInboxRow)
        .values(
            event_id=event_id,
            event_type="payment.webhook",
            processed_at=datetime.now(UTC),
        )
        .on_conflict_do_nothing(index_elements=[ConsumerInboxRow.event_id])
        .returning(ConsumerInboxRow.event_id)
    )
    if claimed is None:
        existing = await session.get(OutboxRow, event_id)
        if (
            existing is None
            or existing.event_type != "payment.webhook.accepted"
            or existing.payload != event_payload
        ):
            raise WebhookEventConflict("event ID was reused with another payload")
        return False

    session.add(OutboxRow(
        id=event_id,
        event_type="payment.webhook.accepted",
        payload=event_payload,
    ))
    await session.flush()
    return True


async def consume_order_created_once(
    session: AsyncSession,
    event: OutboxEvent,
) -> bool:
    """Atomically claim one event and create its durable local effect.

    The surrounding transaction owns commit/rollback.  PostgreSQL's primary
    key on consumer_inbox.event_id is the concurrency guard; process memory is
    deliberately not part of the guarantee.
    """
    from delivery_service.db import ConsumerInboxRow, NotificationRow

    claimed = await session.scalar(
        insert(ConsumerInboxRow)
        .values(
            event_id=event.id,
            event_type=event.event_type,
            processed_at=datetime.now(UTC),
        )
        .on_conflict_do_nothing(index_elements=[ConsumerInboxRow.event_id])
        .returning(ConsumerInboxRow.event_id)
    )
    if claimed is None:
        return False

    if event.event_type != "order.created":
        raise ValueError("unsupported outbox event")
    order_id = UUID(str(event.payload["order_id"]))
    session.add(NotificationRow(
        event_id=event.id,
        order_id=order_id,
        message=f"Order {order_id} was created",
    ))
    await session.flush()
    return True
