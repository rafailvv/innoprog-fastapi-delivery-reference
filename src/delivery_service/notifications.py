"""Idempotent notification dispatch driven by a durable outbox event."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from delivery_service.db import (
    ConsumerInboxRow,
    NotificationPreferenceRow,
    NotificationRow,
)


@dataclass(frozen=True, slots=True)
class ProviderReceipt:
    message_id: str


class NotificationProvider(Protocol):
    async def send(
        self,
        *,
        recipient: str,
        message: str,
        idempotency_key: str,
    ) -> ProviderReceipt: ...


class InMemoryIdempotentNotificationProvider:
    """Development adapter that models a provider honoring an idempotency key."""

    def __init__(self) -> None:
        self.receipts: dict[str, ProviderReceipt] = {}
        self.attempts: list[str] = []

    async def send(
        self,
        *,
        recipient: str,
        message: str,
        idempotency_key: str,
    ) -> ProviderReceipt:
        del recipient, message
        self.attempts.append(idempotency_key)
        receipt = self.receipts.get(idempotency_key)
        if receipt is None:
            receipt = ProviderReceipt(message_id=f"local-{idempotency_key}")
            self.receipts[idempotency_key] = receipt
        return receipt


async def dispatch_order_created_notification_once(
    session: AsyncSession,
    *,
    event_id: UUID,
    event_type: str,
    payload: dict[str, object],
    provider: NotificationProvider,
    now: datetime,
    channel: str = "email",
) -> bool:
    """Claim, apply preference and record one provider result in one transaction.

    A provider call is not atomic with PostgreSQL.  The stable event ID is sent
    as the provider idempotency key, so a crash after provider acceptance but
    before the database commit can safely repeat the same external command.
    """

    if event_type != "order.created":
        raise ValueError("unsupported notification event")
    order_id = UUID(str(payload.get("aggregate_id") or payload["order_id"]))
    tenant_id = UUID(str(payload["tenant_id"]))
    customer_id = UUID(str(payload["customer_id"]))

    claimed = await session.scalar(
        insert(ConsumerInboxRow)
        .values(event_id=event_id, event_type=event_type, processed_at=now)
        .on_conflict_do_nothing(index_elements=[ConsumerInboxRow.event_id])
        .returning(ConsumerInboxRow.event_id)
    )
    if claimed is None:
        return False

    preference = await session.scalar(
        select(NotificationPreferenceRow).where(
            NotificationPreferenceRow.tenant_id == tenant_id,
            NotificationPreferenceRow.user_id == customer_id,
            NotificationPreferenceRow.channel == channel,
        )
    )
    enabled = preference is None or preference.enabled
    notification = NotificationRow(
        event_id=event_id,
        order_id=order_id,
        message=f"Order {order_id} was created",
        channel=channel,
        status="pending" if enabled else "skipped",
    )
    session.add(notification)
    await session.flush()
    if not enabled:
        return True

    receipt = await provider.send(
        recipient=str(customer_id),
        message=notification.message,
        idempotency_key=str(event_id),
    )
    notification.status = "sent"
    notification.provider_message_id = receipt.message_id
    notification.sent_at = now
    await session.flush()
    return True
