"""Untrusted payment-webhook envelope and development inbox implementation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from delivery_service.outbox import WebhookEventConflict


class PaymentData(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    order_id: UUID
    payment_id: str = Field(min_length=1, max_length=128)


class PaymentWebhookEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    id: UUID
    type: Literal["payment.completed"]
    data: PaymentData

    def durable_payload(self, raw_body: bytes) -> dict[str, object]:
        return {
            "event_id": str(self.id),
            "event_type": self.type,
            "order_id": str(self.data.order_id),
            "payment_id": self.data.payment_id,
            "payload_sha256": sha256(raw_body).hexdigest(),
        }


def timestamp_is_fresh(
    value: str,
    *,
    tolerance_seconds: int,
    now: datetime | None = None,
) -> bool:
    try:
        timestamp = int(value)
    except ValueError:
        return False
    current = now or datetime.now(UTC)
    return abs(current.timestamp() - timestamp) <= tolerance_seconds


class InMemoryWebhookInbox:
    """Development fake; production uses PostgreSQL inbox + outbox rows."""

    def __init__(self) -> None:
        self._records: dict[UUID, dict[str, object]] = {}
        self._lock = asyncio.Lock()

    async def accept(self, event_id: UUID, payload: dict[str, object]) -> bool:
        async with self._lock:
            existing = self._records.get(event_id)
            if existing is None:
                self._records[event_id] = dict(payload)
                return True
            if existing != payload:
                raise WebhookEventConflict("event ID was reused with another payload")
            return False
