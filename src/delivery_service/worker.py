"""Celery entry points: small messages, late acknowledgement and durable deduplication."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

from celery import Celery
from delivery_service.config import get_settings
from delivery_service.db import OutboxRow, transactional_session
from delivery_service.notifications import (
    InMemoryIdempotentNotificationProvider,
    dispatch_order_created_notification_once,
)
from delivery_service.outbox import OutboxEvent
from delivery_service.observability import configure_json_logging, log_event


configure_json_logging()
settings = get_settings()
notification_provider = InMemoryIdempotentNotificationProvider()

# Redis is the transport, not the source of business truth.  Task results are
# intentionally disabled: PostgreSQL owns notification and order state.
celery_app = Celery("delivery", broker=settings.celery_broker_url)
celery_app.conf.update(
    accept_content=["json"],
    task_serializer="json",
    result_serializer="json",
    task_ignore_result=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_default_queue="notifications",
    task_routes={"delivery.send_notification": {"queue": "notifications"}},
    broker_transport_options={
        "visibility_timeout": settings.celery_visibility_timeout_seconds,
    },
    task_soft_time_limit=settings.celery_soft_time_limit_seconds,
    task_time_limit=settings.celery_hard_time_limit_seconds,
    enable_utc=True,
    timezone="UTC",
)


class TransientNotificationError(RuntimeError):
    """The same idempotent event may succeed when attempted later."""


class PermanentNotificationError(RuntimeError):
    """The message is malformed or references an impossible durable event."""


async def process_notification_event(event_id: UUID) -> bool:
    """Apply one outbox event exactly once in local PostgreSQL state.

    The inbox primary key and the notification unique key are the concurrency
    boundary.  The surrounding transaction commits before Celery acknowledges
    the message.  A crash after commit and before ACK therefore causes a safe
    duplicate delivery whose result is ``False``.
    """

    async with transactional_session() as session:
        row = await session.get(OutboxRow, event_id)
        if row is None:
            raise PermanentNotificationError("outbox event does not exist")
        event = OutboxEvent(id=row.id, event_type=row.event_type, payload=row.payload)
        return await dispatch_order_created_notification_once(
            session,
            event_id=event.id,
            event_type=event.event_type,
            payload=event.payload,
            provider=notification_provider,
            now=datetime.now(UTC),
        )


def _run_notification_job(event_id: str) -> bool:
    try:
        parsed = UUID(event_id)
    except ValueError as exc:
        raise PermanentNotificationError("event_id must be a UUID") from exc
    # Celery executes this synchronous task in a worker child process, where
    # this function owns the event loop for the duration of one job.
    return asyncio.run(process_notification_event(parsed))


@celery_app.task(
    bind=True,
    name="delivery.send_notification",
    acks_late=True,
    reject_on_worker_lost=True,
    ignore_result=True,
    autoretry_for=(TransientNotificationError,),
    dont_autoretry_for=(PermanentNotificationError,),
    retry_backoff=2,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
)
def send_notification(self, event_id: str) -> None:
    """Consume a stable outbox ID; never serialize ORM objects or secrets."""

    applied = _run_notification_job(event_id)
    log_event(
        "notification_processed",
        event_id=event_id,
        redelivered=bool((self.request.delivery_info or {}).get("redelivered")),
        applied=applied,
    )
