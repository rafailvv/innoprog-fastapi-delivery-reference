from __future__ import annotations

from uuid import uuid4

import pytest

from delivery_service.worker import (
    PermanentNotificationError,
    celery_app,
    send_notification,
)


def test_celery_uses_json_late_ack_bounded_prefetch_and_no_result_backend() -> None:
    assert celery_app.conf.task_serializer == "json"
    assert celery_app.conf.accept_content == ["json"]
    assert celery_app.conf.task_acks_late is True
    assert celery_app.conf.task_reject_on_worker_lost is True
    assert celery_app.conf.worker_prefetch_multiplier == 1
    assert celery_app.conf.task_ignore_result is True
    assert celery_app.backend.as_uri() == "disabled://"
    assert (
        celery_app.conf.broker_transport_options["visibility_timeout"]
        > celery_app.conf.task_time_limit
    )


def test_task_message_contains_only_stable_event_id(monkeypatch) -> None:
    event_id = str(uuid4())
    calls: list[str] = []
    monkeypatch.setattr(
        "delivery_service.worker._run_notification_job",
        lambda value: calls.append(value) or True,
    )

    send_notification.run(event_id)

    assert calls == [event_id]


def test_duplicate_delivery_reuses_same_durable_event_id(monkeypatch) -> None:
    event_id = str(uuid4())
    observed: set[str] = set()
    effects = 0

    def fake_job(value: str) -> bool:
        nonlocal effects
        if value in observed:
            return False
        observed.add(value)
        effects += 1
        return True

    monkeypatch.setattr("delivery_service.worker._run_notification_job", fake_job)
    send_notification.run(event_id)
    send_notification.run(event_id)
    assert effects == 1


def test_malformed_event_id_is_permanent_and_not_retried() -> None:
    with pytest.raises(PermanentNotificationError, match="UUID"):
        send_notification.run("not-an-event-id")
