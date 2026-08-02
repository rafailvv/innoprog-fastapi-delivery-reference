from __future__ import annotations

import json
import logging
import sys

import pytest

from delivery_service.observability import (
    SafeJsonFormatter,
    configure_json_logging,
    log_event,
    logger,
)


def _record(*, event: str = "request_completed") -> logging.LogRecord:
    return logging.LogRecord(
        name="delivery_service",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=event,
        args=(),
        exc_info=None,
    )


def test_json_formatter_keeps_typed_fields_and_drops_unknown_secrets() -> None:
    record = _record()
    record.correlation_id = "request-61"
    record.method = "POST"
    record.route = "/api/v1/orders/{order_id}/proofs"
    record.status = 503
    record.duration_ms = 184.25
    record.error_kind = "storage_timeout"
    record.authorization = "Bearer do-not-log"
    record.password = "do-not-log"
    record.signed_url = "https://objects.example/proof?signature=do-not-log"

    rendered = SafeJsonFormatter().format(record)
    payload = json.loads(rendered)

    assert "\n" not in rendered
    assert payload["event"] == "request_completed"
    assert payload["status"] == 503
    assert payload["duration_ms"] == 184.25
    assert payload["route"] == "/api/v1/orders/{order_id}/proofs"
    assert "authorization" not in payload
    assert "password" not in payload
    assert "signed_url" not in payload
    assert "do-not-log" not in rendered


def test_exception_formatter_exposes_type_but_not_secret_message() -> None:
    try:
        raise RuntimeError("presigned signature=must-not-leak")
    except RuntimeError:
        record = _record(event="unhandled_request_error")
        record.exc_info = sys.exc_info()

    rendered = SafeJsonFormatter().format(record)
    payload = json.loads(rendered)

    assert payload["error_type"] == "RuntimeError"
    assert "must-not-leak" not in rendered


def test_log_event_rejects_unknown_fields_before_logging() -> None:
    with pytest.raises(ValueError, match="unsafe or unknown"):
        log_event(
            "proof_upload_failed",
            correlation_id="request-61",
            signed_url="https://objects.example/proof?signature=secret",
        )


def test_json_logging_configuration_is_idempotent_and_does_not_duplicate_to_root() -> None:
    first = configure_json_logging()
    second = configure_json_logging()

    assert first is second
    assert logger.propagate is False
    assert sum(getattr(handler, "delivery_json", False) for handler in logger.handlers) == 1


@pytest.mark.parametrize("event", ["User logged in", "line\nbreak", "x" * 65])
def test_log_event_rejects_unstable_or_injectable_event_names(event: str) -> None:
    with pytest.raises(ValueError, match="bounded lower-case"):
        log_event(event)
