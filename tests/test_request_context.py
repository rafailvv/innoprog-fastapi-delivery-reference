from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from delivery_service.main import app
from delivery_service.observability import (
    REQUEST_ID_PATTERN,
    TelemetryMiddleware,
    correlation_id,
    current_correlation_id,
    inject_request_context,
    logger,
)


@pytest.mark.asyncio
async def test_concurrent_requests_keep_distinct_context_and_logs(caplog) -> None:
    caplog.set_level(logging.INFO, logger="delivery_service")
    logger.addHandler(caplog.handler)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            first, second = await asyncio.gather(
                client.get("/health/live", headers={"X-Request-ID": "parallel-a"}),
                client.get("/health/live", headers={"X-Request-ID": "parallel-b"}),
            )
    finally:
        logger.removeHandler(caplog.handler)

    assert {first.headers["X-Request-ID"], second.headers["X-Request-ID"]} == {
        "parallel-a",
        "parallel-b",
    }
    completed_ids = {
        getattr(record, "correlation_id", None)
        for record in caplog.records
        if record.message == "request_completed"
    }
    assert {"parallel-a", "parallel-b"} <= completed_ids
    assert current_correlation_id() == ""


@pytest.mark.asyncio
async def test_untrusted_request_id_is_bounded_before_logging() -> None:
    invalid = "x" * 200
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.get("/health/live", headers={"X-Request-ID": invalid})

    generated = response.headers["X-Request-ID"]
    assert generated != invalid
    assert REQUEST_ID_PATTERN.fullmatch(generated)


@pytest.mark.asyncio
async def test_httpx_hook_forwards_current_id_without_overwriting_explicit_header() -> None:
    seen: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["X-Request-ID"])
        return httpx.Response(200, json={"ok": True})

    token = correlation_id.set("outgoing-21")
    try:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            event_hooks={"request": [inject_request_context]},
        ) as client:
            await client.get("https://routes.example/estimate")
            await client.get(
                "https://routes.example/estimate",
                headers={"X-Request-ID": "explicit-adapter-id"},
            )
    finally:
        correlation_id.reset(token)

    assert seen == ["outgoing-21", "explicit-adapter-id"]
    assert current_correlation_id() == ""


@pytest.mark.asyncio
async def test_request_context_is_reset_when_application_raises() -> None:
    async def failing_app(_scope, _receive, _send) -> None:
        assert current_correlation_id() == "failure-61"
        raise RuntimeError("simulated application failure")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=TelemetryMiddleware(failing_app)),
        base_url="http://testserver",
    ) as client:
        with pytest.raises(RuntimeError, match="simulated"):
            await client.get("/failure", headers={"X-Request-ID": "failure-61"})

    assert current_correlation_id() == ""
