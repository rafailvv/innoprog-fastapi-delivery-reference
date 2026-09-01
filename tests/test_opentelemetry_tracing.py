from __future__ import annotations

import httpx
from opentelemetry import trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
import pytest

from delivery_service.integrations import RouteQuery, RouteServiceAdapter
from delivery_service.main import app
from delivery_service.observability import (
    TRACE_PROVIDER,
    inject_request_context,
    set_safe_span_attributes,
    tracer,
)


EXPORTER = InMemorySpanExporter()
TRACE_PROVIDER.add_span_processor(SimpleSpanProcessor(EXPORTER))


@pytest.fixture(autouse=True)
def clear_finished_spans():
    EXPORTER.clear()
    yield
    EXPORTER.clear()


@pytest.mark.asyncio
async def test_server_span_continues_w3c_parent_and_uses_route_template() -> None:
    trace_id = "0123456789abcdef0123456789abcdef"
    caller_span_id = "0123456789abcdef"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver",
    ) as client:
        response = await client.get(
            "/health/live",
            headers={"traceparent": f"00-{trace_id}-{caller_span_id}-01"},
        )

    returned = response.headers["traceparent"].split("-")
    server = next(span for span in EXPORTER.get_finished_spans() if span.kind is SpanKind.SERVER)
    assert response.status_code == 200
    assert f"{server.context.trace_id:032x}" == trace_id
    assert f"{server.parent.span_id:016x}" == caller_span_id
    assert returned[1] == trace_id
    assert returned[2] == f"{server.context.span_id:016x}"
    assert server.name == "GET /health/live"
    assert server.attributes["http.route"] == "/health/live"
    assert "order_id" not in server.attributes


@pytest.mark.asyncio
async def test_invalid_traceparent_starts_new_valid_root() -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver",
    ) as client:
        response = await client.get(
            "/health/live", headers={"traceparent": "00-not-a-trace"},
        )

    parts = response.headers["traceparent"].split("-")
    assert len(parts) == 4
    assert len(parts[1]) == 32 and parts[1] != "0" * 32
    assert len(parts[2]) == 16 and parts[2] != "0" * 16


@pytest.mark.asyncio
async def test_http_adapter_injects_child_context_without_business_identifiers() -> None:
    captured_traceparent = ""

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_traceparent
        captured_traceparent = request.headers["traceparent"]
        return httpx.Response(
            200,
            request=request,
            json={"route_id": "provider-1", "distance_meters": 1200, "duration_seconds": 300},
        )

    async with httpx.AsyncClient(
        base_url="https://routes.example",
        transport=httpx.MockTransport(handler),
        event_hooks={"request": [inject_request_context]},
    ) as client:
        with tracer.start_as_current_span("delivery estimate") as parent:
            estimate = await RouteServiceAdapter(client).estimate(
                RouteQuery(origin="A", destination="B")
            )
            parent_trace_id = parent.get_span_context().trace_id
            parent_span_id = parent.get_span_context().span_id

    client_span = next(
        span for span in EXPORTER.get_finished_spans()
        if span.name == "route-service GET /routes/estimate"
    )
    assert estimate.distance_m == 1200
    assert client_span.kind is SpanKind.CLIENT
    assert client_span.context.trace_id == parent_trace_id
    assert client_span.parent.span_id == parent_span_id
    assert captured_traceparent.split("-")[1] == f"{parent_trace_id:032x}"
    assert set(client_span.attributes) <= {
        "delivery.operation", "http.request.method", "server.address",
    }


def test_span_attribute_allowlist_rejects_secrets_and_high_cardinality_ids() -> None:
    span = trace.INVALID_SPAN
    for forbidden in ("authorization", "http.request.header.cookie", "delivery.order_id"):
        with pytest.raises(ValueError, match="unsafe or unknown"):
            set_safe_span_attributes(span, {forbidden: "secret-or-id"})
