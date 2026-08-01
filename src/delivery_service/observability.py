from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
import json
import logging
import re
import sys
from time import perf_counter
from typing import Final, TextIO
from uuid import UUID, uuid4

import httpx
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import Span, SpanKind, Status, StatusCode
from prometheus_client import Counter, Gauge, Histogram
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from delivery_service.config import get_settings


SERVICE_LEVEL_OBJECTIVE = 0.995
LATENCY_P95_SECONDS = 0.5
REQUEST_DURATION_BUCKETS: Final = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)
SLO_OPERATIONS: Final = {
    ("POST", "/api/v1/orders"): "create_order",
    ("GET", "/api/v1/orders/{order_id}"): "get_order",
    ("GET", "/api/v1/orders/{order_id}/tracking/events"): "track_order",
}
METRIC_METHODS: Final = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"})
REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
EVENT_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_.]{0,63}\Z")
TOKEN_FIELD_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
SAFE_LOG_FIELDS: Final = frozenset({
    "correlation_id",
    "trace_id",
    "method",
    "route",
    "status",
    "duration_ms",
    "outcome",
    "error_kind",
    "order_id",
    "event_id",
    "attempt",
    "retryable",
    "redelivered",
    "applied",
})
SAFE_SPAN_ATTRIBUTES: Final = frozenset({
    "http.request.method",
    "http.route",
    "http.response.status_code",
    "delivery.operation",
    "delivery.outcome",
    "db.system",
    "db.operation.name",
    "db.namespace",
    "server.address",
    "cache.operation",
    "messaging.operation.name",
    "error.type",
})


correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")
REQUESTS = Counter(
    "delivery_http_requests_total",
    "Completed HTTP requests by bounded route template and status code.",
    ("method", "route", "status"),
)
LATENCY = Histogram(
    "delivery_http_request_seconds",
    "Completed HTTP request duration in seconds.",
    ("method", "route"),
    buckets=REQUEST_DURATION_BUCKETS,
)
IN_FLIGHT = Gauge(
    "delivery_http_in_flight_requests",
    "HTTP requests currently executing in this process.",
    ("method",),
)
SLO_REQUESTS = Counter(
    "delivery_slo_requests_total",
    "SLO-eligible operations classified by outcome.",
    ("operation", "outcome"),
)
SLO_LATENCY = Histogram(
    "delivery_slo_request_duration_seconds",
    "Duration of SLO-eligible operations in seconds.",
    ("operation",),
    buckets=REQUEST_DURATION_BUCKETS,
)
logger = logging.getLogger("delivery_service")


def build_trace_provider(
    *, sample_ratio: float = 1.0, exporter: SpanExporter | None = None,
) -> TracerProvider:
    """Build the service-owned provider with parent-aware head sampling."""

    provider = TracerProvider(
        resource=Resource.create({
            "service.name": "delivery-service",
            "service.version": "1.0.0",
        }),
        sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
    )
    if exporter is not None:
        provider.add_span_processor(BatchSpanProcessor(exporter))
    return provider


_trace_settings = get_settings()
TRACE_PROVIDER = build_trace_provider(sample_ratio=_trace_settings.otel_sample_ratio)
trace.set_tracer_provider(TRACE_PROVIDER)
tracer = TRACE_PROVIDER.get_tracer("delivery_service", "1.0.0")
_configured_otlp_endpoints: set[str] = set()


def set_safe_span_attributes(span: Span, attributes: Mapping[str, object]) -> None:
    """Write only bounded technical dimensions; business IDs and secrets are rejected."""

    unknown = set(attributes) - SAFE_SPAN_ATTRIBUTES
    if unknown:
        raise ValueError(f"unsafe or unknown span attributes: {', '.join(sorted(unknown))}")
    for name, value in attributes.items():
        if isinstance(value, bool):
            normalized: str | int | float | bool = value
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            normalized = value
        elif isinstance(value, str) and len(value) <= 160 and "\n" not in value:
            normalized = value
        else:
            raise TypeError(f"{name} must be a bounded scalar")
        span.set_attribute(name, normalized)


@contextmanager
def traced_operation(
    name: str,
    *,
    kind: SpanKind = SpanKind.INTERNAL,
    attributes: Mapping[str, object] | None = None,
) -> Iterator[Span]:
    """Create a stable operation span and record failures without swallowing them."""

    with tracer.start_as_current_span(name, kind=kind) as span:
        if attributes:
            set_safe_span_attributes(span, attributes)
        try:
            yield span
        except BaseException as exc:
            span.record_exception(exc)
            set_safe_span_attributes(span, {"error.type": type(exc).__name__})
            span.set_status(Status(StatusCode.ERROR))
            raise


def configure_otlp_exporter(endpoint: str | None) -> None:
    """Attach one batched OTLP exporter when an explicit endpoint is configured."""

    if not endpoint:
        return
    if endpoint in _configured_otlp_endpoints:
        return
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    TRACE_PROVIDER.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    )
    _configured_otlp_endpoints.add(endpoint)


def flush_tracing() -> bool:
    """Flush bounded queues during graceful shutdown without changing request flow."""

    return bool(TRACE_PROVIDER.force_flush(timeout_millis=5_000))


def request_outcome(status_code: int) -> str:
    """Map a bounded HTTP status to the documented SLO population."""

    if status_code >= 500:
        return "server_error"
    if status_code >= 400:
        return "client_error"
    return "success"


def metric_method(value: str) -> str:
    """Keep attacker-controlled extension methods out of metric cardinality."""

    normalized = value.upper()
    return normalized if normalized in METRIC_METHODS else "OTHER"


def _safe_log_value(name: str, value: object) -> str | int | float | bool:
    """Keep the event schema typed, bounded and free from arbitrary objects."""

    if isinstance(value, UUID):
        value = str(value)
    if name in {"status", "attempt"}:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an integer")
        return value
    if name == "duration_ms":
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
            raise TypeError("duration_ms must be a non-negative number")
        return value
    if name in {"retryable", "redelivered", "applied"}:
        if not isinstance(value, bool):
            raise TypeError(f"{name} must be a boolean")
        return value
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if name == "route":
        if not value.startswith("/") or "?" in value or len(value) > 256:
            raise ValueError("route must be a bounded route template without query")
        return value
    if not TOKEN_FIELD_PATTERN.fullmatch(value):
        raise ValueError(f"{name} contains unsafe or unbounded characters")
    return value


class SafeJsonFormatter(logging.Formatter):
    """Serialize only the explicit delivery-service event schema as JSONL."""

    def format(self, record: logging.LogRecord) -> str:
        raw_event = record.getMessage()
        event = raw_event if EVENT_NAME_PATTERN.fullmatch(raw_event) else "invalid_log_event"
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "service": "delivery-service",
            "event": event,
        }
        for name in SAFE_LOG_FIELDS:
            value = getattr(record, name, None)
            if value is not None:
                payload[name] = _safe_log_value(name, value)
        if record.exc_info:
            payload["error_type"] = record.exc_info[0].__name__
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_json_logging(stream: TextIO | None = None) -> logging.Handler:
    """Install one tagged JSON handler without deleting host logging handlers."""

    existing = next(
        (handler for handler in logger.handlers if getattr(handler, "delivery_json", False)),
        None,
    )
    if existing is not None:
        return existing
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(SafeJsonFormatter())
    handler.delivery_json = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    # Uvicorn/root handlers often render human-readable text.  Propagation
    # would emit the same event twice in two incompatible formats.
    logger.propagate = False
    return handler


def log_event(
    event: str,
    *,
    level: int = logging.INFO,
    exc_info: bool | BaseException | tuple = False,
    **fields: object,
) -> None:
    """Emit one schema-checked event; unknown fields never reach a log sink."""

    if not EVENT_NAME_PATTERN.fullmatch(event):
        raise ValueError("event must be a bounded lower-case identifier")
    unknown = set(fields) - SAFE_LOG_FIELDS
    if unknown:
        raise ValueError(f"unsafe or unknown log fields: {', '.join(sorted(unknown))}")
    if "correlation_id" not in fields and current_correlation_id():
        fields["correlation_id"] = current_correlation_id()
    normalized = {name: _safe_log_value(name, value) for name, value in fields.items()}
    logger.log(level, event, extra=normalized, exc_info=exc_info)


def select_request_id(incoming: str | None) -> str:
    """Preserve a small safe upstream ID, otherwise create an opaque local one."""

    if incoming and REQUEST_ID_PATTERN.fullmatch(incoming):
        return incoming
    return str(uuid4())


def current_correlation_id() -> str:
    return correlation_id.get()


async def inject_request_context(request: httpx.Request) -> None:
    """Forward correlation and the active W3C trace context to an HTTP dependency."""

    request_id = current_correlation_id()
    if request_id and "X-Request-ID" not in request.headers:
        request.headers["X-Request-ID"] = request_id
    inject(request.headers)


class TelemetryMiddleware:
    """Pure-ASGI request context, response metadata and bounded RED metrics."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_headers = Headers(scope=scope)
        request_id = select_request_id(request_headers.get("X-Request-ID"))
        scope.setdefault("state", {})["correlation_id"] = request_id
        token = correlation_id.set(request_id)
        parent_context = extract(dict(request_headers.items()))
        started = perf_counter()
        status_code = 500
        method = metric_method(scope["method"])
        in_flight = IN_FLIGHT.labels(method)
        in_flight.inc()

        with tracer.start_as_current_span(
            "HTTP request", context=parent_context, kind=SpanKind.SERVER,
        ) as server_span:
            async def send_with_context(message: Message) -> None:
                nonlocal status_code
                if message["type"] == "http.response.start":
                    status_code = int(message["status"])
                    headers = MutableHeaders(scope=message)
                    headers["X-Request-ID"] = request_id
                    carrier: dict[str, str] = {}
                    inject(carrier)
                    for header_name in ("traceparent", "tracestate"):
                        if value := carrier.get(header_name):
                            headers[header_name] = value
                await send(message)

            try:
                await self.app(scope, receive, send_with_context)
            except BaseException as exc:
                server_span.record_exception(exc)
                set_safe_span_attributes(server_span, {"error.type": type(exc).__name__})
                server_span.set_status(Status(StatusCode.ERROR))
                raise
            finally:
                route = scope.get("route")
                route_path = getattr(route, "path", "/unmatched")
                server_span.update_name(f"{method} {route_path}")
                set_safe_span_attributes(server_span, {
                    "http.request.method": method,
                    "http.route": route_path,
                    "http.response.status_code": status_code,
                    "delivery.outcome": request_outcome(status_code),
                })
                if status_code >= 500:
                    server_span.set_status(Status(StatusCode.ERROR))
                duration = perf_counter() - started
                try:
                    REQUESTS.labels(method, route_path, str(status_code)).inc()
                    LATENCY.labels(method, route_path).observe(duration)
                    operation = SLO_OPERATIONS.get((method, route_path))
                    if operation is not None:
                        SLO_REQUESTS.labels(operation, request_outcome(status_code)).inc()
                        SLO_LATENCY.labels(operation).observe(duration)
                    log_event(
                        "request_completed",
                        correlation_id=request_id,
                        method=method,
                        route=route_path,
                        status=status_code,
                        duration_ms=round(duration * 1000, 3),
                    )
                finally:
                    in_flight.dec()
                    correlation_id.reset(token)
