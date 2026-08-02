import asyncio
import hashlib
import hmac
from datetime import UTC, datetime, timedelta
from io import BytesIO
import threading
import time
from uuid import uuid4

import httpx
import pytest
from fastapi import UploadFile

from delivery_service.async_tools import (
    LEGACY_IO_CONCURRENCY,
    gather_bounded,
    run_blocking,
)
from delivery_service.observability import correlation_id
from delivery_service.integrations import (
    CircuitBreaker,
    InvalidRouteResponse,
    RouteQuery,
    RouteRequestRejected,
    RouteServiceAdapter,
    RouteServiceUnavailable,
    verify_webhook_signature,
)
from delivery_service.webhooks import timestamp_is_fresh
from delivery_service.retry_policy import build_route_retrying, run_with_route_retry
from delivery_service.retry_policy import CircuitOpenError, CircuitState
from delivery_service.outbox import InMemoryOutboxConsumerFake, OutboxEvent
from delivery_service.events import OrderCreated
from delivery_service.storage import InvalidUpload, UploadTooLarge, inspect_proof
from delivery_service.tracking import TrackingEvent, TrackingHub


def test_webhook_signature_covers_timestamp_and_raw_body() -> None:
    body = b'{"event":"payment.completed"}'
    timestamp = 1_800_000_000
    secret = b"test-secret"
    timestamp_text = str(timestamp)
    signature = hmac.new(
        secret,
        b"delivery.payment.v1\n" + timestamp_text.encode("ascii") + b"\n" + body,
        hashlib.sha256,
    ).hexdigest()
    assert verify_webhook_signature(body, signature, secret, timestamp=timestamp_text)
    assert not verify_webhook_signature(body + b" ", signature, secret, timestamp=timestamp_text)
    assert not verify_webhook_signature(body, signature, secret, timestamp=str(timestamp + 1))
    assert not verify_webhook_signature(body, "not-hex", secret, timestamp=timestamp_text)


def test_webhook_timestamp_rejects_past_and_future_replay_window() -> None:
    now = datetime(2026, 8, 12, tzinfo=UTC)
    assert timestamp_is_fresh(str(int(now.timestamp())), tolerance_seconds=300, now=now)
    assert not timestamp_is_fresh(
        str(int((now - timedelta(minutes=6)).timestamp())), tolerance_seconds=300, now=now
    )
    assert not timestamp_is_fresh(
        str(int((now + timedelta(minutes=6)).timestamp())), tolerance_seconds=300, now=now
    )
    assert not timestamp_is_fresh("not-a-timestamp", tolerance_seconds=300, now=now)


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_logical_failures_and_recovers() -> None:
    now = 10.0
    breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout=5,
        clock=lambda: now,
    )
    await breaker.record_failure(await breaker.acquire())
    assert breaker.state is CircuitState.CLOSED
    await breaker.record_failure(await breaker.acquire())
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        await breaker.acquire()

    now = 15.0
    probe = await breaker.acquire()
    assert probe.half_open_probe
    assert breaker.state is CircuitState.HALF_OPEN
    with pytest.raises(CircuitOpenError, match="already running"):
        await breaker.acquire()
    await breaker.record_success(probe)
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_failed_or_cancelled_half_open_probe_reopens_circuit() -> None:
    now = 0.0
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=3,
        clock=lambda: now,
    )
    await breaker.record_failure(await breaker.acquire())
    now = 3.0
    probe = await breaker.acquire()
    await breaker.record_failure(probe)
    assert breaker.state is CircuitState.OPEN
    now = 6.0
    cancelled_probe = await breaker.acquire()
    await breaker.abandon(cancelled_probe)
    assert breaker.state is CircuitState.OPEN


@pytest.mark.asyncio
async def test_route_adapter_uses_injected_client_and_validates_response() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        duration = 0 if request.url.params["origin"] == "broken" else 1_080
        return httpx.Response(
            200,
            json={
                "route_id": "provider-route-42",
                "distance_meters": 4_200,
                "duration_seconds": duration,
            },
        )

    async with httpx.AsyncClient(
        base_url="https://routes.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        adapter = RouteServiceAdapter(client)
        estimate = await adapter.estimate(RouteQuery("A", "B"))
        assert estimate.distance_m == 4_200
        assert estimate.duration_s == 1_080
        assert estimate.minutes == 18
        assert estimate.provider_ref == "provider-route-42"
        assert (await adapter.estimate(RouteQuery("C", "D"))).minutes == 18
        with pytest.raises(InvalidRouteResponse):
            await adapter.estimate(RouteQuery("broken", "D"))

    assert len(requests) == 3
    assert client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"route_id": "route-1", "distance_meters": "4200", "duration_seconds": 900},
        {"route_id": "", "distance_meters": 4200, "duration_seconds": 900},
        {
            "route_id": "route-1",
            "distance_meters": 4200,
            "duration_seconds": 900,
            "unexpected": True,
        },
    ],
)
async def test_route_adapter_rejects_schema_drift_without_coercion(payload) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(
        base_url="https://routes.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(InvalidRouteResponse):
            await RouteServiceAdapter(client).estimate(RouteQuery("A", "B"))


@pytest.mark.asyncio
async def test_route_adapter_classifies_provider_status_without_leaking_body() -> None:
    status = 404

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="secret provider diagnostics")

    async with httpx.AsyncClient(
        base_url="https://routes.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        adapter = RouteServiceAdapter(client)
        with pytest.raises(RouteRequestRejected, match="cannot be calculated") as rejected:
            await adapter.estimate(RouteQuery("A", "B"))
        assert "secret provider diagnostics" not in str(rejected.value)


@pytest.mark.asyncio
async def test_route_adapter_maps_network_timeout_and_keeps_client_open() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("provider stalled", request=request)

    async with httpx.AsyncClient(
        base_url="https://routes.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        adapter = RouteServiceAdapter(client, retry_factory=_immediate_retry)
        with pytest.raises(RouteServiceUnavailable, match="timed out"):
            await adapter.estimate(RouteQuery("A", "B"))
        assert not client.is_closed


async def _skip_retry_delay(_: float) -> None:
    await asyncio.sleep(0)


def _immediate_retry():
    return build_route_retrying(sleep=_skip_retry_delay)


@pytest.mark.asyncio
async def test_route_adapter_retries_two_transient_failures_then_succeeds() -> None:
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise httpx.ConnectError("temporary", request=request)
        return httpx.Response(
            200,
            json={
                "route_id": "provider-route-42",
                "distance_meters": 4_200,
                "duration_seconds": 1_080,
            },
        )

    async with httpx.AsyncClient(
        base_url="https://routes.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        adapter = RouteServiceAdapter(client, retry_factory=_immediate_retry)
        assert (await adapter.estimate(RouteQuery("A", "B"))).minutes == 18
    assert attempts == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("temporary_status", [429, 502, 503, 504])
async def test_route_adapter_retries_allowlisted_temporary_statuses(temporary_status) -> None:
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(temporary_status, request=request)
        return httpx.Response(
            200,
            request=request,
            json={
                "route_id": "provider-route-42",
                "distance_meters": 4_200,
                "duration_seconds": 1_080,
            },
        )

    async with httpx.AsyncClient(
        base_url="https://routes.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        adapter = RouteServiceAdapter(client, retry_factory=_immediate_retry)
        assert (await adapter.estimate(RouteQuery("A", "B"))).minutes == 18
    assert attempts == 2


@pytest.mark.asyncio
async def test_open_route_circuit_fails_fast_without_network_call() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("offline", request=request)

    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=60)
    async with httpx.AsyncClient(
        base_url="https://routes.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        adapter = RouteServiceAdapter(
            client,
            retry_factory=_immediate_retry,
            circuit_breaker=breaker,
        )
        with pytest.raises(RouteServiceUnavailable):
            await adapter.estimate(RouteQuery("A", "B"))
        first_operation_attempts = calls
        with pytest.raises(RouteServiceUnavailable, match="temporarily disabled"):
            await adapter.estimate(RouteQuery("C", "D"))
    assert first_operation_attempts == 3
    assert calls == first_operation_attempts


@pytest.mark.asyncio
async def test_route_adapter_does_not_retry_permanent_contract_failure() -> None:
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(
            200,
            json={
                "route_id": "provider-route-42",
                "distance_meters": 4_200,
                "duration_seconds": 0,
            },
        )

    async with httpx.AsyncClient(
        base_url="https://routes.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        adapter = RouteServiceAdapter(client, retry_factory=_immediate_retry)
        with pytest.raises(InvalidRouteResponse):
            await adapter.estimate(RouteQuery("A", "B"))
    assert attempts == 1


@pytest.mark.asyncio
async def test_route_retry_propagates_cancellation_after_cleanup() -> None:
    started = asyncio.Event()
    cleaned = asyncio.Event()
    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    task = asyncio.create_task(
        run_with_route_retry(operation, retry_factory=_immediate_retry)
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert attempts == 1
    assert cleaned.is_set()


@pytest.mark.asyncio
async def test_route_adapter_total_deadline_cancels_attempt_and_cleans_up() -> None:
    cleaned = asyncio.Event()
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    async with httpx.AsyncClient(
        base_url="https://routes.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        adapter = RouteServiceAdapter(
            client,
            retry_factory=_immediate_retry,
            total_timeout_seconds=0.01,
        )
        with pytest.raises(RouteServiceUnavailable, match="deadline"):
            await adapter.estimate(RouteQuery("A", "B"))
    assert attempts == 1
    assert cleaned.is_set()


@pytest.mark.asyncio
async def test_tracking_hub_drops_oldest_event_for_slow_subscriber() -> None:
    order_id = uuid4()
    hub = TrackingHub(queue_size=2)
    queue = hub.subscribe(order_id)
    await hub.publish(TrackingEvent(order_id, "created", 1))
    await hub.publish(TrackingEvent(order_id, "assigned", 2))
    await hub.publish(TrackingEvent(order_id, "picked_up", 3))

    first = queue.get_nowait()
    second = queue.get_nowait()
    assert [first.sequence, second.sequence] == [2, 3]
    hub.unsubscribe(order_id, queue)


@pytest.mark.asyncio
async def test_outbox_consumer_applies_each_event_once() -> None:
    class RecordingConsumer(InMemoryOutboxConsumerFake):
        def __init__(self) -> None:
            super().__init__()
            self.applied: list[str] = []

        async def apply(self, event: OutboxEvent) -> None:
            self.applied.append(event.event_type)

    consumer = RecordingConsumer()
    event = OutboxEvent(uuid4(), "order.created", {"order_id": str(uuid4())})
    assert await consumer.handle(event)
    assert not await consumer.handle(event)
    assert consumer.applied == ["order.created"]


def test_order_created_event_is_an_explicit_versioned_snapshot(order_factory) -> None:
    order = order_factory()
    event = OrderCreated.from_order(order)

    assert event.order_id == order.id
    assert event.order_version == order.version
    assert event.integration_payload() == {
        "schema_version": 1,
        "event_id": str(event.event_id),
        "event_type": "order.created",
        "aggregate_id": str(order.id),
        "aggregate_version": order.version,
        "tenant_id": str(order.tenant_id),
        "customer_id": str(order.customer_id),
        "occurred_at": order.created_at.isoformat(),
    }


@pytest.mark.asyncio
async def test_bounded_gather_never_exceeds_declared_concurrency() -> None:
    active = 0
    maximum = 0

    async def operation(value: int) -> int:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep((8 - value) / 10_000)
        active -= 1
        return value

    factories = [lambda value=value: operation(value) for value in range(8)]
    assert await gather_bounded(factories, concurrency=3) == list(range(8))
    assert maximum == 3


@pytest.mark.asyncio
async def test_coroutine_call_is_lazy_and_task_group_schedules_both_waiters() -> None:
    calls: list[str] = []
    both_started = asyncio.Event()

    async def wait_for_release(name: str) -> str:
        calls.append(name)
        if len(calls) == 2:
            both_started.set()
        await both_started.wait()
        return name

    not_started = wait_for_release("lazy")
    assert calls == []
    not_started.close()

    async with asyncio.TaskGroup() as group:
        first = group.create_task(wait_for_release("first"))
        second = group.create_task(wait_for_release("second"))

    assert {first.result(), second.result()} == {"first", "second"}


@pytest.mark.asyncio
async def test_task_group_cancels_sibling_and_waits_for_cleanup() -> None:
    waiting = asyncio.Event()
    cleaned = asyncio.Event()

    async def sibling() -> None:
        try:
            waiting.set()
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    async def fail_after_sibling_started() -> None:
        await waiting.wait()
        raise RuntimeError("route provider failed")

    with pytest.raises(ExceptionGroup):
        async with asyncio.TaskGroup() as group:
            group.create_task(sibling())
            group.create_task(fail_after_sibling_started())

    assert cleaned.is_set()


@pytest.mark.asyncio
async def test_blocking_io_bridge_is_bounded_and_propagates_context() -> None:
    active = 0
    maximum = 0
    lock = threading.Lock()

    def blocking_call(value: int) -> tuple[int, str]:
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        try:
            time.sleep(0.002)
            return value, correlation_id.get()
        finally:
            with lock:
                active -= 1

    token = correlation_id.set("request-26")
    try:
        results = await asyncio.gather(
            *(run_blocking(blocking_call, value) for value in range(16))
        )
    finally:
        correlation_id.reset(token)

    assert [value for value, _ in results] == list(range(16))
    assert {request_id for _, request_id in results} == {"request-26"}
    assert maximum == LEGACY_IO_CONCURRENCY


@pytest.mark.asyncio
async def test_upload_validation_checks_signature_and_limit() -> None:
    tenant_id = uuid4()
    png = UploadFile(filename="proof.png", file=BytesIO(b"\x89PNG\r\n\x1a\nbody"))
    proof = await inspect_proof(
        png, tenant_id=tenant_id, order_id=uuid4(), max_bytes=64
    )
    assert proof.metadata.media_type == "image/png" and proof.metadata.size == 12
    assert proof.content == b"\x89PNG\r\n\x1a\nbody"
    assert png.file.closed

    executable = UploadFile(filename="proof.png", file=BytesIO(b"MZpayload"))
    with pytest.raises(InvalidUpload):
        await inspect_proof(
            executable, tenant_id=tenant_id, order_id=uuid4(), max_bytes=64
        )
    assert executable.file.closed

    mismatched = UploadFile(filename="proof.jpg", file=BytesIO(b"\x89PNG\r\n\x1a\nbody"))
    with pytest.raises(InvalidUpload):
        await inspect_proof(
            mismatched, tenant_id=tenant_id, order_id=uuid4(), max_bytes=64
        )

    oversized = UploadFile(filename="proof.png", file=BytesIO(b"\x89PNG\r\n\x1a\nbody"))
    with pytest.raises(UploadTooLarge):
        await inspect_proof(
            oversized, tenant_id=tenant_id, order_id=uuid4(), max_bytes=8
        )
    assert oversized.file.closed
