from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
import cProfile
from dataclasses import asdict, dataclass
import io
import math
import pstats
import tracemalloc
from typing import Any

import httpx


@dataclass(frozen=True, slots=True)
class OperationSpec:
    """One bounded operation in a repeatable workload mix."""

    name: str
    method: str
    path: str
    weight: int
    expected_statuses: frozenset[int]
    json_factory: Callable[[int], dict[str, Any] | None] | None = None
    header_factory: Callable[[int], dict[str, str]] | None = None

    def __post_init__(self) -> None:
        if not self.name or not self.path.startswith("/"):
            raise ValueError("operation needs a stable name and absolute path")
        if self.weight <= 0 or not self.expected_statuses:
            raise ValueError("weight and expected statuses must be positive")


@dataclass(frozen=True, slots=True)
class LoadPlan:
    duration_seconds: float
    arrival_rate_per_second: float
    concurrency_limit: int
    timeout_seconds: float
    operations: tuple[OperationSpec, ...]

    def __post_init__(self) -> None:
        if self.duration_seconds <= 0 or self.arrival_rate_per_second <= 0:
            raise ValueError("duration and arrival rate must be positive")
        if self.concurrency_limit <= 0 or self.timeout_seconds <= 0:
            raise ValueError("concurrency and timeout must be positive")
        if not self.operations:
            raise ValueError("at least one operation is required")

    @property
    def request_count(self) -> int:
        return max(1, math.ceil(self.duration_seconds * self.arrival_rate_per_second))


@dataclass(frozen=True, slots=True)
class RequestSample:
    operation: str
    scheduled_at: float
    started_at: float
    finished_at: float
    status_code: int | None
    succeeded: bool
    error_type: str | None = None

    @property
    def service_latency_ms(self) -> float:
        return max(0.0, (self.finished_at - self.started_at) * 1000)

    @property
    def end_to_end_latency_ms(self) -> float:
        return max(0.0, (self.finished_at - self.scheduled_at) * 1000)

    @property
    def queue_delay_ms(self) -> float:
        return max(0.0, (self.started_at - self.scheduled_at) * 1000)


@dataclass(frozen=True, slots=True)
class LoadReport:
    requests: int
    successes: int
    failures: int
    error_rate: float
    throughput_per_second: float
    service_p50_ms: float
    service_p95_ms: float
    service_p99_ms: float
    end_to_end_p95_ms: float
    queue_delay_p95_ms: float

    def as_dict(self) -> dict[str, int | float]:
        return asdict(self)


def nearest_rank(values: Sequence[float], percentile: float) -> float:
    """Return the nearest-rank percentile; p95 bounds 95% of observations."""

    if not values:
        raise ValueError("percentile requires at least one observation")
    if not 0 < percentile <= 1:
        raise ValueError("percentile must be in (0, 1]")
    ordered = sorted(values)
    return float(ordered[max(0, math.ceil(percentile * len(ordered)) - 1)])


def weighted_cycle(operations: Sequence[OperationSpec]) -> tuple[OperationSpec, ...]:
    """Build a deterministic cycle so baseline and candidate use the same mix."""

    cycle: list[OperationSpec] = []
    for operation in operations:
        cycle.extend([operation] * operation.weight)
    return tuple(cycle)


def summarize(samples: Sequence[RequestSample], elapsed_seconds: float) -> LoadReport:
    if not samples or elapsed_seconds <= 0:
        raise ValueError("a report needs samples and positive elapsed time")
    service = [sample.service_latency_ms for sample in samples]
    end_to_end = [sample.end_to_end_latency_ms for sample in samples]
    queue = [sample.queue_delay_ms for sample in samples]
    successes = sum(sample.succeeded for sample in samples)
    failures = len(samples) - successes
    return LoadReport(
        requests=len(samples),
        successes=successes,
        failures=failures,
        error_rate=failures / len(samples),
        throughput_per_second=len(samples) / elapsed_seconds,
        service_p50_ms=nearest_rank(service, 0.50),
        service_p95_ms=nearest_rank(service, 0.95),
        service_p99_ms=nearest_rank(service, 0.99),
        end_to_end_p95_ms=nearest_rank(end_to_end, 0.95),
        queue_delay_p95_ms=nearest_rank(queue, 0.95),
    )


async def run_open_loop(client: httpx.AsyncClient, plan: LoadPlan) -> LoadReport:
    """Schedule arrivals by the clock instead of waiting for the previous response."""

    loop = asyncio.get_running_loop()
    experiment_started = loop.time()
    slots = asyncio.Semaphore(plan.concurrency_limit)
    cycle = weighted_cycle(plan.operations)

    async def execute(index: int, scheduled_at: float) -> RequestSample:
        operation = cycle[index % len(cycle)]
        async with slots:
            started_at = loop.time()
            try:
                response = await client.request(
                    operation.method,
                    operation.path,
                    json=operation.json_factory(index) if operation.json_factory else None,
                    headers=operation.header_factory(index) if operation.header_factory else None,
                    timeout=plan.timeout_seconds,
                )
            except (httpx.HTTPError, TimeoutError) as exc:
                return RequestSample(
                    operation.name, scheduled_at, started_at, loop.time(), None, False,
                    type(exc).__name__,
                )
            return RequestSample(
                operation.name,
                scheduled_at,
                started_at,
                loop.time(),
                response.status_code,
                response.status_code in operation.expected_statuses,
            )

    tasks: list[asyncio.Task[RequestSample]] = []
    interval = 1 / plan.arrival_rate_per_second
    for index in range(plan.request_count):
        scheduled_at = experiment_started + index * interval
        await asyncio.sleep(max(0.0, scheduled_at - loop.time()))
        tasks.append(asyncio.create_task(execute(index, scheduled_at)))
    samples = await asyncio.gather(*tasks)
    return summarize(samples, loop.time() - experiment_started)


@dataclass(frozen=True, slots=True)
class ProfileReport:
    iterations: int
    peak_memory_bytes: int
    top_cumulative: str


def profile_sync(operation: Callable[[], object], *, iterations: int = 100) -> ProfileReport:
    """Profile one deterministic hot path; never wrap unrelated application startup."""

    if iterations <= 0:
        raise ValueError("iterations must be positive")
    profiler = cProfile.Profile()
    tracemalloc.start()
    try:
        profiler.enable()
        for _ in range(iterations):
            operation()
        profiler.disable()
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    stream = io.StringIO()
    pstats.Stats(profiler, stream=stream).strip_dirs().sort_stats("cumulative").print_stats(12)
    return ProfileReport(iterations, peak, stream.getvalue())


def compare_reports(
    baseline: LoadReport,
    candidate: LoadReport,
    *,
    max_error_rate: float,
    max_p95_ms: float,
) -> dict[str, float | bool]:
    """Gate a candidate without hiding an error or queue-delay regression."""

    return {
        "p95_change_percent": (
            (candidate.end_to_end_p95_ms - baseline.end_to_end_p95_ms)
            / baseline.end_to_end_p95_ms
            * 100
            if baseline.end_to_end_p95_ms else 0.0
        ),
        "throughput_change_percent": (
            (candidate.throughput_per_second - baseline.throughput_per_second)
            / baseline.throughput_per_second
            * 100
            if baseline.throughput_per_second else 0.0
        ),
        "error_rate_ok": candidate.error_rate <= max_error_rate,
        "p95_ok": candidate.end_to_end_p95_ms <= max_p95_ms,
        "queue_not_worse": candidate.queue_delay_p95_ms <= baseline.queue_delay_p95_ms,
    }
