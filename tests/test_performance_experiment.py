from __future__ import annotations

import asyncio

import httpx
import pytest

from delivery_service.performance import (
    LoadPlan,
    LoadReport,
    OperationSpec,
    RequestSample,
    compare_reports,
    nearest_rank,
    profile_sync,
    run_open_loop,
    summarize,
    weighted_cycle,
)


def test_nearest_rank_p95_is_the_boundary_for_95_percent_of_samples() -> None:
    values = list(range(1, 101))
    assert nearest_rank(values, 0.50) == 50
    assert nearest_rank(values, 0.95) == 95
    assert nearest_rank(values, 0.99) == 99


def test_report_keeps_tail_error_throughput_and_client_queue_separate() -> None:
    samples = [
        RequestSample("read", 0.00, 0.00, 0.01, 200, True),
        RequestSample("read", 0.10, 0.12, 0.16, 200, True),
        RequestSample("write", 0.20, 0.30, 0.50, 503, False),
    ]
    report = summarize(samples, elapsed_seconds=0.5)
    assert report.requests == 3 and report.failures == 1
    assert report.error_rate == pytest.approx(1 / 3)
    assert report.throughput_per_second == 6
    assert report.service_p95_ms == pytest.approx(200)
    assert report.end_to_end_p95_ms == pytest.approx(300)
    assert report.queue_delay_p95_ms == pytest.approx(100)


def test_weighted_cycle_preserves_the_declared_business_mix() -> None:
    read = OperationSpec("read", "GET", "/orders", 4, frozenset({200}))
    write = OperationSpec("write", "POST", "/orders", 1, frozenset({201}))
    assert [item.name for item in weighted_cycle((read, write))] == [
        "read", "read", "read", "read", "write",
    ]


@pytest.mark.asyncio
async def test_open_loop_schedules_arrivals_without_waiting_for_previous_response() -> None:
    active = 0
    peak_active = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, peak_active
        active += 1
        peak_active = max(peak_active, active)
        await asyncio.sleep(0.03)
        active -= 1
        return httpx.Response(200, request=request)

    operation = OperationSpec("read", "GET", "/orders", 1, frozenset({200}))
    plan = LoadPlan(0.05, 100, 4, 1, (operation,))
    async with httpx.AsyncClient(
        base_url="http://load.test", transport=httpx.MockTransport(handler),
    ) as client:
        report = await run_open_loop(client, plan)

    assert report.requests == 5 and report.error_rate == 0
    assert peak_active > 1
    assert report.queue_delay_p95_ms > 0


def test_profiler_names_the_hot_function_and_measures_peak_memory() -> None:
    def parse_fixture() -> int:
        return sum(number * number for number in range(100))

    report = profile_sync(parse_fixture, iterations=20)
    assert report.iterations == 20
    assert report.peak_memory_bytes > 0
    assert "parse_fixture" in report.top_cumulative


def test_candidate_must_not_buy_latency_with_errors_or_client_queueing() -> None:
    baseline = LoadReport(100, 100, 0, 0, 10, 20, 80, 120, 130, 10)
    candidate = LoadReport(100, 98, 2, 0.02, 12, 15, 60, 90, 100, 30)
    verdict = compare_reports(baseline, candidate, max_error_rate=0.01, max_p95_ms=110)
    assert verdict["p95_ok"] is True
    assert verdict["error_rate_ok"] is False
    assert verdict["queue_not_worse"] is False
