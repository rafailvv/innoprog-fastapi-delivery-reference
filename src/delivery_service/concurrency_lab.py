"""Development-only probes that make the sync/async boundary observable.

They are excluded from the production application.  Course stage 13 uses the
router to prove behavior through HTTP instead of accepting source-code tokens.
"""

from __future__ import annotations

import asyncio
import threading

import httpx
from fastapi import APIRouter, HTTPException

from delivery_service.async_tools import run_blocking
from delivery_service.db import orm_mapping_contract, session_runtime_contract
from delivery_service.retry_policy import build_route_retrying, run_with_route_retry


router = APIRouter(prefix="/runtime", include_in_schema=False)
_started = threading.Event()
_release = threading.Event()
_offload_started = threading.Event()
_offload_release = threading.Event()


@router.get("/orm-contract")
async def orm_contract() -> dict[str, object]:
    """Return real configured metadata without opening a database connection."""
    return orm_mapping_contract()


@router.get("/session-contract")
async def session_contract() -> dict[str, object]:
    """Prove two factory calls never share mutable Session state."""
    return await session_runtime_contract()


@router.post("/sync-wait")
def sync_wait() -> dict[str, int]:
    """Block one worker thread until the event-loop endpoint releases it."""
    _release.clear()
    _started.set()
    try:
        if not _release.wait(timeout=5):
            raise HTTPException(status_code=504, detail="runtime probe was not released")
        return {"worker_thread": threading.get_ident()}
    finally:
        _started.clear()


@router.get("/state")
async def runtime_state() -> dict[str, int | bool]:
    return {
        "started": _started.is_set(),
        "offload_started": _offload_started.is_set(),
        "event_loop_thread": threading.get_ident(),
    }


@router.post("/release")
async def release_probe() -> dict[str, bool]:
    _release.set()
    _offload_release.set()
    return {"released": True}


def _blocking_probe() -> int:
    _offload_release.clear()
    _offload_started.set()
    try:
        if not _offload_release.wait(timeout=5):
            raise RuntimeError("offload probe was not released")
        return threading.get_ident()
    finally:
        _offload_started.clear()


@router.post("/offload-wait")
async def offload_wait() -> dict[str, int]:
    """Prove explicit offload keeps an async handler's event loop responsive."""
    return {"worker_thread": await run_blocking(_blocking_probe)}


async def _skip_retry_delay(_: float) -> None:
    await asyncio.sleep(0)


def _immediate_retry():
    return build_route_retrying(sleep=_skip_retry_delay)


@router.post("/retry/transient")
async def retry_transient() -> dict[str, int | str]:
    """Fail twice with a real retryable HTTPX error, then succeed."""
    attempts = 0

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            request = httpx.Request("GET", "https://routes.test/estimate")
            raise httpx.ConnectError("temporary probe failure", request=request)
        return "ok"

    result = await run_with_route_retry(operation, retry_factory=_immediate_retry)
    return {"result": result, "attempts": attempts}


@router.post("/retry/permanent")
async def retry_permanent() -> dict[str, int | bool]:
    """A non-allowlisted contract error must not receive another attempt."""
    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError("permanent contract failure")

    rejected = False
    try:
        await run_with_route_retry(operation, retry_factory=_immediate_retry)
    except ValueError:
        rejected = True
    return {"rejected": rejected, "attempts": attempts}


@router.post("/retry/cancellation")
async def retry_cancellation() -> dict[str, int | bool]:
    """Cancel a real Task and prove cleanup plus propagation."""
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
    cancelled = False
    try:
        await task
    except asyncio.CancelledError:
        cancelled = True
    return {
        "cancelled": cancelled,
        "cleaned": cleaned.is_set(),
        "attempts": attempts,
    }


@router.post("/retry/deadline")
async def retry_deadline() -> dict[str, int | bool]:
    """Expire one total deadline and observe cleanup of the awaited operation."""
    cleaned = asyncio.Event()
    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    expired = False
    try:
        async with asyncio.timeout(0.01):
            await run_with_route_retry(operation, retry_factory=_immediate_retry)
    except TimeoutError:
        expired = True
    return {
        "expired": expired,
        "cleaned": cleaned.is_set(),
        "attempts": attempts,
    }
