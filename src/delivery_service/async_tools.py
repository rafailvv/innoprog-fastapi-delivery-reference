from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from typing import TypeVar, cast

from anyio import CapacityLimiter, to_thread


T = TypeVar("T")
LEGACY_IO_CONCURRENCY = 8
legacy_io_limiter = CapacityLimiter(LEGACY_IO_CONCURRENCY)


async def run_blocking(function: Callable[..., T], /, *args: object) -> T:
    """Move blocking I/O to a separately bounded AnyIO worker thread."""
    return await to_thread.run_sync(function, *args, limiter=legacy_io_limiter)


async def gather_bounded(
    factories: Iterable[Callable[[], Awaitable[T]]], *, concurrency: int
) -> list[T]:
    """Run related awaitables concurrently, bounded and in input order."""
    if concurrency < 1:
        raise ValueError("concurrency must be positive")
    planned = list(factories)
    semaphore = asyncio.Semaphore(concurrency)
    missing = object()
    results: list[T | object] = [missing] * len(planned)

    async def run(index: int, factory: Callable[[], Awaitable[T]]) -> None:
        async with semaphore:
            results[index] = await factory()

    async with asyncio.TaskGroup() as group:
        for index, factory in enumerate(planned):
            group.create_task(run(index, factory), name=f"bounded-{index}")
    assert all(result is not missing for result in results)
    return [cast(T, result) for result in results]
