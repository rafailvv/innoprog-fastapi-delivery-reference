"""Transaction retry policy for PostgreSQL isolation conflicts."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import random
from typing import TypeVar

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


T = TypeVar("T")
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01"})


def postgres_sqlstate(error: BaseException) -> str | None:
    """Find a PostgreSQL SQLSTATE through SQLAlchemy/driver wrappers."""
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        value = getattr(current, "sqlstate", None) or getattr(current, "pgcode", None)
        if value is not None:
            return str(value)
        for nested in (
            getattr(current, "orig", None),
            current.__cause__,
            current.__context__,
        ):
            if isinstance(nested, BaseException):
                pending.append(nested)
    return None


async def run_serializable(
    session_factory: async_sessionmaker[AsyncSession],
    operation: Callable[[AsyncSession], Awaitable[T]],
    *,
    attempts: int = 3,
    base_delay: float = 0.01,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    jitter: Callable[[float, float], float] = random.uniform,
) -> T:
    """Run the whole use case in a fresh SERIALIZABLE transaction per attempt."""
    if attempts < 1:
        raise ValueError("attempts must be positive")
    if base_delay < 0:
        raise ValueError("base_delay cannot be negative")

    for attempt in range(attempts):
        try:
            async with session_factory() as session:
                async with session.begin():
                    # This must be the first database statement of the new
                    # transaction.  The operation then reads and decides from
                    # the SERIALIZABLE snapshot owned by this attempt.
                    await session.execute(
                        text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    )
                    return await operation(session)
        except DBAPIError as error:
            if postgres_sqlstate(error) not in RETRYABLE_SQLSTATES:
                raise
            if attempt + 1 == attempts:
                raise
            exponential = base_delay * (2**attempt)
            await sleep(exponential + jitter(0.0, base_delay / 2))

    raise AssertionError("unreachable")
