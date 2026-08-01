from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError

from delivery_service.transactions import postgres_sqlstate, run_serializable


class DriverFailure(RuntimeError):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


def database_error(sqlstate: str) -> DBAPIError:
    return DBAPIError("statement", {}, DriverFailure(sqlstate), False)


class FakeSession:
    def __init__(self, number: int) -> None:
        self.number = number
        self.statements: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception) -> None:
        return None

    def begin(self):
        return self

    async def execute(self, statement) -> None:
        self.statements.append(str(statement))


class FakeFactory:
    def __init__(self) -> None:
        self.sessions: list[FakeSession] = []

    def __call__(self) -> FakeSession:
        session = FakeSession(len(self.sessions) + 1)
        self.sessions.append(session)
        return session


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ("40001", "40P01"))
async def test_retryable_conflict_restarts_with_a_fresh_session(state: str) -> None:
    factory = FakeFactory()
    calls: list[int] = []
    delays: list[float] = []

    async def operation(session: FakeSession) -> str:
        calls.append(session.number)
        if len(calls) < 3:
            raise database_error(state)
        return "committed"

    async def remember_delay(delay: float) -> None:
        delays.append(delay)

    result = await run_serializable(
        factory,  # type: ignore[arg-type]
        operation,  # type: ignore[arg-type]
        attempts=3,
        sleep=remember_delay,
        jitter=lambda _start, _end: 0.0,
    )

    assert result == "committed"
    assert calls == [1, 2, 3]
    assert len(factory.sessions) == 3
    assert all(
        session.statements == ["SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"]
        for session in factory.sessions
    )
    assert delays == [0.01, 0.02]


@pytest.mark.asyncio
async def test_non_retryable_sqlstate_is_not_repeated() -> None:
    factory = FakeFactory()
    calls = 0

    async def operation(_session: FakeSession) -> None:
        nonlocal calls
        calls += 1
        raise database_error("23505")

    with pytest.raises(DBAPIError):
        await run_serializable(
            factory,  # type: ignore[arg-type]
            operation,  # type: ignore[arg-type]
            sleep=lambda _delay: None,  # type: ignore[arg-type]
        )
    assert calls == 1
    assert len(factory.sessions) == 1


def test_sqlstate_is_found_through_sqlalchemy_wrapper() -> None:
    assert postgres_sqlstate(database_error("40P01")) == "40P01"
    assert postgres_sqlstate(RuntimeError("not a database error")) is None


@pytest.mark.asyncio
async def test_retry_budget_must_be_positive() -> None:
    with pytest.raises(ValueError, match="attempts"):
        await run_serializable(FakeFactory(), lambda _session: None, attempts=0)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_last_retryable_error_is_not_hidden() -> None:
    factory = FakeFactory()
    calls = 0

    async def operation(_session: FakeSession) -> None:
        nonlocal calls
        calls += 1
        raise database_error("40001")

    async def no_delay(_delay: float) -> None:
        return None

    with pytest.raises(DBAPIError) as captured:
        await run_serializable(
            factory,  # type: ignore[arg-type]
            operation,  # type: ignore[arg-type]
            attempts=2,
            sleep=no_delay,
            jitter=lambda _start, _end: 0.0,
        )
    assert postgres_sqlstate(captured.value) == "40001"
    assert calls == 2
    assert len(factory.sessions) == 2
