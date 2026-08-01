"""A real PostgreSQL 16 check for constraints used by courier assignment."""

import asyncio
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

import asyncpg
import pytest


async def _exercise_session_lifecycle(async_url: str) -> None:
    from datetime import UTC, datetime

    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import OrderRow, transactional_session

    local_engine = create_async_engine(async_url, pool_size=2, max_overflow=0)
    local_factory = async_sessionmaker(
        local_engine, class_=AsyncSession, expire_on_commit=False
    )
    committed_id = UUID("00000000-0000-0000-0000-000000000032")
    rolled_back_id = UUID("00000000-0000-0000-0000-000000000033")
    customer = UUID("00000000-0000-0000-0000-000000000034")

    def row(order_id: UUID) -> OrderRow:
        return OrderRow(
            id=order_id,
            customer_id=customer,
            pickup_address="Session street 1",
            destination_address="Pool street 2",
            weight_grams=500,
            status="created",
            version=1,
            created_at=datetime.now(UTC),
        )

    try:
        async with transactional_session(local_factory) as session:
            session.add(row(committed_id))

        try:
            async with transactional_session(local_factory) as session:
                session.add(row(rolled_back_id))
                await session.flush()
                raise RuntimeError("force rollback")
        except RuntimeError:
            pass

        async with local_factory() as session:
            persisted = await session.scalar(
                select(func.count()).select_from(OrderRow).where(
                    OrderRow.id.in_((committed_id, rolled_back_id))
                )
            )
            assert persisted == 1
            assert await session.get(OrderRow, committed_id) is not None
            assert await session.get(OrderRow, rolled_back_id) is None

        async with local_factory() as first, local_factory() as second:
            assert first is not second
            assert first.identity_map is not second.identity_map
            await first.execute(select(1))
            await second.execute(select(1))
    finally:
        await local_engine.dispose()


async def _exercise_repository_crud(async_url: str) -> None:
    from dataclasses import replace

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import OrderRow, transactional_session
    from delivery_service.domain import DeliveryOrder
    from delivery_service.repository import SqlAlchemyOrderRepository

    local_engine = create_async_engine(async_url, pool_size=2, max_overflow=0)
    local_factory = async_sessionmaker(
        local_engine, class_=AsyncSession, expire_on_commit=False
    )
    customer_id = UUID("00000000-0000-0000-0000-000000000044")
    committed = replace(
        DeliveryOrder.create(
            customer_id=customer_id,
            pickup_address="Repository street 1",
            destination_address="Domain avenue 2",
            weight_grams=750,
        ),
        id=UUID("00000000-0000-0000-0000-000000000045"),
    )
    rolled_back = replace(
        DeliveryOrder.create(
            customer_id=customer_id,
            pickup_address="Rollback street 3",
            destination_address="Atomic avenue 4",
            weight_grams=900,
        ),
        id=UUID("00000000-0000-0000-0000-000000000046"),
    )

    try:
        async with transactional_session(local_factory) as session:
            repository = SqlAlchemyOrderRepository(session)
            await repository.add(committed)
            # flush has run, but a different transaction must not observe an
            # uncommitted row.
            async with local_factory() as observer:
                assert await observer.get(OrderRow, committed.id) is None

        async with transactional_session(local_factory) as session:
            repository = SqlAlchemyOrderRepository(session)
            assert await repository.get(committed.id) == committed
            listed = await repository.list(limit=100, offset=0)
            assert committed.id in {order.id for order in listed}
            assert all(isinstance(order, DeliveryOrder) for order in listed)

        with pytest.raises(RuntimeError, match="outbox unavailable"):
            async with transactional_session(local_factory) as session:
                repository = SqlAlchemyOrderRepository(session)
                await repository.add(rolled_back)
                raise RuntimeError("outbox unavailable")

        async with local_factory() as session:
            assert await session.scalar(
                select(OrderRow).where(OrderRow.id == rolled_back.id)
            ) is None
    finally:
        await local_engine.dispose()


async def _exercise_tracking_snapshot_atomicity(async_url: str) -> None:
    from datetime import UTC, datetime

    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import OrderRow, OutboxRow, TrackingSnapshotRow, transactional_session
    from delivery_service.tracking import SqlAlchemyTrackingSnapshotRepository

    local_engine = create_async_engine(async_url, pool_size=4, max_overflow=0)
    local_factory = async_sessionmaker(local_engine, class_=AsyncSession, expire_on_commit=False)
    order_id = UUID("00000000-0000-0000-0000-000000000171")
    tenant_id = UUID("00000000-0000-0000-0000-000000000172")
    customer_id = UUID("00000000-0000-0000-0000-000000000173")
    first_event = UUID("00000000-0000-0000-0000-000000000174")
    second_event = UUID("00000000-0000-0000-0000-000000000175")
    rolled_back_event = UUID("00000000-0000-0000-0000-000000000176")
    try:
        async with transactional_session(local_factory) as session:
            session.add(OrderRow(
                id=order_id,
                tenant_id=tenant_id,
                customer_id=customer_id,
                pickup_address="Tracking street 1",
                destination_address="Snapshot street 2",
                weight_grams=600,
                status="assigned",
                version=2,
                created_at=datetime.now(UTC),
            ))

        async with transactional_session(local_factory) as session:
            repository = SqlAlchemyTrackingSnapshotRepository(session)
            first, changed = await repository.record(
                order_id=order_id, tenant_id=tenant_id, status="assigned",
                latitude=59.9343, longitude=30.3351,
                client_event_id=first_event, recorded_at=datetime.now(UTC),
            )
            assert changed and first.sequence == 1

        async with transactional_session(local_factory) as session:
            repository = SqlAlchemyTrackingSnapshotRepository(session)
            replay, changed = await repository.record(
                order_id=order_id, tenant_id=tenant_id, status="assigned",
                latitude=0, longitude=0,
                client_event_id=first_event, recorded_at=datetime.now(UTC),
            )
            assert not changed and replay.sequence == 1
            assert (replay.latitude, replay.longitude) == (59.9343, 30.3351)

        async with transactional_session(local_factory) as session:
            repository = SqlAlchemyTrackingSnapshotRepository(session)
            second, changed = await repository.record(
                order_id=order_id, tenant_id=tenant_id, status="assigned",
                latitude=59.94, longitude=30.34,
                client_event_id=second_event, recorded_at=datetime.now(UTC),
            )
            assert changed and second.sequence == 2

        with pytest.raises(RuntimeError, match="broker unavailable"):
            async with transactional_session(local_factory) as session:
                repository = SqlAlchemyTrackingSnapshotRepository(session)
                provisional, changed = await repository.record(
                    order_id=order_id, tenant_id=tenant_id, status="assigned",
                    latitude=60, longitude=31,
                    client_event_id=rolled_back_event, recorded_at=datetime.now(UTC),
                )
                assert changed and provisional.sequence == 3
                raise RuntimeError("broker unavailable")

        async with local_factory() as session:
            persisted = await session.get(TrackingSnapshotRow, order_id)
            tracking_events = await session.scalar(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.event_type == "tracking.position_updated",
                    OutboxRow.payload["order_id"].as_string() == str(order_id),
                )
            )
            assert persisted is not None and persisted.sequence == 2
            assert persisted.client_event_id == second_event
            assert tracking_events == 2
    finally:
        await local_engine.dispose()


async def _exercise_query_loading_and_index_plan(
    async_url: str, postgres_url: str,
) -> None:
    """Prove loader round trips and the real PostgreSQL access path."""
    from datetime import UTC, datetime
    import json

    from sqlalchemy import event, select
    from sqlalchemy.exc import InvalidRequestError
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import AssignmentRow, OrderRow, transactional_session
    from delivery_service.repository import SqlAlchemyOrderRepository

    local_engine = create_async_engine(async_url, pool_size=2, max_overflow=0)
    local_factory = async_sessionmaker(
        local_engine, class_=AsyncSession, expire_on_commit=False
    )
    order_ids = [
        UUID(f"36000000-0000-0000-0000-{number:012d}")
        for number in range(1, 4)
    ]
    customer_id = UUID("36000000-0000-0000-0001-000000000001")

    try:
        async with transactional_session(local_factory) as session:
            for number, order_id in enumerate(order_ids, start=1):
                session.add(OrderRow(
                    id=order_id,
                    customer_id=customer_id,
                    pickup_address=f"Query street {number}",
                    destination_address=f"Index avenue {number}",
                    weight_grams=500,
                    status="created",
                    version=1,
                    created_at=datetime(2026, 2, number, tzinfo=UTC),
                    assignments=[AssignmentRow(
                        id=UUID(f"36000000-0000-0000-0002-{number:012d}"),
                        courier_id=UUID(f"36000000-0000-0000-0003-{number:012d}"),
                        active=True,
                    )],
                ))

        selects: list[str] = []

        def count_selects(
            _connection, _cursor, statement, _parameters, _context, _executemany,
        ) -> None:
            if statement.lstrip().upper().startswith("SELECT"):
                selects.append(statement)

        event.listen(
            local_engine.sync_engine, "before_cursor_execute", count_selects
        )
        try:
            async with local_factory() as session:
                repository = SqlAlchemyOrderRepository(session)
                loaded = await repository.list(limit=100, offset=0)
                selected = {order.id: order for order in loaded if order.id in order_ids}
                assert set(selected) == set(order_ids)
                assert all(selected[order_id].courier_id is not None for order_id in order_ids)
            # One SELECT for the page and one selectinload SELECT for every
            # relationship on that page; the count does not depend on N.
            assert len(selects) == 2, selects
        finally:
            event.remove(
                local_engine.sync_engine, "before_cursor_execute", count_selects
            )

        async with local_factory() as session:
            unloaded = await session.scalar(
                select(OrderRow).where(OrderRow.id == order_ids[0])
            )
            assert unloaded is not None
            with pytest.raises(InvalidRequestError):
                _ = unloaded.assignments
    finally:
        await local_engine.dispose()

    connection = await asyncpg.connect(postgres_url)
    try:
        await connection.execute(
            """INSERT INTO delivery_order
            (id, customer_id, pickup_address, destination_address,
             weight_grams, status, version, created_at)
            SELECT
              ('36000000-0000-0001-0000-' || lpad(value::text, 12, '0'))::uuid,
              '36000000-0000-0001-0001-000000000001'::uuid,
              'Planner street', 'B-tree avenue', 500,
              CASE WHEN value % 1000 = 0 THEN 'created' ELSE 'delivered' END,
              1,
              timestamptz '2026-03-01 00:00:00+00' + value * interval '1 second'
            FROM generate_series(1, 50000) AS value"""
        )
        await connection.execute("ANALYZE delivery_order")
        raw_plan = await connection.fetchval(
            """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT id, customer_id, status, created_at
            FROM delivery_order
            WHERE status = 'created'
            ORDER BY created_at, id
            LIMIT 50"""
        )

        plan = json.loads(raw_plan) if isinstance(raw_plan, str) else raw_plan

        def walk(node: dict):
            yield node
            for child in node.get("Plans", []):
                yield from walk(child)

        nodes = list(walk(plan[0]["Plan"]))
        assert any(
            node.get("Index Name") == "ix_delivery_order_status_created"
            for node in nodes
        ), plan
        assert not any(node["Node Type"] == "Sort" for node in nodes), plan
        assert max(node.get("Actual Rows", 0) for node in nodes) >= 50
    finally:
        await connection.close()


async def _exercise_atomic_order_unit_of_work(async_url: str) -> None:
    """Prove order, outbox and idempotency share one transaction outcome."""
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import IdempotencyRow, OrderRow, OutboxRow, transactional_session
    from delivery_service.repository import SqlAlchemyOrderRepository
    from delivery_service.commands import CreateOrderCommand
    from delivery_service.service import OrderService

    local_engine = create_async_engine(async_url, pool_size=2, max_overflow=0)
    local_factory = async_sessionmaker(
        local_engine, class_=AsyncSession, expire_on_commit=False
    )
    command = CreateOrderCommand(
        customer_id=UUID("37000000-0000-0000-0000-000000000001"),
        pickup_address="Transaction street 1",
        destination_address="Unit of Work avenue 2",
        weight_grams=800,
    )

    class FailingIdempotencyRepository(SqlAlchemyOrderRepository):
        async def remember_idempotency(self, scope, request_fingerprint, order) -> None:
            # add() and record_created_event() have already flushed real SQL;
            # failing the required last step must roll both records back.
            raise RuntimeError("idempotency storage unavailable")

    try:
        async with transactional_session(local_factory) as session:
            successful = await OrderService(
                SqlAlchemyOrderRepository(session)
            ).create(command, idempotency_key="lesson-37-success")

        async with local_factory() as observer:
            assert await observer.get(OrderRow, successful.id) is not None
            assert await observer.scalar(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.payload["aggregate_id"].as_string() == str(successful.id)
                )
            ) == 1
            assert await observer.scalar(
                select(func.count()).select_from(IdempotencyRow).where(
                    IdempotencyRow.resource_id == successful.id
                )
            ) == 1

        failed_id = None
        with pytest.raises(RuntimeError, match="idempotency storage unavailable"):
            async with transactional_session(local_factory) as session:
                repository = FailingIdempotencyRepository(session)
                service = OrderService(repository)
                failed = await service.create(
                    command, idempotency_key="lesson-37-failure"
                )
                failed_id = failed.id
        # The exception occurs before service.create returns, so find the
        # attempted order by its unique destination data instead of relying on
        # a Python value that did not cross the failed boundary.
        async with local_factory() as observer:
            failed_rows = list((await observer.scalars(
                select(OrderRow).where(
                    OrderRow.customer_id == command.customer_id,
                    OrderRow.id != successful.id,
                )
            )).all())
            assert failed_id is None
            assert failed_rows == []
            assert await observer.scalar(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.payload["customer_id"].as_string() == str(command.customer_id),
                    OutboxRow.payload["aggregate_id"].as_string() != str(successful.id),
                )
            ) == 0
            assert await observer.scalar(
                select(func.count()).select_from(IdempotencyRow).where(
                    IdempotencyRow.operation == "create_order",
                    IdempotencyRow.resource_id != successful.id,
                )
            ) == 0
    finally:
        await local_engine.dispose()


async def _exercise_transition_history_atomicity(async_url: str) -> None:
    """Prove status, immutable history and outbox share one transaction."""
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.commands import CreateOrderCommand, TransitionOrderCommand
    from delivery_service.db import OrderRow, OutboxRow, TransitionHistoryRow, transactional_session
    from delivery_service.domain import OrderStatus
    from delivery_service.repository import SqlAlchemyOrderRepository
    from delivery_service.service import OrderService

    local_engine = create_async_engine(async_url, pool_size=2, max_overflow=0)
    local_factory = async_sessionmaker(
        local_engine, class_=AsyncSession, expire_on_commit=False
    )

    def create_command(customer: int) -> CreateOrderCommand:
        return CreateOrderCommand(
            customer_id=UUID(int=customer),
            pickup_address="Transition street 1",
            destination_address="Audit avenue 2",
            weight_grams=680,
        )

    def transition(expected_version: int = 1) -> TransitionOrderCommand:
        return TransitionOrderCommand(
            target=OrderStatus.CANCELLED,
            expected_version=expected_version,
            actor_id=UUID(int=6805),
            actor_role="customer",
            correlation_id="postgres-lesson-68",
        )

    class FailingTransitionRepository(SqlAlchemyOrderRepository):
        async def save_transition(self, order, *, expected_version, event) -> None:
            # All three writes have reached PostgreSQL, then a later failure
            # forces the surrounding transaction to roll every one back.
            await super().save_transition(
                order, expected_version=expected_version, event=event,
            )
            raise RuntimeError("transition audit unavailable")

    try:
        async with transactional_session(local_factory) as session:
            successful = await OrderService(SqlAlchemyOrderRepository(session)).create(
                create_command(6810)
            )
        async with transactional_session(local_factory) as session:
            await OrderService(SqlAlchemyOrderRepository(session)).transition(
                successful.id, transition(),
            )

        async with local_factory() as observer:
            persisted = await observer.get(OrderRow, successful.id)
            assert persisted is not None
            assert (persisted.status, persisted.version) == ("cancelled", 2)
            history = await observer.scalar(
                select(TransitionHistoryRow).where(
                    TransitionHistoryRow.order_id == successful.id
                )
            )
            assert history is not None
            assert (history.from_status, history.to_status) == ("created", "cancelled")
            assert history.order_version == 2
            assert history.actor_id == UUID(int=6805)
            assert history.correlation_id == "postgres-lesson-68"
            assert await observer.scalar(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.event_type == "order.status_changed",
                    OutboxRow.payload["aggregate_id"].as_string() == str(successful.id),
                )
            ) == 1

        async with transactional_session(local_factory) as session:
            rolled_back = await OrderService(SqlAlchemyOrderRepository(session)).create(
                create_command(6811)
            )
        with pytest.raises(RuntimeError, match="transition audit unavailable"):
            async with transactional_session(local_factory) as session:
                await OrderService(FailingTransitionRepository(session)).transition(
                    rolled_back.id, transition(),
                )

        async with local_factory() as observer:
            unchanged = await observer.get(OrderRow, rolled_back.id)
            assert unchanged is not None
            assert (unchanged.status, unchanged.version) == ("created", 1)
            assert await observer.scalar(
                select(func.count()).select_from(TransitionHistoryRow).where(
                    TransitionHistoryRow.order_id == rolled_back.id
                )
            ) == 0
            assert await observer.scalar(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.event_type == "order.status_changed",
                    OutboxRow.payload["aggregate_id"].as_string() == str(rolled_back.id),
                )
            ) == 0
    finally:
        await local_engine.dispose()


async def _exercise_isolation_levels(async_url: str, postgres_url: str) -> None:
    """Reproduce READ COMMITTED write skew and SERIALIZABLE recovery."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.transactions import run_serializable

    courier_a = UUID("38000000-0000-0000-0000-000000000001")
    courier_b = UUID("38000000-0000-0000-0000-000000000002")
    setup = await asyncpg.connect(postgres_url)
    try:
        await setup.execute(
            """CREATE TABLE lesson38_shift (
                courier_id uuid PRIMARY KEY,
                available boolean NOT NULL
            )"""
        )
        await setup.executemany(
            "INSERT INTO lesson38_shift(courier_id, available) VALUES($1, true)",
            [(courier_a,), (courier_b,)],
        )

        # At READ COMMITTED both transactions read the same predicate but
        # update different rows.  Row locks therefore do not meet, and both
        # locally valid decisions can commit as write skew.
        first = await asyncpg.connect(postgres_url)
        second = await asyncpg.connect(postgres_url)
        rc_ready = asyncio.Event()
        rc_reads = 0

        async def leave_under_read_committed(
            connection: asyncpg.Connection, courier_id: UUID,
        ) -> bool:
            nonlocal rc_reads
            transaction = connection.transaction(isolation="read_committed")
            await transaction.start()
            try:
                available = await connection.fetchval(
                    "SELECT count(*) FROM lesson38_shift WHERE available"
                )
                rc_reads += 1
                if rc_reads == 2:
                    rc_ready.set()
                await asyncio.wait_for(rc_ready.wait(), timeout=2)
                if available <= 1:
                    await transaction.commit()
                    return False
                await connection.execute(
                    "UPDATE lesson38_shift SET available=false WHERE courier_id=$1",
                    courier_id,
                )
                await transaction.commit()
                return True
            except BaseException:
                await transaction.rollback()
                raise

        try:
            assert await asyncio.gather(
                leave_under_read_committed(first, courier_a),
                leave_under_read_committed(second, courier_b),
            ) == [True, True]
        finally:
            await first.close()
            await second.close()
        assert await setup.fetchval(
            "SELECT count(*) FROM lesson38_shift WHERE available"
        ) == 0

        await setup.execute("UPDATE lesson38_shift SET available=true")

        # The SERIALIZABLE run starts from the same overlap.  PostgreSQL
        # rejects one first attempt with 40001; run_serializable creates a new
        # Session and repeats the read/decision/write use case from the start.
        engine = create_async_engine(async_url, pool_size=3, max_overflow=0)
        factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        serial_ready = asyncio.Event()
        serial_reads = 0
        calls = {courier_a: 0, courier_b: 0}

        def operation(courier_id: UUID):
            async def leave_shift(session: AsyncSession) -> bool:
                nonlocal serial_reads
                calls[courier_id] += 1
                assert await session.scalar(
                    text("SHOW transaction_isolation")
                ) == "serializable"
                available = await session.scalar(
                    text("SELECT count(*) FROM lesson38_shift WHERE available")
                )
                if calls[courier_id] == 1:
                    serial_reads += 1
                    if serial_reads == 2:
                        serial_ready.set()
                    await asyncio.wait_for(serial_ready.wait(), timeout=2)
                if available <= 1:
                    return False
                await session.execute(
                    text(
                        "UPDATE lesson38_shift SET available=false "
                        "WHERE courier_id=:courier_id"
                    ),
                    {"courier_id": courier_id},
                )
                return True

            return leave_shift

        async def no_delay(_: float) -> None:
            await asyncio.sleep(0)

        try:
            outcomes = await asyncio.gather(
                run_serializable(
                    factory, operation(courier_a), sleep=no_delay,
                    jitter=lambda _start, _end: 0.0,
                ),
                run_serializable(
                    factory, operation(courier_b), sleep=no_delay,
                    jitter=lambda _start, _end: 0.0,
                ),
            )
            assert sorted(outcomes) == [False, True]
            assert sum(calls.values()) == 3
            async with factory() as observer:
                assert await observer.scalar(
                    text("SELECT count(*) FROM lesson38_shift WHERE available")
                ) == 1
        finally:
            await engine.dispose()
    finally:
        await setup.execute("DROP TABLE IF EXISTS lesson38_shift")
        await setup.close()


async def _exercise_pessimistic_queue_locking(async_url: str) -> None:
    """Prove SKIP LOCKED, lock lifetime and rollback on PostgreSQL."""
    from datetime import UTC, datetime

    from sqlalchemy import func, select, update
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import (
        AssignmentRow, OrderRow, OutboxRow, TransitionHistoryRow,
        transactional_session,
    )
    from delivery_service.ports import CourierAlreadyBusy
    from delivery_service.repository import SqlAlchemyOrderRepository

    engine = create_async_engine(async_url, pool_size=4, max_overflow=0)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    order_ids = [
        UUID(f"39000000-0000-0000-0000-{number:012d}")
        for number in range(1, 4)
    ]
    courier_ids = [
        UUID(f"39000000-0000-0000-0001-{number:012d}")
        for number in range(1, 5)
    ]
    customer_id = UUID("39000000-0000-0000-0002-000000000001")

    try:
        async with transactional_session(factory) as session:
            # Earlier integration exercises intentionally leave rows behind.
            # Isolate this queue so the deterministic ORDER BY proves exactly
            # the three rows created for lesson 39.
            await session.execute(
                update(OrderRow)
                .where(OrderRow.status == "created")
                .values(status="cancelled")
            )
            for number, order_id in enumerate(order_ids, start=1):
                session.add(OrderRow(
                    id=order_id,
                    customer_id=customer_id,
                    pickup_address=f"Lock street {number}",
                    destination_address=f"Queue avenue {number}",
                    weight_grams=500,
                    status="created",
                    version=1,
                    created_at=datetime(2026, 4, number, tzinfo=UTC),
                ))

        # Keep the first transaction open while the second worker claims.
        # If SKIP LOCKED is absent, this nested call waits and the timeout
        # fails; if the status update is not atomic, both IDs can match.
        async with factory() as first, factory() as second:
            async with first.begin():
                first_claim = await SqlAlchemyOrderRepository(
                    first
                ).claim_next_created_order(courier_id=courier_ids[0])
                assert first_claim is not None
                async with second.begin():
                    second_claim = await asyncio.wait_for(
                        SqlAlchemyOrderRepository(second).claim_next_created_order(
                            courier_id=courier_ids[1]
                        ),
                        timeout=1,
                    )
                    assert second_claim is not None
                    assert second_claim.id != first_claim.id
            assert {first_claim.id, second_claim.id} == set(order_ids[:2])

        async with factory() as observer:
            assert await observer.scalar(
                select(func.count()).select_from(OrderRow).where(
                    OrderRow.id.in_(order_ids[:2]), OrderRow.status == "assigned"
                )
            ) == 2
            assert await observer.scalar(
                select(func.count()).select_from(TransitionHistoryRow).where(
                    TransitionHistoryRow.order_id.in_(order_ids[:2])
                )
            ) == 2
            assert await observer.scalar(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.payload["aggregate_id"].as_string().in_(
                        [str(order_id) for order_id in order_ids[:2]]
                    )
                )
            ) == 2

        # An ordinary FOR UPDATE on the same row waits until the owner commits.
        async def wait_for_third_lock() -> UUID:
            async with factory() as waiting:
                async with waiting.begin():
                    row = await waiting.scalar(
                        select(OrderRow)
                        .where(OrderRow.id == order_ids[2])
                        .with_for_update()
                    )
                    assert row is not None
                    return row.id

        async with factory() as owner:
            async with owner.begin():
                locked = await owner.scalar(
                    select(OrderRow)
                    .where(OrderRow.id == order_ids[2])
                    .with_for_update()
                )
                assert locked is not None
                waiter = asyncio.create_task(wait_for_third_lock())
                await asyncio.sleep(0.1)
                assert not waiter.done()
            assert await asyncio.wait_for(waiter, timeout=1) == order_ids[2]

        # A failed transaction must restore both the queue status and the
        # assignment.  A new worker can then claim the same order.
        with pytest.raises(RuntimeError, match="force claim rollback"):
            async with transactional_session(factory) as session:
                claimed = await SqlAlchemyOrderRepository(
                    session
                ).claim_next_created_order(courier_id=courier_ids[2])
                assert claimed is not None and claimed.id == order_ids[2]
                raise RuntimeError("force claim rollback")

        async with factory() as observer:
            restored = await observer.get(OrderRow, order_ids[2])
            assert restored is not None and restored.status == "created"
            assert await observer.scalar(
                select(func.count()).select_from(AssignmentRow).where(
                    AssignmentRow.order_id == order_ids[2]
                )
            ) == 0
            assert await observer.scalar(
                select(func.count()).select_from(TransitionHistoryRow).where(
                    TransitionHistoryRow.order_id == order_ids[2]
                )
            ) == 0
            assert await observer.scalar(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.payload["aggregate_id"].as_string() == str(order_ids[2])
                )
            ) == 0

        async with transactional_session(factory) as session:
            reclaimed = await SqlAlchemyOrderRepository(
                session
            ).claim_next_created_order(courier_id=courier_ids[3])
            assert reclaimed is not None and reclaimed.id == order_ids[2]

        # Two independent transactions may lock two different orders for the
        # same courier.  The partial unique index must allow exactly one whole
        # transaction and roll back state/history/outbox for the loser.
        same_courier_order_ids = [
            UUID("39000000-0000-0000-0003-000000000001"),
            UUID("39000000-0000-0000-0003-000000000002"),
        ]
        same_courier_id = UUID("39000000-0000-0000-0004-000000000001")
        async with transactional_session(factory) as session:
            for number, order_id in enumerate(same_courier_order_ids, start=10):
                session.add(OrderRow(
                    id=order_id, customer_id=customer_id,
                    pickup_address=f"Same courier street {number}",
                    destination_address=f"Same courier avenue {number}",
                    weight_grams=500, status="created", version=1,
                    created_at=datetime(2026, 4, number, tzinfo=UTC),
                ))

        same_courier_ready = asyncio.Barrier(2)

        async def claim_for_same_courier():
            async with transactional_session(factory) as session:
                await same_courier_ready.wait()
                return await SqlAlchemyOrderRepository(session).claim_next_created_order(
                    courier_id=same_courier_id,
                )

        same_courier_results = await asyncio.gather(
            claim_for_same_courier(), claim_for_same_courier(),
            return_exceptions=True,
        )
        assert sum(
            isinstance(result, CourierAlreadyBusy) for result in same_courier_results
        ) == 1
        assert sum(hasattr(result, "id") for result in same_courier_results) == 1

        async with factory() as observer:
            assert await observer.scalar(
                select(func.count()).select_from(AssignmentRow).where(
                    AssignmentRow.courier_id == same_courier_id,
                    AssignmentRow.active.is_(True),
                )
            ) == 1
            assert await observer.scalar(
                select(func.count()).select_from(OrderRow).where(
                    OrderRow.id.in_(same_courier_order_ids),
                    OrderRow.status == "assigned",
                )
            ) == 1
            assert await observer.scalar(
                select(func.count()).select_from(TransitionHistoryRow).where(
                    TransitionHistoryRow.order_id.in_(same_courier_order_ids)
                )
            ) == 1
            assert await observer.scalar(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.payload["aggregate_id"].as_string().in_(
                        [str(order_id) for order_id in same_courier_order_ids]
                    )
                )
            ) == 1

        # With one queue row and two different couriers, exactly one claim is
        # visible and the losing attempt creates no durable companion rows.
        single_order_id = UUID("39000000-0000-0000-0005-000000000001")
        async with transactional_session(factory) as session:
            await session.execute(
                update(OrderRow).where(OrderRow.status == "created").values(status="cancelled")
            )
            session.add(OrderRow(
                id=single_order_id, customer_id=customer_id,
                pickup_address="Single order street 1",
                destination_address="Single order avenue 2",
                weight_grams=500, status="created", version=1,
                created_at=datetime(2026, 4, 20, tzinfo=UTC),
            ))

        one_order_ready = asyncio.Barrier(2)

        async def claim_single(courier_id: UUID):
            async with transactional_session(factory) as session:
                await one_order_ready.wait()
                return await SqlAlchemyOrderRepository(session).claim_next_created_order(
                    courier_id=courier_id,
                )

        one_order_results = await asyncio.gather(
            claim_single(UUID("39000000-0000-0000-0006-000000000001")),
            claim_single(UUID("39000000-0000-0000-0006-000000000002")),
        )
        assert sum(result is not None for result in one_order_results) == 1
        async with factory() as observer:
            assert await observer.scalar(
                select(func.count()).select_from(AssignmentRow).where(
                    AssignmentRow.order_id == single_order_id,
                    AssignmentRow.active.is_(True),
                )
            ) == 1
            assert await observer.scalar(
                select(func.count()).select_from(TransitionHistoryRow).where(
                    TransitionHistoryRow.order_id == single_order_id,
                )
            ) == 1
            assert await observer.scalar(
                select(func.count()).select_from(OutboxRow).where(
                    OutboxRow.payload["aggregate_id"].as_string() == str(single_order_id)
                )
            ) == 1
    finally:
        await engine.dispose()


async def _exercise_optimistic_update_race(async_url: str) -> None:
    """Prove two stale snapshots cannot both commit on PostgreSQL."""
    from dataclasses import replace

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import transactional_session
    from delivery_service.domain import DeliveryOrder
    from delivery_service.repository import ConcurrentUpdate, SqlAlchemyOrderRepository

    engine = create_async_engine(async_url, pool_size=3, max_overflow=0)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    original = replace(
        DeliveryOrder.create(
            customer_id=UUID("40000000-0000-0000-0000-000000000001"),
            pickup_address="Version street 1",
            destination_address="Snapshot avenue 2",
            weight_grams=500,
        ),
        id=UUID("40000000-0000-0000-0000-000000000002"),
    )
    snapshots = (
        original.update_details(weight_grams=700),
        original.update_details(weight_grams=900),
    )
    ready = asyncio.Barrier(2)

    async def save(snapshot: DeliveryOrder) -> int:
        async with transactional_session(factory) as session:
            repository = SqlAlchemyOrderRepository(session)
            await ready.wait()
            await repository.save(snapshot, expected_version=original.version)
            return snapshot.weight_grams

    try:
        async with transactional_session(factory) as session:
            await SqlAlchemyOrderRepository(session).add(original)

        results = await asyncio.gather(
            *(asyncio.create_task(save(snapshot)) for snapshot in snapshots),
            return_exceptions=True,
        )
        assert sum(isinstance(item, ConcurrentUpdate) for item in results) == 1
        winner_weight = next(item for item in results if isinstance(item, int))

        async with factory() as observer:
            stored = await SqlAlchemyOrderRepository(observer).get(original.id)
            assert stored is not None
            assert stored.version == original.version + 1
            assert stored.weight_grams == winner_weight
            assert stored.pickup_address == original.pickup_address
            assert stored.destination_address == original.destination_address

        # A caller cannot choose the next revision: both adapters reject a
        # domain object that skips a version before any SQL is emitted.
        forged = replace(original, weight_grams=1000, version=99)
        async with factory() as session:
            with pytest.raises(ValueError, match="advance version exactly once"):
                await SqlAlchemyOrderRepository(session).save(
                    forged, expected_version=original.version + 1,
                )
    finally:
        await engine.dispose()


async def _exercise_idempotency_and_consumer_inbox(async_url: str) -> None:
    """Prove concurrent replay, immutable response snapshot and inbox atomicity."""
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import (
        ConsumerInboxRow,
        IdempotencyRow,
        NotificationRow,
        OrderRow,
        OutboxRow,
        transactional_session,
    )
    from delivery_service.outbox import (
        OutboxEvent,
        WebhookEventConflict,
        accept_payment_webhook_once,
        consume_order_created_once,
    )
    from delivery_service.repository import SqlAlchemyOrderRepository
    from delivery_service.commands import CreateOrderCommand
    from delivery_service.service import OrderService

    engine = create_async_engine(async_url, pool_size=4, max_overflow=0)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    customer_id = UUID("41000000-0000-0000-0000-000000000001")
    command = CreateOrderCommand(
        customer_id=customer_id,
        pickup_address="Retry street 1",
        destination_address="Outbox avenue 2",
        weight_grams=640,
    )
    barrier = asyncio.Barrier(2)

    async def create_from_independent_request():
        async with transactional_session(factory) as session:
            await barrier.wait()
            return await OrderService(SqlAlchemyOrderRepository(session)).create(
                command, idempotency_key="lesson-41-concurrent"
            )

    try:
        first, second = await asyncio.gather(
            create_from_independent_request(), create_from_independent_request()
        )
        assert first == second

        async with factory() as observer:
            assert await observer.scalar(select(func.count()).select_from(OrderRow).where(
                OrderRow.customer_id == customer_id
            )) == 1
            assert await observer.scalar(select(func.count()).select_from(OutboxRow).where(
                OutboxRow.payload["aggregate_id"].as_string() == str(first.id)
            )) == 1
            assert await observer.scalar(select(func.count()).select_from(IdempotencyRow).where(
                IdempotencyRow.resource_id == first.id
            )) == 1

        async with transactional_session(factory) as session:
            repository = SqlAlchemyOrderRepository(session)
            changed = first.update_details(weight_grams=720)
            await repository.save(changed, expected_version=first.version)

        async with transactional_session(factory) as session:
            replayed = await OrderService(SqlAlchemyOrderRepository(session)).create(
                command, idempotency_key="lesson-41-concurrent"
            )
            assert replayed == first
            assert replayed.version == 1 and replayed.weight_grams == 640

        async with factory() as observer:
            current = await SqlAlchemyOrderRepository(observer).get(first.id)
            assert current is not None
            assert current.version == 2 and current.weight_grams == 720

        event = OutboxEvent(
            UUID("41000000-0000-0000-0000-000000000010"),
            "order.created",
            {"order_id": str(first.id)},
        )
        async with transactional_session(factory) as session:
            assert await consume_order_created_once(session, event) is True
        async with transactional_session(factory) as session:
            assert await consume_order_created_once(session, event) is False

        failed_event = OutboxEvent(
            UUID("41000000-0000-0000-0000-000000000011"),
            "order.created",
            {},
        )
        with pytest.raises(KeyError):
            async with transactional_session(factory) as session:
                await consume_order_created_once(session, failed_event)

        recovered_event = OutboxEvent(failed_event.id, "order.created", {"order_id": str(first.id)})
        async with transactional_session(factory) as session:
            assert await consume_order_created_once(session, recovered_event) is True

        async with factory() as observer:
            assert await observer.scalar(select(func.count()).select_from(ConsumerInboxRow)) == 2
            assert await observer.scalar(select(func.count()).select_from(NotificationRow)) == 2

        webhook_id = UUID("41000000-0000-0000-0000-000000000020")
        webhook_payload = {
            "event_id": str(webhook_id),
            "event_type": "payment.completed",
            "order_id": str(first.id),
            "payment_id": "pay-postgres-proof",
            "payload_sha256": "a" * 64,
        }
        async with transactional_session(factory) as session:
            assert await accept_payment_webhook_once(
                session, event_id=webhook_id, event_payload=webhook_payload,
            ) is True
        async with transactional_session(factory) as session:
            assert await accept_payment_webhook_once(
                session, event_id=webhook_id, event_payload=webhook_payload,
            ) is False

        conflicting_payload = {**webhook_payload, "payload_sha256": "b" * 64}
        with pytest.raises(WebhookEventConflict):
            async with transactional_session(factory) as session:
                await accept_payment_webhook_once(
                    session, event_id=webhook_id, event_payload=conflicting_payload,
                )

        async with factory() as observer:
            inbox_rows = await observer.scalar(
                select(func.count()).select_from(ConsumerInboxRow).where(
                    ConsumerInboxRow.event_id == webhook_id
                )
            )
            webhook_outbox_rows = await observer.scalar(
                select(func.count()).select_from(OutboxRow).where(OutboxRow.id == webhook_id)
            )
            stored_webhook = await observer.get(OutboxRow, webhook_id)
            assert inbox_rows == 1
            assert webhook_outbox_rows == 1
            assert stored_webhook is not None
            assert stored_webhook.payload == webhook_payload
    finally:
        await engine.dispose()


async def _exercise_refresh_rotation_race(async_url: str) -> None:
    """Prove one refresh wins and reuse atomically revokes the active family."""
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import RefreshSessionRow, transactional_session
    from delivery_service.refresh_sessions import (
        RefreshSession,
        RotationResult,
        SqlAlchemyRefreshSessionRepository,
    )

    engine = create_async_engine(async_url, pool_size=3, max_overflow=0)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime.now(UTC)
    family = UUID("45000000-0000-0000-0000-000000000001")
    subject = "customer:45000000-0000-0000-0000-000000000002"
    current = RefreshSession(
        UUID("45000000-0000-0000-0000-000000000003"),
        family,
        subject,
        now + timedelta(days=7),
    )
    replacements = (
        RefreshSession(
            UUID("45000000-0000-0000-0000-000000000004"),
            family,
            subject,
            now + timedelta(days=7),
        ),
        RefreshSession(
            UUID("45000000-0000-0000-0000-000000000005"),
            family,
            subject,
            now + timedelta(days=7),
        ),
    )
    barrier = asyncio.Barrier(2)

    async def rotate(replacement: RefreshSession) -> RotationResult:
        async with transactional_session(factory) as session:
            await barrier.wait()
            return await SqlAlchemyRefreshSessionRepository(session).rotate(
                presented_jti=current.jti,
                family_id=family,
                subject=subject,
                replacement=replacement,
                now=now,
            )

    try:
        async with transactional_session(factory) as session:
            await SqlAlchemyRefreshSessionRepository(session).register(current)

        results = await asyncio.gather(*(rotate(item) for item in replacements))
        assert sorted(results) == sorted([RotationResult.ROTATED, RotationResult.REUSED])

        async with factory() as observer:
            assert await observer.scalar(
                select(func.count()).select_from(RefreshSessionRow).where(
                    RefreshSessionRow.family_id == family,
                )
            ) == 2
            assert await observer.scalar(
                select(func.count()).select_from(RefreshSessionRow).where(
                    RefreshSessionRow.family_id == family,
                    RefreshSessionRow.revoked_at.is_not(None),
                )
            ) == 2
    finally:
        await engine.dispose()


async def _exercise_outbox_relay(async_url: str) -> None:
    """Prove concurrent SKIP LOCKED batches and at-least-once crash semantics."""
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import delete, select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import OutboxRow, transactional_session
    from delivery_service.outbox import OutboxEvent, publish_pending_outbox

    engine = create_async_engine(async_url, pool_size=4, max_overflow=0)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    base_time = datetime(2026, 8, 14, 6, 0, tzinfo=UTC)
    event_ids = [UUID(f"59000000-0000-0000-0000-{number:012d}") for number in range(1, 5)]

    class CoordinatedPublisher:
        def __init__(self, barrier: asyncio.Barrier, published: list[UUID]) -> None:
            self.barrier = barrier
            self.published = published
            self.first = True

        async def publish(self, event: OutboxEvent) -> None:
            self.published.append(event.id)
            if self.first:
                self.first = False
                await self.barrier.wait()

    try:
        async with transactional_session(factory) as session:
            await session.execute(delete(OutboxRow))
            for offset, event_id in enumerate(event_ids):
                session.add(OutboxRow(
                    id=event_id,
                    event_type="order.created",
                    payload={"schema_version": 1, "aggregate_id": str(event_id)},
                    created_at=base_time + timedelta(seconds=offset),
                ))

        barrier = asyncio.Barrier(2)
        published: list[UUID] = []

        async def run_worker() -> list[UUID]:
            async with transactional_session(factory) as session:
                return await publish_pending_outbox(
                    session,
                    CoordinatedPublisher(barrier, published),
                    published_at=base_time + timedelta(minutes=1),
                    batch_size=2,
                )

        claimed_batches = await asyncio.gather(run_worker(), run_worker())
        assert all(len(batch) == 2 for batch in claimed_batches)
        assert len(published) == len(set(published)) == 4
        assert set(published) == set(event_ids)

        async with factory() as observer:
            rows = list((await observer.scalars(
                select(OutboxRow).order_by(OutboxRow.created_at, OutboxRow.id)
            )).all())
            assert [row.id for row in rows] == event_ids
            assert all(
                row.published_at is not None
                and row.legacy_processed_at == row.published_at
                for row in rows
            )

        failed_id = UUID("59000000-0000-0000-0000-000000000099")
        async with transactional_session(factory) as session:
            session.add(OutboxRow(
                id=failed_id,
                event_type="order.created",
                payload={"schema_version": 1, "aggregate_id": str(failed_id)},
                created_at=base_time + timedelta(minutes=2),
            ))

        attempts: list[UUID] = []

        class CrashAfterBrokerConfirmation:
            async def publish(self, event: OutboxEvent) -> None:
                attempts.append(event.id)
                raise RuntimeError("relay crashed before database commit")

        with pytest.raises(RuntimeError, match="before database commit"):
            async with transactional_session(factory) as session:
                await publish_pending_outbox(
                    session,
                    CrashAfterBrokerConfirmation(),
                    published_at=base_time + timedelta(minutes=3),
                    batch_size=1,
                )

        async with factory() as observer:
            assert (await observer.get(OutboxRow, failed_id)).published_at is None

        class RecoveryPublisher:
            async def publish(self, event: OutboxEvent) -> None:
                attempts.append(event.id)

        async with transactional_session(factory) as session:
            recovered = await publish_pending_outbox(
                session,
                RecoveryPublisher(),
                published_at=base_time + timedelta(minutes=4),
                batch_size=1,
            )
            assert recovered == [failed_id]
        assert attempts == [failed_id, failed_id]
    finally:
        await engine.dispose()


async def _exercise_proof_metadata_and_notification_delivery(async_url: str) -> None:
    """Prove metadata/blob separation, preferences and provider redelivery."""
    from datetime import UTC, datetime

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from delivery_service.db import (
        ConsumerInboxRow,
        NotificationPreferenceRow,
        NotificationRow,
        OrderRow,
        ProofMetadataRow,
        transactional_session,
    )
    from delivery_service.notifications import (
        InMemoryIdempotentNotificationProvider,
        dispatch_order_created_notification_once,
    )
    from delivery_service.storage import SqlAlchemyProofMetadataRepository, StoredProof

    engine = create_async_engine(async_url, pool_size=3, max_overflow=0)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime(2026, 8, 14, 8, 30, tzinfo=UTC)
    tenant_id = UUID("60000000-0000-0000-0000-000000000001")
    customer_id = UUID("60000000-0000-0000-0000-000000000002")
    order_id = UUID("60000000-0000-0000-0000-000000000003")
    digest = "6" * 64
    proof = StoredProof(
        key=f"proofs/{tenant_id}/{order_id}/{digest}.png",
        tenant_id=tenant_id,
        order_id=order_id,
        media_type="image/png",
        size=128,
        sha256=digest,
    )
    payload = {
        "schema_version": 1,
        "aggregate_id": str(order_id),
        "tenant_id": str(tenant_id),
        "customer_id": str(customer_id),
    }
    provider = InMemoryIdempotentNotificationProvider()

    try:
        async with transactional_session(factory) as session:
            session.add(OrderRow(
                id=order_id,
                tenant_id=tenant_id,
                customer_id=customer_id,
                pickup_address="Metadata street",
                destination_address="Object avenue",
                weight_grams=500,
                status="created",
                version=1,
                created_at=now,
            ))
            await session.flush()
            metadata = SqlAlchemyProofMetadataRepository(session)
            await metadata.add(proof, proof_kind="delivery", created_at=now)

        async with factory() as observer:
            stored = await SqlAlchemyProofMetadataRepository(observer).find(
                tenant_id=tenant_id, order_id=order_id, sha256=digest
            )
            assert stored == proof
            assert "content" not in ProofMetadataRow.__table__.columns

        event_id = UUID("60000000-0000-0000-0000-000000000010")
        async with transactional_session(factory) as session:
            assert await dispatch_order_created_notification_once(
                session,
                event_id=event_id,
                event_type="order.created",
                payload=payload,
                provider=provider,
                now=now,
            )
        async with transactional_session(factory) as session:
            assert not await dispatch_order_created_notification_once(
                session,
                event_id=event_id,
                event_type="order.created",
                payload=payload,
                provider=provider,
                now=now,
            )
        assert provider.attempts == [str(event_id)]

        disabled_id = UUID("60000000-0000-0000-0000-000000000011")
        async with transactional_session(factory) as session:
            session.add(NotificationPreferenceRow(
                tenant_id=tenant_id,
                user_id=customer_id,
                channel="email",
                enabled=False,
            ))
        async with transactional_session(factory) as session:
            assert await dispatch_order_created_notification_once(
                session,
                event_id=disabled_id,
                event_type="order.created",
                payload=payload,
                provider=provider,
                now=now,
            )
        async with factory() as observer:
            skipped = await observer.scalar(
                select(NotificationRow).where(NotificationRow.event_id == disabled_id)
            )
            assert skipped is not None and skipped.status == "skipped"
        assert provider.attempts == [str(event_id)]

        crash_id = UUID("60000000-0000-0000-0000-000000000012")
        async with transactional_session(factory) as session:
            preference = await session.scalar(
                select(NotificationPreferenceRow).where(
                    NotificationPreferenceRow.tenant_id == tenant_id,
                    NotificationPreferenceRow.user_id == customer_id,
                    NotificationPreferenceRow.channel == "email",
                )
            )
            assert preference is not None
            preference.enabled = True

        class CrashAfterProviderAcceptance:
            async def send(self, **kwargs):
                await provider.send(**kwargs)
                raise RuntimeError("crash after provider acceptance")

        with pytest.raises(RuntimeError, match="provider acceptance"):
            async with transactional_session(factory) as session:
                await dispatch_order_created_notification_once(
                    session,
                    event_id=crash_id,
                    event_type="order.created",
                    payload=payload,
                    provider=CrashAfterProviderAcceptance(),
                    now=now,
                )
        async with factory() as observer:
            assert await observer.get(ConsumerInboxRow, crash_id) is None
            assert await observer.scalar(
                select(NotificationRow).where(NotificationRow.event_id == crash_id)
            ) is None

        # Redelivery repeats the same provider key; the provider returns the
        # original receipt and the local transaction can finally commit.
        async with transactional_session(factory) as session:
            assert await dispatch_order_created_notification_once(
                session,
                event_id=crash_id,
                event_type="order.created",
                payload=payload,
                provider=provider,
                now=now,
            )
        assert provider.attempts[-2:] == [str(crash_id), str(crash_id)]
        assert len([key for key in provider.receipts if key == str(crash_id)]) == 1
    finally:
        await engine.dispose()


def test_postgres_16_enforces_delivery_schema_invariants(
    postgres_admin_url: str,
) -> None:
    async def scenario(url: str) -> None:
        parsed = urlsplit(url)
        database_name = f"delivery_course_{os.getpid()}"
        admin = await asyncpg.connect(url)
        try:
            assert await admin.fetchval("SHOW server_version_num") >= "160000"
            await admin.execute(f'CREATE DATABASE "{database_name}"')
            migrated_url = urlunsplit((
                parsed.scheme, parsed.netloc, f"/{database_name}", parsed.query, parsed.fragment,
            ))
            async_url = migrated_url.replace("postgresql://", "postgresql+asyncpg://", 1)
            reference_root = Path(__file__).resolve().parents[2]
            environment = {**os.environ, "DELIVERY_DATABASE_URL": async_url}

            def migrate(*arguments: str) -> None:
                migration = subprocess.run(
                    [
                        sys.executable, "-m", "alembic", "-c", "alembic.ini",
                        *arguments,
                    ],
                    cwd=reference_root,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                assert migration.returncode == 0, migration.stderr

            # Prove the real graph instead of checking only that migration
            # functions exist.  The row must survive 0001 -> head -> 0001 ->
            # head while operational indexes and tenant columns appear and disappear.
            migrate("upgrade", "0001")
            migration_probe = await asyncpg.connect(migrated_url)
            migration_order = UUID("00000000-0000-0000-0000-000000000035")
            migration_customer = UUID("00000000-0000-0000-0000-000000000036")
            try:
                assert await migration_probe.fetchval(
                    "SELECT version_num FROM alembic_version"
                ) == "0001"
                assert await migration_probe.fetchval(
                    "SELECT to_regclass('ix_delivery_order_status_created') IS NULL"
                )
                await migration_probe.execute(
                    """INSERT INTO delivery_order
                    (id, customer_id, pickup_address, destination_address,
                     weight_grams, status, version, created_at)
                    VALUES($1,$2,'Migration street','Revision avenue',500,'created',1,now())""",
                    migration_order, migration_customer,
                )
            finally:
                await migration_probe.close()

            migrate("upgrade", "head")
            migration_probe = await asyncpg.connect(migrated_url)
            try:
                assert await migration_probe.fetchval(
                    "SELECT version_num FROM alembic_version"
                ) == "0007"
                assert await migration_probe.fetchval(
                    "SELECT to_regclass('ix_delivery_order_status_created') IS NOT NULL"
                )
                assert await migration_probe.fetchval(
                    "SELECT to_regclass('ix_delivery_order_tenant_id') IS NOT NULL"
                )
                assert await migration_probe.fetchval(
                    "SELECT tenant_id IS NOT NULL FROM delivery_order WHERE id=$1",
                    migration_order,
                )
            finally:
                await migration_probe.close()

            migrate("downgrade", "0001")
            migration_probe = await asyncpg.connect(migrated_url)
            try:
                assert await migration_probe.fetchval(
                    "SELECT version_num FROM alembic_version"
                ) == "0001"
                assert await migration_probe.fetchval(
                    "SELECT to_regclass('ix_delivery_order_status_created') IS NULL"
                )
                assert await migration_probe.fetchval(
                    "SELECT count(*) FROM delivery_order WHERE id=$1", migration_order,
                ) == 1
            finally:
                await migration_probe.close()

            migrate("upgrade", "head")
            migrate("check")
            connection = await asyncpg.connect(migrated_url)
            courier = UUID("00000000-0000-0000-0000-000000000001")
            other_courier = UUID("00000000-0000-0000-0000-000000000002")
            customer = UUID("00000000-0000-0000-0000-000000000031")
            order_one = UUID("00000000-0000-0000-0000-000000000021")
            order_two = UUID("00000000-0000-0000-0000-000000000022")
            try:
                constraints = await connection.fetch(
                    """SELECT conname FROM pg_constraint
                       WHERE conrelid IN ('delivery_order'::regclass, 'courier_assignment'::regclass)"""
                )
                assert {
                    "ck_order_weight", "ck_order_status", "ck_order_version",
                    "fk_assignment_order",
                } <= {record["conname"] for record in constraints}
                assert await connection.fetchval(
                    "SELECT to_regclass('uq_active_assignment_order') IS NOT NULL"
                )
                assert await connection.fetchval(
                    "SELECT to_regclass('uq_active_assignment_courier') IS NOT NULL"
                )
                await connection.execute(
                    """INSERT INTO delivery_order
                    (id, customer_id, pickup_address, destination_address, weight_grams, status, version, created_at)
                    VALUES ($1,$3,'A street','B street',100,'created',1,now()),
                           ($2,$3,'C street','D street',200,'created',1,now())""",
                    order_one, order_two, customer,
                )
                with pytest.raises(asyncpg.CheckViolationError):
                    await connection.execute(
                        """INSERT INTO delivery_order
                        (id, customer_id, pickup_address, destination_address, weight_grams, status, version, created_at)
                        VALUES($1,$2,'A','B',0,'created',1,now())""",
                        UUID("00000000-0000-0000-0000-000000000023"), customer,
                    )
                with pytest.raises(asyncpg.CheckViolationError):
                    await connection.execute(
                        """INSERT INTO delivery_order
                        (id, customer_id, pickup_address, destination_address, weight_grams, status, version, created_at)
                        VALUES($1,$2,'A','B',100,'unknown',1,now())""",
                        UUID("00000000-0000-0000-0000-000000000024"), customer,
                    )
                with pytest.raises(asyncpg.CheckViolationError):
                    await connection.execute(
                        """INSERT INTO delivery_order
                        (id, customer_id, pickup_address, destination_address, weight_grams, status, version, created_at)
                        VALUES($1,$2,'A','B',100,'created',0,now())""",
                        UUID("00000000-0000-0000-0000-000000000025"), customer,
                    )
                with pytest.raises(asyncpg.ForeignKeyViolationError):
                    await connection.execute(
                        "INSERT INTO courier_assignment(id, order_id, courier_id) VALUES($1, $2, $3)",
                        UUID("00000000-0000-0000-0000-000000000013"),
                        UUID("00000000-0000-0000-0000-000000009999"), courier,
                    )
                await connection.execute(
                    "INSERT INTO courier_assignment(id, order_id, courier_id) VALUES($1, $2, $3)",
                    UUID("00000000-0000-0000-0000-000000000011"),
                    order_one, courier,
                )
                with pytest.raises(asyncpg.UniqueViolationError):
                    await connection.execute(
                        "INSERT INTO courier_assignment(id, order_id, courier_id) VALUES($1, $2, $3)",
                        UUID("00000000-0000-0000-0000-000000000012"),
                        order_two, courier,
                    )
                with pytest.raises(asyncpg.UniqueViolationError):
                    await connection.execute(
                        "INSERT INTO courier_assignment(id, order_id, courier_id) VALUES($1, $2, $3)",
                        UUID("00000000-0000-0000-0000-000000000014"),
                        order_one, other_courier,
                    )
                await connection.execute(
                    "UPDATE courier_assignment SET active=false WHERE order_id=$1", order_one
                )
                await connection.execute(
                    "INSERT INTO courier_assignment(id, order_id, courier_id) VALUES($1, $2, $3)",
                    UUID("00000000-0000-0000-0000-000000000015"),
                    order_two, courier,
                )
                await connection.execute(
                    "DELETE FROM delivery_order WHERE id=$1", order_two
                )
                assert await connection.fetchval(
                    "SELECT count(*) FROM courier_assignment WHERE order_id=$1", order_two
                ) == 0
            finally:
                await connection.close()
            await _exercise_session_lifecycle(async_url)
            await _exercise_repository_crud(async_url)
            await _exercise_tracking_snapshot_atomicity(async_url)
            await _exercise_query_loading_and_index_plan(async_url, migrated_url)
            await _exercise_atomic_order_unit_of_work(async_url)
            await _exercise_transition_history_atomicity(async_url)
            await _exercise_isolation_levels(async_url, migrated_url)
            await _exercise_pessimistic_queue_locking(async_url)
            await _exercise_optimistic_update_race(async_url)
            await _exercise_idempotency_and_consumer_inbox(async_url)
            await _exercise_outbox_relay(async_url)
            await _exercise_proof_metadata_and_notification_delivery(async_url)
            await _exercise_refresh_rotation_race(async_url)
        finally:
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1", database_name
            )
            await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
            await admin.close()

    asyncio.run(scenario(postgres_admin_url))
