from __future__ import annotations

import asyncio
from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


def test_two_independent_transactions_preserve_optimistic_invariant(
    postgres_admin_url: str,
) -> None:
    async def scenario(admin_url: str) -> None:
        from delivery_service.db import OrderRow, transactional_session
        from delivery_service.domain import DeliveryOrder
        from delivery_service.repository import ConcurrentUpdate, SqlAlchemyOrderRepository

        parsed = urlsplit(admin_url)
        database_name = f"delivery_race_{os.getpid()}_{uuid4().hex}"
        admin = await asyncpg.connect(admin_url)
        engine = None
        try:
            await admin.execute(f'CREATE DATABASE "{database_name}"')
            database_url = urlunsplit((
                parsed.scheme,
                parsed.netloc,
                f"/{database_name}",
                parsed.query,
                parsed.fragment,
            ))
            async_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
            reference_root = Path(__file__).resolve().parents[2]
            migration = subprocess.run(
                [sys.executable, "-m", "alembic", "-c", "alembic.ini", "upgrade", "head"],
                cwd=reference_root,
                env={**os.environ, "DELIVERY_DATABASE_URL": async_url},
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert migration.returncode == 0, migration.stderr

            engine = create_async_engine(async_url, pool_size=3, max_overflow=0)
            sessions = async_sessionmaker(
                engine, class_=AsyncSession, expire_on_commit=False,
            )
            original = replace(
                DeliveryOrder.create(
                    customer_id=UUID("54000000-0000-0000-0000-000000000010"),
                    pickup_address="Version street 1",
                    destination_address="Snapshot avenue 2",
                    weight_grams=500,
                ),
                id=UUID("54000000-0000-0000-0000-000000000011"),
            )
            ready_to_write = asyncio.Barrier(2)

            async with transactional_session(sessions) as session:
                await SqlAlchemyOrderRepository(session).add(original)

            async def stale_update(weight_grams: int) -> int:
                async with transactional_session(sessions) as session:
                    repository = SqlAlchemyOrderRepository(session)
                    stale = await repository.get(original.id)
                    assert stale is not None and stale.version == 1
                    changed = stale.update_details(weight_grams=weight_grams)
                    await ready_to_write.wait()
                    await repository.save(changed, expected_version=stale.version)
                    return weight_grams

            results = await asyncio.gather(
                asyncio.create_task(stale_update(700)),
                asyncio.create_task(stale_update(900)),
                return_exceptions=True,
            )
            conflicts = [item for item in results if isinstance(item, ConcurrentUpdate)]
            winners = [item for item in results if isinstance(item, int)]
            assert len(conflicts) == 1
            assert len(winners) == 1

            async with sessions() as observer:
                stored = await SqlAlchemyOrderRepository(observer).get(original.id)
                assert stored is not None
                assert stored.version == 2
                assert stored.weight_grams == winners[0]
                assert await observer.scalar(
                    select(func.count()).select_from(OrderRow).where(OrderRow.id == original.id)
                ) == 1
        finally:
            if engine is not None:
                await engine.dispose()
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname=$1 AND pid <> pg_backend_pid()",
                database_name,
            )
            await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
            await admin.close()

    asyncio.run(scenario(postgres_admin_url))
