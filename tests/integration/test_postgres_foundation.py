from __future__ import annotations

import asyncio
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def test_clean_postgres_runs_migrations_and_separates_commit_from_rollback(
    postgres_admin_url: str,
) -> None:
    async def scenario(admin_url: str) -> None:
        parsed = urlsplit(admin_url)
        database_name = f"delivery_foundation_{os.getpid()}_{uuid4().hex}"
        admin = await asyncpg.connect(admin_url)
        engine = None
        try:
            version = int(await admin.fetchval("SHOW server_version_num"))
            assert 160000 <= version < 170000
            await admin.execute(f'CREATE DATABASE "{database_name}"')
            database_url = urlunsplit((
                parsed.scheme,
                parsed.netloc,
                f"/{database_name}",
                parsed.query,
                parsed.fragment,
            ))
            async_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1,
            )
            reference_root = Path(__file__).resolve().parents[2]
            environment = {**os.environ, "DELIVERY_DATABASE_URL": async_url}
            migration = subprocess.run(
                [sys.executable, "-m", "alembic", "-c", "alembic.ini", "upgrade", "head"],
                cwd=reference_root,
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert migration.returncode == 0, migration.stderr

            probe = await asyncpg.connect(database_url)
            try:
                assert await probe.fetchval(
                    "SELECT version_num FROM alembic_version"
                ) == "0007"
                assert await probe.fetchval(
                    "SELECT to_regclass('delivery_order') IS NOT NULL"
                )
                assert await probe.fetchval(
                    "SELECT to_regclass('uq_active_assignment_courier') IS NOT NULL"
                )
            finally:
                await probe.close()

            engine = create_async_engine(async_url, pool_size=2, max_overflow=0)
            committed_id = UUID("53000000-0000-0000-0000-000000000001")
            rolled_back_id = UUID("53000000-0000-0000-0000-000000000002")
            customer_id = UUID("53000000-0000-0000-0000-000000000003")
            insert = text(
                """INSERT INTO delivery_order
                (id, customer_id, pickup_address, destination_address,
                 weight_grams, status, version, created_at)
                VALUES (:id, :customer_id, 'Testcontainers street 1',
                        'Migration avenue 2', 750, 'created', 1, now())"""
            )

            async with engine.begin() as connection:
                await connection.execute(insert, {
                    "id": committed_id, "customer_id": customer_id,
                })

            with pytest.raises(RuntimeError, match="force rollback"):
                async with engine.begin() as connection:
                    await connection.execute(insert, {
                        "id": rolled_back_id, "customer_id": customer_id,
                    })
                    raise RuntimeError("force rollback")

            async with engine.connect() as observer:
                assert await observer.scalar(text(
                    "SELECT count(*) FROM delivery_order WHERE id=:id"
                ), {"id": committed_id}) == 1
                assert await observer.scalar(text(
                    "SELECT count(*) FROM delivery_order WHERE id=:id"
                ), {"id": rolled_back_id}) == 0
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
