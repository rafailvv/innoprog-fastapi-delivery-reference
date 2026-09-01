from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.ext.asyncio import async_engine_from_config

from delivery_service.config import get_settings
from delivery_service.db import Base


config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name, disable_existing_loggers=False)
config.set_main_option("sqlalchemy.url", get_settings().database_url)
target_metadata = Base.metadata
MIGRATION_ADVISORY_LOCK_ID = 6_071_933_001


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        # The transaction-scoped lock is released by PostgreSQL together with
        # the migration commit or rollback. Acquiring a session lock before
        # Alembic starts its transaction would trigger SQLAlchemy autobegin and
        # could leave all DDL in an outer transaction that is later rolled back.
        connection.execute(
            text("SELECT pg_advisory_xact_lock(:key)"),
            {"key": MIGRATION_ADVISORY_LOCK_ID},
        )
        context.run_migrations()


async def run_migrations_online() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    try:
        async with connectable.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
