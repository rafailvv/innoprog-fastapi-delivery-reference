from __future__ import annotations

import os
from pathlib import Path

import pytest


POSTGRES_IMAGE = "postgres:16.14-alpine"


@pytest.fixture(scope="session")
def postgres_admin_url() -> str:
    """Own one PostgreSQL 16 process; each test still owns a fresh database."""
    configured = os.getenv("DELIVERY_TEST_DATABASE_URL")
    if configured:
        yield configured
        return

    docker_socket = Path("/var/run/docker.sock")
    if not docker_socket.exists() and not os.getenv("DOCKER_HOST"):
        pytest.skip("trusted PostgreSQL integration job requires a Docker endpoint")

    postgres_module = pytest.importorskip("testcontainers.postgres")
    with postgres_module.PostgresContainer(POSTGRES_IMAGE) as postgres:
        yield postgres.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql://",
        )
