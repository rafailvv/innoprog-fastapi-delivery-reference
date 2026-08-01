from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Timeout


@asynccontextmanager
async def asgi_client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Run application lifespan around one bounded in-process HTTP client."""
    async with LifespanManager(app) as manager:
        transport = ASGITransport(
            app=manager.app,
            raise_app_exceptions=True,
            client=("127.0.0.1", 50000),
        )
        async with AsyncClient(
            transport=transport,
            base_url="http://testserver",
            timeout=Timeout(2.0),
            follow_redirects=False,
        ) as client:
            yield client
