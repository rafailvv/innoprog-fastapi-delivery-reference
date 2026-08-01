"""One lifecycle-owned HTTPX client for the route-service boundary."""

from __future__ import annotations

import httpx

from delivery_service.config import Settings
from delivery_service.observability import inject_request_context


def build_route_http_client(
    settings: Settings,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Build the client once in lifespan; adapters only receive the instance."""

    timeout = httpx.Timeout(
        connect=settings.route_connect_timeout_seconds,
        pool=settings.route_pool_timeout_seconds,
        write=settings.route_timeout_seconds,
        read=settings.route_timeout_seconds,
    )
    limits = httpx.Limits(
        max_connections=settings.route_max_connections,
        max_keepalive_connections=settings.route_max_keepalive_connections,
        keepalive_expiry=settings.route_keepalive_expiry_seconds,
    )
    return httpx.AsyncClient(
        base_url=settings.route_service_url,
        headers={
            "Authorization": f"Bearer {settings.route_api_key.get_secret_value()}",
            "Accept": "application/json",
        },
        timeout=timeout,
        limits=limits,
        event_hooks={"request": [inject_request_context]},
        transport=transport,
    )
