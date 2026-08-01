from __future__ import annotations

from datetime import timedelta
import logging
from uuid import uuid4

from fastapi.testclient import TestClient

from delivery_service.main import app, get_order_service
from delivery_service.security import create_token
from tests.override_tools import dependency_override


def auth(customer_id) -> dict[str, str]:
    token = create_token(
        f"customer:{customer_id}", token_type="access", lifetime=timedelta(minutes=5)
    )
    return {"Authorization": f"Bearer {token}"}


def test_missing_order_uses_one_public_error_contract() -> None:
    customer_id = uuid4()
    with TestClient(app) as client:
        response = client.get(
            f"/api/v1/orders/{uuid4()}",
            headers={**auth(customer_id), "X-Request-ID": "missing-order-20"},
        )

    assert response.status_code == 404
    assert response.json() == {
        "error_code": "order_not_found",
        "message": "Order was not found",
        "correlation_id": "missing-order-20",
        "details": [],
    }
    assert response.headers["X-Request-ID"] == "missing-order-20"


def test_framework_http_error_uses_the_same_shape() -> None:
    with TestClient(app) as client:
        response = client.get(
            "/route-that-does-not-exist",
            headers={"X-Request-ID": "missing-route-20"},
        )

    assert response.status_code == 404
    assert response.json() == {
        "error_code": "resource_not_found",
        "message": "Resource was not found",
        "correlation_id": "missing-route-20",
        "details": [],
    }
    assert response.headers["X-Request-ID"] == "missing-route-20"
    assert "detail" not in response.json()


class BrokenService:
    async def get(self, _order_id):
        raise RuntimeError("super-secret internal failure")


def test_unknown_error_is_safe_and_observable(caplog) -> None:
    customer_id = uuid4()
    caplog.set_level(logging.ERROR, logger="delivery_service")
    from delivery_service.observability import logger

    logger.addHandler(caplog.handler)
    try:
        with dependency_override(app, get_order_service, lambda: BrokenService()):
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.get(
                    f"/api/v1/orders/{uuid4()}",
                    headers={**auth(customer_id), "X-Request-ID": "failure-20"},
                )
    finally:
        logger.removeHandler(caplog.handler)
    assert response.status_code == 500
    assert response.json() == {
        "error_code": "internal_error",
        "message": "The service could not complete the request",
        "correlation_id": "failure-20",
        "details": [],
    }
    assert response.headers["X-Request-ID"] == "failure-20"
    assert "super-secret" not in response.text
    assert "Traceback" not in response.text
    assert any(
        record.message == "unhandled_request_error"
        and getattr(record, "correlation_id", None) == "failure-20"
        and record.exc_info is not None
        for record in caplog.records
    )


def test_openapi_documents_shared_error_model() -> None:
    with TestClient(app) as client:
        schema = client.get("/openapi.json").json()

    responses = schema["paths"]["/api/v1/orders"]["post"]["responses"]
    assert responses["422"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "/ErrorResponse"
    )
    assert responses["500"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "/ErrorResponse"
    )
