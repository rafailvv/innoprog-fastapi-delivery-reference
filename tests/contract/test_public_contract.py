from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from fastapi.testclient import TestClient

from delivery_service.main import app, get_order_service
from delivery_service.security import create_token
from tests.override_tools import dependency_override


def _auth(customer_id) -> dict[str, str]:
    token = create_token(
        f"customer:{customer_id}",
        token_type="access",
        lifetime=timedelta(minutes=5),
    )
    return {"Authorization": f"Bearer {token}"}


class _BrokenOrderService:
    async def get(self, _order_id):
        raise RuntimeError("secret database detail must not cross the HTTP boundary")


def test_create_order_openapi_contract_is_stable() -> None:
    operation = app.openapi()["paths"]["/api/v1/orders"]["post"]

    assert operation["operationId"] == "createOrderV1"
    assert operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/OrderCreate"
    }
    created = operation["responses"]["201"]
    assert created["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/OrderRead"
    }
    assert "Location" in created["headers"]
    assert operation["security"] == [{"OAuth2PasswordBearer": []}]
    for status_code in ("422", "500"):
        assert operation["responses"][status_code]["content"][
            "application/json"
        ]["schema"] == {"$ref": "#/components/schemas/ErrorResponse"}


def test_runtime_errors_match_documented_error_contract() -> None:
    customer_id = uuid4()
    with TestClient(app) as client:
        invalid = client.post(
            "/api/v1/orders",
            headers={
                **_auth(customer_id),
                "Idempotency-Key": "contract-invalid-order",
                "X-Request-ID": "contract-422",
            },
            json={"customer_id": str(customer_id), "weight_grams": -1},
        )
        missing = client.get(
            f"/api/v1/orders/{uuid4()}",
            headers={**_auth(customer_id), "X-Request-ID": "contract-404"},
        )
    with dependency_override(app, get_order_service, lambda: _BrokenOrderService()):
        with TestClient(app, raise_server_exceptions=False) as client:
            unexpected = client.get(
                f"/api/v1/orders/{uuid4()}",
                headers={**_auth(customer_id), "X-Request-ID": "contract-500"},
            )

    assert invalid.status_code == 422
    assert invalid.json()["error_code"] == "request_invalid"
    assert invalid.json()["correlation_id"] == "contract-422"
    assert invalid.headers["X-Request-ID"] == "contract-422"
    assert isinstance(invalid.json()["details"], list)

    assert missing.status_code == 404
    assert missing.json() == {
        "error_code": "order_not_found",
        "message": "Order was not found",
        "correlation_id": "contract-404",
        "details": [],
    }
    assert missing.headers["X-Request-ID"] == "contract-404"
    assert unexpected.status_code == 500
    assert unexpected.json() == {
        "error_code": "internal_error",
        "message": "The service could not complete the request",
        "correlation_id": "contract-500",
        "details": [],
    }
    assert "secret database detail" not in unexpected.text
    assert "Traceback" not in unexpected.text
