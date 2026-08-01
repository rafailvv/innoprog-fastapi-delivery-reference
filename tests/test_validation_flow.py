from datetime import timedelta
from uuid import uuid4

from fastapi.testclient import TestClient

from delivery_service.main import app, get_order_service
from delivery_service.security import create_token
from tests.override_tools import dependency_override


class ServiceProbe:
    def __init__(self) -> None:
        self.create_calls = 0

    async def create(self, *_args, **_kwargs):
        self.create_calls += 1
        raise AssertionError("use case must not run after request validation failed")


def auth(customer_id) -> dict[str, str]:
    token = create_token(
        f"customer:{customer_id}", token_type="access", lifetime=timedelta(minutes=5)
    )
    return {"Authorization": f"Bearer {token}", "Idempotency-Key": "validation-flow"}


def test_invalid_body_is_rejected_before_endpoint_use_case() -> None:
    customer_id = uuid4()
    service = ServiceProbe()
    with dependency_override(app, get_order_service, lambda: service):
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/orders",
                headers=auth(customer_id),
                json={
                    "customer_id": str(customer_id),
                    "pickup_address": "A street 10",
                    "destination_address": "B street 20",
                    "weight_grams": 0,
                },
            )
    assert response.status_code == 422
    assert response.json()["error_code"] == "request_invalid"
    assert response.json()["details"][0]["location"] == ["body", "weight_grams"]
    assert service.create_calls == 0


def test_validation_response_does_not_echo_password_or_raw_input() -> None:
    secret = "tiny"
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/register",
            json={"subject": f"customer:{uuid4()}", "password": secret},
        )

    payload = response.json()
    assert response.status_code == 422
    assert payload["error_code"] == "request_invalid"
    assert secret not in response.text
    assert all("input" not in detail for detail in payload["details"])
    assert payload["details"][0]["location"] == ["body", "password"]


def test_malformed_json_has_a_precise_body_location() -> None:
    customer_id = uuid4()
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/orders",
            headers={**auth(customer_id), "Content-Type": "application/json"},
            content=b'{"customer_id":',
        )

    assert response.status_code == 422
    detail = response.json()["details"][0]
    assert detail["location"][0] == "body"
    assert detail["type"] == "json_invalid"
