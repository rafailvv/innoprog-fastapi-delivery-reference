from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel


class PublicOrder(BaseModel):
    id: UUID
    status: str


def test_response_model_filters_internal_field() -> None:
    app = FastAPI()

    @app.get("/order-probe", response_model=PublicOrder)
    def read_order() -> dict[str, str]:
        return {
            "id": "00000000-0000-4000-8000-000000000001",
            "status": "created",
            "payment_token": "must-not-leak",
        }

    with TestClient(app) as client:
        response = client.get("/order-probe")

    assert response.status_code == 200
    assert response.json() == {
        "id": "00000000-0000-4000-8000-000000000001",
        "status": "created",
    }
    assert "payment_token" not in response.text
