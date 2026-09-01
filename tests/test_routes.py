from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient


def build_route_probe() -> FastAPI:
    app = FastAPI()

    @app.get("/orders/stats")
    def stats() -> dict[str, int]:
        return {"total": 0}

    @app.get("/orders/{order_id}")
    def get_order(order_id: UUID) -> dict[str, str]:
        return {"id": str(order_id)}

    return app


def test_static_route_is_not_shadowed_by_dynamic_uuid_route() -> None:
    with TestClient(build_route_probe()) as client:
        response = client.get("/orders/stats")
    assert response.status_code == 200
    assert response.json() == {"total": 0}


def test_invalid_dynamic_uuid_is_validation_error() -> None:
    with TestClient(build_route_probe()) as client:
        response = client.get("/orders/not-a-uuid")
    assert response.status_code == 422
