from fastapi.testclient import TestClient

from delivery_service.main import app


def test_health() -> None:
    assert TestClient(app).get("/health/live").json() == {"status": "ok"}
