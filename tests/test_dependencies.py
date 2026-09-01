from collections.abc import Iterator
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient


def test_dependency_cache_and_cleanup() -> None:
    events: list[str] = []

    def resource() -> Iterator[object]:
        events.append("open")
        try:
            yield object()
        finally:
            events.append("close")

    app = FastAPI()

    @app.get("/probe")
    def probe(
        first: Annotated[object, Depends(resource)],
        second: Annotated[object, Depends(resource)],
    ) -> dict[str, bool]:
        events.append("endpoint")
        return {"same": first is second}

    with TestClient(app) as client:
        response = client.get("/probe")

    assert response.json() == {"same": True}
    assert events == ["open", "endpoint", "close"]


def test_application_service_does_not_import_fastapi() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "delivery_service"
        / "service.py"
    ).read_text("utf-8")
    assert "from fastapi" not in source
    assert "import fastapi" not in source
