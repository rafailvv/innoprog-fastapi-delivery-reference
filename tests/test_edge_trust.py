from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from delivery_service.main import app


def test_allowed_origin_receives_exact_preflight_permission() -> None:
    with TestClient(app, base_url="http://testserver") as client:
        response = client.options(
            "/api/v1/orders/example",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "PATCH",
                "Access-Control-Request-Headers": "authorization,x-csrf-token,x-request-id",
            },
        )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:3000"
    assert response.headers["access-control-allow-credentials"] == "true"
    assert "x-csrf-token" in response.headers["access-control-allow-headers"].lower()
    assert "Origin" in response.headers["vary"]


def test_unknown_origin_receives_no_cors_permission() -> None:
    with TestClient(app, base_url="http://testserver") as client:
        response = client.options(
            "/api/v1/orders/example",
            headers={
                "Origin": "https://attacker.example",
                "Access-Control-Request-Method": "PATCH",
            },
        )

    assert response.status_code == 400
    assert "access-control-allow-origin" not in response.headers


def test_unknown_host_is_rejected_before_routing() -> None:
    with TestClient(app, base_url="http://testserver") as client:
        response = client.get("/health/live", headers={"Host": "attacker.example"})

    assert response.status_code == 400


def test_reverse_proxy_configuration_has_an_explicit_trust_boundary() -> None:
    root = Path(__file__).resolve().parents[1]
    nginx = (root / "nginx.conf").read_text(encoding="utf-8")
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")

    assert "proxy_set_header Host $host;" in nginx
    assert "proxy_set_header X-Forwarded-Proto $scheme;" in nginx
    assert "proxy_set_header X-Forwarded-For $remote_addr;" in nginx
    assert "$proxy_add_x_forwarded_for" not in nginx
    assert "--proxy-headers" in dockerfile
    assert "--forwarded-allow-ips=" in dockerfile
    assert "--forwarded-allow-ips=*" not in dockerfile
