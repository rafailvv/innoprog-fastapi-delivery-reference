from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from fastapi.testclient import TestClient

from delivery_service.csrf import CSRF_COOKIE, CSRF_HEADER, SESSION_COOKIE
from delivery_service.main import app
from delivery_service.security import create_token


PASSWORD = "correct horse battery staple"


def _register_and_login(client: TestClient) -> tuple[str, str]:
    customer_id = str(uuid4())
    subject = f"customer:{customer_id}"
    registered = client.post(
        "/api/v1/auth/register",
        json={"subject": subject, "password": PASSWORD},
    )
    assert registered.status_code == 201
    logged_in = client.post(
        "/api/v1/auth/browser-session",
        data={"username": subject, "password": PASSWORD},
    )
    assert logged_in.status_code == 204
    return customer_id, str(client.cookies.get(CSRF_COOKIE))


def _order(customer_id: str) -> dict[str, object]:
    return {
        "customer_id": customer_id,
        "pickup_address": "Nevsky prospect 1",
        "destination_address": "Liteyny prospect 10",
        "weight_grams": 750,
    }


def test_browser_login_sets_host_only_hardened_cookies() -> None:
    with TestClient(app, base_url="https://testserver") as client:
        customer_id = str(uuid4())
        subject = f"customer:{customer_id}"
        assert client.post(
            "/api/v1/auth/register",
            json={"subject": subject, "password": PASSWORD},
        ).status_code == 201
        response = client.post(
            "/api/v1/auth/browser-session",
            data={"username": subject, "password": PASSWORD},
        )

    cookies = response.headers.get_list("set-cookie")
    session = next(value for value in cookies if value.startswith(f"{SESSION_COOKIE}="))
    csrf = next(value for value in cookies if value.startswith(f"{CSRF_COOKIE}="))
    assert response.status_code == 204
    assert response.headers["cache-control"] == "no-store"
    assert all("Secure" in value and "Path=/" in value for value in (session, csrf))
    assert all("SameSite=lax" in value for value in (session, csrf))
    assert "HttpOnly" in session and "HttpOnly" not in csrf
    assert all("Domain=" not in value for value in (session, csrf))


def test_cookie_authenticated_command_requires_valid_session_bound_csrf() -> None:
    with TestClient(app, base_url="https://testserver") as client:
        customer_id, csrf_token = _register_and_login(client)
        payload = _order(customer_id)
        missing = client.post(
            "/api/v1/orders",
            headers={"Idempotency-Key": f"missing-{uuid4()}"},
            json=payload,
        )
        wrong = client.post(
            "/api/v1/orders",
            headers={
                "Idempotency-Key": f"wrong-{uuid4()}",
                CSRF_HEADER: "not-the-cookie-value",
            },
            json=payload,
        )
        accepted = client.post(
            "/api/v1/orders",
            headers={
                "Idempotency-Key": f"accepted-{uuid4()}",
                CSRF_HEADER: csrf_token,
            },
            json=payload,
        )

    assert missing.status_code == wrong.status_code == 403
    assert accepted.status_code == 201


def test_csrf_token_from_another_login_session_is_rejected() -> None:
    with TestClient(app, base_url="https://testserver") as first:
        _, first_csrf = _register_and_login(first)
    with TestClient(app, base_url="https://testserver") as second:
        customer_id, _second_csrf = _register_and_login(second)
        response = second.post(
            "/api/v1/orders",
            headers={
                "Idempotency-Key": f"cross-session-{uuid4()}",
                CSRF_HEADER: first_csrf,
            },
            json=_order(customer_id),
        )

    assert response.status_code == 403


def test_bearer_client_does_not_need_cookie_specific_csrf_token() -> None:
    customer_id = str(uuid4())
    token = create_token(
        f"customer:{customer_id}", token_type="access", lifetime=timedelta(minutes=5)
    )
    with TestClient(app, base_url="https://testserver") as client:
        response = client.post(
            "/api/v1/orders",
            headers={
                "Authorization": f"Bearer {token}",
                "Idempotency-Key": f"bearer-{uuid4()}",
            },
            json=_order(customer_id),
        )

    assert response.status_code == 201


def test_browser_logout_requires_csrf_before_clearing_session() -> None:
    with TestClient(app, base_url="https://testserver") as client:
        _customer_id, csrf_token = _register_and_login(client)
        rejected = client.delete("/api/v1/auth/browser-session")
        accepted = client.delete(
            "/api/v1/auth/browser-session", headers={CSRF_HEADER: csrf_token}
        )
        after_logout = client.get("/api/v1/orders")

    assert rejected.status_code == 403
    assert accepted.status_code == 204
    assert after_logout.status_code == 401
