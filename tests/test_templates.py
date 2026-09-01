from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

from delivery_service.main import app
from delivery_service.security import create_token


def _auth(subject: str) -> dict[str, str]:
    token = create_token(subject, token_type="access", lifetime=timedelta(minutes=5))
    return {"Authorization": f"Bearer {token}"}


def _create_order(client: TestClient, *, customer_id: str, destination: str) -> dict:
    response = client.post(
        "/api/v1/orders",
        headers={
            **_auth(f"customer:{customer_id}"),
            "Idempotency-Key": f"template-{uuid4()}",
        },
        json={
            "customer_id": customer_id,
            "pickup_address": "Nevsky prospect 1",
            "destination_address": destination,
            "weight_grams": 750,
        },
    )
    assert response.status_code == 201
    return response.json()


def test_order_card_uses_allowlisted_view_model_and_escapes_customer_text() -> None:
    customer_id = str(uuid4())
    payload = "<script>window.stolen=true</script> Delivery street 10"
    with TestClient(app, base_url="http://testserver") as client:
        order = _create_order(client, customer_id=customer_id, destination=payload)
        response = client.get(
            f"/pages/orders/{order['id']}",
            headers=_auth(f"customer:{customer_id}"),
        )

    assert response.status_code == 200
    assert response.template.name == "orders/card.html"
    view = response.context["order"]
    assert not hasattr(view, "customer_id") and not hasattr(view, "courier_id")
    assert "<script>" not in response.text
    assert "&lt;script&gt;" in response.text
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["content-security-policy"].startswith("default-src 'none'")


def test_order_card_checks_ownership_before_rendering() -> None:
    owner_id = str(uuid4())
    stranger_id = str(uuid4())
    with TestClient(app, base_url="http://testserver") as client:
        order = _create_order(
            client,
            customer_id=owner_id,
            destination="Liteyny prospect 10",
        )
        response = client.get(
            f"/pages/orders/{order['id']}",
            headers=_auth(f"customer:{stranger_id}"),
        )

    assert response.status_code == 403


def test_order_template_does_not_disable_autoescape() -> None:
    template = (
        Path(__file__).resolve().parents[1]
        / "src/delivery_service/templates/orders/card.html"
    ).read_text(encoding="utf-8")

    assert "|safe" not in template
    assert "{% autoescape false %}" not in template
