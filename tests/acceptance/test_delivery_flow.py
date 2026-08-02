from uuid import uuid4

from fastapi.testclient import TestClient

from delivery_service.main import app


def test_acceptance_order_lifecycle_through_public_api() -> None:
    customer_id = uuid4()
    subject = f"customer:{customer_id}"
    password = "correct horse battery staple"
    payload = {
        "customer_id": str(customer_id), "pickup_address": "A street 10",
        "destination_address": "B street 20", "weight_grams": 500,
    }
    with TestClient(app) as client:
        registered = client.post(
            "/api/v1/auth/register", json={"subject": subject, "password": password}
        )
        assert registered.status_code == 201
        login = client.post(
            "/api/v1/auth/token", data={"username": subject, "password": password}
        )
        assert login.status_code == 200
        headers = {
            "Authorization": f"Bearer {login.json()['access_token']}",
            "Idempotency-Key": "acceptance-order",
        }
        created = client.post("/api/v1/orders", headers=headers, json=payload)
        assert created.status_code == 201
        repeated = client.post("/api/v1/orders", headers=headers, json=payload)
        assert repeated.status_code == 201 and repeated.json()["id"] == created.json()["id"]

        read = client.get(created.headers["Location"], headers=headers)
        assert read.status_code == 200 and read.json()["id"] == created.json()["id"]

        updated = client.patch(
            created.headers["Location"],
            headers={**headers, "X-Expected-Version": "1"},
            json={"weight_grams": 750},
        )
        assert updated.status_code == 200 and updated.json()["version"] == 2
        stale = client.patch(
            created.headers["Location"],
            headers={**headers, "X-Expected-Version": "1"},
            json={"weight_grams": 900},
        )
        assert stale.status_code == 409

        cancelled = client.post(
            f"{created.headers['Location']}/transitions",
            headers={**headers, "X-Expected-Version": "2"},
            json={"status": "cancelled"},
        )
        assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
        visible = client.get("/api/v1/orders?status=cancelled", headers=headers)
        assert visible.status_code == 200
        assert [order["id"] for order in visible.json()] == [created.json()["id"]]

        exported = client.get(f"/api/v1/exports/{created.json()['id']}", headers=headers)
        assert exported.status_code == 200
        assert created.json()["id"] in exported.text
