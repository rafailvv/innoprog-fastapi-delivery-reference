from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
import pytest

from delivery_service.access import Actor, Role, can_access_order
from delivery_service.main import app
from delivery_service.security import create_token


TENANT = UUID("54000000-0000-0000-0000-000000000001")
OTHER_TENANT = UUID("54000000-0000-0000-0000-000000000002")
CUSTOMER = UUID("54000000-0000-0000-0000-000000000003")
OTHER_CUSTOMER = UUID("54000000-0000-0000-0000-000000000004")
COURIER = UUID("54000000-0000-0000-0000-000000000005")
OTHER_COURIER = UUID("54000000-0000-0000-0000-000000000006")
DISPATCHER = UUID("54000000-0000-0000-0000-000000000007")


@pytest.mark.parametrize(
    ("actor", "expected"),
    [
        (Actor(CUSTOMER, Role.CUSTOMER, TENANT), True),
        (Actor(OTHER_CUSTOMER, Role.CUSTOMER, TENANT), False),
        (Actor(COURIER, Role.COURIER, TENANT), True),
        (Actor(OTHER_COURIER, Role.COURIER, TENANT), False),
        (Actor(DISPATCHER, Role.DISPATCHER, TENANT), True),
        (Actor(CUSTOMER, Role.CUSTOMER, OTHER_TENANT), False),
        (Actor(COURIER, Role.COURIER, OTHER_TENANT), False),
        (Actor(DISPATCHER, Role.DISPATCHER, OTHER_TENANT), False),
    ],
    ids=[
        "owner",
        "other-customer",
        "assigned-courier",
        "other-courier",
        "tenant-dispatcher",
        "cross-tenant-customer",
        "cross-tenant-courier",
        "cross-tenant-dispatcher",
    ],
)
def test_order_access_matrix(actor: Actor, expected: bool) -> None:
    assert can_access_order(
        actor,
        order_tenant_id=TENANT,
        customer_id=CUSTOMER,
        courier_id=COURIER,
    ) is expected


def _auth(user_id: UUID, *, tenant_id: UUID) -> dict[str, str]:
    token = create_token(
        f"customer:{user_id}",
        token_type="access",
        lifetime=timedelta(minutes=5),
        tenant_id=tenant_id,
    )
    return {"Authorization": f"Bearer {token}"}


def test_denied_actor_cannot_read_or_change_order() -> None:
    idempotency_key = f"access-matrix-{uuid4()}"
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/orders",
            headers={**_auth(CUSTOMER, tenant_id=TENANT), "Idempotency-Key": idempotency_key},
            json={
                "customer_id": str(CUSTOMER),
                "pickup_address": "Policy street 1",
                "destination_address": "Boundary avenue 2",
                "weight_grams": 540,
            },
        )
        assert created.status_code == 201
        location = created.headers["Location"]
        before = created.json()

        unauthenticated = client.get(location)

        denied_read = client.get(
            location, headers=_auth(OTHER_CUSTOMER, tenant_id=TENANT),
        )
        denied_update = client.patch(
            location,
            headers={
                **_auth(OTHER_CUSTOMER, tenant_id=TENANT),
                "X-Expected-Version": str(before["version"]),
            },
            json={"weight_grams": 999},
        )
        observed = client.get(location, headers=_auth(CUSTOMER, tenant_id=TENANT))

    assert unauthenticated.status_code == 401
    assert unauthenticated.headers["WWW-Authenticate"] == "Bearer"
    assert denied_read.status_code == 403
    assert denied_update.status_code == 403
    assert "Policy street" not in denied_read.text
    assert observed.status_code == 200
    assert observed.json()["version"] == before["version"]
    assert observed.json()["weight_grams"] == before["weight_grams"]
