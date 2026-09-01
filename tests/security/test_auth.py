from datetime import timedelta

from argon2 import PasswordHasher

from delivery_service.security import (
    check_password, create_token, decode_token, hash_password, owns_order,
    verify_password,
)
from tests.override_tools import dependency_override
from delivery_service.main import app, get_identity_repository
from delivery_service.identity import InMemoryIdentityRepository
from fastapi.testclient import TestClient
from uuid import uuid4
from delivery_service.domain import DeliveryOrder
from delivery_service.repository import InMemoryOrderRepository
from delivery_service.access import Actor, Role, can_access_order
import pytest


def test_invalid_password_is_rejected() -> None:
    digest = hash_password("correct horse battery staple")
    assert not verify_password("invalid password", digest)


def test_same_password_gets_a_distinct_library_generated_salt() -> None:
    first = hash_password("correct horse battery staple")
    second = hash_password("correct horse battery staple")
    assert first != second
    assert verify_password("correct horse battery staple", first)
    assert verify_password("correct horse battery staple", second)


def test_old_argon2_parameters_are_rehashed_only_after_success() -> None:
    old_hasher = PasswordHasher(time_cost=1, memory_cost=8_192, parallelism=1)
    old = old_hasher.hash("correct horse battery staple")

    accepted = check_password("correct horse battery staple", old)
    rejected = check_password("invalid password", old)

    assert accepted.verified and accepted.replacement_hash is not None
    assert verify_password("correct horse battery staple", accepted.replacement_hash)
    assert not rejected.verified and rejected.replacement_hash is None


def test_successful_login_replaces_an_obsolete_hash() -> None:
    identities = InMemoryIdentityRepository()
    subject = f"customer:{uuid4()}"
    password = "correct horse battery staple"
    old = PasswordHasher(time_cost=1, memory_cost=8_192, parallelism=1).hash(password)

    async def override_identities():
        yield identities

    with dependency_override(app, get_identity_repository, override_identities):
        import asyncio

        asyncio.run(identities.add(subject, old))
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/auth/token",
                data={"username": subject, "password": password},
            )
        replacement = asyncio.run(identities.password_hash(subject))
    assert response.status_code == 200
    assert replacement is not None and replacement != old
    assert verify_password(password, replacement)


def test_unknown_identity_and_wrong_password_share_the_public_error() -> None:
    identities = InMemoryIdentityRepository()
    known = f"customer:{uuid4()}"

    async def override_identities():
        yield identities

    with dependency_override(app, get_identity_repository, override_identities):
        import asyncio

        asyncio.run(identities.add(known, hash_password("correct horse battery staple")))
        with TestClient(app) as client:
            unknown = client.post(
                "/api/v1/auth/token",
                data={"username": f"customer:{uuid4()}", "password": "wrong password"},
            )
            wrong = client.post(
                "/api/v1/auth/token",
                data={"username": known, "password": "wrong password"},
            )
    assert unknown.status_code == wrong.status_code == 401
    unknown_body = {key: value for key, value in unknown.json().items() if key != "correlation_id"}
    wrong_body = {key: value for key, value in wrong.json().items() if key != "correlation_id"}
    assert unknown_body == wrong_body
    assert unknown_body["error_code"] == "authentication_required"


def test_orders_reject_missing_token() -> None:
    with TestClient(app) as client:
        response = client.get("/api/v1/orders")
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_token_endpoint_accepts_form_and_disables_response_storage() -> None:
    identities = InMemoryIdentityRepository()
    subject = f"customer:{uuid4()}"
    password = "correct horse battery staple"

    async def override_identities():
        yield identities

    with dependency_override(app, get_identity_repository, override_identities):
        import asyncio

        asyncio.run(identities.add(subject, hash_password(password)))
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/auth/token",
                data={"username": subject, "password": password},
            )
            json_attempt = client.post(
                "/api/v1/auth/token",
                json={"username": subject, "password": password},
            )
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Pragma"] == "no-cache"
    assert response.json()["token_type"] == "bearer"
    assert response.json()["expires_in"] > 0
    assert json_attempt.status_code == 422


def test_openapi_describes_password_form_and_bearer_protection() -> None:
    schema = app.openapi()
    scheme = schema["components"]["securitySchemes"]["OAuth2PasswordBearer"]
    assert scheme["type"] == "oauth2"
    assert scheme["flows"]["password"]["tokenUrl"] == "/api/v1/auth/token"
    assert schema["paths"]["/api/v1/orders"]["get"]["security"] == [
        {"OAuth2PasswordBearer": []}
    ]


def test_wrong_authorization_scheme_and_tampered_token_return_bearer_challenge() -> None:
    access = create_token(
        f"customer:{uuid4()}", token_type="access", lifetime=timedelta(minutes=5),
    )
    header, payload, signature = access.split(".")
    replacement = "A" if signature[0] != "A" else "B"
    tampered = ".".join((header, payload, replacement + signature[1:]))

    with TestClient(app) as client:
        wrong_scheme = client.get(
            "/api/v1/orders", headers={"Authorization": "Basic abc"},
        )
        invalid = client.get(
            "/api/v1/orders", headers={"Authorization": f"Bearer {tampered}"},
        )

    for response in (wrong_scheme, invalid):
        assert response.status_code == 401
        assert response.headers["WWW-Authenticate"] == "Bearer"


def test_registration_cannot_self_assign_dispatcher() -> None:
    with TestClient(app) as client:
        response = client.post("/api/v1/auth/register", json={
            "subject": f"dispatcher:{uuid4()}", "password": "correct horse battery staple",
        })
    assert response.status_code == 422


def test_role_is_part_of_object_ownership_decision() -> None:
    customer_id = uuid4()
    assert owns_order(
        f"customer:{customer_id}", customer_id=customer_id, courier_id=None
    )
    assert not owns_order(
        f"courier:{customer_id}", customer_id=customer_id, courier_id=None
    )


def test_refresh_token_is_rejected_as_api_access_token() -> None:
    subject = f"customer:{uuid4()}"
    refresh = create_token(subject, token_type="refresh", lifetime=timedelta(minutes=5))
    with TestClient(app) as client:
        response = client.get(
            "/api/v1/orders", headers={"Authorization": f"Bearer {refresh}"}
        )
    assert response.status_code == 401


def test_tampered_token_is_rejected() -> None:
    access = create_token(
        f"customer:{uuid4()}", token_type="access", lifetime=timedelta(minutes=5)
    )
    header, payload, signature = access.split(".")
    replacement = "A" if signature[0] != "A" else "B"
    tampered = ".".join((header, payload, replacement + signature[1:]))
    with pytest.raises(Exception):
        decode_token(tampered)


def test_foreign_customer_cannot_read_order() -> None:
    owner = uuid4()
    stranger = uuid4()
    owner_token = create_token(
        f"customer:{owner}", token_type="access", lifetime=timedelta(minutes=5)
    )
    stranger_token = create_token(
        f"customer:{stranger}", token_type="access", lifetime=timedelta(minutes=5)
    )
    payload = {
        "customer_id": str(owner), "pickup_address": "A street 10",
        "destination_address": "B street 20", "weight_grams": 500,
    }
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/orders",
            headers={
                "Authorization": f"Bearer {owner_token}",
                "Idempotency-Key": "security-owner-order",
            },
            json=payload,
        )
        assert created.status_code == 201
        denied = client.get(
            created.headers["Location"],
            headers={"Authorization": f"Bearer {stranger_token}"},
        )
    assert denied.status_code in {403, 404}


async def test_visibility_is_filtered_before_pagination() -> None:
    repository = InMemoryOrderRepository()
    owner = uuid4()
    foreign = DeliveryOrder.create(
        customer_id=uuid4(), pickup_address="A street 10",
        destination_address="B street 20", weight_grams=100,
    )
    own = DeliveryOrder.create(
        customer_id=owner, pickup_address="A street 10",
        destination_address="B street 20", weight_grams=100,
    )
    await repository.add(foreign)
    await repository.add(own)
    visible = await repository.list_visible(
        role="customer", actor_id=owner, limit=1, offset=0,
        sort="created_at", status_filter=None,
    )
    assert visible == [own]


def test_same_user_id_in_another_tenant_has_no_order_access() -> None:
    user_id = uuid4()
    order_tenant = uuid4()
    another_tenant = uuid4()

    assert can_access_order(
        Actor(user_id=user_id, role=Role.CUSTOMER, tenant_id=order_tenant),
        order_tenant_id=order_tenant,
        customer_id=user_id,
        courier_id=None,
    )
    assert not can_access_order(
        Actor(user_id=user_id, role=Role.CUSTOMER, tenant_id=another_tenant),
        order_tenant_id=order_tenant,
        customer_id=user_id,
        courier_id=None,
    )


def test_dispatcher_privilege_stops_at_tenant_boundary() -> None:
    tenant = uuid4()
    dispatcher = Actor(user_id=uuid4(), role=Role.DISPATCHER, tenant_id=tenant)
    assert can_access_order(
        dispatcher, order_tenant_id=tenant, customer_id=uuid4(), courier_id=None,
    )
    assert not can_access_order(
        dispatcher, order_tenant_id=uuid4(), customer_id=uuid4(), courier_id=None,
    )


def test_api_enforces_tenant_before_object_relationship() -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    customer_id = uuid4()
    token_a = create_token(
        f"customer:{customer_id}", token_type="access",
        lifetime=timedelta(minutes=5), tenant_id=tenant_a,
    )
    token_b = create_token(
        f"customer:{customer_id}", token_type="access",
        lifetime=timedelta(minutes=5), tenant_id=tenant_b,
    )
    payload = {
        "customer_id": str(customer_id),
        "pickup_address": "A street 10",
        "destination_address": "B street 20",
        "weight_grams": 500,
    }
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/orders",
            headers={
                "Authorization": f"Bearer {token_a}",
                "Idempotency-Key": f"tenant-a-{uuid4()}",
            },
            json=payload,
        )
        assert created.status_code == 201
        location = created.headers["Location"]
        assert client.get(
            location, headers={"Authorization": f"Bearer {token_b}"},
        ).status_code in {403, 404}
        page = client.get(
            "/api/v1/orders",
            headers={"Authorization": f"Bearer {token_b}"},
        )
    assert page.status_code == 200
    assert created.json()["id"] not in {item["id"] for item in page.json()}
