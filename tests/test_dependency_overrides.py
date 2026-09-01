from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from delivery_service.main import app, get_order_repository
from delivery_service.security import create_token
from tests.fakes import FakeOrderRepository
from tests.override_tools import dependency_override


def auth(customer_id: UUID) -> dict[str, str]:
    token = create_token(
        f"customer:{customer_id}",
        token_type="access",
        lifetime=timedelta(minutes=5),
    )
    return {"Authorization": f"Bearer {token}"}


def order_payload(customer_id: UUID) -> dict[str, object]:
    return {
        "customer_id": str(customer_id),
        "pickup_address": "Казань, Баумана, 1",
        "destination_address": "Казань, Кремлёвская, 18",
        "weight_grams": 750,
    }


def test_repository_override_keeps_real_service_and_domain_behavior() -> None:
    customer_id = UUID(int=501)
    fake_repository = FakeOrderRepository()

    with dependency_override(app, get_order_repository, lambda: fake_repository):
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/orders",
                headers={**auth(customer_id), "Idempotency-Key": "override-lesson-51"},
                json=order_payload(customer_id),
            )

    assert response.status_code == 201
    assert response.json()["status"] == "created"
    assert len(fake_repository.added) == 1
    assert fake_repository.created_events == [fake_repository.added[0].id]
    assert get_order_repository not in app.dependency_overrides


def test_override_key_is_the_original_callable_not_its_name() -> None:
    fake_repository = FakeOrderRepository()

    def another_dependency_with_the_same_name():
        return fake_repository

    another_dependency_with_the_same_name.__name__ = get_order_repository.__name__
    with dependency_override(
        app, another_dependency_with_the_same_name, lambda: fake_repository
    ):
        assert get_order_repository not in app.dependency_overrides
        assert app.dependency_overrides[another_dependency_with_the_same_name]() is fake_repository

    assert app.dependency_overrides == {}


def test_override_is_restored_after_test_body_raises() -> None:
    fake_repository = FakeOrderRepository()

    with pytest.raises(RuntimeError, match="deliberate failure"):
        with dependency_override(app, get_order_repository, lambda: fake_repository):
            assert get_order_repository in app.dependency_overrides
            raise RuntimeError("deliberate failure")

    assert get_order_repository not in app.dependency_overrides


def test_nested_override_restores_outer_replacement() -> None:
    outer = FakeOrderRepository()
    inner = FakeOrderRepository()
    outer_provider = lambda: outer

    with dependency_override(app, get_order_repository, outer_provider):
        with dependency_override(app, get_order_repository, lambda: inner):
            assert app.dependency_overrides[get_order_repository]() is inner
        assert app.dependency_overrides[get_order_repository] is outer_provider

    assert get_order_repository not in app.dependency_overrides
