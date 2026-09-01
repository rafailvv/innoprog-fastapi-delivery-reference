from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from fastapi.testclient import TestClient

from delivery_service.main import app
from delivery_service.security import create_token
from scripts.acceptance_smoke import run_acceptance


def token(customer_id) -> str:
    return create_token(
        f"customer:{customer_id}",
        token_type="access",
        lifetime=timedelta(minutes=5),
    )


def test_release_acceptance_uses_public_boundaries_and_returns_evidence() -> None:
    customer_id = uuid4()
    foreign_id = uuid4()
    with TestClient(app) as client:
        evidence = run_acceptance(
            client,
            customer_id=str(customer_id),
            customer_token=token(customer_id),
            foreign_token=token(foreign_id),
            key=f"release-readiness-{uuid4()}",
        )

    assert evidence.replayed
    assert evidence.foreign_access_denied
    assert evidence.stale_update_rejected
    assert evidence.final_status == "cancelled"
    assert evidence.metrics_exposed
