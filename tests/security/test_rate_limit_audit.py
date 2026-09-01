from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from delivery_service.main import app
from delivery_service.policy import (
    AuditTrail,
    SlidingWindowRateLimiter,
    audit_trail,
    login_limit_keys,
    login_rate_limiter,
    normalize_login_subject,
)


@pytest.fixture(autouse=True)
def reset_security_controls() -> None:
    login_rate_limiter.clear()
    audit_trail.clear()
    yield
    login_rate_limiter.clear()
    audit_trail.clear()


def test_sixth_login_has_exact_retry_after_and_safe_audit() -> None:
    raw_subject = f"customer:{uuid4()}"
    password = "never-store-this-password"
    with TestClient(app) as client:
        responses = [
            client.post(
                "/api/v1/auth/token",
                data={"username": raw_subject, "password": password},
            )
            for _ in range(6)
        ]

    assert [response.status_code for response in responses[:5]] == [401] * 5
    assert responses[5].status_code == 429
    assert 1 <= int(responses[5].headers["Retry-After"]) <= 60
    assert [event.outcome for event in audit_trail.events] == ["denied"] * 5 + [
        "rate_limited"
    ]
    serialized = repr(audit_trail.events)
    assert raw_subject not in serialized
    assert password not in serialized
    assert all(event.actor_id is None for event in audit_trail.events)
    assert all(event.correlation_id for event in audit_trail.events)


def test_window_boundary_reports_remaining_time_and_reopens() -> None:
    async def scenario() -> None:
        limiter = SlidingWindowRateLimiter(limit=2, window=timedelta(seconds=60))
        start = datetime(2026, 1, 1, tzinfo=UTC)
        assert (await limiter.check("login:network:test", now=start)).allowed
        assert (await limiter.check(
            "login:network:test", now=start + timedelta(seconds=1)
        )).allowed
        blocked = await limiter.check(
            "login:network:test", now=start + timedelta(seconds=2)
        )
        reopened = await limiter.check(
            "login:network:test", now=start + timedelta(seconds=61)
        )
        assert not blocked.allowed
        assert blocked.retry_after_seconds == 58
        assert reopened.allowed

    asyncio.run(scenario())


def test_network_and_subject_budgets_are_independent_and_atomic() -> None:
    async def scenario() -> None:
        limiter = SlidingWindowRateLimiter(limit=1, window=timedelta(minutes=1))
        first = login_limit_keys(client_network_key="198.51.100.1", subject="customer:a")
        same_network = login_limit_keys(
            client_network_key="198.51.100.1", subject="customer:b"
        )
        same_subject = login_limit_keys(
            client_network_key="198.51.100.2", subject="customer:a"
        )

        assert (await limiter.check_many(first)).allowed
        assert not (await limiter.check_many(same_network)).allowed
        assert not (await limiter.check_many(same_subject)).allowed

        race = SlidingWindowRateLimiter(limit=1, window=timedelta(minutes=1))
        decisions = await asyncio.gather(
            race.check_many(("login:network:one", "login:subject:one")),
            race.check_many(("login:network:one", "login:subject:one")),
        )
        assert sorted(decision.allowed for decision in decisions) == [False, True]

    asyncio.run(scenario())


def test_login_normalization_is_bounded_and_does_not_touch_passwords() -> None:
    assert normalize_login_subject("  CUSTOMER:ABC  ") == "customer:abc"
    with pytest.raises(ValueError):
        normalize_login_subject("x" * 161)
    with pytest.raises(ValueError):
        normalize_login_subject("customer:abc\nforged-audit-event")


def test_registration_and_login_share_the_same_canonical_subject() -> None:
    raw_subject = f"customer:{str(uuid4()).upper()}"
    password = "correct horse battery staple"
    with TestClient(app) as client:
        registered = client.post(
            "/api/v1/auth/register",
            json={"subject": raw_subject, "password": password},
        )
        logged_in = client.post(
            "/api/v1/auth/token",
            data={"username": f"  {raw_subject}  ", "password": password},
        )

    assert registered.status_code == 201
    assert registered.json()["subject"] == raw_subject.casefold()
    assert logged_in.status_code == 200


def test_audit_events_are_exposed_as_an_immutable_snapshot() -> None:
    trail = AuditTrail()
    assert trail.events == ()
    assert isinstance(trail.events, tuple)
