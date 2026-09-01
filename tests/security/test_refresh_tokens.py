from __future__ import annotations

import asyncio
import base64
from datetime import UTC, datetime, timedelta
import json
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from delivery_service.config import get_settings
from delivery_service.main import app
from delivery_service.policy import login_rate_limiter
from delivery_service.refresh_sessions import (
    InMemoryRefreshSessionRepository,
    RefreshSession,
    RotationResult,
)
from delivery_service.security import create_token, decode_token


@pytest.fixture(autouse=True)
def reset_login_limiter() -> None:
    login_rate_limiter._requests.clear()
    yield
    login_rate_limiter._requests.clear()


def _register_and_login(client: TestClient) -> dict[str, object]:
    subject = f"customer:{uuid4()}"
    password = "correct horse battery staple"
    registered = client.post(
        "/api/v1/auth/register", json={"subject": subject, "password": password},
    )
    assert registered.status_code == 201
    issued = client.post(
        "/api/v1/auth/token", data={"username": subject, "password": password},
    )
    assert issued.status_code == 200
    return issued.json()


def test_signed_jwt_payload_is_readable_and_contains_no_password() -> None:
    subject = f"customer:{uuid4()}"
    token = create_token(subject, token_type="access", lifetime=timedelta(minutes=5))
    encoded_payload = token.split(".")[1]
    payload = json.loads(base64.urlsafe_b64decode(encoded_payload + "=" * (-len(encoded_payload) % 4)))

    assert payload["sub"] == subject
    assert payload["type"] == "access"
    assert "password" not in payload


def test_access_decoder_requires_expected_header_claims_issuer_audience_and_time() -> None:
    settings = get_settings()
    now = datetime.now(UTC)
    subject = f"customer:{uuid4()}"
    valid = create_token(
        subject, token_type="access", lifetime=timedelta(minutes=5), now=now,
    )
    header = jwt.get_unverified_header(valid)
    claims = decode_token(valid)

    assert header == {"alg": "HS256", "typ": "at+jwt"}
    assert {
        "sub", "type", "iss", "aud", "iat", "nbf", "exp", "jti",
    } <= claims.keys()

    wrong_audience = jwt.encode(
        {**claims, "aud": "another-api"},
        settings.jwt_secret.get_secret_value(),
        algorithm="HS256",
        headers={"typ": "at+jwt"},
    )
    missing_jti = jwt.encode(
        {key: value for key, value in claims.items() if key != "jti"},
        settings.jwt_secret.get_secret_value(),
        algorithm="HS256",
        headers={"typ": "at+jwt"},
    )
    wrong_typ = jwt.encode(
        claims,
        settings.jwt_secret.get_secret_value(),
        algorithm="HS256",
        headers={"typ": "rt+jwt"},
    )
    expired = create_token(
        subject, token_type="access", lifetime=timedelta(minutes=-2), now=now,
    )

    for rejected in (wrong_audience, missing_jti, wrong_typ, expired):
        with pytest.raises(Exception):
            decode_token(rejected)


def test_refresh_rotation_rejects_reuse_and_revokes_the_active_family() -> None:
    with TestClient(app) as client:
        issued = _register_and_login(client)
        first_refresh = str(issued["refresh_token"])
        rotated = client.post(
            "/api/v1/auth/refresh",
            data={"grant_type": "refresh_token", "refresh_token": first_refresh},
        )
        assert rotated.status_code == 200
        assert rotated.headers["Cache-Control"] == "no-store"
        second_refresh = rotated.json()["refresh_token"]
        assert second_refresh != first_refresh

        reuse = client.post(
            "/api/v1/auth/refresh",
            data={"grant_type": "refresh_token", "refresh_token": first_refresh},
        )
        active_after_reuse = client.post(
            "/api/v1/auth/refresh",
            data={"grant_type": "refresh_token", "refresh_token": second_refresh},
        )

    assert reuse.status_code == 401
    assert reuse.headers["WWW-Authenticate"] == "Bearer"
    assert active_after_reuse.status_code == 401


def test_refresh_rotation_is_atomic_for_two_presentations() -> None:
    repository = InMemoryRefreshSessionRepository()
    now = datetime.now(UTC)
    family = uuid4()
    subject = f"customer:{uuid4()}"
    current = RefreshSession(uuid4(), family, subject, now + timedelta(days=7))
    first_replacement = RefreshSession(uuid4(), family, subject, now + timedelta(days=7))
    second_replacement = RefreshSession(uuid4(), family, subject, now + timedelta(days=7))

    async def race() -> list[RotationResult]:
        await repository.register(current)
        return list(await asyncio.gather(
            repository.rotate(
                presented_jti=current.jti,
                family_id=family,
                subject=subject,
                replacement=first_replacement,
                now=now,
            ),
            repository.rotate(
                presented_jti=current.jti,
                family_id=family,
                subject=subject,
                replacement=second_replacement,
                now=now,
            ),
        ))

    results = asyncio.run(race())
    assert sorted(results) == sorted([RotationResult.ROTATED, RotationResult.REUSED])
