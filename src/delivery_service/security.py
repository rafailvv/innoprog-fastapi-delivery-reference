from __future__ import annotations

from datetime import UTC, datetime, timedelta
from dataclasses import dataclass
import hashlib
import hmac
from typing import Annotated
from uuid import UUID, uuid4

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer

from delivery_service.config import get_settings
from delivery_service.access import Actor, DEFAULT_TENANT_ID, Role, can_access_order
from delivery_service.csrf import (
    CSRF_COOKIE, CSRF_HEADER, SESSION_COOKIE, verify_csrf_token,
)


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token", auto_error=False)
password_hasher = PasswordHasher()
BEARER_CHALLENGE = {"WWW-Authenticate": "Bearer"}


@dataclass(frozen=True)
class PasswordCheck:
    """Observable password-verification result without retaining plaintext."""

    verified: bool
    replacement_hash: str | None = None


@dataclass(frozen=True)
class IssuedTokenPair:
    access_token: str
    refresh_token: str
    access_expires_in: int
    refresh_jti: UUID
    family_id: UUID
    refresh_expires_at: datetime


def hash_password(password: str, *, salt: bytes | None = None) -> str:
    # ``salt`` remains accepted for backwards-compatible course tests, but
    # Argon2 generates and stores a cryptographically random salt itself.
    del salt
    return password_hasher.hash(password)


def check_password(password: str, expected: str) -> PasswordCheck:
    """Verify one Argon2id string and lazily migrate obsolete parameters.

    Rehashing is offered only after a successful verification.  The caller
    owns the atomic database replacement and must never log either value.
    """
    try:
        password_hasher.verify(expected, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return PasswordCheck(verified=False)

    replacement_hash = (
        password_hasher.hash(password)
        if password_hasher.check_needs_rehash(expected)
        else None
    )
    return PasswordCheck(verified=True, replacement_hash=replacement_hash)


def verify_password(password: str, expected: str, *, salt: bytes | None = None) -> bool:
    del salt
    return check_password(password, expected).verified


# Unknown identities still take the password-hashing path.  This removes the
# large application-level timing difference without pretending that every
# network and storage path is perfectly constant-time.
DUMMY_PASSWORD_HASH = hash_password("dummy password used only for timing balance")


def create_token(
    subject: str,
    *,
    token_type: str,
    lifetime: timedelta,
    token_id: UUID | None = None,
    family_id: UUID | None = None,
    now: datetime | None = None,
    tenant_id: UUID = DEFAULT_TENANT_ID,
) -> str:
    if token_type not in {"access", "refresh"}:
        raise ValueError("unsupported token type")
    issued_at = now or datetime.now(UTC)
    token_id = token_id or uuid4()
    settings = get_settings()
    claims: dict[str, object] = {
        "sub": subject,
        "type": token_type,
        "iss": settings.jwt_issuer,
        "aud": settings.jwt_audience,
        "iat": issued_at,
        "nbf": issued_at,
        "exp": issued_at + lifetime,
        "jti": str(token_id),
        "tenant": str(tenant_id),
    }
    if token_type == "refresh":
        claims["family"] = str(family_id or uuid4())
    return jwt.encode(
        claims,
        settings.jwt_secret.get_secret_value(),
        algorithm="HS256",
        headers={"typ": "at+jwt" if token_type == "access" else "rt+jwt"},
    )


def create_refresh_token(subject: str) -> str:
    return create_token(subject, token_type="refresh", lifetime=timedelta(days=7))


def issue_token_pair(
    subject: str,
    *,
    family_id: UUID | None = None,
    now: datetime | None = None,
    tenant_id: UUID = DEFAULT_TENANT_ID,
) -> IssuedTokenPair:
    issued_at = now or datetime.now(UTC)
    family_id = family_id or uuid4()
    refresh_jti = uuid4()
    refresh_lifetime = timedelta(days=7)
    return IssuedTokenPair(
        access_token=create_token(
            subject,
            token_type="access",
            lifetime=timedelta(minutes=15),
            now=issued_at,
            tenant_id=tenant_id,
        ),
        refresh_token=create_token(
            subject,
            token_type="refresh",
            lifetime=refresh_lifetime,
            token_id=refresh_jti,
            family_id=family_id,
            now=issued_at,
            tenant_id=tenant_id,
        ),
        access_expires_in=900,
        refresh_jti=refresh_jti,
        family_id=family_id,
        refresh_expires_at=issued_at + refresh_lifetime,
    )


def decode_token(token: str, *, expected_type: str = "access") -> dict[str, object]:
    settings = get_settings()
    try:
        expected_typ = "at+jwt" if expected_type == "access" else "rt+jwt"
        header = jwt.get_unverified_header(token)
        if header.get("alg") != "HS256" or header.get("typ") != expected_typ:
            raise jwt.InvalidTokenError("unexpected JWT header")
        payload = jwt.decode(
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=["HS256"],
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
            options={
                "require": [
                    "sub", "type", "iss", "aud", "iat", "nbf", "exp", "jti", "tenant",
                ]
            },
            leeway=timedelta(seconds=30),
        )
        if payload["type"] != expected_type:
            raise jwt.InvalidTokenError("unexpected token type")
        UUID(str(payload["jti"]))
        UUID(str(payload["tenant"]))
        if expected_type == "refresh":
            UUID(str(payload["family"]))
        return payload
    except (jwt.PyJWTError, KeyError, ValueError) as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid token",
            headers=BEARER_CHALLENGE,
        ) from error


def current_subject(token: Annotated[str | None, Depends(oauth2_scheme)]) -> str:
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="authentication required",
            headers=BEARER_CHALLENGE,
        )
    return str(decode_token(token)["sub"])


def actor_from_claims(claims: dict[str, object]) -> Actor:
    role, user_id = parse_subject(str(claims["sub"]))
    try:
        return Actor(user_id=user_id, role=Role(role), tenant_id=UUID(str(claims["tenant"])))
    except (KeyError, ValueError) as error:
        raise HTTPException(
            status_code=401, detail="invalid actor context", headers=BEARER_CHALLENGE,
        ) from error


def current_actor(
    request: Request,
    token: Annotated[str | None, Depends(oauth2_scheme)],
) -> Actor:
    """Authenticate Bearer clients or a browser cookie session.

    Bearer credentials are attached explicitly by the caller and therefore do
    not use this cookie-specific CSRF check.  A browser sends a session cookie
    automatically, so every unsafe cookie-authenticated request must prove
    knowledge of the session-bound double-submit token.
    """
    if token is not None:
        return actor_from_claims(decode_token(token))

    session_token = request.cookies.get(SESSION_COOKIE)
    if session_token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="authentication required",
            headers=BEARER_CHALLENGE,
        )
    claims = decode_token(session_token)
    if request.method not in {"GET", "HEAD", "OPTIONS"} and not verify_csrf_token(
        request.cookies.get(CSRF_COOKIE),
        request.headers.get(CSRF_HEADER),
        session_jti=str(claims["jti"]),
    ):
        raise HTTPException(status_code=403, detail="CSRF check failed")
    return actor_from_claims(claims)


def parse_subject(subject: str) -> tuple[str, UUID]:
    role, separator, raw_user_id = subject.partition(":")
    if not separator or role not in {"customer", "courier", "dispatcher"}:
        raise HTTPException(
            status_code=401, detail="invalid subject", headers=BEARER_CHALLENGE,
        )
    try:
        return role, UUID(raw_user_id)
    except ValueError as error:
        raise HTTPException(
            status_code=401, detail="invalid subject", headers=BEARER_CHALLENGE,
        ) from error


def owns_order(
    subject_or_actor: str | Actor,
    *,
    customer_id: UUID,
    courier_id: UUID | None,
    tenant_id: UUID = DEFAULT_TENANT_ID,
) -> bool:
    actor = subject_or_actor
    if isinstance(actor, str):
        role, actor_id = parse_subject(actor)
        actor = Actor(user_id=actor_id, role=Role(role), tenant_id=DEFAULT_TENANT_ID)
    return can_access_order(
        actor,
        order_tenant_id=tenant_id,
        customer_id=customer_id,
        courier_id=courier_id,
    )


def require_role(required: str):
    def dependency(subject: Annotated[str, Depends(current_subject)]) -> str:
        role, _, user_id = subject.partition(":")
        if role != required:
            raise HTTPException(status_code=403, detail="insufficient role")
        return user_id
    return dependency
