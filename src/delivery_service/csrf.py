from __future__ import annotations

import hmac
import secrets

from delivery_service.config import get_settings


CSRF_HEADER = "X-CSRF-Token"
CSRF_COOKIE = "__Host-delivery-csrf"
SESSION_COOKIE = "__Host-delivery-session"


def _csrf_key() -> bytes:
    """Derive a purpose-specific HMAC key from the configured JWT secret."""
    secret = get_settings().jwt_secret.get_secret_value().encode("utf-8")
    return hmac.digest(secret, b"delivery-service:csrf:v1", "sha256")


def _message(session_jti: str, nonce: str) -> bytes:
    # Length prefixes make the boundary between values unambiguous.
    return f"{len(session_jti)}!{session_jti}!{len(nonce)}!{nonce}".encode("utf-8")


def new_csrf_token(session_jti: str) -> str:
    """Create a signed double-submit token bound to one login session."""
    nonce = secrets.token_urlsafe(32)
    signature = hmac.new(_csrf_key(), _message(session_jti, nonce), "sha256").hexdigest()
    return f"{signature}.{nonce}"


def verify_csrf_token(
    cookie_token: str | None,
    header_token: str | None,
    *,
    session_jti: str,
) -> bool:
    """Verify explicit client submission and cryptographic session binding."""
    if not cookie_token or not header_token:
        return False
    if not hmac.compare_digest(cookie_token, header_token):
        return False
    try:
        supplied_signature, nonce = header_token.split(".", maxsplit=1)
    except ValueError:
        return False
    if not supplied_signature or not nonce:
        return False
    expected_signature = hmac.new(
        _csrf_key(), _message(session_jti, nonce), "sha256"
    ).hexdigest()
    return hmac.compare_digest(supplied_signature, expected_signature)
