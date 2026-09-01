from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from delivery_service.notifications import InMemoryIdempotentNotificationProvider
from delivery_service.storage import InvalidProofLink, ProofLinkSigner


def test_presigned_get_binds_operation_key_expiry_and_signature() -> None:
    now = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)
    tenant_id, order_id = uuid4(), uuid4()
    key = f"proofs/{tenant_id}/{order_id}/{'a' * 64}.png"
    signer = ProofLinkSigner("test-proof-link-secret-that-is-long-enough", max_ttl_seconds=300)

    link = signer.issue_get(key, now=now, ttl_seconds=60)
    query = link.url.split("?", 1)[1]
    values = dict(item.split("=", 1) for item in query.split("&"))
    from urllib.parse import unquote_plus

    signer.verify_get(
        key=unquote_plus(values["key"]),
        expires=int(values["expires"]),
        signature=values["signature"],
        now=now + timedelta(seconds=59),
    )
    with pytest.raises(InvalidProofLink, match="signature"):
        signer.verify_get(
            key=key.replace("a" * 64, "b" * 64),
            expires=int(values["expires"]),
            signature=values["signature"],
            now=now,
        )
    with pytest.raises(InvalidProofLink, match="expired"):
        signer.verify_get(
            key=key,
            expires=int(values["expires"]),
            signature=values["signature"],
            now=now + timedelta(seconds=61),
        )


@pytest.mark.asyncio
async def test_notification_provider_deduplicates_same_business_event_id() -> None:
    provider = InMemoryIdempotentNotificationProvider()
    event_id = str(uuid4())

    first = await provider.send(
        recipient="customer-1", message="created", idempotency_key=event_id
    )
    second = await provider.send(
        recipient="customer-1", message="created again", idempotency_key=event_id
    )

    assert first == second
    assert provider.attempts == [event_id, event_id]
    assert len(provider.receipts) == 1
