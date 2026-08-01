"""Private proof objects, durable metadata and short-lived capability links."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import hmac
from pathlib import PurePath
import re
from typing import Literal, Protocol
from urllib.parse import urlencode
from uuid import UUID, uuid4

from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession


PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
JPEG_SIGNATURE = b"\xff\xd8\xff"
READ_CHUNK_SIZE = 64 * 1024
PROOF_KEY = re.compile(
    r"^proofs/[0-9a-f-]{36}/[0-9a-f-]{36}/[0-9a-f]{64}\.(?:png|jpg)$"
)


class InvalidUpload(ValueError):
    """The body does not satisfy the proof-file policy."""


class UploadTooLarge(InvalidUpload):
    """The body exceeded the configured application limit."""


class InvalidProofLink(ValueError):
    """A proof capability was malformed, expired or changed after signing."""


@dataclass(frozen=True, slots=True)
class StoredProof:
    key: str
    tenant_id: UUID
    order_id: UUID
    media_type: str
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class ValidatedProof:
    metadata: StoredProof
    content: bytes


@dataclass(frozen=True, slots=True)
class SignedProofLink:
    url: str
    expires_at: datetime


class ProofObjectStorage(Protocol):
    """Blob boundary. Implementations keep every object private by default."""

    async def put_private(self, proof: ValidatedProof) -> StoredProof: ...

    async def get_private(self, key: str) -> tuple[bytes, str] | None: ...


class InMemoryProofStorage:
    """Development adapter with the same private put/get contract as object storage."""

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, str]] = {}

    async def put_private(self, proof: ValidatedProof) -> StoredProof:
        self._objects[proof.metadata.key] = (proof.content, proof.metadata.media_type)
        return proof.metadata

    async def get_private(self, key: str) -> tuple[bytes, str] | None:
        return self._objects.get(key)

    # Compatibility aliases keep the earlier upload lesson usable while the
    # course introduces the explicit private-storage port in lesson 60.
    async def put(self, proof: ValidatedProof) -> StoredProof:
        return await self.put_private(proof)

    async def get(self, key: str) -> tuple[bytes, str] | None:
        return await self.get_private(key)


class ProofMetadataRepository(Protocol):
    """PostgreSQL boundary for searchable metadata; blob bytes never enter it."""

    async def add(
        self,
        proof: StoredProof,
        *,
        proof_kind: Literal["pickup", "delivery"],
        created_at: datetime,
    ) -> None: ...

    async def find(
        self,
        *,
        tenant_id: UUID,
        order_id: UUID,
        sha256: str,
    ) -> StoredProof | None: ...


class InMemoryProofMetadataRepository:
    def __init__(self) -> None:
        self._records: dict[tuple[UUID, UUID, str], StoredProof] = {}

    async def add(
        self,
        proof: StoredProof,
        *,
        proof_kind: Literal["pickup", "delivery"],
        created_at: datetime,
    ) -> None:
        del proof_kind, created_at
        self._records[(proof.tenant_id, proof.order_id, proof.sha256)] = proof

    async def find(
        self,
        *,
        tenant_id: UUID,
        order_id: UUID,
        sha256: str,
    ) -> StoredProof | None:
        return self._records.get((tenant_id, order_id, sha256))


class SqlAlchemyProofMetadataRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(
        self,
        proof: StoredProof,
        *,
        proof_kind: Literal["pickup", "delivery"],
        created_at: datetime,
    ) -> None:
        from delivery_service.db import ProofMetadataRow

        self.session.add(ProofMetadataRow(
            id=uuid4(),
            tenant_id=proof.tenant_id,
            order_id=proof.order_id,
            object_key=proof.key,
            proof_kind=proof_kind,
            media_type=proof.media_type,
            size_bytes=proof.size,
            sha256=proof.sha256,
            created_at=created_at,
        ))
        await self.session.flush()

    async def find(
        self,
        *,
        tenant_id: UUID,
        order_id: UUID,
        sha256: str,
    ) -> StoredProof | None:
        from sqlalchemy import select

        from delivery_service.db import ProofMetadataRow

        row = await self.session.scalar(
            select(ProofMetadataRow).where(
                ProofMetadataRow.tenant_id == tenant_id,
                ProofMetadataRow.order_id == order_id,
                ProofMetadataRow.sha256 == sha256,
            )
        )
        if row is None:
            return None
        return StoredProof(
            key=row.object_key,
            tenant_id=row.tenant_id,
            order_id=row.order_id,
            media_type=row.media_type,
            size=row.size_bytes,
            sha256=row.sha256,
        )


class ProofLinkSigner:
    """Issue and verify GET-only bearer capabilities for one private object key."""

    def __init__(
        self,
        secret: str,
        *,
        max_ttl_seconds: int = 900,
        base_path: str = "/api/v1/proof-objects",
    ) -> None:
        if len(secret.encode("utf-8")) < 32:
            raise ValueError("proof link secret must contain at least 32 bytes")
        if not 1 <= max_ttl_seconds <= 3600:
            raise ValueError("proof link max TTL must be between 1 and 3600 seconds")
        self._secret = secret.encode("utf-8")
        self._max_ttl_seconds = max_ttl_seconds
        self._base_path = base_path

    @staticmethod
    def _message(*, operation: str, key: str, expires: int) -> bytes:
        return f"delivery.proof.v1\n{operation}\n{key}\n{expires}".encode("utf-8")

    def issue_get(
        self,
        key: str,
        *,
        now: datetime,
        ttl_seconds: int,
    ) -> SignedProofLink:
        if not PROOF_KEY.fullmatch(key):
            raise ValueError("unsafe proof object key")
        if not 1 <= ttl_seconds <= self._max_ttl_seconds:
            raise ValueError("proof link TTL exceeds policy")
        expires_at = now.astimezone(UTC) + timedelta(seconds=ttl_seconds)
        expires = int(expires_at.timestamp())
        signature = hmac.new(
            self._secret,
            self._message(operation="GET", key=key, expires=expires),
            hashlib.sha256,
        ).hexdigest()
        return SignedProofLink(
            url=f"{self._base_path}?{urlencode({'key': key, 'expires': expires, 'signature': signature})}",
            expires_at=expires_at,
        )

    def verify_get(
        self,
        *,
        key: str,
        expires: int,
        signature: str,
        now: datetime,
    ) -> None:
        current = int(now.astimezone(UTC).timestamp())
        if not PROOF_KEY.fullmatch(key):
            raise InvalidProofLink("invalid proof key")
        if expires < current or expires > current + self._max_ttl_seconds:
            raise InvalidProofLink("proof link expired or exceeds policy")
        if len(signature) != 64:
            raise InvalidProofLink("invalid proof signature")
        expected = hmac.new(
            self._secret,
            self._message(operation="GET", key=key, expires=expires),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise InvalidProofLink("invalid proof signature")


def _detect_image(body: bytes) -> tuple[str, str]:
    """Apply a cheap first filter; decoding/scanning belongs to a dedicated adapter."""

    if body.startswith(PNG_SIGNATURE):
        return "image/png", ".png"
    if body.startswith(JPEG_SIGNATURE):
        return "image/jpeg", ".jpg"
    raise InvalidUpload("proof is not a supported PNG or JPEG image")


async def inspect_proof(
    file: UploadFile,
    *,
    tenant_id: UUID,
    order_id: UUID,
    max_bytes: int,
) -> ValidatedProof:
    """Read at most max_bytes + one chunk and always close the spooled file."""

    chunks: list[bytes] = []
    size = 0
    try:
        while chunk := await file.read(READ_CHUNK_SIZE):
            size += len(chunk)
            if size > max_bytes:
                raise UploadTooLarge("proof exceeds configured size limit")
            chunks.append(chunk)
    finally:
        await file.close()

    body = b"".join(chunks)
    media_type, suffix = _detect_image(body)
    original_suffix = PurePath(file.filename or "").suffix.casefold()
    expected_suffixes = {".png"} if suffix == ".png" else {".jpg", ".jpeg"}
    if original_suffix and original_suffix not in expected_suffixes:
        raise InvalidUpload("filename extension does not match detected image type")

    digest = hashlib.sha256(body).hexdigest()
    return ValidatedProof(
        metadata=StoredProof(
            key=f"proofs/{tenant_id}/{order_id}/{digest}{suffix}",
            tenant_id=tenant_id,
            order_id=order_id,
            media_type=media_type,
            size=size,
            sha256=digest,
        ),
        content=body,
    )
