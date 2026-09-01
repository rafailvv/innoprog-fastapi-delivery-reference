from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from delivery_service.db import RefreshSessionRow


class RotationResult(StrEnum):
    ROTATED = "rotated"
    REUSED = "reused"
    INVALID = "invalid"


@dataclass
class RefreshSession:
    jti: UUID
    family_id: UUID
    subject: str
    expires_at: datetime
    used_at: datetime | None = None
    revoked_at: datetime | None = None
    replacement_jti: UUID | None = None


class RefreshSessionRepository(Protocol):
    async def register(self, session: RefreshSession) -> None: ...
    async def rotate(
        self,
        *,
        presented_jti: UUID,
        family_id: UUID,
        subject: str,
        replacement: RefreshSession,
        now: datetime,
    ) -> RotationResult: ...


class InMemoryRefreshSessionRepository:
    def __init__(self) -> None:
        self._sessions: dict[UUID, RefreshSession] = {}
        self._lock = asyncio.Lock()

    async def register(self, session: RefreshSession) -> None:
        async with self._lock:
            if session.jti in self._sessions:
                raise ValueError("refresh jti already exists")
            self._sessions[session.jti] = session

    async def rotate(
        self,
        *,
        presented_jti: UUID,
        family_id: UUID,
        subject: str,
        replacement: RefreshSession,
        now: datetime,
    ) -> RotationResult:
        async with self._lock:
            current = self._sessions.get(presented_jti)
            if (
                current is None
                or current.family_id != family_id
                or current.subject != subject
            ):
                return RotationResult.INVALID
            if current.used_at is not None or current.revoked_at is not None:
                for item in self._sessions.values():
                    if item.family_id == family_id and item.revoked_at is None:
                        item.revoked_at = now
                return RotationResult.REUSED
            if current.expires_at <= now:
                current.revoked_at = now
                return RotationResult.INVALID
            if (
                replacement.family_id != family_id
                or replacement.subject != subject
                or replacement.jti in self._sessions
            ):
                return RotationResult.INVALID

            current.used_at = now
            current.replacement_jti = replacement.jti
            self._sessions[replacement.jti] = replacement
            return RotationResult.ROTATED


class SqlAlchemyRefreshSessionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def register(self, session: RefreshSession) -> None:
        self._session.add(
            RefreshSessionRow(
                jti=session.jti,
                family_id=session.family_id,
                subject=session.subject,
                expires_at=session.expires_at,
            )
        )
        await self._session.flush()

    async def rotate(
        self,
        *,
        presented_jti: UUID,
        family_id: UUID,
        subject: str,
        replacement: RefreshSession,
        now: datetime,
    ) -> RotationResult:
        current = await self._session.scalar(
            select(RefreshSessionRow)
            .where(RefreshSessionRow.jti == presented_jti)
            .with_for_update()
        )
        if (
            current is None
            or current.family_id != family_id
            or current.subject != subject
        ):
            return RotationResult.INVALID
        if current.used_at is not None or current.revoked_at is not None:
            await self._session.execute(
                update(RefreshSessionRow)
                .where(
                    RefreshSessionRow.family_id == family_id,
                    RefreshSessionRow.revoked_at.is_(None),
                )
                .values(revoked_at=now)
            )
            return RotationResult.REUSED
        if current.expires_at <= now:
            current.revoked_at = now
            return RotationResult.INVALID
        if replacement.family_id != family_id or replacement.subject != subject:
            return RotationResult.INVALID

        current.used_at = now
        current.replacement_jti = replacement.jti
        self._session.add(
            RefreshSessionRow(
                jti=replacement.jti,
                family_id=replacement.family_id,
                subject=replacement.subject,
                expires_at=replacement.expires_at,
            )
        )
        await self._session.flush()
        return RotationResult.ROTATED
