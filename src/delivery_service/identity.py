from __future__ import annotations

from typing import Protocol

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from delivery_service.db import IdentityRow
from delivery_service.access import DEFAULT_TENANT_ID
from uuid import UUID


class IdentityRepository(Protocol):
    async def password_hash(self, subject: str) -> str | None: ...
    async def tenant_id(self, subject: str) -> UUID | None: ...
    async def add(
        self, subject: str, password_hash: str, tenant_id: UUID = DEFAULT_TENANT_ID,
    ) -> bool: ...
    async def replace_password_hash(
        self, subject: str, *, expected: str, replacement: str,
    ) -> bool: ...


class InMemoryIdentityRepository:
    def __init__(self) -> None:
        self._passwords: dict[str, str] = {}
        self._tenants: dict[str, UUID] = {}

    async def password_hash(self, subject: str) -> str | None:
        return self._passwords.get(subject)

    async def tenant_id(self, subject: str) -> UUID | None:
        return self._tenants.get(subject)

    async def add(
        self, subject: str, password_hash: str, tenant_id: UUID = DEFAULT_TENANT_ID,
    ) -> bool:
        if subject in self._passwords:
            return False
        self._passwords[subject] = password_hash
        self._tenants[subject] = tenant_id
        return True

    async def replace_password_hash(
        self, subject: str, *, expected: str, replacement: str,
    ) -> bool:
        if self._passwords.get(subject) != expected:
            return False
        self._passwords[subject] = replacement
        return True


class SqlAlchemyIdentityRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def password_hash(self, subject: str) -> str | None:
        return await self._session.scalar(
            select(IdentityRow.password_hash).where(IdentityRow.subject == subject)
        )

    async def tenant_id(self, subject: str) -> UUID | None:
        return await self._session.scalar(
            select(IdentityRow.tenant_id).where(IdentityRow.subject == subject)
        )

    async def add(
        self, subject: str, password_hash: str, tenant_id: UUID = DEFAULT_TENANT_ID,
    ) -> bool:
        from sqlalchemy.dialects.postgresql import insert

        statement = (
            insert(IdentityRow)
            .values(subject=subject, password_hash=password_hash, tenant_id=tenant_id)
            .on_conflict_do_nothing(index_elements=[IdentityRow.subject])
            .returning(IdentityRow.subject)
        )
        return await self._session.scalar(statement) is not None

    async def replace_password_hash(
        self, subject: str, *, expected: str, replacement: str,
    ) -> bool:
        result = await self._session.execute(
            update(IdentityRow)
            .where(
                IdentityRow.subject == subject,
                IdentityRow.password_hash == expected,
            )
            .values(password_hash=replacement)
        )
        return result.rowcount == 1
