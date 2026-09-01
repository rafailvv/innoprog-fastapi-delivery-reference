from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID


# The public registration flow belongs to one demonstration organisation.
# Real production onboarding would resolve this value from a server-verified
# invitation or an administrator action, never from an arbitrary request field.
DEFAULT_TENANT_ID = UUID("00000000-0000-0000-0000-000000000001")


class Role(StrEnum):
    CUSTOMER = "customer"
    COURIER = "courier"
    DISPATCHER = "dispatcher"


@dataclass(frozen=True, slots=True)
class Actor:
    user_id: UUID
    role: Role
    tenant_id: UUID

    @property
    def subject(self) -> str:
        return f"{self.role.value}:{self.user_id}"


class Action(StrEnum):
    READ = "read"
    UPDATE = "update"
    TRANSITION = "transition"


def can_access_order(
    actor: Actor,
    *,
    order_tenant_id: UUID,
    customer_id: UUID,
    courier_id: UUID | None,
) -> bool:
    """Deny by default, then grant one explicit relationship.

    A dispatcher is privileged only inside the dispatcher's own tenant.  A
    matching customer/courier identifier from another tenant never grants
    access, which makes the organisation boundary the first invariant.
    """
    if actor.tenant_id != order_tenant_id:
        return False
    if actor.role is Role.DISPATCHER:
        return True
    if actor.role is Role.CUSTOMER:
        return actor.user_id == customer_id
    if actor.role is Role.COURIER:
        return courier_id is not None and actor.user_id == courier_id
    return False
