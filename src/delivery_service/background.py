from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from uuid import UUID


@dataclass(slots=True)
class PreviewCache:
    """Disposable, process-local projection used only for the lesson example.

    Losing this cache during a restart is acceptable: a later request can rebuild
    the projection from the durable order.  It must never become the source of
    truth for order state.
    """

    values: dict[UUID, int] = field(default_factory=dict)

    async def rebuild(self, order_id: UUID, version: int) -> None:
        # A scheduling point makes the lifecycle visible in tests without adding
        # an artificial network dependency to the reference project.
        await asyncio.sleep(0)
        self.values[order_id] = version

    def version_for(self, order_id: UUID) -> int | None:
        return self.values.get(order_id)

    def clear(self) -> None:
        self.values.clear()
