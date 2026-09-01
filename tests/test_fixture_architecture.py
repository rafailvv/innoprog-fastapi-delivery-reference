from __future__ import annotations

from datetime import datetime
from uuid import UUID

import pytest

from delivery_service.repository import InMemoryOrderRepository


def test_fixture_is_cached_once_inside_one_test_item(
    repository: InMemoryOrderRepository,
    request: pytest.FixtureRequest,
) -> None:
    assert request.getfixturevalue("repository") is repository


@pytest.mark.asyncio
async def test_repository_starts_empty_when_suite_order_changes(
    repository: InMemoryOrderRepository,
) -> None:
    assert await repository.list(limit=10, offset=0) == []
    assert repository.outbox == []


def test_order_factory_returns_fresh_deterministic_objects(
    order_factory,
    fixed_now: datetime,
) -> None:
    first = order_factory()
    second = order_factory()

    assert (first.id, second.id) == (UUID(int=1), UUID(int=2))
    assert first.customer_id != second.customer_id
    assert first.created_at == second.created_at == fixed_now


@pytest.mark.parametrize(
    ("weight_grams", "accepted"),
    ((0, False), (1, True), (100_000, True), (100_001, False)),
    ids=("below-minimum", "minimum", "maximum", "above-maximum"),
)
def test_order_factory_preserves_domain_weight_boundaries(
    order_factory,
    weight_grams: int,
    accepted: bool,
) -> None:
    if accepted:
        assert order_factory(weight_grams=weight_grams).weight_grams == weight_grams
    else:
        with pytest.raises(ValueError, match="between 1 and 100000"):
            order_factory(weight_grams=weight_grams)
