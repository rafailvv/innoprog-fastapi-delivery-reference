import asyncio
from uuid import uuid4

import pytest

from delivery_service.repository import ConcurrentUpdate, InMemoryOrderRepository
from delivery_service.commands import CreateOrderCommand
from delivery_service.service import OrderService


@pytest.mark.asyncio
async def test_assignment_race_rejects_one_stale_version() -> None:
    repository = InMemoryOrderRepository()
    service = OrderService(repository)
    order = await service.create(CreateOrderCommand(
        customer_id=uuid4(), pickup_address="A street 10",
        destination_address="B street 20", weight_grams=500,
    ))
    barrier = asyncio.Barrier(2)

    async def assign(courier_id):
        snapshot = await repository.get(order.id)
        await barrier.wait()
        await repository.save(snapshot.assign(courier_id), expected_version=snapshot.version)

    tasks = [asyncio.create_task(assign(uuid4())) for _ in range(2)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert sum(isinstance(result, ConcurrentUpdate) for result in results) == 1
