from __future__ import annotations

import ast
from pathlib import Path
from uuid import UUID

import pytest

from delivery_service.commands import CreateOrderCommand
from delivery_service.repository import InMemoryOrderRepository
from delivery_service.service import OrderService


PACKAGE = Path(__file__).resolve().parents[1] / "src" / "delivery_service"
FORBIDDEN_INNER_IMPORTS = {
    "fastapi", "starlette", "pydantic", "sqlalchemy", "redis", "celery", "httpx",
}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text("utf-8"), filename=str(path))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".", 1)[0])
    return roots


def test_inner_layers_do_not_import_transport_or_infrastructure_frameworks() -> None:
    inner = ("domain.py", "commands.py", "ports.py", "service.py")
    violations = {
        name: sorted(_imports(PACKAGE / name) & FORBIDDEN_INNER_IMPORTS)
        for name in inner
        if _imports(PACKAGE / name) & FORBIDDEN_INNER_IMPORTS
    }
    assert violations == {}

    service_source = (PACKAGE / "service.py").read_text("utf-8")
    repository_source = (PACKAGE / "repository.py").read_text("utf-8")
    assert "from delivery_service.commands import" in service_source
    assert "from delivery_service.ports import OrderRepository" in service_source
    assert "from delivery_service.ports import OrderRepository" in repository_source
    assert "from sqlalchemy" in repository_source


@pytest.mark.asyncio
async def test_domain_rejection_has_no_repository_or_event_effect() -> None:
    repository = InMemoryOrderRepository()
    service = OrderService(repository)
    invalid = CreateOrderCommand(
        customer_id=UUID(int=67),
        pickup_address="Казань, Баумана, 1",
        destination_address="Казань, Кремлёвская, 18",
        weight_grams=0,
    )

    with pytest.raises(ValueError, match="weight_grams"):
        await service.create(invalid)

    assert await repository.list(limit=10, offset=0) == []
    assert repository.outbox == []


def test_fastapi_depends_stays_in_the_composition_root() -> None:
    service_source = (PACKAGE / "service.py").read_text("utf-8")
    main_source = (PACKAGE / "main.py").read_text("utf-8")
    assert "Depends(" not in service_source
    assert "def get_order_service(" in main_source
    assert "Depends(get_order_repository)" in main_source
