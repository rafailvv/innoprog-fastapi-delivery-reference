from sqlalchemy import CheckConstraint, inspect

from delivery_service.db import AssignmentRow, OrderRow


def test_order_metadata_keeps_named_row_invariants() -> None:
    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in OrderRow.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }

    assert set(checks) == {"ck_order_weight", "ck_order_status", "ck_order_version"}
    assert "weight_grams > 0" in checks["ck_order_weight"]
    assert "cancelled" in checks["ck_order_status"]
    assert checks["ck_order_version"] == "version > 0"


def test_assignment_metadata_matches_active_uniqueness_and_delete_policy() -> None:
    indexes = {index.name: index for index in AssignmentRow.__table__.indexes}
    assert indexes["uq_active_assignment_order"].unique is True
    assert indexes["uq_active_assignment_courier"].unique is True
    assert str(indexes["uq_active_assignment_order"].dialect_options["postgresql"]["where"]) == "active"
    assert str(indexes["uq_active_assignment_courier"].dialect_options["postgresql"]["where"]) == "active"

    order_id = AssignmentRow.__table__.c.order_id
    foreign_key = next(iter(order_id.foreign_keys))
    assert foreign_key.target_fullname == "delivery_order.id"
    assert foreign_key.constraint.name == "fk_assignment_order"
    assert foreign_key.ondelete == "CASCADE"


def test_relationships_are_bidirectional_and_never_hide_async_io() -> None:
    order_relationship = inspect(OrderRow).relationships["assignments"]
    assignment_relationship = inspect(AssignmentRow).relationships["order"]

    assert order_relationship.back_populates == "order"
    assert assignment_relationship.back_populates == "assignments"
    assert order_relationship.lazy == "raise"
    assert assignment_relationship.lazy == "raise"
    assert order_relationship.passive_deletes is True
    assert "delete-orphan" in order_relationship.cascade
