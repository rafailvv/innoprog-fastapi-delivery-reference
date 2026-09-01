"""add ordering and active-assignment indexes"""

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_delivery_order_status_created",
        "delivery_order",
        ["status", "created_at", "id"],
    )
    op.create_index(
        "uq_active_assignment_order",
        "courier_assignment",
        ["order_id"],
        unique=True,
        postgresql_where=sa.text("active"),
    )
    op.create_index(
        "uq_active_assignment_courier",
        "courier_assignment",
        ["courier_id"],
        unique=True,
        postgresql_where=sa.text("active"),
    )
    op.create_index(
        "ix_idempotency_record_resource_id",
        "idempotency_record",
        ["resource_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_idempotency_record_resource_id", table_name="idempotency_record")
    op.drop_index("uq_active_assignment_courier", table_name="courier_assignment")
    op.drop_index("uq_active_assignment_order", table_name="courier_assignment")
    op.drop_index("ix_delivery_order_status_created", table_name="delivery_order")
