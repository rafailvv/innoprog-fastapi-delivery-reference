"""add durable latest tracking snapshot

Revision ID: 0007
Revises: 0006
"""

from alembic import op
import sqlalchemy as sa


revision = "0007"
down_revision = "0006"


def upgrade() -> None:
    op.create_table(
        "order_tracking_snapshot",
        sa.Column("order_id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("order_status", sa.String(length=32), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("client_event_id", sa.Uuid(), nullable=False, unique=True),
        sa.CheckConstraint("latitude >= -90 AND latitude <= 90", name="ck_tracking_latitude"),
        sa.CheckConstraint("longitude >= -180 AND longitude <= 180", name="ck_tracking_longitude"),
        sa.CheckConstraint("sequence > 0", name="ck_tracking_sequence"),
        sa.ForeignKeyConstraint(
            ["order_id"], ["delivery_order.id"],
            name="fk_tracking_snapshot_order", ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_tracking_snapshot_tenant_order",
        "order_tracking_snapshot",
        ["tenant_id", "order_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_tracking_snapshot_tenant_order", table_name="order_tracking_snapshot")
    op.drop_table("order_tracking_snapshot")
