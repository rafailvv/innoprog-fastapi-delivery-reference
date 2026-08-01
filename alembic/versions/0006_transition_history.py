"""add immutable order transition history

Revision ID: 0006
Revises: 0005
"""

from alembic import op
import sqlalchemy as sa


revision = "0006"
down_revision = "0005"


def upgrade() -> None:
    op.create_table(
        "order_transition_history",
        sa.Column("event_id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("from_status", sa.String(length=32), nullable=False),
        sa.Column("to_status", sa.String(length=32), nullable=False),
        sa.Column("order_version", sa.Integer(), nullable=False),
        sa.Column("actor_id", sa.Uuid(), nullable=False),
        sa.Column("actor_role", sa.String(length=32), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("correlation_id", sa.String(length=128), nullable=False),
        sa.CheckConstraint(
            "from_status IN ('created','assigned','picked_up','delivered','cancelled')",
            name="ck_transition_history_from_status",
        ),
        sa.CheckConstraint(
            "to_status IN ('created','assigned','picked_up','delivered','cancelled')",
            name="ck_transition_history_to_status",
        ),
        sa.CheckConstraint(
            "from_status <> to_status", name="ck_transition_history_changed"
        ),
        sa.CheckConstraint(
            "order_version > 1", name="ck_transition_history_version"
        ),
        sa.ForeignKeyConstraint(
            ["order_id"], ["delivery_order.id"],
            name="fk_transition_history_order", ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_transition_history_order_version",
        "order_transition_history",
        ["tenant_id", "order_id", "order_version"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_transition_history_order_version",
        table_name="order_transition_history",
    )
    op.drop_table("order_transition_history")
