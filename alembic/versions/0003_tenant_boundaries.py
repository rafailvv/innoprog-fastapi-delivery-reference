"""add server-owned tenant boundaries"""

from alembic import op
import sqlalchemy as sa


revision = "0003"
down_revision = "0002"

DEFAULT_TENANT = "00000000-0000-0000-0000-000000000001"


def upgrade() -> None:
    op.add_column(
        "delivery_order",
        sa.Column(
            "tenant_id", sa.Uuid(), nullable=False, server_default=DEFAULT_TENANT,
        ),
    )
    op.create_index(
        "ix_delivery_order_tenant_id", "delivery_order", ["tenant_id"]
    )
    op.add_column(
        "identity",
        sa.Column(
            "tenant_id", sa.Uuid(), nullable=False, server_default=DEFAULT_TENANT,
        ),
    )
    op.create_index("ix_identity_tenant_id", "identity", ["tenant_id"])


def downgrade() -> None:
    op.drop_index("ix_identity_tenant_id", table_name="identity")
    op.drop_column("identity", "tenant_id")
    op.drop_index("ix_delivery_order_tenant_id", table_name="delivery_order")
    op.drop_column("delivery_order", "tenant_id")
