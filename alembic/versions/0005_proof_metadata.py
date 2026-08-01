"""store proof metadata separately from private object bytes"""

from alembic import op
import sqlalchemy as sa


revision = "0005"
down_revision = "0004"


def upgrade() -> None:
    op.add_column(
        "delivery_notification",
        sa.Column("channel", sa.String(length=20), nullable=False, server_default="email"),
    )
    op.add_column(
        "delivery_notification",
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
    )
    op.add_column(
        "delivery_notification",
        sa.Column("provider_message_id", sa.String(length=200), nullable=True),
    )
    op.add_column(
        "delivery_notification",
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        "notification_preference",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(length=20), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.CheckConstraint(
            "channel IN ('email','telegram')", name="ck_notification_channel"
        ),
        sa.UniqueConstraint(
            "tenant_id", "user_id", "channel", name="uq_notification_preference"
        ),
    )
    op.create_table(
        "proof_metadata",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("object_key", sa.String(length=300), nullable=False),
        sa.Column("proof_kind", sa.String(length=20), nullable=False),
        sa.Column("media_type", sa.String(length=100), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "proof_kind IN ('pickup','delivery')", name="ck_proof_metadata_kind"
        ),
        sa.CheckConstraint("size_bytes > 0", name="ck_proof_metadata_size"),
        sa.ForeignKeyConstraint(
            ["order_id"], ["delivery_order.id"],
            name="fk_proof_metadata_order", ondelete="CASCADE",
        ),
        sa.UniqueConstraint("object_key", name="uq_proof_metadata_object_key"),
        sa.UniqueConstraint(
            "tenant_id", "order_id", "sha256", name="uq_proof_metadata_digest"
        ),
    )
    op.create_index(
        "ix_proof_metadata_order_created",
        "proof_metadata",
        ["tenant_id", "order_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_proof_metadata_order_created", table_name="proof_metadata")
    op.drop_table("proof_metadata")
    op.drop_table("notification_preference")
    op.drop_column("delivery_notification", "sent_at")
    op.drop_column("delivery_notification", "provider_message_id")
    op.drop_column("delivery_notification", "status")
    op.drop_column("delivery_notification", "channel")
