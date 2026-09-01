"""create delivery order and outbox tables"""

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None


def upgrade() -> None:
    op.create_table(
        "delivery_order",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("customer_id", sa.Uuid(), nullable=False),
        sa.Column("pickup_address", sa.String(300), nullable=False),
        sa.Column("destination_address", sa.String(300), nullable=False),
        sa.Column("weight_grams", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("weight_grams > 0 AND weight_grams <= 100000", name="ck_order_weight"),
        sa.CheckConstraint(
            "status IN ('created','assigned','picked_up','delivered','cancelled')",
            name="ck_order_status",
        ),
        sa.CheckConstraint("version > 0", name="ck_order_version"),
    )
    op.create_index("ix_delivery_order_customer_id", "delivery_order", ["customer_id"])
    op.create_table(
        "courier_assignment",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "order_id",
            sa.Uuid(),
            sa.ForeignKey(
                "delivery_order.id", name="fk_assignment_order", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column("courier_id", sa.Uuid(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.create_index("ix_courier_assignment_courier_id", "courier_assignment", ["courier_id"])
    op.create_table(
        "idempotency_record",
        sa.Column("key", sa.String(200), primary_key=True),
        sa.Column("operation", sa.String(100), nullable=False),
        sa.Column("resource_id", sa.Uuid(), nullable=False),
        sa.Column("response", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
    )
    op.create_table(
        "consumer_inbox",
        sa.Column("event_id", sa.Uuid(), primary_key=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "delivery_notification",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "event_id",
            sa.Uuid(),
            sa.ForeignKey("consumer_inbox.event_id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("message", sa.String(300), nullable=False),
    )
    op.create_index(
        "ix_delivery_notification_order_id", "delivery_notification", ["order_id"]
    )
    op.create_table(
        "identity",
        sa.Column("subject", sa.String(100), primary_key=True),
        sa.Column("password_hash", sa.String(300), nullable=False),
    )
    op.create_table(
        "refresh_session",
        sa.Column("jti", sa.Uuid(), primary_key=True),
        sa.Column("family_id", sa.Uuid(), nullable=False),
        sa.Column("subject", sa.String(100), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True)),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.Column("replacement_jti", sa.Uuid()),
    )
    op.create_index("ix_refresh_session_family_id", "refresh_session", ["family_id"])
    op.create_index("ix_refresh_session_subject", "refresh_session", ["subject"])


def downgrade() -> None:
    op.drop_index("ix_refresh_session_subject", table_name="refresh_session")
    op.drop_index("ix_refresh_session_family_id", table_name="refresh_session")
    op.drop_table("refresh_session")
    op.drop_table("identity")
    op.drop_index("ix_delivery_notification_order_id", table_name="delivery_notification")
    op.drop_table("delivery_notification")
    op.drop_table("consumer_inbox")
    op.drop_table("outbox")
    op.drop_table("idempotency_record")
    op.drop_index("ix_courier_assignment_courier_id", table_name="courier_assignment")
    op.drop_table("courier_assignment")
    op.drop_index("ix_delivery_order_customer_id", table_name="delivery_order")
    op.drop_table("delivery_order")
