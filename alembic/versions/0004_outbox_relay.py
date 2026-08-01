"""expand outbox publication lifecycle without closing rollback window"""

from alembic import op
import sqlalchemy as sa


revision = "0004"
down_revision = "0003"


def upgrade() -> None:
    # ``processed_at`` stays during the compatibility window: the previous
    # image still reads it after an application rollback.  The new image
    # dual-reads and dual-writes both columns until a later contract release.
    op.add_column(
        "outbox",
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "outbox",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.execute(
        "UPDATE outbox SET published_at = processed_at "
        "WHERE published_at IS NULL AND processed_at IS NOT NULL"
    )
    op.create_index(
        "ix_outbox_pending_created",
        "outbox",
        ["published_at", "processed_at", "created_at", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_pending_created", table_name="outbox")
    op.drop_column("outbox", "created_at")
    op.drop_column("outbox", "published_at")
