from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Float,
    Index,
    Integer,
    JSON,
    String,
    UniqueConstraint,
    Uuid,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, selectinload

from delivery_service.config import get_settings
from delivery_service.access import DEFAULT_TENANT_ID


class Base(DeclarativeBase):
    pass


class OrderRow(Base):
    __tablename__ = "delivery_order"
    __table_args__ = (
        CheckConstraint(
            "weight_grams > 0 AND weight_grams <= 100000", name="ck_order_weight"
        ),
        CheckConstraint(
            "status IN ('created','assigned','picked_up','delivered','cancelled')",
            name="ck_order_status",
        ),
        CheckConstraint("version > 0", name="ck_order_version"),
        Index("ix_delivery_order_status_created", "status", "created_at", "id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        Uuid, nullable=False, default=DEFAULT_TENANT_ID, index=True
    )
    customer_id: Mapped[UUID] = mapped_column(Uuid, nullable=False, index=True)
    pickup_address: Mapped[str] = mapped_column(String(300), nullable=False)
    destination_address: Mapped[str] = mapped_column(String(300), nullable=False)
    weight_grams: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    assignments: Mapped[list["AssignmentRow"]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="raise",
    )


class AssignmentRow(Base):
    __tablename__ = "courier_assignment"
    __table_args__ = (
        Index(
            "uq_active_assignment_order",
            "order_id",
            unique=True,
            postgresql_where=text("active"),
        ),
        Index(
            "uq_active_assignment_courier",
            "courier_id",
            unique=True,
            postgresql_where=text("active"),
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey(
            "delivery_order.id", name="fk_assignment_order", ondelete="CASCADE"
        ),
        nullable=False,
    )
    courier_id: Mapped[UUID] = mapped_column(Uuid, nullable=False, index=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    order: Mapped[OrderRow] = relationship(
        back_populates="assignments", lazy="raise"
    )


def orm_mapping_contract() -> dict[str, object]:
    """Expose configured mapper metadata to development-only black-box tests."""
    from sqlalchemy import inspect

    order_relationship = inspect(OrderRow).relationships["assignments"]
    assignment_relationship = inspect(AssignmentRow).relationships["order"]
    foreign_key = next(iter(AssignmentRow.__table__.c.order_id.foreign_keys))
    indexes = {index.name: index for index in AssignmentRow.__table__.indexes}
    return {
        "tables": sorted((OrderRow.__tablename__, AssignmentRow.__tablename__)),
        "foreign_key": {
            "target": foreign_key.target_fullname,
            "name": foreign_key.constraint.name,
            "ondelete": foreign_key.ondelete,
        },
        "relationships": {
            "order_assignments": {
                "back_populates": order_relationship.back_populates,
                "lazy": order_relationship.lazy,
                "passive_deletes": order_relationship.passive_deletes,
                "delete_orphan": "delete-orphan" in order_relationship.cascade,
            },
            "assignment_order": {
                "back_populates": assignment_relationship.back_populates,
                "lazy": assignment_relationship.lazy,
            },
        },
        "partial_unique_indexes": sorted(
            name
            for name, index in indexes.items()
            if index.unique
            and index.dialect_options["postgresql"].get("where") is not None
        ),
    }


class IdempotencyRow(Base):
    __tablename__ = "idempotency_record"

    key: Mapped[str] = mapped_column(String(200), primary_key=True)
    operation: Mapped[str] = mapped_column(String(100))
    resource_id: Mapped[UUID] = mapped_column(Uuid, index=True)
    response: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class OutboxRow(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_pending_created",
            "published_at", "processed_at", "created_at", "id",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict] = mapped_column(JSON)
    # Temporary expand/contract bridge for rollback to the previous relay.
    # Remove only in a later contract migration after that image is retired.
    legacy_processed_at: Mapped[datetime | None] = mapped_column(
        "processed_at", DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TransitionHistoryRow(Base):
    """Immutable business audit row written with the aggregate and outbox."""

    __tablename__ = "order_transition_history"
    __table_args__ = (
        CheckConstraint(
            "from_status IN ('created','assigned','picked_up','delivered','cancelled')",
            name="ck_transition_history_from_status",
        ),
        CheckConstraint(
            "to_status IN ('created','assigned','picked_up','delivered','cancelled')",
            name="ck_transition_history_to_status",
        ),
        CheckConstraint("from_status <> to_status", name="ck_transition_history_changed"),
        CheckConstraint("order_version > 1", name="ck_transition_history_version"),
        Index(
            "ix_transition_history_order_version",
            "tenant_id", "order_id", "order_version",
            unique=True,
        ),
    )

    event_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    tenant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    order_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey(
            "delivery_order.id", name="fk_transition_history_order", ondelete="CASCADE"
        ),
        nullable=False,
    )
    from_status: Mapped[str] = mapped_column(String(32), nullable=False)
    to_status: Mapped[str] = mapped_column(String(32), nullable=False)
    order_version: Mapped[int] = mapped_column(Integer, nullable=False)
    actor_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    actor_role: Mapped[str] = mapped_column(String(32), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(128), nullable=False)


class TrackingSnapshotRow(Base):
    """Latest confirmed courier position; live connections are only a projection."""

    __tablename__ = "order_tracking_snapshot"
    __table_args__ = (
        CheckConstraint("latitude >= -90 AND latitude <= 90", name="ck_tracking_latitude"),
        CheckConstraint("longitude >= -180 AND longitude <= 180", name="ck_tracking_longitude"),
        CheckConstraint("sequence > 0", name="ck_tracking_sequence"),
        Index("ix_tracking_snapshot_tenant_order", "tenant_id", "order_id", unique=True),
    )

    order_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("delivery_order.id", name="fk_tracking_snapshot_order", ondelete="CASCADE"),
        primary_key=True,
    )
    tenant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    order_status: Mapped[str] = mapped_column(String(32), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    client_event_id: Mapped[UUID] = mapped_column(Uuid, nullable=False, unique=True)


class ConsumerInboxRow(Base):
    __tablename__ = "consumer_inbox"

    event_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100))
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class NotificationRow(Base):
    __tablename__ = "delivery_notification"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(
        Uuid, ForeignKey("consumer_inbox.event_id", ondelete="CASCADE"), unique=True
    )
    order_id: Mapped[UUID] = mapped_column(Uuid, index=True)
    message: Mapped[str] = mapped_column(String(300))
    channel: Mapped[str] = mapped_column(String(20), default="email", nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False)
    provider_message_id: Mapped[str | None] = mapped_column(String(200))
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class NotificationPreferenceRow(Base):
    __tablename__ = "notification_preference"
    __table_args__ = (
        CheckConstraint("channel IN ('email','telegram')", name="ck_notification_channel"),
        UniqueConstraint(
            "tenant_id", "user_id", "channel", name="uq_notification_preference"
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    user_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class ProofMetadataRow(Base):
    __tablename__ = "proof_metadata"
    __table_args__ = (
        CheckConstraint(
            "proof_kind IN ('pickup','delivery')", name="ck_proof_metadata_kind"
        ),
        CheckConstraint("size_bytes > 0", name="ck_proof_metadata_size"),
        UniqueConstraint("object_key", name="uq_proof_metadata_object_key"),
        UniqueConstraint(
            "tenant_id", "order_id", "sha256", name="uq_proof_metadata_digest"
        ),
        Index("ix_proof_metadata_order_created", "tenant_id", "order_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    order_id: Mapped[UUID] = mapped_column(
        Uuid,
        ForeignKey("delivery_order.id", name="fk_proof_metadata_order", ondelete="CASCADE"),
        nullable=False,
    )
    object_key: Mapped[str] = mapped_column(String(300), nullable=False)
    proof_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    media_type: Mapped[str] = mapped_column(String(100), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class IdentityRow(Base):
    __tablename__ = "identity"

    subject: Mapped[str] = mapped_column(String(100), primary_key=True)
    tenant_id: Mapped[UUID] = mapped_column(
        Uuid, nullable=False, default=DEFAULT_TENANT_ID, index=True
    )
    password_hash: Mapped[str] = mapped_column(String(300), nullable=False)


class RefreshSessionRow(Base):
    __tablename__ = "refresh_session"

    jti: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    family_id: Mapped[UUID] = mapped_column(Uuid, nullable=False, index=True)
    subject: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    replacement_jti: Mapped[UUID | None] = mapped_column(Uuid)


database_settings = get_settings()
engine = create_async_engine(
    database_settings.database_url,
    pool_size=database_settings.database_pool_size,
    max_overflow=database_settings.database_max_overflow,
    pool_timeout=database_settings.database_pool_timeout_seconds,
    pool_recycle=database_settings.database_pool_recycle_seconds,
    pool_pre_ping=True,
)
session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@asynccontextmanager
async def transactional_session(
    factory: async_sessionmaker[AsyncSession] = session_factory,
) -> AsyncIterator[AsyncSession]:
    """Own one Session and one transaction for exactly one unit of work."""
    from opentelemetry.trace import SpanKind
    from delivery_service.observability import traced_operation

    with traced_operation(
        "postgresql transaction",
        kind=SpanKind.CLIENT,
        attributes={"db.system": "postgresql", "db.operation.name": "transaction"},
    ):
        async with factory() as session:
            async with session.begin():
                yield session


async def session_runtime_contract() -> dict[str, object]:
    """Observe factory isolation and bounded process-level pool settings."""
    async with session_factory() as first, session_factory() as second:
        return {
            "distinct_sessions": first is not second,
            "distinct_identity_maps": first.identity_map is not second.identity_map,
            "expire_on_commit": first.sync_session.expire_on_commit,
            "pool": {
                "size": engine.pool.size(),
                "max_overflow": database_settings.database_max_overflow,
                "timeout_seconds": engine.pool.timeout(),
                "recycle_seconds": database_settings.database_pool_recycle_seconds,
                "pre_ping": engine.pool._pre_ping,
            },
        }


async def load_orders(session: AsyncSession) -> list[OrderRow]:
    query = select(OrderRow).options(selectinload(OrderRow.assignments))
    return list((await session.scalars(query)).all())


async def lock_available_order(session: AsyncSession) -> OrderRow | None:
    """Lock an order inside a transaction owned by the caller."""
    if not session.in_transaction():
        raise RuntimeError("lock_available_order requires an active transaction")
    query = (
        select(OrderRow)
        .where(OrderRow.status == "created")
        .order_by(OrderRow.created_at, OrderRow.id)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    return await session.scalar(query)


async def claim_available_order(session: AsyncSession, courier_id: UUID) -> UUID | None:
    """Lock, mutate and flush before releasing the transaction-level lock."""
    async with session.begin():
        order = await lock_available_order(session)
        if order is None:
            return None
        order.status = "assigned"
        order.version += 1
        session.add(AssignmentRow(order_id=order.id, courier_id=courier_id, active=True))
        await session.flush()
        return order.id
