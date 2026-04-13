from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260413_0002"
down_revision = "20260413_0001"
branch_labels = None
depends_on = None


review_queue_kind = postgresql.ENUM(
    "domain_resolution",
    "public_contact",
    name="reviewqueuekind",
    create_type=False,
)
review_queue_status = postgresql.ENUM(
    "pending",
    "resolved",
    "dismissed",
    name="reviewqueuestatus",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    review_queue_kind.create(bind, checkfirst=True)
    review_queue_status.create(bind, checkfirst=True)

    op.create_table(
        "review_queue_item",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("domain_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("queue_kind", review_queue_kind, nullable=False),
        sa.Column("reason", sa.String(length=128), nullable=False),
        sa.Column("status", review_queue_status, nullable=False),
        sa.Column("fingerprint", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["domain_id"],
            ["official_domain.id"],
            name=op.f("fk_review_queue_item_domain_id_official_domain"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["business_entity.id"],
            name=op.f("fk_review_queue_item_entity_id_business_entity"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_review_queue_item")),
        sa.UniqueConstraint("fingerprint", name=op.f("uq_review_queue_item_fingerprint")),
    )
    op.create_index(op.f("ix_review_queue_item_entity_id"), "review_queue_item", ["entity_id"], unique=False)
    op.create_index(op.f("ix_review_queue_item_queue_kind"), "review_queue_item", ["queue_kind"], unique=False)
    op.create_index(op.f("ix_review_queue_item_reason"), "review_queue_item", ["reason"], unique=False)
    op.create_index(op.f("ix_review_queue_item_status"), "review_queue_item", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_review_queue_item_status"), table_name="review_queue_item")
    op.drop_index(op.f("ix_review_queue_item_reason"), table_name="review_queue_item")
    op.drop_index(op.f("ix_review_queue_item_queue_kind"), table_name="review_queue_item")
    op.drop_index(op.f("ix_review_queue_item_entity_id"), table_name="review_queue_item")
    op.drop_table("review_queue_item")

    bind = op.get_bind()
    review_queue_status.drop(bind, checkfirst=True)
    review_queue_kind.drop(bind, checkfirst=True)
