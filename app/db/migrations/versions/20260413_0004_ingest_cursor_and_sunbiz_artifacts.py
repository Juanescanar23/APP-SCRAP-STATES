from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260413_0004"
down_revision = "20260413_0003"
branch_labels = None
depends_on = None


source_file_kind = postgresql.ENUM(
    "quarterly_corporate",
    "daily_corporate",
    "quarterly_corporate_events",
    "daily_corporate_events",
    name="sourcefilekind",
    create_type=False,
)
source_file_status = postgresql.ENUM(
    "pending",
    "processing",
    "completed",
    "failed",
    "noop",
    name="sourcefilestatus",
    create_type=False,
)
artifact_kind = postgresql.ENUM(
    "sunbiz_detail_html",
    "sunbiz_filing_pdf",
    name="artifactkind",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    artifact_kind.create(bind, checkfirst=True)

    op.add_column("source_file", sa.Column("size_bytes", sa.BigInteger(), nullable=True))

    op.create_table(
        "source_ingest_cursor",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("feed_kind", source_file_kind, nullable=False),
        sa.Column("remote_path", sa.String(length=1024), nullable=False),
        sa.Column("file_date", sa.Date(), nullable=True),
        sa.Column("status", source_file_status, nullable=False),
        sa.Column(
            "last_checked_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_downloaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_ingest_cursor")),
        sa.UniqueConstraint(
            "state",
            "feed_kind",
            "remote_path",
            "file_date",
            name="uq_source_ingest_cursor_state_feed_path_date",
        ),
    )
    op.create_index(
        op.f("ix_source_ingest_cursor_state"),
        "source_ingest_cursor",
        ["state"],
        unique=False,
    )
    op.create_index(
        op.f("ix_source_ingest_cursor_feed_kind"),
        "source_ingest_cursor",
        ["feed_kind"],
        unique=False,
    )
    op.create_index(
        op.f("ix_source_ingest_cursor_file_date"),
        "source_ingest_cursor",
        ["file_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_source_ingest_cursor_status"),
        "source_ingest_cursor",
        ["status"],
        unique=False,
    )

    op.create_table(
        "sunbiz_artifact",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("artifact_kind", artifact_kind, nullable=False),
        sa.Column("source_url", sa.String(length=1024), nullable=False),
        sa.Column("bucket_key", sa.String(length=1024), nullable=True),
        sa.Column("content_hash", sa.String(length=128), nullable=True),
        sa.Column("status", source_file_status, nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column(
            "last_checked_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["business_entity.id"],
            name=op.f("fk_sunbiz_artifact_entity_id_business_entity"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_sunbiz_artifact")),
        sa.UniqueConstraint(
            "entity_id",
            "artifact_kind",
            "source_url",
            name="uq_sunbiz_artifact_entity_kind_url",
        ),
    )
    op.create_index(
        op.f("ix_sunbiz_artifact_entity_id"),
        "sunbiz_artifact",
        ["entity_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_sunbiz_artifact_artifact_kind"),
        "sunbiz_artifact",
        ["artifact_kind"],
        unique=False,
    )
    op.create_index(
        op.f("ix_sunbiz_artifact_content_hash"),
        "sunbiz_artifact",
        ["content_hash"],
        unique=False,
    )
    op.create_index(op.f("ix_sunbiz_artifact_status"), "sunbiz_artifact", ["status"], unique=False)
    op.create_index(
        op.f("ix_sunbiz_artifact_next_retry_at"),
        "sunbiz_artifact",
        ["next_retry_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_sunbiz_artifact_next_retry_at"), table_name="sunbiz_artifact")
    op.drop_index(op.f("ix_sunbiz_artifact_status"), table_name="sunbiz_artifact")
    op.drop_index(op.f("ix_sunbiz_artifact_content_hash"), table_name="sunbiz_artifact")
    op.drop_index(op.f("ix_sunbiz_artifact_artifact_kind"), table_name="sunbiz_artifact")
    op.drop_index(op.f("ix_sunbiz_artifact_entity_id"), table_name="sunbiz_artifact")
    op.drop_table("sunbiz_artifact")

    op.drop_index(op.f("ix_source_ingest_cursor_status"), table_name="source_ingest_cursor")
    op.drop_index(op.f("ix_source_ingest_cursor_file_date"), table_name="source_ingest_cursor")
    op.drop_index(op.f("ix_source_ingest_cursor_feed_kind"), table_name="source_ingest_cursor")
    op.drop_index(op.f("ix_source_ingest_cursor_state"), table_name="source_ingest_cursor")
    op.drop_table("source_ingest_cursor")

    op.drop_column("source_file", "size_bytes")

    bind = op.get_bind()
    artifact_kind.drop(bind, checkfirst=True)
