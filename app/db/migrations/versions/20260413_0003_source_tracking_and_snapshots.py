from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260413_0003"
down_revision = "20260413_0002"
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
source_record_parse_status = postgresql.ENUM(
    "parsed",
    "failed",
    "skipped",
    name="sourcerecordparsestatus",
    create_type=False,
)
entity_status = postgresql.ENUM(
    "active",
    "inactive",
    "unknown",
    name="entitystatus",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    source_file_kind.create(bind, checkfirst=True)
    source_file_status.create(bind, checkfirst=True)
    source_record_parse_status.create(bind, checkfirst=True)

    op.create_table(
        "source_file",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("source_kind", source_file_kind, nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("source_uri", sa.String(length=1024), nullable=False),
        sa.Column("bucket_key", sa.String(length=1024), nullable=True),
        sa.Column("source_checksum", sa.String(length=128), nullable=False),
        sa.Column("record_length", sa.Integer(), nullable=True),
        sa.Column("file_date", sa.Date(), nullable=True),
        sa.Column("is_delta", sa.Boolean(), nullable=False),
        sa.Column("status", source_file_status, nullable=False),
        sa.Column("total_records", sa.Integer(), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "downloaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["job_run_id"],
            ["job_run.id"],
            name=op.f("fk_source_file_job_run_id_job_run"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_file")),
        sa.UniqueConstraint(
            "provider", "source_kind", "filename", name="uq_source_file_provider_kind_filename"
        ),
    )
    op.create_index(op.f("ix_source_file_job_run_id"), "source_file", ["job_run_id"], unique=False)
    op.create_index(op.f("ix_source_file_provider"), "source_file", ["provider"], unique=False)
    op.create_index(
        op.f("ix_source_file_source_kind"), "source_file", ["source_kind"], unique=False
    )
    op.create_index(op.f("ix_source_file_state"), "source_file", ["state"], unique=False)
    op.create_index(
        op.f("ix_source_file_source_checksum"), "source_file", ["source_checksum"], unique=False
    )
    op.create_index(op.f("ix_source_file_file_date"), "source_file", ["file_date"], unique=False)
    op.create_index(op.f("ix_source_file_status"), "source_file", ["status"], unique=False)

    op.create_table(
        "source_record_ref",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_file_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("record_no", sa.Integer(), nullable=False),
        sa.Column("byte_offset", sa.BigInteger(), nullable=False),
        sa.Column("raw_hash", sa.String(length=128), nullable=False),
        sa.Column("external_filing_id", sa.String(length=128), nullable=True),
        sa.Column("parser_version", sa.String(length=32), nullable=False),
        sa.Column("parse_status", source_record_parse_status, nullable=False),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id"],
            ["source_file.id"],
            name=op.f("fk_source_record_ref_source_file_id_source_file"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_source_record_ref")),
        sa.UniqueConstraint("source_file_id", "record_no", name="uq_source_record_ref_file_record"),
    )
    op.create_index(
        op.f("ix_source_record_ref_source_file_id"),
        "source_record_ref",
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_source_record_ref_raw_hash"), "source_record_ref", ["raw_hash"], unique=False
    )
    op.create_index(
        op.f("ix_source_record_ref_external_filing_id"),
        "source_record_ref",
        ["external_filing_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_source_record_ref_parse_status"),
        "source_record_ref",
        ["parse_status"],
        unique=False,
    )

    op.create_table(
        "company_registry_snapshot",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_file_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_record_ref_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("external_filing_id", sa.String(length=128), nullable=False),
        sa.Column("legal_name", sa.String(length=512), nullable=False),
        sa.Column("normalized_name", sa.String(length=512), nullable=False),
        sa.Column("status", entity_status, nullable=False),
        sa.Column("filing_type", sa.String(length=32), nullable=True),
        sa.Column("formed_at", sa.Date(), nullable=True),
        sa.Column("last_transaction_date", sa.Date(), nullable=True),
        sa.Column("latest_report_year", sa.Integer(), nullable=True),
        sa.Column("latest_report_date", sa.Date(), nullable=True),
        sa.Column("fei_number", sa.String(length=32), nullable=True),
        sa.Column(
            "principal_address_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column("mailing_address_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("registered_agent_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("officers_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("registry_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["business_entity.id"],
            name=op.f("fk_company_registry_snapshot_entity_id_business_entity"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id"],
            ["source_file.id"],
            name=op.f("fk_company_registry_snapshot_source_file_id_source_file"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_record_ref_id"],
            ["source_record_ref.id"],
            name=op.f("fk_company_registry_snapshot_source_record_ref_id_source_record_ref"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_company_registry_snapshot")),
        sa.UniqueConstraint(
            "source_record_ref_id", name="uq_company_registry_snapshot_source_record_ref_id"
        ),
    )
    op.create_index(
        op.f("ix_company_registry_snapshot_entity_id"),
        "company_registry_snapshot",
        ["entity_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_company_registry_snapshot_source_file_id"),
        "company_registry_snapshot",
        ["source_file_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_company_registry_snapshot_source_record_ref_id"),
        "company_registry_snapshot",
        ["source_record_ref_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_company_registry_snapshot_state"),
        "company_registry_snapshot",
        ["state"],
        unique=False,
    )
    op.create_index(
        op.f("ix_company_registry_snapshot_external_filing_id"),
        "company_registry_snapshot",
        ["external_filing_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_company_registry_snapshot_normalized_name"),
        "company_registry_snapshot",
        ["normalized_name"],
        unique=False,
    )
    op.create_index(
        "ix_company_registry_snapshot_state_filing",
        "company_registry_snapshot",
        ["state", "external_filing_id"],
        unique=False,
    )

    op.create_table(
        "company_event",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_file_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_record_ref_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("external_filing_id", sa.String(length=128), nullable=False),
        sa.Column("legal_name", sa.String(length=512), nullable=False),
        sa.Column("event_code", sa.String(length=32), nullable=False),
        sa.Column("event_description", sa.String(length=255), nullable=False),
        sa.Column("effective_date", sa.Date(), nullable=True),
        sa.Column("filed_date", sa.Date(), nullable=True),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["business_entity.id"],
            name=op.f("fk_company_event_entity_id_business_entity"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id"],
            ["source_file.id"],
            name=op.f("fk_company_event_source_file_id_source_file"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_record_ref_id"],
            ["source_record_ref.id"],
            name=op.f("fk_company_event_source_record_ref_id_source_record_ref"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_company_event")),
        sa.UniqueConstraint("source_record_ref_id", name="uq_company_event_source_record_ref_id"),
    )
    op.create_index(
        op.f("ix_company_event_entity_id"), "company_event", ["entity_id"], unique=False
    )
    op.create_index(
        op.f("ix_company_event_source_file_id"), "company_event", ["source_file_id"], unique=False
    )
    op.create_index(
        op.f("ix_company_event_source_record_ref_id"),
        "company_event",
        ["source_record_ref_id"],
        unique=False,
    )
    op.create_index(op.f("ix_company_event_state"), "company_event", ["state"], unique=False)
    op.create_index(
        op.f("ix_company_event_external_filing_id"),
        "company_event",
        ["external_filing_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_company_event_event_code"), "company_event", ["event_code"], unique=False
    )
    op.create_index(
        "ix_company_event_state_filing",
        "company_event",
        ["state", "external_filing_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_company_event_state_filing", table_name="company_event")
    op.drop_index(op.f("ix_company_event_event_code"), table_name="company_event")
    op.drop_index(op.f("ix_company_event_external_filing_id"), table_name="company_event")
    op.drop_index(op.f("ix_company_event_state"), table_name="company_event")
    op.drop_index(op.f("ix_company_event_source_record_ref_id"), table_name="company_event")
    op.drop_index(op.f("ix_company_event_source_file_id"), table_name="company_event")
    op.drop_index(op.f("ix_company_event_entity_id"), table_name="company_event")
    op.drop_table("company_event")

    op.drop_index(
        "ix_company_registry_snapshot_state_filing", table_name="company_registry_snapshot"
    )
    op.drop_index(
        op.f("ix_company_registry_snapshot_normalized_name"), table_name="company_registry_snapshot"
    )
    op.drop_index(
        op.f("ix_company_registry_snapshot_external_filing_id"),
        table_name="company_registry_snapshot",
    )
    op.drop_index(
        op.f("ix_company_registry_snapshot_state"), table_name="company_registry_snapshot"
    )
    op.drop_index(
        op.f("ix_company_registry_snapshot_source_record_ref_id"),
        table_name="company_registry_snapshot",
    )
    op.drop_index(
        op.f("ix_company_registry_snapshot_source_file_id"), table_name="company_registry_snapshot"
    )
    op.drop_index(
        op.f("ix_company_registry_snapshot_entity_id"), table_name="company_registry_snapshot"
    )
    op.drop_table("company_registry_snapshot")

    op.drop_index(op.f("ix_source_record_ref_parse_status"), table_name="source_record_ref")
    op.drop_index(op.f("ix_source_record_ref_external_filing_id"), table_name="source_record_ref")
    op.drop_index(op.f("ix_source_record_ref_raw_hash"), table_name="source_record_ref")
    op.drop_index(op.f("ix_source_record_ref_source_file_id"), table_name="source_record_ref")
    op.drop_table("source_record_ref")

    op.drop_index(op.f("ix_source_file_status"), table_name="source_file")
    op.drop_index(op.f("ix_source_file_file_date"), table_name="source_file")
    op.drop_index(op.f("ix_source_file_source_checksum"), table_name="source_file")
    op.drop_index(op.f("ix_source_file_state"), table_name="source_file")
    op.drop_index(op.f("ix_source_file_source_kind"), table_name="source_file")
    op.drop_index(op.f("ix_source_file_provider"), table_name="source_file")
    op.drop_index(op.f("ix_source_file_job_run_id"), table_name="source_file")
    op.drop_table("source_file")

    bind = op.get_bind()
    source_record_parse_status.drop(bind, checkfirst=True)
    source_file_status.drop(bind, checkfirst=True)
    source_file_kind.drop(bind, checkfirst=True)
