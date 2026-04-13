from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260413_0001"
down_revision = None
branch_labels = None
depends_on = None


job_status = postgresql.ENUM("pending", "running", "completed", "failed", name="jobstatus", create_type=False)
entity_status = postgresql.ENUM("active", "inactive", "unknown", name="entitystatus", create_type=False)
domain_status = postgresql.ENUM("candidate", "verified", "rejected", name="domainstatus", create_type=False)
contact_kind = postgresql.ENUM("email", "contact_form", "phone", name="contactkind", create_type=False)
review_status = postgresql.ENUM("pending", "approved", "rejected", name="reviewstatus", create_type=False)


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    bind = op.get_bind()
    job_status.create(bind, checkfirst=True)
    entity_status.create(bind, checkfirst=True)
    domain_status.create(bind, checkfirst=True)
    contact_kind.create(bind, checkfirst=True)
    review_status.create(bind, checkfirst=True)

    op.create_table(
        "job_run",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_kind", sa.String(length=128), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("source_uri", sa.String(length=1024), nullable=False),
        sa.Column("source_checksum", sa.String(length=128), nullable=False),
        sa.Column("status", job_status, nullable=False),
        sa.Column("stats", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_job_run")),
    )
    op.create_index(op.f("ix_job_run_connector_kind"), "job_run", ["connector_kind"], unique=False)
    op.create_index(op.f("ix_job_run_source_checksum"), "job_run", ["source_checksum"], unique=False)
    op.create_index(op.f("ix_job_run_state"), "job_run", ["state"], unique=False)
    op.create_index(op.f("ix_job_run_status"), "job_run", ["status"], unique=False)

    op.create_table(
        "raw_registry_record",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("external_filing_id", sa.String(length=128), nullable=True),
        sa.Column("record_checksum", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_run_id"],
            ["job_run.id"],
            name=op.f("fk_raw_registry_record_job_run_id_job_run"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_raw_registry_record")),
        sa.UniqueConstraint("job_run_id", "record_checksum", name="uq_stage_job_checksum"),
    )
    op.create_index(op.f("ix_raw_registry_record_external_filing_id"), "raw_registry_record", ["external_filing_id"], unique=False)
    op.create_index(op.f("ix_raw_registry_record_job_run_id"), "raw_registry_record", ["job_run_id"], unique=False)
    op.create_index(op.f("ix_raw_registry_record_record_checksum"), "raw_registry_record", ["record_checksum"], unique=False)
    op.create_index(op.f("ix_raw_registry_record_state"), "raw_registry_record", ["state"], unique=False)

    op.create_table(
        "business_entity",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("external_filing_id", sa.String(length=128), nullable=False),
        sa.Column("legal_name", sa.String(length=512), nullable=False),
        sa.Column("normalized_name", sa.String(length=512), nullable=False),
        sa.Column("status", entity_status, nullable=False),
        sa.Column("formed_at", sa.Date(), nullable=True),
        sa.Column("registry_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_business_entity")),
        sa.UniqueConstraint("state", "external_filing_id", name="uq_entity_state_filing"),
    )
    op.create_index(op.f("ix_business_entity_external_filing_id"), "business_entity", ["external_filing_id"], unique=False)
    op.create_index(op.f("ix_business_entity_legal_name"), "business_entity", ["legal_name"], unique=False)
    op.create_index("ix_business_entity_normalized_name_trgm", "business_entity", ["normalized_name"], unique=False, postgresql_using="gin", postgresql_ops={"normalized_name": "gin_trgm_ops"})
    op.create_index(op.f("ix_business_entity_state"), "business_entity", ["state"], unique=False)

    op.create_table(
        "official_domain",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("domain", sa.String(length=255), nullable=False),
        sa.Column("homepage_url", sa.String(length=1024), nullable=False),
        sa.Column("status", domain_status, nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["business_entity.id"],
            name=op.f("fk_official_domain_entity_id_business_entity"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_official_domain")),
        sa.UniqueConstraint("entity_id", "domain", name="uq_domain_entity_domain"),
    )
    op.create_index(op.f("ix_official_domain_domain"), "official_domain", ["domain"], unique=False)
    op.create_index(op.f("ix_official_domain_entity_id"), "official_domain", ["entity_id"], unique=False)

    op.create_table(
        "contact_evidence",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("domain_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("kind", contact_kind, nullable=False),
        sa.Column("value", sa.String(length=512), nullable=False),
        sa.Column("source_url", sa.String(length=1024), nullable=False),
        sa.Column("source_hash", sa.String(length=128), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("review_status", review_status, nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["domain_id"],
            ["official_domain.id"],
            name=op.f("fk_contact_evidence_domain_id_official_domain"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["business_entity.id"],
            name=op.f("fk_contact_evidence_entity_id_business_entity"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_contact_evidence")),
        sa.UniqueConstraint(
            "entity_id",
            "domain_id",
            "kind",
            "value",
            "source_hash",
            name="uq_contact_evidence_identity",
        ),
    )
    op.create_index(op.f("ix_contact_evidence_kind"), "contact_evidence", ["kind"], unique=False)
    op.create_index(op.f("ix_contact_evidence_source_hash"), "contact_evidence", ["source_hash"], unique=False)
    op.create_index(op.f("ix_contact_evidence_value"), "contact_evidence", ["value"], unique=False)

    op.create_table(
        "suppression_entry",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_kind", contact_kind, nullable=False),
        sa.Column("value", sa.String(length=512), nullable=False),
        sa.Column("reason", sa.String(length=255), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_suppression_entry")),
        sa.UniqueConstraint("channel_kind", "value", name="uq_suppression_value"),
    )
    op.create_index(op.f("ix_suppression_entry_channel_kind"), "suppression_entry", ["channel_kind"], unique=False)
    op.create_index(op.f("ix_suppression_entry_value"), "suppression_entry", ["value"], unique=False)

    op.create_table(
        "opt_out_log",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_kind", contact_kind, nullable=False),
        sa.Column("value", sa.String(length=512), nullable=False),
        sa.Column("source", sa.String(length=255), nullable=False),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_opt_out_log")),
    )
    op.create_index(op.f("ix_opt_out_log_channel_kind"), "opt_out_log", ["channel_kind"], unique=False)
    op.create_index(op.f("ix_opt_out_log_value"), "opt_out_log", ["value"], unique=False)

    op.create_table(
        "physical_address_profile",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("label", sa.String(length=255), nullable=False),
        sa.Column("address_line1", sa.String(length=255), nullable=False),
        sa.Column("address_line2", sa.String(length=255), nullable=True),
        sa.Column("city", sa.String(length=128), nullable=False),
        sa.Column("state", sa.String(length=64), nullable=False),
        sa.Column("postal_code", sa.String(length=32), nullable=False),
        sa.Column("country", sa.String(length=64), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_physical_address_profile")),
        sa.UniqueConstraint("label", name=op.f("uq_physical_address_profile_label")),
    )

    op.create_table(
        "send_policy",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("policy_name", sa.String(length=255), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("require_review", sa.Boolean(), nullable=False),
        sa.Column("configuration", sa.JSON(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_send_policy")),
        sa.UniqueConstraint("policy_name", name=op.f("uq_send_policy_policy_name")),
    )


def downgrade() -> None:
    op.drop_table("send_policy")
    op.drop_table("physical_address_profile")

    op.drop_index(op.f("ix_opt_out_log_value"), table_name="opt_out_log")
    op.drop_index(op.f("ix_opt_out_log_channel_kind"), table_name="opt_out_log")
    op.drop_table("opt_out_log")

    op.drop_index(op.f("ix_suppression_entry_value"), table_name="suppression_entry")
    op.drop_index(op.f("ix_suppression_entry_channel_kind"), table_name="suppression_entry")
    op.drop_table("suppression_entry")

    op.drop_index(op.f("ix_contact_evidence_value"), table_name="contact_evidence")
    op.drop_index(op.f("ix_contact_evidence_source_hash"), table_name="contact_evidence")
    op.drop_index(op.f("ix_contact_evidence_kind"), table_name="contact_evidence")
    op.drop_table("contact_evidence")

    op.drop_index(op.f("ix_official_domain_entity_id"), table_name="official_domain")
    op.drop_index(op.f("ix_official_domain_domain"), table_name="official_domain")
    op.drop_table("official_domain")

    op.drop_index(op.f("ix_business_entity_state"), table_name="business_entity")
    op.drop_index("ix_business_entity_normalized_name_trgm", table_name="business_entity")
    op.drop_index(op.f("ix_business_entity_legal_name"), table_name="business_entity")
    op.drop_index(op.f("ix_business_entity_external_filing_id"), table_name="business_entity")
    op.drop_table("business_entity")

    op.drop_index(op.f("ix_raw_registry_record_state"), table_name="raw_registry_record")
    op.drop_index(op.f("ix_raw_registry_record_record_checksum"), table_name="raw_registry_record")
    op.drop_index(op.f("ix_raw_registry_record_job_run_id"), table_name="raw_registry_record")
    op.drop_index(op.f("ix_raw_registry_record_external_filing_id"), table_name="raw_registry_record")
    op.drop_table("raw_registry_record")

    op.drop_index(op.f("ix_job_run_status"), table_name="job_run")
    op.drop_index(op.f("ix_job_run_state"), table_name="job_run")
    op.drop_index(op.f("ix_job_run_source_checksum"), table_name="job_run")
    op.drop_index(op.f("ix_job_run_connector_kind"), table_name="job_run")
    op.drop_table("job_run")

    bind = op.get_bind()
    review_status.drop(bind, checkfirst=True)
    contact_kind.drop(bind, checkfirst=True)
    domain_status.drop(bind, checkfirst=True)
    entity_status.drop(bind, checkfirst=True)
    job_status.drop(bind, checkfirst=True)
