from __future__ import annotations

import enum
import uuid
from datetime import date, datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class JobStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class EntityStatus(str, enum.Enum):
    active = "active"
    inactive = "inactive"
    unknown = "unknown"


class DomainStatus(str, enum.Enum):
    candidate = "candidate"
    verified = "verified"
    rejected = "rejected"


class ContactKind(str, enum.Enum):
    email = "email"
    contact_form = "contact_form"
    phone = "phone"


class ReviewStatus(str, enum.Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


class ReviewQueueKind(str, enum.Enum):
    domain_resolution = "domain_resolution"
    public_contact = "public_contact"


class ReviewQueueStatus(str, enum.Enum):
    pending = "pending"
    resolved = "resolved"
    dismissed = "dismissed"


class SourceFileKind(str, enum.Enum):
    quarterly_corporate = "quarterly_corporate"
    daily_corporate = "daily_corporate"
    quarterly_corporate_events = "quarterly_corporate_events"
    daily_corporate_events = "daily_corporate_events"


class SourceFileStatus(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    completed = "completed"
    failed = "failed"
    noop = "noop"


class SourceRecordParseStatus(str, enum.Enum):
    parsed = "parsed"
    failed = "failed"
    skipped = "skipped"


class ArtifactKind(str, enum.Enum):
    sunbiz_detail_html = "sunbiz_detail_html"
    sunbiz_filing_pdf = "sunbiz_filing_pdf"


class JobRun(Base):
    __tablename__ = "job_run"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connector_kind: Mapped[str] = mapped_column(String(128), index=True)
    state: Mapped[str] = mapped_column(String(2), index=True)
    source_uri: Mapped[str] = mapped_column(String(1024))
    source_checksum: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus), default=JobStatus.pending, index=True
    )
    stats: Mapped[dict] = mapped_column(JSONB, default=dict)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class RawRegistryRecord(Base):
    __tablename__ = "raw_registry_record"
    __table_args__ = (
        UniqueConstraint("job_run_id", "record_checksum", name="uq_stage_job_checksum"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("job_run.id", ondelete="CASCADE"),
        index=True,
    )
    state: Mapped[str] = mapped_column(String(2), index=True)
    external_filing_id: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)
    record_checksum: Mapped[str] = mapped_column(String(128), index=True)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    source_metadata: Mapped[dict] = mapped_column(JSONB, default=dict)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class SourceFile(Base):
    __tablename__ = "source_file"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "source_kind",
            "filename",
            "file_date",
            "source_checksum",
            name="uq_source_file_provider_kind_filename_file_date_checksum",
        ),
        Index(
            "ix_source_file_provider_kind_filename_file_date",
            "provider",
            "source_kind",
            "filename",
            "file_date",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_run_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("job_run.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    provider: Mapped[str] = mapped_column(String(64), index=True)
    source_kind: Mapped[SourceFileKind] = mapped_column(Enum(SourceFileKind), index=True)
    state: Mapped[str] = mapped_column(String(2), index=True)
    filename: Mapped[str] = mapped_column(String(255))
    source_uri: Mapped[str] = mapped_column(String(1024))
    bucket_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    source_checksum: Mapped[str] = mapped_column(String(128), index=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    record_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    file_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    is_delta: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[SourceFileStatus] = mapped_column(
        Enum(SourceFileStatus), default=SourceFileStatus.pending, index=True
    )
    total_records: Mapped[int] = mapped_column(Integer, default=0)
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    downloaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SourceIngestCursor(Base):
    __tablename__ = "source_ingest_cursor"
    __table_args__ = (
        UniqueConstraint(
            "state",
            "feed_kind",
            "remote_path",
            "file_date",
            name="uq_source_ingest_cursor_state_feed_path_date",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    state: Mapped[str] = mapped_column(String(2), index=True)
    feed_kind: Mapped[SourceFileKind] = mapped_column(Enum(SourceFileKind), index=True)
    remote_path: Mapped[str] = mapped_column(String(1024))
    file_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    status: Mapped[SourceFileStatus] = mapped_column(
        Enum(SourceFileStatus),
        default=SourceFileStatus.pending,
        index=True,
    )
    last_checked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    last_downloaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class SourceRecordRef(Base):
    __tablename__ = "source_record_ref"
    __table_args__ = (
        UniqueConstraint("source_file_id", "record_no", name="uq_source_record_ref_file_record"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("source_file.id", ondelete="CASCADE"),
        index=True,
    )
    record_no: Mapped[int] = mapped_column(Integer)
    byte_offset: Mapped[int] = mapped_column(BigInteger)
    raw_hash: Mapped[str] = mapped_column(String(128), index=True)
    external_filing_id: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)
    parser_version: Mapped[str] = mapped_column(String(32))
    parse_status: Mapped[SourceRecordParseStatus] = mapped_column(
        Enum(SourceRecordParseStatus), index=True
    )
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BusinessEntity(Base):
    __tablename__ = "business_entity"
    __table_args__ = (
        UniqueConstraint("state", "external_filing_id", name="uq_entity_state_filing"),
        Index(
            "ix_business_entity_normalized_name_trgm",
            "normalized_name",
            postgresql_using="gin",
            postgresql_ops={"normalized_name": "gin_trgm_ops"},
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    state: Mapped[str] = mapped_column(String(2), index=True)
    external_filing_id: Mapped[str] = mapped_column(String(128), index=True)
    legal_name: Mapped[str] = mapped_column(String(512), index=True)
    normalized_name: Mapped[str] = mapped_column(String(512), index=True)
    status: Mapped[EntityStatus] = mapped_column(Enum(EntityStatus), default=EntityStatus.unknown)
    formed_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    registry_payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class CompanyRegistrySnapshot(Base):
    __tablename__ = "company_registry_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "source_record_ref_id", name="uq_company_registry_snapshot_source_record_ref_id"
        ),
        Index("ix_company_registry_snapshot_state_filing", "state", "external_filing_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("business_entity.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("source_file.id", ondelete="CASCADE"),
        index=True,
    )
    source_record_ref_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("source_record_ref.id", ondelete="CASCADE"),
        index=True,
    )
    state: Mapped[str] = mapped_column(String(2), index=True)
    external_filing_id: Mapped[str] = mapped_column(String(128), index=True)
    legal_name: Mapped[str] = mapped_column(String(512))
    normalized_name: Mapped[str] = mapped_column(String(512), index=True)
    status: Mapped[EntityStatus] = mapped_column(Enum(EntityStatus), default=EntityStatus.unknown)
    filing_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    formed_at: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_transaction_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    latest_report_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latest_report_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    fei_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    principal_address_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    mailing_address_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    registered_agent_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    officers_json: Mapped[list[dict]] = mapped_column(JSONB, default=list)
    registry_payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)


class CompanyEvent(Base):
    __tablename__ = "company_event"
    __table_args__ = (
        UniqueConstraint("source_record_ref_id", name="uq_company_event_source_record_ref_id"),
        Index("ix_company_event_state_filing", "state", "external_filing_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("business_entity.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("source_file.id", ondelete="CASCADE"),
        index=True,
    )
    source_record_ref_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("source_record_ref.id", ondelete="CASCADE"),
        index=True,
    )
    state: Mapped[str] = mapped_column(String(2), index=True)
    external_filing_id: Mapped[str] = mapped_column(String(128), index=True)
    legal_name: Mapped[str] = mapped_column(String(512))
    event_code: Mapped[str] = mapped_column(String(32), index=True)
    event_description: Mapped[str] = mapped_column(String(255))
    effective_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    filed_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    payload_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class SunbizArtifact(Base):
    __tablename__ = "sunbiz_artifact"
    __table_args__ = (
        UniqueConstraint(
            "entity_id",
            "artifact_kind",
            "source_url",
            name="uq_sunbiz_artifact_entity_kind_url",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("business_entity.id", ondelete="CASCADE"),
        index=True,
    )
    artifact_kind: Mapped[ArtifactKind] = mapped_column(Enum(ArtifactKind), index=True)
    source_url: Mapped[str] = mapped_column(String(1024))
    bucket_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    content_hash: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)
    status: Mapped[SourceFileStatus] = mapped_column(
        Enum(SourceFileStatus),
        default=SourceFileStatus.pending,
        index=True,
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_checked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class OfficialDomain(Base):
    __tablename__ = "official_domain"
    __table_args__ = (UniqueConstraint("entity_id", "domain", name="uq_domain_entity_domain"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("business_entity.id", ondelete="CASCADE"),
        index=True,
    )
    domain: Mapped[str] = mapped_column(String(255), index=True)
    homepage_url: Mapped[str] = mapped_column(String(1024))
    status: Mapped[DomainStatus] = mapped_column(Enum(DomainStatus), default=DomainStatus.candidate)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    evidence: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ContactEvidence(Base):
    __tablename__ = "contact_evidence"
    __table_args__ = (
        UniqueConstraint(
            "entity_id",
            "domain_id",
            "kind",
            "value",
            "source_hash",
            name="uq_contact_evidence_identity",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("business_entity.id", ondelete="CASCADE"),
        index=True,
    )
    domain_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("official_domain.id", ondelete="SET NULL"),
        nullable=True,
    )
    kind: Mapped[ContactKind] = mapped_column(Enum(ContactKind), index=True)
    value: Mapped[str] = mapped_column(String(512), index=True)
    source_url: Mapped[str] = mapped_column(String(1024))
    source_hash: Mapped[str] = mapped_column(String(128), index=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    review_status: Mapped[ReviewStatus] = mapped_column(
        Enum(ReviewStatus), default=ReviewStatus.pending
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class SuppressionEntry(Base):
    __tablename__ = "suppression_entry"
    __table_args__ = (UniqueConstraint("channel_kind", "value", name="uq_suppression_value"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    channel_kind: Mapped[ContactKind] = mapped_column(Enum(ContactKind), index=True)
    value: Mapped[str] = mapped_column(String(512), index=True)
    reason: Mapped[str] = mapped_column(String(255))
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OptOutLog(Base):
    __tablename__ = "opt_out_log"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    channel_kind: Mapped[ContactKind] = mapped_column(Enum(ContactKind), index=True)
    value: Mapped[str] = mapped_column(String(512), index=True)
    source: Mapped[str] = mapped_column(String(255))
    details: Mapped[dict] = mapped_column(JSONB, default=dict)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class PhysicalAddressProfile(Base):
    __tablename__ = "physical_address_profile"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    label: Mapped[str] = mapped_column(String(255), unique=True)
    address_line1: Mapped[str] = mapped_column(String(255))
    address_line2: Mapped[str | None] = mapped_column(String(255), nullable=True)
    city: Mapped[str] = mapped_column(String(128))
    state: Mapped[str] = mapped_column(String(64))
    postal_code: Mapped[str] = mapped_column(String(32))
    country: Mapped[str] = mapped_column(String(64), default="US")
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SendPolicy(Base):
    __tablename__ = "send_policy"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    policy_name: Mapped[str] = mapped_column(String(255), unique=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    require_review: Mapped[bool] = mapped_column(Boolean, default=True)
    configuration: Mapped[dict] = mapped_column(JSON, default=dict)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class ReviewQueueItem(Base):
    __tablename__ = "review_queue_item"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("business_entity.id", ondelete="CASCADE"),
        index=True,
    )
    domain_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("official_domain.id", ondelete="SET NULL"),
        nullable=True,
    )
    queue_kind: Mapped[ReviewQueueKind] = mapped_column(Enum(ReviewQueueKind), index=True)
    reason: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[ReviewQueueStatus] = mapped_column(
        Enum(ReviewQueueStatus),
        default=ReviewQueueStatus.pending,
        index=True,
    )
    fingerprint: Mapped[str] = mapped_column(String(128), unique=True)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
