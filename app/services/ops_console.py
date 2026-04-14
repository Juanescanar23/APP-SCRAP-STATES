from __future__ import annotations

import csv
import io
import json
import mimetypes
import uuid
from datetime import UTC, datetime

from sqlalchemy import func, select

from app.db.models import (
    BusinessEntity,
    ContactEvidence,
    DomainStatus,
    EntityStatus,
    JobRun,
    OfficialDomain,
    ReviewQueueItem,
    ReviewQueueStatus,
    ReviewStatus,
    SourceFile,
    SunbizArtifact,
)
from app.db.session import get_session_factory
from app.services.canary_report import run_canary_report
from app.services.cohort_report import run_cohort_report
from app.services.entity_cohorts import classify_entity_cohort, prioritize_records_by_entity_cohort
from app.services.object_store import get_object_store
from app.services.sample_inspector import (
    VISIBLE_WEBSITE_EVIDENCE_KINDS,
    inspect_state_samples,
)

EXPORT_KIND_VALUES = ("identities", "contacts")
STORAGE_KIND_VALUES = ("source-file", "sunbiz-artifact")


def build_ops_dashboard_context(state: str) -> dict[str, object]:
    normalized_state = state.upper()
    cohort_report = run_cohort_report(normalized_state)
    canary_report = run_canary_report(normalized_state, hours=24)

    session = get_session_factory()()
    try:
        pending_review_items = int(
            session.scalar(
                select(func.count(ReviewQueueItem.id))
                .join(BusinessEntity, BusinessEntity.id == ReviewQueueItem.entity_id)
                .where(BusinessEntity.state == normalized_state)
                .where(ReviewQueueItem.status == ReviewQueueStatus.pending)
            )
            or 0
        )
        pending_evidence_review = int(
            session.scalar(
                select(func.count(ContactEvidence.id))
                .join(BusinessEntity, BusinessEntity.id == ContactEvidence.entity_id)
                .where(BusinessEntity.state == normalized_state)
                .where(ContactEvidence.review_status == ReviewStatus.pending)
                .where(ContactEvidence.kind.in_(VISIBLE_WEBSITE_EVIDENCE_KINDS))
            )
            or 0
        )
        latest_run = session.scalars(
            select(JobRun)
            .where(JobRun.state == normalized_state)
            .order_by(JobRun.started_at.desc())
            .limit(1)
        ).first()
    finally:
        session.close()

    return {
        "state": normalized_state,
        "cohort_report": cohort_report,
        "canary_report": canary_report,
        "pending_review_items": pending_review_items,
        "pending_evidence_review": pending_evidence_review,
        "latest_run": _job_run_row(latest_run) if latest_run else None,
        "pending_domain_samples": inspect_state_samples(
            normalized_state,
            sample_kind="pending-domain",
            cohort="fresh",
            include_fresh=True,
            limit=5,
        ),
        "verified_domain_samples": inspect_state_samples(
            normalized_state,
            sample_kind="verified-domain",
            cohort="fresh",
            include_fresh=True,
            limit=5,
        ),
        "website_evidence_samples": inspect_state_samples(
            normalized_state,
            sample_kind="website-evidence",
            cohort="fresh",
            include_fresh=True,
            limit=10,
        ),
        "recent_runs": list_job_runs(normalized_state, limit=10),
        "recent_source_files": list_source_files(normalized_state, limit=10),
        "recent_sunbiz_artifacts": list_sunbiz_artifacts(normalized_state, limit=10),
    }


def list_job_runs(state: str, *, limit: int = 25) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        rows = session.scalars(
            select(JobRun)
            .where(JobRun.state == state.upper())
            .order_by(JobRun.started_at.desc())
            .limit(limit)
        ).all()
        return [_job_run_row(row) for row in rows]
    finally:
        session.close()


def list_source_files(state: str, *, limit: int = 25) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        rows = session.scalars(
            select(SourceFile)
            .where(SourceFile.state == state.upper())
            .order_by(SourceFile.downloaded_at.desc())
            .limit(limit)
        ).all()
        return [_source_file_row(row) for row in rows]
    finally:
        session.close()


def list_sunbiz_artifacts(state: str, *, limit: int = 25) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        rows = session.execute(
            select(SunbizArtifact, BusinessEntity)
            .join(BusinessEntity, BusinessEntity.id == SunbizArtifact.entity_id)
            .where(BusinessEntity.state == state.upper())
            .order_by(SunbizArtifact.updated_at.desc())
            .limit(limit)
        ).all()
        return [_sunbiz_artifact_row(artifact, entity) for artifact, entity in rows]
    finally:
        session.close()


def list_review_queue_rows(state: str, *, limit: int = 50) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        rows = session.execute(
            select(ReviewQueueItem, BusinessEntity)
            .join(BusinessEntity, BusinessEntity.id == ReviewQueueItem.entity_id)
            .where(BusinessEntity.state == state.upper())
            .where(ReviewQueueItem.status == ReviewQueueStatus.pending)
            .order_by(ReviewQueueItem.updated_at.desc())
            .limit(limit)
        ).all()
        return [
            {
                "queue_kind": item.queue_kind.value,
                "reason": item.reason,
                "legal_name": entity.legal_name,
                "external_filing_id": entity.external_filing_id,
                "created_at": _isoformat(item.created_at),
                "updated_at": _isoformat(item.updated_at),
                "payload": json.dumps(item.payload or {}, sort_keys=True),
            }
            for item, entity in rows
        ]
    finally:
        session.close()


def list_pending_evidence_rows(state: str, *, limit: int = 50) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        rows = session.execute(
            select(ContactEvidence, BusinessEntity, OfficialDomain)
            .join(BusinessEntity, BusinessEntity.id == ContactEvidence.entity_id)
            .join(OfficialDomain, OfficialDomain.id == ContactEvidence.domain_id, isouter=True)
            .where(BusinessEntity.state == state.upper())
            .where(ContactEvidence.review_status == ReviewStatus.pending)
            .where(ContactEvidence.kind.in_(VISIBLE_WEBSITE_EVIDENCE_KINDS))
            .order_by(ContactEvidence.observed_at.desc())
            .limit(limit)
        ).all()
        return [
            {
                "legal_name": entity.legal_name,
                "external_filing_id": entity.external_filing_id,
                "domain": domain.domain if domain else None,
                "kind": evidence.kind.value,
                "value": evidence.value,
                "source_url": evidence.source_url,
                "confidence": round(evidence.confidence, 4),
                "observed_at": _isoformat(evidence.observed_at),
                "notes": evidence.notes,
            }
            for evidence, entity, domain in rows
        ]
    finally:
        session.close()


def preview_export_rows(
    export_kind: str,
    *,
    state: str,
    cohort: str = "priority",
    include_fresh: bool = True,
    limit: int = 100,
) -> list[dict[str, object]]:
    rows = _build_export_rows(
        export_kind,
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
    )
    return rows[:limit]


def build_export_csv_bytes(
    export_kind: str,
    *,
    state: str,
    cohort: str = "priority",
    include_fresh: bool = True,
) -> tuple[str, bytes]:
    rows = _build_export_rows(
        export_kind,
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
    )
    buffer = io.StringIO()
    fieldnames = list(rows[0].keys()) if rows else _default_export_headers(export_kind)
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    filename = (
        f"{state.lower()}-{export_kind}-{cohort}-{datetime.now(UTC).date().isoformat()}.csv"
    )
    return filename, buffer.getvalue().encode("utf-8")


def get_storage_object(
    storage_kind: str,
    object_id: uuid.UUID,
) -> tuple[str, str, bytes]:
    normalized_kind = storage_kind.strip().casefold()
    if normalized_kind not in STORAGE_KIND_VALUES:
        allowed = ", ".join(STORAGE_KIND_VALUES)
        raise ValueError(
            f"Unsupported storage kind: {storage_kind!r}. Expected one of: {allowed}."
        )

    session = get_session_factory()()
    try:
        if normalized_kind == "source-file":
            source_file = session.get(SourceFile, object_id)
            if source_file is None or not source_file.bucket_key:
                raise LookupError("Source file not found or missing bucket key.")
            filename = source_file.filename
            key = source_file.bucket_key
        else:
            artifact = session.get(SunbizArtifact, object_id)
            if artifact is None or not artifact.bucket_key:
                raise LookupError("Sunbiz artifact not found or missing bucket key.")
            filename = artifact.bucket_key.rsplit("/", 1)[-1]
            key = artifact.bucket_key
    finally:
        session.close()

    payload = get_object_store().get_bytes(key)
    media_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return filename, media_type, payload


def _build_export_rows(
    export_kind: str,
    *,
    state: str,
    cohort: str,
    include_fresh: bool,
) -> list[dict[str, object]]:
    normalized_kind = export_kind.strip().casefold()
    if normalized_kind not in EXPORT_KIND_VALUES:
        allowed = ", ".join(EXPORT_KIND_VALUES)
        raise ValueError(
            f"Unsupported export kind: {export_kind!r}. Expected one of: {allowed}."
        )

    if normalized_kind == "identities":
        return _build_identity_export_rows(state, cohort=cohort, include_fresh=include_fresh)
    return _build_contact_export_rows(state, cohort=cohort, include_fresh=include_fresh)


def _build_identity_export_rows(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        entities = session.scalars(
            select(BusinessEntity)
            .where(BusinessEntity.state == state.upper())
            .where(BusinessEntity.status == EntityStatus.active)
        ).all()
        prioritized = prioritize_records_by_entity_cohort(
            entities,
            entity_getter=lambda entity: entity,
            cohort=cohort,
            include_fresh=include_fresh,
        )
        entity_ids = [entity.id for entity in prioritized]
        domains = session.scalars(
            select(OfficialDomain)
            .where(OfficialDomain.entity_id.in_(entity_ids))
            .where(OfficialDomain.status == DomainStatus.verified)
            .order_by(OfficialDomain.confidence.desc(), OfficialDomain.created_at.desc())
        ).all() if entity_ids else []
        domain_by_entity: dict[uuid.UUID, OfficialDomain] = {}
        for domain in domains:
            domain_by_entity.setdefault(domain.entity_id, domain)

        rows: list[dict[str, object]] = []
        for entity in prioritized:
            domain = domain_by_entity.get(entity.id)
            rows.append(
                {
                    "entity_id": str(entity.id),
                    "state": entity.state,
                    "external_filing_id": entity.external_filing_id,
                    "legal_name": entity.legal_name,
                    "status": entity.status.value,
                    "formed_at": entity.formed_at.isoformat() if entity.formed_at else None,
                    "cohort": classify_entity_cohort(entity).value,
                    "first_seen_at": _isoformat(entity.first_seen_at),
                    "last_seen_at": _isoformat(entity.last_seen_at),
                    "verified_domain_status": domain.status.value if domain else "pending",
                    "verified_domain": domain.domain if domain else None,
                    "verified_homepage_url": domain.homepage_url if domain else None,
                }
            )
        return rows
    finally:
        session.close()


def _build_contact_export_rows(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        joined_rows = session.execute(
            select(ContactEvidence, OfficialDomain, BusinessEntity)
            .join(OfficialDomain, OfficialDomain.id == ContactEvidence.domain_id)
            .join(BusinessEntity, BusinessEntity.id == OfficialDomain.entity_id)
            .where(BusinessEntity.state == state.upper())
            .where(BusinessEntity.status == EntityStatus.active)
            .where(OfficialDomain.status == DomainStatus.verified)
            .where(ContactEvidence.kind.in_(VISIBLE_WEBSITE_EVIDENCE_KINDS))
            .order_by(ContactEvidence.observed_at.desc())
        ).all()
        prioritized = prioritize_records_by_entity_cohort(
            joined_rows,
            entity_getter=lambda row: row[2],
            cohort=cohort,
            include_fresh=include_fresh,
        )
        rows: list[dict[str, object]] = []
        for evidence, domain, entity in prioritized:
            rows.append(
                {
                    "entity_id": str(entity.id),
                    "state": entity.state,
                    "external_filing_id": entity.external_filing_id,
                    "legal_name": entity.legal_name,
                    "cohort": classify_entity_cohort(entity).value,
                    "domain": domain.domain,
                    "homepage_url": domain.homepage_url,
                    "evidence_kind": evidence.kind.value,
                    "email": evidence.value if evidence.kind.value == "email" else None,
                    "contact_form_url": (
                        evidence.value if evidence.kind.value == "contact_form" else None
                    ),
                    "contact_page_url": (
                        evidence.value if evidence.kind.value == "contact_page" else None
                    ),
                    "source_url": evidence.source_url,
                    "confidence": round(evidence.confidence, 4),
                    "observed_at": _isoformat(evidence.observed_at),
                    "notes": evidence.notes,
                }
            )
        return rows
    finally:
        session.close()


def _default_export_headers(export_kind: str) -> list[str]:
    if export_kind == "identities":
        return [
            "entity_id",
            "state",
            "external_filing_id",
            "legal_name",
            "status",
            "formed_at",
            "cohort",
            "first_seen_at",
            "last_seen_at",
            "verified_domain_status",
            "verified_domain",
            "verified_homepage_url",
        ]
    return [
        "entity_id",
        "state",
        "external_filing_id",
        "legal_name",
        "cohort",
        "domain",
        "homepage_url",
        "evidence_kind",
        "email",
        "contact_form_url",
        "contact_page_url",
        "source_url",
        "confidence",
        "observed_at",
        "notes",
    ]


def _job_run_row(row: JobRun) -> dict[str, object]:
    return {
        "id": str(row.id),
        "connector_kind": row.connector_kind,
        "status": row.status.value,
        "source_uri": row.source_uri,
        "source_checksum": row.source_checksum,
        "started_at": _isoformat(row.started_at),
        "finished_at": _isoformat(row.finished_at),
        "stats": json.dumps(row.stats or {}, sort_keys=True),
    }


def _source_file_row(row: SourceFile) -> dict[str, object]:
    return {
        "id": str(row.id),
        "source_kind": row.source_kind.value,
        "filename": row.filename,
        "file_date": row.file_date.isoformat() if row.file_date else None,
        "status": row.status.value,
        "total_records": row.total_records,
        "bucket_key": row.bucket_key,
        "downloaded_at": _isoformat(row.downloaded_at),
        "processed_at": _isoformat(row.processed_at),
    }


def _sunbiz_artifact_row(
    artifact: SunbizArtifact,
    entity: BusinessEntity,
) -> dict[str, object]:
    return {
        "id": str(artifact.id),
        "legal_name": entity.legal_name,
        "external_filing_id": entity.external_filing_id,
        "artifact_kind": artifact.artifact_kind.value,
        "status": artifact.status.value,
        "bucket_key": artifact.bucket_key,
        "source_url": artifact.source_url,
        "last_checked_at": _isoformat(artifact.last_checked_at),
        "next_retry_at": _isoformat(artifact.next_retry_at),
    }


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat()
