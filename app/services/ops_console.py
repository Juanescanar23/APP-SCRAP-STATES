from __future__ import annotations

import csv
import io
import json
import mimetypes
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

from sqlalchemy import func, select

from app.connectors.florida.parser import (
    iter_binary_members,
    iter_source_records,
    list_archive_members_from_bytes,
)
from app.db.models import (
    BusinessEntity,
    CompanyRegistrySnapshot,
    ContactEvidence,
    ContactKind,
    DomainStatus,
    EntityStatus,
    JobRun,
    OfficialDomain,
    ReviewQueueItem,
    ReviewQueueStatus,
    ReviewStatus,
    SourceFile,
    SourceFileKind,
    SourceFileStatus,
    SunbizArtifact,
)
from app.db.session import get_session_factory
from app.services.canary_report import run_canary_report
from app.services.cohort_report import run_cohort_report
from app.services.entity_cohorts import classify_entity_cohort, prioritize_records_by_entity_cohort
from app.services.object_store import get_object_store
from app.services.sample_inspector import VISIBLE_WEBSITE_EVIDENCE_KINDS

CANONICAL_EXPORT_KIND_VALUES = (
    "base_oficial",
    "empresas",
    "contactos_primary",
    "contactos_evidence",
)
EXPORT_KIND_ALIASES = {
    "identities": "empresas",
    "contacts": "contactos_primary",
}
EXPORT_KIND_VALUES = CANONICAL_EXPORT_KIND_VALUES + tuple(EXPORT_KIND_ALIASES.keys())
STORAGE_KIND_VALUES = ("source-file", "sunbiz-artifact")
LEGAL_PAGE_HINTS = ("privacy", "terms", "legal")
SOURCE_FILE_PREVIEW_FIELDS = {
    SourceFileKind.daily_corporate: (
        "document_number",
        "company_name",
        "status",
        "filing_type",
        "filing_date",
        "principal_city",
        "principal_state",
        "registered_agent_name",
        "latest_report_year",
        "latest_report_date",
    ),
    SourceFileKind.quarterly_corporate: (
        "document_number",
        "company_name",
        "status",
        "filing_type",
        "filing_date",
        "principal_city",
        "principal_state",
        "registered_agent_name",
        "latest_report_year",
        "latest_report_date",
    ),
    SourceFileKind.daily_corporate_events: (
        "document_number",
        "company_name",
        "event_sequence",
        "event_code",
        "event_description",
        "filed_date",
        "effective_date",
        "principal_city",
        "principal_state",
    ),
    SourceFileKind.quarterly_corporate_events: (
        "document_number",
        "company_name",
        "event_sequence",
        "event_code",
        "event_description",
        "filed_date",
        "effective_date",
        "principal_city",
        "principal_state",
    ),
}
BASE_OFICIAL_HEADERS = [
    "entity_id",
    "state",
    "external_filing_id",
    "legal_name",
    "status",
    "filing_type",
    "formed_at",
    "last_transaction_date",
    "latest_report_year",
    "latest_report_date",
    "fei_number",
    "principal_address_1",
    "principal_address_2",
    "principal_city",
    "principal_state",
    "principal_postal_code",
    "mail_address_1",
    "mail_address_2",
    "mail_city",
    "mail_state",
    "mail_zip",
    "registered_agent_name",
    "registered_agent_address",
    "registered_agent_city",
    "registered_agent_state",
    "registered_agent_zip",
    "officers_count",
    "more_than_six_officers",
    "officers_json",
    "cohort",
    "first_seen_at",
    "last_seen_at",
]
EMPRESAS_HEADERS = [
    *BASE_OFICIAL_HEADERS,
    "domain_status",
    "verified_domain",
    "verified_homepage_url",
    "primary_email",
    "contact_form_url",
    "contact_page_url",
    "source_url",
    "evidence_kind",
    "evidence_scope",
    "confidence",
    "observed_at",
]


def build_ops_dashboard_context(state: str) -> dict[str, object]:
    normalized_state = state.upper()
    cohort_report = run_cohort_report(normalized_state)
    canary_report = run_canary_report(normalized_state, hours=24)
    source_summary = build_official_source_summary(normalized_state)

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
        "source_summary": source_summary,
        "pending_review_items": pending_review_items,
        "pending_evidence_review": pending_evidence_review,
        "latest_run": _job_run_row(latest_run) if latest_run else None,
        "base_oficial_preview": preview_export_rows(
            "base_oficial",
            state=normalized_state,
            cohort="priority",
            include_fresh=True,
            limit=25,
        ),
        "empresas_preview": preview_export_rows(
            "empresas",
            state=normalized_state,
            cohort="priority",
            include_fresh=True,
            limit=25,
        ),
        "contactos_primary_preview": preview_export_rows(
            "contactos_primary",
            state=normalized_state,
            cohort="priority",
            include_fresh=True,
            limit=10,
        ),
        "recent_runs": list_job_runs(normalized_state, limit=10),
        "recent_source_files": list_source_files(normalized_state, limit=12),
    }


def build_official_source_summary(state: str) -> dict[str, object]:
    normalized_state = state.upper()
    session = get_session_factory()()
    try:
        active_entities = int(
            session.scalar(
                select(func.count(BusinessEntity.id))
                .where(BusinessEntity.state == normalized_state)
                .where(BusinessEntity.status == EntityStatus.active)
            )
            or 0
        )
        current_snapshots = int(
            session.scalar(
                select(func.count(CompanyRegistrySnapshot.id))
                .where(CompanyRegistrySnapshot.state == normalized_state)
                .where(CompanyRegistrySnapshot.is_current.is_(True))
            )
            or 0
        )
        source_files = session.scalars(
            select(SourceFile)
            .where(SourceFile.state == normalized_state)
            .order_by(SourceFile.downloaded_at.desc())
        ).all()
    finally:
        session.close()

    completed_by_kind: dict[SourceFileKind, list[SourceFile]] = defaultdict(list)
    for row in source_files:
        if row.status == SourceFileStatus.completed:
            completed_by_kind[row.source_kind].append(row)

    quarterly_corporate_shards = _completed_shards(
        completed_by_kind[SourceFileKind.quarterly_corporate],
    )
    quarterly_event_shards = _completed_shards(
        completed_by_kind[SourceFileKind.quarterly_corporate_events],
    )

    summary_rows = [
        _source_summary_row(
            "Quarterly corporativo",
            completed_by_kind[SourceFileKind.quarterly_corporate],
            shards_total=10,
        ),
        _source_summary_row(
            "Quarterly eventos",
            completed_by_kind[SourceFileKind.quarterly_corporate_events],
            shards_total=10,
        ),
        _source_summary_row(
            "Daily corporativo",
            completed_by_kind[SourceFileKind.daily_corporate],
        ),
        _source_summary_row(
            "Daily eventos",
            completed_by_kind[SourceFileKind.daily_corporate_events],
        ),
    ]

    return {
        "active_entities": active_entities,
        "current_snapshots": current_snapshots,
        "quarterly_corporate_completed_shards": len(quarterly_corporate_shards),
        "quarterly_event_completed_shards": len(quarterly_event_shards),
        "quarterly_corporate_shards": quarterly_corporate_shards,
        "quarterly_event_shards": quarterly_event_shards,
        "latest_daily_corporate_date": _latest_file_date(
            completed_by_kind[SourceFileKind.daily_corporate],
        ),
        "latest_daily_events_date": _latest_file_date(
            completed_by_kind[SourceFileKind.daily_corporate_events],
        ),
        "summary_rows": summary_rows,
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
                "tipo_queue": item.queue_kind.value,
                "razon": item.reason,
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
                "dominio": domain.domain if domain else None,
                "tipo_evidencia": evidence.kind.value,
                "valor": evidence.value,
                "source_url": evidence.source_url,
                "confidence": round(evidence.confidence, 4),
                "scope": classify_evidence_scope(evidence, domain.domain if domain else None),
                "observed_at": _isoformat(evidence.observed_at),
                "notes": evidence.notes,
            }
            for evidence, entity, domain in rows
        ]
    finally:
        session.close()


def describe_export(
    export_kind: str,
    *,
    state: str,
    cohort: str = "priority",
    include_fresh: bool = True,
    limit: int = 100,
) -> dict[str, object]:
    rows = _build_export_rows(
        export_kind,
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
    )
    normalized_kind = _normalize_export_kind(export_kind)
    return {
        "export_kind": normalized_kind,
        "row_count": len(rows),
        "columns": list(rows[0].keys()) if rows else _default_export_headers(normalized_kind),
        "rows": rows[:limit],
    }


def preview_export_rows(
    export_kind: str,
    *,
    state: str,
    cohort: str = "priority",
    include_fresh: bool = True,
    limit: int = 100,
) -> list[dict[str, object]]:
    return describe_export(
        export_kind,
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
        limit=limit,
    )["rows"]


def build_export_csv_bytes(
    export_kind: str,
    *,
    state: str,
    cohort: str = "priority",
    include_fresh: bool = True,
) -> tuple[str, bytes]:
    normalized_kind = _normalize_export_kind(export_kind)
    rows = _build_export_rows(
        normalized_kind,
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
    )
    buffer = io.StringIO()
    fieldnames = list(rows[0].keys()) if rows else _default_export_headers(normalized_kind)
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    filename = (
        f"{state.lower()}-{normalized_kind}-{cohort}-{datetime.now(UTC).date().isoformat()}.csv"
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


def build_source_file_preview(
    object_id: uuid.UUID,
    *,
    parsed_limit: int = 12,
    raw_line_limit: int = 8,
) -> dict[str, object]:
    session = get_session_factory()()
    try:
        source_file = session.get(SourceFile, object_id)
        if source_file is None or not source_file.bucket_key:
            raise LookupError("Source file not found or missing bucket key.")
        metadata = {
            "id": str(source_file.id),
            "state": source_file.state,
            "provider": source_file.provider,
            "source_kind": source_file.source_kind.value,
            "filename": source_file.filename,
            "status": source_file.status.value,
            "file_date": source_file.file_date.isoformat() if source_file.file_date else None,
            "total_records": source_file.total_records,
            "record_length": source_file.record_length,
            "size_bytes": source_file.size_bytes,
            "quarterly_shard": _coerce_quarterly_shard(source_file.metadata_json),
            "is_delta": source_file.is_delta,
            "bucket_key": source_file.bucket_key,
            "source_uri": source_file.source_uri,
            "source_checksum": source_file.source_checksum,
            "downloaded_at": _isoformat(source_file.downloaded_at),
            "processed_at": _isoformat(source_file.processed_at),
        }
    finally:
        session.close()

    payload = get_object_store().get_bytes(str(metadata["bucket_key"]))
    archive_members = _resolve_archive_members(
        filename=str(metadata["filename"]),
        payload=payload,
    )
    if archive_members:
        metadata["archive_members"] = ", ".join(archive_members)

    quarterly_shard = metadata.get("quarterly_shard")
    with TemporaryDirectory() as temp_dir:
        source_path = Path(temp_dir) / str(metadata["filename"])
        source_path.write_bytes(payload)
        parsed_rows = _preview_parsed_rows(
            source_path,
            source_kind=SourceFileKind(str(metadata["source_kind"])),
            quarterly_shard=quarterly_shard if isinstance(quarterly_shard, int) else None,
            limit=parsed_limit,
        )
        raw_rows = _preview_raw_rows(
            source_path,
            quarterly_shard=quarterly_shard if isinstance(quarterly_shard, int) else None,
            limit=raw_line_limit,
        )

    return {
        "metadata": metadata,
        "parsed_rows": parsed_rows,
        "raw_rows": raw_rows,
    }


def _build_export_rows(
    export_kind: str,
    *,
    state: str,
    cohort: str,
    include_fresh: bool,
) -> list[dict[str, object]]:
    normalized_kind = _normalize_export_kind(export_kind)
    if normalized_kind == "base_oficial":
        return _build_base_oficial_export_rows(state, cohort=cohort, include_fresh=include_fresh)
    if normalized_kind == "empresas":
        return _build_empresas_export_rows(state, cohort=cohort, include_fresh=include_fresh)
    if normalized_kind == "contactos_primary":
        return _build_contactos_primary_export_rows(
            state,
            cohort=cohort,
            include_fresh=include_fresh,
        )
    return _build_contactos_evidence_export_rows(state, cohort=cohort, include_fresh=include_fresh)


def _resolve_archive_members(*, filename: str, payload: bytes) -> list[str]:
    if not filename.casefold().endswith(".zip"):
        return []
    return list_archive_members_from_bytes(payload)


def _coerce_quarterly_shard(metadata_json: dict | None) -> int | None:
    if not metadata_json:
        return None
    raw_value = metadata_json.get("quarterly_shard")
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _preview_parsed_rows(
    source_path: Path,
    *,
    source_kind: SourceFileKind,
    quarterly_shard: int | None,
    limit: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    selected_fields = SOURCE_FILE_PREVIEW_FIELDS[source_kind]
    for record in islice(
        iter_source_records(source_path, quarterly_shard=quarterly_shard),
        limit,
    ):
        row: dict[str, object] = {
            "record_no": record.record_no,
            "byte_offset": record.byte_offset,
            "parse_status": record.parse_status.value,
            "error_code": record.error_code,
        }
        if source_kind in {
            SourceFileKind.daily_corporate,
            SourceFileKind.quarterly_corporate,
        }:
            row["officers_count"] = len(record.payload.get("officers") or [])
        for field_name in selected_fields:
            row[field_name] = record.payload.get(field_name)
        rows.append(row)
    return rows


def _preview_raw_rows(
    source_path: Path,
    *,
    quarterly_shard: int | None,
    limit: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for member_name, handle in iter_binary_members(source_path, quarterly_shard=quarterly_shard):
        member_line_no = 0
        for raw_line in handle:
            line = raw_line.rstrip(b"\r\n")
            if not line:
                continue
            member_line_no += 1
            rows.append(
                {
                    "member_name": member_name,
                    "line_no": member_line_no,
                    "content": line.decode("ascii", errors="ignore").replace("\x00", ""),
                }
            )
            if len(rows) >= limit:
                return rows
    return rows


def _build_base_oficial_export_rows(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
) -> list[dict[str, object]]:
    entities = _load_prioritized_entities(state, cohort=cohort, include_fresh=include_fresh)
    return [_official_base_row(entity) for entity in entities]


def _build_empresas_export_rows(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
) -> list[dict[str, object]]:
    entities = _load_prioritized_entities(state, cohort=cohort, include_fresh=include_fresh)
    return _build_company_rows(entities)


def _build_contactos_primary_export_rows(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
) -> list[dict[str, object]]:
    entities = _load_prioritized_entities(state, cohort=cohort, include_fresh=include_fresh)
    bundles = _load_enrichment_bundle(entities)
    return [_primary_contact_row(entity, bundle) for entity, bundle in bundles]


def _build_contactos_evidence_export_rows(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
) -> list[dict[str, object]]:
    entities = _load_prioritized_entities(state, cohort=cohort, include_fresh=include_fresh)
    if not entities:
        return []

    entity_ids = [entity.id for entity in entities]
    entity_by_id = {entity.id: entity for entity in entities}

    session = get_session_factory()()
    try:
        rows = session.execute(
            select(ContactEvidence, OfficialDomain)
            .join(OfficialDomain, OfficialDomain.id == ContactEvidence.domain_id)
            .where(ContactEvidence.entity_id.in_(entity_ids))
            .where(ContactEvidence.kind.in_(VISIBLE_WEBSITE_EVIDENCE_KINDS))
            .order_by(ContactEvidence.observed_at.desc())
        ).all()
    finally:
        session.close()

    export_rows: list[dict[str, object]] = []
    for evidence, domain in rows:
        entity = entity_by_id.get(evidence.entity_id)
        if entity is None:
            continue
        scope = classify_evidence_scope(evidence, domain.domain)
        export_rows.append(
            {
                "entity_id": str(entity.id),
                "state": entity.state,
                "external_filing_id": entity.external_filing_id,
                "legal_name": entity.legal_name,
                "cohort": classify_entity_cohort(entity).value,
                "verified_domain": domain.domain,
                "homepage_url": domain.homepage_url,
                "evidence_kind": evidence.kind.value,
                "evidence_scope": scope,
                "email": evidence.value if evidence.kind == ContactKind.email else None,
                "contact_form_url": (
                    evidence.value if evidence.kind == ContactKind.contact_form else None
                ),
                "contact_page_url": (
                    evidence.value if evidence.kind == ContactKind.contact_page else None
                ),
                "source_url": evidence.source_url,
                "confidence": round(evidence.confidence, 4),
                "observed_at": _isoformat(evidence.observed_at),
                "notes": evidence.notes,
            }
        )
    return export_rows


def _load_prioritized_entities(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
) -> list[BusinessEntity]:
    session = get_session_factory()()
    try:
        entities = session.scalars(
            select(BusinessEntity)
            .where(BusinessEntity.state == state.upper())
            .where(BusinessEntity.status == EntityStatus.active)
            .order_by(BusinessEntity.legal_name.asc())
        ).all()
    finally:
        session.close()

    return prioritize_records_by_entity_cohort(
        entities,
        entity_getter=lambda entity: entity,
        cohort=cohort,
        include_fresh=include_fresh,
    )


def _build_company_rows(entities: list[BusinessEntity]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for entity, bundle in _load_enrichment_bundle(entities):
        row = _official_base_row(entity)
        row.update(
            {
                "domain_status": bundle["domain_status"],
                "verified_domain": bundle["verified_domain"],
                "verified_homepage_url": bundle["verified_homepage_url"],
                "primary_email": bundle["primary_email"],
                "contact_form_url": bundle["contact_form_url"],
                "contact_page_url": bundle["contact_page_url"],
                "source_url": bundle["source_url"],
                "evidence_kind": bundle["evidence_kind"],
                "evidence_scope": bundle["evidence_scope"],
                "confidence": bundle["confidence"],
                "observed_at": bundle["observed_at"],
            }
        )
        rows.append(row)
    return rows


def _primary_contact_row(entity: BusinessEntity, bundle: dict[str, object]) -> dict[str, object]:
    return {
        "entity_id": str(entity.id),
        "state": entity.state,
        "external_filing_id": entity.external_filing_id,
        "legal_name": entity.legal_name,
        "cohort": classify_entity_cohort(entity).value,
        "domain_status": bundle["domain_status"],
        "verified_domain": bundle["verified_domain"],
        "verified_homepage_url": bundle["verified_homepage_url"],
        "primary_email": bundle["primary_email"],
        "contact_form_url": bundle["contact_form_url"],
        "contact_page_url": bundle["contact_page_url"],
        "source_url": bundle["source_url"],
        "evidence_kind": bundle["evidence_kind"],
        "evidence_scope": bundle["evidence_scope"],
        "confidence": bundle["confidence"],
        "observed_at": bundle["observed_at"],
    }


def _load_enrichment_bundle(
    entities: list[BusinessEntity],
) -> list[tuple[BusinessEntity, dict[str, object]]]:
    if not entities:
        return []

    entity_ids = [entity.id for entity in entities]
    session = get_session_factory()()
    try:
        domains = session.scalars(
            select(OfficialDomain)
            .where(OfficialDomain.entity_id.in_(entity_ids))
            .order_by(OfficialDomain.created_at.desc())
        ).all()
        evidence_rows = session.execute(
            select(ContactEvidence, OfficialDomain)
            .join(OfficialDomain, OfficialDomain.id == ContactEvidence.domain_id)
            .where(ContactEvidence.entity_id.in_(entity_ids))
            .where(ContactEvidence.kind.in_(VISIBLE_WEBSITE_EVIDENCE_KINDS))
            .order_by(ContactEvidence.observed_at.desc())
        ).all()
    finally:
        session.close()

    verified_domain_by_entity: dict[uuid.UUID, OfficialDomain] = {}
    best_domain_by_entity: dict[uuid.UUID, OfficialDomain] = {}
    for domain in domains:
        existing_best = best_domain_by_entity.get(domain.entity_id)
        if existing_best is None or _domain_sort_key(domain) > _domain_sort_key(existing_best):
            best_domain_by_entity[domain.entity_id] = domain
        if domain.status != DomainStatus.verified:
            continue
        existing_verified = verified_domain_by_entity.get(domain.entity_id)
        if existing_verified is None or _domain_sort_key(domain) > _domain_sort_key(
            existing_verified,
        ):
            verified_domain_by_entity[domain.entity_id] = domain

    evidence_by_entity: dict[
        uuid.UUID,
        list[tuple[ContactEvidence, OfficialDomain]],
    ] = defaultdict(list)
    for evidence, domain in evidence_rows:
        evidence_by_entity[evidence.entity_id].append((evidence, domain))

    bundle_rows: list[tuple[BusinessEntity, dict[str, object]]] = []
    for entity in entities:
        best_domain = best_domain_by_entity.get(entity.id)
        verified_domain = verified_domain_by_entity.get(entity.id)
        selected = select_primary_contact(
            evidence_by_entity.get(entity.id, []),
            verified_domain=verified_domain.domain if verified_domain else None,
        )
        bundle_rows.append(
            (
                entity,
                {
                    "domain_status": (
                        verified_domain.status.value
                        if verified_domain
                        else best_domain.status.value if best_domain else "pending"
                    ),
                    "verified_domain": verified_domain.domain if verified_domain else None,
                    "verified_homepage_url": (
                        verified_domain.homepage_url if verified_domain else None
                    ),
                    "primary_email": selected["primary_email"],
                    "contact_form_url": selected["contact_form_url"],
                    "contact_page_url": selected["contact_page_url"],
                    "source_url": selected["source_url"],
                    "evidence_kind": selected["evidence_kind"],
                    "evidence_scope": selected["evidence_scope"],
                    "confidence": selected["confidence"],
                    "observed_at": selected["observed_at"],
                },
            )
        )

    return bundle_rows


def select_primary_contact(
    evidence_rows: list[tuple[ContactEvidence, OfficialDomain]],
    *,
    verified_domain: str | None,
) -> dict[str, object]:
    best_email: tuple[ContactEvidence, str] | None = None
    best_form: tuple[ContactEvidence, str] | None = None
    best_page: tuple[ContactEvidence, str] | None = None

    for evidence, domain in evidence_rows:
        scope = classify_evidence_scope(evidence, verified_domain or domain.domain)
        if evidence.kind == ContactKind.email:
            if scope != "verified_domain_email":
                continue
            if best_email is None or _evidence_sort_key(evidence) > _evidence_sort_key(
                best_email[0],
            ):
                best_email = (evidence, scope)
            continue
        if evidence.kind == ContactKind.contact_form:
            if best_form is None or _evidence_sort_key(evidence) > _evidence_sort_key(best_form[0]):
                best_form = (evidence, scope)
            continue
        if evidence.kind == ContactKind.contact_page:
            if best_page is None or _evidence_sort_key(evidence) > _evidence_sort_key(best_page[0]):
                best_page = (evidence, scope)

    best_primary = best_email or best_form or best_page
    return {
        "primary_email": best_email[0].value if best_email else None,
        "contact_form_url": best_form[0].value if best_form else None,
        "contact_page_url": best_page[0].value if best_page else None,
        "source_url": best_primary[0].source_url if best_primary else None,
        "evidence_kind": best_primary[0].kind.value if best_primary else None,
        "evidence_scope": best_primary[1] if best_primary else None,
        "confidence": round(best_primary[0].confidence, 4) if best_primary else None,
        "observed_at": _isoformat(best_primary[0].observed_at) if best_primary else None,
    }


def classify_evidence_scope(
    evidence: ContactEvidence,
    verified_domain: str | None,
) -> str:
    if evidence.kind == ContactKind.email:
        email_domain = _email_domain(evidence.value)
        if verified_domain and email_domain and _domains_match(email_domain, verified_domain):
            return "verified_domain_email"
        if _is_legal_source(evidence.source_url):
            return "third_party_observed"
        return "offdomain_observed"
    if evidence.kind == ContactKind.contact_form:
        return "verified_website_form"
    if evidence.kind == ContactKind.contact_page:
        return "verified_website_page"
    return "secondary_observed"


def _normalize_export_kind(export_kind: str) -> str:
    normalized = export_kind.strip().casefold()
    normalized = EXPORT_KIND_ALIASES.get(normalized, normalized)
    if normalized not in CANONICAL_EXPORT_KIND_VALUES:
        allowed = ", ".join(EXPORT_KIND_VALUES)
        raise ValueError(
            f"Unsupported export kind: {export_kind!r}. Expected one of: {allowed}."
        )
    return normalized


def _official_base_row(entity: BusinessEntity) -> dict[str, object]:
    payload = entity.registry_payload or {}
    officers = payload.get("officers") or []
    return {
        "entity_id": str(entity.id),
        "state": entity.state,
        "external_filing_id": entity.external_filing_id,
        "legal_name": entity.legal_name,
        "status": entity.status.value,
        "filing_type": payload.get("filing_type"),
        "formed_at": (
            payload.get("formed_at")
            or (entity.formed_at.isoformat() if entity.formed_at else None)
        ),
        "last_transaction_date": payload.get("last_transaction_date"),
        "latest_report_year": payload.get("latest_report_year"),
        "latest_report_date": payload.get("latest_report_date"),
        "fei_number": payload.get("fei_number"),
        "principal_address_1": payload.get("address_line1"),
        "principal_address_2": payload.get("address_line2"),
        "principal_city": payload.get("city"),
        "principal_state": payload.get("state_name"),
        "principal_postal_code": payload.get("postal_code"),
        "mail_address_1": payload.get("mail_address_1"),
        "mail_address_2": payload.get("mail_address_2"),
        "mail_city": payload.get("mail_city"),
        "mail_state": payload.get("mail_state"),
        "mail_zip": payload.get("mail_zip"),
        "registered_agent_name": payload.get("registered_agent_name"),
        "registered_agent_address": payload.get("registered_agent_address"),
        "registered_agent_city": payload.get("registered_agent_city"),
        "registered_agent_state": payload.get("registered_agent_state"),
        "registered_agent_zip": payload.get("registered_agent_zip"),
        "officers_count": len(officers),
        "more_than_six_officers": bool(payload.get("more_than_six_officers")),
        "officers_json": json.dumps(officers, sort_keys=True),
        "cohort": classify_entity_cohort(entity).value,
        "first_seen_at": _isoformat(entity.first_seen_at),
        "last_seen_at": _isoformat(entity.last_seen_at),
    }


def _default_export_headers(export_kind: str) -> list[str]:
    normalized_kind = _normalize_export_kind(export_kind)
    if normalized_kind == "base_oficial":
        return BASE_OFICIAL_HEADERS
    if normalized_kind == "empresas":
        return EMPRESAS_HEADERS
    if normalized_kind == "contactos_primary":
        return [
            "entity_id",
            "state",
            "external_filing_id",
            "legal_name",
            "cohort",
            "domain_status",
            "verified_domain",
            "verified_homepage_url",
            "primary_email",
            "contact_form_url",
            "contact_page_url",
            "source_url",
            "evidence_kind",
            "evidence_scope",
            "confidence",
            "observed_at",
        ]
    return [
        "entity_id",
        "state",
        "external_filing_id",
        "legal_name",
        "cohort",
        "verified_domain",
        "homepage_url",
        "evidence_kind",
        "evidence_scope",
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
        "quarterly_shard": row.metadata_json.get("quarterly_shard") if row.metadata_json else None,
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


def _source_summary_row(
    label: str,
    rows: list[SourceFile],
    *,
    shards_total: int | None = None,
) -> dict[str, object]:
    shards = _completed_shards(rows)
    return {
        "fuente": label,
        "archivos_completados": len(rows),
        "shards_completados": f"{len(shards)}/{shards_total}" if shards_total else "-",
        "lista_shards": ", ".join(str(value) for value in shards) if shards else None,
        "ultima_file_date": _latest_file_date(rows),
        "ultimo_downloaded_at": _latest_downloaded_at(rows),
        "registros_totales": sum(row.total_records for row in rows),
    }


def _completed_shards(rows: list[SourceFile]) -> list[int]:
    shards = sorted(
        {
            int(row.metadata_json.get("quarterly_shard"))
            for row in rows
            if row.metadata_json and row.metadata_json.get("quarterly_shard") is not None
        }
    )
    return shards


def _latest_file_date(rows: list[SourceFile]) -> str | None:
    dates = [row.file_date for row in rows if row.file_date is not None]
    return max(dates).isoformat() if dates else None


def _latest_downloaded_at(rows: list[SourceFile]) -> str | None:
    dates = [row.downloaded_at for row in rows if row.downloaded_at is not None]
    if not dates:
        return None
    return _isoformat(max(dates))


def _domain_sort_key(domain: OfficialDomain) -> tuple[int, float, float]:
    status_rank = {
        DomainStatus.verified: 3,
        DomainStatus.candidate: 2,
        DomainStatus.rejected: 1,
    }.get(domain.status, 0)
    checked_at = _timestamp_or_zero(domain.last_checked_at or domain.created_at)
    return (status_rank, float(domain.confidence or 0.0), checked_at)


def _evidence_sort_key(evidence: ContactEvidence) -> tuple[float, float]:
    return (float(evidence.confidence or 0.0), _timestamp_or_zero(evidence.observed_at))


def _timestamp_or_zero(value: datetime | None) -> float:
    if value is None:
        return 0.0
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.timestamp()


def _email_domain(value: str) -> str | None:
    if "@" not in value:
        return None
    return value.rsplit("@", 1)[1].strip().casefold() or None


def _domains_match(candidate: str, verified_domain: str) -> bool:
    normalized_candidate = candidate.casefold().strip(".")
    normalized_verified = verified_domain.casefold().strip(".")
    return normalized_candidate == normalized_verified or normalized_candidate.endswith(
        "." + normalized_verified,
    )


def _is_legal_source(source_url: str) -> bool:
    path = (urlparse(source_url).path or "/").casefold()
    return any(token in path for token in LEGAL_PAGE_HINTS)


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat()
