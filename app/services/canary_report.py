from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, or_, select

from app.db.models import (
    ArtifactKind,
    BusinessEntity,
    CompanyEvent,
    CompanyRegistrySnapshot,
    ContactEvidence,
    ContactKind,
    EntityStatus,
    JobRun,
    SourceFile,
    SourceFileStatus,
    SourceRecordRef,
    SunbizArtifact,
)
from app.db.session import run_read_query
from app.services.sunbiz_harvest import is_pdf_mature


@dataclass(slots=True)
class CanaryReport:
    state: str
    hours: int
    window_started_at: datetime
    source_files_created: int = 0
    source_files_completed: int = 0
    source_files_replay: int = 0
    source_files_noop: int = 0
    source_record_refs_created: int = 0
    snapshots_created: int = 0
    events_created: int = 0
    distinct_entities_normalized: int = 0
    active_entities_total: int = 0
    harvested_entities: int = 0
    html_artifacts_completed: int = 0
    pdf_artifacts_completed: int = 0
    pdf_artifacts_pending: int = 0
    html_email_hits: int = 0
    pdf_email_hits: int = 0
    mature_cohort_entities: int = 0
    pdf_hit_entities_mature: int = 0
    pdf_pending_entities_recent: int = 0
    pdf_pending_entities_mature: int = 0
    unresolved_entities: int = 0
    duplicate_entity_keys: int = 0
    email_rows_without_source_url: int = 0
    html_hit_rate: float = 0.0
    pdf_hit_rate_mature_cohort: float = 0.0
    pdf_pending_rate: float = 0.0
    go_ready: bool = False
    blockers: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["html_hit_rate"] = round(self.html_hit_rate, 4)
        payload["pdf_hit_rate_mature_cohort"] = round(self.pdf_hit_rate_mature_cohort, 4)
        payload["pdf_pending_rate"] = round(self.pdf_pending_rate, 4)
        return payload


def run_canary_report(state: str, *, hours: int = 24) -> CanaryReport:
    normalized_state = state.upper()
    window_started_at = datetime.now(UTC) - timedelta(hours=hours)
    
    def _load_report(session) -> CanaryReport:
        report = CanaryReport(
            state=normalized_state,
            hours=hours,
            window_started_at=window_started_at,
        )
        download_runs = session.scalars(
            select(JobRun)
            .where(JobRun.connector_kind == "florida_official_downloader")
            .where(JobRun.state == normalized_state)
            .where(JobRun.started_at >= window_started_at)
        ).all()
        for run in download_runs:
            status = str((run.stats or {}).get("status") or "")
            if status == "replay":
                report.source_files_replay += 1
            elif status == "noop":
                report.source_files_noop += 1

        report.source_files_created = int(
            session.scalar(
                select(func.count(SourceFile.id))
                .where(SourceFile.state == normalized_state)
                .where(SourceFile.downloaded_at >= window_started_at),
            )
            or 0
        )
        report.source_files_completed = int(
            session.scalar(
                select(func.count(SourceFile.id))
                .where(SourceFile.state == normalized_state)
                .where(SourceFile.status == SourceFileStatus.completed)
                .where(SourceFile.downloaded_at >= window_started_at),
            )
            or 0
        )
        report.source_record_refs_created = int(
            session.scalar(
                select(func.count(SourceRecordRef.id))
                .join(SourceFile, SourceFile.id == SourceRecordRef.source_file_id)
                .where(SourceFile.state == normalized_state)
                .where(SourceFile.downloaded_at >= window_started_at),
            )
            or 0
        )
        report.snapshots_created = int(
            session.scalar(
                select(func.count(CompanyRegistrySnapshot.id))
                .join(SourceFile, SourceFile.id == CompanyRegistrySnapshot.source_file_id)
                .where(SourceFile.state == normalized_state)
                .where(SourceFile.downloaded_at >= window_started_at),
            )
            or 0
        )
        report.events_created = int(
            session.scalar(
                select(func.count(CompanyEvent.id))
                .join(SourceFile, SourceFile.id == CompanyEvent.source_file_id)
                .where(SourceFile.state == normalized_state)
                .where(SourceFile.downloaded_at >= window_started_at),
            )
            or 0
        )
        report.distinct_entities_normalized = int(
            session.scalar(
                select(func.count(func.distinct(CompanyRegistrySnapshot.external_filing_id)))
                .join(SourceFile, SourceFile.id == CompanyRegistrySnapshot.source_file_id)
                .where(SourceFile.state == normalized_state)
                .where(SourceFile.downloaded_at >= window_started_at),
            )
            or 0
        )
        report.active_entities_total = int(
            session.scalar(
                select(func.count(BusinessEntity.id))
                .where(BusinessEntity.state == normalized_state)
                .where(BusinessEntity.status == EntityStatus.active),
            )
            or 0
        )

        html_artifacts = session.scalars(
            select(SunbizArtifact)
            .join(BusinessEntity, BusinessEntity.id == SunbizArtifact.entity_id)
            .where(BusinessEntity.state == normalized_state)
            .where(SunbizArtifact.artifact_kind == ArtifactKind.sunbiz_detail_html)
            .where(SunbizArtifact.status == SourceFileStatus.completed)
            .where(SunbizArtifact.last_checked_at >= window_started_at),
        ).all()
        report.html_artifacts_completed = len(html_artifacts)
        harvested_entity_ids = {artifact.entity_id for artifact in html_artifacts}
        report.harvested_entities = len(harvested_entity_ids)

        if harvested_entity_ids:
            pdf_artifacts = session.scalars(
                select(SunbizArtifact)
                .where(SunbizArtifact.entity_id.in_(harvested_entity_ids))
                .where(SunbizArtifact.artifact_kind == ArtifactKind.sunbiz_filing_pdf),
            ).all()
            pdf_artifacts_by_entity: dict[object, list[SunbizArtifact]] = {}
            for artifact in pdf_artifacts:
                pdf_artifacts_by_entity.setdefault(artifact.entity_id, []).append(artifact)

            report.pdf_artifacts_completed = sum(
                artifact.status == SourceFileStatus.completed for artifact in pdf_artifacts
            )
            report.pdf_artifacts_pending = sum(
                artifact.status == SourceFileStatus.pending for artifact in pdf_artifacts
            )

            entities = session.scalars(
                select(BusinessEntity).where(BusinessEntity.id.in_(harvested_entity_ids))
            ).all()
            entities_by_id = {entity.id: entity for entity in entities}

            sunbiz_evidence = session.scalars(
                select(ContactEvidence)
                .where(ContactEvidence.entity_id.in_(harvested_entity_ids))
                .where(ContactEvidence.kind == ContactKind.email)
                .where(ContactEvidence.observed_at >= window_started_at)
                .where(
                    ContactEvidence.notes.in_(
                        ["sunbiz_html_observed", "sunbiz_pdf_observed"]
                    ),
                ),
            ).all()
            html_hits = {
                item.entity_id for item in sunbiz_evidence if item.notes == "sunbiz_html_observed"
            }
            pdf_hits = {
                item.entity_id for item in sunbiz_evidence if item.notes == "sunbiz_pdf_observed"
            }
            report.html_email_hits = len(html_hits)
            report.pdf_email_hits = len(pdf_hits)

            for entity_id in harvested_entity_ids:
                entity = entities_by_id.get(entity_id)
                if entity is None:
                    continue

                artifact_rows = pdf_artifacts_by_entity.get(entity_id, [])
                has_pdf_pending = any(
                    item.status == SourceFileStatus.pending for item in artifact_rows
                )
                has_pdf_hit = entity_id in pdf_hits
                has_html_hit = entity_id in html_hits
                is_mature = is_pdf_mature(entity)

                if is_mature:
                    report.mature_cohort_entities += 1
                    if has_pdf_hit:
                        report.pdf_hit_entities_mature += 1
                    if has_pdf_pending:
                        report.pdf_pending_entities_mature += 1
                elif has_pdf_pending:
                    report.pdf_pending_entities_recent += 1

                if not has_html_hit and not has_pdf_hit and not has_pdf_pending:
                    report.unresolved_entities += 1

        duplicate_subquery = (
            select(BusinessEntity.external_filing_id)
            .where(BusinessEntity.state == normalized_state)
            .group_by(BusinessEntity.external_filing_id)
            .having(func.count(BusinessEntity.id) > 1)
            .subquery()
        )
        report.duplicate_entity_keys = int(
            session.scalar(select(func.count()).select_from(duplicate_subquery)) or 0
        )
        report.email_rows_without_source_url = int(
            session.scalar(
                select(func.count(ContactEvidence.id))
                .join(BusinessEntity, BusinessEntity.id == ContactEvidence.entity_id)
                .where(BusinessEntity.state == normalized_state)
                .where(ContactEvidence.kind == ContactKind.email)
                .where(ContactEvidence.observed_at >= window_started_at)
                .where(
                    or_(
                        ContactEvidence.source_url.is_(None),
                        func.btrim(ContactEvidence.source_url) == "",
                    ),
                ),
            )
            or 0
        )
        return report

    report = run_read_query(_load_report)

    if report.harvested_entities > 0:
        report.html_hit_rate = report.html_email_hits / report.harvested_entities
        report.pdf_pending_rate = (
            (report.pdf_pending_entities_recent + report.pdf_pending_entities_mature)
            / report.harvested_entities
        )
    if report.mature_cohort_entities > 0:
        report.pdf_hit_rate_mature_cohort = (
            report.pdf_hit_entities_mature / report.mature_cohort_entities
        )

    if report.source_files_completed == 0:
        report.blockers.append("no_completed_source_files_in_window")
    if report.duplicate_entity_keys > 0:
        report.blockers.append("duplicate_business_entity_keys")
    if report.email_rows_without_source_url > 0:
        report.blockers.append("email_rows_without_source_url")
    report.go_ready = len(report.blockers) == 0

    return report
