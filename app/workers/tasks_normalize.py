from __future__ import annotations

from uuid import UUID

import dramatiq
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from app.db.models import (
    BusinessEntity,
    CompanyEvent,
    CompanyRegistrySnapshot,
    JobRun,
    RawRegistryRecord,
    SourceFile,
    SourceFileKind,
)
from app.db.session import get_session_factory
from app.services.normalizer import normalize_stage_payload
from app.workers.broker import broker  # noqa: F401


@dramatiq.actor(max_retries=5)
def normalize_entities(
    state: str,
    job_run_id: str | None = None,
    source_file_id: str | None = None,
) -> None:
    imported_entities = run_entity_normalization(
        state,
        job_run_id=job_run_id,
        source_file_id=source_file_id,
    )

    if job_run_id:
        session = get_session_factory()()
        try:
            job_run = session.get(JobRun, UUID(job_run_id))
            if job_run is not None:
                job_run.stats = {**(job_run.stats or {}), "imported_entities": imported_entities}
                session.add(job_run)
                session.commit()
        finally:
            session.close()

    if imported_entities > 0:
        from app.workers.tasks_domains import resolve_domains

        resolve_domains.send(state.upper())


def run_entity_normalization(
    state: str,
    *,
    job_run_id: str | None = None,
    source_file_id: str | None = None,
) -> int:
    if state.upper() == "FL" and source_file_id:
        return _run_florida_entity_normalization(UUID(source_file_id))
    return _run_generic_entity_normalization(state, job_run_id=job_run_id)


def _run_generic_entity_normalization(state: str, *, job_run_id: str | None = None) -> int:
    session = get_session_factory()()
    try:
        stmt = select(RawRegistryRecord).where(RawRegistryRecord.state == state.upper())
        if job_run_id:
            stmt = stmt.where(RawRegistryRecord.job_run_id == UUID(job_run_id))
        stage_rows = session.scalars(stmt.order_by(RawRegistryRecord.ingested_at.desc())).all()

        deduped_upserts: dict[tuple[str, str], dict[str, object]] = {}
        for row in stage_rows:
            try:
                normalized = normalize_stage_payload(state, row.payload)
            except ValueError:
                continue

            key = (normalized.state, normalized.external_filing_id)
            deduped_upserts.setdefault(
                key,
                {
                    "state": normalized.state,
                    "external_filing_id": normalized.external_filing_id,
                    "legal_name": normalized.legal_name,
                    "normalized_name": normalized.normalized_name,
                    "status": normalized.status,
                    "formed_at": normalized.formed_at,
                    "registry_payload": normalized.registry_payload,
                },
            )

        upserts = list(deduped_upserts.values())
        if upserts:
            stmt = insert(BusinessEntity).values(upserts)
            stmt = stmt.on_conflict_do_update(
                index_elements=["state", "external_filing_id"],
                set_={
                    "legal_name": stmt.excluded.legal_name,
                    "normalized_name": stmt.excluded.normalized_name,
                    "status": stmt.excluded.status,
                    "formed_at": stmt.excluded.formed_at,
                    "registry_payload": stmt.excluded.registry_payload,
                    "last_seen_at": func.now(),
                },
            )
            session.execute(stmt)

        session.commit()
        return len(upserts)
    finally:
        session.close()


def _run_florida_entity_normalization(source_file_id: UUID) -> int:
    session = get_session_factory()()
    try:
        source_file = session.get(SourceFile, source_file_id)
        if source_file is None:
            return 0

        if source_file.source_kind in {
            SourceFileKind.quarterly_corporate_events,
            SourceFileKind.daily_corporate_events,
        }:
            _link_florida_events(session, source_file_id)
            session.commit()
            return 0

        snapshot_select = (
            select(
                CompanyRegistrySnapshot.state,
                CompanyRegistrySnapshot.external_filing_id,
                CompanyRegistrySnapshot.legal_name,
                CompanyRegistrySnapshot.normalized_name,
                CompanyRegistrySnapshot.status,
                CompanyRegistrySnapshot.formed_at,
                CompanyRegistrySnapshot.registry_payload,
            )
            .where(CompanyRegistrySnapshot.source_file_id == source_file_id)
            .where(CompanyRegistrySnapshot.is_current.is_(True))
        )

        insert_stmt = insert(BusinessEntity).from_select(
            [
                "state",
                "external_filing_id",
                "legal_name",
                "normalized_name",
                "status",
                "formed_at",
                "registry_payload",
            ],
            snapshot_select,
        )
        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["state", "external_filing_id"],
            set_={
                "legal_name": insert_stmt.excluded.legal_name,
                "normalized_name": insert_stmt.excluded.normalized_name,
                "status": insert_stmt.excluded.status,
                "formed_at": insert_stmt.excluded.formed_at,
                "registry_payload": insert_stmt.excluded.registry_payload,
                "last_seen_at": func.now(),
            },
        )
        session.execute(insert_stmt)

        entity_id_subquery = (
            select(BusinessEntity.id)
            .where(BusinessEntity.state == CompanyRegistrySnapshot.state)
            .where(BusinessEntity.external_filing_id == CompanyRegistrySnapshot.external_filing_id)
            .correlate(CompanyRegistrySnapshot)
            .scalar_subquery()
        )
        session.execute(
            update(CompanyRegistrySnapshot)
            .where(CompanyRegistrySnapshot.source_file_id == source_file_id)
            .values(entity_id=entity_id_subquery),
        )

        _link_florida_events(session, source_file_id)

        imported_entities = session.scalar(
            select(func.count(func.distinct(CompanyRegistrySnapshot.external_filing_id))).where(
                CompanyRegistrySnapshot.source_file_id == source_file_id,
            ),
        )
        session.commit()
        return int(imported_entities or 0)
    finally:
        session.close()


def _link_florida_events(session, source_file_id: UUID) -> None:
    entity_id_subquery = (
        select(BusinessEntity.id)
        .where(BusinessEntity.state == CompanyEvent.state)
        .where(BusinessEntity.external_filing_id == CompanyEvent.external_filing_id)
        .correlate(CompanyEvent)
        .scalar_subquery()
    )
    session.execute(
        update(CompanyEvent)
        .where(CompanyEvent.source_file_id == source_file_id)
        .values(entity_id=entity_id_subquery),
    )
