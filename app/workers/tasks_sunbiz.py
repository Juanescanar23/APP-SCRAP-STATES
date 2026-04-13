from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import dramatiq
import httpx
from sqlalchemy import exists, or_, select
from sqlalchemy.dialects.postgresql import insert

from app.db.models import (
    ArtifactKind,
    BusinessEntity,
    ContactEvidence,
    ContactKind,
    EntityStatus,
    ReviewQueueKind,
    SourceFileStatus,
    SunbizArtifact,
)
from app.db.session import get_session_factory
from app.services.object_store import get_object_store
from app.services.review_queue import ReviewQueueRequest, enqueue_review_item
from app.services.sunbiz_harvest import (
    HarvestedArtifact,
    SunbizHarvestOutcome,
    harvest_sunbiz_entity,
)
from app.workers.broker import broker  # noqa: F401


@dramatiq.actor(max_retries=5, queue_name="fl_sunbiz_harvest")
def fl_sunbiz_harvest(state: str = "FL", limit: int = 100) -> None:
    run_fl_sunbiz_harvest(state, limit=limit)


def run_fl_sunbiz_harvest(state: str = "FL", *, limit: int = 100) -> int:
    entities = _load_entities(state, limit)
    if not entities:
        return 0

    outcome_map = asyncio.run(_harvest_entities(entities))
    session = get_session_factory()()
    try:
        artifact_rows: list[dict[str, object]] = []
        evidence_rows: list[dict[str, object]] = []
        for entity in entities:
            outcome = outcome_map[str(entity.id)]
            artifact_rows.extend(_artifact_rows(entity.id, outcome.artifacts))
            for item in outcome.evidence:
                evidence_rows.append(
                    {
                        "entity_id": entity.id,
                        "domain_id": None,
                        "kind": item.kind,
                        "value": item.value,
                        "source_url": item.source_url,
                        "source_hash": item.source_hash,
                        "confidence": item.confidence,
                        "notes": item.notes,
                    },
                )

            if outcome.review_reason:
                enqueue_review_item(
                    session,
                    ReviewQueueRequest(
                        entity_id=entity.id,
                        queue_kind=ReviewQueueKind.public_contact,
                        reason=outcome.review_reason,
                        payload={
                            "detail_url": outcome.detail_url,
                            "artifact_count": len(outcome.artifacts),
                        },
                    ),
                )

        if artifact_rows:
            stmt = insert(SunbizArtifact).values(artifact_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["entity_id", "artifact_kind", "source_url"],
                set_={
                    "bucket_key": stmt.excluded.bucket_key,
                    "content_hash": stmt.excluded.content_hash,
                    "status": stmt.excluded.status,
                    "attempts": stmt.excluded.attempts,
                    "last_checked_at": stmt.excluded.last_checked_at,
                    "next_retry_at": stmt.excluded.next_retry_at,
                    "metadata_json": stmt.excluded.metadata_json,
                    "updated_at": datetime.now(UTC),
                },
            )
            session.execute(stmt)

        if evidence_rows:
            stmt = insert(ContactEvidence).values(evidence_rows)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["entity_id", "domain_id", "kind", "value", "source_hash"],
            )
            session.execute(stmt)

        session.commit()
        return len(evidence_rows)
    finally:
        session.close()


def _load_entities(state: str, limit: int) -> list[BusinessEntity]:
    session = get_session_factory()()
    try:
        completed_html_exists = exists(
            select(SunbizArtifact.id)
            .where(SunbizArtifact.entity_id == BusinessEntity.id)
            .where(SunbizArtifact.artifact_kind == ArtifactKind.sunbiz_detail_html)
            .where(SunbizArtifact.status == SourceFileStatus.completed)
        )
        retry_due_exists = exists(
            select(SunbizArtifact.id)
            .where(SunbizArtifact.entity_id == BusinessEntity.id)
            .where(SunbizArtifact.next_retry_at.is_not(None))
            .where(SunbizArtifact.next_retry_at <= datetime.now(UTC))
            .where(SunbizArtifact.status == SourceFileStatus.pending)
        )
        existing_sunbiz_email = exists(
            select(ContactEvidence.id)
            .where(ContactEvidence.entity_id == BusinessEntity.id)
            .where(ContactEvidence.kind == ContactKind.email)
            .where(ContactEvidence.source_url.ilike("%search.sunbiz.org%"))
        )

        stmt = (
            select(BusinessEntity)
            .where(BusinessEntity.state == state.upper())
            .where(BusinessEntity.status == EntityStatus.active)
            .where(~existing_sunbiz_email)
            .where(or_(~completed_html_exists, retry_due_exists))
            .order_by(BusinessEntity.last_seen_at.desc(), BusinessEntity.legal_name.asc())
            .limit(limit)
        )
        return session.scalars(stmt).all()
    finally:
        session.close()


async def _harvest_entities(entities: list[BusinessEntity]) -> dict[str, SunbizHarvestOutcome]:
    object_store = get_object_store()
    async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
        outcomes = await asyncio.gather(
            *[harvest_sunbiz_entity(entity, object_store, client) for entity in entities]
        )
    return {str(entity.id): outcome for entity, outcome in zip(entities, outcomes, strict=True)}


def _artifact_rows(entity_id, artifacts: list[HarvestedArtifact]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for artifact in artifacts:
        rows.append(
            {
                "entity_id": entity_id,
                "artifact_kind": artifact.artifact_kind,
                "source_url": artifact.source_url,
                "bucket_key": artifact.bucket_key,
                "content_hash": artifact.content_hash,
                "status": artifact.status,
                "attempts": 1,
                "last_checked_at": datetime.now(UTC),
                "next_retry_at": artifact.next_retry_at,
                "metadata_json": artifact.metadata_json,
            },
        )
    return rows
