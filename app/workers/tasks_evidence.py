from __future__ import annotations

import asyncio

import dramatiq
import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.core.config import get_settings
from app.db.models import (
    BusinessEntity,
    ContactEvidence,
    ContactKind,
    DomainStatus,
    EntityStatus,
    OfficialDomain,
    ReviewQueueKind,
)
from app.db.session import get_session_factory
from app.services.contact_evidence import CollectionOutcome, collect_public_evidence_for_domain
from app.services.metrics import EvidenceCollectionMetrics
from app.services.review_queue import ReviewQueueRequest, enqueue_review_item
from app.services.robots_guard import RobotsGuard
from app.workers.broker import broker  # noqa: F401


@dramatiq.actor(max_retries=5)
def collect_public_contact_evidence(state: str, limit: int = 100) -> None:
    run_public_contact_collection(state, limit=limit, verified_only=True)


def run_public_contact_collection(
    state: str,
    *,
    limit: int = 100,
    verified_only: bool = True,
    dry_run: bool = False,
) -> EvidenceCollectionMetrics:
    domains = _load_domains(state, limit, verified_only)
    metrics = EvidenceCollectionMetrics()
    if not domains:
        return metrics

    outcome_map = asyncio.run(_collect_for_domains(domains))

    if dry_run:
        for domain in domains:
            outcome = outcome_map[str(domain.id)]
            _apply_evidence_metrics(metrics, outcome)
            metrics.evidence_rows_persisted += len(outcome.evidence)
        return metrics

    session = get_session_factory()()
    try:
        upserts = []
        for domain in domains:
            outcome = outcome_map[str(domain.id)]
            _apply_evidence_metrics(metrics, outcome)
            for evidence in outcome.evidence:
                upserts.append(
                    {
                        "entity_id": domain.entity_id,
                        "domain_id": domain.id,
                        "kind": evidence.kind,
                        "value": evidence.value,
                        "source_url": evidence.source_url,
                        "source_hash": evidence.source_hash,
                        "confidence": evidence.confidence,
                        "notes": evidence.notes,
                    },
                )

            if outcome.review_reason:
                enqueue_review_item(
                    session,
                    ReviewQueueRequest(
                        entity_id=domain.entity_id,
                        domain_id=domain.id,
                        queue_kind=ReviewQueueKind.public_contact,
                        reason=outcome.review_reason,
                        payload={
                            "outcome": outcome.outcome,
                            "visited_urls": outcome.visited_urls,
                            "blocked_urls": outcome.blocked_urls,
                            "homepage_url": domain.homepage_url,
                        },
                    ),
                )

        metrics.evidence_rows_persisted = len(upserts)
        if upserts:
            stmt = insert(ContactEvidence).values(upserts)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["entity_id", "domain_id", "kind", "value", "source_hash"],
            )
            session.execute(stmt)

        session.commit()
    finally:
        session.close()

    return metrics


def _load_domains(state: str, limit: int, verified_only: bool) -> list[OfficialDomain]:
    session = get_session_factory()()
    try:
        stmt = (
            select(OfficialDomain)
            .join(BusinessEntity, BusinessEntity.id == OfficialDomain.entity_id)
            .where(BusinessEntity.state == state.upper())
            .where(BusinessEntity.status == EntityStatus.active)
            .order_by(OfficialDomain.confidence.desc())
            .limit(limit)
        )
        if verified_only:
            stmt = stmt.where(OfficialDomain.status == DomainStatus.verified)
        return session.scalars(stmt).all()
    finally:
        session.close()


def _apply_evidence_metrics(metrics: EvidenceCollectionMetrics, outcome: CollectionOutcome) -> None:
    if any(item.kind == ContactKind.email for item in outcome.evidence):
        metrics.evidence_email_found += 1
    if any(item.kind == ContactKind.contact_form for item in outcome.evidence):
        metrics.evidence_contact_form_found += 1
    if outcome.outcome == "robots_blocked":
        metrics.robots_blocked += 1
    if outcome.outcome == "no_public_contact_found":
        metrics.no_public_contact_found += 1


async def _collect_for_domains(domains: list[OfficialDomain]) -> dict[str, CollectionOutcome]:
    settings = get_settings()
    guard = RobotsGuard(user_agent=settings.user_agent)
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=settings.http_timeout_seconds,
    ) as client:
        results = await asyncio.gather(
            *[collect_public_evidence_for_domain(domain, guard, client) for domain in domains],
        )

    return {str(domain.id): outcome for domain, outcome in zip(domains, results, strict=True)}
