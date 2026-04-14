from __future__ import annotations

import asyncio

import dramatiq
import httpx
from sqlalchemy import exists, select
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
from app.services.contact_evidence import (
    WEBSITE_CONTACT_KINDS,
    CollectionOutcome,
    collect_public_evidence_for_domain,
)
from app.services.metrics import EvidenceCollectionMetrics
from app.services.review_queue import ReviewQueueRequest, enqueue_review_item
from app.services.robots_guard import RobotsGuard
from app.workers.broker import broker  # noqa: F401


@dramatiq.actor(max_retries=5, queue_name="website_contact_collect")
def collect_public_contact_evidence(state: str, limit: int = 100) -> None:
    run_public_contact_collection(state, limit=limit, verified_only=True)


def run_public_contact_collection(
    state: str,
    *,
    limit: int = 100,
    verified_only: bool = True,
    pending_only: bool = True,
    dry_run: bool = False,
) -> EvidenceCollectionMetrics:
    domains = _load_domains(state, limit, verified_only, pending_only)
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


def _load_domains(
    state: str,
    limit: int,
    verified_only: bool,
    pending_only: bool,
) -> list[OfficialDomain]:
    session = get_session_factory()()
    try:
        existing_website_evidence = exists(
            select(ContactEvidence.id)
            .where(ContactEvidence.domain_id == OfficialDomain.id)
            .where(ContactEvidence.kind.in_(WEBSITE_CONTACT_KINDS))
        )
        stmt = (
            select(OfficialDomain)
            .join(BusinessEntity, BusinessEntity.id == OfficialDomain.entity_id)
            .where(BusinessEntity.state == state.upper())
            .where(BusinessEntity.status == EntityStatus.active)
            .order_by(BusinessEntity.last_seen_at.desc(), OfficialDomain.confidence.desc())
            .limit(limit)
        )
        if verified_only:
            stmt = stmt.where(OfficialDomain.status == DomainStatus.verified)
        if pending_only:
            stmt = stmt.where(~existing_website_evidence)
        return session.scalars(stmt).all()
    finally:
        session.close()


def _apply_evidence_metrics(metrics: EvidenceCollectionMetrics, outcome: CollectionOutcome) -> None:
    kinds = {item.kind for item in outcome.evidence}
    if ContactKind.email in kinds:
        metrics.evidence_email_found += 1
    if ContactKind.phone in kinds:
        metrics.evidence_phone_found += 1
    if ContactKind.contact_form in kinds:
        metrics.evidence_contact_form_found += 1
    if ContactKind.contact_page in kinds:
        metrics.evidence_contact_page_found += 1
    if outcome.outcome == "website_contact_observed":
        metrics.website_contact_observed += 1
    if outcome.outcome == "contact_form_only":
        metrics.contact_form_only += 1
    if outcome.outcome == "contact_page_only":
        metrics.contact_page_only += 1
    if outcome.outcome == "robots_blocked":
        metrics.robots_blocked += 1
    if outcome.outcome == "unresolved":
        metrics.unresolved += 1
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
