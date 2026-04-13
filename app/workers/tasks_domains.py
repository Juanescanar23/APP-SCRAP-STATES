from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import dramatiq
from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert

from app.core.config import get_settings
from app.db.models import (
    BusinessEntity,
    DomainStatus,
    EntityStatus,
    OfficialDomain,
    ReviewQueueKind,
)
from app.db.session import get_session_factory
from app.services.domain_resolver import DomainResolutionOutcome, resolve_entity_domains
from app.services.metrics import DomainResolutionMetrics
from app.services.review_queue import ReviewQueueRequest, enqueue_review_item
from app.services.search_provider import (
    NullSearchProvider,
    SearchProvider,
    SearchProviderError,
    get_search_provider,
)
from app.services.site_identity import HttpSiteInspector, SiteInspector
from app.workers.broker import broker  # noqa: F401


@dramatiq.actor(max_retries=5, queue_name="domain_resolve")
def resolve_official_domain(state: str, limit: int = 250) -> None:
    metrics = run_domain_resolution(state, limit=limit)
    if metrics.domain_verified > 0:
        from app.workers.tasks_evidence import collect_public_contact_evidence

        collect_public_contact_evidence.send(state.upper(), limit)


@dramatiq.actor(max_retries=5, queue_name="domain_resolve")
def resolve_domains(state: str) -> None:
    resolve_official_domain(state)


def run_domain_resolution(
    state: str,
    *,
    limit: int = 250,
    dry_run: bool = False,
    search_provider: SearchProvider | None = None,
    site_inspector: SiteInspector | None = None,
) -> DomainResolutionMetrics:
    if (
        search_provider is None
        and get_settings().search_provider.strip().casefold() in {"", "none"}
    ):
        return DomainResolutionMetrics()

    entities = _load_entities(state, limit)
    metrics = DomainResolutionMetrics(imported_entities=len(entities))
    if not entities:
        return metrics

    provider_error: str | None = None
    if search_provider is None:
        try:
            search_provider = get_search_provider()
        except SearchProviderError as exc:
            search_provider = NullSearchProvider()
            provider_error = str(exc)

    inspector = site_inspector or HttpSiteInspector()
    resolution_map = asyncio.run(_resolve_entities(entities, search_provider, inspector))

    if dry_run:
        for entity in entities:
            outcome = resolution_map[str(entity.id)]
            _apply_domain_metrics(metrics, outcome)
            if outcome.review_reason:
                metrics.review_items_queued += 1
        return metrics

    session = get_session_factory()()
    try:
        upserts = []
        for entity in entities:
            outcome = resolution_map[str(entity.id)]
            _apply_domain_metrics(metrics, outcome)
            for candidate in outcome.candidates:
                upserts.append(
                    {
                        "entity_id": entity.id,
                        "domain": candidate.domain,
                        "homepage_url": candidate.homepage_url,
                        "status": candidate.status,
                        "confidence": candidate.confidence,
                        "evidence": candidate.evidence,
                        "last_checked_at": datetime.now(UTC),
                    },
                )

            if outcome.review_reason:
                metrics.review_items_queued += 1
                top_candidate = outcome.candidates[0] if outcome.candidates else None
                enqueue_review_item(
                    session,
                    ReviewQueueRequest(
                        entity_id=entity.id,
                        queue_kind=ReviewQueueKind.domain_resolution,
                        reason=outcome.review_reason,
                        payload={
                            "queries": outcome.queries,
                            "provider_error": provider_error,
                            "top_candidate": top_candidate.domain if top_candidate else None,
                            "top_candidate_confidence": (
                                top_candidate.confidence if top_candidate else None
                            ),
                        },
                    ),
                )

        if upserts:
            stmt = insert(OfficialDomain).values(upserts)
            stmt = stmt.on_conflict_do_update(
                index_elements=["entity_id", "domain"],
                set_={
                    "homepage_url": stmt.excluded.homepage_url,
                    "status": stmt.excluded.status,
                    "confidence": stmt.excluded.confidence,
                    "evidence": stmt.excluded.evidence,
                    "last_checked_at": stmt.excluded.last_checked_at,
                },
            )
            session.execute(stmt)

        session.commit()
    finally:
        session.close()

    return metrics


def _load_entities(state: str, limit: int) -> list[BusinessEntity]:
    session = get_session_factory()()
    try:
        return session.scalars(
            select(BusinessEntity)
            .where(BusinessEntity.state == state.upper())
            .where(BusinessEntity.status == EntityStatus.active)
            .where(
                ~exists(
                    select(OfficialDomain.id)
                    .where(OfficialDomain.entity_id == BusinessEntity.id)
                    .where(OfficialDomain.status == DomainStatus.verified),
                ),
            )
            .order_by(BusinessEntity.last_seen_at.desc(), BusinessEntity.legal_name.asc())
            .limit(limit),
        ).all()
    finally:
        session.close()


def _apply_domain_metrics(
    metrics: DomainResolutionMetrics,
    outcome: DomainResolutionOutcome,
) -> None:
    metrics.domain_candidates_generated += len(outcome.candidates)
    verified_count = sum(
        candidate.status == DomainStatus.verified for candidate in outcome.candidates
    )
    metrics.domain_verified += verified_count
    if verified_count == 0:
        metrics.domain_unresolved += 1


async def _resolve_entities(
    entities: list[BusinessEntity],
    provider: SearchProvider,
    inspector: SiteInspector,
) -> dict[str, DomainResolutionOutcome]:
    outcomes = await asyncio.gather(
        *[resolve_entity_domains(entity, provider, inspector) for entity in entities],
    )
    return {str(entity.id): outcome for entity, outcome in zip(entities, outcomes, strict=True)}
