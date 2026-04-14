from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, field

from sqlalchemy import select

from app.core.config import get_settings
from app.db.models import (
    BusinessEntity,
    ContactEvidence,
    ContactKind,
    DomainStatus,
    EntityStatus,
    OfficialDomain,
    ReviewQueueItem,
    ReviewQueueKind,
    ReviewQueueStatus,
)
from app.db.session import get_session_factory
from app.services.entity_cohorts import classify_entity_cohort, iter_cohorts


@dataclass(slots=True)
class CohortMetrics:
    active_entities: int = 0
    pending_domain_resolution: int = 0
    verified_entities: int = 0
    website_contact_observed: int = 0
    email_observed: int = 0
    contact_form_only: int = 0
    contact_page_only: int = 0
    pending_website_evidence: int = 0
    unresolved: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass(slots=True)
class CohortReport:
    state: str
    fresh_max_age_days: int
    tempered_max_age_days: int
    cohorts: dict[str, CohortMetrics] = field(default_factory=dict)

    def as_flat_dict(self) -> dict[str, int | str]:
        payload: dict[str, int | str] = {
            "state": self.state,
            "fresh_max_age_days": self.fresh_max_age_days,
            "tempered_max_age_days": self.tempered_max_age_days,
        }
        for cohort_name in [cohort.value for cohort in iter_cohorts()]:
            metrics = self.cohorts[cohort_name]
            for key, value in metrics.as_dict().items():
                payload[f"{cohort_name}_{key}"] = value
        return payload


def run_cohort_report(state: str) -> CohortReport:
    normalized_state = state.upper()
    settings = get_settings()
    report = CohortReport(
        state=normalized_state,
        fresh_max_age_days=settings.fl_fresh_cohort_days,
        tempered_max_age_days=settings.fl_tempered_cohort_days,
        cohorts={cohort.value: CohortMetrics() for cohort in iter_cohorts()},
    )

    session = get_session_factory()()
    try:
        entities = session.scalars(
            select(BusinessEntity)
            .where(BusinessEntity.state == normalized_state)
            .where(BusinessEntity.status == EntityStatus.active)
        ).all()
        if not entities:
            return report

        entity_ids = [entity.id for entity in entities]

        verified_domains = session.scalars(
            select(OfficialDomain)
            .where(OfficialDomain.entity_id.in_(entity_ids))
            .where(OfficialDomain.status == DomainStatus.verified)
        ).all()
        domain_ids = [domain.id for domain in verified_domains]

        evidence_rows = session.scalars(
            select(ContactEvidence)
            .where(ContactEvidence.domain_id.in_(domain_ids))
            .where(
                ContactEvidence.kind.in_(
                    [
                        ContactKind.email,
                        ContactKind.phone,
                        ContactKind.contact_form,
                        ContactKind.contact_page,
                    ]
                )
            )
        ).all() if domain_ids else []

        unresolved_reviews = session.scalars(
            select(ReviewQueueItem)
            .where(ReviewQueueItem.domain_id.in_(domain_ids))
            .where(ReviewQueueItem.queue_kind == ReviewQueueKind.public_contact)
            .where(ReviewQueueItem.reason == "unresolved")
            .where(ReviewQueueItem.status == ReviewQueueStatus.pending)
        ).all() if domain_ids else []

        verified_entity_ids = {domain.entity_id for domain in verified_domains}
        evidence_kinds_by_entity: dict[object, set[ContactKind]] = defaultdict(set)
        unresolved_entity_ids = {item.entity_id for item in unresolved_reviews}

        for evidence in evidence_rows:
            evidence_kinds_by_entity[evidence.entity_id].add(evidence.kind)

        for entity in entities:
            cohort_name = classify_entity_cohort(entity, settings=settings).value
            metrics = report.cohorts[cohort_name]
            metrics.active_entities += 1

            if entity.id not in verified_entity_ids:
                metrics.pending_domain_resolution += 1
                continue

            metrics.verified_entities += 1
            kinds = evidence_kinds_by_entity.get(entity.id, set())

            if ContactKind.email in kinds or ContactKind.phone in kinds:
                metrics.website_contact_observed += 1
                if ContactKind.email in kinds:
                    metrics.email_observed += 1
                continue

            if ContactKind.contact_form in kinds:
                metrics.contact_form_only += 1
                continue

            if ContactKind.contact_page in kinds:
                metrics.contact_page_only += 1
                continue

            if entity.id in unresolved_entity_ids:
                metrics.unresolved += 1
                continue

            metrics.pending_website_evidence += 1
    finally:
        session.close()

    return report
