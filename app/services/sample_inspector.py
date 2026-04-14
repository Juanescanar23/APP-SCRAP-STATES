from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import exists, select

from app.db.models import (
    BusinessEntity,
    ContactEvidence,
    ContactKind,
    DomainStatus,
    EntityStatus,
    OfficialDomain,
)
from app.db.session import get_session_factory
from app.services.entity_cohorts import classify_entity_cohort, prioritize_records_by_entity_cohort

SAMPLE_KIND_VALUES = ("pending-domain", "verified-domain", "website-evidence")
VISIBLE_WEBSITE_EVIDENCE_KINDS = (
    ContactKind.email,
    ContactKind.contact_form,
    ContactKind.contact_page,
)


def inspect_state_samples(
    state: str,
    *,
    sample_kind: str,
    cohort: str = "priority",
    include_fresh: bool = True,
    limit: int = 10,
) -> list[dict[str, object]]:
    normalized_kind = sample_kind.strip().casefold()
    if normalized_kind not in SAMPLE_KIND_VALUES:
        allowed = ", ".join(SAMPLE_KIND_VALUES)
        raise ValueError(
            f"Unsupported sample kind: {sample_kind!r}. Expected one of: {allowed}."
        )

    if normalized_kind == "pending-domain":
        return _inspect_pending_domain_samples(
            state,
            cohort=cohort,
            include_fresh=include_fresh,
            limit=limit,
        )
    if normalized_kind == "verified-domain":
        return _inspect_verified_domain_samples(
            state,
            cohort=cohort,
            include_fresh=include_fresh,
            limit=limit,
        )
    return _inspect_website_evidence_samples(
        state,
        cohort=cohort,
        include_fresh=include_fresh,
        limit=limit,
    )


def _inspect_pending_domain_samples(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
    limit: int,
) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        entities = session.scalars(
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
        ).all()
        prioritized = prioritize_records_by_entity_cohort(
            entities,
            entity_getter=lambda entity: entity,
            cohort=cohort,
            include_fresh=include_fresh,
        )
        return [
            _entity_sample_row(entity, sample_kind="pending-domain")
            for entity in prioritized[:limit]
        ]
    finally:
        session.close()


def _inspect_verified_domain_samples(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
    limit: int,
) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        rows = session.execute(
            select(OfficialDomain, BusinessEntity)
            .join(BusinessEntity, BusinessEntity.id == OfficialDomain.entity_id)
            .where(BusinessEntity.state == state.upper())
            .where(BusinessEntity.status == EntityStatus.active)
            .where(OfficialDomain.status == DomainStatus.verified)
        ).all()
        prioritized = prioritize_records_by_entity_cohort(
            rows,
            entity_getter=lambda row: row[1],
            cohort=cohort,
            include_fresh=include_fresh,
        )
        return [
            _verified_domain_sample_row(domain, entity)
            for domain, entity in prioritized[:limit]
        ]
    finally:
        session.close()


def _inspect_website_evidence_samples(
    state: str,
    *,
    cohort: str,
    include_fresh: bool,
    limit: int,
) -> list[dict[str, object]]:
    session = get_session_factory()()
    try:
        rows = session.execute(
            select(ContactEvidence, OfficialDomain, BusinessEntity)
            .join(OfficialDomain, OfficialDomain.id == ContactEvidence.domain_id)
            .join(BusinessEntity, BusinessEntity.id == OfficialDomain.entity_id)
            .where(BusinessEntity.state == state.upper())
            .where(BusinessEntity.status == EntityStatus.active)
            .where(ContactEvidence.kind.in_(VISIBLE_WEBSITE_EVIDENCE_KINDS))
            .order_by(ContactEvidence.observed_at.desc())
        ).all()
        prioritized = prioritize_records_by_entity_cohort(
            rows,
            entity_getter=lambda row: row[2],
            cohort=cohort,
            include_fresh=include_fresh,
        )
        return [
            _website_evidence_sample_row(evidence, domain, entity)
            for evidence, domain, entity in prioritized[:limit]
        ]
    finally:
        session.close()


def _entity_sample_row(entity: BusinessEntity, *, sample_kind: str) -> dict[str, object]:
    return {
        "sample_kind": sample_kind,
        "cohort": classify_entity_cohort(entity).value,
        "legal_name": entity.legal_name,
        "external_filing_id": entity.external_filing_id,
        "first_seen_at": _isoformat(entity.first_seen_at),
        "last_seen_at": _isoformat(entity.last_seen_at),
    }


def _verified_domain_sample_row(
    domain: OfficialDomain,
    entity: BusinessEntity,
) -> dict[str, object]:
    row = _entity_sample_row(entity, sample_kind="verified-domain")
    row.update(
        {
            "domain": domain.domain,
            "homepage_url": domain.homepage_url,
            "status": domain.status.value,
            "confidence": round(domain.confidence, 4),
            "last_checked_at": _isoformat(domain.last_checked_at),
        }
    )
    return row


def _website_evidence_sample_row(
    evidence: ContactEvidence,
    domain: OfficialDomain,
    entity: BusinessEntity,
) -> dict[str, object]:
    row = _entity_sample_row(entity, sample_kind="website-evidence")
    row.update(
        {
            "domain": domain.domain,
            "homepage_url": domain.homepage_url,
            "kind": evidence.kind.value,
            "value": evidence.value,
            "source_url": evidence.source_url,
            "confidence": round(evidence.confidence, 4),
            "observed_at": _isoformat(evidence.observed_at),
            "notes": evidence.notes,
        }
    )
    return row


def _isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat()
