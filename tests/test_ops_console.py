from __future__ import annotations

import uuid
from datetime import UTC, datetime

from app.db.models import ContactEvidence, ContactKind, DomainStatus, OfficialDomain, ReviewStatus
from app.services.ops_console import classify_evidence_scope, select_primary_contact


def make_domain(domain: str) -> OfficialDomain:
    return OfficialDomain(
        id=uuid.uuid4(),
        entity_id=uuid.uuid4(),
        domain=domain,
        homepage_url=f"https://{domain}/",
        status=DomainStatus.verified,
        confidence=0.9,
        evidence={},
        created_at=datetime(2026, 4, 14, tzinfo=UTC),
        last_checked_at=datetime(2026, 4, 14, tzinfo=UTC),
    )


def make_evidence(
    *,
    kind: ContactKind,
    value: str,
    source_url: str,
    confidence: float,
) -> ContactEvidence:
    return ContactEvidence(
        id=uuid.uuid4(),
        entity_id=uuid.uuid4(),
        domain_id=uuid.uuid4(),
        kind=kind,
        value=value,
        source_url=source_url,
        source_hash="hash",
        confidence=confidence,
        review_status=ReviewStatus.pending,
        notes=None,
        observed_at=datetime(2026, 4, 14, tzinfo=UTC),
    )


def test_select_primary_contact_prefers_same_domain_email() -> None:
    domain = make_domain("knewhealth.com")
    same_domain_email = make_evidence(
        kind=ContactKind.email,
        value="hello@knewhealth.com",
        source_url="https://www.knewhealth.com/contact",
        confidence=0.95,
    )
    contact_form = make_evidence(
        kind=ContactKind.contact_form,
        value="https://www.knewhealth.com/contact",
        source_url="https://www.knewhealth.com/contact",
        confidence=0.80,
    )

    selected = select_primary_contact(
        [(contact_form, domain), (same_domain_email, domain)],
        verified_domain="knewhealth.com",
    )

    assert selected["primary_email"] == "hello@knewhealth.com"
    assert selected["contact_form_url"] == "https://www.knewhealth.com/contact"
    assert selected["evidence_kind"] == "email"
    assert selected["evidence_scope"] == "verified_domain_email"


def test_select_primary_contact_excludes_third_party_legal_email() -> None:
    domain = make_domain("knewhealth.com")
    third_party_email = make_evidence(
        kind=ContactKind.email,
        value="jkeenan@bernsteinshur.com",
        source_url="https://www.knewhealth.com/privacy",
        confidence=0.95,
    )

    selected = select_primary_contact(
        [(third_party_email, domain)],
        verified_domain="knewhealth.com",
    )

    assert classify_evidence_scope(third_party_email, "knewhealth.com") == "third_party_observed"
    assert selected["primary_email"] is None
    assert selected["evidence_kind"] is None
