from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(slots=True)
class DomainResolutionMetrics:
    imported_entities: int = 0
    domain_candidates_generated: int = 0
    domain_verified: int = 0
    domain_unresolved: int = 0
    review_items_queued: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass(slots=True)
class EvidenceCollectionMetrics:
    website_contact_observed: int = 0
    contact_form_only: int = 0
    contact_page_only: int = 0
    evidence_email_found: int = 0
    evidence_phone_found: int = 0
    evidence_contact_form_found: int = 0
    evidence_contact_page_found: int = 0
    robots_blocked: int = 0
    unresolved: int = 0
    no_public_contact_found: int = 0
    evidence_rows_persisted: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)
