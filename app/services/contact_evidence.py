from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from urllib.parse import urljoin

import httpx
from selectolax.parser import HTMLParser

from app.db.models import ContactKind, OfficialDomain
from app.services.robots_guard import RobotsGuard
from app.services.site_fetch import fetch_allowlisted_site_pages


EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")
PHONE_RE = re.compile(r"\+?\d[\d\s().-]{7,}\d")


@dataclass(slots=True)
class ExtractedEvidence:
    kind: ContactKind
    value: str
    source_url: str
    source_hash: str
    confidence: float
    notes: str | None = None


@dataclass(slots=True)
class CollectionOutcome:
    evidence: list[ExtractedEvidence]
    visited_urls: list[str]
    blocked_urls: list[str]
    outcome: str
    review_reason: str | None


def hash_content(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def extract_evidence_from_html(source_url: str, html: str) -> list[ExtractedEvidence]:
    parser = HTMLParser(html)
    source_hash = hash_content(html)
    evidence: list[ExtractedEvidence] = []
    visible_text = parser.text(separator=" ")

    for match in sorted(set(EMAIL_RE.findall(visible_text))):
        evidence.append(
            ExtractedEvidence(
                kind=ContactKind.email,
                value=match.lower(),
                source_url=source_url,
                source_hash=source_hash,
                confidence=0.85,
            ),
        )

    for anchor in parser.css("a[href^='mailto:']"):
        mailto = anchor.attributes.get("href", "").removeprefix("mailto:").strip()
        if mailto:
            evidence.append(
                ExtractedEvidence(
                    kind=ContactKind.email,
                    value=mailto.lower(),
                    source_url=source_url,
                    source_hash=source_hash,
                    confidence=0.95,
                    notes="explicit_mailto",
                ),
            )

    for match in sorted(set(PHONE_RE.findall(visible_text))):
        evidence.append(
            ExtractedEvidence(
                kind=ContactKind.phone,
                value=normalize_phone(match),
                source_url=source_url,
                source_hash=source_hash,
                confidence=0.70,
            ),
        )

    for form in parser.css("form"):
        action = form.attributes.get("action", "").strip()
        if not action:
            continue
        if form.css("input[type='email'], textarea"):
            evidence.append(
                ExtractedEvidence(
                    kind=ContactKind.contact_form,
                    value=urljoin(source_url, action),
                    source_url=source_url,
                    source_hash=source_hash,
                    confidence=0.80,
                    notes="contact_form",
                ),
            )

    deduped: dict[tuple[ContactKind, str, str], ExtractedEvidence] = {}
    for item in evidence:
        key = (item.kind, item.value, item.source_hash)
        existing = deduped.get(key)
        if existing is None or item.confidence > existing.confidence:
            deduped[key] = item
    return list(deduped.values())


def normalize_phone(value: str) -> str:
    return re.sub(r"[^\d+]", "", value)


async def collect_public_evidence_for_domain(
    domain: OfficialDomain,
    robots_guard: RobotsGuard | None = None,
    client: httpx.AsyncClient | None = None,
) -> CollectionOutcome:
    fetch_outcome = await fetch_allowlisted_site_pages(domain.homepage_url, robots_guard, client)
    evidence: list[ExtractedEvidence] = []
    for page in fetch_outcome.pages:
        evidence.extend(extract_evidence_from_html(page.url, page.html))

    deduped_evidence = _dedupe_evidence(evidence)

    email_found = any(item.kind == ContactKind.email for item in deduped_evidence)
    contact_form_found = any(item.kind == ContactKind.contact_form for item in deduped_evidence)

    if email_found:
        outcome = "email_found"
        review_reason = None
    elif contact_form_found:
        outcome = "contact_form_found"
        review_reason = None
    elif not fetch_outcome.visited_urls and fetch_outcome.blocked_urls:
        outcome = "robots_blocked"
        review_reason = "robots_blocked"
    else:
        outcome = "no_public_contact_found"
        review_reason = "no_public_contact_found"

    return CollectionOutcome(
        evidence=deduped_evidence,
        visited_urls=fetch_outcome.visited_urls,
        blocked_urls=fetch_outcome.blocked_urls,
        outcome=outcome,
        review_reason=review_reason,
    )


def _dedupe_evidence(evidence: list[ExtractedEvidence]) -> list[ExtractedEvidence]:
    deduped: dict[tuple[ContactKind, str, str], ExtractedEvidence] = {}
    for item in evidence:
        key = (item.kind, item.value, item.source_hash)
        existing = deduped.get(key)
        if existing is None or item.confidence > existing.confidence:
            deduped[key] = item
    return list(deduped.values())
