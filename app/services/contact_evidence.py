from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

from app.db.models import ContactKind, OfficialDomain
from app.services.robots_guard import RobotsGuard
from app.services.site_fetch import fetch_allowlisted_site_pages

EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")
PHONE_RE = re.compile(r"\+?\d[\d\s().-]{7,}\d")
CONTACT_PAGE_HINTS = ("contact", "support", "about")
DIRECT_WEBSITE_CONTACT_KINDS = frozenset({ContactKind.email, ContactKind.phone})
WEBSITE_CONTACT_KINDS = (
    ContactKind.email,
    ContactKind.phone,
    ContactKind.contact_form,
    ContactKind.contact_page,
)


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

    if is_contact_page_url(source_url):
        evidence.append(
            ExtractedEvidence(
                kind=ContactKind.contact_page,
                value=canonicalize_url(source_url),
                source_url=source_url,
                source_hash=source_hash,
                confidence=0.60,
                notes="contact_page_observed",
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


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def is_contact_page_url(url: str) -> bool:
    path = (urlparse(url).path or "/").casefold()
    if path in {"", "/"}:
        return False
    return any(keyword in path for keyword in CONTACT_PAGE_HINTS)


def classify_collection_outcome(
    evidence: list[ExtractedEvidence],
    *,
    visited_urls: list[str],
    blocked_urls: list[str],
) -> tuple[str, str | None]:
    kinds = {item.kind for item in evidence}
    if kinds & DIRECT_WEBSITE_CONTACT_KINDS:
        return "website_contact_observed", None
    if ContactKind.contact_form in kinds:
        return "contact_form_only", None
    if ContactKind.contact_page in kinds:
        return "contact_page_only", None
    if not visited_urls and blocked_urls:
        return "robots_blocked", "robots_blocked"
    return "unresolved", "unresolved"


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
    outcome, review_reason = classify_collection_outcome(
        deduped_evidence,
        visited_urls=fetch_outcome.visited_urls,
        blocked_urls=fetch_outcome.blocked_urls,
    )

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
