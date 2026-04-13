from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse

import httpx
from selectolax.parser import HTMLParser

from app.core.config import get_settings
from app.db.models import BusinessEntity
from app.services.site_fetch import FetchedPage, SiteFetchOutcome, fetch_allowlisted_site_pages
from app.services.robots_guard import RobotsGuard


PHONE_HINT_KEYS = (
    "phone",
    "telephone",
    "phone_number",
    "business_phone",
    "registered_agent_phone",
)
ADDRESS_HINT_KEYS = ("address_line1", "principal_address_1", "mail_address_1")
ALIAS_HINT_KEYS = ("dba_name", "trade_name", "alternate_name")
TEXT_RE = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class IdentitySignal:
    kind: str
    value: str
    source_url: str
    confidence: float


@dataclass(slots=True)
class SiteIdentityOutcome:
    verified: bool
    confidence: float
    matched_signals: list[IdentitySignal]
    visited_urls: list[str]
    blocked_urls: list[str]


class SiteInspector(Protocol):
    async def inspect(self, entity: BusinessEntity, homepage_url: str) -> SiteIdentityOutcome:
        raise NotImplementedError


class HttpSiteInspector:
    def __init__(
        self,
        *,
        robots_guard: RobotsGuard | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._robots_guard = robots_guard
        self._client = client

    async def inspect(self, entity: BusinessEntity, homepage_url: str) -> SiteIdentityOutcome:
        fetch_outcome = await fetch_allowlisted_site_pages(
            homepage_url,
            self._robots_guard,
            self._client,
            max_pages=get_settings().evidence_max_pages,
        )
        return evaluate_site_identity(entity, fetch_outcome)


class FakeSiteInspector:
    def __init__(self, outcomes: dict[str, SiteIdentityOutcome]) -> None:
        self._outcomes = outcomes

    async def inspect(self, entity: BusinessEntity, homepage_url: str) -> SiteIdentityOutcome:
        return self._outcomes.get(
            homepage_url,
            SiteIdentityOutcome(
                verified=False,
                confidence=0.0,
                matched_signals=[],
                visited_urls=[],
                blocked_urls=[],
            ),
        )


def evaluate_site_identity(entity: BusinessEntity, fetch_outcome: SiteFetchOutcome) -> SiteIdentityOutcome:
    payload = entity.registry_payload or {}
    matched_signals: list[IdentitySignal] = []

    legal_name = entity.legal_name
    normalized_name = entity.normalized_name
    aliases = [str(payload.get(key)).strip() for key in ALIAS_HINT_KEYS if payload.get(key)]
    city = str(payload.get("city") or payload.get("mail_city") or "").strip()
    state_name = str(payload.get("state_name") or entity.state).strip()
    address_line1 = next((str(payload.get(key)).strip() for key in ADDRESS_HINT_KEYS if payload.get(key)), "")
    phone_hint = next((normalize_phone(str(payload.get(key))) for key in PHONE_HINT_KEYS if payload.get(key)), "")

    for page in fetch_outcome.pages:
        page_text = normalized_text(extract_page_text(page))
        compact_page_text = compact_text(extract_page_text(page))
        page_path = urlparse(page.url).path.casefold()

        if contains_phrase(page_text, legal_name) or contains_phrase(compact_page_text, compact_text(legal_name)):
            matched_signals.append(
                IdentitySignal("legal_name_exact", legal_name, page.url, 0.55),
            )
        elif contains_phrase(page_text, normalized_name):
            matched_signals.append(
                IdentitySignal("legal_name_normalized", normalized_name, page.url, 0.40),
            )

        for alias in aliases:
            if contains_phrase(page_text, alias):
                matched_signals.append(
                    IdentitySignal("alias_exact", alias, page.url, 0.25),
                )

        if city and state_name and city.casefold() in page_text and state_name.casefold() in page_text:
            matched_signals.append(
                IdentitySignal("city_state_match", f"{city}, {state_name}", page.url, 0.15),
            )

        if address_line1 and contains_phrase(page_text, address_line1):
            matched_signals.append(
                IdentitySignal("address_match", address_line1, page.url, 0.25),
            )

        if phone_hint and phone_hint in normalize_phone(page_text):
            matched_signals.append(
                IdentitySignal("phone_match", phone_hint, page.url, 0.20),
            )

        if any(keyword in page_path for keyword in ("contact", "about", "support")):
            matched_signals.append(
                IdentitySignal("contact_page_path", page_path or "/", page.url, 0.10),
            )

    signal_scores = _best_signal_scores(matched_signals)
    confidence = round(min(sum(signal_scores.values()), 1.0), 4)

    has_name_signal = any(
        kind in signal_scores for kind in {"legal_name_exact", "legal_name_normalized", "alias_exact"}
    )
    has_corroborating_signal = any(
        kind in signal_scores
        for kind in {"city_state_match", "address_match", "phone_match", "contact_page_path"}
    )

    verified = has_name_signal and has_corroborating_signal and confidence >= get_settings().site_identity_threshold
    return SiteIdentityOutcome(
        verified=verified,
        confidence=confidence,
        matched_signals=_dedupe_signals(matched_signals),
        visited_urls=fetch_outcome.visited_urls,
        blocked_urls=fetch_outcome.blocked_urls,
    )


def extract_page_text(page: FetchedPage) -> str:
    parser = HTMLParser(page.html)
    title = parser.css_first("title")
    title_text = title.text(strip=True) if title else ""
    return f"{title_text} {parser.text(separator=' ')}".strip()


def normalized_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return " ".join(TEXT_RE.sub(" ", normalized.casefold()).split())


def compact_text(value: str) -> str:
    return "".join(token for token in TEXT_RE.split(normalized_text(value)) if token)


def contains_phrase(text: str, phrase: str) -> bool:
    normalized_phrase = normalized_text(phrase)
    if not normalized_phrase:
        return False
    return normalized_phrase in text


def normalize_phone(value: str) -> str:
    return re.sub(r"[^\d+]", "", value)


def _best_signal_scores(signals: list[IdentitySignal]) -> dict[str, float]:
    best_scores: dict[str, float] = {}
    for signal in signals:
        best_scores[signal.kind] = max(best_scores.get(signal.kind, 0.0), signal.confidence)
    return best_scores


def _dedupe_signals(signals: list[IdentitySignal]) -> list[IdentitySignal]:
    deduped: dict[tuple[str, str, str], IdentitySignal] = {}
    for signal in signals:
        key = (signal.kind, signal.value, signal.source_url)
        existing = deduped.get(key)
        if existing is None or signal.confidence > existing.confidence:
            deduped[key] = signal
    return list(deduped.values())
