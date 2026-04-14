from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from app.core.config import get_settings
from app.db.models import BusinessEntity, DomainStatus
from app.services.normalizer import normalize_company_name
from app.services.scoring import (
    overlap_score,
    score_candidate_domain,
    string_score,
)
from app.services.search_provider import (
    SearchProvider,
    SearchProviderError,
    SearchResult,
)
from app.services.site_identity import SiteIdentityOutcome, SiteInspector

DOMAIN_HINT_KEYS = ("website", "homepage", "url", "domain", "business_website")
LOCATION_HINT_KEYS = ("city", "state_name", "mail_city", "mail_state")
ALIAS_KEYS = ("dba_name", "trade_name", "alternate_name")
BLOCKED_RESULT_HOSTS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "yelp.com",
    "yellowpages.com",
    "mapquest.com",
    "bbb.org",
    "bizapedia.com",
    "opencorporates.com",
    "chamberofcommerce.com",
}


@dataclass(slots=True)
class ResolvedDomainCandidate:
    domain: str
    homepage_url: str
    status: DomainStatus
    confidence: float
    evidence: dict[str, Any]
    search_confidence: float = 0.0
    identity_confidence: float = 0.0


@dataclass(slots=True)
class DomainResolutionOutcome:
    candidates: list[ResolvedDomainCandidate]
    review_reason: str | None
    queries: list[str]


@dataclass(slots=True)
class _CandidateRecord:
    domain: str
    homepage_url: str
    search_confidence: float
    search_evidence: list[dict[str, Any]] = field(default_factory=list)
    registry_hints: list[dict[str, Any]] = field(default_factory=list)
    site_identity: dict[str, Any] | None = None


def normalize_domain(raw_value: str) -> tuple[str, str]:
    candidate = raw_value.strip()
    if not candidate:
        raise ValueError("Domain candidate cannot be empty.")

    if not candidate.startswith(("http://", "https://")):
        candidate = f"https://{candidate}"

    parsed = urlparse(candidate)
    hostname = (parsed.hostname or "").casefold().removeprefix("www.")
    if not hostname:
        raise ValueError("Domain candidate is missing a hostname.")

    homepage_url = f"{parsed.scheme or 'https'}://{hostname}"
    return hostname, homepage_url


def extract_domain_hints(payload: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in DOMAIN_HINT_KEYS:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            hints.append(value.strip())
    return hints


def extract_location_hint(payload: dict[str, Any], state: str) -> str:
    parts = [str(payload.get(key)).strip() for key in LOCATION_HINT_KEYS if payload.get(key)]
    if state:
        parts.append(state)
    return " ".join(part for part in parts if part)


def extract_aliases(payload: dict[str, Any]) -> list[str]:
    aliases: list[str] = []
    for key in ALIAS_KEYS:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            aliases.append(value.strip())
    return aliases


def build_domain_queries(entity: BusinessEntity) -> list[str]:
    payload = entity.registry_payload or {}
    city = str(payload.get("city") or payload.get("mail_city") or "").strip()
    state = entity.state.upper()
    aliases = extract_aliases(payload)

    base_location = f"{city} {state}".strip() if city else state
    queries = [
        f'"{entity.legal_name}" {base_location}'.strip(),
        f'"{entity.legal_name}" {base_location} official site'.strip(),
        f'"{entity.legal_name}" contact',
    ]
    for alias in aliases:
        queries.append(f'"{alias}" {base_location}'.strip())

    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        normalized = " ".join(query.split())
        if normalized and normalized not in seen:
            deduped.append(normalized)
            seen.add(normalized)
    return deduped


def is_blocked_result_host(hostname: str) -> bool:
    return any(
        hostname == blocked or hostname.endswith(f".{blocked}")
        for blocked in BLOCKED_RESULT_HOSTS
    )


def score_search_result(
    entity: BusinessEntity, result: SearchResult, query: str
) -> ResolvedDomainCandidate | None:
    try:
        domain, homepage_url = normalize_domain(result.url)
    except ValueError:
        return None
    if is_blocked_result_host(domain):
        return None

    payload = entity.registry_payload or {}
    location_hint = extract_location_hint(payload, entity.state)
    title_name = normalize_company_name(result.title)
    snippet_name = normalize_company_name(result.snippet)

    domain_score = score_candidate_domain(
        entity.normalized_name, domain, location_hint=location_hint
    )
    title_score = max(
        overlap_score(entity.normalized_name, title_name),
        string_score(entity.normalized_name, title_name),
    )
    snippet_score = overlap_score(entity.normalized_name, snippet_name)
    if location_hint:
        location_score = overlap_score(location_hint, result.title + " " + result.snippet)
    else:
        location_score = 0.0
    rank_score = max(0.0, 1.0 - ((result.rank - 1) * 0.12))

    confidence = round(
        min(
            1.0,
            (domain_score * 0.45)
            + (title_score * 0.25)
            + (snippet_score * 0.10)
            + (location_score * 0.10)
            + (rank_score * 0.10),
        ),
        4,
    )

    if confidence < get_settings().domain_candidate_threshold:
        return None

    return ResolvedDomainCandidate(
        domain=domain,
        homepage_url=homepage_url,
        status=DomainStatus.candidate,
        confidence=confidence,
        search_confidence=confidence,
        evidence={
            "search_evidence": [
                {
                    "provider": result.provider,
                    "query": query,
                    "rank": result.rank,
                    "title": result.title,
                    "snippet": result.snippet,
                    "result_url": result.url,
                    "normalized_domain": domain,
                    "score": confidence,
                },
            ],
        },
    )


async def resolve_entity_domains(
    entity: BusinessEntity,
    search_provider: SearchProvider,
    site_inspector: SiteInspector,
) -> DomainResolutionOutcome:
    payload = entity.registry_payload or {}
    queries = build_domain_queries(entity)
    candidates_by_domain: dict[str, _CandidateRecord] = {}
    provider_runtime_error = False

    for hint in extract_domain_hints(payload):
        domain, homepage_url = normalize_domain(hint)
        confidence = score_candidate_domain(
            entity.normalized_name,
            domain,
            location_hint=extract_location_hint(payload, entity.state),
        )
        record = candidates_by_domain.setdefault(
            domain,
            _CandidateRecord(
                domain=domain,
                homepage_url=homepage_url,
                search_confidence=max(confidence, 0.60),
            ),
        )
        record.search_confidence = max(record.search_confidence, max(confidence, 0.60))
        record.registry_hints.append(
            {
                "field_value": hint,
                "normalized_domain": domain,
                "score": max(confidence, 0.60),
            },
        )

    if search_provider.provider_name != "none":
        for query in queries:
            try:
                search_results = await search_provider.search(
                    query,
                    max_results=get_settings().search_results_per_query,
                )
            except SearchProviderError:
                provider_runtime_error = True
                continue

            for result in search_results:
                candidate = score_search_result(entity, result, query)
                if candidate is None:
                    continue
                record = candidates_by_domain.setdefault(
                    candidate.domain,
                    _CandidateRecord(
                        domain=candidate.domain,
                        homepage_url=candidate.homepage_url,
                        search_confidence=candidate.search_confidence,
                    ),
                )
                record.search_confidence = max(
                    record.search_confidence, candidate.search_confidence
                )
                record.search_evidence.extend(candidate.evidence.get("search_evidence", []))

    if not candidates_by_domain:
        if search_provider.provider_name == "none" or provider_runtime_error:
            return DomainResolutionOutcome([], "search_provider_unavailable", queries)
        return DomainResolutionOutcome([], "domain_unresolved", queries)

    records = list(candidates_by_domain.values())
    identity_outcomes = await _inspect_candidates(entity, records, site_inspector)

    verified_indices: list[int] = []
    candidates: list[ResolvedDomainCandidate] = []
    for index, (record, identity_outcome) in enumerate(
        zip(records, identity_outcomes, strict=True)
    ):
        record.site_identity = _serialize_site_identity(identity_outcome)
        final_confidence = round(
            min((record.search_confidence * 0.35) + (identity_outcome.confidence * 0.65), 1.0),
            4,
        )
        candidate = ResolvedDomainCandidate(
            domain=record.domain,
            homepage_url=record.homepage_url,
            status=DomainStatus.candidate,
            confidence=final_confidence,
            search_confidence=record.search_confidence,
            identity_confidence=identity_outcome.confidence,
            evidence={
                "search_evidence": sorted(
                    record.search_evidence,
                    key=lambda item: (item.get("query", ""), item.get("rank", 999)),
                ),
                "registry_hints": record.registry_hints,
                "site_identity": record.site_identity,
            },
        )
        candidates.append(candidate)
        if identity_outcome.verified:
            verified_indices.append(index)

    candidates.sort(key=lambda candidate: (-candidate.confidence, candidate.domain))

    review_reason = _determine_review_reason(
        candidates,
        verified_indices,
        search_provider.provider_name,
        provider_runtime_error,
    )
    if review_reason is None:
        _promote_single_verified_candidate(candidates)

    return DomainResolutionOutcome(
        candidates=candidates,
        review_reason=review_reason,
        queries=queries,
    )


async def _inspect_candidates(
    entity: BusinessEntity,
    records: list[_CandidateRecord],
    site_inspector: SiteInspector,
) -> list[SiteIdentityOutcome]:
    return await asyncio.gather(
        *[site_inspector.inspect(entity, record.homepage_url) for record in records],
    )


def _serialize_site_identity(outcome: SiteIdentityOutcome) -> dict[str, Any]:
    return {
        "verified": outcome.verified,
        "confidence": outcome.confidence,
        "visited_urls": outcome.visited_urls,
        "blocked_urls": outcome.blocked_urls,
        "matched_signals": [
            {
                "kind": signal.kind,
                "value": signal.value,
                "source_url": signal.source_url,
                "confidence": signal.confidence,
            }
            for signal in outcome.matched_signals
        ],
    }


def _determine_review_reason(
    candidates: list[ResolvedDomainCandidate],
    verified_indices: list[int],
    provider_name: str,
    provider_runtime_error: bool = False,
) -> str | None:
    if len(verified_indices) == 1:
        return None
    if len(verified_indices) > 1:
        return "ambiguous_candidates"
    if candidates:
        return "candidate_needs_review"
    if provider_name == "none" or provider_runtime_error:
        return "search_provider_unavailable"
    return "domain_unresolved"


def _promote_single_verified_candidate(candidates: list[ResolvedDomainCandidate]) -> None:
    for candidate in candidates:
        if candidate.evidence["site_identity"]["verified"]:
            candidate.status = DomainStatus.verified
            break
