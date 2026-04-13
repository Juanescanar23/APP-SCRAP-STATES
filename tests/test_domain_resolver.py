from __future__ import annotations

import pytest

from app.db.models import BusinessEntity, DomainStatus, EntityStatus
from app.services.domain_resolver import build_domain_queries, resolve_entity_domains, score_search_result
from app.services.search_provider import FakeSearchProvider, NullSearchProvider, SearchResult
from app.services.site_identity import FakeSiteInspector, IdentitySignal, SiteIdentityOutcome


@pytest.fixture
def entity() -> BusinessEntity:
    return BusinessEntity(
        state="FL",
        external_filing_id="P24000012345",
        legal_name="Sunrise Labs LLC",
        normalized_name="sunrise labs",
        status=EntityStatus.active,
        registry_payload={"city": "Miami", "state_name": "FL", "dba_name": "Sunrise Health"},
    )


def make_search_result(url: str, *, title: str, snippet: str, rank: int = 1) -> SearchResult:
    return SearchResult(
        url=url,
        title=title,
        snippet=snippet,
        rank=rank,
        provider="fake",
        raw={},
    )


def make_identity_outcome(
    *,
    homepage_url: str,
    verified: bool,
    confidence: float,
    signal_kind: str = "legal_name_exact",
) -> SiteIdentityOutcome:
    return SiteIdentityOutcome(
        verified=verified,
        confidence=confidence,
        matched_signals=[
            IdentitySignal(signal_kind, "Sunrise Labs LLC", homepage_url, confidence),
            IdentitySignal("contact_page_path", "/contact", f"{homepage_url}/contact", 0.10),
        ],
        visited_urls=[homepage_url],
        blocked_urls=[],
    )


def test_build_domain_queries_uses_exact_phrase_city_and_contact(entity: BusinessEntity) -> None:
    queries = build_domain_queries(entity)

    assert '"Sunrise Labs LLC" Miami FL' in queries
    assert '"Sunrise Labs LLC" Miami FL official site' in queries
    assert '"Sunrise Labs LLC" contact' in queries
    assert '"Sunrise Health" Miami FL' in queries


def test_score_search_result_filters_directory_hosts(entity: BusinessEntity) -> None:
    result = make_search_result(
        "https://www.bizapedia.com/fl/sunrise-labs-llc.html",
        title="Sunrise Labs LLC in Florida",
        snippet="Directory listing",
    )

    assert score_search_result(entity, result, '"Sunrise Labs LLC" Miami FL') is None


@pytest.mark.asyncio
async def test_resolve_entity_domains_marks_single_site_match_as_verified(entity: BusinessEntity) -> None:
    query = '"Sunrise Labs LLC" Miami FL'
    provider = FakeSearchProvider(
        {
            query: [
                make_search_result(
                    "https://sunriselabs.com",
                    title="Sunrise Labs LLC - Home",
                    snippet="Miami FL diagnostics and contact information.",
                ),
                make_search_result(
                    "https://sunrise-labs.net",
                    title="Sunrise Labs Network",
                    snippet="Independent network directory",
                    rank=2,
                ),
            ],
        },
    )
    inspector = FakeSiteInspector(
        {
            "https://sunriselabs.com": make_identity_outcome(
                homepage_url="https://sunriselabs.com",
                verified=True,
                confidence=0.82,
            ),
        },
    )

    outcome = await resolve_entity_domains(entity, provider, inspector)

    assert outcome.review_reason is None
    assert any(candidate.status == DomainStatus.verified for candidate in outcome.candidates)
    assert any(candidate.status == DomainStatus.candidate for candidate in outcome.candidates)
    verified_candidate = next(candidate for candidate in outcome.candidates if candidate.status == DomainStatus.verified)
    assert verified_candidate.domain == "sunriselabs.com"
    assert verified_candidate.evidence["search_evidence"][0]["provider"] == "fake"


@pytest.mark.asyncio
async def test_resolve_entity_domains_ignores_directory_and_social_only(entity: BusinessEntity) -> None:
    query = '"Sunrise Labs LLC" Miami FL'
    provider = FakeSearchProvider(
        {
            query: [
                make_search_result(
                    "https://www.bizapedia.com/fl/sunrise-labs-llc.html",
                    title="Sunrise Labs LLC in Florida",
                    snippet="Directory listing",
                ),
                make_search_result(
                    "https://linkedin.com/company/sunrise-labs",
                    title="Sunrise Labs",
                    snippet="LinkedIn",
                    rank=2,
                ),
            ],
        },
    )

    outcome = await resolve_entity_domains(entity, provider, FakeSiteInspector({}))

    assert outcome.candidates == []
    assert outcome.review_reason == "domain_unresolved"


@pytest.mark.asyncio
async def test_resolve_entity_domains_keeps_ambiguous_matches_in_review(entity: BusinessEntity) -> None:
    query = '"Sunrise Labs LLC" Miami FL'
    provider = FakeSearchProvider(
        {
            query: [
                make_search_result(
                    "https://sunriselabs.com",
                    title="Sunrise Labs LLC - Home",
                    snippet="Miami FL diagnostics",
                ),
                make_search_result(
                    "https://sunrise-labs.net",
                    title="Sunrise Labs LLC Network - Home",
                    snippet="Miami FL diagnostics",
                    rank=2,
                ),
            ],
        },
    )
    inspector = FakeSiteInspector(
        {
            "https://sunriselabs.com": make_identity_outcome(
                homepage_url="https://sunriselabs.com",
                verified=True,
                confidence=0.78,
            ),
            "https://sunrise-labs.net": make_identity_outcome(
                homepage_url="https://sunrise-labs.net",
                verified=True,
                confidence=0.76,
            ),
        },
    )

    outcome = await resolve_entity_domains(entity, provider, inspector)

    assert outcome.review_reason == "ambiguous_candidates"
    assert all(candidate.status == DomainStatus.candidate for candidate in outcome.candidates)


@pytest.mark.asyncio
async def test_resolve_entity_domains_handles_no_results(entity: BusinessEntity) -> None:
    outcome = await resolve_entity_domains(entity, FakeSearchProvider({}), FakeSiteInspector({}))

    assert outcome.candidates == []
    assert outcome.review_reason == "domain_unresolved"


@pytest.mark.asyncio
async def test_resolve_entity_domains_without_provider_queues_review(entity: BusinessEntity) -> None:
    outcome = await resolve_entity_domains(entity, NullSearchProvider(), FakeSiteInspector({}))

    assert outcome.candidates == []
    assert outcome.review_reason == "search_provider_unavailable"
