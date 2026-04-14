from __future__ import annotations

from uuid import uuid4

import pytest
from app.db.models import ContactKind, OfficialDomain
from app.services.contact_evidence import (
    collect_public_evidence_for_domain,
    extract_evidence_from_html,
)
from app.services.site_fetch import (
    FetchedPage,
    SiteFetchOutcome,
    extract_internal_allowlisted_links,
)


def test_extract_internal_allowlisted_links_limits_to_same_domain() -> None:
    html = """
    <html>
      <body>
        <a href="/contact">Contact</a>
        <a href="https://example.com/privacy">Privacy</a>
        <a href="https://linkedin.com/company/example">LinkedIn</a>
      </body>
    </html>
    """

    links = extract_internal_allowlisted_links("https://example.com", html)

    assert "https://example.com/contact" in links
    assert "https://example.com/privacy" in links
    assert all("linkedin.com" not in link for link in links)


def test_extract_internal_allowlisted_links_ignores_null_href_values() -> None:
    html = """
    <html>
      <body>
        <a href>Broken</a>
        <a href="/contact">Contact</a>
      </body>
    </html>
    """

    links = extract_internal_allowlisted_links("https://example.com", html)

    assert links == ["https://example.com/contact"]


def test_extract_evidence_from_html_captures_contact_page_phone_and_form() -> None:
    html = """
    <html>
      <body>
        <p>Call us at (305) 555-1212</p>
        <form action="/contact/submit">
          <input type="email" name="email" />
          <textarea name="message"></textarea>
        </form>
      </body>
    </html>
    """

    evidence = extract_evidence_from_html("https://example.com/contact", html)
    values_by_kind = {(item.kind, item.value) for item in evidence}

    assert (ContactKind.phone, "3055551212") in values_by_kind
    assert (ContactKind.contact_form, "https://example.com/contact/submit") in values_by_kind
    assert (ContactKind.contact_page, "https://example.com/contact") in values_by_kind


@pytest.mark.asyncio
async def test_collect_public_evidence_for_domain_marks_phone_as_website_contact_observed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    domain = OfficialDomain(
        id=uuid4(),
        entity_id=uuid4(),
        domain="example.com",
        homepage_url="https://example.com",
    )

    async def fake_fetch(*args, **kwargs) -> SiteFetchOutcome:
        return SiteFetchOutcome(
            pages=[
                FetchedPage(
                    url="https://example.com/contact",
                    html="<html><body>Reach us at 305.555.1212</body></html>",
                )
            ],
            visited_urls=["https://example.com/contact"],
            blocked_urls=[],
        )

    monkeypatch.setattr("app.services.contact_evidence.fetch_allowlisted_site_pages", fake_fetch)

    outcome = await collect_public_evidence_for_domain(domain)

    assert outcome.outcome == "website_contact_observed"
    assert outcome.review_reason is None
    assert any(item.kind == ContactKind.phone for item in outcome.evidence)
    assert any(item.kind == ContactKind.contact_page for item in outcome.evidence)


@pytest.mark.asyncio
async def test_collect_public_evidence_for_domain_marks_contact_form_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    domain = OfficialDomain(
        id=uuid4(),
        entity_id=uuid4(),
        domain="example.com",
        homepage_url="https://example.com",
    )

    async def fake_fetch(*args, **kwargs) -> SiteFetchOutcome:
        return SiteFetchOutcome(
            pages=[
                FetchedPage(
                    url="https://example.com/support",
                    html="""
                    <html>
                      <body>
                        <form action="/help">
                          <textarea name="message"></textarea>
                        </form>
                      </body>
                    </html>
                    """,
                )
            ],
            visited_urls=["https://example.com/support"],
            blocked_urls=[],
        )

    monkeypatch.setattr("app.services.contact_evidence.fetch_allowlisted_site_pages", fake_fetch)

    outcome = await collect_public_evidence_for_domain(domain)

    assert outcome.outcome == "contact_form_only"
    assert outcome.review_reason is None
    assert all(item.kind != ContactKind.email for item in outcome.evidence)
    assert any(item.kind == ContactKind.contact_form for item in outcome.evidence)


@pytest.mark.asyncio
async def test_collect_public_evidence_for_domain_marks_unresolved_when_no_contact_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    domain = OfficialDomain(
        id=uuid4(),
        entity_id=uuid4(),
        domain="example.com",
        homepage_url="https://example.com",
    )

    async def fake_fetch(*args, **kwargs) -> SiteFetchOutcome:
        return SiteFetchOutcome(
            pages=[
                FetchedPage(
                    url="https://example.com/privacy",
                    html="<html><body>Privacy policy only.</body></html>",
                )
            ],
            visited_urls=["https://example.com/privacy"],
            blocked_urls=[],
        )

    monkeypatch.setattr("app.services.contact_evidence.fetch_allowlisted_site_pages", fake_fetch)

    outcome = await collect_public_evidence_for_domain(domain)

    assert outcome.outcome == "unresolved"
    assert outcome.review_reason == "unresolved"
    assert outcome.evidence == []
