from __future__ import annotations

import uuid
from datetime import date, datetime
from pathlib import Path

import httpx
import pytest
from app.db.models import ArtifactKind, BusinessEntity, EntityStatus
from app.services.object_store import LocalObjectStore
from app.services.sunbiz_harvest import (
    build_sunbiz_document_search_url,
    extract_detail_url,
    extract_document_image_links,
    harvest_sunbiz_entity,
    is_pdf_mature,
    should_retry_pdf,
)


def make_entity() -> BusinessEntity:
    return BusinessEntity(
        id=uuid.uuid4(),
        state="FL",
        external_filing_id="P24000012345",
        legal_name="Sunrise Health LLC",
        normalized_name="sunrise health",
        status=EntityStatus.active,
        formed_at=date(2024, 1, 15),
        registry_payload={"last_transaction_date": "2026-04-10"},
        first_seen_at=datetime.now(),
        last_seen_at=datetime.now(),
    )


def test_extract_detail_and_pdf_links() -> None:
    search_url = build_sunbiz_document_search_url("P24000012345")
    detail_href = "/Inquiry/CorporationSearch/SearchResults?Detail=filing-record"
    html = f"""
    <html>
      <body>
        <a href="{detail_href}">Detail by Document Number</a>
      </body>
    </html>
    """
    detail_url = extract_detail_url(search_url, html)
    assert detail_url is not None
    assert detail_url.endswith("Detail=filing-record")

    detail_html = """
    <html>
      <body>
        <a href="/DocumentImages/12345.pdf">View image in PDF format</a>
      </body>
    </html>
    """
    assert extract_document_image_links(detail_url, detail_html) == [
        "https://search.sunbiz.org/DocumentImages/12345.pdf"
    ]


@pytest.mark.asyncio
async def test_harvest_sunbiz_entity_stores_html_and_pdf_and_extracts_email(tmp_path: Path) -> None:
    entity = make_entity()
    store = LocalObjectStore(tmp_path / "objects")

    detail_href = "/Inquiry/CorporationSearch/SearchResults?Detail=filing-record"
    search_html = f"""
    <html>
      <body>
        <a href="{detail_href}">Detail by Document Number</a>
      </body>
    </html>
    """
    detail_html = """
    <html>
      <body>
        <div>Contact us at corp@example.com</div>
        <a href="/DocumentImages/12345.pdf">View image in PDF format</a>
      </body>
    </html>
    """
    pdf_payload = b"support@example.com"

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "ByDocumentNumber" in url:
            return httpx.Response(200, request=request, text=search_html)
        if "Detail=filing-record" in url:
            return httpx.Response(200, request=request, text=detail_html)
        if "DocumentImages/12345.pdf" in url:
            return httpx.Response(200, request=request, content=pdf_payload)
        return httpx.Response(404, request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        outcome = await harvest_sunbiz_entity(entity, store, client)

    assert outcome.review_reason is None
    assert any(item.value == "corp@example.com" for item in outcome.evidence)
    assert any(item.value == "support@example.com" for item in outcome.evidence)
    assert any(item.artifact_kind == ArtifactKind.sunbiz_detail_html for item in outcome.artifacts)
    assert any(item.artifact_kind == ArtifactKind.sunbiz_filing_pdf for item in outcome.artifacts)


def test_pdf_maturity_and_retry_windows_are_complementary() -> None:
    recent = make_entity()
    recent.registry_payload = {"last_transaction_date": "2026-04-10"}

    mature = make_entity()
    mature.registry_payload = {"last_transaction_date": "2026-03-01"}

    assert should_retry_pdf(recent) is True
    assert is_pdf_mature(recent) is False
    assert should_retry_pdf(mature) is False
    assert is_pdf_mature(mature) is True
