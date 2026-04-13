from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from typing import Any
from urllib.parse import urlencode, urljoin

import httpx
from selectolax.parser import HTMLParser

from app.core.config import get_settings
from app.db.models import ArtifactKind, BusinessEntity, ContactKind, SourceFileStatus
from app.services.contact_evidence import EMAIL_RE, ExtractedEvidence, extract_evidence_from_html
from app.services.object_store import ObjectStore


@dataclass(slots=True)
class HarvestedArtifact:
    artifact_kind: ArtifactKind
    source_url: str
    bucket_key: str | None
    content_hash: str | None
    status: SourceFileStatus
    next_retry_at: datetime | None
    metadata_json: dict[str, Any]


@dataclass(slots=True)
class SunbizHarvestOutcome:
    artifacts: list[HarvestedArtifact]
    evidence: list[ExtractedEvidence]
    detail_url: str | None
    review_reason: str | None


def build_sunbiz_document_search_url(document_number: str) -> str:
    settings = get_settings()
    query = urlencode(
        {
            "directionType": "Initial",
            "inquiryType": "DocumentNumber",
            "searchNameOrder": document_number,
        },
    )
    base_url = settings.fl_sunbiz_search_base_url.rstrip("/")
    return f"{base_url}/Inquiry/CorporationSearch/ByDocumentNumber?{query}"


def extract_detail_url(search_url: str, html: str) -> str | None:
    parser = HTMLParser(html)
    for anchor in parser.css("a[href]"):
        href = anchor.attributes.get("href", "").strip()
        if "Detail=" not in href:
            continue
        return urljoin(search_url, href)
    return None


def extract_document_image_links(detail_url: str, html: str) -> list[str]:
    parser = HTMLParser(html)
    links: list[str] = []
    for anchor in parser.css("a[href]"):
        href = anchor.attributes.get("href", "").strip()
        text = anchor.text(strip=True).casefold()
        if not href:
            continue
        if (
            href.casefold().endswith(".pdf")
            or "documentimages" in href.casefold()
            or "view image" in text
        ):
            links.append(urljoin(detail_url, href))
    return list(dict.fromkeys(links))


def extract_evidence_from_pdf(source_url: str, payload: bytes) -> list[ExtractedEvidence]:
    text = extract_pdf_text(payload)
    if not text:
        return []
    source_hash = hash_bytes(payload)

    evidence: list[ExtractedEvidence] = []
    for match in sorted(set(EMAIL_RE.findall(text))):
        evidence.append(
            ExtractedEvidence(
                kind=ContactKind.email,
                value=match.lower(),
                source_url=source_url,
                source_hash=source_hash,
                confidence=0.80,
                notes="sunbiz_pdf_observed",
            ),
        )
    return evidence


def extract_pdf_text(payload: bytes) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except ModuleNotFoundError:
        return payload.decode("latin-1", errors="ignore")

    try:
        reader = PdfReader(BytesIO(payload))
        return " ".join(page.extract_text() or "" for page in reader.pages)
    except Exception:
        return payload.decode("latin-1", errors="ignore")


async def harvest_sunbiz_entity(
    entity: BusinessEntity,
    object_store: ObjectStore,
    client: httpx.AsyncClient | None = None,
) -> SunbizHarvestOutcome:
    owned_client = client is None
    if client is None:
        client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=get_settings().http_timeout_seconds,
        )

    try:
        document_number = entity.external_filing_id
        search_url = build_sunbiz_document_search_url(document_number)
        search_response = await client.get(search_url)
        search_response.raise_for_status()

        detail_url = extract_detail_url(search_url, search_response.text)
        if not detail_url:
            return SunbizHarvestOutcome([], [], None, "sunbiz_lookup_missing")

        detail_response = await client.get(detail_url)
        detail_response.raise_for_status()
        detail_bucket_key = f"artifacts/fl/sunbiz/html/{document_number}.html"
        object_store.put_bytes(
            detail_bucket_key,
            detail_response.content,
            content_type="text/html; charset=utf-8",
            metadata={"document_number": document_number},
        )

        detail_artifact = HarvestedArtifact(
            artifact_kind=ArtifactKind.sunbiz_detail_html,
            source_url=detail_url,
            bucket_key=detail_bucket_key,
            content_hash=hash_bytes(detail_response.content),
            status=SourceFileStatus.completed,
            next_retry_at=None,
            metadata_json={"document_number": document_number},
        )

        evidence = extract_evidence_from_html(detail_url, detail_response.text)
        for item in evidence:
            item.notes = "sunbiz_html_observed"

        artifacts = [detail_artifact]
        pdf_links = extract_document_image_links(detail_url, detail_response.text)
        for index, pdf_url in enumerate(pdf_links, start=1):
            pdf_response = await client.get(pdf_url)
            pdf_response.raise_for_status()
            pdf_bucket_key = f"artifacts/fl/sunbiz/pdf/{document_number}/{index}.pdf"
            object_store.put_bytes(
                pdf_bucket_key,
                pdf_response.content,
                content_type="application/pdf",
                metadata={"document_number": document_number},
            )
            artifacts.append(
                HarvestedArtifact(
                    artifact_kind=ArtifactKind.sunbiz_filing_pdf,
                    source_url=pdf_url,
                    bucket_key=pdf_bucket_key,
                    content_hash=hash_bytes(pdf_response.content),
                    status=SourceFileStatus.completed,
                    next_retry_at=None,
                    metadata_json={"document_number": document_number, "index": index},
                ),
            )
            evidence.extend(extract_evidence_from_pdf(pdf_url, pdf_response.content))

        review_reason = None
        email_found = any(item.kind == ContactKind.email for item in evidence)
        if not pdf_links and not email_found and should_retry_pdf(entity):
            next_retry_at = datetime.now(UTC) + timedelta(days=1)
            artifacts.append(
                HarvestedArtifact(
                    artifact_kind=ArtifactKind.sunbiz_filing_pdf,
                    source_url=detail_url,
                    bucket_key=None,
                    content_hash=None,
                    status=SourceFileStatus.pending,
                    next_retry_at=next_retry_at,
                    metadata_json={
                        "document_number": document_number,
                        "reason": "document_images_pending",
                    },
                ),
            )
            review_reason = "sunbiz_pdf_pending"
        elif not evidence:
            review_reason = "sunbiz_no_public_contact"

        return SunbizHarvestOutcome(artifacts, dedupe_evidence(evidence), detail_url, review_reason)
    finally:
        if owned_client:
            await client.aclose()


def should_retry_pdf(entity: BusinessEntity) -> bool:
    latest = latest_registry_activity_date(entity)
    if latest is None:
        return False
    return (date.today() - latest).days <= get_settings().fl_pdf_retry_days


def is_pdf_mature(entity: BusinessEntity) -> bool:
    latest = latest_registry_activity_date(entity)
    if latest is None:
        return False
    return (date.today() - latest).days > get_settings().fl_pdf_retry_days


def latest_registry_activity_date(entity: BusinessEntity) -> date | None:
    payload = entity.registry_payload or {}
    candidate_dates = [
        parse_iso_date(str(payload.get("last_transaction_date") or "")),
        parse_iso_date(str(payload.get("formed_at") or "")),
        entity.formed_at,
    ]
    return max((value for value in candidate_dates if value is not None), default=None)


def parse_iso_date(value: str) -> date | None:
    value = value.strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def dedupe_evidence(evidence: list[ExtractedEvidence]) -> list[ExtractedEvidence]:
    deduped: dict[tuple[Any, str, str], ExtractedEvidence] = {}
    for item in evidence:
        key = (item.kind, item.value, item.source_hash)
        current = deduped.get(key)
        if current is None or item.confidence > current.confidence:
            deduped[key] = item
    return list(deduped.values())


def hash_bytes(payload: bytes) -> str:
    import hashlib

    return hashlib.sha256(payload).hexdigest()
