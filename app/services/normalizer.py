from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from app.db.models import EntityStatus


LEGAL_SUFFIXES = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "llc",
    "lp",
    "ltd",
    "limited",
    "pllc",
}
PUNCT_RE = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class NormalizedEntityRecord:
    state: str
    external_filing_id: str
    legal_name: str
    normalized_name: str
    status: EntityStatus
    formed_at: date | None
    registry_payload: dict[str, Any]


def normalize_company_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = PUNCT_RE.sub(" ", normalized.casefold())
    tokens = [token for token in normalized.split() if token and token not in LEGAL_SUFFIXES]
    return " ".join(tokens).strip()


def coerce_entity_status(raw_status: str | None) -> EntityStatus:
    if not raw_status:
        return EntityStatus.unknown

    normalized = raw_status.strip().casefold()
    if normalized in {"active", "current", "registered", "in existence"}:
        return EntityStatus.active
    if normalized in {"inactive", "dissolved", "terminated", "withdrawn"}:
        return EntityStatus.inactive
    return EntityStatus.unknown


def parse_date(raw_value: Any) -> date | None:
    if raw_value in {None, ""}:
        return None

    if isinstance(raw_value, date) and not isinstance(raw_value, datetime):
        return raw_value

    if isinstance(raw_value, datetime):
        return raw_value.date()

    if isinstance(raw_value, str):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(raw_value, fmt).date()
            except ValueError:
                continue

    return None


def normalize_stage_payload(state: str, payload: dict[str, Any]) -> NormalizedEntityRecord:
    legal_name = str(payload.get("legal_name") or payload.get("company_name") or "").strip()
    external_filing_id = str(
        payload.get("external_filing_id")
        or payload.get("filing_number")
        or payload.get("document_number")
        or ""
    ).strip()

    if not legal_name or not external_filing_id:
        raise ValueError("Stage payload must include legal_name and external_filing_id.")

    return NormalizedEntityRecord(
        state=state.upper(),
        external_filing_id=external_filing_id,
        legal_name=legal_name,
        normalized_name=normalize_company_name(legal_name),
        status=coerce_entity_status(payload.get("status")),
        formed_at=parse_date(payload.get("formed_at") or payload.get("filing_date")),
        registry_payload=payload,
    )

