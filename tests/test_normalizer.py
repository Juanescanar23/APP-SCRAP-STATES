from __future__ import annotations

from datetime import date

from app.db.models import EntityStatus
from app.services.normalizer import normalize_company_name, normalize_stage_payload


def test_normalize_company_name_removes_common_suffixes() -> None:
    assert normalize_company_name("Acme Holdings LLC") == "acme holdings"


def test_normalize_stage_payload_maps_expected_fields() -> None:
    record = normalize_stage_payload(
        "fl",
        {
            "external_filing_id": "P24000012345",
            "legal_name": "Sunrise Labs Inc.",
            "status": "active",
            "formed_at": "2024-01-08",
        },
    )

    assert record.state == "FL"
    assert record.status is EntityStatus.active
    assert record.formed_at == date(2024, 1, 8)
    assert record.normalized_name == "sunrise labs"

