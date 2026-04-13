from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from app.db.models import EntityStatus
from app.services.normalizer import coerce_entity_status, normalize_company_name, parse_date


@dataclass(slots=True)
class FloridaRegistrySnapshotRecord:
    state: str
    external_filing_id: str
    legal_name: str
    normalized_name: str
    status: EntityStatus
    filing_type: str | None
    formed_at: date | None
    last_transaction_date: date | None
    latest_report_year: int | None
    latest_report_date: date | None
    fei_number: str | None
    principal_address: dict[str, str]
    mailing_address: dict[str, str]
    registered_agent: dict[str, str]
    officers: list[dict[str, str]]
    registry_payload: dict[str, Any]


@dataclass(slots=True)
class FloridaCompanyEventRecord:
    state: str
    external_filing_id: str
    legal_name: str
    event_code: str
    event_description: str
    effective_date: date | None
    filed_date: date | None
    payload: dict[str, Any]


def build_registry_snapshot(payload: dict[str, Any]) -> FloridaRegistrySnapshotRecord:
    legal_name = clean_text(payload.get("company_name"))
    external_filing_id = clean_text(payload.get("document_number"))
    if not legal_name or not external_filing_id:
        raise ValueError("Florida corporate record requires company_name and document_number.")

    principal_address = compact_dict(
        {
            "address_line1": clean_text(payload.get("principal_address_1")),
            "address_line2": clean_text(payload.get("principal_address_2")),
            "city": clean_text(payload.get("principal_city")),
            "state": clean_text(payload.get("principal_state")),
            "postal_code": clean_text(payload.get("principal_zip")),
            "country": clean_text(payload.get("principal_country")),
        },
    )
    mailing_address = compact_dict(
        {
            "address_line1": clean_text(payload.get("mail_address_1")),
            "address_line2": clean_text(payload.get("mail_address_2")),
            "city": clean_text(payload.get("mail_city")),
            "state": clean_text(payload.get("mail_state")),
            "postal_code": clean_text(payload.get("mail_zip")),
            "country": clean_text(payload.get("mail_country")),
        },
    )
    registered_agent = compact_dict(
        {
            "name": clean_text(payload.get("registered_agent_name")),
            "entity_type": clean_text(payload.get("registered_agent_type")),
            "address_line1": clean_text(payload.get("registered_agent_address")),
            "city": clean_text(payload.get("registered_agent_city")),
            "state": clean_text(payload.get("registered_agent_state")),
            "postal_code": clean_text(payload.get("registered_agent_zip")),
        },
    )
    officers = [
        compact_dict(
            {
                "title": clean_text(officer.get("title")),
                "entity_type": clean_text(officer.get("entity_type")),
                "name": clean_text(officer.get("name")),
                "address_line1": clean_text(officer.get("address")),
                "city": clean_text(officer.get("city")),
                "state": clean_text(officer.get("state")),
                "postal_code": clean_text(officer.get("zip")),
            },
        )
        for officer in payload.get("officers", [])
        if clean_text(officer.get("name"))
    ]

    latest_report_year = payload.get("latest_report_year")
    if not isinstance(latest_report_year, int):
        latest_report_year = None

    registry_payload = compact_dict(
        {
            "state": "FL",
            "state_name": principal_address.get("state") or "FL",
            "address_line1": principal_address.get("address_line1"),
            "address_line2": principal_address.get("address_line2"),
            "city": principal_address.get("city"),
            "postal_code": principal_address.get("postal_code"),
            "mail_address_1": mailing_address.get("address_line1"),
            "mail_address_2": mailing_address.get("address_line2"),
            "mail_city": mailing_address.get("city"),
            "mail_state": mailing_address.get("state"),
            "mail_zip": mailing_address.get("postal_code"),
            "registered_agent_name": registered_agent.get("name"),
            "registered_agent_address": registered_agent.get("address_line1"),
            "registered_agent_city": registered_agent.get("city"),
            "registered_agent_state": registered_agent.get("state"),
            "registered_agent_zip": registered_agent.get("postal_code"),
            "filing_type": clean_text(payload.get("filing_type")),
            "fei_number": clean_text(payload.get("fei_number")),
            "status_code": clean_text(payload.get("status_code")),
            "status": clean_text(payload.get("status")),
            "formed_at": iso_date(parse_date(payload.get("filing_date"))),
            "last_transaction_date": iso_date(parse_date(payload.get("last_transaction_date"))),
            "latest_report_year": latest_report_year,
            "latest_report_date": iso_date(parse_date(payload.get("latest_report_date"))),
            "officers": officers,
            "more_than_six_officers": bool(payload.get("more_than_six_officers")),
        },
    )

    return FloridaRegistrySnapshotRecord(
        state="FL",
        external_filing_id=external_filing_id,
        legal_name=legal_name,
        normalized_name=normalize_company_name(legal_name),
        status=coerce_entity_status(clean_text(payload.get("status"))),
        filing_type=clean_text(payload.get("filing_type")) or None,
        formed_at=parse_date(payload.get("filing_date")),
        last_transaction_date=parse_date(payload.get("last_transaction_date")),
        latest_report_year=latest_report_year,
        latest_report_date=parse_date(payload.get("latest_report_date")),
        fei_number=clean_text(payload.get("fei_number")) or None,
        principal_address=principal_address,
        mailing_address=mailing_address,
        registered_agent=registered_agent,
        officers=officers,
        registry_payload=registry_payload,
    )


def build_company_event(payload: dict[str, Any]) -> FloridaCompanyEventRecord:
    legal_name = clean_text(payload.get("company_name"))
    external_filing_id = clean_text(payload.get("document_number"))
    if not legal_name or not external_filing_id:
        raise ValueError("Florida event record requires company_name and document_number.")

    normalized_payload = compact_dict(
        {
            "document_number": external_filing_id,
            "company_name": legal_name,
            "event_sequence": clean_text(payload.get("event_sequence")),
            "event_code": clean_text(payload.get("event_code")),
            "event_description": clean_text(payload.get("event_description")),
            "effective_date": iso_date(parse_date(payload.get("effective_date"))),
            "filed_date": iso_date(parse_date(payload.get("filed_date"))),
            "principal_address_1": clean_text(payload.get("principal_address_1")),
            "principal_address_2": clean_text(payload.get("principal_address_2")),
            "principal_city": clean_text(payload.get("principal_city")),
            "principal_state": clean_text(payload.get("principal_state")),
            "principal_zip": clean_text(payload.get("principal_zip")),
            "mail_address_1": clean_text(payload.get("mail_address_1")),
            "mail_address_2": clean_text(payload.get("mail_address_2")),
            "mail_city": clean_text(payload.get("mail_city")),
            "mail_state": clean_text(payload.get("mail_state")),
            "mail_zip": clean_text(payload.get("mail_zip")),
            "event_note_1": clean_text(payload.get("event_note_1")),
            "event_note_2": clean_text(payload.get("event_note_2")),
            "event_note_3": clean_text(payload.get("event_note_3")),
        },
    )

    return FloridaCompanyEventRecord(
        state="FL",
        external_filing_id=external_filing_id,
        legal_name=legal_name,
        event_code=clean_text(payload.get("event_code")),
        event_description=clean_text(payload.get("event_description")),
        effective_date=parse_date(payload.get("effective_date")),
        filed_date=parse_date(payload.get("filed_date")),
        payload=normalized_payload,
    )


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def compact_dict(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if value is not None and value != "" and value != [] and value != {}
    }


def iso_date(value: date | None) -> str | None:
    return value.isoformat() if value else None
