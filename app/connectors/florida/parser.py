from __future__ import annotations

import hashlib
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO

from app.connectors.base import checksum_payload
from app.db.models import SourceFileKind, SourceRecordParseStatus

PARSER_VERSION = "fl-sunbiz-fixed-width-v1"
CORPORATE_RECORD_LENGTH = 1440
CORPORATE_EVENT_RECORD_LENGTH = 662
DAILY_CORPORATE_RE = re.compile(r"(?P<yyyymmdd>\d{8})c\.txt$", re.IGNORECASE)
DAILY_CORPORATE_EVENTS_RE = re.compile(r"(?P<yyyymmdd>\d{8})ce\.txt$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class FixedWidthField:
    name: str
    start: int
    length: int


@dataclass(slots=True)
class FloridaSourceFileDetails:
    provider: str
    state: str
    source_kind: SourceFileKind
    filename: str
    source_uri: str
    bucket_key: str | None
    source_checksum: str
    record_length: int
    file_date: date | None
    is_delta: bool
    archive_members: list[str]
    parser_version: str = PARSER_VERSION


@dataclass(slots=True)
class FloridaParsedRecord:
    record_no: int
    byte_offset: int
    raw_hash: str
    external_filing_id: str | None
    parse_status: SourceRecordParseStatus
    error_code: str | None
    payload: dict[str, Any]


CORPORATE_FIELDS: tuple[FixedWidthField, ...] = (
    FixedWidthField("document_number", 1, 12),
    FixedWidthField("company_name", 13, 192),
    FixedWidthField("status_code", 205, 1),
    FixedWidthField("filing_type", 206, 15),
    FixedWidthField("principal_address_1", 221, 42),
    FixedWidthField("principal_address_2", 263, 42),
    FixedWidthField("principal_city", 305, 28),
    FixedWidthField("principal_state", 333, 2),
    FixedWidthField("principal_zip", 335, 10),
    FixedWidthField("principal_country", 345, 2),
    FixedWidthField("mail_address_1", 347, 42),
    FixedWidthField("mail_address_2", 389, 42),
    FixedWidthField("mail_city", 431, 28),
    FixedWidthField("mail_state", 459, 2),
    FixedWidthField("mail_zip", 461, 10),
    FixedWidthField("mail_country", 471, 2),
    FixedWidthField("filing_date", 473, 8),
    FixedWidthField("fei_number", 481, 14),
    FixedWidthField("more_than_six_officers_flag", 495, 1),
    FixedWidthField("last_transaction_date", 496, 8),
    FixedWidthField("state_country", 504, 2),
    FixedWidthField("report_year_1", 506, 4),
    FixedWidthField("report_date_1", 511, 8),
    FixedWidthField("report_year_2", 519, 4),
    FixedWidthField("report_date_2", 524, 8),
    FixedWidthField("report_year_3", 532, 4),
    FixedWidthField("report_date_3", 537, 8),
    FixedWidthField("registered_agent_name", 545, 42),
    FixedWidthField("registered_agent_type", 587, 1),
    FixedWidthField("registered_agent_address", 588, 42),
    FixedWidthField("registered_agent_city", 630, 28),
    FixedWidthField("registered_agent_state", 658, 2),
    FixedWidthField("registered_agent_zip", 660, 9),
)

EVENT_FIELDS: tuple[FixedWidthField, ...] = (
    FixedWidthField("document_number", 1, 12),
    FixedWidthField("event_sequence", 13, 5),
    FixedWidthField("event_code", 18, 20),
    FixedWidthField("event_description", 38, 40),
    FixedWidthField("effective_date", 78, 8),
    FixedWidthField("filed_date", 86, 8),
    FixedWidthField("event_note_1", 94, 35),
    FixedWidthField("event_note_2", 129, 35),
    FixedWidthField("event_note_3", 164, 35),
    FixedWidthField("conversion_merger_number", 199, 12),
    FixedWidthField("company_name", 211, 192),
    FixedWidthField("event_name_sequence", 403, 5),
    FixedWidthField("event_cross_name_sequence", 408, 5),
    FixedWidthField("event_name_changed", 413, 1),
    FixedWidthField("event_cross_name_changed", 414, 1),
    FixedWidthField("principal_address_1", 415, 42),
    FixedWidthField("principal_address_2", 457, 42),
    FixedWidthField("principal_city", 499, 28),
    FixedWidthField("principal_state", 527, 2),
    FixedWidthField("principal_zip", 529, 10),
    FixedWidthField("mail_address_1", 539, 42),
    FixedWidthField("mail_address_2", 581, 42),
    FixedWidthField("mail_city", 623, 28),
    FixedWidthField("mail_state", 651, 2),
    FixedWidthField("mail_zip", 653, 10),
)


def inspect_source_file(source_path: Path) -> FloridaSourceFileDetails:
    source_kind = infer_source_kind(source_path)
    checksum = checksum_file(source_path)
    archive_members = (
        list_archive_members(source_path) if source_path.suffix.lower() == ".zip" else []
    )
    return FloridaSourceFileDetails(
        provider="sunbiz",
        state="FL",
        source_kind=source_kind,
        filename=source_path.name,
        source_uri=str(source_path),
        bucket_key=derive_bucket_key(source_path),
        source_checksum=checksum,
        record_length=record_length_for_kind(source_kind),
        file_date=infer_file_date(source_path.name),
        is_delta=source_kind
        in {SourceFileKind.daily_corporate, SourceFileKind.daily_corporate_events},
        archive_members=archive_members,
    )


def infer_source_kind(source_path: Path) -> SourceFileKind:
    normalized = source_path.as_posix().casefold()
    filename = source_path.name.casefold()

    if "corevent" in filename or DAILY_CORPORATE_EVENTS_RE.search(filename):
        return (
            SourceFileKind.quarterly_corporate_events
            if "quarterly" in normalized or "corevent" in filename
            else SourceFileKind.daily_corporate_events
        )
    if "cordata" in filename or DAILY_CORPORATE_RE.search(filename):
        return (
            SourceFileKind.quarterly_corporate
            if "quarterly" in normalized or "cordata" in filename
            else SourceFileKind.daily_corporate
        )
    raise ValueError(f"Unsupported Florida source file: {source_path.name}")


def infer_file_date(filename: str) -> date | None:
    for pattern in (DAILY_CORPORATE_RE, DAILY_CORPORATE_EVENTS_RE):
        match = pattern.search(filename)
        if match:
            return datetime.strptime(match.group("yyyymmdd"), "%Y%m%d").date()
    return None


def record_length_for_kind(source_kind: SourceFileKind) -> int:
    if source_kind in {SourceFileKind.quarterly_corporate, SourceFileKind.daily_corporate}:
        return CORPORATE_RECORD_LENGTH
    return CORPORATE_EVENT_RECORD_LENGTH


def iter_source_records(
    source_path: Path,
    *,
    quarterly_shard: int | None = None,
) -> Iterator[FloridaParsedRecord]:
    source_kind = infer_source_kind(source_path)
    expected_length = record_length_for_kind(source_kind)
    parser = (
        parse_corporate_record
        if source_kind
        in {
            SourceFileKind.quarterly_corporate,
            SourceFileKind.daily_corporate,
        }
        else parse_event_record
    )

    offset = 0
    record_no = 0
    for member_name, handle in iter_binary_members(source_path, quarterly_shard=quarterly_shard):
        del member_name
        for raw_line in handle:
            line = raw_line.rstrip(b"\r\n")
            current_offset = offset
            offset += len(raw_line)
            if not line:
                continue

            record_no += 1
            raw_hash = checksum_payload(line)
            if len(line) != expected_length:
                yield FloridaParsedRecord(
                    record_no=record_no,
                    byte_offset=current_offset,
                    raw_hash=raw_hash,
                    external_filing_id=None,
                    parse_status=SourceRecordParseStatus.failed,
                    error_code="invalid_record_length",
                    payload={},
                )
                continue

            payload = parser(line.decode("ascii", errors="ignore"))
            yield FloridaParsedRecord(
                record_no=record_no,
                byte_offset=current_offset,
                raw_hash=raw_hash,
                external_filing_id=str(payload.get("document_number") or "").strip() or None,
                parse_status=SourceRecordParseStatus.parsed,
                error_code=None,
                payload=payload,
            )


def parse_corporate_record(line: str) -> dict[str, Any]:
    payload = {field.name: extract_field(line, field) for field in CORPORATE_FIELDS}
    payload["status"] = _status_from_code(payload.get("status_code"))
    payload["reports"] = [
        {
            "report_year": payload.get(f"report_year_{index}") or None,
            "report_date": compact_date_string(payload.get(f"report_date_{index}")),
        }
        for index in range(1, 4)
        if payload.get(f"report_year_{index}") or payload.get(f"report_date_{index}")
    ]
    payload["latest_report_year"] = None
    payload["latest_report_date"] = None
    for report in payload["reports"]:
        year = safe_int(report.get("report_year"))
        if year is None:
            continue
        if payload["latest_report_year"] is None or year >= payload["latest_report_year"]:
            payload["latest_report_year"] = year
            payload["latest_report_date"] = report.get("report_date")
    payload["officers"] = parse_officers(line)
    payload["more_than_six_officers"] = payload.get("more_than_six_officers_flag") == "Y"
    return payload


def parse_event_record(line: str) -> dict[str, Any]:
    payload = {field.name: extract_field(line, field) for field in EVENT_FIELDS}
    payload["effective_date"] = compact_date_string(payload.get("effective_date"))
    payload["filed_date"] = compact_date_string(payload.get("filed_date"))
    payload["event_name_changed"] = payload.get("event_name_changed") == "Y"
    payload["event_cross_name_changed"] = payload.get("event_cross_name_changed") == "Y"
    return payload


def parse_officers(line: str) -> list[dict[str, str]]:
    officers: list[dict[str, str]] = []
    for index in range(6):
        start = 669 + (index * 128)
        officer = {
            "title": extract_slice(line, start, 4),
            "entity_type": extract_slice(line, start + 4, 1),
            "name": extract_slice(line, start + 5, 42),
            "address": extract_slice(line, start + 47, 42),
            "city": extract_slice(line, start + 89, 28),
            "state": extract_slice(line, start + 117, 2),
            "zip": extract_slice(line, start + 119, 9),
        }
        if officer["name"]:
            officers.append(officer)
    return officers


def extract_field(line: str, field: FixedWidthField) -> str:
    return extract_slice(line, field.start, field.length)


def extract_slice(line: str, start: int, length: int) -> str:
    return line[start - 1 : start - 1 + length].strip()


def compact_date_string(raw_value: str | None) -> str | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    if len(value) == 8 and value.isdigit():
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    return value


def safe_int(raw_value: str | None) -> int | None:
    value = (raw_value or "").strip()
    return int(value) if value.isdigit() else None


def checksum_file(source_path: Path) -> str:
    digest = hashlib.sha256()
    with source_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def list_archive_members(source_path: Path) -> list[str]:
    with zipfile.ZipFile(source_path) as archive:
        return [name for name in archive.namelist() if not name.endswith("/")]


def list_archive_members_from_bytes(raw_bytes: bytes) -> list[str]:
    with zipfile.ZipFile(BytesIO(raw_bytes)) as archive:
        return [name for name in archive.namelist() if not name.endswith("/")]


def derive_bucket_key(source_path: Path) -> str | None:
    try:
        return source_path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return f"raw/fl/{source_path.name}"


def _status_from_code(code: str | None) -> str:
    normalized = (code or "").strip().upper()
    if normalized == "A":
        return "active"
    if normalized == "I":
        return "inactive"
    return ""


def select_archive_members(members: list[str], quarterly_shard: int | None) -> list[str]:
    if quarterly_shard is None:
        return members
    shard_token = str(quarterly_shard)
    selected = [
        name
        for name in members
        if Path(name).stem.endswith(shard_token) or f"_{shard_token}" in Path(name).stem
    ]
    return selected or members


def iter_binary_members(
    source_path: Path,
    *,
    quarterly_shard: int | None = None,
) -> Iterator[tuple[str, BinaryIO]]:
    suffix = source_path.suffix.lower()
    if suffix != ".zip":
        with source_path.open("rb") as handle:
            yield source_path.name, handle
        return

    with zipfile.ZipFile(source_path) as archive:
        members = select_archive_members(
            [name for name in archive.namelist() if not name.endswith("/")],
            quarterly_shard,
        )
        if not members:
            raise ValueError(f"Florida archive is empty: {source_path}")
        for member_name in members:
            with archive.open(member_name, "r") as handle:
                yield member_name, handle
