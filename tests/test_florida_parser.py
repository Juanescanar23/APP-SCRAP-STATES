from __future__ import annotations

from datetime import date
from pathlib import Path
from zipfile import ZipFile

from app.connectors.florida.mapper import build_company_event, build_registry_snapshot
from app.connectors.florida.parser import (
    CORPORATE_EVENT_RECORD_LENGTH,
    CORPORATE_RECORD_LENGTH,
    inspect_source_file,
    iter_source_records,
)
from app.db.models import EntityStatus, SourceFileKind, SourceRecordParseStatus


def test_parse_quarterly_corporate_zip(tmp_path: Path) -> None:
    corporate_line = build_fixed_width_line(
        CORPORATE_RECORD_LENGTH,
        {
            (1, 12): "P24000012345",
            (13, 192): "Sunrise Health LLC",
            (205, 1): "A",
            (206, 15): "FLAL",
            (221, 42): "123 Ocean Dr",
            (305, 28): "Miami",
            (333, 2): "FL",
            (335, 10): "33101",
            (347, 42): "PO Box 99",
            (431, 28): "Miami",
            (459, 2): "FL",
            (473, 8): "20240115",
            (481, 14): "12345678901234",
            (496, 8): "20250401",
            (506, 4): "2024",
            (511, 8): "20240418",
            (545, 42): "Jane Agent",
            (587, 1): "P",
            (588, 42): "456 Bay Rd",
            (630, 28): "Miami",
            (658, 2): "FL",
            (660, 9): "331010000",
            (669, 4): "MGR",
            (673, 1): "P",
            (674, 42): "John Doe",
            (716, 42): "123 Ocean Dr",
            (758, 28): "Miami",
            (786, 2): "FL",
            (788, 9): "331010000",
        },
    )
    source_path = tmp_path / "cordata.zip"
    with ZipFile(source_path, "w") as archive:
        archive.writestr("cordata.txt", f"{corporate_line}\n")

    details = inspect_source_file(source_path)
    assert details.source_kind == SourceFileKind.quarterly_corporate
    assert details.record_length == CORPORATE_RECORD_LENGTH
    assert details.archive_members == ["cordata.txt"]

    records = list(iter_source_records(source_path))
    assert len(records) == 1
    assert records[0].parse_status == SourceRecordParseStatus.parsed
    assert records[0].external_filing_id == "P24000012345"
    assert records[0].payload["status"] == "active"
    assert records[0].payload["principal_city"] == "Miami"
    assert records[0].payload["officers"][0]["name"] == "John Doe"

    snapshot = build_registry_snapshot(records[0].payload)
    assert snapshot.status == EntityStatus.active
    assert snapshot.latest_report_year == 2024
    assert snapshot.latest_report_date == date(2024, 4, 18)
    assert snapshot.registry_payload["city"] == "Miami"
    assert snapshot.registry_payload["registered_agent_name"] == "Jane Agent"


def test_parse_quarterly_corporate_zip_can_select_shard(tmp_path: Path) -> None:
    line_zero = build_fixed_width_line(
        CORPORATE_RECORD_LENGTH,
        {
            (1, 12): "P24000000000",
            (13, 192): "Zero Holdings LLC",
            (205, 1): "A",
        },
    )
    line_one = build_fixed_width_line(
        CORPORATE_RECORD_LENGTH,
        {
            (1, 12): "P24000000001",
            (13, 192): "One Holdings LLC",
            (205, 1): "A",
        },
    )
    source_path = tmp_path / "cordata.zip"
    with ZipFile(source_path, "w") as archive:
        archive.writestr("cordata_0.txt", f"{line_zero}\n")
        archive.writestr("cordata_1.txt", f"{line_one}\n")

    records = list(iter_source_records(source_path, quarterly_shard=1))
    assert len(records) == 1
    assert records[0].external_filing_id == "P24000000001"


def test_parse_daily_event_text_file(tmp_path: Path) -> None:
    event_line = build_fixed_width_line(
        CORPORATE_EVENT_RECORD_LENGTH,
        {
            (1, 12): "P24000012345",
            (13, 5): "00001",
            (18, 20): "NAME_CHANGE",
            (38, 40): "Name change accepted",
            (78, 8): "20260410",
            (86, 8): "20260411",
            (94, 35): "Primary note",
            (211, 192): "Sunrise Health LLC",
            (415, 42): "123 Ocean Dr",
            (499, 28): "Miami",
            (527, 2): "FL",
            (529, 10): "33101",
            (539, 42): "PO Box 99",
            (623, 28): "Miami",
            (651, 2): "FL",
            (653, 10): "33101",
        },
    )
    source_path = tmp_path / "20260410ce.txt"
    source_path.write_text(f"{event_line}\n", encoding="ascii")

    details = inspect_source_file(source_path)
    assert details.source_kind == SourceFileKind.daily_corporate_events
    assert details.file_date == date(2026, 4, 10)

    records = list(iter_source_records(source_path))
    assert len(records) == 1
    assert records[0].parse_status == SourceRecordParseStatus.parsed
    assert records[0].payload["event_description"] == "Name change accepted"

    event_record = build_company_event(records[0].payload)
    assert event_record.effective_date == date(2026, 4, 10)
    assert event_record.filed_date == date(2026, 4, 11)
    assert event_record.payload["principal_city"] == "Miami"


def build_fixed_width_line(length: int, fields: dict[tuple[int, int], str]) -> str:
    chars = [" "] * length
    for (start, field_length), value in fields.items():
        normalized = value[:field_length].ljust(field_length)
        chars[start - 1 : start - 1 + field_length] = list(normalized)
    return "".join(chars)
