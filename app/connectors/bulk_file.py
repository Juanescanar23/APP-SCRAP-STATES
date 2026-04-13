from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from app.connectors.base import BaseConnector, ConnectorBatch, RawConnectorRecord, checksum_payload


class BulkFileConnector(BaseConnector):
    connector_kind = "bulk_file"

    def load(self, source_path: Path) -> ConnectorBatch:
        suffix = source_path.suffix.lower()
        raw_bytes = source_path.read_bytes()
        source_checksum = checksum_payload(raw_bytes)

        if suffix == ".csv":
            records = self._load_csv(source_path)
        elif suffix in {".jsonl", ".ndjson"}:
            records = self._load_jsonl(source_path)
        elif suffix == ".json":
            records = self._load_json(source_path)
        else:
            raise ValueError(f"Unsupported bulk file format: {source_path.suffix}")

        return ConnectorBatch(
            source_uri=str(source_path),
            source_checksum=source_checksum,
            records=records,
        )

    def _load_csv(self, source_path: Path) -> list[RawConnectorRecord]:
        with source_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            return [self._record_from_payload(row) for row in reader]

    def _load_jsonl(self, source_path: Path) -> list[RawConnectorRecord]:
        records: list[RawConnectorRecord] = []
        with source_path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                records.append(self._record_from_payload(json.loads(line)))
        return records

    def _load_json(self, source_path: Path) -> list[RawConnectorRecord]:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = [payload]
        return [self._record_from_payload(item) for item in payload]

    def _record_from_payload(self, payload: dict[str, Any]) -> RawConnectorRecord:
        external_filing_id = (
            payload.get("external_filing_id")
            or payload.get("filing_number")
            or payload.get("document_number")
        )
        return RawConnectorRecord(
            external_filing_id=str(external_filing_id) if external_filing_id else None,
            payload=payload,
            checksum=checksum_payload(payload),
        )

