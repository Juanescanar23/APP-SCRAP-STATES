from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def checksum_payload(payload: Any) -> str:
    if isinstance(payload, bytes):
        body = payload
    else:
        body = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


@dataclass(slots=True)
class RawConnectorRecord:
    external_filing_id: str | None
    payload: dict[str, Any]
    checksum: str


@dataclass(slots=True)
class ConnectorBatch:
    source_uri: str
    source_checksum: str
    records: list[RawConnectorRecord]


class BaseConnector(ABC):
    connector_kind: str
    state: str

    def __init__(self, state: str) -> None:
        self.state = state.upper()

    @abstractmethod
    def load(self, source_path: Path) -> ConnectorBatch:
        raise NotImplementedError
