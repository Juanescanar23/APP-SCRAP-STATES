from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import httpx

from app.connectors.base import BaseConnector, ConnectorBatch, RawConnectorRecord, checksum_payload


class HTMLSearchConnector(BaseConnector):
    connector_kind = "html_search"

    @abstractmethod
    async def search(self, query: str) -> Iterable[dict[str, Any]]:
        raise NotImplementedError

    async def load_from_queries(self, queries: Iterable[str]) -> ConnectorBatch:
        records: list[RawConnectorRecord] = []
        for query in queries:
            for payload in await self.search(query):
                records.append(
                    RawConnectorRecord(
                        external_filing_id=str(payload.get("external_filing_id") or ""),
                        payload=payload,
                        checksum=checksum_payload(payload),
                    ),
                )

        return ConnectorBatch(
            source_uri=f"html_search:{self.state}",
            source_checksum=checksum_payload([record.checksum for record in records]),
            records=records,
        )

    def load(self, source_path: Path) -> ConnectorBatch:
        raise NotImplementedError("HTMLSearchConnector uses load_from_queries().")

    async def fetch_html(self, url: str) -> str:
        async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

