from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import Any

import httpx

from app.connectors.base import BaseConnector, ConnectorBatch, RawConnectorRecord, checksum_payload


class APIConnector(BaseConnector):
    connector_kind = "api"

    @abstractmethod
    async def fetch_records(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def load_from_api(self) -> ConnectorBatch:
        payload = await self.fetch_records()
        records = [
            RawConnectorRecord(
                external_filing_id=str(item.get("external_filing_id") or ""),
                payload=item,
                checksum=checksum_payload(item),
            )
            for item in payload
        ]
        return ConnectorBatch(
            source_uri=f"api:{self.state}",
            source_checksum=checksum_payload([record.checksum for record in records]),
            records=records,
        )

    def load(self, source_path: Path) -> ConnectorBatch:
        raise NotImplementedError("APIConnector uses load_from_api().")

    async def get_json(self, url: str, **kwargs: Any) -> Any:
        async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
            response = await client.get(url, **kwargs)
            response.raise_for_status()
            return response.json()

