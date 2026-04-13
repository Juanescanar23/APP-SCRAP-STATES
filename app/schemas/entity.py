from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.db.models import DomainStatus, EntityStatus


class OfficialDomainRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    domain: str
    homepage_url: str
    status: DomainStatus
    confidence: float
    created_at: datetime
    last_checked_at: datetime | None = None


class EntityListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    state: str
    external_filing_id: str
    legal_name: str
    normalized_name: str
    status: EntityStatus
    formed_at: date | None
    first_seen_at: datetime
    last_seen_at: datetime


class EntityDetail(EntityListItem):
    registry_payload: dict[str, Any]
    domains: list[OfficialDomainRead] = Field(default_factory=list)
