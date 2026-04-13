from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.db.models import ContactKind, ReviewQueueKind, ReviewQueueStatus, ReviewStatus


class ContactEvidenceRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    entity_id: uuid.UUID
    domain_id: uuid.UUID | None
    kind: ContactKind
    value: str
    source_url: str
    source_hash: str
    confidence: float
    review_status: ReviewStatus
    notes: str | None
    observed_at: datetime


class EvidenceReviewUpdate(BaseModel):
    review_status: ReviewStatus
    notes: str | None = None


class ReviewQueueItemRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    entity_id: uuid.UUID
    domain_id: uuid.UUID | None
    queue_kind: ReviewQueueKind
    reason: str
    status: ReviewQueueStatus
    fingerprint: str
    payload: dict
    notes: str | None
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None


class ReviewQueueUpdate(BaseModel):
    status: ReviewQueueStatus
    notes: str | None = None
