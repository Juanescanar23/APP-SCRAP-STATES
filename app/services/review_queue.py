from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import ReviewQueueItem, ReviewQueueKind, ReviewQueueStatus


@dataclass(slots=True)
class ReviewQueueRequest:
    entity_id: uuid.UUID
    queue_kind: ReviewQueueKind
    reason: str
    payload: dict[str, Any]
    domain_id: uuid.UUID | None = None
    notes: str | None = None


def build_review_fingerprint(request: ReviewQueueRequest) -> str:
    body = {
        "entity_id": str(request.entity_id),
        "domain_id": str(request.domain_id) if request.domain_id else None,
        "queue_kind": request.queue_kind.value,
        "reason": request.reason,
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()


def enqueue_review_item(session: Session, request: ReviewQueueRequest) -> None:
    stmt = insert(ReviewQueueItem).values(
        entity_id=request.entity_id,
        domain_id=request.domain_id,
        queue_kind=request.queue_kind,
        reason=request.reason,
        status=ReviewQueueStatus.pending,
        fingerprint=build_review_fingerprint(request),
        payload=request.payload,
        notes=request.notes,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["fingerprint"],
        set_={
            "status": ReviewQueueStatus.pending,
            "payload": stmt.excluded.payload,
            "notes": stmt.excluded.notes,
            "updated_at": datetime.now(timezone.utc),
            "resolved_at": None,
        },
    )
    session.execute(stmt)

