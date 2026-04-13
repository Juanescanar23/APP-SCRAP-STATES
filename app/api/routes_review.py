from __future__ import annotations

import uuid

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from app.api.deps import DBSession
from app.db.models import ContactEvidence, ReviewQueueItem, ReviewQueueStatus, ReviewStatus
from app.schemas.evidence import (
    ContactEvidenceRead,
    EvidenceReviewUpdate,
    ReviewQueueItemRead,
    ReviewQueueUpdate,
)


router = APIRouter(prefix="/review", tags=["review"])


@router.get("/evidence", response_model=list[ContactEvidenceRead])
def list_evidence_for_review(
    session: DBSession,
    status: ReviewStatus = Query(default=ReviewStatus.pending),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[ContactEvidenceRead]:
    rows = session.scalars(
        select(ContactEvidence)
        .where(ContactEvidence.review_status == status)
        .order_by(ContactEvidence.observed_at.desc())
        .limit(limit),
    ).all()
    return [ContactEvidenceRead.model_validate(row) for row in rows]


@router.patch("/evidence/{evidence_id}", response_model=ContactEvidenceRead)
def review_evidence(
    evidence_id: uuid.UUID,
    payload: EvidenceReviewUpdate,
    session: DBSession,
) -> ContactEvidenceRead:
    evidence = session.get(ContactEvidence, evidence_id)
    if evidence is None:
        raise HTTPException(status_code=404, detail="Evidence not found.")

    evidence.review_status = payload.review_status
    evidence.notes = payload.notes
    session.add(evidence)
    session.commit()
    session.refresh(evidence)
    return ContactEvidenceRead.model_validate(evidence)


@router.get("/items", response_model=list[ReviewQueueItemRead])
def list_review_items(
    session: DBSession,
    status: ReviewQueueStatus = Query(default=ReviewQueueStatus.pending),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[ReviewQueueItemRead]:
    rows = session.scalars(
        select(ReviewQueueItem)
        .where(ReviewQueueItem.status == status)
        .order_by(ReviewQueueItem.updated_at.desc())
        .limit(limit),
    ).all()
    return [ReviewQueueItemRead.model_validate(row) for row in rows]


@router.patch("/items/{item_id}", response_model=ReviewQueueItemRead)
def update_review_item(
    item_id: uuid.UUID,
    payload: ReviewQueueUpdate,
    session: DBSession,
) -> ReviewQueueItemRead:
    item = session.get(ReviewQueueItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Review item not found.")

    item.status = payload.status
    item.notes = payload.notes
    item.resolved_at = datetime.now(timezone.utc) if payload.status != ReviewQueueStatus.pending else None
    session.add(item)
    session.commit()
    session.refresh(item)
    return ReviewQueueItemRead.model_validate(item)
