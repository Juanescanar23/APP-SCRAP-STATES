from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import or_, select

from app.api.deps import DBSession
from app.core.config import get_settings
from app.db.models import BusinessEntity, EntityStatus, OfficialDomain
from app.schemas.entity import EntityDetail, EntityListItem, OfficialDomainRead
from app.services.normalizer import normalize_company_name


router = APIRouter(prefix="/entities", tags=["entities"])


@router.get("", response_model=list[EntityListItem])
def list_entities(
    session: DBSession,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    status: EntityStatus | None = None,
    q: str | None = None,
    limit: int | None = Query(default=None, ge=1),
    offset: int = Query(default=0, ge=0),
) -> list[EntityListItem]:
    settings = get_settings()
    page_size = min(limit or settings.page_size_default, settings.page_size_max)

    stmt = select(BusinessEntity).order_by(BusinessEntity.legal_name.asc())
    if state:
        stmt = stmt.where(BusinessEntity.state == state.upper())
    if status:
        stmt = stmt.where(BusinessEntity.status == status)
    if q:
        normalized_query = normalize_company_name(q)
        stmt = stmt.where(
            or_(
                BusinessEntity.legal_name.ilike(f"%{q}%"),
                BusinessEntity.normalized_name.ilike(f"%{normalized_query}%"),
            ),
        )

    entities = session.scalars(stmt.limit(page_size).offset(offset)).all()
    return [EntityListItem.model_validate(entity) for entity in entities]


@router.get("/{entity_id}", response_model=EntityDetail)
def get_entity(entity_id: uuid.UUID, session: DBSession) -> EntityDetail:
    entity = session.get(BusinessEntity, entity_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Entity not found.")

    domains = session.scalars(
        select(OfficialDomain)
        .where(OfficialDomain.entity_id == entity.id)
        .order_by(OfficialDomain.confidence.desc(), OfficialDomain.created_at.desc()),
    ).all()

    return EntityDetail(
        **EntityListItem.model_validate(entity).model_dump(),
        registry_payload=entity.registry_payload,
        domains=[OfficialDomainRead.model_validate(domain) for domain in domains],
    )
