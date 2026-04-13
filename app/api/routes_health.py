from __future__ import annotations

from fastapi import APIRouter

from app.core.config import get_settings
from app.schemas.jobs import HealthResponse


router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(status="ok", app=settings.app_name, env=settings.env)

