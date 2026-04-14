from __future__ import annotations

from fastapi import FastAPI

from app.api.routes_entities import router as entities_router
from app.api.routes_health import router as health_router
from app.api.routes_ops import router as ops_router
from app.api.routes_review import router as review_router
from app.core.config import get_settings


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name)
    app.include_router(health_router)
    app.include_router(entities_router)
    app.include_router(review_router)
    app.include_router(ops_router)
    return app


app = create_app()
