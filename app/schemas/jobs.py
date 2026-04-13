from __future__ import annotations

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    app: str
    env: str


class JobDispatchRequest(BaseModel):
    state: str = Field(min_length=2, max_length=2)
    source_path: str


class JobDispatchResponse(BaseModel):
    enqueued: bool
    state: str
    source_path: str

