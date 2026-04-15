from __future__ import annotations

from collections.abc import Callable, Generator
from time import sleep
from typing import TypeVar

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings

_ENGINE = None
_SESSION_FACTORY: sessionmaker[Session] | None = None
T = TypeVar("T")


def get_engine():
    global _ENGINE
    if _ENGINE is None:
        settings = get_settings()
        _ENGINE = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_use_lifo=True,
        )
    return _ENGINE


def get_session_factory() -> sessionmaker[Session]:
    global _SESSION_FACTORY
    if _SESSION_FACTORY is None:
        _SESSION_FACTORY = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
    return _SESSION_FACTORY


def get_db_session() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()


def run_read_query(
    operation: Callable[[Session], T],
    *,
    attempts: int = 2,
    retry_sleep_seconds: float = 0.25,
) -> T:
    last_error: OperationalError | None = None
    for attempt in range(attempts):
        session = get_session_factory()()
        try:
            return operation(session)
        except OperationalError as exc:
            last_error = exc
            get_engine().dispose()
            if attempt + 1 >= attempts:
                raise
            sleep(retry_sleep_seconds * (attempt + 1))
        finally:
            session.close()

    assert last_error is not None
    raise last_error
