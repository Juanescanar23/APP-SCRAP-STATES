from __future__ import annotations

from pathlib import Path

from app.db.models import SourceFileKind
from app.workers.tasks_download import fl_download
from app.workers.tasks_import import import_registry_drop


def enqueue_state_refresh(state: str, source_path: str | Path) -> None:
    import_registry_drop.send(state.upper(), str(source_path))


def enqueue_florida_download(
    feed_kind: SourceFileKind,
    *,
    file_date: str | None = None,
    quarterly_shard: int | None = None,
) -> None:
    fl_download.send(feed_kind.value, file_date, quarterly_shard)
