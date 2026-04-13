from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

import dramatiq
import httpx
from sqlalchemy import desc, select
from sqlalchemy.dialects.postgresql import insert

from app.connectors.florida.downloader import (
    FloridaDownloadRequest,
    FloridaDownloadResult,
    download_florida_source_file,
)
from app.connectors.florida.parser import record_length_for_kind
from app.core.config import get_settings
from app.db.models import (
    JobRun,
    JobStatus,
    SourceFile,
    SourceFileKind,
    SourceFileStatus,
    SourceIngestCursor,
)
from app.db.session import get_session_factory
from app.services.object_store import get_object_store
from app.workers.broker import broker  # noqa: F401


@dramatiq.actor(max_retries=5, queue_name="fl_download")
def fl_download(
    feed_kind: str,
    file_date: str | None = None,
    quarterly_shard: int | None = None,
    force: bool = False,
    enqueue_import: bool = True,
) -> None:
    source_file_id = asyncio.run(
        run_fl_download(
            feed_kind,
            file_date=file_date,
            quarterly_shard=quarterly_shard,
            force=force,
        ),
    )
    if source_file_id and enqueue_import:
        from app.workers.tasks_import import import_source_file

        import_source_file.send(str(source_file_id))


async def run_fl_download(
    feed_kind: str,
    *,
    file_date: str | None = None,
    quarterly_shard: int | None = None,
    force: bool = False,
) -> str | None:
    source_kind = SourceFileKind(feed_kind)
    parsed_date = date.fromisoformat(file_date) if file_date else None
    request = FloridaDownloadRequest(
        source_kind=source_kind,
        file_date=parsed_date,
        quarterly_shard=quarterly_shard,
    )

    session = get_session_factory()()
    cursor = _get_or_create_cursor(session, request)
    if cursor.status in {SourceFileStatus.completed, SourceFileStatus.noop} and not force:
        existing = session.scalar(
            select(SourceFile.id)
            .where(SourceFile.provider == "sunbiz")
            .where(SourceFile.source_kind == source_kind)
            .where(SourceFile.filename == request.filename)
            .where(SourceFile.file_date == request.period_date)
            .order_by(desc(SourceFile.downloaded_at))
        )
        if existing is not None:
            now = datetime.now(UTC)
            cursor.last_checked_at = now
            replay_run = JobRun(
                connector_kind="florida_official_downloader",
                state="FL",
                source_uri=request.remote_url,
                source_checksum="replay",
                status=JobStatus.completed,
                finished_at=now,
                stats={
                    "status": "replay",
                    "feed_kind": source_kind.value,
                    "existing_source_file_id": str(existing),
                },
            )
            session.add_all([cursor, replay_run])
            session.commit()
            session.close()
            return str(existing)
        session.close()

    job_run = JobRun(
        connector_kind="florida_official_downloader",
        state="FL",
        source_uri=request.remote_url,
        source_checksum="pending",
        status=JobStatus.running,
        stats={},
    )
    session.add(job_run)
    session.flush()

    cursor.status = SourceFileStatus.processing
    cursor.last_checked_at = datetime.now(UTC)
    cursor.last_error = None
    cursor.metadata_json = {
        **(cursor.metadata_json or {}),
        "quarterly_shard": quarterly_shard,
    }
    session.add(cursor)
    session.commit()
    session.close()

    object_store = get_object_store()
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=get_settings().fl_download_timeout_seconds,
    ) as client:
        result = await download_florida_source_file(request, object_store, client)

    session = get_session_factory()()
    try:
        cursor = _get_or_create_cursor(session, request)
        job_run = session.get(JobRun, job_run.id)
        if job_run is None:
            raise ValueError("Download job run disappeared.")

        if result.status == "noop":
            cursor.status = SourceFileStatus.noop
            cursor.last_checked_at = result.downloaded_at
            job_run.status = JobStatus.completed
            job_run.finished_at = datetime.now(UTC)
            job_run.stats = {"status": "noop", "feed_kind": source_kind.value}
            session.add_all([cursor, job_run])
            session.commit()
            return None

        source_file_id = _upsert_source_file(session, result, job_run.id)
        cursor.status = SourceFileStatus.completed
        cursor.last_checked_at = result.downloaded_at
        cursor.last_downloaded_at = result.downloaded_at
        cursor.last_error = None
        cursor.metadata_json = {
            **(cursor.metadata_json or {}),
            "bucket_key": result.storage_object.key if result.storage_object else None,
            "archive_members": result.archive_members,
        }
        job_run.source_checksum = result.checksum or "missing"
        job_run.status = JobStatus.completed
        job_run.finished_at = datetime.now(UTC)
        job_run.stats = {
            "status": result.status,
            "feed_kind": source_kind.value,
            "size_bytes": result.storage_object.size_bytes if result.storage_object else 0,
        }
        session.add_all([cursor, job_run])
        session.commit()
        return str(source_file_id)
    except Exception as exc:
        session.rollback()
        cursor = _get_or_create_cursor(session, request)
        cursor.status = SourceFileStatus.failed
        cursor.last_checked_at = datetime.now(UTC)
        cursor.last_error = str(exc)
        job_run = session.get(JobRun, job_run.id)
        if job_run is not None:
            job_run.status = JobStatus.failed
            job_run.finished_at = datetime.now(UTC)
            session.add(job_run)
        session.add(cursor)
        session.commit()
        raise
    finally:
        session.close()


def _get_or_create_cursor(session, request: FloridaDownloadRequest) -> SourceIngestCursor:
    cursor_file_date = request.period_date
    cursor = session.scalar(
        select(SourceIngestCursor)
        .where(SourceIngestCursor.state == "FL")
        .where(SourceIngestCursor.feed_kind == request.source_kind)
        .where(SourceIngestCursor.remote_path == request.logical_remote_path)
        .where(SourceIngestCursor.file_date == cursor_file_date),
    )
    if cursor is not None:
        return cursor

    cursor = SourceIngestCursor(
        state="FL",
        feed_kind=request.source_kind,
        remote_path=request.logical_remote_path,
        file_date=cursor_file_date,
        status=SourceFileStatus.pending,
        metadata_json={"quarterly_shard": request.quarterly_shard},
    )
    session.add(cursor)
    session.flush()
    return cursor


def _upsert_source_file(session, result: FloridaDownloadResult, job_run_id) -> str:
    values = {
        "job_run_id": job_run_id,
        "provider": "sunbiz",
        "source_kind": result.request.source_kind,
        "state": "FL",
        "filename": result.request.filename,
        "source_uri": result.request.remote_url,
        "bucket_key": result.storage_object.key if result.storage_object else None,
        "source_checksum": result.checksum,
        "size_bytes": result.storage_object.size_bytes if result.storage_object else None,
        "record_length": record_length_for_kind(result.request.source_kind),
        "file_date": result.request.period_date,
        "is_delta": result.request.is_daily,
        "status": SourceFileStatus.pending,
        "total_records": 0,
        "metadata_json": {
            "quarterly_shard": result.request.quarterly_shard,
            "remote_path": result.request.remote_path,
            "period_key": result.request.period_key,
            "archive_members": result.archive_members,
        },
        "downloaded_at": result.downloaded_at,
    }

    stmt = insert(SourceFile).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["provider", "source_kind", "filename", "file_date", "source_checksum"],
        set_={
            "job_run_id": stmt.excluded.job_run_id,
            "source_uri": stmt.excluded.source_uri,
            "bucket_key": stmt.excluded.bucket_key,
            "size_bytes": stmt.excluded.size_bytes,
            "record_length": stmt.excluded.record_length,
            "file_date": stmt.excluded.file_date,
            "is_delta": stmt.excluded.is_delta,
            "status": stmt.excluded.status,
            "metadata_json": stmt.excluded.metadata_json,
            "downloaded_at": stmt.excluded.downloaded_at,
        },
    ).returning(SourceFile.id)
    return str(session.scalar(stmt))
