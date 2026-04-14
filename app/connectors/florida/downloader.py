from __future__ import annotations

import hashlib
import mimetypes
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urljoin

import httpx

from app.connectors.florida.parser import list_archive_members_from_bytes
from app.connectors.florida.source import (
    DAILY_CORPORATE_EVENTS_PATH_TEMPLATE,
    DAILY_CORPORATE_FILINGS_PATH_TEMPLATE,
    QUARTERLY_CORPORATE_EVENTS_PATH,
    QUARTERLY_CORPORATE_FILINGS_PATH,
)
from app.core.config import get_settings
from app.db.models import SourceFileKind
from app.services.object_store import ObjectStore, StoredObject


@dataclass(slots=True)
class FloridaDownloadRequest:
    source_kind: SourceFileKind
    file_date: date | None = None
    quarterly_shard: int | None = None

    @property
    def remote_path(self) -> str:
        if self.source_kind == SourceFileKind.quarterly_corporate:
            return QUARTERLY_CORPORATE_FILINGS_PATH
        if self.source_kind == SourceFileKind.quarterly_corporate_events:
            return QUARTERLY_CORPORATE_EVENTS_PATH
        if self.file_date is None:
            raise ValueError("Daily Florida downloads require file_date.")
        yyyymmdd = self.file_date.strftime("%Y%m%d")
        if self.source_kind == SourceFileKind.daily_corporate:
            return DAILY_CORPORATE_FILINGS_PATH_TEMPLATE.format(yyyymmdd=yyyymmdd)
        if self.source_kind == SourceFileKind.daily_corporate_events:
            return DAILY_CORPORATE_EVENTS_PATH_TEMPLATE.format(yyyymmdd=yyyymmdd)
        raise ValueError(f"Unsupported Florida feed kind: {self.source_kind}")

    @property
    def logical_remote_path(self) -> str:
        if self.quarterly_shard is None:
            return self.remote_path
        return f"{self.remote_path}#shard={self.quarterly_shard}"

    @property
    def remote_url(self) -> str:
        settings = get_settings()
        return urljoin(f"{settings.fl_base_url.rstrip('/')}/", self.remote_path)

    @property
    def filename(self) -> str:
        base_name = Path(self.remote_path).name
        if self.quarterly_shard is None:
            return base_name
        return f"{base_name}#shard={self.quarterly_shard}"

    @property
    def is_daily(self) -> bool:
        return self.source_kind in {
            SourceFileKind.daily_corporate,
            SourceFileKind.daily_corporate_events,
        }

    @property
    def period_date(self) -> date:
        if self.file_date is not None:
            return self.file_date
        today = date.today()
        quarter_month = ((today.month - 1) // 3) * 3 + 1
        return date(today.year, quarter_month, 1)

    @property
    def period_key(self) -> str:
        if self.is_daily:
            return self.period_date.isoformat()
        quarter = ((self.period_date.month - 1) // 3) + 1
        return f"{self.period_date.year}Q{quarter}"


@dataclass(slots=True)
class FloridaDownloadResult:
    request: FloridaDownloadRequest
    storage_object: StoredObject | None
    checksum: str | None
    downloaded_at: datetime
    archive_members: list[str]
    status: str


async def download_florida_source_file(
    request: FloridaDownloadRequest,
    object_store: ObjectStore,
    client: httpx.AsyncClient | None = None,
    sftp_fetcher: Callable[[FloridaDownloadRequest], bytes] | None = None,
) -> FloridaDownloadResult:
    settings = get_settings()
    if settings.fl_sftp_username and settings.fl_sftp_password:
        try:
            payload = await _download_via_sftp(
                request,
                fetcher=sftp_fetcher or _download_via_sftp_sync,
            )
        except FileNotFoundError:
            if request.is_daily:
                return FloridaDownloadResult(
                    request=request,
                    storage_object=None,
                    checksum=None,
                    downloaded_at=datetime.now(UTC),
                    archive_members=[],
                    status="noop",
                )
            raise
        return _build_download_result(request, payload, object_store)

    owned_client = client is None
    if client is None:
        client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=settings.fl_download_timeout_seconds,
        )

    try:
        response = await client.get(request.remote_url)
        if response.status_code == 404 and request.is_daily:
            return FloridaDownloadResult(
                request=request,
                storage_object=None,
                checksum=None,
                downloaded_at=datetime.now(UTC),
                archive_members=[],
                status="noop",
            )
        response.raise_for_status()

        return _build_download_result(
            request,
            response.content,
            object_store,
            content_type=response.headers.get("content-type"),
        )
    finally:
        if owned_client:
            await client.aclose()


async def _download_via_sftp(
    request: FloridaDownloadRequest,
    *,
    fetcher: Callable[[FloridaDownloadRequest], bytes],
) -> bytes:
    return await __import__("asyncio").to_thread(fetcher, request)


def _download_via_sftp_sync(request: FloridaDownloadRequest) -> bytes:
    settings = get_settings()
    if not settings.fl_sftp_username or not settings.fl_sftp_password:
        raise RuntimeError("Florida SFTP credentials are required for SFTP downloads.")

    try:
        import paramiko  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("paramiko is required for Florida SFTP downloads.") from exc

    transport = paramiko.Transport((settings.fl_sftp_host, settings.fl_sftp_port))
    try:
        transport.connect(
            username=settings.fl_sftp_username,
            password=settings.fl_sftp_password,
        )
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            with sftp.open(request.remote_path, "rb") as remote_file:
                return remote_file.read()
        except OSError as exc:
            if getattr(exc, "errno", None) == 2 or "No such file" in str(exc):
                raise FileNotFoundError(request.remote_path) from exc
            raise
        finally:
            sftp.close()
    finally:
        transport.close()


def _build_download_result(
    request: FloridaDownloadRequest,
    payload: bytes,
    object_store: ObjectStore,
    *,
    content_type: str | None = None,
) -> FloridaDownloadResult:
    checksum = hashlib.sha256(payload).hexdigest()
    inferred_content_type = content_type or mimetypes.guess_type(request.filename)[0]
    stored_object = object_store.put_bytes(
        build_bucket_key(request, checksum=checksum),
        payload,
        content_type=inferred_content_type,
        metadata={
            "remote_path": request.remote_path,
            "period_key": request.period_key,
        },
    )
    archive_members = []
    if request.remote_path.lower().endswith(".zip"):
        archive_members = list_archive_members_from_bytes(payload)
    return FloridaDownloadResult(
        request=request,
        storage_object=stored_object,
        checksum=checksum,
        downloaded_at=datetime.now(UTC),
        archive_members=archive_members,
        status="completed",
    )


def build_bucket_key(
    request: FloridaDownloadRequest,
    *,
    checksum: str | None = None,
    downloaded_at: datetime | None = None,
) -> str:
    timestamp = downloaded_at or datetime.now(UTC)
    date_segment = request.period_key or timestamp.date().isoformat()
    base_name = Path(request.remote_path).name
    if checksum:
        return f"raw/fl/{request.source_kind.value}/{date_segment}/{checksum[:16]}-{base_name}"
    return f"raw/fl/{request.source_kind.value}/{date_segment}/{base_name}"
