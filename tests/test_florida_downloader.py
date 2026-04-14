from __future__ import annotations

from datetime import date
from pathlib import Path
from zipfile import ZipFile

import httpx
import pytest
from app.connectors.florida.downloader import (
    FloridaDownloadRequest,
    build_bucket_key,
    download_florida_source_file,
)
from app.db.models import SourceFileKind
from app.services.object_store import LocalObjectStore


@pytest.mark.asyncio
async def test_daily_download_404_is_noop(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    request = FloridaDownloadRequest(
        source_kind=SourceFileKind.daily_corporate,
        file_date=date(2026, 4, 10),
    )
    transport = httpx.MockTransport(lambda req: httpx.Response(404, request=req))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await download_florida_source_file(request, store, client)

    assert result.status == "noop"
    assert result.storage_object is None


@pytest.mark.asyncio
async def test_quarterly_download_stores_zip_and_lists_members(tmp_path: Path) -> None:
    zip_path = tmp_path / "cordata.zip"
    with ZipFile(zip_path, "w") as archive:
        archive.writestr("cordata_0.txt", "record-0\n")
        archive.writestr("cordata_1.txt", "record-1\n")
    payload = zip_path.read_bytes()

    store = LocalObjectStore(tmp_path / "objects")
    request = FloridaDownloadRequest(
        source_kind=SourceFileKind.quarterly_corporate,
        quarterly_shard=0,
    )
    transport = httpx.MockTransport(lambda req: httpx.Response(200, request=req, content=payload))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await download_florida_source_file(request, store, client)

    assert result.status == "completed"
    assert result.archive_members == ["cordata_0.txt", "cordata_1.txt"]
    assert result.storage_object is not None
    assert store.exists(build_bucket_key(request, checksum=result.checksum))


@pytest.mark.asyncio
async def test_sftp_download_uses_fetcher_when_credentials_exist(
    tmp_path: Path,
    monkeypatch,
) -> None:
    zip_path = tmp_path / "cordata.zip"
    with ZipFile(zip_path, "w") as archive:
        archive.writestr("cordata_0.txt", "record-0\n")
    payload = zip_path.read_bytes()

    monkeypatch.setenv("BIZINTEL_FL_SFTP_USERNAME", "Public")
    monkeypatch.setenv("BIZINTEL_FL_SFTP_PASSWORD", "PubAccess1845!")
    monkeypatch.setenv("BIZINTEL_FL_SFTP_PORT", "22")
    from app.core.config import get_settings

    get_settings.cache_clear()
    store = LocalObjectStore(tmp_path / "objects")
    request = FloridaDownloadRequest(
        source_kind=SourceFileKind.quarterly_corporate,
        quarterly_shard=0,
    )

    def fake_sftp_fetcher(_: FloridaDownloadRequest) -> bytes:
        return payload

    result = await download_florida_source_file(request, store, sftp_fetcher=fake_sftp_fetcher)

    assert result.status == "completed"
    assert result.archive_members == ["cordata_0.txt"]
    assert result.storage_object is not None
    assert store.exists(build_bucket_key(request, checksum=result.checksum))
    get_settings.cache_clear()


def test_build_bucket_key_uses_checksum_versioning_for_quarterly() -> None:
    request = FloridaDownloadRequest(
        source_kind=SourceFileKind.quarterly_corporate,
        quarterly_shard=0,
    )

    key = build_bucket_key(request, checksum="0123456789abcdef0123456789abcdef")

    assert key.startswith("raw/fl/quarterly_corporate/")
    assert key.endswith("/0123456789abcdef-cordata.zip")
