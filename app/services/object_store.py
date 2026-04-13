from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol

from app.core.config import Settings, get_settings


@dataclass(slots=True)
class StoredObject:
    key: str
    size_bytes: int
    etag: str | None = None


class ObjectStore(Protocol):
    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> StoredObject:
        raise NotImplementedError

    def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def write_to_path(self, key: str, destination: Path) -> Path:
        raise NotImplementedError


class LocalObjectStore:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> StoredObject:
        del content_type, metadata
        target = self._resolve_key(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return StoredObject(key=key, size_bytes=len(data))

    def get_bytes(self, key: str) -> bytes:
        return self._resolve_key(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._resolve_key(key).exists()

    def write_to_path(self, key: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.get_bytes(key))
        return destination

    def _resolve_key(self, key: str) -> Path:
        normalized_key = key.lstrip("/").replace("..", "__")
        return self._root / normalized_key


class S3ObjectStore:
    def __init__(self, settings: Settings) -> None:
        if not settings.s3_bucket:
            raise RuntimeError("BIZINTEL_S3_BUCKET is required when storage_backend=s3.")

        try:
            import boto3  # type: ignore
        except ModuleNotFoundError as exc:
            raise RuntimeError("boto3 is required when storage_backend=s3.") from exc

        self._bucket = settings.s3_bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key_id,
            aws_secret_access_key=settings.s3_secret_access_key,
            region_name=settings.s3_region,
        )

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> StoredObject:
        extra: dict[str, Any] = {}
        if content_type:
            extra["ContentType"] = content_type
        if metadata:
            extra["Metadata"] = metadata
        response = self._client.put_object(Bucket=self._bucket, Key=key, Body=data, **extra)
        return StoredObject(
            key=key,
            size_bytes=len(data),
            etag=str(response.get("ETag", "")).strip('"') or None,
        )

    def get_bytes(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except Exception:
            return False
        return True

    def write_to_path(self, key: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("wb") as handle:
            self._client.download_fileobj(self._bucket, key, handle)
        return destination


@lru_cache(maxsize=1)
def get_object_store() -> ObjectStore:
    settings = get_settings()
    if settings.storage_backend.casefold() == "s3":
        return S3ObjectStore(settings)
    return LocalObjectStore(settings.storage_local_root)
