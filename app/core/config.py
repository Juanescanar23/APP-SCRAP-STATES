from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="BIZINTEL_",
        case_sensitive=False,
        extra="ignore",
    )

    env: str = "development"
    app_name: str = "bizintel"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    database_url: str = Field(
        default="postgresql+psycopg://bizintel:bizintel@127.0.0.1:55432/bizintel",
        validation_alias=AliasChoices("BIZINTEL_DATABASE_URL", "DATABASE_URL"),
    )
    redis_url: str = Field(
        default="redis://127.0.0.1:6379/0",
        validation_alias=AliasChoices("BIZINTEL_REDIS_URL", "REDIS_URL"),
    )
    storage_backend: str = "local"
    storage_local_root: str = "data/object_store"
    s3_endpoint: str | None = Field(
        default=None,
        validation_alias=AliasChoices("BIZINTEL_S3_ENDPOINT", "ENDPOINT"),
    )
    s3_bucket: str | None = Field(
        default=None,
        validation_alias=AliasChoices("BIZINTEL_S3_BUCKET", "BUCKET"),
    )
    s3_access_key_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("BIZINTEL_S3_ACCESS_KEY_ID", "ACCESS_KEY_ID"),
    )
    s3_secret_access_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("BIZINTEL_S3_SECRET_ACCESS_KEY", "SECRET_ACCESS_KEY"),
    )
    s3_region: str | None = Field(
        default=None,
        validation_alias=AliasChoices("BIZINTEL_S3_REGION", "REGION"),
    )
    user_agent: str = "bizintel-bot/0.1"
    http_timeout_seconds: float = 10.0
    fl_base_url: str = "https://sftp.floridados.gov"
    fl_sftp_username: str | None = None
    fl_sftp_password: str | None = None
    fl_sftp_port: int = 22
    fl_sunbiz_search_base_url: str = "https://search.sunbiz.org"
    fl_download_timeout_seconds: float = 60.0
    fl_download_retries: int = 3
    fl_pdf_retry_days: int = 5
    fl_fresh_cohort_days: int = 14
    fl_tempered_cohort_days: int = 60
    search_provider: str = "none"
    brave_search_api_key: str | None = None
    search_results_per_query: int = 5
    domain_candidate_threshold: float = 0.55
    domain_confidence_threshold: float = 0.8
    domain_max_candidates: int = 5
    site_identity_threshold: float = 0.65
    domain_disambiguation_margin: float = 0.10
    evidence_path_allowlist: tuple[str, ...] = (
        "/",
        "/contact",
        "/contact-us",
        "/about",
        "/about-us",
        "/support",
        "/privacy",
        "/terms",
        "/careers",
    )
    evidence_link_keywords: tuple[str, ...] = (
        "contact",
        "support",
        "about",
        "privacy",
        "terms",
        "career",
    )
    evidence_max_pages: int = 8
    page_size_default: int = 50
    page_size_max: int = 200

    @property
    def fl_sftp_host(self) -> str:
        parsed = urlparse(self.fl_base_url)
        return parsed.hostname or self.fl_base_url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
