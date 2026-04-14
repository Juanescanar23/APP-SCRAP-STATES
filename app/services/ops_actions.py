from __future__ import annotations

from datetime import date

from app.db.models import SourceFileKind
from app.workers.tasks_domains import resolve_domains
from app.workers.tasks_download import fl_download
from app.workers.tasks_evidence import collect_public_contact_evidence

FLORIDA_SHARDS = tuple(range(10))


def queue_florida_official_refresh(
    state: str,
    *,
    daily_date: date,
) -> dict[str, object]:
    quarterly_jobs = queue_florida_quarterly_refresh(state)
    daily_jobs = queue_florida_daily_refresh(state, file_date=daily_date)
    return {
        "queued_jobs": quarterly_jobs["queued_jobs"] + daily_jobs["queued_jobs"],
        "quarterly_jobs": quarterly_jobs["queued_jobs"],
        "daily_jobs": daily_jobs["queued_jobs"],
        "daily_date": daily_date.isoformat(),
    }


def queue_florida_quarterly_refresh(
    state: str,
    *,
    quarterly_shard: int | None = None,
) -> dict[str, object]:
    normalized_state = state.upper()
    if normalized_state != "FL":
        raise ValueError("Solo Florida soporta esta accion por ahora.")

    shards = (quarterly_shard,) if quarterly_shard is not None else FLORIDA_SHARDS
    queued_jobs = 0
    for shard in shards:
        fl_download.send(SourceFileKind.quarterly_corporate.value, None, shard)
        fl_download.send(SourceFileKind.quarterly_corporate_events.value, None, shard)
        queued_jobs += 2

    return {
        "queued_jobs": queued_jobs,
        "shards": list(shards),
    }


def queue_florida_daily_refresh(
    state: str,
    *,
    file_date: date,
) -> dict[str, object]:
    normalized_state = state.upper()
    if normalized_state != "FL":
        raise ValueError("Solo Florida soporta esta accion por ahora.")

    file_date_str = file_date.isoformat()
    fl_download.send(SourceFileKind.daily_corporate.value, file_date_str)
    fl_download.send(SourceFileKind.daily_corporate_events.value, file_date_str)
    return {
        "queued_jobs": 2,
        "file_date": file_date_str,
    }


def queue_domain_enrichment(
    state: str,
    *,
    cohort: str = "priority",
    include_fresh: bool = True,
) -> dict[str, object]:
    resolve_domains.send(state.upper(), cohort, include_fresh)
    return {
        "queued_jobs": 1,
        "cohort": cohort,
        "include_fresh": include_fresh,
    }


def queue_verified_contact_collection(
    state: str,
    *,
    limit: int = 250,
    cohort: str = "priority",
    include_fresh: bool = True,
) -> dict[str, object]:
    collect_public_contact_evidence.send(state.upper(), limit, cohort, include_fresh)
    return {
        "queued_jobs": 1,
        "limit": limit,
        "cohort": cohort,
        "include_fresh": include_fresh,
    }
