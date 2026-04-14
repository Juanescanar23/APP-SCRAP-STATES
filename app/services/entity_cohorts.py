from __future__ import annotations

import enum
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from typing import TypeVar

from app.core.config import Settings, get_settings
from app.db.models import BusinessEntity


class EntityCohort(enum.StrEnum):
    fresh = "fresh"
    tempered = "tempered"
    mature = "mature"


COHORT_SELECTION_VALUES = ("priority", "mature", "tempered", "fresh")
COHORT_PRIORITY = (
    EntityCohort.mature,
    EntityCohort.tempered,
    EntityCohort.fresh,
)
DEFAULT_COHORT_SELECTION = "priority"

T = TypeVar("T")


def latest_registry_activity_date(entity: BusinessEntity) -> date | None:
    payload = entity.registry_payload or {}
    candidate_dates = [
        parse_iso_date(str(payload.get("last_transaction_date") or "")),
        parse_iso_date(str(payload.get("formed_at") or "")),
        entity.formed_at,
    ]
    return max((value for value in candidate_dates if value is not None), default=None)


def parse_iso_date(value: str) -> date | None:
    value = value.strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def classify_entity_cohort(
    entity: BusinessEntity,
    *,
    reference_date: date | None = None,
    settings: Settings | None = None,
) -> EntityCohort:
    current_settings = settings or get_settings()
    observed_on = _first_seen_date(entity)
    age_days = ((reference_date or date.today()) - observed_on).days
    if age_days <= current_settings.fl_fresh_cohort_days:
        return EntityCohort.fresh
    if age_days <= current_settings.fl_tempered_cohort_days:
        return EntityCohort.tempered
    return EntityCohort.mature


def normalize_cohort_selection(value: str | None) -> str:
    normalized = (value or DEFAULT_COHORT_SELECTION).strip().casefold()
    if normalized not in COHORT_SELECTION_VALUES:
        allowed = ", ".join(COHORT_SELECTION_VALUES)
        raise ValueError(f"Unsupported cohort selection: {value!r}. Expected one of: {allowed}.")
    return normalized


def prioritize_records_by_entity_cohort(
    records: Sequence[T],
    *,
    entity_getter: Callable[[T], BusinessEntity],
    cohort: str = DEFAULT_COHORT_SELECTION,
    include_fresh: bool = True,
    reference_date: date | None = None,
    settings: Settings | None = None,
) -> list[T]:
    selection = normalize_cohort_selection(cohort)
    current_settings = settings or get_settings()
    ranked: list[tuple[int, float, str, int, T]] = []

    for index, record in enumerate(records):
        entity = entity_getter(record)
        entity_cohort = classify_entity_cohort(
            entity,
            reference_date=reference_date,
            settings=current_settings,
        )
        if (
            selection == DEFAULT_COHORT_SELECTION
            and not include_fresh
            and entity_cohort == EntityCohort.fresh
        ):
            continue
        if selection != DEFAULT_COHORT_SELECTION and entity_cohort.value != selection:
            continue

        ranked.append(
            (
                _cohort_priority(entity_cohort),
                -_timestamp_key(entity.last_seen_at),
                entity.legal_name.casefold(),
                index,
                record,
            )
        )

    ranked.sort()
    return [record for _, _, _, _, record in ranked]


def iter_cohorts() -> tuple[EntityCohort, ...]:
    return COHORT_PRIORITY


def _cohort_priority(cohort: EntityCohort) -> int:
    return COHORT_PRIORITY.index(cohort)


def _timestamp_key(value: datetime) -> float:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.timestamp()


def _first_seen_date(entity: BusinessEntity) -> date:
    value = entity.first_seen_at
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.date()
