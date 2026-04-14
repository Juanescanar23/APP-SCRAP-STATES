from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

from app.db.models import BusinessEntity, EntityStatus
from app.services.entity_cohorts import (
    EntityCohort,
    classify_entity_cohort,
    latest_registry_activity_date,
    prioritize_records_by_entity_cohort,
)


def make_entity(
    name: str,
    *,
    last_transaction_date: str | None = None,
    formed_at: date | None = None,
    first_seen_at: datetime | None = None,
    last_seen_at: datetime | None = None,
) -> BusinessEntity:
    payload = {}
    if last_transaction_date is not None:
        payload["last_transaction_date"] = last_transaction_date
    return BusinessEntity(
        id=uuid4(),
        state="FL",
        external_filing_id=name,
        legal_name=name,
        normalized_name=name.casefold(),
        status=EntityStatus.active,
        formed_at=formed_at,
        registry_payload=payload,
        first_seen_at=first_seen_at or datetime(2026, 4, 14, tzinfo=UTC),
        last_seen_at=last_seen_at or datetime(2026, 4, 14, tzinfo=UTC),
    )


def test_latest_registry_activity_date_prefers_most_recent_registry_signal() -> None:
    entity = make_entity(
        "Alpha LLC",
        last_transaction_date="2026-04-10",
        formed_at=date(2024, 1, 15),
    )

    assert latest_registry_activity_date(entity) == date(2026, 4, 10)


def test_classify_entity_cohort_uses_first_seen_windows() -> None:
    reference_date = date(2026, 4, 14)

    assert (
        classify_entity_cohort(
            make_entity(
                "Fresh LLC",
                last_transaction_date="2020-04-08",
                first_seen_at=datetime(2026, 4, 8, tzinfo=UTC),
            ),
            reference_date=reference_date,
        )
        == EntityCohort.fresh
    )
    assert (
        classify_entity_cohort(
            make_entity(
                "Tempered LLC",
                last_transaction_date="2020-03-10",
                first_seen_at=datetime(2026, 3, 10, tzinfo=UTC),
            ),
            reference_date=reference_date,
        )
        == EntityCohort.tempered
    )
    assert (
        classify_entity_cohort(
            make_entity(
                "Mature LLC",
                last_transaction_date="2026-04-10",
                first_seen_at=datetime(2026, 1, 10, tzinfo=UTC),
            ),
            reference_date=reference_date,
        )
        == EntityCohort.mature
    )


def test_prioritize_records_by_entity_cohort_sorts_mature_first_and_filters_explicit_cohort(
) -> None:
    fresh = make_entity(
        "Fresh LLC",
        last_transaction_date="2026-04-08",
        first_seen_at=datetime(2026, 4, 8, tzinfo=UTC),
        last_seen_at=datetime(2026, 4, 14, 9, tzinfo=UTC),
    )
    tempered = make_entity(
        "Tempered LLC",
        last_transaction_date="2026-03-10",
        first_seen_at=datetime(2026, 3, 10, tzinfo=UTC),
        last_seen_at=datetime(2026, 4, 14, 10, tzinfo=UTC),
    )
    mature = make_entity(
        "Mature LLC",
        last_transaction_date="2026-01-10",
        first_seen_at=datetime(2026, 1, 10, tzinfo=UTC),
        last_seen_at=datetime(2026, 4, 14, 11, tzinfo=UTC),
    )
    records = [fresh, tempered, mature]

    prioritized = prioritize_records_by_entity_cohort(
        records,
        entity_getter=lambda entity: entity,
        reference_date=date(2026, 4, 14),
    )
    tempered_only = prioritize_records_by_entity_cohort(
        records,
        entity_getter=lambda entity: entity,
        cohort="tempered",
        reference_date=date(2026, 4, 14),
    )

    assert [entity.legal_name for entity in prioritized] == [
        "Mature LLC",
        "Tempered LLC",
        "Fresh LLC",
    ]
    assert [entity.legal_name for entity in tempered_only] == ["Tempered LLC"]


def test_prioritize_records_by_entity_cohort_can_skip_fresh_for_automatic_runs() -> None:
    fresh = make_entity(
        "Fresh LLC",
        first_seen_at=datetime(2026, 4, 8, tzinfo=UTC),
    )
    mature = make_entity(
        "Mature LLC",
        first_seen_at=datetime(2026, 1, 10, tzinfo=UTC),
    )

    prioritized = prioritize_records_by_entity_cohort(
        [fresh, mature],
        entity_getter=lambda entity: entity,
        include_fresh=False,
        reference_date=date(2026, 4, 14),
    )

    assert [entity.legal_name for entity in prioritized] == ["Mature LLC"]
