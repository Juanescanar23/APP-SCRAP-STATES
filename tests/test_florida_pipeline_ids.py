from __future__ import annotations

from uuid import UUID

from app.workers.tasks_import import _build_source_record_ref_id
from app.workers.tasks_normalize import _build_business_entity_id


def test_source_record_ref_id_is_deterministic_per_file_and_record() -> None:
    source_file_id = UUID("299c4966-af7e-4b47-8837-ca383eda1160")

    assert _build_source_record_ref_id(source_file_id, 42) == _build_source_record_ref_id(
        source_file_id,
        42,
    )
    assert _build_source_record_ref_id(source_file_id, 42) != _build_source_record_ref_id(
        source_file_id,
        43,
    )


def test_business_entity_id_is_stable_per_state_and_filing() -> None:
    entity_id = _build_business_entity_id("fl", "L26000183180")

    assert entity_id == _build_business_entity_id("FL", "L26000183180")
    assert entity_id != _build_business_entity_id("FL", "L26000183181")
