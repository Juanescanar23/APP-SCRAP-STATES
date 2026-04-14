from __future__ import annotations

import app.workers.tasks_domains as tasks_domains
from app.workers import tasks_normalize


def test_normalize_entities_enqueues_domain_resolution_without_fresh(monkeypatch) -> None:
    sent: dict[str, object] = {}

    def fake_run_entity_normalization(
        state: str,
        *,
        job_run_id: str | None = None,
        source_file_id: str | None = None,
    ) -> int:
        assert state == "FL"
        return 12

    def fake_send(
        state: str,
        cohort: str = "priority",
        include_fresh: bool = True,
    ) -> None:
        sent["state"] = state
        sent["cohort"] = cohort
        sent["include_fresh"] = include_fresh

    monkeypatch.setattr(tasks_normalize, "run_entity_normalization", fake_run_entity_normalization)
    monkeypatch.setattr(tasks_domains.resolve_domains, "send", fake_send)

    tasks_normalize.normalize_entities("FL")

    assert sent == {"state": "FL", "cohort": "priority", "include_fresh": False}


def test_normalize_entities_skips_enqueue_when_nothing_imported(monkeypatch) -> None:
    called = False

    def fake_run_entity_normalization(
        state: str,
        *,
        job_run_id: str | None = None,
        source_file_id: str | None = None,
    ) -> int:
        return 0

    def fake_send(
        state: str,
        cohort: str = "priority",
        include_fresh: bool = True,
    ) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(tasks_normalize, "run_entity_normalization", fake_run_entity_normalization)
    monkeypatch.setattr(tasks_domains.resolve_domains, "send", fake_send)

    tasks_normalize.normalize_entities("FL")

    assert called is False
