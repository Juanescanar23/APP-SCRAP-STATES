from __future__ import annotations

import app.workers.tasks_evidence as tasks_evidence
from app.services.metrics import DomainResolutionMetrics
from app.workers import tasks_domains


def test_resolve_official_domain_enqueues_website_evidence_for_processed_batch(
    monkeypatch,
) -> None:
    sent: dict[str, object] = {}

    def fake_run_domain_resolution(
        state: str,
        *,
        limit: int = 250,
        cohort: str = "priority",
    ) -> DomainResolutionMetrics:
        assert state == "fl"
        assert limit == 75
        assert cohort == "mature"
        return DomainResolutionMetrics(imported_entities=4, domain_verified=0)

    def fake_send(state: str, limit: int, cohort: str = "priority") -> None:
        sent["state"] = state
        sent["limit"] = limit
        sent["cohort"] = cohort

    monkeypatch.setattr(tasks_domains, "run_domain_resolution", fake_run_domain_resolution)
    monkeypatch.setattr(tasks_evidence.collect_public_contact_evidence, "send", fake_send)

    tasks_domains.resolve_official_domain("fl", limit=75, cohort="mature")

    assert sent == {"state": "FL", "limit": 75, "cohort": "mature"}


def test_resolve_official_domain_skips_website_evidence_when_nothing_was_processed(
    monkeypatch,
) -> None:
    called = False

    def fake_run_domain_resolution(
        state: str,
        *,
        limit: int = 250,
        cohort: str = "priority",
    ) -> DomainResolutionMetrics:
        return DomainResolutionMetrics(imported_entities=0, domain_verified=0)

    def fake_send(state: str, limit: int, cohort: str = "priority") -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(tasks_domains, "run_domain_resolution", fake_run_domain_resolution)
    monkeypatch.setattr(tasks_evidence.collect_public_contact_evidence, "send", fake_send)

    tasks_domains.resolve_official_domain("FL", limit=25)

    assert called is False
