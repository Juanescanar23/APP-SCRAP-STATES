from __future__ import annotations

from app import cli
from app.services.cohort_report import CohortMetrics, CohortReport
from app.services.metrics import DomainResolutionMetrics, EvidenceCollectionMetrics


def test_cli_resolve_domains_forwards_cohort(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def fake_run_domain_resolution(
        state: str,
        *,
        limit: int = 250,
        cohort: str = "priority",
        include_fresh: bool = True,
        dry_run: bool = False,
        search_provider=None,
        site_inspector=None,
    ) -> DomainResolutionMetrics:
        captured["state"] = state
        captured["limit"] = limit
        captured["cohort"] = cohort
        captured["include_fresh"] = include_fresh
        captured["dry_run"] = dry_run
        return DomainResolutionMetrics(imported_entities=12)

    monkeypatch.setattr(cli, "run_domain_resolution", fake_run_domain_resolution)

    exit_code = cli.main([
        "resolve-domains",
        "--state",
        "FL",
        "--limit",
        "25",
        "--cohort",
        "mature",
        "--exclude-fresh",
        "--dry-run",
    ])

    assert exit_code == 0
    assert captured == {
        "state": "FL",
        "limit": 25,
        "cohort": "mature",
        "include_fresh": False,
        "dry_run": True,
    }
    assert "imported_entities=12" in capsys.readouterr().out


def test_cli_collect_evidence_forwards_cohort(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def fake_run_public_contact_collection(
        state: str,
        *,
        limit: int = 100,
        cohort: str = "priority",
        include_fresh: bool = True,
        verified_only: bool = True,
        pending_only: bool = True,
        dry_run: bool = False,
    ) -> EvidenceCollectionMetrics:
        captured["state"] = state
        captured["limit"] = limit
        captured["cohort"] = cohort
        captured["include_fresh"] = include_fresh
        captured["verified_only"] = verified_only
        captured["pending_only"] = pending_only
        captured["dry_run"] = dry_run
        return EvidenceCollectionMetrics(contact_form_only=3)

    monkeypatch.setattr(cli, "run_public_contact_collection", fake_run_public_contact_collection)

    exit_code = cli.main([
        "collect-evidence",
        "--state",
        "FL",
        "--limit",
        "10",
        "--cohort",
        "tempered",
        "--exclude-fresh",
        "--verified-only",
        "--all-domains",
    ])

    assert exit_code == 0
    assert captured == {
        "state": "FL",
        "limit": 10,
        "cohort": "tempered",
        "include_fresh": False,
        "verified_only": True,
        "pending_only": False,
        "dry_run": False,
    }
    assert "contact_form_only=3" in capsys.readouterr().out


def test_cli_report_cohorts_prints_flattened_metrics(monkeypatch, capsys) -> None:
    def fake_run_cohort_report(state: str) -> CohortReport:
        assert state == "FL"
        return CohortReport(
            state="FL",
            fresh_max_age_days=14,
            tempered_max_age_days=60,
            cohorts={
                "mature": CohortMetrics(active_entities=20, verified_entities=9),
                "tempered": CohortMetrics(active_entities=12, pending_domain_resolution=7),
                "fresh": CohortMetrics(active_entities=8, pending_website_evidence=5),
            },
        )

    monkeypatch.setattr(cli, "run_cohort_report", fake_run_cohort_report)

    exit_code = cli.main(["report-cohorts", "--state", "FL"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "mature_verified_entities=9" in output
    assert "tempered_pending_domain_resolution=7" in output
    assert "fresh_pending_website_evidence=5" in output


def test_cli_inspect_samples_prints_rows(monkeypatch, capsys) -> None:
    def fake_inspect_state_samples(
        state: str,
        *,
        sample_kind: str,
        cohort: str = "priority",
        include_fresh: bool = True,
        limit: int = 10,
    ) -> list[dict[str, object]]:
        assert state == "FL"
        assert sample_kind == "website-evidence"
        assert cohort == "fresh"
        assert include_fresh is True
        assert limit == 2
        return [
            {
                "sample_kind": "website-evidence",
                "legal_name": "KNEW HEALTH, INC.",
                "domain": "knewhealth.com",
                "kind": "email",
                "value": "hello@knewhealth.com",
            }
        ]

    monkeypatch.setattr(cli, "inspect_state_samples", fake_inspect_state_samples)

    exit_code = cli.main(
        [
            "inspect-samples",
            "--state",
            "FL",
            "--kind",
            "website-evidence",
            "--cohort",
            "fresh",
            "--limit",
            "2",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "count=1" in output
    assert "KNEW HEALTH, INC." in output
