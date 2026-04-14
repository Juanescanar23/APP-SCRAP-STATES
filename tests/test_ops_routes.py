from __future__ import annotations

import app.api.routes_ops as routes_ops
from app.main import create_app
from app.services.cohort_report import CohortMetrics, CohortReport
from fastapi.testclient import TestClient


def test_ops_dashboard_renders_summary(monkeypatch) -> None:
    app = create_app()
    client = TestClient(app)

    monkeypatch.setattr(
        routes_ops,
        "build_ops_dashboard_context",
        lambda state: {
            "state": "FL",
            "cohort_report": CohortReport(
                state="FL",
                fresh_max_age_days=14,
                tempered_max_age_days=60,
                cohorts={
                    "mature": CohortMetrics(),
                    "tempered": CohortMetrics(),
                    "fresh": CohortMetrics(active_entities=10, pending_domain_resolution=9),
                },
            ),
            "canary_report": type("Canary", (), {"source_files_completed": 1, "go_ready": True})(),
            "pending_review_items": 2,
            "pending_evidence_review": 3,
            "latest_run": {"status": "completed", "connector_kind": "florida_source_file_import"},
            "pending_domain_samples": [{"legal_name": "00 PIZZA LLC", "sample_kind": "pending"}],
            "verified_domain_samples": [
                {"legal_name": "KNEW HEALTH, INC.", "domain": "knewhealth.com"}
            ],
            "website_evidence_samples": [
                {"legal_name": "KNEW HEALTH, INC.", "value": "hello@knewhealth.com"}
            ],
            "recent_runs": [],
            "recent_source_files": [],
            "recent_sunbiz_artifacts": [],
        },
    )

    response = client.get("/ops?state=FL")

    assert response.status_code == 200
    assert "Ops Console" in response.text
    assert "00 PIZZA LLC" in response.text
    assert "hello@knewhealth.com" in response.text


def test_ops_export_csv_downloads_previewable_rows(monkeypatch) -> None:
    app = create_app()
    client = TestClient(app)

    monkeypatch.setattr(
        routes_ops,
        "build_export_csv_bytes",
        lambda export_kind, **kwargs: (
            f"fl-{export_kind}.csv",
            b"entity_id,legal_name,email\n1,KNEW HEALTH, INC.,hello@knewhealth.com\n",
        ),
    )

    response = client.get("/ops/exports/contacts.csv?state=FL&cohort=fresh")

    assert response.status_code == 200
    assert "attachment; filename=\"fl-contacts.csv\"" == response.headers["content-disposition"]
    assert "hello@knewhealth.com" in response.text


def test_ops_entities_page_uses_sample_inspector(monkeypatch) -> None:
    app = create_app()
    client = TestClient(app)

    monkeypatch.setattr(
        routes_ops,
        "inspect_state_samples",
        lambda state, **kwargs: [
            {
                "sample_kind": "website-evidence",
                "legal_name": "KNEW HEALTH, INC.",
                "domain": "knewhealth.com",
                "value": "hello@knewhealth.com",
            }
        ],
    )

    response = client.get("/ops/entities?state=FL&kind=website-evidence&cohort=fresh")

    assert response.status_code == 200
    assert "Entity Samples" in response.text
    assert "knewhealth.com" in response.text
