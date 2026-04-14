from __future__ import annotations

import uuid

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
            "source_summary": {
                "active_entities": 10,
                "current_snapshots": 10,
                "quarterly_corporate_completed_shards": 2,
                "quarterly_event_completed_shards": 2,
                "latest_daily_corporate_date": "2026-04-14",
                "summary_rows": [{"fuente": "Quarterly corporativo", "archivos_completados": 2}],
            },
            "pending_review_items": 2,
            "pending_evidence_review": 3,
            "latest_run": {"status": "completed", "connector_kind": "florida_source_file_import"},
            "base_oficial_preview": [
                {"legal_name": "00 PIZZA LLC", "external_filing_id": "L260001"},
            ],
            "empresas_preview": [
                {"legal_name": "00 PIZZA LLC", "primary_email": None},
            ],
            "contactos_primary_preview": [
                {"legal_name": "KNEW HEALTH, INC.", "primary_email": "hello@knewhealth.com"}
            ],
            "recent_runs": [],
            "recent_source_files": [],
        },
    )

    response = client.get("/ops?state=FL")

    assert response.status_code == 200
    assert "Base oficial Florida" in response.text
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
    assert "Muestras operativas" in response.text
    assert "knewhealth.com" in response.text


def test_ops_action_florida_oficial_redirects_with_notice(monkeypatch) -> None:
    app = create_app()
    client = TestClient(app)

    monkeypatch.setattr(
        routes_ops,
        "queue_florida_official_refresh",
        lambda state, *, daily_date: {
            "quarterly_jobs": 20,
            "daily_jobs": 2,
            "daily_date": daily_date.isoformat(),
        },
    )

    response = client.post("/ops/actions/florida-oficial?state=FL", follow_redirects=False)

    assert response.status_code == 303
    assert "/ops?state=FL&notice=" in response.headers["location"]


def test_ops_storage_source_file_renders_html_preview(monkeypatch) -> None:
    app = create_app()
    client = TestClient(app)
    source_file_id = uuid.uuid4()

    monkeypatch.setattr(
        routes_ops,
        "build_source_file_preview",
        lambda object_id: {
            "metadata": {
                "id": str(object_id),
                "state": "FL",
                "filename": "20260408c.txt",
                "source_kind": "daily_corporate",
                "bucket_key": "raw/fl/daily/20260408c.txt",
            },
            "parsed_rows": [
                {
                    "record_no": 1,
                    "document_number": "L26000184490",
                    "company_name": "00 PIZZA LLC",
                    "parse_status": "parsed",
                }
            ],
            "raw_rows": [
                {
                    "member_name": "20260408c.txt",
                    "line_no": 1,
                    "content": "L2600018449000 PIZZA LLC",
                }
            ],
        },
    )

    response = client.get(f"/ops/storage/source-file/{source_file_id}")

    assert response.status_code == 200
    assert "Archivo oficial Florida" in response.text
    assert "00 PIZZA LLC" in response.text
    assert "Descargar archivo crudo" in response.text


def test_ops_storage_download_returns_raw_payload(monkeypatch) -> None:
    app = create_app()
    client = TestClient(app)
    source_file_id = uuid.uuid4()

    monkeypatch.setattr(
        routes_ops,
        "get_storage_object",
        lambda storage_kind, object_id: (
            "20260408c.txt",
            "text/plain; charset=utf-8",
            b"raw payload",
        ),
    )

    response = client.get(f"/ops/storage/source-file/{source_file_id}?download=1")

    assert response.status_code == 200
    assert response.text == "raw payload"
    assert 'attachment; filename="20260408c.txt"' == response.headers["content-disposition"]
