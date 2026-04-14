from __future__ import annotations

import html
import uuid
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, Response

from app.services.ops_console import (
    EXPORT_KIND_VALUES,
    STORAGE_KIND_VALUES,
    build_export_csv_bytes,
    build_ops_dashboard_context,
    get_storage_object,
    list_job_runs,
    list_pending_evidence_rows,
    list_review_queue_rows,
    list_source_files,
    list_sunbiz_artifacts,
    preview_export_rows,
)
from app.services.sample_inspector import inspect_state_samples

router = APIRouter(prefix="/ops", tags=["ops"])


@router.get("", response_class=HTMLResponse)
def ops_dashboard(state: str = Query(default="FL", min_length=2, max_length=2)) -> HTMLResponse:
    context = build_ops_dashboard_context(state)
    cohort_report = context["cohort_report"]
    canary_report = context["canary_report"]

    cards = [
        ("Active Entities", _sum_metric(cohort_report, "active_entities"), "Florida activas."),
        (
            "Pending Domains",
            _sum_metric(cohort_report, "pending_domain_resolution"),
            "Todavia sin dominio verificado.",
        ),
        (
            "Verified Domains",
            _sum_metric(cohort_report, "verified_entities"),
            "Con dominio oficial verificado.",
        ),
        (
            "Website Evidence",
            _sum_metric(cohort_report, "website_contact_observed"),
            "Con contacto web observado.",
        ),
        ("Pending Review", context["pending_review_items"], "Items en review queue."),
        (
            "Pending Evidence",
            context["pending_evidence_review"],
            "Evidencia pendiente de revision.",
        ),
        (
            "Source Files 24h",
            canary_report.source_files_completed,
            "Archivos oficiales completados.",
        ),
        ("Go Ready", "yes" if canary_report.go_ready else "no", "Semaforo operativo."),
    ]

    body = "\n".join(
        [
            _render_header(
                "Ops Console",
                f"Estado {html.escape(str(context['state']))}. Consola operativa para Florida v1.",
            ),
            _render_nav(state),
            _render_card_grid(cards),
            _render_section(
                "Latest Run",
                _render_key_value_list(context["latest_run"] or {"status": "no_runs"}),
            ),
            _render_section(
                "Pending Domain Samples",
                _render_table(context["pending_domain_samples"]),
            ),
            _render_section(
                "Verified Domain Samples",
                _render_table(context["verified_domain_samples"]),
            ),
            _render_section(
                "Website Evidence Samples",
                _render_table(context["website_evidence_samples"]),
            ),
            _render_link_row(
                [
                    ("Runs", _url("/ops/runs", state=state)),
                    ("Review", _url("/ops/review", state=state)),
                    ("Artifacts", _url("/ops/artifacts", state=state)),
                    ("Exports", _url("/ops/exports", state=state)),
                ]
            ),
        ]
    )
    return HTMLResponse(_render_page("Ops Console", body))


@router.get("/entities", response_class=HTMLResponse)
def ops_entities(
    state: str = Query(default="FL", min_length=2, max_length=2),
    kind: str = Query(default="pending-domain"),
    cohort: str = Query(default="priority"),
    limit: int = Query(default=25, ge=1, le=200),
    exclude_fresh: bool = False,
) -> HTMLResponse:
    rows = inspect_state_samples(
        state,
        sample_kind=kind,
        cohort=cohort,
        include_fresh=not exclude_fresh,
        limit=limit,
    )
    body = "\n".join(
        [
            _render_header(
                "Entity Samples",
                f"{html.escape(state.upper())} · {html.escape(kind)} · {html.escape(cohort)}",
            ),
            _render_nav(state),
            _render_table(rows),
        ]
    )
    return HTMLResponse(_render_page("Entity Samples", body))


@router.get("/runs", response_class=HTMLResponse)
def ops_runs(
    state: str = Query(default="FL", min_length=2, max_length=2),
    limit: int = Query(default=50, ge=1, le=200),
) -> HTMLResponse:
    rows = list_job_runs(state, limit=limit)
    body = "\n".join(
        [
            _render_header("Job Runs", f"Ultimos jobs para {html.escape(state.upper())}."),
            _render_nav(state),
            _render_table(rows),
        ]
    )
    return HTMLResponse(_render_page("Job Runs", body))


@router.get("/review", response_class=HTMLResponse)
def ops_review(
    state: str = Query(default="FL", min_length=2, max_length=2),
    limit: int = Query(default=50, ge=1, le=200),
) -> HTMLResponse:
    review_rows = list_review_queue_rows(state, limit=limit)
    evidence_rows = list_pending_evidence_rows(state, limit=limit)
    body = "\n".join(
        [
            _render_header(
                "Review Queue",
                f"Pendientes de revision para {html.escape(state.upper())}.",
            ),
            _render_nav(state),
            _render_section("Queue Items", _render_table(review_rows)),
            _render_section("Evidence Review", _render_table(evidence_rows)),
        ]
    )
    return HTMLResponse(_render_page("Review Queue", body))


@router.get("/artifacts", response_class=HTMLResponse)
def ops_artifacts(
    state: str = Query(default="FL", min_length=2, max_length=2),
    limit: int = Query(default=25, ge=1, le=200),
) -> HTMLResponse:
    source_files = list_source_files(state, limit=limit)
    sunbiz_artifacts = list_sunbiz_artifacts(state, limit=limit)

    for row in source_files:
        if row.get("id") and row.get("bucket_key"):
            row["preview"] = _url("/ops/storage/source-file/" + row["id"])
    for row in sunbiz_artifacts:
        if row.get("id") and row.get("bucket_key"):
            row["preview"] = _url("/ops/storage/sunbiz-artifact/" + row["id"])

    body = "\n".join(
        [
            _render_header(
                "Artifacts",
                f"Source files y artifacts para {html.escape(state.upper())}.",
            ),
            _render_nav(state),
            _render_section("Source Files", _render_table(source_files)),
            _render_section("Sunbiz Artifacts", _render_table(sunbiz_artifacts)),
        ]
    )
    return HTMLResponse(_render_page("Artifacts", body))


@router.get("/exports", response_class=HTMLResponse)
def ops_exports(
    state: str = Query(default="FL", min_length=2, max_length=2),
    cohort: str = Query(default="priority"),
    limit: int = Query(default=25, ge=1, le=200),
    exclude_fresh: bool = False,
) -> HTMLResponse:
    include_fresh = not exclude_fresh
    identities = preview_export_rows(
        "identities",
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
        limit=limit,
    )
    contacts = preview_export_rows(
        "contacts",
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
        limit=limit,
    )
    identity_csv = _url("/ops/exports/identities.csv", state=state, cohort=cohort)
    contact_csv = _url("/ops/exports/contacts.csv", state=state, cohort=cohort)
    if exclude_fresh:
        identity_csv += "&exclude_fresh=1"
        contact_csv += "&exclude_fresh=1"

    body = "\n".join(
        [
            _render_header("Exports", f"Preview y descarga para {html.escape(state.upper())}."),
            _render_nav(state),
            _render_link_row(
                [
                    ("Download identities.csv", identity_csv),
                    ("Download contacts.csv", contact_csv),
                ]
            ),
            _render_section("Identity Preview", _render_table(identities)),
            _render_section("Contact Preview", _render_table(contacts)),
        ]
    )
    return HTMLResponse(_render_page("Exports", body))


@router.get("/exports/{export_kind}.csv")
def ops_export_csv(
    export_kind: str,
    state: str = Query(default="FL", min_length=2, max_length=2),
    cohort: str = Query(default="priority"),
    exclude_fresh: bool = False,
) -> Response:
    if export_kind not in EXPORT_KIND_VALUES:
        raise HTTPException(status_code=404, detail="Export kind not found.")
    filename, payload = build_export_csv_bytes(
        export_kind,
        state=state,
        cohort=cohort,
        include_fresh=not exclude_fresh,
    )
    return Response(
        content=payload,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/storage/{storage_kind}/{object_id}")
def ops_storage_object(storage_kind: str, object_id: uuid.UUID) -> Response:
    if storage_kind not in STORAGE_KIND_VALUES:
        raise HTTPException(status_code=404, detail="Storage object not found.")
    try:
        filename, media_type, payload = get_storage_object(storage_kind, object_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return Response(
        content=payload,
        media_type=media_type,
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


def _render_page(title: str, body: str) -> str:
    css = """
    :root {
      --bg: #f4efe7;
      --card: #fffaf3;
      --ink: #1e1b16;
      --muted: #6f6558;
      --line: #d8ccbc;
      --accent: #204c3c;
      --accent-soft: #e2f1e9;
      --warn: #8c3d12;
      --mono: "SFMono-Regular", "Menlo", monospace;
      --sans: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 32px;
      background:
        radial-gradient(circle at top left, rgba(32, 76, 60, 0.08), transparent 32%),
        linear-gradient(180deg, #f7f1e8 0%, #f1e8dc 100%);
      color: var(--ink);
      font-family: var(--sans);
    }
    main {
      max-width: 1380px;
      margin: 0 auto;
      display: grid;
      gap: 24px;
    }
    .panel {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 24px;
      box-shadow: 0 16px 48px rgba(34, 25, 12, 0.07);
    }
    h1, h2, h3 { margin: 0 0 8px; }
    p, li, td, th, a, span { font-size: 15px; }
    .muted { color: var(--muted); }
    .nav, .links {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .nav a, .links a {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--ink);
      text-decoration: none;
      background: #fff;
    }
    .nav a:hover, .links a:hover { border-color: var(--accent); color: var(--accent); }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 16px;
    }
    .card {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 18px;
    }
    .card .label {
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 12px;
    }
    .card .value { font-size: 30px; font-weight: 700; margin: 10px 0 6px; }
    table {
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: #fff;
    }
    th, td {
      padding: 12px 14px;
      border-bottom: 1px solid #efe4d6;
      text-align: left;
      vertical-align: top;
    }
    th {
      background: #f7efe4;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }
    td code, pre {
      font-family: var(--mono);
      font-size: 12px;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .empty {
      padding: 18px;
      border: 1px dashed var(--line);
      border-radius: 16px;
      color: var(--muted);
      background: #fffdfa;
    }
    .stack { display: grid; gap: 16px; }
    """
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(title)}</title>
    <style>{css}</style>
  </head>
  <body>
    <main>{body}</main>
  </body>
</html>"""


def _render_header(title: str, subtitle: str) -> str:
    return (
        '<section class="panel">'
        f"<h1>{html.escape(title)}</h1>"
        f'<p class="muted">{html.escape(subtitle)}</p>'
        "</section>"
    )


def _render_nav(state: str) -> str:
    links = [
        ("Dashboard", _url("/ops", state=state)),
        ("Entities", _url("/ops/entities", state=state)),
        ("Runs", _url("/ops/runs", state=state)),
        ("Review", _url("/ops/review", state=state)),
        ("Artifacts", _url("/ops/artifacts", state=state)),
        ("Exports", _url("/ops/exports", state=state)),
    ]
    return '<section class="panel"><nav class="nav">' + "".join(
        f'<a href="{html.escape(href)}">{html.escape(label)}</a>' for label, href in links
    ) + "</nav></section>"


def _render_card_grid(cards: list[tuple[str, object, str]]) -> str:
    body = "".join(
        (
            '<article class="card">'
            f'<div class="label">{html.escape(label)}</div>'
            f'<div class="value">{html.escape(str(value))}</div>'
            f'<div class="muted">{html.escape(description)}</div>'
            "</article>"
        )
        for label, value, description in cards
    )
    return f'<section class="panel"><div class="cards">{body}</div></section>'


def _render_section(title: str, content: str) -> str:
    return f'<section class="panel stack"><h2>{html.escape(title)}</h2>{content}</section>'


def _render_link_row(items: list[tuple[str, str]]) -> str:
    return '<section class="panel"><div class="links">' + "".join(
        f'<a href="{html.escape(href)}">{html.escape(label)}</a>' for label, href in items
    ) + "</div></section>"


def _render_key_value_list(payload: dict[str, object]) -> str:
    return _render_table([payload])


def _render_table(rows: list[dict[str, object]]) -> str:
    if not rows:
        return '<div class="empty">No rows.</div>'

    headers = list(rows[0].keys())
    head = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        cells = []
        for header in headers:
            value = row.get(header)
            if isinstance(value, str) and value.startswith(("http://", "https://", "/ops/")):
                rendered = f'<a href="{html.escape(value)}">{html.escape(value)}</a>'
            else:
                rendered = f"<code>{html.escape(str(value))}</code>"
            cells.append(f"<td>{rendered}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return (
        "<table>"
        f"<thead><tr>{head}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )


def _sum_metric(report, field_name: str) -> int:
    return sum(getattr(metrics, field_name) for metrics in report.cohorts.values())


def _url(path: str, **params: object) -> str:
    compact = {key: value for key, value in params.items() if value not in {None, ""}}
    if not compact:
        return path
    return f"{path}?{urlencode(compact)}"
