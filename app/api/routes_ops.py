from __future__ import annotations

import html
import uuid
from datetime import date
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from app.services.ops_actions import (
    queue_domain_enrichment,
    queue_florida_daily_refresh,
    queue_florida_official_refresh,
    queue_florida_quarterly_refresh,
    queue_verified_contact_collection,
)
from app.services.ops_console import (
    EXPORT_KIND_VALUES,
    STORAGE_KIND_VALUES,
    build_export_csv_bytes,
    build_ops_dashboard_context,
    describe_export,
    get_storage_object,
    list_job_runs,
    list_pending_evidence_rows,
    list_review_queue_rows,
    list_source_files,
    list_sunbiz_artifacts,
)
from app.services.sample_inspector import inspect_state_samples

router = APIRouter(prefix="/ops", tags=["ops"])

DISPLAY_HEADER_LABELS = {
    "active_entities": "Entidades activas",
    "archivos_completados": "Archivos completados",
    "bucket_key": "Bucket key",
    "cohort": "Cohorte",
    "confidence": "Confianza",
    "connector_kind": "Conector",
    "contact_form_url": "Formulario de contacto",
    "contact_page_url": "Pagina de contacto",
    "created_at": "Creado",
    "daily_jobs": "Jobs daily",
    "domain_status": "Estado dominio",
    "downloaded_at": "Descargado",
    "evidence_kind": "Tipo evidencia",
    "evidence_scope": "Clasificacion",
    "external_filing_id": "ID oficial",
    "fei_number": "FEI",
    "file_date": "Fecha fuente",
    "filename": "Archivo",
    "filing_type": "Tipo filing",
    "finished_at": "Finalizado",
    "first_seen_at": "Primera vez visto",
    "homepage_url": "Homepage",
    "id": "ID",
    "last_seen_at": "Ultima vez visto",
    "last_transaction_date": "Ultima transaccion",
    "latest_report_date": "Ultimo reporte",
    "latest_report_year": "Ano ultimo reporte",
    "legal_name": "Nombre legal",
    "lista_shards": "Lista shards",
    "mail_address_1": "Mail direccion 1",
    "mail_address_2": "Mail direccion 2",
    "mail_city": "Mail ciudad",
    "mail_state": "Mail estado",
    "mail_zip": "Mail zip",
    "more_than_six_officers": "Mas de 6 officers",
    "notes": "Notas",
    "observed_at": "Observado",
    "officers_count": "Cantidad officers",
    "officers_json": "Officers",
    "payload": "Payload",
    "pending_review_items": "Review pendiente",
    "principal_address_1": "Principal direccion 1",
    "principal_address_2": "Principal direccion 2",
    "principal_city": "Principal ciudad",
    "principal_postal_code": "Principal codigo postal",
    "principal_state": "Principal estado",
    "processed_at": "Procesado",
    "quarterly_jobs": "Jobs quarterly",
    "quarterly_shard": "Shard quarterly",
    "queue_kind": "Tipo queue",
    "queued_jobs": "Jobs encolados",
    "razon": "Razon",
    "registros_totales": "Registros totales",
    "registered_agent_address": "Direccion agente registrado",
    "registered_agent_city": "Ciudad agente registrado",
    "registered_agent_name": "Agente registrado",
    "registered_agent_state": "Estado agente registrado",
    "registered_agent_zip": "Zip agente registrado",
    "row_count": "Filas",
    "shards_completados": "Shards completados",
    "source_checksum": "Checksum fuente",
    "source_kind": "Tipo fuente",
    "source_uri": "Fuente",
    "source_url": "URL fuente",
    "started_at": "Iniciado",
    "state": "Estado",
    "stats": "Metricas",
    "status": "Estado",
    "summary_rows": "Resumen",
    "tipo_evidencia": "Tipo evidencia",
    "tipo_queue": "Tipo queue",
    "total_records": "Registros",
    "ultima_file_date": "Ultima fecha fuente",
    "ultimo_downloaded_at": "Ultima descarga",
    "updated_at": "Actualizado",
    "valor": "Valor",
    "verified_domain": "Dominio verificado",
    "verified_homepage_url": "Homepage verificada",
    "primary_email": "Email primario",
}


@router.get("", response_class=HTMLResponse)
def ops_dashboard(
    state: str = Query(default="FL", min_length=2, max_length=2),
    notice: str | None = Query(default=None),
) -> HTMLResponse:
    context = build_ops_dashboard_context(state)
    cohort_report = context["cohort_report"]
    source_summary = context.get(
        "source_summary",
        {
            "active_entities": _sum_metric(cohort_report, "active_entities"),
            "current_snapshots": _sum_metric(cohort_report, "active_entities"),
            "quarterly_corporate_completed_shards": 0,
            "quarterly_event_completed_shards": 0,
            "latest_daily_corporate_date": None,
            "summary_rows": [],
        },
    )
    canary_report = context["canary_report"]

    cards = [
        (
            "Entidades oficiales activas",
            source_summary["active_entities"],
            "Base oficial visible hoy en la consola.",
        ),
        (
            "Snapshots oficiales",
            source_summary["current_snapshots"],
            "Snapshots cargados desde el bulk oficial.",
        ),
        (
            "Quarterly corporativo",
            f"{source_summary['quarterly_corporate_completed_shards']}/10",
            "Shards corporativos completos.",
        ),
        (
            "Quarterly eventos",
            f"{source_summary['quarterly_event_completed_shards']}/10",
            "Shards de eventos completos.",
        ),
        (
            "Dominios verificados",
            _sum_metric(cohort_report, "verified_entities"),
            "Empresas con dominio oficial verificado.",
        ),
        (
            "Contactos web observados",
            _sum_metric(cohort_report, "website_contact_observed"),
            "Empresas con contacto publico observado.",
        ),
        (
            "Ultimo daily corporativo",
            source_summary["latest_daily_corporate_date"] or "pendiente",
            "Fecha mas reciente cargada para daily corporativo.",
        ),
        (
            "Go ready",
            "si" if canary_report.go_ready else "no",
            "Semaforo tecnico del pipeline.",
        ),
    ]

    body_parts = [
        _render_header(
            "Base oficial Florida",
            (
                f"Estado {html.escape(str(context['state']))}. "
                "Primero base oficial; despues enriquecimiento web."
            ),
        ),
        _render_notice(notice),
        _render_nav(state),
        _render_action_panel(state),
        _render_card_grid(cards),
        _render_section(
            "Estado de carga oficial",
            _render_table(source_summary["summary_rows"]),
        ),
        _render_section(
            "Ultimo job",
            _render_key_value_list(context["latest_run"] or {"status": "sin_jobs"}),
        ),
        _render_section(
            "Vista previa base oficial",
            _render_table(context["empresas_preview"]),
        ),
        _render_section(
            "Vista previa contactos primarios",
            _render_table(context["contactos_primary_preview"]),
        ),
        _render_section(
            "Archivos oficiales recientes",
            _render_table(context["recent_source_files"]),
        ),
    ]
    body = "\n".join(part for part in body_parts if part)
    return HTMLResponse(_render_page("Base oficial Florida", body))


@router.post("/actions/florida-oficial")
def ops_action_florida_oficial(
    state: str = Query(default="FL", min_length=2, max_length=2),
) -> RedirectResponse:
    try:
        result = queue_florida_official_refresh(state, daily_date=date.today())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    notice = (
        f"Florida oficial encolada: {result['quarterly_jobs']} jobs quarterly y "
        f"{result['daily_jobs']} jobs daily para {result['daily_date']}."
    )
    return _redirect_dashboard(state, notice)


@router.post("/actions/florida-quarterly")
def ops_action_florida_quarterly(
    state: str = Query(default="FL", min_length=2, max_length=2),
    quarterly_shard: int | None = Query(default=None, ge=0, le=9),
) -> RedirectResponse:
    try:
        result = queue_florida_quarterly_refresh(state, quarterly_shard=quarterly_shard)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if quarterly_shard is None:
        notice = f"Quarterly Florida encolado: {result['queued_jobs']} jobs para shards 0..9."
    else:
        notice = f"Quarterly Florida shard {quarterly_shard} encolado."
    return _redirect_dashboard(state, notice)


@router.post("/actions/florida-daily")
def ops_action_florida_daily(
    state: str = Query(default="FL", min_length=2, max_length=2),
    file_date: Annotated[date | None, Query()] = None,
) -> RedirectResponse:
    selected_date = file_date or date.today()
    try:
        result = queue_florida_daily_refresh(state, file_date=selected_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    notice = f"Daily Florida encolado: {result['queued_jobs']} jobs para {result['file_date']}."
    return _redirect_dashboard(state, notice)


@router.post("/actions/enriquecer-contactos")
def ops_action_enriquecer_contactos(
    state: str = Query(default="FL", min_length=2, max_length=2),
    cohort: str = Query(default="priority"),
    include_fresh: bool = Query(default=True),
) -> RedirectResponse:
    result = queue_domain_enrichment(state, cohort=cohort, include_fresh=include_fresh)
    scope = "incluyendo fresh" if include_fresh else "sin fresh"
    notice = f"Resolver dominios encolado para cohorte {result['cohort']} ({scope})."
    return _redirect_dashboard(state, notice)


@router.post("/actions/recolectar-contactos")
def ops_action_recolectar_contactos(
    state: str = Query(default="FL", min_length=2, max_length=2),
    limit: int = Query(default=250, ge=1, le=5000),
    cohort: str = Query(default="priority"),
    include_fresh: bool = Query(default=True),
) -> RedirectResponse:
    result = queue_verified_contact_collection(
        state,
        limit=limit,
        cohort=cohort,
        include_fresh=include_fresh,
    )
    scope = "incluyendo fresh" if include_fresh else "sin fresh"
    notice = (
        f"Recolectar evidencia encolado para dominios verificados: "
        f"cohorte {result['cohort']} ({scope}), limite {result['limit']}."
    )
    return _redirect_dashboard(state, notice)


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
                "Muestras operativas",
                f"{html.escape(state.upper())} · {html.escape(kind)} · {html.escape(cohort)}",
            ),
            _render_nav(state),
            _render_link_row(
                [
                    (
                        "Pendientes dominio",
                        _url("/ops/entities", state=state, kind="pending-domain", cohort=cohort),
                    ),
                    (
                        "Dominios verificados",
                        _url("/ops/entities", state=state, kind="verified-domain", cohort=cohort),
                    ),
                    (
                        "Evidencia web",
                        _url("/ops/entities", state=state, kind="website-evidence", cohort=cohort),
                    ),
                ]
            ),
            _render_table(rows),
        ]
    )
    return HTMLResponse(_render_page("Muestras operativas", body))


@router.get("/runs", response_class=HTMLResponse)
def ops_runs(
    state: str = Query(default="FL", min_length=2, max_length=2),
    limit: int = Query(default=50, ge=1, le=200),
) -> HTMLResponse:
    rows = list_job_runs(state, limit=limit)
    body = "\n".join(
        [
            _render_header("Ejecuciones", f"Ultimos jobs para {html.escape(state.upper())}."),
            _render_nav(state),
            _render_table(rows),
        ]
    )
    return HTMLResponse(_render_page("Ejecuciones", body))


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
                "Revision",
                f"Pendientes de revision para {html.escape(state.upper())}.",
            ),
            _render_nav(state),
            _render_section("Queue de revision", _render_table(review_rows)),
            _render_section("Evidencia pendiente", _render_table(evidence_rows)),
        ]
    )
    return HTMLResponse(_render_page("Revision", body))


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
                "Artefactos",
                f"Archivos oficiales y artefactos para {html.escape(state.upper())}.",
            ),
            _render_nav(state),
            _render_section("Source files", _render_table(source_files)),
            _render_section("Sunbiz lane B", _render_table(sunbiz_artifacts)),
        ]
    )
    return HTMLResponse(_render_page("Artefactos", body))


@router.get("/exports", response_class=HTMLResponse)
def ops_exports(
    state: str = Query(default="FL", min_length=2, max_length=2),
    cohort: str = Query(default="priority"),
    limit: int = Query(default=25, ge=1, le=200),
    exclude_fresh: bool = False,
) -> HTMLResponse:
    include_fresh = not exclude_fresh
    base_oficial = describe_export(
        "base_oficial",
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
        limit=limit,
    )
    empresas = describe_export(
        "empresas",
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
        limit=limit,
    )
    contactos_primary = describe_export(
        "contactos_primary",
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
        limit=limit,
    )
    contactos_evidence = describe_export(
        "contactos_evidence",
        state=state,
        cohort=cohort,
        include_fresh=include_fresh,
        limit=limit,
    )

    export_links = [
        (
            "Descargar base_oficial.csv",
            _url(
                "/ops/exports/base_oficial.csv",
                state=state,
                cohort=cohort,
                exclude_fresh=int(exclude_fresh),
            ),
        ),
        (
            "Descargar empresas.csv",
            _url(
                "/ops/exports/empresas.csv",
                state=state,
                cohort=cohort,
                exclude_fresh=int(exclude_fresh),
            ),
        ),
        (
            "Descargar contactos_primary.csv",
            _url(
                "/ops/exports/contactos_primary.csv",
                state=state,
                cohort=cohort,
                exclude_fresh=int(exclude_fresh),
            ),
        ),
        (
            "Descargar contactos_evidence.csv",
            _url(
                "/ops/exports/contactos_evidence.csv",
                state=state,
                cohort=cohort,
                exclude_fresh=int(exclude_fresh),
            ),
        ),
    ]

    body = "\n".join(
        [
            _render_header(
                "Exportaciones",
                f"Previews y descargas para {html.escape(state.upper())}.",
            ),
            _render_nav(state),
            _render_link_row(export_links),
            _render_export_preview("Base oficial", base_oficial),
            _render_export_preview("Empresas canonicas", empresas),
            _render_export_preview("Contactos primarios", contactos_primary),
            _render_export_preview("Evidencia de contacto", contactos_evidence),
        ]
    )
    return HTMLResponse(_render_page("Exportaciones", body))


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


def _redirect_dashboard(state: str, notice: str) -> RedirectResponse:
    return RedirectResponse(_url("/ops", state=state, notice=notice), status_code=303)


def _render_page(title: str, body: str) -> str:
    css = """
    :root {
      --bg: #f3f3f3;
      --card: #ffffff;
      --ink: #111111;
      --muted: #6a6a6a;
      --line: #d8d8d8;
      --accent: #111111;
      --accent-soft: #f3f3f3;
      --warn: #111111;
      --warn-soft: #f3f3f3;
      --mono: "SFMono-Regular", "Menlo", monospace;
      --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 32px;
      background: var(--bg);
      color: var(--ink);
      font-family: var(--sans);
    }
    main {
      max-width: 1440px;
      margin: 0 auto;
      display: grid;
      gap: 24px;
    }
    .panel {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 24px;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
    }
    h1, h2, h3 { margin: 0 0 8px; }
    p, li, td, th, a, span, button { font-size: 15px; }
    .muted { color: var(--muted); }
    .nav, .links, .actions, .shards {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .nav a, .links a, .button-link, button {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--ink);
      text-decoration: none;
      background: #fff;
      cursor: pointer;
      font-family: inherit;
      transition: background 120ms ease, color 120ms ease, border-color 120ms ease;
    }
    button.primary { background: var(--accent); color: #fff; border-color: var(--accent); }
    button.secondary { background: #fff; color: var(--ink); }
    .nav a:hover, .links a:hover, button:hover {
      border-color: var(--accent);
      background: #f7f7f7;
      color: var(--ink);
    }
    button.primary:hover {
      color: #fff;
      background: #000;
      border-color: #000;
    }
    form { margin: 0; }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 16px;
    }
    .card {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 16px;
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
      border-radius: 14px;
      border: 1px solid var(--line);
      background: #fff;
    }
    th, td {
      padding: 12px 14px;
      border-bottom: 1px solid #ededed;
      text-align: left;
      vertical-align: top;
    }
    th {
      background: #f7f7f7;
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
      border-radius: 14px;
      color: var(--muted);
      background: #fcfcfc;
    }
    .stack { display: grid; gap: 16px; }
    .notice {
      background: var(--warn-soft);
      border: 1px solid var(--line);
      color: var(--warn);
    }
    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .meta span {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 12px;
      background: #fff;
    }
    """
    return f"""<!doctype html>
<html lang="es">
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


def _render_notice(notice: str | None) -> str:
    if not notice:
        return ""
    return (
        '<section class="panel notice">'
        f"<strong>{html.escape(notice)}</strong>"
        "</section>"
    )


def _render_nav(state: str) -> str:
    links = [
        ("Base oficial", _url("/ops", state=state)),
        ("Muestras", _url("/ops/entities", state=state)),
        ("Ejecuciones", _url("/ops/runs", state=state)),
        ("Revision", _url("/ops/review", state=state)),
        ("Artefactos", _url("/ops/artifacts", state=state)),
        ("Exportaciones", _url("/ops/exports", state=state)),
    ]
    return '<section class="panel"><nav class="nav">' + "".join(
        f'<a href="{html.escape(href)}">{html.escape(label)}</a>' for label, href in links
    ) + "</nav></section>"


def _render_action_panel(state: str) -> str:
    shard_buttons = "".join(
        _render_action_form(
            label=f"Shard {shard}",
            action=_url("/ops/actions/florida-quarterly", state=state, quarterly_shard=shard),
            primary=False,
        )
        for shard in range(10)
    )
    return (
        '<section class="panel stack">'
        "<h2>Acciones</h2>"
        + '<div class="actions">'
        + _render_action_form(
            "Cargar Florida oficial",
            _url("/ops/actions/florida-oficial", state=state),
            primary=True,
        )
        + _render_action_form(
            "Ejecutar quarterly 0..9",
            _url("/ops/actions/florida-quarterly", state=state),
            primary=False,
        )
        + _render_action_form(
            "Ejecutar daily de hoy",
            _url("/ops/actions/florida-daily", state=state),
            primary=False,
        )
        + _render_action_form(
            "Enriquecer contactos",
            _url(
                "/ops/actions/enriquecer-contactos",
                state=state,
                cohort="priority",
                include_fresh=1,
            ),
            primary=False,
        )
        + _render_action_form(
            "Recolectar sobre verificados",
            _url(
                "/ops/actions/recolectar-contactos",
                state=state,
                cohort="priority",
                include_fresh=1,
                limit=250,
            ),
            primary=False,
        )
        + "</div>"
        + "<h3>Quarterly por shard</h3>"
        + f'<div class="shards">{shard_buttons}</div>'
        + "</section>"
    )


def _render_action_form(label: str, action: str, *, primary: bool) -> str:
    class_name = "primary" if primary else "secondary"
    return (
        f'<form method="post" action="{html.escape(action)}">'
        f'<button class="{class_name}" type="submit">{html.escape(label)}</button>'
        "</form>"
    )


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


def _render_export_preview(title: str, payload: dict[str, object]) -> str:
    meta = (
        '<div class="meta">'
        f"<span>Filas: {html.escape(str(payload['row_count']))}</span>"
        f"<span>Columnas: {html.escape(str(len(payload['columns'])))}</span>"
        "</div>"
    )
    return _render_section(title, meta + _render_table(payload["rows"]))


def _render_key_value_list(payload: dict[str, object]) -> str:
    return _render_table([payload])


def _render_table(rows: list[dict[str, object]]) -> str:
    if not rows:
        return '<div class="empty">Sin filas.</div>'

    headers = list(rows[0].keys())
    head = "".join(
        (
            "<th>"
            f"{html.escape(DISPLAY_HEADER_LABELS.get(header, header.replace('_', ' ').title()))}"
            "</th>"
        )
        for header in headers
    )
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
    compact = {
        key: value
        for key, value in params.items()
        if value is not None and value != "" and value is not False
    }
    if not compact:
        return path
    return f"{path}?{urlencode(compact)}"
