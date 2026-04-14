# bizintel

Pipeline interno para ingestar registros empresariales públicos, resolver dominios oficiales y recolectar evidencia pública de contacto con trazabilidad completa.

## Stack

- `uv` para entorno y dependencias.
- `FastAPI` para la API interna.
- `SQLAlchemy 2` + `Alembic` para modelos y migraciones.
- `PostgreSQL` + `psycopg 3` para persistencia.
- `Redis` + `Dramatiq` para workers y reintentos.
- `HTTPX` + `selectolax` para fetch y parse.
- `Playwright` queda reservado como fallback fuera de este scaffold.

## Estructura

- `app/api`: rutas FastAPI para salud, entidades y review.
- `app/connectors`: conectores de archivos, HTML y API.
- `app/db`: modelos, sesión y migraciones.
- `app/services`: normalización, scoring, robots y collector.
- `app/workers`: broker y tareas del pipeline.
- `tests`: pruebas unitarias mínimas.

## Arranque local

```bash
cp .env.example .env
uv sync --group dev
docker-compose up -d postgres redis
uv run alembic upgrade head
uv run pytest -q
uv run python -m app.cli resolve-domains --state FL --limit 50 --dry-run
uv run python -m app.cli collect-evidence --state FL --limit 50 --verified-only --dry-run
uv run python -m app.cli report-canary --state FL --hours 24
uv run uvicorn app.main:app --reload
```

Notas:

- PostgreSQL expone `127.0.0.1:55432` para evitar colisión con instalaciones locales que ya usen `5432`.
- Si corres esto dentro de Codex CLI y `uv` no puede escribir en `~/.cache/uv`, usa `UV_CACHE_DIR=/tmp/uvcache` delante de los comandos de `uv`.
- La prueba real contra Brave vive detrás de `BIZINTEL_BRAVE_SEARCH_API_KEY`; si no está configurada, `pytest` la salta.

Worker:

```bash
uv run dramatiq app.workers.tasks_download app.workers.tasks_import app.workers.tasks_normalize app.workers.tasks_sunbiz app.workers.tasks_domains app.workers.tasks_evidence
```

## Railway

- El repo ya trae configuración lista para Railway en `railway/api/railway.json`, `railway/worker-state/railway.json` y `railway/worker-web/railway.json`.
- El contenedor también soporta `BIZINTEL_SERVICE_ROLE` para reutilizar la misma imagen en varios servicios Railway:
  - `api`
  - `worker-state`
  - `worker-web`
  - `migrate`
- `scripts/railway-api.sh` arranca FastAPI usando `PORT`.
- `scripts/railway-migrate.sh` corre `alembic upgrade head`.
- `scripts/railway-worker-state.sh` consume `fl_download`, `fl_import`, `fl_normalize` y `fl_sunbiz_harvest`.
- `scripts/railway-worker-web.sh` consume `domain_resolve` y `website_contact_collect`.
- `app.core.config` acepta variables nativas de Railway para Postgres/Redis/Bucket además de `BIZINTEL_*`: `DATABASE_URL`, `REDIS_URL`, `ENDPOINT`, `BUCKET`, `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `REGION`.

Servicios recomendados en Railway:

- `api`: config path `/railway/api/railway.json`
- `worker-state`: config path `/railway/worker-state/railway.json`
- `worker-web`: config path `/railway/worker-web/railway.json`
- `postgres`, `redis`, `bucket`: recursos gestionados por Railway

Variables mínimas:

- `BIZINTEL_ENV=staging`
- `BIZINTEL_SEARCH_PROVIDER=none` para el canario oficial de Florida
- `BIZINTEL_USER_AGENT=bizintel-bot/0.1`
- `BIZINTEL_FL_BASE_URL=https://sftp.floridados.gov`
- `BIZINTEL_FL_SUNBIZ_SEARCH_BASE_URL=https://search.sunbiz.org`
- `BIZINTEL_FL_DOWNLOAD_TIMEOUT_SECONDS=60`
- `BIZINTEL_FL_DOWNLOAD_RETRIES=3`
- `BIZINTEL_FL_PDF_RETRY_DAYS=5`

## Notas de arquitectura

- La staging es append-only y trazable por `job_run_id` + `source_checksum`.
- Florida ya no depende de guardar el raw completo por fila en Postgres: `source_file` y `source_record_ref` guardan trazabilidad, mientras `company_registry_snapshot` y `company_event` guardan el dato canónico por archivo.
- `source_file` usa identidad fuerte por checksum más clave lógica de archivo; eso evita colisiones entre quarterly `cordata.zip` de distintos cortes y mantiene replay idempotente.
- `source_ingest_cursor` evita reprocesar feeds oficiales de Florida y `sunbiz_artifact` guarda HTML/PDF auditables con retry state.
- `BusinessEntity`, `OfficialDomain` y `ContactEvidence` están separados por diseño.
- Para Florida, el flujo correcto ahora es `source file -> record refs -> snapshots/eventos -> business_entity -> domain resolution -> public contact evidence`.
- La operación real ya contempla `fl_download` para feeds oficiales y `fl_sunbiz_harvest` para expediente HTML/PDF antes de salir a web abierta.
- `report-canary` separa `html_hit_rate`, `pdf_hit_rate_mature_cohort` y `pdf_pending_rate` para que la validación Sunbiz no mezcle cohortes recientes con cohortes maduras.
- El collector solo opera sobre dominios verificados y páginas públicas allowlisted.
- La evidencia pública conserva `source_url`, `source_hash` y estado de revisión.
