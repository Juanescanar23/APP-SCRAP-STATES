#!/bin/sh
set -eu

exec uv run dramatiq \
  --processes "${DRAMATIQ_PROCESSES:-2}" \
  --threads "${DRAMATIQ_THREADS:-4}" \
  --queues fl_download fl_import fl_normalize fl_sunbiz_harvest \
  app.workers.broker:broker \
  app.workers.tasks_download \
  app.workers.tasks_import \
  app.workers.tasks_normalize \
  app.workers.tasks_sunbiz
