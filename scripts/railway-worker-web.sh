#!/bin/sh
set -eu

exec uv run dramatiq \
  --processes "${DRAMATIQ_PROCESSES:-2}" \
  --threads "${DRAMATIQ_THREADS:-4}" \
  app.workers.broker:broker \
  app.workers.tasks_domains \
  app.workers.tasks_evidence \
  --queues domain_resolve website_contact_collect
