#!/bin/sh
set -eu

role="${BIZINTEL_SERVICE_ROLE:-api}"

case "$role" in
  api)
    exec sh scripts/railway-api.sh
    ;;
  worker-state)
    exec sh scripts/railway-worker-state.sh
    ;;
  worker-web)
    exec sh scripts/railway-worker-web.sh
    ;;
  migrate)
    exec sh scripts/railway-migrate.sh
    ;;
  *)
    echo "Unsupported BIZINTEL_SERVICE_ROLE: $role" >&2
    exit 1
    ;;
esac
