#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -s observability/secrets/google-chat-webhook ]]; then
  echo 'Create observability/secrets/google-chat-webhook containing the Google Chat webhook URL first.' >&2
  exit 1
fi
# Existing database volumes do not rerun init SQL. These grants are idempotent.
docker compose up -d --wait postgres kafka clickhouse debezium
docker compose exec -T clickhouse clickhouse-client --user admin --password admin --multiquery < warehouse/clickhouse/init/06-monitoring.sql
docker compose exec -T postgres psql -U admin -d enterprise_db -v ON_ERROR_STOP=1 < observability/setup-postgres.sql
docker compose up -d --build source-api simulator warehouse-jobs control-center prometheus alertmanager grafana platform-exporter kafka-exporter node-exporter chat-webhook
