#!/usr/bin/env bash
# Starts the whole stack and applies .env to volumes that already exist: database accounts and their
# passwords, the Grafana admin password and the connector config. Every step is idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -s .env || ! -s secrets/postgres_password ]]; then
  echo 'Create .env and secrets/ first: ./setup-env.sh' >&2
  exit 1
fi
if [[ ! -s observability/secrets/google-chat-webhook ]]; then
  echo 'Create observability/secrets/google-chat-webhook containing the Google Chat webhook URL first.' >&2
  exit 1
fi
docker compose up -d --wait postgres kafka clickhouse debezium
# Init scripts only run on a fresh volume
docker compose exec -T postgres psql -U admin -d enterprise_db -v ON_ERROR_STOP=1 -q < observability/setup-postgres.sql
docker compose exec -T postgres psql -U admin -d enterprise_db -v ON_ERROR_STOP=1 -q < source/postgres/roles.sql
docker compose exec -T postgres sh /docker-entrypoint-initdb.d/03-passwords.sh
docker compose exec -T postgres sh /docker-entrypoint-initdb.d/04-identity.sh
docker compose exec -T clickhouse bash /docker-entrypoint-initdb.d/06-monitoring.sh
for sql in 07-snapshots.sql 08-cold-storage.sql; do
  docker compose exec -T clickhouse bash -c 'clickhouse-client --user admin --password "$(cat /run/secrets/clickhouse_password)" --multiquery' \
    < "warehouse/clickhouse/init/$sql"
done
debezium/register.sh
docker compose up -d --build identity source-api simulator warehouse-jobs control-center gateway prometheus alertmanager grafana platform-exporter kafka-exporter node-exporter chat-webhook seaweedfs
docker compose exec -T grafana sh -c 'grafana cli admin reset-admin-password "$(cat /run/secrets/grafana_admin_password)"' >/dev/null
