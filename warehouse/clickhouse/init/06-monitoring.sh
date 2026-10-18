#!/bin/bash
# Read-only users for Grafana and the platform exporter, passwords from the secret files. Runs at init on a
# fresh volume; ./observability/start.sh runs it again on an existing one.
GRAFANA_READER_PASSWORD=$(cat /run/secrets/grafana_reader_password)
MONITOR_PASSWORD=$(cat /run/secrets/monitor_password)
clickhouse-client --host 127.0.0.1 --user "$CLICKHOUSE_USER" --password "$(cat /run/secrets/clickhouse_password)" --multiquery <<SQL
CREATE USER IF NOT EXISTS grafana_reader IDENTIFIED WITH sha256_password BY '${GRAFANA_READER_PASSWORD}';
ALTER USER grafana_reader IDENTIFIED WITH sha256_password BY '${GRAFANA_READER_PASSWORD}';
GRANT SELECT ON mart.* TO grafana_reader;
GRANT SELECT ON ops.mart_refreshes TO grafana_reader;
ALTER USER grafana_reader SETTINGS readonly = 1, max_execution_time CHANGEABLE_IN_READONLY;
CREATE USER IF NOT EXISTS monitor_reader IDENTIFIED WITH sha256_password BY '${MONITOR_PASSWORD}';
ALTER USER monitor_reader IDENTIFIED WITH sha256_password BY '${MONITOR_PASSWORD}';
GRANT SELECT ON system.* TO monitor_reader;
GRANT SELECT ON bronze.* TO monitor_reader;
GRANT SELECT ON silver.* TO monitor_reader;
GRANT SELECT ON consistent.* TO monitor_reader;
GRANT SELECT ON ops.* TO monitor_reader;
ALTER USER monitor_reader SETTINGS readonly = 1;
SQL
