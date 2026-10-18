#!/usr/bin/env bash
# One-off for warehouses created before R9: Kafka engine tables read RawBLOB and strip the Schema Registry header.
# Drops the Kafka tables and their views only while they still use JSONAsString, then re-runs the init SQL (every
# statement there is IF NOT EXISTS). Consumer group offsets live in Kafka, so ingestion resumes where it stopped.
# Must run before Debezium switches to the JSON Schema converter: JSONAsString cannot read the header.
set -euo pipefail
cd "$(dirname "$0")/../.."
ch() { docker compose exec -T clickhouse bash -c 'clickhouse-client --user admin --password "$(cat /run/secrets/clickhouse_password)" --multiquery' ; }
old=$(echo "SELECT count() FROM system.tables WHERE database = 'bronze' AND engine = 'Kafka' AND engine_full LIKE '%JSONAsString%'" | ch)
if [[ "$old" == "0" ]]; then
  echo "bronze Kafka tables already read RawBLOB"
  exit 0
fi
ch <<'SQL'
DROP VIEW IF EXISTS bronze.mv_cdc_events;
DROP VIEW IF EXISTS bronze.mv_cdc_events_errors;
DROP VIEW IF EXISTS bronze.mv_cdc_transactions;
DROP VIEW IF EXISTS bronze.mv_cdc_transactions_errors;
DROP VIEW IF EXISTS bronze.mv_domain_events;
DROP VIEW IF EXISTS bronze.mv_domain_events_errors;
DROP TABLE IF EXISTS bronze.kafka_cdc_events;
DROP TABLE IF EXISTS bronze.kafka_cdc_transactions;
DROP TABLE IF EXISTS bronze.kafka_domain_events;
SQL
ch < warehouse/clickhouse/init/01-bronze.sql
ch < warehouse/clickhouse/init/04-domain-events.sql
echo "bronze Kafka tables now read RawBLOB"
