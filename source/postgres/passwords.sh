#!/bin/sh
# Service account passwords from the environment. Runs at init on a fresh volume; ./observability/start.sh
# runs it again on an existing one, so a changed .env takes effect.
set -eu
psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v admin_pw="$POSTGRES_PASSWORD" -v cdc_pw="$CDC_DB_PASSWORD" -v monitor_pw="$MONITOR_PASSWORD" <<'SQL'
ALTER ROLE CURRENT_USER PASSWORD :'admin_pw';
ALTER ROLE cdc_user PASSWORD :'cdc_pw';
ALTER ROLE monitor_reader PASSWORD :'monitor_pw';
SQL
