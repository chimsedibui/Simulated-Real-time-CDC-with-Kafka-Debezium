#!/usr/bin/env bash
# Load test at a target event rate (T1 peak: ~5,400 CDC events/s). Runs the main simulator plus N extra ones,
# then reports per-minute throughput and lag, API latency and errors, and deadlocks. Data is kept.
#
#   quality/load_test.sh [extra_simulators=5] [seconds=600] [speed=1440]
set -euo pipefail
cd "$(dirname "$0")/.."
EXTRA=${1:-5}; SECS=${2:-600}; SPEED=${3:-1440}
DC="docker compose --profile load"
TICK='{"interval":1,"orders_per_tick":50,"new_customers_per_tick":6}'
ch() { $DC exec -T clickhouse bash -c 'clickhouse-client --user admin --password "$(cat /run/secrets/clickhouse_password)" --query "$0"' "$1"; }
prom() { curl -s localhost:9090/api/v1/query --data-urlencode "query=$1" | python3 -c \
  'import json,sys;r=json.load(sys.stdin)["data"]["result"];print(round(float(r[0]["value"][1]),3) if r else "n/a")'; }
sims() { $DC ps -q simulator-load; }
each() { for c in $(sims); do docker exec "$c" python -c "import urllib.request as u;u.urlopen(u.Request('http://localhost:8200/$1',data=b'''$2''',headers={'Content-Type':'application/json'},method='POST'),timeout=120).read()" >/dev/null || echo "simulator $c: $1 failed"; done; }   # stop waits for the running tick

$DC up -d --build --scale simulator-load="$EXTRA" simulator-load >/dev/null 2>&1
sleep 10
curl -s -X PUT localhost:8000/api/clock -H 'Content-Type: application/json' -d "{\"speed\": $SPEED}" >/dev/null
base=$($DC exec -T postgres psql -U admin -d enterprise_db -Atc "SELECT deadlocks FROM pg_stat_database WHERE datname = 'enterprise_db'")
started=$(date -u '+%Y-%m-%d %H:%M:%S')
curl -s -X POST localhost:8000/api/generator/start -H 'Content-Type: application/json' -d "$TICK" >/dev/null
each start "$TICK"
echo "load: 1 + $EXTRA simulators at x$SPEED for ${SECS}s from $started UTC"
sleep "$SECS"
each stop "{}"
curl -s -X POST localhost:8000/api/generator/stop >/dev/null
curl -s -X PUT localhost:8000/api/clock -H 'Content-Type: application/json' -d '{"speed": 1}' >/dev/null      # as gate.sh leaves it
$DC stop simulator-load >/dev/null 2>&1

echo; echo "== CDC events per minute and commit -> ClickHouse lag"
ch "SELECT toStartOfMinute(ingested_at) AS minute, count() AS events, round(count() / 60) AS per_s,
           round(quantile(0.5)(dateDiff('millisecond', source_ts, ingested_at)) / 1000, 2) AS p50_s,
           round(quantile(0.95)(dateDiff('millisecond', source_ts, ingested_at)) / 1000, 2) AS p95_s
    FROM bronze.cdc_events WHERE op != 'r' AND ingested_at >= toDateTime64('$started', 3, 'UTC')
    GROUP BY minute ORDER BY minute FORMAT PrettyCompactMonoBlock"
w="${SECS}s"
echo "== API over the run"
echo "requests/s          $(prom "sum(rate(http_requests_total{job=\"source-api\"}[$w]))")"
echo "5xx/s               $(prom "sum(rate(http_requests_total{job=\"source-api\",status=~\"5..\"}[$w]))")"
echo "checkout p99 (s)    $(prom "histogram_quantile(0.99, sum by(le)(rate(http_request_duration_seconds_bucket{job=\"source-api\",handler=~\"(/v1)?/checkouts\",method=\"POST\"}[$w])))")"
echo "API p95 (s)         $(prom "histogram_quantile(0.95, sum by(le)(rate(http_request_duration_highr_seconds_bucket{job=\"source-api\"}[$w])))")"
echo "Debezium behind max $(prom "max_over_time(cdc_debezium_behind_source_seconds[$w])")"
echo "consumer lag max    $(prom "max_over_time(sum(kafka_consumergroup_lag)[$w:15s])")"
echo "snapshot delta max  $(prom "max_over_time(warehouse_snapshot_duration_seconds{mode=\"delta\"}[$w])")"
echo "deadlocks           $(( $($DC exec -T postgres psql -U admin -d enterprise_db -Atc "SELECT deadlocks FROM pg_stat_database WHERE datname = 'enterprise_db'") - base ))"
