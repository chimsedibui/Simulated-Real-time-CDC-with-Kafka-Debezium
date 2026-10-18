#!/usr/bin/env bash
# Test gates for the whole pipeline. Pick the smallest one that covers your change:
#
#   quick  running stack, no reset: rebuild code, API + stress tests, 90 s of traffic with the torn-read probe,
#          check_data. For code changes. ~3–4 min
#   smoke  clean stack (schema or ClickHouse SQL changed): seed, tests, 4 min of traffic at ×1440 with an 11.11
#          sale, probe, check_data. ~8–10 min
#   full   clean stack, 16 min at ×720 (~8 business days) with an 11.11 sale and restarts of Debezium and
#          ClickHouse, probe, lag report, check_data. Before pushing a milestone. ~25 min
#
# Every gate stops at the first PostgreSQL deadlock and prints it, instead of waiting for the end.
# Local port clashes: put overrides in docker-compose.override.yml (gitignored); compose loads it automatically.
#
#   quality/gate.sh quick
set -euo pipefail
cd "$(dirname "$0")/.."                     # repository root

MODE=${1:-quick}
DC="docker compose --profile tools"
OUT=${GATE_OUT:-/tmp/cdc-gate}
mkdir -p "$OUT"

pg() { $DC exec -T postgres psql -U admin -d enterprise_db -Atc "$1"; }
ch() { $DC exec -T clickhouse bash -c 'clickhouse-client --user admin --password "$CLICKHOUSE_PASSWORD" --query "$0"' "$1"; }
api() { curl -s -m "${3:-10}" -X "$1" "localhost:8000/api/$2" -H 'Content-Type: application/json' -d "${4:-}"; }
step() { echo; echo "== $*"; }
deadlocks() { pg "SELECT deadlocks FROM pg_stat_database WHERE datname = 'enterprise_db'"; }

show_deadlocks() {
  $DC exec -T postgres bash -c 'grep -h -B1 -A12 "deadlock detected" /var/lib/postgresql/data/logs/*.log | tail -40' || true
}

reset_stack() {
  step "reset"
  $DC down -v >/dev/null 2>&1
  docker run --rm -v "$PWD/source/postgres:/p" alpine sh -c "rm -rf /p/data"
}

core_up() {
  step "core services"
  $DC up -d --build postgres kafka debezium clickhouse source-api warehouse-jobs >/dev/null 2>&1
  for _ in $(seq 1 60); do curl -sf localhost:8083/ >/dev/null && break; sleep 3; done
  if ! curl -sf localhost:8083/connectors/postgres-connector >/dev/null; then
    debezium/register.sh >/dev/null
    sleep 8
  fi
  curl -s localhost:8083/connectors/postgres-connector/status | python3 -c \
    "import json,sys;d=json.load(sys.stdin);print('connector', d['connector']['state'], [t['state'] for t in d['tasks']])"
}

seed() {
  step "simulator + seed"
  SIM_SPEED=240 $DC up -d --build simulator control-center >/dev/null 2>&1
  local r=""
  for _ in $(seq 1 150); do
    r=$(curl -s localhost:8200/status?log_since=999999 | python3 -c \
      "import json,sys;s=json.load(sys.stdin)['seeding'];print(s['running'],s['done'],s['target'])" 2>/dev/null || true)
    [[ "$r" == False* ]] && break; sleep 2
  done
  echo "seeding done: $r · $(curl -s localhost:8100/stats)"
}

tests() {
  step "API and stress tests"
  $DC build validator >/dev/null 2>&1
  if ! $DC run --rm validator pytest -q -x -p no:cacheprovider test_source_api.py test_stress.py > "$OUT/tests.out" 2>&1; then
    grep -v Container "$OUT/tests.out" | tail -30; show_deadlocks; exit 1
  fi
  grep -v Container "$OUT/tests.out" | tail -2
}

# load <seconds> <speed> <mega_sale_at|-> <chaos: yes|no>
load() {
  local secs=$1 speed=$2 mega=$3 chaos=$4 base start now
  step "traffic: ${secs}s at ×${speed}"
  base=$(deadlocks)
  api PUT clock 10 "{\"speed\": $speed}" >/dev/null
  api POST generator/start 10 '{"interval":1,"orders_per_tick":40,"new_customers_per_tick":6}' >/dev/null
  (sleep 10; $DC run --rm validator python probe_consistency.py --seconds $((secs - 20)) > "$OUT/probe.out" 2>&1) &
  local probe=$!
  start=$(date +%s)
  while :; do
    now=$(( $(date +%s) - start ))
    (( now >= secs )) && break
    if (( $(deadlocks) > base )); then
      api POST generator/stop 60 >/dev/null; kill $probe 2>/dev/null || true
      echo "DEADLOCK after ${now}s — stopping"; show_deadlocks; exit 1
    fi
    if [[ "$mega" != "-" && $now -ge $mega && ! -f "$OUT/mega" ]]; then
      api POST actions/mega_sale 60 '{"count":1}' >/dev/null; touch "$OUT/mega"; echo "11.11 at +${now}s"
    fi
    if [[ "$chaos" == yes && $now -ge $((secs / 2)) && ! -f "$OUT/debezium" ]]; then
      curl -s -X POST "localhost:8083/connectors/postgres-connector/restart?includeTasks=true" -o /dev/null
      touch "$OUT/debezium"; echo "Debezium restarted at +${now}s"
    fi
    if [[ "$chaos" == yes && $now -ge $((2 * secs / 3)) && ! -f "$OUT/clickhouse" ]]; then
      docker restart cdc-clickhouse >/dev/null; touch "$OUT/clickhouse"; echo "ClickHouse restarted at +${now}s"
    fi
    sleep 5
  done
  rm -f "$OUT/mega" "$OUT/debezium" "$OUT/clickhouse"
  api POST generator/stop 60 | python3 -c \
    "import json,sys;d=json.load(sys.stdin);print('ticks', d['ticks'], 'errors', d['errors'], 'api', d['api'])"
  api PUT clock 10 '{"speed": 1}' >/dev/null
  wait $probe || true
  step "torn-read probe"; grep -v -E "Container|Http Driver|poll failed" "$OUT/probe.out" || true
  grep -q "^PASS" "$OUT/probe.out" || { echo "probe failed"; exit 1; }
  echo "deadlocks during traffic: $(( $(deadlocks) - base ))"
  step "lag commit → ClickHouse (this run)"
  ch "SELECT count() AS events, round(quantile(0.5)(dateDiff('millisecond', source_ts, ingested_at))/1000, 2) AS p50_s,
             round(quantile(0.95)(dateDiff('millisecond', source_ts, ingested_at))/1000, 2) AS p95_s
      FROM bronze.cdc_events WHERE op != 'r' AND ingested_at > now() - INTERVAL $((secs + 60)) SECOND
      FORMAT PrettyCompactMonoBlock"
}

check() {
  step "data checks"
  $DC run --rm validator python check_data.py 2>&1 | grep -v -E "Container|^\[PASS\]"
}

case "$MODE" in
  quick)
    if ! curl -sf localhost:8083/ >/dev/null || [[ "$(pg 'SELECT count(*) FROM ecommerce.shops' 2>/dev/null)" == "0" ]]; then
      core_up; seed
    else
      step "rebuild code (data kept)"
      api POST generator/stop 30 >/dev/null || true
      $DC up -d --build source-api simulator warehouse-jobs control-center >/dev/null 2>&1
      for _ in $(seq 1 30); do curl -sf localhost:8100/health >/dev/null && break; sleep 2; done
    fi
    pg "SELECT pg_stat_reset()" >/dev/null          # deadlock counter starts from zero for this run
    tests
    load 90 720 - no
    check ;;
  smoke)
    reset_stack; core_up; seed; tests
    load 240 1440 120 no
    check ;;
  full)
    reset_stack; core_up; seed; tests
    load 960 720 320 yes
    check ;;
  *) echo "usage: $0 quick|smoke|full"; exit 2 ;;
esac
