"""Bounded-cardinality metrics; collect in background so Prometheus scrapes never block on SQL."""
import json
import logging
import os
import time
from urllib.request import urlopen

import clickhouse_connect
import psycopg2
from prometheus_client import Gauge, start_http_server

UP = Gauge('cdc_dependency_up', 'Last dependency collection succeeded', ['dependency'])
LAST = Gauge('cdc_dependency_last_success_timestamp_seconds', 'Last successful collection', ['dependency'])
CH = Gauge('cdc_clickhouse_metric', 'ClickHouse system.metrics', ['metric'])
EVENTS = Gauge('cdc_clickhouse_events_total', 'ClickHouse cumulative events since server start', ['event'])
ROWS = Gauge('cdc_events_total', 'Raw CDC events including redeliveries')
LAG = Gauge('cdc_ingest_lag_p95_seconds', 'Commit-to-ingest p95 over last 5 wall-clock minutes')
ERRORS = Gauge('cdc_ingest_errors_total', 'Persisted ingest error rows')
HELD = Gauge('cdc_transactions_held_back', 'Recent transactions held back by watermark')
HELD_AGE = Gauge('cdc_oldest_held_transaction_age_seconds', 'Age of oldest held transaction')
REFRESH = Gauge('cdc_mart_last_success_timestamp_seconds', 'Latest published mart refresh')
DURATION = Gauge('cdc_mart_refresh_duration_seconds', 'Latest published refresh duration')
RECON = Gauge('cdc_reconciliation_failing_tables', 'Tables with two consecutive mismatches')
RECON_LAST = Gauge('cdc_reconciliation_last_success_timestamp_seconds', 'Last reconciliation execution')
PG_CONN = Gauge('cdc_postgres_connections', 'Database sessions')
PG_MAX = Gauge('cdc_postgres_max_connections', 'Configured maximum sessions')
PG_DEAD = Gauge('cdc_postgres_deadlocks_total', 'Database deadlocks since stats reset')
PG_LOCK = Gauge('cdc_postgres_lock_waiters', 'Sessions waiting on a lock')
PG_WAL = Gauge('cdc_postgres_slot_retained_wal_bytes', 'WAL retained by logical slots', ['slot'])
PG_SAFE = Gauge('cdc_postgres_slot_safe_wal_bytes', 'WAL a slot may still fall behind before max_slot_wal_keep_size invalidates it', ['slot'])
PG_SLOT_OK = Gauge('cdc_postgres_slot_wal_reserved', 'Slot WAL is reserved (wal_status reserved/extended, not unreserved/lost)', ['slot'])
CH_PARTS = Gauge('cdc_clickhouse_max_parts_per_partition', 'Most active parts in one partition of a table', ['database', 'table'])
CH_QUERY_P95 = Gauge('cdc_clickhouse_select_p95_seconds', 'p95 duration of finished SELECTs over the last 5 minutes', ['user'])
DBZ_BEHIND = Gauge('cdc_debezium_behind_source_seconds', 'Debezium streaming: time between commit and processing of the last event')
DBZ_SINCE = Gauge('cdc_debezium_since_last_event_seconds', 'Debezium streaming: time since the last event')
DBZ_QUEUE = Gauge('cdc_debezium_queue_remaining_ratio', 'Debezium streaming: free share of the internal change queue')
DBZ_CONNECTED = Gauge('cdc_debezium_connected', 'Debezium streaming: connected to PostgreSQL')
DBZ_EVENTS = Gauge('cdc_debezium_events_seen_total', 'Debezium streaming: events seen since the task started')
CONNECT = Gauge('cdc_connect_running', 'Connector/task is RUNNING', ['connector', 'component'])
SERVICE = Gauge('cdc_service_up', 'HTTP health endpoint succeeds', ['service'])


def collect_clickhouse():
    ch = clickhouse_connect.get_client(host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username='monitor_reader', password=os.environ['MONITOR_PASSWORD'], connect_timeout=3, send_receive_timeout=10)
    try:
        def one(sql):
            return ch.query(sql).result_rows[0]
        for k, v in ch.query('SELECT metric, value FROM system.metrics').result_rows:
            CH.labels(k).set(v)
        for k, v in ch.query("SELECT event, value FROM system.events WHERE event IN ('Query','FailedQuery','InsertedRows','SelectedRows')").result_rows:
            EVENTS.labels(k).set(v)
        # metadata rows avoid scanning the complete changelog every 15 seconds
        ROWS.set(one("SELECT coalesce(sum(rows),0) FROM system.parts WHERE active AND database='bronze' AND table='cdc_events'")[0])
        LAG.set(one("SELECT if(count()=0,0,quantile(0.95)(greatest(0,dateDiff('millisecond',source_ts,ingested_at)))/1000) FROM bronze.cdc_events WHERE ingested_at > now()-INTERVAL 5 MINUTE AND op!='r'")[0])
        ERRORS.set(one('SELECT count() FROM bronze.ingest_errors')[0])
        _, held, oldest = one('SELECT * FROM consistent.watermark')
        HELD.set(held); HELD_AGE.set(max(0, time.time()-oldest.timestamp()) if oldest else 0)
        refresh = ch.query('SELECT toUnixTimestamp(finished_at), duration_ms/1000 FROM ops.mart_refreshes ORDER BY refresh_id DESC LIMIT 1').result_rows
        REFRESH.set(refresh[0][0] if refresh else 0); DURATION.set(refresh[0][1] if refresh else 0)
        n, last = one('SELECT countIf(failing), toUnixTimestamp(max(last_run)) FROM ops.reconciliation_status')
        RECON.set(n); RECON_LAST.set(last)
        CH_PARTS.clear()
        for db, table, parts in ch.query("SELECT database, table, max(c) FROM (SELECT database, table, partition_id, count() AS c "
                                         "FROM system.parts WHERE active AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') "
                                         "GROUP BY database, table, partition_id) GROUP BY database, table").result_rows:
            CH_PARTS.labels(db, table).set(parts)
        CH_QUERY_P95.clear()
        for user, p95 in ch.query("SELECT user, quantile(0.95)(query_duration_ms) / 1000 FROM system.query_log "
                                  "WHERE event_time > now() - INTERVAL 5 MINUTE AND type = 'QueryFinish' AND query_kind = 'Select' "
                                  "AND user IN ('grafana_reader', 'admin') GROUP BY user").result_rows:
            CH_QUERY_P95.labels(user).set(p95)
    finally:
        ch.close()


def collect_postgres():
    with psycopg2.connect(host=os.getenv('PG_HOST', 'postgres'), user='monitor_reader',
                         password=os.environ['MONITOR_PASSWORD'], dbname='enterprise_db', connect_timeout=3,
                         options='-c statement_timeout=5000') as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), count(*) FILTER (WHERE wait_event_type='Lock') FROM pg_stat_activity")
            n, locks = cur.fetchone(); PG_CONN.set(n); PG_LOCK.set(locks)
            cur.execute('SHOW max_connections'); PG_MAX.set(int(cur.fetchone()[0]))
            cur.execute("SELECT deadlocks FROM pg_stat_database WHERE datname=current_database()")
            PG_DEAD.set(cur.fetchone()[0])
            cur.execute("SELECT slot_name, coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn),0) FROM pg_replication_slots WHERE slot_type='logical'")
            PG_WAL.clear()
            for slot, value in cur.fetchall():
                PG_WAL.labels(slot).set(float(value))
            cur.execute("SELECT slot_name, wal_status, safe_wal_size FROM pg_replication_slots WHERE slot_type='logical'")
            PG_SAFE.clear(); PG_SLOT_OK.clear()
            for slot, status, safe in cur.fetchall():
                PG_SLOT_OK.labels(slot).set(status in ('reserved', 'extended'))
                if safe is not None:                # NULL when max_slot_wal_keep_size is unlimited, or the slot is lost
                    PG_SAFE.labels(slot).set(float(safe))


def collect_connect():
    base = os.getenv('DEBEZIUM_URL', 'http://debezium:8083')
    with urlopen(base+'/connectors?expand=status', timeout=5) as r:
        statuses = json.load(r)
    CONNECT.clear()
    if not statuses:
        CONNECT.labels('postgres-connector', 'connector').set(0)
    for name, info in statuses.items():
        status = info['status']
        CONNECT.labels(name, 'connector').set(status['connector']['state']=='RUNNING')
        for task in status.get('tasks', []):
            CONNECT.labels(name, 'task-'+str(task['id'])).set(task['state']=='RUNNING')


def collect_debezium_jmx():
    # Jolokia (JMX over HTTP) inside the Connect container; only reachable on the compose network
    base = os.getenv('DEBEZIUM_JOLOKIA_URL', 'http://debezium:8778/jolokia')
    with urlopen(base+'/read/debezium.postgres:type=connector-metrics,context=streaming,*', timeout=5) as r:
        beans = json.load(r)['value']
    if not beans:
        raise RuntimeError('no streaming metrics yet')
    m = next(iter(beans.values()))
    DBZ_BEHIND.set(max(0, m['MilliSecondsBehindSource']) / 1000)
    DBZ_SINCE.set(max(0, m['MilliSecondsSinceLastEvent']) / 1000)
    DBZ_QUEUE.set(m['QueueRemainingCapacity'] / max(1, m['QueueTotalCapacity']))
    DBZ_CONNECTED.set(m['Connected'])
    DBZ_EVENTS.set(m['TotalNumberOfEventsSeen'])


def main():
    logging.basicConfig(level=logging.INFO)
    start_http_server(9108)
    collectors = {'clickhouse': collect_clickhouse, 'postgres': collect_postgres, 'debezium': collect_connect, 'debezium-jmx': collect_debezium_jmx}
    while True:
        for name, collect in collectors.items():
            try:
                collect(); UP.labels(name).set(1); LAST.labels(name).set(time.time())
            except Exception as error:
                UP.labels(name).set(0)
                logging.warning('%s collection failed (%s)', name, type(error).__name__)
        for service, url in {'source-api':'http://source-api:8100/health', 'control-center':'http://control-center:8000/api/health', 'simulator':'http://simulator:8200/status'}.items():
            try:
                with urlopen(url, timeout=3) as response:
                    SERVICE.labels(service).set(response.status==200)
            except Exception:
                SERVICE.labels(service).set(0)
        time.sleep(15)


if __name__ == '__main__':
    main()
