-- OPS: results of the scheduled warehouse jobs (warehouse_jobs/jobs.py)
CREATE DATABASE IF NOT EXISTS ops;
CREATE DATABASE IF NOT EXISTS mart;      -- gold precomputed at one watermark per refresh (tables made by warehouse-jobs)

-- Reconciliation PostgreSQL ↔ ClickHouse, every few minutes: rows created up to a cutoff, plus a sum of an
-- immutable column, compared table by table at a pinned watermark
CREATE TABLE IF NOT EXISTS ops.reconciliation
(
    run_at      DateTime64(3, 'UTC'),
    v           UInt64,
    cutoff      DateTime64(6, 'UTC'),       -- business time; rows created up to here are compared
    table_name  LowCardinality(String),
    pg_rows     UInt64,
    ch_rows     UInt64,
    pg_sum      Int64,
    ch_sum      Int64,
    ok          Bool
)
ENGINE = MergeTree
ORDER BY (table_name, run_at)
TTL toDateTime(run_at) + INTERVAL 7 DAY;

-- Latest result per table. failing = the last two runs both disagreed: one bad run can be a row deleted
-- in PostgreSQL between the two reads, two in a row is real drift
CREATE OR REPLACE VIEW ops.reconciliation_status AS
SELECT
    table_name,
    tupleElement(runs[1], 1)                                                  AS last_run,
    tupleElement(runs[1], 2)                                                  AS ok,
    length(runs) >= 2 AND NOT tupleElement(runs[1], 2) AND NOT tupleElement(runs[2], 2) AS failing,
    tupleElement(runs[1], 3)                                                  AS pg_rows,
    tupleElement(runs[1], 4)                                                  AS ch_rows,
    tupleElement(runs[1], 5)                                                  AS pg_sum,
    tupleElement(runs[1], 6)                                                  AS ch_sum
FROM (
    SELECT table_name,
           arraySort(r -> -toUnixTimestamp64Milli(r.1), groupArray((run_at, ok, pg_rows, ch_rows, pg_sum, ch_sum))) AS runs
    FROM ops.reconciliation
    WHERE run_at > now64(3) - INTERVAL 1 DAY
    GROUP BY table_name)
ORDER BY failing DESC, ok, table_name;

-- One row per finished mart refresh: readers take the latest refresh_id and read every mart at it
CREATE TABLE IF NOT EXISTS ops.mart_refreshes
(
    refresh_id   UInt64,                    -- the commit watermark v the marts were computed at
    finished_at  DateTime64(3, 'UTC'),
    duration_ms  UInt32,
    marts        UInt16
)
ENGINE = MergeTree
ORDER BY refresh_id
TTL toDateTime(finished_at) + INTERVAL 1 DAY;
