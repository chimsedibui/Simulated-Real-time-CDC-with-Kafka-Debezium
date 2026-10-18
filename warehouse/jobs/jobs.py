"""Scheduled warehouse jobs. Each run pins one commit watermark, so everything it reads is one consistent state.

    snapshot             snap.<table> brought to the watermark: only the keys written by transactions since the last
                         run are re-read, and every table is rebuilt in full every SNAP_REBUILD_S as a safety net
    refresh_marts        gold, read from the snapshots (gold_snap.<view>), precomputed into mart.<view>; dashboards
                         read these tables instead of recomputing gold per viewer (MART_REFRESH_S)
    close_business_days  freeze each finished business day into gold.daily_sales_closed ("as reported"),
                         one business hour after it ends (payment windows are 15 minutes, so GMV is final)
    reconcile            PostgreSQL vs ClickHouse per table into ops.reconciliation; a table that disagrees two runs
                         in a row is logged as an error (RECONCILE_S)

Run by the warehouse-jobs service.
"""

import logging
import os
import re
import time
from datetime import timedelta

import clickhouse_connect
import psycopg2
from prometheus_client import Counter, Gauge, start_http_server

JOB_ERRORS = Counter("warehouse_job_errors_total", "Failed warehouse loop executions")
SNAP_SECONDS = Gauge("warehouse_snapshot_duration_seconds", "Duration of the last snapshot run", ["mode"])
SNAP_KEYS = Gauge("warehouse_snapshot_keys", "Keys re-read by the last delta snapshot run")

LOOP_S = float(os.getenv("JOB_INTERVAL_S", "5"))
MART_REFRESH_S = float(os.getenv("MART_REFRESH_S", "15"))
RECONCILE_S = float(os.getenv("RECONCILE_S", "60"))
SNAP_REBUILD_S = float(os.getenv("SNAP_REBUILD_S", "3600"))
log = logging.getLogger("warehouse-jobs")

MARTS = ["overview_kpis", "daily_sales", "traffic_by_hour", "order_status_summary", "funnel", "payment_summary",
         "category_sales", "product_sales", "shop_performance", "inventory_risk", "customer_tiers", "platform_pnl",
         "delivery_performance", "returns_summary", "return_rate_by_cohort", "voucher_performance",
         "flash_sale_performance", "seller_settlement"]

# table -> column whose sum never changes after insert (None: count only). Skipped: tables without created_at, and
# cart_items, whose rows are hard-deleted all the time: PostgreSQL now and ClickHouse a few seconds behind never agree
# on it while traffic flows (check_data compares it at rest).
RECONCILED = {
    "customers": None, "customer_addresses": None, "sellers": None, "shops": None, "categories": None,
    "brands": None, "products": None, "product_skus": None, "carts": None,
    "checkouts": "grand_total", "orders": "total", "order_items": "net_amount", "order_status_history": None,
    "payments": "amount", "refunds": "amount", "inventory_movements": "on_hand_delta", "shipments": None,
    "shipment_events": None, "price_history": "new_price", "vouchers": "usage_limit", "voucher_usages": "amount",
    "campaigns": None, "flash_sale_items": "quota", "returns": None, "return_items": "quantity",
    "seller_ledger_entries": "amount", "seller_payouts": "amount", "product_reviews": "rating",
}


def clickhouse():
    return clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), port=8123,
                                         username=os.getenv("CLICKHOUSE_USER", "admin"),
                                         password=os.environ["CLICKHOUSE_PASSWORD"])


def postgres():
    conn = psycopg2.connect(host=os.getenv("PG_HOST", "postgres"), port=int(os.getenv("PG_PORT", "5432")),
                            user=os.getenv("PG_USER", "admin"), password=os.environ["PG_PASSWORD"],
                            dbname=os.getenv("PG_DB", "enterprise_db"))
    conn.set_session(readonly=True, autocommit=True)
    return conn


def watermark(ch) -> int:
    return ch.query("SELECT v FROM consistent.watermark").result_rows[0][0]


def create_marts(ch):
    """Preserve published snapshots across reconnects; schema changes require an explicit migration."""
    for name in MARTS:
        ch.command(f"""CREATE TABLE IF NOT EXISTS mart.{name} ENGINE = MergeTree ORDER BY (refresh_id, _row)
                       EMPTY AS SELECT toUInt64(0) AS refresh_id, now64(3) AS refreshed_at,
                                       toUInt32(0) AS _row, * FROM gold.{name}(v = 0)""")
        ddl = ch.query(f"SELECT create_table_query FROM system.tables WHERE database = 'mart' AND name = '{name}'").result_rows[0][0]
        if "TTL" in ddl:
            ch.command(f"ALTER TABLE mart.{name} REMOVE TTL")


def create_gold_snap(ch):
    """gold_snap.<view>: each gold view reading snap.<table> FINAL instead of consistent.<table>(v), so marts cost
    a scan of typed columns. Derived from the gold definitions at startup, never written by hand."""
    ch.command("CREATE DATABASE IF NOT EXISTS gold_snap")
    pending = {}
    for name, sql in ch.query("SELECT name, create_table_query FROM system.tables "
                              "WHERE database = 'gold' AND engine = 'View'").result_rows:
        body = re.sub(r"`consistent\.(\w+)`\(v = \{v:UInt64\}\)(\s+AS\s+\w+)?", r"snap.\1\2 FINAL", sql)
        body = re.sub(r"`gold\.(\w+)`\(v = \{v:UInt64\}\)", r"gold_snap.\1", body)
        if "{v:" not in body:                   # views of the watermark itself stay live-only
            pending[name] = body.replace(f"CREATE VIEW gold.{name} ", f"CREATE OR REPLACE VIEW gold_snap.{name} ", 1)
    while pending:                              # a view can be created only after the views it reads
        ready = [n for n, b in pending.items() if not any(f"gold_snap.{o} " in b or f"gold_snap.{o})" in b
                                                          for o in pending if o != n)]
        if not ready:
            raise RuntimeError(f"gold views depend on each other in a cycle: {sorted(pending)}")
        for n in ready:
            ch.command(pending.pop(n))


def snapshot(ch, v: int, state: dict) -> str | None:
    """Bring every snap.<table> to watermark v. Delta: the keys written by transactions that became visible since
    the last run (snap.changed_keys), each re-read through snap.<table>_delta(v); a key gone at v gets a removal
    row. Full: every table copied from consistent.<table>(v) — at startup, after an error, and every SNAP_REBUILD_S.
    `state` is reset by the caller after a failure, so a half-done run is always followed by a full one."""
    if state.get("v") == v and time.monotonic() < state.get("rebuild_at", 0):
        return None
    started, ran_at = time.monotonic(), _now(ch)
    tables = state.get("tables") or [(t, k) for t, k in ch.query(
        "SELECT name, sorting_key FROM system.tables WHERE database = 'snap' AND engine = 'ReplacingMergeTree'"
    ).result_rows]
    state["tables"] = tables
    full = "v" not in state or v < state["v"] or time.monotonic() >= state.get("rebuild_at", 0)
    keys = 0
    if "cols" not in state:
        state["cols"] = {table: ", ".join(c for (c,) in ch.query(
            f"SELECT name FROM system.columns WHERE database = 'snap' AND table = '{table}' "
            f"AND name NOT IN ('_v', '_gone') ORDER BY position").result_rows) for table, _ in tables}
    if full:
        for table, _ in tables:
            cols = state["cols"][table]
            ch.command(f"TRUNCATE TABLE snap.{table}")
            ch.command(f"INSERT INTO snap.{table} ({cols}, _v, _gone) "
                       f"SELECT {cols}, {2 * v + 1}, 0 FROM consistent.{table}(v = {v})")
        state["rebuild_at"] = time.monotonic() + SNAP_REBUILD_S
    else:
        prev = state["v"]
        oldest = ch.query(f"SELECT min(commit_ts) FROM bronze.cdc_transactions WHERE ingested_at > now64(3) - "
                          f"INTERVAL 2 HOUR AND kafka_offset >= {prev} AND kafka_offset < {v}").result_rows[0][0]
        since = min(t for t in (oldest, state["ran_at"]) if t is not None) - timedelta(minutes=2)
        ch.command(f"""INSERT INTO snap.dirty (v, tbl, k)
                       SELECT {v}, tbl, k FROM snap.changed_keys(prev = {prev}, v = {v},
                                                                since = '{since:%Y-%m-%d %H:%M:%S}')""")
        counts = dict(ch.query(f"SELECT tbl, count() FROM snap.dirty WHERE v = {v} GROUP BY tbl").result_rows)
        for table, key in tables:
            if not counts.get(table):
                continue
            keys += counts[table]
            cols = state["cols"][table]
            if "," not in key:                  # state table: a key may be gone at v (events only ever arrive)
                ch.command(f"INSERT INTO snap.{table} ({key}, _v, _gone) "
                           f"SELECT k, {2 * v}, 1 FROM snap.dirty WHERE v = {v} AND tbl = '{table}'")
            ch.command(f"INSERT INTO snap.{table} ({cols}, _v, _gone) "
                       f"SELECT {cols}, {2 * v + 1}, 0 FROM snap.{table}_delta(v = {v})")
        SNAP_KEYS.set(keys)
    mode = "full" if full else "delta"
    duration = time.monotonic() - started
    SNAP_SECONDS.labels(mode).set(duration)
    ch.command(f"INSERT INTO snap.runs VALUES ({v}, '{mode}', {keys}, now64(3), {int(duration * 1000)})")
    state.update(v=v, ran_at=ran_at)
    return mode


def refresh_marts(ch, v: int) -> bool:
    last = ch.query("SELECT max(refresh_id) FROM ops.mart_refreshes").result_rows[0][0]
    if last == v and ch.query(f"SELECT count() FROM mart.overview_kpis WHERE refresh_id = {v}").result_rows[0][0]:
        return False                            # nothing committed since the last refresh
    started = time.monotonic()
    for name in MARTS:
        # Clear an incomplete attempt before retrying this watermark; a mutation per refresh would be costly.
        if ch.query(f"SELECT count() FROM mart.{name} WHERE refresh_id = {v}").result_rows[0][0]:
            ch.command(f"ALTER TABLE mart.{name} DELETE WHERE refresh_id = {v} SETTINGS mutations_sync = 1")
        ch.command(f"""INSERT INTO mart.{name}
                       SELECT {v}, now64(3), toUInt32(rowNumberInAllBlocks()), * FROM gold_snap.{name}""")
    # recorded last: readers only ever pick a refresh whose marts are all written
    ch.command(f"INSERT INTO ops.mart_refreshes VALUES ({v}, now64(3), {int((time.monotonic() - started) * 1000)}, "
               f"{len(MARTS)})")
    return True


def close_business_days(ch, v: int) -> int:
    before = ch.query("SELECT count() FROM gold.daily_sales_closed").result_rows[0][0]
    ch.command(f"""
        INSERT INTO gold.daily_sales_closed
        WITH (SELECT max(created_at) FROM snap.orders FINAL) AS latest
        SELECT now64(3) AS closed_at, d.*
        FROM gold_snap.daily_sales AS d
        WHERE d.order_date < toDate(latest - INTERVAL 1 HOUR, 'Asia/Ho_Chi_Minh')
          AND d.order_date NOT IN (SELECT order_date FROM gold.daily_sales_closed)
          -- a payment can commit later than an order placed after it (a long transaction at a fast clock):
          -- the day closes only once none of its orders still awaits payment
          AND d.order_date NOT IN (SELECT DISTINCT toDate(created_at, 'Asia/Ho_Chi_Minh') FROM snap.orders FINAL
                                   WHERE status = 'UNPAID')""")
    return ch.query("SELECT count() FROM gold.daily_sales_closed").result_rows[0][0] - before


def reconcile(ch, pg, v: int) -> list:
    """Rows created up to a cutoff one business hour before the watermark: committed long enough ago that
    both sides must have them. Returns the tables that disagreed two runs in a row."""
    latest = ch.query("SELECT max(created_at) FROM snap.orders FINAL").result_rows[0][0]
    if latest is None:
        return []
    cutoff = (latest - timedelta(hours=1)).replace(tzinfo=None)          # UTC on both sides
    ch_cutoff = f"toDateTime64('{cutoff:%Y-%m-%d %H:%M:%S.%f}', 6, 'UTC')"
    rows = []
    for table, column in RECONCILED.items():
        total = f"sum({column})" if column else "0"
        with pg.cursor() as cur:
            cur.execute(f"SELECT count(*), coalesce({total}, 0) FROM ecommerce.{table} "
                        f"WHERE created_at <= %s AT TIME ZONE 'UTC'", (cutoff,))
            pg_rows, pg_sum = cur.fetchone()
        ch_rows, ch_sum = ch.query(
            f"SELECT count(), {total} FROM snap.{table} FINAL WHERE created_at <= {ch_cutoff}").result_rows[0]
        rows.append([v, cutoff, table, pg_rows, ch_rows, int(pg_sum), int(ch_sum or 0),
                     (pg_rows, int(pg_sum)) == (ch_rows, int(ch_sum or 0))])
    ch.insert("ops.reconciliation", [[_now(ch)] + r for r in rows],
              column_names=["run_at", "v", "cutoff", "table_name", "pg_rows", "ch_rows", "pg_sum", "ch_sum", "ok"])
    return [r[0] for r in ch.query("SELECT table_name FROM ops.reconciliation_status WHERE failing").result_rows]


def _now(ch):
    return ch.query("SELECT now64(3)").result_rows[0][0]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start_http_server(9109)
    ch = pg = None
    snap_state = {}
    next_cleanup = 0.0
    next_mart = next_recon = 0.0
    while True:
        try:
            if ch is None:
                ch = clickhouse()
                create_marts(ch)
                create_gold_snap(ch)
            pg = pg or postgres()
            mart_due, recon_due = time.monotonic() >= next_mart, time.monotonic() >= next_recon
            if not (mart_due or recon_due):
                time.sleep(LOOP_S)
                continue
            v = watermark(ch)
            # marts, day close and reconciliation all read the snapshots, so they are brought to v first
            if snapshot(ch, v, snap_state) == "full":
                log.info("snapshots rebuilt at watermark %d", v)
            if mart_due:
                next_mart = time.monotonic() + MART_REFRESH_S
                refresh_marts(ch, v)
                closed = close_business_days(ch, v)
                if closed:
                    log.info("closed %d business day(s) at watermark %d", closed, v)
            if recon_due:
                next_recon = time.monotonic() + RECONCILE_S
                failing = reconcile(ch, pg, v)
                if failing:
                    log.error("reconciliation failing two runs in a row: %s", ", ".join(failing))
            if time.monotonic() >= next_cleanup:
                latest = ch.query("SELECT max(refresh_id) FROM ops.mart_refreshes").result_rows[0][0]
                for name in MARTS:
                    ch.command(f"ALTER TABLE mart.{name} DELETE WHERE refresh_id != {latest} AND refreshed_at < now() - INTERVAL 1 HOUR")
                next_cleanup = time.monotonic() + 3600
        except Exception as e:                  # a dependency not up yet or restarting: try again next round
            JOB_ERRORS.inc()
            log.warning("run skipped: %s", str(e).splitlines()[0][:200])
            ch = pg = None
            snap_state = {}                     # a half-done snapshot run is followed by a full rebuild
        time.sleep(LOOP_S)


if __name__ == "__main__":
    main()
