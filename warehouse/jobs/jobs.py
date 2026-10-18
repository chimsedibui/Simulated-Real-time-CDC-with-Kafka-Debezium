"""Scheduled warehouse jobs. Each run pins one commit watermark, so everything it reads is one consistent state.

    refresh_marts        gold precomputed into mart.<view> at one watermark; dashboards read these tables instead of
                         recomputing gold per viewer (MART_REFRESH_S)
    close_business_days  freeze each finished business day into gold.daily_sales_closed ("as reported"),
                         one business hour after it ends (payment windows are 15 minutes, so GMV is final)
    reconcile            PostgreSQL vs ClickHouse per table into ops.reconciliation; a table that disagrees two runs
                         in a row is logged as an error (RECONCILE_S)

Run by the warehouse-jobs service.
"""

import logging
import os
import time
from datetime import timedelta

import clickhouse_connect
import psycopg2
from prometheus_client import Counter, start_http_server

JOB_ERRORS = Counter("warehouse_job_errors_total", "Failed warehouse loop executions")

LOOP_S = float(os.getenv("JOB_INTERVAL_S", "5"))
MART_REFRESH_S = float(os.getenv("MART_REFRESH_S", "15"))
RECONCILE_S = float(os.getenv("RECONCILE_S", "60"))
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
                       SELECT {v}, now64(3), toUInt32(rowNumberInAllBlocks()), * FROM gold.{name}(v = {v})""")
    # recorded last: readers only ever pick a refresh whose marts are all written
    ch.command(f"INSERT INTO ops.mart_refreshes VALUES ({v}, now64(3), {int((time.monotonic() - started) * 1000)}, "
               f"{len(MARTS)})")
    return True


def close_business_days(ch, v: int) -> int:
    before = ch.query("SELECT count() FROM gold.daily_sales_closed").result_rows[0][0]
    ch.command(f"""
        INSERT INTO gold.daily_sales_closed
        WITH (SELECT max(created_at) FROM consistent.orders(v = {v})) AS latest
        SELECT now64(3) AS closed_at, d.*
        FROM gold.daily_sales(v = {v}) AS d
        WHERE d.order_date < toDate(latest - INTERVAL 1 HOUR, 'Asia/Ho_Chi_Minh')
          AND d.order_date NOT IN (SELECT order_date FROM gold.daily_sales_closed)""")
    return ch.query("SELECT count() FROM gold.daily_sales_closed").result_rows[0][0] - before


def reconcile(ch, pg, v: int) -> list:
    """Rows created up to a cutoff one business hour before the watermark: committed long enough ago that
    both sides must have them. Returns the tables that disagreed two runs in a row."""
    latest = ch.query(f"SELECT max(created_at) FROM consistent.orders(v = {v})").result_rows[0][0]
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
            f"SELECT count(), {total} FROM consistent.{table}(v = {v}) WHERE created_at <= {ch_cutoff}").result_rows[0]
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
    next_cleanup = 0.0
    next_mart = next_recon = 0.0
    while True:
        try:
            if ch is None:
                ch = clickhouse()
                create_marts(ch)
            pg = pg or postgres()
            v = watermark(ch)
            if time.monotonic() >= next_mart:
                next_mart = time.monotonic() + MART_REFRESH_S
                refresh_marts(ch, v)
            closed = close_business_days(ch, v)
            if closed:
                log.info("closed %d business day(s) at watermark %d", closed, v)
            if time.monotonic() >= next_recon:
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
        time.sleep(LOOP_S)


if __name__ == "__main__":
    main()
