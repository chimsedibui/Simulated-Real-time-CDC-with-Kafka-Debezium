"""Scheduled warehouse jobs. Each run pins one commit watermark, so everything it reads is one consistent state.

    close_business_days  freeze each finished business day into gold.daily_sales_closed ("as reported"),
                         one business hour after it ends (payment windows are 15 minutes, so GMV is final)

Run by the warehouse-jobs service; JOB_INTERVAL_S sets the pace (wall clock).
"""

import logging
import os
import time

import clickhouse_connect

INTERVAL_S = float(os.getenv("JOB_INTERVAL_S", "10"))
log = logging.getLogger("warehouse-jobs")


def client():
    return clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), port=8123,
                                         username=os.getenv("CLICKHOUSE_USER", "admin"),
                                         password=os.getenv("CLICKHOUSE_PASSWORD", "admin"))


def watermark(ch) -> int:
    return ch.query("SELECT v FROM consistent.watermark").result_rows[0][0]


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


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ch = None
    while True:
        try:
            ch = ch or client()
            v = watermark(ch)
            closed = close_business_days(ch, v)
            if closed:
                log.info("closed %d business day(s) at watermark %d", closed, v)
        except Exception as e:                  # ClickHouse not up yet or restarting: try again next round
            log.warning("run skipped: %s", str(e).splitlines()[0][:200])
            ch = None
        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
