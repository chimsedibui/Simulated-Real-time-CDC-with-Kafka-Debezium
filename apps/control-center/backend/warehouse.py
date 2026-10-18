"""Read-only queries against the ClickHouse bronze/silver/gold layers."""

import datetime as dt
import threading
from decimal import Decimal

import clickhouse_connect

LAYER_TABLES = ["customers", "shops", "product_skus", "checkouts", "orders", "order_items", "order_status_history",
                "payments", "refunds", "shipments", "shipment_events", "inventory_movements", "voucher_usages",
                "returns", "seller_ledger_entries", "product_reviews"]


class Warehouse:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.cfg = {"host": host, "port": port, "username": user, "password": password}
        self.local = threading.local()

    def _client(self):
        # clickhouse-connect clients are not thread-safe; FastAPI runs sync routes in a pool
        if getattr(self.local, "client", None) is None:
            self.local.client = clickhouse_connect.get_client(**self.cfg)
        return self.local.client

    def rows(self, sql: str) -> list:
        try:
            result = self._client().query(sql)
        except Exception:
            self.local.client = None
            raise
        return [
            {k: _jsonable(v) for k, v in zip(result.column_names, row)}
            for row in result.result_rows
        ]

    def ping(self) -> bool:
        try:
            return self._client().ping()
        except Exception:
            self.local.client = None
            return False

    def overview(self) -> dict:
        """Every mart from one refresh, i.e. one commit watermark, so the numbers on the page agree with each other.

        Marts are gold precomputed by warehouse-jobs every few seconds; before the first refresh exists the page
        computes gold live at the current watermark.
        """
        wm = self.rows("SELECT v, held_back FROM consistent.watermark")[0]
        refresh = self.rows("SELECT refresh_id, finished_at, duration_ms FROM ops.mart_refreshes "
                            "ORDER BY refresh_id DESC LIMIT 1")
        if refresh:
            r = refresh[0]["refresh_id"]

            def gold(name, limit=""):
                return self.rows(f"SELECT * EXCEPT (refresh_id, refreshed_at, _row) FROM mart.{name} "
                                 f"WHERE refresh_id = {r} ORDER BY _row {limit}")
        else:
            def gold(name, limit=""):
                return self.rows(f"SELECT * FROM gold.{name}(v = {wm['v']}) {limit}")

        layers = self.rows(
            "SELECT 'bronze.cdc_events' AS table, count() AS rows FROM bronze.cdc_events "
            "UNION ALL SELECT 'bronze.domain_events', count() FROM bronze.domain_events "
            + " ".join(f"UNION ALL SELECT 'silver.{t}', count() FROM silver.{t} FINAL" for t in LAYER_TABLES)
        )
        dq = self.rows("SELECT countIf(ok) AS ok, countIf(failing) AS failing, count() AS tables, max(last_run) AS last_run "
                       "FROM ops.reconciliation_status")[0]

        return {
            "kpis":              gold("overview_kpis")[0],
            "traffic_by_hour":   _fill_hours(gold("traffic_by_hour")),
            "order_status":      gold("order_status_summary"),
            "funnel":            gold("funnel"),
            "payments":          gold("payment_summary"),
            "category_sales":    gold("category_sales"),
            "top_products":      gold("product_sales", "LIMIT 8"),
            "shops":             gold("shop_performance", "LIMIT 10"),
            "inventory_risk":    gold("inventory_risk", "LIMIT 8"),
            "customer_tiers":    gold("customer_tiers"),
            "freshness":         self.rows("SELECT * FROM gold.cdc_freshness ORDER BY events_15m DESC LIMIT 8"),
            "pnl":               gold("platform_pnl")[0],
            "delivery":          gold("delivery_performance"),
            "returns":           gold("returns_summary"),
            "vouchers":          gold("voucher_performance", "LIMIT 8"),
            "flash_sales":       gold("flash_sale_performance", "LIMIT 6"),
            "settlement":        gold("seller_settlement", "LIMIT 8"),
            "layers":            layers,
            "consistency":       {"watermark": wm["v"], "held_back": wm["held_back"],
                                  "snapshot": refresh[0] if refresh else None},
            "data_quality":      dq,
        }


def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if hasattr(v, "hex") and not isinstance(v, (bytes, str, int, float)):  # UUID
        return str(v)
    return v


def _fill_hours(rows: list) -> list:
    """One point per business hour between the first and last hour with orders, zero-filled."""
    if not rows:
        return []
    by_hour = {r["hour"][:13]: r for r in rows}
    start = dt.datetime.fromisoformat(rows[0]["hour"][:13] + ":00")
    end = dt.datetime.fromisoformat(rows[-1]["hour"][:13] + ":00")
    out = []
    while start <= end:
        key = start.isoformat()[:13]
        r = by_hour.get(key, {})
        out.append({"hour": key + ":00Z", "orders": r.get("orders", 0), "placed_gmv": r.get("placed_gmv", 0),
                    "orders_with_voucher": r.get("orders_with_voucher", 0)})
        start += dt.timedelta(hours=1)
    return out
