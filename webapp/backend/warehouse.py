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
        kpis = self.rows("""
            SELECT
                count()                                                              AS orders,
                sumIf(subtotal, status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER'))   AS gmv,
                sumIf(total, status = 'COMPLETED')                                   AS completed_revenue,
                round(avgIf(total, status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER'))) AS avg_order_value,
                uniqExact(customer_id)                                               AS buyers,
                countIf(status = 'CANCELLED')                                        AS cancelled
            FROM silver.orders FINAL
        """)[0]
        kpis["active_customers"] = self.rows("SELECT countIf(is_active) AS n FROM silver.customers FINAL")[0]["n"]
        kpis["shops"] = self.rows("SELECT count() AS n FROM silver.shops FINAL")[0]["n"]


        layers = self.rows(
            "SELECT 'bronze.cdc_events' AS table, count() AS rows FROM bronze.cdc_events "
            "UNION ALL SELECT 'bronze.domain_events', count() FROM bronze.domain_events "
            + " ".join(f"UNION ALL SELECT 'silver.{t}', count() FROM silver.{t} FINAL" for t in LAYER_TABLES)
        )

        return {
            "kpis":              kpis,
            "traffic_by_hour":   _fill_hours(self.rows("SELECT * FROM gold.traffic_by_hour")),
            "order_status":      self.rows("SELECT * FROM gold.order_status_summary"),
            "funnel":            self.rows("SELECT * FROM gold.funnel"),
            "payments":          self.rows("SELECT * FROM gold.payment_summary"),
            "category_sales":    self.rows("SELECT * FROM gold.category_sales"),
            "top_products":      self.rows("SELECT * FROM gold.product_sales LIMIT 8"),
            "shops":             self.rows("SELECT * FROM gold.shop_performance LIMIT 10"),
            "inventory_risk":    self.rows("SELECT * FROM gold.inventory_risk LIMIT 8"),
            "customer_tiers":    self.rows("SELECT * FROM gold.customer_tiers"),
            "freshness":         self.rows("SELECT * FROM gold.cdc_freshness ORDER BY events_15m DESC LIMIT 8"),
            "pnl":               self.rows("SELECT * FROM gold.platform_pnl")[0],
            "delivery":          self.rows("SELECT * FROM gold.delivery_performance"),
            "returns":           self.rows("SELECT * FROM gold.returns_summary"),
            "vouchers":          self.rows("SELECT * FROM gold.voucher_performance LIMIT 8"),
            "flash_sales":       self.rows("SELECT * FROM gold.flash_sale_performance LIMIT 6"),
            "settlement":        self.rows("SELECT * FROM gold.seller_settlement LIMIT 8"),
            "layers":            layers,
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
        out.append({"hour": key + ":00Z", "orders": r.get("orders", 0), "gmv": r.get("gmv", 0),
                    "orders_with_voucher": r.get("orders_with_voucher", 0)})
        start += dt.timedelta(hours=1)
    return out
