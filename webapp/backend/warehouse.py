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
        # One commit watermark for the whole page: every mart below sees the same transactions
        wm = self.rows("SELECT v, held_back FROM consistent.watermark")[0]
        v = wm["v"]
        # Same definitions as gold (see clickhouse/init/03-gold.sql)
        kpis = self.rows(f"""
            SELECT
                count()                                                          AS orders,
                sumIf(subtotal, confirmed)                                       AS gmv,
                sumIf(subtotal - returned_gross, confirmed AND kept)             AS nmv,
                sumIf(total, status = 'COMPLETED')                               AS completed_revenue,
                round(sumIf(subtotal, confirmed) / nullIf(countIf(confirmed), 0)) AS avg_order_value,
                uniqExactIf(customer_id, confirmed)                              AS buyers,
                countIf(status = 'CANCELLED')                                    AS cancelled
            FROM gold.order_facts(v = {v})
        """)[0]
        kpis["active_customers"] = self.rows(f"SELECT countIf(is_active) AS n FROM consistent.customers(v = {v})")[0]["n"]
        kpis["shops"] = self.rows(f"SELECT count() AS n FROM consistent.shops(v = {v})")[0]["n"]


        layers = self.rows(
            "SELECT 'bronze.cdc_events' AS table, count() AS rows FROM bronze.cdc_events "
            "UNION ALL SELECT 'bronze.domain_events', count() FROM bronze.domain_events "
            + " ".join(f"UNION ALL SELECT 'silver.{t}', count() FROM silver.{t} FINAL" for t in LAYER_TABLES)
        )

        return {
            "kpis":              kpis,
            "traffic_by_hour":   _fill_hours(self.rows(f"SELECT * FROM gold.traffic_by_hour(v = {v})")),
            "order_status":      self.rows(f"SELECT * FROM gold.order_status_summary(v = {v})"),
            "funnel":            self.rows(f"SELECT * FROM gold.funnel(v = {v})"),
            "payments":          self.rows(f"SELECT * FROM gold.payment_summary(v = {v})"),
            "category_sales":    self.rows(f"SELECT * FROM gold.category_sales(v = {v})"),
            "top_products":      self.rows(f"SELECT * FROM gold.product_sales(v = {v}) LIMIT 8"),
            "shops":             self.rows(f"SELECT * FROM gold.shop_performance(v = {v}) LIMIT 10"),
            "inventory_risk":    self.rows(f"SELECT * FROM gold.inventory_risk(v = {v}) LIMIT 8"),
            "customer_tiers":    self.rows(f"SELECT * FROM gold.customer_tiers(v = {v})"),
            "freshness":         self.rows("SELECT * FROM gold.cdc_freshness ORDER BY events_15m DESC LIMIT 8"),
            "pnl":               self.rows(f"SELECT * FROM gold.platform_pnl(v = {v})")[0],
            "delivery":          self.rows(f"SELECT * FROM gold.delivery_performance(v = {v})"),
            "returns":           self.rows(f"SELECT * FROM gold.returns_summary(v = {v})"),
            "vouchers":          self.rows(f"SELECT * FROM gold.voucher_performance(v = {v}) LIMIT 8"),
            "flash_sales":       self.rows(f"SELECT * FROM gold.flash_sale_performance(v = {v}) LIMIT 6"),
            "settlement":        self.rows(f"SELECT * FROM gold.seller_settlement(v = {v}) LIMIT 8"),
            "layers":            layers,
            "watermark":         wm,
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
