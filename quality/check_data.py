"""Data validation for the marketplace source and its warehouse copy.

Groups:
  money       — line / order / checkout totals, payments, refunds (cancel, late payment, returns), COD, loyalty
  state       — checkout vs order, milestone timestamps, carts, time-based rules applied on schedule (business time)
  stock       — inventory = Σ ledger; every ledger movement matches the business event that caused it
  logistics   — parcel status agrees with order / return status; stale tracking events really were stale
  promo       — voucher counters, limits, windows and allocation to lines; flash quota; price at order time
  returns     — return lines, refund amounts, order outcome of each return
  settlement  — seller ledger per completed order, wallet balance, payouts; shop rating aggregates
  write-path  — every row has its creation event; status history replays via allowed moves and actors
  pipeline    — PostgreSQL vs ClickHouse silver (31 tables), consistent.* vs silver, outbox vs bronze.domain_events
  metrics     — every gold metric equals the same definition computed in PostgreSQL; closed days stay as reported
  ops         — marts equal gold at their watermark; scheduled reconciliation agrees; PII never reaches the warehouse

Run with the simulator stopped so the pipeline can settle:
    docker compose --profile tools run --rm validator python check_data.py
Exit code 0 = every check passed.
"""

import json
import os
import re
import sys
import time
from collections import Counter, defaultdict

import clickhouse_connect
import psycopg2
import psycopg2.extras

TIERS = [(50_000_000, "PLATINUM"), (20_000_000, "GOLD"), (5_000_000, "SILVER"), (0, "BRONZE")]
TRANSITIONS = {
    ("UNPAID", "TO_SHIP"): {"PAYMENT"}, ("UNPAID", "CANCELLED"): {"BUYER", "SYSTEM"},
    ("TO_SHIP", "READY_TO_SHIP"): {"SELLER"}, ("TO_SHIP", "CANCELLED"): {"BUYER", "SELLER", "SYSTEM"},
    ("READY_TO_SHIP", "SHIPPED"): {"LOGISTICS"}, ("SHIPPED", "DELIVERED"): {"LOGISTICS"},
    ("SHIPPED", "DELIVERY_FAILED"): {"LOGISTICS"}, ("DELIVERY_FAILED", "SHIPPED"): {"LOGISTICS"},
    ("DELIVERY_FAILED", "RETURNED_TO_SELLER"): {"LOGISTICS"}, ("DELIVERED", "COMPLETED"): {"BUYER", "SYSTEM"},
    ("DELIVERED", "RETURN_REFUND"): {"BUYER"}, ("RETURN_REFUND", "COMPLETED"): {"SYSTEM"},
}
OPEN = ("UNPAID", "TO_SHIP", "READY_TO_SHIP")          # stock still reserved
PAYMENT_FEE_BPS = 200
AUTO_COMPLETE_S = float(os.getenv("AUTO_COMPLETE_S", 3 * 86400))
DISPUTE_WINDOW_S = float(os.getenv("DISPUTE_WINDOW_S", 86400))
JOB_GRACE_REAL_S = 60                                    # scheduler runs every couple of wall-clock seconds
SETTLE_TIMEOUT_S = int(os.getenv("SETTLE_TIMEOUT_S", "180"))

PG = dict(host=os.getenv("PG_HOST", "postgres"), port=5432, user="admin", password="admin", dbname="enterprise_db",
          cursor_factory=psycopg2.extras.RealDictCursor)
# Background jobs keep changing data after the simulator stops: every check reads ONE consistent snapshot ...
pg = psycopg2.connect(**PG)
pg.set_session(isolation_level="REPEATABLE READ", readonly=True)
# ... while reconciliation polls live data until ClickHouse has caught up
pg_live = psycopg2.connect(**PG)
pg_live.set_session(readonly=True, autocommit=True)
ch = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), port=8123,
                                   username="admin", password="admin")
results = []


def q(sql, params=None) -> list:
    with pg.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def record(group, name, bad: list, describe=str, warn_only=False):
    detail = "; ".join(describe(r) for r in bad[:3]) + (f" … (+{len(bad) - 3})" if len(bad) > 3 else "")
    results.append((group, name, len(bad), detail, warn_only))


def sql_check(group, name, sql, describe=lambda r: ", ".join(f"{k}={v}" for k, v in list(r.items())[:4])):
    record(group, name, q(sql), describe)


def tier_for(spent):
    return next(t for threshold, t in TIERS if spent >= threshold)


# ---------------------------------------------------------------------------- money

def money():
    sql_check("money", "line_total = qty × unit_price; net = line − allocated discounts", """
        SELECT id, quantity, unit_price, line_total FROM ecommerce.order_items
        WHERE line_total <> quantity * unit_price
           OR net_amount <> line_total - shop_discount - platform_discount OR net_amount < 0""")
    sql_check("money", "order: subtotal = Σ lines; discounts = Σ line shares; total formula", """
        WITH l AS (SELECT order_id, sum(line_total) s, sum(shop_discount) sd, sum(platform_discount) pd
                   FROM ecommerce.order_items GROUP BY 1)
        SELECT o.order_number, o.subtotal, l.s, o.shop_discount, l.sd FROM ecommerce.orders o
        LEFT JOIN l ON l.order_id = o.id
        WHERE l.s IS DISTINCT FROM o.subtotal OR l.sd <> o.shop_discount OR l.pd <> o.platform_discount
           OR o.shipping_subsidy > o.shipping_fee
           OR o.total <> o.subtotal - o.shop_discount - o.platform_discount + o.shipping_fee - o.shipping_subsidy""")
    sql_check("money", "checkout totals = Σ of its orders", """
        WITH o AS (SELECT checkout_id, sum(subtotal) s, sum(shipping_fee) f, sum(total) t,
                          sum(shop_discount + platform_discount + shipping_subsidy) d FROM ecommerce.orders GROUP BY 1)
        SELECT c.id, c.items_total, o.s, c.grand_total, o.t FROM ecommerce.checkouts c LEFT JOIN o ON o.checkout_id = c.id
        WHERE o.s IS DISTINCT FROM c.items_total OR o.f <> c.shipping_total OR o.t <> c.grand_total
           OR o.d <> c.discount_total OR c.grand_total <> c.items_total + c.shipping_total - c.discount_total""")
    sql_check("money", "paid checkout has exactly one kept online payment = grand_total", """
        WITH kept AS (
            SELECT p.checkout_id, count(*) n, sum(p.amount) amt FROM ecommerce.payments p
            WHERE p.order_id IS NULL AND p.status = 'SUCCEEDED'
              AND NOT EXISTS (SELECT 1 FROM ecommerce.refunds r WHERE r.payment_id = p.id AND r.reason = 'late_payment')
            GROUP BY 1)
        SELECT c.id, c.status, kept.n, kept.amt, c.grand_total FROM ecommerce.checkouts c
        LEFT JOIN kept ON kept.checkout_id = c.id
        WHERE (c.status = 'PAID') <> (coalesce(kept.n, 0) = 1) OR (c.status = 'PAID' AND kept.amt <> c.grand_total)""")
    sql_check("money", "late / superseded online payments are refunded in full", """
        SELECT p.transaction_ref, p.amount, coalesce(sum(r.amount), 0) AS refunded FROM ecommerce.payments p
        JOIN ecommerce.checkouts c ON c.id = p.checkout_id
        LEFT JOIN ecommerce.refunds r ON r.payment_id = p.id AND r.reason = 'late_payment'
        WHERE p.order_id IS NULL AND p.status = 'SUCCEEDED'
        GROUP BY p.id, p.transaction_ref, p.amount, c.status
        HAVING (c.status <> 'PAID' AND coalesce(sum(r.amount), 0) <> p.amount)
            OR coalesce(sum(r.amount), 0) NOT IN (0, p.amount)""")
    sql_check("money", "non-return refunds: paid online order cancelled / returned to seller → exactly its total", """
        WITH r AS (SELECT order_id, sum(amount) amt FROM ecommerce.refunds
                   WHERE order_id IS NOT NULL AND return_id IS NULL GROUP BY 1)
        SELECT o.order_number, o.status, o.payment_method, o.total, r.amt FROM ecommerce.orders o
        LEFT JOIN r ON r.order_id = o.id
        WHERE coalesce(r.amt, 0) <> CASE WHEN o.payment_method <> 'COD' AND o.paid_at IS NOT NULL
                                              AND o.status IN ('CANCELLED', 'RETURNED_TO_SELLER') THEN o.total ELSE 0 END""")
    sql_check("money", "refunds never exceed the payment", """
        SELECT p.transaction_ref, p.amount, sum(r.amount) refunded FROM ecommerce.refunds r
        JOIN ecommerce.payments p ON p.id = r.payment_id GROUP BY p.id, p.transaction_ref, p.amount
        HAVING sum(r.amount) > p.amount""")
    sql_check("money", "COD order has one collection = total, status follows the order", """
        WITH p AS (SELECT order_id, count(*) n, max(amount) amt, max(status) st FROM ecommerce.payments
                   WHERE method = 'COD' GROUP BY 1)
        SELECT o.order_number, o.status, p.n, p.st FROM ecommerce.orders o LEFT JOIN p ON p.order_id = o.id
        WHERE o.payment_method = 'COD' AND (
              p.n IS DISTINCT FROM 1 OR p.amt <> o.total
           OR p.st <> CASE WHEN o.status IN ('DELIVERED', 'COMPLETED', 'RETURN_REFUND') THEN 'SUCCEEDED'
                           WHEN o.status IN ('CANCELLED', 'RETURNED_TO_SELLER') THEN 'CANCELLED'
                           ELSE 'PENDING' END)""")
    customers = q("""WITH paid AS (
                         SELECT o.customer_id, o.total - coalesce((SELECT sum(amount) FROM ecommerce.refunds r
                                                                   WHERE r.order_id = o.id), 0) AS kept
                         FROM ecommerce.orders o WHERE o.status = 'COMPLETED')
                     SELECT c.id, c.tier, c.total_spent, coalesce(sum(paid.kept), 0) AS completed
                     FROM ecommerce.customers c LEFT JOIN paid ON paid.customer_id = c.id GROUP BY c.id""")
    record("money", "total_spent = Σ (total − refunds) of completed orders; tier matches",
           [c for c in customers if c["total_spent"] != c["completed"] or c["tier"] != tier_for(c["total_spent"])],
           lambda c: f"{str(c['id'])[:8]} {c['tier']} spent={c['total_spent']} expected={c['completed']}")


# ---------------------------------------------------------------------------- state

def state():
    speed = q("SELECT speed FROM ecommerce.sim_clock")[0]["speed"]
    grace = JOB_GRACE_REAL_S * speed
    sql_check("state", "checkout status agrees with its orders", """
        SELECT c.id, c.status, array_agg(DISTINCT o.status) AS order_statuses
        FROM ecommerce.checkouts c JOIN ecommerce.orders o ON o.checkout_id = c.id GROUP BY c.id, c.status
        HAVING (c.status = 'PENDING_PAYMENT' AND bool_or(o.status <> 'UNPAID'))
            OR (c.status IN ('EXPIRED', 'CANCELLED') AND bool_or(o.status <> 'CANCELLED'))
            OR (c.status IN ('PAID', 'COD_CONFIRMED') AND bool_or(o.status = 'UNPAID'))""")
    sql_check("state", "milestone timestamps in order; each status has its timestamps", """
        SELECT order_number, status FROM ecommerce.orders
        WHERE NOT (created_at <= coalesce(paid_at, created_at)
               AND coalesce(paid_at, created_at) <= coalesce(ready_at, 'infinity')
               AND coalesce(ready_at, created_at) <= coalesce(shipped_at, 'infinity')
               AND coalesce(shipped_at, created_at) <= coalesce(delivered_at, 'infinity')
               AND coalesce(delivered_at, created_at) <= coalesce(completed_at, 'infinity'))
           OR (status IN ('SHIPPED', 'DELIVERY_FAILED', 'DELIVERED', 'COMPLETED', 'RETURNED_TO_SELLER', 'RETURN_REFUND')
               AND (ready_at IS NULL OR shipped_at IS NULL))
           OR (status IN ('DELIVERED', 'COMPLETED', 'RETURN_REFUND') AND delivered_at IS NULL)
           OR (status = 'COMPLETED' AND completed_at IS NULL)
           OR (status = 'CANCELLED' AND (cancelled_at IS NULL OR cancel_reason IS NULL OR cancelled_by IS NULL))
           OR (status <> 'CANCELLED' AND cancelled_at IS NOT NULL)
           OR (payment_method <> 'COD' AND status NOT IN ('UNPAID', 'CANCELLED') AND paid_at IS NULL)""")
    sql_check("state", "each checked-out cart has exactly one checkout of the same customer", """
        SELECT ca.id, ca.status, count(co.id) n FROM ecommerce.carts ca
        LEFT JOIN ecommerce.checkouts co ON co.cart_id = ca.id AND co.customer_id = ca.customer_id
        GROUP BY ca.id, ca.status HAVING (ca.status = 'CHECKED_OUT') <> (count(co.id) = 1)""")
    sql_check("state", "time rules applied on schedule (payment, SLA, auto-complete, returns)", f"""
        WITH t AS (SELECT ecommerce.business_now() - make_interval(secs => {grace}) AS due)
        SELECT 'unpaid past deadline' AS rule, id::text FROM ecommerce.checkouts, t
            WHERE status = 'PENDING_PAYMENT' AND pay_by < t.due
        UNION ALL SELECT 'to_ship past SLA', order_number FROM ecommerce.orders, t
            WHERE status = 'TO_SHIP' AND confirm_by < t.due
        UNION ALL SELECT 'delivered not auto-completed', order_number FROM ecommerce.orders, t
            WHERE status = 'DELIVERED' AND delivered_at < t.due - make_interval(secs => {AUTO_COMPLETE_S})
        UNION ALL SELECT 'return unanswered past deadline', id::text FROM ecommerce.returns, t
            WHERE status = 'REQUESTED' AND respond_by < t.due
        UNION ALL SELECT 'rejection not closed', id::text FROM ecommerce.returns, t
            WHERE status = 'REJECTED' AND updated_at < t.due - make_interval(secs => {DISPUTE_WINDOW_S})""")


# ---------------------------------------------------------------------------- stock

def stock():
    sql_check("stock", "inventory = Σ ledger deltas; last row's running balance = inventory", """
        WITH l AS (SELECT sku_id, sum(on_hand_delta) oh, sum(reserved_delta) rs,
                          (array_agg(on_hand_after ORDER BY id DESC))[1] oh_last,
                          (array_agg(reserved_after ORDER BY id DESC))[1] rs_last
                   FROM ecommerce.inventory_movements GROUP BY 1)
        SELECT i.sku_id, i.on_hand, l.oh, i.reserved, l.rs FROM ecommerce.inventory i LEFT JOIN l ON l.sku_id = i.sku_id
        WHERE coalesce(l.oh, 0) <> i.on_hand OR coalesce(l.rs, 0) <> i.reserved
           OR (l.oh_last IS NOT NULL AND (l.oh_last <> i.on_hand OR l.rs_last <> i.reserved))""")
    expected = Counter()
    for r in q("""SELECT oi.order_id, oi.sku_id, oi.quantity, o.status, o.shipped_at IS NOT NULL AS shipped
                  FROM ecommerce.order_items oi JOIN ecommerce.orders o ON o.id = oi.order_id"""):
        key = (str(r["order_id"]), str(r["sku_id"]))
        expected[("RESERVE",) + key] += r["quantity"]
        if r["status"] == "CANCELLED":
            expected[("RELEASE",) + key] += r["quantity"]
        if r["shipped"]:
            expected[("SHIP",) + key] += r["quantity"]
        if r["status"] == "RETURNED_TO_SELLER":
            expected[("RETURN_TO_SENDER",) + key] += r["quantity"]
    for r in q("""SELECT ri.return_id, oi.sku_id, ri.quantity FROM ecommerce.return_items ri
                  JOIN ecommerce.order_items oi ON oi.id = ri.order_item_id WHERE ri.condition = 'SELLABLE'"""):
        expected[("CUSTOMER_RETURN", str(r["return_id"]), str(r["sku_id"]))] += r["quantity"]
    actual = Counter()
    for r in q("""SELECT movement_type, ref_id, sku_id, abs(on_hand_delta) + abs(reserved_delta) AS units,
                         on_hand_delta, reserved_delta
                  FROM ecommerce.inventory_movements WHERE ref_type IN ('ORDER', 'RETURN')"""):
        qty = abs(r["reserved_delta"]) if r["movement_type"] in ("RESERVE", "RELEASE", "SHIP") else r["on_hand_delta"]
        actual[(r["movement_type"], str(r["ref_id"]), str(r["sku_id"]))] += qty
    diff = [f"{k[0]} {k[1][:8]} sku {k[2][:8]}: ledger {actual[k]} expected {expected[k]}"
            for k in set(expected) | set(actual) if expected[k] != actual[k]]
    record("stock", "each reserve / release / ship / return movement matches its order or return", diff)


# ---------------------------------------------------------------------------- logistics

def logistics():
    sql_check("logistics", "one forward parcel per packed order; parcel status matches the order", """
        SELECT o.order_number, o.status, s.status AS parcel, s.attempts, o.delivery_attempts
        FROM ecommerce.orders o LEFT JOIN ecommerce.shipments s ON s.order_id = o.id AND s.direction = 'FORWARD'
        WHERE (o.ready_at IS NOT NULL) <> (s.id IS NOT NULL)
           OR (s.id IS NOT NULL AND (s.attempts <> o.delivery_attempts OR NOT s.status::text = ANY(
                CASE o.status
                    WHEN 'READY_TO_SHIP' THEN ARRAY['CREATED']
                    WHEN 'SHIPPED' THEN ARRAY['PICKED_UP', 'IN_TRANSIT', 'OUT_FOR_DELIVERY']
                    WHEN 'DELIVERY_FAILED' THEN ARRAY['DELIVERY_FAILED']
                    WHEN 'RETURNED_TO_SELLER' THEN ARRAY['RETURNED_TO_SENDER']
                    ELSE ARRAY['DELIVERED'] END::text[])))""")
    sql_check("logistics", "return parcel exists from approval on; its status matches the return", """
        SELECT r.id, r.status, s.status AS parcel FROM ecommerce.returns r
        LEFT JOIN ecommerce.shipments s ON s.return_id = r.id AND s.direction = 'RETURN'
        WHERE (r.status IN ('APPROVED', 'RETURNING', 'RECEIVED', 'REFUNDED')) <> (s.id IS NOT NULL)
           OR (r.status = 'APPROVED' AND s.status <> 'CREATED')
           OR (r.status = 'RETURNING' AND s.status NOT IN ('PICKED_UP', 'IN_TRANSIT', 'OUT_FOR_DELIVERY'))
           OR (r.status IN ('RECEIVED', 'REFUNDED') AND s.status <> 'DELIVERED')""")
    sql_check("logistics", "applied tracking replays to the parcel status; ignored events were really stale", """
        WITH ev AS (SELECT e.*, max(e.event_time) FILTER (WHERE e.applied)
                                  OVER (PARTITION BY e.shipment_id ORDER BY e.id
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS latest_before
                    FROM ecommerce.shipment_events e),
             last AS (SELECT DISTINCT ON (shipment_id) shipment_id, event_type, event_time FROM ev
                      WHERE applied ORDER BY shipment_id, event_time DESC)
        SELECT s.tracking_number, s.status, last.event_type FROM ecommerce.shipments s
        LEFT JOIN last ON last.shipment_id = s.id
        WHERE coalesce(last.event_type, 'CREATED') <> s.status
           OR last.event_time IS DISTINCT FROM s.last_event_time
        UNION ALL
        SELECT s.tracking_number, 'stale event not stale', ev.event_type FROM ev JOIN ecommerce.shipments s ON s.id = ev.shipment_id
        WHERE NOT ev.applied AND (ev.latest_before IS NULL OR ev.event_time > ev.latest_before)""")


# ---------------------------------------------------------------------------- promotions

def promo():
    sql_check("promo", "quota slots: Σ capacity = limit; each slot's used = applied usages on it; per-customer limit", """
        WITH u AS (SELECT voucher_id, quota_slot, count(*) n FROM ecommerce.voucher_usages
                   WHERE status = 'APPLIED' GROUP BY 1, 2)
        SELECT v.code, q.slot, q.used, coalesce(u.n, 0) FROM ecommerce.voucher_quota_slots q
        JOIN ecommerce.vouchers v ON v.id = q.voucher_id
        LEFT JOIN u ON u.voucher_id = q.voucher_id AND u.quota_slot = q.slot
        WHERE q.used <> coalesce(u.n, 0) OR q.used > q.capacity
        UNION ALL
        SELECT u.voucher_id::text, u.quota_slot, u.n, NULL FROM u
        WHERE NOT EXISTS (SELECT 1 FROM ecommerce.voucher_quota_slots q WHERE q.voucher_id = u.voucher_id AND q.slot = u.quota_slot)
        UNION ALL
        SELECT v.code, NULL, v.usage_limit, sum(q.capacity) FROM ecommerce.vouchers v
        LEFT JOIN ecommerce.voucher_quota_slots q ON q.voucher_id = v.id
        GROUP BY v.id, v.code, v.usage_limit HAVING coalesce(sum(q.capacity), 0) <> v.usage_limit
        UNION ALL
        SELECT v.code, count(*), NULL, v.per_customer_limit FROM ecommerce.voucher_usages u
        JOIN ecommerce.vouchers v ON v.id = u.voucher_id WHERE u.status = 'APPLIED'
        GROUP BY v.code, v.per_customer_limit, u.customer_id HAVING count(*) > v.per_customer_limit""")
    sql_check("promo", "usages: inside the voucher window; released iff the checkout ended unpaid", """
        SELECT v.code, u.status, c.status AS checkout FROM ecommerce.voucher_usages u
        JOIN ecommerce.vouchers v ON v.id = u.voucher_id JOIN ecommerce.checkouts c ON c.id = u.checkout_id
        WHERE NOT (c.created_at >= v.starts_at AND c.created_at < v.ends_at)
           OR (u.status = 'RELEASED') <> (c.status IN ('EXPIRED', 'CANCELLED'))""")
    sql_check("promo", "voucher amounts = order discounts (shop per order, platform + freeship per checkout)", """
        WITH u AS (SELECT u.checkout_id, sum(u.amount) amt FROM ecommerce.voucher_usages u GROUP BY 1),
             s AS (SELECT u.order_id, sum(u.amount) amt FROM ecommerce.voucher_usages u
                   JOIN ecommerce.vouchers v ON v.id = u.voucher_id WHERE v.scope = 'SHOP' GROUP BY 1)
        SELECT c.id, c.discount_total, u.amt FROM ecommerce.checkouts c LEFT JOIN u ON u.checkout_id = c.id
        WHERE c.discount_total <> coalesce(u.amt, 0)
        UNION ALL
        SELECT o.id, o.shop_discount, s.amt FROM ecommerce.orders o LEFT JOIN s ON s.order_id = o.id
        WHERE o.shop_discount <> coalesce(s.amt, 0)""")
    sql_check("promo", "flash sold = units on live orders ≤ quota; flash lines priced at sale price in window", """
        SELECT f.id::text, f.sold, coalesce(sum(oi.quantity) FILTER (WHERE o.status <> 'CANCELLED'), 0) AS units, f.quota
        FROM ecommerce.flash_sale_items f LEFT JOIN ecommerce.order_items oi ON oi.campaign_item_id = f.id
        LEFT JOIN ecommerce.orders o ON o.id = oi.order_id GROUP BY f.id
        HAVING f.sold <> coalesce(sum(oi.quantity) FILTER (WHERE o.status <> 'CANCELLED'), 0) OR f.sold > f.quota
        UNION ALL
        SELECT oi.id::text, oi.unit_price, f.sale_price, NULL FROM ecommerce.order_items oi
        JOIN ecommerce.flash_sale_items f ON f.id = oi.campaign_item_id JOIN ecommerce.campaigns c ON c.id = f.campaign_id
        JOIN ecommerce.orders o ON o.id = oi.order_id
        WHERE oi.unit_price <> f.sale_price OR NOT (o.created_at >= c.starts_at AND o.created_at < c.ends_at)""")
    sql_check("promo", "regular lines priced at the SKU price in force when the order was placed", """
        SELECT o.order_number, oi.unit_price,
               (SELECT ph.new_price FROM ecommerce.price_history ph WHERE ph.sku_id = oi.sku_id
                  AND ph.created_at <= o.created_at ORDER BY ph.created_at DESC, ph.id DESC LIMIT 1) AS price_then
        FROM ecommerce.order_items oi JOIN ecommerce.orders o ON o.id = oi.order_id
        WHERE oi.campaign_item_id IS NULL
          AND oi.unit_price IS DISTINCT FROM (SELECT ph.new_price FROM ecommerce.price_history ph
                 WHERE ph.sku_id = oi.sku_id AND ph.created_at <= o.created_at
                 ORDER BY ph.created_at DESC, ph.id DESC LIMIT 1)""")


# ---------------------------------------------------------------------------- returns

def returns():
    sql_check("returns", "return lines within ordered qty; refund per line = net × qty ÷ ordered", """
        SELECT ri.id, ri.quantity, oi.quantity AS ordered, ri.refund_amount, oi.net_amount
        FROM ecommerce.return_items ri JOIN ecommerce.order_items oi ON oi.id = ri.order_item_id
        JOIN ecommerce.returns r ON r.id = ri.return_id
        WHERE ri.quantity > oi.quantity OR oi.order_id <> r.order_id
           OR ri.refund_amount <> oi.net_amount * ri.quantity / oi.quantity
           OR (r.status = 'REFUNDED') <> (ri.condition IS NOT NULL)""")
    sql_check("returns", "refunded return: refund rows = Σ line refunds; others refund nothing", """
        WITH li AS (SELECT return_id, sum(refund_amount) amt FROM ecommerce.return_items GROUP BY 1),
             rf AS (SELECT return_id, sum(amount) amt FROM ecommerce.refunds WHERE return_id IS NOT NULL GROUP BY 1)
        SELECT r.id, r.status, r.refund_amount, li.amt, rf.amt FROM ecommerce.returns r
        LEFT JOIN li ON li.return_id = r.id LEFT JOIN rf ON rf.return_id = r.id
        WHERE coalesce(rf.amt, 0) <> r.refund_amount
           OR r.refund_amount <> CASE WHEN r.status = 'REFUNDED' THEN li.amt ELSE 0 END""")
    sql_check("returns", "order outcome follows its return (open → RETURN_REFUND, full refund stays, else COMPLETED)", """
        WITH units AS (SELECT r.id, (SELECT sum(quantity) FROM ecommerce.order_items WHERE order_id = r.order_id) AS ordered,
                              (SELECT sum(quantity) FROM ecommerce.return_items WHERE return_id = r.id) AS returned
                       FROM ecommerce.returns r)
        SELECT o.order_number, r.status, o.status AS order_status FROM ecommerce.returns r
        JOIN ecommerce.orders o ON o.id = r.order_id JOIN units u ON u.id = r.id
        WHERE o.status <> CASE WHEN r.status = 'CLOSED' THEN 'COMPLETED'
                               WHEN r.status = 'REFUNDED' AND u.returned < u.ordered THEN 'COMPLETED'
                               ELSE 'RETURN_REFUND' END
        UNION ALL
        SELECT o.order_number, 'no return', o.status FROM ecommerce.orders o
        WHERE o.status = 'RETURN_REFUND' AND NOT EXISTS (SELECT 1 FROM ecommerce.returns r WHERE r.order_id = o.id)""")


# ---------------------------------------------------------------------------- settlement & reviews

def settlement():
    expected = defaultdict(lambda: {"SALE": 0, "COMMISSION": 0, "PAYMENT_FEE": 0})
    for r in q("""SELECT o.id, o.status, s.commission_rate_bps AS bps, oi.quantity, oi.line_total, oi.shop_discount,
                         coalesce((SELECT ri.quantity FROM ecommerce.return_items ri JOIN ecommerce.returns rt
                                   ON rt.id = ri.return_id WHERE ri.order_item_id = oi.id AND rt.status = 'REFUNDED'), 0) AS ret
                  FROM ecommerce.orders o JOIN ecommerce.order_items oi ON oi.order_id = o.id
                  JOIN ecommerce.shops s ON s.id = o.shop_id WHERE o.status = 'COMPLETED'"""):
        e = expected[str(r["id"])]
        e["bps"] = r["bps"]
        e["SALE"] += (r["line_total"] - r["shop_discount"]) - (r["line_total"] - r["shop_discount"]) * r["ret"] // r["quantity"]
    for e in expected.values():
        e["COMMISSION"] = -(e["SALE"] * e.pop("bps") // 10_000)
        e["PAYMENT_FEE"] = -(e["SALE"] * PAYMENT_FEE_BPS // 10_000)
    actual = defaultdict(lambda: {"SALE": 0, "COMMISSION": 0, "PAYMENT_FEE": 0})
    for r in q("""SELECT order_id, entry_type, sum(amount) amt FROM ecommerce.seller_ledger_entries
                  WHERE order_id IS NOT NULL GROUP BY 1, 2"""):
        actual[str(r["order_id"])][r["entry_type"]] = r["amt"]
    record("settlement", "completed order settles: sale on kept goods − shop voucher, commission, fee; others none",
           [f"{k[:8]} expected {dict(expected[k])} got {dict(actual[k])}"
            for k in set(expected) | set(actual) if dict(expected[k]) != dict(actual[k])])
    sql_check("settlement", "wallet + pending = Σ ledger; pending rows match their entries; payout = its PAYOUT entry", """
        WITH l AS (SELECT shop_id, sum(amount) total FROM ecommerce.seller_ledger_entries GROUP BY 1),
             p AS (SELECT shop_id, sum(amount) pending FROM ecommerce.wallet_pending GROUP BY 1)
        SELECT s.name, w.balance, p.pending, l.total FROM ecommerce.shops s
        LEFT JOIN ecommerce.shop_wallets w ON w.shop_id = s.id
        LEFT JOIN l ON l.shop_id = s.id LEFT JOIN p ON p.shop_id = s.id
        WHERE w.shop_id IS NULL OR w.balance + coalesce(p.pending, 0) <> coalesce(l.total, 0)
        UNION ALL
        SELECT wp.entry_id::text, wp.amount, e.amount, NULL FROM ecommerce.wallet_pending wp
        LEFT JOIN ecommerce.seller_ledger_entries e ON e.id = wp.entry_id
        WHERE e.id IS NULL OR e.amount <> wp.amount OR e.shop_id <> wp.shop_id
        UNION ALL
        SELECT p.id::text, p.amount, -sum(e.amount), count(e.id) FROM ecommerce.seller_payouts p
        LEFT JOIN ecommerce.seller_ledger_entries e ON e.payout_id = p.id AND e.entry_type = 'PAYOUT'
        GROUP BY p.id, p.amount HAVING count(e.id) <> 1 OR -sum(e.amount) <> p.amount""")
    sql_check("settlement", "reviews only on completed orders, by the buyer, for that shop", """
        SELECT pr.id::text FROM ecommerce.product_reviews pr JOIN ecommerce.orders o ON o.id = pr.order_id
        WHERE o.status <> 'COMPLETED' OR o.customer_id <> pr.customer_id OR o.shop_id <> pr.shop_id""")


# ---------------------------------------------------------------------------- write path

def domain_events() -> list:
    """Domain events as delivered to ClickHouse, one per event id (the PostgreSQL outbox is always empty)."""
    rows = ch.query("""SELECT event_type, aggregate_id, any(payload) FROM bronze.domain_events
                       GROUP BY event_id, event_type, aggregate_id""").result_rows
    return [{"type": t, "aggregateid": a, "payload": json.loads(p)} for t, a, p in rows]


def write_path():
    """Every row against its domain events. Runs on live data after the pipeline settled; jobs keep writing,
    so it retries until rows and events agree (or the time is up)."""
    global q
    snapshot_q, deadline = q, time.time() + 90
    q = lambda sql, params=None: _live(sql, params)
    try:
        while True:
            mark = len(results)
            _write_path_once()
            failing = [r for r in results[mark:] if r[2]]
            if not failing or time.time() > deadline:
                return
            del results[mark:]
            time.sleep(3)
    finally:
        q = snapshot_q


def _live(sql, params=None) -> list:
    with pg_live.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _write_path_once():
    by_type = defaultdict(list)
    for e in domain_events():
        by_type[e["type"]].append(e)
    left = q("SELECT count(*) AS n FROM ecommerce.outbox_events")[0]["n"]
    record("write-path", "outbox table is empty (rows are deleted in the transaction that wrote them)",
           [f"{left} rows"] * (left > 0))

    def ids(event_type, field=None):
        return Counter(str(e["payload"][field]) if field else e["aggregateid"] for e in by_type[event_type])

    for table, event_type, field in [("customers", "CustomerRegistered", None),
                                     ("customer_addresses", "AddressAdded", "address_id"),
                                     ("shops", "SellerOnboarded", None), ("products", "ProductListed", None),
                                     ("carts", "CartCreated", None), ("checkouts", "CheckoutCreated", None),
                                     ("orders", "OrderPlaced", None), ("payments", "PaymentCreated", None),
                                     ("refunds", "RefundIssued", "refund_id"), ("shipments", "ShipmentCreated", None),
                                     ("returns", "ReturnRequested", None), ("vouchers", "VoucherCreated", None),
                                     ("campaigns", "CampaignScheduled", None),
                                     ("seller_payouts", "PayoutPaid", "payout_id")]:
        seen = ids(event_type, field)
        rows = [str(r["id"]) for r in q(f"SELECT id FROM ecommerce.{table}")]
        record("write-path", f"{table}: one {event_type} per row",
               [r for r in rows if seen[r] != 1] + [f"event for missing {k}" for k in set(seen) - set(rows)])

    applied = Counter(str(r["shipment_id"]) for r in q("SELECT shipment_id FROM ecommerce.shipment_events WHERE applied"))
    tracked = ids("ShipmentTracked")
    record("write-path", "one ShipmentTracked event per applied tracking event",
           [k for k in set(applied) | set(tracked) if applied[k] != tracked[k]])

    history = defaultdict(list)
    for h in q("SELECT order_id, from_status, to_status, actor FROM ecommerce.order_status_history ORDER BY id"):
        history[str(h["order_id"])].append(h)
    moved = ids("OrderStatusChanged")
    bad = []
    for o in q("SELECT id, order_number, status FROM ecommerce.orders"):
        h = history[str(o["id"])]
        why = None
        if not h or h[0]["from_status"] is not None or h[0]["to_status"] not in ("UNPAID", "TO_SHIP"):
            why = "bad first row"
        else:
            for prev, cur in zip(h, h[1:]):
                if cur["from_status"] != prev["to_status"] or \
                        cur["actor"] not in TRANSITIONS.get((cur["from_status"], cur["to_status"]), set()):
                    why = f"{cur['from_status']}→{cur['to_status']} by {cur['actor']}"
                    break
            if not why and h[-1]["to_status"] != o["status"]:
                why = f"history ends {h[-1]['to_status']}, row is {o['status']}"
            if not why and moved[str(o["id"])] != len(h) - 1:
                why = f"{len(h) - 1} moves but {moved[str(o['id'])]} events"
        if why:
            bad.append(f"{o['order_number']}: {why}")
    record("write-path", "status history replays via allowed moves and actors, one event per move", bad)

    restocks = Counter()
    for e in by_type["InventoryRestocked"]:
        restocks[(e["payload"]["restock_id"], e["aggregateid"])] += e["payload"]["quantity"]
    ledger = Counter({(str(r["ref_id"]), str(r["sku_id"])): r["on_hand_delta"] for r in
                      q("SELECT ref_id, sku_id, on_hand_delta FROM ecommerce.inventory_movements WHERE ref_type = 'RESTOCK'")})
    listed = Counter()
    for e in by_type["ProductListed"]:
        for s in e["payload"]["skus"]:
            if s["initial_stock"]:
                listed[(e["aggregateid"], str(s["sku_id"]))] += s["initial_stock"]
    listed_ledger = Counter({(str(r["ref_id"]), str(r["sku_id"])): r["on_hand_delta"] for r in
                             q("SELECT ref_id, sku_id, on_hand_delta FROM ecommerce.inventory_movements WHERE ref_type = 'LISTING'")})
    record("write-path", "listing and restock movements = ProductListed / InventoryRestocked events",
           [k for k in set(restocks) | set(ledger) if restocks[k] != ledger[k]] +
           [k for k in set(listed) | set(listed_ledger) if listed[k] != listed_ledger[k]])


def no_deadlocks():
    n = q("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")[0]["deadlocks"]
    record("write-path", "no deadlocks in PostgreSQL (the API retries them, so clients never see one)",
           [f"{n} deadlocks since stats reset"] * (n > 0))


# ---------------------------------------------------------------------------- pipeline

TABLES = ["customers", "customer_addresses", "sellers", "shops", "categories", "brands", "products", "product_skus",
          "inventory", "inventory_movements", "carts", "cart_items", "checkouts", "orders", "order_items",
          "order_status_history", "payments", "refunds", "shipments", "shipment_events", "price_history", "vouchers",
          "voucher_usages", "campaigns", "flash_sale_items", "returns", "return_items", "seller_ledger_entries",
          "seller_payouts", "product_reviews", "shop_wallets"]
AGGS = {
    "customers": ["sum:total_spent", "where:is_active"], "shops": ["sum:penalty_points"], "shop_wallets": ["sum:balance"],
    "product_skus": ["sum:price"], "inventory": ["sum:on_hand", "sum:reserved", "sum:available"],
    "inventory_movements": ["sum:on_hand_delta", "sum:reserved_delta"], "carts": ["where:status = 'ACTIVE'"],
    "cart_items": ["sum:quantity"], "checkouts": ["sum:grand_total", "sum:discount_total", "where:status = 'PAID'"],
    "orders": ["sum:total", "sum:platform_discount", "where:status = 'COMPLETED'", "where:status = 'CANCELLED'",
               "sum:delivery_attempts"],
    "order_items": ["sum:line_total", "sum:net_amount", "sum:quantity"],
    "payments": ["sum:amount", "where:status = 'SUCCEEDED'"], "refunds": ["sum:amount"],
    "shipments": ["sum:attempts", "where:status = 'DELIVERED'"], "shipment_events": ["where:applied"],
    "price_history": ["sum:new_price"], "vouchers": ["sum:usage_limit"], "voucher_usages": ["sum:amount", "sum:quota_slot", "where:status = 'APPLIED'"],
    "flash_sale_items": ["sum:sold"], "returns": ["sum:refund_amount", "where:status = 'REFUNDED'"],
    "return_items": ["sum:refund_amount"], "seller_ledger_entries": ["sum:amount"], "seller_payouts": ["sum:amount"],
    "product_reviews": ["sum:rating"],
}


def _pairs(table: str) -> list:
    pairs = [("count(*)", "count()")]
    for s in AGGS.get(table, []):
        kind, arg = s.split(":", 1)
        pairs.append((f"coalesce(sum({arg}), 0)", f"sum({arg})") if kind == "sum"
                     else (f"count(*) FILTER (WHERE {arg})", f"countIf({arg})"))
    return pairs


def reconcile_once():
    diffs = []
    for t in TABLES:
        pairs = _pairs(t)
        with pg_live.cursor(cursor_factory=psycopg2.extensions.cursor) as cur:
            cur.execute(f"SELECT {', '.join(p for p, _ in pairs)} FROM ecommerce.{t}")
            a = tuple(int(v) for v in cur.fetchone())
        b = tuple(int(v or 0) for v in ch.query(f"SELECT {', '.join(c for _, c in pairs)} FROM silver.{t} FINAL").result_rows[0])
        if a != b:
            diffs.append(f"{t} pg={a} ch={b}")
    # each domain event is an outbox insert + delete in a committed transaction (Debezium transaction metadata)
    written, delivered = ch.query("""
        SELECT (SELECT intDiv(sum(c), 2) FROM (SELECT tx_id, max(outbox_changes) AS c FROM bronze.cdc_transactions
                                               GROUP BY tx_id)),
               (SELECT uniqExact(event_id) FROM bronze.domain_events)""").result_rows[0]
    event_diffs = [f"committed {written}, delivered {delivered}"] * (written != delivered)
    return diffs, event_diffs


def pipeline():
    deadline = time.time() + SETTLE_TIMEOUT_S
    diffs, event_diffs = reconcile_once()
    while (diffs or event_diffs) and time.time() < deadline:
        time.sleep(3)
        diffs, event_diffs = reconcile_once()
    record("pipeline", f"silver FINAL = PostgreSQL ({len(TABLES)} tables, counts and sums)", diffs)
    record("pipeline", "domain events: every committed outbox insert reached bronze.domain_events", event_diffs)
    errors = ch.query("SELECT kafka_topic, kafka_offset, error FROM bronze.ingest_errors LIMIT 3").result_rows
    record("pipeline", "no Kafka message failed to ingest (bronze.ingest_errors)", [f"{t}@{o}: {e[:80]}" for t, o, e in errors])
    v, held_back = ch.query("SELECT v, held_back FROM consistent.watermark").result_rows[0]
    view_diffs = []
    for t in TABLES:
        cols = ", ".join(c for _, c in _pairs(t))
        a = ch.query(f"SELECT {cols} FROM silver.{t} FINAL").result_rows[0]
        b = ch.query(f"SELECT {cols} FROM consistent.{t}(v = {v})").result_rows[0]
        if a != b:
            view_diffs.append(f"{t} silver={a} consistent={b}")
    record("pipeline", "settled: watermark holds nothing back and consistent.*(v) = silver FINAL",
           [f"{held_back} transactions held back"] * (held_back > 0) + view_diffs)
    dup_cdc = ch.query("SELECT count() - uniqExact(kafka_topic, kafka_partition, kafka_offset) FROM bronze.cdc_events").result_rows[0][0]
    dup_evt = ch.query("SELECT count() - uniqExact(event_id) FROM bronze.domain_events").result_rows[0][0]
    record("pipeline", "duplicate rows in bronze (at-least-once delivery)",
           [f"cdc_events: {dup_cdc}"] * (dup_cdc > 0) + [f"domain_events: {dup_evt}"] * (dup_evt > 0), warn_only=True)


# ---------------------------------------------------------------------------- metrics

ORDER_FACTS = """
    WITH r AS (SELECT oi.order_id, sum(ri.quantity * oi.unit_price) AS gross
               FROM ecommerce.return_items ri
               JOIN ecommerce.returns rt ON rt.id = ri.return_id AND rt.status = 'REFUNDED'
               JOIN ecommerce.order_items oi ON oi.id = ri.order_item_id GROUP BY 1)
    SELECT o.*, (o.paid_at IS NOT NULL OR o.payment_method = 'COD') AS confirmed,
           o.status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER') AS kept, coalesce(r.gross, 0) AS returned_gross
    FROM ecommerce.orders o LEFT JOIN r ON r.order_id = o.id"""

METRICS = {
    "daily sales: orders, placed GMV, GMV, NMV, unpaid cancellations per business day": (
        f"""WITH f AS ({ORDER_FACTS})
            SELECT (created_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS d, count(*),
                   sum(subtotal), coalesce(sum(subtotal) FILTER (WHERE confirmed), 0),
                   coalesce(sum(subtotal - returned_gross) FILTER (WHERE confirmed AND kept), 0),
                   count(*) FILTER (WHERE status = 'CANCELLED' AND NOT confirmed)
            FROM f GROUP BY 1""",
        "SELECT order_date, orders, placed_gmv, gmv, nmv, unpaid_cancelled FROM gold.daily_sales"),
    "P&L: GMV, NMV, take, promo cost, refunds split by cause, payouts": (
        f"""WITH f AS ({ORDER_FACTS})
            SELECT (SELECT coalesce(sum(subtotal) FILTER (WHERE confirmed), 0) FROM f),
                   (SELECT coalesce(sum(subtotal - returned_gross) FILTER (WHERE confirmed AND kept), 0) FROM f),
                   (SELECT coalesce(-sum(amount) FILTER (WHERE entry_type IN ('COMMISSION', 'PAYMENT_FEE')), 0)
                      FROM ecommerce.seller_ledger_entries),
                   (SELECT coalesce(sum(platform_discount + shipping_subsidy) FILTER (WHERE confirmed AND kept), 0) FROM f),
                   (SELECT coalesce(sum(shop_discount) FILTER (WHERE confirmed AND kept), 0) FROM f),
                   (SELECT coalesce(sum(amount) FILTER (WHERE return_id IS NOT NULL), 0) FROM ecommerce.refunds),
                   (SELECT coalesce(sum(amount) FILTER (WHERE return_id IS NULL AND reason <> 'late_payment'), 0)
                      FROM ecommerce.refunds),
                   (SELECT coalesce(sum(amount) FILTER (WHERE reason = 'late_payment'), 0) FROM ecommerce.refunds),
                   (SELECT coalesce(sum(amount), 0) FROM ecommerce.seller_payouts)""",
        """SELECT gmv, nmv, take, platform_promo_cost, seller_promo_cost, return_refunds, cancellation_refunds,
                  late_payment_refunds, paid_to_sellers FROM gold.platform_pnl"""),
    "product sales: units, gross and net revenue after returns (confirmed, kept orders)": (
        f"""WITH f AS ({ORDER_FACTS}),
                 rt AS (SELECT ri.order_item_id, sum(ri.quantity) qty, sum(ri.refund_amount) amt
                        FROM ecommerce.return_items ri
                        JOIN ecommerce.returns r ON r.id = ri.return_id AND r.status = 'REFUNDED' GROUP BY 1)
            SELECT coalesce(sum(oi.quantity - coalesce(rt.qty, 0)), 0), coalesce(sum(oi.line_total), 0),
                   coalesce(sum(oi.net_amount - coalesce(rt.amt, 0)), 0)
            FROM ecommerce.order_items oi JOIN f ON f.id = oi.order_id AND f.confirmed AND f.kept
            LEFT JOIN rt ON rt.order_item_id = oi.id""",
        "SELECT sum(units_sold), sum(gross_revenue), sum(net_revenue) FROM gold.product_sales"),
    "pack time per shop: TO_SHIP → READY_TO_SHIP, COD included (hours)": (
        """WITH h AS (SELECT order_id, min(created_at) FILTER (WHERE to_status = 'TO_SHIP') a,
                             min(created_at) FILTER (WHERE to_status = 'READY_TO_SHIP') b
                      FROM ecommerce.order_status_history GROUP BY 1)
           SELECT s.name, avg(extract(epoch FROM date_trunc('second', h.b)) - extract(epoch FROM date_trunc('second', h.a))) / 3600
           FROM h JOIN ecommerce.orders o ON o.id = h.order_id JOIN ecommerce.shops s ON s.id = o.shop_id
           WHERE h.b IS NOT NULL GROUP BY 1""",
        "SELECT shop, avg_hours_to_pack FROM gold.shop_performance WHERE avg_hours_to_pack IS NOT NULL"),
    "return cohorts: delivered and returned orders per delivery date": (
        """SELECT (delivered_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date, count(*),
                  count(*) FILTER (WHERE EXISTS (SELECT 1 FROM ecommerce.returns r WHERE r.order_id = o.id))
           FROM ecommerce.orders o WHERE delivered_at IS NOT NULL GROUP BY 1""",
        "SELECT delivery_date, delivered, returned FROM gold.return_rate_by_cohort"),
}


TOLERANCE = {"pack time per shop: TO_SHIP → READY_TO_SHIP, COD included (hours)": 0.011}   # gold rounds to 0.01 h


def _keyed(rows) -> dict:
    """Leading non-numeric columns are the key (date, shop); the rest are numbers to compare."""
    out = {}
    for r in rows:
        nums = [v for v in r if isinstance(v, (int, float)) or type(v).__name__ == "Decimal"]
        out[tuple(str(v) for v in r[:len(r) - len(nums)])] = [float(v) for v in nums]
    return out


def metrics_once() -> dict:
    diffs = {}
    for name, (pg_sql, ch_sql) in METRICS.items():
        with pg_live.cursor(cursor_factory=psycopg2.extensions.cursor) as cur:
            cur.execute(pg_sql)
            a = _keyed(cur.fetchall())
        v = ch.query("SELECT v FROM consistent.watermark").result_rows[0][0]
        b = _keyed(ch.query(re.sub(r"FROM (gold\.\w+)", rf"FROM \1(v = {v})", ch_sql)).result_rows)
        tol = TOLERANCE.get(name, 0)
        diffs[name] = [f"{'/'.join(k) or 'total'} pg={a.get(k)} ch={b.get(k)}" for k in sorted(set(a) | set(b))
                       if a.get(k) is None or b.get(k) is None
                       or any(abs(x - y) > tol for x, y in zip(a[k], b[k]))]
    return diffs


def metrics():
    """Gold against the same definitions in PostgreSQL. Jobs keep changing data, so retry until both agree."""
    deadline = time.time() + 60
    diffs = metrics_once()
    while any(diffs.values()) and time.time() < deadline:
        time.sleep(3)
        diffs = metrics_once()
    for name, bad in diffs.items():
        record("metrics", name, bad)
    v = ch.query("SELECT v FROM consistent.watermark").result_rows[0][0]
    closed = ch.query(f"""
        SELECT c.order_date, c.placed_gmv, d.placed_gmv, c.gmv, d.gmv
        FROM gold.daily_sales_closed AS c
        INNER JOIN gold.daily_sales(v = {v}) AS d ON d.order_date = c.order_date
        WHERE c.placed_gmv != d.placed_gmv OR c.gmv != d.gmv""").result_rows
    dup = ch.query("SELECT order_date FROM gold.daily_sales_closed GROUP BY 1 HAVING count() > 1").result_rows
    n_closed = ch.query("SELECT count() FROM gold.daily_sales_closed").result_rows[0][0]
    record("metrics", f"closed days ({n_closed}) frozen once; their placed GMV and GMV never change afterwards",
           [f"{r[0]} placed {r[1]}→{r[2]} gmv {r[3]}→{r[4]}" for r in closed] + [f"{r[0]} closed twice" for r in dup])


# ---------------------------------------------------------------------------- ops (warehouse-jobs, PII)

def _wait_for(sql, timeout_s=120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if ch.query(sql).result_rows[0][0]:
            return True
        time.sleep(3)
    return False


def ops():
    # background jobs keep committing, so the watermark moves on: compare the first refresh at or after it
    v = ch.query("SELECT v FROM consistent.watermark").result_rows[0][0]
    bad = []
    if not _wait_for(f"SELECT count() FROM ops.mart_refreshes WHERE refresh_id >= {v}"):
        bad.append(f"no mart refresh at or after watermark {v} within 120 s")
    else:
        v = ch.query(f"SELECT min(refresh_id) FROM ops.mart_refreshes WHERE refresh_id >= {v}").result_rows[0][0]
        for name in ("overview_kpis", "daily_sales", "platform_pnl", "product_sales", "shop_performance"):
            diff = ch.query(f"""SELECT count() FROM (
                SELECT * EXCEPT (refresh_id, refreshed_at, _row) FROM mart.{name} WHERE refresh_id = {v}
                EXCEPT SELECT * FROM gold.{name}(v = {v}))""").result_rows[0][0]
            bad += [f"mart.{name}: {diff} rows differ from gold"] * (diff > 0)
    record("ops", "marts at the current watermark equal gold computed live", bad)

    settled = ch.query("SELECT now64(3)").result_rows[0][0]
    ran = _wait_for(f"SELECT count() FROM ops.reconciliation WHERE run_at > toDateTime64('{settled:%Y-%m-%d %H:%M:%S.%f}', 3, 'UTC')", 150)
    rows = ch.query("""SELECT table_name, pg_rows, ch_rows, pg_sum, ch_sum FROM ops.reconciliation
                       WHERE run_at = (SELECT max(run_at) FROM ops.reconciliation) AND NOT ok""").result_rows
    record("ops", "scheduled reconciliation after settling: every table matches PostgreSQL",
           (["no reconciliation run after the pipeline settled"] if not ran else []) +
           [f"{t}: rows {a}/{b} sum {c}/{d}" for t, a, b, c, d in rows])

    leaks = ch.query("""SELECT 'customers.email', count() FROM silver.customers FINAL WHERE email LIKE '%@%'
                        UNION ALL SELECT 'customers.phone', count() FROM silver.customers FINAL WHERE match(phone, '^0[0-9]{8,10}$')
                        UNION ALL SELECT 'orders.ship_to_address', count() FROM silver.orders FINAL WHERE position(ship_to_address, ', ') > 0
                        UNION ALL SELECT 'bronze before/after', count() FROM bronze.cdc_events
                            WHERE source_table = 'customers' AND (after LIKE '%@%' OR before LIKE '%@%')""").result_rows
    record("ops", "PII is hashed before it leaves PostgreSQL (no emails, phones or addresses in the warehouse)",
           [f"{c}: {n} rows in clear" for c, n in leaks if n])


def main():
    for group in (money, state, stock, logistics, promo, returns, settlement, no_deadlocks, pipeline, write_path,
                  metrics, ops):
        group()
    summary = q("""SELECT (SELECT count(*) FROM ecommerce.customers) customers, (SELECT count(*) FROM ecommerce.shops) shops,
                          (SELECT count(*) FROM ecommerce.product_skus) skus, (SELECT count(*) FROM ecommerce.checkouts) checkouts,
                          (SELECT count(*) FROM ecommerce.orders) orders, (SELECT count(*) FROM ecommerce.returns) returns,
                          (SELECT count(*) FROM ecommerce.shipment_events) tracking_events,
                          (SELECT round(extract(epoch FROM ecommerce.business_now() - min(created_at)) / 86400, 1)
                             FROM ecommerce.orders) AS business_days""")[0]
    summary["events"] = ch.query("SELECT uniqExact(event_id) FROM bronze.domain_events").result_rows[0][0]
    print(f"Data under test: {dict(summary)}\n")
    width = max(len(r[1]) for r in results)
    failed = 0
    for group, name, n, detail, warn_only in results:
        status = "PASS" if n == 0 else ("WARN" if warn_only else "FAIL")
        failed += status == "FAIL"
        print(f"[{status}] {group:<10} {name:<{width}}  {'' if n == 0 else f'{n}: {detail}'}")
    print(f"\n{len(results) - failed}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
