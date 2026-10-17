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
  pipeline    — PostgreSQL vs ClickHouse silver (30 tables) and outbox vs bronze.domain_events

Run with the simulator stopped so the pipeline can settle:
    docker compose --profile tools run --rm validator python check_data.py
Exit code 0 = every check passed.
"""

import os
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
UNFULFILLED = ("CANCELLED", "RETURNED_TO_SELLER")
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
    sql_check("promo", "voucher used_count = applied usages ≤ limit; per-customer limit", """
        SELECT v.code, v.used_count, count(u.id) FILTER (WHERE u.status = 'APPLIED') AS applied, v.usage_limit
        FROM ecommerce.vouchers v LEFT JOIN ecommerce.voucher_usages u ON u.voucher_id = v.id GROUP BY v.id
        HAVING v.used_count <> count(u.id) FILTER (WHERE u.status = 'APPLIED') OR v.used_count > v.usage_limit
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
    sql_check("settlement", "wallet = Σ ledger = last running balance; payout = its PAYOUT entry", """
        WITH l AS (SELECT shop_id, sum(amount) total, (array_agg(balance_after ORDER BY id DESC))[1] last
                   FROM ecommerce.seller_ledger_entries GROUP BY 1)
        SELECT s.name, s.wallet_balance, l.total, l.last FROM ecommerce.shops s LEFT JOIN l ON l.shop_id = s.id
        WHERE s.wallet_balance <> coalesce(l.total, 0) OR s.wallet_balance <> coalesce(l.last, 0)
        UNION ALL
        SELECT p.id::text, p.amount, -sum(e.amount), count(e.id) FROM ecommerce.seller_payouts p
        LEFT JOIN ecommerce.seller_ledger_entries e ON e.payout_id = p.id AND e.entry_type = 'PAYOUT'
        GROUP BY p.id, p.amount HAVING count(e.id) <> 1 OR -sum(e.amount) <> p.amount""")
    sql_check("settlement", "shop rating aggregates = Σ reviews; reviews only on completed orders by the buyer", """
        WITH r AS (SELECT shop_id, count(*) n, sum(rating) s FROM ecommerce.product_reviews GROUP BY 1)
        SELECT s.name, s.rating_count, r.n, s.rating_sum, r.s FROM ecommerce.shops s LEFT JOIN r ON r.shop_id = s.id
        WHERE s.rating_count <> coalesce(r.n, 0) OR s.rating_sum <> coalesce(r.s, 0)
        UNION ALL
        SELECT pr.id::text, NULL, NULL, NULL, NULL FROM ecommerce.product_reviews pr JOIN ecommerce.orders o ON o.id = pr.order_id
        WHERE o.status <> 'COMPLETED' OR o.customer_id <> pr.customer_id OR o.shop_id <> pr.shop_id""")


# ---------------------------------------------------------------------------- write path

def write_path():
    events = q("SELECT type, aggregateid, payload FROM ecommerce.outbox_events ORDER BY created_at, id")
    by_type = defaultdict(list)
    for e in events:
        by_type[e["type"]].append(e)

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


# ---------------------------------------------------------------------------- pipeline

TABLES = ["customers", "customer_addresses", "sellers", "shops", "categories", "brands", "products", "product_skus",
          "inventory", "inventory_movements", "carts", "cart_items", "checkouts", "orders", "order_items",
          "order_status_history", "payments", "refunds", "shipments", "shipment_events", "price_history", "vouchers",
          "voucher_usages", "campaigns", "flash_sale_items", "returns", "return_items", "seller_ledger_entries",
          "seller_payouts", "product_reviews"]
AGGS = {
    "customers": ["sum:total_spent", "where:is_active"], "shops": ["sum:penalty_points", "sum:wallet_balance", "sum:rating_sum"],
    "product_skus": ["sum:price"], "inventory": ["sum:on_hand", "sum:reserved", "sum:available"],
    "inventory_movements": ["sum:on_hand_delta", "sum:reserved_delta"], "carts": ["where:status = 'ACTIVE'"],
    "cart_items": ["sum:quantity"], "checkouts": ["sum:grand_total", "sum:discount_total", "where:status = 'PAID'"],
    "orders": ["sum:total", "sum:platform_discount", "where:status = 'COMPLETED'", "where:status = 'CANCELLED'",
               "sum:delivery_attempts"],
    "order_items": ["sum:line_total", "sum:net_amount", "sum:quantity"],
    "payments": ["sum:amount", "where:status = 'SUCCEEDED'"], "refunds": ["sum:amount"],
    "shipments": ["sum:attempts", "where:status = 'DELIVERED'"], "shipment_events": ["where:applied"],
    "price_history": ["sum:new_price"], "vouchers": ["sum:used_count"], "voucher_usages": ["sum:amount", "where:status = 'APPLIED'"],
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
    with pg_live.cursor(cursor_factory=psycopg2.extensions.cursor) as cur:
        cur.execute("SELECT type, count(*) FROM ecommerce.outbox_events GROUP BY 1")
        pg_events = dict(cur.fetchall())
    ch_events = dict(ch.query("SELECT event_type, uniqExact(event_id) FROM bronze.domain_events GROUP BY 1").result_rows)
    event_diffs = [f"{t} pg={pg_events.get(t, 0)} ch={ch_events.get(t, 0)}"
                   for t in set(pg_events) | set(ch_events) if pg_events.get(t, 0) != ch_events.get(t, 0)]
    return diffs, event_diffs


def pipeline():
    deadline = time.time() + SETTLE_TIMEOUT_S
    diffs, event_diffs = reconcile_once()
    while (diffs or event_diffs) and time.time() < deadline:
        time.sleep(3)
        diffs, event_diffs = reconcile_once()
    record("pipeline", f"silver FINAL = PostgreSQL ({len(TABLES)} tables, counts and sums)", diffs)
    record("pipeline", "domain events: outbox = bronze.domain_events per type", event_diffs)
    dup_cdc = ch.query("SELECT count() - uniqExact(kafka_topic, kafka_partition, kafka_offset) FROM bronze.cdc_events").result_rows[0][0]
    dup_evt = ch.query("SELECT count() - uniqExact(event_id) FROM bronze.domain_events").result_rows[0][0]
    record("pipeline", "duplicate rows in bronze (at-least-once delivery)",
           [f"cdc_events: {dup_cdc}"] * (dup_cdc > 0) + [f"domain_events: {dup_evt}"] * (dup_evt > 0), warn_only=True)


def main():
    for group in (money, state, stock, logistics, promo, returns, settlement, write_path, pipeline):
        group()
    summary = q("""SELECT (SELECT count(*) FROM ecommerce.customers) customers, (SELECT count(*) FROM ecommerce.shops) shops,
                          (SELECT count(*) FROM ecommerce.product_skus) skus, (SELECT count(*) FROM ecommerce.checkouts) checkouts,
                          (SELECT count(*) FROM ecommerce.orders) orders, (SELECT count(*) FROM ecommerce.returns) returns,
                          (SELECT count(*) FROM ecommerce.shipment_events) tracking_events,
                          (SELECT count(*) FROM ecommerce.outbox_events) events,
                          (SELECT round(extract(epoch FROM ecommerce.business_now() - min(created_at)) / 86400, 1)
                             FROM ecommerce.orders) AS business_days""")[0]
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
