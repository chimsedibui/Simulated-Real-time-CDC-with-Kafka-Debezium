"""Lock-contention stress: many checkouts, cancellations and payments on the same few rows at once.

Rare lock-order bugs (deadlocks) and lost updates need hours of ordinary traffic to show up; here every request
hits the same SKUs, flash items and vouchers, so they show up in seconds. The API retries deadlocks internally
and answers 503 only when retries run out, so this test reads PostgreSQL's deadlock counter instead of relying on
failed requests, then checks stock, flash quota and voucher quota against the orders.

    docker compose --profile tools run --rm validator pytest -q test_stress.py
"""

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import pytest

from test_source_api import add, api, buyer, checkout, listing, ok, pay, shop, tag

SECONDS = float(os.getenv("STRESS_SECONDS", "20"))
WORKERS = int(os.getenv("STRESS_WORKERS", "24"))
OPEN = ("UNPAID", "TO_SHIP", "READY_TO_SHIP")


def pg():
    return psycopg2.connect(host=os.getenv("PG_HOST", "postgres"), port=5432, user="admin", password="admin",
                            dbname="enterprise_db")


def deadlocks() -> int:
    with pg() as conn, conn.cursor() as cur:
        cur.execute("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")
        return cur.fetchone()[0]


@pytest.fixture(scope="module")
def hot_rows():
    ok(api.post("/admin/categories", json={"slug": "test-root", "name": "Test"}))
    ok(api.post("/admin/categories", json={"slug": "test-leaf", "name": "Test leaf", "parent_slug": "test-root"}))
    ok(api.post("/admin/brands", json={"name": "TestBrand"}))
    s1, s2 = shop("Hà Nội"), shop("TP.HCM")
    a, b = listing(s1["id"], price=150_000, stock=5000, variants=2)
    (c,) = listing(s2["id"], price=90_000, stock=5000)
    camp = ok(api.post("/admin/campaigns", json={
        "name": f"Stress {tag()}", "kind": "FLASH_SALE", "duration_s": 7 * 86400,
        "items": [{"sku_id": a, "sale_price": 99_000, "quota": 300}, {"sku_id": c, "sale_price": 59_000, "quota": 300}]}), 201)
    platform = f"SP{tag()}"
    ok(api.post("/admin/vouchers", json={"code": platform, "kind": "FIXED", "value": 5_000, "usage_limit": 5000,
                                         "per_customer_limit": 1000, "duration_s": 7 * 86400}), 201)
    shop_v = f"SS{tag()}"
    ok(api.post(f"/shops/{s1['id']}/vouchers", json={"code": shop_v, "kind": "FIXED", "value": 10_000,
                                                     "usage_limit": 5000, "per_customer_limit": 1000,
                                                     "duration_s": 7 * 86400}), 201)
    return {"skus": [a, b, c], "campaign": camp["id"], "vouchers": [platform, shop_v]}


def _shopper(customer_id, rows, deadline, tally):
    while time.time() < deadline:
        for sku in random.sample(rows["skus"], random.randint(2, 3)):     # usually two shops → two orders
            add(customer_id, sku, random.randint(1, 2))
        method = random.choice(["WALLET", "CARD", "COD"])
        r = checkout(customer_id, method, random.sample(rows["vouchers"], random.randint(0, 2)))
        tally[r.status_code] = tally.get(r.status_code, 0) + 1
        if r.status_code != 201:
            continue
        res = r.json()
        first = res["orders"][0]["id"]
        roll = random.random()
        if method != "COD" and roll < 0.5:                             # unpaid checkout cancelled: every order,
            api.post(f"/orders/{first}/cancel", json={"customer_id": customer_id})   # stock, flash and vouchers back
        elif method != "COD":
            pay(res["payment"]["transaction_ref"])
            if roll > 0.8:
                api.post(f"/orders/{first}/cancel", json={"customer_id": customer_id})   # paid: refund + release
        elif roll < 0.3:
            api.post(f"/orders/{first}/cancel", json={"customer_id": customer_id})


def test_hot_rows_under_contention_no_deadlock_no_drift(hot_rows):
    before = deadlocks()
    buyers = [buyer()["id"] for _ in range(WORKERS)]
    tally = {}
    deadline = time.time() + SECONDS
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        list(pool.map(lambda c: _shopper(c, hot_rows, deadline, tally), buyers))
    assert tally.get(201, 0) > 20, f"too little traffic to mean anything: {tally}"
    assert tally.get(500, 0) == 0, f"server errors: {tally}"

    with pg() as conn, conn.cursor() as cur:
        cur.execute("""SELECT i.sku_id, i.reserved, coalesce(sum(oi.quantity) FILTER (WHERE o.status = ANY(%s)), 0)
                       FROM ecommerce.inventory i
                       LEFT JOIN ecommerce.order_items oi ON oi.sku_id = i.sku_id
                       LEFT JOIN ecommerce.orders o ON o.id = oi.order_id
                       WHERE i.sku_id = ANY(%s::uuid[]) GROUP BY i.sku_id, i.reserved""",
                    (list(OPEN), hot_rows["skus"]))
        stock_drift = [r for r in cur.fetchall() if r[1] != r[2]]
        cur.execute("""SELECT f.id, f.sold, f.quota, coalesce(sum(oi.quantity) FILTER (WHERE o.status <> 'CANCELLED'), 0)
                       FROM ecommerce.flash_sale_items f
                       LEFT JOIN ecommerce.order_items oi ON oi.campaign_item_id = f.id
                       LEFT JOIN ecommerce.orders o ON o.id = oi.order_id
                       WHERE f.campaign_id = %s GROUP BY f.id, f.sold, f.quota""", (hot_rows["campaign"],))
        flash_drift = [r for r in cur.fetchall() if r[1] != r[3] or r[1] > r[2]]
        cur.execute("""SELECT v.code, sum(q.used), (SELECT count(*) FROM ecommerce.voucher_usages u
                                                     WHERE u.voucher_id = v.id AND u.status = 'APPLIED')
                       FROM ecommerce.vouchers v JOIN ecommerce.voucher_quota_slots q ON q.voucher_id = v.id
                       WHERE v.code = ANY(%s) GROUP BY v.id, v.code""", (hot_rows["vouchers"],))
        voucher_drift = [r for r in cur.fetchall() if r[1] != r[2]]
    assert not stock_drift, f"reserved stock ≠ open order units: {stock_drift}"
    assert not flash_drift, f"flash sold ≠ live units (or over quota): {flash_drift}"
    assert not voucher_drift, f"quota slots used ≠ applied usages: {voucher_drift}"
    assert deadlocks() == before, f"{deadlocks() - before} deadlock(s) during the run ({tally})"
