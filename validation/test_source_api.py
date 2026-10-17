"""Functional tests of the Source API marketplace rules (run against a live stack).

    docker compose --profile tools run --rm validator pytest -q test_source_api.py

Each test builds its own shops, listings and buyers through the API, so it can run alongside the
simulator and never depends on existing data.
"""

import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import httpx
import pytest

API = os.getenv("SOURCE_API_URL", "http://source-api:8100")
api = httpx.Client(base_url=API, timeout=30)


def ok(r: httpx.Response, status: int = 200) -> dict:
    assert r.status_code == status, f"{r.status_code}: {r.text}"
    return r.json()


def code(r: httpx.Response) -> str:
    return r.json()["detail"]["code"]


def tag() -> str:
    return uuid.uuid4().hex[:8].upper()


@pytest.fixture(scope="session", autouse=True)
def master_data():
    ok(api.post("/admin/categories", json={"slug": "test-root", "name": "Test"}))
    ok(api.post("/admin/categories", json={"slug": "test-leaf", "name": "Test leaf", "parent_slug": "test-root"}))
    ok(api.post("/admin/brands", json={"name": "TestBrand"}))


def shop(province="TP.HCM", tier="NORMAL") -> dict:
    t = tag()
    return ok(api.post("/sellers", json={"legal_name": f"Seller {t}", "email": f"s{t}@test.vn",
                                         "shop": {"name": f"Test shop {t}", "tier": tier, "province": province}}), 201)


def listing(shop_id, price=100_000, stock=50, weight_g=400, variants=1) -> list:
    t = tag()
    skus = [{"sku_code": f"T{t}-{i}", "variant_name": f"V{i}", "list_price": price, "price": price,
             "weight_g": weight_g, "initial_stock": stock} for i in range(variants)]
    body = {"category_slug": "test-leaf", "brand": "TestBrand", "name": f"Product {t}", "skus": skus}
    return [s["sku_id"] for s in ok(api.post(f"/shops/{shop_id}/products", json=body), 201)["skus"]]


def buyer(province="Hà Nội") -> dict:
    t = tag()
    return ok(api.post("/customers", json={
        "full_name": "Người Mua Test", "email": f"b{t}@test.vn", "phone": "0900000000",
        "address": {"recipient_name": "Người Mua Test", "phone": "0900000000", "street": "1 Lê Lợi",
                    "district": "Quận 1", "province": province}}), 201)


def add(customer_id, sku_id, qty=1):
    return api.post("/carts/items", json={"customer_id": customer_id, "sku_id": sku_id, "quantity": qty})


def checkout(customer_id, method="COD", vouchers=()) -> httpx.Response:
    return api.post("/checkouts", json={"customer_id": customer_id, "payment_method": method,
                                        "voucher_codes": list(vouchers)})


def stock(sku_id) -> dict:
    return next(s for s in ok(api.get("/skus", params={"limit": 20000})) if s["id"] == sku_id)


def order(order_id) -> dict:
    return ok(api.get(f"/orders/{order_id}"))


def shop_row(shop_id) -> dict:
    return next(s for s in ok(api.get("/shops", params={"limit": 10000})) if s["id"] == shop_id)


def pay(ref, status="SUCCEEDED", key=None):
    headers = {"Idempotency-Key": key} if key else {}
    return api.post("/payments/webhook", json={"transaction_ref": ref, "status": status}, headers=headers)


_clock_base = None


def carrier_time(step: int) -> str:
    """Carrier event times: business 'now' at first use, plus `step` minutes (strictly ordered per test)."""
    global _clock_base
    if _clock_base is None:
        _clock_base = datetime.fromisoformat(ok(api.get("/clock"))["now"]) + timedelta(days=1)
    return (_clock_base + timedelta(minutes=step)).isoformat()


_step = iter(range(1, 10**9, 10))


def track(tracking, event, at=None, event_id=None):
    return api.post("/logistics/webhook", json={
        "tracking_number": tracking, "event": event, "carrier_event_id": event_id or f"evt-{uuid.uuid4()}",
        "event_time": at or carrier_time(next(_step)), "location": "Hub"})


def pack(o, s) -> str:
    return ok(api.post(f"/orders/{o['id']}/pack", json={"shop_id": s["id"]}))["shipment"]["tracking_number"]


def deliver(o, s) -> str:
    tn = pack(o, s)
    for ev in ("PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"):
        assert ok(track(tn, ev))["result"] == "applied"
    return tn


def placed(method="COD", qty=1, price=100_000, stock_units=50):
    s = shop()
    sku = listing(s["id"], price=price, stock=stock_units)[0]
    b = buyer()
    ok(add(b["id"], sku, qty), 201)
    res = ok(checkout(b["id"], method), 201)
    return s, sku, b, res


# ---------------------------------------------------------------------------- P1 checkout & payments

def test_checkout_splits_orders_by_shop_with_rate_card_shipping():
    hcm, hn = shop("TP.HCM"), shop("Hà Nội")
    a, b = listing(hcm["id"], price=120_000, weight_g=1_200)[0], listing(hn["id"], price=80_000, weight_g=300)[0]
    c = buyer("Hà Nội")
    ok(add(c["id"], a, 1), 201)
    ok(add(c["id"], b, 2), 201)
    res = ok(checkout(c["id"]), 201)
    orders = {o["shop_id"]: o for o in res["orders"]}
    assert orders[hcm["id"]]["shipping_fee"] == 32_000 + 2 * 5_000
    assert orders[hn["id"]]["shipping_fee"] == 15_000 + 1 * 2_500
    assert res["checkout"]["grand_total"] == sum(o["total"] for o in res["orders"])


def test_cod_checkout_reserves_stock_and_awaits_seller():
    s, sku, b, res = placed("COD", qty=3)
    assert res["orders"][0]["status"] == "TO_SHIP"
    st = stock(sku)
    assert (st["on_hand"], st["reserved"], st["available"]) == (50, 3, 47)


def test_empty_cart_cannot_check_out():
    assert code(checkout(buyer()["id"])) == "cart_empty"


def test_online_payment_dedupes_callbacks():
    s, sku, b, res = placed("WALLET")
    ref, key = res["payment"]["transaction_ref"], str(uuid.uuid4())
    assert ok(pay(ref, key=key))["result"] == "paid"
    assert pay(ref, key=key).headers.get("Idempotent-Replay") == "true"
    assert ok(pay(ref))["result"] == "duplicate_ignored"
    assert order(res["orders"][0]["id"])["status"] == "TO_SHIP"


def test_failed_payment_then_retry_succeeds():
    s, sku, b, res = placed("CARD")
    co_id = res["checkout"]["id"]
    assert ok(pay(res["payment"]["transaction_ref"], "FAILED"))["result"] == "failed"
    retry = ok(api.post(f"/checkouts/{co_id}/payments"), 201)
    assert ok(pay(retry["transaction_ref"]))["result"] == "paid"


def test_late_payment_after_buyer_cancel_is_refunded():
    s, sku, b, res = placed("BANK_TRANSFER", qty=2)
    ok(api.post(f"/orders/{res['orders'][0]['id']}/cancel", json={"customer_id": b["id"]}))
    assert stock(sku)["available"] == 50
    assert ok(pay(res["payment"]["transaction_ref"]))["result"] == "refunded_late_payment"


def test_concurrent_checkouts_never_oversell():
    s = shop()
    sku = listing(s["id"], stock=5)[0]
    buyers = [buyer()["id"] for _ in range(12)]
    for c in buyers:
        ok(add(c, sku, 1), 201)
    with ThreadPoolExecutor(max_workers=12) as pool:
        codes = list(pool.map(lambda c: checkout(c).status_code, buyers))
    assert sorted(set(codes)) == [201, 409] and codes.count(201) == 5
    assert stock(sku)["available"] == 0


@pytest.mark.parametrize("who", ["wrong_status", "wrong_shop", "wrong_buyer"])
def test_rules_on_who_can_move_an_order(who):
    s, sku, b, res = placed("WALLET")
    o = res["orders"][0]
    if who == "wrong_status":
        assert code(api.post(f"/orders/{o['id']}/pack", json={"shop_id": s["id"]})) == "invalid_transition"
    elif who == "wrong_shop":
        ok(pay(res["payment"]["transaction_ref"]))
        assert code(api.post(f"/orders/{o['id']}/pack", json={"shop_id": shop()["id"]})) == "not_your_order"
    else:
        assert code(api.post(f"/orders/{o['id']}/cancel", json={"customer_id": buyer()["id"]})) == "not_your_order"


# ---------------------------------------------------------------------------- P2 logistics & stock ledger

def test_tracking_moves_order_and_ships_stock():
    s, sku, b, res = placed("COD", qty=2, price=300_000)
    o = res["orders"][0]
    tn = deliver(o, s)
    st = stock(sku)
    assert (st["on_hand"], st["reserved"], st["available"]) == (48, 0, 48)
    assert order(o["id"])["status"] == "DELIVERED"
    events = ok(api.get(f"/shipments/{tn}"))["events"]
    assert [e["event_type"] for e in events] == ["PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"]
    ok(api.post(f"/orders/{o['id']}/confirm-received", json={"customer_id": b["id"]}))
    assert ok(api.get(f"/customers/{b['id']}"))["total_spent"] == o["total"]


def test_late_out_of_order_and_duplicate_carrier_events():
    s, sku, b, res = placed("COD")
    o = res["orders"][0]
    tn = pack(o, s)
    t1, t2, t3 = carrier_time(next(_step)), carrier_time(next(_step)), carrier_time(next(_step))
    ok(track(tn, "PICKED_UP", t1))
    assert ok(track(tn, "DELIVERED", t3))["result"] == "applied"               # out-for-delivery scan missing
    assert ok(track(tn, "OUT_FOR_DELIVERY", t2))["result"] == "stale_ignored"  # arrives late with an older time
    assert ok(track(tn, "DELIVERED", t3))["result"] == "stale_ignored"         # redelivered under a new event id
    events = ok(api.get(f"/shipments/{tn}"))["events"]
    assert [(e["event_type"], e["applied"]) for e in events] == [
        ("PICKED_UP", True), ("OUT_FOR_DELIVERY", False), ("DELIVERED", True), ("DELIVERED", False)]
    assert order(o["id"])["status"] == "DELIVERED"


def test_carrier_event_id_dedup():
    s, sku, b, res = placed("COD")
    tn = pack(res["orders"][0], s)
    eid = "evt-" + tag()
    assert ok(track(tn, "PICKED_UP", event_id=eid))["result"] == "applied"
    assert ok(track(tn, "PICKED_UP", event_id=eid))["result"] == "duplicate_ignored"
    assert stock(sku)["on_hand"] == 49


def test_three_failed_deliveries_return_parcel_to_seller():
    s, sku, b, res = placed("COD")
    o = res["orders"][0]
    tn = pack(o, s)
    ok(track(tn, "PICKED_UP"))
    for attempt in range(3):
        ok(track(tn, "OUT_FOR_DELIVERY"))
        ok(track(tn, "DELIVERY_FAILED"))
    assert code(track(tn, "OUT_FOR_DELIVERY")) == "max_attempts_reached"
    ok(track(tn, "RETURNED_TO_SENDER"))
    assert order(o["id"])["status"] == "RETURNED_TO_SELLER"
    st = stock(sku)
    assert (st["on_hand"], st["reserved"], st["available"]) == (50, 0, 50)


# ---------------------------------------------------------------------------- P3 promotions & pricing

def test_flash_sale_price_and_quota_released_on_cancel():
    s = shop()
    sku = listing(s["id"], price=200_000)[0]
    camp = ok(api.post("/admin/campaigns", json={"name": "Flash test", "kind": "FLASH_SALE", "duration_s": 3600,
                                                 "items": [{"sku_id": sku, "sale_price": 99_000, "quota": 3}]}), 201)
    b1, b2 = buyer(), buyer()
    ok(add(b1["id"], sku, 2), 201)
    o1 = ok(checkout(b1["id"]), 201)["orders"][0]
    assert order(o1["id"])["items"][0]["unit_price"] == 99_000
    ok(add(b2["id"], sku, 2), 201)                                     # 2 more would exceed quota 3
    o2 = ok(checkout(b2["id"]), 201)["orders"][0]
    assert order(o2["id"])["items"][0]["unit_price"] == 200_000
    item = ok(api.get(f"/campaigns/{camp['id']}/items"))[0]
    assert item["sold"] == 2
    ok(api.post(f"/orders/{o1['id']}/cancel", json={"customer_id": b1["id"]}))
    assert ok(api.get(f"/campaigns/{camp['id']}/items"))[0]["sold"] == 0


def test_vouchers_shop_platform_freeship_are_allocated_to_lines():
    s1, s2 = shop("Hà Nội"), shop("Hà Nội")
    a, c = listing(s1["id"], price=300_000)[0], listing(s2["id"], price=100_000)[0]
    shop_code, plat_code, ship_code = f"SV{tag()}", f"PV{tag()}", f"FS{tag()}"
    ok(api.post(f"/shops/{s1['id']}/vouchers", json={"code": shop_code, "kind": "FIXED", "value": 20_000,
                                                     "min_spend": 100_000, "usage_limit": 10, "duration_s": 86400}), 201)
    ok(api.post("/admin/vouchers", json={"code": plat_code, "kind": "PERCENT", "value": 1000, "max_discount": 50_000,
                                         "usage_limit": 10, "duration_s": 86400}), 201)
    ok(api.post("/admin/vouchers", json={"code": ship_code, "kind": "FREESHIP", "value": 30_000,
                                         "usage_limit": 10, "duration_s": 86400}), 201)
    b = buyer("Hà Nội")
    ok(add(b["id"], a, 2), 201)
    ok(add(b["id"], c, 1), 201)
    res = ok(checkout(b["id"], vouchers=[shop_code, plat_code, ship_code]), 201)
    orders = {o["shop_id"]: order(o["id"]) for o in res["orders"]}
    o1, o2 = orders[s1["id"]], orders[s2["id"]]
    assert o1["shop_discount"] == 20_000 and o2["shop_discount"] == 0
    # platform 10% of (600k − 20k + 100k) = 68k → capped at 50k, split 580:100
    assert o1["platform_discount"] + o2["platform_discount"] == 50_000
    assert o1["shipping_subsidy"] + o2["shipping_subsidy"] == min(30_000, o1["shipping_fee"] + o2["shipping_fee"])
    for o in (o1, o2):
        assert sum(i["net_amount"] for i in o["items"]) == o["subtotal"] - o["shop_discount"] - o["platform_discount"]
    co = ok(api.get(f"/checkouts/{res['checkout']['id']}"))
    assert co["discount_total"] == sum(o["shop_discount"] + o["platform_discount"] + o["shipping_subsidy"] for o in (o1, o2))
    # per-customer limit 1
    ok(add(b["id"], c, 1), 201)
    r = checkout(b["id"], vouchers=[plat_code])
    assert code(r) == "voucher_invalid" and r.json()["detail"]["why"] == "per_customer_limit"


def test_voucher_given_back_when_unpaid_checkout_is_cancelled():
    s = shop()
    sku = listing(s["id"])[0]
    v = f"PV{tag()}"
    ok(api.post("/admin/vouchers", json={"code": v, "kind": "FIXED", "value": 10_000, "usage_limit": 1,
                                         "duration_s": 86400}), 201)
    b = buyer()
    ok(add(b["id"], sku), 201)
    res = ok(checkout(b["id"], "WALLET", [v]), 201)
    ok(api.post(f"/orders/{res['orders'][0]['id']}/cancel", json={"customer_id": b["id"]}))
    b2 = buyer()
    ok(add(b2["id"], sku), 201)
    ok(checkout(b2["id"], vouchers=[v]), 201)                         # limit 1, usable again


def test_voucher_usage_limit_holds_under_concurrency():
    s = shop()
    sku = listing(s["id"], stock=100)[0]
    v = f"PV{tag()}"
    ok(api.post("/admin/vouchers", json={"code": v, "kind": "FIXED", "value": 10_000, "usage_limit": 3,
                                         "duration_s": 86400}), 201)
    buyers = [buyer()["id"] for _ in range(8)]
    for c in buyers:
        ok(add(c, sku), 201)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda c: checkout(c, vouchers=[v]), buyers))
    assert [r.status_code for r in results].count(201) == 3
    assert {r.json()["detail"]["why"] for r in results if r.status_code == 409} == {"exhausted"}


def test_price_change_applies_to_new_orders_only():
    s, sku, b, res = placed("COD", price=150_000)
    ok(api.patch(f"/skus/{sku}/price", json={"price": 120_000}))
    b2 = buyer()
    ok(add(b2["id"], sku), 201)
    new = ok(checkout(b2["id"]), 201)["orders"][0]
    assert order(new["id"])["items"][0]["unit_price"] == 120_000
    assert order(res["orders"][0]["id"])["items"][0]["unit_price"] == 150_000


# ---------------------------------------------------------------------------- P4 returns, settlement, reviews

def _delivered_cod_order(lines):
    """One shop, a COD order with the given (price, qty) lines, delivered."""
    s = shop()
    b = buyer()
    skus = [listing(s["id"], price=p)[0] for p, _ in lines]
    for sku, (_, q) in zip(skus, lines):
        ok(add(b["id"], sku, q), 201)
    o = ok(checkout(b["id"]), 201)["orders"][0]
    deliver(o, s)
    return s, b, skus, order(o["id"])


def _return(o, b, items, reason="DAMAGED"):
    return ok(api.post(f"/orders/{o['id']}/returns", json={"customer_id": b["id"], "reason": reason, "items": items}), 201)


def _ship_back(ret_id, s, o):
    ok(api.post(f"/returns/{ret_id}/approve", json={"shop_id": s["id"]}))
    tn = next(x["tracking_number"] for x in order(o["id"])["shipments"] if x["direction"] == "RETURN")
    ok(track(tn, "PICKED_UP"))
    ok(track(tn, "DELIVERED"))


def test_partial_return_refunds_paid_amount_restocks_and_settles_seller():
    s, b, (a, c), o = _delivered_cod_order([(300_000, 2), (100_000, 1)])
    line_a = next(i for i in o["items"] if i["sku_id"] == a)
    ret = _return(o, b, [{"order_item_id": line_a["id"], "quantity": 1}])
    _ship_back(ret["id"], s, o)
    done = ok(api.post(f"/returns/{ret['id']}/inspect", json={"shop_id": s["id"], "conditions": {line_a["id"]: "SELLABLE"}}))
    assert done["status"] == "REFUNDED" and done["refund_amount"] == line_a["net_amount"] // 2
    assert stock(a)["on_hand"] == 49                                    # 2 shipped, 1 back on the shelf
    after = order(o["id"])
    assert after["status"] == "COMPLETED"
    assert ok(api.get(f"/customers/{b['id']}"))["total_spent"] == o["total"] - done["refund_amount"]
    sale = 600_000 + 100_000 - 300_000                                  # no shop voucher: kept goods
    expected = sale - sale * 400 // 10_000 - sale * 200 // 10_000       # NORMAL shop: 4% commission, 2% fee
    assert shop_row(s["id"])["wallet_balance"] == expected


def test_full_return_leaves_order_refunded_and_seller_unpaid():
    s, b, (a,), o = _delivered_cod_order([(250_000, 1)])
    ret = _return(o, b, [{"order_item_id": o["items"][0]["id"], "quantity": 1}])
    _ship_back(ret["id"], s, o)
    ok(api.post(f"/returns/{ret['id']}/inspect", json={"shop_id": s["id"], "conditions": {o["items"][0]["id"]: "DAMAGED"}}))
    assert order(o["id"])["status"] == "RETURN_REFUND"
    assert stock(a)["on_hand"] == 49                                    # damaged: not restocked
    assert shop_row(s["id"])["wallet_balance"] == 0


@pytest.mark.parametrize("decision", ["REFUND", "REJECT"])
def test_rejected_return_dispute_resolved_by_platform(decision):
    s, b, _, o = _delivered_cod_order([(200_000, 1)])
    ret = _return(o, b, [{"order_item_id": o["items"][0]["id"], "quantity": 1}], "NOT_AS_DESCRIBED")
    ok(api.post(f"/returns/{ret['id']}/reject", json={"shop_id": s["id"]}))
    ok(api.post(f"/returns/{ret['id']}/dispute", json={"customer_id": b["id"]}))
    res = ok(api.post(f"/admin/returns/{ret['id']}/resolve", json={"decision": decision}))
    if decision == "REFUND":
        assert res["status"] == "APPROVED"
        assert any(x["direction"] == "RETURN" for x in order(o["id"])["shipments"])
    else:
        done = order(o["id"])
        assert res["status"] == "CLOSED" and done["status"] == "COMPLETED" and done["completed_at"]
        # a rejected return refunds nothing, so the seller is settled on the full sale
        assert shop_row(s["id"])["wallet_balance"] == 200_000 - 200_000 * 400 // 10_000 - 200_000 * 200 // 10_000


def test_return_requires_delivered_order_and_owner():
    s, sku, b, res = placed("COD")
    o = order(res["orders"][0]["id"])
    r = api.post(f"/orders/{o['id']}/returns", json={"customer_id": b["id"], "reason": "DAMAGED",
                                                     "items": [{"order_item_id": o["items"][0]["id"], "quantity": 1}]})
    assert code(r) == "invalid_transition"


def test_reviews_update_shop_rating_once_per_item():
    s, b, _, o = _delivered_cod_order([(120_000, 1)])
    ok(api.post(f"/orders/{o['id']}/confirm-received", json={"customer_id": b["id"]}))
    body = {"customer_id": b["id"], "items": [{"order_item_id": o["items"][0]["id"], "rating": 4, "comment": "Tốt"}]}
    ok(api.post(f"/orders/{o['id']}/reviews", json=body), 201)
    assert code(api.post(f"/orders/{o['id']}/reviews", json=body)) == "already_reviewed"
    row = shop_row(s["id"])
    assert (row["rating_count"], row["rating_sum"]) == (1, 4)


# ---------------------------------------------------------------------------- business clock (last: it speeds time up)

def test_business_clock_speed_reanchors_without_jumping():
    before = ok(api.get("/clock"))
    ok(api.put("/admin/clock", json={"speed": 3600}))
    time.sleep(1)
    fast = ok(api.get("/clock"))
    ok(api.put("/admin/clock", json={"speed": before["speed"]}))
    gained = (datetime.fromisoformat(fast["now"]) - datetime.fromisoformat(before["now"])).total_seconds()
    assert 3000 < gained < 4500
