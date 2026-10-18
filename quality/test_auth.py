"""Authentication and authorization of the Source API and the identity service (AUTH_MODE=enforce).

    docker compose --profile tools run --rm validator pytest -q test_auth.py

Buyers and sellers act with their own tokens here; the other tests set up data as the platform admin.
"""

import uuid

import httpx
import pytest

from test_source_api import API, IDENTITY, SECRETS, ClientToken, api, listing, ok, signed, shop, tag


def register_buyer() -> tuple[dict, str, str]:
    """A real sign-up through the identity service: (customer, access token, email)."""
    email, password = f"auth-{tag().lower()}@test.vn", "correct horse battery staple"
    r = httpx.post(f"{IDENTITY}/v1/auth/register", timeout=30, json={
        "email": email, "password": password, "full_name": "Người Mua Auth", "phone": "0900000001",
        "address": {"recipient_name": "Người Mua Auth", "phone": "0900000001", "street": "1 Lê Lợi",
                    "district": "Quận 1", "province": "Hà Nội"}})
    body = ok(r, 201)
    return body["customer"], body["access_token"], email


def as_(token: str) -> httpx.Client:
    return httpx.Client(base_url=API, timeout=30, headers={"Authorization": f"Bearer {token}"})


def as_shop(shop_id: str) -> httpx.Client:
    return httpx.Client(base_url=API, timeout=30, auth=ClientToken(subject=f"shop:{shop_id}"))


@pytest.fixture(scope="module")
def world():
    s = shop()
    sku = listing(s["id"], price=120_000, stock=20)[0]
    a, a_token, a_email = register_buyer()
    b, b_token, _ = register_buyer()
    return {"shop": s, "sku": sku, "a": a, "a_token": a_token, "a_email": a_email, "b": b, "b_token": b_token}


def test_anonymous_calls_need_a_token(world):
    anon = httpx.Client(base_url=API, timeout=30)
    r = anon.post("/carts/items", json={"sku_id": world["sku"], "quantity": 1})
    assert r.status_code == 401 and r.json()["detail"]["code"] == "unauthenticated"
    assert r.headers["www-authenticate"].startswith("Bearer")
    assert anon.get("/categories").status_code == 200                      # the public catalogue stays open
    assert anon.get("/customers").status_code == 401


def test_a_forged_or_foreign_token_is_rejected(world):
    forged = world["a_token"][:-4] + ("AAAA" if not world["a_token"].endswith("AAAA") else "BBBB")
    assert as_(forged).get(f"/customers/{world['a']['id']}").status_code == 401
    assert as_("not.a.jwt").get(f"/customers/{world['a']['id']}").status_code == 401


def test_a_buyer_acts_only_as_themselves(world):
    a, b = as_(world["a_token"]), as_(world["b_token"])
    ok(a.post("/carts/items", json={"sku_id": world["sku"], "quantity": 1}), 201)    # identity from the token
    item = ok(a.get("/carts/active"))["items"][0]
    # someone else's cart line and profile look like they do not exist (no existence leak)
    r = b.patch(f"/carts/items/{item['id']}", json={"quantity": 2})
    assert r.status_code == 404 and r.json()["detail"]["code"] == "cart_item_not_found"
    assert b.get(f"/customers/{world['a']['id']}").status_code == 404
    assert b.get("/carts/active", params={"customer_id": world["a"]["id"]}).status_code == 404
    # a buyer cannot name another customer in a command, nor read platform-wide lists
    r = b.post("/carts/items", json={"customer_id": world["a"]["id"], "sku_id": world["sku"], "quantity": 1})
    assert r.status_code == 404
    assert b.get("/customers").status_code == 403
    assert b.get("/stats").status_code == 403


def test_orders_are_visible_to_their_buyer_and_shop_only(world):
    a, b = as_(world["a_token"]), as_(world["b_token"])
    ok(a.post("/carts/items", json={"sku_id": world["sku"], "quantity": 1}), 201)
    res = ok(a.post("/checkouts", json={"payment_method": "COD"}), 201)
    order_id = res["orders"][0]["id"]
    assert ok(a.get(f"/orders/{order_id}"))["id"] == order_id
    assert ok(as_shop(world["shop"]["id"]).get(f"/orders/{order_id}"))["id"] == order_id
    assert b.get(f"/orders/{order_id}").status_code == 404
    assert b.post(f"/orders/{order_id}/cancel", json={}).status_code == 404
    other = shop()
    assert as_shop(other["id"]).post(f"/orders/{order_id}/pack", json={}).status_code == 404
    mine = ok(a.get("/orders"))                                             # a listing is scoped to the caller
    assert mine and {o["customer_id"] for o in mine} == {world["a"]["id"]}


def test_a_seller_manages_only_their_own_shop(world):
    other = shop()
    seller = as_shop(other["id"])
    assert seller.patch(f"/skus/{world['sku']}/price", json={"price": 1_000, "reason": "repricing"}).status_code == 404
    assert seller.post(f"/skus/{world['sku']}/restock", json={"quantity": 5}).status_code == 404
    own = listing(other["id"])[0]
    ok(seller.patch(f"/skus/{own}/price", json={"price": 90_000, "reason": "repricing"}))
    public = as_(world["b_token"]).get("/shops", params={"limit": 10000}).json()
    assert all("wallet_balance" not in s for s in public)                  # wallets are not public


def test_webhooks_must_be_signed_by_the_right_partner(world):
    a = as_(world["a_token"])
    ok(a.post("/carts/items", json={"sku_id": world["sku"], "quantity": 1}), 201)
    res = ok(a.post("/checkouts", json={"payment_method": "WALLET"}), 201)       # WALLET is paid through MOMO
    ref = res["payment"]["transaction_ref"]
    body = {"transaction_ref": ref, "status": "SUCCEEDED"}
    anon = httpx.Client(base_url=API, timeout=30)
    assert anon.post("/payments/webhook", json=body).status_code == 401                      # unsigned
    stale = signed("MOMO", body)
    stale["headers"]["X-Signature"] = stale["headers"]["X-Signature"].replace("t=", "t=1", 1)
    assert anon.post("/payments/webhook", **stale).status_code == 401                        # wrong time
    assert anon.post("/payments/webhook", **signed("VNPAY", body)).status_code == 403        # another gateway
    assert ok(anon.post("/payments/webhook", **signed("MOMO", body)))["result"] == "paid"


def test_idempotency_keys_are_per_caller(world):
    key = f"shared-{uuid.uuid4()}"
    a, b = as_(world["a_token"]), as_(world["b_token"])
    ra = ok(a.post("/carts/items", json={"sku_id": world["sku"], "quantity": 1}, headers={"Idempotency-Key": key}), 201)
    rb = b.post("/carts/items", json={"sku_id": world["sku"], "quantity": 1}, headers={"Idempotency-Key": key})
    assert rb.status_code == 201 and not rb.headers.get("idempotent-replay")    # not a replay of A's response
    assert rb.json()["cart_id"] != ra["cart_id"]


def test_unversioned_routes_are_gone():
    assert api.get(f"{API.removesuffix('/v1')}/categories").status_code == 404


def test_refresh_rotation_and_reuse_detection(world):
    r = ok(httpx.post(f"{IDENTITY}/v1/auth/login", json={"email": world["a_email"],
                                                         "password": "correct horse battery staple"}))
    first = r["refresh_token"]
    second = ok(httpx.post(f"{IDENTITY}/v1/auth/refresh", json={"refresh_token": first}))["refresh_token"]
    assert httpx.post(f"{IDENTITY}/v1/auth/refresh", json={"refresh_token": first}).status_code == 401
    # presenting a used token ends the whole session: the newer one is revoked too
    assert httpx.post(f"{IDENTITY}/v1/auth/refresh", json={"refresh_token": second}).status_code == 401


def test_account_locks_after_repeated_wrong_passwords():
    _, _, email = register_buyer()
    for _ in range(10):
        r = httpx.post(f"{IDENTITY}/v1/auth/login", json={"email": email, "password": "wrong password!"})
        assert r.status_code == 401 and r.json()["detail"]["code"] == "invalid_credentials"
    r = httpx.post(f"{IDENTITY}/v1/auth/login", json={"email": email, "password": "correct horse battery staple"})
    assert r.status_code == 423                                             # even the right password now
    unknown = httpx.post(f"{IDENTITY}/v1/auth/login", json={"email": "nobody@test.vn", "password": "x" * 12})
    assert unknown.status_code == 401 and unknown.json()["detail"]["code"] == "invalid_credentials"


def test_service_clients_cannot_overreach():
    r = httpx.post(f"{IDENTITY}/v1/auth/token", data={
        "grant_type": "client_credentials", "client_id": "quality", "client_secret": "wrong"})
    assert r.status_code == 401
    r = httpx.post(f"{IDENTITY}/v1/auth/token", data={
        "grant_type": "client_credentials", "client_id": "quality",
        "client_secret": (SECRETS / "clients" / "quality").read_text().strip(), "scope": "customers:provision"})
    assert r.status_code == 400 and r.json()["detail"]["code"] == "invalid_scope"
