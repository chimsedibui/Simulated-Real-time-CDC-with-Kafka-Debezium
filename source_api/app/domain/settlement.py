"""Seller wallet: an append-only ledger; shops.wallet_balance is its running sum.

When an order completes the seller earns what the buyer paid for the goods it kept, minus the seller-funded
shop voucher (platform vouchers and free shipping are paid by the platform), and is charged commission and a
payment fee on that amount. Payouts empty the wallet.
"""

from .. import outbox
from ..settings import PAYMENT_FEE_BPS


def post(cur, shop_id, entry_type: str, amount: int, order_id=None, payout_id=None) -> int:
    cur.execute("UPDATE ecommerce.shops SET wallet_balance = wallet_balance + %s WHERE id = %s RETURNING wallet_balance",
                (amount, shop_id))
    balance = cur.fetchone()["wallet_balance"]
    cur.execute(
        """INSERT INTO ecommerce.seller_ledger_entries (shop_id, order_id, payout_id, entry_type, amount, balance_after)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (shop_id, order_id, payout_id, entry_type, amount, balance),
    )
    return balance


def seller_share(line_total: int, shop_discount: int, qty: int, of_qty: int) -> int:
    """Seller's revenue for `qty` of a line of `of_qty` units (same rounding everywhere)."""
    return (line_total - shop_discount) * qty // of_qty


def settle_order(cur, order: dict):
    cur.execute(
        """SELECT oi.quantity, oi.line_total, oi.shop_discount,
                  coalesce((SELECT ri.quantity FROM ecommerce.return_items ri
                            JOIN ecommerce.returns r ON r.id = ri.return_id
                            WHERE ri.order_item_id = oi.id AND r.status = 'REFUNDED'), 0) AS returned
           FROM ecommerce.order_items oi
           WHERE oi.order_id = %s""",
        (order["id"],),
    )
    sale = 0
    for ln in cur.fetchall():
        returned = ln["returned"] if ln["returned"] else 0
        sale += seller_share(ln["line_total"], ln["shop_discount"], ln["quantity"], ln["quantity"])
        if returned:
            sale -= seller_share(ln["line_total"], ln["shop_discount"], returned, ln["quantity"])
    cur.execute("SELECT commission_rate_bps FROM ecommerce.shops WHERE id = %s", (order["shop_id"],))
    commission = sale * cur.fetchone()["commission_rate_bps"] // 10_000
    fee = sale * PAYMENT_FEE_BPS // 10_000
    for entry_type, amount in (("SALE", sale), ("COMMISSION", -commission), ("PAYMENT_FEE", -fee)):
        if amount:
            post(cur, order["shop_id"], entry_type, amount, order_id=order["id"])
    outbox.emit(cur, "shop", order["shop_id"], "OrderSettled",
                {"order_id": order["id"], "shop_id": order["shop_id"], "sale": sale, "commission": commission,
                 "payment_fee": fee, "net": sale - commission - fee})


def payout(cur, shop_id) -> dict | None:
    cur.execute("SELECT wallet_balance FROM ecommerce.shops WHERE id = %s FOR UPDATE", (shop_id,))
    balance = cur.fetchone()["wallet_balance"]
    if balance <= 0:
        return None
    cur.execute("INSERT INTO ecommerce.seller_payouts (shop_id, amount) VALUES (%s, %s) RETURNING *", (shop_id, balance))
    p = cur.fetchone()
    post(cur, shop_id, "PAYOUT", -balance, payout_id=p["id"])
    outbox.emit(cur, "shop", shop_id, "PayoutPaid", {"payout_id": p["id"], "shop_id": shop_id, "amount": balance})
    return p
