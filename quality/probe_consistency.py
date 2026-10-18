"""Torn-read probe: run WHILE the simulator writes, to show what a reader sees mid-stream.

Every invariant below holds inside one PostgreSQL transaction but spans several Kafka topics, so silver (one topic
per table) can break it for a few seconds. consistent.<table>(v), read with one watermark v per poll, only shows
fully landed transactions and must never break it.

    docker compose --profile tools run --rm validator python probe_consistency.py --seconds 300

Exit code 0 = no violation ever seen through consistent.*.
"""

import argparse
import os
import sys
import time

import clickhouse_connect

INVARIANTS = {
    "completed order without its SALE ledger entry": """
        SELECT count() FROM {orders} AS o
        LEFT ANTI JOIN (SELECT order_id FROM {seller_ledger_entries} WHERE entry_type = 'SALE') AS l ON l.order_id = o.id
        WHERE o.status = 'COMPLETED'
          AND o.id NOT IN (SELECT order_id FROM {returns} WHERE status = 'REFUNDED')   -- fully returned: no sale""",
    "order line without its order": """
        SELECT count() FROM {order_items} AS oi LEFT ANTI JOIN (SELECT id FROM {orders}) AS o ON o.id = oi.order_id""",
    "order without its checkout": """
        SELECT count() FROM {orders} AS o LEFT ANTI JOIN (SELECT id FROM {checkouts}) AS c ON c.id = o.checkout_id""",
    "payout without its PAYOUT ledger entry": """
        SELECT count() FROM {seller_payouts} AS p
        LEFT ANTI JOIN (SELECT payout_id FROM {seller_ledger_entries} WHERE entry_type = 'PAYOUT') AS l ON l.payout_id = p.id""",
    "inventory on hand ≠ Σ stock ledger": """
        SELECT count() FROM {inventory} AS i
        LEFT JOIN (SELECT sku_id, sum(on_hand_delta) AS n FROM {inventory_movements} GROUP BY sku_id) AS m ON m.sku_id = i.sku_id
        WHERE i.on_hand != m.n""",
    "voucher usage without its checkout": """
        SELECT count() FROM {voucher_usages} AS u LEFT ANTI JOIN (SELECT id FROM {checkouts}) AS c ON c.id = u.checkout_id""",
}
TABLES = ["orders", "order_items", "checkouts", "seller_payouts", "seller_ledger_entries", "inventory",
          "inventory_movements", "voucher_usages", "returns"]
READERS = {"silver FINAL": {t: f"(SELECT * FROM silver.{t} FINAL)" for t in TABLES},
           "consistent": {t: f"consistent.{t}(v = {{v}})" for t in TABLES}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=300)
    ap.add_argument("--every", type=float, default=2.0)
    args = ap.parse_args()
    ch = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), port=8123,
                                       username="admin", password="admin")
    worst = {(r, name): 0 for r in READERS for name in INVARIANTS}
    hits = {(r, name): 0 for r in READERS for name in INVARIANTS}
    polls, open_max = 0, 0
    end = time.time() + args.seconds
    unreachable = 0
    while time.time() < end:
        try:
            # one poll pins one watermark and reads every invariant through both readers back to back
            v, open_now = ch.query("SELECT v, held_back FROM consistent.watermark").result_rows[0]
            seen = {(r, name): ch.query(sql.format(**tables).replace("{v}", str(v))).result_rows[0][0]
                    for r, tables in READERS.items() for name, sql in INVARIANTS.items()}
        except Exception as e:                  # ClickHouse restarting (chaos step of the gate)
            unreachable += 1
            if unreachable <= 3:
                print(f"poll failed: {str(e).splitlines()[0][:300]}", file=sys.stderr)
            time.sleep(args.every)
            continue
        polls += 1
        open_max = max(open_max, open_now)
        for k, n in seen.items():
            worst[k] = max(worst[k], n)
            hits[k] += n > 0
        time.sleep(args.every)

    print(f"{polls} polls over {args.seconds}s ({unreachable} skipped, ClickHouse unreachable) · "
          f"up to {open_max} transactions held back at once\n")
    width = max(len(n) for n in INVARIANTS)
    print(f"{'invariant':<{width}}  {'silver FINAL':>22}  {'consistent':>22}")
    for name in INVARIANTS:
        cells = [f"{worst[(r, name)]} max, {hits[(r, name)]}/{polls} polls" for r in READERS]
        print(f"{name:<{width}}  {cells[0]:>22}  {cells[1]:>22}")
    torn = sum(worst[("consistent", n)] for n in INVARIANTS)
    if polls == 0:
        print("\nFAIL: no poll succeeded")
        sys.exit(1)
    print(f"\n{'PASS' if torn == 0 else 'FAIL'}: consistent.* showed {torn} violations")
    sys.exit(1 if torn else 0)


if __name__ == "__main__":
    main()
