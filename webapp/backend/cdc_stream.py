"""Tails the cdc.ecommerce.* topics and keeps a rolling window for the live view."""

import json
import logging
import threading
import time
import uuid
from collections import defaultdict, deque

from confluent_kafka import Consumer

log = logging.getLogger("cdc_stream")

# Columns that change on every update and add noise to the diff view
NOISY_FIELDS = {"updated_at"}


class CdcStream:
    def __init__(self, broker: str, topics: list, window_seconds: int = 120):
        self.broker = broker
        self.topics = topics
        self.window_seconds = window_seconds
        self.lock = threading.Lock()
        self.events = deque(maxlen=400)
        self.seq = 0
        self.totals = defaultdict(lambda: defaultdict(int))   # table -> op -> count
        self.per_second = defaultdict(lambda: defaultdict(int))  # epoch second -> op -> count
        self.connected = False
        self.last_error = None
        self.started_at = time.time()

    def start(self):
        threading.Thread(target=self._run, name="cdc-stream", daemon=True).start()

    def _run(self):
        while True:
            try:
                consumer = Consumer({
                    "bootstrap.servers": self.broker,
                    "group.id": f"cdc-webapp-{uuid.uuid4().hex[:8]}",
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": False,
                    "topic.metadata.refresh.interval.ms": 5000,
                })
                consumer.subscribe(self.topics)
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        self.connected = True
                        continue
                    if msg.error():
                        self.last_error = str(msg.error())
                        continue
                    self.connected = True
                    if msg.value() is not None:   # skip tombstones
                        self._ingest(msg)
            except Exception as exc:
                self.connected = False
                self.last_error = str(exc)
                log.warning(f"Kafka consumer failed, retrying: {exc}")
                time.sleep(5)

    def _ingest(self, msg):
        try:
            value = json.loads(msg.value())
        except ValueError:
            return
        op = value.get("op")
        if not op:
            return
        before = value.get("before") or {}
        after = value.get("after") or {}
        row = after or before
        table = msg.topic().rsplit(".", 1)[-1]

        changes = []
        if op == "u":
            changes = [
                {"field": k, "before": before.get(k), "after": after.get(k)}
                for k in after
                if k not in NOISY_FIELDS and before.get(k) != after.get(k)
            ]

        now = time.time()
        source_ts = (value.get("source") or {}).get("ts_ms") or value.get("ts_ms")
        with self.lock:
            self.seq += 1
            self.events.append({
                "seq":         self.seq,
                "table":       table,
                "op":          op,
                "key":         row.get("id") or row.get("sku_id"),
                "label":       _label(table, row),
                "source_ts":   source_ts,
                "received_at": int(now * 1000),
                "changes":     changes,
                "row":         row,
            })
            self.totals[table][op] += 1
            self.per_second[int(now)][op] += 1
            cutoff = int(now) - self.window_seconds
            for sec in [s for s in self.per_second if s < cutoff]:
                del self.per_second[sec]

    def snapshot(self, since: int) -> dict:
        now = int(time.time())
        with self.lock:
            events = [e for e in self.events if e["seq"] > since]
            timeline = [
                {"t": sec, **{op: self.per_second.get(sec, {}).get(op, 0) for op in ("c", "u", "d", "r")}}
                for sec in range(now - 59, now + 1)
            ]
            totals = {t: dict(ops) for t, ops in self.totals.items()}
            seq = self.seq
        last_10s = sum(p["c"] + p["u"] + p["d"] + p["r"] for p in timeline[-10:])
        return {
            "seq":         seq,
            "events":      events[-100:],
            "timeline":    timeline,
            "totals":      totals,
            "rate":        round(last_10s / 10, 2),
            "connected":   self.connected,
            "last_error":  self.last_error,
            "started_at":  self.started_at,
        }


def _vnd(v) -> str:
    try:
        return f"{int(v):,}₫".replace(",", ".")
    except (TypeError, ValueError):
        return "?"


LABELS = {
    "customers":          lambda r: f"{r.get('full_name', '')} · {r.get('segment', '')} · {r.get('tier', '')}",
    "customer_addresses": lambda r: f"{r.get('district', '')}, {r.get('province', '')}",
    "shops":              lambda r: f"{r.get('name', '')} · {r.get('shop_tier', '')} · {r.get('province', '')}",
    "products":           lambda r: r.get("name", ""),
    "product_skus":       lambda r: f"{r.get('variant_name', '')} · {_vnd(r.get('price'))}",
    "inventory":          lambda r: f"on hand {r.get('on_hand')} · reserved {r.get('reserved')} · available {r.get('available')}",
    "carts":              lambda r: f"cart {r.get('status', '')}",
    "cart_items":         lambda r: f"qty {r.get('quantity')}",
    "checkouts":          lambda r: f"{r.get('status', '')} · {r.get('payment_method', '')} · {_vnd(r.get('grand_total'))}",
    "orders":             lambda r: f"{r.get('order_number', '')} · {r.get('status', '')} · {_vnd(r.get('total'))}",
    "order_items":        lambda r: f"qty {r.get('quantity')} × {_vnd(r.get('unit_price'))}",
    "order_status_history": lambda r: f"{r.get('from_status') or '∅'} → {r.get('to_status')} by {r.get('actor')}",
    "payments":           lambda r: f"{r.get('method', '')} {r.get('status', '')} · {_vnd(r.get('amount'))}",
    "refunds":            lambda r: f"{r.get('reason', '')} · {_vnd(r.get('amount'))}",
    "shipments":          lambda r: f"{r.get('carrier', '')} {r.get('tracking_number', '')} · {r.get('direction', '')} · {r.get('status', '')}",
    "shipment_events":    lambda r: f"{r.get('event_type', '')} @ {r.get('location') or '?'}" + ("" if r.get("applied") else " (stale)"),
    "inventory_movements": lambda r: f"{r.get('movement_type', '')} on_hand {r.get('on_hand_delta'):+} reserved {r.get('reserved_delta'):+}"
                                     if isinstance(r.get("on_hand_delta"), int) else r.get("movement_type", ""),
    "price_history":      lambda r: f"{_vnd(r.get('old_price'))} → {_vnd(r.get('new_price'))} · {r.get('reason', '')}",
    "vouchers":           lambda r: f"{r.get('code', '')} · used {r.get('used_count')}/{r.get('usage_limit')}",
    "voucher_usages":     lambda r: f"{r.get('status', '')} · {_vnd(r.get('amount'))}",
    "campaigns":          lambda r: f"{r.get('kind', '')} · {r.get('name', '')}",
    "flash_sale_items":   lambda r: f"sold {r.get('sold')}/{r.get('quota')} at {_vnd(r.get('sale_price'))}",
    "returns":            lambda r: f"{r.get('reason', '')} · {r.get('status', '')} · {_vnd(r.get('refund_amount'))}",
    "return_items":       lambda r: f"qty {r.get('quantity')} · {r.get('condition') or 'not inspected'}",
    "seller_ledger_entries": lambda r: f"{r.get('entry_type', '')} {_vnd(r.get('amount'))} → balance {_vnd(r.get('balance_after'))}",
    "seller_payouts":     lambda r: f"payout {_vnd(r.get('amount'))}",
    "product_reviews":    lambda r: "★" * int(r.get("rating") or 0) + (f" · {r.get('comment')}" if r.get("comment") else ""),
}


def _label(table: str, row: dict) -> str:
    fn = LABELS.get(table)
    return fn(row) if fn else ""
