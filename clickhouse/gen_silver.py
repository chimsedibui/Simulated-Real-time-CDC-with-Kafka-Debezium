"""Generates clickhouse/init/02-silver.sql from a compact spec of the source tables.

    python3 clickhouse/gen_silver.py

State tables become ReplacingMergeTree(_version = WAL LSN, _is_deleted) keyed by the primary key;
event tables (append-only in PostgreSQL) take inserts only.

Each table also gets a parameterized view consistent.<table>(v): the table as of commit watermark v, i.e. with
exactly the transactions whose END record precedes v on cdc.transaction (see consistent.watermark). Silver tables
are fed by one Kafka topic each, so for a few seconds one table can show a transaction that another does not.
A reader takes v once (SELECT v FROM consistent.watermark) and passes the same v to every view it reads, so
the whole query sees one state that existed in PostgreSQL. ClickHouse has no cross-table snapshot inside a
query, which is why v is a parameter rather than computed inside each view.
Column types: U = UUID, S = String, L = LowCardinality(String), I = Int64, i = Int32, B = Bool, T = DateTime64(6);
a trailing ? makes the column Nullable.
"""

from pathlib import Path

DT = "DT"
TABLES = {
    "customers": ("state", "id", "id U, full_name S, email S, phone S, segment L, registered_channel L, tier L, "
                  "total_spent I, is_active B, created_at T, updated_at T"),
    "customer_addresses": ("state", "id", "id U, customer_id U, recipient_name S, phone S, street S, district S, "
                           "province L, is_default B, created_at T, updated_at T"),
    "sellers": ("state", "id", "id U, legal_name S, email S, created_at T, updated_at T"),
    "shops": ("state", "id", "id U, seller_id U, name S, shop_tier L, province L, commission_rate_bps i, "
              "penalty_points i, is_active B, created_at T, updated_at T"),
    "categories": ("state", "id", "id U, parent_id U?, name S, slug S, created_at T, updated_at T"),
    "brands": ("state", "id", "id U, name S, created_at T, updated_at T"),
    "products": ("state", "id", "id U, shop_id U, category_id U, brand_id U?, name S, description S, status L, "
                 "created_at T, updated_at T"),
    "product_skus": ("state", "id", "id U, product_id U, sku_code S, variant_name S, list_price I, price I, "
                     "weight_g i, is_active B, created_at T, updated_at T"),
    "inventory": ("state", "sku_id", "sku_id U, shop_id U, on_hand i, reserved i, available i, reorder_point i, "
                  "last_restocked_at T?, updated_at T"),
    "carts": ("state", "id", "id U, customer_id U, status L, created_at T, updated_at T"),
    "cart_items": ("state", "id", "id U, cart_id U, sku_id U, quantity i, created_at T, updated_at T"),
    "checkouts": ("state", "id", "id U, customer_id U, cart_id U, address_id U, payment_method L, status L, "
                  "items_total I, shipping_total I, discount_total I, grand_total I, pay_by T?, created_at T, updated_at T"),
    "orders": ("state", "id", "id U, order_number S, checkout_id U, customer_id U, shop_id U, status L, "
               "payment_method L, subtotal I, shop_discount I, platform_discount I, shipping_fee I, shipping_subsidy I, "
               "total I, ship_to_name S, ship_to_address S, ship_to_province L, delivery_attempts i, confirm_by T?, "
               "cancel_reason L?, cancelled_by L?, paid_at T?, ready_at T?, shipped_at T?, delivered_at T?, "
               "completed_at T?, cancelled_at T?, version i, created_at T, updated_at T"),
    "order_items": ("state", "id", "id U, order_id U, sku_id U, product_id U, quantity i, list_price I, unit_price I, "
                    "line_total I, shop_discount I, platform_discount I, net_amount I, campaign_item_id U?, "
                    "created_at T, updated_at T"),
    "order_status_history": ("event", "order_id, id", "id I, order_id U, from_status L?, to_status L, actor L, "
                             "reason L?, created_at T"),
    "payments": ("state", "id", "id U, checkout_id U, order_id U?, method L, provider L, transaction_ref S, amount I, "
                 "status L, failure_reason L?, paid_at T?, created_at T, updated_at T"),
    "refunds": ("state", "id", "id U, payment_id U, order_id U?, return_id U?, amount I, reason L, status L, "
                "created_at T, updated_at T"),
    # P2 logistics & stock ledger
    "inventory_movements": ("event", "sku_id, id", "id I, sku_id U, movement_type L, on_hand_delta i, reserved_delta i, "
                            "on_hand_after i, reserved_after i, ref_type L, ref_id U, created_at T"),
    "shipments": ("state", "id", "id U, order_id U, return_id U?, direction L, carrier L, tracking_number S, status L, "
                  "from_province L, to_province L, attempts i, last_event_time T?, picked_up_at T?, delivered_at T?, "
                  "created_at T, updated_at T"),
    "shipment_events": ("event", "shipment_id, id", "id I, shipment_id U, carrier_event_id S, event_type L, "
                        "event_time T, location L?, applied B, created_at T"),
    # P3 promotions & pricing
    "price_history": ("event", "sku_id, id", "id I, sku_id U, old_price I?, new_price I, reason L, created_at T"),
    "vouchers": ("state", "id", "id U, code S, scope L, shop_id U?, kind L, value I, max_discount I?, min_spend I, "
                 "usage_limit i, per_customer_limit i, starts_at T, ends_at T, created_at T, updated_at T"),
    "voucher_usages": ("state", "id", "id U, voucher_id U, customer_id U, checkout_id U, order_id U?, amount I, "
                       "quota_slot i, status L, created_at T, updated_at T"),
    "campaigns": ("state", "id", "id U, name S, kind L, starts_at T, ends_at T, created_at T, updated_at T"),
    "flash_sale_items": ("state", "id", "id U, campaign_id U, sku_id U, sale_price I, quota i, sold i, "
                         "created_at T, updated_at T"),
    # P4 returns, settlement, reviews
    "returns": ("state", "id", "id U, order_id U, customer_id U, shop_id U, reason L, status L, refund_amount I, "
                "respond_by T, resolved_at T?, created_at T, updated_at T"),
    "return_items": ("state", "id", "id U, return_id U, order_item_id U, quantity i, refund_amount I, condition L?, "
                     "created_at T, updated_at T"),
    "seller_ledger_entries": ("event", "shop_id, id", "id I, shop_id U, order_id U?, payout_id U?, entry_type L, "
                              "amount I, created_at T"),
    "shop_wallets": ("state", "shop_id", "shop_id U, balance I, updated_at T"),
    "seller_payouts": ("state", "id", "id U, shop_id U, amount I, status L, created_at T, updated_at T"),
    "product_reviews": ("state", "id", "id U, order_item_id U, order_id U, product_id U, shop_id U, customer_id U, "
                        "rating i, comment S?, created_at T, updated_at T"),
}

CH_TYPE = {"U": "UUID", "S": "String", "L": "LowCardinality(String)", "I": "Int64", "i": "Int32", "B": "Bool",
           "T": "DateTime64(6, 'UTC')"}


def column_type(code: str) -> str:
    base = CH_TYPE[code.rstrip("?")]
    if not code.endswith("?"):
        return base
    if base.startswith("LowCardinality"):
        return "LowCardinality(Nullable(String))"
    return f"Nullable({base})"


def extract(name: str, code: str) -> str:
    nullable, c = code.endswith("?"), code.rstrip("?")
    if c == "U":
        return f"toUUIDOrNull(JSONExtractString(row, '{name}'))" if nullable else f"toUUID(JSONExtractString(row, '{name}'))"
    if c in ("S", "L"):
        return f"JSONExtract(row, '{name}', 'Nullable(String)')" if nullable else f"JSONExtractString(row, '{name}')"
    if c == "I":
        return f"JSONExtract(row, '{name}', 'Nullable(Int64)')" if nullable else f"JSONExtractInt(row, '{name}')"
    if c == "i":
        return f"toInt32(JSONExtractInt(row, '{name}'))"
    if c == "B":
        return f"JSONExtractBool(row, '{name}')"
    if c == "T":
        fn = "parseDateTime64BestEffortOrNull" if nullable else "parseDateTime64BestEffort"
        return f"{fn}(JSONExtractString(row, '{name}'), 6, 'UTC')"
    raise ValueError(code)


def table_sql(table: str, kind: str, key: str, spec: str) -> str:
    cols = [c.strip().split() for c in spec.split(",")]
    width = max(len(n) for n, _ in cols) + 1
    defs = [f"    {n:<{width}} {column_type(t)}" for n, t in cols]
    sels = [f"    {extract(n, t):<72} AS {n}" for n, t in cols]
    if kind == "state":
        defs += [f"    {'_version':<{width}} UInt64", f"    {'_is_deleted':<{width}} UInt8",
                 f"    {'_synced_at':<{width}} DateTime64(3, 'UTC')"]
        is_deleted = "op = 'd'"
        sels += [f"    {'lsn':<72} AS _version", f"    {is_deleted:<72} AS _is_deleted",
                 f"    {'ingested_at':<72} AS _synced_at"]
        # the row before this version (REPLICA IDENTITY FULL; 'null' on insert): lets consistent.* show a key
        # as it was when its newest version belongs to a transaction past the watermark
        defs += [f"    {'_before':<{width}} String CODEC(ZSTD(3))"]
        sels += [f"    {'before':<72} AS _before"]
        engine = f"ReplacingMergeTree(_version, _is_deleted)\nORDER BY {key}"
        where = f"source_table = '{table}'"
    else:
        defs += [f"    {'_synced_at':<{width}} DateTime64(3, 'UTC')"]
        sels += [f"    {'ingested_at':<72} AS _synced_at"]
        # no version column: a redelivered event (at-least-once) collapses onto its key
        engine = f"ReplacingMergeTree\nORDER BY ({key})"
        where = f"source_table = '{table}' AND op IN ('c', 'r')"
    defs += [f"    {'_tx_id':<{width}} UInt64"]
    sels += [f"    {'tx_id':<72} AS _tx_id"]
    return (
        f"-- {table} ({kind})\n"
        f"CREATE TABLE IF NOT EXISTS silver.{table}\n(\n" + ",\n".join(defs) + f"\n)\nENGINE = {engine};\n\n"
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_{table} TO silver.{table} AS\n"
        f"WITH if(op = 'd', before, after) AS row\nSELECT\n" + ",\n".join(sels) +
        f"\nFROM bronze.cdc_events\nWHERE {where};\n"
    )


RECENT = "30 MINUTE"       # rows older than this are visible without checking their transaction
VISIBLE = ("SELECT tx_id FROM bronze.cdc_transactions "
           "WHERE ingested_at > now64(3) - INTERVAL 90 MINUTE AND kafka_offset < {v:UInt64}")


def consistent_sql(table: str, kind: str, key: str, spec: str) -> str:
    """consistent.<table>(v): silver as of commit watermark v.

    A row version is visible when its transaction ended before v (or it is a snapshot read, or older than RECENT).
    Event rows past v are hidden. A state row whose newest version is past v is shown as it was before: the
    `before` image of its earliest change past v, from bronze, or the version's own _before when that change
    reached silver after bronze was read. Older versions cannot come from silver: merges drop them at any time.
    """
    visible_row = f"_tx_id = 0 OR _synced_at < now64(3) - INTERVAL {RECENT} OR _tx_id IN (SELECT tx_id FROM visible)"
    if kind == "event":
        return (f"CREATE OR REPLACE VIEW consistent.{table} AS\n"
                f"WITH visible AS ({VISIBLE})\n"
                f"SELECT * FROM silver.{table} FINAL\nWHERE {visible_row};\n")
    cols = [c.strip().split() for c in spec.split(",")]
    key_type = dict(cols)[key]
    meta = ["_version", "_is_deleted", "_synced_at", "_tx_id", "_before"]
    current = [f"    if(vis, {n}, CAST({extract(n, t).replace('(row,', '(_before,')} AS {column_type(t)})) AS {n}"
               for n, t in cols] + [f"    {m}" for m in meta]
    restored = [f"    CAST({extract(n, t)} AS {column_type(t)}) AS {n}" for n, t in cols]
    restored += ["    toUInt64(0) AS _version", "    toUInt8(0) AS _is_deleted", "    now64(3) AS _synced_at",
                 "    toUInt64(0) AS _tx_id", "    'null' AS _before"]
    key_expr = extract(key, key_type).replace("(row,", "(if(op = 'd', before, after),")
    return (
        f"CREATE OR REPLACE VIEW consistent.{table} AS\n"
        f"WITH\n"
        f"    visible AS ({VISIBLE}),\n"
        f"    restore AS (\n"
        f"        SELECT {key_expr} AS k, argMin(before, lsn) AS row\n"
        f"        FROM bronze.cdc_events\n"
        f"        WHERE source_table = '{table}' AND event_ts > now64(3) - INTERVAL 35 MINUTE AND tx_id != 0\n"
        f"          AND tx_id NOT IN (SELECT tx_id FROM visible)\n"
        f"        GROUP BY k)\n"
        f"SELECT\n" + ",\n".join(current) + "\n"
        f"FROM (SELECT *, {visible_row} AS vis\n"
        f"      FROM silver.{table} FINAL WHERE {key} NOT IN (SELECT k FROM restore))\n"
        f"WHERE vis OR _before != 'null'\n"
        f"UNION ALL\nSELECT\n" + ",\n".join(restored) + "\nFROM restore\nWHERE row != 'null';\n"
    )


WATERMARK = """-- Commit watermark for consistent.<table>(v): the commit position (offset on cdc.transaction, one partition,
-- commit order) of the first transaction not fully landed yet; every transaction before it is complete.
-- Taking v is cheap and depends only on landed data, so readers can call it before every query.
CREATE OR REPLACE VIEW consistent.watermark AS
WITH
    ended AS (
        SELECT tx_id, max(expected) AS expected, min(kafka_offset) AS seq, min(commit_ts) AS commit_ts,
               max(ingested_at) AS seen
        FROM bronze.cdc_transactions
        WHERE ingested_at > now64(3) - INTERVAL 60 MINUTE
        GROUP BY tx_id),
    landed AS (
        SELECT tx_id, uniqExact(source_table, lsn) AS n
        FROM bronze.cdc_events
        WHERE event_ts > now64(3) - INTERVAL 3 HOUR AND tx_id IN (SELECT tx_id FROM ended)
        GROUP BY tx_id)
SELECT
    v,
    arrayCount(s -> s >= v, recent_seqs)                                              AS held_back,
    if(held_back = 0, NULL, arrayMin(arrayFilter((c, s) -> s >= v, recent_commits, recent_seqs))) AS oldest_held_back_commit
FROM (
    SELECT
        if(countIf(l.n < e.expected) > 0, minIf(e.seq, l.n < e.expected), max(e.seq) + 1)  AS v,
        groupArrayIf(e.seq, e.seen > now64(3) - INTERVAL 10 MINUTE)                       AS recent_seqs,
        groupArrayIf(e.commit_ts, e.seen > now64(3) - INTERVAL 10 MINUTE)                 AS recent_commits
    FROM ended AS e
    LEFT JOIN landed AS l ON l.tx_id = e.tx_id);
"""

HEADER = """-- GENERATED by clickhouse/gen_silver.py — edit the spec there, not this file.
--
-- SILVER: current state of each source table, rebuilt from bronze.cdc_events.
-- State tables: ReplacingMergeTree(_version, _is_deleted) keeps the row with the highest WAL LSN per key;
-- a delete is its final version with _is_deleted = 1. Merges converge eventually: read with FINAL.
-- Event tables (append-only at the source) take inserts only; FINAL removes redelivered duplicates.
"""

if __name__ == "__main__":
    out = Path(__file__).parent / "init" / "02-silver.sql"
    out.write_text(HEADER + "\n" + "\n\n".join(table_sql(t, *spec) for t, spec in TABLES.items()) + "\n\n"
                   + WATERMARK + "\n" + "\n".join(consistent_sql(t, *spec) for t, spec in TABLES.items()))
    print(f"wrote {out} ({len(TABLES)} tables)")
