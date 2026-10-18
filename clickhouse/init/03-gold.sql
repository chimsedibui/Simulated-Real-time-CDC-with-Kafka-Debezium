-- GOLD: marketplace marts as parameterized views over consistent.<table>(v): silver as of commit watermark v.
-- Read them with one watermark for the whole query, so every mart sees the same transactions:
--     SELECT v FROM consistent.watermark;                 -- e.g. 431977
--     SELECT * FROM gold.daily_sales(v = 431977);
-- Money is VND (Int64). Business dates are Vietnam time on the business clock (created_at), never ingest time.
--
-- Metric definitions (one place; every view below uses these words with these meanings):
--   placed GMV   merchandise value (order subtotal, before vouchers) of every order placed; never changes afterwards
--   GMV          placed GMV of confirmed orders: paid online, or cash on delivery (confirmed at checkout)
--   NMV          GMV of confirmed orders that were kept (not cancelled, not returned to seller), minus the
--                merchandise value of units refunded through returns
--   net revenue  what buyers paid for goods after vouchers (order_items.net_amount), minus return refunds
--   take         commission + payment fees charged to sellers (seller ledger); take rate = take / GMV of completed orders
-- GMV settles once the payment window closes; NMV keeps moving with cancellations and returns, so finished
-- business days are frozen in gold.daily_sales_closed ("as reported") next to the live gold.daily_sales.

-- One row per order with the inputs every sales metric needs
CREATE OR REPLACE VIEW gold.order_facts AS
SELECT
    o.id                                                         AS order_id,
    o.shop_id                                                    AS shop_id,
    o.customer_id                                                AS customer_id,
    o.status                                                     AS status,
    o.payment_method                                             AS payment_method,
    o.created_at                                                 AS created_at,
    o.subtotal                                                   AS subtotal,
    o.shop_discount                                              AS shop_discount,
    o.platform_discount                                          AS platform_discount,
    o.shipping_subsidy                                           AS shipping_subsidy,
    o.total                                                      AS total,
    CAST(o.paid_at IS NOT NULL OR o.payment_method = 'COD' AS Bool) AS confirmed,
    CAST(o.status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER') AS Bool) AS kept,
    r.returned_gross                                             AS returned_gross,
    r.returned_net                                               AS returned_net
FROM consistent.orders(v = {v:UInt64}) AS o
LEFT JOIN (
    SELECT oi.order_id AS order_id, sum(ri.quantity * oi.unit_price) AS returned_gross, sum(ri.refund_amount) AS returned_net
    FROM consistent.return_items(v = {v:UInt64}) AS ri
    INNER JOIN (SELECT id FROM consistent.returns(v = {v:UInt64}) WHERE status = 'REFUNDED') AS rt ON rt.id = ri.return_id
    INNER JOIN (SELECT id, order_id, unit_price FROM consistent.order_items(v = {v:UInt64})) AS oi ON oi.id = ri.order_item_id
    GROUP BY oi.order_id
) AS r ON r.order_id = o.id;

-- Sales per business day
CREATE OR REPLACE VIEW gold.daily_sales AS
SELECT
    toDate(created_at, 'Asia/Ho_Chi_Minh')                                  AS order_date,
    count()                                                                 AS orders,
    countIf(confirmed)                                                      AS confirmed_orders,
    uniqExactIf(customer_id, confirmed)                                     AS buyers,
    sum(subtotal)                                                           AS placed_gmv,
    sumIf(subtotal, confirmed)                                              AS gmv,
    sumIf(subtotal - returned_gross, confirmed AND kept)                    AS nmv,
    sumIf(total, status = 'COMPLETED')                                      AS completed_revenue,
    countIf(status = 'CANCELLED')                                           AS cancelled_orders,
    countIf(status = 'CANCELLED' AND NOT confirmed)                         AS unpaid_cancelled,
    round(sumIf(subtotal, confirmed) / nullIf(countIf(confirmed), 0))       AS avg_order_value
FROM gold.order_facts(v = {v:UInt64})
GROUP BY order_date
ORDER BY order_date DESC;

-- Finished business days as first reported: warehouse-jobs freezes a day one business hour after it ends
-- (payment windows are 15 minutes, so its GMV is final by then). Later returns move gold.daily_sales(v), never these.
CREATE TABLE IF NOT EXISTS gold.daily_sales_closed
(
    closed_at         DateTime64(3, 'UTC'),
    order_date        Date,
    orders            UInt64,
    confirmed_orders  UInt64,
    buyers            UInt64,
    placed_gmv        Int64,
    gmv               Int64,
    nmv               Int64,
    completed_revenue Int64,
    cancelled_orders  UInt64,
    unpaid_cancelled  UInt64,
    avg_order_value   Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY order_date;

-- Current order pipeline
CREATE OR REPLACE VIEW gold.order_status_summary AS
SELECT status, count() AS orders, sum(total) AS amount
FROM consistent.orders(v = {v:UInt64})
GROUP BY status
ORDER BY orders DESC;

-- Purchase funnel (carts and checkouts, then orders)
CREATE OR REPLACE VIEW gold.funnel AS
SELECT stage, n FROM (
    SELECT 1 AS step, 'Carts created' AS stage, count() AS n FROM consistent.carts(v = {v:UInt64})
    UNION ALL SELECT 2, 'Checkouts', count() FROM consistent.checkouts(v = {v:UInt64})
    UNION ALL SELECT 3, 'Checkouts paid / COD', countIf(status IN ('PAID', 'COD_CONFIRMED')) FROM consistent.checkouts(v = {v:UInt64})
    UNION ALL SELECT 4, 'Orders shipped', countIf(shipped_at IS NOT NULL) FROM consistent.orders(v = {v:UInt64})
    UNION ALL SELECT 5, 'Orders completed', countIf(status = 'COMPLETED') FROM consistent.orders(v = {v:UInt64})
)
ORDER BY step;

-- Units and revenue per product on confirmed, kept orders; returned units and their refunds taken off
CREATE OR REPLACE VIEW gold.product_sales AS
WITH returned AS (
    SELECT ri.order_item_id AS order_item_id, sum(ri.quantity) AS qty, sum(ri.refund_amount) AS amount
    FROM consistent.return_items(v = {v:UInt64}) AS ri
    INNER JOIN (SELECT id FROM consistent.returns(v = {v:UInt64}) WHERE status = 'REFUNDED') AS r ON r.id = ri.return_id
    GROUP BY ri.order_item_id
)
SELECT
    p.name                                  AS product_name,
    c.name                                  AS category,
    sh.name                                 AS shop,
    sum(oi.quantity - rt.qty)               AS units_sold,
    sum(oi.line_total)                      AS gross_revenue,
    sum(oi.net_amount - rt.amount)          AS net_revenue,
    uniqExact(oi.order_id)                  AS orders
FROM consistent.order_items(v = {v:UInt64}) AS oi
INNER JOIN (SELECT order_id, shop_id FROM gold.order_facts(v = {v:UInt64}) WHERE confirmed AND kept) AS o ON o.order_id = oi.order_id
LEFT JOIN returned AS rt ON rt.order_item_id = oi.id
INNER JOIN (SELECT id, name, category_id FROM consistent.products(v = {v:UInt64})) AS p ON p.id = oi.product_id
INNER JOIN (SELECT id, name FROM consistent.categories(v = {v:UInt64})) AS c ON c.id = p.category_id
INNER JOIN (SELECT id, name FROM consistent.shops(v = {v:UInt64})) AS sh ON sh.id = o.shop_id
GROUP BY product_name, category, shop
ORDER BY net_revenue DESC;

-- Revenue per top-level category, same basis as product_sales
CREATE OR REPLACE VIEW gold.category_sales AS
WITH returned AS (
    SELECT ri.order_item_id AS order_item_id, sum(ri.quantity) AS qty, sum(ri.refund_amount) AS amount
    FROM consistent.return_items(v = {v:UInt64}) AS ri
    INNER JOIN (SELECT id FROM consistent.returns(v = {v:UInt64}) WHERE status = 'REFUNDED') AS r ON r.id = ri.return_id
    GROUP BY ri.order_item_id
)
SELECT
    coalesce(root.name, leaf.name)          AS category,
    sum(oi.quantity - rt.qty)               AS units_sold,
    sum(oi.line_total)                      AS gross_revenue,
    sum(oi.net_amount - rt.amount)          AS net_revenue
FROM consistent.order_items(v = {v:UInt64}) AS oi
INNER JOIN (SELECT order_id FROM gold.order_facts(v = {v:UInt64}) WHERE confirmed AND kept) AS o ON o.order_id = oi.order_id
LEFT JOIN returned AS rt ON rt.order_item_id = oi.id
INNER JOIN (SELECT id, category_id FROM consistent.products(v = {v:UInt64})) AS p ON p.id = oi.product_id
INNER JOIN (SELECT id, name, parent_id FROM consistent.categories(v = {v:UInt64})) AS leaf ON leaf.id = p.category_id
LEFT JOIN (SELECT id, name FROM consistent.categories(v = {v:UInt64})) AS root ON root.id = leaf.parent_id
GROUP BY category
ORDER BY net_revenue DESC;

-- Seller scorecard. Pack time runs from the order entering TO_SHIP (payment, or checkout for COD) to READY_TO_SHIP
CREATE OR REPLACE VIEW gold.shop_performance AS
SELECT
    sh.name                                                                   AS shop,
    sh.shop_tier                                                              AS tier,
    sh.province                                                               AS province,
    count()                                                                   AS orders,
    sumIf(f.subtotal, f.confirmed)                                            AS gmv,
    sumIf(f.subtotal - f.returned_gross, f.confirmed AND f.kept)              AS nmv,
    countIf(f.status = 'COMPLETED')                                           AS completed,
    round(countIf(f.status = 'CANCELLED') / count(), 3)                       AS cancel_rate,
    countIf(o.cancel_reason = 'seller_sla_breach')                            AS sla_breaches,
    if(any(pk.packed) = 0, NULL, round(any(pk.avg_s) / 3600, 2))              AS avg_hours_to_pack,
    any(sh.penalty_points)                                                    AS penalty_points,
    if(any(rv.n) = 0, NULL, round(any(rv.total) / any(rv.n), 2))              AS rating,
    any(rv.n)                                                                 AS reviews
FROM gold.order_facts(v = {v:UInt64}) AS f
INNER JOIN (SELECT id, cancel_reason FROM consistent.orders(v = {v:UInt64})) AS o ON o.id = f.order_id
INNER JOIN (SELECT id, name, shop_tier, province, penalty_points FROM consistent.shops(v = {v:UInt64})) AS sh
    ON sh.id = f.shop_id
LEFT JOIN (SELECT shop_id, count() AS n, sum(rating) AS total FROM consistent.product_reviews(v = {v:UInt64}) GROUP BY shop_id) AS rv
    ON rv.shop_id = f.shop_id
LEFT JOIN (
    SELECT o2.shop_id AS shop_id, count() AS packed, avg(h.pack_s) AS avg_s
    FROM (
        SELECT order_id,
               dateDiff('second', minIf(created_at, to_status = 'TO_SHIP'), minIf(created_at, to_status = 'READY_TO_SHIP')) AS pack_s
        FROM consistent.order_status_history(v = {v:UInt64})
        WHERE to_status IN ('TO_SHIP', 'READY_TO_SHIP')
        GROUP BY order_id
        HAVING countIf(to_status = 'READY_TO_SHIP') > 0
    ) AS h
    INNER JOIN (SELECT id, shop_id FROM consistent.orders(v = {v:UInt64})) AS o2 ON o2.id = h.order_id
    GROUP BY o2.shop_id
) AS pk ON pk.shop_id = f.shop_id
GROUP BY shop, tier, province
ORDER BY gmv DESC;

-- Payment attempts and outcomes per method (COD: one collection per order)
CREATE OR REPLACE VIEW gold.payment_summary AS
SELECT
    p.method                                                      AS method,
    count()                                                       AS attempts,
    countIf(p.status = 'SUCCEEDED')                               AS succeeded,
    countIf(p.status = 'FAILED')                                  AS failed,
    countIf(p.status = 'EXPIRED')                                 AS expired,
    countIf(p.status = 'CANCELLED')                               AS cancelled,
    countIf(p.status = 'PENDING')                                 AS pending,
    round(countIf(p.status = 'SUCCEEDED') / nullIf(countIf(p.status != 'PENDING'), 0), 3) AS success_rate,
    sumIf(p.amount, p.status = 'SUCCEEDED')                       AS collected,
    any(r.refunded)                                               AS refunded
FROM consistent.payments(v = {v:UInt64}) AS p
LEFT JOIN (
    SELECT pay.method AS method, sum(rf.amount) AS refunded
    FROM consistent.refunds(v = {v:UInt64}) AS rf
    INNER JOIN (SELECT id, method FROM consistent.payments(v = {v:UInt64})) AS pay ON pay.id = rf.payment_id
    GROUP BY method
) AS r ON r.method = p.method
GROUP BY method
ORDER BY attempts DESC;

-- SKUs at or below their reorder point
CREATE OR REPLACE VIEW gold.inventory_risk AS
SELECT
    s.sku_code                                AS sku,
    p.name                                    AS product_name,
    s.variant_name                            AS variant,
    sh.name                                   AS shop,
    i.available                               AS available,
    i.reserved                                AS reserved,
    i.reorder_point                           AS reorder_point,
    CAST(i.available <= i.reorder_point AS Bool) AS needs_reorder
FROM consistent.inventory(v = {v:UInt64}) AS i
INNER JOIN (SELECT id, sku_code, variant_name, product_id FROM consistent.product_skus(v = {v:UInt64})) AS s ON s.id = i.sku_id
INNER JOIN (SELECT id, name FROM consistent.products(v = {v:UInt64})) AS p ON p.id = s.product_id
INNER JOIN (SELECT id, name FROM consistent.shops(v = {v:UInt64})) AS sh ON sh.id = i.shop_id
ORDER BY i.available - i.reorder_point ASC;

-- Customer base by loyalty tier
CREATE OR REPLACE VIEW gold.customer_tiers AS
SELECT
    tier,
    countIf(is_active)          AS active_customers,
    count()                     AS total_customers,
    sum(total_spent)            AS lifetime_spent,
    round(avg(total_spent))     AS avg_spent
FROM consistent.customers(v = {v:UInt64})
GROUP BY tier
ORDER BY lifetime_spent DESC;

-- Orders and placed GMV per business hour over the last 48 business hours (daily peaks, flash sales, 11.11).
-- Placed GMV, because the latest hour's orders may still be inside their payment window.
CREATE OR REPLACE VIEW gold.traffic_by_hour AS
WITH (SELECT max(created_at) FROM consistent.orders(v = {v:UInt64})) AS latest
SELECT
    toStartOfHour(created_at)                                              AS hour,
    count()                                                                AS orders,
    sum(subtotal)                                                          AS placed_gmv,
    countIf(shop_discount + platform_discount + shipping_subsidy > 0)      AS orders_with_voucher
FROM consistent.orders(v = {v:UInt64})
WHERE created_at > latest - INTERVAL 48 HOUR
GROUP BY hour
ORDER BY hour;

-- Voucher redemption and cost (released usages excluded)
CREATE OR REPLACE VIEW gold.voucher_performance AS
SELECT
    v.code, v.scope, v.kind,
    any(v.usage_limit)                    AS usage_limit,
    count()                               AS redemptions,
    sum(u.amount)                         AS discount_cost,
    round(count() / any(v.usage_limit), 3) AS redemption_rate
FROM consistent.voucher_usages(v = {v:UInt64}) AS u
INNER JOIN (SELECT id, code, scope, kind, usage_limit FROM consistent.vouchers(v = {v:UInt64})) AS v ON v.id = u.voucher_id
WHERE u.status = 'APPLIED'
GROUP BY v.code, v.scope, v.kind
ORDER BY discount_cost DESC;

-- Flash sale sell-through and revenue at flash price
CREATE OR REPLACE VIEW gold.flash_sale_performance AS
SELECT
    c.name                                         AS campaign,
    c.kind                                         AS kind,
    c.starts_at                                    AS starts_at,
    count()                                        AS skus,
    sum(f.quota)                                   AS quota,
    sum(f.sold)                                    AS sold,
    round(sum(f.sold) / nullIf(sum(f.quota), 0), 3) AS sell_through,
    sum(f.sold * f.sale_price)                     AS revenue_at_flash_price
FROM consistent.flash_sale_items(v = {v:UInt64}) AS f
INNER JOIN (SELECT id, name, kind, starts_at FROM consistent.campaigns(v = {v:UInt64})) AS c ON c.id = f.campaign_id
GROUP BY campaign, kind, starts_at
ORDER BY starts_at DESC;

-- Carrier scorecard (forward parcels): delivery success, failed attempts, lead time in business hours
CREATE OR REPLACE VIEW gold.delivery_performance AS
SELECT
    carrier,
    count()                                                                         AS parcels,
    countIf(status = 'DELIVERED')                                                   AS delivered,
    countIf(status = 'RETURNED_TO_SENDER')                                          AS returned_to_sender,
    sum(attempts)                                                                   AS failed_attempts,
    round(countIf(status = 'DELIVERED') / nullIf(countIf(status IN ('DELIVERED', 'RETURNED_TO_SENDER')), 0), 3) AS success_rate,
    round(avgIf(dateDiff('second', picked_up_at, delivered_at), delivered_at IS NOT NULL) / 3600, 1) AS avg_hours_in_transit
FROM consistent.shipments(v = {v:UInt64})
WHERE direction = 'FORWARD'
GROUP BY carrier
ORDER BY parcels DESC;

-- Return rate per delivery-date cohort. A cohort is final ("matured") once its return window is over:
-- a delivered order can be returned until it auto-completes, 3 business days after delivery.
CREATE OR REPLACE VIEW gold.return_rate_by_cohort AS
WITH (SELECT max(created_at) FROM consistent.orders(v = {v:UInt64})) AS latest
SELECT
    toDate(o.delivered_at, 'Asia/Ho_Chi_Minh')                                AS delivery_date,
    count()                                                                   AS delivered,
    countIf(o.id IN (SELECT order_id FROM consistent.returns(v = {v:UInt64})))                AS returned,
    round(returned / delivered, 4)                                            AS return_rate,
    max(o.delivered_at) <= latest - INTERVAL 3 DAY                            AS matured
FROM consistent.orders(v = {v:UInt64}) AS o
WHERE o.delivered_at IS NOT NULL
GROUP BY delivery_date
ORDER BY delivery_date DESC;

-- Returns by reason; return rate over matured delivery cohorts only, so open return windows do not dilute it
CREATE OR REPLACE VIEW gold.returns_summary AS
WITH
    (SELECT max(created_at) FROM consistent.orders(v = {v:UInt64})) AS latest,
    matured AS (SELECT id FROM consistent.orders(v = {v:UInt64}) WHERE delivered_at IS NOT NULL AND delivered_at <= latest - INTERVAL 3 DAY)
SELECT
    r.reason                                     AS reason,
    count()                                      AS returns,
    countIf(r.status = 'REFUNDED')               AS refunded,
    countIf(r.status = 'CLOSED')                 AS rejected,
    countIf(r.status NOT IN ('REFUNDED', 'CLOSED')) AS open,
    sum(r.refund_amount)                         AS refund_amount,
    round(countIf(r.order_id IN (SELECT id FROM matured)) / nullIf((SELECT count() FROM matured), 0), 4) AS return_rate
FROM consistent.returns(v = {v:UInt64}) AS r
GROUP BY reason
ORDER BY returns DESC;

-- Seller wallet: earnings, platform charges, payouts, balance (ledger is append-only)
CREATE OR REPLACE VIEW gold.seller_settlement AS
SELECT
    sh.name                                         AS shop,
    sh.shop_tier                                    AS tier,
    sumIf(e.amount, e.entry_type = 'SALE')          AS sales,
    -sumIf(e.amount, e.entry_type = 'COMMISSION')   AS commission,
    -sumIf(e.amount, e.entry_type = 'PAYMENT_FEE')  AS payment_fees,
    -sumIf(e.amount, e.entry_type = 'PAYOUT')       AS paid_out,
    sum(e.amount)                                   AS balance
FROM consistent.seller_ledger_entries(v = {v:UInt64}) AS e
INNER JOIN (SELECT id, name, shop_tier FROM consistent.shops(v = {v:UInt64})) AS sh ON sh.id = e.shop_id
GROUP BY shop, tier
ORDER BY sales DESC;

-- Platform P&L. Refunds are split by cause: only return refunds are a cost of sales; cancellation and
-- late-payment refunds hand back money for orders that never counted as sales.
CREATE OR REPLACE VIEW gold.platform_pnl AS
SELECT
    (SELECT sumIf(subtotal, confirmed) FROM gold.order_facts(v = {v:UInt64}))                                                AS gmv,
    (SELECT sumIf(subtotal - returned_gross, confirmed AND kept) FROM gold.order_facts(v = {v:UInt64}))                      AS nmv,
    (SELECT -sumIf(amount, entry_type IN ('COMMISSION', 'PAYMENT_FEE')) FROM consistent.seller_ledger_entries(v = {v:UInt64})) AS take,
    (SELECT sumIf(platform_discount + shipping_subsidy, confirmed AND kept) FROM gold.order_facts(v = {v:UInt64}))            AS platform_promo_cost,
    (SELECT sumIf(shop_discount, confirmed AND kept) FROM gold.order_facts(v = {v:UInt64}))                                  AS seller_promo_cost,
    (SELECT sumIf(amount, return_id IS NOT NULL) FROM consistent.refunds(v = {v:UInt64}))                                    AS return_refunds,
    (SELECT sumIf(amount, return_id IS NULL AND reason != 'late_payment') FROM consistent.refunds(v = {v:UInt64}))           AS cancellation_refunds,
    (SELECT sumIf(amount, reason = 'late_payment') FROM consistent.refunds(v = {v:UInt64}))                                  AS late_payment_refunds,
    (SELECT sum(amount) FROM consistent.seller_payouts(v = {v:UInt64}))                                                      AS paid_to_sellers,
    round(take / nullIf((SELECT sumIf(subtotal, status = 'COMPLETED') FROM gold.order_facts(v = {v:UInt64})), 0), 4)          AS take_rate;

-- Pipeline health: end-to-end lag (Postgres commit -> ClickHouse) per table, last 15 minutes
CREATE OR REPLACE VIEW gold.cdc_freshness AS
SELECT
    source_table,
    count()                                                                            AS events_15m,
    countIf(op = 'c')                                                                  AS inserts,
    countIf(op = 'u')                                                                  AS updates,
    countIf(op = 'd')                                                                  AS deletes,
    round(avg(dateDiff('millisecond', source_ts, ingested_at)) / 1000, 2)              AS avg_lag_s,
    round(quantile(0.95)(dateDiff('millisecond', source_ts, ingested_at)) / 1000, 2)   AS p95_lag_s,
    max(ingested_at)                                                                   AS last_ingested_at
FROM bronze.cdc_events
WHERE ingested_at >= now64(3) - INTERVAL 15 MINUTE
  AND op != 'r'
GROUP BY source_table
ORDER BY source_table;
