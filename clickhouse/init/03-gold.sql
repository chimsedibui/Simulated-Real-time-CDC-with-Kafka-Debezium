-- GOLD: marketplace marts as views over silver (FINAL), always consistent with the latest CDC state.
-- Money is VND (Int64). GMV = merchandise value of orders not cancelled; revenue = paid-out value of COMPLETED orders.

-- Orders, GMV and completed revenue per day (Vietnam time)
CREATE OR REPLACE VIEW gold.daily_sales AS
SELECT
    toDate(created_at, 'Asia/Ho_Chi_Minh')                                 AS order_date,
    count()                                                                AS orders,
    uniqExact(customer_id)                                                 AS buyers,
    sumIf(subtotal, status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER'))     AS gmv,
    sumIf(total, status = 'COMPLETED')                                     AS completed_revenue,
    countIf(status = 'CANCELLED')                                          AS cancelled_orders,
    round(avgIf(total, status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER')))  AS avg_order_value
FROM silver.orders FINAL
GROUP BY order_date
ORDER BY order_date DESC;

-- Current order pipeline
CREATE OR REPLACE VIEW gold.order_status_summary AS
SELECT status, count() AS orders, sum(total) AS amount
FROM silver.orders FINAL
GROUP BY status
ORDER BY orders DESC;

-- Purchase funnel (carts and checkouts, then orders)
CREATE OR REPLACE VIEW gold.funnel AS
SELECT stage, n FROM (
    SELECT 1 AS step, 'Carts created' AS stage, count() AS n FROM silver.carts FINAL
    UNION ALL SELECT 2, 'Checkouts', count() FROM silver.checkouts FINAL
    UNION ALL SELECT 3, 'Checkouts paid / COD', countIf(status IN ('PAID', 'COD_CONFIRMED')) FROM silver.checkouts FINAL
    UNION ALL SELECT 4, 'Orders shipped', countIf(shipped_at IS NOT NULL) FROM silver.orders FINAL
    UNION ALL SELECT 5, 'Orders completed', countIf(status = 'COMPLETED') FROM silver.orders FINAL
)
ORDER BY step;

-- Units and revenue per product, from order lines at checkout price (orders not cancelled / returned)
CREATE OR REPLACE VIEW gold.product_sales AS
SELECT
    p.name                       AS product_name,
    c.name                       AS category,
    sh.name                      AS shop,
    sum(oi.quantity)             AS units_sold,
    sum(oi.line_total)           AS revenue,
    uniqExact(oi.order_id)       AS orders
FROM silver.order_items AS oi FINAL
INNER JOIN (SELECT id, shop_id, status FROM silver.orders FINAL) AS o ON o.id = oi.order_id
INNER JOIN (SELECT id, name, category_id FROM silver.products FINAL) AS p ON p.id = oi.product_id
INNER JOIN (SELECT id, name FROM silver.categories FINAL) AS c ON c.id = p.category_id
INNER JOIN (SELECT id, name FROM silver.shops FINAL) AS sh ON sh.id = o.shop_id
WHERE o.status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER')
GROUP BY product_name, category, shop
ORDER BY revenue DESC;

-- Revenue per top-level category
CREATE OR REPLACE VIEW gold.category_sales AS
SELECT
    coalesce(root.name, leaf.name) AS category,
    sum(oi.quantity)               AS units_sold,
    sum(oi.line_total)             AS revenue
FROM silver.order_items AS oi FINAL
INNER JOIN (SELECT id, status FROM silver.orders FINAL) AS o ON o.id = oi.order_id
INNER JOIN (SELECT id, category_id FROM silver.products FINAL) AS p ON p.id = oi.product_id
INNER JOIN (SELECT id, name, parent_id FROM silver.categories FINAL) AS leaf ON leaf.id = p.category_id
LEFT JOIN (SELECT id, name FROM silver.categories FINAL) AS root ON root.id = leaf.parent_id
WHERE o.status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER')
GROUP BY category
ORDER BY revenue DESC;

-- Seller scorecard
CREATE OR REPLACE VIEW gold.shop_performance AS
SELECT
    sh.name                                                                   AS shop,
    sh.shop_tier                                                              AS tier,
    sh.province                                                               AS province,
    count()                                                                   AS orders,
    sumIf(o.subtotal, o.status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER'))    AS gmv,
    countIf(o.status = 'COMPLETED')                                           AS completed,
    round(countIf(o.status = 'CANCELLED') / count(), 3)                       AS cancel_rate,
    countIf(o.cancel_reason = 'seller_sla_breach')                            AS sla_breaches,
    round(avgIf(dateDiff('second', o.paid_at, o.ready_at), o.ready_at IS NOT NULL) / 3600, 2) AS avg_hours_to_pack,
    any(sh.penalty_points)                                                    AS penalty_points,
    round(any(sh.rating_sum) / nullIf(any(sh.rating_count), 0), 2)            AS rating,
    any(sh.rating_count)                                                      AS reviews
FROM silver.orders AS o FINAL
INNER JOIN (SELECT id, name, shop_tier, province, penalty_points, rating_sum, rating_count FROM silver.shops FINAL) AS sh
    ON sh.id = o.shop_id
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
FROM silver.payments AS p FINAL
LEFT JOIN (
    SELECT pay.method AS method, sum(rf.amount) AS refunded
    FROM silver.refunds AS rf FINAL
    INNER JOIN (SELECT id, method FROM silver.payments FINAL) AS pay ON pay.id = rf.payment_id
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
    i.available <= i.reorder_point            AS needs_reorder
FROM silver.inventory AS i FINAL
INNER JOIN (SELECT id, sku_code, variant_name, product_id FROM silver.product_skus FINAL) AS s ON s.id = i.sku_id
INNER JOIN (SELECT id, name FROM silver.products FINAL) AS p ON p.id = s.product_id
INNER JOIN (SELECT id, name FROM silver.shops FINAL) AS sh ON sh.id = i.shop_id
ORDER BY i.available - i.reorder_point ASC;

-- Customer base by loyalty tier
CREATE OR REPLACE VIEW gold.customer_tiers AS
SELECT
    tier,
    countIf(is_active)          AS active_customers,
    count()                     AS total_customers,
    sum(total_spent)            AS lifetime_spent,
    round(avg(total_spent))     AS avg_spent
FROM silver.customers FINAL
GROUP BY tier
ORDER BY lifetime_spent DESC;

-- Orders and GMV per business hour over the last 48 business hours (shows daily peaks, flash sales, 11.11)
CREATE OR REPLACE VIEW gold.traffic_by_hour AS
WITH (SELECT max(created_at) FROM silver.orders FINAL) AS latest
SELECT
    toStartOfHour(created_at)                                              AS hour,
    count()                                                                AS orders,
    sumIf(subtotal, status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER'))     AS gmv,
    countIf(shop_discount + platform_discount + shipping_subsidy > 0)      AS orders_with_voucher
FROM silver.orders FINAL
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
FROM silver.voucher_usages AS u FINAL
INNER JOIN (SELECT id, code, scope, kind, usage_limit FROM silver.vouchers FINAL) AS v ON v.id = u.voucher_id
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
FROM silver.flash_sale_items AS f FINAL
INNER JOIN (SELECT id, name, kind, starts_at FROM silver.campaigns FINAL) AS c ON c.id = f.campaign_id
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
FROM silver.shipments FINAL
WHERE direction = 'FORWARD'
GROUP BY carrier
ORDER BY parcels DESC;

-- Returns by reason; return rate against delivered orders
CREATE OR REPLACE VIEW gold.returns_summary AS
SELECT
    r.reason                                     AS reason,
    count()                                      AS returns,
    countIf(r.status = 'REFUNDED')               AS refunded,
    countIf(r.status = 'CLOSED')                 AS rejected,
    countIf(r.status NOT IN ('REFUNDED', 'CLOSED')) AS open,
    sum(r.refund_amount)                         AS refund_amount,
    round(count() / (SELECT countIf(delivered_at IS NOT NULL) FROM silver.orders FINAL), 4) AS return_rate
FROM silver.returns AS r FINAL
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
FROM silver.seller_ledger_entries AS e FINAL
INNER JOIN (SELECT id, name, shop_tier FROM silver.shops FINAL) AS sh ON sh.id = e.shop_id
GROUP BY shop, tier
ORDER BY sales DESC;

-- Platform P&L: GMV, take (commission + fees), promotion cost it funds, refunds, take rate
CREATE OR REPLACE VIEW gold.platform_pnl AS
SELECT
    (SELECT sumIf(subtotal, status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER')) FROM silver.orders FINAL)   AS gmv,
    (SELECT -sumIf(amount, entry_type IN ('COMMISSION', 'PAYMENT_FEE')) FROM silver.seller_ledger_entries FINAL) AS take,
    (SELECT sumIf(platform_discount + shipping_subsidy, status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER'))
       FROM silver.orders FINAL)                                                                             AS platform_promo_cost,
    (SELECT sumIf(shop_discount, status NOT IN ('CANCELLED', 'RETURNED_TO_SELLER')) FROM silver.orders FINAL) AS seller_promo_cost,
    (SELECT sum(amount) FROM silver.refunds FINAL)                                                           AS refunds,
    (SELECT sum(amount) FROM silver.seller_payouts FINAL)                                                    AS paid_to_sellers,
    round(take / nullIf((SELECT sumIf(subtotal, status = 'COMPLETED') FROM silver.orders FINAL), 0), 4)      AS take_rate;

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
