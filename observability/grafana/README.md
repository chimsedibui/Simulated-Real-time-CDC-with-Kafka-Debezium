# Marketplace decision dashboards

Five role-based business dashboards replace the original long list of mart tables. They use Grafana's built-in visualizations and the existing read-only ClickHouse datasource. No schema migrations, marts, app services, or credentials change. `cdc-platform` is generated as before.

| Dashboard | Owner / decisions | Main views |
|---|---|---|
| `/d/cdc-business` · Điều hành sàn | Daily business review, where GMV is lost, which team acts next | Matched-period GMV/NMV comparison, AOV, retained-value ratio, current-day pulse, 48-hour order rhythm, action queue |
| `/d/cdc-operations` · Đơn hàng & trải nghiệm | Fulfillment / customer service | Backlog by stage, carrier outcome mix, transit time and sample size, open returns by cause, mature vs immature return cohorts |
| `/d/cdc-growth` · Danh mục & chiến dịch | Merchandising / campaigns / CRM | Category revenue mix, product ranking, voucher cost and quota, started-vs-future campaign context, loyalty-tier mix |
| `/d/cdc-sellers` · Nhà bán & tồn kho | Seller success / inventory | Tier → province → shop filters, minimum order sample, packing/cancellation intervention list, SKU stock gap queue |
| `/d/cdc-finance` · Thu phí & dòng tiền | Finance / payments | Take and its denominator, platform-funded promotion, refunds separated by cause, payment outcomes, same-grain checkout diagnostics, seller wallet reconciliation |

## Example operating routines

1. **Daily business review:** choose 7/14/30 prior business days on Điều hành. Inspect matched-period GMV and NMV, then the difference between placed and confirmed GMV versus the gap from GMV to NMV. Use the action queue to route work to warehouse, seller success or CS. The ongoing day is shown separately.
2. **Traffic surge / sale event:** inspect the 48-hour order rhythm, open Platform Health for technical capacity, then review carrier/packing backlog and SKU gaps. The business dashboard does not infer a scale action from order count alone.
3. **Seller intervention:** filter tier and province, then shop. Choose the minimum order count before comparing cancellation rates. Review real SLA violations and packing time together; investigate cancellations before attributing them to seller misconduct.
4. **Marketing review:** compare top-level category share, drill into leaf-category products, then inspect voucher funding/cost. Use flash-sale quota and start-time context to select campaigns for review; future campaigns with zero sales are not marked as underperforming.
5. **Returns review:** prioritize open requests by reason and compare delivery cohorts only after the return window has closed. New cohorts remain explicitly immature.
6. **Payment / finance review:** distinguish payment attempts from checkouts and split refund causes. Large late-payment refunds call for a gateway/timeout investigation, not a conclusion that product quality deteriorated. Seller balance is not automatically an overdue payout.

## Scope and metric safeguards

- All panel queries use the same hidden `refresh_id` resolved from a completed `ops.mart_refreshes` entry. Navigation carries compatible visible filters. Each page resolves its latest completed snapshot independently; different pages opened at different times may show different updates. No query reads live source or gold tables.
- Business time is accelerated and may be months ahead of wall time. The wall-clock picker is hidden. The selected 7/14/30-day window applies only to the overview's dated sales section. Other sections explicitly show cumulative/current-snapshot scope.
- Matched periods exclude the latest, potentially partial business day. These are **live values for past order dates**, not frozen closed-day reports. Period change is NULL if history is insufficient or the previous denominator is zero; never invent a positive growth percentage. The newest NMV cohorts can still be inside the return window, so equal-length periods do not imply equal maturity.
- Never sum daily distinct buyer counts into period uniques. The overview's buyer KPI is explicitly all-history distinct buyers.
- Product category is a leaf dimension; category-sales is a root dimension. The product-category filter intentionally affects only product panels. Voucher funding and payment-method filters also show their scope in the label.
- Multi-select filters expand to SQL-quoted values; dependent tier/province/shop filters are tested. Top N, period length and minimum sample are fixed-choice numeric variables.
- Seller filters include shops appearing in `shop_performance`, i.e. shops with orders. Inventory for shops without orders is intentionally outside that view. Existing marts group by shop/product names, so these are not guaranteed entity-ID drilldowns.
- Cancellation includes unpaid timeout / buyer cancellation; it is not seller fault rate. Packing average is over packed orders; minimum sample is total shop orders because packed count is not in the mart.
- Return cohort rate includes **any** request, including open/rejected ones. Mature-only rate is weighted using summed numerator/denominator; it is not the average of daily percentages.
- Carrier success rate uses finished forward parcels. Failed-attempt count can contain repeated attempts and is not a distinct failed-order rate.
- Inventory gap is `max(reorder_point + 1 - available, 0)`. It is the minimum to exceed the threshold, not a demand forecast, days of cover, or optimal purchase quantity.
- Voucher redemption is not ROI, ROAS or incremental revenue. Voucher costs and platform P&L have different status filters and should not be forced to reconcile.
- Flash sale mart has start time but no end time; the dashboard cannot label a campaign ended. The newest order-hour bucket is a conservative business-time reference, potentially one hour behind.
- Product/category net revenue is labeled **giá trị hàng ròng**, not cash received: confirmed/kept orders include COD orders that may not yet be collected.
- Financial metrics have different recognition bases. Charts are side-by-side amounts, not an accounting waterfall or a computed profit. `take_rate` is defined by gold using completed-order GMV.
- `is_active` customer count is a profile flag, not rolling active customers. Loyalty spend is not predicted CLV or retention.
- Table color cutoffs (e.g. seller 20% cancel / 24h packing, return 5%/10%, carrier 90%/97%) are exploratory review thresholds, not a newly invented business SLA or penalty rule. Look at sample size and status maturity.

## Generate and verify

```bash
python3 observability/grafana/generate_dashboards.py
python3 observability/grafana/verify_business_dashboards.py
```

Generation is deterministic and updates the provisioned JSON in `dashboards/`. Grafana reloads files periodically without restarting the warehouse or simulator. `business_dashboards.py` contains the business queries and panel layouts. The existing generator still owns Platform Health.

The verifier uses `secrets/grafana_admin_password` without printing it, pins one completed snapshot, runs every business panel through Grafana's actual datasource, tests All and individual dependent selections, and checks basic daily-mart ratio assumptions. It never mutates business data. If Claude changes mart schemas concurrently, rerun it to catch contract mismatches before trusting a screenshot.

Useful future marts: order-level backlog age/deadlines, paid-checkout cohorts, SKU daily demand/stock cover, voucher attribution/control groups, order/shop/date grain and customer retention cohorts. These require explicit warehouse work; the current dashboard does not fabricate them from aggregate tables.
