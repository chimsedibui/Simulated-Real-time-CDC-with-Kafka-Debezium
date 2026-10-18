import { AlertTriangle, Percent, Receipt, ShoppingBag, Store, TrendingUp, Truck, Undo2, Wallet } from "lucide-react";
import {
  Bar, BarChart, CartesianGrid, Cell, ComposedChart, Line, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import { api } from "../api";
import { usePoll } from "../hooks";
import { CHART_PALETTE, Card, Empty, Stat, fmt, tooltipStyle } from "../ui";

const STATUS_COLORS: Record<string, string> = {
  UNPAID: "#94a3b8",
  TO_SHIP: "#fbbf24",
  READY_TO_SHIP: "#fb923c",
  SHIPPED: "#a78bfa",
  DELIVERY_FAILED: "#f472b6",
  DELIVERED: "#22d3ee",
  COMPLETED: "#34d399",
  CANCELLED: "#fb7185",
  RETURNED_TO_SELLER: "#e11d48",
  RETURN_REFUND: "#c084fc",
};

const TIER_COLORS: Record<string, string> = {
  PLATINUM: "#e2e8f0",
  GOLD: "#fbbf24",
  SILVER: "#94a3b8",
  BRONZE: "#d97706",
  MALL: "#f87171",
  PREFERRED: "#fb923c",
  NORMAL: "#64748b",
};

const pct = (v: unknown) => `${(Number(v ?? 0) * 100).toFixed(1)}%`;

export function WarehousePanel() {
  const { data, error } = usePoll(api.warehouse, 3000);

  if (!data) {
    return (
      <Card>
        <Empty>{error ? `ClickHouse not reachable: ${error}` : "Loading warehouse…"}</Empty>
      </Card>
    );
  }

  const { kpis } = data;
  const hourData = data.traffic_by_hour.map((h) => ({
    ...h,
    label: new Date(h.hour).toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh", day: "2-digit", month: "2-digit", hour: "2-digit" }),
  }));
  const pnl = data.pnl;
  const statusTotal = data.order_status.reduce((s, r) => s + Number(r.orders), 0);
  const tierMax = Math.max(1, ...data.customer_tiers.map((t) => Number(t.total_customers)));
  const funnelMax = Math.max(1, ...data.funnel.map((f) => Number(f.n)));

  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <Stat label="GMV" value={fmt.compactMoney(kpis.gmv)} hint={`NMV ${fmt.compactMoney(kpis.nmv)} after cancellations and returns`}
          icon={<TrendingUp size={14} />} accent="text-emerald-300" />
        <Stat label="Orders" value={fmt.num(kpis.orders)} hint={`${fmt.num(kpis.cancelled)} cancelled`} icon={<Receipt size={14} />} />
        <Stat label="Avg order value" value={fmt.compactMoney(kpis.avg_order_value)} hint="confirmed orders (paid or COD)"
          icon={<ShoppingBag size={14} />} accent="text-sky-300" />
        <Stat label="Buyers" value={fmt.num(kpis.buyers)} hint={`${fmt.num(kpis.active_customers)} active customers · ${fmt.num(kpis.shops)} shops`}
          icon={<Store size={14} />} accent="text-pink-300" />
      </div>

      <div className="grid gap-4 xl:grid-cols-3">
        <Card title="GMV per business hour" subtitle="gold.traffic_by_hour · last 48 business hours (Vietnam time) — daily peaks, flash sales, 11.11" className="xl:col-span-2">
          <div className="h-60">
            <ResponsiveContainer>
              <ComposedChart data={hourData} margin={{ left: 0, right: 0, top: 4 }}>
                <defs>
                  <linearGradient id="rev" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#34d399" stopOpacity={0.9} />
                    <stop offset="100%" stopColor="#34d399" stopOpacity={0.3} />
                  </linearGradient>
                </defs>
                <CartesianGrid stroke="rgba(148,163,184,0.08)" vertical={false} />
                <XAxis dataKey="label" tick={{ fill: "#64748b", fontSize: 10 }} interval={5} tickLine={false} axisLine={false} />
                <YAxis yAxisId="rev" tickFormatter={fmt.compactMoney} tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} width={64} />
                <YAxis yAxisId="ord" orientation="right" allowDecimals={false} tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} width={28} />
                <Tooltip {...tooltipStyle} formatter={(v: number, name: string) => (name === "GMV" ? fmt.money(v) : v)} />
                <Bar yAxisId="rev" dataKey="placed_gmv" name="Placed GMV" fill="url(#rev)" radius={[4, 4, 0, 0]} isAnimationActive={false} />
                <Line yAxisId="ord" type="monotone" dataKey="orders" name="Orders" stroke="#60a5fa" strokeWidth={2} dot={false} isAnimationActive={false} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </Card>

        <Card title="Order pipeline" subtitle="gold.order_status_summary">
          {statusTotal === 0 ? <Empty>No orders yet</Empty> : (
            <div className="flex items-center gap-4">
              <div className="relative h-40 w-40 shrink-0">
                <ResponsiveContainer>
                  <PieChart>
                    <Pie data={data.order_status} dataKey="orders" nameKey="status" innerRadius={48} outerRadius={72}
                      paddingAngle={2} stroke="none" isAnimationActive={false}>
                      {data.order_status.map((r) => <Cell key={String(r.status)} fill={STATUS_COLORS[String(r.status)] ?? "#94a3b8"} />)}
                    </Pie>
                    <Tooltip {...tooltipStyle} />
                  </PieChart>
                </ResponsiveContainer>
                <div className="pointer-events-none absolute inset-0 grid place-items-center text-center">
                  <div>
                    <div className="text-xl font-semibold text-slate-100">{fmt.num(statusTotal)}</div>
                    <div className="text-[10px] uppercase tracking-wider text-slate-500">orders</div>
                  </div>
                </div>
              </div>
              <ul className="flex-1 space-y-1.5 text-xs">
                {data.order_status.map((r) => (
                  <li key={String(r.status)} className="flex items-center justify-between gap-2">
                    <span className="flex items-center gap-2 text-slate-300">
                      <span className="h-2 w-2 shrink-0 rounded-full" style={{ background: STATUS_COLORS[String(r.status)] }} />
                      {String(r.status).toLowerCase().replace(/_/g, " ")}
                    </span>
                    <span className="font-mono text-slate-400">{fmt.num(r.orders)}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </Card>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <Card title="Purchase funnel" subtitle="gold.funnel · carts and checkouts, then orders">
          <ul className="space-y-3">
            {data.funnel.map((f, i) => {
              const prev = i > 0 ? Number(data.funnel[i - 1].n) : null;
              return (
                <li key={String(f.stage)}>
                  <div className="mb-1 flex justify-between text-xs">
                    <span className="text-slate-300">{f.stage}</span>
                    <span className="font-mono text-slate-400">
                      {fmt.num(f.n)}
                      {prev ? <span className="ml-2 text-slate-600">{pct(Number(f.n) / prev)}</span> : null}
                    </span>
                  </div>
                  <div className="h-2.5 rounded-full bg-white/5">
                    <div className="h-full rounded-full bg-gradient-to-r from-sky-500 to-emerald-400 transition-all duration-500"
                      style={{ width: `${(Number(f.n) / funnelMax) * 100}%` }} />
                  </div>
                </li>
              );
            })}
          </ul>
        </Card>

        <Card title="Payments" subtitle="gold.payment_summary · success rate excludes pending">
          {data.payments.length === 0 ? <Empty>No payments yet</Empty> : (
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-[11px] uppercase tracking-wider text-slate-500">
                  <th className="pb-2 font-medium">Method</th>
                  <th className="pb-2 text-right font-medium">Attempts</th>
                  <th className="pb-2 pl-4 font-medium">Success</th>
                  <th className="pb-2 text-right font-medium">Collected</th>
                  <th className="pb-2 text-right font-medium">Refunded</th>
                </tr>
              </thead>
              <tbody>
                {data.payments.map((p) => (
                  <tr key={String(p.method)} className="border-t border-white/5">
                    <td className="py-2 font-mono text-xs text-slate-200">{p.method}</td>
                    <td className="py-2 text-right font-mono text-slate-400">{fmt.num(p.attempts)}</td>
                    <td className="py-2 pl-4">
                      <div className="flex items-center gap-2">
                        <div className="h-1.5 w-16 rounded-full bg-white/5">
                          <div className="h-full rounded-full bg-emerald-400" style={{ width: pct(p.success_rate) }} />
                        </div>
                        <span className="font-mono text-xs text-slate-300">{p.success_rate === null ? "–" : pct(p.success_rate)}</span>
                      </div>
                    </td>
                    <td className="py-2 text-right font-mono text-slate-200">{fmt.compactMoney(p.collected)}</td>
                    <td className="py-2 text-right font-mono text-amber-300/90">{p.refunded ? fmt.compactMoney(p.refunded) : "–"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <Card title="Revenue by category" subtitle="gold.category_sales · top-level categories">
          {data.category_sales.length === 0 ? <Empty>No sales yet</Empty> : (
            <div className="h-64">
              <ResponsiveContainer>
                <BarChart data={data.category_sales} layout="vertical" margin={{ left: 10, right: 16 }}>
                  <CartesianGrid stroke="rgba(148,163,184,0.08)" horizontal={false} />
                  <XAxis type="number" tickFormatter={fmt.compactMoney} tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} />
                  <YAxis type="category" dataKey="category" tick={{ fill: "#cbd5e1", fontSize: 11 }} tickLine={false} axisLine={false} width={150} />
                  <Tooltip {...tooltipStyle} formatter={(v: number) => fmt.money(v)} />
                  <Bar dataKey="net_revenue" name="Net revenue" radius={[0, 6, 6, 0]} isAnimationActive={false}>
                    {data.category_sales.map((_, i) => <Cell key={i} fill={CHART_PALETTE[i % CHART_PALETTE.length]} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </Card>

        <Card title="Top products" subtitle="gold.product_sales">
          {data.top_products.length === 0 ? <Empty>No sales yet</Empty> : (
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-[11px] uppercase tracking-wider text-slate-500">
                  <th className="pb-2 font-medium">Product</th>
                  <th className="pb-2 text-right font-medium">Units</th>
                  <th className="pb-2 text-right font-medium">Net revenue</th>
                </tr>
              </thead>
              <tbody>
                {data.top_products.map((p, i) => {
                  const share = Number(p.net_revenue) / Number(data.top_products[0].net_revenue || 1);
                  return (
                    <tr key={`${p.product_name}-${p.shop}`} className="border-t border-white/5">
                      <td className="py-2 pr-2">
                        <div className="flex items-center gap-2">
                          <span className="w-4 font-mono text-xs text-slate-600">{i + 1}</span>
                          <div className="min-w-0 flex-1">
                            <div className="truncate text-slate-200">{p.product_name}</div>
                            <div className="truncate text-[11px] text-slate-500">{p.shop} · {p.category}</div>
                            <div className="mt-1 h-1 rounded-full bg-white/5">
                              <div className="h-full rounded-full bg-gradient-to-r from-sky-500 to-indigo-400" style={{ width: `${share * 100}%` }} />
                            </div>
                          </div>
                        </div>
                      </td>
                      <td className="py-2 text-right font-mono text-slate-400">{fmt.num(p.units_sold)}</td>
                      <td className="py-2 text-right font-mono text-slate-200">{fmt.compactMoney(p.net_revenue)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </Card>
      </div>

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <Stat label="Platform take" value={fmt.compactMoney(pnl.take)} hint={`take rate ${pct(pnl.take_rate)} of completed GMV`}
          icon={<Percent size={14} />} accent="text-emerald-300" />
        <Stat label="Promo cost (platform)" value={fmt.compactMoney(pnl.platform_promo_cost)}
          hint={`sellers funded ${fmt.compactMoney(pnl.seller_promo_cost)}`} icon={<Receipt size={14} />} accent="text-orange-300" />
        <Stat label="Return refunds" value={fmt.compactMoney(pnl.return_refunds)}
          hint={`+ ${fmt.compactMoney(pnl.cancellation_refunds)} cancellations · ${fmt.compactMoney(pnl.late_payment_refunds)} late payments`}
          icon={<Undo2 size={14} />} accent="text-amber-300" />
        <Stat label="Paid out to sellers" value={fmt.compactMoney(pnl.paid_to_sellers)} hint="daily payouts from seller wallets"
          icon={<Wallet size={14} />} accent="text-sky-300" />
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <Card title="Carriers" subtitle="gold.delivery_performance · forward parcels" action={<Truck size={16} className="text-slate-500" />}>
          <MiniTable rows={data.delivery} cols={[
            ["carrier", "Carrier"], ["parcels", "Parcels", "num"], ["success_rate", "Success", "pct"],
            ["failed_attempts", "Failed tries", "num"], ["returned_to_sender", "RTS", "num"], ["avg_hours_in_transit", "Avg h", "raw"]]} />
        </Card>
        <Card title="Returns by reason" subtitle="gold.returns_summary · return rate vs delivered orders" action={<Undo2 size={16} className="text-slate-500" />}>
          <MiniTable rows={data.returns} cols={[
            ["reason", "Reason"], ["returns", "Returns", "num"], ["refunded", "Refunded", "num"], ["rejected", "Rejected", "num"],
            ["refund_amount", "Refund", "money"], ["return_rate", "Rate", "pct"]]} />
        </Card>
        <Card title="Vouchers" subtitle="gold.voucher_performance · applied redemptions">
          <MiniTable rows={data.vouchers} cols={[
            ["code", "Code"], ["kind", "Kind"], ["redemptions", "Used", "num"], ["redemption_rate", "Of limit", "pct"],
            ["discount_cost", "Cost", "money"]]} />
        </Card>
        <Card title="Flash sales" subtitle="gold.flash_sale_performance · sell-through of quota">
          <MiniTable rows={data.flash_sales} cols={[
            ["campaign", "Campaign"], ["skus", "SKUs", "num"], ["sold", "Sold", "num"], ["sell_through", "Sell-through", "pct"],
            ["revenue_at_flash_price", "Revenue", "money"]]} />
        </Card>
      </div>

      <Card title="Seller wallets" subtitle="gold.seller_settlement · ledger of completed orders, platform charges and payouts">
        <MiniTable rows={data.settlement} cols={[
          ["shop", "Shop"], ["tier", "Tier"], ["sales", "Sales", "money"], ["commission", "Commission", "money"],
          ["payment_fees", "Fees", "money"], ["paid_out", "Paid out", "money"], ["balance", "Balance", "money"]]} />
      </Card>

      <Card title="Shop scorecard" subtitle="gold.shop_performance · top 10 by GMV">
        {data.shops.length === 0 ? <Empty>No orders yet</Empty> : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-[11px] uppercase tracking-wider text-slate-500">
                  <th className="pb-2 font-medium">Shop</th>
                  <th className="pb-2 text-right font-medium">Orders</th>
                  <th className="pb-2 text-right font-medium">GMV</th>
                  <th className="pb-2 text-right font-medium">Completed</th>
                  <th className="pb-2 text-right font-medium">Cancel rate</th>
                  <th className="pb-2 text-right font-medium">SLA breaches</th>
                  <th className="pb-2 text-right font-medium">Avg h to pack</th>
                  <th className="pb-2 text-right font-medium">Rating</th>
                </tr>
              </thead>
              <tbody>
                {data.shops.map((s) => (
                  <tr key={String(s.shop)} className="border-t border-white/5">
                    <td className="py-2">
                      <div className="flex items-center gap-2">
                        <span className="rounded px-1.5 py-0.5 font-mono text-[10px]"
                          style={{ color: TIER_COLORS[String(s.tier)], background: "rgba(148,163,184,0.1)" }}>{s.tier}</span>
                        <span className="truncate text-slate-200">{s.shop}</span>
                        <span className="text-[11px] text-slate-500">{s.province}</span>
                      </div>
                    </td>
                    <td className="py-2 text-right font-mono text-slate-400">{fmt.num(s.orders)}</td>
                    <td className="py-2 text-right font-mono text-slate-200">{fmt.compactMoney(s.gmv)}</td>
                    <td className="py-2 text-right font-mono text-slate-400">{fmt.num(s.completed)}</td>
                    <td className={`py-2 text-right font-mono ${Number(s.cancel_rate) > 0.15 ? "text-rose-300" : "text-slate-400"}`}>{pct(s.cancel_rate)}</td>
                    <td className={`py-2 text-right font-mono ${Number(s.sla_breaches) > 0 ? "text-amber-300" : "text-slate-500"}`}>{fmt.num(s.sla_breaches)}</td>
                    <td className="py-2 text-right font-mono text-slate-400">{s.avg_hours_to_pack === null ? "–" : Number(s.avg_hours_to_pack).toFixed(1)}</td>
                    <td className="py-2 text-right font-mono text-amber-200">{s.rating === null ? "–" : `★ ${Number(s.rating).toFixed(1)}`}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <div className="grid gap-4 xl:grid-cols-3">
        <Card title="Inventory risk" subtitle="gold.inventory_risk · closest to reorder point">
          <ul className="space-y-3">
            {data.inventory_risk.map((r) => {
              const avail = Number(r.available);
              const point = Number(r.reorder_point);
              const ratio = Math.min(1, avail / Math.max(point * 3, 1));
              const low = Boolean(r.needs_reorder);
              return (
                <li key={String(r.sku)}>
                  <div className="mb-1 flex items-center justify-between gap-2 text-xs">
                    <span className="flex min-w-0 items-center gap-1.5 truncate text-slate-300" title={`${r.product_name} · ${r.variant} · ${r.shop}`}>
                      {low && <AlertTriangle size={12} className="shrink-0 text-amber-400" />}
                      {r.product_name} <span className="text-slate-500">{r.variant}</span>
                    </span>
                    <span className="shrink-0 font-mono text-slate-400">{avail} / {point}</span>
                  </div>
                  <div className="h-1.5 rounded-full bg-white/5">
                    <div className={`h-full rounded-full ${low ? "bg-rose-400" : ratio < 0.5 ? "bg-amber-400" : "bg-emerald-400"}`}
                      style={{ width: `${Math.max(ratio * 100, 3)}%` }} />
                  </div>
                </li>
              );
            })}
          </ul>
        </Card>

        <Card title="Customer tiers" subtitle="gold.customer_tiers · lifetime spend on completed orders">
          <ul className="space-y-4">
            {data.customer_tiers.map((t) => (
              <li key={String(t.tier)}>
                <div className="mb-1 flex justify-between text-xs">
                  <span className="font-medium" style={{ color: TIER_COLORS[String(t.tier)] }}>{t.tier}</span>
                  <span className="font-mono text-slate-400">{fmt.num(t.total_customers)} · {fmt.compactMoney(t.lifetime_spent)}</span>
                </div>
                <div className="h-2 rounded-full bg-white/5">
                  <div className="h-full rounded-full opacity-80"
                    style={{ width: `${Math.max((Number(t.total_customers) / tierMax) * 100, 1)}%`, background: TIER_COLORS[String(t.tier)] }} />
                </div>
              </li>
            ))}
          </ul>
        </Card>

        <Card title="Warehouse layers" subtitle="Rows per table (silver read with FINAL)">
          <table className="w-full text-xs">
            <tbody>
              {data.layers.map((l) => {
                const [layer, name] = String(l.table).split(".");
                return (
                  <tr key={String(l.table)} className="border-t border-white/5 first:border-0">
                    <td className="py-1">
                      <span className={`mr-2 rounded px-1.5 py-0.5 font-mono text-[10px] ${layer === "bronze" ? "bg-amber-700/30 text-amber-300" : "bg-slate-500/20 text-slate-300"}`}>{layer}</span>
                      <span className="font-mono text-slate-300">{name}</span>
                    </td>
                    <td className="py-1 text-right font-mono text-slate-200">{fmt.num(l.rows)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div className="mt-4 border-t border-white/5 pt-3">
            <div className="mb-2 text-[11px] uppercase tracking-wider text-slate-500">End-to-end lag · last 15 min</div>
            {data.freshness.length === 0 ? <p className="text-xs text-slate-500">No recent changes</p> : (
              <table className="w-full text-xs">
                <tbody>
                  {data.freshness.map((f) => (
                    <tr key={String(f.source_table)}>
                      <td className="py-0.5 font-mono text-slate-300">{f.source_table}</td>
                      <td className="py-0.5 text-right font-mono text-slate-400">{fmt.num(f.events_15m)} ev</td>
                      <td className="py-0.5 text-right font-mono text-emerald-300">p95 {f.p95_lag_s}s</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </Card>
      </div>
    </div>
  );
}

type Fmt = "num" | "pct" | "money" | "raw";

function MiniTable({ rows, cols }: { rows: Record<string, unknown>[]; cols: ([string, string] | [string, string, Fmt])[] }) {
  if (rows.length === 0) return <Empty>No data yet</Empty>;
  const show = (v: unknown, f?: Fmt) =>
    v === null || v === undefined ? "–" : f === "num" ? fmt.num(v) : f === "pct" ? pct(v) : f === "money" ? fmt.compactMoney(v) : String(v);
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-[11px] uppercase tracking-wider text-slate-500">
            {cols.map(([k, label, f]) => <th key={k} className={`pb-2 font-medium ${f ? "text-right" : ""}`}>{label}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-t border-white/5">
              {cols.map(([k, , f]) => (
                <td key={k} className={`py-1.5 ${f ? "text-right font-mono text-slate-300" : "truncate text-slate-200"}`}>{show(r[k], f)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
