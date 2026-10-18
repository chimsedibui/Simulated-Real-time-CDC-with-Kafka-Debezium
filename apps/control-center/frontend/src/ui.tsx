import type { ReactNode } from "react";
import type { Op } from "./api";

export const OP_META: Record<Op, { label: string; color: string; badge: string }> = {
  c: { label: "INSERT", color: "#34d399", badge: "bg-emerald-400/10 text-emerald-300 ring-emerald-400/30" },
  u: { label: "UPDATE", color: "#60a5fa", badge: "bg-sky-400/10 text-sky-300 ring-sky-400/30" },
  d: { label: "DELETE", color: "#fb7185", badge: "bg-rose-400/10 text-rose-300 ring-rose-400/30" },
  r: { label: "SNAPSHOT", color: "#a78bfa", badge: "bg-violet-400/10 text-violet-300 ring-violet-400/30" },
};

const CHART_PALETTE = ["#60a5fa", "#34d399", "#fbbf24", "#f472b6", "#a78bfa", "#22d3ee", "#fb923c", "#94a3b8", "#4ade80"];

const KNOWN_TABLE_COLORS: Record<string, string> = {
  customers: "#f472b6",
  orders: "#60a5fa",
  order_items: "#34d399",
  inventory: "#a78bfa",
  checkouts: "#fbbf24",
  payments: "#22d3ee",
  cart_items: "#fb923c",
  order_status_history: "#94a3b8",
};

/** Stable colour per table; tables beyond the known ones get a palette colour from their name. */
function tableColor(table: string): string {
  if (KNOWN_TABLE_COLORS[table]) return KNOWN_TABLE_COLORS[table];
  let h = 0;
  for (const ch of table) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  return CHART_PALETTE[h % CHART_PALETTE.length];
}

const num = new Intl.NumberFormat("en-US");

export const fmt = {
  num: (v: unknown) => num.format(Number(v ?? 0)),
  ago: (ms: number, now: number) => {
    const s = Math.max(0, Math.round((now - ms) / 1000));
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.floor(s / 60)}m ago`;
    return `${Math.floor(s / 3600)}h ago`;
  },
  bytes: (b: number) => (b < 1024 ? `${b} B` : b < 1048576 ? `${(b / 1024).toFixed(1)} KB` : `${(b / 1048576).toFixed(1)} MB`),
};

export function Card({ title, subtitle, action, children, className = "" }: {
  title?: ReactNode;
  subtitle?: ReactNode;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`rounded-2xl border border-white/5 bg-slate-900/60 p-5 shadow-xl shadow-black/20 backdrop-blur ${className}`}>
      {(title || action) && (
        <header className="mb-4 flex items-start justify-between gap-3">
          <div>
            {title && <h3 className="text-sm font-semibold text-slate-100">{title}</h3>}
            {subtitle && <p className="mt-0.5 text-xs text-slate-400">{subtitle}</p>}
          </div>
          {action}
        </header>
      )}
      {children}
    </section>
  );
}

export function Stat({ label, value, hint, accent = "text-slate-50", icon }: {
  label: string;
  value: ReactNode;
  hint?: ReactNode;
  accent?: string;
  icon?: ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-white/5 bg-gradient-to-br from-slate-900/80 to-slate-900/40 p-4">
      <div className="flex items-center justify-between text-xs font-medium uppercase tracking-wider text-slate-400">
        {label}
        {icon && <span className="text-slate-500">{icon}</span>}
      </div>
      <div className={`mt-2 text-2xl font-semibold tabular-nums ${accent}`}>{value}</div>
      {hint && <div className="mt-1 text-xs text-slate-500">{hint}</div>}
    </div>
  );
}

export function OpBadge({ op }: { op: Op }) {
  const m = OP_META[op];
  return (
    <span className={`inline-flex w-[4.75rem] justify-center rounded-md px-1.5 py-0.5 font-mono text-[10px] font-medium ring-1 ring-inset ${m.badge}`}>
      {m.label}
    </span>
  );
}

export function TableChip({ table }: { table: string }) {
  return (
    <span className="inline-flex items-center gap-1.5 font-mono text-xs text-slate-300">
      <span className="h-2 w-2 rounded-full" style={{ background: tableColor(table) }} />
      {table}
    </span>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="flex h-full min-h-[8rem] items-center justify-center text-center text-sm text-slate-500">{children}</div>;
}

export const tooltipStyle = {
  contentStyle: { background: "#0f172a", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 12, fontSize: 12 },
  labelStyle: { color: "#cbd5e1" },
  itemStyle: { color: "#e2e8f0" },
  cursor: { fill: "rgba(148,163,184,0.06)" },
};
