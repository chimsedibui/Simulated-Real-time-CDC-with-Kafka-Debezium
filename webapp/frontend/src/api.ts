export type Op = "c" | "u" | "d" | "r";

export interface Health {
  postgres: { up: boolean; counts: Record<string, number>; slot_lag_bytes?: number | null; error?: string };
  debezium: { up: boolean; state: string };
  kafka: { up: boolean; rate: number };
  clickhouse: { up: boolean; bronze_rows?: number; error?: string };
  generator: { up: boolean; running: boolean; seeding?: Seeding };
  source_api: { up: boolean };
}

export interface Seeding {
  running: boolean;
  phase: string | null;
  done: number;
  target: number;
  preset: string;
}

export interface GeneratorConfig {
  interval: number;
  orders_per_tick: number;
  new_customers_per_tick: number;
}

export interface LogLine {
  seq: number;
  ts: string;
  level: string;
  message: string;
}

export interface GeneratorStatus {
  running: boolean;
  config: GeneratorConfig;
  ticks: number;
  errors: number;
  last_error: string | null;
  started_at: number | null;
  last_tick_at: number | null;
  seeding: Seeding;
  api: { requests: number; retries: number; replays: number; rejected: number; failed: number };
  metrics: Record<string, number>;
  clock: { business_now: string | null; speed: number };
  campaign: string | null;
  pending_payment_callbacks: number;
  logs: LogLine[];
}

export interface CdcEvent {
  seq: number;
  table: string;
  op: Op;
  key: string;
  label: string;
  source_ts: number;
  received_at: number;
  changes: { field: string; before: unknown; after: unknown }[];
  row: Record<string, unknown>;
}

export interface CdcSnapshot {
  seq: number;
  events: CdcEvent[];
  timeline: ({ t: number } & Record<Op, number>)[];
  totals: Record<string, Partial<Record<Op, number>>>;
  rate: number;
  connected: boolean;
  last_error: string | null;
}

export type Row = Record<string, string | number | boolean | null>;

export interface WarehouseOverview {
  kpis: {
    orders: number;
    gmv: number;
    nmv: number;
    completed_revenue: number;
    avg_order_value: number;
    buyers: number;
    cancelled: number;
    active_customers: number;
    shops: number;
  };
  traffic_by_hour: { hour: string; orders: number; placed_gmv: number; orders_with_voucher: number }[];
  order_status: Row[];
  funnel: Row[];
  payments: Row[];
  shops: Row[];
  category_sales: Row[];
  top_products: Row[];
  inventory_risk: Row[];
  customer_tiers: Row[];
  freshness: Row[];
  layers: Row[];
  pnl: Row;
  delivery: Row[];
  returns: Row[];
  vouchers: Row[];
  flash_sales: Row[];
  settlement: Row[];
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail ?? `${res.status} ${res.statusText}`);
  }
  return res.json();
}

export const api = {
  health: () => request<Health>("/api/health"),
  generator: (logSince: number) => request<GeneratorStatus>(`/api/generator?log_since=${logSince}`),
  start: (cfg: GeneratorConfig) => request("/api/generator/start", { method: "POST", body: JSON.stringify(cfg) }),
  stop: () => request("/api/generator/stop", { method: "POST" }),
  config: (cfg: Partial<GeneratorConfig>) => request("/api/generator/config", { method: "PUT", body: JSON.stringify(cfg) }),
  action: (name: string, count: number) =>
    request(`/api/actions/${name}`, { method: "POST", body: JSON.stringify({ count }) }),
  setClock: (speed: number) => request("/api/clock", { method: "PUT", body: JSON.stringify({ speed }) }),
  registerConnector: () => request("/api/connector/register", { method: "POST" }),
  cdc: (since: number) => request<CdcSnapshot>(`/api/cdc?since=${since}`),
  warehouse: () => request<WarehouseOverview>("/api/warehouse"),
};
