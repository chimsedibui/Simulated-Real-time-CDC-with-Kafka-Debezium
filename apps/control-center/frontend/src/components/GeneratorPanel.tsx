import { useEffect, useRef, useState } from "react";
import { ArrowRightCircle, Ban, Clock, Flame, Package, Pause, Play, ShoppingBasket, ShoppingCart, Sparkles, UserPlus } from "lucide-react";
import { api, type GeneratorConfig, type LogLine } from "../api";
import { usePoll } from "../hooks";
import { Card } from "../ui";

const SPEEDS = [60, 240, 720, 1440];

function businessTime(iso: string | null | undefined) {
  if (!iso) return "–";
  return new Date(iso).toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh", weekday: "short", day: "2-digit",
    month: "2-digit", hour: "2-digit", minute: "2-digit" });
}

const ACTIONS = [
  { name: "order", label: "Buyer checkout", icon: ShoppingCart, tint: "text-sky-300" },
  { name: "cart", label: "Cart only", icon: ShoppingBasket, tint: "text-violet-300" },
  { name: "advance", label: "Pay · pack · ship", icon: ArrowRightCircle, tint: "text-emerald-300" },
  { name: "customer", label: "New customer", icon: UserPlus, tint: "text-pink-300" },
  { name: "cancel", label: "Buyer cancel", icon: Ban, tint: "text-rose-300" },
  { name: "restock", label: "Restock", icon: Package, tint: "text-amber-300" },
];

function Slider({ label, value, min, max, step, unit, onChange }: {
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  unit?: string;
  onChange: (v: number) => void;
}) {
  return (
    <label className="block">
      <div className="mb-1.5 flex justify-between text-xs">
        <span className="text-slate-400">{label}</span>
        <span className="font-mono text-slate-200">{value}{unit}</span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-full accent-sky-400"
      />
    </label>
  );
}

function logTone(line: LogLine) {
  const m = line.message;
  if (line.level === "ERROR") return "text-rose-300";
  if (m.includes("[campaign]") || m.includes("[ops]") || m.includes("[clock]")) return "text-violet-300";
  if (m.includes("[return.")) return "text-amber-300/90";
  if (line.level === "WARNING" || m.includes("[payment.failed]") || m.includes("refunded")) return "text-amber-300";
  if (m.includes("[order.cancelled]") || m.includes("returned_to_seller") || m.includes("delivery_failed")) return "text-rose-300/90";
  if (m.includes("[checkout]") || m.includes("[payment.paid]") || m.includes("[order.completed]") || m.includes(".registered]"))
    return "text-emerald-300/90";
  if (m.includes("[order.packed]") || m.includes("[carrier.") || m.includes("[inventory.")) return "text-sky-300/90";
  return "text-slate-400";
}

export function GeneratorPanel() {
  const logSince = useRef(0);
  const [logs, setLogs] = useState<LogLine[]>([]);
  const [cfg, setCfg] = useState<GeneratorConfig | null>(null);
  const [batch, setBatch] = useState(1);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const { data: status } = usePoll(async () => {
    const s = await api.generator(logSince.current);
    if (s.logs.length) {
      logSince.current = s.logs[s.logs.length - 1].seq;
      setLogs((prev) => [...s.logs.slice().reverse(), ...prev].slice(0, 120));
    }
    return s;
  }, 1500);

  // Initialise sliders from the server once
  useEffect(() => {
    if (status && !cfg) setCfg(status.config);
  }, [status, cfg]);

  const pushConfig = (next: GeneratorConfig) => {
    setCfg(next);
    api.config(next).catch(() => {});
  };

  const run = async (key: string, fn: () => Promise<unknown>) => {
    setBusy(key);
    setError(null);
    try {
      await fn();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  };

  const running = status?.running ?? false;

  return (
    <div className="flex flex-col gap-4">
      <Card
        title="Traffic simulator"
        subtitle="Simulated users calling the Source API — no direct database writes"
        action={
          <span className={`rounded-full px-2 py-0.5 text-[11px] font-medium ring-1 ring-inset ${running ? "bg-emerald-400/10 text-emerald-300 ring-emerald-400/30" : "bg-slate-500/10 text-slate-400 ring-slate-500/30"}`}>
            {running ? "RUNNING" : "STOPPED"}
          </span>
        }
      >
        <button
          disabled={!cfg || busy === "toggle"}
          onClick={() => run("toggle", () => (running ? api.stop() : api.start(cfg!)))}
          className={`flex w-full items-center justify-center gap-2 rounded-xl px-4 py-3 text-sm font-semibold transition ${
            running
              ? "bg-rose-500/15 text-rose-200 ring-1 ring-rose-400/30 hover:bg-rose-500/25"
              : "bg-gradient-to-r from-sky-500 to-indigo-500 text-white shadow-lg shadow-sky-500/20 hover:brightness-110"
          }`}
        >
          {running ? <Pause size={16} /> : <Play size={16} />}
          {running ? "Stop simulation" : "Start simulation"}
        </button>

        {cfg && (
          <div className="mt-5 space-y-4">
            <Slider label="Buyer sessions / business hour (at peak ×1)" value={cfg.orders_per_tick} min={0} max={1000} step={10}
              onChange={(v) => pushConfig({ ...cfg, orders_per_tick: v })} />
            <Slider label="New customers / business hour" value={cfg.new_customers_per_tick} min={0} max={200} step={2}
              onChange={(v) => pushConfig({ ...cfg, new_customers_per_tick: v })} />
            <Slider label="Tick interval (wall clock)" value={cfg.interval} min={0.5} max={5} step={0.5} unit="s"
              onChange={(v) => pushConfig({ ...cfg, interval: v })} />
          </div>
        )}

        <div className="mt-5 rounded-xl border border-white/5 bg-white/[0.03] p-3">
          <div className="flex items-center justify-between text-xs">
            <span className="flex items-center gap-1.5 text-slate-400"><Clock size={13} /> Business time</span>
            {status?.campaign && (
              <span className="flex items-center gap-1 rounded-full bg-rose-500/15 px-2 py-0.5 text-[10px] font-semibold text-rose-300">
                <Flame size={11} /> {status.campaign.replace(/_/g, " ")}
              </span>
            )}
          </div>
          <div className="mt-1 font-mono text-sm text-slate-100">{businessTime(status?.clock.business_now)}</div>
          <div className="mt-2 flex gap-1">
            {SPEEDS.map((sp) => (
              <button key={sp} onClick={() => run("speed", () => api.setClock(sp))}
                className={`flex-1 rounded-md py-1 font-mono text-[11px] ${status?.clock.speed === sp ? "bg-sky-500/30 text-sky-100" : "bg-white/5 text-slate-400 hover:text-slate-200"}`}>
                ×{sp}
              </button>
            ))}
          </div>
          <p className="mt-1.5 text-[10px] text-slate-500">
            ×{status?.clock.speed ?? "–"}: 1 business day = {status ? (1440 / status.clock.speed).toFixed(1) : "–"} real minutes
          </p>
          <div className="mt-3 grid grid-cols-2 gap-2">
            <button onClick={() => run("flash", () => api.action("flash_sale", 1))} disabled={busy !== null}
              className="flex items-center justify-center gap-1.5 rounded-lg bg-orange-500/15 py-2 text-xs font-semibold text-orange-200 ring-1 ring-orange-400/30 hover:bg-orange-500/25">
              <Flame size={13} /> Flash sale now
            </button>
            <button onClick={() => run("mega", () => api.action("mega_sale", 1))} disabled={busy !== null}
              className="flex items-center justify-center gap-1.5 rounded-lg bg-rose-500/15 py-2 text-xs font-semibold text-rose-200 ring-1 ring-rose-400/30 hover:bg-rose-500/25">
              <Sparkles size={13} /> Start 11.11
            </button>
          </div>
        </div>

        {status?.seeding.running && (
          <div className="mt-5">
            <div className="mb-1 flex justify-between text-xs text-slate-400">
              <span>Seeding {status.seeding.phase} via API ({status.seeding.preset})</span>
              <span className="font-mono">{status.seeding.done}/{status.seeding.target}</span>
            </div>
            <div className="h-1.5 rounded-full bg-white/5">
              <div className="h-full rounded-full bg-sky-400 transition-all"
                style={{ width: `${(status.seeding.done / Math.max(status.seeding.target, 1)) * 100}%` }} />
            </div>
          </div>
        )}

        <div className="mt-5 grid grid-cols-4 gap-2 text-center">
          {[
            { label: "ticks", value: status?.ticks ?? 0 },
            { label: "API calls", value: status?.api.requests ?? 0 },
            { label: "rejected", value: status?.api.rejected ?? 0, tone: "text-amber-300" },
            { label: "errors", value: (status?.errors ?? 0) + (status?.api.failed ?? 0), tone: "text-rose-300" },
          ].map((s) => (
            <div key={s.label} className="rounded-lg bg-white/[0.03] py-2">
              <div className={`font-mono text-base ${s.value && s.tone ? s.tone : "text-slate-100"}`}>{s.value}</div>
              <div className="text-[11px] text-slate-500">{s.label}</div>
            </div>
          ))}
        </div>
      </Card>

      <Card
        title="Manual actions"
        subtitle="Fire a single business event and watch it flow"
        action={
          <div className="flex rounded-lg bg-white/5 p-0.5 text-xs">
            {[1, 5, 20].map((n) => (
              <button key={n} onClick={() => setBatch(n)}
                className={`rounded-md px-2 py-1 font-mono ${batch === n ? "bg-slate-700 text-white" : "text-slate-400 hover:text-slate-200"}`}>
                ×{n}
              </button>
            ))}
          </div>
        }
      >
        <div className="grid grid-cols-2 gap-2">
          {ACTIONS.map(({ name, label, icon: Icon, tint }) => (
            <button
              key={name}
              disabled={busy !== null}
              onClick={() => run(name, () => api.action(name, batch))}
              className="group flex items-center gap-2 rounded-xl border border-white/5 bg-white/[0.03] px-3 py-2.5 text-left text-sm text-slate-200 transition hover:border-white/10 hover:bg-white/[0.07] disabled:opacity-50"
            >
              <Icon size={16} className={`${tint} transition group-hover:scale-110`} />
              {busy === name ? "…" : label}
            </button>
          ))}
        </div>
        {error && <p className="mt-3 rounded-lg bg-rose-500/10 px-3 py-2 text-xs text-rose-300">{error}</p>}
      </Card>

      <Card title="Activity log" subtitle="What the generator just wrote to PostgreSQL">
        <div className="h-64 overflow-y-auto pr-1 font-mono text-[11px] leading-5">
          {logs.length === 0 && <p className="text-slate-500">No activity yet — start the simulation or fire an action.</p>}
          {logs.map((l) => (
            <div key={l.seq} className={`animate-slide-in truncate ${logTone(l)}`} title={l.message}>
              <span className="text-slate-600">{l.ts.slice(11, 19)} </span>
              {l.message}
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}
