import { useRef, useState } from "react";
import { Activity, ChevronRight } from "lucide-react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api, type CdcEvent, type Op } from "../api";
import { useNow, usePoll } from "../hooks";
import { Card, Empty, OP_META, OpBadge, Stat, TableChip, fmt, tooltipStyle } from "../ui";

const OPS: Op[] = ["c", "u", "d", "r"];

function formatValue(v: unknown) {
  if (v === null || v === undefined) return "∅";
  return typeof v === "object" ? JSON.stringify(v) : String(v);
}

function EventRow({ e, now }: { e: CdcEvent; now: number }) {
  const [open, setOpen] = useState(false);
  const lag = e.source_ts ? e.received_at - e.source_ts : null;
  return (
    <li className="animate-slide-in border-b border-white/5 last:border-0">
      <button onClick={() => setOpen(!open)} className="flex w-full items-center gap-3 px-2 py-2 text-left hover:bg-white/[0.03]">
        <ChevronRight size={14} className={`shrink-0 text-slate-500 transition ${open ? "rotate-90" : ""}`} />
        <OpBadge op={e.op} />
        <span className="w-44 shrink-0 truncate"><TableChip table={e.table} /></span>
        <span className="min-w-0 flex-1 truncate text-sm text-slate-300">
          {e.label}
          {e.changes.length > 0 && (
            <span className="ml-2 text-xs text-slate-500">
              {e.changes.map((c) => c.field).join(", ")}
            </span>
          )}
        </span>
        {lag !== null && <span className="hidden font-mono text-[11px] text-slate-500 sm:inline">{lag} ms</span>}
        <span className="w-14 shrink-0 text-right font-mono text-[11px] text-slate-500">{fmt.ago(e.received_at, now)}</span>
      </button>
      {open && (
        <div className="mx-2 mb-3 rounded-lg bg-black/30 p-3 font-mono text-[11px]">
          {e.changes.length > 0 ? (
            <table className="w-full">
              <tbody>
                {e.changes.map((c) => (
                  <tr key={c.field}>
                    <td className="py-0.5 pr-4 text-slate-400">{c.field}</td>
                    <td className="py-0.5 pr-2 text-rose-300/80 line-through">{formatValue(c.before)}</td>
                    <td className="py-0.5 text-emerald-300">{formatValue(c.after)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <pre className="overflow-x-auto whitespace-pre-wrap text-slate-300">{JSON.stringify(e.row, null, 2)}</pre>
          )}
        </div>
      )}
    </li>
  );
}

export function LivePanel() {
  const since = useRef(0);
  const [events, setEvents] = useState<CdcEvent[]>([]);
  const [filter, setFilter] = useState<string>("all");
  const now = useNow();

  const { data, error } = usePoll(async () => {
    const snap = await api.cdc(since.current);
    if (snap.seq < since.current) {
      // backend restarted
      since.current = 0;
      setEvents([]);
    }
    if (snap.events.length) {
      since.current = snap.events[snap.events.length - 1].seq;
      setEvents((prev) => [...snap.events.slice().reverse(), ...prev].slice(0, 200));
    }
    return snap;
  }, 1000);

  const totals = data?.totals ?? {};
  const sumOp = (op: Op) => Object.values(totals).reduce((s, t) => s + (t[op] ?? 0), 0);
  const tableTotal = (t: string) => OPS.reduce((s, op) => s + (totals[t]?.[op] ?? 0), 0);
  // busiest tables first; the schema has more tables than fit in the breakdown
  const tables = Object.keys(totals).sort((a, b) => tableTotal(b) - tableTotal(a)).slice(0, 8);
  const maxTable = Math.max(1, ...tables.map(tableTotal));
  const timeline = (data?.timeline ?? []).map((p) => ({ ...p, label: new Date(p.t * 1000).toLocaleTimeString([], { minute: "2-digit", second: "2-digit" }) }));
  const shown = filter === "all" ? events : events.filter((e) => e.table === filter);

  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Stat label="Throughput" value={<>{data?.rate ?? 0}<span className="ml-1 text-sm text-slate-500">ev/s</span></>}
          hint="avg over last 10s" icon={<Activity size={14} />} accent="text-sky-300" />
        <Stat label="Inserts" value={fmt.num(sumOp("c"))} hint="since page opened" accent="text-emerald-300" />
        <Stat label="Updates" value={fmt.num(sumOp("u"))} hint="since page opened" accent="text-sky-300" />
        <Stat label="Deletes" value={fmt.num(sumOp("d"))} hint="since page opened" accent="text-rose-300" />
      </div>

      <div className="grid gap-4 xl:grid-cols-3">
        <Card title="Change events per second" subtitle="Last 60 seconds, stacked by operation" className="xl:col-span-2">
          <div className="h-56">
            <ResponsiveContainer>
              <AreaChart data={timeline} margin={{ left: -20, right: 4, top: 4 }}>
                <defs>
                  {OPS.map((op) => (
                    <linearGradient key={op} id={`g-${op}`} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={OP_META[op].color} stopOpacity={0.5} />
                      <stop offset="100%" stopColor={OP_META[op].color} stopOpacity={0.02} />
                    </linearGradient>
                  ))}
                </defs>
                <CartesianGrid stroke="rgba(148,163,184,0.08)" vertical={false} />
                <XAxis dataKey="label" tick={{ fill: "#64748b", fontSize: 10 }} interval={9} tickLine={false} axisLine={false} />
                <YAxis allowDecimals={false} tick={{ fill: "#64748b", fontSize: 10 }} tickLine={false} axisLine={false} />
                <Tooltip {...tooltipStyle} />
                {/* deletes at the bottom so an all-zero series doesn't draw over the stack's top edge */}
                {(["d", "c", "u"] as Op[]).map((op) => (
                  <Area key={op} type="monotone" dataKey={op} name={OP_META[op].label} stackId="1"
                    stroke={OP_META[op].color} fill={`url(#g-${op})`} strokeWidth={1.5} isAnimationActive={false} />
                ))}
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Card>

        <Card title="Events by table" subtitle="Busiest 8 tables since page opened">
          <div className="space-y-3">
            {tables.map((t) => {
              const total = tableTotal(t);
              return (
                <div key={t}>
                  <div className="mb-1 flex justify-between text-xs">
                    <TableChip table={t} />
                    <span className="font-mono text-slate-400">{fmt.num(total)}</span>
                  </div>
                  <div className="flex h-2 overflow-hidden rounded-full bg-white/5">
                    {OPS.map((op) => {
                      const n = totals[t]?.[op] ?? 0;
                      return n ? (
                        <div key={op} title={`${OP_META[op].label}: ${n}`} className="h-full transition-all duration-500"
                          style={{ width: `${(n / maxTable) * 100}%`, background: OP_META[op].color }} />
                      ) : null;
                    })}
                  </div>
                </div>
              );
            })}
            <div className="flex flex-wrap gap-3 pt-1 text-[11px] text-slate-400">
              {OPS.map((op) => (
                <span key={op} className="flex items-center gap-1.5">
                  <span className="h-2 w-2 rounded-sm" style={{ background: OP_META[op].color }} />
                  {OP_META[op].label.toLowerCase()}
                </span>
              ))}
            </div>
          </div>
        </Card>
      </div>

      <Card
        title="Live event feed"
        subtitle="Debezium envelopes consumed from cdc.ecommerce.* — click a row for the before/after diff"
        action={
          <select value={filter} onChange={(e) => setFilter(e.target.value)}
            className="rounded-lg border border-white/10 bg-slate-800 px-2 py-1 text-xs text-slate-200">
            <option value="all">All tables</option>
            {Object.keys(totals).sort().map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
        }
      >
        {error && !data && <Empty>Cannot reach the API: {error}</Empty>}
        {data && !data.connected && <Empty>Connecting to Kafka…</Empty>}
        {data?.connected && shown.length === 0 && <Empty>Waiting for change events — start the generator on the left.</Empty>}
        {shown.length > 0 && (
          <ul className="max-h-[28rem] overflow-y-auto">
            {shown.map((e) => <EventRow key={e.seq} e={e} now={now} />)}
          </ul>
        )}
      </Card>
    </div>
  );
}
