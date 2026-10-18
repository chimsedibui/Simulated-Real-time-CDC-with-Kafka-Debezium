import { useState } from "react";
import { Cable, Database, Radio, Server, Sparkles, Warehouse } from "lucide-react";
import { api, type Health } from "../api";
import { fmt } from "../ui";

type NodeState = "ok" | "warn" | "down";

const DOT: Record<NodeState, string> = {
  ok: "bg-emerald-400 shadow-[0_0_12px] shadow-emerald-400/60",
  warn: "bg-amber-400 shadow-[0_0_12px] shadow-amber-400/60",
  down: "bg-rose-500 shadow-[0_0_12px] shadow-rose-500/60",
};

function Node({ icon, name, state, detail }: { icon: React.ReactNode; name: string; state: NodeState; detail: React.ReactNode }) {
  return (
    <div className="flex min-w-0 flex-1 items-center gap-3 rounded-xl border border-white/5 bg-slate-900/70 px-3 py-2.5">
      <div className="grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-white/5 text-slate-200">{icon}</div>
      <div className="min-w-0">
        <div className="flex items-center gap-2 text-sm font-medium text-slate-100">
          {name}
          <span className={`h-1.5 w-1.5 rounded-full ${DOT[state]}`} />
        </div>
        <div className="truncate text-xs text-slate-400">{detail}</div>
      </div>
    </div>
  );
}

function Link({ active }: { active: boolean }) {
  return (
    <div className="hidden h-[2px] w-6 shrink-0 self-center rounded lg:block xl:w-10">
      <div
        className={`h-full w-full rounded ${active ? "animate-flow" : ""}`}
        style={{
          background: active
            ? "repeating-linear-gradient(90deg, #38bdf8 0 8px, transparent 8px 12px)"
            : "rgba(148,163,184,0.2)",
          backgroundSize: "24px 2px",
        }}
      />
    </div>
  );
}

export function PipelineBar({ health }: { health: Health | null }) {
  const [registering, setRegistering] = useState(false);
  if (!health) {
    return <div className="h-[60px] animate-pulse rounded-xl bg-slate-900/60" />;
  }
  const { postgres, debezium, kafka, clickhouse, generator, source_api } = health;
  const seeding = generator.seeding?.running ? generator.seeding : null;
  const flowing = kafka.rate > 0;
  const connectorOk = debezium.state === "RUNNING";

  const register = async () => {
    setRegistering(true);
    try {
      await api.registerConnector();
    } finally {
      setRegistering(false);
    }
  };

  return (
    <div className="flex flex-col gap-2 lg:flex-row lg:items-stretch">
      <Node
        icon={<Sparkles size={18} />}
        name="Simulator"
        state={!generator.up ? "down" : generator.running || seeding ? "ok" : "warn"}
        detail={
          !generator.up ? "unreachable"
            : seeding ? `seeding ${fmt.num(seeding.done)}/${fmt.num(seeding.target)}`
            : generator.running ? "calling the API" : "idle"
        }
      />
      <Link active={generator.running || Boolean(seeding)} />
      <Node
        icon={<Server size={18} />}
        name="Source API"
        state={source_api.up ? "ok" : "down"}
        detail={source_api.up ? "only writer" : "unreachable"}
      />
      <Link active={generator.running || Boolean(seeding)} />
      <Node
        icon={<Database size={18} />}
        name="PostgreSQL"
        state={postgres.up ? "ok" : "down"}
        detail={
          postgres.up
            ? `${fmt.num(postgres.counts.orders)} orders · slot lag ${fmt.bytes(postgres.slot_lag_bytes ?? 0)}`
            : "unreachable"
        }
      />
      <Link active={flowing} />
      <Node
        icon={<Cable size={18} />}
        name="Debezium"
        state={connectorOk ? "ok" : debezium.up ? "warn" : "down"}
        detail={
          debezium.state === "NOT_REGISTERED" ? (
            <button onClick={register} disabled={registering} className="font-medium text-sky-300 hover:text-sky-200">
              {registering ? "registering…" : "not registered → register"}
            </button>
          ) : (
            debezium.state.toLowerCase()
          )
        }
      />
      <Link active={flowing} />
      <Node
        icon={<Radio size={18} />}
        name="Kafka"
        state={kafka.up ? "ok" : "down"}
        detail={kafka.up ? `${kafka.rate} events/s` : "unreachable"}
      />
      <Link active={flowing && clickhouse.up} />
      <Node
        icon={<Warehouse size={18} />}
        name="ClickHouse"
        state={clickhouse.up ? "ok" : "down"}
        detail={clickhouse.up ? `${fmt.num(clickhouse.bronze_rows ?? 0)} events stored` : "unreachable"}
      />
    </div>
  );
}
