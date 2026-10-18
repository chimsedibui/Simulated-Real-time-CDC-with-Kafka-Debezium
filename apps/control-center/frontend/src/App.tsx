import { useState } from "react";
import { Radio, Warehouse } from "lucide-react";
import { api } from "./api";
import { GeneratorPanel } from "./components/GeneratorPanel";
import { LivePanel } from "./components/LivePanel";
import { PipelineBar } from "./components/PipelineBar";
import { WarehousePanel } from "./components/WarehousePanel";
import { usePoll } from "./hooks";

const TABS = [
  { id: "live", label: "Live CDC stream", icon: Radio },
  { id: "warehouse", label: "Warehouse analytics", icon: Warehouse },
] as const;

type TabId = (typeof TABS)[number]["id"];

const tabFromHash = (): TabId => (window.location.hash === "#warehouse" ? "warehouse" : "live");

export default function App() {
  const [tab, setTabState] = useState<TabId>(tabFromHash);
  const { data: health } = usePoll(api.health, 3000);

  const setTab = (id: TabId) => {
    setTabState(id);
    history.replaceState(null, "", `#${id}`);
  };

  return (
    <div className="min-h-screen bg-slate-950 bg-[radial-gradient(ellipse_at_top,_rgba(56,189,248,0.08),_transparent_60%)] text-slate-200">
      <div className="mx-auto max-w-[1600px] px-4 py-6 sm:px-6">
        <header className="mb-5 flex flex-wrap items-end justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-white">
              CDC Control Center
            </h1>
            <p className="mt-1 text-sm text-slate-400">
              PostgreSQL → Debezium → Kafka → ClickHouse · generate traffic, watch every change, query the warehouse
            </p>
          </div>
        </header>

        <PipelineBar health={health} />

        <div className="mt-5 grid gap-5 lg:grid-cols-[360px_minmax(0,1fr)]">
          <aside className="lg:sticky lg:top-6 lg:self-start">
            <GeneratorPanel />
          </aside>

          <main className="min-w-0">
            <nav className="mb-4 inline-flex rounded-xl border border-white/5 bg-slate-900/60 p-1">
              {TABS.map(({ id, label, icon: Icon }) => (
                <button
                  key={id}
                  onClick={() => setTab(id)}
                  className={`flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition ${
                    tab === id ? "bg-slate-700/70 text-white shadow" : "text-slate-400 hover:text-slate-200"
                  }`}
                >
                  <Icon size={15} />
                  {label}
                </button>
              ))}
            </nav>
            {/* Keep LivePanel mounted so the feed keeps accumulating while on the warehouse tab */}
            <div className={tab === "live" ? "" : "hidden"}><LivePanel /></div>
            {tab === "warehouse" && <WarehousePanel />}
          </main>
        </div>
      </div>
    </div>
  );
}
