import { ExternalLink } from "lucide-react";
import { api } from "./api";
import { GeneratorPanel } from "./components/GeneratorPanel";
import { LivePanel } from "./components/LivePanel";
import { PipelineBar } from "./components/PipelineBar";
import { usePoll } from "./hooks";

export default function App() {
  const { data: health } = usePoll(api.health, 3000);
  const grafana = `${window.location.protocol}//${window.location.hostname}:3000`;

  return (
    <div className="min-h-screen bg-slate-950 bg-[radial-gradient(ellipse_at_top,_rgba(56,189,248,0.08),_transparent_60%)] text-slate-200">
      <div className="mx-auto max-w-[1600px] px-4 py-6 sm:px-6">
        <header className="mb-5 flex flex-wrap items-end justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-white">
              CDC Control Center
            </h1>
            <p className="mt-1 text-sm text-slate-400">
              PostgreSQL → Debezium → Kafka → ClickHouse · generate traffic and watch every change
            </p>
          </div>
        </header>

        <PipelineBar health={health} />

        <div className="mt-5 grid gap-5 lg:grid-cols-[360px_minmax(0,1fr)]">
          <aside className="lg:sticky lg:top-6 lg:self-start">
            <GeneratorPanel />
          </aside>

          <main className="min-w-0">
            <nav className="mb-4 flex flex-wrap gap-3 text-sm text-sky-400">
              <a href={`${grafana}/d/cdc-business`} target="_blank" rel="noreferrer" className="inline-flex items-center gap-2">
                Business analytics <ExternalLink size={14} />
              </a>
              <a href={`${grafana}/d/cdc-platform`} target="_blank" rel="noreferrer" className="inline-flex items-center gap-2">
                Platform health <ExternalLink size={14} />
              </a>
            </nav>
            <LivePanel />
          </main>
        </div>
      </div>
    </div>
  );
}
