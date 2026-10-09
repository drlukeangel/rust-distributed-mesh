import { useEffect, useState } from "react";
import { api, type Overview } from "./api";
import { SpawnBar } from "./SpawnBar";
import { Topology } from "./tabs/Topology";
import { Nodes } from "./tabs/Nodes";
import { Builds } from "./tabs/Builds";
import { BootWaterfall } from "./tabs/BootWaterfall";
import { Chaos } from "./tabs/Chaos";
import { Timeline } from "./tabs/Timeline";

const TABS = ["Topology", "Nodes", "Builds", "Boot Waterfall", "Chaos", "Timeline"] as const;
type Tab = (typeof TABS)[number];

const STATE_COLOR: Record<string, string> = {
  pending: "var(--warn)",
  "state-sync": "var(--warn)",
  "state-commit": "var(--accent)",
  "ready-for-traffic": "var(--ok)",
  draining: "var(--warn)",
  retired: "var(--err)",
  degraded: "var(--err)",
};

function Header({ o }: { o: Overview | null }) {
  if (!o) return <div className="fabric-line muted">reading node-admin…</div>;
  if (o.error || !o.fabric) {
    return <div className="fabric-line" style={{ color: "var(--err)" }}>{o.error ?? "no fabric"}</div>;
  }
  const f = o.fabric;
  const col = STATE_COLOR[f.status] ?? "var(--fg-dim)";
  return (
    <div className="fabric-line mono" data-testid="fabric-header">
      <span>fabric <b>{f.name}</b></span>
      <span className="pill" style={{ borderColor: col, color: col }} data-testid="fabric-state">{f.status}</span>
      <span>
        build{" "}
        <b data-testid="current-build">{o.build ? `${o.build.build_id} (${o.build.state}, attempt ${o.build.attempt})` : "none"}</b>
      </span>
      <span>{o.meshes.map((m) => `${m.name}: ${m.nodes} nodes, ${m.status}`).join(" · ") || "no meshes"}</span>
      <span>{o.nodes_total} nodes total</span>
      <span className="muted">provider {f.provider} · fabric primary {f.fabric_primary ?? "—"}</span>
    </div>
  );
}

export function App() {
  const [tab, setTab] = useState<Tab>("Topology");
  const [overview, setOverview] = useState<Overview | null>(null);

  useEffect(() => {
    const load = () => api.overview().then(setOverview).catch((e) => setOverview({ fabric: null, meshes: [], nodes_total: 0, build: null, error: String(e) }));
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="layout">
      <header>
        <h1>rdm mesh — live</h1>
        <Header o={overview} />
        <SpawnBar />
      </header>
      <div className="tabs">
        {TABS.map((t) => (
          <div key={t} className={"tab" + (tab === t ? " active" : "")} onClick={() => setTab(t)}>
            {t}
          </div>
        ))}
      </div>
      <main>
        {tab === "Topology" && <Topology />}
        {tab === "Nodes" && <Nodes />}
        {tab === "Builds" && <Builds />}
        {tab === "Boot Waterfall" && <BootWaterfall />}
        {tab === "Chaos" && <Chaos />}
        {tab === "Timeline" && <Timeline />}
      </main>
    </div>
  );
}
