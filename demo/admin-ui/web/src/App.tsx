import { useEffect, useState } from "react";
import { api, type Overview, type TrafficState } from "./api";
import { SpawnBar } from "./SpawnBar";
import { Topology } from "./tabs/Topology";
import { Nodes } from "./tabs/Nodes";
import { Builds } from "./tabs/Builds";
import { BootWaterfall } from "./tabs/BootWaterfall";
import { Chaos } from "./tabs/Chaos";
import { Timeline } from "./tabs/Timeline";
import { Alerts } from "./tabs/Alerts";
import { Messages } from "./tabs/Messages";
import { Tests } from "./tabs/Tests";

const TABS = ["Topology", "Nodes", "Messages", "Boot Waterfall", "Chaos", "Timeline", "Alerts", "Tests", "Builds"] as const;
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

function TrafficLine() {
  const [t, setT] = useState<TrafficState | null>(null);
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    const load = () => api.traffic().then((x) => { setT(x); setErr(null); }).catch((e) => setErr(String(e)));
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);
  if (err) return <div className="fabric-line muted mono" data-testid="traffic-line">traffic: {err}</div>;
  if (!t) return null;
  const fresh = Date.now() - t.updated_ms < 10000;
  return (
    <div className="fabric-line mono" data-testid="traffic-line">
      <span>traffic <b style={{ color: fresh ? "var(--ok)" : "var(--err)" }}>{fresh ? "running" : "stale"}</b></span>
      <span>{t.issued} operations</span>
      <span>{Object.entries(t.by_outcome).map(([k, v]) => `${k} ${v}`).join(" · ")}</span>
      <span className="muted">{Object.entries(t.by_route).map(([k, v]) => `${k} ${v}`).join(" · ")}</span>
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
        {overview?.summary && (
          <div className="cluster-summary" data-testid="cluster-summary">
            {overview.summary.spawned} spawned · meshes: {overview.summary.meshes.join(", ") || "—"} · chaos: {overview.summary.chaos_per_min}/min · mean peers:{" "}
            {overview.summary.mean_peers === null ? "—" : overview.summary.mean_peers.toFixed(1)}
          </div>
        )}
        <Header o={overview} />
        <TrafficLine />
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
        {tab === "Messages" && <Messages />}
        {tab === "Builds" && <Builds />}
        {tab === "Boot Waterfall" && <BootWaterfall />}
        {tab === "Chaos" && <Chaos />}
        {tab === "Alerts" && <Alerts />}
        {tab === "Timeline" && <Timeline />}
        {tab === "Tests" && <Tests />}
      </main>
    </div>
  );
}
