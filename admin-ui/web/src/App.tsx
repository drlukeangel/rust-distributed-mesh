import { useEffect, useState } from "react";
import { SpawnBar } from "./SpawnBar";
import { Topology } from "./tabs/Topology";
import { BootWaterfall } from "./tabs/BootWaterfall";
import { Nodes } from "./tabs/Nodes";
import { Alerts } from "./tabs/Alerts";
import { Chaos } from "./tabs/Chaos";
import { Timeline } from "./tabs/Timeline";
import { Tests } from "./tabs/Tests";
import { Messages } from "./tabs/Messages";
import { api, type ClusterSummary } from "./api";

const TABS = [
  "Topology",
  "Nodes",
  "Messages",
  "Boot Waterfall",
  "Chaos",
  "Timeline",
  "Alerts",
  "Tests",
] as const;
type Tab = (typeof TABS)[number];

export function App() {
  const [tab, setTab] = useState<Tab>("Topology");
  const [summary, setSummary] = useState<ClusterSummary | null>(null);

  useEffect(() => {
    const refresh = () =>
      api.summary().then(setSummary).catch(() => setSummary(null));
    refresh();
    const id = setInterval(refresh, 3000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="layout">
      <header>
        <h1>rafka mesh — live</h1>
        {summary && (
          <div className="cluster-summary">
            {summary.spawned} spawned · meshes: {summary.meshes.join(", ") || "—"} ·
            chaos: {summary.chaos_per_min}/min · mean peers: {summary.mean_peers.toFixed(1)}
          </div>
        )}
        <SpawnBar />
      </header>
      <div className="tabs">
        {TABS.map((t) => (
          <div
            key={t}
            className={"tab" + (tab === t ? " active" : "")}
            onClick={() => setTab(t)}
          >
            {t}
          </div>
        ))}
      </div>
      <main>
        {tab === "Topology" && <Topology />}
        {tab === "Nodes" && <Nodes />}
        {tab === "Boot Waterfall" && <BootWaterfall />}
        {tab === "Chaos" && <Chaos />}
        {tab === "Timeline" && <Timeline />}
        {tab === "Alerts" && <Alerts />}
        {tab === "Tests" && <Tests />}
        {tab === "Messages" && <Messages />}
      </main>
    </div>
  );
}
