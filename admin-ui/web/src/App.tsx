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
import { Cache } from "./tabs/Cache";
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
  "Cache",
] as const;
type Tab = (typeof TABS)[number];

// Clean path-based routing (PRD §6a). Each tab maps to a path slug so tabs are
// directly linkable: /topology, /nodes, /messages, /boot-waterfall, /timeline,
// /alerts, /chaos, /tests, /cache. The server SPA-fallback (main.rs) serves
// index.html for any non-/api, non-asset GET so deep links boot the app.
const TAB_TO_SLUG: Record<Tab, string> = {
  Topology: "topology",
  Nodes: "nodes",
  Messages: "messages",
  "Boot Waterfall": "boot-waterfall",
  Chaos: "chaos",
  Timeline: "timeline",
  Alerts: "alerts",
  Tests: "tests",
  Cache: "cache",
};
const SLUG_TO_TAB: Record<string, Tab> = Object.fromEntries(
  (Object.entries(TAB_TO_SLUG) as [Tab, string][]).map(([t, s]) => [s, t]),
) as Record<string, Tab>;

function tabFromPath(pathname: string): Tab {
  const slug = pathname.replace(/^\/+/, "").split("/")[0].toLowerCase();
  return SLUG_TO_TAB[slug] ?? "Topology";
}

export function App() {
  const [tab, setTab] = useState<Tab>(() => tabFromPath(window.location.pathname));
  const [summary, setSummary] = useState<ClusterSummary | null>(null);

  // Back/forward navigation: sync tab from the URL on popstate.
  useEffect(() => {
    const onPop = () => setTab(tabFromPath(window.location.pathname));
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  // Clicking a tab updates the address bar without a reload (pushState).
  const selectTab = (t: Tab) => {
    setTab(t);
    const path = `/${TAB_TO_SLUG[t]}`;
    if (window.location.pathname !== path) {
      window.history.pushState({ tab: t }, "", path);
    }
  };

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
            onClick={() => selectTab(t)}
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
        {tab === "Cache" && <Cache />}
      </main>
    </div>
  );
}
