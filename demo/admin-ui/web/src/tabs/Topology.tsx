import { useEffect, useMemo, useState } from "react";
import ReactFlow, { Background, Controls, MarkerType, MiniMap, type Edge as FlowEdge, type Node as FlowNode } from "reactflow";
import { api, type Edge, type Topology as Topo, type TopologyNode } from "../api";
import { colourOfKind } from "../kinds";
import { LoadLines } from "../Load";

const MESH_COLORS = ["#bc8cff", "#f0883e", "#56d4dd", "#ff7b72", "#79c0ff", "#d2a8ff"];
const GOLD = "#e3b341";
const STATE_COLOUR: Record<string, string> = { unheard: "#db6d28", recovered: "#d29922", failed: "#f85149", disconnected: "#6e7681" };

const meshColor = (names: string[], m: string) => MESH_COLORS[names.indexOf(m) % MESH_COLORS.length];
const ROWS = ["node_admin", "gateway", "broker", "compute"];
const W = 720, H = 600, GAP = 780, NODE_W = 150, NODE_H = 118;

/// Nodes grouped by mesh in labelled regions, one row per role: the node-admins (the backbone's
/// listeners, drawn as the old bridges were: gold, at the top), then gateways, brokers, computes.
function layout(nodes: TopologyNode[]): FlowNode[] {
  const meshes = Array.from(new Set(nodes.map((n) => n.mesh))).sort();
  const out: FlowNode[] = [];
  meshes.forEach((m, i) => {
    const members = nodes.filter((n) => n.mesh === m).sort((a, b) => a.name.localeCompare(b.name));
    const mc = meshColor(meshes, m);
    out.push({
      id: `group-${m}`,
      type: "group",
      position: { x: 40 + i * GAP, y: 50 },
      style: { width: W, height: H, background: `${mc}0F`, border: `1px dashed ${mc}`, borderRadius: 12 },
      data: { label: m },
      selectable: false,
    });
    out.push({
      id: `label-${m}`,
      position: { x: 40 + i * GAP + W / 2 - 90, y: 14 },
      data: { label: `${m} · ${members.length} nodes` },
      style: { background: "transparent", border: "none", color: mc, fontFamily: "ui-monospace, monospace", fontWeight: 600, fontSize: 13, width: 180 },
      draggable: false,
      selectable: false,
    });
    members.forEach((n) => {
      const row = Math.max(0, ROWS.indexOf(n.kind));
      const inRow = members.filter((x) => x.kind === n.kind);
      const idx = inRow.findIndex((x) => x.name === n.name);
      const slot = W / (inRow.length + 1);
      const c = colourOfKind(n.kind);
      const badge = n.backbone === "publisher" ? "backbone publisher" : n.backbone === "listener" ? "backbone listener" : "";
      out.push({
        id: n.name,
        parentNode: `group-${m}`,
        extent: "parent",
        position: { x: slot * (idx + 1) - NODE_W / 2, y: 30 + row * 140 },
        data: {
          label: (
            <div style={{ textAlign: "center", lineHeight: 1.15 }} data-testid={`node-${n.name}`}>
              <div className="mono" style={{ fontSize: 10, color: "#c9d1d9" }}>{n.name}</div>
              <div style={{ fontSize: 9, color: "#8b949e", marginTop: 2 }}>{n.kind}</div>
              <div style={{ fontSize: 9, color: n.status === "ready-for-traffic" ? "#8b949e" : "#f85149" }}>{n.status}</div>
              <LoadLines load={n.load} />
              <div style={{ fontSize: 9, color: "#6e7681" }}>RX/TX: pending R-L2</div>
              {n.seat && <div style={{ fontSize: 9, color: GOLD }} data-testid={`seat-${n.name}`}>{n.seat}</div>}
              {badge && <div style={{ fontSize: 9, color: GOLD }} data-testid={`backbone-${n.name}`}>{badge}</div>}
            </div>
          ),
        },
        style: { background: `${c}33`, border: `2px solid ${c}`, color: "#fff", width: NODE_W, height: NODE_H, borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center" },
      });
    });
  });
  return out;
}

/// Intra-mesh edges are faint grey; the cross-mesh edges between backbone listeners are dashed gold;
/// any other cross-mesh edge is dashed grey-blue; a proxy is dashed with its carrier; a failed edge is red.
function style(e: Edge): { colour: string; width: number; dash?: string; opacity: number } {
  if (e.state === "failed") return { colour: STATE_COLOUR.failed, width: 2.5, dash: e.kind === "proxy" ? "7 5" : undefined, opacity: 1 };
  if (e.state === "unheard") return { colour: STATE_COLOUR.unheard, width: 1.5, dash: "3 3", opacity: 0.8 };
  if (e.state === "recovered" && e.cross_mesh && e.backbone) return { colour: GOLD, width: 2.5, dash: "8 6", opacity: 1 };
  if (e.state === "recovered") return { colour: STATE_COLOUR.recovered, width: 2, dash: e.cross_mesh ? "7 5" : undefined, opacity: 0.9 };
  if (e.state === "disconnected") return { colour: STATE_COLOUR.disconnected, width: 1.2, dash: "2 4", opacity: 0.6 };
  if (e.kind === "proxy") return { colour: "#58a6ff", width: 2, dash: "7 5", opacity: 0.9 };
  if (e.cross_mesh && e.backbone) return { colour: GOLD, width: 2.5, dash: "8 6", opacity: 1 };
  if (e.cross_mesh) return { colour: "#79c0ff", width: 1.2, dash: "5 5", opacity: 0.55 };
  return { colour: "#8b949e", width: 1, opacity: 0.22 };
}

function flowEdges(edges: Edge[]): FlowEdge[] {
  return edges.map((e, i) => {
    const st = style(e);
    const label = e.kind === "proxy" ? `proxy via ${e.carrier ?? "?"}${e.state !== "connected" ? ` (${e.state})` : ""}` : e.state === "failed" || e.state === "recovered" || e.state === "unheard" ? e.state : undefined;
    return {
      id: `${e.source}>${e.destination}:${e.kind}:${e.state}:${i}`,
      type: e.cross_mesh ? "default" : "straight",
      source: e.source,
      target: e.destination,
      label,
      labelStyle: { fill: st.colour, fontSize: 10, fontFamily: "ui-monospace, monospace" },
      labelBgStyle: { fill: "#0d1117" },
      zIndex: e.cross_mesh ? 10 : 0,
      style: { stroke: st.colour, strokeWidth: st.width, strokeDasharray: st.dash, opacity: st.opacity },
      markerEnd: e.cross_mesh || e.state === "failed" ? { type: MarkerType.ArrowClosed, color: st.colour } : undefined,
      data: { state: e.state, basis: e.basis, backbone: e.backbone },
    };
  });
}

export function Topology() {
  const [data, setData] = useState<Topo>({ nodes: [], edges: [], edge_errors: [] });
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    const load = () => api.topology().then((d) => { setData(d); setErr(null); }).catch((e) => setErr(String(e)));
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);
  const nodes = useMemo(() => layout(data.nodes), [data.nodes]);
  const edges = useMemo(() => flowEdges(data.edges), [data.edges]);
  const bb = data.edges.filter((e) => e.cross_mesh && e.backbone);

  if (err) return <div className="card">topology load failed: {err}</div>;
  if (data.nodes.length === 0) return <div className="card muted">no nodes yet — press <b>bootstrap 2-mesh</b> above.</div>;
  return (
    <div>
      <div className="legend mono" data-testid="topology-legend">
        <span style={{ color: "#8b949e" }}>━━ inside a mesh (faint)</span>
        <span style={{ color: GOLD }}>╍╍ backbone: node-admin to node-admin across meshes</span>
        <span style={{ color: "#79c0ff" }}>╍╍ other cross-mesh</span>
        <span style={{ color: "#58a6ff" }}>╍╍ proxy (labelled with its carrier)</span>
        <span style={{ color: STATE_COLOUR.recovered }}>━━ recovered</span>
        <span style={{ color: STATE_COLOUR.unheard }}>╍╍ unheard: a held connection of a node that is not ready-for-traffic</span>
        <span style={{ color: STATE_COLOUR.failed }}>━━ failed</span>
        <span style={{ color: STATE_COLOUR.disconnected }}>·· disconnected</span>
      </div>
      <div className="legend mono muted" data-testid="topology-rule">
        Edge rule: every node writes the facts about its own connections; an edge is the CURRENT state of the pair. A fact naming a
        process birth that is gone, or a node that left, is not drawn. A failed fact is red only while a network cut in force separates the
        two ends or either end is not ready-for-traffic; once a later connected fact on the pair exists (either direction), or both ends are
        ready-for-traffic and heard, it is drawn as recovered. A connected fact whose end is not ready-for-traffic is drawn unheard. Node-admins on the backbone (gold cards; the mesh primary publishes there)
        are always linked across meshes: from their facts, else from the backbone membership itself.
      </div>
      <div className="legend mono muted" data-testid="topology-counts">
        {(["connected", "unheard", "recovered", "failed", "disconnected"] as const).map((s) => `${s} ${data.edges.filter((e) => e.state === s).length}`).join(" · ")}
        {" · "}{data.edges.length} edges from {data.nodes.length} nodes · backbone links {bb.length}
        {data.edge_errors.length ? ` · unreadable: ${data.edge_errors.join("; ")}` : ""}
      </div>
      <div style={{ height: "calc(100vh - 400px)", minHeight: 520, border: "1px solid var(--border)", borderRadius: 6 }}>
        <ReactFlow nodes={nodes} edges={edges} fitView fitViewOptions={{ padding: 0.1 }} minZoom={0.2} nodesConnectable={false} elementsSelectable={false} proOptions={{ hideAttribution: true }}>
          <Background gap={20} color="#161b22" />
          <Controls showInteractive={false} />
          <MiniMap pannable zoomable nodeColor={(n) => (n.id.startsWith("group-") || n.id.startsWith("label-") ? "#161b22" : colourOfKind((data.nodes.find((x) => x.name === n.id)?.kind) ?? ""))} maskColor="rgba(13,17,23,0.7)" />
        </ReactFlow>
      </div>
    </div>
  );
}
