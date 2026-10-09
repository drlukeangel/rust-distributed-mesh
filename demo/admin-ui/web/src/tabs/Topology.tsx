import { useEffect, useMemo, useState } from "react";
import ReactFlow, { Background, Controls, MarkerType, type Edge as FlowEdge, type Node as FlowNode } from "reactflow";
import { api, type Edge, type Topology as Topo, type TopologyNode } from "../api";

const KIND_COLOR: Record<string, string> = { node_admin: "#e3b341", rpc_node: "#8b949e" };
const MESH_COLORS = ["#58a6ff", "#bc8cff", "#f0883e", "#56d4dd", "#ff7b72"];

const meshColor = (names: string[], m: string) => MESH_COLORS[names.indexOf(m) % MESH_COLORS.length];
const bad = (e: Edge) => e.state !== "connected";

function layout(nodes: TopologyNode[]): FlowNode[] {
  const meshes = Array.from(new Set(nodes.map((n) => n.mesh))).sort();
  const out: FlowNode[] = [];
  const W = 560, H = 460, GAP = 620;
  meshes.forEach((m, i) => {
    const members = nodes.filter((n) => n.mesh === m).sort((a, b) => a.name.localeCompare(b.name));
    out.push({
      id: `group-${m}`,
      type: "group",
      position: { x: 40 + i * GAP, y: 40 },
      style: { width: W, height: H, background: `${meshColor(meshes, m)}0F`, border: `1px dashed ${meshColor(meshes, m)}`, borderRadius: 12 },
      data: { label: m },
      selectable: false,
    });
    out.push({
      id: `label-${m}`,
      position: { x: 40 + i * GAP + 10, y: 6 },
      data: { label: `${m} · ${members.length} nodes` },
      style: { background: "transparent", border: "none", color: meshColor(meshes, m), fontFamily: "ui-monospace, monospace", fontWeight: 600, fontSize: 13, width: 220 },
      draggable: false,
      selectable: false,
    });
    members.forEach((n, idx) => {
      const ang = (2 * Math.PI * idx) / Math.max(1, members.length) - Math.PI / 2;
      const r = 165;
      const c = KIND_COLOR[n.kind] ?? "#8b949e";
      out.push({
        id: n.name,
        parentNode: `group-${m}`,
        extent: "parent",
        position: { x: W / 2 + r * Math.cos(ang) - 85, y: H / 2 + r * Math.sin(ang) - 46 },
        data: {
          label: (
            <div style={{ textAlign: "center", lineHeight: 1.2 }} data-testid={`node-${n.name}`}>
              <div className="mono" style={{ fontSize: 11, color: "#c9d1d9" }}>{n.name}</div>
              <div style={{ fontSize: 10, color: "#8b949e" }}>{n.kind} · {n.status}</div>
              {n.seat && <div style={{ fontSize: 10, color: "#e3b341" }} data-testid={`seat-${n.name}`}>{n.seat}</div>}
            </div>
          ),
        },
        style: { background: `${c}22`, border: `2px solid ${c}`, color: "#fff", width: 170, height: 92, borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center" },
      });
    });
  });
  return out;
}

function flowEdges(edges: Edge[]): FlowEdge[] {
  return edges.map((e, i) => {
    const colour = bad(e) ? "#f85149" : e.kind === "direct" ? "#3fb950" : "#58a6ff";
    return {
      id: `${e.source}>${e.destination}:${e.kind}:${i}`,
      type: "straight",
      source: e.source,
      target: e.destination,
      label: e.kind === "proxy" ? `proxy via ${e.carrier ?? "?"}${bad(e) ? ` (${e.state})` : ""}` : bad(e) ? e.state : undefined,
      labelStyle: { fill: colour, fontSize: 10, fontFamily: "ui-monospace, monospace" },
      labelBgStyle: { fill: "#0d1117" },
      style: { stroke: colour, strokeWidth: 2, strokeDasharray: e.kind === "proxy" ? "7 5" : undefined },
      markerEnd: { type: MarkerType.ArrowClosed, color: colour },
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

  if (err) return <div className="card">topology load failed: {err}</div>;
  if (data.nodes.length === 0) return <div className="card muted">no nodes yet — press <b>bootstrap 2+3</b> above.</div>;
  return (
    <div>
      <div className="legend mono">
        <span style={{ color: "#3fb950" }}>━━ direct</span>
        <span style={{ color: "#58a6ff" }}>╍╍ proxy (labelled with its carrier)</span>
        <span style={{ color: "#f85149" }}>━━ failed / disconnected</span>
        <span className="muted">{data.edges.length} connection facts{data.edge_errors.length ? ` · unreadable: ${data.edge_errors.join("; ")}` : ""}</span>
      </div>
      <div style={{ height: "calc(100vh - 290px)", border: "1px solid var(--border)", borderRadius: 6 }}>
        <ReactFlow nodes={nodes} edges={edges} fitView fitViewOptions={{ padding: 0.15 }} nodesConnectable={false} elementsSelectable={false} proOptions={{ hideAttribution: true }}>
          <Background gap={20} color="#161b22" />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
    </div>
  );
}
