import { useCallback, useEffect, useMemo, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  type Edge,
  type Node,
} from "reactflow";
import { api, type TopologyResponse, type NodeType } from "../api";

const TYPE_COLOR: Record<NodeType, string> = {
  gateway: "#58a6ff",
  broker: "#f0883e",
  compute: "#3fb950",
  registry: "#bc8cff",
  "admin-ui": "#8b949e",
};

function meshColor(mesh: string): string {
  if (mesh === "mesh1" || mesh === "mesh-a") return "#bc8cff";
  if (mesh === "mesh2" || mesh === "mesh-b") return "#f0883e";
  if (mesh === "default") return "#8b949e";
  // hash-derived for arbitrary meshes
  let h = 0;
  for (const c of mesh) h = (h * 31 + c.charCodeAt(0)) & 0xffffffff;
  const palette = ["#56d4dd", "#ff7b72", "#79c0ff", "#d2a8ff", "#ffa657"];
  return palette[Math.abs(h) % palette.length];
}

/// Map a utilization ratio (used / budget) to a color used in the node
/// tile. Green = healthy, amber = getting warm, red = saturated. Returns
/// the muted text grey when budget is 0 (no data yet).
function utilColor(used: number | undefined, budget: number | undefined): string {
  if (used === undefined || budget === undefined || budget <= 0) return "#8b949e";
  const ratio = used / budget;
  if (ratio < 0.5) return "#3fb950";
  if (ratio < 0.8) return "#d29922";
  return "#f85149";
}

function nodeTypeColor(type: string): string {
  return TYPE_COLOR[type as NodeType] ?? "#8b949e";
}

/// sprint-20: node lifecycle/health → halo color. Alive is neutral (null = fall
/// back to the type color); the rest get a distinct ring so an operator sees a
/// node's state at a glance. Leaving/Dead never reach render (evicted), but are
/// mapped for completeness.
const STATE_COLOR: Record<string, string> = {
  Joining: "#58a6ff", // blue — booting/joining
  Alive: "", // neutral — use the node-type color
  Degraded: "#d29922", // amber — over budget / unhealthy
  Updating: "#a371f7", // purple — rolling/restarting
  Draining: "#db6d28", // orange — winding down
  Leaving: "#6e7681", // grey — graceful departure (evicted before render)
  Dead: "#f85149", // red — crashed (evicted before render)
};
function stateColor(state: string | undefined): string {
  if (!state) return "";
  return STATE_COLOR[state] ?? "";
}

function buildGraph(t: TopologyResponse): { nodes: Node[]; edges: Edge[] } {
  // Sprint-14 B6: the admin-ui is a NORMAL node now (mesh_id=mesh1, type=admin-ui)
  // and renders in its home mesh like everyone. The legacy mesh_id=="admin"
  // observer is gone, so no special filter. Remote-mesh nodes carry
  // source:"backbone"; the backbone directory now ships per-node CPU/RAM too, so
  // they render full metrics, plus the per-mesh aggregate in the group header.
  const observable = t.nodes;

  const byMesh = new Map<string, typeof observable>();
  for (const n of observable) {
    const m = n.mesh_id;
    if (!byMesh.has(m)) byMesh.set(m, []);
    byMesh.get(m)!.push(n);
  }
  const meshes = Array.from(byMesh.keys()).sort();

  const nodes: Node[] = [];
  const meshGap = 560; // px between mesh group centers
  const meshTop = 60;
  const meshWidth = 460;
  const meshHeight = 460;

  // Group containers — react-flow renders these as parent nodes
  meshes.forEach((m, i) => {
    nodes.push({
      id: `group-${m}`,
      type: "group",
      position: { x: 80 + i * meshGap, y: meshTop },
      style: {
        width: meshWidth,
        height: meshHeight,
        background: `${meshColor(m)}0F`,
        border: `1px dashed ${meshColor(m)}`,
        borderRadius: 12,
      },
      data: { label: m },
      draggable: true,
      selectable: false,
    });

    // Mesh label + the per-mesh AGGREGATE rollup (the thing the backbone ships).
    // For our own mesh the aggregate is summed locally; for a remote mesh it
    // arrives over the backbone. Show mesh totals: CPU (cores) and RAM (GB), plus
    // frames/sec when present (the rate over the interval since the last sample).
    const agg = t.mesh_aggregates?.[m];
    nodes.push({
      id: `label-${m}`,
      type: "default",
      position: { x: 80 + i * meshGap + meshWidth / 2 - 90, y: meshTop - 48 },
      data: {
        label: (
          <div style={{ textAlign: "center", lineHeight: 1.25 }}>
            <div style={{ fontWeight: 600, fontSize: 13 }}>
              {m} · {byMesh.get(m)!.length} nodes
            </div>
            {agg && (
              <div style={{ fontSize: 10, color: "#8b949e", fontWeight: 500 }}>
                Σ CPU {agg.cpu_used.toFixed(2)}/{agg.cpu_budget.toFixed(1)}c ·{" "}
                RAM {agg.ram_used.toFixed(2)}/{agg.ram_budget.toFixed(1)}gb
                {agg.frames_per_sec > 0 && <> · {agg.frames_per_sec.toFixed(1)} fr/s</>}
                {agg.source === "backbone" && (
                  <span style={{ color: "#e3b341" }}> · backbone</span>
                )}
              </div>
            )}
          </div>
        ),
      },
      style: {
        background: "transparent",
        border: "none",
        color: meshColor(m),
        fontFamily: "ui-monospace, monospace",
        width: 180,
      },
      draggable: false,
      selectable: false,
    });

    // Lay members in a circle inside their mesh group
    const list = byMesh.get(m)!;
    const cx = meshWidth / 2;
    const cy = meshHeight / 2;
    const r = Math.min(meshWidth, meshHeight) * 0.32;
    const NODE_W = 100;
    const NODE_H = 86;
    list.forEach((n, idx) => {
      const ang = (2 * Math.PI * idx) / Math.max(1, list.length) - Math.PI / 2;
      const color = nodeTypeColor(n.type);
      // sprint-20: the halo/ring is the node's STATE; the fill stays the type
      // color. Alive → neutral (ring = type color). A non-Alive state rings the
      // node in its state color + adds a glow so it's unmistakable.
      const sColor = stateColor(n.state);
      const ringColor = sColor || color;
      const stateLabel = n.state && n.state !== "Alive" ? n.state : "";
      nodes.push({
        id: n.id,
        parentNode: `group-${m}`,
        extent: "parent",
        position: {
          x: cx + r * Math.cos(ang) - NODE_W / 2,
          y: cy + r * Math.sin(ang) - NODE_H / 2,
        },
        data: {
          label: (
            <div style={{ textAlign: "center", lineHeight: 1.15 }}>
              <div
                style={{
                  fontFamily: "ui-monospace, monospace",
                  fontSize: 10,
                  color: "#c9d1d9",
                }}
              >
                {n.id.length > 14 ? n.id.slice(0, 12) + "…" : n.id}
              </div>
              <div style={{ fontSize: 9, color: "#8b949e", marginTop: 2 }}>
                {n.type}
              </div>
              {stateLabel && (
                <div style={{ fontSize: 9, fontWeight: 700, color: ringColor }}>
                  {stateLabel}
                </div>
              )}
              {(n.frames_sent_total ?? 0) > 0 && (
                <div style={{ fontSize: 9, color: "#3fb950" }}>
                  TX:{n.frames_sent_total}
                </div>
              )}
              {(n.frames_recv_total ?? 0) > 0 && (
                <div style={{ fontSize: 9, color: "#58a6ff" }}>
                  RX:{n.frames_recv_total}
                </div>
              )}
              {(n.cpu_budget ?? 0) > 0 && (
                <div style={{ fontSize: 9, color: utilColor(n.cpu_used, n.cpu_budget) }}>
                  CPU:{(n.cpu_used ?? 0).toFixed(2)}/{(n.cpu_budget ?? 0).toFixed(1)}
                </div>
              )}
              {(n.ram_budget ?? 0) > 0 && (
                <div style={{ fontSize: 9, color: utilColor(n.ram_used, n.ram_budget) }}>
                  MEM:{(n.ram_used ?? 0).toFixed(2)}/{(n.ram_budget ?? 0).toFixed(2)}gb
                </div>
              )}
            </div>
          ),
        },
        style: {
          background: `${color}33`,
          border: `2px solid ${ringColor}`,
          boxShadow: stateLabel ? `0 0 8px 1px ${ringColor}` : "none",
          color: "#fff",
          width: NODE_W,
          height: NODE_H,
          borderRadius: 8,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        },
      });
    });
  });

  // Edges: within-mesh (dim gray) and cross-mesh (gold dashed, animated).
  // Drop edges that reference the filtered-out observer node (admin-ui).
  const visibleNodeIds = new Set(observable.map((n) => n.id));
  const edges: Edge[] = t.edges
    .filter((e) => visibleNodeIds.has(e.from) && visibleNodeIds.has(e.to))
    .map((e) => {
      const isCross = e.kind === "cross";
      return {
        id: `${e.from}->${e.to}`,
        source: e.from,
        target: e.to,
        style: isCross
          ? { stroke: "#e3b341", strokeWidth: 1.5, strokeDasharray: "5,4" }
          : { stroke: "#30363d", strokeWidth: 1, opacity: 0.4 },
        animated: isCross,
      };
    });

  return { nodes, edges };
}

export function Topology() {
  const [data, setData] = useState<TopologyResponse>({ nodes: [], edges: [] });
  const [err, setErr] = useState<string | null>(null);

  const refresh = useCallback(() => {
    api.topology()
      .then((d) => { setData(d); setErr(null); })
      .catch((e) => setErr(e.message));
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 2000);
    return () => clearInterval(id);
  }, [refresh]);

  const { nodes, edges } = useMemo(() => buildGraph(data), [data]);

  if (err) {
    return <div className="card">topology load failed: {err}</div>;
  }
  if (data.nodes.length === 0) {
    return (
      <div className="card">
        <div className="muted">
          no nodes yet — click <b>bootstrap 2-mesh</b> above to spawn the full
          topology, or use the individual + buttons.
        </div>
      </div>
    );
  }

  return (
    <div style={{ height: "calc(100vh - 220px)", border: "1px solid var(--border)", borderRadius: 6 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        nodesDraggable
        nodesConnectable={false}
        elementsSelectable={false}
        proOptions={{ hideAttribution: true }}
      >
        <Background gap={20} color="#161b22" />
        <Controls showInteractive={false} />
        <MiniMap pannable zoomable maskColor="rgba(13,17,23,0.85)" />
      </ReactFlow>
    </div>
  );
}
