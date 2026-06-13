import { useCallback, useEffect, useMemo, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  type Node,
} from "reactflow";
import { api, type TopologyNodeCacheResponse, type NodeType } from "../api";

const TYPE_COLOR: Record<NodeType, string> = {
  gateway: "#58a6ff",
  broker: "#f0883e",
  compute: "#3fb950",
  registry: "#bc8cff",
  "node-admin": "#8b949e",
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

function buildGraph(t: TopologyNodeCacheResponse): { nodes: Node[] } {
  // entity-cache nodes only — no edges (peer_ids are not in the cache)
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

    // Mesh label — no mesh aggregate in the cache; show mesh name + count only.
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
      // node_type from cache; fall back to grey if unknown
      const color = nodeTypeColor(n.node_type);
      // state halo — same coloring as the gossip topology
      const sColor = stateColor(n.state);
      const ringColor = sColor || color;
      const stateLabel = n.state && n.state !== "Alive" ? n.state : "";
      nodes.push({
        id: n.node_id,
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
                {n.node_name.length > 14 ? n.node_name.slice(0, 12) + "…" : n.node_name}
              </div>
              <div style={{ fontSize: 9, color: "#8b949e", marginTop: 2 }}>
                {n.node_type}
              </div>
              {stateLabel && (
                <div style={{ fontSize: 9, fontWeight: 700, color: ringColor }}>
                  {stateLabel}
                </div>
              )}
              {n.stateful && (
                <div style={{ fontSize: 8, fontWeight: 700, color: "#f0a000", background: "#2d2000", borderRadius: 3, padding: "0 3px", display: "inline-block", marginTop: 1 }}>
                  Stateful
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

  // No edges — the entity-cache has no peer_ids (it's data-plane topology,
  // not gossip-overlay edges). The original Topology tab carries those.
  return { nodes };
}

export function TopologyCache() {
  const [data, setData] = useState<TopologyNodeCacheResponse>({ nodes: [] });
  const [err, setErr] = useState<string | null>(null);

  const refresh = useCallback(() => {
    api.topologyNode()
      .then((d) => { setData(d); setErr(null); })
      .catch((e) => setErr(e.message));
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 2000);
    return () => clearInterval(id);
  }, [refresh]);

  const { nodes } = useMemo(() => buildGraph(data), [data]);

  if (err) {
    return <div className="card">topology-cache load failed: {err}</div>;
  }
  if (data.nodes.length === 0) {
    return (
      <div className="card">
        <div className="muted">
          no nodes yet — waiting for entity-cache to populate.
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "calc(100vh - 220px)" }}>
      <div style={{ fontSize: 11, color: "#8b949e", padding: "4px 8px", fontFamily: "ui-monospace, monospace" }}>
        source: entity-cache (/api/topology/node) — nodes only, edges are control-plane
      </div>
      <div style={{ flex: 1, border: "1px solid var(--border)", borderRadius: 6 }}>
        <ReactFlow
          nodes={nodes}
          edges={[]}
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
    </div>
  );
}
