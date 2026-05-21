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
  bridge: "#e3b341",
};

function meshColor(mesh: string): string {
  if (mesh === "mesh-a") return "#bc8cff";
  if (mesh === "mesh-b") return "#f0883e";
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

function buildGraph(t: TopologyResponse): { nodes: Node[]; edges: Edge[] } {
  // The admin-ui process registers itself with mesh_id="admin" (see
  // admin-ui/src/main.rs ~3842). It's an observer, not a mesh participant —
  // showing it as its own swim lane both clutters the view and breaks bridge
  // centering (with 3 sorted meshes [admin, mesh-a, mesh-b], the row center
  // lands exactly on mesh-a, so bridges render on top of the wrong group).
  const observable = t.nodes.filter((n) => (n.mesh_id || "default") !== "admin");
  const bridges = observable.filter((n) => n.type === "bridge");
  const members = observable.filter((n) => n.type !== "bridge");

  const byMesh = new Map<string, typeof members>();
  for (const n of members) {
    const m = n.mesh_id || "default";
    if (!byMesh.has(m)) byMesh.set(m, []);
    byMesh.get(m)!.push(n);
  }
  const meshes = Array.from(byMesh.keys()).sort();

  const nodes: Node[] = [];
  const meshGap = 480; // px between mesh group centers
  const meshTop = 60;
  const meshWidth = 380;
  const meshHeight = 380;

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

    // Mesh label
    nodes.push({
      id: `label-${m}`,
      type: "default",
      position: { x: 80 + i * meshGap + meshWidth / 2 - 60, y: meshTop - 30 },
      data: { label: `${m} · ${byMesh.get(m)!.length} nodes` },
      style: {
        background: "transparent",
        border: "none",
        color: meshColor(m),
        fontFamily: "ui-monospace, monospace",
        fontWeight: 600,
        fontSize: 13,
        width: 120,
      },
      draggable: false,
      selectable: false,
    });

    // Lay members in a circle inside their mesh group
    const list = byMesh.get(m)!;
    const cx = meshWidth / 2;
    const cy = meshHeight / 2;
    const r = Math.min(meshWidth, meshHeight) * 0.35;
    list.forEach((n, idx) => {
      const ang = (2 * Math.PI * idx) / Math.max(1, list.length) - Math.PI / 2;
      nodes.push({
        id: n.id,
        parentNode: `group-${m}`,
        extent: "parent",
        position: { x: cx + r * Math.cos(ang) - 35, y: cy + r * Math.sin(ang) - 35 },
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
                  CPU:{(n.cpu_used ?? 0).toFixed(1)}/{(n.cpu_budget ?? 0).toFixed(1)}
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
          background: `${TYPE_COLOR[n.type]}33`,
          border: `2px solid ${TYPE_COLOR[n.type]}`,
          color: "#fff",
          width: 70,
          height: 70,
          borderRadius: "50%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        },
      });
    });
  });

  // Bridge nodes — placed ABOVE the mesh groups, centered horizontally
  // across the row of meshes. Bridges visibly sit "on top of" everything
  // they bridge, not jammed in the gap between meshes.
  bridges.forEach((b, i) => {
    const centerOfAllMeshes =
      meshes.length > 0
        ? 80 + (meshes.length - 1) * meshGap * 0.5 + meshWidth / 2
        : 400;
    // spread multiple bridges horizontally around the center
    const spread = 100;
    const x = centerOfAllMeshes - 35 + (i - (bridges.length - 1) / 2) * spread;
    // y above the mesh group tops with some padding
    const y = Math.max(20, meshTop - 110);

    nodes.push({
      id: b.id,
      position: { x, y },
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
              {b.id.length > 14 ? b.id.slice(0, 12) + "…" : b.id}
            </div>
            <div style={{ fontSize: 9, color: "#e3b341", marginTop: 2 }}>
              bridge
            </div>
          </div>
        ),
      },
      style: {
        background: `${TYPE_COLOR.bridge}33`,
        border: `2px solid ${TYPE_COLOR.bridge}`,
        color: "#fff",
        width: 80,
        height: 80,
        borderRadius: 8,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      },
    });
  });

  // Edges: server emits within-mesh full clique + bridge→anchor cross edges.
  // Render within = dim gray (architectural, not animated). Cross = gold
  // dashed + animated (visually distinguishes the bridge link).
  // Drop edges that reference the filtered-out observer node (admin-ui),
  // otherwise React Flow renders them with missing endpoints.
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
