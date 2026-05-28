import { useEffect, useMemo, useState } from "react";
import { api, type TopologyNode, type NodeType } from "../api";

const TYPE_COLOR: Record<string, string> = {
  gateway: "#58a6ff",
  broker: "#f0883e",
  compute: "#3fb950",
  registry: "#bc8cff",
  bridge: "#e3b341",
};

function utilColor(used: number | undefined, budget: number | undefined): string {
  if (used === undefined || budget === undefined || budget <= 0) return "#8b949e";
  const ratio = used / budget;
  if (ratio < 0.5) return "#3fb950";
  if (ratio < 0.8) return "#d29922";
  return "#f85149";
}

/// Compute "age" (node lifetime) preferring spawn_time_ms over wall_time_ms.
/// spawn_time_ms is the admin-ui-recorded spawn timestamp — monotonic, the
/// thing operators expect for "how long has this node been alive."
/// wall_time_ms is the per-digest emit time — bounces with gossip cadence,
/// only useful as a fallback for nodes admin-ui didn't spawn itself.
function ageSeconds(spawn_time_ms: number | undefined, wall_time_ms: number | undefined): string {
  const base = spawn_time_ms ?? wall_time_ms;
  if (!base) return "?";
  const ageMs = Date.now() - base;
  if (ageMs < 0) return "0.0s";
  if (ageMs < 60_000) return `${(ageMs / 1000).toFixed(1)}s`;
  if (ageMs < 3_600_000) return `${(ageMs / 60_000).toFixed(1)}m`;
  return `${(ageMs / 3_600_000).toFixed(1)}h`;
}

interface UtilBarProps {
  label: string;
  used: number;
  budget: number;
  unit: string;
}
function UtilBar({ label, used, budget, unit }: UtilBarProps) {
  const pct = budget > 0 ? Math.min(100, (used / budget) * 100) : 0;
  const color = utilColor(used, budget);
  return (
    <div style={{ margin: "6px 0" }}>
      <div
        className="mono"
        style={{
          fontSize: 11,
          color: "#c9d1d9",
          display: "flex",
          justifyContent: "space-between",
        }}
      >
        <span>{label}</span>
        <span style={{ color }}>
          {used.toFixed(2)} / {budget.toFixed(2)} {unit} ({pct.toFixed(0)}%)
        </span>
      </div>
      <div
        style={{
          height: 6,
          background: "#161b22",
          borderRadius: 3,
          overflow: "hidden",
          marginTop: 2,
        }}
      >
        <div
          style={{
            width: `${pct}%`,
            height: "100%",
            background: color,
            transition: "width 200ms linear",
          }}
        />
      </div>
    </div>
  );
}

export function Nodes() {
  const [nodes, setNodes] = useState<TopologyNode[]>([]);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const load = () =>
    api.topology()
      .then((r) => setNodes(r.nodes))
      .catch(() => {});
  useEffect(() => {
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);

  // Build hex node_id → friendly node_name map for resolving peer_ids.
  const idToName = useMemo(() => {
    const m = new Map<string, string>();
    for (const n of nodes) {
      if (n.node_id) m.set(n.node_id, n.id);
    }
    return m;
  }, [nodes]);

  const doKill = async (name: string) => {
    setBusy(name);
    try {
      await api.kill(name);
      await load();
      if (expanded === name) setExpanded(null);
    } finally {
      setBusy(null);
    }
  };

  if (nodes.length === 0) {
    return <div className="card muted">no nodes — spawn or bootstrap first</div>;
  }

  // Sort nodes: by mesh, then by type, then by name. Keeps the layout
  // stable across refreshes so the row you clicked stays where it was.
  const sorted = [...nodes].sort((a, b) => {
    if (a.mesh_id !== b.mesh_id) return a.mesh_id.localeCompare(b.mesh_id);
    if (a.type !== b.type) return a.type.localeCompare(b.type);
    return a.id.localeCompare(b.id);
  });

  return (
    <div className="grid grid-cards">
      {sorted.map((n) => {
        const isOpen = expanded === n.id;
        const typeColor = TYPE_COLOR[n.type] || "#fff";
        return (
          <div
            key={n.id}
            className="card"
            style={{
              position: "relative",
              cursor: "pointer",
              borderColor: isOpen ? typeColor : undefined,
              borderWidth: isOpen ? 2 : undefined,
              gridColumn: isOpen ? "1 / -1" : undefined,
            }}
            onClick={() => setExpanded(isOpen ? null : n.id)}
          >
            <button
              className="danger"
              disabled={busy === n.id}
              onClick={(e) => {
                e.stopPropagation();
                doKill(n.id);
              }}
              style={{
                position: "absolute",
                top: 8,
                right: 8,
                fontSize: 10,
                padding: "2px 8px",
              }}
            >
              kill
            </button>

            <div
              className="mono"
              style={{
                color: typeColor,
                fontWeight: 600,
                marginBottom: 4,
              }}
            >
              {n.id}
            </div>

            <div className="muted mono" style={{ fontSize: 11 }}>
              type: {n.type}
              <br />
              mesh: {n.mesh_id || "?"}
              <br />
              peers: {n.peer_count ?? 0}
              <br />
              age: {ageSeconds(n.spawn_time_ms, n.wall_time_ms)}
              <br />
              status: {n.status ?? "?"}
            </div>

            {(n.cpu_budget ?? 0) > 0 && (
              <UtilBar
                label="CPU"
                used={n.cpu_used ?? 0}
                budget={n.cpu_budget ?? 0}
                unit="cores"
              />
            )}
            {(n.ram_budget ?? 0) > 0 && (
              <UtilBar
                label="RAM"
                used={n.ram_used ?? 0}
                budget={n.ram_budget ?? 0}
                unit="gb"
              />
            )}

            {isOpen && (
              <div
                style={{
                  marginTop: 12,
                  paddingTop: 10,
                  borderTop: "1px solid #30363d",
                }}
              >
                <div
                  className="mono muted"
                  style={{ fontSize: 10, marginBottom: 6 }}
                >
                  node_id: {n.node_id || "(pending)"}
                </div>

                <div
                  className="mono"
                  style={{
                    fontSize: 11,
                    marginTop: 10,
                    display: "grid",
                    gridTemplateColumns: "auto 1fr",
                    gap: "2px 12px",
                  }}
                >
                  <span className="muted">frames TX:</span>
                  <span style={{ color: "#3fb950" }}>
                    {n.frames_sent_total ?? 0}
                  </span>
                  <span className="muted">frames RX:</span>
                  <span style={{ color: "#58a6ff" }}>
                    {n.frames_recv_total ?? 0}
                  </span>
                </div>

                <div style={{ marginTop: 10 }}>
                  <div
                    className="mono muted"
                    style={{ fontSize: 10, marginBottom: 4 }}
                  >
                    peers ({(n.peer_ids ?? []).length}):
                  </div>
                  {(n.peer_ids ?? []).length === 0 ? (
                    <div className="muted mono" style={{ fontSize: 10 }}>
                      no peer connections reported
                    </div>
                  ) : (
                    <div
                      className="mono"
                      style={{
                        fontSize: 10,
                        display: "grid",
                        gridTemplateColumns:
                          "repeat(auto-fill, minmax(180px, 1fr))",
                        gap: 2,
                      }}
                    >
                      {(n.peer_ids ?? []).map((pid) => {
                        const name = idToName.get(pid);
                        const peer = name
                          ? nodes.find((x) => x.id === name)
                          : undefined;
                        const peerColor: NodeType | undefined = peer?.type;
                        return (
                          <div
                            key={pid}
                            style={{
                              padding: "2px 6px",
                              background: "#161b22",
                              borderRadius: 3,
                              borderLeft: peerColor
                                ? `3px solid ${TYPE_COLOR[peerColor]}`
                                : "3px solid #30363d",
                              color: name ? "#c9d1d9" : "#6e7681",
                            }}
                            title={pid}
                          >
                            {name ?? pid.slice(0, 12) + "…"}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
