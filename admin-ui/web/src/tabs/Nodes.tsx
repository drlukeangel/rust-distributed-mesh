import { useEffect, useState } from "react";
import { api, type TopologyNode } from "../api";

const TYPE_COLOR: Record<string, string> = {
  gateway: "#58a6ff",
  broker: "#f0883e",
  compute: "#3fb950",
  registry: "#bc8cff",
  rpc_node: "#8b949e",
  node_admin: "#e3b341",
};

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
              status: {n.status ?? "?"}
              <br />
              seat: {n.is_fabric_primary ? "fabric primary" : n.is_primary ? "mesh primary" : "member"}
            </div>

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
                  <span className="muted">incarnation:</span>
                  <span style={{ color: "#3fb950" }}>{n.incarnation_id ?? "(none)"}</span>
                  <span className="muted">declared:</span>
                  <span style={{ color: "#58a6ff" }}>{n.declared ?? "(none)"}</span>
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
