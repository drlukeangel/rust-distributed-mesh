import { useEffect, useState } from "react";
import { api, type TimelineEvent } from "../api";

const KIND_COLOR: Record<string, string> = {
  "node.ready": "var(--ok)",
  "node.spawn": "var(--accent)",
  "node.killed": "var(--err)",
  "peer.connected": "var(--ok)",
  "peer.disconnected": "var(--warn)",
  "chaos.kill": "var(--err)",
  "chaos.respawn": "var(--accent)",
};

export function Timeline() {
  const [events, setEvents] = useState<TimelineEvent[]>([]);
  useEffect(() => {
    const load = () =>
      api.timeline().then((r) => setEvents(r.events)).catch(() => {});
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);

  if (events.length === 0) {
    return <div className="card muted">no events yet</div>;
  }

  return (
    <div className="card">
      {events.map((e, i) => (
        <div
          key={i}
          className="row"
          style={{
            padding: "4px 0",
            borderBottom: "1px solid var(--border)",
            fontSize: 12,
          }}
        >
          <span className="muted mono" style={{ width: 110 }}>
            {new Date(e.ts_us / 1000).toLocaleTimeString()}
          </span>
          <span
            className="pill"
            style={{
              borderColor: KIND_COLOR[e.kind] || "var(--fg-dim)",
              color: KIND_COLOR[e.kind] || "var(--fg-dim)",
              minWidth: 110,
              textAlign: "center",
            }}
          >
            {e.kind}
          </span>
          <span className="mono" style={{ flex: 1 }}>
            {e.node_name || "—"}
            {e.mesh_id && (
              <span className="muted"> ({e.mesh_id})</span>
            )}
          </span>
          {e.detail && <span className="muted mono">{e.detail}</span>}
        </div>
      ))}
    </div>
  );
}
