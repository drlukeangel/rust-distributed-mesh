import { useEffect, useState } from "react";
import { api, type Heartbeat as HB } from "../api";

const TYPE_COLOR: Record<string, string> = {
  gateway: "#58a6ff",
  broker: "#f0883e",
  compute: "#3fb950",
  registry: "#bc8cff",
  bridge: "#e3b341",
};

export function Heartbeat() {
  const [hbs, setHbs] = useState<HB[]>([]);
  const [busy, setBusy] = useState<string | null>(null);

  const load = () =>
    api.heartbeats().then((r) => setHbs(r.heartbeats)).catch(() => {});
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
    } finally {
      setBusy(null);
    }
  };

  if (hbs.length === 0) {
    return <div className="card muted">no heartbeats — spawn or bootstrap first</div>;
  }

  return (
    <div className="grid grid-cards">
      {hbs.map((h) => (
        <div key={h.node_name} className="card" style={{ position: "relative" }}>
          <button
            className="danger"
            disabled={busy === h.node_name}
            onClick={() => doKill(h.node_name)}
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
              color: TYPE_COLOR[h.node_type] || "#fff",
              fontWeight: 600,
              marginBottom: 4,
            }}
          >
            {h.node_name}
          </div>
          <div className="muted mono" style={{ fontSize: 11 }}>
            type: {h.node_type}<br />
            mesh: {h.mesh_id || "?"}<br />
            peers: {h.peer_count}<br />
            age: {(h.age_ms / 1000).toFixed(1)}s
          </div>
        </div>
      ))}
    </div>
  );
}
