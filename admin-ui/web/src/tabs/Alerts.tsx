import { useEffect, useState } from "react";
import { api, type AlertItem } from "../api";

const SEV: Record<string, string> = {
  info: "var(--accent)",
  warn: "var(--warn)",
  error: "var(--err)",
};

export function Alerts() {
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  useEffect(() => {
    const load = () =>
      api.alerts().then((r) => setAlerts(r.alerts)).catch(() => {});
    load();
    const id = setInterval(load, 3000);
    return () => clearInterval(id);
  }, []);

  if (alerts.length === 0) {
    return <div className="card muted">no alerts — system healthy</div>;
  }

  return (
    <div className="grid">
      {alerts.map((a, i) => (
        <div
          key={i}
          className="card"
          style={{ borderLeft: `4px solid ${SEV[a.severity] ?? "var(--accent)"}` }}
        >
          <div className="row" style={{ justifyContent: "space-between", marginBottom: 4 }}>
            <span className="pill" style={{ borderColor: SEV[a.severity], color: SEV[a.severity] }}>
              {a.severity}
            </span>
            <span className="muted mono">{new Date(a.ts_us / 1000).toLocaleTimeString()}</span>
          </div>
          <div className="mono">{a.message}</div>
          {(a.node_name || a.mesh_id) && (
            <div className="muted mono" style={{ fontSize: 11, marginTop: 4 }}>
              {a.node_name && `node=${a.node_name} `}
              {a.mesh_id && `mesh=${a.mesh_id}`}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
