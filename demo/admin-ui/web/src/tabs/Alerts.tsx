import { useEffect, useState } from "react";
import { api, type AlertItem } from "../api";

const SEV: Record<string, string> = { info: "var(--accent)", warn: "var(--warn)", error: "var(--err)" };

/// CPU and RAM threshold crossings, node state changes and every chaos action, newest first.
export function Alerts() {
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  useEffect(() => {
    const load = () => api.alerts().then((r) => setAlerts(r.alerts)).catch(() => {});
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);
  if (alerts.length === 0) return <div className="card muted" data-testid="no-alerts">no alerts raised yet</div>;
  return (
    <div className="grid">
      {alerts.map((a) => (
        <div key={a.id} className="card" style={{ borderLeft: `4px solid ${SEV[a.severity]}` }} data-testid="alert">
          <div className="row" style={{ justifyContent: "space-between", marginBottom: 4 }}>
            <span>
              <span className="pill" style={{ borderColor: SEV[a.severity], color: SEV[a.severity] }}>{a.severity}</span>{" "}
              <span className="pill">{a.kind}</span>
            </span>
            <span className="muted mono">{new Date(a.ts_ms).toLocaleTimeString()}</span>
          </div>
          <div className="mono">{a.message}</div>
          {(a.node || a.mesh) && <div className="muted mono" style={{ fontSize: 11, marginTop: 4 }}>{a.node && `node=${a.node} `}{a.mesh && `mesh=${a.mesh}`}</div>}
        </div>
      ))}
    </div>
  );
}
