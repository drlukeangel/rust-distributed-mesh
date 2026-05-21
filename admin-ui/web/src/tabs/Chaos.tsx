import { useEffect, useState } from "react";
import { api, type ChaosState } from "../api";

export function Chaos() {
  const [s, setS] = useState<ChaosState | null>(null);
  useEffect(() => {
    const load = () =>
      api.chaosState().then(setS).catch(() => setS(null));
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);

  if (!s) return <div className="card muted">chaos state unknown</div>;

  return (
    <div className="card">
      <div className="row" style={{ marginBottom: 8 }}>
        <span
          className="pill"
          style={{
            borderColor: s.running ? "var(--ok)" : "var(--fg-dim)",
            color: s.running ? "var(--ok)" : "var(--fg-dim)",
          }}
        >
          {s.running ? "RUNNING" : "STOPPED"}
        </span>
        <span className="muted mono">cadence: {s.cadence_ms} ms</span>
        <span className="muted mono">total events: {s.total_events}</span>
      </div>
      <div className="muted">
        Chaos kills a random non-bridge node every {Math.round(s.cadence_ms / 1000)}s
        and spawns a same-type replacement in the same mesh. Bridges are
        protected. Use the buttons in the top bar to toggle.
      </div>
    </div>
  );
}
