import { useEffect, useState } from "react";
import { api, type Timeline as T } from "../api";

const colourOf = (name: string): string => {
  if (name.startsWith("ui.chaos")) return "var(--err)";
  if (name.includes("status") || name.includes("seat") || name.includes("election")) return "var(--warn)";
  if (name.includes("build") || name.includes("deployment")) return "var(--accent)";
  if (name.includes("connection")) return "var(--ok)";
  return "var(--fg-dim)";
};

export function Timeline() {
  const [all, setAll] = useState(false);
  const [t, setT] = useState<T | null>(null);
  useEffect(() => {
    const load = () => api.timeline(all).then(setT).catch(() => {});
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, [all]);

  return (
    <div>
      <div className="row" style={{ marginBottom: 8 }}>
        <label className="muted">
          <input type="checkbox" checked={all} onChange={(e) => setAll(e.target.checked)} /> include high-volume events
          {t ? ` (${t.hidden_high_volume} hidden)` : ""}
        </label>
        <span className="muted mono">{t ? `${t.shown} events from ${t.files} span files, newest first` : ""}</span>
      </div>
      {!t || t.events.length === 0 ? (
        <div className="card muted">no events read yet from the nodes' span files</div>
      ) : (
        <div className="card" data-testid="timeline">
          {t.events.map((e, i) => (
            <div key={i} className="row" style={{ padding: "3px 0", borderBottom: "1px solid var(--border)", fontSize: 12 }}>
              <span className="muted mono" style={{ width: 100, flexShrink: 0 }}>
                {new Date(e.ts_ms).toLocaleTimeString([], { hour12: false })}.{String(e.ts_ms % 1000).padStart(3, "0")}
              </span>
              <span className="mono" style={{ width: 130, flexShrink: 0 }}>{e.node}</span>
              <span className="mono" style={{ width: 330, flexShrink: 0, color: colourOf(e.name), overflow: "hidden", textOverflow: "ellipsis" }}>{e.name}</span>
              <span className="mono muted" style={{ flex: 1 }}>{e.summary}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
