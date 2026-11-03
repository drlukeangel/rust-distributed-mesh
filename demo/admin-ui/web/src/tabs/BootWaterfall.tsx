import { useEffect, useState } from "react";
import { api, comparePathName, pathNameText, type BootSpan, type TopologyNode } from "../api";

export function BootWaterfall() {
  const [nodes, setNodes] = useState<TopologyNode[]>([]);
  /// The picked node's id.
  const [pick, setPick] = useState("");
  const [spans, setSpans] = useState<BootSpan[]>([]);
  const [note, setNote] = useState("");

  useEffect(() => {
    api.topology().then((r) => {
      const sorted = [...r.nodes].sort((a, b) => comparePathName(a, b));
      setNodes(sorted);
      if (!pick && sorted[0]) setPick(sorted[0].node_id);
    }).catch(() => {});
  }, [pick]);

  useEffect(() => {
    if (!pick) return;
    const node = nodes.find((n) => n.node_id === pick);
    const label = node ? pathNameText(node) : `node ${pick}`;
    api.bootWaterfall(pick, node?.kind ?? "").then((r) => {
      const raw = r.data?.[0]?.spans ?? [];
      const parsed = raw.map((s) => ({ name: s.operationName, start_us: s.startTime, duration_ms: s.duration / 1000 })).sort((a, b) => a.start_us - b.start_us);
      setSpans(parsed);
      setNote(parsed.length ? "" : `Jaeger holds no boot trace for ${label}`);
    }).catch((e) => { setSpans([]); setNote(`Jaeger query failed: ${String(e)}`); });
  }, [pick, nodes]);

  if (nodes.length === 0) return <div className="card muted">no nodes yet</div>;
  const t0 = spans[0]?.start_us ?? 0;
  const total = spans.length ? Math.max(...spans.map((s) => s.start_us + s.duration_ms * 1000)) - t0 : 1;
  return (
    <div>
      <div className="row" style={{ marginBottom: 12 }}>
        <label className="muted">node:</label>
        <select value={pick} onChange={(e) => setPick(e.target.value)}>
          {nodes.map((n) => <option key={n.node_id} value={n.node_id}>{pathNameText(n)}</option>)}
        </select>
        {note && <span className="muted mono">{note}</span>}
      </div>
      {spans.length > 0 && (
        <div className="card">
          {spans.map((s, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4, fontSize: 11 }}>
              <span className="mono muted" style={{ width: 300, overflow: "hidden", textOverflow: "ellipsis" }}>{s.name}</span>
              <div style={{ position: "relative", flex: 1, height: 14, background: "var(--bg)", borderRadius: 2 }}>
                <div style={{ position: "absolute", left: `${((s.start_us - t0) / total) * 100}%`, width: `${Math.max(0.5, ((s.duration_ms * 1000) / total) * 100)}%`, top: 0, bottom: 0, background: "var(--accent)", opacity: 0.7, borderRadius: 2 }} />
              </div>
              <span className="mono muted" style={{ width: 70, textAlign: "right" }}>{s.duration_ms.toFixed(1)} ms</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
