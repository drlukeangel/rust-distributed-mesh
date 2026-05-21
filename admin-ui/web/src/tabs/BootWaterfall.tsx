import { useEffect, useState } from "react";
import { api, type BootSpan, type TopologyNode } from "../api";

export function BootWaterfall() {
  const [nodes, setNodes] = useState<TopologyNode[]>([]);
  const [pick, setPick] = useState<string>("");
  const [spans, setSpans] = useState<BootSpan[]>([]);

  // Use /api/topology (local spawned_meta) instead of /api/heartbeats so the
  // node dropdown populates instantly after bootstrap, before Jaeger has
  // ingested any heartbeat spans.
  useEffect(() => {
    const load = () =>
      api.topology()
        .then((r) => {
          const sorted = [...r.nodes].sort((a, b) =>
            (a.mesh_id + a.id).localeCompare(b.mesh_id + b.id),
          );
          setNodes(sorted);
          if (!pick && sorted[0]) setPick(sorted[0].id);
        })
        .catch(() => {});
    load();
    const id = setInterval(load, 3000);
    return () => clearInterval(id);
  }, [pick]);

  useEffect(() => {
    if (!pick) return;
    api
      .bootWaterfall(pick)
      .then((r) => {
        // Jaeger raw format → sorted BootSpan rows
        const raw = r.data?.[0]?.spans ?? [];
        const parsed: BootSpan[] = raw
          .map((s) => ({
            name: s.operationName,
            start_us: s.startTime,
            duration_ms: s.duration / 1000,
          }))
          .sort((a, b) => a.start_us - b.start_us);
        setSpans(parsed);
      })
      .catch(() => setSpans([]));
  }, [pick]);

  if (nodes.length === 0) {
    return <div className="card muted">no nodes — bootstrap or spawn first</div>;
  }

  const t0 = spans[0]?.start_us ?? 0;
  const totalUs = spans.length
    ? Math.max(...spans.map((s) => s.start_us + s.duration_ms * 1000)) - t0
    : 1;

  return (
    <div>
      <div className="row" style={{ marginBottom: 12 }}>
        <label className="muted">node:</label>
        <select value={pick} onChange={(e) => setPick(e.target.value)}>
          {nodes.map((n) => (
            <option key={n.id} value={n.id}>
              {n.mesh_id || "?"} : {n.id}
            </option>
          ))}
        </select>
        {spans.length === 0 && (
          <span className="muted mono" style={{ fontSize: 11 }}>
            waiting on Jaeger ingestion for {pick}…
          </span>
        )}
      </div>
      {spans.length === 0 ? (
        <div className="card muted">no boot spans for {pick}</div>
      ) : (
        <div className="card">
          {spans.map((s) => {
            const left = ((s.start_us - t0) / totalUs) * 100;
            const width = Math.max(0.5, ((s.duration_ms * 1000) / totalUs) * 100);
            return (
              <div
                key={s.name + s.start_us}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  marginBottom: 4,
                  fontSize: 11,
                }}
              >
                <span
                  className="mono muted"
                  style={{ width: 260, overflow: "hidden", textOverflow: "ellipsis" }}
                >
                  {s.name}
                </span>
                <div
                  style={{
                    position: "relative",
                    flex: 1,
                    height: 14,
                    background: "var(--bg)",
                    borderRadius: 2,
                  }}
                >
                  <div
                    style={{
                      position: "absolute",
                      left: `${left}%`,
                      width: `${width}%`,
                      top: 0,
                      bottom: 0,
                      background: "var(--accent)",
                      opacity: 0.7,
                      borderRadius: 2,
                    }}
                  />
                </div>
                <span className="mono muted" style={{ width: 70, textAlign: "right" }}>
                  {s.duration_ms.toFixed(1)} ms
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
