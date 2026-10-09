import { useEffect, useState } from "react";
import { api, type MessageRow } from "../api";

const kindColour = (k: string) => (k === "rpc" ? "#58a6ff" : "#bc8cff");

/// The live feed of what the nodes say to each other, read from their own span records: Node RPC
/// calls and serves, and the gossip channels' seat, forwarded and backbone activity.
export function Messages() {
  const [rows, setRows] = useState<MessageRow[]>([]);
  const [kind, setKind] = useState("all");
  const [q, setQ] = useState("");
  const [paused, setPaused] = useState(false);
  const [err, setErr] = useState("");
  useEffect(() => {
    if (paused) return;
    const load = () => api.messages(kind, q).then((r) => { setRows(r.messages); setErr(""); }).catch((e) => setErr(String(e)));
    load();
    const id = setInterval(load, 1500);
    return () => clearInterval(id);
  }, [kind, q, paused]);
  return (
    <div>
      <div className="row" style={{ marginBottom: 10 }}>
        <label className="muted">feed:</label>
        <select value={kind} onChange={(e) => setKind(e.target.value)} data-testid="msg-kind">
          <option value="all">all</option>
          <option value="rpc">Node RPC</option>
          <option value="gossip">gossip</option>
        </select>
        <input placeholder="filter (node, op, outcome…)" value={q} onChange={(e) => setQ(e.target.value)} style={{ width: 260 }} data-testid="msg-filter" />
        <button onClick={() => setPaused(!paused)}>{paused ? "resume" : "pause"}</button>
        <span className="muted mono" data-testid="msg-count">{rows.length} shown{err ? ` · ${err}` : ""}</span>
      </div>
      {rows.length === 0 ? <div className="card muted">no messages match</div> : (
        <div className="card" style={{ padding: 0 }}>
          {rows.map((m, i) => (
            <div key={i} className="mono" style={{ display: "flex", gap: 10, fontSize: 11, padding: "3px 10px", borderBottom: "1px solid var(--border)" }} data-testid="msg-row">
              <span className="muted" style={{ width: 82 }}>{new Date(m.ts_ms).toLocaleTimeString()}</span>
              <span style={{ width: 52, color: kindColour(m.kind) }}>{m.kind}</span>
              <span style={{ width: 330, whiteSpace: "nowrap" }}>{m.from} → {m.to}</span>
              <span style={{ width: 70 }}>{m.op ? `op ${m.op}` : ""}</span>
              <span style={{ width: 90 }}>{m.protocol}</span>
              <span style={{ width: 150, color: m.outcome === "Reply" || m.outcome === "held" ? "var(--ok)" : m.outcome ? "var(--warn)" : undefined }}>{m.outcome}</span>
              <span style={{ width: 60 }}>{m.elapsed_ms ? `${m.elapsed_ms} ms` : ""}</span>
              <span className="muted" style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{m.span.replace("rdm.", "")} {m.detail}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
