import { useEffect, useState } from "react";
import { api, type BuildView, type Builds as B } from "../api";

const outcomeText = (o: unknown) => {
  if (o === "complete") return "complete";
  if (o && typeof o === "object" && "failed" in o) return `failed: ${JSON.stringify((o as { failed: unknown }).failed)}`;
  return JSON.stringify(o);
};
const stateColor = (s: string) => (s === "complete" || s === "converged" ? "var(--ok)" : s === "failed" ? "var(--err)" : "var(--warn)");

function BuildCard({ b, current }: { b: BuildView; current: boolean }) {
  const attempts = Array.from(new Set(b.steps.map((s) => s.attempt))).sort((x, y) => x - y);
  return (
    <div className="card" style={{ marginBottom: 12 }} data-testid={`build-${b.build_id}`}>
      <div className="row" style={{ marginBottom: 6 }}>
        <span className="mono" style={{ fontWeight: 600 }}>{b.build_id}</span>
        <span className="pill" style={{ borderColor: stateColor(b.state), color: stateColor(b.state) }}>{b.state}</span>
        {current && <span className="pill" style={{ borderColor: "var(--accent)", color: "var(--accent)" }}>accepted topology</span>}
        <span className="muted mono">{b.submitted_change?.kind ?? "—"} · reason {b.reason} · attempt {b.attempt} · executor {b.executor ?? "—"}</span>
        <span className="muted mono">{new Date(b.submitted_at_ms).toLocaleTimeString()}</span>
      </div>
      {b.last_failure && <div style={{ color: "var(--err)" }} className="mono">last failure: {b.last_failure}</div>}
      {attempts.length === 0 && <div className="muted">no step receipts yet</div>}
      {attempts.map((a) => {
        const ops = Array.from(new Set(b.steps.filter((s) => s.attempt === a).map((s) => s.operation)));
        return (
          <div key={a} style={{ marginTop: 6 }}>
            <div className="muted mono">attempt {a} · {ops.length} operations</div>
            {ops.map((op) => {
              const steps = b.steps.filter((s) => s.attempt === a && s.operation === op);
              const failed = steps.filter((s) => s.outcome !== "complete");
              return (
                <details key={op} style={{ paddingLeft: 14 }}>
                  <summary className="mono" style={{ fontSize: 12 }}>
                    {op} · {steps.length} steps ·{" "}
                    <span style={{ color: failed.length ? "var(--err)" : "var(--ok)" }}>
                      {failed.length ? `${failed.length} failed` : "all complete"}
                    </span>
                  </summary>
                  {steps.map((s, i) => (
                    <div key={i} className="mono" style={{ fontSize: 11, paddingLeft: 16 }}>
                      {s.step} · <span style={{ color: s.outcome === "complete" ? "var(--ok)" : "var(--err)" }}>{outcomeText(s.outcome)}</span>
                    </div>
                  ))}
                </details>
              );
            })}
          </div>
        );
      })}
    </div>
  );
}

export function Builds() {
  const [d, setD] = useState<B | null>(null);
  const [err, setErr] = useState("");
  useEffect(() => {
    const load = () => api.builds().then((r) => { setD(r); setErr(""); }).catch((e) => setErr(String(e)));
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);
  if (err) return <div className="card">builds unreadable: {err}</div>;
  if (!d) return <div className="card muted">reading builds…</div>;
  if (d.builds.length === 0) return <div className="card muted">no Build is known to this UI yet</div>;
  return <div>{d.builds.map((b) => <BuildCard key={b.build_id} b={b} current={b.build_id === d.current} />)}</div>;
}
