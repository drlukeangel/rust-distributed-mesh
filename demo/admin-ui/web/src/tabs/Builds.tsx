import { useEffect, useState } from "react";
import { api, type BuildEnded, type BuildView, type Builds as B } from "../api";

const outcomeText = (o: unknown) => {
  if (o === "complete") return "complete";
  if (o && typeof o === "object" && "failed" in o) return `failed: ${JSON.stringify((o as { failed: unknown }).failed)}`;
  return JSON.stringify(o);
};
/// The Build's end as its last `rdm.build.ended` says it (Q346): complete, incomplete naming each
/// section it did not finish, or failed: it could not start.
const endedText = (e: BuildEnded) => {
  if (e === null) return "not ended";
  if (e === "complete") return "complete";
  if ("incomplete" in e) return `incomplete: ${e.incomplete.sections.map((s) => `${s.mesh} ${s.op} ${s.step}: ${s.reason}`).join("; ")}`;
  return `failed: ${e.failed.reason}`;
};
const stateColor = (s: string) => (s === "complete" ? "var(--ok)" : s === "failed" || s === "incomplete" ? "var(--err)" : "var(--warn)");

function BuildCard({ b, current }: { b: BuildView; current: boolean }) {
  const meshes = Array.from(new Set([...b.sections, ...b.steps.map((s) => s.mesh)])).sort();
  return (
    <div className="card" style={{ marginBottom: 12 }} data-testid={`build-${b.build_id}`}>
      <div className="row" style={{ marginBottom: 6 }}>
        <span className="mono" style={{ fontWeight: 600 }}>{b.build_id}</span>
        <span className="pill" style={{ borderColor: stateColor(b.state), color: stateColor(b.state) }}>{b.state}</span>
        {current && <span className="pill" style={{ borderColor: "var(--accent)", color: "var(--accent)" }}>accepted topology</span>}
        <span className="muted mono">{b.nodes} nodes · {b.runs} runs · {b.steps.length} step events</span>
        <span className="muted mono">{b.time ? new Date(b.time).toLocaleTimeString() : "—"}</span>
      </div>
      {b.ended !== null && b.ended !== "complete" && <div style={{ color: "var(--err)" }} className="mono">{endedText(b.ended)}</div>}
      {meshes.length === 0 && <div className="muted">no section dispatched yet</div>}
      {meshes.map((mesh) => {
        const ops = Array.from(new Set(b.steps.filter((s) => s.mesh === mesh).map((s) => s.op)));
        return (
          <div key={mesh} style={{ marginTop: 6 }}>
            <div className="muted mono">section {mesh} · {ops.length} operations</div>
            {ops.map((op) => {
              const steps = b.steps.filter((s) => s.mesh === mesh && s.op === op);
              const failed = steps.filter((s) => s.state !== "complete");
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
                      {s.step} · <span style={{ color: s.state === "complete" ? "var(--ok)" : "var(--err)" }}>{outcomeText(s.state)}</span>
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
