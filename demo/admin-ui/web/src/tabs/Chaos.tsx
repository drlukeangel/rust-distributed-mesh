import { useEffect, useState } from "react";
import { api, type ChaosState, type FaultRecord, type TopologyNode } from "../api";

const reason = (e: unknown) => (e instanceof Error ? e.message : String(e));

function Outcome({ f }: { f: FaultRecord }) {
  const ok = f.outcome === "applied";
  return (
    <div className="mono" style={{ fontSize: 12, padding: "3px 0", borderBottom: "1px solid var(--border)" }} data-testid="fault-outcome">
      <span className="muted">{new Date(f.ts_ms).toLocaleTimeString()}</span>{" "}
      <span className="pill" style={{ borderColor: ok ? "var(--ok)" : "var(--err)", color: ok ? "var(--ok)" : "var(--err)" }}>{f.outcome}</span>{" "}
      <b>{f.action}</b> {f.target} <span className="muted">{JSON.stringify(f.detail)}</span>
    </div>
  );
}

export function Chaos() {
  const [nodes, setNodes] = useState<TopologyNode[]>([]);
  const [state, setState] = useState<ChaosState | null>(null);
  const [msg, setMsg] = useState("");
  const load = () => {
    api.topology().then((r) => setNodes(r.nodes)).catch(() => {});
    api.chaos().then(setState).catch((e) => setMsg(reason(e)));
  };
  useEffect(() => {
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);

  const run = async (f: () => Promise<FaultRecord>) => {
    try {
      const r = await f();
      setMsg(`${r.action} ${r.target}: ${r.outcome}`);
    } catch (e) {
      setMsg(`refused: ${reason(e)}`);
    }
    load();
  };

  const meshes = Array.from(new Set(nodes.map((n) => n.mesh))).sort();
  const sorted = [...nodes].sort((a, b) => a.name.localeCompare(b.name));
  return (
    <div>
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="muted">
          Faults are applied to the exact runtime a node published (process id and start token), by the chaos kit.
          Each fault answers with a typed outcome: applied, or refused with the reason. The fabric primary
          ({state?.fabric_primary ?? "—"}) is never a target.
        </div>
      </div>
      {msg && <div className="card mono" style={{ marginBottom: 12 }} data-testid="chaos-msg">{msg}</div>}

      <div className="grid grid-cards" style={{ marginBottom: 16 }}>
        {sorted.map((n) => (
          <div key={n.name} className="card">
            <div className="mono" style={{ fontWeight: 600 }}>{n.name}</div>
            <div className="muted mono" style={{ fontSize: 11 }}>{n.kind} · {n.status} {n.seat && `· ${n.seat}`}</div>
            {n.is_fabric_primary ? (
              <div className="muted" style={{ marginTop: 8 }}>fabric primary: never a fault target</div>
            ) : (
              <div className="row" style={{ marginTop: 8, flexWrap: "wrap" }}>
                <button className="warn" data-testid={`stop-${n.name}`} onClick={() => run(() => api.fault(n.name, "stop"))}>stop</button>
                <button onClick={() => run(() => api.fault(n.name, "continue"))}>continue</button>
                <button className="danger" onClick={() => run(() => api.fault(n.name, "kill"))}>kill</button>
                <button onClick={() => run(() => api.cut("node", n.name))}>cut network</button>
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="card" style={{ marginBottom: 16 }}>
        <div style={{ fontWeight: 600, marginBottom: 6 }}>Peer meshes</div>
        {meshes.map((m) => {
          const holdsFp = nodes.some((n) => n.mesh === m && n.is_fabric_primary);
          return (
            <div key={m} className="row" style={{ marginBottom: 4 }}>
              <span className="mono" style={{ width: 120 }}>{m}</span>
              {holdsFp ? <span className="muted">holds the fabric primary: never cut</span> : <button onClick={() => run(() => api.cut("mesh", m))}>cut mesh from the rest</button>}
            </div>
          );
        })}
        {state && state.cuts.length > 0 && (
          <div style={{ marginTop: 8 }}>
            <div className="muted">active network cuts</div>
            {state.cuts.map((c) => (
              <div key={c.id} className="row mono" style={{ fontSize: 12 }}>
                <span>#{c.id} {c.scope} {c.target} ({c.members.length} members: {c.members.join(", ")}; udp ports {c.ports.join(", ")})</span>
                <button onClick={() => run(() => api.heal(c.id))}>heal</button>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="card">
        <div style={{ fontWeight: 600, marginBottom: 6 }}>Fault outcomes</div>
        {!state || state.faults.length === 0 ? <div className="muted">no fault applied yet</div> : state.faults.map((f) => <Outcome key={f.id} f={f} />)}
      </div>
    </div>
  );
}
