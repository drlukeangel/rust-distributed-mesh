import { useEffect, useState } from "react";
import { api, type TopologyNode } from "../api";
import { colourOfKind } from "../kinds";
import { FramesPlaceholder, UtilBars } from "../Load";

const reason = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function Nodes() {
  const [nodes, setNodes] = useState<TopologyNode[]>([]);
  const [msg, setMsg] = useState("");
  const load = () => api.topology().then((r) => setNodes(r.nodes)).catch(() => {});
  useEffect(() => {
    load();
    const id = setInterval(load, 2000);
    return () => clearInterval(id);
  }, []);

  const act = async (what: string, name: string, f: () => Promise<{ build_id: string; attempt: number }>) => {
    try {
      const r = await f();
      setMsg(`${what} ${name}: build ${r.build_id} attempt ${r.attempt}`);
    } catch (e) {
      setMsg(`${what} ${name} refused: ${reason(e)}`);
    }
    load();
  };

  if (nodes.length === 0) return <div className="card muted">no nodes yet</div>;
  const sorted = [...nodes].sort((a, b) => (a.mesh !== b.mesh ? a.mesh.localeCompare(b.mesh) : a.kind !== b.kind ? a.kind.localeCompare(b.kind) : a.name.localeCompare(b.name)));
  return (
    <div>
      {msg && <div className="card mono" style={{ marginBottom: 8 }} data-testid="nodes-msg">{msg}</div>}
      <div className="grid grid-cards">
        {sorted.map((n) => {
          const c = colourOfKind(n.kind);
          return (
            <div key={n.name} className="card" style={{ position: "relative", borderColor: c }} data-testid={`card-${n.name}`}>
              <div className="mono" style={{ color: c, fontWeight: 600 }}>{n.name}</div>
              <div className="muted mono" style={{ fontSize: 11, marginTop: 4 }}>
                kind: {n.kind}<br />
                mesh: {n.mesh}<br />
                status: {n.status}<br />
                seat: {n.seat || "member"}{n.backbone ? ` · backbone ${n.backbone}` : ""}<br />
                declared: {n.declared ?? "—"}<br />
                incarnation: {n.incarnation_id ? n.incarnation_id.slice(0, 12) : "—"}
              </div>
              <UtilBars load={n.load} testid={`load-${n.name}`} />
              <FramesPlaceholder />
              <div className="row" style={{ marginTop: 8 }}>
                <button onClick={() => act("restart", n.name, () => api.restart(n.name))}>restart</button>
                <button className="danger" onClick={() => act("remove", n.name, () => api.remove(n.name))} title="Submits a RemoveNode Build to node-admin">remove</button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
