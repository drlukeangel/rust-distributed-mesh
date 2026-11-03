import { useEffect, useState } from "react";
import { api, comparePathName, pathNameText, type CommandEnded, type PathName, type TopologyNode } from "../api";
import { NodeName } from "../NodeName";

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

  const act = async (what: string, node: PathName, f: () => Promise<{ build_id: string } | CommandEnded>) => {
    try {
      const r = await f();
      setMsg("terminal" in r ? `${what} ${pathNameText(node)}: ${r.terminal}` : `${what} ${pathNameText(node)}: build ${r.build_id}`);
    } catch (e) {
      setMsg(`${what} ${pathNameText(node)} refused: ${reason(e)}`);
    }
    load();
  };

  if (nodes.length === 0) return <div className="card muted">no nodes yet</div>;
  const sorted = [...nodes].sort((a, b) => comparePathName(a, b));
  return (
    <div>
      {msg && <div className="card mono" style={{ marginBottom: 8 }}>{msg}</div>}
      <div className="grid grid-cards">
        {sorted.map((n) => (
          <div key={n.node_id} className="card">
            <div className="mono" style={{ color: n.type === "node-admin" ? "#e3b341" : "#c9d1d9", fontWeight: 600 }}><NodeName node={n} /></div>
            <div className="muted mono" style={{ fontSize: 11, marginTop: 4 }}>
              type: {n.type}<br />
              mesh: {n.mesh}<br />
              status: {n.status}<br />
              role: {n.role || "member"}<br />
              declared: {n.declared ?? "—"}<br />
              incarnation: {n.incarnation_id ? n.incarnation_id.slice(0, 12) : "—"}
            </div>
            <div className="row" style={{ marginTop: 8 }}>
              <button onClick={() => act("stop", n, () => api.stop(n))}>stop</button>
              <button onClick={() => act("start", n, () => api.start(n))}>start</button>
              <button className="danger" onClick={() => act("remove", n, () => api.remove(n))}
                title="Submits a RemoveNode Build to node-admin">remove</button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
