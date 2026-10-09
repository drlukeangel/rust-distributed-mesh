import { useState } from "react";
import { api, type ManagedKind } from "./api";

/// What node-admin manages. Each button submits one Build to node-admin; the
/// UI never starts or stops a runtime itself.
const KINDS: ManagedKind[] = ["rpc_node", "node_admin"];
const reason = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function SpawnBar() {
  const [mesh, setMesh] = useState("mesh1");
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState("");

  const note = (m: string) => {
    setMsg(m);
    setTimeout(() => setMsg(""), 6000);
  };
  const run = async (key: string, what: string, f: () => Promise<{ build_id: string; attempt: number }>) => {
    setBusy(key);
    try {
      const r = await f();
      note(`${what}: build ${r.build_id} attempt ${r.attempt}`);
    } catch (e) {
      note(`${what} refused: ${reason(e)}`);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="spawn-bar">
      <label className="muted">mesh:</label>
      <input value={mesh} onChange={(e) => setMesh(e.target.value)} style={{ width: 90 }} />
      {KINDS.map((k) => (
        <button key={k} disabled={busy === k} onClick={() => run(k, `add ${k} to ${mesh}`, () => api.spawn(k, mesh))}>
          + {k}
        </button>
      ))}
      <button disabled={busy === "mesh"} onClick={() => run("mesh", `create ${mesh} (2 node-admins, 3 rpc nodes)`, () => api.createMesh(mesh, 2, 3))}
        title="Create this mesh: 2 node-admins and 3 rpc nodes, one Build">
        + mesh
      </button>
      <button className="danger" disabled={busy === "rm-mesh"} onClick={() => run("rm-mesh", `remove ${mesh}`, () => api.removeMesh(mesh))}
        title="Retire this whole mesh: members first, its node-admins last">
        remove mesh
      </button>
      <span style={{ flex: 1 }} />
      <button className="primary" disabled={busy === "bootstrap"} onClick={() => run("bootstrap", "bootstrap", api.bootstrap)}
        title="Reconcile the fabric to mesh1 with 2 node-admins and 3 rpc nodes">
        bootstrap 2+3
      </button>
      {msg && <span className="muted mono">{msg}</span>}
    </div>
  );
}
