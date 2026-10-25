import { useState } from "react";
import { api, type ManagedKind } from "./api";

/// What node-admin manages. Each button submits one Build to node-admin; the
/// UI never starts or stops a runtime itself.
const KINDS: ManagedKind[] = ["rpc_node", "node_admin", "gateway", "compute", "broker"];
/// The R-shape estate (i143 e11.s1): two meshes of 2 node-admins, 2 computes, 3 gateways and 3 brokers.
const R_SHAPE = (name: string) => ({ name, node_admin: 2, compute: 2, gateway: 3, broker: 3 });
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
      <button disabled={busy === "mesh"} onClick={() => run("mesh", `create ${mesh} (2 node-admins, 3 rpc nodes)`, () => api.createMesh({ name: mesh, node_admin: 2, rpc_node: 3 }))}
        title="Create this mesh: 2 node-admins and 3 rpc nodes, one Build">
        + mesh
      </button>
      <button disabled={busy === "rmesh"} onClick={() => run("rmesh", `create ${mesh} (2 node-admins, 2 computes, 3 gateways, 3 brokers)`, () => api.createMesh(R_SHAPE(mesh)))}
        title="Create this mesh in the R shape: 2 node-admins, 2 computes, 3 gateways, 3 brokers, one Build">
        + R mesh
      </button>
      <button className="danger" disabled={busy === "rm-mesh"} onClick={() => run("rm-mesh", `remove ${mesh}`, () => api.removeMesh(mesh))}
        title="Retire this whole mesh: members first, its node-admins last">
        remove mesh
      </button>
      <button disabled={busy === "resubmit"} onClick={() => run("resubmit", "resubmit topology", () => api.resubmit())}
        title="Submit the current topology again, each node on the executable its row records: one Build with nothing to execute">
        resubmit
      </button>
      <button disabled={busy === "replace"} onClick={() => {
          const node = window.prompt("replace the executable of node (path.name)");
          const executable = node ? window.prompt(`the executable file ${node} runs next`) : null;
          if (node && executable) run("replace", `replace ${node}'s executable`, () => api.replace(node, executable));
        }}
        title="Replace a node's executable: node.delete then node.create at its path, on the file named">
        replace executable
      </button>
      <span style={{ flex: 1 }} />
      <button className="primary" disabled={busy === "bootstrap"} onClick={() => run("bootstrap", "bootstrap", () => api.bootstrap())}
        title="Reconcile the fabric to mesh1 with 2 node-admins and 3 rpc nodes">
        bootstrap 2+3
      </button>
      <button className="primary" disabled={busy === "rshape"} onClick={() => run("rshape", "bootstrap R shape", () => api.bootstrap([R_SHAPE("mesh1"), R_SHAPE("mesh2")]))}
        title="Reconcile the fabric to the R shape: mesh1 and mesh2, each 2 node-admins, 2 computes, 3 gateways, 3 brokers; one Build">
        bootstrap R 2/2/3/3 ×2
      </button>
      {msg && <span className="muted mono">{msg}</span>}
    </div>
  );
}
