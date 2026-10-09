import { useEffect, useState } from "react";
import { api, type ManagedKind, type Overview } from "./api";

/// What node-admin manages in the R-shape. Each button submits one Build to node-admin; the UI never
/// starts or stops a runtime itself.
const KINDS: ManagedKind[] = ["node_admin", "gateway", "broker", "compute"];
const reason = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function SpawnBar() {
  const [mesh, setMesh] = useState("");
  const [ov, setOv] = useState<Overview | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState("");

  useEffect(() => {
    const load = () => api.overview().then(setOv).catch(() => {});
    load();
    const id = setInterval(load, 3000);
    return () => clearInterval(id);
  }, []);
  const meshes = ov?.meshes.map((m) => m.name) ?? [];
  const fpMesh = ov?.meshes.find((m) => m.primary_admin && m.primary_admin === ov.fabric?.fabric_primary)?.name;
  const chosen = mesh && meshes.includes(mesh) ? mesh : meshes[0] ?? "";
  const nextMesh = `mesh${meshes.reduce((n, m) => Math.max(n, parseInt(m.replace(/\D/g, "") || "0", 10)), 0) + 1}`;

  const note = (m: string) => {
    setMsg(m);
    setTimeout(() => setMsg(""), 8000);
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
      <select value={chosen} onChange={(e) => setMesh(e.target.value)} data-testid="mesh-select">
        {meshes.map((m) => <option key={m} value={m}>{m}{m === fpMesh ? " (primary)" : ""}</option>)}
      </select>
      {KINDS.map((k) => (
        <button key={k} disabled={busy === k || !chosen} onClick={() => run(k, `add ${k} to ${chosen}`, () => api.spawn(k, chosen))}>
          + {k}
        </button>
      ))}
      <button disabled={busy === "mesh"} onClick={() => run("mesh", `create ${nextMesh} (2 node-admins, 3 gateways, 3 brokers, 2 computes)`, () => api.createMesh(nextMesh))}
        title={`Create ${nextMesh} in the canonical per-mesh shape: 2 node-admins, 3 gateways, 3 brokers, 2 computes; one Build`}>
        + mesh
      </button>
      <button className="danger" disabled={busy === "rm-mesh" || !chosen} onClick={() => run("rm-mesh", `remove ${chosen}`, () => api.removeMesh(chosen))}
        title="Retire the selected mesh: members first, its node-admins last">
        remove mesh
      </button>
      <span className="muted" style={{ fontSize: 11 }} data-testid="budget-note" title="A node's CPU and RAM budget is read from its own process environment at launch; a Build carries none, so there is nothing for a preset box to set.">
        cpu/ram budgets: fixed at launch (no Build field)
      </span>
      <span style={{ flex: 1 }} />
      <button className="primary" disabled={busy === "bootstrap"} onClick={() => run("bootstrap", "bootstrap", api.bootstrap)}
        title="Reconcile the fabric to the canonical R-shape: mesh1 and mesh2, each 2 node-admins, 3 gateways, 3 brokers, 2 computes (20 nodes)">
        bootstrap 2-mesh
      </button>
      {msg && <span className="muted mono" data-testid="spawn-msg">{msg}</span>}
    </div>
  );
}
