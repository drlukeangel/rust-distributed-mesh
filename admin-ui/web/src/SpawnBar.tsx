import { useEffect, useState } from "react";
import { api, type ManagedKind } from "./api";

/// What node-admin manages. Each button submits one Build to node-admin; the
/// UI never starts or stops a runtime itself.
const KINDS: ManagedKind[] = ["rpc_node", "node_admin"];

const reason = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function SpawnBar() {
  const [mesh, setMesh] = useState("mesh1");
  const [chaosRunning, setChaosRunning] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState("");

  useEffect(() => {
    api.chaosState()
      .then((s) => setChaosRunning(s.running))
      .catch(() => {});
  }, []);

  const note = (m: string) => {
    setMsg(m);
    setTimeout(() => setMsg(""), 4000);
  };

  const onMeshChange = (v: string) => {
    if (v === "__new__") {
      const name = prompt("mesh name (e.g. mesh2)");
      if (name && name.trim()) setMesh(name.trim());
    } else {
      setMesh(v);
    }
  };

  const doSpawn = async (kind: ManagedKind) => {
    setBusy(`spawn-${kind}`);
    try {
      const r = await api.spawn(kind, mesh);
      note(`add ${kind} to ${mesh}: build ${r.build_id}`);
    } catch (e) {
      note(`add ${kind} refused: ${reason(e)}`);
    } finally {
      setBusy(null);
    }
  };

  const doBootstrap = async () => {
    setBusy("bootstrap");
    try {
      const r = await api.bootstrap();
      note(`bootstrap: build ${r.build_id}`);
    } catch (e) {
      note(`bootstrap refused: ${reason(e)}`);
    } finally {
      setBusy(null);
    }
  };

  const toggleChaos = async () => {
    setBusy("chaos");
    try {
      const next = chaosRunning ? await api.chaosStop() : await api.chaosStart();
      setChaosRunning(next.running);
      note(next.running ? "chaos started" : "chaos stopped");
    } catch (e) {
      note(`chaos toggle failed: ${reason(e)}`);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="spawn-bar">
      <label className="muted">mesh:</label>
      <select value={mesh} onChange={(e) => onMeshChange(e.target.value)}>
        <option value="mesh1">mesh1</option>
        {mesh !== "mesh1" && <option value={mesh}>{mesh}</option>}
        <option value="__new__">+ other mesh…</option>
      </select>

      {KINDS.map((k) => (
        <button key={k} disabled={busy === `spawn-${k}`} onClick={() => doSpawn(k)}>
          + {k}
        </button>
      ))}
      <span style={{ flex: 1 }} />
      <button
        className="primary"
        disabled={busy === "bootstrap"}
        onClick={doBootstrap}
        title="Reconcile the fabric to the MN shape: mesh1 with 2 node-admins and 3 rpc nodes"
      >
        bootstrap MN
      </button>
      <button
        className={chaosRunning ? "danger" : "warn"}
        disabled={busy === "chaos"}
        onClick={toggleChaos}
        title="Every cadence, ask node-admin to restart a random rpc node"
      >
        {chaosRunning ? "stop chaos" : "start chaos"}
      </button>
      {msg && <span className="muted mono">{msg}</span>}
    </div>
  );
}
