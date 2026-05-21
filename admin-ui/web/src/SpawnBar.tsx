import { useEffect, useState } from "react";
import { api, type NodeType } from "./api";

const TYPES: NodeType[] = ["gateway", "broker", "compute", "registry", "bridge"];

export function SpawnBar() {
  const [mesh, setMesh] = useState("mesh-a");
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
      const name = prompt("new mesh id (e.g. mesh-c)");
      if (name && name.trim()) setMesh(name.trim());
    } else {
      setMesh(v);
    }
  };

  const doSpawn = async (t: NodeType) => {
    setBusy(`spawn-${t}`);
    try {
      const r = await api.spawn(t, mesh);
      note(`spawned ${r.node_name} into ${mesh}`);
    } catch (e: any) {
      note(`spawn failed: ${e.message}`);
    } finally {
      setBusy(null);
    }
  };

  const doBootstrap = async () => {
    setBusy("bootstrap");
    try {
      const r = await api.bootstrap();
      note(`bootstrapped ${r.spawned.length} nodes`);
    } catch (e: any) {
      note(`bootstrap failed: ${e.message}`);
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
    } catch (e: any) {
      note(`chaos toggle failed: ${e.message}`);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="spawn-bar">
      <label className="muted">mesh:</label>
      <select value={mesh} onChange={(e) => onMeshChange(e.target.value)}>
        <option value="mesh-a">mesh-a (primary)</option>
        <option value="mesh-b">mesh-b (secondary)</option>
        {mesh !== "mesh-a" && mesh !== "mesh-b" && (
          <option value={mesh}>{mesh}</option>
        )}
        <option value="__new__">+ new mesh…</option>
      </select>
      {TYPES.map((t) => (
        <button
          key={t}
          disabled={busy === `spawn-${t}`}
          onClick={() => doSpawn(t)}
        >
          + {t}
        </button>
      ))}
      <span style={{ flex: 1 }} />
      <button
        className="primary"
        disabled={busy === "bootstrap"}
        onClick={doBootstrap}
        title="Spawn full two-mesh topology (4×each type per mesh + 2 bridges)"
      >
        bootstrap 2-mesh
      </button>
      <button
        className={chaosRunning ? "danger" : "warn"}
        disabled={busy === "chaos"}
        onClick={toggleChaos}
      >
        {chaosRunning ? "stop chaos" : "start chaos"}
      </button>
      {msg && <span className="muted mono">{msg}</span>}
    </div>
  );
}
