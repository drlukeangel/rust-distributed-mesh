import { useEffect, useState } from "react";
import { api, type NodeType } from "./api";

const TYPES: NodeType[] = ["gateway", "broker", "compute", "registry", "bridge"];

export function SpawnBar() {
  const [mesh, setMesh] = useState("mesh-a");
  const [chaosRunning, setChaosRunning] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState("");
  // Pure overrides. Blank = no extra_env entry → admin-ui spawn_one applies
  // the per-crate .env.dev preset. Filled = override the preset for this spawn.
  const [cpuOverride, setCpuOverride] = useState<string>("");
  const [ramOverride, setRamOverride] = useState<string>("");

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
      // Blank inputs → no field sent → admin-ui adds no CLI flag → child
      // binary's main() falls through to its .env.dev preset or sysinfo.
      const opts: { cpu_budget?: number; ram_budget?: number } = {};
      if (cpuOverride.trim() !== "") {
        const c = parseFloat(cpuOverride.trim());
        if (!Number.isNaN(c)) opts.cpu_budget = c;
      }
      if (ramOverride.trim() !== "") {
        const r = parseFloat(ramOverride.trim());
        if (!Number.isNaN(r)) opts.ram_budget = r;
      }
      const r = await api.spawn(t, mesh, opts);
      const overridesNote =
        cpuOverride || ramOverride
          ? ` (overrides: cpu=${cpuOverride || "preset"} ram=${ramOverride || "preset"})`
          : "";
      note(`spawned ${r.node_name} into ${mesh}${overridesNote}`);
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

      <label
        className="muted"
        title="override cpu budget for the NEXT spawn (cores, fractional ok). leave blank to use the per-crate .env.dev preset."
      >
        cpu:
      </label>
      <input
        style={{ width: 50 }}
        placeholder="preset"
        value={cpuOverride}
        onChange={(e) => setCpuOverride(e.target.value)}
        inputMode="decimal"
      />
      <label
        className="muted"
        title="override ram budget for the NEXT spawn (GB, fractional ok). leave blank to use the per-crate .env.dev preset."
      >
        ram:
      </label>
      <input
        style={{ width: 50 }}
        placeholder="preset"
        value={ramOverride}
        onChange={(e) => setRamOverride(e.target.value)}
        inputMode="decimal"
      />
      <span className="muted" style={{ fontSize: "0.75rem" }}>gb</span>

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
