import type { NodeLoad } from "./api";

const cores = (m: number) => (m / 1000).toFixed(2);
const gb = (b: number) => (b / 1e9).toFixed(2);
const pct = (used: number, budget: number) => (budget > 0 ? Math.min(100, (used / budget) * 100) : 0);

/// CPU (cores) and MEM (GB) of one node, used/budget as its mesh digest published them. No load yet is said so, by name.
export function UtilBars({ load, testid }: { load: NodeLoad | null; testid?: string }) {
  if (!load) {
    return <div className="muted mono" style={{ fontSize: 11, marginTop: 6 }} data-testid={testid ? `${testid}-noload` : undefined}>load: none published yet</div>;
  }
  const rows: Array<[string, number, string]> = [
    ["CPU", pct(load.cpu_used_millicores, load.cpu_budget_millicores), `${cores(load.cpu_used_millicores)}/${cores(load.cpu_budget_millicores)}`],
    ["MEM", pct(load.ram_used_bytes, load.ram_budget_bytes), `${gb(load.ram_used_bytes)}/${gb(load.ram_budget_bytes)}gb`],
  ];
  return (
    <div style={{ marginTop: 6 }} data-testid={testid}>
      {rows.map(([k, p, label]) => (
        <div key={k} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10 }} className="mono">
          <span style={{ width: 28 }}>{k}</span>
          <div style={{ flex: 1, height: 6, background: "#21262d", borderRadius: 3 }}>
            <div style={{ width: `${Math.max(p, 1)}%`, height: 6, borderRadius: 3, background: p > 80 ? "var(--err)" : p > 50 ? "var(--warn)" : "var(--ok)" }} />
          </div>
          <span style={{ width: 120, textAlign: "right" }}>{label}</span>
        </div>
      ))}
    </div>
  );
}

/// RX/TX frame counts are Luke's ruling R-L2: no node publishes them yet, so the place is held and labelled.
export function FramesPlaceholder() {
  return <div className="muted mono" style={{ fontSize: 10, marginTop: 4 }} data-testid="frames-placeholder">RX/TX frames: pending ruling R-L2</div>;
}

/// The card lines of a topology node: CPU and MEM as the old cards drew them.
export function LoadLines({ load }: { load: NodeLoad | null }) {
  if (!load) return <div style={{ fontSize: 9, color: "#6e7681" }}>CPU/MEM: no load yet</div>;
  return (
    <>
      <div style={{ fontSize: 9, color: "#3fb950" }} className="mono">CPU:{cores(load.cpu_used_millicores)}/{cores(load.cpu_budget_millicores)}</div>
      <div style={{ fontSize: 9, color: "#3fb950" }} className="mono">MEM:{gb(load.ram_used_bytes)}/{gb(load.ram_budget_bytes)}gb</div>
    </>
  );
}
