import { useEffect, useMemo, useState } from "react";
import { api, type TestReport } from "../api";

const STATUS_COLOR: Record<string, string> = {
  passed: "var(--ok)",
  failed: "var(--err)",
  running: "var(--accent)",
};

// Mirror of TEST_REGISTRY in cli/rfa/src/main.rs. Kept here so the Tests tab
// can show "never run" entries before they appear as reports on disk.
const REGISTRY: { name: string; kind: string; description: string }[] = [
  { name: "framer-roundtrip", kind: "functional", description: "proptest: tag+varint+postcard frame round-trips" },
  { name: "framer-truncation", kind: "functional", description: "proptest: dropped-last-byte surfaces FramerError::Truncated" },
  { name: "traced-frame-roundtrip", kind: "functional", description: "TracedFrame preserves trace_id+span_id across encode/decode" },
  { name: "unknown-tag-rejected", kind: "functional", description: "non-0x10 tags do NOT deserialize as TracedFrame" },
  { name: "bi-stream-echo", kind: "functional", description: "two iroh endpoints exchange a tag=0x11 framed payload over QUIC bi-stream" },
  { name: "backpressure-stream-flood", kind: "chaos", description: "32 concurrent bi-streams flood 1KiB for 10s; 0 errors + ≥200 round-trips" },
  { name: "chaos-soak-9prim-1min", kind: "chaos", description: "1-minute soak with 9-primitive pool" },
  { name: "chaos-soak-9prim-5min", kind: "chaos", description: "5-minute soak with 9-primitive pool" },
  { name: "mesh-five-types-present", kind: "chaos", description: "spawn 5 types, verify all visible + heartbeats fresh" },
  { name: "remove-resilience", kind: "chaos", description: "kill 3 of 6, verify survivors detect within 15s" },
  { name: "gossip-swarm-forms", kind: "chaos", description: "verify iroh-gossip spans fire (swarm forms)" },
  { name: "gossip-mesh-to-mesh", kind: "chaos", description: "mesh-A and mesh-B gossip stay isolated; cross.peer_connected fires" },
];

export function Tests() {
  const [reports, setReports] = useState<TestReport[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [running, setRunning] = useState<Set<string>>(new Set());

  useEffect(() => {
    const load = () =>
      api
        .tests()
        .then((r) => {
          setReports(Array.isArray(r?.reports) ? r.reports : []);
          setErr(null);
        })
        .catch((e) => setErr(e.message));
    load();
    const id = setInterval(load, 3000);
    return () => clearInterval(id);
  }, []);

  const doRun = async (name: string) => {
    setRunning((s) => new Set(s).add(name));
    try {
      await api.runTest(name);
    } catch (e) {
      // surface in next reports poll; nothing else to do
      console.error("test run failed", e);
    } finally {
      setRunning((s) => {
        const n = new Set(s);
        n.delete(name);
        return n;
      });
    }
  };

  const doRunAll = async () => {
    for (const t of REGISTRY) {
      await doRun(t.name);
    }
  };

  // Group reports by name to show the latest run for each test alongside its
  // run count. The server doesn't currently return a registry list — derive
  // one from observed reports + display a hint about CLI-driven runs.
  const byName = useMemo(() => {
    const m = new Map<string, TestReport[]>();
    for (const r of reports) {
      if (!r?.name) continue;
      if (!m.has(r.name)) m.set(r.name, []);
      m.get(r.name)!.push(r);
    }
    for (const list of m.values()) {
      list.sort((a, b) => (b.finished_at ?? 0) - (a.finished_at ?? 0));
    }
    return Array.from(m.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  }, [reports]);

  if (err) {
    return <div className="card">tests load failed: {err}</div>;
  }

  const anyRunning = running.size > 0;

  return (
    <div className="grid">
      <div className="card row" style={{ justifyContent: "space-between" }}>
        <div>
          <div style={{ fontWeight: 600 }}>
            Tests — {REGISTRY.length} in registry · {reports.length} reports
          </div>
          <div className="muted mono" style={{ fontSize: 11, marginTop: 2 }}>
            Click <b>run</b> to execute via the rfa CLI. Reports written to
            <code> E:/tmp/rafka-tests/</code> appear instantly.
          </div>
        </div>
        <button
          className="primary"
          onClick={doRunAll}
          disabled={anyRunning}
          title="Run every test in the registry, sequentially"
        >
          run all
        </button>
      </div>
      <div className="grid grid-cards">
        {REGISTRY.map((t) => {
          const runs = byName.find(([n]) => n === t.name)?.[1] ?? [];
          const last = runs[0];
          const isRunning = running.has(t.name);
          return (
            <div key={t.name} className="card">
              <div className="row" style={{ justifyContent: "space-between" }}>
                <span className="mono" style={{ fontWeight: 600 }}>{t.name}</span>
                <button
                  onClick={() => doRun(t.name)}
                  disabled={isRunning}
                  style={{ fontSize: 10, padding: "2px 8px" }}
                >
                  {isRunning ? "running…" : "run"}
                </button>
              </div>
              <div className="muted" style={{ fontSize: 11, margin: "4px 0" }}>{t.description}</div>
              <div className="row">
                <span className="pill">{t.kind}</span>
                {isRunning ? (
                  <span className="pill" style={{ borderColor: STATUS_COLOR.running, color: STATUS_COLOR.running }}>
                    running
                  </span>
                ) : last ? (
                  <span
                    className="pill"
                    style={{
                      borderColor: STATUS_COLOR[last.status] || "var(--fg-dim)",
                      color: STATUS_COLOR[last.status] || "var(--fg-dim)",
                    }}
                  >
                    {last.status}
                  </span>
                ) : (
                  <span className="muted">never run</span>
                )}
                {runs.length > 0 && (
                  <span className="muted mono">{runs.length} run{runs.length === 1 ? "" : "s"}</span>
                )}
              </div>
              {last?.duration_ms ? (
                <div className="muted mono" style={{ fontSize: 11, marginTop: 4 }}>
                  last: {(last.duration_ms / 1000).toFixed(1)}s
                  {last.detail && ` · ${last.detail.slice(0, 60)}`}
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
      {err && <div className="card">tests load failed: {err}</div>}
    </div>
  );
}
