import { useCallback, useEffect, useMemo, useState } from "react";

interface Cell {
  job: string;
  issue: number;
  layer: string;
  cell: string;
  dir: string;
  rshape: boolean;
}
interface TestEntry {
  id: string;
  exe_name: string;
  ignored: boolean;
  tiers: string[];
  container: boolean;
  e11: boolean;
  cells: Cell[];
}
interface StemEntry {
  name: string;
  exe: string;
  tiers: string[];
  container: boolean;
  tests: TestEntry[];
}
interface CrateEntry {
  name: string;
  stems: StemEntry[];
}
interface Inventory {
  tree: string;
  tree_sha: string;
  built_sha: string | null;
  counts: { crates: number; stems: number; tests: number; executables: number; registry_cells: number; registry_unmatched: number };
  crates: CrateEntry[];
  unmatched_cells: string[];
  registry_problems: string[];
  jaeger: string;
}
interface Outcome {
  id: string;
  status: string;
  message: string | null;
}
interface TraceRef {
  trace_id: string;
  spans: number;
  root: string | null;
  url: string;
}
interface Job {
  idx: number;
  cadence: string;
  krate: string;
  stem: string;
  label: string;
  tiers: string[];
  container: boolean;
  state: string;
  note: string | null;
  wall_ms: number | null;
  started_ms: number | null;
  exit_code: number | null;
  command: string;
  results: Outcome[];
  test_ids: string[];
  artifacts_dir: string;
  traces: { distinct: number; span_files: number; spans: number; top: TraceRef[] } | null;
  counts: [number, number, number] | null;
  output_tail: string;
}
interface RunSummary {
  id: string;
  label: string;
  started_ms: number;
  finished_ms: number | null;
  parallel: number;
  cancelled: boolean;
  tree_sha: string;
  jobs: number;
  by_state: Record<string, number>;
  tests_passed: number;
  tests_failed: number;
  running: boolean;
}
interface RunDetail extends RunSummary {
  job_list: Job[];
}
interface StatusRow {
  status: string;
  message: string | null;
  run_id: string;
  job: number;
  wall_ms: number | null;
  job_wall_ms: number | null;
  traces: number | null;
}
interface BuildInfo {
  build: { state: string; exit_code: number | null; sha: string | null; started_ms: number | null; finished_ms: number | null };
  have_build_list: boolean;
  log_tail: string;
  tree: string;
  tree_sha: string;
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, { headers: { "Content-Type": "application/json" }, ...init });
  const text = await r.text();
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
  }
  if (!r.ok) {
    const b = body as { error?: string; detail?: string };
    throw new Error(`${r.status} ${b.error ?? ""}: ${b.detail ?? text}`);
  }
  return body as T;
}
const post = <T,>(path: string, body?: unknown) => call<T>(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body) });

const COLORS: Record<string, string> = {
  passed: "var(--ok)",
  failed: "var(--err)",
  "timed-out": "var(--err)",
  running: "var(--accent)",
  queued: "var(--fg-dim)",
  cancelled: "var(--warn)",
  skipped: "var(--warn)",
  blocked: "var(--warn)",
  ignored: "var(--warn)",
  "not-run": "var(--warn)",
};
const TIER_COLORS: Record<string, string> = {
  fast: "var(--ok)",
  release: "var(--warn)",
  canonical: "var(--accent)",
  acceptance: "var(--fg-dim)",
  other: "var(--fg-dim)",
};

const Badge = ({ text, color, testid }: { text: string; color?: string; testid?: string }) => (
  <span className="pill" style={{ borderColor: color ?? "var(--fg-dim)", color: color ?? "var(--fg-dim)", marginRight: 4, whiteSpace: "nowrap" }} data-testid={testid}>
    {text}
  </span>
);
const ms = (v: number | null | undefined) => (v == null ? "—" : v < 1000 ? `${v} ms` : `${(v / 1000).toFixed(1)} s`);
const key = (krate: string, id: string) => `${krate}|${id}`;

function aggregate(rows: (StatusRow | undefined)[]): { passed: number; failed: number; running: number; other: number; none: number } {
  const a = { passed: 0, failed: 0, running: 0, other: 0, none: 0 };
  for (const r of rows) {
    if (!r) a.none++;
    else if (r.status === "passed") a.passed++;
    else if (r.status === "failed" || r.status === "timed-out") a.failed++;
    else if (r.status === "running" || r.status === "queued") a.running++;
    else a.other++;
  }
  return a;
}
function Agg({ a, testid }: { a: ReturnType<typeof aggregate>; testid?: string }) {
  return (
    <span className="mono" style={{ fontSize: 11 }} data-testid={testid}>
      {a.passed > 0 && <span style={{ color: "var(--ok)" }}>{a.passed} pass </span>}
      {a.failed > 0 && <span style={{ color: "var(--err)" }}>{a.failed} fail </span>}
      {a.running > 0 && <span style={{ color: "var(--accent)" }}>{a.running} running </span>}
      {a.other > 0 && <span style={{ color: "var(--warn)" }}>{a.other} other </span>}
      {a.none > 0 && <span className="muted">{a.none} not run</span>}
    </span>
  );
}

export function Tests() {
  const [inv, setInv] = useState<Inventory | null>(null);
  const [invErr, setInvErr] = useState<string | null>(null);
  const [status, setStatus] = useState<Record<string, StatusRow>>({});
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [sel, setSel] = useState<string | null>(null);
  const [detail, setDetail] = useState<RunDetail | null>(null);
  const [build, setBuild] = useState<BuildInfo | null>(null);
  const [parallel, setParallel] = useState(8);
  const [cadence, setCadence] = useState("gate");
  const [filter, setFilter] = useState("");
  const [tier, setTier] = useState("all");
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [msg, setMsg] = useState<string | null>(null);
  const [files, setFiles] = useState<Record<string, { path: string; bytes: number }[]>>({});
  const [viewer, setViewer] = useState<{ path: string; text: string } | null>(null);

  const loadInv = useCallback(() => {
    call<Inventory>("/api/tests/inventory").then((i) => { setInv(i); setInvErr(null); }).catch((e) => { setInv(null); setInvErr(String(e.message ?? e)); });
  }, []);
  useEffect(() => {
    loadInv();
    call<{ parallel: number }>("/api/tests/config").then((c) => setParallel(c.parallel)).catch(() => undefined);
  }, [loadInv]);

  const anyRunning = runs.some((r) => r.running) || build?.build.state === "running";
  useEffect(() => {
    const poll = () => {
      call<{ runs: RunSummary[] }>("/api/tests/runs").then((r) => setRuns(r.runs)).catch(() => undefined);
      call<Record<string, StatusRow>>("/api/tests/status").then(setStatus).catch(() => undefined);
      call<BuildInfo>("/api/tests/build").then(setBuild).catch(() => undefined);
    };
    poll();
    const id = setInterval(poll, anyRunning ? 1000 : 4000);
    return () => clearInterval(id);
  }, [anyRunning]);

  useEffect(() => {
    if (!sel) { setDetail(null); return; }
    const load = () => call<RunDetail>(`/api/tests/runs/${sel}`).then(setDetail).catch(() => undefined);
    load();
    const id = setInterval(load, 1000);
    return () => clearInterval(id);
  }, [sel, runs.length]);

  const buildState = build?.build.state;
  useEffect(() => { if (buildState === "ok") loadInv(); }, [buildState, loadInv]);

  const run = async (body: Record<string, unknown>, what: string) => {
    try {
      const r = await post<{ run_id: string; jobs: number }>("/api/tests/run", { ...body, cadence });
      setMsg(`started ${what}: ${r.jobs} process${r.jobs === 1 ? "" : "es"} (${r.run_id})`);
      setSel(r.run_id);
    } catch (e) {
      setMsg(`refused ${what}: ${(e as Error).message}`);
    }
  };

  const matches = useCallback((s: StemEntry, t?: TestEntry): boolean => {
    const q = filter.trim().toLowerCase();
    const tierOk = (tiers: string[]) => tier === "all" || (tier === "e11" ? false : tiers.includes(tier));
    if (t) return (!q || t.id.toLowerCase().includes(q)) && (tier === "e11" ? t.e11 : tierOk(t.tiers));
    return s.tests.some((x) => matches(s, x));
  }, [filter, tier]);

  const crates = useMemo(() => inv?.crates ?? [], [inv]);
  const activeRun = runs.find((r) => r.running);
  const openFiles = async (runId: string, idx: number) => {
    const k = `${runId}/${idx}`;
    if (files[k]) { setFiles({ ...files, [k]: undefined as never }); return; }
    const r = await call<{ files: { path: string; bytes: number }[] }>(`/api/tests/runs/${runId}/jobs/${idx}/files`);
    setFiles({ ...files, [k]: r.files });
  };
  const view = async (path: string) => {
    const r = await fetch(`/api/tests/file?path=${encodeURIComponent(path)}`);
    setViewer({ path, text: await r.text() });
  };

  const totals = aggregate(crates.flatMap((c) => c.stems.flatMap((s) => s.tests.map((t) => status[key(c.name, t.id)]))));

  return (
    <div data-testid="tests-tab">
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="row" style={{ flexWrap: "wrap", marginBottom: 8 }}>
          <b>Tests</b>
          {inv && (
            <span className="mono muted" data-testid="tests-counts">
              {inv.counts.crates} crates · {inv.counts.stems} stems · {inv.counts.tests} tests · {inv.counts.executables} executables · registry {inv.counts.registry_cells} cells, {inv.counts.registry_unmatched} unmatched
            </span>
          )}
          <span className="mono muted">tree {build?.tree ?? inv?.tree ?? "—"} @ {build?.tree_sha ?? "—"}</span>
          {inv && inv.built_sha && inv.built_sha !== inv.tree_sha && <Badge text={`built @ ${inv.built_sha} ≠ tree @ ${inv.tree_sha}: build again`} color="var(--warn)" />}
          <Agg a={totals} testid="tests-totals" />
        </div>
        <div className="row" style={{ flexWrap: "wrap", gap: 6 }}>
          <button className="primary" data-testid="run-fast-gate" disabled={!inv} onClick={() => run({ scope: "tier", tier: "fast" }, "the fast gate set")}>run fast gate</button>
          <button className="primary" data-testid="run-e11" disabled={!inv} onClick={() => run({ scope: "tier", tier: "e11" }, "the e11 cells")}>run e11 cells</button>
          <button data-testid="run-release" disabled={!inv} onClick={() => run({ scope: "tier", tier: "release" }, "the release-only set")}>run release set</button>
          <button data-testid="run-canonical" disabled={!inv} onClick={() => run({ scope: "tier", tier: "canonical" }, "the R-shape canonical set")}>run canonical (R-shape)</button>
          <button className="warn" data-testid="run-all" disabled={!inv} onClick={() => run({ scope: "all" }, "everything")}>run all</button>
          <button className="danger" data-testid="cancel-run" disabled={!activeRun} onClick={() => activeRun && post(`/api/tests/runs/${activeRun.id}/cancel`).then(() => setMsg(`cancelling ${activeRun.id}`))}>cancel active run</button>
          <span className="muted">|</span>
          <button data-testid="build" disabled={build?.build.state === "running"} onClick={() => post("/api/tests/build").then(() => setMsg("build started")).catch((e) => setMsg(String(e.message)))}>
            {build?.build.state === "running" ? "building…" : "build (cargo build --tests --bins)"}
          </button>
          <button data-testid="refresh-inventory" onClick={loadInv}>refresh inventory</button>
          <span className="muted">cadence</span>
          <select data-testid="cadence" value={cadence} onChange={(e) => setCadence(e.target.value)} title="gate: release-only stems run at production windows, the rest at the fast test cadence">
            <option value="gate">per gate rule</option><option value="fast">fast (3 s / 500 ms)</option><option value="production">production (30 s / 2 s)</option>
          </select>
          <span className="muted">parallel</span>
          <input data-testid="parallel" type="number" min={1} max={64} value={parallel} style={{ width: 56 }} onChange={(e) => setParallel(Number(e.target.value))} />
          <button data-testid="set-parallel" onClick={() => post("/api/tests/config", { parallel }).then(() => setMsg(`parallel ${parallel}`)).catch((e) => setMsg(String(e.message)))}>set</button>
        </div>
        {msg && <div className="mono" style={{ marginTop: 6, fontSize: 12 }} data-testid="tests-msg">{msg}</div>}
        {build && build.build.state !== "idle" && (
          <details style={{ marginTop: 6 }} open={build.build.state === "running"}>
            <summary className="mono" style={{ fontSize: 12 }}>
              build {build.build.state}{build.build.exit_code != null ? ` (exit ${build.build.exit_code})` : ""} {build.build.sha ? `@ ${build.build.sha}` : ""}
            </summary>
            <pre className="mono" data-testid="build-log" style={{ fontSize: 11, maxHeight: 180, overflow: "auto" }}>{build.log_tail}</pre>
          </details>
        )}
      </div>

      {invErr && (
        <div className="card" style={{ borderColor: "var(--warn)", marginBottom: 12 }} data-testid="inventory-error">
          <div className="mono" style={{ color: "var(--warn)" }}>{invErr}</div>
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "minmax(420px, 1fr) minmax(0, 1.2fr)", gap: 12, alignItems: "start" }}>
        <div className="card" data-testid="test-tree">
          <div className="row" style={{ marginBottom: 8, flexWrap: "wrap" }}>
            <input data-testid="filter" placeholder="filter tests…" value={filter} onChange={(e) => setFilter(e.target.value)} style={{ flex: 1, minWidth: 160 }} />
            <select data-testid="tier-filter" value={tier} onChange={(e) => setTier(e.target.value)}>
              {["all", "fast", "release", "canonical", "acceptance", "other", "e11"].map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
            <button data-testid="expand-all" onClick={() => setOpen(Object.fromEntries(crates.flatMap((c) => [[c.name, true], ...c.stems.map((s) => [`${c.name}/${s.name}`, true])])))}>expand</button>
            <button data-testid="collapse-all" onClick={() => setOpen({})}>collapse</button>
          </div>
          {crates.map((c) => {
            const stems = c.stems.filter((s) => matches(s));
            if (stems.length === 0) return null;
            const ca = aggregate(stems.flatMap((s) => s.tests.filter((t) => matches(s, t)).map((t) => status[key(c.name, t.id)])));
            const ck = c.name;
            return (
              <div key={c.name} style={{ marginBottom: 6 }} data-testid={`crate-${c.name}`}>
                <div className="row">
                  <span style={{ cursor: "pointer", fontWeight: 600 }} onClick={() => setOpen({ ...open, [ck]: !open[ck] })}>{open[ck] ? "▾" : "▸"} {c.name}</span>
                  <span className="muted mono">{stems.length} stems</span>
                  <Agg a={ca} />
                  <button className="primary" data-testid={`run-crate-${c.name}`} onClick={() => run({ scope: "crate", crate: c.name }, `crate ${c.name}`)}>run crate</button>
                </div>
                {open[ck] && stems.map((s) => {
                  const sk = `${c.name}/${s.name}`;
                  const ts = s.tests.filter((t) => matches(s, t));
                  const sa = aggregate(ts.map((t) => status[key(c.name, t.id)]));
                  return (
                    <div key={sk} style={{ paddingLeft: 16, marginTop: 3 }} data-testid={`stem-${c.name}-${s.name}`}>
                      <div className="row" style={{ flexWrap: "wrap" }}>
                        <span style={{ cursor: "pointer" }} className="mono" onClick={() => setOpen({ ...open, [sk]: !open[sk] })}>{open[sk] ? "▾" : "▸"} {s.name}</span>
                        {s.tiers.map((t) => <Badge key={t} text={t} color={TIER_COLORS[t]} />)}
                        {s.container && <Badge text="container" color="var(--warn)" />}
                        <span className="muted mono">{ts.length}</span>
                        <Agg a={sa} />
                        <button data-testid={`run-stem-${c.name}-${s.name}`} onClick={() => run({ scope: "stem", crate: c.name, stem: s.name }, `stem ${s.name}`)}>run stem</button>
                      </div>
                      {open[sk] && ts.map((t) => {
                        const st = status[key(c.name, t.id)];
                        return (
                          <div key={t.id} style={{ paddingLeft: 18, marginTop: 2 }} data-testid={`test-${c.name}-${t.id}`}>
                            <div className="row" style={{ flexWrap: "wrap" }}>
                              <span className="mono" style={{ fontSize: 12 }}>{t.id.slice(s.name.length + 2)}</span>
                              {t.tiers.map((x) => <Badge key={x} text={x} color={TIER_COLORS[x]} />)}
                              {t.container && <Badge text="container" color="var(--warn)" />}
                              {t.e11 && <Badge text="e11" color="var(--accent)" />}
                              {t.ignored && <Badge text="ignored" color="var(--warn)" />}
                              {t.cells.map((x) => <Badge key={x.job + x.cell} text={`#${x.issue} ${x.job} (${x.layer})`} />)}
                              {st && <Badge text={st.status} color={COLORS[st.status]} />}
                              {st?.wall_ms != null && <span className="muted mono">{ms(st.wall_ms)}</span>}
                              <button onClick={() => run({ scope: "test", crate: c.name, stem: s.name, test: t.id }, t.id)} data-testid={`run-test-${c.name}-${t.id}`}>run</button>
                              {st && <button onClick={() => setSel(st.run_id)}>run {st.run_id.split("-")[0]}</button>}
                            </div>
                            {st?.message && st.status !== "passed" && <pre className="mono" style={{ color: "var(--err)", fontSize: 11, whiteSpace: "pre-wrap", margin: "2px 0 2px 8px" }}>{st.message}</pre>}
                          </div>
                        );
                      })}
                    </div>
                  );
                })}
              </div>
            );
          })}
          {inv && inv.unmatched_cells.length > 0 && (
            <details><summary className="mono" style={{ fontSize: 12 }}>{inv.unmatched_cells.length} registry cells match no listed test</summary>
              {inv.unmatched_cells.map((u) => <div key={u} className="mono muted">{u}</div>)}</details>
          )}
          {inv && inv.registry_problems.length > 0 && (
            <details><summary className="mono" style={{ fontSize: 12 }}>{inv.registry_problems.length} registry entries that are not tests</summary>
              {inv.registry_problems.map((u) => <div key={u} className="mono muted">{u}</div>)}</details>
          )}
        </div>

        <div style={{ minWidth: 0, overflowWrap: "anywhere" }}>
          <div className="card" style={{ marginBottom: 12 }} data-testid="run-history">
            <b>Run history</b>
            {runs.length === 0 && <div className="muted">no runs yet</div>}
            {runs.map((r) => (
              <div key={r.id} className="row" style={{ marginTop: 4, cursor: "pointer", background: sel === r.id ? "rgba(255,255,255,0.06)" : undefined }} onClick={() => setSel(r.id)} data-testid={`run-row-${r.id}`}>
                <Badge text={r.running ? "running" : r.cancelled ? "cancelled" : (r.by_state.failed ?? 0) + (r.by_state["timed-out"] ?? 0) > 0 ? "failed" : "done"} color={r.running ? "var(--accent)" : (r.by_state.failed ?? 0) + (r.by_state["timed-out"] ?? 0) > 0 ? "var(--err)" : "var(--ok)"} />
                <span className="mono" style={{ fontSize: 12 }}>{r.label}</span>
                <span className="muted mono">{r.jobs} procs · <span style={{ color: "var(--ok)" }}>{r.tests_passed}</span>/<span style={{ color: "var(--err)" }}>{r.tests_failed}</span> · {new Date(r.started_ms).toLocaleTimeString()} · {ms((r.finished_ms ?? Date.now()) - r.started_ms)} · @{r.tree_sha}</span>
              </div>
            ))}
          </div>

          {detail && (
            <div className="card" data-testid="run-detail">
              <div className="row" style={{ flexWrap: "wrap", marginBottom: 6 }}>
                <b className="mono">{detail.id}</b>
                <span className="mono">{detail.label}</span>
                <span className="muted mono">{Object.entries(detail.by_state).map(([k, v]) => `${v} ${k}`).join(" · ")} · parallel {detail.parallel}</span>
                {detail.running && <button className="danger" data-testid="cancel-selected" onClick={() => post(`/api/tests/runs/${detail.id}/cancel`).then(() => setMsg(`cancelling ${detail.id}`))}>cancel</button>}
              </div>
              <table className="mono" style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
                <thead><tr className="muted" style={{ textAlign: "left" }}><th>state</th><th>job</th><th>tests</th><th>wall</th><th>traces</th></tr></thead>
                <tbody>
                  {detail.job_list.map((j) => {
                    const fk = `${detail.id}/${j.idx}`;
                    return (
                      <JobRows key={j.idx} j={j} files={files[fk]} onFiles={() => openFiles(detail.id, j.idx)} onView={view} runId={detail.id} />
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
          {viewer && (
            <div className="card" style={{ marginTop: 12 }} data-testid="file-viewer">
              <div className="row"><b className="mono" style={{ fontSize: 12 }}>{viewer.path}</b><button onClick={() => setViewer(null)}>close</button></div>
              <pre className="mono" style={{ fontSize: 11, maxHeight: 320, overflow: "auto" }}>{viewer.text}</pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

interface TraceRow {
  trace_id: string;
  spans: number;
  duration_ms: number;
  errors: number;
  warns: number;
  services: string[];
  root: string | null;
  url: string;
}
interface FlameSpan {
  span_id: string;
  parent: string;
  name: string;
  service: string;
  node: string | null;
  start_ms: number;
  dur_ms: number;
  severity: "ok" | "warn" | "error";
  attrs: Record<string, unknown>;
  events: string[];
}
interface FlameData {
  trace_id: string;
  url: string;
  total_spans: number;
  shown: number;
  duration_ms: number;
  spans: FlameSpan[];
}

const SERVICE_HUES: Record<string, number> = {};
const hue = (svc: string) => {
  if (!(svc in SERVICE_HUES)) SERVICE_HUES[svc] = (Object.keys(SERVICE_HUES).length * 67 + 200) % 360;
  return SERVICE_HUES[svc];
};
const fmtMs = (v: number) => (v < 1 ? `${(v * 1000).toFixed(0)} µs` : v < 1000 ? `${v.toFixed(1)} ms` : `${(v / 1000).toFixed(2)} s`);

/** Depth-first order of the spans by parent; a span whose parent is not in the trace is a root. */
function nest(spans: FlameSpan[]): { s: FlameSpan; depth: number }[] {
  const ids = new Set(spans.map((x) => x.span_id));
  const kids = new Map<string, FlameSpan[]>();
  const roots: FlameSpan[] = [];
  for (const x of spans) {
    if (x.parent && ids.has(x.parent)) kids.set(x.parent, [...(kids.get(x.parent) ?? []), x]);
    else roots.push(x);
  }
  const out: { s: FlameSpan; depth: number }[] = [];
  const stack: { s: FlameSpan; depth: number }[] = roots.slice().reverse().map((s) => ({ s, depth: 0 }));
  while (stack.length) {
    const n = stack.pop()!;
    out.push(n);
    const ch = kids.get(n.s.span_id) ?? [];
    for (let i = ch.length - 1; i >= 0; i--) stack.push({ s: ch[i], depth: n.depth + 1 });
  }
  return out;
}

function FlamePanel({ runId, idx, test }: { runId: string; idx: number; test: string }) {
  const [traces, setTraces] = useState<{ traces: TraceRow[]; total_traces: number } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [sel, setSel] = useState<string | null>(null);
  const [flame, setFlame] = useState<FlameData | null>(null);
  const [pin, setPin] = useState<FlameSpan | null>(null);
  const q = `?test=${encodeURIComponent(test)}`;
  useEffect(() => {
    call<{ traces: TraceRow[]; total_traces: number }>(`/api/tests/runs/${runId}/jobs/${idx}/traces${q}`)
      .then((t) => { setTraces(t); if (t.traces[0]) setSel(t.traces[0].trace_id); })
      .catch((e) => setErr(String(e.message ?? e)));
  }, [runId, idx, q]);
  useEffect(() => {
    if (!sel) return;
    setFlame(null);
    setPin(null);
    call<FlameData>(`/api/tests/runs/${runId}/jobs/${idx}/trace/${sel}${q}`).then(setFlame).catch((e) => setErr(String(e.message ?? e)));
  }, [runId, idx, sel, q]);
  const rows = useMemo(() => (flame ? nest(flame.spans) : []), [flame]);
  const total = Math.max(flame?.duration_ms ?? 1, 0.001);
  if (err) return <div style={{ color: "var(--err)" }}>{err}</div>;
  if (!traces) return <div className="muted">reading the run's spans…</div>;
  if (traces.traces.length === 0) return <div className="muted" data-testid="flame-empty">this run left no spans for {test}</div>;
  return (
    <div style={{ border: "1px solid var(--border, #333)", padding: 6, margin: "4px 0" }} data-testid="flame">
      <div className="muted">{traces.total_traces} traces (failing first, then slowest){traces.total_traces > traces.traces.length ? `, first ${traces.traces.length} listed` : ""}</div>
      <div style={{ maxHeight: 130, overflow: "auto", marginBottom: 6 }} data-testid="flame-traces">
        {traces.traces.map((t) => (
          <div key={t.trace_id} className="row" style={{ background: sel === t.trace_id ? "rgba(255,255,255,0.08)" : undefined, cursor: "pointer" }} onClick={() => setSel(t.trace_id)}>
            {t.errors > 0 ? <Badge text={`${t.errors} failed`} color="var(--err)" /> : t.warns > 0 ? <Badge text={`${t.warns} warn`} color="var(--warn)" /> : <Badge text="ok" color="var(--ok)" />}
            <span>{t.trace_id.slice(0, 12)}…</span>
            <span className="muted">{fmtMs(t.duration_ms)} · {t.spans} spans · {t.services.join(", ")} · {t.root ?? "no root span here"}</span>
            <a href={t.url} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}>Jaeger</a>
          </div>
        ))}
      </div>
      {!flame && sel && <div className="muted">drawing…</div>}
      {flame && (
        <div>
          <div className="muted" data-testid="flame-head">trace {flame.trace_id} · {fmtMs(flame.duration_ms)} · {flame.shown}/{flame.total_spans} spans · <a href={flame.url} target="_blank" rel="noreferrer">open in Jaeger</a></div>
          <div style={{ maxHeight: 420, overflow: "auto", border: "1px solid #222" }} data-testid="flame-rows">
            {rows.map(({ s, depth }) => {
              const color = s.severity === "error" ? "var(--err)" : s.severity === "warn" ? "var(--warn)" : `hsl(${hue(s.service)} 55% 45%)`;
              const left = (s.start_ms / total) * 100;
              const width = Math.max((s.dur_ms / total) * 100, 0.25);
              return (
                <div key={s.span_id} style={{ display: "flex", height: 16, alignItems: "center", fontSize: 11 }} data-severity={s.severity}
                  title={`${s.name}\n${s.service}${s.node ? " · " + s.node : ""}\n${fmtMs(s.dur_ms)} @ +${fmtMs(s.start_ms)}\n${Object.entries(s.attrs).slice(0, 12).map(([k, v]) => `${k}=${String(v)}`).join("\n")}${s.events.length ? "\nevents: " + s.events.join(" | ") : ""}`}
                  onClick={() => setPin(s)}>
                  <div style={{ width: 380, flex: "none", paddingLeft: depth * 8, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", color: s.severity === "error" ? "var(--err)" : undefined }}>
                    {s.name} <span className="muted">{s.node ?? s.service}</span>
                  </div>
                  <div style={{ position: "relative", flex: 1, height: 10 }}>
                    <div style={{ position: "absolute", left: `${left}%`, width: `${Math.min(width, 100 - left)}%`, height: 10, background: color, opacity: 0.9 }} />
                  </div>
                  <div style={{ width: 64, flex: "none", textAlign: "right" }} className="muted">{fmtMs(s.dur_ms)}</div>
                </div>
              );
            })}
          </div>
          {pin && (
            <pre style={{ fontSize: 11, whiteSpace: "pre-wrap" }} data-testid="flame-pin">
              {pin.name} [{pin.severity}] {pin.service}{pin.node ? " · " + pin.node : ""} {fmtMs(pin.dur_ms)} @ +{fmtMs(pin.start_ms)}{"\n"}
              {Object.entries(pin.attrs).map(([k, v]) => `${k}=${String(v)}`).join("\n")}
              {pin.events.length ? "\nevents:\n  " + pin.events.join("\n  ") : ""}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}

function FlameToggle({ runId, idx, id }: { runId: string; idx: number; id: string }) {
  const [open, setOpen] = useState(false);
  const short = id.includes("::") ? id.slice(id.lastIndexOf("::") + 2) : id;
  return (
    <>
      <button style={{ marginLeft: 6 }} data-testid={`flame-${runId}-${idx}-${short}`} onClick={(e) => { e.stopPropagation(); setOpen(!open); }}>{open ? "hide flame" : "flame"}</button>
      {open && <FlamePanel runId={runId} idx={idx} test={short} />}
    </>
  );
}

function JobRows({ j, files, onFiles, onView, runId }: { j: Job; files?: { path: string; bytes: number }[]; onFiles: () => void; onView: (p: string) => void; runId: string }) {
  const [open, setOpen] = useState(false);
  const failed = j.results.filter((r) => r.status === "failed");
  return (
    <>
      <tr style={{ cursor: "pointer", verticalAlign: "top" }} onClick={() => setOpen(!open)} data-testid={`job-${runId}-${j.idx}`}>
        <td><Badge text={j.state} color={COLORS[j.state]} /></td>
        <td>{j.krate} · {j.label} <Badge text={j.cadence} />{j.container && <Badge text="container" color="var(--warn)" />}</td>
        <td style={{ whiteSpace: "nowrap" }}>{j.counts ? `${j.counts[0]} pass ${j.counts[1]} fail ${j.counts[2]} ign` : `${j.test_ids.length}`}</td>
        <td style={{ whiteSpace: "nowrap" }}>{ms(j.wall_ms)}</td>
        <td>{j.traces ? j.traces.distinct : "—"}</td>
      </tr>
      {open && (
        <tr><td colSpan={5} style={{ paddingLeft: 12 }}>
          {j.note && <div style={{ color: "var(--warn)" }}>{j.note}</div>}
          <div className="muted" style={{ wordBreak: "break-all" }}>{j.command.length > 360 ? `${j.command.slice(0, 360)}…` : j.command}</div>
          <div className="muted">exit {j.exit_code ?? "—"} · artifacts {j.artifacts_dir}</div>
          {j.results.map((r) => (
            <div key={r.id}><Badge text={r.status} color={COLORS[r.status]} />{r.id}<FlameToggle runId={runId} idx={j.idx} id={r.id} />
              {r.message && <pre style={{ color: "var(--err)", whiteSpace: "pre-wrap", fontSize: 11 }} data-testid="failure-message">{r.message}</pre>}</div>
          ))}
          {failed.length === 0 && j.state !== "passed" && j.output_tail && <pre style={{ fontSize: 11, maxHeight: 200, overflow: "auto" }}>{j.output_tail}</pre>}
          {j.traces && j.traces.top.length > 0 && (
            <div data-testid={`traces-${runId}-${j.idx}`}>
              <div className="muted">{j.traces.distinct} traces in {j.traces.spans} spans of {j.traces.span_files} files (top {j.traces.top.length} by root + size)</div>
              {j.traces.top.map((t) => (
                <div key={t.trace_id}><a href={t.url} target="_blank" rel="noreferrer">{t.trace_id}</a> <span className="muted">{t.spans} spans · {t.root ?? "no root span here"}</span></div>
              ))}
            </div>
          )}
          <div className="row" style={{ marginTop: 4 }}>
            <button onClick={(e) => { e.stopPropagation(); onFiles(); }} data-testid={`files-${runId}-${j.idx}`}>{files ? "hide" : "show"} evidence files</button>
          </div>
          {files && files.map((f) => (
            <div key={f.path}><a href="#" onClick={(e) => { e.preventDefault(); onView(f.path); }}>{f.path.includes("/artifacts/") ? f.path.slice(f.path.indexOf("/artifacts/") + 11) : f.path.split("/").slice(1).join("/")}</a> <span className="muted">{f.bytes} B</span></div>
          ))}
          <details open={j.state === "running"}><summary className="muted">output tail</summary>
            <pre style={{ fontSize: 11, maxHeight: 220, overflow: "auto" }} data-testid="output-tail">{j.output_tail}</pre></details>
        </td></tr>
      )}
    </>
  );
}
