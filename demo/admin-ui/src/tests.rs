//! The Tests tab's server: the inventory of every test executable the RDM tree has built, and the
//! runs that drive them.
//!
//! The tests run from `RDM_TESTS_TREE` (default the main checkout) using its prebuilt test
//! executables and node binaries; a build action runs `cargo build --tests --bins` there and keeps
//! cargo's JSON artifact list, which is the only thing the inventory reads executables from. A test is
//! listed by the executable itself (`<exe> --list --format terse`), joined to the acceptance registry
//! (`tools/mesh-audit/i143-acceptance-jobs.json`) by crate and test path.
//!
//! One run is a set of jobs; a job is ONE process (one stem of one crate's executable), with the
//! gate's cadence env and its own `RDM_ARTIFACTS_DIR`. Tests birth their own estates: this server's
//! own `RDM_NODE_ADMIN_API_BASE` / `RDM_EVIDENCE_DIR` (the live estate it draws) are removed from every
//! child's environment, and a child runs in its own process group so a cancel or timeout kills that
//! group and nothing else.

use axum::extract::{Path as AxPath, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

const DEFAULT_TREE: &str = "/home/admin/rust-distributed-mesh";
const REGISTRY: &str = "tools/mesh-audit/i143-acceptance-jobs.json";
const OTLP: &str = "http://192.168.68.99:5317";
const JAEGER_UI: &str = "http://192.168.68.99:16687";
const TAIL_BYTES: u64 = 24 * 1024;

/// The gate's cadence (`rdm-gate-at.sh` fast) and its OTLP endpoint.
const CADENCE: &[(&str, &str)] = &[
    ("RDM_STALENESS_MS", "3000"),
    ("RDM_GOSSIP_INTERVAL_MS", "500"),
    ("RDM_BACKBONE_INTERVAL_MS", "500"),
    ("RAFKA_STALENESS_MS", "3000"),
    ("RAFKA_GOSSIP_INTERVAL_MS", "500"),
    ("RAFKA_BACKBONE_INTERVAL_MS", "500"),
    ("I143_ACCEPTANCE_SKIP_BUILD", "1"),
];

/// Removed from every child: the live estate this server draws, and the server's own settings.
const SCRUBBED: &[&str] = &[
    "RDM_NODE_ADMIN_API_BASE",
    "RDM_NODE_ADMIN_API_BIND",
    "RDM_EVIDENCE_DIR",
    "RDM_UI_STATIC_DIR",
    "RDM_ADMIN_UI_BIND_ADDR",
    "RDM_BIN_DIR",
    "CARGO_TARGET_DIR",
    "MESH_SPAWN_TYPE",
    "RDM_REQUIRE_CONTAINER",
    "RAFKA_REQUIRE_CONTAINER",
    "RDM_RSHAPE_CONSUMER_BIN_DIR",
    "RUSTC_BOOTSTRAP",
];

/// Scenario stems the fast gate runs (`SCENARIOS` of `rdm-gate-at.sh`).
const GATE_SCENARIOS: &[&str] = &[
    "node_lifecycle__node_delete", "node_lifecycle__node_replace", "node_lifecycle__node_restart", "node_rpc__routing", "node_rpc__status",
    "mesh_shapes__shape_reconcile", "mesh_shapes__live_resize", "mesh_shapes__role_build", "mesh_elections__role_cohort", "node_rpc__restart_fence",
    "node_rpc__role_routing", "node_rpc__role_context", "mesh_runtime__role_wedge", "mesh_elections__late_authority", "mesh_elections__sticky_seat",
    "mesh_lifecycle__mesh_create", "mesh_lifecycle__mesh_replace", "mesh_identity__canonical_ids", "mesh_elections__cohort_election", "mesh_rpc__proof_store",
    "fabric_build__accepted_topology", "i143_acceptance_2803", "node_rpc__rpc_certainty", "i143_acceptance_2803_detect", "i143_acceptance_rg6",
];
/// Gate stems that only the release gate runs (`RELEASE_ONLY`).
const GATE_RELEASE_ONLY: &[&str] = &[
    "mesh_runtime__role_wedge", "mesh_elections__cohort_election", "i143_acceptance_2803", "i143_acceptance_2803_detect", "i143_acceptance_2777",
];
/// Crates whose every executable the fast gate runs.
const GATE_CRATES: &[&str] = &[
    "rafka-mesh-audit", "rafka-node-rpc-contract", "rafka-node-rpc", "rafka-node-admin-core", "rafka-node-rpc-testkit", "rafka-node-base", "rafka-mesh-transport",
];
/// Stems that need a container runtime (the gate's `container-proof` job).
const CONTAINER_STEMS: &[&str] = &["container_proof", "mesh_runtime__container_kill"];
/// The e11 cells (PRD §13.1): (crate, stem).
const E11: &[(&str, &str)] = &[
    ("rafka-test-scenario", "node_rpc__restart_fence"),
    ("rafka-test-scenario", "node_rpc__role_routing"),
    ("rafka-test-scenario", "node_rpc__role_context"),
    ("rafka-test-scenario", "mesh_runtime__role_wedge"),
    ("rafka-test-scenario", "mesh_shapes__role_build"),
    ("rafka-test-scenario", "mesh_elections__role_cohort"),
    ("rafka-node-base", "role_process"),
    ("rafka-node-base", "leadership"),
    ("rafka-node-rpc", "pool"),
    ("rafka-mesh-audit", "dependency_rules"),
];

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

// ---------------------------------------------------------------------------------------------
// The registry

/// One cell of the acceptance registry, parsed from its command.
#[derive(Debug, Clone, Serialize)]
pub struct Cell {
    pub job: String,
    pub issue: u64,
    pub layer: String,
    pub cell: String,
    pub dir: String,
    #[serde(skip)]
    pub env: Vec<(String, String)>,
    #[serde(skip)]
    pub krate: String,
    #[serde(skip)]
    pub id: String,
    pub rshape: bool,
}

/// `[K=V ...] cargo test -p <crate> [--test <t>] <filter> [-- --exact]` -> (env, crate, id).
pub fn parse_command(command: &str) -> Result<(Vec<(String, String)>, String, String), String> {
    let toks: Vec<&str> = command.split_whitespace().collect();
    let mut env = Vec::new();
    let mut i = 0;
    while i < toks.len() && toks[i] != "cargo" && toks[i].contains('=') {
        let (k, v) = toks[i].split_once('=').unwrap();
        env.push((k.to_string(), v.to_string()));
        i += 1;
    }
    if toks.get(i) != Some(&"cargo") || toks.get(i + 1) != Some(&"test") {
        return Err(format!("not a `cargo test` command: {command}"));
    }
    i += 2;
    let (mut krate, mut target, mut filter) = (None, None, None);
    while i < toks.len() && toks[i] != "--" {
        match toks[i] {
            "-p" | "--package" => {
                krate = toks.get(i + 1).map(|s| s.to_string());
                i += 2;
            }
            "--test" => {
                target = toks.get(i + 1).map(|s| s.to_string());
                i += 2;
            }
            t if t.starts_with('-') => i += 1,
            t => {
                filter.get_or_insert_with(|| t.to_string());
                i += 1;
            }
        }
    }
    let krate = krate.ok_or_else(|| format!("no -p in: {command}"))?;
    let filter = filter.ok_or_else(|| format!("no test filter in: {command}"))?;
    let id = match target.as_deref() {
        Some("main") | None => filter,
        Some(t) => format!("{t}::{filter}"),
    };
    Ok((env, krate, id))
}

pub fn load_registry(tree: &Path) -> Result<(Vec<Cell>, Vec<String>), String> {
    let path = tree.join(REGISTRY);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("registry {}: {e}", path.display()))?;
    let v: Value = serde_json::from_str(&text).map_err(|e| format!("registry {}: {e}", path.display()))?;
    let mut cells = Vec::new();
    let mut problems = Vec::new();
    for (section, rshape) in [("jobs", false), ("rshape_jobs", true)] {
        let Some(jobs) = v[section].as_object() else { continue };
        for (job, body) in jobs {
            if !body.is_object() {
                continue;
            }
            let issue = body["issue"].as_u64().unwrap_or(0);
            let layer = body["layer"].as_str().unwrap_or("").to_string();
            for c in body["cells"].as_array().into_iter().flatten() {
                let (name, dir, command) = (c["name"].as_str().unwrap_or(""), c["dir"].as_str().unwrap_or(""), c["command"].as_str().unwrap_or(""));
                match parse_command(command) {
                    Ok((env, krate, id)) => cells.push(Cell { job: job.clone(), issue, layer: layer.clone(), cell: name.into(), dir: dir.into(), env, krate, id, rshape }),
                    Err(e) => problems.push(format!("{job}/{name}: {e}")),
                }
            }
        }
    }
    Ok((cells, problems))
}

// ---------------------------------------------------------------------------------------------
// The inventory

#[derive(Debug, Clone)]
pub struct Exe {
    pub krate: String,
    pub target: String,
    pub kind: String,
    pub path: PathBuf,
    pub manifest_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct TestEntry {
    /// The stem-qualified id the registry and the UI use.
    pub id: String,
    /// The name the executable lists (what `--exact` takes).
    pub exe_name: String,
    pub ignored: bool,
    pub tiers: Vec<String>,
    pub container: bool,
    pub e11: bool,
    pub cells: Vec<Cell>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StemEntry {
    pub name: String,
    pub exe: String,
    pub tiers: Vec<String>,
    pub container: bool,
    pub tests: Vec<TestEntry>,
    #[serde(skip)]
    pub exe_idx: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CrateEntry {
    pub name: String,
    pub stems: Vec<StemEntry>,
}

#[derive(Debug, Clone)]
pub struct Inventory {
    pub crates: Vec<CrateEntry>,
    pub exes: Vec<Exe>,
    pub unmatched_cells: Vec<String>,
    pub registry_problems: Vec<String>,
    pub registry_cells: usize,
}

/// The stem of a test path inside one executable.
fn stem_of(exe: &Exe, name: &str) -> (String, String) {
    // (stem, id)
    if exe.kind == "test" && exe.target == "main" {
        let stem = name.split_once("::").map(|(s, _)| s).unwrap_or("(root)");
        (stem.to_string(), name.to_string())
    } else if exe.kind == "test" {
        (exe.target.clone(), format!("{}::{name}", exe.target))
    } else {
        let seg = name.split_once("::").map(|(s, _)| s).unwrap_or("(root)");
        (format!("{}/{seg}", exe.target), format!("{}::{name}", exe.target))
    }
}

fn list_exe(exe: &Exe, ignored: bool) -> Result<Vec<String>, String> {
    let mut cmd = std::process::Command::new(&exe.path);
    cmd.current_dir(&exe.manifest_dir).env("CARGO_MANIFEST_DIR", &exe.manifest_dir).args(["--list", "--format", "terse"]);
    if ignored {
        cmd.arg("--ignored");
    }
    let out = cmd.output().map_err(|e| format!("{}: {e}", exe.path.display()))?;
    if !out.status.success() {
        return Err(format!("{} --list: {}", exe.path.display(), String::from_utf8_lossy(&out.stderr).trim()));
    }
    Ok(String::from_utf8_lossy(&out.stdout).lines().filter_map(|l| l.strip_suffix(": test")).map(str::to_string).collect())
}

/// The executables cargo built as tests, from the saved `--message-format=json` list.
pub fn read_exes(tree: &Path, artifacts: &Path) -> Result<Vec<Exe>, String> {
    let meta = std::process::Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1", "--offline"])
        .current_dir(tree)
        .env_remove("CARGO_TARGET_DIR")
        .output()
        .map_err(|e| format!("cargo metadata: {e}"))?;
    if !meta.status.success() {
        return Err(format!("cargo metadata: {}", String::from_utf8_lossy(&meta.stderr).trim()));
    }
    let meta: Value = serde_json::from_slice(&meta.stdout).map_err(|e| format!("cargo metadata json: {e}"))?;
    let mut by_manifest: HashMap<String, String> = HashMap::new();
    for p in meta["packages"].as_array().into_iter().flatten() {
        by_manifest.insert(p["manifest_path"].as_str().unwrap_or("").to_string(), p["name"].as_str().unwrap_or("").to_string());
    }
    let text = std::fs::read_to_string(artifacts).map_err(|e| format!("{}: {e}", artifacts.display()))?;
    let mut exes = BTreeMap::new();
    for l in text.lines() {
        let Ok(m) = serde_json::from_str::<Value>(l) else { continue };
        if m["reason"] != "compiler-artifact" || m["profile"]["test"] != true {
            continue;
        }
        let (Some(path), Some(manifest)) = (m["executable"].as_str(), m["manifest_path"].as_str()) else { continue };
        let Some(krate) = by_manifest.get(manifest) else { continue };
        let kind = m["target"]["kind"][0].as_str().unwrap_or("").to_string();
        let kind = if kind == "test" { "test".to_string() } else { kind };
        let exe = Exe {
            krate: krate.clone(),
            target: m["target"]["name"].as_str().unwrap_or("").to_string(),
            kind,
            path: PathBuf::from(path),
            manifest_dir: Path::new(manifest).parent().unwrap_or(Path::new("/")).to_path_buf(),
        };
        exes.insert((exe.krate.clone(), exe.target.clone(), exe.kind.clone()), exe);
    }
    Ok(exes.into_values().collect())
}

fn tiers_for(krate: &str, stem: &str, cells: &[Cell]) -> (Vec<String>, bool) {
    let mut tiers = BTreeSet::new();
    let container_stem = CONTAINER_STEMS.contains(&stem);
    let mut container = container_stem;
    let release_only = GATE_RELEASE_ONLY.contains(&stem) || container_stem;
    if krate == "rafka-test-scenario" {
        if GATE_SCENARIOS.contains(&stem) {
            tiers.insert(if release_only { "release" } else { "fast" });
        }
    } else if GATE_CRATES.contains(&krate) {
        tiers.insert(if container_stem { "release" } else { "fast" });
    }
    if container_stem {
        tiers.insert("release");
    }
    for c in cells {
        if c.rshape {
            tiers.insert("canonical");
        } else {
            tiers.insert("acceptance");
        }
        if c.layer.contains("container") || c.env.iter().any(|(k, v)| k == "MESH_SPAWN_TYPE" && v == "container") {
            container = true;
        }
    }
    if tiers.is_empty() {
        tiers.insert("other");
    }
    (tiers.into_iter().map(String::from).collect(), container)
}

pub fn build_inventory(tree: &Path, artifacts: &Path) -> Result<Inventory, String> {
    let exes = read_exes(tree, artifacts)?;
    let (cells, registry_problems) = load_registry(tree)?;
    let mut by_id: HashMap<(String, String), Vec<Cell>> = HashMap::new();
    for c in &cells {
        by_id.entry((c.krate.clone(), c.id.clone())).or_default().push(c.clone());
    }
    let mut matched: BTreeSet<(String, String)> = BTreeSet::new();
    let mut crates: BTreeMap<String, BTreeMap<String, StemEntry>> = BTreeMap::new();
    for (idx, exe) in exes.iter().enumerate() {
        let listed = list_exe(exe, false)?;
        let ignored: BTreeSet<String> = list_exe(exe, true)?.into_iter().collect();
        for name in listed {
            let (stem, id) = stem_of(exe, &name);
            let key = (exe.krate.clone(), id.clone());
            let cells_for = by_id.get(&key).cloned().unwrap_or_default();
            if !cells_for.is_empty() {
                matched.insert(key);
            }
            let e = crates.entry(exe.krate.clone()).or_default().entry(stem.clone()).or_insert_with(|| StemEntry {
                name: stem.clone(),
                exe: format!("{}:{}", exe.kind, exe.target),
                tiers: vec![],
                container: false,
                tests: vec![],
                exe_idx: idx,
            });
            let (tiers, container) = tiers_for(&exe.krate, &stem, &cells_for);
            let e11 = E11.iter().any(|(c, s)| *c == exe.krate && *s == stem);
            e.tests.push(TestEntry { ignored: ignored.contains(&name), id, exe_name: name, tiers, container, e11, cells: cells_for });
        }
    }
    let mut out = Vec::new();
    for (name, stems) in crates {
        let mut stems: Vec<StemEntry> = stems.into_values().collect();
        for s in &mut stems {
            s.tests.sort_by(|a, b| a.id.cmp(&b.id));
            let mut t = BTreeSet::new();
            for x in &s.tests {
                t.extend(x.tiers.iter().cloned());
            }
            s.tiers = t.into_iter().collect();
            s.container = s.tests.iter().any(|t| t.container);
        }
        out.push(CrateEntry { name, stems });
    }
    let unmatched_cells = cells
        .iter()
        .filter(|c| !matched.contains(&(c.krate.clone(), c.id.clone())))
        .map(|c| format!("{} / {} -> {}::{}", c.job, c.cell, c.krate, c.id))
        .collect();
    Ok(Inventory { crates: out, exes, unmatched_cells, registry_problems, registry_cells: cells.len() })
}

// ---------------------------------------------------------------------------------------------
// libtest output

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Outcome {
    pub id: String,
    /// passed | failed | ignored | not-run
    pub status: String,
    pub message: Option<String>,
}

/// Per-test results from a libtest log. `names` maps the executable's listed name to the UI id.
pub fn parse_libtest(log: &str, names: &[(String, String)]) -> (Vec<Outcome>, Option<(usize, usize, usize)>) {
    let mut summary = None;
    for l in log.lines() {
        if let Some(rest) = l.strip_prefix("test result: ") {
            let num = |tag: &str| rest.split(';').find_map(|p| p.trim().trim_start_matches("ok. ").trim_start_matches("FAILED. ").strip_suffix(tag).and_then(|n| n.trim().parse::<usize>().ok()));
            summary = Some((num(" passed").unwrap_or(0), num(" failed").unwrap_or(0), num(" ignored").unwrap_or(0)));
        }
    }
    let mut failures: BTreeSet<&str> = BTreeSet::new();
    for l in log.lines() {
        if let Some(n) = l.strip_prefix("---- ").and_then(|r| r.strip_suffix(" stdout ----")) {
            failures.insert(n);
        }
    }
    let mut out = Vec::new();
    for (exe_name, id) in names {
        let marker = format!("test {exe_name} ... ");
        let mut status = None;
        for l in log.lines() {
            if let Some(pos) = l.find(&marker) {
                let rest = &l[pos + marker.len()..];
                status = if rest.starts_with("ok") {
                    Some("passed")
                } else if rest.starts_with("FAILED") {
                    Some("failed")
                } else if rest.starts_with("ignored") {
                    Some("ignored")
                } else {
                    status
                };
            }
        }
        if status.is_none() && failures.contains(exe_name.as_str()) {
            status = Some("failed");
        }
        let status = status.unwrap_or("not-run").to_string();
        let message = if status == "failed" { panic_message(log, exe_name) } else { None };
        out.push(Outcome { id: id.clone(), status, message });
    }
    (out, summary)
}

/// The panic of one failed test: its `---- name stdout ----` section, trimmed.
pub fn panic_message(log: &str, name: &str) -> Option<String> {
    let head = format!("---- {name} stdout ----");
    let start = log.find(&head)? + head.len();
    let body = &log[start..];
    let end = body.find("\n---- ").or_else(|| body.find("\nfailures:")).unwrap_or(body.len());
    let section = body[..end].trim();
    let msg = match section.find("panicked at") {
        Some(p) => {
            let from = section[..p].rfind("thread '").unwrap_or(p);
            let tail = &section[from..];
            tail.split("\nnote: run with").next().unwrap_or(tail).trim().to_string()
        }
        None => section.to_string(),
    };
    let msg: String = msg.chars().take(2400).collect();
    (!msg.is_empty()).then_some(msg)
}

// ---------------------------------------------------------------------------------------------
// Traces

#[derive(Debug, Clone, Serialize)]
pub struct TraceRef {
    pub trace_id: String,
    pub spans: usize,
    pub root: Option<String>,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct Traces {
    pub distinct: usize,
    pub span_files: usize,
    pub spans: usize,
    pub top: Vec<TraceRef>,
}

#[derive(Deserialize)]
struct SpanLine {
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    parent_span_id: String,
    #[serde(default)]
    name: String,
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>, depth: usize) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            if depth < 8 {
                walk(&p, out, depth + 1);
            }
        } else {
            out.push(p);
        }
    }
}

pub fn scan_traces(dir: &Path, jaeger: &str) -> Traces {
    let mut files = Vec::new();
    walk(dir, &mut files, 0);
    let spans_files: Vec<_> = files.iter().filter(|p| p.to_string_lossy().ends_with(".spans.jsonl")).collect();
    let mut agg: HashMap<String, (usize, Option<String>)> = HashMap::new();
    let mut total = 0;
    for f in &spans_files {
        let Ok(fh) = std::fs::File::open(f) else { continue };
        for line in BufReader::new(fh).lines().map_while(Result::ok) {
            let Ok(s) = serde_json::from_str::<SpanLine>(&line) else { continue };
            if s.trace_id.is_empty() {
                continue;
            }
            total += 1;
            let e = agg.entry(s.trace_id).or_insert((0, None));
            e.0 += 1;
            if s.parent_span_id.is_empty() && e.1.is_none() {
                e.1 = Some(s.name);
            }
        }
    }
    let distinct = agg.len();
    let mut rows: Vec<_> = agg.into_iter().collect();
    rows.sort_by(|a, b| (b.1 .1.is_some(), b.1 .0).cmp(&(a.1 .1.is_some(), a.1 .0)));
    let top = rows.into_iter().take(24).map(|(id, (n, root))| TraceRef { url: format!("{jaeger}/trace/{id}"), trace_id: id, spans: n, root }).collect();
    Traces { distinct, span_files: spans_files.len(), spans: total, top }
}

// ---------------------------------------------------------------------------------------------
// Runs

#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub idx: usize,
    pub krate: String,
    pub stem: String,
    pub label: String,
    pub tiers: Vec<String>,
    pub container: bool,
    /// queued | running | passed | failed | timed-out | cancelled | skipped | blocked
    pub state: String,
    pub note: Option<String>,
    pub started_ms: Option<u64>,
    pub finished_ms: Option<u64>,
    pub wall_ms: Option<u64>,
    pub exit_code: Option<i32>,
    pub pid: Option<u32>,
    pub command: String,
    pub results: Vec<Outcome>,
    pub test_ids: Vec<String>,
    pub artifacts_dir: String,
    pub log_path: String,
    pub traces: Option<Traces>,
    pub counts: Option<(usize, usize, usize)>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Run {
    pub id: String,
    pub label: String,
    pub scope: Value,
    pub started_ms: u64,
    pub finished_ms: Option<u64>,
    pub parallel: usize,
    pub cancelled: bool,
    pub tree_sha: String,
    pub jobs: Vec<Job>,
}

struct JobSpec {
    exe: Exe,
    names: Vec<(String, String)>,
    env: Vec<(String, String)>,
    timeout: Duration,
    container: bool,
}

struct Gate {
    limit: AtomicUsize,
    running: Mutex<usize>,
    notify: Notify,
}

impl Gate {
    async fn acquire(&self) {
        loop {
            let n = self.notify.notified();
            tokio::pin!(n);
            n.as_mut().enable();
            {
                let mut r = self.running.lock().unwrap();
                if *r < self.limit.load(Ordering::SeqCst) {
                    *r += 1;
                    return;
                }
            }
            n.await;
        }
    }
    fn release(&self) {
        *self.running.lock().unwrap() -= 1;
        self.notify.notify_waiters();
    }
}

#[derive(Clone, Serialize, Default)]
struct BuildState {
    /// idle | running | ok | failed
    state: String,
    started_ms: Option<u64>,
    finished_ms: Option<u64>,
    exit_code: Option<i32>,
    sha: Option<String>,
}

struct Inner {
    tree: PathBuf,
    jaeger: String,
    gate: Gate,
    runs: Mutex<Vec<Run>>,
    cancels: Mutex<HashMap<String, Arc<AtomicBool>>>,
    seq: AtomicUsize,
    build: Mutex<BuildState>,
    inventory: Mutex<Option<(std::time::SystemTime, Arc<Inventory>)>>,
    container_runtime: tokio::sync::OnceCell<Result<(), String>>,
}

type S = Arc<Inner>;

impl Inner {
    fn ui_dir(&self) -> PathBuf {
        self.tree.join("target/ui-tests")
    }
    fn artifacts(&self) -> PathBuf {
        self.ui_dir().join("artifacts.json")
    }
    fn runs_dir(&self) -> PathBuf {
        self.tree.join("target/ui-test-runs")
    }
}

fn err(status: StatusCode, error: &str, detail: impl std::fmt::Display) -> Response {
    (status, Json(json!({"error": error, "detail": detail.to_string()}))).into_response()
}

fn tree_sha(tree: &Path) -> String {
    std::process::Command::new("git").args(["rev-parse", "--short", "HEAD"]).current_dir(tree).output().ok().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).filter(|s| !s.is_empty()).unwrap_or_else(|| "unknown".into())
}

fn inventory(s: &S) -> Result<Arc<Inventory>, Response> {
    let art = s.artifacts();
    let mtime = std::fs::metadata(&art).and_then(|m| m.modified()).map_err(|_| {
        err(StatusCode::CONFLICT, "not-built", format!("no build list at {}: run the build action (cargo build --tests --bins in {}) first", art.display(), s.tree.display()))
    })?;
    let mut cache = s.inventory.lock().unwrap();
    if let Some((t, inv)) = cache.as_ref() {
        if *t == mtime {
            return Ok(inv.clone());
        }
    }
    let inv = Arc::new(build_inventory(&s.tree, &art).map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "inventory-failed", e))?);
    *cache = Some((mtime, inv.clone()));
    Ok(inv)
}

async fn get_inventory(State(s): State<S>) -> Response {
    let s2 = s.clone();
    let inv = match tokio::task::spawn_blocking(move || inventory(&s2)).await {
        Ok(Ok(i)) => i,
        Ok(Err(r)) => return r,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, "inventory-task", e),
    };
    let tests: usize = inv.crates.iter().flat_map(|c| &c.stems).map(|s| s.tests.len()).sum();
    let stems: usize = inv.crates.iter().map(|c| c.stems.len()).sum();
    let build = s.build.lock().unwrap().clone();
    Json(json!({
        "tree": s.tree, "tree_sha": tree_sha(&s.tree), "built_sha": build.sha,
        "counts": {"crates": inv.crates.len(), "stems": stems, "tests": tests, "executables": inv.exes.len(), "registry_cells": inv.registry_cells, "registry_unmatched": inv.unmatched_cells.len()},
        "executables": inv.exes.iter().map(|e| json!({"crate": e.krate, "target": e.target, "kind": e.kind, "path": e.path})).collect::<Vec<_>>(),
        "crates": inv.crates, "unmatched_cells": inv.unmatched_cells, "registry_problems": inv.registry_problems,
        "jaeger": s.jaeger,
    }))
    .into_response()
}

// -- build ------------------------------------------------------------------------------------

async fn start_build(State(s): State<S>) -> Response {
    {
        let mut b = s.build.lock().unwrap();
        if b.state == "running" {
            return err(StatusCode::CONFLICT, "build-running", "a build is already running");
        }
        *b = BuildState { state: "running".into(), started_ms: Some(now_ms()), ..Default::default() };
    }
    let dir = s.ui_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return err(StatusCode::INTERNAL_SERVER_ERROR, "build-dir", format!("{}: {e}", dir.display()));
    }
    let (json_tmp, log) = (dir.join("artifacts.json.tmp"), dir.join("build.log"));
    let (out, errf) = match (std::fs::File::create(&json_tmp), std::fs::File::create(&log)) {
        (Ok(a), Ok(b)) => (a, b),
        (a, b) => return err(StatusCode::INTERNAL_SERVER_ERROR, "build-files", format!("{:?} {:?}", a.err(), b.err())),
    };
    let sha = tree_sha(&s.tree);
    let mut cmd = tokio::process::Command::new("cargo");
    cmd.args(["build", "--message-format=json", "--tests", "--bins"]).current_dir(&s.tree).env_remove("CARGO_TARGET_DIR").stdout(out).stderr(errf).process_group(0);
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            *s.build.lock().unwrap() = BuildState { state: "failed".into(), finished_ms: Some(now_ms()), ..Default::default() };
            return err(StatusCode::INTERNAL_SERVER_ERROR, "build-spawn", e);
        }
    };
    let s2 = s.clone();
    tokio::spawn(async move {
        let st = child.wait().await;
        let ok = st.as_ref().map(|s| s.success()).unwrap_or(false);
        if ok {
            let _ = std::fs::rename(s2.ui_dir().join("artifacts.json.tmp"), s2.artifacts());
        }
        let mut b = s2.build.lock().unwrap();
        b.state = if ok { "ok" } else { "failed" }.into();
        b.finished_ms = Some(now_ms());
        b.exit_code = st.ok().and_then(|s| s.code());
        b.sha = Some(sha);
    });
    Json(json!({"started": true})).into_response()
}

fn tail_of(path: &Path, bytes: u64) -> String {
    let Ok(mut f) = std::fs::File::open(path) else { return String::new() };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    let _ = f.seek(SeekFrom::Start(len.saturating_sub(bytes)));
    let mut buf = Vec::new();
    let _ = f.read_to_end(&mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

async fn get_build(State(s): State<S>) -> Response {
    let b = s.build.lock().unwrap().clone();
    let have_list = s.artifacts().exists();
    Json(json!({"build": b, "have_build_list": have_list, "log_tail": tail_of(&s.ui_dir().join("build.log"), TAIL_BYTES), "tree": s.tree, "tree_sha": tree_sha(&s.tree)})).into_response()
}

// -- config -----------------------------------------------------------------------------------

async fn get_config(State(s): State<S>) -> Json<Value> {
    Json(json!({"parallel": s.gate.limit.load(Ordering::SeqCst), "tree": s.tree, "jaeger": s.jaeger, "otlp": OTLP, "cadence": CADENCE}))
}

#[derive(Deserialize)]
struct ConfigBody {
    parallel: usize,
}

async fn set_config(State(s): State<S>, Json(b): Json<ConfigBody>) -> Response {
    if !(1..=64).contains(&b.parallel) {
        return err(StatusCode::BAD_REQUEST, "bad-parallel", "parallel is 1..=64");
    }
    s.gate.limit.store(b.parallel, Ordering::SeqCst);
    s.gate.notify.notify_waiters();
    Json(json!({"parallel": b.parallel})).into_response()
}

// -- running ----------------------------------------------------------------------------------

#[derive(Deserialize, Clone)]
struct RunBody {
    /// test | stem | crate | tier | all
    scope: String,
    #[serde(rename = "crate")]
    krate: Option<String>,
    stem: Option<String>,
    test: Option<String>,
    tier: Option<String>,
}

fn timeout_for(tiers: &[String], container: bool) -> Duration {
    let base: u64 = std::env::var("RDM_TESTS_JOB_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(300);
    let mult = if tiers.iter().any(|t| t == "canonical") {
        6
    } else if container || tiers.iter().any(|t| t == "release" || t == "acceptance") {
        3
    } else {
        1
    };
    Duration::from_secs(base * mult)
}

fn slug(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' }).collect()
}

/// The env a test's registry cells ask for, absolute where a value names a tree path.
fn cell_env(tree: &Path, t: &TestEntry) -> Vec<(String, String)> {
    let mut env: BTreeMap<String, String> = BTreeMap::new();
    for c in &t.cells {
        for (k, v) in &c.env {
            if k == "RDM_ARTIFACTS_DIR" {
                continue;
            }
            let v = if v.starts_with("target/") { tree.join(v).display().to_string() } else { v.clone() };
            env.insert(k.clone(), v);
        }
    }
    if t.container {
        env.entry("RDM_REQUIRE_CONTAINER".into()).or_insert_with(|| "1".into());
        env.entry("RAFKA_REQUIRE_CONTAINER".into()).or_insert_with(|| "1".into());
    }
    env.into_iter().collect()
}

struct Planned {
    krate: String,
    stem: String,
    exe_idx: usize,
    tests: Vec<TestEntry>,
}

fn select(inv: &Inventory, b: &RunBody) -> Result<Vec<Planned>, String> {
    let mut out = Vec::new();
    for c in &inv.crates {
        if let Some(k) = &b.krate {
            if b.scope != "tier" && b.scope != "all" && &c.name != k {
                continue;
            }
        }
        for st in &c.stems {
            let tests: Vec<TestEntry> = match b.scope.as_str() {
                "all" => st.tests.clone(),
                "crate" => st.tests.clone(),
                "stem" => {
                    if Some(&st.name) != b.stem.as_ref() {
                        continue;
                    }
                    st.tests.clone()
                }
                "test" => {
                    if Some(&st.name) != b.stem.as_ref() {
                        continue;
                    }
                    st.tests.iter().filter(|t| Some(&t.id) == b.test.as_ref()).cloned().collect()
                }
                "tier" => {
                    let tier = b.tier.as_deref().unwrap_or("");
                    st.tests.iter().filter(|t| if tier == "e11" { t.e11 } else { t.tiers.iter().any(|x| x == tier) }).cloned().collect()
                }
                other => return Err(format!("unknown scope `{other}`: test, stem, crate, tier or all")),
            };
            if !tests.is_empty() {
                out.push(Planned { krate: c.name.clone(), stem: st.name.clone(), exe_idx: st.exe_idx, tests });
            }
        }
    }
    if out.is_empty() {
        return Err(format!("nothing in the inventory matches {}", serde_json::to_string(&json!({"scope": b.scope, "crate": b.krate, "stem": b.stem, "test": b.test, "tier": b.tier})).unwrap_or_default()));
    }
    Ok(out)
}

async fn post_run(State(s): State<S>, Json(b): Json<RunBody>) -> Response {
    let s2 = s.clone();
    let inv = match tokio::task::spawn_blocking(move || inventory(&s2)).await {
        Ok(Ok(i)) => i,
        Ok(Err(r)) => return r,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, "inventory-task", e),
    };
    let planned = match select(&inv, &b) {
        Ok(p) => p,
        Err(e) => return err(StatusCode::NOT_FOUND, "no-such-test", e),
    };
    let n = s.seq.fetch_add(1, Ordering::SeqCst) + 1;
    let run_id = format!("r{n}-{}", now_ms());
    let run_dir = s.runs_dir().join(&run_id);
    let mut jobs = Vec::new();
    let mut specs = Vec::new();
    // Tests of a stem that ask for different env run in separate processes of the same stem.
    for p in planned {
        let mut groups: BTreeMap<Vec<(String, String)>, Vec<TestEntry>> = BTreeMap::new();
        for t in p.tests {
            groups.entry(cell_env(&s.tree, &t)).or_default().push(t);
        }
        let single_group = groups.len() == 1;
        for (env, tests) in groups {
            let idx = jobs.len();
            let exe = inv.exes[p.exe_idx].clone();
            let tiers: Vec<String> = tests.iter().flat_map(|t| t.tiers.clone()).collect::<BTreeSet<_>>().into_iter().collect();
            let container = tests.iter().any(|t| t.container);
            let dir = run_dir.join(format!("{idx:03}-{}-{}", slug(&p.krate), slug(&p.stem)));
            let art = dir.join("artifacts");
            let mut full_env: Vec<(String, String)> = CADENCE.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
            full_env.push(("OTEL_EXPORTER_OTLP_ENDPOINT".into(), OTLP.into()));
            full_env.extend(env.clone());
            full_env.push(("RDM_ARTIFACTS_DIR".into(), art.display().to_string()));
            if let ([t], true) = (&tests[..], tests.iter().all(|t| t.cells.len() == 1)) {
                let c = &t.cells[0];
                full_env.push(("I143_ACCEPTANCE_DIR".into(), dir.join("acceptance").display().to_string()));
                full_env.push(("I143_ACCEPTANCE_CELL".into(), c.cell.clone()));
            }
            let names: Vec<(String, String)> = tests.iter().map(|t| (t.exe_name.clone(), t.id.clone())).collect();
            let label = if tests.len() == 1 { tests[0].id.clone() } else { format!("{}::*{}", p.stem, if single_group { String::new() } else { format!(" [{}]", env.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" ")) }) };
            let command = format!("cd {} && {} {} --exact", exe.manifest_dir.display(), exe.path.display(), names.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(" "));
            jobs.push(Job {
                idx,
                krate: p.krate.clone(),
                stem: p.stem.clone(),
                label,
                tiers: tiers.clone(),
                container,
                state: "queued".into(),
                note: None,
                started_ms: None,
                finished_ms: None,
                wall_ms: None,
                exit_code: None,
                pid: None,
                command,
                results: vec![],
                test_ids: tests.iter().map(|t| t.id.clone()).collect(),
                artifacts_dir: art.display().to_string(),
                log_path: dir.join("output.log").display().to_string(),
                traces: None,
                counts: None,
            });
            specs.push(JobSpec { exe, names, env: full_env, timeout: timeout_for(&tiers, container), container });
        }
    }
    // Long, container and release jobs last.
    let label = match b.scope.as_str() {
        "test" => format!("test {}", b.test.clone().unwrap_or_default()),
        "stem" => format!("stem {}", b.stem.clone().unwrap_or_default()),
        "crate" => format!("crate {}", b.krate.clone().unwrap_or_default()),
        "tier" => format!("{} set", b.tier.clone().unwrap_or_default()),
        _ => "everything".to_string(),
    };
    let run = Run {
        id: run_id.clone(),
        label,
        scope: serde_json::to_value(json!({"scope": b.scope, "crate": b.krate, "stem": b.stem, "test": b.test, "tier": b.tier})).unwrap_or_default(),
        started_ms: now_ms(),
        finished_ms: None,
        parallel: s.gate.limit.load(Ordering::SeqCst),
        cancelled: false,
        tree_sha: tree_sha(&s.tree),
        jobs,
    };
    let cancel = Arc::new(AtomicBool::new(false));
    s.cancels.lock().unwrap().insert(run_id.clone(), cancel.clone());
    s.runs.lock().unwrap().push(run);
    let njobs = specs.len();
    let remaining = Arc::new(AtomicUsize::new(njobs));
    for (idx, spec) in specs.into_iter().enumerate() {
        let (s3, rid, cancel, remaining) = (s.clone(), run_id.clone(), cancel.clone(), remaining.clone());
        tokio::spawn(async move {
            run_job(s3.clone(), &rid, idx, spec, cancel).await;
            if remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                let mut runs = s3.runs.lock().unwrap();
                if let Some(r) = runs.iter_mut().find(|r| r.id == rid) {
                    r.finished_ms = Some(now_ms());
                }
            }
        });
    }
    Json(json!({"run_id": run_id, "jobs": njobs})).into_response()
}

fn update(s: &S, rid: &str, idx: usize, f: impl FnOnce(&mut Job)) {
    let mut runs = s.runs.lock().unwrap();
    if let Some(j) = runs.iter_mut().find(|r| r.id == rid).and_then(|r| r.jobs.get_mut(idx)) {
        f(j);
    }
}

fn kill_group(pid: u32) {
    let _ = std::process::Command::new("kill").args(["-9", "--", &format!("-{pid}")]).status();
}

async fn run_job(s: S, rid: &str, idx: usize, spec: JobSpec, cancel: Arc<AtomicBool>) {
    s.gate.acquire().await;
    let outcome = run_job_inner(&s, rid, idx, &spec, &cancel).await;
    s.gate.release();
    if let Err((state, note)) = outcome {
        update(&s, rid, idx, |j| {
            j.state = state.into();
            j.note = Some(note);
            j.finished_ms = Some(now_ms());
        });
    }
}

async fn run_job_inner(s: &S, rid: &str, idx: usize, spec: &JobSpec, cancel: &AtomicBool) -> Result<(), (&'static str, String)> {
    if cancel.load(Ordering::SeqCst) {
        return Err(("cancelled", "the run was cancelled before this job started".into()));
    }
    if spec.container {
        if let Err(e) = s.container_runtime.get_or_init(|| async { container_runtime().await }).await {
            return Err(("skipped", format!("needs a container runtime and has none: {e}")));
        }
    }
    if let Some((_, dir)) = spec.env.iter().find(|(k, _)| k == "RDM_RSHAPE_CONSUMER_BIN_DIR") {
        if !Path::new(dir).is_dir() {
            return Err(("blocked", format!("RDM_RSHAPE_CONSUMER_BIN_DIR {dir} does not exist: build the consumer with scripts/i143-rshape-build-consumer.sh")));
        }
    }
    let (log_path, art) = {
        let runs = s.runs.lock().unwrap();
        let j = runs.iter().find(|r| r.id == rid).and_then(|r| r.jobs.get(idx)).ok_or(("failed", "run vanished".to_string()))?;
        (PathBuf::from(&j.log_path), PathBuf::from(&j.artifacts_dir))
    };
    std::fs::create_dir_all(&art).map_err(|e| ("failed", format!("{}: {e}", art.display())))?;
    let out = std::fs::File::create(&log_path).map_err(|e| ("failed", format!("{}: {e}", log_path.display())))?;
    let errf = out.try_clone().map_err(|e| ("failed", format!("{}: {e}", log_path.display())))?;
    let mut cmd = tokio::process::Command::new(&spec.exe.path);
    cmd.current_dir(&spec.exe.manifest_dir).env("CARGO_MANIFEST_DIR", &spec.exe.manifest_dir);
    for k in SCRUBBED {
        cmd.env_remove(k);
    }
    for (k, v) in &spec.env {
        cmd.env(k, v);
    }
    for (n, _) in &spec.names {
        cmd.arg(n);
    }
    cmd.arg("--exact").stdout(errf.try_clone().map_err(|e| ("failed", e.to_string()))?).stderr(errf).stdin(std::process::Stdio::null()).process_group(0);
    let _ = out;
    let started = Instant::now();
    let mut child = cmd.spawn().map_err(|e| ("failed", format!("spawn {}: {e}", spec.exe.path.display())))?;
    let pid = child.id();
    update(s, rid, idx, |j| {
        j.state = "running".into();
        j.started_ms = Some(now_ms());
        j.pid = pid;
    });
    let deadline = tokio::time::sleep(spec.timeout);
    tokio::pin!(deadline);
    let mut timed_out = false;
    let mut was_cancelled = false;
    let status = loop {
        tokio::select! {
            st = child.wait() => break st,
            _ = &mut deadline => { timed_out = true; if let Some(p) = pid { kill_group(p); } break child.wait().await; }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {
                if cancel.load(Ordering::SeqCst) { was_cancelled = true; if let Some(p) = pid { kill_group(p); } break child.wait().await; }
            }
        }
    };
    // Whatever the test left in its group is gone with it.
    if let Some(p) = pid {
        kill_group(p);
    }
    let wall = started.elapsed().as_millis() as u64;
    let code = status.as_ref().ok().and_then(|s| s.code());
    let log = std::fs::read(&log_path).map(|b| String::from_utf8_lossy(&b).into_owned()).unwrap_or_default();
    let (mut results, counts) = parse_libtest(&log, &spec.names);
    let mut state = match (timed_out, was_cancelled, code) {
        (true, _, _) => "timed-out",
        (_, true, _) => "cancelled",
        (_, _, Some(0)) => "passed",
        _ => "failed",
    };
    let mut note = None;
    if timed_out {
        note = Some(format!("killed after {}s (the job's bound)", spec.timeout.as_secs()));
    }
    if state == "passed" {
        let passed = results.iter().filter(|r| r.status == "passed").count();
        if passed == 0 {
            state = "failed";
            note = Some(format!("exit 0 but no requested test passed (summary {counts:?}): ignored or filtered out, so nothing was proven"));
        } else if results.iter().any(|r| r.status == "not-run") {
            state = "failed";
            note = Some("exit 0 but some requested tests were not run".into());
        }
    }
    if state == "failed" && results.iter().all(|r| r.status != "failed") {
        // The process died before libtest named a failure (a crash, a signal, an abort).
        if let Some(r) = results.iter_mut().find(|r| r.status == "not-run") {
            r.message = Some(format!("the process ended without reporting this test (exit {code:?})"));
        }
    }
    let jaeger = s.jaeger.clone();
    let art2 = art.clone();
    let traces = tokio::task::spawn_blocking(move || scan_traces(&art2, &jaeger)).await.ok();
    update(s, rid, idx, |j| {
        j.state = state.into();
        j.note = note;
        j.wall_ms = Some(wall);
        j.finished_ms = Some(now_ms());
        j.exit_code = code;
        j.results = results;
        j.counts = counts;
        j.traces = traces;
    });
    Ok(())
}

async fn container_runtime() -> Result<(), String> {
    match tokio::process::Command::new("docker").args(["info", "--format", "{{.ServerVersion}}"]).output().await {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => Err(format!("docker info: {}", String::from_utf8_lossy(&o.stderr).trim())),
        Err(e) => Err(format!("docker: {e}")),
    }
}

// -- views ------------------------------------------------------------------------------------

fn summary(r: &Run) -> Value {
    let mut by: BTreeMap<&str, usize> = BTreeMap::new();
    let (mut tp, mut tf) = (0, 0);
    for j in &r.jobs {
        *by.entry(j.state.as_str()).or_default() += 1;
        tp += j.results.iter().filter(|x| x.status == "passed").count();
        tf += j.results.iter().filter(|x| x.status == "failed").count();
    }
    json!({"id": r.id, "label": r.label, "scope": r.scope, "started_ms": r.started_ms, "finished_ms": r.finished_ms, "parallel": r.parallel, "cancelled": r.cancelled, "tree_sha": r.tree_sha,
           "jobs": r.jobs.len(), "by_state": by, "tests_passed": tp, "tests_failed": tf, "running": r.finished_ms.is_none()})
}

async fn list_runs(State(s): State<S>) -> Json<Value> {
    let runs = s.runs.lock().unwrap();
    Json(json!({"runs": runs.iter().rev().map(summary).collect::<Vec<_>>()}))
}

async fn get_run(State(s): State<S>, AxPath(id): AxPath<String>) -> Response {
    let run = s.runs.lock().unwrap().iter().find(|r| r.id == id).cloned();
    let Some(run) = run else { return err(StatusCode::NOT_FOUND, "no-such-run", format!("run {id} is not in this server's history")) };
    let jobs: Vec<Value> = run
        .jobs
        .iter()
        .map(|j| {
            let mut v = serde_json::to_value(j).unwrap_or_default();
            v["output_tail"] = Value::String(tail_of(Path::new(&j.log_path), if j.state == "running" { 8 * 1024 } else { TAIL_BYTES }));
            v
        })
        .collect();
    let mut v = summary(&run);
    v["job_list"] = Value::Array(jobs);
    Json(v).into_response()
}

async fn cancel_run(State(s): State<S>, AxPath(id): AxPath<String>) -> Response {
    let Some(flag) = s.cancels.lock().unwrap().get(&id).cloned() else { return err(StatusCode::NOT_FOUND, "no-such-run", format!("run {id} is not in this server's history")) };
    flag.store(true, Ordering::SeqCst);
    if let Some(r) = s.runs.lock().unwrap().iter_mut().find(|r| r.id == id) {
        r.cancelled = true;
    }
    Json(json!({"cancelled": id})).into_response()
}

#[derive(Deserialize)]
struct FileQuery {
    path: String,
}

/// An evidence file of a run, `path` relative to the runs directory; text, last 2 MiB.
async fn get_file(State(s): State<S>, Query(q): Query<FileQuery>) -> Response {
    let root = match s.runs_dir().canonicalize() {
        Ok(r) => r,
        Err(e) => return err(StatusCode::NOT_FOUND, "no-runs-dir", format!("{}: {e}", s.runs_dir().display())),
    };
    let full = match root.join(&q.path).canonicalize() {
        Ok(f) if f.starts_with(&root) && f.is_file() => f,
        Ok(_) => return err(StatusCode::BAD_REQUEST, "outside-runs-dir", "the path is not a file under the runs directory"),
        Err(e) => return err(StatusCode::NOT_FOUND, "no-such-file", format!("{}: {e}", q.path)),
    };
    let body = tail_of(&full, 2 * 1024 * 1024);
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

async fn job_files(State(s): State<S>, AxPath((id, idx)): AxPath<(String, usize)>) -> Response {
    let dir = {
        let runs = s.runs.lock().unwrap();
        runs.iter().find(|r| r.id == id).and_then(|r| r.jobs.get(idx)).map(|j| PathBuf::from(&j.log_path).parent().map(Path::to_path_buf).unwrap_or_default())
    };
    let Some(dir) = dir else { return err(StatusCode::NOT_FOUND, "no-such-job", format!("run {id} job {idx}")) };
    let root = s.runs_dir();
    let files = tokio::task::spawn_blocking(move || {
        let mut all = Vec::new();
        walk(&dir, &mut all, 0);
        all.sort();
        all.into_iter()
            .take(600)
            .map(|p| json!({"path": p.strip_prefix(&root).unwrap_or(&p).display().to_string(), "bytes": std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0)}))
            .collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();
    Json(json!({"files": files})).into_response()
}

/// The routes of the Tests tab; the state is its own.
pub fn router() -> Router {
    let tree = PathBuf::from(std::env::var("RDM_TESTS_TREE").ok().filter(|t| !t.trim().is_empty()).unwrap_or_else(|| DEFAULT_TREE.into()));
    let parallel = std::env::var("RDM_TESTS_PARALLEL").ok().and_then(|v| v.parse().ok()).filter(|n: &usize| *n >= 1).unwrap_or(8);
    let state: S = Arc::new(Inner {
        tree,
        jaeger: std::env::var("RDM_TESTS_JAEGER_UI_URL").unwrap_or_else(|_| JAEGER_UI.into()),
        gate: Gate { limit: AtomicUsize::new(parallel), running: Mutex::new(0), notify: Notify::new() },
        runs: Mutex::new(vec![]),
        cancels: Mutex::new(HashMap::new()),
        seq: AtomicUsize::new(0),
        build: Mutex::new(BuildState { state: "idle".into(), ..Default::default() }),
        inventory: Mutex::new(None),
        container_runtime: tokio::sync::OnceCell::new(),
    });
    Router::new()
        .route("/api/tests/inventory", get(get_inventory))
        .route("/api/tests/build", get(get_build).post(start_build))
        .route("/api/tests/config", get(get_config).post(set_config))
        .route("/api/tests/run", post(post_run))
        .route("/api/tests/runs", get(list_runs))
        .route("/api/tests/runs/{id}", get(get_run))
        .route("/api/tests/runs/{id}/cancel", post(cancel_run))
        .route("/api/tests/runs/{id}/jobs/{idx}/files", get(job_files))
        .route("/api/tests/file", get(get_file))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_command_parses_env_crate_and_stem_qualified_id() {
        let (env, krate, id) = parse_command("MESH_SPAWN_TYPE=process RDM_ARTIFACTS_DIR=target/x cargo test -p rafka-test-scenario --test main a_stem::a_test -- --exact").unwrap();
        assert_eq!(krate, "rafka-test-scenario");
        assert_eq!(id, "a_stem::a_test");
        assert_eq!(env, vec![("MESH_SPAWN_TYPE".to_string(), "process".to_string()), ("RDM_ARTIFACTS_DIR".to_string(), "target/x".to_string())]);
    }

    #[test]
    fn registry_command_on_a_file_per_exe_target_qualifies_the_id_with_the_target() {
        let (_, krate, id) = parse_command("cargo test -p rafka-node-rpc --test pool some_test -- --exact").unwrap();
        assert_eq!((krate.as_str(), id.as_str()), ("rafka-node-rpc", "pool::some_test"));
    }

    #[test]
    fn registry_command_that_is_not_cargo_test_is_refused_by_name() {
        let e = parse_command("scripts/foo.sh bar").unwrap_err();
        assert!(e.contains("not a `cargo test` command"), "{e}");
    }

    #[test]
    fn libtest_log_gives_each_test_its_own_status_and_panic() {
        let log = "running 3 tests\ntest s::a ... ok\ntest s::b ... FAILED\ntest s::c ... ignored\n\nfailures:\n\n---- s::b stdout ----\nthread 's::b' panicked at x.rs:1:2:\nboom: left 1 right 2\nnote: run with `RUST_BACKTRACE=1`\n\nfailures:\n    s::b\n\ntest result: FAILED. 1 passed; 1 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.10s\n";
        let names: Vec<(String, String)> = ["s::a", "s::b", "s::c", "s::d"].iter().map(|n| (n.to_string(), n.to_string())).collect();
        let (out, summary) = parse_libtest(log, &names);
        let st: Vec<_> = out.iter().map(|o| o.status.as_str()).collect();
        assert_eq!(st, ["passed", "failed", "ignored", "not-run"]);
        assert!(out[1].message.as_deref().unwrap().contains("boom: left 1 right 2"), "{:?}", out[1].message);
        assert_eq!(summary, Some((1, 1, 1)));
    }

    #[test]
    fn libtest_line_split_by_a_child_writing_to_the_same_fd_still_names_the_failure() {
        let log = "test s::b ... node says hi\nFAILED\n\n---- s::b stdout ----\nthread 'x' panicked at y.rs:1:1:\nno\n\nfailures:\n    s::b\n";
        let (out, _) = parse_libtest(log, &[("s::b".into(), "s::b".into())]);
        assert_eq!(out[0].status, "failed");
    }
}
