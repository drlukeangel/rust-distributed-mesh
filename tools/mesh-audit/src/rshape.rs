//! i143.e11.s6 (#2949): the R-shape export qualification audit.
//!
//! Reads the qualification matrix, the registry beside it, every owner receipt and every R-shape
//! receipt, and answers one question: is the evidence complete, at one clean candidate, on the
//! exact twenty-node shape, for both providers? Every failure is a violation named
//! `rshape-qualification-<rule>`; the audit returns all of them, never the first. A clean report
//! is prerequisite evidence consumed by the final parity/export certificate, not that certificate.

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The R-shape job that runs this audit; its own receipt cannot be an input to itself.
pub const EXPORT_JOB: &str = "i143-rshape-export-qualification";

#[derive(Debug, Clone)]
pub struct Args {
    pub matrix: PathBuf,
    pub evidence: PathBuf,
    pub owner_receipts: PathBuf,
    pub output: PathBuf,
    /// Receipt artifact paths and matrix `dir`s are relative to this root.
    pub root: PathBuf,
    /// The candidate every receipt must carry; default `RDM_CANDIDATE_SHA`, else `git rev-parse HEAD` in the root.
    pub candidate: Option<String>,
    /// A `parity-scan --json` report; default `<evidence>/parity.json`.
    pub parity: Option<PathBuf>,
    /// The registry; default `i143-acceptance-jobs.json` beside the matrix.
    pub registry: Option<PathBuf>,
    /// The consumer build manifest; default `<evidence>/consumer-build/manifest.json`.
    pub consumer_manifest: Option<PathBuf>,
}

pub struct Outcome {
    pub violations: Vec<String>,
    pub report: Value,
    pub checks: Vec<Value>,
}

pub fn sha_file(p: &Path) -> Result<String, String> {
    let bytes = std::fs::read(p).map_err(|e| format!("{}: {e}", p.display()))?;
    Ok(Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect())
}

fn read_json(p: &Path) -> Result<Value, String> {
    let t = std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display()))?;
    serde_json::from_str(&t).map_err(|e| format!("{} is not JSON: {e}", p.display()))
}

fn st<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or_default()
}

fn resolve(root: &Path, p: &str) -> PathBuf {
    let p = Path::new(p);
    if p.is_absolute() { p.to_path_buf() } else { root.join(p) }
}

struct Audit {
    v: Vec<String>,
    checks: Vec<Value>,
}

impl Audit {
    fn fail(&mut self, rule: &str, detail: String) {
        self.v.push(format!("rshape-qualification-{rule}: {detail}"));
    }
    fn check(&mut self, name: &str, before: usize) {
        self.checks.push(json!({"invariant": name, "holds": self.v.len() == before}));
    }
}

/// One receipt, judged: outcome, cleanliness, candidate, every cell ok and every artifact hashed.
fn judge_receipt(a: &mut Audit, root: &Path, label: &str, r: &Value, candidate: &str) {
    if st(r, "outcome") != "ok" {
        a.fail("receipt-not-ok", format!("{label}: outcome is `{}`", st(r, "outcome")));
    }
    if r["dirty_paths"].as_u64() != Some(0) {
        a.fail("dirty", format!("{label}: ran on a dirty tree ({} paths)", r["dirty_paths"]));
    }
    if st(r, "source_sha") != candidate {
        a.fail("stale-sha", format!("{label}: source sha `{}` is not the candidate `{candidate}`", st(r, "source_sha")));
    }
    for c in r["cells"].as_array().into_iter().flatten() {
        if st(c, "outcome") != "ok" {
            a.fail("receipt-not-ok", format!("{label}/{}: cell outcome is `{}`: {}", st(c, "name"), st(c, "outcome"), c["refusal"]));
        }
        match c["artifacts"].as_object() {
            None => a.fail("hash-mismatch", format!("{label}/{}: the cell lists no artifacts", st(c, "name"))),
            Some(m) => {
                for (path, want) in m {
                    match sha_file(&resolve(root, path)) {
                        Err(e) => a.fail("hash-mismatch", format!("{label}/{}: artifact missing: {e}", st(c, "name"))),
                        Ok(h) if Some(h.as_str()) != want.as_str() => a.fail("hash-mismatch", format!("{label}/{}: artifact {path} hashes to {h}, the receipt records {want}", st(c, "name"))),
                        Ok(_) => {}
                    }
                }
            }
        }
    }
}

fn receipt_files(dir: &Path) -> Vec<(String, PathBuf)> {
    let mut v: Vec<(String, PathBuf)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
        .map(|e| (e.path().file_stem().unwrap().to_string_lossy().to_string(), e.path()))
        .collect();
    v.sort();
    v
}

pub fn qualify(args: &Args) -> Outcome {
    let mut a = Audit { v: vec![], checks: vec![] };
    let root = &args.root;
    let mut report = json!({"tool": "rshape-qualification", "matrix": args.matrix, "evidence": args.evidence, "owner_receipts": args.owner_receipts});

    let matrix = match read_json(&args.matrix) {
        Ok(m) => m,
        Err(e) => {
            a.fail("matrix-unreadable", e);
            return Outcome { violations: a.v, report, checks: a.checks };
        }
    };
    let registry_path = args.registry.clone().unwrap_or_else(|| args.matrix.with_file_name("i143-acceptance-jobs.json"));
    let registry = match read_json(&registry_path) {
        Ok(m) => m,
        Err(e) => {
            a.fail("registry-unreadable", e);
            return Outcome { violations: a.v, report, checks: a.checks };
        }
    };
    let candidate = args
        .candidate
        .clone()
        .or_else(|| std::env::var("RDM_CANDIDATE_SHA").ok())
        .or_else(|| {
            let o = std::process::Command::new("git").arg("-C").arg(root).args(["rev-parse", "HEAD"]).output().ok()?;
            o.status.success().then(|| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .unwrap_or_default();
    if candidate.is_empty() {
        a.fail("candidate-unknown", "no --candidate-sha, no RDM_CANDIDATE_SHA and no git HEAD in the root".into());
    }
    report["candidate_sha"] = json!(candidate);

    // 1. Owner receipts: every registered owner job has its receipt; a receipt of no registered job is refused.
    let before = a.v.len();
    let owner_jobs: BTreeSet<String> = registry["jobs"].as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default();
    let rshape_jobs: BTreeSet<String> = registry["rshape_jobs"].as_object().map(|m| m.keys().filter(|k| !k.starts_with('_')).cloned().collect()).unwrap_or_default();
    let mut owner_seen = 0;
    for job in &owner_jobs {
        let p = args.owner_receipts.join(format!("{job}.json"));
        match read_json(&p) {
            Err(e) => a.fail("missing-receipt", format!("owner job {job} has no readable receipt: {e}")),
            Ok(r) => {
                owner_seen += 1;
                if st(&r, "job") != job {
                    a.fail("unknown-job", format!("{} names job `{}`, not `{job}`", p.display(), st(&r, "job")));
                }
                judge_receipt(&mut a, root, &format!("owner job {job}"), &r, &candidate);
            }
        }
    }
    for (stem, p) in receipt_files(&args.owner_receipts) {
        if !owner_jobs.contains(&stem) {
            a.fail("unknown-job", format!("{} is a receipt of job `{stem}`, which the registry does not list under `jobs`", p.display()));
        }
    }
    a.check("every registered owner job has one clean receipt at the candidate, and no receipt names an unregistered job", before);
    report["owner_jobs"] = json!({"registered": owner_jobs.len(), "receipts": owner_seen});

    // 2. R-shape receipts for every required job (canonical and static cells), judged and cross-checked to the matrix.
    let before = a.v.len();
    let cells: Vec<&Value> = matrix["cells"].as_array().into_iter().flatten().collect();
    let mut required: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for c in &cells {
        if matches!(st(c, "tier"), "canonical" | "static") && st(c, "job") != EXPORT_JOB {
            required.entry(st(c, "job").to_string()).or_default().push(c);
        }
        if !rshape_jobs.contains(st(c, "job")) && c.get("pending").is_none() {
            a.fail("unknown-job", format!("matrix cell {} names job `{}` which the registry does not list under `rshape_jobs`", st(c, "test"), st(c, "job")));
        }
    }
    let jobs_dir = args.evidence.join("jobs");
    for (stem, p) in receipt_files(&jobs_dir) {
        if stem != EXPORT_JOB && !rshape_jobs.contains(&stem) {
            a.fail("unknown-job", format!("{} is a receipt of job `{stem}`, which the registry does not list under `rshape_jobs`", p.display()));
        }
    }
    let canon = &matrix["shapes"]["canonical"];
    let mut result_candidates: BTreeMap<String, String> = BTreeMap::new();
    let mut cells_judged = 0;
    for (job, jcells) in &required {
        let p = jobs_dir.join(format!("{job}.json"));
        let r = match read_json(&p) {
            Ok(r) => r,
            Err(e) => {
                a.fail("missing-receipt", format!("R-shape job {job} has no readable receipt: {e}"));
                continue;
            }
        };
        if st(&r, "job") != job {
            a.fail("unknown-job", format!("{} names job `{}`, not `{job}`", p.display(), st(&r, "job")));
        }
        judge_receipt(&mut a, root, &format!("R-shape job {job}"), &r, &candidate);
        for c in jcells {
            let test = st(c, "test");
            if !r["cells"].as_array().into_iter().flatten().any(|rc| st(rc, "name") == test) {
                a.fail("missing-cell", format!("{job} receipt lists no cell {test}"));
                continue;
            }
            cells_judged += 1;
            let dir = resolve(root, st(c, "dir"));
            let provider = st(c, "provider");
            if matches!(provider, "process" | "container") {
                match read_json(&dir.join("manifest.json")) {
                    Err(e) => a.fail("wrong-provider", format!("{job}/{test}: no cell manifest to read the provider from: {e}")),
                    Ok(m) if st(&m, "provider") != provider => a.fail("wrong-provider", format!("{job}/{test}: the matrix requires provider `{provider}`, the cell ran on `{}`", st(&m, "provider"))),
                    Ok(_) => {}
                }
            }
            if st(c, "tier") == "canonical" {
                match read_json(&dir.join("topology-initial.json")) {
                    Err(e) => a.fail("wrong-shape", format!("{job}/{test}: no topology-initial.json to read the shape from: {e}")),
                    Ok(t) => {
                        let sh = &t["shape_requested"];
                        if st(sh, "tier") != "canonical" || sh["per_mesh"] != canon["per_mesh"] || sh["total"] != canon["total"] || sh["meshes"].as_array().map(|m| m.len() as u64) != canon["meshes"].as_u64() {
                            a.fail("wrong-shape", format!("{job}/{test}: shape {sh} is not the canonical {} meshes x {} = {} nodes", canon["meshes"], canon["per_mesh"], canon["total"]));
                        }
                    }
                }
                if let Ok(res) = read_json(&dir.join("result.json")) {
                    if let Some(sha) = res["rdm_candidate_sha"].as_str() {
                        result_candidates.insert(format!("{job}/{test}"), sha.to_string());
                    }
                }
            }
        }
    }
    a.check("every canonical and static cell has a clean receipt at the candidate, its provider and the twenty-node shape", before);
    report["rshape_cells_judged"] = json!(cells_judged);

    // 3. One imported candidate across every runtime result, equal to the consumer build's.
    let before = a.v.len();
    let cm = args.consumer_manifest.clone().unwrap_or_else(|| args.evidence.join("consumer-build/manifest.json"));
    let imported = match read_json(&cm) {
        Ok(m) => st(&m, "candidate_sha").to_string(),
        Err(e) => {
            a.fail("stale-sha", format!("no consumer build manifest to name the imported candidate: {e}"));
            String::new()
        }
    };
    let distinct: BTreeSet<&String> = result_candidates.values().collect();
    if distinct.len() > 1 {
        a.fail("stale-sha", format!("the cells' results name {} imported candidates: {distinct:?}", distinct.len()));
    }
    for (cell, sha) in &result_candidates {
        if !imported.is_empty() && *sha != imported {
            a.fail("stale-sha", format!("{cell}: its result imported candidate `{sha}`, the consumer build manifest names `{imported}`"));
        }
    }
    a.check("every runtime result imported the one candidate the consumer build manifest names", before);
    report["imported_candidate_sha"] = json!(imported);

    // 4. Soaks: each of S1-S5 on each provider ran its whole duration; each provider replayed all five.
    let before = a.v.len();
    for n in 1..=5 {
        let id = format!("S{n}");
        let so = &matrix["soaks"][&id];
        for p in ["process", "container"] {
            let Some(c) = cells.iter().find(|c| st(c, "test") == st(so, "test") && st(c, "provider") == p && st(c, "tier") == "canonical") else {
                a.fail("missing-cell", format!("{id} has no canonical {p} soak cell in the matrix"));
                continue;
            };
            match read_json(&resolve(root, st(c, "dir")).join("result.json")) {
                Err(e) => a.fail("missing-cell", format!("{id} {p}: no soak result: {e}")),
                Ok(res) => {
                    if res["soak_seconds"].as_u64() < so["seconds"].as_u64() || res["qualifies_export"] != json!(true) {
                        a.fail("soak-too-short", format!("{id} {p}: ran {} s, qualifies_export={}; the matrix requires {} s", res["soak_seconds"], res["qualifies_export"], so["seconds"]));
                    }
                }
            }
        }
    }
    for p in ["process", "container"] {
        let Some(rc) = cells.iter().find(|c| st(c, "test") == "mock_soak_replay_reproduces_qualified_invariants" && st(c, "provider") == p) else {
            a.fail("replay-incomplete", format!("the matrix has no {p} replay cell"));
            continue;
        };
        let man = match read_json(&resolve(root, st(rc, "dir")).join("replay-manifest.json")) {
            Ok(m) => m,
            Err(e) => {
                a.fail("replay-incomplete", format!("{p}: no replay manifest: {e}"));
                continue;
            }
        };
        if st(&man, "provider") != p {
            a.fail("wrong-provider", format!("{p} replay manifest names provider `{}`", st(&man, "provider")));
        }
        for n in 1..=5 {
            let id = format!("S{n}");
            let so = &matrix["soaks"][&id];
            let want = cells.iter().find(|c| st(c, "test") == st(so, "test") && st(c, "provider") == p).map(|c| st(c, "dir")).unwrap_or_default();
            let Some(e) = man["stories"].as_array().into_iter().flatten().find(|e| st(e, "story") == id) else {
                a.fail("replay-incomplete", format!("{p} replay manifest has no {id}"));
                continue;
            };
            if st(e, "source_dir") != want {
                a.fail("replay-source-mismatch", format!("{p} {id} replayed `{}`, the provider's soak run is `{want}`", st(e, "source_dir")));
                continue;
            }
            match sha_file(&resolve(root, want).join("result.json")) {
                Ok(h) if h == st(e, "source_result_sha256") => {}
                other => a.fail("replay-source-mismatch", format!("{p} {id}: the replay recorded source hash {}, the run's result.json is {other:?}", e["source_result_sha256"])),
            }
            if e["seed"] != matrix["seed"] || e["reproduced"] != json!(true) {
                a.fail("replay-incomplete", format!("{p} {id}: seed {} (matrix {}), reproduced={}", e["seed"], matrix["seed"], e["reproduced"]));
            }
        }
    }
    a.check("S1-S5 ran their full duration on both providers and each provider replayed all five of its own runs", before);

    // 5. Parity.
    let before = a.v.len();
    let pp = args.parity.clone().unwrap_or_else(|| args.evidence.join("parity.json"));
    match read_json(&pp) {
        Err(e) => a.fail("parity-ineligible", format!("no parity report: {e}")),
        Ok(r) if r["eligible"] != json!(true) => a.fail("parity-ineligible", format!("{} reports eligible={}, mirror_pending={}", pp.display(), r["eligible"], r["mirror_pending"])),
        Ok(_) => {}
    }
    a.check("the current parity report is eligible", before);

    report["eligible"] = json!(a.v.is_empty());
    report["violations"] = json!(a.v);
    Outcome { violations: a.v, report, checks: a.checks }
}
