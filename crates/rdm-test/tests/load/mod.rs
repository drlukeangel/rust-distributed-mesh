//! Load: RDM measured at a size (docs/plans/proposal/load-testing.md, Q265). These cells measure; they
//! assert behaviour nowhere another cell does. Each run writes one row per measure to
//! `$HOME/rust-distributed-mesh-deploy/load/<sha>.jsonl` and fails when a measure with a target misses it.
//! They are ignored by the gate (a run takes minutes) and run by `scripts/rdm-load.sh <profile>`.

use rdm_test::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

/// A profile's shape (load-testing.md "Profiles"): meshes, node-admins and rpc nodes per mesh.
struct Profile {
    name: &'static str,
    meshes: u32,
    admins: u32,
    rpcs: u32,
    /// M1's target at this profile.
    bring_up: Duration,
}

const L1: Profile = Profile { name: "L1", meshes: 2, admins: 2, rpcs: 8, bring_up: Duration::from_secs(15) };

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "load".into(),
        subfeature: "profile".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// One measure's row: the profile, the measure, its samples' p50/p99/max, the target and the outcome.
struct Rows {
    sha: String,
    profile: &'static str,
    build: String,
    /// Whether the nodes wrote span evidence (`RDM_EVIDENCE_DIR`): a row is compared only with one
    /// taken the same way (load-testing.md "Conditions").
    evidence: bool,
    context: Value,
    rows: Vec<Value>,
    missed: Vec<String>,
}

impl Rows {
    fn new(profile: &'static str, evidence: bool) -> Self {
        let sha = rdm_test::faults::candidate_sha();
        let build = if rdm_test::estate::bin_dir().to_string_lossy().contains("/release") { "release" } else { "debug" }.to_string();
        Self { sha, profile, build, evidence, context: box_context(), rows: Vec::new(), missed: Vec::new() }
    }

    /// A measure of `samples` (milliseconds), with an optional target on its max.
    fn time(&mut self, measure: &str, what: &str, mut samples: Vec<f64>, target_ms: Option<f64>, slowest_trace: Option<String>) {
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pick = |q: f64| samples.get(((samples.len() as f64 - 1.0) * q).round() as usize).copied().unwrap_or(f64::NAN);
        let max = samples.last().copied().unwrap_or(f64::NAN);
        let pass = target_ms.map(|t| max <= t);
        if pass == Some(false) {
            self.missed.push(format!("{measure} {what}: max {max:.0} ms over the target {:.0} ms", target_ms.unwrap()));
        }
        self.rows.push(json!({"measure": measure, "what": what, "unit": "ms", "n": samples.len(), "p50": pick(0.5), "p99": pick(0.99), "max": max, "target": target_ms, "pass": pass, "slowest_trace": slowest_trace}));
    }

    /// A measure of one value with an optional upper target.
    fn value(&mut self, measure: &str, what: &str, unit: &str, v: f64, target: Option<f64>) {
        let pass = target.map(|t| v <= t);
        if pass == Some(false) {
            self.missed.push(format!("{measure} {what}: {v:.3} {unit} over the target {:.3}", target.unwrap()));
        }
        self.rows.push(json!({"measure": measure, "what": what, "unit": unit, "value": v, "target": target, "pass": pass}));
    }

    /// Append every row to the SHA's file and print the table; panics naming every missed target.
    fn finish(mut self) {
        let end = box_context();
        let dir = std::path::PathBuf::from(std::env::var("HOME").unwrap()).join("rust-distributed-mesh-deploy/load");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.jsonl", &self.sha[..9.min(self.sha.len())]));
        let mut out = String::new();
        for r in self.rows.iter_mut() {
            r["sha"] = json!(self.sha);
            r["profile"] = json!(self.profile);
            r["build"] = json!(self.build);
            r["evidence"] = json!(self.evidence);
            r["box_start"] = self.context.clone();
            r["box_end"] = end.clone();
            out.push_str(&r.to_string());
            out.push('\n');
            println!("[load] {} {} {} {:<4} {:<28} {}", self.profile, self.build, if self.evidence { "evidence" } else { "no-evidence" }, s(&r["measure"]), s(&r["what"]), summary(r));
        }
        use std::io::Write as _;
        std::fs::OpenOptions::new().create(true).append(true).open(&path).unwrap().write_all(out.as_bytes()).unwrap();
        println!("[load] rows: {}", path.display());
        assert!(self.missed.is_empty(), "{} missed {} target(s):\n{}", self.profile, self.missed.len(), self.missed.join("\n"));
    }
}

fn s(v: &Value) -> String {
    v.as_str().map(String::from).unwrap_or_else(|| v.to_string())
}

fn summary(r: &Value) -> String {
    let pass = match r["pass"].as_bool() {
        Some(true) => "pass",
        Some(false) => "MISS",
        None => "reported",
    };
    if r["value"].is_null() {
        format!("p50 {:>8.0} p99 {:>8.0} max {:>8.0} {}  target {}  {pass}", r["p50"].as_f64().unwrap_or(f64::NAN), r["p99"].as_f64().unwrap_or(f64::NAN), r["max"].as_f64().unwrap_or(f64::NAN), s(&r["unit"]), s(&r["target"]))
    } else {
        format!("{:.3} {}  target {}  {pass}", r["value"].as_f64().unwrap_or(f64::NAN), s(&r["unit"]), s(&r["target"]))
    }
}

/// The box as the run met it (load-testing.md "Conditions"): its load average and the lanes holding a process.
fn box_context() -> Value {
    let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    let busy: Vec<String> = std::process::Command::new("bash")
        .arg("-c")
        .arg("$HOME/rust-distributed-mesh/scripts/lane-status.sh 2>/dev/null | awk 'NR>1 && $5>0 {print $1}'")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).split_whitespace().map(String::from).collect())
        .unwrap_or_default();
    json!({"loadavg": load.split_whitespace().take(3).collect::<Vec<_>>().join(" "), "busy_lanes": busy})
}

/// CPU ticks (user + system) of `pid`, from `/proc/<pid>/stat`.
fn cpu_ticks(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

/// Resident memory of `pid` in GB, from `/proc/<pid>/statm`.
fn rss_gb(pid: u32) -> Option<f64> {
    let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    let pages: f64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096.0 / 1e9)
}

/// The role of a live runtime from its directory name (`mesh1.admin.1-<id>`, `mesh2.rpc.3-<id>`).
fn role_of(dir: &std::path::Path) -> &'static str {
    let name = dir.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
    if name.contains(".admin.") {
        "node-admin"
    } else {
        "rpc-node"
    }
}

/// M8: idle CPU (cores) and RSS (GB) of every runtime over `window`, the largest per role.
async fn idle_cost(estate: &Estate, window: Duration) -> Vec<(&'static str, f64, f64)> {
    let runtimes = estate.live_runtimes();
    let before: Vec<(&'static str, u32, u64)> = runtimes.iter().filter_map(|(d, p)| cpu_ticks(*p).map(|t| (role_of(d), *p, t))).collect();
    tokio::time::sleep(window).await;
    let hz = 100.0;
    let mut worst: std::collections::BTreeMap<&'static str, (f64, f64)> = Default::default();
    for (role, pid, t0) in before {
        let (Some(t1), Some(rss)) = (cpu_ticks(pid), rss_gb(pid)) else { continue };
        let cores = (t1.saturating_sub(t0)) as f64 / hz / window.as_secs_f64();
        let w = worst.entry(role).or_insert((0.0, 0.0));
        w.0 = w.0.max(cores);
        w.1 = w.1.max(rss);
    }
    worst.into_iter().map(|(r, (c, m))| (r, c, m)).collect()
}

/// L1 (load-testing.md Steps, 1): bring-up (M1), create rate (M2, reported), one-node Build latency
/// (M5, 20 samples, a target on the max) and idle cost (M8), on the 2-mesh, 20-node profile.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh"]
async fn load_profile_l1_baseline() {
    run_profile(&L1, "load_profile_l1_baseline", true).await;
}

/// L1 with no span evidence written: the pass M8 (and M6, as it is added) is read from.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh --no-evidence"]
async fn load_profile_l1_no_evidence() {
    run_profile(&L1, "load_profile_l1_no_evidence", false).await;
}

/// Run profile `p`. With `evidence` off no node writes span evidence (load-testing.md "Conditions": the
/// pass M6 and M8 are read from); M2, the one measure counted from the spans, is then not reported.
async fn run_profile(p: &Profile, test: &str, evidence: bool) {
    let mut rows = Rows::new(p.name, evidence);
    let meshes: Vec<String> = (1..=p.meshes).map(|i| format!("mesh{i}")).collect();
    let shape: Vec<(&str, u32, u32)> = meshes.iter().map(|m| (m.as_str(), p.admins, p.rpcs)).collect();

    // M1, M2: the Day-0 Build of the whole shape, submitted to the bootstrap admin, to the fabric's ready.
    let t0 = Instant::now();
    let mut estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", if evidence { &[] } else { &[("RDM_EVIDENCE_DIR", "")] }).await;
    let mut stream = estate.submit_shape("fabric1", &shape).await;
    let _ = Estate::until_complete(&mut stream).await;
    estate.settles_to(&shape, Duration::from_secs(300)).await;
    estate.until_fabric_ready(Duration::from_secs(300)).await;
    let bring_up = t0.elapsed();
    rows.time("M1", "bring-up, launch to ready", vec![bring_up.as_millis() as f64], Some(p.bring_up.as_millis() as f64), None);
    if evidence {
        let spans = estate.spans();
        let creates = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["step"] == "Complete" && sp["attributes"]["outcome"] == "complete").count();
        rows.value("M2", "node creates per second", "creates/s", creates as f64 / bring_up.as_secs_f64(), None);
    }

    // M8: idle cost once settled.
    tokio::time::sleep(Duration::from_secs(10)).await;
    for (role, cores, gb) in idle_cost(&estate, Duration::from_secs(20)).await {
        rows.value("M8", &format!("{role} idle cpu"), "cores", cores, Some(0.10));
        rows.value("M8", &format!("{role} idle rss"), "GB", gb, Some(0.5));
    }

    // M5: one node added then removed, each its own Build, accepted to complete; 20 samples each.
    let mut grow = Vec::new();
    let mut shrink = Vec::new();
    let bigger: Vec<(&str, u32, u32)> = shape.iter().enumerate().map(|(i, (m, a, r))| if i == 0 { (*m, *a, r + 1) } else { (*m, *a, *r) }).collect();
    for _ in 0..20 {
        let t = Instant::now();
        let mut s = estate.submit_shape("fabric1", &bigger).await;
        let _ = Estate::until_complete(&mut s).await;
        grow.push(t.elapsed().as_millis() as f64);
        let t = Instant::now();
        let mut s = estate.submit_shape("fabric1", &shape).await;
        let _ = Estate::until_complete(&mut s).await;
        shrink.push(t.elapsed().as_millis() as f64);
    }
    rows.time("M5", "one node.create Build", grow, Some(5000.0), None);
    rows.time("M5", "one node.delete Build", shrink, Some(5000.0), None);

    estate.stop().await;
    rows.finish();
}
