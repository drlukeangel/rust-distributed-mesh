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

    /// A rate with a lower target: it passes at or above `floor`.
    fn at_least(&mut self, measure: &str, what: &str, unit: &str, v: f64, floor: f64) {
        let pass = v >= floor;
        if !pass {
            self.missed.push(format!("{measure} {what}: {v:.1} {unit} under the target {floor:.1}"));
        }
        self.rows.push(json!({"measure": measure, "what": what, "unit": unit, "value": v, "target_at_least": floor, "pass": pass}));
    }

    /// A measure of `samples` (milliseconds) with a target on its p99, for a measure with hundreds of
    /// samples or more (load-testing.md "Samples": a p99 target only at 100 samples).
    fn time_p99(&mut self, measure: &str, what: &str, samples: Vec<f64>, target_ms: f64) {
        let n = self.rows.len();
        self.time(measure, what, samples, None, None);
        let p99 = self.rows[n]["p99"].as_f64().unwrap_or(f64::NAN);
        let pass = samples_enough(&self.rows[n]) && p99 <= target_ms;
        if !pass {
            self.missed.push(format!("{measure} {what}: p99 {p99:.0} ms over the target {target_ms:.0} ms (n {})", self.rows[n]["n"]));
        }
        self.rows[n]["target_p99"] = json!(target_ms);
        self.rows[n]["pass"] = json!(pass);
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
            r["cadence"] = cadence();
            r["profiled"] = json!(std::env::var_os("RDM_LOAD_PERF").is_some());
            r["box_start"] = self.context.clone();
            r["box_end"] = end.clone();
            out.push_str(&r.to_string());
            out.push('\n');
            println!("[load] {} {} {} gossip {}/{} {:<4} {:<28} {}", self.profile, self.build, if self.evidence { "evidence" } else { "no-evidence" }, s(&r["cadence"]["gossip_ms"]), s(&r["cadence"]["backbone_ms"]), s(&r["measure"]), s(&r["what"]), summary(r));
        }
        use std::io::Write as _;
        std::fs::OpenOptions::new().create(true).append(true).open(&path).unwrap().write_all(out.as_bytes()).unwrap();
        println!("[load] rows: {}", path.display());
        assert!(self.missed.is_empty(), "{} missed {} target(s):\n{}", self.profile, self.missed.len(), self.missed.join("\n"));
    }
}

/// The cadence the run's nodes ran at (load-testing.md "Conditions": a changed cadence is a new baseline,
/// never a comparison), as the environment the estate hands every node names it.
fn cadence() -> Value {
    let ms = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
    json!({"staleness_ms": ms("RDM_STALENESS_MS"), "gossip_ms": ms("RDM_GOSSIP_INTERVAL_MS"), "backbone_ms": ms("RDM_BACKBONE_INTERVAL_MS")})
}

fn samples_enough(row: &Value) -> bool {
    row["n"].as_u64().unwrap_or(0) >= 100
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
    let target = if !r["target_at_least"].is_null() {
        format!("at least {}", s(&r["target_at_least"]))
    } else if !r["target_p99"].is_null() {
        format!("p99 {}", s(&r["target_p99"]))
    } else {
        s(&r["target"])
    };
    if r["value"].is_null() {
        format!("n {} p50 {:>8.0} p99 {:>8.0} max {:>8.0} {}  target {target}  {pass}", r["n"], r["p50"].as_f64().unwrap_or(f64::NAN), r["p99"].as_f64().unwrap_or(f64::NAN), r["max"].as_f64().unwrap_or(f64::NAN), s(&r["unit"]))
    } else {
        format!("{:.3} {}  target {target}  {pass}", r["value"].as_f64().unwrap_or(f64::NAN), s(&r["unit"]))
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
/// With `RDM_LOAD_PERF` naming a directory, a CPU profile of one process of each role over the idle window
/// (`perf record -g`, `<dir>/<role>.<pid>.perf.data`): the flame an idle cost over its baseline is read
/// from (load-testing.md: a finding is read as a flame before any number moves). The profiler's own cost
/// is in that run's M8, which `Rows` marks `profiled`.
fn profile_one_per_role(procs: &[(&'static str, u32, u64)], window: Duration) -> Vec<std::process::Child> {
    let mut seen = std::collections::BTreeSet::new();
    procs.iter().filter(|(role, ..)| seen.insert(*role)).filter_map(|(role, pid, _)| profile_one(role, *pid, window)).collect()
}

/// With `RDM_LOAD_PERF` naming a directory, `pid`'s CPU profile over `window` into
/// `<dir>/<label>.<pid>.perf.data`; `None` without it.
fn profile_one(label: &str, pid: u32, window: Duration) -> Option<std::process::Child> {
    let dir = std::env::var("RDM_LOAD_PERF").ok()?;
    std::fs::create_dir_all(&dir).unwrap();
    let out = format!("{dir}/{label}.{pid}.perf.data");
    println!("[load] profiling {label} pid {pid} over {} s into {out}", window.as_secs());
    Some(
        std::process::Command::new("perf")
            .args(["record", "-q", "-F", "499", "-g", "-p", &pid.to_string(), "-o", &out, "--", "sleep", &window.as_secs().to_string()])
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("perf runs"),
    )
}

fn role_of(dir: &std::path::Path) -> &'static str {
    let name = dir.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
    if name.contains(".admin.") {
        "node-admin"
    } else {
        "rpc-node"
    }
}

/// M6: `concurrency` callers each sending `node.topology.get` of `node`'s own mesh to `node` directly,
/// back to back, for `window`: every completed call's latency (ms), the calls that ended without a reply
/// (never counted as completions), and the seconds measured. The read is the node's own topology (query.md
/// §2), served from what it holds: a Status request declares, applies or asks a birth to reassert
/// itself, so a Status load would measure those effects, not the RPC.
async fn rpc_load(estate: &Estate, node: &rdm_core::PathName, concurrency: usize, window: Duration) -> (Vec<f64>, u64, f64) {
    let carrier = estate.peers().await.carrier_at(node.clone());
    let start = Instant::now();
    let end = start + window;
    let callers: Vec<_> = (0..concurrency)
        .map(|_| {
            let (carrier, node) = (carrier.clone(), node.clone());
            tokio::spawn(async move {
                let rpc = rdm_client::NodeRpc::new(carrier.client());
                let (target, opts) = (rdm_transport::NodeTarget::CurrentPath(node.clone()), rdm_transport::CallOptions::default());
                let (mut done, mut errors) = (Vec::new(), 0u64);
                while Instant::now() < end {
                    let t = Instant::now();
                    match rpc.topology_get(&target, Some(node.mesh.clone()), None, &opts).await {
                        Ok(_) => done.push(t.elapsed().as_secs_f64() * 1000.0),
                        Err(_) => errors += 1,
                    }
                }
                (done, errors)
            })
        })
        .collect();
    let (mut latencies, mut errors) = (Vec::new(), 0);
    for c in callers {
        let (done, e) = c.await.unwrap();
        latencies.extend(done);
        errors += e;
    }
    (latencies, errors, start.elapsed().as_secs_f64())
}

/// M6's window per concurrency (load-testing.md: 60 s per op).
const M6_WINDOW: Duration = Duration::from_secs(60);

/// M7's steady window (load-testing.md: 5 minutes at steady state).
const STEADY: Duration = Duration::from_secs(300);

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// M7: the gossip bytes per second each node takes in between `from` and `to` (ns), from every
/// `rdm.mesh.topology.update.via-frame` span (one per frame delivered to the node, `bytes` its content):
/// the largest node's total over every channel, and per channel the largest node's. A frame's bytes
/// are what the node's gossip delivered, not what its transport carried (repeats and gossip control
/// messages are the transport's own).
fn gossip_bytes(spans: &[Value], from: u64, to: u64) -> (f64, std::collections::BTreeMap<String, f64>) {
    let secs = (to - from) as f64 / 1e9;
    let mut by: std::collections::BTreeMap<(String, String), u64> = Default::default();
    for sp in named(spans, "rdm.mesh.topology.update.via-frame") {
        let at = sp["start_unix_nano"].as_u64().or_else(|| sp["start_unix_nano"].as_str().and_then(|v| v.parse().ok())).unwrap_or(0);
        if at < from || at >= to {
            continue;
        }
        let a = &sp["attributes"];
        let bytes = a["bytes"].as_u64().or_else(|| a["bytes"].as_str().and_then(|v| v.parse().ok())).unwrap_or(0);
        *by.entry((s(&a["node"]), channel_kind(&s(&a["channel"])))).or_default() += bytes;
    }
    let kbs = |b: u64| b as f64 / 1024.0 / secs;
    let mut nodes: std::collections::BTreeMap<String, u64> = Default::default();
    let mut channels: std::collections::BTreeMap<String, f64> = Default::default();
    for ((node, channel), b) in by {
        *nodes.entry(node).or_default() += b;
        let c = channels.entry(channel).or_default();
        *c = c.max(kbs(b));
    }
    (nodes.into_values().map(kbs).fold(0.0, f64::max), channels)
}

/// A channel's kind for M7's rows: `mesh` for every mesh's own channel (`mesh:<name>`), else its name
/// (`backbone`).
fn channel_kind(channel: &str) -> String {
    if channel.starts_with("mesh:") { "mesh".into() } else { channel.into() }
}

/// M8: idle CPU (cores) and RSS (GB) of every runtime over `window`, the largest per role.
async fn idle_cost(estate: &Estate, window: Duration) -> Vec<(&'static str, f64, f64)> {
    let runtimes = estate.live_runtimes();
    let before: Vec<(&'static str, u32, u64)> = runtimes.iter().filter_map(|(d, p)| cpu_ticks(*p).map(|t| (role_of(d), *p, t))).collect();
    let profiles = profile_one_per_role(&before, window);
    tokio::time::sleep(window).await;
    for mut p in profiles {
        let _ = p.wait();
    }
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

/// L1 (load-testing.md Steps, 1 and 2): bring-up (M1), create rate (M2, reported), gossip cost (M7, 5 min
/// steady), one-node Build latency (M5, 20 samples, a target on the max) and idle cost (M8), on the
/// 2-mesh, 20-node profile.
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
    // Each phase's end is reported beside the whole, so a bring-up that grows names the phase it grew in.
    let t0 = Instant::now();
    let mut estate = Estate::bootstrap_with_env(owner(test), "fabric1", "mesh1", if evidence { &[] } else { &[("RDM_EVIDENCE_DIR", "")] }).await;
    let admin_up = t0.elapsed();
    let mut stream = estate.submit_shape("fabric1", &shape).await;
    let _ = Estate::until_complete(&mut stream).await;
    let complete = t0.elapsed();
    estate.settles_to(&shape, Duration::from_secs(300)).await;
    let settled = t0.elapsed();
    estate.until_fabric_ready(Duration::from_secs(300)).await;
    let bring_up = t0.elapsed();
    let ms = |d: Duration| vec![d.as_millis() as f64];
    rows.time("M1", "bring-up: day-0 admin up", ms(admin_up), None, None);
    rows.time("M1", "bring-up: the Day-0 Build complete", ms(complete), None, None);
    rows.time("M1", "bring-up: every node ready in the view", ms(settled), None, None);
    rows.time("M1", "bring-up, launch to ready", ms(bring_up), Some(p.bring_up.as_millis() as f64), None);
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

    // M6 (load-testing.md: 60 s per op at a held concurrency): Node RPC against one idle node-admin,
    // evidence off only. The target is read at the held concurrency (32 callers); 128 callers is the
    // saturation probe, reported with no target: past the node's ceiling the rate holds and only the
    // queue grows (E6), so its p99 is the queue's. The node's CPU over each window is reported beside it.
    if !evidence {
        let at: rdm_core::PathName = format!("{}.admin.{}", meshes.last().unwrap(), p.admins).parse().unwrap();
        let pid = estate.live_runtimes().into_iter().find(|(d, _)| d.file_name().is_some_and(|n| n.to_string_lossy().starts_with(&format!("{at}-")))).map(|(_, pid)| pid);
        for (callers, target) in [(32, true), (128, false)] {
            let (t0, c0) = (pid.and_then(cpu_ticks), cpu_ticks(std::process::id()));
            // With `RDM_LOAD_PERF`, the node's CPU profile over the held concurrency's window.
            let profile = pid.filter(|_| target).and_then(|pid| profile_one(&format!("m6-{at}"), pid, M6_WINDOW));
            let (latencies, errors, secs) = rpc_load(&estate, &at, callers, M6_WINDOW).await;
            if let Some(mut p) = profile {
                let _ = p.wait();
            }
            let what = format!("topology.get at {at}, {callers} callers");
            let rate = latencies.len() as f64 / secs;
            if target {
                rows.at_least("M6", &format!("{what}, completions"), "/s", rate, 2000.0);
            } else {
                rows.value("M6", &format!("{what}, completions"), "/s", rate, None);
            }
            rows.value("M6", &format!("{what}, calls without a reply"), "count", errors as f64, None);
            if let (Some(t0), Some(t1)) = (t0, pid.and_then(cpu_ticks)) {
                rows.value("M6", &format!("{what}, the node's cpu"), "cores", t1.saturating_sub(t0) as f64 / 100.0 / secs, None);
            }
            if let (Some(c0), Some(c1)) = (c0, cpu_ticks(std::process::id())) {
                rows.value("M6", &format!("{what}, the callers' process cpu"), "cores", c1.saturating_sub(c0) as f64 / 100.0 / secs, None);
            }
            if target {
                rows.time_p99("M6", &what, latencies, 50.0);
            } else {
                rows.time("M6", &what, latencies, None, None);
            }
        }
    }

    // M7: gossip cost at steady state, read from the frames each node takes in (evidence on only).
    if evidence {
        let from = now_ns();
        tokio::time::sleep(STEADY).await;
        let to = now_ns();
        let (per_node, per_channel) = gossip_bytes(&estate.spans(), from, to);
        rows.value("M7", "largest node, all channels", "KB/s", per_node, Some(50.0));
        for (channel, kbs) in per_channel {
            rows.value("M7", &format!("largest node, {channel}"), "KB/s", kbs, None);
        }
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
