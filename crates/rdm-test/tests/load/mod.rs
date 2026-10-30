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
const L2: Profile = Profile { name: "L2", meshes: 4, admins: 2, rpcs: 10, bring_up: Duration::from_secs(25) };
const L3: Profile = Profile { name: "L3", meshes: 10, admins: 2, rpcs: 8, bring_up: Duration::from_secs(40) };
const L4: Profile = Profile { name: "L4", meshes: 10, admins: 3, rpcs: 17, bring_up: Duration::from_secs(60) };

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

/// Milliseconds from now until every node-admin in `admins` holds `node` in its own topology as `status`
/// says: `ready` (given `not_id`, as a birth other than that node id), or `leaving`, a stop, which a
/// node-admin holds as leaving or no longer holds at all (a stopped node leaves the view). Polled every
/// 50 ms, bounded at 120 s.
async fn every_admin_holds(estate: &Estate, admins: &[rdm_core::PathName], node: &rdm_core::PathName, status: &str, not_id: Option<&str>) -> f64 {
    let t = Instant::now();
    let mut left: Vec<&rdm_core::PathName> = admins.iter().collect();
    while !left.is_empty() {
        assert!(t.elapsed() < Duration::from_secs(120), "{node}: {left:?} did not hold it {status} within 120 s");
        let mut still = Vec::new();
        for a in left {
            let values = estate.held_values_at(a).await;
            let mine: Vec<&Value> = values.iter().filter(|v| v["name"] == node.to_string() && not_id.is_none_or(|id| s(&v["node_id"]) != id)).collect();
            let held = if status == "leaving" { mine.iter().all(|v| s(&v["status"]) != "ready") } else { mine.iter().any(|v| s(&v["status"]) == status) };
            if !held {
                still.push(a);
            }
        }
        left = still;
        if !left.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    t.elapsed().as_millis() as f64
}

/// A span's start, in Unix nanoseconds.
fn start_ns(sp: &&Value) -> u64 {
    sp["start_unix_nano"].as_u64().or_else(|| sp["start_unix_nano"].as_str().and_then(|v| v.parse().ok())).unwrap_or(0)
}

/// M3 from the spans, all at or after `since`: the mesh-primary's write of `member`'s `status` (a stop: its
/// admission of the stop command, `from-command-admitted` leaving; a start: its `from-node-started`) to the
/// last node-admin of
/// `admins` applying it to its own topology (`via-member-state`, `via-projected-status`, or that write
/// itself). `Err` names the node-admins no span shows applying it.
fn spread_from_spans(spans: &[Value], admins: &[rdm_core::PathName], member: &str, status: &str, since: u64) -> Result<f64, Vec<String>> {
    let a = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let is = |sp: &Value| a(sp, "status").eq_ignore_ascii_case(status);
    let writes = |sp: &Value| match s(&sp["name"]).as_str() {
        "rdm.mesh.topology.update.from-command-admitted" => status == "leaving" && a(sp, "member") == member && is(sp),
        "rdm.mesh.topology.update.from-node-started" => status == "ready" && a(sp, "member") == member && is(sp),
        _ => false,
    };
    let applies = |sp: &Value| writes(sp) || (matches!(s(&sp["name"]).as_str(), "rdm.mesh.topology.update.via-member-state" | "rdm.mesh.topology.update.via-projected-status") && a(sp, "member") == member && is(sp));
    let after: Vec<&Value> = spans.iter().filter(|sp| start_ns(sp) >= since).collect();
    let origin = after.iter().filter(|sp| writes(sp)).map(start_ns).min().ok_or_else(|| vec![format!("the mesh-primary's write of {member} {status}")])?;
    let mut last = origin;
    let mut missing = Vec::new();
    for admin in admins {
        match after.iter().filter(|sp| applies(sp) && a(sp, "node") == admin.to_string()).map(start_ns).min() {
            Some(at) => last = last.max(at),
            None => missing.push(admin.to_string()),
        }
    }
    if missing.is_empty() { Ok((last - origin) as f64 / 1e6) } else { Err(missing) }
}

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

/// Open file descriptors of `pid`, from `/proc/<pid>/fd`.
fn open_files(pid: u32) -> Option<f64> {
    std::fs::read_dir(format!("/proc/{pid}/fd")).ok().map(|d| d.count() as f64)
}

/// The pid a span file belongs to: `<service>.<pid>[-<ns>].spans.jsonl`.
fn pid_of_span_file(name: &str) -> Option<u32> {
    name.split('.').nth(1).and_then(|p| p.split('-').next()).and_then(|p| p.parse().ok())
}

/// One M10 window: when it ended (seconds into the soak), and per process its RSS (GB), open files,
/// CPU (cores over the window) and span bytes per minute, and the largest node's gossip KB/s.
struct Window {
    at: f64,
    rss: std::collections::BTreeMap<u32, f64>,
    fds: std::collections::BTreeMap<u32, f64>,
    cores: std::collections::BTreeMap<u32, f64>,
    spans_per_min: std::collections::BTreeMap<u32, f64>,
    gossip_kbs: f64,
}

/// The largest relative change of `f` per process between the mean over `first` and over `last`
/// (a fraction: 0.1 is 10 %), and the process it is at.
fn largest_growth(first: &[&Window], last: &[&Window], f: impl Fn(&Window) -> &std::collections::BTreeMap<u32, f64>) -> (f64, Option<u32>) {
    let mean = |ws: &[&Window], pid: u32| {
        let v: Vec<f64> = ws.iter().filter_map(|w| f(w).get(&pid).copied()).collect();
        (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64)
    };
    let pids: std::collections::BTreeSet<u32> = first.iter().flat_map(|w| f(w).keys().copied()).collect();
    pids.into_iter()
        .filter_map(|pid| match (mean(first, pid), mean(last, pid)) {
            (Some(a), Some(b)) if a > 0.0 => Some(((b - a) / a, pid)),
            _ => None,
        })
        .fold((0.0, None), |acc, (g, pid)| if g.abs() > acc.0.abs() { (g, Some(pid)) } else { acc })
}

/// M10 (load-testing.md: "M7 and M8 over 4 h at the largest passing profile; the first 30 minutes are not
/// read, and between the first and the last hour after them every process's RSS grows under 10 %, its
/// open-file count under 5 %, and the span volume per node per minute stays within 10 % of the first
/// hour's"). The soak is read in 5-minute windows (a run shorter than 40 minutes, a proof run, scales the
/// window, the unread start and the hours to its length); each window's span evidence is read, then
/// dropped, so a 4-hour run never outgrows its evidence directory.
async fn soak(estate: &Estate, length: Duration, rows: &mut Rows) {
    let total = length.as_secs_f64();
    let scale = (total / 14_400.0).min(1.0);
    let (window, unread, hour) = (Duration::from_secs_f64((300.0 * scale).max(10.0)), 1_800.0 * scale, 3_600.0 * scale);
    let started = Instant::now();
    let _ = estate.drop_evidence();
    let mut windows: Vec<Window> = Vec::new();
    while started.elapsed() < length {
        let pids: Vec<u32> = estate.live_runtimes().into_iter().map(|(_, pid)| pid).collect();
        let ticks0: std::collections::BTreeMap<u32, u64> = pids.iter().filter_map(|p| cpu_ticks(*p).map(|t| (*p, t))).collect();
        let from = now_ns();
        tokio::time::sleep(window).await;
        let to = now_ns();
        let secs = window.as_secs_f64();
        let (gossip_kbs, _) = gossip_bytes(&estate.spans_named(&["rdm.mesh.topology.update.via-frame"]), from, to);
        let spans_per_min = estate.drop_evidence().into_iter().filter_map(|(name, bytes)| pid_of_span_file(&name).map(|p| (p, bytes as f64 / (secs / 60.0)))).collect();
        windows.push(Window {
            at: started.elapsed().as_secs_f64(),
            rss: pids.iter().filter_map(|p| rss_gb(*p).map(|v| (*p, v))).collect(),
            fds: pids.iter().filter_map(|p| open_files(*p).map(|v| (*p, v))).collect(),
            cores: ticks0.iter().filter_map(|(p, t0)| cpu_ticks(*p).map(|t1| (*p, t1.saturating_sub(*t0) as f64 / 100.0 / secs))).collect(),
            spans_per_min,
            gossip_kbs,
        });
    }
    let first: Vec<&Window> = windows.iter().filter(|w| w.at > unread && w.at <= unread + hour).collect();
    let last: Vec<&Window> = windows.iter().filter(|w| w.at > total - hour).collect();
    let read: Vec<&Window> = windows.iter().filter(|w| w.at > unread).collect();
    let pct = |g: f64| g * 100.0;
    let (rss, rss_pid) = largest_growth(&first, &last, |w| &w.rss);
    let (fds, fds_pid) = largest_growth(&first, &last, |w| &w.fds);
    let (vol, vol_pid) = largest_growth(&first, &last, |w| &w.spans_per_min);
    rows.value("M10", &format!("largest RSS growth, first to last hour (pid {rss_pid:?})"), "%", pct(rss), Some(10.0));
    rows.value("M10", &format!("largest open-file growth, first to last hour (pid {fds_pid:?})"), "%", pct(fds), Some(5.0));
    rows.value("M10", &format!("largest span-volume change per node per minute, first to last hour (pid {vol_pid:?})"), "%", pct(vol.abs()), Some(10.0));
    rows.value("M10", "windows read", "count", read.len() as f64, None);
    let max_of = |f: &dyn Fn(&Window) -> f64| read.iter().map(|w| f(w)).fold(0.0_f64, f64::max);
    rows.value("M7", "largest node, all channels, worst soak window", "KB/s", max_of(&|w| w.gossip_kbs), Some(50.0));
    rows.value("M8", "largest process cpu, worst soak window", "cores", max_of(&|w| w.cores.values().copied().fold(0.0_f64, f64::max)), None);
    rows.value("M8", "largest process rss, worst soak window", "GB", max_of(&|w| w.rss.values().copied().fold(0.0_f64, f64::max)), Some(0.5));
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

/// L2, the 4-mesh, 48-node profile (load-testing.md Steps, 3): every measure L1 takes, at this profile's M1 target.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh"]
async fn load_profile_l2_baseline() {
    run_profile(&L2, "load_profile_l2_baseline", true).await;
}

/// L2 with no span evidence written: the pass M6 and M8 are read from.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh --no-evidence"]
async fn load_profile_l2_no_evidence() {
    run_profile(&L2, "load_profile_l2_no_evidence", false).await;
}

/// L3, the 10-mesh, 100-node profile (load-testing.md Steps, 3): every measure L1 takes, at this profile's M1 target.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh"]
async fn load_profile_l3_baseline() {
    run_profile(&L3, "load_profile_l3_baseline", true).await;
}

/// L3 with no span evidence written: the pass M6 and M8 are read from.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh --no-evidence"]
async fn load_profile_l3_no_evidence() {
    run_profile(&L3, "load_profile_l3_no_evidence", false).await;
}

/// L4 again with 100 samples of M3 and M5, the target on p99 (load-testing.md "Samples": at the largest
/// passing profile M3 and M5 run once more with 100 samples, before M10).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh L4 --samples"]
async fn load_profile_l4_samples() {
    run_as(&L4, "load_profile_l4_samples", true, Run::Samples).await;
}

/// M10 at L4, the largest passing profile: 4 hours, or `RDM_LOAD_SOAK_SECS` for a proof run.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh L4 --soak"]
async fn load_profile_l4_soak() {
    run_as(&L4, "load_profile_l4_soak", true, Run::Soak(soak_length())).await;
}

/// M10's harness at L1, for a short proof run of the soak's reading (`RDM_LOAD_SOAK_SECS`).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh L1 --soak"]
async fn load_profile_l1_soak() {
    run_as(&L1, "load_profile_l1_soak", true, Run::Soak(soak_length())).await;
}

/// M10's length: 4 hours, or `RDM_LOAD_SOAK_SECS`.
fn soak_length() -> Duration {
    Duration::from_secs(std::env::var("RDM_LOAD_SOAK_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(14_400))
}

/// L4, the 10-mesh, 200-node profile (load-testing.md Steps, 3): every measure L1 takes, at this profile's M1 target.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh"]
async fn load_profile_l4_baseline() {
    run_profile(&L4, "load_profile_l4_baseline", true).await;
}

/// L4 with no span evidence written: the pass M6 and M8 are read from.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load: run by scripts/rdm-load.sh --no-evidence"]
async fn load_profile_l4_no_evidence() {
    run_profile(&L4, "load_profile_l4_no_evidence", false).await;
}

/// Run profile `p`. With `evidence` off no node writes span evidence (load-testing.md "Conditions": the
/// pass M6 and M8 are read from); M2, the one measure counted from the spans, is then not reported.
async fn run_profile(p: &Profile, test: &str, evidence: bool) {
    run_as(p, test, evidence, Run::Baseline).await
}

/// What one run of a profile measures (load-testing.md "Samples", M10).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Run {
    /// Every measure: M1-M8, 20 samples for M3 and M5, a target on the max.
    Baseline,
    /// At the largest passing profile, before M10: M3 and M5 again with 100 samples, the target on p99.
    Samples,
    /// M10: M7 and M8 over this long at the largest passing profile, read for growth.
    Soak(Duration),
}

impl Run {
    /// M3's and M5's samples.
    fn samples(self) -> usize {
        if self == Run::Samples { 100 } else { 20 }
    }
}

async fn run_as(p: &Profile, test: &str, evidence: bool, run: Run) {
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
        let spans = estate.spans_named(&["rdm.node_admin.deployment.update.via-step"]);
        let creates = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["step"] == "Complete" && sp["attributes"]["outcome"] == "complete").count();
        rows.value("M2", "node creates per second", "creates/s", creates as f64 / bring_up.as_secs_f64(), None);
    }

    if let Run::Soak(length) = run {
        soak(&estate, length, &mut rows).await;
        estate.stop().await;
        rows.finish();
        return;
    }

    if run == Run::Baseline {
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
            let (per_node, per_channel) = gossip_bytes(&estate.spans_named(&["rdm.mesh.topology.update.via-frame"]), from, to);
            rows.value("M7", "largest node, all channels", "KB/s", per_node, Some(50.0));
            for (channel, kbs) in per_channel {
                rows.value("M7", &format!("largest node, {channel}"), "KB/s", kbs, None);
            }
        }
    }

    // M3: a node's change to the last node-admin's own view holding it (load-testing.md M3): a node.stop
    // of one rpc node, from its Complete to every node-admin's own topology (node.topology.get) holding it
    // stopped, then its node.start to every node-admin holding it ready; 20 samples each (100 in a
    // samples run, the target then on p99).
    let admins: Vec<rdm_core::PathName> = meshes.iter().flat_map(|m| (1..=p.admins).map(move |i| format!("{m}.admin.{i}").parse().unwrap())).collect();
    let subject: rdm_core::PathName = format!("{}.rpc.1", meshes[0]).parse().unwrap();
    // With evidence the sample is read from the spans: the mesh-primary's write of the status to the last
    // node-admin applying it to its own topology; without, from the call's Complete to every node-admin's
    // `node.topology.get` holding it, polled.
    let (mut stops, mut starts, mut unseen) = (Vec::new(), Vec::new(), std::collections::BTreeSet::new());
    for _ in 0..run.samples() {
        // A stop reaches a node's own topology as `leaving` (the held statuses: pending, ready,
        // connections-deleting, leaving); the row's stopped is the mesh-primary's.
        for (status, out) in [("leaving", &mut stops), ("ready", &mut starts)] {
            let since = now_ns();
            let objs = estate.nodes_obj().await;
            let call = if status == "leaving" { objs.stop(&subject).await } else { objs.start(&subject).await };
            let _ = Estate::workflow(&format!("{status} of {subject}"), call).await;
            let polled = every_admin_holds(&estate, &admins, &subject, status, None).await;
            if status == "leaving" {
                estate.until_stopped_at_the_fabric_primary(&subject).await;
            }
            if !evidence {
                out.push(polled);
                continue;
            }
            // The view poll above returned once every admin's topology held the status; each admin's
            // BatchSpanProcessor exports its closed spans on a 200 ms schedule (telemetry.rs:298), so
            // read incremental spans across one export window before declaring a sample unseen.
            let flush_until = Instant::now() + Duration::from_millis(500);
            let sampled = loop {
                let spans = estate.spans_named(&[
                    "rdm.mesh.topology.update.from-command-admitted",
                    "rdm.mesh.topology.update.from-node-started",
                    "rdm.mesh.topology.update.via-member-state",
                    "rdm.mesh.topology.update.via-projected-status",
                ]);
                match spread_from_spans(&spans, &admins, &subject.to_string(), status, since) {
                    Ok(ms) => break Ok(ms),
                    Err(missing) if Instant::now() >= flush_until => break Err(missing),
                    Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
                }
            };
            match sampled {
                Ok(ms) => out.push(ms),
                Err(missing) => unseen.extend(missing),
            }
        }
    }
    if run == Run::Samples {
        rows.time_p99("M3", "a stop to every node-admin's own view", stops, 4000.0);
        rows.time_p99("M3", "a start to every node-admin's own view", starts, 4000.0);
    } else {
        rows.time("M3", "a stop to every node-admin's own view", stops, Some(4000.0), None);
        rows.time("M3", "a start to every node-admin's own view", starts, Some(4000.0), None);
    }
    if !unseen.is_empty() {
        println!("[load] M3: no span names the change applied at {unseen:?}: those samples are left out");
    }

    if run == Run::Baseline {
        // M4: a SIGKILLed rpc node, kills spread across every mesh: the kill to its re-created birth Ready in
        // every node-admin's own view; with evidence, the kill to the first stale mark of it
        // (`via-member-stale`) and that mark to Ready on their own; 10 kills.
        let (mut to_ready, mut to_mark, mut mark_to_ready) = (Vec::new(), Vec::new(), Vec::new());
        for k in 0..10usize {
            let mesh = &meshes[k % meshes.len()];
            let victim: rdm_core::PathName = format!("{mesh}.rpc.{}", (k / meshes.len()) as u32 % p.rpcs + 1).parse().unwrap();
            let before = estate.held_values_at(&admins[0]).await.into_iter().find(|v| v["name"] == victim.to_string()).map(|v| s(&v["node_id"]));
            let killed_ns = now_ns();
            estate.kill_node(&victim.to_string()).await;
            let ms = every_admin_holds(&estate, &admins, &victim, "ready", before.as_deref()).await;
            let ready_ns = killed_ns + (ms * 1e6) as u64;
            to_ready.push(ms);
            if evidence {
                let spans = estate.spans_named(&["rdm.mesh.topology.update.via-member-stale"]);
                let mark = named(&spans, "rdm.mesh.topology.update.via-member-stale")
                    .into_iter()
                    .filter(|sp| s(&sp["attributes"]["member"]) == victim.to_string())
                    .map(|sp| start_ns(&sp))
                    .filter(|at| *at >= killed_ns)
                    .min();
                if let Some(at) = mark {
                    to_mark.push((at - killed_ns) as f64 / 1e6);
                    mark_to_ready.push(ready_ns.saturating_sub(at) as f64 / 1e6);
                }
            }
        }
        rows.time("M4", "the kill to Ready", to_ready, Some(60_000.0), None);
        if evidence {
            rows.time("M4", "the kill to the stale mark", to_mark, Some(32_000.0), None);
            rows.time("M4", "the stale mark to Ready", mark_to_ready, Some(25_000.0), None);
        }
    }

    // M5: one node added then removed, each its own Build, accepted to complete; 20 samples each.
    let mut grow = Vec::new();
    let mut shrink = Vec::new();
    let bigger: Vec<(&str, u32, u32)> = shape.iter().enumerate().map(|(i, (m, a, r))| if i == 0 { (*m, *a, r + 1) } else { (*m, *a, *r) }).collect();
    for _ in 0..run.samples() {
        let t = Instant::now();
        let mut s = estate.submit_shape("fabric1", &bigger).await;
        let _ = Estate::until_complete(&mut s).await;
        grow.push(t.elapsed().as_millis() as f64);
        let t = Instant::now();
        let mut s = estate.submit_shape("fabric1", &shape).await;
        let _ = Estate::until_complete(&mut s).await;
        shrink.push(t.elapsed().as_millis() as f64);
    }
    if run == Run::Samples {
        rows.time_p99("M5", "one node.create Build", grow, 5000.0);
        rows.time_p99("M5", "one node.delete Build", shrink, 5000.0);
    } else {
        rows.time("M5", "one node.create Build", grow, Some(5000.0), None);
        rows.time("M5", "one node.delete Build", shrink, Some(5000.0), None);
    }

    estate.stop().await;
    rows.finish();
}
