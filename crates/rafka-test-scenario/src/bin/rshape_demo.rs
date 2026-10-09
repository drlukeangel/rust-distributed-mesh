//! `rshape-demo`: births the canonical R-shape estate (two meshes x node_admin 2, gateway 3, broker 3,
//! compute 2 = 20 nodes) on the demo consumer's four executables, drives R-shape traffic through it
//! until stopped, and holds the estate for as long as it runs.
//!
//! It composes the estate harness the acceptance cells use ([`rafka_test_scenario::estate::Estate`]
//! born by `bootstrap_external`, then ONE accepted Build for both meshes) and the same synthetic
//! traffic the burn-in cells issue (`rafka-rpc-probe put --target exact:<broker> [--via path:<gateway>]`).
//! Nothing is started by hand: every node but the Day-0 admin is born by the Build the rectifier executes.
//!
//! Environment (all required, no defaults):
//! - `RDM_RSHAPE_CONSUMER_BIN_DIR`: the folder holding the four `rshape-*` executables;
//! - `RSHAPE_MANIFEST`: the consumer build's `manifest.json` (candidate sha and executable hashes);
//! - `RSHAPE_DEMO_STATE`: a folder this process writes `estate.json` (once the estate is ready) and
//!   `traffic.json` (every second) into;
//! - `RDM_ARTIFACTS_DIR`: where the estate's manifest, ledger and span records land.
//!
//! SIGTERM / SIGINT stops the traffic and shuts the estate down through the fabric-primary's
//! shutdown route; the estate's reaper removes anything that outlives it.

use rafka_test_scenario::estate::{binding_set_from_build_manifest, Estate, Owner, ProbeHandle};
use rafka_test_scenario::model::Rng;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MESHES: [&str; 2] = ["mesh1", "mesh2"];
const PER_MESH: [(&str, u32); 4] = [("node_admin", 2), ("compute", 2), ("gateway", 3), ("broker", 3)];

fn env(var: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| {
        eprintln!("rshape-demo: REFUSED: {var} is not set");
        std::process::exit(2)
    })
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn write_atomic(path: &std::path::Path, v: &Value) {
    let tmp = path.with_extension("tmp");
    if std::fs::write(&tmp, serde_json::to_vec_pretty(v).unwrap()).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

fn names() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for m in MESHES {
        for (kind, n) in PER_MESH {
            let seg = if kind == "node_admin" { "admin" } else { kind };
            for i in 1..=n {
                out.insert(format!("{m}.{seg}.{i}"));
            }
        }
    }
    out
}

#[derive(Default)]
struct Traffic {
    issued: u64,
    by_outcome: BTreeMap<String, u64>,
    by_route: BTreeMap<String, u64>,
    recent: VecDeque<Value>,
}

/// One put into a broker of the fabric, a half of them carried through a gateway (the same mesh as
/// the broker or the other one). The typed outcome is recorded; nothing is repeated.
async fn one_op(probe: &ProbeHandle, admin: &str, nodes: &[Value], rng: &mut Rng, seq: u64, t: &mut Traffic) {
    let brokers: Vec<&Value> = nodes.iter().filter(|n| n["kind"] == "broker").collect();
    let gateways: Vec<&Value> = nodes.iter().filter(|n| n["kind"] == "gateway").collect();
    if brokers.is_empty() || gateways.is_empty() {
        return;
    }
    let broker = brokers[rng.below(brokers.len() as u64) as usize];
    let via = (rng.below(2) == 0).then(|| gateways[rng.below(gateways.len() as u64) as usize]);
    let (bname, gname) = (s(&broker["name"]), via.map(|g| s(&g["name"])));
    let route = match &gname {
        None => "exact-node".to_string(),
        Some(g) if g.split('.').next() == bname.split('.').next() => "carried-same-mesh".to_string(),
        Some(_) => "carried-cross-mesh".to_string(),
    };
    let (target, key, value) = (format!("exact:{}", s(&broker["node_id"])), format!("demo-{seq:07}"), format!("v{seq}-{:x}", rng.below(1 << 24)));
    let mut args: Vec<String> = ["put", "--target", &target, "--key", &key, "--value", &value].iter().map(|a| a.to_string()).collect();
    if let Some(g) = &gname {
        args.extend(["--via".into(), format!("path:{g}")]);
    }
    let (p, a) = (probe.clone(), admin.to_string());
    let started = now_ms();
    let out = tokio::task::spawn_blocking(move || {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        p.run(&a, &refs)
    })
    .await;
    let (outcome, reason, trace) = match out {
        Ok(Ok(v)) => (s(&v["outcome"]).to_string(), s(&v["reason"]), s(&v["traceparent"]).split('-').nth(1).unwrap_or_default().to_string()),
        Ok(Err(e)) => ("probe-failed".to_string(), e.chars().take(160).collect(), String::new()),
        Err(e) => ("probe-failed".to_string(), e.to_string(), String::new()),
    };
    t.issued += 1;
    *t.by_outcome.entry(outcome.clone()).or_default() += 1;
    *t.by_route.entry(route.clone()).or_default() += 1;
    t.recent.push_back(json!({"seq": seq, "at_ms": started, "route": route, "target": bname, "via": gname, "outcome": outcome, "reason": reason, "trace_id": trace}));
    if t.recent.len() > 40 {
        t.recent.pop_front();
    }
}

#[tokio::main]
async fn main() {
    let _telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rshape-demo");
    let bin_dir = PathBuf::from(env("RDM_RSHAPE_CONSUMER_BIN_DIR"));
    let manifest = PathBuf::from(env("RSHAPE_MANIFEST"));
    let state = PathBuf::from(env("RSHAPE_DEMO_STATE"));
    let _ = env("RDM_ARTIFACTS_DIR");
    std::fs::create_dir_all(&state).unwrap();
    let set = binding_set_from_build_manifest(&manifest, &bin_dir, false).unwrap_or_else(|e| {
        eprintln!("rshape-demo: REFUSED: the consumer build manifest does not bind the executables: {e}");
        std::process::exit(2)
    });
    let candidate = set.candidate.sha.clone();
    let owner = Owner { product: "mesh".into(), feature: "rshape-demo".into(), subfeature: "canonical".into(), rung: "MN".into(), provider: "process".into(), test: "live".into() };
    eprintln!("rshape-demo: births the canonical R-shape on candidate {candidate}");
    let mut estate = match Estate::bootstrap_external(owner, "fabric1", "mesh1", &set, &candidate, &["broker", "gateway", "compute"]).await {
        Ok(e) => e,
        Err(e) => {
            eprintln!("rshape-demo: REFUSED: the consumer's binding set is refused: {e}");
            std::process::exit(2)
        }
    };
    estate.set_seed(1431101);
    let meshes: Vec<Value> = MESHES.iter().map(|m| json!({"name": m, "node_admin": 2, "compute": 2, "gateway": 3, "broker": 3})).collect();
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": meshes})).await;
    assert_eq!(status, 202, "{a}");
    let build_id = s(&a["build_id"]);
    estate.await_build(&build_id, Duration::from_secs(300)).await;
    let nodes = estate.settled(&names(), Duration::from_secs(120)).await;
    eprintln!("rshape-demo: {} nodes ready for traffic (build {build_id})", nodes.len());
    write_atomic(
        &state.join("estate.json"),
        &json!({
            "node_admin_api_base": estate.admin, "evidence_dir": estate.evidence, "estate_root": estate.root, "artifacts": estate.artifacts,
            "fabric_id": estate.fabric_id, "candidate_sha": candidate, "build_id": build_id, "pid": std::process::id(), "born_ms": now_ms(),
            "bootstrap_admin_pid": estate.bootstrap_pid(),
        }),
    );

    let probe = estate.probe_handle();
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    let mut rng = Rng(1431101);
    let mut traffic = Traffic::default();
    let (mut seq, mut nodes_view, mut refreshed) = (0u64, nodes, std::time::Instant::now());
    let mut last_write = std::time::Instant::now();
    loop {
        tokio::select! {
            _ = term.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(Duration::from_millis(400)) => {}
        }
        if refreshed.elapsed() > Duration::from_secs(3) {
            if let Ok(Ok(r)) = tokio::time::timeout(Duration::from_secs(3), async { estate.get("/api/nodes").await }).await.map(|(st, v)| if st == 200 { Ok(v) } else { Err(st) }) {
                if let Some(a) = r["nodes"].as_array() {
                    nodes_view = a.clone();
                }
            }
            refreshed = std::time::Instant::now();
        }
        // `<state>/traffic.pause` holds the driver (an idle estate is measurable); removing it resumes.
        if state.join("traffic.pause").exists() {
            write_atomic(&state.join("traffic.json"), &json!({"updated_ms": now_ms(), "paused": true, "issued": traffic.issued, "by_outcome": traffic.by_outcome, "by_route": traffic.by_route, "recent": traffic.recent}));
            continue;
        }
        seq += 1;
        let ready: Vec<Value> = nodes_view.iter().filter(|n| n["status"] == "ready-for-traffic").cloned().collect();
        one_op(&probe, &estate.admin.clone(), &ready, &mut rng, seq, &mut traffic).await;
        if last_write.elapsed() > Duration::from_secs(1) {
            write_atomic(&state.join("traffic.json"), &json!({"updated_ms": now_ms(), "issued": traffic.issued, "by_outcome": traffic.by_outcome, "by_route": traffic.by_route, "recent": traffic.recent}));
            last_write = std::time::Instant::now();
        }
    }
    eprintln!("rshape-demo: stopping after {} operations", traffic.issued);
    estate.stop().await;
}
