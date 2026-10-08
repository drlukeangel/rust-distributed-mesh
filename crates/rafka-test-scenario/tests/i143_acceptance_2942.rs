//! i143.e4.s18 acceptance (rafka-v2 #2942, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2942-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest and every process's spans land under it, feature `i143-2942`, test the
//! cell's name) at the test cadence (staleness 3 s, gossip 500 ms).
//!
//! The estate is the whole-mesh retire of `mesh_replace`: {mesh1, mesh2} -> {mesh2}, executed by
//! a mesh2 admin. The last admin of mesh1 is the one whose retire pipeline carries
//! `ObserveDeparture` under the Build; its own leave is read from its leg spans.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

/// The unchanged linger (`RDM_LEAVE_LINGER_MS` default) and the unchanged observation bound.
const LINGER_MS: u64 = 1000;
const OBSERVE_BOUND_MS: u64 = 10_000;
const ANNOUNCEMENTS: usize = 5;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2942".into(),
        subfeature: "leave-legs".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2942/process").join(cell),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}

fn attr_u64(sp: &Value, k: &str) -> u64 {
    sp["attributes"][k].as_str().and_then(|v| v.parse().ok()).unwrap_or(u64::MAX)
}

fn mesh(name: &str) -> Value {
    json!({"name": name, "node_admin": 2, "rpc_node": 2})
}

fn names(meshes: &[&str]) -> BTreeSet<String> {
    meshes.iter().flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1"), format!("{m}.rpc.2")]).collect()
}

fn alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

/// A mesh1 retirement through the whole-mesh retire, run to its end and read back: every span.
async fn retire_mesh1(test: &str) -> (String, Vec<Value>) {
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let before = estate.settled(&names(&["mesh1", "mesh2"]), Duration::from_secs(30)).await;
    let mut pids: Vec<(String, u64)> = Vec::new();
    for n in before.iter().filter(|n| n["mesh"] == "mesh1") {
        let name = s(&n["name"]);
        let pid = match estate.bootstrap_pid() {
            Some(p) if name == "mesh1.admin.1" => u64::from(p),
            _ => estate.pid_of(&name).await,
        };
        pids.push((name, pid));
    }
    // Control moves to mesh2 before mesh1 goes.
    estate.admin = before
        .iter()
        .find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["status"] == "ready-for-traffic")
        .map(|n| s(&n["admin_api_base"]))
        .expect("a ready mesh2 admin");
    let (status, b) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{b}");
    let b = s(&b["build_id"]);
    estate.await_build(&b, Duration::from_secs(240)).await;
    estate.settled(&names(&["mesh2"]), Duration::from_secs(60)).await;
    for (name, pid) in &pids {
        wait_for(&format!("{name}'s runtime is gone"), Duration::from_secs(30), || async { (!alive(*pid)).then_some(()) }).await;
    }
    let (_, fabric_now) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric_now["admin_api_base"]);
    estate.stop().await;
    (b, estate.spans())
}

/// The last admin of mesh1 completes five `Leaving` announcements on the mesh channel and five on
/// the backbone inside the unchanged one-second linger, and the executor's ObserveDeparture for
/// that exact birth completes inside the unchanged 10 s.
///
/// CONTRACT: a retiring last admin must put every one of its linger announcements out, on both
/// channels, before the linger ends; the executor that retires it hears its Leaving from the
/// held view, never from silence or the provider's exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retiring_admin_completes_five_leaving_announcements_within_linger() {
    let cell = "retiring_admin_completes_five_leaving_announcements_within_linger";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let (b, spans) = retire_mesh1(cell).await;

    // The last admin: the mesh1 node whose retire carried ObserveDeparture under B (the latest).
    let observe: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["step"] == "ObserveDeparture" && sp["attributes"]["build_id"] == b.as_str() && s(&sp["attributes"]["node"]).starts_with("mesh1.")).collect();
    assert!(!observe.is_empty(), "the whole-mesh retire observed a mesh1 departure under {b}");
    let obs = *observe.iter().max_by_key(|sp| start(sp)).unwrap();
    let last = s(&obs["attributes"]["node"]);
    assert!(last.starts_with("mesh1.admin."), "the departure observed last is an admin's: {last}");

    // Executor side: the step completed inside the unchanged bound, a child of the pipeline run
    // for this node and Build in the same trace.
    let obs_elapsed = attr_u64(obs, "elapsed_ms");
    assert_eq!(obs["attributes"]["outcome"], "complete", "ObserveDeparture of {last} completed: {obs}");
    assert!(obs_elapsed < OBSERVE_BOUND_MS, "ObserveDeparture of {last} completed inside {OBSERVE_BOUND_MS} ms ({obs_elapsed} ms)");
    let pipeline = named(&spans, "rdm.node_admin.deployment.update.via-pipeline")
        .into_iter()
        .find(|sp| sp["span_id"] == obs["parent_span_id"] && sp["trace_id"] == obs["trace_id"])
        .unwrap_or_else(|| panic!("ObserveDeparture's parent is a pipeline span of its trace: {obs}"));
    assert_eq!(pipeline["attributes"]["node"], last.as_str());
    assert_eq!(pipeline["attributes"]["build_id"], b.as_str());

    // The retiring admin's own leave: the signal span and every leg beneath it.
    let mine = |name: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["node"] == last.as_str()).collect() };
    let signals: Vec<&Value> = named(&spans, "rdm.mesh.node.delete.via-signal");
    let draining = mine("rdm.mesh.node.update.via-leave-draining");
    assert_eq!(draining.len(), 1, "one Draining say by {last}: {draining:?}");
    let signal = signals.iter().find(|sp| sp["span_id"] == draining[0]["parent_span_id"] && sp["trace_id"] == draining[0]["trace_id"]).unwrap_or_else(|| panic!("the Draining leg sits under the leave's signal span: {}", draining[0]));
    let shutdown = mine("rdm.mesh.node.update.via-leave-shutdown");
    assert_eq!(shutdown.len(), 1, "one transport shutdown by {last}: {shutdown:?}");
    assert_eq!(shutdown[0]["parent_span_id"], signal["span_id"], "the shutdown leg sits under the same leave");
    assert_eq!(shutdown[0]["attributes"]["outcome"], "closed");

    let all = mine("rdm.mesh.node.update.via-leave-announcement");
    let on = |ch: &str| -> Vec<&Value> {
        let mut v: Vec<&Value> = all.iter().copied().filter(|sp| sp["attributes"]["channel"] == ch).collect();
        v.sort_by_key(|sp| start(sp));
        v
    };
    let (mesh_ch, bb_ch, view_ch) = (on("mesh"), on("backbone"), on("view"));
    let t0 = start(mesh_ch.first().expect("the first mesh announcement"));
    let deadline = t0 + LINGER_MS * 1_000_000;
    let mut rows = Vec::new();
    for (ch, legs) in [("mesh", &mesh_ch), ("backbone", &bb_ch)] {
        assert_eq!(legs.len(), ANNOUNCEMENTS, "{last} completed {ANNOUNCEMENTS} announcements on the {ch} channel inside its linger, not {}: {legs:#?}", legs.len());
        let mut seen: BTreeSet<u64> = BTreeSet::new();
        for sp in legs {
            assert_eq!(sp["attributes"]["outcome"], "sent", "{ch} announcement sent: {sp}");
            assert_eq!(sp["parent_span_id"], signal["span_id"], "{ch} announcement sits under the leave: {sp}");
            assert!(end(sp) <= deadline, "{ch} announcement {} completed inside the {LINGER_MS} ms linger ({} ms after the first)", sp["attributes"]["announcement"], (end(sp).saturating_sub(t0)) / 1_000_000);
            seen.insert(attr_u64(sp, "announcement"));
            rows.push(json!({
                "channel": ch, "announcement": sp["attributes"]["announcement"], "start_unix_nano": start(sp), "end_unix_nano": end(sp),
                "after_first_ms": (start(sp).saturating_sub(t0)) / 1_000_000, "elapsed_ms": sp["attributes"]["elapsed_ms"], "outcome": sp["attributes"]["outcome"],
                "publishing": sp["attributes"]["publishing"], "members": sp["attributes"]["members"], "trace_id": sp["trace_id"], "span_id": sp["span_id"], "parent_span_id": sp["parent_span_id"],
            }));
        }
        assert_eq!(seen, (1..=ANNOUNCEMENTS as u64).collect::<BTreeSet<_>>(), "{ch}: announcements 1..={ANNOUNCEMENTS} each once");
    }
    // A backbone say counts only from a publishing seat: a non-publisher's `sent` is a no-op.
    assert!(bb_ch.iter().all(|sp| sp["attributes"]["publishing"] == "true"), "the last admin of the mesh is the backbone's publisher for it: {bb_ch:#?}");
    assert_eq!(view_ch.len(), ANNOUNCEMENTS, "the topology read before each backbone say ran {ANNOUNCEMENTS} times");

    let result = json!({
        "cell": cell,
        "build_id": b,
        "retiring_admin": last,
        "linger_ms": LINGER_MS,
        "leave": { "signal_span_id": signal["span_id"], "trace_id": signal["trace_id"], "start_unix_nano": start(signal), "end_unix_nano": end(signal) },
        "draining": { "start_unix_nano": start(draining[0]), "elapsed_ms": draining[0]["attributes"]["elapsed_ms"], "outcome": draining[0]["attributes"]["outcome"], "span_id": draining[0]["span_id"] },
        "announcements": rows,
        "topology_reads": view_ch.iter().map(|sp| json!({ "announcement": sp["attributes"]["announcement"], "elapsed_ms": sp["attributes"]["elapsed_ms"], "span_id": sp["span_id"] })).collect::<Vec<_>>(),
        "shutdown": { "start_unix_nano": start(shutdown[0]), "elapsed_ms": shutdown[0]["attributes"]["elapsed_ms"], "outcome": shutdown[0]["attributes"]["outcome"], "span_id": shutdown[0]["span_id"] },
        "executor": { "step": "ObserveDeparture", "node": last, "outcome": obs["attributes"]["outcome"], "elapsed_ms": obs_elapsed, "bound_ms": OBSERVE_BOUND_MS, "trace_id": obs["trace_id"], "span_id": obs["span_id"], "pipeline_span_id": pipeline["span_id"] },
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
