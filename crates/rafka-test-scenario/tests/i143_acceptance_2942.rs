//! i143.e4.s18 acceptance (rafka-v2 #2942), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2942-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest and every process's spans land under it, feature `i143-2942`, test the
//! cell's name) at the test cadence (staleness 3 s, gossip 500 ms).
//!
//! The estate is the mesh delete of `mesh_lifecycle__fabric_primary_handover`: {mesh1, mesh2} -> {mesh2}, owned by the
//! fabric-primary (a mesh2 admin). The mesh.delete ends at mesh.deleted with mesh1's last admin, its
//! final primary, alive; its end is its own node.delete, which the fabric-primary executes and whose
//! `TerminateRuntime` step proves its exit (mesh-delete.md step 7). The last admin's departure is read
//! from its own leg spans, the executor's steps and a peer's resolver.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

/// The unchanged observation bound.
const OBSERVE_BOUND_MS: u64 = 10_000;

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

/// What a mesh1 delete run to its end leaves to read: the last admin, the mesh.delete's Build and
/// its last admin's node.delete's (an attempt of the accepted Build), what a peer's resolver answers
/// for the last admin's birth after the delete, and every span.
struct Retired {
    last: String,
    builds: Vec<String>,
    resolution: Value,
    spans: Vec<Value>,
}

/// mesh1 deleted, then its last admin's own node.delete, run to their end and read back.
async fn retire_mesh1(test: &str) -> Retired {
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
    // mesh.deleted leaves mesh1's last admin alive; its end is its own node.delete.
    let last = wait_for("mesh1's last admin alive after mesh.deleted, its members deleted", Duration::from_secs(60), || async {
        let nodes = estate.nodes().await;
        let mesh1: Vec<String> = nodes.iter().filter(|n| n["mesh"] == "mesh1").map(|n| s(&n["name"])).collect();
        let alive = nodes.iter().any(|n| n["mesh"] == "mesh1" && n["status"] == "ready-for-traffic");
        (mesh1.len() == 1 && alive).then(|| mesh1[0].clone())
    })
    .await;
    let last_id = before.iter().find(|n| s(&n["name"]) == last).map(|n| s(&n["node_id"])).unwrap();
    let (_, fabric_now) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric_now["admin_api_base"]);
    let (status, c) = estate.delete(&format!("/api/nodes/{last}")).await;
    assert_eq!(status, 202, "the node.delete of mesh1's last admin {last}: {c}");
    let c_attempt = Estate::attempt_of(&c);
    let c = s(&c["build_id"]);
    estate.await_attempt(&c, c_attempt, Duration::from_secs(120)).await;
    estate.settled(&names(&["mesh2"]), Duration::from_secs(60)).await;
    for (name, pid) in &pids {
        wait_for(&format!("{name}'s runtime is gone"), Duration::from_secs(30), || async { (!alive(*pid)).then_some(()) }).await;
    }
    // A peer mesh's ordinary node holds the deleted birth Gone, from its executor's NodeDeleted.
    let query = format!("exact:{last_id}");
    let resolution = wait_for(&format!("mesh2.rpc.1 holds {last} Gone"), Duration::from_secs(30), || async {
        let r = estate.probe(&["resolve", "--target", "path:mesh2.rpc.1", "--query", &query]);
        (r["reply"]["resolution"] == "gone").then_some(r)
    })
    .await;
    estate.stop().await;
    let mut builds = vec![b];
    if !builds.contains(&c) {
        builds.push(c);
    }
    Retired { last, builds, resolution, spans: estate.spans() }
}

/// mesh1's last admin, stopped by its own node.delete, announces nothing after its stop's cut, and
/// the fabric-primary that executes the node.delete proves its exit inside the unchanged 10 s; a peer
/// holds the birth Gone from the executor's NodeDeleted.
///
/// CONTRACT: R-ON1 (Luke 2026-10-10): "a stopping node gossips nothing; node.left is a reply frame
/// on the stop call's stream; the mesh primary publishes NodeStopped in its in-flight overlay";
/// node-stop.md step 4: "Emit node.stopped on that same reply stream. The node gossips nothing for
/// this step." A stopped birth's departure is the executor's: the provider's exact exit proof
/// (TerminateRuntime), then NodeDeleted, from which every peer holds the birth Gone (node-delete.md
/// steps 2-5). Must NOT happen: a stopped node announcing its own Leaving after its cut, or a peer
/// that learns the departure from anything but the executor's NodeDeleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_last_admin_announces_nothing_and_its_executor_proves_its_exit_inside_the_bound() {
    let cell = "a_stopped_last_admin_announces_nothing_and_its_executor_proves_its_exit_inside_the_bound";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let Retired { last, builds, resolution, spans } = retire_mesh1(cell).await;
    let of_builds = |sp: &Value| builds.iter().any(|x| sp["attributes"]["build_id"] == x.as_str());

    // The executor's exit proof: the TerminateRuntime of the last admin's node.delete, complete inside
    // the unchanged bound, a child of the pipeline run for this node and Build in the same trace.
    let observe: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["step"] == "TerminateRuntime" && of_builds(sp) && sp["attributes"]["node"] == last.as_str()).collect();
    let obs = *observe.iter().max_by_key(|sp| start(sp)).unwrap_or_else(|| panic!("the node.delete proved {last}'s exit under {builds:?}"));
    let obs_elapsed = attr_u64(obs, "elapsed_ms");
    assert_eq!(obs["attributes"]["outcome"], "complete", "TerminateRuntime of {last} completed: {obs}");
    assert!(obs_elapsed < OBSERVE_BOUND_MS, "TerminateRuntime of {last} completed inside {OBSERVE_BOUND_MS} ms ({obs_elapsed} ms)");
    let pipeline = named(&spans, "rdm.node_admin.deployment.update.via-pipeline")
        .into_iter()
        .find(|sp| sp["span_id"] == obs["parent_span_id"] && sp["trace_id"] == obs["trace_id"])
        .unwrap_or_else(|| panic!("TerminateRuntime's parent is a pipeline span of its trace: {obs}"));
    assert_eq!(pipeline["attributes"]["node"], last.as_str());
    assert_eq!(pipeline["attributes"]["build_id"], obs["attributes"]["build_id"]);

    // The stopped node's own legs: its stop's cut, then nothing said by it on any channel, no
    // Draining and no Leaving announcement; its transport closes.
    let mine = |name: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["node"] == last.as_str()).collect() };
    let cut = mine("rdm.node_admin.node.connections.delete.via-stop-cut");
    assert_eq!(cut.len(), 1, "{last}'s stop cut its connections once: {cut:?}");
    let cut_at = start(cut[0]);
    let announced: Vec<&Value> = mine("rdm.mesh.node.update.via-leave-announcement");
    assert!(announced.is_empty(), "{last} announced nothing, stopped (R-ON1): {announced:#?}");
    let draining = mine("rdm.mesh.node.update.via-leave-draining");
    assert!(draining.is_empty(), "{last} said no Draining of its own: {draining:?}");
    let shutdown = mine("rdm.mesh.node.update.via-leave-shutdown");
    assert_eq!(shutdown.len(), 1, "one transport shutdown by {last}: {shutdown:?}");
    assert!(start(shutdown[0]) > cut_at, "{last}'s transport shut down after its stop's cut");

    // The departure is the executor's: NodeDeleted for the exact birth after the exit proof, and a
    // peer's resolver holds it Gone.
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node"] == last.as_str() && of_builds(sp)).collect();
    assert_eq!(deleted.len(), 1, "NodeDeleted published once for {last}: {deleted:?}");
    assert!(start(deleted[0]) >= end(obs), "NodeDeleted follows the exit proof: {}", deleted[0]);
    assert_eq!(resolution["reply"]["resolution"], "gone", "a peer holds {last} Gone: {resolution}");

    let result = json!({
        "cell": cell,
        "builds": builds,
        "retiring_admin": last,
        "stop_cut": { "start_unix_nano": cut_at, "trace_id": cut[0]["trace_id"], "span_id": cut[0]["span_id"] },
        "announcements_after_stop": announced.len(),
        "shutdown": { "start_unix_nano": start(shutdown[0]), "elapsed_ms": shutdown[0]["attributes"]["elapsed_ms"], "outcome": shutdown[0]["attributes"]["outcome"], "span_id": shutdown[0]["span_id"] },
        "executor": { "step": "TerminateRuntime", "node": last, "outcome": obs["attributes"]["outcome"], "elapsed_ms": obs_elapsed, "bound_ms": OBSERVE_BOUND_MS, "trace_id": obs["trace_id"], "span_id": obs["span_id"], "pipeline_span_id": pipeline["span_id"] },
        "node_deleted": { "trace_id": deleted[0]["trace_id"], "span_id": deleted[0]["span_id"] },
        "peer_resolution": resolution["reply"],
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    eprintln!("2942: {last} stopped (cut trace {}), nothing announced, TerminateRuntime {obs_elapsed} ms (trace {}), NodeDeleted trace {}", s(&cut[0]["trace_id"]), s(&obs["trace_id"]), s(&deleted[0]["trace_id"]));
}
