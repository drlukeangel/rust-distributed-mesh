//! product=mesh, feature=node-lifecycle, subfeature=node-stop, rung=MN, provider=process.
//!
//! Stop is a standalone operation (node-stop.md): `stop-node:<path>` as the next attempt of the
//! accepted Build, reachable at `POST /api/nodes/{name}/stop`. It sends stop-node, waits for the
//! birth's node-left and completes on the provider's Exited proof. No implicit drain, no NodeDeleted.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-stop".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn step_span<'a>(spans: &'a [Value], build: &str, attempt: &str, node: &str, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && s(&sp["attributes"]["node"]) == node && s(&sp["attributes"]["step"]) == step)
}

/// CONTRACT: `POST /api/nodes/{name}/stop` opens the next attempt of the accepted Build (reason
/// stop, action stop fenced to the live birth, no Build minted). The operation is exactly
/// `stop-node:<path>`: stop-node to the birth, the wait for its node-left, the provider's
/// TerminateRuntime with its Exited proof, then Complete. No drain is sent first and no NodeDeleted
/// is published. The runtime exited and the accepted topology still names the path, yet no
/// repair attempt brings it back: the stop's Complete receipt is the proof the exit was asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_stop_ends_with_exited_proof_and_no_node_deleted_and_no_respawn() {
    let test = "a_standalone_stop_ends_with_exited_proof_and_no_node_deleted_and_no_respawn";
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]})).await;
    assert_eq!(status, 202, "{a}");
    let birth_build = s(&a["build_id"]);
    estate.await_build(&birth_build, SETTLE).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(60)).await;

    const NODE: &str = "mesh1.rpc.1";
    let before = estate.node(NODE).await;
    let pid = estate.pid_of(NODE).await;
    let (status, r) = estate.post(&format!("/api/nodes/{NODE}/stop"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    assert_eq!(s(&r["build_id"]), birth_build, "no Build is minted: {r}");
    let attempt_no = Estate::attempt_of(&r);
    let opened = wait_for("the stop attempt is the Build's current attempt", Duration::from_secs(30), || async {
        let (_, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
        (b["reason"] == "stop").then_some(b)
    })
    .await;
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("stop", NODE), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "fenced to the live birth: {opened}");
    estate.await_attempt(&birth_build, attempt_no, SETTLE).await;

    let attempt = attempt_no.to_string();
    let spans = wait_for("the stop operation's last step and its reconcile span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let reconciled = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt);
        (step_span(&spans, &birth_build, &attempt, NODE, "Complete").is_some() && reconciled).then_some(spans)
    })
    .await;
    let reconcile = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt).unwrap();
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("stop-node:{NODE}"), "one stop operation and nothing else: {reconcile}");
    let mut last = 0u64;
    for step in ["StopNode", "AwaitNodeLeft", "TerminateRuntime", "Complete"] {
        let sp = step_span(&spans, &birth_build, &attempt, NODE, step).unwrap_or_else(|| panic!("step {step} ran"));
        assert_eq!(s(&sp["attributes"]["outcome"]), "complete", "{sp}");
        let at = sp["start_unix_nano"].as_u64().unwrap();
        assert!(at >= last, "{step} starts before the step it follows");
        last = at;
    }
    for step in ["DrainNode", "AwaitNodeDrained", "NodeDeleting", "NodeDeleted", "RemoveTopologyMembership"] {
        assert!(step_span(&spans, &birth_build, &attempt, NODE, step).is_none(), "a standalone stop runs no {step}");
    }
    assert!(named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "a stop publishes no NodeDeleted");

    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists(), "the stopped process {pid} has exited");
    // Not brought back: nothing opens another attempt for the exit a stop asked for.
    tokio::time::sleep(Duration::from_secs(15)).await;
    let (_, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
    assert_eq!(b["attempt"].as_u64(), Some(attempt_no), "no repair attempt followed the stop: {b}");
    let spans = estate.spans();
    assert!(
        named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().all(|sp| s(&sp["attributes"]["build_id"]) != birth_build || s(&sp["attributes"]["scope"]).find(NODE).is_none()),
        "the drift reconciler did not treat the stop as drift"
    );
    if let Some(n) = estate.node_opt(NODE).await {
        assert_eq!(n["incarnation_id"], before["incarnation_id"], "no new birth at {NODE}: {n}");
    }
    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}
