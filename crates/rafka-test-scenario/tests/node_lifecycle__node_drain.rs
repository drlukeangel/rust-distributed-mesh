//! product=mesh, feature=node-lifecycle, subfeature=node-drain, rung=MN, provider=process.
//!
//! Drain is a standalone operation (node-drain.md): `drain-node:<path>` as the next attempt of the
//! accepted Build, reachable at `POST /api/nodes/{name}/drain`. It sends drain-node, waits for the
//! birth's node-drained and stops there: the process keeps running.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-drain".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn step_span<'a>(spans: &'a [Value], build: &str, attempt: &str, node: &str, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && s(&sp["attributes"]["node"]) == node && s(&sp["attributes"]["step"]) == step)
}

/// CONTRACT: `POST /api/nodes/{name}/drain` opens the next attempt of the accepted Build (reason
/// drain, action drain fenced to the live birth, no Build minted). The operation is exactly
/// `drain-node:<path>`: drain-node to the birth, the wait for its node-drained, then Complete. The
/// Build completes on node-drained; the node is Draining under the SAME NodeId and incarnation and
/// its process is still running afterwards. No stop, no terminate, no NodeDeleted is run for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_standalone_drain_completes_on_node_drained_and_leaves_the_process_running() {
    let test = "a_standalone_drain_completes_on_node_drained_and_leaves_the_process_running";
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]})).await;
    assert_eq!(status, 202, "{a}");
    let birth_build = s(&a["build_id"]);
    estate.await_build(&birth_build, SETTLE).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(60)).await;

    const NODE: &str = "mesh1.rpc.1";
    let before = estate.node(NODE).await;
    let pid = estate.pid_of(NODE).await;
    let (status, r) = estate.post(&format!("/api/nodes/{NODE}/drain"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    assert_eq!(s(&r["build_id"]), birth_build, "no Build is minted: {r}");
    let attempt_no = Estate::attempt_of(&r);
    let opened = wait_for("the drain attempt is the Build's current attempt", Duration::from_secs(30), || async {
        let (_, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
        (b["reason"] == "drain").then_some(b)
    })
    .await;
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("drain", NODE), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "fenced to the live birth: {opened}");
    estate.await_attempt(&birth_build, attempt_no, SETTLE).await;

    let attempt = attempt_no.to_string();
    let spans = wait_for("the drain operation's last step and its reconcile span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let reconciled = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt);
        (step_span(&spans, &birth_build, &attempt, NODE, "Complete").is_some() && reconciled).then_some(spans)
    })
    .await;
    let reconcile = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt).unwrap();
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("drain-node:{NODE}"), "one drain operation and nothing else: {reconcile}");
    let mut last = 0u64;
    for step in ["DrainNode", "AwaitNodeDrained", "Complete"] {
        let sp = step_span(&spans, &birth_build, &attempt, NODE, step).unwrap_or_else(|| panic!("step {step} ran"));
        assert_eq!(s(&sp["attributes"]["outcome"]), "complete", "{sp}");
        let at = sp["start_unix_nano"].as_u64().unwrap();
        assert!(at >= last, "{step} starts before the step it follows");
        last = at;
    }
    for step in ["StopNode", "AwaitNodeLeft", "TerminateRuntime", "NodeDeleting", "NodeDeleted", "RemoveTopologyMembership"] {
        assert!(step_span(&spans, &birth_build, &attempt, NODE, step).is_none(), "a standalone drain runs no {step}");
    }
    assert!(named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().all(|sp| sp["attributes"]["node_id"] != before["node_id"]), "a drain deletes nothing");

    // The birth is Draining, the same birth, and its process is still alive.
    let after = wait_for(&format!("{NODE} is draining"), Duration::from_secs(30), || async { estate.node_opt(NODE).await.filter(|n| n["status"] == "draining") }).await;
    assert_eq!(after["node_id"], before["node_id"]);
    assert_eq!(after["incarnation_id"], before["incarnation_id"]);
    assert!(std::path::Path::new(&format!("/proc/{pid}")).exists(), "the drained process {pid} is still running");
    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}
