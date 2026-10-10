//! product=mesh, feature=node-lifecycle, subfeature=node-replace, rung=MN, provider=process.
//!
//! Replace and the unplanned-exit classification (Luke R-R1, R-T5). A planned replacement and
//! every unplanned exit are the next attempt of the accepted Build with a typed action; no Build
//! is minted. An exit whose reason is the reserved `TRANSPORT_STOPPED` (exit code 4) is restarted
//! as the same node; every other exit is replaced by a new node.

use rafka_test_scenario::estate::{descends_from, named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-replace".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn ready(node: &Value) -> bool {
    node["status"] == "ready-for-traffic"
}

/// mesh1 with two node-admins and three rpc nodes, through the rectifier; the birth Build's id.
async fn estate(test: &str) -> (Estate, String) {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]})).await;
    assert_eq!(status, 202, "{a}");
    let build = s(&a["build_id"]);
    estate.await_build(&build, SETTLE).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(60)).await;
    (estate, build)
}

/// Two meshes, so a mesh2 node is launched by mesh2's admin and proven by the fabric-primary in mesh1,
/// which did not launch it and so cannot read its exit code from a child handle.
async fn two_mesh_estate(test: &str) -> (Estate, String) {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 2}, {"name": "mesh2", "node_admin": 1, "rpc_node": 2}]}))
        .await;
    assert_eq!(status, 202, "{a}");
    let build = s(&a["build_id"]);
    estate.await_build(&build, SETTLE).await;
    estate.settled_shape(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(90)).await;
    (estate, build)
}

/// The drift attempt's span for `node` carrying `action`, once exported.
async fn drift_span(estate: &Estate, build: &str, node: &str, action: &str) -> Value {
    wait_for(&format!("the proven-drift attempt for {node} ({action}) is exported"), Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.build.update.via-proven-drift")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["action"]) == action && s(&sp["attributes"]["scope"]).contains(&format!("exited: {node}")))
            .cloned()
    })
    .await
}

/// CONTRACT: a rpc node whose mesh transport stops for good exits with the reserved code 4 and
/// records that reason in its data dir; the fabric-primary proves the exit from the provider and
/// the record, and the next attempt of the same Build (reason proven drift, action restart, no
/// Build minted) brings the SAME node back: same NodeId, same transport key, same data dir, a new
/// incarnation. No departure is published for it, and no other node changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_stopped_exit_is_restarted_as_the_same_node() {
    let (estate, birth_build) = two_mesh_estate("a_transport_stopped_exit_is_restarted_as_the_same_node").await;
    // The node lives in the mesh that does NOT hold the fabric-primary: its mesh's admin launched it, so the
    // fabric-primary has no child handle and proves its exit from the provider and the runtime's own record.
    let primary = estate.nodes().await.into_iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary");
    let node = format!("{}.rpc.1", if primary["mesh"] == "mesh1" { "mesh2" } else { "mesh1" });
    let node_path = node.as_str();
    let before = estate.node(node_path).await;
    let data_dir = estate.data_dir_of(node_path).await;
    let others: Vec<(String, String)> = estate.nodes().await.iter().filter(|n| n["name"] != node_path).map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();

    // The real transport-stop path: the testkit door marks the transport stopped; the process exits through it.
    let marked = estate.probe(&["stop-transport", "--target", &format!("path:{node_path}"), "--key", "x"]);
    assert_eq!(marked["outcome"], "Reply", "{marked}");

    let after = wait_for(&format!("{node_path} ready under a new incarnation"), SETTLE, || async {
        let n = estate.node_opt(node_path).await?;
        (ready(&n) && n["incarnation_id"] != before["incarnation_id"]).then_some(n)
    })
    .await;
    assert_eq!(after["node_id"], before["node_id"], "the same NodeId");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same transport key");
    assert_eq!(estate.data_dir_of(node_path).await, data_dir, "the same data dir");
    assert_ne!(after["deployment_id"], before["deployment_id"], "a new runtime");

    // The record the exiting runtime wrote is keyed to the birth that wrote it (the new birth cleared it at spawn).
    let drift = drift_span(&estate, &birth_build, node_path, "restart").await;
    assert_eq!(s(&drift["attributes"]["exit_code"]), "4", "the proof names TRANSPORT_STOPPED: {drift}");
    // mesh2's admin launched the node, so the fabric-primary has no child handle: the code 4 is the
    // runtime's own record in its data dir, keyed to its deployment and incarnation.
    assert_eq!(s(&drift["attributes"]["exit_proof"]), "exit-record", "{drift}");
    assert_eq!(s(&drift["attributes"]["reason"]), "proven-drift");
    // The node is Ready before the attempt closes; the same Build settles with the repair as its attempt.
    let build = estate.await_build(&birth_build, SETTLE).await;
    assert_eq!(build["build_id"], birth_build.as_str(), "the same Build carries the repair: {build}");
    assert_eq!(s(&build["reason"]), "proven-drift", "{build}");
    // The restart's own span closes when its attempt ends: read the evidence once it is exported,
    // never from a snapshot taken before the Build settled.
    let spans = wait_for("the restart attempt's spans are exported", SETTLE, || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.node.update.via-build")
            .into_iter()
            .any(|sp| sp["attributes"]["node"] == node_path && s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == s(&drift["attributes"]["attempt"]))
            .then_some(spans)
    })
    .await;

    // Restart, not replace: the NodeId never departs, and the node's logs say why it exited.
    let departed: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert!(departed.is_empty(), "a restart emits no NodeDeleted for the same node: {departed:?}");
    let stopped = named(&spans, "rdm.mesh.node.delete.via-transport-stopped");
    assert!(!stopped.is_empty(), "the exiting runtime's own span names the reason");
    let restarts = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().filter(|sp| sp["attributes"]["node"] == node_path && s(&sp["attributes"]["build_id"]) == birth_build).collect::<Vec<_>>();
    assert!(restarts.iter().any(|r| descends_from(&spans, r, &drift) || s(&r["attributes"]["attempt"]) == s(&drift["attributes"]["attempt"])), "the restart ran under the drift attempt: {restarts:?}");
    estate.record_trace_url(drift["trace_id"].as_str().unwrap_or(""));

    // Nothing else moved.
    for (name, id) in others {
        assert_eq!(s(&estate.node(&name).await["node_id"]), id, "{name} is untouched");
    }
    estate.settled_shape(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT: every other unplanned exit is replaced. A rpc node killed with SIGKILL leaves no
/// recorded reason: the fabric-primary proves the exit from the provider alone, the next attempt of
/// the same Build is a Replace fenced to that birth (no Build minted), and the path comes back as a
/// NEW node: the old identity is renamed `<path>.old`, its storage handed over, the successor started
/// and ready, and only then does the old NodeId depart, exactly once, from the provider's proof,
/// named by its exact incarnation. No drain is sent to the proven-terminal birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn any_other_unplanned_exit_is_replaced_by_a_new_node() {
    let (estate, birth_build) = estate("any_other_unplanned_exit_is_replaced_by_a_new_node").await;
    const NODE: &str = "mesh1.rpc.3";
    let before = estate.node(NODE).await;
    estate.kill_node(NODE).await;

    let after = wait_for(&format!("{NODE} re-created ready under a new NodeId"), SETTLE, || async {
        let n = estate.node_opt(NODE).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    assert_ne!(after["incarnation_id"], before["incarnation_id"]);

    let drift = drift_span(&estate, &birth_build, NODE, "replace").await;
    assert_eq!(s(&drift["attributes"]["exit_code"]), "", "a killed runtime proves no reason: {drift}");
    assert_eq!(s(&drift["attributes"]["exit_proof"]), "none", "{drift}");
    let attempt = s(&drift["attributes"]["attempt"]);
    let spans = wait_for("the recovery operation's last step and its reconcile span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        (step_span(&spans, &birth_build, &attempt, NODE, "Complete").is_some() && reconcile_span(&spans, &birth_build, &attempt).is_some()).then_some(spans)
    })
    .await;
    assert_replace_followed_the_spec(&spans, &birth_build, &attempt, NODE, &before, &after, &RECOVERY_ORDER);
    for step in ["DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft"] {
        assert!(step_span(&spans, &birth_build, &attempt, NODE, step).is_none(), "no RPC is required of the proven-terminal old birth: {step} ran");
    }
    assert!(
        named(&spans, "rdm.node_admin.node.create.via-build").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE),
        "the recovery's node operation span is node.create.via-build"
    );
    let (_, build) = estate.get(&format!("/api/builds?id={birth_build}")).await;
    assert_eq!(build["build_id"], birth_build.as_str(), "no Build was minted: {build}");
    estate.record_trace_url(drift["trace_id"].as_str().unwrap_or(""));
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// The attempt's reconcile span. It closes after the attempt's last step, so it is exported after
/// that step's span.
fn reconcile_span<'a>(spans: &'a [Value], build: &str, attempt: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt)
}

/// The `deployment.update.via-step` span of `step` for `node` in `attempt` of `build`.
fn step_span<'a>(spans: &'a [Value], build: &str, attempt: &str, node: &str, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && s(&sp["attributes"]["node"]) == node && s(&sp["attributes"]["step"]) == step)
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().expect("a span start")
}

fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().expect("a span end")
}

/// The steps of one `replace-node:<path>` operation, in the order node-replace.md numbers them: the
/// old birth drains (drain-node, node-drained), is renamed `<path>.old`, the successor's identity is
/// allocated, the old birth is stopped (stop-node, node-left) with exact terminal proof and its storage handed over, the successor starts, is
/// admitted and ready, and only then the old identity is deleted.
const RECOVERY_ORDER: [&str; 11] = [
    "NodeDeleting",
    "RenamePredecessor",
    "AllocateIdentity",
    "TerminateRuntime",
    "HandoffStorage",
    "DeployRuntime",
    "WaitForBind",
    "WaitForMeshJoin",
    "WaitForNodeReady",
    "NodeDeleted",
    "Complete",
];

/// The same, for a planned replace of a live birth.
const REPLACE_ORDER: [&str; 15] = [
    "NodeDeleting",
    "DrainNode",
    "AwaitNodeDrained",
    "RenamePredecessor",
    "AllocateIdentity",
    "StopNode",
    "AwaitNodeLeft",
    "TerminateRuntime",
    "HandoffStorage",
    "DeployRuntime",
    "WaitForBind",
    "WaitForMeshJoin",
    "WaitForNodeReady",
    "NodeDeleted",
    "Complete",
];

/// The replace of `node` in `attempt` of `build` ran as ONE operation `replace-node:<node>`, its steps in
/// [`REPLACE_ORDER`], the successor admitted and ready before the old identity's `NodeDeleted`, and the
/// predecessor renamed `<node>.old` before the successor's identity was allocated.
///
/// `order` is [`REPLACE_ORDER`] for a planned replace; for the recovery of a birth already proven
/// terminal ([`RECOVERY_ORDER`]) no drain is sent to it.
fn assert_replace_followed_the_spec(spans: &[Value], build: &str, attempt: &str, node: &str, old: &Value, new: &Value, order: &[&str]) {
    let reconcile = reconcile_span(spans, build, attempt)
        .unwrap_or_else(|| panic!("the attempt's reconcile span is exported"));
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("replace-node:{node}"), "one replace operation, never retire then create: {reconcile}");
    let mut last = 0u64;
    for step in order {
        let sp = step_span(spans, build, attempt, node, step).unwrap_or_else(|| panic!("step {step} ran"));
        assert!(start(sp) >= last, "step {step} starts before the step it follows: {sp}");
        last = start(sp);
    }
    let ready = step_span(spans, build, attempt, node, "WaitForNodeReady").unwrap();
    let deleted_step = step_span(spans, build, attempt, node, "NodeDeleted").unwrap();
    assert!(start(deleted_step) >= end(ready), "NodeDeleted of the old identity is published after the successor is ready: {deleted_step} vs {ready}");
    // The gossip event itself, exactly once, for the old identity, after the successor's boot.
    let deleted: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == old["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "{deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], old["incarnation_id"], "named by the exact old birth");
    assert_eq!(s(&deleted[0]["attributes"]["operation"]), format!("replace-node:{node}"));
    let boot = named(spans, "rdm.mesh.node.create.via-deployment").into_iter().find(|sp| sp["attributes"]["node_id"] == new["node_id"]).expect("the successor's boot span");
    assert!(start(boot) < start(deleted[0]), "the successor booted before the old identity was deleted");
    // The rename is spanned, names the predecessor's id, and happens before the successor's identity exists.
    let rename = named(spans, "rdm.node_admin.node.update.via-replace-rename").into_iter().find(|sp| sp["attributes"]["node_id"] == old["node_id"]).expect("the rename span");
    assert_eq!(s(&rename["attributes"]["from"]), node);
    assert_eq!(s(&rename["attributes"]["to"]), format!("{node}.old"));
    let allocate = step_span(spans, build, attempt, node, "AllocateIdentity").unwrap();
    assert!(end(rename) <= start(allocate), "the predecessor is renamed before the successor is created: {rename} vs {allocate}");
    // The old birth is stopped, with exact terminal proof, before the successor starts.
    let terminate = step_span(spans, build, attempt, node, "TerminateRuntime").unwrap();
    let deploy = step_span(spans, build, attempt, node, "DeployRuntime").unwrap();
    assert!(end(terminate) <= start(deploy), "the predecessor is terminal before the successor starts");
}

/// CONTRACT: `POST /api/nodes/{name}/replace` replaces a live node as the next attempt of the
/// accepted Build (reason replace, no Build minted) in the order node-replace.md gives: one
/// operation `replace-node:<path>`; the old birth drains and is renamed `<path>.old`; the successor's
/// identity is allocated; the old birth is stopped and proven terminal; the successor starts, joins
/// and is ready; only then is the old identity deleted (NodeDeleted, once). The path comes back as a
/// new NodeId with a new transport key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_replace_admits_the_successor_before_the_old_identity_is_deleted() {
    let (estate, birth_build) = estate("a_planned_replace_admits_the_successor_before_the_old_identity_is_deleted").await;
    const NODE: &str = "mesh1.rpc.1";
    let before = estate.node(NODE).await;
    let (status, r) = estate.post(&format!("/api/nodes/{NODE}/replace"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    assert_eq!(s(&r["build_id"]), birth_build, "no Build is minted: {r}");
    // The attempt is the Build's current attempt as the admin at the control API learns it.
    let opened = wait_for("the replace attempt is the Build's current attempt", Duration::from_secs(30), || async {
        let (_, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
        (b["reason"] == "replace").then_some(b)
    })
    .await;
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("replace", NODE), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "fenced to the live birth: {opened}");
    let attempt_no = Estate::attempt_of(&r);
    // While the replace runs, /api/nodes never holds two births at `<path>`: the successor is the one
    // birth there. That the view named the predecessor `<path>.old` is read from the span below, not
    // from this poll: the window is the drain-to-NodeDeleted span of the replace.
    let old_name = format!("{NODE}.old");
    let after = wait_for(&format!("{NODE} ready under a new NodeId"), SETTLE, || async {
        let nodes = estate.nodes().await;
        assert!(nodes.iter().filter(|n| n["name"] == NODE).count() <= 1, "two births at {NODE}: {nodes:#?}");
        nodes.into_iter().find(|n| n["name"] == NODE && ready(n) && n["node_id"] != before["node_id"])
    })
    .await;

    estate.await_attempt(&birth_build, attempt_no, SETTLE).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new node has a new transport key");
    let attempt = attempt_no.to_string();
    let spans = wait_for("the replace operation's last step and its reconcile span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        (step_span(&spans, &birth_build, &attempt, NODE, "Complete").is_some() && reconcile_span(&spans, &birth_build, &attempt).is_some()).then_some(spans)
    })
    .await;
    assert_replace_followed_the_spec(&spans, &birth_build, &attempt, NODE, &before, &after, &REPLACE_ORDER);
    let held_old = named(&spans, "rdm.node_admin.topology.update.via-predecessor-held")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == old_name.as_str() && sp["attributes"]["node_id"] == before["node_id"])
        .unwrap_or_else(|| panic!("the executor's view never named the predecessor {old_name}"));
    let deleted_step = step_span(&spans, &birth_build, &attempt, NODE, "NodeDeleted").expect("NodeDeleted ran");
    assert!(start(held_old) < start(deleted_step), "the view named the predecessor {old_name} before its NodeDeleted: {held_old}");
    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    let settled = estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    assert_eq!(settled.iter().filter(|n| n["name"] == NODE).count(), 1, "exactly one birth at {NODE}");
    assert!(!settled.iter().any(|n| n["name"] == old_name.as_str()), "the departed predecessor is gone from the view");
    estate.shutdown().await;
}
