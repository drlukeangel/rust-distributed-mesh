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
/// NEW node while the old NodeId departs exactly once, from the provider's proof, named by its
/// exact incarnation.
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
    let spans = estate.spans();
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted")
        .into_iter()
        .filter(|sp| sp["attributes"]["node_id"] == before["node_id"] && s(&sp["attributes"]["build_id"]) == birth_build)
        .collect();
    assert_eq!(deleted.len(), 1, "exactly one NodeDeleted for the old node: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact old birth");
    let (_, build) = estate.get(&format!("/api/builds?id={birth_build}")).await;
    assert_eq!(build["build_id"], birth_build.as_str(), "no Build was minted: {build}");
    estate.record_trace_url(drift["trace_id"].as_str().unwrap_or(""));
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT: `POST /api/nodes/{name}/replace` replaces a live node as the next attempt of the
/// accepted Build (reason replace, no Build minted): the old birth is retired and departs, and the
/// path comes back as a new NodeId.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_replace_retires_the_live_birth_and_creates_a_new_node() {
    let (estate, birth_build) = estate("a_planned_replace_retires_the_live_birth_and_creates_a_new_node").await;
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
    estate.await_build(&birth_build, SETTLE).await;
    let after = wait_for(&format!("{NODE} ready under a new NodeId"), SETTLE, || async {
        let n = estate.node_opt(NODE).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new node has a new transport key");
    let spans = wait_for("the old node's departure is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.node.delete.via-node-deleted").iter().any(|sp| sp["attributes"]["node_id"] == before["node_id"]).then_some(spans)
    })
    .await;
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "{deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"]);
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}
