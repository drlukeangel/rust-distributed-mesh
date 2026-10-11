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

/// A birth that holds or contends for a seat and serves (`NodeStatus::is_live`).
fn live(node: &Value) -> bool {
    matches!(node["status"].as_str(), Some("pending" | "ready-for-traffic" | "draining"))
}

/// `GET /api/nodes` from the admin at `base`, or `None` while it does not answer (an admin being
/// replaced, a successor not bound yet).
async fn view_of(base: &str) -> Option<Vec<Value>> {
    let http = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().ok()?;
    let r = http.get(format!("{base}/api/nodes")).send().await.ok()?;
    let v: Value = r.json().await.ok()?;
    v["nodes"].as_array().cloned()
}

/// Every admin's view at `bases` holds at most one live birth at `path`.
async fn at_most_one_live_birth(bases: &[String], path: &str) {
    for base in bases {
        let Some(nodes) = view_of(base).await else { continue };
        let at: Vec<(String, String)> = nodes.iter().filter(|n| n["name"] == path && live(n)).map(|n| (s(&n["node_id"]), s(&n["status"]))).collect();
        assert!(at.len() <= 1, "the view of {base} holds two live births at {path}: {at:?}");
    }
}

/// The predecessor `old`'s stop published its departure (`NodeStopped`) before its rename and before
/// the successor's identity (node-replace.md 1-3), and each of the `admins` admins dropped it from its
/// book on that NodeStopped, before the replace's Complete. The executor does not wait on peer
/// gossip ("The recipe's caller acts on streamed events and receipts, never on peer gossip"); that no
/// admin's view holds two live births at the path is the polled views' assertion.
fn assert_parked_before_the_successor(spans: &[Value], build: &str, attempt: &str, node: &str, old: &Value, admins: usize) {
    let stopped = named(spans, "rdm.node_admin.node.update.via-node-stopped")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == old["node_id"] && s(&sp["attributes"]["build_id"]) == build)
        .unwrap_or_else(|| panic!("the replace's stop published NodeStopped for {node} ({})", s(&old["node_id"])));
    assert_eq!(stopped["attributes"]["incarnation_id"], old["incarnation_id"], "NodeStopped names the exact old birth: {stopped}");
    let rename = named(spans, "rdm.node_admin.node.update.via-replace-rename").into_iter().find(|sp| sp["attributes"]["node_id"] == old["node_id"]).expect("the rename span");
    let allocate = step_span(spans, build, attempt, node, "AllocateIdentity").expect("AllocateIdentity ran");
    assert!(end(stopped) <= start(rename), "the predecessor is parked before it is renamed: {stopped} vs {rename}");
    assert!(end(stopped) <= start(allocate), "the predecessor is parked before the successor's identity exists");
    let complete = step_span(spans, build, attempt, node, "Complete").expect("Complete ran");
    // Each admin's book drops the parked birth once, on the NodeStopped it hears (the executor's own
    // book on its publish).
    let dropped: Vec<&Value> = named(spans, "rdm.mesh.membership.remove.via-node-stopped")
        .into_iter()
        .filter(|sp| sp["service"] == "rafka-node-admin" && sp["attributes"]["node_id"] == old["node_id"] && sp["attributes"]["was_held"] == "true" && start(sp) < start(complete))
        .collect();
    assert!(dropped.len() >= admins, "each of the {admins} admins dropped the parked {node} from its book on its NodeStopped: {dropped:?}");
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
    // Each process exports its own spans: the successor's boot span is read once the successor exported it.
    let spans = wait_for("the recovery operation's last step, its reconcile span and the successor's boot span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        (step_span(&spans, &birth_build, &attempt, NODE, "Complete").is_some() && reconcile_span(&spans, &birth_build, &attempt).is_some() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    assert_replace_followed_the_spec(&spans, &birth_build, &attempt, NODE, &before, &RECOVERY_ORDER);
    assert_recovery_deleted_the_old_identity_after_the_successor(&spans, &birth_build, &attempt, NODE, &before, &after);
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

/// Whether the boot span of `node`'s birth is exported (its own process exports it, on its own cadence).
fn booted(spans: &[Value], node: &Value) -> bool {
    named(spans, "rdm.mesh.node.create.via-deployment").into_iter().any(|sp| sp["attributes"]["node_id"] == node["node_id"])
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

/// The steps of the recovery of a birth the provider already proves terminal, one `replace-node:<path>`
/// operation: no RPC to the exited birth; it is renamed `<path>.old`, the successor's identity is
/// allocated, the exited birth is proven terminal and its storage handed over, the successor starts,
/// is admitted and ready, and only then the old identity is deleted.
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

/// The steps of a planned replace of a live birth, in the order node-replace.md numbers them: the old
/// birth drains (drain-node, node-drained) and stops (stop-node, node-left, its NodeStopped) and is
/// parked (1), is renamed `<path>.old` while parked (2), and node.create starts the successor at the
/// freed name (3). The predecessor stays parked with its data.
const REPLACE_ORDER: [&str; 11] = [
    "DrainNode",
    "AwaitNodeDrained",
    "StopNode",
    "AwaitNodeLeft",
    "RenamePredecessor",
    "AllocateIdentity",
    "DeployRuntime",
    "WaitForBind",
    "WaitForMeshJoin",
    "WaitForNodeReady",
    "Complete",
];

/// The steps a planned replace never runs: it deletes nothing, proves no exit and moves no data.
const NOT_IN_A_REPLACE: [&str; 5] = ["NodeDeleting", "TerminateRuntime", "HandoffStorage", "NodeDeleted", "ReleaseStorage"];

/// The replace of `node` in `attempt` of `build` ran as ONE operation `replace-node:<node>`, its steps in
/// `order`, and the predecessor was renamed `<node>.old` before the successor's identity was allocated.
fn assert_replace_followed_the_spec(spans: &[Value], build: &str, attempt: &str, node: &str, old: &Value, order: &[&str]) {
    let reconcile = reconcile_span(spans, build, attempt)
        .unwrap_or_else(|| panic!("the attempt's reconcile span is exported"));
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("replace-node:{node}"), "one replace operation, never retire then create: {reconcile}");
    let mut last = 0u64;
    for step in order {
        let sp = step_span(spans, build, attempt, node, step).unwrap_or_else(|| panic!("step {step} ran"));
        assert!(start(sp) >= last, "step {step} starts before the step it follows: {sp}");
        last = start(sp);
    }
    // The rename is spanned, names the predecessor's id, and happens before the successor's identity exists.
    let rename = named(spans, "rdm.node_admin.node.update.via-replace-rename").into_iter().find(|sp| sp["attributes"]["node_id"] == old["node_id"]).expect("the rename span");
    assert_eq!(s(&rename["attributes"]["from"]), node);
    assert_eq!(s(&rename["attributes"]["to"]), format!("{node}.old"));
    let allocate = step_span(spans, build, attempt, node, "AllocateIdentity").unwrap();
    assert!(end(rename) <= start(allocate), "the predecessor is renamed before the successor is created: {rename} vs {allocate}");
}

/// The recovery of the exited `old` at `node`: the successor admitted and ready before the old
/// identity's `NodeDeleted`, published once, and the exited birth proven terminal before the successor
/// starts.
fn assert_recovery_deleted_the_old_identity_after_the_successor(spans: &[Value], build: &str, attempt: &str, node: &str, old: &Value, new: &Value) {
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
    // The old birth is proven terminal before the successor starts.
    let terminate = step_span(spans, build, attempt, node, "TerminateRuntime").unwrap();
    let deploy = step_span(spans, build, attempt, node, "DeployRuntime").unwrap();
    assert!(end(terminate) <= start(deploy), "the predecessor is terminal before the successor starts");
}

/// A planned replace of `old` at `node` deleted nothing, proved no exit and moved no data: none of
/// [`NOT_IN_A_REPLACE`] ran, and no NodeDeleted was published for the old identity.
fn assert_replace_deleted_nothing(spans: &[Value], build: &str, attempt: &str, node: &str, old: &Value) {
    for step in NOT_IN_A_REPLACE {
        assert!(step_span(spans, build, attempt, node, step).is_none(), "a replace runs no {step}: the predecessor stays parked, its delete is the recipe's own later step");
    }
    let deleted: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == old["node_id"]).collect();
    assert!(deleted.is_empty(), "no NodeDeleted for the parked predecessor: {deleted:?}");
}

fn alive(pid: u64) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

/// CONTRACT: `POST /api/nodes/{name}/replace` replaces a live node as the next attempt of the
/// accepted Build (reason replace, no Build minted) by Rafka's recipe (node-replace.md 1-3): one
/// operation `replace-node:<path>`; the old birth drains and stops, its NodeStopped drops it from
/// every admin's book, and it is renamed `<path>.old` while parked; node.create then starts the
/// successor at the freed name, a new NodeId with a new transport key and its own directory. No
/// admin's view holds two live births at the path at any step. The predecessor stays parked with its
/// data: when the successor is ready, and after the operation completes, its process is alive and its
/// data is where it was ("Rafka transfers its data as its own recipe step"). The replace deletes
/// nothing (no NodeDeleting, terminal proof, storage hand-over or NodeDeleted): the predecessor's
/// row moves to `<path>.old` and stays its row, and the successor's row is seeded from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_replace_creates_the_successor_while_the_predecessor_stays_parked_with_its_data() {
    let (estate, birth_build) = estate("a_planned_replace_creates_the_successor_while_the_predecessor_stays_parked_with_its_data").await;
    const NODE: &str = "mesh1.rpc.1";
    let before = estate.node(NODE).await;
    // The predecessor's process and data: Rafka's to move after the create, never RDM's.
    let pid = estate.pid_of(NODE).await;
    let old_dir = std::path::PathBuf::from(estate.data_dir_of(NODE).await);
    let segment = old_dir.join("app-data").join("seg-0");
    std::fs::create_dir_all(segment.parent().unwrap()).unwrap();
    std::fs::write(&segment, b"records").unwrap();
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
    // While the replace runs, no admin's view holds two live births at the path at any step
    // (node-replace.md: the predecessor is parked and renamed before node.create claims the freed
    // name). That the view named the predecessor `<path>.old` is read from the span below.
    let admins: Vec<String> = estate.nodes().await.iter().filter(|n| n["kind"] == "node_admin").map(|n| s(&n["admin_api_base"])).collect();
    assert_eq!(admins.len(), 2, "both admins serve a view");
    let old_name = format!("{NODE}.old");
    let (after, parked_at_ready, data_at_ready) = wait_for(&format!("{NODE} ready under a new NodeId"), SETTLE, || async {
        at_most_one_live_birth(&admins, NODE).await;
        let nodes = estate.nodes().await;
        assert!(nodes.iter().filter(|n| n["name"] == NODE).count() <= 1, "two births at {NODE}: {nodes:#?}");
        let after = nodes.into_iter().find(|n| n["name"] == NODE && ready(n) && n["node_id"] != before["node_id"])?;
        Some((after, alive(pid), std::fs::read(&segment).ok()))
    })
    .await;
    assert!(parked_at_ready, "the predecessor's process {pid} is alive, parked, when the successor is ready");
    assert_eq!(data_at_ready.as_deref(), Some(&b"records"[..]), "the predecessor's data is in its own directory when the successor is ready: {segment:?}");

    estate.await_attempt(&birth_build, attempt_no, SETTLE).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new node has a new transport key");
    let attempt = attempt_no.to_string();
    let spans = wait_for("the replace operation's last step, its reconcile span and the successor's boot span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        (step_span(&spans, &birth_build, &attempt, NODE, "Complete").is_some() && reconcile_span(&spans, &birth_build, &attempt).is_some() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    assert_replace_followed_the_spec(&spans, &birth_build, &attempt, NODE, &before, &REPLACE_ORDER);
    assert_replace_deleted_nothing(&spans, &birth_build, &attempt, NODE, &before);
    assert_parked_before_the_successor(&spans, &birth_build, &attempt, NODE, &before, admins.len());
    let held_old = named(&spans, "rdm.node_admin.topology.update.via-predecessor-held")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == old_name.as_str() && sp["attributes"]["node_id"] == before["node_id"])
        .unwrap_or_else(|| panic!("the executor's view never named the predecessor {old_name}"));
    let complete = step_span(&spans, &birth_build, &attempt, NODE, "Complete").expect("Complete ran");
    assert!(start(held_old) < start(complete), "the view named the predecessor {old_name} before the replace completed: {held_old}");
    // The node's row (ops-naming "Node data": "a Deleted row, or the <name>.old row"): at the
    // executor's RenamePredecessor the fabric-primary moves the predecessor's row to `<path>.old`,
    // where it stays the parked predecessor's row, and the successor's row at the path.name is
    // claimed seeded from the .old row.
    let moved: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-rename").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(moved.len(), 1, "one move of the predecessor's row: {moved:?}");
    assert_eq!((s(&moved[0]["attributes"]["from"]), s(&moved[0]["attributes"]["to"]), s(&moved[0]["attributes"]["outcome"])), (NODE.to_string(), old_name.clone(), "renamed".to_string()), "{}", moved[0]);
    let old_deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == old_name.as_str()).collect();
    assert!(old_deleted.is_empty(), "no Deleted row at {old_name}: the predecessor is parked, not deleted: {old_deleted:?}");
    let seeded: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == NODE && sp["attributes"]["node_id"] == after["node_id"]).collect();
    assert_eq!(seeded.len(), 1, "the successor claims its row once: {seeded:?}");
    assert_eq!((s(&seeded[0]["attributes"]["outcome"]), s(&seeded[0]["attributes"]["seeded_from"])), ("row-seeded".to_string(), s(&before["node_id"])), "{}", seeded[0]);
    assert!(start(moved[0]) < start(seeded[0]), "the predecessor's row moved before the successor claimed the path.name");
    estate.artifact("node-rows.json", &json!({"moved": moved[0], "seeded": seeded[0]}));
    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    // After the operation: the predecessor is parked with its data, the successor has none of it.
    assert!(alive(pid), "the predecessor's process {pid} is alive, parked, after the replace completed");
    assert_eq!(std::fs::read(&segment).unwrap(), b"records", "the predecessor's data stays where it was: {segment:?}");
    let new_dir = std::path::PathBuf::from(estate.data_dir_of(NODE).await);
    assert_ne!(new_dir, old_dir, "the successor has its own directory");
    assert!(!new_dir.join("app-data").exists(), "RDM handed nothing to the successor's directory: {new_dir:?}");
    // The shape is whole: two admins and three rpc nodes ready, the successor the one birth at the
    // path; the parked predecessor is no live birth.
    let settled = wait_for("mesh1 holds 2 admins and 3 rpc nodes ready besides the parked predecessor", Duration::from_secs(30), || async {
        let nodes = estate.nodes().await;
        let rest: Vec<&Value> = nodes.iter().filter(|n| n["node_id"] != before["node_id"]).collect();
        let count = |kind: &str| rest.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == kind).count();
        (count("node_admin") == 2 && count("rpc_node") == 3 && rest.iter().all(|n| ready(n))).then_some(nodes)
    })
    .await;
    assert_eq!(settled.iter().filter(|n| n["name"] == NODE).count(), 1, "exactly one birth at {NODE}");
    assert!(!settled.iter().any(|n| n["node_id"] == before["node_id"] && live(n)), "the parked predecessor is no live birth: {settled:#?}");
    estate.shutdown().await;
}

/// The steps of `node.delete` of a parked birth (node-decommission.md: its stop steps are already done,
/// "Stop's node.stopped is pre-exit (parked) and insufficient"): the removal is announced, the
/// provider's exact Exited is the proof, then the executor publishes NodeDeleted and the operation's
/// Complete names the birth. Storage is not released ("It releases node.id and name, not storage").
const DELETE_PARKED_ORDER: [&str; 4] = ["NodeDeleting", "TerminateRuntime", "NodeDeleted", "Complete"];

/// CONTRACT: the recipe's `node[1].delete` of the parked predecessor a replace left at `<path>.old`
/// (node-replace.md; node-decommission.md 2-4): `DELETE /api/nodes/<path>.old` is the next attempt of
/// the accepted Build (reason delete, action delete fenced to the exact old birth, no Build minted).
/// One operation `delete-node:<path>.old` asks the provider to end the parked runtime and inspects
/// it; only the provider's Exited is the proof, after which the executor publishes NodeDeleted for
/// the exact old birth and the fabric-primary writes the Deleted row at `<path>.old`, its record
/// kept. The predecessor's process is gone; its data dir stays; the successor at the path is
/// untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_the_parked_predecessor_proves_its_exit_publishes_node_deleted_and_writes_its_deleted_row() {
    let (estate, birth_build) = estate("deleting_the_parked_predecessor_proves_its_exit_publishes_node_deleted_and_writes_its_deleted_row").await;
    const NODE: &str = "mesh1.rpc.1";
    let before = estate.node(NODE).await;
    let pid = estate.pid_of(NODE).await;
    let old_dir = std::path::PathBuf::from(estate.data_dir_of(NODE).await);
    let (status, r) = estate.post(&format!("/api/nodes/{NODE}/replace"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    estate.await_attempt(&birth_build, Estate::attempt_of(&r), SETTLE).await;
    let after = wait_for(&format!("{NODE} ready under a new NodeId"), SETTLE, || async {
        estate.nodes().await.into_iter().find(|n| n["name"] == NODE && ready(n) && n["node_id"] != before["node_id"])
    })
    .await;
    assert!(alive(pid), "the predecessor {pid} is parked, alive, before its delete");
    let old_name = format!("{NODE}.old");

    let (status, d) = estate.delete(&format!("/api/nodes/{old_name}")).await;
    assert_eq!(status, 202, "node.delete of the parked {old_name} is accepted: {d}");
    assert_eq!(s(&d["build_id"]), birth_build, "no Build is minted: {d}");
    let opened = wait_for("the delete attempt is the Build's current attempt", Duration::from_secs(30), || async {
        let (_, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
        (b["reason"] == "delete").then_some(b)
    })
    .await;
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("delete", old_name.as_str()), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "fenced to the exact old birth: {opened}");
    let attempt_no = Estate::attempt_of(&d);
    estate.await_attempt(&birth_build, attempt_no, SETTLE).await;
    let attempt = attempt_no.to_string();
    let spans = wait_for("the delete's last step, its reconcile span and the Deleted row are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let row = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().any(|sp| sp["attributes"]["subject"] == old_name.as_str());
        (step_span(&spans, &birth_build, &attempt, &old_name, "Complete").is_some() && reconcile_span(&spans, &birth_build, &attempt).is_some() && row).then_some(spans)
    })
    .await;
    let reconcile = reconcile_span(&spans, &birth_build, &attempt).unwrap();
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("delete-node:{old_name}"), "one delete operation of the parked birth: {reconcile}");
    let mut last = 0u64;
    for step in DELETE_PARKED_ORDER {
        let sp = step_span(&spans, &birth_build, &attempt, &old_name, step).unwrap_or_else(|| panic!("step {step} of the delete ran"));
        assert!(start(sp) >= last, "step {step} starts before the step it follows: {sp}");
        last = start(sp);
    }
    for step in ["DrainNode", "StopNode", "ReleaseStorage"] {
        assert!(step_span(&spans, &birth_build, &attempt, &old_name, step).is_none(), "the parked birth's delete runs no {step}");
    }
    // The provider's Exited is the proof, and it precedes the NodeDeleted the executor publishes.
    let terminate = step_span(&spans, &birth_build, &attempt, &old_name, "TerminateRuntime").unwrap();
    assert_eq!(s(&terminate["attributes"]["outcome"]), "complete", "{terminate}");
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "NodeDeleted once for the old identity: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact old birth");
    assert_eq!(s(&deleted[0]["attributes"]["operation"]), format!("delete-node:{old_name}"));
    assert!(end(terminate) <= start(deleted[0]), "NodeDeleted follows the exit proof: {terminate} vs {}", deleted[0]);
    // The fabric-primary's Deleted row at `<path>.old`, the record kept.
    let rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == old_name.as_str()).collect();
    assert_eq!(rows.len(), 1, "one Deleted row at {old_name}: {rows:?}");
    assert_eq!((s(&rows[0]["attributes"]["node_id"]), s(&rows[0]["attributes"]["outcome"])), (s(&before["node_id"]), "row-deleted".to_string()), "{}", rows[0]);
    estate.artifact("deleted-row.json", &json!({"row": rows[0], "node_deleted": deleted[0], "terminate": terminate}));
    let op = named(&spans, "rdm.node_admin.node.delete.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == old_name.as_str()).expect("the delete operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    // The parked process is gone, its data dir stays, and the successor is untouched.
    assert!(!alive(pid), "the parked predecessor's process {pid} exited");
    assert!(old_dir.exists(), "node.delete releases node.id and name, not storage: {old_dir:?}");
    let now = estate.node(NODE).await;
    assert!(now["node_id"] == after["node_id"] && ready(&now), "the successor at {NODE} is untouched: {now}");
    estate.shutdown().await;
}

/// CONTRACT: a planned replace of a node-admin that holds no seat (node-replace.md, the same recipe
/// as any node): its stop parks it and its NodeStopped drops it from every book before it is renamed
/// `<path>.old` and before its successor exists, so no admin's view holds two live births at the path
/// at any step, and the successor admin's own view holds itself at the path, ready, under its new
/// NodeId (never the predecessor's birth).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_node_admin_sees_itself_and_no_view_holds_two_live_births() {
    let (estate, birth_build) = estate("a_replaced_node_admin_sees_itself_and_no_view_holds_two_live_births").await;
    let nodes = estate.nodes().await;
    let admins: Vec<&Value> = nodes.iter().filter(|n| n["kind"] == "node_admin").collect();
    assert_eq!(admins.len(), 2, "{nodes:#?}");
    let before = admins.iter().find(|n| n["is_primary"] != true && n["is_fabric_primary"] != true).map(|n| (*n).clone()).expect("an admin that holds no seat");
    let stays = s(&admins.iter().find(|n| n["node_id"] != before["node_id"]).unwrap()["admin_api_base"]);
    let node = s(&before["name"]);
    let (status, r) = estate.post(&format!("/api/nodes/{node}/replace"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    assert_eq!(s(&r["build_id"]), birth_build, "no Build is minted: {r}");
    let attempt_no = Estate::attempt_of(&r);
    let after = wait_for(&format!("{node} ready under a new NodeId"), SETTLE, || async {
        at_most_one_live_birth(&[stays.clone()], &node).await;
        let nodes = view_of(&stays).await?;
        let successor = nodes.into_iter().find(|n| n["name"] == node.as_str() && ready(n) && n["node_id"] != before["node_id"])?;
        at_most_one_live_birth(&[s(&successor["admin_api_base"])], &node).await;
        Some(successor)
    })
    .await;
    // The successor admin's own view: itself at the path, ready, under its new NodeId.
    let own = s(&after["admin_api_base"]);
    let me = wait_for(&format!("the successor {node} sees itself"), Duration::from_secs(30), || async {
        let nodes = view_of(&own).await?;
        let at: Vec<Value> = nodes.into_iter().filter(|n| n["name"] == node.as_str() && live(n)).collect();
        assert!(at.len() <= 1, "the successor's view holds two live births at {node}: {at:#?}");
        at.into_iter().find(|n| n["node_id"] == after["node_id"] && ready(n))
    })
    .await;
    assert_eq!(me["incarnation_id"], after["incarnation_id"], "the successor's view holds its own birth: {me}");
    estate.await_attempt(&birth_build, attempt_no, SETTLE).await;
    let attempt = attempt_no.to_string();
    let spans = wait_for("the replace operation's last step is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        step_span(&spans, &birth_build, &attempt, &node, "Complete").is_some().then_some(spans)
    })
    .await;
    // The admins that hold the predecessor in their books: every admin but the predecessor itself.
    assert_parked_before_the_successor(&spans, &birth_build, &attempt, &node, &before, 1);
    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == node.as_str()).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}
