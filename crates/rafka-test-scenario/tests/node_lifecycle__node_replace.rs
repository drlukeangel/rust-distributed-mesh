//! product=mesh, feature=node-lifecycle, subfeature=node-replace, rung=MN, provider=process.
//!
//! A planned replace (Luke R-R1) and node recovery (node-recovery.md, R-RC1). A planned
//! replacement is the next attempt of the accepted Build with a typed action; no Build is minted.
//! A member that exits outside any workflow is recovered by its own mesh-primary on the provider's
//! exact exit proof: node.delete of the exited instance, then node.create at the same path.name
//! over the inherited data dir (same endpoint key, new node.id). A member that is hung but still
//! running is never recovered; a repeat submit is answered, never run twice; an exit during a
//! workflow fails that workflow and the next tick recovers the member.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::process_faults::{ExactRuntime, Fault};
use serde_json::{json, Value};
use std::path::Path;
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
/// book on that NodeStopped, before the replace's NodeDeleted. The executor does not wait on peer
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
    let deleted = step_span(spans, build, attempt, node, "NodeDeleted").expect("NodeDeleted ran");
    // Each admin's book drops the parked birth once, on the NodeStopped it hears (the executor's own
    // book on its publish).
    let dropped: Vec<&Value> = named(spans, "rdm.mesh.membership.remove.via-node-stopped")
        .into_iter()
        .filter(|sp| sp["service"] == "rafka-node-admin" && sp["attributes"]["node_id"] == old["node_id"] && sp["attributes"]["was_held"] == "true" && start(sp) < start(deleted))
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

/// The mesh-primary's recovery submit for `node`, once exported (node-recovery.md: the tickle,
/// the TRUE OFFLINE mark, the provider inspection and the submit each have a span on the
/// mesh-primary).
async fn recovery_span(estate: &Estate, node: &str, node_id: &Value) -> Value {
    wait_for(&format!("the mesh-primary's recovery submit for {node} is exported"), Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| sp["attributes"]["node"] == node && sp["attributes"]["node_id"] == *node_id).cloned()
    })
    .await
}

/// The member under test in a two-mesh estate: `<mesh>.rpc.1` of the mesh that does NOT hold the
/// fabric-primary, so its mesh-primary is not the fabric-primary; that mesh-primary's path.name; and
/// the fabric-primary's view row.
fn placement(nodes: &[Value]) -> (String, String, Value) {
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh = if primary["mesh"] == "mesh1" { "mesh2" } else { "mesh1" };
    let mesh_primary = s(&nodes.iter().find(|n| n["mesh"] == mesh && n["kind"] == "node_admin" && n["is_primary"] == true).expect("the node's mesh-primary")["name"]);
    assert_ne!(mesh_primary, s(&primary["name"]), "the member's mesh-primary is not the fabric-primary");
    (format!("{mesh}.rpc.1"), mesh_primary, primary)
}

/// The exact runtime `node`'s birth published in its data dir: the fault door acts on it alone.
async fn exact_runtime(estate: &Estate, node: &str) -> ExactRuntime {
    let dir = estate.data_dir_of(node).await;
    ExactRuntime::published(Path::new(&dir)).unwrap_or_else(|r| panic!("{node}: no exact runtime published in {dir}: {r:?}"))
}

/// The highest attempt of Build `build` as the admin at `api` holds it.
async fn attempt_at(estate: &Estate, api: &str, build: &str) -> u64 {
    let (_, b) = estate.http_get(api, &format!("/api/builds?id={build}")).await;
    b["attempt"].as_u64().unwrap_or_else(|| panic!("Build {build} at {api} names no attempt: {b}"))
}

/// The fabric-primary's own Build log holds the recovery of `node` at `attempt`, heard on the Build
/// topic: the `recover-node:<node>` NodeDeleted receipt and the operation's Complete.
async fn fp_recovery_receipts(estate: &Estate, fp_api: &str, build: &str, node: &str, attempt: &str) -> (Value, Value) {
    let operation = format!("recover-node:{node}");
    wait_for("the fabric-primary holds the recovery's receipts", Duration::from_secs(30), || async {
        let (_, b) = estate.http_get(fp_api, &format!("/api/builds?id={build}")).await;
        let steps = b["steps"].as_array().cloned().unwrap_or_default();
        let at = |step: &str| steps.iter().find(|r| r["operation"] == operation.as_str() && r["step"] == step && r["attempt"].to_string() == attempt && r["outcome"] == "complete").cloned();
        Some((at("NodeDeleted")?, at("Complete")?))
    })
    .await
}

/// CONTRACT: a rpc node whose mesh transport stops for good exits (reserved code 4, recorded in
/// its data dir). Its mesh-primary (node-recovery.md), not the fabric-primary, proves the exit:
/// it marks the node TRUE OFFLINE, asks the provider about the exact runtime, and on Exited
/// submits one recovery to the fabric-primary keyed on the exited birth; the recovery is the next
/// attempt of the accepted Build (no Build minted), run by that mesh-primary: node.delete of the
/// exited instance (node.deleted from the exit proof, once, named by the exact birth), then
/// node.create at the same path.name over the inherited data dir. The path comes back with the
/// SAME transport key and data dir and a NEW node.id and incarnation. No other node changes, and
/// the fabric-primary's drift check opens nothing for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_stopped_exit_is_restarted_as_the_same_node() {
    let (estate, birth_build) = two_mesh_estate("a_transport_stopped_exit_is_restarted_as_the_same_node").await;
    // The node lives in the mesh that does NOT hold the fabric-primary: its own mesh-primary is the
    // reconciler, and it submits the recovery across meshes to the fabric-primary.
    let nodes = estate.nodes().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh = if primary["mesh"] == "mesh1" { "mesh2" } else { "mesh1" };
    let node = format!("{mesh}.rpc.1");
    let node_path = node.as_str();
    let mesh_primary = s(&nodes.iter().find(|n| n["mesh"] == mesh && n["kind"] == "node_admin" && n["is_primary"] == true).expect("the node's mesh-primary")["name"]);
    let before = estate.node(node_path).await;
    let data_dir = estate.data_dir_of(node_path).await;
    let others: Vec<(String, String)> = nodes.iter().filter(|n| n["name"] != node_path).map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();

    // The real transport-stop path: the testkit door marks the transport stopped; the process exits through it.
    let marked = estate.probe(&["stop-transport", "--target", &format!("path:{node_path}"), "--key", "x"]);
    assert_eq!(marked["outcome"], "Reply", "{marked}");

    let after = wait_for(&format!("{node_path} ready as a new node"), SETTLE, || async {
        let n = estate.node_opt(node_path).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    assert_ne!(after["node_id"], before["node_id"], "recovery creates a new node.id");
    assert_ne!(after["incarnation_id"], before["incarnation_id"], "and a new incarnation");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same transport key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir_of(node_path).await, data_dir, "the same data dir");
    assert_ne!(after["deployment_id"], before["deployment_id"], "a new runtime");

    // The mesh-primary proved the exit and submitted the recovery, keyed on the exited birth.
    let recovery = recovery_span(&estate, node_path, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "the node's own mesh-primary submits: {recovery}");
    assert_eq!(recovery["attributes"]["incarnation_id"], before["incarnation_id"], "keyed on the exact exited birth: {recovery}");
    assert_eq!(s(&recovery["attributes"]["exit_code"]), "4", "the proof names TRANSPORT_STOPPED: {recovery}");
    // The mesh-primary launched its member, so its provider holds the runtime's own wait status: the
    // exit code is the provider's, not read back from the exit record in the data dir.
    assert_eq!(s(&recovery["attributes"]["exit_proof"]), "provider", "{recovery}");
    assert_eq!(s(&recovery["attributes"]["build_id"]), birth_build, "the recovery is an attempt of the accepted Build: {recovery}");
    assert_eq!(s(&recovery["attributes"]["disposition"]), "created", "op 0x20 opened it, spelled as the in-process door spells it: {recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    // The submit followed the recovery to its terminal frame and records it (never "submitted").
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "the recovery's terminal: {recovery}");
    let build = estate.await_build(&birth_build, SETTLE).await;
    assert_eq!(build["build_id"], birth_build.as_str(), "no Build was minted: {build}");
    // The fabric-primary heard the recovery's receipts on the Build topic (its recovery dedup reads
    // them): NodeDeleted naming the exited birth, and the operation's Complete.
    let heard = fp_recovery_receipts(&estate, &s(&primary["admin_api_base"]), &birth_build, node_path, &attempt).await;
    assert_eq!(heard.0["output"]["node_id"], before["node_id"], "the fabric-primary's NodeDeleted names the exited node: {:?}", heard.0);
    assert_eq!(heard.0["output"]["incarnation"], before["incarnation_id"], "and its exact birth: {:?}", heard.0);
    assert!(heard.1["output"].is_null(), "Complete commits no output: {:?}", heard.1);

    // Each process exports its own spans: the new node's boot span is read once the new node exported it.
    let spans = wait_for("the recovery operation's last step and the new node's boot span are exported", SETTLE, || async {
        let spans = estate.spans();
        (step_span(&spans, &birth_build, &attempt, node_path, "Complete").is_some() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // Delete, then create: the exited birth departs once, named by its exact birth, before the new node boots.
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact exited birth");
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).expect("the new node's boot span");
    assert!(start(deleted[0]) < start(boot), "the exited birth is deleted (fenced) before the new node joins");
    for step in ["DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft", "RenamePredecessor"] {
        assert!(step_span(&spans, &birth_build, &attempt, node_path, step).is_none(), "no RPC to the exited process and no .old rename: {step} ran");
    }
    assert!(!named(&spans, "rdm.mesh.node.delete.via-transport-stopped").is_empty(), "the exiting runtime's own span names the reason");
    // The fabric-primary's drift check opens nothing for a member: its mesh-primary recovers it.
    let drift: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| s(&sp["attributes"]["scope"]).contains(&format!("exited: {node_path}"))).collect();
    assert!(drift.is_empty(), "the fabric-primary opened a drift attempt for a member: {drift:?}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));

    // Nothing else moved.
    for (name, id) in others {
        assert_eq!(s(&estate.node(&name).await["node_id"]), id, "{name} is untouched");
    }
    estate.settled_shape(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md): every other unplanned exit is a recovery like the transport stop.
/// A rpc node killed with SIGKILL leaves no recorded reason: its mesh-primary, which in a one-mesh
/// fabric is also the fabric-primary (the in-process arm: its own Build door, no op 0x20 call),
/// marks it TRUE OFFLINE, proves the exit from the provider alone (no exit code) and submits one
/// recovery keyed on the exited birth; the recovery is the next attempt of the accepted Build (no
/// Build minted), one `recover-node:<path>` operation run by that mesh-primary: node.delete of the
/// exited instance (no RPC to it, no stop frames), node.deleted once named by its exact birth and
/// before the new node boots, then node.create at the same path.name over the inherited data dir.
/// The path comes back with the SAME transport key and data dir and a NEW node.id and incarnation;
/// no `<path>.old` rename, no storage hand-over, and the fabric-primary's drift opens nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn any_other_unplanned_exit_is_recovered_as_a_new_node_over_the_same_data_dir() {
    let (estate, birth_build) = estate("any_other_unplanned_exit_is_recovered_as_a_new_node_over_the_same_data_dir").await;
    const NODE: &str = "mesh1.rpc.3";
    let nodes = estate.nodes().await;
    let primary = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    let mesh_primary = s(&nodes.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("the mesh-primary")["name"]);
    assert_eq!(mesh_primary, primary, "one mesh: its primary holds the fabric-primary seat too");
    let before = estate.node(NODE).await;
    let data_dir = estate.data_dir_of(NODE).await;
    let others: Vec<(String, String)> = nodes.iter().filter(|n| n["name"] != NODE).map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
    estate.kill_node(NODE).await;

    let after = wait_for(&format!("{NODE} ready as a new node"), SETTLE, || async {
        let n = estate.node_opt(NODE).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    assert_ne!(after["incarnation_id"], before["incarnation_id"], "a new incarnation");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same transport key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir_of(NODE).await, data_dir, "the same data dir");

    let recovery = recovery_span(&estate, NODE, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "{recovery}");
    assert_eq!(s(&recovery["attributes"]["fabric_primary"]), primary, "submitted to its own seat: {recovery}");
    assert_eq!(s(&recovery["attributes"]["disposition"]), "created", "the in-process door opened it: {recovery}");
    assert_eq!(recovery["attributes"]["incarnation_id"], before["incarnation_id"], "keyed on the exact exited birth: {recovery}");
    assert_eq!(s(&recovery["attributes"]["exit_code"]), "", "a killed runtime proves no reason: {recovery}");
    assert_eq!(s(&recovery["attributes"]["exit_proof"]), "none", "{recovery}");
    assert_eq!(s(&recovery["attributes"]["build_id"]), birth_build, "the recovery is an attempt of the accepted Build: {recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "the recovery's terminal: {recovery}");

    // Each process exports its own spans: the successor's boot span is read once the successor exported it.
    let spans = wait_for("the recovery operation's last step, its reconcile span and the successor's boot span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        (step_span(&spans, &birth_build, &attempt, NODE, "Complete").is_some() && reconcile_span(&spans, &birth_build, &attempt).is_some() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    let reconcile = reconcile_span(&spans, &birth_build, &attempt).expect("the attempt's reconcile span");
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("recover-node:{NODE}"), "one recovery operation: {reconcile}");
    let mut last = 0u64;
    for step in ["NodeDeleting", "NodeDeleted", "DeployRuntime", "WaitForNodeReady", "Complete"] {
        let sp = step_span(&spans, &birth_build, &attempt, NODE, step).unwrap_or_else(|| panic!("the recovery's {step} step span"));
        assert!(start(sp) >= last, "{step} runs after the step before it");
        last = start(sp);
    }
    for step in ["DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft", "RenamePredecessor", "HandoffStorage"] {
        assert!(step_span(&spans, &birth_build, &attempt, NODE, step).is_none(), "no RPC to the exited process, no .old rename, no storage hand-over: {step} ran");
    }
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact exited birth");
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).expect("the new node's boot span");
    assert!(start(deleted[0]) < start(boot), "the exited birth is deleted (fenced) before the new node joins");
    assert!(
        named(&spans, "rdm.node_admin.node.create.via-build").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE),
        "the recovery's node operation span is node.create.via-build"
    );
    let drift: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| s(&sp["attributes"]["scope"]).contains(&format!("exited: {NODE}"))).collect();
    assert!(drift.is_empty(), "the fabric-primary opened a drift attempt for a member: {drift:?}");
    assert!(nodes_named_old(&estate.nodes().await).is_empty(), "no <path>.old identity");
    let (_, build) = estate.get(&format!("/api/builds?id={birth_build}")).await;
    assert_eq!(build["build_id"], birth_build.as_str(), "no Build was minted: {build}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    for (name, id) in others {
        assert_eq!(s(&estate.node(&name).await["node_id"]), id, "{name} is untouched");
    }
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// Whether the boot span of `node`'s birth is exported (its own process exports it, on its own cadence).
fn booted(spans: &[Value], node: &Value) -> bool {
    named(spans, "rdm.mesh.node.create.via-deployment").into_iter().any(|sp| sp["attributes"]["node_id"] == node["node_id"])
}

/// The nodes whose path.name is a retired `<path>.old`.
fn nodes_named_old(nodes: &[Value]) -> Vec<String> {
    nodes.iter().map(|n| s(&n["name"])).filter(|n| n.ends_with(".old")).collect()
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

/// The steps of one `replace-node:<path>` operation of a live birth, in the order node-replace.md
/// numbers them: the old birth drains (drain-node, node-drained) and stops (stop-node, node-left, its
/// NodeStopped) and is parked, is renamed `<path>.old`, the successor's identity is allocated, the
/// parked birth is ended with exact terminal proof and its storage handed over, the successor starts,
/// is admitted and ready, and only then the old identity is deleted.
const REPLACE_ORDER: [&str; 15] = [
    "NodeDeleting",
    "DrainNode",
    "AwaitNodeDrained",
    "StopNode",
    "AwaitNodeLeft",
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

/// The replace of `node` in `attempt` of `build` ran as ONE operation `replace-node:<node>`, its steps in
/// [`REPLACE_ORDER`], the successor admitted and ready before the old identity's `NodeDeleted`, and the
/// predecessor renamed `<node>.old` before the successor's identity was allocated.
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
/// operation `replace-node:<path>`; the old birth drains and stops, its NodeStopped drops it from
/// every admin's book, and it is renamed `<path>.old` while parked; the successor's identity is
/// allocated; the parked birth is proven terminal; no admin's view holds two live births at the path; the successor starts, joins
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
    // from this poll: the window is the drain-to-NodeDeleted span of the replace. No admin's view
    // holds two live births at the path at any step (node-replace.md: the predecessor is parked and
    // renamed before node.create claims the freed name).
    let admins: Vec<String> = estate.nodes().await.iter().filter(|n| n["kind"] == "node_admin").map(|n| s(&n["admin_api_base"])).collect();
    assert_eq!(admins.len(), 2, "both admins serve a view");
    let old_name = format!("{NODE}.old");
    let after = wait_for(&format!("{NODE} ready under a new NodeId"), SETTLE, || async {
        at_most_one_live_birth(&admins, NODE).await;
        let nodes = estate.nodes().await;
        assert!(nodes.iter().filter(|n| n["name"] == NODE).count() <= 1, "two births at {NODE}: {nodes:#?}");
        nodes.into_iter().find(|n| n["name"] == NODE && ready(n) && n["node_id"] != before["node_id"])
    })
    .await;

    estate.await_attempt(&birth_build, attempt_no, SETTLE).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new node has a new transport key");
    let attempt = attempt_no.to_string();
    let spans = wait_for("the replace operation's last step, its reconcile span and the successor's boot span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        (step_span(&spans, &birth_build, &attempt, NODE, "Complete").is_some() && reconcile_span(&spans, &birth_build, &attempt).is_some() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    assert_replace_followed_the_spec(&spans, &birth_build, &attempt, NODE, &before, &after, &REPLACE_ORDER);
    assert_parked_before_the_successor(&spans, &birth_build, &attempt, NODE, &before, admins.len());
    let held_old = named(&spans, "rdm.node_admin.topology.update.via-predecessor-held")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == old_name.as_str() && sp["attributes"]["node_id"] == before["node_id"])
        .unwrap_or_else(|| panic!("the executor's view never named the predecessor {old_name}"));
    let deleted_step = step_span(&spans, &birth_build, &attempt, NODE, "NodeDeleted").expect("NodeDeleted ran");
    assert!(start(held_old) < start(deleted_step), "the view named the predecessor {old_name} before its NodeDeleted: {held_old}");
    // The node's row (ops-naming "Node data": "a Deleted row, or the <name>.old row"): at the
    // executor's RenamePredecessor the fabric-primary moves the predecessor's row to `<path>.old`,
    // its NodeDeleted writes the Deleted row there, and the successor's row at the path.name is
    // claimed seeded from the .old row.
    let moved: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-rename").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(moved.len(), 1, "one move of the predecessor's row: {moved:?}");
    assert_eq!((s(&moved[0]["attributes"]["from"]), s(&moved[0]["attributes"]["to"]), s(&moved[0]["attributes"]["outcome"])), (NODE.to_string(), old_name.clone(), "renamed".to_string()), "{}", moved[0]);
    let old_deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == old_name.as_str()).collect();
    assert_eq!(old_deleted.len(), 1, "one Deleted row at {old_name}: {old_deleted:?}");
    assert_eq!((s(&old_deleted[0]["attributes"]["outcome"]), s(&old_deleted[0]["attributes"]["node_id"])), ("row-deleted".to_string(), s(&before["node_id"])), "{}", old_deleted[0]);
    let seeded: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == NODE && sp["attributes"]["node_id"] == after["node_id"]).collect();
    assert_eq!(seeded.len(), 1, "the successor claims its row once: {seeded:?}");
    assert_eq!((s(&seeded[0]["attributes"]["outcome"]), s(&seeded[0]["attributes"]["seeded_from"])), ("row-seeded".to_string(), s(&before["node_id"])), "{}", seeded[0]);
    assert!(start(moved[0]) < start(seeded[0]), "the predecessor's row moved before the successor claimed the path.name");
    estate.artifact("node-rows.json", &json!({"moved": moved[0], "deleted": old_deleted[0], "seeded": seeded[0]}));
    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    let settled = estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    assert_eq!(settled.iter().filter(|n| n["name"] == NODE).count(), 1, "exactly one birth at {NODE}");
    assert!(!settled.iter().any(|n| n["name"] == old_name.as_str()), "the departed predecessor is gone from the view");
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

/// CONTRACT (node-recovery.md "Detection and proof" step 3: "Still running is no proof, however
/// long the silence: the node is hung or partitioned, not gone, and nothing is recovered"): a member
/// of the mesh that does not hold the fabric-primary is held (SIGSTOP of its exact runtime). Its
/// mesh-primary sees it silent, its tickle finds no path twice (TRUE OFFLINE), and it asks the
/// provider about the exact runtime: still running, so the inspection is spanned once with verdict
/// not-exited and no recovery is submitted (no via-node-recovery for that birth, no attempt opened).
/// Released, the SAME birth (node.id and incarnation) is ready again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_member_whose_runtime_still_runs_is_never_recovered() {
    let (mut estate, birth_build) = two_mesh_estate("a_hung_member_whose_runtime_still_runs_is_never_recovered").await;
    let (node, mesh_primary, primary) = placement(&estate.nodes().await);
    let fp_api = s(&primary["admin_api_base"]);
    let before = estate.node(&node).await;
    let attempt_before = attempt_at(&estate, &fp_api, &birth_build).await;
    let rt = exact_runtime(&estate, &node).await;
    let held = rt.apply(Fault::Stop).expect("the exact runtime is held");
    assert_eq!(held.state_after, Some('T'), "the OS holds the runtime: {held:?}");

    // The ladder: silent at the floor, one tickle half a floor later, TRUE OFFLINE at least one floor
    // after the first no-path, then the inspection on that tick.
    let floor = rafka_mesh_transport::membership::staleness_floor();
    let inspected = wait_for(&format!("{node}'s mesh-primary inspects its exact runtime"), floor * 3 + Duration::from_secs(30), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned()
    })
    .await;
    assert_eq!(s(&inspected["attributes"]["verdict"]), "not-exited", "a held runtime is no exit proof: {inspected}");
    assert!(s(&inspected["attributes"]["reason"]).contains("Running"), "the provider says the exact runtime runs: {inspected}");
    assert_eq!(s(&inspected["attributes"]["inspector"]), mesh_primary, "the member's own mesh-primary inspects: {inspected}");
    assert_eq!(inspected["attributes"]["incarnation_id"], before["incarnation_id"], "the exact held birth: {inspected}");
    let spans = estate.spans();
    let true_offline = named(&spans, "rdm.node_admin.node.resolve.via-offline-tickle-failed").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned();
    let true_offline = true_offline.unwrap_or_else(|| panic!("the inspection follows the TRUE OFFLINE mark of {node}"));
    assert!(start(&true_offline) <= start(&inspected), "TRUE OFFLINE, then the inspection");
    assert_eq!(attempt_at(&estate, &fp_api, &birth_build).await, attempt_before, "no attempt is opened for a hung member");
    assert_eq!(estate.node(&node).await["node_id"], before["node_id"], "the held member is not replaced");

    let thawed = rt.apply(Fault::Continue).expect("the hold is released");
    assert_ne!(thawed.state_after, Some('T'), "{thawed:?}");
    wait_for(&format!("{node} back as its own birth"), floor * 2 + Duration::from_secs(30), || async {
        estate.node_opt(&node).await.filter(|n| ready(n) && n["node_id"] == before["node_id"] && n["incarnation_id"] == before["incarnation_id"])
    })
    .await;
    assert_eq!(attempt_at(&estate, &fp_api, &birth_build).await, attempt_before, "the Build gained no attempt");
    assert_eq!(exact_runtime(&estate, &node).await, rt, "the runtime that was held serves the path");
    estate.record_trace_url(inspected["trace_id"].as_str().unwrap_or(""));
    estate.settled_shape(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.stop().await;

    // Every process flushed its spans on exit: the whole record.
    let spans = estate.spans();
    let inspections: Vec<&Value> = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(inspections.len(), 1, "the verdict is named once while it holds: {inspections:?}");
    let submits: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert!(submits.is_empty(), "no recovery is submitted for a running runtime: {submits:?}");
}

/// CONTRACT (node-recovery.md sequence step 1: "A repeat submit answers AlreadyApplied or
/// BuildInProgress, never a second recovery"; step 3: "the old (node.id, incarnation) fenced in the
/// peer-location book"): a member of the mesh that does not hold the fabric-primary is killed and
/// recovered by its mesh-primary through the fabric-primary. Once the fabric-primary holds the
/// recovery's receipts, the same recovery submitted again on op 0x20 (`build.create` Recover keyed on
/// the exited birth) answers Started AlreadyApplied naming the recovery's attempt, then its Complete
/// terminal: nothing runs, the Build gains no attempt, the path keeps the recovered node, and exactly
/// one recovery was ever opened for that birth. From the fabric-primary's mesh, a call aimed at the
/// exited birth's node.id is not sent (`Resolve(Unknown)`): it never reaches the new process that
/// holds the same endpoint key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeat_recovery_submit_for_the_same_exited_birth_is_answered_already_applied() {
    use rafka_node_admin_client::BuildFrame;
    use rafka_node_rpc_contract::build::{BuildChange, Disposition};
    let (mut estate, birth_build) = two_mesh_estate("a_repeat_recovery_submit_for_the_same_exited_birth_is_answered_already_applied").await;
    let (node, mesh_primary, primary) = placement(&estate.nodes().await);
    let fp_api = s(&primary["admin_api_base"]);
    let before = estate.node(&node).await;
    let killed = exact_runtime(&estate, &node).await.apply(Fault::Kill).expect("the exact runtime is killed");
    assert!(killed.exited, "the OS observed the exit: {killed:?}");

    let after = wait_for(&format!("{node} ready as a new node"), SETTLE, || async {
        let n = estate.node_opt(&node).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    let recovery = recovery_span(&estate, &node, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "{recovery}");
    assert_eq!(s(&recovery["attributes"]["disposition"]), "created", "{recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "{recovery}");
    // A resubmit before the fabric-primary heard the receipts would race the Build topic.
    fp_recovery_receipts(&estate, &fp_api, &birth_build, &node, &attempt).await;

    let caller = crate::fabric_build__build_op::Caller::of(&estate.admin).await;
    let change = BuildChange::Recover {
        node: node.parse().expect("a path.name"),
        node_id: serde_json::from_value(before["node_id"].clone()).expect("a node id"),
        incarnation: serde_json::from_value(before["incarnation_id"].clone()).expect("an incarnation"),
    };
    let mut repeat = caller.carrier().await.create(change).await.expect("the fabric-primary answers the repeat");
    assert_eq!(repeat.disposition(), Disposition::AlreadyApplied, "the repeat is answered, never run");
    assert_eq!(repeat.build_id().0, birth_build, "the accepted Build");
    assert_eq!(repeat.attempt().to_string(), attempt, "naming the attempt that recovered the birth");
    match repeat.next().await {
        Some(Ok(BuildFrame::Complete { attempt: a })) => assert_eq!(a.to_string(), attempt, "its terminal names the same attempt"),
        other => panic!("AlreadyApplied is followed by its Complete terminal: {other:?}"),
    }
    assert!(repeat.next().await.is_none(), "nothing after the terminal");
    assert_eq!(attempt_at(&estate, &fp_api, &birth_build).await.to_string(), attempt, "the Build gained no attempt");
    assert_eq!(estate.node(&node).await["node_id"], after["node_id"], "the path keeps the recovered node");

    // The fabric-primary's mesh never dials the exited birth's node.id, though its key now serves the new node.
    let old = estate.probe(&["get", "--target", &format!("exact:{}", s(&before["node_id"])), "--key", "83"]);
    assert_eq!((&old["outcome"], &old["reason"], &old["route"]), (&json!("NotSent"), &json!("Resolve(Unknown)"), &json!("direct")), "exact:<old> resolves nothing: {old}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settled_shape(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.stop().await;

    // Every process flushed its spans on exit: the whole record.
    let spans = estate.spans();
    let opened: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"] && sp["attributes"]["outcome"] == "admitted").collect();
    assert_eq!(opened.len(), 1, "exactly one recovery is opened for the exited birth: {opened:?}");
    let applied: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-recovery-already-applied").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(applied.len(), 1, "the repeat is answered from the recovery's receipts: {applied:?}");
    assert_eq!(s(&applied[0]["attributes"]["attempt"]), attempt, "{applied:?}");
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "the exited birth is deleted once: {deleted:?}");
}

/// CONTRACT (node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that
/// workflow: its Build ends Failed, and the mesh-primary's next tick finds the member still proven
/// exited and submits the recovery"): a member of the mesh that does not hold the fabric-primary is
/// held (SIGSTOP) and a drain of it is requested at once, attempt N of the accepted Build
/// (`drain-node:<path>`, run by its mesh-primary). While that drain waits on the held process, the
/// process is killed. The mesh-primary holds a connection to the member from the Build's own
/// commit-state and open-traffic rounds (it commanded the member on each; `await_build` returns
/// after them, and the connection idles out only after a staleness floor), so the drain-node
/// command is written and finished on that connection to the held process, and its reply is lost
/// when the process is killed: the admission is Indeterminate (ReplyLost), never Applied, so no
/// node-drained can follow: attempt N ends Failed at AwaitNodeDrained, named by that admission
/// (node-drain.md "Admission is not drain completion"), never Complete. Then the mesh-primary proves the
/// exit and submits the recovery, a later attempt of the same Build, which completes: a new node.id
/// at the path over the same endpoint key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exit_during_a_workflow_fails_that_workflow_and_the_next_tick_recovers_the_member() {
    let (estate, birth_build) = two_mesh_estate("an_exit_during_a_workflow_fails_that_workflow_and_the_next_tick_recovers_the_member").await;
    let (node, mesh_primary, primary) = placement(&estate.nodes().await);
    let fp_api = s(&primary["admin_api_base"]);
    let before = estate.node(&node).await;
    let rt = exact_runtime(&estate, &node).await;
    assert_eq!(rt.apply(Fault::Stop).expect("the exact runtime is held").state_after, Some('T'));
    let (status, r) = estate.post(&format!("/api/nodes/{node}/drain"), &json!({})).await;
    assert_eq!(status, 202, "the view still holds the member ready: {r}");
    assert_eq!(s(&r["build_id"]), birth_build, "{r}");
    let drain_attempt = Estate::attempt_of(&r);
    let killed = rt.apply(Fault::Kill).expect("the held runtime is killed");
    assert!(killed.exited, "{killed:?}");

    let operation = format!("drain-node:{node}");
    let failed = wait_for("the drain attempt ends Failed", SETTLE, || async {
        let (_, b) = estate.http_get(&fp_api, &format!("/api/builds?id={birth_build}")).await;
        b["steps"].as_array().cloned().unwrap_or_default().into_iter().find(|r| r["operation"] == operation.as_str() && r["attempt"] == json!(drain_attempt) && r["outcome"]["failed"].is_object())
    })
    .await;
    // The drain-node command was written to the held birth on the connection the Build's rounds
    // opened, and its reply was lost at the kill: never admitted, so no node-drained can follow.
    assert_eq!(s(&failed["step"]), "AwaitNodeDrained", "the drain is not complete: {failed}");
    let why = s(&failed["outcome"]["failed"]["reason"]);
    assert!(why.contains("drain-node was not admitted (Indeterminate") && why.contains("ReplyLost("), "the admission names why: {failed}");

    let after = wait_for(&format!("{node} ready as a new node"), SETTLE, || async {
        let n = estate.node_opt(&node).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, from the inherited data dir");
    let recovery = wait_for("the mesh-primary's completed recovery is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"] && s(&sp["attributes"]["outcome"]).starts_with("complete")).cloned()
    })
    .await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "{recovery}");
    assert_eq!(s(&recovery["attributes"]["build_id"]), birth_build, "{recovery}");
    let attempt: u64 = s(&recovery["attributes"]["attempt"]).parse().expect("an attempt");
    assert!(attempt > drain_attempt, "the recovery is a later attempt than the failed drain: {recovery}");
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "{recovery}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settled_shape(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// The `observer`'s first offline-tickle span of `round` for `node_id` (round 1 opens the hold-down,
/// round 2 is TRUE OFFLINE), once exported.
fn tickle_span<'a>(spans: &'a [Value], observer: &str, node_id: &Value, round: u8) -> Option<&'a Value> {
    let name = if round == 1 { "rdm.node_admin.node.update.via-offline-hold-down-opened" } else { "rdm.node_admin.node.resolve.via-offline-tickle-failed" };
    let mut found: Vec<&Value> = named(spans, name).into_iter().filter(|sp| sp["attributes"]["observer"] == observer && sp["attributes"]["node_id"] == *node_id).collect();
    found.sort_by_key(|sp| start(sp));
    found.into_iter().next()
}

/// A span attribute that is a number, exported as a string or a number.
fn num_attr(sp: &Value, k: &str) -> u64 {
    let v = &sp["attributes"][k];
    v.as_u64().or_else(|| v.as_i64().map(|i| i as u64)).or_else(|| v.as_str().and_then(|s| s.parse().ok())).unwrap_or_else(|| panic!("{k} is not a number: {sp}"))
}

/// CONTRACT (#3051 acceptance 2, node-recovery.md "Detection and proof" step 1: "Every node-admin
/// keeps the silence bookkeeping (`first_seen`), so a node-admin that takes the seat mid-window
/// continues the window already running"): a member of mesh2 (two node-admins, its mesh-primary not
/// the fabric-primary) is killed; once mesh2's primary has run tickle round 1 for it, that primary is
/// stopped, which hands mesh2's seat to its second admin. The new mesh-primary tickles on the window
/// it has kept since before it took the seat (its round 1 names a `first_seen_ms` earlier than the
/// seat change by a probe delay or more, never a window opened at the seat), reaches TRUE OFFLINE,
/// asks the provider, and its submit follows the one recovery to `complete`: a new node.id at the
/// path over the same endpoint key. Must NOT happen: the window restarted at the seat change, the
/// recovery submitted by the stopped primary, or a proven-drift attempt for the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_handover_mid_ladder_continues_the_silence_window_and_completes_the_recovery() {
    let estate = Estate::bootstrap(owner("a_mesh_primary_handover_mid_ladder_continues_the_silence_window_and_completes_the_recovery"), "fabric1", "mesh1").await;
    let (status, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 1}, {"name": "mesh2", "node_admin": 2, "rpc_node": 2}]}))
        .await;
    assert_eq!(status, 202, "{a}");
    let birth_build = s(&a["build_id"]);
    estate.await_build(&birth_build, SETTLE).await;
    estate.settled_shape(&[("mesh1", 1, 1), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let floor = rafka_mesh_transport::membership::staleness_floor();
    let nodes = estate.nodes().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    assert!(fp.starts_with("mesh1."), "the day-0 admin keeps the fabric seat: {fp}");
    let mp = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2's primary")["name"]);
    let standby = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != mp.as_str()).expect("mesh2's second admin")["name"]);
    const NODE: &str = "mesh2.rpc.1";
    let before = estate.node(NODE).await;
    estate.kill_node(NODE).await;

    // Round 1 on the mesh-primary that holds the seat now. Its span ends the round, due by
    // node-recovery.md steps 1-2: the silence (one floor, marked on the pruner's gossip-interval
    // sweep), the tick (one gossip interval) half a floor after the mark, then one direct connect and
    // up to VIA_PEER_TICKLE_FANOUT connects through peers, one after another, each to a killed
    // process and so spending its whole dial: the connection's idle timeout, the staleness floor
    // (R-T8), inside the call's 10 s budget. Then the export's 200 ms schedule.
    let gossip = rafka_mesh_entity::cadence::gossip_interval();
    let connect = floor.min(Duration::from_secs(10));
    let round1_due = floor + gossip + gossip + floor / 2 + connect * (1 + rafka_node_admin_core::offline::VIA_PEER_TICKLE_FANOUT as u32) + Duration::from_millis(200);
    let round1 = wait_for(&format!("{mp} runs tickle round 1 for {NODE}"), round1_due, || async { tickle_span(&estate.spans(), &mp, &before["node_id"], 1).cloned() }).await;
    // Mid-ladder: the mesh-primary is stopped, and mesh2's seat goes to its second admin.
    let (status, stop) = estate.post(&format!("/api/nodes/{mp}/stop"), &json!({})).await;
    assert_eq!(status, 202, "the stop of {mp} is accepted: {stop}");
    let seat = wait_for(&format!("{standby} takes mesh2's seat"), floor * 2, || async {
        named(&estate.spans(), "rdm.mesh.election.resolve.via-mesh-primary")
            .into_iter()
            .find(|sp| sp["attributes"]["observer"] == standby.as_str() && sp["attributes"]["winner_path"] == standby.as_str() && start(sp) > start(&round1))
            .cloned()
    })
    .await;

    // The new mesh-primary's submit follows the one recovery to its end.
    let recovery = wait_for(&format!("{standby} recovers {NODE}"), SETTLE, || async {
        named(&estate.spans(), "rdm.node_admin.build.update.via-node-recovery")
            .into_iter()
            .find(|sp| sp["attributes"]["node_id"] == before["node_id"] && s(&sp["attributes"]["outcome"]).starts_with("complete: attempt"))
            .cloned()
    })
    .await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), standby, "the new mesh-primary submitted the recovery: {recovery}");
    assert_eq!(s(&recovery["attributes"]["fabric_primary"]), fp, "{recovery}");
    let after = wait_for(&format!("{NODE} ready as a new node"), SETTLE, || async {
        let n = estate.node_opt(NODE).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same transport key, loaded from the inherited data dir");

    let spans = estate.spans();
    let taken_ns = start(&seat);
    let new_round1 = tickle_span(&spans, &standby, &before["node_id"], 1).unwrap_or_else(|| panic!("{standby}'s tickle round 1 for {NODE}"));
    let new_round2 = tickle_span(&spans, &standby, &before["node_id"], 2).unwrap_or_else(|| panic!("{standby}'s TRUE OFFLINE for {NODE}"));
    assert!(start(new_round1) > taken_ns, "{standby} tickles only once it holds the seat");
    // The window it kept: the round runs on the instant it first saw the member silent, before the
    // seat changed hands and a probe delay or more before it, never on a window opened at the seat.
    // (The round's span is written when the round ends: the failed direct ping and via-peer kicks
    // take their call deadlines, so its time after the seat is not the wait before it.)
    let first_seen_ms = num_attr(new_round1, "first_seen_ms");
    assert!(first_seen_ms * 1_000_000 + (floor / 2).as_nanos() as u64 <= taken_ns, "{standby}'s window began a probe delay or more before it took the seat: first_seen {first_seen_ms} ms, seat at {} ms", taken_ns / 1_000_000);
    let waited = Duration::from_nanos(start(new_round1) - taken_ns);
    assert_eq!(num_attr(new_round2, "first_seen_ms"), first_seen_ms, "TRUE OFFLINE on the same window");
    assert!(start(new_round2) < start(&recovery), "TRUE OFFLINE before the submit");
    let inspection = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == before["node_id"] && sp["attributes"]["inspector"] == standby.as_str())
        .unwrap_or_else(|| panic!("{standby}'s provider inspection of {NODE}"));
    assert_eq!(s(&inspection["attributes"]["verdict"]), "exited", "{inspection}");
    assert!(tickle_span(&spans, &mp, &before["node_id"], 2).is_none() || !named(&spans, "rdm.node_admin.build.update.via-node-recovery").iter().any(|sp| sp["attributes"]["submitter"] == mp.as_str() && s(&sp["attributes"]["outcome"]).starts_with("complete")), "the stopped primary recovered nothing");
    let drift: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| s(&sp["attributes"]["scope"]).contains(NODE)).collect();
    assert!(drift.is_empty(), "the fabric-primary opened a drift attempt for a member: {drift:?}");
    eprintln!(
        "acceptance 2: old mp {mp} round1 trace {} | seat {standby} at {} ms | new round1 {} (first_seen {first_seen_ms} ms, {waited:?} after the seat) | round2 {} | recovery trace {} ({})",
        round1["trace_id"].as_str().unwrap_or(""),
        taken_ns / 1_000_000,
        new_round1["trace_id"].as_str().unwrap_or(""),
        new_round2["trace_id"].as_str().unwrap_or(""),
        recovery["trace_id"].as_str().unwrap_or(""),
        s(&recovery["attributes"]["outcome"]),
    );
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}
