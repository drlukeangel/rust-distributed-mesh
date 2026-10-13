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
    // The initial fabric refuses a node replace until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(SETTLE).await;
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
    // The cells hold, kill and replace members: workflows the initial fabric refuses until it has first
    // reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(SETTLE).await;
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
/// topic: its node.delete's `retire-node:<node>` NodeDeleted receipt, and the Complete of its last
/// operation, node.update (`update-node:<node>`).
async fn fp_recovery_receipts(estate: &Estate, fp_api: &str, build: &str, node: &str, attempt: &str) -> (Value, Value) {
    let (delete, update) = (format!("retire-node:{node}"), format!("update-node:{node}"));
    wait_for("the fabric-primary holds the recovery's receipts", Duration::from_secs(30), || async {
        let (_, b) = estate.http_get(fp_api, &format!("/api/builds?id={build}")).await;
        let steps = b["steps"].as_array().cloned().unwrap_or_default();
        let at = |operation: &str, step: &str| steps.iter().find(|r| r["operation"] == operation && r["step"] == step && r["attempt"].to_string() == attempt && r["outcome"] == "complete").cloned();
        Some((at(&delete, "NodeDeleted")?, at(&update, "Complete")?))
    })
    .await
}

/// CONTRACT: a rpc node whose mesh transport stops for good exits (reserved code 4, recorded in
/// its data dir). Its mesh-primary (node-recovery.md), not the fabric-primary, proves the exit:
/// it marks the node TRUE OFFLINE, asks the provider about the exact runtime, and on Exited
/// submits one recovery to the fabric-primary keyed on the exited birth; the recovery is the next
/// attempt of the accepted Build (no Build minted), run by that mesh-primary: node.delete of the
/// exited instance (node.deleted from the exit proof, once, named by the exact birth), then
/// node.create at the same path.name over the inherited data dir, then node.update. The path comes
/// back with the SAME transport key and data dir and a NEW node.id and incarnation. No other node
/// changes, and the fabric-primary's drift check opens nothing for it.
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
    // them): the delete's NodeDeleted naming the exited birth, and the update's Complete.
    let heard = fp_recovery_receipts(&estate, &s(&primary["admin_api_base"]), &birth_build, node_path, &attempt).await;
    assert_eq!(heard.0["output"]["node_id"], before["node_id"], "the fabric-primary's NodeDeleted names the exited node: {:?}", heard.0);
    assert_eq!(heard.0["output"]["incarnation"], before["incarnation_id"], "and its exact birth: {:?}", heard.0);
    assert!(heard.1["output"].is_null(), "Complete commits no output: {:?}", heard.1);

    // Each process exports its own spans: the new node's boot span is read once the new node exported it.
    let spans = wait_for("the recovery's last operation, the exited birth's fence and the new node's boot span are exported", SETTLE, || async {
        let spans = estate.spans();
        (step_span(&spans, &birth_build, &attempt, node_path, "UpdateNode").is_some() && !exit_fences(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // Delete, then create: the exited birth departs once, named by its exact birth, before the new node boots.
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact exited birth");
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).expect("the new node's boot span");
    assert!(start(deleted[0]) < start(boot), "the exited birth is deleted (fenced) before the new node joins");
    // node-recovery.md "Delete fences the old instance before the new join": the fence is the
    // create's, run by the executor that launches the new node with the retained key, inside the
    // create's own operation and before that launch. A fence run after the delete's Complete is left
    // undone when that executor is lost before it, and the next attempt's create launches unfenced.
    let create = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == node_path)
        .expect("the recovery's create operation span");
    let fences = exit_fences(&spans, &before);
    assert_eq!(fences.len(), 1, "the exited birth is fenced once: {fences:?}");
    assert!(start(create) <= start(fences[0]) && end(fences[0]) <= end(create), "the fence runs inside the create operation: {} vs {create}", fences[0]);
    assert!(end(fences[0]) <= start(boot), "the fence is done before the new node with the same key boots: {} vs {boot}", fences[0]);
    for step in ["StopNode", "AwaitNodeLeft", "RenamePredecessor"] {
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
/// Build minted), three operations run by that mesh-primary in order (node-recovery.md "Steps"):
/// node.delete of the exited instance (`retire-node:<path>`: no RPC to it, no stop frames,
/// node.deleted once named by its exact birth and before the new node boots), node.create at the
/// same path.name over the inherited data dir (`create-node:<path>`), node.update
/// (`update-node:<path>`), each as its own workflow's pipeline and node operation span.
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
    let spans = wait_for("the recovery's last operation, its reconcile span, the exited birth's fence and the successor's boot span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        (step_span(&spans, &birth_build, &attempt, NODE, "UpdateNode").is_some() && reconcile_span(&spans, &birth_build, &attempt).is_some() && !exit_fences(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    let reconcile = reconcile_span(&spans, &birth_build, &attempt).expect("the attempt's reconcile span");
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("retire-node:{NODE},create-node:{NODE},update-node:{NODE}"), "the recovery's three operations, in order: {reconcile}");
    let mut last = 0u64;
    for step in ["NodeDeleting", "NodeDeleted", "RemoveTopologyMembership", "DeployRuntime", "WaitForNodeReady", "UpdateNode"] {
        let sp = step_span(&spans, &birth_build, &attempt, NODE, step).unwrap_or_else(|| panic!("the recovery's {step} step span"));
        assert!(start(sp) >= last, "{step} runs after the step before it");
        last = start(sp);
    }
    for step in ["StopNode", "AwaitNodeLeft", "RenamePredecessor", "HandoffStorage"] {
        assert!(step_span(&spans, &birth_build, &attempt, NODE, step).is_none(), "no RPC to the exited process, no .old rename, no storage hand-over: {step} ran");
    }
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact exited birth");
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).expect("the new node's boot span");
    assert!(start(deleted[0]) < start(boot), "the exited birth is deleted (fenced) before the new node joins");
    // node-recovery.md "Delete fences the old instance before the new join": the fence is the
    // create's, run by the executor that launches the new node with the retained key, inside the
    // create's own operation and before that launch. A fence run after the delete's Complete is left
    // undone when that executor is lost before it, and the next attempt's create launches unfenced.
    let create = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE)
        .expect("the recovery's create operation span");
    let fences = exit_fences(&spans, &before);
    assert_eq!(fences.len(), 1, "the exited birth is fenced once: {fences:?}");
    assert!(start(create) <= start(fences[0]) && end(fences[0]) <= end(create), "the fence runs inside the create operation: {} vs {create}", fences[0]);
    assert!(end(fences[0]) <= start(boot), "the fence is done before the new node with the same key boots: {} vs {boot}", fences[0]);
    // Each operation is its workflow: its node operation span and its own pipeline, never a recovery
    // pipeline of its own.
    for (op, pipeline) in [("delete", "retire"), ("create", "create"), ("update", "update")] {
        let name = format!("rdm.node_admin.node.{op}.via-build");
        let at = named(&spans, &name).into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).unwrap_or_else(|| panic!("the recovery's {name} span"));
        assert_eq!(at["attributes"]["node_id"], before["node_id"], "{name} names the exited birth it acts for: {at}");
        assert!(
            named(&spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE && sp["attributes"]["pipeline"] == pipeline),
            "the {pipeline} workflow's pipeline span"
        );
    }
    assert!(!named(&spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| sp["attributes"]["pipeline"] == "recover"), "no recovery pipeline of its own");
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

/// CONTRACT (node-recovery.md "Failure and receipts": the mesh-primary is the submitter that submits
/// again; sequence step 1: keyed on the exited birth, a repeat answers AlreadyApplied or
/// BuildInProgress, never a second recovery; node-rpc-envelope.md "Caller execution and
/// certainty": a later attempt resumes from the recorded receipts, never from step 1): a member's
/// data dir loses its node-key and the member is killed. Its recovery's node.delete completes (the
/// exited birth departs, node.deleted once) and its node.create refuses by name at PrepareStorage,
/// so the attempt ends Failed. The key is put back. The mesh-primary submits the same exited birth
/// again on a later tick, though no view holds that birth any longer: a later attempt of the same
/// Build runs only the create and the update, and the path comes back as a new node over the same
/// data dir and endpoint key. Must NOT happen: the failed recovery left for good (nothing at the
/// path), a second node.delete of the exited birth, a Build minted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_whose_create_failed_after_its_delete_is_submitted_again_and_completes() {
    let (estate, birth_build) = estate("a_recovery_whose_create_failed_after_its_delete_is_submitted_again_and_completes").await;
    const NODE: &str = "mesh1.rpc.2";
    let before = estate.node(NODE).await;
    let data_dir = estate.data_dir_of(NODE).await;
    let key_path = Path::new(&data_dir).join("node-key");
    let key = std::fs::read(&key_path).unwrap_or_else(|e| panic!("{NODE}'s node-key in {data_dir}: {e}"));
    std::fs::remove_file(&key_path).unwrap();
    estate.kill_node(NODE).await;

    let submits = |spans: &[Value]| -> Vec<Value> { named(spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let failed = wait_for("the recovery's create fails at PrepareStorage", SETTLE, || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("failed")) }).await;
    let failed_attempt: u64 = s(&failed["attributes"]["attempt"]).parse().expect("the failed submit names its attempt");
    let outcome = s(&failed["attributes"]["outcome"]);
    assert!(outcome.contains(&format!("create-node:{NODE} step PrepareStorage")) && outcome.contains("node-key is missing"), "the create refused the dir with no key by name: {failed}");
    assert_eq!(s(&failed["attributes"]["build_id"]), birth_build, "{failed}");
    std::fs::write(&key_path, &key).unwrap();

    let after = wait_for(&format!("{NODE} ready as a new node after its recovery failed"), Duration::from_secs(60), || async {
        let n = estate.node_opt(NODE).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir_of(NODE).await, data_dir, "the same data dir");
    let done = wait_for("the completed submit of the exited birth is exported", Duration::from_secs(60), || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("complete")) }).await;
    assert_eq!(s(&done["attributes"]["build_id"]), birth_build, "the same Build, no Build minted: {done}");
    assert_eq!(done["attributes"]["incarnation_id"], before["incarnation_id"], "keyed on the exact exited birth: {done}");
    let attempt: u64 = s(&done["attributes"]["attempt"]).parse().expect("the completed submit names its attempt");
    assert!(attempt > failed_attempt, "a later attempt than the failed one: {done}");
    assert_eq!(s(&done["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "{done}");
    assert!(s(&done["attributes"]["after_failed_attempt"]).parse::<u64>().is_ok_and(|a| a >= failed_attempt && a < attempt), "the submit follows a Failed one of the same birth: {done}");

    let (_, build) = estate.get(&format!("/api/builds?id={birth_build}")).await;
    let steps = build["steps"].as_array().cloned().unwrap_or_default();
    let deleted: Vec<&Value> = steps.iter().filter(|r| r["operation"] == format!("retire-node:{NODE}").as_str() && r["step"] == "NodeDeleted").collect();
    assert_eq!(deleted.len(), 1, "one node.delete of the exited birth, never again: {deleted:?}");
    assert!(steps.iter().all(|r| !(r["operation"] == format!("retire-node:{NODE}").as_str() && r["attempt"] == json!(attempt))), "the completing attempt deletes nothing");
    let spans = wait_for("the completing attempt's reconcile span is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        reconcile_span(&spans, &birth_build, &attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    let reconcile = reconcile_span(&spans, &birth_build, &attempt.to_string()).expect("the attempt's reconcile span");
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("create-node:{NODE},update-node:{NODE}"), "only what was left: {reconcile}");
    estate.record_trace_url(done["trace_id"].as_str().unwrap_or(""));
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md "Failure and receipts": "the mesh-primary is the submitter that
/// resubmits by `build_id` ... the mesh-primary's next tick finds the member still proven exited and
/// submits the recovery"; CLAUDE.md read rule: node-local state that should be the durable answer is
/// replaced by the read): in mesh2 (two node-admins, its mesh-primary not the fabric-primary) a
/// member's data dir loses its node-key and the member is killed. Its Build's node.delete completes
/// and its node.create refuses at PrepareStorage, so the attempt ends Failed. mesh2's primary is then
/// killed with the key still missing (nothing it submitted can complete), and mesh2's other admin
/// takes the seat. Once no attempt for the member is in flight, the key is put back. The new
/// mesh-primary never submitted for the member, never inspects it (its birth departed) and holds
/// nothing of it in memory: it reads the accepted Build's Failed attempt and the birth's exit-proof
/// row and submits the same exited birth over op 0x20 to the fabric-primary in mesh1. A later attempt
/// of the same Build runs only the create and the update, and the path comes back as a new node over
/// the same data dir and endpoint key. Must NOT happen: the path left empty after the seat moved, a
/// second node.delete of the exited birth, a Build minted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_build_for_a_proven_exit_is_submitted_again_by_the_admin_that_takes_the_mesh_primary_seat() {
    let estate = Estate::bootstrap(owner("a_failed_build_for_a_proven_exit_is_submitted_again_by_the_admin_that_takes_the_mesh_primary_seat"), "fabric1", "mesh1").await;
    let (status, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 1}, {"name": "mesh2", "node_admin": 2, "rpc_node": 2}]}))
        .await;
    assert_eq!(status, 202, "{a}");
    let birth_build = s(&a["build_id"]);
    estate.await_build(&birth_build, SETTLE).await;
    estate.settled_shape(&[("mesh1", 1, 1), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    // The initial fabric refuses a node replace until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(SETTLE).await;
    let nodes = estate.nodes().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    assert!(fp.starts_with("mesh1."), "the day-0 admin keeps the fabric seat: {fp}");
    let mp = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2's primary")["name"]);
    let heir = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != mp.as_str()).expect("mesh2's second admin")["name"]);
    const NODE: &str = "mesh2.rpc.1";
    let before = estate.node(NODE).await;
    let data_dir = estate.data_dir_of(NODE).await;
    let key_path = Path::new(&data_dir).join("node-key");
    let key = std::fs::read(&key_path).unwrap_or_else(|e| panic!("{NODE}'s node-key in {data_dir}: {e}"));
    std::fs::remove_file(&key_path).unwrap();
    estate.kill_node(NODE).await;

    let submits = |spans: &[Value]| -> Vec<Value> { named(spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let failed = wait_for("the create of the Build for the proven exit fails at PrepareStorage", SETTLE, || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("failed")) }).await;
    let failed_attempt: u64 = s(&failed["attributes"]["attempt"]).parse().expect("the failed submit names its attempt");
    let outcome = s(&failed["attributes"]["outcome"]);
    assert!(outcome.contains(&format!("create-node:{NODE} step PrepareStorage")) && outcome.contains("node-key is missing"), "the create refused the dir with no key by name: {failed}");
    assert_eq!((s(&failed["attributes"]["submitter"]), s(&failed["attributes"]["fabric_primary"]), s(&failed["attributes"]["build_id"])), (mp.clone(), fp.clone(), birth_build.clone()), "{mp} submitted over op 0x20 to {fp}: {failed}");

    // mesh2's seat moves: its primary is killed with the key still missing, so nothing it submitted
    // can complete, and its second admin takes the seat.
    estate.kill_node(&mp).await;
    let seat = wait_for(&format!("{heir} takes mesh2's seat"), SETTLE, || async {
        named(&estate.spans(), "rdm.mesh.election.resolve.via-mesh-primary")
            .into_iter()
            .find(|sp| sp["attributes"]["observer"] == heir.as_str() && sp["attributes"]["winner_path"] == heir.as_str() && start(sp) > start(&failed))
            .cloned()
    })
    .await;
    // No attempt for the member is in flight (one the killed primary opened or ran is continued by
    // the drive and fails on the missing key): only a later submit can bring the path back.
    let (_, idle) = wait_for(&format!("no attempt for {NODE} is in flight in {birth_build}"), SETTLE, || async {
        let (status, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
        let running = matches!(b["state"].as_str(), Some("pending" | "running"));
        let member = b["action"]["action"] == "recover" && b["action"]["path"] == NODE;
        (status == 200 && !(running && member)).then_some((status, b))
    })
    .await;
    std::fs::write(&key_path, &key).unwrap();

    let after = wait_for(&format!("{NODE} ready as a new node after mesh2's seat moved"), SETTLE, || async {
        let n = estate.node_opt(NODE).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir_of(NODE).await, data_dir, "the same data dir");
    let done = wait_for("the completed submit of the exited birth is exported", Duration::from_secs(60), || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("complete")) }).await;
    assert_eq!((s(&done["attributes"]["submitter"]), s(&done["attributes"]["fabric_primary"])), (heir.clone(), fp.clone()), "the admin that took the seat submitted over op 0x20 to {fp}: {done}");
    assert_eq!(s(&done["attributes"]["disposition"]), "created", "the fabric-primary opened the next attempt: {done}");
    assert_eq!(s(&done["attributes"]["build_id"]), birth_build, "the same Build, no Build minted: {done}");
    assert_eq!(done["attributes"]["incarnation_id"], before["incarnation_id"], "keyed on the exact exited birth: {done}");
    let attempt: u64 = s(&done["attributes"]["attempt"]).parse().expect("the completed submit names its attempt");
    assert!(attempt > failed_attempt, "a later attempt than the failed one: {done}");
    assert_eq!(s(&done["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "{done}");
    assert!(s(&done["attributes"]["after_failed_attempt"]).parse::<u64>().is_ok_and(|a| a >= failed_attempt && a < attempt), "the submit follows the Failed attempt the Build holds: {done}");

    let spans = wait_for("the completing attempt's reconcile span is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        reconcile_span(&spans, &birth_build, &attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    // The fabric-primary admitted it from the op 0x20 door (route build.create), at that attempt.
    let admitted = named(&spans, "rdm.node_admin.build.update.via-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == before["node_id"] && s(&sp["attributes"]["attempt"]) == attempt.to_string())
        .unwrap_or_else(|| panic!("{fp} admitted attempt {attempt} for {NODE}"));
    assert_eq!((s(&admitted["attributes"]["route"]), s(&admitted["attributes"]["outcome"])), ("build.create".to_string(), "admitted".to_string()), "{admitted}");
    assert!(
        !named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").iter().any(|sp| sp["attributes"]["node_id"] == before["node_id"] && sp["attributes"]["inspector"] == heir.as_str()),
        "{heir} never inspected the departed birth: the submit came from the durable facts"
    );
    let reconcile = reconcile_span(&spans, &birth_build, &attempt.to_string()).expect("the attempt's reconcile span");
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("create-node:{NODE},update-node:{NODE}"), "only what was left: {reconcile}");
    let (_, build) = estate.get(&format!("/api/builds?id={birth_build}")).await;
    let steps = build["steps"].as_array().cloned().unwrap_or_default();
    let deleted: Vec<&Value> = steps.iter().filter(|r| r["operation"] == format!("retire-node:{NODE}").as_str() && r["step"] == "NodeDeleted").collect();
    assert_eq!(deleted.len(), 1, "one node.delete of the exited birth, never again: {deleted:?}");
    eprintln!(
        "s57 part 3: failed attempt {failed_attempt} by {mp} trace {} | seat {heir} trace {} | idle at {} attempt {} | complete attempt {attempt} by {heir} trace {} | admitted trace {}",
        failed["trace_id"].as_str().unwrap_or(""),
        seat["trace_id"].as_str().unwrap_or(""),
        s(&idle["state"]),
        idle["attempt"],
        done["trace_id"].as_str().unwrap_or(""),
        admitted["trace_id"].as_str().unwrap_or(""),
    );
    estate.record_trace_url(done["trace_id"].as_str().unwrap_or(""));
    estate.settled_shape(&[("mesh1", 1, 1), ("mesh2", 2, 2)], SETTLE).await;
    estate.shutdown().await;
}

/// The fence spans of the exited birth `old` (node-recovery.md "Delete fences the old instance
/// before the new join"): its resolver entry and the direct paths of its endpoint key retired.
fn exit_fences<'a>(spans: &'a [Value], old: &Value) -> Vec<&'a Value> {
    named(spans, "rdm.node_admin.node.update.via-exit-paths-retired").into_iter().filter(|sp| sp["attributes"]["node_id"] == old["node_id"] && sp["attributes"]["kind"] == "recovery").collect()
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

/// The steps of a planned replace of a live birth, in the order node-replace.md numbers them: the old
/// birth stops (stop-node, its node.connections.delete { All } inside it, node-left, its NodeStopped) and is
/// parked (1), is renamed `<path>.old` while parked (2), and node.create starts the successor at the
/// freed name (3). The predecessor stays parked with its data.
const REPLACE_ORDER: [&str; 9] = [
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
/// operation `replace-node:<path>`; the old birth stops (its connections deleted inside the stop), its NodeStopped drops it from
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
    // executor's RenamePredecessor the mesh's primary, which executes it, moves the predecessor's row
    // to `<path>.old`, where it stays the parked predecessor's row, and the successor's create claims
    // its row at the path.name, seeded from the .old row, before it is launched (R13).
    let moved: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-rename").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(moved.len(), 1, "one move of the predecessor's row: {moved:?}");
    assert_eq!((s(&moved[0]["attributes"]["from"]), s(&moved[0]["attributes"]["to"]), s(&moved[0]["attributes"]["outcome"])), (NODE.to_string(), old_name.clone(), "renamed".to_string()), "{}", moved[0]);
    let old_deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == old_name.as_str()).collect();
    assert!(old_deleted.is_empty(), "no Deleted row at {old_name}: the predecessor is parked, not deleted: {old_deleted:?}");
    let seeded: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == NODE && sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["moment"] == "claim").collect();
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

/// CONTRACT: the recipe's `node[1].delete` of the stopped predecessor a replace left at `<path>.old`
/// (node-replace.md; node-decommission.md 2-4): `DELETE /api/nodes/<path>.old` is the next attempt of
/// the accepted Build (reason delete, action delete fenced to the exact old birth, no Build minted).
/// One operation `delete-node:<path>.old` asks the provider to end the stopped runtime and inspects
/// it; only the provider's Exited is the proof, after which the executor publishes NodeDeleted for
/// the exact old birth and the mesh's primary, which executes it, writes the Deleted row at
/// `<path>.old`, its record kept (R13). The predecessor's process is gone; its data dir stays; the successor at the path is
/// untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_the_stopped_predecessor_proves_its_exit_publishes_node_deleted_and_writes_its_deleted_row() {
    let (estate, birth_build) = estate("deleting_the_stopped_predecessor_proves_its_exit_publishes_node_deleted_and_writes_its_deleted_row").await;
    let parked = replace_and_park(&estate, &birth_build, "mesh1.rpc.1").await;
    delete_parked_and_assert(&estate, &birth_build, &parked).await;
    estate.shutdown().await;
}

/// CONTRACT: the same delete when the fabric-primary is in another mesh than the admin that ran the
/// replace (node-decommission.md 3; R13: the mesh-primary of a node's mesh is the only writer of that
/// mesh's rows). The fabric-primary in mesh1 admits `delete-node:<path>.old` of a mesh2 node from the
/// record it holds of the parked predecessor (read from the replace's frames its drive read), fenced
/// to that exact birth, although its own view never names `<path>.old`; mesh2's primary executes the
/// delete and writes the Deleted row at `<path>.old`; the fabric-primary writes no row of mesh2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_peer_mesh_parked_predecessor_is_admitted_from_the_fabric_primary_held_record() {
    let (estate, birth_build) = two_mesh_estate("deleting_a_peer_mesh_parked_predecessor_is_admitted_from_the_fabric_primary_held_record").await;
    let parked = replace_and_park(&estate, &birth_build, "mesh2.rpc.1").await;
    assert!(parked.replaced_by.starts_with("mesh2.admin."), "mesh2's primary ran the replace: {}", parked.replaced_by);
    assert!(parked.fabric_primary.starts_with("mesh1.admin."), "the fabric-primary is in mesh1: {}", parked.fabric_primary);
    let executor = delete_parked_and_assert(&estate, &birth_build, &parked).await;
    assert_eq!(executor, parked.replaced_by, "mesh2's primary, which ran the replace, executes the delete");
    estate.shutdown().await;
}

/// CONTRACT: the same delete when the parked predecessor's mesh changed its primary between the
/// replace and the delete (node-decommission.md 2: "Stop and inspect the exact runtime through
/// provider control"). mesh2's new primary did not run the replace and its view does not name
/// `<path>.old`; it resolves the exact parked birth the action is fenced to from what it holds of that
/// birth (its membership's parked digest, else its durable runtime row of it), ends it with the
/// provider's Exited proof and publishes NodeDeleted for it, and, as mesh2's primary, writes the
/// Deleted row at `<path>.old`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_parked_predecessor_after_its_mesh_primary_changed_is_executed_by_the_new_primary_on_the_exact_birth() {
    let estate = Estate::bootstrap(owner("deleting_a_parked_predecessor_after_its_mesh_primary_changed_is_executed_by_the_new_primary_on_the_exact_birth"), "fabric1", "mesh1").await;
    let (status, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 2}, {"name": "mesh2", "node_admin": 2, "rpc_node": 2}]}))
        .await;
    assert_eq!(status, 202, "{a}");
    let birth_build = s(&a["build_id"]);
    estate.await_build(&birth_build, SETTLE).await;
    estate.settled_shape(&[("mesh1", 1, 2), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    // The initial fabric refuses a node replace until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(SETTLE).await;
    let parked = replace_and_park(&estate, &birth_build, "mesh2.rpc.1").await;
    let first_primary = parked.replaced_by.clone();
    assert!(first_primary.starts_with("mesh2.admin."), "mesh2's primary ran the replace: {first_primary}");
    // mesh2's primary changes: its planned replace hands the seat to mesh2's other admin, which
    // executes that replace (executor_for: the mesh primary never executes its own).
    let before_admin = estate.node(&first_primary).await;
    let (status, r) = estate.post(&format!("/api/nodes/{first_primary}/replace"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    let admin_attempt = Estate::attempt_of(&r);
    estate.await_attempt(&birth_build, admin_attempt, SETTLE).await;
    wait_for(&format!("{first_primary} ready under a new NodeId"), SETTLE, || async {
        estate.nodes().await.into_iter().find(|n| n["name"] == first_primary.as_str() && ready(n) && n["node_id"] != before_admin["node_id"])
    })
    .await;
    let spans = wait_for("the admin replace's dispatch is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        dispatch_executor(&spans, &birth_build, &admin_attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    let new_primary = dispatch_executor(&spans, &birth_build, &admin_attempt.to_string()).unwrap();
    assert!(new_primary.starts_with("mesh2.admin.") && new_primary != first_primary, "mesh2's other admin took the seat: {new_primary}");
    let executor = delete_parked_and_assert(&estate, &birth_build, &parked).await;
    assert_eq!(executor, new_primary, "mesh2's new primary, which did not run the replace, executes the delete");
    estate.shutdown().await;
}

/// The parked predecessor a replace left: who it was, where, and who ran the replace.
struct Parked {
    node: String,
    before: Value,
    after: Value,
    pid: u64,
    old_dir: std::path::PathBuf,
    /// The admin that executed the replace (its dispatch's executor).
    replaced_by: String,
    /// The admin that drove it as fabric-primary (its dispatch's `node`).
    fabric_primary: String,
}

/// The executor the fabric-primary dispatched `attempt` of `build` to.
fn dispatch_executor(spans: &[Value], build: &str, attempt: &str) -> Option<String> {
    dispatch(spans, build, attempt).map(|sp| s(&sp["attributes"]["executor"]))
}

fn dispatch<'a>(spans: &'a [Value], build: &str, attempt: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt)
}

/// A planned replace of `node`, completed, the successor ready and the predecessor parked.
async fn replace_and_park(estate: &Estate, build: &str, node: &str) -> Parked {
    let before = estate.node(node).await;
    let pid = estate.pid_of(node).await;
    let old_dir = std::path::PathBuf::from(estate.data_dir_of(node).await);
    let (status, r) = estate.post(&format!("/api/nodes/{node}/replace"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    let attempt = Estate::attempt_of(&r);
    estate.await_attempt(build, attempt, SETTLE).await;
    let after = wait_for(&format!("{node} ready under a new NodeId"), SETTLE, || async {
        estate.nodes().await.into_iter().find(|n| n["name"] == node && ready(n) && n["node_id"] != before["node_id"])
    })
    .await;
    assert!(alive(pid), "the predecessor {pid} is parked, alive, before its delete");
    let spans = wait_for("the replace's dispatch is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        dispatch(&spans, build, &attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    let d = dispatch(&spans, build, &attempt.to_string()).unwrap();
    Parked { node: node.to_string(), before, after, pid, old_dir, replaced_by: s(&d["attributes"]["executor"]), fabric_primary: s(&d["attributes"]["node"]) }
}

/// `DELETE /api/nodes/<path>.old` of `parked`, and everything node.delete owes (node-decommission.md
/// 2-4). Returns the admin that executed the delete.
async fn delete_parked_and_assert(estate: &Estate, birth_build: &str, parked: &Parked) -> String {
    let Parked { node, before, after, pid, old_dir, .. } = parked;
    let old_name = format!("{node}.old");
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
    estate.await_attempt(birth_build, attempt_no, SETTLE).await;
    let attempt = attempt_no.to_string();
    let spans = wait_for("the delete's last step, its reconcile span, its dispatch and the Deleted row are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let row = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().any(|sp| sp["attributes"]["subject"] == old_name.as_str());
        (step_span(&spans, birth_build, &attempt, &old_name, "Complete").is_some() && reconcile_span(&spans, birth_build, &attempt).is_some() && dispatch(&spans, birth_build, &attempt).is_some() && row).then_some(spans)
    })
    .await;
    let reconcile = reconcile_span(&spans, birth_build, &attempt).unwrap();
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("delete-node:{old_name}"), "one delete operation of the parked birth: {reconcile}");
    let mut last = 0u64;
    for step in DELETE_PARKED_ORDER {
        let sp = step_span(&spans, birth_build, &attempt, &old_name, step).unwrap_or_else(|| panic!("step {step} of the delete ran"));
        assert!(start(sp) >= last, "step {step} starts before the step it follows: {sp}");
        last = start(sp);
    }
    for step in ["DeleteConnections", "StopNode", "ReleaseStorage"] {
        assert!(step_span(&spans, birth_build, &attempt, &old_name, step).is_none(), "the parked birth's delete runs no {step}");
    }
    // The provider's Exited is the proof, and it precedes the NodeDeleted the executor publishes.
    let terminate = step_span(&spans, birth_build, &attempt, &old_name, "TerminateRuntime").unwrap();
    assert_eq!(s(&terminate["attributes"]["outcome"]), "complete", "{terminate}");
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "NodeDeleted once for the old identity: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact old birth");
    assert_eq!(s(&deleted[0]["attributes"]["operation"]), format!("delete-node:{old_name}"));
    assert!(end(terminate) <= start(deleted[0]), "NodeDeleted follows the exit proof: {terminate} vs {}", deleted[0]);
    // The Deleted row at `<path>.old`, written by the mesh's primary that executes the delete (R13).
    let dispatched = dispatch(&spans, birth_build, &attempt).unwrap();
    let executor = s(&dispatched["attributes"]["executor"]);
    let rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == old_name.as_str()).collect();
    assert_eq!(rows.len(), 1, "one Deleted row at {old_name}: {rows:?}");
    assert_eq!(
        (s(&rows[0]["attributes"]["node"]), s(&rows[0]["attributes"]["node_id"]), s(&rows[0]["attributes"]["outcome"])),
        (executor.clone(), s(&before["node_id"]), "row-deleted".to_string()),
        "the mesh's primary executing the delete writes the Deleted row of the exact old node: {}",
        rows[0]
    );
    estate.artifact("deleted-row.json", &json!({"row": rows[0], "node_deleted": deleted[0], "terminate": terminate, "dispatch": dispatched}));
    let op = named(&spans, "rdm.node_admin.node.delete.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == old_name.as_str()).expect("the delete operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    // The parked process is gone, its data dir stays, and the successor is untouched.
    assert!(!alive(*pid), "the parked predecessor's process {pid} exited");
    assert!(old_dir.exists(), "node.delete releases node.id and name, not storage: {old_dir:?}");
    let now = estate.node(node).await;
    assert!(now["node_id"] == after["node_id"] && ready(&now), "the successor at {node} is untouched: {now}");
    s(&dispatched["attributes"]["executor"])
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
    assert_eq!(repeat.build_id().as_str(), birth_build, "the accepted Build");
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
/// held (SIGSTOP) and a `node.connections.delete { All }` of it is requested at once, attempt N of the
/// accepted Build (`node.connections.delete:<path>`, carried by its mesh-primary). While that call
/// waits on the held process, the process is killed. The held, then exited, birth never answers
/// node.connections.deleted: attempt N ends Failed at DeleteConnections, named by how the call ended
/// (node-connections-delete.md "Admission is not completion"), never Complete. Which transport outcome the
/// lost command had (never sent, or sent and its reply lost) is not the contract; the workflow's
/// Failed end is. Then the mesh-primary proves the
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
    let (status, r) = estate.post(&format!("/api/nodes/{node}/connections/delete"), &json!({})).await;
    assert_eq!(status, 202, "the view still holds the member ready: {r}");
    assert_eq!(s(&r["build_id"]), birth_build, "{r}");
    let delete_attempt = Estate::attempt_of(&r);
    let killed = rt.apply(Fault::Kill).expect("the held runtime is killed");
    assert!(killed.exited, "{killed:?}");

    let operation = format!("node.connections.delete:{node}");
    let failed = wait_for("the node.connections.delete attempt ends Failed", SETTLE, || async {
        let (_, b) = estate.http_get(&fp_api, &format!("/api/builds?id={birth_build}")).await;
        b["steps"].as_array().cloned().unwrap_or_default().into_iter().find(|r| r["operation"] == operation.as_str() && r["attempt"] == json!(delete_attempt) && r["outcome"]["failed"].is_object())
    })
    .await;
    // The exit belongs to the node.connections.delete: the held, then exited, birth never answered
    // node.connections.deleted, and the attempt ends Failed, never Complete.
    assert_eq!(s(&failed["step"]), "DeleteConnections", "the node.connections.delete is not complete: {failed}");
    assert!(s(&failed["outcome"]["failed"]["reason"]).contains("node.connections.delete { All } ended"), "the failure names how the call ended: {failed}");

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
    assert!(attempt > delete_attempt, "the recovery is a later attempt than the failed node.connections.delete: {recovery}");
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

/// The span that ends the `observer`'s first tickle round for `node_id` once its direct connect
/// failed: the via-peer step (`rdm.node_admin.node.resolve.via-peer-tickle`), closed by the round
/// itself when its verdict comes back (offline.rs `OfflineTickle::tick`, the round's `step` span),
/// once exported. The hold-down span ([`tickle_span`] round 1) is the loop's fold of that verdict,
/// made by the first pass after the round ended.
fn round_end_span<'a>(spans: &'a [Value], observer: &str, node_id: &Value) -> Option<&'a Value> {
    let mut found: Vec<&Value> = named(spans, "rdm.node_admin.node.resolve.via-peer-tickle")
        .into_iter()
        .filter(|sp| sp["attributes"]["observer"] == observer && sp["attributes"]["node_id"] == *node_id && sp["attributes"]["verdict"].is_string())
        .collect();
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
    // The stop of mesh2's primary below moves a seat, which the initial fabric refuses until it has first
    // reached ready-for-traffic; a killed member keeps it from ever climbing.
    estate.until_fabric_ready(SETTLE).await;
    let floor = rafka_mesh_transport::membership::staleness_floor();
    let nodes = estate.nodes().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    assert!(fp.starts_with("mesh1."), "the day-0 admin keeps the fabric seat: {fp}");
    let mp = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2's primary")["name"]);
    let standby = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != mp.as_str()).expect("mesh2's second admin")["name"]);
    const NODE: &str = "mesh2.rpc.1";
    let before = estate.node(NODE).await;
    estate.kill_node(NODE).await;

    // Round 1 on the mesh-primary that holds the seat now. Its via-peer step's span ends the round
    // (`round_end_span`), due by node-recovery.md steps 1-2: the silence (one floor after the last
    // digest heard: offline.rs `silent_set` derives the mark from it), the loop's pass half a floor
    // after the mark (on the gossip-interval grid: up to one interval late), then one tickle round:
    // one direct connect, then the connects through up to two peers at once, each to a killed process
    // and so spending its whole dial: the connection's idle timeout, the staleness floor (R-T8),
    // inside the call's 10 s budget. Then the export's 200 ms schedule and this cell's read of it
    // (one gossip interval). The hold-down span is not the round's end: the loop folds the verdict at
    // its first pass after the round ended, and a round of two whole dials starts on a pass and ends
    // just past one, so that fold comes almost one interval after the verdict.
    let gossip = rafka_mesh_entity::cadence::gossip_interval();
    let connect = floor.min(Duration::from_secs(10));
    let round1_due = floor + gossip + gossip + floor / 2 + rafka_node_admin_core::offline::tickle_round(connect) + Duration::from_millis(200);
    let round1 = wait_for(&format!("{mp} runs tickle round 1 for {NODE}"), round1_due, || async { round_end_span(&estate.spans(), &mp, &before["node_id"]).cloned() }).await;
    assert_eq!(s(&round1["attributes"]["verdict"]), "no-path", "{mp}'s round 1 finds no path to the killed {NODE}: {round1}");
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

/// CONTRACT (the approved proposal docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md
/// Part C1 "The no-successor cases", Q7 ruled Luke 2026-10-12: "`node.primary.update` commanded for a
/// cohort with no other node of that type in the mesh is refused by name, `cannot transfer: no secondary
/// of type <node_type> in mesh <mesh>`, until a secondary is available. The holder stays primary, nothing
/// is cut off, no row is written, and the workflow that commanded it ... fails by name with that
/// reason"): in a fabric whose fabric-primary is its mesh's only node-admin, each of the routes `replace`,
/// `restart`, `connections/delete` and `stop` on the fabric-primary is refused by name, 422 `last-admin`
/// with exactly `cannot transfer: no secondary of type node-admin in mesh <its mesh>`, one
/// `rdm.node_admin.build.reject.via-last-admin` per route with the same text. The mesh seat has no
/// secondary, so the peer mesh's Ready primary is no successor of both seats (C2's outside search is
/// mesh.delete's only). No attempt opens, no seat move is commanded, no row is written, and every node
/// stays as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replace_of_the_fabric_primary_that_is_its_meshs_last_admin_is_refused_by_name() {
    let (estate, birth_build) = two_mesh_estate("a_replace_of_the_fabric_primary_that_is_its_meshs_last_admin_is_refused_by_name").await;
    let before = estate.nodes().await;
    let primary = before.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let fp = s(&primary["name"]);
    let mesh = s(&primary["mesh"]);
    assert_eq!(before.iter().filter(|n| n["kind"] == "node_admin" && n["mesh"] == mesh.as_str()).count(), 1, "the fabric-primary is its mesh's only admin: {before:#?}");
    let (_, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
    let attempt = b["attempt"].clone();
    let q7 = format!("cannot transfer: no secondary of type node-admin in mesh {mesh}");
    for action in ["replace", "restart", "connections/delete", "stop"] {
        let (status, r) = estate.post(&format!("/api/nodes/{fp}/{action}"), &json!({})).await;
        assert_eq!((status, r["error"].as_str(), r["detail"].as_str()), (422, Some("last-admin"), Some(q7.as_str())), "{action}: refused with Q7's exact text: {r}");
    }
    let (_, b) = estate.get(&format!("/api/builds?id={birth_build}")).await;
    assert_eq!(b["attempt"], attempt, "no attempt opened: {b}");
    let spans = wait_for("the four refusals are exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        (named(&spans, "rdm.node_admin.build.reject.via-last-admin").len() >= 4).then_some(spans)
    })
    .await;
    let refused = named(&spans, "rdm.node_admin.build.reject.via-last-admin");
    let routes: std::collections::BTreeSet<String> = refused.iter().map(|sp| s(&sp["attributes"]["route"])).collect();
    assert_eq!(routes.len(), 4, "one refusal per route: {routes:?}");
    assert!(refused.iter().all(|sp| s(&sp["attributes"]["detail"]) == q7), "every refusal span carries Q7's exact text: {refused:?}");
    assert!(named(&spans, "rdm.node_admin.seat.update.via-primary-updating").is_empty(), "no node.primary.update ran");
    assert!(named(&spans, "rdm.node_admin.seat.update.via-seat-row-written").is_empty(), "no row was written");
    assert!(
        named(&spans, "rdm.node_admin.build.update.via-rest").into_iter().all(|sp| s(&sp["attributes"]["attempt"]).is_empty()),
        "no attempt number was recorded by any refused request"
    );
    estate.record_trace_url(refused[0]["trace_id"].as_str().unwrap_or(""));
    for n in &before {
        assert_eq!(estate.node(&s(&n["name"])).await["incarnation_id"], n["incarnation_id"], "{} is untouched", s(&n["name"]));
    }
    estate.shutdown().await;
}

/// A caller outside the fabric that reaches the node-admin `admin` by its exact birth: its own
/// endpoint, and a resolver holding that birth as the answering admin's view names it.
async fn caller_of(admin: &Value) -> (std::sync::Arc<rafka_node_rpc::NodeRpcClient>, rafka_mesh_entity::NodeId) {
    use rafka_node_rpc::{NodeRpcClient, ResolvedNode, StaticResolver};
    let ep = rafka_node_rpc::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller binds an endpoint");
    let resolver = std::sync::Arc::new(StaticResolver::new());
    let node_id: rafka_mesh_entity::NodeId = s(&admin["node_id"]).parse().expect("a node id");
    resolver.insert(ResolvedNode {
        node_id: node_id.clone(),
        name: s(&admin["name"]).parse().unwrap(),
        endpoint_id: s(&admin["endpoint_id"]).parse().expect("an iroh key"),
        transport_addr: s(&admin["transport_addr"]).parse().expect("a transport address"),
        incarnation: rafka_mesh_entity::IncarnationId::parse(&s(&admin["incarnation_id"])).unwrap(),
    });
    (std::sync::Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), node_id)
}

/// CONTRACT: a recovered node keeps the config its mesh-primary told its predecessor
/// (node-recovery.md "Retained: ... config (seeded from the Deleted row)"; fabric-node-lifecycle.md
/// "A rewrite is lossless for every field its writer knows"). The caller puts `<mesh>.rpc.1`'s config
/// through its mesh-primary (not the fabric-primary), the node carries it, then its runtime is
/// killed: the mesh-primary's recovery writes the exited birth's Deleted row with that config, the
/// new node's name claim is seeded from it (`via-node-row` moment `claim`, `seeded_from` the exited
/// node, one key), the new node's join answer hands it that config (`via-join` `config_keys` 1),
/// and the new node (new node.id, same path.name) carries the config.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_node_keeps_the_config_its_mesh_primary_told_its_predecessor() {
    let (estate, _build) = two_mesh_estate("a_recovered_node_keeps_the_config_its_mesh_primary_told_its_predecessor").await;
    let nodes = estate.nodes().await;
    let (node, mesh_primary, _fp) = placement(&nodes);
    let node_path = node.as_str();
    let before = estate.node(node_path).await;
    let admin = nodes.iter().find(|n| n["name"] == mesh_primary.as_str()).expect("the mesh-primary's view row").clone();
    let (client, mp_id) = caller_of(&admin).await;
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let node_id: rafka_mesh_entity::NodeId = s(&before["node_id"]).parse().unwrap();
    let birth = rafka_node_admin_client::ExactBirth { target: rafka_node_rpc::NodeTarget::ExactNode(node_id.clone()), node_id, incarnation: rafka_mesh_entity::IncarnationId::parse(&s(&before["incarnation_id"])).unwrap() };
    let reply = rafka_node_admin_client::NodeRpc::new(&client)
        .config_update(&mp_id, &rafka_node_rpc::RouteChoice::Direct, &birth, told.clone(), &rafka_node_rpc::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.config.update of {node_path} at {mesh_primary}: {e:?}"));
    assert_eq!(reply.name(), "config-updated", "the mesh-primary wrote the row and the node applied it: {reply:?}");
    let told_json = json!({"tier": "gold"});
    wait_for(&format!("{node_path} carries its config"), Duration::from_secs(30), || async { (estate.node(node_path).await["config"] == told_json).then_some(()) }).await;

    estate.kill_node(node_path).await;
    let after = wait_for(&format!("{node_path} ready as a new node"), SETTLE, || async {
        let n = estate.node_opt(node_path).await?;
        (ready(&n) && n["node_id"] != before["node_id"]).then_some(n)
    })
    .await;
    // The new node's name claim, written by the mesh-primary, is seeded from the exited birth's
    // Deleted row: its config is the one told.
    let claim = wait_for("the new node's name claim is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["moment"] == "claim").cloned()
    })
    .await;
    let a = &claim["attributes"];
    assert_eq!((s(&a["node"]), s(&a["outcome"]), s(&a["seeded_from"])), (mesh_primary.clone(), "row-seeded".to_string(), s(&before["node_id"])), "the mesh-primary claims the name seeded from the exited birth's Deleted row: {claim}");
    assert_eq!(s(&a["keys"]), "1", "the seeded row carries the config told to the predecessor: {claim}");
    let deleted: Vec<Value> = named(&estate.spans(), "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect();
    assert_eq!(deleted.len(), 1, "one Deleted row of the exited birth: {deleted:?}");
    assert_eq!(s(&deleted[0]["attributes"]["keys"]), "1", "the Deleted row keeps the config: {deleted:?}");
    // The new node is handed that config in its join answer (node-start.md step 2), before it
    // publishes anything or declares readiness: the claim was written before it was launched.
    let joined = wait_for("the new node's join is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).cloned()
    })
    .await;
    assert_eq!((s(&joined["attributes"]["outcome"]), s(&joined["attributes"]["config_keys"])), ("installed".to_string(), "1".to_string()), "the new node's join answer hands its row's config: {joined}");
    let carried = wait_for(&format!("the new {node_path} carries the config"), Duration::from_secs(30), || async { let n = estate.node(node_path).await; (n["config"] == told_json).then_some(n) }).await;
    assert_eq!(carried["node_id"], after["node_id"], "the new node, not the exited one, carries it");
    let told: Vec<Value> = named(&estate.spans(), "rdm.node_admin.node.update.via-config").into_iter().filter(|sp| sp["attributes"]["node_id"] == after["node_id"]).cloned().collect();
    assert!(told.is_empty(), "the join handed the config: no config call is sent to the starting node: {told:?}");
    eprintln!("trace {}: via-node-row delete node_id={} keys={}; trace {}: via-node-row claim node={} seeded_from={} keys={}", s(&deleted[0]["trace_id"]), s(&before["node_id"]), deleted[0]["attributes"]["keys"], s(&claim["trace_id"]), mesh_primary, s(&a["seeded_from"]), a["keys"]);
}
