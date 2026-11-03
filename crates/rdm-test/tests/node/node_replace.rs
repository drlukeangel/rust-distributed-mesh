//! product=mesh, feature=node-lifecycle, subfeature=node-replace, rung=MN, provider=process.
//!
//! A planned replace (Luke R-R1) and node recovery (node-recovery.md, R-RC1). A planned
//! replacement is the next attempt of the accepted Build with a typed action; no Build is minted.
//! A member that exits outside any workflow is recovered by its own mesh-primary on the provider's
//! exact exited verdict: node.delete of the exited instance, then node.create at the same (path, name)
//! over the inherited data dir (same endpoint key, new node.id). A member that is hung but still
//! running is never recovered; a repeat submit is answered, never run twice; an exit during a
//! workflow fails that workflow and the next tick recovers the member.

use crate::build_log;
use rdm_client::{BuildFrame, CallEnd, Change, Frame, NodeEvent, NodeStatus, Nodes, PathName};
use rdm_core::build::Step;
use rdm_test::estate::{at_location, descends_from, is_operation, location, named, wait_for, Estate, Owner};
use rdm_core::{NodeId, OperationSubject};
use rdm_test::steps::{replace as replace_steps, replace_keeping_predecessor, rollback as rollback_steps};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use rdm_test::process_faults::{ExactRuntime, Fault};
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
    node["status"] == "ready"
}

/// A birth that holds or contends for a role and serves (`NodeStatus::is_live`).
fn live(node: &Value) -> bool {
    matches!(node["status"].as_str(), Some("pending" | "ready" | "connections-deleting"))
}

/// The node id a view row names.
fn node_id_of(v: &Value) -> NodeId {
    serde_json::from_value(v["node_id"].clone()).expect("a view row names its node id")
}

/// A reconcile span's `operations`: the attempt's operations as the executor lists them.
fn operations(ops: &[rdm_core::OperationId]) -> String {
    format!("{ops:?}")
}

/// Every admin's view at `admins` holds at most one live birth at `path`. An admin that does not answer
/// `node.get` for this Fabric within 2 s (being replaced, a successor not bound yet) is skipped.
async fn at_most_one_live_birth(estate: &Estate, admins: &[PathName], path: &PathName) {
    for admin in admins {
        let Some(views) = estate.node_views_at(admin).await else { continue };
        let at: Vec<(String, String)> = Estate::values(&views).iter().filter(|n| n["name"] == json!(path) && live(n)).map(|n| (s(&n["node_id"]), s(&n["status"]))).collect();
        assert!(at.len() <= 1, "the view of {admin:?} holds two live births at {path:?}: {at:?}");
    }
}

/// The node at `name` once `holds` is true of what `node.get` lists there, as the JSON the view
/// serializes to.
async fn until_node_where(estate: &Estate, name: &PathName, what: &str, within: Duration, holds: impl Fn(&Value) -> bool) -> Value {
    estate
        .until_node(name, what, within, |n| {
            let v = serde_json::to_value(n?).expect("a view serializes");
            holds(&v).then_some(v)
        })
        .await
}

/// The node at `name` once it is listed ready under a node id other than `before`'s.
async fn until_new_birth(estate: &Estate, name: &PathName, before: &Value, within: Duration) -> Value {
    until_node_where(estate, name, &format!("{name:?} ready as a new node"), within, |n| ready(n) && n["node_id"] != before["node_id"]).await
}

/// A fabric born at `shape` (each `(mesh, node_admin, rpc_node)`) by `build.create`, followed to its
/// terminal frame, the view settled on it within `settle` and the fabric ready; the birth
/// Build's id. The cells hold, kill and replace members: workflows the initial fabric refuses until it has
/// first reached ready (states.md "The initial fabric before ready").
async fn fabric_of(test: &str, shape: &[(&str, u32, u32)], settle: Duration) -> (Estate, String) {
    born_at(Estate::bootstrap(owner(test), "fabric1", "mesh1").await, shape, settle).await
}

/// [`fabric_of`] with every node-admin the testkit's faulted node-admin (`faults::binding_set`): its
/// fault door (testkit Node RPC op 0x75) holds the admin at a cut the cell arms, and nothing else of it
/// differs.
async fn faulted_fabric_of(test: &str, shape: &[(&str, u32, u32)], settle: Duration) -> (Estate, String) {
    let sha = candidate_sha();
    let estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    born_at(estate, shape, settle).await
}

/// `estate`'s fabric born at `shape` by `build.create`, followed to its terminal frame, the view settled
/// on it within `settle` and the fabric ready; the birth Build's id.
async fn born_at(estate: Estate, shape: &[(&str, u32, u32)], settle: Duration) -> (Estate, String) {
    let born = estate.reconcile_shape("fabric1", shape).await;
    estate.settles_to(shape, settle).await;
    estate.until_fabric_ready(SETTLE).await;
    (estate, born.build_id.to_string())
}
/// mesh1 with two node-admins and three nodes, through the rectifier; the birth Build's id.
async fn estate(test: &str) -> (Estate, String) {
    fabric_of(test, &[("mesh1", 2, 3)], Duration::from_secs(60)).await
}

/// Two meshes, so a mesh2 node is launched by mesh2's admin and proven by the fabric-primary in mesh1,
/// which did not launch it and so cannot read its exit code from a child handle.
async fn two_mesh_estate(test: &str) -> (Estate, String) {
    fabric_of(test, &[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(90)).await
}

/// The mesh-primary's recovery submit for `node`, once exported (node-recovery.md: the tickle,
/// the TRUE OFFLINE mark, the provider inspection and the submit each have a span on the
/// mesh-primary).
async fn recovery_span(estate: &Estate, node: &PathName, node_id: &Value) -> Value {
    wait_for(&format!("the mesh-primary's recovery submit for {node:?} is exported"), Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().find(|sp| at_location(sp, node) && sp["attributes"]["node_id"] == *node_id).cloned()
    })
    .await
}

/// The member under test in a two-mesh estate: a node holding no role, of the mesh that does NOT hold the
/// fabric-primary, so its mesh-primary is not the fabric-primary; that mesh-primary's view row; and
/// the fabric-primary's view row.
fn member_and_primaries(nodes: &[Value]) -> (PathName, Value, Value) {
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh = if primary["mesh"] == "mesh1" { "mesh2" } else { "mesh1" };
    let mesh_primary = nodes.iter().find(|n| n["mesh"] == mesh && n["type"] == "node-admin" && n["is_primary"] == true).expect("the node's mesh-primary").clone();
    assert_ne!(mesh_primary["node_id"], primary["node_id"], "the member's mesh-primary is not the fabric-primary");
    // The member is a node holding no role: a node-type primary runs node.primary.update before its
    // own node.connection.delete, stop or delete (lifecycles/node-connections-delete.md step 0), which
    // is not the member workflow these cells follow.
    let member = nodes.iter().filter(|n| n["mesh"] == mesh && n["type"] == "node" && n["is_primary"] != true).map(location).min().expect("an rpc member of the mesh that holds no primary role");
    (member, mesh_primary, primary)
}

/// The exact runtime `node`'s birth published in its data dir: the fault door acts on it alone.
async fn exact_runtime(estate: &Estate, node: &PathName) -> ExactRuntime {
    let dir = estate.data_dir(node).await;
    ExactRuntime::published(Path::new(&dir)).unwrap_or_else(|r| panic!("{node:?}: no exact runtime published in {dir}: {r:?}"))
}

/// The spans of the trace `of` is in: one recovery submit's own run (its delete and its create, their
/// operations and steps).
fn in_trace(spans: &[Value], of: &Value) -> Vec<Value> {
    spans.iter().filter(|sp| sp["trace_id"] == of["trace_id"]).cloned().collect()
}

/// Whether `sp` is a Node RPC call of `op` (the call span names the op number).
fn calls_op(sp: &Value, op: u8) -> bool {
    sp["attributes"]["op"] == op.to_string().as_str() || sp["attributes"]["op"] == json!(op)
}

/// A recovery belongs to no Build (Q342): the spans of its delete and its create carry the empty Build
/// and the attempt constant (D44).
const NO_BUILD: &str = "";
const NO_ATTEMPT: &str = "0";

/// The recovery of `before` run by its mesh-primary `mp`, in the submit's own trace `run`
/// (node-recovery.md "Execution", Q342): `mp` submits it (`via-node-recovery`, the root) and runs it
/// itself, its own workflow: node.delete of the exited birth, then node.create at its path and name,
/// each under the submit. It is never a Build: no Build call (op 0x20) and no claim call (op 0x1C) in
/// its run, no attempt run, no reconcile and no Build's open of it.
fn assert_mesh_primary_ran_the_recovery(run: &[Value], mp: &Value, before: &Value) {
    let submit = named(run, "rdm.node_admin.node.update.via-node-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == before["node_id"])
        .expect("the submit roots the run");
    assert_eq!(s(&submit["attributes"]["submitter"]), s(&mp["node_id"]), "{:?} submits: {submit}", location(mp));
    for name in ["rdm.node_admin.node.delete.via-node-recovery", "rdm.node_admin.node.create.via-node-recovery"] {
        let step = named(run, name).into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).unwrap_or_else(|| panic!("the recovery's {name} in the submit's trace"));
        assert_eq!(step["parent_span_id"], submit["span_id"], "{name} runs under the submit: {step}");
    }
    let calls = |op: u8| named(run, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| calls_op(sp, op)).count();
    assert_eq!((calls(0x20), calls(0x1C)), (0, 0), "no Build call and no claim call in the recovery's run");
    for name in ["rdm.node_admin.build.serve.via-attempt-run", "rdm.node_admin.build.update.via-reconcile", "rdm.node_admin.build.update.via-recovery"] {
        assert!(named(run, name).is_empty(), "the recovery is no Build: {name} in its run");
    }
}

/// The recovery of the exited birth `before`, as the records hold it (node-recovery.md, Q342): the
/// exited birth's record written deleted, and the investigation's `rdm.node.investigated` event for that
/// birth on the fabric's store root (build.md §5), whose `next` is the re-create. The record and the event.
async fn recovery_recorded(estate: &Estate, before: &Value) -> (Value, Value) {
    let node_id = s(&before["node_id"]);
    wait_for(&format!("{node_id}'s record deleted and its rdm.node.investigated event"), Duration::from_secs(30), || async {
        let record = estate.node_record(&node_id).filter(|r| r["record"]["status"] == "deleted")?;
        let event = build_log::node_investigated(estate, &node_id).into_iter().rev().find(|e| e["data"]["incarnation"] == before["incarnation_id"])?;
        Some((record, event))
    })
    .await
}

/// CONTRACT: a node whose mesh transport stops for good exits (reserved code 4, recorded in
/// its data dir). Its mesh-primary (node-recovery.md), not the fabric-primary, proves the exit:
/// it marks the node TRUE OFFLINE, asks the provider about the exact runtime, and on Exited runs the
/// recovery itself, its own workflow keyed on the exited birth (node-recovery.md "Execution", Q342):
/// node.delete of the exited instance (node.deleted from the exited verdict, once, named by the exact
/// birth), then node.create at the same (path, name) over the inherited data dir, and nothing after
/// it. Each step is recorded on the node record; no Build is written or called. The path comes back
/// with the SAME transport key and data dir and a NEW node.id and incarnation. No other node changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_stopped_exit_is_restarted_as_the_same_node() {
    let (estate, birth_build) =
        two_mesh_estate("a_transport_stopped_exit_is_restarted_as_the_same_node").await;
    // The node lives in the mesh that does NOT hold the fabric-primary: its own mesh-primary runs it.
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh = if primary["mesh"] == "mesh1" { "mesh2" } else { "mesh1" };
    let node = Estate::path(&format!("{mesh}.node"), "1");
    let node_path = &node;
    let mesh_primary = nodes.iter().find(|n| n["mesh"] == mesh && n["type"] == "node-admin" && n["is_primary"] == true).expect("the node's mesh-primary").clone();
    let before = estate.node_value(node_path).await;
    let data_dir = estate.data_dir(node_path).await;
    let others: Vec<(PathName, String)> = nodes.iter().filter(|n| n["name"] != json!(node_path)).map(|n| (location(n), s(&n["node_id"]))).collect();

    // The real transport-stop path: the testkit door marks the transport stopped; the process exits through it.
    let builds = estate.build_ids().await;
    let marked = estate.probe(&["stop-transport", "--target", &rdm_test::estate::path_target(node_path), "--key", "x"]);
    assert_eq!(marked["outcome"], "Reply", "{marked}");

    let after = until_node_where(&estate, node_path, &format!("{node_path:?} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_ne!(after["node_id"], before["node_id"], "recovery creates a new node.id");
    assert_ne!(after["incarnation_id"], before["incarnation_id"], "and a new incarnation");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same transport key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir(node_path).await, data_dir, "the same data dir");
    assert_ne!(after["deployment_id"], before["deployment_id"], "a new runtime");

    // The mesh-primary proved the exit and submitted the recovery, keyed on the exited birth.
    let recovery = recovery_span(&estate, node_path, &before["node_id"]).await;
    assert_eq!(
        s(&recovery["attributes"]["submitter"]),
        s(&mesh_primary["node_id"]),
        "the node's own mesh-primary submits: {recovery}"
    );
    assert_eq!(
        recovery["attributes"]["incarnation_id"], before["incarnation_id"],
        "keyed on the exact exited birth: {recovery}"
    );
    assert_eq!(
        s(&recovery["attributes"]["verdict"]),
        "exited",
        "on the verdict exited: {recovery}"
    );
    assert_eq!(s(&recovery["attributes"]["mesh"]), mesh, "the member's mesh: {recovery}");
    // The submit followed the recovery to its end and records it (never "submitted").
    assert_eq!(s(&recovery["attributes"]["outcome"]), "complete", "the recovery's end: {recovery}");
    assert_eq!(estate.build_ids().await, builds, "no Build was minted");
    // The records hold the recovery: the exited birth's record deleted, and the verdict's event.
    let (record, event) = recovery_recorded(&estate, &before).await;
    assert_eq!(event["data"]["verdict"], "exited", "the investigation's verdict: {event}");
    assert_eq!(record["record"]["node_id"], before["node_id"], "{record}");

    // Each process exports its own spans: the new node's boot span is read once the new node exported it.
    let spans = wait_for("the recovery's create, the exited birth's target and the new node's boot span are exported", SETTLE, || async {
        let spans = estate.spans();
        let run = in_trace(&spans, &recovery);
        (create_span(&run, NO_BUILD, NO_ATTEMPT, node_path).is_some() && !exited_birth_spans(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // The recovery's own run, one trace from the mesh-primary's submit: its delete and its create.
    let run = in_trace(&spans, &recovery);
    assert_mesh_primary_ran_the_recovery(&run, &mesh_primary, &before);
    // Delete, then create: the exited birth departs once, named by its exact birth, before the new node boots.
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact exited birth");
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).expect("the new node's boot span");
    assert!(start(deleted[0]) < start(boot), "the exited birth is deleted (targeted) before the new node joins");
    // node-recovery.md "Delete targets the old instance before the new join": the target is the
    // create's, run by the executor that launches the new node with the retained key, inside the
    // create's own operation and before that launch. A target run after the delete's Complete is left
    // undone when that executor is lost before it, and the next attempt's create launches untargeted.
    let create = create_span(&run, NO_BUILD, NO_ATTEMPT, node_path).expect("the recovery's create operation span");
    let targets = exited_birth_spans(&spans, &before);
    assert_eq!(targets.len(), 1, "the exited birth is targeted once: {targets:?}");
    assert!(start(create) <= start(targets[0]) && end(targets[0]) <= end(create), "the target runs inside the create operation: {} vs {create}", targets[0]);
    assert!(end(targets[0]) <= start(boot), "the target is done before the new node with the same key boots: {} vs {boot}", targets[0]);
    for step in ["StopNode", "AwaitNodeLeft"] {
        assert!(step_span(&run, NO_BUILD, NO_ATTEMPT, node_path, step).is_none(), "no RPC to the exited process: {step} ran");
    }
    assert!(!named(&spans, "rdm.mesh.node.delete.via-transport-stopped").is_empty(), "the exiting runtime's own span names the reason");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));

    // Nothing else moved.
    for (name, id) in others {
        assert_eq!(s(&estate.node_value(&name).await["node_id"]), id, "{name:?} is untouched");
    }
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md): every other unplanned exit is a recovery like the transport stop.
/// A node killed with SIGKILL leaves no recorded reason: its mesh-primary, which in a one-mesh
/// fabric is also the fabric-primary, marks it TRUE OFFLINE, proves the exit from the provider alone
/// (no exit code) and runs one recovery keyed on the exited birth, its own workflow (no Build written
/// or minted, Q342), two operations in order (node-recovery.md "Steps"):
/// node.delete of the exited instance (`node.delete:<path>`: no RPC to it, no stop frames,
/// node.deleted once named by its exact birth and before the new node boots), then node.create at
/// the same (path, name) over the inherited data dir (`node.create:<path>`), each as its own workflow's
/// pipeline and node operation span; no node.update follows (proposal node-properties-own-commands.md
/// §5 item 8).
/// The path comes back with the SAME transport key and data dir and a NEW node.id and incarnation;
/// no `<name>-old` at the unchanged path rename and no storage hand-over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn any_other_unplanned_exit_is_recovered_as_a_new_node_over_the_same_data_dir() {
    let (estate, birth_build) = estate("any_other_unplanned_exit_is_recovered_as_a_new_node_over_the_same_data_dir").await;
    let node = &Estate::path("mesh1.node", "3");
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh_primary = nodes.iter().find(|n| n["mesh"] == "mesh1" && n["type"] == "node-admin" && n["is_primary"] == true).expect("the mesh-primary").clone();
    assert_eq!(mesh_primary["node_id"], primary["node_id"], "one mesh: its primary holds the fabric-primary role too");
    let before = estate.node_value(node).await;
    let data_dir = estate.data_dir(node).await;
    let others: Vec<(PathName, String)> = nodes.iter().filter(|n| n["name"] != json!(node)).map(|n| (location(n), s(&n["node_id"]))).collect();
    let builds = estate.build_ids().await;
    estate.kill_node(node).await;

    let after = until_node_where(&estate, node, &format!("{node:?} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_ne!(after["incarnation_id"], before["incarnation_id"], "a new incarnation");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same transport key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir(node).await, data_dir, "the same data dir");

    let recovery = recovery_span(&estate, node, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), s(&mesh_primary["node_id"]), "{recovery}");
    assert_eq!(s(&recovery["attributes"]["mesh"]), "mesh1", "{recovery}");
    assert_eq!(recovery["attributes"]["incarnation_id"], before["incarnation_id"], "keyed on the exact exited birth: {recovery}");
    assert_eq!(s(&recovery["attributes"]["verdict"]), "exited", "on the verdict exited: {recovery}");
    assert_eq!(s(&recovery["attributes"]["outcome"]), "complete", "the recovery's end: {recovery}");
    let (_, event) = recovery_recorded(&estate, &before).await;
    assert_eq!(event["data"]["verdict"], "exited", "the investigation's verdict on the record: {event}");

    // Each process exports its own spans: the successor's boot span is read once the successor exported it.
    let spans = wait_for("the recovery's create, the exited birth's target and the successor's boot span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let run = in_trace(&spans, &recovery);
        (create_span(&run, NO_BUILD, NO_ATTEMPT, node).is_some() && !exited_birth_spans(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // The recovery's own run, in the submit's trace.
    let run = in_trace(&spans, &recovery);
    assert_mesh_primary_ran_the_recovery(&run, &mesh_primary, &before);
    let mut last = 0u64;
    for step in ["NodeDeleting", "NodeDeleted", "RemoveTopologyMembership", "DeployRuntime", "WaitForNodeReady"] {
        let sp = step_span(&run, NO_BUILD, NO_ATTEMPT, node, step).unwrap_or_else(|| panic!("the recovery's {step} step span"));
        assert!(start(sp) >= last, "{step} runs after the step before it");
        last = start(sp);
    }
    for step in ["StopNode", "AwaitNodeLeft"] {
        assert!(step_span(&run, NO_BUILD, NO_ATTEMPT, node, step).is_none(), "no RPC to the exited process: {step} ran");
    }
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact exited birth");
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).expect("the new node's boot span");
    assert!(start(deleted[0]) < start(boot), "the exited birth is deleted (targeted) before the new node joins");
    // node-recovery.md "Delete targets the old instance before the new join": the target is the
    // create's, run by the executor that launches the new node with the retained key, inside the
    // create's own operation and before that launch. A target run after the delete's Complete is left
    // undone when that executor is lost before it, and the next attempt's create launches untargeted.
    let create = create_span(&run, NO_BUILD, NO_ATTEMPT, node).expect("the recovery's create operation span");
    let targets = exited_birth_spans(&spans, &before);
    assert_eq!(targets.len(), 1, "the exited birth is targeted once: {targets:?}");
    assert!(start(create) <= start(targets[0]) && end(targets[0]) <= end(create), "the target runs inside the create operation: {} vs {create}", targets[0]);
    assert!(end(targets[0]) <= start(boot), "the target is done before the new node with the same key boots: {} vs {boot}", targets[0]);
    // Each operation is its workflow: its node operation span and its own pipeline, never a recovery
    // pipeline of its own.
    for (op, pipeline) in [("delete", "delete"), ("create", "create")] {
        let name = format!("rdm.node_admin.node.{op}.via-build");
        let at = named(&run, &name).into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == NO_BUILD && s(&sp["attributes"]["attempt"]) == NO_ATTEMPT && at_location(sp, node)).unwrap_or_else(|| panic!("the recovery's {name} span"));
        assert_eq!(at["attributes"]["node_id"], before["node_id"], "{name} names the exited birth it acts for: {at}");
        assert!(
            named(&run, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == NO_BUILD && s(&sp["attributes"]["attempt"]) == NO_ATTEMPT && at_location(sp, node) && sp["attributes"]["pipeline"] == pipeline),
            "the {pipeline} workflow's pipeline span"
        );
    }
    assert!(!named(&spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| sp["attributes"]["pipeline"] == "recover"), "no recovery pipeline of its own");
    assert!(
        !named(&run, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| at_location(sp, node) && sp["attributes"]["pipeline"] == "update"),
        "no node.update after the create: the create's row seeded what the recovery keeps"
    );
    assert!(nodes_named_old(&estate.node_values().await).is_empty(), "no name-old replacement");
    assert_eq!(estate.build_ids().await, builds, "no Build was minted");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    for (name, id) in others {
        assert_eq!(s(&estate.node_value(&name).await["node_id"]), id, "{name:?} is untouched");
    }
    estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md "Failure and receipts": the mesh-primary runs its failed recovery again;
/// keyed on the exited birth, never a second recovery; D48): a member's data dir loses its node-key and
/// the member is killed. Its recovery's node.delete completes (the exited birth departs, node.deleted
/// once) and its node.create refuses by name at PrepareStorage, so the recovery ends failed. The key is put back. The mesh-primary submits the same exited birth
/// again after its backoff, though no view holds that birth any longer: the recovery owed in its
/// memory runs again (D48), the birth already deleted, and the path comes back as a new node over the
/// same data dir and endpoint key. Must NOT happen: the failed recovery left for good (nothing at the
/// path), a second node.deleted of the exited birth, a Build minted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_whose_create_failed_after_its_delete_is_submitted_again_and_completes() {
    let (estate, birth_build) = estate("a_recovery_whose_create_failed_after_its_delete_is_submitted_again_and_completes").await;
    let node = &Estate::path("mesh1.node", "2");
    let before = estate.node_value(node).await;
    let data_dir = estate.data_dir(node).await;
    let key_path = Path::new(&data_dir).join("node-key");
    let key = std::fs::read(&key_path).unwrap_or_else(|e| panic!("{node:?}'s node-key in {data_dir}: {e}"));
    std::fs::remove_file(&key_path).unwrap();
    let builds = estate.build_ids().await;
    estate.kill_node(node).await;

    let submits = |spans: &[Value]| -> Vec<Value> { named(spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let failed = wait_for("the recovery's create fails at PrepareStorage", SETTLE, || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("failed")) }).await;
    let outcome = s(&failed["attributes"]["outcome"]);
    assert!(outcome.contains("PrepareStorage") && outcome.contains("node-key is missing"), "the create refused the dir with no key by name: {failed}");
    std::fs::write(&key_path, &key).unwrap();

    let after = until_node_where(&estate, node, &format!("{node:?} ready as a new node after its recovery failed"), Duration::from_secs(60), |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir(node).await, data_dir, "the same data dir");
    let done = wait_for("the completed submit of the exited birth is exported", Duration::from_secs(60), || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("complete")) }).await;
    assert_eq!(done["attributes"]["incarnation_id"], before["incarnation_id"], "keyed on the exact exited birth: {done}");
    assert_eq!(s(&done["attributes"]["outcome"]), "complete", "{done}");
    assert_eq!(s(&done["attributes"]["again"]), "true", "the submit runs again the recovery that failed, owed in its mesh-primary's memory (D48): {done}");
    assert_eq!(s(&done["attributes"]["mesh"]), "mesh1", "{done}");
    assert_eq!(estate.build_ids().await, builds, "no Build was minted");
    let spans = estate.spans();
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "one node.deleted of the exited birth, never again: {deleted:?}");
    estate.record_trace_url(done["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// The recovery's `TerminateRuntime` of the birth at `node` in the submit's own trace (node-recovery.md
/// sequence step 3), completed, and the runtime inspection it read at its instant (the only inspection in
/// that trace), once both are exported.
async fn recovery_terminate(estate: &Estate, recovery: &Value, node: &PathName, node_id: &Value) -> (Value, Value) {
    wait_for(&format!("the recovery's TerminateRuntime of {node:?} and its inspection are exported"), SETTLE, || async {
        let spans = estate.spans();
        let run = in_trace(&spans, recovery);
        let terminate = step_span(&run, NO_BUILD, NO_ATTEMPT, node, "TerminateRuntime").filter(|sp| sp["attributes"]["outcome"] == "complete").cloned()?;
        let inspected = named(&run, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().find(|sp| sp["attributes"]["node_id"] == *node_id).cloned()?;
        Some((terminate, inspected))
    })
    .await
}

/// The target spans of the exited birth `old` (node-recovery.md "Delete targets the old instance
/// before the new join"): its resolver entry and the direct paths of its endpoint key deleted.
fn exited_birth_spans<'a>(spans: &'a [Value], old: &Value) -> Vec<&'a Value> {
    named(spans, "rdm.node_admin.node.update.via-exit-paths-removed").into_iter().filter(|sp| sp["attributes"]["node_id"] == old["node_id"] && sp["attributes"]["kind"] == "recovery").collect()
}

/// Whether the boot span of `node`'s birth is exported (its own process exports it, on its own cadence).
fn booted(spans: &[Value], node: &Value) -> bool {
    named(spans, "rdm.mesh.node.create.via-deployment").into_iter().any(|sp| sp["attributes"]["node_id"] == node["node_id"])
}

/// The nodes whose (path, name) is a deleted `<name>-old` at the unchanged path.
fn nodes_named_old(nodes: &[Value]) -> Vec<String> {
    nodes.iter().map(|n| s(&n["name"])).filter(|n| n.ends_with(".old")).collect()
}

/// The reconcile span of the fabric lane's `attempt` of `build`. It closes after the attempt's last
/// step, so it is exported after that step's span.
fn reconcile_span<'a>(spans: &'a [Value], build: &str, attempt: &str) -> Option<&'a Value> {
    lane_reconcile_span(spans, build, "", attempt)
}

/// The reconcile span of `attempt` of `build` in `lane` ("" the fabric lane, else that mesh's drift
/// lane: R-BI7 numbers each lane's attempts on its own, so an attempt is `(build, lane, attempt)`).
fn lane_reconcile_span<'a>(spans: &'a [Value], build: &str, lane: &str, attempt: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["lane"]) == lane && s(&sp["attributes"]["attempt"]) == attempt)
}

/// The `deployment.update.via-step` span of `step` for `node` in `attempt` of `build`.
/// The node.create operation span of `node` at `attempt` of `build`: exported once the create, a
/// recovery's last operation, ended.
fn create_span<'a>(spans: &'a [Value], build: &str, attempt: &str, node: &PathName) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.node.create.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && at_location(sp, node))
}

fn step_span<'a>(spans: &'a [Value], build: &str, attempt: &str, node: &PathName, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && at_location(sp, node) && s(&sp["attributes"]["step"]) == step)
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().expect("a span start")
}

fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().expect("a span end")
}

fn events(frames: &[Frame]) -> Vec<NodeEvent> {
    frames.iter().filter_map(|f| if let Frame::Event(e) = f { Some(*e) } else { None }).collect()
}

fn alive(pid: u64) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

/// Every frame of `stream`, in order, up to and including the terminal one.
async fn frames(mut stream: rdm_client::WorkflowStream) -> Vec<Frame> {
    let mut out = Vec::new();
    while let Some(f) = stream.next().await {
        let f = f.unwrap_or_else(|e| panic!("the reply stream broke: {e}"));
        let terminal = f.is_terminal();
        out.push(f);
        if terminal {
            break;
        }
    }
    out
}

/// CONTRACT (lifecycles/node-replace.md: "Replace is Rafka's steps, submitted as one Build. RDM
/// executes its primitives and records their receipts; it has no replace opcode"; steps 1-3: "A node is
/// renamed only while it is live: its connections deleted and its mesh left first ...; it is stopped
/// after. A stopped node is never renamed"; build.md §1): the caller submits the replace of
/// `mesh1.node.1` as ONE Build over the `node` object, and RDM runs its steps in order, each as its own
/// op's operation: node[1].connections.delete `{ All }`, node[1].mesh.leave, node[1].update
/// (path `mesh1.node`, name `1-old`), node[1].stop at `<name>-old`, node.create(`mesh1.node.1`), node[1].delete at `<name>-old`.
/// The stream is `Started`, every step's events in that order, `Complete`; the Build's operations are
/// exactly those six keys in that order, and no replace operation exists. The predecessor leaves its
/// mesh before its rename (the mesh-primary carries `LeaveMesh` and holds its `MeshLeft`), takes
/// `<name>-old` live (the rename's `via-writer` span says `connections=deleted mesh=left`, applied by the
/// node, which publishes nothing off its mesh), and is stopped as `<name>-old` after (its NodeStopped once,
/// its stop span naming `<name>-old`). The successor is a re-birth at `mesh1.node.1`, the name the rename
/// released (a new node.id, transport key and data dir), its row a successor's seeded from the
/// predecessor's row, and its join admitted under `mesh1.node.1`: it is never born at any other name.
/// The predecessor is deleted at `<name>-old` on the provider's exited verdict (NodeDeleted once, its Deleted row
/// at `<name>-old`, its data dir kept). RDM moves no data. At the end `mesh1.node.1` is the successor, ready,
/// and nothing is listed at `<name>-old`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replace_runs_as_one_build_of_node_primitives_in_order() {
    let (mut estate, _birth) = estate("a_replace_runs_as_one_build_of_node_primitives_in_order").await;
    let path = PathName::new("mesh1.node", "1").unwrap();
    let old = path.renamed().unwrap();
    assert_eq!(old.path, path.path, "replacement preserves the path");
    assert_eq!(old.name, "1-old", "only the predecessor name changes");
    let nodes = objects(&estate).await;
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let before = mesh.iter().find(|n| n.location() == path).cloned().expect("node.get lists the node");
    let pid = estate.pid_of(&path).await;
    let old_dir = std::path::PathBuf::from(estate.data_dir(&path).await);
    let segment = old_dir.join("app-data").join("seg-0");
    std::fs::create_dir_all(segment.parent().unwrap()).unwrap();
    std::fs::write(&segment, b"records").unwrap();

    let stream = nodes.steps(replace_steps(&path, &before.mesh, &before.r#type).expect("valid replacement fixture name")).await.expect("the Build was accepted");
    let accepted = stream.accepted().cloned().expect("the call opened a Build");
    let got = frames(stream).await;
    assert!(matches!(got.first(), Some(Frame::Started { .. })), "{got:?}");
    use NodeEvent::*;
    let want = vec![ConnectionsDeleted, MeshLeft, Updated, ConnectionsDeleted, Stopped, Joined, Started, Created, Deleted];
    assert_eq!(events(&got), want, "every step's events, in step order: {got:?}");
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");

    // The Build's operations: the six steps, each its own op's operation, in order; no replace.
    let build = accepted.build_id.to_string();
    let b = build_log::build_value(&estate, &build).await;
    let steps = b["steps"].as_array().cloned().unwrap_or_default();
    let mut operations: Vec<rdm_core::OperationId> = Vec::new();
    for r in steps.iter().filter(|r| r["attempt"].as_u64().unwrap_or(0) >= accepted.attempt as u64) {
        let op: rdm_core::OperationId = serde_json::from_value(r["operation"].clone()).expect("structured operation");
        if !operations.contains(&op) {
            operations.push(op);
        }
    }
    let keys = [
        rdm_core::OperationId::node("node.connection.delete", &before.node_id).at_step(Some(1)),
        rdm_core::OperationId::node("node.mesh.leave", &before.node_id).at_step(Some(2)),
        rdm_core::OperationId::node("node.update", &before.node_id).at_step(Some(3)),
        rdm_core::OperationId::node("node.stop", &before.node_id).at_step(Some(4)),
        rdm_core::OperationId::location("node.create", &path).at_step(Some(5)),
        rdm_core::OperationId::node("node.delete", &before.node_id).at_step(Some(6)),
    ];
    assert_eq!(operations, keys.to_vec(), "the Build's operations, in order: {b:#}");

    // The end state: the successor holds the name, ready; nothing at `<name>-old`.
    let after = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let successor = after.iter().find(|n| n.location() == path).cloned().expect("node.get lists the successor at the name");
    assert!(successor.status == NodeStatus::Ready && !successor.stopped, "{successor:?}");
    assert_ne!(successor.node_id, before.node_id, "a new node.id");
    assert_ne!(successor.endpoint_id, before.endpoint_id, "a new transport key");
    assert!(!after.iter().any(|n| n.location() == path.renamed().unwrap()), "nothing is listed under name-old: {after:#?}");
    assert!(!alive(pid), "the predecessor's process {pid} exited at its delete");
    assert_eq!(std::fs::read(&segment).unwrap(), b"records", "node.delete keeps storage, and RDM moved no data: {segment:?}");
    let new_dir = std::path::PathBuf::from(estate.data_dir(&path).await);
    assert_ne!(new_dir, old_dir, "the successor has its own data dir");
    assert!(!new_dir.join("app-data").exists(), "RDM handed nothing to the successor: {new_dir:?}");

    estate.stop().await;
    let spans = estate.spans();
    let of = |name: &str, id: &rdm_core::NodeId| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == id.to_string()).cloned().collect() };
    // The predecessor: its connections deleted and its mesh left, then renamed live, then stopped.
    let left = named(&spans, "rdm.node_admin.node.remove.via-mesh-leave").into_iter().filter(|sp| s(&sp["attributes"]["subject_id"]) == before.node_id.to_string()).cloned().collect::<Vec<_>>();
    assert_eq!(left.len(), 1, "the mesh-primary carried the predecessor's leave once: {left:#?}");
    assert_eq!((s(&left[0]["attributes"]["subject"]), s(&left[0]["attributes"]["outcome"])), (before.node_id.to_string(), "mesh-left".to_string()), "{:#}", left[0]);
    let renames1 = of("rdm.node_admin.name.update.via-writer", &before.node_id);
    assert_eq!(renames1.len(), 1, "{renames1:#?}");
    let r1 = &renames1[0];
    assert_eq!(
        (s(&r1["attributes"]["from_path"]), s(&r1["attributes"]["from_name"]), s(&r1["attributes"]["to_path"]), s(&r1["attributes"]["to_name"]), s(&r1["attributes"]["connections"]), s(&r1["attributes"]["mesh"]), s(&r1["attributes"]["answered"]), s(&r1["attributes"]["outcome"])),
        (path.path.clone(), path.name.clone(), old.path.clone(), old.name.clone(), "deleted".into(), "left".into(), "applied".into(), "name-updated".into()),
        "{r1:#}"
    );
    assert!(end(&left[0]) <= start(r1), "the predecessor leaves its mesh before it gives up its name");
    let applied: Vec<Value> = named(&spans, "rdm.node_admin.node.update.via-apply-node-name").into_iter().filter(|sp| sp["attributes"]["node_id"] == before.node_id.as_str() && sp["attributes"]["to_path"] == old.path && sp["attributes"]["to_name"] == old.name).cloned().collect();
    assert_eq!(applied.iter().map(|sp| (s(&sp["attributes"]["outcome"]), s(&sp["attributes"]["digest"]))).collect::<Vec<_>>(), vec![("applied".to_string(), "off-mesh".to_string())], "the predecessor takes name-old off its mesh and publishes nothing: {applied:#?}");
    let stopped1 = of("rdm.node_admin.node.update.via-node-stopped", &before.node_id);
    assert_eq!(stopped1.len(), 1, "the predecessor's NodeStopped once: {stopped1:#?}");
    assert!(end(r1) <= start(&stopped1[0]), "the predecessor gives up its name before it is stopped");
    let stop_node: Vec<String> = named(&spans, "rdm.node_admin.status.update.via-stop-node").into_iter().filter(|sp| sp["attributes"]["operation"] == "node.stop" && sp["attributes"]["node"] == before.node_id.as_str() && sp["attributes"]["build_id"] == build).map(|sp| s(&sp["attributes"]["node"])).collect();
    assert_eq!(stop_node, vec![before.node_id.to_string()], "the predecessor stops as the same node after its rename");
    // The successor: a re-birth at the name, its row seeded from the predecessor's row, admitted there.
    let created = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == successor.node_id.to_string() && sp["attributes"]["moment"] == "create").cloned().expect("the successor's row");
    assert_eq!((s(&created["attributes"]["subject_path"]), s(&created["attributes"]["subject_name"]), s(&created["attributes"]["seeded_from"])), (path.path.clone(), path.name.clone(), before.node_id.to_string()), "the successor writes its row at the released name, seeded from the predecessor's row: {created:#}");
    assert!(end(&stopped1[0]) <= start(&created), "the successor is created once the predecessor is stopped as name-old");
    let join = of("rdm.node_admin.node.update.via-join", &successor.node_id).into_iter().filter(|sp| s(&sp["attributes"]["outcome"]) == "installed").collect::<Vec<_>>();
    assert!(join.iter().all(|j| j["attributes"]["admitted_path"] == path.path && j["attributes"]["admitted_name"] == path.name) && !join.is_empty(), "the successor's join is admitted at {path:?} and no other name: {join:#?}");
    assert!(of("rdm.node_admin.name.update.via-writer", &successor.node_id).is_empty(), "the successor is never renamed");
    // The predecessor's delete at `<name>-old`: the provider's exited verdict, NodeDeleted once, the Deleted row.
    let deleted = of("rdm.node_admin.node.delete.via-node-deleted", &before.node_id);
    assert_eq!(deleted.len(), 1, "NodeDeleted once for the predecessor: {deleted:#?}");
    assert_eq!(s(&deleted[0]["attributes"]["operation"]), "node.delete");
    let rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["node_id"] == before.node_id.as_str() && sp["attributes"]["subject_path"] == old.path && sp["attributes"]["subject_name"] == old.name).collect();
    assert!(rows.len() == 1 && s(&rows[0]["attributes"]["outcome"]) == "row-deleted" && s(&rows[0]["attributes"]["node_id"]) == before.node_id.to_string(), "one Deleted row under name-old for the predecessor: {rows:#?}");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "nothing of the Build is drift");
    let create = named(&spans, "rdm.node_admin.node.create.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build && sp["attributes"]["node_path"] == path.path && sp["attributes"]["node_name"] == path.name).cloned().expect("the successor's create operation span");
    estate.artifact("steps.json", &json!({"operations": keys, "leave": left[0], "rename_old": r1, "stopped_old": stopped1[0], "created": created, "node_deleted": deleted[0], "deleted_row": rows[0]}));
    estate.record_trace_url(create["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (build.md §4.4 gate 4: "A node stands at the path | the fabric-primary's view by name ... |
/// `unknown-node`"; Q223: one gate, one refusal, whatever the Build): steps whose first step stops the
/// live `mesh1.node.1` and whose second starts `mesh1.node.9`, a path no birth holds and no earlier step
/// makes, are refused at acceptance `unknown-node`, the detail naming step 2 and `mesh1.node.9`, under
/// the fabric-primary's `rdm.node_admin.build.reject.via-unknown-node` span naming both. Nothing
/// executes: the accepted Build opens no attempt and records no receipt, and `mesh1.node.1` is still
/// the same birth in the same process, ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn steps_naming_an_unknown_node_are_refused_unknown_node_naming_the_step_and_execute_nothing() {
    let (mut estate, birth) = estate("steps_naming_an_unknown_node_are_refused_unknown_node_naming_the_step_and_execute_nothing").await;
    let path = PathName::new("mesh1.node", "1").unwrap();
    let unknown = PathName::new("mesh1.node", "9").unwrap();
    let unknown_detail = format!("path `{}`, name `{}`", unknown.path, unknown.name);
    let nodes = objects(&estate).await;
    let before = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().find(|n| n.location() == path).expect("node.get lists the node");
    let pid = estate.pid_of(&path).await;
    let b0 = build_log::build_value(&estate, &birth).await;

    let steps = vec![Step::Stop { path: path.path.clone(), name: path.name.clone()}, Step::Start { path: unknown.path.clone(), name: unknown.name.clone()}];
    let (reason, detail) = match nodes.steps(steps).await {
        Err(CallEnd::Rejected { reason, detail }) => (reason, detail),
        Err(other) => panic!("steps naming an unknown node end Rejected: {other:?}"),
        Ok(_) => panic!("steps naming an unknown node were accepted"),
    };
    assert_eq!(reason, "unknown-node", "the one name build.md §4.4 gate 4 gives: {detail}");
    assert!(detail.contains("step 2 ") && detail.contains(&unknown_detail), "the detail names the step and the target: {detail}");

    let b1 = build_log::build_value(&estate, &birth).await;
    assert_eq!((&b1["attempt"], b1["steps"].as_array().map(Vec::len)), (&b0["attempt"], b0["steps"].as_array().map(Vec::len)), "the refusal opened no attempt and recorded no receipt: {b0:#} / {b1:#}");
    let after = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().find(|n| n.location() == path).expect("node.get lists the node");
    assert!(after.status == NodeStatus::Ready && !after.stopped, "step 1 never ran: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation_id), (&before.node_id, &before.incarnation_id), "the same birth");
    assert_eq!(estate.pid_of(&path).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let refused: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-unknown-node").into_iter().filter(|sp| s(&sp["attributes"]["detail"]).contains("step 2 ") && s(&sp["attributes"]["detail"]).contains(&unknown_detail)).collect();
    assert_eq!(refused.len(), 1, "the fabric-primary's refusal span, its detail naming the step and the target: {refused:#?}");
    assert!(named(&spans, "rdm.node_admin.status.update.via-stop-node").into_iter().all(|sp| sp["attributes"]["node"] != before.node_id.as_str() || sp["attributes"]["operation"] != "node.stop"), "no stop of the selected node ran");
    estate.artifact("refused.json", &json!({"reason": reason, "detail": detail, "span": refused[0]}));
}

/// CONTRACT (node-replace.md "Properties": "Tags remain in the subject's own property record; a
/// current-pair lookup selects the successor's ID, not the predecessor's properties"; query.md:
/// "Historical records ... remain queryable by their own IDs and matching properties"): mesh2's
/// primary tells `mesh2.node.1` a config and a tag,
/// then the node is replaced up to the successor's create (no delete of the predecessor). The
/// predecessor leaves its mesh, takes `<name>-old` live and is stopped there; the successor's create at
/// `<name>`, the name it released, puts its row with its Build entry's config and tags (Q305,
/// node-replace.md "Properties"): this Build of steps takes its entry from its topology, which names
/// none of the config and the tag the calls wrote, so the successor's row carries neither, never the
/// retained `.old` row's (`seeded_from` the predecessor all the same). The create then marks the
/// retained row `inherited_by` the successor. The successor's join under `<name>` hands it no config;
/// node.get by the tag names the retained predecessor alone, which stays at `<name>-old` with its tag
/// in its own property record, never the successor. Must NOT happen: the dead predecessor's config or
/// tags copied onto the successor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_nodes_successor_takes_its_build_entrys_config_and_tags_at_its_create_and_the_retained_row_leaves_the_tags_index() {
    let (estate, _build) = two_mesh_estate("a_replaced_nodes_successor_takes_its_build_entrys_config_and_tags_at_its_create_and_the_retained_row_leaves_the_tags_index").await;
    let views = estate.node_views().await;
    let fabric_primary = views.iter().find(|n| n.is_fabric_primary).expect("fabric primary");
    let primary = views.iter().find(|n| n.mesh != fabric_primary.mesh && n.r#type == "node-admin" && n.is_primary).expect("peer mesh primary");
    let member = views.iter().find(|n| n.mesh == primary.mesh && n.r#type == "node" && !n.is_primary).expect("ordinary RPC member");
    let node_path = &member.location();
    let old_name = node_path.renamed().unwrap();
    let mesh_primary = &primary.location();
    let fp = serde_json::to_value(fabric_primary).unwrap();
    let before = serde_json::to_value(member).unwrap();
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let node_id: rdm_core::NodeId = s(&before["node_id"]).parse().unwrap();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node_id.clone()), node_id, incarnation: rdm_core::IncarnationId::parse(&s(&before["incarnation_id"])).unwrap() };
    // node-config-update.md step 1: the caller sends it to the fabric-primary, which sends it down.
    let (fp_client, fp_id) = caller_of(&fp).await;
    let reply = rdm_client::NodeRpc::new(&fp_client)
        .config_update(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, told.clone(), &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.config.update of {node_path:?} at the fabric-primary: {e:?}"));
    assert_eq!(reply.name(), "config-updated", "the mesh-primary wrote the row and the node applied it: {reply:?}");
    let told_json = json!({"tier": "gold"});
    wait_for(&format!("{node_path:?} carries its config"), Duration::from_secs(30), || async { (estate.node_value(node_path).await["config"] == told_json).then_some(()) }).await;
    let labels: std::collections::BTreeSet<String> = ["rabbit".to_string()].into();
    // Commands go down: the caller sends it to the fabric-primary, which sends it to the mesh-primary (Q203).
    let tagged = rdm_client::NodeRpc::new(&fp_client)
        .tags_create(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, &labels, &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.tag.create of {node_path:?} at the fabric-primary: {e:?}"));
    assert_eq!(tagged.name(), "tags-created", "the mesh-primary wrote the label's row: {tagged:?}");
    wait_for(&format!("{node_path:?} carries its tag"), Duration::from_secs(30), || async { (estate.node_view(node_path).await.tags == labels).then_some(()) }).await;

    let create_steps: Vec<Step> = replace_steps(node_path, &s(&before["mesh"]), before["type"].as_str().unwrap()).expect("valid replacement fixture name").into_iter().take(5).collect();
    let got = frames(estate.nodes_obj().await.steps(create_steps).await.expect("the Build up to the successor's create was accepted")).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    let after = estate.until_node(node_path, "the successor is ready at the original location", SETTLE, |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != member.node_id).map(|n| serde_json::to_value(n).unwrap())).await;
    let spans = wait_for("the successor's row, the retained row's mark and the successor's join under the name are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let joined = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().any(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["admitted_path"] == node_path.path && sp["attributes"]["admitted_name"] == node_path.name);
        let marked = named(&spans, "rdm.node_admin.node.update.via-inherited-row").into_iter().any(|sp| sp["attributes"]["node_id"] == before["node_id"]);
        (joined && marked).then_some(spans)
    })
    .await;
    // Step 3: the successor's create at the released name writes its entry's properties (none here), then marks the retained row.
    let created = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["moment"] == "create").expect("the successor's row").clone();
    let a = &created["attributes"];
    assert_eq!(
        (s(&a["node_path"]), s(&a["node_name"]), s(&a["subject_path"]), s(&a["subject_name"]), s(&a["outcome"]), s(&a["seeded_from"]), s(&a["keys"]), s(&a["tags"])),
        (mesh_primary.path.clone(), mesh_primary.name.clone(), node_path.path.clone(), node_path.name.clone(), "row-seeded".to_string(), s(&before["node_id"]), "0".to_string(), "".to_string()),
        "the mesh-primary puts the successor at {node_path:?} with its entry's config and tags, none, never the retained row's: {created}"
    );
    assert!(named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().all(|sp| sp["attributes"]["node_id"] != after["node_id"]), "the successor is never renamed");
    let mark = named(&spans, "rdm.node_admin.node.update.via-inherited-row").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).expect("the retained row's mark").clone();
    let m = &mark["attributes"];
    assert_eq!((s(&m["original_path"]), s(&m["original_name"]), s(&m["inherited_by"]), s(&m["outcome"])), (node_path.path.clone(), node_path.name.clone(), s(&after["node_id"]), "inherited".to_string()), "the retained row says who inherited it (its tag stays in its own property record, read below): {mark}");
    assert_eq!(mark["parent_span_id"], created["span_id"], "the mark is the create's last write: {mark} under {created}");
    // The successor's join under the name is installed (node-start.md step 2); the answer carries no config.
    let joined = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["admitted_path"] == node_path.path && sp["attributes"]["admitted_name"] == node_path.name).expect("the successor's join under the name").clone();
    assert_eq!(s(&joined["attributes"]["outcome"]), "installed".to_string(), "the successor's join under {node_path:?} is installed: {joined}");
    let carried = estate.node_value(node_path).await;
    assert_eq!((carried["node_id"].clone(), carried["config"].clone()), (after["node_id"].clone(), json!({})), "the successor carries its entry's config, not the predecessor's {told_json}");
    assert!(estate.node_view(node_path).await.tags.is_empty(), "the successor's row carries none of the predecessor's tags");
    let selected: Vec<String> = estate
        .nodes_obj()
        .await
        .get(&rdm_client::Filter::new().eq("tags", labels.iter().cloned()))
        .await
        .unwrap_or_else(|e| panic!("node.get [tags: rabbit]: {e:?}"))
        .into_iter()
        .map(|n| n.node_id.to_string())
        .collect();
    assert_eq!(selected, vec![s(&before["node_id"])], "a read by the tag names the retained predecessor, whose own record keeps it, and never the successor, whose entry named no tag: {selected:?}");
    let retained = estate.node_values().await.into_iter().find(|n| n["node_id"] == before["node_id"]).unwrap_or_else(|| panic!("the predecessor is retained at {old_name:?}"));
    assert_eq!(retained["name"], json!(old_name), "predecessor keeps its path and changes only its name");
    let retained_record = rdm_admin::store_log::read_record(&estate.fabric_store_root().join(rdm_admin::store_root::NODES_FILE), "node-properties", Some(member.node_id.as_str())).expect("durable retained predecessor properties");
    assert_eq!(retained_record["record"]["tags"], json!(labels), "the retained row keeps its tags after leaving the active index");
    assert_eq!(retained_record["record"]["config"], told_json, "the retained row keeps its own config");
    assert_eq!(retained["node_id"], before["node_id"], "the retained {old_name:?} is the predecessor: {retained}");
    eprintln!("trace {}: successor={} predecessor={} path={:?} name={:?}; inherited_by={}; trace {}: join", s(&created["trace_id"]), s(&after["node_id"]), s(&before["node_id"]), node_path.path, node_path.name, s(&m["inherited_by"]), s(&joined["trace_id"]));
    estate.shutdown().await;
}

/// CONTRACT (lifecycles/node-replace.md "Rollback": "While node[1] is stopped as `<name>-old` and its
/// data remains available: 1. node[2]: node.connection.delete `{ scope: All }`, node.mesh.leave, then
/// node[2].delete releases `<name>` after exact runtime exited verdict. 2. node[1].start rejoins the same
/// stopped process with AfterStop under `<name>-old` ... 3. node[1]: node.connection.delete `{ scope:
/// All }`, node.mesh.leave, then node.name.update to `<name>` ..., then node[1].stop and node[1].start
/// under `<name>`. Its ready ends the rollback"): `mesh1.node.1` is replaced up to the
/// successor's create (the predecessor stopped at `.old`), then the rollback runs as one Build of nine
/// steps, each its own op's operation in order. The successor at `mesh1.node.1` has its connections
/// deleted, leaves its mesh and is deleted on its exited verdict; the predecessor starts as
/// path `mesh1.node`, name `1-old` (the same process), has its connections deleted, leaves its mesh, takes
/// `mesh1.node.1` live, and is stopped and started under it. At the end `mesh1.node.1` is the predecessor,
/// the same node.id, incarnation and process, ready; nothing is listed at `.old`, and the
/// successor's process exited. Both of the predecessor's renames are of a node off its mesh
/// (`connections=deleted mesh=left`, applied), and no rename of a stopped node is decided.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replace_rollback_ends_with_the_predecessor_ready_under_its_name() {
    let (mut estate, _birth) = estate("a_replace_rollback_ends_with_the_predecessor_ready_under_its_name").await;
    let path = PathName::new("mesh1.node", "1").unwrap();
    let old = path.renamed().unwrap();
    assert_eq!(old.path, path.path);
    let nodes = objects(&estate).await;
    let before = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().find(|n| n.location() == path).expect("node.get lists the node");
    let pid = estate.pid_of(&path).await;

    let forward: Vec<Step> = replace_steps(&path, &before.mesh, &before.r#type).expect("valid replacement fixture name").into_iter().take(5).collect();
    let got = frames(nodes.steps(forward).await.expect("the replace up to the successor's create was accepted")).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    let successor = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().find(|n| n.location() == path).expect("the successor at the name");
    assert_ne!(successor.node_id, before.node_id, "the successor is a re-birth");
    let successor_pid = estate.pid_of(&path).await;
    assert_ne!(successor_pid, pid, "the successor runs its own process");

    let stream = nodes.steps(rollback_steps(&path).expect("valid replacement fixture name")).await.expect("the rollback steps were accepted");
    let accepted = stream.accepted().cloned().expect("the call opened a Build");
    let got = frames(stream).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    let build = accepted.build_id.to_string();
    let b = build_log::build_value(&estate, &build).await;
    let mut operations: Vec<rdm_core::OperationId> = Vec::new();
    for r in b["steps"].as_array().cloned().unwrap_or_default().iter().filter(|r| r["attempt"].as_u64().unwrap_or(0) >= accepted.attempt as u64) {
        let op: rdm_core::OperationId = serde_json::from_value(r["operation"].clone()).expect("structured operation");
        if !operations.contains(&op) {
            operations.push(op);
        }
    }
    let keys: Vec<_> = [
        ("node.connection.delete", &successor.node_id),
        ("node.mesh.leave", &successor.node_id),
        ("node.delete", &successor.node_id),
        ("node.start", &before.node_id),
        ("node.connection.delete", &before.node_id),
        ("node.mesh.leave", &before.node_id),
        ("node.update", &before.node_id),
        ("node.stop", &before.node_id),
        ("node.start", &before.node_id),
    ].into_iter().enumerate().map(|(index, (action, id))| rdm_core::OperationId::node(action, id).at_step(Some(index as u32 + 1))).collect();
    assert_eq!(operations, keys.to_vec(), "the rollback's operations, in order: {b:#}");

    let after = estate.until_node(&path, "the predecessor is ready under its original name", SETTLE, |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id == before.node_id).map(|n| serde_json::to_value(n).unwrap())).await;
    assert_eq!(s(&after["incarnation_id"]), before.incarnation_id.as_ref().unwrap().to_string(), "the same birth: {after}");
    assert_eq!(estate.pid_of(&path).await, pid, "the same process");
    assert!(estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").iter().all(|n| n.location() != path.renamed().unwrap()), "nothing is listed under name-old");
    assert!(!alive(successor_pid), "the successor's process {successor_pid} exited at its delete");

    estate.stop().await;
    let spans = estate.spans();
    let renames: Vec<Value> = named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string()).cloned().collect();
    assert_eq!(
        renames.iter().map(|sp| (s(&sp["attributes"]["to_path"]), s(&sp["attributes"]["to_name"]), s(&sp["attributes"]["connections"]), s(&sp["attributes"]["mesh"]), s(&sp["attributes"]["outcome"]))).collect::<Vec<_>>(),
        vec![(old.path.clone(), old.name.clone(), "deleted".to_string(), "left".to_string(), "name-updated".to_string()), (path.path.clone(), path.name.clone(), "deleted".to_string(), "left".to_string(), "name-updated".to_string())],
        "the predecessor is renamed twice, each time live and off its mesh: {renames:#?}"
    );
    assert!(named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().all(|sp| s(&sp["attributes"]["traffic"]) != "Stopped"), "no rename of a stopped node is decided");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed").map(|sp| PathName::new(s(&sp["attributes"]["admitted_path"]), s(&sp["attributes"]["admitted_name"])).unwrap()).collect::<Vec<_>>();
    assert_eq!(join, vec![path.clone(), old.clone(), path.clone()], "the predecessor joined at its birth, then as name-old at its start, then at {path:?} at its last start: {join:?}");
    let deleted = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == successor.node_id.to_string()).count();
    assert_eq!(deleted, 1, "NodeDeleted once for the successor");
    estate.artifact("rollback.json", &json!({"operations": keys, "renames": renames}));
    estate.record_trace_url(renames[1]["trace_id"].as_str().unwrap_or(""));
}

/// The steps of `node.delete` of a stopped birth at its `.old` path (node-delete.md: its stop steps
/// are already done): the removal is announced, the provider's exact Exited is the proof, then the
/// executor publishes NodeDeleted and the operation's Complete names the birth. Storage is not
/// released.
const DELETE_STOPPED_ORDER: [&str; 4] = ["NodeDeleting", "TerminateRuntime", "NodeDeleted", "Complete"];

/// CONTRACT: the Build's `node[1].delete` of the stopped predecessor at `<name>-old` at the unchanged path
/// (lifecycles/node-replace.md step 2; node-delete.md) as its own call: `mesh1.node.1` has its
/// connections deleted, leaves its mesh, is renamed path `mesh1.node`, name `1-old` live by node.name.update at its
/// mesh-primary and is stopped there, its successor created at `1` (the replace steps keeping the
/// predecessor for a rollback, node-replace.md steps 1-3), then `build.create` of a
/// RemoveNode change at `<name>-old` at the unchanged path is the next attempt of the accepted Build (reason delete,
/// action delete targeted to the exact stopped birth, no Build minted). One operation
/// `node.delete:<path>.old` (node.delete's one key) asks the provider to end the stopped runtime
/// and inspects it; only the provider's Exited is the proof, after which the executor publishes
/// NodeDeleted for the exact birth and the mesh's primary, which executes it, writes the Deleted
/// row at `<name>-old` at the unchanged path, its record kept (R13). The process is gone; its data dir stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_the_stopped_predecessor_proves_its_exit_publishes_node_deleted_and_writes_its_deleted_row() {
    let (estate, birth_build) = estate("deleting_the_stopped_predecessor_proves_its_exit_publishes_node_deleted_and_writes_its_deleted_row").await;
    let nodes = objects(&estate).await;
    let stopped = leave_rename_and_stop(&estate, &nodes, &Estate::path("mesh1.node", "1")).await;
    delete_stopped_and_assert(&estate, &birth_build, &stopped).await;
    estate.shutdown().await;
}

/// CONTRACT: the same delete when the fabric-primary is in another mesh than the node (R13: the
/// mesh-primary of a node's mesh is the only writer of that mesh's rows). The fabric-primary in mesh1
/// admits `node.delete:<path>.old` of a mesh2 node from its view, which reads the stopped birth at the
/// row's name by its node id (R12), targeted to that exact birth; mesh2's primary executes the delete
/// and writes the Deleted row at `<name>-old` at the unchanged path; the fabric-primary writes no row of mesh2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_peer_mesh_stopped_predecessor_is_admitted_from_the_fabric_primary_held_record() {
    let (estate, birth_build) = two_mesh_estate("deleting_a_peer_mesh_stopped_predecessor_is_admitted_from_the_fabric_primary_held_record").await;
    let nodes = objects(&estate).await;
    let stopped = leave_rename_and_stop(&estate, &nodes, &Estate::path("mesh2.node", "1")).await;
    let (renamer, fp) = (estate.node_view_by_id(&stopped.renamed_by).await, estate.node_view_by_id(&stopped.fabric_primary).await);
    assert!(renamer.mesh == "mesh2" && renamer.r#type == "node-admin", "mesh2's primary renamed it: {renamer:?}");
    assert!(fp.mesh == "mesh1" && fp.r#type == "node-admin", "the fabric-primary is in mesh1: {fp:?}");
    let executor = delete_stopped_and_assert(&estate, &birth_build, &stopped).await;
    assert_eq!(executor, stopped.renamed_by, "mesh2's primary, the writer of its rows, executes the delete");
    estate.shutdown().await;
}

/// CONTRACT: the same delete when the stopped birth's mesh changed its primary between the rename and
/// the delete (node-delete.md: "Stop and inspect the exact runtime through provider control"). mesh2's
/// primary is stopped, its roles moving to mesh2's other admin; that admin did not rename the birth.
/// It resolves the exact stopped birth the action is targeted to from what it holds of that birth (its
/// topology view's stopped digest, else its durable runtime row of it), ends it with the provider's
/// Exited proof and publishes NodeDeleted for it, and, as mesh2's primary, writes the Deleted row at
/// `<name>-old` at the unchanged path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_stopped_predecessor_after_its_mesh_primary_changed_is_executed_by_the_new_primary_on_the_exact_birth() {
    let (estate, birth_build) = fabric_of("deleting_a_stopped_predecessor_after_its_mesh_primary_changed_is_executed_by_the_new_primary_on_the_exact_birth", &[("mesh1", 1, 2), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let nodes = objects(&estate).await;
    let stopped_birth = leave_rename_and_stop(&estate, &nodes, &Estate::path("mesh2.node", "1")).await;
    let first_primary = estate.node_view_by_id(&stopped_birth.renamed_by).await;
    assert!(first_primary.mesh == "mesh2" && first_primary.r#type == "node-admin", "mesh2's primary renamed it: {first_primary:?}");
    // mesh2's primary changes: its stop hands its roles to mesh2's other admin, which executes that
    // stop (executor_for: the mesh primary never executes its own).
    let stopped = frames(nodes.stop(&first_primary.location()).await.expect("the stop of mesh2's primary was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let new_primary = wait_for("mesh2's other admin holds the primary role", Duration::from_secs(30), || async {
        estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.ok()?.into_iter().find(|n| n.r#type == "node-admin" && n.is_primary && n.node_id != first_primary.node_id).map(|n| n.node_id)
    })
    .await;
    let executor = delete_stopped_and_assert(&estate, &birth_build, &stopped_birth).await;
    assert_eq!(executor, new_primary, "mesh2's new primary, which did not rename the birth, executes the delete");
    estate.shutdown().await;
}

/// CONTRACT (node-admin-lifecycle.md §6.1: "A replace of a birth absent from the view runs node.delete
/// against its exact runtime, rebuilt from the Build action and the data-root runtime record. It emits
/// `rdm.node_admin.node.update.via-rebuilt-birth` and refuses by name when no record names the birth.
/// Completion requires the NodeDeleted receipt of that exact birth"): mesh2's only admin runs the
/// predecessor's half of the replace of `mesh2.node.1` (stopped at `.old`); mesh2 then takes a second
/// admin, born after the stop, so no digest of the stopped birth ever reached its view; mesh2's first
/// admin is stopped and its roles move to the second. The Build's `node[1].delete` at `.old` is executed
/// by that admin, whose view does not hold the birth: it rebuilds the birth from the Build action's
/// exact birth and the runtime record in the birth's data dir under the data root
/// (`via-rebuilt-birth`, `outcome = rebuilt`, naming the birth, its data dir and the record's
/// deployment), then deletes that exact runtime with everything node.delete owes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_a_birth_absent_from_the_executors_view_deletes_the_runtime_rebuilt_from_the_build_action_and_the_data_root_record() {
    const SHAPE: [(&str, u32, u32); 2] = [("mesh1", 1, 2), ("mesh2", 1, 2)];
    const GROWN: [(&str, u32, u32); 2] = [("mesh1", 1, 2), ("mesh2", 2, 2)];
    let (estate, _birth_build) = fabric_of("replacing_a_birth_absent_from_the_executors_view_deletes_the_runtime_rebuilt_from_the_build_action_and_the_data_root_record", &SHAPE, Duration::from_secs(90)).await;
    let nodes = objects(&estate).await;
    let stopped_birth = leave_rename_and_stop(&estate, &nodes, &Estate::path("mesh2.node", "1")).await;
    let (first_admin, second_admin) = (Estate::path("mesh2.node-admin", "1"), Estate::path("mesh2.node-admin", "2"));
    assert_eq!(estate.node_view_by_id(&stopped_birth.renamed_by).await.location(), first_admin, "mesh2's only admin renamed and stopped it");
    // mesh2's second admin is born after the stop: no digest of the stopped birth reaches its view.
    let grown = estate.reconcile_shape("fabric1", &GROWN).await;
    // The view holds the stopped `.old` beside the grown node types, so it is read by name.
    wait_for("mesh2.node-admin.2 is ready", Duration::from_secs(90), || async {
        estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.ok()?.into_iter().find(|n| n.location() == second_admin && n.status == NodeStatus::Ready).map(|_| ())
    })
    .await;
    let nodes = objects(&estate).await;
    let stopped = frames(nodes.stop(&first_admin).await.expect("the stop of mesh2's first admin was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    wait_for("mesh2.node-admin.2 holds mesh2's role", Duration::from_secs(30), || async {
        estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.ok()?.into_iter().find(|n| n.location() == second_admin && n.is_primary).map(|_| ())
    })
    .await;
    let record = match rdm_core::RuntimeFact::read_record(&stopped_birth.old_dir) {
        Some(Ok(f)) => f,
        other => panic!("the stopped birth's data dir {:?} holds its runtime record: {other:?}", stopped_birth.old_dir),
    };
    let executor = delete_stopped_and_assert(&estate, &grown.build_id.to_string(), &stopped_birth).await;
    assert_eq!(estate.node_view_by_id(&executor).await.location(), second_admin, "mesh2's new primary, born after the stop, executes the delete");

    let spans = estate.spans();
    let old = stopped_birth.node.renamed().expect("the reserved name fits");
    let rebuilt: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-rebuilt-birth").into_iter().filter(|sp| at_location(sp, &old)).collect();
    assert_eq!(rebuilt.len(), 1, "the executor rebuilt the birth its view does not hold, once: {rebuilt:#?}");
    let a = &rebuilt[0]["attributes"];
    assert_eq!(
        (s(&a["executor_node_id"]), s(&a["node_id"]), s(&a["incarnation_id"]), s(&a["outcome"])),
        (executor.to_string(), s(&stopped_birth.before["node_id"]), s(&stopped_birth.before["incarnation_id"]), "rebuilt".to_string()),
        "the exact birth the Build action names, rebuilt by the executor: {:#}",
        rebuilt[0]
    );
    assert_eq!(s(&a["data_dir"]), stopped_birth.old_dir.display().to_string(), "the birth's data dir under the data root: {:#}", rebuilt[0]);
    assert_eq!(s(&a["deployment_id"]), record.deployment_id, "the runtime its data-root record names: {:#}", rebuilt[0]);
    let op = named(&spans, "rdm.node_admin.node.delete.via-build").into_iter().find(|sp| at_location(sp, &old)).cloned().expect("the delete operation span");
    assert!(descends_from(&spans, rebuilt[0], &op), "the rebuild runs inside the delete operation: {:#} / {op:#}", rebuilt[0]);
    estate.artifact("rebuilt-birth.json", &json!({"rebuilt": rebuilt[0], "record": record.deployment_id}));
    estate.shutdown().await;
}

/// The stopped birth a Build's rename left at `<name>-old` at the unchanged path: who it was, where, who renamed it.
struct StoppedBirth {
    node: PathName,
    before: Value,
    pid: u64,
    old_dir: std::path::PathBuf,
    /// The mesh-primary that served the node.name.update (the writer of the node's row), by node id.
    renamed_by: NodeId,
    /// The fabric-primary when the node was renamed, by node id.
    fabric_primary: NodeId,
}

fn dispatch<'a>(spans: &'a [Value], build: &str, attempt: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt)
}

/// The `node` objects over the estate's fabric-primary, every member's address learned.
async fn objects(estate: &Estate) -> Nodes {
    let _ = estate.try_node_views().await;
    estate.nodes_obj().await
}

/// `node` off its mesh, renamed `<name>-old` at its unchanged path live and stopped there, and its
/// successor created at `<name>`, by the replace steps that keep the predecessor stopped for a rollback
/// (lifecycles/node-replace.md steps 1-3): the stopped predecessor a Build's `node[1].delete` deletes.
/// The successor's create in the same Build is what makes the renamed birth a predecessor; a name
/// ending `-old` does not.
async fn leave_rename_and_stop(estate: &Estate, nodes: &Nodes, node: &PathName) -> StoppedBirth {
    let before = estate.node_value(node).await;
    let pid = estate.pid_of(node).await;
    let old_dir = std::path::PathBuf::from(estate.data_dir(node).await);
    // The fabric-primary as fabric.get names it, and the node's mesh-primary as mesh.get names it.
    let fabric_primary = rdm_test::typed::the_fabric(&estate.fabrics_obj().await).await.expect("fabric.get").fabric_primary.expect("a fabric-primary");
    let primary = rdm_test::typed::one_mesh(&estate.meshes_obj().await, &s(&before["mesh"])).await.expect("mesh.get").expect("the admin holds the node's mesh").primary_admin.expect("the node's mesh-primary");
    let r#type: &str = before["type"].as_str().expect("a node type");
    let got = frames(nodes.steps(replace_keeping_predecessor(node, &s(&before["mesh"]), r#type).expect("valid replacement fixture name")).await.expect("the replace keeping its predecessor was accepted")).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    let old = node.renamed().expect("the reserved name fits");
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", [&s(&before["mesh"])])).await.expect("node.get [mesh]");
    let stopped = mesh.iter().find(|n| n.location() == old && n.stopped).cloned().unwrap_or_else(|| panic!("node.get lists the stopped {node:?} stopped at {old:?} once its steps completed: {mesh:#?}"));
    assert_eq!(stopped.node_id.to_string(), s(&before["node_id"]), "the stopped birth at {old:?} is {node:?}'s");
    assert!(alive(pid), "the stopped {node:?} ({pid}) is alive before its delete");
    StoppedBirth { node: node.clone(), before, pid, old_dir, renamed_by: primary, fabric_primary }
}

/// `build.create` of a RemoveNode change at `<name>-old` at the unchanged path of `stopped`, and everything
/// node.delete owes (node-delete.md). Returns the node id of the admin that executed the delete.
async fn delete_stopped_and_assert(estate: &Estate, birth_build: &str, stopped: &StoppedBirth) -> NodeId {
    let StoppedBirth { node, before, pid, old_dir, .. } = stopped;
    let old = node.renamed().expect("the reserved name fits");
    let node_id: NodeId = serde_json::from_value(before["node_id"].clone()).expect("a node id");
    let delete = estate.submit_change(Change::RemoveNode { node: old.clone() }).await;
    assert_eq!(delete.build_id().as_str(), birth_build, "no Build is minted");
    let attempt_no = u64::from(delete.attempt());
    let opened = build_log::build_fact(&estate, birth_build, "the delete attempt is the Build's current attempt", Duration::from_secs(30), |p| serde_json::to_value(p).expect("a Build view serializes")["reason"] == "delete")
        .await
        .map(|p| serde_json::to_value(p).expect("a Build view serializes"))
        .unwrap_or_else(|end| panic!("{end}"));
    assert_eq!((s(&opened["action"]["action"]).as_str(), &opened["action"]["path"]), ("delete", &json!(old)), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "targeted to the exact stopped birth: {opened}");
    estate.follow(delete).await;
    let attempt = attempt_no.to_string();
    let deletes = OperationSubject::Node(node_id.clone());
    let spans = wait_for("the delete's last step, its reconcile span, its dispatch and the Deleted row are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let row = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().any(|sp| rdm_test::estate::names_location(sp, "subject", &old));
        (step_span(&spans, birth_build, &attempt, &old, "Complete").is_some() && reconcile_span(&spans, birth_build, &attempt).is_some() && dispatch(&spans, birth_build, &attempt).is_some() && row).then_some(spans)
    })
    .await;
    let reconcile = reconcile_span(&spans, birth_build, &attempt).unwrap();
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("{:?}", vec![rdm_core::OperationId::node("node.delete", &node_id)]), "one delete operation of the stopped birth: {reconcile}");
    let mut last = 0u64;
    for step in DELETE_STOPPED_ORDER {
        let sp = step_span(&spans, birth_build, &attempt, &old, step).unwrap_or_else(|| panic!("step {step} of the delete ran"));
        assert!(start(sp) >= last, "step {step} starts before the step it follows: {sp}");
        last = start(sp);
    }
    for step in ["DeleteConnections", "StopNode", "ReleaseStorage"] {
        assert!(step_span(&spans, birth_build, &attempt, &old, step).is_none(), "the stopped birth's delete runs no {step}");
    }
    // The provider's Exited is the proof, and it precedes the NodeDeleted the executor publishes.
    let terminate = step_span(&spans, birth_build, &attempt, &old, "TerminateRuntime").unwrap();
    assert_eq!(s(&terminate["attributes"]["outcome"]), "complete", "{terminate}");
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "NodeDeleted once for the stopped birth: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact birth");
    assert!(rdm_test::estate::span_operation(deleted[0], "node.delete", &deletes), "the delete of the exact node: {}", deleted[0]);
    assert!(end(terminate) <= start(deleted[0]), "NodeDeleted follows the exited verdict: {terminate} vs {}", deleted[0]);
    // The Deleted row at `<name>-old` at the unchanged path, written by the mesh's primary that executes the delete (R13).
    let dispatched = dispatch(&spans, birth_build, &attempt).unwrap();
    let executor: NodeId = serde_json::from_value(reconcile["attributes"]["executor_node_id"].clone()).expect("the executor's node id");
    let executor_at = estate.node_view_by_id(&executor).await.location();
    let rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| rdm_test::estate::names_location(sp, "subject", &old)).collect();
    assert_eq!(rows.len(), 1, "one Deleted row at {old:?}: {rows:?}");
    assert!(at_location(rows[0], &executor_at) && rows[0]["attributes"]["node_id"] == before["node_id"] && rows[0]["attributes"]["outcome"] == "row-deleted", "the mesh's primary executing the delete writes the Deleted row of the exact node: {}", rows[0]);
    estate.artifact("deleted-row.json", &json!({"row": rows[0], "node_deleted": deleted[0], "terminate": terminate, "dispatch": dispatched}));
    let op = named(&spans, "rdm.node_admin.node.delete.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && at_location(sp, &old)).expect("the delete operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    // The stopped process is gone and its data dir stays.
    assert!(!alive(*pid), "the stopped birth's process {pid} exited");
    assert!(old_dir.exists(), "node.delete releases node.id and name, not storage: {old_dir:?}");
    executor
}

/// CONTRACT (lifecycles/node-replace.md, the same steps for a node-admin): the replace steps of a
/// node-admin that holds no role runs as one Build of node primitives: the predecessor has its
/// connections deleted, leaves its mesh and the backbone, is renamed `<name>-old` at the unchanged path live and then stopped
/// (its NodeStopped drops it from every view), the successor is a re-birth at the path, and the
/// predecessor is deleted. The successor admin's own view holds itself at the path, ready, under its
/// new NodeId, and no admin's view holds two live births at the path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_node_admin_sees_itself_and_no_view_holds_two_live_births() {
    let (mut estate, _birth) = estate("a_replaced_node_admin_sees_itself_and_no_view_holds_two_live_births").await;
    let nodes = estate.node_values().await;
    let admins: Vec<&Value> = nodes.iter().filter(|n| n["type"] == "node-admin").collect();
    assert_eq!(admins.len(), 2, "{nodes:#?}");
    let before = admins.iter().find(|n| n["is_primary"] != true && n["is_fabric_primary"] != true).map(|n| (*n).clone()).expect("an admin that holds no primary role");
    let stays = serde_json::from_value::<PathName>(admins.iter().find(|n| n["node_id"] != before["node_id"]).unwrap()["name"].clone()).expect("structured location");
    assert_eq!(estate.entry().await, stays, "the admin that stays is the entry the typed objects ask");
    let node: PathName = serde_json::from_value(before["name"].clone()).expect("structured location");
    let node_path = node.clone();
    let nodes_obj = objects(&estate).await;
    let got = frames(nodes_obj.steps(replace_steps(&node_path, &s(&before["mesh"]), before["type"].as_str().unwrap()).expect("valid replacement fixture name")).await.expect("the Build was accepted")).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    at_most_one_live_birth(&estate, &[stays.clone()], &node).await;
    let after = estate.until_node(&node, "the successor admin is ready", SETTLE, |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id.as_str() != before["node_id"].as_str().unwrap()).map(|n| serde_json::to_value(n).unwrap())).await;
    // The successor admin's own view: itself at the path, ready, under its new NodeId.
    let me = estate
        .until_view_at(&node_path, &format!("the successor {node:?} sees itself"), Duration::from_secs(30), |views| {
            let at: Vec<Value> = Estate::values(views).into_iter().filter(|n| n["name"] == json!(node) && live(n)).collect();
            assert!(at.len() <= 1, "the successor's view holds two live births at {node:?}: {at:#?}");
            at.into_iter().find(|n| n["node_id"] == after["node_id"] && ready(n))
        })
        .await;
    assert_eq!(me["incarnation_id"], after["incarnation_id"], "the successor's view holds its own birth: {me}");
    estate.stop().await;
    let spans = estate.spans();
    let stopped = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().expect("the predecessor's NodeStopped");
    let rename = named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().expect("the predecessor's rename");
    assert!(end(&rename) <= start(&stopped), "the predecessor gives up its name before it is stopped");
    assert_eq!((s(&rename["attributes"]["mesh"]), s(&rename["attributes"]["outcome"])), ("left".to_string(), "name-updated".to_string()), "the predecessor is renamed off its mesh: {rename:#}");
    estate.record_trace_url(rename["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-recovery.md step 3: "Any other verdict (`running`, ...): the recovery below, at once.
/// Nothing holds"; sequence step 3: "When the verdict was `running`, the first step is
/// `TerminateRuntime { node_id, incarnation, handle }`"): a member is hung (every task but the leader
/// held by a tracer: the provider reads it running), and its mesh-primary's investigation ends in the
/// verdict `running`: the recovery is submitted on it at once, its node.delete's `TerminateRuntime`
/// reads the runtime running at its instant and terminates it, and the member is re-created as a new
/// birth at the same `(path, name)`. Must NOT happen: the hung member held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_member_whose_runtime_runs_is_terminated_and_recovered_at_once() {
    let (mut estate, birth_build) = born_at(
        Estate::bootstrap_with_env(owner("a_hung_member_whose_runtime_runs_is_terminated_and_recovered_at_once"), "fabric1", "mesh1", &[]).await,
        &[("mesh1", 1, 2), ("mesh2", 1, 2)],
        Duration::from_secs(90),
    )
    .await;
    let (node, mesh_primary, _) = member_and_primaries(&estate.node_values().await);
    let before = estate.node_value(&node).await;
    let mesh = s(&before["mesh"]);
    let rt = exact_runtime(&estate, &node).await;
    let builds = estate.build_ids().await;
    let held = rt.hang().expect("the exact runtime is hung");
    assert!(!held.held.is_empty(), "every task but the leader is held: the provider sees it running");

    let after = until_node_where(&estate, &node, &format!("{node:?} recovered as a new birth"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    let recovery = recovery_span(&estate, &node, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), s(&mesh_primary["node_id"]), "the member's mesh-primary submits the recovery: {recovery}");
    assert_eq!(s(&recovery["attributes"]["verdict"]), "running", "on the investigation's verdict: {recovery}");
    assert_eq!(s(&recovery["attributes"]["outcome"]), "complete", "{recovery}");
    assert_eq!(s(&recovery["attributes"]["mesh"]), mesh, "{recovery}");
    recovery_recorded(&estate, &before).await;
    let (terminate, inspected) = recovery_terminate(&estate, &recovery, &node, &before["node_id"]).await;
    assert_eq!(s(&inspected["attributes"]["verdict"]), "running", "the TerminateRuntime read the runtime running at its instant and terminated it: {inspected}");
    assert_mesh_primary_ran_the_recovery(&in_trace(&estate.spans(), &recovery), &mesh_primary, &before);
    assert_eq!(estate.build_ids().await, builds, "no Build was minted");
    eprintln!(
        "member cell: {node:?} (birth {}) recovered by {}: trace {} | TerminateRuntime {} | new birth {}",
        s(&before["node_id"]),
        s(&mesh_primary["node_id"]),
        s(&recovery["trace_id"]),
        s(&terminate["span_id"]),
        s(&after["node_id"]),
    );
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.stop().await;
}

/// CONTRACT (node-recovery.md: "The mesh-primary is the reconciler for its own members, whatever their
/// type"; step 2: "Half a floor after the mark it runs node investigation on the exact birth"; step 3:
/// "Any other verdict (`running`, ...): the recovery below, at once. Nothing holds, nothing is inspected
/// again"; node-investigation.md Caller: "its mesh-primary for a member of any type"; Q181, Luke
/// 2026-10-26: "a node cut off should have been replaced"): mesh1 holds two node-admins, mesh1.node-admin.1
/// its primary and the fabric-primary, and mesh1.node-admin.2 a member. mesh1.node-admin.2 is hung (every task
/// but the leader held by a tracer: the provider reads it running). Its mesh-primary mesh1.node-admin.1 runs
/// the one investigation on it, half a floor after the mark, and on the verdict `running` submits its
/// recovery at once: mesh1.node-admin.2 is re-created as a new birth at its (path, name). Must NOT happen: the
/// mesh-primary passing over its own node-admin member, a hold-down opened on it, or a second ladder
/// before the recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_node_admin_member_is_investigated_by_its_mesh_primary_and_re_created_at_once() {
    let (mut estate, _birth_build) = fabric_of("a_hung_node_admin_member_is_investigated_by_its_mesh_primary_and_re_created_at_once", &[("mesh1", 2, 1)], Duration::from_secs(60)).await;
    let node = &Estate::path("mesh1.node-admin", "2");
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh_primary = primary.clone();
    assert_eq!(location(&mesh_primary), Estate::path("mesh1.node-admin", "1"), "the bootstrap admin is mesh1's primary and the fabric-primary");
    let before = estate.node_value(node).await;
    let rt = exact_runtime(&estate, node).await;
    let held = rt.hang().expect("the exact runtime is hung");
    assert!(!held.held.is_empty(), "every task but the leader is held: the provider sees it running");

    let after = until_new_birth(&estate, node, &before, SETTLE).await;
    let recovery = recovery_span(&estate, node, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), s(&mesh_primary["node_id"]), "the node-admin's own mesh-primary submits its recovery: {recovery}");
    assert_eq!(s(&recovery["attributes"]["verdict"]), "running", "on the investigation's verdict: {recovery}");
    let spans = estate.spans();
    let held_down: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-offline-hold-down-opened").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert!(held_down.is_empty(), "nothing holds between the ladder nobody answered and the recovery: {held_down:?}");
    let ladders: Vec<&Value> = named(&spans, "rdm.node.tickle.returns-unknown.via-unreached").into_iter().filter(|sp| sp["attributes"]["caller"] == mesh_primary["node_id"] && sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(ladders.len(), 1, "the inspection, then one ladder (Q273): {ladders:?}");
    assert_eq!(s(&ladders[0]["attributes"]["next"]), "verdict", "the inspection read it running, so the ladder nobody answered ends in the verdict (node-investigation.md step 4, Q273): {}", ladders[0]);
    eprintln!("node-admin member cell: {node:?} (birth {}) recovered by {}: trace {} | new birth {}", s(&before["node_id"]), s(&mesh_primary["node_id"]), s(&recovery["trace_id"]), s(&after["node_id"]));
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 2, 1)], Duration::from_secs(30)).await;
    estate.stop().await;
}

/// CONTRACT (node-recovery.md "Execution": "A lost mesh-primary is investigated and recovered by the
/// successor its election names, as any member of its mesh, its own workflow; the fabric-primary
/// investigates and recovers it only when no successor takes the role"; step 2: the fabric-primary runs
/// it only when the mesh's aggregate stays unheard past the staleness floor; elections §4.1: the
/// successor's first act is to publish its mesh's aggregate on the backbone naming itself mesh-primary,
/// then it investigates the lost birth as any member; sequence step 2: `TerminateRuntime` first when the
/// verdict was `running`; Q335): `mesh2` has two node-admins (`mesh2.node-admin.1` primary, `mesh2.node-admin.2`
/// standby). `mesh2.node-admin.1` is hung (every task but the leader held by a tracer: the provider reads it
/// running). The election names `mesh2.node-admin.2`, which publishes mesh2's aggregate on the backbone
/// naming itself, then runs its ladder on the hung birth, writes the `running` verdict as the birth's
/// `rdm.node.investigated` event and recovers it as its own workflow: its node.delete terminates the
/// exact runtime first, and `mesh2.node-admin.1` is re-created as a new birth with no primary role. No Build
/// is minted. Must NOT happen: the fabric-primary investigating or recovering the hung birth (mesh2's
/// aggregate is heard from the successor), the successor's ladder before its first aggregate, the hung
/// mesh-primary held, the re-created birth primary, or a second recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_mesh_primary_is_investigated_and_recovered_by_its_successor_after_it_publishes_its_aggregate() {
    let (mut estate, _birth_build) = born_at(
        Estate::bootstrap_with_env(owner("a_hung_mesh_primary_is_investigated_and_recovered_by_its_successor_after_it_publishes_its_aggregate"), "fabric1", "mesh1", &[]).await,
        &[("mesh1", 1, 1), ("mesh2", 2, 1)],
        Duration::from_secs(90),
    )
    .await;
    let nodes = estate.node_values().await;
    let fp_row = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let fp = s(&fp_row["node_id"]);
    let mp = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).cloned().expect("mesh2's primary");
    let mp_name = location(&mp);
    let standby_row = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["type"] == "node-admin" && n["node_id"] != mp["node_id"]).expect("mesh2's standby admin").clone();
    let standby = location(&standby_row);
    let successor = s(&standby_row["node_id"]);
    assert_ne!(fp_row["mesh"], "mesh2", "the fabric-primary is in mesh1: {fp_row}");

    let rt = exact_runtime(&estate, &mp_name).await;
    let builds = estate.build_ids().await;
    let held = rt.hang().expect("mesh2's primary runtime is hung");
    assert!(!held.held.is_empty(), "every task of mesh2's primary but the leader is held: the provider sees it running");

    let after = until_node_where(&estate, &mp_name, &format!("{mp_name:?} recovered as a new birth"), SETTLE, |n| ready(n) && n["node_id"] != mp["node_id"]).await;
    let standby_after = until_node_where(&estate, &standby, &format!("{standby:?} primary as mesh2's primary"), Duration::from_secs(60), |n| n["is_primary"] == json!(true)).await;
    assert_eq!(standby_after["is_primary"], json!(true), "{standby:?} holds mesh2's role");
    assert_eq!(after["is_primary"], json!(false), "the re-created birth starts with no primary role: {after}");

    let recovery = recovery_span(&estate, &mp_name, &mp["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), successor, "the successor the election named runs the lost mesh-primary's recovery: {recovery}");
    assert_eq!(s(&recovery["attributes"]["route"]), "node-recovery", "as any member's: {recovery}");
    assert_eq!(s(&recovery["attributes"]["outcome"]), "complete", "{recovery}");
    let (terminate, inspected) = recovery_terminate(&estate, &recovery, &mp_name, &mp["node_id"]).await;
    assert_eq!(s(&inspected["attributes"]["verdict"]), "running", "the TerminateRuntime read the hung runtime running and terminated it: {inspected}");
    assert_eq!(estate.build_ids().await, builds, "no Build was minted");
    let spans = estate.spans();
    // The successor's verdict is the birth's event; the fabric-primary looks at nothing.
    let investigated: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-on-node-investigated").into_iter().filter(|sp| sp["attributes"]["node_id"] == mp["node_id"]).collect();
    assert_eq!(investigated.len(), 1, "one rdm.node.investigated event of the hung birth: {investigated:#?}");
    assert!(investigated[0]["parent_span_id"] == recovery["span_id"], "written by the successor's recovery {recovery}: {}", investigated[0]);
    let fp_looks: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-investigation").into_iter().filter(|sp| sp["attributes"]["suspect_node_id"] == mp["node_id"] && at_location(sp, &location(&fp_row))).collect();
    assert!(fp_looks.is_empty(), "the fabric-primary does not investigate a mesh-primary whose mesh's aggregate is heard: {fp_looks:#?}");
    // The successor's first act: its aggregate on the backbone, before its ladder on the lost birth.
    let start = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let first_aggregate = named(&spans, "rdm.mesh.backbone.update.via-primary-aggregate")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["mesh"]) == "mesh2" && s(&sp["attributes"]["publisher"]).contains(&successor))
        .min_by_key(|sp| start(sp))
        .cloned()
        .expect("the successor publishes mesh2's aggregate on the backbone naming itself");
    let ladders: Vec<&Value> = named(&spans, "rdm.node.tickle.returns-unknown.via-unreached").into_iter().filter(|sp| s(&sp["attributes"]["caller"]) == successor && sp["attributes"]["node_id"] == mp["node_id"]).collect();
    assert!(!ladders.is_empty() && ladders.iter().all(|sp| start(sp) > start(&first_aggregate)), "the successor's ladder on the lost birth follows its first aggregate {first_aggregate}: {ladders:#?}");
    let role_span = named(&spans, "rdm.mesh.election.resolve.via-mesh-primary")
        .into_iter()
        .find(|sp| sp["attributes"]["winner_node_id"] == standby_row["node_id"])
        .cloned()
        .expect("the election names the standby");
    // The new birth at mesh2.node-admin.1 is created by mesh2's mesh-primary, the successor. No second
    // recovery of the hung birth.
    let created_rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-row")
        .into_iter()
        .filter(|sp| rdm_test::estate::span_operation(sp, "node.create", &OperationSubject::Location(mp_name.clone())) && sp["attributes"]["node_id"] == after["node_id"] && matches!(sp["attributes"]["outcome"].as_str(), Some("row-seeded" | "row-written")))
        .collect();
    assert!(!created_rows.is_empty() && created_rows.iter().all(|sp| at_location(sp, &standby)), "the new birth at {mp_name:?} is created by {standby:?}, mesh2's primary, never by {fp}: {created_rows:#?}");
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == mp["node_id"]).collect();
    assert!(recoveries.iter().all(|sp| s(&sp["attributes"]["submitter"]) == successor), "the hung birth is recovered by {standby:?} alone: {recoveries:#?}");
    eprintln!(
        "mesh-primary loss cell: {mp_name:?} (birth {}) recovered by {successor}: trace {} | first aggregate trace {} | TerminateRuntime {} | {standby:?} primary (span {}) | new birth {}",
        s(&mp["node_id"]),
        s(&recovery["trace_id"]),
        s(&first_aggregate["trace_id"]),
        s(&terminate["span_id"]),
        s(&role_span["span_id"]),
        s(&after["node_id"]),
    );
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 1), ("mesh2", 2, 1)], Duration::from_secs(30)).await;
    estate.stop().await;
}

/// CONTRACT (Q122; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" steps 1-4;
/// node-recovery.md step 3): `mesh1.node-admin.1`, the fabric-primary and mesh1's primary, is hung (every task
/// but the leader held by a tracer: the operating system shows it running). Unheard past the staleness
/// floor, `mesh1.node-admin.2`, the lowest Ready node-admin of its mesh, takes no primary role: it
/// runs fabric recovery. Node investigation reads the hung birth `running`; the recovery's first step is
/// `TerminateRuntime`, then `mesh1.node-admin.1` is re-created as a new birth at the same (path, name), and the
/// workflow restores that node as mesh-primary and fabric-primary. At that take the re-created
/// fabric-primary reads the Build records (Q227: "the exception of FP, who will need to read the last
/// build record to determine if anything is in flight"; build.md §5 "new fabric-primary -> reads the
/// Builds newest first"): its process's one read of `builds.jsonl` is the take's, after it became
/// fabric-primary, naming the current Build. Must NOT happen: the hung birth held,
/// `mesh1.node-admin.2` named mesh-primary or fabric-primary, the hung birth recovered by anyone else or twice,
/// or the re-created admin reading the Build records before it is fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_fabric_primary_is_terminated_and_re_created_by_fabric_recovery_from_its_own_mesh() {
    let (mut estate, _birth_build) = born_at(
        Estate::bootstrap_with_env(owner("a_hung_fabric_primary_is_terminated_and_re_created_by_fabric_recovery_from_its_own_mesh"), "fabric1", "mesh1", &[]).await,
        &[("mesh1", 2, 1), ("mesh2", 1, 1)],
        Duration::from_secs(90),
    )
    .await;
    let nodes = estate.node_values().await;
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    let fp_name = location(&fp);
    assert_eq!((fp["mesh"].as_str(), fp["is_primary"].as_bool()), (Some("mesh1"), Some(true)), "the Day-0 admin is mesh-primary and fabric-primary: {fp}");
    let standby_row = nodes.iter().find(|n| n["mesh"] == "mesh1" && n["type"] == "node-admin" && n["node_id"] != fp["node_id"]).expect("mesh1's other admin").clone();
    let (standby, standby_id) = (location(&standby_row), s(&standby_row["node_id"]));
    // Control is read through the other admin from here: a hung admin answers nothing.
    estate.enter(&standby).await;

    let rt = exact_runtime(&estate, &fp_name).await;
    let held = rt.hang().expect("the fabric-primary runtime is hung");
    assert!(!held.held.is_empty(), "every task of the fabric-primary but the leader is held: the provider sees it running");
    let hung_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // Only the node at the fabric-primary's (path, name) is ever mesh1's primary or the fabric-primary.
    let after = until_node_where(&estate, &fp_name, &format!("{fp_name:?} re-created and restored as mesh-primary and fabric-primary"), SETTLE, |n| {
        ready(n) && n["node_id"] != fp["node_id"] && n["is_fabric_primary"] == true && n["is_primary"] == true
    })
    .await;
    let other = estate.node_value(&standby).await;
    assert!(other["is_fabric_primary"] != true && other["is_primary"] != true, "{standby:?} took no primary role: {other}");
    let re_created_pid = estate.pid_of(&fp_name).await;
    drop(held);
    estate.settles_to(&[("mesh1", 2, 1), ("mesh2", 1, 1)], Duration::from_secs(30)).await;
    estate.stop().await;

    let spans = estate.spans();
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let ran: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| at(sp) >= hung_ns).collect();
    let run = ran.iter().find(|sp| at_location(sp, &standby) && sp["attributes"]["arm"] == "own-mesh" && s(&sp["attributes"]["outcome"]).starts_with("restored")).unwrap_or_else(|| panic!("{standby:?} ran fabric recovery from its own mesh and restored {fp_name:?}: {ran:#?}"));
    assert_eq!(s(&run["attributes"]["verdict"]), "running", "node investigation read the hung birth running: {run}");
    assert!(ran.iter().all(|sp| at_location(sp, &standby)), "nobody else ran it: {ran:#?}");
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == fp["node_id"]).collect();
    assert!(!recoveries.is_empty() && recoveries.iter().all(|sp| s(&sp["attributes"]["submitter"]) == standby_id), "the hung birth is recovered by {standby:?} alone: {recoveries:#?}");
    let terminate = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().find(|sp| at_location(sp, &fp_name) && sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["outcome"] == "complete").cloned();
    assert!(terminate.is_some(), "the recovery's TerminateRuntime of the hung birth completed");
    let elected: Vec<&Value> = named(&spans, "rdm.mesh.election.resolve.via-mesh-primary").into_iter().filter(|sp| at(sp) >= hung_ns && sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["winner_node_id"] == standby_id.as_str()).collect();
    assert!(elected.is_empty(), "mesh1 elected no node-admin while fabric recovery ran: {elected:#?}");
    // The re-created fabric-primary's own spans: it reads the Build records at its take, and only then.
    let own: Vec<Value> = std::fs::read_dir(&estate.evidence)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(&format!("rdm-node-admin.{re_created_pid}-")))
        .flat_map(|e| std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()).collect::<Vec<_>>())
        .collect();
    let took = named(&own, "rdm.node_admin.primary.update.via-primary-updated").into_iter().map(at).min().unwrap_or_else(|| panic!("the re-created {fp_name:?} (pid {re_created_pid}) took its primary roles by node.primary.update"));
    let read = named(&own, "rdm.node_admin.build.resolve.via-records-read").into_iter().find(|sp| sp["attributes"]["outcome"] == "read").cloned().unwrap_or_else(|| panic!("the re-created {fp_name:?} (pid {re_created_pid}) reads the Build records at its take: {:#?}", named(&own, "rdm.node.store.resolve.via-fold")));
    assert!(at(&read) >= took, "the read is the take's, after {fp_name:?} became fabric-primary: {read}");
    assert!(!s(&read["attributes"]["current_build_id"]).is_empty(), "the take's read names the current Build: {read}");
    let folds: Vec<&Value> = named(&own, "rdm.node.store.resolve.via-fold").into_iter().filter(|sp| s(&sp["attributes"]["file"]).ends_with("builds.jsonl")).collect();
    assert!(!folds.is_empty() && folds.iter().all(|sp| at(*sp) >= took), "{fp_name:?} reads builds.jsonl from its take on, never before it: {folds:#?}");
    eprintln!("span rdm.node_admin.build.resolve.via-records-read trace_id={} {}", s(&read["trace_id"]), read["attributes"]);
    estate.record_trace_url(run["trace_id"].as_str().unwrap_or(""));
    eprintln!("Q122 hung fabric-primary: {fp_name:?} (birth {}) re-created as {} by {standby:?}'s fabric recovery", s(&fp["node_id"]), s(&after["node_id"]));
}

/// The `observer`'s first fold span of its tickle ladder for `node_id` that nobody answered (TRUE
/// OFFLINE, node-recovery.md steps 2-3: the inspection and the recovery follow at once), once exported.
fn tickle_span<'a>(spans: &'a [Value], observer: &str, node_id: &Value) -> Option<&'a Value> {
    let mut found: Vec<&Value> = named(spans, "rdm.node_admin.node.update.via-true-offline").into_iter().filter(|sp| sp["attributes"]["observer"] == observer && sp["attributes"]["node_id"] == *node_id).collect();
    found.sort_by_key(|sp| start(sp));
    found.into_iter().next()
}

/// The span that ends the `caller`'s first tickle ladder for `node_id` with nobody reaching it: the
/// ladder's result span (`rdm.node.tickle.returns-unknown.via-unreached`, node-tickle.md), closed by
/// the ladder itself when the ladder ends (offline.rs `OfflineTickle::tick`), once exported. The TRUE
/// OFFLINE span ([`tickle_span`]) is the loop's fold of that result, made by the first pass after the
/// ladder ended.
fn ladder_end_span<'a>(spans: &'a [Value], caller: &str, node_id: &Value) -> Option<&'a Value> {
    let mut found: Vec<&Value> = named(spans, "rdm.node.tickle.returns-unknown.via-unreached")
        .into_iter()
        .filter(|sp| sp["attributes"]["caller"] == caller && sp["attributes"]["node_id"] == *node_id && sp["attributes"]["next"].is_string())
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
/// keeps the silence bookkeeping (`first_seen`), so a node-admin that takes the role mid-window
/// continues the window already running"): a member of mesh2 (two node-admins, its mesh-primary not
/// the fabric-primary) is hung (every task but the leader held by a tracer: the provider reads it running,
/// so node investigation's inspect, first since Q273, leaves it to the tickles; a killed member is
/// decided `exited` at the inspect with no ladder at all); once mesh2's primary has run tickle ladder 1 for it, that primary is
/// stopped, which hands mesh2's role to its second admin. The new mesh-primary tickles on the window
/// it has kept since before it took the role (its ladder names a `first_seen_ms` earlier than the
/// role change by a probe delay or more, never a window opened at the role), reaches TRUE OFFLINE,
/// on the inspect's `running`, and its submit follows the one recovery to `complete`: a new node.id at the
/// path over a newly allocated data dir, so a new endpoint key (node-investigation.md "Data dir"). The old primary is stopped mid-ladder, once its direct probe has
/// ended and before its ladder is folded (node-recovery.md step 3: a ladder nobody answers is the
/// recovery at once, Q181). Must NOT happen: the window restarted at the role change, the recovery
/// submitted by the stopped primary, or a proven-drift attempt for the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_handover_mid_ladder_continues_the_silence_window_and_completes_the_recovery() {
    let (estate, birth_build) = fabric_of("a_mesh_primary_handover_mid_ladder_continues_the_silence_window_and_completes_the_recovery", &[("mesh1", 1, 1), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let floor = rdm_transport::topology::staleness_floor();
    let nodes = estate.node_values().await;
    let fp_row = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    assert_eq!(fp_row["mesh"], "mesh1", "the day-0 admin keeps the fabric-primary role: {fp_row}");
    // Each admin by node id, as the spans name an actor, and by location for its own Stop.
    let mp_row = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).expect("mesh2's primary").clone();
    let standby_row = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["type"] == "node-admin" && n["node_id"] != mp_row["node_id"]).expect("mesh2's second admin").clone();
    let (mp, mp_at, standby) = (s(&mp_row["node_id"]), location(&mp_row), s(&standby_row["node_id"]));
    let node = &Estate::path("mesh2.node", "1");
    let before = estate.node_value(node).await;
    let rt = exact_runtime(&estate, node).await;
    let _held = rt.hang().expect("the exact runtime is hung");

    // The ladder on the mesh-primary that holds the role now, mid-way: its direct probe ended. Due by
    // node-recovery.md steps 1-2: the silence (one floor after the last digest heard: offline.rs
    // `silent_set` derives the mark from it), the loop's pass half a floor after the mark (on the
    // gossip-interval grid: up to one interval late), the inspect (`running`), then the direct probe to a hung process, which
    // spends its whole dial: the connection's idle timeout, the staleness floor (R-T8), inside the
    // call's 10 s budget. Then the export's 200 ms schedule and this cell's read of it (one gossip
    // interval). Its probes through the peers still run: the ladder is not folded yet.
    let gossip = rdm_core::cadence::gossip_interval();
    let connect = floor.min(Duration::from_secs(10));
    let round1_due = floor + gossip + gossip + floor / 2 + connect + Duration::from_millis(200);
    let round1 = wait_for(&format!("{mp}'s ladder for {node:?} ends its direct probe"), round1_due, || async {
        named(&estate.spans(), "rdm.node.tickle.resolve.via-direct").into_iter().find(|sp| sp["attributes"]["caller"] == mp.as_str() && sp["attributes"]["node_id"] == before["node_id"]).cloned()
    })
    .await;
    // Mid-ladder: the mesh-primary is stopped, and mesh2's role goes to its second admin.
    // A node command of the primary above the mesh-primary, the fabric-primary (Q342): no Build.
    let _ = &birth_build;
    rdm_test::estate::Estate::workflow(&format!("node.stop of {mp_at:?}"), estate.nodes_obj().await.stop(&mp_at).await).await;
    let role = wait_for(&format!("{standby:?} takes mesh2's role"), floor * 2, || async {
        named(&estate.spans(), "rdm.mesh.election.resolve.via-mesh-primary")
            .into_iter()
            .find(|sp| sp["attributes"]["observer_node_id"] == standby.as_str() && sp["attributes"]["winner_node_id"] == standby.as_str() && start(sp) > start(&round1))
            .cloned()
    })
    .await;

    // The new mesh-primary's submit follows the one recovery to its end.
    let recovery = wait_for(&format!("{standby:?} recovers {node:?}"), SETTLE, || async {
        named(&estate.spans(), "rdm.node_admin.node.update.via-node-recovery")
            .into_iter()
            .find(|sp| sp["attributes"]["node_id"] == before["node_id"] && s(&sp["attributes"]["outcome"]).starts_with("complete: attempt"))
            .cloned()
    })
    .await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), standby, "the new mesh-primary submitted the recovery: {recovery}");
    assert_eq!(s(&recovery["attributes"]["mesh"]), "mesh2", "an attempt of mesh2's lane, opened by the new mesh-primary itself: {recovery}");
    let after = until_node_where(&estate, node, &format!("{node:?} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a verdict other than exited re-creates over a newly allocated data dir, so a new transport key");

    let spans = estate.spans();
    let taken_ns = start(&role);
    let new_round1 = tickle_span(&spans, &standby, &before["node_id"]).unwrap_or_else(|| panic!("{standby:?}'s TRUE OFFLINE for {node:?}"));
    assert!(start(new_round1) > taken_ns, "{standby:?} tickles only once it holds the primary role");
    // The window it kept: the ladder runs on the instant it first saw the member silent, before the
    // role changed hands and a probe delay or more before it, never on a window opened at the role.
    // (The ladder's span is written when the ladder ends: the failed direct probe and via-peer probes
    // take their call deadlines, so its time after the role is not the wait before it.)
    let first_seen_ms = num_attr(new_round1, "first_seen_ms");
    assert!(first_seen_ms * 1_000_000 + (floor / 2).as_nanos() as u64 <= taken_ns, "{standby:?}'s window began a probe delay or more before it took the role: first_seen {first_seen_ms} ms, role at {} ms", taken_ns / 1_000_000);
    let waited = Duration::from_nanos(start(new_round1) - taken_ns);
    assert!(start(new_round1) < start(&recovery), "TRUE OFFLINE before the submit");
    let inspection = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == before["node_id"] && sp["attributes"]["inspector"] == standby.as_str())
        .unwrap_or_else(|| panic!("{standby:?}'s provider inspection of {node:?}"));
    assert_eq!(s(&inspection["attributes"]["verdict"]), "running", "{inspection}");
    assert!(!named(&spans, "rdm.node_admin.node.update.via-node-recovery").iter().any(|sp| sp["attributes"]["submitter"] == mp.as_str() && s(&sp["attributes"]["outcome"]).starts_with("complete")), "the stopped primary recovered nothing");
    let drift: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| s(&sp["attributes"]["scope"]).contains(before["node_id"].as_str().unwrap_or("-"))).collect();
    assert!(drift.is_empty(), "the fabric-primary opened a drift attempt for a member: {drift:?}");
    eprintln!(
        "acceptance 2: old mp {mp} direct probe trace {} | role {standby:?} at {} ms | new TRUE OFFLINE {} (first_seen {first_seen_ms} ms, {waited:?} after the role) | recovery trace {} ({})",
        round1["trace_id"].as_str().unwrap_or(""),
        taken_ns / 1_000_000,
        new_round1["trace_id"].as_str().unwrap_or(""),
        recovery["trace_id"].as_str().unwrap_or(""),
        s(&recovery["attributes"]["outcome"]),
    );
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}

/// CONTRACT (the approved proposal docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md
/// Part C1 "The no-successor cases", Q7 ruled Luke 2026-10-12: "`node.primary.update` commanded for a
/// node type with no other node of that type in the mesh is refused by name, `cannot transfer: no secondary
/// of type <type> in mesh <mesh>`, until a secondary is available. The primary stays primary, nothing
/// is cut off, no row is written, and the workflow that commanded it ... fails by name with that
/// reason"): in a fabric whose fabric-primary is its mesh's only node-admin, each of the changes Steps (replace),
/// DeleteConnections and Stop of the fabric-primary, submitted by `build.create`, is refused by name, `last-admin`
/// with exactly `cannot transfer: no secondary of type node-admin in mesh <its mesh>`, one
/// `rdm.node_admin.build.reject.via-last-admin` per call with the same text. The mesh-primary role has no
/// secondary, so the peer mesh's Ready primary is no successor of both roles (C2's outside search is
/// mesh.delete's only). No attempt opens, no role move is commanded, no row is written, and every node
/// stays as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replace_of_the_fabric_primary_that_is_its_meshs_last_admin_is_refused_by_name() {
    let (estate, birth_build) = two_mesh_estate("a_replace_of_the_fabric_primary_that_is_its_meshs_last_admin_is_refused_by_name").await;
    let before = estate.node_values().await;
    let primary = before.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let fp: PathName = serde_json::from_value(primary["name"].clone()).expect("structured primary location");
    let mesh = s(&primary["mesh"]);
    assert_eq!(before.iter().filter(|n| n["type"] == "node-admin" && n["mesh"] == mesh.as_str()).count(), 1, "the fabric-primary is its mesh's only admin: {before:#?}");
    let attempt = build_log::build_value(&estate, &birth_build).await["attempt"].clone();
    let q7 = format!("cannot transfer: no secondary of type node-admin in mesh {mesh}");
    let node = fp.clone();
    let carrier = estate.carrier().await;
    // The replace is a Build; the connections delete and the stop are node commands of the primary
    // above the fabric-primary, itself (Q342, D40): each is refused by the same name and text.
    match carrier.create(Change::Steps { steps: replace_steps(&node, &mesh, "node-admin").expect("valid replacement fixture name") }).await {
        // The Build call ends naming the reason, then the refusal's text: `<reason>: <text>`.
        Err(CallEnd::Rejected { reason, detail }) => assert_eq!((reason.as_str(), detail.as_str()), ("last-admin", format!("last-admin: {q7}").as_str()), "replace: refused with Q7's exact text"),
        Ok(_) => panic!("replace of the fabric-primary {fp:?}, its mesh's last admin, was accepted"),
        Err(other) => panic!("replace of the fabric-primary {fp:?}, its mesh's last admin, is refused last-admin, not {other:?}"),
    }
    let nodes = estate.nodes_obj().await;
    for (action, call) in [("node.connection.delete", nodes.delete_connections(&node, &rdm_core::status::ConnectionScope::All).await), ("stop", nodes.stop(&node).await)] {
        match call {
            Err(CallEnd::Rejected { reason, detail }) => assert_eq!((reason.as_str(), detail.as_str()), ("last-admin", format!("last-admin: {q7}").as_str()), "{action}: refused with Q7's exact text"),
            Ok(_) => panic!("{action} of the fabric-primary {fp:?}, its mesh's last admin, was accepted"),
            Err(other) => panic!("{action} of the fabric-primary {fp:?}, its mesh's last admin, is refused last-admin, not {other:?}"),
        }
    }
    let b = build_log::build_value(&estate, &birth_build).await;
    assert_eq!(b["attempt"], attempt, "no attempt opened: {b}");
    let spans = wait_for("the three refusals are exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        (named(&spans, "rdm.node_admin.build.reject.via-last-admin").len() >= 3).then_some(spans)
    })
    .await;
    let refused = named(&spans, "rdm.node_admin.build.reject.via-last-admin");
    assert_eq!(refused.len(), 3, "one refusal per call: {refused:?}");
    assert!(refused.iter().all(|sp| s(&sp["attributes"]["detail"]) == q7), "every refusal span carries Q7's exact text: {refused:?}");
    assert!(named(&spans, "rdm.node_admin.primary.update.via-update-primary").is_empty(), "no node.primary.update ran");
    assert!(
        named(&spans, "rdm.node_admin.build.update.via-node-rpc").into_iter().all(|sp| s(&sp["attributes"]["attempt"]).is_empty()),
        "no attempt number was recorded by any refused request"
    );
    estate.record_trace_url(refused[0]["trace_id"].as_str().unwrap_or(""));
    for n in &before {
        assert_eq!(serde_json::to_value(estate.node_view_by_id(&s(&n["node_id"]).parse().unwrap()).await).unwrap()["incarnation_id"], n["incarnation_id"], "{} is untouched", s(&n["name"]));
    }
    estate.shutdown().await;
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md Part C1 "The
/// no-successor cases", Q7 ruled Luke 2026-10-12: "`node.primary.update` commanded for a node type with no
/// other node of that type in the mesh is refused by name, `cannot transfer: no secondary of type
/// <type> in mesh <mesh>` ... The primary stays primary, nothing is cut off, no row is written, and
/// the workflow that commanded it ... fails by name with that reason"; node-replace.md: the replace
/// steps never leave a mesh with no live node-admin): in a fabric whose mesh2 has one node-admin, its
/// mesh-primary and not the fabric-primary, `build.create` of the replace steps of mesh2's primary is refused
/// `last-admin` with exactly `cannot transfer: no secondary of type node-admin in mesh mesh2`, under one
/// `rdm.node_admin.build.reject.via-last-admin` with the same text, before any step (its first, the
/// node.connection.delete that moves a primary's roles, has no successor): no attempt opens, no
/// connection is deleted, no role move is commanded, no row is written, and every node keeps its birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replace_of_a_mesh_primary_that_is_its_meshs_only_node_admin_is_refused_by_name_before_any_step() {
    let (estate, birth_build) = two_mesh_estate("a_replace_of_a_mesh_primary_that_is_its_meshs_only_node_admin_is_refused_by_name_before_any_step").await;
    let before = estate.node_values().await;
    let admins: Vec<&Value> = before.iter().filter(|n| n["type"] == "node-admin" && n["mesh"] == "mesh2").collect();
    assert_eq!(admins.len(), 1, "mesh2 has one node-admin: {before:#?}");
    let primary = admins[0];
    assert_eq!((primary["is_primary"].clone(), primary["is_fabric_primary"].clone()), (json!(true), json!(false)), "mesh2's only admin is its mesh-primary, not the fabric-primary: {primary}");
    let mp: PathName = serde_json::from_value(primary["name"].clone()).expect("structured primary location");
    let attempt = build_log::build_value(&estate, &birth_build).await["attempt"].clone();
    let q7 = "cannot transfer: no secondary of type node-admin in mesh mesh2";
    match estate.carrier().await.create(Change::Steps { steps: replace_steps(&mp, "mesh2", "node-admin").expect("valid replacement fixture name") }).await {
        // The call ends naming the reason, then the refusal's text: `<reason>: <text>`.
        Err(CallEnd::Rejected { reason, detail }) => assert_eq!((reason.as_str(), detail.as_str()), ("last-admin", format!("last-admin: {q7}").as_str()), "the replace of {mp:?} is refused with Q7's exact text"),
        Ok(_) => panic!("the replace of {mp:?}, mesh2's only node-admin, was accepted"),
        Err(other) => panic!("the replace of {mp:?}, mesh2's only node-admin, is refused last-admin, not {other:?}"),
    }
    assert_eq!(build_log::build_value(&estate, &birth_build).await["attempt"], attempt, "no attempt opened");
    let spans = wait_for("the refusal is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        (!named(&spans, "rdm.node_admin.build.reject.via-last-admin").is_empty()).then_some(spans)
    })
    .await;
    let refused = named(&spans, "rdm.node_admin.build.reject.via-last-admin");
    assert_eq!(refused.len(), 1, "one refusal: {refused:?}");
    assert_eq!(s(&refused[0]["attributes"]["detail"]), q7, "the refusal span carries Q7's exact text: {refused:?}");
    assert!(named(&spans, "rdm.node_admin.connection.delete.via-mesh-primary").is_empty(), "no connection was deleted");
    assert!(named(&spans, "rdm.node_admin.primary.update.via-update-primary").is_empty(), "no node.primary.update ran");
    estate.record_trace_url(refused[0]["trace_id"].as_str().unwrap_or(""));
    for n in &before {
        let now = serde_json::to_value(estate.node_view_by_id(&s(&n["node_id"]).parse().unwrap()).await).unwrap();
        assert_eq!((&now["node_id"], &now["incarnation_id"], &now["is_primary"]), (&n["node_id"], &n["incarnation_id"], &n["is_primary"]), "{} is untouched", s(&n["name"]));
    }
    eprintln!("trace {}: rdm.node_admin.build.reject.via-last-admin detail={q7:?} (replace of {mp:?})", s(&refused[0]["trace_id"]));
    estate.shutdown().await;
}

/// A caller outside the fabric that reaches the node-admin `admin` by its exact birth: its own
/// endpoint, and a resolver holding that birth as the answering admin's view names it.
async fn caller_of(admin: &Value) -> (std::sync::Arc<rdm_transport::NodeRpcClient>, rdm_core::NodeId) {
    use rdm_transport::{NodeRpcClient, ResolvedNode, StaticResolver};
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller binds an endpoint");
    let resolver = std::sync::Arc::new(StaticResolver::new());
    let node_id: rdm_core::NodeId = s(&admin["node_id"]).parse().expect("a node id");
    resolver.insert(ResolvedNode { mesh: s(&admin["mesh"]), r#type: (admin["type"].as_str().unwrap()).to_string(),
        node_id: node_id.clone(),
        path: s(&admin["path"]),
        name: s(&admin["name"]),
        endpoint_id: s(&admin["endpoint_id"]).parse().expect("an iroh key"),
        transport_addr: s(&admin["transport_addr"]).parse().expect("a transport address"),
        incarnation: rdm_core::IncarnationId::parse(&s(&admin["incarnation_id"])).unwrap(),
    });
    (std::sync::Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), node_id)
}

/// CONTRACT: a recovered node keeps the config and the tags its mesh-primary told its predecessor
/// (node-recovery.md "Retained: ... config (seeded from the Deleted row)"; proposal
/// node-properties-own-commands.md §8, recovery: "the successor's claim over it seeds config and tags
/// from that Deleted row and writes the tags index entries under the successor's id, in the same
/// write that seeds config"; node-identity-and-state.md "A rewrite is lossless for every field its
/// writer knows"). The caller puts `<mesh>.rpc.1`'s config and the tag `rabbit` through its
/// mesh-primary (not the fabric-primary), the node carries them, then its runtime is killed: the
/// mesh-primary's recovery writes the exited birth's Deleted row with that config and tag, the new
/// node's row is seeded from it (`via-node-row` moment `create`, `seeded_from` the exited node,
/// one key, `tags` rabbit), the new node (new node.id, same (path, name)) carries the config and the tag, and
/// `node.get` by the tag names the new node and not the exited one. Nothing copies them after the
/// create: no node.update runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_node_keeps_the_config_and_tags_its_mesh_primary_told_its_predecessor() {
    let (estate, _build) = two_mesh_estate("a_recovered_node_keeps_the_config_and_tags_its_mesh_primary_told_its_predecessor").await;
    let nodes = estate.node_values().await;
    let (node, mesh_primary, fp) = member_and_primaries(&nodes);
    let node_path = &node;
    let before = estate.node_value(node_path).await;
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let node_id: rdm_core::NodeId = s(&before["node_id"]).parse().unwrap();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node_id.clone()), node_id, incarnation: rdm_core::IncarnationId::parse(&s(&before["incarnation_id"])).unwrap() };
    // node-config-update.md step 1: the caller sends it to the fabric-primary, which sends it down.
    let (fp_client, fp_id) = caller_of(&fp).await;
    let reply = rdm_client::NodeRpc::new(&fp_client)
        .config_update(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, told.clone(), &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.config.update of {node_path:?} at the fabric-primary: {e:?}"));
    assert_eq!(reply.name(), "config-updated", "the mesh-primary wrote the row and the node applied it: {reply:?}");
    let told_json = json!({"tier": "gold"});
    wait_for(&format!("{node_path:?} carries its config"), Duration::from_secs(30), || async { (estate.node_value(node_path).await["config"] == told_json).then_some(()) }).await;
    let labels: std::collections::BTreeSet<String> = ["rabbit".to_string()].into();
    // Commands go down: the caller sends it to the fabric-primary, which sends it to the mesh-primary (Q203).
    let tagged = rdm_client::NodeRpc::new(&fp_client)
        .tags_create(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, &labels, &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.tag.create of {node_path:?} at the fabric-primary: {e:?}"));
    assert_eq!(tagged.name(), "tags-created", "the mesh-primary wrote the label's row: {tagged:?}");
    wait_for(&format!("{node_path:?} carries its tag"), Duration::from_secs(30), || async { (estate.node_view(node_path).await.tags == labels).then_some(()) }).await;

    estate.kill_node(node_path).await;
    let after = until_node_where(&estate, node_path, &format!("{node_path:?} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    // The new node's row, written by the mesh-primary, is seeded from the exited birth's
    // Deleted row: its config is the one told.
    let created = wait_for("the new node's row is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["moment"] == "create").cloned()
    })
    .await;
    let a = &created["attributes"];
    assert!(at_location(&created, &location(&mesh_primary)) && a["outcome"] == "row-seeded" && s(&a["seeded_from"]) == s(&before["node_id"]), "the mesh-primary writes the row at the name seeded from the exited birth's Deleted row: {created}");
    assert_eq!(s(&a["keys"]), "1", "the seeded row carries the config told to the predecessor: {created}");
    assert_eq!(s(&a["tags"]), "rabbit", "the create seeds the Deleted row's tags in the same write: {created}");
    let deleted: Vec<Value> = named(&estate.spans(), "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect();
    assert_eq!(deleted.len(), 1, "one Deleted row of the exited birth: {deleted:?}");
    // The record and its config stay (node-delete.md step 3): the exited node's own property record,
    // at its node id, still holds the config and the tag.
    let exited = estate.node_properties(&s(&before["node_id"]).parse().unwrap()).expect("the exited node's property record stays");
    assert_eq!((exited.config.clone(), exited.tags.clone()), (told.clone(), labels.clone()), "the exited node's property record keeps its config and tag: {exited:?}");
    // The new node writes its own property record from its launch snapshot, the exited birth's
    // acknowledged config and tags (node-recovery.md; its join answer is not where config lives).
    let own = wait_for("the new node's own property record", Duration::from_secs(30), || async { estate.node_properties(&s(&after["node_id"]).parse().unwrap()) }).await;
    assert_eq!((own.config.clone(), own.tags.clone()), (told.clone(), labels.clone()), "the new node's own record holds the exited birth's config and tag: {own:?}");
    let carried = wait_for(&format!("the new {node_path:?} carries the config"), Duration::from_secs(30), || async { let n = estate.node_value(node_path).await; (n["config"] == told_json).then_some(n) }).await;
    assert_eq!(carried["node_id"], after["node_id"], "the new node, not the exited one, carries it");
    assert_eq!(estate.node_view(node_path).await.tags, labels, "the new node's row carries the exited node's tag");
    let selected: Vec<String> = estate
        .nodes_obj()
        .await
        .get(&rdm_client::Filter::new().eq("tags", labels.iter().cloned()))
        .await
        .unwrap_or_else(|e| panic!("node.get [tags: rabbit]: {e:?}"))
        .into_iter()
        .map(|n| n.node_id.to_string())
        .collect();
    assert_eq!(selected, vec![s(&after["node_id"])], "the tags index names the new node under the label, and not the exited one");
    let told: Vec<Value> = named(&estate.spans(), "rdm.node_admin.node.update.via-config").into_iter().filter(|sp| sp["attributes"]["node_id"] == after["node_id"]).cloned().collect();
    assert!(told.is_empty(), "the join handed the config: no config call is sent to the starting node: {told:?}");
    eprintln!("trace {}: via-node-row delete node_id={} keys={} tags={}; trace {}: via-node-row create writer={} node_id={} seeded_from={} keys={} tags={}", s(&deleted[0]["trace_id"]), s(&before["node_id"]), deleted[0]["attributes"]["keys"], s(&deleted[0]["attributes"]["tags"]), s(&created["trace_id"]), s(&mesh_primary["node_id"]), s(&after["node_id"]), s(&a["seeded_from"]), a["keys"], s(&a["tags"]));
}

/// CONTRACT (node-recovery.md "Execution", Q342; node-stop.md, D40): mesh1 holds the fabric-primary and a
/// second node-admin; mesh2 holds one node-admin, its mesh-primary, and two rpc nodes. The fabric-primary
/// is stopped by a caller's node command (node.stop: its roles move to mesh1's other node-admin, then it
/// stops). With it stopped, `mesh2.node.1` is killed. mesh2's primary proves the exit and runs the
/// recovery itself, its own workflow: no other admin submits it, nothing is reported up and no Build is
/// written, and the stopped fabric-primary stays stopped through it. Must NOT happen: the recovery
/// waiting for the stopped fabric-primary, or it running again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_exit_while_the_fabric_primary_is_stopped_is_recovered_through_its_successor() {
    let (estate, build) = fabric_of(
        "a_member_exit_while_the_fabric_primary_is_stopped_is_recovered_through_its_successor",
        &[("mesh1", 2, 1), ("mesh2", 1, 2)],
        Duration::from_secs(90),
    )
    .await;
    let nodes = estate.node_values().await;
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    let fp_name = location(&fp);
    assert_eq!(fp["mesh"], "mesh1", "the day-0 admin holds the fabric-primary role: {fp_name:?}");
    let successor = nodes.iter().find(|n| n["mesh"] == "mesh1" && n["type"] == "node-admin" && n["node_id"] != fp["node_id"]).expect("mesh1's other node-admin").clone();
    let successor_name = location(&successor);
    let mp_row = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).expect("mesh2's primary").clone();
    let mp = s(&mp_row["node_id"]);

    // The fabric-primary is stopped by a caller's node command: its roles move to mesh1's other
    // node-admin, then it stops.
    let mut stop = estate.nodes_obj().await.stop(&fp_name).await.unwrap_or_else(|e| panic!("node.stop of the fabric-primary {fp_name:?} was refused: {e}"));
    Estate::complete(&mut stop).await;
    let stopped = estate
        .until_view(&format!("{fp_name:?} is stopped and {successor_name:?} holds the fabric-primary role"), SETTLE, |views| {
            let old = views.iter().find(|n| n.location() == fp_name)?;
            let new = views.iter().find(|n| n.location() == successor_name)?;
            (old.stopped && !old.is_fabric_primary && new.is_fabric_primary).then(|| serde_json::to_value(old).expect("a view serializes"))
        })
        .await;
    assert_eq!(stopped["incarnation_id"], fp["incarnation_id"], "the stopped fabric-primary is the same birth, stopped: {stopped}");

    // With the fabric-primary stopped, a member of mesh2 exits.
    let node = &Estate::path("mesh2.node", "1");
    let before = estate.node_value(node).await;
    let builds = estate.build_ids().await;
    estate.kill_node(node).await;
    let after = until_new_birth(&estate, node, &before, SETTLE).await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, loaded from the inherited data dir");
    let submits = |spans: &[Value]| -> Vec<Value> { named(spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let done = wait_for("the completed recovery submit is exported", Duration::from_secs(60), || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]) == "complete") }).await;
    assert_eq!((s(&done["attributes"]["submitter"]), s(&done["attributes"]["mesh"])), (mp.clone(), "mesh2".to_string()), "mesh2's primary runs it: {done}");
    let spans = wait_for("the recovery's create is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        create_span(&in_trace(&spans, &done), NO_BUILD, NO_ATTEMPT, node).is_some().then_some(spans)
    })
    .await;
    let run = in_trace(&spans, &done);
    assert_mesh_primary_ran_the_recovery(&run, &mp_row, &before);
    // No other admin submitted it.
    for sp in submits(&spans) {
        assert_eq!(s(&sp["attributes"]["submitter"]), mp, "only mesh2's primary submits its member's recovery: {sp}");
    }
    assert_eq!(estate.build_ids().await, builds, "no Build was minted");
    // The stopped fabric-primary stayed stopped through the recovery.
    let still = estate.node_value(&fp_name).await;
    assert_eq!((still["stopped"].clone(), still["incarnation_id"].clone()), (json!(true), fp["incarnation_id"].clone()), "{fp_name:?} stayed stopped: {still}");
    recovery_recorded(&estate, &before).await;
    eprintln!("member exit with the fabric-primary {fp_name:?} stopped, role at {successor_name:?}; {node:?} recovered by {mp}: trace {}", s(&done["trace_id"]));
    estate.record_trace_url(done["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md "Lost data dir (R-FM2)", crit:e12.s87.1): when `mesh1.node.2`'s runtime
/// exits and its `data_dir` is removed (`rm -rf` / unmounted), the mesh-primary proves `Exited` on
/// two consecutive ladders, holds `RDM_DATA_DIR_GONE_FLOORS` (2) staleness floors while `data_dir`
/// remains absent, records `data-dir-lost` on the exited birth's status row
/// (`rdm.node_admin.node.update.via-data-dir-lost`), and runs `node.delete -> node.create` over a
/// newly allocated `data_dir`. The recovered birth has a NEW `node.id`, a NEW endpoint key
/// (`endpoint_id != old_endpoint_id`), a NEW `data_dir != old_data_dir`, and reaches
/// `Ready` with `rdm.mesh.node.update.via-pre-node-ready-attempt` (`inherited = false`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_whose_data_dir_is_removed_after_exit_recovers_over_a_fresh_dir_with_a_new_key_after_two_floors() {
    let (estate, _build) = fabric_of(
        "a_member_whose_data_dir_is_removed_after_exit_recovers_over_a_fresh_dir_with_a_new_key_after_two_floors",
        &[("mesh1", 1, 2)],
        Duration::from_secs(60),
    )
    .await;
    let node = &Estate::path("mesh1.node", "2");
    let before = estate.node_value(node).await;
    let old_dir = estate.data_dir(node).await;
    let old_node_id = s(&before["node_id"]);
    let old_incarnation = s(&before["incarnation_id"]);
    let old_endpoint = s(&before["endpoint_id"]);

    estate.kill_node(node).await;
    std::fs::remove_dir_all(&old_dir).expect("remove the exited member's data_dir");

    let lost_span = wait_for("rdm.node_admin.node.update.via-data-dir-lost span", crate::node_recovery_due(), || async {
        named(&estate.spans(), "rdm.node_admin.node.update.via-data-dir-lost")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["node_id"]) == old_node_id)
            .cloned()
    })
    .await;
    assert_eq!(s(&lost_span["attributes"]["incarnation_id"]), old_incarnation, "{lost_span}");
    assert_eq!(s(&lost_span["attributes"]["path"]), old_dir, "{lost_span}");

    let after = until_new_birth(&estate, node, &before, SETTLE).await;
    assert_ne!(after["node_id"], before["node_id"], "new node.id minted: {after}");
    assert_ne!(s(&after["endpoint_id"]), old_endpoint, "new endpoint key minted (never re-minted from lost dir): {after}");
    let new_dir = estate.data_dir(node).await;
    assert_ne!(new_dir, old_dir, "newly allocated data_dir beside the old path: {after}");
    assert!(Path::new(&new_dir).is_dir(), "new data_dir exists on disk: {new_dir}");
    assert_eq!(s(&after["data_dir_lost"]["path"]), old_dir, "node.get surfaces the data-dir-lost fact: {after}");
    assert!(after["data_dir_lost"]["since"].as_u64().unwrap_or(0) > 0, "data-dir-lost carries since timestamp: {after}");

    let attempt = wait_for("rdm.mesh.node.update.via-pre-node-ready-attempt for recovered birth", crate::node_recovery_due(), || async {
        named(&estate.spans(), "rdm.mesh.node.update.via-pre-node-ready-attempt")
            .into_iter()
            .find(|sp| sp["attributes"]["node_id"] == after["node_id"])
            .cloned()
    })
    .await;
    assert_eq!(s(&attempt["attributes"]["inherited"]), "false", "fresh recovery birth runs its pre_node_ready hook with inherited = false: {attempt}");

    let submit = recovery_span(&estate, node, &before["node_id"]).await;
    eprintln!(
        "s87 cell 1 (gone): {node:?} old ({old_node_id}, {old_incarnation}, dir {old_dir}, key {old_endpoint}) -> new ({}, {}, dir {new_dir}, key {}) | via-data-dir-lost trace {} span {} | submit trace {} | pre_node_ready trace {} inherited={}",
        s(&after["node_id"]),
        s(&after["incarnation_id"]),
        s(&after["endpoint_id"]),
        s(&lost_span["trace_id"]),
        s(&lost_span["span_id"]),
        s(&submit["trace_id"]),
        s(&attempt["trace_id"]),
        attempt["attributes"]["inherited"],
    );
    estate.record_trace_url(submit["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md "Lost data dir (R-FM2)", deployment-provider.md `PrepareStorage`,
/// crit:e12.s87.2): when `mesh1.node.2`'s runtime exits and `<data_dir>/node-key` is corrupted, the
/// mesh-primary proves `Exited`, attempts `PrepareStorage`, fails `load_transport_key`, records
/// `data-dir-unreadable { path, cause }` on the birth's status row, emits
/// `rdm.node_admin.node.reject.via-data-dir-unreadable`, and HOLDS recovery. Over 3 additional
/// staleness floors: NO new `node.id` is minted, NO new endpoint key is minted, NO fresh `data_dir`
/// is allocated, NO second `rdm.node_admin.node.update.via-node-recovery` span is opened while the
/// dir remains unreadable, and `node.get` surfaces the `data-dir-unreadable` fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_whose_data_dir_is_unreadable_records_the_fact_and_holds_recovery_without_retrying() {
    let (estate, _build) = fabric_of(
        "a_member_whose_data_dir_is_unreadable_records_the_fact_and_holds_recovery_without_retrying",
        &[("mesh1", 1, 2)],
        Duration::from_secs(60),
    )
    .await;
    let node = &Estate::path("mesh1.node", "2");
    let before = estate.node_value(node).await;
    let old_dir = estate.data_dir(node).await;
    let old_node_id = s(&before["node_id"]);
    let old_incarnation = s(&before["incarnation_id"]);
    let key_path = Path::new(&old_dir).join("node-key");

    estate.kill_node(node).await;
    std::fs::write(&key_path, "corrupt-not-valid-hex-key").expect("corrupt node-key");

    let reject_span = wait_for("rdm.node_admin.node.reject.via-data-dir-unreadable span", crate::node_recovery_due(), || async {
        named(&estate.spans(), "rdm.node_admin.node.reject.via-data-dir-unreadable")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["node_id"]) == old_node_id)
            .cloned()
    })
    .await;
    assert_eq!(s(&reject_span["attributes"]["incarnation_id"]), old_incarnation, "{reject_span}");
    assert_eq!(s(&reject_span["attributes"]["path"]), old_dir, "{reject_span}");
    assert!(!s(&reject_span["attributes"]["cause"]).is_empty(), "cause recorded: {reject_span}");

    let held_view = until_node_where(&estate, node, "node.get surfaces data_dir_unreadable", crate::node_recovery_due(), |n| {
        n["data_dir_unreadable"]["path"] == json!(old_dir) && !s(&n["data_dir_unreadable"]["cause"]).is_empty()
    })
    .await;
    assert_eq!(s(&held_view["node_id"]), old_node_id, "no new node.id minted while unreadable: {held_view}");

    // Hold over 3 additional staleness floors: no second recovery submit,
    // no new node_id, no fresh data_dir.
    tokio::time::sleep(rdm_core::cadence::staleness_floor() * 3 + Duration::from_millis(200)).await;

    let spans = estate.spans();
    let submits: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["node_id"]) == old_node_id)
        .collect();
    assert_eq!(submits.len(), 1, "no second via-node-recovery span opened while data_dir remains unreadable: {submits:#?}");

    let views = estate.node_values().await;
    let at_name: Vec<&Value> = views.iter().filter(|n| n["name"] == json!(node)).collect();
    assert_eq!(at_name.len(), 1, "still only the held birth at {node:?}: {at_name:#?}");
    assert_eq!(s(&at_name[0]["node_id"]), old_node_id, "no new node_id minted: {at_name:#?}");
    assert_eq!(s(&at_name[0]["data_dir_unreadable"]["path"]), old_dir, "data_dir_unreadable still surfaced: {at_name:#?}");

    eprintln!(
        "s87 cell 2 (broken hold): {node:?} ({old_node_id}, {old_incarnation}) held with data_dir_unreadable at {old_dir} | reject trace {} span {} cause={:?} | submits count={}",
        s(&reject_span["trace_id"]),
        s(&reject_span["span_id"]),
        s(&reject_span["attributes"]["cause"]),
        submits.len(),
    );
    estate.record_trace_url(reject_span["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md "Lost data dir (R-FM2)", crit:e12.s87.3): when `<data_dir>/node-key`
/// is restored after an unreadable hold, on the next tick the mesh-primary clears
/// `data-dir-unreadable` (`rdm.node_admin.node.resolve.via-data-dir-repaired`), completes
/// `node.create` over the SAME `data_dir` with the RETAINED endpoint key
/// (`endpoint_id == old_endpoint_id`) and a new `node.id`, and the birth reaches `Ready`
/// (`inherited = true`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_held_on_an_unreadable_data_dir_completes_recovery_with_its_retained_key_once_repaired() {
    let (estate, _build) = fabric_of(
        "a_member_held_on_an_unreadable_data_dir_completes_recovery_with_its_retained_key_once_repaired",
        &[("mesh1", 1, 2)],
        Duration::from_secs(60),
    )
    .await;
    let node = &Estate::path("mesh1.node", "2");
    let before = estate.node_value(node).await;
    let old_dir = estate.data_dir(node).await;
    let old_node_id = s(&before["node_id"]);
    let old_incarnation = s(&before["incarnation_id"]);
    let old_endpoint = s(&before["endpoint_id"]);
    let key_path = Path::new(&old_dir).join("node-key");
    let saved_key = std::fs::read_to_string(&key_path).expect("read original node-key before corruption");

    estate.kill_node(node).await;
    std::fs::write(&key_path, "corrupt-not-valid-hex-key").expect("corrupt node-key");

    let reject_span = wait_for("rdm.node_admin.node.reject.via-data-dir-unreadable span", crate::node_recovery_due(), || async {
        named(&estate.spans(), "rdm.node_admin.node.reject.via-data-dir-unreadable")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["node_id"]) == old_node_id)
            .cloned()
    })
    .await;
    let _held_view = until_node_where(&estate, node, "node.get surfaces data_dir_unreadable", crate::node_recovery_due(), |n| {
        n["data_dir_unreadable"]["path"] == json!(old_dir)
    })
    .await;

    // Repair `<data_dir>/node-key` from backup.
    std::fs::write(&key_path, &saved_key).expect("restore valid node-key");

    let repaired_span = wait_for("rdm.node_admin.node.resolve.via-data-dir-repaired span", crate::node_recovery_due(), || async {
        named(&estate.spans(), "rdm.node_admin.node.resolve.via-data-dir-repaired")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["node_id"]) == old_node_id)
            .cloned()
    })
    .await;
    assert_eq!(s(&repaired_span["attributes"]["incarnation_id"]), old_incarnation, "{repaired_span}");
    assert_eq!(s(&repaired_span["attributes"]["path"]), old_dir, "{repaired_span}");

    let after = until_new_birth(&estate, node, &before, SETTLE).await;
    assert_ne!(after["node_id"], before["node_id"], "new node.id minted: {after}");
    assert_eq!(s(&after["endpoint_id"]), old_endpoint, "retained endpoint key from repaired data_dir: {after}");
    assert_eq!(estate.data_dir(node).await, old_dir, "recovered over the same data_dir: {after}");
    assert!(after["data_dir_unreadable"].is_null(), "data_dir_unreadable cleared on node.get: {after}");

    let attempt = wait_for("rdm.mesh.node.update.via-pre-node-ready-attempt for repaired birth", crate::node_recovery_due(), || async {
        named(&estate.spans(), "rdm.mesh.node.update.via-pre-node-ready-attempt")
            .into_iter()
            .find(|sp| sp["attributes"]["node_id"] == after["node_id"])
            .cloned()
    })
    .await;
    assert_eq!(s(&attempt["attributes"]["inherited"]), "true", "repaired recovery birth runs its pre_node_ready hook with inherited = true: {attempt}");

    eprintln!(
        "s87 cell 3 (repaired): {node:?} old ({old_node_id}, {old_incarnation}) -> new ({}, {}) over same dir {old_dir} with retained key {old_endpoint} | reject trace {} | repaired trace {} span {} | pre_node_ready trace {} inherited={}",
        s(&after["node_id"]),
        s(&after["incarnation_id"]),
        s(&reject_span["trace_id"]),
        s(&repaired_span["trace_id"]),
        s(&repaired_span["span_id"]),
        s(&attempt["trace_id"]),
        attempt["attributes"]["inherited"],
    );
    estate.record_trace_url(repaired_span["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}



/// The executable `node`'s process runs, as the OS names it.
async fn exe_of(estate: &Estate, node: &PathName) -> std::path::PathBuf {
    let pid = estate.pid_of(node).await;
    std::fs::read_link(format!("/proc/{pid}/exe")).unwrap_or_else(|e| panic!("{node:?} (pid {pid}): /proc exe: {e}"))
}

/// The reason of the Failed frame a Build ends with.
async fn failed_steps(nodes: &Nodes, steps: Vec<Step>) -> String {
    let got = frames(nodes.steps(steps).await.expect("the Build was accepted")).await;
    match got.last() {
        Some(Frame::Failed { reason, .. }) => reason.clone(),
        other => panic!("the Build ended {other:?}, not Failed: {got:?}"),
    }
}

/// CONTRACT (node-rpc-envelope.md "Build schema": "Create names the node and the executable binding the
/// Build fixes for it (launch id, hash); the executing admin launches that binding, verified at launch,
/// whichever admin runs or resumes the step"; node-replace.md step 3: "The Build fixes the bin on its
/// Create step, so every admin that runs or resumes the step launches that one, and the row
/// records it beside `config`"; node-recovery.md "Retained": "config and the executable (both seeded
/// from the deleted row: a recovered node runs the bin its row records, never the recovering
/// admin's)"; build-deployment.md §2: every create names its executable): the fabric is born on
/// binding set A (`rpc_node` = the built node), every path named on set A's executable of its type,
/// and mesh1's and mesh2's admins alike launch set A's. A Create binding, a copy the set does not name, is the Create step of a
/// replace steps of `mesh1.node.1` (run by mesh1's admin) and of `mesh2.node.1` (run by mesh2's admin):
/// both successors run that copy. `mesh1.node.1` is then killed and recovered over its inherited data
/// dir, and `mesh2.node.1` held (SIGSTOP) and recovered over a newly allocated one: each recovered node
/// runs the copy its row records, not set A's. A Create step naming a file that is not there,
/// and one naming the copy with a hash its bytes do not have, end Failed by name, nothing launched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_steps_executable_reaches_the_provider_at_every_admin_and_through_recovery() {
    use rdm_client::binding::{sha256_file, Binding};
    let sha = candidate_sha();
    let set_a = binding_set(&sha);
    let estate = Estate::bootstrap_on(owner("a_create_steps_executable_reaches_the_provider_at_every_admin_and_through_recovery"), "fabric1", "mesh1", &set_a, &sha, &["node"])
        .await
        .expect("binding set A is accepted");
    let rpc_a = set_a.bindings.iter().find(|b| b.launch_id == "node").cloned().expect("set A binds rpc_node");
    // A copy of the node's bytes at its own path: the one the Build fixes.
    let execs = estate.root.join("execs");
    std::fs::create_dir_all(&execs).unwrap();
    let fixed = execs.join("rpc-node-fixed");
    std::fs::copy(&rpc_a.executable, &fixed).unwrap();
    let binding = Binding { launch_id: "node".into(), sha256: sha256_file(&fixed).unwrap(), executable: fixed.clone(), image: None };
    let shape = [("mesh1", 1, 2), ("mesh2", 1, 2)];
    let estate = born_at(estate, &shape, Duration::from_secs(90)).await.0;
    assert_eq!(exe_of(&estate, &Estate::path("mesh1.node", "2")).await, rpc_a.executable, "mesh1's admin launches set A's node, the one its path names");
    assert_eq!(exe_of(&estate, &Estate::path("mesh2.node", "2")).await, rpc_a.executable, "mesh2's admin launches set A's node, the one its path names");

    let nodes = objects(&estate).await;
    let with_binding = |node: &PathName, mesh: &str, b: &Binding| -> Vec<Step> {
        replace_steps(node, mesh, "node").expect("valid replacement fixture name").into_iter().map(|s| match s {
            Step::Create { path: node_path, name: node_name, mesh, r#type, .. } => { let node = &rdm_core::PathName { path: node_path.clone(), name: node_name.clone() }; Step::Create { path: node.path.clone(), name: node.name.clone(), mesh, r#type, executable: Some(b.clone()) } },
            other => other,
        }).collect()
    };
    for (node, mesh) in [(&Estate::path("mesh1.node", "1"), "mesh1"), (&Estate::path("mesh2.node", "1"), "mesh2")] {
        let path: PathName = node.clone();
        let got = frames(nodes.steps(with_binding(&path, mesh, &binding)).await.expect("the Build was accepted")).await;
        assert_eq!(got.last(), Some(&Frame::Complete), "the replace of {node:?}: {got:?}");
        let n = until_node_where(&estate, node, &format!("{node:?} ready"), SETTLE, ready).await;
        assert_eq!(exe_of(&estate, node).await, fixed, "{node:?}'s successor runs the executable its Build fixed, not its admin's: {n}");
    }

    // Recovery over the inherited data dir: the exited birth's row records the executable.
    let before = estate.node_value(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).await;
    let dir = estate.data_dir(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).await;
    estate.kill_node(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).await;
    let after = until_new_birth(&estate, &Estate::path("mesh1.node", "1"), &before, SETTLE).await;
    assert_eq!((after["endpoint_id"].clone(), estate.data_dir(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).await), (before["endpoint_id"].clone(), dir), "recovered over the inherited data dir");
    assert_eq!(exe_of(&estate, &Estate::path("mesh1.node", "1")).await, fixed, "the node recovered over its inherited dir runs the executable its row records, not its path's entry's");

    // Recovery over a newly allocated data dir: a held member is terminated and created afresh.
    let before = estate.node_value(&rdm_core::PathName::new("mesh2.node", "1").unwrap()).await;
    let dir = estate.data_dir(&rdm_core::PathName::new("mesh2.node", "1").unwrap()).await;
    assert_eq!(exact_runtime(&estate, &Estate::path("mesh2.node", "1")).await.apply(Fault::Stop).expect("the exact runtime is held").state_after, Some('T'));
    let after = until_new_birth(&estate, &Estate::path("mesh2.node", "1"), &before, SETTLE).await;
    assert!(after["endpoint_id"] != before["endpoint_id"] && estate.data_dir(&rdm_core::PathName::new("mesh2.node", "1").unwrap()).await != dir, "recovered over a newly allocated data dir: {after}");
    assert_eq!(exe_of(&estate, &Estate::path("mesh2.node", "1")).await, fixed, "the node recovered over a new dir runs the executable its row records, not its path's entry's");

    // The refusals, by name, nothing launched.
    let absent = Binding { executable: execs.join("not-there"), ..binding.clone() };
    let reason = failed_steps(&nodes, vec![Step::Create { mesh: "mesh1".into(), r#type: "node".into(), path: "mesh1.node".into(), name: "7".into(), executable: Some(absent) }]).await;
    assert!(reason.contains("not-there") && reason.contains("cannot be read"), "a missing file is refused by name: {reason}");
    let mismatched = Binding { sha256: "0".repeat(64), ..binding.clone() };
    let reason = failed_steps(&nodes, vec![Step::Create { mesh: "mesh1".into(), r#type: "node".into(), path: "mesh1.node".into(), name: "8".into(), executable: Some(mismatched) }]).await;
    assert!(reason.contains("rpc-node-fixed") && reason.contains("the binding expects"), "a hash mismatch is refused by name: {reason}");
    for node in ["mesh1.node.7", "mesh1.node.8"] {
        assert!(!estate.node_values().await.iter().any(|n| n["name"] == node), "nothing is launched at {node:?}");
    }
    estate.shutdown().await;
}

/// CONTRACT (node-investigation.md: "A node that comes back after it was re-created is refused ... The
/// node that refused it reports it to the fabric-primary, which terminates its runtime"; Q110, Q128): a
/// member is held (SIGSTOP) until its mesh-primary recovers it as a new birth at its path, then the held
/// birth is continued. Its digest is refused (no view lists its old node id), its mesh-primary reports it
/// (`via-returned-reported`), the fabric-primary runs `TerminateRuntime` on its exact runtime under
/// `node.terminate:<path>@<incarnation>`, and its process is gone; the path keeps one live birth, the
/// re-created one. What must NOT happen: the old process left running, or the old node id in a view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_superseded_birth_that_comes_back_is_refused_reported_and_killed() {
    let (mut estate, _build) = estate("a_superseded_birth_that_comes_back_is_refused_reported_and_killed").await;
    let node = &Estate::path("mesh1.node", "2");
    let before = estate.node_value(node).await;
    let old_id = s(&before["node_id"]);
    let rt = exact_runtime(&estate, node).await;
    let pid = rt.pid;
    assert_eq!(rt.apply(Fault::Stop).expect("the exact runtime is held").state_after, Some('T'));
    let after = until_new_birth(&estate, node, &before, SETTLE).await;
    let resumed_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let cont = std::process::Command::new("kill").args(["-CONT", &pid.to_string()]).status().map(|s| s.success()).unwrap_or(false);
    let floor = rdm_transport::topology::staleness_floor();
    let mut samples = Vec::new();
    let until = std::time::Instant::now() + floor * 2 + Duration::from_secs(30);
    while std::time::Instant::now() < until && alive(pid as u64) {
        let views = estate.node_values().await;
        let old_listed: Vec<String> = views.iter().filter(|n| s(&n["node_id"]) == old_id).map(|n| format!("{}={}", s(&n["name"]), s(&n["status"]))).collect();
        let live_at_path: Vec<String> = views.iter().filter(|n| n["name"] == json!(node) && live(n)).map(|n| s(&n["node_id"])).collect();
        samples.push(json!({"t_ms": (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64 - resumed_ns) / 1_000_000, "old_listed": old_listed, "live_at_path": live_at_path, "old_process_alive": alive(pid as u64)}));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    estate.stop().await;
    let spans = estate.spans();
    let start_ns = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let mut named_old: std::collections::BTreeMap<String, usize> = Default::default();
    for sp in spans.iter().filter(|sp| start_ns(sp) >= resumed_ns) {
        let a = sp["attributes"].to_string();
        if a.contains(&old_id) || a.contains(&s(&before["incarnation_id"])) {
            *named_old.entry(s(&sp["name"])).or_default() += 1;
        }
    }
    let refusals: Vec<Value> = spans.iter().filter(|sp| start_ns(sp) >= resumed_ns && (s(&sp["name"]).contains(".reject.") || s(&sp["name"]).contains("refus")) && sp["attributes"].to_string().contains(&old_id)).map(|sp| json!({"name": sp["name"], "attributes": sp["attributes"]})).collect();
    let facts = json!({"node": node, "old_node_id": old_id, "new_node_id": after["node_id"], "continued": cont, "samples": samples, "spans_naming_the_old_birth": named_old, "refusals": refusals});
    estate.artifact("superseded.json", &facts);
    eprintln!("superseded birth facts: {}", serde_json::to_string_pretty(&facts).unwrap());
    let last = samples.last().cloned().unwrap_or(Value::Null);
    assert!(cont, "the held birth was continued: {facts}");
    assert!(samples.iter().all(|x| x["old_listed"] == json!([])), "no view lists the old node id again: {facts}");
    assert!(!alive(pid as u64), "the returning birth's process was terminated: {facts}");
    let reported = spans.iter().any(|sp| sp["name"] == "rdm.node_admin.node.update.via-returned-reported" && sp["attributes"]["node_id"] == old_id.as_str());
    assert!(reported, "its mesh-primary reported it to the fabric-primary: {facts}");
    // node.terminate of the exact returned birth: its run's span names the birth, and its TerminateRuntime
    // step completed under it.
    let runs: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-terminate-returned").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == old_id && sp["attributes"]["incarnation_id"] == before["incarnation_id"]).collect();
    let terminated = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().any(|sp| sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["outcome"] == "complete" && runs.iter().any(|run| descends_from(&spans, sp, run)));
    assert!(terminated, "TerminateRuntime ran under node.terminate of {node:?}@{}: {runs:?} {facts}", s(&before["incarnation_id"]));
    assert_eq!(last["live_at_path"], json!([s(&after["node_id"])]), "the path keeps one live birth, the re-created one: {facts}");
}

/// CONTRACT (Q207, #3211 red 3: "a FP/MP looks to see if something at that (path, name) exists currently;
/// if it does it should delete the caller"): a mesh2 node is held (SIGSTOP) until mesh2's primary
/// re-creates it as a new birth at its path, then continued: a caller its mesh-primary does not hold
/// (its Digest, or a declaration, refused). Mesh2's primary decides it from its atlas by its (path, name):
/// another birth is there, so the caller is deleted; not the fabric-primary, it verifies at the root
/// with `fabric.atlas.get` first, which agrees; it reports the caller to the fabric-primary for
/// `node.terminate`. One `via-refused-caller` span per decision names the door, the
/// decision, the root's answer and the outcome. What must NOT happen: the caller refused and nothing
/// decided, the caller re-admitted over the re-created birth, its process left running, or the path
/// left with any live birth but the re-created one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_re_created_birth_that_comes_back_is_deleted_by_its_path_name_verified_at_the_root() {
    let (mut estate, _build) = two_mesh_estate("a_re_created_birth_that_comes_back_is_deleted_by_its_path_name_verified_at_the_root").await;
    let node = &Estate::path("mesh2.node", "1");
    let before = estate.node_value(node).await;
    let old_id = s(&before["node_id"]);
    let rt = exact_runtime(&estate, node).await;
    let pid = rt.pid;
    assert_eq!(rt.apply(Fault::Stop).expect("the exact runtime is held").state_after, Some('T'));
    let after = until_new_birth(&estate, node, &before, SETTLE).await;
    let cont = std::process::Command::new("kill").args(["-CONT", &pid.to_string()]).status().map(|s| s.success()).unwrap_or(false);
    let floor = rdm_transport::topology::staleness_floor();
    let decided = |spans: &[Value]| spans.iter().filter(|sp| sp["name"] == "rdm.node_admin.node.update.via-refused-caller" && sp["attributes"]["node_id"] == old_id.as_str()).map(|sp| sp["attributes"].clone()).collect::<Vec<Value>>();
    let until = std::time::Instant::now() + floor * 2 + Duration::from_secs(30);
    while std::time::Instant::now() < until && (alive(pid as u64) || decided(&estate.spans()).is_empty()) {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let live_at_path: Vec<String> = estate.node_values().await.iter().filter(|n| n["name"] == json!(node) && live(n)).map(|n| s(&n["node_id"])).collect();
    estate.stop().await;
    let spans = estate.spans();
    let decisions = decided(&spans);
    let facts = json!({"node": node, "old_node_id": old_id, "new_node_id": after["node_id"], "continued": cont, "decisions": decisions, "old_process_alive": alive(pid as u64), "live_at_path": live_at_path});
    estate.artifact("refused-caller.json", &facts);
    eprintln!("refused caller facts: {}", serde_json::to_string_pretty(&facts).unwrap());
    assert!(cont, "the held birth was continued: {facts}");
    assert!(!decisions.is_empty(), "mesh2's primary decided the caller by its (path, name): {facts}");
    for d in &decisions {
        assert!(d["node_path"] == "mesh2.node-admin" && d["node_name"] == "1", "mesh2's primary decides its own mesh's caller: {facts}");
        assert!(d["subject_path"] == node.path.as_str() && d["subject_name"] == node.name.as_str(), "decided at the caller's (path, name): {facts}");
        assert!(d["door"] == "digest" || d["door"] == "declaration", "decided at a door a refused caller comes through, its Digest or its declaration: {facts}");
        assert_eq!(d["decision"], "delete-path-held", "another birth is at the path: the caller is deleted: {facts}");
        assert_eq!(d["root"], "agrees", "the fabric-primary's atlas agrees: {facts}");
    }
    assert!(decisions.iter().any(|d| d["outcome"] == "terminated"), "the caller is reported to the fabric-primary for node.terminate: {facts}");
    assert!(!alive(pid as u64), "the caller's process is gone: {facts}");
    assert_eq!(facts["live_at_path"], json!([s(&after["node_id"])]), "the path keeps one live birth, the re-created one: {facts}");
}

/// The mesh-primary's recovery submit for `node`'s birth that completed, once exported: a submit
/// answered build-in-progress is exported under the same name first (node-recovery.md step 1), so the
/// first span is not the one.
async fn completed_recovery_span(estate: &Estate, node: &PathName, node_id: &Value) -> Value {
    wait_for(&format!("the completed recovery submit for {node:?} is exported"), Duration::from_secs(90), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().find(|sp| at_location(sp, node) && sp["attributes"]["node_id"] == *node_id && s(&sp["attributes"]["outcome"]).starts_with("complete")).cloned()
    })
    .await
}

/// CONTRACT (node-recovery.md "Execution": each mesh-primary runs its own members' recoveries, its own
/// workflow, never a Build (Q342)): one node of mesh1 and one of mesh2 are killed (kill -9) in the
/// same instant. Each mesh-primary investigates and runs its own member's recovery at the same time, and
/// both members are re-created as new births, each recovery's node.delete running its TerminateRuntime.
/// Must NOT happen: one recovery waiting on the other, a Build written, either member recovered twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_members_of_two_meshes_killed_at_once_are_recovered_by_their_own_mesh_primaries_at_the_same_time() {
    let (mut estate, birth_build) = two_mesh_estate("two_members_of_two_meshes_killed_at_once_are_recovered_by_their_own_mesh_primaries_at_the_same_time").await;
    let nodes = estate.node_values().await;
    let member_of = |mesh: &str| nodes.iter().filter(|n| n["mesh"] == mesh && n["type"] == "node" && n["is_primary"] != true).map(location).min().unwrap_or_else(|| panic!("an rpc member of {mesh} holding no primary role"));
    let primary_of = |mesh: &str| nodes.iter().find(|n| n["mesh"] == mesh && n["type"] == "node-admin" && n["is_primary"] == true).cloned().unwrap_or_else(|| panic!("{mesh}'s primary"));
    let (a, b) = (member_of("mesh1"), member_of("mesh2"));
    let (before_a, before_b) = (estate.node_value(&a).await, estate.node_value(&b).await);
    let builds = estate.build_ids().await;
    estate.kill_node(&a).await;
    estate.kill_node(&b).await;

    let (after_a, after_b) = tokio::join!(until_new_birth(&estate, &a, &before_a, SETTLE), until_new_birth(&estate, &b, &before_b, SETTLE));
    let (rec_a, rec_b) = tokio::join!(completed_recovery_span(&estate, &a, &before_a["node_id"]), completed_recovery_span(&estate, &b, &before_b["node_id"]));
    assert_eq!(s(&rec_a["attributes"]["submitter"]), s(&primary_of("mesh1")["node_id"]), "mesh1's primary submits mesh1's recovery: {rec_a}");
    assert_eq!(s(&rec_b["attributes"]["submitter"]), s(&primary_of("mesh2")["node_id"]), "mesh2's primary submits mesh2's recovery: {rec_b}");
    for (rec, mesh) in [(&rec_a, "mesh1"), (&rec_b, "mesh2")] {
        assert_eq!((s(&rec["attributes"]["outcome"]), s(&rec["attributes"]["mesh"])), ("complete".to_string(), mesh.to_string()), "{mesh}'s recovery completes: {rec}");
    }
    // Each recovery ran its own delete, its TerminateRuntime complete, and its own create, under its submit.
    for (before, node, rec, mesh) in [(&before_a, &a, &rec_a, "mesh1"), (&before_b, &b, &rec_b, "mesh2")] {
        recovery_recorded(&estate, before).await;
        let (terminate, _) = recovery_terminate(&estate, rec, node, &before["node_id"]).await;
        assert_eq!(s(&terminate["attributes"]["outcome"]), "complete", "{node:?}'s TerminateRuntime: {terminate}");
        assert_mesh_primary_ran_the_recovery(&in_trace(&estate.spans(), rec), &primary_of(mesh), before);
    }
    let submits: Vec<Value> = named(&estate.spans(), "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| s(&sp["attributes"]["outcome"]) == "complete" && (sp["attributes"]["node_id"] == before_a["node_id"] || sp["attributes"]["node_id"] == before_b["node_id"])).cloned().collect();
    assert_eq!(submits.len(), 2, "each member recovered once: {submits:#?}");
    assert_eq!(estate.build_ids().await, builds, "no Build was minted");
    eprintln!(
        "two-mesh cell: {a:?} and {b:?} killed at once; recovered as {} and {}; traces {} | {}",
        s(&after_a["node_id"]), s(&after_b["node_id"]), s(&rec_a["trace_id"]), s(&rec_b["trace_id"])
    );
    estate.record_trace_url(rec_a["trace_id"].as_str().unwrap_or(""));
    estate.record_trace_url(rec_b["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.stop().await;
}
