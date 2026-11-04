//! product=mesh, feature=node-lifecycle, subfeature=node-recovery, rung=MN, provider=process.
//!
//! Node recovery (node-recovery.md, R-RC1). A member that exits outside any workflow is recovered by its own mesh-primary on the provider's
//! exact exited verdict: node.delete of the exited instance, then node.create at the same (path, name)
//! over its predecessor's data dir when its storage is `persistent`, else a new one (always a new
//! node.id and endpoint, Q358, Q359). A member that is hung but still
//! running is never recovered; a repeat submit is answered, never run twice; an exit during a
//! workflow fails that workflow and the next tick recovers the member.

use crate::build_log;
use rdm_client::{NodeStatus, PathName};
use rdm_test::estate::{at_location, descends_from, location, named, wait_for, Estate, Owner};
use rdm_test::process_faults::{ExactRuntime, Fault};
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-recovery".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn ready(node: &Value) -> bool {
    node["state"] == "ready"
}

/// A birth that holds or contends for a role and serves (`NodeStatus::is_live`).
fn live(node: &Value) -> bool {
    matches!(node["state"].as_str(), Some("pending" | "ready" | "connections-deleting"))
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
/// Build's id. The cells hold and kill members: workflows the initial fabric refuses until it has
/// first reached ready (states.md "The initial fabric before ready").
async fn fabric_of(test: &str, shape: &[(&str, u32, u32)], settle: Duration) -> (Estate, String) {
    born_at(Estate::bootstrap(owner(test), "fabric1", "mesh1").await, shape, settle).await
}

/// `estate`'s fabric born at `shape` by `build.create`, followed to its terminal frame, the view settled
/// on it within `settle` and the fabric ready; the birth Build's id.
async fn born_at(estate: Estate, shape: &[(&str, u32, u32)], settle: Duration) -> (Estate, String) {
    let born = estate.resize_shape("fabric1", shape).await;
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
    let mesh = if rdm_test::estate::mesh_of(&primary) == "mesh1" { "mesh2" } else { "mesh1" };
    let mesh_primary = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == mesh && n["type"] == "node-admin" && n["is_primary"] == true).expect("the node's mesh-primary").clone();
    assert_ne!(mesh_primary["node_id"], primary["node_id"], "the member's mesh-primary is not the fabric-primary");
    // The member is a node holding no role: a node-type primary runs node.primary.update before its
    // own node.connection.delete, stop or delete (lifecycles/node-connections-delete.md step 0), which
    // is not the member workflow these cells follow.
    let member = nodes.iter().filter(|n| rdm_test::estate::mesh_of(&n) == mesh && n["type"] == "node" && n["is_primary"] != true).map(location).min().expect("an rpc member of the mesh that holds no primary role");
    (member, mesh_primary, primary)
}

/// The exact runtime `node`'s birth published in its data dir: the fault service acts on it alone.
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

/// A recovery belongs to no Build (Q342): the spans of its delete and its create carry the empty Build.
const NO_BUILD: &str = "";

/// The recovery of `before` run by its mesh-primary `mp`, in the submit's own trace `run`
/// (node-recovery.md "Execution", Q342): `mp` submits it (`via-node-recovery`, the root) and runs it
/// itself, its own workflow: node.delete of the exited birth, then node.create at its path and name,
/// each under the submit. It is never a Build: no Build call (op 0x20) and no claim call (op 0x1C) in
/// its run, no section call and no Build's open of it.
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
    for name in ["rdm.node_admin.build.serve.via-section-run", "rdm.node_admin.build.serve.via-own-section", "rdm.node_admin.build.update.via-dispatched"] {
        assert!(named(run, name).is_empty(), "the recovery is no Build: {name} in its run");
    }
}

/// The recovery of the exited birth `before`, as the records hold it (node-recovery.md, Q342): the
/// exited birth's record written deleted, and the investigation's `rdm.node.investigated` event for that
/// birth on the fabric's store root (build.md §5), whose `next` is the re-create. The record and the event.
async fn recovery_recorded(estate: &Estate, before: &Value) -> (Value, Value) {
    let node_id = s(&before["node_id"]);
    wait_for(&format!("{node_id}'s record deleted and its rdm.node.investigated event"), Duration::from_secs(30), || async {
        let record = estate.node_record(&node_id).filter(|r| r["data"]["state"] == "deleted")?;
        let event = build_log::node_investigated(estate, &node_id).into_iter().rev().find(|e| e["data"]["incarnation"] == before["incarnation"])?;
        Some((record, event))
    })
    .await
}

/// CONTRACT: a node whose mesh transport stops for good exits (reserved code 4, recorded in
/// its data dir). Its mesh-primary (node-recovery.md), not the fabric-primary, proves the exit:
/// it marks the node TRUE OFFLINE, asks the provider about the exact runtime, and on Exited runs the
/// recovery itself, its own workflow keyed on the exited birth (node-recovery.md "Execution", Q342):
/// node.delete of the exited instance (node.deleted from the exited verdict, once, named by the exact
/// birth), then node.create at the same (path, name), and nothing after it. Each step is recorded on
/// the node record; no Build is written or called. The path comes back with a NEW node.id,
/// incarnation and `endpoint_id` (Q358), over a newly allocated data dir: a `node`'s storage is
/// `ephemeral` (Q359). No other node changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_stopped_exit_is_restarted_as_the_same_node() {
    let (estate, birth_build) =
        two_mesh_estate("a_transport_stopped_exit_is_restarted_as_the_same_node").await;
    // The node lives in the mesh that does NOT hold the fabric-primary: its own mesh-primary runs it.
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh = if rdm_test::estate::mesh_of(&primary) == "mesh1" { "mesh2" } else { "mesh1" };
    let node = Estate::path(&format!("{mesh}.node"), "1");
    let node_path = &node;
    let mesh_primary = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == mesh && n["type"] == "node-admin" && n["is_primary"] == true).expect("the node's mesh-primary").clone();
    let before = estate.node_value(node_path).await;
    let data_dir = estate.data_dir(node_path).await;
    let others: Vec<(PathName, String)> = nodes.iter().filter(|n| !rdm_test::estate::is_location(&n, &node_path)).map(|n| (location(n), s(&n["node_id"]))).collect();

    // The real transport-stop path: the testkit service marks the transport stopped; the process exits through it.
    let builds = estate.build_ids().await;
    let marked = estate.probe(&["stop-transport", "--target", &rdm_test::estate::path_target(node_path), "--key", "x"]);
    assert_eq!(marked["outcome"], "Reply", "{marked}");

    let after = until_node_where(&estate, node_path, &format!("{node_path:?} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_ne!(after["node_id"], before["node_id"], "recovery creates a new node.id");
    assert_ne!(after["incarnation"], before["incarnation"], "and a new incarnation");
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new endpoint_id (Q358)");
    assert_ne!(estate.data_dir(node_path).await, data_dir, "a newly allocated data dir: the node's storage is ephemeral (Q359)");
    assert_ne!(after["runtime"]["deployment_id"], before["runtime"]["deployment_id"], "a new runtime");

    // The mesh-primary proved the exit and submitted the recovery, keyed on the exited birth.
    let recovery = recovery_span(&estate, node_path, &before["node_id"]).await;
    assert_eq!(
        s(&recovery["attributes"]["submitter"]),
        s(&mesh_primary["node_id"]),
        "the node's own mesh-primary submits: {recovery}"
    );
    assert_eq!(
        recovery["attributes"]["incarnation_id"], before["incarnation"],
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
    assert_eq!(record["data"]["node_id"], before["node_id"], "{record}");

    // Each process exports its own spans: the new node's boot span is read once the new node exported it.
    let spans = wait_for("the recovery's create, the exited birth's target and the new node's boot span are exported", SETTLE, || async {
        let spans = estate.spans();
        let run = in_trace(&spans, &recovery);
        (recovery_create_span(&run, node_path).is_some() && !exited_birth_spans(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // The recovery's own run, one trace from the mesh-primary's submit: its delete and its create.
    let run = in_trace(&spans, &recovery);
    assert_mesh_primary_ran_the_recovery(&run, &mesh_primary, &before);
    // Delete, then create: the exited birth departs once, named by its exact birth, before the new node boots.
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation"], "named by the exact exited birth");
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).expect("the new node's boot span");
    assert!(start(deleted[0]) < start(boot), "the exited birth is deleted (targeted) before the new node joins");
    // node-recovery.md "Delete blocks the old instance before the new join": the target is the
    // create's, run by the executor that launches the new node, inside the create's own operation and
    // before that launch. A target run after the delete's Complete is left
    // undone when that executor is lost before it, and a later create launches untargeted.
    let create = recovery_create_span(&run, node_path).expect("the recovery's create operation span");
    let targets = exited_birth_spans(&spans, &before);
    assert_eq!(targets.len(), 1, "the exited birth is targeted once: {targets:?}");
    assert!(start(create) <= start(targets[0]) && end(targets[0]) <= end(create), "the target runs inside the create operation: {} vs {create}", targets[0]);
    assert!(end(targets[0]) <= start(boot), "the target is done before the new node boots: {} vs {boot}", targets[0]);
    for step in ["StopNode", "AwaitNodeLeft"] {
        assert!(step_span(&run, NO_BUILD, node_path, step).is_none(), "no RPC to the exited process: {step} ran");
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
/// the same (path, name) (`node.create:<path>`), each as its own workflow's
/// pipeline and node operation span; no node.update follows (proposal node-properties-own-commands.md
/// §5 item 8).
/// The path comes back with a NEW node.id, incarnation and `endpoint_id` (Q358) over a newly
/// allocated data dir, a `node`'s storage being `ephemeral` (Q359); no rename and no storage
/// hand-over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn any_other_unplanned_exit_is_recovered_as_a_new_node_with_a_new_endpoint() {
    let (estate, birth_build) = estate("any_other_unplanned_exit_is_recovered_as_a_new_node_with_a_new_endpoint").await;
    let node = &Estate::path("mesh1.node", "3");
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh_primary = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh1" && n["type"] == "node-admin" && n["is_primary"] == true).expect("the mesh-primary").clone();
    assert_eq!(mesh_primary["node_id"], primary["node_id"], "one mesh: its primary holds the fabric-primary role too");
    let before = estate.node_value(node).await;
    let data_dir = estate.data_dir(node).await;
    let others: Vec<(PathName, String)> = nodes.iter().filter(|n| !rdm_test::estate::is_location(&n, &node)).map(|n| (location(n), s(&n["node_id"]))).collect();
    let builds = estate.build_ids().await;
    estate.kill_node(node).await;

    let after = until_node_where(&estate, node, &format!("{node:?} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_ne!(after["incarnation"], before["incarnation"], "a new incarnation");
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new endpoint_id (Q358)");
    assert_ne!(estate.data_dir(node).await, data_dir, "a newly allocated data dir: the node's storage is ephemeral (Q359)");

    let recovery = recovery_span(&estate, node, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), s(&mesh_primary["node_id"]), "{recovery}");
    assert_eq!(s(&recovery["attributes"]["mesh"]), "mesh1", "{recovery}");
    assert_eq!(recovery["attributes"]["incarnation_id"], before["incarnation"], "keyed on the exact exited birth: {recovery}");
    assert_eq!(s(&recovery["attributes"]["verdict"]), "exited", "on the verdict exited: {recovery}");
    assert_eq!(s(&recovery["attributes"]["outcome"]), "complete", "the recovery's end: {recovery}");
    let (_, event) = recovery_recorded(&estate, &before).await;
    assert_eq!(event["data"]["verdict"], "exited", "the investigation's verdict on the record: {event}");

    // Each process exports its own spans: the successor's boot span is read once the successor exported it.
    let spans = wait_for("the recovery's create, the exited birth's target and the successor's boot span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let run = in_trace(&spans, &recovery);
        (recovery_create_span(&run, node).is_some() && !exited_birth_spans(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // The recovery's own run, in the submit's trace.
    let run = in_trace(&spans, &recovery);
    assert_mesh_primary_ran_the_recovery(&run, &mesh_primary, &before);
    let mut last = 0u64;
    for step in ["NodeDeleting", "NodeDeleted", "RemoveTopologyMembership", "DeployRuntime", "WaitForNodeReady"] {
        let sp = step_span(&run, NO_BUILD, node, step).unwrap_or_else(|| panic!("the recovery's {step} step span"));
        assert!(start(sp) >= last, "{step} runs after the step before it");
        last = start(sp);
    }
    for step in ["StopNode", "AwaitNodeLeft"] {
        assert!(step_span(&run, NO_BUILD, node, step).is_none(), "no RPC to the exited process: {step} ran");
    }
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "node.deleted of the exited birth, exactly once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation"], "named by the exact exited birth");
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).expect("the new node's boot span");
    assert!(start(deleted[0]) < start(boot), "the exited birth is deleted (targeted) before the new node joins");
    // node-recovery.md "Delete blocks the old instance before the new join": the target is the
    // create's, run by the executor that launches the new node, inside the create's own operation and
    // before that launch. A target run after the delete's Complete is left
    // undone when that executor is lost before it, and a later create launches untargeted.
    let create = recovery_create_span(&run, node).expect("the recovery's create operation span");
    let targets = exited_birth_spans(&spans, &before);
    assert_eq!(targets.len(), 1, "the exited birth is targeted once: {targets:?}");
    assert!(start(create) <= start(targets[0]) && end(targets[0]) <= end(create), "the target runs inside the create operation: {} vs {create}", targets[0]);
    assert!(end(targets[0]) <= start(boot), "the target is done before the new node boots: {} vs {boot}", targets[0]);
    // Each operation is its workflow: its node operation span and its own pipeline, never a recovery
    // pipeline of its own.
    for (op, pipeline) in [("delete", "delete"), ("create", "create")] {
        let name = format!("rdm.node_admin.node.{op}.via-node-recovery");
        let at = named(&run, &name).into_iter().find(|sp| at_location(sp, node)).unwrap_or_else(|| panic!("the recovery's {name} span"));
        assert_eq!(at["attributes"]["node_id"], before["node_id"], "{name} names the exited birth it acts for: {at}");
        assert!(
            named(&run, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == NO_BUILD && at_location(sp, node) && sp["attributes"]["pipeline"] == pipeline),
            "the {pipeline} workflow's pipeline span"
        );
    }
    assert!(!named(&spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| sp["attributes"]["pipeline"] == "recover"), "no recovery pipeline of its own");
    assert!(
        !named(&run, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| at_location(sp, node) && sp["attributes"]["pipeline"] == "update"),
        "no node.update after the create: the new node writes what the recovery keeps from its launch snapshot"
    );
    assert_eq!(estate.build_ids().await, builds, "no Build was minted");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    for (name, id) in others {
        assert_eq!(s(&estate.node_value(&name).await["node_id"]), id, "{name:?} is untouched");
    }
    estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md "Detection and the verdict": "A recovery that ends failed is run again by
/// the next check that finds the node still lost; its calls retry only under their `Send` (Q344)";
/// "Failure and receipts": keyed on the exited birth, never a second recovery; D48): the directory a
/// member's new data dir is allocated in is made read-only and the member is killed. Its recovery's
/// node.delete completes (the exited birth departs, node.deleted once) and its node.create refuses by
/// name at PrepareStorage, so the recovery ends failed. While the directory stays read-only the
/// mesh-primary runs that recovery again on each next tick, though no view holds the birth any longer
/// (the recovery owed in its memory, D48): each run starts within a tick of the failed one's end, with
/// no wait of its own growing between them, and fails by name again. The directory is made writable
/// again, the next run completes, the path comes back as a new node, and no recovery of the birth runs
/// after it. Must NOT happen: the failed recovery left for good (nothing at the path), a wait of its
/// own between two runs, two runs at once, a run after the one that completed, a second node.deleted of
/// the exited birth, a Build minted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_whose_create_failed_after_its_delete_is_run_again_by_the_next_tick_and_completes() {
    let (estate, birth_build) = estate("a_recovery_whose_create_failed_after_its_delete_is_run_again_by_the_next_tick_and_completes").await;
    let node = &Estate::path("mesh1.node", "2");
    let before = estate.node_value(node).await;
    let data_dir = estate.data_dir(node).await;
    // A `node`'s storage is ephemeral: its re-create is allocated a new data dir beside the old one.
    use std::os::unix::fs::PermissionsExt;
    let root = Path::new(&data_dir).parent().expect("the data root").to_path_buf();
    let mode = std::fs::metadata(&root).unwrap().permissions().mode();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o555)).unwrap();
    let builds = estate.build_ids().await;
    estate.kill_node(node).await;

    let submits = |spans: &[Value]| -> Vec<Value> { named(spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    // Three runs that fail by name, the directory read-only throughout.
    let failed_runs = |spans: &[Value]| -> Vec<Value> {
        let mut v: Vec<Value> = submits(spans).into_iter().filter(|sp| s(&sp["attributes"]["outcome"]).starts_with("failed")).collect();
        v.sort_by_key(start);
        v
    };
    let failed = wait_for("the recovery's create fails at PrepareStorage three times", SETTLE, || async { let f = failed_runs(&estate.spans()); (f.len() >= 3).then_some(f) }).await;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(mode)).unwrap();
    for run in &failed {
        let outcome = s(&run["attributes"]["outcome"]);
        assert!(outcome.contains("PrepareStorage") && outcome.contains(&root.display().to_string()), "the create refused the data dir it could not write, by name: {run}");
    }
    assert!(failed[1..].iter().all(|sp| s(&sp["attributes"]["again"]) == "true"), "each later run is the failed recovery run again: {failed:#?}");
    // No wait of its own: each run starts on the tick after the failed one ended, never beside it.
    let gossip = rdm_core::cadence::gossip_interval();
    let gaps: Vec<Duration> = failed.windows(2).map(|p| Duration::from_nanos(start(&p[1]).saturating_sub(end(&p[0])))).collect();
    assert!(failed.windows(2).all(|p| start(&p[1]) >= end(&p[0])), "never two runs of the birth at once: {failed:#?}");
    assert!(gaps.iter().all(|g| *g <= gossip * 2), "each run again starts within a tick of the failed one's end ({gossip:?}): gaps {gaps:?}");

    let after = until_node_where(&estate, node, &format!("{node:?} ready as a new node after its recovery failed"), Duration::from_secs(60), |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new endpoint_id (Q358)");
    let done = wait_for("the completed submit of the exited birth is exported", Duration::from_secs(60), || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("complete")) }).await;
    assert_eq!(done["attributes"]["incarnation_id"], before["incarnation"], "keyed on the exact exited birth: {done}");
    assert_eq!(s(&done["attributes"]["outcome"]), "complete", "{done}");
    assert_eq!(s(&done["attributes"]["again"]), "true", "the submit runs again the recovery that failed, owed in its mesh-primary's memory (D48): {done}");
    assert_eq!(s(&done["attributes"]["mesh"]), "mesh1", "{done}");
    assert_eq!(estate.build_ids().await, builds, "no Build was minted");
    // Once it completed, nothing runs it again: the node stands at the path.
    tokio::time::sleep(rdm_core::cadence::staleness_floor()).await;
    let spans = estate.spans();
    let after_done: Vec<Value> = submits(&spans).into_iter().filter(|sp| start(sp) >= end(&done)).collect();
    assert!(after_done.is_empty(), "no recovery of the birth runs after the one that completed: {after_done:#?}");
    eprintln!("the recovery of {node:?} failed {} times by name, gaps {gaps:?} after each, then completed; trace {}", failed.len(), s(&done["trace_id"]));
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
        let terminate = step_span(&run, NO_BUILD, node, "TerminateRuntime").filter(|sp| sp["attributes"]["outcome"] == "complete").cloned()?;
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

/// The `deployment.update.via-step` span of `step` for `node` in `build` (the empty Build for a recovery).
/// The recovery's node.create span of `node` (the primary's own workflow, never a Build: node-recovery.md,
/// Q342): exported once the create, a recovery's last operation, ended.
fn recovery_create_span<'a>(spans: &'a [Value], node: &PathName) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.node.create.via-node-recovery").into_iter().find(|sp| at_location(sp, node))
}

fn step_span<'a>(spans: &'a [Value], build: &str, node: &PathName, step: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == build && at_location(sp, node) && s(&sp["attributes"]["step"]) == step)
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().expect("a span start")
}

fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().expect("a span end")
}

fn alive(pid: u64) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
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
    let mesh = s(&rdm_test::estate::mesh_of(&before));
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
/// again"; node-investigation.md Caller: "its mesh-primary for a member of any type"; Q181: a node cut
/// off is recovered): mesh1 holds two node-admins, mesh1.node-admin.1
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
    let mp = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).cloned().expect("mesh2's primary");
    let mp_name = location(&mp);
    let standby_row = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin" && n["node_id"] != mp["node_id"]).expect("mesh2's standby admin").clone();
    let standby = location(&standby_row);
    let successor = s(&standby_row["node_id"]);
    assert_ne!(rdm_test::estate::mesh_of(&fp_row), "mesh2", "the fabric-primary is in mesh1: {fp_row}");

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
    // The new birth at mesh2.node-admin.1 is created by mesh2's mesh-primary, the successor, in its
    // recovery of the hung birth; it writes its own record at its start (Q356). No second recovery of
    // the hung birth.
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == mp["node_id"]).collect();
    assert!(recoveries.iter().all(|sp| s(&sp["attributes"]["submitter"]) == successor), "the hung birth is recovered by {standby:?} alone: {recoveries:#?}");
    let created: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["created_node_id"] == after["node_id"]).collect();
    assert!(!created.is_empty() && created.iter().all(|c| recoveries.iter().any(|r| r["trace_id"] == c["trace_id"])), "the new birth at {mp_name:?} is created by {standby:?}'s recovery, never by {fp}: {created:#?}");
    let (writer, _) = estate.record_lines(&serde_json::from_value(after["node_id"].clone()).expect("the new birth's node id")).into_iter().next().expect("the new birth's record");
    assert_eq!(writer, s(&after["node_id"]), "the new birth writes its own record's first line");
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
    assert_eq!((rdm_test::estate::mesh_of(&fp).as_str(), fp["is_primary"].as_bool()), (Some("mesh1"), Some(true)), "the Day-0 admin is mesh-primary and fabric-primary: {fp}");
    let standby_row = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh1" && n["type"] == "node-admin" && n["node_id"] != fp["node_id"]).expect("mesh1's other admin").clone();
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
/// submitted by the stopped primary, or a proven-drift Build for the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_handover_mid_ladder_continues_the_silence_window_and_completes_the_recovery() {
    let (estate, birth_build) = fabric_of("a_mesh_primary_handover_mid_ladder_continues_the_silence_window_and_completes_the_recovery", &[("mesh1", 1, 1), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let floor = rdm_core::cadence::staleness_floor();
    let nodes = estate.node_values().await;
    let fp_row = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    assert_eq!(rdm_test::estate::mesh_of(&fp_row), "mesh1", "the day-0 admin keeps the fabric-primary role: {fp_row}");
    // Each admin by node id, as the spans name an actor, and by location for its own Stop.
    let mp_row = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).expect("mesh2's primary").clone();
    let standby_row = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin" && n["node_id"] != mp_row["node_id"]).expect("mesh2's second admin").clone();
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
            .find(|sp| sp["attributes"]["node_id"] == before["node_id"] && s(&sp["attributes"]["outcome"]) == "complete")
            .cloned()
    })
    .await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), standby, "the new mesh-primary submitted the recovery: {recovery}");
    assert_eq!(s(&recovery["attributes"]["mesh"]), "mesh2", "mesh2's recovery, run by the new mesh-primary itself: {recovery}");
    let after = until_node_where(&estate, node, &format!("{node:?} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a re-created node always gets a new endpoint_id (Q358)");

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
    assert!(drift.is_empty(), "the fabric-primary opened a drift Build for a member: {drift:?}");
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

/// A caller outside the fabric that reaches the node-admin `admin` by its exact birth: its own
/// endpoint, and a resolver holding that birth as the answering admin's view names it.
async fn caller_of(admin: &Value) -> (std::sync::Arc<rdm_transport::NodeRpcClient>, rdm_core::NodeId) {
    use rdm_transport::{NodeRpcClient, ResolvedNode, StaticResolver};
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller binds an endpoint");
    let resolver = std::sync::Arc::new(StaticResolver::new());
    let node_id: rdm_core::NodeId = s(&admin["node_id"]).parse().expect("a node id");
    resolver.insert(ResolvedNode { mesh: s(&rdm_test::estate::mesh_of(&admin)), r#type: (admin["type"].as_str().unwrap()).to_string(),
        node_id: node_id.clone(),
        path: s(&admin["path"]),
        name: s(&admin["name"]),
        endpoint_id: s(&admin["endpoint_id"]).parse().expect("an iroh key"),
        transport_addr: s(&admin["transport_addr"]).parse().expect("a transport address"),
        incarnation: rdm_core::IncarnationId::parse(&s(&admin["incarnation"])).unwrap(),
    });
    (std::sync::Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), node_id)
}

/// CONTRACT: a recovered node keeps the config and the tags its mesh-primary told its predecessor
/// (node-recovery.md "Retained: ... config"; Q356: a node writes its own record, its mesh-primary only
/// a lost, killed or deleted node's; a recovery's launch snapshot is the predecessor's record). The
/// caller puts `<mesh>.rpc.1`'s config and the tag `rabbit` through its mesh-primary (not the
/// fabric-primary), the node writes them on its own record and carries them, then its runtime is
/// killed: the mesh-primary's recovery writes the exited birth's record Deleted from its latest line,
/// config and tag kept, and creates the new node (new node.id, same (path, name)) with the exited
/// record's config and tag as its launch snapshot; the new node writes them on its own record, carries
/// them, and `node.get` by the tag names the new node and not the exited one. Nothing copies them
/// after the create: no node.update runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_node_keeps_the_config_and_tags_its_mesh_primary_told_its_predecessor() {
    let (estate, _build) = two_mesh_estate("a_recovered_node_keeps_the_config_and_tags_its_mesh_primary_told_its_predecessor").await;
    let nodes = estate.node_values().await;
    let (node, mesh_primary, fp) = member_and_primaries(&nodes);
    let node_path = &node;
    let before = estate.node_value(node_path).await;
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let node_id: rdm_core::NodeId = s(&before["node_id"]).parse().unwrap();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node_id.clone()), node_id, incarnation: rdm_core::IncarnationId::parse(&s(&before["incarnation"])).unwrap() };
    // node-config-update.md step 1: the caller sends it to the fabric-primary, which sends it down.
    let (fp_client, fp_id) = caller_of(&fp).await;
    let reply = rdm_client::NodeRpc::new(&fp_client)
        .config_update(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, told.clone(), &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.config.update of {node_path:?} at the fabric-primary: {e:?}"));
    assert_eq!(reply.name(), "config-updated", "the node wrote the config on its own record and applied it: {reply:?}");
    let told_json = json!({"tier": "gold"});
    wait_for(&format!("{node_path:?} carries its config"), Duration::from_secs(30), || async { (estate.node_value(node_path).await["config"] == told_json).then_some(()) }).await;
    let labels: std::collections::BTreeSet<String> = ["rabbit".to_string()].into();
    // Commands go down: the caller sends it to the fabric-primary, which sends it to the mesh-primary (Q203).
    let tagged = rdm_client::NodeRpc::new(&fp_client)
        .tags_create(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, &labels, &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.tag.create of {node_path:?} at the fabric-primary: {e:?}"));
    assert_eq!(tagged.name(), "tags-created", "the node wrote the label on its own record: {tagged:?}");
    wait_for(&format!("{node_path:?} carries its tag"), Duration::from_secs(30), || async { (estate.node_view(node_path).await.tags == labels).then_some(()) }).await;

    estate.kill_node(node_path).await;
    let after = until_node_where(&estate, node_path, &format!("{node_path:?} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    // The recovery's create of the new node, by the mesh-primary.
    let created = wait_for("the recovery's create is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.node.create.via-node-recovery").into_iter().find(|sp| sp["attributes"]["created_node_id"] == after["node_id"]).cloned()
    })
    .await;
    assert_eq!(created["attributes"]["node_id"], before["node_id"], "the recovery of the exited node creates the new one: {created}");
    let deleted: Vec<Value> = named(&estate.spans(), "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect();
    assert_eq!(deleted.len(), 1, "one Deleted record line of the exited birth: {deleted:?}");
    // The record and its config stay (node-delete.md step 3): the exited node's record, at its node id,
    // written Deleted by the mesh-primary (the node being gone, Q356), still holds the config and the tag.
    let exited_id: rdm_core::NodeId = s(&before["node_id"]).parse().unwrap();
    let exited = estate.record(&exited_id).expect("the exited node's record stays");
    assert_eq!((exited.state, exited.config.clone(), exited.tags.clone()), (rdm_core::model::NodeStatus::Deleted, told.clone(), labels.clone()), "the exited node's record keeps its config and tag: {exited:?}");
    let (deleted_by, _) = estate.record_lines(&exited_id).pop().expect("the exited node's record");
    assert_eq!(deleted_by, s(&mesh_primary["node_id"]), "its Deleted line is its mesh-primary's");
    // The new node writes its own record from its launch snapshot, the exited birth's acknowledged
    // config and tags (node-recovery.md; its join answer is not where config lives).
    let own = wait_for("the new node's own record", Duration::from_secs(30), || async { estate.record(&s(&after["node_id"]).parse().unwrap()) }).await;
    assert_eq!((own.config.clone(), own.tags.clone()), (told.clone(), labels.clone()), "the new node's own record holds the exited birth's config and tag: {own:?}");
    let carried = wait_for(&format!("the new {node_path:?} carries the config"), Duration::from_secs(30), || async { let n = estate.node_value(node_path).await; (n["config"] == told_json).then_some(n) }).await;
    assert_eq!(carried["node_id"], after["node_id"], "the new node, not the exited one, carries it");
    assert_eq!(estate.node_view(node_path).await.tags, labels, "the new node's record carries the exited node's tag");
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
    eprintln!("trace {}: via-node-row delete node_id={}; trace {}: via-node-recovery create created_node_id={}", s(&deleted[0]["trace_id"]), s(&before["node_id"]), s(&created["trace_id"]), s(&after["node_id"]));
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
    let (mut estate, build) = fabric_of(
        "a_member_exit_while_the_fabric_primary_is_stopped_is_recovered_through_its_successor",
        &[("mesh1", 2, 1), ("mesh2", 1, 2)],
        Duration::from_secs(90),
    )
    .await;
    let nodes = estate.node_values().await;
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    let fp_name = location(&fp);
    assert_eq!(rdm_test::estate::mesh_of(&fp), "mesh1", "the day-0 admin holds the fabric-primary role: {fp_name:?}");
    let successor = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh1" && n["type"] == "node-admin" && n["node_id"] != fp["node_id"]).expect("mesh1's other node-admin").clone();
    let successor_name = location(&successor);
    let mp_row = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).expect("mesh2's primary").clone();
    let mp = s(&mp_row["node_id"]);

    // The fabric-primary is stopped by a caller's node command: its roles move to mesh1's other
    // node-admin, then it stops.
    let mut stop = estate.nodes_obj().await.stop(&fp_name).await.unwrap_or_else(|e| panic!("node.stop of the fabric-primary {fp_name:?} was refused: {e}"));
    Estate::complete(&mut stop).await;
    let stopped = estate
        .until_view(&format!("{fp_name:?} is stopped and {successor_name:?} holds the fabric-primary role"), SETTLE, |views| {
            let old = views.iter().find(|n| n.location() == fp_name)?;
            let new = views.iter().find(|n| n.location() == successor_name)?;
            (old.state == NodeStatus::Stopped && !old.is_fabric_primary && new.is_fabric_primary).then(|| serde_json::to_value(old).expect("a view serializes"))
        })
        .await;
    assert_eq!(stopped["incarnation"], fp["incarnation"], "the stopped fabric-primary is the same birth, stopped: {stopped}");
    // A stopped node answers only the Status family (node-rpc.md §16, the ConnectionsDeleting guard): the
    // Build reads go to the fabric-primary now, its successor.
    estate.enter(&successor_name).await;

    // With the fabric-primary stopped, a member of mesh2 exits.
    let node = &Estate::path("mesh2.node", "1");
    let before = estate.node_value(node).await;
    let builds = estate.build_ids().await;
    estate.kill_node(node).await;
    let after = until_new_birth(&estate, node, &before, SETTLE).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new endpoint_id (Q358)");
    let submits = |spans: &[Value]| -> Vec<Value> { named(spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let done = wait_for("the completed recovery submit is exported", Duration::from_secs(60), || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]) == "complete") }).await;
    assert_eq!((s(&done["attributes"]["submitter"]), s(&done["attributes"]["mesh"])), (mp.clone(), "mesh2".to_string()), "mesh2's primary runs it: {done}");
    let spans = wait_for("the recovery's create is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        recovery_create_span(&in_trace(&spans, &done), node).is_some().then_some(spans)
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
    assert_eq!((still["state"].clone(), still["incarnation"].clone()), (json!("stopped"), fp["incarnation"].clone()), "{fp_name:?} stayed stopped: {still}");
    recovery_recorded(&estate, &before).await;
    eprintln!("member exit with the fabric-primary {fp_name:?} stopped, role at {successor_name:?}; {node:?} recovered by {mp}: trace {}", s(&done["trace_id"]));
    estate.record_trace_url(done["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}

/// `pid`'s environment, by name, from `/proc/<pid>/environ`.
fn environ_of(pid: u64) -> std::collections::BTreeMap<String, String> {
    let raw = std::fs::read(format!("/proc/{pid}/environ")).unwrap_or_else(|e| panic!("pid {pid}: /proc environ: {e}"));
    raw.split(|b| *b == 0).filter_map(|e| std::str::from_utf8(e).ok()?.split_once('=').map(|(k, v)| (k.to_string(), v.to_string()))).collect()
}

/// The mesh-primary's `PrepareStorage` step of the create of `node_id` at `node`, once exported: what
/// data dir it launched the node on, whether it was the predecessor's, and the new `endpoint_id`.
async fn prepared_storage(estate: &Estate, node: &PathName, endpoint_id: &Value) -> Value {
    wait_for(&format!("the re-create's PrepareStorage span for {node:?} is exported"), Duration::from_secs(60), || async {
        named(&estate.spans(), "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .find(|sp| at_location(sp, node) && sp["attributes"]["step"] == "PrepareStorage" && sp["attributes"]["endpoint_id"] == *endpoint_id)
            .cloned()
    })
    .await
}

/// CONTRACT (storage.md §1 "Persistence belongs to the logical `path` and `name`, not its node.id":
/// "A node re-created at the same `path` and `name` with `persistent` storage is launched on its
/// predecessor's data dir when that directory exists; with `ephemeral` storage, or when it does not
/// exist, on a new one ... Its endpoint is always new (Q358)"; node-recovery.md sequence step 3; Q359):
/// in a mesh of two node-admins, a broker (`persistent`) and a gateway (`ephemeral`), each killed
/// outside any workflow is re-created by its mesh-primary on the verdict `exited` with a new node.id,
/// `endpoint_id` and `transport_addr`. The broker is launched on its predecessor's data dir
/// (`RDM_INHERITED_DATA_DIR=1`), the application's file there untouched; the gateway on a new one
/// (`0`); the broker killed again after its data dir is removed, on a new one (`0`). Must NOT happen:
/// a re-created node holding its predecessor's endpoint, an ephemeral node handed its predecessor's
/// directory, a recovery held or refused for what the directory holds or for its absence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_re_created_node_is_launched_on_its_predecessors_data_dir_only_when_its_storage_is_persistent_and_the_dir_exists() {
    let test = "a_re_created_node_is_launched_on_its_predecessors_data_dir_only_when_its_storage_is_persistent_and_the_dir_exists";
    let sha = rdm_test::faults::candidate_sha();
    let estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &rdm_test::faults::binding_set_with_roles(&sha), &sha, &["node", "broker", "gateway", "compute"])
        .await
        .expect("the binding set with the role types is accepted");
    estate.resize(&rdm_client::FabricDesired { fabric: "fabric1".into(), meshes: vec![rdm_client::MeshDesired::of("mesh1", [("node-admin", 2), ("broker", 1), ("gateway", 1)])] }).await;
    let (broker, gateway) = (Estate::path("mesh1.broker", "1"), Estate::path("mesh1.gateway", "1"));
    let want: std::collections::BTreeSet<PathName> = [Estate::path("mesh1.node-admin", "1"), Estate::path("mesh1.node-admin", "2"), broker.clone(), gateway.clone()].into();
    estate.settles_to_named(&want, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    assert_eq!(s(&estate.node_value(&broker).await["storage"]["persistence"]["persistence"]), "persistent", "the broker's storage is persistent");
    assert_eq!(s(&estate.node_value(&gateway).await["storage"]["persistence"]["persistence"]), "ephemeral", "the gateway's storage is ephemeral");

    // 1. The broker (persistent), its data dir there: launched on it, with a new endpoint.
    let before = estate.node_value(&broker).await;
    let dir = estate.data_dir(&broker).await;
    std::fs::write(Path::new(&dir).join("app-data"), b"the broker's").unwrap();
    estate.kill_node(&broker).await;
    let after = until_new_birth(&estate, &broker, &before, SETTLE).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new endpoint_id (Q358)");
    assert_ne!(after["transport_addr"], before["transport_addr"], "a new transport_addr (Q358)");
    assert_eq!(estate.data_dir(&broker).await, dir, "launched on its predecessor's data dir");
    assert_eq!(std::fs::read(Path::new(&dir).join("app-data")).unwrap(), b"the broker's", "the application's data there is untouched");
    let env = environ_of(estate.pid_of(&broker).await);
    assert_eq!((env.get("RDM_DATA_DIR"), env.get("RDM_INHERITED_DATA_DIR").map(String::as_str)), (Some(&dir), Some("1")), "the node is told it runs on its predecessor's data dir");
    let prepared = prepared_storage(&estate, &broker, &after["endpoint_id"]).await;
    assert_eq!((s(&prepared["attributes"]["data_dir"]), s(&prepared["attributes"]["inherited"])), (dir.clone(), "true".to_string()), "PrepareStorage names the inherited data dir: {prepared}");
    let recovery = recovery_span(&estate, &broker, &before["node_id"]).await;
    assert_eq!((s(&recovery["attributes"]["verdict"]), s(&recovery["attributes"]["outcome"])), ("exited".to_string(), "complete".to_string()), "{recovery}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    eprintln!("persistent: {broker:?} {} -> {} on {dir} (inherited), endpoint {} -> {}, transport {} -> {}: recovery trace {}, PrepareStorage trace {}", s(&before["node_id"]), s(&after["node_id"]), s(&before["endpoint_id"]), s(&after["endpoint_id"]), before["transport_addr"], after["transport_addr"], s(&recovery["trace_id"]), s(&prepared["trace_id"]));

    // 2. The gateway (ephemeral): a new data dir.
    let before = estate.node_value(&gateway).await;
    let dir = estate.data_dir(&gateway).await;
    estate.kill_node(&gateway).await;
    let after = until_new_birth(&estate, &gateway, &before, SETTLE).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new endpoint_id (Q358)");
    let new_dir = estate.data_dir(&gateway).await;
    assert_ne!(new_dir, dir, "an ephemeral node is launched on a new data dir");
    assert_eq!(environ_of(estate.pid_of(&gateway).await).get("RDM_INHERITED_DATA_DIR").map(String::as_str), Some("0"), "and told so");
    let prepared = prepared_storage(&estate, &gateway, &after["endpoint_id"]).await;
    assert_eq!((s(&prepared["attributes"]["data_dir"]), s(&prepared["attributes"]["inherited"])), (new_dir.clone(), "false".to_string()), "{prepared}");

    // 3. The broker again, its data dir removed: a new one, and the recovery is not held.
    let before = estate.node_value(&broker).await;
    let dir = estate.data_dir(&broker).await;
    estate.kill_node(&broker).await;
    std::fs::remove_dir_all(&dir).unwrap();
    let after = until_new_birth(&estate, &broker, &before, SETTLE).await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new endpoint_id (Q358)");
    let new_dir = estate.data_dir(&broker).await;
    assert_ne!(new_dir, dir, "a predecessor's data dir that does not exist is never re-made: a new one");
    assert!(!Path::new(&dir).exists(), "the absent directory is not created again");
    assert_eq!(environ_of(estate.pid_of(&broker).await).get("RDM_INHERITED_DATA_DIR").map(String::as_str), Some("0"), "and told so");
    estate.settles_to_named(&want, Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT (storage.md §1 "What is in the data dir is the application's to judge": "the node's
/// `pre_node_ready` answers whether it is usable: ... `Failed` when it is not (Q350, Q359)";
/// node-recovery.md "The data dir": "`Failed` when it is not, which ends the recovery's step failed
/// with the node `Pending`"): a member whose re-created node's `pre_node_ready` answers `Failed` for
/// its data is killed. Its mesh-primary's recovery deletes the exited birth and creates the new node,
/// whose start ends `Failed` with the hook's reason: the recovery's step ends failed naming it, and the
/// new node stays at the path, `Pending`, never Ready. Nothing retries until someone acts: the recovery
/// is not run again (node-recovery.md sequence step 1: never a second recovery; its run on the next
/// tick ends `already-recovered`), so the path holds that one new node. Must NOT happen: the node
/// Ready over data its application refused, the node stopped or deleted for it, a second create.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_re_created_node_whose_pre_node_ready_answers_failed_stays_pending_and_its_recovery_ends_failed() {
    let (estate, _) = estate("a_re_created_node_whose_pre_node_ready_answers_failed_stays_pending_and_its_recovery_ends_failed").await;
    let node = &Estate::path("mesh1.node", "3");
    let before = estate.node_value(node).await;
    // The node running now passed its hook; the knob is read by the next node at the path.
    rdm_test::write_pre_node_ready_knob(&estate.root, node, "failed the data is not usable").unwrap();
    rdm_test::write_pre_node_ready_knob_attempts(&estate.root, node, 1).unwrap();
    estate.kill_node(node).await;

    let submits = |spans: &[Value]| -> Vec<Value> { named(spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let failed = wait_for("the recovery ends failed", SETTLE, || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("failed")) }).await;
    let outcome = s(&failed["attributes"]["outcome"]);
    assert!(outcome.contains("the data is not usable"), "the recovery's step names the hook's reason: {failed}");
    let after = until_node_where(&estate, node, &format!("{node:?} a new node, pending"), Duration::from_secs(60), |n| n["node_id"] != before["node_id"] && n["state"] == "pending").await;
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a new endpoint_id (Q358)");
    // The recovery owed after its failed step is not run again: the node it created stands there.
    let settled = wait_for("the recovery's run on the next tick ends already-recovered", SETTLE, || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]) == "already-recovered") }).await;
    assert_eq!(s(&settled["attributes"]["again"]), "true", "{settled}");
    // It stays Pending, and no second create: nothing makes it Ready over the data its application refused.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let still = estate.node_value(node).await;
    assert_eq!((still["node_id"].clone(), s(&still["state"])), (after["node_id"].clone(), "pending".to_string()), "the new node stays Pending: {still}");
    let spans = estate.spans();
    let creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(creates.len(), 1, "one create of the exited birth, never a second recovery: {creates:?}");
    let failed_submits: Vec<Value> = submits(&spans).into_iter().filter(|sp| s(&sp["attributes"]["outcome"]).starts_with("failed")).collect();
    assert_eq!(failed_submits.len(), 1, "the recovery's step failed once: {failed_submits:?}");
    estate.record_trace_url(failed["trace_id"].as_str().unwrap_or(""));
    eprintln!("pre_node_ready Failed: {node:?} {} -> {} pending; recovery trace {} outcome {outcome}", s(&before["node_id"]), s(&after["node_id"]), s(&failed["trace_id"]));
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
    let floor = rdm_core::cadence::staleness_floor();
    let mut samples = Vec::new();
    let until = std::time::Instant::now() + floor * 2 + Duration::from_secs(30);
    while std::time::Instant::now() < until && alive(pid as u64) {
        let views = estate.node_values().await;
        let old_listed: Vec<String> = views.iter().filter(|n| s(&n["node_id"]) == old_id).map(|n| format!("{}={}", s(&n["name"]), s(&n["state"]))).collect();
        let live_at_path: Vec<String> = views.iter().filter(|n| rdm_test::estate::is_location(&n, &node) && live(n)).map(|n| s(&n["node_id"])).collect();
        samples.push(json!({"t_ms": (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64 - resumed_ns) / 1_000_000, "old_listed": old_listed, "live_at_path": live_at_path, "old_process_alive": alive(pid as u64)}));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    estate.stop().await;
    let spans = estate.spans();
    let start_ns = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let mut named_old: std::collections::BTreeMap<String, usize> = Default::default();
    for sp in spans.iter().filter(|sp| start_ns(sp) >= resumed_ns) {
        let a = sp["attributes"].to_string();
        if a.contains(&old_id) || a.contains(&s(&before["incarnation"])) {
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
    let runs: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-terminate-returned").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == old_id && sp["attributes"]["incarnation_id"] == before["incarnation"]).collect();
    let terminated = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().any(|sp| sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["outcome"] == "complete" && runs.iter().any(|run| descends_from(&spans, sp, run)));
    assert!(terminated, "TerminateRuntime ran under node.terminate of {node:?}@{}: {runs:?} {facts}", s(&before["incarnation"]));
    assert_eq!(last["live_at_path"], json!([s(&after["node_id"])]), "the path keeps one live birth, the re-created one: {facts}");
}

/// CONTRACT (Q207, #3211 red 3: "a FP/MP looks to see if something at that (path, name) exists currently;
/// if it does it should delete the caller"): a mesh2 node is held (SIGSTOP) until mesh2's primary
/// re-creates it as a new birth at its path, then continued: a caller its mesh-primary does not hold
/// (its Digest, or a declaration, refused). Mesh2's primary decides it from its atlas by its (path, name):
/// another birth is there, so the caller is deleted; not the fabric-primary, it verifies at the root
/// with `fabric.atlas.get` first, which agrees; it reports the caller to the fabric-primary for
/// `node.terminate`. One `via-refused-caller` span per decision names its `via`, the
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
    let floor = rdm_core::cadence::staleness_floor();
    let decided = |spans: &[Value]| spans.iter().filter(|sp| sp["name"] == "rdm.node_admin.node.update.via-refused-caller" && sp["attributes"]["node_id"] == old_id.as_str()).map(|sp| sp["attributes"].clone()).collect::<Vec<Value>>();
    let until = std::time::Instant::now() + floor * 2 + Duration::from_secs(30);
    while std::time::Instant::now() < until && (alive(pid as u64) || decided(&estate.spans()).is_empty()) {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let live_at_path: Vec<String> = estate.node_values().await.iter().filter(|n| rdm_test::estate::is_location(&n, &node) && live(n)).map(|n| s(&n["node_id"])).collect();
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
        assert!(d["via"] == "digest" || d["via"] == "declaration", "decided at a service a refused caller comes through, its Digest or its declaration: {facts}");
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
    let member_of = |mesh: &str| nodes.iter().filter(|n| rdm_test::estate::mesh_of(&n) == mesh && n["type"] == "node" && n["is_primary"] != true).map(location).min().unwrap_or_else(|| panic!("an rpc member of {mesh} holding no primary role"));
    let primary_of = |mesh: &str| nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == mesh && n["type"] == "node-admin" && n["is_primary"] == true).cloned().unwrap_or_else(|| panic!("{mesh}'s primary"));
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
