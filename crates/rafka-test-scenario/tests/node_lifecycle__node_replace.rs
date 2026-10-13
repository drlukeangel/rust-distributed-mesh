//! product=mesh, feature=node-lifecycle, subfeature=node-replace, rung=MN, provider=process.
//!
//! A planned replace (Luke R-R1) and node recovery (node-recovery.md, R-RC1). A planned
//! replacement is the next attempt of the accepted Build with a typed action; no Build is minted.
//! A member that exits outside any workflow is recovered by its own mesh-primary on the provider's
//! exact exit proof: node.delete of the exited instance, then node.create at the same path.name
//! over the inherited data dir (same endpoint key, new node.id). A member that is hung but still
//! running is never recovered; a repeat submit is answered, never run twice; an exit during a
//! workflow fails that workflow and the next tick recovers the member.

use crate::build_log;
use rafka_node_admin_client::{BuildFrame, CallEnd, Change, PathName};
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
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

/// Every admin's view at `admins` holds at most one live birth at `path`. An admin that does not answer
/// `node.get` for this Fabric within 2 s (being replaced, a successor not bound yet) is skipped.
async fn at_most_one_live_birth(estate: &Estate, admins: &[PathName], path: &str) {
    for admin in admins {
        let Some(views) = estate.node_views_at(admin).await else { continue };
        let at: Vec<(String, String)> = Estate::values(&views).iter().filter(|n| n["name"] == path && live(n)).map(|n| (s(&n["node_id"]), s(&n["status"]))).collect();
        assert!(at.len() <= 1, "the view of {admin} holds two live births at {path}: {at:?}");
    }
}

/// The node at `name` once `holds` is true of what `node.get` lists there, as the JSON the view
/// serializes to.
async fn until_node_where(estate: &Estate, name: &str, what: &str, within: Duration, holds: impl Fn(&Value) -> bool) -> Value {
    estate
        .until_node(name, what, within, |n| {
            let v = serde_json::to_value(n?).expect("a view serializes");
            holds(&v).then_some(v)
        })
        .await
}

/// The node at `name` once it is listed ready under a node id other than `before`'s.
async fn until_new_birth(estate: &Estate, name: &str, before: &Value, within: Duration) -> Value {
    until_node_where(estate, name, &format!("{name} ready as a new node"), within, |n| ready(n) && n["node_id"] != before["node_id"]).await
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
    let cutover = step_span(spans, build, attempt, node, "RenameSuccessor").expect("RenameSuccessor ran");
    assert!(end(stopped) <= start(rename), "the predecessor is parked before it is renamed: {stopped} vs {rename}");
    assert!(end(stopped) <= start(cutover), "the predecessor is parked before the successor takes {node}");
    let complete = step_span(spans, build, attempt, node, "Complete").expect("Complete ran");
    // Each admin's book drops the parked birth once, on the NodeStopped it hears (the executor's own
    // book on its publish).
    let dropped: Vec<&Value> = named(spans, "rdm.mesh.membership.remove.via-node-stopped")
        .into_iter()
        .filter(|sp| sp["service"] == "rafka-node-admin" && sp["attributes"]["node_id"] == old["node_id"] && sp["attributes"]["was_held"] == "true" && start(sp) < start(complete))
        .collect();
    assert!(dropped.len() >= admins, "each of the {admins} admins dropped the parked {node} from its book on its NodeStopped: {dropped:?}");
}

/// A fabric born at `shape` (each `(mesh, node_admin, rpc_node)`) by `build.create`, followed to its
/// terminal frame, the view settled on it within `settle` and the fabric ready-for-traffic; the birth
/// Build's id. The cells hold, kill and replace members: workflows the initial fabric refuses until it has
/// first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
async fn fabric_of(test: &str, shape: &[(&str, u32, u32)], settle: Duration) -> (Estate, String) {
    born_at(Estate::bootstrap(owner(test), "fabric1", "mesh1").await, shape, settle).await
}

/// [`fabric_of`] with every node-admin the testkit's faulted node-admin (`faults::binding_set`): its
/// fault door (testkit Node RPC op 0x75) holds the admin at a cut the cell arms, and nothing else of it
/// differs.
async fn faulted_fabric_of(test: &str, shape: &[(&str, u32, u32)], settle: Duration) -> (Estate, String) {
    let sha = candidate_sha();
    let estate = Estate::bootstrap_external(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    born_at(estate, shape, settle).await
}

/// `estate`'s fabric born at `shape` by `build.create`, followed to its terminal frame, the view settled
/// on it within `settle` and the fabric ready-for-traffic; the birth Build's id.
async fn born_at(estate: Estate, shape: &[(&str, u32, u32)], settle: Duration) -> (Estate, String) {
    let born = estate.reconcile_shape("fabric1", shape).await;
    estate.settles_to(shape, settle).await;
    estate.until_fabric_ready(SETTLE).await;
    (estate, born.build_id.to_string())
}

/// mesh1 with two node-admins and three rpc nodes, through the rectifier; the birth Build's id.
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
    let dir = estate.data_dir(node).await;
    ExactRuntime::published(Path::new(&dir)).unwrap_or_else(|r| panic!("{node}: no exact runtime published in {dir}: {r:?}"))
}

/// The highest attempt of `mesh`'s drift lane of Build `build` as the Build log of the node-admin `admin`
/// holds it (R-BI7: a recovery is an attempt of its mesh's lane, numbered per mesh from 1); 0 for a lane
/// never opened.
async fn lane_attempt_at(estate: &Estate, admin: &PathName, build: &str, mesh: &str) -> u64 {
    let b = build_log::build_value(&estate.carrier_at(admin).await, build).await;
    b["lanes"][mesh]["attempt"].as_u64().unwrap_or(0)
}

/// `build`'s steps of `mesh`'s drift lane, as the Build view `b` holds them (R-BI7: a recovery's
/// receipts carry its mesh's lane; the Build's own `steps` are the fabric lane's).
fn lane_steps(b: &Value, mesh: &str) -> Vec<Value> {
    b["lanes"][mesh]["steps"].as_array().cloned().unwrap_or_default()
}

/// The spans of the trace `of` is in: one recovery submit's own run (its open, claim, attempt run,
/// reconcile, operations and steps), which an attempt number alone does not name when a drift lane
/// and the fabric lane both hold that number (R-BI7).
fn in_trace(spans: &[Value], of: &Value) -> Vec<Value> {
    spans.iter().filter(|sp| sp["trace_id"] == of["trace_id"]).cloned().collect()
}

/// Whether `sp` is a Node RPC call of `op` (the call span names the op number).
fn calls_op(sp: &Value, op: u8) -> bool {
    sp["attributes"]["op"] == op.to_string().as_str() || sp["attributes"]["op"] == json!(op)
}

/// The completed recovery of `before` at `attempt` of `mesh`'s drift lane of `build` ran on its
/// mesh-primary `mp` alone, in the submit's own trace `run` (R-BI7: "the mesh-primary opens the
/// recovery attempt of the current Build itself: claim on its mesh's rows, then run; no call to the
/// fabric-primary"; node-recovery.md "Observability contract"): the open at `mp`'s own door
/// (`via-recovery`, route `node-recovery`, a child of the submit), the claim decided by `mp` on the
/// mesh's rows (`via-claim-decision`, lane `mesh`, won), the attempt's run (`via-attempt-run`, lane
/// `mesh`, executor `mp`) and its reconcile (`via-reconcile`, lane `mesh`, converged). No Build call
/// (op 0x20), no claim call (op 0x1C) and no fabric-primary dispatch is in the run.
fn assert_mesh_primary_ran_the_recovery(run: &[Value], mp: &str, mesh: &str, build: &str, attempt: &str, before: &Value) {
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let submit = named(run, "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).expect("the submit roots the run");
    let open = named(run, "rdm.node_admin.build.update.via-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).unwrap_or_else(|| panic!("{mp} opened the recovery at its own door"));
    assert_eq!(
        (attr(open, "route"), attr(open, "outcome"), attr(open, "mesh"), attr(open, "build_id"), attr(open, "attempt")),
        ("node-recovery".to_string(), "admitted".to_string(), mesh.to_string(), build.to_string(), attempt.to_string()),
        "the open at the mesh-primary's own door, attempt {attempt} of {mesh}'s lane: {open}"
    );
    assert_eq!(open["parent_span_id"], submit["span_id"], "the open is the submit's own in-process call: {open}");
    let claim = named(run, "rdm.node_admin.build.update.via-claim-decision").into_iter().find(|sp| attr(sp, "attempt") == attempt).unwrap_or_else(|| panic!("{mp}'s claim decision of attempt {attempt}"));
    assert_eq!(
        (attr(claim, "node"), attr(claim, "lane"), attr(claim, "executor"), attr(claim, "outcome")),
        (mp.to_string(), mesh.to_string(), mp.to_string(), "won".to_string()),
        "the mesh-primary decided its own claim on its mesh's rows: {claim}"
    );
    let started = named(run, "rdm.node_admin.build.serve.via-attempt-run").into_iter().find(|sp| attr(sp, "attempt") == attempt).expect("the attempt's run");
    assert_eq!((attr(started, "node"), attr(started, "lane"), attr(started, "executor")), (mp.to_string(), mesh.to_string(), mp.to_string()), "{started}");
    let reconcile = named(run, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|sp| attr(sp, "build_id") == build && attr(sp, "attempt") == attempt).expect("the attempt's reconcile");
    assert_eq!((attr(reconcile, "lane"), attr(reconcile, "executor"), attr(reconcile, "outcome")), (mesh.to_string(), mp.to_string(), "converged".to_string()), "{reconcile}");
    let calls: Vec<&Value> = named(run, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| calls_op(sp, 0x20) || calls_op(sp, 0x1C)).collect();
    assert!(calls.is_empty(), "no Build call and no claim call in the recovery's run: {calls:?}");
    assert!(named(run, "rdm.node_admin.build.update.via-dispatch").is_empty(), "no fabric-primary dispatch in the recovery's run");
}

/// The receipts of the recovery of `node` at `attempt` of `mesh`'s drift lane a Build holds: its
/// node.delete's `retire-node:<node>` NodeDeleted receipt, and the Complete of its last operation,
/// node.create (`create-node:<node>`).
fn recovery_receipts(b: &Value, mesh: &str, node: &str, attempt: &str) -> Option<(Value, Value)> {
    let (delete, create) = (format!("retire-node:{node}"), format!("create-node:{node}"));
    let steps = lane_steps(b, mesh);
    let at = |operation: &str, step: &str| steps.iter().find(|r| r["operation"] == operation && r["step"] == step && r["attempt"].to_string() == attempt && r["outcome"] == "complete").cloned();
    Some((at(&delete, "NodeDeleted")?, at(&create, "Complete")?))
}

/// The fabric-primary's own Build log holds the recovery of `node` at `attempt` of `mesh`'s drift
/// lane, heard on the Build topic (node-recovery.md, R-BI7: the fabric-primary learns it from the
/// Build topic): its node.delete's `retire-node:<node>` NodeDeleted receipt, and the Complete of its
/// last operation, node.create (`create-node:<node>`).
async fn fp_recovery_receipts(estate: &Estate, fp: &PathName, build: &str, mesh: &str, node: &str, attempt: &str) -> (Value, Value) {
    let held = build_log::build_fact(&estate.carrier_at(fp).await, build, &format!("the recovery's receipts in {mesh}'s lane"), Duration::from_secs(30), |p| {
        recovery_receipts(&serde_json::to_value(p).expect("a Build view serializes"), mesh, node, attempt).is_some()
    })
    .await
    .unwrap_or_else(|end| panic!("{end}"));
    recovery_receipts(&serde_json::to_value(held).expect("a Build view serializes"), mesh, node, attempt).expect("the receipts were just held")
}

/// CONTRACT: a rpc node whose mesh transport stops for good exits (reserved code 4, recorded in
/// its data dir). Its mesh-primary (node-recovery.md), not the fabric-primary, proves the exit:
/// it marks the node TRUE OFFLINE, asks the provider about the exact runtime, and on Exited opens
/// one recovery keyed on the exited birth at its own door, as the next attempt of its mesh's lane
/// of the accepted Build (no Build minted), claims it on its mesh's rows and runs it (R-BI7: "the
/// mesh-primary opens the recovery attempt of the current Build itself: claim on its mesh's rows,
/// then run; no call to the fabric-primary"): node.delete of the exited instance (node.deleted from
/// the exit proof, once, named by the exact birth), then node.create at the same path.name over the
/// inherited data dir, and nothing after it. The fabric-primary learns the receipts from the Build
/// topic. The path comes back with the SAME transport key and data dir and a NEW node.id and
/// incarnation. No other node changes, and the fabric-primary's drift check opens nothing for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_stopped_exit_is_restarted_as_the_same_node() {
    let (estate, birth_build) = two_mesh_estate("a_transport_stopped_exit_is_restarted_as_the_same_node").await;
    // The node lives in the mesh that does NOT hold the fabric-primary: its own mesh-primary is the
    // reconciler, and it opens, claims and runs the recovery on its own mesh's rows.
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh = if primary["mesh"] == "mesh1" { "mesh2" } else { "mesh1" };
    let node = format!("{mesh}.rpc.1");
    let node_path = node.as_str();
    let mesh_primary = s(&nodes.iter().find(|n| n["mesh"] == mesh && n["kind"] == "node_admin" && n["is_primary"] == true).expect("the node's mesh-primary")["name"]);
    let before = estate.node_value(node_path).await;
    let data_dir = estate.data_dir(node_path).await;
    let others: Vec<(String, String)> = nodes.iter().filter(|n| n["name"] != node_path).map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();

    // The real transport-stop path: the testkit door marks the transport stopped; the process exits through it.
    let marked = estate.probe(&["stop-transport", "--target", &format!("path:{node_path}"), "--key", "x"]);
    assert_eq!(marked["outcome"], "Reply", "{marked}");

    let after = until_node_where(&estate, node_path, &format!("{node_path} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_ne!(after["node_id"], before["node_id"], "recovery creates a new node.id");
    assert_ne!(after["incarnation_id"], before["incarnation_id"], "and a new incarnation");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same transport key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir(node_path).await, data_dir, "the same data dir");
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
    assert_eq!(s(&recovery["attributes"]["disposition"]), "created", "the mesh-primary's own door opened it: {recovery}");
    assert_eq!(s(&recovery["attributes"]["mesh"]), mesh, "an attempt of the member's mesh's lane: {recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    // The submit followed the recovery to its terminal frame and records it (never "submitted").
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "the recovery's terminal: {recovery}");
    let build = build_log::attempt_complete(&estate.carrier().await, &birth_build, attempt.parse().expect("an attempt"), SETTLE).await;
    assert_eq!(build.build_id.as_str(), birth_build, "no Build was minted: {build:?}");
    // The fabric-primary heard the recovery's receipts on the Build topic (its recovery dedup reads
    // them), in the mesh's lane: the delete's NodeDeleted naming the exited birth, and the create's Complete.
    let heard = fp_recovery_receipts(&estate, &Estate::path(&s(&primary["name"])), &birth_build, mesh, node_path, &attempt).await;
    assert_eq!(heard.0["output"]["node_id"], before["node_id"], "the fabric-primary's NodeDeleted names the exited node: {:?}", heard.0);
    assert_eq!(heard.0["output"]["incarnation"], before["incarnation_id"], "and its exact birth: {:?}", heard.0);
    assert!(heard.1["output"].is_null(), "Complete commits no output: {:?}", heard.1);

    // Each process exports its own spans: the new node's boot span is read once the new node exported it.
    let spans = wait_for("the recovery's last operation, the exited birth's fence and the new node's boot span are exported", SETTLE, || async {
        let spans = estate.spans();
        (create_span(&in_trace(&spans, &recovery), &birth_build, &attempt, node_path).is_some() && !exit_fences(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // The recovery's own run, one trace on the mesh-primary: its open at its own door, its claim on
    // its mesh's rows, the attempt's run and its reconcile, each naming the mesh's lane.
    let run = in_trace(&spans, &recovery);
    assert_mesh_primary_ran_the_recovery(&run, &mesh_primary, mesh, &birth_build, &attempt, &before);
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
    let create = named(&run, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == node_path)
        .expect("the recovery's create operation span");
    let fences = exit_fences(&spans, &before);
    assert_eq!(fences.len(), 1, "the exited birth is fenced once: {fences:?}");
    assert!(start(create) <= start(fences[0]) && end(fences[0]) <= end(create), "the fence runs inside the create operation: {} vs {create}", fences[0]);
    assert!(end(fences[0]) <= start(boot), "the fence is done before the new node with the same key boots: {} vs {boot}", fences[0]);
    for step in ["StopNode", "AwaitNodeLeft", "RenamePredecessor"] {
        assert!(step_span(&run, &birth_build, &attempt, node_path, step).is_none(), "no RPC to the exited process and no .old rename: {step} ran");
    }
    assert!(!named(&spans, "rdm.mesh.node.delete.via-transport-stopped").is_empty(), "the exiting runtime's own span names the reason");
    // The fabric-primary's drift check opens nothing for a member: its mesh-primary recovers it.
    let drift: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| s(&sp["attributes"]["scope"]).contains(&format!("exited: {node_path}"))).collect();
    assert!(drift.is_empty(), "the fabric-primary opened a drift attempt for a member: {drift:?}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));

    // Nothing else moved.
    for (name, id) in others {
        assert_eq!(s(&estate.node_value(&name).await["node_id"]), id, "{name} is untouched");
    }
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md): every other unplanned exit is a recovery like the transport stop.
/// A rpc node killed with SIGKILL leaves no recorded reason: its mesh-primary, which in a one-mesh
/// fabric is also the fabric-primary, marks it TRUE OFFLINE, proves the exit from the provider alone
/// (no exit code) and opens one recovery keyed on the exited birth at its own door, the next attempt
/// of mesh1's lane of the accepted Build (no Build minted; R-BI7: the lane is the mesh's whoever else
/// the mesh-primary is), claimed on mesh1's rows, two operations run by that mesh-primary in order
/// (node-recovery.md "Steps"):
/// node.delete of the exited instance (`retire-node:<path>`: no RPC to it, no stop frames,
/// node.deleted once named by its exact birth and before the new node boots), then node.create at
/// the same path.name over the inherited data dir (`create-node:<path>`), each as its own workflow's
/// pipeline and node operation span; no node.update follows (proposal node-properties-own-commands.md
/// §5 item 8).
/// The path comes back with the SAME transport key and data dir and a NEW node.id and incarnation;
/// no `<path>.old` rename, no storage hand-over, and the fabric-primary's drift opens nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn any_other_unplanned_exit_is_recovered_as_a_new_node_over_the_same_data_dir() {
    let (estate, birth_build) = estate("any_other_unplanned_exit_is_recovered_as_a_new_node_over_the_same_data_dir").await;
    const NODE: &str = "mesh1.rpc.3";
    let nodes = estate.node_values().await;
    let primary = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    let mesh_primary = s(&nodes.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("the mesh-primary")["name"]);
    assert_eq!(mesh_primary, primary, "one mesh: its primary holds the fabric-primary seat too");
    let before = estate.node_value(NODE).await;
    let data_dir = estate.data_dir(NODE).await;
    let others: Vec<(String, String)> = nodes.iter().filter(|n| n["name"] != NODE).map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
    estate.kill_node(NODE).await;

    let after = until_node_where(&estate, NODE, &format!("{NODE} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_ne!(after["incarnation_id"], before["incarnation_id"], "a new incarnation");
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same transport key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir(NODE).await, data_dir, "the same data dir");

    let recovery = recovery_span(&estate, NODE, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "{recovery}");
    assert_eq!(s(&recovery["attributes"]["mesh"]), "mesh1", "an attempt of mesh1's lane: {recovery}");
    assert_eq!(s(&recovery["attributes"]["disposition"]), "created", "its own door opened it: {recovery}");
    assert_eq!(recovery["attributes"]["incarnation_id"], before["incarnation_id"], "keyed on the exact exited birth: {recovery}");
    assert_eq!(s(&recovery["attributes"]["exit_code"]), "", "a killed runtime proves no reason: {recovery}");
    assert_eq!(s(&recovery["attributes"]["exit_proof"]), "none", "{recovery}");
    assert_eq!(s(&recovery["attributes"]["build_id"]), birth_build, "the recovery is an attempt of the accepted Build: {recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "the recovery's terminal: {recovery}");

    // Each process exports its own spans: the successor's boot span is read once the successor exported it.
    let spans = wait_for("the recovery's last operation, its reconcile span, the exited birth's fence and the successor's boot span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let run = in_trace(&spans, &recovery);
        (create_span(&run, &birth_build, &attempt, NODE).is_some() && lane_reconcile_span(&run, &birth_build, "mesh1", &attempt).is_some() && !exit_fences(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // The recovery's own run: the attempt number is mesh1's lane's, which the fabric lane's numbering
    // also holds, so its spans are read in the submit's trace.
    let run = in_trace(&spans, &recovery);
    assert_mesh_primary_ran_the_recovery(&run, &mesh_primary, "mesh1", &birth_build, &attempt, &before);
    let reconcile = lane_reconcile_span(&run, &birth_build, "mesh1", &attempt).expect("the attempt's reconcile span");
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("retire-node:{NODE},create-node:{NODE}"), "the recovery's two operations, in order: {reconcile}");
    let mut last = 0u64;
    for step in ["NodeDeleting", "NodeDeleted", "RemoveTopologyMembership", "DeployRuntime", "WaitForNodeReady"] {
        let sp = step_span(&run, &birth_build, &attempt, NODE, step).unwrap_or_else(|| panic!("the recovery's {step} step span"));
        assert!(start(sp) >= last, "{step} runs after the step before it");
        last = start(sp);
    }
    for step in ["StopNode", "AwaitNodeLeft", "RenamePredecessor", "HandoffStorage"] {
        assert!(step_span(&run, &birth_build, &attempt, NODE, step).is_none(), "no RPC to the exited process, no .old rename, no storage hand-over: {step} ran");
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
    let create = named(&run, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE)
        .expect("the recovery's create operation span");
    let fences = exit_fences(&spans, &before);
    assert_eq!(fences.len(), 1, "the exited birth is fenced once: {fences:?}");
    assert!(start(create) <= start(fences[0]) && end(fences[0]) <= end(create), "the fence runs inside the create operation: {} vs {create}", fences[0]);
    assert!(end(fences[0]) <= start(boot), "the fence is done before the new node with the same key boots: {} vs {boot}", fences[0]);
    // Each operation is its workflow: its node operation span and its own pipeline, never a recovery
    // pipeline of its own.
    for (op, pipeline) in [("delete", "retire"), ("create", "create")] {
        let name = format!("rdm.node_admin.node.{op}.via-build");
        let at = named(&run, &name).into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).unwrap_or_else(|| panic!("the recovery's {name} span"));
        assert_eq!(at["attributes"]["node_id"], before["node_id"], "{name} names the exited birth it acts for: {at}");
        assert!(
            named(&run, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE && sp["attributes"]["pipeline"] == pipeline),
            "the {pipeline} workflow's pipeline span"
        );
    }
    assert!(!named(&spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| sp["attributes"]["pipeline"] == "recover"), "no recovery pipeline of its own");
    assert!(
        !named(&run, "rdm.node_admin.deployment.update.via-pipeline").into_iter().any(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE && sp["attributes"]["pipeline"] == "update"),
        "no node.update after the create: the create's claim seeded what the recovery keeps"
    );
    let drift: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| s(&sp["attributes"]["scope"]).contains(&format!("exited: {NODE}"))).collect();
    assert!(drift.is_empty(), "the fabric-primary opened a drift attempt for a member: {drift:?}");
    assert!(nodes_named_old(&estate.node_values().await).is_empty(), "no <path>.old identity");
    let build = build_log::build_value(&estate.carrier().await, &birth_build).await;
    assert_eq!(build["build_id"], birth_build.as_str(), "no Build was minted: {build}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    for (name, id) in others {
        assert_eq!(s(&estate.node_value(&name).await["node_id"]), id, "{name} is untouched");
    }
    estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md "Failure and receipts": the mesh-primary is the submitter that submits
/// again; sequence step 1: keyed on the exited birth, a repeat answers AlreadyApplied or
/// BuildInProgress, never a second recovery; node-rpc-envelope.md "Caller execution and
/// certainty": a later attempt resumes from the recorded receipts, never from step 1): a member's
/// data dir loses its node-key and the member is killed. Its recovery's node.delete completes (the
/// exited birth departs, node.deleted once) and its node.create refuses by name at PrepareStorage,
/// so the attempt ends Failed. The key is put back. The mesh-primary submits the same exited birth
/// again on a later tick, though no view holds that birth any longer: a later attempt of mesh1's lane
/// of the same Build (R-BI7) runs only the create, and the path comes back as a new
/// node over the same data dir and endpoint key. Must NOT happen: the failed recovery left for good
/// (nothing at the path), a second node.delete of the exited birth, a Build minted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_whose_create_failed_after_its_delete_is_submitted_again_and_completes() {
    let (estate, birth_build) = estate("a_recovery_whose_create_failed_after_its_delete_is_submitted_again_and_completes").await;
    const NODE: &str = "mesh1.rpc.2";
    let before = estate.node_value(NODE).await;
    let data_dir = estate.data_dir(NODE).await;
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

    let after = until_node_where(&estate, NODE, &format!("{NODE} ready as a new node after its recovery failed"), Duration::from_secs(60), |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir(NODE).await, data_dir, "the same data dir");
    let done = wait_for("the completed submit of the exited birth is exported", Duration::from_secs(60), || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("complete")) }).await;
    assert_eq!(s(&done["attributes"]["build_id"]), birth_build, "the same Build, no Build minted: {done}");
    assert_eq!(done["attributes"]["incarnation_id"], before["incarnation_id"], "keyed on the exact exited birth: {done}");
    let attempt: u64 = s(&done["attributes"]["attempt"]).parse().expect("the completed submit names its attempt");
    assert!(attempt > failed_attempt, "a later attempt than the failed one: {done}");
    assert_eq!(s(&done["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "{done}");
    assert!(s(&done["attributes"]["after_failed_attempt"]).parse::<u64>().is_ok_and(|a| a >= failed_attempt && a < attempt), "the submit follows a Failed one of the same birth: {done}");

    assert_eq!(s(&done["attributes"]["mesh"]), "mesh1", "both attempts are mesh1's lane's: {done}");
    let build = build_log::build_value(&estate.carrier().await, &birth_build).await;
    let steps = lane_steps(&build, "mesh1");
    let deleted: Vec<&Value> = steps.iter().filter(|r| r["operation"] == format!("retire-node:{NODE}").as_str() && r["step"] == "NodeDeleted").collect();
    assert_eq!(deleted.len(), 1, "one node.delete of the exited birth, never again: {deleted:?}");
    assert!(steps.iter().all(|r| !(r["operation"] == format!("retire-node:{NODE}").as_str() && r["attempt"] == json!(attempt))), "the completing attempt deletes nothing");
    let spans = wait_for("the completing attempt's reconcile span is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        lane_reconcile_span(&in_trace(&spans, &done), &birth_build, "mesh1", &attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    let run = in_trace(&spans, &done);
    let reconcile = lane_reconcile_span(&run, &birth_build, "mesh1", &attempt.to_string()).expect("the attempt's reconcile span of mesh1's lane");
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("create-node:{NODE}"), "only what was left: {reconcile}");
    estate.record_trace_url(done["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
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
/// row and opens the same exited birth's recovery at its own door, claimed on mesh2's rows (R-BI7:
/// no call to the fabric-primary in mesh1). A later attempt of mesh2's lane of the same Build runs
/// only the create, and the path comes back as a new node over the same data dir and
/// endpoint key. Must NOT happen: the path left empty after the seat moved, a second node.delete of
/// the exited birth, a Build minted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_build_for_a_proven_exit_is_submitted_again_by_the_admin_that_takes_the_mesh_primary_seat() {
    let (estate, birth_build) = fabric_of("a_failed_build_for_a_proven_exit_is_submitted_again_by_the_admin_that_takes_the_mesh_primary_seat", &[("mesh1", 1, 1), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let nodes = estate.node_values().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    assert!(fp.starts_with("mesh1."), "the day-0 admin keeps the fabric seat: {fp}");
    let mp = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2's primary")["name"]);
    let heir = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != mp.as_str()).expect("mesh2's second admin")["name"]);
    const NODE: &str = "mesh2.rpc.1";
    let before = estate.node_value(NODE).await;
    let data_dir = estate.data_dir(NODE).await;
    let key_path = Path::new(&data_dir).join("node-key");
    let key = std::fs::read(&key_path).unwrap_or_else(|e| panic!("{NODE}'s node-key in {data_dir}: {e}"));
    std::fs::remove_file(&key_path).unwrap();
    estate.kill_node(NODE).await;

    let submits = |spans: &[Value]| -> Vec<Value> { named(spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let failed = wait_for("the create of the Build for the proven exit fails at PrepareStorage", SETTLE, || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("failed")) }).await;
    let failed_attempt: u64 = s(&failed["attributes"]["attempt"]).parse().expect("the failed submit names its attempt");
    let outcome = s(&failed["attributes"]["outcome"]);
    assert!(outcome.contains(&format!("create-node:{NODE} step PrepareStorage")) && outcome.contains("node-key is missing"), "the create refused the dir with no key by name: {failed}");
    assert_eq!((s(&failed["attributes"]["submitter"]), s(&failed["attributes"]["mesh"]), s(&failed["attributes"]["build_id"])), (mp.clone(), "mesh2".to_string(), birth_build.clone()), "{mp} opened it in mesh2's lane: {failed}");

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
    // No attempt for the member is in flight in mesh2's lane (one the killed primary was running
    // when it died is ended by the admin that took the seat, on the killed primary's proven exit, and
    // fails on the missing key): only a later submit can bring the path back.
    let idle = build_log::build_fact(&estate.carrier().await, &birth_build, &format!("no attempt for {NODE} is in flight in mesh2's lane"), SETTLE, |p| {
        let b = serde_json::to_value(p).expect("a Build view serializes");
        let lane = &b["lanes"]["mesh2"];
        let running = matches!(lane["state"].as_str(), Some("pending" | "running"));
        let member = lane["action"]["action"] == "recover" && lane["action"]["path"] == NODE;
        !(running && member)
    })
    .await
    .map(|p| serde_json::to_value(p).expect("a Build view serializes"))
    .unwrap_or_else(|end| panic!("{end}"));
    std::fs::write(&key_path, &key).unwrap();

    let after = until_node_where(&estate, NODE, &format!("{NODE} ready as a new node after mesh2's seat moved"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir(NODE).await, data_dir, "the same data dir");
    let done = wait_for("the completed submit of the exited birth is exported", Duration::from_secs(60), || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("complete")) }).await;
    assert_eq!((s(&done["attributes"]["submitter"]), s(&done["attributes"]["mesh"])), (heir.clone(), "mesh2".to_string()), "the admin that took the seat opened it in mesh2's lane: {done}");
    assert_eq!(s(&done["attributes"]["disposition"]), "created", "its own door opened the next attempt: {done}");
    assert_eq!(s(&done["attributes"]["build_id"]), birth_build, "the same Build, no Build minted: {done}");
    assert_eq!(done["attributes"]["incarnation_id"], before["incarnation_id"], "keyed on the exact exited birth: {done}");
    let attempt: u64 = s(&done["attributes"]["attempt"]).parse().expect("the completed submit names its attempt");
    assert!(attempt > failed_attempt, "a later attempt than the failed one: {done}");
    assert_eq!(s(&done["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "{done}");
    assert!(s(&done["attributes"]["after_failed_attempt"]).parse::<u64>().is_ok_and(|a| a >= failed_attempt && a < attempt), "the submit follows the Failed attempt the Build holds: {done}");

    let spans = wait_for("the completing attempt's reconcile span is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        lane_reconcile_span(&in_trace(&spans, &done), &birth_build, "mesh2", &attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    // The admin that took the seat opened, claimed and ran it itself, at that attempt of mesh2's lane,
    // with no call to the fabric-primary.
    let run = in_trace(&spans, &done);
    assert_mesh_primary_ran_the_recovery(&run, &heir, "mesh2", &birth_build, &attempt.to_string(), &before);
    let admitted = named(&run, "rdm.node_admin.build.update.via-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).expect("the open of the completing attempt");
    assert!(
        !named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").iter().any(|sp| sp["attributes"]["node_id"] == before["node_id"] && sp["attributes"]["inspector"] == heir.as_str()),
        "{heir} never inspected the departed birth: the submit came from the durable facts"
    );
    let reconcile = lane_reconcile_span(&run, &birth_build, "mesh2", &attempt.to_string()).expect("the attempt's reconcile span");
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("create-node:{NODE}"), "only what was left: {reconcile}");
    let build = build_log::build_value(&estate.carrier().await, &birth_build).await;
    let steps = lane_steps(&build, "mesh2");
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
    estate.settles_to(&[("mesh1", 1, 1), ("mesh2", 2, 2)], SETTLE).await;
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
fn create_span<'a>(spans: &'a [Value], build: &str, attempt: &str, node: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.node.create.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == node)
}

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

/// The steps of a planned replace of a live birth, in the order node-replace.md numbers them:
/// node.create makes the successor at `<path>.new` through its full FirstStart to Ready, carrying no
/// traffic (1); the old birth stops (stop-node, its node.connections.delete { All } inside it,
/// node-left, its NodeStopped) (2) and is renamed `<path>.old` by node.name.update (3); the successor
/// stops, takes `<path>` by node.name.update (the cutover) and starts under it (4). The predecessor
/// stays stopped with its data.
const REPLACE_ORDER: [&str; 14] = [
    "AllocateIdentity",
    "DeployRuntime",
    "WaitForBind",
    "WaitForMeshJoin",
    "WaitForNodeReady",
    "StopNode",
    "AwaitNodeLeft",
    "RenamePredecessor",
    "StopSuccessor",
    "AwaitSuccessorLeft",
    "RenameSuccessor",
    "StartSuccessor",
    "WaitForSuccessorReady",
    "Complete",
];

/// The steps a planned replace never runs: it deletes nothing, proves no exit and moves no data.
const NOT_IN_A_REPLACE: [&str; 5] = ["NodeDeleting", "TerminateRuntime", "HandoffStorage", "NodeDeleted", "ReleaseStorage"];

/// The replace of `node` in `attempt` of `build` ran as ONE operation `replace-node:<node>`, its steps in
/// `order`: the successor was Ready at `<node>.new` before the predecessor was stopped, and the
/// predecessor was renamed `<node>.old` before the successor took `<node>`.
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
    let ready = step_span(spans, build, attempt, node, "WaitForNodeReady").unwrap();
    let stop = step_span(spans, build, attempt, node, "StopNode").unwrap();
    assert!(end(ready) <= start(stop), "the successor is Ready at {node}.new before the predecessor is stopped: {ready} vs {stop}");
    let cutover = step_span(spans, build, attempt, node, "RenameSuccessor").unwrap();
    assert!(end(rename) <= start(cutover), "the predecessor is renamed before the successor takes {node}: {rename} vs {cutover}");
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

/// CONTRACT: `build.create` of a Replace change replaces a live node as the next attempt of the
/// accepted Build (reason replace, no Build minted) by Rafka's recipe (node-replace.md 1-4): one
/// operation `replace-node:<path>`; node.create makes the successor at `<path>.new`, a new NodeId
/// with a new transport key and its own directory, Ready and carrying no traffic; the old birth stops
/// (its connections deleted inside the stop), its NodeStopped drops it from every admin's book, and
/// it is renamed `<path>.old` by node.name.update; the successor stops, takes `<path>` by
/// node.name.update and starts under it. No admin's view holds two live births at the path at any
/// step. The predecessor stays parked with its data: when the successor is ready, and after the
/// operation completes, its process is alive and its data is where it was ("Rafka transfers its data
/// as its own recipe step"). The replace deletes nothing (no NodeDeleting, terminal proof, storage
/// hand-over or NodeDeleted): the predecessor's row moves to `<path>.old` and stays its row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_replace_creates_the_successor_while_the_predecessor_stays_parked_with_its_data() {
    let (estate, birth_build) = estate("a_planned_replace_creates_the_successor_while_the_predecessor_stays_parked_with_its_data").await;
    const NODE: &str = "mesh1.rpc.1";
    let before = estate.node_value(NODE).await;
    // The predecessor's process and data: Rafka's to move after the create, never RDM's.
    let pid = estate.pid_of(NODE).await;
    let old_dir = std::path::PathBuf::from(estate.data_dir(NODE).await);
    let segment = old_dir.join("app-data").join("seg-0");
    std::fs::create_dir_all(segment.parent().unwrap()).unwrap();
    std::fs::write(&segment, b"records").unwrap();
    let replace = estate.submit_change(Change::Replace { node: Estate::path(NODE), incarnation: None }).await;
    assert_eq!(replace.build_id().as_str(), birth_build, "no Build is minted");
    let attempt_no = u64::from(replace.attempt());
    // The attempt is the Build's current attempt as the entry admin's Build log holds it.
    let opened = build_log::build_fact(&estate.carrier().await, &birth_build, "the replace attempt is the Build's current attempt", Duration::from_secs(30), |p| serde_json::to_value(p).expect("a Build view serializes")["reason"] == "replace")
        .await
        .map(|p| serde_json::to_value(p).expect("a Build view serializes"))
        .unwrap_or_else(|end| panic!("{end}"));
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("replace", NODE), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "fenced to the live birth: {opened}");
    // While the replace runs, no admin's view holds two live births at the path at any step
    // (node-replace.md: the predecessor is parked and renamed before node.create claims the freed
    // name). That the view named the predecessor `<path>.old` is read from the span below.
    let admins: Vec<PathName> = estate.node_values().await.iter().filter(|n| n["kind"] == "node_admin").map(|n| Estate::path(&s(&n["name"]))).collect();
    assert_eq!(admins.len(), 2, "both admins serve a view");
    let old_name = format!("{NODE}.old");
    let ready_what = format!("{NODE} ready under a new NodeId");
    let watch = wait_for(&ready_what, SETTLE, || async {
        at_most_one_live_birth(&estate, &admins, NODE).await;
        let nodes = estate.node_values().await;
        assert!(nodes.iter().filter(|n| n["name"] == NODE).count() <= 1, "two births at {NODE}: {nodes:#?}");
        let after = nodes.into_iter().find(|n| n["name"] == NODE && ready(n) && n["node_id"] != before["node_id"])?;
        Some((after, alive(pid), std::fs::read(&segment).ok()))
    });
    let (_, (after, alive_at_ready, data_at_ready)) = tokio::join!(estate.follow(replace), watch);
    assert!(alive_at_ready, "the predecessor's process {pid} is alive, parked, when the successor is ready");
    assert_eq!(data_at_ready.as_deref(), Some(&b"records"[..]), "the predecessor's data is in its own directory when the successor is ready: {segment:?}");

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
    // Each admin's view names it once; the executor's, which moved the row, is the first.
    let held_old = named(&spans, "rdm.node_admin.topology.update.via-predecessor-held")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == old_name.as_str() && sp["attributes"]["node_id"] == before["node_id"])
        .min_by_key(|sp| start(sp))
        .unwrap_or_else(|| panic!("the executor's view never named the predecessor {old_name}"));
    let complete = step_span(&spans, &birth_build, &attempt, NODE, "Complete").expect("Complete ran");
    assert!(start(held_old) < start(complete), "the view named the predecessor {old_name} before the replace completed: {held_old}");
    // The node's row (ops-naming "Node data": "a Deleted row, or the <name>.old row"): at the
    // executor's RenamePredecessor the mesh's primary, which executes it, moves the predecessor's row
    // to `<path>.old`, where it stays the parked predecessor's row, and the successor's create claims
    // its row at the path.name, seeded from the .old row, before it is launched (R13).
    // node-replace.md step 3: "node[1].update(name=`<name>.old`) as node.name.update on the stopped
    // predecessor": the RenamePredecessor step is the mesh-primary's node.name.update, decided by its
    // validator in-process (exact birth, writer, the stopped precondition, the name) under the step.
    let rename_step = step_span(&spans, &birth_build, &attempt, NODE, "RenamePredecessor").expect("RenamePredecessor ran");
    let moved: Vec<&Value> = named(&spans, "rdm.node_admin.node.name.update.via-writer").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(moved.len(), 1, "one node.name.update of the predecessor: {moved:?}");
    assert_eq!(
        (s(&moved[0]["attributes"]["from"]), s(&moved[0]["attributes"]["to"]), s(&moved[0]["attributes"]["traffic"]), s(&moved[0]["attributes"]["outcome"])),
        (NODE.to_string(), old_name.clone(), "Stopped".to_string(), "name-updated".to_string()),
        "{}",
        moved[0]
    );
    assert_eq!(moved[0]["parent_span_id"], rename_step["span_id"], "the predecessor's node.name.update runs inside its RenamePredecessor step: {} vs {rename_step}", moved[0]);
    let old_deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == old_name.as_str()).collect();
    assert!(old_deleted.is_empty(), "no Deleted row at {old_name}: the predecessor is parked, not deleted: {old_deleted:?}");
    // node-replace.md step 1: the successor's create claims `<name>.new`, seeded from nothing; it is
    // born before the predecessor gives up anything.
    let new_name = format!("{NODE}.new");
    let seeded: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().filter(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["moment"] == "claim").collect();
    assert_eq!(seeded.len(), 1, "the successor claims its row once: {seeded:?}");
    assert_eq!((s(&seeded[0]["attributes"]["subject"]), s(&seeded[0]["attributes"]["outcome"]), s(&seeded[0]["attributes"]["seeded_from"])), (new_name.clone(), "row-written".to_string(), String::new()), "{}", seeded[0]);
    assert!(start(seeded[0]) < start(moved[0]), "the successor claimed {new_name} before the predecessor's row moved");
    // Step 4: the stopped successor takes `<name>` by node.name.update inside its RenameSuccessor
    // step, inheriting from the predecessor's retained `<name>.old` row.
    let cutover_step = step_span(&spans, &birth_build, &attempt, NODE, "RenameSuccessor").expect("RenameSuccessor ran");
    let cutover: Vec<&Value> = named(&spans, "rdm.node_admin.node.name.update.via-writer").into_iter().filter(|sp| sp["attributes"]["node_id"] == after["node_id"]).collect();
    assert_eq!(cutover.len(), 1, "one node.name.update of the successor: {cutover:?}");
    assert_eq!(
        (s(&cutover[0]["attributes"]["from"]), s(&cutover[0]["attributes"]["to"]), s(&cutover[0]["attributes"]["traffic"]), s(&cutover[0]["attributes"]["inherited_from"]), s(&cutover[0]["attributes"]["outcome"])),
        (new_name.clone(), NODE.to_string(), "Stopped".to_string(), s(&before["node_id"]), "name-updated".to_string()),
        "{}",
        cutover[0]
    );
    assert_eq!(cutover[0]["parent_span_id"], cutover_step["span_id"], "the successor's node.name.update runs inside its RenameSuccessor step: {} vs {cutover_step}", cutover[0]);
    estate.artifact("node-rows.json", &json!({"moved": moved[0], "seeded": seeded[0], "cutover": cutover[0]}));
    let op = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE).expect("the node operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    // After the operation: the predecessor is parked with its data, the successor has none of it.
    assert!(alive(pid), "the predecessor's process {pid} is alive, parked, after the replace completed");
    assert_eq!(std::fs::read(&segment).unwrap(), b"records", "the predecessor's data stays where it was: {segment:?}");
    let new_dir = std::path::PathBuf::from(estate.data_dir(NODE).await);
    assert_ne!(new_dir, old_dir, "the successor has its own directory");
    assert!(!new_dir.join("app-data").exists(), "RDM handed nothing to the successor's directory: {new_dir:?}");
    // The shape is whole: two admins and three rpc nodes ready, the successor the one birth at the
    // path; the parked predecessor is no live birth.
    let settled = estate.until_view("mesh1 holds 2 admins and 3 rpc nodes ready besides the parked predecessor", Duration::from_secs(30), |views| {
        let nodes = Estate::values(views);
        let rest: Vec<&Value> = nodes.iter().filter(|n| n["node_id"] != before["node_id"]).collect();
        let count = |kind: &str| rest.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == kind).count();
        (count("node_admin") == 2 && count("rpc_node") == 3 && rest.iter().all(|n| ready(n))).then_some(nodes)
    })
    .await;
    assert_eq!(settled.iter().filter(|n| n["name"] == NODE).count(), 1, "exactly one birth at {NODE}");
    assert!(!settled.iter().any(|n| n["node_id"] == before["node_id"] && live(n)), "the parked predecessor is no live birth: {settled:#?}");
    estate.shutdown().await;
}

/// CONTRACT (node-replace.md step 4: "the mesh-primary seeds the successor's config from the
/// `<name>.old` row"; proposal node-properties-own-commands.md §8, replace: "the successor, born
/// `<name>.new`, inherits config and tags from the retained predecessor row at the cutover rename, the
/// write that puts it at `<name>`, and the index entries are written under the successor's id then"):
/// mesh2's primary tells `mesh2.rpc.1` a config and a tag, then the node is replaced. The successor's
/// claim at `<name>.new` carries its create spec's (no config, no tags, seeded from nothing). At the
/// cutover the mesh-primary's node.name.update puts the successor's row at `<name>` with the config
/// and the tag of the retained `<name>.old` row (`inherited_from` the predecessor), releases
/// `<name>.new`, then writes the retained row's `inherited_by` (its labels leave the tags index; the
/// row keeps them). The successor's join under `<name>` hands it that config; node.get by the tag
/// names the successor alone, while the predecessor stays retained at `<name>.old`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_nodes_successor_takes_its_config_and_tags_at_the_cutover_and_the_retained_row_leaves_the_tags_index() {
    let (estate, _build) = two_mesh_estate("a_replaced_nodes_successor_takes_its_config_and_tags_at_the_cutover_and_the_retained_row_leaves_the_tags_index").await;
    let nodes = estate.node_values().await;
    let (node, mesh_primary, _fp) = placement(&nodes);
    let node_path = node.as_str();
    let (old_name, new_name) = (format!("{node_path}.old"), format!("{node_path}.new"));
    let before = estate.node_value(node_path).await;
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
    wait_for(&format!("{node_path} carries its config"), Duration::from_secs(30), || async { (estate.node_value(node_path).await["config"] == told_json).then_some(()) }).await;
    let labels: std::collections::BTreeSet<String> = ["rabbit".to_string()].into();
    let tagged = rafka_node_admin_client::NodeRpc::new(&client)
        .tags_create(&mp_id, &rafka_node_rpc::RouteChoice::Direct, &birth, &labels, &rafka_node_rpc::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.tags.create of {node_path} at {mesh_primary}: {e:?}"));
    assert_eq!(tagged.name(), "tags-created", "the mesh-primary wrote the label's row: {tagged:?}");
    wait_for(&format!("{node_path} carries its tag"), Duration::from_secs(30), || async { (estate.node_view(node_path).await.tags == labels).then_some(()) }).await;

    let replace = estate.submit_change(Change::Replace { node: Estate::path(node_path), incarnation: None }).await;
    estate.follow(replace).await;
    let after = until_node_where(&estate, node_path, &format!("{node_path} ready under a new NodeId"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    let spans = wait_for("the successor's claim, its cutover, the retained row's mark and the successor's join under the name are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let joined = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().any(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["admitted_as"] == node_path);
        let marked = named(&spans, "rdm.node_admin.node.update.via-inherited-row").into_iter().any(|sp| sp["attributes"]["node_id"] == before["node_id"]);
        (joined && marked).then_some(spans)
    })
    .await;
    // Step 1: the successor's claim at `<name>.new` carries its create spec's: nothing is seeded.
    let claim = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["moment"] == "claim").expect("the successor's claim").clone();
    let a = &claim["attributes"];
    assert_eq!((s(&a["subject"]), s(&a["outcome"]), s(&a["seeded_from"]), s(&a["keys"]), s(&a["tags"])), (new_name.clone(), "row-written".to_string(), String::new(), "0".to_string(), String::new()), "the successor is born at {new_name} with its create spec's config and tags, none: {claim}");
    // Step 4: the cutover inherits from the retained row, then marks it.
    let cutover = named(&spans, "rdm.node_admin.node.name.update.via-writer").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"]).expect("the successor's node.name.update").clone();
    let c = &cutover["attributes"];
    assert_eq!((s(&c["node"]), s(&c["from"]), s(&c["to"]), s(&c["inherited_from"]), s(&c["keys"]), s(&c["tags"]), s(&c["outcome"])), (mesh_primary.clone(), new_name.clone(), node_path.to_string(), s(&before["node_id"]), "1".to_string(), "rabbit".to_string(), "name-updated".to_string()), "the mesh-primary puts the successor at {node_path} with the retained row's config and tag: {cutover}");
    let mark = named(&spans, "rdm.node_admin.node.update.via-inherited-row").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).expect("the retained row's mark").clone();
    let m = &mark["attributes"];
    assert_eq!((s(&m["subject"]), s(&m["inherited_by"]), s(&m["tags"]), s(&m["outcome"])), (old_name.clone(), s(&after["node_id"]), "rabbit".to_string(), "inherited".to_string()), "the retained row says who inherited it and keeps its tag: {mark}");
    assert_eq!(mark["parent_span_id"], cutover["span_id"], "the mark is the cutover's last write: {mark} under {cutover}");
    // The successor's join under the name hands it the row's config (node-start.md step 2).
    let joined = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["admitted_as"] == node_path).expect("the successor's join under the name").clone();
    assert_eq!((s(&joined["attributes"]["outcome"]), s(&joined["attributes"]["config_keys"])), ("installed".to_string(), "1".to_string()), "the successor's join answer under {node_path} hands its row's config: {joined}");
    let carried = wait_for(&format!("the successor at {node_path} carries the config"), Duration::from_secs(30), || async { let n = estate.node_value(node_path).await; (n["config"] == told_json).then_some(n) }).await;
    assert_eq!(carried["node_id"], after["node_id"], "the successor carries it");
    assert_eq!(estate.node_view(node_path).await.tags, labels, "the successor's row carries the predecessor's tag");
    let selected: Vec<String> = estate
        .nodes_obj()
        .await
        .get(&rafka_node_admin_client::NodeSelector::Tags(rafka_node_admin_client::TagFilter::Any(labels.clone())))
        .await
        .unwrap_or_else(|e| panic!("node.get by the tag rabbit: {e:?}"))
        .into_iter()
        .map(|n| n.node_id.to_string())
        .collect();
    assert_eq!(selected, vec![s(&after["node_id"])], "the tags index names the successor under the label, and not the retained predecessor");
    let retained = estate.node_values().await.into_iter().find(|n| n["name"] == old_name.as_str()).unwrap_or_else(|| panic!("the predecessor is retained at {old_name}"));
    assert_eq!(retained["node_id"], before["node_id"], "the retained {old_name} is the predecessor: {retained}");
    eprintln!("trace {}: via-writer node={} from={} to={} inherited_from={} keys={} tags={}; via-inherited-row subject={} inherited_by={}; trace {}: via-join admitted_as={} config_keys={}", s(&cutover["trace_id"]), mesh_primary, new_name, node_path, s(&c["inherited_from"]), s(&c["keys"]), s(&c["tags"]), old_name, s(&m["inherited_by"]), s(&joined["trace_id"]), node_path, s(&joined["attributes"]["config_keys"]));
    estate.shutdown().await;
}

/// The steps of `node.delete` of a parked birth (node-decommission.md: its stop steps are already done,
/// "Stop's node.stopped is pre-exit (parked) and insufficient"): the removal is announced, the
/// provider's exact Exited is the proof, then the executor publishes NodeDeleted and the operation's
/// Complete names the birth. Storage is not released ("It releases node.id and name, not storage").
const DELETE_PARKED_ORDER: [&str; 4] = ["NodeDeleting", "TerminateRuntime", "NodeDeleted", "Complete"];

/// CONTRACT: the recipe's `node[1].delete` of the stopped predecessor a replace left at `<path>.old`
/// (node-replace.md; node-decommission.md 2-4): `build.create` of a RemoveNode change at `<path>.old` is the next attempt of
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
    let (estate, birth_build) = fabric_of("deleting_a_parked_predecessor_after_its_mesh_primary_changed_is_executed_by_the_new_primary_on_the_exact_birth", &[("mesh1", 1, 2), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let parked = replace_and_park(&estate, &birth_build, "mesh2.rpc.1").await;
    let first_primary = parked.replaced_by.clone();
    assert!(first_primary.starts_with("mesh2.admin."), "mesh2's primary ran the replace: {first_primary}");
    // mesh2's primary changes: its planned replace hands the seat to mesh2's other admin, which
    // executes that replace (executor_for: the mesh primary never executes its own).
    let before_admin = estate.node_value(&first_primary).await;
    let replace = estate.submit_change(Change::Replace { node: Estate::path(&first_primary), incarnation: None }).await;
    let admin_attempt = u64::from(replace.attempt());
    estate.follow(replace).await;
    until_new_birth(&estate, &first_primary, &before_admin, SETTLE).await;
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
    let before = estate.node_value(node).await;
    let pid = estate.pid_of(node).await;
    let old_dir = std::path::PathBuf::from(estate.data_dir(node).await);
    let replace = estate.submit_change(Change::Replace { node: Estate::path(node), incarnation: None }).await;
    assert_eq!(replace.build_id().as_str(), build, "no Build is minted");
    let attempt = u64::from(replace.attempt());
    estate.follow(replace).await;
    let after = until_new_birth(estate, node, &before, SETTLE).await;
    assert!(alive(pid), "the predecessor {pid} is parked, alive, before its delete");
    let spans = wait_for("the replace's dispatch is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        dispatch(&spans, build, &attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    let d = dispatch(&spans, build, &attempt.to_string()).unwrap();
    Parked { node: node.to_string(), before, after, pid, old_dir, replaced_by: s(&d["attributes"]["executor"]), fabric_primary: s(&d["attributes"]["node"]) }
}

/// `build.create` of a RemoveNode change at `<path>.old` of `parked`, and everything node.delete owes (node-decommission.md
/// 2-4). Returns the admin that executed the delete.
async fn delete_parked_and_assert(estate: &Estate, birth_build: &str, parked: &Parked) -> String {
    let Parked { node, before, after, pid, old_dir, .. } = parked;
    let old_name = format!("{node}.old");
    let delete = estate.submit_change(Change::RemoveNode { node: Estate::path(&old_name) }).await;
    assert_eq!(delete.build_id().as_str(), birth_build, "no Build is minted");
    let attempt_no = u64::from(delete.attempt());
    let opened = build_log::build_fact(&estate.carrier().await, birth_build, "the delete attempt is the Build's current attempt", Duration::from_secs(30), |p| serde_json::to_value(p).expect("a Build view serializes")["reason"] == "delete")
        .await
        .map(|p| serde_json::to_value(p).expect("a Build view serializes"))
        .unwrap_or_else(|end| panic!("{end}"));
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("delete", old_name.as_str()), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "fenced to the exact old birth: {opened}");
    estate.follow(delete).await;
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
    let now = estate.node_value(node).await;
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
    let nodes = estate.node_values().await;
    let admins: Vec<&Value> = nodes.iter().filter(|n| n["kind"] == "node_admin").collect();
    assert_eq!(admins.len(), 2, "{nodes:#?}");
    let before = admins.iter().find(|n| n["is_primary"] != true && n["is_fabric_primary"] != true).map(|n| (*n).clone()).expect("an admin that holds no seat");
    let stays = Estate::path(&s(&admins.iter().find(|n| n["node_id"] != before["node_id"]).unwrap()["name"]));
    assert_eq!(estate.entry().await, stays, "the admin that stays is the entry the typed objects ask");
    let node = s(&before["name"]);
    let node_path = Estate::path(&node);
    let replace = estate.submit_change(Change::Replace { node: node_path.clone(), incarnation: None }).await;
    assert_eq!(replace.build_id().as_str(), birth_build, "no Build is minted");
    let attempt_no = u64::from(replace.attempt());
    // The entry admin's view is the staying admin's: read through the entry, it also learns the successor's birth.
    let ready_what = format!("{node} ready under a new NodeId");
    let watch = wait_for(&ready_what, SETTLE, || async {
        let nodes = estate.node_values().await;
        let at: Vec<&Value> = nodes.iter().filter(|n| n["name"] == node.as_str() && live(n)).collect();
        assert!(at.len() <= 1, "the view of {stays} holds two live births at {node}: {at:#?}");
        let successor = nodes.iter().find(|n| n["name"] == node.as_str() && ready(n) && n["node_id"] != before["node_id"]).cloned()?;
        at_most_one_live_birth(&estate, &[node_path.clone()], &node).await;
        Some(successor)
    });
    let (_, after) = tokio::join!(estate.follow(replace), watch);
    // The successor admin's own view: itself at the path, ready, under its new NodeId.
    let me = estate
        .until_view_at(&node_path, &format!("the successor {node} sees itself"), Duration::from_secs(30), |views| {
            let at: Vec<Value> = Estate::values(views).into_iter().filter(|n| n["name"] == node.as_str() && live(n)).collect();
            assert!(at.len() <= 1, "the successor's view holds two live births at {node}: {at:#?}");
            at.into_iter().find(|n| n["node_id"] == after["node_id"] && ready(n))
        })
        .await;
    assert_eq!(me["incarnation_id"], after["incarnation_id"], "the successor's view holds its own birth: {me}");
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
/// not-exited and no recovery is submitted (no via-node-recovery for that birth, no attempt opened in
/// its mesh's lane).
/// Released, the SAME birth (node.id and incarnation) is ready again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_member_whose_runtime_still_runs_is_never_recovered() {
    let (mut estate, birth_build) = two_mesh_estate("a_hung_member_whose_runtime_still_runs_is_never_recovered").await;
    let (node, mesh_primary, primary) = placement(&estate.node_values().await);
    let fp = Estate::path(&s(&primary["name"]));
    let before = estate.node_value(&node).await;
    let mesh = s(&before["mesh"]);
    let attempt_before = lane_attempt_at(&estate, &fp, &birth_build, &mesh).await;
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
    assert_eq!(lane_attempt_at(&estate, &fp, &birth_build, &mesh).await, attempt_before, "no attempt is opened in {mesh}'s lane for a hung member");
    assert_eq!(estate.node_value(&node).await["node_id"], before["node_id"], "the held member is not replaced");

    let thawed = rt.apply(Fault::Continue).expect("the hold is released");
    assert_ne!(thawed.state_after, Some('T'), "{thawed:?}");
    until_node_where(&estate, &node, &format!("{node} back as its own birth"), floor * 2 + Duration::from_secs(30), |n| ready(n) && n["node_id"] == before["node_id"] && n["incarnation_id"] == before["incarnation_id"]).await;
    assert_eq!(lane_attempt_at(&estate, &fp, &birth_build, &mesh).await, attempt_before, "{mesh}'s lane of the Build gained no attempt");
    assert_eq!(exact_runtime(&estate, &node).await, rt, "the runtime that was held serves the path");
    estate.record_trace_url(inspected["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
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
/// recovered by its mesh-primary on its own mesh's rows (R-BI7). Once the fabric-primary holds the
/// recovery's receipts in that mesh's lane, heard on the Build topic, the same recovery submitted
/// again to it on op 0x20 (`build.create` Recover keyed on the exited birth) answers Started
/// AlreadyApplied naming the recovery's attempt, then its Complete terminal: keyed on the exited
/// birth, whoever asks; nothing runs, the mesh's lane gains no attempt, the path keeps the recovered
/// node, and exactly one recovery was ever opened for that birth. From the fabric-primary's mesh, a call aimed at the
/// exited birth's node.id is not sent (`Resolve(Unknown)`): it never reaches the new process that
/// holds the same endpoint key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeat_recovery_submit_for_the_same_exited_birth_is_answered_already_applied() {
    use rafka_node_rpc_contract::build::{BuildChange, Disposition};
    let (mut estate, birth_build) = two_mesh_estate("a_repeat_recovery_submit_for_the_same_exited_birth_is_answered_already_applied").await;
    let (node, mesh_primary, primary) = placement(&estate.node_values().await);
    let fp = Estate::path(&s(&primary["name"]));
    let before = estate.node_value(&node).await;
    let mesh = s(&before["mesh"]);
    let killed = exact_runtime(&estate, &node).await.apply(Fault::Kill).expect("the exact runtime is killed");
    assert!(killed.exited, "the OS observed the exit: {killed:?}");

    let after = until_node_where(&estate, &node, &format!("{node} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    let recovery = recovery_span(&estate, &node, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "{recovery}");
    assert_eq!(s(&recovery["attributes"]["disposition"]), "created", "{recovery}");
    assert_eq!(s(&recovery["attributes"]["mesh"]), mesh, "an attempt of {mesh}'s lane: {recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "{recovery}");
    // A resubmit before the fabric-primary heard the receipts would race the Build topic.
    fp_recovery_receipts(&estate, &fp, &birth_build, &mesh, &node, &attempt).await;

    let change = BuildChange::Recover {
        node: node.parse().expect("a path.name"),
        node_id: serde_json::from_value(before["node_id"].clone()).expect("a node id"),
        incarnation: serde_json::from_value(before["incarnation_id"].clone()).expect("an incarnation"),
    };
    let mut repeat = estate.carrier().await.create(change).await.expect("the fabric-primary answers the repeat");
    assert_eq!(repeat.disposition(), Disposition::AlreadyApplied, "the repeat is answered, never run");
    assert_eq!(repeat.build_id().as_str(), birth_build, "the accepted Build");
    assert_eq!(repeat.attempt().to_string(), attempt, "naming the attempt that recovered the birth");
    match repeat.next().await {
        Some(Ok(BuildFrame::Complete { attempt: a })) => assert_eq!(a.to_string(), attempt, "its terminal names the same attempt"),
        other => panic!("AlreadyApplied is followed by its Complete terminal: {other:?}"),
    }
    assert!(repeat.next().await.is_none(), "nothing after the terminal");
    assert_eq!(lane_attempt_at(&estate, &fp, &birth_build, &mesh).await.to_string(), attempt, "{mesh}'s lane of the Build gained no attempt");
    assert_eq!(estate.node_value(&node).await["node_id"], after["node_id"], "the path keeps the recovered node");

    // The fabric-primary's mesh never dials the exited birth's node.id, though its key now serves the new node.
    let old = estate.probe(&["get", "--target", &format!("exact:{}", s(&before["node_id"])), "--key", "83"]);
    assert_eq!((&old["outcome"], &old["reason"], &old["route"]), (&json!("NotSent"), &json!("Resolve(Unknown)"), &json!("direct")), "exact:<old> resolves nothing: {old}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
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
/// Failed end is. Then the mesh-primary proves the exit and opens the recovery, an attempt of its
/// mesh's lane of the same Build (R-BI7), once the fabric lane's attempt N no longer holds the mesh
/// (one attempt in flight per mesh): it completes, a new node.id at the path over the same endpoint
/// key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exit_during_a_workflow_fails_that_workflow_and_the_next_tick_recovers_the_member() {
    let (estate, birth_build) = two_mesh_estate("an_exit_during_a_workflow_fails_that_workflow_and_the_next_tick_recovers_the_member").await;
    let (node, mesh_primary, primary) = placement(&estate.node_values().await);
    let fp = Estate::path(&s(&primary["name"]));
    let before = estate.node_value(&node).await;
    let rt = exact_runtime(&estate, &node).await;
    assert_eq!(rt.apply(Fault::Stop).expect("the exact runtime is held").state_after, Some('T'));
    let mut deleting = estate.submit_change(Change::DeleteConnections { node: Estate::path(&node) }).await;
    assert_eq!(deleting.build_id().as_str(), birth_build, "no Build is minted");
    let delete_attempt = u64::from(deleting.attempt());
    let killed = rt.apply(Fault::Kill).expect("the held runtime is killed");
    assert!(killed.exited, "{killed:?}");
    // The call's own stream ends at the failed step: the held, then exited, birth never answered.
    let ended = loop {
        match deleting.next().await {
            Some(Ok(BuildFrame::Failed { attempt, operation, step, reason })) => break (attempt, operation, step, reason),
            Some(Ok(_)) => continue,
            other => panic!("the stream of the node.connections.delete ended without its Failed frame: {other:?}"),
        }
    };
    assert_eq!((u64::from(ended.0), ended.2.as_str()), (delete_attempt, "DeleteConnections"), "the stream names the failed step: {ended:?}");

    let operation = format!("node.connections.delete:{node}");
    let failed_step = |p: &Value| p["steps"].as_array().cloned().unwrap_or_default().into_iter().find(|r| r["operation"] == operation.as_str() && r["attempt"] == json!(delete_attempt) && r["outcome"]["failed"].is_object());
    let held = build_log::build_fact(&estate.carrier_at(&fp).await, &birth_build, "the node.connections.delete attempt ends Failed", SETTLE, |p| failed_step(&serde_json::to_value(p).expect("a Build view serializes")).is_some())
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    let failed = failed_step(&serde_json::to_value(held).expect("a Build view serializes")).expect("the failed step was just held");
    // The exit belongs to the node.connections.delete: the held, then exited, birth never answered
    // node.connections.deleted, and the attempt ends Failed, never Complete.
    assert_eq!(s(&failed["step"]), "DeleteConnections", "the node.connections.delete is not complete: {failed}");
    assert!(s(&failed["outcome"]["failed"]["reason"]).contains("node.connections.delete { All } ended"), "the failure names how the call ended: {failed}");

    let after = until_node_where(&estate, &node, &format!("{node} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, from the inherited data dir");
    let recovery = wait_for("the mesh-primary's completed recovery is exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"] && s(&sp["attributes"]["outcome"]).starts_with("complete")).cloned()
    })
    .await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "{recovery}");
    assert_eq!(s(&recovery["attributes"]["build_id"]), birth_build, "{recovery}");
    let attempt: u64 = s(&recovery["attributes"]["attempt"]).parse().expect("an attempt");
    let mesh = s(&before["mesh"]);
    assert_eq!(s(&recovery["attributes"]["mesh"]), mesh, "the recovery is an attempt of {mesh}'s lane: {recovery}");
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "{recovery}");
    // One attempt in flight per mesh: the recovery is opened only after the fabric lane's attempt N,
    // whose executor acts in the same mesh, ended (its reconcile span closes at the verdict).
    let spans = wait_for("the failed node.connections.delete attempt's reconcile span is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        reconcile_span(&spans, &birth_build, &delete_attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    let workflow = reconcile_span(&spans, &birth_build, &delete_attempt.to_string()).expect("attempt N's reconcile span");
    assert_eq!(s(&workflow["attributes"]["outcome"]), "failed", "{workflow}");
    let opened = named(&in_trace(&spans, &recovery), "rdm.node_admin.build.update.via-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"] && sp["attributes"]["outcome"] == "admitted").cloned().expect("the recovery's open");
    assert!(end(workflow) <= start(&opened), "the recovery opens once attempt N ended: {workflow} vs {opened}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
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
    let (estate, birth_build) = fabric_of("a_mesh_primary_handover_mid_ladder_continues_the_silence_window_and_completes_the_recovery", &[("mesh1", 1, 1), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let floor = rafka_mesh_transport::membership::staleness_floor();
    let nodes = estate.node_values().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    assert!(fp.starts_with("mesh1."), "the day-0 admin keeps the fabric seat: {fp}");
    let mp = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2's primary")["name"]);
    let standby = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != mp.as_str()).expect("mesh2's second admin")["name"]);
    const NODE: &str = "mesh2.rpc.1";
    let before = estate.node_value(NODE).await;
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
    let stopping = estate.submit_change(Change::Stop { node: Estate::path(&mp) }).await;
    assert_eq!(stopping.build_id().as_str(), birth_build, "no Build is minted");
    estate.follow(stopping).await;
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
    assert_eq!(s(&recovery["attributes"]["mesh"]), "mesh2", "an attempt of mesh2's lane, opened by the new mesh-primary itself: {recovery}");
    let after = until_node_where(&estate, NODE, &format!("{NODE} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
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
/// reason"): in a fabric whose fabric-primary is its mesh's only node-admin, each of the changes Replace,
/// Restart, DeleteConnections and Stop of the fabric-primary, submitted by `build.create`, is refused by name, `last-admin`
/// with exactly `cannot transfer: no secondary of type node-admin in mesh <its mesh>`, one
/// `rdm.node_admin.build.reject.via-last-admin` per call with the same text. The mesh seat has no
/// secondary, so the peer mesh's Ready primary is no successor of both seats (C2's outside search is
/// mesh.delete's only). No attempt opens, no seat move is commanded, no row is written, and every node
/// stays as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replace_of_the_fabric_primary_that_is_its_meshs_last_admin_is_refused_by_name() {
    let (estate, birth_build) = two_mesh_estate("a_replace_of_the_fabric_primary_that_is_its_meshs_last_admin_is_refused_by_name").await;
    let before = estate.node_values().await;
    let primary = before.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let fp = s(&primary["name"]);
    let mesh = s(&primary["mesh"]);
    assert_eq!(before.iter().filter(|n| n["kind"] == "node_admin" && n["mesh"] == mesh.as_str()).count(), 1, "the fabric-primary is its mesh's only admin: {before:#?}");
    let attempt = build_log::build_value(&estate.carrier().await, &birth_build).await["attempt"].clone();
    let q7 = format!("cannot transfer: no secondary of type node-admin in mesh {mesh}");
    let node = Estate::path(&fp);
    let carrier = estate.carrier().await;
    for (action, change) in [
        ("replace", Change::Replace { node: node.clone(), incarnation: None }),
        ("restart", Change::Restart { node: node.clone() }),
        ("node.connections.delete", Change::DeleteConnections { node: node.clone() }),
        ("stop", Change::Stop { node: node.clone() }),
    ] {
        match carrier.create(change).await {
            // The call ends naming the reason, then the refusal's text: `<reason>: <text>`.
            Err(CallEnd::Rejected { reason, detail }) => assert_eq!((reason.as_str(), detail.as_str()), ("last-admin", format!("last-admin: {q7}").as_str()), "{action}: refused with Q7's exact text"),
            Ok(_) => panic!("{action} of the fabric-primary {fp}, its mesh's last admin, was accepted"),
            Err(other) => panic!("{action} of the fabric-primary {fp}, its mesh's last admin, is refused last-admin, not {other:?}"),
        }
    }
    let b = build_log::build_value(&estate.carrier().await, &birth_build).await;
    assert_eq!(b["attempt"], attempt, "no attempt opened: {b}");
    let spans = wait_for("the four refusals are exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        (named(&spans, "rdm.node_admin.build.reject.via-last-admin").len() >= 4).then_some(spans)
    })
    .await;
    let refused = named(&spans, "rdm.node_admin.build.reject.via-last-admin");
    assert_eq!(refused.len(), 4, "one refusal per call: {refused:?}");
    assert!(refused.iter().all(|sp| s(&sp["attributes"]["detail"]) == q7), "every refusal span carries Q7's exact text: {refused:?}");
    assert!(named(&spans, "rdm.node_admin.seat.update.via-primary-updating").is_empty(), "no node.primary.update ran");
    assert!(named(&spans, "rdm.node_admin.seat.update.via-seat-row-written").is_empty(), "no row was written");
    assert!(
        named(&spans, "rdm.node_admin.build.update.via-node-rpc").into_iter().all(|sp| s(&sp["attributes"]["attempt"]).is_empty()),
        "no attempt number was recorded by any refused request"
    );
    estate.record_trace_url(refused[0]["trace_id"].as_str().unwrap_or(""));
    for n in &before {
        assert_eq!(estate.node_value(&s(&n["name"])).await["incarnation_id"], n["incarnation_id"], "{} is untouched", s(&n["name"]));
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

/// CONTRACT: a recovered node keeps the config and the tags its mesh-primary told its predecessor
/// (node-recovery.md "Retained: ... config (seeded from the Deleted row)"; proposal
/// node-properties-own-commands.md §8, recovery: "the successor's claim over it seeds config and tags
/// from that Deleted row and writes the tags index entries under the successor's id, in the same
/// write that seeds config"; fabric-node-lifecycle.md "A rewrite is lossless for every field its
/// writer knows"). The caller puts `<mesh>.rpc.1`'s config and the tag `rabbit` through its
/// mesh-primary (not the fabric-primary), the node carries them, then its runtime is killed: the
/// mesh-primary's recovery writes the exited birth's Deleted row with that config and tag, the new
/// node's name claim is seeded from it (`via-node-row` moment `claim`, `seeded_from` the exited node,
/// one key, `tags` rabbit), the new node's join answer hands it that config (`via-join`
/// `config_keys` 1), the new node (new node.id, same path.name) carries the config and the tag, and
/// `node.get` by the tag names the new node and not the exited one. Nothing copies them after the
/// create: no node.update runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_node_keeps_the_config_and_tags_its_mesh_primary_told_its_predecessor() {
    let (estate, _build) = two_mesh_estate("a_recovered_node_keeps_the_config_and_tags_its_mesh_primary_told_its_predecessor").await;
    let nodes = estate.node_values().await;
    let (node, mesh_primary, _fp) = placement(&nodes);
    let node_path = node.as_str();
    let before = estate.node_value(node_path).await;
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
    wait_for(&format!("{node_path} carries its config"), Duration::from_secs(30), || async { (estate.node_value(node_path).await["config"] == told_json).then_some(()) }).await;
    let labels: std::collections::BTreeSet<String> = ["rabbit".to_string()].into();
    let tagged = rafka_node_admin_client::NodeRpc::new(&client)
        .tags_create(&mp_id, &rafka_node_rpc::RouteChoice::Direct, &birth, &labels, &rafka_node_rpc::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.tags.create of {node_path} at {mesh_primary}: {e:?}"));
    assert_eq!(tagged.name(), "tags-created", "the mesh-primary wrote the label's row: {tagged:?}");
    wait_for(&format!("{node_path} carries its tag"), Duration::from_secs(30), || async { (estate.node_view(node_path).await.tags == labels).then_some(()) }).await;

    estate.kill_node(node_path).await;
    let after = until_node_where(&estate, node_path, &format!("{node_path} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
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
    assert_eq!(s(&a["tags"]), "rabbit", "the claim seeds the Deleted row's tags in the same write: {claim}");
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
    let carried = wait_for(&format!("the new {node_path} carries the config"), Duration::from_secs(30), || async { let n = estate.node_value(node_path).await; (n["config"] == told_json).then_some(n) }).await;
    assert_eq!(carried["node_id"], after["node_id"], "the new node, not the exited one, carries it");
    assert_eq!(estate.node_view(node_path).await.tags, labels, "the new node's row carries the exited node's tag");
    let selected: Vec<String> = estate
        .nodes_obj()
        .await
        .get(&rafka_node_admin_client::NodeSelector::Tags(rafka_node_admin_client::TagFilter::Any(labels.clone())))
        .await
        .unwrap_or_else(|e| panic!("node.get by the tag rabbit: {e:?}"))
        .into_iter()
        .map(|n| n.node_id.to_string())
        .collect();
    assert_eq!(selected, vec![s(&after["node_id"])], "the tags index names the new node under the label, and not the exited one");
    let told: Vec<Value> = named(&estate.spans(), "rdm.node_admin.node.update.via-config").into_iter().filter(|sp| sp["attributes"]["node_id"] == after["node_id"]).cloned().collect();
    assert!(told.is_empty(), "the join handed the config: no config call is sent to the starting node: {told:?}");
    eprintln!("trace {}: via-node-row delete node_id={} keys={} tags={}; trace {}: via-node-row claim node={} node_id={} seeded_from={} keys={} tags={}", s(&deleted[0]["trace_id"]), s(&before["node_id"]), deleted[0]["attributes"]["keys"], s(&deleted[0]["attributes"]["tags"]), s(&claim["trace_id"]), mesh_primary, s(&after["node_id"]), s(&a["seeded_from"]), a["keys"], s(&a["tags"]));
}

/// CONTRACT (R-BI7, proposal Part C: "The mesh-primary is the reconciler of its own mesh and does not
/// wait for the fabric-primary to heal it. The fabric-primary can be offline"; its flame: "a member
/// exits while the fabric-primary is stopped, and the trace shows the mesh-primary's proven exit, its
/// own claim on its mesh's rows, and the recovery completing with no fabric-primary call"): mesh1 holds
/// the fabric-primary and a second node-admin; mesh2 holds one node-admin, its mesh-primary, and two
/// rpc nodes. The fabric-primary is stopped by plan (node.stop: its seats move to mesh1's other
/// node-admin, then it stops). With it stopped, `mesh2.rpc.1` is killed. mesh2's primary proves the
/// exit and runs the recovery itself: the submit's trace holds the open at its own door, its claim
/// decided on mesh2's rows in mesh2's lane, the attempt's run and its converged reconcile, and no
/// Build call or claim call (op 0x20, 0x1C) and no dispatch. No other admin submits, opens or claims
/// that recovery, no attempt of the fabric lane acts on the member, and the stopped fabric-primary
/// stays stopped through it; the fabric-primary that holds the seat now learns the receipts from the
/// Build topic. Must NOT happen: the recovery waiting for a fabric-primary, a call to one, or the
/// stopped fabric-primary running again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_exit_while_the_fabric_primary_is_stopped_is_recovered_by_its_mesh_primary_alone() {
    let (mut estate, build) = fabric_of("a_member_exit_while_the_fabric_primary_is_stopped_is_recovered_by_its_mesh_primary_alone", &[("mesh1", 2, 1), ("mesh2", 1, 2)], Duration::from_secs(90)).await;
    let nodes = estate.node_values().await;
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    let fp_name = s(&fp["name"]);
    assert!(fp_name.starts_with("mesh1."), "the day-0 admin holds the fabric seat: {fp_name}");
    let successor_name = s(&nodes.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["name"] != fp["name"]).expect("mesh1's other node-admin")["name"]);
    let mp = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2's primary")["name"]);

    // The fabric-primary is stopped by plan: its seats move to mesh1's other node-admin, then it stops.
    // The stream ends at the old seat once the seat moves, naming the new one; the stop is
    // followed there to its completion.
    let mut stop = estate.nodes_obj().await.stop(&Estate::path(&fp_name)).await.unwrap_or_else(|e| panic!("node.stop of the fabric-primary {fp_name} was refused: {e}"));
    let accepted = stop.accepted().clone();
    let frames = Estate::frames_of(&mut stop).await;
    match frames.last() {
        Some(rafka_node_admin_client::Frame::SeatMoved { fabric_primary }) => assert_eq!(fabric_primary.as_deref(), Some(successor_name.as_str()), "the seat moved to mesh1's other node-admin: {frames:?}"),
        other => panic!("the stop of the fabric-primary moves its seat first: {other:?} of {frames:?}"),
    }
    estate.enter(&Estate::path(&successor_name)).await;
    let mut resumed = estate.nodes_obj().await.resume(stop.kind().clone(), &accepted).await.unwrap_or_else(|e| panic!("{successor_name} does not stream the stop of {fp_name} (Build {} attempt {}): {e}", accepted.build_id, accepted.attempt));
    Estate::complete(&mut resumed).await;
    let stopped = estate
        .until_view(&format!("{fp_name} is stopped and {successor_name} holds the fabric seat"), SETTLE, |views| {
            let old = views.iter().find(|n| n.name.to_string() == fp_name)?;
            let new = views.iter().find(|n| n.name.to_string() == successor_name)?;
            (old.parked && !old.is_fabric_primary && new.is_fabric_primary).then(|| serde_json::to_value(old).expect("a view serializes"))
        })
        .await;
    assert_eq!(stopped["incarnation_id"], fp["incarnation_id"], "the stopped fabric-primary is the same birth, stopped: {stopped}");

    // With the fabric-primary stopped, a member of mesh2 exits.
    const NODE: &str = "mesh2.rpc.1";
    let before = estate.node_value(NODE).await;
    estate.kill_node(NODE).await;
    let after = until_new_birth(&estate, NODE, &before, SETTLE).await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, loaded from the inherited data dir");
    let submits = |spans: &[Value]| -> Vec<Value> { named(spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().collect() };
    let done = wait_for("the completed recovery submit is exported", Duration::from_secs(60), || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("complete")) }).await;
    assert_eq!(
        (s(&done["attributes"]["submitter"]), s(&done["attributes"]["mesh"]), s(&done["attributes"]["build_id"]), s(&done["attributes"]["disposition"])),
        (mp.clone(), "mesh2".to_string(), build.clone(), "created".to_string()),
        "mesh2's primary opened it in mesh2's lane of the accepted Build: {done}"
    );
    let attempt = s(&done["attributes"]["attempt"]);
    let spans = wait_for("the recovery's reconcile is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        lane_reconcile_span(&in_trace(&spans, &done), &build, "mesh2", &attempt).is_some().then_some(spans)
    })
    .await;
    let run = in_trace(&spans, &done);
    assert_mesh_primary_ran_the_recovery(&run, &mp, "mesh2", &build, &attempt, &before);
    // No other admin submitted, opened or claimed it, and no attempt of the fabric lane acted on it.
    for sp in submits(&spans) {
        assert_eq!(s(&sp["attributes"]["submitter"]), mp, "only mesh2's primary submits its member's recovery: {sp}");
    }
    let submit_traces: std::collections::BTreeSet<String> = submits(&spans).iter().map(|sp| s(&sp["trace_id"])).collect();
    for sp in named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]) {
        assert!(submit_traces.contains(&s(&sp["trace_id"])), "every open of the recovery is under mesh2's primary's submit: {sp}");
    }
    for sp in named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == build && sp["attributes"]["lane"] == "mesh2") {
        assert_eq!(s(&sp["attributes"]["node"]), mp, "only mesh2's primary decides a claim in mesh2's lane: {sp}");
    }
    let member_ops: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| s(&r["attributes"]["build_id"]) == build && s(&r["attributes"]["operations"]).contains(&format!("create-node:{NODE}")) && start(r) >= start(&done)).collect();
    assert!(member_ops.iter().all(|r| r["attributes"]["lane"] == "mesh2" && s(&r["attributes"]["executor"]) == mp), "only mesh2's lane acts on the member after its exit: {member_ops:?}");
    // The stopped fabric-primary stayed stopped through the recovery, and the fabric-primary that
    // holds the seat now learns the receipts from the Build topic.
    let still = estate.node_value(&fp_name).await;
    assert_eq!((still["parked"].clone(), still["incarnation_id"].clone()), (json!(true), fp["incarnation_id"].clone()), "{fp_name} stayed stopped: {still}");
    let (deleted, created) = fp_recovery_receipts(&estate, &Estate::path(&successor_name), &build, "mesh2", NODE, &attempt).await;
    let open = named(&run, "rdm.node_admin.build.update.via-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().expect("the open");
    let claim = named(&run, "rdm.node_admin.build.update.via-claim-decision").into_iter().find(|sp| s(&sp["attributes"]["attempt"]) == attempt).cloned().expect("the claim");
    eprintln!(
        "s78 acceptance 1: fabric-primary {fp_name} stopped, seat at {successor_name}; {NODE} recovered by {mp} at attempt {attempt} of mesh2's lane of {build}: trace {} | via-node-recovery span {} | via-recovery span {} outcome {} | via-claim-decision span {} node {} outcome {} | {successor_name} holds receipts {} and {}",
        s(&done["trace_id"]), s(&done["span_id"]), s(&open["span_id"]), s(&open["attributes"]["outcome"]), s(&claim["span_id"]), s(&claim["attributes"]["node"]), s(&claim["attributes"]["outcome"]), s(&deleted["step"]), s(&created["step"]),
    );
    estate.record_trace_url(done["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}

/// CONTRACT (R-BI7 caveat 2: each mesh-primary claims its mesh's drift attempts on its mesh's rows;
/// build.md :365 as the approved proposal A4 words it: "The next attempt is claimed after the running
/// attempt's terminal receipt (Complete, Failed or HandedOff), or, when the executor is lost with the
/// attempt unfinished, after exact departure proof"): mesh2 has two node-admins, each the testkit's
/// faulted node-admin. `mesh2.rpc.1` is killed; mesh2's primary P opens and claims its recovery as an
/// attempt of mesh2's lane and runs it. The fault door holds P inside the run, at the member's
/// node.delete NodeDeleted receipt (the `receipt` cut: NodeDeleting is durable and its pre-notice
/// published, the departure is proven, the NodeDeleted receipt is not yet durable). Once NA2's own
/// Build log holds that attempt running under P's claim with its NodeDeleting receipt, P is killed
/// while the cut holds it (claimed, mid-run, no verdict). mesh2's other admin NA2
/// takes mesh2's seat, proves P's exit and submits P's recovery: at NA2's own door the attempt P left
/// running ends Failed on that proof (`build.update.via-departed-claimant`, outcome `ended`, naming P's
/// exact birth), then NA2 claims the next attempt of mesh2's lane for P's recovery
/// (`build.update.via-claim-decision`, NA2, won), in that order, in the submit's own trace. P comes
/// back as a new node, and the member's recovery, owed again from the Failed attempt, completes under
/// NA2. Must NOT happen: mesh2's lane held for good by the lost claimant, or another admin claiming
/// in mesh2's lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_whose_mesh_primary_exits_mid_run_is_taken_over_by_the_new_mesh_primary_on_its_exit_proof() {
    let (estate, build) = faulted_fabric_of("a_recovery_whose_mesh_primary_exits_mid_run_is_taken_over_by_the_new_mesh_primary_on_its_exit_proof", &[("mesh1", 1, 1), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let nodes = estate.node_values().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    let p = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().expect("mesh2's primary");
    let p_name = s(&p["name"]);
    let na2 = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != p["name"]).expect("mesh2's other node-admin")["name"]);
    assert!(!fp.starts_with("mesh2."), "the fabric-primary is not in mesh2: {fp}");

    // The member's recovery is the one node.delete of it P runs: the cut holds P at its NodeDeleted
    // receipt until P is killed.
    const NODE: &str = "mesh2.rpc.1";
    let door = Door::at(&estate, &p_name).await;
    door.arm("member-deleted", json!({"kind": "receipt", "step": "NodeDeleted", "operation": "retire-node", "node": NODE})).await;
    let before = estate.node_value(NODE).await;
    estate.kill_node(NODE).await;
    let held = door.wait_held("member-deleted").await;
    assert_eq!((s(&held["hit"]["build_id"]), s(&held["hit"]["operation"])), (build.clone(), format!("retire-node:{NODE}")), "P holds the member's NodeDeleted receipt of the accepted Build: {held}");
    let lost_attempt = held["hit"]["attempt"].as_u64().expect("the held receipt names its attempt");
    // NA2 heard the claim and the NodeDeleting receipt on the Build topic: in its own log mesh2's lane
    // runs that attempt for the member's recovery, claimed by P, with no verdict.
    let heard = build_log::build_fact(&estate.carrier_at(&Estate::path(&na2)).await, &build, &format!("attempt {lost_attempt} of mesh2's lane running under {p_name}'s claim, its NodeDeleting durable"), SETTLE, |pr| {
        let b = serde_json::to_value(pr).expect("a Build view serializes");
        let lane = &b["lanes"]["mesh2"];
        let deleting = lane_steps(&b, "mesh2").iter().any(|r| r["operation"] == format!("retire-node:{NODE}").as_str() && r["step"] == "NodeDeleting" && r["attempt"] == json!(lost_attempt) && r["outcome"] == "complete");
        deleting && lane["attempt"] == json!(lost_attempt) && lane["state"] == "running" && lane["claimant"] == p_name.as_str() && lane["action"]["action"] == "recover" && lane["action"]["path"] == NODE
    })
    .await
    .unwrap_or_else(|end| panic!("{end}"));
    estate.kill_node(&p_name).await;

    // NA2 takes the lane over on P's exit proof, then claims the next attempt for P's recovery.
    let ended = wait_for(&format!("{na2} ends attempt {lost_attempt} of mesh2's lane on {p_name}'s exit proof"), SETTLE, || async {
        named(&estate.spans(), "rdm.node_admin.build.update.via-departed-claimant")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == na2.as_str() && sp["attributes"]["claimant"] == p_name.as_str() && s(&sp["attributes"]["attempt"]) == lost_attempt.to_string() && sp["attributes"]["outcome"] == "ended")
            .cloned()
    })
    .await;
    assert_eq!((s(&ended["attributes"]["lane"]), s(&ended["attributes"]["node_id"]), s(&ended["attributes"]["incarnation_id"])), ("mesh2".to_string(), s(&p["node_id"]), s(&p["incarnation_id"])), "the take-over names P's exact birth: {ended}");
    let spans = wait_for("the take-over's claim and submit are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let run = in_trace(&spans, &ended);
        let claimed = named(&run, "rdm.node_admin.build.update.via-claim-decision").into_iter().any(|sp| sp["attributes"]["node"] == na2.as_str());
        let submitted = named(&run, "rdm.node_admin.build.update.via-node-recovery").into_iter().any(|sp| sp["attributes"]["submitter"] == na2.as_str());
        (claimed && submitted).then_some(spans)
    })
    .await;
    let run = in_trace(&spans, &ended);
    let submit = named(&run, "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| sp["attributes"]["submitter"] == na2.as_str()).cloned().expect("NA2's submit");
    assert_eq!((s(&submit["attributes"]["node"]), s(&submit["attributes"]["node_id"])), (p_name.clone(), s(&p["node_id"])), "the take-over is NA2's submit of P's recovery: {submit}");
    let claim = named(&run, "rdm.node_admin.build.update.via-claim-decision").into_iter().find(|sp| sp["attributes"]["node"] == na2.as_str()).cloned().expect("NA2's claim");
    assert_eq!(
        (s(&claim["attributes"]["lane"]), s(&claim["attributes"]["attempt"]), s(&claim["attributes"]["executor"]), s(&claim["attributes"]["outcome"])),
        ("mesh2".to_string(), (lost_attempt + 1).to_string(), na2.clone(), "won".to_string()),
        "NA2 claimed the next attempt of mesh2's lane: {claim}"
    );
    assert!(end(&ended) <= start(&claim), "the lost attempt ended before the next was claimed");
    // P and the member come back as new nodes; the member's recovery completes under NA2.
    for (path, old) in [(p_name.as_str(), &p), (NODE, &before)] {
        until_new_birth(&estate, path, old, SETTLE).await;
    }
    let member = wait_for("the member's completed recovery submit is exported", Duration::from_secs(60), || async {
        named(&estate.spans(), "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"] && s(&sp["attributes"]["outcome"]).starts_with("complete")).cloned()
    })
    .await;
    assert_eq!((s(&member["attributes"]["submitter"]), s(&member["attributes"]["mesh"])), (na2.clone(), "mesh2".to_string()), "the member's recovery completes under NA2 in mesh2's lane: {member}");
    let spans = estate.spans();
    for sp in named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == build && sp["attributes"]["lane"] == "mesh2") {
        assert!([p_name.as_str(), na2.as_str()].contains(&s(&sp["attributes"]["node"]).as_str()), "only mesh2's primaries decide claims in mesh2's lane: {sp}");
    }
    eprintln!(
        "s78 gap 1 live: {p_name} held by the fault door at the member's NodeDeleted receipt {} and killed running attempt {lost_attempt} of mesh2's lane of {build} ({na2} held it running under {:?}); take-over trace {} | via-node-recovery span {} submitter {na2} node {p_name} | via-departed-claimant span {} outcome {} claimant {} exit_proof {} | via-claim-decision span {} attempt {} outcome {} | member complete trace {} attempt {}",
        held["hit"], heard.lanes.get("mesh2").and_then(|l| l.claimant.clone()), s(&ended["trace_id"]), s(&submit["span_id"]), s(&ended["span_id"]), s(&ended["attributes"]["outcome"]), s(&ended["attributes"]["claimant"]), s(&ended["attributes"]["exit_proof"]), s(&claim["span_id"]), s(&claim["attributes"]["attempt"]), s(&claim["attributes"]["outcome"]), s(&member["trace_id"]), s(&member["attributes"]["attempt"]),
    );
    estate.record_trace_url(ended["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}
