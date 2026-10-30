//! product=mesh, feature=node-lifecycle, subfeature=node-replace, rung=MN, provider=process.
//!
//! A planned replace (Luke R-R1) and node recovery (node-recovery.md, R-RC1). A planned
//! replacement is the next attempt of the accepted Build with a typed action; no Build is minted.
//! A member that exits outside any workflow is recovered by its own mesh-primary on the provider's
//! exact exited verdict: node.delete of the exited instance, then node.create at the same path.name
//! over the inherited data dir (same endpoint key, new node.id). A member that is hung but still
//! running is never recovered; a repeat submit is answered, never run twice; an exit during a
//! workflow fails that workflow and the next tick recovers the member.

use crate::build_log;
use rdm_client::{BuildFrame, CallEnd, Change, Frame, NodeEvent, NodeStatus, Nodes, PathName};
use rdm_core::build::Step;
use rdm_test::estate::{descends_from, named, wait_for, Estate, Owner};
use rdm_test::steps::{predecessor_steps, replace as replace_steps, rollback as rollback_steps};
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
    let estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
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

/// The member under test in a two-mesh estate: an rpc node holding no role, of the mesh that does NOT hold the
/// fabric-primary, so its mesh-primary is not the fabric-primary; that mesh-primary's path.name; and
/// the fabric-primary's view row.
fn placement(nodes: &[Value]) -> (String, String, Value) {
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh = if primary["mesh"] == "mesh1" { "mesh2" } else { "mesh1" };
    let mesh_primary = s(&nodes.iter().find(|n| n["mesh"] == mesh && n["kind"] == "node_admin" && n["is_primary"] == true).expect("the node's mesh-primary")["name"]);
    assert_ne!(mesh_primary, s(&primary["name"]), "the member's mesh-primary is not the fabric-primary");
    // The member is an rpc node holding no role: a node-type primary runs node.primary.update before its
    // own node.connection.delete, stop or delete (lifecycles/node-connections-delete.md step 0), which
    // is not the member workflow these cells follow.
    let member = nodes.iter().filter(|n| n["mesh"] == mesh && n["kind"] == "rpc_node" && n["is_primary"] != true).map(|n| s(&n["name"])).min().expect("an rpc member of the mesh that holds no primary role");
    (member, mesh_primary, primary)
}

/// The exact runtime `node`'s birth published in its data dir: the fault door acts on it alone.
async fn exact_runtime(estate: &Estate, node: &str) -> ExactRuntime {
    let dir = estate.data_dir(node).await;
    ExactRuntime::published(Path::new(&dir)).unwrap_or_else(|r| panic!("{node}: no exact runtime published in {dir}: {r:?}"))
}

/// The highest attempt of `mesh`'s drift lane of Build `build` as the fabric's Build records hold it
/// (R-BI7: a recovery is an attempt of its mesh's lane, numbered per mesh from 1); 0 for a lane never
/// opened.
async fn mesh_attempt_of(estate: &Estate, build: &str, mesh: &str) -> u64 {
    let b = build_log::build_value(&estate, build).await;
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

/// The run of the recovery of `before` at `attempt` of `lane` that the submit `done` completed: its
/// own trace and, when an earlier report opened that attempt (`done`'s open is `held-open`), the
/// attempt's execution under the open that opened it. The fabric-primary runs an attempt in the
/// context recorded when it was opened (build_op.rs, `contexts.get_lane`), so its
/// `build.attempt.run` and reconcile continue the opening report's trace, not the completing one's.
fn recovery_run(spans: &[Value], done: &Value, before: &Value, lane: &str, attempt: &str) -> Vec<Value> {
    let mut run = in_trace(spans, done);
    let opened = named(spans, "rdm.node_admin.build.update.via-recovery").into_iter().find(|sp| {
        sp["attributes"]["node_id"] == before["node_id"]
            && sp["attributes"]["mesh"] == lane
            && s(&sp["attributes"]["attempt"]) == attempt
            && sp["attributes"]["open_attempt"] == "opened-here"
            && sp["trace_id"] != done["trace_id"]
    });
    if let Some(open) = opened {
        run.extend(in_trace(spans, open).into_iter().filter(|sp| descends_from(spans, sp, open)));
        run.sort_by_key(start);
    }
    run
}

/// Whether `run` holds the fabric-primary's spans of the reported recovery of `before`: its serve of
/// the report (the open's parent) and its run of `attempt`. They close with the report's stream, the
/// instant the submit does, but in the fabric-primary's process, whose evidence file is written on
/// its own 200 ms batch: a wait on the mesh-primary's spans alone does not hold them.
fn report_served(run: &[Value], before: &Value, attempt: &str) -> bool {
    let Some(open) = named(run, "rdm.node_admin.build.update.via-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]) else { return false };
    run.iter().any(|sp| sp["span_id"] == open["parent_span_id"])
        && named(run, "rdm.node_admin.build.update.via-drift-run").into_iter().any(|sp| s(&sp["attributes"]["attempt"]) == attempt)
}

/// Whether `sp` is a Node RPC call of `op` (the call span names the op number).
fn calls_op(sp: &Value, op: u8) -> bool {
    sp["attributes"]["op"] == op.to_string().as_str() || sp["attributes"]["op"] == json!(op)
}

/// The completed recovery of `before` at `attempt` of `mesh`'s drift lane of `build`, in the submit's
/// own trace `run` (node-recovery.md "Execution", Q118; "Observability contract"). Its mesh-primary
/// `mp` submits it (`via-node-recovery`, the root). When `mp` is not the fabric-primary `fp`, `mp`
/// reports the drift up as `build.create` (op 0x20) and `fp` opens it (`via-recovery`, route
/// `build.create`, outcome `reported`, under `fp`'s serve of the call); `mp` claims it through
/// BuildClaim (op 0x1C) and `fp` decides the claim for executor `mp` in `mesh`'s lane; `fp` runs it
/// (`via-drift-run`, complete), calling `build.attempt.run` naming the mesh. When `mp` is the
/// fabric-primary, it opens (route `node-recovery`, admitted) and claims at its own door, with no Build
/// call, no claim call and no dispatch. Either way `mp` runs the attempt (`via-attempt-run`, lane
/// `mesh`, executor `mp`) and its reconcile converges in `mesh`'s lane.
fn assert_mesh_primary_ran_the_recovery(
    run: &[Value],
    mp: &str,
    fp: &str,
    mesh: &str,
    build: &str,
    attempt: &str,
    before: &Value,
) {
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let submit = named(run, "rdm.node_admin.build.update.via-node-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == before["node_id"])
        .expect("the submit roots the run");
    assert_eq!(attr(submit, "submitter"), mp, "{mp} submits: {submit}");
    let open = named(run, "rdm.node_admin.build.update.via-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == before["node_id"])
        .unwrap_or_else(|| panic!("the recovery's open"));
    let claim = named(run, "rdm.node_admin.build.update.via-claim-decision")
        .into_iter()
        .find(|sp| attr(sp, "attempt") == attempt && attr(sp, "lane") == mesh)
        .unwrap_or_else(|| panic!("the claim decision of attempt {attempt} of {mesh}'s lane"));
    let calls = |op: u8| {
        named(run, "rdm.node_rpc.request.update.via-call")
            .into_iter()
            .filter(|sp| calls_op(sp, op))
            .count()
    };
    if mp == fp {
        assert_eq!(
            (
                attr(open, "route"),
                attr(open, "outcome"),
                attr(open, "mesh"),
                attr(open, "build_id"),
                attr(open, "attempt")
            ),
            (
                "node-recovery".to_string(),
                "admitted".to_string(),
                mesh.to_string(),
                build.to_string(),
                attempt.to_string()
            ),
            "the open at the fabric-primary's own door, attempt {attempt} of {mesh}'s lane: {open}"
        );
        assert_eq!(
            open["parent_span_id"], submit["span_id"],
            "the open is the submit's own in-process call: {open}"
        );
        assert_eq!(
            (
                attr(claim, "node"),
                attr(claim, "executor"),
                attr(claim, "outcome")
            ),
            (mp.to_string(), mp.to_string(), "won".to_string()),
            "decided at its own door: {claim}"
        );
        assert_eq!(
            (calls(0x20), calls(0x1C)),
            (0, 0),
            "no Build call and no claim call in the recovery's run"
        );
        assert!(
            named(run, "rdm.node_admin.build.update.via-drift-run").is_empty(),
            "no reported drift run"
        );
    } else {
        assert_eq!(
            (attr(open, "route"), attr(open, "outcome"), attr(open, "mesh"), attr(open, "build_id"), attr(open, "attempt")),
            ("build.create".to_string(), "reported".to_string(), mesh.to_string(), build.to_string(), attempt.to_string()),
            "the fabric-primary opens the reported drift at attempt {attempt} of {mesh}'s lane: {open}"
        );
        let serve = run
            .iter()
            .find(|sp| sp["span_id"] == open["parent_span_id"])
            .unwrap_or_else(|| panic!("the open's parent: {open}"));
        assert_eq!(
            (s(&serve["name"]), attr(serve, "node"), attr(serve, "call")),
            (
                "rdm.node_admin.build.serve.via-node-rpc".to_string(),
                fp.to_string(),
                "create".to_string()
            ),
            "the open is {fp}'s serve of the report: {serve}"
        );
        assert!(calls(0x20) >= 2 && calls(0x1C) == 1, "the report (op 0x20), the claim (op 0x1C) and build.attempt.run (op 0x20) in the recovery's run");
        assert_eq!(
            (
                attr(claim, "node"),
                attr(claim, "executor"),
                attr(claim, "outcome")
            ),
            (fp.to_string(), mp.to_string(), "won".to_string()),
            "{fp} decides {mp}'s claim: {claim}"
        );
        let drift = named(run, "rdm.node_admin.build.update.via-drift-run")
            .into_iter()
            .find(|sp| attr(sp, "attempt") == attempt)
            .unwrap_or_else(|| panic!("{fp}'s run of the reported attempt"));
        assert_eq!(
            (
                attr(drift, "node"),
                attr(drift, "lane"),
                attr(drift, "executor"),
                attr(drift, "outcome")
            ),
            (
                fp.to_string(),
                mesh.to_string(),
                mp.to_string(),
                "complete".to_string()
            ),
            "{drift}"
        );
    }
    let started = named(run, "rdm.node_admin.build.serve.via-attempt-run")
        .into_iter()
        .find(|sp| attr(sp, "attempt") == attempt && attr(sp, "lane") == mesh)
        .expect("the attempt's run");
    assert_eq!(
        (attr(started, "node"), attr(started, "executor")),
        (mp.to_string(), mp.to_string()),
        "{started}"
    );
    let reconcile = named(run, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|sp| attr(sp, "build_id") == build && attr(sp, "attempt") == attempt)
        .expect("the attempt's reconcile");
    assert_eq!(
        (
            attr(reconcile, "lane"),
            attr(reconcile, "executor"),
            attr(reconcile, "outcome")
        ),
        (mesh.to_string(), mp.to_string(), "converged".to_string()),
        "{reconcile}"
    );
}

/// The receipts of the recovery of `node` at `attempt` of `mesh`'s drift lane a Build holds: its
/// node.delete's `node.delete:<node>` NodeDeleted receipt, and the Complete of its last operation,
/// node.create (`node.create:<node>`).
fn recovery_receipts(b: &Value, mesh: &str, node: &str, attempt: &str) -> Option<(Value, Value)> {
    let (delete, create) = (format!("node.delete:{node}"), format!("node.create:{node}"));
    let steps = lane_steps(b, mesh);
    let at = |operation: &str, step: &str| steps.iter().find(|r| r["operation"] == operation && r["step"] == step && r["attempt"].to_string() == attempt && r["outcome"] == "complete").cloned();
    Some((at(&delete, "NodeDeleted")?, at(&create, "Complete")?))
}

/// The fabric's Build records hold the recovery of `node` at `attempt` of `mesh`'s drift
/// lane (node-recovery.md "Execution", Q118: the fabric-primary writes every Build record, from the
/// receipts the executor reports up the stream): its node.delete's `node.delete:<node>` NodeDeleted receipt, and the Complete of its
/// last operation, node.create (`node.create:<node>`).
async fn recovery_receipts_held(estate: &Estate, build: &str, mesh: &str, node: &str, attempt: &str) -> (Value, Value) {
    let held = build_log::build_fact(&estate, build, &format!("the recovery's receipts in {mesh}'s lane"), Duration::from_secs(30), |p| {
        recovery_receipts(&serde_json::to_value(p).expect("a Build view serializes"), mesh, node, attempt).is_some()
    })
    .await
    .unwrap_or_else(|end| panic!("{end}"));
    recovery_receipts(&serde_json::to_value(held).expect("a Build view serializes"), mesh, node, attempt).expect("the receipts were just held")
}

/// CONTRACT: a rpc node whose mesh transport stops for good exits (reserved code 4, recorded in
/// its data dir). Its mesh-primary (node-recovery.md), not the fabric-primary, proves the exit:
/// it marks the node TRUE OFFLINE, asks the provider about the exact runtime, and on Exited reports
/// the drift up as `build.create { Change(Recover) }` keyed on the exited birth (node-recovery.md
/// "Execution", Q118). The fabric-primary opens it as the next attempt of the mesh's lane of the
/// accepted Build (no Build minted), the mesh-primary claims it through BuildClaim, and the
/// fabric-primary calls `build.attempt.run` naming the mesh; the mesh-primary runs it: node.delete
/// of the exited instance (node.deleted from the exited verdict, once, named by the exact birth), then
/// node.create at the same path.name over the inherited data dir, and nothing after it. The
/// fabric-primary writes the receipts the run reports up the stream. The path comes back with the SAME transport key and data dir and a NEW node.id and
/// incarnation. No other node changes, and the fabric-primary's drift check opens nothing for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_stopped_exit_is_restarted_as_the_same_node() {
    let (estate, birth_build) =
        two_mesh_estate("a_transport_stopped_exit_is_restarted_as_the_same_node").await;
    // The node lives in the mesh that does NOT hold the fabric-primary: its own mesh-primary reports
    // the drift to the fabric-primary, claims the attempt and runs it.
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
    assert_eq!(
        s(&recovery["attributes"]["submitter"]),
        mesh_primary,
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
    assert_eq!(
        s(&recovery["attributes"]["build_id"]),
        birth_build,
        "the recovery is an attempt of the accepted Build: {recovery}"
    );
    assert_eq!(
        s(&recovery["attributes"]["disposition"]),
        "created",
        "the fabric-primary opened it: {recovery}"
    );
    assert_eq!(
        s(&recovery["attributes"]["mesh"]),
        mesh,
        "an attempt of the member's mesh's lane: {recovery}"
    );
    let attempt = s(&recovery["attributes"]["attempt"]);
    // The submit followed the recovery to its terminal frame and records it (never "submitted").
    assert_eq!(
        s(&recovery["attributes"]["outcome"]),
        format!("complete: attempt {attempt}"),
        "the recovery's terminal: {recovery}"
    );
    let build = build_log::attempt_complete(
        &estate,
        &birth_build,
        attempt.parse().expect("an attempt"),
        SETTLE,
    )
    .await;
    assert_eq!(
        build.build_id.as_str(),
        birth_build,
        "no Build was minted: {build:?}"
    );
    // The fabric-primary wrote the recovery's receipts (its recovery dedup reads them), in the mesh's
    // lane: the delete's NodeDeleted naming the exited birth, and the create's Complete.
    let heard = recovery_receipts_held(
        &estate,
        &birth_build,
        mesh,
        node_path,
        &attempt,
    )
    .await;
    assert_eq!(
        heard.0["output"]["node_id"], before["node_id"],
        "the fabric-primary's NodeDeleted names the exited node: {:?}",
        heard.0
    );
    assert_eq!(
        heard.0["output"]["incarnation"], before["incarnation_id"],
        "and its exact birth: {:?}",
        heard.0
    );
    assert!(
        heard.1["output"].is_null(),
        "Complete commits no output: {:?}",
        heard.1
    );

    // Each process exports its own spans: the new node's boot span is read once the new node exported it.
    let spans = wait_for("the recovery's last operation, the fabric-primary's serve of its report, the exited birth's target and the new node's boot span are exported", SETTLE, || async {
        let spans = estate.spans();
        let run = in_trace(&spans, &recovery);
        (create_span(&run, &birth_build, &attempt, node_path).is_some() && (mesh_primary == s(&primary["name"]) || report_served(&run, &before, &attempt)) && !exited_birth_spans(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // The recovery's own run, one trace from the mesh-primary's submit: the report, the fabric-primary's
    // open, the claim it decides, its run naming the mesh, and the attempt's run and reconcile.
    let run = in_trace(&spans, &recovery);
    assert_mesh_primary_ran_the_recovery(
        &run,
        &mesh_primary,
        &s(&primary["name"]),
        mesh,
        &birth_build,
        &attempt,
        &before,
    );
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
    let create = named(&run, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == node_path)
        .expect("the recovery's create operation span");
    let targets = exited_birth_spans(&spans, &before);
    assert_eq!(targets.len(), 1, "the exited birth is targeted once: {targets:?}");
    assert!(start(create) <= start(targets[0]) && end(targets[0]) <= end(create), "the target runs inside the create operation: {} vs {create}", targets[0]);
    assert!(end(targets[0]) <= start(boot), "the target is done before the new node with the same key boots: {} vs {boot}", targets[0]);
    for step in ["StopNode", "AwaitNodeLeft"] {
        assert!(step_span(&run, &birth_build, &attempt, node_path, step).is_none(), "no RPC to the exited process: {step} ran");
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
/// node.delete of the exited instance (`node.delete:<path>`: no RPC to it, no stop frames,
/// node.deleted once named by its exact birth and before the new node boots), then node.create at
/// the same path.name over the inherited data dir (`node.create:<path>`), each as its own workflow's
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
    assert_eq!(mesh_primary, primary, "one mesh: its primary holds the fabric-primary role too");
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
    assert_eq!(s(&recovery["attributes"]["verdict"]), "exited", "on the verdict exited: {recovery}");
    assert_eq!(s(&recovery["attributes"]["build_id"]), birth_build, "the recovery is an attempt of the accepted Build: {recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "the recovery's terminal: {recovery}");

    // Each process exports its own spans: the successor's boot span is read once the successor exported it.
    let spans = wait_for("the recovery's last operation, its reconcile span, the exited birth's target and the successor's boot span are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let run = in_trace(&spans, &recovery);
        (create_span(&run, &birth_build, &attempt, NODE).is_some() && lane_reconcile_span(&run, &birth_build, "mesh1", &attempt).is_some() && !exited_birth_spans(&spans, &before).is_empty() && booted(&spans, &after)).then_some(spans)
    })
    .await;
    // The recovery's own run: the attempt number is mesh1's lane's, which the fabric lane's numbering
    // also holds, so its spans are read in the submit's trace.
    let run = in_trace(&spans, &recovery);
    assert_mesh_primary_ran_the_recovery(
        &run,
        &mesh_primary,
        &mesh_primary,
        "mesh1",
        &birth_build,
        &attempt,
        &before,
    );
    let reconcile = lane_reconcile_span(&run, &birth_build, "mesh1", &attempt)
        .expect("the attempt's reconcile span");
    assert_eq!(
        s(&reconcile["attributes"]["operations"]),
        format!("node.delete:{NODE},node.create:{NODE}"),
        "the recovery's two operations, in order: {reconcile}"
    );
    let mut last = 0u64;
    for step in ["NodeDeleting", "NodeDeleted", "RemoveTopologyMembership", "DeployRuntime", "WaitForNodeReady"] {
        let sp = step_span(&run, &birth_build, &attempt, NODE, step).unwrap_or_else(|| panic!("the recovery's {step} step span"));
        assert!(start(sp) >= last, "{step} runs after the step before it");
        last = start(sp);
    }
    for step in ["StopNode", "AwaitNodeLeft"] {
        assert!(step_span(&run, &birth_build, &attempt, NODE, step).is_none(), "no RPC to the exited process: {step} ran");
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
    let create = named(&run, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == NODE)
        .expect("the recovery's create operation span");
    let targets = exited_birth_spans(&spans, &before);
    assert_eq!(targets.len(), 1, "the exited birth is targeted once: {targets:?}");
    assert!(start(create) <= start(targets[0]) && end(targets[0]) <= end(create), "the target runs inside the create operation: {} vs {create}", targets[0]);
    assert!(end(targets[0]) <= start(boot), "the target is done before the new node with the same key boots: {} vs {boot}", targets[0]);
    // Each operation is its workflow: its node operation span and its own pipeline, never a recovery
    // pipeline of its own.
    for (op, pipeline) in [("delete", "delete"), ("create", "create")] {
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
    let build = build_log::build_value(&estate, &birth_build).await;
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
    assert!(outcome.contains(&format!("node.create:{NODE} step PrepareStorage")) && outcome.contains("node-key is missing"), "the create refused the dir with no key by name: {failed}");
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
    let build = build_log::build_value(&estate, &birth_build).await;
    let steps = lane_steps(&build, "mesh1");
    let deleted: Vec<&Value> = steps.iter().filter(|r| r["operation"] == format!("node.delete:{NODE}").as_str() && r["step"] == "NodeDeleted").collect();
    assert_eq!(deleted.len(), 1, "one node.delete of the exited birth, never again: {deleted:?}");
    assert!(steps.iter().all(|r| !(r["operation"] == format!("node.delete:{NODE}").as_str() && r["attempt"] == json!(attempt))), "the completing attempt deletes nothing");
    let spans = wait_for("the completing attempt's reconcile span is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        lane_reconcile_span(&in_trace(&spans, &done), &birth_build, "mesh1", &attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    let run = in_trace(&spans, &done);
    let reconcile = lane_reconcile_span(&run, &birth_build, "mesh1", &attempt.to_string()).expect("the attempt's reconcile span of mesh1's lane");
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("node.create:{NODE}"), "only what was left: {reconcile}");
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
/// takes the role. Once no attempt for the member is in flight, the key is put back. The new
/// mesh-primary never submitted for the member, never inspects it (its birth deleted) and holds
/// nothing of it in memory: it reads the accepted Build's Failed attempt and reports the same birth's
/// recovery to the fabric-primary in mesh1, which opens it; the new mesh-primary claims it through
/// BuildClaim and runs it (node-recovery.md "Execution", Q118). A later attempt of mesh2's lane of the same Build runs
/// only the create, and the path comes back as a new node over the same data dir and
/// endpoint key. Must NOT happen: the path left empty after the role moved, a second node.delete of
/// the exited birth, a Build minted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_build_for_an_exited_member_is_submitted_again_by_the_admin_that_takes_over_mesh_primary() {
    let (estate, birth_build) = fabric_of("a_failed_build_for_an_exited_member_is_submitted_again_by_the_admin_that_takes_over_mesh_primary", &[("mesh1", 1, 1), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let nodes = estate.node_values().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    assert!(fp.starts_with("mesh1."), "the day-0 admin keeps the fabric-primary role: {fp}");
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
    let failed = wait_for("the create of the Build for the exited verdict fails at PrepareStorage", SETTLE, || async { submits(&estate.spans()).into_iter().find(|sp| s(&sp["attributes"]["outcome"]).starts_with("failed")) }).await;
    let failed_attempt: u64 = s(&failed["attributes"]["attempt"]).parse().expect("the failed submit names its attempt");
    let outcome = s(&failed["attributes"]["outcome"]);
    assert!(outcome.contains(&format!("node.create:{NODE} step PrepareStorage")) && outcome.contains("node-key is missing"), "the create refused the dir with no key by name: {failed}");
    assert_eq!((s(&failed["attributes"]["submitter"]), s(&failed["attributes"]["mesh"]), s(&failed["attributes"]["build_id"])), (mp.clone(), "mesh2".to_string(), birth_build.clone()), "{mp} opened it in mesh2's lane: {failed}");

    // mesh2's role moves: its primary is killed with the key still missing, so nothing it submitted
    // can complete, and its second admin takes the role.
    estate.kill_node(&mp).await;
    let role = wait_for(&format!("{heir} takes mesh2's role"), SETTLE, || async {
        named(&estate.spans(), "rdm.mesh.election.resolve.via-mesh-primary")
            .into_iter()
            .find(|sp| sp["attributes"]["observer"] == heir.as_str() && sp["attributes"]["winner_path"] == heir.as_str() && start(sp) > start(&failed))
            .cloned()
    })
    .await;
    // No attempt for the member is in flight in mesh2's lane (one the killed primary was running
    // when it died is ended by the admin that took the role, on the killed primary's exited verdict, and
    // fails on the missing key): only a later submit can bring the path back.
    let idle = build_log::build_fact(&estate, &birth_build, &format!("no attempt for {NODE} is in flight in mesh2's lane"), SETTLE, |p| {
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

    let after = until_node_where(&estate, NODE, &format!("{NODE} ready as a new node after mesh2's role moved"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    assert_eq!(after["endpoint_id"], before["endpoint_id"], "the same endpoint key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir(NODE).await, data_dir, "the same data dir");
    let done = wait_for(
        "the completed submit of the exited birth is exported",
        Duration::from_secs(60),
        || async {
            submits(&estate.spans())
                .into_iter()
                .find(|sp| s(&sp["attributes"]["outcome"]).starts_with("complete"))
        },
    )
    .await;
    assert_eq!(
        (
            s(&done["attributes"]["submitter"]),
            s(&done["attributes"]["mesh"])
        ),
        (heir.clone(), "mesh2".to_string()),
        "the admin that took the role opened it in mesh2's lane: {done}"
    );
    assert_eq!(
        s(&done["attributes"]["disposition"]),
        "created",
        "the fabric-primary opened the next attempt: {done}"
    );
    assert_eq!(
        s(&done["attributes"]["build_id"]),
        birth_build,
        "the same Build, no Build minted: {done}"
    );
    assert_eq!(
        done["attributes"]["incarnation_id"], before["incarnation_id"],
        "keyed on the exact exited birth: {done}"
    );
    let attempt: u64 = s(&done["attributes"]["attempt"])
        .parse()
        .expect("the completed submit names its attempt");
    assert!(
        attempt > failed_attempt,
        "a later attempt than the failed one: {done}"
    );
    assert_eq!(
        s(&done["attributes"]["outcome"]),
        format!("complete: attempt {attempt}"),
        "{done}"
    );
    assert!(
        s(&done["attributes"]["after_failed_attempt"])
            .parse::<u64>()
            .is_ok_and(|a| a >= failed_attempt && a < attempt),
        "the submit follows the Failed attempt the Build holds: {done}"
    );

    let spans = wait_for(
        "the completing attempt's reconcile span and the fabric-primary's serve of its report are exported",
        Duration::from_secs(30),
        || async {
            let spans = estate.spans();
            let run = recovery_run(&spans, &done, &before, "mesh2", &attempt.to_string());
            (lane_reconcile_span(&run, &birth_build, "mesh2", &attempt.to_string()).is_some()
                && (heir == fp || report_served(&run, &before, &attempt.to_string())))
            .then_some(spans)
        },
    )
    .await;
    // The admin that took the role reported it, claimed it and ran it, at that attempt of mesh2's lane
    // the fabric-primary opened (its run under the report that opened it, when that was an earlier one).
    let run = recovery_run(&spans, &done, &before, "mesh2", &attempt.to_string());
    assert_mesh_primary_ran_the_recovery(
        &run,
        &heir,
        &fp,
        "mesh2",
        &birth_build,
        &attempt.to_string(),
        &before,
    );
    let admitted = named(&run, "rdm.node_admin.build.update.via-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == before["node_id"])
        .expect("the open of the completing attempt");
    assert!(
        !named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").iter().any(|sp| sp["attributes"]["node_id"] == before["node_id"] && sp["attributes"]["inspector"] == heir.as_str()),
        "{heir} never inspected the deleted birth: the submit came from the durable facts"
    );
    let reconcile = lane_reconcile_span(&run, &birth_build, "mesh2", &attempt.to_string()).expect("the attempt's reconcile span");
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("node.create:{NODE}"), "only what was left: {reconcile}");
    let build = build_log::build_value(&estate, &birth_build).await;
    let steps = lane_steps(&build, "mesh2");
    let deleted: Vec<&Value> = steps.iter().filter(|r| r["operation"] == format!("node.delete:{NODE}").as_str() && r["step"] == "NodeDeleted").collect();
    assert_eq!(deleted.len(), 1, "one node.delete of the exited birth, never again: {deleted:?}");
    eprintln!(
        "s57 part 3: failed attempt {failed_attempt} by {mp} trace {} | role {heir} trace {} | idle at {} attempt {} | complete attempt {attempt} by {heir} trace {} | admitted trace {}",
        failed["trace_id"].as_str().unwrap_or(""),
        role["trace_id"].as_str().unwrap_or(""),
        s(&idle["state"]),
        idle["attempt"],
        done["trace_id"].as_str().unwrap_or(""),
        admitted["trace_id"].as_str().unwrap_or(""),
    );
    estate.record_trace_url(done["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 1), ("mesh2", 2, 2)], SETTLE).await;
    estate.shutdown().await;
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

/// The nodes whose path.name is a deleted `<path>.old`.
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
/// `mesh1.rpc.1` as ONE Build over the `node` object, and RDM runs its steps in order, each as its own
/// op's operation: node[1].connections.delete `{ All }`, node[1].mesh.leave, node[1].update
/// (`mesh1.rpc.1.old`), node[1].stop at `.old`, node.create(`mesh1.rpc.1`), node[1].delete at `.old`.
/// The stream is `Started`, every step's events in that order, `Complete`; the Build's operations are
/// exactly those six keys in that order, and no replace operation exists. The predecessor leaves its
/// mesh before its rename (the mesh-primary carries `LeaveMesh` and holds its `MeshLeft`), takes
/// `.old` live (the rename's `via-writer` span says `connections=deleted mesh=left`, applied by the
/// node, which publishes nothing off its mesh), and is stopped as `.old` after (its NodeStopped once,
/// its stop span naming `.old`). The successor is a re-birth at `mesh1.rpc.1`, the name the rename
/// released (a new node.id, transport key and data dir), its claim a successor's seeded from the
/// predecessor's row, and its join admitted under `mesh1.rpc.1`: it is never born at any other name.
/// The predecessor is deleted at `.old` on the provider's exited verdict (NodeDeleted once, its Deleted row
/// at `.old`, its data dir kept). RDM moves no data. At the end `mesh1.rpc.1` is the successor, ready,
/// and nothing is listed at `.old`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replace_runs_as_one_build_of_node_primitives_in_order() {
    let (mut estate, _birth) = estate("a_replace_runs_as_one_build_of_node_primitives_in_order").await;
    const NODE: &str = "mesh1.rpc.1";
    let path: PathName = NODE.parse().unwrap();
    let old = format!("{NODE}.old");
    let nodes = objects(&estate).await;
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let before = mesh.iter().find(|n| n.name == path).cloned().expect("node.get lists the node");
    let pid = estate.pid_of(NODE).await;
    let old_dir = std::path::PathBuf::from(estate.data_dir(NODE).await);
    let segment = old_dir.join("app-data").join("seg-0");
    std::fs::create_dir_all(segment.parent().unwrap()).unwrap();
    std::fs::write(&segment, b"records").unwrap();

    let stream = nodes.steps(replace_steps(&path)).await.expect("the Build was accepted");
    let accepted = stream.accepted().clone();
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
    let mut operations: Vec<String> = Vec::new();
    for r in steps.iter().filter(|r| r["attempt"].as_u64().unwrap_or(0) >= accepted.attempt as u64) {
        let op = s(&r["operation"]);
        if !operations.contains(&op) {
            operations.push(op);
        }
    }
    let keys = [
        format!("node.connection.delete:{NODE}"),
        format!("node.mesh.leave:{NODE}"),
        format!("node.update:{NODE}"),
        format!("node.stop:{old}"),
        format!("node.create:{NODE}"),
        format!("node.delete:{old}"),
    ];
    assert_eq!(operations, keys.to_vec(), "the Build's operations, in order: {b:#}");

    // The end state: the successor holds the name, ready; nothing at `.old`.
    let after = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
    let successor = after.iter().find(|n| n.name == path).cloned().expect("node.get lists the successor at the name");
    assert!(successor.status == NodeStatus::Ready && !successor.stopped, "{successor:?}");
    assert_ne!(successor.node_id, before.node_id, "a new node.id");
    assert_ne!(successor.endpoint_id, before.endpoint_id, "a new transport key");
    assert!(!after.iter().any(|n| n.name == path.renamed()), "nothing is listed at .old: {after:#?}");
    assert!(!alive(pid), "the predecessor's process {pid} exited at its delete");
    assert_eq!(std::fs::read(&segment).unwrap(), b"records", "node.delete keeps storage, and RDM moved no data: {segment:?}");
    let new_dir = std::path::PathBuf::from(estate.data_dir(NODE).await);
    assert_ne!(new_dir, old_dir, "the successor has its own data dir");
    assert!(!new_dir.join("app-data").exists(), "RDM handed nothing to the successor: {new_dir:?}");

    estate.stop().await;
    let spans = estate.spans();
    let of = |name: &str, id: &rdm_core::NodeId| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == id.to_string()).cloned().collect() };
    // The predecessor: its connections deleted and its mesh left, then renamed live, then stopped.
    let left = named(&spans, "rdm.node_admin.node.remove.via-mesh-leave").into_iter().filter(|sp| s(&sp["attributes"]["subject_id"]) == before.node_id.to_string()).cloned().collect::<Vec<_>>();
    assert_eq!(left.len(), 1, "the mesh-primary carried the predecessor's leave once: {left:#?}");
    assert_eq!((s(&left[0]["attributes"]["subject"]), s(&left[0]["attributes"]["outcome"])), (NODE.to_string(), "mesh-left".to_string()), "{:#}", left[0]);
    let renames1 = of("rdm.node_admin.name.update.via-writer", &before.node_id);
    assert_eq!(renames1.len(), 1, "{renames1:#?}");
    let r1 = &renames1[0];
    assert_eq!(
        (s(&r1["attributes"]["from"]), s(&r1["attributes"]["to"]), s(&r1["attributes"]["connections"]), s(&r1["attributes"]["mesh"]), s(&r1["attributes"]["answered"]), s(&r1["attributes"]["outcome"])),
        (NODE.to_string(), old.clone(), "deleted".into(), "left".into(), "applied".into(), "name-updated".into()),
        "{r1:#}"
    );
    assert!(end(&left[0]) <= start(r1), "the predecessor leaves its mesh before it gives up its name");
    let applied: Vec<Value> = named(&spans, "rdm.node_admin.node.update.via-apply-node-name").into_iter().filter(|sp| s(&sp["attributes"]["to"]) == old).cloned().collect();
    assert_eq!(applied.iter().map(|sp| (s(&sp["attributes"]["outcome"]), s(&sp["attributes"]["digest"]))).collect::<Vec<_>>(), vec![("applied".to_string(), "off-mesh".to_string())], "the predecessor takes .old off its mesh and publishes nothing: {applied:#?}");
    let stopped1 = of("rdm.node_admin.node.update.via-node-stopped", &before.node_id);
    assert_eq!(stopped1.len(), 1, "the predecessor's NodeStopped once: {stopped1:#?}");
    assert!(end(r1) <= start(&stopped1[0]), "the predecessor gives up its name before it is stopped");
    let stop_node: Vec<String> = named(&spans, "rdm.node_admin.status.update.via-stop-node").into_iter().filter(|sp| sp["attributes"]["operation"] == format!("node.stop:{old}").as_str()).map(|sp| s(&sp["attributes"]["node"])).collect();
    assert_eq!(stop_node, vec![old.clone()], "the predecessor stops under the name it took");
    // The successor: a re-birth at the name, its claim seeded from the predecessor's row, admitted there.
    let claim = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == successor.node_id.to_string() && sp["attributes"]["moment"] == "claim").cloned().expect("the successor's claim");
    assert_eq!((s(&claim["attributes"]["subject"]), s(&claim["attributes"]["seeded_from"])), (NODE.to_string(), before.node_id.to_string()), "the successor claims the released name, seeded from the predecessor's row: {claim:#}");
    assert!(end(&stopped1[0]) <= start(&claim), "the successor is created once the predecessor is stopped as .old");
    let join = of("rdm.node_admin.node.update.via-join", &successor.node_id).into_iter().filter(|sp| s(&sp["attributes"]["outcome"]) == "installed").collect::<Vec<_>>();
    assert!(join.iter().all(|j| s(&j["attributes"]["admitted_as"]) == NODE) && !join.is_empty(), "the successor's join is admitted under {NODE} and no other name: {join:#?}");
    assert!(of("rdm.node_admin.name.update.via-writer", &successor.node_id).is_empty(), "the successor is never renamed");
    // The predecessor's delete at `.old`: the provider's exited verdict, NodeDeleted once, the Deleted row.
    let deleted = of("rdm.node_admin.node.delete.via-node-deleted", &before.node_id);
    assert_eq!(deleted.len(), 1, "NodeDeleted once for the predecessor: {deleted:#?}");
    assert_eq!(s(&deleted[0]["attributes"]["operation"]), format!("node.delete:{old}"));
    let rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| s(&sp["attributes"]["subject"]) == old).collect();
    assert!(rows.len() == 1 && s(&rows[0]["attributes"]["outcome"]) == "row-deleted" && s(&rows[0]["attributes"]["node_id"]) == before.node_id.to_string(), "one Deleted row at .old for the predecessor: {rows:#?}");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").is_empty(), "nothing of the Build is drift");
    let create = named(&spans, "rdm.node_admin.node.create.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["node"]) == NODE).cloned().expect("the successor's create operation span");
    estate.artifact("steps.json", &json!({"operations": keys, "leave": left[0], "rename_old": r1, "stopped_old": stopped1[0], "claim": claim, "node_deleted": deleted[0], "deleted_row": rows[0]}));
    estate.record_trace_url(create["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (build.md §4.4 gate 4: "A node stands at the path | the fabric-primary's view by name ... |
/// `unknown-node`"; Q223: one gate, one refusal, whatever the Build): steps whose first step stops the
/// live `mesh1.rpc.1` and whose second starts `mesh1.rpc.9`, a path no birth holds and no earlier step
/// makes, are refused at acceptance `unknown-node`, the detail naming step 2 and `mesh1.rpc.9`, under
/// the fabric-primary's `rdm.node_admin.build.reject.via-unknown-node` span naming both. Nothing
/// executes: the accepted Build opens no attempt and records no receipt, and `mesh1.rpc.1` is still
/// the same birth in the same process, ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn steps_naming_an_unknown_node_are_refused_unknown_node_naming_the_step_and_execute_nothing() {
    let (mut estate, birth) = estate("steps_naming_an_unknown_node_are_refused_unknown_node_naming_the_step_and_execute_nothing").await;
    const NODE: &str = "mesh1.rpc.1";
    const UNKNOWN: &str = "mesh1.rpc.9";
    let path: PathName = NODE.parse().unwrap();
    let nodes = objects(&estate).await;
    let before = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().find(|n| n.name == path).expect("node.get lists the node");
    let pid = estate.pid_of(NODE).await;
    let b0 = build_log::build_value(&estate, &birth).await;

    let steps = vec![Step::Stop { node: path.clone() }, Step::Start { node: UNKNOWN.parse().unwrap() }];
    let (reason, detail) = match nodes.steps(steps).await {
        Err(CallEnd::Rejected { reason, detail }) => (reason, detail),
        Err(other) => panic!("steps naming an unknown node end Rejected: {other:?}"),
        Ok(_) => panic!("steps naming an unknown node were accepted"),
    };
    assert_eq!(reason, "unknown-node", "the one name build.md §4.4 gate 4 gives: {detail}");
    assert!(detail.contains("step 2 ") && detail.contains(&format!("`{UNKNOWN}`")), "the detail names the step and the target: {detail}");

    let b1 = build_log::build_value(&estate, &birth).await;
    assert_eq!((&b1["attempt"], b1["steps"].as_array().map(Vec::len)), (&b0["attempt"], b0["steps"].as_array().map(Vec::len)), "the refusal opened no attempt and recorded no receipt: {b0:#} / {b1:#}");
    let after = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().find(|n| n.name == path).expect("node.get lists the node");
    assert!(after.status == NodeStatus::Ready && !after.stopped, "step 1 never ran: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation_id), (&before.node_id, &before.incarnation_id), "the same birth");
    assert_eq!(estate.pid_of(NODE).await, pid, "the same process");

    estate.stop().await;
    let spans = estate.spans();
    let refused: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-unknown-node").into_iter().filter(|sp| s(&sp["attributes"]["detail"]).contains("step 2 ") && s(&sp["attributes"]["detail"]).contains(&format!("`{UNKNOWN}`"))).collect();
    assert_eq!(refused.len(), 1, "the fabric-primary's refusal span, its detail naming the step and the target: {refused:#?}");
    assert!(named(&spans, "rdm.node_admin.status.update.via-stop-node").into_iter().all(|sp| sp["attributes"]["operation"] != format!("node.stop:{NODE}").as_str()), "no stop of {NODE} ran");
    estate.artifact("refused.json", &json!({"reason": reason, "detail": detail, "span": refused[0]}));
}

/// CONTRACT (node-replace.md step 3: "Its row inherits config and tags from the predecessor row (the
/// retained `<name>.old` row ...), whose labels leave the active tags index"; Inherited: "The
/// successor's tag-index entries use its own id. Retained predecessor labels are removed from the
/// active index, not erased from its row"): mesh2's primary tells `mesh2.rpc.1` a config and a tag,
/// then the node is replaced up to the successor's create (no delete of the predecessor). The
/// predecessor leaves its mesh, takes `<name>.old` live and is stopped there; the successor's claim at
/// `<name>`, the name it released, puts its row with the config and the tag of the retained `.old` row
/// (`seeded_from` the predecessor), then marks the retained row `inherited_by` the successor inside the
/// same claim (its labels leave the tags index; the row keeps them). The successor's join under
/// `<name>` hands it that config; node.get by the tag names the successor alone, while the predecessor
/// stays retained at `<name>.old`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_nodes_successor_takes_its_config_and_tags_at_its_create_and_the_retained_row_leaves_the_tags_index() {
    let (estate, _build) = two_mesh_estate("a_replaced_nodes_successor_takes_its_config_and_tags_at_its_create_and_the_retained_row_leaves_the_tags_index").await;
    let nodes = estate.node_values().await;
    let (node, mesh_primary, fp) = placement(&nodes);
    let node_path = node.as_str();
    let old_name = format!("{node_path}.old");
    let before = estate.node_value(node_path).await;
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let node_id: rdm_core::NodeId = s(&before["node_id"]).parse().unwrap();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node_id.clone()), node_id, incarnation: rdm_core::IncarnationId::parse(&s(&before["incarnation_id"])).unwrap() };
    // node-config-update.md step 1: the caller sends it to the fabric-primary, which sends it down.
    let (fp_client, fp_id) = caller_of(&fp).await;
    let reply = rdm_client::NodeRpc::new(&fp_client)
        .config_update(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, told.clone(), &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.config.update of {node_path} at the fabric-primary: {e:?}"));
    assert_eq!(reply.name(), "config-updated", "the mesh-primary wrote the row and the node applied it: {reply:?}");
    let told_json = json!({"tier": "gold"});
    wait_for(&format!("{node_path} carries its config"), Duration::from_secs(30), || async { (estate.node_value(node_path).await["config"] == told_json).then_some(()) }).await;
    let labels: std::collections::BTreeSet<String> = ["rabbit".to_string()].into();
    // Commands go down: the caller sends it to the fabric-primary, which sends it to the mesh-primary (Q203).
    let tagged = rdm_client::NodeRpc::new(&fp_client)
        .tags_create(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, &labels, &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.tag.create of {node_path} at the fabric-primary: {e:?}"));
    assert_eq!(tagged.name(), "tags-created", "the mesh-primary wrote the label's row: {tagged:?}");
    wait_for(&format!("{node_path} carries its tag"), Duration::from_secs(30), || async { (estate.node_view(node_path).await.tags == labels).then_some(()) }).await;

    let create_steps: Vec<Step> = replace_steps(&Estate::path(node_path)).into_iter().take(5).collect();
    let got = frames(estate.nodes_obj().await.steps(create_steps).await.expect("the Build up to the successor's create was accepted")).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    let after = until_node_where(&estate, node_path, &format!("{node_path} ready under a new NodeId"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    let spans = wait_for("the successor's claim, the retained row's mark and the successor's join under the name are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let joined = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().any(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["admitted_as"] == node_path);
        let marked = named(&spans, "rdm.node_admin.node.update.via-inherited-row").into_iter().any(|sp| sp["attributes"]["node_id"] == before["node_id"]);
        (joined && marked).then_some(spans)
    })
    .await;
    // Step 3: the successor's claim at the released name inherits from the retained row, then marks it.
    let claim = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["moment"] == "claim").expect("the successor's claim").clone();
    let a = &claim["attributes"];
    assert_eq!(
        (s(&a["node"]), s(&a["subject"]), s(&a["outcome"]), s(&a["seeded_from"]), s(&a["keys"]), s(&a["tags"])),
        (mesh_primary.clone(), node_path.to_string(), "row-seeded".to_string(), s(&before["node_id"]), "1".to_string(), "rabbit".to_string()),
        "the mesh-primary puts the successor at {node_path} with the retained row's config and tag: {claim}"
    );
    assert!(named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().all(|sp| sp["attributes"]["node_id"] != after["node_id"]), "the successor is never renamed");
    let mark = named(&spans, "rdm.node_admin.node.update.via-inherited-row").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).expect("the retained row's mark").clone();
    let m = &mark["attributes"];
    assert_eq!((s(&m["subject"]), s(&m["inherited_by"]), s(&m["tags"]), s(&m["outcome"])), (old_name.clone(), s(&after["node_id"]), "rabbit".to_string(), "inherited".to_string()), "the retained row says who inherited it and keeps its tag: {mark}");
    assert_eq!(mark["parent_span_id"], claim["span_id"], "the mark is the claim's last write: {mark} under {claim}");
    // The successor's join under the name hands it the row's config (node-start.md step 2).
    let joined = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["admitted_as"] == node_path).expect("the successor's join under the name").clone();
    assert_eq!((s(&joined["attributes"]["outcome"]), s(&joined["attributes"]["config_keys"])), ("installed".to_string(), "1".to_string()), "the successor's join answer under {node_path} hands its row's config: {joined}");
    let carried = wait_for(&format!("the successor at {node_path} carries the config"), Duration::from_secs(30), || async { let n = estate.node_value(node_path).await; (n["config"] == told_json).then_some(n) }).await;
    assert_eq!(carried["node_id"], after["node_id"], "the successor carries it");
    assert_eq!(estate.node_view(node_path).await.tags, labels, "the successor's row carries the predecessor's tag");
    let selected: Vec<String> = estate
        .nodes_obj()
        .await
        .get(&rdm_client::Filter::new().eq("tags", labels.iter().cloned()))
        .await
        .unwrap_or_else(|e| panic!("node.get [tags: rabbit]: {e:?}"))
        .into_iter()
        .map(|n| n.node_id.to_string())
        .collect();
    assert_eq!(selected, vec![s(&after["node_id"])], "the tags index names the successor under the label, and not the retained predecessor");
    let retained = estate.node_values().await.into_iter().find(|n| n["name"] == old_name.as_str()).unwrap_or_else(|| panic!("the predecessor is retained at {old_name}"));
    assert_eq!(retained["node_id"], before["node_id"], "the retained {old_name} is the predecessor: {retained}");
    eprintln!("trace {}: via-node-row node={} subject={} seeded_from={} keys={} tags={}; via-inherited-row subject={} inherited_by={}; trace {}: via-join admitted_as={} config_keys={}", s(&claim["trace_id"]), mesh_primary, node_path, s(&a["seeded_from"]), s(&a["keys"]), s(&a["tags"]), old_name, s(&m["inherited_by"]), s(&joined["trace_id"]), node_path, s(&joined["attributes"]["config_keys"]));
    estate.shutdown().await;
}

/// CONTRACT (lifecycles/node-replace.md "Rollback": "While node[1] is stopped as `<name>.old` and its
/// data remains available: 1. node[2]: node.connection.delete `{ scope: All }`, node.mesh.leave, then
/// node[2].delete releases `<name>` after exact runtime exited verdict. 2. node[1].start rejoins the same
/// stopped process with AfterStop under `<name>.old` ... 3. node[1]: node.connection.delete `{ scope:
/// All }`, node.mesh.leave, then node.name.update to `<name>` ..., then node[1].stop and node[1].start
/// under `<name>`. Its ready ends the rollback"): `mesh1.rpc.1` is replaced up to the
/// successor's create (the predecessor stopped at `.old`), then the rollback runs as one Build of nine
/// steps, each its own op's operation in order. The successor at `mesh1.rpc.1` has its connections
/// deleted, leaves its mesh and is deleted on its exited verdict; the predecessor starts as
/// `mesh1.rpc.1.old` (the same process), has its connections deleted, leaves its mesh, takes
/// `mesh1.rpc.1` live, and is stopped and started under it. At the end `mesh1.rpc.1` is the predecessor,
/// the same node.id, incarnation and process, ready; nothing is listed at `.old`, and the
/// successor's process exited. Both of the predecessor's renames are of a node off its mesh
/// (`connections=deleted mesh=left`, applied), and no rename of a stopped node is decided.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replace_rollback_ends_with_the_predecessor_ready_under_its_name() {
    let (mut estate, _birth) = estate("a_replace_rollback_ends_with_the_predecessor_ready_under_its_name").await;
    const NODE: &str = "mesh1.rpc.1";
    let path: PathName = NODE.parse().unwrap();
    let old = format!("{NODE}.old");
    let nodes = objects(&estate).await;
    let before = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().find(|n| n.name == path).expect("node.get lists the node");
    let pid = estate.pid_of(NODE).await;

    let forward: Vec<Step> = replace_steps(&path).into_iter().take(5).collect();
    let got = frames(nodes.steps(forward).await.expect("the replace up to the successor's create was accepted")).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    let successor = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").into_iter().find(|n| n.name == path).expect("the successor at the name");
    assert_ne!(successor.node_id, before.node_id, "the successor is a re-birth");
    let successor_pid = estate.pid_of(NODE).await;
    assert_ne!(successor_pid, pid, "the successor runs its own process");

    let stream = nodes.steps(rollback_steps(&path)).await.expect("the rollback steps were accepted");
    let accepted = stream.accepted().clone();
    let got = frames(stream).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    let build = accepted.build_id.to_string();
    let b = build_log::build_value(&estate, &build).await;
    let mut operations: Vec<String> = Vec::new();
    for r in b["steps"].as_array().cloned().unwrap_or_default().iter().filter(|r| r["attempt"].as_u64().unwrap_or(0) >= accepted.attempt as u64) {
        let op = s(&r["operation"]);
        if !operations.contains(&op) {
            operations.push(op);
        }
    }
    let keys = [
        format!("node.connection.delete:{NODE}"),
        format!("node.mesh.leave:{NODE}"),
        format!("node.delete:{NODE}"),
        format!("node.start:{old}"),
        format!("node.connection.delete:{old}"),
        format!("node.mesh.leave:{old}"),
        format!("node.update:{old}"),
        format!("node.stop:{NODE}"),
        format!("node.start:{NODE}"),
    ];
    assert_eq!(operations, keys.to_vec(), "the rollback's operations, in order: {b:#}");

    let after = until_node_where(&estate, NODE, &format!("{NODE} ready as the predecessor again"), SETTLE, |n| ready(n) && n["node_id"] == before.node_id.to_string()).await;
    assert_eq!(s(&after["incarnation_id"]), before.incarnation_id.as_ref().unwrap().to_string(), "the same birth: {after}");
    assert_eq!(estate.pid_of(NODE).await, pid, "the same process");
    assert!(estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]").iter().all(|n| n.name != path.renamed()), "nothing is listed at .old");
    assert!(!alive(successor_pid), "the successor's process {successor_pid} exited at its delete");

    estate.stop().await;
    let spans = estate.spans();
    let renames: Vec<Value> = named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string()).cloned().collect();
    assert_eq!(
        renames.iter().map(|sp| (s(&sp["attributes"]["to"]), s(&sp["attributes"]["connections"]), s(&sp["attributes"]["mesh"]), s(&sp["attributes"]["outcome"]))).collect::<Vec<_>>(),
        vec![(old.clone(), "deleted".to_string(), "left".to_string(), "name-updated".to_string()), (NODE.to_string(), "deleted".to_string(), "left".to_string(), "name-updated".to_string())],
        "the predecessor is renamed twice, each time live and off its mesh: {renames:#?}"
    );
    assert!(named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().all(|sp| s(&sp["attributes"]["traffic"]) != "Stopped"), "no rename of a stopped node is decided");
    let join = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.to_string() && s(&sp["attributes"]["outcome"]) == "installed").map(|sp| s(&sp["attributes"]["admitted_as"])).collect::<Vec<_>>();
    assert_eq!(join, vec![NODE.to_string(), old.clone(), NODE.to_string()], "the predecessor joined at its birth, then as .old at its start, then as {NODE} at its last start: {join:?}");
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

/// CONTRACT: the Build's `node[1].delete` of the stopped predecessor at `<path>.old`
/// (lifecycles/node-replace.md step 2; node-delete.md) as its own call: `mesh1.rpc.1` has its
/// connections deleted, leaves its mesh, is renamed `mesh1.rpc.1.old` live by node.name.update at its
/// mesh-primary and is stopped there (the predecessor's half of the replace steps), then `build.create` of a
/// RemoveNode change at `<path>.old` is the next attempt of the accepted Build (reason delete,
/// action delete targeted to the exact stopped birth, no Build minted). One operation
/// `node.delete:<path>.old` (node.delete's one key) asks the provider to end the stopped runtime
/// and inspects it; only the provider's Exited is the proof, after which the executor publishes
/// NodeDeleted for the exact birth and the mesh's primary, which executes it, writes the Deleted
/// row at `<path>.old`, its record kept (R13). The process is gone; its data dir stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_the_stopped_predecessor_proves_its_exit_publishes_node_deleted_and_writes_its_deleted_row() {
    let (estate, birth_build) = estate("deleting_the_stopped_predecessor_proves_its_exit_publishes_node_deleted_and_writes_its_deleted_row").await;
    let nodes = objects(&estate).await;
    let stopped = leave_rename_and_stop(&estate, &nodes, "mesh1.rpc.1").await;
    delete_stopped_and_assert(&estate, &birth_build, &stopped).await;
    estate.shutdown().await;
}

/// CONTRACT: the same delete when the fabric-primary is in another mesh than the node (R13: the
/// mesh-primary of a node's mesh is the only writer of that mesh's rows). The fabric-primary in mesh1
/// admits `node.delete:<path>.old` of a mesh2 node from its view, which reads the stopped birth at the
/// row's name by its node id (R12), targeted to that exact birth; mesh2's primary executes the delete
/// and writes the Deleted row at `<path>.old`; the fabric-primary writes no row of mesh2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_peer_mesh_stopped_predecessor_is_admitted_from_the_fabric_primary_held_record() {
    let (estate, birth_build) = two_mesh_estate("deleting_a_peer_mesh_stopped_predecessor_is_admitted_from_the_fabric_primary_held_record").await;
    let nodes = objects(&estate).await;
    let stopped = leave_rename_and_stop(&estate, &nodes, "mesh2.rpc.1").await;
    assert!(stopped.renamed_by.starts_with("mesh2.admin."), "mesh2's primary renamed it: {}", stopped.renamed_by);
    assert!(stopped.fabric_primary.starts_with("mesh1.admin."), "the fabric-primary is in mesh1: {}", stopped.fabric_primary);
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
/// `<path>.old`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_stopped_predecessor_after_its_mesh_primary_changed_is_executed_by_the_new_primary_on_the_exact_birth() {
    let (estate, birth_build) = fabric_of("deleting_a_stopped_predecessor_after_its_mesh_primary_changed_is_executed_by_the_new_primary_on_the_exact_birth", &[("mesh1", 1, 2), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let nodes = objects(&estate).await;
    let stopped_birth = leave_rename_and_stop(&estate, &nodes, "mesh2.rpc.1").await;
    let first_primary = stopped_birth.renamed_by.clone();
    assert!(first_primary.starts_with("mesh2.admin."), "mesh2's primary renamed it: {first_primary}");
    // mesh2's primary changes: its stop hands its roles to mesh2's other admin, which executes that
    // stop (executor_for: the mesh primary never executes its own).
    let stopped = frames(nodes.stop(&first_primary.parse().unwrap()).await.expect("the stop of mesh2's primary was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let new_primary = wait_for("mesh2's other admin holds the primary role", Duration::from_secs(30), || async {
        estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.ok()?.into_iter().find(|n| n.kind == rdm_core::NodeKind::NodeAdmin && n.is_primary && n.name.to_string() != first_primary).map(|n| n.name.to_string())
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
/// predecessor's half of the replace of `mesh2.rpc.1` (stopped at `.old`); mesh2 then takes a second
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
    let stopped_birth = leave_rename_and_stop(&estate, &nodes, "mesh2.rpc.1").await;
    assert_eq!(stopped_birth.renamed_by, "mesh2.admin.1", "mesh2's only admin renamed and stopped it");
    // mesh2's second admin is born after the stop: no digest of the stopped birth reaches its view.
    let grown = estate.reconcile_shape("fabric1", &GROWN).await;
    // The view holds the stopped `.old` beside the grown node types, so it is read by name.
    wait_for("mesh2.admin.2 is ready", Duration::from_secs(90), || async {
        estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.ok()?.into_iter().find(|n| n.name.to_string() == "mesh2.admin.2" && n.status == NodeStatus::Ready).map(|_| ())
    })
    .await;
    let nodes = objects(&estate).await;
    let stopped = frames(nodes.stop(&"mesh2.admin.1".parse().unwrap()).await.expect("the stop of mesh2's first admin was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    wait_for("mesh2.admin.2 holds mesh2's role", Duration::from_secs(30), || async {
        estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh2"])).await.ok()?.into_iter().find(|n| n.name.to_string() == "mesh2.admin.2" && n.is_primary).map(|_| ())
    })
    .await;
    let record = match rdm_core::RuntimeFact::read_record(&stopped_birth.old_dir) {
        Some(Ok(f)) => f,
        other => panic!("the stopped birth's data dir {:?} holds its runtime record: {other:?}", stopped_birth.old_dir),
    };
    let executor = delete_stopped_and_assert(&estate, &grown.build_id.to_string(), &stopped_birth).await;
    assert_eq!(executor, "mesh2.admin.2", "mesh2's new primary, born after the stop, executes the delete");

    let spans = estate.spans();
    let old_name = format!("{}.old", stopped_birth.node);
    let rebuilt: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-rebuilt-birth").into_iter().filter(|sp| sp["attributes"]["node"] == old_name.as_str()).collect();
    assert_eq!(rebuilt.len(), 1, "the executor rebuilt the birth its view does not hold, once: {rebuilt:#?}");
    let a = &rebuilt[0]["attributes"];
    assert_eq!(
        (s(&a["executor"]), s(&a["node_id"]), s(&a["incarnation_id"]), s(&a["outcome"])),
        ("mesh2.admin.2".to_string(), s(&stopped_birth.before["node_id"]), s(&stopped_birth.before["incarnation_id"]), "rebuilt".to_string()),
        "the exact birth the Build action names, rebuilt by the executor: {:#}",
        rebuilt[0]
    );
    assert_eq!(s(&a["data_dir"]), stopped_birth.old_dir.display().to_string(), "the birth's data dir under the data root: {:#}", rebuilt[0]);
    assert_eq!(s(&a["deployment_id"]), record.deployment_id, "the runtime its data-root record names: {:#}", rebuilt[0]);
    let op = named(&spans, "rdm.node_admin.node.delete.via-build").into_iter().find(|sp| sp["attributes"]["node"] == old_name.as_str()).cloned().expect("the delete operation span");
    assert!(descends_from(&spans, rebuilt[0], &op), "the rebuild runs inside the delete operation: {:#} / {op:#}", rebuilt[0]);
    estate.artifact("rebuilt-birth.json", &json!({"rebuilt": rebuilt[0], "record": record.deployment_id}));
    estate.shutdown().await;
}

/// The stopped birth a Build's rename left at `<path>.old`: who it was, where, who renamed it.
struct StoppedBirth {
    node: String,
    before: Value,
    pid: u64,
    old_dir: std::path::PathBuf,
    /// The mesh-primary that served the node.name.update (the writer of the node's row).
    renamed_by: String,
    /// The fabric-primary when the node was renamed.
    fabric_primary: String,
}

fn dispatch<'a>(spans: &'a [Value], build: &str, attempt: &str) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == build && s(&sp["attributes"]["attempt"]) == attempt)
}

/// The `node` objects over the estate's fabric-primary, every member's address learned.
async fn objects(estate: &Estate) -> Nodes {
    let _ = estate.try_node_views().await;
    estate.nodes_obj().await
}

/// `node` off its mesh, renamed `<node>.old` live and stopped there by the predecessor's half of a
/// replace steps (lifecycles/node-replace.md steps 1-2): the stopped birth a Build's `node[1].delete`
/// deletes.
async fn leave_rename_and_stop(estate: &Estate, nodes: &Nodes, node: &str) -> StoppedBirth {
    let path: PathName = node.parse().unwrap();
    let before = estate.node_value(node).await;
    let pid = estate.pid_of(node).await;
    let old_dir = std::path::PathBuf::from(estate.data_dir(node).await);
    // The fabric-primary as fabric.get names it, and the node's mesh-primary as mesh.get names it.
    let fabric_primary = rdm_test::typed::the_fabric(&estate.fabrics_obj().await).await.expect("fabric.get").fabric_primary.expect("a fabric-primary").to_string();
    let primary = rdm_test::typed::one_mesh(&estate.meshes_obj().await, &path.mesh).await.expect("mesh.get").expect("the admin holds the node's mesh").primary_admin.expect("the node's mesh-primary");
    let got = frames(nodes.steps(predecessor_steps(&path)).await.expect("the predecessor's half of the replace was accepted")).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", [&path.mesh])).await.expect("node.get [mesh]");
    let stopped = mesh.iter().find(|n| n.name == path.renamed() && n.stopped).cloned().unwrap_or_else(|| panic!("node.get lists the stopped {node} stopped at {} once its steps completed: {mesh:#?}", path.renamed()));
    assert_eq!(stopped.node_id.to_string(), s(&before["node_id"]), "the stopped birth at {} is {node}'s", path.renamed());
    assert!(alive(pid), "the stopped {node} ({pid}) is alive before its delete");
    StoppedBirth { node: node.to_string(), before, pid, old_dir, renamed_by: primary.to_string(), fabric_primary }
}

/// `build.create` of a RemoveNode change at `<path>.old` of `stopped`, and everything node.delete owes (node-delete.md).
/// Returns the admin that executed the delete.
async fn delete_stopped_and_assert(estate: &Estate, birth_build: &str, stopped: &StoppedBirth) -> String {
    let StoppedBirth { node, before, pid, old_dir, .. } = stopped;
    let old_name = format!("{node}.old");
    let delete = estate.submit_change(Change::RemoveNode { node: Estate::path(&old_name) }).await;
    assert_eq!(delete.build_id().as_str(), birth_build, "no Build is minted");
    let attempt_no = u64::from(delete.attempt());
    let opened = build_log::build_fact(&estate, birth_build, "the delete attempt is the Build's current attempt", Duration::from_secs(30), |p| serde_json::to_value(p).expect("a Build view serializes")["reason"] == "delete")
        .await
        .map(|p| serde_json::to_value(p).expect("a Build view serializes"))
        .unwrap_or_else(|end| panic!("{end}"));
    assert_eq!((s(&opened["action"]["action"]).as_str(), s(&opened["action"]["path"]).as_str()), ("delete", old_name.as_str()), "{opened}");
    assert_eq!(opened["action"]["from_incarnation"], before["incarnation_id"], "targeted to the exact stopped birth: {opened}");
    estate.follow(delete).await;
    let attempt = attempt_no.to_string();
    let spans = wait_for("the delete's last step, its reconcile span, its dispatch and the Deleted row are exported", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let row = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().any(|sp| sp["attributes"]["subject"] == old_name.as_str());
        (step_span(&spans, birth_build, &attempt, &old_name, "Complete").is_some() && reconcile_span(&spans, birth_build, &attempt).is_some() && dispatch(&spans, birth_build, &attempt).is_some() && row).then_some(spans)
    })
    .await;
    let reconcile = reconcile_span(&spans, birth_build, &attempt).unwrap();
    assert_eq!(s(&reconcile["attributes"]["operations"]), format!("node.delete:{old_name}"), "one delete operation of the stopped birth: {reconcile}");
    let mut last = 0u64;
    for step in DELETE_STOPPED_ORDER {
        let sp = step_span(&spans, birth_build, &attempt, &old_name, step).unwrap_or_else(|| panic!("step {step} of the delete ran"));
        assert!(start(sp) >= last, "step {step} starts before the step it follows: {sp}");
        last = start(sp);
    }
    for step in ["DeleteConnections", "StopNode", "ReleaseStorage"] {
        assert!(step_span(&spans, birth_build, &attempt, &old_name, step).is_none(), "the stopped birth's delete runs no {step}");
    }
    // The provider's Exited is the proof, and it precedes the NodeDeleted the executor publishes.
    let terminate = step_span(&spans, birth_build, &attempt, &old_name, "TerminateRuntime").unwrap();
    assert_eq!(s(&terminate["attributes"]["outcome"]), "complete", "{terminate}");
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(deleted.len(), 1, "NodeDeleted once for the stopped birth: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], before["incarnation_id"], "named by the exact birth");
    assert_eq!(s(&deleted[0]["attributes"]["operation"]), format!("node.delete:{old_name}"));
    assert!(end(terminate) <= start(deleted[0]), "NodeDeleted follows the exited verdict: {terminate} vs {}", deleted[0]);
    // The Deleted row at `<path>.old`, written by the mesh's primary that executes the delete (R13).
    let dispatched = dispatch(&spans, birth_build, &attempt).unwrap();
    let executor = s(&dispatched["attributes"]["executor"]);
    let rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == old_name.as_str()).collect();
    assert_eq!(rows.len(), 1, "one Deleted row at {old_name}: {rows:?}");
    assert_eq!(
        (s(&rows[0]["attributes"]["node"]), s(&rows[0]["attributes"]["node_id"]), s(&rows[0]["attributes"]["outcome"])),
        (executor.clone(), s(&before["node_id"]), "row-deleted".to_string()),
        "the mesh's primary executing the delete writes the Deleted row of the exact node: {}",
        rows[0]
    );
    estate.artifact("deleted-row.json", &json!({"row": rows[0], "node_deleted": deleted[0], "terminate": terminate, "dispatch": dispatched}));
    let op = named(&spans, "rdm.node_admin.node.delete.via-build").into_iter().find(|sp| s(&sp["attributes"]["build_id"]) == birth_build && s(&sp["attributes"]["attempt"]) == attempt && sp["attributes"]["node"] == old_name.as_str()).expect("the delete operation span");
    estate.record_trace_url(op["trace_id"].as_str().unwrap_or(""));
    // The stopped process is gone and its data dir stays.
    assert!(!alive(*pid), "the stopped birth's process {pid} exited");
    assert!(old_dir.exists(), "node.delete releases node.id and name, not storage: {old_dir:?}");
    executor
}

/// CONTRACT (lifecycles/node-replace.md, the same steps for a node-admin): the replace steps of a
/// node-admin that holds no role runs as one Build of node primitives: the predecessor has its
/// connections deleted, leaves its mesh and the backbone, is renamed `<path>.old` live and then stopped
/// (its NodeStopped drops it from every view), the successor is a re-birth at the path, and the
/// predecessor is deleted. The successor admin's own view holds itself at the path, ready, under its
/// new NodeId, and no admin's view holds two live births at the path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_node_admin_sees_itself_and_no_view_holds_two_live_births() {
    let (mut estate, _birth) = estate("a_replaced_node_admin_sees_itself_and_no_view_holds_two_live_births").await;
    let nodes = estate.node_values().await;
    let admins: Vec<&Value> = nodes.iter().filter(|n| n["kind"] == "node_admin").collect();
    assert_eq!(admins.len(), 2, "{nodes:#?}");
    let before = admins.iter().find(|n| n["is_primary"] != true && n["is_fabric_primary"] != true).map(|n| (*n).clone()).expect("an admin that holds no primary role");
    let stays = Estate::path(&s(&admins.iter().find(|n| n["node_id"] != before["node_id"]).unwrap()["name"]));
    assert_eq!(estate.entry().await, stays, "the admin that stays is the entry the typed objects ask");
    let node = s(&before["name"]);
    let node_path = Estate::path(&node);
    let nodes_obj = objects(&estate).await;
    let got = frames(nodes_obj.steps(replace_steps(&node_path)).await.expect("the Build was accepted")).await;
    assert_eq!(got.last(), Some(&Frame::Complete), "{got:?}");
    at_most_one_live_birth(&estate, &[stays.clone()], &node).await;
    let after = until_new_birth(&estate, &node, &before, SETTLE).await;
    // The successor admin's own view: itself at the path, ready, under its new NodeId.
    let me = estate
        .until_view_at(&node_path, &format!("the successor {node} sees itself"), Duration::from_secs(30), |views| {
            let at: Vec<Value> = Estate::values(views).into_iter().filter(|n| n["name"] == node.as_str() && live(n)).collect();
            assert!(at.len() <= 1, "the successor's view holds two live births at {node}: {at:#?}");
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
/// birth at the same `path.name`. Must NOT happen: the hung member held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_member_whose_runtime_runs_is_terminated_and_recovered_at_once() {
    let (mut estate, birth_build) = born_at(
        Estate::bootstrap_with_env(owner("a_hung_member_whose_runtime_runs_is_terminated_and_recovered_at_once"), "fabric1", "mesh1", &[]).await,
        &[("mesh1", 1, 2), ("mesh2", 1, 2)],
        Duration::from_secs(90),
    )
    .await;
    let (node, mesh_primary, _) = placement(&estate.node_values().await);
    let before = estate.node_value(&node).await;
    let mesh = s(&before["mesh"]);
    let rt = exact_runtime(&estate, &node).await;
    let held = rt.hang().expect("the exact runtime is hung");
    assert!(!held.held.is_empty(), "every task but the leader is held: the provider sees it running");

    let after = until_node_where(&estate, &node, &format!("{node} recovered as a new birth"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    let recovery = recovery_span(&estate, &node, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "the member's mesh-primary submits the recovery: {recovery}");
    assert_eq!(s(&recovery["attributes"]["verdict"]), "running", "on the investigation's verdict: {recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    assert_eq!(s(&recovery["attributes"]["outcome"]), format!("complete: attempt {attempt}"), "{recovery}");
    let (deleted, created) = recovery_receipts_held(&estate, &birth_build, &mesh, &node, &attempt).await;
    let b = build_log::build_value(&estate, &birth_build).await;
    let steps = lane_steps(&b, &mesh);
    let terminate = steps
        .iter()
        .find(|r| r["attempt"] == deleted["attempt"] && r["operation"] == format!("node.delete:{node}") && r["step"] == "TerminateRuntime" && r["outcome"] == "complete")
        .cloned()
        .unwrap_or_else(|| panic!("expected TerminateRuntime receipt in {mesh} lane steps: {steps:?}"));
    assert_eq!(terminate["output"]["verdict"], "running", "the TerminateRuntime read the runtime running at its instant and terminated it: {terminate}");
    eprintln!(
        "member cell: {node} (birth {}) recovered by {mesh_primary} at attempt {attempt} of {mesh} lane of {birth_build}: trace {} | TerminateRuntime receipt {:?} | new birth {}",
        s(&before["node_id"]),
        s(&recovery["trace_id"]),
        terminate["output"],
        s(&after["node_id"]),
    );
    let _ = created;
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.stop().await;
}

/// CONTRACT (node-recovery.md: "The mesh-primary is the reconciler for its own members, whatever their
/// type"; step 2: "Half a floor after the mark it runs node investigation on the exact birth"; step 3:
/// "Any other verdict (`running`, ...): the recovery below, at once. Nothing holds, nothing is inspected
/// again"; node-investigation.md Caller: "its mesh-primary for a member of any type"; Q181, Luke
/// 2026-10-26: "a node cut off should have been replaced"): mesh1 holds two node-admins, mesh1.admin.1
/// its primary and the fabric-primary, and mesh1.admin.2 a member. mesh1.admin.2 is hung (every task
/// but the leader held by a tracer: the provider reads it running). Its mesh-primary mesh1.admin.1 runs
/// the one investigation on it, half a floor after the mark, and on the verdict `running` submits its
/// recovery at once: mesh1.admin.2 is re-created as a new birth at its path.name. Must NOT happen: the
/// mesh-primary passing over its own node-admin member, a hold-down opened on it, or a second ladder
/// before the recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_node_admin_member_is_investigated_by_its_mesh_primary_and_re_created_at_once() {
    let (mut estate, _birth_build) = fabric_of("a_hung_node_admin_member_is_investigated_by_its_mesh_primary_and_re_created_at_once", &[("mesh1", 2, 1)], Duration::from_secs(60)).await;
    let node = "mesh1.admin.2";
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary").clone();
    let mesh_primary = s(&primary["name"]);
    assert_eq!(mesh_primary, "mesh1.admin.1", "the bootstrap admin is mesh1's primary and the fabric-primary");
    let before = estate.node_value(node).await;
    let rt = exact_runtime(&estate, node).await;
    let held = rt.hang().expect("the exact runtime is hung");
    assert!(!held.held.is_empty(), "every task but the leader is held: the provider sees it running");

    let after = until_new_birth(&estate, node, &before, SETTLE).await;
    let recovery = recovery_span(&estate, node, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "the node-admin's own mesh-primary submits its recovery: {recovery}");
    assert_eq!(s(&recovery["attributes"]["verdict"]), "running", "on the investigation's verdict: {recovery}");
    let spans = estate.spans();
    let held_down: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-offline-hold-down-opened").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert!(held_down.is_empty(), "nothing holds between the ladder nobody answered and the recovery: {held_down:?}");
    let ladders: Vec<&Value> = named(&spans, "rdm.node.tickle.returns-unknown.via-unreached").into_iter().filter(|sp| sp["attributes"]["caller"] == mesh_primary.as_str() && sp["attributes"]["node_id"] == before["node_id"]).collect();
    assert_eq!(ladders.len(), 1, "the inspection, then one ladder (Q273): {ladders:?}");
    assert_eq!(s(&ladders[0]["attributes"]["next"]), "verdict", "the inspection read it running, so the ladder nobody answered ends in the verdict (node-investigation.md step 4, Q273): {}", ladders[0]);
    eprintln!("node-admin member cell: {node} (birth {}) recovered by {mesh_primary}: trace {} | new birth {}", s(&before["node_id"]), s(&recovery["trace_id"]), s(&after["node_id"]));
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 2, 1)], Duration::from_secs(30)).await;
    estate.stop().await;
}

/// CONTRACT (node-recovery.md step 2: "For an unheard mesh-primary the fabric-primary runs it", step 3:
/// any verdict but `reached`, the recovery at once, and sequence step 1: "for an unheard mesh-primary,
/// the fabric-primary opens it"; sequence step 3: `TerminateRuntime` first when the verdict was
/// `running`): `mesh2` has two node-admins (`mesh2.admin.1` primary, `mesh2.admin.2` standby).
/// `mesh2.admin.1` is hung (every task but the leader held by a tracer: the provider reads it running).
/// The fabric-primary's investigation ends in `running` and it submits the recovery at once, in the
/// fabric lane: its node.delete terminates the exact runtime first; the deleted birth is gone from
/// every role, the election names `mesh2.admin.2`, and `mesh2.admin.1` is re-created as a new birth,
/// created by `mesh2.admin.2` (commands go down: the fabric-primary claims the create on its own log and
/// the in its role mesh-primary executes it, mesh-investigation.md step 6). Must NOT happen: the hung
/// mesh-primary held, the fabric-primary creating mesh2's new birth itself, or a second recovery of the
/// hung birth in mesh2's lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_mesh_primary_is_terminated_and_recovered_at_once_by_the_fabric_primary() {
    let (mut estate, birth_build) = born_at(
        Estate::bootstrap_with_env(owner("a_hung_mesh_primary_is_terminated_and_recovered_at_once_by_the_fabric_primary"), "fabric1", "mesh1", &[]).await,
        &[("mesh1", 1, 1), ("mesh2", 2, 1)],
        Duration::from_secs(90),
    )
    .await;
    let nodes = estate.node_values().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    let mp = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().expect("mesh2's primary");
    let mp_name = s(&mp["name"]);
    let standby = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != mp["name"]).expect("mesh2's standby admin")["name"]);
    assert!(!fp.starts_with("mesh2."), "the fabric-primary is in mesh1: {fp}");

    let rt = exact_runtime(&estate, &mp_name).await;
    let held = rt.hang().expect("mesh2's primary runtime is hung");
    assert!(!held.held.is_empty(), "every task of mesh2's primary but the leader is held: the provider sees it running");

    let after = until_node_where(&estate, &mp_name, &format!("{mp_name} recovered as a new birth"), SETTLE, |n| ready(n) && n["node_id"] != mp["node_id"]).await;
    let standby_after = until_node_where(&estate, &standby, &format!("{standby} primary as mesh2's primary once the hung birth is deleted"), Duration::from_secs(60), |n| n["is_primary"] == json!(true)).await;
    assert_eq!(standby_after["is_primary"], json!(true), "{standby} holds mesh2's role after the delete");

    let recovery = recovery_span(&estate, &mp_name, &mp["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), fp, "the fabric-primary submits a hung mesh-primary's recovery: {recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    let b = build_log::build_value(&estate, &birth_build).await;
    // The fabric-primary runs an unheard mesh-primary's recovery in the fabric lane: mesh2's own lane is
    // written by its primary, the hung birth.
    let steps = b["steps"].as_array().cloned().unwrap_or_default();
    let terminate = steps
        .iter()
        .find(|r| r["operation"] == format!("node.delete:{mp_name}") && r["step"] == "TerminateRuntime" && r["outcome"] == "complete")
        .cloned()
        .unwrap_or_else(|| panic!("expected TerminateRuntime receipt in the fabric lane's steps: {steps:?}"));
    assert_eq!(terminate["output"]["verdict"], "running", "the TerminateRuntime read the hung runtime running and terminated it: {terminate}");
    let spans = estate.spans();
    let role_span = named(&spans, "rdm.mesh.election.resolve.via-mesh-primary")
        .into_iter()
        .find(|sp| sp["attributes"]["winner_path"] == standby.as_str())
        .cloned()
        .expect("the election roles the standby once the hung birth is deleted");
    // Commands go down (mesh-investigation.md step 6 and its messages: "the fabric-primary claims on its
    // own log, the executor adopts the won claim"): the delete is the fabric-primary's (R-FM1), and the
    // new birth at mesh2.admin.1 is created by the admin mesh2 roles in the hung birth's place, which
    // takes its join, its node.started and its member status. mesh2's lane opens no second recovery of
    // the hung birth.
    let created_rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-row")
        .into_iter()
        .filter(|sp| sp["attributes"]["operation"] == format!("node.create:{mp_name}").as_str() && sp["attributes"]["node_id"] == after["node_id"] && matches!(sp["attributes"]["outcome"].as_str(), Some("row-seeded" | "row-written")))
        .collect();
    assert!(!created_rows.is_empty() && created_rows.iter().all(|sp| sp["attributes"]["node"] == standby.as_str()), "the new birth at {mp_name} is created by {standby}, mesh2's primary primary, never by {fp}: {created_rows:#?}");
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == mp["node_id"]).collect();
    assert!(recoveries.iter().all(|sp| s(&sp["attributes"]["submitter"]) == fp), "the hung birth is recovered once, by {fp}: no recovery of it is submitted in mesh2's lane: {recoveries:#?}");
    eprintln!(
        "mesh-primary cell: {mp_name} (birth {}) terminated and recovered by {fp} at attempt {attempt}: trace {} | TerminateRuntime {:?} | {standby} primary (span {}) | new birth {}",
        s(&mp["node_id"]),
        s(&recovery["trace_id"]),
        terminate["output"],
        s(&role_span["span_id"]),
        s(&after["node_id"]),
    );
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 1), ("mesh2", 2, 1)], Duration::from_secs(30)).await;
    estate.stop().await;
}

/// CONTRACT (Q122; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" steps 1-4;
/// node-recovery.md step 3): `mesh1.admin.1`, the fabric-primary and mesh1's primary, is hung (every task
/// but the leader held by a tracer: the operating system shows it running). Unheard past the staleness
/// floor, `mesh1.admin.2`, the lowest Ready node-admin of its mesh, takes no primary role: it
/// runs fabric recovery. Node investigation reads the hung birth `running`; the recovery's first step is
/// `TerminateRuntime`, then `mesh1.admin.1` is re-created as a new birth at the same path.name, and the
/// workflow restores that node as mesh-primary and fabric-primary. At that take the re-created
/// fabric-primary reads the Build records (Q227: "the exception of FP, who will need to read the last
/// build record to determine if anything is in flight"; build.md §5 "new fabric-primary -> reads the
/// Builds newest first"): its process's one read of `builds.jsonl` is the take's, after it became
/// fabric-primary, naming the current Build. Must NOT happen: the hung birth held,
/// `mesh1.admin.2` named mesh-primary or fabric-primary, the hung birth recovered by anyone else or twice,
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
    let fp_name = s(&fp["name"]);
    assert_eq!((fp["mesh"].as_str(), fp["is_primary"].as_bool()), (Some("mesh1"), Some(true)), "the Day-0 admin is mesh-primary and fabric-primary: {fp}");
    let standby = s(&nodes.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["name"] != fp["name"]).expect("mesh1's other admin")["name"]);
    // Control is read through the other admin from here: a hung admin answers nothing.
    estate.enter(&Estate::path(&standby)).await;

    let rt = exact_runtime(&estate, &fp_name).await;
    let held = rt.hang().expect("the fabric-primary runtime is hung");
    assert!(!held.held.is_empty(), "every task of the fabric-primary but the leader is held: the provider sees it running");
    let hung_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // Only the node at the fabric-primary's path.name is ever mesh1's primary or the fabric-primary.
    let after = until_node_where(&estate, &fp_name, &format!("{fp_name} re-created and restored as mesh-primary and fabric-primary"), SETTLE, |n| {
        ready(n) && n["node_id"] != fp["node_id"] && n["is_fabric_primary"] == true && n["is_primary"] == true
    })
    .await;
    let other = estate.node_value(&standby).await;
    assert!(other["is_fabric_primary"] != true && other["is_primary"] != true, "{standby} took no primary role: {other}");
    let re_created_pid = estate.pid_of(&fp_name).await;
    drop(held);
    estate.settles_to(&[("mesh1", 2, 1), ("mesh2", 1, 1)], Duration::from_secs(30)).await;
    estate.stop().await;

    let spans = estate.spans();
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let ran: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| at(sp) >= hung_ns).collect();
    let run = ran.iter().find(|sp| s(&sp["attributes"]["node"]) == standby && sp["attributes"]["arm"] == "own-mesh" && s(&sp["attributes"]["outcome"]).starts_with("restored")).unwrap_or_else(|| panic!("{standby} ran fabric recovery from its own mesh and restored {fp_name}: {ran:#?}"));
    assert_eq!(s(&run["attributes"]["verdict"]), "running", "node investigation read the hung birth running: {run}");
    assert!(ran.iter().all(|sp| s(&sp["attributes"]["node"]) == standby), "nobody else ran it: {ran:#?}");
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == fp["node_id"]).collect();
    assert!(!recoveries.is_empty() && recoveries.iter().all(|sp| s(&sp["attributes"]["submitter"]) == standby), "the hung birth is recovered by {standby} alone: {recoveries:#?}");
    let terminate = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().find(|sp| sp["attributes"]["node"] == fp_name.as_str() && sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["outcome"] == "complete").cloned();
    assert!(terminate.is_some(), "the recovery's TerminateRuntime of the hung birth completed");
    let elected: Vec<&Value> = named(&spans, "rdm.mesh.election.resolve.via-mesh-primary").into_iter().filter(|sp| at(sp) >= hung_ns && sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["winner_path"] == standby.as_str()).collect();
    assert!(elected.is_empty(), "mesh1 elected no node-admin while fabric recovery ran: {elected:#?}");
    // The re-created fabric-primary's own spans: it reads the Build records at its take, and only then.
    let own: Vec<Value> = std::fs::read_dir(&estate.evidence)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(&format!("rdm-node-admin.{re_created_pid}-")))
        .flat_map(|e| std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()).collect::<Vec<_>>())
        .collect();
    let took = named(&own, "rdm.node_admin.primary.update.via-primary-updated").into_iter().map(at).min().unwrap_or_else(|| panic!("the re-created {fp_name} (pid {re_created_pid}) took its primary roles by node.primary.update"));
    let read = named(&own, "rdm.node_admin.build.resolve.via-records-read").into_iter().find(|sp| sp["attributes"]["outcome"] == "read").cloned().unwrap_or_else(|| panic!("the re-created {fp_name} (pid {re_created_pid}) reads the Build records at its take: {:#?}", named(&own, "rdm.node.store.resolve.via-fold")));
    assert!(at(&read) >= took, "the read is the take's, after {fp_name} became fabric-primary: {read}");
    assert!(!s(&read["attributes"]["current_build_id"]).is_empty(), "the take's read names the current Build: {read}");
    let folds: Vec<&Value> = named(&own, "rdm.node.store.resolve.via-fold").into_iter().filter(|sp| s(&sp["attributes"]["file"]).ends_with("builds.jsonl")).collect();
    assert!(!folds.is_empty() && folds.iter().all(|sp| at(*sp) >= took), "{fp_name} reads builds.jsonl from its take on, never before it: {folds:#?}");
    eprintln!("span rdm.node_admin.build.resolve.via-records-read trace_id={} {}", s(&read["trace_id"]), read["attributes"]);
    estate.record_trace_url(run["trace_id"].as_str().unwrap_or(""));
    eprintln!("Q122 hung fabric-primary: {fp_name} (birth {}) re-created as {} by {standby}'s fabric recovery", s(&fp["node_id"]), s(&after["node_id"]));
}

/// CONTRACT (node-recovery.md sequence step 1: "A repeat submit answers AlreadyApplied or
/// BuildInProgress, never a second recovery"; step 3: "the old (node.id, incarnation) targeted in the
/// peer-location view"): a member of the mesh that does not hold the fabric-primary is killed and
/// recovered through its mesh-primary's report (node-recovery.md "Execution", Q118). Once the
/// fabric-primary holds the recovery's receipts in that mesh's lane, the same recovery submitted
/// again to it on op 0x20 (`build.create` Recover keyed on the exited birth) answers Started
/// AlreadyApplied naming the recovery's attempt, then its Complete terminal: keyed on the exited
/// birth, whoever asks; nothing runs, the mesh's lane gains no attempt, the path keeps the recovered
/// node, and exactly one recovery was ever opened for that birth. From the fabric-primary's mesh, a call aimed at the
/// exited birth's node.id is not sent (`Resolve(Unknown)`): it never reaches the new process that
/// holds the same endpoint key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeat_recovery_submit_for_the_same_exited_birth_is_answered_already_applied() {
    use rdm_core::build::{BuildChange, Disposition};
    let (mut estate, birth_build) = two_mesh_estate("a_repeat_recovery_submit_for_the_same_exited_birth_is_answered_already_applied").await;
    let (node, mesh_primary, _) = placement(&estate.node_values().await);
    let before = estate.node_value(&node).await;
    let mesh = s(&before["mesh"]);
    let killed = exact_runtime(&estate, &node).await.apply(Fault::Kill(estate.fabric_ready())).expect("the exact runtime is killed");
    assert!(killed.exited, "the OS observed the exit: {killed:?}");

    let after = until_node_where(&estate, &node, &format!("{node} ready as a new node"), SETTLE, |n| ready(n) && n["node_id"] != before["node_id"]).await;
    let recovery = recovery_span(&estate, &node, &before["node_id"]).await;
    assert_eq!(s(&recovery["attributes"]["submitter"]), mesh_primary, "{recovery}");
    assert_eq!(s(&recovery["attributes"]["disposition"]), "created", "{recovery}");
    assert_eq!(s(&recovery["attributes"]["mesh"]), mesh, "an attempt of {mesh}'s lane: {recovery}");
    let attempt = s(&recovery["attributes"]["attempt"]);
    assert_eq!(
        s(&recovery["attributes"]["outcome"]),
        format!("complete: attempt {attempt}"),
        "{recovery}"
    );
    // The repeat follows the receipts the fabric-primary wrote.
    recovery_receipts_held(&estate, &birth_build, &mesh, &node, &attempt).await;

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
    assert_eq!(mesh_attempt_of(&estate, &birth_build, &mesh).await.to_string(), attempt, "{mesh}'s lane of the Build gained no attempt");
    assert_eq!(estate.node_value(&node).await["node_id"], after["node_id"], "the path keeps the recovered node");

    // The fabric-primary's mesh never dials the exited birth's node.id, though its key now serves the new node.
    let old = estate.probe(&["get", "--target", &format!("exact:{}", s(&before["node_id"])), "--key", "83"]);
    assert_eq!((&old["outcome"], &old["reason"], &old["route"]), (&json!("NotSent"), &json!("Resolve(Unknown)"), &json!("direct")), "exact:<old> resolves nothing: {old}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.stop().await;

    // Every process flushed its spans on exit: the whole record.
    let spans = estate.spans();
    let opened: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-recovery")
        .into_iter()
        .filter(|sp| {
            sp["attributes"]["node_id"] == before["node_id"]
                && matches!(
                    sp["attributes"]["outcome"].as_str(),
                    Some("admitted" | "reported")
                )
        })
        .collect();
    assert_eq!(
        opened.len(),
        1,
        "exactly one recovery is opened for the exited birth: {opened:?}"
    );
    let applied: Vec<&Value> = named(
        &spans,
        "rdm.node_admin.build.reject.via-recovery-already-applied",
    )
    .into_iter()
    .filter(|sp| sp["attributes"]["node_id"] == before["node_id"])
    .collect();
    assert_eq!(
        applied.len(),
        1,
        "the repeat is answered from the recovery's receipts: {applied:?}"
    );
    assert_eq!(
        s(&applied[0]["attributes"]["attempt"]),
        attempt,
        "{applied:?}"
    );
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted")
        .into_iter()
        .filter(|sp| sp["attributes"]["node_id"] == before["node_id"])
        .collect();
    assert_eq!(
        deleted.len(),
        1,
        "the exited birth is deleted once: {deleted:?}"
    );
}

/// CONTRACT (node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that
/// workflow: its Build ends Failed, and the mesh-primary's next tick finds the member still proven
/// exited and submits the recovery"): a member of the mesh that does not hold the fabric-primary is
/// held (SIGSTOP) and a `node.connection.delete { All }` of it is requested at once, attempt N of the
/// accepted Build (`node.connection.delete:<path>`, carried by its mesh-primary). While that call
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
    let (node, mesh_primary, _) = placement(&estate.node_values().await);
    let before = estate.node_value(&node).await;
    let rt = exact_runtime(&estate, &node).await;
    assert_eq!(rt.apply(Fault::Stop).expect("the exact runtime is held").state_after, Some('T'));
    let mut deleting = estate.submit_change(Change::DeleteConnections { node: Estate::path(&node) }).await;
    assert_eq!(deleting.build_id().as_str(), birth_build, "no Build is minted");
    let delete_attempt = u64::from(deleting.attempt());
    let killed = rt.apply(Fault::Kill(estate.fabric_ready())).expect("the held runtime is killed");
    assert!(killed.exited, "{killed:?}");
    // The call's own stream ends at the failed step: the held, then exited, birth never answered.
    let ended = loop {
        match deleting.next().await {
            Some(Ok(BuildFrame::Failed { attempt, operation, step, reason, .. })) => break (attempt, operation, step, reason),
            Some(Ok(_)) => continue,
            other => panic!("the stream of the node.connection.delete ended without its Failed frame: {other:?}"),
        }
    };
    assert_eq!((u64::from(ended.0), ended.2.as_str()), (delete_attempt, "DeleteConnections"), "the stream names the failed step: {ended:?}");

    let operation = format!("node.connection.delete:{node}");
    let failed_step = |p: &Value| p["steps"].as_array().cloned().unwrap_or_default().into_iter().find(|r| r["operation"] == operation.as_str() && r["attempt"] == json!(delete_attempt) && r["outcome"]["failed"].is_object());
    let held = build_log::build_fact(&estate, &birth_build, "the node.connection.delete attempt ends Failed", SETTLE, |p| failed_step(&serde_json::to_value(p).expect("a Build view serializes")).is_some())
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    let failed = failed_step(&serde_json::to_value(held).expect("a Build view serializes")).expect("the failed step was just held");
    // The exit belongs to the node.connection.delete: the held, then exited, birth never answered
    // node.connections.deleted, and the attempt ends Failed, never Complete.
    assert_eq!(s(&failed["step"]), "DeleteConnections", "the node.connection.delete is not complete: {failed}");
    assert!(s(&failed["outcome"]["failed"]["reason"]).contains("node.connection.delete { All } ended"), "the failure names how the call ended: {failed}");

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
    let spans = wait_for("the failed node.connection.delete attempt's reconcile span is exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        reconcile_span(&spans, &birth_build, &delete_attempt.to_string()).is_some().then_some(spans)
    })
    .await;
    let workflow = reconcile_span(&spans, &birth_build, &delete_attempt.to_string()).expect("attempt N's reconcile span");
    assert_eq!(s(&workflow["attributes"]["outcome"]), "failed", "{workflow}");
    let opened = named(&in_trace(&spans, &recovery), "rdm.node_admin.build.update.via-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"] && matches!(sp["attributes"]["outcome"].as_str(), Some("admitted" | "reported"))).cloned().expect("the recovery's open");
    assert!(end(workflow) <= start(&opened), "the recovery opens once attempt N ended: {workflow} vs {opened}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    estate.settles_to(&[("mesh1", 1, 2), ("mesh2", 1, 2)], Duration::from_secs(30)).await;
    estate.shutdown().await;
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
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    assert!(fp.starts_with("mesh1."), "the day-0 admin keeps the fabric-primary role: {fp}");
    let mp = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2's primary")["name"]);
    let standby = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != mp.as_str()).expect("mesh2's second admin")["name"]);
    const NODE: &str = "mesh2.rpc.1";
    let before = estate.node_value(NODE).await;
    let rt = exact_runtime(&estate, NODE).await;
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
    let round1 = wait_for(&format!("{mp}'s ladder for {NODE} ends its direct probe"), round1_due, || async {
        named(&estate.spans(), "rdm.node.tickle.resolve.via-direct").into_iter().find(|sp| sp["attributes"]["caller"] == mp.as_str() && sp["attributes"]["node_id"] == before["node_id"]).cloned()
    })
    .await;
    // Mid-ladder: the mesh-primary is stopped, and mesh2's role goes to its second admin.
    let stopping = estate.submit_change(Change::Stop { node: Estate::path(&mp) }).await;
    assert_eq!(stopping.build_id().as_str(), birth_build, "no Build is minted");
    estate.follow(stopping).await;
    let role = wait_for(&format!("{standby} takes mesh2's role"), floor * 2, || async {
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
    assert_ne!(after["endpoint_id"], before["endpoint_id"], "a verdict other than exited re-creates over a newly allocated data dir, so a new transport key");

    let spans = estate.spans();
    let taken_ns = start(&role);
    let new_round1 = tickle_span(&spans, &standby, &before["node_id"]).unwrap_or_else(|| panic!("{standby}'s TRUE OFFLINE for {NODE}"));
    assert!(start(new_round1) > taken_ns, "{standby} tickles only once it holds the primary role");
    // The window it kept: the ladder runs on the instant it first saw the member silent, before the
    // role changed hands and a probe delay or more before it, never on a window opened at the role.
    // (The ladder's span is written when the ladder ends: the failed direct probe and via-peer probes
    // take their call deadlines, so its time after the role is not the wait before it.)
    let first_seen_ms = num_attr(new_round1, "first_seen_ms");
    assert!(first_seen_ms * 1_000_000 + (floor / 2).as_nanos() as u64 <= taken_ns, "{standby}'s window began a probe delay or more before it took the role: first_seen {first_seen_ms} ms, role at {} ms", taken_ns / 1_000_000);
    let waited = Duration::from_nanos(start(new_round1) - taken_ns);
    assert!(start(new_round1) < start(&recovery), "TRUE OFFLINE before the submit");
    let inspection = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection")
        .into_iter()
        .find(|sp| sp["attributes"]["node_id"] == before["node_id"] && sp["attributes"]["inspector"] == standby.as_str())
        .unwrap_or_else(|| panic!("{standby}'s provider inspection of {NODE}"));
    assert_eq!(s(&inspection["attributes"]["verdict"]), "running", "{inspection}");
    assert!(!named(&spans, "rdm.node_admin.build.update.via-node-recovery").iter().any(|sp| sp["attributes"]["submitter"] == mp.as_str() && s(&sp["attributes"]["outcome"]).starts_with("complete")), "the stopped primary recovered nothing");
    let drift: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| s(&sp["attributes"]["scope"]).contains(NODE)).collect();
    assert!(drift.is_empty(), "the fabric-primary opened a drift attempt for a member: {drift:?}");
    eprintln!(
        "acceptance 2: old mp {mp} direct probe trace {} | role {standby} at {} ms | new TRUE OFFLINE {} (first_seen {first_seen_ms} ms, {waited:?} after the role) | recovery trace {} ({})",
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
/// of type <node_type> in mesh <mesh>`, until a secondary is available. The primary stays primary, nothing
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
    let fp = s(&primary["name"]);
    let mesh = s(&primary["mesh"]);
    assert_eq!(before.iter().filter(|n| n["kind"] == "node_admin" && n["mesh"] == mesh.as_str()).count(), 1, "the fabric-primary is its mesh's only admin: {before:#?}");
    let attempt = build_log::build_value(&estate, &birth_build).await["attempt"].clone();
    let q7 = format!("cannot transfer: no secondary of type node-admin in mesh {mesh}");
    let node = Estate::path(&fp);
    let carrier = estate.carrier().await;
    for (action, change) in [
        ("replace", Change::Steps { steps: replace_steps(&node) }),
        ("node.connection.delete", Change::DeleteConnections { node: node.clone() }),
        ("stop", Change::Stop { node: node.clone() }),
    ] {
        match carrier.create(change).await {
            // The call ends naming the reason, then the refusal's text: `<reason>: <text>`.
            Err(CallEnd::Rejected { reason, detail }) => assert_eq!((reason.as_str(), detail.as_str()), ("last-admin", format!("last-admin: {q7}").as_str()), "{action}: refused with Q7's exact text"),
            Ok(_) => panic!("{action} of the fabric-primary {fp}, its mesh's last admin, was accepted"),
            Err(other) => panic!("{action} of the fabric-primary {fp}, its mesh's last admin, is refused last-admin, not {other:?}"),
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
        assert_eq!(estate.node_value(&s(&n["name"])).await["incarnation_id"], n["incarnation_id"], "{} is untouched", s(&n["name"]));
    }
    estate.shutdown().await;
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md Part C1 "The
/// no-successor cases", Q7 ruled Luke 2026-10-12: "`node.primary.update` commanded for a node type with no
/// other node of that type in the mesh is refused by name, `cannot transfer: no secondary of type
/// <node_type> in mesh <mesh>` ... The primary stays primary, nothing is cut off, no row is written, and
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
    let admins: Vec<&Value> = before.iter().filter(|n| n["kind"] == "node_admin" && n["mesh"] == "mesh2").collect();
    assert_eq!(admins.len(), 1, "mesh2 has one node-admin: {before:#?}");
    let primary = admins[0];
    assert_eq!((primary["is_primary"].clone(), primary["is_fabric_primary"].clone()), (json!(true), json!(false)), "mesh2's only admin is its mesh-primary, not the fabric-primary: {primary}");
    let mp = s(&primary["name"]);
    let attempt = build_log::build_value(&estate, &birth_build).await["attempt"].clone();
    let q7 = "cannot transfer: no secondary of type node-admin in mesh mesh2";
    match estate.carrier().await.create(Change::Steps { steps: replace_steps(&Estate::path(&mp)) }).await {
        // The call ends naming the reason, then the refusal's text: `<reason>: <text>`.
        Err(CallEnd::Rejected { reason, detail }) => assert_eq!((reason.as_str(), detail.as_str()), ("last-admin", format!("last-admin: {q7}").as_str()), "the replace of {mp} is refused with Q7's exact text"),
        Ok(_) => panic!("the replace of {mp}, mesh2's only node-admin, was accepted"),
        Err(other) => panic!("the replace of {mp}, mesh2's only node-admin, is refused last-admin, not {other:?}"),
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
        let now = estate.node_value(&s(&n["name"])).await;
        assert_eq!((&now["node_id"], &now["incarnation_id"], &now["is_primary"]), (&n["node_id"], &n["incarnation_id"], &n["is_primary"]), "{} is untouched", s(&n["name"]));
    }
    eprintln!("trace {}: rdm.node_admin.build.reject.via-last-admin detail={q7:?} (replace of {mp})", s(&refused[0]["trace_id"]));
    estate.shutdown().await;
}

/// A caller outside the fabric that reaches the node-admin `admin` by its exact birth: its own
/// endpoint, and a resolver holding that birth as the answering admin's view names it.
async fn caller_of(admin: &Value) -> (std::sync::Arc<rdm_transport::NodeRpcClient>, rdm_core::NodeId) {
    use rdm_transport::{NodeRpcClient, ResolvedNode, StaticResolver};
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller binds an endpoint");
    let resolver = std::sync::Arc::new(StaticResolver::new());
    let node_id: rdm_core::NodeId = s(&admin["node_id"]).parse().expect("a node id");
    resolver.insert(ResolvedNode {
        node_id: node_id.clone(),
        name: s(&admin["name"]).parse().unwrap(),
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
/// node's name claim is seeded from it (`via-node-row` moment `claim`, `seeded_from` the exited node,
/// one key, `tags` rabbit), the new node's join answer hands it that config (`via-join`
/// `config_keys` 1), the new node (new node.id, same path.name) carries the config and the tag, and
/// `node.get` by the tag names the new node and not the exited one. Nothing copies them after the
/// create: no node.update runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_node_keeps_the_config_and_tags_its_mesh_primary_told_its_predecessor() {
    let (estate, _build) = two_mesh_estate("a_recovered_node_keeps_the_config_and_tags_its_mesh_primary_told_its_predecessor").await;
    let nodes = estate.node_values().await;
    let (node, mesh_primary, fp) = placement(&nodes);
    let node_path = node.as_str();
    let before = estate.node_value(node_path).await;
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let node_id: rdm_core::NodeId = s(&before["node_id"]).parse().unwrap();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node_id.clone()), node_id, incarnation: rdm_core::IncarnationId::parse(&s(&before["incarnation_id"])).unwrap() };
    // node-config-update.md step 1: the caller sends it to the fabric-primary, which sends it down.
    let (fp_client, fp_id) = caller_of(&fp).await;
    let reply = rdm_client::NodeRpc::new(&fp_client)
        .config_update(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, told.clone(), &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.config.update of {node_path} at the fabric-primary: {e:?}"));
    assert_eq!(reply.name(), "config-updated", "the mesh-primary wrote the row and the node applied it: {reply:?}");
    let told_json = json!({"tier": "gold"});
    wait_for(&format!("{node_path} carries its config"), Duration::from_secs(30), || async { (estate.node_value(node_path).await["config"] == told_json).then_some(()) }).await;
    let labels: std::collections::BTreeSet<String> = ["rabbit".to_string()].into();
    // Commands go down: the caller sends it to the fabric-primary, which sends it to the mesh-primary (Q203).
    let tagged = rdm_client::NodeRpc::new(&fp_client)
        .tags_create(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, &labels, &rdm_transport::CallOptions::default())
        .await
        .unwrap_or_else(|e| panic!("node.tag.create of {node_path} at the fabric-primary: {e:?}"));
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
        .get(&rdm_client::Filter::new().eq("tags", labels.iter().cloned()))
        .await
        .unwrap_or_else(|e| panic!("node.get [tags: rabbit]: {e:?}"))
        .into_iter()
        .map(|n| n.node_id.to_string())
        .collect();
    assert_eq!(selected, vec![s(&after["node_id"])], "the tags index names the new node under the label, and not the exited one");
    let told: Vec<Value> = named(&estate.spans(), "rdm.node_admin.node.update.via-config").into_iter().filter(|sp| sp["attributes"]["node_id"] == after["node_id"]).cloned().collect();
    assert!(told.is_empty(), "the join handed the config: no config call is sent to the starting node: {told:?}");
    eprintln!("trace {}: via-node-row delete node_id={} keys={} tags={}; trace {}: via-node-row claim node={} node_id={} seeded_from={} keys={} tags={}", s(&deleted[0]["trace_id"]), s(&before["node_id"]), deleted[0]["attributes"]["keys"], s(&deleted[0]["attributes"]["tags"]), s(&claim["trace_id"]), mesh_primary, s(&after["node_id"]), s(&a["seeded_from"]), a["keys"], s(&a["tags"]));
}

/// CONTRACT (node-recovery.md "Execution", Q118: the mesh-primary reports the drift up, the
/// fabric-primary opens the attempt, the mesh-primary claims it through BuildClaim, the fabric-primary
/// calls `build.attempt.run` naming the mesh and writes every Build record): mesh1 holds the
/// fabric-primary and a second node-admin; mesh2 holds one node-admin, its mesh-primary, and two rpc
/// nodes. The fabric-primary is stopped by plan (node.stop: its roles move to mesh1's other
/// node-admin, then it stops). With it stopped, `mesh2.rpc.1` is killed. mesh2's primary proves the
/// exit and reports it to the admin that took the fabric-primary role: the submit's trace holds that
/// admin's open in mesh2's lane, its decision of mesh2's primary's claim, its run naming the mesh, and
/// mesh2's primary's run of the attempt and its converged reconcile. No other admin submits the
/// recovery, no attempt of the fabric lane acts on the member, the stopped fabric-primary stays
/// stopped through it, and the fabric-primary now holds the receipts. Must NOT happen: the recovery
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
    let fp_name = s(&fp["name"]);
    assert!(fp_name.starts_with("mesh1."), "the day-0 admin holds the fabric-primary role: {fp_name}");
    let successor_name = s(&nodes.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["name"] != fp["name"]).expect("mesh1's other node-admin")["name"]);
    let mp = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2's primary")["name"]);

    // The fabric-primary is stopped by plan: its roles move to mesh1's other node-admin, then it stops.
    // The stream ends at the old role once the role moves, naming the new one; the stop is
    // followed there to its completion.
    let mut stop = estate.nodes_obj().await.stop(&Estate::path(&fp_name)).await.unwrap_or_else(|e| panic!("node.stop of the fabric-primary {fp_name} was refused: {e}"));
    let accepted = stop.accepted().clone();
    let frames = Estate::frames_of(&mut stop).await;
    match frames.last() {
        Some(rdm_client::Frame::FabricPrimaryMoved { fabric_primary }) => assert_eq!(fabric_primary.as_deref(), Some(successor_name.as_str()), "the role moved to mesh1's other node-admin: {frames:?}"),
        other => panic!("the stop of the fabric-primary moves its role first: {other:?} of {frames:?}"),
    }
    estate.enter(&Estate::path(&successor_name)).await;
    let mut resumed = estate.nodes_obj().await.resume(stop.kind().clone(), &accepted).await.unwrap_or_else(|e| panic!("{successor_name} does not stream the stop of {fp_name} (Build {} attempt {}): {e}", accepted.build_id, accepted.attempt));
    Estate::complete(&mut resumed).await;
    let stopped = estate
        .until_view(&format!("{fp_name} is stopped and {successor_name} holds the fabric-primary role"), SETTLE, |views| {
            let old = views.iter().find(|n| n.name.to_string() == fp_name)?;
            let new = views.iter().find(|n| n.name.to_string() == successor_name)?;
            (old.stopped && !old.is_fabric_primary && new.is_fabric_primary).then(|| serde_json::to_value(old).expect("a view serializes"))
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
    let spans = wait_for("the recovery's reconcile and the fabric-primary's serve of its report are exported", Duration::from_secs(30), || async {
        let spans = estate.spans();
        let run = in_trace(&spans, &done);
        (lane_reconcile_span(&run, &build, "mesh2", &attempt).is_some() && report_served(&run, &before, &attempt)).then_some(spans)
    })
    .await;
    let run = in_trace(&spans, &done);
    assert_mesh_primary_ran_the_recovery(
        &run,
        &mp,
        &successor_name,
        "mesh2",
        &build,
        &attempt,
        &before,
    );
    // No other admin submitted, opened or claimed it, and no attempt of the fabric lane acted on it.
    for sp in submits(&spans) {
        assert_eq!(s(&sp["attributes"]["submitter"]), mp, "only mesh2's primary submits its member's recovery: {sp}");
    }
    let submit_traces: std::collections::BTreeSet<String> = submits(&spans).iter().map(|sp| s(&sp["trace_id"])).collect();
    for sp in named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| sp["attributes"]["node_id"] == before["node_id"]) {
        assert!(submit_traces.contains(&s(&sp["trace_id"])), "every open of the recovery is under mesh2's primary's submit: {sp}");
    }
    for sp in named(&spans, "rdm.node_admin.build.update.via-claim-decision")
        .into_iter()
        .filter(|sp| {
            s(&sp["attributes"]["build_id"]) == build
                && sp["attributes"]["lane"] == "mesh2"
                && start(sp) >= start(&done)
        })
    {
        assert_eq!(
            s(&sp["attributes"]["node"]),
            successor_name,
            "only the fabric-primary decides a claim in mesh2's lane: {sp}"
        );
    }
    let member_ops: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .filter(|r| {
            s(&r["attributes"]["build_id"]) == build
                && s(&r["attributes"]["operations"]).contains(&format!("node.create:{NODE}"))
                && start(r) >= start(&done)
        })
        .collect();
    assert!(
        member_ops
            .iter()
            .all(|r| r["attributes"]["lane"] == "mesh2" && s(&r["attributes"]["executor"]) == mp),
        "only mesh2's lane acts on the member after its exit: {member_ops:?}"
    );
    // The stopped fabric-primary stayed stopped through the recovery, and the fabric-primary now holds
    // the receipts it wrote.
    let still = estate.node_value(&fp_name).await;
    assert_eq!((still["stopped"].clone(), still["incarnation_id"].clone()), (json!(true), fp["incarnation_id"].clone()), "{fp_name} stayed stopped: {still}");
    let (deleted, created) = recovery_receipts_held(&estate, &build, "mesh2", NODE, &attempt).await;
    let open = named(&run, "rdm.node_admin.build.update.via-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"]).cloned().expect("the open");
    let claim = named(&run, "rdm.node_admin.build.update.via-claim-decision").into_iter().find(|sp| s(&sp["attributes"]["attempt"]) == attempt).cloned().expect("the claim");
    eprintln!(
        "s78 acceptance 1: fabric-primary {fp_name} stopped, role at {successor_name}; {NODE} recovered by {mp} at attempt {attempt} of mesh2's lane of {build}: trace {} | via-node-recovery span {} | via-recovery span {} outcome {} | via-claim-decision span {} node {} outcome {} | {successor_name} holds receipts {} and {}",
        s(&done["trace_id"]), s(&done["span_id"]), s(&open["span_id"]), s(&open["attributes"]["outcome"]), s(&claim["span_id"]), s(&claim["attributes"]["node"]), s(&claim["attributes"]["outcome"]), s(&deleted["step"]), s(&created["step"]),
    );
    estate.record_trace_url(done["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md "Execution", Q118: the fabric-primary opens a reported drift, decides its
/// mesh-primary's claim and runs it naming the mesh; build.md :365 as the approved proposal A4 words it:
/// "The next attempt is claimed after the running attempt's terminal receipt (Complete, Failed or
/// HandedOff), or, when the executor is lost with the attempt unfinished, after exact departure
/// proof"): mesh2 has two node-admins, each the testkit's faulted node-admin. `mesh2.rpc.1` is killed;
/// mesh2's primary P reports its recovery, the fabric-primary FP opens it as an attempt of mesh2's
/// lane, P claims it and FP runs it at P. The fault door holds P inside the run, at the member's
/// node.delete `deleted` event (the `event` cut: the NodeDeleted receipt reported and written by FP,
/// node.deleted not yet published). Once FP's Build log, the one writer's and the one an admin reads the
/// Build records into (Q118, Q227), holds that attempt running with P its executor and its NodeDeleting
/// receipt, P is killed while the cut holds it (claimed, mid-run, no
/// verdict). FP ends that attempt Failed only on P's proven departure, naming it; mesh2's other admin
/// NA2 is elected mesh2's primary, and FP, which opens an unheard mesh-primary's recovery
/// (node-recovery.md step 2 and sequence step 1), claims the next attempt of the fabric lane for P's
/// recovery for NA2, which runs it (node-recovery.md "Execution": "the mesh-primary claims it ... the
/// mesh-primary runs it"; `build.update.via-claim-decision` at FP, executor NA2, won) after the lost
/// attempt ended. P comes back as a
/// new node, and the member's recovery, owed again from the Failed attempt, completes through NA2 in
/// mesh2's lane. Must NOT happen: mesh2's lane held for good by the lost executor, or an admin other
/// than the fabric-primary deciding a claim in mesh2's lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_whose_mesh_primary_exits_mid_run_ends_on_its_departure_and_the_fabric_primary_recovers_it() {
    let (estate, build) = faulted_fabric_of("a_recovery_whose_mesh_primary_exits_mid_run_ends_on_its_departure_and_the_fabric_primary_recovers_it", &[("mesh1", 1, 1), ("mesh2", 2, 2)], Duration::from_secs(90)).await;
    let nodes = estate.node_values().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    let p = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().expect("mesh2's primary");
    let p_name = s(&p["name"]);
    let na2 = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != p["name"]).expect("mesh2's other node-admin")["name"]);
    assert!(!fp.starts_with("mesh2."), "the fabric-primary is not in mesh2: {fp}");

    // The member's recovery is the one node.delete of it P runs: the cut holds P at its node.deleted
    // event until P is killed.
    const NODE: &str = "mesh2.rpc.1";
    let door = Door::at(&estate, &p_name).await;
    door.arm(
        "member-deleted",
        json!({"kind": "event", "event": "deleted", "node": NODE}),
    )
    .await;
    let before = estate.node_value(NODE).await;
    estate.kill_node(NODE).await;
    let held = door.wait_held("member-deleted").await;
    assert_eq!(
        (s(&held["hit"]["build_id"]), s(&held["hit"]["operation"])),
        (build.clone(), format!("node.delete:{NODE}")),
        "P holds the member's node.deleted of the accepted Build: {held}"
    );
    let lost_attempt = held["hit"]["attempt"]
        .as_u64()
        .expect("the held event names its attempt");
    // FP wrote the attempt's records (Q118): in its log mesh2's lane runs that attempt for the member's
    // recovery, P its executor, with no verdict. NA2 reads no Build record (Q227).
    let heard = build_log::build_fact(&estate, &build, &format!("attempt {lost_attempt} of mesh2's lane running with {p_name} its executor, its NodeDeleting durable"), SETTLE, |pr| {
        let b = serde_json::to_value(pr).expect("a Build view serializes");
        let lane = &b["lanes"]["mesh2"];
        let deleting = lane_steps(&b, "mesh2").iter().any(|r| r["operation"] == format!("node.delete:{NODE}").as_str() && r["step"] == "NodeDeleting" && r["attempt"] == json!(lost_attempt) && r["outcome"] == "complete");
        deleting && lane["attempt"] == json!(lost_attempt) && lane["state"] == "running" && lane["executor"] == p_name.as_str() && lane["claimant"] == fp.as_str() && lane["action"]["action"] == "recover" && lane["action"]["path"] == NODE
    })
    .await
    .unwrap_or_else(|end| panic!("{end}"));
    estate.kill_node(&p_name).await;

    // FP ends P's attempt of mesh2's lane on P's proven departure: at its run of the attempt, or at its
    // door when that run had not proven it yet.
    let ended = wait_for(
        &format!("{fp} ends attempt {lost_attempt} of mesh2's lane on {p_name}'s departure"),
        SETTLE,
        || async {
            let spans = estate.spans();
            let at_door = named(&spans, "rdm.node_admin.build.update.via-claimant-departure")
                .into_iter()
                .find(|sp| {
                    sp["attributes"]["node"] == fp.as_str()
                        && sp["attributes"]["executor"] == p_name.as_str()
                        && sp["attributes"]["lane"] == "mesh2"
                        && s(&sp["attributes"]["attempt"]) == lost_attempt.to_string()
                        && sp["attributes"]["outcome"] == "ended"
                });
            let at_run = named(&spans, "rdm.node_admin.build.update.via-drift-run")
                .into_iter()
                .find(|sp| {
                    sp["attributes"]["node"] == fp.as_str()
                        && sp["attributes"]["executor"] == p_name.as_str()
                        && sp["attributes"]["lane"] == "mesh2"
                        && s(&sp["attributes"]["attempt"]) == lost_attempt.to_string()
                        && sp["attributes"]["outcome"] == "failed"
                        && s(&sp["attributes"]["reason"]).contains("departure is proven")
                });
            at_door.or(at_run).cloned()
        },
    )
    .await;
    let take_over = wait_for(
        "FP's submit and claim of P's recovery are exported",
        Duration::from_secs(60),
        || async {
            let spans = estate.spans();
            // FP's submit may repeat: one sent while mesh2's lane still runs its attempt is answered
            // BuildInProgress (node-recovery.md step 1: "A repeat answers AlreadyApplied or
            // BuildInProgress, never a second recovery"), and the submit that opens the recovery is the
            // one whose trace carries FP's claim decision.
            named(&spans, "rdm.node_admin.build.update.via-node-recovery")
                .into_iter()
                .filter(|sp| {
                    sp["attributes"]["submitter"] == fp.as_str()
                        && sp["attributes"]["node_id"] == p["node_id"]
                })
                .find_map(|submit| {
                    let claim = named(
                        &in_trace(&spans, submit),
                        "rdm.node_admin.build.update.via-claim-decision",
                    )
                    .into_iter()
                    .find(|sp| sp["attributes"]["node"] == fp.as_str())
                    .cloned()?;
                    Some((submit.clone(), claim))
                })
        },
    )
    .await;
    let (submit, claim) = take_over;
    assert_eq!(
        s(&submit["attributes"]["node"]),
        p_name,
        "FP's submit is of P's recovery: {submit}"
    );
    assert_eq!(
        (s(&claim["attributes"]["lane"]), s(&claim["attributes"]["executor"]), s(&claim["attributes"]["outcome"])),
        ("fabric".to_string(), na2.clone(), "won".to_string()),
        "FP claimed the fabric lane for P's recovery for NA2, mesh2's elected primary, which runs it (node-recovery.md \"Execution\"): {claim}"
    );
    assert_eq!(s(&claim["attributes"]["node"]), fp, "FP decides the claim: {claim}");
    assert!(
        end(&ended) <= start(&claim),
        "the lost attempt ended before the next was claimed"
    );
    // P and the member come back as new nodes; the member's recovery completes through NA2, mesh2's primary.
    for (path, old) in [(p_name.as_str(), &p), (NODE, &before)] {
        until_new_birth(&estate, path, old, SETTLE).await;
    }
    let member = wait_for("the member's completed recovery submit is exported", Duration::from_secs(60), || async {
        named(&estate.spans(), "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| sp["attributes"]["node_id"] == before["node_id"] && s(&sp["attributes"]["outcome"]).starts_with("complete")).cloned()
    })
    .await;
    assert_eq!(
        (
            s(&member["attributes"]["submitter"]),
            s(&member["attributes"]["mesh"])
        ),
        (na2.clone(), "mesh2".to_string()),
        "the member's recovery completes through NA2 in mesh2's lane: {member}"
    );
    let spans = estate.spans();
    for sp in named(&spans, "rdm.node_admin.build.update.via-claim-decision")
        .into_iter()
        .filter(|sp| {
            s(&sp["attributes"]["build_id"]) == build && sp["attributes"]["lane"] == "mesh2"
        })
    {
        assert_eq!(
            s(&sp["attributes"]["node"]),
            fp,
            "only the fabric-primary decides claims in mesh2's lane: {sp}"
        );
    }
    eprintln!(
        "s78 gap 1 live: {p_name} held by the fault door at the member's node.deleted {} and killed running attempt {lost_attempt} of mesh2's lane of {build} ({na2} held it running with executor {:?}); ended by {} span {} | FP submit span {} node {p_name} | via-claim-decision span {} attempt {} outcome {} | member complete trace {} attempt {}",
        held["hit"], heard.lanes.get("mesh2").and_then(|l| l.executor.clone()), s(&ended["name"]), s(&ended["span_id"]), s(&submit["span_id"]), s(&claim["span_id"]), s(&claim["attributes"]["attempt"]), s(&claim["attributes"]["outcome"]), s(&member["trace_id"]), s(&member["attributes"]["attempt"]),
    );
    estate.record_trace_url(ended["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}

/// CONTRACT (node-recovery.md "Lost data dir (R-FM2)", crit:e12.s87.1): when `mesh1.rpc.2`'s runtime
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
    const NODE: &str = "mesh1.rpc.2";
    let before = estate.node_value(NODE).await;
    let old_dir = estate.data_dir(NODE).await;
    let old_node_id = s(&before["node_id"]);
    let old_incarnation = s(&before["incarnation_id"]);
    let old_endpoint = s(&before["endpoint_id"]);

    estate.kill_node(NODE).await;
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

    let after = until_new_birth(&estate, NODE, &before, SETTLE).await;
    assert_ne!(after["node_id"], before["node_id"], "new node.id minted: {after}");
    assert_ne!(s(&after["endpoint_id"]), old_endpoint, "new endpoint key minted (never re-minted from lost dir): {after}");
    let new_dir = estate.data_dir(NODE).await;
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

    let submit = recovery_span(&estate, NODE, &before["node_id"]).await;
    eprintln!(
        "s87 cell 1 (gone): {NODE} old ({old_node_id}, {old_incarnation}, dir {old_dir}, key {old_endpoint}) -> new ({}, {}, dir {new_dir}, key {}) | via-data-dir-lost trace {} span {} | submit trace {} | pre_node_ready trace {} inherited={}",
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
/// crit:e12.s87.2): when `mesh1.rpc.2`'s runtime exits and `<data_dir>/node-key` is corrupted, the
/// mesh-primary proves `Exited`, attempts `PrepareStorage`, fails `load_transport_key`, records
/// `data-dir-unreadable { path, cause }` on the birth's status row, emits
/// `rdm.node_admin.node.reject.via-data-dir-unreadable`, and HOLDS recovery. Over 3 additional
/// staleness floors: NO new `node.id` is minted, NO new endpoint key is minted, NO fresh `data_dir`
/// is allocated, NO second `rdm.node_admin.build.update.via-node-recovery` span is opened while the
/// dir remains unreadable, and `node.get` surfaces the `data-dir-unreadable` fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_whose_data_dir_is_unreadable_records_the_fact_and_holds_recovery_without_retrying() {
    let (estate, _build) = fabric_of(
        "a_member_whose_data_dir_is_unreadable_records_the_fact_and_holds_recovery_without_retrying",
        &[("mesh1", 1, 2)],
        Duration::from_secs(60),
    )
    .await;
    const NODE: &str = "mesh1.rpc.2";
    let before = estate.node_value(NODE).await;
    let old_dir = estate.data_dir(NODE).await;
    let old_node_id = s(&before["node_id"]);
    let old_incarnation = s(&before["incarnation_id"]);
    let key_path = Path::new(&old_dir).join("node-key");

    estate.kill_node(NODE).await;
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

    let held_view = until_node_where(&estate, NODE, "node.get surfaces data_dir_unreadable", crate::node_recovery_due(), |n| {
        n["data_dir_unreadable"]["path"] == json!(old_dir) && !s(&n["data_dir_unreadable"]["cause"]).is_empty()
    })
    .await;
    assert_eq!(s(&held_view["node_id"]), old_node_id, "no new node.id minted while unreadable: {held_view}");

    // Hold over 3 additional staleness floors: no second recovery submit,
    // no new node_id, no fresh data_dir.
    tokio::time::sleep(rdm_core::cadence::staleness_floor() * 3 + Duration::from_millis(200)).await;

    let spans = estate.spans();
    let submits: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-node-recovery")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["node_id"]) == old_node_id)
        .collect();
    assert_eq!(submits.len(), 1, "no second via-node-recovery span opened while data_dir remains unreadable: {submits:#?}");

    let views = estate.node_values().await;
    let at_name: Vec<&Value> = views.iter().filter(|n| n["name"] == NODE).collect();
    assert_eq!(at_name.len(), 1, "still only the held birth at {NODE}: {at_name:#?}");
    assert_eq!(s(&at_name[0]["node_id"]), old_node_id, "no new node_id minted: {at_name:#?}");
    assert_eq!(s(&at_name[0]["data_dir_unreadable"]["path"]), old_dir, "data_dir_unreadable still surfaced: {at_name:#?}");

    eprintln!(
        "s87 cell 2 (broken hold): {NODE} ({old_node_id}, {old_incarnation}) held with data_dir_unreadable at {old_dir} | reject trace {} span {} cause={:?} | submits count={}",
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
    const NODE: &str = "mesh1.rpc.2";
    let before = estate.node_value(NODE).await;
    let old_dir = estate.data_dir(NODE).await;
    let old_node_id = s(&before["node_id"]);
    let old_incarnation = s(&before["incarnation_id"]);
    let old_endpoint = s(&before["endpoint_id"]);
    let key_path = Path::new(&old_dir).join("node-key");
    let saved_key = std::fs::read_to_string(&key_path).expect("read original node-key before corruption");

    estate.kill_node(NODE).await;
    std::fs::write(&key_path, "corrupt-not-valid-hex-key").expect("corrupt node-key");

    let reject_span = wait_for("rdm.node_admin.node.reject.via-data-dir-unreadable span", crate::node_recovery_due(), || async {
        named(&estate.spans(), "rdm.node_admin.node.reject.via-data-dir-unreadable")
            .into_iter()
            .find(|sp| s(&sp["attributes"]["node_id"]) == old_node_id)
            .cloned()
    })
    .await;
    let _held_view = until_node_where(&estate, NODE, "node.get surfaces data_dir_unreadable", crate::node_recovery_due(), |n| {
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

    let after = until_new_birth(&estate, NODE, &before, SETTLE).await;
    assert_ne!(after["node_id"], before["node_id"], "new node.id minted: {after}");
    assert_eq!(s(&after["endpoint_id"]), old_endpoint, "retained endpoint key from repaired data_dir: {after}");
    assert_eq!(estate.data_dir(NODE).await, old_dir, "recovered over the same data_dir: {after}");
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
        "s87 cell 3 (repaired): {NODE} old ({old_node_id}, {old_incarnation}) -> new ({}, {}) over same dir {old_dir} with retained key {old_endpoint} | reject trace {} | repaired trace {} span {} | pre_node_ready trace {} inherited={}",
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
async fn exe_of(estate: &Estate, node: &str) -> std::path::PathBuf {
    let pid = estate.pid_of(node).await;
    std::fs::read_link(format!("/proc/{pid}/exe")).unwrap_or_else(|e| panic!("{node} (pid {pid}): /proc exe: {e}"))
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
/// binding set A (`rpc_node` = the built rpc node), every path named on set A's executable of its kind,
/// and mesh1's and mesh2's admins alike launch set A's. A Create binding, a copy the set does not name, is the Create step of a
/// replace steps of `mesh1.rpc.1` (run by mesh1's admin) and of `mesh2.rpc.1` (run by mesh2's admin):
/// both successors run that copy. `mesh1.rpc.1` is then killed and recovered over its inherited data
/// dir, and `mesh2.rpc.1` held (SIGSTOP) and recovered over a newly allocated one: each recovered node
/// runs the copy its row records, not set A's. A Create step naming a file that is not there,
/// and one naming the copy with a hash its bytes do not have, end Failed by name, nothing launched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_steps_executable_reaches_the_provider_at_every_admin_and_through_recovery() {
    use rdm_client::binding::{sha256_file, Binding};
    let sha = candidate_sha();
    let set_a = binding_set(&sha);
    let estate = Estate::bootstrap_on(owner("a_create_steps_executable_reaches_the_provider_at_every_admin_and_through_recovery"), "fabric1", "mesh1", &set_a, &sha, &["rpc_node"])
        .await
        .expect("binding set A is accepted");
    let rpc_a = set_a.bindings.iter().find(|b| b.launch_id == "rpc_node").cloned().expect("set A binds rpc_node");
    // A copy of the rpc node's bytes at its own path: the one the Build fixes.
    let execs = estate.root.join("execs");
    std::fs::create_dir_all(&execs).unwrap();
    let fixed = execs.join("rpc-node-fixed");
    std::fs::copy(&rpc_a.executable, &fixed).unwrap();
    let binding = Binding { launch_id: "rpc_node".into(), sha256: sha256_file(&fixed).unwrap(), executable: fixed.clone(), image: None };
    let shape = [("mesh1", 1, 2), ("mesh2", 1, 2)];
    let estate = born_at(estate, &shape, Duration::from_secs(90)).await.0;
    assert_eq!(exe_of(&estate, "mesh1.rpc.2").await, rpc_a.executable, "mesh1's admin launches set A's rpc node, the one its path names");
    assert_eq!(exe_of(&estate, "mesh2.rpc.2").await, rpc_a.executable, "mesh2's admin launches set A's rpc node, the one its path names");

    let nodes = objects(&estate).await;
    let with_binding = |node: &PathName, b: &Binding| -> Vec<Step> {
        replace_steps(node).into_iter().map(|s| match s {
            Step::Create { node, .. } => Step::Create { node, executable: Some(b.clone()) },
            other => other,
        }).collect()
    };
    for node in ["mesh1.rpc.1", "mesh2.rpc.1"] {
        let path: PathName = node.parse().unwrap();
        let got = frames(nodes.steps(with_binding(&path, &binding)).await.expect("the Build was accepted")).await;
        assert_eq!(got.last(), Some(&Frame::Complete), "the replace of {node}: {got:?}");
        let n = until_node_where(&estate, node, &format!("{node} ready"), SETTLE, ready).await;
        assert_eq!(exe_of(&estate, node).await, fixed, "{node}'s successor runs the executable its Build fixed, not its admin's: {n}");
    }

    // Recovery over the inherited data dir: the exited birth's row records the executable.
    let before = estate.node_value("mesh1.rpc.1").await;
    let dir = estate.data_dir("mesh1.rpc.1").await;
    estate.kill_node("mesh1.rpc.1").await;
    let after = until_new_birth(&estate, "mesh1.rpc.1", &before, SETTLE).await;
    assert_eq!((after["endpoint_id"].clone(), estate.data_dir("mesh1.rpc.1").await), (before["endpoint_id"].clone(), dir), "recovered over the inherited data dir");
    assert_eq!(exe_of(&estate, "mesh1.rpc.1").await, fixed, "the node recovered over its inherited dir runs the executable its row records, not its path's entry's");

    // Recovery over a newly allocated data dir: a held member is terminated and created afresh.
    let before = estate.node_value("mesh2.rpc.1").await;
    let dir = estate.data_dir("mesh2.rpc.1").await;
    assert_eq!(exact_runtime(&estate, "mesh2.rpc.1").await.apply(Fault::Stop).expect("the exact runtime is held").state_after, Some('T'));
    let after = until_new_birth(&estate, "mesh2.rpc.1", &before, SETTLE).await;
    assert!(after["endpoint_id"] != before["endpoint_id"] && estate.data_dir("mesh2.rpc.1").await != dir, "recovered over a newly allocated data dir: {after}");
    assert_eq!(exe_of(&estate, "mesh2.rpc.1").await, fixed, "the node recovered over a new dir runs the executable its row records, not its path's entry's");

    // The refusals, by name, nothing launched.
    let absent = Binding { executable: execs.join("not-there"), ..binding.clone() };
    let reason = failed_steps(&nodes, vec![Step::Create { node: "mesh1.rpc.7".parse().unwrap(), executable: Some(absent) }]).await;
    assert!(reason.contains("not-there") && reason.contains("cannot be read"), "a missing file is refused by name: {reason}");
    let mismatched = Binding { sha256: "0".repeat(64), ..binding.clone() };
    let reason = failed_steps(&nodes, vec![Step::Create { node: "mesh1.rpc.8".parse().unwrap(), executable: Some(mismatched) }]).await;
    assert!(reason.contains("rpc-node-fixed") && reason.contains("the binding expects"), "a hash mismatch is refused by name: {reason}");
    for node in ["mesh1.rpc.7", "mesh1.rpc.8"] {
        assert!(!estate.node_values().await.iter().any(|n| n["name"] == node), "nothing is launched at {node}");
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
    const NODE: &str = "mesh1.rpc.2";
    let before = estate.node_value(NODE).await;
    let old_id = s(&before["node_id"]);
    let rt = exact_runtime(&estate, NODE).await;
    let pid = rt.pid;
    assert_eq!(rt.apply(Fault::Stop).expect("the exact runtime is held").state_after, Some('T'));
    let after = until_new_birth(&estate, NODE, &before, SETTLE).await;
    let resumed_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let cont = std::process::Command::new("kill").args(["-CONT", &pid.to_string()]).status().map(|s| s.success()).unwrap_or(false);
    let floor = rdm_transport::topology::staleness_floor();
    let mut samples = Vec::new();
    let until = std::time::Instant::now() + floor * 2 + Duration::from_secs(30);
    while std::time::Instant::now() < until && alive(pid as u64) {
        let views = estate.node_values().await;
        let old_listed: Vec<String> = views.iter().filter(|n| s(&n["node_id"]) == old_id).map(|n| format!("{}={}", s(&n["name"]), s(&n["status"]))).collect();
        let live_at_path: Vec<String> = views.iter().filter(|n| n["name"] == NODE && live(n)).map(|n| s(&n["node_id"])).collect();
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
    let facts = json!({"node": NODE, "old_node_id": old_id, "new_node_id": after["node_id"], "continued": cont, "samples": samples, "spans_naming_the_old_birth": named_old, "refusals": refusals});
    estate.artifact("superseded.json", &facts);
    eprintln!("superseded birth facts: {}", serde_json::to_string_pretty(&facts).unwrap());
    let last = samples.last().cloned().unwrap_or(Value::Null);
    assert!(cont, "the held birth was continued: {facts}");
    assert!(samples.iter().all(|x| x["old_listed"] == json!([])), "no view lists the old node id again: {facts}");
    assert!(!alive(pid as u64), "the returning birth's process was terminated: {facts}");
    let reported = spans.iter().any(|sp| sp["name"] == "rdm.node_admin.node.update.via-returned-reported" && sp["attributes"]["node_id"] == old_id.as_str());
    assert!(reported, "its mesh-primary reported it to the fabric-primary: {facts}");
    let operation = format!("node.terminate:{NODE}@{}", s(&before["incarnation_id"]));
    let terminated = spans.iter().any(|sp| sp["name"] == "rdm.node_admin.deployment.update.via-step" && sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["outcome"] == "complete" && sp.to_string().contains(&operation));
    let receipted = spans.iter().any(|sp| sp.to_string().contains(&operation));
    assert!(terminated || receipted, "the fabric-primary ran TerminateRuntime under {operation}: {facts}");
    assert_eq!(last["live_at_path"], json!([s(&after["node_id"])]), "the path keeps one live birth, the re-created one: {facts}");
}

/// CONTRACT (Q207, #3211 red 3: "a FP/MP looks to see if something at that path.name exists currently;
/// if it does it should delete the caller"): a mesh2 node is held (SIGSTOP) until mesh2's primary
/// re-creates it as a new birth at its path, then continued: a caller its mesh-primary does not hold
/// (its Digest, or a declaration, refused). Mesh2's primary decides it from its atlas by its path.name:
/// another birth is there, so the caller is deleted; not the fabric-primary, it verifies at the root
/// with `fabric.atlas.get` first, which agrees; it reports the caller to the fabric-primary for
/// `node.terminate`. One `via-refused-caller` span per decision names the door, the
/// decision, the root's answer and the outcome. What must NOT happen: the caller refused and nothing
/// decided, the caller re-admitted over the re-created birth, its process left running, or the path
/// left with any live birth but the re-created one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_re_created_birth_that_comes_back_is_deleted_by_its_path_name_verified_at_the_root() {
    let (mut estate, _build) = two_mesh_estate("a_re_created_birth_that_comes_back_is_deleted_by_its_path_name_verified_at_the_root").await;
    const NODE: &str = "mesh2.rpc.1";
    let before = estate.node_value(NODE).await;
    let old_id = s(&before["node_id"]);
    let rt = exact_runtime(&estate, NODE).await;
    let pid = rt.pid;
    assert_eq!(rt.apply(Fault::Stop).expect("the exact runtime is held").state_after, Some('T'));
    let after = until_new_birth(&estate, NODE, &before, SETTLE).await;
    let cont = std::process::Command::new("kill").args(["-CONT", &pid.to_string()]).status().map(|s| s.success()).unwrap_or(false);
    let floor = rdm_transport::topology::staleness_floor();
    let decided = |spans: &[Value]| spans.iter().filter(|sp| sp["name"] == "rdm.node_admin.node.update.via-refused-caller" && sp["attributes"]["node_id"] == old_id.as_str()).map(|sp| sp["attributes"].clone()).collect::<Vec<Value>>();
    let until = std::time::Instant::now() + floor * 2 + Duration::from_secs(30);
    while std::time::Instant::now() < until && (alive(pid as u64) || decided(&estate.spans()).is_empty()) {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let live_at_path: Vec<String> = estate.node_values().await.iter().filter(|n| n["name"] == NODE && live(n)).map(|n| s(&n["node_id"])).collect();
    estate.stop().await;
    let spans = estate.spans();
    let decisions = decided(&spans);
    let facts = json!({"node": NODE, "old_node_id": old_id, "new_node_id": after["node_id"], "continued": cont, "decisions": decisions, "old_process_alive": alive(pid as u64), "live_at_path": live_at_path});
    estate.artifact("refused-caller.json", &facts);
    eprintln!("refused caller facts: {}", serde_json::to_string_pretty(&facts).unwrap());
    assert!(cont, "the held birth was continued: {facts}");
    assert!(!decisions.is_empty(), "mesh2's primary decided the caller by its path.name: {facts}");
    for d in &decisions {
        assert_eq!(d["node"], "mesh2.admin.1", "mesh2's primary decides its own mesh's caller: {facts}");
        assert_eq!(d["subject"], NODE, "decided at the caller's path.name: {facts}");
        assert!(d["door"] == "digest" || d["door"] == "declaration", "decided at a door a refused caller comes through, its Digest or its declaration: {facts}");
        assert_eq!(d["decision"], "delete-path-held", "another birth is at the path: the caller is deleted: {facts}");
        assert_eq!(d["root"], "agrees", "the fabric-primary's atlas agrees: {facts}");
    }
    assert!(decisions.iter().any(|d| d["outcome"] == "terminate-reported"), "the caller is reported to the fabric-primary for node.terminate: {facts}");
    assert!(!alive(pid as u64), "the caller's process is gone: {facts}");
    assert_eq!(facts["live_at_path"], json!([s(&after["node_id"])]), "the path keeps one live birth, the re-created one: {facts}");
}
