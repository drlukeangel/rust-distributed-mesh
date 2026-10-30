//! i143.e8.s2 acceptance (rafka-v2 #2780, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2780-process`, which exports `I143_ACCEPTANCE_DIR` (this
//! cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest and every process's spans land under it, feature `i143-2780`, test the cell's name).
//!
//! A real estate whose `node_admin` launch id is bound to the testkit's `faulted-node-admin`: the
//! public node-admin runtime with the testkit's stall decorators wired in through `Wiring`
//! (`rdm_test::admin_faults`). Every stall is armed, acknowledged, observed and
//! released through that admin's door; the Builds that trigger them are the ones a caller makes
//! (`node.create` / `node.delete` / `build.create` accepted by the fabric-primary ->
//! executor reconcile -> node operation -> deployment pipeline), driven through the typed objects over
//! Node RPC. The Build is read as the fabric's Build records hold it, from its store root (Q292,
//! `build_log`).

use crate::build_log;
use rdm_client::{LaunchKind as NodeKind, MeshDesired, NodeStatus, Nodes, PathName, WorkflowStream};
use rdm_test::typed::BuildSpec;
use rdm_admin::build_state::BuildState;
use rdm_admin::deployment::pipeline::{AdoptStep, CreateStep, DeleteStep};
use rdm_test::estate::{named, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

const CELL: &str = "failpoint_explorer_releases_build_and_hook_stalls_recovers";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2780".into(),
        subfeature: "failpoint-explorer".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: CELL.into(),
    }
}

fn path(name: &str) -> PathName {
    name.parse().unwrap_or_else(|e| panic!("{name} is not a path.name: {e:?}"))
}

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2780/process").join(CELL),
    }
}

// ---- the door: rdm_test::faults::Door ----------------------------------------------------

// ---- reading the estate -----------------------------------------------------------------------------

/// The steps of `operation` (at `attempt`, when given) the Build holds a `complete` receipt for, in order.
fn done_steps(build: &Value, operation: &str, attempt: Option<u64>) -> Vec<String> {
    build["steps"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|r| r["operation"] == operation && r["outcome"] == "complete" && attempt.is_none_or(|a| r["attempt"] == a))
        .map(|r| r["step"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// The attempts a Build that put the fabric back to pending ends at when its one run ran every
/// operation: that run, attempt 1, then the attempt claimed once the fabric changed to
/// ready, which completes it and runs no step (Q154; states.md "Resize and executable replacement").
/// The page names when the Build completes (the fabric ready again from its own change), not at which
/// attempt: a removal's change can be ready before its run is planned (the node it removes is not a
/// planned member, so every mesh is ready at once), and that run completes the Build at attempt 1.
/// Such a Build ends at 1 or 2 attempts, never more.
const RUN_THEN_CHANGE: u64 = 2;

/// How many receipts of each step `operation` holds at `attempt`.
fn step_counts(build: &Value, operation: &str, attempt: u64) -> BTreeMap<String, usize> {
    let mut m = BTreeMap::new();
    for r in build["steps"].as_array().into_iter().flatten().filter(|r| r["operation"] == operation && r["attempt"] == attempt) {
        *m.entry(r["step"].as_str().unwrap_or_default().to_string()).or_default() += 1;
    }
    m
}


/// What one cut did while it held, read before its release.
struct Hold {
    arm_ack: Value,
    held: Value,
    build_id: String,
    attempt: u64,
    during: Value,
}

fn row(h: Hold, group: &str, id: &str, spec: &Value, release_ack: Value, after: Value) -> Value {
    json!({
        "cut": id, "group": group, "spec": spec,
        "failure": {"arm_ack": h.arm_ack, "held_ack": h.held, "build_id": h.build_id, "attempt": h.attempt, "during_hold": h.during},
        "replay": {"release_ack": release_ack, "after_release": after},
    })
}

struct Run {
    estate: Estate,
    /// The directory holding every node's data dir and the faults' doors.
    root: PathBuf,
    doors: std::sync::Mutex<BTreeMap<String, Door>>,
    rows: std::sync::Mutex<Vec<Value>>,
    /// Every Build the run made, with the attempt count it must have at the end.
    builds: std::sync::Mutex<Vec<(String, u64)>>,
    /// What the exported spans must show for each cut, checked once the estate has stopped.
    span_checks: std::sync::Mutex<Vec<Value>>,
}

impl Run {
    /// Build `id` as the fabric's Build records hold it, the records every admin reads (Q292).
    async fn build(&self, id: &str) -> Value {
        build_log::build_value(&self.estate, id).await
    }

    /// the current build_id as the fabric primary holds it: `fabric.get` at the admin behind its door.
    async fn fabric_build_id(&self) -> String {
        let door = self.fabric_door().await;
        let at = path(&door.name);
        self.estate.fabric_view_at(&at).await.unwrap_or_else(|| panic!("{at} does not answer fabric.get for this estate's Fabric")).build_id.unwrap_or_default()
    }

    /// Runtimes of `node` the provider holds alive now (process provider: its `deployment.json`).
    fn runtimes(&self, node: &str) -> usize {
        self.processes(node).len()
    }

    /// The pids of `node`'s live runtimes.
    fn processes(&self, node: &str) -> Vec<u32> {
        self.estate.live_runtimes().into_iter().filter(|(dir, _)| dir.file_name().is_some_and(|n| n.to_string_lossy().starts_with(&format!("{node}-")))).map(|(_, pid)| pid).collect()
    }

    /// `node.create` of a `kind` node in `mesh`, accepted: the stream of the Build that runs it.
    async fn spawn(&self, mesh: &str, kind: NodeKind) -> WorkflowStream {
        self.estate.create_node(mesh, kind).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of a {kind:?} in {mesh}: {e}"))
    }

    /// `node.delete` of `node`, accepted: the stream of the Build that runs it.
    async fn delete(&self, node: &str) -> WorkflowStream {
        self.estate.nodes_obj().await.delete(&path(node)).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of {node}: {e}"))
    }

    /// The Build a workflow call opened or joined.
    fn id_of(stream: &WorkflowStream) -> String {
        stream.accepted().build_id.to_string()
    }

    /// The workflow's stream to its `Complete`.
    async fn complete(&self, stream: &mut WorkflowStream) {
        Estate::complete(stream).await;
    }

    /// `node` once the admin the estate asks lists it ready.
    async fn node_ready(&self, node: &str) -> Value {
        let ready = self.estate.until_node(node, &format!("{node} ready"), Duration::from_secs(60), |v| v.filter(|n| n.status == NodeStatus::Ready).cloned()).await;
        serde_json::to_value(ready).expect("a view serializes")
    }

    /// Wait until the admin the estate asks lists no `node`.
    async fn node_gone(&self, node: &str) {
        self.estate.until_node(node, &format!("{node} left the view"), Duration::from_secs(60), |v| v.is_none().then_some(())).await
    }

    /// Build `id` must end at `attempts` attempts (a later note for the same Build replaces this one).
    fn note_build(&self, id: &str, attempts: u64) {
        let mut b = self.builds.lock().unwrap();
        b.retain(|(i, _)| i != id);
        b.push((id.to_string(), attempts));
    }

    /// `node` as the admin behind `door` holds it.
    async fn node_from(&self, door: &Door, node: &str) -> Option<Value> {
        self.estate.node_values_at(&door.name).await.into_iter().find(|n| n["name"] == node)
    }

    /// Await the fact `holds` of Build `id` as the fabric's Build records hold it, the records the admin
    /// behind `door` reads (Q292). A Build they hold as failed fails the cell with the Build.
    async fn build_fact(&self, door: &Door, id: &str, fact: &str, holds: impl Fn(&rdm_admin::build_state::BuildProjection) -> bool) -> Value {
        let build = build_log::build_fact(&self.estate, id, fact, Duration::from_secs(120), |b| {
            assert!(b.state != BuildState::Failed, "Build {id} failed at {}: {b:#?}", door.name);
            holds(b)
        })
        .await
        .unwrap_or_else(|end| panic!("{end}"));
        serde_json::to_value(build).expect("a Build view serializes")
    }

    /// Wait until the admin behind `door` holds Build `id` complete (at any attempt).
    async fn build_complete(&self, door: &Door, id: &str) -> Value {
        self.build_fact(door, id, "complete", |b| b.state == BuildState::Complete).await
    }

    /// Wait until the admin behind `door` holds Build `id` complete at `attempt`.
    async fn attempt_complete(&self, door: &Door, id: &str, attempt: u64) -> Value {
        self.build_fact(door, id, &format!("complete at attempt {attempt}"), |b| b.state == BuildState::Complete && u64::from(b.attempt) == attempt).await
    }

    /// A healthy create of `kind` in `mesh1`: the node, ready, and its Build complete.
    async fn plain_create(&self, kind: NodeKind, node: &str) -> String {
        let mut stream = self.spawn("mesh1", kind).await;
        let id = Self::id_of(&stream);
        self.complete(&mut stream).await;
        self.node_ready(node).await;
        self.note_build(&id, RUN_THEN_CHANGE);
        id
    }

    async fn plain_delete(&self, node: &str) -> String {
        let mut stream = self.delete(node).await;
        let id = Self::id_of(&stream);
        self.complete(&mut stream).await;
        self.node_gone(node).await;
        self.note_build(&id, RUN_THEN_CHANGE);
        id
    }

    /// The cut `id` on `door` holds a call of Build `build_id`: read the Build while it holds.
    /// `expect_done` is the receipts of `operation` the Build must hold now (none: not asserted);
    /// `runtimes` the runtimes of a node alive now.
    #[allow(clippy::too_many_arguments)]
    async fn hold(&self, door: &Door, id: &str, arm_ack: Value, build_id: &str, operation: &str, expect_done: Option<Vec<String>>, runtimes: Option<(&str, usize)>) -> Hold {
        let held = door.wait_held(id).await;
        if !held["hit"]["build_id"].is_null() {
            assert_eq!(held["hit"]["build_id"], build_id, "`{id}` holds a call of the Build that triggered it: {held}");
        }
        let b = self.build(build_id).await;
        let attempt = b["attempt"].as_u64().unwrap();
        let done = done_steps(&b, operation, Some(attempt));
        if let Some(want) = &expect_done {
            assert_eq!(&done, want, "`{id}`: while the cut holds, the Build holds exactly the receipts before it: {b}");
        }
        assert!(b["state"] != "complete", "`{id}`: the Build is not complete while a call is held: {b}");
        let runtimes_now = runtimes.map(|(n, want)| {
            let have = self.runtimes(n);
            assert_eq!(have, want, "`{id}`: runtimes of {n} alive while the cut holds");
            have
        });
        let still = door.cut(id).await;
        assert_eq!(still["held"], true, "the injection stays active through the observation: {still}");
        let during = json!({
            "build_state": b["state"], "attempt": attempt, "operation": operation, "done_steps": done, "runtimes": runtimes_now,
            "fabric_build_id": self.fabric_build_id().await, "cut_still_held": still["held"], "seen_while_held": still["seen_while_held"],
        });
        Hold { arm_ack, held, build_id: build_id.to_string(), attempt, during }
    }

    /// The door of the admin named `name`, opened once.
    async fn door_of(&self, name: &str) -> Door {
        if let Some(d) = self.doors.lock().unwrap().get(name) {
            return d.clone();
        }
        let d = Door::at(&self.estate, name).await;
        self.doors.lock().unwrap().insert(name.to_string(), d.clone());
        d
    }

    /// The admin that executes the operations on `mesh`'s members: the mesh's admin primary.
    async fn member_door(&self, mesh: &str) -> Door {
        let nodes = self.estate.node_views().await;
        let primary = nodes.iter().find(|n| n.kind == NodeKind::NodeAdmin && n.mesh == mesh && n.is_primary).unwrap_or_else(|| panic!("{mesh} has an admin primary: {nodes:?}"));
        self.door_of(&primary.name.to_string()).await
    }

    /// The admin that executes everything else (mesh creation and deletion): the fabric primary.
    async fn fabric_door(&self) -> Door {
        let nodes = self.estate.node_views().await;
        let primary = nodes.iter().find(|n| n.is_fabric_primary).unwrap_or_else(|| panic!("the fabric has a primary: {nodes:?}"));
        self.door_of(&primary.name.to_string()).await
    }

    fn push(&self, v: Value) {
        self.rows.lock().unwrap().push(v);
    }

    fn span_check(&self, v: Value) {
        self.span_checks.lock().unwrap().push(v);
    }

    /// After a released create: the Build is complete once, each documented step has one receipt,
    /// one runtime serves the node and the current build_id names this Build.
    async fn after_create(&self, door: &Door, build_id: &str, node: &str, order: &[String]) -> Value {
        let b = self.build_complete(door, build_id).await;
        assert_eq!(b["state"], "complete", "{b}");
        assert!((1..=RUN_THEN_CHANGE).contains(&b["attempt"].as_u64().unwrap_or(0)), "no other attempt was opened than the run and, unless the fabric was ready from the Build's change before the run was planned, the Complete after it (RUN_THEN_CHANGE): {b}");
        assert!(b["steps"].as_array().into_iter().flatten().all(|r| r["attempt"] == 1), "the Complete after the fabric's change runs no step: {b}");
        let op = format!("node.create:{node}");
        assert_eq!(done_steps(&b, &op, Some(1)), order, "release completes every documented step once, in order: {b}");
        let counts = step_counts(&b, &op, 1);
        assert!(counts.values().all(|n| *n == 1) && counts.len() == order.len(), "no step receipt is recorded twice: {counts:?}");
        let view = self.node_ready(node).await;
        let ident = b["steps"].as_array().unwrap().iter().find(|r| r["operation"] == op.as_str() && r["step"] == "AllocateIdentity").map(|r| r["output"]["node_id"].clone()).unwrap();
        assert_eq!(ident, view["node_id"], "the node the Build decided is the node that serves: one birth");
        assert_eq!(self.runtimes(node), 1, "exactly one runtime serves {node}: release made no second birth");
        assert_eq!(self.fabric_build_id().await, build_id, "the current build_id names the accepted Build");
        json!({"build_state": b["state"], "attempt": b["attempt"], "step_receipts": counts, "node": {"status": view["status"], "node_id": view["node_id"], "incarnation_id": view["incarnation_id"]}, "runtimes": 1, "fabric_build_id": build_id})
    }

    /// After a released removal: complete once, each delete step once, the node gone, no runtime left.
    async fn after_delete(&self, door: &Door, build_id: &str, node: &str, order: &[String]) -> Value {
        let b = self.build_complete(door, build_id).await;
        assert_eq!(b["state"], "complete", "{b}");
        assert!((1..=RUN_THEN_CHANGE).contains(&b["attempt"].as_u64().unwrap_or(0)), "no other attempt was opened than the run and, unless the fabric was ready from the Build's change before the run was planned, the Complete after it (RUN_THEN_CHANGE): {b}");
        assert!(b["steps"].as_array().into_iter().flatten().all(|r| r["attempt"] == 1), "the Complete after the fabric's change runs no step: {b}");
        let op = format!("node.delete:{node}");
        assert_eq!(done_steps(&b, &op, Some(1)), order, "release completes every documented delete step once, in order: {b}");
        let counts = step_counts(&b, &op, 1);
        assert!(counts.values().all(|n| *n == 1) && counts.len() == order.len(), "no step receipt is recorded twice: {counts:?}");
        self.node_gone(node).await;
        assert_eq!(self.runtimes(node), 0, "no runtime of {node} is left");
        assert_eq!(self.fabric_build_id().await, build_id, "the current build_id names the accepted Build");
        json!({"build_state": b["state"], "attempt": b["attempt"], "step_receipts": counts, "node_in_view": false, "runtimes": 0, "fabric_build_id": build_id})
    }
}

fn index_of(order: &[String], step: &str) -> usize {
    order.iter().position(|s| s == step).unwrap_or_else(|| panic!("{step} is in the documented order {order:?}"))
}

/// A create step stalled at its receipt: the executor has done the step's work and not recorded it.
async fn create_step_cut(run: &Run, order: &[String], i: usize, node: &str) {
    let step = &order[i];
    let id = format!("create:{step}");
    let spec = json!({"kind": "receipt", "step": step, "operation": "node.create", "node": node});
    let op = format!("node.create:{node}");
    let door = run.member_door("mesh1").await;
    let ack = door.arm(&id, spec.clone()).await;
    let mut stream = run.spawn("mesh1", NodeKind::RpcNode).await;
    let build_id = Run::id_of(&stream);
    // The runtime exists once DeployRuntime's work is done.
    let runtimes = usize::from(i >= index_of(order, CreateStep::DeployRuntime.name()));
    let h = run.hold(&door, &id, ack, &build_id, &op, Some(order[..i].to_vec()), Some((node, runtimes))).await;
    let rel = door.release(&id).await;
    run.complete(&mut stream).await;
    let after = run.after_create(&door, &build_id, node, order).await;
    run.note_build(&build_id, RUN_THEN_CHANGE);
    run.span_check(json!({"kind": "step", "cut": id, "build_id": build_id, "node": node, "pipeline": "create", "attempt": 1, "steps": order, "held_step": step}));
    run.push(row(h, "create-step", &id, &spec, rel, after));
}

/// A removal step stalled at its receipt, on the node the matching create left.
async fn delete_step_cut(run: &Run, order: &[String], i: usize, node: &str) {
    let step = &order[i];
    let id = format!("delete:{step}");
    let spec = json!({"kind": "receipt", "step": step, "operation": "node.delete", "node": node});
    let op = format!("node.delete:{node}");
    let door = run.member_door("mesh1").await;
    let ack = door.arm(&id, spec.clone()).await;
    let mut stream = run.delete(node).await;
    let build_id = Run::id_of(&stream);
    // Before StopNode asked the birth to stop it must be alive; once TerminateRuntime's work is
    // done it is dead. Between, the birth may have exited by its own stop: not asserted.
    let (mark, terminate) = (index_of(order, DeleteStep::StopNode.name()), index_of(order, DeleteStep::TerminateRuntime.name()));
    let runtimes = if i < mark { Some((node, 1)) } else if i >= terminate { Some((node, 0)) } else { None };
    let h = run.hold(&door, &id, ack, &build_id, &op, Some(order[..i].to_vec()), runtimes).await;
    let rel = door.release(&id).await;
    run.complete(&mut stream).await;
    let after = run.after_delete(&door, &build_id, node, order).await;
    run.note_build(&build_id, RUN_THEN_CHANGE);
    run.span_check(json!({"kind": "step", "cut": id, "build_id": build_id, "node": node, "pipeline": "delete", "attempt": 1, "steps": order, "held_step": step}));
    run.push(row(h, "delete-step", &id, &spec, rel, after));
}

/// CONTRACT (#2780): every documented Build/deployment step,
/// accepted-Build persistence cut, runtime publication cut and Pending gate of a real
/// node-admin is held independently at its one point and released. For each cut: the testkit door
/// acknowledges the arming and then the active hold (naming the exact call that was held: Build,
/// attempt, operation, step); while it holds, the Build holds exactly the receipts before the cut
/// and is not complete, the later steps have not begun, and
/// the runtime count is the one the cut position implies; release alone completes the Build with
/// each step's receipt exactly once, one runtime (none after a delete), the current build_id on the
/// Build that was accepted and no other Build or
/// attempt. The exported spans put the held step's span before the hold and the next step's span
/// after the release, and each witness event (a lifecycle event, the Build's acceptance, a node's Ready)
/// after the release. What must NOT happen: a step receipt recorded twice, a second runtime born by
/// the release, a new unchanged-topology Build, or a Build that completes while its cut holds. No
/// application lifecycle hook is cut: application work enters only through Fabric and node hooks
/// (build.md §9, Q177).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failpoint_explorer_releases_build_and_hook_stalls_recovers() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let estate = Estate::bootstrap_on(owner(), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let root = estate.root.clone();
    let run = Run { estate, root, doors: Default::default(), rows: Default::default(), builds: Default::default(), span_checks: Default::default() };
    // A lone admin hears no other member and is cut off, so it executes nothing: the mesh holds two
    // admins before any cut is armed, and either may hold the roles.
    let shape = run.estate.reconcile_shape("fabric1", &[("mesh1", 2, 0)]).await.build_id.to_string();
    // The initial fabric refuses a node delete until it has first reached ready (states.md "The initial fabric before ready").
    run.estate.until_fabric_ready(Duration::from_secs(120)).await;
    run.estate.settles_to(&[("mesh1", 2, 0)], Duration::from_secs(60)).await;
    run.note_build(&shape, RUN_THEN_CHANGE);

    let create_order: Vec<String> = CreateStep::ORDER.iter().map(|s| s.name().to_string()).collect();
    let delete_order: Vec<String> = DeleteStep::ORDER.iter().map(|s| s.name().to_string()).collect();
    let node = "mesh1.rpc.1";

    // The healthy control: an unarmed create and removal of the same node. Its receipts are the
    // documented orders, taken from the product's own lists; every released cut must reproduce them.
    let control_create = run.plain_create(NodeKind::RpcNode, node).await;
    let b = run.build(&control_create).await;
    assert_eq!(done_steps(&b, &format!("node.create:{node}"), Some(1)), create_order, "the healthy create runs every documented step once, in order: {b}");
    let control_delete = run.plain_delete(node).await;
    let b = run.build(&control_delete).await;
    assert_eq!(done_steps(&b, &format!("node.delete:{node}"), Some(1)), delete_order, "the healthy removal runs every documented step once, in order: {b}");
    run.push(json!({"cut": "control", "group": "healthy-control", "create_build_id": control_create, "create_steps": create_order, "delete_build_id": control_delete, "delete_steps": delete_order}));

    // Every create step, each at its own receipt; the node it leaves is removed with the matching
    // removal step stalled (the removal's eleven), the remaining creates' nodes plainly.
    for i in 0..create_order.len() {
        create_step_cut(&run, &create_order, i, node).await;
        if i < delete_order.len() {
            delete_step_cut(&run, &delete_order, i, node).await;
        } else {
            run.plain_delete(node).await;
        }
    }

    // The removal's lifecycle events, each between its durable receipt and its publish.
    for event in ["deleting", "deleted"] {
        run.plain_create(NodeKind::RpcNode, node).await;
        event_cut(&run, &delete_order, event, node).await;
    }

    // The accepted Build is its own entity, written once and current from the moment it is durable
    // (R-BI3: "Each accepted Build is its own entity in the builds store, written once"; "The Fabric
    // row carries no pointer to the newest Build"): stalled at that one write, with the accepting
    // request unanswered.
    accept_cut(&run, &create_order, "accept:build-durable", json!({"kind": "accepted-build"}), "persist-accepted-build", node).await;
    run.plain_delete(node).await;

    // A mesh's first admin and the Pending gate.
    mesh_pending_cut(&run, &create_order).await;

    // A joining admin, held at its Ready gate. (It is not removed afterwards: a joined admin with the
    // lowest NodeId holds the fabric-primary role and would execute its own delete, which is not what this cut
    // explores.) `WaitForNodeReady` holds no receipt until it completes, so a Build waiting on a held
    // admin holds a receipt for every step before it.
    let pre_node_ready = index_of(&create_order, CreateStep::WaitForNodeReady.name());
    joining_admin_cut(&run, &create_order, "pending:provider-domain", json!({"kind": "provider-domain"}), "pending-gate", "mesh1.admin.3", pre_node_ready, "refusing to act on its locator").await;

    finish(run, &dir, &create_order, &delete_order).await;
}

/// Stop the estate, check the spans against what each cut did, and write `result.json`.
async fn finish(run: Run, dir: &Path, create_order: &[String], delete_order: &[String]) {
    let Run { mut estate, rows, builds, span_checks, .. } = run;
    // Every Build the run made still has exactly the attempts it was expected to have: no cut left
    // a second attempt or a new Build behind.
    let rows = rows.into_inner().unwrap();
    let builds = builds.into_inner().unwrap();
    for (id, attempts) in &builds {
        let b = build_log::build_value(&estate, id).await;
        let attempt = b["attempt"].as_u64().unwrap_or(0);
        let expected = if *attempts == RUN_THEN_CHANGE { (1..=RUN_THEN_CHANGE).contains(&attempt) } else { attempt == *attempts };
        assert!(b["state"] == "complete" && expected, "Build {id} ends complete at attempt {attempts}{}: {b}", if *attempts == RUN_THEN_CHANGE { " or 1 (RUN_THEN_CHANGE)" } else { "" });
    }
    let last = builds.last().map(|(id, _)| id.clone()).unwrap_or_default();
    assert_eq!(estate.accepted_build_id().await, last, "no Build was made after the last one the run triggered");
    estate.stop().await;
    let spans = estate.spans();
    let mut checked = Vec::new();
    for c in span_checks.into_inner().unwrap() {
        checked.push(check_spans(&spans, &c));
    }
    // Exhaustive by construction: every step of the product's documented orders has its cut row.
    let explored: std::collections::BTreeSet<String> = rows.iter().filter_map(|r| r["cut"].as_str().map(String::from)).collect();
    let mut documented: Vec<String> = create_order.iter().map(|s| format!("create:{s}")).collect();
    documented.extend(delete_order.iter().map(|s| format!("delete:{s}")));
    for want in &documented {
        assert!(explored.contains(want), "no cut row for the documented step `{want}`; explored: {explored:?}");
    }
    let unreachable = json!([
        {
            "cuts": AdoptStep::ORDER.iter().map(|s| format!("adopt-current:{}", s.name())).collect::<Vec<_>>(),
            "where": "crates/rafka-node-admin-core/src/deployment/pipeline.rs:252 (CurrentRuntimeAdoption::begin_in), :297 (publish), :306 (step); run at crates/rafka-node-admin-core/src/admin.rs:1812",
            "why": "the Day-0 adoption steps are synchronous closures that write runtime-adoption.json with std::fs::write and call no trait object; they run inside start() before any decorator is consulted. A stall needs fault code in the product."
        },
        {
            "cuts": ["day-0 first Build acceptance at boot"],
            "where": "crates/rafka-node-admin-core/src/admin.rs:1660-1661",
            "why": "runs inside the Day-0 admin's start(), before the estate harness can reach any door: crates/rdm-test-scenario/src/estate.rs `born` removes and recreates the estate root (so no boot cut can be placed before it) and blocks on the admin's control API (so a held Day-0 start stalls the harness itself)."
        }
    ]);
    let result = json!({
        "cell": CELL,
        "cuts_explored": rows.iter().filter_map(|r| r["cut"].as_str()).collect::<Vec<_>>(),
        "documented_orders": {"create": create_order, "delete": delete_order, "adopt_current": AdoptStep::ORDER.iter().map(|s| s.name()).collect::<Vec<_>>()},
        "rows": rows,
        "builds": builds,
        "span_checks": checked,
        "not_reachable_without_product_fault_code": unreachable,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

fn attr<'a>(s: &'a Value, k: &str) -> &'a str {
    s["attributes"][k].as_str().unwrap_or_default()
}

fn span_start(s: &Value) -> u64 {
    s["start_unix_nano"].as_u64().unwrap()
}

fn span_end(s: &Value) -> u64 {
    s["end_unix_nano"].as_u64().unwrap()
}

/// The exported spans of one cut: the held step's span ends before the hold begins, the next step's
/// span begins after the hold ends, every step ran once under its pipeline span.
fn check_spans(spans: &[Value], c: &Value) -> Value {
    let cut = c["cut"].as_str().unwrap();
    let hold_cut = c["hold_cut"].as_str().unwrap_or(cut);
    let holds: Vec<&Value> = named(spans, "rdm.testkit.fault.update.via-hold").into_iter().filter(|s| attr(s, "cut") == hold_cut).collect();
    assert!(!holds.is_empty(), "`{cut}`: a hold span was exported for the cut");
    // Every call a cut holds has its own hold span (a cut may hold several writers): the hold is
    // from the first one's start to the last one's end.
    let first = holds.iter().min_by_key(|s| span_start(s)).unwrap();
    let last = holds.iter().max_by_key(|s| span_end(s)).unwrap();
    let hold = &json!({"span_id": first["span_id"], "start_unix_nano": span_start(first), "end_unix_nano": span_end(last), "service": first["service"]});
    assert!(span_end(hold) > span_start(hold));
    let held_calls = holds.len();
    let kind = c["kind"].as_str().unwrap();
    let hold_ms = (span_end(hold) - span_start(hold)) / 1_000_000;
    match kind {
        "event" => {
            let name = c["span"].as_str().unwrap();
            let published: Vec<&Value> = named(spans, name).into_iter().filter(|s| attr(s, "build_id") == c["build_id"].as_str().unwrap() && attr(s, "attempt") == c["attempt"].as_u64().unwrap().to_string()).collect();
            assert!(!published.is_empty(), "`{cut}`: {name} was published for the Build");
            assert!(published.iter().all(|s| span_start(s) >= span_end(hold)), "`{cut}`: the event was published only after the release");
            return json!({"cut": cut, "hold_span_id": hold["span_id"], "held_calls": held_calls, "hold_ms": hold_ms, "published_span_id": published[0]["span_id"], "published_ms_after_release": (span_start(published[0]) - span_end(hold)) / 1_000_000});
        }
        "accepted" => {
            let written: Vec<&Value> = named(spans, "rdm.node_admin.build.create.via-accepted").into_iter().filter(|s| attr(s, "build_id") == c["build_id"].as_str().unwrap()).collect();
            assert_eq!(written.len(), 1, "`{cut}`: the Build was written once");
            // The acceptance spans the one write (R-BI3: the Build is current once durable), so the hold
            // before the write ends before the span does.
            assert!(span_end(written[0]) >= span_end(hold), "`{cut}`: the acceptance did not end before the release");
            return json!({"cut": cut, "hold_span_id": hold["span_id"], "held_calls": held_calls, "hold_ms": hold_ms, "accepted_span_id": written[0]["span_id"], "node": attr(written[0], "node"), "ended_ms_after_release": (span_end(written[0]) as i64 - span_end(hold) as i64) / 1_000_000});
        }
        "ready-after" => {
            let node = c["node"].as_str().unwrap();
            let ready: Vec<&Value> = named(spans, "rdm.mesh.node.update.via-ready").into_iter().filter(|s| attr(s, "node") == node).collect();
            assert_eq!(ready.len(), 1, "`{cut}`: {node} committed Ready once");
            assert!(span_start(ready[0]) >= span_end(hold), "`{cut}`: {node} reached Ready only after the release");
            let want = c["blocked_detail"].as_str().unwrap();
            if want.is_empty() {
                // Held inside its own start: it has not bound its control listener, published or evaluated a gate.
                return json!({"cut": cut, "hold_span_id": hold["span_id"], "held_calls": held_calls, "hold_ms": hold_ms, "ready_span_id": ready[0]["span_id"], "ready_ms_after_release": (span_start(ready[0]) - span_end(hold)) / 1_000_000});
            }
            let blocked: Vec<&Value> = named(spans, "rdm.node_admin.runtime.reject.via-not-ready")
                .into_iter()
                .filter(|s| attr(s, "node") == node && attr(s, "detail").contains(want))
                .collect();
            // The event that clears the blocker: for a mesh's first admin, the fabric primary's Pending
            // hand-off at its mesh; otherwise the release of the cut that holds the admin.
            let (cleared_by, cleared_at) = match c["handoff_mesh"].as_str() {
                Some(mesh) => {
                    let handoff: Vec<&Value> = named(spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|s| attr(s, "mesh") == mesh && attr(s, "outcome") == "applied").collect();
                    assert_eq!(handoff.len(), 1, "`{cut}`: one applied Pending hand-off for {mesh}: {handoff:#?}");
                    // The node's own apply of the Pending it was handed: the fabric primary's span records the
                    // call's answer once it returns, which a node quick to Ready can precede.
                    let applied: Vec<&Value> = named(spans, "rdm.node_admin.status.update.via-declaration").into_iter().filter(|s| attr(s, "node") == node && attr(s, "op") == "apply-mesh-state" && attr(s, "outcome") == "applied").collect();
                    assert!(!applied.is_empty(), "`{cut}`: {node} applied the Pending it was handed");
                    assert!(span_start(ready[0]) >= span_start(applied[0]), "`{cut}`: {node} reached Ready only after the Pending hand-off applied");
                    ("pending-handoff", span_start(applied[0]))
                }
                None => ("release", span_end(hold)),
            };
            let blocked: Vec<&Value> = blocked.into_iter().filter(|s| span_start(s) < cleared_at).collect();
            assert!(!blocked.is_empty(), "`{cut}`: {node} named its blocker (\"{want}\") before the {cleared_by}");
            return json!({"cut": cut, "hold_span_id": hold["span_id"], "held_calls": held_calls, "hold_ms": hold_ms, "ready_span_id": ready[0]["span_id"], "ready_ms_after_release": (span_start(ready[0]) - span_end(hold)) / 1_000_000, "blocked_span_id": blocked[0]["span_id"], "blocked_detail": attr(blocked[0], "detail")});
        }
        _ => {}
    }
    let (build_id, node, pipeline, attempt) = (c["build_id"].as_str().unwrap(), c["node"].as_str().unwrap(), c["pipeline"].as_str().unwrap(), c["attempt"].as_u64().unwrap());
    let pipelines: Vec<&Value> = named(spans, "rdm.node_admin.deployment.update.via-pipeline")
        .into_iter()
        .filter(|p| attr(p, "build_id") == build_id && attr(p, "node") == node && attr(p, "pipeline") == pipeline && attr(p, "attempt") == attempt.to_string())
        .collect();
    assert_eq!(pipelines.len(), 1, "`{cut}`: one {pipeline} pipeline span for {node} in {build_id}");
    let mut steps: Vec<&Value> = named(spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| s["parent_span_id"] == pipelines[0]["span_id"]).collect();
    steps.sort_by_key(|s| span_start(s));
    let names: Vec<&str> = steps.iter().map(|s| attr(s, "step")).collect();
    let want: Vec<&str> = c["steps"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
    assert_eq!(names, want, "`{cut}`: every documented step ran once under its pipeline span");
    assert!(steps.iter().all(|s| attr(s, "outcome") == "complete"), "`{cut}`: every step span completed");
    let at = want.iter().position(|w| *w == c["held_step"].as_str().unwrap()).unwrap();
    assert!(span_end(steps[at]) <= span_start(hold), "`{cut}`: the held step's work ended before the hold began");
    let next_after = steps.get(at + 1).map(|n| {
        assert!(span_start(n) >= span_end(hold), "`{cut}`: the next step began only after the release");
        (span_start(n) - span_end(hold)) / 1_000_000
    });
    json!({"cut": cut, "pipeline_span_id": pipelines[0]["span_id"], "trace_id": pipelines[0]["trace_id"], "held_step_span_id": steps[at]["span_id"], "hold_span_id": hold["span_id"], "hold_ms": (span_end(hold) - span_start(hold)) / 1_000_000, "next_step_began_ms_after_release": next_after})
}

// ---- events --------------------------------------------------------------------------------

/// The span the admin emits when it publishes `event` (lifecycle events doc, span provenance table).
fn event_span(event: &str) -> &'static str {
    match event {
        "deleting" => "rdm.node_admin.node.update.via-node-deleting",
        "deleted" => "rdm.node_admin.node.delete.via-node-deleted",
        other => panic!("{other} is not a lifecycle event"),
    }
}

/// A lifecycle event of a removal stalled between its durable receipt and its publish.
async fn event_cut(run: &Run, delete_order: &[String], event: &'static str, node: &str) {
    let id = format!("event:{event}");
    let spec = json!({"kind": "event", "event": event, "node": node});
    let op = format!("node.delete:{node}");
    let door = run.member_door("mesh1").await;
    let ack = door.arm(&id, spec.clone()).await;
    let mut stream = run.delete(node).await;
    let build_id = Run::id_of(&stream);
    // `deleting` follows the NodeDeleting receipt, with the birth untouched; `deleted` follows the
    // NodeDeleted receipt, with the runtime already terminal.
    let (receipts, runtimes) = match event {
        "deleting" => (index_of(delete_order, DeleteStep::NodeDeleting.name()) + 1, 1),
        "deleted" => (index_of(delete_order, DeleteStep::NodeDeleted.name()) + 1, 0),
        other => panic!("{other} is not a removal event"),
    };
    let mut h = run.hold(&door, &id, ack, &build_id, &op, Some(delete_order[..receipts].to_vec()), Some((node, runtimes))).await;
    let view = run.node_from(&door, node).await;
    // Observed, not asserted: the executor derives the open overlay from its own Build facts each
    // hierarchy checklist (`routable` goes false from the NodeDeleting receipt alone, within one checklist),
    // so the executor's view is not a witness of the publish. The witness is the event's own span,
    // started only after the release (checked on the exported spans).
    h.during["node_in_executor_view"] = view.map(|v| json!({"status": v["status"], "routable": v["routable"]})).unwrap_or(Value::Null);
    let rel = door.release(&id).await;
    run.complete(&mut stream).await;
    let after = run.after_delete(&door, &build_id, node, delete_order).await;
    run.note_build(&build_id, RUN_THEN_CHANGE);
    run.span_check(json!({"kind": "event", "cut": id, "span": event_span(event), "build_id": build_id, "attempt": 1}));
    run.span_check(json!({"kind": "step", "cut": format!("{id}#steps"), "hold_cut": id, "build_id": build_id, "node": node, "pipeline": "delete", "attempt": 1, "steps": delete_order, "held_step": delete_order[receipts - 1]}));
    run.push(row(h, "lifecycle-event", &id, &spec, rel, after));
}

// ---- accepted-Build persistence ------------------------------------------------------

/// The Build's one durable write is held: stalled at `spec`, with the accepting request unanswered.
/// Until the release the Build is held by no admin and is the current Build of none (R-BI3: "Each
/// accepted Build is its own entity in the builds store, written once"; it is current from the moment
/// it is durable), and no admin executes it; the release makes it durable, current and run. The
/// request is made from a task, because it does not return until release.
async fn accept_cut(run: &Run, create_order: &[String], id: &str, spec: Value, group: &str, node: &str) {
    let door = run.fabric_door().await;
    let previous = run.fabric_build_id().await;
    let ack = door.arm(id, spec.clone()).await;
    let request = {
        let nodes = Nodes::new(run.estate.carrier_at(&path(&door.name)).await);
        let (based_on, request) = run.estate.create_node_request("mesh1", NodeKind::RpcNode).await;
        tokio::spawn(async move { nodes.create_named(based_on, request).await })
    };
    let held = door.wait_held(id).await;
    let build_id = held["hit"]["build_id"].as_str().expect("the cut names the Build").to_string();
    // Does any admin hold, or execute, a Build whose write is held? Every admin's own log, read for a
    // few executor turns (a turn is 300 ms) while the cut holds.
    let mut held_by = Value::Null;
    for _ in 0..12 {
        assert_eq!(run.fabric_build_id().await, previous, "`{id}`: the Build is not durable, so it is not the current Build");
        for admin in run.estate.node_views().await.into_iter().filter(|n| n.kind == NodeKind::NodeAdmin) {
            // An admin whose log holds no fact of the Build ends the read `unknown build`.
            let Ok(facts) = build_log::log_read(&run.estate, &build_id).await else { continue };
            if let Some(seen) = build_log::projection(&facts, &build_id) {
                held_by = json!({"admin": admin.name.to_string(), "state": seen.state, "attempt": seen.attempt, "executor": seen.executor, "receipts": seen.steps.len()});
            }
        }
        if !held_by.is_null() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(!request.is_finished(), "`{id}`: the accepting request is unanswered while the cut holds");
    assert!(held_by.is_null(), "`{id}`: a Build whose one write is held is held by no admin's log; one holds it: {held_by}");
    let during = json!({
        "build_held_by_an_admin": false, "current_build_id": previous, "previous_build_id": previous, "accepting_request_answered": false,
    });
    let h = Hold { arm_ack: ack, held, build_id: build_id.clone(), attempt: 0, during };
    let rel = door.release(id).await;
    let mut accepted = request.await.unwrap().unwrap_or_else(|e| panic!("`{id}`: release did not answer the accepting request with the Build: {e}"));
    assert_eq!(Run::id_of(&accepted), build_id, "release answers the accepting request with the Build the cut named");
    assert_eq!(run.fabric_build_id().await, build_id, "`{id}`: the Build is durable once released, so it is the current Build");
    run.complete(&mut accepted).await;
    let exec = run.member_door("mesh1").await;
    let after = run.after_create(&exec, &build_id, node, create_order).await;
    run.note_build(&build_id, RUN_THEN_CHANGE);
    run.span_check(json!({"kind": "accepted", "cut": id, "build_id": build_id}));
    run.push(row(h, group, id, &spec, rel, after));
}

// ---- the Pending gate and the joining admin's Ready gate --------------------------------------------

/// A mesh's first admin is born and bound and published, and the fabric primary has not yet applied
/// its mesh's Pending: stalled before the `ApplyMeshPending` step. The admin must not reach Ready
/// until the release.
async fn mesh_pending_cut(run: &Run, create_order: &[String]) -> String {
    let admin = "mesh2.admin.1";
    let id = "pending:mesh-first-admin";
    let step = CreateStep::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata.name();
    let spec = json!({"kind": "receipt", "step": step, "operation": "node.create", "node": admin});
    let door = run.fabric_door().await;
    let ack = door.arm(id, spec.clone()).await;
    let stream = run.estate.submit(&BuildSpec::CreateMesh(MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1)]))).await;
    let build_id = stream.build_id().to_string();
    let at = index_of(create_order, step);
    let h = run.hold(&door, id, ack, &build_id, &format!("node.create:{admin}"), Some(create_order[..at].to_vec()), Some((admin, 1))).await;
    let rel = door.release(id).await;
    run.estate.follow(stream).await;
    let after = run.after_create(&door, &build_id, admin, create_order).await;
    let b = run.build(&build_id).await;
    let pending = b["steps"].as_array().unwrap().iter().find(|r| r["operation"] == format!("node.create:{admin}").as_str() && r["step"] == "ApplyMeshPending").map(|r| r["output"].clone()).unwrap();
    assert_eq!(pending["applied"], "Pending", "after the release the fabric primary applied the mesh's Pending at its first admin: {pending}");
    run.note_build(&build_id, RUN_THEN_CHANGE);
    run.span_check(json!({"kind": "step", "cut": id, "build_id": build_id, "node": admin, "pipeline": "create", "attempt": 1, "steps": create_order, "held_step": step}));
    run.span_check(json!({"kind": "ready-after", "cut": id, "node": admin, "blocked_detail": "Pending has not been applied", "handoff_mesh": "mesh2"}));
    run.push(row(h, "pending-gate", id, &spec, rel, after));
    build_id
}

/// A joining node-admin held at a boot cut (armed from `<root>/faults/<name>.boot.json`, before its
/// first line runs): the create Build waits on it, and it must not be Ready until the release.
#[allow(clippy::too_many_arguments)]
async fn joining_admin_cut(run: &Run, create_order: &[String], id: &str, boot: Value, group: &str, admin: &str, receipts_before: usize, blocked_detail: &str) {
    let faults = run.root.join("faults");
    std::fs::create_dir_all(&faults).unwrap();
    let mut spec = boot.clone();
    spec["id"] = json!(id);
    std::fs::write(faults.join(format!("{admin}.boot.json")), serde_json::to_vec(&json!([spec])).unwrap()).unwrap();
    let exec = run.fabric_door().await;
    let mut stream = run.spawn("mesh1", NodeKind::NodeAdmin).await;
    let build_id = Run::id_of(&stream);
    let door = Door::at(&run.estate, admin).await;
    let ack = json!({"armed": id, "node": admin, "spec": boot, "armed_by": "boot file before the birth's first line"});
    let held = door.wait_held(id).await;
    // The pipeline goes on while the admin boots, and stops at the step that needs the admin Ready,
    // which only the release lets complete: it must settle at `receipts_before` receipts.
    let op = format!("node.create:{admin}");
    let want = create_order[..receipts_before].to_vec();
    let receipts = |b: &rdm_admin::build_state::BuildProjection| done_steps(&serde_json::to_value(b).expect("a Build view serializes"), &op, Some(1));
    let b = run.build_fact(&exec, &build_id, &format!("`{id}`: the Build holds a receipt for every step before WaitForNodeReady (through {}) and waits on the held admin", want.last().unwrap()), |b| receipts(b) == want).await;
    let done = done_steps(&b, &op, Some(1));
    assert!(done.len() <= receipts_before && done == create_order[..done.len()].to_vec(), "`{id}`: the Build holds a prefix of the documented order that stops before WaitForNodeReady: {done:?}");
    assert!(b["state"] != "complete", "`{id}`: the Build is not complete while the joining admin is held: {b}");
    let view = run.estate.node_value_opt(admin).await;
    if let Some(v) = &view {
        assert_ne!(v["status"], "ready", "`{id}`: the held admin is not Ready: {v}");
    }
    let during = json!({"build_state": b["state"], "attempt": 1, "operation": op, "done_steps": done, "admin_in_view": view.map(|v| json!({"status": v["status"]})), "cut_still_held": door.cut(id).await["held"]});
    let h = Hold { arm_ack: ack, held, build_id: build_id.clone(), attempt: 1, during };
    let rel = door.release(id).await;
    run.complete(&mut stream).await;
    let after = run.after_create(&exec, &build_id, admin, create_order).await;
    run.note_build(&build_id, RUN_THEN_CHANGE);
    run.span_check(json!({"kind": "ready-after", "cut": id, "node": admin, "blocked_detail": blocked_detail}));
    run.push(row(h, group, id, &boot, rel, after));
}
