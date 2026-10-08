//! i143.e8.s2 acceptance (rafka-v2 #2780, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2780-process`, which exports `I143_ACCEPTANCE_DIR` (this
//! cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest and every process's spans land under it, feature `i143-2780`, test the cell's name).
//!
//! A real estate whose `node_admin` launch id is bound to the testkit's `faulted-node-admin`: the
//! public node-admin runtime with the testkit's stall decorators wired in through `Wiring`
//! (`rafka_node_rpc_testkit::admin_faults`). Every stall is armed, acknowledged, observed and
//! released through that admin's door; the Builds that trigger them are the ones an operator makes
//! (REST accept -> executor reconcile -> node operation -> deployment pipeline).

use rafka_node_admin_core::deployment::pipeline::{AdoptStep, CreateStep, RetireStep};
use rafka_node_admin_core::lifecycle::HookPhase;
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
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

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2780/process").join(CELL),
    }
}

// ---- the door: rafka_test_scenario::faults::Door ----------------------------------------------------

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
    /// Build `id` as the admin behind `door` holds it: an executing admin's own record of what it wrote.
    async fn build(&self, door: &Door, id: &str) -> Value {
        let (status, b) = self.estate.http_get(&door.api, &format!("/api/builds?id={id}")).await;
        assert_eq!(status, 200, "GET {}/api/builds?id={id}: {b}", door.api);
        b
    }

    /// `Fabric.build_id` as the fabric primary holds it.
    async fn fabric_build_id(&self) -> String {
        let door = self.fabric_door().await;
        self.estate.http_get(&door.api, "/api/fabric").await.1["build_id"].as_str().unwrap_or_default().to_string()
    }

    /// Runtimes of `node` the provider holds alive now (process provider: its `deployment.json`).
    fn runtimes(&self, node: &str) -> usize {
        self.estate.live_runtimes().iter().filter(|(dir, _)| dir.file_name().is_some_and(|n| n.to_string_lossy().starts_with(&format!("{node}-")))).count()
    }

    async fn spawn(&self, mesh: &str, kind: &str) -> String {
        let (status, a) = self.estate.post("/api/nodes/spawn", &json!({"mesh": mesh, "kind": kind})).await;
        assert_eq!(status, 202, "spawn {mesh} {kind}: {a}");
        a["build_id"].as_str().unwrap().to_string()
    }

    async fn delete(&self, node: &str) -> String {
        let (status, a) = self.estate.delete(&format!("/api/nodes/{node}")).await;
        assert_eq!(status, 202, "delete {node}: {a}");
        a["build_id"].as_str().unwrap().to_string()
    }

    async fn complete(&self, build_id: &str) -> Value {
        self.estate.await_build(build_id, Duration::from_secs(120)).await
    }

    async fn node_ready(&self, node: &str) -> Value {
        wait_for(&format!("{node} ready for traffic"), Duration::from_secs(60), || async { self.estate.node_opt(node).await.filter(|n| n["status"] == "ready-for-traffic") }).await
    }

    async fn node_gone(&self, node: &str) {
        wait_for(&format!("{node} left the view"), Duration::from_secs(60), || async { self.estate.node_opt(node).await.is_none().then_some(()) }).await
    }

    /// Build `id` must end at `attempts` attempts (a later note for the same Build replaces this one).
    fn note_build(&self, id: &str, attempts: u64) {
        let mut b = self.builds.lock().unwrap();
        b.retain(|(i, _)| i != id);
        b.push((id.to_string(), attempts));
    }

    /// `node` as the admin behind `door` holds it.
    async fn node_from(&self, door: &Door, node: &str) -> Option<Value> {
        self.estate.nodes_at(&door.api).await.into_iter().find(|n| n["name"] == node)
    }

    /// Wait until the admin behind `door` holds Build `id` complete at `attempt`.
    async fn attempt_complete(&self, door: &Door, id: &str, attempt: u64) -> Value {
        wait_for(&format!("Build {id} attempt {attempt} complete"), Duration::from_secs(120), || async {
            let (_, b) = self.estate.http_get(&door.api, &format!("/api/builds?id={id}")).await;
            assert!(b["state"] != "failed", "Build {id} failed: {b:#}");
            (b["state"] == "complete" && b["attempt"] == attempt).then_some(b)
        })
        .await
    }

    /// A healthy create of `kind` in `mesh1`: the node, ready, and its Build complete.
    async fn plain_create(&self, kind: &str, node: &str) -> String {
        let id = self.spawn("mesh1", kind).await;
        self.complete(&id).await;
        self.node_ready(node).await;
        self.note_build(&id, 1);
        id
    }

    async fn plain_delete(&self, node: &str) -> String {
        let id = self.delete(node).await;
        self.complete(&id).await;
        self.node_gone(node).await;
        self.note_build(&id, 1);
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
        let b = self.build(door, build_id).await;
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
        let api = self.estate.nodes().await.iter().find(|n| n["name"] == name).and_then(|n| n["admin_api_base"].as_str().map(String::from)).unwrap_or_else(|| panic!("{name} advertises its control API"));
        let d = Door::open(&self.root, name, &api).await;
        self.doors.lock().unwrap().insert(name.to_string(), d.clone());
        d
    }

    /// The admin that executes the operations on `mesh`'s members: the mesh's admin primary.
    async fn member_door(&self, mesh: &str) -> Door {
        let nodes = self.estate.nodes().await;
        let primary = nodes.iter().find(|n| n["kind"] == "node_admin" && n["mesh"] == mesh && n["is_primary"] == true).unwrap_or_else(|| panic!("{mesh} has an admin primary: {nodes:?}"));
        self.door_of(primary["name"].as_str().unwrap()).await
    }

    /// The admin that executes everything else (admin cohorts, mesh creation and retirement): the fabric primary.
    async fn fabric_door(&self) -> Door {
        let nodes = self.estate.nodes().await;
        let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).unwrap_or_else(|| panic!("the fabric has a primary: {nodes:?}"));
        self.door_of(primary["name"].as_str().unwrap()).await
    }

    /// The admin that executes `retire-mesh:<mesh>`: the fabric primary when it sits outside `mesh`,
    /// else the lowest-NodeId ready admin primary of another mesh (executor.rs `executor_for`).
    async fn retire_mesh_door(&self, mesh: &str) -> Door {
        let nodes = self.estate.nodes().await;
        let fabric = nodes.iter().find(|n| n["is_fabric_primary"] == true).unwrap_or_else(|| panic!("the fabric has a primary: {nodes:?}"));
        let name = if fabric["mesh"] != mesh {
            fabric["name"].clone()
        } else {
            nodes
                .iter()
                .filter(|n| n["kind"] == "node_admin" && n["is_primary"] == true && n["mesh"] != mesh && n["status"] == "ready-for-traffic")
                .min_by_key(|n| n["node_id"].as_str().unwrap_or_default().to_string())
                .unwrap_or_else(|| panic!("an admin primary outside {mesh} exists: {nodes:?}"))["name"]
                .clone()
        };
        self.door_of(name.as_str().unwrap()).await
    }

    fn push(&self, v: Value) {
        self.rows.lock().unwrap().push(v);
    }

    fn span_check(&self, v: Value) {
        self.span_checks.lock().unwrap().push(v);
    }

    /// After a released create: the Build is complete once, each documented step has one receipt,
    /// one runtime serves the node and `Fabric.build_id` names this Build.
    async fn after_create(&self, door: &Door, build_id: &str, node: &str, order: &[String]) -> Value {
        let b = self.build(door, build_id).await;
        assert_eq!(b["state"], "complete", "{b}");
        assert_eq!(b["attempt"], 1, "no other attempt was opened: {b}");
        let op = format!("create-node:{node}");
        assert_eq!(done_steps(&b, &op, Some(1)), order, "release completes every documented step once, in order: {b}");
        let counts = step_counts(&b, &op, 1);
        assert!(counts.values().all(|n| *n == 1) && counts.len() == order.len(), "no step receipt is recorded twice: {counts:?}");
        let view = self.node_ready(node).await;
        let ident = b["steps"].as_array().unwrap().iter().find(|r| r["operation"] == op.as_str() && r["step"] == "AllocateIdentity").map(|r| r["output"]["node_id"].clone()).unwrap();
        assert_eq!(ident, view["node_id"], "the node the Build decided is the node that serves: one birth");
        assert_eq!(self.runtimes(node), 1, "exactly one runtime serves {node}: release made no second birth");
        assert_eq!(self.fabric_build_id().await, build_id, "Fabric.build_id names the accepted Build");
        json!({"build_state": b["state"], "attempt": 1, "step_receipts": counts, "node": {"status": view["status"], "node_id": view["node_id"], "incarnation_id": view["incarnation_id"]}, "runtimes": 1, "fabric_build_id": build_id})
    }

    /// After a released removal: complete once, each retire step once, the node gone, no runtime left.
    async fn after_retire(&self, door: &Door, build_id: &str, node: &str, order: &[String]) -> Value {
        let b = self.build(door, build_id).await;
        assert_eq!(b["state"], "complete", "{b}");
        assert_eq!(b["attempt"], 1, "no other attempt was opened: {b}");
        let op = format!("retire-node:{node}");
        assert_eq!(done_steps(&b, &op, Some(1)), order, "release completes every documented retire step once, in order: {b}");
        let counts = step_counts(&b, &op, 1);
        assert!(counts.values().all(|n| *n == 1) && counts.len() == order.len(), "no step receipt is recorded twice: {counts:?}");
        self.node_gone(node).await;
        assert_eq!(self.runtimes(node), 0, "no runtime of {node} is left");
        assert_eq!(self.fabric_build_id().await, build_id, "Fabric.build_id names the accepted Build");
        json!({"build_state": b["state"], "attempt": 1, "step_receipts": counts, "node_in_view": false, "runtimes": 0, "fabric_build_id": build_id})
    }
}

fn index_of(order: &[String], step: &str) -> usize {
    order.iter().position(|s| s == step).unwrap_or_else(|| panic!("{step} is in the documented order {order:?}"))
}

/// A create step stalled at its receipt: the executor has done the step's work and not recorded it.
async fn create_step_cut(run: &Run, order: &[String], i: usize, node: &str) {
    let step = &order[i];
    let id = format!("create:{step}");
    let spec = json!({"kind": "receipt", "step": step, "operation": "create-node", "node": node});
    let op = format!("create-node:{node}");
    let door = run.member_door("mesh1").await;
    let ack = door.arm(&id, spec.clone()).await;
    let build_id = run.spawn("mesh1", "rpc_node").await;
    // The runtime exists once DeployRuntime's work is done.
    let runtimes = usize::from(i >= index_of(order, CreateStep::DeployRuntime.name()));
    let h = run.hold(&door, &id, ack, &build_id, &op, Some(order[..i].to_vec()), Some((node, runtimes))).await;
    let rel = door.release(&id).await;
    run.complete(&build_id).await;
    let after = run.after_create(&door, &build_id, node, order).await;
    run.note_build(&build_id, 1);
    run.span_check(json!({"kind": "step", "cut": id, "build_id": build_id, "node": node, "pipeline": "create", "attempt": 1, "steps": order, "held_step": step}));
    run.push(row(h, "create-step", &id, &spec, rel, after));
}

/// A removal step stalled at its receipt, on the node the matching create left.
async fn retire_step_cut(run: &Run, order: &[String], i: usize, node: &str) {
    let step = &order[i];
    let id = format!("retire:{step}");
    let spec = json!({"kind": "receipt", "step": step, "operation": "retire-node", "node": node});
    let op = format!("retire-node:{node}");
    let door = run.member_door("mesh1").await;
    let ack = door.arm(&id, spec.clone()).await;
    let build_id = run.delete(node).await;
    // Before MarkDraining asked the birth to drain it must be alive; once TerminateRuntime's work is
    // done it is dead. Between, the birth may have exited by its own drain: not asserted.
    let (mark, terminate) = (index_of(order, RetireStep::MarkDraining.name()), index_of(order, RetireStep::TerminateRuntime.name()));
    let runtimes = if i < mark { Some((node, 1)) } else if i >= terminate { Some((node, 0)) } else { None };
    let h = run.hold(&door, &id, ack, &build_id, &op, Some(order[..i].to_vec()), runtimes).await;
    let rel = door.release(&id).await;
    run.complete(&build_id).await;
    let after = run.after_retire(&door, &build_id, node, order).await;
    run.note_build(&build_id, 1);
    run.span_check(json!({"kind": "step", "cut": id, "build_id": build_id, "node": node, "pipeline": "retire", "attempt": 1, "steps": order, "held_step": step}));
    run.push(row(h, "retire-step", &id, &spec, rel, after));
}

/// CONTRACT (#2780): every documented Build/deployment step, lifecycle hook, accepted-Build
/// persist-before-pointer cut, runtime publication/hydration cut and Pending gate of a real
/// node-admin is held independently at its one point and released. For each cut: the testkit door
/// acknowledges the arming and then the active hold (naming the exact call that parked: Build,
/// attempt, operation, step); while it holds, the Build holds exactly the receipts before the cut
/// and is not complete, the later steps have not begun, no admin executes an unaccepted Build, and
/// the runtime count is the one the cut position implies; release alone completes the Build with
/// each step's receipt exactly once, one runtime (none after a retire), `Fabric.build_id` on the
/// Build that was accepted (a restart on the same Build, one attempt later) and no other Build or
/// attempt. The exported spans put the held step's span before the hold and the next step's span
/// after the release, and each witness event (a lifecycle event, the pointer move, a node's Ready)
/// after the release. What must NOT happen: a step receipt recorded twice, a second runtime born by
/// the release, a new unchanged-topology Build, a Build that completes while its cut holds, a
/// Build executed before `Fabric.build_id` names it, or a hook skipped for a re-born node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failpoint_explorer_releases_build_and_hook_stalls_recovers() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let estate = Estate::bootstrap_external(owner(), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let root = estate.root.clone();
    let run = Run { estate, root, doors: Default::default(), rows: Default::default(), builds: Default::default(), span_checks: Default::default() };
    // A lone admin hears no other member and is cut off, so it executes nothing: the mesh holds two
    // admins before any cut is armed, and either may hold the seats.
    let (status, a) = run.estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    let shape = a["build_id"].as_str().unwrap().to_string();
    run.complete(&shape).await;
    run.estate.settled(&["mesh1.admin.1", "mesh1.admin.2"].iter().map(|n| n.to_string()).collect(), Duration::from_secs(60)).await;
    run.note_build(&shape, 1);

    let create_order: Vec<String> = CreateStep::ORDER.iter().map(|s| s.name().to_string()).collect();
    let retire_order: Vec<String> = RetireStep::ORDER.iter().map(|s| s.name().to_string()).collect();
    let node = "mesh1.rpc.1";

    // The healthy control: an unarmed create and removal of the same node. Its receipts are the
    // documented orders, taken from the product's own lists; every released cut must reproduce them.
    let control_create = run.plain_create("rpc_node", node).await;
    let b = run.build(&run.member_door("mesh1").await, &control_create).await;
    assert_eq!(done_steps(&b, &format!("create-node:{node}"), Some(1)), create_order, "the healthy create runs every documented step once, in order: {b}");
    let control_delete = run.plain_delete(node).await;
    let b = run.build(&run.member_door("mesh1").await, &control_delete).await;
    assert_eq!(done_steps(&b, &format!("retire-node:{node}"), Some(1)), retire_order, "the healthy removal runs every documented step once, in order: {b}");
    run.push(json!({"cut": "control", "group": "healthy-control", "create_build_id": control_create, "create_steps": create_order, "retire_build_id": control_delete, "retire_steps": retire_order}));

    // Every create step, each at its own receipt; the node it leaves is removed with the matching
    // removal step stalled (the removal's eleven), the remaining creates' nodes plainly.
    for i in 0..create_order.len() {
        create_step_cut(&run, &create_order, i, node).await;
        if i < retire_order.len() {
            retire_step_cut(&run, &retire_order, i, node).await;
        } else {
            run.plain_delete(node).await;
        }
    }

    // The removal's lifecycle events, each between its durable receipt and its publish.
    for event in ["deleting", "deleted"] {
        run.plain_create("rpc_node", node).await;
        event_cut(&run, &retire_order, event, node).await;
    }

    // A restart is an attempt of the accepted Build: stalled at its pre-event receipt, then at the
    // pre-event's publish.
    run.plain_create("rpc_node", node).await;
    let receipt = json!({"kind": "receipt", "step": RetireStep::NodeRestarting.name(), "operation": "retire-node", "node": node});
    restart_cut(&run, &create_order, &retire_order, "restart:NodeRestarting", receipt, "restart-step", 0, node, 2).await;
    let event = json!({"kind": "event", "event": "restarting", "node": node});
    restart_cut(&run, &create_order, &retire_order, "event:restarting", event, "lifecycle-event", 1, node, 3).await;
    run.plain_delete(node).await;

    // Each reachable phase of the Node Pending -> ReadyForTraffic transition's hooks.
    for phase in [HookPhase::BeforeEligibility, HookPhase::AfterEligibilityBeforeCommit, HookPhase::AfterTransition] {
        hook_cut(&run, &create_order, phase.as_str(), node).await;
        run.plain_delete(node).await;
    }

    // The accepted Build is durable before Fabric.build_id names it: stalled between the two, and
    // inside the pointer's own write.
    accept_cut(&run, &create_order, "accept:build-durable-before-pointer", json!({"kind": "accepted-build"}), "persist-before-pointer", node).await;
    run.plain_delete(node).await;
    accept_cut(&run, &create_order, "pointer:fabric-record-write", json!({"kind": "pointer-write", "moves_pointer": true}), "persist-before-pointer", node).await;
    run.plain_delete(node).await;

    // A mesh's first admin and the Pending gate; the whole-mesh retire's ObserveDeparture.
    mesh_pending_cut(&run, &create_order).await;
    observe_departure_cut(&run).await;

    // A joining admin, held at its hydration of the Fabric record and at its Ready gate.
    // (Neither is removed afterwards: a joined admin with the lowest NodeId holds the fabric seat and
    // would execute its own retire, which is not what these cuts explore.)
    joining_admin_cut(&run, &create_order, "hydration:fabric-record-write", json!({"kind": "pointer-write", "moves_pointer": true}), "hydration", "mesh1.admin.3", 12, false, "").await;
    joining_admin_cut(&run, &create_order, "pending:provider-domain", json!({"kind": "provider-domain"}), "pending-gate", "mesh1.admin.4", 12, true, "refusing to act on its locator").await;

    finish(run, &dir, &create_order, &retire_order).await;
}

/// Stop the estate, check the spans against what each cut did, and write `result.json`.
async fn finish(run: Run, dir: &Path, create_order: &[String], retire_order: &[String]) {
    let Run { mut estate, rows, builds, span_checks, .. } = run;
    // Every Build the run made still has exactly the attempts it was expected to have: no cut left
    // a second attempt or a new Build behind.
    let rows = rows.into_inner().unwrap();
    let builds = builds.into_inner().unwrap();
    for (id, attempts) in &builds {
        let b = estate.get(&format!("/api/builds?id={id}")).await.1;
        assert_eq!((b["state"].as_str(), b["attempt"].as_u64()), (Some("complete"), Some(*attempts)), "Build {id} ends complete at attempt {attempts}: {b}");
    }
    let last = builds.last().map(|(id, _)| id.clone()).unwrap_or_default();
    assert_eq!(estate.get("/api/fabric").await.1["build_id"], last.as_str(), "no Build was made after the last one the run triggered");
    estate.stop().await;
    let spans = estate.spans();
    let mut checked = Vec::new();
    for c in span_checks.into_inner().unwrap() {
        checked.push(check_spans(&spans, &c));
    }
    // Exhaustive by construction: every step of the product's documented orders has its cut row.
    let explored: std::collections::BTreeSet<String> = rows.iter().filter_map(|r| r["cut"].as_str().map(String::from)).collect();
    let mut documented: Vec<String> = create_order.iter().map(|s| format!("create:{s}")).collect();
    documented.extend(retire_order.iter().map(|s| format!("retire:{s}")));
    documented.push(format!("restart:{}", RetireStep::NodeRestarting.name()));
    documented.push("retire-mesh:ObserveDeparture".into());
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
            "cuts": ["hook:before_drain", "hook:after_drain"],
            "where": "crates/rafka-node-admin-core/src/lifecycle.rs:354 (drain phases run only when a transition enters Draining); the only Transition the admin builds is crates/rafka-node-admin-core/src/admin.rs:923-929 (Node Pending -> ReadyForTraffic)",
            "why": "no admin path runs a transition into Draining, so no hook of the drain phases can be reached; building one would be a new transition, not a stall."
        },
        {
            "cuts": ["day-0 first Build acceptance and first pointer at boot"],
            "where": "crates/rafka-node-admin-core/src/admin.rs:1660-1661",
            "why": "runs inside the Day-0 admin's start(), before the estate harness can reach any door: crates/rafka-test-scenario/src/estate.rs `born` removes and recreates the estate root (so no boot cut can be placed before it) and blocks on the admin's control API (so a held Day-0 start stalls the harness itself)."
        }
    ]);
    let result = json!({
        "cell": CELL,
        "cuts_explored": rows.iter().filter_map(|r| r["cut"].as_str()).collect::<Vec<_>>(),
        "documented_orders": {"create": create_order, "retire": retire_order, "adopt_current": AdoptStep::ORDER.iter().map(|s| s.name()).collect::<Vec<_>>()},
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
    // Every call a cut parks has its own hold span (a pointer cut may park two writers): the hold is
    // from the first one's start to the last one's end.
    let first = holds.iter().min_by_key(|s| span_start(s)).unwrap();
    let last = holds.iter().max_by_key(|s| span_end(s)).unwrap();
    let hold = &json!({"span_id": first["span_id"], "start_unix_nano": span_start(first), "end_unix_nano": span_end(last), "service": first["service"]});
    assert!(span_end(hold) > span_start(hold));
    let parked_calls = holds.len();
    let kind = c["kind"].as_str().unwrap();
    let hold_ms = (span_end(hold) - span_start(hold)) / 1_000_000;
    match kind {
        "event" => {
            let name = c["span"].as_str().unwrap();
            let published: Vec<&Value> = named(spans, name).into_iter().filter(|s| attr(s, "build_id") == c["build_id"].as_str().unwrap() && attr(s, "attempt") == c["attempt"].as_u64().unwrap().to_string()).collect();
            assert!(!published.is_empty(), "`{cut}`: {name} was published for the Build");
            assert!(published.iter().all(|s| span_start(s) >= span_end(hold)), "`{cut}`: the event was published only after the release");
            return json!({"cut": cut, "hold_span_id": hold["span_id"], "parked_calls": parked_calls, "hold_ms": hold_ms, "published_span_id": published[0]["span_id"], "published_ms_after_release": (span_start(published[0]) - span_end(hold)) / 1_000_000});
        }
        "hook" => {
            let hooks: Vec<&Value> = named(spans, "rdm.node_admin.lifecycle_hook.update.via-transition")
                .into_iter()
                .filter(|s| attr(s, "hook_id") == c["hook_id"].as_str().unwrap() && attr(s, "transition_id").contains(&format!(":{}:pending->readyfortraffic@", c["node"].as_str().unwrap())) && span_start(s) <= span_start(hold) && span_end(s) >= span_end(hold))
                .collect();
            assert_eq!(hooks.len(), 1, "`{cut}`: one hook span covers the hold: {hooks:#?}");
            assert_eq!(attr(hooks[0], "outcome"), "complete");
            return json!({"cut": cut, "hold_span_id": hold["span_id"], "hook_span_id": hooks[0]["span_id"], "hook_phase": attr(hooks[0], "phase"), "hold_ms": hold_ms});
        }
        "pointer" => {
            let moved: Vec<&Value> = named(spans, "rdm.node_admin.fabric.update.via-build-accepted").into_iter().filter(|s| attr(s, "build_id") == c["build_id"].as_str().unwrap()).collect();
            assert!(!moved.is_empty(), "`{cut}`: Fabric.build_id moved to the Build");
            // The accepting admin's move spans its own write (so it began inside the hold and ends after
            // the release); every other admin moves its pointer only on hearing the record, which the
            // accepting admin broadcasts after its write.
            assert!(moved.iter().all(|s| span_end(s) >= span_end(hold)), "`{cut}`: no pointer move completed before the release");
            assert!(moved.iter().filter(|s| attr(s, "via").starts_with("gossip")).all(|s| span_start(s) >= span_end(hold)), "`{cut}`: no other admin heard the record before the release");
            return json!({"cut": cut, "hold_span_id": hold["span_id"], "parked_calls": parked_calls, "hold_ms": hold_ms, "pointer_span_id": moved[0]["span_id"], "pointer_moves": moved.iter().map(|s| json!({"node": attr(s, "node"), "via": attr(s, "via").chars().take(8).collect::<String>(), "ended_ms_after_release": (span_end(s) as i64 - span_end(hold) as i64) / 1_000_000})).collect::<Vec<_>>()});
        }
        "ready-after" => {
            let node = c["node"].as_str().unwrap();
            let ready: Vec<&Value> = named(spans, "rdm.mesh.node.update.via-ready").into_iter().filter(|s| attr(s, "node") == node).collect();
            assert_eq!(ready.len(), 1, "`{cut}`: {node} committed Ready once");
            assert!(span_start(ready[0]) >= span_end(hold), "`{cut}`: {node} reached Ready only after the release");
            let want = c["blocked_detail"].as_str().unwrap();
            if want.is_empty() {
                // Held inside its own start: it has not bound its control listener, published or evaluated a gate.
                return json!({"cut": cut, "hold_span_id": hold["span_id"], "parked_calls": parked_calls, "hold_ms": hold_ms, "ready_span_id": ready[0]["span_id"], "ready_ms_after_release": (span_start(ready[0]) - span_end(hold)) / 1_000_000});
            }
            let blocked: Vec<&Value> = named(spans, "rdm.node_admin.runtime.reject.via-not-authority-capable")
                .into_iter()
                .filter(|s| attr(s, "node") == node && attr(s, "detail").contains(want) && span_start(s) < span_end(hold))
                .collect();
            assert!(!blocked.is_empty(), "`{cut}`: {node} named its blocker (\"{want}\") before the release");
            return json!({"cut": cut, "hold_span_id": hold["span_id"], "parked_calls": parked_calls, "hold_ms": hold_ms, "ready_span_id": ready[0]["span_id"], "ready_ms_after_release": (span_start(ready[0]) - span_end(hold)) / 1_000_000, "blocked_span_id": blocked[0]["span_id"], "blocked_detail": attr(blocked[0], "detail")});
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

// ---- events, restarts, hooks --------------------------------------------------------------------------

/// The span the admin emits when it publishes `event` (lifecycle events doc, span provenance table).
fn event_span(event: &str) -> &'static str {
    match event {
        "deleting" => "rdm.node_admin.node.update.via-node-deleting",
        "restarting" => "rdm.node_admin.node.update.via-node-restarting",
        "deleted" => "rdm.node_admin.node.delete.via-node-deleted",
        other => panic!("{other} is not a lifecycle event"),
    }
}

/// A lifecycle event of a removal stalled between its durable receipt and its publish.
async fn event_cut(run: &Run, retire_order: &[String], event: &'static str, node: &str) {
    let id = format!("event:{event}");
    let spec = json!({"kind": "event", "event": event, "node": node});
    let op = format!("retire-node:{node}");
    let door = run.member_door("mesh1").await;
    let ack = door.arm(&id, spec.clone()).await;
    let build_id = run.delete(node).await;
    // `deleting` follows the NodeDeleting receipt, with the birth untouched; `deleted` follows the
    // NodeDeleted receipt, with the runtime already terminal.
    let (receipts, runtimes) = match event {
        "deleting" => (index_of(retire_order, RetireStep::NodeDeleting.name()) + 1, 1),
        "deleted" => (index_of(retire_order, RetireStep::NodeDeleted.name()) + 1, 0),
        other => panic!("{other} is not a removal event"),
    };
    let mut h = run.hold(&door, &id, ack, &build_id, &op, Some(retire_order[..receipts].to_vec()), Some((node, runtimes))).await;
    let view = run.node_from(&door, node).await;
    // Observed, not asserted: the executor derives the open overlay from its own Build facts each
    // hierarchy round (`routable` goes false from the NodeDeleting receipt alone, within one round),
    // so the executor's view is not a witness of the publish. The witness is the event's own span,
    // started only after the release (checked on the exported spans).
    h.during["node_in_executor_view"] = view.map(|v| json!({"status": v["status"], "routable": v["routable"]})).unwrap_or(Value::Null);
    let rel = door.release(&id).await;
    run.complete(&build_id).await;
    let after = run.after_retire(&door, &build_id, node, retire_order).await;
    run.note_build(&build_id, 1);
    run.span_check(json!({"kind": "event", "cut": id, "span": event_span(event), "build_id": build_id, "attempt": 1}));
    run.span_check(json!({"kind": "step", "cut": format!("{id}#steps"), "hold_cut": id, "build_id": build_id, "node": node, "pipeline": "retire", "attempt": 1, "steps": retire_order, "held_step": retire_order[receipts - 1]}));
    run.push(row(h, "lifecycle-event", &id, &spec, rel, after));
}

/// The retire half of a restart, in order.
fn restart_retire_order(retire_order: &[String]) -> Vec<String> {
    let mut o: Vec<String> = vec![RetireStep::NodeRestarting.name().to_string()];
    o.extend(retire_order.iter().filter(|s| ![RetireStep::NodeDeleting.name(), RetireStep::NodeDeleted.name(), RetireStep::ReleaseStorage.name()].contains(&s.as_str())).cloned());
    o
}

/// A restart (an attempt of the accepted Build, never a new Build) stalled at `spec`.
#[allow(clippy::too_many_arguments)]
async fn restart_cut(run: &Run, create_order: &[String], retire_order: &[String], id: &str, spec: Value, group: &str, receipts_before: usize, node: &str, attempt: u64) {
    let rr = restart_retire_order(retire_order);
    let door = run.member_door("mesh1").await;
    let before = run.node_ready(node).await;
    let pointer_before = run.fabric_build_id().await;
    let ack = door.arm(id, spec.clone()).await;
    let (status, a) = run.estate.post(&format!("/api/nodes/{node}/restart"), &Value::Null).await;
    assert_eq!(status, 202, "restart {node}: {a}");
    let build_id = a["build_id"].as_str().unwrap().to_string();
    assert_eq!(build_id, pointer_before, "a restart opens an attempt of the accepted Build; it makes no Build");
    let mut h = run.hold(&door, id, ack, &build_id, &format!("retire-node:{node}"), Some(rr[..receipts_before].to_vec()), Some((node, 1))).await;
    assert_eq!(h.attempt, attempt, "the restart is attempt {attempt} of the Build");
    h.during["attempt_is_a_restart_of_the_accepted_build"] = json!(true);
    let rel = door.release(id).await;
    let b = run.attempt_complete(&door, &build_id, attempt).await;
    let rop = format!("retire-node:{node}");
    let cop = format!("restart-node:{node}");
    assert_eq!(done_steps(&b, &rop, Some(attempt)), rr, "the restart's retire half runs every step once, in order: {b}");
    assert_eq!(done_steps(&b, &cop, Some(attempt)), create_order, "the restart's create half runs every step once, in order: {b}");
    for (op, n) in [(&rop, rr.len()), (&cop, create_order.len())] {
        let counts = step_counts(&b, op, attempt);
        assert!(counts.values().all(|c| *c == 1) && counts.len() == n, "no step receipt twice in {op}: {counts:?}");
    }
    let after_node = wait_for(&format!("{node} reborn ready under a new incarnation"), Duration::from_secs(60), || async {
        run.estate.node_opt(node).await.filter(|n| n["status"] == "ready-for-traffic" && n["incarnation_id"] != before["incarnation_id"])
    })
    .await;
    assert_eq!(after_node["node_id"], before["node_id"], "a restart keeps the NodeId");
    assert_eq!(run.runtimes(node), 1, "exactly one runtime serves {node} after the restart");
    assert_eq!(run.fabric_build_id().await, pointer_before, "Fabric.build_id is unchanged by a restart");
    run.note_build(&build_id, attempt);
    if spec["kind"] == "event" {
        run.span_check(json!({"kind": "event", "cut": id, "span": event_span("restarting"), "build_id": build_id, "attempt": attempt}));
        run.span_check(json!({"kind": "step", "cut": format!("{id}#steps"), "hold_cut": id, "build_id": build_id, "node": node, "pipeline": "retire", "attempt": attempt, "steps": rr, "held_step": RetireStep::NodeRestarting.name()}));
    } else {
        run.span_check(json!({"kind": "step", "cut": id, "build_id": build_id, "node": node, "pipeline": "retire", "attempt": attempt, "steps": rr, "held_step": RetireStep::NodeRestarting.name()}));
    }
    let after = json!({"build_state": b["state"], "attempt": attempt, "retire_half": rr, "create_half": create_order.len(), "node": {"node_id": after_node["node_id"], "old_incarnation_id": before["incarnation_id"], "new_incarnation_id": after_node["incarnation_id"]}, "runtimes": 1, "fabric_build_id": pointer_before});
    run.push(row(h, group, id, &spec, rel, after));
}

/// A lifecycle hook of `Node: Pending -> ReadyForTraffic` stalled; the create's pipeline is done.
async fn hook_cut(run: &Run, create_order: &[String], phase: &str, node: &str) {
    let id = format!("hook:{phase}");
    let spec = json!({"kind": "hook", "phase": phase, "node": node});
    let door = run.member_door("mesh1").await;
    let ack = door.arm(&id, spec.clone()).await;
    let build_id = run.spawn("mesh1", "rpc_node").await;
    let h = run.hold(&door, &id, ack, &build_id, &format!("create-node:{node}"), Some(create_order.to_vec()), Some((node, 1))).await;
    // A hook receipt belongs to one birth: the held transition is the one of the incarnation this Build decided.
    let b = run.build(&door, &build_id).await;
    let incarnation = b["steps"].as_array().unwrap().iter().find(|r| r["operation"] == format!("create-node:{node}").as_str() && r["step"] == "AllocateIdentity").map(|r| r["output"]["incarnation"].clone()).unwrap();
    assert!(h.held["hit"]["transition_id"].as_str().unwrap().ends_with(&format!("@{}", incarnation.as_str().unwrap())), "the held hook belongs to the birth this Build made: {}", h.held);
    let rel = door.release(&id).await;
    run.complete(&build_id).await;
    let after = run.after_create(&door, &build_id, node, create_order).await;
    run.note_build(&build_id, 1);
    run.span_check(json!({"kind": "hook", "cut": id, "hook_id": format!("testkit-{phase}"), "node": node}));
    run.push(row(h, "lifecycle-hook", &id, &spec, rel, after));
}

// ---- accepted-Build persistence and the pointer ------------------------------------------------------

/// The Build is durable and `Fabric.build_id` has not moved: stalled at `spec`, with the accepting
/// request unanswered. The request is made from a task, because it does not return until release.
async fn accept_cut(run: &Run, create_order: &[String], id: &str, spec: Value, group: &str, node: &str) {
    let door = run.fabric_door().await;
    let pointer_before = run.fabric_build_id().await;
    let ack = door.arm(id, spec.clone()).await;
    let request = {
        let (url, http) = (format!("{}/api/nodes/spawn", door.api), reqwest::Client::new());
        tokio::spawn(async move {
            let r = http.post(url).json(&json!({"mesh": "mesh1", "kind": "rpc_node"})).send().await.expect("the accepting request is sent");
            (r.status().as_u16(), r.json::<Value>().await.unwrap_or(Value::Null))
        })
    };
    let held = door.wait_held(id).await;
    let build_id = held["hit"]["build_id"].as_str().or(held["hit"]["pointer_to_build_id"].as_str()).expect("the cut names the Build").to_string();
    let b = run.build(&door, &build_id).await;
    assert_eq!(run.fabric_build_id().await, pointer_before, "`{id}`: the Build is durable and Fabric.build_id still names the previous Build");
    // Does anything execute a Build the pointer does not name yet? Every admin's own record of it,
    // read for a few executor rounds (a round is 300 ms) while the cut holds.
    let mut executed_during_hold = Value::Null;
    for _ in 0..12 {
        for n in run.estate.nodes().await.iter().filter(|n| n["kind"] == "node_admin").filter_map(|n| n["admin_api_base"].as_str().map(String::from)) {
            let (st, seen) = run.estate.http_get(&n, &format!("/api/builds?id={build_id}")).await;
            if st == 200 && (seen["attempt"].as_u64().unwrap_or(0) > 0 || seen["steps"].as_array().is_some_and(|s| !s.is_empty())) {
                executed_during_hold = json!({"admin_api": n, "state": seen["state"], "attempt": seen["attempt"], "executor": seen["executor"], "receipts": seen["steps"].as_array().map(|s| s.len())});
            }
        }
        if !executed_during_hold.is_null() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(!request.is_finished(), "`{id}`: the accepting request is unanswered while the cut holds");
    assert!(
        executed_during_hold.is_null(),
        "`{id}`: an unaccepted Build (Fabric.build_id still names the previous one) is executed by no admin; one claimed it: {executed_during_hold}"
    );
    let during = json!({
        "build_readable": true, "build_state": b["state"], "attempt": b["attempt"], "receipts_while_held": b["steps"].as_array().map(|s| s.len()),
        "fabric_build_id": pointer_before, "previous_build_id": pointer_before, "accepting_request_answered": false, "executed_before_the_pointer_named_it": executed_during_hold,
    });
    let h = Hold { arm_ack: ack, held, build_id: build_id.clone(), attempt: b["attempt"].as_u64().unwrap_or(0), during };
    let rel = door.release(id).await;
    let (status, body) = request.await.unwrap();
    assert_eq!((status, body["build_id"].as_str()), (202, Some(build_id.as_str())), "release answers the accepting request: {body}");
    run.complete(&build_id).await;
    let exec = run.member_door("mesh1").await;
    let after = run.after_create(&exec, &build_id, node, create_order).await;
    run.note_build(&build_id, 1);
    run.span_check(json!({"kind": "pointer", "cut": id, "build_id": build_id}));
    run.push(row(h, group, id, &spec, rel, after));
}

// ---- the Pending gate, hydration and the whole-mesh retire --------------------------------------------

/// A mesh's first admin is born and bound and published, and the fabric primary has not yet applied
/// its mesh's Pending: stalled before the `ApplyMeshPending` step. The admin must not reach Ready
/// until the release.
async fn mesh_pending_cut(run: &Run, create_order: &[String]) -> String {
    let admin = "mesh2.admin.1";
    let id = "pending:mesh-first-admin";
    let step = CreateStep::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata.name();
    let spec = json!({"kind": "receipt", "step": step, "operation": "create-node", "node": admin});
    let door = run.fabric_door().await;
    let ack = door.arm(id, spec.clone()).await;
    let (status, a) = run.estate.post("/api/meshes", &json!({"name": "mesh2", "node_admin": 1})).await;
    assert_eq!(status, 202, "create mesh2: {a}");
    let build_id = a["build_id"].as_str().unwrap().to_string();
    let at = index_of(create_order, step);
    let h = run.hold(&door, id, ack, &build_id, &format!("create-node:{admin}"), Some(create_order[..at].to_vec()), Some((admin, 1))).await;
    let rel = door.release(id).await;
    run.complete(&build_id).await;
    let after = run.after_create(&door, &build_id, admin, create_order).await;
    let b = run.build(&door, &build_id).await;
    let pending = b["steps"].as_array().unwrap().iter().find(|r| r["operation"] == format!("create-node:{admin}").as_str() && r["step"] == "ApplyMeshPending").map(|r| r["output"].clone()).unwrap();
    assert_eq!(pending["applied"], "Pending", "after the release the fabric primary applied the mesh's Pending at its first admin: {pending}");
    run.note_build(&build_id, 1);
    run.span_check(json!({"kind": "step", "cut": id, "build_id": build_id, "node": admin, "pipeline": "create", "attempt": 1, "steps": create_order, "held_step": step}));
    run.span_check(json!({"kind": "ready-after", "cut": id, "node": admin, "blocked_detail": "Pending has not been applied"}));
    run.push(row(h, "pending-gate", id, &spec, rel, after));
    build_id
}

/// The whole-mesh retire's own step, `ObserveDeparture`, stalled at its receipt.
async fn observe_departure_cut(run: &Run) {
    let admin = "mesh2.admin.1";
    let id = "retire-mesh:ObserveDeparture";
    let door = run.retire_mesh_door("mesh2").await;
    let spec = json!({"kind": "receipt", "step": RetireStep::ObserveDeparture.name(), "operation": "retire-node", "node": admin});
    let ack = door.arm(id, spec.clone()).await;
    let (status, a) = run.estate.delete("/api/meshes/mesh2").await;
    assert_eq!(status, 202, "retire mesh2: {a}");
    let build_id = a["build_id"].as_str().unwrap().to_string();
    // The whole-mesh order: an ordinary node retire with ObserveDeparture after TerminateRuntime.
    let mut order: Vec<String> = RetireStep::ORDER.iter().map(|s| s.name().to_string()).collect();
    order.insert(index_of(&order, RetireStep::TerminateRuntime.name()) + 1, RetireStep::ObserveDeparture.name().to_string());
    let at = index_of(&order, RetireStep::ObserveDeparture.name());
    let h = run.hold(&door, id, ack, &build_id, &format!("retire-node:{admin}"), Some(order[..at].to_vec()), Some((admin, 0))).await;
    let rel = door.release(id).await;
    run.complete(&build_id).await;
    let after = run.after_retire(&door, &build_id, admin, &order).await;
    run.note_build(&build_id, 1);
    run.span_check(json!({"kind": "step", "cut": id, "build_id": build_id, "node": admin, "pipeline": "retire", "attempt": 1, "steps": order, "held_step": RetireStep::ObserveDeparture.name()}));
    run.push(row(h, "retire-step", id, &spec, rel, after));
}

/// A joining node-admin held at a boot cut (armed from `<root>/faults/<name>.boot.json`, before its
/// first line runs): the create Build waits on it, and it must not be Ready until the release.
#[allow(clippy::too_many_arguments)]
async fn joining_admin_cut(run: &Run, create_order: &[String], id: &str, boot: Value, group: &str, admin: &str, receipts_before: usize, exact: bool, blocked_detail: &str) {
    let faults = run.root.join("faults");
    std::fs::create_dir_all(&faults).unwrap();
    // A door file from an earlier birth of this path is stale; the birth rewrites it.
    let _ = std::fs::remove_file(faults.join(format!("{admin}.door")));
    let mut spec = boot.clone();
    spec["id"] = json!(id);
    std::fs::write(faults.join(format!("{admin}.boot.json")), serde_json::to_vec(&json!([spec])).unwrap()).unwrap();
    let exec = run.fabric_door().await;
    let build_id = run.spawn("mesh1", "node_admin").await;
    let door = Door::open(&run.root, admin, "").await;
    let ack = json!({"armed": id, "node": admin, "spec": boot, "armed_by": "boot file before the birth's first line"});
    let held = door.wait_held(id).await;
    // The pipeline goes on while the admin boots, and stops at the step that needs the admin Ready,
    // which only the release lets complete. `exact`: it must settle at `receipts_before` receipts;
    // otherwise (the admin may be held inside its own start or after it) it is a prefix of the order
    // that never reaches WaitForNodeReady.
    let op = format!("create-node:{admin}");
    let want = create_order[..receipts_before].to_vec();
    let b = if exact {
        wait_for(&format!("`{id}`: the Build reaches {} and waits on the held admin", want.last().unwrap()), Duration::from_secs(60), || async {
            let b = run.build(&exec, &build_id).await;
            (done_steps(&b, &op, Some(1)) == want).then_some(b)
        })
        .await
    } else {
        wait_for(&format!("`{id}`: the Build reaches the join of the held admin"), Duration::from_secs(60), || async {
            let b = run.build(&exec, &build_id).await;
            let d = done_steps(&b, &op, Some(1));
            (d.len() >= index_of(create_order, CreateStep::WaitForBind.name())).then_some(b)
        })
        .await
    };
    let done = done_steps(&b, &op, Some(1));
    assert!(done.len() <= receipts_before && done == create_order[..done.len()].to_vec(), "`{id}`: the Build holds a prefix of the documented order that stops before WaitForNodeReady: {done:?}");
    assert!(b["state"] != "complete", "`{id}`: the Build is not complete while the joining admin is held: {b}");
    let view = run.estate.node_opt(admin).await;
    if let Some(v) = &view {
        assert_ne!(v["status"], "ready-for-traffic", "`{id}`: the held admin is not Ready: {v}");
    }
    let during = json!({"build_state": b["state"], "attempt": 1, "operation": op, "done_steps": done, "admin_in_view": view.map(|v| json!({"status": v["status"]})), "cut_still_held": door.cut(id).await["held"]});
    let h = Hold { arm_ack: ack, held, build_id: build_id.clone(), attempt: 1, during };
    let rel = door.release(id).await;
    run.complete(&build_id).await;
    let after = run.after_create(&exec, &build_id, admin, create_order).await;
    run.note_build(&build_id, 1);
    run.span_check(json!({"kind": "ready-after", "cut": id, "node": admin, "blocked_detail": blocked_detail}));
    run.push(row(h, group, id, &boot, rel, after));
}
