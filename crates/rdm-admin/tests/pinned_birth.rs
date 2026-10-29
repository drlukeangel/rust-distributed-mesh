//! The exact birth an operation carries (build.md §4.4, closing rule; Q218 A): "The operation
//! carries the exact birth the gates pinned (node id and incarnation). The mesh-primary that runs it
//! verifies that birth again against its own view at execution, so a birth replaced between
//! acceptance and execution is refused there, by name."
//!
//! Each cell accepts the operation on the fabric-primary's control plane in process over the memory
//! Build store, plans it from the Build the store holds, then replaces the birth in the view the
//! executing mesh-primary checks it against. Nothing here depends on timing.

use rdm_admin::accepted::{plan_for_build, AcceptedStore, FabricTopology, TopologyChange};
use rdm_admin::build::{BuildOperation, PinnedBirth};
use rdm_admin::build_claim::AttemptContexts;
use rdm_admin::build_state::{AttemptReason, BuildStateAdapter, LocalBuildLog, MemoryBuildStateAdapter};
use rdm_admin::control_plane::{ActionKind, ControlPlane};
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use std::sync::Arc;

fn node(name: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// mesh1: its admin, the fabric-primary, and two rpc nodes.
fn view() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node("mesh1.admin.1", true), node("mesh1.rpc.1", false), node("mesh1.rpc.2", false)],
    }
}

/// `t` with the birth at `path` replaced by another birth there: a new node id, and the same
/// incarnation when `same_incarnation`.
fn replaced(t: &Topology, path: &str, same_incarnation: bool) -> Topology {
    let mut t = t.clone();
    let n = t.nodes.iter_mut().find(|n| n.name.to_string() == path).unwrap();
    n.node_id = NodeId::mint();
    if !same_incarnation {
        n.incarnation_id = Some(IncarnationId::mint());
    }
    t
}

fn birth(t: &Topology, path: &str) -> PinnedBirth {
    PinnedBirth::of(t.node(&path.parse().unwrap()).unwrap()).unwrap()
}

async fn control_plane(t: &Topology) -> (Arc<MemoryBuildStateAdapter>, ControlPlane) {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(t), "mesh1.admin.1").await.unwrap();
    let contexts = Arc::new(MemoryBuildStateAdapter::new());
    let mut cp = ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time())
        .with_contexts(Arc::new(AttemptContexts::on(contexts as Arc<dyn LocalBuildLog>)));
    // The node records a create writes: the gates read a birth from its record, never from the gossip view (Q207).
    let _ = cp.node_rows.set(crate::records_of(t).await);
    cp.topology = Arc::new(tokio::sync::RwLock::new(t.clone()));
    (builds, cp)
}

fn json_birth(v: &serde_json::Value) -> (serde_json::Value, serde_json::Value) {
    (v["node_id"].clone(), v["incarnation"].clone())
}

fn as_json(b: &PinnedBirth) -> (serde_json::Value, serde_json::Value) {
    json_birth(&serde_json::to_value(b).unwrap())
}

/// CONTRACT (build.md §4.4): a standalone node.stop accepted on a birth carries that birth's node id
/// and incarnation in its attempt's action and in the operation the plan hands the executor.
/// Must NOT happen: an action or an operation carrying the incarnation alone, which another birth at
/// the path may hold.
#[tokio::test]
async fn a_stop_carries_the_node_id_and_incarnation_the_gates_pinned() {
    let t = view();
    let pinned = birth(&t, "mesh1.rpc.1");
    let (builds, cp) = control_plane(&t).await;
    let opened = cp.open_attempt_in(tracing::info_span!("pinned-stop"), "pinned-stop", AttemptReason::Stop, "mesh1.rpc.1".parse().unwrap(), ActionKind::Stop).await.expect("the stop is admitted");
    let build = builds.read_build(&opened.build_id).await.unwrap();
    let action = serde_json::to_value(build.action.as_ref().expect("the attempt carries its action")).unwrap();
    assert_eq!((action["node_id"].clone(), action["from_incarnation"].clone()), as_json(&pinned), "the stop's action carries the pinned birth: {action}");
    let ops = plan_for_build(&build, &t).operations;
    let stop = ops.iter().find(|op| op.key() == "node.stop:mesh1.rpc.1").unwrap_or_else(|| panic!("the stop is planned: {ops:?}"));
    let stop = serde_json::to_value(stop).unwrap();
    assert_eq!((stop["node_id"].clone(), stop["from_incarnation"].clone()), as_json(&pinned), "the operation the executor runs carries the pinned birth: {stop}");
}

/// CONTRACT (build.md §4.4; Q218: "a Build's node delete ... acts on whatever birth the view holds at
/// the path and carries no birth"): a node.delete Build accepted on a birth pins it, and the delete
/// the plan hands the executor carries it.
/// Must NOT happen: a delete operation that names its path alone, for the executor to resolve.
#[tokio::test]
async fn a_node_delete_build_carries_the_birth_it_was_accepted_on() {
    let t = view();
    let pinned = birth(&t, "mesh1.rpc.2");
    let (builds, cp) = control_plane(&t).await;
    let opened = cp.submit("pinned-delete", TopologyChange::RemoveNode { node: "mesh1.rpc.2".parse().unwrap() }).await.expect("the delete is accepted");
    let build = builds.read_build(&opened.build_id).await.unwrap();
    let ops = plan_for_build(&build, &t).operations;
    let delete = ops.iter().find(|op| op.key() == "node.delete:mesh1.rpc.2").unwrap_or_else(|| panic!("the delete is planned: {ops:?}"));
    assert!(matches!(delete, BuildOperation::DeleteLiveNode { .. }), "a live birth's delete: {delete:?}");
    let delete = serde_json::to_value(delete).unwrap();
    assert_eq!(json_birth(&delete["birth"]), as_json(&pinned), "the delete carries the birth it was accepted on: {delete}");
}

/// CONTRACT (build.md §4.4; Q218 cell: "the same incarnation under a different node id included"):
/// the executing mesh-primary's view holds another birth at the path, with the pinned incarnation
/// under another node id: the operation is refused `birth-replaced`, naming the pinned birth and the
/// birth the view holds, and the birth the view holds is not the one handed to the operation.
/// Must NOT happen: the birth found by its incarnation alone and acted on.
#[tokio::test]
async fn a_birth_replaced_under_the_same_incarnation_is_refused_birth_replaced() {
    let t = view();
    let pinned = birth(&t, "mesh1.rpc.1");
    let now = replaced(&t, "mesh1.rpc.1", true);
    let held = birth(&now, "mesh1.rpc.1");
    assert_eq!(held.incarnation, pinned.incarnation);
    let path: PathName = "mesh1.rpc.1".parse().unwrap();
    let refused = pinned.held_in(&now, "node.stop:mesh1.rpc.1", &path).expect_err("the replaced birth is refused");
    assert_eq!(refused.reason(), "birth-replaced");
    assert_eq!((refused.expected.clone(), refused.observed.clone()), (pinned.clone(), Some(held.clone())), "the refusal names the pinned birth and the birth the view holds");
    assert!(refused.to_string().starts_with("birth-replaced: "), "{refused}");
    assert_eq!(birth(&now, "mesh1.rpc.1"), held, "the replacement is untouched");
}

/// CONTRACT (build.md §4.4): the check against the executing mesh-primary's view. A birth with
/// another node id and incarnation at the path is refused `birth-replaced`; the pinned birth is the
/// one handed to the operation, found by its node id wherever the view lists it (a birth renamed to
/// `<path>.old` included); a view holding nothing of it answers `None` for the caller's own records.
#[tokio::test]
async fn the_check_hands_the_operation_only_the_pinned_birth() {
    let t = view();
    let path: PathName = "mesh1.rpc.1".parse().unwrap();
    let pinned = birth(&t, "mesh1.rpc.1");
    let now = replaced(&t, "mesh1.rpc.1", false);
    let refused = pinned.held_in(&now, "node.delete:mesh1.rpc.1", &path).expect_err("another birth at the path is refused");
    assert_eq!((refused.reason(), refused.observed.clone()), ("birth-replaced", Some(birth(&now, "mesh1.rpc.1"))));
    let held = pinned.held_in(&t, "node.delete:mesh1.rpc.1", &path).expect("the pinned birth passes").expect("the view holds it");
    assert_eq!(PinnedBirth::of(&held), Some(pinned.clone()));
    // Renamed to `.old` and a successor at the path: the pinned birth is the renamed one.
    let mut renamed = replaced(&t, "mesh1.rpc.1", false);
    let mut old = t.node(&path).unwrap().clone();
    old.name = "mesh1.rpc.1.old".parse().unwrap();
    renamed.nodes.push(old);
    let held = pinned.held_in(&renamed, "node.stop:mesh1.rpc.1", &path).expect("the renamed pinned birth passes").expect("the view holds it");
    assert_eq!(PinnedBirth::of(&held), Some(pinned.clone()));
    let mut gone = t.clone();
    gone.nodes.retain(|n| n.name != path);
    assert_eq!(pinned.held_in(&gone, "node.delete:mesh1.rpc.1", &path), Ok(None));
}

/// CONTRACT (node-delete.md Never evidence: "Gossip, silence, Ping failure and timeout never say the
/// runtime exited"; Q219: "At execution Dead alone never skips the stop or the terminate"): a Build
/// step's node.delete of a birth the executing view marks Dead runs the delete that composes its stop,
/// pinned to that birth; only a stopped birth's delete composes none.
/// Must NOT happen: a Dead birth deleted as a stopped one, its stop skipped on silence alone.
#[test]
fn a_dead_births_step_delete_composes_its_stop() {
    let t = view();
    let path: PathName = "mesh1.rpc.1".parse().unwrap();
    let pinned = birth(&t, "mesh1.rpc.1");
    let mut dead = t.node(&path).unwrap().clone();
    dead.status = NodeStatus::Dead;
    let op = rdm_admin::build::step_delete(&path, &pinned, Some(&dead));
    assert!(matches!(&op, BuildOperation::DeleteLiveNode { exited: None, birth: Some(b), .. } if *b == pinned), "a Dead birth's delete stops it first: {op:?}");
    let mut stopped = t.node(&path).unwrap().clone();
    stopped.stopped = true;
    assert!(matches!(rdm_admin::build::step_delete(&path, &pinned, Some(&stopped)), BuildOperation::DeleteNode { .. }), "a stopped birth's delete composes no stop");
    let live = t.node(&path).unwrap().clone();
    assert!(matches!(rdm_admin::build::step_delete(&path, &pinned, Some(&live)), BuildOperation::DeleteLiveNode { exited: None, birth: Some(_), .. }), "a live birth's delete stops it first");
}

struct AtlasGate(Option<PinnedBirth>);

#[async_trait::async_trait]
impl rdm_admin::build_drive::Gate for AtlasGate {
    async fn atlas_birth(&self, _path: &PathName, _observed: &Topology) -> Result<Option<rdm_admin::accepted::AtlasBirth>, String> {
        Ok(Some(rdm_admin::accepted::AtlasBirth(self.0.clone())))
    }
}

struct PinnedRunner {
    topology: Arc<tokio::sync::RwLock<Topology>>,
    builds: Arc<MemoryBuildStateAdapter>,
    permanent_error: std::sync::atomic::AtomicBool,
    acted_on: std::sync::Mutex<Vec<PinnedBirth>>,
}

#[async_trait::async_trait]
impl rdm_admin::executor::OperationRunner for PinnedRunner {
    async fn run(&self, build: &rdm_admin::build::BuildId, _lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        let BuildOperation::StopNode { node, node_id, from_incarnation } = op else {
            return Err(rdm_admin::executor::RunEnd::failed(format!("unexpected operation: {op:?}")));
        };
        let pin = PinnedBirth { node_id: node_id.clone(), incarnation: from_incarnation.clone() };
        let held = pin
            .held_in(&*self.topology.read().await, &op.key(), node)
            .map_err(|r| rdm_admin::executor::RunEnd::failed(r.to_string()))?;
        let Some(held) = held else {
            return Err(rdm_admin::executor::RunEnd::Incomplete(format!("{}: birth {} at incarnation {} is not in this admin's view", op.key(), pin.node_id, pin.incarnation.as_str())));
        };
        if self.permanent_error.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(rdm_admin::executor::RunEnd::failed(format!("{}: permanent runtime failure on {}", op.key(), held.node_id)));
        }
        let actual = PinnedBirth::of(&held).expect("held birth has an incarnation");
        self.acted_on.lock().unwrap().push(actual);
        for step in ["StopNode", "Complete"] {
            self.builds
                .append_step_receipt(&rdm_admin::build_state::BuildStepReceipt {
                    mesh: None,
                    build_id: build.clone(),
                    attempt,
                    operation: op.key(),
                    step: step.into(),
                    outcome: rdm_admin::build_state::StepOutcome::Complete,
                    output: None,
                    executor: None,
                })
                .await
                .map_err(|e| rdm_admin::executor::RunEnd::failed(e.to_string()))?;
        }
        Ok(())
    }
}

/// CONTRACT (build.md:338, Q274): a pinned birth the executor's view does not hold yet ends the
/// step incomplete (`RunEnd::Incomplete`), hands the attempt back to the fabric-primary leaving the
/// Build in progress, claims no new attempt while the view is unchanged, and resumes on that same
/// pinned birth once the view moves to hold it; another birth at the name is still refused
/// `birth-replaced`, and a permanent error on the held birth is still `Failed`.
#[tokio::test]
async fn a_not_yet_held_pinned_birth_ends_incomplete_and_resumes_on_that_birth_once_the_view_holds_it() {
    use rdm_admin::build_claim::ClaimDoor;
    use rdm_admin::build_drive::{Drive, DriveEnd, DriveEnv};
    use rdm_admin::build_state::{AttemptOutcome, BuildFact, BuildState};

    let full = view();
    let path: PathName = "mesh1.rpc.1".parse().unwrap();
    let pinned = birth(&full, "mesh1.rpc.1");
    let (builds, cp) = control_plane(&full).await;
    let opened = cp
        .open_attempt_in(tracing::info_span!("pinned-resume"), "pinned-resume", AttemptReason::Stop, path.clone(), ActionKind::Stop)
        .await
        .expect("the stop is admitted");

    // Start with the executor's view missing `mesh1.rpc.1`.
    let mut missing = full.clone();
    missing.nodes.retain(|n| n.name != path);
    let topology = Arc::new(tokio::sync::RwLock::new(missing));
    let runner = Arc::new(PinnedRunner {
        topology: topology.clone(),
        builds: builds.clone(),
        permanent_error: std::sync::atomic::AtomicBool::new(false),
        acted_on: std::sync::Mutex::new(Vec::new()),
    });
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let fp_id = full.node(&me).unwrap().node_id.clone();
    let dispatch = crate::loopback::Loopback::new(builds.clone(), builds.clone(), topology.clone(), runner.clone());
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: topology.clone(),
        builds: builds.clone(),
        door: Arc::new(ClaimDoor {
            node_id: fp_id,
            me,
            topology: topology.clone(),
            builds: builds.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            claims: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
            claimed: Default::default(),
        }),
        dispatcher: dispatch.clone(),
        gate: Arc::new(AtlasGate(Some(pinned.clone()))),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        notices: crate::Told::default().notices(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });

    // Pass 1: the view does not hold the pinned birth yet -> the opened attempt ends Incomplete
    // and hands back to the fabric-primary; the Build stays in progress (`BuildState::Pending`).
    let drive = Drive::detached(opened.build_id.clone());
    let first = env.run(&drive).await;
    assert!(matches!(&first, DriveEnd::Stalled(why) if why.contains("ended incomplete")), "the opened attempt stalls on incomplete evidence: {first:?}");
    let p1 = builds.read_build(&opened.build_id).await.unwrap();
    assert_eq!((p1.attempt, p1.state), (opened.attempt, BuildState::Pending), "the Build stays in progress after incomplete hand-back");
    let verdict1 = builds.facts().await.unwrap().into_iter().find_map(|f| match f {
        BuildFact::Attempt(a) if a.build_id == opened.build_id && a.attempt == opened.attempt => Some(a.outcome),
        _ => None,
    });
    assert_eq!(verdict1, Some(AttemptOutcome::HandedOff { to: "mesh1.admin.1".into() }));

    // Pass 2 with nothing moved in the view: no new attempt is claimed or dispatched.
    let second = env.run(&drive).await;
    assert!(matches!(second, DriveEnd::Stalled(_)), "unchanged view stalls without claiming: {second:?}");
    assert_eq!(builds.read_build(&opened.build_id).await.unwrap().attempt, opened.attempt);
    assert_eq!(dispatch.dispatched.lock().unwrap().len(), 1);

    // Pass 3: the view moves to hold the exact pinned birth -> the next attempt is claimed,
    // resumes on that same pinned birth, and completes the Build.
    let records = rdm_admin::admin::Records::default();
    records.install_projected(&topology, || full.clone()).await;
    assert_eq!(env.run(&drive).await, DriveEnd::Terminal);
    let p2 = builds.read_build(&opened.build_id).await.unwrap();
    assert_eq!((p2.attempt, p2.state), (opened.attempt + 1, BuildState::Complete));
    assert_eq!(*runner.acted_on.lock().unwrap(), vec![pinned.clone()], "resumed on the exact pinned birth");

    // Reopen with another birth at the name -> refused `birth-replaced` (`BuildState::Failed`).
    let replaced_view = replaced(&full, "mesh1.rpc.1", false);
    records.install_projected(&topology, || replaced_view).await;
    let reopened = cp
        .open_attempt_in(tracing::info_span!("pinned-replaced"), "pinned-replaced", AttemptReason::Stop, path.clone(), ActionKind::Stop)
        .await
        .unwrap();
    let drive_replaced = Drive::detached(reopened.build_id.clone());
    assert_eq!(env.run(&drive_replaced).await, DriveEnd::Terminal);
    assert_eq!(builds.read_build(&reopened.build_id).await.unwrap().state, BuildState::Failed, "another birth at the name fails the Build with birth-replaced");

    // Reopen with the exact pinned birth held, but a permanent error on execution -> `BuildState::Failed`.
    records.install_projected(&topology, || full.clone()).await;
    runner.permanent_error.store(true, std::sync::atomic::Ordering::SeqCst);
    let reopened_err = cp
        .open_attempt_in(tracing::info_span!("pinned-perm-err"), "pinned-perm-err", AttemptReason::Stop, path, ActionKind::Stop)
        .await
        .unwrap();
    let drive_err = Drive::detached(reopened_err.build_id.clone());
    assert_eq!(env.run(&drive_err).await, DriveEnd::Terminal);
    assert_eq!(builds.read_build(&reopened_err.build_id).await.unwrap().state, BuildState::Failed, "a permanent error on the held birth fails the Build");
}

