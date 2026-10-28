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
