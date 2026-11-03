//! The exact birth an operation carries (build.md §4.4, closing rule; Q218 A): "The operation
//! carries the exact birth the gates pinned (node id and incarnation). The mesh-primary that runs it
//! verifies that birth again against its own view at execution, so a birth replaced between
//! acceptance and execution is refused there, by name."
//!
//! Each cell accepts the operation on the fabric-primary's control plane in process over the memory
//! Build store, plans it from the Build the store holds, then replaces the birth in the view the
//! executing mesh-primary checks it against. Nothing here depends on timing.

use rdm_admin::accepted::{plan_for_build, AcceptedStore, FabricTopology};
use rdm_admin::build::{BuildOperation, PinnedBirth};
use rdm_admin::build_claim::AttemptContexts;
use rdm_admin::build_state::{BuildStateAdapter, LocalBuildLog, MemoryBuildStateAdapter};
use rdm_admin::control_plane::ControlPlane;
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use std::sync::Arc;

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// mesh1: its admin, the fabric-primary, and two nodes.
fn view() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", false), node(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", false)],
    }
}

/// `t` with the birth at `path` replaced by another birth there: a new node id, and the same
/// incarnation when `same_incarnation`.
fn replaced(t: &Topology, path: &PathName, same_incarnation: bool) -> Topology {
    let mut t = t.clone();
    let n = t.nodes.iter_mut().find(|n| &n.location() == path).unwrap();
    n.node_id = NodeId::mint();
    if !same_incarnation {
        n.incarnation_id = Some(IncarnationId::mint());
    }
    t
}

fn birth(t: &Topology, path: &PathName) -> PinnedBirth {
    PinnedBirth::of(t.node(path).unwrap()).unwrap()
}

async fn control_plane(t: &Topology) -> (Arc<MemoryBuildStateAdapter>, ControlPlane) {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(t), &t.node(&PathName::new("mesh1.node-admin", "1").unwrap()).unwrap().birth().unwrap(), "mesh1").await.unwrap();
    let contexts = Arc::new(MemoryBuildStateAdapter::new());
    let mut cp = crate::control_plane(builds.clone(), accepted, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), t.clone(), crate::adopted_time())
        .with_contexts(Arc::new(AttemptContexts::on(contexts as Arc<dyn LocalBuildLog>)));
    // The node records a create writes: the gates read a birth from its record, never from the gossip view (Q207).
    crate::bind_records(&cp, &[t]).await;
    cp.topology = Arc::new(tokio::sync::RwLock::new(t.clone()));
    (builds, cp)
}

fn json_birth(v: &serde_json::Value) -> (serde_json::Value, serde_json::Value) {
    (v["node_id"].clone(), v["incarnation"].clone())
}

fn as_json(b: &PinnedBirth) -> (serde_json::Value, serde_json::Value) {
    json_birth(&serde_json::to_value(b).unwrap())
}

/// CONTRACT (build.md §4.4; Q218: "a Build's node delete ... acts on whatever birth the view holds at
/// the path and carries no birth"): a node.delete Build accepted on a birth pins it, and the delete
/// the plan hands the executor carries it.
/// Must NOT happen: a delete operation that names its path alone, for the executor to resolve.
#[tokio::test]
async fn a_node_delete_build_carries_the_birth_it_was_accepted_on() {
    let t = view();
    let pinned = birth(&t, &PathName::new("mesh1.node", "2").unwrap());
    let (builds, cp) = control_plane(&t).await;
    let opened = cp.submit("pinned-delete", crate::named_request(&cp, move |t: &rdm_admin::topology::Topology| crate::removed(t, &[rdm_core::PathName { path: "mesh1.node".into(), name: "2".into() }])).await).await.expect("the delete is accepted");
    let build = builds.read_build(&opened.build_id).await.unwrap();
    let ops = plan_for_build(&build, &t).operations;
    let delete = ops.iter().find(|op| op.key() == rdm_core::OperationId::node("node.delete", &pinned.node_id)).unwrap_or_else(|| panic!("the delete is planned: {ops:?}"));
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
    let pinned = birth(&t, &PathName::new("mesh1.node", "1").unwrap());
    let now = replaced(&t, &PathName::new("mesh1.node", "1").unwrap(), true);
    let held = birth(&now, &PathName::new("mesh1.node", "1").unwrap());
    assert_eq!(held.incarnation, pinned.incarnation);
    let path: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
    let refused = pinned.held_in(&now, &rdm_core::OperationId::node("node.stop", &pinned.node_id), &path).expect_err("the replaced birth is refused");
    assert_eq!(refused.reason(), "birth-replaced");
    assert_eq!((refused.expected.clone(), refused.observed.clone()), (pinned.clone(), Some(held.clone())), "the refusal names the pinned birth and the birth the view holds");
    assert!(refused.to_string().starts_with("birth-replaced: "), "{refused}");
    assert_eq!(birth(&now, &PathName::new("mesh1.node", "1").unwrap()), held, "the replacement is untouched");
}

/// CONTRACT (build.md §4.4): the check against the executing mesh-primary's view. A birth with
/// another node id and incarnation at the path is refused `birth-replaced`; the pinned birth is the
/// one handed to the operation, found by its node id wherever the view lists it (a birth renamed to
/// `<name>-old` at the unchanged path included); a view holding nothing of it answers `None` for the caller's own records.
#[tokio::test]
async fn the_check_hands_the_operation_only_the_pinned_birth() {
    let t = view();
    let path: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
    let pinned = birth(&t, &PathName::new("mesh1.node", "1").unwrap());
    let now = replaced(&t, &PathName::new("mesh1.node", "1").unwrap(), false);
    let refused = pinned.held_in(&now, &rdm_core::OperationId::node("node.delete", &pinned.node_id), &path).expect_err("another birth at the path is refused");
    assert_eq!((refused.reason(), refused.observed.clone()), ("birth-replaced", Some(birth(&now, &PathName::new("mesh1.node", "1").unwrap()))));
    let held = pinned.held_in(&t, &rdm_core::OperationId::node("node.delete", &pinned.node_id), &path).expect("the pinned birth passes").expect("the view holds it");
    assert_eq!(PinnedBirth::of(&held), Some(pinned.clone()));
    // Renamed to `<name>-old` at the unchanged path and a successor at the path: the pinned birth is the renamed one.
    let mut renamed = replaced(&t, &PathName::new("mesh1.node", "1").unwrap(), false);
    let mut old = t.node(&path).unwrap().clone();
    { let at = rdm_core::PathName::new("mesh1.node", "1-old").unwrap(); old.path = at.path; old.name = at.name; }
    renamed.nodes.push(old);
    let held = pinned.held_in(&renamed, &rdm_core::OperationId::node("node.stop", &pinned.node_id), &path).expect("the renamed pinned birth passes").expect("the view holds it");
    assert_eq!(PinnedBirth::of(&held), Some(pinned.clone()));
    let mut gone = t.clone();
    gone.nodes.retain(|n| n.location() != path);
    assert_eq!(pinned.held_in(&gone, &rdm_core::OperationId::node("node.delete", &pinned.node_id), &path), Ok(None));
}

struct AtlasGate(Option<PinnedBirth>);

#[async_trait::async_trait]
impl rdm_admin::build_drive::Gate for AtlasGate {
    async fn atlas_birth(&self, _node_id: &NodeId, _observed: &Topology) -> Result<Option<rdm_admin::accepted::AtlasBirth>, String> {
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
        let BuildOperation::StopNode { path, name, node_id, from_incarnation, .. } = op else {
            return Err(rdm_admin::executor::RunEnd::failed(format!("unexpected operation: {op:?}")));
        };
        let node = &rdm_core::PathName { path: path.clone(), name: name.clone() };
        let pin = PinnedBirth { mesh: op.mesh().unwrap().to_owned(), r#type: op.r#type().unwrap(), node_id: node_id.clone(), incarnation: from_incarnation.clone() };
        let held = pin
            .held_in(&*self.topology.read().await, &op.key(), node)
            .map_err(|r| rdm_admin::executor::RunEnd::failed(r.to_string()))?;
        let Some(held) = held else {
            return Err(rdm_admin::executor::RunEnd::Incomplete(format!("{:?}: birth {} at incarnation {} is not in this admin's view", op.key(), pin.node_id, pin.incarnation.as_str())));
        };
        if self.permanent_error.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(rdm_admin::executor::RunEnd::failed(format!("{:?}: permanent runtime failure on {}", op.key(), held.node_id)));
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


