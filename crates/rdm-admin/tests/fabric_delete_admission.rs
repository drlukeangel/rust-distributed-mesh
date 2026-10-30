//! fabric.delete at the Build door before the initial fabric is ready (build.md §1: "The
//! one exception is fabric.delete, which may interrupt the Build in progress because its target is
//! nothing"; fabric-delete.md "Precondition"). Every cell reads the Build log it decides from: nothing
//! here depends on timing.

use rdm_admin::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::control_plane::{ControlPlane, Refusal};
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use std::sync::Arc;

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n
}

/// Two meshes of two node-admins each, two rpc nodes in mesh1 and one in mesh2. `mesh1.admin.1` holds
/// the fabric-primary role and mesh1's, `mesh2.admin.1` holds mesh2's, `mesh1.rpc.1` mesh1's rpc role with
/// `mesh1.rpc.2` its secondary: a stop, restart or replace of `mesh1.rpc.1` moves its role by
/// node.primary.update and is not refused for want of a secondary (proposal Part C1, Q7). The fabric
/// is pending.
fn view(status: ScopeStatus) -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status, provider: ProviderKind::Process },
        meshes: ["mesh1", "mesh2"].iter().map(|m| Mesh { id: Some(MeshId::mint()), name: m.to_string(), status: ScopeStatus::Ready }).collect(),
        nodes: vec![
            node("mesh1.admin.1", true, true),
            node("mesh1.admin.2", false, false),
            node("mesh1.rpc.1", true, false),
            node("mesh1.rpc.2", false, false),
            node("mesh2.admin.1", true, false),
            node("mesh2.admin.2", false, false),
            node("mesh2.rpc.1", true, false),
        ],
    }
}

struct Rig {
    cp: Arc<ControlPlane>,
    builds: Arc<MemoryBuildStateAdapter>,
}

/// This admin (`mesh1.admin.1`, the fabric-primary) holds a settled accepted Build of the view; the
/// fabric has not first reached ready.
async fn rig(status: ScopeStatus) -> Rig {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(status);
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    Rig { cp, builds }
}

impl Rig {
    async fn facts(&self) -> usize {
        self.builds.facts().await.unwrap().len()
    }
}

/// The claim of `attempt` of `build` by this admin for itself, decided on its own log: the attempt runs.
async fn running(r: &Rig, build: &rdm_admin::build::BuildId, attempt: u32) {
    use rdm_admin::build_state::{BuildAttemptClaim, ClaimOutcome};
    let claim = BuildAttemptClaim { build_id: build.clone(), attempt, executor: "mesh1.admin.1".into(), claimant: Some("mesh1.admin.1".into()), mesh: None };
    assert_eq!(r.builds.claim_attempt(&claim).await.unwrap(), ClaimOutcome::Won, "attempt {attempt} of {build} is claimed");
}

/// CONTRACT (build.md §1: "The one exception is fabric.delete, which may interrupt the Build in progress
/// because its target is nothing"; fabric-delete.md "Precondition"): while the initial fabric is pending
/// and a growth Build's attempt runs, a standalone mesh.delete and node.delete are refused
/// build-in-progress and fabric.delete is accepted as the next Build, the current one from then on;
/// beside it a standalone mesh.delete is still refused build-in-progress. What must NOT happen:
/// fabric.delete refused, or a standalone mesh.delete or node.delete admitted beside a Build in progress.
#[tokio::test]
async fn a_fabric_delete_before_the_first_ready_interrupts_the_build_in_progress_and_standalone_deletes_stay_refused() {
    let r = rig(ScopeStatus::Pending).await;
    let grow = r.cp.submit("test", crate::named_request(&r.cp, TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: rdm_core::NodeKind::RpcNode }).await).await.expect("growth is admitted before the first ready");
    running(&r, &grow.build_id, 1).await;
    assert_eq!(r.builds.current_build().await.unwrap(), rdm_admin::build_state::CurrentBuild::InProgress(grow.build_id.clone()), "the growth Build is in progress");
    for change in [TopologyChange::RemoveMesh { mesh: "mesh2".into() }, TopologyChange::RemoveNode { node: "mesh1.rpc.1".parse().unwrap() }] {
        match r.cp.submit("test", change.clone()).await {
            Err(Refusal::BuildInProgress(_)) => {}
            other => panic!("the standalone {change:?} was not refused build-in-progress: {other:?}"),
        }
    }
    let before = r.facts().await;
    let delete = r.cp.submit("test", TopologyChange::DeleteFabric).await.unwrap_or_else(|e| panic!("fabric.delete is admitted before the first ready while a Build is in progress: {e:?}"));
    assert_ne!(delete.build_id, grow.build_id, "the delete is a Build of its own");
    assert_eq!((delete.attempt, delete.mesh.as_deref()), (1, None));
    assert_eq!(r.facts().await, before + 1, "the delete's acceptance is the one fact written");
    assert_eq!(r.builds.current_build().await.unwrap(), rdm_admin::build_state::CurrentBuild::InProgress(delete.build_id.clone()), "the delete is the current Build from its acceptance");
    match r.cp.submit("test", TopologyChange::RemoveMesh { mesh: "mesh2".into() }).await {
        Err(Refusal::BuildInProgress(_)) => {}
        other => panic!("a standalone mesh.delete beside the delete was not refused build-in-progress: {other:?}"),
    }
}

/// CONTRACT (build.md §1: "a successor fabric-primary reads the delete as the current Build and continues
/// the teardown"; "An operation that fails to end within its bound fails the delete's attempt by name, the
/// intent retained"; "No other Build ends another"): a second fabric.delete while the first is in
/// progress answers the first, at the attempt it stands at, and accepts nothing; once the first's attempt
/// failed, a fabric.delete answers its next attempt and still accepts nothing. What must NOT happen: a
/// second delete Build accepted, interrupting the first.
#[tokio::test]
async fn a_second_fabric_delete_resumes_the_first_in_progress_or_failed_and_accepts_no_build() {
    use rdm_admin::build_state::{AttemptOutcome, BuildAttemptReceipt};
    let r = rig(ScopeStatus::Pending).await;
    let first = r.cp.submit("test", TopologyChange::DeleteFabric).await.unwrap_or_else(|e| panic!("fabric.delete is admitted: {e:?}"));
    let before = r.facts().await;
    let again = r.cp.submit("test", TopologyChange::DeleteFabric).await.unwrap_or_else(|e| panic!("a second fabric.delete resumes the first: {e:?}"));
    assert_eq!((&again.build_id, again.attempt), (&first.build_id, 1), "the second delete answers the first at its attempt");
    assert_eq!(r.facts().await, before, "nothing is accepted");
    running(&r, &first.build_id, 1).await;
    let failed = BuildAttemptReceipt { build_id: first.build_id.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: "mesh.delete:x: a member did not end within its bound".into(), code: rdm_core::outcome::Code::DeadlineExceeded }, mesh: None };
    r.builds.append_attempt_receipt(&failed).await.unwrap();
    let before = r.facts().await;
    let resumed = r.cp.submit("test", TopologyChange::DeleteFabric).await.unwrap_or_else(|e| panic!("a fabric.delete after the first's attempt failed resumes it: {e:?}"));
    assert_eq!((&resumed.build_id, resumed.attempt), (&first.build_id, 2), "the failed delete is resumed at its next attempt");
    assert_eq!(r.facts().await, before, "nothing is accepted");
}
