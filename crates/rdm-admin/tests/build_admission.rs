//! One topology Build in flight (ops-naming-proposal.md: "Two topology changes never run together:
//! one Build is in flight, and a second is refused `BuildInProgress { build_id }`"; R-ON8).
//!
//! The fabric-primary admits a Build at one place. The cells force the ordering: the first
//! submission is held inside its durable write, after it passed its check, and the second is
//! polled once while the first is held. Nothing here depends on timing.

use rdm_admin::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome, AttemptOpened, MemoryBuildStateAdapter,
};
use rdm_admin::control_plane::{ControlPlane, Refusal};
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn view() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node("mesh1.admin.1", true, true), node("mesh1.rpc.1", false, false)],
    }
}

/// The Build state, holding the FIRST acceptance inside its write until the cell releases it.
struct Held {
    inner: MemoryBuildStateAdapter,
    first: AtomicBool,
    entered: Notify,
    release: Notify,
    /// Armed: the fabric role moves off this admin the moment an acceptance is durable, as the accepted
    /// Build's own drive moves it (the approved proposal node-primary-update-fabric-mesh-and-node-type.md,
    /// Part C1 step 8, mesh-delete.md step 1), so every gated write after the acceptance is refused `RoleYielded`.
    moves_at_accept: AtomicBool,
    /// The role moved: every gated write is refused `RoleYielded`, as `fabric_builds.rs` `enter_naming`
    /// refuses once the written primary record names another primary.
    moved: AtomicBool,
}

impl Held {
    fn refused_once_moved(&self) -> Result<(), BuildStateError> {
        if self.moved.load(Ordering::SeqCst) {
            return Err(BuildStateError::RoleYielded { node: "mesh1.admin.1".into(), by: "the fabric-primary role record names mesh2 (revision 2)".into(), to: Some("mesh2.admin.1".into()) });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BuildStateAdapter for Held {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        if !self.first.swap(true, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.refused_once_moved()?;
        self.inner.publish_accepted(accepted).await?;
        if self.moves_at_accept.load(Ordering::SeqCst) {
            self.moved.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(opened).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(claim).await
    }
    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(claim).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.inner.append_step_receipt(receipt).await
    }
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &rdm_admin::build::BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
}

async fn rig() -> (Arc<Held>, Arc<ControlPlane>) {
    let t = view();
    let held = Arc::new(Held { inner: MemoryBuildStateAdapter::new(), first: AtomicBool::new(false), entered: Notify::new(), release: Notify::new(), moves_at_accept: AtomicBool::new(false), moved: AtomicBool::new(false) });
    // The seed is written before the hold is armed.
    held.first.store(true, Ordering::SeqCst);
    let accepted = AcceptedStore::seeded(&*held, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    held.first.store(false, Ordering::SeqCst);
    let mut cp = ControlPlane::new(held.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time());
    cp.topology = Arc::new(tokio::sync::RwLock::new(t));
    (held, Arc::new(cp))
}

fn add_rpc() -> TopologyChange {
    TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: rdm_core::NodeKind::RpcNode }
}

/// CONTRACT: two topology submissions that both arrive while the accepted Build is settled admit
/// exactly one Build. The first is held inside its durable write, past its check; the second is
/// polled once meanwhile. When the first is released, the second answers `BuildInProgress`
/// naming the first's build id, and the Build state holds one active Build.
#[tokio::test]
async fn two_concurrent_topology_submissions_admit_one_build_and_refuse_the_other_naming_it() {
    let (held, cp) = rig().await;
    let a = {
        let cp = cp.clone();
        tokio::spawn(async move { cp.submit("test-a", crate::named_request(&cp, add_rpc()).await).await })
    };
    held.entered.notified().await;
    let mut b = Box::pin({
        let cp = cp.clone();
        async move { cp.submit("test-b", crate::named_request(&cp, add_rpc()).await).await }
    });
    // One poll of the second while the first is held in its write.
    let early = tokio::select! {
        biased;
        r = &mut b => Some(r),
        _ = std::future::ready(()) => None,
    };
    held.release.notify_one();
    let a = a.await.unwrap();
    let b = match early {
        Some(r) => r,
        None => b.await,
    };
    let (ok, refused): (Vec<_>, Vec<_>) = [a, b].into_iter().partition(|r| r.is_ok());
    assert_eq!(ok.len(), 1, "exactly one submission is admitted: refused={refused:?}");
    let admitted = ok.into_iter().next().unwrap().unwrap().build_id;
    match refused.into_iter().next() {
        Some(Err(Refusal::BuildInProgress(running))) => assert_eq!(running.current_build_id, admitted, "the refusal names the running Build"),
        other => panic!("the second submission is refused BuildInProgress naming the first: {other:?}"),
    }
    let active = held.list_active().await.unwrap();
    assert_eq!(active.iter().map(|b| b.build_id.clone()).collect::<Vec<_>>(), vec![admitted], "one Build is active");
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 8: "a
/// planned role move mid-Build ends the caller's outer stream with the typed new-role frame", never a
/// refusal; http.rs: "The Build is current the moment it is durable in the builds store"): the door's
/// acceptance is its point of no return, and nothing it writes after it can be refused. The accepted
/// Build's drive starts from the reconcile loop the moment the Build is current (admin.rs `ensure_active`)
/// and may move the fabric-primary role at once (a delete of the fabric-primary's own mesh); here the role moves
/// the moment the acceptance is durable. The door answers the Build it accepted, which the Build log
/// holds as the current Build. A role that moved before the door wrote anything refuses the submission
/// `RoleYielded` with nothing accepted.
/// Must NOT happen: the door refusing a Build it accepted (the gate's red on 3355f190, build_op :439,
/// trace f0e5d40db922f0c6f1015502974a03be: `publish_fabric` after the accept refused `RoleYielded`).
#[tokio::test]
async fn a_primary_move_after_the_acceptance_leaves_the_door_answering_the_build_it_accepted() {
    let (held, cp) = rig().await;
    // No acceptance is held inside its write here.
    held.first.store(true, Ordering::SeqCst);
    held.moves_at_accept.store(true, Ordering::SeqCst);
    let opened = cp.submit("test-moved-at-accept", crate::named_request(&cp, add_rpc()).await).await.unwrap_or_else(|r| panic!("the door answers the Build it accepted, not {r:?}"));
    assert!(held.moved.load(Ordering::SeqCst), "the role moved at the acceptance");
    let active = held.list_active().await.unwrap();
    assert_eq!(active.iter().map(|b| b.build_id.clone()).collect::<Vec<_>>(), vec![opened.build_id], "the Build the door answered is the one accepted");

    let (held, cp) = rig().await;
    held.first.store(true, Ordering::SeqCst);
    held.moved.store(true, Ordering::SeqCst);
    match cp.submit("test-moved-before", crate::named_request(&cp, add_rpc()).await).await {
        Err(Refusal::State(BuildStateError::RoleYielded { to, .. })) => assert_eq!(to.as_deref(), Some("mesh2.admin.1"), "the refusal names the new role"),
        other => panic!("a role that moved before the door wrote anything refuses RoleYielded: {other:?}"),
    }
    assert!(held.list_active().await.unwrap().is_empty(), "nothing was accepted");
}
