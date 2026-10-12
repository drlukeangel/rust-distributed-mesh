//! One topology Build in flight (ops-naming-proposal.md: "Two topology changes never run together:
//! one Build is in flight, and a second is refused `BuildInProgress { build_id }`"; R-ON8).
//!
//! The fabric-primary admits a Build at one place. The cells force the ordering: the first
//! submission is held inside its durable write, after it passed its check, and the second is
//! polled once while the first is held. Nothing here depends on timing.

use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{
    BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome, AttemptOpened, MemoryBuildStateAdapter,
};
use rafka_node_admin_core::http::{ControlPlane, Refusal};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn view() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![node("mesh1.admin.1", true, true), node("mesh1.rpc.1", false, false)],
    }
}

/// The Build state, holding the FIRST acceptance inside its write until the cell releases it.
struct Held {
    inner: MemoryBuildStateAdapter,
    first: AtomicBool,
    entered: Notify,
    release: Notify,
}

#[async_trait::async_trait]
impl BuildStateAdapter for Held {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        if !self.first.swap(true, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.publish_accepted(accepted).await
    }
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(opened).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
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
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
}

async fn rig() -> (Arc<Held>, Arc<ControlPlane>) {
    let t = view();
    let held = Arc::new(Held { inner: MemoryBuildStateAdapter::new(), first: AtomicBool::new(false), entered: Notify::new(), release: Notify::new() });
    // The seed is written before the hold is armed.
    held.first.store(true, Ordering::SeqCst);
    let accepted = AcceptedStore::seeded(&*held, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    held.first.store(false, Ordering::SeqCst);
    let mut cp = ControlPlane::new(held.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time());
    cp.topology = Arc::new(tokio::sync::RwLock::new(t));
    (held, Arc::new(cp))
}

fn add_rpc() -> TopologyChange {
    TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: rafka_mesh_entity::NodeKind::RpcNode }
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
        tokio::spawn(async move { cp.submit("test-a", add_rpc()).await })
    };
    held.entered.notified().await;
    let mut b = Box::pin({
        let cp = cp.clone();
        async move { cp.submit("test-b", add_rpc()).await }
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
        Some(Err(Refusal::BuildInProgress(running))) => assert_eq!(running, admitted, "the refusal names the running Build"),
        other => panic!("the second submission is refused BuildInProgress naming the first: {other:?}"),
    }
    let active = held.list_active().await.unwrap();
    assert_eq!(active.iter().map(|b| b.build_id.clone()).collect::<Vec<_>>(), vec![admitted], "one Build is active");
}
