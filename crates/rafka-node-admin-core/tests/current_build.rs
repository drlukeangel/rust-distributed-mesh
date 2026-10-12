//! builds.storage functional: the current Build is read from the Build entities alone, last in,
//! first out (R-BI3, rulings 2026-10-12: "it reads the builds till it finds the last
//! committed/completed build id and then uses that as known state, because other builds could have
//! occurred and failed or be in progress").
//!
//! Each accepted Build is its own entity in the builds store, written once. The current Build is
//! the first, newest first, that is in progress or completed; a failed one is skipped and never
//! current. The Fabric row carries no pointer to the newest Build, so nothing but the Build
//! entities says which Build is current.

use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build::{BuildId, MeshDesired};
use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::http::{ControlPlane, Refusal};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use std::sync::Arc;

fn node(name: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    if n.kind == NodeKind::NodeAdmin {
        n.is_fabric_primary = primary;
    }
    n
}

fn observed() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![node("mesh1.admin.1", true), node("mesh1.admin.2", false), node("mesh1.rpc.1", true)],
    }
}

fn mesh(name: &str) -> MeshDesired {
    serde_json::from_value(serde_json::json!({"name": name, "node_admin": 1})).unwrap()
}

/// The fabric-primary `mesh1.admin.1` holding one settled Build (B0) of the observed topology.
async fn fabric_primary() -> (Arc<ControlPlane>, Arc<MemoryBuildStateAdapter>, BuildId) {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = observed();
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    let b0 = builds.list_active().await.unwrap();
    assert!(b0.is_empty(), "the seeded Build is settled: {b0:?}");
    let facts = builds.facts().await.unwrap();
    let b0 = facts.first().expect("the seeded Build's acceptance").build_id().clone();
    (cp, builds, b0)
}

async fn fail(builds: &MemoryBuildStateAdapter, id: &BuildId) {
    let attempt = builds.read_build(id).await.unwrap().attempt + 1;
    builds.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
    builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::Failed { reason: "the cell fails it".into() } }).await.unwrap();
}

/// CONTRACT (R-BI3: "failed Builds are skipped. A failed Build is never current"; "completed: it is
/// the known state"): a Build whose attempt failed is not the topology the next change compiles
/// against. B0 is complete, B1 (add mesh2) is accepted and fails; the next change (add mesh3)
/// compiles against B0, so the Build it makes holds mesh1 and mesh3 and never mesh2.
#[tokio::test]
async fn a_change_after_a_failed_build_compiles_against_the_newest_completed_build() {
    let (cp, builds, b0) = fabric_primary().await;
    let b1 = cp.submit("POST /api/meshes", TopologyChange::CreateMesh { desired: mesh("mesh2") }).await.expect("B1 is accepted").build_id;
    fail(&builds, &b1).await;
    let b2 = cp.submit("POST /api/meshes", TopologyChange::CreateMesh { desired: mesh("mesh3") }).await.expect("B2 is accepted over the completed B0, the failed B1 skipped").build_id;
    let meshes: Vec<String> = builds.read_build(&b2).await.unwrap().topology.meshes.keys().cloned().collect();
    assert_eq!(meshes, vec!["mesh1".to_string(), "mesh3".to_string()], "B2 compiled against B0 ({b0}), never the failed B1 ({b1})");
}

/// CONTRACT (R-BI3: "Each accepted Build is its own entity in the builds store, written once"; the
/// Fabric row carries no pointer to the newest Build): a Build is current once its entity is in the
/// builds store, with nothing else written. B1 is put straight into the store, newer than the
/// settled B0 and not converged: it is the Build in progress, and a change submitted now is refused
/// naming it (one Build in flight).
#[tokio::test]
async fn an_accepted_build_is_current_from_the_build_store_alone() {
    let (cp, builds, b0) = fabric_primary().await;
    let topology = builds.read_build(&b0).await.unwrap().topology;
    let b1 = BuildId::mint();
    builds.publish_accepted(&BuildAccepted { build_id: b1.clone(), topology, submitted_change: None, submitted_at_ms: 5 }).await.unwrap();
    match cp.submit("POST /api/meshes", TopologyChange::CreateMesh { desired: mesh("mesh2") }).await {
        Err(Refusal::BuildInProgress(id)) => assert_eq!(id, b1, "the refusal names the Build in progress"),
        Err(other) => panic!("refused, but not as the Build in progress {b1}: {other:?}"),
        Ok(o) => panic!("a change was accepted as {} while B1 ({b1}), newer than B0 ({b0}), is in progress", o.build_id),
    }
}
