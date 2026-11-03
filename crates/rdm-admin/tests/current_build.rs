//! builds.storage functional: the current Build is read from the Build entities alone, last in,
//! first out (R-BI3, rulings 2026-10-12: "it reads the builds till it finds the last
//! committed/completed build id and then uses that as known state, because other builds could have
//! occurred and failed or be in progress").
//!
//! Each accepted Build is its own entity in the builds store, written once. The current Build is
//! the first, newest first, that is in progress or completed; a failed one is skipped and never
//! current. The Fabric row carries no pointer to the newest Build, so nothing but the Build
//! entities says which Build is current.

use rdm_admin::accepted::{AcceptedStore, FabricTopology};
use rdm_admin::build::{BuildId, MeshDesired};
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::control_plane::{ControlPlane, Refusal};
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use std::sync::Arc;

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    if n.r#type == "node-admin" {
        n.is_fabric_primary = primary;
    }
    n
}

fn observed() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true), node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", false), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", true)],
    }
}

fn mesh(name: &str) -> MeshDesired {
    serde_json::from_value(serde_json::json!({"name": name, "counts": {"node-admin": 1}})).unwrap()
}

/// The fabric-primary `mesh1.node-admin.1` holding one settled Build (B0) of the observed topology.
async fn fabric_primary() -> (Arc<ControlPlane>, Arc<MemoryBuildStateAdapter>, BuildId) {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = observed();
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), &t.node(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).unwrap().birth().unwrap()).await.unwrap();
    let cp = Arc::new(crate::control_plane(builds.clone(), accepted, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), t, crate::adopted_time()));
    let b0 = builds.accepted_newest_first().await.unwrap().last().cloned().expect("the seeded Build's acceptance");
    assert_eq!(builds.read_build(&b0).await.unwrap().state, rdm_admin::build_state::BuildState::Complete, "the seeded Build is complete");
    (cp, builds, b0)
}

async fn fail(builds: &MemoryBuildStateAdapter, id: &BuildId, executor: &rdm_core::framing::Birth) {
    crate::end_build(builds, id, executor, rdm_admin::events_storage::EndedState::Failed { reason: "the cell fails it".into(), code: "FAILED_PRECONDITION".into() }).await;
}

/// CONTRACT (R-BI3: "failed Builds are skipped. A failed Build is never current"; "completed: it is
/// the known state"): a Build that ended failed is not the topology the next change compiles
/// against. B0 is complete, B1 (add mesh2) is accepted and fails; the next change (add mesh3)
/// compiles against B0, so the Build it makes holds mesh1 and mesh3 and never mesh2.
#[tokio::test]
async fn a_change_after_a_failed_build_compiles_against_the_newest_completed_build() {
    let (cp, builds, b0) = fabric_primary().await;
    let b1 = cp.submit("POST /api/meshes", crate::named_request(&cp, move |t: &rdm_admin::topology::Topology| crate::grown(t, &[mesh("mesh2")])).await).await.expect("B1 is accepted").build_id;
    fail(&builds, &b1, &cp.topology.read().await.node(&cp.me()).unwrap().birth().unwrap()).await;
    let b2 = cp.submit("POST /api/meshes", crate::named_request(&cp, move |t: &rdm_admin::topology::Topology| crate::grown(t, &[mesh("mesh3")])).await).await.expect("B2 is accepted over the completed B0, the failed B1 skipped").build_id;
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
    builds.publish_accepted(&BuildAccepted { build_id: b1.clone(), topology, change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 5 }).await.unwrap();
    match cp.submit("POST /api/meshes", crate::named_request(&cp, move |t: &rdm_admin::topology::Topology| crate::grown(t, &[mesh("mesh2")])).await).await {
        Err(Refusal::BuildInProgress(held)) => assert_eq!(held.current_build_id, b1, "the refusal names the Build in progress"),
        Err(other) => panic!("refused, but not as the Build in progress {b1}: {other:?}"),
        Ok(o) => panic!("a change was accepted as {} while B1 ({b1}), newer than B0 ({b0}), is in progress", o.build_id),
    }
}
