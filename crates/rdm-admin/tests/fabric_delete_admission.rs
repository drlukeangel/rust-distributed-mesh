//! fabric.delete at the Build door before the initial fabric is ready (build.md §1: "The
//! one exception is fabric.delete, which may interrupt the Build in progress because its target is
//! nothing"; fabric-delete.md "Precondition"). Every cell reads the Build log it decides from: nothing
//! here depends on timing.

use rdm_admin::accepted::{AcceptedStore, FabricTopology};
use rdm_core::build::BuildChange;
use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::control_plane::{ControlPlane, Refusal};
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use std::sync::Arc;

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n
}

/// Two meshes of two node-admins each, two nodes in mesh1 and one in mesh2. `mesh1.node-admin.1` holds
/// the fabric-primary role and mesh1's, `mesh2.node-admin.1` holds mesh2's, `mesh1.node.1` mesh1's rpc role with
/// `mesh1.node.2` its secondary: a stop, restart or replace of `mesh1.node.1` moves its role by
/// node.primary.update and is not refused for want of a secondary (proposal Part C1, Q7). The fabric
/// is pending.
fn view(status: ScopeStatus) -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status, provider: ProviderKind::Process },
        meshes: ["mesh1", "mesh2"].iter().map(|m| Mesh { id: Some(MeshId::mint()), name: m.to_string(), status: ScopeStatus::Ready }).collect(),
        nodes: vec![
            node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true),
            node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", false, false),
            node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", true, false),
            node(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", false, false),
            node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false),
            node(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", false, false),
            node(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", true, false),
        ],
    }
}

struct Rig {
    executor: rdm_core::framing::Birth,
    cp: Arc<ControlPlane>,
    builds: Arc<MemoryBuildStateAdapter>,
}

/// This admin (`mesh1.node-admin.1`, the fabric-primary) holds a settled accepted Build of the view; the
/// fabric has not first reached ready.
async fn rig(status: ScopeStatus) -> Rig {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(status);
    let admin = t.nodes.iter().find(|n| n.is_fabric_primary).unwrap();
    let executor = rdm_core::framing::Birth { node_id: admin.node_id.clone(), incarnation: admin.incarnation_id.clone().unwrap() };
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), &executor).await.unwrap();
    let cp = Arc::new(crate::control_plane(builds.clone(), accepted, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), t, crate::adopted_time()));
    Rig { executor, cp, builds }
}

impl Rig {
    async fn facts(&self) -> usize {
        self.builds.accepted_newest_first().await.unwrap().len()
    }
}

/// A run of `build` by this admin began: its `rdm.build.started`.
async fn running(r: &Rig, build: &rdm_admin::build::BuildId) {
    crate::start_build(&*r.builds, build, &r.executor).await;
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
    let grow = r.cp.submit("test", crate::named_request(&r.cp, move |t: &rdm_admin::topology::Topology| crate::grown_by_one(t, "mesh1", "node")).await).await.expect("growth is admitted before the first ready");
    running(&r, &grow.build_id).await;
    assert_eq!(r.builds.current_build().await.unwrap(), rdm_admin::build_state::CurrentBuild::InProgress(grow.build_id.clone()), "the growth Build is in progress");
    let view = r.cp.topology.read().await.clone();
    for change in [BuildChange::Delete { meshes: vec!["mesh2".into()], node_ids: Vec::new() }, crate::removed(&view, &[rdm_core::PathName::new("mesh1.node", "1").unwrap()])] {
        match r.cp.submit("test", change.clone()).await {
            Err(Refusal::BuildInProgress(_)) => {}
            other => panic!("the standalone {change:?} was not refused build-in-progress: {other:?}"),
        }
    }
    let before = r.facts().await;
    let delete = r.cp.submit("test", BuildChange::DeleteFabric).await.unwrap_or_else(|e| panic!("fabric.delete is admitted before the first ready while a Build is in progress: {e:?}"));
    assert_ne!(delete.build_id, grow.build_id, "the delete is a Build of its own");
    assert_eq!(r.facts().await, before + 1, "the delete's acceptance is the one record written");
    assert_eq!(r.builds.current_build().await.unwrap(), rdm_admin::build_state::CurrentBuild::InProgress(delete.build_id.clone()), "the delete is the current Build from its acceptance");
    match r.cp.submit("test", crate::named_request(&r.cp, move |_: &rdm_admin::topology::Topology| rdm_core::build::BuildChange::Delete { meshes: vec!["mesh2".into()], node_ids: Vec::new() }).await).await {
        Err(Refusal::BuildInProgress(_)) => {}
        other => panic!("a standalone mesh.delete beside the delete was not refused build-in-progress: {other:?}"),
    }
}

/// CONTRACT (build.md §1: "a successor fabric-primary reads the delete as the current Build and continues
/// the teardown"; "An operation that fails to end within its bound fails the delete by name, the intent
/// retained"; "No other Build ends another"): a second fabric.delete while the first is in progress
/// answers the first and accepts nothing; once the first ended incomplete, a fabric.delete resumes it and
/// still accepts nothing. What must NOT happen: a second delete Build accepted, interrupting the first.
#[tokio::test]
async fn a_second_fabric_delete_resumes_the_first_in_progress_or_failed_and_accepts_no_build() {

    let r = rig(ScopeStatus::Pending).await;
    let first = r.cp.submit("test", BuildChange::DeleteFabric).await.unwrap_or_else(|e| panic!("fabric.delete is admitted: {e:?}"));
    let before = r.facts().await;
    let again = r.cp.submit("test", BuildChange::DeleteFabric).await.unwrap_or_else(|e| panic!("a second fabric.delete resumes the first: {e:?}"));
    assert_eq!(again.build_id, first.build_id, "the second delete answers the first");
    assert_eq!(r.facts().await, before, "nothing is accepted");
    running(&r, &first.build_id).await;
    // A failed step ends its section and the Build ends incomplete (Q346).
    let source = rdm_admin::events_storage::EventSource { fabric_id: crate::cell_fabric(), node_id: r.executor.node_id.clone(), incarnation: r.executor.incarnation.clone() };
    let incomplete = rdm_admin::events_storage::EndedState::Incomplete { sections: vec![rdm_admin::events_storage::EndedSection { mesh: "mesh1".into(), op: "mesh.delete:x".into(), step: "MeshDeleted".into(), reason: "a member did not end within its bound".into(), code: "DEADLINE_EXCEEDED".into() }] };
    r.builds.events().append(&rdm_admin::events_storage::Event::build_ended(&source, 2, &first.build_id, &incomplete)).await.unwrap();
    let before = r.facts().await;
    let resumed = r.cp.submit("test", BuildChange::DeleteFabric).await.unwrap_or_else(|e| panic!("a fabric.delete after the first ended incomplete resumes it: {e:?}"));
    assert_eq!(resumed.build_id, first.build_id, "the incomplete delete is resumed");
    assert_eq!(r.facts().await, before, "nothing is accepted");
}
