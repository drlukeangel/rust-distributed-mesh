//! The gates before an operation (build.md §4.4): every operation on the topology, a Build or a
//! standalone node.stop, node.start, node.connection.delete or node.delete, passes the
//! fabric-primary's gates in the page's order before anything runs; each gate reads one source, and a
//! gate that fails refuses by name and nothing is written.
//!
//! Each cell drives the fabric-primary's control plane in process over the memory Build store, with
//! the view set by the cell. Nothing here depends on timing.

use rdm_admin::accepted::{AcceptedStore, FabricTopology};
use rdm_admin::build::BuildReject;

use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::control_plane::{ControlPlane, Refusal};
use rdm_admin::model::*;
use rdm_admin::node_investigation::Finding;
use rdm_admin::topology::Topology;
use std::sync::Arc;

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, status: NodeStatus, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = status;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// Two meshes: mesh1 (the fabric-primary's) with an admin and two nodes, rpc.1 in `rpc1`, and
/// mesh2 with its admin.
fn view(rpc1: NodeStatus) -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![
            Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
            Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
        ],
        nodes: vec![
            node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", NodeStatus::Ready, true, true),
            node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", rpc1, false, false),
            node(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", NodeStatus::Ready, false, false),
            node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", NodeStatus::Ready, true, false),
        ],
    }
}

fn executor(t: &Topology) -> rdm_core::framing::Birth {
    let n = t.nodes.iter().find(|n| n.is_fabric_primary).unwrap();
    rdm_core::framing::Birth { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap() }
}

struct Rig {
    builds: Arc<MemoryBuildStateAdapter>,
    cp: Arc<ControlPlane>,
}

async fn rig(t: Topology) -> Rig {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), &executor(&t)).await.unwrap();
    let mut cp = crate::control_plane(builds.clone(), accepted, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), t.clone(), crate::adopted_time());
    // The node records a create writes: the gates read a birth from its record, never from the gossip view (Q207).
    crate::bind_records(&cp, &[&t]).await;
    cp.topology = Arc::new(tokio::sync::RwLock::new(t));
    Rig { builds, cp: Arc::new(cp) }
}

fn reason(r: &Result<rdm_admin::control_plane::Opened, Refusal>) -> String {
    match r {
        Ok(o) => format!("admitted as {}", o.build_id),
        Err(Refusal::Reject(j)) => j.reason().to_string(),
        Err(Refusal::BuildInProgress(h)) => format!("build-in-progress ({})", h.current_build_id),
        Err(other) => format!("{other:?}"),
    }
}

/// Node investigation as the cells set it (node-investigation.md): the provider reads the one exact
/// birth `exited` names (node id and incarnation) exited; every other birth is unproven.
struct Investigation {
    exited: Option<(NodeId, IncarnationId)>,
}

#[async_trait::async_trait]
impl rdm_admin::path_investigation::PathProbe for Investigation {
    async fn investigate(&self, node: &Node) -> Finding {
        match &self.exited {
            Some((id, incarnation)) if *id == node.node_id && Some(incarnation) == node.incarnation_id.as_ref() => Finding::Exited { code: Some(137) },
            _ => Finding::Unproven { reason: "the provider holds no exited runtime of this exact birth".into() },
        }
    }
}

/// The gates rig over `t`, whose accepted topology names mesh1.node.1, with the fabric-primary's view
/// then marking that birth Dead: silence, the same birth (node id and incarnation) unheard.
async fn dead_rig(t: Topology) -> Rig {
    let r = rig(t).await;
    let path: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
    r.cp.topology.write().await.nodes.iter_mut().find(|n| n.location() == path).unwrap().status = NodeStatus::Dead;
    r
}

fn birth_of(t: &Topology, path: &PathName) -> (NodeId, IncarnationId) {
    let n = t.node(path).unwrap();
    (n.node_id.clone(), n.incarnation_id.clone().unwrap())
}

async fn delete_of(r: &Rig, path: &PathName) -> Result<rdm_admin::control_plane::Opened, Refusal> {
    r.cp.submit("gates-delete", crate::named_request(&r.cp, move |t: &rdm_admin::topology::Topology| crate::removed(t, &[rdm_core::PathName { path: path.path.clone(), name: path.name.clone() }])).await).await
}

/// CONTRACT (build.md §4.4 gate 5: "a delete a stopped one, one whose exit is proven, or a live one";
/// node-delete.md Never evidence: "Gossip, silence, Ping failure and timeout never say the runtime
/// exited"; Q219): a birth the view marks Dead (silence) that is neither stopped nor has the node
/// investigation's `exited` verdict fails gate 5 `node-not-live`, with no investigation held and
/// with one that cannot say it exited.
/// Must NOT happen: a silence-derived Dead birth admitted as live or as exited.
#[tokio::test]
async fn a_dead_birth_with_no_exited_verdict_is_refused_node_not_live() {
    let r = dead_rig(view(NodeStatus::Ready)).await;
    let before = r.builds.current().await.unwrap().map(|b| b.build_id);
    let out = delete_of(&r, &PathName::new("mesh1.node", "1").unwrap()).await;
    assert!(matches!(&out, Err(Refusal::Reject(BuildReject::NodeNotLive { .. }))), "no investigation is held: refused node-not-live, not {}", reason(&out));
    let _ = r.cp.investigation.set(Arc::new(Investigation { exited: None }));
    let out = delete_of(&r, &PathName::new("mesh1.node", "1").unwrap()).await;
    assert!(matches!(&out, Err(Refusal::Reject(BuildReject::NodeNotLive { .. }))), "the investigation's verdict is unproven: refused node-not-live, not {}", reason(&out));
    assert_eq!(r.builds.current().await.unwrap().map(|b| b.build_id), before, "nothing is accepted");
}

/// CONTRACT (build.md §4.4 gate 5, "one whose exit is proven"; node-delete.md Crashed target; Q219:
/// "The exact runtime's Exited admits it"): a Dead birth whose exact runtime the node investigation
/// reads exited is admitted, and its delete carries that birth.
/// Must NOT happen: the exited birth refused, or its delete planned for another birth.
#[tokio::test]
async fn the_exact_runtimes_exited_verdict_admits_a_dead_births_delete() {
    let t = view(NodeStatus::Ready);
    let r = dead_rig(t.clone()).await;
    let _ = r.cp.investigation.set(Arc::new(Investigation { exited: Some(birth_of(&t, &PathName::new("mesh1.node", "1").unwrap())) }));
    let out = delete_of(&r, &PathName::new("mesh1.node", "1").unwrap()).await;
    let opened = out.unwrap_or_else(|e| panic!("the exited birth's delete is admitted, not {}", reason(&Err(e))));
    let build = r.builds.read_build(&opened.build_id).await.unwrap();
    let (node_id, incarnation) = birth_of(&t, &PathName::new("mesh1.node", "1").unwrap());
    assert!(build.topology.pinned.iter().any(|(p, b)| *p == PathName::new("mesh1.node", "1").unwrap() && b.node_id == node_id && b.incarnation == incarnation), "the delete is pinned to the exited birth: {:?}", build.topology.pinned);
}

/// CONTRACT (Q219: "Another birth's exit evidence admits nothing"): the investigation reads exited a
/// runtime of the same node id under another incarnation, or of another node id under the same
/// incarnation; the Dead birth at the path is not that runtime, and its delete is refused
/// `node-not-live`.
/// Must NOT happen: exit evidence matched on the node id alone or the incarnation alone.
#[tokio::test]
async fn another_births_exit_evidence_admits_nothing() {
    let t = view(NodeStatus::Ready);
    let (node_id, incarnation) = birth_of(&t, &PathName::new("mesh1.node", "1").unwrap());
    for other in [(node_id.clone(), IncarnationId::mint()), (NodeId::mint(), incarnation.clone())] {
        let r = dead_rig(t.clone()).await;
        let _ = r.cp.investigation.set(Arc::new(Investigation { exited: Some(other) }));
        let out = delete_of(&r, &PathName::new("mesh1.node", "1").unwrap()).await;
        assert!(matches!(&out, Err(Refusal::Reject(BuildReject::NodeNotLive { .. }))), "another birth's exit admits nothing: refused node-not-live, not {}", reason(&out));
    }
}

/// CONTRACT (build.md §4.4 gate 5, "or a live one, which its own sequence stops first (node.delete
/// Live target)"; Q220): a live birth's delete is admitted and planned as node.delete of that birth,
/// whose sequence composes node.stop before TerminateRuntime (`node_delete_pipeline`'s order); no
/// investigation is asked.
/// Must NOT happen: a live birth refused, or planned as the delete of a stopped birth, which skips its stop.
#[tokio::test]
async fn a_live_births_delete_is_admitted_and_planned_to_stop_it_first() {
    let t = view(NodeStatus::Ready);
    let r = rig(t.clone()).await;
    let out = delete_of(&r, &PathName::new("mesh1.node", "1").unwrap()).await;
    let opened = out.unwrap_or_else(|e| panic!("the live birth's delete is admitted, not {}", reason(&Err(e))));
    let build = r.builds.read_build(&opened.build_id).await.unwrap();
    let ops = rdm_admin::accepted::plan_for_build(&build, &t).operations;
    let delete = ops.iter().find(|op| op.key() == rdm_core::OperationId::node("node.delete", &birth_of(&t, &PathName::new("mesh1.node", "1").unwrap()).0)).unwrap_or_else(|| panic!("the delete is planned: {ops:?}"));
    assert!(matches!(delete, rdm_admin::build::BuildOperation::DeleteLiveNode { exited: None, birth: Some(_), .. }), "the live birth's delete composes its stop: {delete:?}");
}

/// CONTRACT (build.md §4.4 gate 6: "The node's mesh is heard | the fabric-primary's view | refused by
/// name, naming the mesh and how long it has gone unheard (R-D1)"; Q221 X: "Gate 6 runs at acceptance
/// for every ordinary Build"): a node.delete Build on a node of mesh2, which the fabric-primary's
/// investigation holds unheard, is refused at acceptance naming mesh2 and how long it is unheard, and
/// nothing is accepted.
/// Must NOT happen: the Build accepted and held only later, at its drive.
#[tokio::test]
async fn a_build_on_a_node_of_an_unheard_mesh_is_refused_at_acceptance() {
    let mut t = view(NodeStatus::Ready);
    t.nodes.push(node(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", NodeStatus::Ready, false, false));
    let r = rig(t).await;
    let _ = r.cp.mesh_unheard.set(Arc::new(|m: &str| (m == "mesh2").then_some(40_000)));
    let before = r.builds.current().await.unwrap().map(|b| b.build_id);
    let out = delete_of(&r, &PathName::new("mesh2.node", "1").unwrap()).await;
    match &out {
        Err(Refusal::Reject(BuildReject::UnheardMesh { mesh, unheard_ms, .. })) => assert_eq!((mesh.as_str(), *unheard_ms), ("mesh2", 40_000)),
        _ => panic!("refused naming mesh2 and how long it is unheard, not {}", reason(&out)),
    }
    assert_eq!(r.builds.current().await.unwrap().map(|b| b.build_id), before, "nothing is accepted");
}
