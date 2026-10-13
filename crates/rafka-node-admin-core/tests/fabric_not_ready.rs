//! The Build door's one admission rule before the initial fabric is ready-for-traffic (states.md, "The
//! initial fabric before ready-for-traffic"; node-rpc-envelope.md, "Build refusals"; R-PB1, R-PC1).
//!
//! Until the initial fabric first reaches ready-for-traffic, its Fabric row, each mesh's Mesh row and the
//! members' Node rows live only in their holders' scratchpads. Nothing that moves a seat or writes a
//! caller's row runs: node.stop, node.start, node.restart, node.delete, the replace recipe, a drain of a
//! seat holder and mesh.delete are refused `fabric-not-ready` at the one place a Build is admitted, with
//! no attempt opened and no Build accepted. The initial accepted Build, and growth, run as usual. Every
//! cell reads the Build log it decides from: nothing here depends on timing.

use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build::{BuildReject, FabricDesired, MeshDesired};
use rafka_node_admin_core::build_state::{AttemptReason, BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::http::{ActionKind, ControlPlane, Refusal};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc_contract::build::BuildReply;
use std::sync::Arc;

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n
}

/// Two meshes of two node-admins and one rpc node each. `mesh1.admin.1` holds the fabric seat and
/// mesh1's, `mesh2.admin.1` holds mesh2's. The fabric is in state-sync.
fn view(status: ScopeStatus) -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status, provider: ProviderKind::Process },
        meshes: ["mesh1", "mesh2"].iter().map(|m| Mesh { id: Some(MeshId::mint()), name: m.to_string(), status: ScopeStatus::ReadyForTraffic }).collect(),
        nodes: vec![
            node("mesh1.admin.1", true, true),
            node("mesh1.admin.2", false, false),
            node("mesh1.rpc.1", true, false),
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
/// fabric has not first reached ready-for-traffic.
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
    async fn attempt(&self, reason: AttemptReason, path: &str, kind: ActionKind) -> Result<rafka_node_admin_core::http::Opened, Refusal> {
        self.cp.open_attempt("test", reason, path.parse().unwrap(), kind, None).await
    }
}

/// The refusal's workflow and status, or a panic naming what came back.
fn refused(r: Result<rafka_node_admin_core::http::Opened, Refusal>) -> (String, String) {
    match r {
        Err(Refusal::Reject(BuildReject::FabricNotReady { workflow, status })) => (workflow, status),
        other => panic!("expected the fabric-not-ready refusal, got {other:?}"),
    }
}

/// CONTRACT (R-PC1): until the initial fabric has first reached ready-for-traffic, a caller's node.stop,
/// node.start, node.restart, node.delete of a parked `.old` and replace are each refused
/// `fabric-not-ready`, whatever the node's own state: the refusal names the workflow and its node and
/// the fabric's status (state-sync), no attempt is opened, and no Build fact is written. One
/// `rdm.node_admin.build.reject.via-fabric-not-ready` span per refusal says the same.
/// What must NOT happen: an attempt opened for a workflow that writes a row the successor cannot read.
#[tokio::test]
async fn a_caller_node_workflow_before_the_fabric_first_reaches_ready_for_traffic_is_refused_by_name_and_opens_no_attempt() {
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-s23-node-workflows");
    let r = rig(ScopeStatus::StateSync).await;
    let before = r.facts().await;
    let cases = [
        (AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop, "node.stop mesh1.rpc.1"),
        (AttemptReason::Start, "mesh1.rpc.1", ActionKind::Start, "node.start mesh1.rpc.1"),
        (AttemptReason::Restart, "mesh1.rpc.1", ActionKind::Restart, "node.restart mesh1.rpc.1"),
        (AttemptReason::Replace, "mesh1.rpc.1", ActionKind::Replace, "node.replace mesh1.rpc.1"),
        (AttemptReason::Delete, "mesh1.rpc.1.old", ActionKind::Delete, "node.delete mesh1.rpc.1.old"),
    ];
    for (reason, path, kind, workflow) in cases {
        let (named, status) = refused(r.attempt(reason, path, kind).await);
        assert_eq!((named.as_str(), status.as_str()), (workflow, "state-sync"), "{workflow}");
    }
    assert_eq!(r.facts().await, before, "no attempt is opened and nothing is accepted by a refused workflow");
    let spans = capture.spans("rdm.node_admin.build.reject.via-fabric-not-ready");
    assert_eq!(spans.len(), cases.len(), "one refusal span per workflow: {spans:?}");
    for ((_, a), (_, _, _, workflow)) in spans.iter().zip(cases) {
        assert_eq!((a.get("workflow").map(String::as_str), a.get("status").map(String::as_str)), (Some(workflow), Some("state-sync")), "{a:?}");
    }
    eprintln!("trace {}: rdm.node_admin.build.reject.via-fabric-not-ready x{} (stop, start, restart, replace, delete) status=state-sync", spans[0].0, spans.len());
}

/// CONTRACT (R-PB1): a node.connections.delete moves a seat when its node holds the fabric-primary or
/// its mesh's primary seat. Until the initial fabric has first reached ready-for-traffic that
/// node.connections.delete is refused `fabric-not-ready`, naming the node; one of a node that holds no
/// seat is admitted (it moves none).
/// What must NOT happen: `TakeSeats` sent for a seat whose rows only its holder's scratchpad holds.
#[tokio::test]
async fn a_connections_delete_of_a_seat_holder_before_the_fabric_first_reaches_ready_for_traffic_is_refused_and_one_of_another_node_is_admitted() {
    let r = rig(ScopeStatus::StateSync).await;
    let before = r.facts().await;
    for holder in ["mesh1.admin.1", "mesh2.admin.1"] {
        let (workflow, status) = refused(r.attempt(AttemptReason::DeleteConnections, holder, ActionKind::DeleteConnections).await);
        assert_eq!((workflow.as_str(), status.as_str()), (format!("node.connections.delete {holder} (a seat holder)").as_str(), "state-sync"));
    }
    assert_eq!(r.facts().await, before, "a refused node.connections.delete opens no attempt");
    let opened = r.attempt(AttemptReason::DeleteConnections, "mesh1.admin.2", ActionKind::DeleteConnections).await.expect("a node that holds no seat has its connections deleted");
    assert_eq!(opened.attempt, 2);
    assert_eq!(r.facts().await, before + 1, "the admitted node.connections.delete opened its one attempt");
}

/// CONTRACT (R-PC1, R-PB1): until the initial fabric has first reached ready-for-traffic, mesh.delete and
/// node.delete are refused `fabric-not-ready` in every shape a Build takes them: `RemoveMesh`,
/// `RemoveNode`, a `ReconcileFabric` that drops a mesh and a `ReconcileMesh` that drops a node. Each
/// names the workflow and the node or mesh it deletes; no Build is accepted and the current Build
/// does not move. What must NOT happen: a Deleting row written to a scratchpad whose primary is then lost.
#[tokio::test]
async fn a_mesh_delete_or_node_delete_before_the_fabric_first_reaches_ready_for_traffic_is_refused_in_every_shape_and_accepts_no_build() {
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-s23-deletes");
    let r = rig(ScopeStatus::StateSync).await;
    let before = r.facts().await;
    let current = r.builds.current_build().await.unwrap();
    let mesh1 = || MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 1)]);
    let cases: Vec<(TopologyChange, &str)> = vec![
        (TopologyChange::RemoveMesh { mesh: "mesh2".into() }, "mesh.delete mesh2"),
        (TopologyChange::RemoveNode { node: "mesh1.rpc.1".parse().unwrap() }, "node.delete mesh1.rpc.1"),
        (TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1()] } }, "mesh.delete mesh2"),
        (TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2)]) }, "node.delete mesh1.rpc.1"),
    ];
    for (change, workflow) in cases {
        match r.cp.submit("test", change.clone()).await {
            Err(Refusal::Reject(BuildReject::FabricNotReady { workflow: named, status })) => assert_eq!((named.as_str(), status.as_str()), (workflow, "state-sync"), "{change:?}"),
            other => panic!("{change:?} was not refused fabric-not-ready: {other:?}"),
        }
    }
    assert_eq!(r.facts().await, before, "no Build is accepted");
    assert_eq!(r.builds.current_build().await.unwrap(), current, "the current Build does not move");
    let spans = capture.spans("rdm.node_admin.build.reject.via-fabric-not-ready");
    assert_eq!(spans.len(), 4, "{spans:?}");
    eprintln!("trace {}: rdm.node_admin.build.reject.via-fabric-not-ready x4 (RemoveMesh, RemoveNode, ReconcileFabric drop, ReconcileMesh drop) status=state-sync", spans[0].0);
}

/// CONTRACT (R-PC1: "the initial accepted Build, which creates the fabric's nodes and brings it to
/// ready-for-traffic, runs as usual"): while the fabric is pending or in state-sync, a `ReconcileFabric`
/// that grows a mesh, an `AddNode` and a `CreateMesh` are each accepted as the next Build.
/// What must NOT happen: the initial climb refused by the rule that guards it.
#[tokio::test]
async fn the_initial_accepted_build_and_growth_are_admitted_before_the_fabric_first_reaches_ready_for_traffic() {
    for status in [ScopeStatus::Pending, ScopeStatus::StateSync, ScopeStatus::StateCommit] {
        let grown = |rpc: u32| FabricDesired {
            fabric: "fabric1".into(),
            meshes: ["mesh1", "mesh2"].iter().map(|m| MeshDesired::of(m.to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, rpc)])).collect(),
        };
        for change in [
            TopologyChange::ReconcileFabric { desired: grown(3) },
            TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: rafka_mesh_entity::NodeKind::RpcNode },
            TopologyChange::CreateMesh { desired: MeshDesired::of("mesh3".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 1)]) },
        ] {
            let r = rig(status).await;
            let before = r.facts().await;
            let opened = r.cp.submit("test", change.clone()).await.unwrap_or_else(|e| panic!("{change:?} at {status:?} was refused: {e:?}"));
            assert_eq!(opened.attempt, 1, "{change:?}");
            assert_eq!(r.facts().await, before + 1, "{change:?}: one accepted Build");
        }
    }
}

/// CONTRACT (states.md: "Once the fabric is ready-for-traffic these are admitted as usual"; "A resize or
/// roll puts the fabric back to pending"): once the fabric has first reached ready-for-traffic a
/// workflow is admitted, and stays admitted when a resize then puts the fabric back to pending or
/// state-sync: the rows it published to the store root are no longer only in a scratchpad.
/// What must NOT happen: a node.stop refused during a resize climb.
#[tokio::test]
async fn a_workflow_is_admitted_once_the_fabric_has_reached_ready_for_traffic_and_stays_admitted_through_a_resize() {
    let r = rig(ScopeStatus::StateSync).await;
    refused(r.attempt(AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop).await);
    r.cp.first_ready.reach("the test fabric reached ready-for-traffic");
    r.cp.topology.write().await.fabric.status = ScopeStatus::Pending;
    let opened = r.attempt(AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop).await.expect("admitted once the fabric has been ready-for-traffic");
    assert_eq!(opened.attempt, 2);
}

/// CONTRACT (states.md "The initial fabric before ready-for-traffic"; status_storage.rs: a Fabric event is a
/// keyed row of its mesh's status rows): an admin that boots reads the one durable fact that the fabric
/// first reached ready-for-traffic, the ready-for-traffic event's row of its fabric among its mesh's status
/// rows. On a root where the fabric is in state-commit, the Fabric row published and no such row, the
/// admin refuses a caller's workflow `fabric-not-ready`; another fabric's row, or a different event's, is
/// not the fact. Once the row stands the admin admits the same workflow, though the fabric it hears is pending.
/// What must NOT happen: a boot in state-commit admitting, or a boot after the row refusing.
#[tokio::test]
async fn an_admin_booting_on_a_root_in_state_commit_refuses_and_one_booting_after_the_ready_for_traffic_row_admits() {
    use rafka_node_admin_core::status_storage::{FabricEventRow, MemoryStatusStorage, StatusStorage};
    let r = rig(ScopeStatus::StateCommit).await;
    let fabric = r.cp.topology.read().await.fabric.id.clone();
    let status = MemoryStatusStorage::default();

    // The root: a different fabric's ready-for-traffic row and this fabric's other event.
    status.put_fabric_event(&FabricEventRow { fabric_id: FabricId::mint(), event: "ready-for-traffic".into() }).await.unwrap();
    status.put_fabric_event(&FabricEventRow { fabric_id: fabric.clone(), event: "shutdown-initiated:mesh1.admin.1".into() }).await.unwrap();
    r.cp.first_ready.read_from(&status, &fabric).await.unwrap();
    let (workflow, at) = refused(r.attempt(AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop).await);
    assert_eq!((workflow.as_str(), at.as_str()), ("node.stop mesh1.rpc.1", "state-commit"), "no row of this fabric's ready-for-traffic: the fabric has not first reached it");

    // The ready-for-traffic row of the fabric stands: the next boot, hearing pending, knows.
    let booted = rig(ScopeStatus::Pending).await;
    let fabric = booted.cp.topology.read().await.fabric.id.clone();
    status.put_fabric_event(&FabricEventRow { fabric_id: fabric.clone(), event: "ready-for-traffic".into() }).await.unwrap();
    booted.cp.first_ready.read_from(&status, &fabric).await.unwrap();
    let opened = booted.attempt(AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop).await.expect("admitted: the row says the fabric first reached ready-for-traffic");
    assert_eq!(opened.attempt, 2);
}

/// CONTRACT (node-rpc-envelope.md, "Build refusals": "the existing `Rejected { reason, detail }` reply with
/// reason `fabric-not-ready`, whose `detail` names the refused workflow and the fabric's current
/// status"): the refusal the Build door answers is `Rejected` with that reason, and its detail names
/// both, for a node workflow the door opens as an attempt (node.stop) and for a change it submits as a
/// Build (mesh.delete).
/// What must NOT happen: `NotReady`, which stays the serving door's answer.
#[tokio::test]
async fn the_build_door_answers_the_refusal_as_rejected_fabric_not_ready_naming_the_workflow_and_the_status() {
    let reject = BuildReject::FabricNotReady { workflow: "mesh.delete mesh2".into(), status: "state-sync".into() };
    match rafka_node_admin_core::build_op::refusal_reply(Refusal::Reject(reject)) {
        BuildReply::Rejected { reason, detail } => {
            assert_eq!(reason, "fabric-not-ready");
            assert!(detail.contains("mesh.delete mesh2") && detail.contains("state-sync"), "the detail names the workflow and the status: {detail}");
        }
        other => panic!("expected Rejected: {other:?}"),
    }
    // The two ways a change reaches the door's admission (build_op.rs `admit_change`): a node workflow
    // opens an attempt of the accepted Build, a topology change is submitted as a Build. Each refusal is
    // answered through `refusal_reply`, as the door answers it.
    let r = rig(ScopeStatus::StateSync).await;
    let stop = r.attempt(AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop).await.map(|o| o.attempt);
    let delete = r.cp.submit("build.create", TopologyChange::RemoveMesh { mesh: "mesh2".into() }).await.map(|o| o.attempt);
    for (refused, workflow) in [(stop, "node.stop mesh1.rpc.1"), (delete, "mesh.delete mesh2")] {
        let refusal = refused.expect_err(&format!("{workflow} is refused before the fabric first reaches ready-for-traffic"));
        match rafka_node_admin_core::build_op::refusal_reply(refusal) {
            BuildReply::Rejected { reason, detail } => {
                assert_eq!(reason, "fabric-not-ready", "{workflow}: {detail}");
                assert!(detail.contains(workflow) && detail.contains("state-sync"), "{workflow}: the detail names the workflow and the status: {detail}");
            }
            other => panic!("{workflow}: expected Rejected: {other:?}"),
        }
    }
}
