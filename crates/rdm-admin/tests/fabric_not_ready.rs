//! The Build door's one admission rule before the initial fabric is ready-for-traffic (states.md, "The
//! initial fabric before ready-for-traffic"; node-rpc-envelope.md, "Build refusals"; R-PB1, R-PC1).
//!
//! Until the initial fabric first reaches ready-for-traffic, its Fabric row, each mesh's Mesh row and the
//! members' Node rows live only in their holders' scratchpads. Nothing that moves a seat or writes a
//! caller's row runs: node.stop, node.start, node.restart, node.delete, the replace recipe, a drain of a
//! seat holder and mesh.delete are refused `fabric-not-ready` at the one place a Build is admitted, with
//! no attempt opened and no Build accepted. The initial accepted Build, and growth, run as usual. Every
//! cell reads the Build log it decides from: nothing here depends on timing.

use rdm_admin::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rdm_admin::build::{BuildReject, FabricDesired, MeshDesired};
use rdm_admin::build_state::{AttemptReason, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::control_plane::{ActionKind, ControlPlane, Refusal};
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use rdm_core::build::BuildReply;
use std::sync::Arc;

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n
}

/// Two meshes of two node-admins each, two rpc nodes in mesh1 and one in mesh2. `mesh1.admin.1` holds
/// the fabric seat and mesh1's, `mesh2.admin.1` holds mesh2's, `mesh1.rpc.1` mesh1's rpc seat with
/// `mesh1.rpc.2` its secondary: a stop, restart or replace of `mesh1.rpc.1` moves its seat by
/// node.primary.update and is not refused for want of a secondary (proposal Part C1, Q7). The fabric
/// is in state-sync.
fn view(status: ScopeStatus) -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status, provider: ProviderKind::Process },
        meshes: ["mesh1", "mesh2"].iter().map(|m| Mesh { id: Some(MeshId::mint()), name: m.to_string(), status: ScopeStatus::ReadyForTraffic }).collect(),
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
/// fabric has not first reached ready-for-traffic.
async fn rig(status: ScopeStatus) -> Rig {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(status);
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    Rig { cp, builds }
}

/// [`rig`], the control plane reading the fabric store `rows`.
async fn rig_over(status: ScopeStatus, rows: Arc<rdm_admin::fabric_storage::MemoryFabricStorage>) -> Rig {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(status);
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()).with_first_ready(rdm_admin::fabric_state::FirstReady::new(), rows));
    Rig { cp, builds }
}

impl Rig {
    async fn facts(&self) -> usize {
        self.builds.facts().await.unwrap().len()
    }
    async fn attempt(&self, reason: AttemptReason, path: &str, kind: ActionKind) -> Result<rdm_admin::control_plane::Opened, Refusal> {
        self.cp.open_attempt("test", reason, path.parse().unwrap(), kind).await
    }
}

/// The refusal's workflow and status, or a panic naming what came back.
fn refused(r: Result<rdm_admin::control_plane::Opened, Refusal>) -> (String, String) {
    match r {
        Err(Refusal::Reject(BuildReject::FabricNotReady { workflow, status })) => (workflow, status),
        other => panic!("expected the fabric-not-ready refusal, got {other:?}"),
    }
}

/// CONTRACT (R-PC1): until the initial fabric has first reached ready-for-traffic, a caller's node.stop,
/// node.start, node.restart, node.delete of a parked `.old` and recipe are each refused
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
        (AttemptReason::Delete, "mesh1.rpc.1.old", ActionKind::Delete, "node.delete mesh1.rpc.1.old"),
    ];
    for (reason, path, kind, workflow) in cases {
        let (named, status) = refused(r.attempt(reason, path, kind).await);
        assert_eq!((named.as_str(), status.as_str()), (workflow, "state-sync"), "{workflow}");
    }
    let recipe_step = rdm_core::build::RecipeStep::DeleteConnections { node: "mesh1.rpc.1".parse().unwrap() };
    let (named, status) = refused(r.cp.open_recipe_in(tracing::Span::current(), "test", vec![recipe_step]).await);
    assert_eq!((named.as_str(), status.as_str()), ("recipe (node.connections.delete:mesh1.rpc.1)", "state-sync"));
    assert_eq!(r.facts().await, before, "no attempt is opened and nothing is accepted by a refused workflow");
    let spans = capture.spans("rdm.node_admin.build.reject.via-fabric-not-ready");
    let workflows = ["node.stop mesh1.rpc.1", "node.start mesh1.rpc.1", "node.restart mesh1.rpc.1", "node.delete mesh1.rpc.1.old", "recipe (node.connections.delete:mesh1.rpc.1)"];
    assert_eq!(spans.len(), workflows.len(), "one refusal span per workflow: {spans:?}");
    for ((_, a), workflow) in spans.iter().zip(workflows) {
        assert_eq!((a.get("workflow").map(String::as_str), a.get("status").map(String::as_str)), (Some(workflow), Some("state-sync")), "{a:?}");
    }
    eprintln!("trace {}: rdm.node_admin.build.reject.via-fabric-not-ready x{} (stop, start, restart, delete, recipe) status=state-sync", spans[0].0, spans.len());
}

/// CONTRACT (R-PB1): a node.connections.delete moves a seat when its node holds the fabric-primary or
/// its mesh's primary seat. Until the initial fabric has first reached ready-for-traffic that
/// node.connections.delete is refused `fabric-not-ready`, naming the node; one of a node that holds no
/// seat is admitted (it moves none).
/// What must NOT happen: node.primary.update called for a seat whose rows only its holder's scratchpad holds.
#[tokio::test]
async fn a_connections_delete_of_a_primary_before_the_fabric_first_reaches_ready_for_traffic_is_refused_and_one_of_another_node_is_admitted() {
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
    let mesh1 = || MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]);
    let cases: Vec<(TopologyChange, &str)> = vec![
        (TopologyChange::RemoveMesh { mesh: "mesh2".into() }, "mesh.delete mesh2"),
        (TopologyChange::RemoveNode { node: "mesh1.rpc.1".parse().unwrap() }, "node.delete mesh1.rpc.1"),
        (TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1()] } }, "mesh.delete mesh2"),
        (TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2)]) }, "node.delete mesh1.rpc.1"),
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
            meshes: ["mesh1", "mesh2"].iter().map(|m| MeshDesired::of(m.to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, rpc)])).collect(),
        };
        for change in [
            TopologyChange::ReconcileFabric { desired: grown(3) },
            TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: rdm_core::NodeKind::RpcNode },
            TopologyChange::CreateMesh { desired: MeshDesired::of("mesh3".to_string(), [(rdm_core::NodeKind::NodeAdmin, 1)]) },
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

/// CONTRACT (states.md "The initial fabric before ready-for-traffic"; a Fabric event is a keyed fact on the
/// fabric): an admin that boots reads the one durable fact that the fabric first reached ready-for-traffic,
/// the ready-for-traffic event of its fabric on the fabric store. On a root where the fabric is in state-commit, the Fabric row published and no such row, the
/// admin refuses a caller's workflow `fabric-not-ready`; another fabric's row, or a different event's, is
/// not the fact. Once the row stands the admin admits the same workflow, though the fabric it hears is pending.
/// What must NOT happen: a boot in state-commit admitting, or a boot after the row refusing.
#[tokio::test]
async fn an_admin_booting_on_a_root_in_state_commit_refuses_and_one_booting_after_the_ready_for_traffic_row_admits() {
    use rdm_admin::fabric_storage::{FabricRecord, FabricStorage, MemoryFabricStorage};
    let r = rig(ScopeStatus::StateCommit).await;
    let fabric = r.cp.topology.read().await.fabric.id.clone();
    let status = MemoryFabricStorage::new();

    // The root: a different fabric's ready-for-traffic event, then this fabric's record with another event.
    status.put_fabric(&FabricRecord { fabric_id: FabricId::mint(), name: "fabric0".into(), events: vec!["ready-for-traffic".into()], provider: None }).await.unwrap();
    r.cp.first_ready.read_from(&status, &fabric).await.unwrap();
    status.put_fabric(&FabricRecord { fabric_id: fabric.clone(), name: "fabric1".into(), events: vec!["shutdown-initiated:mesh1.admin.1".into()], provider: None }).await.unwrap();
    r.cp.first_ready.read_from(&status, &fabric).await.unwrap();
    let (workflow, at) = refused(r.attempt(AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop).await);
    assert_eq!((workflow.as_str(), at.as_str()), ("node.stop mesh1.rpc.1", "state-commit"), "no row of this fabric's ready-for-traffic: the fabric has not first reached it");

    // The ready-for-traffic row of the fabric stands: the next boot, hearing pending, knows.
    let booted = rig(ScopeStatus::Pending).await;
    let fabric = booted.cp.topology.read().await.fabric.id.clone();
    status.put_fabric(&FabricRecord { fabric_id: fabric.clone(), name: "fabric1".into(), events: vec!["ready-for-traffic".into()], provider: None }).await.unwrap();
    booted.cp.first_ready.read_from(&status, &fabric).await.unwrap();
    let opened = booted.attempt(AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop).await.expect("admitted: the row says the fabric first reached ready-for-traffic");
    assert_eq!(opened.attempt, 2);
}

/// CONTRACT (states.md "The initial fabric before ready-for-traffic": the ready-for-traffic event on the
/// fabric store is the durable fact that the fabric first reached it; R-PB1, R-PC1): the
/// fabric-primary's control plane hears the initial fabric in state-sync and never knew it ready (an
/// outside successor of the fabric seat, whose fabric state gossip has not brought ready yet). Over a
/// root whose fabric store holds no ready-for-traffic fact of its fabric, a caller's node.stop is refused
/// `fabric-not-ready`. Once the fabric store holds it, the same node.stop is
/// admitted, decided on the row: `rdm.node_admin.fabric.update.via-first-ready` with reason
/// `ready-for-traffic-row-on-store-root`, and the attempt opens. What must NOT happen: a refusal
/// `fabric-not-ready` while the root holds the fabric's ready-for-traffic row.
#[tokio::test]
async fn a_workflow_is_admitted_on_the_ready_for_traffic_row_on_the_store_root_while_the_fabric_heard_is_not_ready() {
    use rdm_admin::fabric_storage::{FabricStorage, MemoryFabricStorage};
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-s23-admission-on-root");
    let rows = Arc::new(MemoryFabricStorage::new());
    let r = rig_over(ScopeStatus::StateSync, rows.clone()).await;
    let (workflow, at) = refused(r.attempt(AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop).await);
    assert_eq!((workflow.as_str(), at.as_str()), ("node.stop mesh1.rpc.1", "state-sync"), "no ready-for-traffic row on the root: refused");
    let fabric = r.cp.topology.read().await.fabric.id.clone();
    { let mut r = rows.fabric().await.unwrap().unwrap_or(rdm_admin::fabric_storage::FabricRecord { fabric_id: fabric, name: "fabric1".into(), events: Vec::new(), provider: None }); r.events.push("ready-for-traffic".into()); rows.put_fabric(&r).await.unwrap(); }
    let opened = r.attempt(AttemptReason::Stop, "mesh1.rpc.1", ActionKind::Stop).await.expect("admitted: the root's row says the fabric first reached ready-for-traffic");
    assert_eq!(opened.attempt, 2);
    let ready = capture.spans("rdm.node_admin.fabric.update.via-first-ready");
    assert_eq!(ready.iter().map(|(_, a)| a.get("reason").cloned()).collect::<Vec<_>>(), vec![Some("ready-for-traffic-row-on-store-root".to_string())], "decided on the root's row: {ready:?}");
    eprintln!("trace {}: rdm.node_admin.fabric.update.via-first-ready reason=ready-for-traffic-row-on-store-root", ready[0].0);
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
    match rdm_admin::build_op::refusal_reply(Refusal::Reject(reject)) {
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
        match rdm_admin::build_op::refusal_reply(refusal) {
            BuildReply::Rejected { reason, detail } => {
                assert_eq!(reason, "fabric-not-ready", "{workflow}: {detail}");
                assert!(detail.contains(workflow) && detail.contains("state-sync"), "{workflow}: the detail names the workflow and the status: {detail}");
            }
            other => panic!("{workflow}: expected Rejected: {other:?}"),
        }
    }
}

/// The claim of `attempt` of `build` by this admin for itself, decided on its own log: the attempt runs.
async fn running(r: &Rig, build: &rdm_admin::build::BuildId, attempt: u32) {
    use rdm_admin::build_state::{BuildAttemptClaim, ClaimOutcome};
    let claim = BuildAttemptClaim { build_id: build.clone(), attempt, executor: "mesh1.admin.1".into(), claimant: Some("mesh1.admin.1".into()), mesh: None };
    assert_eq!(r.builds.claim_attempt(&claim).await.unwrap(), ClaimOutcome::Won, "attempt {attempt} of {build} is claimed");
}

/// CONTRACT (build.md §1: "The one exception is fabric.delete, which may interrupt the Build in progress
/// because its target is nothing"; states.md "The initial fabric before ready-for-traffic": "fabric.delete
/// is admitted before the first ready-for-traffic, the Build in progress or not ... its own mesh.delete and
/// node.delete steps, and only those, run exempt from this refusal"; fabric-delete.md "Precondition"):
/// while the initial fabric is in state-sync and a growth Build's attempt runs, a standalone mesh.delete
/// and node.delete are refused `fabric-not-ready` and fabric.delete is accepted as the next Build, the
/// current one from then on. Once it is current, a standalone mesh.delete is still refused
/// `fabric-not-ready`, before the Build in progress is read (Q154's order). What must NOT happen:
/// fabric.delete refused `fabric-not-ready` or build-in-progress, or a standalone mesh.delete or
/// node.delete admitted beside it.
#[tokio::test]
async fn a_fabric_delete_before_the_first_ready_for_traffic_interrupts_the_build_in_progress_and_standalone_deletes_stay_refused() {
    let r = rig(ScopeStatus::StateSync).await;
    for (change, workflow) in [(TopologyChange::RemoveMesh { mesh: "mesh2".into() }, "mesh.delete mesh2"), (TopologyChange::RemoveNode { node: "mesh1.rpc.1".parse().unwrap() }, "node.delete mesh1.rpc.1")] {
        match r.cp.submit("test", change.clone()).await {
            Err(Refusal::Reject(BuildReject::FabricNotReady { workflow: named, status })) => assert_eq!((named.as_str(), status.as_str()), (workflow, "state-sync"), "{change:?}"),
            other => panic!("the standalone {workflow} was not refused fabric-not-ready: {other:?}"),
        }
    }
    let grow = r.cp.submit("test", TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: rdm_core::NodeKind::RpcNode }).await.expect("growth is admitted before the first ready-for-traffic");
    running(&r, &grow.build_id, 1).await;
    assert_eq!(r.builds.current_build().await.unwrap(), rdm_admin::build_state::CurrentBuild::InProgress(grow.build_id.clone()), "the growth Build is in progress");
    let before = r.facts().await;
    let delete = r.cp.submit("test", TopologyChange::DeleteFabric).await.unwrap_or_else(|e| panic!("fabric.delete is admitted before the first ready-for-traffic while a Build is in progress: {e:?}"));
    assert_ne!(delete.build_id, grow.build_id, "the delete is a Build of its own");
    assert_eq!((delete.attempt, delete.mesh.as_deref()), (1, None));
    assert_eq!(r.facts().await, before + 1, "the delete's acceptance is the one fact written");
    assert_eq!(r.builds.current_build().await.unwrap(), rdm_admin::build_state::CurrentBuild::InProgress(delete.build_id.clone()), "the delete is the current Build from its acceptance");
    match r.cp.submit("test", TopologyChange::RemoveMesh { mesh: "mesh2".into() }).await {
        Err(Refusal::Reject(BuildReject::FabricNotReady { workflow, status })) => assert_eq!((workflow.as_str(), status.as_str()), ("mesh.delete mesh2", "state-sync"), "the standalone mesh.delete beside the delete"),
        other => panic!("a standalone mesh.delete beside the delete was not refused fabric-not-ready: {other:?}"),
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
    let r = rig(ScopeStatus::StateSync).await;
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
