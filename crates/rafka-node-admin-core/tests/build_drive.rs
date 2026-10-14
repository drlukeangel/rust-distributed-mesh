//! The fabric-primary's drive of a Build, one cell per gate it passes and per way it can be stopped
//! (op `0x20`; node-rpc-envelope.md "Build, op `0x20`"): the Build state, the view and the
//! executors are fixtures, the drive is the product's.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.admin.1`, its mesh primary
//! `mesh1.admin.2` (so a node of `mesh1` is executed by an admin that is not the fabric-primary),
//! and `mesh1.rpc.1`. The Build grows `mesh1` by one rpc node.

use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build::{BuildId, BuildOperation, FabricDesired, MeshDesired};
use rafka_node_admin_core::build_claim::{AttemptContexts, ClaimDoor};
use rafka_node_admin_core::build_drive::{Drive, DriveEnd, DriveEnv, Gate, PrecedingUpdate, Read};
use rafka_node_admin_core::build_state::{BuildStateAdapter, BuildStateError, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::executor::OperationRunner;
use rafka_node_admin_core::control_plane::ControlPlane;
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc_contract::build::BuildReply;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

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
        nodes: vec![node("mesh1.admin.1", false, true), node("mesh1.admin.2", true, false), node("mesh1.rpc.1", false, false)],
    }
}

/// A gate the cell opens and closes.
struct Switch {
    authorizes: AtomicBool,
    fenced: AtomicBool,
    /// The admin a committed seat transfer named in the fence, when the cell sets one.
    successor: Mutex<Option<String>>,
    /// Set with `fenced` by a cell that fences while a dispatch is in flight.
    fence_tx: tokio::sync::watch::Sender<bool>,
    /// The holder the open seat record names (this birth's path.name), when the cell sets one.
    record_holder: Mutex<Option<String>>,
    /// What the peer-mesh investigation holds per mesh, when the cell sets it.
    peer: Mutex<std::collections::HashMap<String, rafka_node_admin_core::investigate::PeerMesh>>,
}

impl Switch {
    /// Fence the Build log now, naming `successor` as the admin the committed transfer seated.
    fn fence_to(&self, successor: Option<&str>) {
        *self.successor.lock().unwrap() = successor.map(str::to_string);
        self.fenced.store(true, Ordering::SeqCst);
        self.fence_tx.send_replace(true);
    }
}

#[async_trait::async_trait]
impl Gate for Switch {
    fn authorizes(&self) -> bool {
        self.authorizes.load(Ordering::SeqCst)
    }
    async fn fence(&self) -> Result<Option<String>, BuildStateError> {
        if self.fenced.load(Ordering::SeqCst) {
            Err(BuildStateError::Fenced { node: "mesh1.admin.1".into(), by: "leaving".into(), to: self.successor.lock().unwrap().clone() })
        } else {
            Ok(self.record_holder.lock().unwrap().clone())
        }
    }
    async fn fence_set(&self) {
        let _ = self.fence_tx.subscribe().wait_for(|f| *f).await;
    }
    fn peer_mesh(&self, mesh: &str) -> Option<rafka_node_admin_core::investigate::PeerMesh> {
        self.peer.lock().unwrap().get(mesh).copied()
    }
}

/// Records what ran, and commits each operation's `Complete` receipt on the executor's log (the
/// fixture's admins share one log) as the create pipeline does: a step's receipt is its durable
/// commit (`deployment/pipeline.rs`, `append_step_receipt` in the step runner).
struct Creates(Mutex<Vec<String>>, Arc<MemoryBuildStateAdapter>);

#[async_trait::async_trait]
impl OperationRunner for Creates {
    async fn run(&self, build: &BuildId, _lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        self.0.lock().unwrap().push(op.key());
        let receipt = BuildStepReceipt { mesh: None, build_id: build.clone(), attempt, operation: op.key(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None };
        self.1.append_step_receipt(&receipt).await.map_err(|e| e.to_string().into())
    }
}

struct Rig {
    builds: Arc<MemoryBuildStateAdapter>,
    env: Arc<DriveEnv>,
    switch: Arc<Switch>,
    dispatch: Arc<crate::loopback::Loopback>,
    proofs: Arc<crate::loopback::Proofs>,
    ran: Arc<Creates>,
    build: BuildId,
}

async fn rig() -> Rig {
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) };
    rig_for(view(), change, |ran, _| ran).await
}

/// A Build of `change` over `t`, run by `runner(ran, topology)`: `ran` records what ran.
async fn rig_for(t: Topology, change: TopologyChange, runner: impl FnOnce(Arc<Creates>, Arc<RwLock<Topology>>) -> Arc<dyn OperationRunner>) -> Rig {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let topology = Arc::new(RwLock::new(t.clone()));
    let fp_id = t.node(&"mesh1.admin.1".parse().unwrap()).expect("the fabric-primary is in the view").node_id.clone();
    let mut cp = ControlPlane::new(builds.clone(), accepted.clone(), "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time());
    cp.topology = topology.clone();
    let opened = cp.submit("test", change).await.unwrap();
    let ran = Arc::new(Creates(Mutex::new(Vec::new()), builds.clone()));
    let dispatch = crate::loopback::Loopback::new(builds.clone(), builds.clone(), topology.clone(), runner(ran.clone(), topology.clone()));
    let proofs = Arc::new(crate::loopback::Proofs::default());
    let switch = Arc::new(Switch { authorizes: AtomicBool::new(true), fenced: AtomicBool::new(false), successor: Mutex::new(None), fence_tx: tokio::sync::watch::channel(false).0, record_holder: Mutex::new(None), peer: Mutex::new(Default::default()) });
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: topology.clone(),
        builds: builds.clone(),
        door: Arc::new(ClaimDoor { node_id: fp_id.clone(), me, topology: topology.clone(), builds: builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()), claims: Arc::new(rafka_node_admin_core::mesh_claims::MeshClaims::in_memory()) }),
        dispatcher: dispatch.clone(),
        gate: switch.clone(),
        departure: proofs.clone(),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        turns: rafka_node_admin_core::park_gate::ParkGate::new(),
    });
    Rig { builds, env, switch, dispatch, proofs, ran, build: opened.build_id }
}

async fn next_within(reader: &mut rafka_node_admin_core::build_drive::DriveReader) -> Option<Read> {
    tokio::time::timeout(Duration::from_millis(50), reader.next()).await.ok().flatten()
}

/// CONTRACT: a fabric-primary whose committed seat record moved dispatches nothing. The Build
/// log's sticky fence is checked before the claim: the drive ends `SeatLost` naming the fence, no attempt is
/// claimed, no executor is called, and the callers' streams end with the seat loss (the caller
/// re-submits by the Build id at the new seat).
#[tokio::test]
async fn a_fabric_primary_whose_seat_record_moved_claims_and_dispatches_nothing() {
    let r = rig().await;
    r.switch.fenced.store(true, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::SeatLost(why) => assert!(why.contains("no longer holds the fabric-primary seat: the committed seat row moved it"), "{why}"),
        other => panic!("a fenced seat ends the drive SeatLost: {other:?}"),
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
    assert!(r.ran.0.lock().unwrap().is_empty());
    assert!(matches!(next_within(&mut reader).await, Some(Read::SeatLost(loss)) if loss.why.contains("leaving") && loss.to.is_none()));
}

/// CONTRACT: an admin knows the fabric-primary seat is its own by the node id it was born with,
/// never by path.name. When its view shows another birth at its own path.name holding the seat (a
/// successor or a retired birth of that path), the drive ends `SeatLost` naming that birth, claims
/// no attempt and calls no executor, and the callers' streams end naming the holder the committed
/// seat record names (none here), never the view's.
#[tokio::test]
async fn a_fabric_primary_whose_view_shows_another_birth_of_its_path_holding_the_seat_dispatches_nothing() {
    let r = rig().await;
    let other = rafka_mesh_entity::NodeId::mint();
    {
        let mut t = r.env.topology.write().await;
        let fp = t.nodes.iter_mut().find(|n| n.is_fabric_primary).expect("the view shows a fabric-primary");
        assert_eq!(fp.name, r.env.me, "the seat is shown at this admin's own path.name");
        fp.node_id = other.clone();
        fp.incarnation_id = Some(IncarnationId::mint());
    }
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::SeatLost(why) => assert!(why.contains("does not hold the fabric-primary seat") && why.contains(other.as_str()), "the refusal names the birth that holds the seat: {why}"),
        other => panic!("another birth of this path holding the seat ends the drive SeatLost: {other:?}"),
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
    assert!(r.ran.0.lock().unwrap().is_empty());
    assert!(matches!(next_within(&mut reader).await, Some(Read::SeatLost(loss)) if loss.to.is_none()), "the record names no holder");
}

/// CONTRACT (proposal node-primary-update Part C1 step 5: "From this write the holder is no longer
/// primary"): the committed seat record,
/// never the view, names where the seat is. A fabric-primary whose record still names this birth
/// while its view shows another admin holding the seat dispatches nothing, and the callers'
/// streams end naming the holder the record names (this birth), never the view's; a record naming
/// none names no holder.
#[tokio::test]
async fn a_fabric_primary_whose_view_shows_another_holder_names_the_seat_records_holder_never_the_views() {
    let r = rig().await;
    {
        let mut t = r.env.topology.write().await;
        for n in t.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh1.admin.2";
        }
    }
    *r.switch.record_holder.lock().unwrap() = Some("mesh1.admin.1".into());
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::SeatLost(why) => assert!(why.contains("does not hold the fabric-primary seat") && why.contains("seat record names mesh1.admin.1"), "the refusal names what the record names: {why}"),
        other => panic!("a view showing another holder ends the drive SeatLost: {other:?}"),
    }
    let loss = match next_within(&mut reader).await {
        Some(Read::SeatLost(loss)) => loss,
        _ => panic!("the callers' streams end with the seat loss"),
    };
    assert_eq!(loss.to.as_deref(), Some("mesh1.admin.1"), "the seat loss names the record's holder, not the view's mesh1.admin.2: {loss:?}");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
    *r.switch.record_holder.lock().unwrap() = None;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    assert!(matches!(r.env.run(&drive).await, DriveEnd::SeatLost(why) if why.contains("seat record names none")));
    assert!(matches!(next_within(&mut reader).await, Some(Read::SeatLost(loss)) if loss.to.is_none()), "a record naming none names no holder");
}

/// CONTRACT (proposal node-primary-update Part C1 step 5: "From this write the holder is no longer
/// primary"; Build dispatch from the old seat is refused `Fenced`): the seat record decides before the view does. A fabric-primary
/// whose seat record moved is refused `Fenced` while its view still shows it as the holder and
/// holds no row of the new one yet, and the callers' streams end naming the holder the record names,
/// never the view's. No executor is called.
#[tokio::test]
async fn a_fenced_fabric_primary_is_refused_fenced_whatever_its_view_shows_and_names_the_new_seat() {
    let r = rig_for(view(), TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) }, |ran, _| ran).await;
    assert!(r.env.topology.read().await.node(&"mesh2.admin.1".parse().unwrap()).is_none(), "the view holds no row of the new holder");
    r.switch.fence_to(Some("mesh2.admin.1"));
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::SeatLost(why) => assert!(why.contains("no longer holds the fabric-primary seat: the committed seat row moved it"), "the fence refused it, not the view: {why}"),
        other => panic!("a fenced seat ends the drive SeatLost: {other:?}"),
    }
    assert!(matches!(next_within(&mut reader).await, Some(Read::SeatLost(loss)) if loss.to.as_deref() == Some("mesh2.admin.1")));
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
}

/// CONTRACT: a committed seat transfer names the new holder in the fence itself. The old seat's view
/// is re-projected on its own clock, so right after the transfer it may still show this admin as the
/// fabric-primary: the callers' streams still end naming the successor the transfer committed, never
/// an empty seat read from a view that has not moved yet.
#[tokio::test]
async fn a_fence_set_by_a_committed_transfer_names_the_successor_while_the_view_still_shows_the_old_seat() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    t.nodes.push(node("mesh2.admin.1", true, false));
    let r = rig_for(t, TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) }, |ran, _| ran).await;
    assert_eq!(r.env.topology.read().await.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.1"), "the view still shows the old seat");
    *r.switch.successor.lock().unwrap() = Some("mesh2.admin.1".into());
    r.switch.fenced.store(true, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    assert!(matches!(r.env.run(&drive).await, DriveEnd::SeatLost(_)), "a fenced seat ends the drive SeatLost");
    match next_within(&mut reader).await {
        Some(Read::SeatLost(loss)) => assert_eq!(loss.to.as_deref(), Some("mesh2.admin.1"), "the stream names the committed successor: {loss:?}"),
        other => panic!("the reader ends with the seat loss, got {other:?}"),
    }
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
}

/// CONTRACT: a cut-off view dispatches nothing. It authorizes no claim and no call; the drive
/// pauses without ending, and the same drive goes on to the Build's end once the view authorizes.
#[tokio::test]
async fn a_cut_off_view_dispatches_nothing_and_the_drive_goes_on_once_it_authorizes() {
    let r = rig().await;
    r.switch.authorizes.store(false, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    assert!(matches!(r.env.run(&drive).await, DriveEnd::Paused(why) if why.contains("cut off")));
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty());
    r.switch.authorizes.store(true, Ordering::SeqCst);
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh1.rpc.2".to_string()]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "the node of mesh1 is run by mesh1's primary");
}

/// CONTRACT: an executor that cannot be reached is replaced only on proof of its departure. The
/// attempt claimed for it stays its own, the drive blocks with a frame naming why (once, however
/// many passes find the same reason), and no other admin is claimed for it and nothing runs.
#[tokio::test]
async fn an_unreachable_executor_without_proof_keeps_its_attempt_and_blocks_once() {
    let r = rig().await;
    r.dispatch.lose("mesh1.admin.2");
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    for _ in 0..3 {
        match r.env.run(&drive).await {
            DriveEnd::Stalled(why) => assert!(why.contains("departure is not proven"), "{why}"),
            other => panic!("silence proves nothing: {other:?}"),
        }
    }
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.2")), "attempt 1 is still the unreachable admin's");
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran on silence");
    match next_within(&mut reader).await {
        Some(Read::Frame(BuildReply::Blocked { attempt: 1, step, reason, .. })) => {
            assert_eq!(step, "departure-proof");
            assert!(reason.contains("mesh1.admin.2") && reason.contains("not proven"), "{reason}");
        }
        other => panic!("the drive says why it waits: {other:?}"),
    }
    assert!(next_within(&mut reader).await.is_none(), "the same blocker is framed once, not on every pass");
    // The proof arrives: the next attempt is claimed for the admin the view now names, and the
    // caller's reader carries its frames to the terminal.
    r.proofs.exited.lock().unwrap().insert("mesh1.admin.2".into());
    {
        let mut v = r.env.topology.write().await;
        for n in v.nodes.iter_mut() {
            match n.name.to_string().as_str() {
                "mesh1.admin.2" => {
                    n.status = NodeStatus::PendingReconnect;
                    n.is_primary = false;
                }
                "mesh1.admin.1" => n.is_primary = true,
                _ => {}
            }
        }
    }
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 2)], "attempt 2 went to the admin the view names, not to the lost one");
    assert_eq!(
        *r.ran.0.lock().unwrap(),
        vec!["create-node:mesh1.admin.2".to_string(), "create-node:mesh1.rpc.2".to_string()],
        "the lost admin's path is part of what is left, planned from the view as it is now"
    );
}

/// CONTRACT (#3016; elections §5: the seat goes to "the lowest ReadyForTraffic NodeId of its own mesh";
/// "Executor departure proof, never silence, is required before claiming the next attempt for its
/// successor"): mesh2's seated admin, the claimed executor, is lost and proven exited; mesh2's other
/// admin is held but silent, not yet seated. The fabric-primary of mesh1 does not take mesh2's
/// work: the drive stalls naming the held admin, every re-run uses the departure already recorded
/// (no second call to the lost executor, no second proof), and once mesh2.admin.2 holds the seat
/// the next attempt is claimed for it.
#[tokio::test]
async fn a_lost_executors_successor_is_its_same_mesh_admin_and_the_drive_waits_for_its_seat() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    t.nodes.push(node("mesh2.admin.1", true, false));
    t.nodes.push(node("mesh2.admin.2", false, false));
    t.nodes.push(node("mesh2.rpc.1", true, false));
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) };
    let r = rig_for(t, change, |ran, _| ran).await;
    // Attempt 1 is mesh2.admin.1's; then it is lost, and the view shows its exit and mesh2.admin.2 silent.
    assert!(matches!(r.env.door.claim("mesh2.admin.1", &r.build, 1).await, rafka_node_rpc_contract::build_claim::BuildClaimReply::Won { .. }));
    r.dispatch.lose("mesh2.admin.1");
    r.proofs.exited.lock().unwrap().insert("mesh2.admin.1".into());
    {
        let mut v = r.env.topology.write().await;
        for n in v.nodes.iter_mut() {
            match n.name.to_string().as_str() {
                "mesh2.admin.1" => {
                    n.status = NodeStatus::Dead;
                    n.is_primary = false;
                }
                "mesh2.admin.2" => n.status = NodeStatus::PendingReconnect,
                _ => {}
            }
        }
    }
    let drive = Drive::detached(r.build.clone());
    for pass in 0..3 {
        match r.env.run(&drive).await {
            DriveEnd::Stalled(why) => assert!(why.contains("mesh2") && why.contains("mesh2.admin.2") && why.contains("PendingReconnect"), "pass {pass}: {why}"),
            other => panic!("pass {pass}: the fabric-primary never takes mesh2's work while mesh2.admin.2 is held: {other:?}"),
        }
    }
    assert_eq!(r.proofs.calls.load(Ordering::SeqCst), 1, "the departure is proven once; every re-run reads it");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh2.admin.1")), "nothing is claimed while mesh2's seat is held");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty() && r.ran.0.lock().unwrap().is_empty());
    // mesh2.admin.2 is seated: the next attempt is its own.
    for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.2") {
        n.status = NodeStatus::ReadyForTraffic;
        n.is_primary = true;
    }
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh2.admin.2".to_string(), 2)], "attempt 2 is the same-mesh successor's");
    assert_eq!(r.proofs.calls.load(Ordering::SeqCst), 1);
}

/// Births `mesh2.admin.1` anew in place of its exited birth when its create has run, seated as
/// mesh2's primary, as the restore of a mesh's first admin does.
struct Reborn {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for Reborn {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        self.ran.run(build, lane, attempt, op).await?;
        if op.key() == "create-node:mesh2.admin.1" {
            let mut v = self.topology.write().await;
            v.nodes.retain(|n| n.name.to_string() != "mesh2.admin.1");
            v.nodes.push(node("mesh2.admin.1", true, false));
        }
        Ok(())
    }
}

/// CONTRACT (R-D1, Luke 10-08: "Loss of peer-mesh control-plane visibility fences lifecycle
/// mutation for every node in that mesh ... Only the peer-mesh investigation may authorize
/// restoration of the mesh's first recovering node-admin; once mesh authority is restored, that mesh
/// performs its own ordinary-node reconciliation"; mesh-investigation.md steps 5-6): mesh2's only
/// node-admin is proven exited, so the fabric-primary of mesh1 is the executor of mesh2's work in
/// its view, and the investigation holds mesh2 unheard. While no rebirth is decided the drive claims
/// nothing and dispatches nothing for mesh2, and says so on its own span naming the mesh, the
/// operation and how long the mesh is unheard. Once the investigation decides the rebirth, the
/// mesh's first recovering node-admin, and only it, is claimed and run on the fabric-primary; the
/// mesh's second admin and its ordinary node wait until the mesh is heard again, and are then run by
/// mesh2's own primary.
#[tokio::test]
async fn a_peer_mesh_held_unheard_is_never_claimed_for_until_its_rebirth_and_then_only_its_first_admin() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    t.nodes.push(node("mesh2.rpc.1", true, false));
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(Reborn { ran, topology })).await;
    let held = |rebirth_decided: bool| rafka_node_admin_core::investigate::PeerMesh { unheard_ms: Some(31_000), rebirth_decided, unreachable_decided: false };
    r.switch.peer.lock().unwrap().insert("mesh2".into(), held(false));
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3016-unheard-mesh-drive");
    let drive = Drive::detached(r.build.clone());
    let before = r.builds.read_build(&r.build).await.unwrap();

    // Held unheard, no rebirth decided: nothing is claimed or dispatched for mesh2.
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("mesh2") && why.contains("31000"), "the stall names the mesh and how long it is unheard: {why}"),
        other => panic!("the fabric-primary claims nothing for a peer mesh held unheard: {other:?}"),
    }
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (before.attempt, None), "no attempt is claimed across the fence");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty() && r.ran.0.lock().unwrap().is_empty(), "nothing dispatched, nothing run");
    let fenced = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    assert_eq!(fenced.len(), 1, "the fence is named once: {fenced:?}");
    let a = &fenced[0].1;
    assert_eq!((a.get("mesh").map(String::as_str), a.get("target").map(String::as_str), a.get("unheard_ms").map(String::as_str), a.get("rebirth_decided").map(String::as_str)), (Some("mesh2"), Some("mesh2.admin.1"), Some("31000"), Some("false")), "{a:?}");
    eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={} {a:?}", fenced[0].0);

    // The investigation decided the rebirth: the first recovering node-admin, and only it, runs, on
    // the fabric-primary.
    r.switch.peer.lock().unwrap().insert("mesh2".into(), held(true));
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("create-node:mesh2.admin.2") && why.contains("first recovering node-admin"), "the mesh's second admin waits for the mesh to be heard: {why}"),
        other => panic!("mesh2's other nodes wait until the mesh is heard again: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh2.admin.1".to_string()], "only the first recovering node-admin ran");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), before.attempt + 1)], "the restore ran on the fabric-primary");
    assert!(r.proofs.given.lock().unwrap().is_empty(), "a rebirth the ladder decided is told by the ladder, never by the proof of a fully crashed mesh");
    let second = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    assert_eq!(second.len(), 2, "{second:?}");
    assert_eq!((second[1].1.get("target").map(String::as_str), second[1].1.get("rebirth_decided").map(String::as_str)), (Some("mesh2.admin.2"), Some("true")), "{:?}", second[1].1);
    eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={} {:?}", second[1].0, second[1].1);

    // Heard again: mesh2's own primary runs the rest of the mesh.
    r.switch.peer.lock().unwrap().remove("mesh2");
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh2.admin.1".to_string(), "create-node:mesh2.admin.2".to_string(), "create-node:mesh2.rpc.2".to_string()]);
    let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
    assert_eq!(dispatched.last().map(|(x, _)| x.as_str()), Some("mesh2.admin.1"), "the mesh reconciles its own nodes once heard: {dispatched:?}");
}

/// mesh2 has lost every node it had, both node-admins included, and the Build grows its rpc nodes to
/// two; the investigation's state on mesh2 is the cell's.
async fn crashed_rig() -> Rig {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    for n in ["mesh2.admin.1", "mesh2.admin.2", "mesh2.rpc.1"] {
        let mut lost = node(n, false, false);
        lost.status = NodeStatus::Dead;
        t.nodes.push(lost);
    }
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) };
    rig_for(t, change, |ran, topology| Arc::new(Reborn { ran, topology })).await
}

/// CONTRACT (mesh-investigation.md step 5, "Fully crashed": "The fabric-primary acts on the provider's
/// exact exit proof, never on silence, and creates mesh-x's node-admin in process"; R-D1): mesh2 has
/// lost every node-admin and the investigation's 60 s decision on it came out `unreachable`, a hold.
/// The drive reads the provider's exit proof of every node of mesh2 the accepted topology names (its
/// two node-admins, its lost rpc node and the rpc node the Build grows, not yet born) itself, and on
/// that proof it runs the create of the mesh's first node-admin, and only it, on the fabric-primary; the mesh's
/// second admin and its ordinary node still wait for the mesh to be heard. With no decision at all
/// the fence stands for the first admin too. Must NOT happen: the fence refusing the one create the
/// exit proof admits (the mesh waiting for ever), or the hold releasing more than that create.
#[tokio::test]
async fn the_create_of_a_fully_crashed_meshs_first_node_admin_runs_on_the_fabric_primary_and_nothing_else_of_the_held_mesh_does() {
    let r = crashed_rig().await;
    r.proofs.exited.lock().unwrap().extend(["mesh2.admin.1", "mesh2.admin.2", "mesh2.rpc.1", "mesh2.rpc.2"].map(String::from));
    let held = |unreachable_decided: bool| rafka_node_admin_core::investigate::PeerMesh { unheard_ms: Some(61_000), rebirth_decided: false, unreachable_decided };
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-s56-crashed-mesh-drive");
    let drive = Drive::detached(r.build.clone());
    let before = r.builds.read_build(&r.build).await.unwrap();

    // No decision: the fence stands for the first admin.
    r.switch.peer.lock().unwrap().insert("mesh2".into(), held(false));
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("create-node:mesh2.admin.1") && why.contains("no rebirth decided"), "{why}"),
        other => panic!("nothing is claimed for a held mesh with no decision: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing dispatched, nothing run");

    // The 60 s decision came out unreachable: the create of the first node-admin, and only it, runs on the fabric-primary.
    r.switch.peer.lock().unwrap().insert("mesh2".into(), held(true));
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("create-node:mesh2.admin.2") && why.contains("unreachable"), "the second admin waits for the mesh to be heard, and the stall names the decision: {why}"),
        other => panic!("the mesh's other nodes wait until it is heard again: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh2.admin.1".to_string()], "only the mesh's first node-admin ran");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), before.attempt + 1)], "the create ran in process on the fabric-primary");
    // R-DG1 (mesh-investigation.md steps 5-6): the fabric-primary gave the mesh a node-admin when it claimed the create, once.
    assert_eq!(*r.proofs.given.lock().unwrap(), vec![("mesh2".to_string(), r.build.to_string(), before.attempt + 1)], "the claim of the fully crashed mesh's create told the proof the mesh was given a node-admin");
    let fenced = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    assert_eq!(fenced.len(), 2, "the fence is named for the undecided admin.1 and then for admin.2: {fenced:?}");
    assert_eq!((fenced[1].1.get("target").map(String::as_str), fenced[1].1.get("rebirth_decided").map(String::as_str)), (Some("mesh2.admin.2"), Some("false")), "{:?}", fenced[1].1);
    eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={} {:?}", fenced[1].0, fenced[1].1);
}

/// CONTRACT (mesh-investigation.md step 5, "Fully crashed": "The fabric-primary acts on the provider's
/// exact exit proof, never on silence ... else hold; backbone refeed continues"): the investigation's
/// 60 s decision on mesh2 came out `unreachable`, and the provider proves `mesh2.admin.1` exited but
/// not `mesh2.admin.2`. The mesh is held: the drive claims and dispatches nothing for it, the fence
/// names the decision, and the proof read is its own span naming the node-admin not proven.
/// Must NOT happen: the create of mesh2's first node-admin admitted on the `unreachable` decision
/// alone, a silence, while a node-admin of the mesh is not proven exited.
#[tokio::test]
async fn an_unreachable_mesh_with_a_node_admin_the_provider_does_not_prove_exited_is_held_and_nothing_of_it_runs() {
    let r = crashed_rig().await;
    r.proofs.exited.lock().unwrap().insert("mesh2.admin.1".to_string());
    r.switch.peer.lock().unwrap().insert("mesh2".into(), rafka_node_admin_core::investigate::PeerMesh { unheard_ms: Some(61_000), rebirth_decided: false, unreachable_decided: true });
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-s56-crash-proof-held");
    let drive = Drive::detached(r.build.clone());
    let before = r.builds.read_build(&r.build).await.unwrap();
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("create-node:mesh2.admin.1") && why.contains("unreachable"), "the fence holds the mesh's first admin and names the decision: {why}"),
        other => panic!("nothing is claimed for an unreachable mesh with a node-admin not proven exited: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing dispatched, nothing run");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (before.attempt, None), "no attempt is claimed across the fence");
    let proofs = capture.spans("rdm.node_admin.mesh.update.via-crash-proof");
    assert_eq!(proofs.len(), 1, "the proof read is named once: {proofs:?}");
    let a = &proofs[0].1;
    assert_eq!((a.get("mesh").map(String::as_str), a.get("outcome").map(String::as_str), a.get("proven").map(String::as_str)), (Some("mesh2"), Some("held"), Some("mesh2.admin.1")), "{a:?}");
    assert!(a.get("unproven").is_some_and(|u| u.contains("mesh2.admin.2")), "the hold names the node-admin not proven: {a:?}");
    eprintln!("span rdm.node_admin.mesh.update.via-crash-proof trace_id={} {a:?}", proofs[0].0);
}

/// The R-D1 fixture: mesh2's only node-admin is proven exited (so the fabric-primary of mesh1 is
/// the executor of mesh2's work in its view), mesh2 keeps its rpc node, the Build grows mesh2 to two
/// admins and two rpc nodes, a restored `mesh2.admin.1` is seated as mesh2's primary, and the
/// investigation holds mesh2 unheard with its rebirth decided.
async fn rebirth_rig() -> Rig {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    t.nodes.push(node("mesh2.rpc.1", true, false));
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(Reborn { ran, topology })).await;
    r.switch.peer.lock().unwrap().insert("mesh2".into(), rafka_node_admin_core::investigate::PeerMesh { unheard_ms: Some(61_000), rebirth_decided: true, unreachable_decided: false });
    r
}

/// CONTRACT (R-D1; mesh-investigation.md step 6, "A rebirth verdict starts the existing Mesh
/// Recovery flow ... on the SAME accepted Build"; mesh-recovery.md step 2, the attempt "runs
/// node.create for the mesh's first recovering node-admin"): which node-admin of a held mesh the
/// rebirth already admitted is a fact of the Build (the receipts of an attempt an admin outside the
/// mesh held), not of the drive that dispatched it. After one drive restored `mesh2.admin.1` and
/// stopped at the fence on `mesh2.admin.2`, a second drive instance over the same Build admits no
/// second admin, whether it finds the next attempt open or already claimed for another admin
/// outside mesh2: nothing more runs, and it names the fence on `mesh2.admin.2`.
#[tokio::test]
async fn a_second_drive_over_the_same_build_admits_no_second_recovering_admin_of_a_held_mesh() {
    let r = rebirth_rig().await;
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3016-second-drive");
    let first = Drive::detached(r.build.clone());
    match r.env.run(&first).await {
        DriveEnd::Stalled(why) => assert!(why.contains("create-node:mesh2.admin.2"), "the first drive stops at mesh2's second admin: {why}"),
        other => panic!("the first drive restores one admin and stops: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh2.admin.1".to_string()]);
    // A second drive instance (a drive replaced on the same fabric-primary) over the same Build.
    let second = Drive::detached(r.build.clone());
    match r.env.run(&second).await {
        DriveEnd::Stalled(why) => assert!(why.contains("create-node:mesh2.admin.2") && why.contains("first recovering node-admin"), "a second drive admits no second admin: {why}"),
        other => panic!("a second drive over the same Build admits no second recovering admin: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh2.admin.1".to_string()], "no second admin of mesh2 crossed the fence");
    // The next attempt already claimed for another admin outside mesh2 when a third drive finds it.
    let open = r.builds.read_build(&r.build).await.unwrap().attempt + 1;
    assert!(matches!(r.env.door.claim("mesh1.admin.2", &r.build, open).await, rafka_node_rpc_contract::build_claim::BuildClaimReply::Won { .. }));
    let third = Drive::detached(r.build.clone());
    let end = r.env.run(&third).await;
    assert!(matches!(&end, DriveEnd::Stalled(why) if why.contains("create-node:mesh2.admin.2")), "a running attempt held by another admin admits no second admin either: {end:?}");
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh2.admin.1".to_string()], "no second admin of mesh2 crossed the fence");
    // Each drive names the fence on mesh2's second admin once, on its own span.
    let fenced = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    assert_eq!(fenced.len(), 3, "{fenced:?}");
    for (trace, a) in &fenced {
        assert_eq!((a.get("target").map(String::as_str), a.get("rebirth_decided").map(String::as_str)), (Some("mesh2.admin.2"), Some("true")), "{a:?}");
        eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={trace} {a:?}");
    }
}

/// Commits each operation's receipt and, when `lands`, puts the created node in the view live, as a
/// create's launch and join do.
struct Lands {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
    lands: bool,
}

#[async_trait::async_trait]
impl OperationRunner for Lands {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        self.ran.run(build, lane, attempt, op).await?;
        if let (true, BuildOperation::CreateNode { node: p, .. }) = (self.lands, op) {
            self.topology.write().await.nodes.push(node(&p.to_string(), false, false));
        }
        Ok(())
    }
}

/// Two meshes: the fabric-primary `mesh1.admin.1` is mesh1's primary too, so it executes mesh1's
/// members; mesh2's seated admin is proven exited and its other admin is held but silent
/// (PendingReconnect), so no admin executes mesh2's work in this view. The Build grows mesh1 by one
/// rpc node and restores mesh2's two admins.
async fn held_successor_rig(lands: bool) -> Rig {
    let mut t = view();
    for n in t.nodes.iter_mut() {
        n.is_primary = n.name.to_string() == "mesh1.admin.1";
    }
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    let mut held = node("mesh2.admin.2", false, false);
    held.status = NodeStatus::PendingReconnect;
    t.nodes.push(held);
    t.nodes.push(node("mesh2.rpc.1", false, false));
    let counts = |mesh: &str, admins: u32, rpcs: u32| MeshDesired::of(mesh.to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, admins), (rafka_mesh_entity::NodeKind::RpcNode, rpcs)]);
    let change = TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![counts("mesh1", 2, 2), counts("mesh2", 2, 1)] } };
    rig_for(t, change, move |ran, topology| Arc::new(Lands { ran, topology, lands })).await
}

/// CONTRACT (elections §5, the same-mesh successor; R-A2: cross-mesh only when
/// the whole cohort is proven lost): a run of the fabric-primary is cut before a later operation no
/// admin executes in its view. The fabric-primary runs mesh1's rpc node and stops there: mesh2's
/// admins are not carried by the run, and the drive stalls naming mesh2's held admin, whose seat the
/// rest waits for.
#[tokio::test]
async fn a_run_is_cut_before_an_operation_of_a_mesh_whose_admin_is_held_and_the_rest_waits_for_its_seat() {
    let r = held_successor_rig(true).await;
    let drive = Drive::detached(r.build.clone());
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("mesh2") && why.contains("mesh2.admin.2") && why.contains("PendingReconnect"), "the stall names the held admin: {why}"),
        other => panic!("mesh2's work waits for its own seat: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh1.rpc.2".to_string()], "the run was cut before mesh2's admins");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1)]);
}

/// CONTRACT (node-rpc-envelope.md "Build, op `0x20`": "no completed step runs twice"): what a run
/// completed is read from the Build's step receipts, never from the view. The fabric-primary's run of
/// mesh1's rpc node is cut before mesh2's work and handed back to it; its view has not heard the new
/// node yet. The next claim does not dispatch the completed operation again: the drive dispatches
/// once and stalls naming mesh2's held admin.
#[tokio::test]
async fn a_run_handed_back_is_never_dispatched_again_for_an_operation_its_receipts_completed() {
    let r = held_successor_rig(false).await;
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3016-handed-back");
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(2), r.env.run(&drive)).await;
    let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
    let ran = r.ran.0.lock().unwrap().clone();
    assert_eq!((dispatched.len(), ran.len()), (1, 1), "the completed operation is dispatched once: dispatched {} times, ran {:?}", dispatched.len(), &ran[..ran.len().min(4)]);
    match end {
        Ok(DriveEnd::Stalled(why)) => assert!(why.contains("mesh2.admin.2") && why.contains("PendingReconnect"), "{why}"),
        other => panic!("the drive stalls naming mesh2's held admin: {other:?}"),
    }
    let drives = capture.spans("rdm.node_admin.build.update.via-drive");
    assert_eq!(drives.len(), 1, "{drives:?}");
    assert_eq!((drives[0].1.get("dispatches").map(String::as_str), drives[0].1.get("outcome").map(String::as_str)), (Some("1"), Some("stalled")), "{:?}", drives[0].1);
    eprintln!("span rdm.node_admin.build.update.via-drive trace_id={} {:?}", drives[0].0, drives[0].1);
}

/// Records what ran and writes nothing: the executor's own log holds the receipts
/// (`crate::loopback::Loopback::own_log`).
struct Records(Arc<Creates>);

#[async_trait::async_trait]
impl OperationRunner for Records {
    async fn run(&self, _: &BuildId, _lane: Option<&str>, _: u32, op: &BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        self.0 .0.lock().unwrap().push(op.key());
        Ok(())
    }
}

/// CONTRACT (node-rpc-envelope.md, Build op `0x20`: a `Step` frame goes out "only after the step's
/// Complete receipt is durable on the executor that wrote it"; the stream reply is the authority and
/// progress waits on no gossip): an executor on another host writes its receipts on its own log, and
/// the fabric-primary's log holds them only once the Build topic carries them. mesh1's primary
/// `mesh1.admin.2` runs mesh1's rpc node, and its run is cut before mesh2's work and handed back.
/// Neither the fabric-primary's log nor its view holds the new node yet; the `Complete` frame on the
/// run's stream says it completed, and the drive plans from that: the completed operation is not
/// dispatched again, and the drive stalls naming mesh2's held admin.
#[tokio::test]
async fn a_remote_executors_completed_operation_is_never_dispatched_again_before_the_build_topic_carries_its_receipt() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    let mut held = node("mesh2.admin.2", false, false);
    held.status = NodeStatus::PendingReconnect;
    t.nodes.push(held);
    t.nodes.push(node("mesh2.rpc.1", false, false));
    let counts = |mesh: &str, admins: u32, rpcs: u32| MeshDesired::of(mesh.to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, admins), (rafka_mesh_entity::NodeKind::RpcNode, rpcs)]);
    let change = TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![counts("mesh1", 2, 2), counts("mesh2", 2, 1)] } };
    let r = rig_for(t, change, |ran, _| Arc::new(Records(ran))).await;
    let remote = r.dispatch.own_log("mesh1.admin.2");
    // The verdict an executor reports is recorded on the fabric-primary's own log, as the product's
    // `LocalVerdicts` does; the step receipts are not.
    let e = &r.env;
    let env = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: e.gate.clone(),
        departure: e.departure.clone(),
        verdicts: Arc::new(rafka_node_admin_core::build_op::LocalVerdicts { local: r.builds.clone() }),
        turns: e.turns.clone(),
    });
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3016-remote-receipt");
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(2), env.run(&drive)).await;
    let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
    let ran = r.ran.0.lock().unwrap().clone();
    assert_eq!((dispatched.len(), ran.len()), (1, 1), "the completed operation is dispatched once: dispatched {:?}, ran {:?}", &dispatched[..dispatched.len().min(4)], &ran[..ran.len().min(4)]);
    assert!(remote.read_build(&r.build).await.unwrap().completed_in_run("create-node:mesh1.rpc.2"), "the receipt is durable on the executor");
    assert!(!r.builds.read_build(&r.build).await.unwrap().completed_in_run("create-node:mesh1.rpc.2"), "the fabric-primary's log has not heard the receipt");
    match end {
        Ok(DriveEnd::Stalled(why)) => assert!(why.contains("mesh2.admin.2") && why.contains("PendingReconnect"), "{why}"),
        other => panic!("the drive stalls naming mesh2's held admin: {other:?}"),
    }
    let drives = capture.spans("rdm.node_admin.build.update.via-drive");
    assert_eq!(drives.len(), 1, "{drives:?}");
    assert_eq!(drives[0].1.get("dispatches").map(String::as_str), Some("1"), "{:?}", drives[0].1);
    eprintln!("span rdm.node_admin.build.update.via-drive trace_id={} {:?}", drives[0].0, drives[0].1);
}

/// Commits each operation's receipt and lands the created node live; the run of `mesh1.rpc.2` also
/// seats mesh2's held admin as mesh2's primary, as a mesh whose seat is taken while another mesh's
/// run goes on.
struct LandsAndSeats {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for LandsAndSeats {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        self.ran.run(build, lane, attempt, op).await?;
        if let BuildOperation::CreateNode { node: p, .. } = op {
            let mut v = self.topology.write().await;
            v.nodes.push(node(&p.to_string(), false, false));
            if p.to_string() == "mesh1.rpc.2" {
                for n in v.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.2") {
                    n.status = NodeStatus::ReadyForTraffic;
                    n.is_primary = true;
                }
            }
        }
        Ok(())
    }
}

/// CONTRACT: a run handed back to the fabric-primary is claimed for the admin its view names for what
/// is left, never for the fabric-primary to hand on unrun. The fabric-primary's run of mesh1's rpc
/// node is cut before mesh2's admin (no admin of mesh2 is seated when it is planned) and handed back;
/// by then mesh2's admin holds the seat. The next attempt is claimed for `mesh2.admin.2` directly: no
/// attempt of the fabric-primary carries nothing to it.
#[tokio::test]
async fn a_run_handed_back_is_claimed_for_the_admin_the_view_names_for_what_is_left() {
    let mut t = view();
    for n in t.nodes.iter_mut() {
        n.is_primary = n.name.to_string() == "mesh1.admin.1";
    }
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    let mut held = node("mesh2.admin.2", false, false);
    held.status = NodeStatus::PendingReconnect;
    t.nodes.push(held);
    t.nodes.push(node("mesh2.rpc.1", false, false));
    let counts = |mesh: &str, admins: u32, rpcs: u32| MeshDesired::of(mesh.to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, admins), (rafka_mesh_entity::NodeKind::RpcNode, rpcs)]);
    let change = TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![counts("mesh1", 2, 2), counts("mesh2", 2, 1)] } };
    let r = rig_for(t, change, |ran, topology| Arc::new(LandsAndSeats { ran, topology })).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh1.rpc.2".to_string(), "create-node:mesh2.admin.1".to_string()]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1), ("mesh2.admin.2".to_string(), 2)], "the hand-back is claimed for mesh2's seated admin");
}

/// CONTRACT (node-rpc-envelope.md, Build op `0x20`: "An executor that cannot be reached is replaced
/// only on proof of its departure"; the successor re-checks the world for what the lost executor's run
/// left): what counts as done is what an executor reported, by its run's verdict (`HandedOff` or
/// `Converged`). `mesh1.admin.2` wrote the `Complete` receipt of `mesh1.rpc.2` in attempt 1 and was
/// lost before its run ended: no verdict, and the view never held the node. Its departure is proven,
/// mesh1 seats the fabric-primary, and attempt 2 creates `mesh1.rpc.2` with the rest of the plan: a lost run's
/// receipt does not make its node done.
#[tokio::test]
async fn a_lost_executors_unreported_receipt_does_not_make_its_operation_done() {
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 3)]) };
    let r = rig_for(view(), change, |ran, _| ran).await;
    assert!(matches!(r.env.door.claim("mesh1.admin.2", &r.build, 1).await, rafka_node_rpc_contract::build_claim::BuildClaimReply::Won { .. }));
    let receipt = BuildStepReceipt { mesh: None, build_id: r.build.clone(), attempt: 1, operation: "create-node:mesh1.rpc.2".into(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None };
    r.builds.append_step_receipt(&receipt).await.unwrap();
    r.dispatch.lose("mesh1.admin.2");
    r.proofs.exited.lock().unwrap().insert("mesh1.admin.2".into());
    for n in r.env.topology.write().await.nodes.iter_mut() {
        match n.name.to_string().as_str() {
            "mesh1.admin.2" => {
                n.status = NodeStatus::Dead;
                n.is_primary = false;
            }
            "mesh1.admin.1" => n.is_primary = true,
            _ => {}
        }
    }
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh1.admin.2".to_string(), "create-node:mesh1.rpc.2".to_string(), "create-node:mesh1.rpc.3".to_string()], "the lost run's node is created again");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 2)]);
}

/// CONTRACT: an executor runs the first operation of the attempt the fabric-primary claimed for it
/// whatever its own view says. The fabric-primary's view named `mesh1.admin.2` the executor; the view
/// of `mesh1.admin.2` holds no mesh primary yet (it has not elected itself since the old one was
/// lost), so by it the operation is the fabric-primary's. The attempt runs on `mesh1.admin.2` and is
/// not passed back: every hand-off follows a step completed, so two views that disagree never
/// claim attempts back and forth.
#[tokio::test]
async fn an_executor_whose_view_lags_runs_the_first_operation_it_was_claimed_for_and_the_attempt_is_not_passed_back() {
    let r = rig().await;
    let mut theirs = view();
    for n in theirs.nodes.iter_mut() {
        n.is_primary = false;
    }
    r.dispatch.sees("mesh1.admin.2", Arc::new(RwLock::new(theirs)));
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh1.rpc.2".to_string()]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "one attempt, on the admin the fabric-primary's view named");
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 1);
}

/// The gate of a fabric-primary whose node.fabric-primary.update moves its seat to `mesh2.admin.1`:
/// `move_seats` answers once the successor's installation is acknowledged, after which the committed
/// record names `mesh2.admin.1` and the view shows it holding the seat. Every other gate is `inner`'s.
struct SeatMoves {
    inner: Arc<Switch>,
    topology: Arc<RwLock<Topology>>,
    /// Every move asked: `(holder, mesh seat, fabric seat, operation)`.
    moved: Mutex<Vec<(String, bool, bool, String)>>,
}

#[async_trait::async_trait]
impl Gate for SeatMoves {
    fn authorizes(&self) -> bool {
        self.inner.authorizes()
    }
    async fn fence(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.fence().await
    }
    async fn fence_set(&self) {
        self.inner.fence_set().await
    }
    async fn move_seats(&self, m: &rafka_node_admin_core::executor::SeatMove, _: &BuildId, _: u32, _: u32, operation: &str) -> Result<(), rafka_node_admin_core::build_drive::SeatMoveEnd> {
        self.moved.lock().unwrap().push((m.holder.name.to_string(), m.mesh_seat, m.fabric_seat, operation.to_string()));
        for n in self.topology.write().await.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh2.admin.1";
        }
        self.inner.fence_to(Some("mesh2.admin.1"));
        Ok(())
    }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The last
/// node-admin of a mesh": "If the mesh holds the fabric seat, that seat moves first by
/// `node.fabric-primary.update` with the departing mesh excluded (mesh-delete.md step 1)"; C1 step 1:
/// "The holder is commanded from above, as a step of the Build"; step 8: "Only then does the commanding
/// Build continue"): a mesh retire whose fabric-primary sits inside the leaving mesh moves the fabric
/// seat alone (the mesh seat stays, mesh.delete never updates it) from the drive, as a step of the
/// attempt the holder claims for itself; that attempt ends `HandedOff` to the new seat once the move
/// answered, and the drive ends `SeatLost` naming the new holder.
/// Must NOT happen: an operation dispatched at the leaving mesh's admin, the retire run there, or the
/// attempt ending with no verdict or handed to any admin but the new seat.
#[tokio::test]
async fn a_mesh_retire_whose_fabric_primary_sits_inside_moves_the_fabric_seat_alone_in_its_own_attempt_handed_to_the_new_seat() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    t.nodes.push(node("mesh2.admin.1", true, false));
    let r = rig_for(t, TopologyChange::RemoveMesh { mesh: "mesh1".into() }, |ran, _| ran).await;
    let gate = Arc::new(SeatMoves { inner: r.switch.clone(), topology: r.env.topology.clone(), moved: Mutex::new(Vec::new()) });
    let env = Arc::new(DriveEnv {
        me: r.env.me.clone(),
        topology: r.env.topology.clone(),
        builds: r.env.builds.clone(),
        door: r.env.door.clone(),
        dispatcher: r.env.dispatcher.clone(),
        gate: gate.clone(),
        departure: r.env.departure.clone(),
        verdicts: r.env.verdicts.clone(),
        turns: r.env.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match env.run(&drive).await {
        DriveEnd::SeatLost(why) => assert!(why.contains("the committed seat row moved it"), "the drive ends at the committed seat record: {why}"),
        other => panic!("the old seat holder's drive ends SeatLost once the seat moved: {other:?}"),
    }
    let moved = gate.moved.lock().unwrap().clone();
    assert_eq!(moved.len(), 1, "one node.fabric-primary.update: {moved:?}");
    let (holder, mesh_seat, fabric_seat, operation) = &moved[0];
    assert_eq!((holder.as_str(), *mesh_seat, *fabric_seat), ("mesh1.admin.1", false, true), "the fabric seat alone moves from the fabric-primary: {moved:?}");
    assert!(operation.starts_with("shutdown-mesh:"), "the update names the mesh retire it precedes: {operation}");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no operation was dispatched: {:?}", r.dispatch.dispatched.lock().unwrap());
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran at the leaving mesh's admin: {:?}", r.ran.0.lock().unwrap());
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.attempt_executors.get(&1).map(String::as_str)), (1, Some("mesh1.admin.1")), "the move is a step of the attempt the holder claimed for itself: {:?}", p.attempt_executors);
    let verdicts: Vec<_> = r.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
        rafka_node_admin_core::build_state::BuildFact::Attempt(a) if a.build_id == r.build => Some((a.attempt, a.outcome)),
        _ => None,
    }).collect();
    assert_eq!(verdicts, vec![(1, rafka_node_admin_core::build_state::AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() })], "the holder's attempt ends handed to the new seat, which claims the next");
    assert!(matches!(next_within(&mut reader).await, Some(Read::SeatLost(loss)) if loss.to.as_deref() == Some("mesh2.admin.1")), "the callers' streams end naming the new seat");
}

/// CONTRACT (R-ON8: the fabric-primary plans and dispatches): an executor runs exactly the
/// operations the call carries and never plans the Build again from its own view. The
/// fabric-primary's view holds the node a previous run just launched (`mesh1.rpc.2`); the
/// executor's view has not heard it yet. What is left is `mesh1.rpc.3` alone, and that is all the
/// executor runs: the node launched a moment ago is not created a second time.
#[tokio::test]
async fn an_executor_whose_view_lacks_a_just_launched_node_creates_only_what_the_call_carries() {
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 3)]) };
    let mut ours = view();
    ours.nodes.push(node("mesh1.rpc.2", false, false));
    let r = rig_for(ours, change, |ran, _| ran).await;
    r.dispatch.sees("mesh1.admin.2", Arc::new(RwLock::new(view())));
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh1.rpc.3".to_string()], "only what the fabric-primary planned ran; mesh1.rpc.2 was launched already");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)]);
}

/// Seats `mesh2.admin.1` as mesh2's primary when its create has run, as the launch and join of the
/// first admin of a mesh do.
struct MeshBirth {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for MeshBirth {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        self.ran.run(build, lane, attempt, op).await?;
        if op.key() == "create-node:mesh2.admin.1" {
            self.topology.write().await.nodes.push(node("mesh2.admin.1", true, false));
        }
        Ok(())
    }
}

/// CONTRACT: the members of a mesh are run by that mesh's own primary once it exists. The
/// fabric-primary creates mesh2's first admin; the view then names that admin the executor of the
/// operations after it, so the fabric-primary's run ends there and the next attempt is claimed for
/// mesh2's primary, which runs the rest.
#[tokio::test]
async fn a_run_that_seats_a_meshs_first_admin_hands_the_rest_of_the_mesh_to_it() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 1), (rafka_mesh_entity::NodeKind::RpcNode, 1)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(MeshBirth { ran, topology })).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh2.admin.1".to_string(), "create-node:mesh2.rpc.1".to_string()]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1), ("mesh2.admin.1".to_string(), 2)], "mesh2's own primary ran the members");
}

/// Runs nothing to its end: every operation is held open, and the cell is told when the run began.
struct HeldOpen {
    began: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl OperationRunner for HeldOpen {
    async fn run(&self, _: &BuildId, _lane: Option<&str>, _: u32, _: &BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        self.began.notify_one();
        std::future::pending().await
    }
}

/// CONTRACT (proposal node-primary-update Part C1 step 5): the committed seat row is the target every
/// decision is judged against. The fence is set while the drive waits on an executor's run, and the
/// record names another holder than that executor: the drive ends `SeatLost` at the fence, without
/// waiting for that run's stream to end, the callers' streams end naming the holder the record names,
/// and no later attempt is claimed.
#[tokio::test]
async fn a_seat_row_naming_another_holder_than_the_executor_ends_the_drive_naming_that_holder() {
    let held = Arc::new(HeldOpen { began: tokio::sync::Notify::new() });
    let runner = held.clone();
    let r = rig_for(view(), TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) }, move |_, _| -> Arc<dyn OperationRunner> { runner }).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let (env, d) = (r.env.clone(), drive.clone());
    let running = tokio::spawn(async move { env.run(&d).await });
    held.began.notified().await;
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "attempt 1 is running at the executor");
    r.switch.fence_to(Some("mesh2.admin.1"));
    let end = tokio::time::timeout(Duration::from_secs(5), running).await.expect("the drive ends at the fence while the executor's run is still open").unwrap();
    assert!(matches!(&end, DriveEnd::SeatLost(why) if why.contains("no longer holds the fabric-primary seat: the committed seat row moved it")), "the fence ends the drive SeatLost: {end:?}");
    match next_within(&mut reader).await {
        Some(Read::SeatLost(loss)) => assert_eq!(loss.to.as_deref(), Some("mesh2.admin.1"), "the stream names the holder the record names: {loss:?}"),
        other => panic!("the reader ends with the seat loss, got {other:?}"),
    }
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.2")), "no later attempt was claimed");
    assert_eq!(r.dispatch.dispatched.lock().unwrap().len(), 1, "nothing more was dispatched");
}

/// An executor's run the cell feeds item by item: `dispatch` hands the drive the stream and says so.
struct Fed {
    items: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<rafka_node_admin_core::build_drive::InnerItem>>>,
    dispatched: tokio::sync::Notify,
}

struct FedStream(tokio::sync::mpsc::UnboundedReceiver<rafka_node_admin_core::build_drive::InnerItem>);

#[async_trait::async_trait]
impl rafka_node_admin_core::build_drive::InnerStream for FedStream {
    async fn next(&mut self) -> rafka_node_admin_core::build_drive::InnerItem {
        self.0.recv().await.unwrap_or(rafka_node_admin_core::build_drive::InnerItem::Ended)
    }
}

#[async_trait::async_trait]
impl rafka_node_admin_core::build_drive::Dispatcher for Fed {
    async fn dispatch(&self, _turn: &rafka_node_admin_core::park_gate::Turn, _: &PathName, _: &BuildId, _: u32, _: rafka_node_rpc_contract::context::CallContext, _: Vec<Vec<u8>>, _: rafka_node_admin_core::executor::RunPlan) -> rafka_node_admin_core::build_drive::Dispatched {
        let items = self.items.lock().unwrap().take().expect("one dispatch");
        self.dispatched.notify_one();
        rafka_node_admin_core::build_drive::Dispatched::Stream(Box::new(FedStream(items)))
    }
}

/// CONTRACT (node-rpc-envelope.md "TakeSeats: Build-driven seat transfer": "after the row P ends the
/// caller's outer stream with the typed new-seat frame, the caller resubmits the same build_id to N,
/// and N resumes the same attempt as the new fabric-primary"; "The reply opens N's execution gate"):
/// the fence is set while the drive waits on the executor's run, and the record names that executor:
/// the transfer is the run's own SeatTransfer step. The executor is the fabric-primary once it has
/// installed the committed seats, and it says so on the run's own stream, which it ends with
/// SeatMoved naming itself. Until then the callers' streams carry the run's frames and no seat loss;
/// on the executor's SeatMoved the drive ends `SeatLost` naming the executor, and no later attempt
/// is claimed (the executor resumes the same attempt).
/// Must NOT happen: a caller's stream ending with the seat loss, which sends the caller to the
/// executor, before the executor's run said it holds the seat.
#[tokio::test]
async fn a_seat_row_naming_the_executor_ends_the_drive_on_the_executors_new_seat_frame_never_before() {
    use rafka_node_admin_core::build_drive::InnerItem;
    let r = rig().await;
    let (feed, items) = tokio::sync::mpsc::unbounded_channel();
    let fed = Arc::new(Fed { items: Mutex::new(Some(items)), dispatched: tokio::sync::Notify::new() });
    let e = &r.env;
    let env = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: fed.clone(),
        gate: e.gate.clone(),
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let (e, d) = (env.clone(), drive.clone());
    let running = tokio::spawn(async move { e.run(&d).await });
    tokio::time::timeout(Duration::from_secs(5), fed.dispatched.notified()).await.expect("attempt 1 is dispatched");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.2")), "attempt 1 runs at the executor");
    // The executor's SeatTransfer step reached the holder, whose row is the fence.
    r.switch.fence_to(Some("mesh1.admin.2"));
    let step = BuildReply::Step { build_id: r.build.to_string(), attempt: 1, operation: "stop-node:mesh1.admin.1".into(), step: "SeatTransfer".into() };
    feed.send(InnerItem::Frame(step.clone())).unwrap();
    match next_within(&mut reader).await {
        Some(Read::Frame(f)) => assert_eq!(f, step, "the run's frame reaches the callers"),
        other => panic!("before the executor says it holds the seat, the callers read the run's frames and no seat loss, got {other:?}"),
    }
    assert!(next_within(&mut reader).await.is_none(), "no seat loss before the executor's new-seat frame");
    assert!(!running.is_finished(), "the drive waits for the executor's new-seat frame");
    // The executor installed the committed seats: its run's stream ends with the new seat.
    feed.send(InnerItem::Frame(BuildReply::SeatMoved { build_id: r.build.to_string(), fabric_primary: Some("mesh1.admin.2".into()) })).unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), running).await.expect("the drive ends on the executor's new-seat frame").unwrap();
    assert!(matches!(&end, DriveEnd::SeatLost(why) if why.contains("no longer holds the fabric-primary seat: the committed seat row moved it")), "the fence ends the drive SeatLost: {end:?}");
    match next_within(&mut reader).await {
        Some(Read::SeatLost(loss)) => assert_eq!(loss.to.as_deref(), Some("mesh1.admin.2"), "the stream names the executor the transfer seated: {loss:?}"),
        other => panic!("the reader ends with the seat loss, got {other:?}"),
    }
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.2")), "no later attempt was claimed: the executor resumes attempt 1");
}

/// The terminal `drive` ends with, read from its first frame on.
async fn terminal_of(drive: &Arc<Drive>) -> BuildReply {
    let mut reader = drive.reader(0);
    loop {
        match tokio::time::timeout(Duration::from_secs(5), reader.next()).await.expect("the drive ends") {
            Some(Read::Terminal(t)) => return t,
            Some(Read::Frame(_)) => {}
            other => panic!("the drive ended without a terminal: {other:?}"),
        }
    }
}

/// CONTRACT (node-rpc-envelope.md "Caller execution and certainty": "a completed Build answers
/// AlreadyApplied with its terminal frame; a running Build with a live holder returns that holder's
/// stream"): the drive that completed attempt N is the record of that run. A caller whose read of
/// the Build predates the verdict of N (it showed attempt N running) is handed that drive, and
/// nothing runs again; once attempt N+1 is opened on the complete Build (a restart reopens it), the
/// caller is handed a new drive, which claims and dispatches N+1.
/// Must NOT happen: the completed drive replaced for a stale read (a second `build.attempt.run` of
/// attempt N), or kept for the reopened Build (attempt N+1 never claimed).
#[tokio::test]
async fn a_completed_drive_is_kept_for_its_own_attempt_and_replaced_once_the_build_is_reopened() {
    use rafka_node_admin_core::build_drive::Drives;
    use rafka_node_admin_core::build_state::{AttemptOpened, AttemptOutcome, AttemptReason, BuildAttemptReceipt};
    let r = rig().await;
    let drives = Arc::new(Drives::default());
    let first = drives.ensure(&r.env, &r.build, 1);
    let BuildReply::Complete { attempt: done, .. } = terminal_of(&first).await else { panic!("the first run completes the Build") };
    r.builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: r.build.clone(), attempt: done, outcome: AttemptOutcome::Converged }).await.unwrap();
    let stale = drives.ensure(&r.env, &r.build, done);
    assert!(Arc::ptr_eq(&stale, &first), "a read that showed attempt {done} running is handed the drive that completed it");
    r.builds
        .open_attempt(&AttemptOpened { mesh: None, build_id: r.build.clone(), attempt: done + 1, reason: AttemptReason::Restart, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 0 })
        .await
        .unwrap();
    let reopened = drives.ensure(&r.env, &r.build, done + 1);
    assert!(!Arc::ptr_eq(&reopened, &first), "the reopened Build is driven by a new drive");
    assert!(matches!(terminal_of(&reopened).await, BuildReply::Complete { attempt, .. } if attempt == done + 1), "the new drive claims and ends attempt {}", done + 1);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), done), ("mesh1.admin.2".to_string(), done + 1)], "one build.attempt.run per attempt");
}

/// Ends a mesh shutdown the way the owner's workflow does when the leaving mesh's primary cannot
/// finish the leave (`admin/leave.rs`): `Incomplete` while the primary is lost, naming the workflow;
/// once it is not, the shutdown runs and the mesh leaves the view. Every other operation runs as in
/// [`Creates`].
struct LostLeaver {
    ran: std::sync::OnceLock<Arc<Creates>>,
    topology: Arc<RwLock<Topology>>,
    /// What the shutdown ends with while the primary is lost.
    ends: Mutex<Option<rafka_node_admin_core::executor::RunEnd>>,
    /// The incomplete shutdown first records a receipt of the leave (an exit, an amendment): the
    /// attempt moved something before it ended.
    progress: std::sync::atomic::AtomicBool,
    /// The shutdown ends incomplete on its first run only.
    once: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl OperationRunner for LostLeaver {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        let ends = self.ends.lock().unwrap().clone();
        if let (BuildOperation::ShutdownMesh { .. }, Some(end)) = (op, ends) {
            if self.progress.load(Ordering::SeqCst) {
                let receipt = BuildStepReceipt { mesh: None, build_id: build.clone(), attempt, operation: op.key(), step: "RosterAmended".into(), outcome: StepOutcome::Complete, output: None, executor: None };
                self.ran.get().expect("the rig set the recorder").1.append_step_receipt(&receipt).await.map_err(|e| e.to_string())?;
            }
            if self.once.load(Ordering::SeqCst) {
                *self.ends.lock().unwrap() = None;
            }
            return Err(end);
        }
        self.ran.get().expect("the rig set the recorder").run(build, lane, attempt, op).await?;
        if let BuildOperation::ShutdownMesh { mesh, .. } = op {
            self.topology.write().await.nodes.retain(|n| n.mesh != *mesh);
        }
        Ok(())
    }
}

/// The fixture view with a second mesh, `mesh2`, whose primary is `mesh2.admin.1`, and the Build
/// that removes it: the fabric-primary `mesh1.admin.1` executes the mesh's shutdown (`executor_for`).
async fn removing_mesh2(ends: Option<rafka_node_admin_core::executor::RunEnd>) -> (Rig, Arc<LostLeaver>) {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    t.nodes.push(node("mesh2.admin.1", true, false));
    t.nodes.push(node("mesh2.rpc.1", false, false));
    let held: Arc<Mutex<Option<Arc<LostLeaver>>>> = Arc::default();
    let slot = held.clone();
    let r = rig_for(t, TopologyChange::RemoveMesh { mesh: "mesh2".into() }, move |ran, topology| {
        let mesh_deleter = Arc::new(LostLeaver { ran: std::sync::OnceLock::new(), topology, ends: Mutex::new(ends), progress: AtomicBool::new(false), once: AtomicBool::new(false) });
        let _ = mesh_deleter.ran.set(ran);
        *slot.lock().unwrap() = Some(mesh_deleter.clone());
        mesh_deleter
    })
    .await;
    let mesh_deleter = held.lock().unwrap().clone().expect("the rig built the runner");
    (r, mesh_deleter)
}

/// The Build the rig seeded before the one under test (the older one).
async fn seeded_before(r: &Rig) -> rafka_node_admin_core::build_state::CurrentBuild {
    use rafka_node_admin_core::build_state::{BuildFact, CurrentBuild};
    let older = r.builds.facts().await.unwrap().into_iter().find_map(|f| match f {
        BuildFact::Accepted(a) if a.build_id != r.build => Some(a.build_id),
        _ => None,
    });
    CurrentBuild::Completed(older.expect("the rig seeded a Build before the one under test"))
}

const LEAVE_INCOMPLETE: &str = "shutdown-mesh: mesh2.admin.1 sent no valid mesh-deleted; the workflow is incomplete and its receipts stand";

/// CONTRACT (R-BI4; mesh-delete.md, "Failure and receipts": "The next attempt resumes from the
/// receipts"; "Resume after the primary's loss"): a mesh.delete whose leaving mesh's primary is lost
/// ends its attempt incomplete, not failed. The attempt is handed back to the fabric-primary, so the
/// Build is still in progress and the current-Build read (last in, first out) returns it: it is not a
/// failed Build the read skips. The drive claims no further attempt while nothing the resume decides
/// on has moved, and names why; when the leaving mesh's primary is proven gone it claims the next
/// attempt, which runs the shutdown from the receipts and completes the Build.
/// Must NOT happen: the Build failed and skipped (the current Build becomes the older one, nothing
/// resumes the leave), or an attempt claimed again on every pass of the drive.
#[test]
fn a_leave_that_ends_incomplete_on_a_lost_primary_leaves_the_build_in_progress_and_resumes_when_the_loss_is_proven() {
    use rafka_node_admin_core::build_state::{AttemptOutcome, BuildFact, BuildState, CurrentBuild};
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, first, second, third, facts, ran, dispatched, after) = cap.run(async {
        let root = tracing::info_span!("cell.leave-incomplete-stays-in-progress");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rafka_node_admin_core::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        // Attempt 1: the shutdown ends incomplete. One drive across the passes, as the fabric-primary keeps it.
        let drive = Drive::detached(r.build.clone());
        let first = r.env.run(&drive).await;
        let first = (first, r.builds.read_build(&r.build).await.unwrap(), r.builds.current_build().await.unwrap());
        // The same drive again with nothing moved: no attempt is claimed.
        let second = r.env.run(&drive).await;
        let second = (second, r.builds.read_build(&r.build).await.unwrap().attempt, r.dispatch.dispatched.lock().unwrap().len());
        // The leaving mesh's primary is proven gone: it is no longer live in the view, and the shutdown can run.
        *mesh_deleter.ends.lock().unwrap() = None;
        for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.1") {
            n.status = NodeStatus::Dead;
            n.is_primary = false;
        }
        let third = r.env.run(&drive).await;
        let third = (third, r.builds.read_build(&r.build).await.unwrap(), r.builds.current_build().await.unwrap());
        let facts = r.builds.facts().await.unwrap();
        let ran = r.ran.0.lock().unwrap().clone();
        let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
        (trace_id, first, second, third, facts, ran, dispatched, r.build.clone())
    });
    let spans = cap.spans();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/leave_incomplete_in_progress");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let (end, p, current) = first;
    assert_eq!(p.state, BuildState::Pending, "the attempt ended handed back to the fabric-primary: the Build waits for its next claim, it is not failed");
    assert_eq!(current, CurrentBuild::InProgress(after.clone()), "the current-Build read returns the Build in progress; a failed Build would be skipped for the seeded one");
    assert!(matches!(&end, DriveEnd::Stalled(why) if why.contains("attempt 1 of Build") && why.contains("ended incomplete and was handed back to this fabric-primary with nothing moved")), "the drive names why it claims nothing more: {end:?}");
    let verdict = facts.iter().find_map(|f| match f {
        BuildFact::Attempt(a) if a.build_id == after && a.attempt == 1 => Some(a.outcome.clone()),
        _ => None,
    });
    assert_eq!(verdict, Some(AttemptOutcome::HandedOff { to: "mesh1.admin.1".into() }), "attempt 1's verdict hands back to the fabric-primary: {verdict:?}");
    let (end, attempt, dispatches) = second;
    assert!(matches!(end, DriveEnd::Stalled(_)), "nothing moved: the drive stalls again, {end:?}");
    assert_eq!((attempt, dispatches), (1, 1), "no attempt is claimed and no executor is called while nothing the resume decides on has moved");
    let (end, p, current) = third;
    assert_eq!(end, DriveEnd::Terminal, "the proven loss moved an input: attempt 2 is claimed and runs the shutdown to its end");
    assert_eq!((p.attempt, p.state), (2, BuildState::Complete));
    assert_eq!(current, CurrentBuild::Completed(after), "the resumed Build is the current one, completed");
    assert_eq!(ran.iter().filter(|k| k.starts_with("shutdown-mesh:")).count(), 1, "the shutdown ran once, on the resumed attempt: {ran:?}");
    assert_eq!(dispatched, vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.1".to_string(), 2)], "attempt 2 was claimed for the fabric-primary, once");
    // The executor's run is parented to the attempt's recorded context (the claim's), so it is read by name.
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.build.update.via-reconcile").iter().any(|s| s["attributes"]["attempt"] == "1" && s["attributes"]["outcome"] == "handed-off" && s["attributes"]["incomplete"] == LEAVE_INCOMPLETE), "the executor's span says the attempt ended handed back, incomplete, with the reason");
    assert!(named("rdm.node_admin.build.update.via-reconcile").iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["outcome"] == "converged"), "attempt 2 resumed the shutdown and converged");
    let stalls: Vec<_> = named("rdm.node_admin.build.update.via-stalled").into_iter().filter(|s| s["trace_id"] == trace_id.as_str()).collect();
    assert_eq!(stalls.len(), 1, "the idle wait is named once across the two passes that made it: {stalls:?}");
}

/// CONTRACT (R-BI4: "Only an attempt that ended with a named terminal failure makes a Build
/// failed"): a shutdown that ends with the workflow's own named failure (not a lost participant) ends
/// the attempt `Failed`; the Build is failed, the current-Build read skips it and returns the Build
/// before it, and the drive claims nothing more: a caller resubmits it.
#[test]
fn a_leave_that_ends_with_a_named_failure_fails_the_build_and_the_read_skips_it() {
    use rafka_node_admin_core::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (end, p, current, seeded, dispatched) = cap.run(async {
        let root = tracing::info_span!("cell.leave-named-failure-fails-the-build");
        let _in_root = root.enter();
        let (r, _leaver) = removing_mesh2(Some(rafka_node_admin_core::executor::RunEnd::Failed("shutdown-mesh: mesh2 is refused before it is dismantled: its handover is not confirmed".into()))).await;
        let seeded = seeded_before(&r).await;
        let end = r.env.run(&Drive::detached(r.build.clone())).await;
        let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
        (end, r.builds.read_build(&r.build).await.unwrap(), r.builds.current_build().await.unwrap(), seeded, dispatched)
    });
    assert_eq!(end, DriveEnd::Terminal, "the named failure ends the drive");
    assert_eq!(p.state, BuildState::Failed);
    assert_eq!(current, seeded, "the failed Build is skipped: the Build before it is current again");
    assert_eq!(dispatched, vec![("mesh1.admin.1".to_string(), 1)], "no further attempt is claimed");
}

/// Moves what the resume of a leave decides on: `mesh2.admin.1`'s status in the view.
async fn mesh2_primary_becomes(r: &Rig, status: NodeStatus) {
    for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.1") {
        n.status = status;
        n.is_primary = status.is_live();
    }
}

/// CONTRACT (R-D1; mesh-delete.md, "Resume after the primary's loss": "A silent but unproven primary
/// refuses the resume by name; silence is not death"): the peer-mesh investigation holds `mesh2`
/// unheard, every birth of it silent, and the leave of it ends incomplete because its primary is
/// unproven. The hold fences restart, replace and delete of the mesh's births, never the shutdown's
/// resume: the shutdown is dispatched on attempt 1, the Build stays in progress and nothing is claimed
/// again while the primary stays unproven, and once its exit is proven attempt 2 runs the shutdown
/// and completes the Build.
/// Must NOT happen: the drive stopping at the unheard-mesh fence before the shutdown, the Build
/// failed, or the Build left in progress with no attempt claimed after the proof.
#[test]
fn a_leave_of_a_mesh_held_unheard_is_refused_by_name_while_the_primary_is_unproven_and_resumes_when_it_is_proven() {
    use rafka_node_admin_core::build_state::{BuildState, CurrentBuild};
    let cap = crate::mesh_delete_resume::capture();
    let (first, second, third, current, dispatched, ran) = cap.run(async {
        let root = tracing::info_span!("cell.leave-unheard-unproven-primary");
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rafka_node_admin_core::executor::RunEnd::Incomplete("shutdown-mesh: the final primary mesh2.admin.1 is silent but not proven exited, so no admin is created for it".into()))).await;
        mesh2_primary_becomes(&r, NodeStatus::PendingReconnect).await;
        for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.mesh == "mesh2") {
            n.status = NodeStatus::PendingReconnect;
        }
        r.switch.peer.lock().unwrap().insert("mesh2".into(), rafka_node_admin_core::investigate::PeerMesh { unheard_ms: Some(120_000), rebirth_decided: false, unreachable_decided: false });
        let drive = Drive::detached(r.build.clone());
        let first = r.env.run(&drive).await;
        let dispatches = r.dispatch.dispatched.lock().unwrap().len();
        let first = (first, r.builds.read_build(&r.build).await.unwrap().state, r.builds.current_build().await.unwrap(), dispatches);
        let second = r.env.run(&drive).await;
        let dispatches = r.dispatch.dispatched.lock().unwrap().len();
        let second = (second, r.builds.read_build(&r.build).await.unwrap().attempt, dispatches);
        // The provider proves the primary's exit: it is gone from the view, and the shutdown can run.
        *mesh_deleter.ends.lock().unwrap() = None;
        mesh2_primary_becomes(&r, NodeStatus::Dead).await;
        let third = r.env.run(&drive).await;
        let current = r.builds.current_build().await.unwrap();
        let (dispatched, ran) = (r.dispatch.dispatched.lock().unwrap().clone(), r.ran.0.lock().unwrap().clone());
        (first, second, (third, r.builds.read_build(&r.build).await.unwrap().state), current, dispatched, (ran, r.build.clone()))
    });
    let spans = cap.spans();
    let (end, state, current_then, dispatches) = first;
    assert!(matches!(&end, DriveEnd::Stalled(why) if why.contains("ended incomplete and was handed back")), "the drive names the incomplete attempt: {end:?}");
    assert_eq!((state, dispatches), (BuildState::Pending, 1), "the shutdown was dispatched on attempt 1 although mesh2 is held unheard, and the Build waits for its next claim");
    assert!(matches!(current_then, CurrentBuild::InProgress(_)), "the Build is in progress, not failed: {current_then:?}");
    let (end, attempt, dispatches) = second;
    assert!(matches!(end, DriveEnd::Stalled(_)) && (attempt, dispatches) == (1, 1), "the unproven primary refuses the resume by name and no attempt is claimed: {end:?} attempt {attempt}, {dispatches} dispatches");
    let (end, state) = third;
    assert_eq!((end, state), (DriveEnd::Terminal, BuildState::Complete), "the proven exit moved an input: attempt 2 runs the shutdown and completes the Build");
    let (ran, build) = ran;
    assert_eq!(current, CurrentBuild::Completed(build));
    assert_eq!(ran.iter().filter(|k| k.starts_with("shutdown-mesh:")).count(), 1, "{ran:?}");
    assert_eq!(dispatched, vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.1".to_string(), 2)]);
    assert!(!spans.iter().any(|s| s["name"] == "rdm.node_admin.build.reject.via-unheard-mesh-drive"), "the unheard hold did not fence the shutdown's resume");
    assert!(spans.iter().any(|s| s["name"] == "rdm.node_admin.build.update.via-stalled"), "the refusal is spanned by name");
}

/// CONTRACT (R-S2: event-driven, never once per pass; mesh-delete.md, "Failure and receipts"): an
/// attempt of a leave that ends incomplete with nothing changed since it was claimed gets no next
/// attempt from the drive, however many times the drive passes; one input moving (the seat of a
/// member of the leaving mesh) gets exactly one. Attempt 1 ends incomplete, two passes claim nothing,
/// the move opens attempt 2, which ends incomplete the same way and again claims nothing, and the
/// next move opens attempt 3, which completes.
/// Must NOT happen: an attempt claimed per pass of a leave whose primary stays lost.
#[test]
fn a_leave_that_ends_incomplete_with_nothing_changed_claims_one_attempt_per_change_never_one_per_pass() {
    use rafka_node_admin_core::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (passes, state, dispatched) = cap.run(async {
        let root = tracing::info_span!("cell.leave-one-attempt-per-change");
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rafka_node_admin_core::executor::RunEnd::Incomplete("shutdown-mesh: mesh2 holds no node-admin primary to hand the workflow to".into()))).await;
        let drive = Drive::detached(r.build.clone());
        let mut passes = Vec::new();
        for pass in 0..6 {
            match pass {
                2 => mesh2_primary_becomes(&r, NodeStatus::PendingReconnect).await,
                4 => {
                    *mesh_deleter.ends.lock().unwrap() = None;
                    mesh2_primary_becomes(&r, NodeStatus::Dead).await;
                }
                _ => {}
            }
            let end = r.env.run(&drive).await;
            passes.push((matches!(end, DriveEnd::Stalled(_)), r.builds.read_build(&r.build).await.unwrap().attempt));
        }
        let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
        (passes, r.builds.read_build(&r.build).await.unwrap().state, dispatched)
    });
    // Passes 0-1: attempt 1, then nothing. Pass 2 moves an input: attempt 2, which ends incomplete the same way.
    // Pass 3: nothing. Pass 4 moves the next input: attempt 3 completes. Pass 5: the Build is complete.
    assert_eq!(passes[..5], [(true, 1), (true, 1), (true, 2), (true, 2), (false, 3)], "one attempt for the failure, none while nothing changed, one for each change: {passes:?}");
    assert_eq!(state, BuildState::Complete);
    assert_eq!(dispatched, vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.1".to_string(), 2), ("mesh1.admin.1".to_string(), 3)], "three attempts for three states of the world, not one per pass");
}

/// CONTRACT (mesh-delete.md, "Resume after the primary's loss": the lost primary's Exited is recorded,
/// the amendment admits the new primary, "the new mesh-primary gets a fresh claim and runs only the
/// tiers without a terminal receipt"): a leave whose primary was lost records the exit and the
/// amendment as receipts of its Build and ends incomplete because the new primary is not seated yet.
/// The receipts are what moved: the Build stays in progress and the drive claims the next attempt at
/// once, with no wait for any other change, and that attempt completes the leave.
/// Must NOT happen: the drive idling after an attempt that recorded receipts, or the Build failed.
#[test]
fn a_leave_that_recorded_the_exit_and_the_amendment_before_it_ended_incomplete_is_resumed_by_the_next_attempt_at_once() {
    use rafka_node_admin_core::build_state::{BuildFact, BuildState};
    let cap = crate::mesh_delete_resume::capture();
    let (end, state, attempt, dispatched, steps) = cap.run(async {
        let root = tracing::info_span!("cell.leave-recorded-then-incomplete");
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rafka_node_admin_core::executor::RunEnd::Incomplete("shutdown-mesh: mesh2.admin.3 was created for mesh2 and holds no mesh-primary seat yet; the next attempt amends the roster for it".into()))).await;
        mesh_deleter.progress.store(true, Ordering::SeqCst);
        mesh_deleter.once.store(true, Ordering::SeqCst);
        let end = r.env.run(&Drive::detached(r.build.clone())).await;
        let p = r.builds.read_build(&r.build).await.unwrap();
        let steps: Vec<_> = r.builds.facts().await.unwrap().into_iter().filter_map(|f| if let BuildFact::Step(s) = f { Some((s.attempt, s.step)) } else { None }).collect();
        let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
        (end, p.state, p.attempt, dispatched, steps)
    });
    assert_eq!(end, DriveEnd::Terminal, "attempt 1 moved the leave on, so the drive went straight on to attempt 2");
    assert_eq!((state, attempt), (BuildState::Complete, 2));
    assert_eq!(dispatched, vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.1".to_string(), 2)]);
    assert!(steps.contains(&(1, "RosterAmended".to_string())), "attempt 1's receipt stands for the resumed attempt: {steps:?}");
}

/// An operation runner that holds its first operation until the cell lets it go: the attempt is
/// running at its executor while it is held. Each operation's receipt is the fixture's (`Creates`).
struct HeldUntilLetGo {
    ran: Arc<Creates>,
    began: tokio::sync::watch::Sender<bool>,
    let_go: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl OperationRunner for HeldUntilLetGo {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        self.began.send_replace(true);
        let _go = self.let_go.acquire().await.expect("the cell never closes its semaphore");
        self.ran.run(build, lane, attempt, op).await
    }
}

/// CONTRACT (park_gate.rs: a task that takes the view lock begins each turn at the gate's wait
/// point, and a stop's quiesce resolves once every turn already past its wait point has ended;
/// node-stop.md steps 3-4, R-ON1): the drive is one of its admin's gated tasks. A drive whose
/// attempt is running at its executor when the admin's gate parks is a turn in progress at the
/// park: the park names it, the quiesce waits for it, and resolves only once the drive handled the
/// attempt's terminal and recorded the Build's end. Must NOT happen: a park that finds no turn of a
/// drive whose attempt is running, so a stop cuts while that drive still claims, dispatches or
/// records.
#[tokio::test]
async fn a_drive_whose_attempt_is_running_when_its_admin_stops_is_a_turn_the_stops_quiesce_waits_for() {
    let (began, mut running) = tokio::sync::watch::channel(false);
    let let_go = Arc::new(tokio::sync::Semaphore::new(0));
    let held = let_go.clone();
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) };
    let r = rig_for(view(), change, move |ran, _| -> Arc<dyn OperationRunner> { Arc::new(HeldUntilLetGo { ran, began, let_go: held }) }).await;
    let gate = r.env.turns.clone();
    let drive = Drive::detached(r.build.clone());
    let (env, d) = (r.env.clone(), drive.clone());
    let driving = tokio::spawn(async move { env.run(&d).await });
    tokio::time::timeout(Duration::from_secs(5), running.wait_for(|b| *b)).await.expect("attempt 1 never reached its operation").unwrap();
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "attempt 1 is running at the executor");
    // The stop parks the gate while the attempt runs.
    let at = gate.park();
    assert!(at.turns.contains_key("build-drive"), "the park finds the drive's turn in progress while its attempt runs; turns at the park: [{}]", at.named());
    let quiesce = tokio::spawn({
        let gate = gate.clone();
        async move { gate.quiesce().await }
    });
    tokio::task::yield_now().await;
    assert!(!quiesce.is_finished(), "the quiesce resolved while the drive's attempt was running");
    let_go.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), quiesce).await.expect("the quiesce never resolved once the attempt ended").unwrap();
    assert!(drive.completed(), "the quiesce resolved before the drive recorded the Build's end");
    assert_eq!(tokio::time::timeout(Duration::from_secs(5), driving).await.expect("the drive ended with its turn").unwrap(), DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh1.rpc.2".to_string()]);
}

/// CONTRACT (park_gate.rs: no turn begins while the gate is parked; node-stop.md step 4, R-ON1: a
/// stopped node-admin calls nothing out): a drive that begins while its admin's gate is parked (the
/// executor's last turn before a stop ensured it) waits at the gate's wait point. It claims no
/// attempt and calls no executor until the gate opens (a start), and then drives the Build to its
/// end. Must NOT happen: a parked admin's drive claiming an attempt or calling an executor.
#[tokio::test(start_paused = true)]
async fn a_drive_that_begins_once_its_admin_stopped_claims_and_dispatches_nothing_until_the_admin_starts() {
    let r = rig().await;
    let gate = r.env.turns.clone();
    gate.park();
    let drive = Drive::detached(r.build.clone());
    let (env, d) = (r.env.clone(), drive.clone());
    let mut driving = tokio::spawn(async move { env.run(&d).await });
    // Paused time moves on only once every task is idle: a drive that could go on has gone as far
    // as it can by then.
    let while_stopped = tokio::time::timeout(Duration::from_secs(60), &mut driving).await;
    assert!(while_stopped.is_err(), "the drive ended while its admin's gate was parked: {while_stopped:?}");
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed while the gate was parked");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called while the gate was parked");
    gate.resume();
    assert_eq!(tokio::time::timeout(Duration::from_secs(60), driving).await.expect("the drive went on once the gate opened").unwrap(), DriveEnd::Terminal);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "attempt 1 is claimed and run once the gate opened");
}

/// The view after the old holder `mesh1.admin.2` committed its seat row naming this admin, the
/// fabric-primary `mesh1.admin.1`, its successor for both seats: this admin's view names it the
/// holder of both, and the old holder holds none.
fn view_after_the_row() -> Topology {
    let mut t = view();
    t.nodes = vec![node("mesh1.admin.1", true, true), node("mesh1.admin.2", false, false), node("mesh1.rpc.1", false, false)];
    t
}

/// A gate over the seat rows the fixture committed: each update a row records. It records every
/// acknowledgement the drive asks for, with how many dispatches had been made when it asked, and
/// refuses it with `refusal` when the cell sets one.
struct Acknowledges {
    inner: Arc<Switch>,
    rows: Vec<PrecedingUpdate>,
    dispatch: Arc<crate::loopback::Loopback>,
    asked: Mutex<Vec<(PrecedingUpdate, usize)>>,
    refusal: Option<String>,
}

#[async_trait::async_trait]
impl Gate for Acknowledges {
    fn authorizes(&self) -> bool {
        self.inner.authorizes()
    }
    async fn fence(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.fence().await
    }
    async fn fence_set(&self) {
        self.inner.fence_set().await
    }
    async fn seats_acknowledged(&self, updates: &[PrecedingUpdate], _: &BuildId, _: u32) -> Result<(), String> {
        let dispatched = self.dispatch.dispatched.lock().unwrap().len();
        for u in updates.iter().filter(|u| self.rows.contains(u)) {
            self.asked.lock().unwrap().push((u.clone(), dispatched));
            if let Some(why) = &self.refusal {
                return Err(why.clone());
            }
        }
        Ok(())
    }
}

/// The Build removing the old holder `mesh1.admin.2` after its row, driven by this admin, its
/// successor, through `Acknowledges` over that row (`refusal` set: the acknowledgement fails).
async fn after_the_row(refusal: Option<&str>) -> (Rig, Arc<DriveEnv>, Arc<Acknowledges>, PrecedingUpdate) {
    let r = rig_for(view_after_the_row(), TopologyChange::RemoveNode { node: "mesh1.admin.2".parse().unwrap() }, |ran, _| ran).await;
    let op = "retire-node:mesh1.admin.2";
    let row = PrecedingUpdate { mesh: "mesh1".into(), operation: rafka_node_admin_core::fabric_handover::update_operation(rafka_mesh_entity::Seat::MeshPrimary, r.build.as_str(), 1, op), holder: Some("mesh1.admin.2".parse().unwrap()) };
    let gate = Arc::new(Acknowledges { inner: r.switch.clone(), rows: vec![row.clone()], dispatch: r.dispatch.clone(), asked: Mutex::new(Vec::new()), refusal: refusal.map(str::to_string) });
    let e = &r.env;
    let env = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: gate.clone(),
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: e.turns.clone(),
    });
    (r, env, gate, row)
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 failure
/// contract: "the next writer to read the row publishes 'updated'"; "The workflow continues only
/// after the result is installed and acknowledged: node.primary.updated answered to the resuming
/// executor's call, never a read alone"): this admin is the successor the old holder's committed
/// row names, and its view already names it the holder of both seats, so no seat moves before the
/// Build's operation on the old holder. The drive still asks its gate to acknowledge the update the
/// row records before it dispatches that operation: the acknowledgement is asked with no dispatch
/// made, and the operation is dispatched once after it. Must NOT happen: the operation on the old
/// holder dispatched on the view alone, before the update the row records is acknowledged.
#[tokio::test]
async fn a_successor_whose_view_already_holds_the_seats_acknowledges_the_rows_update_before_it_dispatches_on_the_old_holder() {
    let (r, env, gate, row) = after_the_row(None).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(env.run(&drive).await, DriveEnd::Terminal);
    let asked = gate.asked.lock().unwrap().clone();
    assert_eq!(asked.first(), Some(&(row.clone(), 0)), "the row's update is acknowledged before any dispatch: asked {asked:?}");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1)], "the operation on the old holder is dispatched once, to this admin, its mesh's primary");
    assert_eq!(r.ran.0.lock().unwrap().len(), 1, "it ran once: {:?}", r.ran.0.lock().unwrap());
}

/// CONTRACT (as above; C1 step 8: "Only then does the commanding Build continue"): an update the row
/// records that this admin cannot acknowledge (the successor's call fails, the row cannot be read)
/// leaves the Build where it is: the drive blocks once, naming why, stalls, and dispatches nothing.
/// Must NOT happen: the operation on the old holder dispatched past an update not acknowledged.
#[tokio::test]
async fn an_update_the_drive_cannot_acknowledge_stalls_the_drive_by_name_and_dispatches_nothing() {
    let (r, env, gate, row) = after_the_row(Some("the successor's call ended indeterminate")).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains(&row.operation) && why.contains("the successor's call ended indeterminate"), "the stall names the update and why: {why}"),
        other => panic!("an update not acknowledged stalls the drive: {other:?}"),
    }
    assert_eq!(gate.asked.lock().unwrap().len(), 1, "asked once: {:?}", gate.asked.lock().unwrap());
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing was dispatched: {:?}", r.dispatch.dispatched.lock().unwrap());
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran");
    match next_within(&mut reader).await {
        Some(Read::Frame(BuildReply::Blocked { step, reason, .. })) => assert!(step == "primary-update" && reason.contains(&row.operation), "the blocker names the update: {step} {reason}"),
        other => panic!("the caller reads the blocker naming the update, got {other:?}"),
    }
}

/// The view before the holder's row: the fabric-primary `mesh1.admin.1` holds both seats, and
/// `mesh1.admin.2` is the ready admin of its mesh that the update names its successor.
fn view_holder_of_both() -> Topology {
    let mut t = view();
    t.nodes = vec![node("mesh1.admin.1", true, true), node("mesh1.admin.2", false, false), node("mesh1.rpc.1", false, false)];
    t
}

/// The holder's own node.primary.update, run from its drive (`SeatGate::move_seats` for its own
/// seats): at the row the shared view names the successor the holder of both seats and the holder's
/// Build log decides nothing more, naming it (`row` resolves: the successor installs and its drive may begin);
/// then the update waits at steps 6-7 until the cell lets it go, and says "primary updated" (step 7).
struct HolderUpdate {
    inner: Arc<Switch>,
    topology: Arc<RwLock<Topology>>,
    row: Arc<tokio::sync::Notify>,
    let_go: Arc<tokio::sync::Semaphore>,
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl Gate for HolderUpdate {
    fn authorizes(&self) -> bool {
        self.inner.authorizes()
    }
    async fn fence(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.fence().await
    }
    async fn fence_set(&self) {
        self.inner.fence_set().await
    }
    async fn move_seats(&self, _: &rafka_node_admin_core::executor::SeatMove, _: &BuildId, attempt: u32, _: u32, _: &str) -> Result<(), rafka_node_admin_core::build_drive::SeatMoveEnd> {
        for n in self.topology.write().await.nodes.iter_mut() {
            let successor = n.name.to_string() == "mesh1.admin.2";
            (n.is_primary, n.is_fabric_primary) = (successor, successor);
        }
        self.inner.fence_to(Some("mesh1.admin.2"));
        self.events.lock().unwrap().push(format!("row attempt {attempt}"));
        self.row.notify_one();
        let _ = self.let_go.acquire().await.expect("the cell keeps the semaphore open");
        self.events.lock().unwrap().push("primary updated".into());
        Ok(())
    }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 7:
/// "The writer publishes 'primary updated' with the same identifiers"; step 8: "Only then does the
/// commanding Build continue"; build proposal Part A, build.md:365: "The next attempt is claimed after
/// the running attempt's terminal receipt (Complete, Failed or HandedOff), or, when the executor is
/// lost with the attempt unfinished, after exact departure proof. A live run is reattached, never
/// re-claimed"): the fabric-primary `mesh1.admin.1` holds both seats and its Build retires it, so its
/// drive moves both seats itself first. At its row the successor `mesh1.admin.2` holds the seats and
/// its own drive of the same Build begins, while the holder is still at steps 6-7. The update is a
/// step of the holder's attempt: the successor's drive reattaches to that attempt, claims nothing and
/// dispatches nothing until it ends, and only after the holder said "primary updated" does the attempt
/// end `HandedOff` to the successor, which then claims the next attempt and runs the retire.
/// Must NOT happen: the successor claiming an attempt, dispatching, or the retire running while the
/// holder (alive) has not reached step 7.
#[tokio::test(start_paused = true)]
async fn a_successor_claims_and_dispatches_nothing_while_the_holders_update_has_not_said_primary_updated() {
    let r = rig_for(view_holder_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let (row, let_go, events) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Semaphore::new(0)), Arc::new(Mutex::new(Vec::new())));
    let holder_gate = Arc::new(HolderUpdate { inner: r.switch.clone(), topology: r.env.topology.clone(), row: row.clone(), let_go: let_go.clone(), events: events.clone() });
    let e = &r.env;
    let holder = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: holder_gate,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: e.turns.clone(),
    });
    // The successor: its own door, gate and park gate over the fixture's one Build log, view and store
    // root (its mesh rows, `mesh_claims`, are the holder's).
    let n_name: PathName = "mesh1.admin.2".parse().unwrap();
    let n_id = r.env.topology.read().await.node(&n_name).unwrap().node_id.clone();
    let n_switch = Arc::new(Switch { authorizes: AtomicBool::new(true), fenced: AtomicBool::new(false), successor: Mutex::new(None), fence_tx: tokio::sync::watch::channel(false).0, record_holder: Mutex::new(None), peer: Mutex::new(Default::default()) });
    let successor = Arc::new(DriveEnv {
        me: n_name.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: Arc::new(ClaimDoor { node_id: n_id.clone(), me: n_name.clone(), topology: e.topology.clone(), builds: r.builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()), claims: e.door.claims.clone() }),
        dispatcher: e.dispatcher.clone(),
        gate: n_switch,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: rafka_node_admin_core::park_gate::ParkGate::new(),
    });
    let (p_drive, n_drive) = (Drive::detached(r.build.clone()), Drive::detached(r.build.clone()));
    let p_driving = tokio::spawn({
        let (env, d) = (holder.clone(), p_drive.clone());
        async move { env.run(&d).await }
    });
    tokio::time::timeout(Duration::from_secs(5), row.notified()).await.expect("the holder's update never wrote its row");
    let mut n_driving = tokio::spawn({
        let (env, d) = (successor.clone(), n_drive.clone());
        async move { env.run(&d).await }
    });
    // Paused time moves on only once every task is idle: the successor's drive has gone as far as it
    // can while the holder is at steps 6-7.
    let early = tokio::time::timeout(Duration::from_secs(60), &mut n_driving).await;
    let to_successor: Vec<(String, u32)> = r.dispatch.dispatched.lock().unwrap().iter().filter(|(to, _)| *to == n_name.to_string()).cloned().collect();
    assert!(r.ran.0.lock().unwrap().is_empty() && to_successor.is_empty() && early.is_err(), "while the holder had not said primary updated, the successor's drive dispatched {to_successor:?}, ran {:?}, and ended {early:?}; events {:?}", r.ran.0.lock().unwrap(), events.lock().unwrap());
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.1")), "the update runs in the holder's own attempt, and no other is claimed");
    let_go.add_permits(1);
    assert_eq!(tokio::time::timeout(Duration::from_secs(60), n_driving).await.expect("the successor's drive went on once the holder's attempt ended").unwrap(), DriveEnd::Terminal);
    assert!(matches!(tokio::time::timeout(Duration::from_secs(60), p_driving).await.expect("the holder's drive ended").unwrap(), DriveEnd::SeatLost(_)), "the holder's drive ends at its row");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(p.attempt_executors.get(&1).map(String::as_str), Some("mesh1.admin.1"), "attempt 1 is the holder's: {:?}", p.attempt_executors);
    assert_eq!(p.attempt_executors.get(&2).map(String::as_str), Some("mesh1.admin.2"), "the successor claimed the next attempt after the holder's ended: {:?}", p.attempt_executors);
    let handed: Vec<_> = r.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
        rafka_node_admin_core::build_state::BuildFact::Attempt(a) if a.build_id == r.build && a.attempt == 1 => Some(a.outcome),
        _ => None,
    }).collect();
    assert_eq!(handed, vec![rafka_node_admin_core::build_state::AttemptOutcome::HandedOff { to: "mesh1.admin.2".into() }], "the holder's attempt ended handed off to the successor");
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["retire-node:mesh1.admin.1".to_string()], "the retire ran once, at the successor");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.2".to_string(), 2)], "the successor reattached to the holder's attempt, then dispatched the next to itself");
    assert_eq!(*events.lock().unwrap(), vec!["row attempt 1".to_string(), "primary updated".into()]);
}

/// The holder's own node.primary.update refused before its row, as the door refuses it with no
/// successor (`fabric_handover::update_primary` step 2: nothing said, cut off or written): the view is
/// left as it is and the refusal is named as the admin's gate names a refused reply.
struct RefusedUpdate {
    inner: Arc<Switch>,
    calls: std::sync::atomic::AtomicUsize,
    gone_then_move: Option<(Arc<RwLock<Topology>>, Arc<rafka_node_admin_core::build_state::MemoryBuildStateAdapter>, &'static str, Arc<Mutex<Vec<(u32, u32, Option<String>)>>>)>,
}

/// The Q7 text the refused update names.
const NO_SECONDARY: &str = "cannot transfer: no secondary of type node-admin in mesh mesh1";

#[async_trait::async_trait]
impl Gate for RefusedUpdate {
    fn authorizes(&self) -> bool {
        self.inner.authorizes()
    }
    async fn fence(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.fence().await
    }
    async fn fence_set(&self) {
        self.inner.fence_set().await
    }
    async fn move_seats(&self, m: &rafka_node_admin_core::executor::SeatMove, build: &BuildId, attempt: u32, run: u32, _: &str) -> Result<(), rafka_node_admin_core::build_drive::SeatMoveEnd> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some((topology, builds, successor, calls)) = &self.gone_then_move {
            let claimed = builds.read_build(build).await.ok().and_then(|p| p.attempt_executors.get(&attempt).cloned());
            let first = {
                let mut recorded = calls.lock().unwrap();
                recorded.push((attempt, run, claimed));
                recorded.len() == 1
            };
            if first {
                return Err(rafka_node_admin_core::build_drive::SeatMoveEnd::Failed { step: "primary-update".into(), reason: GONE_REASON.into() });
            }
            for n in topology.write().await.nodes.iter_mut() {
                if n.mesh == m.holder.mesh {
                    let is_succ = n.name.to_string() == *successor;
                    if m.mesh_seat {
                        n.is_primary = is_succ && n.name.kind == NodeKind::NodeAdmin;
                    }
                }
                if m.fabric_seat {
                    n.is_fabric_primary = n.name.to_string() == *successor;
                }
            }
            if m.fabric_seat {
                self.inner.fence_to(Some(successor));
            }
            return Ok(());
        }
        Err(rafka_node_admin_core::build_drive::SeatMoveEnd::Refused(format!("rejected-no-successor: RejectedNoSuccessor {{ reason: {NO_SECONDARY:?} }}")))
    }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 failure
/// contract: "Refused before the row (step 2, or the writer's own pre-check of the call's revision):
/// refused by name, nothing changed, the Build's step fails by name. Nothing was cut off, so nothing
/// is restored"; R-BI4: "Only an attempt that ended with a named terminal failure (Failed { step,
/// reason }) makes the Build failed"): the fabric-primary `mesh1.admin.1` holds both seats and its
/// Build retires it, so it claims attempt 1 for itself and runs its own node.primary.update first,
/// which is refused before the row with Q7's text. Attempt 1 ends `Failed`, its reason naming the
/// refusal, and the drive ends at that terminal: nothing is dispatched or run and the view still names
/// `mesh1.admin.1` the holder of both seats. No cut-off and no row is the refusing gate's own record
/// here (it wrote nothing); the door's refusal writing neither is
/// `fabric_handover::a_transfer_with_no_successor_is_refused_with_q7s_exact_texts_and_nothing_moves`.
/// Must NOT happen: the attempt handed back to the holder and the drive idle on a refusal (an
/// incomplete attempt's arm), the update asked again, or the retire dispatched.
#[tokio::test(start_paused = true)]
async fn a_holders_own_update_refused_before_the_row_fails_its_attempt_by_name_and_nothing_moves() {
    let r = rig_for(view_holder_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let gate = Arc::new(RefusedUpdate { inner: r.switch.clone(), calls: Default::default(), gone_then_move: None });
    let e = &r.env;
    let holder = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: gate.clone(),
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), holder.run(&drive)).await.expect("the drive ends");
    let p = r.builds.read_build(&r.build).await.unwrap();
    let verdict: Vec<_> = r.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
        rafka_node_admin_core::build_state::BuildFact::Attempt(a) if a.build_id == r.build && a.attempt == 1 => Some(a.outcome),
        _ => None,
    }).collect();
    assert_eq!(end, DriveEnd::Terminal, "the refusal ends the drive at a terminal: verdicts {verdict:?}");
    assert!(matches!(verdict.as_slice(), [rafka_node_admin_core::build_state::AttemptOutcome::Failed { reason }] if reason.contains(NO_SECONDARY)), "attempt 1 failed naming the refusal: {verdict:?}");
    assert_eq!((p.attempt, p.attempt_executors.get(&1).map(String::as_str)), (1, Some("mesh1.admin.1")), "the holder claimed attempt 1 for itself and no other attempt was claimed");
    match terminal_of(&drive).await {
        BuildReply::Failed { attempt, reason, .. } => assert!(attempt == 1 && reason.contains(NO_SECONDARY), "{reason}"),
        other => panic!("the drive's terminal is the failed step: {other:?}"),
    }
    assert_eq!(gate.calls.load(std::sync::atomic::Ordering::SeqCst), 1, "the update was asked once");
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing ran or was dispatched: ran {:?}, dispatched {:?}", r.ran.0.lock().unwrap(), r.dispatch.dispatched.lock().unwrap());
    let holder_node = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").map(|n| (n.is_primary, n.is_fabric_primary));
    assert_eq!(holder_node, Some((true, true)), "the view still names the holder of both seats");
}

/// CONTRACT (build.md: "The next attempt is claimed after the running attempt's terminal receipt
/// (Complete, Failed or HandedOff)"; R-BI7 caveat 3, mesh_claims.rs: a mesh's in-flight row "is
/// released at that attempt's verdict", by "the admin that durably recorded the verdict"): the
/// fabric-primary `mesh1.admin.1` holds both seats and its Build retires it, so it moves both seats in
/// the attempt it claims for itself, on mesh1's rows of the fabric's one store root, and that attempt
/// ends `HandedOff` to the successor `mesh1.admin.2`. The successor's drive begins only after the
/// attempt ended, so it reads the verdict and reattaches to nothing; it claims the next attempt on the
/// same mesh1 rows and runs the retire.
/// Must NOT happen: the successor's claim refused build-in-progress by the holder's ended attempt, its
/// drive stalled on a mesh no attempt in flight holds.
#[tokio::test(start_paused = true)]
async fn a_successor_that_reads_the_holders_attempt_ended_claims_the_next_attempt_on_the_mesh_the_holder_released() {
    let r = rig_for(view_holder_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let (row, events) = (Arc::new(tokio::sync::Notify::new()), Arc::new(Mutex::new(Vec::new())));
    // The holder says primary updated as soon as its row is written: nothing holds it at steps 6-7.
    let holder_gate = Arc::new(HolderUpdate { inner: r.switch.clone(), topology: r.env.topology.clone(), row, let_go: Arc::new(tokio::sync::Semaphore::new(1)), events: events.clone() });
    let e = &r.env;
    let holder = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: holder_gate,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: e.turns.clone(),
    });
    let p_drive = Drive::detached(r.build.clone());
    assert!(matches!(tokio::time::timeout(Duration::from_secs(60), holder.run(&p_drive)).await.expect("the holder's drive ended"), DriveEnd::SeatLost(_)), "the holder's drive ends at its row");
    let p = r.builds.read_build(&r.build).await.unwrap();
    let handed: Vec<_> = r.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
        rafka_node_admin_core::build_state::BuildFact::Attempt(a) if a.build_id == r.build && a.attempt == 1 => Some(a.outcome),
        _ => None,
    }).collect();
    assert_eq!((p.attempt, handed), (1, vec![rafka_node_admin_core::build_state::AttemptOutcome::HandedOff { to: "mesh1.admin.2".into() }]), "the holder's attempt ended handed off to the successor before the successor's drive begins");
    let n_name: PathName = "mesh1.admin.2".parse().unwrap();
    let n_id = r.env.topology.read().await.node(&n_name).unwrap().node_id.clone();
    let n_switch = Arc::new(Switch { authorizes: AtomicBool::new(true), fenced: AtomicBool::new(false), successor: Mutex::new(None), fence_tx: tokio::sync::watch::channel(false).0, record_holder: Mutex::new(None), peer: Mutex::new(Default::default()) });
    // The successor: its own door, gate and park gate over the fixture's one Build log, view and store
    // root (its mesh rows, `mesh_claims`, are the holder's).
    let successor = Arc::new(DriveEnv {
        me: n_name.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: Arc::new(ClaimDoor { node_id: n_id.clone(), me: n_name.clone(), topology: e.topology.clone(), builds: r.builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()), claims: e.door.claims.clone() }),
        dispatcher: e.dispatcher.clone(),
        gate: n_switch,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: rafka_node_admin_core::park_gate::ParkGate::new(),
    });
    let n_drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), successor.run(&n_drive)).await.expect("the successor's drive ends");
    assert_eq!(end, DriveEnd::Terminal, "the successor claimed the next attempt on mesh1 and ran the retire");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt_executors.get(&1).map(String::as_str), p.attempt_executors.get(&2).map(String::as_str)), (Some("mesh1.admin.1"), Some("mesh1.admin.2")), "the successor claimed the attempt after the holder's: {:?}", p.attempt_executors);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 2)], "the successor reattached to nothing and dispatched the next attempt to itself");
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["retire-node:mesh1.admin.1".to_string()], "the retire ran once, at the successor");
    assert_eq!(*events.lock().unwrap(), vec!["row attempt 1".to_string(), "primary updated".into()]);
}

/// The successor's gate in a holder-lost hand-over: each acknowledgement the drive asks for the rows
/// the fixture committed, with whether the holder's departure was proven by then and how many
/// dispatches had been made.
struct AfterTheProof {
    inner: Arc<Switch>,
    rows: Vec<PrecedingUpdate>,
    proofs: Arc<crate::loopback::Proofs>,
    dispatch: Arc<crate::loopback::Loopback>,
    asked: Mutex<Vec<(PrecedingUpdate, bool, usize)>>,
}

#[async_trait::async_trait]
impl Gate for AfterTheProof {
    fn authorizes(&self) -> bool {
        self.inner.authorizes()
    }
    async fn fence(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.fence().await
    }
    async fn fence_set(&self) {
        self.inner.fence_set().await
    }
    async fn seats_acknowledged(&self, updates: &[PrecedingUpdate], _: &BuildId, _: u32) -> Result<(), String> {
        let proven = self.proofs.recorded.lock().unwrap().contains("mesh1.admin.1");
        let dispatched = self.dispatch.dispatched.lock().unwrap().len();
        for u in updates.iter().filter(|u| self.rows.contains(u)) {
            self.asked.lock().unwrap().push((u.clone(), proven, dispatched));
        }
        Ok(())
    }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 failure
/// contract: "The holder lost after the row, before 'updated': the successor and every reader take the
/// seat from the row; the next writer to read the row publishes 'updated'"; "A call that reached a
/// process other than the named birth, a refusal or no answer is never an exit proof"; build.md: the
/// next attempt is claimed "when the executor is lost with the attempt unfinished, after exact
/// departure proof"): the fabric-primary `mesh1.admin.1` holds both seats and moves them in its own
/// attempt; it dies after its row, before it says "primary updated". Its successor `mesh1.admin.2`,
/// holding the seats from the row, finds that attempt held by the holder and its call to it reaches
/// nothing: only the holder's proven departure lets it claim the next attempt, and only then does it
/// acknowledge the row's update (the acknowledgement in which the successor, the next writer to read the
/// row, says "primary updated" once the holder's exit is proven: fabric_handover.rs
/// `a_successor_that_answered_the_holders_call_says_primary_updated_only_once_the_holder_is_proven_exited`),
/// before it dispatches the retire.
/// Must NOT happen: the successor claiming the holder's attempt over it on silence, the acknowledgement
/// before the departure proof, or the retire dispatched before the acknowledgement.
#[tokio::test(start_paused = true)]
async fn a_holder_lost_after_its_row_is_proven_departed_before_its_successor_acknowledges_the_row_and_dispatches() {
    let r = rig_for(view_holder_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let (row, let_go, events) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Semaphore::new(0)), Arc::new(Mutex::new(Vec::new())));
    let holder_gate = Arc::new(HolderUpdate { inner: r.switch.clone(), topology: r.env.topology.clone(), row: row.clone(), let_go, events: events.clone() });
    let e = &r.env;
    let holder = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: holder_gate,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: e.turns.clone(),
    });
    let p_drive = Drive::detached(r.build.clone());
    let p_driving = tokio::spawn({
        let (env, d) = (holder.clone(), p_drive.clone());
        async move { env.run(&d).await }
    });
    tokio::time::timeout(Duration::from_secs(5), row.notified()).await.expect("the holder's update never wrote its row");
    // The holder dies after its row, before step 7: its process (the drive and the run it held) is gone,
    // nothing answers at its address, and the provider proves its exact runtime exited.
    p_driving.abort();
    let _ = p_driving.await;
    r.dispatch.lose("mesh1.admin.1");
    r.proofs.exited.lock().unwrap().insert("mesh1.admin.1".into());
    let n_name: PathName = "mesh1.admin.2".parse().unwrap();
    let n_id = r.env.topology.read().await.node(&n_name).unwrap().node_id.clone();
    let n_switch = Arc::new(Switch { authorizes: AtomicBool::new(true), fenced: AtomicBool::new(false), successor: Mutex::new(None), fence_tx: tokio::sync::watch::channel(false).0, record_holder: Mutex::new(None), peer: Mutex::new(Default::default()) });
    let op = "retire-node:mesh1.admin.1";
    let update = PrecedingUpdate { mesh: "mesh1".into(), operation: rafka_node_admin_core::fabric_handover::update_operation(rafka_mesh_entity::Seat::MeshPrimary, r.build.as_str(), 1, op), holder: Some("mesh1.admin.1".parse().unwrap()) };
    let n_gate = Arc::new(AfterTheProof { inner: n_switch, rows: vec![update.clone()], proofs: r.proofs.clone(), dispatch: r.dispatch.clone(), asked: Mutex::new(Vec::new()) });
    let successor = Arc::new(DriveEnv {
        me: n_name.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: Arc::new(ClaimDoor { node_id: n_id.clone(), me: n_name.clone(), topology: e.topology.clone(), builds: r.builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()), claims: e.door.claims.clone() }),
        dispatcher: e.dispatcher.clone(),
        gate: n_gate.clone(),
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: rafka_node_admin_core::park_gate::ParkGate::new(),
    });
    let n_drive = Drive::detached(r.build.clone());
    assert_eq!(tokio::time::timeout(Duration::from_secs(60), successor.run(&n_drive)).await.expect("the successor's drive ends"), DriveEnd::Terminal);
    assert_eq!(r.proofs.calls.load(Ordering::SeqCst), 1, "the holder's departure was proven once");
    assert_eq!(*n_gate.asked.lock().unwrap(), vec![(update, true, 0)], "the row's update was acknowledged once, after the departure proof and before any dispatch");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt_executors.get(&1).map(String::as_str), p.attempt_executors.get(&2).map(String::as_str)), (Some("mesh1.admin.1"), Some("mesh1.admin.2")), "the successor claimed the attempt after the holder's: {:?}", p.attempt_executors);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 2)], "nothing reached the lost holder; the retire was dispatched once, to the successor");
    assert_eq!(*r.ran.0.lock().unwrap(), vec![op.to_string()], "the retire ran once, at the successor");
    assert_eq!(*events.lock().unwrap(), vec!["row attempt 1".to_string()], "the holder never said primary updated");
}

const GONE_REASON: &str = "the new holder's process is gone, provider-confirmed, after the committed row: step primary-update; new holder mesh1 000000000003; moved mesh-primary (mesh1) revision 4; ordinary rule seated mesh-primary (mesh1): mesh1.admin.1; provider proof: the provider proved its exact birth exited with code 0";

/// A node.primary.update whose new holder's process is gone, provider-confirmed, after the row
/// (ruling R-PU2, Q10), then, in the next run, a transfer to `successor`: the first call ends
/// `SeatMoveEnd::Failed` with the view unchanged (the ordinary rule seats the holder again), the next
/// moves the requested seats to `successor`. Records `(attempt, run, claimed_before)` for each call.
fn gone_after_row_then_moved(
    inner: Arc<Switch>,
    topology: Arc<RwLock<Topology>>,
    builds: Arc<rafka_node_admin_core::build_state::MemoryBuildStateAdapter>,
    successor: &'static str,
) -> (Arc<RefusedUpdate>, Arc<Mutex<Vec<(u32, u32, Option<String>)>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(RefusedUpdate { inner, calls: Default::default(), gone_then_move: Some((topology, builds, successor, calls.clone())) });
    (gate, calls)
}

/// CONTRACT (ruling R-PU2, Q8/Q9/Q10 + Luke 2026-10-13: when the new holder's process is gone,
/// provider-confirmed, after the committed row, the transfer step fails with `Failed { step, reason }`,
/// the Build is Failed under R-BI4; recovery order (4) "The executor durably records Failed { step,
/// reason } and ends the workflow; it never proceeds to P's stop. The terminal receipt goes through the
/// Build journal"; (5) "The caller's resubmit opens the next attempt of the same Build ... under its
/// own key, (build_id, run), never the Build id alone"): the fabric-primary `mesh1.admin.1` holds both
/// seats and its Build removes it. Its own update in attempt 1 (run 1) ends with the new holder's
/// process gone after the row and the ordinary rule seats it again: attempt 1 ends `Failed` at step
/// primary-update, the drive ends at that terminal, and nothing is dispatched or run. The caller's
/// resubmit (a fresh drive of the failed Build) claims attempt 2, which opens run 2: its update is a
/// new transfer (run 2), which moves the seats to `mesh1.admin.3`, and the drive goes on from the
/// moved seat. Must NOT happen: the attempt handed back to the holder and the drive idle on it, the
/// remove dispatched in attempt 1, or attempt 2's update keyed by run 1 (it would replay run 1's row).
#[tokio::test(start_paused = true)]
async fn a_holders_own_update_whose_new_holders_process_is_gone_after_the_row_fails_and_the_resubmit_runs_a_new_transfer() {
    let mut t = view();
    t.nodes = vec![node("mesh1.admin.1", true, true), node("mesh1.admin.2", false, false), node("mesh1.admin.3", false, false), node("mesh1.rpc.1", false, false)];
    let r = rig_for(t, TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let (gate, calls) = gone_after_row_then_moved(r.switch.clone(), r.env.topology.clone(), r.builds.clone(), "mesh1.admin.3");
    let e = &r.env;
    let holder = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), holder.run(&drive)).await.expect("the drive ends");
    let verdicts = |attempt: u32| {
        let builds = r.builds.clone();
        let build = r.build.clone();
        async move {
            builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
                rafka_node_admin_core::build_state::BuildFact::Attempt(a) if a.build_id == build && a.attempt == attempt => Some(a.outcome),
                _ => None,
            }).collect::<Vec<_>>()
        }
    };
    let first = verdicts(1).await;
    assert_eq!(end, DriveEnd::Terminal, "the failed step ends the drive at a terminal: verdicts {first:?}");
    assert!(matches!(first.as_slice(), [rafka_node_admin_core::build_state::AttemptOutcome::Failed { reason }] if reason.starts_with("the new holder's process is gone, provider-confirmed, after the committed row")), "attempt 1 failed with the plain-English reason: {first:?}");
    match terminal_of(&drive).await {
        BuildReply::Failed { attempt, step, reason, .. } => assert!(attempt == 1 && step == "primary-update" && reason.starts_with("the new holder's process is gone, provider-confirmed, after the committed row"), "{step}: {reason}"),
        other => panic!("the drive's terminal is the failed step: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "the remove is never reached: ran {:?}, dispatched {:?}", r.ran.0.lock().unwrap(), r.dispatch.dispatched.lock().unwrap());
    // The caller's resubmit: a drive of the failed Build claims its next attempt.
    let resubmit = Drive::detached(r.build.clone());
    let _ = tokio::time::timeout(Duration::from_secs(60), holder.run(&resubmit)).await.expect("the resubmitted drive ends");
    assert_eq!(*calls.lock().unwrap(), vec![(1, 1, Some("mesh1.admin.1".into())), (2, 2, Some("mesh1.admin.1".into()))], "attempt 2 opens run 2: its update is a new transfer key");
    let second = verdicts(2).await;
    assert!(matches!(second.as_slice(), [rafka_node_admin_core::build_state::AttemptOutcome::HandedOff { to }] if to == "mesh1.admin.3"), "attempt 2 moved the seats and handed the Build to mesh1.admin.3: {second:?}");
}

/// CONTRACT (Q10 commanded arm: "When a commanded node.primary.update ends, the holder answers the
/// fabric-primary with the Build carrier's own terminal, `Failed { step, reason }` (R-BI4; the
/// envelope's only durable step outcomes are Complete and Failed). 0x21 gains that variant;
/// `Incomplete` is not used for a step that ended. The FP fails the step on the variant, never on the
/// reason's text ... The FP claims the attempt at the claim door BEFORE it commands"): the
/// fabric-primary `mesh1.admin.1` drives a Build that removes `mesh2.admin.1`, mesh2's primary. There
/// is no separate arm: the fabric-primary claims attempt 1 at the claim door before it commands
/// `mesh2.admin.1`'s seat move. In run 1, the commanded move answers `Failed { step, reason }`: attempt
/// 1 ends `Failed`, the Build is `Failed`, and nothing is dispatched to mesh2. On the caller's
/// resubmit, the fabric-primary claims attempt 2 (opening run 2) before commanding `mesh2.admin.1`'s
/// seat move, which moves mesh2's seat to `mesh2.admin.3`, hands off attempt 2, and claims attempt 3
/// for `mesh2.admin.3` to run the remove.
#[tokio::test(start_paused = true)]
async fn a_commanded_update_claims_the_attempt_first_and_when_it_ends_failed_fails_the_build_and_the_resubmit_opens_the_next_run() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    t.nodes.push(node("mesh2.admin.1", true, false));
    t.nodes.push(node("mesh2.admin.2", false, false));
    t.nodes.push(node("mesh2.admin.3", false, false));
    let r = rig_for(t, TopologyChange::RemoveNode { node: "mesh2.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let (gate, calls) = gone_after_row_then_moved(r.switch.clone(), r.env.topology.clone(), r.builds.clone(), "mesh2.admin.3");
    let e = &r.env;
    let commander = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), commander.run(&drive)).await.expect("the drive ends");
    assert_eq!(end, DriveEnd::Terminal, "the commanded Failed reply ends the drive at a terminal");
    assert_eq!(*calls.lock().unwrap(), vec![(1, 1, Some("mesh1.admin.1".into()))], "the fabric-primary claimed attempt 1 at the claim door BEFORE it commanded");
    match terminal_of(&drive).await {
        BuildReply::Failed { attempt, step, reason, .. } => assert!(attempt == 1 && step == "primary-update" && reason == GONE_REASON, "{step}: {reason}"),
        other => panic!("the drive's terminal is the failed step: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing was dispatched when the commanded update ended Failed");
    // The caller's resubmit opens run 2: attempt 2 is claimed before commanding, moves mesh2's seat to
    // mesh2.admin.3, and attempt 3 dispatches the remove to mesh2.admin.3.
    let resubmit = Drive::detached(r.build.clone());
    let end2 = tokio::time::timeout(Duration::from_secs(60), commander.run(&resubmit)).await.expect("the resubmitted drive ends");
    assert_eq!(end2, DriveEnd::Terminal, "the resubmitted Build completes");
    assert_eq!(*calls.lock().unwrap(), vec![(1, 1, Some("mesh1.admin.1".into())), (2, 2, Some("mesh1.admin.1".into()))], "attempt 2 opened run 2 and was claimed before commanding");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh2.admin.3".to_string(), 3)], "attempt 3 dispatched the remove to the new mesh2 primary");
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["retire-node:mesh2.admin.1".to_string()]);
}
