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
use rafka_node_admin_core::build_drive::{Drive, DriveEnd, DriveEnv, Gate, Read};
use rafka_node_admin_core::build_state::{BuildStateAdapter, BuildStateError, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::executor::OperationRunner;
use rafka_node_admin_core::http::ControlPlane;
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
    async fn run(&self, build: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        self.0.lock().unwrap().push(op.key());
        let receipt = BuildStepReceipt { build_id: build.clone(), attempt, operation: op.key(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None };
        self.1.append_step_receipt(&receipt).await.map_err(|e| e.to_string())
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
        accepted,
        builds: builds.clone(),
        door: Arc::new(ClaimDoor { node_id: fp_id.clone(), me, topology: topology.clone(), builds: builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()) }),
        dispatcher: dispatch.clone(),
        gate: switch.clone(),
        departure: proofs.clone(),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
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

/// CONTRACT (node-rpc-envelope.md TakeSeats: "The row is the fence"): the committed seat record,
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

/// CONTRACT (node-rpc-envelope.md TakeSeats: "The row is the fence ... Build dispatch from the old
/// seat is refused with Fenced"): the seat record decides before the view does. A fabric-primary
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

/// CONTRACT (#3016; node-rpc-envelope.md TakeSeats: the executor is "the same-mesh successor N";
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
    async fn run(&self, build: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        self.ran.run(build, attempt, op).await?;
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

/// CONTRACT (mesh-investigation.md step 5, "Fully crashed"; R-D1): mesh2 has lost every node-admin and
/// the investigation's 60 s decision on it came out `unreachable`, a hold. The fabric-primary opened
/// the attempt on the provider's exact exit proof of every node-admin, so the drive runs the create of
/// the mesh's first node-admin, and only it, on the fabric-primary; the mesh's second admin and its
/// ordinary node still wait for the mesh to be heard. With no decision at all the fence stands for the
/// first admin too. Must NOT happen: the fence refusing the one create the exit proof opened (the
/// mesh waiting for ever), or the hold releasing more than that create.
#[tokio::test]
async fn the_create_of_a_fully_crashed_meshs_first_node_admin_runs_on_the_fabric_primary_and_nothing_else_of_the_held_mesh_does() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    for n in ["mesh2.admin.1", "mesh2.rpc.1"] {
        let mut lost = node(n, false, false);
        lost.status = NodeStatus::Dead;
        t.nodes.push(lost);
    }
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(Reborn { ran, topology })).await;
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
    let fenced = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    assert_eq!(fenced.len(), 2, "the fence is named for the undecided admin.1 and then for admin.2: {fenced:?}");
    assert_eq!((fenced[1].1.get("target").map(String::as_str), fenced[1].1.get("rebirth_decided").map(String::as_str)), (Some("mesh2.admin.2"), Some("false")), "{:?}", fenced[1].1);
    eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={} {:?}", fenced[1].0, fenced[1].1);
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
    async fn run(&self, build: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        self.ran.run(build, attempt, op).await?;
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

/// CONTRACT (node-rpc-envelope.md TakeSeats, the same-mesh successor; R-A2: cross-mesh only when
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
    async fn run(&self, _: &BuildId, _: u32, op: &BuildOperation) -> Result<(), String> {
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
        accepted: e.accepted.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: e.gate.clone(),
        departure: e.departure.clone(),
        verdicts: Arc::new(rafka_node_admin_core::build_op::LocalVerdicts { local: r.builds.clone() }),
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
    async fn run(&self, build: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        self.ran.run(build, attempt, op).await?;
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
    let receipt = BuildStepReceipt { build_id: r.build.clone(), attempt: 1, operation: "create-node:mesh1.rpc.2".into(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None };
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

/// Moves the fabric seat to `mesh2.admin.1` when the operation is prepared, as the mesh-retire
/// handover does for the mesh that holds the seat.
struct SeatMover {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for SeatMover {
    async fn run(&self, build: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        self.ran.run(build, attempt, op).await
    }
    async fn prepare(&self, _: &BuildId, _: u32, op: &BuildOperation) -> Result<(), String> {
        if matches!(op, BuildOperation::ShutdownMesh { .. }) {
            for n in self.topology.write().await.nodes.iter_mut() {
                n.is_fabric_primary = n.name.to_string() == "mesh2.admin.1";
            }
        }
        Ok(())
    }
}

/// CONTRACT: a mesh retire whose fabric-primary sits inside the leaving mesh is run by the admin
/// the handover made the fabric-primary. The claimed executor hands the seat over when the
/// operation is prepared, the executor of the operation is then another admin, and the attempt is
/// passed to it before the operation runs: the leaving mesh's admin never runs its own retire, and
/// its drive, now showing another seat holder, ends by naming it.
#[tokio::test]
async fn a_mesh_retire_whose_seat_moved_at_prepare_is_passed_to_the_new_fabric_primary_unrun() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    t.nodes.push(node("mesh2.admin.1", true, false));
    let r = rig_for(t, TopologyChange::RemoveMesh { mesh: "mesh1".into() }, |ran, topology| Arc::new(SeatMover { ran, topology })).await;
    let drive = Drive::detached(r.build.clone());
    match r.env.run(&drive).await {
        DriveEnd::SeatLost(why) => assert!(why.contains("mesh2.admin.1"), "the drive names the seat's new holder: {why}"),
        other => panic!("the old seat holder's drive ends SeatLost: {other:?}"),
    }
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1)]);
    assert!(!r.ran.0.lock().unwrap().iter().any(|k| k.starts_with("shutdown-mesh")), "the retire did not run at the leaving mesh's admin: {:?}", r.ran.0.lock().unwrap());
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(p.state, rafka_node_admin_core::build_state::BuildState::Pending, "the handed-off attempt leaves the Build waiting for the new seat's claim");
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
    async fn run(&self, build: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        self.ran.run(build, attempt, op).await?;
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
    async fn run(&self, _: &BuildId, _: u32, _: &BuildOperation) -> Result<(), String> {
        self.began.notify_one();
        std::future::pending().await
    }
}

/// CONTRACT (node-rpc-envelope.md "TakeSeats: Build-driven seat transfer"): the committed seat row
/// is the fence, and when the holder was the fabric-primary it ends the caller's outer stream with
/// the new seat after the row. The fence is set while the drive waits on the executor's run: the
/// drive ends `SeatLost` at the fence, without waiting for that run's stream to end, the callers'
/// streams end naming the successor the transfer committed, and no later attempt is claimed (the
/// successor resumes the same attempt).
#[tokio::test]
async fn a_fence_set_while_the_executor_runs_ends_the_drive_at_the_fence_naming_the_successor() {
    let held = Arc::new(HeldOpen { began: tokio::sync::Notify::new() });
    let runner = held.clone();
    let r = rig_for(view(), TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) }, move |_, _| -> Arc<dyn OperationRunner> { runner }).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let (env, d) = (r.env.clone(), drive.clone());
    let running = tokio::spawn(async move { env.run(&d).await });
    held.began.notified().await;
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "attempt 1 is running at the executor");
    r.switch.fence_to(Some("mesh1.admin.2"));
    let end = tokio::time::timeout(Duration::from_secs(5), running).await.expect("the drive ends at the fence while the executor's run is still open").unwrap();
    assert!(matches!(&end, DriveEnd::SeatLost(why) if why.contains("no longer holds the fabric-primary seat: the committed seat row moved it")), "the fence ends the drive SeatLost: {end:?}");
    match next_within(&mut reader).await {
        Some(Read::SeatLost(loss)) => assert_eq!(loss.to.as_deref(), Some("mesh1.admin.2"), "the stream names the committed successor: {loss:?}"),
        other => panic!("the reader ends with the seat loss, got {other:?}"),
    }
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.2")), "no later attempt was claimed: the successor resumes attempt 1");
    assert_eq!(r.dispatch.dispatched.lock().unwrap().len(), 1, "nothing more was dispatched");
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
    r.builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: r.build.clone(), attempt: done, outcome: AttemptOutcome::Converged }).await.unwrap();
    let stale = drives.ensure(&r.env, &r.build, done);
    assert!(Arc::ptr_eq(&stale, &first), "a read that showed attempt {done} running is handed the drive that completed it");
    r.builds
        .open_attempt(&AttemptOpened { build_id: r.build.clone(), attempt: done + 1, reason: AttemptReason::Restart, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 0 })
        .await
        .unwrap();
    let reopened = drives.ensure(&r.env, &r.build, done + 1);
    assert!(!Arc::ptr_eq(&reopened, &first), "the reopened Build is driven by a new drive");
    assert!(matches!(terminal_of(&reopened).await, BuildReply::Complete { attempt, .. } if attempt == done + 1), "the new drive claims and ends attempt {}", done + 1);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), done), ("mesh1.admin.2".to_string(), done + 1)], "one build.attempt.run per attempt");
}
