//! The fabric-primary's drive of a Build, one cell per gate it passes and per way it can be stopped
//! (op `0x20`; node-rpc-envelope.md "Build, op `0x20`"): the Build state, the view and the
//! executors are fixtures, the drive is the product's.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.admin.1`, its mesh primary
//! `mesh1.admin.2` (so a node of `mesh1` is executed by an admin that is not the fabric-primary),
//! and `mesh1.rpc.1`. The Build grows `mesh1` by one rpc node.

use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build::{BuildId, BuildOperation, MeshDesired};
use rafka_node_admin_core::build_claim::{AttemptContexts, ClaimDoor};
use rafka_node_admin_core::build_drive::{Drive, DriveEnd, DriveEnv, Gate, Read};
use rafka_node_admin_core::build_state::{BuildStateAdapter, BuildStateError, MemoryBuildStateAdapter};
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
    async fn fence(&self) -> Result<(), BuildStateError> {
        if self.fenced.load(Ordering::SeqCst) {
            Err(BuildStateError::Fenced { node: "mesh1.admin.1".into(), by: "leaving".into(), to: self.successor.lock().unwrap().clone() })
        } else {
            Ok(())
        }
    }
    async fn fence_set(&self) {
        let _ = self.fence_tx.subscribe().wait_for(|f| *f).await;
    }
}

struct Creates(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl OperationRunner for Creates {
    async fn run(&self, _: &BuildId, _: u32, op: &BuildOperation) -> Result<(), String> {
        self.0.lock().unwrap().push(op.key());
        Ok(())
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
    let ran = Arc::new(Creates(Mutex::new(Vec::new())));
    let dispatch = crate::loopback::Loopback::new(builds.clone(), builds.clone(), topology.clone(), runner(ran.clone(), topology.clone()));
    let proofs = Arc::new(crate::loopback::Proofs::default());
    let switch = Arc::new(Switch { authorizes: AtomicBool::new(true), fenced: AtomicBool::new(false), successor: Mutex::new(None), fence_tx: tokio::sync::watch::channel(false).0 });
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: topology.clone(),
        accepted,
        builds: builds.clone(),
        door: Arc::new(ClaimDoor { node_id: fp_id.clone(), me, topology, builds: builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()) }),
        dispatcher: dispatch.clone(),
        gate: switch.clone(),
        departure: proofs.clone(),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        rows: Arc::new(rafka_node_admin_core::node_rows::NodeRows::new("mesh1.admin.1".parse().unwrap(), fp_id, Arc::new(rafka_node_admin_core::storage::MemoryNodesStorage::default()))),
    });
    Rig { builds, env, switch, dispatch, proofs, ran, build: opened.build_id }
}

async fn next_within(reader: &mut rafka_node_admin_core::build_drive::DriveReader) -> Option<Read> {
    tokio::time::timeout(Duration::from_millis(50), reader.next()).await.ok().flatten()
}

/// CONTRACT: a fabric-primary that yielded its seat dispatches nothing. The Build log's sticky
/// fence is checked before the claim: the drive ends `SeatLost` naming the fence, no attempt is
/// claimed, no executor is called, and the callers' streams end with the seat loss (the caller
/// re-submits by the Build id at the new seat).
#[tokio::test]
async fn a_fabric_primary_that_yielded_its_seat_claims_and_dispatches_nothing() {
    let r = rig().await;
    r.switch.fenced.store(true, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::SeatLost(why) => assert!(why.contains("yielded the fabric-primary seat"), "{why}"),
        other => panic!("a fenced seat ends the drive SeatLost: {other:?}"),
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
    assert!(r.ran.0.lock().unwrap().is_empty());
    assert!(matches!(next_within(&mut reader).await, Some(Read::SeatLost(loss)) if loss.why.contains("leaving") && loss.to.is_none()));
}

/// CONTRACT: an admin knows the fabric-primary seat is its own by the node id it was born with,
/// never by path.name. When its view shows another birth at its own path.name holding the seat (a
/// successor or a retired birth of that path), the drive ends `SeatLost` naming that holder, claims
/// no attempt and calls no executor, and the callers' streams end naming the holder.
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
    assert!(matches!(next_within(&mut reader).await, Some(Read::SeatLost(loss)) if loss.to.as_deref() == Some("mesh1.admin.1")));
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
        DriveEnd::SeatLost(why) => assert!(why.contains("yielded the fabric-primary seat"), "the fence refused it, not the view: {why}"),
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
    assert!(matches!(&end, DriveEnd::SeatLost(why) if why.contains("yielded the fabric-primary seat")), "the fence ends the drive SeatLost: {end:?}");
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
