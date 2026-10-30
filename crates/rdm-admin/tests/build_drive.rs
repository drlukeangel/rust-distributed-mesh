//! The fabric-primary's drive of a Build, one cell per gate it passes and per way it can be stopped
//! (op `0x20`; node-rpc-envelope.md "Build, op `0x20`"): the Build state, the view and the
//! executors are fixtures, the drive is the product's.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.admin.1`, its mesh primary
//! `mesh1.admin.2` (so a node of `mesh1` is executed by an admin that is not the fabric-primary),
//! and `mesh1.rpc.1`. The Build grows `mesh1` by one rpc node.

use rdm_admin::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rdm_admin::build::{BuildId, BuildOperation, FabricDesired, MeshDesired};
use rdm_admin::build_claim::{AttemptContexts, ClaimDoor};
use rdm_admin::build_drive::{Drive, DriveEnd, DriveEnv, Gate, PrecedingUpdate, Read};
use rdm_admin::build_state::{BuildStateAdapter, BuildStateError, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::executor::OperationRunner;
use rdm_admin::control_plane::ControlPlane;
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use rdm_core::build::BuildReply;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn view() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node("mesh1.admin.1", false, true), node("mesh1.admin.2", true, false), node("mesh1.rpc.1", false, false)],
    }
}

/// A gate the cell opens and closes.
struct Switch {
    targeted: AtomicBool,
    /// The admin the target names, when the cell sets one.
    successor: Mutex<Option<String>>,
    /// Set with `targeted` by a cell that targets while a dispatch is in flight.
    target_tx: tokio::sync::watch::Sender<bool>,
    /// The primary the open primary record names (this birth's path.name), when the cell sets one.
    recorded_primary: Mutex<Option<String>>,
    /// What the peer-mesh investigation holds per mesh, when the cell sets it.
    peer: Mutex<std::collections::HashMap<String, rdm_admin::investigate::PeerMesh>>,
    /// The meshes whose Mesh record is Dead, when the cell sets them.
    dead: Mutex<std::collections::HashSet<MeshId>>,
    /// The view the fixture's nodes live in: its rows ([`rows_of`]).
    view: Option<Arc<RwLock<Topology>>>,
}

/// What the node rows of a fixture whose nodes run what their entries name record: each live node of
/// `view` at a path whose entry in `topology` names an executable, on that executable (the create's
/// claim records it, build-deployment.md §2.3). A fixture lands its nodes in the view alone.
async fn rows_of(topology: &rdm_admin::accepted::FabricTopology, view: &RwLock<Topology>) -> rdm_admin::accepted::Recorded {
    let v = view.read().await;
    v.nodes
        .iter()
        .filter(|n| n.status.is_live())
        .filter_map(|n| {
            let entry = rdm_admin::accepted::target_executable(topology, &n.name)?;
            Some((n.name.clone(), rdm_admin::accepted::RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: n.node_id.clone(), sha256: Some(entry.sha256.clone()) }))
        })
        .collect()
}

impl Switch {
    /// Target the Build log now, naming `successor` as the admin the committed transfer in its role.
    fn target_to(&self, successor: Option<&str>) {
        *self.successor.lock().unwrap() = successor.map(str::to_string);
        self.targeted.store(true, Ordering::SeqCst);
        self.target_tx.send_replace(true);
    }
}

#[async_trait::async_trait]
impl Gate for Switch {
    async fn target(&self) -> Result<Option<String>, BuildStateError> {
        if self.targeted.load(Ordering::SeqCst) {
            Err(BuildStateError::RoleYielded { node: "mesh1.admin.1".into(), by: "leaving".into(), to: self.successor.lock().unwrap().clone() })
        } else {
            Ok(self.recorded_primary.lock().unwrap().clone())
        }
    }
    async fn target_set(&self) {
        let _ = self.target_tx.subscribe().wait_for(|f| *f).await;
    }
    fn peer_mesh(&self, mesh: &str) -> Option<rdm_admin::investigate::PeerMesh> {
        self.peer.lock().unwrap().get(mesh).copied()
    }
    async fn dead_meshes(&self, _observed: &Topology) -> Result<std::collections::HashSet<MeshId>, String> {
        Ok(self.dead.lock().unwrap().clone())
    }
    async fn recorded_executables(&self, topology: &rdm_admin::accepted::FabricTopology) -> Result<rdm_admin::accepted::Recorded, String> {
        Ok(match &self.view {
            Some(view) => rows_of(topology, view).await,
            None => Default::default(),
        })
    }
}

/// Records what ran, and commits each operation's `Complete` receipt on the executor's log (the
/// fixture's admins share one log) as the create pipeline does: a step's receipt is its durable
/// commit (`deployment/pipeline.rs`, `append_step_receipt` in the step runner).
struct Creates(Mutex<Vec<String>>, Arc<MemoryBuildStateAdapter>);

#[async_trait::async_trait]
impl OperationRunner for Creates {
    async fn run(&self, build: &BuildId, _lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
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
    /// The application the drive sends its Build notices to (Q256).
    told: crate::Told,
}

async fn rig() -> Rig {
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) };
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
    let change = crate::named_request(&cp, change).await;
    let opened = cp.submit("test", change).await.unwrap();
    let ran = Arc::new(Creates(Mutex::new(Vec::new()), builds.clone()));
    let dispatch = crate::loopback::Loopback::new(builds.clone(), builds.clone(), topology.clone(), runner(ran.clone(), topology.clone()));
    let proofs = Arc::new(crate::loopback::Proofs::default());
    let switch = Arc::new(Switch { targeted: AtomicBool::new(false), successor: Mutex::new(None), target_tx: tokio::sync::watch::channel(false).0, recorded_primary: Mutex::new(None), peer: Mutex::new(Default::default()), dead: Mutex::new(Default::default()), view: Some(topology.clone()) });
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let told = crate::Told::default();
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: topology.clone(),
        builds: builds.clone(),
        door: Arc::new(ClaimDoor {
            node_id: fp_id.clone(),
            me,
            topology: topology.clone(),
            builds: builds.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            claims: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
            claimed: Default::default(),
        }),
        dispatcher: dispatch.clone(),
        gate: switch.clone(),
        departure: proofs.clone(),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        notices: told.notices(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    Rig { builds, env, switch, dispatch, proofs, ran, build: opened.build_id, told }
}

async fn next_within(reader: &mut rdm_admin::build_drive::DriveReader) -> Option<Read> {
    tokio::time::timeout(Duration::from_millis(50), reader.next()).await.ok().flatten()
}

/// Every frame the reader holds now, up to the first wait.
async fn frames_now(reader: &mut rdm_admin::build_drive::DriveReader) -> Vec<Read> {
    let mut read = Vec::new();
    while let Some(r) = next_within(reader).await {
        read.push(r);
    }
    read
}

/// The `Blocked` frames of `read`.
fn blocked_of(read: &[Read]) -> Vec<BuildReply> {
    read.iter().filter_map(|r| match r {
        Read::Frame(f @ BuildReply::Blocked { .. }) => Some(f.clone()),
        _ => None,
    }).collect()
}

/// CONTRACT: a fabric-primary whose fabric-primary record names another live birth dispatches nothing.
/// The Build log's target is checked before the claim: the drive ends `RoleLost` naming the target, no attempt is
/// claimed, no executor is called, and the callers' streams end with the role loss (the caller
/// re-submits by the Build id at the new role).
#[tokio::test]
async fn a_fabric_primary_whose_record_names_another_birth_claims_and_dispatches_nothing() {
    let r = rig().await;
    r.switch.targeted.store(true, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("is no longer the fabric-primary: "), "{why}"),
        other => panic!("a targeted role ends the drive RoleLost: {other:?}"),
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
    assert!(r.ran.0.lock().unwrap().is_empty());
    assert!(matches!(next_within(&mut reader).await, Some(Read::RoleLost(loss)) if loss.why.contains("leaving") && loss.to.is_none()));
}

/// CONTRACT: an admin knows the fabric-primary role is its own by the node id it was born with,
/// never by path.name. When its view shows another birth at its own path.name holding the role (a
/// successor or a deleted birth of that path), the drive ends `RoleLost` naming that birth, claims
/// no attempt and calls no executor, and the callers' streams end naming the primary the committed
/// primary record names (none here), never the view's.
#[tokio::test]
async fn a_fabric_primary_whose_view_shows_another_birth_of_its_path_as_fabric_primary_dispatches_nothing() {
    let r = rig().await;
    let other = rdm_core::NodeId::mint();
    {
        let mut t = r.env.topology.write().await;
        let fp = t.nodes.iter_mut().find(|n| n.is_fabric_primary).expect("the view shows a fabric-primary");
        assert_eq!(fp.name, r.env.me, "the role is shown at this admin's own path.name");
        fp.node_id = other.clone();
        fp.incarnation_id = Some(IncarnationId::mint());
    }
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("does not hold the fabric-primary role") && why.contains(other.as_str()), "the refusal names the birth that holds the primary role: {why}"),
        other => panic!("another birth of this path holding the primary role ends the drive RoleLost: {other:?}"),
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
    assert!(r.ran.0.lock().unwrap().is_empty());
    assert!(matches!(next_within(&mut reader).await, Some(Read::RoleLost(loss)) if loss.to.is_none()), "the record names no primary");
}

/// CONTRACT (lifecycles/node-primary-update.md step 4: the successor becomes primary when it installs
/// the election's result): the fabric-primary record this admin holds, never the view, names the
/// fabric-primary. A fabric-primary whose record still names this birth
/// while its view shows another admin holding the role dispatches nothing, and the callers'
/// streams end naming the primary the record names (this birth), never the view's; a record naming
/// none names no primary.
#[tokio::test]
async fn a_fabric_primary_whose_view_shows_another_fabric_primary_names_the_records_never_the_views() {
    let r = rig().await;
    {
        let mut t = r.env.topology.write().await;
        for n in t.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh1.admin.2";
        }
    }
    *r.switch.recorded_primary.lock().unwrap() = Some("mesh1.admin.1".into());
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("does not hold the fabric-primary role") && why.contains("the fabric-primary record it holds names mesh1.admin.1"), "the refusal names what the record names: {why}"),
        other => panic!("a view showing another primary ends the drive RoleLost: {other:?}"),
    }
    let loss = match next_within(&mut reader).await {
        Some(Read::RoleLost(loss)) => loss,
        _ => panic!("the callers' streams end with the role loss"),
    };
    assert_eq!(loss.to.as_deref(), Some("mesh1.admin.1"), "the role loss names the record's primary, not the view's mesh1.admin.2: {loss:?}");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
    *r.switch.recorded_primary.lock().unwrap() = None;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    assert!(matches!(r.env.run(&drive).await, DriveEnd::RoleLost(why) if why.contains("the fabric-primary record it holds names none")));
    assert!(matches!(next_within(&mut reader).await, Some(Read::RoleLost(loss)) if loss.to.is_none()), "a record naming none names no primary");
}

/// CONTRACT (lifecycles/node-primary-update.md step 4; Build dispatch from the old fabric-primary is
/// refused `Targeted`): the fabric-primary record decides before the view does. A fabric-primary
/// whose primary record moved is refused `Targeted` while its view still shows it as the primary and
/// holds no row of the new one yet, and the callers' streams end naming the primary the record names,
/// never the view's. No executor is called.
#[tokio::test]
async fn a_moved_fabric_primary_is_refused_whatever_its_view_shows_and_names_the_new_fabric_primary() {
    let r = rig_for(view(), TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) }, |ran, _| ran).await;
    assert!(r.env.topology.read().await.node(&"mesh2.admin.1".parse().unwrap()).is_none(), "the view holds no row of the new primary");
    r.switch.target_to(Some("mesh2.admin.1"));
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("is no longer the fabric-primary: "), "the target refused it, not the view: {why}"),
        other => panic!("a targeted role ends the drive RoleLost: {other:?}"),
    }
    assert!(matches!(next_within(&mut reader).await, Some(Read::RoleLost(loss)) if loss.to.as_deref() == Some("mesh2.admin.1")));
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
}

/// CONTRACT: node.primary.update names the new fabric-primary in the target itself. The old role's view
/// is re-projected on its own clock, so right after the transfer it may still show this admin as the
/// fabric-primary: the callers' streams still end naming the successor the update installed, never
/// an empty role read from a view that has not moved yet.
#[tokio::test]
async fn the_callers_streams_name_the_successor_a_primary_update_installed_while_the_view_shows_the_old_fabric_primary() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    t.nodes.push(node("mesh2.admin.1", true, false));
    let r = rig_for(t, TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) }, |ran, _| ran).await;
    assert_eq!(r.env.topology.read().await.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.1"), "the view still shows the old role");
    *r.switch.successor.lock().unwrap() = Some("mesh2.admin.1".into());
    r.switch.targeted.store(true, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    assert!(matches!(r.env.run(&drive).await, DriveEnd::RoleLost(_)), "a targeted role ends the drive RoleLost");
    match next_within(&mut reader).await {
        Some(Read::RoleLost(loss)) => assert_eq!(loss.to.as_deref(), Some("mesh2.admin.1"), "the stream names the committed successor: {loss:?}"),
        other => panic!("the reader ends with the role loss, got {other:?}"),
    }
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
}

/// CONTRACT: an executor that cannot be reached is replaced only on proof of its departure. The
/// attempt claimed for it stays its own, the drive stalls naming why on its span, and no other admin
/// is claimed for it and nothing runs. The wait frames nothing to the caller (node-rpc-envelope.md:
/// Blocked's "only source is the subject's Status `Blocked` reply to the executor's ProbeNodeState").
#[tokio::test]
async fn an_unreachable_executor_without_proof_keeps_its_attempt_and_frames_nothing() {
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
    let waiting = frames_now(&mut reader).await;
    assert_eq!(blocked_of(&waiting), Vec::<BuildReply>::new(), "the wait for the departure proof frames no Blocked to the caller: no subject's Status Blocked reply made one");
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
        vec!["node.create:mesh1.admin.2".to_string(), "node.create:mesh1.rpc.2".to_string()],
        "the lost admin's path is part of what is left, planned from the view as it is now"
    );
}

/// CONTRACT (#3016; elections §5: the role goes to "the lowest Ready NodeId of its own mesh";
/// "Executor departure proof, never silence, is required before claiming the next attempt for its
/// successor"): mesh2's in its role admin, the claimed executor, is lost and proven exited; mesh2's other
/// admin is held but silent, not yet in its role. The fabric-primary of mesh1 does not take mesh2's
/// work: the drive stalls naming the held admin on its span, never on the caller's stream
/// (node-rpc-envelope.md: Blocked's only source is the subject's Status `Blocked`), every re-run uses the departure already recorded
/// (no second call to the lost executor, no second proof), and once mesh2.admin.2 holds the role
/// the next attempt is claimed for it.
#[tokio::test]
async fn a_lost_executors_successor_is_its_same_mesh_admin_and_the_drive_waits_for_its_election() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    t.nodes.push(node("mesh2.admin.1", true, false));
    t.nodes.push(node("mesh2.admin.2", false, false));
    t.nodes.push(node("mesh2.rpc.1", true, false));
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) };
    let r = rig_for(t, change, |ran, _| ran).await;
    // Attempt 1 is mesh2.admin.1's; then it is lost, and the view shows its exit and mesh2.admin.2 silent.
    assert!(matches!(r.env.door.claim("mesh2.admin.1", &r.build, 1).await, rdm_core::build_claim::BuildClaimReply::Won { .. }));
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
    let mut reader = drive.reader(1);
    for pass in 0..3 {
        match r.env.run(&drive).await {
            DriveEnd::Stalled(why) => assert!(why.contains("mesh2") && why.contains("mesh2.admin.2") && why.contains("PendingReconnect"), "pass {pass}: {why}"),
            other => panic!("pass {pass}: the fabric-primary never takes mesh2's work while mesh2.admin.2 is held: {other:?}"),
        }
    }
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the wait for mesh2's election frames no Blocked to the caller: no subject's Status Blocked reply made one");
    assert_eq!(r.proofs.calls.load(Ordering::SeqCst), 3, "each pass proves the departure itself: nothing holds it between passes");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh2.admin.1")), "nothing is claimed while mesh2's role is held");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty() && r.ran.0.lock().unwrap().is_empty());
    // mesh2.admin.2 is primary: the next attempt is its own.
    for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.2") {
        n.status = NodeStatus::Ready;
        n.is_primary = true;
    }
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh2.admin.2".to_string(), 2)], "attempt 2 is the same-mesh successor's");
    assert_eq!(r.proofs.calls.load(Ordering::SeqCst), 4, "the run that claims attempt 2 proves the departure once more");
}

/// Births `mesh2.admin.1` anew in place of its exited birth when its create has run, in its role as
/// mesh2's primary, as the restore of a mesh's first admin does.
struct Recovered {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for Recovered {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.run(build, lane, attempt, op).await?;
        if op.key() == "node.create:mesh2.admin.1" {
            let mut v = self.topology.write().await;
            v.nodes.retain(|n| n.name.to_string() != "mesh2.admin.1");
            v.nodes.push(node("mesh2.admin.1", true, false));
        }
        Ok(())
    }
}

/// CONTRACT (R-D1, Luke 10-08: "Loss of peer-mesh control-plane visibility targets lifecycle
/// mutation for every node in that mesh ... Only the peer-mesh investigation may begin
/// restoration of the mesh's first recovering node-admin; once mesh authority is restored, that mesh
/// performs its own ordinary-node reconciliation"; mesh-investigation.md steps 5-6): mesh2's only
/// node-admin is proven exited, so the fabric-primary of mesh1 is the executor of mesh2's work in
/// its view, and the investigation holds mesh2 unheard. While no recovery is decided the drive claims
/// nothing and dispatches nothing for mesh2, and says so on its own span naming the mesh, the
/// operation and how long the mesh is unheard, never on the caller's stream (node-rpc-envelope.md:
/// Blocked's only source is the subject's Status `Blocked`). Once the investigation decides the recovery, the
/// mesh's first recovering node-admin, and only it, is claimed and run on the fabric-primary; the
/// mesh's second admin and its ordinary node wait until the mesh is heard again, and are then run by
/// mesh2's own primary.
#[tokio::test]
async fn a_peer_mesh_held_unheard_is_never_claimed_for_until_its_recovery_and_then_only_its_first_admin() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    t.nodes.push(node("mesh2.rpc.1", true, false));
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(Recovered { ran, topology })).await;
    let held = |recovery_decided: bool| rdm_admin::investigate::PeerMesh { unheard_ms: Some(31_000), recovery_decided, unreachable_decided: false };
    r.switch.peer.lock().unwrap().insert("mesh2".into(), held(false));
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3016-unheard-mesh-drive");
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let before = r.builds.read_build(&r.build).await.unwrap();

    // Held unheard, no recovery decided: nothing is claimed or dispatched for mesh2.
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("mesh2") && why.contains("31000"), "the stall names the mesh and how long it is unheard: {why}"),
        other => panic!("the fabric-primary claims nothing for a peer mesh held unheard: {other:?}"),
    }
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (before.attempt, None), "no attempt is claimed across the target");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty() && r.ran.0.lock().unwrap().is_empty(), "nothing dispatched, nothing run");
    let targeted = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    assert_eq!(targeted.len(), 1, "the target is named once: {targeted:?}");
    let a = &targeted[0].1;
    assert_eq!((a.get("mesh").map(String::as_str), a.get("target").map(String::as_str), a.get("unheard_ms").map(String::as_str), a.get("recovery_decided").map(String::as_str)), (Some("mesh2"), Some("mesh2.admin.1"), Some("31000"), Some("false")), "{a:?}");
    eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={} {a:?}", targeted[0].0);
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the target on the unheard mesh is named on its span and frames no Blocked to the caller");

    // The investigation decided the recovery: the first recovering node-admin, and only it, runs, on
    // the fabric-primary.
    r.switch.peer.lock().unwrap().insert("mesh2".into(), held(true));
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("node.create:mesh2.admin.2") && why.contains("first recovering node-admin"), "the mesh's second admin waits for the mesh to be heard: {why}"),
        other => panic!("mesh2's other nodes wait until the mesh is heard again: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh2.admin.1".to_string()], "only the first recovering node-admin ran");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), before.attempt + 1)], "the restore ran on the fabric-primary");
    assert!(r.proofs.given.lock().unwrap().is_empty(), "a recovery the ladder decided is told by the ladder, never by the proof of a fully crashed mesh");
    let second = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    assert_eq!(second.len(), 2, "{second:?}");
    assert_eq!((second[1].1.get("target").map(String::as_str), second[1].1.get("recovery_decided").map(String::as_str)), (Some("mesh2.admin.2"), Some("true")), "{:?}", second[1].1);
    eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={} {:?}", second[1].0, second[1].1);
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the second admin's wait for the mesh frames no Blocked to the caller");

    // Heard again: mesh2's own primary runs the rest of the mesh.
    r.switch.peer.lock().unwrap().remove("mesh2");
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh2.admin.1".to_string(), "node.create:mesh2.admin.2".to_string(), "node.create:mesh2.rpc.2".to_string()]);
    let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
    assert_eq!(dispatched.last().map(|(x, _)| x.as_str()), Some("mesh2.admin.1"), "the mesh reconciles its own nodes once heard: {dispatched:?}");
}

/// mesh2 has lost every node it had, both node-admins included, and the Build grows its rpc nodes to
/// two; the investigation's state on mesh2 is the cell's.
async fn crashed_rig() -> Rig {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    for n in ["mesh2.admin.1", "mesh2.admin.2", "mesh2.rpc.1"] {
        let mut lost = node(n, false, false);
        lost.status = NodeStatus::Dead;
        t.nodes.push(lost);
    }
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) };
    rig_for(t, change, |ran, topology| Arc::new(Recovered { ran, topology })).await
}

/// CONTRACT (mesh-investigation.md step 5, "Fully crashed": "The fabric-primary acts on the provider's
/// exact exited verdict, never on silence, and creates mesh-x's node-admin in process"; R-D1): mesh2 has
/// lost every node-admin and the investigation's 60 s decision on it came out `unreachable`, a hold.
/// The drive reads the provider's exited verdict of every node of mesh2 the accepted topology names (its
/// two node-admins, its lost rpc node and the rpc node the Build grows, not yet born) itself, and on
/// that proof it runs the create of the mesh's first node-admin, and only it, on the fabric-primary; the mesh's
/// second admin and its ordinary node still wait for the mesh to be heard. With no decision at all
/// the target stands for the first admin too. Must NOT happen: the target refusing the one create the
/// exited verdict admits (the mesh waiting for ever), or the hold releasing more than that create.
#[tokio::test]
async fn the_create_of_a_fully_crashed_meshs_first_node_admin_runs_on_the_fabric_primary_and_nothing_else_of_the_held_mesh_does() {
    let r = crashed_rig().await;
    r.proofs.exited.lock().unwrap().extend(["mesh2.admin.1", "mesh2.admin.2", "mesh2.rpc.1", "mesh2.rpc.2"].map(String::from));
    let held = |unreachable_decided: bool| rdm_admin::investigate::PeerMesh { unheard_ms: Some(61_000), recovery_decided: false, unreachable_decided };
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-s56-crashed-mesh-drive");
    let drive = Drive::detached(r.build.clone());
    let before = r.builds.read_build(&r.build).await.unwrap();

    // No decision: the target stands for the first admin.
    r.switch.peer.lock().unwrap().insert("mesh2".into(), held(false));
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("node.create:mesh2.admin.1") && why.contains("no recovery decided"), "{why}"),
        other => panic!("nothing is claimed for a held mesh with no decision: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing dispatched, nothing run");

    // The 60 s decision came out unreachable: the create of the first node-admin, and only it, runs on the fabric-primary.
    r.switch.peer.lock().unwrap().insert("mesh2".into(), held(true));
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("node.create:mesh2.admin.2") && why.contains("unreachable"), "the second admin waits for the mesh to be heard, and the stall names the decision: {why}"),
        other => panic!("the mesh's other nodes wait until it is heard again: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh2.admin.1".to_string()], "only the mesh's first node-admin ran");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), before.attempt + 1)], "the create ran in process on the fabric-primary");
    // R-DG1 (mesh-investigation.md steps 5-6): the fabric-primary gave the mesh a node-admin when it claimed the create, once.
    assert_eq!(*r.proofs.given.lock().unwrap(), vec![("mesh2".to_string(), r.build.to_string(), before.attempt + 1)], "the claim of the fully crashed mesh's create told the proof the mesh was given a node-admin");
    let targeted = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    assert_eq!(targeted.len(), 2, "the target is named for the undecided admin.1 and then for admin.2: {targeted:?}");
    assert_eq!((targeted[1].1.get("target").map(String::as_str), targeted[1].1.get("recovery_decided").map(String::as_str)), (Some("mesh2.admin.2"), Some("false")), "{:?}", targeted[1].1);
    eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={} {:?}", targeted[1].0, targeted[1].1);
}

/// The R-D1 fixture: mesh2's only node-admin is proven exited (so the fabric-primary of mesh1 is
/// the executor of mesh2's work in its view), mesh2 keeps its rpc node, the Build grows mesh2 to two
/// admins and two rpc nodes, a restored `mesh2.admin.1` is primary as mesh2's primary, and the
/// investigation holds mesh2 unheard with its recovery decided.
async fn recovery_rig() -> Rig {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    t.nodes.push(node("mesh2.rpc.1", true, false));
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(Recovered { ran, topology })).await;
    r.switch.peer.lock().unwrap().insert("mesh2".into(), rdm_admin::investigate::PeerMesh { unheard_ms: Some(61_000), recovery_decided: true, unreachable_decided: false });
    r
}

/// CONTRACT (R-D1; mesh-investigation.md step 6, "A recovery verdict starts the existing Mesh
/// Recovery flow ... on the SAME accepted Build"; mesh-recovery.md step 2, the attempt "runs
/// node.create for the mesh's first recovering node-admin"): which node-admin of a held mesh the
/// recovery already admitted is a fact of the Build (the receipts of an attempt an admin outside the
/// mesh held), not of the drive that dispatched it. After one drive restored `mesh2.admin.1` and
/// stopped at the target on `mesh2.admin.2`, a second drive instance over the same Build admits no
/// second admin, whether it finds the next attempt open or already claimed for another admin
/// outside mesh2: nothing more runs, and it names the target on `mesh2.admin.2`.
#[tokio::test]
async fn a_second_drive_over_the_same_build_admits_no_second_recovering_admin_of_a_held_mesh() {
    let r = recovery_rig().await;
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3016-second-drive");
    let first = Drive::detached(r.build.clone());
    match r.env.run(&first).await {
        DriveEnd::Stalled(why) => assert!(why.contains("node.create:mesh2.admin.2"), "the first drive stops at mesh2's second admin: {why}"),
        other => panic!("the first drive restores one admin and stops: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh2.admin.1".to_string()]);
    // A second drive instance (a drive replaced on the same fabric-primary) over the same Build.
    let second = Drive::detached(r.build.clone());
    match r.env.run(&second).await {
        DriveEnd::Stalled(why) => assert!(why.contains("node.create:mesh2.admin.2") && why.contains("first recovering node-admin"), "a second drive admits no second admin: {why}"),
        other => panic!("a second drive over the same Build admits no second recovering admin: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh2.admin.1".to_string()], "no second admin of mesh2 crossed the target");
    // The next attempt already claimed for another admin outside mesh2 when a third drive finds it.
    let open = r.builds.read_build(&r.build).await.unwrap().attempt + 1;
    assert!(matches!(r.env.door.claim("mesh1.admin.2", &r.build, open).await, rdm_core::build_claim::BuildClaimReply::Won { .. }));
    let third = Drive::detached(r.build.clone());
    let end = r.env.run(&third).await;
    assert!(matches!(&end, DriveEnd::Stalled(why) if why.contains("node.create:mesh2.admin.2")), "a running attempt held by another admin admits no second admin either: {end:?}");
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh2.admin.1".to_string()], "no second admin of mesh2 crossed the target");
    // Each drive names the target on mesh2's second admin once, on its own span.
    let targeted = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    assert_eq!(targeted.len(), 3, "{targeted:?}");
    for (trace, a) in &targeted {
        assert_eq!((a.get("target").map(String::as_str), a.get("recovery_decided").map(String::as_str)), (Some("mesh2.admin.2"), Some("true")), "{a:?}");
        eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={trace} {a:?}");
    }
}

/// CONTRACT (R-BI7 caveat 3: one attempt in flight per mesh, the mesh's in-flight row on the store
/// root, released by the attempt's end): a node recovery takes the in-flight row of the mesh the
/// recovered node is in, whoever executes it (control_plane.rs, `decide_claim` on the node's mesh). The
/// fabric-primary `mesh1.admin.1` claims the recovery of `mesh2.rpc.1`, which takes mesh2's row,
/// and the drive runs the attempt to its terminal: mesh2's row is released with it. Must NOT happen:
/// the end releasing only its executor's mesh (mesh1), leaving mesh2 held by an attempt that ended,
/// so every later attempt in mesh2 is refused build-in-progress naming it (the authority soak,
/// 14034530d: Build nbw0yxzwgfg0 stalled 120 s on mesh2's row left by attempt 6 of 7z548bear6nj).
#[tokio::test]
async fn a_recovery_the_fabric_primary_executes_in_another_mesh_releases_that_meshs_in_flight_row_at_its_end() {
    use rdm_admin::accepted::AttemptAction;
    use rdm_admin::build_state::{AttemptOpened, AttemptReason, BuildAttemptClaim};
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    for (n, primary) in [("mesh2.admin.1", true), ("mesh2.rpc.1", false)] {
        t.nodes.push(node(n, primary, false));
    }
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(Lands { ran, topology, lands: true })).await;
    let first = Drive::detached(r.build.clone());
    let end = r.env.run(&first).await;
    assert!(matches!(end, DriveEnd::Terminal), "the Build's own attempt ends: {end:?}");

    // The recovery of mesh2.rpc.1, opened and claimed by the fabric-primary on mesh2's row.
    let recovered = r.env.topology.read().await.node(&"mesh2.rpc.1".parse().unwrap()).unwrap().clone();
    let attempt = r.builds.read_build(&r.build).await.unwrap().attempt + 1;
    let action = AttemptAction::Recover { path: recovered.name.clone(), node_id: recovered.node_id.clone(), from_incarnation: recovered.incarnation_id.clone().unwrap(), backbone: false };
    r.builds
        .open_attempt(&AttemptOpened { build_id: r.build.clone(), attempt, reason: AttemptReason::Recovery, action: Some(action), opened_by: "mesh1.admin.1".into(), opened_at_ms: 1, mesh: None, context: None })
        .await
        .unwrap();
    let claim = BuildAttemptClaim { build_id: r.build.clone(), attempt, executor: "mesh1.admin.1".into(), claimant: Some("mesh1.admin.1".into()), mesh: None };
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let won = rdm_admin::build_claim::decide_claim(&me, &*r.builds, &*r.env.door.claims, &r.env.door.contexts, &claim, "mesh2", None).await;
    assert!(matches!(won, rdm_core::build_claim::BuildClaimReply::Won { .. }), "{won:?}");
    assert_eq!(r.env.door.claims.in_flight("mesh2").await.unwrap().map(|h| h.attempt), Some(attempt), "the recovery holds mesh2's row");

    let second = Drive::detached(r.build.clone());
    let _ = r.env.run(&second).await;
    let ended = r.builds.read_build(&r.build).await.unwrap();
    assert!(ended.attempt >= attempt, "the recovery attempt ran: {:?}", ended.attempt);
    let held = r.env.door.claims.in_flight("mesh2").await.unwrap();
    assert!(held.as_ref().is_none_or(|h| h.attempt != attempt), "mesh2's row is released at the recovery's end, not left naming attempt {attempt}: {held:?}");
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
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.run(build, lane, attempt, op).await?;
        if let (true, BuildOperation::CreateNode { node: p, .. }) = (self.lands, op) {
            self.topology.write().await.nodes.push(node(&p.to_string(), false, false));
        }
        Ok(())
    }
}

/// Two meshes: the fabric-primary `mesh1.admin.1` is mesh1's primary too, so it executes mesh1's
/// members; mesh2's in its role admin is proven exited and its other admin is held but silent
/// (PendingReconnect), so no admin executes mesh2's work in this view. The Build grows mesh1 by one
/// rpc node and restores mesh2's two admins.
async fn held_successor_rig(lands: bool) -> Rig {
    let mut t = view();
    for n in t.nodes.iter_mut() {
        n.is_primary = n.name.to_string() == "mesh1.admin.1";
    }
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    let mut held = node("mesh2.admin.2", false, false);
    held.status = NodeStatus::PendingReconnect;
    t.nodes.push(held);
    t.nodes.push(node("mesh2.rpc.1", false, false));
    let counts = |mesh: &str, admins: u32, rpcs: u32| MeshDesired::of(mesh.to_string(), [(rdm_core::NodeKind::NodeAdmin, admins), (rdm_core::NodeKind::RpcNode, rpcs)]);
    let change = TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![counts("mesh1", 2, 2), counts("mesh2", 2, 1)] } };
    rig_for(t, change, move |ran, topology| Arc::new(Lands { ran, topology, lands })).await
}

/// CONTRACT (elections §5, the same-mesh successor; R-A2: cross-mesh only when
/// every node-admin of the mesh is proven lost): a run of the fabric-primary is cut before a later operation no
/// admin executes in its view. The fabric-primary runs mesh1's rpc node and stops there: mesh2's
/// admins are not carried by the run, and the drive stalls naming mesh2's held admin, whose role the
/// rest waits for.
#[tokio::test]
async fn a_run_is_cut_before_an_operation_of_a_mesh_whose_admin_is_held_and_the_rest_waits_for_its_election() {
    let r = held_successor_rig(true).await;
    let drive = Drive::detached(r.build.clone());
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("mesh2") && why.contains("mesh2.admin.2") && why.contains("PendingReconnect"), "the stall names the held admin: {why}"),
        other => panic!("mesh2's work waits for its own role: {other:?}"),
    }
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh1.rpc.2".to_string()], "the run was cut before mesh2's admins");
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
    async fn run(&self, _: &BuildId, _lane: Option<&str>, _: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
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
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    let mut held = node("mesh2.admin.2", false, false);
    held.status = NodeStatus::PendingReconnect;
    t.nodes.push(held);
    t.nodes.push(node("mesh2.rpc.1", false, false));
    let counts = |mesh: &str, admins: u32, rpcs: u32| MeshDesired::of(mesh.to_string(), [(rdm_core::NodeKind::NodeAdmin, admins), (rdm_core::NodeKind::RpcNode, rpcs)]);
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
        verdicts: Arc::new(rdm_admin::build_op::LocalVerdicts { local: r.builds.clone() }),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3016-remote-receipt");
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(2), env.run(&drive)).await;
    let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
    let ran = r.ran.0.lock().unwrap().clone();
    assert_eq!((dispatched.len(), ran.len()), (1, 1), "the completed operation is dispatched once: dispatched {:?}, ran {:?}", &dispatched[..dispatched.len().min(4)], &ran[..ran.len().min(4)]);
    assert!(remote.read_build(&r.build).await.unwrap().completed_in_run("node.create:mesh1.rpc.2"), "the receipt is durable on the executor");
    assert!(!r.builds.read_build(&r.build).await.unwrap().completed_in_run("node.create:mesh1.rpc.2"), "the fabric-primary's log has not heard the receipt");
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
/// names mesh2's held admin as mesh2's primary, as a mesh whose role is taken while another mesh's
/// run goes on.
struct LandsAndTakesRole {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for LandsAndTakesRole {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.run(build, lane, attempt, op).await?;
        if let BuildOperation::CreateNode { node: p, .. } = op {
            let mut v = self.topology.write().await;
            v.nodes.push(node(&p.to_string(), false, false));
            if p.to_string() == "mesh1.rpc.2" {
                for n in v.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.2") {
                    n.status = NodeStatus::Ready;
                    n.is_primary = true;
                }
            }
        }
        Ok(())
    }
}

/// CONTRACT: a run handed back to the fabric-primary is claimed for the admin its view names for what
/// is left, never for the fabric-primary to hand on unrun. The fabric-primary's run of mesh1's rpc
/// node is cut before mesh2's admin (no admin of mesh2 is primary when it is planned) and handed back;
/// by then mesh2's admin holds the role. The next attempt is claimed for `mesh2.admin.2` directly: no
/// attempt of the fabric-primary carries nothing to it.
#[tokio::test]
async fn a_run_handed_back_is_claimed_for_the_admin_the_view_names_for_what_is_left() {
    let mut t = view();
    for n in t.nodes.iter_mut() {
        n.is_primary = n.name.to_string() == "mesh1.admin.1";
    }
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    let mut lost = node("mesh2.admin.1", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    let mut held = node("mesh2.admin.2", false, false);
    held.status = NodeStatus::PendingReconnect;
    t.nodes.push(held);
    t.nodes.push(node("mesh2.rpc.1", false, false));
    let counts = |mesh: &str, admins: u32, rpcs: u32| MeshDesired::of(mesh.to_string(), [(rdm_core::NodeKind::NodeAdmin, admins), (rdm_core::NodeKind::RpcNode, rpcs)]);
    let change = TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![counts("mesh1", 2, 2), counts("mesh2", 2, 1)] } };
    let r = rig_for(t, change, |ran, topology| Arc::new(LandsAndTakesRole { ran, topology })).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh1.rpc.2".to_string(), "node.create:mesh2.admin.1".to_string()]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1), ("mesh2.admin.2".to_string(), 2)], "the hand-back is claimed for mesh2's primary admin");
}

/// CONTRACT (node-rpc-envelope.md, Build op `0x20`: "An executor that cannot be reached is replaced
/// only on proof of its departure"; the successor re-checks the world for what the lost executor's run
/// left): what counts as done is what an executor reported, by its run's verdict (`HandedOff` or
/// `Converged`). `mesh1.admin.2` wrote the `Complete` receipt of `mesh1.rpc.2` in attempt 1 and was
/// lost before its run ended: no verdict, and the view never held the node. Its departure is proven,
/// mesh1 names the fabric-primary, and attempt 2 creates `mesh1.rpc.2` with the rest of the plan: a lost run's
/// receipt does not make its node done.
#[tokio::test]
async fn a_lost_executors_unreported_receipt_does_not_make_its_operation_done() {
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 3)]) };
    let r = rig_for(view(), change, |ran, _| ran).await;
    assert!(matches!(r.env.door.claim("mesh1.admin.2", &r.build, 1).await, rdm_core::build_claim::BuildClaimReply::Won { .. }));
    let receipt = BuildStepReceipt { mesh: None, build_id: r.build.clone(), attempt: 1, operation: "node.create:mesh1.rpc.2".into(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None };
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
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh1.admin.2".to_string(), "node.create:mesh1.rpc.2".to_string(), "node.create:mesh1.rpc.3".to_string()], "the lost run's node is created again");
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
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh1.rpc.2".to_string()]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "one attempt, on the admin the fabric-primary's view named");
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 1);
}

/// The gate of a fabric-primary whose node.fabric-primary.update moves its role to `mesh2.admin.1`:
/// `update_primaries` answers once the successor's installation is acknowledged, after which the committed
/// record names `mesh2.admin.1` and the view shows it holding the role. Every other gate is `inner`'s.
struct RoleMoves {
    inner: Arc<Switch>,
    topology: Arc<RwLock<Topology>>,
    /// Every move asked: `(primary, mesh-primary role, fabric-primary role, operation)`.
    moved: Mutex<Vec<(String, bool, bool, String)>>,
}

#[async_trait::async_trait]
impl Gate for RoleMoves {
    async fn target(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn update_primaries(&self, m: &rdm_admin::executor::RoleMove, _: &BuildId, _: u32, _: u32, operation: &str) -> Result<(), rdm_admin::build_drive::RoleMoveEnd> {
        self.moved.lock().unwrap().push((m.primary.name.to_string(), m.mesh_role, m.fabric_role, operation.to_string()));
        for n in self.topology.write().await.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh2.admin.1";
        }
        self.inner.target_to(Some("mesh2.admin.1"));
        Ok(())
    }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The last
/// node-admin of a mesh": "If the mesh holds the fabric-primary role, that role moves first by
/// `node.fabric-primary.update` with the departing mesh excluded (mesh-delete.md step 1)"; C1 step 1:
/// "The primary is commanded from above, as a step of the Build"; step 8: "Only then does the commanding
/// Build continue"): a mesh delete whose fabric-primary sits inside the leaving mesh moves the fabric
/// role alone (the mesh-primary role stays, mesh.delete never updates it) from the drive, as a step of the
/// attempt the primary claims for itself; that attempt ends `HandedOff` to the new role once the move
/// answered, and the drive ends `RoleLost` naming the new primary.
/// Must NOT happen: an operation dispatched at the leaving mesh's admin, the delete run there, or the
/// attempt ending with no verdict or handed to any admin but the new role.
#[tokio::test]
async fn a_mesh_delete_whose_fabric_primary_sits_inside_moves_the_fabric_primary_alone_in_its_own_attempt_handed_to_the_new_one() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    t.nodes.push(node("mesh2.admin.1", true, false));
    let r = rig_for(t, TopologyChange::RemoveMesh { mesh: "mesh1".into() }, |ran, _| ran).await;
    let gate = Arc::new(RoleMoves { inner: r.switch.clone(), topology: r.env.topology.clone(), moved: Mutex::new(Vec::new()) });
    let env = Arc::new(DriveEnv {
        me: r.env.me.clone(),
        topology: r.env.topology.clone(),
        builds: r.env.builds.clone(),
        door: r.env.door.clone(),
        dispatcher: r.env.dispatcher.clone(),
        gate: gate.clone(),
        departure: r.env.departure.clone(),
        verdicts: r.env.verdicts.clone(),
        notices: r.env.notices.clone(),
        turns: r.env.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("is no longer the fabric-primary: "), "the drive ends at the target: {why}"),
        other => panic!("the old primary's drive ends RoleLost once the role moved: {other:?}"),
    }
    let moved = gate.moved.lock().unwrap().clone();
    assert_eq!(moved.len(), 1, "one node.fabric-primary.update: {moved:?}");
    let (primary, mesh_role, fabric_role, operation) = &moved[0];
    assert_eq!((primary.as_str(), *mesh_role, *fabric_role), ("mesh1.admin.1", false, true), "the fabric-primary role alone moves from the fabric-primary: {moved:?}");
    assert!(operation.starts_with("mesh.delete:"), "the update names the mesh delete it precedes: {operation}");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no operation was dispatched: {:?}", r.dispatch.dispatched.lock().unwrap());
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran at the leaving mesh's admin: {:?}", r.ran.0.lock().unwrap());
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.attempt_executors.get(&1).map(String::as_str)), (1, Some("mesh1.admin.1")), "the move is a step of the attempt the holder claimed for itself: {:?}", p.attempt_executors);
    let verdicts: Vec<_> = r.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
        rdm_admin::build_state::BuildFact::Attempt(a) if a.build_id == r.build => Some((a.attempt, a.outcome)),
        _ => None,
    }).collect();
    assert_eq!(verdicts, vec![(1, rdm_admin::build_state::AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() })], "the holder's attempt ends handed to the new role, which claims the next");
    assert!(matches!(next_within(&mut reader).await, Some(Read::RoleLost(loss)) if loss.to.as_deref() == Some("mesh2.admin.1")), "the callers' streams end naming the new role");
}

/// CONTRACT (R-ON8: the fabric-primary plans and dispatches): an executor runs exactly the
/// operations the call carries and never plans the Build again from its own view. The
/// fabric-primary's view holds the node a previous run just launched (`mesh1.rpc.2`); the
/// executor's view has not heard it yet. What is left is `mesh1.rpc.3` alone, and that is all the
/// executor runs: the node launched a moment ago is not created a second time.
#[tokio::test]
async fn an_executor_whose_view_lacks_a_just_launched_node_creates_only_what_the_call_carries() {
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 3)]) };
    let mut ours = view();
    ours.nodes.push(node("mesh1.rpc.2", false, false));
    let r = rig_for(ours, change, |ran, _| ran).await;
    r.dispatch.sees("mesh1.admin.2", Arc::new(RwLock::new(view())));
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh1.rpc.3".to_string()], "only what the fabric-primary planned ran; mesh1.rpc.2 was launched already");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)]);
}

/// Primaries `mesh2.admin.1` as mesh2's primary when its create has run, as the launch and join of the
/// first admin of a mesh do.
struct MeshBirth {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for MeshBirth {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.run(build, lane, attempt, op).await?;
        if op.key() == "node.create:mesh2.admin.1" {
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
async fn a_run_that_creates_a_meshs_first_admin_hands_the_rest_of_the_mesh_to_it() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 1)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(MeshBirth { ran, topology })).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh2.admin.1".to_string(), "node.create:mesh2.rpc.1".to_string()]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1), ("mesh2.admin.1".to_string(), 2)], "mesh2's own primary ran the members");
}

/// Runs nothing to its end: every operation is held open, and the cell is told when the run began.
struct HeldOpen {
    began: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl OperationRunner for HeldOpen {
    async fn run(&self, _: &BuildId, _lane: Option<&str>, _: u32, _: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.began.notify_one();
        std::future::pending().await
    }
}

/// CONTRACT (lifecycles/node-primary-update.md step 4): the fabric-primary record this admin holds is
/// the target every decision is judged against. The target is set while the drive waits on an executor's run, and the
/// record names another primary than that executor: the drive ends `RoleLost` at the target, without
/// waiting for that run's stream to end, the callers' streams end naming the primary the record names,
/// and no later attempt is claimed.
#[tokio::test]
async fn a_fabric_primary_record_naming_another_birth_than_the_executor_ends_the_drive_naming_that_birth() {
    let held = Arc::new(HeldOpen { began: tokio::sync::Notify::new() });
    let runner = held.clone();
    let r = rig_for(view(), TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) }, move |_, _| -> Arc<dyn OperationRunner> { runner }).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let (env, d) = (r.env.clone(), drive.clone());
    let running = tokio::spawn(async move { env.run(&d).await });
    held.began.notified().await;
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "attempt 1 is running at the executor");
    r.switch.target_to(Some("mesh2.admin.1"));
    let end = tokio::time::timeout(Duration::from_secs(5), running).await.expect("the drive ends at the target while the executor's run is still open").unwrap();
    assert!(matches!(&end, DriveEnd::RoleLost(why) if why.contains("is no longer the fabric-primary: ")), "the target ends the drive RoleLost: {end:?}");
    match next_within(&mut reader).await {
        Some(Read::RoleLost(loss)) => assert_eq!(loss.to.as_deref(), Some("mesh2.admin.1"), "the stream names the primary the record names: {loss:?}"),
        other => panic!("the reader ends with the role loss, got {other:?}"),
    }
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.2")), "no later attempt was claimed");
    assert_eq!(r.dispatch.dispatched.lock().unwrap().len(), 1, "nothing more was dispatched");
}

/// An executor's run the cell feeds item by item: `dispatch` hands the drive the stream and says so.
struct Fed {
    items: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<rdm_admin::build_drive::InnerItem>>>,
    dispatched: tokio::sync::Notify,
}

struct FedStream(tokio::sync::mpsc::UnboundedReceiver<rdm_admin::build_drive::InnerItem>);

#[async_trait::async_trait]
impl rdm_admin::build_drive::InnerStream for FedStream {
    async fn next(&mut self) -> rdm_admin::build_drive::InnerItem {
        self.0.recv().await.unwrap_or(rdm_admin::build_drive::InnerItem::Ended)
    }
}

#[async_trait::async_trait]
impl rdm_admin::build_drive::Dispatcher for Fed {
    async fn dispatch(&self, _turn: &rdm_admin::stop_gate::Turn, _: &PathName, _: &BuildId, _: u32, _: rdm_core::context::CallContext, _: Vec<Vec<u8>>, _: rdm_admin::executor::RunPlan) -> rdm_admin::build_drive::Dispatched {
        let items = self.items.lock().unwrap().take().expect("one dispatch");
        self.dispatched.notify_one();
        rdm_admin::build_drive::Dispatched::Stream(Box::new(FedStream(items)))
    }
}

/// CONTRACT (node-rpc-envelope.md 0x20: "A fabric-primary change ends the outer stream with
/// FabricPrimaryMoved, **not Indeterminate**"; FabricPrimaryMoved: "caller resubmits the same build_id
/// there"): the target is set while the drive waits on the executor's run, and the record names that
/// executor: the run's own node.primary.update made it fabric-primary. The executor is the fabric-primary once it has
/// installed the election's result, and it says so on the run's own stream, which it ends with
/// FabricPrimaryMoved naming itself. Until then the callers' streams carry the run's frames and no role loss;
/// on the executor's FabricPrimaryMoved the drive ends `RoleLost` naming the executor, and no later attempt
/// is claimed (the executor resumes the same attempt).
/// Must NOT happen: a caller's stream ending with the role loss, which sends the caller to the
/// executor, before the executor's run said it holds the role.
#[tokio::test]
async fn a_fabric_primary_record_naming_the_executor_ends_the_drive_on_its_fabric_primary_moved_frame_never_before() {
    use rdm_admin::build_drive::InnerItem;
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
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let (e, d) = (env.clone(), drive.clone());
    let running = tokio::spawn(async move { e.run(&d).await });
    tokio::time::timeout(Duration::from_secs(5), fed.dispatched.notified()).await.expect("attempt 1 is dispatched");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.2")), "attempt 1 runs at the executor");
    // The executor's PrimaryUpdated step reached the primary, whose row is the target.
    r.switch.target_to(Some("mesh1.admin.2"));
    let step = BuildReply::Step {
        build_id: r.build.to_string(),
        attempt: 1,
        operation: "node.stop:mesh1.admin.1".into(),
        step: "PrimaryUpdated".into(),
        receipt: None,
    };
    feed.send(InnerItem::Frame(step.clone())).unwrap();
    match next_within(&mut reader).await {
        Some(Read::Frame(f)) => assert_eq!(f, step, "the run's frame reaches the callers"),
        other => panic!("before the executor says it holds the primary role, the callers read the run's frames and no role loss, got {other:?}"),
    }
    assert!(next_within(&mut reader).await.is_none(), "no role loss before the executor's new-primary frame");
    assert!(!running.is_finished(), "the drive waits for the executor's new-primary frame");
    // The executor installed the election's result: its run's stream ends FabricPrimaryMoved.
    feed.send(InnerItem::Frame(BuildReply::FabricPrimaryMoved { build_id: r.build.to_string(), fabric_primary: Some("mesh1.admin.2".into()) })).unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), running).await.expect("the drive ends on the executor's new-primary frame").unwrap();
    assert!(matches!(&end, DriveEnd::RoleLost(why) if why.contains("is no longer the fabric-primary: ")), "the target ends the drive RoleLost: {end:?}");
    match next_within(&mut reader).await {
        Some(Read::RoleLost(loss)) => assert_eq!(loss.to.as_deref(), Some("mesh1.admin.2"), "the stream names the executor the transfer primary: {loss:?}"),
        other => panic!("the reader ends with the role loss, got {other:?}"),
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
/// AlreadyApplied with its terminal frame; a running Build with a live primary returns that primary's
/// stream"): the drive that completed attempt N is the record of that run. A caller whose read of
/// the Build predates the verdict of N (it showed attempt N running) is handed that drive, and
/// nothing runs again; once attempt N+1 is opened on the complete Build (a restart reopens it), the
/// caller is handed a new drive, which claims and dispatches N+1.
/// Must NOT happen: the completed drive replaced for a stale read (a second `build.attempt.run` of
/// attempt N), or kept for the reopened Build (attempt N+1 never claimed).
#[tokio::test]
async fn a_completed_drive_is_kept_for_its_own_attempt_and_replaced_once_the_build_is_reopened() {
    use rdm_admin::build_drive::Drives;
    use rdm_admin::build_state::{AttemptOpened, AttemptOutcome, AttemptReason, BuildAttemptReceipt};
    let r = rig().await;
    let drives = Arc::new(Drives::default());
    let first = drives.ensure(&r.env, &r.build, 1);
    let BuildReply::Complete { attempt: done, .. } = terminal_of(&first).await else { panic!("the first run completes the Build") };
    r.builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: r.build.clone(), attempt: done, outcome: AttemptOutcome::Converged }).await.unwrap();
    let stale = drives.ensure(&r.env, &r.build, done);
    assert!(Arc::ptr_eq(&stale, &first), "a read that showed attempt {done} running is handed the drive that completed it");
    r.builds
        .open_attempt(&AttemptOpened { mesh: None, build_id: r.build.clone(), attempt: done + 1, reason: AttemptReason::Stop, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 0, context: None })
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
    ends: Mutex<Option<rdm_admin::executor::RunEnd>>,
    /// The incomplete shutdown first records a receipt of the leave (an exit, an amendment): the
    /// attempt moved something before it ended.
    progress: std::sync::atomic::AtomicBool,
    /// The shutdown ends incomplete on its first run only.
    once: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl OperationRunner for LostLeaver {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
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
async fn removing_mesh2(ends: Option<rdm_admin::executor::RunEnd>) -> (Rig, Arc<LostLeaver>) {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
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
async fn seeded_before(r: &Rig) -> rdm_admin::build_state::CurrentBuild {
    use rdm_admin::build_state::{BuildFact, CurrentBuild};
    let older = r.builds.facts().await.unwrap().into_iter().find_map(|f| match f {
        BuildFact::Accepted(a) if a.build_id != r.build => Some(a.build_id),
        _ => None,
    });
    CurrentBuild::Completed(older.expect("the rig seeded a Build before the one under test"))
}

const LEAVE_INCOMPLETE: &str = "mesh.delete: mesh2.admin.1 sent no valid mesh-deleted; the workflow is incomplete and its receipts stand";

/// CONTRACT (R-BI4; mesh-delete.md, "Failure and receipts": "The next attempt resumes from the
/// receipts"; "Resume after the primary's loss"): a mesh.delete whose leaving mesh's primary is lost
/// ends its attempt incomplete, not failed. The attempt is handed back to the fabric-primary, so the
/// Build is still in progress and the current-Build read (last in, first out) returns it: it is not a
/// failed Build the read skips. The drive claims no further attempt while nothing the resume decides
/// on has moved, and names why; when the leaving mesh's primary is proven gone it claims the next
/// attempt, which runs the shutdown from the receipts and completes the Build. The hand-back frames
/// no `Blocked` to the caller (node-rpc-envelope.md: Blocked's "only source is the subject's Status
/// `Blocked` reply to the executor's ProbeNodeState"): neither the executor's run nor the drive frames
/// the incomplete operation; the caller's stream reads the resumed attempt's terminal.
/// Must NOT happen: the Build failed and skipped (the current Build becomes the older one, nothing
/// resumes the leave), an attempt claimed again on every pass of the drive, or a `Blocked` frame on
/// the caller's stream that no subject's Status `Blocked` reply made.
#[test]
fn a_leave_that_ends_incomplete_on_a_lost_primary_leaves_the_build_in_progress_and_resumes_when_the_loss_is_proven() {
    use rdm_admin::build_state::{AttemptOutcome, BuildFact, BuildState, CurrentBuild};
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, first, second, third, facts, ran, dispatched, after, read) = cap.run(async {
        let root = tracing::info_span!("cell.leave-incomplete-stays-in-progress");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        // The executor's run frames its progress on the call's stream, as an admin's does.
        r.dispatch.frames("mesh1.admin.1");
        // Attempt 1: the shutdown ends incomplete. One drive across the passes, as the fabric-primary keeps it.
        let drive = Drive::detached(r.build.clone());
        let mut reader = drive.reader(1);
        let first = r.env.run(&drive).await;
        let first = (first, r.builds.read_build(&r.build).await.unwrap(), r.builds.current_build().await.unwrap());
        let read_first = frames_now(&mut reader).await;
        // The same drive again with nothing moved: no attempt is claimed.
        let second = r.env.run(&drive).await;
        let second = (second, r.builds.read_build(&r.build).await.unwrap().attempt, r.dispatch.dispatched.lock().unwrap().len());
        let read_second = frames_now(&mut reader).await;
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
        let read_third = frames_now(&mut reader).await;
        (trace_id, first, second, third, facts, ran, dispatched, r.build.clone(), (read_first, read_second, read_third))
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
    assert_eq!(ran.iter().filter(|k| k.starts_with("mesh.delete:")).count(), 1, "the shutdown ran once, on the resumed attempt: {ran:?}");
    assert_eq!(dispatched, vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.1".to_string(), 2)], "attempt 2 was claimed for the fabric-primary, once");
    // The executor's run is parented to the attempt's recorded context (the claim's), so it is read by name.
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.build.update.via-reconcile").iter().any(|s| s["attributes"]["attempt"] == "1" && s["attributes"]["outcome"] == "handed-off" && s["attributes"]["incomplete"] == LEAVE_INCOMPLETE), "the executor's span says the attempt ended handed back, incomplete, with the reason");
    assert!(named("rdm.node_admin.build.update.via-reconcile").iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["outcome"] == "converged"), "attempt 2 resumed the shutdown and converged");
    let stalls: Vec<_> = named("rdm.node_admin.build.update.via-stalled").into_iter().filter(|s| s["trace_id"] == trace_id.as_str()).collect();
    assert_eq!(stalls.len(), 1, "the idle wait is named once across the two passes that made it: {stalls:?}");
    let (read_first, read_second, read_third) = read;
    assert_eq!(blocked_of(&read_first), Vec::<BuildReply>::new(), "the hand-back of attempt 1 frames no Blocked to the caller: no subject's Status Blocked reply made one");
    assert_eq!(blocked_of(&read_second), Vec::<BuildReply>::new(), "the idle wait frames no Blocked to the caller");
    assert_eq!(blocked_of(&read_third), Vec::<BuildReply>::new(), "the resumed attempt frames no Blocked to the caller");
    assert!(matches!(read_third.last(), Some(Read::Terminal(BuildReply::Complete { attempt: 2, .. }))), "the caller's stream ends with attempt 2's Complete: {read_third:?}");
}

/// CONTRACT (R-BI4: "Only an attempt that ended with a named terminal failure makes a Build
/// failed"): a shutdown that ends with the workflow's own named failure (not a lost participant) ends
/// the attempt `Failed`; the Build is failed, the current-Build read skips it and returns the Build
/// before it, and the drive claims nothing more: a caller resubmits it.
#[test]
fn a_leave_that_ends_with_a_named_failure_fails_the_build_and_the_read_skips_it() {
    use rdm_admin::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (end, p, current, seeded, dispatched) = cap.run(async {
        let root = tracing::info_span!("cell.leave-named-failure-fails-the-build");
        let _in_root = root.enter();
        let (r, _leaver) = removing_mesh2(Some(rdm_admin::executor::RunEnd::failed("mesh.delete: mesh2 is refused before it is dismantled: its handover is not confirmed".into()))).await;
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
/// unproven. The hold targets restart, replace and delete of the mesh's births, never the shutdown's
/// resume: the shutdown is dispatched on attempt 1, the Build stays in progress and nothing is claimed
/// again while the primary stays unproven, and once its exit is proven attempt 2 runs the shutdown
/// and completes the Build. The caller's stream carries no `Blocked` for the incomplete leave
/// (node-rpc-envelope.md: Blocked's "only source is the subject's Status `Blocked` reply to the
/// executor's ProbeNodeState").
/// Must NOT happen: the drive stopping at the unheard-mesh target before the shutdown, the Build
/// failed, the Build left in progress with no attempt claimed after the proof, or a `Blocked` frame on
/// the caller's stream that no subject's Status `Blocked` reply made.
#[test]
fn a_leave_of_a_mesh_held_unheard_is_refused_by_name_while_the_primary_is_unproven_and_resumes_when_it_is_proven() {
    use rdm_admin::build_state::{BuildState, CurrentBuild};
    let cap = crate::mesh_delete_resume::capture();
    let (first, second, third, current, dispatched, ran, read) = cap.run(async {
        let root = tracing::info_span!("cell.leave-unheard-unproven-primary");
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete("mesh.delete: the final primary mesh2.admin.1 is silent but not proven exited, so no admin is created for it".into()))).await;
        mesh2_primary_becomes(&r, NodeStatus::PendingReconnect).await;
        for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.mesh == "mesh2") {
            n.status = NodeStatus::PendingReconnect;
        }
        r.switch.peer.lock().unwrap().insert("mesh2".into(), rdm_admin::investigate::PeerMesh { unheard_ms: Some(120_000), recovery_decided: false, unreachable_decided: false });
        // The executor's run frames its progress on the call's stream, as an admin's does.
        r.dispatch.frames("mesh1.admin.1");
        let drive = Drive::detached(r.build.clone());
        let mut reader = drive.reader(1);
        let first = r.env.run(&drive).await;
        let dispatches = r.dispatch.dispatched.lock().unwrap().len();
        let first = (first, r.builds.read_build(&r.build).await.unwrap().state, r.builds.current_build().await.unwrap(), dispatches);
        let mut read = frames_now(&mut reader).await;
        let second = r.env.run(&drive).await;
        let dispatches = r.dispatch.dispatched.lock().unwrap().len();
        let second = (second, r.builds.read_build(&r.build).await.unwrap().attempt, dispatches);
        // The provider proves the primary's exit: it is gone from the view, and the shutdown can run.
        *mesh_deleter.ends.lock().unwrap() = None;
        mesh2_primary_becomes(&r, NodeStatus::Dead).await;
        read.extend(frames_now(&mut reader).await);
        let third = r.env.run(&drive).await;
        let current = r.builds.current_build().await.unwrap();
        let (dispatched, ran) = (r.dispatch.dispatched.lock().unwrap().clone(), r.ran.0.lock().unwrap().clone());
        read.extend(frames_now(&mut reader).await);
        (first, second, (third, r.builds.read_build(&r.build).await.unwrap().state), current, dispatched, (ran, r.build.clone()), read)
    });
    let spans = cap.spans();
    let (end, state, current_then, dispatches) = first;
    assert!(matches!(&end, DriveEnd::Stalled(why) if why.contains("ended incomplete and was handed back")), "the drive names the incomplete attempt: {end:?}");
    assert_eq!((state, dispatches), (BuildState::Pending, 1), "the shutdown was dispatched on attempt 1 although mesh2 is held unheard, and the Build waits for its next claim");
    assert!(matches!(current_then, CurrentBuild::InProgress(_)), "the Build is in progress, not failed: {current_then:?}");
    let (end, attempt, dispatches) = second;
    assert!(matches!(end, DriveEnd::Stalled(_)) && (attempt, dispatches) == (1, 1), "the unproven primary refuses the resume by name and no attempt is claimed: {end:?} attempt {attempt}, {dispatches} dispatches");
    let (end, state) = third;
    assert_eq!((end, state), (DriveEnd::Terminal, BuildState::Complete), "the exited verdict moved an input: attempt 2 runs the shutdown and completes the Build");
    let (ran, build) = ran;
    assert_eq!(current, CurrentBuild::Completed(build));
    assert_eq!(ran.iter().filter(|k| k.starts_with("mesh.delete:")).count(), 1, "{ran:?}");
    assert_eq!(dispatched, vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.1".to_string(), 2)]);
    assert!(!spans.iter().any(|s| s["name"] == "rdm.node_admin.build.reject.via-unheard-mesh-drive"), "the unheard hold did not target the shutdown's resume");
    assert_eq!(blocked_of(&read), Vec::<BuildReply>::new(), "neither the executor's run nor the drive frames a Blocked for the incomplete leave");
    assert!(matches!(read.last(), Some(Read::Terminal(BuildReply::Complete { attempt: 2, .. }))), "the caller's stream ends with attempt 2's Complete: {read:?}");
    assert!(spans.iter().any(|s| s["name"] == "rdm.node_admin.build.update.via-stalled"), "the refusal is spanned by name");
}

/// CONTRACT (R-S2: event-driven, never once per pass; mesh-delete.md, "Failure and receipts"): an
/// attempt of a leave that ends incomplete with nothing changed since it was claimed gets no next
/// attempt from the drive, however many times the drive passes; one input moving (the role of a
/// member of the leaving mesh) gets exactly one. Attempt 1 ends incomplete, two passes claim nothing,
/// the move opens attempt 2, which ends incomplete the same way and again claims nothing, and the
/// next move opens attempt 3, which completes.
/// Must NOT happen: an attempt claimed per pass of a leave whose primary stays lost.
#[test]
fn a_leave_that_ends_incomplete_with_nothing_changed_claims_one_attempt_per_change_never_one_per_pass() {
    use rdm_admin::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (passes, state, dispatched) = cap.run(async {
        let root = tracing::info_span!("cell.leave-one-attempt-per-change");
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete("mesh.delete: mesh2 holds no node-admin primary to hand the workflow to".into()))).await;
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

/// Adds `mesh3` (a Mesh record and its primary and one rpc node) to the rig's view: a mesh the leave
/// of `mesh2` does not act on.
async fn add_mesh3(r: &Rig) {
    let mut t = r.env.topology.write().await;
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh3".into(), status: ScopeStatus::Ready });
    t.nodes.push(node("mesh3.admin.1", true, false));
    t.nodes.push(node("mesh3.rpc.1", false, false));
}

/// CONTRACT (Q253 A; the drive's wait after an attempt handed back with nothing moved waits on
/// `world()`: the first operation left, the Build's receipts, the members of the mesh that operation
/// acts on by birth, status and primary, whether the fabric holds ready): a member change in a mesh the
/// next attempt's first operation does not act on claims no attempt. The leave of `mesh2` ends
/// incomplete; `mesh3`'s primary changes status, its rpc node is born again and its primary moves, and
/// the drive stays idle through each: no claim, no dispatch, one via-stalled span. A change of `mesh2`'s
/// own primary then claims the next attempt.
/// Must NOT happen: an attempt claimed for a change in another mesh.
#[test]
fn a_member_change_in_a_mesh_the_next_attempt_does_not_act_on_claims_no_attempt() {
    use rdm_admin::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes, state, dispatched) = cap.run(async {
        let root = tracing::info_span!("cell.other-mesh-change-claims-nothing");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        add_mesh3(&r).await;
        let drive = Drive::detached(r.build.clone());
        let mut passes = Vec::new();
        for pass in 0..6 {
            match pass {
                2 => {
                    for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh3.admin.1") {
                        n.status = NodeStatus::PendingReconnect;
                    }
                }
                3 => {
                    for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh3.rpc.1") {
                        n.incarnation_id = Some(IncarnationId::mint());
                    }
                }
                4 => {
                    let mut t = r.env.topology.write().await;
                    for n in t.nodes.iter_mut().filter(|n| n.name.to_string().starts_with("mesh3.")) {
                        n.is_primary = !n.is_primary;
                    }
                }
                5 => {
                    *mesh_deleter.ends.lock().unwrap() = None;
                    mesh2_primary_becomes(&r, NodeStatus::Dead).await;
                }
                _ => {}
            }
            let end = r.env.run(&drive).await;
            passes.push((matches!(end, DriveEnd::Stalled(_)), r.builds.read_build(&r.build).await.unwrap().attempt, r.dispatch.dispatched.lock().unwrap().len()));
        }
        let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
        (trace_id, passes, r.builds.read_build(&r.build).await.unwrap().state, dispatched)
    });
    let spans = cap.spans();
    let stalls: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-stalled" && s["trace_id"] == trace_id.as_str()).collect();
    println!("trace_id={trace_id}");
    assert_eq!(passes[..5], [(true, 1, 1); 5], "attempt 1 handed back; mesh3's status, birth and primary changes claim nothing and dispatch nothing: {passes:?}");
    assert_eq!(passes[5], (false, 2, 2), "mesh2's own primary changing claims attempt 2, which completes: {passes:?}");
    assert_eq!(state, BuildState::Complete);
    assert_eq!(dispatched, vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.1".to_string(), 2)], "two attempts: the other mesh's changes added none");
    assert_eq!(stalls.len(), 1, "the idle wait is installed once across the five passes that made or kept it: {stalls:?}");
}

/// CONTRACT (Q253 A: "a successor reconstructs it by one claim"): a successor fabric-primary is a fresh
/// drive with no idle marker over a Build whose last attempt was handed back with nothing moved. It
/// claims exactly one attempt; that attempt ends the same way and installs the wait; further passes with
/// the world unchanged claim nothing.
/// Must NOT happen: no claim by the successor (the Build never resumes), or one attempt per pass.
#[test]
fn a_successor_drive_over_a_handed_back_build_claims_one_attempt_installs_the_wait_and_claims_no_more() {
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, first_run, successor, dispatched) = cap.run(async {
        let root = tracing::info_span!("cell.successor-claims-one-attempt");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        // The first fabric-primary's drive: attempt 1 ends handed back, the wait is installed.
        let first_end = r.env.run(&Drive::detached(r.build.clone())).await;
        let first_run = (matches!(first_end, DriveEnd::Stalled(_)), r.builds.read_build(&r.build).await.unwrap().attempt, r.dispatch.dispatched.lock().unwrap().len());
        // The successor's drive: no idle marker in memory, the same Build and the same world.
        let drive = Drive::detached(r.build.clone());
        let mut successor = Vec::new();
        for _ in 0..4 {
            let end = r.env.run(&drive).await;
            successor.push((matches!(end, DriveEnd::Stalled(_)), r.builds.read_build(&r.build).await.unwrap().attempt, r.dispatch.dispatched.lock().unwrap().len()));
        }
        let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
        (trace_id, first_run, successor, dispatched)
    });
    let spans = cap.spans();
    let stalls: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-stalled" && s["trace_id"] == trace_id.as_str()).collect();
    println!("trace_id={trace_id}");
    assert_eq!(first_run, (true, 1, 1), "the first drive claims attempt 1, which is handed back");
    assert_eq!(successor, [(true, 2, 2); 4], "the successor claims exactly one attempt, which ends the same way; the three passes after claim nothing: {successor:?}");
    assert_eq!(dispatched, vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.1".to_string(), 2)], "two dispatches in all: one per drive, never one per pass");
    assert_eq!(stalls.len(), 2, "the wait is installed once by each drive: {stalls:?}");
}

/// CONTRACT (mesh-delete.md, "Resume after the primary's loss": the lost primary's Exited is recorded,
/// the amendment admits the new primary, "the new mesh-primary gets a fresh claim and runs only the
/// tiers without a terminal receipt"): a leave whose primary was lost records the exit and the
/// amendment as receipts of its Build and ends incomplete because the new primary is not in its role yet.
/// The receipts are what moved: the Build stays in progress and the drive claims the next attempt at
/// once, with no wait for any other change, and that attempt completes the leave.
/// Must NOT happen: the drive idling after an attempt that recorded receipts, or the Build failed.
#[test]
fn a_leave_that_recorded_the_exit_and_the_amendment_before_it_ended_incomplete_is_resumed_by_the_next_attempt_at_once() {
    use rdm_admin::build_state::{BuildFact, BuildState};
    let cap = crate::mesh_delete_resume::capture();
    let (end, state, attempt, dispatched, steps) = cap.run(async {
        let root = tracing::info_span!("cell.leave-recorded-then-incomplete");
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete("mesh.delete: mesh2.admin.3 was created for mesh2 and holds no mesh-primary role yet; the next attempt amends the roster for it".into()))).await;
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
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.began.send_replace(true);
        let _go = self.let_go.acquire().await.expect("the cell never closes its semaphore");
        self.ran.run(build, lane, attempt, op).await
    }
}

/// CONTRACT (stop_gate.rs: a task that takes the view lock begins each turn at the gate's wait
/// point, and a stop's quiesce resolves once every turn already past its wait point has ended;
/// node-stop.md steps 3-4, R-ON1): the drive is one of its admin's gated tasks. A drive whose
/// attempt is running at its executor when the admin's gate stops is a turn in progress at the
/// stop: the stop names it, the quiesce waits for it, and resolves only once the drive handled the
/// attempt's terminal and recorded the Build's end. Must NOT happen: a stop that finds no turn of a
/// drive whose attempt is running, so a stop cuts while that drive still claims, dispatches or
/// records.
#[tokio::test]
async fn a_drive_whose_attempt_is_running_when_its_admin_stops_is_a_turn_the_stops_quiesce_waits_for() {
    let (began, mut running) = tokio::sync::watch::channel(false);
    let let_go = Arc::new(tokio::sync::Semaphore::new(0));
    let held = let_go.clone();
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) };
    let r = rig_for(view(), change, move |ran, _| -> Arc<dyn OperationRunner> { Arc::new(HeldUntilLetGo { ran, began, let_go: held }) }).await;
    let gate = r.env.turns.clone();
    let drive = Drive::detached(r.build.clone());
    let (env, d) = (r.env.clone(), drive.clone());
    let driving = tokio::spawn(async move { env.run(&d).await });
    tokio::time::timeout(Duration::from_secs(5), running.wait_for(|b| *b)).await.expect("attempt 1 never reached its operation").unwrap();
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "attempt 1 is running at the executor");
    // The stop holds the gate while the attempt runs.
    let at = gate.stop();
    assert!(at.turns.contains_key("build-drive"), "the stop finds the drive's turn in progress while its attempt runs; turns at the stop: [{}]", at.named());
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
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.create:mesh1.rpc.2".to_string()]);
}

/// CONTRACT (stop_gate.rs: no turn begins while the gate is stopped; node-stop.md step 4, R-ON1: a
/// stopped node-admin calls nothing out): a drive that begins while its admin's gate is stopped (the
/// executor's last turn before a stop ensured it) waits at the gate's wait point. It claims no
/// attempt and calls no executor until the gate opens (a start), and then drives the Build to its
/// end. Must NOT happen: a stopped admin's drive claiming an attempt or calling an executor.
#[tokio::test(start_paused = true)]
async fn a_drive_that_begins_once_its_admin_stopped_claims_and_dispatches_nothing_until_the_admin_starts() {
    let r = rig().await;
    let gate = r.env.turns.clone();
    gate.stop();
    let drive = Drive::detached(r.build.clone());
    let (env, d) = (r.env.clone(), drive.clone());
    let mut driving = tokio::spawn(async move { env.run(&d).await });
    // Paused time moves on only once every task is idle: a drive that could go on has gone as far
    // as it can by then.
    let while_stopped = tokio::time::timeout(Duration::from_secs(60), &mut driving).await;
    assert!(while_stopped.is_err(), "the drive ended while its admin's gate was stopped: {while_stopped:?}");
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed while the gate was stopped");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called while the gate was stopped");
    gate.resume();
    assert_eq!(tokio::time::timeout(Duration::from_secs(60), driving).await.expect("the drive went on once the gate opened").unwrap(), DriveEnd::Terminal);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "attempt 1 is claimed and run once the gate opened");
}

/// The view after the old primary `mesh1.admin.2` committed its role row naming this admin, the
/// fabric-primary `mesh1.admin.1`, its successor for both roles: this admin's view names it the
/// primary of both, and the old primary holds none.
fn view_after_the_row() -> Topology {
    let mut t = view();
    t.nodes = vec![node("mesh1.admin.1", true, true), node("mesh1.admin.2", false, false), node("mesh1.rpc.1", false, false)];
    t
}

/// A gate over the role rows the fixture committed: each update a row records. It records every
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
    async fn target(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn primaries_updated(&self, updates: &[PrecedingUpdate], _: &BuildId, _: u32) -> Result<(), String> {
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

/// The Build removing the old primary `mesh1.admin.2` after its row, driven by this admin, its
/// successor, through `Acknowledges` over that row (`refusal` set: the acknowledgement fails).
async fn after_the_row(refusal: Option<&str>) -> (Rig, Arc<DriveEnv>, Arc<Acknowledges>, PrecedingUpdate) {
    let r = rig_for(view_after_the_row(), TopologyChange::RemoveNode { node: "mesh1.admin.2".parse().unwrap() }, |ran, _| ran).await;
    let op = "node.delete:mesh1.admin.2";
    let row = PrecedingUpdate { mesh: "mesh1".into(), operation: rdm_admin::fabric_handover::update_operation(rdm_core::PrimaryRole::MeshPrimary, r.build.as_str(), 1, op), primary: Some("mesh1.admin.2".parse().unwrap()) };
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
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    (r, env, gate, row)
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 failure
/// contract: "the next writer to read the row publishes 'updated'"; "The workflow continues only
/// after the result is installed and acknowledged: node.primary.updated answered to the resuming
/// executor's call, never a read alone"): this admin is the successor the old primary's committed
/// row names, and its view already names it the primary of both roles, so no role moves before the
/// Build's operation on the old primary. The drive still asks its gate to acknowledge the update the
/// row records before it dispatches that operation: the acknowledgement is asked with no dispatch
/// made, and the operation is dispatched once after it. Must NOT happen: the operation on the old
/// primary dispatched on the view alone, before the update the row records is acknowledged.
#[tokio::test]
async fn a_successor_whose_view_already_names_it_primary_acknowledges_the_rows_update_before_it_dispatches_on_the_old_primary() {
    let (r, env, gate, row) = after_the_row(None).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(env.run(&drive).await, DriveEnd::Terminal);
    let asked = gate.asked.lock().unwrap().clone();
    assert_eq!(asked.first(), Some(&(row.clone(), 0)), "the row's update is acknowledged before any dispatch: asked {asked:?}");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1)], "the operation on the old primary is dispatched once, to this admin, its mesh's primary");
    assert_eq!(r.ran.0.lock().unwrap().len(), 1, "it ran once: {:?}", r.ran.0.lock().unwrap());
}

/// CONTRACT (as above; C1 step 8: "Only then does the commanding Build continue"): an update the row
/// records that this admin cannot acknowledge (the successor's call fails, the row cannot be read)
/// leaves the Build where it is: the drive stalls naming why on its span, frames nothing to the caller
/// (node-rpc-envelope.md: Blocked's only source is the subject's Status `Blocked`), and dispatches nothing.
/// Must NOT happen: the operation on the old primary dispatched past an update not acknowledged.
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
    let waiting = frames_now(&mut reader).await;
    assert_eq!(blocked_of(&waiting), Vec::<BuildReply>::new(), "the update not acknowledged frames no Blocked to the caller: the stall names it on its span");
}

/// The view before the primary's row: the fabric-primary `mesh1.admin.1` holds both roles, and
/// `mesh1.admin.2` is the ready admin of its mesh that the update names its successor.
fn view_primary_of_both() -> Topology {
    let mut t = view();
    t.nodes = vec![node("mesh1.admin.1", true, true), node("mesh1.admin.2", false, false), node("mesh1.rpc.1", false, false)];
    t
}

/// The primary's own node.primary.update, run from its drive (`RoleGate::update_primaries` for its own
/// roles): at the row the shared view names the successor the primary of both roles and the primary's
/// Build log decides nothing more, naming it (`row` resolves: the successor installs and its drive may begin);
/// then the update waits at steps 6-7 until the cell lets it go, and says "primary updated" (step 7).
struct PrimaryUpdate {
    inner: Arc<Switch>,
    topology: Arc<RwLock<Topology>>,
    row: Arc<tokio::sync::Notify>,
    let_go: Arc<tokio::sync::Semaphore>,
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl Gate for PrimaryUpdate {
    async fn target(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn update_primaries(&self, _: &rdm_admin::executor::RoleMove, _: &BuildId, attempt: u32, _: u32, _: &str) -> Result<(), rdm_admin::build_drive::RoleMoveEnd> {
        for n in self.topology.write().await.nodes.iter_mut() {
            let successor = n.name.to_string() == "mesh1.admin.2";
            (n.is_primary, n.is_fabric_primary) = (successor, successor);
        }
        self.inner.target_to(Some("mesh1.admin.2"));
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
/// re-claimed"): the fabric-primary `mesh1.admin.1` holds both roles and its Build deletes it, so its
/// drive moves both roles itself first. At its row the successor `mesh1.admin.2` holds the roles and
/// its own drive of the same Build begins, while the primary is still at steps 6-7. The update is a
/// step of the primary's attempt: the successor's drive reattaches to that attempt, claims nothing and
/// dispatches nothing until it ends, and only after the primary said "primary updated" does the attempt
/// end `HandedOff` to the successor, which then claims the next attempt and runs the delete.
/// Must NOT happen: the successor claiming an attempt, dispatching, or the delete running while the
/// primary (alive) has not reached step 7.
#[tokio::test(start_paused = true)]
async fn a_successor_claims_and_dispatches_nothing_while_the_primarys_update_has_not_said_primary_updated() {
    let r = rig_for(view_primary_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let (row, let_go, events) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Semaphore::new(0)), Arc::new(Mutex::new(Vec::new())));
    let primary_gate = Arc::new(PrimaryUpdate { inner: r.switch.clone(), topology: r.env.topology.clone(), row: row.clone(), let_go: let_go.clone(), events: events.clone() });
    let e = &r.env;
    let primary = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: primary_gate,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    // The successor: its own door, gate and stop gate over the fixture's one Build log, view and store
    // root (its mesh rows, `mesh_claims`, are the primary's).
    let n_name: PathName = "mesh1.admin.2".parse().unwrap();
    let n_id = r.env.topology.read().await.node(&n_name).unwrap().node_id.clone();
    let n_switch = Arc::new(Switch { targeted: AtomicBool::new(false), successor: Mutex::new(None), target_tx: tokio::sync::watch::channel(false).0, recorded_primary: Mutex::new(None), peer: Mutex::new(Default::default()), dead: Mutex::new(Default::default()), view: None });
    let successor = Arc::new(DriveEnv {
        me: n_name.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: Arc::new(ClaimDoor {
            node_id: n_id.clone(),
            me: n_name.clone(),
            topology: e.topology.clone(),
            builds: r.builds.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            claims: e.door.claims.clone(),
            claimed: Default::default(),
        }),
        dispatcher: e.dispatcher.clone(),
        gate: n_switch,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: Default::default(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    let (p_drive, n_drive) = (Drive::detached(r.build.clone()), Drive::detached(r.build.clone()));
    let p_driving = tokio::spawn({
        let (env, d) = (primary.clone(), p_drive.clone());
        async move { env.run(&d).await }
    });
    tokio::time::timeout(Duration::from_secs(5), row.notified()).await.expect("the primary's update never wrote its row");
    let mut n_driving = tokio::spawn({
        let (env, d) = (successor.clone(), n_drive.clone());
        async move { env.run(&d).await }
    });
    // Paused time moves on only once every task is idle: the successor's drive has gone as far as it
    // can while the primary is at steps 6-7.
    let early = tokio::time::timeout(Duration::from_secs(60), &mut n_driving).await;
    let to_successor: Vec<(String, u32)> = r.dispatch.dispatched.lock().unwrap().iter().filter(|(to, _)| *to == n_name.to_string()).cloned().collect();
    assert!(r.ran.0.lock().unwrap().is_empty() && to_successor.is_empty() && early.is_err(), "while the holder had not said primary updated, the successor's drive dispatched {to_successor:?}, ran {:?}, and ended {early:?}; events {:?}", r.ran.0.lock().unwrap(), events.lock().unwrap());
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.1")), "the update runs in the holder's own attempt, and no other is claimed");
    let_go.add_permits(1);
    assert_eq!(tokio::time::timeout(Duration::from_secs(60), n_driving).await.expect("the successor's drive went on once the holder's attempt ended").unwrap(), DriveEnd::Terminal);
    assert!(matches!(tokio::time::timeout(Duration::from_secs(60), p_driving).await.expect("the holder's drive ended").unwrap(), DriveEnd::RoleLost(_)), "the holder's drive ends at its row");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(p.attempt_executors.get(&1).map(String::as_str), Some("mesh1.admin.1"), "attempt 1 is the holder's: {:?}", p.attempt_executors);
    assert_eq!(p.attempt_executors.get(&2).map(String::as_str), Some("mesh1.admin.2"), "the successor claimed the next attempt after the holder's ended: {:?}", p.attempt_executors);
    let handed: Vec<_> = r.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
        rdm_admin::build_state::BuildFact::Attempt(a) if a.build_id == r.build && a.attempt == 1 => Some(a.outcome),
        _ => None,
    }).collect();
    assert_eq!(handed, vec![rdm_admin::build_state::AttemptOutcome::HandedOff { to: "mesh1.admin.2".into() }], "the holder's attempt ended handed off to the successor");
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.delete:mesh1.admin.1".to_string()], "the delete ran once, at the successor");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 1), ("mesh1.admin.2".to_string(), 2)], "the successor reattached to the holder's attempt, then dispatched the next to itself");
    assert_eq!(*events.lock().unwrap(), vec!["row attempt 1".to_string(), "primary updated".into()]);
}

/// The primary's own node.primary.update refused before its row, as the door refuses it with no
/// successor (`fabric_handover::update_primary` step 2: nothing said, cut off or written): the view is
/// left as it is and the refusal is named as the admin's gate names a refused reply.
struct RefusedUpdate {
    inner: Arc<Switch>,
    calls: std::sync::atomic::AtomicUsize,
    gone_then_move: Option<(Arc<RwLock<Topology>>, Arc<rdm_admin::build_state::MemoryBuildStateAdapter>, &'static str, Arc<Mutex<Vec<(u32, u32, Option<String>)>>>)>,
}

/// The Q7 text the refused update names.
const NO_SECONDARY: &str = "cannot transfer: no secondary of type node-admin in mesh mesh1";

#[async_trait::async_trait]
impl Gate for RefusedUpdate {
    async fn target(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn update_primaries(&self, m: &rdm_admin::executor::RoleMove, build: &BuildId, attempt: u32, run: u32, _: &str) -> Result<(), rdm_admin::build_drive::RoleMoveEnd> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some((topology, builds, successor, calls)) = &self.gone_then_move {
            let claimed = builds.read_build(build).await.ok().and_then(|p| p.attempt_executors.get(&attempt).cloned());
            let first = {
                let mut recorded = calls.lock().unwrap();
                recorded.push((attempt, run, claimed));
                recorded.len() == 1
            };
            if first {
                return Err(rdm_admin::build_drive::RoleMoveEnd::Failed { step: "primary-update".into(), reason: GONE_REASON.into() , code: rdm_core::outcome::Code::FailedPrecondition });
            }
            for n in topology.write().await.nodes.iter_mut() {
                if n.mesh == m.primary.mesh {
                    let is_succ = n.name.to_string() == *successor;
                    if m.mesh_role {
                        n.is_primary = is_succ && n.name.kind == NodeKind::NodeAdmin;
                    }
                }
                if m.fabric_role {
                    n.is_fabric_primary = n.name.to_string() == *successor;
                }
            }
            if m.fabric_role {
                self.inner.target_to(Some(successor));
            }
            return Ok(());
        }
        Err(rdm_admin::build_drive::RoleMoveEnd::Refused(format!("rejected-no-successor: RejectedNoSuccessor {{ reason: {NO_SECONDARY:?} }}")))
    }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 failure
/// contract: "Refused before the row (step 2, or the writer's own pre-check of the call's revision):
/// refused by name, nothing changed, the Build's step fails by name. Nothing was cut off, so nothing
/// is restored"; R-BI4: "Only an attempt that ended with a named terminal failure (Failed { step,
/// reason }) makes the Build failed"): the fabric-primary `mesh1.admin.1` holds both roles and its
/// Build deletes it, so it claims attempt 1 for itself and runs its own node.primary.update first,
/// which is refused before the row with Q7's text. Attempt 1 ends `Failed`, its reason naming the
/// refusal, and the drive ends at that terminal: nothing is dispatched or run and the view still names
/// `mesh1.admin.1` the primary of both roles. No cut-off and no row is the refusing gate's own record
/// here (it wrote nothing); the door's refusal writing neither is
/// `fabric_handover::a_transfer_with_no_successor_is_refused_with_q7s_exact_texts_and_nothing_moves`.
/// Must NOT happen: the attempt handed back to the primary and the drive idle on a refusal (an
/// incomplete attempt's arm), the update asked again, or the delete dispatched.
#[tokio::test(start_paused = true)]
async fn a_primarys_own_update_refused_before_the_row_fails_its_attempt_by_name_and_nothing_moves() {
    let r = rig_for(view_primary_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let gate = Arc::new(RefusedUpdate { inner: r.switch.clone(), calls: Default::default(), gone_then_move: None });
    let e = &r.env;
    let primary = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: gate.clone(),
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), primary.run(&drive)).await.expect("the drive ends");
    let p = r.builds.read_build(&r.build).await.unwrap();
    let verdict: Vec<_> = r.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
        rdm_admin::build_state::BuildFact::Attempt(a) if a.build_id == r.build && a.attempt == 1 => Some(a.outcome),
        _ => None,
    }).collect();
    assert_eq!(end, DriveEnd::Terminal, "the refusal ends the drive at a terminal: verdicts {verdict:?}");
    assert!(matches!(verdict.as_slice(), [rdm_admin::build_state::AttemptOutcome::Failed { reason , .. }] if reason.contains(NO_SECONDARY)), "attempt 1 failed naming the refusal: {verdict:?}");
    assert_eq!((p.attempt, p.attempt_executors.get(&1).map(String::as_str)), (1, Some("mesh1.admin.1")), "the holder claimed attempt 1 for itself and no other attempt was claimed");
    match terminal_of(&drive).await {
        BuildReply::Failed { attempt, reason, .. } => assert!(attempt == 1 && reason.contains(NO_SECONDARY), "{reason}"),
        other => panic!("the drive's terminal is the failed step: {other:?}"),
    }
    assert_eq!(gate.calls.load(std::sync::atomic::Ordering::SeqCst), 1, "the update was asked once");
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing ran or was dispatched: ran {:?}, dispatched {:?}", r.ran.0.lock().unwrap(), r.dispatch.dispatched.lock().unwrap());
    let primary_node = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").map(|n| (n.is_primary, n.is_fabric_primary));
    assert_eq!(primary_node, Some((true, true)), "the view still names the primary of both roles");
}

/// CONTRACT (build.md: "The next attempt is claimed after the running attempt's terminal receipt
/// (Complete, Failed or HandedOff)"; R-BI7 caveat 3, mesh_claims.rs: a mesh's in-flight row "is
/// released at that attempt's verdict", by "the admin that durably recorded the verdict"): the
/// fabric-primary `mesh1.admin.1` holds both roles and its Build deletes it, so it moves both roles in
/// the attempt it claims for itself, on mesh1's rows of the fabric's one store root, and that attempt
/// ends `HandedOff` to the successor `mesh1.admin.2`. The successor's drive begins only after the
/// attempt ended, so it reads the verdict and reattaches to nothing; it claims the next attempt on the
/// same mesh1 rows and runs the delete.
/// Must NOT happen: the successor's claim refused build-in-progress by the primary's ended attempt, its
/// drive stalled on a mesh no attempt in flight holds.
#[tokio::test(start_paused = true)]
async fn a_successor_that_reads_the_primarys_attempt_ended_claims_the_next_attempt_on_the_mesh_the_primary_released() {
    let r = rig_for(view_primary_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let (row, events) = (Arc::new(tokio::sync::Notify::new()), Arc::new(Mutex::new(Vec::new())));
    // The primary says primary updated as soon as its row is written: nothing holds it at steps 6-7.
    let primary_gate = Arc::new(PrimaryUpdate { inner: r.switch.clone(), topology: r.env.topology.clone(), row, let_go: Arc::new(tokio::sync::Semaphore::new(1)), events: events.clone() });
    let e = &r.env;
    let primary = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: primary_gate,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let p_drive = Drive::detached(r.build.clone());
    assert!(matches!(tokio::time::timeout(Duration::from_secs(60), primary.run(&p_drive)).await.expect("the holder's drive ended"), DriveEnd::RoleLost(_)), "the holder's drive ends at its row");
    let p = r.builds.read_build(&r.build).await.unwrap();
    let handed: Vec<_> = r.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
        rdm_admin::build_state::BuildFact::Attempt(a) if a.build_id == r.build && a.attempt == 1 => Some(a.outcome),
        _ => None,
    }).collect();
    assert_eq!((p.attempt, handed), (1, vec![rdm_admin::build_state::AttemptOutcome::HandedOff { to: "mesh1.admin.2".into() }]), "the holder's attempt ended handed off to the successor before the successor's drive begins");
    let n_name: PathName = "mesh1.admin.2".parse().unwrap();
    let n_id = r.env.topology.read().await.node(&n_name).unwrap().node_id.clone();
    let n_switch = Arc::new(Switch { targeted: AtomicBool::new(false), successor: Mutex::new(None), target_tx: tokio::sync::watch::channel(false).0, recorded_primary: Mutex::new(None), peer: Mutex::new(Default::default()), dead: Mutex::new(Default::default()), view: None });
    // The successor: its own door, gate and stop gate over the fixture's one Build log, view and store
    // root (its mesh rows, `mesh_claims`, are the primary's).
    let successor = Arc::new(DriveEnv {
        me: n_name.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: Arc::new(ClaimDoor {
            node_id: n_id.clone(),
            me: n_name.clone(),
            topology: e.topology.clone(),
            builds: r.builds.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            claims: e.door.claims.clone(),
            claimed: Default::default(),
        }),
        dispatcher: e.dispatcher.clone(),
        gate: n_switch,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: Default::default(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    let n_drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), successor.run(&n_drive)).await.expect("the successor's drive ends");
    assert_eq!(end, DriveEnd::Terminal, "the successor claimed the next attempt on mesh1 and ran the delete");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt_executors.get(&1).map(String::as_str), p.attempt_executors.get(&2).map(String::as_str)), (Some("mesh1.admin.1"), Some("mesh1.admin.2")), "the successor claimed the attempt after the holder's: {:?}", p.attempt_executors);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 2)], "the successor reattached to nothing and dispatched the next attempt to itself");
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.delete:mesh1.admin.1".to_string()], "the delete ran once, at the successor");
    assert_eq!(*events.lock().unwrap(), vec!["row attempt 1".to_string(), "primary updated".into()]);
}

/// The successor's gate in a primary-lost hand-over: each acknowledgement the drive asks for the rows
/// the fixture committed, with whether the primary's departure was proven by then and how many
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
    async fn target(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn primaries_updated(&self, updates: &[PrecedingUpdate], _: &BuildId, _: u32) -> Result<(), String> {
        let proven = self.proofs.recorded.lock().unwrap().contains("mesh1.admin.1");
        let dispatched = self.dispatch.dispatched.lock().unwrap().len();
        for u in updates.iter().filter(|u| self.rows.contains(u)) {
            self.asked.lock().unwrap().push((u.clone(), proven, dispatched));
        }
        Ok(())
    }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 failure
/// contract: "The primary lost after the row, before 'updated': the successor and every reader take the
/// role from the row; the next writer to read the row publishes 'updated'"; "A call that reached a
/// process other than the named birth, a refusal or no answer is never an exited verdict"; build.md: the
/// next attempt is claimed "when the executor is lost with the attempt unfinished, after exact
/// departure proof"): the fabric-primary `mesh1.admin.1` holds both roles and moves them in its own
/// attempt; it dies after its row, before it says "primary updated". Its successor `mesh1.admin.2`,
/// holding the roles from the row, finds that attempt held by the primary and its call to it reaches
/// nothing: only the primary's proven departure lets it claim the next attempt, and only then does it
/// acknowledge the row's update (the acknowledgement in which the successor, the next writer to read the
/// row, says "primary updated" once the primary's exit is proven: fabric_handover.rs
/// `a_successor_that_answered_the_primaries_call_says_primary_updated_only_once_the_primary_is_proven_exited`),
/// before it dispatches the delete.
/// Must NOT happen: the successor claiming the primary's attempt over it on silence, the acknowledgement
/// before the departure proof, or the delete dispatched before the acknowledgement.
#[tokio::test(start_paused = true)]
async fn a_fabric_primary_lost_after_its_row_has_exited_before_its_successor_acknowledges_the_row_and_dispatches() {
    let r = rig_for(view_primary_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let (row, let_go, events) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Semaphore::new(0)), Arc::new(Mutex::new(Vec::new())));
    let primary_gate = Arc::new(PrimaryUpdate { inner: r.switch.clone(), topology: r.env.topology.clone(), row: row.clone(), let_go, events: events.clone() });
    let e = &r.env;
    let primary = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: primary_gate,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let p_drive = Drive::detached(r.build.clone());
    let p_driving = tokio::spawn({
        let (env, d) = (primary.clone(), p_drive.clone());
        async move { env.run(&d).await }
    });
    tokio::time::timeout(Duration::from_secs(5), row.notified()).await.expect("the primary's update never wrote its row");
    // The primary dies after its row, before step 7: its process (the drive and the run it held) is gone,
    // nothing answers at its address, and the provider proves its exact runtime exited.
    p_driving.abort();
    let _ = p_driving.await;
    r.dispatch.lose("mesh1.admin.1");
    r.proofs.exited.lock().unwrap().insert("mesh1.admin.1".into());
    let n_name: PathName = "mesh1.admin.2".parse().unwrap();
    let n_id = r.env.topology.read().await.node(&n_name).unwrap().node_id.clone();
    let n_switch = Arc::new(Switch { targeted: AtomicBool::new(false), successor: Mutex::new(None), target_tx: tokio::sync::watch::channel(false).0, recorded_primary: Mutex::new(None), peer: Mutex::new(Default::default()), dead: Mutex::new(Default::default()), view: None });
    let op = "node.delete:mesh1.admin.1";
    let update = PrecedingUpdate { mesh: "mesh1".into(), operation: rdm_admin::fabric_handover::update_operation(rdm_core::PrimaryRole::MeshPrimary, r.build.as_str(), 1, op), primary: Some("mesh1.admin.1".parse().unwrap()) };
    let n_gate = Arc::new(AfterTheProof { inner: n_switch, rows: vec![update.clone()], proofs: r.proofs.clone(), dispatch: r.dispatch.clone(), asked: Mutex::new(Vec::new()) });
    let successor = Arc::new(DriveEnv {
        me: n_name.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: Arc::new(ClaimDoor {
            node_id: n_id.clone(),
            me: n_name.clone(),
            topology: e.topology.clone(),
            builds: r.builds.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            claims: e.door.claims.clone(),
            claimed: Default::default(),
        }),
        dispatcher: e.dispatcher.clone(),
        gate: n_gate.clone(),
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: Default::default(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    let n_drive = Drive::detached(r.build.clone());
    assert_eq!(tokio::time::timeout(Duration::from_secs(60), successor.run(&n_drive)).await.expect("the successor's drive ends"), DriveEnd::Terminal);
    assert_eq!(r.proofs.calls.load(Ordering::SeqCst), 1, "the primary's departure was proven once");
    assert_eq!(*n_gate.asked.lock().unwrap(), vec![(update, true, 0)], "the row's update was acknowledged once, after the departure proof and before any dispatch");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt_executors.get(&1).map(String::as_str), p.attempt_executors.get(&2).map(String::as_str)), (Some("mesh1.admin.1"), Some("mesh1.admin.2")), "the successor claimed the attempt after the holder's: {:?}", p.attempt_executors);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 2)], "nothing reached the lost primary; the delete was dispatched once, to the successor");
    assert_eq!(*r.ran.0.lock().unwrap(), vec![op.to_string()], "the delete ran once, at the successor");
    assert_eq!(*events.lock().unwrap(), vec!["row attempt 1".to_string()], "the holder never said primary updated");
}

const GONE_REASON: &str = "the new primary's process is gone, provider-confirmed, after the committed row: step primary-update; new primary mesh1 000000000003; moved mesh-primary (mesh1) revision 4; ordinary rule primary mesh-primary (mesh1): mesh1.admin.1; provider proof: the provider proved its exact birth exited with code 0";

/// A node.primary.update whose new primary's process is gone, provider-confirmed, after the row
/// (ruling R-PU2, Q10), then, in the next run, a transfer to `successor`: the first call ends
/// `RoleMoveEnd::Failed` with the view unchanged (the ordinary rule names the primary again), the next
/// moves the requested roles to `successor`. Records `(attempt, run, claimed_before)` for each call.
fn gone_after_row_then_moved(
    inner: Arc<Switch>,
    topology: Arc<RwLock<Topology>>,
    builds: Arc<rdm_admin::build_state::MemoryBuildStateAdapter>,
    successor: &'static str,
) -> (Arc<RefusedUpdate>, Arc<Mutex<Vec<(u32, u32, Option<String>)>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(RefusedUpdate { inner, calls: Default::default(), gone_then_move: Some((topology, builds, successor, calls.clone())) });
    (gate, calls)
}

/// CONTRACT (ruling R-PU2, Q8/Q9/Q10 + Luke 2026-10-13: when the new primary's process is gone,
/// provider-confirmed, after the committed row, the transfer step fails with `Failed { step, reason }`,
/// the Build is Failed under R-BI4; recovery order (4) "The executor durably records Failed { step,
/// reason } and ends the workflow; it never proceeds to P's stop. The terminal receipt goes through the
/// Build journal"; (5) "The caller's resubmit opens the next attempt of the same Build ... under its
/// own key, (build_id, run), never the Build id alone"): the fabric-primary `mesh1.admin.1` holds both
/// roles and its Build removes it. Its own update in attempt 1 (run 1) ends with the new primary's
/// process gone after the row and the ordinary rule names it again: attempt 1 ends `Failed` at step
/// primary-update, the drive ends at that terminal, and nothing is dispatched or run. The caller's
/// resubmit (a fresh drive of the failed Build) claims attempt 2, which opens run 2: its update is a
/// new transfer (run 2), which moves the roles to `mesh1.admin.3`, and the drive goes on from the
/// moved role. Must NOT happen: the attempt handed back to the primary and the drive idle on it, the
/// remove dispatched in attempt 1, or attempt 2's update keyed by run 1 (it would replay run 1's row).
#[tokio::test(start_paused = true)]
async fn a_primarys_own_update_whose_successors_process_is_gone_after_the_row_fails_and_the_resubmit_runs_a_new_transfer() {
    let mut t = view();
    t.nodes = vec![node("mesh1.admin.1", true, true), node("mesh1.admin.2", false, false), node("mesh1.admin.3", false, false), node("mesh1.rpc.1", false, false)];
    let r = rig_for(t, TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let (gate, calls) = gone_after_row_then_moved(r.switch.clone(), r.env.topology.clone(), r.builds.clone(), "mesh1.admin.3");
    let e = &r.env;
    let primary = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate,
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), primary.run(&drive)).await.expect("the drive ends");
    let verdicts = |attempt: u32| {
        let builds = r.builds.clone();
        let build = r.build.clone();
        async move {
            builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
                rdm_admin::build_state::BuildFact::Attempt(a) if a.build_id == build && a.attempt == attempt => Some(a.outcome),
                _ => None,
            }).collect::<Vec<_>>()
        }
    };
    let first = verdicts(1).await;
    assert_eq!(end, DriveEnd::Terminal, "the failed step ends the drive at a terminal: verdicts {first:?}");
    assert!(matches!(first.as_slice(), [rdm_admin::build_state::AttemptOutcome::Failed { reason , .. }] if reason.starts_with("the new primary's process is gone, provider-confirmed, after the committed row")), "attempt 1 failed with the plain-English reason: {first:?}");
    match terminal_of(&drive).await {
        BuildReply::Failed { attempt, step, reason, .. } => assert!(attempt == 1 && step == "primary-update" && reason.starts_with("the new primary's process is gone, provider-confirmed, after the committed row"), "{step}: {reason}"),
        other => panic!("the drive's terminal is the failed step: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "the remove is never reached: ran {:?}, dispatched {:?}", r.ran.0.lock().unwrap(), r.dispatch.dispatched.lock().unwrap());
    // The caller's resubmit: a drive of the failed Build claims its next attempt.
    let resubmit = Drive::detached(r.build.clone());
    let _ = tokio::time::timeout(Duration::from_secs(60), primary.run(&resubmit)).await.expect("the resubmitted drive ends");
    assert_eq!(*calls.lock().unwrap(), vec![(1, 1, Some("mesh1.admin.1".into())), (2, 2, Some("mesh1.admin.1".into()))], "attempt 2 opens run 2: its update is a new transfer key");
    let second = verdicts(2).await;
    assert!(matches!(second.as_slice(), [rdm_admin::build_state::AttemptOutcome::HandedOff { to }] if to == "mesh1.admin.3"), "attempt 2 moved the roles and handed the Build to mesh1.admin.3: {second:?}");
}

/// CONTRACT (Q10 commanded arm: "When a commanded node.primary.update ends, the primary answers the
/// fabric-primary with the Build carrier's own terminal, `Failed { step, reason }` (R-BI4; the
/// envelope's only durable step outcomes are Complete and Failed). 0x21 gains that variant;
/// `Incomplete` is not used for a step that ended. The FP fails the step on the variant, never on the
/// reason's text ... The FP claims the attempt at the claim door BEFORE it commands"): the
/// fabric-primary `mesh1.admin.1` drives a Build that removes `mesh2.admin.1`, mesh2's primary. There
/// is no separate arm: the fabric-primary claims attempt 1 at the claim door before it commands
/// `mesh2.admin.1`'s role move. In run 1, the commanded move answers `Failed { step, reason }`: attempt
/// 1 ends `Failed`, the Build is `Failed`, and nothing is dispatched to mesh2. On the caller's
/// resubmit, the fabric-primary claims attempt 2 (opening run 2) before commanding `mesh2.admin.1`'s
/// role move, which moves mesh2's role to `mesh2.admin.3`, hands off attempt 2, and claims attempt 3
/// for `mesh2.admin.3` to run the remove.
#[tokio::test(start_paused = true)]
async fn a_commanded_update_claims_the_attempt_first_and_when_it_ends_failed_fails_the_build_and_the_resubmit_opens_the_next_run() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
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
        notices: e.notices.clone(),
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
    // The caller's resubmit opens run 2: attempt 2 is claimed before commanding, moves mesh2's role to
    // mesh2.admin.3, and attempt 3 dispatches the remove to mesh2.admin.3.
    let resubmit = Drive::detached(r.build.clone());
    let end2 = tokio::time::timeout(Duration::from_secs(60), commander.run(&resubmit)).await.expect("the resubmitted drive ends");
    assert_eq!(end2, DriveEnd::Terminal, "the resubmitted Build completes");
    assert_eq!(*calls.lock().unwrap(), vec![(1, 1, Some("mesh1.admin.1".into())), (2, 2, Some("mesh1.admin.1".into()))], "attempt 2 opened run 2 and was claimed before commanding");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh2.admin.3".to_string(), 3)], "attempt 3 dispatched the remove to the new mesh2 primary");
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["node.delete:mesh2.admin.1".to_string()]);
}

/// CONTRACT (R12 / i143 e12.s47 #64): a node-admin born as `mesh1.admin.9` and renamed in place by
/// `node.name.update` to `mesh1.admin.1` acts and spans under the name it holds now, never the boot
/// `PathName` its structs were constructed with. Its `ControlPlane` submits, its
/// `ClaimDoor` claims, its `DriveEnv` drives, its `RpcDispatcher` dispatches in-process to its own
/// `RunDoor` and `BuildExecutor`, its `NodeRows` writes rows, and every control-plane, Build and
/// node-row span names `mesh1.admin.1` and never `mesh1.admin.9`.
#[test]
fn a_renamed_admin_claims_drives_dispatches_and_spans_under_its_held_name_never_its_boot_name() {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("renamed-admin-held-name", exporter.clone());
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
        .build()
        .unwrap();
    let _g = tracing::dispatcher::set_default(&dispatch);

    rt.block_on(async {
        let boot: PathName = "mesh1.admin.9".parse().unwrap();
        let held: PathName = "mesh1.admin.1".parse().unwrap();
        let t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![node("mesh1.admin.1", true, true), node("mesh1.rpc.1", false, false)],
        };
        let fp_id = t.node(&held).expect("the renamed admin is in the view at its held name").node_id.clone();
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
        let topology = Arc::new(RwLock::new(t.clone()));
        let contexts = Arc::new(AttemptContexts::in_memory());
        let claims = Arc::new(rdm_admin::storage::MemoryMeshStorage::default());
        let mut cp = ControlPlane::new(builds.clone(), accepted, boot.clone(), t, crate::adopted_time()).with_contexts(contexts.clone()).with_claims(claims.clone());
        cp.topology = topology.clone();

        let change = TopologyChange::ReconcileMesh {
            desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 2)]),
        };
        let change = crate::named_request(&cp, change).await;
        let opened = cp.submit("test", change).await.expect("the renamed fabric-primary admits the Build");

        let nodes_storage = Arc::new(rdm_admin::storage::MemoryNodesStorage::default());
        let rows = rdm_admin::node_rows::NodeRows::new(boot.clone(), fp_id.clone(), topology.clone(), nodes_storage);
        rows.claim(
            &"mesh1.rpc.2".parse().unwrap(),
            &NodeId::mint(),
            "node.create:mesh1.rpc.2",
            None,
            None,
            rdm_admin::node_rows::ClaimFor::Create,
        )
        .await
        .expect("the renamed mesh-primary claims the node row");

        let ran = Arc::new(Creates(Mutex::new(Vec::new()), builds.clone()));
        let exec = Arc::new(rdm_admin::executor::BuildExecutor {
            executor: boot.to_string(),
            builds: builds.clone(),
            topology: topology.clone(),
            runner: ran.clone(),
        });
        let run_door = Arc::new(rdm_admin::build_run::RunDoor {
            me: boot.clone(),
            builds: builds.clone(),
            exec,
            runs: Arc::new(rdm_admin::build_run::AttemptRuns::default()),
            local: builds.clone(),
            gate: rdm_admin::stop_gate::StopGate::new(),
        });
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(rdm_transport::NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
        let rpc_dispatcher = Arc::new(rdm_admin::build_op::RpcDispatcher {
            me: boot.clone(),
            local: run_door,
            client,
            topology: topology.clone(),
        });
        let switch = Arc::new(Switch {
            targeted: AtomicBool::new(false),
            successor: Mutex::new(None),
            target_tx: tokio::sync::watch::channel(false).0,
            recorded_primary: Mutex::new(None),
            peer: Mutex::new(Default::default()),
            dead: Mutex::new(Default::default()),
            view: None,
        });
        let env = Arc::new(DriveEnv {
            me: boot.clone(),
            topology: topology.clone(),
            builds: builds.clone(),
            door: Arc::new(ClaimDoor {
                node_id: fp_id,
                me: boot.clone(),
                topology: topology.clone(),
                builds: builds.clone(),
                contexts,
                claims,
                claimed: Default::default(),
            }),
            dispatcher: rpc_dispatcher,
            gate: switch,
            departure: Arc::new(crate::loopback::Proofs::default()),
            verdicts: Arc::new(crate::loopback::NoVerdicts),
            notices: Default::default(),
            turns: rdm_admin::stop_gate::StopGate::new(),
        });

        let drive = Drive::detached(opened.build_id.clone());
        let end = env.run(&drive).await;
        assert_eq!(end, DriveEnd::Terminal, "the renamed admin drives and dispatches in-process under its held name");
        assert_eq!(*ran.0.lock().unwrap(), vec!["node.create:mesh1.rpc.2".to_string()]);
        let projected = builds.read_build(&opened.build_id).await.unwrap();
        assert_eq!(projected.state, rdm_admin::build_state::BuildState::Complete);
        assert_eq!(projected.executor.as_deref(), Some("mesh1.admin.1"), "the attempt was claimed for the held name");
    });

    capture.flush(Duration::from_secs(10)).expect("spans flush");
    let spans = exporter.get_finished_spans().unwrap();
    crate::assert_own_story(spans.iter().map(|s| s.name.as_ref()));
    for s in &spans {
        for kv in &s.attributes {
            let val = kv.value.to_string();
            assert!(
                !val.contains("mesh1.admin.9"),
                "span {} attribute {}={} still carries the boot name mesh1.admin.9",
                s.name,
                kv.key,
                val
            );
        }
    }
    let attr = |name: &str, key: &str| -> Vec<String> {
        spans
            .iter()
            .filter(|s| s.name == name)
            .filter_map(|s| s.attributes.iter().find(|kv| kv.key.as_str() == key).map(|kv| kv.value.to_string()))
            .collect()
    };
    assert_eq!(attr("rdm.node_admin.build.create.via-accepted", "node"), vec!["mesh1.admin.1", "mesh1.admin.1"]);
    assert_eq!(attr("rdm.node_admin.node.create.via-node-row", "node"), vec!["mesh1.admin.1"]);
    assert_eq!(attr("rdm.node_admin.build.update.via-claim-decision", "node"), vec!["mesh1.admin.1"]);
    assert_eq!(attr("rdm.node_admin.build.update.via-claim-decision", "executor"), vec!["mesh1.admin.1"]);
    assert_eq!(attr("rdm.node_admin.build.update.via-drive", "node"), vec!["mesh1.admin.1"]);
    assert_eq!(attr("rdm.node_admin.build.update.via-dispatch", "node"), vec!["mesh1.admin.1"]);
    assert_eq!(attr("rdm.node_admin.build.update.via-dispatch", "executor"), vec!["mesh1.admin.1"]);
    assert_eq!(attr("rdm.node_admin.build.serve.via-attempt-run", "node"), vec!["mesh1.admin.1"]);
    assert_eq!(attr("rdm.node_admin.build.update.via-reconcile", "executor"), vec!["mesh1.admin.1"]);
}

/// CONTRACT (R-BI3: only a caller's resubmit drives a failed Build on): a Build that completed once
/// stays the known state when a later attempt of it fails (`CurrentBuild::Completed`), and the
/// reconcile loop's drive of it claims no next attempt until a caller resubmits.
/// Must NOT happen: the reconcile loop claiming the attempt after the failed one by itself (gate
/// e6s7-b7d25a17, node_restart: attempt 2 of a restart failed, and 52 ms later the drive claimed
/// attempt 3 with no resubmit).
#[test]
fn a_build_whose_later_attempt_failed_claims_no_next_attempt_without_a_resubmit() {
    use rdm_admin::build_state::{AttemptOpened, AttemptOutcome, AttemptReason, BuildAttemptClaim, BuildAttemptReceipt, CurrentBuild};
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("failed-attempt-no-next", exporter.clone());
    let _g = tracing::dispatcher::set_default(&tracing::Dispatch::new(tracing_subscriber::registry().with(layer)));
    let drive_outcomes = || -> Vec<String> {
        capture.flush(Duration::from_secs(10)).expect("spans flush");
        exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == "rdm.node_admin.build.update.via-drive")
            .map(|s| s.attributes.iter().find(|kv| kv.key.as_str() == "outcome").map(|kv| kv.value.to_string()).unwrap_or_default())
            .collect()
    };
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let r = rig().await;
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal, "attempt 1 completes the Build");
        // A later attempt of it (a restart) is opened, claimed and fails at its executor.
        r.builds.open_attempt(&AttemptOpened { mesh: None, build_id: r.build.clone(), attempt: 2, reason: AttemptReason::Stop, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 5, context: None }).await.unwrap();
        r.builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: r.build.clone(), attempt: 2, executor: "mesh1.admin.2".into() }).await.unwrap();
        let failed = AttemptOutcome::Failed { reason: "the restart's stop was not admitted".into(), code: rdm_core::outcome::Code::FailedPrecondition };
        r.builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: r.build.clone(), attempt: 2, outcome: failed }).await.unwrap();
        assert_eq!(r.builds.current_build().await.unwrap(), CurrentBuild::Completed(r.build.clone()), "the Build that completed once is the known state");
        let drives = Arc::new(rdm_admin::build_drive::Drives::default());
        rdm_admin::build_op::ensure_active(&drives, &r.env).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let p = r.builds.read_build(&r.build).await.unwrap();
        assert_eq!(p.attempt, 2, "no attempt after the failed one is claimed without a resubmit; via-drive outcomes {:?}", drive_outcomes());
        assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "only attempt 1 was dispatched");
    });
}

/// CONTRACT (mesh-delete.md: the mesh row goes Dead at mesh.deleted and Dead is final; build.md §1: each
/// attempt computes the remaining work from the accepted topology, the records and its receipts; Luke
/// 2026-10-24: the plan reads mesh death from the Mesh record and the receipts, never from a view that has
/// not caught up): mesh3 was deleted by an earlier Build, its Mesh record Dead, and this fabric-primary's
/// view has not heard its Dead yet (it shows mesh3 Ready, its final primary still a member). A
/// Build whose topology leaves mesh3 out plans nothing for it: no mesh.delete is dispatched, and the Build
/// completes. Must NOT happen: a deleted mesh's mesh.delete planned again.
#[tokio::test]
async fn a_mesh_the_mesh_records_hold_dead_is_never_planned_again_while_the_view_still_shows_it_live() {
    let mut t = view();
    let mesh3 = MeshId::mint();
    t.meshes.push(Mesh { id: Some(mesh3.clone()), name: "mesh3".into(), status: ScopeStatus::Ready });
    t.nodes.push(node("mesh3.admin.1", true, false));
    let counts = |mesh: &str, admins: u32, rpcs: u32| MeshDesired::of(mesh.to_string(), [(rdm_core::NodeKind::NodeAdmin, admins), (rdm_core::NodeKind::RpcNode, rpcs)]);
    let change = TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: "fabric1".into(), meshes: vec![counts("mesh1", 2, 1)] } };
    let r = rig_for(t, change, |ran, _| ran).await;
    r.switch.dead.lock().unwrap().insert(mesh3.clone());
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(30), r.env.run(&drive)).await;
    assert!(!r.ran.0.lock().unwrap().iter().any(|op| op == &format!("mesh.delete:{mesh3}")), "the Dead mesh is not deleted again: {:?}", r.ran.0.lock().unwrap());
    assert_eq!(end.ok(), Some(DriveEnd::Terminal), "the Build completes");
}

/// A gate whose fabric holds ready only once the cell sets it: the fabric's change a
/// Build put it back to pending for.
#[derive(Default)]
struct FabricChange {
    ready: AtomicBool,
    /// The view the fixture's nodes live in: its rows ([`rows_of`]).
    view: Option<Arc<RwLock<Topology>>>,
}

#[async_trait::async_trait]
impl Gate for FabricChange {
    fn fabric_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
    async fn recorded_executables(&self, topology: &rdm_admin::accepted::FabricTopology) -> Result<rdm_admin::accepted::Recorded, String> {
        Ok(match &self.view {
            Some(view) => rows_of(topology, view).await,
            None => Default::default(),
        })
    }
}

/// CONTRACT (Q154; states.md "Resize and executable replacement": "Its Build is Complete only when the fabric is
/// ready again from this Build's own change ... while the fabric changes, the Build reports
/// Blocked naming the change, never Complete"; build.md §1: every accepted Build is reconciled, "Every
/// execution attempt re-reads the accepted topology and current realized/observed state and computes
/// remaining work"; states.md "The fabric's ready": the fabric's change names the accepted Build's `attempt`):
/// a Build whose topology put the fabric back to pending and whose work the view already holds (nothing
/// left to execute) claims its first attempt, which hands back with nothing run, so the fabric's change
/// has the attempt to name; it reports Blocked `fabric-change`, never Complete, while the fabric changes,
/// and is Complete once the fabric holds ready. Must NOT happen: no attempt ever claimed, the
/// fabric's change waiting on the attempt and the drive waiting on the fabric's change, for ever.
#[tokio::test]
async fn a_build_with_nothing_to_execute_claims_the_attempt_the_fabric_change_names_and_is_complete_after_it() {
    let r = rig().await;
    // The view already holds what the Build adds: nothing is left to execute.
    {
        let mut v = r.env.topology.write().await;
        v.nodes.push(node("mesh1.rpc.2", false, false));
    }
    let gate = Arc::new(FabricChange { view: Some(r.env.topology.clone()), ..Default::default() });
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
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    for pass in 0..2 {
        match tokio::time::timeout(Duration::from_secs(10), env.run(&drive)).await {
            Ok(DriveEnd::Stalled(why)) => assert!(why.contains("ready"), "pass {pass}: the drive waits for the fabric's change: {why}"),
            other => panic!("pass {pass}: the Build is never Complete while the fabric changes: {other:?}"),
        }
    }
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert!(p.attempt >= 1, "the fabric's change names the accepted Build's attempt (states.md, the fabric's ready): an attempt of the Build is claimed, not none ({:?} at attempt {})", p.state, p.attempt);
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing is left to execute and nothing ran: {:?}", r.ran.0.lock().unwrap());
    let mut frames = Vec::new();
    while let Some(read) = next_within(&mut reader).await {
        frames.push(read);
    }
    assert!(frames.iter().any(|f| matches!(f, Read::Frame(BuildReply::Blocked { step, .. }) if step == "fabric-change")), "the Build reports Blocked naming the fabric's change: {frames:?}");
    assert!(!frames.iter().any(|f| matches!(f, Read::Terminal(_) | Read::Frame(BuildReply::Complete { .. } | BuildReply::Failed { .. }))), "never Complete while the fabric changes: {frames:?}");
    // The fabric's change set ready: the Build is Complete.
    gate.ready.store(true, Ordering::SeqCst);
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), env.run(&drive)).await.expect("the drive ends once the fabric is ready"), DriveEnd::Terminal);
    let done = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(done.state, rdm_admin::build_state::BuildState::Complete, "the Build is Complete after its own ready: {done:?}");
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran: {:?}", r.ran.0.lock().unwrap());
}

/// CONTRACT (states.md "The check-in checklist": "Individual lifecycle status changes do not wait for
/// the entire check-in checklist"; build.md §1: node.stop is an attempt of the current Build, never a
/// new Build; node-stop.md: the stop's completion is its stopped frame; Q154 holds the Build's own
/// topology change, states.md "Resize and executable replacement"): a Build that put the fabric back
/// to pending and completed once, after its own ready, takes a node.stop as its next
/// attempt while the fabric is changing again. The stop runs, and its attempt ends `Complete` at the
/// stop: the caller's stream ends there. Must NOT happen: the stop's attempt handed back and its
/// stream left `Blocked` on `fabric-change`, waiting for a ready the stopped node itself
/// keeps from being set.
#[tokio::test]
async fn a_stop_attempt_of_a_build_that_completed_ends_at_its_stop_while_the_fabric_changes() {
    use rdm_admin::accepted::AttemptAction;
    use rdm_admin::build_state::{AttemptOpened, AttemptReason};
    let r = rig().await;
    let gate = Arc::new(FabricChange { view: Some(r.env.topology.clone()), ..Default::default() });
    gate.ready.store(true, Ordering::SeqCst);
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
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    // The Build's topology change ran and the fabric held ready from it: Complete.
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), env.run(&Drive::detached(r.build.clone()))).await.expect("the Build's own change completes"), DriveEnd::Terminal);
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Complete);
    // The fabric is changing again, and a node.stop opens the Build's next attempt.
    gate.ready.store(false, Ordering::SeqCst);
    let path: PathName = "mesh1.rpc.1".parse().unwrap();
    let (node_id, from_incarnation) = r.env.topology.read().await.node(&path).and_then(|n| n.incarnation_id.clone().map(|i| (n.node_id.clone(), i))).expect("mesh1.rpc.1 has a birth");
    let action = AttemptAction::Stop { path: path.clone(), node_id, from_incarnation };
    r.builds.open_attempt(&AttemptOpened { mesh: None, build_id: r.build.clone(), attempt: 2, reason: AttemptReason::Stop, action: Some(action), opened_by: "mesh1.admin.1".into(), opened_at_ms: 5, context: None }).await.unwrap();
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(2);
    let end = tokio::time::timeout(Duration::from_secs(10), env.run(&drive)).await;
    let mut frames = Vec::new();
    while let Some(read) = next_within(&mut reader).await {
        frames.push(read);
    }
    assert_eq!(end.ok(), Some(DriveEnd::Terminal), "the stop's attempt ends at its stop, whatever the fabric's change: {frames:?}");
    assert!(r.ran.0.lock().unwrap().iter().any(|op| op == &format!("node.stop:{path}")), "the stop ran: {:?}", r.ran.0.lock().unwrap());
    assert!(!frames.iter().any(|f| matches!(f, Read::Frame(BuildReply::Blocked { step, .. }) if step == "fabric-change")), "a lifecycle attempt never waits on the fabric's change: {frames:?}");
    assert!(frames.iter().any(|f| matches!(f, Read::Terminal(BuildReply::Complete { attempt: 2, .. }))), "the stop's stream ends Complete for its attempt: {frames:?}");
}

/// The fixture's dispatcher, the fabric's change setting ready while the attempt it calls runs:
/// after the drive planned the run and before it reads how the run ended.
struct ReadyDuringTheRun {
    inner: Arc<crate::loopback::Loopback>,
    gate: Arc<FabricChange>,
}

#[async_trait::async_trait]
impl rdm_admin::build_drive::Dispatcher for ReadyDuringTheRun {
    async fn dispatch(&self, turn: &rdm_admin::stop_gate::Turn, executor: &PathName, build_id: &BuildId, attempt: u32, context: rdm_core::context::CallContext, intent: Vec<Vec<u8>>, plan: rdm_admin::executor::RunPlan) -> rdm_admin::build_drive::Dispatched {
        let run = self.inner.dispatch(turn, executor, build_id, attempt, context, intent, plan).await;
        self.gate.ready.store(true, Ordering::SeqCst);
        run
    }
    fn hold(&self, me: &PathName, build_id: &BuildId, attempt: u32) -> Option<rdm_admin::build_run::HeldRun> {
        self.inner.hold(me, build_id, attempt)
    }
}

/// CONTRACT (Q154, Q158; states.md "Resize and executable replacement": "Its Build is Complete only when the
/// fabric is ready again from this Build's own change"; a Build is never blocked for ever): a
/// Build that put the fabric back to pending, nothing left to execute, claims its attempt, whose run hands
/// back at the hold for the fabric's change; the fabric reaches ready while that run is in
/// flight. The run was held for the fabric's change, which is decided from the plan it ran, and the fabric's
/// move is movement: the Build is Complete. Must NOT happen: the hand-back read as "ended incomplete with
/// nothing moved" because the fabric was ready by the time it was read, the drive idle on a world that
/// never moves, the Build Pending at attempt 1 for ever.
#[tokio::test]
async fn a_fabric_ready_while_the_held_run_is_in_flight_completes_the_build() {
    let r = rig().await;
    {
        let mut v = r.env.topology.write().await;
        v.nodes.push(node("mesh1.rpc.2", false, false));
    }
    let gate = Arc::new(FabricChange { view: Some(r.env.topology.clone()), ..Default::default() });
    let e = &r.env;
    let env = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: Arc::new(ReadyDuringTheRun { inner: r.dispatch.clone(), gate: gate.clone() }),
        gate: gate.clone(),
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let mut ends = Vec::new();
    for _ in 0..4 {
        let end = tokio::time::timeout(Duration::from_secs(10), env.run(&drive)).await.expect("a pass ends");
        ends.push(end.clone());
        if end == DriveEnd::Terminal {
            break;
        }
    }
    assert_eq!(ends.last(), Some(&DriveEnd::Terminal), "the Build ends once the fabric is ready: {ends:?}");
    let done = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(done.state, rdm_admin::build_state::BuildState::Complete, "the Build is Complete after its own ready: {done:?}");
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran: {:?}", r.ran.0.lock().unwrap());
}

/// The fabric's state at every write of the Build journal.
struct Ledger {
    state: Mutex<&'static str>,
    writes: Mutex<Vec<(String, &'static str)>>,
}

impl Ledger {
    fn new() -> Arc<Self> {
        Arc::new(Self { state: Mutex::new("pending"), writes: Mutex::new(Vec::new()) })
    }
    fn note(&self, what: String) {
        let at = *self.state.lock().unwrap();
        self.writes.lock().unwrap().push((what, at));
    }
    fn written(&self) -> Vec<(String, &'static str)> {
        self.writes.lock().unwrap().clone()
    }
}

/// The fabric-primary's Build log over a [`Ledger`]: every write is noted with the fabric's state.
struct LedgerBuilds {
    inner: Arc<MemoryBuildStateAdapter>,
    ledger: Arc<Ledger>,
}

#[async_trait::async_trait]
impl BuildStateAdapter for LedgerBuilds {
    async fn publish_accepted(&self, a: &rdm_admin::build_state::BuildAccepted) -> Result<(), BuildStateError> {
        self.ledger.note(format!("accepted:{}", a.build_id));
        self.inner.publish_accepted(a).await
    }
    async fn open_attempt(&self, o: &rdm_admin::build_state::AttemptOpened) -> Result<(), BuildStateError> {
        self.ledger.note(format!("opened:{}", o.attempt));
        self.inner.open_attempt(o).await
    }
    async fn read_build(&self, id: &BuildId) -> Result<rdm_admin::build_state::BuildProjection, BuildStateError> {
        self.inner.read_build(id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<rdm_admin::build_state::BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, c: &rdm_admin::build_state::BuildAttemptClaim) -> Result<rdm_admin::build_state::ClaimOutcome, BuildStateError> {
        self.ledger.note(format!("claim:{}", c.attempt));
        self.inner.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &rdm_admin::build_state::BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.ledger.note(format!("adopted-claim:{}", c.attempt));
        self.inner.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.ledger.note(format!("step:{}:{}:{}", r.attempt, r.operation, r.step));
        self.inner.append_step_receipt(r).await
    }
    async fn append_attempt_receipt(&self, r: &rdm_admin::build_state::BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let outcome = match &r.outcome {
            rdm_admin::build_state::AttemptOutcome::Converged => "converged",
            rdm_admin::build_state::AttemptOutcome::Failed { .. } => "failed",
            rdm_admin::build_state::AttemptOutcome::HandedOff { .. } => "handed-off",
        };
        self.ledger.note(format!("attempt:{}:{outcome}", r.attempt));
        self.inner.append_attempt_receipt(r).await
    }
    async fn facts(&self) -> Result<Vec<rdm_admin::build_state::BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
}

/// CONTRACT (states.md "Resize and executable replacement": "Its Build is Complete only when the fabric is
/// ready again from this Build's own change, as fabric.create's is"; fabric-create.md, Completion:
/// "`fabric.created` when the fabric is ready"): the Day-0 Build (fabric.create's: the
/// bootstrap admin alone, no change submitted) claims its first attempt, which the fabric's change names,
/// and holds its terminal while the fabric is pending. Its `Converged` receipt is written once the fabric
/// is ready, and the Build reads Complete. Must NOT happen: the Day-0 Build's terminal written
/// while the fabric is pending.
#[tokio::test]
async fn the_day0_build_writes_its_complete_only_after_its_own_ready() {
    let mesh_id = MeshId::mint();
    let t = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Pending, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(mesh_id.clone()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node("mesh1.admin.1", true, true)],
    };
    let inner = Arc::new(MemoryBuildStateAdapter::new());
    let ledger = Ledger::new();
    let builds: Arc<dyn BuildStateAdapter> = Arc::new(LedgerBuilds { inner: inner.clone(), ledger: ledger.clone() });
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let day0 = BuildId::mint();
    let contexts = Arc::new(AttemptContexts::in_memory());
    contexts.put(&day0, 1, &rdm_admin::build_claim::current_context()).await.unwrap();
    let accepted = rdm_admin::build_state::BuildAccepted { build_id: day0.clone(), topology: FabricTopology::root("fabric1", "mesh1", mesh_id), submitted_change: None, submitted_at_ms: 1 };
    rdm_admin::build_state::accept(&*builds, &accepted, None, &me.to_string(), "day-0").await.unwrap();
    let topology = Arc::new(RwLock::new(t.clone()));
    let ran = Arc::new(Creates(Mutex::new(Vec::new()), inner.clone()));
    let dispatch = crate::loopback::Loopback::new(builds.clone(), inner.clone(), topology.clone(), ran.clone());
    let gate = Arc::new(FabricChange { view: Some(topology.clone()), ..Default::default() });
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: topology.clone(),
        builds: builds.clone(),
        door: Arc::new(ClaimDoor {
            node_id: t.node(&me).unwrap().node_id.clone(),
            me: me.clone(),
            topology: topology.clone(),
            builds: builds.clone(),
            contexts,
            claims: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
            claimed: Default::default(),
        }),
        dispatcher: dispatch.clone(),
        gate: gate.clone(),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        notices: Default::default(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    let drive = Drive::detached(day0.clone());
    let mut reader = drive.reader(1);
    // The fabric is pending: the drive waits for its change.
    let held = tokio::time::timeout(Duration::from_secs(10), env.run(&drive)).await.expect("the pass ends");
    let mut frames = Vec::new();
    while let Some(read) = next_within(&mut reader).await {
        frames.push(read);
    }
    let while_pending: Vec<String> = ledger.written().into_iter().filter(|(_, state)| *state == "pending").map(|(what, _)| what).collect();
    assert!(
        matches!(&held, DriveEnd::Stalled(why) if why.contains("ready")),
        "the Day-0 Build waits for the fabric's ready, never Complete while pending: {held:?}; written while pending: {while_pending:?}; frames {frames:?}"
    );
    assert!(frames.iter().any(|f| matches!(f, Read::Frame(BuildReply::Blocked { step, .. }) if step == "fabric-change")), "the Day-0 Build reports Blocked naming the fabric's change: {frames:?}");
    assert!(!frames.iter().any(|f| matches!(f, Read::Terminal(_) | Read::Frame(BuildReply::Complete { .. } | BuildReply::Failed { .. }))), "never Complete while the fabric changes: {frames:?}");
    // Every mesh is ready: the fabric is ready.
    *ledger.state.lock().unwrap() = "ready";
    gate.ready.store(true, Ordering::SeqCst);
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), env.run(&drive)).await.expect("the drive ends once the fabric is ready"), DriveEnd::Terminal);
    let written = ledger.written();
    let terminals: Vec<&(String, &'static str)> = written.iter().filter(|(what, _)| what.starts_with("attempt:") && (what.ends_with(":converged") || what.ends_with(":failed"))).collect();
    assert_eq!(terminals.iter().map(|(_, state)| *state).collect::<Vec<_>>(), vec!["ready"], "the Day-0 Build's one terminal is written after ready: {written:?}");
    eprintln!("written while the fabric was pending: {while_pending:?}");
    let done = builds.read_build(&day0).await.unwrap();
    assert_eq!(done.state, rdm_admin::build_state::BuildState::Complete, "the Day-0 Build is Complete after its own ready: {done:?}");
    assert!(ran.0.lock().unwrap().is_empty(), "the Day-0 Build has nothing to execute: {:?}", ran.0.lock().unwrap());
}

/// CONTRACT (build.md §1: fabric.delete "may interrupt the Build in progress"; the teardown opens once every
/// open attempt of the Builds it interrupts has ended; an attempt "whose executor deleted ends on that
/// departure's exact proof"): an interrupted Build whose attempt is claimed by this fabric-primary itself,
/// with no run of it in this process, is ended by this fabric-primary at once: its attempt `Failed` naming
/// the delete, no departure proof asked of its own runtime, which cannot exit while it runs the delete.
/// While a run of that attempt is live in this process, the attempt is not ended under it: the run stops
/// at its next operation and hands back, and the delete's own drive waits for it, framing nothing to its
/// caller. Must NOT happen: the delete waiting on the exit of the process that runs it, or a Blocked frame
/// on the delete's stream that no subject's Status `Blocked` reply made.
#[tokio::test]
async fn a_fabric_delete_ends_an_attempt_this_fabric_primary_holds_without_waiting_on_its_own_exit() {
    let r = rig().await;
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let claim = rdm_admin::build_state::BuildAttemptClaim { build_id: r.build.clone(), attempt: 1, executor: me.to_string(), claimant: Some(me.to_string()), mesh: None };
    assert_eq!(r.builds.claim_attempt(&claim).await.unwrap(), rdm_admin::build_state::ClaimOutcome::Won);
    let grow = r.builds.read_build(&r.build).await.unwrap();
    let at = r.builds.newest_accepted_at_ms().await.unwrap().unwrap_or(0) + 1;
    let delete = rdm_admin::build_state::BuildAccepted { build_id: BuildId::mint(), topology: grow.topology.clone(), submitted_change: Some(TopologyChange::DeleteFabric), submitted_at_ms: at };
    rdm_admin::build_state::accept(&*r.builds, &delete, Some(&r.build), &me.to_string(), "accepted").await.unwrap();
    // A run of the attempt is live here: it is not ended under the run.
    let live = rdm_admin::build_drive::Dispatcher::hold(&*r.dispatch, &me, &r.build, 1).expect("no run of the attempt is live before the cell holds one");
    let drive = Drive::detached(r.build.clone());
    match tokio::time::timeout(Duration::from_secs(10), r.env.run(&drive)).await {
        Ok(DriveEnd::Stalled(why)) => assert!(why.contains("runs in this process"), "the live run is named: {why}"),
        other => panic!("an attempt whose run is live here ends with that run: {other:?}"),
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Running, "nothing is written under the live run");
    // The delete's own drive waits for the interrupted attempt to end: named on its span, never framed
    // to the caller (node-rpc-envelope.md: Blocked's only source is the subject's Status `Blocked`).
    let teardown = Drive::detached(delete.build_id.clone());
    let mut teardown_reader = teardown.reader(1);
    match tokio::time::timeout(Duration::from_secs(10), r.env.run(&teardown)).await {
        Ok(DriveEnd::Stalled(why)) => assert!(why.contains("runs in this process"), "the delete's drive names the live run it waits for: {why}"),
        other => panic!("the delete's teardown opens only once the interrupted attempt ended: {other:?}"),
    }
    assert_eq!(blocked_of(&frames_now(&mut teardown_reader).await), Vec::<BuildReply>::new(), "the delete's wait for the interrupted Build frames no Blocked to the caller");
    drop(live);
    // No run is live: this fabric-primary ends its own attempt at once.
    let mut reader = drive.reader(1);
    let started = std::time::Instant::now();
    let end = tokio::time::timeout(Duration::from_secs(10), r.env.run(&drive)).await;
    assert_eq!(end.ok(), Some(DriveEnd::Terminal), "the interrupted Build ends at once, never waiting on this fabric-primary's own exit");
    assert!(started.elapsed() < Duration::from_secs(5), "ended within its bound: {:?}", started.elapsed());
    assert_eq!(r.proofs.calls.load(Ordering::SeqCst), 0, "no departure proof is asked of this fabric-primary's own runtime");
    let ended = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((ended.state, ended.failed_at), (rdm_admin::build_state::BuildState::Failed, Some(1)), "attempt 1 ended Failed: {ended:?}");
    assert!(ended.last_failure.as_deref().is_some_and(|f| f.contains(delete.build_id.as_str())), "the failure names the delete: {:?}", ended.last_failure);
    let mut frames = Vec::new();
    while let Some(read) = next_within(&mut reader).await {
        frames.push(read);
    }
    assert!(frames.iter().any(|f| matches!(f, Read::Terminal(BuildReply::Failed { step, .. }) if step == "fabric.delete")), "the stream ends Failed naming the delete: {frames:?}");
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing of the interrupted Build ran: {:?}", r.ran.0.lock().unwrap());
}

/// CONTRACT (fabric-node-hooks.md `on_build_ended`, Q256: "Each time a Build's terminal state is
/// recorded, the fabric-primary sends the application one `on_build_ended` notice"; `on_build_started`:
/// "Each time a Build's first attempt is claimed"; build.md §1: a Build a fabric.delete interrupts with no
/// attempt running is ended by the fabric-primary, which "claims the next attempt for itself and ends it
/// `Failed` naming the delete, running nothing"): a Build a fabric.delete interrupts before its first
/// attempt is claimed sends one `on_build_ended` `Failed` naming the step `fabric.delete` and the
/// delete, and no `on_build_started`: the attempt claimed only to end it opened nothing.
/// Must NOT happen: the interrupted Build's ending untold, or an opening told for the attempt that only
/// ended it.
#[tokio::test]
async fn a_build_a_fabric_delete_interrupts_before_its_first_claim_sends_one_failed_ended_and_no_started() {
    let r = rig().await;
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let grow = r.builds.read_build(&r.build).await.unwrap();
    let at = r.builds.newest_accepted_at_ms().await.unwrap().unwrap_or(0) + 1;
    let delete = rdm_admin::build_state::BuildAccepted { build_id: BuildId::mint(), topology: grow.topology.clone(), submitted_change: Some(TopologyChange::DeleteFabric), submitted_at_ms: at };
    rdm_admin::build_state::accept(&*r.builds, &delete, Some(&r.build), &me.to_string(), "accepted").await.unwrap();
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(10), r.env.run(&drive)).await;
    assert_eq!(end.ok(), Some(DriveEnd::Terminal), "the interrupted Build ends");
    let ended = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((ended.state, ended.failed_at), (rdm_admin::build_state::BuildState::Failed, Some(1)), "attempt 1, claimed only to end it, ended Failed: {ended:?}");
    let told = r.told.settled(1).await;
    let keys: Vec<(String, u32, &str)> = told.iter().map(crate::Notice::key).collect();
    assert_eq!(keys, vec![(r.build.to_string(), 1, "failed")], "one Failed ending and no opening: {told:?}");
    let crate::Notice::Ended(c) = &told[0] else { unreachable!() };
    match &c.state {
        rdm_admin::wiring::BuildEndedState::Failed { step, reason, code } => {
            assert_eq!((step.as_str(), *code), ("fabric.delete", rdm_core::outcome::Code::Aborted), "{c:?}");
            assert!(reason.contains(delete.build_id.as_str()), "the ending names the delete: {c:?}");
        }
        other => panic!("the ending is Failed: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing of the interrupted Build ran");
}

/// While attempt 1 runs, another admin's claim of attempt 2 lands on the Build log (fabric-recovery.md
/// step 4: the executor's "one winning claim"), and the run's call is refused naming the later attempt
/// the receiver holds: the run ends `Failed`.
struct ClaimedPastTheRun(Arc<MemoryBuildStateAdapter>);

#[async_trait::async_trait]
impl OperationRunner for ClaimedPastTheRun {
    async fn run(&self, build: &BuildId, _lane: Option<&str>, attempt: u32, _op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        let claim = rdm_admin::build_state::BuildAttemptClaim { build_id: build.clone(), attempt: attempt + 1, executor: "mesh2.admin.1".into(), claimant: Some("mesh2.admin.1".into()), mesh: None };
        assert_eq!(self.0.claim_attempt(&claim).await.unwrap(), rdm_admin::build_state::ClaimOutcome::Won, "mesh2.admin.1 wins attempt {}", attempt + 1);
        Err(rdm_admin::executor::RunEnd::failed(format!("mesh1.admin.2 refused delete-mesh: rejected-stale-workflow (held attempt {}, offered attempt {attempt})", attempt + 1)))
    }
}

/// CONTRACT (build.md §5.2: the current attempt is the "folded maximum/winning claim", and "a delayed
/// dispatch or receipt of an earlier attempt never reopens a terminally closed run"; fabric-recovery.md
/// step 4, Q160: a returning fabric-primary's writes are ignored and "it stops on its first read after the
/// heal (Q109)"): the fabric-primary's run of attempt 1 ends `Failed` after mesh2.admin.1 won attempt 2.
/// The drive reads the Build before it decides on the run's end: attempt 2 is the current attempt and not
/// this admin's claim, so the end of attempt 1 decides nothing. The drive ends `RoleLost` naming
/// mesh2.admin.1, the callers' streams end with that move, and the Build stays in progress under attempt 2.
/// What must NOT happen: the superseded attempt's `Failed` sent to the callers as the Build's terminal, or
/// the Build read failed.
#[tokio::test]
async fn a_fabric_primary_whose_attempt_another_admin_superseded_mid_run_sends_no_terminal_and_names_the_claimant() {
    let r = rig_for(view(), TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) }, |ran, _| Arc::new(ClaimedPastTheRun(ran.1.clone()))).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let (capture, _guard) = crate::receipt_refusal::Capture::new("superseded-mid-run");
    let end = tokio::time::timeout(Duration::from_secs(2), r.env.run(&drive)).await.expect("the drive ends");
    match end {
        DriveEnd::RoleLost(why) => assert!(why.contains("attempt 2") && why.contains("mesh2.admin.1"), "the end names the current attempt and its claimant: {why}"),
        other => panic!("the superseded attempt's end decides nothing and the drive stops on its read: {other:?}"),
    }
    let mut reads = Vec::new();
    while let Some(read) = next_within(&mut reader).await {
        reads.push(read);
    }
    assert!(!reads.iter().any(|r| matches!(r, Read::Terminal(_))), "no terminal of the superseded attempt reaches the callers: {reads:?}");
    assert!(reads.iter().any(|r| matches!(r, Read::RoleLost(loss) if loss.to.as_deref() == Some("mesh2.admin.1"))), "the callers are sent to mesh2.admin.1: {reads:?}");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.state, p.executor.as_deref()), (2, rdm_admin::build_state::BuildState::Running, Some("mesh2.admin.1")), "the Build stays in progress under attempt 2");
    let read = capture.spans("rdm.node_admin.build.reject.via-superseded-attempt");
    assert_eq!(read.len(), 1, "the read that stopped the drive is named: {read:?}");
    assert_eq!((read[0].1.get("attempt").map(String::as_str), read[0].1.get("current_attempt").map(String::as_str), read[0].1.get("to").map(String::as_str)), (Some("1"), Some("2"), Some("mesh2.admin.1")), "{:?}", read[0].1);
}

/// CONTRACT (build.md §4.4 gate 6; Q221 X: "The one exception is the operation mesh-investigation.md's
/// decision step names on a node-admin of the unheard mesh: carrier-edge-lost admits the same-Build
/// node.create of a node-admin into that mesh ... nothing else in it is exempt"): mesh2 is held unheard
/// with its recovery decided, and the Build accepted while it was heard deletes mesh2's second
/// node-admin. That delete is an operation on a node-admin of the unheard mesh but not the decision
/// step's create: the drive claims and dispatches nothing for it and names the hold.
/// Must NOT happen: the decided recovery admitting any operation on a node-admin of the mesh.
#[tokio::test]
async fn a_decided_recovery_admits_no_delete_of_a_node_admin_of_the_held_mesh() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    t.nodes.push(node("mesh2.admin.1", true, false));
    t.nodes.push(node("mesh2.admin.2", false, false));
    t.nodes.push(node("mesh2.rpc.1", false, false));
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh2".to_string(), [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 1)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(Recovered { ran, topology })).await;
    r.switch.peer.lock().unwrap().insert("mesh2".into(), rdm_admin::investigate::PeerMesh { unheard_ms: Some(61_000), recovery_decided: true, unreachable_decided: false });
    let drive = Drive::detached(r.build.clone());
    let end = r.env.run(&drive).await;
    assert!(matches!(&end, DriveEnd::Stalled(why) if why.contains("node.delete:mesh2.admin.2")), "the delete of mesh2's second admin waits for the mesh to be heard: {end:?}");
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing of the held mesh ran: {:?}", r.ran.0.lock().unwrap());
}

/// CONTRACT (node-recovery.md step 2: "For an unheard mesh-primary the fabric-primary runs it, without
/// the half-floor wait, after the election has named the new mesh-primary"; "Execution": "the
/// mesh-primary claims it ... the mesh-primary runs it"; node-identity-and-state.md: "The
/// fabric-primary writes a row only as acting primary of a mesh with no primary"): the drive's claim of
/// an unheard mesh-primary's recovery attempt, opened in the fabric lane and claimed by no one, follows
/// the election's result as the claim at its open does. While the fabric-primary's view still names the
/// unheard `mesh2.admin.1` mesh2's primary and mesh2 holds another node-admin (`mesh2.admin.2`, unheard
/// in that view as all of mesh2 is), the drive claims nothing and stalls naming the mesh and that admin
/// on its span, framing nothing to the caller (node-rpc-envelope.md: Blocked's only source is the
/// subject's Status `Blocked`);
/// once the view names `mesh2.admin.2` mesh2's primary, the attempt is claimed for it. Must NOT happen:
/// the attempt claimed for the fabric-primary itself while mesh2 holds another node-admin.
#[tokio::test]
async fn the_drive_claims_an_unheard_mesh_primarys_recovery_for_the_mesh_primary_the_election_names() {
    use rdm_admin::accepted::AttemptAction;
    use rdm_admin::build_state::{AttemptOpened, AttemptReason};
    let mut t = view();
    for n in t.nodes.iter_mut() {
        n.is_primary = n.name.to_string() == "mesh1.admin.1";
    }
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    for (n, primary) in [("mesh2.admin.1", true), ("mesh2.admin.2", false), ("mesh2.rpc.1", false)] {
        t.nodes.push(node(n, primary, false));
    }
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) };
    let r = rig_for(t, change, |ran, topology| Arc::new(Lands { ran, topology, lands: true })).await;
    let first = Drive::detached(r.build.clone());
    let end = r.env.run(&first).await;
    assert!(matches!(end, DriveEnd::Terminal), "the Build's own attempt ends: {end:?}");

    // mesh2 goes unheard in the fabric-primary's view, its primary still named there.
    r.env.topology.write().await.nodes.iter_mut().filter(|n| n.mesh == "mesh2").for_each(|n| n.status = NodeStatus::PendingReconnect);
    let unheard = r.env.topology.read().await.node(&"mesh2.admin.1".parse().unwrap()).unwrap().clone();
    let attempt = r.builds.read_build(&r.build).await.unwrap().attempt + 1;
    let action = AttemptAction::Recover { path: unheard.name.clone(), node_id: unheard.node_id.clone(), from_incarnation: unheard.incarnation_id.clone().unwrap(), backbone: false };
    r.builds
        .open_attempt(&AttemptOpened { build_id: r.build.clone(), attempt, reason: AttemptReason::Recovery, action: Some(action), opened_by: "mesh1.admin.1".into(), opened_at_ms: 1, mesh: None, context: None })
        .await
        .unwrap();
    let unnamed = Drive::detached(r.build.clone());
    let mut reader = unnamed.reader(attempt);
    let end = tokio::time::timeout(Duration::from_secs(5), r.env.run(&unnamed)).await.expect("the drive ends");
    let claimed = r.builds.read_build(&r.build).await.unwrap().attempt_executors;
    assert!(matches!(&end, DriveEnd::Stalled(why) if why.contains("mesh2") && why.contains("mesh2.admin.2")), "the drive stalls naming the mesh and the node-admin the election names here: {end:?}; claims: {claimed:?}");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the wait for the election's mesh-primary frames no Blocked to the caller");
    assert_eq!(claimed.get(&attempt), None, "nothing is claimed before the view names mesh2's primary: {claimed:?}");

    // The election named mesh2.admin.2: the view names it mesh2's primary, still unheard here.
    r.env.topology.write().await.nodes.iter_mut().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin).for_each(|n| n.is_primary = n.name.to_string() == "mesh2.admin.2");
    let named = Drive::detached(r.build.clone());
    let _ = tokio::time::timeout(Duration::from_secs(5), r.env.run(&named)).await.expect("the drive ends");
    let claimed = r.builds.read_build(&r.build).await.unwrap().attempt_executors;
    assert_eq!(claimed.get(&attempt).map(String::as_str), Some("mesh2.admin.2"), "claimed for the mesh-primary the election named: {claimed:?}");
}

/// CONTRACT (build.md: the Build facts are "written by the fabric-primary", and "a call carrying a claim
/// that is not the current folded attempt of the receiver's projection is refused `StaleClaim`"; Q118;
/// Q239 addendum 0): an executor on another admin reports its step receipts up the attempt's stream,
/// and the fabric-primary writes each by the claim record it holds: a receipt of the claimed attempt is
/// written; a receipt of an attempt the record does not hold is refused
/// `rdm.node_admin.build.reject.via-stale-claim` and written nowhere; the run's reported `Complete` is
/// written as the attempt's verdict by the fabric-primary. Must NOT happen: a receipt the record does
/// not hold written, or a remote run's attempt left with no verdict.
#[tokio::test]
async fn a_remote_executors_reported_receipts_are_written_by_the_claim_record_and_any_other_attempts_refused() {
    use rdm_admin::build_drive::InnerItem;
    use rdm_admin::build_state::{BuildStepReceipt, StepOutcome};
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-rdm108-reported-receipts");
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
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let (e, d) = (env.clone(), drive.clone());
    let running = tokio::spawn(async move { e.run(&d).await });
    tokio::time::timeout(Duration::from_secs(5), fed.dispatched.notified()).await.expect("attempt 1 is dispatched");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.2")), "attempt 1 runs at the remote executor");
    let receipt = |attempt: u32, step: &str| BuildStepReceipt { build_id: r.build.clone(), attempt, operation: "node.create:mesh1.rpc.2".into(), step: step.into(), outcome: StepOutcome::Complete, output: None, executor: Some("mesh1.admin.2".into()), mesh: None };
    let frame = |attempt: u32, step: &str| InnerItem::Frame(BuildReply::Step { build_id: r.build.to_string(), attempt, operation: "node.create:mesh1.rpc.2".into(), step: step.into(), receipt: Some(serde_json::to_vec(&receipt(attempt, step)).unwrap()) });
    feed.send(frame(1, "AllocateIdentity")).unwrap();
    feed.send(frame(2, "PrepareStorage")).unwrap();
    feed.send(InnerItem::Frame(BuildReply::Complete { build_id: r.build.to_string(), attempt: 1 })).unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), running).await.expect("the drive ends on the run's terminal").unwrap();
    assert!(matches!(end, DriveEnd::Terminal), "{end:?}");
    let p = r.builds.read_build(&r.build).await.unwrap();
    let steps: Vec<(u32, String)> = p.steps.iter().filter(|s| s.operation == "node.create:mesh1.rpc.2").map(|s| (s.attempt, s.step.clone())).collect();
    assert_eq!(steps, vec![(1, "AllocateIdentity".to_string())], "the claimed attempt's receipt is written and the other attempt's is not");
    assert!(p.reported.contains(&1), "the fabric-primary writes the remote run's verdict: {:?}", p.reported);
    let refused = capture.spans("rdm.node_admin.build.reject.via-stale-claim");
    assert_eq!(refused.len(), 1, "{refused:?}");
    let attrs = &refused[0].1;
    assert_eq!((attrs.get("attempt").map(String::as_str), attrs.get("held_attempt").map(String::as_str), attrs.get("executor").map(String::as_str)), (Some("2"), Some("1"), Some("mesh1.admin.2")), "{attrs:?}");
    eprintln!("span rdm.node_admin.build.reject.via-stale-claim trace_id={} {:?}", refused[0].0, attrs);
}

/// Creates that take a while, counting how many run at once; the create of `fails` ends failed, its
/// first `failing` runs (every run unless a cell sets fewer).
struct AtOnce {
    ran: Arc<Creates>,
    running: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
    fails: Option<&'static str>,
    failing: std::sync::atomic::AtomicUsize,
}

impl AtOnce {
    fn new(ran: Arc<Creates>, fails: Option<&'static str>) -> Self {
        Self::failing(ran, fails, usize::MAX)
    }

    fn failing(ran: Arc<Creates>, fails: Option<&'static str>, times: usize) -> Self {
        Self { ran, running: Default::default(), most: Default::default(), fails, failing: std::sync::atomic::AtomicUsize::new(times) }
    }
}

#[async_trait::async_trait]
impl OperationRunner for AtOnce {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        if self.fails.is_some_and(|f| op.key() == format!("node.create:{f}")) && self.failing.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
            self.ran.0.lock().unwrap().push(op.key());
            let node = op.key().trim_start_matches("node.create:").to_string();
            return Err(rdm_admin::executor::RunEnd::failed(format!("WaitForBind failed: {node} did not report where it bound (JoinNode) within 300ms")));
        }
        self.ran.run(build, lane, attempt, op).await
    }
}

async fn at_once(fails: Option<&'static str>) -> (Rig, Arc<AtOnce>) {
    at_once_to(fails, 2, 5).await
}

/// [`at_once`], mesh1 grown to `admins` node-admins and `rpcs` rpc nodes.
async fn at_once_to(fails: Option<&'static str>, admins: u32, rpcs: u32) -> (Rig, Arc<AtOnce>) {
    at_once_failing(fails, usize::MAX, admins, rpcs).await
}

/// [`at_once_to`], the create of `fails` failing its first `times` runs only.
async fn at_once_failing(fails: Option<&'static str>, times: usize, admins: u32, rpcs: u32) -> (Rig, Arc<AtOnce>) {
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, admins), (rdm_core::NodeKind::RpcNode, rpcs)]) };
    let runner: Arc<Mutex<Option<Arc<AtOnce>>>> = Arc::default();
    let held = runner.clone();
    let r = rig_for(view(), change, move |ran, _| -> Arc<dyn OperationRunner> {
        let at = Arc::new(AtOnce::failing(ran, fails, times));
        *held.lock().unwrap() = Some(at.clone());
        at
    })
    .await;
    // mesh1's primary `mesh1.admin.2` executes the creates as an admin on another host does (Q118): its
    // run reports every receipt up the stream, a failed step's among them, and the fabric-primary writes it.
    r.dispatch.reports("mesh1.admin.2");
    let at = runner.lock().unwrap().clone().unwrap();
    (r, at)
}

/// CONTRACT (Q245): the member creates of a mesh that one admin executes run at once, never one after
/// another. mesh1 grows by four rpc nodes; its mesh primary runs the four creates together.
#[tokio::test]
async fn a_meshs_member_creates_run_at_once() {
    let (r, at) = at_once(None).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    let mut ran = r.ran.0.lock().unwrap().clone();
    ran.sort();
    assert_eq!(ran, ["node.create:mesh1.rpc.2", "node.create:mesh1.rpc.3", "node.create:mesh1.rpc.4", "node.create:mesh1.rpc.5"].map(String::from).to_vec());
    assert_eq!(at.most.load(Ordering::SeqCst), 4, "the four creates ran at once");
}

/// CONTRACT (Q245, Q252): a member create that fails stops none of the others: each of the four runs to
/// its own end, and the attempt ends once every one has. Failing every time, the next attempt creates it
/// again, and its second failure ends the Build, the others done.
#[tokio::test]
async fn a_failed_member_create_stops_none_of_the_others() {
    let (r, at) = at_once(Some("mesh1.rpc.3")).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.expect("the drive ends within 20 s"), DriveEnd::Terminal);
    let mut ran = r.ran.0.lock().unwrap().clone();
    ran.sort();
    assert_eq!(ran, ["node.create:mesh1.rpc.2", "node.create:mesh1.rpc.3", "node.create:mesh1.rpc.3", "node.create:mesh1.rpc.4", "node.create:mesh1.rpc.5"].map(String::from).to_vec(), "every create ran to its end, the failed one on the next attempt too");
    assert_eq!(at.most.load(Ordering::SeqCst), 4);
}

/// CONTRACT (Q245): a mesh that already has its node-admins grows by a further node-admin and three rpc
/// nodes: the further node-admin is born with them, the four creates at once. Only a mesh's first
/// node-admin is created on its own.
#[tokio::test]
async fn a_further_node_admin_is_created_with_its_meshs_other_nodes() {
    let (r, at) = at_once_to(None, 3, 4).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    let mut ran = r.ran.0.lock().unwrap().clone();
    ran.sort();
    assert_eq!(ran, ["node.create:mesh1.admin.3", "node.create:mesh1.rpc.2", "node.create:mesh1.rpc.3", "node.create:mesh1.rpc.4"].map(String::from).to_vec());
    assert_eq!(at.most.load(Ordering::SeqCst), 4, "the further node-admin's create ran with the rpc nodes'");
}

/// The fixture view with mesh2 and mesh3 beside mesh1, each its own mesh-primary's: `mesh1.admin.1` is the
/// fabric-primary and mesh1's primary, `mesh2.admin.1` and `mesh3.admin.1` the primaries of theirs.
fn three_meshes() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: ["mesh1", "mesh2", "mesh3"].map(|m| Mesh { id: Some(MeshId::mint()), name: m.into(), status: ScopeStatus::Ready }).to_vec(),
        nodes: vec![node("mesh1.admin.1", true, true), node("mesh2.admin.1", true, false), node("mesh2.rpc.1", false, false), node("mesh3.admin.1", true, false), node("mesh3.rpc.1", false, false)],
    }
}

/// CONTRACT (Q250): a Build grows mesh2 and mesh3, each by two rpc nodes, each mesh's nodes its own
/// mesh-primary's to create. The fabric-primary sends each mesh-primary its section at once, each an attempt
/// of that mesh's lane: the two sections' creates run together, and the Build completes. Must NOT happen:
/// mesh3's creates waiting for mesh2's attempt to end.
#[tokio::test]
async fn each_meshs_section_is_sent_to_its_mesh_primary_at_once() {
    let fabric = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(rdm_core::NodeKind::NodeAdmin, 1)]), MeshDesired::of("mesh2", [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 3)]), MeshDesired::of("mesh3", [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 3)])] };
    let runner: Arc<Mutex<Option<Arc<AtOnce>>>> = Arc::default();
    let held = runner.clone();
    let r = rig_for(three_meshes(), TopologyChange::ReconcileFabric { desired: fabric }, move |ran, _| -> Arc<dyn OperationRunner> {
        let at = Arc::new(AtOnce::new(ran, None));
        *held.lock().unwrap() = Some(at.clone());
        at
    })
    .await;
    let at = runner.lock().unwrap().clone().unwrap();
    let drive = Drive::detached(r.build.clone());
    let ended = tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.expect("the drive ends within 20 s");
    assert_eq!(ended, DriveEnd::Terminal);
    let mut ran = r.ran.0.lock().unwrap().clone();
    ran.sort();
    assert_eq!(ran, ["node.create:mesh2.rpc.2", "node.create:mesh2.rpc.3", "node.create:mesh3.rpc.2", "node.create:mesh3.rpc.3"].map(String::from).to_vec());
    let mut lanes: Vec<(String, String)> = r.dispatch.sections.lock().unwrap().iter().map(|(e, l, _)| (e.clone(), l.clone())).collect();
    lanes.sort();
    assert_eq!(lanes, vec![("mesh2.admin.1".to_string(), "mesh2".to_string()), ("mesh3.admin.1".to_string(), "mesh3".to_string())], "each mesh-primary was sent its mesh's section, in its mesh's lane");
    assert_eq!(at.most.load(Ordering::SeqCst), 4, "the two sections' creates ran together");
}

/// CONTRACT (Q250, Q245, Q252): mesh3's section fails at mesh3.rpc.2's create every time. mesh2's section
/// completes all the same: a failed section stops none of the others. mesh3's operations are planned again
/// and its section sent again on the next pass, a new attempt of its lane; the same operation failing there
/// too ends the Build `Failed` with the step and reason of the second failure (node-rpc-envelope.md: "Only a
/// fabric-primary change or a failed step ends an attempt"). Must NOT happen: mesh2's creates held by
/// mesh3's failure, mesh3's section sent without end, a stall, or a Blocked frame before the terminal.
#[tokio::test]
async fn a_section_that_fails_stops_no_other_and_the_same_operation_failing_twice_ends_the_build_failed() {
    use rdm_admin::build_state::BuildState;
    let fabric = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(rdm_core::NodeKind::NodeAdmin, 1)]), MeshDesired::of("mesh2", [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 3)]), MeshDesired::of("mesh3", [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 3)])] };
    let r = rig_for(three_meshes(), TopologyChange::ReconcileFabric { desired: fabric }, move |ran, _| -> Arc<dyn OperationRunner> { Arc::new(AtOnce::new(ran, Some("mesh3.rpc.2"))) }).await;
    // Each mesh-primary serves its section as an admin on another host does: its failed step's receipt
    // goes up the stream, and its Failed terminal names the step.
    r.dispatch.reports("mesh2.admin.1");
    r.dispatch.reports("mesh3.admin.1");
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let ended = tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.expect("the drive ends within 20 s");
    assert_eq!(ended, DriveEnd::Terminal, "the same operation failing twice running ends the Build");
    let mut frames = Vec::new();
    while let Some(read) = next_within(&mut reader).await {
        frames.push(read);
    }
    match frames.last() {
        Some(Read::Terminal(BuildReply::Failed { operation, step, reason, .. })) => {
            assert_eq!((operation.as_str(), step.as_str()), ("node.create:mesh3.rpc.2", "WaitForBind"), "the terminal names the second failure's operation and step");
            assert!(reason.contains("mesh3.rpc.2 did not report where it bound"), "the step's own reason: {reason}");
        }
        other => panic!("the stream ends Failed {{ step, reason }}: {other:?}; frames {frames:?}"),
    }
    assert!(!frames.iter().any(|f| matches!(f, Read::Frame(BuildReply::Blocked { .. }))), "no Blocked frame reaches the caller: {frames:?}");
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, BuildState::Failed, "the Build record is Failed");
    let ran = r.ran.0.lock().unwrap().clone();
    assert!(["node.create:mesh2.rpc.2", "node.create:mesh2.rpc.3"].iter().all(|k| ran.iter().any(|r| r == k)), "mesh2's section completed: {ran:?}");
    let sent: Vec<(String, u32)> = r.dispatch.sections.lock().unwrap().iter().map(|(_, l, a)| (l.clone(), *a)).collect();
    assert_eq!(sent.iter().filter(|(l, _)| l == "mesh3").map(|(_, a)| *a).collect::<Vec<_>>(), vec![1, 2], "mesh3's section was sent again, as its lane's next attempt, once: {sent:?}");
}

/// The executors' reconcile spans of a cell's capture, as `(attempt, outcome, operations, incomplete)`
/// (an executor's span parents to the dispatch's traceparent, not to the cell's root).
fn reconciles(spans: &[serde_json::Value]) -> Vec<(String, String, String, String)> {
    let at = |s: &serde_json::Value, k: &str| s["attributes"][k].as_str().unwrap_or("").to_string();
    spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-reconcile").map(|s| (at(s, "attempt"), at(s, "outcome"), at(s, "operations"), at(s, "incomplete"))).collect()
}

/// The drive's dispatch spans of a cell's capture, as `(attempt, executor, node, outcome)`.
fn dispatches(spans: &[serde_json::Value]) -> Vec<(String, String, String, String)> {
    let at = |s: &serde_json::Value, k: &str| s["attributes"][k].as_str().unwrap_or("").to_string();
    spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-dispatch").map(|s| (at(s, "attempt"), at(s, "executor"), at(s, "node"), at(s, "outcome"))).collect()
}

/// CONTRACT (Q245: "a failed node does not stop it, it just self-heals like it already does"): in the
/// fabric lane a member create that ends failed hands its attempt back to the fabric-primary, as an
/// incomplete operation does: the creates that completed stand, the Build stays in progress, and the
/// next attempt the fabric-primary claims plans the failed create again, creates it, and the Build
/// completes. Must NOT happen: the Build failed by one failed create, or a completed create run again.
#[test]
fn a_member_create_that_fails_once_is_created_by_the_next_attempt_and_the_build_completes() {
    use rdm_admin::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (end, p, ran, frames, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.member-create-fails-once");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _at) = at_once_failing(Some("mesh1.rpc.3"), 1, 2, 5).await;
        let drive = Drive::detached(r.build.clone());
        let mut reader = drive.reader(1);
        let end = tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.expect("the drive ends within 20 s");
        let mut frames = Vec::new();
        while let Some(read) = next_within(&mut reader).await {
            frames.push(read);
        }
        let ran = r.ran.0.lock().unwrap().clone();
        (end, r.builds.read_build(&r.build).await.unwrap(), ran, frames, trace_id)
    });
    assert_eq!(end, DriveEnd::Terminal, "the Build completes");
    assert!(matches!(frames.last(), Some(Read::Terminal(BuildReply::Complete { .. }))), "the stream ends Complete: {frames:?}");
    assert!(!frames.iter().any(|f| matches!(f, Read::Frame(BuildReply::Blocked { .. } | BuildReply::Failed { .. }))), "the heal frames nothing to the caller: {frames:?}");
    assert_eq!(p.state, BuildState::Complete, "one failed create does not fail the Build");
    let mut ran = ran;
    ran.sort();
    assert_eq!(ran, ["node.create:mesh1.rpc.2", "node.create:mesh1.rpc.3", "node.create:mesh1.rpc.3", "node.create:mesh1.rpc.4", "node.create:mesh1.rpc.5"].map(String::from).to_vec(), "the failed create ran again and no completed create did");
    let spans = cap.spans();
    let runs = reconciles(&spans);
    eprintln!("cell trace_id={trace_id} reconcile spans {runs:?}");
    assert!(runs.iter().any(|(a, o, _, inc)| a == "1" && o == "handed-off" && inc.contains("WaitForBind failed: mesh1.rpc.3")), "attempt 1 is handed back with the failed create's reason: {runs:?}");
    assert!(runs.iter().any(|(a, o, ops, _)| a == "2" && o == "converged" && ops == "node.create:mesh1.rpc.3"), "attempt 2 runs the failed create alone and converges: {runs:?}");
}

/// CONTRACT (Q245, Q252; node-rpc-envelope.md: "A failure is `Failed { step, reason }`", "Only a
/// fabric-primary change or a failed step ends an attempt"): in the fabric lane a member create that fails
/// is created again by the next attempt, once; failing there too, that attempt ends `Failed` with the
/// failed step and its own reason, and the Build is failed. Nothing else reaches the caller: no Blocked
/// frame (its only source is the subject's Status `Blocked`). Must NOT happen: a third attempt, a
/// stall, the completed creates run again, or a Blocked frame before the terminal.
#[test]
fn a_member_create_that_fails_on_the_next_attempt_too_ends_the_build_failed_naming_its_step() {
    use rdm_admin::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (end, p, ran, frames, dispatched, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.member-create-fails-twice");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _at) = at_once(Some("mesh1.rpc.3")).await;
        let drive = Drive::detached(r.build.clone());
        let mut reader = drive.reader(1);
        let end = tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.ok();
        let mut frames = Vec::new();
        while let Some(read) = next_within(&mut reader).await {
            frames.push(read);
        }
        let ran = r.ran.0.lock().unwrap().clone();
        let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
        (end, r.builds.read_build(&r.build).await.unwrap(), ran, frames, dispatched, trace_id)
    });
    let spans = cap.spans();
    let d = dispatches(&spans);
    assert_eq!(end, Some(DriveEnd::Terminal), "the second failure ends the Build within 20 s; {} dispatches, the first 4 {:?}, reconciles {:?}", d.len(), &d[..d.len().min(4)], reconciles(&spans));
    assert_eq!(p.state, BuildState::Failed, "the Build record is Failed");
    assert_eq!(dispatched, vec![("mesh1.admin.2".to_string(), 1), ("mesh1.admin.2".to_string(), 2)], "no third attempt is claimed");
    match frames.last() {
        Some(Read::Terminal(BuildReply::Failed { attempt, operation, step, reason, .. })) => {
            assert_eq!((*attempt, operation.as_str(), step.as_str()), (2, "node.create:mesh1.rpc.3", "WaitForBind"), "the terminal names the failed operation and step");
            assert!(reason.contains("mesh1.rpc.3 did not report where it bound"), "the step's own reason: {reason}");
        }
        other => panic!("the stream ends Failed {{ step, reason }}: {other:?}; frames {frames:?}"),
    }
    assert!(!frames.iter().any(|f| matches!(f, Read::Frame(BuildReply::Blocked { .. }))), "no Blocked frame reaches the caller: {frames:?}");
    let count = |k: &str| ran.iter().filter(|r| *r == k).count();
    assert_eq!(count("node.create:mesh1.rpc.3"), 2, "the failed create is run by attempt 1 and once more by attempt 2: {ran:?}");
    assert!(["node.create:mesh1.rpc.2", "node.create:mesh1.rpc.4", "node.create:mesh1.rpc.5"].iter().all(|k| count(k) == 1), "the completed creates ran once: {ran:?}");
    let runs = reconciles(&spans);
    eprintln!("cell trace_id={trace_id} reconcile spans {runs:?}");
    assert!(runs.iter().any(|(a, o, ops, _)| a == "2" && o == "failed" && ops == "node.create:mesh1.rpc.3"), "attempt 2 runs the create alone and fails: {runs:?}");
    let healed: Vec<String> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-create-failed" && s["attributes"]["operation"] == "node.create:mesh1.rpc.3").map(|s| s["attributes"]["attempt"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(healed, ["1"], "one heal attempt: only attempt 1's failure hands back");
    assert!(spans.iter().all(|s| s["name"] != "rdm.node_admin.build.update.via-stalled"), "nothing stalls");
}

/// mesh1's in-flight row taken by an attempt of another Build, claimed by `mesh1.admin.2` (R-BI7
/// caveat 3): every claim of an attempt that acts in mesh1 is refused build-in-progress naming it.
async fn mesh1_held_by_another_build(r: &Rig) -> BuildId {
    use rdm_admin::storage::MeshStorage as _;
    let other = BuildId::mint();
    let row = rdm_admin::mesh_claims::InFlight { build_id: other.clone(), lane: None, attempt: 1, claimant: "mesh1.admin.2".into(), claimant_node: None };
    assert_eq!(r.env.door.claims.take_in_flight("mesh1", &row).await.unwrap(), rdm_admin::mesh_claims::Taken::Won, "the other Build's attempt holds mesh1");
    other
}

/// CONTRACT (R-BI7 caveat 3: one attempt in flight per mesh; node-rpc-envelope.md: Blocked is a
/// "nonterminal hook blocker ... Its only source is the subject's Status `Blocked` reply to the
/// executor's ProbeNodeState"): mesh1 is held by another Build's attempt, so the claim of this Build's
/// next attempt in mesh1 is refused build-in-progress. The drive claims and dispatches nothing and
/// stalls naming the mesh and the attempt that holds it on its span. Must NOT happen: a Blocked frame on
/// the caller's stream for the claim's refusal, which no subject's Status `Blocked` reply made.
#[tokio::test]
async fn a_claim_refused_build_in_progress_on_its_mesh_stalls_the_drive_and_frames_nothing() {
    let r = rig().await;
    let other = mesh1_held_by_another_build(&r).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("mesh mesh1") && why.contains(other.as_str()) && why.contains("one attempt in flight per mesh"), "the stall names mesh1 and the attempt holding it: {why}"),
        other => panic!("nothing is claimed in a mesh another attempt holds: {other:?}"),
    }
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing is dispatched");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the claim's refusal frames no Blocked to the caller");
}

/// Refuses every `build.attempt.run` by name, as an executor refuses a call whose claim names another
/// recipient (`NotExecutor`, node-rpc-envelope.md:1366).
struct Refuses;

#[async_trait::async_trait]
impl rdm_admin::build_drive::Dispatcher for Refuses {
    async fn dispatch(&self, _turn: &rdm_admin::stop_gate::Turn, executor: &PathName, _build_id: &BuildId, _attempt: u32, _context: rdm_core::context::CallContext, _intent: Vec<Vec<u8>>, _plan: rdm_admin::executor::RunPlan) -> rdm_admin::build_drive::Dispatched {
        rdm_admin::build_drive::Dispatched::Refused(BuildReply::NotExecutor { named: executor.to_string(), recipient: "mesh1.admin.9".into() })
    }
}

/// CONTRACT (node-rpc-envelope.md:1366: build.attempt.run "refuses by name when
/// the recipient is not the executor named in the claim (NotExecutor { named, recipient })"; Blocked's
/// "only source is the subject's Status `Blocked` reply to the executor's ProbeNodeState"): the executor
/// refuses the dispatch of the claimed attempt by name. The drive records the refusal on its dispatch
/// span and stalls naming it; nothing runs. Must NOT happen: a Blocked frame on the caller's stream for
/// the executor's refusal.
#[tokio::test]
async fn an_executor_that_refuses_the_dispatch_stalls_the_drive_and_frames_nothing() {
    let r = rig().await;
    let e = &r.env;
    let env = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: Arc::new(Refuses),
        gate: e.gate.clone(),
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("refused attempt 1") && why.contains("mesh1.admin.9"), "the stall names the refusal: {why}"),
        other => panic!("a refused dispatch stalls the drive: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the executor's refusal frames no Blocked to the caller");
}

/// CONTRACT (Q250; node-rpc-envelope.md: Blocked's "only source is the subject's Status `Blocked` reply
/// to the executor's ProbeNodeState"): a Build grows mesh2 and mesh3, each its own mesh-primary's, and
/// mesh3's mesh-primary cannot be reached, its departure not proven. mesh2's section completes; mesh3's
/// holds its mesh, and the drive stalls naming it on its span. Must NOT happen: a Blocked frame on the
/// caller's stream for the held section.
#[tokio::test]
async fn a_section_whose_mesh_primary_is_unreached_stalls_the_drive_and_frames_nothing() {
    let fabric = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(rdm_core::NodeKind::NodeAdmin, 1)]), MeshDesired::of("mesh2", [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 3)]), MeshDesired::of("mesh3", [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 3)])] };
    let r = rig_for(three_meshes(), TopologyChange::ReconcileFabric { desired: fabric }, move |ran, _| -> Arc<dyn OperationRunner> { Arc::new(AtOnce::new(ran, None)) }).await;
    r.dispatch.lose("mesh3.admin.1");
    let drive = Drive::detached(r.build.clone());
    // Every frame of the drive: no attempt of the fabric's own numbering is open while sections run.
    let mut reader = drive.reader(0);
    match tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.expect("the drive ends within 20 s") {
        DriveEnd::Stalled(why) => assert!(why.contains("mesh mesh3") && why.contains("not reached") && why.contains("not proven"), "the stall names mesh3's held section: {why}"),
        other => panic!("a section whose mesh-primary is unreached holds the Build: {other:?}"),
    }
    let ran = r.ran.0.lock().unwrap().clone();
    assert!(["node.create:mesh2.rpc.2", "node.create:mesh2.rpc.3"].iter().all(|k| ran.iter().any(|r| r == k)), "mesh2's section completed: {ran:?}");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the held section frames no Blocked to the caller");
}

/// Accepts a fabric.delete after the Build while the operation runs (build.md §1: it "may interrupt the
/// Build in progress"), and the operation, admitted before it, fails within its bound.
struct DeleteDuring(Arc<Creates>, Mutex<Option<BuildId>>);

#[async_trait::async_trait]
impl OperationRunner for DeleteDuring {
    async fn run(&self, build: &BuildId, _lane: Option<&str>, _attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        let builds = &self.0.1;
        let grow = builds.read_build(build).await.unwrap();
        let at = builds.newest_accepted_at_ms().await.unwrap().unwrap_or(0) + 1;
        let delete = rdm_admin::build_state::BuildAccepted { build_id: BuildId::mint(), topology: grow.topology.clone(), submitted_change: Some(TopologyChange::DeleteFabric), submitted_at_ms: at };
        rdm_admin::build_state::accept(&**builds, &delete, Some(build), "mesh1.admin.1", "accepted").await.unwrap();
        *self.1.lock().unwrap() = Some(delete.build_id);
        self.0.0.lock().unwrap().push(op.key());
        Err(rdm_admin::executor::RunEnd::failed(format!("{} did not end within its bound", op.key())))
    }
}

/// CONTRACT (build.md §1: "When every open attempt of the interrupted Build has ended, its run receives
/// `Failed { step, reason }` naming the delete"; node-rpc-envelope.md: Blocked's "only source is the
/// subject's Status `Blocked` reply to the executor's ProbeNodeState"): a fabric.delete is accepted
/// while the executor `mesh1.admin.2` runs the Build's create, which fails within its bound. The run
/// hands back to the fabric-primary, named on its span, and the caller's stream ends `Failed` naming
/// the delete. Must NOT happen: a Blocked frame on the caller's stream for the interruption, which no
/// subject's Status `Blocked` reply made.
#[tokio::test]
async fn a_run_a_fabric_delete_interrupts_frames_nothing_before_the_failed_naming_the_delete() {
    let deleted: Arc<Mutex<Option<Arc<DeleteDuring>>>> = Arc::default();
    let held = deleted.clone();
    let r = rig_for(view(), TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)]) }, move |ran, _| -> Arc<dyn OperationRunner> {
        let d = Arc::new(DeleteDuring(ran, Mutex::new(None)));
        *held.lock().unwrap() = Some(d.clone());
        d
    })
    .await;
    // The executor frames its run's progress on the call's stream, as an admin's Build state does.
    r.dispatch.frames("mesh1.admin.2");
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let mut ends = Vec::new();
    for _ in 0..3 {
        let end = tokio::time::timeout(Duration::from_secs(10), r.env.run(&drive)).await.expect("the drive ends within 10 s");
        ends.push(end.clone());
        if end == DriveEnd::Terminal {
            break;
        }
    }
    assert_eq!(ends.last(), Some(&DriveEnd::Terminal), "the interrupted Build ends: {ends:?}");
    let delete = deleted.lock().unwrap().clone().unwrap().1.lock().unwrap().clone().expect("the delete was accepted during the run");
    let frames = frames_now(&mut reader).await;
    assert!(matches!(frames.last(), Some(Read::Terminal(BuildReply::Failed { step, reason, .. })) if step == "fabric.delete" && reason.contains(delete.as_str())), "the stream ends Failed naming the delete: {frames:?}");
    assert_eq!(blocked_of(&frames), Vec::<BuildReply>::new(), "the interruption frames no Blocked to the caller");
}

/// CONTRACT (node-primary-update.md failure contract: "The workflow continues only after the result is
/// installed and acknowledged"; node-rpc-envelope.md: Blocked's "only source is the subject's Status
/// `Blocked` reply to the executor's ProbeNodeState"): the fabric-primary `mesh1.admin.1` holds both
/// primaries and its Build deletes it, so its drive updates them first; the update answers and this view
/// still names it the primary of both. The drive stalls naming it on its span and dispatches nothing.
/// Must NOT happen: a Blocked frame on the caller's stream for the update the view does not show.
#[tokio::test(start_paused = true)]
async fn a_primary_update_the_view_does_not_show_stalls_the_drive_and_frames_nothing() {
    let r = rig_for(view_primary_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match tokio::time::timeout(Duration::from_secs(60), r.env.run(&drive)).await.expect("the drive ends") {
        DriveEnd::Stalled(why) => assert!(why.contains("still names it") && why.contains("node.delete:mesh1.admin.1"), "the stall names the update the view does not show: {why}"),
        other => panic!("an update the view does not show stalls the drive: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the update's wait frames no Blocked to the caller");
}

/// The primary's own node.primary.update that moves both primaries to `mesh1.admin.2` and the fabric
/// record with them, then ends without node.primary.updated (`Incomplete`).
struct MovedUnanswered {
    inner: Arc<Switch>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl Gate for MovedUnanswered {
    async fn target(&self) -> Result<Option<String>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn update_primaries(&self, _: &rdm_admin::executor::RoleMove, _: &BuildId, _: u32, _: u32, _: &str) -> Result<(), rdm_admin::build_drive::RoleMoveEnd> {
        for n in self.topology.write().await.nodes.iter_mut() {
            let successor = n.name.to_string() == "mesh1.admin.2";
            (n.is_primary, n.is_fabric_primary) = (successor, successor);
        }
        self.inner.target_to(Some("mesh1.admin.2"));
        Err(rdm_admin::build_drive::RoleMoveEnd::Incomplete("the successor's answer was not read".into()))
    }
}

/// CONTRACT (node-primary-update.md failure contract: "After the successor installed itself, it is
/// primary whatever the call returns"; node-rpc-envelope.md: Blocked's "only source is the subject's
/// Status `Blocked` reply to the executor's ProbeNodeState"): the fabric-primary's own update moves both
/// primaries to `mesh1.admin.2` and ends without its answer. The drive records why on its dispatch span
/// and ends at the fabric record naming the successor. Must NOT happen: a Blocked frame on the caller's
/// stream for the unanswered update.
#[tokio::test(start_paused = true)]
async fn a_primary_update_that_moved_the_primaries_without_its_answer_frames_nothing() {
    let r = rig_for(view_primary_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let e = &r.env;
    let env = Arc::new(DriveEnv {
        me: e.me.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        door: e.door.clone(),
        dispatcher: e.dispatcher.clone(),
        gate: Arc::new(MovedUnanswered { inner: r.switch.clone(), topology: e.topology.clone() }),
        departure: e.departure.clone(),
        verdicts: e.verdicts.clone(),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    });
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let end = tokio::time::timeout(Duration::from_secs(60), env.run(&drive)).await.expect("the drive ends");
    assert!(matches!(&end, DriveEnd::RoleLost(_)), "the drive ends at the record naming the successor: {end:?}");
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the unanswered update frames no Blocked to the caller");
}

/// CONTRACT (node-primary-update.md failure contract: "The fabric-primary claims the attempt at the claim
/// door before it commands"; R-BI7 caveat 3; node-rpc-envelope.md: Blocked's "only source is the
/// subject's Status `Blocked` reply to the executor's ProbeNodeState"): the fabric-primary's Build deletes
/// it, so its drive claims the attempt for its own update first, and mesh1 is held by another Build's
/// attempt: the claim is refused build-in-progress. The drive commands nothing and stalls naming the
/// refusal on its span. Must NOT happen: the update commanded, or a Blocked frame on the caller's stream
/// for the claim's refusal.
#[tokio::test(start_paused = true)]
async fn a_primary_update_whose_attempt_is_not_claimed_stalls_the_drive_and_frames_nothing() {
    let r = rig_for(view_primary_of_both(), TopologyChange::RemoveNode { node: "mesh1.admin.1".parse().unwrap() }, |ran, _| ran).await;
    let other = mesh1_held_by_another_build(&r).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match tokio::time::timeout(Duration::from_secs(60), r.env.run(&drive)).await.expect("the drive ends") {
        DriveEnd::Stalled(why) => assert!(why.contains("was not claimed for") && why.contains(other.as_str()), "the stall names the claim's refusal: {why}"),
        other => panic!("an update whose attempt is not claimed stalls the drive: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing ran or was dispatched");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the claim's refusal frames no Blocked to the caller");
}

// The reconcile loop's wait (Q253, amended by Luke 2026-10-28: "I hate timers; make this event
// driven", rdm#164): every pass of the fabric-primary's reconcile loop is woken by an event (a view
// install, a Build submitted, a fabric state change, the peer-mesh investigation's move, a drive's
// end), never by a tick. The loop below is the admin's (boot.rs): `wake.next()`, then one pass, whose
// drive part is `ensure_active`.

/// What wakes the loop under test, each the admin's own source: the control plane's
/// `build_submitted`, the admin's records (a view installed, the fabric state), the peer-mesh
/// investigation's ladder and the drives.
struct Wakes {
    submitted: Arc<tokio::sync::Notify>,
    records: Arc<rdm_admin::admin::Records>,
    ladder: Arc<Mutex<rdm_admin::investigate::Ladder>>,
    drives: Arc<rdm_admin::build_drive::Drives>,
    provider: Arc<rdm_admin::deployment::process::ProcessDeploymentProvider>,
}

impl Wakes {
    fn new() -> Self {
        Self { submitted: Arc::new(tokio::sync::Notify::new()), records: Arc::new(rdm_admin::admin::Records::default()), ladder: Arc::new(Mutex::new(rdm_admin::investigate::Ladder::new(Self::rungs()))), drives: Arc::new(rdm_admin::build_drive::Drives::default()), provider: Arc::new(rdm_admin::deployment::process::ProcessDeploymentProvider::new()) }
    }

    /// The ladder's rungs at the gate's cadence (3 s floor, 500 ms backbone interval).
    fn rungs() -> rdm_admin::investigate::Rungs {
        rdm_admin::investigate::Rungs::at(Duration::from_secs(3), Duration::from_millis(500))
    }

    fn wake(&self) -> rdm_admin::admin::ReconcileWake {
        rdm_admin::admin::ReconcileWake::new(self.submitted.clone(), self.records.subscribe_view(), self.records.subscribe_fabric_state(), self.ladder.lock().unwrap().subscribe_moved(), self.drives.subscribe_ended(), rdm_admin::deployment::provider::DeploymentProvider::exits(&*self.provider))
    }
}

/// The reconcile loop as the admin runs it, over `env` and the drives of `wakes`, inside the cell's
/// root span: what woke each pass, in order. With `hold`, each pass waits at its start for one permit
/// the cell gives.
fn reconciler(env: &Arc<DriveEnv>, wakes: &Wakes, hold: Option<Arc<tokio::sync::Semaphore>>) -> (Arc<Mutex<Vec<rdm_admin::admin::Woken>>>, tokio::task::JoinHandle<()>) {
    use tracing::Instrument;
    let passes: Arc<Mutex<Vec<rdm_admin::admin::Woken>>> = Arc::default();
    let (env, drives, mut wake, seen) = (env.clone(), wakes.drives.clone(), wakes.wake(), passes.clone());
    let task = tokio::spawn(
        async move {
            loop {
                let woken = wake.next().await;
                seen.lock().unwrap().push(woken);
                if let Some(hold) = &hold {
                    hold.acquire().await.unwrap().forget();
                }
                rdm_admin::build_op::ensure_active(&drives, &env).await;
            }
        }
        .instrument(tracing::Span::current()),
    );
    (passes, task)
}

/// How long the cell leaves the loop and its drive to settle after an event: far past a pass and a
/// drive's run over the in-memory rig, and past three of the 300 ms arms the loop had.
const SETTLE: Duration = Duration::from_millis(1000);

/// The `rdm.node_admin.build.update.via-drive` spans of `trace_id`: one per pass that drove the Build.
fn drive_passes(spans: &[serde_json::Value], trace_id: &str) -> usize {
    spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-drive" && s["trace_id"] == trace_id).count()
}

/// CONTRACT (Q253 amended: "the wake-up is an event, never a tick"): a fabric-primary whose drive waits
/// after an attempt handed back with nothing moved, and to which nothing happens, runs no reconcile pass:
/// over 2 s the loop's first pass is its only one, and the Build was driven once.
/// Must NOT happen: a pass every 300 ms, each driving the waiting Build again.
#[test]
fn an_idle_fabric_primary_with_a_waiting_drive_runs_no_reconcile_pass_without_an_event() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes, attempt) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-idle-runs-no-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&r.env, &wakes, None);
        tokio::time::sleep(SETTLE + Duration::from_secs(2)).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes, r.builds.read_build(&r.build).await.unwrap().attempt)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First], "the loop's first pass is its only one while nothing happens: {passes:?}");
    assert_eq!(drive_passes(&spans, &trace_id), 1, "the waiting Build is driven once");
    assert_eq!(attempt, 1, "attempt 1 was handed back and no other is claimed");
}

/// CONTRACT (Q253 amended: "a view install (`Records::view_tick`) ... runs one reconcile pass, which
/// recomputes the world from the live view and claims the next attempt the first time it differs"): one
/// view install that moves what the waiting drive decides on (the leaving mesh's primary) runs exactly
/// one pass, which claims attempt 2.
/// Must NOT happen: the install unseen until a tick, or passes beyond the one it woke.
#[test]
fn one_view_install_runs_one_reconcile_pass_which_claims_the_next_attempt() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes, attempt, dispatched) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-view-install-runs-one-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&r.env, &wakes, None);
        tokio::time::sleep(SETTLE).await;
        let mut moved = r.env.topology.read().await.clone();
        for n in moved.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.1") {
            n.status = NodeStatus::PendingReconnect;
            n.is_primary = false;
        }
        wakes.records.install_projected(&r.env.topology, || moved).await;
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        let dispatched = r.dispatch.dispatched.lock().unwrap().len();
        (trace_id, passes, r.builds.read_build(&r.build).await.unwrap().attempt, dispatched)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::ViewInstalled], "one install, one pass: {passes:?}");
    assert_eq!(drive_passes(&spans, &trace_id), 2, "the Build is driven by the first pass and the install's");
    assert_eq!((attempt, dispatched), (2, 2), "the install's pass claims and dispatches attempt 2");
}

/// CONTRACT (Q253 amended: "a Build accepted or an attempt opened (`build_submitted`)" runs one pass):
/// one submit, notified as the control plane notifies it, runs exactly one pass; the waiting drive's
/// world did not move, so it claims nothing.
/// Must NOT happen: passes beyond the one the submit woke.
#[test]
fn one_submit_runs_one_reconcile_pass() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes, attempt) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-submit-runs-one-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&r.env, &wakes, None);
        tokio::time::sleep(SETTLE).await;
        wakes.submitted.notify_waiters();
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes, r.builds.read_build(&r.build).await.unwrap().attempt)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::Submitted], "one submit, one pass: {passes:?}");
    assert_eq!(drive_passes(&spans, &trace_id), 2);
    assert_eq!(attempt, 1, "nothing the drive decides on moved: no attempt is claimed");
}

/// CONTRACT (Q253 amended: "a fabric state change (a watch set in `set_fabric_state`)" runs one pass):
/// the fabric state set by `Records::set_fabric_state` runs exactly one pass, and setting the same
/// state again runs none.
/// Must NOT happen: the change unseen until a tick, or a pass for a state that did not change.
#[test]
fn one_fabric_state_change_runs_one_reconcile_pass() {
    use rdm_admin::admin::Woken;
    use rdm_admin::fabric_state::FabricState;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-fabric-state-runs-one-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&r.env, &wakes, None);
        tokio::time::sleep(SETTLE).await;
        wakes.records.set_fabric_state(FabricState::Ready);
        tokio::time::sleep(SETTLE).await;
        wakes.records.set_fabric_state(FabricState::Ready);
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::FabricState], "one change, one pass; the same state again, none: {passes:?}");
    assert_eq!(drive_passes(&spans, &trace_id), 2);
}

/// CONTRACT (Q253 amended: anything the 300 ms arm silently carried is carried by an event; the drift
/// check and the drive read the peer-mesh investigation's hold and decision, R-D1): the ladder moving
/// (a peer mesh tracked as unheard) runs exactly one pass.
/// Must NOT happen: the investigation's move unseen until an unrelated event.
#[test]
fn a_move_of_the_peer_mesh_investigation_runs_one_reconcile_pass() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-peer-mesh-move-runs-one-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&r.env, &wakes, None);
        tokio::time::sleep(SETTLE).await;
        let track = Wakes::rungs().track;
        let steps = wakes.ladder.lock().unwrap().step("mesh2", track);
        assert!(matches!(steps.as_slice(), [rdm_admin::investigate::Step::Track { .. }]), "the ladder tracks mesh2: {steps:?}");
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::PeerMesh], "one move of the ladder, one pass: {passes:?}");
    assert_eq!(drive_passes(&spans, &trace_id), 2);
}

/// CONTRACT (Q253 amended; the drift check opens no attempt while a Build is in progress, so the end
/// of the Build is what it waits on): a drive that ends its Build runs exactly one more pass.
/// Must NOT happen: the Build's end unseen until an unrelated event.
#[test]
fn a_drive_that_ends_its_build_runs_one_more_reconcile_pass() {
    use rdm_admin::admin::Woken;
    use rdm_admin::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes, state) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-drive-end-runs-one-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let r = rig().await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&r.env, &wakes, None);
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes, r.builds.read_build(&r.build).await.unwrap().state)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(state, BuildState::Complete, "the first pass's drive completes the Build");
    assert_eq!(passes, vec![Woken::First, Woken::DriveEnded], "the Build's end runs one more pass: {passes:?}");
    assert_eq!(drive_passes(&spans, &trace_id), 1, "the completed Build is not driven again");
}

/// CONTRACT (Q253 amended: "a change during the hand-back is read at the next event, never missed"): a
/// submit notified while a pass runs (`notify_waiters`, as the control plane notifies) runs the next
/// pass once that one ends.
/// Must NOT happen: an event during a pass lost because the loop was not yet waiting.
#[test]
fn a_submit_during_a_reconcile_pass_runs_the_next_pass() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-submit-during-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        let wakes = Wakes::new();
        let hold = Arc::new(tokio::sync::Semaphore::new(0));
        let (passes, task) = reconciler(&r.env, &wakes, Some(hold.clone()));
        // The first pass is held at its start; the submit lands while it runs.
        while passes.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        wakes.submitted.notify_waiters();
        hold.add_permits(1);
        tokio::time::sleep(SETTLE).await;
        hold.add_permits(1);
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::Submitted], "the submit during the first pass runs the second, and nothing else runs: {passes:?}");
    assert_eq!(drive_passes(&spans, &trace_id), 2);
}

/// An executor's run of a mesh's leave that waits for the cell, then ends incomplete with nothing moved.
struct HeldThenIncomplete {
    began: tokio::sync::watch::Sender<bool>,
    let_go: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl OperationRunner for HeldThenIncomplete {
    async fn run(&self, _build: &BuildId, _lane: Option<&str>, _attempt: u32, _op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.began.send_replace(true);
        self.let_go.acquire().await.expect("the cell never closes its semaphore").forget();
        Err(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))
    }
}

/// CONTRACT (Q253 amended: "a change during the hand-back is read at the next event, never missed"):
/// an event whose pass finds the Build's drive still running (it read the world before the event and
/// ends waiting) runs that drive again once its run ends. The leave's executor is held while the event
/// (a submit) runs its pass; let go, the attempt ends incomplete with nothing moved and the drive
/// waits, and the drive runs once more for the event its pass could not hand it.
/// Must NOT happen: the event spent on a running drive, so a drive that read the world before it waits
/// for the next event (rdm#164's gate: a Day-0 Build left waiting for the fabric's ready state that was
/// set while its drive ran).
#[test]
fn an_event_whose_pass_finds_the_drive_running_runs_the_drive_again_once_it_ends() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes, attempt) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-event-during-drive");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let mut t = view();
        t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
        t.nodes.push(node("mesh2.admin.1", true, false));
        t.nodes.push(node("mesh2.rpc.1", false, false));
        let (began, mut running) = tokio::sync::watch::channel(false);
        let let_go = Arc::new(tokio::sync::Semaphore::new(0));
        let held = let_go.clone();
        let r = rig_for(t, TopologyChange::RemoveMesh { mesh: "mesh2".into() }, move |_, _| -> Arc<dyn OperationRunner> { Arc::new(HeldThenIncomplete { began, let_go: held }) }).await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&r.env, &wakes, None);
        tokio::time::timeout(Duration::from_secs(5), running.wait_for(|b| *b)).await.expect("attempt 1 never reached its executor").unwrap();
        wakes.submitted.notify_waiters();
        while passes.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let_go.add_permits(8);
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes, r.builds.read_build(&r.build).await.unwrap().attempt)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::Submitted], "{passes:?}");
    assert_eq!(attempt, 1, "attempt 1 was handed back with nothing moved: no other is claimed");
    assert_eq!(drive_passes(&spans, &trace_id), 2, "the drive runs again for the submit its pass found it running for");
}

/// CONTRACT (Q254: the reconcile loop "wakes on events (... a provider exit)"; a drive's departure proof
/// reads the provider's exit): a runtime the admin's provider started, killed from outside, runs exactly
/// one pass once the provider reaps its exit.
/// Must NOT happen: the exit unseen until an unrelated event.
#[test]
fn a_provider_exit_runs_one_reconcile_pass() {
    use rdm_admin::admin::Woken;
    use rdm_admin::deployment::provider::DeploymentProvider;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-provider-exit-runs-one-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        let wakes = Wakes::new();
        let dir = std::env::temp_dir().join(format!("reconcile-provider-exit-{}", MeshId::mint()));
        let spec = rdm_admin::deployment::provider::ResolvedNodeLaunch { secrets: Default::default(), withhold: Vec::new(), node: "mesh1.rpc.9".parse().unwrap(), deployment_id: rdm_admin::model::DeploymentId::mint(), executable: "/bin/sleep".into(), args: vec!["30".into()], env: Default::default(), data_dir: dir.clone(), transport: "127.0.0.1:0".parse().unwrap(), listeners: vec![] };
        let h = wakes.provider.spawn(&spec).await.expect("the runtime starts");
        let (passes, task) = reconciler(&r.env, &wakes, None);
        tokio::time::sleep(SETTLE).await;
        // SAFETY: kill(2) of the child this cell's provider started, by its exact pid.
        assert_eq!(unsafe { libc::kill(h.pid.expect("a pid") as i32, libc::SIGKILL) }, 0, "the runtime was alive to kill");
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let _ = std::fs::remove_dir_all(&dir);
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::ProviderExit], "one exit, one pass: {passes:?}");
    assert_eq!(drive_passes(&spans, &trace_id), 2);
}

struct FailOnAttempt {
    inner: Arc<Creates>,
    fail_attempt: u32,
}

#[async_trait::async_trait]
impl OperationRunner for FailOnAttempt {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        if attempt == self.fail_attempt {
            return Err(rdm_admin::executor::RunEnd::failed("reopened stop attempt failed on node.stop".to_string()));
        }
        self.inner.run(build, lane, attempt, op).await
    }
}

// @feature: on-build-started-ended
/// CONTRACT: docs/architecture/fabric-node-hooks.md:139
/// "Each time a Build's first attempt is claimed, or an attempt that reopens a Build that has
/// ended is claimed, the fabric-primary sends the application one `on_build_started` notice ...
/// An attempt that continues a Build in progress (a takeover, or the next attempt after a
/// hand-back) sends none. Each `on_build_started` is followed by one `on_build_ended` for the
/// same opening."
/// docs/architecture/fabric-node-hooks.md:143
/// "Each time a Build's terminal state is recorded, the fabric-primary sends the application one
/// `on_build_ended` notice ... its state, `Complete` or `Failed { step, reason, code }`"
#[tokio::test]
async fn build_drive_fires_on_build_started_on_opening_and_reopening_skips_takeover_and_fires_on_build_ended_on_terminals() {
    let change = TopologyChange::ReconcileMesh {
        desired: MeshDesired::of(
            "mesh1".to_string(),
            [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)],
        ),
    };
    let r = rig_for(view(), change, |ran, _| {
        Arc::new(FailOnAttempt {
            inner: ran,
            fail_attempt: 3,
        })
    })
    .await;

    // 1. Attempt 1 is claimed for `mesh1.admin.2` (which opens the Build -> `on_build_started` for
    //    attempt 1 with `AttemptReason::Requested`), then `mesh1.admin.2` is unreachable and proven
    //    exited so attempt 2 takes over on `mesh1.admin.1` without sending a second `on_build_started`,
    //    and completes -> `on_build_ended` for attempt 2 with `BuildEndedState::Complete`.
    r.dispatch.lose("mesh1.admin.2");
    let drive1 = Drive::detached(r.build.clone());
    assert!(matches!(r.env.run(&drive1).await, DriveEnd::Stalled(_)));
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
    assert_eq!(r.env.run(&drive1).await, DriveEnd::Terminal);

    // 2. Reopen the completed Build at attempt 3 with `AttemptReason::Stop` and `AttemptAction::Stop`
    //    and run to failure.
    let path: PathName = "mesh1.rpc.1".parse().unwrap();
    let (node_id, from_incarnation) = r
        .env
        .topology
        .read()
        .await
        .node(&path)
        .and_then(|n| n.incarnation_id.clone().map(|i| (n.node_id.clone(), i)))
        .expect("mesh1.rpc.1 has a birth");
    let action = rdm_admin::accepted::AttemptAction::Stop {
        path,
        node_id,
        from_incarnation,
    };
    r.builds
        .open_attempt(&rdm_admin::build_state::AttemptOpened {
            mesh: None,
            build_id: r.build.clone(),
            attempt: 3,
            reason: rdm_admin::build_state::AttemptReason::Stop,
            action: Some(action),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 2_000,
            context: None,
        })
        .await
        .unwrap();
    let drive2 = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive2).await, DriveEnd::Terminal);

    let told = r.told.settled(4).await;
    let keys: Vec<(String, u32, &str)> = told.iter().map(crate::Notice::key).collect();
    assert_eq!(
        keys,
        vec![
            (r.build.to_string(), 1, "started"),
            (r.build.to_string(), 2, "complete"),
            (r.build.to_string(), 3, "started"),
            (r.build.to_string(), 3, "failed"),
        ],
        "attempt 1 opens, takeover attempt 2 sends no started and ends Complete, attempt 3 reopens and ends Failed: {told:?}"
    );
    match &told[0] {
        crate::Notice::Started(c) => {
            assert_eq!(c.reason, rdm_admin::build_state::AttemptReason::Requested);
        }
        other => panic!("expected Started at index 0, got {other:?}"),
    }
    match &told[2] {
        crate::Notice::Started(c) => {
            assert_eq!(c.reason, rdm_admin::build_state::AttemptReason::Stop);
        }
        other => panic!("expected Started at index 2, got {other:?}"),
    }
    match &told[3] {
        crate::Notice::Ended(c) => {
            assert_eq!(c.reason, rdm_admin::build_state::AttemptReason::Stop);
            assert!(
                matches!(&c.state, rdm_admin::wiring::BuildEndedState::Failed { reason, .. } if reason.contains("reopened stop attempt failed")),
                "expected Failed state on attempt 3, got {:?}",
                c.state
            );
        }
        other => panic!("expected Ended at index 3, got {other:?}"),
    }
}

struct CutOnceThenFailOnAttempt {
    inner: Arc<Creates>,
    cut_once: AtomicBool,
    fail_attempt: u32,
}

#[async_trait::async_trait]
impl OperationRunner for CutOnceThenFailOnAttempt {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        if attempt == self.fail_attempt {
            return Err(rdm_admin::executor::RunEnd::failed("injected executor step failure".to_string()));
        }
        self.inner.run(build, lane, attempt, op).await?;
        if self.cut_once.swap(false, Ordering::SeqCst) {
            return Err(rdm_admin::executor::RunEnd::Incomplete("injected mid-run cut after first operation".to_string()));
        }
        Ok(())
    }
}

// @feature: build-op
/// CONTRACT: docs/architecture/build.md:179, 240-245
/// "Every execution attempt re-reads the accepted topology and current realized/observed state and
/// computes remaining work. Completed step receipts prevent repeating already-satisfied work; a
/// Build never resumes from a mutable instruction pointer. A step receipt is `(build_id, attempt,
/// operation, step)` with its output ... Each workflow's `AttemptOpened` starts a run; a resume
/// after the executor is lost (R-BI4) continues that run; a receipt is reused only within its run."
#[tokio::test]
async fn build_drive_step_faults_recover_or_fail_as_the_page_specifies() {
    let change = TopologyChange::ReconcileMesh {
        desired: MeshDesired::of(
            "mesh1".to_string(),
            [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 2)],
        ),
    };
    let r = rig_for(view(), change, |ran, _| {
        Arc::new(CutOnceThenFailOnAttempt {
            inner: ran,
            cut_once: AtomicBool::new(true),
            fail_attempt: 4,
        })
    })
    .await;

    let (capture, _guard) = crate::receipt_refusal::Capture::new("build-drive-step-faults");
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);

    // Fault 1: Executor `mesh1.admin.2` is unreachable without departure proof.
    // Attempt 1 is claimed for `mesh1.admin.2`, the drive stalls naming the unproven departure,
    // no other admin is claimed, nothing runs, and no `Blocked` frame is emitted to the caller.
    r.dispatch.lose("mesh1.admin.2");
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("departure is not proven"), "{why}"),
        other => panic!("unproven executor silence must stall the drive: {other:?}"),
    }
    let p1 = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p1.attempt, p1.executor.as_deref()), (1, Some("mesh1.admin.2")));
    assert!(r.ran.0.lock().unwrap().is_empty());
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new());

    // Fault 2: Executor `mesh1.admin.2` is proven exited (`r.proofs.exited`), `mesh1.admin.1` takes
    // the mesh-primary role, and attempt 2 is cut mid-run after completing the first operation
    // (`node.create:mesh1.admin.2`). On the next pass, the completed receipt is reused (not run
    // twice) and the remaining operation (`node.create:mesh1.rpc.2`) completes (`DriveEnd::Terminal`).
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
    assert_eq!(
        *r.ran.0.lock().unwrap(),
        vec![
            "node.create:mesh1.admin.2".to_string(),
            "node.create:mesh1.rpc.2".to_string(),
        ],
        "completed operation is not re-run after mid-run cut and hand-back"
    );

    // Fault 3: Reopen the Build at attempt 4 (`fail_attempt: 4`) and inject an executor step failure.
    // The drive records the terminal failure and emits `on_build_ended` with `BuildEndedState::Failed`.
    let path: PathName = "mesh1.rpc.1".parse().unwrap();
    let (node_id, from_incarnation) = r
        .env
        .topology
        .read()
        .await
        .node(&path)
        .and_then(|n| n.incarnation_id.clone().map(|i| (n.node_id.clone(), i)))
        .unwrap();
    r.builds
        .open_attempt(&rdm_admin::build_state::AttemptOpened {
            mesh: None,
            build_id: r.build.clone(),
            attempt: 4,
            reason: rdm_admin::build_state::AttemptReason::Stop,
            action: Some(rdm_admin::accepted::AttemptAction::Stop {
                path,
                node_id,
                from_incarnation,
            }),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 3_000,
            context: None,
        })
        .await
        .unwrap();
    let drive_fail = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive_fail).await, DriveEnd::Terminal);

    let told = r.told.settled(4).await;
    let keys: Vec<(String, u32, &str)> = told.iter().map(crate::Notice::key).collect();
    assert_eq!(
        keys,
        vec![
            (r.build.to_string(), 1, "started"),
            (r.build.to_string(), 3, "complete"),
            (r.build.to_string(), 4, "started"),
            (r.build.to_string(), 4, "failed"),
        ],
        "takeover attempts 2 and 3 send no duplicate started notice; attempt 3 completes and reopened attempt 4 fails: {told:?}"
    );

    // Fault 4: Fabric-primary role is yielded/targeted (`target_to(Some("mesh1.admin.2"))`).
    // The drive refuses to claim or dispatch (`DriveEnd::RoleLost`) and ends the caller's stream
    // with `Read::RoleLost` naming the successor.
    r.switch.target_to(Some("mesh1.admin.2"));
    let drive_lost = Drive::detached(r.build.clone());
    let mut lost_reader = drive_lost.reader(1);
    assert!(matches!(r.env.run(&drive_lost).await, DriveEnd::RoleLost(why) if why.contains("is no longer the fabric-primary")));
    assert!(matches!(next_within(&mut lost_reader).await, Some(Read::RoleLost(loss)) if loss.to.as_deref() == Some("mesh1.admin.2")));
    assert!(!capture.spans("rdm.node_admin.build.update.via-drive").is_empty());
}
