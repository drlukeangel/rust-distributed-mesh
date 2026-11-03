//! The fabric-primary's drive of a Build, one cell per gate it passes and per way it can be stopped
//! (op `0x20`; node-rpc-envelope.md "Build, op `0x20`"): the Build state, the view and the
//! executors are fixtures, the drive is the product's.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.node-admin.1`, its mesh primary
//! `mesh1.node-admin.2` (so a node of `mesh1` is executed by an admin that is not the fabric-primary),
//! and `mesh1.node.1`. The Build grows `mesh1` by one node.

use rdm_admin::accepted::{AcceptedStore, FabricTopology};
use rdm_core::build::BuildChange;
use rdm_admin::build::{BuildId, BuildOperation, FabricDesired, MeshDesired};
use rdm_admin::build_drive::{Dispatcher, Drive, DriveEnd, DriveEnv, Gate, PrecedingUpdate, Read};
use rdm_admin::build_state::{BuildStateAdapter, BuildStateError, MemoryBuildStateAdapter};
use rdm_admin::executor::OperationRunner;
use rdm_admin::control_plane::ControlPlane;
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use rdm_core::build::BuildReply;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
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
        nodes: vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", false, true), node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", true, false), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", false, false)],
    }
}

/// A gate the cell opens and closes.
struct Switch {
    node_id: NodeId,
    targeted: AtomicBool,
    /// The admin the target names, when the cell sets one.
    successor: Mutex<Option<rdm_core::framing::Birth>>,
    /// Set with `targeted` by a cell that targets while a dispatch is in flight.
    target_tx: tokio::sync::watch::Sender<bool>,
    /// The primary the open primary record names (this birth's node ID), when the cell sets one.
    recorded_primary: Mutex<Option<NodeId>>,
    /// How long each peer mesh has gone unheard past the staleness floor, in milliseconds, when the cell
    /// sets it (R-D1).
    peer: Mutex<std::collections::HashMap<String, u64>>,
    /// The meshes whose Mesh record is Dead, when the cell sets them.
    dead: Mutex<std::collections::HashSet<MeshId>>,
}


impl Switch {
    /// Target the Build log now, naming `successor` as the admin the committed transfer in its role.
    fn target_to(&self, successor: Option<&rdm_core::framing::Birth>) {
        *self.successor.lock().unwrap() = successor.cloned();
        self.targeted.store(true, Ordering::SeqCst);
        self.target_tx.send_replace(true);
    }
}

#[async_trait::async_trait]
impl Gate for Switch {
    async fn target(&self) -> Result<Option<NodeId>, BuildStateError> {
        if self.targeted.load(Ordering::SeqCst) {
            Err(BuildStateError::RoleYielded { node: self.node_id.clone(), by: "leaving".into(), to: self.successor.lock().unwrap().clone() })
        } else {
            Ok(self.recorded_primary.lock().unwrap().clone())
        }
    }
    async fn target_set(&self) {
        let _ = self.target_tx.subscribe().wait_for(|f| *f).await;
    }
    fn mesh_unheard_ms(&self, mesh: &str) -> Option<u64> {
        self.peer.lock().unwrap().get(mesh).copied()
    }
    async fn deleted_meshes(&self, _observed: &Topology) -> Result<std::collections::HashSet<MeshId>, String> {
        Ok(self.dead.lock().unwrap().clone())
    }
}

/// Records what ran, over the fixture's Build store. Each admin's step log writes the operation's
/// `Complete` step event once it ran, as a pipeline's step runner does (`crate::loopback::Loopback`).
struct Creates(Mutex<Vec<rdm_core::OperationId>>, Arc<MemoryBuildStateAdapter>);

#[async_trait::async_trait]
impl OperationRunner for Creates {
    async fn run(&self, _build: &BuildId, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.0.lock().unwrap().push(op.key());
        Ok(())
    }
}

struct Rig {
    builds: Arc<MemoryBuildStateAdapter>,
    env: Arc<DriveEnv>,
    switch: Arc<Switch>,
    dispatch: Arc<crate::loopback::Loopback>,
    ran: Arc<Creates>,
    build: BuildId,
    /// The application the drive sends its Build notices to (Q256).
    told: crate::Told,
}

impl Rig {
    /// The node.id the rig's Build record minted for the node it creates at `at` (D58).
    async fn created(&self, at: &PathName) -> rdm_core::NodeId {
        let build = self.builds.read_build(&self.build).await.unwrap();
        build.topology.created_id(at).cloned().unwrap_or_else(|| panic!("the Build creates no node at {at:?}: {:?}", build.topology.created))
    }

    async fn birth(&self, path: &str, name: &str) -> rdm_core::framing::Birth {
        let location = PathName::new(path, name).unwrap();
        let topology = self.env.topology.read().await;
        let node = topology.node(&location).expect("fixture node exists at this location");
        rdm_core::framing::Birth { node_id: node.node_id.clone(), incarnation: node.incarnation_id.clone().expect("fixture node has a birth") }
    }
}

/// `v` in order: the created nodes' node.ids are minted at acceptance, so their keys sort as the ids do.
fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v
}

async fn rig() -> Rig {
    let change = move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh1".to_string(), [("node-admin", 2), ("node", 2)])]);
    rig_for(view(), change, |ran, _| ran).await
}

/// A Build of `change` over `t`, run by `runner(ran, topology)`: `ran` records what ran.
async fn rig_for(t: Topology, change: impl FnOnce(&Topology) -> BuildChange, runner: impl FnOnce(Arc<Creates>, Arc<RwLock<Topology>>) -> Arc<dyn OperationRunner>) -> Rig {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let fp = t.node(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).expect("the fabric-primary is in the view");
    let fp_birth = rdm_core::framing::Birth { node_id: fp.node_id.clone(), incarnation: fp.incarnation_id.clone().unwrap() };
    let fabric_id = t.fabric.id.clone();
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), &fp_birth).await.unwrap();
    let topology = Arc::new(RwLock::new(t.clone()));
    let fp_id = t.node(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).expect("the fabric-primary is in the view").node_id.clone();
    let mut cp = crate::control_plane(builds.clone(), accepted.clone(), rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), t, crate::adopted_time());
    cp.topology = topology.clone();
    let change = crate::named_request(&cp, change).await;
    let opened = cp.submit("test", change).await.unwrap();
    let ran = Arc::new(Creates(Mutex::new(Vec::new()), builds.clone()));
    let dispatch = crate::loopback::Loopback::new(builds.clone(), builds.clone(), topology.clone(), runner(ran.clone(), topology.clone()));
    let switch = Arc::new(Switch { node_id: fp_id.clone(), targeted: AtomicBool::new(false), successor: Mutex::new(None), target_tx: tokio::sync::watch::channel(false).0, recorded_primary: Mutex::new(None), peer: Mutex::new(Default::default()), dead: Mutex::new(Default::default()) });
    let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
    let told = crate::Told::default();
    let env = Arc::new(DriveEnv { me_mesh: "mesh1".into(),
        me_path: me.path.clone(), me_name: me.name.clone(),
        birth: fp_birth.clone(),
        topology: topology.clone(),
        builds: builds.clone(),
        dispatcher: dispatch.clone(),
        gate: switch.clone(),
        notices: crate::written_by(told.notices(), &*builds, &fabric_id, &fp_birth),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    Rig { builds, env, switch, dispatch, ran, build: opened.build_id, told }
}


/// `e` with its gate, its dispatcher, or both, replaced.
fn env_with(e: &DriveEnv, gate: Option<Arc<dyn Gate>>, dispatcher: Option<Arc<dyn Dispatcher>>) -> Arc<DriveEnv> {
    Arc::new(DriveEnv {
        me_mesh: e.me_mesh.clone(),
        me_path: e.me_path.clone(),
        me_name: e.me_name.clone(),
        birth: e.birth.clone(),
        topology: e.topology.clone(),
        builds: e.builds.clone(),
        dispatcher: dispatcher.unwrap_or_else(|| e.dispatcher.clone()),
        gate: gate.unwrap_or_else(|| e.gate.clone()),
        notices: e.notices.clone(),
        turns: e.turns.clone(),
    })
}

/// The `rdm.build.ended` states of `build`, in event order.
pub(crate) async fn ended_states(builds: &dyn BuildStateAdapter, build: &BuildId) -> Vec<rdm_admin::events_storage::EndedState> {
    let events = builds.events().of_build(build).await.unwrap();
    events.iter().filter(|e| e.r#type == rdm_admin::events_storage::BUILD_ENDED).filter_map(|e| serde_json::from_value(e.data.get("state")?.clone()).ok()).collect()
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
/// The Build log's target is checked before anything is started: the drive ends `RoleLost` naming the
/// target, the Build is not started, no section is sent, and the callers' streams end with the role loss (the caller
/// re-submits by the Build id at the new role).
#[tokio::test]
async fn a_fabric_primary_whose_record_names_another_birth_starts_and_dispatches_nothing() {
    let r = rig().await;
    r.switch.targeted.store(true, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    match r.env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("is no longer the fabric-primary: "), "{why}"),
        other => panic!("a targeted role ends the drive RoleLost: {other:?}"),
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Pending, "the Build was not started");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no section was sent");
    assert!(r.ran.0.lock().unwrap().is_empty());
    assert!(matches!(next_within(&mut reader).await, Some(Read::RoleLost(loss)) if loss.why.contains("leaving") && loss.to.is_none()));
}

/// CONTRACT: an admin knows the fabric-primary role is its own by the node id it was born with,
/// never by path.name. When its view shows another birth at its own path.name holding the role (a
/// successor or a deleted birth of that path), the drive ends `RoleLost` naming that birth, starts
/// nothing and sends no section, and the callers' streams end naming the primary the committed
/// primary record names (none here), never the view's.
#[tokio::test]
async fn a_fabric_primary_whose_view_shows_another_birth_of_its_path_as_fabric_primary_dispatches_nothing() {
    let r = rig().await;
    let other = rdm_core::NodeId::mint();
    {
        let mut t = r.env.topology.write().await;
        let fp = t.nodes.iter_mut().find(|n| n.is_fabric_primary).expect("the view shows a fabric-primary");
        assert_eq!(fp.location(), r.env.me(), "the role is shown at this admin's own path.name");
        fp.node_id = other.clone();
        fp.incarnation_id = Some(IncarnationId::mint());
    }
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    match r.env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("does not hold the fabric-primary role") && why.contains(other.as_str()), "the refusal names the birth that holds the primary role: {why}"),
        other => panic!("another birth of this path holding the primary role ends the drive RoleLost: {other:?}"),
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Pending, "the Build was not started");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no section was sent");
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
            n.is_fabric_primary = n.location() == rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        }
    }
    *r.switch.recorded_primary.lock().unwrap() = Some(r.switch.node_id.clone());
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    match r.env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("does not hold the fabric-primary role") && why.contains(&format!("the fabric-primary record it holds names {}", r.switch.node_id)), "the refusal names what the record names: {why}"),
        other => panic!("a view showing another primary ends the drive RoleLost: {other:?}"),
    }
    let loss = match next_within(&mut reader).await {
        Some(Read::RoleLost(loss)) => loss,
        _ => panic!("the callers' streams end with the role loss"),
    };
    assert_eq!(loss.to.as_ref(), Some(&r.switch.node_id), "the role loss names the record's primary, not the view's mesh1.node-admin.2: {loss:?}");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
    *r.switch.recorded_primary.lock().unwrap() = None;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
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
    let r = rig_for(view(), move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh1".to_string(), [("node-admin", 2), ("node", 2)])]), |ran, _| ran).await;
    assert!(r.env.topology.read().await.node(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).is_none(), "the view holds no row of the new primary");
    let successor_id = NodeId::mint();
    r.switch.target_to(Some(&rdm_core::framing::Birth { node_id: successor_id.clone(), incarnation: IncarnationId::mint() }));
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    match r.env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("is no longer the fabric-primary: "), "the target refused it, not the view: {why}"),
        other => panic!("a targeted role ends the drive RoleLost: {other:?}"),
    }
    assert!(matches!(next_within(&mut reader).await, Some(Read::RoleLost(loss)) if loss.to.as_ref() == Some(&successor_id)));
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
    t.nodes.push(node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false));
    let r = rig_for(t, move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh1".to_string(), [("node-admin", 2), ("node", 2)])]), |ran, _| ran).await;
    assert_eq!(r.env.topology.read().await.fabric_primary().map(|n| n.node_id.clone()), Some(r.switch.node_id.clone()), "the view still shows the old role");
    *r.switch.successor.lock().unwrap() = Some(r.birth("mesh2.node-admin", "1").await);
    r.switch.targeted.store(true, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    assert!(matches!(r.env.run(&drive).await, DriveEnd::RoleLost(_)), "a targeted role ends the drive RoleLost");
    match next_within(&mut reader).await {
        Some(Read::RoleLost(loss)) => assert_eq!(loss.to.as_ref(), Some(&r.birth("mesh2.node-admin", "1").await.node_id), "the stream names the committed successor: {loss:?}"),
        other => panic!("the reader ends with the role loss, got {other:?}"),
    }
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
}

/// Puts the node-admin a create of mesh2 made in the view, live, as mesh2's primary: the mesh's only
/// admin was exited, so the first admin the Build creates takes the role.
struct Recovered {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for Recovered {
    async fn run(&self, build: &BuildId, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.run(build, op).await?;
        if let BuildOperation::CreateNode { path, name, node_id, .. } = op {
            if path == "mesh2.node-admin" {
                let mut created = node(rdm_core::PathName::new(path, name).unwrap(), "mesh2", "node-admin", true, false);
                created.node_id = node_id.clone();
                self.topology.write().await.nodes.push(created);
            }
        }
        Ok(())
    }
}

/// CONTRACT (R-D1: "Loss of peer-mesh control-plane visibility targets lifecycle mutation for every
/// node in that mesh"; build.md §4.4: "The node's mesh is heard ... refused by name, naming the mesh and
/// how long it has gone unheard"): mesh2's only node-admin is proven exited, so the fabric-primary of
/// mesh1 is the executor of mesh2's work in its view, and mesh2 is unheard past the staleness floor.
/// The drive runs nothing and sends no section for mesh2, and says so on one span per operation naming
/// the mesh, the operation and how long the mesh is unheard, never on the caller's stream
/// (node-rpc-envelope.md: Blocked's only source is the subject's Status `Blocked`). Once mesh2 is heard
/// again, the Build runs.
#[tokio::test]
async fn a_peer_mesh_held_unheard_is_never_dispatched_to_until_it_is_heard_again() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    let mut lost = node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", false, false);
    lost.status = NodeStatus::Dead;
    t.nodes.push(lost);
    t.nodes.push(node(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", true, false));
    let change = move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh2".to_string(), [("node-admin", 2), ("node", 2)])]);
    let r = rig_for(t, change, |ran, topology| Arc::new(Recovered { ran, topology })).await;
    r.switch.peer.lock().unwrap().insert("mesh2".into(), 31_000);
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3016-unheard-mesh-drive");
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();

    // Held unheard: nothing is run or dispatched for mesh2.
    match r.env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("mesh2") && why.contains("31000"), "the stall names the mesh and how long it is unheard: {why}"),
        other => panic!("the fabric-primary runs nothing for a peer mesh held unheard: {other:?}"),
    }
    assert!(r.builds.read_build(&r.build).await.unwrap().steps.is_empty(), "no step was written across the target");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty() && r.ran.0.lock().unwrap().is_empty(), "nothing dispatched, nothing run");
    // Each of mesh2's operations is named once, on its own span: the mesh, the operation, the node it
    // acts on and how long the mesh is unheard.
    let targeted = capture.spans("rdm.node_admin.build.reject.via-unheard-mesh-drive");
    let (admin2, node2) = (r.created(&rdm_core::PathName::new("mesh2.node-admin", "2").unwrap()).await, r.created(&rdm_core::PathName::new("mesh2.node", "2").unwrap()).await);
    let named: Vec<_> = targeted.iter().map(|(_, a)| (a.get("mesh").cloned(), a.get("operation").cloned(), a.get("target_path").cloned(), a.get("target_name").cloned(), a.get("unheard_ms").cloned())).collect();
    let span_of = |op: &rdm_core::OperationId, path: &str| (Some("mesh2".to_string()), Some(op.to_string()), Some(path.to_string()), Some("2".to_string()), Some("31000".to_string()));
    assert_eq!(sorted(named), sorted(vec![span_of(&rdm_core::OperationId::node("node.create", &admin2), "mesh2.node-admin"), span_of(&rdm_core::OperationId::node("node.create", &node2), "mesh2.node")]), "each operation on the unheard mesh is named once: {targeted:?}");
    eprintln!("span rdm.node_admin.build.reject.via-unheard-mesh-drive trace_id={} {:?}", targeted[0].0, targeted[0].1);
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the target on the unheard mesh is named on its span and frames no Blocked to the caller");

    // Heard again: the Build runs, and mesh2's own primary runs the rest of the mesh.
    r.switch.peer.lock().unwrap().remove("mesh2");
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec![rdm_core::OperationId::node("node.create", &admin2), rdm_core::OperationId::node("node.create", &node2)], "the mesh's admin first, then its node");
    let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
    assert_eq!(dispatched.last().map(|(x, _)| x), Some(&r.birth("mesh2.node-admin", "2").await), "the mesh's own primary runs its nodes once it has one: {dispatched:?}");
}


/// The gate of a fabric-primary whose node.fabric-primary.update moves its role to `mesh2.node-admin.1`:
/// `update_primaries` answers once the successor's installation is acknowledged, after which the committed
/// record names `mesh2.node-admin.1` and the view shows it holding the role. Every other gate is `inner`'s.
struct RoleMoves {
    inner: Arc<Switch>,
    topology: Arc<RwLock<Topology>>,
    /// Every move asked: `(primary, mesh-primary role, fabric-primary role, operation)`.
    moved: Mutex<Vec<(NodeId, bool, bool, rdm_core::OperationId)>>,
}

#[async_trait::async_trait]
impl Gate for RoleMoves {
    async fn target(&self) -> Result<Option<NodeId>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn update_primaries(&self, m: &rdm_admin::executor::RoleMove, _: &BuildId, operation: &rdm_core::OperationId) -> Result<(), rdm_admin::build_drive::RoleMoveEnd> {
        self.moved.lock().unwrap().push((m.primary.node_id.clone(), m.mesh_role, m.fabric_role, operation.clone()));
        for n in self.topology.write().await.nodes.iter_mut() {
            n.is_fabric_primary = n.location() == rdm_core::PathName::new("mesh2.node-admin", "1").unwrap();
        }
        let topology = self.topology.read().await;
        let successor = topology.node(&PathName::new("mesh2.node-admin", "1").unwrap()).expect("fixture successor");
        self.inner.target_to(Some(&rdm_core::framing::Birth { node_id: successor.node_id.clone(), incarnation: successor.incarnation_id.clone().unwrap() }));
        Ok(())
    }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The last
/// node-admin of a mesh": "If the mesh holds the fabric-primary role, that role moves first by
/// `node.fabric-primary.update` with the departing mesh excluded (mesh-delete.md step 1)"; C1 step 1:
/// "The primary is commanded from above, as a step of the Build"; step 8: "Only then does the commanding
/// Build continue"): a mesh delete whose fabric-primary sits inside the leaving mesh moves the fabric
/// role alone (the mesh-primary role stays, mesh.delete never updates it) from the drive, as a step of the
/// Build, and once the move answered the drive ends `RoleLost` naming the new primary, which drives the
/// rest of the Build from its events.
/// Must NOT happen: an operation dispatched at the leaving mesh's admin, the delete run there, or the
/// Build ended by the old primary.
#[tokio::test]
async fn a_mesh_delete_whose_fabric_primary_sits_inside_moves_the_fabric_primary_alone_and_ends_at_the_new_one() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    t.nodes.push(node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false));
    let r = rig_for(t, move |_: &rdm_admin::topology::Topology| rdm_core::build::BuildChange::Delete { meshes: vec!["mesh1".into()], node_ids: Vec::new() }, |ran, _| ran).await;
    let gate = Arc::new(RoleMoves { inner: r.switch.clone(), topology: r.env.topology.clone(), moved: Mutex::new(Vec::new()) });
    let env = env_with(&r.env, Some(gate.clone()), None);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    match env.run(&drive).await {
        DriveEnd::RoleLost(why) => assert!(why.contains("is no longer the fabric-primary: "), "the drive ends at the target: {why}"),
        other => panic!("the old primary's drive ends RoleLost once the role moved: {other:?}"),
    }
    let moved = gate.moved.lock().unwrap().clone();
    assert_eq!(moved.len(), 1, "one node.fabric-primary.update: {moved:?}");
    let (primary, mesh_role, fabric_role, operation) = &moved[0];
    assert_eq!((primary, *mesh_role, *fabric_role), (&r.switch.node_id, false, true), "the fabric-primary role alone moves from the fabric-primary: {moved:?}");
    assert!(operation.action == "mesh.delete", "the update names the mesh delete it precedes: {operation:?}");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no operation was dispatched: {:?}", r.dispatch.dispatched.lock().unwrap());
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran at the leaving mesh's admin: {:?}", r.ran.0.lock().unwrap());
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Running, "the old primary started the Build and never ended it: the new primary drives the rest");
    assert!(ended_states(&*r.builds, &r.build).await.is_empty(), "no rdm.build.ended was written by the old primary");
    assert!(matches!(next_within(&mut reader).await, Some(Read::RoleLost(loss)) if loss.to.as_ref() == Some(&r.birth("mesh2.node-admin", "1").await.node_id)), "the callers' streams end naming the new role");
}

/// Primaries `mesh2.node-admin.1` as mesh2's primary when its create has run, as the launch and join of the
/// first admin of a mesh do.
struct MeshBirth {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for MeshBirth {
    async fn run(&self, build: &BuildId, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.run(build, op).await?;
        if op.created_node() == Some(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()) {
            self.topology.write().await.nodes.push(node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false));
        }
        Ok(())
    }
}

/// CONTRACT: the members of a mesh are run by that mesh's own primary once it exists. The
/// fabric-primary creates mesh2's first admin; the view then names that admin the executor of the
/// operations after it, so the fabric-primary's run ends there and the drive sends the rest of mesh2's
/// section to mesh2's primary, which runs it.
#[tokio::test]
async fn a_run_that_creates_a_meshs_first_admin_hands_the_rest_of_the_mesh_to_it() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    let change = move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh2".to_string(), [("node-admin", 1), ("node", 1)])]);
    let r = rig_for(t, change, |ran, topology| Arc::new(MeshBirth { ran, topology })).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec![rdm_core::OperationId::node("node.create", &r.created(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&rdm_core::PathName::new("mesh2.node", "1").unwrap()).await)]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![(r.birth("mesh1.node-admin", "1").await, "mesh2".to_string()), (r.birth("mesh2.node-admin", "1").await, "mesh2".to_string())], "mesh2's own primary ran the members");
}

/// Runs nothing to its end: every operation is held open, and the cell is told when the run began.
struct HeldOpen {
    began: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl OperationRunner for HeldOpen {
    async fn run(&self, _: &BuildId, _: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.began.notify_one();
        std::future::pending().await
    }
}

/// CONTRACT (lifecycles/node-primary-update.md step 4): the fabric-primary record this admin holds is
/// the target every decision is judged against. The target is set while the drive waits on a mesh-primary's
/// section, and the record names another primary than that mesh-primary: the drive ends `RoleLost` at the
/// target, without waiting for that section's stream to end, the callers' streams end naming the primary
/// the record names, and nothing more is sent.
#[tokio::test]
async fn a_fabric_primary_record_naming_another_birth_than_the_executor_ends_the_drive_naming_that_birth() {
    let held = Arc::new(HeldOpen { began: tokio::sync::Notify::new() });
    let runner = held.clone();
    let r = rig_for(view(), move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh1".to_string(), [("node-admin", 2), ("node", 2)])]), move |_, _| -> Arc<dyn OperationRunner> { runner }).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    let (env, d) = (r.env.clone(), drive.clone());
    let running = tokio::spawn(async move { env.run(&d).await });
    held.began.notified().await;
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![(r.birth("mesh1.node-admin", "2").await, "mesh1".to_string())], "mesh1's section is running at its mesh-primary");
    let successor_id = NodeId::mint();
    r.switch.target_to(Some(&rdm_core::framing::Birth { node_id: successor_id.clone(), incarnation: IncarnationId::mint() }));
    let end = tokio::time::timeout(Duration::from_secs(5), running).await.expect("the drive ends at the target while the executor's run is still open").unwrap();
    assert!(matches!(&end, DriveEnd::RoleLost(why) if why.contains("is no longer the fabric-primary: ")), "the target ends the drive RoleLost: {end:?}");
    match next_within(&mut reader).await {
        Some(Read::RoleLost(loss)) => assert_eq!(loss.to.as_ref(), Some(&successor_id), "the stream names the primary the record names: {loss:?}"),
        other => panic!("the reader ends with the role loss, got {other:?}"),
    }
    assert_eq!(r.dispatch.dispatched.lock().unwrap().len(), 1, "nothing more was dispatched");
    assert!(ended_states(&*r.builds, &r.build).await.is_empty(), "the old primary wrote no rdm.build.ended");
}

/// A mesh-primary's section the cell feeds item by item: `dispatch` hands the drive the stream and says
/// so.
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
    async fn dispatch(&self, _: &BuildId, _: &str, _: rdm_core::context::CallContext) -> rdm_admin::build_drive::Dispatched {
        let items = self.items.lock().unwrap().take().expect("one dispatch");
        self.dispatched.notify_one();
        rdm_admin::build_drive::Dispatched::Stream(Box::new(FedStream(items)))
    }
    async fn run_own(&self, _: &rdm_admin::stop_gate::Turn, _: &BuildId, mesh: &str, _: rdm_core::context::CallContext, _: Vec<BuildOperation>) -> rdm_admin::build_drive::Dispatched {
        panic!("the fabric-primary runs no section of mesh {mesh} itself in this cell")
    }
}

/// CONTRACT (node-rpc-envelope.md 0x20: "A fabric-primary change ends the outer stream with
/// FabricPrimaryMoved, **not Indeterminate**"; FabricPrimaryMoved: "caller resubmits the same build_id
/// there"): the target is set while the drive waits on mesh1's section, and the record names mesh1's
/// primary, which runs it: the section's own node.primary.update made it fabric-primary. It is the
/// fabric-primary once it has installed the election's result, and it says so on the section's own
/// stream, which it ends with FabricPrimaryMoved naming itself. Until then the callers' streams carry the
/// section's frames and no role loss; on its FabricPrimaryMoved the drive ends `RoleLost` naming it, and
/// writes no end of the Build (the new fabric-primary drives the rest from the events).
/// Must NOT happen: a caller's stream ending with the role loss, which sends the caller to the
/// mesh-primary, before its section said it holds the role.
#[tokio::test]
async fn a_fabric_primary_record_naming_the_executor_ends_the_drive_on_its_fabric_primary_moved_frame_never_before() {
    use rdm_admin::build_drive::InnerItem;
    let r = rig().await;
    let (feed, items) = tokio::sync::mpsc::unbounded_channel();
    let fed = Arc::new(Fed { items: Mutex::new(Some(items)), dispatched: tokio::sync::Notify::new() });
    let e = &r.env;
    let env = env_with(&e, None, Some(fed.clone()));
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    let (e, d) = (env.clone(), drive.clone());
    let running = tokio::spawn(async move { e.run(&d).await });
    tokio::time::timeout(Duration::from_secs(5), fed.dispatched.notified()).await.expect("mesh1's section is sent");
    // The section's PrimaryUpdated step reached the primary, whose row is the target.
    r.switch.target_to(Some(&r.birth("mesh1.node-admin", "2").await));
    let step = BuildReply::Step {
        build_id: r.build.to_string(),
        mesh: "mesh1".into(),
        operation: rdm_core::OperationId::node("node.stop", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap())).to_string(),
        step: "PrimaryUpdated".into(),
        result: rdm_core::build::StepResult::Complete,
    };
    feed.send(InnerItem::Frame(step.clone())).unwrap();
    match next_within(&mut reader).await {
        Some(Read::Frame(f)) => assert_eq!(f, step, "the run's frame reaches the callers"),
        other => panic!("before the executor says it holds the primary role, the callers read the run's frames and no role loss, got {other:?}"),
    }
    assert!(next_within(&mut reader).await.is_none(), "no role loss before the executor's new-primary frame");
    assert!(!running.is_finished(), "the drive waits for the executor's new-primary frame");
    // The executor installed the election's result: its run's stream ends FabricPrimaryMoved.
    feed.send(InnerItem::Frame(BuildReply::FabricPrimaryMoved { build_id: r.build.to_string(), fabric_primary: Some(r.birth("mesh1.node-admin", "2").await.node_id) })).unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), running).await.expect("the drive ends on the executor's new-primary frame").unwrap();
    assert!(matches!(&end, DriveEnd::RoleLost(why) if why.contains("is no longer the fabric-primary: ")), "the target ends the drive RoleLost: {end:?}");
    match next_within(&mut reader).await {
        Some(Read::RoleLost(loss)) => assert_eq!(loss.to.as_ref(), Some(&r.birth("mesh1.node-admin", "2").await.node_id), "the stream names the executor the transfer primary: {loss:?}"),
        other => panic!("the reader ends with the role loss, got {other:?}"),
    }
    assert!(ended_states(&*r.builds, &r.build).await.is_empty(), "the old primary wrote no rdm.build.ended: the new one drives the rest");
}

/// The terminal `drive` ends with, read from its first frame on.
async fn terminal_of(drive: &Arc<Drive>) -> BuildReply {
    let mut reader = drive.reader();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), reader.next()).await.expect("the drive ends") {
            Some(Read::Terminal(t)) => return t,
            Some(Read::Frame(_)) => {}
            other => panic!("the drive ended without a terminal: {other:?}"),
        }
    }
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
    /// The incomplete shutdown first writes a step of the leave ended `complete` (`RosterAmended`):
    /// the run moved something before it ended.
    progress: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl OperationRunner for LostLeaver {
    async fn run(&self, build: &BuildId, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        let ends = self.ends.lock().unwrap().clone();
        if let (BuildOperation::ShutdownMesh { .. }, Some(end)) = (op, ends) {
            if self.progress.load(Ordering::SeqCst) {
                // The leave's step log, as its pipeline writes it: the step's `rdm.build.step` event.
                let builds = self.ran.get().expect("the rig set the recorder").1.clone();
                let log = rdm_admin::build_run::FramedBuilds::stepping(builds, &crate::cell_fabric(), &NodeId::parse("100000000001").unwrap(), "mesh2");
                let step = rdm_admin::build_state::StepWrite { build_id: build.clone(), mesh: None, operation: op.key(), step: "RosterAmended".into(), outcome: rdm_admin::build_state::StepOutcome::Complete };
                log.write_step(&step).await.map_err(|e| e.to_string())?;
            }
            return Err(end);
        }
        self.ran.get().expect("the rig set the recorder").run(build, op).await?;
        if let BuildOperation::ShutdownMesh { mesh, .. } = op {
            self.topology.write().await.nodes.retain(|n| n.mesh != *mesh);
        }
        Ok(())
    }
}

/// The fixture view with a second mesh, `mesh2`, whose primary is `mesh2.node-admin.1`, and the Build
/// that removes it: the fabric-primary `mesh1.node-admin.1` executes the mesh's shutdown (`executor_for`).
async fn removing_mesh2(ends: Option<rdm_admin::executor::RunEnd>) -> (Rig, Arc<LostLeaver>) {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    t.nodes.push(node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false));
    t.nodes.push(node(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", false, false));
    let held: Arc<Mutex<Option<Arc<LostLeaver>>>> = Arc::default();
    let slot = held.clone();
    let r = rig_for(t, move |_: &rdm_admin::topology::Topology| rdm_core::build::BuildChange::Delete { meshes: vec!["mesh2".into()], node_ids: Vec::new() }, move |ran, topology| {
        let mesh_deleter = Arc::new(LostLeaver { ran: std::sync::OnceLock::new(), topology, ends: Mutex::new(ends), progress: AtomicBool::new(false) });
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
    let older = r.builds.accepted_newest_first().await.unwrap().into_iter().find(|id| *id != r.build);
    rdm_admin::build_state::CurrentBuild::Completed(older.expect("the rig seeded a Build before the one under test"))
}

const LEAVE_INCOMPLETE: &str = "mesh.delete: mesh2.node-admin.1 sent no valid mesh-deleted; the workflow is incomplete and its step events stand";

/// CONTRACT (Q346: a Build ends "`incomplete` when it ran but not every section finished (what ran
/// stands)"; build.md §4: an incomplete Build is skipped by the current Build's read and a caller
/// resubmits it by its build_id; R-BI3: only a caller's resubmit drives it on): a mesh.delete whose
/// leaving mesh's primary is lost ends mesh2's section short, and the Build ends `incomplete` naming that
/// section. The step its run ended `complete` (`RosterAmended`) stands; the current Build's read skips the
/// Build for the one before it, and the reconcile loop's pass drives nothing of it. Once the leaving
/// mesh's primary is gone, the caller's resubmit runs it again: one more `rdm.build.started`, the
/// shutdown runs, and the Build ends `complete`.
/// Must NOT happen: the Build failed, the reconcile loop driving it on by itself, or a `Blocked` frame on
/// the caller's stream that no subject's Status `Blocked` reply made.
#[test]
fn a_leave_that_ends_incomplete_on_a_lost_primary_ends_the_build_incomplete_and_its_resubmit_completes_it() {
    use rdm_admin::build_state::{BuildState, CurrentBuild};
    use rdm_admin::events_storage::EndedState;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, first, driven, again, ran, read) = cap.run(async {
        let root = tracing::info_span!("cell.leave-incomplete-ends-the-build-incomplete");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete(LEAVE_INCOMPLETE.into()))).await;
        mesh_deleter.progress.store(true, Ordering::SeqCst);
        let seeded = seeded_before(&r).await;
        let drive = Drive::detached(r.build.clone());
        let mut reader = drive.reader();
        let end = r.env.run(&drive).await;
        let first = (end, r.builds.read_build(&r.build).await.unwrap(), r.builds.current_build().await.unwrap(), seeded);
        let read_first = frames_now(&mut reader).await;
        // The reconcile loop's pass drives nothing of an incomplete Build.
        let driven = rdm_admin::build_op::ensure_active(&Arc::new(rdm_admin::build_drive::Drives::default()), &r.env).await.len();
        // The leaving mesh's primary is gone, and the caller resubmits the Build by its build_id.
        *mesh_deleter.ends.lock().unwrap() = None;
        mesh2_primary_becomes(&r, NodeStatus::Dead).await;
        let resubmit = Drive::detached(r.build.clone());
        let mut reader = resubmit.reader();
        let end = r.env.run(&resubmit).await;
        let again = (end, r.builds.read_build(&r.build).await.unwrap(), r.builds.current_build().await.unwrap(), ended_states(&*r.builds, &r.build).await);
        let read_again = frames_now(&mut reader).await;
        let ran = r.ran.0.lock().unwrap().clone();
        (trace_id, first, driven, again, ran, (read_first, read_again))
    });
    println!("trace_id={trace_id}");
    let (end, p, current, seeded) = first;
    assert_eq!(end, DriveEnd::Terminal, "the section ending short ends the run");
    assert_eq!(p.state, BuildState::Incomplete, "a lost primary ends mesh2's section, never the Build failed: {p:?}");
    assert!(matches!(p.unfinished.as_slice(), [u] if u.mesh == "mesh2" && u.reason.contains(LEAVE_INCOMPLETE)), "the Build names mesh2's section and why it ended short: {:?}", p.unfinished);
    let shutdown = p.steps.iter().find(|s| s.step == "RosterAmended").map(|s| s.operation.clone()).expect("the leave wrote its RosterAmended step");
    assert!(p.completed(&shutdown, "RosterAmended"), "what ran stands: {:?}", p.steps);
    assert_eq!(current, seeded, "the incomplete Build is skipped: the Build before it is current");
    assert_eq!(driven, 0, "the reconcile loop's pass drives nothing of an incomplete Build");
    let (end, p, current, ended) = again;
    assert_eq!((end, p.state), (DriveEnd::Terminal, BuildState::Complete), "the resubmit runs the shutdown and completes the Build");
    assert_eq!(current, CurrentBuild::Completed(p.build_id.clone()), "the resubmitted Build is the current one, complete");
    assert!(matches!(ended.as_slice(), [EndedState::Incomplete { .. }, EndedState::Complete]), "one rdm.build.ended per run: {ended:?}");
    assert_eq!(ran.iter().filter(|k| k.action == "mesh.delete").count(), 1, "the shutdown ran to its end once, in the resubmit: {ran:?}");
    let (read_first, read_again) = read;
    assert_eq!(blocked_of(&read_first), Vec::<BuildReply>::new(), "the section ending short frames no Blocked to the caller");
    assert!(matches!(read_first.last(), Some(Read::Terminal(BuildReply::Incomplete { .. }))), "the first run's stream ends Incomplete: {read_first:?}");
    assert!(matches!(read_again.last(), Some(Read::Terminal(BuildReply::Complete { .. }))), "the resubmit's stream ends Complete: {read_again:?}");
}

/// CONTRACT (Q346: "A failed step fails its mesh's section, never the Build"): a shutdown that ends with
/// the workflow's own named failure (not a lost participant) ends mesh2's section: the Build ends
/// `incomplete`, never `failed`, naming the section, the failure's reason and its code, and the current
/// Build's read skips it for the Build before it.
#[test]
fn a_leave_that_ends_with_a_named_failure_ends_its_section_and_the_build_incomplete() {
    use rdm_admin::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (fp, end, p, current, seeded, dispatched) = cap.run(async {
        let root = tracing::info_span!("cell.leave-named-failure-ends-its-section");
        let _in_root = root.enter();
        let (r, _leaver) = removing_mesh2(Some(rdm_admin::executor::RunEnd::failed("mesh.delete: mesh2 is refused before it is dismantled: its handover is not confirmed".into()))).await;
        let fp = r.birth("mesh1.node-admin", "1").await;
        let seeded = seeded_before(&r).await;
        let end = r.env.run(&Drive::detached(r.build.clone())).await;
        let dispatched = r.dispatch.dispatched.lock().unwrap().clone();
        (fp, end, r.builds.read_build(&r.build).await.unwrap(), r.builds.current_build().await.unwrap(), seeded, dispatched)
    });
    assert_eq!(end, DriveEnd::Terminal, "the named failure ends the run");
    assert_eq!(p.state, BuildState::Incomplete, "the failure ends mesh2's section, never the Build failed");
    assert!(matches!(p.unfinished.as_slice(), [u] if u.mesh == "mesh2" && u.reason.contains("its handover is not confirmed") && u.code == rdm_core::outcome::Code::FailedPrecondition), "the Build names the section, the reason and its code: {:?}", p.unfinished);
    assert_eq!(current, seeded, "the incomplete Build is skipped: the Build before it is current again");
    assert_eq!(dispatched, vec![(fp, "mesh2".to_string())], "mesh2's section ran once, at the fabric-primary: nothing of it is sent again");
}

/// Moves what the resume of a leave decides on: `mesh2.node-admin.1`'s status in the view.
async fn mesh2_primary_becomes(r: &Rig, status: NodeStatus) {
    for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.location() == rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()) {
        n.status = status;
        n.is_primary = status.is_live();
    }
}

/// CONTRACT (R-D1: the hold on a peer mesh held unheard targets lifecycle mutation of its births, never
/// the shutdown; mesh-delete.md, "Resume after the primary's loss": "A silent but unproven primary
/// refuses the resume by name; silence is not death"): every birth of mesh2 is silent and mesh2 is held
/// unheard, and the leave of it ends incomplete because its primary is unproven. The shutdown runs all
/// the same, and the Build ends `incomplete` naming the unproven primary; once it has exited, the
/// caller's resubmit runs the shutdown and completes the Build.
/// Must NOT happen: the drive stopping at the unheard-mesh target before the shutdown, or a `Blocked`
/// frame on the caller's stream that no subject's Status `Blocked` reply made.
#[test]
fn a_leave_of_a_mesh_held_unheard_runs_and_its_resubmit_completes_once_the_primary_has_exited() {
    use rdm_admin::build_state::BuildState;
    let cap = crate::mesh_delete_resume::capture();
    let (first, second, ran, read) = cap.run(async {
        let root = tracing::info_span!("cell.leave-unheard-unproven-primary");
        let _in_root = root.enter();
        let (r, mesh_deleter) = removing_mesh2(Some(rdm_admin::executor::RunEnd::Incomplete("mesh.delete: the final primary mesh2.node-admin.1 is silent but not proven exited, so no admin is created for it".into()))).await;
        mesh2_primary_becomes(&r, NodeStatus::PendingReconnect).await;
        for n in r.env.topology.write().await.nodes.iter_mut().filter(|n| n.mesh == "mesh2") {
            n.status = NodeStatus::PendingReconnect;
        }
        r.switch.peer.lock().unwrap().insert("mesh2".into(), 120_000);
        let drive = Drive::detached(r.build.clone());
        let mut reader = drive.reader();
        let end = r.env.run(&drive).await;
        let first = (end, r.builds.read_build(&r.build).await.unwrap(), r.dispatch.dispatched.lock().unwrap().len());
        let mut read = frames_now(&mut reader).await;
        // The primary has exited: it is no longer live in the view, and the caller resubmits.
        *mesh_deleter.ends.lock().unwrap() = None;
        mesh2_primary_becomes(&r, NodeStatus::Dead).await;
        let resubmit = Drive::detached(r.build.clone());
        let mut reader = resubmit.reader();
        let second = (r.env.run(&resubmit).await, r.builds.read_build(&r.build).await.unwrap().state);
        read.extend(frames_now(&mut reader).await);
        let ran = r.ran.0.lock().unwrap().clone();
        (first, second, ran, read)
    });
    let (end, p, dispatches) = first;
    assert_eq!((end, p.state, dispatches), (DriveEnd::Terminal, BuildState::Incomplete, 1), "the shutdown ran although mesh2 is held unheard, and the Build ended incomplete: {:?}", p.unfinished);
    assert!(matches!(p.unfinished.as_slice(), [u] if u.mesh == "mesh2" && u.reason.contains("not proven exited")), "the Build names the unproven primary: {:?}", p.unfinished);
    assert_eq!(second, (DriveEnd::Terminal, BuildState::Complete), "once the primary has exited, the resubmit runs the shutdown and completes the Build");
    assert_eq!(ran.iter().filter(|k| k.action == "mesh.delete").count(), 1, "the shutdown ran to its end once: {ran:?}");
    assert_eq!(blocked_of(&read), Vec::<BuildReply>::new(), "neither run frames a Blocked to the caller: {read:?}");
}

/// CONTRACT (node-rpc-envelope.md "Caller execution and certainty": "a completed Build answers
/// AlreadyApplied with its terminal frame"): the drive that completed a Build is the record of that run.
/// A later ensure of the same Build (a caller whose read predates the end, a resubmit) is handed that
/// drive, and nothing runs again.
/// Must NOT happen: a second section sent for a complete Build.
#[tokio::test]
async fn a_completed_drive_is_kept_and_a_later_ensure_runs_nothing_again() {
    use rdm_admin::build_drive::Drives;
    let r = rig().await;
    let drives = Arc::new(Drives::default());
    let first = drives.ensure(&r.env, &r.build);
    assert!(matches!(terminal_of(&first).await, BuildReply::Complete { .. }), "the first run completes the Build");
    let later = drives.ensure(&r.env, &r.build);
    assert!(Arc::ptr_eq(&later, &first), "a later ensure is handed the drive that completed the Build");
    assert!(matches!(terminal_of(&later).await, BuildReply::Complete { .. }), "its terminal answers the later reader");
    assert_eq!(r.dispatch.dispatched.lock().unwrap().len(), 1, "one section was sent: {:?}", r.dispatch.dispatched.lock().unwrap());
}

/// An operation runner that holds its first operation until the cell lets it go: the section is
/// running at its mesh-primary while it is held. Each operation's step is written as the fixture's
/// (`Creates`).
struct HeldUntilLetGo {
    ran: Arc<Creates>,
    began: tokio::sync::watch::Sender<bool>,
    let_go: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl OperationRunner for HeldUntilLetGo {
    async fn run(&self, build: &BuildId, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.began.send_replace(true);
        let _go = self.let_go.acquire().await.expect("the cell never closes its semaphore");
        self.ran.run(build, op).await
    }
}

/// CONTRACT (stop_gate.rs: a task that takes the view lock begins each turn at the gate's wait
/// point, and a stop's quiesce resolves once every turn already past its wait point has ended;
/// node-stop.md steps 3-4, R-ON1): the drive is one of its admin's gated tasks. A drive whose
/// section is running at its mesh-primary when the admin's gate stops is a turn in progress at the
/// stop: the stop names it, the quiesce waits for it, and resolves only once the drive handled the
/// section's terminal and recorded the Build's end. Must NOT happen: a stop that finds no turn of a
/// drive whose section is running, so a stop cuts while that drive still dispatches or records.
#[tokio::test]
async fn a_drive_whose_section_is_running_when_its_admin_stops_is_a_turn_the_stops_quiesce_waits_for() {
    let (began, mut running) = tokio::sync::watch::channel(false);
    let let_go = Arc::new(tokio::sync::Semaphore::new(0));
    let held = let_go.clone();
    let change = move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh1".to_string(), [("node-admin", 2), ("node", 2)])]);
    let r = rig_for(view(), change, move |ran, _| -> Arc<dyn OperationRunner> { Arc::new(HeldUntilLetGo { ran, began, let_go: held }) }).await;
    let gate = r.env.turns.clone();
    let drive = Drive::detached(r.build.clone());
    let (env, d) = (r.env.clone(), drive.clone());
    let driving = tokio::spawn(async move { env.run(&d).await });
    tokio::time::timeout(Duration::from_secs(5), running.wait_for(|b| *b)).await.expect("the section never reached its operation").unwrap();
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![(r.birth("mesh1.node-admin", "2").await, "mesh1".to_string())], "mesh1's section is running at its mesh-primary");
    // The stop holds the gate while the section runs.
    let at = gate.stop();
    assert!(at.turns.contains_key("build-drive"), "the stop finds the drive's turn in progress while its section runs; turns at the stop: [{}]", at.named());
    let quiesce = tokio::spawn({
        let gate = gate.clone();
        async move { gate.quiesce().await }
    });
    tokio::task::yield_now().await;
    assert!(!quiesce.is_finished(), "the quiesce resolved while the drive's section was running");
    let_go.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), quiesce).await.expect("the quiesce never resolved once the section ended").unwrap();
    assert!(drive.completed(), "the quiesce resolved before the drive recorded the Build's end");
    assert_eq!(tokio::time::timeout(Duration::from_secs(5), driving).await.expect("the drive ended with its turn").unwrap(), DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec![rdm_core::OperationId::node("node.create", &r.created(&rdm_core::PathName::new("mesh1.node", "2").unwrap()).await)]);
}

/// CONTRACT (stop_gate.rs: no turn begins while the gate is stopped; node-stop.md step 4, R-ON1: a
/// stopped node-admin calls nothing out): a drive that begins while its admin's gate is stopped (the
/// executor's last turn before a stop ensured it) waits at the gate's wait point. It claims no
/// Build and sends no section until the gate opens (a start), and then drives the Build to its end.
/// Must NOT happen: a stopped admin's drive starting the Build or sending a section.
#[tokio::test(start_paused = true)]
async fn a_drive_that_begins_once_its_admin_stopped_starts_and_dispatches_nothing_until_the_admin_starts() {
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
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Pending, "the Build was not started while the gate was stopped");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no section was sent while the gate was stopped");
    gate.resume();
    assert_eq!(tokio::time::timeout(Duration::from_secs(60), driving).await.expect("the drive went on once the gate opened").unwrap(), DriveEnd::Terminal);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![(r.birth("mesh1.node-admin", "2").await, "mesh1".to_string())], "mesh1's section is sent and run once the gate opened");
}

/// The view after the old primary `mesh1.node-admin.2` committed its role row naming this admin, the
/// fabric-primary `mesh1.node-admin.1`, its successor for both roles: this admin's view names it the
/// primary of both, and the old primary holds none.
fn view_after_the_row() -> Topology {
    let mut t = view();
    t.nodes = vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true), node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", false, false), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", false, false)];
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
    async fn target(&self) -> Result<Option<NodeId>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn primaries_updated(&self, updates: &[PrecedingUpdate], _: &BuildId) -> Result<(), String> {
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

/// The Build removing the old primary `mesh1.node-admin.2` after its row, driven by this admin, its
/// successor, through `Acknowledges` over that row (`refusal` set: the acknowledgement fails).
async fn after_the_row(refusal: Option<&str>) -> (Rig, Arc<DriveEnv>, Arc<Acknowledges>, PrecedingUpdate) {
    let r = rig_for(view_after_the_row(), move |t: &rdm_admin::topology::Topology| crate::removed(t, &[rdm_core::PathName { path: "mesh1.node-admin".into(), name: "2".into() }]), |ran, _| ran).await;
    let primary = r.birth("mesh1.node-admin", "2").await;
    let op = rdm_core::OperationId::node("node.delete", &primary.node_id);
    let row = PrecedingUpdate { mesh: "mesh1".into(), operation: rdm_admin::fabric_handover::update_operation(rdm_core::PrimaryRole::MeshPrimary, &op), primary: Some(rdm_core::OperationSubject::Birth { node_id: primary.node_id, incarnation: primary.incarnation }) };
    let gate = Arc::new(Acknowledges { inner: r.switch.clone(), rows: vec![row.clone()], dispatch: r.dispatch.clone(), asked: Mutex::new(Vec::new()), refusal: refusal.map(str::to_string) });
    let e = &r.env;
    let env = env_with(&e, Some(gate.clone()), None);
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
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![(r.birth("mesh1.node-admin", "1").await, "mesh1".to_string())], "the operation on the old primary is run once, by this admin, its mesh's primary");
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
    let mut reader = drive.reader();
    match env.run(&drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains(&row.operation.action) && why.contains("the successor's call ended indeterminate"), "the stall names the update and why: {why}"),
        other => panic!("an update not acknowledged stalls the drive: {other:?}"),
    }
    assert_eq!(gate.asked.lock().unwrap().len(), 1, "asked once: {:?}", gate.asked.lock().unwrap());
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing was dispatched: {:?}", r.dispatch.dispatched.lock().unwrap());
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran");
    let waiting = frames_now(&mut reader).await;
    assert_eq!(blocked_of(&waiting), Vec::<BuildReply>::new(), "the update not acknowledged frames no Blocked to the caller: the stall names it on its span");
}

/// The view before the primary's row: the fabric-primary `mesh1.node-admin.1` holds both roles, and
/// `mesh1.node-admin.2` is the ready admin of its mesh that the update names its successor.
fn view_primary_of_both() -> Topology {
    let mut t = view();
    t.nodes = vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true), node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", false, false), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", false, false)];
    t
}

/// The primary's own node.primary.update refused before its row, as the door refuses it with no
/// successor (`fabric_handover::update_primary` step 2: nothing said, cut off or written): the view is
/// left as it is and the refusal is named as the admin's gate names a refused reply.
struct RefusedUpdate {
    inner: Arc<Switch>,
    calls: std::sync::atomic::AtomicUsize,
    /// The view the update moves the roles in, and the successor it moves them to on its second call.
    gone_then_move: Option<(Arc<RwLock<Topology>>, NodeId)>,
}

/// The Q7 text the refused update names.
const NO_SECONDARY: &str = "cannot transfer: no secondary of type node-admin in mesh mesh1";

#[async_trait::async_trait]
impl Gate for RefusedUpdate {
    async fn target(&self) -> Result<Option<NodeId>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn update_primaries(&self, m: &rdm_admin::executor::RoleMove, _: &BuildId, _: &rdm_core::OperationId) -> Result<(), rdm_admin::build_drive::RoleMoveEnd> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if let Some((topology, successor)) = &self.gone_then_move {
            if call == 1 {
                return Err(rdm_admin::build_drive::RoleMoveEnd::Failed { step: "primary-update".into(), reason: GONE_REASON.into() , code: rdm_core::outcome::Code::FailedPrecondition });
            }
            for n in topology.write().await.nodes.iter_mut() {
                if n.mesh == m.primary.mesh {
                    let is_succ = n.node_id == *successor;
                    if m.mesh_role {
                        n.is_primary = is_succ && n.r#type == "node-admin";
                    }
                }
                if m.fabric_role {
                    n.is_fabric_primary = n.node_id == *successor;
                }
            }
            if m.fabric_role {
                let topology = topology.read().await;
                let successor = topology.nodes.iter().find(|n| n.node_id == *successor).expect("fixture successor ID");
                self.inner.target_to(Some(&rdm_core::framing::Birth { node_id: successor.node_id.clone(), incarnation: successor.incarnation_id.clone().unwrap() }));
            }
            return Ok(());
        }
        Err(rdm_admin::build_drive::RoleMoveEnd::Refused(format!("rejected-no-successor: RejectedNoSuccessor {{ reason: {NO_SECONDARY:?} }}")))
    }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 failure
/// contract: "Refused before the row (step 2, or the writer's own pre-check of the call's revision):
/// refused by name, nothing changed, the Build's step fails by name. Nothing was cut off, so nothing
/// is restored"; Q346: "A failed step fails its mesh's section, never the Build"): the fabric-primary
/// `mesh1.node-admin.1` holds both roles and its Build deletes it, so it runs its own
/// node.primary.update first, which is refused before the row with Q7's text. The step
/// `primary-update` ends mesh1's section, and the Build ends `incomplete` naming that step and the
/// refusal: nothing is dispatched or run and the view still names `mesh1.node-admin.1` the primary of
/// both roles. No cut-off and no row is the refusing gate's own record here (it wrote nothing); the
/// door's refusal writing neither is
/// `fabric_handover::a_transfer_with_no_successor_is_refused_with_q7s_exact_texts_and_nothing_moves`.
/// Must NOT happen: the drive idle on a refusal, the update asked again, or the delete dispatched.
#[tokio::test(start_paused = true)]
async fn a_primarys_own_update_refused_before_the_row_ends_its_section_by_name_and_nothing_moves() {
    let r = rig_for(view_primary_of_both(), move |t: &rdm_admin::topology::Topology| crate::removed(t, &[rdm_core::PathName { path: "mesh1.node-admin".into(), name: "1".into() }]), |ran, _| ran).await;
    let gate = Arc::new(RefusedUpdate { inner: r.switch.clone(), calls: Default::default(), gone_then_move: None });
    let e = &r.env;
    let primary = env_with(&e, Some(gate.clone()), None);
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), primary.run(&drive)).await.expect("the drive ends");
    assert_eq!(end, DriveEnd::Terminal, "the refusal ends the run at a terminal");
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Incomplete, "the refused step ends mesh1's section, never the Build failed");
    match terminal_of(&drive).await {
        BuildReply::Incomplete { sections, .. } => assert!(matches!(sections.as_slice(), [u] if u.mesh == "mesh1" && u.step == "primary-update" && u.reason.contains(NO_SECONDARY)), "{sections:?}"),
        other => panic!("the drive's terminal names the section the refusal ended: {other:?}"),
    }
    assert_eq!(gate.calls.load(std::sync::atomic::Ordering::SeqCst), 1, "the update was asked once");
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing ran or was dispatched: ran {:?}, dispatched {:?}", r.ran.0.lock().unwrap(), r.dispatch.dispatched.lock().unwrap());
    let primary_node = r.env.topology.read().await.nodes.iter().find(|n| n.location() == rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).map(|n| (n.is_primary, n.is_fabric_primary));
    assert_eq!(primary_node, Some((true, true)), "the view still names the primary of both roles");
}

const GONE_REASON: &str = "the new primary's process is gone, provider-confirmed, after the committed row: step primary-update; new primary mesh1 000000000003; moved mesh-primary (mesh1) revision 4; ordinary rule primary mesh-primary (mesh1): mesh1.node-admin.1; provider proof: the provider proved its exact birth exited with code 0";

/// A node.primary.update whose new primary's process is gone, provider-confirmed, after the row
/// (ruling R-PU2, Q10), then, in the next run, a transfer to `successor`: the first call ends
/// `RoleMoveEnd::Failed` with the view unchanged (the ordinary rule names the primary again), the next
/// moves the requested roles to `successor`. The gate counts its calls.
fn gone_after_row_then_moved(inner: Arc<Switch>, topology: Arc<RwLock<Topology>>, successor: NodeId) -> Arc<RefusedUpdate> {
    Arc::new(RefusedUpdate { inner, calls: Default::default(), gone_then_move: Some((topology, successor)) })
}

/// CONTRACT (ruling R-PU2, Q8/Q9/Q10 + Luke 2026-10-13: when the new primary's process is gone,
/// provider-confirmed, after the committed row, the transfer step fails with `Failed { step, reason }`;
/// Q346: "A failed step fails its mesh's section, never the Build"; build.md §4: an incomplete Build is
/// resubmitted by its build_id): the fabric-primary `mesh1.node-admin.1` holds both roles and its Build
/// removes it. Its own update ends with the new primary's process gone after the row and the ordinary
/// rule names it again: the step `primary-update` ends mesh1's section, the Build ends `incomplete`
/// naming it, and nothing is dispatched or run. The caller's resubmit runs the Build again: the update is
/// asked again, moves the roles to `mesh1.node-admin.3`, and the drive ends at the moved role.
/// Must NOT happen: the remove dispatched in the first run, or the resubmit's update never asked.
#[tokio::test(start_paused = true)]
async fn a_primarys_own_update_whose_successors_process_is_gone_after_the_row_ends_its_section_and_the_resubmit_moves_the_roles() {
    let mut t = view();
    t.nodes = vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true), node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", false, false), node(rdm_core::PathName::new("mesh1.node-admin", "3").unwrap(), "mesh1", "node-admin", false, false), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", false, false)];
    let r = rig_for(t, move |t: &rdm_admin::topology::Topology| crate::removed(t, &[rdm_core::PathName { path: "mesh1.node-admin".into(), name: "1".into() }]), |ran, _| ran).await;
    let gate = gone_after_row_then_moved(r.switch.clone(), r.env.topology.clone(), r.birth("mesh1.node-admin", "3").await.node_id);
    let primary = env_with(&r.env, Some(gate.clone()), None);
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), primary.run(&drive)).await.expect("the drive ends");
    assert_eq!(end, DriveEnd::Terminal, "the failed step ends the run at a terminal");
    match terminal_of(&drive).await {
        BuildReply::Incomplete { sections, .. } => assert!(matches!(sections.as_slice(), [u] if u.mesh == "mesh1" && u.step == "primary-update" && u.reason.starts_with("the new primary's process is gone, provider-confirmed, after the committed row")), "{sections:?}"),
        other => panic!("the drive's terminal names the section the failed step ended: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "the remove is never reached: ran {:?}, dispatched {:?}", r.ran.0.lock().unwrap(), r.dispatch.dispatched.lock().unwrap());
    // The caller's resubmit runs the Build again.
    let resubmit = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), primary.run(&resubmit)).await.expect("the resubmitted drive ends");
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2, "the resubmit asked the update again");
    match end {
        DriveEnd::RoleLost(_) => {}
        other => panic!("the resubmit moved the roles, and the drive ends at the moved role: {other:?}"),
    }
    match next_within(&mut resubmit.reader()).await {
        Some(Read::RoleLost(loss)) => assert_eq!(loss.to.as_ref(), Some(&r.birth("mesh1.node-admin", "3").await.node_id), "the callers are sent to mesh1.node-admin.3: {loss:?}"),
        other => panic!("the resubmit's stream ends with the role loss, got {other:?}"),
    }
}

/// CONTRACT (Q10 commanded arm: "When a commanded node.primary.update ends, the primary answers the
/// fabric-primary with the Build carrier's own terminal, `Failed { step, reason }` ... The FP fails the
/// step on the variant, never on the reason's text"; Q346: the failed step fails its mesh's section,
/// never the Build): the fabric-primary `mesh1.node-admin.1` drives a Build that removes
/// `mesh2.node-admin.1`, mesh2's primary. The commanded role move answers `Failed { step, reason }`: the
/// step `primary-update` ends mesh2's section, the Build ends `incomplete` naming it, and nothing is
/// dispatched to mesh2. On the caller's resubmit, the move takes mesh2's role to `mesh2.node-admin.3`,
/// and the drive sends mesh2's section to it, which runs the remove and completes the Build.
#[tokio::test(start_paused = true)]
async fn a_commanded_update_that_ends_failed_ends_its_section_and_the_resubmit_runs_the_remove_at_the_new_primary() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready });
    t.nodes.push(node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false));
    t.nodes.push(node(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", false, false));
    t.nodes.push(node(rdm_core::PathName::new("mesh2.node-admin", "3").unwrap(), "mesh2", "node-admin", false, false));
    let r = rig_for(t, move |t: &rdm_admin::topology::Topology| crate::removed(t, &[rdm_core::PathName { path: "mesh2.node-admin".into(), name: "1".into() }]), |ran, _| ran).await;
    let gate = gone_after_row_then_moved(r.switch.clone(), r.env.topology.clone(), r.birth("mesh2.node-admin", "3").await.node_id);
    let commander = env_with(&r.env, Some(gate.clone()), None);
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), commander.run(&drive)).await.expect("the drive ends");
    assert_eq!(end, DriveEnd::Terminal, "the commanded Failed reply ends the run at a terminal");
    match terminal_of(&drive).await {
        BuildReply::Incomplete { sections, .. } => assert!(matches!(sections.as_slice(), [u] if u.mesh == "mesh2" && u.step == "primary-update" && u.reason == GONE_REASON), "{sections:?}"),
        other => panic!("the drive's terminal names mesh2's section: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty() && r.dispatch.dispatched.lock().unwrap().is_empty(), "nothing was dispatched when the commanded update ended Failed");
    // The caller's resubmit: the move takes mesh2's role to mesh2.node-admin.3, which runs the remove.
    let resubmit = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(60), commander.run(&resubmit)).await.expect("the resubmitted drive ends");
    assert_eq!(end, DriveEnd::Terminal, "the resubmitted Build completes");
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Complete);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2, "the resubmit asked the move again");
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![(r.birth("mesh2.node-admin", "3").await, "mesh2".to_string())], "mesh2's section went to its new primary");
    assert_eq!(*r.ran.0.lock().unwrap(), vec![rdm_core::OperationId::node("node.delete", &r.birth("mesh2.node-admin", "1").await.node_id)]);
}

/// CONTRACT (R12 / i143 e12.s47 #64): a node-admin born as `mesh1.node-admin.9` and renamed in place by
/// `node.name.update` to `mesh1.node-admin.1` acts and spans under the name it holds now, never the boot
/// `PathName` its structs were constructed with. Its `ControlPlane` submits, its `DriveEnv` drives,
/// its `RpcDispatcher` runs its own section in process through its own `RunDoor` and `BuildExecutor`,
/// its `NodeRows` writes rows, and every control-plane, Build and node-row span names
/// `mesh1.node-admin.1` and never `mesh1.node-admin.9`.
#[test]
fn a_renamed_admin_drives_runs_and_spans_under_its_held_name_never_its_boot_name() {
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

    let fp_id = rt.block_on(async {
        let boot: PathName = rdm_core::PathName::new("mesh1.node-admin", "9").unwrap();
        let held: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", false, false)],
        };
        let fp_id = t.node(&held).expect("the renamed admin is in the view at its held name").node_id.clone();
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let fp = t.node(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).expect("the fabric-primary is in the view");
    let fp_birth = rdm_core::framing::Birth { node_id: fp.node_id.clone(), incarnation: fp.incarnation_id.clone().unwrap() };
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), &fp_birth).await.unwrap();
        let topology = Arc::new(RwLock::new(t.clone()));
        let fabric_id = t.fabric.id.clone();
        let mut cp = crate::control_plane(builds.clone(), accepted, boot.clone(), t, crate::adopted_time());
        // Its birth's node id, whatever name it booted under (boot sets it from the birth).
        let _ = cp.node_id.set(fp_id.clone());
        cp.topology = topology.clone();

        let change = move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh1".to_string(), [("node-admin", 1), ("node", 2)])]);
        let change = crate::named_request(&cp, change).await;
        let opened = cp.submit("test", change).await.expect("the renamed fabric-primary admits the Build");

        let nodes_storage = Arc::new(rdm_admin::storage::MemoryNodesStorage::default());
        let rows = rdm_admin::node_rows::NodeRows::new(boot.clone(), fp_id.clone(), topology.clone(), nodes_storage);
        rows.create_row(&rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", &NodeId::mint(), &rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&PathName::new("mesh1.node", "2").unwrap())), None, None, rdm_admin::node_rows::RowFor::Create)
        .await
        .expect("the renamed mesh-primary claims the node row");

        let ran = Arc::new(Creates(Mutex::new(Vec::new()), builds.clone()));
        // Its step log, as boot wires it: each step it runs written as its own `rdm.build.step` event.
        let runs = Arc::new(rdm_admin::build_run::SectionRuns::default());
        let mut events = rdm_admin::events_storage::EventsWriter::on(builds.events(), &fabric_id, &fp_id);
        events.source.incarnation = fp_birth.incarnation.clone();
        let steps: Arc<dyn BuildStateAdapter> = Arc::new(rdm_admin::build_run::FramedBuilds::new(builds.clone(), runs.clone()).writing_steps(events, "mesh1"));
        let exec = Arc::new(rdm_admin::executor::BuildExecutor {
            executor: fp_birth.clone(),
            builds: steps.clone(),
            topology: topology.clone(),
            runner: Arc::new(crate::loopback::Steps::new(ran.clone(), steps.clone())),
        });
        let run_door = Arc::new(rdm_admin::build_run::RunDoor {
            me_path: boot.path.clone(), me_name: boot.name.clone(),
            builds: steps,
            exec,
            runs,
            local: builds.clone(),
            gate: rdm_admin::stop_gate::StopGate::new(),
        });
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(rdm_transport::NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
        let rpc_dispatcher = Arc::new(rdm_admin::build_op::RpcDispatcher {
            me_path: boot.path.clone(), me_name: boot.name.clone(),
            local: run_door,
            client,
            topology: topology.clone(),
            send: rdm_transport::Send::default(),
        });
        let switch = Arc::new(Switch {
            node_id: fp_id.clone(),
            targeted: AtomicBool::new(false),
            successor: Mutex::new(None),
            target_tx: tokio::sync::watch::channel(false).0,
            recorded_primary: Mutex::new(None),
            peer: Mutex::new(Default::default()),
            dead: Mutex::new(Default::default()),
        });
        let env = Arc::new(DriveEnv { me_mesh: "mesh1".into(),
            me_path: boot.path.clone(), me_name: boot.name.clone(),
            birth: fp_birth.clone(),
            topology: topology.clone(),
            builds: builds.clone(),
            dispatcher: rpc_dispatcher,
            gate: switch,
            notices: crate::written_by(Default::default(), &*builds, &fabric_id, &fp_birth),
            turns: rdm_admin::stop_gate::StopGate::new(),
        });

        let drive = Drive::detached(opened.build_id.clone());
        let end = env.run(&drive).await;
        assert_eq!(end, DriveEnd::Terminal, "the renamed admin drives and dispatches in-process under its held name");
        assert_eq!(*ran.0.lock().unwrap(), vec![rdm_core::OperationId::node("node.create", &builds.read_build(&opened.build_id).await.unwrap().topology.created_id(&rdm_core::PathName::new("mesh1.node", "2").unwrap()).cloned().expect("the Build creates mesh1.node.2"))]);
        let projected = builds.read_build(&opened.build_id).await.unwrap();
        assert_eq!(projected.state, rdm_admin::build_state::BuildState::Complete);
        assert!(projected.steps.iter().all(|s| s.source == format!("rdm://{fabric_id}/{fp_id}/{}", fp_birth.incarnation)), "every step event is the held birth's: {:?}", projected.steps);
        fp_id
    });

    capture.flush(Duration::from_secs(10)).expect("spans flush");
    let spans = exporter.get_finished_spans().unwrap();
    crate::assert_own_story(spans.iter().map(|s| s.name.as_ref()));
    // The boot name is path "mesh1.node-admin", name "9": no span names it, in any `<role>_path`/`<role>_name` pair.
    for s in &spans {
        let get = |k: &str| s.attributes.iter().find(|kv| kv.key.as_str() == k).map(|kv| kv.value.to_string());
        for kv in s.attributes.iter().filter(|kv| kv.key.as_str().ends_with("_path")) {
            let role = kv.key.as_str().trim_end_matches("_path");
            assert!(
                !(kv.value.to_string() == "mesh1.node-admin" && get(&format!("{role}_name")).as_deref() == Some("9")),
                "span {} attribute {role}_path/{role}_name still carries the boot name mesh1.node-admin 9",
                s.name
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
    // Each span names the held admin: by its held path and name, or by its node id where it names a node.
    let named = |name: &str, n: usize| {
        let (paths, names, ids) = (attr(name, "node_path"), attr(name, "node_name"), attr(name, "node"));
        assert!(!paths.is_empty() || !ids.is_empty(), "{name} is spanned");
        assert!(paths.iter().all(|p| p == "mesh1.node-admin") && names.iter().all(|n| n == "1"), "{name} names the held name: {paths:?} {names:?}");
        assert!(ids.iter().all(|i| *i == fp_id.to_string()), "{name} names the held birth's node id: {ids:?}");
        assert_eq!(paths.len().max(ids.len()), n, "{name} spans");
    };
    named("rdm.node_admin.build.create.via-accepted", 2);
    named("rdm.node_admin.node.create.via-node-row", 1);
    named("rdm.node_admin.build.update.via-drive", 1);
    named("rdm.node_admin.build.serve.via-own-section", 1);
    for (span, key) in [("rdm.node_admin.build.update.via-reconcile", "executor")] {
        let executors = attr(span, key);
        assert!(!executors.is_empty() && executors.iter().all(|e| e.starts_with(&fp_id.to_string())), "{span} {key} is the held birth: {executors:?}");
    }
}

/// CONTRACT (mesh-delete.md: the mesh row goes Dead at mesh.deleted and Dead is final; build.md §1: each
/// attempt computes the remaining work from the accepted topology, the records and its receipts; Luke
/// 2026-10-24: the plan reads mesh death from the Mesh record and the receipts, never from a view that has
/// not caught up): mesh3 was deleted by an earlier Build, its Mesh record Dead, and this fabric-primary's
/// view has not heard its Dead yet (it shows mesh3 Ready, its final primary still a member). A
/// Delete of mesh3 plans nothing for it: no mesh.delete is dispatched, and the Build
/// completes. Must NOT happen: a deleted mesh's mesh.delete planned again.
#[tokio::test]
async fn a_mesh_the_mesh_records_hold_dead_is_never_planned_again_while_the_view_still_shows_it_live() {
    let mut t = view();
    let mesh3 = MeshId::mint();
    t.meshes.push(Mesh { id: Some(mesh3.clone()), name: "mesh3".into(), status: ScopeStatus::Ready });
    t.nodes.push(node(rdm_core::PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin", true, false));
    let change = |_: &rdm_admin::topology::Topology| BuildChange::Delete { meshes: vec!["mesh3".into()], node_ids: Vec::new() };
    let r = rig_for(t, change, |ran, _| ran).await;
    r.switch.dead.lock().unwrap().insert(mesh3.clone());
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(30), r.env.run(&drive)).await;
    assert!(!r.ran.0.lock().unwrap().iter().any(|op| op == &rdm_core::OperationId::new("mesh.delete", rdm_core::OperationSubject::Mesh(mesh3.clone()))), "the Dead mesh is not deleted again: {:?}", r.ran.0.lock().unwrap());
    assert_eq!(end.ok(), Some(DriveEnd::Terminal), "the Build completes");
}

/// A gate whose fabric holds ready only once the cell sets it: the fabric's change a
/// Build put it back to pending for.
#[derive(Default)]
struct FabricChange {
    ready: AtomicBool,
}

#[async_trait::async_trait]
impl Gate for FabricChange {
    fn fabric_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
}

/// CONTRACT (Q154; states.md "Resize and executable replacement": "Its Build is Complete only when the fabric is
/// ready again from this Build's own change ... while the fabric changes, the Build reports
/// Blocked naming the change, never Complete"; build.md §1: every accepted Build is reconciled, "Every
/// execution attempt re-reads the accepted topology and current realized/observed state and computes
/// remaining work"): a Build whose topology put the fabric back to pending and whose work the view
/// already holds (nothing left to execute) is started, runs nothing, reports Blocked `fabric-change`,
/// never Complete, while the fabric changes, and is Complete once the fabric holds ready. Must NOT
/// happen: the Build never started, or Complete while the fabric changes.
#[tokio::test]
async fn a_build_with_nothing_to_execute_is_started_and_complete_once_the_fabric_is_ready() {
    let r = rig().await;
    // The view already holds what the Build adds: nothing is left to execute.
    {
        let mut v = r.env.topology.write().await;
        v.nodes.push(node(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", false, false));
    }
    let gate = Arc::new(FabricChange::default());
    let e = &r.env;
    let env = env_with(&e, Some(gate.clone()), None);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    for pass in 0..2 {
        match tokio::time::timeout(Duration::from_secs(10), env.run(&drive)).await {
            Ok(DriveEnd::Stalled(why)) => assert!(why.contains("ready"), "pass {pass}: the drive waits for the fabric's change: {why}"),
            other => panic!("pass {pass}: the Build is never Complete while the fabric changes: {other:?}"),
        }
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Running, "the Build is started while the fabric changes");
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

/// CONTRACT (states.md "Resize and executable replacement": "Its Build is Complete only when the fabric is
/// ready again from this Build's own change, as fabric.create's is"; fabric-create.md, Completion:
/// "`fabric.created` when the fabric is ready"): the Day-0 Build (fabric.create's: the bootstrap admin
/// alone, no change submitted) is started and holds its end while the fabric is pending. Its
/// `rdm.build.ended` `complete` is written once the fabric is ready, and the Build reads Complete.
/// Must NOT happen: the Day-0 Build's end written while the fabric is pending.
#[tokio::test]
async fn the_day0_build_writes_its_complete_only_after_its_own_ready() {
    let mesh_id = MeshId::mint();
    let t = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Pending, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(mesh_id.clone()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true)],
    };
    let inner = Arc::new(MemoryBuildStateAdapter::new());
    let builds: Arc<dyn BuildStateAdapter> = inner.clone();
    let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
    let birth = t.node(&me).unwrap().birth().unwrap();
    let day0 = BuildId::mint();
    let accepted = rdm_admin::build_state::BuildAccepted { build_id: day0.clone(), topology: FabricTopology::root("fabric1", "mesh1", mesh_id), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 1 };
    rdm_admin::build_state::accept(&*builds, &accepted, None, birth.node_id.as_str(), "day-0").await.unwrap();
    let topology = Arc::new(RwLock::new(t.clone()));
    let ran = Arc::new(Creates(Mutex::new(Vec::new()), inner.clone()));
    let dispatch = crate::loopback::Loopback::new(builds.clone(), inner.clone(), topology.clone(), ran.clone());
    let gate = Arc::new(FabricChange::default());
    let env = Arc::new(DriveEnv { me_mesh: "mesh1".into(),
        me_path: me.path.clone(), me_name: me.name.clone(),
        birth: birth.clone(),
        topology: topology.clone(),
        builds: builds.clone(),
        dispatcher: dispatch.clone(),
        gate: gate.clone(),
        notices: crate::written_by(Default::default(), &*builds, &t.fabric.id, &birth),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    let drive = Drive::detached(day0.clone());
    let mut reader = drive.reader();
    // The fabric is pending: the drive waits for its change.
    let held = tokio::time::timeout(Duration::from_secs(10), env.run(&drive)).await.expect("the pass ends");
    let frames = frames_now(&mut reader).await;
    let while_pending = ended_states(&*builds, &day0).await;
    assert!(matches!(&held, DriveEnd::Stalled(why) if why.contains("ready")), "the Day-0 Build waits for the fabric's ready, never Complete while pending: {held:?}; frames {frames:?}");
    assert!(while_pending.is_empty(), "no rdm.build.ended is written while the fabric is pending: {while_pending:?}");
    assert!(frames.iter().any(|f| matches!(f, Read::Frame(BuildReply::Blocked { step, .. }) if step == "fabric-change")), "the Day-0 Build reports Blocked naming the fabric's change: {frames:?}");
    assert!(!frames.iter().any(|f| matches!(f, Read::Terminal(_))), "never Complete while the fabric changes: {frames:?}");
    // Every mesh is ready: the fabric is ready.
    gate.ready.store(true, Ordering::SeqCst);
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), env.run(&drive)).await.expect("the drive ends once the fabric is ready"), DriveEnd::Terminal);
    assert_eq!(ended_states(&*builds, &day0).await, vec![rdm_admin::events_storage::EndedState::Complete], "the Day-0 Build's one end is written after ready");
    let done = builds.read_build(&day0).await.unwrap();
    assert_eq!(done.state, rdm_admin::build_state::BuildState::Complete, "the Day-0 Build is Complete after its own ready: {done:?}");
    assert!(ran.0.lock().unwrap().is_empty(), "the Day-0 Build has nothing to execute: {:?}", ran.0.lock().unwrap());
}


/// CONTRACT (fabric-node-hooks.md `on_build_ended`, Q256: "Each time a Build's terminal state is
/// recorded, the fabric-primary sends the application one `on_build_ended` notice"; Q346: a Build ends
/// `failed` "only when it could not start"; build.md §1: a fabric.delete may interrupt the Builds
/// before it): a Build a fabric.delete interrupts before it started could not start: it ends `failed`,
/// its reason naming the delete with code `ABORTED`, and sends one `on_build_ended` `Failed` and no
/// `on_build_started`.
/// Must NOT happen: the interrupted Build's ending untold, or an opening told for a Build that never
/// started.
#[tokio::test]
async fn a_build_a_fabric_delete_interrupts_before_it_started_sends_one_failed_ended_and_no_started() {
    let r = rig().await;
    let grow = r.builds.read_build(&r.build).await.unwrap();
    let at = r.builds.newest_accepted_at_ms().await.unwrap().unwrap_or(0) + 1;
    let delete = rdm_admin::build_state::BuildAccepted { build_id: BuildId::mint(), topology: grow.topology.clone(), change: BuildChange::DeleteFabric, source: None, time: at };
    rdm_admin::build_state::accept(&*r.builds, &delete, Some(&r.build), r.switch.node_id.as_str(), "accepted").await.unwrap();
    let drive = Drive::detached(r.build.clone());
    let end = tokio::time::timeout(Duration::from_secs(10), r.env.run(&drive)).await;
    assert_eq!(end.ok(), Some(DriveEnd::Terminal), "the interrupted Build ends");
    let ended = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(ended.state, rdm_admin::build_state::BuildState::Failed, "the Build could not start: {ended:?}");
    assert!(ended.failure.as_ref().is_some_and(|f| f.code == "ABORTED" && f.reason.contains(delete.build_id.as_str())), "the failure names the delete: {:?}", ended.failure);
    let told = r.told.settled(1).await;
    let keys: Vec<(String, &str)> = told.iter().map(crate::Notice::key).collect();
    assert_eq!(keys, vec![(r.build.to_string(), "failed")], "one Failed ending and no opening: {told:?}");
    let crate::Notice::Ended(c) = &told[0] else { unreachable!() };
    match &c.state {
        rdm_admin::wiring::BuildEndedState::Failed { reason, code } => {
            assert_eq!(*code, rdm_core::outcome::Code::Aborted, "{c:?}");
            assert!(reason.contains(delete.build_id.as_str()), "the ending names the delete: {c:?}");
        }
        other => panic!("the ending is Failed: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing of the interrupted Build ran");
}


/// Creates that take a while, counting how many run at once; the create of `fails` ends failed, its
/// first `failing` runs (every run unless a cell sets fewer).
struct AtOnce {
    ran: Arc<Creates>,
    running: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
    fails: Option<PathName>,
    failing: std::sync::atomic::AtomicUsize,
}

impl AtOnce {
    fn new(ran: Arc<Creates>, fails: Option<PathName>) -> Self {
        Self::failing(ran, fails, usize::MAX)
    }

    fn failing(ran: Arc<Creates>, fails: Option<PathName>, times: usize) -> Self {
        Self { ran, running: Default::default(), most: Default::default(), fails, failing: std::sync::atomic::AtomicUsize::new(times) }
    }
}

#[async_trait::async_trait]
impl OperationRunner for AtOnce {
    async fn run(&self, build: &BuildId, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        if self.fails.as_ref().is_some_and(|f| op.created_node().as_ref() == Some(f)) && self.failing.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
            self.ran.0.lock().unwrap().push(op.key());
            let node = self.fails.as_ref().expect("the selected create failed");
            return Err(rdm_admin::executor::RunEnd::failed(format!("WaitForBind failed: path {:?}, name {:?} did not report where it bound (JoinNode) within 300ms", node.path, node.name)));
        }
        self.ran.run(build, op).await
    }
}

async fn at_once(fails: Option<PathName>) -> (Rig, Arc<AtOnce>) {
    at_once_to(fails, 2, 5).await
}

/// [`at_once`], mesh1 grown to `admins` node-admins and `rpcs` nodes.
async fn at_once_to(fails: Option<PathName>, admins: u32, rpcs: u32) -> (Rig, Arc<AtOnce>) {
    at_once_failing(fails, usize::MAX, admins, rpcs).await
}

/// [`at_once_to`], the create of `fails` failing its first `times` runs only.
async fn at_once_failing(fails: Option<PathName>, times: usize, admins: u32, rpcs: u32) -> (Rig, Arc<AtOnce>) {
    let change = move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh1".to_string(), [("node-admin", admins), ("node", rpcs)])]);
    let runner: Arc<Mutex<Option<Arc<AtOnce>>>> = Arc::default();
    let held = runner.clone();
    let r = rig_for(view(), change, move |ran, _| -> Arc<dyn OperationRunner> {
        let at = Arc::new(AtOnce::failing(ran, fails, times));
        *held.lock().unwrap() = Some(at.clone());
        at
    })
    .await;
    let at = runner.lock().unwrap().clone().unwrap();
    (r, at)
}

/// CONTRACT (Q245): the member creates of a mesh that one admin executes run at once, never one after
/// another. mesh1 grows by four nodes; its mesh primary runs the four creates together.
#[tokio::test]
async fn a_meshs_member_creates_run_at_once() {
    let (r, at) = at_once(None).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    let mut ran = r.ran.0.lock().unwrap().clone();
    ran.sort();
    assert_eq!(ran, sorted(vec![rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh1.node", "2").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh1.node", "3").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh1.node", "4").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh1.node", "5").unwrap()).await)]));
    assert_eq!(at.most.load(Ordering::SeqCst), 4, "the four creates ran at once");
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
    assert_eq!(ran, sorted(vec![rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh1.node", "2").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh1.node", "3").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh1.node", "4").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh1.node-admin", "3").unwrap()).await)]), "the creates, sorted");
    assert_eq!(at.most.load(Ordering::SeqCst), 4, "the further node-admin's create ran with the nodes'");
}

/// The fixture view with mesh2 and mesh3 beside mesh1, each its own mesh-primary's: `mesh1.node-admin.1` is the
/// fabric-primary and mesh1's primary, `mesh2.node-admin.1` and `mesh3.node-admin.1` the primaries of theirs.
fn three_meshes() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: ["mesh1", "mesh2", "mesh3"].map(|m| Mesh { id: Some(MeshId::mint()), name: m.into(), status: ScopeStatus::Ready }).to_vec(),
        nodes: vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true), node(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false), node(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", false, false), node(rdm_core::PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin", true, false), node(rdm_core::PathName::new("mesh3.node", "1").unwrap(), "mesh3", "node", false, false)],
    }
}

/// CONTRACT (Q250): a Build grows mesh2 and mesh3, each by two nodes, each mesh's nodes its own
/// mesh-primary's to create. The fabric-primary sends each mesh-primary its section at once: the two
/// sections' creates run together, and the Build completes. Must NOT happen: mesh3's creates waiting for
/// mesh2's section to end.
#[tokio::test]
async fn each_meshs_section_is_sent_to_its_mesh_primary_at_once() {
    let fabric = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1)]), MeshDesired::of("mesh2", [("node-admin", 1), ("node", 3)]), MeshDesired::of("mesh3", [("node-admin", 1), ("node", 3)])] };
    let runner: Arc<Mutex<Option<Arc<AtOnce>>>> = Arc::default();
    let held = runner.clone();
    let r = rig_for(three_meshes(), move |t: &rdm_admin::topology::Topology| crate::grown(t, &(fabric).meshes), move |ran, _| -> Arc<dyn OperationRunner> {
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
    assert_eq!(ran, sorted(vec![rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh2.node", "2").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh2.node", "3").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh3.node", "2").unwrap()).await), rdm_core::OperationId::node("node.create", &r.created(&PathName::new("mesh3.node", "3").unwrap()).await)]));
    let mut sections = r.dispatch.sections();
    sections.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(sections, vec![(r.birth("mesh2.node-admin", "1").await, "mesh2".to_string()), (r.birth("mesh3.node-admin", "1").await, "mesh3".to_string())], "each mesh-primary was sent its mesh's section");
    assert_eq!(at.most.load(Ordering::SeqCst), 4, "the two sections' creates ran together");
}

/// The `node.create` operation of the node the rig's Build creates at `path`.`name`.
async fn create_op(r: &Rig, path: &str, name: &str) -> rdm_core::OperationId {
    rdm_core::OperationId::node("node.create", &r.created(&PathName::new(path, name).unwrap()).await)
}

/// A Build of `three_meshes`, mesh2 and mesh3 each grown by two nodes, run by `AtOnce` whose create of
/// `fails` ends failed.
async fn three_meshes_growing(fails: Option<PathName>) -> Rig {
    let fabric = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1)]), MeshDesired::of("mesh2", [("node-admin", 1), ("node", 3)]), MeshDesired::of("mesh3", [("node-admin", 1), ("node", 3)])] };
    rig_for(three_meshes(), move |t: &rdm_admin::topology::Topology| crate::grown(t, &(fabric).meshes), move |ran, _| -> Arc<dyn OperationRunner> { Arc::new(AtOnce::new(ran, fails)) }).await
}

/// CONTRACT (Q245, Q252; Q346: "A failed step fails its mesh's section, never the Build"): a member
/// create that fails stops none of the others: each of the four creates runs to its own end. The failed
/// create's step (`WaitForBind`) ends `failed`, which ends mesh1's section: the Build ends `incomplete`
/// naming that section's operation, step and reason, and the three creates that completed stand.
/// Must NOT happen: the other creates stopped by the failure, the failed create run again in the same
/// run, or the Build ended `failed`.
#[tokio::test]
async fn a_failed_member_create_stops_none_of_the_others_and_ends_its_section() {
    let (r, at) = at_once(Some(PathName::new("mesh1.node", "3").unwrap())).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    assert_eq!(tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.expect("the drive ends within 20 s"), DriveEnd::Terminal);
    let mut ran = r.ran.0.lock().unwrap().clone();
    ran.sort();
    let mut each = Vec::new();
    for n in ["2", "3", "4", "5"] {
        each.push(create_op(&r, "mesh1.node", n).await);
    }
    assert_eq!(ran, sorted(each.clone()), "each create ran once");
    assert_eq!(at.most.load(Ordering::SeqCst), 4, "the four creates ran at once");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(p.state, rdm_admin::build_state::BuildState::Incomplete, "the failed step ends mesh1's section, never the Build failed");
    let failed = each[1].to_string();
    assert!(matches!(p.unfinished.as_slice(), [u] if u.mesh == "mesh1" && u.operation == failed && u.step == "WaitForBind" && u.reason.contains(r#"path "mesh1.node", name "3" did not report where it bound"#)), "the Build names the section, its failed step and reason: {:?}", p.unfinished);
    assert!(p.step_of(&failed, "WaitForBind").is_some_and(|s| matches!(s.outcome, rdm_admin::build_state::StepOutcome::Failed { .. })), "the failed step's event ended failed: {:?}", p.steps);
    for (i, op) in each.iter().enumerate().filter(|(i, _)| *i != 1) {
        assert!(p.operation_completed(op), "create {i} completed and stands: {:?}", p.steps);
    }
    let frames = frames_now(&mut reader).await;
    assert!(matches!(frames.last(), Some(Read::Terminal(BuildReply::Incomplete { .. }))), "the stream ends Incomplete: {frames:?}");
    assert_eq!(blocked_of(&frames), Vec::<BuildReply>::new(), "no Blocked frame reaches the caller");
}

/// CONTRACT (Q250, Q245, Q252; Q346): mesh3's section fails at mesh3.node.2's create. mesh2's section
/// completes all the same: a failed section stops none of the others. mesh3's section ends at its failed
/// step and is sent nothing more, and the Build ends `incomplete` naming mesh3's section, the step and its
/// reason (node-rpc-envelope.md: `Incomplete { sections }`, "each named by its mesh, operation, step,
/// reason and code"). Must NOT happen: mesh2's creates held by mesh3's failure, mesh3's section sent
/// again, the Build ended `failed`, or a Blocked frame before the terminal.
#[tokio::test]
async fn a_section_that_fails_stops_no_other_and_the_build_ends_incomplete_naming_it() {
    use rdm_admin::build_state::BuildState;
    let r = three_meshes_growing(Some(PathName::new("mesh3.node", "2").unwrap())).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    let ended = tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.expect("the drive ends within 20 s");
    assert_eq!(ended, DriveEnd::Terminal, "the failed section ends its part of the run, and the Build ends");
    let failed = create_op(&r, "mesh3.node", "2").await.to_string();
    let frames = frames_now(&mut reader).await;
    match frames.last() {
        Some(Read::Terminal(BuildReply::Incomplete { sections, .. })) => {
            assert!(matches!(sections.as_slice(), [u] if u.mesh == "mesh3" && u.operation == failed && u.step == "WaitForBind" && u.reason.contains(r#"path "mesh3.node", name "2" did not report where it bound"#)), "the terminal names mesh3's section alone: {sections:?}");
        }
        other => panic!("the stream ends Incomplete {{ sections }}: {other:?}; frames {frames:?}"),
    }
    assert_eq!(blocked_of(&frames), Vec::<BuildReply>::new(), "no Blocked frame reaches the caller");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(p.state, BuildState::Incomplete, "the Build is incomplete, never failed");
    for n in ["2", "3"] {
        assert!(p.operation_completed(&create_op(&r, "mesh2.node", n).await), "mesh2.node.{n}'s create completed: {:?}", p.steps);
    }
    let sent: Vec<String> = r.dispatch.sections().into_iter().map(|(_, mesh)| mesh).filter(|m| m == "mesh3").collect();
    assert_eq!(sent.len(), 1, "mesh3's section was sent once: nothing more of it runs in this run");
}

/// CONTRACT (Q346: an incomplete Build "ran but not every section finished (what ran stands)"; build.md
/// §4: it is resubmitted by its build_id; build.md §1: "a step whose event ended `complete` is never run
/// again"): a member create fails once. Its section ends, and the Build ends `incomplete` with the other
/// three creates complete. The caller's resubmit runs the Build again: its section runs only the create
/// whose step did not end `complete`, and the Build ends `complete`.
/// Must NOT happen: a create that completed run again by the resubmit, or the Build ended `failed`.
#[test]
fn a_member_create_that_fails_once_ends_the_build_incomplete_and_the_resubmit_runs_only_it() {
    use rdm_admin::build_state::BuildState;
    use rdm_admin::events_storage::EndedState;
    let cap = crate::mesh_delete_resume::capture();
    let (first, second, ran, each, ended, frames, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.member-create-fails-once-resubmit");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, _at) = at_once_failing(Some(PathName::new("mesh1.node", "3").unwrap()), 1, 2, 5).await;
        let drive = Drive::detached(r.build.clone());
        let first = tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.expect("the drive ends within 20 s");
        let first = (first, r.builds.read_build(&r.build).await.unwrap().state);
        // The caller's resubmit, by the Build's id.
        let resubmit = Drive::detached(r.build.clone());
        let mut reader = resubmit.reader();
        let second = tokio::time::timeout(Duration::from_secs(20), r.env.run(&resubmit)).await.expect("the resubmitted drive ends within 20 s");
        let second = (second, r.builds.read_build(&r.build).await.unwrap().state);
        let mut each = Vec::new();
        for n in ["2", "3", "4", "5"] {
            each.push(create_op(&r, "mesh1.node", n).await);
        }
        let ended = ended_states(&*r.builds, &r.build).await;
        let (ran, frames) = (r.ran.0.lock().unwrap().clone(), frames_now(&mut reader).await);
        (first, second, ran, each, ended, frames, trace_id)
    });
    println!("trace_id={trace_id}");
    assert_eq!(first, (DriveEnd::Terminal, BuildState::Incomplete), "the failed create ends its section, and the Build ends incomplete");
    assert_eq!(second, (DriveEnd::Terminal, BuildState::Complete), "the resubmit completes the Build");
    let count = |op: &rdm_core::OperationId| ran.iter().filter(|r| *r == op).count();
    assert_eq!(count(&each[1]), 2, "the failed create ran in the first run and once more in the resubmit: {ran:?}");
    for op in [&each[0], &each[2], &each[3]] {
        assert_eq!(count(op), 1, "{op} completed in the first run and never runs again: {ran:?}");
    }
    assert!(matches!(ended.as_slice(), [EndedState::Incomplete { .. }, EndedState::Complete]), "one rdm.build.ended per run: {ended:?}");
    assert!(matches!(frames.last(), Some(Read::Terminal(BuildReply::Complete { .. }))), "the resubmit's stream ends Complete: {frames:?}");
    let spans = cap.spans();
    let resubmitted: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-reconcile" && s["attributes"]["outcome"] == "complete").filter_map(|s| s["attributes"]["operations"].as_str().map(str::to_string)).collect();
    assert!(resubmitted.iter().any(|ops| *ops == format!("{:?}", [each[1].clone()])), "the resubmit's section ran the failed create alone: {resubmitted:?}");
}

/// Refuses every `build.attempt.run` by name, as an admin that is not the mesh's primary in its view
/// refuses (`NotExecutor`, node-rpc-envelope.md "Refusals").
struct Refuses;

#[async_trait::async_trait]
impl rdm_admin::build_drive::Dispatcher for Refuses {
    async fn dispatch(&self, _build_id: &BuildId, _mesh: &str, _context: rdm_core::context::CallContext) -> rdm_admin::build_drive::Dispatched {
        rdm_admin::build_drive::Dispatched::Refused(BuildReply::NotExecutor { named: rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, recipient: rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() } })
    }
    async fn run_own(&self, _: &rdm_admin::stop_gate::Turn, _: &BuildId, mesh: &str, _: rdm_core::context::CallContext, _: Vec<BuildOperation>) -> rdm_admin::build_drive::Dispatched {
        panic!("the fabric-primary runs no section of mesh {mesh} itself in this cell")
    }
}

/// CONTRACT (node-rpc-envelope.md "Refusals": build.attempt.run "refuses by name when the recipient is
/// not" the mesh's primary (`NotExecutor { named, recipient }`); Q344: a call that ends without the
/// section's terminal ends its section; Blocked's "only source is the subject's Status `Blocked` reply to
/// the executor's ProbeNodeState"): mesh1's primary refuses its section by name. The section ends, named
/// by the refusal, and the Build ends `incomplete`; nothing runs. Must NOT happen: a Blocked frame on the
/// caller's stream for the refusal, or the section sent again.
#[tokio::test]
async fn a_mesh_primary_that_refuses_its_section_ends_the_section_by_name_and_frames_nothing() {
    let r = rig().await;
    let env = env_with(&r.env, None, Some(Arc::new(Refuses)));
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    assert_eq!(env.run(&drive).await, DriveEnd::Terminal, "the refused section ends the run");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(p.state, rdm_admin::build_state::BuildState::Incomplete);
    assert!(matches!(p.unfinished.as_slice(), [u] if u.mesh == "mesh1" && u.reason.contains("was refused") && u.reason.contains("not-executor")), "the section is named by the refusal: {:?}", p.unfinished);
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the refusal frames no Blocked to the caller");
}

/// CONTRACT (Q344: "One `Send { attempts, TimeLimit }` on every call, the only retry mechanism"; "No call
/// keeps a retry of its own: the Build drive's next-pass pickup ... go(es)"; Q346): a Build grows mesh2 and
/// mesh3, each its own mesh-primary's, and the call that sends mesh3 its section is spent: its primary
/// cannot be reached. mesh2's section completes; mesh3's ends `UNAVAILABLE`, naming the call, and the
/// Build ends `incomplete` naming it. Must NOT happen: the drive waiting on mesh3 with a retry of its
/// own, mesh2's section held by mesh3's, or a Blocked frame on the caller's stream.
#[tokio::test]
async fn a_spent_section_call_ends_its_section_and_the_build_ends_incomplete() {
    let r = three_meshes_growing(None).await;
    r.dispatch.lose(&r.birth("mesh3.node-admin", "1").await.node_id);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    assert_eq!(tokio::time::timeout(Duration::from_secs(20), r.env.run(&drive)).await.expect("the drive ends within 20 s"), DriveEnd::Terminal, "the spent call ends its section, and the Build ends");
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(p.state, rdm_admin::build_state::BuildState::Incomplete);
    assert!(matches!(p.unfinished.as_slice(), [u] if u.mesh == "mesh3" && u.code == rdm_core::outcome::Code::Unavailable && u.reason.contains("not reached")), "the Build names mesh3's section and the spent call: {:?}", p.unfinished);
    for n in ["2", "3"] {
        assert!(p.operation_completed(&create_op(&r, "mesh2.node", n).await), "mesh2.node.{n}'s create completed: {:?}", p.steps);
    }
    let frames = frames_now(&mut reader).await;
    assert!(matches!(frames.last(), Some(Read::Terminal(BuildReply::Incomplete { .. }))), "the stream ends Incomplete: {frames:?}");
    assert_eq!(blocked_of(&frames), Vec::<BuildReply>::new(), "the spent call frames no Blocked to the caller");
}

/// Accepts a fabric.delete after the Build while the operation runs (build.md §1: it "may interrupt the
/// Build in progress"), and the operation, admitted before it, fails within its bound.
struct DeleteDuring(Arc<Creates>, Mutex<Option<BuildId>>);

#[async_trait::async_trait]
impl OperationRunner for DeleteDuring {
    async fn run(&self, build: &BuildId, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        let builds = &self.0.1;
        let grow = builds.read_build(build).await.unwrap();
        let at = builds.newest_accepted_at_ms().await.unwrap().unwrap_or(0) + 1;
        let delete = rdm_admin::build_state::BuildAccepted { build_id: BuildId::mint(), topology: grow.topology.clone(), change: BuildChange::DeleteFabric, source: None, time: at };
        rdm_admin::build_state::accept(&**builds, &delete, Some(build), "mesh1.node-admin.1", "accepted").await.unwrap();
        *self.1.lock().unwrap() = Some(delete.build_id);
        self.0.0.lock().unwrap().push(op.key());
        Err(rdm_admin::executor::RunEnd::failed(format!("{:?} did not end within its bound", op.key())))
    }
}

/// CONTRACT (build.md §1: a fabric.delete "may interrupt the Build in progress"; Q346: the interrupted
/// Build ran, so it ends `incomplete`, each section it did not finish naming the delete;
/// node-rpc-envelope.md: Blocked's "only source is the subject's Status `Blocked` reply to the executor's
/// ProbeNodeState"): a fabric.delete is accepted while mesh1's primary runs the Build's create, which
/// fails within its bound. The caller's stream ends `Incomplete`, the section named at the step
/// `fabric.delete` with the delete's id. Must NOT happen: a Blocked frame on the caller's stream for the
/// interruption, or the interrupted Build run on.
#[tokio::test]
async fn a_run_a_fabric_delete_interrupts_ends_incomplete_naming_the_delete_and_frames_nothing() {
    let deleted: Arc<Mutex<Option<Arc<DeleteDuring>>>> = Arc::default();
    let held = deleted.clone();
    let r = rig_for(view(), move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh1".to_string(), [("node-admin", 2), ("node", 2)])]), move |ran, _| -> Arc<dyn OperationRunner> {
        let d = Arc::new(DeleteDuring(ran, Mutex::new(None)));
        *held.lock().unwrap() = Some(d.clone());
        d
    })
    .await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    let end = tokio::time::timeout(Duration::from_secs(10), r.env.run(&drive)).await.expect("the drive ends within 10 s");
    assert_eq!(end, DriveEnd::Terminal, "the interrupted Build ends");
    let delete = deleted.lock().unwrap().clone().unwrap().1.lock().unwrap().clone().expect("the delete was accepted during the run");
    let frames = frames_now(&mut reader).await;
    match frames.last() {
        Some(Read::Terminal(BuildReply::Incomplete { sections, .. })) => assert!(!sections.is_empty() && sections.iter().all(|u| u.step == "fabric.delete" && u.reason.contains(delete.as_str())), "every section left names the delete: {sections:?}"),
        other => panic!("the stream ends Incomplete naming the delete: {other:?}; frames {frames:?}"),
    }
    assert_eq!(blocked_of(&frames), Vec::<BuildReply>::new(), "the interruption frames no Blocked to the caller");
    assert_eq!(r.ran.0.lock().unwrap().len(), 1, "nothing of the interrupted Build ran after the delete: {:?}", r.ran.0.lock().unwrap());
}

/// CONTRACT (node-primary-update.md failure contract: "The workflow continues only after the result is
/// installed and acknowledged"; node-rpc-envelope.md: Blocked's "only source is the subject's Status
/// `Blocked` reply to the executor's ProbeNodeState"): the fabric-primary `mesh1.node-admin.1` holds both
/// primaries and its Build deletes it, so its drive updates them first; the update answers and this view
/// still names it the primary of both. The drive stalls naming it on its span and dispatches nothing.
/// Must NOT happen: a Blocked frame on the caller's stream for the update the view does not show.
#[tokio::test(start_paused = true)]
async fn a_primary_update_the_view_does_not_show_stalls_the_drive_and_frames_nothing() {
    let r = rig_for(view_primary_of_both(), move |t: &rdm_admin::topology::Topology| crate::removed(t, &[rdm_core::PathName { path: "mesh1.node-admin".into(), name: "1".into() }]), |ran, _| ran).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    match tokio::time::timeout(Duration::from_secs(60), r.env.run(&drive)).await.expect("the drive ends") {
        DriveEnd::Stalled(why) => assert!(why.contains("still names it") && why.contains("node.delete"), "the stall names the update the view does not show: {why}"),
        other => panic!("an update the view does not show stalls the drive: {other:?}"),
    }
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the update's wait frames no Blocked to the caller");
}

/// The primary's own node.primary.update that moves both primaries to `mesh1.node-admin.2` and the fabric
/// record with them, then ends without node.primary.updated (`Incomplete`).
struct MovedUnanswered {
    inner: Arc<Switch>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl Gate for MovedUnanswered {
    async fn target(&self) -> Result<Option<NodeId>, BuildStateError> {
        self.inner.target().await
    }
    async fn target_set(&self) {
        self.inner.target_set().await
    }
    async fn update_primaries(&self, _: &rdm_admin::executor::RoleMove, _: &BuildId, _: &rdm_core::OperationId) -> Result<(), rdm_admin::build_drive::RoleMoveEnd> {
        for n in self.topology.write().await.nodes.iter_mut() {
            let successor = n.location() == rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
            (n.is_primary, n.is_fabric_primary) = (successor, successor);
        }
        let topology = self.topology.read().await;
        let successor = topology.node(&PathName::new("mesh1.node-admin", "2").unwrap()).expect("fixture successor");
        self.inner.target_to(Some(&rdm_core::framing::Birth { node_id: successor.node_id.clone(), incarnation: successor.incarnation_id.clone().unwrap() }));
        Err(rdm_admin::build_drive::RoleMoveEnd::Incomplete("the successor's answer was not read".into()))
    }
}

/// CONTRACT (node-primary-update.md failure contract: "After the successor installed itself, it is
/// primary whatever the call returns"; node-rpc-envelope.md: Blocked's "only source is the subject's
/// Status `Blocked` reply to the executor's ProbeNodeState"): the fabric-primary's own update moves both
/// primaries to `mesh1.node-admin.2` and ends without its answer. The drive records why on its dispatch span
/// and ends at the fabric record naming the successor. Must NOT happen: a Blocked frame on the caller's
/// stream for the unanswered update.
#[tokio::test(start_paused = true)]
async fn a_primary_update_that_moved_the_primaries_without_its_answer_frames_nothing() {
    let r = rig_for(view_primary_of_both(), move |t: &rdm_admin::topology::Topology| crate::removed(t, &[rdm_core::PathName { path: "mesh1.node-admin".into(), name: "1".into() }]), |ran, _| ran).await;
    let e = &r.env;
    let env = env_with(&e, Some(Arc::new(MovedUnanswered { inner: r.switch.clone(), topology: e.topology.clone() })), None);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader();
    let end = tokio::time::timeout(Duration::from_secs(60), env.run(&drive)).await.expect("the drive ends");
    assert!(matches!(&end, DriveEnd::RoleLost(_)), "the drive ends at the record naming the successor: {end:?}");
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran");
    assert_eq!(blocked_of(&frames_now(&mut reader).await), Vec::<BuildReply>::new(), "the unanswered update frames no Blocked to the caller");
}

// The reconcile loop's wait (Q253, amended by Luke 2026-10-28: "I hate timers; make this event
// driven", rdm#164): every pass of the fabric-primary's reconcile loop is woken by an event (a view
// install, a Build submitted, a fabric state change, a drive's end), never by a tick. The loop below is the admin's (boot.rs): `wake.next()`, then one pass, whose
// drive part is `ensure_active`.

/// What wakes the loop under test, each the admin's own source: the control plane's
/// `build_submitted`, the admin's records (a view installed, the fabric state) and the drives.
struct Wakes {
    submitted: Arc<tokio::sync::Notify>,
    records: Arc<rdm_admin::admin::Records>,
    drives: Arc<rdm_admin::build_drive::Drives>,
    provider: Arc<dyn rdm_admin::deployment::provider::DeploymentProvider>,
}

impl Wakes {
    fn new() -> Self {
        Self::on(Arc::new(rdm_admin::deployment::process::ProcessDeploymentProvider::new()))
    }

    /// The wakes of an admin whose provider is `provider`.
    fn on(provider: Arc<dyn rdm_admin::deployment::provider::DeploymentProvider>) -> Self {
        Self { submitted: Arc::new(tokio::sync::Notify::new()), records: Arc::new(rdm_admin::admin::Records::default()), drives: Arc::new(rdm_admin::build_drive::Drives::default()), provider }
    }

    fn wake(&self) -> rdm_admin::admin::ReconcileWake {
        rdm_admin::admin::ReconcileWake::new(self.submitted.clone(), self.records.subscribe_view(), self.records.subscribe_fabric_state(), self.drives.subscribe_ended(), rdm_admin::deployment::provider::DeploymentProvider::exits(&*self.provider))
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

/// The cell's `rdm.node_admin.build.update.via-drive` spans: one per pass that drove the Build, each its
/// own root, joined by build_id (telemetry.md §4), never a child of the event that woke it.
fn drive_passes(spans: &[serde_json::Value]) -> usize {
    let passes: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-drive").collect();
    assert!(passes.iter().all(|s| s["parent_span_id"].as_str().unwrap_or("").trim_start_matches('0').is_empty()), "each drive pass is a root: {passes:?}");
    passes.len()
}

/// CONTRACT (Q253 amended: "the wake-up is an event, never a tick"): a fabric-primary whose drive waits
/// on the fabric's change, and to which nothing happens, runs no reconcile pass:
/// over 2 s the loop's first pass is its only one, and the Build was driven once.
/// Must NOT happen: a pass every 300 ms, each driving the waiting Build again.
#[test]
fn an_idle_fabric_primary_with_a_waiting_drive_runs_no_reconcile_pass_without_an_event() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-idle-runs-no-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, env) = waiting().await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&env, &wakes, None);
        tokio::time::sleep(SETTLE + Duration::from_secs(2)).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        drop(r);
        (trace_id, passes)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First], "the loop's first pass is its only one while nothing happens: {passes:?}");
    assert_eq!(drive_passes(&spans), 1, "the waiting Build is driven once");
}

/// CONTRACT (Q253 amended: "a view install (`Records::view_tick`) ... runs one reconcile pass, which
/// recomputes the world from the live view"): one view install runs exactly one pass, which drives the
/// waiting Build again.
/// Must NOT happen: the install unseen until a tick, or passes beyond the one it woke.
#[test]
fn one_view_install_runs_one_reconcile_pass() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-view-install-runs-one-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, env) = waiting().await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&env, &wakes, None);
        tokio::time::sleep(SETTLE).await;
        let mut moved = r.env.topology.read().await.clone();
        for n in moved.nodes.iter_mut().filter(|n| n.location() == rdm_core::PathName::new("mesh1.node", "1").unwrap()) {
            n.status = NodeStatus::PendingReconnect;
        }
        wakes.records.install_projected(&r.env.topology, || moved).await;
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::ViewInstalled], "one install, one pass: {passes:?}");
    assert_eq!(drive_passes(&spans), 2, "the Build is driven by the first pass and the install's");
}

/// CONTRACT (Q253 amended: "a Build accepted (`build_submitted`)" runs one pass): one submit, notified as
/// the control plane notifies it, runs exactly one pass; the waiting drive's world did not move, so it
/// waits again.
/// Must NOT happen: passes beyond the one the submit woke.
#[test]
fn one_submit_runs_one_reconcile_pass() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-submit-runs-one-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, env) = waiting().await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&env, &wakes, None);
        tokio::time::sleep(SETTLE).await;
        wakes.submitted.notify_waiters();
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        assert_eq!(r.builds.read_build(&r.build).await.unwrap().state, rdm_admin::build_state::BuildState::Running, "nothing the drive decides on moved: the Build waits");
        (trace_id, passes)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::Submitted], "one submit, one pass: {passes:?}");
    assert_eq!(drive_passes(&spans), 2);
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
        let (r, env) = waiting().await;
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&env, &wakes, None);
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
    assert_eq!(drive_passes(&spans), 2);
}

/// CONTRACT (Q253 amended; the end of the Build is what the loop waits on): a drive that ends its Build runs exactly one more pass.
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
    assert_eq!(drive_passes(&spans), 1, "the completed Build is not driven again");
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
        let (r, env) = waiting().await;
        let wakes = Wakes::new();
        let hold = Arc::new(tokio::sync::Semaphore::new(0));
        let (passes, task) = reconciler(&env, &wakes, Some(hold.clone()));
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
    assert_eq!(drive_passes(&spans), 2);
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
        let (r, env) = waiting().await;
        let wakes = Wakes::new();
        let dir = std::env::temp_dir().join(format!("reconcile-provider-exit-{}", MeshId::mint()));
        let spec = rdm_admin::deployment::provider::ResolvedNodeLaunch { mesh: "mesh1".to_owned(), r#type: "node".into(), secrets: Default::default(), withhold: Vec::new(), path: "mesh1.node".into(), name: "9".into(), deployment_id: rdm_admin::model::DeploymentId::mint(), executable: "/bin/sleep".into(), args: vec!["30".into()], env: Default::default(), data_dir: dir.clone(), transport: "127.0.0.1:0".parse().unwrap(), listeners: vec![] };
        let h = wakes.provider.spawn(&spec, rdm_admin::deployment::deadline::Deadline::step()).await.expect("the runtime starts");
        let (passes, task) = reconciler(&env, &wakes, None);
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
    assert_eq!(drive_passes(&spans), 2);
}

/// CONTRACT (Q254, as `a_provider_exit_runs_one_reconcile_pass`, under the container provider): a
/// container the admin's provider started, killed from outside it with nothing else happening, runs
/// exactly one reconcile pass, woken by the provider's exit.
/// Must NOT happen: the container's exit unseen until an unrelated event (the pruner's stale mark).
/// Opt-in with the container proof (`RDM_CONTAINER_PROOF=1`), as every cell that runs a container.
#[test]
fn a_container_exit_runs_one_reconcile_pass() {
    use rdm_admin::admin::Woken;
    use rdm_admin::deployment::provider::DeploymentProvider;
    let cap = crate::mesh_delete_resume::capture();
    let ran = cap.run(async {
        let fabric = crate::container_proof::fabric_name("exit-pass");
        let p = crate::container_proof::provider(&fabric).await?;
        let _cleanup = crate::container_proof::Fabric(fabric.clone());
        let root = tracing::info_span!("cell.reconcile-container-exit-runs-one-pass");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (r, env) = waiting().await;
        let spec = crate::container_proof::launch(&p, &fabric, 0);
        let wakes = Wakes::on(Arc::new(p));
        let h = wakes.provider.spawn(&spec, rdm_admin::deployment::deadline::Deadline::step()).await.expect("the container starts");
        let (passes, task) = reconciler(&env, &wakes, None);
        tokio::time::sleep(SETTLE).await;
        let id = h.container.clone().expect("a container handle names its container");
        let killed = std::process::Command::new("docker").args(["kill", &id]).output().unwrap();
        assert!(killed.status.success(), "docker kill {id}: {}", String::from_utf8_lossy(&killed.stderr));
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        Some((trace_id, passes))
    });
    let Some((trace_id, passes)) = ran else { return };
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::ProviderExit], "one container exit, one pass: {passes:?}");
    assert_eq!(drive_passes(&spans), 2);
}

/// A rig whose Build waits: the view already holds the work its Build adds, and the fabric's change
/// is not ready (`FabricChange`): every drive of it stalls naming the fabric's change, the Build in
/// progress. The drive env reads the waiting gate.
async fn waiting() -> (Rig, Arc<DriveEnv>) {
    let r = rig().await;
    r.env.topology.write().await.nodes.push(node(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", false, false));
    let env = env_with(&r.env, Some(Arc::new(FabricChange::default())), None);
    (r, env)
}

/// CONTRACT (Q253 amended: "a change during a pass is read at the next event, never missed"): an event
/// whose pass finds the Build's drive still running (it read the world before the event and ends
/// waiting) runs that drive again once its run ends. mesh1's section is held while the event (a submit)
/// runs its pass; let go, the section completes, the drive waits on the fabric's change, and the drive
/// runs once more for the event its pass could not hand it.
/// Must NOT happen: the event spent on a running drive, so a drive that read the world before it waits
/// for the next event (rdm#164's gate: a Day-0 Build left waiting for the fabric's ready state that was
/// set while its drive ran).
#[test]
fn an_event_whose_pass_finds_the_drive_running_runs_the_drive_again_once_it_ends() {
    use rdm_admin::admin::Woken;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, passes, state) = cap.run(async {
        let root = tracing::info_span!("cell.reconcile-event-during-drive");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let (began, mut running) = tokio::sync::watch::channel(false);
        let let_go = Arc::new(tokio::sync::Semaphore::new(0));
        let held = let_go.clone();
        let change = move |t: &rdm_admin::topology::Topology| crate::grown(t, &[MeshDesired::of("mesh1".to_string(), [("node-admin", 2), ("node", 2)])]);
        let r = rig_for(view(), change, move |ran, _| -> Arc<dyn OperationRunner> { Arc::new(HeldUntilLetGo { ran, began, let_go: held }) }).await;
        let env = env_with(&r.env, Some(Arc::new(FabricChange::default())), None);
        let wakes = Wakes::new();
        let (passes, task) = reconciler(&env, &wakes, None);
        tokio::time::timeout(Duration::from_secs(5), running.wait_for(|b| *b)).await.expect("mesh1's section never reached its operation").unwrap();
        wakes.submitted.notify_waiters();
        while passes.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let_go.add_permits(8);
        tokio::time::sleep(SETTLE).await;
        task.abort();
        let passes = passes.lock().unwrap().clone();
        (trace_id, passes, r.builds.read_build(&r.build).await.unwrap().state)
    });
    let spans = cap.spans();
    println!("trace_id={trace_id}");
    assert_eq!(passes, vec![Woken::First, Woken::Submitted], "{passes:?}");
    assert_eq!(state, rdm_admin::build_state::BuildState::Running, "the Build waits on the fabric's change");
    assert_eq!(drive_passes(&spans), 2, "the drive runs again for the submit its pass found it running for");
}

// @feature: on-build-started-ended
/// CONTRACT: docs/architecture/fabric-node-hooks.md, on_build_started: "Each time the fabric-primary
/// writes a Build's `rdm.build.started` event, at acceptance or when a caller's resubmission runs a failed
/// or incomplete Build again, it sends the application one `on_build_started` notice from that write";
/// on_build_ended: "Each time a Build's terminal state is recorded, the fabric-primary sends the
/// application one `on_build_ended` notice ... from the write of the Build's `rdm.build.ended` event": a
/// Build whose create fails once is told started, then ended `incomplete` naming the failed step; its
/// resubmit is told started again, then ended `complete`. Each notice follows its event's write.
#[tokio::test]
async fn build_drive_fires_on_build_started_on_each_run_and_on_build_ended_on_each_end() {
    use rdm_admin::events_storage::{BUILD_ENDED, BUILD_STARTED};
    let (r, _at) = at_once_failing(Some(PathName::new("mesh1.node", "3").unwrap()), 1, 2, 3).await;
    assert_eq!(tokio::time::timeout(Duration::from_secs(20), r.env.run(&Drive::detached(r.build.clone()))).await.expect("the first run ends"), DriveEnd::Terminal);
    assert_eq!(tokio::time::timeout(Duration::from_secs(20), r.env.run(&Drive::detached(r.build.clone()))).await.expect("the resubmit ends"), DriveEnd::Terminal);
    let told = r.told.settled(4).await;
    let keys: Vec<(String, &str)> = told.iter().map(crate::Notice::key).collect();
    let b = r.build.to_string();
    assert_eq!(keys, vec![(b.clone(), "started"), (b.clone(), "incomplete"), (b.clone(), "started"), (b, "complete")], "one started and one ended per run: {told:?}");
    match &told[1] {
        crate::Notice::Ended(c) => assert!(matches!(&c.state, rdm_admin::wiring::BuildEndedState::Incomplete { sections } if matches!(sections.as_slice(), [u] if u.mesh == "mesh1" && u.step == "WaitForBind")), "the first end names the failed step's section: {c:?}"),
        other => panic!("the second notice is the first run's end: {other:?}"),
    }
    let events = r.builds.events().of_build(&r.build).await.unwrap();
    let ends: Vec<&str> = events.iter().map(|e| e.r#type.as_str()).filter(|t| *t == BUILD_STARTED || *t == BUILD_ENDED).collect();
    assert_eq!(ends, vec![BUILD_STARTED, BUILD_ENDED, BUILD_STARTED, BUILD_ENDED], "each notice is from its event's write");
}
