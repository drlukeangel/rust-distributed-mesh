//! i143 functional: node-admin's Build executor converges the topology after a loss, and only
//! after a proven one.
//!
//! The real chain runs end to end — `admin::reconcile_drift` (the fabric-primary inspects each
//! unheard birth's runtime and opens the next attempt of the accepted Build on proof),
//! the fabric-primary's drive (`DriveEnv::run`: the claim on its own log, `accepted::plan`, `executor_for`,
//! `build.attempt.run` to each executor in turn, hand-off, receipts),
//! `election::resolve` (the seats), and `fence::fence` before every create — over a world
//! fixture: a provider whose runtimes run or have exited, and a probe answering whether a birth
//! answers. Only the world is a fixture; every decision is the product code's.
//!
//! The fabric-primary's drift repairs node-admins only: an exited ordinary member is its
//! mesh-primary's recovery (node-recovery.md), submitted at the fabric-primary's Build door
//! (`ControlPlane::open_recovery`) and driven by the same chain to that mesh-primary as executor.
//!
//! The cells: an exited rpc node is recovered once by its mesh-primary, whatever its exit code;
//! a silent node whose runtime runs opens no attempt and is never replaced; a lost mesh-primary admin and a lost fabric-primary admin are
//! reborn by the right executor; members unheard through a lost forwarder are fenced, never
//! duplicated; a path the view does not hold but where a runtime runs is held. After every
//! loss: exactly the lost paths have new births, every path of the accepted topology has
//! exactly one live birth, and the accepted Build is complete with one more attempt.

use rafka_mesh_entity::{MeshDigest, MeshNode, MemberStatus, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rafka_mesh_transport::membership::DigestBook;
use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, MeshTopology};
use rafka_node_admin_core::build::{BuildId, BuildOperation};
use rafka_node_admin_core::build_state::{BuildState, MemoryBuildStateAdapter};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::build_drive::{Drive, DriveEnd, DriveEnv, OpenGate};
use rafka_node_admin_core::executor::OperationRunner;
use rafka_node_admin_core::fence::{fence, FenceOutcome, PathProbe};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

const DOMAIN: &str = "process:test-world";

/// One runtime of the world: which birth it is, and whether it runs.
#[derive(Debug, Clone)]
struct Runtime {
    path: String,
    node_id: String,
    pid: u32,
    status: DeploymentStatus,
    answers: bool,
}

/// The world the admins act in: runtimes by pid.
#[derive(Default)]
struct World {
    runtimes: Mutex<BTreeMap<u32, Runtime>>,
    next_pid: Mutex<u32>,
}

impl World {
    fn add(&self, path: &str, node_id: &str) -> u32 {
        let mut p = self.next_pid.lock().unwrap();
        *p += 1;
        let pid = 1000 + *p;
        self.runtimes.lock().unwrap().insert(pid, Runtime { path: path.into(), node_id: node_id.into(), pid, status: DeploymentStatus::Running, answers: true });
        pid
    }
    fn of(&self, node_id: &str) -> Option<Runtime> {
        self.runtimes.lock().unwrap().values().find(|r| r.node_id == node_id).cloned()
    }
    /// The runtime is killed: it exited, and it answers nothing.
    fn kill(&self, node_id: &str) {
        for r in self.runtimes.lock().unwrap().values_mut().filter(|r| r.node_id == node_id) {
            r.status = DeploymentStatus::Exited { code: None };
            r.answers = false;
        }
    }
    /// The runtime exited with `code`.
    fn exit_with(&self, node_id: &str, code: Option<i32>) {
        for r in self.runtimes.lock().unwrap().values_mut().filter(|r| r.node_id == node_id) {
            r.status = DeploymentStatus::Exited { code };
            r.answers = false;
        }
    }
    /// The runtime is frozen: it runs, and it answers nothing.
    fn freeze(&self, node_id: &str) {
        for r in self.runtimes.lock().unwrap().values_mut().filter(|r| r.node_id == node_id) {
            r.answers = false;
        }
    }
}

/// The provider: inspection is the world's.
struct Provider(Arc<World>);

#[async_trait::async_trait]
impl DeploymentProvider for Provider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.to_string(), reason: "births are made by the runner in this world".into() })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        h.pid.and_then(|p| self.0.runtimes.lock().unwrap().get(&p).map(|r| r.status.clone())).unwrap_or(DeploymentStatus::Unknown)
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        None
    }
}

/// The fence's questions, answered by the world.
struct Probe(Arc<World>);

#[async_trait::async_trait]
impl PathProbe for Probe {
    async fn answers(&self, node: &Node) -> bool {
        self.0.of(node.node_id.as_str()).is_some_and(|r| r.answers)
    }
    async fn inspect(&self, node: &Node) -> Result<DeploymentStatus, String> {
        self.0.of(node.node_id.as_str()).map(|r| r.status).ok_or_else(|| format!("{} has no runtime in this world", node.name))
    }
    async fn recorded(&self, path: &PathName) -> Vec<(String, DeploymentStatus)> {
        self.0.runtimes.lock().unwrap().values().filter(|r| r.path == path.to_string()).map(|r| (format!("pid {}", r.pid), r.status.clone())).collect()
    }
}


/// The node id of the birth `me` names in `t`: the drift pass's authority identity.
fn own_id(t: &rafka_node_admin_core::topology::Topology, me: &PathName) -> rafka_mesh_entity::NodeId {
    t.node(me).map(|n| n.node_id.clone()).unwrap_or_else(|| panic!("{me} is not in the view"))
}

fn fact(pid: u32) -> RuntimeFact {
    RuntimeFact { deployment_id: format!("dep-{pid}"), provider: RuntimeProvider::Process, control_domain: DOMAIN.into(), locator: RuntimeLocator::Process { pid, start: 1 } }
}

/// The estate: the world, the accepted Build, the admins' shared view and membership.
struct Estate {
    world: Arc<World>,
    builds: Arc<MemoryBuildStateAdapter>,
    accepted: Arc<AcceptedStore>,
    view: Arc<RwLock<Topology>>,
    book: DigestBook,
    provider: Provider,
    runner: Arc<Runner>,
    dispatch: Arc<crate::loopback::Loopback>,
}

const SHAPE: [(&str, u32, u32); 2] = [("mesh1", 2, 3), ("mesh2", 2, 3)];

/// Every path of the accepted shape born, live, held in the view and membership; seats elected.
async fn estate() -> Estate {
    let world = Arc::new(World::default());
    let fabric_id = FabricId::mint();
    let topology = FabricTopology { fabric: "fabric1".into(), meshes: SHAPE.iter().map(|(m, a, r)| ((*m).to_string(), MeshTopology::of(&rafka_node_admin_core::build::MeshDesired::of(*m, [(rafka_mesh_entity::NodeKind::NodeAdmin, *a), (rafka_mesh_entity::NodeKind::RpcNode, *r)])))).collect() };
    let mut nodes = Vec::new();
    let book = DigestBook::default();
    for m in topology.meshes.values() {
        for p in &m.nodes {
            nodes.push(birth(&world, &book, &fabric_id, &p.to_string(), None));
        }
    }
    rafka_node_admin_core::election::resolve(&mut nodes);
    let view = Arc::new(RwLock::new(Topology {
        fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: SHAPE.iter().map(|(m, _, _)| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::ReadyForTraffic }).collect(),
        nodes,
    }));
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let fp = view.read().await.fabric_primary().unwrap().name.to_string();
    let accepted = AcceptedStore::seeded(&*builds, fabric_id.clone(), topology, &fp).await.unwrap();
    let runner = Arc::new(Runner { world: world.clone(), view: view.clone(), book: book.clone(), fabric_id, ran: Mutex::new(Vec::new()), born: Mutex::new(Vec::new()), builds: builds.clone(), hear_after_birth: std::sync::atomic::AtomicBool::new(false) });
    let dispatch = crate::loopback::Loopback::new(builds.clone(), builds.clone(), view.clone(), runner.clone());
    Estate { provider: Provider(world.clone()), world, builds, accepted, view, book, runner, dispatch }
}

/// A new birth at `path`: a runtime in the world, a digest in membership, a live node.
fn birth(world: &World, book: &DigestBook, fabric_id: &FabricId, path: &str, supersedes: Option<IncarnationId>) -> Node {
    let mut n = Node::allocated(path.parse().unwrap());
    n.node_id = NodeId::mint();
    n.incarnation_id = Some(IncarnationId::mint());
    n.endpoint_id = Some(EndpointId(iroh::SecretKey::generate().public().to_string()));
    n.status = NodeStatus::ReadyForTraffic;
    let pid = world.add(path, n.node_id.as_str());
    book.record(MeshDigest {
        fabric_id: fabric_id.clone(),
        node: MeshNode {
            node_id: n.node_id.clone(),
            name: n.name.clone(),
            endpoint_id: n.endpoint_id.clone().unwrap(),
            transport_addr: "127.0.0.1:1".parse().unwrap(),
            incarnation: n.incarnation_id.clone().unwrap(),
            supersedes,
            runtime: Some(fact(pid)),
        },
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        emitted_at_rafka_ms: 0,
        digest_seq: 1,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
    });
    n
}

/// Realises `CreateNode` and `ReplaceNode` as the admin does: the real fence first; a birth only when it clears.
struct Runner {
    world: Arc<World>,
    view: Arc<RwLock<Topology>>,
    book: DigestBook,
    fabric_id: FabricId,
    ran: Mutex<Vec<(String, BuildOperation, FenceOutcome)>>,
    born: Mutex<Vec<String>>,
    /// The Build log the recovery's receipts are written to, as the admin's recover pipeline writes them.
    builds: Arc<MemoryBuildStateAdapter>,
    /// Membership repair runs right after every birth, before the attempt's next operation: what a
    /// reborn forwarder does to the executor's view mid-attempt.
    hear_after_birth: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl OperationRunner for Runner {
    async fn run(&self, build_id: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        if let BuildOperation::RecoverNode { node: path, node_id, from_incarnation } = op {
            // node-recovery.md: node.delete of the exited instance (no RPC, no stop frames, no probe:
            // the provider's exit proof is the fence), then node.create at the same path.name with a
            // new node.id and no supersedes. A runtime that still runs is no proof: refused by name,
            // as the admin's recover re-inspects before it deletes anything.
            if let Some(r) = self.world.runtimes.lock().unwrap().values().find(|r| r.node_id == node_id.as_str()) {
                if matches!(r.status, DeploymentStatus::Running) {
                    return Err(format!("{path}'s birth {node_id} at {} still runs: not proven exited", from_incarnation.0));
                }
            }
            // The recovery's receipts, as the recover pipeline writes them: NodeDeleted naming the
            // exited birth, then the operation's Complete (no output). The fabric-primary's recovery
            // dedup reads them.
            let operation = format!("recover-node:{path}");
            let receipt = |step: &str, output: Option<serde_json::Value>| rafka_node_admin_core::build_state::BuildStepReceipt {
                build_id: build_id.clone(),
                attempt,
                operation: operation.clone(),
                step: step.into(),
                outcome: rafka_node_admin_core::build_state::StepOutcome::Complete,
                output,
                executor: None,
            };
            use rafka_node_admin_core::build_state::BuildStateAdapter as _;
            self.builds.append_step_receipt(&receipt("NodeDeleted", Some(serde_json::json!({"node_id": node_id, "incarnation": from_incarnation})))).await.map_err(|e| e.to_string())?;
            let n = birth(&self.world, &self.book, &self.fabric_id, &path.to_string(), None);
            let mut v = self.view.write().await;
            v.nodes.retain(|x| x.name != *path);
            v.nodes.push(n);
            rafka_node_admin_core::election::resolve(&mut v.nodes);
            drop(v);
            self.born.lock().unwrap().push(path.to_string());
            self.builds.append_step_receipt(&receipt("Complete", None)).await.map_err(|e| e.to_string())?;
            return Ok(());
        }
        // A replacement is realised like a creation at a path whose exact birth the provider's proof
        // clears: the fence stands for the exact terminal proof the replace's stop step obtains.
        let (BuildOperation::CreateNode { node: path } | BuildOperation::ReplaceNode { node: path, .. }) = op else { return Err(format!("unexpected {op:?}")) };
        let prev = self.view.read().await.node(path).cloned();
        let out = fence(path, prev.clone(), &Probe(self.world.clone())).await;
        self.ran.lock().unwrap().push((path.to_string(), op.clone(), out.clone()));
        if matches!(out, FenceOutcome::Clear { .. }) {
            let n = birth(&self.world, &self.book, &self.fabric_id, &path.to_string(), prev.and_then(|p| p.incarnation_id));
            let mut v = self.view.write().await;
            v.nodes.retain(|x| x.name != *path);
            v.nodes.push(n);
            rafka_node_admin_core::election::resolve(&mut v.nodes);
            self.born.lock().unwrap().push(path.to_string());
            drop(v);
            if self.hear_after_birth.load(std::sync::atomic::Ordering::SeqCst) {
                hear(&self.world, &self.view).await;
            }
        }
        Ok(())
    }
}

/// Membership repair: every `Dead` node whose runtime runs and answers, in a mesh with a live admin
/// to forward it, is heard again.
async fn hear(world: &World, view: &RwLock<Topology>) {
    let mut v = view.write().await;
    let forwarded: BTreeSet<String> = v.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.status.is_live()).map(|n| n.mesh.clone()).collect();
    let mut changed = false;
    for n in v.nodes.iter_mut().filter(|n| n.status == NodeStatus::PendingReconnect && forwarded.contains(&n.mesh)) {
        if world.of(n.node_id.as_str()).is_some_and(|r| matches!(r.status, DeploymentStatus::Running) && r.answers) {
            n.status = NodeStatus::ReadyForTraffic;
            changed = true;
        }
    }
    if changed {
        rafka_node_admin_core::election::resolve(&mut v.nodes);
    }
}

impl Estate {
    async fn hear(&self) {
        hear(&self.world, &self.view).await;
    }

    /// A view change: `paths` are unheard (Dead) in the view; the seats are elected again.
    async fn unheard(&self, paths: &[&str]) {
        let mut v = self.view.write().await;
        for n in v.nodes.iter_mut().filter(|n| paths.contains(&n.name.to_string().as_str())) {
            n.status = NodeStatus::PendingReconnect;
        }
        rafka_node_admin_core::election::resolve(&mut v.nodes);
    }

    async fn node_id(&self, path: &str) -> String {
        self.view.read().await.nodes.iter().find(|n| n.name.to_string() == path).unwrap().node_id.to_string()
    }

    async fn fabric_primary(&self) -> String {
        self.view.read().await.fabric_primary().map(|n| n.name.to_string()).unwrap_or_default()
    }

    /// The fabric-primary's drift pass, as the admin's executor loop runs it.
    async fn drift(&self) -> Option<(BuildId, u32)> {
        let me: PathName = self.fabric_primary().await.parse().ok()?;
        let t = self.view.read().await.clone();
        let mut started = HashSet::new();
        rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &self.accepted, &self.book, &self.provider, &*self.builds, &rafka_node_admin_core::build_claim::AttemptContexts::in_memory(), &tokio::sync::Mutex::new(()), &[], &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await
    }

    /// The fabric-primary drives the accepted Build until no Build is active: the claim on its own
    /// log, `build.attempt.run` to each executor `executor_for` names, hand-offs and receipts are
    /// the product's. Between passes membership does what gossip does in the product: a runtime
    /// that runs and answers, in a mesh that has a live admin to forward its digests, is heard
    /// again and leaves `Dead` in the view.
    async fn converge(&self) {
        for _ in 0..8 {
            let env = self.drive_env().await;
            if let Some(current) = self.accepted.current(&*self.builds).await {
                if matches!(current.state, BuildState::Pending | BuildState::Running) {
                    let done = env.run(&Drive::detached(current.build_id.clone())).await;
                    if std::env::var("CONVERGE_TRACE").is_ok() {
                        eprintln!("converge: {done:?}");
                    }
                }
            }
            self.hear().await;
            let current = self.accepted.current(&*self.builds).await.unwrap();
            if !matches!(current.state, BuildState::Pending | BuildState::Running) {
                return;
            }
        }
        panic!("the accepted Build did not finish: {:?}", self.accepted.current(&*self.builds).await.map(|b| (b.state, b.attempt)));
    }

    /// The fabric-primary's drive over the estate: the claim decided at its own door, as the
    /// product decides it.
    async fn drive_env(&self) -> Arc<DriveEnv> {
        let primary: PathName = self.fabric_primary().await.parse().expect("a fabric-primary");
        let primary_id = own_id(&*self.view.read().await, &primary);
        let door = Arc::new(rafka_node_admin_core::build_claim::ClaimDoor {
            node_id: primary_id.clone(),
            me: primary.clone(),
            topology: self.view.clone(),
            builds: self.builds.clone(),
            contexts: Arc::new(rafka_node_admin_core::build_claim::AttemptContexts::in_memory()),
        });
        Arc::new(DriveEnv {
            me: primary.clone(),
            topology: self.view.clone(),
            accepted: self.accepted.clone(),
            builds: self.builds.clone(),
            door,
            dispatcher: self.dispatch.clone(),
            gate: Arc::new(OpenGate),
            departure: Arc::new(crate::loopback::Proofs::default()),
            verdicts: Arc::new(crate::loopback::NoVerdicts),
            rows: Arc::new(rafka_node_admin_core::node_rows::NodeRows::new(primary, primary_id, Arc::new(rafka_node_admin_core::storage::MemoryNodesStorage::default()))),
        })
    }

    /// One pass of the fabric-primary's drive over the accepted Build.
    async fn drive_once(&self) -> DriveEnd {
        let current = self.accepted.current(&*self.builds).await.expect("an accepted Build");
        self.drive_env().await.run(&Drive::detached(current.build_id)).await
    }

    /// `path`, a node-admin whose runtime runs and answers, is heard again; `lost`, its mesh's
    /// primary proven exited, holds no seat: its mesh seats `path` (election.rs: lowest Ready).
    async fn seat(&self, path: &str, lost: &str) {
        let mut v = self.view.write().await;
        for n in v.nodes.iter_mut() {
            if n.name.to_string() == path {
                n.status = NodeStatus::ReadyForTraffic;
            } else if n.name.to_string() == lost {
                n.status = NodeStatus::Dead;
            }
        }
        rafka_node_admin_core::election::resolve(&mut v.nodes);
    }

    /// The fabric-primary's first drive pass after a peer mesh lost its primary while its other
    /// node-admin `survivor` is held silent (R-D1; R-A2 "cross-mesh only when the whole cohort is
    /// proven lost"; node-rpc-envelope.md TakeSeats: the executor is the same-mesh successor): the
    /// drive stalls naming the mesh and `survivor`, nothing is dispatched and nothing is born; then
    /// `survivor` is seated by its own mesh and is the executor from there.
    async fn stalls_until_seated(&self, lost_mesh: &str, lost: &str) -> String {
        let survivor = self.view.read().await.cohort(lost_mesh, NodeKind::NodeAdmin).map(|n| n.name.to_string()).find(|n| n != lost).expect("a second admin in the lost mesh");
        match self.drive_once().await {
            DriveEnd::Stalled(why) => assert!(why.contains(lost_mesh) && why.contains(&survivor), "the stall names the mesh and its held admin: {why}"),
            other => panic!("the fabric-primary never acts on {lost_mesh} while {survivor} is held: {other:?}"),
        }
        assert!(self.dispatch.dispatched.lock().unwrap().is_empty() && self.born().is_empty(), "nothing dispatched and nothing born while {survivor} is held");
        self.seat(&survivor, lost).await;
        assert_eq!(self.mesh_primary_of(lost).await, survivor, "{lost_mesh} seats its own surviving admin");
        survivor
    }

    /// Every attempt the drive dispatched ran on `executor`.
    fn executed_only_by(&self, executor: &str) {
        let dispatched = self.dispatch.dispatched.lock().unwrap().clone();
        assert!(!dispatched.is_empty() && dispatched.iter().all(|(x, _)| x == executor), "the seated same-mesh admin executes the recovery: {dispatched:?}");
    }

    /// The topology holds: every path of the accepted shape has exactly one live birth.
    async fn holds_the_shape(&self) {
        let v = self.view.read().await;
        let mut per_path: BTreeMap<String, usize> = BTreeMap::new();
        for n in v.nodes.iter().filter(|n| n.status.is_live()) {
            *per_path.entry(n.name.to_string()).or_default() += 1;
        }
        let want: BTreeSet<String> = SHAPE.iter().flat_map(|(m, a, r)| MeshTopology::of(&rafka_node_admin_core::build::MeshDesired::of(*m, [(rafka_mesh_entity::NodeKind::NodeAdmin, *a), (rafka_mesh_entity::NodeKind::RpcNode, *r)])).nodes.into_iter().map(|p| p.to_string())).collect();
        assert_eq!(per_path.keys().cloned().collect::<BTreeSet<_>>(), want, "every accepted path is live");
        assert!(per_path.values().all(|c| *c == 1), "one live birth per path: {per_path:?}");
        // And the world: one running runtime per path.
        let mut running: BTreeMap<String, usize> = BTreeMap::new();
        for r in self.world.runtimes.lock().unwrap().values().filter(|r| matches!(r.status, DeploymentStatus::Running)) {
            *running.entry(r.path.clone()).or_default() += 1;
        }
        assert!(running.values().all(|c| *c == 1), "one running runtime per path in the world: {running:?}");
    }

    /// The fabric-primary's Build door over `builds`, on the estate's view and accepted Build.
    async fn door(&self, builds: Arc<dyn rafka_node_admin_core::build_state::BuildStateAdapter>) -> Arc<rafka_node_admin_core::http::ControlPlane> {
        let me: PathName = self.fabric_primary().await.parse().unwrap();
        let mut cp = rafka_node_admin_core::http::ControlPlane::new(builds, self.accepted.clone(), me, self.view.read().await.clone(), crate::adopted_time());
        cp.topology = self.view.clone();
        Arc::new(cp)
    }

    /// The mesh-primary's submit of `path`'s proven-exited birth (node-recovery.md sequence step 1):
    /// `build.create(recover <path.name>, exited birth)` at the fabric-primary's door.
    async fn submit_recovery(&self, path: &str) -> Result<rafka_node_admin_core::http::Recovery, rafka_node_admin_core::http::Refusal> {
        let n = self.view.read().await.node(&path.parse().unwrap()).cloned().expect("the exited birth is in the view");
        self.door(self.builds.clone()).await.open_recovery("build.create", n.name.clone(), n.node_id.clone(), n.incarnation_id.clone().unwrap()).await
    }

    /// [`Self::submit_recovery`] opened the recovery attempt, then the fabric-primary drives it.
    async fn recover(&self, path: &str) -> rafka_node_admin_core::http::Opened {
        let Ok(rafka_node_admin_core::http::Recovery::Opened(o)) = self.submit_recovery(path).await else { panic!("{path}'s recovery was not opened") };
        self.converge().await;
        o
    }

    /// The mesh-primary of `path`'s mesh.
    async fn mesh_primary_of(&self, path: &str) -> String {
        let mesh = path.split('.').next().unwrap();
        self.view.read().await.cohort_primary(mesh, NodeKind::NodeAdmin).unwrap().name.to_string()
    }

    /// The node-admin of the fabric-primary's own mesh that is not the fabric-primary: a birth the
    /// fabric-primary's drift proves and repairs (its own mesh is never deferred or held).
    async fn sibling_admin(&self) -> String {
        let fp = self.fabric_primary().await;
        let mesh = fp.split('.').next().unwrap().to_string();
        self.view.read().await.cohort(&mesh, NodeKind::NodeAdmin).map(|n| n.name.to_string()).find(|n| *n != fp).expect("a second admin in the fabric-primary's mesh")
    }

    fn born(&self) -> Vec<String> {
        self.runner.born.lock().unwrap().clone()
    }

    fn fence_of(&self, path: &str) -> Vec<FenceOutcome> {
        self.runner.ran.lock().unwrap().iter().filter(|(p, _, _)| p == path).map(|(_, _, o)| o.clone()).collect()
    }
}

/// CONTRACT (node-recovery.md): an exited ordinary member is its mesh-primary's to recover. The
/// fabric-primary's drift opens nothing for it; the mesh-primary's recovery submit opens the next
/// attempt of the same Build (reason Recovery), which the fabric-primary dispatches back to that
/// mesh-primary as executor; exactly the lost path has a new node (new node.id), nothing is fenced
/// by probe and nothing else is created.
#[tokio::test]
async fn an_exited_rpc_node_is_recovered_once_by_its_mesh_primary_and_nothing_else_is_created() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let old = e.node_id("mesh1.rpc.2").await;
    let exited = e.view.read().await.node(&"mesh1.rpc.2".parse().unwrap()).cloned().unwrap();
    e.world.kill(&old);
    e.unheard(&["mesh1.rpc.2"]).await;
    assert_eq!(e.drift().await, None, "the fabric-primary repairs no ordinary member");
    let mp = e.mesh_primary_of("mesh1.rpc.2").await;
    let opened = e.recover("mesh1.rpc.2").await;
    // node-recovery.md sequence step 1: a repeat submit for the same exited birth answers
    // AlreadyApplied from the recovery's receipts, naming its attempt, and opens nothing.
    let repeat = e.door(e.builds.clone()).await.open_recovery("build.create", exited.name.clone(), exited.node_id.clone(), exited.incarnation_id.clone().unwrap()).await;
    assert!(matches!(&repeat, Ok(rafka_node_admin_core::http::Recovery::AlreadyApplied(o)) if o.attempt == opened.attempt), "{repeat:?}");
    assert_eq!((opened.build_id.clone(), opened.attempt), (before.build_id.clone(), before.attempt + 1), "the same Build, one more attempt");
    assert_eq!(e.dispatch.dispatched.lock().unwrap().clone(), vec![(mp, before.attempt + 1)], "the recovery ran on the member's mesh-primary");
    assert_eq!(e.born(), vec!["mesh1.rpc.2".to_string()], "exactly the lost path is recovered");
    assert_eq!(e.fence_of("mesh1.rpc.2"), vec![], "the exit proof fences the old birth: no probe");
    assert_ne!(e.node_id("mesh1.rpc.2").await, old, "a new node, not the old one");
    e.holds_the_shape().await;
    let after = e.accepted.current(&*e.builds).await.unwrap();
    assert_eq!((after.build_id, after.attempt, after.state), (before.build_id, before.attempt + 1, BuildState::Complete));
}

#[tokio::test]
async fn a_silent_node_whose_runtime_runs_opens_no_attempt_and_is_never_replaced() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let silent = e.sibling_admin().await;
    let frozen = e.node_id(&silent).await;
    e.world.freeze(&frozen);
    e.unheard(&[silent.as_str()]).await;
    assert_eq!(e.drift().await, None, "silence is not proof: no attempt");
    e.converge().await;
    assert!(e.born().is_empty(), "nothing was created: {:?}", e.born());
    let after = e.accepted.current(&*e.builds).await.unwrap();
    assert_eq!((after.attempt, after.state), (before.attempt, BuildState::Complete));
    assert_eq!(e.world.of(&frozen).unwrap().status, DeploymentStatus::Running, "the frozen runtime still runs");
}

/// CONTRACT (mesh-recovery.md: "A lost member of a mesh with a live mesh-primary is node recovery,
/// driven by that mesh-primary"; node-recovery.md): a peer mesh's primary admin is lost while its
/// second admin lives, so the second admin holds the mesh's seat and the loss is node recovery. The
/// fabric-primary's drift opens nothing; the new mesh-primary's recovery submit opens the next
/// attempt of the same Build, dispatched back to that mesh-primary, and exactly the lost path gets a
/// new node.id. Must NOT happen: a proven-drift Replace of the lost admin.
#[tokio::test]
async fn a_lost_mesh_primary_admin_is_recovered_by_the_admin_its_mesh_seats_and_only_it() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let fp = e.fabric_primary().await;
    let fp_mesh = fp.split('.').next().unwrap().to_string();
    let other = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    // The other mesh's primary admin is lost.
    let primary = e.view.read().await.cohort_primary(other, NodeKind::NodeAdmin).unwrap().name.to_string();
    let old = e.node_id(&primary).await;
    e.world.kill(&old);
    e.unheard(&[&primary]).await;
    let seated = e.mesh_primary_of(&primary).await;
    assert_ne!(seated, primary, "the surviving admin holds its mesh's seat");
    assert_eq!(e.drift().await, None, "the fabric-primary repairs no node-admin of a mesh with a live mesh-primary");
    let opened = e.recover(&primary).await;
    assert_eq!((opened.build_id, opened.attempt), (before.build_id.clone(), before.attempt + 1), "the same Build, one more attempt");
    assert_eq!(e.dispatch.dispatched.lock().unwrap().clone(), vec![(seated, before.attempt + 1)], "the recovery ran on the mesh's new primary");
    assert_eq!(e.born(), vec![primary.clone()], "exactly the lost admin is recovered");
    assert_ne!(e.node_id(&primary).await, old, "a new node.id");
    e.holds_the_shape().await;
}

/// CONTRACT (node-recovery.md, mesh-recovery.md:8-9): a lost fabric-primary whose mesh keeps a second
/// admin is a lost member of a mesh with a live mesh-primary: the successor's drift opens nothing for
/// it, and the admin its mesh seats recovers it as a new node.id at the same path.
#[tokio::test]
async fn a_lost_fabric_primary_is_recovered_by_its_own_meshs_new_primary() {
    let e = estate().await;
    let fp = e.fabric_primary().await;
    let old = e.node_id(&fp).await;
    e.world.kill(&old);
    e.unheard(&[&fp]).await;
    let successor = e.fabric_primary().await;
    assert!(!successor.is_empty() && successor != fp, "the seat moved: {successor}");
    let seated = e.mesh_primary_of(&fp).await;
    assert_ne!(seated, fp, "its mesh's second admin holds the mesh's seat");
    assert_eq!(e.drift().await, None, "the successor's drift repairs no node-admin of a mesh with a live mesh-primary");
    let opened = e.recover(&fp).await;
    assert_eq!(e.dispatch.dispatched.lock().unwrap().clone(), vec![(seated, opened.attempt)], "the recovery ran on the lost admin's own mesh-primary");
    assert_eq!(e.born(), vec![fp.clone()], "exactly the lost fabric primary's path is recovered");
    assert_ne!(e.node_id(&fp).await, old, "a new node.id");
    e.holds_the_shape().await;
}

/// CONTRACT (R-D1: the fabric-primary acts on a peer mesh's node-admin only when every admin of
/// that mesh is proven gone, or as the restore the investigation's rebirth verdict authorizes; R-A2:
/// cross-mesh only when the whole cohort is proven lost): a peer mesh's primary admin exits and every
/// member of that mesh goes silent at once (its forwarder is gone), its second admin included,
/// though it runs and answers. The fabric-primary's drive stalls by name while that admin is held,
/// and once its own mesh seats it, it executes the repair: only the exited admin is reborn and no
/// member that answers is created over.
#[tokio::test]
async fn members_unheard_through_a_lost_forwarder_are_fenced_never_duplicated() {
    let e = estate().await;
    let fp = e.fabric_primary().await;
    let fp_mesh = fp.split('.').next().unwrap().to_string();
    let lost = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    // The other mesh's primary admin dies; in the fabric primary's view every member of that mesh
    // goes silent at once (its forwarder is gone), though all but the dead admin run and answer.
    let primary = e.view.read().await.cohort_primary(lost, NodeKind::NodeAdmin).unwrap().name.to_string();
    e.world.kill(&e.node_id(&primary).await);
    let members: Vec<String> = e.view.read().await.nodes.iter().filter(|n| n.mesh == lost).map(|n| n.name.to_string()).collect();
    let refs: Vec<&str> = members.iter().map(String::as_str).collect();
    e.unheard(&refs).await;
    e.drift().await.expect("the one proven loss opens the next attempt");
    let survivor = e.stalls_until_seated(lost, &primary).await;
    e.converge().await;
    e.executed_only_by(&survivor);
    assert_eq!(e.born(), vec![primary.clone()], "only the proven-exited admin is reborn: {:?}", e.runner.ran.lock().unwrap());
    for m in members.iter().filter(|m| **m != primary) {
        assert!(e.fence_of(m).iter().all(|o| *o == FenceOutcome::Alive), "{m} answers: never created over: {:?}", e.fence_of(m));
    }
    // Heard again through the reborn forwarder: the topology holds.
    e.holds_the_shape().await;
}

#[tokio::test]
async fn a_path_the_view_does_not_hold_but_where_a_runtime_runs_is_held() {
    let e = estate().await;
    // rpc.3 is not in this view at all (never heard here), but its runtime runs, frozen.
    let hidden = e.node_id("mesh1.rpc.3").await;
    e.world.freeze(&hidden);
    e.view.write().await.nodes.retain(|n| n.name.to_string() != "mesh1.rpc.3");
    // A real loss elsewhere opens a recovery whose plan is exactly that birth: the path the view
    // lacks is its own attempt's, so this one neither creates over it nor fences it.
    e.world.kill(&e.node_id("mesh1.rpc.2").await);
    e.unheard(&["mesh1.rpc.2"]).await;
    e.recover("mesh1.rpc.2").await;
    assert_eq!(e.born(), vec!["mesh1.rpc.2".to_string()], "the absent-but-running path is not created over");
    assert_eq!(e.fence_of("mesh1.rpc.3"), vec![], "an attempt that carries an action plans only its birth");
}

/// The seeded soak's finding (seed 7, round 3, trace in the spans of
/// `fabric_soak__seeded`): the lost mesh's primary is reborn as the attempt's first operation, its
/// members are heard again through it before the attempt's next operation, and that operation
/// must find them live and leave them alone. A plan made on the older view is not a licence. The
/// lost mesh's second admin runs and answers, so the fabric-primary stalls by name until that
/// mesh seats it, and the seated admin executes the attempt (R-D1, R-A2).
#[tokio::test]
async fn members_heard_again_mid_attempt_through_the_reborn_forwarder_are_never_created_over() {
    let e = estate().await;
    e.runner.hear_after_birth.store(true, std::sync::atomic::Ordering::SeqCst);
    let fp = e.fabric_primary().await;
    let fp_mesh = fp.split('.').next().unwrap().to_string();
    let lost = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let primary = e.view.read().await.cohort_primary(lost, NodeKind::NodeAdmin).unwrap().name.to_string();
    e.world.kill(&e.node_id(&primary).await);
    let members: Vec<String> = e.view.read().await.nodes.iter().filter(|n| n.mesh == lost).map(|n| n.name.to_string()).collect();
    let refs: Vec<&str> = members.iter().map(String::as_str).collect();
    e.unheard(&refs).await;
    e.drift().await.expect("the one proven loss opens the next attempt");
    let survivor = e.stalls_until_seated(lost, &primary).await;
    e.converge().await;
    e.executed_only_by(&survivor);
    assert_eq!(e.born(), vec![primary.clone()], "only the proven-exited admin is reborn: {:?}", e.runner.ran.lock().unwrap());
    for m in members.iter().filter(|m| **m != primary) {
        assert!(e.fence_of(m).iter().all(|o| *o == FenceOutcome::Alive), "{m} is live in the view by the time its operation runs: never created over: {:?}", e.fence_of(m));
    }
    e.holds_the_shape().await;
}

/// CONTRACT (node-recovery.md, "Lineage: No supersedes"): an exit carrying the TRANSPORT_STOPPED code
/// (4) is recovered like any other exit. The fabric-primary opens no Restart; the mesh-primary's
/// recovery creates a new node.id at the path, never the same identity under a new incarnation.
#[tokio::test]
async fn a_transport_stopped_exit_is_recovered_as_a_new_node_never_restarted() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let id = e.node_id("mesh1.rpc.2").await;
    e.world.exit_with(&id, Some(rafka_mesh_entity::runtime::TRANSPORT_STOPPED_EXIT_CODE));
    e.unheard(&["mesh1.rpc.2"]).await;
    assert_eq!(e.drift().await, None, "the fabric-primary restarts no ordinary member");
    e.recover("mesh1.rpc.2").await;
    let opened = e.accepted.current(&*e.builds).await.unwrap();
    assert_eq!(opened.reason, rafka_node_admin_core::build_state::AttemptReason::Recovery);
    assert!(matches!(&opened.action, Some(rafka_node_admin_core::accepted::AttemptAction::Recover { path, node_id, .. }) if path.to_string() == "mesh1.rpc.2" && node_id.as_str() == id), "{:?}", opened.action);
    assert_eq!(e.born(), vec!["mesh1.rpc.2".to_string()]);
    assert_ne!(e.node_id("mesh1.rpc.2").await, id, "a new node.id");
    e.holds_the_shape().await;
    let after = e.accepted.current(&*e.builds).await.unwrap();
    assert_eq!((after.build_id, after.attempt, after.state), (before.build_id, before.attempt + 1, BuildState::Complete));
}

/// CONTRACT (mesh-recovery.md:8-9 "A lost member of a mesh with a live mesh-primary is node
/// recovery, driven by that mesh-primary"; node-restart.md: a restart parks a live process, so a
/// proven exit is never restarted): a secondary node-admin of a peer mesh (its mesh-primary live and
/// not the fabric-primary) that exits, with the TRANSPORT_STOPPED code (4) and with another code
/// (137), is recovered by its mesh-primary: the fabric-primary's drift opens nothing, the recovery
/// is the next attempt of the same Build keyed on the exact exited birth, dispatched back to that
/// mesh-primary, and the path gets a new node.id. Must NOT happen: a proven-drift Replace or a Restart.
#[tokio::test]
async fn a_secondary_node_admins_exit_is_recovered_by_its_mesh_primary_whatever_its_code() {
    for code in [Some(rafka_mesh_entity::runtime::TRANSPORT_STOPPED_EXIT_CODE), Some(137)] {
        let e = estate().await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let fp = e.fabric_primary().await;
        let peer = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
        let mp = e.view.read().await.cohort_primary(peer, NodeKind::NodeAdmin).unwrap().name.to_string();
        let admin = e.view.read().await.cohort(peer, NodeKind::NodeAdmin).map(|n| n.name.to_string()).find(|n| *n != mp).unwrap();
        let id = e.node_id(&admin).await;
        let incarnation = e.view.read().await.node(&admin.parse().unwrap()).and_then(|n| n.incarnation_id.clone()).unwrap();
        e.world.exit_with(&id, code);
        e.unheard(&[&admin]).await;
        assert_eq!(e.mesh_primary_of(&admin).await, mp, "{code:?}: the mesh keeps its live primary");
        assert_eq!(e.drift().await, None, "{code:?}: the fabric-primary opens nothing for a secondary node-admin");
        let opened = e.recover(&admin).await;
        assert_eq!((opened.build_id.clone(), opened.attempt), (before.build_id.clone(), before.attempt + 1), "{code:?}: the same Build, one more attempt");
        let current = e.accepted.current(&*e.builds).await.unwrap();
        assert_eq!(current.reason, rafka_node_admin_core::build_state::AttemptReason::Recovery, "{code:?}");
        assert!(
            matches!(&current.action, Some(rafka_node_admin_core::accepted::AttemptAction::Recover { path, node_id, from_incarnation }) if path.to_string() == admin && node_id.as_str() == id && *from_incarnation == incarnation),
            "{code:?}: a Recover of the exact exited birth: {:?}",
            current.action
        );
        assert_eq!(e.dispatch.dispatched.lock().unwrap().clone(), vec![(mp.clone(), before.attempt + 1)], "{code:?}: executed by its mesh-primary");
        assert_eq!(e.born(), vec![admin.clone()], "{code:?}: exactly the lost admin is recovered");
        assert_ne!(e.node_id(&admin).await, id, "{code:?}: a new node.id");
        e.holds_the_shape().await;
    }
}

/// CONTRACT: every exit, whatever its code (1, unknown, 137), is recovered by the mesh-primary, one
/// exited birth per attempt: a submit while another recovery runs is refused BuildInProgress (one
/// Build in flight), and is opened once that one ends. The fabric-primary opens nothing for any of them.
#[tokio::test]
async fn every_exit_is_recovered_one_birth_per_attempt_and_a_submit_waits_for_the_running_one() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let (a, b, c) = (e.node_id("mesh1.rpc.1").await, e.node_id("mesh1.rpc.2").await, e.node_id("mesh1.rpc.3").await);
    e.world.exit_with(&c, Some(1));
    e.world.exit_with(&a, None);
    e.world.exit_with(&b, Some(137));
    e.unheard(&["mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3"]).await;
    assert_eq!(e.drift().await, None, "the fabric-primary repairs no ordinary member");
    let paths = ["mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3"];
    for (pass, path) in paths.into_iter().enumerate() {
        let Ok(rafka_node_admin_core::http::Recovery::Opened(o)) = e.submit_recovery(path).await else { panic!("pass {pass}: {path}'s recovery was not opened") };
        assert_eq!(o.attempt, before.attempt + 1 + pass as u32);
        if let Some(next) = paths.get(pass + 1) {
            let waits = e.submit_recovery(next).await;
            assert!(matches!(waits, Err(rafka_node_admin_core::http::Refusal::BuildInProgress(ref held)) if *held == before.build_id), "pass {pass}: {next} waits for the running recovery: {:?}", waits.map(|_| ()).map_err(|r| format!("{r:?}")));
        }
        e.converge().await;
        assert_eq!(e.born().last().map(String::as_str), Some(path));
    }
    assert_eq!(e.born().len(), 3);
    assert!(paths.iter().all(|p| e.fence_of(p).is_empty()), "no recovery fences by probe");
    e.holds_the_shape().await;
    assert_eq!(e.drift().await, None, "nothing is left to repair");
}

/// A successor fabric primary that never heard a dead sibling holds nothing of it but the durable
/// row its predecessor handed over: that row is the exact runtime to inspect.
#[tokio::test]
async fn a_successor_proves_a_sibling_admins_exit_from_its_durable_runtime_row() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    // A peer mesh's node-admin cohort is lost (its first admin hung and silent, so no live
    // mesh-primary is left to recover anything: mesh recovery); its second admin is one the
    // successor never heard.
    let peer = if e.fabric_primary().await.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let lost = format!("{peer}.admin.2");
    let lost = lost.as_str();
    let hung = format!("{peer}.admin.1");
    e.world.freeze(&e.node_id(&hung).await);
    e.unheard(&[hung.as_str()]).await;
    let old = e.node_id(lost).await;
    let row = {
        let (d, _) = e.book.get(&old).unwrap();
        rafka_node_admin_core::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }
    };
    e.world.kill(&old);
    // The successor's membership and view: everything but the lost admin.
    let heard = DigestBook::default();
    for d in e.book.all().into_iter().filter(|d| d.node.name.to_string() != lost) {
        heard.record(d);
    }
    let mut t = e.view.read().await.clone();
    t.nodes.retain(|n| n.name.to_string() != lost);
    rafka_node_admin_core::election::resolve(&mut t.nodes);
    let me: PathName = t.fabric_primary().unwrap().name.clone();
    let mut started = HashSet::new();
    let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
    let opened = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &heard, &e.provider, &*e.builds, &contexts, &tokio::sync::Mutex::new(()), std::slice::from_ref(&row), &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await;
    assert_eq!(opened, Some((before.build_id.clone(), before.attempt + 1)), "the durable row's exact runtime exited: the next attempt of the same Build is open");
    let action = e.accepted.current(&*e.builds).await.unwrap().action;
    assert!(matches!(&action, Some(rafka_node_admin_core::accepted::AttemptAction::Replace { path, .. }) if path.to_string() == lost), "the mesh-recovery repair of the durable row's birth: {action:?}");
}

#[tokio::test]
async fn a_durable_runtime_row_of_a_runtime_that_runs_opens_no_attempt() {
    let e = estate().await;
    let lost = "mesh1.admin.2";
    let old = e.node_id(lost).await;
    let row = {
        let (d, _) = e.book.get(&old).unwrap();
        rafka_node_admin_core::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }
    };
    e.world.freeze(&old);
    let heard = DigestBook::default();
    for d in e.book.all().into_iter().filter(|d| d.node.name.to_string() != lost) {
        heard.record(d);
    }
    let mut t = e.view.read().await.clone();
    t.nodes.retain(|n| n.name.to_string() != lost);
    rafka_node_admin_core::election::resolve(&mut t.nodes);
    let me: PathName = t.fabric_primary().unwrap().name.clone();
    let mut started = HashSet::new();
    let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
    let opened = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &heard, &e.provider, &*e.builds, &contexts, &tokio::sync::Mutex::new(()), std::slice::from_ref(&row), &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await;
    assert_eq!(opened, None, "a silent member whose runtime runs is held, never replaced");
}

/// CONTRACT (mesh-recovery.md:8-9 "A lost member of a mesh with a live mesh-primary is node
/// recovery, driven by that mesh-primary"; node-recovery.md): a secondary node-admin of a peer mesh
/// that its mesh-primary never heard and never launched, known to it only by the durable runtime row
/// its maker handed over, is watched by that mesh-primary's tick as a silent birth (`PendingReconnect`,
/// the row's exact node id and incarnation); once TRUE OFFLINE it is due for recovery, the provider
/// proves its exact runtime exited from the row, and the recovery submitted at the fabric-primary's
/// door is the next attempt of the same Build, executed by that mesh-primary, with a new node.id at
/// the path. The fabric-primary's drift, holding the same row, opens nothing. Must NOT happen: the
/// birth watched by nobody (no recovery ever), or a proven-drift Replace from the fabric-primary.
#[tokio::test]
async fn a_secondary_node_admin_known_only_by_its_durable_row_is_recovered_by_its_mesh_primary() {
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3051-durable-row");
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let fp = e.fabric_primary().await;
    let peer = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let mp: PathName = e.view.read().await.cohort_primary(peer, NodeKind::NodeAdmin).unwrap().name.clone();
    let lost = e.view.read().await.cohort(peer, NodeKind::NodeAdmin).map(|n| n.name.to_string()).find(|n| *n != mp.to_string()).unwrap();
    let old = e.node_id(&lost).await;
    let row = {
        let (d, _) = e.book.get(&old).unwrap();
        rafka_node_admin_core::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }
    };
    e.world.kill(&old);
    // Nobody heard the lost admin: it is in no view and no membership book, only in the row.
    let heard = DigestBook::default();
    for d in e.book.all().into_iter().filter(|d| d.node.name.to_string() != lost) {
        heard.record(d);
    }
    {
        let mut v = e.view.write().await;
        v.nodes.retain(|n| n.name.to_string() != lost);
        rafka_node_admin_core::election::resolve(&mut v.nodes);
    }
    let t = e.view.read().await.clone();
    assert_eq!(t.cohort_primary(peer, NodeKind::NodeAdmin).map(|n| n.name.clone()), Some(mp.clone()), "the mesh keeps its live primary");

    // The fabric-primary's drift folds the row and leaves the birth to that mesh-primary.
    let me: PathName = fp.parse().unwrap();
    let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
    let opened = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &heard, &e.provider, &*e.builds, &contexts, &tokio::sync::Mutex::new(()), std::slice::from_ref(&row), &mut HashSet::new(), &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await;
    assert_eq!(opened, None, "the fabric-primary opens nothing for a node-admin its mesh-primary recovers");

    // The mesh-primary's tick: what it watches, from its view and its durable rows.
    let accepted = e.accepted.current(&*e.builds).await.unwrap().topology;
    let births = rafka_node_admin_core::admin::durable_births(&t, std::slice::from_ref(&row), &|p| accepted.contains(p), &|id| heard.is_departed(id));
    let watched = rafka_node_admin_core::admin::watched_births(&t, &t.node(&mp).expect("the mesh-primary is in the view").node_id, peer, &births);
    let w = watched.get(&old).unwrap_or_else(|| panic!("{mp}'s tick watches {lost}, known only by its durable row: watched {:?}", watched.values().map(|n| n.name.to_string()).collect::<Vec<_>>()));
    assert_eq!((w.name.to_string(), w.incarnation_id.clone(), w.status), (lost.clone(), Some(row.incarnation_id.clone()), NodeStatus::PendingReconnect), "the row's exact birth, silent");
    // TRUE OFFLINE (the tickle's verdict): due for recovery at the seat holder, and at nobody else.
    let offline: HashSet<(NodeId, IncarnationId)> = [(row.node_id.clone(), row.incarnation_id.clone())].into();
    let due: Vec<String> = rafka_node_admin_core::admin::recovery_due(true, peer, &watched, &offline).iter().map(|n| n.name.to_string()).collect();
    assert_eq!(due, vec![lost.clone()], "{mp} inspects {lost} for recovery");
    // The provider's exact-exit proof, from the row.
    let proof = rafka_node_admin_core::admin::exit_proof_of(&e.provider, &None, Some(&row)).await;
    assert!(proof.is_ok(), "the row's exact runtime is proven exited: {proof:?}");

    // The submit at the fabric-primary's door, executed by the mesh-primary.
    let submitted = e.door(e.builds.clone()).await.open_recovery("build.create", row.name.clone(), row.node_id.clone(), row.incarnation_id.clone()).await;
    let Ok(rafka_node_admin_core::http::Recovery::Opened(o)) = submitted else { panic!("{lost}'s recovery was not opened: {:?}", submitted.map(|_| ()).map_err(|r| format!("{r:?}"))) };
    assert_eq!((o.build_id.clone(), o.attempt), (before.build_id.clone(), before.attempt + 1), "the same Build, one more attempt");
    e.converge().await;
    assert_eq!(e.dispatch.dispatched.lock().unwrap().clone(), vec![(mp.to_string(), before.attempt + 1)], "executed by its mesh-primary");
    assert_eq!(e.born(), vec![lost.clone()], "exactly the lost admin is recovered");
    assert_ne!(e.node_id(&lost).await, old, "a new node.id");
    e.holds_the_shape().await;
    let admitted: Vec<_> = capture.spans("rdm.node_admin.build.update.via-recovery").into_iter().filter(|(_, a)| a.get("node") == Some(&lost) && a.get("node_id") == Some(&old)).collect();
    assert_eq!(admitted.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["admitted".to_string()], "one recovery of the row's exact birth, admitted: {admitted:?}");
    assert!(capture.spans("rdm.node_admin.build.reject.via-drift-unprovable").is_empty(), "the fabric-primary's drift names nothing for the birth");
    eprintln!("span rdm.node_admin.build.update.via-recovery trace_id={} {:?}", admitted[0].0, admitted[0].1);
}

/// Two openers of one attempt number: the requested attempt (REST) and the proven-drift pass read the
/// same settled Build, so both compute the next attempt as `k + 1`. The first append takes the number;
/// the loser is told by name, never acknowledged for an attempt that carries someone else's action.
mod one_attempt_number {
    use super::*;
    use axum::response::IntoResponse;
    use rafka_node_admin_core::build_state::{AttemptOpened, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome};
    use rafka_node_admin_core::http::ControlPlane;
    use tokio::sync::Notify;

    /// The write a [`Gate`] stops at the log's door.
    #[derive(Clone, Copy, PartialEq)]
    enum Holds {
        /// An `open_attempt` of this reason, after its caller computed the number.
        Attempt(AttemptReason),
        /// The first `publish_accepted`: a submitted change past its check, inside its durable write.
        Acceptance,
    }

    /// The estate's Build log with one gate: the write it `holds` stops at the log's door until
    /// the cell lets it write.
    struct Gate {
        inner: Arc<MemoryBuildStateAdapter>,
        holds: Holds,
        arrived: Notify,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl BuildStateAdapter for Gate {
        async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
            if self.holds == Holds::Acceptance {
                self.arrived.notify_one();
                self.release.notified().await;
            }
            self.inner.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
            if self.holds == Holds::Attempt(o.reason) {
                self.arrived.notify_one();
                self.release.notified().await;
            }
            self.inner.open_attempt(o).await
        }
        async fn read_build(&self, b: &BuildId) -> Result<BuildProjection, BuildStateError> {
            self.inner.read_build(b).await
        }
        async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
            self.inner.list_active().await
        }
        async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
            self.inner.claim_attempt(c).await
        }
        async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), BuildStateError> {
            self.inner.adopt_claim(c).await
        }
        async fn append_step_receipt(&self, r: &BuildStepReceipt) -> Result<(), BuildStateError> {
            self.inner.append_step_receipt(r).await
        }
        async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
            self.inner.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
            self.inner.facts().await
        }
        async fn recovery_of(&self, path: &rafka_node_admin_core::model::PathName, node_id: &rafka_node_admin_core::model::NodeId, incarnation: &rafka_node_admin_core::model::IncarnationId) -> Result<Option<(rafka_node_admin_core::build::BuildId, u32, String)>, BuildStateError> {
            self.inner.recovery_of(path, node_id, incarnation).await
        }
        async fn forget(&self, b: &BuildId) -> Result<(), BuildStateError> {
            self.inner.forget(b).await
        }
    }

    fn gate(e: &Estate, holds: Holds) -> Arc<Gate> {
        Arc::new(Gate { inner: e.builds.clone(), holds, arrived: Notify::new(), release: Notify::new() })
    }

    /// The fabric-primary's REST door over `builds`, on the estate's view and accepted Build.
    async fn door(e: &Estate, builds: Arc<dyn BuildStateAdapter>) -> Arc<ControlPlane> {
        let me: PathName = e.fabric_primary().await.parse().unwrap();
        let mut cp = ControlPlane::new(builds, e.accepted.clone(), me, e.view.read().await.clone(), crate::adopted_time());
        cp.topology = e.view.clone();
        Arc::new(cp)
    }

    /// A peer mesh loses every node-admin: their runtimes exited and the view holds them unheard, so
    /// no mesh-primary is left to recover them and the drift pass has a birth to prove (mesh
    /// recovery; a member of a mesh with a live mesh-primary is that mesh-primary's). Returns the
    /// Build's attempt before anything opens.
    async fn lose_a_node(e: &Estate) -> u32 {
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let peer = if e.fabric_primary().await.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
        let lost = [format!("{peer}.admin.1"), format!("{peer}.admin.2")];
        for l in &lost {
            e.world.kill(&e.node_id(l).await);
        }
        e.unheard(&[lost[0].as_str(), lost[1].as_str()]).await;
        before.attempt
    }

    async fn opened_facts(e: &Estate, attempt: u32) -> Vec<AttemptOpened> {
        e.builds.facts().await.unwrap().into_iter().filter_map(|f| match f { BuildFact::Opened(o) if o.attempt == attempt => Some(o), _ => None }).collect()
    }

    /// The proven-drift pass over `builds`, under an admission of its own: the number race of two
    /// authorities, which only the log's insert-and-fail decides.
    async fn drift_pass(e: &Estate, builds: &dyn BuildStateAdapter) -> Option<(BuildId, u32)> {
        drift_pass_under(e, builds, &tokio::sync::Mutex::new(())).await
    }

    /// The proven-drift pass over `builds`, under `admission`.
    async fn drift_pass_under(e: &Estate, builds: &dyn BuildStateAdapter, admission: &tokio::sync::Mutex<()>) -> Option<(BuildId, u32)> {
        let me: PathName = e.fabric_primary().await.parse().unwrap();
        let t = e.view.read().await.clone();
        let mut started = HashSet::new();
        rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &e.book, &e.provider, builds, &rafka_node_admin_core::build_claim::AttemptContexts::in_memory(), admission, &[], &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await
    }

    /// CONTRACT: the proven-drift pass has read the settled Build and computed attempt k+1 for a lost
    /// node when a requested restart of another node opens k+1 first. The restart holds the number:
    /// the drift pass is told it lost, opens nothing and returns no attempt to execute, and the one
    /// Opened fact of k+1 is the restart's. Canon: build_state.rs `AttemptOpened` (insert-and-fail on
    /// `(build_id, attempt)`), CLAUDE.md "an acknowledged write is the write that executes".
    #[tokio::test]
    async fn a_drift_attempt_that_loses_its_number_to_a_requested_restart_opens_nothing() {
        let e = estate().await;
        let k = lose_a_node(&e).await;
        let g = gate(&e, Holds::Attempt(AttemptReason::ProvenDrift));
        let cp = door(&e, e.builds.clone()).await;
        // The drift pass either stops at the log's door with its number computed, or it ended
        // without opening anything: that is the cell's failure, named, never a wait.
        let drift = drift_pass(&e, &*g);
        tokio::pin!(drift);
        tokio::select! {
            biased;
            _ = g.arrived.notified() => {}
            ended = &mut drift => panic!("the drift pass ended without reaching the Build log's door to open attempt {}: it proved no loss ({ended:?})", k + 1),
        }
        let rest = cp.open_attempt("POST /api/nodes/{name}/restart", AttemptReason::Restart, "mesh2.rpc.1".parse().unwrap(), rafka_node_admin_core::http::ActionKind::Restart, None).await;
        g.release.notify_one();
        let drift = drift.await;
        let rest = rest.expect("the restart took the free number");
        assert_eq!(rest.attempt, k + 1);
        let held = opened_facts(&e, k + 1).await;
        assert_eq!(held.len(), 1, "one Opened fact for attempt {}: {held:?}", k + 1);
        assert_eq!((held[0].reason, held[0].opened_by.as_str()), (AttemptReason::Restart, cp.me.to_string().as_str()), "the number is the restart's: {held:?}");
        assert_eq!(drift, None, "the drift pass lost the number: it opened nothing and has no attempt to execute");
    }

    /// CONTRACT: the requested restart has read the settled Build and computed attempt k+1 when the
    /// proven-drift pass opens k+1 for a lost node first. The drift attempt holds the number: the
    /// restart is refused 409 `attempt-taken` naming the attempt, never acknowledged 202 for an attempt
    /// that replaces the lost node and restarts nothing, and no second fact is appended.
    #[tokio::test]
    async fn a_requested_restart_that_loses_its_number_to_a_drift_attempt_is_refused_by_name() {
        let e = estate().await;
        let k = lose_a_node(&e).await;
        let g = gate(&e, Holds::Attempt(AttemptReason::Restart));
        let cp = door(&e, g.clone()).await;
        let (rest, drift) = tokio::join!(cp.open_attempt("POST /api/nodes/{name}/restart", AttemptReason::Restart, "mesh2.rpc.1".parse().unwrap(), rafka_node_admin_core::http::ActionKind::Restart, None), async {
            g.arrived.notified().await;
            let d = drift_pass(&e, &*e.builds).await;
            g.release.notify_one();
            d
        });
        assert_eq!(drift, Some((e.accepted.build_id().await.unwrap(), k + 1)), "the drift pass took the free number");
        let held = opened_facts(&e, k + 1).await;
        assert_eq!(held.len(), 1, "one Opened fact for attempt {}: {held:?}", k + 1);
        assert_eq!(held[0].reason, AttemptReason::ProvenDrift, "the number is the drift attempt's: {held:?}");
        let refusal = rest.expect_err("the restart lost the number and was told so");
        let response = refusal.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
        let body: serde_json::Value = serde_json::from_slice(&http_body_util::BodyExt::collect(response.into_body()).await.unwrap().to_bytes()).unwrap();
        assert_eq!(body["error"], "attempt-taken", "{body}");
        assert!(body["detail"].as_str().unwrap().contains(&format!("{}", k + 1)), "the refusal names the attempt: {body}");
    }

    /// The cell's span directory: the gate's, else this cell's own under the workspace target.
    fn evidence_dir(cell: &str) -> std::path::PathBuf {
        match std::env::var("I143_ACCEPTANCE_DIR") {
            Ok(d) => std::path::PathBuf::from(d),
            Err(_) => std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/3061/unit").join(cell),
        }
    }

    /// Every span this process emitted, from the evidence exporter's JSONL files in `dir`.
    fn collect_spans(dir: &std::path::Path) -> Vec<serde_json::Value> {
        let mut spans = Vec::new();
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.file_name().is_some_and(|n| n.to_string_lossy().ends_with(".spans.jsonl")) {
                spans.extend(std::fs::read_to_string(&p).unwrap().lines().filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok()));
            }
        }
        spans
    }

    /// CONTRACT: the proven-drift pass has read the accepted Build settled and computed attempt k+1
    /// for a lost node while a submitted topology change, past the same check, is inside its durable
    /// write. The submitted change is admitted as the next Build; the drift pass opens nothing on
    /// the Build the pointer no longer names, writes no Opened fact, and returns no attempt to
    /// execute. One Build is active: the submitted one. Must NOT happen: an attempt of a superseded
    /// Build opened beside the new Build, two plans of two topologies in flight. Canon:
    /// ops-naming-proposal.md "Two topology changes ... never run together: one Build is in flight".
    #[tokio::test]
    async fn a_drift_attempt_checked_while_a_submitted_change_is_written_opens_nothing() {
        if crate::own_process::delegated(module_path!(), "a_drift_attempt_checked_while_a_submitted_change_is_written_opens_nothing") {
            return;
        }
        let dir = evidence_dir("a_drift_attempt_checked_while_a_submitted_change_is_written_opens_nothing");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RDM_EVIDENCE_DIR", &dir);
        let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-node-admin-core-s35");
        let root = tracing::info_span!("rdm.test.build.admission.drift-versus-submit");
        use tracing::Instrument as _;
        let e = estate().await;
        let k = lose_a_node(&e).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let g = gate(&e, Holds::Acceptance);
        let cp = door(&e, g.clone()).await;
        let submit = tokio::spawn({
            let cp = cp.clone();
            async move { cp.submit("POST /api/topology", rafka_node_admin_core::accepted::TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: rafka_mesh_entity::NodeKind::RpcNode }).await }.instrument(root.clone())
        });
        g.arrived.notified().await;
        // The drift pass of the same fabric-primary: the admission its door holds.
        let mut pass = Box::pin(drift_pass_under(&e, &*e.builds, &cp.admission).instrument(root.clone()));
        // One poll of the drift pass while the submitted change is held inside its write.
        let early = tokio::select! {
            biased;
            r = &mut pass => Some(r),
            _ = std::future::ready(()) => None,
        };
        let waited = early.is_none();
        g.release.notify_one();
        let admitted = submit.await.unwrap().expect("the submitted change is admitted").build_id;
        let drift = match early {
            Some(r) => r,
            None => (&mut pass).await,
        };
        drop(pass);
        assert_eq!(drift, None, "the drift pass opened an attempt of {} beside the admitted Build {admitted}", before.build_id);
        assert!(waited, "the drift pass waited at the admission the submitted change held");
        let opened: Vec<AttemptOpened> = opened_facts(&e, k + 1).await.into_iter().filter(|o| o.build_id == before.build_id).collect();
        assert!(opened.is_empty(), "no attempt of the superseded Build is open: {opened:?}");
        let active = e.builds.list_active().await.unwrap();
        assert_eq!(active.iter().map(|b| b.build_id.clone()).collect::<Vec<_>>(), vec![admitted.clone()], "one Build is active: the submitted one");
        drop(root);
        drop(telemetry);
        let spans = collect_spans(&dir);
        let refused: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.reject.via-build-in-progress").collect();
        assert_eq!(refused.len(), 1, "the drift pass is refused once, by name: {refused:?}");
        assert!(!spans.iter().any(|s| s["name"] == "rdm.node_admin.build.update.via-proven-drift"), "a refused drift pass opened nothing and names no attempt update");
        let created = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.create.via-rest").expect("the submitted change's span");
        assert_eq!(refused[0]["trace_id"], created["trace_id"], "the refusal and the admitted Build are one story");
        let a = &refused[0]["attributes"];
        assert_eq!(a["route"], "proven-drift", "{a}");
        assert_eq!(a["build_id"], before.build_id.0.as_str(), "{a}");
        assert!(a["accepted_now"].as_str().unwrap_or_default().contains(admitted.0.as_str()), "the refusal names the Build admitted first: {a}");
        println!("trace_id={} admitted={admitted} refused_drift_on={}", refused[0]["trace_id"].as_str().unwrap_or_default(), before.build_id);
    }

    /// CONTRACT: the proven-drift pass for a peer mesh whose rebirth the ladder decided (its two
    /// node-admins exited, the mesh unheard) is refused at the admission, because a requested restart
    /// of the same Build was admitted first. A refused pass opened nothing, so it spends nothing:
    /// once the restart's attempt completes, the next pass releases the mesh's first recovering
    /// node-admin as attempt k+2. Must NOT happen: the refused pass records the mesh's one release as
    /// spent, and the decided mesh's node-admins stay held for as long as the Build stays accepted.
    #[tokio::test]
    async fn a_drift_pass_refused_at_the_admission_spends_no_recovery_release() {
        use rafka_node_admin_core::investigate::PeerMesh;
        let e = estate().await;
        let fp = e.fabric_primary().await;
        let own = fp.split('.').next().unwrap().to_string();
        let peer_mesh = SHAPE.iter().map(|(m, _, _)| (*m).to_string()).find(|m| *m != own).expect("a peer mesh");
        let admins = [format!("{peer_mesh}.admin.1"), format!("{peer_mesh}.admin.2")];
        for p in &admins {
            let id = e.node_id(p).await;
            e.world.exit_with(&id, Some(1));
        }
        e.unheard(&admins.iter().map(String::as_str).collect::<Vec<_>>()).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let g = gate(&e, Holds::Attempt(AttemptReason::Restart));
        let cp = door(&e, g.clone()).await;
        let me: PathName = fp.parse().unwrap();
        let decided = |m: &str| (m == peer_mesh).then_some(PeerMesh { unheard_ms: Some(30_000), rebirth_decided: true });
        let mut started = HashSet::new();
        let mut held = HashSet::new();
        let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
        let time = crate::adopted_time();
        let hold = |m: &str| decided(m).and_then(|p| p.unheard_ms);
        let defers = |_: &str| false;
        let rebirth = |m: &str| decided(m).is_some_and(|p| p.rebirth_decided);
        let restart = tokio::spawn({
            let (cp, path): (_, PathName) = (cp.clone(), format!("{own}.rpc.1").parse().unwrap());
            async move { cp.open_attempt("POST /api/nodes/{name}/restart", AttemptReason::Restart, path, rafka_node_admin_core::http::ActionKind::Restart, None).await }
        });
        g.arrived.notified().await;
        let t = e.view.read().await.clone();
        let me_id = own_id(&t, &me);
        let refused = {
            let mut pass = Box::pin(rafka_node_admin_core::admin::reconcile_drift(&time, &me, &me_id, &t, &e.accepted, &e.book, &e.provider, &*e.builds, &contexts, &cp.admission, &[], &mut started, &mut held, &hold, &defers, &rebirth, &|_, _| {}));
            // One poll while the restart holds the admission inside its write.
            let early = tokio::select! {
                biased;
                r = &mut pass => Some(r),
                _ = std::future::ready(()) => None,
            };
            assert!(early.is_none(), "the drift pass waits at the admission the restart holds: {early:?}");
            g.release.notify_one();
            let r = restart.await.unwrap().expect("the restart was admitted first");
            assert_eq!(r.attempt, before.attempt + 1);
            pass.await
        };
        assert_eq!(refused, None, "the restart holds attempt k+1: the drift pass opened nothing");
        e.converge().await;
        let settled = e.accepted.current(&*e.builds).await.unwrap();
        assert_eq!((settled.build_id.clone(), settled.attempt), (before.build_id.clone(), before.attempt + 1), "the restart's attempt completed on the same Build");
        let t = e.view.read().await.clone();
        let opened = rafka_node_admin_core::admin::reconcile_drift(&time, &me, &own_id(&t, &me), &t, &e.accepted, &e.book, &e.provider, &*e.builds, &contexts, &cp.admission, &[], &mut started, &mut held, &hold, &defers, &rebirth, &|_, _| {}).await;
        assert_eq!(opened, Some((before.build_id.clone(), before.attempt + 2)), "the refused pass spent nothing: the decision still releases the first recovering node-admin (started={started:?})");
        let Some(rafka_node_admin_core::accepted::AttemptAction::Replace { path, .. }) = e.accepted.current(&*e.builds).await.unwrap().action else { panic!("not a Replace") };
        assert_eq!(path.to_string(), admins[0], "the first admin in path order");
    }
}

/// CONTRACT: a fabric primary that took the seat holds a peer mesh it never heard on the backbone as
/// `ready` from topology alone (topology is not liveness). Once the ladder has decided that mesh's
/// rebirth, its node-admins are proven from their exact runtimes and repaired like any unheard birth.
/// Must NOT happen: the decided mesh waits forever because its view never says it is silent.
#[tokio::test]
async fn a_decided_peer_mesh_held_ready_from_topology_is_proven_and_repaired() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let me: PathName = e.fabric_primary().await.parse().unwrap();
    let lost = if me.mesh == "mesh1" { "mesh2" } else { "mesh1" };
    for i in 1..=2 {
        let id = e.node_id(&format!("{lost}.admin.{i}")).await;
        e.world.kill(&id);
    }
    let t = e.view.read().await.clone();
    assert!(t.cohort(lost, NodeKind::NodeAdmin).all(|a| a.status.is_live()), "the view still holds the dead admins ready");
    let mut started = HashSet::new();
    let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
    let undecided = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &e.book, &e.provider, &*e.builds, &contexts, &tokio::sync::Mutex::new(()), &[], &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await;
    assert_eq!(undecided, None, "nothing names the mesh's admins before the ladder decides");
    let seen = std::sync::Mutex::new(Vec::new());
    let decided = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &e.book, &e.provider, &*e.builds, &contexts, &tokio::sync::Mutex::new(()), &[], &mut started, &mut Default::default(), &|_| None, &|_| false, &|m| m == lost, &|n, i| seen.lock().unwrap().push((n.clone(), i.clone()))).await;
    assert_eq!(decided, Some((before.build_id.clone(), before.attempt + 1)), "the decided mesh's exited admins open the next attempt");
    assert_eq!(seen.lock().unwrap().len(), 2, "both proven exits are handed to the view, so neither dead admin holds the mesh's seat");
}

/// CONTRACT: a departure is not drift. A birth whose proven departure the authority holds
/// (`NodeDeleted`) may still have its durable runtime row; that row names a runtime that left on
/// purpose, and no attempt repairs it. Must NOT happen: the deleted node is re-created as drift.
#[tokio::test]
async fn a_durable_runtime_row_of_a_departed_birth_opens_no_attempt() {
    let e = estate().await;
    let lost = "mesh1.admin.2";
    let old = e.node_id(lost).await;
    let (row, digest) = {
        let (d, _) = e.book.get(&old).unwrap();
        (rafka_node_admin_core::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }, d)
    };
    e.world.kill(&old);
    let heard = DigestBook::default();
    for d in e.book.all().into_iter().filter(|d| d.node.name.to_string() != lost) {
        heard.record(d);
    }
    heard.depart(rafka_mesh_entity::LifecycleOp { build_id: "b".into(), attempt: 1, operation: format!("retire-node:{lost}"), node_id: digest.node.node_id.clone(), incarnation: digest.node.incarnation.clone(), name: digest.node.name.clone(), event_at_rafka_ms: 1 });
    let mut t = e.view.read().await.clone();
    t.nodes.retain(|n| n.name.to_string() != lost);
    rafka_node_admin_core::election::resolve(&mut t.nodes);
    let me: PathName = t.fabric_primary().unwrap().name.clone();
    let mut started = HashSet::new();
    let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
    let opened = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &heard, &e.provider, &*e.builds, &contexts, &tokio::sync::Mutex::new(()), std::slice::from_ref(&row), &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await;
    assert_eq!(opened, None, "a proven departure is held: its runtime row is not drift");
}

/// R-D1: a peer mesh the fabric primary holds unheard is held as a mesh. The provider's proof that one of
/// its births exited is kept as evidence and authorizes no Restart, Replace or NodeDeleted; a heard mesh is
/// repaired as ever; the hold ends with the silence and leaves nothing behind.
mod unheard_mesh_hold {
    use super::*;
    use rafka_node_admin_core::investigate::PeerMesh;

    type Held = HashSet<(NodeId, IncarnationId)>;

    /// The fabric primary's mesh, and the other one.
    async fn meshes(e: &Estate) -> (String, String) {
        let fp = e.fabric_primary().await;
        let own = fp.split('.').next().unwrap().to_string();
        let peer = SHAPE.iter().map(|(m, _, _)| (*m).to_string()).find(|m| *m != own).expect("a peer mesh");
        (own, peer)
    }

    async fn pass(e: &Estate, started: &mut HashSet<(BuildId, u32, Vec<String>)>, held: &mut Held, peer: &(dyn Fn(&str) -> Option<PeerMesh> + Sync)) -> Option<(BuildId, u32)> {
        let me: PathName = e.fabric_primary().await.parse().expect("a fabric-primary");
        let t = e.view.read().await.clone();
        rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &e.book, &e.provider, &*e.builds, &rafka_node_admin_core::build_claim::AttemptContexts::in_memory(), &tokio::sync::Mutex::new(()), &[], started, held, &|m| peer(m).and_then(|p| p.unheard_ms), &|m| peer(m).is_some_and(|p| !p.rebirth_decided), &|m| peer(m).is_some_and(|p| p.rebirth_decided), &|_, _| {}).await
    }

    const UNHEARD: Option<PeerMesh> = Some(PeerMesh { unheard_ms: Some(4_000), rebirth_decided: false });
    const HEARD: Option<PeerMesh> = Some(PeerMesh { unheard_ms: None, rebirth_decided: false });

    #[tokio::test]
    async fn an_exited_birth_of_an_unheard_peer_mesh_opens_no_attempt_and_is_kept_as_evidence_once() {
        let e = estate().await;
        let (_, peer_mesh) = meshes(&e).await;
        let path = format!("{peer_mesh}.rpc.1");
        let id = e.node_id(&path).await;
        e.world.exit_with(&id, Some(137));
        e.unheard(&[path.as_str()]).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        let unheard = |_: &str| UNHEARD;
        assert_eq!(pass(&e, &mut started, &mut held, &unheard).await, None, "the mesh is unheard: its exited birth authorizes nothing");
        assert_eq!(held.len(), 1, "the evidence of the one exited birth is held: {held:?}");
        assert_eq!(pass(&e, &mut started, &mut held, &unheard).await, None);
        assert_eq!(held.len(), 1, "the same birth is one hold, not one per pass");
        let after = e.accepted.current(&*e.builds).await.unwrap();
        assert_eq!(after.attempt, before.attempt, "no attempt was opened");
        assert!(e.born().is_empty(), "no node was created: {:?}", e.born());
    }

    /// The hold ends with the silence and leaves nothing behind; the exited ordinary birth is then its
    /// own mesh-primary's to recover (node-recovery.md), never the fabric-primary's.
    #[tokio::test]
    async fn the_hold_ends_when_the_mesh_is_heard_again_and_the_exited_birth_is_left_to_its_mesh_primary() {
        let e = estate().await;
        let (_, peer_mesh) = meshes(&e).await;
        let path = format!("{peer_mesh}.rpc.1");
        let id = e.node_id(&path).await;
        e.world.exit_with(&id, Some(137));
        e.unheard(&[path.as_str()]).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        assert_eq!(pass(&e, &mut started, &mut held, &|_| UNHEARD).await, None);
        assert_eq!(held.len(), 1);
        let opened = pass(&e, &mut started, &mut held, &|_| HEARD).await;
        assert_eq!(opened, None, "the mesh is heard again: its ordinary member is its mesh-primary's to recover");
        assert!(held.is_empty(), "the hold left nothing behind: {held:?}");
        let o = e.recover(&path).await;
        assert_eq!((o.build_id, o.attempt), (before.build_id.clone(), before.attempt + 1));
        assert_eq!(e.born(), vec![path.clone()], "the mesh-primary's recovery puts the path back");
    }

    #[tokio::test]
    async fn a_birth_of_the_fabric_primarys_own_mesh_is_never_held() {
        let e = estate().await;
        let path = e.sibling_admin().await;
        let id = e.node_id(&path).await;
        e.world.exit_with(&id, Some(1));
        e.unheard(&[path.as_str()]).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        let opened = pass(&e, &mut started, &mut held, &|_| UNHEARD).await;
        assert_eq!(opened, None, "the own mesh's secondary node-admin is its mesh-primary's to recover, never the drift's");
        assert!(held.is_empty(), "the own mesh is heard by definition: nothing of it is held");
        let o = e.recover(&path).await;
        assert_eq!((o.build_id, o.attempt), (before.build_id.clone(), before.attempt + 1));
        assert_eq!(e.born(), vec![path.clone()], "its mesh-primary's recovery puts the path back");
    }

    #[tokio::test]
    async fn only_the_first_recovering_node_admin_of_a_reborn_mesh_is_replaced_from_here() {
        let e = estate().await;
        let (_, peer_mesh) = meshes(&e).await;
        let admins = [format!("{peer_mesh}.admin.1"), format!("{peer_mesh}.admin.2")];
        let rpc = format!("{peer_mesh}.rpc.1");
        for p in admins.iter().chain([&rpc]) {
            let id = e.node_id(p).await;
            e.world.exit_with(&id, Some(1));
        }
        let all: Vec<&str> = admins.iter().map(String::as_str).chain([rpc.as_str()]).collect();
        e.unheard(&all).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        assert_eq!(pass(&e, &mut started, &mut held, &|_| UNHEARD).await, None, "before the ladder's decision the mesh's node-admins wait for it");
        let rpc_id = e.node_id(&rpc).await;
        assert_eq!(held.iter().map(|(n, _)| n.to_string()).collect::<Vec<_>>(), vec![rpc_id], "only the ordinary node's exit is held as evidence; the admins wait for the decision");
        let decided = |_: &str| Some(PeerMesh { unheard_ms: Some(30_000), rebirth_decided: true });
        let opened = pass(&e, &mut started, &mut held, &decided).await;
        assert_eq!(opened, Some((before.build_id.clone(), before.attempt + 1)), "the decision releases the first recovering node-admin");
        let Some(rafka_node_admin_core::accepted::AttemptAction::Replace { path, .. }) = e.accepted.current(&*e.builds).await.unwrap().action else { panic!("not a Replace") };
        assert_eq!(path.to_string(), admins[0], "the first admin in path order");
        let first = e.node_id(&admins[0]).await;
        assert!(!held.iter().any(|(n, _)| n.as_str() == first), "the admin the decision released is not held");
        // The restore runs on the same Build (mesh-investigation.md step 6): its attempt is claimed for the
        // fabric-primary, its receipt written and the Build settled; the view holds the restored admin's
        // new birth, which the fabric-primary has not heard on the backbone yet.
        use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter as _, BuildStepReceipt, ClaimOutcome, StepOutcome};
        let restore = before.attempt + 1;
        let fp = e.fabric_primary().await;
        assert_eq!(e.builds.claim_attempt(&BuildAttemptClaim { build_id: before.build_id.clone(), attempt: restore, executor: fp }).await.unwrap(), ClaimOutcome::Won);
        e.builds
            .append_step_receipt(&BuildStepReceipt { build_id: before.build_id.clone(), attempt: restore, operation: format!("replace-node:{}", admins[0]), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None })
            .await
            .unwrap();
        e.builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: before.build_id.clone(), attempt: restore, outcome: AttemptOutcome::Converged }).await.unwrap();
        {
            let fabric_id = e.view.read().await.fabric.id.clone();
            let reborn = birth(&e.world, &e.book, &fabric_id, &admins[0], None);
            let mut v = e.view.write().await;
            v.nodes.retain(|n| n.name.to_string() != admins[0]);
            v.nodes.push(reborn);
        }
        // The decision released one node-admin, and the Build says which: a reconcile pass that holds
        // nothing of the earlier passes (a fresh `started`) reads it there, so the second node-admin's exit
        // is evidence, not an attempt, until the mesh is heard again.
        let mut fresh = HashSet::new();
        assert_eq!(pass(&e, &mut fresh, &mut held, &decided).await, None, "no second node-admin of the reborn mesh is replaced from here");
        let second = e.node_id(&admins[1]).await;
        assert!(held.iter().any(|(n, _)| n.as_str() == second), "the second node-admin's exit is held as evidence: {held:?}");
        assert_eq!(e.accepted.current(&*e.builds).await.unwrap().attempt, restore, "no attempt was opened after the restore");
    }
}

/// CONTRACT: a live birth the accepted topology does not name is surplus, retired by an attempt of its
/// own, UNLESS the Build already receipted its retirement (`NodeDeleting` of that exact birth): that
/// operation retires it, and this view is only late in hearing it go.
#[tokio::test]
async fn a_birth_whose_retirement_the_build_receipted_is_not_surplus() {
    use rafka_node_admin_core::build_state::{BuildStateAdapter, BuildStepReceipt, StepOutcome};
    for receipted in [false, true] {
        let e = estate().await;
        let current = e.accepted.current(&*e.builds).await.unwrap();
        let extra = birth(&e.world, &e.book, &e.view.read().await.fabric.id.clone(), "mesh1.rpc.9", None);
        e.view.write().await.nodes.push(extra.clone());
        if receipted {
            let op = rafka_mesh_entity::LifecycleOp {
                build_id: current.build_id.to_string(),
                attempt: current.attempt,
                operation: "retire-node:mesh1.rpc.9".into(),
                node_id: extra.node_id.clone(),
                incarnation: extra.incarnation_id.clone().unwrap(),
                name: extra.name.clone(),
                event_at_rafka_ms: 0,
            };
            e.builds
                .append_step_receipt(&BuildStepReceipt { build_id: current.build_id.clone(), attempt: current.attempt, operation: op.operation.clone(), step: "NodeDeleting".into(), outcome: StepOutcome::Complete, output: serde_json::to_value(&op).ok(), executor: None })
                .await
                .unwrap();
            assert_eq!(e.drift().await, None, "a receipted retirement is not surplus");
        } else {
            assert!(e.drift().await.is_some(), "an unreceipted live birth outside the accepted topology is surplus");
        }
    }
}

/// CONTRACT: a removed mesh whose shutdown ended incomplete is resumed from evidence. The accepted
/// Build's topology no longer names the leaving mesh (the one without the fabric seat), its attempt
/// failed (the leaving primary's member workflows were incomplete), and the view still holds that
/// mesh's births live (an incomplete leave leaves at least its final primary running and live).
/// With no `MeshLeft` receipt under `shutdown-mesh:<mesh id>` the fabric-primary opens the next
/// attempt of the same Build, with no action, and that attempt's plan is the mesh's shutdown again; once `MeshLeft` is recorded it opens
/// nothing (mesh-leave.md, "Failure/certainty behavior": the workflow stays incomplete, resumed
/// from its receipts).
#[test]
fn a_removed_mesh_still_live_with_no_mesh_left_receipt_opens_its_shutdown_again() {
    let cap = crate::mesh_leave_resume::capture();
    let trace_id = cap.run(async {
    let root = tracing::info_span!("cell.mesh-leave-reopen");
    let trace_id = crate::mesh_leave_resume::trace_id(&root);
    let _in_root = root.enter();
    use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, BuildStepReceipt, StepOutcome};
    for left in [false, true] {
        let e = estate().await;
        let fp = e.fabric_primary().await;
        // The leaving mesh is the one that does not hold the fabric seat (the seat is handed over first).
        let leaving = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
        let mut topology = e.accepted.current(&*e.builds).await.unwrap().topology;
        topology.meshes.remove(leaving);
        let build_id = BuildId::mint();
        e.builds.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: topology.clone(), submitted_change: None, submitted_at_ms: 1 }).await.unwrap();
        e.builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: fp.clone() }).await.unwrap();
        let leaving_id = e.view.read().await.meshes.iter().find(|m| m.name == leaving).and_then(|m| m.id.clone()).unwrap();
        let outer = rafka_node_admin_core::mesh_leave::operation(&leaving_id);
        if left {
            e.builds
                .append_step_receipt(&BuildStepReceipt { build_id: build_id.clone(), attempt: 1, operation: outer.clone(), step: rafka_node_admin_core::mesh_leave::STEP_MESH_LEFT.into(), outcome: StepOutcome::Complete, output: None, executor: Some(fp.clone()) })
                .await
                .unwrap();
        }
        e.builds
            .append_attempt_receipt(&BuildAttemptReceipt { build_id: build_id.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: format!("{outer}: {leaving}'s primary reported its member workflows incomplete") } })
            .await
            .unwrap();
        e.accepted.point(&build_id, 1, "test").await.unwrap();
        let opened = e.drift().await;
        if left {
            assert_eq!(opened, None, "a mesh whose MeshLeft is recorded has left: nothing is opened");
            continue;
        }
        assert_eq!(opened, Some((build_id.clone(), 2)), "the removed mesh still live with no MeshLeft opens the next attempt of the same Build");
        let current = e.accepted.current(&*e.builds).await.unwrap();
        assert_eq!(current.state, rafka_node_admin_core::build_state::BuildState::Pending, "attempt 2 is open to be claimed");
        let view = e.view.read().await.clone();
        assert_eq!(
            rafka_node_admin_core::accepted::plan(&current.topology, &view, None).operations,
            vec![BuildOperation::ShutdownMesh { mesh: leaving.into(), mesh_id: leaving_id.clone() }],
            "the opened attempt plans {leaving}'s shutdown again"
        );
    }
    trace_id
    });
    let spans = cap.spans();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_leave_reopen");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let drift: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-proven-drift" && s["trace_id"] == trace_id.as_str()).collect();
    assert!(drift.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["action"] == "none" && s["attributes"]["scope"].as_str().is_some_and(|x| x.contains("shutdown-mesh:"))), "the opened attempt names the incomplete shutdown: {drift:?}");
}

/// CONTRACT (R11; mesh-leave.md, "Resume after the primary's loss"): a removed mesh whose leave was
/// admitted (its AcceptedRoster recorded, no MeshLeft) and whose every birth went silent to this
/// fabric-primary, so the ladder holds the mesh unheard and the view holds none of its births live,
/// still has its shutdown planned again. The resume carries its own proof: it records the lost final
/// primary's exact provider Exited before anything is created for the mesh, and refuses by name
/// when the exit is unproven (R-D1: unheard is not dead). The unheard hold fences restart, replace
/// and NodeDeleted of the mesh's births; it never fences the shutdown's resume, which with no
/// surviving admin is the only way the leave can finish.
#[test]
fn a_removed_mesh_held_unheard_with_no_live_admin_opens_its_shutdown_again() {
    use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter};
    let cap = crate::mesh_leave_resume::capture();
    let (opened, build_id, leaving, leaving_id, plan, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.mesh-leave-reopen-unheard");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        let _in_root = root.enter();
        let e = estate().await;
        let fp = e.fabric_primary().await;
        let leaving = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
        let mut topology = e.accepted.current(&*e.builds).await.unwrap().topology;
        topology.meshes.remove(leaving);
        let build_id = BuildId::mint();
        e.builds.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology, submitted_change: None, submitted_at_ms: 1 }).await.unwrap();
        e.builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: fp.clone() }).await.unwrap();
        let leaving_id = e.view.read().await.meshes.iter().find(|m| m.name == leaving).and_then(|m| m.id.clone()).unwrap();
        let outer = rafka_node_admin_core::mesh_leave::operation(&leaving_id);
        // The leave was admitted: its roster, with a node-admin as final primary, is recorded.
        let members: Vec<Node> = e.view.read().await.nodes.iter().filter(|n| n.mesh == leaving).cloned().collect();
        let roster: Vec<rafka_node_admin_core::mesh_leave::RosterBirth> = members.iter().map(|n| rafka_node_admin_core::mesh_leave::RosterBirth::of(n).unwrap()).collect();
        let last = roster.iter().find(|b| b.node.contains(".admin.")).cloned().unwrap();
        let proof = rafka_node_admin_core::deployment::pipeline::RuntimeProof { deployment_id: format!("dep-{}", last.node), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: last.incarnation_id.clone() };
        let accepted_roster = rafka_node_admin_core::mesh_leave::ExitManifest { mesh_id: leaving_id.to_string(), roster, final_primary: last, final_runtime: Some(proof), receipts: vec![] };
        e.builds
            .append_step_receipt(&rafka_node_admin_core::build_state::BuildStepReceipt { build_id: build_id.clone(), attempt: 1, operation: outer.clone(), step: rafka_node_admin_core::mesh_leave::STEP_ACCEPTED_ROSTER.into(), outcome: rafka_node_admin_core::build_state::StepOutcome::Complete, output: Some(serde_json::to_value(&accepted_roster).unwrap()), executor: Some(fp.clone()) })
            .await
            .unwrap();
        // Every birth of it is silent past the floor here: its admins were lost, and its ordinary
        // nodes reached this fabric-primary only through an admin of their own mesh.
        {
            let mut v = e.view.write().await;
            for n in v.nodes.iter_mut().filter(|n| n.mesh == leaving) {
                n.status = NodeStatus::PendingReconnect;
                n.is_primary = false;
            }
        }
        e.builds
            .append_attempt_receipt(&BuildAttemptReceipt { build_id: build_id.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: format!("{outer}: the leaving primary sent no valid mesh-leave") } })
            .await
            .unwrap();
        e.accepted.point(&build_id, 1, "test").await.unwrap();
        let me: PathName = fp.parse().unwrap();
        let t = e.view.read().await.clone();
        let hold = |m: &str| (m == leaving).then_some(120_000u64);
        let opened = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &e.book, &e.provider, &*e.builds, &rafka_node_admin_core::build_claim::AttemptContexts::in_memory(), &tokio::sync::Mutex::new(()), &[], &mut HashSet::new(), &mut Default::default(), &hold, &|_| false, &|_| false, &|_, _| {}).await;
        let current = e.accepted.current(&*e.builds).await.unwrap();
        let plan = rafka_node_admin_core::accepted::plan(&current.topology, &t, None).operations;
        (opened, build_id, leaving, leaving_id, plan, trace_id)
    });
    let spans = cap.spans();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_leave_reopen_unheard");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    assert_eq!(opened, Some((build_id, 2)), "the unheard removed mesh with no MeshLeft opens the next attempt of the same Build");
    assert_eq!(plan, vec![BuildOperation::ShutdownMesh { mesh: leaving.into(), mesh_id: leaving_id }], "the opened attempt plans {leaving}'s shutdown again");
    let drift: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-proven-drift" && s["trace_id"] == trace_id.as_str()).collect();
    assert!(drift.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["action"] == "none" && s["attributes"]["scope"].as_str().is_some_and(|x| x.contains("shutdown-mesh:"))), "the opened attempt names the shutdown: {drift:?}");
}

/// CONTRACT (mesh-leave.md, "Resume after the primary's loss"; node-status-change.md: "a failed
/// Build ... gets its next attempt"; R-S2: event-driven, no timer): the resume of an incomplete leave
/// is opened once per change of what it decides on, never once per pass. A resumed attempt that
/// fails with nothing changed (the leaving mesh's seat, its live node-admins and the receipts under
/// `shutdown-mesh:<mesh id>` all as they were when it opened) gets no next attempt: the drift pass
/// names the unchanged inputs and opens nothing. When one input moves (here: a node-admin of the
/// leaving mesh is live and seated) the next pass opens exactly one attempt. Live form (b) before
/// this: each attempt failed fast "live, holds no seat yet" and the drift opened attempt 803 in ~5 min.
#[test]
fn a_resumed_leave_that_fails_with_nothing_changed_is_not_opened_again() {
    use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter};
    let cap = crate::mesh_leave_resume::capture();
    let (passes, build_id, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.mesh-leave-reopen-once-per-change");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        let _in_root = root.enter();
        let e = estate().await;
        let fp = e.fabric_primary().await;
        let leaving = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
        let mut topology = e.accepted.current(&*e.builds).await.unwrap().topology;
        topology.meshes.remove(leaving);
        let build_id = BuildId::mint();
        e.builds.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology, submitted_change: None, submitted_at_ms: 1 }).await.unwrap();
        e.builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: fp.clone() }).await.unwrap();
        let leaving_id = e.view.read().await.meshes.iter().find(|m| m.name == leaving).and_then(|m| m.id.clone()).unwrap();
        let outer = rafka_node_admin_core::mesh_leave::operation(&leaving_id);
        let members: Vec<Node> = e.view.read().await.nodes.iter().filter(|n| n.mesh == leaving).cloned().collect();
        let roster: Vec<rafka_node_admin_core::mesh_leave::RosterBirth> = members.iter().map(|n| rafka_node_admin_core::mesh_leave::RosterBirth::of(n).unwrap()).collect();
        let last = roster.iter().find(|b| b.node.contains(".admin.")).cloned().unwrap();
        let proof = rafka_node_admin_core::deployment::pipeline::RuntimeProof { deployment_id: format!("dep-{}", last.node), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: last.incarnation_id.clone() };
        let accepted_roster = rafka_node_admin_core::mesh_leave::ExitManifest { mesh_id: leaving_id.to_string(), roster, final_primary: last, final_runtime: Some(proof), receipts: vec![] };
        e.builds
            .append_step_receipt(&rafka_node_admin_core::build_state::BuildStepReceipt { build_id: build_id.clone(), attempt: 1, operation: outer.clone(), step: rafka_node_admin_core::mesh_leave::STEP_ACCEPTED_ROSTER.into(), outcome: rafka_node_admin_core::build_state::StepOutcome::Complete, output: Some(serde_json::to_value(&accepted_roster).unwrap()), executor: Some(fp.clone()) })
            .await
            .unwrap();
        {
            let mut v = e.view.write().await;
            for n in v.nodes.iter_mut().filter(|n| n.mesh == leaving) {
                n.status = NodeStatus::PendingReconnect;
                n.is_primary = false;
            }
        }
        let fail = |attempt: u32| BuildAttemptReceipt { build_id: build_id.clone(), attempt, outcome: AttemptOutcome::Failed { reason: format!("{outer}: {leaving} holds no node-admin primary to hand the workflow to") } };
        e.builds.append_attempt_receipt(&fail(1)).await.unwrap();
        e.accepted.point(&build_id, 1, "test").await.unwrap();
        let me: PathName = fp.parse().unwrap();
        let hold = |m: &str| (m == leaving).then_some(120_000u64);
        let mut started = HashSet::new();
        let mut held = Default::default();
        let mut passes = Vec::new();
        for pass in 0..3 {
            if pass == 2 {
                // One input moves: a node-admin of the leaving mesh is live and seated.
                let mut v = e.view.write().await;
                let admin = v.nodes.iter_mut().find(|n| n.mesh == leaving && n.kind == NodeKind::NodeAdmin).unwrap();
                admin.status = NodeStatus::ReadyForTraffic;
                admin.is_primary = true;
            }
            let t = e.view.read().await.clone();
            let opened = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.accepted, &e.book, &e.provider, &*e.builds, &rafka_node_admin_core::build_claim::AttemptContexts::in_memory(), &tokio::sync::Mutex::new(()), &[], &mut started, &mut held, &hold, &|_| false, &|_| false, &|_, _| {}).await;
            passes.push(opened.clone());
            // The opened attempt is claimed and fails fast, writing nothing (the live form's "holds no seat yet").
            if let Some((_, attempt)) = opened {
                e.builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt, executor: fp.clone() }).await.unwrap();
                e.builds.append_attempt_receipt(&fail(attempt)).await.unwrap();
            }
        }
        (passes, build_id, trace_id)
    });
    let spans = cap.spans();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_leave_reopen_once_per_change");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    assert_eq!(passes, vec![Some((build_id.clone(), 2)), None, Some((build_id, 3))], "one opening for the failure, none while nothing changed, one for the change");
    let named: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.reject.via-drift-unchanged" && s["trace_id"] == trace_id.as_str()).collect();
    assert!(named.iter().any(|s| s["attributes"]["failed_attempt"] == "2" && s["attributes"]["scope"].as_str().is_some_and(|x| x.contains("shutdown-mesh:"))), "the unchanged pass names the failed attempt and the inputs it decided on: {named:?}");
}

/// CONTRACT: a removed mesh whose final primary's exact exit is recorded, but whose `MeshLeft` is
/// not (the attempt failed after the owner's stop of the final primary), is resumed from its
/// receipts although the view holds no birth of it live: the fabric-primary opens the next attempt
/// of the same Build with no action, and that attempt's plan is the mesh's shutdown, which records
/// `MeshLeft` from the receipts (`mesh_leave::owner_resume`).
#[test]
fn a_removed_mesh_whose_final_primary_exited_without_mesh_left_opens_its_shutdown_again() {
    use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, BuildStepReceipt, StepOutcome};
    use rafka_node_admin_core::deployment::pipeline::{RetireStep, RuntimeProof, TerminalReceipt};
    use rafka_node_admin_core::mesh_leave::{member_operation, operation, ExitManifest, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_OTHER_MEMBERS_EXITED};
    let cap = crate::mesh_leave_resume::capture();
    let (opened, build_id, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.mesh-leave-reopen-final-exited");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        let _in_root = root.enter();
        let e = estate().await;
        let fp = e.fabric_primary().await;
        let leaving = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
        let mut topology = e.accepted.current(&*e.builds).await.unwrap().topology;
        topology.meshes.remove(leaving);
        let build_id = BuildId::mint();
        e.builds.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology, submitted_change: None, submitted_at_ms: 1 }).await.unwrap();
        e.builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: fp.clone() }).await.unwrap();
        let mesh_id = e.view.read().await.meshes.iter().find(|m| m.name == leaving).and_then(|m| m.id.clone()).unwrap();
        let outer = operation(&mesh_id);
        // Every birth of the leaving mesh exited: none is live in the view.
        let members: Vec<Node> = {
            let mut v = e.view.write().await;
            for n in v.nodes.iter_mut().filter(|n| n.mesh == leaving) {
                n.status = NodeStatus::Dead;
            }
            v.nodes.iter().filter(|n| n.mesh == leaving).cloned().collect()
        };
        let roster: Vec<RosterBirth> = members.iter().map(|n| RosterBirth::of(n).unwrap()).collect();
        let last = roster.iter().find(|b| b.node.contains(".admin.")).cloned().unwrap();
        let proof = |b: &RosterBirth| RuntimeProof { deployment_id: format!("dep-{}", b.node), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: b.incarnation_id.clone() };
        let terminal = |b: &RosterBirth| TerminalReceipt { node: b.node.clone(), node_id: b.node_id.clone(), incarnation_id: b.incarnation_id.clone(), runtime: Some(proof(b)), exit_code: Some(0) };
        let step = |operation: String, step: &str, output: serde_json::Value| BuildStepReceipt { build_id: build_id.clone(), attempt: 1, operation, step: step.into(), outcome: StepOutcome::Complete, output: Some(output), executor: Some(fp.clone()) };
        let accepted = ExitManifest { mesh_id: mesh_id.to_string(), roster: roster.clone(), final_primary: last.clone(), final_runtime: Some(proof(&last)), receipts: vec![] };
        let others = ExitManifest { receipts: roster.iter().filter(|b| **b != last).map(terminal).collect(), ..accepted.clone() };
        for r in [
            step(outer.clone(), STEP_ACCEPTED_ROSTER, serde_json::to_value(&accepted).unwrap()),
            step(outer.clone(), STEP_OTHER_MEMBERS_EXITED, serde_json::to_value(&others).unwrap()),
            step(member_operation(&outer, &last.node), RetireStep::TerminateRuntime.name(), serde_json::to_value(terminal(&last)).unwrap()),
        ] {
            e.builds.append_step_receipt(&r).await.unwrap();
        }
        e.builds
            .append_attempt_receipt(&BuildAttemptReceipt { build_id: build_id.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: format!("{outer}: the MeshLeft receipt could not be recorded") } })
            .await
            .unwrap();
        e.accepted.point(&build_id, 1, "test").await.unwrap();
        (e.drift().await, build_id, trace_id)
    });
    let spans = cap.spans();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_leave_reopen_final_exited");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    assert_eq!(opened, Some((build_id, 2)), "the recorded final exit with no MeshLeft opens the next attempt of the same Build");
    let drift: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-proven-drift" && s["trace_id"] == trace_id.as_str()).collect();
    assert!(drift.iter().any(|s| s["attributes"]["action"] == "none" && s["attributes"]["scope"].as_str().is_some_and(|x| x.contains("shutdown-mesh:"))), "{drift:?}");
}
