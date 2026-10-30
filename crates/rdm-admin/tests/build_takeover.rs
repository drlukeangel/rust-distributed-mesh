//! i143.e1.s5 functional: Build identity is fabric control state.
//!
//! Two node-admins of one fabric hold the fabric's Build records on its shared store root (build.md
//! §5.3; Q185: no Build channel). Admin A is the fabric-primary: it accepts a Build, claims attempt 1,
//! runs it, completes one operation and dies in the middle of the next. Admin B, never connected to
//! A, reads the Build from the store root, takes the role and drives the SAME build id (A's process
//! is gone with it): the attempt A held is dispatched to A, which answers nothing, and only after B
//! holds proof of A's departure does it claim attempt 2. B re-plans against the observed topology,
//! runs only what is left and completes the Build, under `rdm.node_admin.build.update.via-reconcile`.

use iroh::endpoint::presets;
use iroh::{Endpoint, SecretKey};
use rdm_admin::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rdm_admin::build::{BuildId, BuildOperation, MeshDesired};
use rdm_admin::build_state::{BuildState, BuildStateAdapter};
use rdm_admin::build_drive::{Drive, DriveEnv, DriveEnd};
use rdm_admin::executor::OperationRunner;
use rdm_admin::fabric_builds::FabricBuildStateAdapter;
use rdm_admin::control_plane::ControlPlane;
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tracing_subscriber::layer::SubscriberExt;

/// Every span by id: name and recorded fields.
#[derive(Clone, Default)]
struct Spans(Arc<Mutex<HashMap<u64, (String, BTreeMap<String, String>)>>>);

struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl tracing::field::Visit for Fields<'_> {
    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
        self.0.insert(f.name().into(), v.into());
    }
    fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
        self.0.insert(f.name().into(), v.to_string());
    }
    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        self.0.insert(f.name().into(), format!("{v:?}"));
    }
}

impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> tracing_subscriber::Layer<S> for Spans {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().insert(id.into_u64(), (attrs.metadata().name().into(), fields));
    }
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if let Some((_, fields)) = self.0.lock().unwrap().get_mut(&id.into_u64()) {
            values.record(&mut Fields(fields));
        }
    }
}

fn node(name: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// The observed fabric: mesh1 with two admins and one rpc node.
fn observed() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![
            Node { is_fabric_primary: true, ..node("mesh1.admin.1", true) },
            node("mesh1.admin.2", false),
            node("mesh1.rpc.1", true),
        ],
    }
}

/// The view once A hands off its roles (`node.primary.update`) to B and exits: B holds mesh1 and
/// the fabric while A's runtime is unreachable until its exited verdict is read.
async fn lose_a(topology: &RwLock<Topology>) {
    let mut t = topology.write().await;
    for n in t.nodes.iter_mut() {
        match n.name.to_string().as_str() {
            "mesh1.admin.1" => {
                n.status = NodeStatus::PendingReconnect;
                n.is_primary = false;
                n.is_fabric_primary = false;
            }
            "mesh1.admin.2" => {
                n.is_primary = true;
                n.is_fabric_primary = true;
            }
            _ => {}
        }
    }
}

/// Realises `CreateNode` by adding the node to the observed topology. When
/// armed with `die_at`, the admin dies at that operation: it never returns.
struct Runner {
    topology: Arc<RwLock<Topology>>,
    ran: Mutex<Vec<(u32, BuildOperation)>>,
    die_at: Option<PathName>,
    died: Arc<Notify>,
}

#[async_trait::async_trait]
impl OperationRunner for Runner {
    async fn run(&self, _: &BuildId, _lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.lock().unwrap().push((attempt, op.clone()));
        let BuildOperation::CreateNode { node: name, .. } = op else { return Err(format!("unexpected {op:?}").into()) };
        if self.die_at.as_ref() == Some(name) {
            self.died.notify_one();
            std::future::pending::<()>().await;
        }
        self.topology.write().await.nodes.push(node(&name.to_string(), false));
        Ok(())
    }
}

/// The drive's gate over the fixture's view: the node rows record what each live node of the view runs, the executable its entry names (the create's claim records it,
/// build-deployment.md §2.3), as production rows do.
struct RowsGate {
    view: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl rdm_admin::build_drive::Gate for RowsGate {
    async fn recorded_executables(&self, topology: &FabricTopology) -> Result<rdm_admin::accepted::Recorded, String> {
        let v = self.view.read().await;
        Ok(v.nodes
            .iter()
            .filter(|n| n.status.is_live())
            .filter_map(|n| {
                let entry = rdm_admin::accepted::target_executable(topology, &n.name)?;
                Some((n.name.clone(), rdm_admin::accepted::RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: n.node_id.clone(), sha256: Some(entry.sha256.clone()) }))
            })
            .collect())
    }
}

/// A fabric-primary's drive of the Build over `builds`: its claim door is its own log, and
/// `dispatch` reaches the executors that are still alive.
fn drive_env(me: &str, a: &Admin, topology: &Arc<RwLock<Topology>>, dispatch: Arc<crate::loopback::Loopback>, proofs: Arc<crate::loopback::Proofs>, told: &crate::Told) -> Arc<DriveEnv> {
    Arc::new(DriveEnv {
        me: me.parse().unwrap(),
        topology: topology.clone(),
        builds: a.builds.clone(),
        door: Arc::new(rdm_admin::build_claim::ClaimDoor {
            me: me.parse().unwrap(),
            node_id: topology
                .try_read()
                .expect("the view is free")
                .node(&me.parse().unwrap())
                .expect("the fabric-primary is in the view")
                .node_id
                .clone(),
            topology: topology.clone(),
            builds: a.builds.clone(),
            contexts: Arc::new(rdm_admin::build_claim::AttemptContexts::in_memory()),
            claims: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
            claimed: Default::default(),
        }),
        dispatcher: dispatch,
        gate: Arc::new(RowsGate { view: topology.clone() }),
        departure: proofs,
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        notices: told.notices(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    })
}

struct Admin {
    /// The admin's endpoint: what its view names it by.
    endpoint: Endpoint,
    builds: Arc<FabricBuildStateAdapter>,
    /// The admin's Build journal on the fabric's shared store root, which the Build's committed records
    /// it reads are absorbed into (Q292).
    local: Arc<dyn rdm_admin::build_state::LocalBuildLog>,
    /// The Fabric identity as the admin holds it.
    accepted: Arc<AcceptedStore>,
}

/// An admin of the fabric whose store root is `root`, connected to no other admin.
async fn admin(root: &std::path::Path) -> Admin {
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    // As a started admin: its fabric.storage holds the Fabric record, and it holds no Build yet.
    let storage = Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new());
    rdm_admin::fabric_storage::claim_identity(&*storage, &rdm_admin::fabric_storage::FabricIdentity { fabric_id: fabric1(), name: "fabric1".into() }).await.unwrap().unwrap();
    let accepted = Arc::new(AcceptedStore::new(storage, "test-admin"));
    // Its Build journal as its file backend opens it, on the fabric's store root.
    let scope = rdm_admin::storage_backend::StorageScope { data_dir: root.join(format!("data-{}", NodeId::mint())), fabric_id: fabric1(), own_mesh: "mesh1".into(), writer: IncarnationId::mint(), node_id: NodeId::mint() };
    let local = {
        use rdm_admin::storage_backend::StorageBackend as _;
        rdm_admin::storage_backend::FileBackend::at(root).open(&scope).await.unwrap().builds
    };
    let builds = Arc::new(FabricBuildStateAdapter::new(local.clone(), &rdm_transport::topology::LocationView::new(&endpoint, "test-admin").unwrap(), "test-admin".into(), rdm_admin::fabric_builds::PrimarySource::unrecorded()));
    Admin { endpoint, builds, local, accepted }
}

async fn eventually<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..200 {
        if f().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("never observed: {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_admin_completes_the_same_build_after_the_executor_dies_mid_build() {
    let spans = Spans::default();
    rdm_transport::enable_callsites();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let topology = Arc::new(RwLock::new(observed()));

    // One store root, the fabric's; A and B are never connected to each other.
    let root = std::env::temp_dir().join(format!("build-takeover-{}", NodeId::mint()));
    std::fs::create_dir_all(&root).unwrap();
    let a = admin(&root).await;
    let b = admin(&root).await;

    // A accepts the Build through its control plane: grow mesh1 to 3 rpc nodes.
    // A holds a settled accepted Build of what is observed: the one the change compiles against.
    // The settled Build A is seeded into the fabric's builds store, which its control plane moves.
    let seed = rdm_admin::build::BuildId::mint();
    a.builds
        .publish_accepted(&rdm_admin::build_state::BuildAccepted { build_id: seed.clone(), topology: FabricTopology::of_observed(&observed()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    a.builds.claim_attempt(&rdm_admin::build_state::BuildAttemptClaim { claimant: None, mesh: None, build_id: seed.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    a.builds
        .append_attempt_receipt(&rdm_admin::build_state::BuildAttemptReceipt { mesh: None, build_id: seed.clone(), attempt: 1, outcome: rdm_admin::build_state::AttemptOutcome::Converged })
        .await
        .unwrap();
    let mut cp = ControlPlane::new(a.builds.clone(), a.accepted.clone(), "mesh1.admin.1".parse().unwrap(), observed(), crate::adopted_time());
    cp.topology = topology.clone();
    let build_id = cp
        .submit("POST /api/build", crate::named_request(&cp, TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rdm_core::NodeKind::NodeAdmin, 2), (rdm_core::NodeKind::RpcNode, 3)]) }).await)
        .await
        .unwrap()
        .build_id;

    // A drives: mesh1.rpc.2 completes, A dies in mesh1.rpc.3.
    let died = Arc::new(Notify::new());
    let a_runner = Arc::new(Runner { topology: topology.clone(), ran: Mutex::new(vec![]), die_at: Some("mesh1.rpc.3".parse().unwrap()), died: died.clone() });
    let a_dispatch = crate::loopback::Loopback::new(a.builds.clone(), a.local.clone(), topology.clone(), a_runner.clone());
    let a_told = crate::Told::default();
    let a_env = drive_env("mesh1.admin.1", &a, &topology, a_dispatch.clone(), Arc::default(), &a_told);
    let a_drive = Drive::detached(build_id.clone());
    let mut a_driving = Box::pin({
        let (env, drive) = (a_env.clone(), a_drive.clone());
        async move { env.run(&drive).await }
    });
    tokio::select! {
        r = &mut a_driving => panic!("A was to stop inside mesh1.rpc.3, but its drive ended: {r:?}"),
        _ = died.notified() => {}
    }
    // While A is stuck in mesh1.rpc.3, B, not the fabric-primary, reads the Build from the store
    // root: the intent and A's claim of attempt 1.
    eventually("B's projection shows A's attempt", || {
        let (b, id) = (b.builds.clone(), build_id.clone());
        async move { b.read_build(&id).await.is_ok_and(|v| v.attempt == 1 && v.executor.as_deref() == Some("mesh1.admin.1")) }
    })
    .await;
    // A dies: its execution and Build state go with it.
    drop(a_driving);
    let Admin { endpoint: a_endpoint, builds: a_builds, .. } = a;
    drop(a_env);
    drop(a_dispatch);
    drop(a_builds);
    a_endpoint.close().await;
    lose_a(&topology).await;

    // B drives the same Build: the same build id, the next attempt, only what is left.
    let b_runner = Arc::new(Runner { topology: topology.clone(), ran: Mutex::new(vec![]), die_at: None, died: Arc::new(Notify::new()) });
    // B holds the Build from the store root, never set by hand: the successor may drive only
    // its current Build (R-BI3), read from the fabric's builds store.
    eventually("B's current Build is the Build", || {
        let (builds, id) = (b.builds.clone(), build_id.clone());
        async move { builds.current_build().await.ok().and_then(|c| c.into_build_id()) == Some(id) }
    })
    .await;
    let b_dispatch = crate::loopback::Loopback::new(b.builds.clone(), b.local.clone(), topology.clone(), b_runner.clone());
    // A is gone: nothing answers at its address.
    b_dispatch.lose("mesh1.admin.1");
    let proofs = Arc::new(crate::loopback::Proofs::default());
    let b_told = crate::Told::default();
    let b_env = drive_env("mesh1.admin.2", &b, &topology, b_dispatch.clone(), proofs.clone(), &b_told);
    let b_drive = Drive::detached(build_id.clone());
    // Silence is not proof: A is unreachable, its departure is not proven, and the attempt it holds
    // is not claimed for another admin.
    match b_env.run(&b_drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("departure is not proven") && why.contains("mesh1.admin.1 is gone: nothing answers at its address"), "the stall names the unproven departure and why the call to A ended: {why}"),
        other => panic!("an unreachable executor without proof stalls the drive: {other:?}"),
    }
    assert_eq!(b.builds.read_build(&build_id).await.unwrap().attempt, 1, "no attempt was claimed on silence");
    assert!(b_runner.ran.lock().unwrap().is_empty(), "nothing ran on silence");
    // The proof arrives: the exact runtime of A is inspected and exited.
    proofs.exited.lock().unwrap().insert("mesh1.admin.1".into());
    assert_eq!(b_env.run(&b_drive).await, DriveEnd::Terminal);
    assert_eq!(
        *a_runner.ran.lock().unwrap(),
        vec![
            (1, BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap(), over: None, executable: Some(crate::test_executable(rdm_core::NodeKind::RpcNode)), entry: None }),
            (1, BuildOperation::CreateNode { node: "mesh1.rpc.3".parse().unwrap(), over: None, executable: Some(crate::test_executable(rdm_core::NodeKind::RpcNode)), entry: None }),
        ]
    );
    assert_eq!(
        *b_runner.ran.lock().unwrap(),
        vec![
            // A's admin is lost too: its path is part of what is left.
            (2, BuildOperation::CreateNode { node: "mesh1.admin.1".parse().unwrap(), over: None, executable: None, entry: None }),
            // The successor reads the Build from the store, where each entry keeps its executable (Q186).
            (2, BuildOperation::CreateNode { node: "mesh1.rpc.3".parse().unwrap(), over: None, executable: Some(crate::test_executable(rdm_core::NodeKind::RpcNode)), entry: None }),
        ],
        "mesh1.rpc.2 is never created twice"
    );
    assert_eq!(*b_dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 2)], "attempt 2 was dispatched to B alone");
    let rpcs = topology.read().await.nodes.iter().filter(|n| n.kind == NodeKind::RpcNode).count();
    assert_eq!(rpcs, 3);

    let view = b.builds.read_build(&build_id).await.unwrap();
    assert_eq!((view.state, view.attempt, view.executor.as_deref()), (BuildState::Complete, 2, Some("mesh1.admin.2")));

    // The takeover is one reconcile span on B, and one departure proof span naming A.
    let all = spans.0.lock().unwrap().clone();
    let logged = build_id.log().to_string();
    let reconciles: Vec<&BTreeMap<String, String>> = all
        .values()
        .filter(|(n, f)| n == "rdm.node_admin.build.update.via-reconcile" && f.get("build_id").map(String::as_str) == Some(logged.as_str()))
        .map(|(_, f)| f)
        .collect();
    let takeover = reconciles.iter().find(|f| f.get("executor").map(String::as_str) == Some("mesh1.admin.2")).unwrap_or_else(|| panic!("B's reconcile span among {reconciles:?}"));
    assert_eq!(takeover.get("attempt").map(String::as_str), Some("2"));
    // The span names the executor the takeover follows: the admin that held attempt 1, not the
    // claim of attempt 2 the executor absorbed before it runs.
    assert_eq!(takeover.get("previous_executor").map(String::as_str), Some("mesh1.admin.1"), "the takeover names the executor it follows: {takeover:?}");
    assert_eq!(takeover.get("operations").map(String::as_str), Some("node.create:mesh1.admin.1,node.create:mesh1.rpc.3"));
    assert_eq!(takeover.get("outcome").map(String::as_str), Some("converged"));
    let proven = all
        .values()
        .filter(|(n, f)| n == "rdm.node_admin.build.update.via-departure-proven" && f.get("build_id").map(String::as_str) == Some(logged.as_str()))
        .map(|(_, f)| f)
        .collect::<Vec<_>>();
    assert_eq!(proven.len(), 1, "the departure was proven once: {proven:?}");
    assert_eq!(proven[0].get("executor").map(String::as_str), Some("mesh1.admin.1"));
    // The application's Build notices (fabric-node-hooks.md, Q256): A told it the Build started at the
    // claim of attempt 1; B's takeover (attempt 2) continues the Build in progress and sends no
    // on_build_started, and B, which recorded its Complete, sends the one on_build_ended.
    let id = build_id.to_string();
    let a_keys: Vec<(String, u32, &str)> = a_told.settled(1).await.iter().map(crate::Notice::key).collect();
    assert_eq!(a_keys, vec![(id.clone(), 1, "started")], "A told the opening at attempt 1's claim and no ending");
    let b_keys: Vec<(String, u32, &str)> = b_told.settled(1).await.iter().map(crate::Notice::key).collect();
    assert_eq!(b_keys, vec![(id.clone(), 2, "complete")], "the takeover sends no on_build_started; B sends the ending");
    b.endpoint.close().await;
    let _ = std::fs::remove_dir_all(&root);
}

/// The test fabric's canonical id.
fn fabric1() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}
