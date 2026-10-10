//! i143.e1.s5 functional: Build identity is fabric control state.
//!
//! Two node-admins of one fabric hold the fabric's Build facts over the Build
//! topic (iroh-gossip). Admin A is the fabric-primary: it accepts a Build, claims attempt 1 on its
//! own log, runs it, completes one operation and dies in the middle of the next. Admin B takes the
//! seat and drives the SAME build id from its own projection (A's state is gone with it): the
//! attempt A held is dispatched to A, which answers nothing, and only after B holds proof of A's
//! departure does it claim attempt 2 on its own log. B re-plans against the observed topology,
//! runs only what is left and completes the Build, under
//! `rdm.node_admin.build.update.via-reconcile`.

use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, SecretKey};
use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build::{BuildId, BuildOperation, MeshDesired};
use rafka_node_admin_core::build_state::{BuildState, BuildStateAdapter};
use rafka_node_admin_core::build_drive::{Drive, DriveEnv, DriveEnd, OpenGate};
use rafka_node_admin_core::executor::OperationRunner;
use rafka_node_admin_core::fabric_builds::FabricBuildStateAdapter;
use rafka_node_admin_core::http::ControlPlane;
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
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
    n.status = NodeStatus::ReadyForTraffic;
    n.is_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// The observed fabric: mesh1 with two admins and one rpc node.
fn observed() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![
            Node { is_fabric_primary: true, ..node("mesh1.admin.1", true) },
            node("mesh1.admin.2", false),
            node("mesh1.rpc.1", true),
        ],
    }
}

/// The view once A is lost: its admin is dead and B holds mesh1 and the fabric.
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
    async fn run(&self, _: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        self.ran.lock().unwrap().push((attempt, op.clone()));
        let BuildOperation::CreateNode { node: name, .. } = op else { return Err(format!("unexpected {op:?}")) };
        if self.die_at.as_ref() == Some(name) {
            self.died.notify_one();
            std::future::pending::<()>().await;
        }
        self.topology.write().await.nodes.push(node(&name.to_string(), false));
        Ok(())
    }
}

/// A fabric-primary's drive of the Build over `builds`: its claim door is its own log, and
/// `dispatch` reaches the executors that are still alive.
fn drive_env(me: &str, a: &Admin, topology: &Arc<RwLock<Topology>>, dispatch: Arc<crate::loopback::Loopback>, proofs: Arc<crate::loopback::Proofs>) -> Arc<DriveEnv> {
    Arc::new(DriveEnv {
        me: me.parse().unwrap(),
        topology: topology.clone(),
        accepted: a.accepted.clone(),
        builds: a.builds.clone(),
        door: Arc::new(rafka_node_admin_core::build_claim::ClaimDoor { me: me.parse().unwrap(), topology: topology.clone(), builds: a.builds.clone(), contexts: Arc::new(rafka_node_admin_core::build_claim::AttemptContexts::in_memory()) }),
        dispatcher: dispatch,
        gate: Arc::new(OpenGate),
        departure: proofs,
        verdicts: Arc::new(crate::loopback::NoVerdicts),
    })
}

struct Admin {
    endpoint: Endpoint,
    router: Router,
    builds: Arc<FabricBuildStateAdapter>,
    /// The admin's own Build log, which the Build topic feeds and the intent a call carries is absorbed into.
    local: Arc<rafka_node_admin_core::build_state::MemoryBuildStateAdapter>,
    /// `Fabric.build_id` as the admin holds it: what its Build topic feeds.
    accepted: Arc<AcceptedStore>,
}

async fn admin(peers: Vec<iroh::EndpointAddr>) -> Admin {
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .alpns(vec![iroh_gossip::ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    // As a started admin: its fabric.storage holds the Fabric record (no Build named yet).
    let storage = Arc::new(rafka_node_admin_core::fabric_storage::MemoryFabricStorage::new());
    rafka_node_admin_core::fabric_storage::FabricStorage::put_identity(&*storage, &rafka_node_admin_core::fabric_storage::FabricIdentity { fabric_id: fabric1(), name: "fabric1".into() }).await.unwrap();
    let accepted = Arc::new(AcceptedStore::new(storage, "test-admin"));
    let local = Arc::new(rafka_node_admin_core::build_state::MemoryBuildStateAdapter::new());
    let builds = Arc::new(FabricBuildStateAdapter::join(&gossip, &endpoint, &fabric1(), peers, local.clone(), accepted.clone(), rafka_node_admin_core::shutdown::ShutdownControl::memory("test-admin").await, "test-admin".into(), None).await.unwrap());
    Admin { endpoint, router, builds, local, accepted }
}

fn addr(a: &Admin) -> iroh::EndpointAddr {
    let ip = a.endpoint.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap();
    iroh::EndpointAddr::new(a.endpoint.id()).with_ip_addr(ip)
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
    crate::enable_callsites();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let topology = Arc::new(RwLock::new(observed()));

    let a = admin(vec![]).await;
    let b = admin(vec![addr(&a)]).await;
    // B returns from joining once connected to A on the fabric Build topic.

    // A accepts the Build through its control plane: grow mesh1 to 3 rpc nodes.
    // A holds a settled accepted Build of what is observed: the one the change compiles against.
    // The settled Build A's pointer names is seeded into A's own fabric.storage, the store its
    // Build topic adapter serves to a neighbour and its control plane moves.
    let seed = rafka_node_admin_core::build::BuildId::mint();
    a.builds
        .publish_accepted(&rafka_node_admin_core::build_state::BuildAccepted { build_id: seed.clone(), topology: FabricTopology::of_observed(&observed()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    a.builds.claim_attempt(&rafka_node_admin_core::build_state::BuildAttemptClaim { build_id: seed.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    a.builds
        .append_attempt_receipt(&rafka_node_admin_core::build_state::BuildAttemptReceipt { build_id: seed.clone(), attempt: 1, outcome: rafka_node_admin_core::build_state::AttemptOutcome::Converged })
        .await
        .unwrap();
    a.accepted.point(&seed, 0, "seeded").await.unwrap();
    let mut cp = ControlPlane::new(a.builds.clone(), a.accepted.clone(), "mesh1.admin.1".parse().unwrap(), observed(), crate::adopted_time());
    cp.topology = topology.clone();
    let build_id = cp
        .submit("POST /api/build", TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 3)]) })
        .await
        .unwrap()
        .build_id;

    // A drives: mesh1.rpc.2 completes, A dies in mesh1.rpc.3.
    let died = Arc::new(Notify::new());
    let a_runner = Arc::new(Runner { topology: topology.clone(), ran: Mutex::new(vec![]), die_at: Some("mesh1.rpc.3".parse().unwrap()), died: died.clone() });
    let a_dispatch = crate::loopback::Loopback::new(a.builds.clone(), a.local.clone(), topology.clone(), a_runner.clone());
    let a_env = drive_env("mesh1.admin.1", &a, &topology, a_dispatch.clone(), Arc::default());
    let a_drive = Drive::detached(build_id.clone());
    let mut a_driving = Box::pin({
        let (env, drive) = (a_env.clone(), a_drive.clone());
        async move { env.run(&drive).await }
    });
    tokio::select! {
        r = &mut a_driving => panic!("A was to stop inside mesh1.rpc.3, but its drive ended: {r:?}"),
        _ = died.notified() => {}
    }
    // While A is stuck in mesh1.rpc.3, the fabric projection carries the
    // Build to B: the intent and A's claim of attempt 1.
    eventually("B's projection shows A's attempt", || {
        let (b, id) = (b.builds.clone(), build_id.clone());
        async move { b.read_build(&id).await.is_ok_and(|v| v.attempt == 1 && v.executor.as_deref() == Some("mesh1.admin.1")) }
    })
    .await;
    // A dies: its execution, endpoint, gossip and Build state go with it.
    drop(a_driving);
    let Admin { router, builds: a_builds, .. } = a;
    drop(a_env);
    drop(a_dispatch);
    drop(a_builds);
    router.shutdown().await.unwrap();
    lose_a(&topology).await;

    // B drives the same Build: the same build id, the next attempt, only what is left.
    let b_runner = Arc::new(Runner { topology: topology.clone(), ran: Mutex::new(vec![]), die_at: None, died: Arc::new(Notify::new()) });
    // B's pointer names the Build through its Build topic, never set by hand: the successor may
    // drive only what Fabric.build_id names.
    eventually("B's Fabric.build_id names the Build", || {
        let (accepted, id) = (b.accepted.clone(), build_id.clone());
        async move { accepted.build_id().await == Some(id) }
    })
    .await;
    let b_dispatch = crate::loopback::Loopback::new(b.builds.clone(), b.local.clone(), topology.clone(), b_runner.clone());
    // A is gone: nothing answers at its address.
    b_dispatch.lose("mesh1.admin.1");
    let proofs = Arc::new(crate::loopback::Proofs::default());
    let b_env = drive_env("mesh1.admin.2", &b, &topology, b_dispatch.clone(), proofs.clone());
    let b_drive = Drive::detached(build_id.clone());
    // Silence is not proof: A is unreachable, its departure is not proven, and the attempt it holds
    // is not claimed for another admin.
    match b_env.run(&b_drive).await {
        DriveEnd::Stalled(why) => assert!(why.contains("departure is not proven"), "{why}"),
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
            (1, BuildOperation::CreateNode { node: "mesh1.rpc.2".parse().unwrap() }),
            (1, BuildOperation::CreateNode { node: "mesh1.rpc.3".parse().unwrap() }),
        ]
    );
    assert_eq!(
        *b_runner.ran.lock().unwrap(),
        vec![
            // A's admin is lost too: its path is part of what is left.
            (2, BuildOperation::CreateNode { node: "mesh1.admin.1".parse().unwrap() }),
            (2, BuildOperation::CreateNode { node: "mesh1.rpc.3".parse().unwrap() }),
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
    let reconciles: Vec<&BTreeMap<String, String>> = all
        .values()
        .filter(|(n, f)| n == "rdm.node_admin.build.update.via-reconcile" && f.get("build_id") == Some(&build_id.0))
        .map(|(_, f)| f)
        .collect();
    let takeover = reconciles.iter().find(|f| f.get("executor").map(String::as_str) == Some("mesh1.admin.2")).unwrap_or_else(|| panic!("B's reconcile span among {reconciles:?}"));
    assert_eq!(takeover.get("attempt").map(String::as_str), Some("2"));
    assert_eq!(takeover.get("operations").map(String::as_str), Some("create-node:mesh1.admin.1,create-node:mesh1.rpc.3"));
    assert_eq!(takeover.get("outcome").map(String::as_str), Some("converged"));
    let proven = all
        .values()
        .filter(|(n, f)| n == "rdm.node_admin.build.update.via-departure-proven" && f.get("build_id") == Some(&build_id.0))
        .map(|(_, f)| f)
        .collect::<Vec<_>>();
    assert_eq!(proven.len(), 1, "the departure was proven once: {proven:?}");
    assert_eq!(proven[0].get("executor").map(String::as_str), Some("mesh1.admin.1"));
    b.router.shutdown().await.unwrap();
}

/// The test fabric's canonical id.
fn fabric1() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}
