//! The rpc node runtime: bind the assigned endpoint, serve Node RPC, join
//! fabric topology, publish the digest.

use rdm_core::launch::Launch;
use anyhow::{anyhow, Context, Result};
use iroh::protocol::Router;
use iroh::{EndpointAddr, SecretKey};
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId};
use rdm_transport::topology::TopologyView;
use rdm_transport::{NodeRpcServer, ServerBuilder};
use rdm_core::catalog::OpOwner;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The node's transport key, kept in its data dir: a restart keeps it, a
/// replacement (fresh data dir) gets a new one.
pub fn load_or_mint_key(data_dir: &Path) -> Result<SecretKey> {
    std::fs::create_dir_all(data_dir).with_context(|| format!("data dir {}", data_dir.display()))?;
    let path = data_dir.join("node-key");
    if let Ok(hexkey) = std::fs::read_to_string(&path) {
        let bytes: [u8; 32] = hex::decode(hexkey.trim())
            .map_err(|e| anyhow!("{}: {e}", path.display()))?
            .try_into()
            .map_err(|_| anyhow!("{}: not 32 bytes", path.display()))?;
        return Ok(SecretKey::from_bytes(&bytes));
    }
    let key = SecretKey::generate();
    std::fs::write(&path, hex::encode(key.to_bytes()))?;
    Ok(key)
}

/// The core families (Ping, Forward) every node serves, over the process's one `client`.
pub fn core_protocols(b: ServerBuilder, client: Arc<rdm_transport::NodeRpcClient>, edges: Option<Arc<dyn rdm_transport::CarrierEdges>>) -> ServerBuilder {
    b.serve_core(client, edges)
}

/// A node running on the imported substrate: its endpoint routers, the topology view, Node RPC server and
/// tasks.
pub struct RunningNode {
    /// Rafka-time: the app's way to read it. The one reader every gossip stamp this node publishes
    /// reads, already adopted when the node has started. Cloneable; every clone reads the same
    /// instance. `now_ms()` is `max(reference + monotonic elapsed, last returned)`; an app that
    /// stamps its records with rafka-time (the OrderKey's `writer_timestamp_ms`) reads it here and
    /// never from the OS clock.
    pub rafka_time: rdm_transport::clock::RafkaTime,
    /// The member cert the accepting authority's signer issued for exactly this birth, as the
    /// opaque bytes the join answer carried (`fabric-certs.md`). Empty when the authority is
    /// configured with no certs. RDM never parses it.
    pub member_cert: Vec<u8>,
    /// The endpoint's protocol routers.
    pub routers: Vec<Router>,
    /// The node's topology view.
    pub topology_view: TopologyView,
    /// The node's Node RPC server.
    pub server: NodeRpcServer,
    /// The node's own status, as it publishes it.
    pub status: Arc<Mutex<MemberStatus>>,
    /// The node's current digest.
    pub digest: MeshDigest,
    /// This process's one Node RPC client and live resolver: every Node RPC
    /// caller in the process takes it by clone.
    pub node_rpc: crate::node_rpc::ProcessNodeRpc,
    /// The state this birth owes the holder of its mesh's primary seat: one owed call per
    /// declaration (i146 Q52), retargeted when the seat moves (`declare_loop`).
    pub owed_state: Arc<Mutex<OwedDeclaration>>,
    /// Cancelled when this birth is retired (stop-node, its process stopping): the
    /// `pre_node_ready` hook and any pull it has in flight end.
    pub cancel: rdm_transport::Retirement,
    /// The surface that serves this birth's lifecycle and state commands
    /// (`crate::node_self`).
    pub own: Arc<crate::node_self::NodeSelf>,
    pre_node_ready: Arc<Mutex<Option<crate::node_hooks::PreNodeReadyHandle>>>,
    pre_node_ready_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    publisher: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    node_rpc_feed: tokio::task::JoinHandle<()>,
    /// This node's connections projection: the owed declaration's carriers ([`declaration_route`]).
    held: Arc<Mutex<rdm_core::connections::ConnectionsHeld>>,
    declare_loop: tokio::task::JoinHandle<()>,
    data_connections: rdm_transport::connections::ConnectionsRegistry,
    /// The listeners this birth bound from its launch (`RDM_LISTENERS`, Q151), held open for its life.
    listeners: Arc<Vec<(String, std::net::TcpListener)>>,
}

/// Bind each listener `assigned` names at its assigned address (port 0: the operating system selects
/// the port), non-blocking, in order. Nothing is probed and released: each bind is the one the
/// birth keeps. A bind refused is named with its listener and address.
fn bind_listeners(assigned: &[(String, std::net::SocketAddr)]) -> Result<Vec<(String, std::net::TcpListener)>> {
    assigned
        .iter()
        .map(|(name, addr)| {
            let l = std::net::TcpListener::bind(addr).with_context(|| format!("the node cannot bind its listener {name} at {addr}"))?;
            l.set_nonblocking(true).with_context(|| format!("the listener {name} bound at {addr} cannot be made non-blocking"))?;
            Ok((name.clone(), l))
        })
        .collect()
}

/// Alias for [`RunningNode`] as named in the `i148` surface (`Node::connections`).
pub type Node = RunningNode;

impl RunningNode {
    /// The listeners this birth bound from its launch (`RDM_LISTENERS`), by name, held open for its
    /// life: the application serves on them (`TcpListener::try_clone`).
    pub fn listeners(&self) -> &[(String, std::net::TcpListener)] {
        &self.listeners
    }

    /// Take the one [`rdm_transport::connections::Connections`] handle for `alpn` (`i148`).
    ///
    /// Returns [`rdm_transport::connections::ConnectError::UnregisteredAlpn`] when `alpn` was not
    /// registered at bind, and [`rdm_transport::connections::ConnectError::AlreadyTaken`] on any
    /// second call for the same `alpn`.
    pub fn connections(&self, alpn: &[u8]) -> std::result::Result<rdm_transport::connections::Connections, rdm_transport::connections::ConnectError> {
        self.data_connections.connections(alpn)
    }

    /// The node's data-plane connections registry (testkit fault seam only; absent from production builds).
    #[cfg(feature = "testkit-gossip")]
    pub fn data_connections_for_testkit(&self) -> &rdm_transport::connections::ConnectionsRegistry {
        &self.data_connections
    }

    /// Where this birth's `pre_node_ready` stands; `None` when no hook was registered.
    pub fn pre_node_ready(&self) -> Option<crate::node_hooks::PreNodeReadyHandle> {
        self.pre_node_ready.lock().unwrap().clone()
    }

    /// Resolves with the reason once this node's hook failed after the node came up Pending; never
    /// resolves otherwise. A process selects on it beside its stop signal and ends by name.
    pub async fn pre_node_ready_failed(&self) -> String {
        let handle = self.pre_node_ready.lock().unwrap().clone();
        match handle {
            Some(h) => h.failed().await,
            None => std::future::pending().await,
        }
    }

    /// End a node that never became ready: its tasks stop and its endpoint closes.
    async fn close(self) {
        self.cancel.cancel();
        self.abort_publisher();
        self.node_rpc_feed.abort();
        self.declare_loop.abort();
        for r in self.routers {
            let _ = r.shutdown().await;
        }
    }

    fn abort_publisher(&self) {
        if let Some(p) = self.publisher.lock().unwrap().take() {
            p.abort();
        }
    }

    /// The shutdown of a stopped process (stopped by `stop-node`, not started again): its mesh
    /// connections were cut and `Leaving` declared when it stopped, so nothing is announced. The
    /// tasks end and the endpoints close once every reply it owes is settled.
    pub async fn stop_commanded(self) {
        crate::taskdump::watched("stop_commanded", self.stop_commanded_inner()).await
    }

    async fn stop_commanded_inner(self) {
        self.server.settled().await;
        self.cancel.cancel();
        self.abort_publisher();
        if let Some(h) = self.pre_node_ready_task.lock().unwrap().take() {
            h.abort();
        }
        self.node_rpc_feed.abort();
        self.declare_loop.abort();
        for r in self.routers {
            let _ = r.shutdown().await;
        }
    }

    /// The stop this process's own signal runs. A birth whose `stop-node` was admitted (under way,
    /// done, or followed by a start not yet ended) runs its stop at the command and leaves stopped only
    /// through a start or a delete (node-stop.md step 6): its signal runs no second stop, and its
    /// process ends as [`Self::stop_commanded`] ends it, once every reply it owes is settled. A live birth runs node-stop.md steps 2-4 as its own subject (i146 Q55,
    /// [`crate::node_self::NodeSelf::stop_on_signal`]): the whole `node.connections.delete { All }`
    /// under the birth's bound, its connections-deleting owed to its mesh-primary; the leave step
    /// (its gossip neighbours left, its Node RPC pool cut, nothing published); its stopped state owed
    /// to its mesh-primary. Nothing waits on an answer. Then each router shuts down:
    /// `Router::shutdown` shuts every protocol it serves down, gossip included, once, and then closes
    /// the endpoint; the owed declarations end with the process (node-rpc.md §34).
    pub async fn stop(self) {
        if self.own.stop_admitted() {
            return self.stop_commanded().await;
        }
        crate::taskdump::watched("stop", self.stop_inner()).await
    }

    async fn stop_inner(self) {
        self.cancel.cancel();
        let stopped = self.own.stop_on_signal().await;
        self.abort_publisher();
        self.node_rpc_feed.abort();
        self.declare_loop.abort();
        for r in self.routers {
            let _ = r.shutdown().await;
        }
        for owed in stopped.declarations {
            owed.abort();
        }
    }
}

/// Install the member cert a join answered (node-mesh-join.md steps 5 and 7, Q87): every join is issued a
/// fresh one, a start after a stop and a commanded join as much as a first start, and every connection
/// this birth opens from now presents it.
pub fn install_join_cert(registry: &rdm_transport::connections::ConnectionsRegistry, cert: &[u8], node: &str, node_id: &NodeId, incarnation: &IncarnationId) {
    registry.set_member_cert(cert.to_vec());
    let expires_at = rdm_core::certs::cert_from_bytes(cert).map(|s| s.cert.expiry_ms).unwrap_or(0);
    tracing::info_span!("rdm.node.cert.update.via-join", node = %node, node_id = %node_id, incarnation_id = %incarnation.as_str(), expires_at, cert_len = cert.len() as u64)
        .in_scope(|| tracing::info!("the member cert the join answered is installed: every connection opened from now presents it"));
}

/// Hold the row's name and config a join answer handed (R-JN1, node-start.md step 2): every digest
/// `topology_view` publishes from now on carries them.
fn install_handed(topology_view: &TopologyView, control: &crate::wire::JoinControl) {
    if let Some(name) = &control.name {
        topology_view.set_name(name.clone());
    }
    if let Some(config) = &control.config {
        topology_view.set_config(config.clone());
    }
    tracing::info!(name = ?control.name.as_ref().map(|n| n.to_string()), config_keys = control.config.as_ref().map_or(0, |c| c.len()), "the row's name and config the join handed are held");
}

#[cfg(feature = "file-backend")]
/// [`start_with_seams`] for an app with a hook before Ready: `pre_node_ready` carries its
/// `pre_node_ready` hook (and the gate its own server-side ops sit behind).
pub async fn start_pre_node_ready(launch: &Launch, node_hooks: crate::node_hooks::NodeHooks, register: impl FnOnce(ServerBuilder, NodeSeams) -> ServerBuilder) -> Result<RunningNode> {
    start_with_rafka_time_pre_node_ready_and(launch, rdm_transport::clock::RafkaTime::unadopted(), node_hooks, |s| s, None, register, &[]).await
}

#[cfg(feature = "file-backend")]
/// Bring the node up exactly as `launch` says.
pub async fn start(launch: &Launch, register: impl FnOnce(ServerBuilder, Arc<rdm_transport::LiveNodeResolver>) -> ServerBuilder) -> Result<RunningNode> {
    start_with_client(launch, |b, resolver, _client| register(b, resolver)).await
}

#[cfg(feature = "file-backend")]
/// Bring the node up with the application data-plane `alpns` registered on its one endpoint.
pub async fn start_with_alpns(
    launch: &Launch,
    alpns: &[rdm_transport::connections::Alpn],
    register: impl FnOnce(ServerBuilder, NodeSeams) -> ServerBuilder,
) -> Result<RunningNode> {
    start_with_rafka_time_pre_node_ready_alpns_and(
        launch,
        rdm_transport::clock::RafkaTime::unadopted(),
        crate::node_hooks::NodeHooks::none(),
        |s| s,
        None,
        register,
        &[],
        alpns,
    )
    .await
}

/// The process seams a node's protocol registration callback receives.
#[derive(Clone)]
pub struct NodeSeams {
    /// This process's live resolver, fed as the topology view changes.
    pub resolver: Arc<rdm_transport::LiveNodeResolver>,
    /// The data-plane client, whose dials are recorded as Direct connections.
    pub client: Arc<rdm_transport::NodeRpcClient>,
    /// This node's connections writer.
    pub connections: Arc<crate::connections_writer::ConnectionsWriter>,
    /// The name this birth holds, read per call by every door that names it.
    pub name: crate::held_name::HeldName,
}

/// Optional callback invoked after rafka-time is refreshed during startup.
pub type RafkaTimeKnobFn = Box<dyn FnOnce(&Launch, &rdm_transport::clock::RafkaTime, &tracing::Span) -> Option<Box<dyn FnOnce() + Send>> + Send>;

/// The stop signal of this process (SIGTERM or ctrl-c), latched from the moment [`listen_for_stop`]
/// is first called. A node binary calls it on its first line: the handler then replaces the default
/// disposition (which would end the process before its telemetry drained) for the whole boot, and a
/// stop that arrives before anything awaits it is not lost.
static STOP: std::sync::OnceLock<tokio::sync::watch::Receiver<bool>> = std::sync::OnceLock::new();

/// Start listening for this process's stop signal (idempotent). Call it on the process's runtime.
pub fn listen_for_stop() {
    STOP.get_or_init(|| {
        let (tx, rx) = tokio::sync::watch::channel(false);
        #[cfg(unix)]
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::spawn(async move {
            #[cfg(unix)]
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
            let _ = tx.send(true);
        });
        rx
    });
}

/// Resolves once this process has been told to stop ([`listen_for_stop`]).
pub async fn stop_requested() {
    listen_for_stop();
    let mut rx = STOP.get().expect("listening for the stop signal").clone();
    if rx.wait_for(|stop| *stop).await.is_err() {
        // The listener's runtime is gone: no signal can arrive through it.
        std::future::pending::<()>().await;
    }
}


/// Until this process is told to stop: a SIGTERM/ctrl-c, or the mesh transport stopping for
/// good (a runtime that can neither be heard nor answer ends; its exit is the death proof the
/// fabric recovers from). `binary` names the process in the exit line.
pub async fn wait_for_signal(binary: &str) {
    let stopped = async {
        let reason = rdm_transport::topology::until_transport_stopped().await;
        tracing::info_span!("rdm.mesh.node.delete.via-transport-stopped", reason = %reason)
            .in_scope(|| tracing::error!("the mesh transport stopped; this runtime exits"));
        eprintln!("{binary}: the mesh transport stopped: {reason}");
        rdm_transport::flush_before_exit();
        rdm_core::runtime::exit_transport_stopped(&reason);
    };
    tokio::select! {
        () = stop_requested() => {}
        () = stopped => {}
    }
}

#[cfg(feature = "file-backend")]
/// [`start`], handing `register` the process's one client too (a product family that calls out,
/// or carries for others, holds it before the server is built).
pub async fn start_with_client(launch: &Launch, register: impl FnOnce(ServerBuilder, Arc<rdm_transport::LiveNodeResolver>, Arc<rdm_transport::NodeRpcClient>) -> ServerBuilder) -> Result<RunningNode> {
    start_with_seams(launch, |b, s| register(b, s.resolver, s.client)).await
}

#[cfg(feature = "file-backend")]
/// [`start_with_client`], handing `register` every process seam:
/// the resolver, the client, the connections writer and the held name.
pub async fn start_with_seams(launch: &Launch, register: impl FnOnce(ServerBuilder, NodeSeams) -> ServerBuilder) -> Result<RunningNode> {
    let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
    start_with_rafka_time_pre_node_ready_and(launch, rafka_time, crate::node_hooks::NodeHooks::none(), |s| s, None, register, &[]).await
}

#[cfg(feature = "file-backend")]
/// Start a node with explicit `rafka_time`, `pre_node_ready`, storage decorator, optional rafka-time knob, and `register` callback.
pub async fn start_with_rafka_time_pre_node_ready_and(
    launch: &Launch,
    rafka_time: rdm_transport::clock::RafkaTime,
    node_hooks: crate::node_hooks::NodeHooks,
    decorate_storage: impl FnOnce(Arc<dyn crate::storage::ConnectionsStorage>) -> Arc<dyn crate::storage::ConnectionsStorage>,
    knob_fn: Option<RafkaTimeKnobFn>,
    register: impl FnOnce(ServerBuilder, NodeSeams) -> ServerBuilder,
    types: &[&str],
) -> Result<RunningNode> {
    start_with_rafka_time_pre_node_ready_alpns_and(launch, rafka_time, node_hooks, decorate_storage, knob_fn, register, types, &[]).await
}

#[cfg(feature = "file-backend")]
/// [`start_with_rafka_time_pre_node_ready_and`], also registering the application's data-plane `alpns`.
#[allow(clippy::too_many_arguments)]
pub async fn start_with_rafka_time_pre_node_ready_alpns_and(
    launch: &Launch,
    rafka_time: rdm_transport::clock::RafkaTime,
    node_hooks: crate::node_hooks::NodeHooks,
    decorate_storage: impl FnOnce(Arc<dyn crate::storage::ConnectionsStorage>) -> Arc<dyn crate::storage::ConnectionsStorage>,
    knob_fn: Option<RafkaTimeKnobFn>,
    register: impl FnOnce(ServerBuilder, NodeSeams) -> ServerBuilder,
    types: &[&str],
    alpns: &[rdm_transport::connections::Alpn],
) -> Result<RunningNode> {
    start_on(launch, crate::storage_backend::file_node_backend(), rafka_time, node_hooks, decorate_storage, knob_fn, register, types, alpns).await
}

/// [`start_with_rafka_time_pre_node_ready_alpns_and`] on `storage`: the node's connections rows are the
/// ones it opens.
#[allow(clippy::too_many_arguments)]
pub async fn start_on(
    launch: &Launch,
    storage: Arc<dyn crate::storage_backend::NodeStorageBackend>,
    rafka_time: rdm_transport::clock::RafkaTime,
    node_hooks: crate::node_hooks::NodeHooks,
    decorate_storage: impl FnOnce(Arc<dyn crate::storage::ConnectionsStorage>) -> Arc<dyn crate::storage::ConnectionsStorage>,
    knob_fn: Option<RafkaTimeKnobFn>,
    register: impl FnOnce(ServerBuilder, NodeSeams) -> ServerBuilder,
    types: &[&str],
    alpns: &[rdm_transport::connections::Alpn],
) -> Result<RunningNode> {
    let app_types: Vec<String> = types.iter().map(|s| s.to_string()).collect();
    rdm_core::app_types::check(&app_types).map_err(|k| anyhow!("app type declaration: {k:?}"))?;
    rdm_transport::connections::validate_alpns(alpns).map_err(|e| anyhow!("{e}"))?;
    let boot = tracing::info_span!(parent: None, "rdm.mesh.node.create.via-boot", node = %launch.name, node_id = %launch.node_id, incarnation_id = %launch.incarnation.as_str(), kind = launch.name.kind.name());
    let started = crate::taskdump::watched("start", start_booted(launch, storage, rafka_time, node_hooks, decorate_storage, knob_fn, register, app_types, alpns.to_vec(), &boot)).await;
    if let Err(e) = &started {
        boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
    }
    started
}

#[allow(clippy::too_many_arguments)]
async fn start_booted(
    launch: &Launch,
    storage: Arc<dyn crate::storage_backend::NodeStorageBackend>,
    rafka_time: rdm_transport::clock::RafkaTime,
    node_hooks: crate::node_hooks::NodeHooks,
    decorate_storage: impl FnOnce(Arc<dyn crate::storage::ConnectionsStorage>) -> Arc<dyn crate::storage::ConnectionsStorage>,
    knob_fn: Option<RafkaTimeKnobFn>,
    register: impl FnOnce(ServerBuilder, NodeSeams) -> ServerBuilder,
    app_types: Vec<String>,
    alpns: Vec<rdm_transport::connections::Alpn>,
    boot: &tracing::Span,
) -> Result<RunningNode> {
    // An ordinary node is admitted by the admin that launched it (`node-ready-for-traffic.md`
    // step 1): a launch naming no launcher has nothing to be admitted by.
    let launcher = launch.launcher.as_ref().ok_or_else(|| anyhow!("{} was launched with no launcher: a node joins through the admin that deployed it and is refused without one", launch.name))?;
    let identity_exists = launch.data_dir.join("node-key").exists();
    let step = if identity_exists {
        tracing::info_span!(parent: boot, "rdm.mesh.node.resolve.via-identity-loaded", node = %launch.name)
    } else {
        tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-identity-minted", node = %launch.name)
    };
    let key = load_or_mint_key(&launch.data_dir)?;
    drop(step);
    // This process's one live resolver: a handler registered below may hold it; it is fed once
    // the topology view is joined.
    let resolver = Arc::new(rdm_transport::LiveNodeResolver::default());
    // This birth's exact runtime, as its provider recorded it: published with
    // the birth so any admin can manage it, whoever launched it.
    let dir = launch.data_dir.clone();
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.resolve.via-runtime-record", node = %launch.name);
    let runtime = crate::node_self::wait_for_own_record(&dir, Duration::from_secs(10)).await.map_err(|e| anyhow!("{e}"))?;
    drop(step);
    // The identity this process records its intentional exit under (a transport that stopped).
    rdm_core::runtime::set_own_exit(rdm_core::runtime::OwnExit {
        data_dir: launch.data_dir.clone(),
        deployment_id: runtime.deployment_id.clone(),
        incarnation: launch.incarnation.to_string(),
    });
    // One endpoint, one socket for the process: Node RPC, gossip and every registered app Alpn share it by ALPN.
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-endpoint-bound", node = %launch.name, requested = %launch.bind_addr, bound = tracing::field::Empty, listeners = tracing::field::Empty);
    let alpn_names: Vec<Vec<u8>> = alpns.iter().map(|a| a.name.clone()).collect();
    let ep0 = rdm_transport::endpoint::bind_with_alpns(key.clone(), launch.bind_addr, &alpn_names)
        .await
        .with_context(|| format!("the node cannot bind {}", launch.bind_addr))?;
    // The operating system assigned the port: the address the node reports is the one the bound
    // endpoint holds, never one anybody chose.
    let bound_addr = ep0.bound_sockets().into_iter().find(|a| a.is_ipv4()).ok_or_else(|| anyhow!("the endpoint bound at {} holds no IPv4 socket", launch.bind_addr))?;
    step.record("bound", tracing::field::display(bound_addr));
    // The listeners node-admin assigned (`RDM_LISTENERS`, Q151): each bound at its assigned address,
    // port 0, and held open from here through the join and for the birth's life; the port the
    // operating system selected is the one the join reports. A bind refused ends the start by name.
    let bound_listeners: Arc<Vec<(String, std::net::TcpListener)>> = Arc::new(bind_listeners(&launch.listeners)?);
    let listener_addrs: Vec<(String, std::net::SocketAddr)> = bound_listeners.iter().map(|(n, l)| l.local_addr().map(|a| (n.clone(), a))).collect::<std::io::Result<_>>().context("a bound listener reads back no address")?;
    step.record("listeners", rdm_core::launch::encode_listeners(&listener_addrs).as_str());
    drop(step);
    // The process's one client, made before the server is built: the server carries the proof
    // store for others through it (one direct inner call per forward, never a second hop).
    // This node's own connections: hydrated from its connections storage (the file backend's
    // `connections/<its path.name>/` of the fabric's store root, R14), then kept by what its client
    // observes of its pooled connections (connections.md sections 4.2, 9 and 10).
    // The storage fault the originate door arms (testkit only): the backend's storage, decorated.
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.resolve.via-connections-hydrated", node = %launch.name, writes_to = tracing::field::Empty);
    let node_storage = storage.open(launch).await.map_err(|e| anyhow!("{e}"))?;
    step.record("writes_to", node_storage.writes_to.as_str());
    let base_storage = node_storage.connections.clone();
    let connections = Arc::new(crate::connections_writer::ConnectionsWriter::new(
        rdm_core::connections::ConnectionEnd { name: launch.name.clone(), node_id: launch.node_id.clone(), incarnation: Some(launch.incarnation.clone()) },
        decorate_storage(base_storage),
        Arc::new(std::sync::Mutex::new(rdm_core::connections::ConnectionsHeld::new())),
    ));
    // The topology view's current process births: how the held projection judges a Proxy's recorded
    // destination and carrier, and a Direct fact's destination (connections.md section 8).
    connections.held().lock().unwrap().set_topology_view(resolver.clone());
    connections.hydrate().await.map_err(|e| anyhow!("connections hydrate: {e}"))?;
    drop(step);
    let (data_connections, alpn_handlers) = rdm_transport::connections::ConnectionsRegistry::new(
        ep0.clone(),
        resolver.clone(),
        connections.clone(),
        &alpns,
        Vec::new(),
    )
    .map_err(|e| anyhow!("{e}"))?;
    // node.connections.admit judges every dialer of the app's protocols on the root set the launch
    // carries (`RDM_ROOTS`), held where node.roots.update replaces it, on this node's rafka-time; a
    // launch with none (its launcher configured with no certs) admits by name.
    data_connections.admit_on(launch.roots.clone(), rafka_time.clone());
    // The control plane: Node RPC is no connection (connections.md "Two planes, two words"), so its
    // dials and accepts write nothing to the connections log.
    // One delivery state for the process (node-rpc.md §43): its birth mints every frame's identity,
    // and its clients and its server share the windows and the open invocations.
    let delivery = rdm_transport::Delivery::new(rdm_core::framing::Birth { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone() });
    let client = Arc::new(rdm_transport::NodeRpcClient::new(ep0.clone(), resolver.clone()).with_caller_system("rdm").with_delivery(delivery.clone()));
    // The data plane of RDM standalone: the originate door's own client, whose dials are the
    // connections this node originates for traffic, each a Direct entry of its log.
    let data_plane = Arc::new(rdm_transport::NodeRpcClient::new(ep0.clone(), resolver.clone()).with_caller_system("rdm").with_connection_observer(connections.clone()).with_delivery(delivery.clone()));
    // The node's one connections reconciler (connections.md §9.4, §10): an owed Proxy retirement
    // whose write was refused is attempted again while owed, and a recovery attempt an accept armed
    // (R-CN1) is dialled through the data plane once its backoff ends, its fact the data plane's own
    // observation. The task ends with the process.
    let recovery_dial: crate::connection_reconnect::RecoveryDial = {
        let (data_plane, connections) = (data_plane.clone(), connections.clone());
        Arc::new(move |destination: rdm_core::connections::ConnectionEnd| {
            let (data_plane, connections) = (data_plane.clone(), connections.clone());
            Box::pin(async move {
                use rdm_core::ping::{Ping, PingRequest};
                let (out, _) = data_plane.call::<Ping>(&rdm_transport::NodeTarget::ExactNode(destination.node_id.clone()), &PingRequest::Ping { payload: b"recovery".to_vec() }, &rdm_transport::CallOptions::default()).await;
                // The dial names the entry its connection opened under, its row owed (Q169).
                if let (true, Some(entry)) = (out.name() == "Reply", connections.opened_to(&destination)) {
                    tracing::Span::current().record("entry", entry.to_string().as_str());
                }
                out.name().to_string()
            })
        })
    };
    let alpn_dial = crate::connection_reconnect::registry_recovery_dial(&data_connections, Some(recovery_dial));
    let _reconciler = crate::connection_reconnect::spawn_alpn_reconciler(&connections, crate::connection_reconnect::RECONCILER_TICK, alpn_dial);
    // The status kick (node-identity-and-state.md §7.3): answered once this node has joined.
    let subject: KickSlot = Arc::new(std::sync::OnceLock::new());
    // The name this birth holds, read per call by every door that names it: its launch's until it joins.
    let held_name = crate::held_name::HeldName::of_birth(launch.name.clone());
    let seams = NodeSeams { resolver: resolver.clone(), client: data_plane.clone(), connections: connections.clone(), name: held_name.clone() };
    let topology_slot: crate::topology_read::TopologySlot = Arc::new(std::sync::OnceLock::new());
    // node.primary.update (op 0x21) on a node that is not a node-admin (lifecycles/node-primary-update.md):
    // the primary its mesh-primary commands, and the successor that primary calls.
    let primary_update_slot: crate::member_handover::MemberSlot = Arc::new(std::sync::OnceLock::new());
    // Closed until this node has joined its mesh (the kick slot is filled): every op, ping
    // included, is a typed NotReady before then.
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-catalog-built", node = %launch.name);
    // Every connection this node accepts from a live peer is a recovery trigger for its own dial
    // toward that birth, never a Direct entry (R-CN1).
    let server = serve_node_command(serve_kick(register(crate::member_handover::serve_member(crate::topology_read::serve(core_protocols(ServerBuilder::new().with_accept_trigger(resolver.clone(), connections.clone()), client.clone(), Some(connections.clone())), topology_slot.clone()), primary_update_slot.clone()), seams), subject.clone()), subject.clone())
        .carry::<rdm_core::status::Status>()
        .with_ready_gate(ready.clone())
        .with_delivery(delivery.clone())
        .build(rdm_transport::ServedBirth { node_id: launch.node_id.to_string(), incarnation: launch.incarnation.to_string() })
        .map_err(|e| anyhow!("protocol catalog refused to build: {e:?}"))?;
    drop(step);
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-gossip-started", node = %launch.name);
    let g = iroh_gossip::net::Gossip::builder().spawn(ep0.clone());
    drop(step);
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.add.via-alpn-registered", node = %launch.name, alpns = "node-rpc,gossip");
    let mut router_builder = Router::builder(ep0.clone())
        .accept(rdm_transport::ALPN, server.clone())
        .accept(iroh_gossip::ALPN, g.clone());
    for (alpn_bytes, handler) in alpn_handlers {
        router_builder = router_builder.accept(alpn_bytes, handler);
    }
    let routers = vec![router_builder.spawn()];
    drop(step);
    tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-accept-loop-started", node = %launch.name).in_scope(|| tracing::info!("the router accepts node-rpc and gossip connections"));
    let seeds: Vec<EndpointAddr> = launch
        .seeds
        .iter()
        .filter_map(|(k, a)| k.parse::<iroh::PublicKey>().ok().map(|pk| EndpointAddr::new(pk).with_ip_addr(*a)))
        .collect();
    let name = launch.name.to_string();
    // This birth's digest as it reports it: the address is the one the bound endpoint holds.
    let digest = MeshDigest { held_hash: rdm_core::app_types::hash(&app_types.iter().cloned().collect()),
        fabric_id: launch.fabric_id.clone(),
        node: MeshNode {
            node_id: launch.node_id.clone(),
            name: launch.name.clone(),
            endpoint_id: rdm_core::EndpointId(key.public().to_string()),
            incarnation: launch.incarnation.clone(),
            supersedes: launch.supersedes.clone(),
            transport_addr: bound_addr,
            runtime: Some(runtime.clone()),
        },
        status: MemberStatus::ReadyForTraffic,
        node_state: Default::default(),
        admin_api_base: None,
        // Stamped by `TopologyView::publish`: this birth's own sequence and the composed clock.
        emitted_at_rafka_ms: 0,
        digest_seq: 0,
        mesh_id: None,
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
        data_dir: Some(launch.data_dir.display().to_string()), is_primary: None, is_fabric_primary: None
    };
    // The join: this node's first call after it binds. `JoinNode` carries the digest above to the
    // admin that deployed it; the admin verifies it against the deployment and answers what it
    // holds. A node whose join is not accepted ends by name, whether the admin refused the digest
    // or could not take the join within the attempts: it is never ready for traffic.
    let launcher = launcher.clone();
    let resolve: Box<dyn FnOnce() -> Result<crate::workflows::node_start::JoinTo, crate::node_self::StartFailed> + Send> = {
        let (resolver, launch_name, launcher, seed) = (resolver.clone(), launch.name.clone(), launcher.clone(), seeds.first().cloned());
        Box::new(move || {
            use crate::node_self::StartFailed;
            let seed = seed.ok_or_else(|| StartFailed { step: "node.join", reason: format!("{} names launcher {} but no seed to reach it at", launch_name, launcher.name) })?;
            let launcher_ref = rdm_transport::ResolvedNode {
                node_id: launcher.node_id.clone(),
                name: launcher.name.clone(),
                endpoint_id: seed.id,
                transport_addr: seed.ip_addrs().next().copied().ok_or_else(|| StartFailed { step: "node.join", reason: "the launching admin's seed names no address".into() })?,
                incarnation: launcher.incarnation.clone(),
            };
            resolver.apply(launcher_ref, None);
            Ok(crate::workflows::node_start::JoinTo::Launcher { target: rdm_transport::NodeTarget::ExactNode(launcher.node_id.clone()), endpoint: seed.id, name: launcher.name.clone() })
        })
    };
    // What the start brought up, once it did: handed to the caller, or ended if the hook fails.
    let out: Arc<Mutex<Option<RunningNode>>> = Arc::default();
    let starting = crate::workflows::node_start::Starting { root: boot.clone(), name: name.clone(), path: launch.name.clone(), node_id: launch.node_id.clone(), client: client.clone(), rafka_time: rafka_time.clone() };
    let joining = crate::workflows::node_start::Joining::Authority { label: launcher.name.to_string() };
    let join_digest = digest.clone();
    let join_types = app_types.clone();
    let enter: crate::workflows::node_start::EnterSeam<anyhow::Error> = {
        let (launch, boot, out, name, data_connections, bound_listeners, listener_addrs) = (launch.clone(), boot.clone(), out.clone(), name.clone(), data_connections.clone(), bound_listeners.clone(), listener_addrs.clone());
        Box::new(move |step: tracing::Span, joined: Option<crate::wire::JoinAnswer>| -> crate::workflows::node_start::BoxFut<Result<(TopologyView, crate::workflows::node_start::JoinedSeam<anyhow::Error>), anyhow::Error>> {
            Box::pin(async move {
                let joined = joined.expect("a first start through its launcher enters its channel with the join answer");
                let member_cert = joined.control.member_cert.clone();
                install_join_cert(&data_connections, &member_cert, &launch.name.to_string(), &launch.node_id, &launch.incarnation);
                // The mesh's id names its channel; the launching admin writes it.
                let mesh_id = launch.mesh_id.clone().ok_or_else(|| anyhow!("a node needs its mesh's id from its launch"))?;
                step.record("mesh_id", tracing::field::display(&mesh_id));
                step.record("seeds", seeds.len());

            let topology_view = TopologyView::join(&g, &ep0, &launch.fabric_id, &launch.name.mesh, &mesh_id, &name, Arc::new(rafka_time.clone()), seeds, &app_types.iter().map(String::as_str).collect::<Vec<_>>()).await?;
            topology_view.set_author(&rdm_core::PublisherId { node: name.clone(), node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone() });
            // The name this birth holds is its topology view's (R12): its connections rows name it as their
            // source from here on, and every door names the node by it, whatever name the join hands it
            // or an `ApplyNodeName` gives it.
            {
                let (held, birth) = (topology_view.clone(), launch.name.clone());
                connections.follow_held_name(move || held.held_name(&birth)).map_err(|e| anyhow!(e))?;
                let (held, birth) = (topology_view.clone(), launch.name.clone());
                held_name.follow(move || held.held_name(&birth)).map_err(|e| anyhow!(e))?;
            }
            // The row's name and config the join answer handed are held before this node publishes
            // anything or declares readiness: every digest from its first carries them (R-JN1).
            install_handed(&topology_view, &joined.control);
            let current_name = topology_view.node_name();
            tracing::info_span!(
                parent: &step,
                "rdm.node.mesh.update.via-joined",
                node = %current_name,
                node_id = %launch.node_id,
                mesh = %launch.name.mesh,
                mesh_id = %mesh_id,
                which = rdm_core::status::WhichMesh::Own.name(),
            )
            .in_scope(|| tracing::info!("joined the mesh channel"));
            drop(step);
            let (node_rpc, node_rpc_feed) = crate::node_rpc::ProcessNodeRpc::with_client(resolver.clone(), client.clone(), &topology_view.digest_view, &current_name);
            // This node serves the topology it holds from here on (until now a read is `NotReady`).
            let status = Arc::new(Mutex::new(MemberStatus::Pending));
            {
                let (own, st, m) = (digest.clone(), status.clone(), topology_view.clone());
                let _ = topology_slot.set(Arc::new(crate::topology_read::TopologyDoor::new(
                    topology_view.clone(),
                    Arc::new(move || {
                        let mut d = own.clone();
                        d.status = *st.lock().unwrap();
                        // The config this birth was told rides its own digest here as in every publish.
                        m.with_config(&d)
                    }),
                    rafka_time.clone(),
                )));
            }
                let channel = topology_view.clone();
                let joined_seam: crate::workflows::node_start::JoinedSeam<anyhow::Error> = Box::new(
                    move |_: Option<crate::workflows::node_start::JoinThrough>| -> crate::workflows::node_start::BoxFut<Result<(crate::workflows::node_start::HookParts, Option<crate::workflows::node_start::HookStarted<anyhow::Error>>), anyhow::Error>> {
                        Box::pin(async move {
                            // A delta that does not follow what this node holds desynchronizes that source; the node tops
                            // up from its Mesh's own primary (gossip.md §3.3) with `GetTopology` of each such mesh.
                            topology_view.spawn_top_up(crate::topology_read::top_up_fetch(client.clone()));
                            // From here the node is on the mesh, Pending: it answers status and its node commands, and
                            // says Pending on the topology cadence while its `pre_node_ready` hook runs. It is
                            // published ReadyForTraffic only once the hook has passed ("born full").
                            let cancel = rdm_transport::Retirement::new();
                            let addressed = crate::node_hooks::Addressed::new();
                            let owed_state: Arc<Mutex<OwedDeclaration>> = Arc::default();
                            let own = {
                                let (set_digest, set_status_cell, now_status) = (digest.clone(), status.clone(), status.clone());
                                let set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync> = Arc::new(move |st| set_own_status(&set_status_cell, &set_digest, st));
                                let current: Arc<dyn Fn() -> MemberStatus + Send + Sync> = Arc::new(move || *now_status.lock().unwrap());
                                let declare: crate::node_self::Declare = {
                                    let (node, topology_view, client, owed, held) = (digest.node.clone(), topology_view.clone(), client.clone(), owed_state.clone(), connections.held());
                                    Arc::new(move |state, operation| {
                                        owed.lock().unwrap().owe((state, operation));
                                        let (node, topology_view, client, owed, held) = (node.clone(), topology_view.clone(), client.clone(), owed.clone(), held.clone());
                                        Box::pin(async move {
                                            let _ = declare_until_mesh_primary_moves(&node, &topology_view, &client, &owed, &held).await;
                                        })
                                    })
                                };
                                Arc::new(
                                    crate::node_self::NodeSelf::new(launch.node_id.clone(), launch.incarnation.clone(), launch.name.clone(), server.clone(), client.clone(), node_rpc.resolver.clone(), topology_view.clone(), set_status, current, Some(declare), launch.connections_delete_deadline)
                                        .with_cancel(cancel.clone()),
                                )
                            };
                            own.set_connections(connections.clone());
                            own.set_data_plane(data_plane.clone());
                            own.set_data_connections(data_connections.clone());
                            let publisher: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::default();
                            let pre_node_ready_cell: Arc<Mutex<Option<crate::node_hooks::PreNodeReadyHandle>>> = Arc::default();
                            let pre_node_ready_task_cell: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::default();
                            own.set_session(Arc::new(NodeSession {
                                name: launch.name.clone(),
                                node_id: launch.node_id.clone(),
                                incarnation: launch.incarnation.clone(),
                                fabric_id: launch.fabric_id.clone(),
                                mesh_id: mesh_id.clone(),
                                runtime: runtime.clone(),
                                topology_view: topology_view.clone(),
                                client: client.clone(),
                                server: server.clone(),
                                digest: digest.clone(),
                                status: status.clone(),
                                owed: owed_state.clone(),
                                held: connections.held(),
                                data_connections: data_connections.clone(),
                                rafka_time: rafka_time.clone(),
                                node_hooks: node_hooks.clone(),
                                addressed: addressed.clone(),
                                publisher: publisher.clone(),
                                pre_node_ready_cell: pre_node_ready_cell.clone(),
                                pre_node_ready_task: pre_node_ready_task_cell.clone(),
                                listeners: listener_addrs.clone(),
                            }));
                            let _ = primary_update_slot.set(Arc::new(crate::member_handover::MemberDoor::new(launch.name.clone(), launch.node_id.clone(), launch.incarnation.clone(), topology_view.clone(), client.clone())));
                            let _ = subject.set(Arc::new(Kicked { topology_view: topology_view.clone(), digest: digest.clone(), status: status.clone(), own: own.clone(), addressed: addressed.clone(), pre_node_ready: pre_node_ready_cell.clone(), stats: server.stats() }));
                            ready.store(true, std::sync::atomic::Ordering::SeqCst);
                            *publisher.lock().unwrap() = Some(spawn_publisher(&topology_view, &digest, &status, &server));
                            // The owed declaration's owner: whatever state this birth owes is one Owed call to the
                            // holder of its mesh's primary seat (i146 Q52), which ends on the holder's answer or
                            // proven exit; nothing here sends it again on a cadence. When the mesh's primary seat
                            // moves to another birth, the call in flight is dropped and the state is owed to the new
                            // primary (R-S2: a declaration is owed again when the authority is another node or a new
                            // incarnation of the same node): that declaration is how the new primary learns this birth
                            // addresses it, and sends again a down op this birth refused while it still named the old holder.
                            let declare_loop = {
                                let (node, topology_view, client, owed, status, held) = (digest.node.clone(), topology_view.clone(), node_rpc.client.clone(), owed_state.clone(), status.clone(), connections.held());
                                tokio::spawn(async move {
                                    let mut seats = topology_view.primaries().subscribe();
                                    let mut told: Option<(NodeId, IncarnationId)> = None;
                                    loop {
                                        seats.borrow_and_update();
                                        let holder = topology_view.primaries().mesh(&node.name.mesh);
                                        if mesh_primary_moved(told.as_ref(), holder.as_ref()) && owed.lock().unwrap().owed.is_none() {
                                            let state = node_state_of(*status.lock().unwrap());
                                            tracing::info_span!("rdm.node_rpc.status.update.via-mesh-primary-moved", node = %topology_view.held_name(&node.name), state = ?state, to = %holder.as_ref().map(|h| h.node_id.to_string()).unwrap_or_default())
                                                .in_scope(|| tracing::info!("the mesh's primary seat moved to another birth: this birth's state is owed to it"));
                                            owed.lock().unwrap().owe_again(state);
                                        }
                                        // The seat moving is the one retargeting (i146 Q39): a call still in flight
                                        // to the old holder is cancelled, and the next pass declares to the holder
                                        // the seat names now.
                                        if owed.lock().unwrap().owed.is_some() && declare_until_mesh_primary_moves(&node, &topology_view, &client, &owed, &held).await == Declared::FabricPrimaryMoved {
                                            continue;
                                        }
                                        if owed.lock().unwrap().owed.is_none() {
                                            told = holder.map(|h| (h.node_id, h.incarnation));
                                        }
                                        // A declaration still owed after its call ended (its holder answered that it is
                                        // not the authority, or was proven gone) waits for the seat to name a holder.
                                        if seats.changed().await.is_err() {
                                            return;
                                        }
                                    }
                                })
                            };
                            let becoming = Becoming {
                                boot: boot.clone(),
                                name: topology_view.node_name(),
                                kind: launch.name.kind.name(),
                                incarnation_id: launch.incarnation.to_string(),
                                node_id: launch.node_id.clone(),
                                mesh_id: mesh_id.to_string(),
                                fabric_id: launch.fabric_id.to_string(),
                                runtime: runtime.clone(),
                                topology_view: topology_view.clone(),
                                digest: digest.clone(),
                                status: status.clone(),
                                owed: owed_state.clone(),
                                client: node_rpc.client.clone(),
                                held: connections.held(),
                                operation: None,
                                progress: None,
                            };
                            let parts = crate::workflows::node_start::HookParts {
                                birth: crate::node_hooks::Birth {
                                    name: launch.name.clone(),
                                    node_id: launch.node_id.clone(),
                                    incarnation: launch.incarnation.clone(),
                                    fabric_id: launch.fabric_id.clone(),
                                    mesh_id: mesh_id.clone(),
                                    kind: launch.name.kind,
                                },
                                node_hooks: node_hooks.clone(),
                                events: crate::node_hooks::Events::new(topology_view.digest_view.clone(), addressed),
                                cancel: cancel.token(),
                                state: pre_node_ready_cell.clone(),
                                runs: crate::workflows::node_start::HookRuns::Inline { blocked: pre_node_ready_task_cell.clone() },
                                after: Box::new(RpcAfterHook {
                                    fabric_id: launch.fabric_id.clone(),
                                    node_id: launch.node_id.clone(),
                                    name: launch.name.clone(),
                                    becoming,
                                    // A step that fails ends the node by name from its start, as an unaccepted join does.
                                    ends: Some(out.clone()),
                                    knob: knob_fn.and_then(|f| f(&launch, &rafka_time, &boot)),
                                    leave: None,
                                }),
                            };
                            *out.lock().unwrap() = Some(RunningNode { rafka_time: rafka_time.clone(), member_cert, routers, topology_view, server, status, digest, node_rpc, owed_state, cancel, own, pre_node_ready: pre_node_ready_cell, pre_node_ready_task: pre_node_ready_task_cell, publisher, node_rpc_feed, held: connections.held(), declare_loop, data_connections, listeners: bound_listeners.clone() });
                            Ok::<_, anyhow::Error>((parts, None))
                        })
                    },
                );
                Ok::<_, anyhow::Error>((channel, joined_seam))
            })
        })
    };
    let mut start = crate::workflows::node_start::FirstStart { types: join_types, listeners: listener_addrs,
        starting: Some(starting),
        joining: Some(joining),
        resolve: Some(resolve),
        digest: Some(join_digest),
        enter: Some(enter),
        joined: None,
        hook_started: None,
    };
    crate::workflows::node_start::node_start(&mut start, crate::node_hooks::StartKind::FirstStart).await?;
    let running = out.lock().unwrap().take();
    running.ok_or_else(|| anyhow!("{name}: the start ended without the node it brought up"))
}

/// Set this birth's own status, the one cell its digest cadence, its topology door and its answers
/// read, to `to`, and hand back `digest` carrying it: the one place an rpc or role node sets its own
/// status (every status its `NodeSelf` sets, and its ReadyForTraffic at node.started). The rule a set
/// applies before it is made is its caller's.
fn set_own_status(status: &Mutex<MemberStatus>, digest: &MeshDigest, to: MemberStatus) -> MeshDigest {
    *status.lock().unwrap() = to;
    let mut d = digest.clone();
    d.status = to;
    d
}

/// The digest cadence of a node: its digest as it stands, its status and its in-flight count.
fn spawn_publisher(topology_view: &TopologyView, digest: &MeshDigest, status: &Arc<Mutex<MemberStatus>>, server: &NodeRpcServer) -> tokio::task::JoinHandle<()> {
    let (d, st, stats) = (digest.clone(), status.clone(), server.stats());
    topology_view.publish_every(rdm_transport::topology::gossip_interval(), move || {
        let mut d = d.clone();
        d.status = *st.lock().unwrap();
        d.in_flight = Some(rdm_transport::ServerStats::get(&stats.in_flight));
        d
    })
}

/// How an rpc node or role node leaves its mesh when stopped and joins it again when started
/// (`node_self::Session`): the parts it was born with, kept for the rejoin.
struct NodeSession {
    name: rdm_core::PathName,
    node_id: NodeId,
    incarnation: IncarnationId,
    fabric_id: rdm_core::FabricId,
    mesh_id: rdm_core::MeshId,
    runtime: rdm_core::runtime::RuntimeFact,
    topology_view: TopologyView,
    client: Arc<rdm_transport::NodeRpcClient>,
    server: NodeRpcServer,
    digest: MeshDigest,
    status: Arc<Mutex<MemberStatus>>,
    /// This node's connections projection: the owed declaration's carriers ([`declaration_route`]).
    held: Arc<Mutex<rdm_core::connections::ConnectionsHeld>>,
    owed: Arc<Mutex<OwedDeclaration>>,
    /// The app protocols this birth opens connections on: what the member cert each join answers is
    /// installed on.
    data_connections: rdm_transport::connections::ConnectionsRegistry,
    rafka_time: rdm_transport::clock::RafkaTime,
    node_hooks: crate::node_hooks::NodeHooks,
    addressed: Arc<crate::node_hooks::Addressed>,
    publisher: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pre_node_ready_cell: Arc<Mutex<Option<crate::node_hooks::PreNodeReadyHandle>>>,
    pre_node_ready_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// The listeners this birth bound from its launch, each with its selected port: every join of
    /// the birth reports them (Q151).
    listeners: Vec<(String, std::net::SocketAddr)>,
}

#[async_trait::async_trait]
impl crate::node_self::Session for NodeSession {
    async fn leave_channels(&self) {
        if let Some(p) = self.publisher.lock().unwrap().take() {
            p.abort();
        }
        if let Some(h) = self.pre_node_ready_task.lock().unwrap().take() {
            h.abort();
        }
        // A birth off its channels serves no app op until a start's hook has passed again.
        self.node_hooks.gate().close();
        crate::node_self::leave_channels(&self.topology_view, None, &self.node_id).await;
    }

    async fn cut(&self, spare: &[iroh::PublicKey]) -> crate::node_self::CutReport {
        let neighbours = self.topology_view.neighbours();
        self.leave_channels().await;
        let pooled = self.client.close_pooled("node.connections.delete");
        let accepted = self.server.close_connections_except(spare, "node.connections.delete");
        crate::node_self::CutReport { neighbours, pooled, accepted, turns_in_progress: 0 }
    }

    async fn rejoin(&self, retire: rdm_transport::CancelToken, commander: tokio::sync::watch::Receiver<iroh::PublicKey>, rows: Option<crate::connections_writer::RowsHeld>, operation: rdm_core::status::DeclaredOperation, progress: Option<crate::node_self::CommandProgress>, held_again: Option<tokio::sync::oneshot::Sender<()>>) -> Result<(), crate::node_self::StartFailed> {
        // The digest this birth presents is the one it holds: a name it took live (ApplyNodeName)
        // rides it, never the birth's frozen name (R-JN1).
        // The stopped cycle's declaration ends with it: the start begins the next cycle, whose states
        // this birth declares from here (its ReadyForTraffic names the start).
        self.owed.lock().unwrap().owed = None;
        let held = self.topology_view.with_config(&self.digest);
        let name = held.node.name.to_string();
        let root = tracing::info_span!(parent: None, "rdm.mesh.node.update.via-start", node = %name, node_id = %self.node_id, incarnation_id = %self.incarnation.as_str());
        let mut start = AfterStop { session: self, root, name, held, commander, retire: Some(retire), rows, operation, progress, held_again };
        crate::workflows::node_start::node_start(&mut start, crate::node_hooks::StartKind::AfterStop).await
    }

}

impl NodeSession {
    /// This rpc node's channel entry inside `node.mesh.join`, for its start and its standalone join
    /// alike: the row's name and config the join answer handed are held before the node re-enters its
    /// topic or says anything (a node renamed while stopped publishes its row's name, R-JN1), and only
    /// then are its connections recorded (`rows`), under the handed name.
    async fn enter_mesh(&self, step: tracing::Span, joined: Option<&crate::wire::JoinAnswer>, rows: Option<crate::connections_writer::RowsHeld>) -> TopologyView {
        step.record("mesh_id", tracing::field::display(&self.mesh_id));
        if let Some(joined) = joined {
            install_handed(&self.topology_view, &joined.control);
            install_join_cert(&self.data_connections, &joined.control.member_cert, &self.topology_view.node_name().to_string(), &self.node_id, &self.incarnation);
        }
        drop(rows);
        self.topology_view.start().await;
        tracing::info_span!(
            "rdm.node.mesh.update.via-joined",
            node = %self.topology_view.node_name(),
            node_id = %self.node_id,
            mesh = %self.topology_view.mesh_name(),
            mesh_id = %self.mesh_id,
            which = rdm_core::status::WhichMesh::Own.name(),
        )
        .in_scope(|| tracing::info!("joined the mesh channel"));
        drop(step);
        self.topology_view.clone()
    }
}

/// An rpc node's start after its stop, as it plugs into `node.start`: it rejoins the admin that
/// commanded the start, through the parts it was born with.
struct AfterStop<'a> {
    session: &'a NodeSession,
    root: tracing::Span,
    /// The name the birth holds at the start.
    name: String,
    /// The digest the birth holds: the one its join presents.
    held: MeshDigest,
    /// The endpoint key of the call that commanded the start last: the join goes back over that call,
    /// and the admin that answers is the hook's accepting authority.
    commander: tokio::sync::watch::Receiver<iroh::PublicKey>,
    /// The new cycle's token: the hook runs under it.
    retire: Option<rdm_transport::CancelToken>,
    /// The start's hold on this birth's connections rows, dropped once the handed name is held.
    rows: Option<crate::connections_writer::RowsHeld>,
    /// The start's operation: the ReadyForTraffic this birth declares at the start's end names it.
    operation: rdm_core::status::DeclaredOperation,
    progress: Option<crate::node_self::CommandProgress>,
    /// Sent once this birth holds its mesh again: a commanded `node.mesh.join` answers on it.
    held_again: Option<tokio::sync::oneshot::Sender<()>>,
}

#[async_trait::async_trait]
impl crate::workflows::node_mesh_join::MeshJoin for AfterStop<'_> {
    type Error = crate::node_self::StartFailed;

    fn join_types(&self) -> Vec<String> { self.session.topology_view.app_types().into_iter().collect() }

    fn join_listeners(&self) -> Vec<(String, std::net::SocketAddr)> {
        self.session.listeners.clone()
    }

    fn join_digest(&self) -> MeshDigest {
        self.held.clone()
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&crate::wire::JoinAnswer>) -> Result<TopologyView, crate::node_self::StartFailed> {
        Ok(self.session.enter_mesh(step, joined, self.rows.take()).await)
    }
}

#[async_trait::async_trait]
impl crate::workflows::node_start::StartNode for AfterStop<'_> {

    fn starting(&self) -> crate::workflows::node_start::Starting {
        let s = self.session;
        crate::workflows::node_start::Starting { root: self.root.clone(), name: self.name.clone(), path: s.name.clone(), node_id: s.node_id.clone(), client: s.client.clone(), rafka_time: s.rafka_time.clone() }
    }

    fn joining(&self) -> crate::workflows::node_start::Joining {
        crate::workflows::node_start::Joining::Authority { label: self.commander.borrow().fmt_short().to_string() }
    }

    async fn resolve_join_to(&mut self) -> Result<crate::workflows::node_start::JoinTo, crate::node_self::StartFailed> {
        Ok(crate::workflows::node_start::JoinTo::Commander(self.commander.clone()))
    }

    fn operation(&self) -> Option<String> {
        Some(self.operation.operation.clone())
    }

    async fn joined(
        &mut self,
        topology_view: &TopologyView,
        _: Option<&crate::workflows::node_start::JoinThrough>,
        _: Option<&crate::wire::JoinAnswer>,
    ) -> Result<crate::workflows::node_start::HookParts, crate::node_self::StartFailed> {
        let s = self.session;
        // From here on it is named by the name it installed from its join answer (`enter_channel`).
        let me = s.topology_view.held_name(&s.name);
        // Back on its channel, this node reports NodeJoined: the start finishes on its direct calls and
        // the node is back in gossip whenever gossip gets there. Its mesh-primary recorded its Pending at
        // the accepted join report (node-mesh-join.md steps 9 and 10, Q97).
        if let Some(progress) = &self.progress {
            let _ = progress.lock().await.progress(rdm_core::node_command::NodeCommandReply::NodeJoined).await;
        }
        if let Some(held) = self.held_again.take() {
            let _ = held.send(());
        }
        *s.publisher.lock().unwrap() = Some(spawn_publisher(topology_view, &s.digest, &s.status, &s.server));
        let becoming = Becoming {
            boot: self.root.clone(),
            name: me.to_string(),
            kind: s.name.kind.name(),
            incarnation_id: s.incarnation.to_string(),
            node_id: s.node_id.clone(),
            mesh_id: s.mesh_id.to_string(),
            fabric_id: s.fabric_id.to_string(),
            runtime: s.runtime.clone(),
            topology_view: topology_view.clone(),
            digest: s.digest.clone(),
            status: s.status.clone(),
            owed: s.owed.clone(),
            client: s.client.clone(),
            held: s.held.clone(),
            operation: Some(self.operation.clone()),
            progress: self.progress.clone(),
        };
        Ok(crate::workflows::node_start::HookParts {
            birth: crate::node_hooks::Birth { name: me.clone(), node_id: s.node_id.clone(), incarnation: s.incarnation.clone(), fabric_id: s.fabric_id.clone(), mesh_id: s.mesh_id.clone(), kind: s.name.kind },
            node_hooks: s.node_hooks.clone(),
            events: crate::node_hooks::Events::new(topology_view.digest_view.clone(), s.addressed.clone()),
            cancel: self.retire.take().expect("the hook's token is handed once"),
            state: s.pre_node_ready_cell.clone(),
            runs: crate::workflows::node_start::HookRuns::Inline { blocked: s.pre_node_ready_task.clone() },
            after: Box::new(RpcAfterHook {
                fabric_id: s.fabric_id.clone(),
                node_id: s.node_id.clone(),
                name: me.clone(),
                becoming,
                ends: None,
                knob: None,
                leave: self.operation.operation.starts_with("node.mesh.join:").then(|| Leave { topology_view: topology_view.clone(), node_hooks: s.node_hooks.clone(), pre_node_ready_task: s.pre_node_ready_task.clone(), publisher: s.publisher.clone(), node_id: s.node_id.clone(), name: me.to_string() }),
            }),
        })
    }
}

/// An rpc node's steps after its hook: it reaches the peers of its mesh the topology names, owes no
/// Build floor (it holds no Build log), and reports through [`Becoming`].
struct RpcAfterHook {
    fabric_id: rdm_core::FabricId,
    /// The birth's node id: the topology's members it reaches are every other node of its mesh.
    node_id: NodeId,
    /// The name the birth holds: its join answer's at a start after a stop (R-JN1).
    name: rdm_core::PathName,
    becoming: Becoming,
    /// What a step that fails inside the start ends: a first start's node (a start after a stop ends
    /// nothing; its process stays stopped's).
    ends: Option<Arc<Mutex<Option<RunningNode>>>>,
    /// A scenario's rafka-time knob, applied once rafka-time is refreshed, before the report.
    knob: Option<Box<dyn FnOnce() + Send>>,
    /// A commanded `node.mesh.join` (Q87): a step that fails after the hook leaves the birth off its
    /// channels, Pending, for its authority's next command (node-recovery.md step 2), as a failure
    /// inside the start's own task does; a Build's start after a stop leaves nothing (its process stays stopped).
    leave: Option<Leave>,
}

/// What a birth does to leave its channels from a task that is not the session's: the parts
/// `Session::leave_channels` uses, cloned; the channel leave itself is the one
/// `crate::node_self::leave_channels`.
struct Leave {
    topology_view: TopologyView,
    node_hooks: crate::node_hooks::NodeHooks,
    pre_node_ready_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    publisher: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    node_id: NodeId,
    name: String,
}

impl Leave {
    async fn run(self, step: &str, reason: &str) {
        if let Some(p) = self.publisher.lock().unwrap().take() {
            p.abort();
        }
        if let Some(h) = self.pre_node_ready_task.lock().unwrap().take() {
            h.abort();
        }
        self.node_hooks.gate().close();
        crate::node_self::leave_channels(&self.topology_view, None, &self.node_id).await;
        tracing::info_span!("rdm.mesh.node.reject.via-mesh-join-start-failed", node = %self.name, step, reason)
            .in_scope(|| tracing::info!("the commanded start stopped at a step after its hook: the birth leaves its channels for its authority's next command"));
    }
}

#[async_trait::async_trait]
impl crate::workflows::node_start::AfterHook for RpcAfterHook {
    async fn topology_installed(&mut self, topology_view: &TopologyView, read: &crate::topology_read::TopologyRead) {
        let mesh_peers: Vec<EndpointAddr> = read
            .installed
            .iter()
            .flat_map(|m| m.members.iter())
            .filter(|d| d.fabric_id == self.fabric_id && d.node.node_id != self.node_id && d.node.name.mesh == self.name.mesh)
            .filter_map(rdm_transport::topology::gossip_addr)
            .collect();
        let _ = topology_view.join_peers_as("node.mesh_join", mesh_peers).await;
    }

    async fn report(self: Box<Self>) {
        let RpcAfterHook { becoming, knob, .. } = *self;
        if let Some(knob) = knob {
            knob();
        }
        becoming.go().await;
    }

    async fn start_failed(self: Box<Self>, failed: &crate::node_self::StartFailed, _: bool) {
        let running = self.ends.and_then(|out| out.lock().unwrap().take());
        if let Some(running) = running {
            running.close().await;
        }
        if let Some(leave) = self.leave {
            leave.run(failed.step, &failed.reason).await;
        }
    }
}



/// What turns a Pending node whose hook passed into a ReadyForTraffic one: `via-ready` closes the
/// boot trace, the status moves, the digest is published at once and the owed declaration is sent.
struct Becoming {
    boot: tracing::Span,
    name: String,
    kind: &'static str,
    incarnation_id: String,
    node_id: rdm_core::NodeId,
    mesh_id: String,
    fabric_id: String,
    runtime: rdm_core::runtime::RuntimeFact,
    topology_view: TopologyView,
    digest: MeshDigest,
    status: Arc<Mutex<MemberStatus>>,
    owed: Arc<Mutex<OwedDeclaration>>,
    client: Arc<rdm_transport::NodeRpcClient>,
    /// This node's connections projection: the owed declaration's carriers ([`declaration_route`]).
    held: Arc<Mutex<rdm_core::connections::ConnectionsHeld>>,
    /// The operation that makes this birth ready: the start's, or `None` at its first boot, a state
    /// the birth measured itself (i146 Q36).
    operation: Option<rdm_core::status::DeclaredOperation>,
    progress: Option<crate::node_self::CommandProgress>,
}

impl Becoming {
    /// node.started: the report `node.start` makes once its steps after the hook ended.
    async fn go(&self) {
        // A node retired while its hook ran is not Pending any more and is not made Ready.
        if *self.status.lock().unwrap() != MemberStatus::Pending {
            return;
        }
        tracing::info_span!(
            parent: &self.boot,
            "rdm.mesh.node.update.via-ready",
            node = %self.name,
            kind = self.kind,
            incarnation_id = %self.incarnation_id,
            meshes = self.topology_view.meshes_held(),
            node_id = %self.node_id,
            mesh_id = %self.mesh_id,
            fabric_id = %self.fabric_id,
            id_format = rdm_core::ID_FORMAT,
            deployment_id = %self.runtime.deployment_id,
            provider = self.runtime.provider.as_str(),
            provider_control_domain_fingerprint = %self.runtime.domain_fingerprint(),
            runtime_locator_kind = self.runtime.locator.kind(),
            runtime_locator_fingerprint = %self.runtime.locator_fingerprint(),
            source = "self-published-topology"
        )
        .in_scope(|| tracing::info!("ready for traffic"));
        let d = set_own_status(&self.status, &self.digest, MemberStatus::ReadyForTraffic);
        if self.progress.is_none() {
            self.owed.lock().unwrap().owe((rdm_core::status::NodeState::ReadyForTraffic, self.operation.clone()));
        } else {
            // The open start stream reports readiness; keep the declaration for a future seat move.
            self.owed.lock().unwrap().last = Some((rdm_core::status::NodeState::ReadyForTraffic, self.operation.clone()));
        }
        let _ = self.topology_view.publish(&d).await;
        let (node, topology_view, client, owed, held) = (self.digest.node.clone(), self.topology_view.clone(), self.client.clone(), self.owed.clone(), self.held.clone());
        tokio::spawn(async move {
            declare_until_mesh_primary_moves(&node, &topology_view, &client, &owed, &held).await;
        });
    }
}

/// Whether the primary seat of this birth's mesh moved to another birth than the one this birth
/// last told its state: the holder it told is `told`, the holder its seat record names is `holder`.
/// A birth that told no holder yet has nothing to repeat.
fn mesh_primary_moved(told: Option<&(NodeId, IncarnationId)>, holder: Option<&rdm_core::Primary>) -> bool {
    match (told, holder) {
        (Some((node, incarnation)), Some(h)) => !h.is_birth(node, incarnation),
        _ => false,
    }
}

/// A declaration: the state and the operation that produced it, when one did (i146 Q36).
pub(crate) type Declaration = (rdm_core::status::NodeState, Option<rdm_core::status::DeclaredOperation>);

/// What this birth owes its mesh-primary: the declaration still to be answered by name, and the last
/// one it made, owed again with its operation when the mesh's primary seat moves to another birth.
#[derive(Debug, Default)]
pub(crate) struct OwedDeclaration {
    pub(crate) owed: Option<Declaration>,
    last: Option<Declaration>,
}

impl OwedDeclaration {
    /// Owe `declaration` from now on, until it is answered by name.
    pub(crate) fn owe(&mut self, declaration: Declaration) {
        self.last = Some(declaration.clone());
        self.owed = Some(declaration);
    }

    /// Owe `state` again to a new authority: with the operation of the last declaration when that
    /// declared the same state, else as a state the birth measured itself.
    fn owe_again(&mut self, state: rdm_core::status::NodeState) {
        let operation = self.last.clone().filter(|(s, _)| *s == state).and_then(|(_, op)| op);
        self.owe((state, operation));
    }
}

/// How [`declare_until_seat_moves`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Declared {
    /// The attempt ran to its end (answered, refused, or not delivered: still owed).
    Attempted,
    /// The mesh's primary seat moved while the attempt was in flight: its call was cancelled.
    FabricPrimaryMoved,
}

/// [`declare_once`] until this birth's mesh's primary seat moves (i146 Q39): the seat moving is the one
/// retargeting, so a call still in flight to the old holder is cancelled, and the declaration stays
/// owed to the holder the seat names next. A seat record of another mesh changes nothing here.
async fn declare_until_mesh_primary_moves(me: &MeshNode, topology_view: &TopologyView, client: &rdm_transport::NodeRpcClient, owed: &Mutex<OwedDeclaration>, held: &Arc<Mutex<rdm_core::connections::ConnectionsHeld>>) -> Declared {
    let birth = |h: Option<rdm_core::Primary>| h.map(|h| (h.node_id, h.incarnation));
    let mut seats = topology_view.primaries().subscribe();
    seats.borrow_and_update();
    let from = birth(topology_view.primaries().mesh(&me.name.mesh));
    let moved = async {
        loop {
            if seats.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
            if birth(topology_view.primaries().mesh(&me.name.mesh)) != from {
                break;
            }
        }
    };
    tokio::select! {
        _ = declare_once(me, topology_view, client, owed, held) => Declared::Attempted,
        () = moved => {
            tracing::info_span!("rdm.node_rpc.status.remove.via-seat-moved", node = %topology_view.held_name(&me.name), from = %from.as_ref().map(|(n, _)| n.to_string()).unwrap_or_default())
                .in_scope(|| tracing::info!("the mesh's primary seat moved while the declaration was in flight: its call to the old holder is cancelled"));
            Declared::FabricPrimaryMoved
        }
    }
}

/// Q39's carrier rule as the route policy of one owed declaration (i146 Q52): direct while this node
/// holds a connected Direct edge to the primary (its connections table, as the route projection answers),
/// else the live peers of the holder's mesh, never the holder and never this birth: the members this
/// node holds a connected Direct edge to first, else the members it hears; each attempt takes the next
/// in turn. With no such peer the attempt goes direct.
fn declaration_route(held: &Mutex<rdm_core::connections::ConnectionsHeld>, topology_view: &TopologyView, me: &NodeId, holder: &NodeId, mesh: &str, attempt: u32) -> rdm_transport::route::RouteChoice {
    let connected: Vec<rdm_core::connections::NodeConnection> = held.lock().unwrap().own_latest_directs().into_iter().filter(|c| c.state == rdm_core::connections::ConnectionState::Connected).cloned().collect();
    if connected.iter().any(|c| c.destination.node_id == *holder) {
        return rdm_transport::route::RouteChoice::Direct;
    }
    let mut peers: Vec<(rdm_core::PathName, NodeId)> = connected
        .into_iter()
        .filter(|c| c.destination.name.mesh == mesh && c.destination.node_id != *holder && c.destination.node_id != *me)
        .map(|c| (c.destination.name.clone(), c.destination.node_id.clone()))
        .collect();
    if peers.is_empty() {
        peers = topology_view
            .digest_view
            .current(topology_view.digest_view.staleness_floor())
            .into_iter()
            .filter(|d| d.node.name.mesh == mesh && d.node.node_id != *holder && d.node.node_id != *me)
            .map(|d| (d.node.name.clone(), d.node.node_id.clone()))
            .collect();
    }
    peers.sort();
    peers.dedup();
    match peers.get(attempt as usize % peers.len().max(1)) {
        Some((path, node_id)) => rdm_transport::route::RouteChoice::ViaPeer { carrier: node_id.clone(), path: path.clone() },
        None => rdm_transport::route::RouteChoice::Direct,
    }
}

/// The owed declaration (node-rpc.md §34, §43; i146 Q39, Q52): one `Budget::Owed` call to the holder of
/// this birth's mesh's primary seat, resolved from the seat this birth holds (its LeadershipView), never
/// to an admin it merely hears. The call goes direct first; once an attempt sends nothing, its resends
/// take the route [`declaration_route`] answers, a live peer of the mesh on Forward. It ends on the
/// holder's answer or on its proven exit, or when the seat moves ([`declare_until_seat_moves`] drops
/// it); no deadline or carrier failure ends it. A definitive answer clears the debt; a holder that is not
/// the authority, or a call that ended without an answer, leaves it owed until the seat moves. With no
/// holder held nothing is sent. Nothing here gates gossip: the digest already says the state.
async fn declare_once(me: &MeshNode, topology_view: &TopologyView, client: &rdm_transport::NodeRpcClient, owed: &Mutex<OwedDeclaration>, held: &Arc<Mutex<rdm_core::connections::ConnectionsHeld>>) -> Option<String> {
    use rdm_core::outcome::RpcOutcome;
    use rdm_core::status::{Status, StatusReply, StatusRequest};
    let Some((state, operation)) = owed.lock().unwrap().owed.clone() else { return None };
    let req = StatusRequest::DeclareNodeState { node_id: me.node_id.clone(), incarnation: me.incarnation.clone(), state, operation: operation.clone() };
    let node = topology_view.held_name(&me.name);
    let op_name = operation.as_ref().map(|o| o.operation.clone()).unwrap_or_default();
    let Some(holder) = topology_view.primaries().mesh(&me.name.mesh) else {
        tracing::info_span!("rdm.node_rpc.status.update.via-declare-own", node = %node, state = ?state, operation = %op_name, to = "", route = "none", outcome = "no-holder: this birth holds no primary seat for its mesh", definitive = false)
            .in_scope(|| tracing::info!("no primary seat is held for this birth's mesh: the declaration stays owed"));
        return Some("no-holder".into());
    };
    let to = topology_view.digest_view.current(topology_view.digest_view.staleness_floor()).iter().find(|d| d.node.node_id == holder.node_id).map(|d| d.node.name.to_string()).unwrap_or_else(|| holder.node_id.to_string());
    let policy = {
        let (held, topology_view, me_id, holder_id, mesh) = (held.clone(), topology_view.clone(), me.node_id.clone(), holder.node_id.clone(), me.name.mesh.clone());
        rdm_transport::RoutePolicy(Arc::new(move |attempt| declaration_route(&held, &topology_view, &me_id, &holder_id, &mesh, attempt)))
    };
    let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Owed, route: Some(policy), ..rdm_transport::CallOptions::default() };
    let (out, _) = client.call::<Status>(&rdm_transport::NodeTarget::ExactNode(holder.node_id.clone()), &req, &opts).await;
    let (outcome, definitive) = match &out {
        RpcOutcome::Reply(r) => match r.value() {
            StatusReply::RejectedNotPrimary { .. } => (format!("{:?}", r.value()), false),
            v => (v.name().to_string(), true),
        },
        other => (format!("{}: {other:?}", other.name()), false),
    };
    tracing::info_span!("rdm.node_rpc.status.update.via-declare-own", node = %node, state = ?state, operation = %op_name, to = %to, route = "owed", outcome = %outcome, definitive)
        .in_scope(|| tracing::info!("declared own state to the holder of the mesh's primary seat"));
    if definitive {
        let mut o = owed.lock().unwrap();
        if o.owed.as_ref() == Some(&(state, operation.clone())) {
            o.owed = None;
        }
    }
    Some(outcome)
}


/// What this node needs to answer a node-admin's status kick about itself.
pub(crate) struct Kicked {
    topology_view: TopologyView,
    digest: MeshDigest,
    status: Arc<Mutex<MemberStatus>>,
    /// This birth serving its node commands from its mesh-admin.
    own: Arc<crate::node_self::NodeSelf>,
    /// The down ops node-admins sent this birth: what a hook blocked on its authority's word wakes on.
    addressed: Arc<crate::node_hooks::Addressed>,
    /// Where this birth's `pre_node_ready` stands (the cell a start's hook writes): while it is
    /// Blocked, a probe is answered `Blocked` carrying its blocker.
    pre_node_ready: Arc<Mutex<Option<crate::node_hooks::PreNodeReadyHandle>>>,
    /// This node's server counters: its in-flight calls decide whether it may take a new name.
    stats: Arc<rdm_transport::ServerStats>,
}

type KickSlot = Arc<std::sync::OnceLock<Arc<Kicked>>>;

fn node_state_of(s: MemberStatus) -> rdm_core::status::NodeState {
    use rdm_core::status::NodeState as N;
    match s {
        MemberStatus::Pending => N::Pending,
        MemberStatus::ReadyForTraffic => N::ReadyForTraffic,
        MemberStatus::ConnectionsDeleting => N::ConnectionsDeleting,
        MemberStatus::Leaving => N::Stopped,
    }
}

/// The node command door (op 0x22, i144 R22; Luke Q6, Q7): a command from this birth's mesh-primary,
/// answered on the command's ordered reply stream by [`crate::node_self::NodeSelf::serve_node_command`].
/// Nothing checks who is calling (node-rpc.md §14): the caller, the birth holding the call's endpoint
/// key in the member projection this node installed, is named in spans and is the command's commander.
fn serve_node_command(b: ServerBuilder, slot: KickSlot) -> ServerBuilder {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
    b.serve_stream::<NodeCommand, _, _>(OpOwner::Product("rdm".into()), move |peer: rdm_transport::PeerContext, req: NodeCommandRequest, sink| {
        let slot = slot.clone();
        async move {
            let Some(me) = slot.get().cloned() else {
                return Ok(rdm_core::reply::node_command::not_ready("this node has not joined its mesh yet".into()));
            };
            let sender = me.topology_view.projected_member_at(&peer.endpoint_id.to_string());
            let sender_name = sender.as_ref().map(|d| d.node.name.to_string()).unwrap_or_else(|| format!("unknown peer, endpoint {}", peer.endpoint_id));
            me.addressed.note(&peer.endpoint_id);
            let commander = sender.as_ref().map(|d| d.node.node_id.clone());
            let reply = me.own.serve_node_command_on(commander.as_ref(), Some(peer.endpoint_id), &req, Box::new(crate::node_self::StreamFrames::new(sink))).await;
            tracing::info_span!("rdm.node_admin.status.update.via-command", node = %me.topology_view.held_name(&me.digest.node.name), op = req.op(), sender = %sender_name, outcome = reply.name(), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("a node command naming this node was decided"));
            Ok(reply)
        }
    })
}


/// Serve `Status` on an rpc node: the downward exact-node operations, answered once this node
/// has joined (until then `NotReady`). An rpc node is never an authority: every upward
/// declaration, every mesh or fabric operation and every view read (`node.get`, `mesh.get`,
/// `fabric.get`, which a node-admin answers from its view) is refused by name. A `NodeLeft`, never
/// sent, is refused as every receiver refuses it ([`crate::node_self::refuse_node_left`]).
/// `node.connections.get` of this exact birth is answered with the connections it holds.
///
/// The tickle: ask the exact birth to reassert itself. Protocol name: `ProbeNodeState`. The node
/// re-publishes its presence on the channel it is on (the check-in it owes, states.md "The check-in
/// checklist") and answers its current state; no transition: a birth its authority holds off
/// its mesh is commanded back through `node.mesh.join` (node-tickle.md, Q87), never by a probe. While its `pre_node_ready` hook is Blocked it answers `Blocked` with
/// the hook's blocker and republishes nothing. The reserved Status positions (`Reserved6`, `Reserved7`, `StopNode`, `StartNode`, `Reserved12` to `Reserved15`) go to
/// `crate::node_self`, which refuses them by name: node commands ride op 0x22. A stale incarnation is refused with the one held. Nothing
/// checks who is calling (node-rpc.md §14) or the node id a call names (R-RX5): every request is
/// decided on the op and the exact birth it names; the caller, the birth holding the call's endpoint key in the member
/// projection this node installed, is named in spans and is the authority a blocked hook was addressed by.
fn serve_kick(b: ServerBuilder, slot: KickSlot) -> ServerBuilder {
    use rdm_core::status::{Status, StatusReply, StatusRequest};
    b.serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |peer: rdm_transport::PeerContext, req: StatusRequest| {
        let slot = slot.clone();
        async move {
            let Some(me) = slot.get().cloned() else {
                return Ok(rdm_core::reply::status::not_ready("this node has not joined its mesh yet".into()));
            };
            // The hook's blocker as it stood when the call arrived, before this call addresses the
            // birth (a hook blocked on its authority's word runs again on it).
            let blocker = me.pre_node_ready.lock().unwrap().as_ref().and_then(|h| h.state().probe_blocker());
            // The sender is the birth that holds the call's endpoint key in the member projection this
            // node installed (gossip.md §3.1, the authoritative member record), never the heartbeat
            // view, which holds a member only once its own digest reached this node.
            let sender = me.topology_view.projected_member_at(&peer.endpoint_id.to_string());
            // A sender no projection this node holds names is named by the one fact it holds of it: its endpoint.
            let sender_name = sender.as_ref().map(|d| d.node.name.to_string()).unwrap_or_else(|| format!("unknown peer, endpoint {}", peer.endpoint_id));
            // Every call addressed this birth from its endpoint: a hook blocked on its authority's word
            // counts it, whether or not a record names the caller.
            me.addressed.note(&peer.endpoint_id);
            if matches!(req, StatusRequest::Reserved6 { .. } | StatusRequest::Reserved7 { .. } | StatusRequest::Reserved12 { .. } | StatusRequest::Reserved13 { .. } | StatusRequest::Reserved14 { .. } | StatusRequest::Reserved15 { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. } | StatusRequest::JoinMesh { .. } | StatusRequest::LeaveMesh { .. }) {
                let commander = sender.as_ref().map(|d| d.node.node_id.clone());
                let reply = me.own.serve(commander.as_ref(), Some(peer.endpoint_id), &req).await.expect("a command");
                tracing::info_span!("rdm.node_admin.status.update.via-command", node = %me.topology_view.held_name(&me.digest.node.name), op = req.op(), sender = %sender_name, outcome = reply.name(), "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a node command naming this node was decided"));
                return Ok(reply);
            }
            // node-left is never sent: refused by name as a node-admin refuses it, never sent on to one.
            if let StatusRequest::NodeLeft { node_id, .. } = &req {
                let name = me.topology_view.digest_view.all().into_iter().find(|d| d.node.node_id == *node_id).map(|d| d.node.name.to_string()).unwrap_or_default();
                return Ok(crate::node_self::refuse_node_left(&me.topology_view.held_name(&me.digest.node.name).to_string(), &name, &sender_name, &req));
            }
            let incarnation = match &req {
                StatusRequest::ProbeNodeState { incarnation, .. } | StatusRequest::ApplyNodeState { incarnation, .. } | StatusRequest::ApplyNodeConfig { incarnation, .. } | StatusRequest::ApplyNodeName { incarnation, .. } | StatusRequest::GetConnections { incarnation, .. } | StatusRequest::RenewCert { incarnation, .. } => incarnation,
                // No handler on this door for the request: not handled here (R24), decided by dispatch alone.
                _ => return Ok(crate::node_self::request_not_handled(&me.topology_view.held_name(&me.digest.node.name).to_string(), &req)),
            };
            // The birth the call reached answers it, whatever node id it names (R-RX5); the exact
            // birth it names is checked, so a call that reached another birth is refused naming the
            // one held.
            if *incarnation != me.digest.node.incarnation {
                return Ok(rdm_core::reply::stale(me.digest.node.incarnation.clone()));
            }
            match req {
                // node.connections.get: what this birth holds, under the name it holds; a read.
                StatusRequest::GetConnections { .. } => Ok(me.own.connections_get().await),
                // node.cert.renew: its issuing authority installs a fresh member cert on this birth.
                StatusRequest::RenewCert { cert, .. } => Ok(me.own.renew_cert(&cert)),
                StatusRequest::ProbeNodeState { .. } => {
                    // A stopped node is Leaving with a live process: it says so and presents nothing.
                    if me.own.is_stopped() {
                        return Ok(StatusReply::CurrentStopped { node_id: me.digest.node.node_id.clone(), incarnation: me.digest.node.incarnation.clone() });
                    }
                    // node-start.md step 3: while its hook is Blocked, the probe served this exact birth
                    // and found an active readiness blocker; it answers Blocked with the hook's own
                    // reason and retry event, before any republish, so a polled blocked birth
                    // republishes nothing.
                    if let Some((reason, retry_on)) = blocker {
                        let on = retry_on.name();
                        let reply = rdm_core::reply::status::bounded_blocked(me.digest.node.node_id.clone(), me.digest.node.incarnation.clone(), &reason, retry_on);
                        tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %me.topology_view.held_name(&me.digest.node.name), sender = %sender_name, state = "blocked", blocker = %reason, retry_on = on)
                            .in_scope(|| tracing::info!("probed by a node-admin: the hook's blocker answered Blocked, nothing re-published"));
                        return Ok(reply);
                    }
                    // The check-in a probed birth owes (states.md "The check-in checklist": a new primary
                    // re-sends the checklist and members already done check in again): its presence
                    // re-published on the channel it is on, its current state answered; no transition.
                    // A birth its authority holds off its mesh is commanded back through
                    // `node.mesh.join` (node-tickle.md, Q87), never by a probe.
                    let status = *me.status.lock().unwrap();
                    let mut d = me.digest.clone();
                    d.status = status;
                    let _ = me.topology_view.publish(&d).await;
                    tracing::info_span!(
                        "rdm.node_admin.status.update.via-probe",
                        node = %me.topology_view.held_name(&me.digest.node.name),
                        sender = %sender_name,
                        state = ?node_state_of(status),
                    )
                    .in_scope(|| tracing::info!("probed by a node-admin: presence re-published, current state answered"));
                    Ok(StatusReply::Current { node_id: me.digest.node.node_id.clone(), incarnation: me.digest.node.incarnation.clone(), state: node_state_of(status) })
                }
                StatusRequest::ApplyNodeState { state, .. } => {
                    // Not a node command: a node's connections are deleted only by `node.connections.delete` on op 0x22.
                    let current = node_state_of(*me.status.lock().unwrap());
                    tracing::info_span!("rdm.node_admin.status.reject.via-apply-node-state", node = %me.topology_view.held_name(&me.digest.node.name), sender = %sender_name, requested = ?state, current = ?current, reason = "node.connections.delete is the only path to connections-deleting and stop-node the only stop; apply-node-state is served for no state")
                        .in_scope(|| tracing::info!("apply-node-state refused"));
                    Ok(rdm_core::reply::status::rejected_invalid_node_transition(current))
                }
                StatusRequest::ApplyNodeConfig { config, .. } => {
                    // ops-naming "Node data": the node applies the config and carries it in its next
                    // digest. It is held first, so every later publish carries it; a live node
                    // publishes now, a stopped one presents nothing until it starts.
                    me.topology_view.set_config(config.clone());
                    let published = if me.own.is_stopped() {
                        "stopped"
                    } else {
                        let mut d = me.digest.clone();
                        d.status = *me.status.lock().unwrap();
                        match me.topology_view.publish(&d).await {
                            Ok(()) => "published",
                            Err(_) => "publish-failed",
                        }
                    };
                    tracing::info_span!("rdm.node_admin.node.update.via-apply-node-config", node = %me.topology_view.held_name(&me.digest.node.name), sender = %sender_name, keys = config.len(), digest = published, outcome = "applied", "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("the config its mesh-primary wrote is held and rides this birth's digest"));
                    Ok(rdm_core::reply::applied())
                }
                StatusRequest::ApplyNodeName { name, .. } => {
                    // R12: the node owns its name and takes the row's name only with no traffic, off its
                    // mesh (node-name-update.md Precondition). It is held first, so every later publish
                    // carries it; off its mesh it publishes nothing (node-mesh-leave.md), and its stop
                    // or its next join carries the name.
                    let held = me.topology_view.with_config(&me.digest).node.name;
                    let asked = match name.parse::<rdm_core::PathName>() {
                        Ok(n) => n,
                        Err(e) => {
                            tracing::info_span!("rdm.node_admin.node.reject.via-apply-node-name", node = %held, sender = %sender_name, to = %name, reason = %format!("the name {name:?} is not a path.name: {e}"), "otel.kind" = "internal")
                                .in_scope(|| tracing::info!("apply-node-name refused: the name does not parse"));
                            return Ok(rdm_core::reply::status::malformed(rdm_core::outcome::MalformedKind::Corrupt));
                        }
                    };
                    // R-CR1: the name moves only while the node has no connection open, read from
                    // what is open now, never from the rows that have landed (Q169).
                    let routes = match me.own.live_routes() {
                        Ok(r) => r,
                        Err(reason) => return Ok(rdm_core::reply::status::not_ready(reason)),
                    };
                    let decision = crate::node_name::decide(&held, &asked, &routes);
                    let published = match decision {
                        crate::node_name::NameApply::Applied => {
                            me.topology_view.set_name(asked.clone());
                            if *me.status.lock().unwrap() == MemberStatus::Leaving {
                                "off-mesh"
                            } else {
                                let mut d = me.digest.clone();
                                d.status = *me.status.lock().unwrap();
                                match me.topology_view.publish(&d).await {
                                    Ok(()) => "published",
                                    Err(_) => "publish-failed",
                                }
                            }
                        }
                        _ => "unchanged",
                    };
                    let held_routes = routes.iter().map(crate::node_name::route_name).collect::<Vec<_>>().join(", ");
                    tracing::info_span!("rdm.node_admin.node.update.via-apply-node-name", node = %held, sender = %sender_name, from = %held, to = %asked, connections = routes.len(), routes = %held_routes, digest = published, outcome = decision.name(), "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("the mesh-primary's name for this birth was decided"));
                    Ok(crate::node_name::reply(&decision))
                }
                _ => Ok(crate::node_self::request_not_handled(&me.topology_view.held_name(&me.digest.node.name).to_string(), &req)),
            }
        }
    })
}

#[cfg(test)]
mod authority_tests {
    use super::*;
    use rdm_core::Primary;

    fn primary(node: &NodeId, incarnation: &IncarnationId, revision: u64) -> Primary {
        Primary { mesh: "mesh1".into(), node_id: node.clone(), incarnation: incarnation.clone(), revision }
    }

    /// CONTRACT (R-S2): a birth owes its state again when its mesh's primary seat names another birth than the
    /// one it told: another node, or a new incarnation of the same node. A birth that told no one, or whose seat
    /// is unchanged or unheard, owes nothing new.
    #[test]
    fn a_birth_owes_its_state_again_only_when_its_meshs_mesh_primary_moved_to_another_birth() {
        let (a, b, inc_a, inc_b) = (NodeId::mint(), NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        let told = (a.clone(), inc_a.clone());
        assert!(!mesh_primary_moved(None, Some(&primary(&a, &inc_a, 1))), "nothing was told yet");
        assert!(!mesh_primary_moved(Some(&told), None), "the seat record is not heard: the seat is unknown, not moved");
        assert!(!mesh_primary_moved(Some(&told), Some(&primary(&a, &inc_a, 1))), "the same birth holds the seat");
        assert!(mesh_primary_moved(Some(&told), Some(&primary(&b, &inc_b, 2))), "another node holds the seat");
        assert!(mesh_primary_moved(Some(&told), Some(&primary(&a, &inc_b, 2))), "a new incarnation of the same node holds the seat");
    }
}
