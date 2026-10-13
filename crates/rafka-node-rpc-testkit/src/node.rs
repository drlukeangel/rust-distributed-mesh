//! The rpc node runtime: bind the assigned endpoint, serve Node RPC, join
//! fabric membership, publish the digest.

use rafka_mesh_entity::launch::Launch;
use anyhow::{anyhow, Context, Result};
use iroh::protocol::Router;
use iroh::{EndpointAddr, SecretKey};
use rafka_mesh_entity::{IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId};
use rafka_mesh_transport::membership::Membership;
use rafka_node_rpc::{NodeRpcServer, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
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
pub fn core_protocols(b: ServerBuilder, client: Arc<rafka_node_rpc::NodeRpcClient>, edges: Option<Arc<dyn rafka_node_rpc::CarrierEdges>>) -> ServerBuilder {
    b.serve_core(client, edges)
}

/// A node running on the imported substrate: its endpoint routers, membership, Node RPC server and
/// tasks.
pub struct RunningNode {
    /// Rafka-time: the app's way to read it. The one reader every gossip stamp this node publishes
    /// reads, already adopted when the node has started. Cloneable; every clone reads the same
    /// instance. `now_ms()` is `max(reference + monotonic elapsed, last returned)`; an app that
    /// stamps its records with rafka-time (the OrderKey's `writer_timestamp_ms`) reads it here and
    /// never from the OS clock.
    pub rafka_time: rafka_mesh_transport::clock::RafkaTime,
    /// The member cert the accepting authority's signer issued for exactly this birth, as the
    /// opaque bytes the join answer carried (`fabric-certs.md`). Empty when the authority is
    /// configured with no certs. RDM never parses it.
    pub member_cert: Vec<u8>,
    /// The endpoint's protocol routers.
    pub routers: Vec<Router>,
    /// The node's membership.
    pub membership: Membership,
    /// The node's Node RPC server.
    pub server: NodeRpcServer,
    /// The node's own status, as it publishes it.
    pub status: Arc<Mutex<MemberStatus>>,
    /// The node's current digest.
    pub digest: MeshDigest,
    /// This process's one Node RPC client and live resolver: every Node RPC
    /// caller in the process takes it by clone.
    pub node_rpc: crate::node_rpc::ProcessNodeRpc,
    /// The state this birth owes its authority (i143.e4.s11): re-declared every publish cadence
    /// until the authority answers by name (`declare_loop`).
    pub owed_state: Arc<Mutex<Option<rafka_node_rpc_contract::status::NodeState>>>,
    /// Cancelled when this birth is retired (drain-node, stop-node, its process stopping): the
    /// `hydrate_before_ready` hook and any pull it has in flight end.
    pub cancel: rafka_node_rpc::Retirement,
    /// The surface that obeys this birth's lifecycle and round commands
    /// (`rafka_node_admin_core::node_self`).
    pub own: Arc<rafka_node_admin_core::node_self::NodeSelf>,
    hydration: Arc<Mutex<Option<rafka_node_admin_core::app_hydration::HydrationHandle>>>,
    hydrating: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    publisher: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    node_rpc_feed: tokio::task::JoinHandle<()>,
    declare_loop: tokio::task::JoinHandle<()>,
}

/// The digest key carrying a node's in-flight handler count.

/// `RDM_DRAIN_DEADLINE_MS` (default 5000): how long a stopping node waits
/// for in-flight handlers. Strictly shorter than node-admin's stop grace.
/// Drain deadline plus the leave linger
/// ([`rafka_mesh_transport::membership::leave_linger_from_env`]) stay inside
/// node-admin's stop grace.
pub use rafka_mesh_transport::membership::leave_linger_from_env;

/// Whether this process is parked (stopped by `stop-node`, not started again): the shutdown that
/// follows has no drain leg, and only a live node drains itself on a signal.
pub fn stop_commanded() -> bool {
    rafka_node_admin_core::node_self::parking().is_parked()
}

/// The drain deadline from `RDM_DRAIN_DEADLINE_MS`, 5000 ms when unset.
pub fn drain_deadline_from_env() -> Duration {
    Duration::from_millis(std::env::var("RDM_DRAIN_DEADLINE_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(5000))
}

impl RunningNode {
    /// Where this birth's `hydrate_before_ready` stands; `None` when no hook was registered.
    pub fn hydration(&self) -> Option<rafka_node_admin_core::app_hydration::HydrationHandle> {
        self.hydration.lock().unwrap().clone()
    }

    /// Resolves with the reason once this node's hook failed after the node came up Pending; never
    /// resolves otherwise. A process selects on it beside its stop signal and ends by name.
    pub async fn hydration_failed(&self) -> String {
        let handle = self.hydration.lock().unwrap().clone();
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

    /// Declare this birth's `state` to its authority (i143.e4.s11): the node-admins of its mesh
    /// it hears, tried in path order until one answers by name (the mesh-primary applies; another
    /// admin answers `RejectedNotAuthority: receiver-not-primary` and the next is tried). A
    /// definitive answer ends the attempt; `NotSent`/`Indeterminate` leaves it to the next call
    /// site. Nothing here gates gossip: the digest already says the state.
    pub(crate) async fn declare_own(&self, state: rafka_node_rpc_contract::status::NodeState) -> Option<String> {
        *self.owed_state.lock().unwrap() = Some(state);
        declare_once(&self.digest.node, &self.membership, &self.node_rpc.client, &self.owed_state).await
    }

    /// Two-phase shutdown, phase one (node-rpc §35): new calls get a typed
    /// `Draining`, the digest says `Draining` with the in-flight count, and
    /// existing handlers may finish until `deadline`. Returns how many were
    /// still running at the deadline.
    pub async fn drain(&self, deadline: Duration) -> u64 {
        self.cancel.cancel();
        self.server.drain();
        *self.status.lock().unwrap() = MemberStatus::Draining;
        let _ = self.declare_own(rafka_node_rpc_contract::status::NodeState::Draining).await;
        let stats = self.server.stats();
        let until = tokio::time::Instant::now() + deadline;
        loop {
            let in_flight = rafka_node_rpc::ServerStats::get(&stats.in_flight);
            let mut d = self.digest.clone();
            d.status = MemberStatus::Draining;
            d.in_flight = Some(in_flight);
            let _ = self.membership.publish(&d).await;
            if in_flight == 0 || tokio::time::Instant::now() >= until {
                return in_flight;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The shutdown of a parked process (stopped by `stop-node`, not started again): its mesh
    /// connections were cut and `Leaving` declared when it stopped, so nothing is announced. The
    /// tasks end and the endpoints close once every reply it owes is settled.
    pub async fn stop_commanded(self) {
        crate::taskdump::watched("stop_commanded", self.stop_commanded_inner()).await
    }

    async fn stop_commanded_inner(self) {
        self.server.settled().await;
        self.cancel.cancel();
        self.abort_publisher();
        if let Some(h) = self.hydrating.lock().unwrap().take() {
            h.abort();
        }
        self.node_rpc_feed.abort();
        self.declare_loop.abort();
        for r in self.routers {
            let _ = r.shutdown().await;
        }
    }

    /// Publish `Leaving` and stop serving.
    /// Phase two: say `Leaving` on the fabric, keep saying it every
    /// `LEAVE_EVERY` for `linger`, then shut each router down: `Router::shutdown` shuts every
    /// protocol it serves down, gossip included, once, and then closes the endpoint. Nothing here
    /// shuts gossip down beside it: a second `Gossip::shutdown` meets an actor already stopped.
    ///
    /// iroh-gossip queues a broadcast and acknowledges nothing, and closing
    /// the endpoint drops whatever is still unsent; the linger keeps the
    /// node on the fabric long enough for its own `Leaving` to go out.
    pub async fn stop(self, linger: Duration) {
        crate::taskdump::watched("stop", self.stop_inner(linger)).await
    }

    async fn stop_inner(self, linger: Duration) {
        self.cancel.cancel();
        *self.status.lock().unwrap() = MemberStatus::Leaving;
        let _ = self.declare_own(rafka_node_rpc_contract::status::NodeState::Leaving).await;
        let (digest, membership) = (self.digest.clone(), self.membership.clone());
        rafka_mesh_transport::membership::announce_leaving(linger, rafka_mesh_transport::membership::LEAVE_EVERY, || {
            let mut d = digest.clone();
            d.status = MemberStatus::Leaving;
            let m = membership.clone();
            async move {
                let _ = m.publish(&d).await;
            }
        })
        .await;
        self.abort_publisher();
        self.node_rpc_feed.abort();
        self.declare_loop.abort();
        for r in self.routers {
            let _ = r.shutdown().await;
        }
    }
}

/// Hold the row's name and config a join answer handed (R-JN1, node-start.md step 2): every digest
/// `membership` publishes from now on carries them.
fn install_handed(membership: &Membership, control: &rafka_node_admin_core::wire::JoinControl) {
    if let Some(name) = &control.name {
        membership.set_name(name.clone());
    }
    if let Some(config) = &control.config {
        membership.set_config(config.clone());
    }
    tracing::info!(name = ?control.name.as_ref().map(|n| n.to_string()), config_keys = control.config.as_ref().map_or(0, |c| c.len()), "the row's name and config the join handed are held");
}

/// [`start_with_seams`] for an app that hydrates before Ready: `hydration` carries its
/// `hydrate_before_ready` hook (and the gate its own server-side ops sit behind).
pub async fn start_hydrating(launch: &Launch, hydration: rafka_node_admin_core::app_hydration::Hydration, register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder) -> Result<RunningNode> {
    start_with_rafka_time_hydrating(launch, rafka_mesh_transport::clock::RafkaTime::unadopted(), hydration, register).await
}

/// Bring the node up exactly as `launch` says.
pub async fn start(launch: &Launch, register: impl FnOnce(ServerBuilder, Arc<rafka_node_rpc::LiveNodeResolver>) -> ServerBuilder) -> Result<RunningNode> {
    start_with_client(launch, |b, resolver, _client| register(b, resolver)).await
}

/// The process's stop signal, latched from its first line (node-admin's own listener: one per process).
pub use rafka_node_admin_core::entry::{listen_for_stop, stop_requested};

/// Until this process is told to stop: a SIGTERM/ctrl-c, or the mesh transport stopping for
/// good (a runtime that can neither be heard nor answer ends; its exit is the death proof the
/// fabric recovers from). `binary` names the process in the exit line.
pub async fn wait_for_signal(binary: &str) {
    let stopped = async {
        let reason = rafka_mesh_transport::membership::until_transport_stopped().await;
        tracing::info_span!("rdm.mesh.node.delete.via-transport-stopped", reason = %reason)
            .in_scope(|| tracing::error!("the mesh transport stopped; this runtime exits"));
        eprintln!("{binary}: the mesh transport stopped: {reason}");
        rafka_mesh_telemetry::flush_before_exit();
        rafka_mesh_entity::runtime::exit_transport_stopped(&reason);
    };
    tokio::select! {
        () = stop_requested() => {}
        () = stopped => {}
    }
}

/// [`start`], handing `register` the process's one client too (a product family that calls out,
/// or carries for others, holds it before the server seals).
pub async fn start_with_client(launch: &Launch, register: impl FnOnce(ServerBuilder, Arc<rafka_node_rpc::LiveNodeResolver>, Arc<rafka_node_rpc::NodeRpcClient>) -> ServerBuilder) -> Result<RunningNode> {
    start_with_seams(launch, |b, s| register(b, s.resolver, s.client)).await
}

/// [`start_with_client`], handing `register` every process seam the testkit doors act through:
/// the resolver, the client, the connections writer and the storage fault.
pub async fn start_with_seams(launch: &Launch, register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder) -> Result<RunningNode> {
    // The Rafka-time this node composes: adopted from its authority's join answer, before its first
    // gossip frame. Every stamp this node puts on a frame, and the handle `RunningNode::rafka_time`
    // gives the app, read this one instance.
    let rafka_time = rafka_mesh_transport::clock::RafkaTime::unadopted();
    start_with_rafka_time(launch, rafka_time, register).await
}

async fn start_with_rafka_time(launch: &Launch, rafka_time: rafka_mesh_transport::clock::RafkaTime, register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder) -> Result<RunningNode> {
    start_with_rafka_time_hydrating(launch, rafka_time, rafka_node_admin_core::app_hydration::Hydration::none(), register).await
}

/// [`start_with_rafka_time`], running `hydration`'s hook between the mesh join and Ready.
async fn start_with_rafka_time_hydrating(launch: &Launch, rafka_time: rafka_mesh_transport::clock::RafkaTime, hydration: rafka_node_admin_core::app_hydration::Hydration, register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder) -> Result<RunningNode> {
    // One boot trace per birth: this root is never entered (iroh's tasks must not inherit it), each
    // step below is a child that lives exactly as long as the step, and `via-ready` closes it.
    let boot = tracing::info_span!(parent: None, "rdm.mesh.node.create.via-boot", node = %launch.name, node_id = %launch.node_id, incarnation_id = %launch.incarnation.as_str(), kind = launch.name.kind.name());
    let started = crate::taskdump::watched("start", start_booted(launch, rafka_time, hydration, register, &boot)).await;
    if let Err(e) = &started {
        boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
    }
    started
}

async fn start_booted(launch: &Launch, rafka_time: rafka_mesh_transport::clock::RafkaTime, hydration: rafka_node_admin_core::app_hydration::Hydration, register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder, boot: &tracing::Span) -> Result<RunningNode> {
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
    // membership is joined.
    let resolver = Arc::new(rafka_node_rpc::LiveNodeResolver::default());
    // This birth's exact runtime, as its provider recorded it: published with
    // the birth so any admin can manage it, whoever launched it.
    let dir = launch.data_dir.clone();
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.resolve.via-runtime-record", node = %launch.name);
    let runtime = rafka_node_admin_core::node_self::wait_for_own_record(&dir, Duration::from_secs(10)).await.map_err(|e| anyhow!("{e}"))?;
    drop(step);
    // The identity this process records its intentional exit under (a transport that stopped).
    rafka_mesh_entity::runtime::set_own_exit(rafka_mesh_entity::runtime::OwnExit {
        data_dir: launch.data_dir.clone(),
        deployment_id: runtime.deployment_id.clone(),
        incarnation: launch.incarnation.to_string(),
    });
    // One endpoint, one socket for the process: Node RPC and gossip share it by ALPN. A request
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-endpoint-bound", node = %launch.name, requested = %launch.bind_addr, bound = tracing::field::Empty);
    let ep0 = rafka_node_rpc::endpoint::bind(key.clone(), launch.bind_addr)
        .await
        .with_context(|| format!("the node cannot bind {}", launch.bind_addr))?;
    // The operating system assigned the port: the address the node reports is the one the bound
    // endpoint holds, never one anybody chose.
    let bound_addr = ep0.bound_sockets().into_iter().find(|a| a.is_ipv4()).ok_or_else(|| anyhow!("the endpoint bound at {} holds no IPv4 socket", launch.bind_addr))?;
    step.record("bound", tracing::field::display(bound_addr));
    drop(step);
    // The process's one client, made before the server seals: the server carries the proof
    // store for others through it (one direct inner call per forward, never a second hop).
    // This node's own connections: hydrated from `connections/<its path.name>/` of the fabric's
    // store root (R14), then kept by what its client
    // observes of its pooled connections (connections.md sections 4.2, 9 and 10).
    // The storage fault the originate door arms (testkit only): the product's file storage,
    // decorated.
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.resolve.via-connections-hydrated", node = %launch.name, writes_to = tracing::field::Empty);
    // Before the fabric's first state-commit the node holds its connections rows in its scratchpad;
    // its commit-state action publishes them (states.md, Rounds).
    let store_root = rafka_node_admin_core::store_root::StoreRoot::open(&launch.store_dir, &launch.fabric_id).map_err(|e| anyhow!("refusing to start: {e}"))?;
    step.record("writes_to", store_root.writes_to());
    let fault = Arc::new(crate::faults::StorageFault::default());
    let connections = Arc::new(rafka_node_admin_core::connections_writer::ConnectionsWriter::new(
        rafka_mesh_entity::connections::ConnectionEnd { name: launch.name.clone(), node_id: launch.node_id.clone(), incarnation: Some(launch.incarnation.clone()) },
        Arc::new(crate::faults::FaultedConnectionsStorage::new(
            Arc::new(
                rafka_node_admin_core::storage::FileConnectionsStorage::open(
                    &store_root,
                    &launch.name,
                    &launch.incarnation,
                )
                .await
                .map_err(|e| anyhow!("connections storage: {e}"))?,
            ),
            fault.clone(),
        )),
        Arc::new(std::sync::Mutex::new(rafka_mesh_entity::connections::ConnectionsHeld::new())),
    ));
    // Membership's current process births: how the held projection judges a Proxy's recorded
    // destination and carrier, and a Direct fact's destination (connections.md section 8).
    connections.held().lock().unwrap().set_membership(resolver.clone());
    connections.hydrate().await.map_err(|e| anyhow!("connections hydrate: {e}"))?;
    drop(step);
    // An owed Proxy retirement whose write was refused is attempted again while owed
    // (connections.md §10); the task ends with the process.
    let _retirements = connections.spawn_retirement_reconciler(rafka_node_admin_core::connections_writer::RETIREMENT_RETRY);
    // The control plane: Node RPC is no connection (connections.md "Two planes, two words"), so its
    // dials and accepts write nothing to the connections log.
    let client = Arc::new(rafka_node_rpc::NodeRpcClient::new(ep0.clone(), resolver.clone()).with_caller_system("rdm"));
    // The data plane of RDM standalone: the originate door's own client, whose dials are the
    // connections this node originates for traffic, each a Direct entry of its log.
    let data_plane = Arc::new(rafka_node_rpc::NodeRpcClient::new(ep0.clone(), resolver.clone()).with_caller_system("rdm").with_connection_observer(connections.clone()));
    // The status kick (fabric-node-lifecycle.md §7.3): answered once this node has joined.
    let subject: KickSlot = Arc::new(std::sync::OnceLock::new());
    let seams = crate::originate::Seams { resolver: resolver.clone(), client: data_plane.clone(), connections: connections.clone(), fault };
    let topology_slot: rafka_node_admin_core::topology_read::TopologySlot = Arc::new(std::sync::OnceLock::new());
    // Closed until this node has joined its mesh (the kick slot is filled): every op, ping
    // included, is a typed NotReady before then.
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-catalog-sealed", node = %launch.name);
    let server = serve_kick(register(rafka_node_admin_core::topology_read::serve(core_protocols(ServerBuilder::new(), client.clone(), Some(connections.clone())), topology_slot.clone()), seams), subject.clone())
        .carry::<crate::proof_store::ProofStore>()
        .carry::<rafka_node_rpc_contract::status::Status>()
        .with_ready_gate(ready.clone())
        .seal(rafka_node_rpc::ServedBirth { node_id: launch.node_id.to_string(), incarnation: launch.incarnation.to_string() })
        .map_err(|e| anyhow!("protocol catalog refused to seal: {e:?}"))?;
    drop(step);
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-gossip-started", node = %launch.name);
    let g = iroh_gossip::net::Gossip::builder().spawn(ep0.clone());
    drop(step);
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.add.via-alpn-registered", node = %launch.name, alpns = "node-rpc,gossip");
    let routers = vec![Router::builder(ep0.clone()).accept(rafka_node_rpc::ALPN, server.clone()).accept(iroh_gossip::ALPN, g.clone()).spawn()];
    drop(step);
    tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-accept-loop-started", node = %launch.name).in_scope(|| tracing::info!("the router accepts node-rpc and gossip connections"));
    let seeds: Vec<EndpointAddr> = launch
        .seeds
        .iter()
        .filter_map(|(k, a)| k.parse::<iroh::PublicKey>().ok().map(|pk| EndpointAddr::new(pk).with_ip_addr(*a)))
        .collect();
    let name = launch.name.to_string();
    // This birth's digest as it reports it: the address is the one the bound endpoint holds.
    let digest = MeshDigest {
        fabric_id: launch.fabric_id.clone(),
        node: MeshNode {
            node_id: launch.node_id.clone(),
            name: launch.name.clone(),
            endpoint_id: rafka_mesh_entity::EndpointId(key.public().to_string()),
            incarnation: launch.incarnation.clone(),
            supersedes: launch.supersedes.clone(),
            transport_addr: bound_addr,
            runtime: Some(runtime.clone()),
        },
        status: MemberStatus::ReadyForTraffic,
        node_state: Default::default(),
        admin_api_base: None,
        // Stamped by `Membership::publish`: this birth's own sequence and the composed clock.
        emitted_at_rafka_ms: 0,
        digest_seq: 0,
        mesh_id: None,
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
        data_dir: Some(launch.data_dir.display().to_string()),
    };
    // The join: this node's first call after it binds. `JoinNode` carries the digest above to the
    // admin that deployed it; the admin verifies it against the deployment and answers what it
    // holds. A node whose join is not accepted ends by name, whether the admin refused the digest
    // or could not take the join within the attempts: it is never ready for traffic.
    let launcher = launcher.clone();
    let resolve: Box<dyn FnOnce() -> Result<rafka_node_admin_core::workflows::node_start::JoinTo, rafka_node_admin_core::node_self::StartFailed> + Send> = {
        let (resolver, launch_name, launcher, seed) = (resolver.clone(), launch.name.clone(), launcher.clone(), seeds.first().cloned());
        Box::new(move || {
            use rafka_node_admin_core::node_self::StartFailed;
            let seed = seed.ok_or_else(|| StartFailed { step: "node.join", reason: format!("{} names launcher {} but no seed to reach it at", launch_name, launcher.name) })?;
            let launcher_ref = rafka_node_rpc::ResolvedNode {
                node_id: launcher.node_id.clone(),
                name: launcher.name.clone(),
                endpoint_id: seed.id,
                transport_addr: seed.ip_addrs().next().copied().ok_or_else(|| StartFailed { step: "node.join", reason: "the launching admin's seed names no address".into() })?,
                incarnation: launcher.incarnation.clone(),
            };
            resolver.apply(launcher_ref, None);
            Ok(rafka_node_admin_core::workflows::node_start::JoinTo::Launcher { target: rafka_node_rpc::NodeTarget::ExactNode(launcher.node_id.clone()), endpoint: seed.id, name: launcher.name.clone() })
        })
    };
    // What the start brought up, once it did: handed to the caller, or ended if the hook fails.
    let out: Arc<Mutex<Option<RunningNode>>> = Arc::default();
    let starting = rafka_node_admin_core::workflows::node_start::Starting { root: boot.clone(), name: name.clone(), path: launch.name.clone(), node_id: launch.node_id.clone(), client: client.clone(), rafka_time: rafka_time.clone() };
    let joining = rafka_node_admin_core::workflows::node_start::Joining::Authority { label: launcher.name.to_string() };
    let join_digest = digest.clone();
    let enter: rafka_node_admin_core::workflows::node_start::EnterSeam<anyhow::Error> = {
        let (launch, boot, out, name) = (launch.clone(), boot.clone(), out.clone(), name.clone());
        Box::new(move |step: tracing::Span, joined: Option<rafka_node_admin_core::wire::JoinAnswer>| -> rafka_node_admin_core::workflows::node_start::BoxFut<Result<(Membership, rafka_node_admin_core::workflows::node_start::JoinedSeam<anyhow::Error>), anyhow::Error>> {
            Box::pin(async move {
                let joined = joined.expect("a first start through its launcher enters its channel with the join answer");
                let member_cert = joined.control.member_cert.clone();
                // The mesh's id names its channel; the launching admin writes it.
                let mesh_id = launch.mesh_id.clone().ok_or_else(|| anyhow!("a node needs its mesh's id from its launch"))?;
                step.record("mesh_id", tracing::field::display(&mesh_id));
                step.record("seeds", seeds.len());

            let membership = Membership::join(&g, &ep0, &launch.fabric_id, &launch.name.mesh, &mesh_id, &name, Arc::new(rafka_time.clone()), seeds).await?;
            // The name this birth holds is its membership's (R12): its connections rows name it as their
            // source from here on, whatever name the join hands it or an `ApplyNodeName` gives it.
            {
                let (held, birth) = (membership.clone(), launch.name.clone());
                connections.follow_held_name(move || held.held_name(&birth)).map_err(|e| anyhow!(e))?;
            }
            // The row's name and config the join answer handed are held before this node publishes
            // anything or declares readiness: every digest from its first carries them (R-JN1).
            install_handed(&membership, &joined.control);
                drop(step);
            let (node_rpc, node_rpc_feed) = crate::node_rpc::ProcessNodeRpc::with_client(resolver.clone(), client.clone(), &membership.book, &name);
            // This node serves the topology it holds from here on (until now a read is `NotReady`).
            let status = Arc::new(Mutex::new(MemberStatus::Pending));
            {
                let (own, st, m) = (digest.clone(), status.clone(), membership.clone());
                let _ = topology_slot.set(Arc::new(rafka_node_admin_core::topology_read::TopologyDoor::new(
                    membership.clone(),
                    Arc::new(move || {
                        let mut d = own.clone();
                        d.status = *st.lock().unwrap();
                        // The config this birth was told rides its own digest here as in every publish.
                        m.with_config(&d)
                    }),
                    rafka_time.clone(),
                )));
            }
                let channel = membership.clone();
                let joined_seam: rafka_node_admin_core::workflows::node_start::JoinedSeam<anyhow::Error> = Box::new(
                    move |_: Option<rafka_node_admin_core::workflows::node_start::JoinThrough>| -> rafka_node_admin_core::workflows::node_start::BoxFut<Result<(rafka_node_admin_core::workflows::node_start::HookParts, Option<rafka_node_admin_core::workflows::node_start::HookStarted<anyhow::Error>>), anyhow::Error>> {
                        Box::pin(async move {
                            // A delta that does not follow what this node holds desynchronizes that source; the node tops
                            // up from its Mesh's own primary (gossip.md §3.3) with `GetTopology` of each such mesh.
                            membership.spawn_top_up(rafka_node_admin_core::topology_read::top_up_fetch(client.clone()));
                            // From here the node is on the mesh, Pending: it answers status, drain-node and stop-node, and
                            // says Pending on the membership cadence while its `hydrate_before_ready` hook runs. It is
                            // published ReadyForTraffic only once the hook has passed ("born full").
                            let cancel = rafka_node_rpc::Retirement::new();
                            let addressed = rafka_node_admin_core::app_hydration::Addressed::new();
                            let owed_state: Arc<Mutex<Option<rafka_node_rpc_contract::status::NodeState>>> = Arc::new(Mutex::new(None));
                            let own = {
                                let (set_digest, set_status_cell, now_status) = (digest.clone(), status.clone(), status.clone());
                                let set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync> = Arc::new(move |st| {
                                    *set_status_cell.lock().unwrap() = st;
                                    let mut d = set_digest.clone();
                                    d.status = st;
                                    d
                                });
                                let current: Arc<dyn Fn() -> MemberStatus + Send + Sync> = Arc::new(move || *now_status.lock().unwrap());
                                let declare: rafka_node_admin_core::node_self::Declare = {
                                    let (node, membership, client, owed) = (digest.node.clone(), membership.clone(), client.clone(), owed_state.clone());
                                    Arc::new(move |state| {
                                        *owed.lock().unwrap() = Some(state);
                                        let (node, membership, client, owed) = (node.clone(), membership.clone(), client.clone(), owed.clone());
                                        Box::pin(async move {
                                            let _ = declare_once(&node, &membership, &client, &owed).await;
                                        })
                                    })
                                };
                                Arc::new(
                                    rafka_node_admin_core::node_self::NodeSelf::new(launch.node_id.clone(), launch.incarnation.clone(), launch.name.clone(), server.clone(), client.clone(), node_rpc.resolver.clone(), membership.clone(), set_status, current, Some(declare), launch.connections_delete_deadline)
                                        .with_retire(cancel.clone())
                                        .with_round_actions(Arc::new(rafka_node_admin_core::fabric_rounds::StorePublication::new(store_root.scratchpad().cloned(), None))),
                                )
                            };
                            own.set_connections(connections.clone());
                            own.set_data_plane(data_plane.clone());
                            let publisher: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::default();
                            let hydration_cell: Arc<Mutex<Option<rafka_node_admin_core::app_hydration::HydrationHandle>>> = Arc::default();
                            let hydrating_cell: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::default();
                            own.set_session(Arc::new(NodeSession {
                                name: launch.name.clone(),
                                node_id: launch.node_id.clone(),
                                incarnation: launch.incarnation.clone(),
                                fabric_id: launch.fabric_id.clone(),
                                mesh_id: mesh_id.clone(),
                                runtime: runtime.clone(),
                                membership: membership.clone(),
                                client: client.clone(),
                                server: server.clone(),
                                digest: digest.clone(),
                                status: status.clone(),
                                owed: owed_state.clone(),
                                rafka_time: rafka_time.clone(),
                                hydration: hydration.clone(),
                                addressed: addressed.clone(),
                                publisher: publisher.clone(),
                                hydration_cell: hydration_cell.clone(),
                                hydrating: hydrating_cell.clone(),
                            }));
                            let _ = subject.set(Arc::new(Kicked { membership: membership.clone(), digest: digest.clone(), status: status.clone(), own: own.clone(), addressed: addressed.clone(), hydration: hydration_cell.clone(), stats: server.stats() }));
                            ready.store(true, std::sync::atomic::Ordering::SeqCst);
                            *publisher.lock().unwrap() = Some(spawn_publisher(&membership, &digest, &status, &server));
                            // The owed declaration loop: whatever state this birth owes is re-declared every publish
                            // cadence until an authority answers by name. An authority that does not yet hold this
                            // birth answers `subject-unknown`; the next cadence carries the digest and the retry lands.
                            // When the mesh's primary seat moves to another birth, this birth's state is owed to the new
                            // holder (R-S2: a declaration is owed again when the authority is another node or a new
                            // incarnation of the same node): that declaration is how the new primary learns this birth
                            // addresses it, and sends again a down op this birth refused while it still named the old holder.
                            let declare_loop = {
                                let (node, membership, client, owed, status) = (digest.node.clone(), membership.clone(), node_rpc.client.clone(), owed_state.clone(), status.clone());
                                tokio::spawn(async move {
                                    let mut seats = membership.seats().subscribe();
                                    let mut told: Option<(NodeId, IncarnationId)> = None;
                                    loop {
                                        seats.borrow_and_update();
                                        let holder = membership.seats().mesh(&node.name.mesh);
                                        if authority_moved(told.as_ref(), holder.as_ref()) && owed.lock().unwrap().is_none() {
                                            let state = node_state_of(*status.lock().unwrap());
                                            tracing::info_span!("rdm.node_rpc.status.update.via-authority-moved", node = %node.name, state = ?state, to = %holder.as_ref().map(|h| h.node_id.to_string()).unwrap_or_default())
                                                .in_scope(|| tracing::info!("the mesh's primary seat moved to another birth: this birth's state is owed to it"));
                                            *owed.lock().unwrap() = Some(state);
                                        }
                                        if owed.lock().unwrap().is_some() {
                                            declare_once(&node, &membership, &client, &owed).await;
                                        }
                                        if owed.lock().unwrap().is_none() {
                                            told = holder.map(|h| (h.node_id, h.incarnation));
                                        }
                                        tokio::select! {
                                            _ = seats.changed() => {}
                                            _ = tokio::time::sleep(rafka_mesh_transport::membership::gossip_interval()) => {}
                                        }
                                    }
                                })
                            };
                            let becoming = Becoming {
                                boot: boot.clone(),
                                name: name.clone(),
                                kind: launch.name.kind.name(),
                                incarnation_id: launch.incarnation.to_string(),
                                node_id: launch.node_id.clone(),
                                mesh_id: mesh_id.to_string(),
                                fabric_id: launch.fabric_id.to_string(),
                                runtime: runtime.clone(),
                                membership: membership.clone(),
                                digest: digest.clone(),
                                status: status.clone(),
                                owed: owed_state.clone(),
                                client: node_rpc.client.clone(),
                            };
                            let parts = rafka_node_admin_core::workflows::node_start::HookParts {
                                birth: rafka_node_admin_core::app_hydration::Birth {
                                    name: launch.name.clone(),
                                    node_id: launch.node_id.clone(),
                                    incarnation: launch.incarnation.clone(),
                                    fabric_id: launch.fabric_id.clone(),
                                    mesh_id: mesh_id.clone(),
                                    kind: launch.name.kind,
                                },
                                hydration: hydration.clone(),
                                events: rafka_node_admin_core::app_hydration::Events::new(membership.book.clone(), addressed),
                                cancel: cancel.token(),
                                state: hydration_cell.clone(),
                                runs: rafka_node_admin_core::workflows::node_start::HookRuns::Inline { blocked: hydrating_cell.clone() },
                                after: Box::new(RpcAfterHook {
                                    fabric_id: launch.fabric_id.clone(),
                                    node_id: launch.node_id.clone(),
                                    name: launch.name.clone(),
                                    becoming,
                                    // A step that fails ends the node by name from its start, as an unaccepted join does.
                                    ends: Some(out.clone()),
                                    knob: rafka_time_knob(&launch, &rafka_time, &boot),
                                }),
                            };
                            *out.lock().unwrap() = Some(RunningNode { rafka_time: rafka_time.clone(), member_cert, routers, membership, server, status, digest, node_rpc, owed_state, cancel, own, hydration: hydration_cell, hydrating: hydrating_cell, publisher, node_rpc_feed, declare_loop });
                            Ok::<_, anyhow::Error>((parts, None))
                        })
                    },
                );
                Ok::<_, anyhow::Error>((channel, joined_seam))
            })
        })
    };
    let mut start = rafka_node_admin_core::workflows::node_start::FirstStart {
        starting: Some(starting),
        joining: Some(joining),
        resolve: Some(resolve),
        digest: Some(join_digest),
        enter: Some(enter),
        joined: None,
        hook_started: None,
    };
    rafka_node_admin_core::workflows::node_start::node_start(&mut start, rafka_node_admin_core::app_hydration::StartKind::FirstStart).await?;
    let running = out.lock().unwrap().take();
    running.ok_or_else(|| anyhow!("{name}: the start ended without the node it brought up"))
}

/// The digest cadence of a node: its digest as it stands, its status and its in-flight count.
fn spawn_publisher(membership: &Membership, digest: &MeshDigest, status: &Arc<Mutex<MemberStatus>>, server: &NodeRpcServer) -> tokio::task::JoinHandle<()> {
    let (d, st, stats) = (digest.clone(), status.clone(), server.stats());
    membership.publish_every(rafka_mesh_transport::membership::gossip_interval(), move || {
        let mut d = d.clone();
        d.status = *st.lock().unwrap();
        d.in_flight = Some(rafka_node_rpc::ServerStats::get(&stats.in_flight));
        d
    })
}

/// How an rpc node or role node leaves its mesh when parked and joins it again when started
/// (`node_self::Session`): the parts it was born with, kept for the rejoin.
struct NodeSession {
    name: rafka_mesh_entity::PathName,
    node_id: NodeId,
    incarnation: IncarnationId,
    fabric_id: rafka_mesh_entity::FabricId,
    mesh_id: rafka_mesh_entity::MeshId,
    runtime: rafka_mesh_entity::runtime::RuntimeFact,
    membership: Membership,
    client: Arc<rafka_node_rpc::NodeRpcClient>,
    server: NodeRpcServer,
    digest: MeshDigest,
    status: Arc<Mutex<MemberStatus>>,
    owed: Arc<Mutex<Option<rafka_node_rpc_contract::status::NodeState>>>,
    rafka_time: rafka_mesh_transport::clock::RafkaTime,
    hydration: rafka_node_admin_core::app_hydration::Hydration,
    addressed: Arc<rafka_node_admin_core::app_hydration::Addressed>,
    publisher: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    hydration_cell: Arc<Mutex<Option<rafka_node_admin_core::app_hydration::HydrationHandle>>>,
    hydrating: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

#[async_trait::async_trait]
impl rafka_node_admin_core::node_self::Session for NodeSession {
    async fn cut(&self, spare: &[iroh::PublicKey]) -> rafka_node_admin_core::node_self::CutReport {
        if let Some(p) = self.publisher.lock().unwrap().take() {
            p.abort();
        }
        if let Some(h) = self.hydrating.lock().unwrap().take() {
            h.abort();
        }
        // A parked node serves no app op until a start has hydrated it again.
        self.hydration.gate().close();
        let neighbours = self.membership.neighbours();
        self.membership.park().await;
        let pooled = self.client.close_pooled("node.connections.delete");
        let accepted = self.server.close_connections_except(spare, "node.connections.delete");
        rafka_node_admin_core::node_self::CutReport { neighbours, pooled, accepted, turns_in_progress: 0 }
    }

    async fn rejoin(&self, retire: rafka_node_rpc::CancelToken, commander: tokio::sync::watch::Receiver<iroh::PublicKey>, rows: Option<rafka_node_admin_core::connections_writer::RowsHeld>) -> Result<(), rafka_node_admin_core::node_self::StartFailed> {
        // The digest this birth presents is the one it holds: a name it took live (ApplyNodeName)
        // rides it, never the birth's frozen name (R-JN1).
        let held = self.membership.with_config(&self.digest);
        let name = held.node.name.to_string();
        let root = tracing::info_span!(parent: None, "rdm.mesh.node.update.via-start", node = %name, node_id = %self.node_id, incarnation_id = %self.incarnation.as_str());
        let mut start = AfterStop { session: self, root, name, held, commander, retire: Some(retire), rows };
        rafka_node_admin_core::workflows::node_start::node_start(&mut start, rafka_node_admin_core::app_hydration::StartKind::AfterStop).await
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
    retire: Option<rafka_node_rpc::CancelToken>,
    /// The start's hold on this birth's connections rows, dropped once the handed name is held.
    rows: Option<rafka_node_admin_core::connections_writer::RowsHeld>,
}

#[async_trait::async_trait]
impl rafka_node_admin_core::workflows::node_start::StartNode for AfterStop<'_> {
    type Error = rafka_node_admin_core::node_self::StartFailed;

    fn starting(&self) -> rafka_node_admin_core::workflows::node_start::Starting {
        let s = self.session;
        rafka_node_admin_core::workflows::node_start::Starting { root: self.root.clone(), name: self.name.clone(), path: s.name.clone(), node_id: s.node_id.clone(), client: s.client.clone(), rafka_time: s.rafka_time.clone() }
    }

    fn joining(&self) -> rafka_node_admin_core::workflows::node_start::Joining {
        rafka_node_admin_core::workflows::node_start::Joining::Authority { label: self.commander.borrow().fmt_short().to_string() }
    }

    async fn resolve_authority(&mut self) -> Result<rafka_node_admin_core::workflows::node_start::JoinTo, rafka_node_admin_core::node_self::StartFailed> {
        Ok(rafka_node_admin_core::workflows::node_start::JoinTo::Commander(self.commander.clone()))
    }

    fn join_digest(&self) -> MeshDigest {
        self.held.clone()
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&rafka_node_admin_core::wire::JoinAnswer>) -> Result<Membership, rafka_node_admin_core::node_self::StartFailed> {
        let s = self.session;
        step.record("mesh_id", tracing::field::display(&s.mesh_id));
        // The row's name and config the join answer handed are held before the node re-enters its
        // topic or says anything: a node renamed while stopped publishes its row's name (R-JN1).
        if let Some(joined) = joined {
            install_handed(&s.membership, &joined.control);
        }
        // Only now are its connections recorded: the join's names the handed name.
        drop(self.rows.take());
        s.membership.unpark().await;
        drop(step);
        Ok(s.membership.clone())
    }

    async fn joined(
        &mut self,
        membership: &Membership,
        _: Option<&rafka_node_admin_core::workflows::node_start::JoinThrough>,
        _: Option<&rafka_node_admin_core::wire::JoinAnswer>,
    ) -> Result<rafka_node_admin_core::workflows::node_start::HookParts, rafka_node_admin_core::node_self::StartFailed> {
        let s = self.session;
        // From here on it is named by the name it installed from its join answer (`enter_channel`).
        let me = s.membership.held_name(&s.name);
        // Back on the topic means a neighbour: only then does this node say it is back, once, as
        // Pending, so every node that held it parked or restarting hears the rejoin.
        membership.wait_for_neighbour().await;
        let mut pending = s.digest.clone();
        pending.status = MemberStatus::Pending;
        let _ = membership.publish(&pending).await;
        *s.publisher.lock().unwrap() = Some(spawn_publisher(membership, &s.digest, &s.status, &s.server));
        let becoming = Becoming {
            boot: self.root.clone(),
            name: me.to_string(),
            kind: s.name.kind.name(),
            incarnation_id: s.incarnation.to_string(),
            node_id: s.node_id.clone(),
            mesh_id: s.mesh_id.to_string(),
            fabric_id: s.fabric_id.to_string(),
            runtime: s.runtime.clone(),
            membership: membership.clone(),
            digest: s.digest.clone(),
            status: s.status.clone(),
            owed: s.owed.clone(),
            client: s.client.clone(),
        };
        Ok(rafka_node_admin_core::workflows::node_start::HookParts {
            birth: rafka_node_admin_core::app_hydration::Birth { name: me.clone(), node_id: s.node_id.clone(), incarnation: s.incarnation.clone(), fabric_id: s.fabric_id.clone(), mesh_id: s.mesh_id.clone(), kind: s.name.kind },
            hydration: s.hydration.clone(),
            events: rafka_node_admin_core::app_hydration::Events::new(membership.book.clone(), s.addressed.clone()),
            cancel: self.retire.take().expect("the hook's token is handed once"),
            state: s.hydration_cell.clone(),
            runs: rafka_node_admin_core::workflows::node_start::HookRuns::Inline { blocked: s.hydrating.clone() },
            after: Box::new(RpcAfterHook { fabric_id: s.fabric_id.clone(), node_id: s.node_id.clone(), name: me, becoming, ends: None, knob: None }),
        })
    }
}

/// An rpc node's steps after its hook: it reaches the peers of its mesh the topology names, owes no
/// Build floor (it holds no Build log), and reports through [`Becoming`].
struct RpcAfterHook {
    fabric_id: rafka_mesh_entity::FabricId,
    /// The birth's node id: the topology's members it reaches are every other node of its mesh.
    node_id: NodeId,
    /// The name the birth holds: its join answer's at a start after a stop (R-JN1).
    name: rafka_mesh_entity::PathName,
    becoming: Becoming,
    /// What a step that fails inside the start ends: a first start's node (a start after a stop ends
    /// nothing; its process stays stopped's).
    ends: Option<Arc<Mutex<Option<RunningNode>>>>,
    /// A scenario's rafka-time knob, applied once rafka-time is refreshed, before the report.
    knob: Option<Box<dyn FnOnce() + Send>>,
}

#[async_trait::async_trait]
impl rafka_node_admin_core::workflows::node_start::AfterHook for RpcAfterHook {
    async fn topology_installed(&mut self, membership: &Membership, read: &rafka_node_admin_core::topology_read::TopologyRead) {
        let mesh_peers: Vec<EndpointAddr> = read
            .installed
            .iter()
            .flat_map(|m| m.members.iter())
            .filter(|d| d.fabric_id == self.fabric_id && d.node.node_id != self.node_id && d.node.name.mesh == self.name.mesh)
            .filter_map(rafka_mesh_transport::membership::gossip_addr)
            .collect();
        let _ = membership.join_peers(mesh_peers).await;
    }

    async fn report(self: Box<Self>) {
        let RpcAfterHook { becoming, knob, .. } = *self;
        if let Some(knob) = knob {
            knob();
        }
        becoming.go().await;
    }

    async fn start_failed(self: Box<Self>, _: &rafka_node_admin_core::node_self::StartFailed, _: bool) {
        let running = self.ends.and_then(|out| out.lock().unwrap().take());
        if let Some(running) = running {
            running.close().await;
        }
    }
}

/// A scenario's rafka-time knob for `launch` (the testkit-skew feature): this node's rafka-time put off
/// its authority's, once its start refreshed it.
#[cfg(feature = "testkit-skew")]
fn rafka_time_knob(launch: &Launch, rafka_time: &rafka_mesh_transport::clock::RafkaTime, boot: &tracing::Span) -> Option<Box<dyn FnOnce() + Send>> {
    let root = launch.data_dir.parent()?.to_path_buf();
    let (name, rafka_time, boot) = (launch.name.to_string(), rafka_time.clone(), boot.clone());
    Some(Box::new(move || {
        crate::apply_rafka_time_knob(&root, &name, &rafka_time, &boot);
    }))
}

/// Without the testkit-skew feature no scenario puts a node's rafka-time off.
#[cfg(not(feature = "testkit-skew"))]
fn rafka_time_knob(_: &Launch, _: &rafka_mesh_transport::clock::RafkaTime, _: &tracing::Span) -> Option<Box<dyn FnOnce() + Send>> {
    None
}

/// What turns a Pending node whose hook passed into a ReadyForTraffic one: `via-ready` closes the
/// boot trace, the status moves, the digest is published at once and the owed declaration is sent.
struct Becoming {
    boot: tracing::Span,
    name: String,
    kind: &'static str,
    incarnation_id: String,
    node_id: rafka_mesh_entity::NodeId,
    mesh_id: String,
    fabric_id: String,
    runtime: rafka_mesh_entity::runtime::RuntimeFact,
    membership: Membership,
    digest: MeshDigest,
    status: Arc<Mutex<MemberStatus>>,
    owed: Arc<Mutex<Option<rafka_node_rpc_contract::status::NodeState>>>,
    client: Arc<rafka_node_rpc::NodeRpcClient>,
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
            meshes = self.membership.meshes_held(),
            node_id = %self.node_id,
            mesh_id = %self.mesh_id,
            fabric_id = %self.fabric_id,
            id_format = rafka_mesh_entity::ID_FORMAT,
            deployment_id = %self.runtime.deployment_id,
            provider = self.runtime.provider.as_str(),
            provider_control_domain_fingerprint = %self.runtime.domain_fingerprint(),
            runtime_locator_kind = self.runtime.locator.kind(),
            runtime_locator_fingerprint = %self.runtime.locator_fingerprint(),
            source = "self-published-membership"
        )
        .in_scope(|| tracing::info!("ready for traffic"));
        *self.status.lock().unwrap() = MemberStatus::ReadyForTraffic;
        *self.owed.lock().unwrap() = Some(rafka_node_rpc_contract::status::NodeState::ReadyForTraffic);
        let mut d = self.digest.clone();
        d.status = MemberStatus::ReadyForTraffic;
        let _ = self.membership.publish(&d).await;
        let (node, membership, client, owed) = (self.digest.node.clone(), self.membership.clone(), self.client.clone(), self.owed.clone());
        tokio::spawn(async move {
            declare_once(&node, &membership, &client, &owed).await;
        });
    }
}

/// Whether the primary seat of this birth's mesh moved to another birth than the one this birth
/// last told its state: the holder it told is `told`, the holder its seat record names is `holder`.
/// A birth that told no holder yet has nothing to repeat.
fn authority_moved(told: Option<&(NodeId, IncarnationId)>, holder: Option<&rafka_mesh_entity::SeatHolder>) -> bool {
    match (told, holder) {
        (Some((node, incarnation)), Some(h)) => !h.is_birth(node, incarnation),
        _ => false,
    }
}

/// One attempt at the owed declaration: the node-admins of this birth's mesh it hears, in path
/// order, until one answers by name. A definitive answer clears the debt; `RejectedNotAuthority`
/// (the admin is not the primary, or does not hold this birth yet) and `NotSent`/`Indeterminate`
/// leave it owed for the next cadence. Nothing here gates gossip: the digest already says the state.
async fn declare_once(me: &MeshNode, membership: &Membership, client: &rafka_node_rpc::NodeRpcClient, owed: &Mutex<Option<rafka_node_rpc_contract::status::NodeState>>) -> Option<String> {
    use rafka_node_rpc_contract::outcome::RpcOutcome;
    use rafka_node_rpc_contract::status::{Status, StatusReply, StatusRequest};
    let Some(state) = *owed.lock().unwrap() else { return None };
    let req = StatusRequest::DeclareNodeState { node_id: me.node_id.clone(), incarnation: me.incarnation.clone(), state };
    let mut admins: Vec<MeshDigest> = membership
        .book
        .current(membership.book.staleness_floor())
        .into_iter()
        .filter(|d| d.node.name.kind == rafka_mesh_entity::NodeKind::NodeAdmin && d.node.name.mesh == me.name.mesh)
        .collect();
    admins.sort_by_key(|d| d.node.name.to_string());
    let mut last = None;
    for a in &admins {
        let (out, _) = client.call::<Status>(&rafka_node_rpc::NodeTarget::ExactNode(a.node.node_id.clone()), &req, &rafka_node_rpc::CallOptions::default()).await;
        let (outcome, definitive) = match &out {
            RpcOutcome::Reply(r) => match r.value() {
                StatusReply::RejectedNotAuthority { .. } => (format!("{:?}", r.value()), false),
                v => (v.name().to_string(), true),
            },
            other => (format!("{}: {other:?}", other.name()), false),
        };
        tracing::info_span!("rdm.node_rpc.status.update.via-declare-own", node = %membership.held_name(&me.name), state = ?state, to = %a.node.name, outcome = %outcome, definitive)
            .in_scope(|| tracing::info!("declared own state to an admin of the mesh"));
        last = Some(outcome.clone());
        if definitive {
            if *owed.lock().unwrap() == Some(state) {
                *owed.lock().unwrap() = None;
            }
            return Some(outcome);
        }
    }
    last
}


/// What this node needs to answer a node-admin's status kick about itself.
pub(crate) struct Kicked {
    membership: Membership,
    digest: MeshDigest,
    status: Arc<Mutex<MemberStatus>>,
    /// This birth obeying `drain-node` / `stop-node` from its mesh-admin.
    own: Arc<rafka_node_admin_core::node_self::NodeSelf>,
    /// The down ops node-admins sent this birth: what a hook blocked on its authority's word wakes on.
    addressed: Arc<rafka_node_admin_core::app_hydration::Addressed>,
    /// Where this birth's `hydrate_before_ready` stands (the cell a start's hook writes): while it is
    /// Blocked, a probe is answered `Blocked` carrying its blocker.
    hydration: Arc<Mutex<Option<rafka_node_admin_core::app_hydration::HydrationHandle>>>,
    /// This node's server counters: its in-flight calls decide whether it may take a new name.
    stats: Arc<rafka_node_rpc::ServerStats>,
}

type KickSlot = Arc<std::sync::OnceLock<Arc<Kicked>>>;

fn node_state_of(s: MemberStatus) -> rafka_node_rpc_contract::status::NodeState {
    use rafka_node_rpc_contract::status::NodeState as N;
    match s {
        MemberStatus::Pending => N::Pending,
        MemberStatus::ReadyForTraffic => N::ReadyForTraffic,
        MemberStatus::Draining => N::Draining,
        MemberStatus::Leaving => N::Leaving,
    }
}

/// Serve `Status` on an rpc node: the downward exact-node operations, answered once this node
/// has joined (until then `NotReady`). An rpc node is never an authority: every upward
/// declaration, every mesh or fabric operation and every view read (`node.get`, `mesh.get`,
/// `fabric.get`, which a node-admin answers from its view) is refused by name. A `NodeLeft`, never
/// sent, is refused as every receiver refuses it ([`rafka_node_admin_core::node_commands::refuse_node_left`]).
/// `node.connections.get` of this exact birth is answered with the connections it holds.
///
/// The tickle: ask the exact birth to reassert itself. Protocol name: `ProbeNodeState`. The node
/// hands its peers to its mesh channel again, publishes its digest, and answers its current
/// state; no transition. While its `hydrate_before_ready` hook is Blocked it answers `Blocked` with
/// the hook's blocker and republishes nothing. `DrainNode`, `StopNode` and `StartNode` are obeyed by
/// `rafka_node_admin_core::node_self`. A stale incarnation is refused with the one held. Nothing
/// checks who is calling (node-rpc.md §14) or the node id a call names (R-RX5): every request is
/// decided on the op and the exact birth it names; the caller, the birth holding the call's endpoint key in the member
/// projection this node installed, is named in spans and is the authority a blocked hook was addressed by.
fn serve_kick(b: ServerBuilder, slot: KickSlot) -> ServerBuilder {
    use rafka_node_rpc_contract::status::{NotAuthority, Status, StatusReply, StatusRequest};
    b.serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |peer: rafka_node_rpc::PeerContext, req: StatusRequest| {
        let slot = slot.clone();
        async move {
            let Some(me) = slot.get().cloned() else {
                return Ok(StatusReply::NotReady { reason: "this node has not joined its mesh yet".into() });
            };
            // The hook's blocker as it stood when the call arrived, before this call addresses the
            // birth (a hook blocked on its authority's word runs again on it).
            let blocker = me.hydration.lock().unwrap().as_ref().and_then(|h| h.state().probe_blocker());
            // The sender is the birth that holds the call's endpoint key in the member projection this
            // node installed (gossip.md §3.1, the authoritative member record), never the heartbeat
            // book, which holds a member only once its own digest reached this node.
            let sender = me.membership.projected_member_at(&peer.endpoint_id.to_string());
            // A sender no projection this node holds names is named by the one fact it holds of it: its endpoint.
            let sender_name = sender.as_ref().map(|d| d.node.name.to_string()).unwrap_or_else(|| format!("unknown peer, endpoint {}", peer.endpoint_id));
            // Every call addressed this birth from its endpoint: a hook blocked on its authority's word
            // counts it, whether or not a record names the caller.
            me.addressed.note(&peer.endpoint_id);
            if matches!(req, StatusRequest::DrainNode { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. } | StatusRequest::DeleteConnections { .. }) {
                let commander = sender.as_ref().map(|d| d.node.node_id.clone());
                let reply = me.own.serve(commander.as_ref(), Some(peer.endpoint_id), &req).await.expect("a command");
                tracing::info_span!("rdm.node_admin.status.update.via-command", node = %me.membership.held_name(&me.digest.node.name), op = req.op(), sender = %sender_name, outcome = reply.name(), "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a node command naming this node was decided"));
                return Ok(reply);
            }
            if matches!(req, StatusRequest::CommitState { .. } | StatusRequest::OpenTraffic { .. }) {
                // A member checks in with the primary that commanded it (fabric-state-commit.md, round
                // sequence: "M->>N: commit-state", "N->>M: state-committed"): the caller, which a new
                // primary is before this node hears it take the seat. Only a caller no projection this
                // node holds names is checked in at the node-admin its seat book names.
                let mesh = me.digest.node.name.mesh.clone();
                let commander = sender.as_ref().map(|d| d.node.node_id.clone()).or_else(|| me.membership.seats().mesh(&mesh).map(|held| held.node_id));
                let reply = match commander {
                    None => StatusReply::NotReady { reason: format!("{} names no caller at endpoint {} and has not yet heard who holds the primary seat of {mesh}", me.digest.node.name, peer.endpoint_id) },
                    Some(commander) => me.own.serve(Some(&commander), None, &req).await.expect("a round op"),
                };
                tracing::info_span!("rdm.node_admin.status.update.via-round-op", node = %me.membership.held_name(&me.digest.node.name), op = req.op(), sender = %sender_name, outcome = reply.name(), "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a round command naming this node was decided"));
                return Ok(reply);
            }
            // node-left is never sent: refused by name as a node-admin refuses it, never sent on to one.
            if let StatusRequest::NodeLeft { node_id, .. } = &req {
                let name = me.membership.book.all().into_iter().find(|d| d.node.node_id == *node_id).map(|d| d.node.name.to_string()).unwrap_or_default();
                return Ok(rafka_node_admin_core::node_commands::refuse_node_left(&me.membership.held_name(&me.digest.node.name).to_string(), &name, &sender_name, &req));
            }
            let incarnation = match &req {
                StatusRequest::ProbeNodeState { incarnation, .. } | StatusRequest::ApplyNodeState { incarnation, .. } | StatusRequest::ApplyNodeConfig { incarnation, .. } | StatusRequest::ApplyNodeName { incarnation, .. } | StatusRequest::GetConnections { incarnation, .. } => incarnation,
                _ => return Ok(StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a node-admin".into() } }),
            };
            // The birth the call reached answers it, whatever node id it names (R-RX5); the exact
            // birth it names is checked, so a call that reached another birth is refused naming the
            // one held.
            if *incarnation != me.digest.node.incarnation {
                return Ok(StatusReply::RejectedStaleIncarnation { held: me.digest.node.incarnation.clone() });
            }
            match req {
                // node.connections.get: what this birth holds, under the name it holds; a read.
                StatusRequest::GetConnections { .. } => Ok(me.own.connections_get().await),
                StatusRequest::ProbeNodeState { .. } => {
                    // A parked node is Leaving with a live process: it says so and presents nothing.
                    if me.own.is_parked() {
                        return Ok(StatusReply::CurrentParked { node_id: me.digest.node.node_id.clone(), incarnation: me.digest.node.incarnation.clone() });
                    }
                    // node-start.md step 3: while its hook is Blocked, the probe served this exact birth
                    // and found an active readiness blocker; it answers Blocked with the hook's own
                    // reason and retry event, before any republish, so a polled blocked birth
                    // republishes nothing.
                    if let Some((reason, retry_on)) = blocker {
                        let on = retry_on.name();
                        let reply = StatusReply::blocked(me.digest.node.node_id.clone(), me.digest.node.incarnation.clone(), &reason, retry_on);
                        tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %me.digest.node.name, sender = %sender_name, state = "blocked", blocker = %reason, retry_on = on)
                            .in_scope(|| tracing::info!("probed by a node-admin: the hook's blocker answered Blocked, nothing re-published"));
                        return Ok(reply);
                    }
                    let peers: Vec<EndpointAddr> = me.membership.book.all().iter().filter_map(rafka_mesh_transport::membership::gossip_addr).collect();
                    let _ = me.membership.join_peers(peers).await;
                    let status = *me.status.lock().unwrap();
                    let mut d = me.digest.clone();
                    d.status = status;
                    let _ = me.membership.publish(&d).await;
                    tracing::info_span!(
                        "rdm.node_admin.status.update.via-probe",
                        node = %me.membership.held_name(&me.digest.node.name),
                        sender = %sender_name,
                        state = ?node_state_of(status),
                    )
                    .in_scope(|| tracing::info!("probed by a node-admin: presence re-published, current state answered"));
                    Ok(StatusReply::Current { node_id: me.digest.node.node_id.clone(), incarnation: me.digest.node.incarnation.clone(), state: node_state_of(status) })
                }
                StatusRequest::ApplyNodeState { state, .. } => {
                    // Not a drain command and not a stop: the only drain path is drain-node.
                    let current = node_state_of(*me.status.lock().unwrap());
                    tracing::info_span!("rdm.node_admin.status.reject.via-apply-node-state", node = %me.membership.held_name(&me.digest.node.name), sender = %sender_name, requested = ?state, current = ?current, reason = "drain-node is the only drain path and stop-node the only stop; apply-node-state is served for no state")
                        .in_scope(|| tracing::info!("apply-node-state refused"));
                    Ok(StatusReply::RejectedInvalidNodeTransition { current })
                }
                StatusRequest::ApplyNodeConfig { config, .. } => {
                    // ops-naming "Node data": the node applies the config and carries it in its next
                    // digest. It is held first, so every later publish carries it; a live node
                    // publishes now, a parked one presents nothing until it starts.
                    me.membership.set_config(config.clone());
                    let published = if me.own.is_parked() {
                        "parked"
                    } else {
                        let mut d = me.digest.clone();
                        d.status = *me.status.lock().unwrap();
                        match me.membership.publish(&d).await {
                            Ok(()) => "published",
                            Err(_) => "publish-failed",
                        }
                    };
                    tracing::info_span!("rdm.node_admin.node.update.via-apply-node-config", node = %me.membership.held_name(&me.digest.node.name), sender = %sender_name, keys = config.len(), digest = published, outcome = "applied", "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("the config its mesh-primary wrote is held and rides this birth's digest"));
                    Ok(StatusReply::Applied)
                }
                StatusRequest::ApplyNodeName { name, .. } => {
                    // R12: the node owns its name and takes the row's name only with no traffic. It is
                    // held first, so every later publish carries it; a live node publishes now, a
                    // stopped one presents nothing until it starts.
                    let held = me.membership.with_config(&me.digest).node.name;
                    let asked = match name.parse::<rafka_mesh_entity::PathName>() {
                        Ok(n) => n,
                        Err(e) => {
                            tracing::info_span!("rdm.node_admin.node.reject.via-apply-node-name", node = %held, sender = %sender_name, to = %name, reason = %format!("the name {name:?} is not a path.name: {e}"), "otel.kind" = "internal")
                                .in_scope(|| tracing::info!("apply-node-name refused: the name does not parse"));
                            return Ok(StatusReply::Malformed { kind: rafka_node_rpc_contract::outcome::MalformedKind::Corrupt });
                        }
                    };
                    // R-CR1: the name moves only while the node holds no connection, read from its
                    // connections writer once every observation has landed.
                    let routes = match me.own.held_routes().await {
                        Ok(r) => r,
                        Err(reason) => return Ok(StatusReply::NotReady { reason }),
                    };
                    let decision = rafka_node_admin_core::node_name::decide(&held, &asked, &routes);
                    let published = match decision {
                        rafka_node_admin_core::node_name::NameApply::Applied => {
                            me.membership.set_name(asked.clone());
                            if me.own.is_parked() {
                                "stopped"
                            } else {
                                let mut d = me.digest.clone();
                                d.status = *me.status.lock().unwrap();
                                match me.membership.publish(&d).await {
                                    Ok(()) => "published",
                                    Err(_) => "publish-failed",
                                }
                            }
                        }
                        _ => "unchanged",
                    };
                    // The apply resolved: the admission block `All` set lifts, under the name now held.
                    if matches!(decision, rafka_node_admin_core::node_name::NameApply::Applied | rafka_node_admin_core::node_name::NameApply::AlreadyApplied) {
                        me.own.name_apply_resolved();
                    }
                    let held_routes = routes.iter().map(rafka_node_admin_core::node_name::route_name).collect::<Vec<_>>().join(", ");
                    tracing::info_span!("rdm.node_admin.node.update.via-apply-node-name", node = %held, sender = %sender_name, from = %held, to = %asked, connections = routes.len(), routes = %held_routes, digest = published, outcome = decision.name(), "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("the mesh-primary's name for this birth was decided"));
                    Ok(rafka_node_admin_core::node_name::reply(&decision))
                }
                _ => Ok(StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a node-admin".into() } }),
            }
        }
    })
}

#[cfg(test)]
mod authority_tests {
    use super::*;
    use rafka_mesh_entity::SeatHolder;

    fn holder(node: &NodeId, incarnation: &IncarnationId, epoch: u64) -> SeatHolder {
        SeatHolder { mesh: "mesh1".into(), node_id: node.clone(), incarnation: incarnation.clone(), epoch }
    }

    /// CONTRACT (R-S2): a birth owes its state again when its mesh's primary seat names another birth than the
    /// one it told: another node, or a new incarnation of the same node. A birth that told no one, or whose seat
    /// is unchanged or unheard, owes nothing new.
    #[test]
    fn a_birth_owes_its_state_again_only_when_its_meshs_primary_seat_moved_to_another_birth() {
        let (a, b, inc_a, inc_b) = (NodeId::mint(), NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        let told = (a.clone(), inc_a.clone());
        assert!(!authority_moved(None, Some(&holder(&a, &inc_a, 1))), "nothing was told yet");
        assert!(!authority_moved(Some(&told), None), "the seat record is not heard: the seat is unknown, not moved");
        assert!(!authority_moved(Some(&told), Some(&holder(&a, &inc_a, 1))), "the same birth holds the seat");
        assert!(authority_moved(Some(&told), Some(&holder(&b, &inc_b, 2))), "another node holds the seat");
        assert!(authority_moved(Some(&told), Some(&holder(&a, &inc_b, 2))), "a new incarnation of the same node holds the seat");
    }
}
