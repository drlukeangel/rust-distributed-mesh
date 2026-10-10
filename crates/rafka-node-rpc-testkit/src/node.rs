//! The rpc node runtime: bind the assigned endpoint, serve Node RPC, join
//! fabric membership, publish the digest.

use rafka_mesh_entity::launch::Launch;
use anyhow::{anyhow, Context, Result};
use iroh::protocol::Router;
use iroh::{EndpointAddr, SecretKey};
use rafka_mesh_entity::{MemberStatus, MeshDigest, MeshNode};
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
    gossip: iroh_gossip::net::Gossip,
    publisher: tokio::task::JoinHandle<()>,
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

/// Whether this process was commanded to stop (`stop-node`): the shutdown that follows has no
/// drain leg, and only a signal drains the node itself.
pub fn stop_commanded() -> bool {
    rafka_node_admin_core::node_self::stop_command().commanded()
}

/// The drain deadline from `RDM_DRAIN_DEADLINE_MS`, 5000 ms when unset.
pub fn drain_deadline_from_env() -> Duration {
    Duration::from_millis(std::env::var("RDM_DRAIN_DEADLINE_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(5000))
}

impl RunningNode {
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

    /// The shutdown that follows an admitted `stop-node` (node-stop.md): the node already entered
    /// `Leaving`, published it and called `node-left` while its endpoint was open
    /// (`node_self`), so nothing is announced again. The tasks end and the endpoints close.
    pub async fn stop_commanded(self) {
        self.publisher.abort();
        self.node_rpc_feed.abort();
        self.declare_loop.abort();
        let _ = self.gossip.shutdown().await;
        for r in self.routers {
            let _ = r.shutdown().await;
        }
    }

    /// Publish `Leaving` and stop serving.
    /// Phase two: say `Leaving` on the fabric, keep saying it every
    /// `LEAVE_EVERY` for `linger`, then leave gossip and close every endpoint.
    ///
    /// iroh-gossip queues a broadcast and acknowledges nothing, and closing
    /// the endpoint drops whatever is still unsent; the linger keeps the
    /// node on the fabric long enough for its own `Leaving` to go out.
    pub async fn stop(self, linger: Duration) {
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
        self.publisher.abort();
        self.node_rpc_feed.abort();
        self.declare_loop.abort();
        let _ = self.gossip.shutdown().await;
        for r in self.routers {
            let _ = r.shutdown().await;
        }
    }
}

/// Bring the node up exactly as `launch` says.
pub async fn start(launch: &Launch, register: impl FnOnce(ServerBuilder, Arc<rafka_node_rpc::LiveNodeResolver>) -> ServerBuilder) -> Result<RunningNode> {
    start_with_client(launch, |b, resolver, _client| register(b, resolver)).await
}

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
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
            () = rafka_node_admin_core::node_self::stop_command().wait() => {}
            () = stopped => {}
        }
    }
    #[cfg(not(unix))]
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = rafka_node_admin_core::node_self::stop_command().wait() => {}
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
    start_with_clock(launch, rafka_mesh_transport::clock::os_clock(), register).await
}

/// [`start_with_seams`], stamping every gossip frame this node publishes with `clock`: the
/// Rafka-time the executable composes (an RDM executable supplies the OS clock).
pub(crate) async fn start_with_clock(launch: &Launch, clock: rafka_mesh_transport::clock::SharedClock, register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder) -> Result<RunningNode> {
    // One boot trace per birth: this root is never entered (iroh's tasks must not inherit it), each
    // step below is a child that lives exactly as long as the step, and `via-ready` closes it.
    let boot = tracing::info_span!(parent: None, "rdm.mesh.node.create.via-boot", node = %launch.name, node_id = %launch.node_id, incarnation_id = %launch.incarnation.0, kind = launch.name.kind.name());
    let started = start_booted(launch, clock, register, &boot).await;
    if let Err(e) = &started {
        boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
    }
    started
}

async fn start_booted(launch: &Launch, clock: rafka_mesh_transport::clock::SharedClock, register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder, boot: &tracing::Span) -> Result<RunningNode> {
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
    let runtime = tokio::task::spawn_blocking(move || rafka_mesh_entity::runtime::await_own_record(&dir, Duration::from_secs(10)))
        .await
        .map_err(|e| anyhow!("reading the runtime record: {e}"))?
        .map_err(|e| anyhow!("{e}"))?;
    drop(step);
    // The identity this process records its intentional exit under (a transport that stopped).
    rafka_mesh_entity::runtime::set_own_exit(rafka_mesh_entity::runtime::OwnExit {
        data_dir: launch.data_dir.clone(),
        deployment_id: runtime.deployment_id.clone(),
        incarnation: launch.incarnation.0.clone(),
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
    // This node's own connections: hydrated from its data dir, then kept by what its client
    // observes of its pooled connections (connections.md sections 4.2, 9 and 10).
    // The storage fault the originate door arms (testkit only): the product's file storage,
    // decorated.
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.resolve.via-connections-hydrated", node = %launch.name);
    let fault = Arc::new(crate::faults::StorageFault::default());
    let connections = Arc::new(rafka_node_admin_core::connections_writer::ConnectionsWriter::new(
        rafka_mesh_entity::connections::ConnectionEnd { name: launch.name.clone(), node_id: launch.node_id.clone(), incarnation: Some(launch.incarnation.clone()) },
        Arc::new(crate::faults::FaultedConnectionsStorage::new(
            Arc::new(rafka_node_admin_core::storage::FileConnectionsStorage::open(&launch.data_dir).map_err(|e| anyhow!("connections storage: {e}"))?),
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
    let client = Arc::new(rafka_node_rpc::NodeRpcClient::new(ep0.clone(), resolver.clone()).with_caller_system("rdm").with_connection_observer(connections.clone()));
    // The status kick (fabric-node-lifecycle.md §7.3): answered once this node has joined.
    let subject: KickSlot = Arc::new(std::sync::OnceLock::new());
    // Every connection this node accepts from a live peer is Direct Connected from this node
    // to that peer (connections.md §10), reported to the same writer as its own dials.
    let seams = crate::originate::Seams { resolver: resolver.clone(), client: client.clone(), connections: connections.clone(), fault };
    let topology_slot: rafka_node_admin_core::topology_read::TopologySlot = Arc::new(std::sync::OnceLock::new());
    // Closed until this node has joined its mesh (the kick slot is filled): every op, ping
    // included, is a typed NotReady before then.
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.create.via-catalog-sealed", node = %launch.name);
    let server = serve_kick(register(rafka_node_admin_core::topology_read::serve(core_protocols(ServerBuilder::new().with_connection_observer(resolver.clone(), connections.clone()), client.clone(), Some(connections.clone())), topology_slot.clone()), seams), subject.clone())
        .carry::<crate::proof_store::ProofStore>()
        .carry::<rafka_node_rpc_contract::status::Status>()
        .with_ready_gate(ready.clone())
        .seal(rafka_node_rpc::ServedBirth { node_id: launch.node_id.to_string(), incarnation: launch.incarnation.0.clone() })
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
        admin_api_base: None,
        // Stamped by `Membership::publish`: this birth's own sequence and the composed clock.
        emitted_at_rafka_ms: 0,
        digest_seq: 0,
        mesh_id: None,
        in_flight: None,
        extra: Default::default(),
        load: None,
        gossip: None,
        data_dir: Some(launch.data_dir.display().to_string()),
    };
    // The join: this node's first call after it binds. `JoinNode` carries the digest above to the
    // admin that deployed it; the admin verifies it against the deployment and answers what it
    // holds. An admin that refuses the digest by name ends this node by that name.
    let mut joined = None;
    let join_step = tracing::info_span!(parent: boot, "rdm.mesh.node.update.via-join-admitted", node = %launch.name, launcher = launch.launcher.as_ref().map(|l| l.name.to_string()).unwrap_or_default());
    if let (Some(launcher), Some(seed)) = (&launch.launcher, seeds.first()) {
        let launcher_ref = rafka_node_rpc::ResolvedNode {
            node_id: launcher.node_id.clone(),
            name: launcher.name.clone(),
            endpoint_id: seed.id,
            transport_addr: seed.ip_addrs().next().copied().ok_or_else(|| anyhow!("the launching admin's seed names no address"))?,
            incarnation: launcher.incarnation.clone(),
        };
        resolver.apply(launcher_ref, None);
        let mut join_digest = digest.clone();
        join_digest.status = MemberStatus::Pending;
        match rafka_node_admin_core::join::call_join(&client, &rafka_node_rpc::NodeTarget::ExactNode(launcher.node_id.clone()), &seed.id.fmt_short().to_string(), &join_digest, 5).await {
            Ok(answer) => joined = Some(answer),
            Err(rafka_node_admin_core::join::JoinFailure::Refused(why)) => return Err(anyhow!("{} was refused its join by {}: {why}", launch.name, launcher.name)),
            Err(e @ rafka_node_admin_core::join::JoinFailure::Unreached(_)) => tracing::warn!(node = %launch.name, error = %e, "the launching admin could not take the join: this node's view fills from gossip"),
        }
    }
    // The mesh's id names its channel; the launching admin writes it.
    let mesh_id = match (&launch.mesh_id, &joined) {
        (Some(id), _) => id.clone(),
        (None, _) => return Err(anyhow!("a node needs its mesh's id from its launch")),
    };
    drop(join_step);
    let step = tracing::info_span!(parent: boot, "rdm.mesh.node.update.via-membership-joined", node = %launch.name, mesh_id = %mesh_id, seeds = seeds.len());
    let membership = Membership::join(&g, &ep0, &launch.fabric_id, &launch.name.mesh, &mesh_id, &name, clock, seeds).await?;
    drop(step);
    let (node_rpc, node_rpc_feed) = crate::node_rpc::ProcessNodeRpc::with_client(resolver.clone(), client.clone(), &membership.book, &name);
    // This node serves the topology it holds from here on (until now a read is `NotReady`).
    let status = Arc::new(Mutex::new(MemberStatus::Pending));
    {
        let (own, st) = (digest.clone(), status.clone());
        let _ = topology_slot.set(Arc::new(rafka_node_admin_core::topology_read::TopologyDoor::new(
            membership.clone(),
            Arc::new(move || {
                let mut d = own.clone();
                d.status = *st.lock().unwrap();
                d
            }),
        )));
    }
    // Take the launching admin's topology before marking ready: the join answered the control
    // state; `GetTopology` reads the topology from the same admin, installed per mesh only when
    // its snapshot is complete.
    if let (Some(answer), Some(launcher)) = (joined, &launch.launcher) {
        let _topology_step = tracing::info_span!(parent: boot, "rdm.mesh.node.update.via-topology-taken", node = %launch.name, launcher = %launcher.name);
        membership.learn_statuses(&answer.statuses);
        let read = rafka_node_admin_core::topology_read::get_topology(&client, &rafka_node_rpc::NodeTarget::ExactNode(launcher.node_id.clone()), &launcher.name.mesh, &membership, None, None)
            .await
            .map_err(|e| anyhow!("{} could not read the topology of {}: {e}", launch.name, launcher.name))?;
        let mesh_peers: Vec<EndpointAddr> = read
            .installed
            .iter()
            .flat_map(|m| m.members.iter())
            .filter(|d| d.fabric_id == launch.fabric_id && d.node.name != launch.name && d.node.name.mesh == launch.name.mesh)
            .filter_map(rafka_mesh_transport::membership::gossip_addr)
            .collect();
        let _ = membership.join_peers(mesh_peers).await;
    }
    // A delta that does not follow what this node holds desynchronizes that source; the node tops
    // up from its Mesh's own primary (gossip.md §3.3) with `GetTopology` of each such mesh.
    {
        let client = client.clone();
        membership.spawn_top_up(Arc::new(move |membership: Membership, primary: MeshDigest, meshes: Vec<String>| {
            let client = client.clone();
            Box::pin(async move {
                let mut done = rafka_mesh_transport::membership::TopUpDone::default();
                for mesh in meshes {
                    let read = rafka_node_admin_core::topology_read::get_topology(&client, &rafka_node_rpc::NodeTarget::ExactNode(primary.node.node_id.clone()), &primary.node.name.mesh, &membership, Some(&mesh), None)
                        .await
                        .map_err(|e| format!("{mesh}: {e}"))?;
                    done.installed.extend(read.installed.iter().map(|m| (m.mesh.clone(), m.topology_version)));
                }
                Ok(done)
            })
        }));
    }
    // Born full: this node is published only now, its entry taken.
    tracing::info_span!(
        parent: boot,
        "rdm.mesh.node.update.via-ready",
        node = %name,
        kind = launch.name.kind.name(),
        incarnation_id = %launch.incarnation.0,
        meshes = membership.meshes_held(),
        node_id = %launch.node_id,
        mesh_id = %mesh_id,
        fabric_id = %launch.fabric_id,
        id_format = rafka_mesh_entity::ID_FORMAT,
        deployment_id = %runtime.deployment_id,
        provider = runtime.provider.as_str(),
        provider_control_domain_fingerprint = %runtime.domain_fingerprint(),
        runtime_locator_kind = runtime.locator.kind(),
        runtime_locator_fingerprint = %runtime.locator_fingerprint(),
        source = "self-published-membership"
    )
        .in_scope(|| tracing::info!("ready for traffic"));
    *status.lock().unwrap() = MemberStatus::ReadyForTraffic;
    let owed_state: Arc<Mutex<Option<rafka_node_rpc_contract::status::NodeState>>> = Arc::new(Mutex::new(Some(rafka_node_rpc_contract::status::NodeState::ReadyForTraffic)));
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
        Arc::new(rafka_node_admin_core::node_self::NodeSelf::new(launch.node_id.clone(), launch.incarnation.clone(), launch.name.clone(), server.clone(), client.clone(), membership.clone(), set_status, current, Some(declare)))
    };
    let _ = subject.set(Arc::new(Kicked { membership: membership.clone(), digest: digest.clone(), status: status.clone(), own }));
    ready.store(true, std::sync::atomic::Ordering::SeqCst);
    let (d, st, stats) = (digest.clone(), status.clone(), server.stats());
    let publisher = membership.publish_every(rafka_mesh_transport::membership::gossip_interval(), move || {
        let mut d = d.clone();
        d.status = *st.lock().unwrap();
        d.in_flight = Some(rafka_node_rpc::ServerStats::get(&stats.in_flight));
        d
    });
    // The owed declaration loop: whatever state this birth owes is re-declared every publish
    // cadence until an authority answers by name. An authority that does not yet hold this
    // birth answers `sender-not-subject`; the next cadence carries the digest and the retry lands.
    let declare_loop = {
        let (node, membership, client, owed) = (digest.node.clone(), membership.clone(), node_rpc.client.clone(), owed_state.clone());
        tokio::spawn(async move {
            loop {
                if owed.lock().unwrap().is_some() {
                    declare_once(&node, &membership, &client, &owed).await;
                }
                tokio::time::sleep(rafka_mesh_transport::membership::gossip_interval()).await;
            }
        })
    };
    Ok(RunningNode { routers, membership, server, status, digest, node_rpc, owed_state, gossip: g, publisher, node_rpc_feed, declare_loop })
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
        tracing::info_span!("rdm.node_rpc.status.update.via-declare-own", node = %me.name, state = ?state, to = %a.node.name, outcome = %outcome, definitive)
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
/// declaration and every mesh or fabric operation is refused by name.
///
/// The tickle: ask the exact birth to reassert itself. Protocol name: `ProbeNodeState`. The node
/// hands its peers to its mesh channel again, publishes its digest, and answers its current
/// state; no transition. `DrainNode` and `StopNode` from a node-admin are obeyed by
/// `rafka_node_admin_core::node_self`. A stale incarnation is refused with the one held.
fn serve_kick(b: ServerBuilder, slot: KickSlot) -> ServerBuilder {
    use rafka_node_rpc_contract::status::{NotAuthority, Status, StatusReply, StatusRequest};
    b.serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |peer: rafka_node_rpc::PeerContext, req: StatusRequest| {
        let slot = slot.clone();
        async move {
            let Some(me) = slot.get().cloned() else {
                return Ok(StatusReply::NotReady { reason: "this node has not joined its mesh yet".into() });
            };
            let sender = me.membership.book.all().into_iter().find(|d| d.node.endpoint_id.0 == peer.endpoint_id.to_string());
            let from_admin = sender.as_ref().is_some_and(|d| d.node.name.kind == rafka_mesh_entity::NodeKind::NodeAdmin);
            let sender_name = sender.as_ref().map(|d| d.node.name.to_string()).unwrap_or_default();
            if matches!(req, StatusRequest::DrainNode { .. } | StatusRequest::StopNode { .. }) {
                let Some(from) = sender.as_ref().filter(|d| d.node.name.kind == rafka_mesh_entity::NodeKind::NodeAdmin) else {
                    return Ok(StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender_name } });
                };
                let reply = me.own.serve(&from.node.node_id, &req).await.expect("a command");
                tracing::info_span!("rdm.node_admin.status.update.via-command", node = %me.digest.node.name, op = req.op(), sender = %from.node.name, outcome = reply.name(), "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a node command naming this node was decided"));
                return Ok(reply);
            }
            let (node_id, incarnation) = match &req {
                StatusRequest::ProbeNodeState { node_id, incarnation } | StatusRequest::ApplyNodeState { node_id, incarnation, .. } => (node_id, incarnation),
                _ => return Ok(StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a node-admin".into() } }),
            };
            if *node_id != me.digest.node.node_id || !from_admin {
                return Ok(StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "the subject node itself, from a node-admin".into() } });
            }
            if *incarnation != me.digest.node.incarnation {
                return Ok(StatusReply::RejectedStaleIncarnation { held: me.digest.node.incarnation.clone() });
            }
            match req {
                StatusRequest::ProbeNodeState { .. } => {
                    let peers: Vec<EndpointAddr> = me.membership.book.all().iter().filter_map(rafka_mesh_transport::membership::gossip_addr).collect();
                    let _ = me.membership.join_peers(peers).await;
                    let status = *me.status.lock().unwrap();
                    let mut d = me.digest.clone();
                    d.status = status;
                    let _ = me.membership.publish(&d).await;
                    tracing::info_span!(
                        "rdm.node_admin.status.update.via-probe",
                        node = %me.digest.node.name,
                        sender = %sender_name,
                        state = ?node_state_of(status),
                    )
                    .in_scope(|| tracing::info!("probed by a node-admin: presence re-published, current state answered"));
                    Ok(StatusReply::Current { node_id: me.digest.node.node_id.clone(), incarnation: me.digest.node.incarnation.clone(), state: node_state_of(status) })
                }
                StatusRequest::ApplyNodeState { state, .. } => {
                    // Not a drain command and not a stop: the only drain path is drain-node.
                    let current = node_state_of(*me.status.lock().unwrap());
                    tracing::info_span!("rdm.node_admin.status.reject.via-apply-node-state", node = %me.digest.node.name, sender = %sender_name, requested = ?state, current = ?current, reason = "drain-node is the only drain path and stop-node the only stop; apply-node-state is served for no state")
                        .in_scope(|| tracing::info!("apply-node-state refused"));
                    Ok(StatusReply::RejectedInvalidNodeTransition { current })
                }
                _ => Ok(StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a node-admin".into() } }),
            }
        }
    })
}
