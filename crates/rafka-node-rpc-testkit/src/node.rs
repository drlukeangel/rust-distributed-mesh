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
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
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

/// Core Ping is served by every rpc node.
pub fn core_protocols(b: ServerBuilder) -> ServerBuilder {
    b.serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
        let PingRequest::Ping { payload, .. } = req;
        Ok(PingReply::Pong { payload })
    })
}

pub struct RunningNode {
    pub routers: Vec<Router>,
    pub membership: Membership,
    pub server: NodeRpcServer,
    pub status: Arc<Mutex<MemberStatus>>,
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

pub fn drain_deadline_from_env() -> Duration {
    Duration::from_millis(std::env::var("RDM_DRAIN_DEADLINE_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(5000))
}

impl RunningNode {
    /// Declare this birth's `state` to its authority (i143.e4.s11): the node-admins of its mesh
    /// it hears, tried in path order until one answers by name (the mesh-primary applies; another
    /// admin answers `RejectedNotAuthority: receiver-not-primary` and the next is tried). A
    /// definitive answer ends the attempt; `NotSent`/`Indeterminate` leaves it to the next call
    /// site. Nothing here gates gossip: the digest already says the state.
    pub async fn declare_own(&self, state: rafka_node_rpc_contract::status::NodeState) -> Option<String> {
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
        std::process::exit(4);
    };
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
            () = stopped => {}
        }
    }
    #[cfg(not(unix))]
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
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
pub async fn start_with_clock(launch: &Launch, clock: rafka_mesh_transport::clock::SharedClock, register: impl FnOnce(ServerBuilder, crate::originate::Seams) -> ServerBuilder) -> Result<RunningNode> {
    let key = load_or_mint_key(&launch.data_dir)?;
    // This process's one live resolver: a handler registered below may hold it; it is fed once
    // membership is joined.
    let resolver = Arc::new(rafka_node_rpc::LiveNodeResolver::default());
    // This birth's exact runtime, as its provider recorded it: published with
    // the birth so any admin can manage it, whoever launched it.
    let dir = launch.data_dir.clone();
    let runtime = tokio::task::spawn_blocking(move || rafka_mesh_entity::runtime::await_own_record(&dir, Duration::from_secs(10)))
        .await
        .map_err(|e| anyhow!("reading the runtime record: {e}"))?
        .map_err(|e| anyhow!("{e}"))?;
    // This runtime holds the claims of its handed addresses as itself before binding them: the
    // admin that reserved them may die while this node lives on them.
    rafka_node_admin_core::deployment::endpoint::hold_as_runtime(std::iter::once(launch.transport_addr).chain(launch.listeners.iter().map(|(_, a)| *a)));
    // One endpoint, one socket for the process: Node RPC and gossip share it by ALPN. A request
    let ep0 = rafka_node_rpc::endpoint::bind(key.clone(), launch.transport_addr)
        .await
        .with_context(|| format!("the node cannot bind its assigned transport address {}", launch.transport_addr))?;
    // The process's one client, made before the server seals: the server carries the proof
    // store for others through it (one direct inner call per forward, never a second hop).
    // This node's own connections: hydrated from its data dir, then kept by what its client
    // observes of its pooled connections (connections.md sections 4.2, 9 and 10).
    // The storage fault the originate door arms (testkit only): the product's file storage,
    // decorated.
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
    // An owed Proxy retirement whose write was refused is attempted again while owed
    // (connections.md §10); the task ends with the process.
    let _retirements = connections.spawn_retirement_reconciler(rafka_node_admin_core::connections_writer::RETIREMENT_RETRY);
    let client = Arc::new(rafka_node_rpc::NodeRpcClient::new(ep0.clone(), resolver.clone()).with_caller_system("rdm").with_connection_observer(connections.clone()));
    // The status kick (fabric-node-lifecycle.md §7.3): answered once this node has joined.
    let subject: KickSlot = Arc::new(std::sync::OnceLock::new());
    // Every connection this node accepts from a live peer is Direct Connected from this node
    // to that peer (connections.md §10), reported to the same writer as its own dials.
    let seams = crate::originate::Seams { resolver: resolver.clone(), client: client.clone(), connections: connections.clone(), fault };
    let server = serve_kick(register(core_protocols(ServerBuilder::new().with_connection_observer(resolver.clone(), connections.clone())), seams), subject.clone())
        .carry::<crate::proof_store::ProofStore>()
        .carry::<rafka_node_rpc_contract::status::Status>()
        .serve_forward_with_edges(client.clone(), connections.clone())
        .seal(rafka_node_rpc::ServedBirth { node_id: launch.node_id.to_string(), incarnation: launch.incarnation.0.clone() })
        .map_err(|e| anyhow!("protocol catalog refused to seal: {e:?}"))?;
    let g = iroh_gossip::net::Gossip::builder().spawn(ep0.clone());
    let routers = vec![Router::builder(ep0.clone()).accept(rafka_node_rpc::ALPN, server.clone()).accept(iroh_gossip::ALPN, g.clone()).spawn()];
    let seeds: Vec<EndpointAddr> = launch
        .seeds
        .iter()
        .filter_map(|(k, a)| k.parse::<iroh::PublicKey>().ok().map(|pk| EndpointAddr::new(pk).with_ip_addr(*a)))
        .collect();
    let anchor = seeds.first().cloned();
    let name = launch.name.to_string();
    // The mesh's id names its channel; the launching admin writes it. A
    // launch without one takes it from the entry pull's projection.
    let mut pulled = None;
    let mesh_id = match (&launch.mesh_id, &anchor) {
        (Some(id), _) => id.clone(),
        (None, Some(a)) => {
            let answer = rafka_mesh_transport::entry::pull(&ep0, a.clone(), &name, 5).await.map_err(|e| anyhow!("entry pull: {e}"))?;
            let id = answer.topology["meshes"]
                .as_array()
                .and_then(|ms| ms.iter().find(|m| m["name"] == launch.name.mesh.as_str()))
                .and_then(|m| m["id"].as_str())
                .ok_or_else(|| anyhow!("the entry answer names no id for mesh {}", launch.name.mesh))
                .and_then(|v| rafka_mesh_entity::MeshId::parse(v).map_err(|e| anyhow!("the entry answer's mesh id: {e}")))?;
            pulled = Some(answer);
            id
        }
        (None, None) => return Err(anyhow!("a node needs its mesh's id or an admin to ask for it")),
    };
    // Subscribe first, pull second: what changes during the pull arrives by
    // gossip, and the book keeps the newer copy.
    let membership = Membership::join(&g, &ep0, &launch.fabric_id, &launch.name.mesh, &mesh_id, &name, clock, seeds).await?;
    let (node_rpc, node_rpc_feed) = crate::node_rpc::ProcessNodeRpc::with_client(resolver, client, &membership.book, &name);
    // Entry: take the launching admin's membership before marking ready. An
    // admin that cannot answer does not hold the node: its view fills from
    // gossip instead (the pull is named either way).
    if let Some(anchor) = anchor {
        let answer = match pulled {
            Some(a) => Ok(a),
            None => rafka_mesh_transport::entry::pull(&ep0, anchor, &name, 5).await,
        };
        if let Ok(answer) = answer {
            let mut mesh_peers = Vec::new();
            for d in answer.members.into_iter().filter(|d| d.fabric_id == launch.fabric_id && d.node.name != launch.name) {
                if d.node.name.mesh == launch.name.mesh {
                    mesh_peers.extend(rafka_mesh_transport::membership::gossip_addr(&d));
                }
                membership.learn(d, "entry");
            }
            membership.learn_statuses(&answer.statuses);
            let _ = membership.join_peers(mesh_peers).await;
        }
    }
    let digest = MeshDigest {
        fabric_id: launch.fabric_id.clone(),
        node: MeshNode {
            node_id: launch.node_id.clone(),
            name: launch.name.clone(),
            endpoint_id: rafka_mesh_entity::EndpointId(key.public().to_string()),
            incarnation: launch.incarnation.clone(),
            supersedes: launch.supersedes.clone(),
            transport_addr: launch.transport_addr,
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
        data_dir: Some(launch.data_dir.display().to_string()),
    };
    // Born full: this node is published only now, its entry taken.
    tracing::info_span!(
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
    let status = Arc::new(Mutex::new(MemberStatus::ReadyForTraffic));
    let _ = subject.set(Arc::new(Kicked { membership: membership.clone(), digest: digest.clone(), status: status.clone(), server: server.clone() }));
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
    let owed_state: Arc<Mutex<Option<rafka_node_rpc_contract::status::NodeState>>> = Arc::new(Mutex::new(Some(rafka_node_rpc_contract::status::NodeState::ReadyForTraffic)));
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
pub struct Kicked {
    membership: Membership,
    digest: MeshDigest,
    status: Arc<Mutex<MemberStatus>>,
    /// This node's server: what an `ApplyNodeState(Draining)` drains, and whose in-flight count it answers.
    server: rafka_node_rpc::NodeRpcServer,
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
/// state; no transition. `ApplyNodeState(Draining)`: the server refuses new calls, the status
/// says `Draining`, and the answer is the work still in flight, the current count on every
/// repeat. A stale incarnation is refused with the one held.
fn serve_kick(b: ServerBuilder, slot: KickSlot) -> ServerBuilder {
    use rafka_node_rpc_contract::status::{NodeState, NotAuthority, Status, StatusReply, StatusRequest};
    b.serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |peer: rafka_node_rpc::PeerContext, req: StatusRequest| {
        let slot = slot.clone();
        async move {
            let Some(me) = slot.get().cloned() else {
                return Ok(StatusReply::NotReady { reason: "this node has not joined its mesh yet".into() });
            };
            let sender = me.membership.book.all().into_iter().find(|d| d.node.endpoint_id.0 == peer.endpoint_id.to_string());
            let from_admin = sender.as_ref().is_some_and(|d| d.node.name.kind == rafka_mesh_entity::NodeKind::NodeAdmin);
            let sender_name = sender.as_ref().map(|d| d.node.name.to_string()).unwrap_or_default();
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
                StatusRequest::ApplyNodeState { state: NodeState::Draining, .. } => {
                    me.server.drain();
                    *me.status.lock().unwrap() = MemberStatus::Draining;
                    // The work still in flight besides this call: the handler answering the
                    // drain is itself counted by the server until it returns.
                    let in_flight = rafka_node_rpc::ServerStats::get(&me.server.stats().in_flight).saturating_sub(1);
                    let mut d = me.digest.clone();
                    d.status = MemberStatus::Draining;
                    d.in_flight = Some(in_flight);
                    let _ = me.membership.publish(&d).await;
                    tracing::info_span!(
                        "rdm.node_admin.status.update.via-apply-draining",
                        node = %me.digest.node.name,
                        sender = %sender_name,
                        in_flight,
                    )
                    .in_scope(|| tracing::info!("draining applied by a node-admin: new calls refused, in-flight work finishing"));
                    Ok(StatusReply::NodeDrainingApplied { in_flight })
                }
                StatusRequest::ApplyNodeState { state, .. } => {
                    Ok(StatusReply::NotReady { reason: format!("{} serves apply-node-state only for Draining in this build; {state:?} is not served", me.digest.node.name) })
                }
                _ => Ok(StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a node-admin".into() } }),
            }
        }
    })
}
