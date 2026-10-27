//! The originate door: a testkit-only protocol on op `0x73` that asks the executing node to
//! make a proof-store call of its own to a destination over its own held connections projection
//! (`NodeRpcClient::call_connected`, connections.md §5), to arm or release its storage fault
//! ([`crate::faults`]), and to answer a snapshot of what it holds and owes. A scenario thereby
//! proves CONN-B, CONN-D and CONN-G on a real, restartable node: the node is the source, its
//! writer records the facts, its storage is the one hydrated after a restart. Testkit range only
//! ([`TESTKIT_OPS`]); never a product binary.
//!
//! [`TESTKIT_OPS`]: rdm_core::catalog::TESTKIT_OPS

use crate::faults::StorageFault;
use crate::proof_store::{ProofReply, ProofRequest, ProofStore};
use rdm_core::connections::{resolve, CarrierPolicy, ConnectionState, NodeConnection};
use rdm_core::launch::Launch;
use rdm_core::{NodeKind, PathName};
use rdm_node::connections_writer::ConnectionsWriter;
use rdm_transport::{CallOptions, LiveNodeResolver, NodeResolver, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The carrier policy the testkit applies to its proof-store calls: proxied through an rpc
/// node (the product's own policies are the product's).
pub const POLICY: CarrierPolicy = CarrierPolicy::Proxy { carrier_kind: NodeKind::RpcNode };

/// The originate door protocol on op `0x73`.
pub struct Originate;

/// A proof-store operation the originating node makes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProofOp {
    /// Read `key`.
    Get {
        /// The key to read.
        key: Vec<u8>,
    },
    /// Write `value` under `key`.
    Put {
        /// The key to write.
        key: Vec<u8>,
        /// The value to write.
        value: Vec<u8>,
    },
}

/// An originate door call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OriginateRequest {
    /// Call `destination` (a path) with `op`, over this node's held projection.
    Call {
        /// The `path.name` to call.
        destination: String,
        /// The proof-store operation to make.
        op: ProofOp,
    },
    /// Refuse the next `refuse_index` index writes and `refuse_history` history appends, after
    /// letting `pass_history` history appends through.
    ArmFault {
        /// The number of index writes to refuse.
        refuse_index: u32,
        /// The number of history appends to refuse.
        refuse_history: u32,
        /// The number of history appends to let through first.
        pass_history: u32,
    },
    /// Release the armed storage fault.
    ReleaseFault,
    /// Answer a snapshot of what the node holds and owes.
    Snapshot,
    /// Seed this node's own durable facts toward `destination` as a proven carried path leaves
    /// them (connections.md section 7): `failed_attempts` Direct Failed observations through the
    /// writer's own observer path, then `Proxy Connected` through `carrier`, both naming the
    /// births this node's topology view holds now. Testkit only: the product writes a Proxy after
    /// cold discovery, which nothing in this repository runs.
    RecordProxy {
        /// The `path.name` the facts concern.
        destination: String,
        /// The `path.name` of the carrier.
        carrier: String,
        /// The number of Direct Failed observations to record first.
        failed_attempts: u32,
    },
    /// One direct core Ping to `destination`, outside route resolution, as a node's own
    /// background traffic reaches a peer: the pooled connection it opens is reported to the
    /// destination's writer as Direct Connected from the destination's side (accepted) and to
    /// this node's writer as its own dial.
    Dial {
        /// The `path.name` to ping.
        destination: String,
    },
    /// Open a data-plane stream (connections.md §5: `Connections::open`, `open_bi`) to the current
    /// birth of `destination` on this node's one registered ALPN, write `payload`, finish, and read
    /// what the far role answers. The reply names the connection's kind (Direct or Proxy) and peer.
    Stream {
        /// The `path.name` to stream to.
        destination: String,
        /// The bytes written.
        payload: Vec<u8>,
    },
    /// Mark this process's mesh transport stopped, exactly as iroh-gossip refusing a subscription
    /// does (`topology::mark_transport_stopped`): the node exits through the one transport-stopped
    /// exit, recording its reason in its data dir. The reply is sent before the mark.
    StopTransport {
        /// The reason the transport stopped.
        reason: String,
    },
    /// Block (`blocked: true`) or release this node's Direct dials toward `destination` on its one
    /// data-plane ALPN: a blocked dial answers `NotSent` and records Direct Failed, as an unreachable
    /// direct dial does (`connection_faults::block_direct`, connections.md §13 A, F, J).
    BlockDirect {
        /// The `path.name` whose Direct dials are blocked or released.
        destination: String,
        /// Block when true, release when false.
        blocked: bool,
    },
}

/// Who answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnsweredBy {
    /// The answering node's id.
    pub node_id: String,
    /// The answering node's `path.name`.
    pub node: String,
    /// The answering node's incarnation id.
    pub incarnation_id: String,
}

/// An originate door answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OriginateReply {
    /// The call was made.
    Called {
        /// Who made the call.
        by: AnsweredBy,
        /// The destination's node id.
        destination_node_id: String,
        /// The effective route's token: `via-peer`, `direct`, `direct-unknown` or `no-active-route`.
        route: String,
        /// The carrier the route used, when it used one.
        carrier: Option<String>,
        /// The own Proxy the resolution found invalid, and why, if any.
        retired: Option<String>,
        /// The call's outcome name.
        outcome: String,
        /// The proof-store reply, when the call replied.
        reply: Option<ProofReply>,
        /// The source's pooled connections after the call, by target path.
        pooled: Vec<String>,
    },
    /// The storage fault was armed.
    FaultArmed {
        /// Who armed it.
        by: AnsweredBy,
        /// Index writes still to refuse.
        refuse_index: u32,
        /// History appends still to refuse.
        refuse_history: u32,
    },
    /// The storage fault was released.
    FaultReleased {
        /// Who released it.
        by: AnsweredBy,
        /// The number of writes the fault refused.
        refused: u32,
    },
    /// What the node holds and owes.
    Snapshot {
        /// Who answered.
        by: AnsweredBy,
        /// The node's active Proxy entries.
        own_active_proxies: Vec<NodeConnection>,
        /// The node's latest Direct entry per destination.
        own_latest_directs: Vec<NodeConnection>,
        /// The number of active Direct facts held that name a destination process still current.
        active_len: usize,
        /// Every active Direct fact held, whatever its source (connections.md §4.1: the fleet-wide
        /// Direct graph a source discovers carriers from).
        active_directs: Vec<NodeConnection>,
        /// The Proxy retirements the node owes now.
        owed: Vec<NodeConnection>,
        /// The number of writes the fault has refused.
        fault_refused: u32,
    },
    /// The destination does not parse or does not resolve on this node.
    BadDestination {
        /// Who answered.
        by: AnsweredBy,
        /// Why the destination is refused.
        reason: String,
    },
    /// The peer the call needed could not be resolved.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The node is not ready to serve.
    NotReady {
        /// Why the node is not ready.
        reason: String,
    },
    /// The node is at its admission bound.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// The node is connections-deleting and takes no new work.
    ConnectionsDeleting {
        /// Why it refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
    /// The facts a `RecordProxy` wrote, with the births they name.
    ProxyRecorded {
        /// Who recorded the facts.
        by: AnsweredBy,
        /// The destination's node id.
        destination_node_id: String,
        /// The destination incarnation the facts name.
        destination_incarnation: String,
        /// The carrier's node id.
        carrier_node_id: String,
        /// The carrier incarnation the facts name.
        carrier_incarnation: String,
        /// The Direct Failed observations recorded.
        failed_attempts: u32,
    },
    /// The answer of a `Dial`: the typed outcome name of the one Ping.
    Dialed {
        /// Who dialled.
        by: AnsweredBy,
        /// The destination's node id.
        destination_node_id: String,
        /// The Ping's outcome name.
        outcome: String,
    },
    /// The stream was opened, written and answered.
    Streamed {
        /// Who opened it.
        by: AnsweredBy,
        /// The far peer the connection reached (its exact birth).
        peer: AnsweredBy,
        /// `direct` or `proxy`.
        kind: String,
        /// The connection row's id.
        connection_id: String,
        /// What the far role answered on the stream.
        answered: Vec<u8>,
    },
    /// The stream could not be opened, written or read.
    StreamFailed {
        /// Who tried.
        by: AnsweredBy,
        /// Why, by name.
        reason: String,
    },
    /// The transport stop is marked; the process is exiting.
    TransportStopMarked {
        /// Who marked the stop.
        by: AnsweredBy,
        /// The reason recorded.
        reason: String,
    },
    /// The Direct dial block toward `destination` is set as asked.
    DirectBlocked {
        /// Who set it.
        by: AnsweredBy,
        /// The `path.name` the block names.
        destination: String,
        /// Whether Direct dials toward it are now blocked.
        blocked: bool,
    },
}

impl NodeProtocol for Originate {
    const OP: u8 = 0x73;
    const NAME: &'static str = "originate";
    const MAX_REQUEST_FRAME_BYTES: usize = 4096;
    const MAX_REPLY_FRAME_BYTES: usize = 65536;
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 9;
    const REPLY_VARIANTS: u32 = 17;
    type Request = OriginateRequest;
    type Reply = OriginateReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            OriginateReply::Called { .. } => "called",
            OriginateReply::FaultArmed { .. } => "fault-armed",
            OriginateReply::FaultReleased { .. } => "fault-released",
            OriginateReply::Snapshot { .. } => "snapshot",
            OriginateReply::BadDestination { .. } => "bad-destination",
            OriginateReply::PeerUnresolved { .. } => "peer-unresolved",
            OriginateReply::NotReady { .. } => "not-ready",
            OriginateReply::Busy { .. } => "busy",
            OriginateReply::ConnectionsDeleting { .. } => "connections-deleting",
            OriginateReply::Malformed { .. } => "malformed",
            OriginateReply::Unauthorized { .. } => "unauthorized",
            OriginateReply::ProxyRecorded { .. } => "proxy-recorded",
            OriginateReply::Dialed { .. } => "dialed",
            OriginateReply::Streamed { .. } => "streamed",
            OriginateReply::StreamFailed { .. } => "stream-failed",
            OriginateReply::TransportStopMarked { .. } => "transport-stop-marked",
            OriginateReply::DirectBlocked { .. } => "direct-blocked",
        }
    }
    fn classify_reply(reply: &OriginateReply) -> ReplyKind {
        match reply {
            OriginateReply::Called { .. } => ReplyKind::Success,
            OriginateReply::FaultArmed { .. } => ReplyKind::Success,
            OriginateReply::FaultReleased { .. } => ReplyKind::Success,
            OriginateReply::Snapshot { .. } => ReplyKind::Success,
            OriginateReply::BadDestination { .. } => ReplyKind::new(rdm_core::outcome::Code::InvalidArgument),
            OriginateReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            OriginateReply::NotReady { .. } => ReplyKind::NotReady,
            OriginateReply::Busy { .. } => ReplyKind::Busy,
            OriginateReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            OriginateReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            OriginateReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            OriginateReply::ProxyRecorded { .. } => ReplyKind::Success,
            OriginateReply::Dialed { .. } => ReplyKind::Success,
            OriginateReply::Streamed { .. } => ReplyKind::Success,
            OriginateReply::StreamFailed { .. } => ReplyKind::Success,
            OriginateReply::TransportStopMarked { .. } => ReplyKind::Success,
            OriginateReply::DirectBlocked { .. } => ReplyKind::Success,
        }
    }

    fn peer_unresolved(reason: String) -> OriginateReply {
        OriginateReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> OriginateReply {
        OriginateReply::NotReady { reason }
    }
    fn busy(reason: String) -> OriginateReply {
        OriginateReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> OriginateReply {
        OriginateReply::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> OriginateReply {
        OriginateReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> OriginateReply {
        OriginateReply::Unauthorized { reason }
    }
}

/// The process seams the door acts through.
#[derive(Clone)]
pub struct Seams {
    /// The resolver the door acts through.
    pub resolver: Arc<LiveNodeResolver>,
    /// The client the door calls through.
    pub client: Arc<NodeRpcClient>,
    /// The connections writer the door records through.
    pub connections: Arc<ConnectionsWriter>,
    /// The storage fault the door arms and releases.
    pub fault: Arc<StorageFault>,
    /// The name this birth holds now: every door names the node by it, per call.
    pub name: crate::held_name::HeldName,
    /// The dial half of this node's one data-plane ALPN, set once the node runs
    /// (`node::start_with_seams_and_alpn`); unset, `Stream` is refused by name.
    pub data_plane: Arc<std::sync::OnceLock<rdm_transport::connections::Dialer>>,
    /// This node's data-plane connections registry and the name of its one data-plane ALPN, set
    /// with [`Self::data_plane`]: the testkit's Direct dial block acts on it (`BlockDirect`).
    pub data_plane_registry: Arc<std::sync::OnceLock<(rdm_transport::connections::ConnectionsRegistry, Vec<u8>)>>,
}

/// Serve the door on this node's own seams, with the testkit's default carrier policy ([`POLICY`]).
pub fn serve(b: ServerBuilder, seams: Seams, launch: &Launch) -> ServerBuilder {
    serve_with_policy(b, seams, launch, POLICY)
}

/// Serve the door with the carrier policy the serving application declares: a Proxy held for a
/// destination is the effective route only through a carrier of `policy`'s kind
/// (`rdm_core::connections::resolve`). A shape whose carriers are not `rpc_node`s (a
/// gateway carrying for a compute) declares its own kind here.
pub fn serve_with_policy(b: ServerBuilder, seams: Seams, launch: &Launch, policy: CarrierPolicy) -> ServerBuilder {
    let (node_id, incarnation_id) = (launch.node_id.to_string(), launch.incarnation.to_string());
    b.serve::<Originate, _, _>(OpOwner::Testkit, move |_peer: PeerContext, req: OriginateRequest| {
        // The node answers and routes under the name it holds at this call (R12).
        let own: PathName = seams.name.now();
        let by = AnsweredBy { node_id: node_id.clone(), node: own.to_string(), incarnation_id: incarnation_id.clone() };
        let seams = seams.clone();
        async move {
            Ok(match req {
                OriginateRequest::Call { destination, op } => call(&by, &seams, &own, &destination, op, policy).await,
                OriginateRequest::ArmFault { refuse_index, refuse_history, pass_history } => {
                    seams.fault.arm(refuse_index, refuse_history, pass_history);
                    OriginateReply::FaultArmed { by, refuse_index, refuse_history }
                }
                OriginateRequest::ReleaseFault => OriginateReply::FaultReleased { by, refused: seams.fault.release() },
                OriginateRequest::RecordProxy { destination, carrier, failed_attempts } => record_proxy(&by, &seams, &destination, &carrier, failed_attempts).await,
                OriginateRequest::Dial { destination } => dial(&by, &seams, &destination).await,
                OriginateRequest::Stream { destination, payload } => stream(&by, &seams, &destination, payload).await,
                OriginateRequest::BlockDirect { destination, blocked } => block_direct(&by, &seams, &destination, blocked),
                OriginateRequest::StopTransport { reason } => {
                    // The mark runs after this reply is on its way: the exit it causes is the real one.
                    let marked = reason.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        rdm_transport::topology::mark_transport_stopped(marked);
                    });
                    OriginateReply::TransportStopMarked { by, reason }
                }
                OriginateRequest::Snapshot => {
                    let held = seams.connections.held();
                    let held = held.lock().unwrap();
                    OriginateReply::Snapshot {
                        by,
                        own_active_proxies: held.own_active_proxies().into_iter().cloned().collect(),
                        own_latest_directs: held.own_latest_directs().into_iter().cloned().collect(),
                        active_len: held.active_len(),
                        active_directs: held.active_directs().into_iter().cloned().collect(),
                        owed: rdm_core::reconnect::owed_direct_restored(&held, 0),
                        fault_refused: seams.fault.refused(),
                    }
                }
            })
        }
    })
}

async fn call(by: &AnsweredBy, seams: &Seams, own: &PathName, destination: &str, op: ProofOp, policy: CarrierPolicy) -> OriginateReply {
    let path: PathName = match destination.parse() {
        Ok(p) => p,
        Err(e) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination:?}: {e}") },
    };
    let target = match seams.resolver.resolve(&NodeTarget::CurrentPath(path.clone())) {
        Ok(n) => n,
        Err(f) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination} does not resolve on this node: {f:?}") },
    };
    // Any owed retirement is settled before a new call resolves its route (connections.md §10):
    // a refused write leaves the Proxy effective, by name.
    let _ = seams.connections.settle_owed().await;
    let resolution = {
        let held = seams.connections.held();
        let held = held.lock().unwrap();
        resolve(&held, own, &path, policy)
    };
    let req = match op {
        ProofOp::Get { key } => ProofRequest::Get { key },
        ProofOp::Put { key, value } => ProofRequest::Put { key, value },
    };
    let call = seams.client.call_resolved::<ProofStore>(resolution, own, &path, &target.node_id, &req, &CallOptions::default()).await;
    // A Proxy the resolution found invalid is retired by this source, as the caller of the seam.
    let retired = match call.retire {
        Some((proxy, why)) => {
            let row = NodeConnection { id: rdm_core::ConnectionId::mint(), state: ConnectionState::Disconnected, reason: Some(why.to_string()), recovery: None, logged_at_ms: now_ms().max(proxy.logged_at_ms + 1), ..proxy };
            let _ = seams.connections.record(row).await;
            Some(why.to_string())
        }
        None => None,
    };
    let (route, carrier) = match &call.route {
        rdm_core::connections::EffectiveRoute::ViaPeer { carrier, .. } => ("via-peer".to_string(), Some(carrier.to_string())),
        other => (other.token().to_string(), None),
    };
    let (outcome, reply) = match call.outcome {
        RpcOutcome::Reply(r) => ("reply".to_string(), Some(r.into_value())),
        other => (other.name().to_string(), None),
    };
    OriginateReply::Called {
        by: by.clone(),
        destination_node_id: target.node_id.to_string(),
        route,
        carrier,
        retired,
        outcome,
        reply,
        pooled: seams.client.pooled().into_iter().map(|k| format!("{k:?}")).collect(),
    }
}

async fn dial(by: &AnsweredBy, seams: &Seams, destination: &str) -> OriginateReply {
    use rdm_core::ping::{Ping, PingRequest};
    let path: PathName = match destination.parse() {
        Ok(p) => p,
        Err(e) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination:?}: {e}") },
    };
    let target = match seams.resolver.resolve(&NodeTarget::CurrentPath(path)) {
        Ok(n) => n,
        Err(f) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination} does not resolve on this node: {f:?}") },
    };
    let (out, _) = seams.client.call::<Ping>(&NodeTarget::ExactNode(target.node_id.clone()), &PingRequest::Ping { payload: b"dial".to_vec() }, &CallOptions::default()).await;
    OriginateReply::Dialed { by: by.clone(), destination_node_id: target.node_id.to_string(), outcome: out.name().to_string() }
}

/// One data-plane round trip to `destination`'s current birth: open on the node's ALPN, one
/// bidirectional stream, the payload written and finished, the far role's answer read whole.
async fn stream(by: &AnsweredBy, seams: &Seams, destination: &str, payload: Vec<u8>) -> OriginateReply {
    let failed = |reason: String| OriginateReply::StreamFailed { by: by.clone(), reason };
    let path: PathName = match destination.parse() {
        Ok(p) => p,
        Err(e) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination:?}: {e}") },
    };
    let target = match seams.resolver.resolve(&NodeTarget::CurrentPath(path.clone())) {
        Ok(n) => n,
        Err(f) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination} does not resolve on this node: {f:?}") },
    };
    let Some(dialer) = seams.data_plane.get() else {
        return failed("no-data-plane-alpn: this node registered no data-plane ALPN".into());
    };
    let birth = rdm_transport::connections::Birth { name: target.name.clone(), node_id: target.node_id.clone(), incarnation: target.incarnation.clone() };
    let conn = match dialer.open(&birth).await {
        Ok(c) => c,
        Err(e) => return failed(format!("open: {e:?}")),
    };
    let (mut send, mut recv) = match conn.open_bi().await {
        Ok(s) => s,
        Err(e) => return failed(format!("open_bi: {e:?}")),
    };
    if let Err(e) = send.write_all(&payload).await {
        return failed(format!("write: {e:?}"));
    }
    if let Err(e) = send.finish() {
        return failed(format!("finish: {e:?}"));
    }
    let answered = match recv.read_to_end(1 << 20).await {
        Ok(b) => b,
        Err(e) => return failed(format!("read: {e:?}")),
    };
    let peer = conn.peer();
    OriginateReply::Streamed {
        by: by.clone(),
        peer: AnsweredBy { node_id: peer.node_id.to_string(), node: peer.name.to_string(), incarnation_id: peer.incarnation.to_string() },
        kind: match conn.kind() { rdm_transport::connections::ConnectionKind::Direct => "direct".into(), rdm_transport::connections::ConnectionKind::Proxy { via } => format!("proxy via {}", via.name) },
        connection_id: conn.id().to_string(),
        answered,
    }
}

/// Block or release this node's Direct dials toward `destination` on its one data-plane ALPN.
fn block_direct(by: &AnsweredBy, seams: &Seams, destination: &str, blocked: bool) -> OriginateReply {
    let path: PathName = match destination.parse() {
        Ok(p) => p,
        Err(e) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination:?}: {e}") },
    };
    let Some((registry, alpn)) = seams.data_plane_registry.get() else {
        return OriginateReply::StreamFailed { by: by.clone(), reason: "no-data-plane-alpn: this node registered no data-plane ALPN".into() };
    };
    if blocked {
        registry.block_direct_dial(&path, alpn);
    } else {
        registry.unblock_direct_dial(&path, alpn);
    }
    OriginateReply::DirectBlocked { by: by.clone(), destination: path.to_string(), blocked }
}

async fn record_proxy(by: &AnsweredBy, seams: &Seams, destination: &str, carrier: &str, failed_attempts: u32) -> OriginateReply {
    use rdm_core::connections::{ConnectionEnd, ConnectionKind};
    use rdm_transport::ConnectionObserver;
    let bad = |reason: String| OriginateReply::BadDestination { by: by.clone(), reason };
    let mut ends = Vec::new();
    for what in [destination, carrier] {
        let path: PathName = match what.parse() {
            Ok(p) => p,
            Err(e) => return bad(format!("{what:?}: {e}")),
        };
        match seams.resolver.resolve(&NodeTarget::CurrentPath(path)) {
            Ok(n) => ends.push(n),
            Err(f) => return bad(format!("{what} does not resolve on this node: {f:?}")),
        }
    }
    let (dest, via) = (ends.remove(0), ends.remove(0));
    for _ in 0..failed_attempts {
        seams.connections.direct_failed(&dest, "testkit seed: the direct dial ended NotSent");
    }
    seams.connections.observations_landed().await;
    let end = |n: &rdm_transport::ResolvedNode| ConnectionEnd { name: n.name.clone(), node_id: n.node_id.clone(), incarnation: Some(n.incarnation.clone()) };
    let row = NodeConnection {
        id: rdm_core::ConnectionId::mint(),
        source: seams.connections.own(),
        destination: end(&dest),
        kind: ConnectionKind::Proxy,
        alpn: rdm_core::connections::ALPN.to_vec(),
        state: ConnectionState::Connected,
        carrier: Some(end(&via)),
        recovery: None,
        reason: None,
        logged_at_ms: now_ms().saturating_add(1),
    };
    if let Err(e) = seams.connections.record(row).await {
        return bad(format!("the Proxy row was refused by this node's storage: {e}"));
    }
    OriginateReply::ProxyRecorded {
        by: by.clone(),
        destination_node_id: dest.node_id.to_string(),
        destination_incarnation: dest.incarnation.to_string(),
        carrier_node_id: via.node_id.to_string(),
        carrier_incarnation: via.incarnation.to_string(),
        failed_attempts,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::connections::{ConnectionEnd, ConnectionKind, ConnectionsHeld, EffectiveRoute};
    use rdm_core::{IncarnationId, NodeId};

    fn end(kind: NodeKind, ordinal: u32, inc: &str) -> ConnectionEnd {
        ConnectionEnd { name: PathName::new("mesh1", kind, ordinal), node_id: NodeId::mint(), incarnation: Some(IncarnationId::parse(&rdm_core::fixture_crockford60(inc)).unwrap()) }
    }

    fn row(source: &ConnectionEnd, destination: &ConnectionEnd, kind: ConnectionKind, carrier: Option<&ConnectionEnd>, at: u64) -> NodeConnection {
        NodeConnection { id: rdm_core::ConnectionId::mint(), source: source.clone(), destination: destination.clone(), kind, alpn: rdm_core::connections::ALPN.to_vec(), state: ConnectionState::Connected, carrier: carrier.cloned(), recovery: None, reason: None, logged_at_ms: at }
    }

    /// CONTRACT: the door resolves a held Proxy under the carrier policy it is served with. A
    /// Proxy through a gateway is the effective route under a gateway policy, and is skipped (the
    /// route stays Direct, the Proxy is not retired) under the default rpc_node policy.
    #[test]
    fn door_resolves_a_gateway_carried_proxy_only_under_a_gateway_policy() {
        let (own, carrier, dest) = (end(NodeKind::Compute, 1, "o1"), end(NodeKind::Gateway, 1, "c1"), end(NodeKind::Broker, 1, "d1"));
        let mut held = ConnectionsHeld::new();
        held.set_own_source(own.name.clone());
        held.mark_complete();
        held.apply(row(&carrier, &dest, ConnectionKind::Direct, None, 10)).unwrap();
        let p = row(&own, &dest, ConnectionKind::Proxy, Some(&carrier), 20);
        held.apply(p.clone()).unwrap();
        let gateway = CarrierPolicy::Proxy { carrier_kind: NodeKind::Gateway };
        let r = resolve(&held, &own.name, &dest.name, gateway);
        assert_eq!(r.route, EffectiveRoute::ViaPeer { carrier: carrier.name.clone(), proxy: p });
        assert_eq!(r.retire, None);
        let d = resolve(&held, &own.name, &dest.name, POLICY);
        assert_eq!(d.route, EffectiveRoute::Direct { known: false }, "the gateway-carried Proxy is not the route under the rpc_node policy: with no Direct fact the pair dials directly");
        assert_eq!(d.retire, None, "a Proxy through a kind the policy does not name is skipped, never retired");
    }
}
