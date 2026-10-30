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
    ///
    /// Position 0 on the wire.
    Get {
        /// The key to read.
        key: Vec<u8>,
    },
    /// Write `value` under `key`.
    ///
    /// Position 1 on the wire.
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
    ///
    /// Position 0 on the wire.
    Call {
        /// The `path.name` to call.
        destination: String,
        /// The proof-store operation to make.
        op: ProofOp,
    },
    /// Refuse the next `refuse_index` index writes and `refuse_history` history appends, after
    /// letting `pass_history` history appends through.
    ///
    /// Position 1 on the wire.
    ArmFault {
        /// The number of index writes to refuse.
        refuse_index: u32,
        /// The number of history appends to refuse.
        refuse_history: u32,
        /// The number of history appends to let through first.
        pass_history: u32,
    },
    /// Release the armed storage fault.
    ///
    /// Position 2 on the wire.
    ReleaseFault,
    /// Answer a snapshot of what the node holds and owes.
    ///
    /// Position 3 on the wire.
    Snapshot,
    /// Seed this node's own durable facts toward `destination` as a proven carried path leaves
    /// them (connections.md section 7): `failed_attempts` Direct Failed observations through the
    /// writer's own observer path, then `Proxy Connected` through `carrier`, both naming the
    /// births this node's topology view holds now. Testkit only: the product writes a Proxy after
    /// cold discovery, which nothing in this repository runs.
    ///
    /// Position 4 on the wire.
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
    ///
    /// Position 5 on the wire.
    Dial {
        /// The `path.name` to ping.
        destination: String,
    },
    /// Open a data-plane stream (connections.md §5: `Connections::open`, `open_bi`) to the current
    /// birth of `destination` on this node's one registered ALPN, write `payload`, finish, and read
    /// what the far role answers. The reply names the connection's kind (Direct or Proxy) and peer.
    ///
    /// Position 6 on the wire.
    Stream {
        /// The `path.name` to stream to.
        destination: String,
        /// The bytes written.
        payload: Vec<u8>,
    },
    /// Mark this process's mesh transport stopped, exactly as iroh-gossip refusing a subscription
    /// does (`topology::mark_transport_stopped`): the node exits through the one transport-stopped
    /// exit, recording its reason in its data dir. The reply is sent before the mark.
    ///
    /// Position 7 on the wire.
    StopTransport {
        /// The reason the transport stopped.
        reason: String,
    },
    /// Block (`blocked: true`) or release this node's Direct dials toward `destination` on its one
    /// data-plane ALPN: a blocked dial answers `NotSent` and records Direct Failed, as an unreachable
    /// direct dial does (`connection_faults::block_direct`, connections.md §13 A, F, J).
    ///
    /// Position 8 on the wire.
    BlockDirect {
        /// The `path.name` whose Direct dials are blocked or released.
        destination: String,
        /// Block when true, release when false.
        blocked: bool,
    },
    /// Put this node's own config, the whole map, through its own `node.config.update`
    /// (`RunningNode::config_update`; node-config-update.md, a node's own update): answered with its
    /// mesh-primary's reply. Appended last.
    ///
    /// Position 9 on the wire.
    UpdateOwnConfig {
        /// The whole map.
        config: std::collections::BTreeMap<String, String>,
    },
    /// The data plane measured (load-testing.md M9): open this node's data-plane connection to the
    /// current birth of `destination` (Direct, or Proxy when its Direct dial is blocked), then
    /// `streams` bidirectional streams in turn on it, each writing `bytes` bytes generated here and
    /// reading the far role's echo whole. The reply carries the open's and the streams' times and the
    /// bytes moved both ways, never the bytes. Appended last.
    ///
    /// Position 10 on the wire.
    Throughput {
        /// The `path.name` to measure toward.
        destination: String,
        /// The streams opened in turn on the one connection.
        streams: u32,
        /// The bytes each stream writes (the echo answers them back), at most 1 MiB less the echo's prefix.
        bytes: u32,
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
    ///
    /// Position 0 on the wire.
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
        deleted: Option<String>,
        /// The call's outcome name.
        outcome: String,
        /// The proof-store reply, when the call replied.
        reply: Option<ProofReply>,
        /// The source's pooled connections after the call, by target path.
        pooled: Vec<String>,
    },
    /// The storage fault was armed.
    ///
    /// Position 1 on the wire.
    FaultArmed {
        /// Who armed it.
        by: AnsweredBy,
        /// Index writes still to refuse.
        refuse_index: u32,
        /// History appends still to refuse.
        refuse_history: u32,
    },
    /// The storage fault was released.
    ///
    /// Position 2 on the wire.
    FaultReleased {
        /// Who released it.
        by: AnsweredBy,
        /// The number of writes the fault refused.
        refused: u32,
    },
    /// What the node holds and owes.
    ///
    /// Position 3 on the wire.
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
        /// The Proxy deletions the node owes now.
        owed: Vec<NodeConnection>,
        /// The number of writes the fault has refused.
        fault_refused: u32,
    },
    /// The destination does not parse or does not resolve on this node.
    ///
    /// Position 4 on the wire.
    BadDestination {
        /// Who answered.
        by: AnsweredBy,
        /// Why the destination is refused.
        reason: String,
    },
    /// The peer the call needed could not be resolved.
    ///
    /// Position 5 on the wire.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The node is not ready to serve.
    ///
    /// Position 6 on the wire.
    NotReady {
        /// Why the node is not ready.
        reason: String,
    },
    /// The node is at its admission bound.
    ///
    /// Position 7 on the wire.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// The node is connections-deleting and takes no new work.
    ///
    /// Position 8 on the wire.
    ConnectionsDeleting {
        /// Why it refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 9 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    ///
    /// Position 10 on the wire.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
    /// The facts a `RecordProxy` wrote, with the births they name.
    ///
    /// Position 11 on the wire.
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
    ///
    /// Position 12 on the wire.
    Dialed {
        /// Who dialled.
        by: AnsweredBy,
        /// The destination's node id.
        destination_node_id: String,
        /// The Ping's outcome name.
        outcome: String,
    },
    /// The stream was opened, written and answered.
    ///
    /// Position 13 on the wire.
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
    ///
    /// Position 14 on the wire.
    StreamFailed {
        /// Who tried.
        by: AnsweredBy,
        /// Why, by name.
        reason: String,
    },
    /// The transport stop is marked; the process is exiting.
    ///
    /// Position 15 on the wire.
    TransportStopMarked {
        /// Who marked the stop.
        by: AnsweredBy,
        /// The reason recorded.
        reason: String,
    },
    /// The Direct dial block toward `destination` is set as asked.
    ///
    /// Position 16 on the wire.
    DirectBlocked {
        /// Who set it.
        by: AnsweredBy,
        /// The `path.name` the block names.
        destination: String,
        /// Whether Direct dials toward it are now blocked.
        blocked: bool,
    },
    /// The node's own config update answered by its mesh-primary: the reply by name, and the row as
    /// written it carries (its config and revision) where it carries one. Appended last.
    ///
    /// Position 17 on the wire.
    OwnConfigUpdated {
        /// The node that put it.
        by: AnsweredBy,
        /// The mesh-primary's reply by name, or how the call ended.
        outcome: String,
        /// The row's config the reply carries.
        config: std::collections::BTreeMap<String, String>,
        /// The row's revision the reply carries; 0 when it carries none.
        revision: u64,
    },
    /// The data plane measured toward `destination` ([`OriginateRequest::Throughput`]).
    ///
    /// Position 18 on the wire.
    Measured {
        /// Who answered.
        by: AnsweredBy,
        /// The connection's kind: `direct`, or `proxy via <carrier>`.
        kind: String,
        /// The open's time, microseconds.
        open_us: u64,
        /// The streams' time, every one in turn, microseconds.
        streams_us: u64,
        /// The bytes moved, written and read back, over every stream.
        bytes: u64,
    },
}

impl NodeProtocol for Originate {
    const OP: u8 = 0x73;
    const NAME: &'static str = "originate";
    const MAX_REQUEST_FRAME_BYTES: usize = 4096;
    const MAX_REPLY_FRAME_BYTES: usize = 65536;
    const FORWARDABLE: bool = false;
    type Request = OriginateRequest;
    type Reply = OriginateReply;

    rdm_core::protocol_variants! {
        request OriginateRequest { Call, ArmFault, ReleaseFault, Snapshot, RecordProxy, Dial, Stream, StopTransport, BlockDirect, UpdateOwnConfig, Throughput }
        reply OriginateReply {
            Called = "called" => ReplyKind::Success,
            FaultArmed = "fault-armed" => ReplyKind::Success,
            FaultReleased = "fault-released" => ReplyKind::Success,
            Snapshot = "snapshot" => ReplyKind::Success,
            BadDestination = "bad-destination" => ReplyKind::new(rdm_core::outcome::Code::InvalidArgument),
            @reserved,
            ProxyRecorded = "proxy-recorded" => ReplyKind::Success,
            Dialed = "dialed" => ReplyKind::Success,
            Streamed = "streamed" => ReplyKind::Success,
            StreamFailed = "stream-failed" => ReplyKind::Success,
            TransportStopMarked = "transport-stop-marked" => ReplyKind::Success,
            DirectBlocked = "direct-blocked" => ReplyKind::Success,
            OwnConfigUpdated = "own-config-updated" => ReplyKind::Success,
            Measured = "measured" => ReplyKind::Success,
        }
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
    /// This node's own config door, set once the node runs; unset, `UpdateOwnConfig` is refused by name.
    pub own_config: Arc<std::sync::OnceLock<rdm_node::node::OwnConfig>>,
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
                OriginateRequest::Throughput { destination, streams, bytes } => throughput(&by, &seams, &destination, streams, bytes).await,
                OriginateRequest::BlockDirect { destination, blocked } => block_direct(&by, &seams, &destination, blocked),
                OriginateRequest::UpdateOwnConfig { config } => match seams.own_config.get().cloned() {
                    None => OriginateReply::NotReady { reason: format!("{own}: the node does not run yet; its own config has no door") },
                    Some(door) => {
                        use rdm_core::status::StatusReply as R;
                        let (outcome, config, revision) = match door.update(config).await {
                            Ok(reply) => match &reply {
                                R::ConfigUpdated { config, revision, .. } | R::ConfigRowRefused { config, revision, .. } => (reply.name().to_string(), config.clone(), *revision),
                                other => (format!("{other:?}"), Default::default(), 0),
                            },
                            Err(end) => (format!("{end:?}"), Default::default(), 0),
                        };
                        OriginateReply::OwnConfigUpdated { by, outcome, config, revision }
                    }
                },
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
    // Any owed deletion is settled before a new call resolves its route (connections.md §10):
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
    // A Proxy the resolution found invalid is deleted by this source, as the caller of the seam.
    let deleted = match call.delete {
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
        deleted,
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

/// The data plane measured toward `destination`'s current birth: the open, then `streams` streams in
/// turn on the one connection, each `bytes` bytes written and their echo read whole.
async fn throughput(by: &AnsweredBy, seams: &Seams, destination: &str, streams: u32, bytes: u32) -> OriginateReply {
    let failed = |reason: String| OriginateReply::StreamFailed { by: by.clone(), reason };
    if bytes as usize > (1 << 20) - 256 {
        return failed(format!("{bytes} bytes a stream: the echo reads at most 1 MiB"));
    }
    let path: PathName = match destination.parse() {
        Ok(p) => p,
        Err(e) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination:?}: {e}") },
    };
    let target = match seams.resolver.resolve(&NodeTarget::CurrentPath(path)) {
        Ok(n) => n,
        Err(f) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination} does not resolve on this node: {f:?}") },
    };
    let Some(dialer) = seams.data_plane.get() else {
        return failed("no-data-plane-alpn: this node registered no data-plane ALPN".into());
    };
    let birth = rdm_transport::connections::Birth { name: target.name.clone(), node_id: target.node_id.clone(), incarnation: target.incarnation.clone() };
    let opened = std::time::Instant::now();
    let conn = match dialer.open(&birth).await {
        Ok(c) => c,
        Err(e) => return failed(format!("open: {e:?}")),
    };
    let open_us = opened.elapsed().as_micros() as u64;
    let payload = vec![b'x'; bytes as usize];
    let (mut moved, began) = (0u64, std::time::Instant::now());
    for i in 0..streams {
        let (mut send, mut recv) = match conn.open_bi().await {
            Ok(s) => s,
            Err(e) => return failed(format!("open_bi {i}: {e:?}")),
        };
        if let Err(e) = send.write_all(&payload).await {
            return failed(format!("write {i}: {e:?}"));
        }
        if let Err(e) = send.finish() {
            return failed(format!("finish {i}: {e:?}"));
        }
        match recv.read_to_end(1 << 20).await {
            Ok(b) => moved += payload.len() as u64 + b.len() as u64,
            Err(e) => return failed(format!("read {i}: {e:?}")),
        }
    }
    OriginateReply::Measured {
        by: by.clone(),
        kind: match conn.kind() { rdm_transport::connections::ConnectionKind::Direct => "direct".into(), rdm_transport::connections::ConnectionKind::Proxy { via } => format!("proxy via {}", via.name) },
        open_us,
        streams_us: began.elapsed().as_micros() as u64,
        bytes: moved,
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
    use rdm_core::outcome::Code;
    use rdm_core::protocol::DecodeFailure;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    /// Every variant's postcard bytes, decoded back, and its classification and name, against the
    /// pins taken from the hand-written declaration: one listing of every mismatch.
    fn pinned<P: NodeProtocol>(requests: &[(P::Request, &str)], replies: &[(P::Reply, &str, &str, Code)]) -> Vec<String>
    where
        P::Request: PartialEq + std::fmt::Debug,
        P::Reply: PartialEq + std::fmt::Debug,
    {
        let mut off = Vec::new();
        for (i, (r, want)) in requests.iter().enumerate() {
            let got = hex(&P::encode_request(r).expect("a request encodes"));
            if got != *want {
                off.push(format!("request {i}: {got} (pinned {want})"));
            }
            assert_eq!(P::decode_request(&P::encode_request(r).unwrap()).as_ref(), Ok(r), "request {i} decodes to itself");
        }
        for (i, (r, want, detail, code)) in replies.iter().enumerate() {
            let got = hex(&P::encode_reply(r).expect("a reply encodes"));
            if got != *want {
                off.push(format!("reply {i}: {got} (pinned {want})"));
            }
            assert_eq!(P::decode_reply(&P::encode_reply(r).unwrap()).as_ref(), Ok(r), "reply {i} decodes to itself");
            assert_eq!((P::reply_detail(r), P::classify_reply(r).code()), (*detail, *code), "reply {i}: its name and code");
        }
        assert_eq!((P::REQUEST_VARIANTS as usize, P::REPLY_VARIANTS as usize), (requests.len(), replies.len()), "one pin per variant");
        assert_eq!(P::decode_request(&[P::REQUEST_VARIANTS as u8]).err(), Some(DecodeFailure::UnknownVariant), "the request count is the first unknown position");
        assert_eq!(P::decode_reply(&[P::REPLY_VARIANTS as u8]).err(), Some(DecodeFailure::UnknownVariant), "the reply count is the first unknown position");
        off
    }

    /// CONTRACT: `Originate`'s wire is the one its hand-written declaration had: each request and
    /// reply variant encodes to the pinned bytes and decodes back, each reply keeps its name and code,
    /// the variant counts are 11 and 19, and the six reserved constructors build their own variants
    /// at positions 5 to 10, between the application's variants.
    #[test]
    fn originate_keeps_every_variant_on_its_pinned_bytes() {
        let s = || "r".to_string();
        let by = || AnsweredBy { node_id: "i".into(), node: "n".into(), incarnation_id: "c".into() };
        let requests = [
            (OriginateRequest::Call { destination: "d".into(), op: ProofOp::Put { key: vec![1], value: vec![2] } }, "0001640101010102"),
            (OriginateRequest::ArmFault { refuse_index: 1, refuse_history: 2, pass_history: 3 }, "01010203"),
            (OriginateRequest::ReleaseFault, "02"),
            (OriginateRequest::Snapshot, "03"),
            (OriginateRequest::RecordProxy { destination: "d".into(), carrier: "c".into(), failed_attempts: 2 }, "040164016302"),
            (OriginateRequest::Dial { destination: "d".into() }, "050164"),
            (OriginateRequest::Stream { destination: "d".into(), payload: vec![9] }, "0601640109"),
            (OriginateRequest::StopTransport { reason: s() }, "070172"),
            (OriginateRequest::BlockDirect { destination: "d".into(), blocked: true }, "08016401"),
            (OriginateRequest::UpdateOwnConfig { config: [("a".to_string(), "b".to_string())].into() }, "090101610162"),
            (OriginateRequest::Throughput { destination: "d".into(), streams: 2, bytes: 3 }, "0a01640203"),
        ];
        let replies = [
            (OriginateReply::Called { by: by(), destination_node_id: "d".into(), route: "direct".into(), carrier: None, deleted: Some("x".into()), outcome: "reply".into(), reply: None, pooled: vec!["p".into()] }, "000169016e016301640664697265637400010178057265706c7900010170", "called", Code::Ok),
            (OriginateReply::FaultArmed { by: by(), refuse_index: 1, refuse_history: 2 }, "010169016e01630102", "fault-armed", Code::Ok),
            (OriginateReply::FaultReleased { by: by(), refused: 4 }, "020169016e016304", "fault-released", Code::Ok),
            (OriginateReply::Snapshot { by: by(), own_active_proxies: vec![], own_latest_directs: vec![], active_len: 5, active_directs: vec![], owed: vec![], fault_refused: 6 }, "030169016e0163000005000006", "snapshot", Code::Ok),
            (OriginateReply::BadDestination { by: by(), reason: s() }, "040169016e01630172", "bad-destination", Code::InvalidArgument),
            (OriginateReply::PeerUnresolved { reason: s() }, "050172", "peer-unresolved", Code::NotFound),
            (OriginateReply::NotReady { reason: s() }, "060172", "not-ready", Code::Unavailable),
            (OriginateReply::Busy { reason: s() }, "070172", "busy", Code::Unavailable),
            (OriginateReply::ConnectionsDeleting { reason: s() }, "080172", "connections-deleting", Code::Unavailable),
            (OriginateReply::Malformed { kind: MalformedKind::Corrupt }, "0902", "malformed", Code::DataLoss),
            (OriginateReply::Unauthorized { reason: s() }, "0a0172", "unauthorized", Code::PermissionDenied),
            (OriginateReply::ProxyRecorded { by: by(), destination_node_id: "d".into(), destination_incarnation: "e".into(), carrier_node_id: "c".into(), carrier_incarnation: "f".into(), failed_attempts: 2 }, "0b0169016e0163016401650163016602", "proxy-recorded", Code::Ok),
            (OriginateReply::Dialed { by: by(), destination_node_id: "d".into(), outcome: "o".into() }, "0c0169016e01630164016f", "dialed", Code::Ok),
            (OriginateReply::Streamed { by: by(), peer: by(), kind: "direct".into(), connection_id: "x".into(), answered: vec![8] }, "0d0169016e01630169016e01630664697265637401780108", "streamed", Code::Ok),
            (OriginateReply::StreamFailed { by: by(), reason: s() }, "0e0169016e01630172", "stream-failed", Code::Ok),
            (OriginateReply::TransportStopMarked { by: by(), reason: s() }, "0f0169016e01630172", "transport-stop-marked", Code::Ok),
            (OriginateReply::DirectBlocked { by: by(), destination: "d".into(), blocked: false }, "100169016e0163016400", "direct-blocked", Code::Ok),
            (OriginateReply::OwnConfigUpdated { by: by(), outcome: "o".into(), config: [("a".to_string(), "b".to_string())].into(), revision: 9 }, "110169016e0163016f010161016209", "own-config-updated", Code::Ok),
            (OriginateReply::Measured { by: by(), kind: "k".into(), open_us: 1, streams_us: 2, bytes: 3 }, "120169016e0163016b010203", "measured", Code::Ok),
        ];
        let off = pinned::<Originate>(&requests, &replies);
        assert!(off.is_empty(), "the wire moved:\n{}", off.join("\n"));
        assert_eq!(
            [Originate::peer_unresolved(s()), Originate::not_ready(s()), Originate::busy(s()), Originate::connections_deleting(s()), Originate::malformed(MalformedKind::Corrupt), Originate::unauthorized(s())],
            [replies[5].0.clone(), replies[6].0.clone(), replies[7].0.clone(), replies[8].0.clone(), replies[9].0.clone(), replies[10].0.clone()],
            "each reserved constructor builds its own variant"
        );
    }
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
    /// route stays Direct, the Proxy is not deleted) under the default rpc_node policy.
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
        assert_eq!(r.delete, None);
        let d = resolve(&held, &own.name, &dest.name, POLICY);
        assert_eq!(d.route, EffectiveRoute::Direct { known: false }, "the gateway-carried Proxy is not the route under the rpc_node policy: with no Direct fact the pair dials directly");
        assert_eq!(d.delete, None, "a Proxy through a kind the policy does not name is skipped, never deleted");
    }
}
