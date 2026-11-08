//! The originate service: a testkit-only protocol on op `0x73` that asks the executing node to
//! make a testkit-store call of its own to a destination over its own held connections projection
//! (`NodeRpcClient::call_connected`, connections.md §5), to arm or release its storage fault
//! ([`crate::faults`]), and to answer a copy of what it holds and owes. A scenario thereby
//! proves CONN-B, CONN-D and CONN-G on a real, restartable node: the node is the source, its
//! writer records the facts, its storage is the one hydrated after a restart. Testkit range only
//! ([`TESTKIT_OPS`]); never a product binary.
//!
//! [`TESTKIT_OPS`]: rdm_core::catalog::TESTKIT_OPS

use crate::faults::StorageFault;
use crate::testkit_store::{TestkitStoreReply, TestkitStoreRequest, TestkitStore};
use rdm_core::connections::{resolve, CarrierPolicy, ConnectionState, NodeConnection};
use rdm_core::launch::Launch;
use rdm_core::{PathName};
use rdm_node::connections_writer::ConnectionsWriter;
use rdm_transport::{CallOptions, LiveNodeResolver, NodeResolver, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The carrier policy the testkit applies to its testkit-store calls: proxied through an rpc
/// node (the product's own policies are the product's).
pub fn policy() -> CarrierPolicy {
    CarrierPolicy::Proxy { carrier_type: "node".into() }
}

/// The originate service protocol on op `0x73`.
pub struct Originate;

/// A testkit-store operation the originating node makes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OriginateStoreOp {
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

/// An originate service call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OriginateRequest {
    /// Call `destination` (a node ID) with `op`, over this node's held projection.
    ///
    /// Position 0 on the wire.
    Call {
        /// The node ID to call.
        destination: rdm_core::NodeId,
        /// The testkit-store operation to make.
        op: OriginateStoreOp,
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
    /// Answer a copy of what the node holds and owes.
    ///
    /// Position 3 on the wire.
    Copy,
    /// Seed this node's own durable facts toward `destination` as a proven carried path leaves
    /// them (connections.md section 7): `failed_attempts` Direct Failed observations through the
    /// writer's own observer path, then `Proxy Connected` through `carrier`, both naming the
    /// births this node's topology view holds now. Testkit only: the product writes a Proxy after
    /// cold discovery, which nothing in this repository runs.
    ///
    /// Position 4 on the wire.
    RecordProxy {
        /// The node ID the facts concern.
        destination: rdm_core::NodeId,
        /// The node ID of the carrier.
        carrier: rdm_core::NodeId,
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
        /// The node ID to ping.
        destination: rdm_core::NodeId,
    },
    /// Open a data-plane stream (connections.md §5: `Connections::open`, `open_bi`) to the current
    /// birth of `destination` on this node's one registered ALPN, write `payload`, finish, and read
    /// what the far role answers. The reply names the connection's kind (Direct or Proxy) and peer.
    ///
    /// Position 6 on the wire.
    Stream {
        /// The node ID to stream to.
        destination: rdm_core::NodeId,
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
        /// The node ID whose Direct dials are blocked or released.
        destination: rdm_core::NodeId,
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
        /// The node ID to measure toward.
        destination: rdm_core::NodeId,
        /// The streams opened in turn on the one connection.
        streams: u32,
        /// The bytes each stream writes (the echo answers them back), at most 1 MiB less the echo's prefix.
        bytes: u32,
    },
    /// Publish one event on the testkit application's channel ([`crate::test_app::APP_CHANNEL`],
    /// gossip.md §8) through this process's publisher ([`crate::test_app::app_channel_publisher`]), in
    /// the service's serve span: the event's `subject`, `sequence` and `data` as given. Appended last.
    ///
    /// Position 11 on the wire.
    Publish {
        /// The event's subject.
        subject: String,
        /// The event's sequence.
        sequence: u64,
        /// The event's data.
        data: Vec<u8>,
    },
    /// Publish `count` messages of `size` bytes each on the testkit application's channel, one after
    /// another, from inside this node (load-testing.md M11: the gossip load, never one Node RPC per
    /// message): subjects `<prefix>/<i>`, sequences `i`. Each publish has its own channel span. Appended
    /// last.
    ///
    /// Position 12 on the wire.
    PublishBurst {
        /// The messages' subject prefix.
        prefix: String,
        /// How many messages.
        count: u32,
        /// Each message's data, in bytes.
        size: u32,
    },
}

/// Who answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnsweredBy {
    /// The answering node's id.
    pub node_id: String,
    /// The answering node's separate path.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// The answering node's incarnation id.
    pub incarnation_id: String,
}

impl AnsweredBy {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

/// An originate service answer.
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
        carrier: Option<rdm_core::NodeId>,
        /// The own Proxy the resolution found invalid, and why, if any.
        deleted: Option<String>,
        /// The call's outcome name.
        outcome: String,
        /// The testkit-store reply, when the call replied.
        reply: Option<TestkitStoreReply>,
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
    Copy {
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
        /// The node ID the block names.
        destination: rdm_core::NodeId,
        /// Whether Direct dials toward it are now blocked.
        blocked: bool,
    },
    /// The node's own config update answered by its property writer: the reply by name, and the
    /// config it carries where it carries one. Appended last.
    ///
    /// Position 17 on the wire.
    OwnConfigUpdated {
        /// The node that put it.
        by: AnsweredBy,
        /// The mesh-primary's reply by name, or how the call ended.
        outcome: String,
        /// The config the reply carries.
        config: std::collections::BTreeMap<String, String>,
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
    /// The event [`OriginateRequest::Publish`] named, handed to the channel's gossip or refused.
    ///
    /// Position 19 on the wire.
    Published {
        /// Who published it.
        by: AnsweredBy,
        /// `handed-to-gossip`, or the channel's refusal by name.
        outcome: String,
    },
    /// The burst [`OriginateRequest::PublishBurst`] named: how many were handed to gossip, how many
    /// the channel refused, and the first refusal by name.
    ///
    /// Position 20 on the wire.
    PublishedBurst {
        /// Who published them.
        by: AnsweredBy,
        /// Messages handed to the channel's gossip.
        published: u32,
        /// Messages the channel refused.
        refused: u32,
        /// The first refusal, by name; empty when none.
        first_refusal: String,
        /// The burst's time inside the node, microseconds.
        elapsed_us: u64,
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
        request OriginateRequest { Call, ArmFault, ReleaseFault, Copy, RecordProxy, Dial, Stream, StopTransport, BlockDirect, UpdateOwnConfig, Throughput, Publish, PublishBurst }
        reply OriginateReply {
            Called = "called" => ReplyKind::Success,
            FaultArmed = "fault-armed" => ReplyKind::Success,
            FaultReleased = "fault-released" => ReplyKind::Success,
            Copy = "copy" => ReplyKind::Success,
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
            Published = "published" => ReplyKind::Success,
            PublishedBurst = "published-burst" => ReplyKind::Success,
        }
    }
}

/// The process seams the originate service acts through.
#[derive(Clone)]
pub struct Seams {
    /// The resolver the originate service acts through.
    pub resolver: Arc<LiveNodeResolver>,
    /// The client the originate service calls through.
    pub client: Arc<NodeRpcClient>,
    /// The connections writer the originate service records through.
    pub connections: Arc<ConnectionsWriter>,
    /// The storage fault the originate service arms and releases.
    pub fault: Arc<StorageFault>,
    /// The name this birth holds now: every service names the node by it, per call.
    pub name: crate::held_name::HeldName,
    /// The dial half of this node's one data-plane ALPN, set once the node runs
    /// (`node::start_with_seams_and_alpn`); unset, `Stream` is refused by name.
    pub data_plane: Arc<std::sync::OnceLock<rdm_transport::connections::Dialer>>,
    /// This node's data-plane connections registry and the name of its one data-plane ALPN, set
    /// with [`Self::data_plane`]: the testkit's Direct dial block acts on it (`BlockDirect`).
    pub data_plane_registry: Arc<std::sync::OnceLock<(rdm_transport::connections::ConnectionsRegistry, Vec<u8>)>>,
    /// This node's own `OwnConfig`, set once the node runs; unset, `UpdateOwnConfig` is refused by name.
    pub own_config: Arc<std::sync::OnceLock<rdm_node::node::OwnConfig>>,
}

/// Serve the originate service on this node's own seams, with the testkit's default carrier policy ([`policy`]).
pub fn serve(b: ServerBuilder, seams: Seams, launch: &Launch) -> ServerBuilder {
    serve_with_policy(b, seams, launch, policy())
}

/// Serve the originate service with the carrier policy the serving application declares: a Proxy held for a
/// destination is the effective route only through a carrier of `policy`'s kind
/// (`rdm_core::connections::resolve`). A shape whose carriers are not `rpc_node`s (a
/// gateway carrying for a compute) declares its own kind here.
pub fn serve_with_policy(b: ServerBuilder, seams: Seams, launch: &Launch, policy: CarrierPolicy) -> ServerBuilder {
    let (node_id, incarnation_id) = (launch.node_id.to_string(), launch.incarnation.to_string());
    let launch_node_id = launch.node_id.clone();
    b.serve::<Originate, _, _>(OpOwner::Testkit, move |_peer: PeerContext, req: OriginateRequest| {
        // The node answers and routes under the name it holds at this call (R12).
        let own: PathName = seams.name.now();
        let by = AnsweredBy { node_id: node_id.clone(), path: own.path.clone(), name: own.name.clone(), incarnation_id: incarnation_id.clone() };
        let seams = seams.clone();
        let launch_node_id = launch_node_id.clone();
        let policy = policy.clone();
        async move {
            Ok(match req {
                OriginateRequest::Call { destination, op } => call(&by, &seams, &launch_node_id, &destination, op, policy).await,
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
                    None => OriginateReply::NotReady { reason: format!("{own:?}: the node does not run yet; its own config is not installed yet") },
                    Some(own) => {
                        use rdm_core::status::StatusReply as R;
                        let (outcome, config) = match own.update(config).await {
                            Ok(reply) => match &reply {
                                R::ConfigUpdated { config, .. } => (reply.name().to_string(), config.clone()),
                                R::ConfigRowRefused { .. } => (reply.name().to_string(), Default::default()),
                                other => (format!("{other:?}"), Default::default()),
                            },
                            Err(end) => (format!("{end:?}"), Default::default()),
                        };
                        OriginateReply::OwnConfigUpdated { by, outcome, config }
                    }
                },
                OriginateRequest::Publish { subject, sequence, data } => match crate::test_app::app_channel_publisher() {
                    None => OriginateReply::NotReady { reason: format!("{own:?}: this node joined no {} channel", crate::test_app::APP_CHANNEL) },
                    Some(publisher) => {
                        let event = rdm_gossip::app::Event { subject, sequence: Some(sequence), dataschema: Some("testkit.app.v1".into()), data: data.into() };
                        let outcome = match publisher.publish(event).await {
                            Ok(()) => "handed-to-gossip".to_string(),
                            Err(e) => e.to_string(),
                        };
                        OriginateReply::Published { by, outcome }
                    }
                },
                OriginateRequest::PublishBurst { prefix, count, size } => match crate::test_app::app_channel_publisher() {
                    None => OriginateReply::NotReady { reason: format!("{own:?}: this node joined no {} channel", crate::test_app::APP_CHANNEL) },
                    Some(publisher) => {
                        let started = std::time::Instant::now();
                        let (mut published, mut refused, mut first_refusal) = (0u32, 0u32, String::new());
                        for i in 0..count {
                            let event = rdm_gossip::app::Event { subject: format!("{prefix}/{i}"), sequence: Some(i as u64), dataschema: Some("testkit.app.v1".into()), data: vec![b'x'; size as usize].into() };
                            match publisher.publish(event).await {
                                Ok(()) => published += 1,
                                Err(e) => {
                                    refused += 1;
                                    if first_refusal.is_empty() {
                                        first_refusal = e.to_string();
                                    }
                                }
                            }
                        }
                        OriginateReply::PublishedBurst { by, published, refused, first_refusal, elapsed_us: started.elapsed().as_micros() as u64 }
                    }
                },
                OriginateRequest::StopTransport { reason } => {
                    // The mark runs after this reply is on its way: the exit it causes is the real one.
                    let marked = reason.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        rdm_gossip::channel::mark_transport_stopped(marked);
                    });
                    OriginateReply::TransportStopMarked { by, reason }
                }
                OriginateRequest::Copy => {
                    let held = seams.connections.held();
                    let held = held.lock().unwrap();
                    OriginateReply::Copy {
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

async fn call(by: &AnsweredBy, seams: &Seams, own: &rdm_core::NodeId, destination: &rdm_core::NodeId, op: OriginateStoreOp, policy: CarrierPolicy) -> OriginateReply {
    let target = match seams.resolver.resolve(&NodeTarget::ExactNode(destination.clone())) {
        Ok(n) => n,
        Err(f) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination} does not resolve on this node: {f:?}") },
    };
    // Any owed deletion is settled before a new call resolves its route (connections.md §10):
    // a refused write leaves the Proxy effective, by name.
    let _ = seams.connections.settle_owed().await;
    let resolution = {
        let held = seams.connections.held();
        let held = held.lock().unwrap();
        resolve(&held, own, &target.node_id, policy)
    };
    let req = match op {
        OriginateStoreOp::Get { key } => TestkitStoreRequest::Get { key },
        OriginateStoreOp::Put { key, value } => TestkitStoreRequest::Put { key, value },
    };
    let call = seams.client.call_resolved::<TestkitStore>(resolution, own, &target.location(), &target.node_id, &req, &CallOptions::default()).await;
    // Each Proxy the resolution found invalid is deleted by this source, as the caller of the seam;
    // the reply names the first, the one toward this destination when it was among them.
    let deleted = call.delete.first().map(|(_, why)| why.to_string());
    for (proxy, why) in call.delete {
        let row = NodeConnection { id: rdm_core::ConnectionId::mint(), state: ConnectionState::Disconnected, reason: Some(why.to_string()), logged_at_ms: now_ms().max(proxy.logged_at_ms + 1), ..proxy };
        let _ = seams.connections.record(row).await;
    }
    let (route, carrier) = match &call.route {
        rdm_core::connections::EffectiveRoute::ViaPeer { proxy, .. } => ("via-peer".to_string(), proxy.carrier.as_ref().map(|n| n.node_id.clone())),
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

async fn dial(by: &AnsweredBy, seams: &Seams, destination: &rdm_core::NodeId) -> OriginateReply {
    use rdm_core::ping::{Ping, PingRequest};
    let target = match seams.resolver.resolve(&NodeTarget::ExactNode(destination.clone())) {
        Ok(n) => n,
        Err(f) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination} does not resolve on this node: {f:?}") },
    };
    let (out, _) = seams.client.call::<Ping>(&NodeTarget::ExactNode(target.node_id.clone()), &PingRequest::Ping { payload: b"dial".to_vec() }, &CallOptions::default()).await;
    OriginateReply::Dialed { by: by.clone(), destination_node_id: target.node_id.to_string(), outcome: out.name().to_string() }
}

/// One data-plane round trip to `destination`'s current birth: open on the node's ALPN, one
/// bidirectional stream, the payload written and finished, the far role's answer read whole.
async fn stream(by: &AnsweredBy, seams: &Seams, destination: &rdm_core::NodeId, payload: Vec<u8>) -> OriginateReply {
    let failed = |reason: String| OriginateReply::StreamFailed { by: by.clone(), reason };
    let target = match seams.resolver.resolve(&NodeTarget::ExactNode(destination.clone())) {
        Ok(n) => n,
        Err(f) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination} does not resolve on this node: {f:?}") },
    };
    let Some(dialer) = seams.data_plane.get() else {
        return failed("no-data-plane-alpn: this node registered no data-plane ALPN".into());
    };
    let birth = rdm_transport::connections::Birth { mesh: target.mesh.clone(), r#type: target.r#type.clone(), path: target.path.clone(), name: target.name.clone(), node_id: target.node_id.clone(), incarnation: target.incarnation.clone() };
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
        peer: AnsweredBy { node_id: peer.node_id.to_string(), path: peer.path.clone(), name: peer.name.clone(), incarnation_id: peer.incarnation.to_string() },
        kind: match conn.kind() { rdm_transport::connections::ConnectionKind::Direct => "direct".into(), rdm_transport::connections::ConnectionKind::Proxy { via } => format!("proxy via {}", via.node_id) },
        connection_id: conn.id().to_string(),
        answered,
    }
}

/// The data plane measured toward `destination`'s current birth: the open, then `streams` streams in
/// turn on the one connection, each `bytes` bytes written and their echo read whole.
async fn throughput(by: &AnsweredBy, seams: &Seams, destination: &rdm_core::NodeId, streams: u32, bytes: u32) -> OriginateReply {
    let failed = |reason: String| OriginateReply::StreamFailed { by: by.clone(), reason };
    if bytes as usize > (1 << 20) - 256 {
        return failed(format!("{bytes} bytes a stream: the echo reads at most 1 MiB"));
    }
    let target = match seams.resolver.resolve(&NodeTarget::ExactNode(destination.clone())) {
        Ok(n) => n,
        Err(f) => return OriginateReply::BadDestination { by: by.clone(), reason: format!("{destination} does not resolve on this node: {f:?}") },
    };
    let Some(dialer) = seams.data_plane.get() else {
        return failed("no-data-plane-alpn: this node registered no data-plane ALPN".into());
    };
    let birth = rdm_transport::connections::Birth { mesh: target.mesh.clone(), r#type: target.r#type.clone(), path: target.path.clone(), name: target.name.clone(), node_id: target.node_id.clone(), incarnation: target.incarnation.clone() };
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
        kind: match conn.kind() { rdm_transport::connections::ConnectionKind::Direct => "direct".into(), rdm_transport::connections::ConnectionKind::Proxy { via } => format!("proxy via {}", via.node_id) },
        open_us,
        streams_us: began.elapsed().as_micros() as u64,
        bytes: moved,
    }
}

/// Block or release this node's Direct dials toward `destination` on its one data-plane ALPN.
fn block_direct(by: &AnsweredBy, seams: &Seams, destination: &rdm_core::NodeId, blocked: bool) -> OriginateReply {
    let Some((registry, alpn)) = seams.data_plane_registry.get() else {
        return OriginateReply::StreamFailed { by: by.clone(), reason: "no-data-plane-alpn: this node registered no data-plane ALPN".into() };
    };
    if blocked {
        registry.block_direct_dial(destination, alpn);
    } else {
        registry.unblock_direct_dial(destination, alpn);
    }
    OriginateReply::DirectBlocked { by: by.clone(), destination: destination.clone(), blocked }
}

async fn record_proxy(by: &AnsweredBy, seams: &Seams, destination: &rdm_core::NodeId, carrier: &rdm_core::NodeId, failed_attempts: u32) -> OriginateReply {
    use rdm_core::connections::{ConnectionEnd, ConnectionKind};
    use rdm_transport::ConnectionObserver;
    let bad = |reason: String| OriginateReply::BadDestination { by: by.clone(), reason };
    let mut ends = Vec::new();
    for what in [destination, carrier] {
        match seams.resolver.resolve(&NodeTarget::ExactNode(what.clone())) {
            Ok(n) => ends.push(n),
            Err(f) => return bad(format!("{what} does not resolve on this node: {f:?}")),
        }
    }
    let (dest, via) = (ends.remove(0), ends.remove(0));
    for _ in 0..failed_attempts {
        seams.connections.direct_failed(&dest, "testkit seed: the direct dial ended NotSent");
    }
    seams.connections.observations_landed().await;
    let end = |n: &rdm_transport::ResolvedNode| ConnectionEnd { mesh: n.mesh.clone(), r#type: n.r#type.clone(), path: n.path.clone(), name: n.name.clone(), node_id: n.node_id.clone(), incarnation: Some(n.incarnation.clone()) };
    let row = NodeConnection {
        id: rdm_core::ConnectionId::mint(),
        source: seams.connections.own(),
        destination: end(&dest),
        kind: ConnectionKind::Proxy,
        alpn: rdm_core::connections::ALPN.to_vec(),
        state: ConnectionState::Connected,
        carrier: Some(end(&via)),
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
    /// the variant counts are 12 and 20, and the six reserved constructors build their own variants
    /// at positions 5 to 10, between the application's variants.
    #[test]
    fn originate_keeps_every_variant_on_its_pinned_bytes() {
        let s = || "r".to_string();
        let by = || AnsweredBy { node_id: "i".into(), path: "p".into(), name: "n".into(), incarnation_id: "c".into() };
        let requests = [
            (OriginateRequest::Call { destination: rdm_core::NodeId::parse("dddddddddddd").unwrap(), op: OriginateStoreOp::Put { key: vec![1], value: vec![2] } }, "000c6464646464646464646464640101010102"),
            (OriginateRequest::ArmFault { refuse_index: 1, refuse_history: 2, pass_history: 3 }, "01010203"),
            (OriginateRequest::ReleaseFault, "02"),
            (OriginateRequest::Copy, "03"),
            (OriginateRequest::RecordProxy { destination: rdm_core::NodeId::parse("dddddddddddd").unwrap(), carrier: rdm_core::NodeId::parse("cccccccccccc").unwrap(), failed_attempts: 2 }, "040c6464646464646464646464640c63636363636363636363636302"),
            (OriginateRequest::Dial { destination: rdm_core::NodeId::parse("dddddddddddd").unwrap() }, "050c646464646464646464646464"),
            (OriginateRequest::Stream { destination: rdm_core::NodeId::parse("dddddddddddd").unwrap(), payload: vec![9] }, "060c6464646464646464646464640109"),
            (OriginateRequest::StopTransport { reason: s() }, "070172"),
            (OriginateRequest::BlockDirect { destination: rdm_core::NodeId::parse("dddddddddddd").unwrap(), blocked: true }, "080c64646464646464646464646401"),
            (OriginateRequest::UpdateOwnConfig { config: [("a".to_string(), "b".to_string())].into() }, "090101610162"),
            (OriginateRequest::Throughput { destination: rdm_core::NodeId::parse("dddddddddddd").unwrap(), streams: 2, bytes: 3 }, "0a0c6464646464646464646464640203"),
            (OriginateRequest::Publish { subject: "s".into(), sequence: 2, data: vec![9] }, "0b0173020109"),
            (OriginateRequest::PublishBurst { prefix: "p".into(), count: 2, size: 3 }, "0c01700203"),
        ];
        let replies = [
            (OriginateReply::Called { by: by(), destination_node_id: "d".into(), route: "direct".into(), carrier: None, deleted: Some("x".into()), outcome: "reply".into(), reply: None, pooled: vec!["p".into()] }, "0001690170016e016301640664697265637400010178057265706c7900010170", "called", Code::Ok),
            (OriginateReply::FaultArmed { by: by(), refuse_index: 1, refuse_history: 2 }, "0101690170016e01630102", "fault-armed", Code::Ok),
            (OriginateReply::FaultReleased { by: by(), refused: 4 }, "0201690170016e016304", "fault-released", Code::Ok),
            (OriginateReply::Copy { by: by(), own_active_proxies: vec![], own_latest_directs: vec![], active_len: 5, active_directs: vec![], owed: vec![], fault_refused: 6 }, "0301690170016e0163000005000006", "copy", Code::Ok),
            (OriginateReply::BadDestination { by: by(), reason: s() }, "0401690170016e01630172", "bad-destination", Code::InvalidArgument),
            (OriginateReply::PeerUnresolved { reason: s() }, "050172", "peer-unresolved", Code::NotFound),
            (OriginateReply::NotReady { reason: s() }, "060172", "not-ready", Code::Unavailable),
            (OriginateReply::Busy { reason: s() }, "070172", "busy", Code::Unavailable),
            (OriginateReply::ConnectionsDeleting { reason: s() }, "080172", "connections-deleting", Code::Unavailable),
            (OriginateReply::Malformed { kind: MalformedKind::Corrupt }, "0902", "malformed", Code::DataLoss),
            (OriginateReply::Unauthorized { reason: s() }, "0a0172", "unauthorized", Code::PermissionDenied),
            (OriginateReply::ProxyRecorded { by: by(), destination_node_id: "d".into(), destination_incarnation: "e".into(), carrier_node_id: "c".into(), carrier_incarnation: "f".into(), failed_attempts: 2 }, "0b01690170016e0163016401650163016602", "proxy-recorded", Code::Ok),
            (OriginateReply::Dialed { by: by(), destination_node_id: "d".into(), outcome: "o".into() }, "0c01690170016e01630164016f", "dialed", Code::Ok),
            (OriginateReply::Streamed { by: by(), peer: by(), kind: "direct".into(), connection_id: "x".into(), answered: vec![8] }, "0d01690170016e016301690170016e01630664697265637401780108", "streamed", Code::Ok),
            (OriginateReply::StreamFailed { by: by(), reason: s() }, "0e01690170016e01630172", "stream-failed", Code::Ok),
            (OriginateReply::TransportStopMarked { by: by(), reason: s() }, "0f01690170016e01630172", "transport-stop-marked", Code::Ok),
            (OriginateReply::DirectBlocked { by: by(), destination: rdm_core::NodeId::parse("dddddddddddd").unwrap(), blocked: false }, "1001690170016e01630c64646464646464646464646400", "direct-blocked", Code::Ok),
            (OriginateReply::OwnConfigUpdated { by: by(), outcome: "o".into(), config: [("a".to_string(), "b".to_string())].into() }, "1101690170016e0163016f0101610162", "own-config-updated", Code::Ok),
            (OriginateReply::Measured { by: by(), kind: "k".into(), open_us: 1, streams_us: 2, bytes: 3 }, "1201690170016e0163016b010203", "measured", Code::Ok),
            (OriginateReply::Published { by: by(), outcome: "o".into() }, "1301690170016e0163016f", "published", Code::Ok),
            (OriginateReply::PublishedBurst { by: by(), published: 1, refused: 2, first_refusal: "f".into(), elapsed_us: 3 }, "1401690170016e01630102016603", "published-burst", Code::Ok),
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

    fn end(r#type: &str, ordinal: u32, inc: &str) -> ConnectionEnd {
        ConnectionEnd { mesh: "mesh1".into(), r#type: r#type.to_string(), path: format!("{}.{}", "mesh1", r#type), name: (ordinal).to_string(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::parse(&rdm_core::fixture_crockford60(inc)).unwrap()) }
    }

    fn row(source: &ConnectionEnd, destination: &ConnectionEnd, kind: ConnectionKind, carrier: Option<&ConnectionEnd>, at: u64) -> NodeConnection {
        NodeConnection { id: rdm_core::ConnectionId::mint(), source: source.clone(), destination: destination.clone(), kind, alpn: rdm_core::connections::ALPN.to_vec(), state: ConnectionState::Connected, carrier: carrier.cloned(), reason: None, logged_at_ms: at }
    }

    /// CONTRACT: the originate service resolves a held Proxy under the carrier policy it is served with. A
    /// Proxy through a gateway is the effective route under a gateway policy, and is skipped (the
    /// route stays Direct, the Proxy is not deleted) under the default rpc_node policy.
    #[test]
    fn originate_resolves_a_gateway_carried_proxy_only_under_a_gateway_policy() {
        let (own, carrier, dest) = (end("compute", 1, "o1"), end("gateway", 1, "c1"), end("broker", 1, "d1"));
        let mut held = ConnectionsHeld::new();
        held.set_own_source(own.node_id.clone());
        held.mark_complete();
        held.apply(row(&carrier, &dest, ConnectionKind::Direct, None, 10)).unwrap();
        let p = row(&own, &dest, ConnectionKind::Proxy, Some(&carrier), 20);
        held.apply(p.clone()).unwrap();
        let gateway = CarrierPolicy::Proxy { carrier_type: "gateway".into() };
        let r = resolve(&held, &own.node_id, &dest.node_id, gateway);
        assert_eq!(r.route, EffectiveRoute::ViaPeer { carrier: carrier.node_id.clone(), proxy: p });
        assert!(r.delete.is_empty());
        let d = resolve(&held, &own.node_id, &dest.node_id, policy());
        assert_eq!(d.route, EffectiveRoute::Direct { known: false }, "the gateway-carried Proxy is not the route under the rpc_node policy: with no Direct fact the pair dials directly");
        assert!(d.delete.is_empty(), "a Proxy through a kind the policy does not name is skipped, never deleted");
    }
}
