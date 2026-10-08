//! The originate door: a testkit-only protocol on op `0x73` that asks the executing node to
//! make a proof-store call of its own to a destination over its own held connections projection
//! (`NodeRpcClient::call_connected`, connections.md §5), to arm or release its storage fault
//! ([`crate::faults`]), and to answer a snapshot of what it holds and owes. A scenario thereby
//! proves CONN-B, CONN-D and CONN-G on a real, restartable node: the node is the source, its
//! writer records the facts, its storage is the one hydrated after a restart. Testkit range only
//! ([`TESTKIT_OPS`]); never a product binary.
//!
//! [`TESTKIT_OPS`]: rafka_node_rpc_contract::catalog::TESTKIT_OPS

use crate::faults::StorageFault;
use crate::proof_store::{ProofReply, ProofRequest, ProofStore};
use rafka_mesh_entity::connections::{resolve, CarrierPolicy, ConnectionState, NodeConnection};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::{NodeKind, PathName};
use rafka_node_admin_core::connections_writer::ConnectionsWriter;
use rafka_node_rpc::{CallOptions, LiveNodeResolver, NodeResolver, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The carrier policy the testkit applies to its proof-store calls: forwardable through an rpc
/// node (the product's own policies are the product's).
pub const POLICY: CarrierPolicy = CarrierPolicy::Forwardable { carrier_kind: NodeKind::RpcNode };

pub struct Originate;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProofOp {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OriginateRequest {
    /// Call `destination` (a path) with `op`, over this node's held projection.
    Call { destination: String, op: ProofOp },
    /// Refuse the next `refuse_index` index writes and `refuse_history` history appends, after
    /// letting `pass_history` history appends through.
    ArmFault { refuse_index: u32, refuse_history: u32, pass_history: u32 },
    ReleaseFault,
    Snapshot,
    /// Seed this node's own durable facts toward `destination` as a proven carried path leaves
    /// them (connections.md section 7): `failed_attempts` Direct Failed observations through the
    /// writer's own observer path, then `Proxy Connected` through `carrier`, both naming the
    /// births this node's membership holds now. Testkit only: the product writes a Proxy after
    /// cold discovery, which nothing in this repository runs.
    RecordProxy { destination: String, carrier: String, failed_attempts: u32 },
    /// One direct core Ping to `destination`, outside route resolution, as a node's own
    /// background traffic reaches a peer: the pooled connection it opens is reported to the
    /// destination's writer as Direct Connected from the destination's side (accepted) and to
    /// this node's writer as its own dial.
    Dial { destination: String },
    /// Mark this process's mesh transport stopped, exactly as iroh-gossip refusing a subscription
    /// does (`membership::mark_transport_stopped`): the node exits through the one transport-stopped
    /// exit, recording its reason in its data dir. The reply is sent before the mark.
    StopTransport { reason: String },
}

/// Who answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnsweredBy {
    pub node_id: String,
    pub node: String,
    pub incarnation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OriginateReply {
    Called {
        by: AnsweredBy,
        destination_node_id: String,
        /// The effective route's token: `via-peer`, `direct`, `direct-unknown` or `no-active-route`.
        route: String,
        carrier: Option<String>,
        /// The own Proxy the resolution found invalid, and why, if any.
        retired: Option<String>,
        outcome: String,
        reply: Option<ProofReply>,
        /// The source's pooled connections after the call, by target path.
        pooled: Vec<String>,
    },
    FaultArmed { by: AnsweredBy, refuse_index: u32, refuse_history: u32 },
    FaultReleased { by: AnsweredBy, refused: u32 },
    Snapshot { by: AnsweredBy, own_active_proxies: Vec<NodeConnection>, own_latest_directs: Vec<NodeConnection>, active_len: usize, owed: Vec<NodeConnection>, fault_refused: u32 },
    /// The destination does not parse or does not resolve on this node.
    BadDestination { by: AnsweredBy, reason: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
    /// The facts a `RecordProxy` wrote, with the births they name.
    ProxyRecorded { by: AnsweredBy, destination_node_id: String, destination_incarnation: String, carrier_node_id: String, carrier_incarnation: String, failed_attempts: u32 },
    /// The answer of a `Dial`: the typed outcome name of the one Ping.
    Dialed { by: AnsweredBy, destination_node_id: String, outcome: String },
    /// The transport stop is marked; the process is exiting.
    TransportStopMarked { by: AnsweredBy, reason: String },
}

impl NodeProtocol for Originate {
    const OP: u8 = 0x73;
    const NAME: &'static str = "originate";
    const MAX_REQUEST_FRAME_BYTES: usize = 4096;
    const MAX_REPLY_FRAME_BYTES: usize = 65536;
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 7;
    const REPLY_VARIANTS: u32 = 14;
    type Request = OriginateRequest;
    type Reply = OriginateReply;
    fn classify_reply(reply: &OriginateReply) -> ReplyKind {
        match reply {
            OriginateReply::Called { .. }
            | OriginateReply::FaultArmed { .. }
            | OriginateReply::FaultReleased { .. }
            | OriginateReply::Snapshot { .. }
            | OriginateReply::ProxyRecorded { .. }
            | OriginateReply::Dialed { .. }
            | OriginateReply::TransportStopMarked { .. } => ReplyKind::Success,
            OriginateReply::BadDestination { .. } => ReplyKind::ProtocolRefusal,
            OriginateReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            OriginateReply::NotReady { .. } => ReplyKind::NotReady,
            OriginateReply::Busy { .. } => ReplyKind::Busy,
            OriginateReply::Draining { .. } => ReplyKind::Draining,
            OriginateReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            OriginateReply::Unauthorized { .. } => ReplyKind::Unauthorized,
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
    fn draining(reason: String) -> OriginateReply {
        OriginateReply::Draining { reason }
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
    pub resolver: Arc<LiveNodeResolver>,
    pub client: Arc<NodeRpcClient>,
    pub connections: Arc<ConnectionsWriter>,
    pub fault: Arc<StorageFault>,
}

/// Serve the door on this node's own seams, with the testkit's default carrier policy ([`POLICY`]).
pub fn serve(b: ServerBuilder, seams: Seams, launch: &Launch) -> ServerBuilder {
    serve_with_policy(b, seams, launch, POLICY)
}

/// Serve the door with the carrier policy the serving application declares: a Proxy held for a
/// destination is the effective route only through a carrier of `policy`'s kind
/// (`rafka_mesh_entity::connections::resolve`). A shape whose carriers are not `rpc_node`s (a
/// gateway carrying for a compute) declares its own kind here.
pub fn serve_with_policy(b: ServerBuilder, seams: Seams, launch: &Launch, policy: CarrierPolicy) -> ServerBuilder {
    let by = AnsweredBy { node_id: launch.node_id.to_string(), node: launch.name.to_string(), incarnation_id: launch.incarnation.to_string() };
    let own: PathName = launch.name.clone();
    b.serve::<Originate, _, _>(OpOwner::Testkit, move |_peer: PeerContext, req: OriginateRequest| {
        let (by, seams, own) = (by.clone(), seams.clone(), own.clone());
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
                OriginateRequest::StopTransport { reason } => {
                    // The mark runs after this reply is on its way: the exit it causes is the real one.
                    let marked = reason.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        rafka_mesh_transport::membership::mark_transport_stopped(marked);
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
                        owed: rafka_mesh_entity::reconnect::owed_retirements(&held, 0),
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
            let row = NodeConnection { state: ConnectionState::Disconnected, reason: Some(why.to_string()), recovery: None, logged_at_ms: now_ms().max(proxy.logged_at_ms + 1), ..proxy };
            let _ = seams.connections.record(row).await;
            Some(why.to_string())
        }
        None => None,
    };
    let (route, carrier) = match &call.route {
        rafka_mesh_entity::connections::EffectiveRoute::ViaPeer { carrier, .. } => ("via-peer".to_string(), Some(carrier.to_string())),
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
    use rafka_node_rpc_contract::ping::{Ping, PingRequest};
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

async fn record_proxy(by: &AnsweredBy, seams: &Seams, destination: &str, carrier: &str, failed_attempts: u32) -> OriginateReply {
    use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionKind};
    use rafka_node_rpc::ConnectionObserver;
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
    seams.connections.drain().await;
    let end = |n: &rafka_node_rpc::ResolvedNode| ConnectionEnd { name: n.name.clone(), node_id: n.node_id.clone(), incarnation: Some(n.incarnation.clone()) };
    let row = NodeConnection {
        source: seams.connections.own().clone(),
        destination: end(&dest),
        kind: ConnectionKind::Proxy,
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
    use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionKind, ConnectionsHeld, EffectiveRoute};
    use rafka_mesh_entity::{IncarnationId, NodeId};

    fn end(kind: NodeKind, ordinal: u32, inc: &str) -> ConnectionEnd {
        ConnectionEnd { name: PathName { mesh: "mesh1".into(), kind, ordinal }, node_id: NodeId::mint(), incarnation: Some(IncarnationId(inc.into())) }
    }

    fn row(source: &ConnectionEnd, destination: &ConnectionEnd, kind: ConnectionKind, carrier: Option<&ConnectionEnd>, at: u64) -> NodeConnection {
        NodeConnection { source: source.clone(), destination: destination.clone(), kind, state: ConnectionState::Connected, carrier: carrier.cloned(), recovery: None, reason: None, logged_at_ms: at }
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
        let gateway = CarrierPolicy::Forwardable { carrier_kind: NodeKind::Gateway };
        let r = resolve(&held, &own.name, &dest.name, gateway);
        assert_eq!(r.route, EffectiveRoute::ViaPeer { carrier: carrier.name.clone(), proxy: p });
        assert_eq!(r.retire, None);
        let d = resolve(&held, &own.name, &dest.name, POLICY);
        assert_eq!(d.route, EffectiveRoute::Direct { known: false }, "the gateway-carried Proxy is not the route under the rpc_node policy: with no Direct fact the pair dials directly");
        assert_eq!(d.retire, None, "a Proxy through a kind the policy does not name is skipped, never retired");
    }
}
