//! The carried-call rig shared by the forward cells: a target that serves the `Probe` family,
//! a carrier that carries it, and an origin that knows only the carrier and the target.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, CarrierEdges, HandlerFault, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::{LedgerEntry, OpOwner, OpState};
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind};
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A forwardable test family on a ledgered test op.
pub struct Probe;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeRequest {
    Probe { payload: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeReply {
    /// The payload, and the transport id the target saw as its caller.
    Probed { payload: Vec<u8>, caller: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Probe {
    const OP: u8 = 0x5E;
    const NAME: &'static str = "probe";
    const MAX_REQUEST_FRAME_BYTES: usize = 4096;
    const MAX_REPLY_FRAME_BYTES: usize = 4096;
    const FORWARDABLE: bool = true;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 7;
    type Request = ProbeRequest;
    type Reply = ProbeReply;
    fn classify_reply(r: &ProbeReply) -> ReplyKind {
        match r {
            ProbeReply::Probed { .. } => ReplyKind::Success,
            ProbeReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            ProbeReply::NotReady { .. } => ReplyKind::NotReady,
            ProbeReply::Busy { .. } => ReplyKind::Busy,
            ProbeReply::Draining { .. } => ReplyKind::Draining,
            ProbeReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            ProbeReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> ProbeReply {
        ProbeReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> ProbeReply {
        ProbeReply::NotReady { reason }
    }
    fn busy(reason: String) -> ProbeReply {
        ProbeReply::Busy { reason }
    }
    fn draining(reason: String) -> ProbeReply {
        ProbeReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> ProbeReply {
        ProbeReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> ProbeReply {
        ProbeReply::Unauthorized { reason }
    }
}

/// The builder starts from the core ledger; the test family adds its own row.
pub fn test_ledger() -> Vec<LedgerEntry> {
    vec![LedgerEntry { op: Probe::OP, family: "probe".into(), owner: OpOwner::Product("test".into()), state: OpState::Live }]
}

pub struct Node {
    pub router: Router,
    pub key: SecretKey,
    pub resolved: ResolvedNode,
}

/// A birth's identity, minted before its server seals.
pub fn birth() -> (NodeId, IncarnationId) {
    (NodeId::mint(), IncarnationId::mint())
}

pub fn served(b: &(NodeId, IncarnationId)) -> ServedBirth {
    ServedBirth { node_id: b.0.to_string(), incarnation: b.1 .0.clone() }
}

pub async fn start(server: rafka_node_rpc::NodeRpcServer, key: SecretKey, name: &str, b: (NodeId, IncarnationId)) -> Node {
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id: b.0, name: name.parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation: b.1 };
    Node { router, key, resolved }
}

pub async fn client_with(nodes: &[&ResolvedNode]) -> NodeRpcClient {
    let resolver = Arc::new(StaticResolver::new());
    for n in nodes {
        resolver.insert((*n).clone());
    }
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    NodeRpcClient::new(ep, resolver)
}

pub struct Rig {
    pub origin: NodeRpcClient,
    pub carrier: Node,
    pub target: Node,
    pub handled: Arc<AtomicU64>,
    pub applied: Arc<tokio::sync::Notify>,
    pub carrier_server: rafka_node_rpc::NodeRpcServer,
}

/// target serves Probe and Ping; the carrier serves forward and carries Probe (and Ping, which is
/// not forwardable and so is never carried); the origin knows only the carrier and the target.
pub async fn rig() -> Rig {
    rig_with(None, false).await
}

/// `edges`: the carrier's account of its Direct edges. `carrier_misdials`: the carrier's own view
/// of the target names another transport identity, so its dial fails at the handshake.
pub async fn rig_with(edges: Option<Arc<dyn CarrierEdges>>, carrier_misdials: bool) -> Rig {
    let handled = Arc::new(AtomicU64::new(0));
    let applied = Arc::new(tokio::sync::Notify::new());
    let (h, a) = (handled.clone(), applied.clone());
    let target_birth = birth();
    let target_server = ServerBuilder::new()
        .ledger(test_ledger())
        .serve::<Probe, _, _>(OpOwner::Product("test".into()), move |peer, req: ProbeRequest| {
            let (h, a) = (h.clone(), a.clone());
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                a.notify_one();
                let ProbeRequest::Probe { payload } = req;
                if payload == b"hold" {
                    std::future::pending::<()>().await;
                }
                Ok::<_, HandlerFault>(ProbeReply::Probed { payload, caller: peer.endpoint_id.to_string() })
            }
        })
        .serve::<Ping, _, _>(OpOwner::Core, |_p, req: PingRequest| async move {
            let PingRequest::Ping { payload, .. } = req;
            Ok(PingReply::Pong { payload })
        })
        .seal(served(&target_birth))
        .unwrap();
    let target = start(target_server, SecretKey::generate(), "mesh1.rpc.3", target_birth).await;

    // The carrier calls out on the endpoint it serves on, as a node does: the target sees the
    // carrier's own transport identity.
    let carrier_key = SecretKey::generate();
    let carrier_ep = rafka_node_rpc::endpoint::bind(carrier_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let carrier_addr = carrier_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let carrier_resolver = Arc::new(StaticResolver::new());
    let mut seen = target.resolved.clone();
    if carrier_misdials {
        seen.endpoint_id = SecretKey::generate().public();
    }
    carrier_resolver.insert(seen);
    let carrier_client = Arc::new(NodeRpcClient::new(carrier_ep.clone(), carrier_resolver));
    let carrier_birth = birth();
    let carrier_builder = ServerBuilder::new().ledger(test_ledger()).carry::<Probe>().carry::<Ping>();
    let carrier_server = match edges {
        Some(e) => carrier_builder.serve_forward_with_edges(carrier_client, e),
        None => carrier_builder.serve_forward(carrier_client),
    }
    .seal(served(&carrier_birth))
    .unwrap();
    let carrier = Node {
        router: Router::builder(carrier_ep).accept(rafka_node_rpc::ALPN, carrier_server.clone()).spawn(),
        key: carrier_key.clone(),
        resolved: ResolvedNode {
            node_id: carrier_birth.0,
            name: "mesh1.rpc.2".parse().unwrap(),
            endpoint_id: carrier_key.public(),
            transport_addr: carrier_addr,
            incarnation: carrier_birth.1,
        },
    };
    let origin = client_with(&[&carrier.resolved, &target.resolved]).await;
    Rig { origin, carrier, target, handled, applied, carrier_server }
}

pub fn probe(p: &[u8]) -> ProbeRequest {
    ProbeRequest::Probe { payload: p.to_vec() }
}

pub fn carrier_of(r: &Rig) -> NodeTarget {
    NodeTarget::ExactNode(r.carrier.resolved.node_id.clone())
}

/// A call bounded by `d` as its whole budget.
pub fn overall(d: Duration) -> CallOptions {
    CallOptions { budget: rafka_node_rpc::Budget::Overall(d), ..CallOptions::default() }
}
