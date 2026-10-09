//! The routing rig shared by the routing cells (i143.e6.s6) and the connections-integration
//! cells (i143.e6.s5): a forwardable Probe family, three nodes (B, C, and the carrier P), an
//! origin client, and connections fixtures in the shape the writer records them.
#![allow(dead_code)]

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::connections::{
    ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery, NodeConnection,
};
use rafka_mesh_entity::{IncarnationId, NodeId, PathName};
use rafka_node_rpc::{CallOptions, HandlerFault, NodeRpcClient, PeerContext, ResolvedNode, RouteChoice, RouteLeg, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::{LedgerEntry, OpOwner, OpState};
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A forwardable test family: the reply names the node that served it.
pub struct Probe;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeRequest {
    Probe { payload: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeReply {
    Probed { payload: Vec<u8>, served_by: String, caller: String },
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

pub fn test_ledger() -> Vec<LedgerEntry> {
    vec![LedgerEntry { op: Probe::OP, family: "probe".into(), owner: OpOwner::Product("test".into()), state: OpState::Live }]
}

pub struct Node {
    pub _router: Router,
    pub key: SecretKey,
    pub resolved: ResolvedNode,
    pub handled: Arc<AtomicU64>,
}

/// A node serving Probe (answering with its own name) and, with `client`, carrying Probe for others.
pub async fn node(name: &str, carrier_client: Option<Arc<NodeRpcClient>>, key: SecretKey, ep: iroh::Endpoint) -> Node {
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let handled = Arc::new(AtomicU64::new(0));
    let (h, me) = (handled.clone(), name.to_string());
    let mut b = ServerBuilder::new().ledger(test_ledger()).serve::<Probe, _, _>(OpOwner::Product("test".into()), move |peer: PeerContext, req: ProbeRequest| {
        let (h, me) = (h.clone(), me.clone());
        async move {
            h.fetch_add(1, Ordering::SeqCst);
            let ProbeRequest::Probe { payload } = req;
            if payload == b"hold" {
                std::future::pending::<()>().await;
            }
            Ok::<_, HandlerFault>(ProbeReply::Probed { payload, served_by: me, caller: peer.endpoint_id.to_string() })
        }
    });
    if let Some(c) = carrier_client {
        b = b.carry::<Probe>().serve_forward(c);
    }
    let server = b.seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() }).unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id, name: name.parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation };
    Node { _router: router, key, resolved, handled }
}

pub async fn bind() -> (SecretKey, iroh::Endpoint) {
    let key = SecretKey::generate();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    (key, ep)
}

pub struct Rig {
    pub origin: NodeRpcClient,
    pub b: Node,
    pub c: Node,
    pub p: Node,
}

/// B and C are two semantically eligible targets; P is a carrier that can reach both.
pub async fn rig() -> Rig {
    let (bk, be) = bind().await;
    let b = node("mesh1.rpc.1", None, bk, be).await;
    let (ck, ce) = bind().await;
    let c = node("mesh1.rpc.2", None, ck, ce).await;
    let (pk, pe) = bind().await;
    let p_resolver = Arc::new(StaticResolver::new());
    p_resolver.insert(b.resolved.clone());
    p_resolver.insert(c.resolved.clone());
    let p_client = Arc::new(NodeRpcClient::new(pe.clone(), p_resolver));
    let p = node("mesh1.rpc.3", Some(p_client), pk, pe).await;
    let resolver = Arc::new(StaticResolver::new());
    for n in [&b, &c, &p] {
        resolver.insert(n.resolved.clone());
    }
    let (_, oe) = bind().await;
    Rig { origin: NodeRpcClient::new(oe, resolver), b, c, p }
}

/// The domain: a selector that chooses by name and nothing else.
pub fn select<'a>(rig: &'a Rig, choice: &str) -> &'a Node {
    match choice {
        "B" => &rig.b,
        "C" => &rig.c,
        _ => unreachable!(),
    }
}

pub fn probe(p: &[u8]) -> ProbeRequest {
    ProbeRequest::Probe { payload: p.to_vec() }
}

pub fn served_by(out: &RpcOutcome<ProbeReply>) -> Option<(String, String)> {
    match out {
        RpcOutcome::Reply(r) => match r.value() {
            ProbeReply::Probed { served_by, caller, .. } => Some((served_by.clone(), caller.clone())),
            _ => None,
        },
        _ => None,
    }
}

pub async fn routed(rig: &Rig, target: &Node, route: RouteChoice, payload: &[u8]) -> (RpcOutcome<ProbeReply>, Option<rafka_node_rpc::CallEvidence>, RouteLeg) {
    rig.origin.call_routed::<Probe>(&target.resolved.node_id, &route, &probe(payload), &CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_millis(1500)), ..Default::default() }).await
}


/// Connections fixtures, as the held projection records them.
pub fn end(n: &Node) -> ConnectionEnd {
    ConnectionEnd { name: n.resolved.name.clone(), node_id: n.resolved.node_id.clone(), incarnation: Some(n.resolved.incarnation.clone()) }
}

pub fn own_end(own: &PathName) -> ConnectionEnd {
    ConnectionEnd { name: own.clone(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) }
}

/// A connections fact at stamp `at`, in the shape the writer records it.
pub fn edge(from: ConnectionEnd, to: ConnectionEnd, kind: ConnectionKind, state: ConnectionState, carrier: Option<ConnectionEnd>, at: u64) -> NodeConnection {
    let recovery = (kind == ConnectionKind::Direct && state == ConnectionState::Failed).then_some(DirectRecovery { recovery_epoch: 1, attempt_ordinal: 1 });
    NodeConnection { source: from, destination: to, kind, state, carrier, recovery, reason: None, logged_at_ms: at }
}

pub fn held_for(own: &PathName) -> ConnectionsHeld {
    let mut held = ConnectionsHeld::new();
    held.set_own_source(own.clone());
    held.mark_complete();
    held
}

/// The one in-memory span exporter of this test executable, behind its one global subscriber.
/// Every cell that reads spans reads them through this and filters to its own trace; a cell never
/// installs a subscriber of its own, because one executable holds exactly one global.
pub fn spans_exporter() -> opentelemetry_sdk::testing::trace::InMemorySpanExporter {
    use opentelemetry_sdk::testing::trace::InMemorySpanExporter;
    use opentelemetry_sdk::trace::{SimpleSpanProcessor, TracerProvider};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    static EXPORTER: std::sync::OnceLock<InMemorySpanExporter> = std::sync::OnceLock::new();
    EXPORTER
        .get_or_init(|| {
            let exporter = InMemorySpanExporter::default();
            let provider = TracerProvider::builder().with_span_processor(SimpleSpanProcessor::new(Box::new(exporter.clone()))).build();
            let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "node-rpc-tests");
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer)).init();
            exporter
        })
        .clone()
}
