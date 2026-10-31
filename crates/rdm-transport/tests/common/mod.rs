//! The routing rig shared by the routing cells (i143.e6.s6) and the connections-integration
//! cells (i143.e6.s5): a forwardable Probe family, three nodes (B, C, and the carrier P), an
//! origin client, and connections fixtures in the shape the writer records them.
#![allow(dead_code)]

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::connections::{
    ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery, NodeConnection,
};
use rdm_core::{IncarnationId, NodeId, PathName};
use rdm_transport::{CallOptions, HandlerFault, NodeRpcClient, PeerContext, ResolvedNode, RouteChoice, RouteLeg, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::catalog::{LedgerEntry, OpOwner, OpState};
use rdm_core::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
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
    ConnectionsDeleting { reason: String },
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

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            ProbeReply::Probed { .. } => "probed",
            ProbeReply::PeerUnresolved { .. } => "peer-unresolved",
            ProbeReply::NotReady { .. } => "not-ready",
            ProbeReply::Busy { .. } => "busy",
            ProbeReply::ConnectionsDeleting { .. } => "connections-deleting",
            ProbeReply::Malformed { .. } => "malformed",
            ProbeReply::Unauthorized { .. } => "unauthorized",
        }
    }
    fn classify_reply(r: &ProbeReply) -> ReplyKind {
        match r {
            ProbeReply::Probed { .. } => ReplyKind::Success,
            ProbeReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            ProbeReply::NotReady { .. } => ReplyKind::NotReady,
            ProbeReply::Busy { .. } => ReplyKind::Busy,
            ProbeReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
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
    fn connections_deleting(reason: String) -> ProbeReply {
        ProbeReply::ConnectionsDeleting { reason }
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
    let server = b.build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() }).unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id, name: name.parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation };
    Node { _router: router, key, resolved, handled }
}

pub async fn bind() -> (SecretKey, iroh::Endpoint) {
    let key = SecretKey::generate();
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
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

pub async fn routed(rig: &Rig, target: &Node, route: RouteChoice, payload: &[u8]) -> (RpcOutcome<ProbeReply>, Option<rdm_transport::CallEvidence>, RouteLeg) {
    rig.origin.call_routed::<Probe>(&target.resolved.node_id, &route, &probe(payload), &CallOptions { budget: rdm_transport::Budget::Overall(Duration::from_millis(1500)), ..Default::default() }).await
}


/// Connections fixtures, as the held projection records them.
pub fn end(n: &Node) -> ConnectionEnd {
    ConnectionEnd { name: n.resolved.name.clone(), node_id: n.resolved.node_id.clone(), incarnation: Some(n.resolved.incarnation.clone()) }
}

pub fn own_end(own: &PathName) -> ConnectionEnd {
    ConnectionEnd { name: own.clone(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) }
}

/// A connections fact at stamp `at`, in the shape the writer records it.
pub fn connection(from: ConnectionEnd, to: ConnectionEnd, kind: ConnectionKind, state: ConnectionState, carrier: Option<ConnectionEnd>, at: u64) -> NodeConnection {
    let recovery = (kind == ConnectionKind::Direct && state == ConnectionState::Failed).then_some(DirectRecovery { recovery_round: 1, attempt_ordinal: 1 });
    NodeConnection { id: rdm_core::ConnectionId::mint(), source: from, destination: to, kind, alpn: rdm_core::connections::ALPN.to_vec(), state, carrier, recovery, reason: None, logged_at_ms: at }
}

pub fn held_for(own: &PathName) -> ConnectionsHeld {
    let mut held = ConnectionsHeld::new();
    held.set_own_source(own.clone());
    held.mark_complete();
    held
}

/// The span capture of this test executable: the product span pipeline
/// (`rdm_transport::install_span_capture`: product's filter and its `BatchSpanProcessor`)
/// in front of an in-memory exporter, as the executable's one global subscriber. The subscriber is
/// global because spans end on threads a cell does not own (the endpoint's transport driver, tasks
/// it spawns). Ending a span queues it; the one way to read spans is [`Spans::get_finished_spans`],
/// which flushes first (bounded) and so cannot be called without the flush.
#[derive(Clone)]
pub struct Spans {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: std::sync::Arc<rdm_transport::SpanCapture>,
}

impl Spans {
    /// Every span ended before this call, whichever thread ended it. Panics, naming why, when the
    /// flush does not return in time, errors, or leaves a span behind.
    pub fn get_finished_spans(&self) -> Result<Vec<opentelemetry_sdk::trace::SpanData>, opentelemetry_sdk::error::OTelSdkError> {
        if let Err(e) = self.capture.flush(std::time::Duration::from_secs(10)) {
            panic!("reading spans: {e}");
        }
        self.exporter.get_finished_spans()
    }
}

pub fn spans_exporter() -> Spans {
    static SPANS: std::sync::OnceLock<Spans> = std::sync::OnceLock::new();
    SPANS
        .get_or_init(|| {
            let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
            let capture = rdm_transport::install_span_capture("node-rpc-tests", exporter.clone()).expect("the test executable's span capture");
            Spans { exporter, capture: std::sync::Arc::new(capture) }
        })
        .clone()
}

/// A cell's trace left on disk: when the guard drops, whether the cell passed or panicked, every span
/// of `trace` the executable's capture holds goes to `target/i143-node-rpc/<cell>.<pid>.<trace>.spans.json`,
/// so a cell is read from its own trace and a red never needs a rerun.
pub struct TraceArtifact {
    trace: String,
}

impl TraceArtifact {
    pub fn new(trace: &str) -> Self {
        Self { trace: trace.to_string() }
    }
}

impl Drop for TraceArtifact {
    fn drop(&mut self) {
        let spans = spans_exporter();
        let flushed = spans.capture.flush(Duration::from_secs(10));
        let rows: Vec<serde_json::Value> = spans
            .exporter
            .get_finished_spans()
            .unwrap_or_default()
            .into_iter()
            .filter(|s| s.span_context.trace_id().to_string() == self.trace)
            .map(|s| {
                serde_json::json!({
                    "name": s.name,
                    "trace_id": s.span_context.trace_id().to_string(),
                    "span_id": s.span_context.span_id().to_string(),
                    "parent_span_id": s.parent_span_id.to_string(),
                    "start_us": s.start_time.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0),
                    "end_us": s.end_time.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0),
                    "attributes": s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect::<std::collections::BTreeMap<_, _>>(),
                })
            })
            .collect();
        let cell = std::thread::current().name().unwrap_or("unnamed").replace("::", "__");
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-node-rpc");
        let path = dir.join(format!("{cell}.{}.{}.spans.json", std::process::id(), self.trace));
        let written = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, serde_json::to_vec_pretty(&rows).unwrap()));
        eprintln!("node-rpc: trace {} ({} spans) -> {} (flush: {flushed:?}, write: {written:?})", self.trace, rows.len(), path.display());
    }
}
