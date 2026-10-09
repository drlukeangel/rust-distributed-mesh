//! i143.e6.s9 functional: caller identity and W3C context ride the request envelope, end to end.
//!
//! `caller_system`, `traceparent`, `tracestate` and `baggage` travel beside the target fence,
//! never inside a protocol codec. A direct call continues the caller's trace at the target; a
//! carried call keeps the origin's causal parent and `caller_system` through the hop. Nothing
//! here reaches target selection, authority, the fence or the result: a missing context is
//! valid, and a malformed or over-bound part is dropped, named as `context_dropped` on the
//! local span, while the call proceeds unchanged. Baggage propagates whole and only allowlisted
//! keys (`test_case`, `scenario`, `operation`) become span attributes.
//!
//! Spans are captured in-process through an OpenTelemetry in-memory exporter, so every claim
//! about a span's parent or attributes is read from the span itself.

use iroh::protocol::Router;
use iroh::SecretKey;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::export::trace::SpanData;
use opentelemetry_sdk::testing::trace::InMemorySpanExporter;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, PeerContext, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::{LedgerEntry, OpOwner, OpState};
use rafka_node_rpc_contract::context::{CallContext, MAX_BAGGAGE_BYTES, MAX_TRACESTATE_BYTES};
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing_opentelemetry::OpenTelemetrySpanExt;

const PARENT: &str = "b7ad6b7169203331";

/// A trace of this cell's own: the cells share one exporter, so each reads only its trace.
struct Trace {
    id: String,
    tp: String,
}

fn trace() -> Trace {
    let id = format!("{:032x}", (u128::from(rand_u64()) << 64) | u128::from(rand_u64()));
    Trace { tp: format!("00-{id}-{PARENT}-01"), id }
}

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new().build_hasher().finish() | 1
}

/// One in-memory exporter for the test binary; each cell reads only its own trace or payload.
fn spans() -> InMemorySpanExporter {
    crate::common::spans_exporter()
}

fn attr(s: &SpanData, key: &str) -> Option<String> {
    s.attributes.iter().find(|kv| kv.key.as_str() == key).map(|kv| kv.value.as_str().into_owned())
}

fn finished(named: &str, trace: Option<&str>) -> Vec<SpanData> {
    spans()
        .get_finished_spans()
        .unwrap()
        .into_iter()
        .filter(|s| s.name == named && trace.map_or(true, |t| s.span_context.trace_id().to_string() == t))
        .collect()
}

fn echo(payload: &[u8]) -> PingRequest {
    PingRequest::Ping { payload: payload.to_vec() }
}

/// A forwardable test family on a ledgered test op (Ping is not forwardable by design).
struct Probe;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum ProbeRequest {
    Probe { payload: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum ProbeReply {
    Probed { payload: Vec<u8> },
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

fn test_ledger() -> Vec<LedgerEntry> {
    vec![LedgerEntry { op: Probe::OP, family: "probe".into(), owner: OpOwner::Product("test".into()), state: OpState::Live }]
}

/// A process serving Ping on `rpc-0`; the handler keeps the PeerContext each call brought.
struct Process {
    _router: Router,
    resolved: ResolvedNode,
    seen: Arc<Mutex<Vec<PeerContext>>>,
}

async fn process(name: &str) -> Process {
    let key = SecretKey::generate();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
        let endpoint = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = endpoint.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (s, s2) = (seen.clone(), seen.clone());
    let server = ServerBuilder::new()
        .ledger(test_ledger())
        .serve::<Ping, _, _>(OpOwner::Core, move |peer, req: PingRequest| {
            s.lock().unwrap().push(peer);
            async move {
                let PingRequest::Ping { payload } = req;
                Ok(PingReply::Pong { payload })
            }
        })
        .serve::<Probe, _, _>(OpOwner::Product("test".into()), move |peer, req: ProbeRequest| {
            s2.lock().unwrap().push(peer);
            async move {
                let ProbeRequest::Probe { payload } = req;
                Ok(ProbeReply::Probed { payload })
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let router = Router::builder(endpoint).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id, name: name.parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation };
    Process { _router: router, resolved, seen }
}

async fn client(caller_system: Option<&str>, nodes: &[&ResolvedNode]) -> (NodeRpcClient, Arc<StaticResolver>) {
    let resolver = Arc::new(StaticResolver::new());
    for n in nodes {
        resolver.insert((*n).clone());
    }
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let mut c = NodeRpcClient::new(ep, resolver.clone());
    if let Some(s) = caller_system {
        c = c.with_caller_system(s);
    }
    (c, resolver)
}

fn with(context: CallContext) -> CallOptions {
    CallOptions { context: Some(context), ..Default::default() }
}

fn full(t: &Trace) -> CallContext {
    CallContext {
        caller_system: Some("rdm".into()),
        traceparent: Some(t.tp.clone()),
        tracestate: Some("rojo=00f067aa0ba902b7".into()),
        baggage: Some("test_case=ctx,scenario=direct,operation=echo,secret=never".into()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_direct_call_continues_the_callers_trace_and_records_only_allowlisted_baggage() {
    spans();
    let p = process("mesh1.rpc.1").await;
    let (c, _) = client(None, &[&p.resolved]).await;
    let t = trace();
    let (out, _) = c.call::<Ping>(&NodeTarget::ExactNode(p.resolved.node_id.clone()), &echo(b"direct"), &with(full(&t))).await;
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");

    // The whole context reaches the handler: baggage and tracestate propagate whole.
    let seen = p.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].context, full(&t), "the handler sees the context the caller sent, whole");

    // The serve span is a child of the caller's remote span, carries its tracestate, names the
    // caller system and exposes the allowlisted keys only.
    let serve = finished("rdm.node_rpc.request.serve.via-direct", Some(&t.id));
    assert_eq!(serve.len(), 1, "{serve:?}");
    let s = &serve[0];
    assert_eq!(s.parent_span_id.to_string(), PARENT, "the caller's span is the parent");
    assert_eq!(s.span_context.trace_state().get("rojo"), Some("00f067aa0ba902b7"), "tracestate reached the target's span context");
    assert_eq!(attr(s, "caller_system").as_deref(), Some("rdm"));
    assert_eq!(attr(s, "test_case").as_deref(), Some("ctx"));
    assert_eq!(attr(s, "scenario").as_deref(), Some("direct"));
    assert_eq!(attr(s, "operation").as_deref(), Some("echo"));
    assert_eq!(attr(s, "secret"), None, "a key outside the allowlist is never a span attribute");
    assert_eq!(attr(s, "context_dropped"), None, "nothing was dropped");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_without_explicit_context_sends_its_current_span_and_its_caller_system() {
    spans();
    let p = process("mesh1.rpc.1").await;
    let (c, _) = client(Some("rdm"), &[&p.resolved]).await;
    let caller = tracing::info_span!("test.caller");
    let trace = caller.context().span().span_context().trace_id().to_string();
    let parent = caller.context().span().span_context().span_id().to_string();
    let out = {
        let _g = caller.enter();
        tracing::Instrument::instrument(
            c.call::<Ping>(&NodeTarget::ExactNode(p.resolved.node_id.clone()), &echo(b"implicit"), &CallOptions::default()),
            caller.clone(),
        )
        .await
        .0
    };
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
    let seen = p.seen.lock().unwrap().clone();
    assert_eq!(seen[0].context.caller_system.as_deref(), Some("rdm"), "the client's caller_system rides every call");
    let serve = finished("rdm.node_rpc.request.serve.via-direct", Some(&trace));
    assert_eq!(serve.len(), 1, "the serve span joined the caller's current trace: {serve:?}");
    // The unbroken hierarchy (node-rpc.md §48): the caller's span, its call span holding the
    // per-call evidence, then the target's serve span.
    let call = finished("rdm.node_rpc.request.update.via-call", Some(&trace));
    assert_eq!(call.len(), 1, "one call span in the caller's trace: {call:?}");
    assert_eq!(call[0].parent_span_id.to_string(), parent, "the call span is a child of the caller's span");
    assert_eq!(attr(&call[0], "outcome").as_deref(), Some("Reply"));
    assert_eq!(serve[0].parent_span_id, call[0].span_context.span_id(), "the serve span is a child of the call span");
    assert_eq!(attr(&serve[0], "caller_system").as_deref(), Some("rdm"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_context_is_valid_and_starts_a_new_trace() {
    spans();
    let p = process("mesh1.rpc.1").await;
    let (c, _) = client(None, &[&p.resolved]).await;
    let (out, _) = c.call::<Ping>(&NodeTarget::ExactNode(p.resolved.node_id.clone()), &echo(b"none"), &with(CallContext::default())).await;
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
    let seen = p.seen.lock().unwrap().clone();
    assert_eq!(seen[0].context, CallContext::default());
    let serve: Vec<SpanData> = finished("rdm.node_rpc.request.serve.via-direct", None)
        .into_iter()
        .filter(|s| attr(s, "peer").as_deref() == Some(&c.endpoint().id().to_string()))
        .collect();
    assert_eq!(serve.len(), 1, "{serve:?}");
    assert!(serve[0].parent_span_id == opentelemetry::trace::SpanId::INVALID, "no context: a new root trace");
    assert_eq!(attr(&serve[0], "caller_system"), None);
    assert_eq!(attr(&serve[0], "context_dropped"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_or_over_bound_context_is_dropped_by_name_and_never_changes_the_outcome() {
    spans();
    let p = process("mesh1.rpc.1").await;
    let (c, _) = client(None, &[&p.resolved]).await;
    // Hand the raw context past the client's own sanitizing: the target must drop it too.
    let bad = CallContext {
        caller_system: Some("Not Valid".into()),
        traceparent: Some("00-not-a-trace-01".into()),
        tracestate: Some("rojo=1".into()),
        baggage: Some(format!("test_case={}", "x".repeat(MAX_BAGGAGE_BYTES))),
    };
    let (out, ev) = c.call::<Ping>(&NodeTarget::ExactNode(p.resolved.node_id.clone()), &echo(b"bad"), &with(bad)).await;
    assert!(matches!(&out, RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { payload } if payload == b"bad")), "the call proceeds: {out:?}");
    assert!(ev.unwrap().committed);
    // The caller dropped every bad part before sending, named on its own span...
    let dropped = finished("rdm.node_rpc.request.update.via-context-dropped", None);
    let caller_side: Vec<_> = dropped.iter().filter(|s| attr(s, "decided_by").as_deref() == Some("caller")).collect();
    assert_eq!(caller_side.len(), 1, "{dropped:?}");
    assert_eq!(attr(caller_side[0], "context_dropped").as_deref(), Some("caller_system,traceparent,baggage"));
    // ...so the target received nothing to drop, saw no context and still served.
    let seen = p.seen.lock().unwrap().clone();
    assert_eq!(seen[0].context, CallContext::default(), "an invalid traceparent took the tracestate with it; nothing survived");

    // A tracestate over its bound travels with a valid traceparent and is dropped alone.
    let t = trace();
    let ts = CallContext { tracestate: Some("k=".to_string() + &"v".repeat(MAX_TRACESTATE_BYTES)), ..full(&t) };
    let (out, _) = c.call::<Ping>(&NodeTarget::ExactNode(p.resolved.node_id.clone()), &echo(b"ts"), &with(ts)).await;
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
    let seen = p.seen.lock().unwrap().clone();
    assert_eq!(seen[1].context, CallContext { tracestate: None, ..full(&t) });
    let serve = finished("rdm.node_rpc.request.serve.via-direct", Some(&t.id));
    assert!(serve.iter().any(|s| attr(s, "caller_system").as_deref() == Some("rdm") && s.parent_span_id.to_string() == PARENT), "the valid parts still apply: {serve:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn context_never_reaches_the_fence_decision() {
    spans();
    let p = process("mesh1.rpc.1").await;
    let (c, _) = client(None, &[&p.resolved]).await;
    // The caller's view names another node id at this process: 425 whatever the context says.
    let mut stale = p.resolved.clone();
    stale.node_id = NodeId::mint();
    let (c2, _) = client(None, &[&stale]).await;
    let t = trace();
    let ctx = CallContext { baggage: Some("operation=make-it-current".into()), ..full(&t) };
    let (out, _) = c2.call::<Ping>(&NodeTarget::ExactNode(stale.node_id.clone()), &echo(b"stale"), &with(ctx)).await;
    assert!(matches!(out, RpcOutcome::RejectedStale(_)), "the fence decides, the context cannot: {out:?}");
    assert!(p.seen.lock().unwrap().is_empty(), "nothing was dispatched");
    // The refusal was decided on the fence alone: the context was never read, so the refusal
    // is not in the caller's trace and carries no caller_system.
    assert!(finished("rdm.node_rpc.connection.reject.via-stale-target", Some(&t.id)).is_empty(), "a 425 owes nothing to section 1");
    let refused = finished("rdm.node_rpc.connection.reject.via-stale-target", None);
    assert!(refused.iter().any(|s| attr(s, "decided_by").as_deref() == Some("target") && attr(s, "caller_system").is_none()), "{refused:?}");
    // The current client serves with the same context.
    let (out, _) = c.call::<Ping>(&NodeTarget::ExactNode(p.resolved.node_id.clone()), &echo(b"current"), &with(full(&t))).await;
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_carried_call_keeps_the_origins_trace_and_caller_system_through_the_hop() {
    spans();
    let target = process("mesh1.rpc.3").await;
    // The carrier calls out as itself (`caller_system = "carrier"`), serving Forward for Ping.
    let carrier_key = SecretKey::generate();
    let carrier_ep = rafka_node_rpc::endpoint::bind(carrier_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let carrier_addr = carrier_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let carrier_resolver = Arc::new(StaticResolver::new());
    carrier_resolver.insert(target.resolved.clone());
    let carrier_client = Arc::new(NodeRpcClient::new(carrier_ep.clone(), carrier_resolver).with_caller_system("carrier"));
    let (cid, cinc) = (NodeId::mint(), IncarnationId::mint());
    let carrier_server = ServerBuilder::new()
        .ledger(test_ledger())
        .carry::<Probe>()
        .serve_forward(carrier_client)
        .seal(ServedBirth { node_id: cid.to_string(), incarnation: cinc.0.clone() })
        .unwrap();
    let _carrier_router = Router::builder(carrier_ep).accept(rafka_node_rpc::ALPN, carrier_server).spawn();
    let carrier = ResolvedNode { node_id: cid, name: "mesh1.rpc.2".parse().unwrap(), endpoint_id: carrier_key.public(), transport_addr: carrier_addr, incarnation: cinc };

    let (origin, _) = client(Some("rdm"), &[&carrier, &target.resolved]).await;
    let t = trace();
    let ctx = CallContext { baggage: Some("test_case=ctx,scenario=carried".into()), ..full(&t) };
    let probe = ProbeRequest::Probe { payload: b"via".to_vec() };
    let (out, _) = origin.call_via::<Probe>(&NodeTarget::ExactNode(carrier.node_id.clone()), &target.resolved.node_id, &probe, &with(ctx.clone())).await;
    assert!(matches!(&out, RpcOutcome::Reply(r) if matches!(r.value(), ProbeReply::Probed { payload } if payload == b"via")), "{out:?}");

    // The target saw the origin's context unchanged: caller_system stays "rdm", not "carrier".
    let seen = target.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].context, ctx, "the carrier hands the origin's context through unchanged");
    assert_eq!(seen[0].endpoint_id, carrier_key.public(), "the target authenticates the carrier, never a copied origin");

    // One trace: the Forward serve and the hop are children of the origin's span, and the
    // target's serve span keeps the origin's causal parent rather than the hop.
    let forward_serve: Vec<_> = finished("rdm.node_rpc.request.serve.via-direct", Some(&t.id)).into_iter().filter(|s| attr(s, "protocol").as_deref() == Some("forward")).collect();
    assert_eq!(forward_serve.len(), 1, "{forward_serve:?}");
    assert_eq!(forward_serve[0].parent_span_id.to_string(), PARENT);
    assert_eq!(attr(&forward_serve[0], "caller_system").as_deref(), Some("rdm"));
    let hop = finished("rdm.node_rpc.request.serve.via-carried-inner", Some(&t.id));
    assert_eq!(hop.len(), 1, "{hop:?}");
    assert_eq!(hop[0].parent_span_id.to_string(), PARENT, "the hop is a child span in the origin's trace");
    assert_eq!(attr(&hop[0], "caller_system").as_deref(), Some("rdm"));
    let inner: Vec<_> = finished("rdm.node_rpc.request.serve.via-direct", Some(&t.id)).into_iter().filter(|s| attr(s, "protocol").as_deref() == Some("probe")).collect();
    assert_eq!(inner.len(), 1, "{inner:?}");
    assert_eq!(inner[0].parent_span_id.to_string(), PARENT, "the origin's parent, not the hop");
    assert_eq!(attr(&inner[0], "caller_system").as_deref(), Some("rdm"));
    assert_eq!(attr(&inner[0], "scenario").as_deref(), Some("carried"));
}
