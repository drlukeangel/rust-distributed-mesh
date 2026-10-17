//! i143.e6.s3: server streaming — order, cancellation that releases the invocation, and
//! backpressure without unbounded buffering.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::stream::{SinkError, StreamFailure, StreamItem};
use opentelemetry_sdk::export::trace::SpanData;
use rdm_transport::{ServedBirth, CallOptions, HandlerFault, NodeRpcClient, NodeRpcServer, NodeTarget, ResolvedNode, ServerBuilder, ServerStats, StaticResolver};
use rdm_core::context::CallContext;
use rdm_core::catalog::{LedgerEntry, OpOwner, OpState};
use rdm_core::outcome::{IndeterminateReason, MalformedKind, ReplyKind};
use rdm_core::protocol::NodeProtocol;
use rdm_core::streaming::{FrameKind, StreamingProtocol};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A streaming test family: `Count { n, size }` streams `n` items of `size` bytes.
struct Count;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum CountRequest {
    Count { n: u32, size: u32, mode: Mode },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Mode {
    Normal,
    /// Refuse before `Started`.
    Refuse,
    /// Return a `Data` frame as the final frame: an order violation the runtime refuses.
    BadFinal,
    /// Panic after `Started`.
    Panic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum CountFrame {
    Started,
    Item { i: u32, bytes: Vec<u8> },
    Done { sent: u32, caller_gone: bool },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Count {
    const OP: u8 = 0x5D;
    const NAME: &'static str = "count";
    const MAX_REQUEST_FRAME_BYTES: usize = 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 256 * 1024;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 9;
    type Request = CountRequest;
    type Reply = CountFrame;
    fn classify_reply(r: &CountFrame) -> ReplyKind {
        match r {
            CountFrame::Started | CountFrame::Item { .. } | CountFrame::Done { .. } => ReplyKind::Success,
            CountFrame::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            CountFrame::NotReady { .. } => ReplyKind::NotReady,
            CountFrame::Busy { .. } => ReplyKind::Busy,
            CountFrame::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            CountFrame::Malformed { kind } => ReplyKind::Malformed(*kind),
            CountFrame::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> CountFrame {
        CountFrame::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> CountFrame {
        CountFrame::NotReady { reason }
    }
    fn busy(reason: String) -> CountFrame {
        CountFrame::Busy { reason }
    }
    fn connections_deleting(reason: String) -> CountFrame {
        CountFrame::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> CountFrame {
        CountFrame::Malformed { kind }
    }
    fn unauthorized(reason: String) -> CountFrame {
        CountFrame::Unauthorized { reason }
    }
}

impl StreamingProtocol for Count {
    fn frame_kind(f: &CountFrame) -> FrameKind {
        match f {
            CountFrame::Started => FrameKind::Started,
            CountFrame::Item { .. } => FrameKind::Data,
            CountFrame::Done { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Count::classify_reply(other)),
        }
    }
}

struct Rig {
    _router: Router,
    server: NodeRpcServer,
    client: NodeRpcClient,
    target: NodeTarget,
    stats: Arc<ServerStats>,
    produced: Arc<AtomicU64>,
    caller_gone: Arc<AtomicU64>,
    finished: Arc<tokio::sync::Notify>,
}

async fn rig() -> Rig {
    let produced = Arc::new(AtomicU64::new(0));
    let caller_gone = Arc::new(AtomicU64::new(0));
    let finished = Arc::new(tokio::sync::Notify::new());
    let (p, g, fin) = (produced.clone(), caller_gone.clone(), finished.clone());
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let server = ServerBuilder::new()
        .ledger([LedgerEntry { op: Count::OP, family: "count".into(), owner: OpOwner::Product("test".into()), state: OpState::Live }])
        .serve_stream::<Count, _, _>(OpOwner::Product("test".into()), move |_peer, req: CountRequest, sink| {
            let (p, g, fin) = (p.clone(), g.clone(), fin.clone());
            async move {
                let CountRequest::Count { n, size, mode } = req;
                if mode == Mode::Refuse {
                    return Ok(CountFrame::Busy { reason: "refused before started".into() });
                }
                let mut sink = match sink.started(CountFrame::Started).await {
                    Ok(s) => s,
                    Err(_) => return Ok(CountFrame::Done { sent: 0, caller_gone: true }),
                };
                if mode == Mode::Panic {
                    panic!("handler panic after started");
                }
                let mut sent = 0;
                let mut gone = false;
                for i in 0..n {
                    match sink.data(CountFrame::Item { i, bytes: vec![7u8; size as usize] }).await {
                        Ok(()) => {
                            sent += 1;
                            p.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(SinkError::CallerGone(_)) => {
                            gone = true;
                            g.fetch_add(1, Ordering::SeqCst);
                            break;
                        }
                        Err(e) => return Err(HandlerFault::invariant_broken(format!("{e:?}"))),
                    }
                }
                fin.notify_one();
                if mode == Mode::BadFinal {
                    return Ok(CountFrame::Item { i: n, bytes: vec![] });
                }
                Ok(CountFrame::Done { sent, caller_gone: gone })
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .unwrap();
    let stats = server.stats();
    let key = SecretKey::generate();
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server.clone()).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode {
        node_id: node_id.clone(),
        name: "mesh1.rpc.1".parse().unwrap(),
        endpoint_id: key.public(),
        transport_addr: addr,
        incarnation,
    });
    let cep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    Rig { _router: router, server, client: NodeRpcClient::new(cep, resolver), target: NodeTarget::ExactNode(node_id), stats, produced, caller_gone, finished }
}

fn count(n: u32, size: u32, mode: Mode) -> CountRequest {
    CountRequest::Count { n, size, mode }
}

/// A trace of the cell's own, continued at the target: the cells share the executable's one span
/// exporter, so each reads only the spans of its trace.
fn traced() -> (String, CallOptions) {
    use std::hash::{BuildHasher, Hasher};
    let half = || std::collections::hash_map::RandomState::new().build_hasher().finish() | 1;
    let id = format!("{:016x}{:016x}", half(), half());
    let context = CallContext { traceparent: Some(format!("00-{id}-b7ad6b7169203331-01")), ..Default::default() };
    (id, CallOptions { context: Some(context), ..Default::default() })
}

/// Every span of `trace` the executable's capture holds.
fn spans_of(trace: &str) -> Vec<SpanData> {
    crate::common::spans_exporter().get_finished_spans().unwrap().into_iter().filter(|s| s.span_context.trace_id().to_string() == trace).collect()
}

/// The one span named `name` in `spans`.
fn one<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    let named: Vec<&SpanData> = spans.iter().filter(|s| s.name == name).collect();
    assert_eq!(named.len(), 1, "one {name} span in the cell's trace; the trace holds {:?}", spans.iter().map(|s| (s.name.to_string(), s.span_context.span_id().to_string(), s.parent_span_id.to_string())).collect::<Vec<_>>());
    named[0]
}

fn attr(s: &SpanData, key: &str) -> Option<String> {
    s.attributes.iter().find(|kv| kv.key.as_str() == key).map(|kv| kv.value.as_str().into_owned())
}

async fn in_flight_drains(stats: &ServerStats) -> bool {
    for _ in 0..100 {
        if ServerStats::get(&stats.in_flight) == 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[tokio::test]
async fn a_stream_delivers_started_then_data_then_its_terminal_in_order() {
    let r = rig().await;
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(5, 8, Mode::Normal), &CallOptions::default()).await.map(|(s, e)| (s.read_only(), e)).map_err(|e| e.0).unwrap();
    let mut kinds = Vec::new();
    let mut items = Vec::new();
    while let Some(item) = s.next().await {
        match item {
            StreamItem::Frame(k, f) => {
                kinds.push(k);
                if let CountFrame::Item { i, .. } = f {
                    items.push(i);
                }
                if let CountFrame::Done { sent, caller_gone } = f {
                    assert_eq!((sent, caller_gone), (5, false));
                }
            }
            StreamItem::Failed(f) => panic!("{f:?}"),
        }
    }
    assert_eq!(kinds.first(), Some(&FrameKind::Started));
    assert_eq!(kinds.last(), Some(&FrameKind::Terminal));
    assert_eq!(items, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn a_refusal_before_started_is_the_only_frame() {
    let r = rig().await;
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(5, 8, Mode::Refuse), &CallOptions::default()).await.map(|(s, e)| (s.read_only(), e)).map_err(|e| e.0).unwrap();
    assert!(matches!(s.next().await, Some(StreamItem::Frame(FrameKind::Refusal(ReplyKind::Busy), _))));
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn cancelling_mid_stream_tells_the_handler_and_releases_the_invocation() {
    let r = rig().await;
    let (mut s, _) =
        r.client.call_stream::<Count>(&r.target, &count(100_000, 32 * 1024, Mode::Normal), &CallOptions::default()).await.map(|(s, e)| (s.read_only(), e)).map_err(|e| e.0).unwrap();
    for _ in 0..3 {
        assert!(matches!(s.next().await, Some(StreamItem::Frame(_, _))));
    }
    drop(s);
    tokio::time::timeout(Duration::from_secs(10), r.finished.notified()).await.expect("the handler ends once the caller is gone");
    assert_eq!(r.caller_gone.load(Ordering::SeqCst), 1, "the sink answered CallerGone");
    assert!(r.produced.load(Ordering::SeqCst) < 100_000, "production stopped early");
    assert!(in_flight_drains(&r.stats).await, "the invocation and its admission are released");
}

#[tokio::test]
async fn a_slow_consumer_holds_the_producer_without_unbounded_buffering() {
    let r = rig().await;
    let (n, size) = (2_000u32, 64 * 1024u32); // 128 MiB if nothing held the producer back
    // The whole 128 MiB is read at the end: the budget covers that transfer (this cell proves the
    // producer is held, not a throughput), so the default unary reply bound does not apply.
    let budget = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(120) }, ..Default::default() };
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(n, size, Mode::Normal), &budget).await.map(|(s, e)| (s.read_only(), e)).map_err(|e| e.0).unwrap();
    assert!(matches!(s.next().await, Some(StreamItem::Frame(FrameKind::Started, _))));
    tokio::time::sleep(Duration::from_millis(800)).await;
    let ahead = r.produced.load(Ordering::SeqCst);
    assert!(ahead < u64::from(n), "the producer was held: {ahead} of {n} frames written while nothing was read");
    assert!(ahead * u64::from(size) < 32 * 1024 * 1024, "at most a flow-control window ahead: {} bytes", ahead * u64::from(size));
    let mut items = 0u32;
    while let Some(item) = s.next().await {
        match item {
            StreamItem::Frame(FrameKind::Data, _) => items += 1,
            StreamItem::Frame(_, _) => {}
            StreamItem::Failed(f) => panic!("{f:?}"),
        }
    }
    assert_eq!(items, n, "every frame arrives once the consumer reads");
}

#[tokio::test]
async fn a_final_frame_that_breaks_the_order_is_refused_and_the_caller_sees_indeterminate() {
    let r = rig().await;
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(2, 8, Mode::BadFinal), &CallOptions::default()).await.map(|(s, e)| (s.read_only(), e)).map_err(|e| e.0).unwrap();
    let mut last = None;
    while let Some(item) = s.next().await {
        last = Some(item);
    }
    assert!(
        matches!(last, Some(StreamItem::Failed(StreamFailure::Indeterminate(IndeterminateReason::Reset(424))))),
        "424 after Started without a terminal: {last:?}"
    );
}

#[tokio::test]
async fn a_handler_that_panics_after_started_leaves_the_caller_indeterminate() {
    let r = rig().await;
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(2, 8, Mode::Panic), &CallOptions::default()).await.map(|(s, e)| (s.read_only(), e)).map_err(|e| e.0).unwrap();
    let mut last = None;
    while let Some(item) = s.next().await {
        last = Some(item);
    }
    assert!(matches!(last, Some(StreamItem::Failed(StreamFailure::Indeterminate(IndeterminateReason::Reset(423))))), "{last:?}");
    assert!(in_flight_drains(&r.stats).await);
}

/// CONTRACT: a stream's end is a step of its serve: the terminal frame the runtime writes once the
/// handler returns is named under the stream's serve span, in the caller's trace, and the serve span
/// ends once that terminal is written. Must NOT happen: the terminal span in a trace of its own with no
/// parent, attributable to no call. Its trace: the caller's remote span, the serve span under it, and
/// the terminal (five frames: Started, three items, the terminal) under the serve span.
#[tokio::test]
async fn a_streams_terminal_is_named_under_its_serve_span_in_the_callers_trace() {
    crate::common::spans_exporter();
    let r = rig().await;
    let (trace, opts) = traced();
    let _left = crate::common::TraceArtifact::new(&trace);
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(3, 8, Mode::Normal), &opts).await.map(|(s, e)| (s.read_only(), e)).map_err(|e| e.0).unwrap();
    while let Some(item) = s.next().await {
        if let StreamItem::Failed(f) = item {
            panic!("{f:?}");
        }
    }
    // The serve is over once its reply is settled: every span it opened has ended.
    r.server.settled().await;
    let spans = spans_of(&trace);
    let serve = one(&spans, "rdm.node_rpc.stream.serve.via-direct");
    let terminal = one(&spans, "rdm.node_rpc.stream.serve.via-terminal");
    assert_eq!(terminal.parent_span_id, serve.span_context.span_id(), "the terminal is a child of the stream's serve span");
    assert_eq!(attr(terminal, "frames").as_deref(), Some("5"), "Started, three items and the terminal");
    assert!(terminal.end_time <= serve.end_time, "the serve span ends once its terminal is written");
}

/// CONTRACT: a stream that does not end in its terminal is refused by name under its serve span, in
/// the caller's trace: a handler that panics after Started is an internal RPC failure naming the
/// panic, and a handler that ends with a Data frame is a protocol violation naming the missing
/// terminal. Must NOT happen: either refusal in a trace of its own with no parent. Its trace, per call:
/// the serve span under the caller's remote span, the refusal under the serve span.
#[tokio::test]
async fn a_stream_that_panics_or_ends_without_its_terminal_is_refused_under_its_serve_span() {
    crate::common::spans_exporter();
    let r = rig().await;
    for (mode, refusal, key, names) in [
        (Mode::Panic, "rdm.node_rpc.handler.reject.via-internal-rpc-failure", "fault", "panic"),
        (Mode::BadFinal, "rdm.node_rpc.stream.reject.via-protocol-violation", "reason", "not its terminal"),
    ] {
        let (trace, opts) = traced();
        let _left = crate::common::TraceArtifact::new(&trace);
        let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(2, 8, mode), &opts).await.map(|(s, e)| (s.read_only(), e)).map_err(|e| e.0).unwrap();
        while s.next().await.is_some() {}
        r.server.settled().await;
        let spans = spans_of(&trace);
        let serve = one(&spans, "rdm.node_rpc.stream.serve.via-direct");
        let refused = one(&spans, refusal);
        assert_eq!(refused.parent_span_id, serve.span_context.span_id(), "{mode:?}: the refusal is a child of the stream's serve span");
        let named = attr(refused, key).unwrap_or_default();
        assert!(named.contains(names), "{mode:?}: the refusal's {key} names {names:?}: {named:?}");
    }
}
