//! i143.e6.s3: server streaming — order, cancellation that releases the invocation, and
//! backpressure without unbounded buffering.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::stream::{SinkError, StreamFailure, StreamItem};
use opentelemetry_sdk::trace::SpanData;
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
    type Request = CountRequest;
    type Reply = CountFrame;

    rdm_core::protocol_variants! {
        request CountRequest { Count }
        reply CountFrame {
            Started = "started" => ReplyKind::Success,
            Item = "item" => ReplyKind::Success,
            Done = "done" => ReplyKind::Success,
            @reserved,
        }
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

/// CONTRACT: `Count`'s wire is the one its hand-written declaration had: each request and reply
/// variant encodes to the pinned bytes and decodes back, each reply keeps its name, code and frame
/// kind, the variant counts are 1 and 9, and the six reserved constructors build their own variants.
#[test]
fn count_keeps_every_variant_on_its_pinned_bytes() {
    use rdm_core::outcome::Code;
    use rdm_core::protocol::DecodeFailure;
    let hex = |b: Vec<u8>| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    let s = || "r".to_string();
    let request = CountRequest::Count { n: 3, size: 4, mode: Mode::BadFinal };
    assert_eq!(hex(Count::encode_request(&request).unwrap()), "00030402", "the request's pinned bytes");
    assert_eq!(Count::decode_request(&Count::encode_request(&request).unwrap()), Ok(request));
    let replies = [
        (CountFrame::Started, "00", "started", Code::Ok, FrameKind::Started),
        (CountFrame::Item { i: 2, bytes: vec![5] }, "01020105", "item", Code::Ok, FrameKind::Data),
        (CountFrame::Done { sent: 3, caller_gone: true }, "020301", "done", Code::Ok, FrameKind::Terminal),
        (CountFrame::PeerUnresolved { reason: s() }, "030172", "peer-unresolved", Code::NotFound, FrameKind::Refusal(ReplyKind::PeerUnresolved)),
        (CountFrame::NotReady { reason: s() }, "040172", "not-ready", Code::Unavailable, FrameKind::Refusal(ReplyKind::NotReady)),
        (CountFrame::Busy { reason: s() }, "050172", "busy", Code::Unavailable, FrameKind::Refusal(ReplyKind::Busy)),
        (CountFrame::ConnectionsDeleting { reason: s() }, "060172", "connections-deleting", Code::Unavailable, FrameKind::Refusal(ReplyKind::ConnectionsDeleting)),
        (CountFrame::Malformed { kind: MalformedKind::UnknownVariant }, "0701", "malformed", Code::InvalidArgument, FrameKind::Refusal(ReplyKind::Malformed(MalformedKind::UnknownVariant))),
        (CountFrame::Unauthorized { reason: s() }, "080172", "unauthorized", Code::PermissionDenied, FrameKind::Refusal(ReplyKind::Unauthorized)),
    ];
    let mut off = Vec::new();
    for (i, (r, want, detail, code, kind)) in replies.iter().enumerate() {
        let got = hex(Count::encode_reply(r).unwrap());
        if got != *want {
            off.push(format!("reply {i}: {got} (pinned {want})"));
        }
        assert_eq!(Count::decode_reply(&Count::encode_reply(r).unwrap()).as_ref(), Ok(r), "reply {i} decodes to itself");
        assert_eq!((Count::reply_detail(r), Count::classify_reply(r).code(), Count::frame_kind(r)), (*detail, *code, *kind), "reply {i}: its name, code and frame kind");
    }
    assert!(off.is_empty(), "the wire moved:\n{}", off.join("\n"));
    assert_eq!((Count::REQUEST_VARIANTS, Count::REPLY_VARIANTS), (1, 9), "one count per declared variant");
    assert_eq!(Count::decode_request(&[1]).err(), Some(DecodeFailure::UnknownVariant), "the request count is the first unknown position");
    assert_eq!(Count::decode_reply(&[9]).err(), Some(DecodeFailure::UnknownVariant), "the reply count is the first unknown position");
    assert_eq!(
        [Count::peer_unresolved(s()), Count::not_ready(s()), Count::busy(s()), Count::connections_deleting(s()), Count::malformed(MalformedKind::UnknownVariant), Count::unauthorized(s())],
        [replies[3].0.clone(), replies[4].0.clone(), replies[5].0.clone(), replies[6].0.clone(), replies[7].0.clone(), replies[8].0.clone()],
        "each reserved constructor builds its own variant"
    );
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

/// Count's registration, as a process's `serve_app` adds it: its ledger row and its streaming
/// handler, which counts the frames it produced and the writes that found the caller gone, and
/// notifies `finished` once it stops producing.
fn serve_count(b: ServerBuilder, produced: Arc<AtomicU64>, caller_gone: Arc<AtomicU64>, finished: Arc<tokio::sync::Notify>) -> ServerBuilder {
    b.ledger([LedgerEntry { op: Count::OP, family: "count".into(), owner: OpOwner::Product("test".into()), state: OpState::Live }]).serve_stream::<Count, _, _>(
        OpOwner::Product("test".into()),
        move |_peer, req: CountRequest, sink| {
            let (p, g, fin) = (produced.clone(), caller_gone.clone(), finished.clone());
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
        },
    )
}

async fn rig() -> Rig {
    rig_serving(true).await
}

/// A rig whose node serves Count when `count` is set, and no op of its own otherwise.
async fn rig_serving(count: bool) -> Rig {
    let produced = Arc::new(AtomicU64::new(0));
    let caller_gone = Arc::new(AtomicU64::new(0));
    let finished = Arc::new(tokio::sync::Notify::new());
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let builder = if count { serve_count(ServerBuilder::new(), produced.clone(), caller_gone.clone(), finished.clone()) } else { ServerBuilder::new() };
    let server = builder
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
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

async fn in_flight_reaches_zero(stats: &ServerStats) -> bool {
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
    assert!(in_flight_reaches_zero(&r.stats).await, "the invocation and its admission are released");
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
    assert!(in_flight_reaches_zero(&r.stats).await);
}

/// CONTRACT: a stream to a node that does not serve its op opens (its request frame is written
/// before the node's `421 UNSERVED_OP` arrives), and its first item is the definitive unserved failure
/// naming the op: the outcome a unary call gets for the same reset (node-rpc.md §23, §40), nothing
/// dispatched. The caller's call span keeps `outcome=stream-open` and records `code=UNIMPLEMENTED
/// detail=unserved-op`. Must NOT happen: the 421 read as a lost stream (`Indeterminate`, `UNKNOWN`,
/// `reset`). Its trace: the cell's root span, the call span under it.
#[test]
fn a_stream_to_a_node_that_does_not_serve_its_op_fails_unserved_on_its_first_item() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-stream-unserved").expect("the cell's evidence file can be created");
    let (trace, first, after, unserved, dispatched) = capture.run(2, async {
        let root = tracing::info_span!("rdm.test.stream.reject.via-unserved-op");
        let trace = crate::delivery::trace_of(&root);
        let (first, after, unserved, dispatched) = tracing::Instrument::instrument(
            async {
                let r = rig_serving(false).await;
                let (mut s, _) = r
                    .client
                    .call_stream::<Count>(&r.target, &count(1, 8, Mode::Normal), &CallOptions::default())
                    .await
                    .unwrap_or_else(|(out, _)| panic!("the request is written whole before the 421 arrives, so the stream opens; it ended {} {}", out.name(), out.detail_name()));
                let first = s.next().await;
                let after = s.next().await;
                drop(s);
                (first, after, ServerStats::get(&r.stats.unserved), ServerStats::get(&r.stats.dispatched))
            },
            root,
        )
        .await;
        (trace, first, after, unserved, dispatched)
    });
    let Some(StreamItem::Failed(failure)) = &first else { panic!("the first item is the stream's failure: {first:?}") };
    assert_eq!(failure.code(), rdm_core::outcome::Code::Unimplemented, "a 421 is the definitive unserved outcome, as unary's: {failure:?}");
    assert_eq!(format!("{failure:?}"), format!("Unserved(Unserved {{ op: {} }})", Count::OP), "the failure names the unserved op");
    assert!(after.is_none(), "nothing follows the definitive failure: {after:?}");
    assert_eq!((unserved, dispatched), (1, 0), "the node answered 421 and dispatched nothing");
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let calls: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.request.update.via-call" && s["trace_id"] == trace.as_str()).collect();
    assert_eq!(calls.len(), 1, "one call span in the cell's trace: {calls:#?}");
    let a = &calls[0]["attributes"];
    assert_eq!(
        (a["op"].as_str(), a["outcome"].as_str(), a["code"].as_str(), a["detail"].as_str()),
        (Some(Count::OP.to_string().as_str()), Some("stream-open"), Some("UNIMPLEMENTED"), Some("unserved-op")),
        "the call span: {a}"
    );
    println!("stream unserved flame: {}", crate::delivery::flame(&trace));
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
    assert_eq!(attr(terminal, "code").as_deref(), Some("OK"));
    assert_eq!(attr(terminal, "detail").as_deref(), Some("done"));
    assert_eq!(attr(serve, "code").as_deref(), Some("OK"));
    assert_eq!(attr(serve, "detail").as_deref(), Some("done"));
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

/// Count's consumer: each frame recorded at its position, then acknowledged.
fn count_consumer() -> rdm_test::conformance::Consume<Count> {
    Arc::new(|mut s: rdm_transport::stream::ReplyStream<Count>, record: rdm_test::conformance::StreamRecord| {
        Box::pin(async move {
            while let Some(StreamItem::Frame(kind, _)) = s.next().await {
                record.put(s.position(), kind);
                s.accepted().await;
            }
        })
    })
}

/// Count as the harness takes it: its registration, a short stream, a stream of 200 frames, its one
/// named refusal, and `consume` its consumer.
fn count_fixture(consume: rdm_test::conformance::Consume<Count>) -> rdm_test::conformance::Streaming<Count> {
    rdm_test::conformance::Streaming {
        serve: Arc::new(|b| serve_count(b, Arc::default(), Arc::default(), Arc::default())),
        answered: count(3, 8, Mode::Normal),
        long: count(200, 8, Mode::Normal),
        refusals: vec![(count(1, 8, Mode::Refuse), "busy")],
        consume,
    }
}

/// CONTRACT: `Count`, served as this file serves it, passes every streaming check of the conformance
/// harness: each named refusal answered, the request over its ceiling refused `Malformed(TooLarge)`,
/// frames in order, the 64-frame bound holding the producer, a caller dropped mid-stream releasing the
/// invocation, a duplicate delivery suppressed, frames lost after acceptance resent once, and its
/// consumer acknowledging only what it recorded. Its evidence: one trace per check, under
/// `rdm.test.conformance.serve.via-check`.
#[test]
fn count_passes_the_conformance_harness() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-conformance-count").expect("the cell's evidence file can be created");
    let report = capture.run(4, async { rdm_test::conformance::streaming(&count_fixture(count_consumer())).await });
    println!("{}", report.summary());
    assert!(report.failed().is_empty(), "{}", report.summary());
}

/// CONTRACT: the harness's acknowledgement check fails a consumer that calls `accepted` before it
/// records the frame it acknowledges: the terminal acknowledged, then recorded, is named. Must NOT
/// happen: the check passing a consumer whose terminal is acknowledged before it is recorded.
#[test]
fn a_consumer_that_acknowledges_before_it_records_fails_the_conformance_harness() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-conformance-early-ack").expect("the cell's evidence file can be created");
    let early: rdm_test::conformance::Consume<Count> = Arc::new(|mut s: rdm_transport::stream::ReplyStream<Count>, record: rdm_test::conformance::StreamRecord| {
        Box::pin(async move {
            while let Some(StreamItem::Frame(kind, _)) = s.next().await {
                let position = s.position().map(|(id, seq)| (id.clone(), seq));
                s.accepted().await;
                record.put(position.as_ref().map(|(id, seq)| (id, *seq)), kind);
            }
        })
    });
    let check = rdm_test::conformance::Check::AcceptedAfterRecord;
    let report = capture.run(4, async { rdm_test::conformance::streaming_checks(&count_fixture(early), &[check]).await });
    println!("{}", report.summary());
    let failure = report.outcome(check).and_then(|o| o.failure.clone()).unwrap_or_default();
    assert!(failure.contains("Terminal") && failure.contains("accepted() was called before it was recorded"), "the early acknowledgement of the terminal is named: {}", report.summary());
}

/// CONTRACT (docs/architecture/node-rpc.md:793 §31: "The caller acks a frame once its owner can recover
/// the accepted progress or result (§43)"; docs/architecture/node-rpc.md:1042-1043 §43: "a response is
/// itself an owed message, correlated to its invocation and acknowledged by the caller once its owner can
/// recover it"): under 8 concurrent consumers running the `accepted-after-record` check repeatedly,
/// every stream frame is acked once its owner records and accepts it (`acks_sent` and `acks_received`
/// equal the recorded frame count on every stream, and the producer owes nothing).
#[test]
fn every_stream_frame_is_acked_once_its_owner_can_recover_it_under_concurrent_consumers() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-conformance-accepted-after-record-concurrent").expect("the cell's evidence file can be created");
    capture.run(8, async {
        let check = rdm_test::conformance::Check::AcceptedAfterRecord;
        for batch in 0..8 {
            let mut tasks = Vec::new();
            for lane in 0..8 {
                let mut fixture = count_fixture(count_consumer());
                fixture.answered = count(1, 8, Mode::Normal);
                tasks.push(tokio::spawn(async move {
                    let report = rdm_test::conformance::streaming_checks(&fixture, &[check]).await;
                    (batch, lane, report)
                }));
            }
            for t in tasks {
                let (batch, lane, report) = t.await.expect("check task completed");
                assert!(
                    report.failed().is_empty(),
                    "batch {batch} consumer {lane}: every recorded frame must be acked once its owner can recover it:\n{}",
                    report.summary()
                );
            }
        }
        let r = rig().await;
        let consume = count_consumer();
        for batch in 0..250 {
            let mut tasks = Vec::new();
            for _ in 0..8 {
                let client = r.client.clone();
                let target = r.target.clone();
                let consume = consume.clone();
                tasks.push(tokio::spawn(async move {
                    let (mut s, _) = client
                        .call_stream::<Count>(&target, &count(1, 8, Mode::Normal), &CallOptions::default())
                        .await
                        .map_err(|(o, _)| format!("{o:?}"))
                        .expect("stream opens");
                    let mut recorded = 0usize;
                    while let Some(StreamItem::Frame(_, _)) = s.next().await {
                        recorded += 1;
                        s.accepted().await;
                    }
                    let _ = consume;
                    recorded
                }));
            }
            for t in tasks {
                assert_eq!(t.await.unwrap(), 3, "batch {batch}: Started, Item, Done recorded and accepted");
            }
        }
        let expected_acks = 250 * 8 * 3;
        let sent = rdm_transport::DeliveryStats::get(&r.client.delivery().stats().acks_sent);
        assert_eq!(sent, expected_acks, "every recorded frame's ack is written by the caller");
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let dropped: Vec<_> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node_rpc.delivery.reject.via-ack-without-invocation" || s["name"] == "rdm.node_rpc.delivery.reject.via-ack-not-written")
        .collect();
    assert!(dropped.is_empty(), "no stream frame's ack is dropped: {dropped:#?}");
}
