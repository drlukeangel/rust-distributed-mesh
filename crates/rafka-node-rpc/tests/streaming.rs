//! i143.e6.s3: server streaming — order, cancellation that releases the invocation, and
//! backpressure without unbounded buffering.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::stream::{SinkError, StreamFailure, StreamItem};
use rafka_node_rpc::{ServedBirth, CallOptions, HandlerFault, NodeRpcClient, NodeTarget, ResolvedNode, ServerBuilder, ServerStats, StaticResolver};
use rafka_node_rpc_contract::catalog::{LedgerEntry, TagOwner, TagState};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, MalformedKind, ReplyKind};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_node_rpc_contract::streaming::{FrameKind, StreamingProtocol};
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
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Count {
    const TAG: u8 = 0x5D;
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
            CountFrame::Draining { .. } => ReplyKind::Draining,
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
    fn draining(reason: String) -> CountFrame {
        CountFrame::Draining { reason }
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
        .ledger([LedgerEntry { tag: Count::TAG, family: "count".into(), owner: TagOwner::Product("test".into()), state: TagState::Live }])
        .serve_stream::<Count, _, _>(TagOwner::Product("test".into()), move |_peer, req: CountRequest, sink| {
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
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let stats = server.stats();
    let key = SecretKey::generate();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode {
        node_id: node_id.clone(),
        name: "mesh1.rpc.1".parse().unwrap(),
        endpoint_id: key.public(),
        transport_addr: addr,
        incarnation,
    });
    let cep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    Rig { _router: router, client: NodeRpcClient::new(cep, resolver), target: NodeTarget::ExactNode(node_id), stats, produced, caller_gone, finished }
}

fn count(n: u32, size: u32, mode: Mode) -> CountRequest {
    CountRequest::Count { n, size, mode }
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
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(5, 8, Mode::Normal), &CallOptions::default()).await.map_err(|e| e.0).unwrap();
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
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(5, 8, Mode::Refuse), &CallOptions::default()).await.map_err(|e| e.0).unwrap();
    assert!(matches!(s.next().await, Some(StreamItem::Frame(FrameKind::Refusal(ReplyKind::Busy), _))));
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn cancelling_mid_stream_tells_the_handler_and_releases_the_invocation() {
    let r = rig().await;
    let (mut s, _) =
        r.client.call_stream::<Count>(&r.target, &count(100_000, 32 * 1024, Mode::Normal), &CallOptions::default()).await.map_err(|e| e.0).unwrap();
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
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(n, size, Mode::Normal), &CallOptions::default()).await.map_err(|e| e.0).unwrap();
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
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(2, 8, Mode::BadFinal), &CallOptions::default()).await.map_err(|e| e.0).unwrap();
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
    let (mut s, _) = r.client.call_stream::<Count>(&r.target, &count(2, 8, Mode::Panic), &CallOptions::default()).await.map_err(|e| e.0).unwrap();
    let mut last = None;
    while let Some(item) = s.next().await {
        last = Some(item);
    }
    assert!(matches!(last, Some(StreamItem::Failed(StreamFailure::Indeterminate(IndeterminateReason::Reset(423))))), "{last:?}");
    assert!(in_flight_drains(&r.stats).await);
}
