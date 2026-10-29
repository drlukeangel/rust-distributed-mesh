//! i146.e1: the delivery layer (node-rpc.md §43, §33; node-rpc-envelope.md "Transport frames").
//!
//! Real endpoints on 127.0.0.1, the production frames, a shortened schedule (the publish cadence
//! 100 ms, the floor 800 ms, no jitter). Each cell asserts the caller's outcome and both sides'
//! counters, so "dispatched once" and "acked without dispatch" are proven on the receiver, never
//! inferred from the caller. A "responder" built from raw frames stands in where a cell needs a
//! frame no conforming responder writes (out of order, conflicting, an ack stream).

use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::SecretKey;
use rdm_core::catalog::{LedgerEntry, OpOwner, OpState};
use rdm_core::framing::{
    decode_header, decode_request, encode_ack_frame, encode_request_frame, encode_response_frame, Birth, DeliveryId, HeaderRead, RequestHeader, Target, TransportHeader,
};
use rdm_core::outcome::{IndeterminateReason, MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use rdm_core::streaming::{FrameKind, StreamingProtocol};
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::stream::{StreamFailure, StreamItem};
use rdm_transport::{
    Budget, CallOptions, CarriedIdentity, Delivery, DeliveryStats, HandlerFault, NodeRpcClient, NodeRpcServer, NodeTarget, Pacing, PeerContext, ReplyWithhold, ResolvedNode, ServedBirth,
    ServerBuilder, ServerStats, StaticResolver,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Instant;

const CADENCE: Duration = Duration::from_millis(100);
const FLOOR: Duration = Duration::from_millis(800);

fn pacing() -> Pacing {
    Pacing { cadence: CADENCE, floor: FLOOR, jitter: false }
}

fn birth() -> Birth {
    Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }
}

fn get(c: &AtomicU64) -> u64 {
    DeliveryStats::get(c)
}

// ---- a family that changes something, and one that streams ----

/// `Run` applies once per dispatch; `Read` only reads (acknowledged as it is dispatched).
struct Command;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum CommandRequest {
    Run { tag: u32, mode: Mode },
    Read { tag: u32, hold: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Mode {
    /// Reply at once: the reply acks the request.
    Apply,
    /// Hold until released, then reply: nothing acks the request before.
    Hold,
    /// Declare acceptance at once (the request is acked), then hold until released.
    AcceptThenHold,
    /// Hold until released, then declare acceptance and reply.
    HoldThenAccept,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum CommandReply {
    Ran { tag: u32, runs: u64 },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Command {
    const OP: u8 = 0x5B;
    const NAME: &'static str = "command";
    const MAX_REQUEST_FRAME_BYTES: usize = 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 1024;
    const FORWARDABLE: bool = true;
    const REQUEST_VARIANTS: u32 = 2;
    const REPLY_VARIANTS: u32 = 7;
    type Request = CommandRequest;
    type Reply = CommandReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            CommandReply::Ran { .. } => "ran",
            CommandReply::PeerUnresolved { .. } => "peer-unresolved",
            CommandReply::NotReady { .. } => "not-ready",
            CommandReply::Busy { .. } => "busy",
            CommandReply::ConnectionsDeleting { .. } => "connections-deleting",
            CommandReply::Malformed { .. } => "malformed",
            CommandReply::Unauthorized { .. } => "unauthorized",
        }
    }
    fn read_only(req: &CommandRequest) -> bool {
        matches!(req, CommandRequest::Read { .. })
    }
    fn classify_reply(r: &CommandReply) -> ReplyKind {
        match r {
            CommandReply::Ran { .. } => ReplyKind::Success,
            CommandReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            CommandReply::NotReady { .. } => ReplyKind::NotReady,
            CommandReply::Busy { .. } => ReplyKind::Busy,
            CommandReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            CommandReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            CommandReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> CommandReply {
        CommandReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> CommandReply {
        CommandReply::NotReady { reason }
    }
    fn busy(reason: String) -> CommandReply {
        CommandReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> CommandReply {
        CommandReply::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> CommandReply {
        CommandReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> CommandReply {
        CommandReply::Unauthorized { reason }
    }
}

/// A streaming family the raw responder answers: `Started`, `Item`s, `Done`.
struct Tick;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum TickRequest {
    Tick,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum TickFrame {
    Started,
    Item { i: u32 },
    Done,
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Tick {
    const OP: u8 = 0x5A;
    const NAME: &'static str = "tick";
    const MAX_REQUEST_FRAME_BYTES: usize = 64;
    const MAX_REPLY_FRAME_BYTES: usize = 16;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 9;
    type Request = TickRequest;
    type Reply = TickFrame;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            TickFrame::Started => "started",
            TickFrame::Item { .. } => "item",
            TickFrame::Done => "done",
            TickFrame::PeerUnresolved { .. } => "peer-unresolved",
            TickFrame::NotReady { .. } => "not-ready",
            TickFrame::Busy { .. } => "busy",
            TickFrame::ConnectionsDeleting { .. } => "connections-deleting",
            TickFrame::Malformed { .. } => "malformed",
            TickFrame::Unauthorized { .. } => "unauthorized",
        }
    }
    fn classify_reply(r: &TickFrame) -> ReplyKind {
        match r {
            TickFrame::Started | TickFrame::Item { .. } | TickFrame::Done => ReplyKind::Success,
            TickFrame::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            TickFrame::NotReady { .. } => ReplyKind::NotReady,
            TickFrame::Busy { .. } => ReplyKind::Busy,
            TickFrame::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            TickFrame::Malformed { kind } => ReplyKind::Malformed(*kind),
            TickFrame::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> TickFrame {
        TickFrame::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> TickFrame {
        TickFrame::NotReady { reason }
    }
    fn busy(reason: String) -> TickFrame {
        TickFrame::Busy { reason }
    }
    fn connections_deleting(reason: String) -> TickFrame {
        TickFrame::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> TickFrame {
        TickFrame::Malformed { kind }
    }
    fn unauthorized(reason: String) -> TickFrame {
        TickFrame::Unauthorized { reason }
    }
}

impl StreamingProtocol for Tick {
    fn frame_kind(f: &TickFrame) -> FrameKind {
        match f {
            TickFrame::Started => FrameKind::Started,
            TickFrame::Item { .. } => FrameKind::Data,
            TickFrame::Done => FrameKind::Terminal,
            other => FrameKind::Refusal(Tick::classify_reply(other)),
        }
    }
}

fn ledger() -> Vec<LedgerEntry> {
    vec![
        LedgerEntry { op: Command::OP, family: "command".into(), owner: OpOwner::Product("test".into()), state: OpState::Live },
        LedgerEntry { op: Tick::OP, family: "tick".into(), owner: OpOwner::Product("test".into()), state: OpState::Live },
    ]
}

// ---- the responder that serves Command ----

struct Served {
    _router: Router,
    server: NodeRpcServer,
    delivery: Delivery,
    node: ResolvedNode,
    stats: Arc<ServerStats>,
    /// How many times a `Run` handler ran.
    runs: Arc<AtomicU64>,
    /// The identity each dispatched request carried.
    seen: Arc<Mutex<Vec<Option<CarriedIdentity>>>>,
    /// Releases one held handler per permit.
    gate: Arc<Semaphore>,
}

impl Served {
    fn release(&self) {
        self.gate.add_permits(1);
    }

    fn target(&self) -> NodeTarget {
        NodeTarget::ExactNode(self.node.node_id.clone())
    }
}

async fn served_with(build: impl FnOnce(ServerBuilder) -> ServerBuilder) -> Served {
    let key = SecretKey::generate();
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    served_on(birth(), key, ep, build).await
}

/// The responder as birth `me`, using the socket its endpoint already owns.
async fn served_on(me: Birth, key: SecretKey, ep: iroh::Endpoint, build: impl FnOnce(ServerBuilder) -> ServerBuilder) -> Served {
    let delivery = Delivery::with_pacing(me.clone(), pacing());
    let runs = Arc::new(AtomicU64::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(Semaphore::new(0));
    let (r, s, g) = (runs.clone(), seen.clone(), gate.clone());
    let builder = ServerBuilder::new().ledger(ledger()).serve::<Command, _, _>(OpOwner::Product("test".into()), move |peer: PeerContext, req| {
        let (r, s, g) = (r.clone(), s.clone(), g.clone());
        async move {
            s.lock().unwrap().push(peer.delivery.clone());
            match req {
                CommandRequest::Run { tag, mode } => {
                    let runs = r.fetch_add(1, Ordering::SeqCst) + 1;
                    match mode {
                        Mode::Apply => {}
                        Mode::Hold => g.acquire().await.unwrap().forget(),
                        Mode::AcceptThenHold => {
                            peer.acceptance.accepted();
                            g.acquire().await.unwrap().forget();
                        }
                        Mode::HoldThenAccept => {
                            g.acquire().await.unwrap().forget();
                            peer.acceptance.accepted();
                        }
                    }
                    Ok::<_, HandlerFault>(CommandReply::Ran { tag, runs })
                }
                CommandRequest::Read { tag, hold } => {
                    if hold {
                        g.acquire().await.unwrap().forget();
                    }
                    Ok(CommandReply::Ran { tag, runs: r.load(Ordering::SeqCst) })
                }
            }
        }
    });
    let server = build(builder).with_delivery(delivery.clone()).build(ServedBirth { node_id: me.node_id.to_string(), incarnation: me.incarnation.to_string() }).unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server.clone()).spawn();
    let node = ResolvedNode { node_id: me.node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation: me.incarnation.clone() };
    Served { _router: router, stats: server.stats(), server, delivery, node, runs, seen, gate }
}

async fn served() -> Served {
    served_with(|b| b).await
}

async fn client_of(nodes: &[&ResolvedNode]) -> (NodeRpcClient, Arc<StaticResolver>) {
    let resolver = Arc::new(StaticResolver::new());
    for n in nodes {
        resolver.insert((*n).clone());
    }
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep, resolver.clone()).with_delivery(Delivery::with_pacing(birth(), pacing()));
    (client, resolver)
}

fn run(tag: u32, mode: Mode) -> CommandRequest {
    CommandRequest::Run { tag, mode }
}

fn budget(d: Duration) -> CallOptions {
    CallOptions { budget: Budget::Overall(d), ..CallOptions::default() }
}

async fn eventually(what: &str, within: Duration, f: impl Fn() -> bool) {
    let until = Instant::now() + within;
    while Instant::now() < until {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("never observed within {within:?}: {what}");
}

/// The trace a span opened under this cell's capture belongs to.
pub(crate) fn trace_of(span: &tracing::Span) -> String {
    rdm_transport::traceparent_of(span).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context: its capture is installed")
}

/// The trace's Jaeger link.
pub(crate) fn flame(trace: &str) -> String {
    format!("{}/trace/{trace}", std::env::var("JAEGER_QUERY_URL").unwrap_or_else(|_| "http://192.168.68.99:16687".into()))
}

fn ran(out: &RpcOutcome<CommandReply>) -> Option<(u32, u64)> {
    match out.reply().map(|r| r.value()) {
        Some(CommandReply::Ran { tag, runs }) => Some((*tag, *runs)),
        _ => None,
    }
}

// ---- the raw responder ----

/// Hands every connection accepted on the Node RPC ALPN to the cell.
#[derive(Debug, Clone)]
struct Accepted(mpsc::UnboundedSender<Connection>);

impl ProtocolHandler for Accepted {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let _ = self.0.send(connection.clone());
        connection.closed().await;
        Ok(())
    }
}

struct Raw {
    _router: Router,
    node: ResolvedNode,
    me: Birth,
    conns: mpsc::UnboundedReceiver<Connection>,
}

async fn raw() -> Raw {
    let me = birth();
    let key = SecretKey::generate();
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let (tx, conns) = mpsc::unbounded_channel();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, Accepted(tx)).spawn();
    let node = ResolvedNode { node_id: me.node_id.clone(), name: "mesh1.rpc.9".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation: me.incarnation.clone() };
    Raw { _router: router, node, me, conns }
}

/// Read one request frame: its header, and the bytes after it once its sections are complete.
async fn read_request(recv: &mut RecvStream) -> (TransportHeader, Vec<u8>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if let HeaderRead::Read(h, at) = decode_header(&buf) {
            if decode_request(&buf[at..], |_| Some(1024)).is_ok() {
                return (h, buf);
            }
        }
        let n = recv.read(&mut chunk).await.unwrap().expect("the request arrives whole");
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Read one frame header from `recv`, with `pending` the bytes read and not consumed.
async fn read_header(recv: &mut RecvStream, pending: &mut Vec<u8>) -> Option<TransportHeader> {
    read_header_chunks::<4096>(recv, pending).await
}

async fn read_header_chunks<const N: usize>(recv: &mut RecvStream, pending: &mut Vec<u8>) -> Option<TransportHeader> {
    let mut chunk = [0u8; N];
    loop {
        match decode_header(pending) {
            HeaderRead::Read(h, used) => {
                pending.drain(..used);
                return Some(h);
            }
            HeaderRead::Bad(why) => panic!("not a header: {why}"),
            HeaderRead::NeedMore => {}
        }
        match recv.read(&mut chunk).await {
            Ok(Some(n)) => pending.extend_from_slice(&chunk[..n]),
            _ => return None,
        }
    }
}

fn response(from: &Birth, delivery_seq: u64, invocation: &DeliveryId, seq: u32, frame: &TickFrame) -> Vec<u8> {
    let h = TransportHeader::Response { sender: from.clone(), delivery_seq, forget_below: 1, invocation_id: invocation.clone(), seq };
    encode_response_frame(&h, &Tick::encode_reply(frame).unwrap())
}

fn command_response(from: &Birth, delivery_seq: u64, invocation: &DeliveryId, reply: &CommandReply) -> Vec<u8> {
    let h = TransportHeader::Response { sender: from.clone(), delivery_seq, forget_below: 1, invocation_id: invocation.clone(), seq: 0 };
    encode_response_frame(&h, &Command::encode_reply(reply).unwrap())
}

async fn write(send: &mut SendStream, bytes: &[u8]) {
    send.write_all(bytes).await.unwrap();
}

// ---- 1-12: the owed request and the owed response ----

/// Cell 1. CONTRACT: a request whose connection drops after it was read and before it was acked is
/// resent with the same identity and acked; its handler runs once. Must NOT happen: a second
/// identity minted, or a second dispatch.
#[tokio::test]
async fn a_request_whose_connection_drops_before_its_ack_is_resent_with_its_identity_and_acked() {
    let s = served().await;
    let (client, _) = client_of(&[&s.node]).await;
    s.delivery.seams().cut_connection_after_next_request();
    let (out, _) = client.call::<Command>(&s.target(), &run(1, Mode::Apply), &budget(Duration::from_secs(5))).await;
    assert_eq!(ran(&out), Some((1, 1)), "{out:?}");
    assert_eq!(client.delivery().minted(), 1, "one identity, resent");
    assert!(get(&client.delivery().stats().resends) >= 1, "the request was resent");
    assert_eq!(get(&s.delivery.stats().requests_new), 2, "the cut request was forgotten undispatched, so its resend arrived first");
    assert_eq!(s.runs.load(Ordering::SeqCst), 1, "dispatched once");
    let seen = s.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].as_ref().map(|c| c.id.delivery_seq), Some(1), "the resend carried the first identity");
}

/// Cell 2. CONTRACT: a lost ack is repaired by the resend: the repeat arrives after acceptance,
/// is acked without dispatch, and the handler runs once.
#[tokio::test]
async fn a_lost_ack_is_repaired_by_the_resend_and_the_handler_runs_once() {
    let s = served().await;
    let (client, _) = client_of(&[&s.node]).await;
    s.delivery.seams().drop_next_request_ack();
    let call = tokio::spawn({
        let target = s.target();
        async move { client.call::<Command>(&target, &run(2, Mode::AcceptThenHold), &budget(Duration::from_secs(5))).await.0 }
    });
    eventually("the repeat after acceptance is acked", Duration::from_secs(3), || get(&s.delivery.stats().duplicates_acked) >= 1).await;
    s.release();
    let out = call.await.unwrap();
    assert_eq!(ran(&out), Some((2, 1)), "{out:?}");
    assert_eq!(s.runs.load(Ordering::SeqCst), 1, "the handler ran once");
    assert_eq!(get(&s.stats.dispatched), 1, "the repeat never reached dispatch");
}

/// Cell 3. CONTRACT: a repeat that arrives while the original executes, before its acceptance, is
/// held pending and unacked; once the original is accepted (the handler's declaration or its reply,
/// whichever comes first) it is acked, never dispatched. Its trace: the call with more than one
/// attempt and its `via-resend`, one serve, and the repeat's `via-ack` (`duplicate`), every one in
/// the caller's trace.
#[test]
fn a_repeat_during_execution_stays_pending_and_unacked_then_is_acked_without_dispatch() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-delivery-repeat").expect("the cell's evidence file can be created");
    let trace = capture.run(4, async {
        let root = tracing::info_span!("rdm.test.delivery.serve.via-repeat-during-execution");
        let trace = trace_of(&root);
        tracing::Instrument::instrument(
            async {
                let s = served().await;
                let (client, _) = client_of(&[&s.node]).await;
                let cd = client.delivery().clone();
                let call = tokio::spawn(tracing::Instrument::instrument(
                    {
                        let target = s.target();
                        async move { client.call::<Command>(&target, &run(3, Mode::HoldThenAccept), &budget(Duration::from_secs(5))).await.0 }
                    },
                    tracing::Span::current(),
                ));
                eventually("a repeat is held pending", Duration::from_secs(3), || get(&s.delivery.stats().duplicates_pending) >= 1).await;
                assert_eq!(get(&cd.stats().acks_received), 0, "nothing acked the request before its acceptance");
                s.release();
                let out = call.await.unwrap();
                assert_eq!(ran(&out), Some((3, 1)), "{out:?}");
                assert_eq!(get(&s.stats.dispatched), 1, "dispatched once");
                assert_eq!(s.runs.load(Ordering::SeqCst), 1);
            },
            root,
        )
        .await;
        trace
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let named = |name: &str| -> Vec<&serde_json::Value> { spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace.as_str()).collect() };
    let calls = named("rdm.node_rpc.request.update.via-call");
    let attempts = |c: &serde_json::Value| c["attributes"]["attempts"].as_u64().or_else(|| c["attributes"]["attempts"].as_str().and_then(|a| a.parse().ok())).unwrap_or(0);
    assert!(calls.iter().any(|c| c["attributes"]["delivery"] == "acked" && attempts(c) >= 2), "the call took more than one attempt: {calls:#?}");
    assert!(!named("rdm.node_rpc.delivery.update.via-resend").is_empty(), "the resend is spanned in the caller's trace");
    assert_eq!(named("rdm.node_rpc.request.serve.via-direct").len(), 1, "one serve: the repeat never reached dispatch");
    let acks: Vec<String> = named("rdm.node_rpc.delivery.serve.via-ack").iter().map(|s| s["attributes"]["acceptance"].as_str().unwrap_or("").to_string()).collect();
    assert!(acks.iter().any(|a| a == "duplicate"), "the repeat's ack, in the caller's trace: {acks:?}");
    println!("delivery flame (a repeat held pending, then acked without dispatch): {}", flame(&trace));
}

/// Cell 4. CONTRACT: a read-only request is acked as it is dispatched: its ack reaches the caller
/// while its handler still holds its reply, and once acked it is never resent; the receiver
/// dispatched it once. A resend sent before the ack arrived is the delivery rule's own (node-rpc.md
/// §43: without an ack, the first retry at the publish cadence, this file's `CADENCE`), suppressed
/// at the receiver, never dispatched. The cell runs on one thread, as a `#[tokio::test]` does, and
/// prints the ack's arrival against the request's write and every `via-resend`, read from its trace.
#[test]
fn a_read_only_request_is_acked_at_once() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-delivery-read-only-ack").expect("the cell's evidence file can be created");
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("the cell's runtime");
    let (trace, (at_ack, after, dispatched)) = rt.block_on(async {
        let root = tracing::info_span!("rdm.test.delivery.serve.via-read-only-ack");
        let trace = trace_of(&root);
        let seen = tracing::Instrument::instrument(
            async {
                let s = served().await;
                let (client, _) = client_of(&[&s.node]).await;
                let cd = client.delivery().clone();
                let call = tokio::spawn(tracing::Instrument::instrument(
                    {
                        let target = s.target();
                        async move { client.call::<Command>(&target, &CommandRequest::Read { tag: 4, hold: true }, &budget(Duration::from_secs(5))).await.0 }
                    },
                    tracing::Span::current(),
                ));
                let until = Instant::now() + Duration::from_secs(1);
                while get(&cd.stats().acks_received) == 0 {
                    assert!(Instant::now() < until, "the read was not acked within 1 s while its handler held its reply");
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                tracing::info!(step = "ack-observed", "the read's ack reached the caller while its handler holds the reply");
                let at_ack = get(&cd.stats().resends);
                tokio::time::sleep(CADENCE * 4).await;
                let after = get(&cd.stats().resends);
                let dispatched = get(&s.stats.dispatched);
                s.release();
                let out = call.await.unwrap();
                assert_eq!(ran(&out), Some((4, 0)), "{out:?}");
                (at_ack, after, dispatched)
            },
            root,
        )
        .await;
        (trace, seen)
    });
    drop(rt);
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let in_trace = |name: &str| -> Vec<&serde_json::Value> { spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace.as_str()).collect() };
    let nano = |v: &serde_json::Value| v.as_u64().unwrap_or(0);
    let written = in_trace("rdm.node_rpc.request.update.via-call")
        .iter()
        .flat_map(|c| c["events"].as_array().cloned().unwrap_or_default())
        .find(|e| e["attributes"]["step"] == "stream-opened")
        .map(|e| nano(&e["time_unix_nano"]))
        .unwrap_or(0);
    let acked = in_trace("rdm.test.delivery.serve.via-read-only-ack")
        .iter()
        .flat_map(|r| r["events"].as_array().cloned().unwrap_or_default())
        .find(|e| e["attributes"]["step"] == "ack-observed")
        .map(|e| nano(&e["time_unix_nano"]))
        .unwrap_or(0);
    let at = |t: u64| (i128::from(t) - i128::from(written)) / 1_000_000;
    let served_at: Vec<i128> = in_trace("rdm.node_rpc.request.serve.via-direct").iter().map(|s| at(nano(&s["start_unix_nano"]))).collect();
    let resent_at: Vec<i128> = in_trace("rdm.node_rpc.delivery.update.via-resend").iter().map(|s| at(nano(&s["start_unix_nano"]))).collect();
    let timeline = format!("from the request's stream open: dispatched at {served_at:?} ms, ack observed at {} ms, resends at {resent_at:?} ms (cadence {} ms)", at(acked), CADENCE.as_millis());
    assert_eq!(after, at_ack, "an acked request is never resent: {at_ack} resends when its ack arrived, {after} after; {timeline}");
    assert_eq!(dispatched, 1, "dispatched once: a resend before the ack is suppressed at the receiver; {timeline}");
    println!("delivery flame (a read-only request acked as it is dispatched): {}; {timeline}", flame(&trace));
}

/// Cell 5. CONTRACT: a named refusal ends the request's retry, and the refusal is itself delivered
/// until the caller acks it.
#[tokio::test]
async fn a_named_refusal_ends_the_retry_and_is_delivered_until_acked() {
    let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let s = served_with(|b| b.with_ready_gate(gate.clone())).await;
    let (client, _) = client_of(&[&s.node]).await;
    let cd = client.delivery().clone();
    cd.seams().drop_next_response_ack();
    let (out, _) = client.call::<Command>(&s.target(), &run(5, Mode::Apply), &budget(Duration::from_secs(5))).await;
    assert_eq!(out.reply().map(|r| r.class()), Some(ReplyKind::NotReady), "{out:?}");
    assert!(matches!(out.reply().unwrap().detail(), CommandReply::NotReady { .. }), "{out:?}");
    eventually("the refusal's unacked frame is resent and acked", Duration::from_secs(3), || s.delivery.owed_frames() == 0 && get(&s.delivery.stats().responses_resent) >= 1).await;
    assert_eq!(get(&cd.stats().resends), 0, "the refusal ended the request's retry");
    assert_eq!(get(&s.stats.dispatched), 0);
}

/// Cell 6. CONTRACT: an owed request to a silent receiver (it takes the request and never acks) keeps
/// being resent; the receiver's exited verdict (the view's removal) ends it, by name.
#[tokio::test]
async fn the_receivers_removal_from_the_view_ends_the_owed_request_and_silence_does_not() {
    let r = raw().await;
    let (client, resolver) = client_of(&[&r.node]).await;
    let cd = client.delivery().clone();
    let target = NodeTarget::ExactNode(r.node.node_id.clone());
    let started = Instant::now();
    let call = tokio::spawn(async move { client.call::<Command>(&target, &run(6, Mode::Apply), &budget(Duration::from_secs(20))).await.0 });
    eventually("silence keeps the request owed", Duration::from_secs(3), || get(&cd.stats().resends) >= 3).await;
    assert!(!call.is_finished(), "silence ends nothing");
    resolver.remove_gone(&r.node.node_id);
    let out = call.await.unwrap();
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::ReceiverGone(_))), "{out:?}");
    assert!(started.elapsed() < Duration::from_secs(10), "the proof ended it, not the budget");
}

/// Cell 7. CONTRACT: the owner's cancellation ends the owed request: once the call is dropped, no
/// further resend reaches the receiver.
#[tokio::test]
async fn the_owners_cancellation_ends_the_owed_request() {
    let s = served().await;
    let (client, _) = client_of(&[&s.node]).await;
    let target = s.target();
    let call = tokio::spawn(async move { client.call::<Command>(&target, &run(7, Mode::Hold), &budget(Duration::from_secs(30))).await });
    eventually("the unacked request is being resent", Duration::from_secs(3), || get(&s.delivery.stats().duplicates_pending) >= 2).await;
    call.abort();
    let _ = call.await;
    tokio::time::sleep(CADENCE).await;
    let after = get(&s.delivery.stats().duplicates_pending);
    tokio::time::sleep(FLOOR * 2).await;
    assert_eq!(get(&s.delivery.stats().duplicates_pending), after, "nothing is resent after the owner ended it");
    s.release();
}

/// Cell 8 (over the wire; the schedule's arithmetic is `rdm_core::delivery`'s cell). CONTRACT: the
/// resends of one request arrive at the publish cadence, then doubling, capped at the floor.
#[tokio::test]
async fn resends_arrive_at_the_cadence_then_doubling_capped_at_the_floor() {
    let mut r = raw().await;
    let (client, _) = client_of(&[&r.node]).await;
    let target = NodeTarget::ExactNode(r.node.node_id.clone());
    let called_at = Instant::now();
    let call = tokio::spawn(async move { client.call::<Command>(&target, &run(8, Mode::Apply), &budget(Duration::from_secs(4))).await });
    let conn = r.conns.recv().await.unwrap();
    let mut arrivals = Vec::new();
    let mut ids = Vec::new();
    // The schedule puts six attempts inside 2.3 s: bounded, so a request that is never resent fails here.
    tokio::time::timeout(Duration::from_secs(4), async {
        while arrivals.len() < 6 {
            let (_send, mut recv) = conn.accept_bi().await.unwrap();
            let (h, _) = read_request(&mut recv).await;
            arrivals.push(Instant::now());
            ids.push(h.delivery_id());
        }
    })
    .await
    .unwrap_or_else(|_| panic!("six attempts of the one request within the schedule; {} arrived", arrivals.len()));
    call.abort();
    assert!(ids.windows(2).all(|w| w[0] == w[1]), "every attempt carries the one identity: {ids:?}");
    // The first resend follows the first send by the cadence: measured from the call, since the first
    // arrival also carries this cell's own accept of a new connection. Every later attempt rides the
    // warm connection, so arrival to arrival is the schedule.
    let first_resend = (arrivals[1] - called_at).as_millis();
    assert!((100..=100 + 150).contains(&first_resend), "the first resend {first_resend} ms after the call, the cadence 100 ms after the first send");
    let gaps: Vec<u128> = arrivals[1..].windows(2).map(|w| (w[1] - w[0]).as_millis()).collect();
    let expected = [200u128, 400, 800, 800];
    for (gap, want) in gaps.iter().zip(expected) {
        assert!(*gap + 30 >= want && *gap <= want + 150, "gaps {gaps:?}, expected about {expected:?}");
    }
}

/// Cell 9. CONTRACT: a resend takes the ordinary dial (a client with no connections log at all
/// delivers it), never a read of the application connections log.
#[tokio::test]
async fn a_resend_takes_the_ordinary_dial_and_reads_no_connections_log() {
    let s = served().await;
    let (client, _) = client_of(&[&s.node]).await;
    // A client with no connection observer: nothing here can read or write a connections log.
    let (warm, _) = client.call::<Command>(&s.target(), &CommandRequest::Read { tag: 0, hold: false }, &CallOptions::default()).await;
    assert!(warm.reply().is_some(), "{warm:?}");
    s.delivery.seams().cut_connection_after_next_request();
    let (out, evidence) = client.call::<Command>(&s.target(), &run(9, Mode::Apply), &budget(Duration::from_secs(5))).await;
    assert_eq!(ran(&out), Some((9, 1)), "{out:?}");
    assert!(!evidence.unwrap().reused, "the resend dialled afresh: the cut connection left the pool");
}

/// Cell 10. CONTRACT: a lost ack of a response frame is repaired by the frame's resend; the caller
/// acks the repeat (its invocation complete) and the responder owes nothing.
#[tokio::test]
async fn a_lost_ack_of_a_response_frame_is_repaired_by_its_resend() {
    let s = served().await;
    let (client, _) = client_of(&[&s.node]).await;
    let cd = client.delivery().clone();
    cd.seams().drop_next_response_ack();
    let (out, _) = client.call::<Command>(&s.target(), &run(10, Mode::Apply), &budget(Duration::from_secs(5))).await;
    assert_eq!(ran(&out), Some((10, 1)), "{out:?}");
    eventually("the response frame is resent and acked", Duration::from_secs(3), || s.delivery.owed_frames() == 0).await;
    assert!(get(&s.delivery.stats().responses_resent) >= 1);
    assert!(get(&cd.stats().responses_of_completed) >= 1, "the repeat of a completed invocation's frame is acked and dropped");
}

/// Cell 11. CONTRACT: a response written after the request's connection is gone is delivered on a
/// new stream once the caller reconnects; the handler ran once. Its trace: the responder's
/// `via-response-resend`, its cause named, ending `acked`, in the caller's trace. The cut races the
/// reply's write, so the cause is either of the server's two names for an unacked response: the ack
/// reader saw the request direction end first (server.rs, `read_acks`), or the reply's write did.
#[test]
fn a_response_is_delivered_on_a_new_stream_after_a_reconnect() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-delivery-response-resend").expect("the cell's evidence file can be created");
    let trace = capture.run(4, async {
        let root = tracing::info_span!("rdm.test.delivery.serve.via-response-after-reconnect");
        let trace = trace_of(&root);
        tracing::Instrument::instrument(
            async {
                let s = served().await;
                let (client, _) = client_of(&[&s.node]).await;
                let call = tokio::spawn(tracing::Instrument::instrument(
                    {
                        let target = s.target();
                        async move { client.call::<Command>(&target, &run(11, Mode::AcceptThenHold), &budget(Duration::from_secs(8))).await.0 }
                    },
                    tracing::Span::current(),
                ));
                eventually("the request is accepted", Duration::from_secs(2), || get(&s.stats.dispatched) == 1).await;
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert!(s.server.close_connections_except(&[], "the cell cuts the connection") >= 1);
                s.release();
                let out = call.await.unwrap();
                assert_eq!(ran(&out), Some((11, 1)), "{out:?}");
                assert!(get(&s.delivery.stats().responses_resent) >= 1, "the response went out on a new stream");
                assert_eq!(s.runs.load(Ordering::SeqCst), 1);
                eventually("the resent response is acked", Duration::from_secs(2), || s.delivery.owed_frames() == 0).await;
            },
            root,
        )
        .await;
        trace
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let resends: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.delivery.update.via-response-resend" && s["trace_id"] == trace.as_str()).collect();
    assert!(
        resends.iter().any(|s| s["attributes"]["outcome"] == "acked"
            && s["attributes"]["reason"].as_str().is_some_and(|r| {
                r.starts_with("the reply's write")
                    || (r.starts_with("the request direction ended") && r.ends_with("with a response frame unacked"))
            })),
        "the response resend, its cause named and acked, in the caller's trace: {resends:#?}"
    );
    println!("delivery flame (a response resent on a new stream after a reconnect): {}", flame(&trace));
}

/// Cell 11b (rdm#180). CONTRACT: a read-only request is accepted at its dispatch and its `Ack` is
/// written before its handler runs (rdm#216); when that write loses to a cut of the request's
/// connection, the reply direction is gone before a withheld reply is released. The release is recorded as a failed
/// late reply naming the failed write, and spanned (`rdm.node_rpc.delivery.update.via-withheld-reply-released`,
/// in the caller's trace); the reply stays owed and reaches the caller on a new stream, the handler
/// dispatched once. Must NOT happen: a release that records nothing (the late reply never resolves),
/// or a late reply written whole on the cut stream.
#[test]
fn a_withheld_reply_whose_direction_a_lost_ack_write_ended_is_a_failed_late_reply() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-delivery-withheld-reply-direction-gone").expect("the cell's evidence file can be created");
    let trace = capture.run(4, async {
        let root = tracing::info_span!("rdm.test.delivery.serve.via-withheld-reply-direction-gone");
        let trace = trace_of(&root);
        tracing::Instrument::instrument(
            async {
                let withhold = Arc::new(ReplyWithhold::default());
                let s = served_with({
                    let w = withhold.clone();
                    move |b| b.withhold_replies(w)
                })
                .await;
                let (client, _) = client_of(&[&s.node]).await;
                withhold.arm();
                s.delivery.seams().hold_next_request_ack();
                let call = tokio::spawn(tracing::Instrument::instrument(
                    {
                        let target = s.target();
                        async move { client.call::<Command>(&target, &CommandRequest::Read { tag: 180, hold: false }, &budget(Duration::from_secs(8))).await.0 }
                    },
                    tracing::Span::current(),
                ));
                // The ack is written before the handler runs: held, the connection is cut under it, and
                // released its write fails on the cut; then the handler reaches its withheld reply.
                tokio::time::timeout(Duration::from_secs(2), s.delivery.seams().request_ack_held()).await.expect("the read-only request's ack is held within 2 s");
                assert!(s.server.close_connections_except(&[], "the cell cuts the connection") >= 1);
                tokio::time::timeout(Duration::from_secs(2), s.delivery.seams().release_request_ack()).await.expect("the held ack's write ends within 2 s");
                tokio::time::timeout(Duration::from_secs(2), withhold.reached()).await.expect("the handler reaches its reply within 2 s");
                withhold.release();
                let late = tokio::time::timeout(Duration::from_secs(2), withhold.late_reply()).await.expect("the released reply is recorded within 2 s, never skipped");
                let written = late.written.clone().expect_err("the late reply is not written whole on the cut stream");
                assert!(written.contains("a write on it failed"), "the late reply names the write that ended its direction: {late:?}");
                let out = call.await.unwrap();
                assert_eq!(ran(&out), Some((180, 0)), "{out:?}");
                assert!(get(&s.delivery.stats().responses_resent) >= 1, "the reply went out on a new stream");
                assert_eq!(get(&s.stats.dispatched), 1, "the handler was dispatched once");
                eventually("the resent reply is acked", Duration::from_secs(2), || s.delivery.owed_frames() == 0).await;
            },
            root,
        )
        .await;
        trace
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    assert!(
        spans.iter().any(|s| s["name"] == "rdm.node_rpc.delivery.update.via-withheld-reply-released"
            && s["trace_id"] == trace.as_str()
            && s["attributes"]["written"].as_str().is_some_and(|w| w.contains("a write on it failed"))),
        "the release, its failed write named, in the caller's trace"
    );
    println!("delivery flame (a withheld reply whose direction a lost ack write ended): {}", flame(&trace));
}

/// Cell 11c (rdm#180). CONTRACT: a read-only request (`Read`: accepted at its dispatch, its `Ack`
/// written by a task of its own, racing the check's cut) passes the conformance harness's
/// reply-lost-after-acceptance check: the released reply is recorded, written into the cut stream or
/// named unwritten, and reaches the caller on a new stream, the handler dispatched once. Its
/// evidence: the check's trace under `rdm.test.conformance.serve.via-check`. Must NOT happen: the
/// check timing out on a release that recorded nothing.
#[test]
fn a_read_only_request_passes_the_reply_lost_after_acceptance_check() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-conformance-read-only-reply-lost").expect("the cell's evidence file can be created");
    let fixture = rdm_test::conformance::Unary::<Command> {
        serve: Arc::new(|b: ServerBuilder| {
            b.ledger(ledger()).serve::<Command, _, _>(OpOwner::Product("test".into()), |_peer: PeerContext, req| async move {
                Ok::<_, HandlerFault>(match req {
                    CommandRequest::Read { tag, .. } => CommandReply::Ran { tag, runs: 0 },
                    CommandRequest::Run { tag, .. } => CommandReply::Ran { tag, runs: 1 },
                })
            })
        }),
        answered: CommandRequest::Read { tag: 180, hold: false },
        refusals: Vec::new(),
    };
    let report = capture.run(4, async { rdm_test::conformance::unary_checks(&fixture, &[rdm_test::conformance::Check::ReplyLostAfterAcceptance]).await });
    println!("{}", report.summary());
    assert!(report.failed().is_empty(), "{}", report.summary());
}

/// Cell 12. CONTRACT: a repeat after acceptance is answered `Ack` then EOF: delivery only. The
/// caller's invocation stays open and the original's result arrives.
#[tokio::test]
async fn a_repeats_ack_then_eof_leaves_the_invocation_open_and_the_result_arrives() {
    let s = served().await;
    let (client, _) = client_of(&[&s.node]).await;
    s.delivery.seams().drop_next_request_ack();
    let call = tokio::spawn({
        let target = s.target();
        async move { client.call::<Command>(&target, &run(12, Mode::AcceptThenHold), &budget(Duration::from_secs(5))).await.0 }
    });
    eventually("the repeat is acked without dispatch", Duration::from_secs(3), || get(&s.delivery.stats().duplicates_acked) >= 1).await;
    tokio::time::sleep(CADENCE * 3).await;
    assert!(!call.is_finished(), "the repeat's EOF is no result");
    s.release();
    let out = call.await.unwrap();
    assert_eq!(ran(&out), Some((12, 1)), "the original's result: {out:?}");
}

// ---- 13-21: order, identity, windows ----

/// Open a Tick stream from `client` to the raw responder; hand back the request's identity and the
/// responder's side of the stream.
async fn tick_stream(r: &mut Raw, client: &NodeRpcClient) -> (rdm_transport::stream::ReplyStream<Tick>, DeliveryId, SendStream, RecvStream, Connection) {
    let target = NodeTarget::ExactNode(r.node.node_id.clone());
    let opening = {
        let client = client.clone();
        tokio::spawn(async move { client.call_stream::<Tick>(&target, &TickRequest::Tick, &CallOptions { budget: Budget::Stream { send: Duration::from_secs(5) }, ..CallOptions::default() }).await })
    };
    let conn = r.conns.recv().await.unwrap();
    let (mut send, mut recv) = conn.accept_bi().await.unwrap();
    let (h, _) = read_request(&mut recv).await;
    let id = h.delivery_id();
    // Acknowledge the request, so it is not resent while the cell writes frames.
    write(&mut send, &encode_ack_frame(&h.ack())).await;
    let (stream, _) = opening.await.unwrap().map_err(|(o, _)| format!("{o:?}")).unwrap();
    (stream, id, send, recv, conn)
}

/// Cell 13. CONTRACT: two different frames at one `(invocation_id, seq)` are a `424`, whatever
/// their delivery identities.
#[tokio::test]
async fn two_different_frames_at_one_position_are_refused() {
    let mut r = raw().await;
    let (client, _) = client_of(&[&r.node]).await;
    let (mut stream, id, mut send, mut recv, _conn) = tick_stream(&mut r, &client).await;
    write(&mut send, &response(&r.me, 1, &id, 0, &TickFrame::Started)).await;
    assert_eq!(stream.next().await, Some(StreamItem::Frame(FrameKind::Started, TickFrame::Started)));
    write(&mut send, &response(&r.me, 2, &id, 0, &TickFrame::Item { i: 0 })).await;
    assert!(matches!(stream.next().await, Some(StreamItem::Failed(StreamFailure::Violation(_)))), "another frame at a delivered position");
    let mut pending = Vec::new();
    while read_header(&mut recv, &mut pending).await.is_some() {}
    assert!(get(&client.delivery().stats().violations) >= 1);
}

/// Cell 14. CONTRACT: a terminal past a missing position is held, never delivered, until the
/// positions before it arrive; then the stream yields them in order.
#[tokio::test]
async fn a_terminal_is_held_while_an_earlier_position_is_missing() {
    let mut r = raw().await;
    let (client, _) = client_of(&[&r.node]).await;
    let (mut stream, id, mut send, _recv, _conn) = tick_stream(&mut r, &client).await;
    write(&mut send, &response(&r.me, 3, &id, 2, &TickFrame::Done)).await;
    write(&mut send, &response(&r.me, 1, &id, 0, &TickFrame::Started)).await;
    assert_eq!(stream.next().await, Some(StreamItem::Frame(FrameKind::Started, TickFrame::Started)));
    let held = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
    assert!(held.is_err(), "the terminal completes nothing while position 1 is missing: {held:?}");
    write(&mut send, &response(&r.me, 2, &id, 1, &TickFrame::Item { i: 0 })).await;
    assert_eq!(stream.next().await, Some(StreamItem::Frame(FrameKind::Data, TickFrame::Item { i: 0 })));
    assert_eq!(stream.next().await, Some(StreamItem::Frame(FrameKind::Terminal, TickFrame::Done)));
}

/// Cell 15. CONTRACT: a response frame for an invocation the caller completed is acked and dropped;
/// one for an invocation it never held is neither delivered nor acked.
#[tokio::test]
async fn a_response_of_a_completed_invocation_is_acked_and_one_never_held_is_not() {
    let mut r = raw().await;
    let (client, _) = client_of(&[&r.node]).await;
    let target = NodeTarget::ExactNode(r.node.node_id.clone());
    let call = {
        let client = client.clone();
        tokio::spawn(async move { client.call::<Command>(&target, &run(15, Mode::Apply), &budget(Duration::from_secs(5))).await.0 })
    };
    let conn = r.conns.recv().await.unwrap();
    let (mut send, mut recv) = conn.accept_bi().await.unwrap();
    let (h, _) = read_request(&mut recv).await;
    let id = h.delivery_id();
    write(&mut send, &command_response(&r.me, 1, &id, &CommandReply::Ran { tag: 15, runs: 1 })).await;
    let out = call.await.unwrap();
    assert_eq!(ran(&out), Some((15, 1)), "{out:?}");
    // The completed invocation's frame again, on a new stream of the connection.
    let (mut send2, mut recv2) = conn.open_bi().await.unwrap();
    write(&mut send2, &command_response(&r.me, 2, &id, &CommandReply::Ran { tag: 15, runs: 1 })).await;
    let mut pending = Vec::new();
    let acked = tokio::time::timeout(Duration::from_secs(2), read_header(&mut recv2, &mut pending)).await.unwrap();
    assert_eq!(acked, Some(TransportHeader::Ack { sender: r.me.clone(), delivery_seq: 2 }), "acked and dropped");
    // An invocation this caller never held.
    let never = DeliveryId { sender: client.delivery().me().clone(), delivery_seq: 999 };
    let (mut send3, mut recv3) = conn.open_bi().await.unwrap();
    write(&mut send3, &command_response(&r.me, 3, &never, &CommandReply::Ran { tag: 0, runs: 0 })).await;
    send3.finish().unwrap();
    let mut pending = Vec::new();
    let none = tokio::time::timeout(Duration::from_millis(500), read_header(&mut recv3, &mut pending)).await;
    assert!(matches!(none, Ok(None) | Err(_)), "never acked: {none:?}");
    assert_eq!(get(&client.delivery().stats().responses_unheld), 1);
}

/// Cell 16. CONTRACT: a request's duplicate state outlives the staleness floor, forgotten only when
/// the sender's `forget_below` passes it.
#[tokio::test]
async fn duplicate_state_outlives_the_floor_until_the_senders_watermark_passes_it() {
    let s = served().await;
    let me = birth();
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let conn = ep.connect(iroh::EndpointAddr::new(s.node.endpoint_id).with_ip_addr(s.node.transport_addr), rdm_transport::ALPN).await.unwrap();
    let request = |seq: u64, forget_below: u64| {
        let h = TransportHeader::Request { sender: me.clone(), delivery_seq: seq, forget_below };
        let target = RequestHeader::target(Target { node_id: s.node.node_id.to_string(), op: Command::OP });
        encode_request_frame(&h, &target, &Command::encode_request(&run(16, Mode::Apply)).unwrap())
    };
    let send_once = |bytes: Vec<u8>| {
        let conn = conn.clone();
        async move {
            let (mut send, mut recv) = conn.open_bi().await.unwrap();
            send.write_all(&bytes).await.unwrap();
            let mut pending = Vec::new();
            let mut kinds = Vec::new();
            while let Some(h) = read_header(&mut recv, &mut pending).await {
                kinds.push(h.kind());
                if let TransportHeader::Response { .. } = h {
                    // The reply body: ack it, so the response is settled.
                    let (len, at) = match rdm_core::framing::decode_varint(&pending) {
                        rdm_core::framing::Varint::Complete { value, len } => (value as usize, len),
                        _ => panic!("a reply length"),
                    };
                    while pending.len() < at + len {
                        let mut chunk = [0u8; 2048];
                        let n = recv.read(&mut chunk).await.unwrap().unwrap();
                        pending.extend_from_slice(&chunk[..n]);
                    }
                    pending.drain(..at + len);
                    send.write_all(&encode_ack_frame(&h.ack())).await.unwrap();
                    let _ = send.finish();
                }
            }
            kinds
        }
    };
    assert_eq!(send_once(request(1, 1)).await, vec!["response"], "dispatched and answered");
    tokio::time::sleep(FLOOR * 2).await;
    assert_eq!(send_once(request(1, 1)).await, vec!["ack"], "past the floor, the repeat is still acked without dispatch");
    assert_eq!(s.runs.load(Ordering::SeqCst), 1);
    assert_eq!(s.delivery.window_of(&me), (1, 1));
    assert_eq!(send_once(request(2, 2)).await, vec!["response"], "a new request, whose watermark passes 1");
    assert_eq!(s.delivery.window_of(&me), (1, 2), "1 forgotten, 2 held");
    assert_eq!(send_once(request(1, 2)).await, vec!["ack"], "below the watermark: long acknowledged");
    assert_eq!(s.runs.load(Ordering::SeqCst), 2);
}

/// Cell 17. CONTRACT: a carried call keeps its origin's identity: sent again through another
/// carrier, it is suppressed at the final target, which ran the handler once.
#[tokio::test]
async fn a_carried_calls_resend_through_another_carrier_is_suppressed_by_its_origin_identity() {
    let target = served().await;
    let mut carriers = Vec::new();
    for _ in 0..2 {
        let b = birth();
        let key = SecretKey::generate();
        let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(target.node.clone());
        let delivery = Delivery::with_pacing(b.clone(), pacing());
        let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver).with_delivery(delivery.clone()));
        let server = ServerBuilder::new().ledger(ledger()).carry::<Command>().serve_forward(client).with_delivery(delivery).build(ServedBirth { node_id: b.node_id.to_string(), incarnation: b.incarnation.to_string() }).unwrap();
        let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
        carriers.push((router, ResolvedNode { node_id: b.node_id.clone(), name: "mesh1.rpc.2".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation: b.incarnation }));
    }
    let (origin, _) = client_of(&[&target.node, &carriers[0].1, &carriers[1].1]).await;
    let identity = CarriedIdentity { id: DeliveryId { sender: origin.delivery().me().clone(), delivery_seq: 7 }, forget_below: 7 };
    let opts = |d: Duration| CallOptions { budget: Budget::Overall(d), carried: Some(identity.clone()), ..CallOptions::default() };
    let via = |i: usize| NodeTarget::ExactNode(carriers[i].1.node_id.clone());
    let (first, _) = origin.call_via::<Command>(&via(0), &target.node.node_id, &run(17, Mode::Apply), &opts(Duration::from_secs(2))).await;
    assert_eq!(ran(&first), Some((17, 1)), "{first:?}");
    // The same request again, through the other carrier: its result was delivered to the first
    // carrier, so this delivery has no result to end on; what matters is that it reached the target
    // as the origin's repeat and the target ran nothing.
    let second = {
        let (origin, via1, node) = (origin.clone(), via(1), target.node.node_id.clone());
        let opts = opts(Duration::from_secs(5));
        tokio::spawn(async move { origin.call_via::<Command>(&via1, &node, &run(17, Mode::Apply), &opts).await })
    };
    eventually("the second carrier's inner call reaches the target", Duration::from_secs(5), || get(&target.delivery.stats().duplicates_acked) >= 1).await;
    second.abort();
    assert_eq!(target.runs.load(Ordering::SeqCst), 1, "the final target ran it once");
    assert_eq!(get(&target.delivery.stats().duplicates_acked), 1, "the second carrier's inner call was the origin's repeat");
    let seen = target.seen.lock().unwrap().clone();
    assert_eq!(seen[0].as_ref().map(|c| c.id.sender.clone()), Some(origin.delivery().me().clone()), "the window is the origin's, never the carrier's");
}

/// Cell 18. CONTRACT: an `Ack` on an Ack stream of the current connection is consumed and settles
/// the request it names: the request is not resent.
#[tokio::test]
async fn an_ack_on_an_ack_stream_settles_the_request_it_names() {
    let mut r = raw().await;
    let (client, _) = client_of(&[&r.node]).await;
    let cd = client.delivery().clone();
    let target = NodeTarget::ExactNode(r.node.node_id.clone());
    let call = {
        let client = client.clone();
        tokio::spawn(async move { client.call::<Command>(&target, &run(18, Mode::Apply), &budget(Duration::from_secs(5))).await.0 })
    };
    let conn = r.conns.recv().await.unwrap();
    let (mut send, mut recv) = conn.accept_bi().await.unwrap();
    let (h, _) = read_request(&mut recv).await;
    // The ack goes on a stream of its own, first frame an Ack.
    let (mut acks, _) = conn.open_bi().await.unwrap();
    write(&mut acks, &encode_ack_frame(&h.ack())).await;
    acks.finish().unwrap();
    eventually("the ack stream's ack is consumed", Duration::from_secs(1), || get(&cd.stats().acks_received) == 1).await;
    tokio::time::sleep(CADENCE * 4).await;
    assert_eq!(get(&cd.stats().resends), 0, "an acked request is never resent");
    write(&mut send, &command_response(&r.me, 1, &h.delivery_id(), &CommandReply::Ran { tag: 18, runs: 1 })).await;
    assert_eq!(ran(&call.await.unwrap()), Some((18, 1)));
}

/// Cell 19. CONTRACT: an endpoint that admits no new command (connections-deleting) still
/// receives an owed response: response frames reach the demultiplexer before admission.
#[tokio::test]
async fn a_connections_deleting_endpoint_still_receives_an_owed_response() {
    // The caller is a node: its client and its server share one delivery state.
    let me = birth();
    let delivery = Delivery::with_pacing(me.clone(), pacing());
    let r = raw().await;
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(r.node.clone());
    let key = SecretKey::generate();
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let client = NodeRpcClient::new(ep.clone(), resolver).with_delivery(delivery.clone());
    let server = ServerBuilder::new().ledger(ledger()).with_delivery(delivery.clone()).build(ServedBirth { node_id: me.node_id.to_string(), incarnation: me.incarnation.to_string() }).unwrap();
    let _router = Router::builder(ep).accept(rdm_transport::ALPN, server.clone()).spawn();
    server.begin_connections_delete();
    let mut r = r;
    let target = NodeTarget::ExactNode(r.node.node_id.clone());
    let call = tokio::spawn(async move { client.call::<Command>(&target, &run(19, Mode::Apply), &budget(Duration::from_secs(5))).await.0 });
    let conn = r.conns.recv().await.unwrap();
    let (mut send, mut recv) = conn.accept_bi().await.unwrap();
    let (h, _) = read_request(&mut recv).await;
    write(&mut send, &encode_ack_frame(&h.ack())).await;
    // The response comes to the caller's own server, on a connection the responder dials.
    let rep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let back = rep.connect(iroh::EndpointAddr::new(key.public()).with_ip_addr(addr), rdm_transport::ALPN).await.unwrap();
    let (mut rsend, mut rrecv) = back.open_bi().await.unwrap();
    write(&mut rsend, &command_response(&r.me, 1, &h.delivery_id(), &CommandReply::Ran { tag: 19, runs: 1 })).await;
    let mut pending = Vec::new();
    let acked = tokio::time::timeout(Duration::from_secs(2), read_header(&mut rrecv, &mut pending)).await.unwrap();
    assert_eq!(acked, Some(TransportHeader::Ack { sender: r.me.clone(), delivery_seq: 1 }));
    assert_eq!(ran(&call.await.unwrap()), Some((19, 1)), "delivered while the endpoint is connections-deleting");
    assert_eq!(get(&server.stats().connections_deleting), 0, "a response frame never meets admission");
}

/// Cell 20. CONTRACT: reordering is bounded: a frame more than 64 positions past the next expected
/// one, or a held frame over the op's reply ceiling (the byte bound, per frame), is a `424`.
#[tokio::test]
async fn reordering_past_its_positions_or_its_bytes_is_refused() {
    let mut r = raw().await;
    let (client, _) = client_of(&[&r.node]).await;
    let (mut stream, id, mut send, _recv, _conn) = tick_stream(&mut r, &client).await;
    write(&mut send, &response(&r.me, 1, &id, 65, &TickFrame::Item { i: 65 })).await;
    let refused = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(matches!(refused, Ok(Some(StreamItem::Failed(StreamFailure::Violation(_))))), "65 past the next expected position: {refused:?}");
    let mut r = raw().await;
    let (client, _) = client_of(&[&r.node]).await;
    let (mut stream, id, mut send, _recv, _conn) = tick_stream(&mut r, &client).await;
    write(&mut send, &response(&r.me, 1, &id, 64, &TickFrame::Item { i: 64 })).await;
    // Tick's reply ceiling is 16 bytes: a held frame of 17 is refused before it is buffered.
    write(&mut send, &response(&r.me, 2, &id, 1, &TickFrame::Unauthorized { reason: "x".repeat(15) })).await;
    let refused = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(matches!(refused, Ok(Some(StreamItem::Failed(_)))), "a frame over the reply ceiling: {refused:?}");
    assert!(get(&client.delivery().stats().violations) >= 1);
}

/// Cell 21. CONTRACT: a new birth of the sender starts its `delivery_seq` at 1 and is dispatched;
/// the receiver keeps the old birth's window until the view proves that birth ended.
#[tokio::test]
async fn a_new_sender_birth_starts_at_one_and_the_old_window_stays_until_its_end_is_proven() {
    let s = served().await;
    // The receiver's own view: it learns of the sender's births through it.
    let receiver_view = Arc::new(StaticResolver::new());
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _receiver_client = NodeRpcClient::new(ep, receiver_view.clone()).with_delivery(s.delivery.clone());
    let node_id = NodeId::mint();
    let old = Birth { node_id: node_id.clone(), incarnation: IncarnationId::mint() };
    let new = Birth { node_id: node_id.clone(), incarnation: IncarnationId::mint() };
    let as_birth = |b: &Birth| ResolvedNode { node_id: b.node_id.clone(), name: "mesh1.rpc.5".parse().unwrap(), endpoint_id: SecretKey::generate().public(), transport_addr: "127.0.0.1:9".parse().unwrap(), incarnation: b.incarnation.clone() };
    receiver_view.insert(as_birth(&old));
    let sender = |b: &Birth| {
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(s.node.clone());
        let b = b.clone();
        async move {
            let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
            NodeRpcClient::new(ep, resolver).with_delivery(Delivery::with_pacing(b, pacing()))
        }
    };
    let first = sender(&old).await;
    let (out, _) = first.call::<Command>(&s.target(), &run(21, Mode::Apply), &CallOptions::default()).await;
    assert_eq!(ran(&out), Some((21, 1)), "{out:?}");
    let second = sender(&new).await;
    let (out, _) = second.call::<Command>(&s.target(), &run(21, Mode::Apply), &CallOptions::default()).await;
    assert_eq!(ran(&out), Some((21, 2)), "the new birth's first identity is 1, and it is its own: dispatched");
    assert_eq!(s.seen.lock().unwrap()[1].as_ref().map(|c| c.id.delivery_seq), Some(1));
    assert_eq!(s.delivery.window_of(&old).0, 1, "the old birth's window is kept");
    // The view now names the new birth: the old one's end is proven.
    receiver_view.insert(as_birth(&new));
    eventually("the old birth's window is forgotten", Duration::from_secs(2), || s.delivery.window_of(&old).0 == 0).await;
    assert_eq!(s.delivery.window_of(&new).0, 1, "the live birth's window stays");
}

/// Cell 22. CONTRACT (node-rpc.md §31, §43): a response frame its consumer has read but not yet
/// recovered is not acked: the responder still owes it, and after a reconnect it is resent. The
/// consumer, which holds it, is not handed it again; its acceptance acks it on the stream it now
/// arrives on, and the responder settles.
#[tokio::test]
async fn a_frame_its_consumer_has_not_recovered_is_not_acked_and_is_resent_after_a_reconnect() {
    let hold = Arc::new(Semaphore::new(0));
    let h = hold.clone();
    let s = served_with(move |b| {
        b.serve_stream::<Tick, _, _>(OpOwner::Product("test".into()), move |_peer, _req: TickRequest, sink| {
            let h = h.clone();
            async move {
                let Ok(mut sink) = sink.started(TickFrame::Started).await else { return Ok(TickFrame::Done) };
                if sink.data(TickFrame::Item { i: 1 }).await.is_err() {
                    return Ok(TickFrame::Done);
                }
                h.acquire().await.unwrap().forget();
                Ok::<_, HandlerFault>(TickFrame::Done)
            }
        })
    })
    .await;
    let (client, _) = client_of(&[&s.node]).await;
    let (mut stream, _) = client.call_stream::<Tick>(&s.target(), &TickRequest::Tick, &budget(Duration::from_secs(8))).await.map_err(|(o, _)| o).expect("the stream opens");
    assert!(matches!(stream.next().await, Some(StreamItem::Frame(FrameKind::Started, TickFrame::Started))));
    assert!(matches!(stream.next().await, Some(StreamItem::Frame(FrameKind::Data, TickFrame::Item { i: 1 }))));
    tokio::time::sleep(CADENCE * 3).await;
    assert_eq!(s.delivery.owed_frames(), 2, "read, not yet recovered: neither frame is acked");
    assert!(s.server.close_connections_except(&[], "the cell cuts the connection") >= 1);
    eventually("the frames still owed are resent after the reconnect", Duration::from_secs(3), || get(&s.delivery.stats().responses_resent) >= 1).await;
    tokio::time::sleep(CADENCE * 3).await;
    assert_eq!(s.delivery.owed_frames(), 2, "resent and still owed: the consumer has not recovered them");
    stream.accepted().await;
    eventually("the consumer's recovery acks both, on the stream the copies came on", Duration::from_secs(3), || s.delivery.owed_frames() == 0).await;
    hold.add_permits(1);
    // The consumer holds both frames: the resent copies are never handed to it again.
    let next = stream.next().await;
    assert!(matches!(next, Some(StreamItem::Frame(FrameKind::Terminal, TickFrame::Done))), "the next frame is the terminal: {next:?}");
    stream.accepted().await;
    assert!(stream.next().await.is_none());
    eventually("the terminal is recovered and acked", Duration::from_secs(3), || s.delivery.owed_frames() == 0).await;
}

/// Cell 23. CONTRACT (i146 Q38): every return of a request's attempt task names its path, spanned
/// under its call in the caller's trace (`rdm.node_rpc.delivery.update.via-attempt-ended`, its
/// `path` and `delivery_id`): a served call's attempt ends `stream-complete`, one whose reply
/// direction the receiver resets with `INTERNAL_RPC_FAILURE` ends `reset`, one whose receiver's exit
/// is proven ends `receiver-gone`. Must NOT happen: a call whose attempt task ended without the span,
/// or a span naming a path other than the one its call ended on.
#[test]
fn every_return_of_a_requests_attempt_task_is_spanned_with_its_path() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-delivery-attempt-ended").expect("the cell's evidence file can be created");
    let (trace, calls) = capture.run(4, async {
        let root = tracing::info_span!("rdm.test.delivery.update.via-attempt-ended");
        let trace = trace_of(&root);
        let calls = tracing::Instrument::instrument(
            async {
                // Served: the reply completes the invocation on the attempt's stream.
                let s = served().await;
                let (client, _) = client_of(&[&s.node]).await;
                let served_id = DeliveryId { sender: client.delivery().me().clone(), delivery_seq: 1 };
                let (out, _) = client.call::<Command>(&s.target(), &run(23, Mode::Apply), &budget(Duration::from_secs(5))).await;
                assert_eq!(ran(&out), Some((23, 1)), "{out:?}");

                // Reset: the receiver reads the whole request, then resets its reply direction.
                let mut r = raw().await;
                let (client, _) = client_of(&[&r.node]).await;
                let reset_id = DeliveryId { sender: client.delivery().me().clone(), delivery_seq: 1 };
                let target = NodeTarget::ExactNode(r.node.node_id.clone());
                let call = tokio::spawn(tracing::Instrument::instrument(
                    async move { client.call::<Command>(&target, &run(23, Mode::Apply), &budget(Duration::from_secs(5))).await.0 },
                    tracing::Span::current(),
                ));
                let conn = r.conns.recv().await.unwrap();
                let (mut send, mut recv) = conn.accept_bi().await.unwrap();
                let _ = read_request(&mut recv).await;
                let failure = rdm_core::ResetCode::InternalRpcFailure.code();
                send.reset(iroh::endpoint::VarInt::from_u32(failure)).unwrap();
                let out = call.await.unwrap();
                assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::Reset(u64::from(failure))), "{out:?}");

                // Receiver gone: a silent receiver, then its exited verdict.
                let gone = raw().await;
                let (client, resolver) = client_of(&[&gone.node]).await;
                let cd = client.delivery().clone();
                let gone_id = DeliveryId { sender: cd.me().clone(), delivery_seq: 1 };
                let target = NodeTarget::ExactNode(gone.node.node_id.clone());
                let call = tokio::spawn(tracing::Instrument::instrument(
                    async move { client.call::<Command>(&target, &run(23, Mode::Apply), &budget(Duration::from_secs(20))).await.0 },
                    tracing::Span::current(),
                ));
                eventually("the silent receiver's request is resent", Duration::from_secs(3), || get(&cd.stats().resends) >= 1).await;
                resolver.remove_gone(&gone.node.node_id);
                let out = call.await.unwrap();
                assert!(matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::ReceiverGone(_))), "{out:?}");
                vec![(served_id, "stream-complete"), (reset_id, "reset"), (gone_id, "receiver-gone")]
            },
            root,
        )
        .await;
        (trace, calls)
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let named = |name: &str| -> Vec<&serde_json::Value> { spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace.as_str()).collect() };
    let calls_spanned = named("rdm.node_rpc.request.update.via-call");
    let ended = named("rdm.node_rpc.delivery.update.via-attempt-ended");
    for (id, path) in &calls {
        let id = id.to_string();
        let call = calls_spanned.iter().find(|c| c["attributes"]["delivery_id"] == id.as_str()).unwrap_or_else(|| panic!("the call {id} is spanned in the caller's trace: {}", flame(&trace)));
        let of_call: Vec<&&serde_json::Value> = ended.iter().filter(|e| e["attributes"]["delivery_id"] == id.as_str()).collect();
        assert_eq!(of_call.len(), 1, "one end of the call {id}'s attempt task, spanned: {of_call:#?} ({})", flame(&trace));
        assert_eq!(of_call[0]["attributes"]["path"], *path, "the call {id} ended on `{path}`: {:#?} ({})", of_call[0]["attributes"], flame(&trace));
        assert_eq!(of_call[0]["parent_span_id"], call["span_id"], "the end is spanned under its own call: {} ({})", of_call[0], flame(&trace));
    }
    println!("delivery flame (every attempt task's end, named by its path): {}", flame(&trace));
}

fn owed() -> CallOptions {
    CallOptions { budget: Budget::Owed, ..CallOptions::default() }
}

/// A birth, its endpoint key and an address nothing listens on: the endpoint that held it is closed.
async fn unreachable() -> (Birth, SecretKey, ResolvedNode) {
    let (me, key) = (birth(), SecretKey::generate());
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    ep.close().await;
    let node = ResolvedNode { node_id: me.node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation: me.incarnation.clone() };
    (me, key, node)
}

/// Cell 24. CONTRACT (i146 Q44; node-rpc.md §34): an Owed call has no send or reply deadline. To a
/// receiver that cannot be reached it is resent past the bound at which an Overall call to the same
/// receiver ends `NotSent`/`Deadline`, and it is delivered, its handler run once, when the receiver
/// comes up. Must NOT happen: the Owed call ending while nothing ended it.
#[tokio::test]
async fn an_owed_call_outlives_every_bound_and_is_delivered_when_its_receiver_comes() {
    let (me, key) = (birth(), SecretKey::generate());
    // Keep the OS-assigned socket while the receiver is not serving Node RPC. Releasing a
    // chosen port and rebinding it later lets another concurrent rig take it in between.
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    ep.set_alpns(vec![]);
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let node = ResolvedNode { node_id: me.node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation: me.incarnation.clone() };
    let (client, _) = client_of(&[&node]).await;
    let cd = client.delivery().clone();
    let target = NodeTarget::ExactNode(node.node_id.clone());
    let (c, t) = (client.clone(), target.clone());
    let owed_call = tokio::spawn(async move { c.call::<Command>(&t, &run(24, Mode::Apply), &owed()).await.0 });
    let (bounded, _) = client.call::<Command>(&target, &run(240, Mode::Apply), &budget(FLOOR * 2)).await;
    assert!(matches!(&bounded, RpcOutcome::NotSent(n) if matches!(n.reason(), rdm_core::outcome::NotSentReason::Deadline)), "the Overall call ends at its bound: {bounded:?}");
    tokio::time::sleep(FLOOR * 2).await;
    assert!(!owed_call.is_finished(), "the Owed call outlives the bound the Overall call ended at");
    let resent = get(&cd.stats().resends);
    assert!(resent >= 3, "the Owed call is resent on the schedule: {resent}");
    let s = served_on(me, key, ep, |b| b).await;
    let out = tokio::time::timeout(FLOOR * 4, owed_call).await.expect("delivered once the receiver came").unwrap();
    assert_eq!(ran(&out), Some((24, 1)), "{out:?}");
    assert_eq!(get(&s.stats.dispatched), 1, "the handler ran once");
}

/// Cell 24b (Q10 = a). CONTRACT (node-rpc.md §43: retry stops "on the ack, on the receiver's exited verdict
/// or accepted removal, or on explicit cancellation or retargeting by the owning workflow; never on a
/// timer, never on absence from a view"): an Owed call to a receiver this caller's view does not hold yet
/// stays owed. It is opened once the receiver resolves (read again at the resend pacing and on every
/// change of the view), delivered, and its handler runs once. Controls: an Overall call to the same
/// absent receiver still ends `NotSent` at once, and an Owed call to a receiver the view holds proven
/// gone ends `NotSent`/`Resolve(Gone)` at once. Must NOT happen: the Owed call ending
/// `NotSent`/`Resolve(Unknown)` because the view did not hold its receiver yet (the re-born admin cell:
/// a member resumed after its mesh's primary was re-born declared Ready to it, the call ended
/// so, and nothing sent it again; kept/e6s7-i144-e4-re-born-admin-owed-declaration, trace
/// 704617ac4b3ac6bbd29b0c8707af019b).
#[tokio::test]
async fn an_owed_call_to_a_receiver_its_view_holds_only_later_is_delivered_once_it_resolves() {
    let s = served().await;
    let (client, resolver) = client_of(&[]).await;
    let target = s.target();
    let (bounded, _) = client.call::<Command>(&target, &run(241, Mode::Apply), &budget(FLOOR)).await;
    assert!(matches!(&bounded, RpcOutcome::NotSent(n) if matches!(n.reason(), rdm_core::outcome::NotSentReason::Resolve(rdm_core::outcome::ResolveFailure::Unknown))), "an Overall call ends at once: {bounded:?}");
    let (c, t) = (client.clone(), target.clone());
    let call = tokio::spawn(async move { c.call::<Command>(&t, &run(242, Mode::Apply), &owed()).await.0 });
    tokio::time::sleep(FLOOR * 2).await;
    assert!(!call.is_finished(), "absence from the view ends nothing: {:?}", if call.is_finished() { Some(call.await.unwrap()) } else { None });
    resolver.insert(s.node.clone());
    let out = tokio::time::timeout(FLOOR * 4, call).await.expect("delivered once the receiver resolved").unwrap();
    assert_eq!(ran(&out), Some((242, 1)), "{out:?}");
    assert_eq!(get(&s.stats.dispatched), 1, "the handler ran once");
    let gone = NodeId::mint();
    resolver.remove_gone(&gone);
    let started = Instant::now();
    let (out, _) = client.call::<Command>(&NodeTarget::ExactNode(gone), &run(243, Mode::Apply), &owed()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), rdm_core::outcome::NotSentReason::Resolve(rdm_core::outcome::ResolveFailure::Gone))), "a receiver proven gone ends it: {out:?}");
    assert!(started.elapsed() < FLOOR, "at once: {:?}", started.elapsed());
}

/// Cell 25 (control, i146 Q44). CONTRACT: an Overall call and a Split call still lapse as before: to a
/// receiver that cannot be reached, each ends `NotSent`/`Deadline` at its send bound; to a receiver
/// that holds the request unanswered, each ends `Indeterminate`/`ReplyDeadline` at its bound.
#[tokio::test]
async fn an_overall_and_a_split_call_still_lapse_at_their_bounds() {
    let split = || CallOptions { budget: Budget::Split { send: FLOOR, reply: FLOOR }, ..CallOptions::default() };
    let (_, _, node) = unreachable().await;
    let (client, _) = client_of(&[&node]).await;
    let target = NodeTarget::ExactNode(node.node_id.clone());
    for (what, opts) in [("overall", budget(FLOOR)), ("split", split())] {
        let started = Instant::now();
        let (out, _) = client.call::<Command>(&target, &run(25, Mode::Apply), &opts).await;
        assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), rdm_core::outcome::NotSentReason::Deadline)), "{what}: {out:?}");
        assert!(started.elapsed() < FLOOR * 3, "{what}: ended at its send bound, {:?}", started.elapsed());
    }
    let s = served().await;
    let (client, _) = client_of(&[&s.node]).await;
    for (what, opts) in [("overall", budget(FLOOR)), ("split", split())] {
        let (out, _) = client.call::<Command>(&s.target(), &run(26, Mode::Hold), &opts).await;
        assert!(matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::ReplyDeadline)), "{what}: {out:?}");
        s.release();
    }
}

/// Cell 26 (i146 Q44). CONTRACT: an owner dropping its Owed call ends the resend: no further resend
/// reaches the receiver once the call is dropped.
#[tokio::test]
async fn an_owner_dropping_its_owed_call_ends_the_resend() {
    let s = served().await;
    let (client, _) = client_of(&[&s.node]).await;
    let target = s.target();
    let call = tokio::spawn(async move { client.call::<Command>(&target, &run(27, Mode::Hold), &owed()).await });
    eventually("the unacked request is being resent", Duration::from_secs(3), || get(&s.delivery.stats().duplicates_pending) >= 2).await;
    call.abort();
    let _ = call.await;
    tokio::time::sleep(CADENCE).await;
    let after = get(&s.delivery.stats().duplicates_pending);
    tokio::time::sleep(FLOOR * 2).await;
    assert_eq!(get(&s.delivery.stats().duplicates_pending), after, "nothing is resent after the owner dropped it");
    s.release();
}

/// Cell 27 (i146 Q44). CONTRACT: an Owed call to a silent receiver ends on the receiver's exited verdict
/// (the view's removal), `ReceiverGone`, code `UNAVAILABLE`: the proof ends it, never silence.
#[tokio::test]
async fn an_owed_call_ends_on_its_receivers_removal_from_the_view_unavailable() {
    let r = raw().await;
    let (client, resolver) = client_of(&[&r.node]).await;
    let cd = client.delivery().clone();
    let target = NodeTarget::ExactNode(r.node.node_id.clone());
    let call = tokio::spawn(async move { client.call::<Command>(&target, &run(28, Mode::Apply), &owed()).await.0 });
    eventually("silence keeps the request owed", Duration::from_secs(3), || get(&cd.stats().resends) >= 3).await;
    assert!(!call.is_finished(), "silence ends nothing");
    resolver.remove_gone(&r.node.node_id);
    let out = call.await.unwrap();
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::ReceiverGone(_))), "{out:?}");
    assert_eq!(out.code(), rdm_core::outcome::Code::Unavailable, "{out:?}");
}

/// Cell 28 (i146 Q44; node-rpc.md §43, the outcome table "receiving birth proven exited or removed"):
/// an Owed call in flight ends when its receiver's exit is proven (the live resolver deletes the birth),
/// `ReceiverGone`, code `UNAVAILABLE`: the proof of the exit ends it, as the removal does, never
/// silence. Must NOT happen: the call resent to a birth proven exited.
#[tokio::test]
async fn an_owed_call_ends_when_its_receivers_exit_is_proven_unavailable() {
    let r = raw().await;
    let resolver = Arc::new(rdm_transport::LiveNodeResolver::default());
    resolver.apply(r.node.clone(), None);
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep, resolver.clone()).with_delivery(Delivery::with_pacing(birth(), pacing()));
    let cd = client.delivery().clone();
    let target = NodeTarget::ExactNode(r.node.node_id.clone());
    let call = tokio::spawn(async move { client.call::<Command>(&target, &run(29, Mode::Apply), &owed()).await.0 });
    eventually("silence keeps the request owed", Duration::from_secs(3), || get(&cd.stats().resends) >= 3).await;
    assert!(!call.is_finished(), "silence ends nothing");
    resolver.mark_exited(&r.node.node_id, &r.node.incarnation);
    let out = tokio::time::timeout(FLOOR * 2, call).await.expect("the proof of the exit ended the call").unwrap();
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::ReceiverGone(why) if why.contains("exit is proven"))), "{out:?}");
    assert_eq!(out.code(), rdm_core::outcome::Code::Unavailable, "{out:?}");
}

/// A dial that completed but has not been pooled makes the supersession race deterministic.
/// Ending the exact receiving birth ends an Owed call (receiver-gone); a moved endpoint slot of a
/// living birth is neither the holder's answer nor its exited verdict (i146 Q52: "The obligation ends on
/// the holder's answer or exited verdict"), so the attempt sent nothing and is resent to the slot the view
/// names then. Neither request is dispatched through the superseded slot.
async fn owed_dial_superseded(ended_birth: bool) {
    let s = served().await;
    let (client, resolver) = client_of(&[&s.node]).await;
    let cd = client.delivery().clone();
    let target = s.target();
    let point = Arc::new(rdm_transport::pool::Failpoint::default());
    let opts = CallOptions { budget: Budget::Owed, after_connect: Some(point.clone()), ..Default::default() };
    let call = tokio::spawn(async move { client.call::<Command>(&target, &run(30, Mode::Apply), &opts).await.0 });
    tokio::time::timeout(Duration::from_secs(3), point.reached.notified()).await.expect("the dial connected before its slot is pooled");
    let mut current = s.node.clone();
    if ended_birth {
        current.incarnation = IncarnationId::mint();
    } else {
        // The old endpoint is still alive and its exact incarnation has not ended. Only the
        // resolved slot moved while the old slot's dial was in flight.
        current.endpoint_id = SecretKey::generate().public();
    }
    resolver.insert(current);
    point.release.notify_one();
    if ended_birth {
        let out = tokio::time::timeout(FLOOR * 2, call).await.expect("the resolver's typed change ends the call").unwrap();
        assert_eq!(get(&s.stats.dispatched), 0, "no request went through the stale slot");
        assert_eq!(out.code(), rdm_core::outcome::Code::Unavailable, "{out:?}");
        assert_eq!(out.detail_name(), "receiver-gone");
        assert!(matches!(out, RpcOutcome::NotSent(_)), "the superseded dial never crossed the commit cut");
    } else {
        eventually("the superseded attempt is resent, the call still owed", Duration::from_secs(3), || get(&cd.stats().resends) >= 1).await;
        assert!(!call.is_finished(), "a moved slot of a living birth ends nothing");
        assert_eq!(get(&s.stats.dispatched), 0, "no request went through the superseded slot");
        // The view names the birth's slot again: the next attempt reaches it (its dial passes the
        // same after-connect point, released once more) and is answered.
        resolver.insert(s.node.clone());
        point.release.notify_one();
        let out = tokio::time::timeout(FLOOR * 4, call).await.expect("the resent call is answered").unwrap();
        assert!(out.reply().is_some(), "the receiver's reply: {out:?}");
        assert_eq!(get(&s.stats.dispatched), 1, "the receiver dispatched the request once");
    }
}

#[tokio::test]
async fn an_owed_dial_superseded_by_a_new_incarnation_ends_receiver_gone_unavailable() { owed_dial_superseded(true).await; }

#[tokio::test]
async fn an_owed_dial_on_a_moved_slot_of_a_living_birth_is_resent_and_answered() { owed_dial_superseded(false).await; }


// i146 e2: the existing delivery contract, on the production 0x22 stream.
async fn node_command_stream(r: &mut Raw, client: &NodeRpcClient) -> (rdm_transport::stream::ReplyStream<rdm_core::node_command::NodeCommand>, DeliveryId, SendStream, RecvStream, Connection) {
    use rdm_core::node_command::{NodeCommand, NodeCommandRequest};
    let target = NodeTarget::ExactNode(r.node.node_id.clone());
    let request = NodeCommandRequest::DeleteConnections { node_id: r.me.node_id.clone(), incarnation: r.me.incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
    let opening = {
        let client = client.clone();
        tokio::spawn(async move { client.call_stream::<NodeCommand>(&target, &request, &CallOptions { budget: Budget::Stream { send: Duration::from_secs(5) }, ..CallOptions::default() }).await })
    };
    let (conn, mut send, recv, h) = tokio::time::timeout(Duration::from_secs(5), async {
        let conn = r.conns.recv().await.unwrap();
        loop {
            let (send, mut recv) = conn.accept_bi().await.unwrap();
            // Deliberately split the read at every header byte: the body must be consumed even
            // when it is not delivered in the read that completes the transport header.
            let mut pending = Vec::new();
            let h = read_header_chunks::<1>(&mut recv, &mut pending).await.unwrap();
            if matches!(h, TransportHeader::Request { .. }) {
                // A request is its header AND its target/context/body sections. Until those are
                // consumed, the next bytes in this direction are not transport ACK headers.
                let mut chunk = [0u8; 4096];
                loop {
                    match decode_request(&pending, |_| Some(NodeCommand::MAX_REQUEST_FRAME_BYTES)) {
                        Ok((op, _, _)) => { assert_eq!(op, NodeCommand::OP); break; }
                        Err(rdm_core::framing::RequestHead::NeedMore | rdm_core::framing::RequestHead::Targeted { .. } | rdm_core::framing::RequestHead::Ready { .. }) => {
                            let n = recv.read(&mut chunk).await.unwrap().expect("the request body arrives whole");
                            pending.extend_from_slice(&chunk[..n]);
                        }
                        Err(other) => panic!("the 0x22 request does not decode: {other:?}"),
                    }
                }
                break (conn, send, recv, h);
            }
            assert!(matches!(h, TransportHeader::Ack { .. }), "only an earlier frame's ack may precede the invocation");
        }
    }).await.expect("the 0x22 invocation arrives within its send bound");
    let id = h.delivery_id();
    write(&mut send, &encode_ack_frame(&h.ack())).await;
    let (stream, _) = opening.await.unwrap().map_err(|(o, _)| format!("{o:?}")).unwrap();
    (stream, id, send, recv, conn)
}

fn node_command_frames(me: &Birth) -> Vec<rdm_core::node_command::NodeCommandReply> {
    use rdm_core::node_command::NodeCommandReply as N;
    vec![N::CommandAccepted { node_id: me.node_id.clone(), incarnation: me.incarnation.clone() },
         N::ConnectionsDeleting { node_id: me.node_id.clone(), incarnation: me.incarnation.clone() },
         N::ConnectionsDeleted { closed: vec![], not_open: vec![], held: vec![], receipt: None }]
}

fn node_response(me: &Birth, id: &DeliveryId, seq: u32, reply: &rdm_core::node_command::NodeCommandReply) -> Vec<u8> {
    let header = TransportHeader::Response { sender: me.clone(), delivery_seq: u64::from(seq) + 1, forget_below: 1, invocation_id: id.clone(), seq };
    encode_response_frame(&header, &rdm_core::node_command::NodeCommand::encode_reply(reply).unwrap())
}

#[test]
fn every_frame_of_a_stream_carries_its_stream_id_and_sequence() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-node-command-positions").unwrap();
    let id = capture.run(4, async {
        let mut r = raw().await;
        let (client, _) = client_of(&[&r.node]).await;
        let (mut stream, id, mut send, _recv, _conn) = node_command_stream(&mut r, &client).await;
        for (seq, reply) in node_command_frames(&r.me).into_iter().enumerate() {
            write(&mut send, &node_response(&r.me, &id, seq as u32, &reply)).await;
            assert_eq!(stream.next().await, Some(StreamItem::Frame([FrameKind::Started, FrameKind::Data, FrameKind::Terminal][seq], reply)));
            stream.accepted().await;
        }
        assert!(stream.next().await.is_none());
        id.to_string()
    });
    let spans = capture.spans().unwrap();
    let received: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.stream.update.via-received-frame").collect();
    assert_eq!(received.len(), 3);
    for (seq, span) in received.iter().enumerate() {
        assert_eq!(span["attributes"]["invocation_id"], id, "the owner sees the stream identity: {span}");
        assert_eq!(span["attributes"]["seq"].as_str(), Some(seq.to_string().as_str()), "the header's position: {span}");
    }
}

#[tokio::test]
async fn a_node_command_progress_repeat_is_suppressed_before_its_owner_recovers_it() {
    let mut r = raw().await;
    let (client, _) = client_of(&[&r.node]).await;
    let (mut stream, id, mut send, _recv, _conn) = node_command_stream(&mut r, &client).await;
    let frames = node_command_frames(&r.me);
    write(&mut send, &node_response(&r.me, &id, 0, &frames[0])).await;
    assert_eq!(stream.next().await, Some(StreamItem::Frame(FrameKind::Started, frames[0].clone())));
    stream.accepted().await;
    write(&mut send, &node_response(&r.me, &id, 1, &frames[1])).await;
    assert_eq!(stream.next().await, Some(StreamItem::Frame(FrameKind::Data, frames[1].clone())));
    // The owner holds the progress record; no ack yet. Replay that SAME invocation and position,
    // rather than publishing another progress frame at a new position.
    let mut records = vec![frames[1].clone()];
    write(&mut send, &node_response(&r.me, &id, 1, &frames[1])).await;
    write(&mut send, &node_response(&r.me, &id, 2, &frames[2])).await;
    while let Some(item) = stream.next().await {
        match item {
            StreamItem::Frame(FrameKind::Data, frame) => records.push(frame),
            StreamItem::Frame(FrameKind::Terminal, frame) => assert_eq!(frame, frames[2]),
            other => panic!("no second admission or failure: {other:?}"),
        }
    }
    stream.accepted().await;
    assert_eq!(records, vec![frames[1].clone()], "the owner records one progress frame before recovery acknowledges it");
}

#[tokio::test]
async fn a_reconnect_after_the_terminal_frame_delivers_no_old_admission() {
    let mut r = raw().await;
    let (client, _) = client_of(&[&r.node]).await;
    let (mut stream, id, mut send, _recv, conn) = node_command_stream(&mut r, &client).await;
    let frames = node_command_frames(&r.me);
    for (seq, reply) in frames.iter().enumerate() {
        write(&mut send, &node_response(&r.me, &id, seq as u32, reply)).await;
        assert!(matches!(stream.next().await, Some(StreamItem::Frame(_, _))));
        stream.accepted().await;
    }
    conn.close(0u32.into(), b"completed invocation reconnect");
    // Reconnect the same sending birth and its completed-invocation memory on a fresh endpoint.
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(r.node.clone());
    let reconnected = NodeRpcClient::new(ep, resolver).with_delivery(client.delivery().clone());
    let (mut fresh, new_id, mut send, mut recv, _conn) = node_command_stream(&mut r, &reconnected).await;
    assert_ne!(new_id, id);
    write(&mut send, &node_response(&r.me, &id, 0, &frames[0])).await;
    let ack = read_header(&mut recv, &mut Vec::new()).await.expect("old frame is acknowledged without admission");
    assert_eq!(ack, TransportHeader::Ack { sender: r.me.clone(), delivery_seq: 1 });
    write(&mut send, &node_response(&r.me, &new_id, 0, &frames[0])).await;
    assert_eq!(fresh.next().await, Some(StreamItem::Frame(FrameKind::Started, frames[0].clone())));
    assert!(stream.next().await.is_none());
    assert_eq!(get(&client.delivery().stats().responses_of_completed), 1);
}
