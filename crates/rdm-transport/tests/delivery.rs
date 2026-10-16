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
    Budget, CallOptions, CarriedIdentity, Delivery, DeliveryStats, HandlerFault, NodeRpcClient, NodeRpcServer, NodeTarget, Pacing, PeerContext, ResolvedNode, ServedBirth,
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
    let me = birth();
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
    let server = build(builder).with_delivery(delivery.clone()).seal(ServedBirth { node_id: me.node_id.to_string(), incarnation: me.incarnation.to_string() }).unwrap();
    let key = SecretKey::generate();
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
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
fn trace_of(span: &tracing::Span) -> String {
    rdm_transport::traceparent_of(span).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context: its capture is installed")
}

/// The trace's Jaeger link.
fn flame(trace: &str) -> String {
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
    let mut chunk = [0u8; 4096];
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
    let h = TransportHeader::Response { sender: from.clone(), delivery_seq, retire_below: 1, invocation_id: invocation.clone(), seq };
    encode_response_frame(&h, &Tick::encode_reply(frame).unwrap())
}

fn command_response(from: &Birth, delivery_seq: u64, invocation: &DeliveryId, reply: &CommandReply) -> Vec<u8> {
    let h = TransportHeader::Response { sender: from.clone(), delivery_seq, retire_below: 1, invocation_id: invocation.clone(), seq: 0 };
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

/// Cell 4. CONTRACT: a read-only request is acked as it is dispatched: no resend while its handler
/// holds its reply.
#[tokio::test]
async fn a_read_only_request_is_acked_at_once() {
    let s = served().await;
    let (client, _) = client_of(&[&s.node]).await;
    let cd = client.delivery().clone();
    let call = tokio::spawn({
        let target = s.target();
        async move { client.call::<Command>(&target, &CommandRequest::Read { tag: 4, hold: true }, &budget(Duration::from_secs(5))).await.0 }
    });
    eventually("the read is acked", Duration::from_secs(1), || get(&cd.stats().acks_received) == 1).await;
    tokio::time::sleep(CADENCE * 4).await;
    assert_eq!(get(&cd.stats().resends), 0, "an acked request is never resent");
    s.release();
    let out = call.await.unwrap();
    assert_eq!(ran(&out), Some((4, 0)), "{out:?}");
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
    eventually("the refusal's unacked frame is resent and acked", Duration::from_secs(3), || s.delivery.owed_frames() == 0 && get(&s.delivery.stats().responses_resent) >= 1).await;
    assert_eq!(get(&cd.stats().resends), 0, "the refusal ended the request's retry");
    assert_eq!(get(&s.stats.dispatched), 0);
}

/// Cell 6. CONTRACT: an owed request to a silent receiver (it takes the request and never acks) keeps
/// being resent; the receiver's proven exit (the view's removal) ends it, by name.
#[tokio::test]
async fn a_proven_exit_ends_the_owed_request_and_silence_does_not() {
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
/// `via-response-resend`, its cause named, ending `acked`, in the caller's trace.
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
        resends.iter().any(|s| s["attributes"]["outcome"] == "acked" && s["attributes"]["reason"].as_str().is_some_and(|r| r.starts_with("the reply's write"))),
        "the response resend, its cause named and acked, in the caller's trace: {resends:#?}"
    );
    println!("delivery flame (a response resent on a new stream after a reconnect): {}", flame(&trace));
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

/// Cell 16. CONTRACT: a request's duplicate state outlives the staleness floor, retired only when
/// the sender's `retire_below` passes it.
#[tokio::test]
async fn duplicate_state_outlives_the_floor_until_the_senders_watermark_passes_it() {
    let s = served().await;
    let me = birth();
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let conn = ep.connect(iroh::EndpointAddr::new(s.node.endpoint_id).with_ip_addr(s.node.transport_addr), rdm_transport::ALPN).await.unwrap();
    let request = |seq: u64, retire_below: u64| {
        let h = TransportHeader::Request { sender: me.clone(), delivery_seq: seq, retire_below };
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
    assert_eq!(s.delivery.window_of(&me), (1, 2), "1 retired, 2 held");
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
        let server = ServerBuilder::new().ledger(ledger()).carry::<Command>().serve_forward(client).with_delivery(delivery).seal(ServedBirth { node_id: b.node_id.to_string(), incarnation: b.incarnation.to_string() }).unwrap();
        let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
        carriers.push((router, ResolvedNode { node_id: b.node_id.clone(), name: "mesh1.rpc.2".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation: b.incarnation }));
    }
    let (origin, _) = client_of(&[&target.node, &carriers[0].1, &carriers[1].1]).await;
    let identity = CarriedIdentity { id: DeliveryId { sender: origin.delivery().me().clone(), delivery_seq: 7 }, retire_below: 7 };
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
    let server = ServerBuilder::new().ledger(ledger()).with_delivery(delivery.clone()).seal(ServedBirth { node_id: me.node_id.to_string(), incarnation: me.incarnation.to_string() }).unwrap();
    let _router = Router::builder(ep).accept(rdm_transport::ALPN, server.clone()).spawn();
    server.drain();
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
    assert_eq!(get(&server.stats().draining), 0, "a response frame never meets admission");
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
