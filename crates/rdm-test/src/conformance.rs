//! The conformance harness (consumer-guide-ops.md §8): the cells any Node RPC protocol runs by
//! naming its type. [`unary`] runs a unary protocol, [`streaming`] a server-streaming one; each
//! check builds its own server and client on 127.0.0.1 from the protocol's own registration (the
//! `serve_app` shape, `FnOnce(ServerBuilder) -> ServerBuilder`), with the production frames and a
//! shortened delivery schedule, and reads the runtime's counters on both sides, never the handler's.
//!
//! The checks: each named refusal answered (the reserved constructors classified, the runtime's
//! `not_ready`, `busy` and `connections_deleting`, the protocol's own named refusals); the request
//! over its ceiling refused `Malformed(TooLarge)`, and one no variant decodes
//! `Malformed(UnknownVariant)`; a duplicate delivery suppressed; a reply lost after acceptance
//! delivered once; and for a streaming protocol its frames in order, the 64-frame bound holding the
//! producer, a caller dropped mid-stream releasing the invocation, and the consumer's
//! acknowledgement after its record: `accepted` called before the terminal is recorded fails the
//! check ([`StreamRecord`]).
//!
//! A check never panics: the [`Report`] names every check, its failure and its trace, and the
//! cell asserts it. Each check runs under its own root span, `rdm.test.conformance.serve.via-check`
//! (`check`, `protocol`), so a cell that installs a capture has one trace per check.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::delivery::MAX_REORDER_POSITIONS;
use rdm_core::framing::{Birth, DeliveryId};
use rdm_core::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use rdm_core::streaming::{FrameKind, StreamingProtocol};
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::admission::Limits;
use rdm_transport::stream::{ReplyStream, StreamItem};
use rdm_transport::{
    Budget, CallOptions, CarriedIdentity, Decode, Delivery, DeliveryStats, NodeRpcClient, NodeRpcServer, NodeTarget, Pacing, ReplyWithhold, ResolvedNode, ServedBirth, ServerBuilder,
    ServerStats, StaticResolver,
};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::Instrument;

/// The delivery schedule both sides of a check run: the publish cadence 100 ms, the floor 800 ms,
/// no jitter (the delivery cells' schedule), so a lost reply or ack is repaired within a second.
const PACING: Pacing = Pacing { cadence: Duration::from_millis(100), floor: Duration::from_millis(800), jitter: false };
/// One check's bound: a check still running past it fails, named.
const CHECK_BOUND: Duration = Duration::from_secs(30);
/// One frame's bound, from the moment the check asks for it.
const FRAME_BOUND: Duration = Duration::from_secs(5);
/// How long a held producer is watched for a frame past the 64-frame bound.
const HELD_WATCH: Duration = Duration::from_millis(500);

/// The protocol's registration: what its process's `serve_app` adds to the builder (its ledger rows
/// and its `serve` or `serve_stream`). Called once per check, on a fresh builder.
pub type Serve = Arc<dyn Fn(ServerBuilder) -> ServerBuilder + Send + Sync>;

/// A consumer of a stream: it reads the stream to its end, records each frame in `record` (the
/// stand-in for the application's durable record) and acknowledges with `accepted`.
pub type Consume<P> = Arc<dyn Fn(ReplyStream<P>, StreamRecord) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// What a unary protocol hands the harness.
pub struct Unary<P: NodeProtocol> {
    /// The protocol's registration.
    pub serve: Serve,
    /// A request its handler answers with a success.
    pub answered: P::Request,
    /// The protocol's own named refusals: a request and the reply name (`reply_detail`) it is
    /// refused with.
    pub refusals: Vec<(P::Request, &'static str)>,
}

/// What a server-streaming protocol hands the harness.
pub struct Streaming<P: StreamingProtocol> {
    /// The protocol's registration.
    pub serve: Serve,
    /// A request its handler answers with a short stream: `Started`, data frames, its terminal.
    pub answered: P::Request,
    /// A request its handler answers with more than 64 small frames (the bound's check).
    pub long: P::Request,
    /// The protocol's own named refusals: a request and the reply name it is refused with, the
    /// stream's one frame.
    pub refusals: Vec<(P::Request, &'static str)>,
    /// The protocol's consumer, run against the acknowledgement check.
    pub consume: Consume<P>,
}

/// One check of the harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Check {
    /// The reserved constructors the runtime answers with (`peer_unresolved`, `not_ready`, `busy`,
    /// `connections_deleting`, `malformed`) build replies classified as their refusal, named by
    /// their standard name, that decode back to themselves (and, streaming, are refusal frames).
    ReservedRefusals,
    /// A request the handler answers is answered with a success, dispatched once.
    Answered,
    /// A node whose ready gate is closed answers the protocol's `not_ready`; no handler runs.
    NotReady,
    /// A node at its admission bound answers the protocol's `busy`; no handler runs.
    Busy,
    /// A connections-deleting node answers the protocol's `connections_deleting`; no handler runs.
    ConnectionsDeleting,
    /// A request over its ceiling is refused `Malformed(TooLarge)` before its body; no handler runs.
    TooLarge,
    /// A request no variant decodes is refused `Malformed(UnknownVariant)`.
    UnknownVariant,
    /// Each of the protocol's own named refusals is answered by its name.
    NamedRefusals,
    /// A second delivery of one request (its identity) is acked without dispatch: the handler runs once.
    DuplicateSuppressed,
    /// A reply lost after the request's acceptance is delivered on a new stream; the handler runs once.
    ReplyLostAfterAcceptance,
    /// Streaming: `Started`, data frames, the terminal, at consecutive positions of one invocation.
    FramesInOrder,
    /// Streaming: with 64 frames unacknowledged the producer writes no further frame until the
    /// consumer acknowledges, then the stream reaches its terminal.
    BoundHoldsProducer,
    /// Streaming: a caller that drops the stream mid-stream ends the invocation: the handler ends and
    /// the producer owes nothing.
    CallerDropped,
    /// Streaming: the protocol's consumer acknowledges only frames it recorded; `accepted` called
    /// before the terminal is recorded fails the check.
    AcceptedAfterRecord,
}

impl Check {
    /// The check's name, on its root span and in the report.
    pub fn name(self) -> &'static str {
        match self {
            Self::ReservedRefusals => "reserved-refusals",
            Self::Answered => "answered",
            Self::NotReady => "not-ready",
            Self::Busy => "busy",
            Self::ConnectionsDeleting => "connections-deleting",
            Self::TooLarge => "too-large",
            Self::UnknownVariant => "unknown-variant",
            Self::NamedRefusals => "named-refusals",
            Self::DuplicateSuppressed => "duplicate-suppressed",
            Self::ReplyLostAfterAcceptance => "reply-lost-after-acceptance",
            Self::FramesInOrder => "frames-in-order",
            Self::BoundHoldsProducer => "bound-holds-producer",
            Self::CallerDropped => "caller-dropped",
            Self::AcceptedAfterRecord => "accepted-after-record",
        }
    }
}

/// Every check a unary protocol runs.
pub const UNARY: &[Check] = &[
    Check::ReservedRefusals,
    Check::Answered,
    Check::NotReady,
    Check::Busy,
    Check::ConnectionsDeleting,
    Check::TooLarge,
    Check::UnknownVariant,
    Check::NamedRefusals,
    Check::DuplicateSuppressed,
    Check::ReplyLostAfterAcceptance,
];

/// Every check a server-streaming protocol runs.
pub const STREAMING: &[Check] = &[
    Check::ReservedRefusals,
    Check::NotReady,
    Check::Busy,
    Check::ConnectionsDeleting,
    Check::TooLarge,
    Check::UnknownVariant,
    Check::NamedRefusals,
    Check::FramesInOrder,
    Check::BoundHoldsProducer,
    Check::CallerDropped,
    Check::DuplicateSuppressed,
    Check::ReplyLostAfterAcceptance,
    Check::AcceptedAfterRecord,
];

/// What became of one check.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// The check.
    pub check: Check,
    /// `None` when it passed; why it failed otherwise.
    pub failure: Option<String>,
    /// The trace its root span opened, when the cell installed a capture.
    pub trace: Option<String>,
}

/// One protocol's run of the harness.
#[derive(Debug, Clone)]
pub struct Report {
    /// The protocol's catalog name.
    pub protocol: &'static str,
    /// Every check run, in order.
    pub outcomes: Vec<Outcome>,
}

impl Report {
    /// The checks that failed.
    pub fn failed(&self) -> Vec<&Outcome> {
        self.outcomes.iter().filter(|o| o.failure.is_some()).collect()
    }

    /// The outcome of `check`, if it ran.
    pub fn outcome(&self, check: Check) -> Option<&Outcome> {
        self.outcomes.iter().find(|o| o.check == check)
    }

    /// One line per check: passed or failed, its trace's Jaeger link, and why it failed.
    pub fn summary(&self) -> String {
        let base = std::env::var("JAEGER_QUERY_URL").unwrap_or_else(|_| "http://192.168.68.99:16687".into());
        let mut lines = vec![format!("conformance of {}: {} checks, {} failed", self.protocol, self.outcomes.len(), self.failed().len())];
        for o in &self.outcomes {
            let trace = o.trace.as_ref().map(|t| format!(" {base}/trace/{t}")).unwrap_or_default();
            match &o.failure {
                None => lines.push(format!("  pass {}{trace}", o.check.name())),
                Some(why) => lines.push(format!("  FAIL {}{trace}: {why}", o.check.name())),
            }
        }
        lines.join("\n")
    }
}

/// The stand-in for a streaming consumer's durable record. The consumer records each frame here,
/// at the position `ReplyStream::position` names, before it acknowledges it with `accepted`. A
/// frame recorded after more frames were acknowledged than were recorded is a consumer that called
/// `accepted` before it recorded what it acknowledged: the acknowledgement check fails, naming it.
#[derive(Clone)]
pub struct StreamRecord {
    inner: Arc<Mutex<Recorded>>,
    /// The consumer's client's delivery state: its `acks_sent` counts the frames `accepted` acked.
    delivery: Delivery,
}

#[derive(Default)]
struct Recorded {
    frames: Vec<(Option<u32>, FrameKind)>,
    early: Vec<String>,
}

impl StreamRecord {
    fn new(delivery: Delivery) -> Self {
        Self { inner: Arc::default(), delivery }
    }

    /// Record the frame of `kind` at `position` (the stream's position of the frame just handed over).
    pub fn put(&self, position: Option<(&DeliveryId, u32)>, kind: FrameKind) {
        let acked = DeliveryStats::get(&self.delivery.stats().acks_sent);
        let mut r = self.inner.lock().unwrap();
        let seq = position.map(|(_, seq)| seq);
        let recorded = r.frames.len();
        if acked > recorded as u64 {
            r.early.push(format!("the frame at position {seq:?} ({kind:?}) was recorded after {acked} frames were acknowledged and {recorded} recorded: accepted() was called before it was recorded"));
        }
        r.frames.push((seq, kind));
    }

    /// The frames recorded, in order: each one's position and kind.
    pub fn recorded(&self) -> Vec<(Option<u32>, FrameKind)> {
        self.inner.lock().unwrap().frames.clone()
    }
}

/// Run every unary check against `P`.
pub async fn unary<P>(f: &Unary<P>) -> Report
where
    P: NodeProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    unary_checks(f, UNARY).await
}

/// Run `checks` against unary `P`; a streaming-only check fails, named.
pub async fn unary_checks<P>(f: &Unary<P>, checks: &[Check]) -> Report
where
    P: NodeProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let mut outcomes = Vec::new();
    for &c in checks {
        outcomes.push(
            run(c, P::NAME, async {
                match c {
                    Check::ReservedRefusals => reserved_refusals::<P>(None),
                    Check::Answered => unary_answered(f).await,
                    Check::NotReady | Check::Busy | Check::ConnectionsDeleting => match refusal_rig::<P>(&f.serve, c).await {
                        Ok((r, kind, name)) => {
                            let answer = unary_answer::<P>(&r, &f.answered).await;
                            refused(&r, answer, kind, name)
                        }
                        Err(why) => Err(why),
                    },
                    Check::TooLarge | Check::UnknownVariant => malformed::<P>(&f.serve, c).await,
                    Check::NamedRefusals => match Rig::new(&f.serve, P::OP, false).await {
                        Ok(r) => {
                            let mut answers = Vec::new();
                            for (req, want) in &f.refusals {
                                answers.push((format!("{req:?}"), *want, unary_answer::<P>(&r, req).await));
                            }
                            named(answers)
                        }
                        Err(why) => Err(why),
                    },
                    Check::DuplicateSuppressed => unary_duplicate(f).await,
                    Check::ReplyLostAfterAcceptance => unary_reply_lost(f).await,
                    other => Err(format!("{} is a streaming check", other.name())),
                }
            })
            .await,
        );
    }
    Report { protocol: P::NAME, outcomes }
}

/// Run every streaming check against `P`.
pub async fn streaming<P>(f: &Streaming<P>) -> Report
where
    P: StreamingProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    streaming_checks(f, STREAMING).await
}

/// Run `checks` against streaming `P`.
pub async fn streaming_checks<P>(f: &Streaming<P>, checks: &[Check]) -> Report
where
    P: StreamingProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let mut outcomes = Vec::new();
    for &c in checks {
        outcomes.push(
            run(c, P::NAME, async {
                match c {
                    Check::ReservedRefusals => reserved_refusals::<P>(Some(P::frame_kind)),
                    Check::Answered | Check::FramesInOrder => frames_in_order(f).await,
                    Check::NotReady | Check::Busy | Check::ConnectionsDeleting => match refusal_rig::<P>(&f.serve, c).await {
                        Ok((r, kind, name)) => {
                            let answer = stream_answer::<P>(&r, &f.answered).await;
                            refused(&r, answer, kind, name)
                        }
                        Err(why) => Err(why),
                    },
                    Check::TooLarge | Check::UnknownVariant => malformed::<P>(&f.serve, c).await,
                    Check::NamedRefusals => match Rig::new(&f.serve, P::OP, false).await {
                        Ok(r) => {
                            let mut answers = Vec::new();
                            for (req, want) in &f.refusals {
                                answers.push((format!("{req:?}"), *want, stream_answer::<P>(&r, req).await));
                            }
                            named(answers)
                        }
                        Err(why) => Err(why),
                    },
                    Check::DuplicateSuppressed => stream_duplicate(f).await,
                    Check::ReplyLostAfterAcceptance => stream_reply_lost(f).await,
                    Check::BoundHoldsProducer => bound_holds_producer(f).await,
                    Check::CallerDropped => caller_dropped(f).await,
                    Check::AcceptedAfterRecord => accepted_after_record(f).await,
                }
            })
            .await,
        );
    }
    Report { protocol: P::NAME, outcomes }
}

async fn run(check: Check, protocol: &'static str, body: impl Future<Output = Result<(), String>>) -> Outcome {
    let root = tracing::info_span!("rdm.test.conformance.serve.via-check", check = check.name(), protocol, outcome = tracing::field::Empty);
    let trace = rdm_transport::traceparent_of(&root).and_then(|tp| tp.split('-').nth(1).map(str::to_string));
    let failure = match tokio::time::timeout(CHECK_BOUND, body.instrument(root.clone())).await {
        Ok(Ok(())) => None,
        Ok(Err(why)) => Some(why),
        Err(_) => Some(format!("the check did not finish within {CHECK_BOUND:?}")),
    };
    root.record("outcome", if failure.is_none() { "pass" } else { "fail" });
    if let Some(why) = &failure {
        root.in_scope(|| tracing::info!(reason = %why, "the check failed"));
    }
    Outcome { check, failure, trace }
}

// ---- the rig: one server, one client, per check ----

struct Rig {
    _router: Router,
    server: NodeRpcServer,
    client: NodeRpcClient,
    target: NodeTarget,
    ready: Arc<AtomicBool>,
    withhold: Arc<ReplyWithhold>,
}

impl Rig {
    /// A server serving what `serve` registers, its ready gate open (and, `busy`, its admission
    /// bound for `op` at zero), and a client that resolves it.
    async fn new(serve: &Serve, op: u8, busy: bool) -> Result<Self, String> {
        let me = Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let ready = Arc::new(AtomicBool::new(true));
        let withhold = Arc::new(ReplyWithhold::default());
        let mut b = serve(ServerBuilder::new()).with_delivery(Delivery::with_pacing(me.clone(), PACING)).with_ready_gate(ready.clone()).withhold_replies(withhold.clone());
        if busy {
            b = b.limits(op, Limits { node_wide: 0, per_caller: 0 });
        }
        let server = b.build(ServedBirth { node_id: me.node_id.to_string(), incarnation: me.incarnation.to_string() }).map_err(|e| format!("the registration does not build: {e:?}"))?;
        let key = SecretKey::generate();
        let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().expect("an address")).await.map_err(|e| format!("the server's endpoint does not bind: {e}"))?;
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).ok_or("the server's endpoint has no IPv4 socket")?;
        let router = Router::builder(ep).accept(rdm_transport::ALPN, server.clone()).spawn();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { node_id: me.node_id.clone(), name: "mesh1.rpc.1".parse().expect("a path name"), endpoint_id: key.public(), transport_addr: addr, incarnation: me.incarnation.clone() });
        let cep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().expect("an address")).await.map_err(|e| format!("the client's endpoint does not bind: {e}"))?;
        let caller = Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let client = NodeRpcClient::new(cep, resolver).with_delivery(Delivery::with_pacing(caller, PACING));
        Ok(Self { _router: router, server, client, target: NodeTarget::ExactNode(me.node_id), ready, withhold })
    }

    fn stats(&self) -> Arc<ServerStats> {
        self.server.stats()
    }

    fn dispatched(&self) -> u64 {
        ServerStats::get(&self.stats().dispatched)
    }

    /// The handler ran exactly `n` times.
    fn dispatched_exactly(&self, n: u64) -> Result<(), String> {
        match self.dispatched() {
            d if d == n => Ok(()),
            d => Err(format!("dispatched {d} times, expected {n}")),
        }
    }

    /// Every one of the `n` frames handed over was acknowledged: the consumer wrote `n` acks and the
    /// producer received each, so it holds nothing because the acks arrived.
    async fn acked_every(&self, n: usize, what: &str) -> Result<(), String> {
        let sent = DeliveryStats::get(&self.client.delivery().stats().acks_sent);
        let stats = self.server.delivery().stats();
        let arrived = eventually(FRAME_BOUND, || DeliveryStats::get(&stats.acks_received) >= n as u64).await;
        let received = DeliveryStats::get(&stats.acks_received);
        if sent == n as u64 && arrived && received == n as u64 {
            Ok(())
        } else {
            Err(format!("{what}: {n} frames handed over, {sent} acks written by the consumer, {received} received by the producer"))
        }
    }

    /// The producer owes the caller nothing: every frame it wrote was acked or is owed no longer.
    async fn owes_nothing(&self, what: &str) -> Result<(), String> {
        let d = self.server.delivery().clone();
        if eventually(FRAME_BOUND, || d.owed_frames() == 0).await {
            Ok(())
        } else {
            Err(format!("{what}: the producer still owes {} frames after {FRAME_BOUND:?}", d.owed_frames()))
        }
    }
}

fn options() -> CallOptions {
    CallOptions { budget: Budget::Overall(Duration::from_secs(10)), ..CallOptions::default() }
}

async fn eventually(within: Duration, f: impl Fn() -> bool) -> bool {
    let until = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < until {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    f()
}

/// A reply's class and name, or why there is no reply.
type Answer = Result<(ReplyKind, &'static str), String>;

fn answer_of<R: Debug>(out: &RpcOutcome<R>) -> Answer {
    match out.reply() {
        Some(r) => Ok((r.class(), r.detail_name())),
        None => Err(format!("no reply: {out:?}")),
    }
}

fn expect(answer: Answer, kind: ReplyKind, name: &str) -> Result<(), String> {
    let (k, n) = answer?;
    if (k, n) == (kind, name) {
        Ok(())
    } else {
        Err(format!("answered {n} ({}), expected {name} ({})", k.code().name(), kind.code().name()))
    }
}

async fn unary_answer<P>(r: &Rig, req: &P::Request) -> Answer
where
    P: NodeProtocol,
    P::Reply: Debug,
{
    answer_of(&r.client.call::<P>(&r.target, req, &options()).await.0)
}

/// The answer to `req` on a stream: the refusal that is its one frame (or that answered before the
/// stream opened), or the class and name of its first frame.
async fn stream_answer<P>(r: &Rig, req: &P::Request) -> Answer
where
    P: StreamingProtocol,
    P::Reply: Debug,
{
    match r.client.call_stream::<P>(&r.target, req, &options()).await {
        Err((out, _)) => answer_of(&out),
        Ok((mut s, _)) => {
            let (kind, reply) = frame(&mut s).await?;
            let named = (P::classify_reply(&reply), P::reply_detail(&reply));
            if let FrameKind::Refusal(_) = kind {
                s.accepted().await;
                match tokio::time::timeout(FRAME_BOUND, s.next()).await {
                    Ok(None) => {}
                    other => return Err(format!("a refusal is a stream's one frame; after it: {other:?}")),
                }
            }
            Ok(named)
        }
    }
}

/// The stream's next frame, or why none came.
async fn frame<P>(s: &mut ReplyStream<P>) -> Result<(FrameKind, P::Reply), String>
where
    P: StreamingProtocol,
    P::Reply: Debug,
{
    match tokio::time::timeout(FRAME_BOUND, s.next()).await {
        Ok(Some(StreamItem::Frame(kind, reply))) => Ok((kind, reply)),
        Ok(Some(StreamItem::Failed(f))) => Err(format!("the stream failed: {f:?}")),
        Ok(None) => Err("the stream ended".into()),
        Err(_) => Err(format!("no frame within {FRAME_BOUND:?}")),
    }
}

async fn open<P>(r: &Rig, req: &P::Request, opts: &CallOptions) -> Result<ReplyStream<P>, String>
where
    P: StreamingProtocol,
    P::Reply: Debug,
{
    r.client.call_stream::<P>(&r.target, req, opts).await.map(|(s, _)| s).map_err(|(out, _)| format!("the stream did not open: {out:?}"))
}

/// Read `s` to its terminal, acknowledging each frame once it is read (a consumer whose record is
/// the frame list), and hand back each frame's kind and position.
async fn read_to_terminal<P>(s: &mut ReplyStream<P>, seen: &mut Vec<(FrameKind, Option<(DeliveryId, u32)>)>) -> Result<(), String>
where
    P: StreamingProtocol,
    P::Reply: Debug,
{
    loop {
        let (kind, _) = frame(s).await.map_err(|why| format!("{why} after {} frames", seen.len()))?;
        seen.push((kind, s.position().map(|(id, seq)| (id.clone(), seq))));
        s.accepted().await;
        if kind == FrameKind::Terminal {
            return Ok(());
        }
    }
}

/// The frames are `Started`, data frames, the terminal, at positions 0, 1, … of one invocation.
fn in_order(seen: &[(FrameKind, Option<(DeliveryId, u32)>)]) -> Result<(), String> {
    let kinds: Vec<FrameKind> = seen.iter().map(|(k, _)| *k).collect();
    let n = kinds.len();
    let shaped = n >= 2 && kinds[0] == FrameKind::Started && kinds[n - 1] == FrameKind::Terminal && kinds[1..n - 1].iter().all(|k| *k == FrameKind::Data);
    if !shaped {
        return Err(format!("the frames are not Started, data, Terminal: {kinds:?}"));
    }
    let positions: Vec<Option<u32>> = seen.iter().map(|(_, p)| p.as_ref().map(|(_, seq)| *seq)).collect();
    if positions.iter().enumerate().any(|(i, p)| *p != Some(i as u32)) {
        return Err(format!("the positions are not 0, 1, … in order, each once: {positions:?}"));
    }
    let first = seen[0].1.as_ref().map(|(id, _)| id);
    if seen.iter().any(|(_, p)| p.as_ref().map(|(id, _)| id) != first) {
        return Err("the frames name more than one invocation".into());
    }
    Ok(())
}

// ---- the checks ----

/// The reserved constructors the runtime answers with: each classified as its refusal, named by the
/// name the runtime's refusal carries, decoding back to its own bytes and, for a stream, a refusal
/// frame of its class. `unauthorized` holds its codec position only (no mesh receiver sends it), and
/// its reply is built only in a protocol's definition and its `#[test]` cells (the name check's
/// reserved-code rule): the protocol's wire-pin cell proves it.
fn reserved_refusals<P>(frame_kind: Option<fn(&P::Reply) -> FrameKind>) -> Result<(), String>
where
    P: NodeProtocol,
{
    let r = || "conformance".to_string();
    let five: [(&str, P::Reply, ReplyKind, &str); 5] = [
        ("peer_unresolved", P::peer_unresolved(r()), ReplyKind::PeerUnresolved, "peer-unresolved"),
        ("not_ready", P::not_ready(r()), ReplyKind::NotReady, "not-ready"),
        ("busy", P::busy(r()), ReplyKind::Busy, "busy"),
        ("connections_deleting", P::connections_deleting(r()), ReplyKind::ConnectionsDeleting, "connections-deleting"),
        ("malformed", P::malformed(MalformedKind::TooLarge), ReplyKind::Malformed(MalformedKind::TooLarge), "malformed"),
    ];
    let mut off = Vec::new();
    for (ctor, reply, kind, name) in &five {
        let (k, n) = (P::classify_reply(reply), P::reply_detail(reply));
        if (k, n) != (*kind, *name) {
            off.push(format!("{ctor}: classified {n} ({}), expected {name} ({})", k.code().name(), kind.code().name()));
        }
        match P::encode_reply(reply).map(|b| (P::decode_reply(&b).map(|d| P::encode_reply(&d).ok()), b)) {
            Ok((Ok(Some(again)), bytes)) if again == bytes => {}
            other => off.push(format!("{ctor}: does not decode back to its own bytes: {:?}", other.map(|(d, _)| d.is_ok()))),
        }
        if let Some(fk) = frame_kind {
            if fk(reply) != FrameKind::Refusal(*kind) {
                off.push(format!("{ctor}: frame kind {:?}, expected Refusal", fk(reply)));
            }
        }
    }
    if off.is_empty() {
        Ok(())
    } else {
        Err(off.join("; "))
    }
}

async fn unary_answered<P>(f: &Unary<P>) -> Result<(), String>
where
    P: NodeProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let r = Rig::new(&f.serve, P::OP, false).await?;
    let (kind, name) = unary_answer::<P>(&r, &f.answered).await?;
    if kind != ReplyKind::Success {
        return Err(format!("answered {name} ({}), expected a success", kind.code().name()));
    }
    r.dispatched_exactly(1)?;
    r.owes_nothing("the answered reply").await
}

/// The rig of the runtime's refusal `check` (not-ready, busy, connections-deleting), its condition
/// set, and the protocol's own reserved variant that answers it.
async fn refusal_rig<P: NodeProtocol>(serve: &Serve, check: Check) -> Result<(Rig, ReplyKind, &'static str), String> {
    let r = Rig::new(serve, P::OP, check == Check::Busy).await?;
    let (kind, name) = match check {
        Check::NotReady => {
            r.ready.store(false, Ordering::SeqCst);
            (ReplyKind::NotReady, P::reply_detail(&P::not_ready(String::new())))
        }
        Check::Busy => (ReplyKind::Busy, P::reply_detail(&P::busy(String::new()))),
        Check::ConnectionsDeleting => {
            r.server.begin_connections_delete();
            (ReplyKind::ConnectionsDeleting, P::reply_detail(&P::connections_deleting(String::new())))
        }
        other => return Err(format!("{} is not a runtime refusal", other.name())),
    };
    Ok((r, kind, name))
}

/// The runtime's refusal answered the call by the protocol's reserved variant, and no handler ran.
fn refused(r: &Rig, answer: Answer, kind: ReplyKind, name: &str) -> Result<(), String> {
    expect(answer, kind, name)?;
    r.dispatched_exactly(0)
}

/// postcard's unsigned varint of `n`: an enum's discriminant on the wire.
fn varint(mut n: u32) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// A request over its ceiling (`MAX_REQUEST_FRAME_BYTES + 1` bytes) refused `Malformed(TooLarge)`
/// before its body is read, no handler run; or a request whose discriminant is the first one past
/// the protocol's variants refused `Malformed(UnknownVariant)`.
async fn malformed<P>(serve: &Serve, check: Check) -> Result<(), String>
where
    P: NodeProtocol,
    P::Reply: Debug,
{
    let r = Rig::new(serve, P::OP, false).await?;
    let (payload, kind) = match check {
        Check::TooLarge => (vec![0u8; P::MAX_REQUEST_FRAME_BYTES + 1], MalformedKind::TooLarge),
        _ => (varint(P::REQUEST_VARIANTS), MalformedKind::UnknownVariant),
    };
    let (out, _) = r
        .client
        .invoke_raw::<P::Reply, _>(&r.target, P::OP, payload, P::MAX_REPLY_FRAME_BYTES, &options(), |d| match d {
            Decode::Committed(c, b) => c.reply::<P>(b),
            Decode::Early(e, b) => e.reply::<P>(b),
        })
        .await;
    expect(answer_of(&out), ReplyKind::Malformed(kind), P::reply_detail(&P::malformed(kind)))?;
    if check == Check::TooLarge {
        r.dispatched_exactly(0)?;
    }
    Ok(())
}

/// Each of the protocol's own named refusals answered by its name, a refusal (never a success):
/// each request (as written), the name it is refused with, and its answer.
fn named(answers: Vec<(String, &'static str, Answer)>) -> Result<(), String> {
    let mut off = Vec::new();
    for (req, want, answer) in answers {
        match answer {
            Ok((kind, name)) if name == want && kind != ReplyKind::Success => {}
            Ok((kind, name)) => off.push(format!("{req}: answered {name} ({}), expected the refusal {want}", kind.code().name())),
            Err(why) => off.push(format!("{req}: {why}")),
        }
    }
    if off.is_empty() {
        Ok(())
    } else {
        Err(off.join("; "))
    }
}

/// One identity, delivered twice: the first answered, the second (after the first's acceptance)
/// acked without dispatch. The identity is the client's own birth's, as a resend carries it.
fn one_identity(r: &Rig) -> CallOptions {
    let id = DeliveryId { sender: r.client.delivery().me().clone(), delivery_seq: 1 };
    CallOptions { carried: Some(CarriedIdentity { id, forget_below: 1 }), ..options() }
}

async fn repeat_acked_without_dispatch(r: &Rig, second: tokio::task::JoinHandle<()>) -> Result<(), String> {
    let stats = r.server.delivery().stats();
    let acked = eventually(FRAME_BOUND, || DeliveryStats::get(&stats.duplicates_acked) >= 1).await;
    second.abort();
    if !acked {
        return Err(format!("the second delivery of the identity was not acked as a duplicate within {FRAME_BOUND:?}"));
    }
    r.dispatched_exactly(1).map_err(|why| format!("the second delivery reached the handler: {why}"))
}

async fn unary_duplicate<P>(f: &Unary<P>) -> Result<(), String>
where
    P: NodeProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let r = Rig::new(&f.serve, P::OP, false).await?;
    let opts = one_identity(&r);
    let (kind, name) = answer_of(&r.client.call::<P>(&r.target, &f.answered, &opts).await.0)?;
    if kind != ReplyKind::Success {
        return Err(format!("the first delivery answered {name}, expected a success"));
    }
    let second = {
        let (client, target, req) = (r.client.clone(), r.target.clone(), f.answered.clone());
        tokio::spawn(
            async move {
                let _ = client.call::<P>(&target, &req, &opts).await;
            }
            .instrument(tracing::Span::current()),
        )
    };
    repeat_acked_without_dispatch(&r, second).await
}

async fn stream_duplicate<P>(f: &Streaming<P>) -> Result<(), String>
where
    P: StreamingProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let r = Rig::new(&f.serve, P::OP, false).await?;
    let opts = one_identity(&r);
    let mut s = open::<P>(&r, &f.answered, &opts).await?;
    read_to_terminal(&mut s, &mut Vec::new()).await.map_err(|why| format!("the first delivery: {why}"))?;
    drop(s);
    let second = {
        let (client, target, req) = (r.client.clone(), r.target.clone(), f.answered.clone());
        tokio::spawn(
            async move {
                if let Ok((mut s, _)) = client.call_stream::<P>(&target, &req, &opts).await {
                    let _ = s.next().await;
                }
            }
            .instrument(tracing::Span::current()),
        )
    };
    repeat_acked_without_dispatch(&r, second).await
}

/// The handler answers; its reply is withheld (the request is accepted at the release), the
/// connection the request came on is cut, and the release writes the reply into the cut stream: it
/// is lost. The caller still gets that reply, resent on a new stream, and the handler ran once.
async fn unary_reply_lost<P>(f: &Unary<P>) -> Result<(), String>
where
    P: NodeProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let r = Rig::new(&f.serve, P::OP, false).await?;
    r.withhold.arm();
    let call = {
        let (client, target, req) = (r.client.clone(), r.target.clone(), f.answered.clone());
        tokio::spawn(async move { answer_of(&client.call::<P>(&target, &req, &options()).await.0) }.instrument(tracing::Span::current()))
    };
    tokio::time::timeout(FRAME_BOUND, r.withhold.reached()).await.map_err(|_| format!("the handler did not reach its reply within {FRAME_BOUND:?}"))?;
    if r.server.close_connections_except(&[], "conformance: the reply is lost after acceptance") == 0 {
        return Err("no connection to cut while the reply was withheld".into());
    }
    r.withhold.release();
    let late = tokio::time::timeout(FRAME_BOUND, r.withhold.late_reply()).await.map_err(|_| "the released reply was never written".to_string())?;
    if late.written.is_ok() {
        return Err(format!("the released reply was written whole on its first stream, so nothing was lost: {late:?}"));
    }
    let (kind, name) = call.await.map_err(|e| format!("the call's task: {e}"))??;
    if kind != ReplyKind::Success {
        return Err(format!("the caller got {name}, expected the handler's success"));
    }
    let resent = DeliveryStats::get(&r.server.delivery().stats().responses_resent);
    if resent == 0 {
        return Err("the caller has the reply but no response frame was resent".into());
    }
    r.dispatched_exactly(1)?;
    r.owes_nothing("the resent reply").await
}

async fn frames_in_order<P>(f: &Streaming<P>) -> Result<(), String>
where
    P: StreamingProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let r = Rig::new(&f.serve, P::OP, false).await?;
    let mut handed = 0;
    for req in [&f.answered, &f.long] {
        let mut s = open::<P>(&r, req, &options()).await?;
        let mut seen = Vec::new();
        read_to_terminal(&mut s, &mut seen).await?;
        handed += seen.len();
        in_order(&seen).map_err(|why| format!("{req:?}: {why}"))?;
        match tokio::time::timeout(FRAME_BOUND, s.next()).await {
            Ok(None) => {}
            other => return Err(format!("{req:?}: something followed the terminal: {other:?}")),
        }
    }
    r.dispatched_exactly(2)?;
    r.acked_every(handed, "both streams read to their terminals").await?;
    r.owes_nothing("both streams read to their terminals").await
}

/// The consumer reads the long stream and acknowledges nothing: 64 frames reach it and no further
/// frame is written; once it acknowledges, the stream goes on to its terminal.
async fn bound_holds_producer<P>(f: &Streaming<P>) -> Result<(), String>
where
    P: StreamingProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let bound = MAX_REORDER_POSITIONS as usize;
    let r = Rig::new(&f.serve, P::OP, false).await?;
    let mut s = open::<P>(&r, &f.long, &options()).await?;
    for i in 0..bound {
        let (kind, _) = frame(&mut s).await.map_err(|why| format!("frame {i} of the first {bound}: {why}"))?;
        if kind == FrameKind::Terminal {
            return Err(format!("the long request's stream ended after {} frames; the check needs more than {bound}", i + 1));
        }
    }
    let owed = r.server.delivery().clone();
    if !eventually(FRAME_BOUND, || owed.owed_frames() == bound).await {
        return Err(format!("{} frames owed with {bound} handed over and none acknowledged", owed.owed_frames()));
    }
    if let Ok(item) = tokio::time::timeout(HELD_WATCH, s.next()).await {
        return Err(format!("a frame past the bound arrived while {bound} were unacknowledged: {item:?}"));
    }
    if owed.owed_frames() != bound {
        return Err(format!("the producer wrote past the bound: {} frames owed", owed.owed_frames()));
    }
    s.accepted().await;
    let mut rest = Vec::new();
    read_to_terminal(&mut s, &mut rest).await.map_err(|why| format!("once acknowledged: {why}"))?;
    r.dispatched_exactly(1)?;
    r.acked_every(bound + rest.len(), "the long stream read to its terminal").await?;
    r.owes_nothing("the long stream read to its terminal").await
}

/// The caller drops the long stream mid-stream: the handler ends (the invocation and its admission
/// are released) and the producer owes nothing.
async fn caller_dropped<P>(f: &Streaming<P>) -> Result<(), String>
where
    P: StreamingProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let r = Rig::new(&f.serve, P::OP, false).await?;
    let mut s = open::<P>(&r, &f.long, &options()).await?;
    for i in 0..3 {
        let (kind, _) = frame(&mut s).await.map_err(|why| format!("frame {i}: {why}"))?;
        if kind == FrameKind::Terminal {
            return Err("the long request's stream ended before the caller could drop it".into());
        }
    }
    drop(s);
    let stats = r.stats();
    if !eventually(FRAME_BOUND, || ServerStats::get(&stats.in_flight) == 0).await {
        return Err(format!("the handler still runs {FRAME_BOUND:?} after its caller dropped the stream"));
    }
    r.owes_nothing("a caller that dropped the stream").await
}

/// The consumer reads the long stream to the bound without acknowledging; every connection the
/// server holds is cut, the frames it owes are resent on a new stream, and once acknowledged the
/// stream goes on to its terminal: every position handed over once, in order, the handler run once.
async fn stream_reply_lost<P>(f: &Streaming<P>) -> Result<(), String>
where
    P: StreamingProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let bound = MAX_REORDER_POSITIONS as usize;
    let r = Rig::new(&f.serve, P::OP, false).await?;
    let mut s = open::<P>(&r, &f.long, &options()).await?;
    let mut seen = Vec::new();
    for i in 0..bound {
        let (kind, _) = frame(&mut s).await.map_err(|why| format!("frame {i}: {why}"))?;
        seen.push((kind, s.position().map(|(id, seq)| (id.clone(), seq))));
    }
    let d = r.server.delivery().clone();
    if !eventually(FRAME_BOUND, || d.owed_frames() == bound).await {
        return Err(format!("{} frames owed before the cut, expected {bound}", d.owed_frames()));
    }
    if r.server.close_connections_except(&[], "conformance: the stream's frames are lost after acceptance") == 0 {
        return Err("no connection to cut".into());
    }
    if !eventually(FRAME_BOUND, || DeliveryStats::get(&d.stats().responses_resent) >= 1).await {
        return Err(format!("no owed frame was resent within {FRAME_BOUND:?} of the cut"));
    }
    s.accepted().await;
    read_to_terminal(&mut s, &mut seen).await.map_err(|why| format!("after the cut: {why}"))?;
    in_order(&seen)?;
    r.dispatched_exactly(1)?;
    r.acked_every(seen.len(), "the stream read to its terminal after the cut").await?;
    r.owes_nothing("the stream read to its terminal after the cut").await
}

/// The protocol's consumer reads the short stream with a [`StreamRecord`]: no frame is
/// acknowledged before it is recorded, the terminal among them, and every recorded frame is
/// acknowledged.
async fn accepted_after_record<P>(f: &Streaming<P>) -> Result<(), String>
where
    P: StreamingProtocol,
    P::Request: Clone + Debug + 'static,
    P::Reply: Debug,
{
    let r = Rig::new(&f.serve, P::OP, false).await?;
    let s = open::<P>(&r, &f.answered, &options()).await?;
    let record = StreamRecord::new(r.client.delivery().clone());
    tokio::time::timeout(CHECK_BOUND / 2, (f.consume)(s, record.clone())).await.map_err(|_| format!("the consumer did not finish within {:?}", CHECK_BOUND / 2))?;
    let (frames, early) = {
        let g = record.inner.lock().unwrap();
        (g.frames.clone(), g.early.clone())
    };
    if !early.is_empty() {
        return Err(early.join("; "));
    }
    if !frames.iter().any(|(_, k)| *k == FrameKind::Terminal) {
        return Err(format!("the consumer never recorded the terminal: {frames:?}"));
    }
    let acked = DeliveryStats::get(&r.client.delivery().stats().acks_sent);
    if acked != frames.len() as u64 {
        return Err(format!("{acked} frames acknowledged and {} recorded: each recorded frame is acknowledged, none before it is recorded", frames.len()));
    }
    r.acked_every(frames.len(), "the consumer's recorded frames").await?;
    r.owes_nothing("the consumer's recorded frames").await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: the discriminant the unknown-variant check writes is postcard's varint.
    #[test]
    fn the_unknown_variant_discriminant_is_postcards_varint() {
        assert_eq!(varint(9), vec![9]);
        assert_eq!(varint(127), vec![0x7f]);
        assert_eq!(varint(128), vec![0x80, 0x01]);
        assert_eq!(varint(300), vec![0xac, 0x02]);
    }
}
