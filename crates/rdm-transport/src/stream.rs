//! Server streaming (node-rpc.md §11.2, §31, §34; i143.e6.s3).
//!
//! Server: a streaming handler never touches the transport. It gets one typed, non-cloneable
//! [`ReplySink`] in the `NotStarted` state: `started(frame)` moves it to `Streaming`, and
//! `data(frame)` writes one frame. There is no `terminal()`: the handler returns the terminal frame
//! (or, before `Started`, a refusal) and Node RPC writes it after the handler is joined. Each frame
//! is encoded once, checked against the protocol's reply limit, and written as a response frame at
//! its position (`Started` 0, each `Data` the next, `Terminal` the last), waiting on QUIC flow
//! control: a slow consumer slows the producer. Every frame is owed until the caller acks it: one
//! written while the stream is gone is resent on a new stream. When the caller dropped the
//! invocation, `data()` answers `CallerGone`.
//!
//! Client: [`NodeRpcClient::call_stream`] returns a [`ReplyStream`] fed by its invocation's frames,
//! from whatever stream they arrive on, in position order; it enforces the frame order with the
//! protocol's classifier. A stream that ends after `Started` without a `Terminal` is
//! `Indeterminate`. Dropping the stream ends the invocation: the server's next write sees the caller
//! gone.

use crate::client::{AttemptEnd, Attempted, Budget, CallEvidence, CallOptions, NodeRpcClient, OwedCall, Payload};
use crate::delivery::{AckOnRecovery, FrameOut, Settled, SlotEvent, Written};
use crate::resolve::NodeTarget;
use crate::server::{Accepting, BoxFut, Erased, HandlerFault, PeerContext, Refusal, ServerBuilder, ServerStats};
use rdm_core::catalog::{CatalogEntry, Shape, OpOwner};
use rdm_core::codes::ResetCode;
use rdm_core::delivery::MAX_REORDER_POSITIONS;
use rdm_core::outcome::{IndeterminateReason, NotSentReason, PreCommit, ResolveFailure, RpcOutcome};
use rdm_core::streaming::{FrameKind, FrameOrder, OrderState, StreamingProtocol};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::Instant;

/// Why a frame could not be written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    /// The caller stopped the reply direction or the connection is gone.
    CallerGone(String),
    /// The encoded frame exceeds the protocol's reply-frame limit.
    /// The encoded frame is larger than the limit.
    TooLarge {
        /// The encoded size in bytes.
        size: usize,
        /// The protocol's reply-frame limit in bytes.
        max: usize,
    },
    /// The frame breaks the stream order.
    Order(String),
}

struct OutInner {
    out: FrameOut,
    order: FrameOrder,
    max: usize,
    frames: u64,
    bytes: u64,
    caller_gone: Option<String>,
    gate: Accepting,
}

/// The server side of one stream: the reply direction, its order and its owed frames. Shared by the
/// sink and the runtime, which writes the terminal frame.
#[derive(Clone)]
pub(crate) struct StreamOut(Arc<tokio::sync::Mutex<OutInner>>);

impl StreamOut {
    pub(crate) fn new(out: FrameOut, max: usize, gate: Accepting) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(OutInner { out, order: FrameOrder::new(), max, frames: 0, bytes: 0, caller_gone: None, gate })))
    }

    /// Owe the frame at the next position and write it. A write that fails because the stream is
    /// gone leaves the frame owed, resent on a new stream; one the caller stopped means it dropped
    /// the invocation.
    async fn put(g: &mut OutInner, payload: Vec<u8>, terminal: bool) -> Result<(), SinkError> {
        g.gate.implicit();
        let (d, id) = g.gate.owed();
        let (d, id) = (d.clone(), id.clone());
        let seq = u32::try_from(g.frames).map_err(|_| SinkError::Order("a stream past its last position".into()))?;
        let Some(frame) = d.owe_frame(&id, seq, payload, terminal) else {
            let reason = "the caller ended the invocation".to_string();
            g.caller_gone = Some(reason.clone());
            return Err(SinkError::CallerGone(reason));
        };
        match g.out.send(&frame).await {
            Written::Ok => {}
            Written::Stopped(0) => {
                d.drop_response(&id);
                let reason = "the caller dropped the invocation".to_string();
                g.caller_gone = Some(reason.clone());
                return Err(SinkError::CallerGone(reason));
            }
            w @ (Written::Stopped(_) | Written::Gone(_)) => d.push(&id, None, &format!("a stream frame's write {w:?}")),
        }
        g.frames += 1;
        g.bytes += frame.len() as u64;
        Ok(())
    }

    async fn write(&self, kind: FrameKind, payload: Vec<u8>) -> Result<(), SinkError> {
        let mut g = self.0.lock().await;
        if let Some(reason) = &g.caller_gone {
            return Err(SinkError::CallerGone(reason.clone()));
        }
        if payload.len() > g.max {
            return Err(SinkError::TooLarge { size: payload.len(), max: g.max });
        }
        // At most 64 frames unacked: the caller acks a frame once its owner can recover it, so a
        // consumer that has not recovered them holds the producer here. A response owed no longer (its caller's birth ended, or it dropped
        // the invocation) ends the wait.
        loop {
            let (d, id) = g.gate.owed();
            if d.owed_frames_of(id) < MAX_REORDER_POSITIONS as usize {
                break;
            }
            let Some(mut acked) = d.acked_watch(id) else {
                let reason = "the response is owed no longer".to_string();
                g.caller_gone = Some(reason.clone());
                return Err(SinkError::CallerGone(reason));
            };
            if acked.changed().await.is_err() {
                let reason = "the response is owed no longer".to_string();
                g.caller_gone = Some(reason.clone());
                return Err(SinkError::CallerGone(reason));
            }
        }
        g.order.admit(kind).map_err(SinkError::Order)?;
        Self::put(&mut g, payload, false).await
    }

    /// Write the handler's result: its terminal (or pre-`Started` refusal) frame, then finish.
    pub(crate) async fn finish(&self, joined: Result<StreamEnd, tokio::task::JoinError>, stats: &Arc<ServerStats>) {
        let mut g = self.0.lock().await;
        let (frames, bytes, gone) = (g.frames, g.bytes, g.caller_gone.clone());
        let (d, id) = g.gate.owed();
        let (d, id) = (d.clone(), id.clone());
        let ended = |c: ResetCode| {
            tracing::Span::current().record("code", "INTERNAL");
            tracing::Span::current().record("detail", if c == ResetCode::ProtocolViolation { "protocol-violation" } else { "handler-fault" });
            d.settle_request(&id, Settled::Ended(u64::from(c.code())));
            d.drop_response(&id);
        };
        match joined {
            Ok(StreamEnd::Final { kind, payload, .. }) if payload.len() > g.max => {
                stats.violations.fetch_add(1, Ordering::SeqCst);
                g.out.reset(ResetCode::ProtocolViolation).await;
                ended(ResetCode::ProtocolViolation);
                tracing::info_span!("rdm.node_rpc.stream.reject.via-protocol-violation", code = "INTERNAL", detail = "protocol-violation", reason = "final frame over the reply limit", ?kind)
                    .in_scope(|| tracing::info!("424"));
            }
            Ok(StreamEnd::Final { kind, payload, code, detail }) => match final_frame(&mut g.order, kind) {
                Ok(()) => {
                    tracing::Span::current().record("code", code.name());
                    tracing::Span::current().record("detail", detail);
                    let _ = Self::put(&mut g, payload, true).await;
                    g.out.finish().await;
                    tracing::info_span!(
                        "rdm.node_rpc.stream.serve.via-terminal",
                        code = code.name(),
                        detail,
                        frames = frames + 1,
                        bytes,
                        caller_cancelled = gone.is_some(),
                        cancellation_reason = gone.as_deref().unwrap_or("")
                    )
                    .in_scope(|| tracing::info!("stream ended"));
                }
                Err(reason) => {
                    stats.violations.fetch_add(1, Ordering::SeqCst);
                    g.out.reset(ResetCode::ProtocolViolation).await;
                    ended(ResetCode::ProtocolViolation);
                    tracing::info_span!("rdm.node_rpc.stream.reject.via-protocol-violation", code = "INTERNAL", detail = "protocol-violation", reason = %reason)
                        .in_scope(|| tracing::info!("424"));
                }
            },
            Ok(StreamEnd::Fault(fault)) => {
                stats.faults.fetch_add(1, Ordering::SeqCst);
                g.out.reset(ResetCode::InternalRpcFailure).await;
                ended(ResetCode::InternalRpcFailure);
                tracing::info_span!("rdm.node_rpc.handler.reject.via-internal-rpc-failure", code = "INTERNAL", detail = "handler-fault", fault = fault.reason())
                    .in_scope(|| tracing::info!("423"));
            }
            Err(_panic) => {
                stats.faults.fetch_add(1, Ordering::SeqCst);
                g.out.reset(ResetCode::InternalRpcFailure).await;
                ended(ResetCode::InternalRpcFailure);
                tracing::info_span!("rdm.node_rpc.handler.reject.via-internal-rpc-failure", code = "INTERNAL", detail = "handler-fault", fault = "panic")
                    .in_scope(|| tracing::info!("423"));
            }
        }
    }
}

/// The handler's returned frame must end the stream: a `Terminal` after `Started`, or a refusal
/// before it. A `Data` frame is legal mid-stream but never as the end.
fn final_frame(order: &mut FrameOrder, kind: FrameKind) -> Result<(), String> {
    match (order.state(), kind) {
        (OrderState::NotStarted, FrameKind::Refusal(_)) | (OrderState::Streaming, FrameKind::Terminal) => order.admit(kind),
        (state, kind) => Err(format!("the handler ended a {state:?} stream with {kind:?}, not its terminal")),
    }
}

/// How a streaming handler ended.
pub(crate) enum StreamEnd {
    /// Its returned frame (terminal, or a refusal before `Started`), classified.
    Final { kind: FrameKind, payload: Vec<u8>, code: rdm_core::outcome::Code, detail: &'static str },
    Fault(HandlerFault),
}

/// `ReplySink` states.
pub struct NotStarted;
/// The state of a sink that has written its first frame.
pub struct Streaming;

/// The one typed sink a streaming handler writes through.
pub struct ReplySink<P, S> {
    out: StreamOut,
    _p: PhantomData<(fn() -> P, S)>,
}

impl<P: StreamingProtocol> ReplySink<P, NotStarted> {
    /// Write the `Started` frame.
    pub async fn started(self, frame: P::Reply) -> Result<ReplySink<P, Streaming>, SinkError> {
        write_frame::<P>(&self.out, &frame).await?;
        Ok(ReplySink { out: self.out, _p: PhantomData })
    }
}

impl<P: StreamingProtocol> ReplySink<P, Streaming> {
    /// Write one `Data` frame, waiting on flow control.
    pub async fn data(&mut self, frame: P::Reply) -> Result<(), SinkError> {
        write_frame::<P>(&self.out, &frame).await
    }
}

async fn write_frame<P: StreamingProtocol>(out: &StreamOut, frame: &P::Reply) -> Result<(), SinkError> {
    let payload = P::encode_reply(frame).map_err(|e| SinkError::Order(format!("frame does not encode: {}", e.0)))?;
    let code = P::classify_reply(frame).code();
    let detail = P::reply_detail(frame);
    tracing::info_span!("rdm.node_rpc.stream.update.via-frame", protocol = P::NAME, code = code.name(), detail)
        .in_scope(|| tracing::debug!("the typed frame was classified"));
    out.write(P::frame_kind(frame), payload).await
}

struct TypedStream<P, F> {
    f: Arc<F>,
    _p: PhantomData<fn() -> P>,
}

impl<P, F, Fut> Erased for TypedStream<P, F>
where
    P: StreamingProtocol,
    F: Fn(PeerContext, P::Request, ReplySink<P, NotStarted>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<P::Reply, HandlerFault>> + Send + 'static,
{
    fn call(&self, _peer: PeerContext, _payload: Vec<u8>) -> (tracing::Span, BoxFut<Result<Vec<u8>, HandlerFault>>) {
        (tracing::Span::none(), Box::pin(async { Err(HandlerFault::invariant_broken("a streaming protocol is served through its stream path")) }))
    }

    fn refusal(&self, r: Refusal) -> Vec<u8> {
        let reply = match r {
            Refusal::NotReady(s) => P::not_ready(s),
            Refusal::Busy(s) => P::busy(s),
            Refusal::ConnectionsDeleting(s) => P::connections_deleting(s),
            Refusal::Malformed(k) => P::malformed(k),
        };
        P::encode_reply(&reply).unwrap_or_default()
    }

    fn max_reply(&self) -> usize {
        P::MAX_REPLY_FRAME_BYTES
    }

    fn read_only(&self, payload: &[u8]) -> bool {
        P::decode_request(payload).is_ok_and(|r| P::read_only(&r))
    }

    fn is_stream(&self) -> bool {
        true
    }

    fn stream(&self, peer: PeerContext, payload: Vec<u8>, out: StreamOut) -> Option<(tracing::Span, BoxFut<StreamEnd>)> {
        let f = self.f.clone();
        let req = match P::decode_request(&payload) {
            Ok(r) => r,
            Err(d) => {
                let refusal = P::malformed(match d {
                    rdm_core::protocol::DecodeFailure::UnknownVariant => rdm_core::outcome::MalformedKind::UnknownVariant,
                    rdm_core::protocol::DecodeFailure::Corrupt => rdm_core::outcome::MalformedKind::Corrupt,
                });
                let end = StreamEnd::Final { kind: P::frame_kind(&refusal), payload: P::encode_reply(&refusal).unwrap_or_default(), code: P::classify_reply(&refusal).code(), detail: P::reply_detail(&refusal) };
                return Some((tracing::Span::none(), Box::pin(async move { end })));
            }
        };
        let span = tracing::info_span!(
            "rdm.node_rpc.stream.serve.via-direct",
            code = tracing::field::Empty,
            detail = tracing::field::Empty,
            protocol = P::NAME,
            op = P::OP,
            peer = %peer.endpoint_id,
            caller_system = tracing::field::Empty,
            context_dropped = tracing::field::Empty,
            test_case = tracing::field::Empty,
            scenario = tracing::field::Empty,
            operation = tracing::field::Empty,
        );
        crate::server::apply_context(&span, &peer);
        let serve = span.clone();
        let handler = Box::pin(async move {
            let sink = ReplySink::<P, NotStarted> { out, _p: PhantomData };
            match tracing::Instrument::instrument(f(peer, req, sink), serve).await {
                Ok(last) => StreamEnd::Final { kind: P::frame_kind(&last), payload: P::encode_reply(&last).unwrap_or_default(), code: P::classify_reply(&last).code(), detail: P::reply_detail(&last) },
                Err(fault) => StreamEnd::Fault(fault),
            }
        });
        Some((span, handler))
    }
}

impl ServerBuilder {
    /// Serve server-streaming protocol `P` (owned by `owner`) with handler `f`.
    pub fn serve_stream<P, F, Fut>(mut self, owner: OpOwner, f: F) -> Self
    where
        P: StreamingProtocol,
        F: Fn(PeerContext, P::Request, ReplySink<P, NotStarted>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<P::Reply, HandlerFault>> + Send + 'static,
    {
        self.catalog = self.catalog.serve(CatalogEntry::canonical::<P>(owner, Shape::ServerStreaming));
        self.handlers.insert(P::OP, Arc::new(TypedStream::<P, F> { f: Arc::new(f), _p: PhantomData }));
        self
    }
}

/// Why a stream did not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamFailure {
    /// The request committed; the stream ended, reset or ran out of time before its terminal.
    Indeterminate(IndeterminateReason),
    /// A frame broke the stream order or did not decode.
    Violation(String),
}

impl StreamFailure {
    /// Standard code from the typed stream ending.
    pub fn code(&self) -> rdm_core::outcome::Code {
        match self { Self::Indeterminate(reason) => reason.code(), Self::Violation(_) => rdm_core::outcome::Code::Internal }
    }
}

/// One received item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamItem<R> {
    /// A frame of the stream: its kind and its decoded body.
    Frame(FrameKind, R),
    /// The stream ended in a failure.
    Failed(StreamFailure),
}

/// The caller side of one committed stream: its invocation's frames, in position order, from
/// whatever stream they arrive on. Dropped, the invocation ends.
///
/// A frame [`Self::next`] hands over is acked once its owner can recover it (node-rpc.md §31, §43):
/// the owner says so with [`Self::accepted`] after it recorded the frames, or declares at the start
/// that it records nothing of them ([`Self::read_only`]), when reading a frame is its recovery.
/// Until then the responder owes the frame: it holds the producer (at most 64 frames unacked), and a
/// reconnect has it resent, never handed to the owner twice.
pub struct ReplyStream<P: StreamingProtocol> {
    span: tracing::Span,
    started: Instant,
    owed: OwedCall,
    order: FrameOrder,
    deadline: Option<Instant>,
    done: bool,
    /// The acks of the frames handed over and not yet recovered by the owner, in order.
    unrecovered: Vec<AckOnRecovery>,
    /// The authoritative transport position of the last frame handed to this owner.
    position: Option<(rdm_core::framing::DeliveryId, u32)>,
    /// The terminal was handed over: the invocation completes once the owner recovers it.
    terminal_handed: bool,
    read_only: bool,
    /// The path the attempt task ended on, once it said (i146 Q38): named if the stream's attempts end
    /// before its terminal.
    attempt_end: Option<AttemptEnd>,
    _p: PhantomData<fn() -> P>,
}

impl<P: StreamingProtocol> ReplyStream<P> {
    /// The invocation and sequence from the last handed-over frame's transport header. Owners
    /// record this identity with progress before calling `accepted`; no protocol body supplies it.
    pub fn position(&self) -> Option<(&rdm_core::framing::DeliveryId, u32)> {
        self.position.as_ref().map(|(invocation, seq)| (invocation, *seq))
    }

    /// The next frame in order, a failure, or `None` once the stream has ended.
    pub async fn next(&mut self) -> Option<StreamItem<P::Reply>> {
        if self.done {
            return None;
        }
        loop {
            let wait = async {
                match self.deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                // A frame already delivered is taken before anything the attempt task said after it.
                biased;
                f = self.owed.frames.recv() => match f {
                    Some(SlotEvent::Frame(bytes, ack)) => {
                        let frame = match P::decode_reply(&bytes) {
                            Ok(f) => f,
                            Err(e) => return Some(self.fail(StreamFailure::Violation(format!("a frame does not decode: {e:?}")))),
                        };
                        self.position = ack.as_ref().map(|a| {
                            let (invocation, seq) = a.position();
                            (invocation.clone(), seq)
                        });
                        let (invocation_id, seq) = self.position.as_ref().map(|(id, seq)| (id.to_string(), *seq)).expect("stream response carries its transport position");
                        let kind = P::frame_kind(&frame);
                        let code = P::classify_reply(&frame).code();
                        let detail = P::reply_detail(&frame);
                        self.span.in_scope(|| tracing::info_span!("rdm.node_rpc.stream.update.via-received-frame", protocol = P::NAME, invocation_id, seq, code = code.name(), detail).in_scope(|| {}));
                        if let Err(reason) = self.order.admit(kind) {
                            return Some(self.fail(StreamFailure::Violation(reason)));
                        }
                        // Handed over, not yet recovered: its ack waits for the owner.
                        if let Some(ack) = ack {
                            self.unrecovered.push(ack);
                        }
                        if self.order.state() == OrderState::Done {
                            self.done = true;
                            self.terminal_handed = true;
                            self.span.record("code", code.name()).record("detail", detail).record("elapsed_ms", self.started.elapsed().as_millis() as u64);
                        }
                        if self.read_only {
                            self.accepted().await;
                        }
                        return Some(StreamItem::Frame(kind, frame));
                    }
                    Some(SlotEvent::Violation(why)) => return Some(self.fail(StreamFailure::Violation(why))),
                    None => return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::ReplyLost("the stream ended before its terminal".into())))),
                },
                ev = self.owed.attempted.recv() => match ev {
                    Some(Attempted::Reset(c)) => return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::Reset(c)))),
                    Some(Attempted::ReceiverGone(why)) => return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::ReceiverGone(why)))),
                    Some(Attempted::TransportLost(why)) => return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::ReplyLost(why)))),
                    Some(Attempted::EndedPreCommit(_)) => {
                        return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::ReplyLost("the stream ended before its terminal".into()))));
                    }
                    Some(Attempted::Ended(path)) => self.attempt_end = Some(path),
                    None => {
                        let path = self.attempt_end.map_or("no path reported: the attempt task ended without naming one", AttemptEnd::name);
                        return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::ReplyLost(format!("the stream ended before its terminal: its attempts ended ({path})")))));
                    }
                    Some(_) => {}
                },
                () = wait => return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::ReplyDeadline))),
            }
        }
    }

    /// The owner can recover every frame handed over so far (it recorded them): their acks are
    /// written in order, and once the terminal is among them the invocation is complete.
    pub async fn accepted(&mut self) {
        for ack in self.unrecovered.drain(..) {
            ack.write().await;
        }
        if std::mem::take(&mut self.terminal_handed) {
            self.owed.guard.complete();
            // The attempt task ends once the invocation is complete: nothing of the call outlives
            // its terminal.
            self.owed.close(false).await;
        }
    }

    /// The owner records nothing of the frames (a read-only consumer, node-rpc.md §43): reading a
    /// frame is its recovery, and each is acked as it is handed over.
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    fn fail(&mut self, f: StreamFailure) -> StreamItem<P::Reply> {
        self.done = true;
        let (code, detail) = match &f {
            StreamFailure::Indeterminate(reason) => (reason.code(), reason.detail_name()),
            StreamFailure::Violation(_) => (rdm_core::outcome::Code::Internal, "protocol-violation"),
        };
        self.span.record("code", code.name()).record("detail", detail).record("elapsed_ms", self.started.elapsed().as_millis() as u64);
        // The owner has its end: the invocation is owed no longer.
        self.owed.guard.complete();
        if let Some(h) = &self.owed._task.0 {
            h.abort();
        }
        StreamItem::Failed(f)
    }
}

impl NodeRpcClient {
    /// Invoke streaming protocol `P` on `target`. `Err` carries a pre-commit or early-refusal
    /// outcome; `Ok` the committed stream.
    pub async fn call_stream<P: StreamingProtocol>(
        &self,
        target: &NodeTarget,
        req: &P::Request,
        opts: &CallOptions,
    ) -> Result<(ReplyStream<P>, CallEvidence), (RpcOutcome<P::Reply>, Option<CallEvidence>)> {
        let payload = match P::encode_request(req) {
            Ok(p) => p,
            Err(e) => return Err((PreCommit::begin(P::OP).not_sent(NotSentReason::Connection(format!("request does not encode: {}", e.0))), None)),
        };
        // The caller's side of the call that opens the stream: its outcome and how long the open took.
        let span = tracing::info_span!(
            "rdm.node_rpc.request.update.via-call",
            target = ?target,
            op = P::OP,
            outcome = tracing::field::Empty,
            code = tracing::field::Empty,
            detail = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
            delivery_id = tracing::field::Empty,
            attempts = tracing::field::Empty,
            delivery = tracing::field::Empty,
        );
        let started = Instant::now();
        let start = started;
        let (send_deadline, overall) = crate::client::deadlines_of(opts.budget, start);
        let mut opening = crate::client::CallSpanGuard::new(&span);
        let opened = self.owe(target, None, P::OP, Payload::Ready(payload), P::MAX_REPLY_FRAME_BYTES, opts, false, (send_deadline, overall), &span);
        let finish = |outcome: &str| {
            span.record("outcome", outcome);
            span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        };
        let mut owed = match opened {
            Ok(o) => o,
            Err((out, ev)) => {
                opening.complete();
                finish(out.name());
                span.record("code", out.code().name());
                span.record("detail", out.detail_name());
                span.in_scope(|| tracing::info!("node rpc stream call finished"));
                return Err((out.retyped().expect("no reply before anything is owed"), ev));
            }
        };
        let mut last = NotSentReason::Deadline;
        let mut pre = Some(PreCommit::begin(P::OP));
        let ended: RpcOutcome<P::Reply> = loop {
            tokio::select! {
                ev = owed.attempted.recv() => match ev {
                    Some(Attempted::Committed { .. }) => {
                        opening.complete();
                        finish("stream-open");
                        span.in_scope(|| tracing::info!("node rpc stream call opened"));
                        let deadline = match (opts.budget, overall) {
                            (_, Some(d)) => Some(d),
                            (Budget::Split { reply, .. }, None) => Some(Instant::now() + reply),
                            _ => None,
                        };
                        let evidence = owed.evidence.lock().unwrap().clone();
                        return Ok((ReplyStream { span: span.clone(), started, owed, order: FrameOrder::new(), deadline, done: false, unrecovered: Vec::new(), position: None, terminal_handed: false, read_only: false, attempt_end: None, _p: PhantomData }, evidence));
                    }
                    Some(Attempted::NotSent(r)) => last = r,
                    Some(Attempted::SendDeadline(r)) => {
                        span.record("delivery", "cancelled");
                        break pre.take().expect("pre-commit").not_sent(r);
                    }
                    Some(Attempted::StoppedBeforeComplete) => {
                        // Refused before the request was complete: the refusal is the stream's one frame.
                        let early = pre.take().expect("stopped once").stopped_before_finish();
                        let frame = match send_deadline {
                            Some(d) => tokio::time::timeout_at(d, owed.frames.recv()).await.ok().flatten(),
                            None => owed.frames.recv().await,
                        };
                        let bytes = match frame {
                            Some(SlotEvent::Frame(b, ack)) => {
                                if let Some(ack) = ack {
                                    ack.write().await;
                                }
                                Some(b)
                            }
                            _ => None,
                        };
                        break early.reply::<P>(bytes.as_deref());
                    }
                    Some(Attempted::EndedPreCommit(o)) => break o.retyped().expect("no reply before the commit cut"),
                    Some(Attempted::Reset(c)) => break pre.take().expect("pre-commit").not_sent(NotSentReason::Connection(format!("reset {c} before the commit cut"))),
                    Some(Attempted::ReceiverGone(_)) => break pre.take().expect("pre-commit").not_sent(NotSentReason::Resolve(ResolveFailure::Gone)),
                    Some(Attempted::TransportLost(why)) => break pre.take().expect("pre-commit").not_sent(NotSentReason::Connection(why)),
                    // The attempt task's last word: what ended it was said above, or its end follows.
                    Some(Attempted::Ended(_)) => {}
                    None => break pre.take().expect("pre-commit").not_sent(last.clone()),
                },
            }
        };
        opening.complete();
        finish(ended.name());
        span.record("code", ended.code().name());
        span.record("detail", ended.detail_name());
        span.in_scope(|| tracing::info!("node rpc stream call finished"));
        let evidence = owed.evidence.lock().unwrap().clone();
        owed.close(true).await;
        Err((ended, Some(evidence)))
    }
}

impl<P: StreamingProtocol> Drop for ReplyStream<P> {
    fn drop(&mut self) {
        if !self.done {
            self.span.record("code", "CANCELLED").record("detail", "caller-gone").record("elapsed_ms", self.started.elapsed().as_millis() as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::node_command::{NodeCommand, NodeCommandReply};
    use rdm_core::protocol::NodeProtocol;

    #[tokio::test]
    async fn a_frame_handed_without_its_position_ends_the_stream_by_name() {
        let birth = rdm_core::framing::Birth { node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() };
        let delivery = crate::Delivery::new(birth.clone());
        let id = delivery.mint(&[]);
        let (guard, _, _) = delivery.open_call(id, NodeCommand::MAX_REPLY_FRAME_BYTES, false);
        let (tx, frames) = tokio::sync::mpsc::unbounded_channel();
        let (_attempts_tx, attempted) = tokio::sync::mpsc::unbounded_channel();
        let owed = OwedCall {
            guard, frames, attempted,
            evidence: Arc::new(std::sync::Mutex::new(CallEvidence { node_id: None, addr: "127.0.0.1:0".parse().unwrap(), committed: true, connection: None, reused: false, dial_failure: None })),
            attempts: Arc::new(std::sync::atomic::AtomicU32::new(1)),
            _task: crate::client::AbortOnDrop(None),
        };
        let mut stream = ReplyStream::<NodeCommand> { span: tracing::Span::none(), started: Instant::now(), owed, order: FrameOrder::new(), deadline: None, done: false, unrecovered: Vec::new(), position: None, terminal_handed: false, read_only: false, attempt_end: None, _p: PhantomData };
        let frame = NodeCommandReply::CommandAccepted { node_id: birth.node_id, incarnation: birth.incarnation };
        tx.send(SlotEvent::Frame(NodeCommand::encode_reply(&frame).unwrap(), None)).unwrap();
        assert!(matches!(stream.next().await, Some(StreamItem::Failed(StreamFailure::Violation(reason))) if reason == "a response frame carries no transport position"));
        assert!(stream.next().await.is_none(), "nothing follows the named violation");
        assert!(stream.position().is_none());
    }
}
