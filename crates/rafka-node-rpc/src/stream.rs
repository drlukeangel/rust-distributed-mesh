//! Server streaming (node-rpc.md §11.2, §31, §34; i143.e6.s3).
//!
//! Server: a streaming handler never touches the transport. It gets one typed, non-cloneable
//! [`ReplySink`] in the `NotStarted` state: `started(frame)` moves it to `Streaming`, and
//! `data(frame)` writes one frame. There is no `terminal()`: the handler returns the terminal frame
//! (or, before `Started`, a refusal) and Node RPC writes it after the handler is joined. Each frame
//! is encoded once, checked against the protocol's reply limit, and written straight to the QUIC
//! stream, waiting on its flow control: no Node RPC queue, so a slow consumer slows the producer.
//! When the caller is gone, `data()` answers `CallerGone`.
//!
//! Client: [`NodeRpcClient::call_stream`] returns a [`ReplyStream`] that enforces the frame order
//! with the protocol's classifier. A stream that ends or resets after `Started` without a
//! `Terminal` is `Indeterminate`. Dropping the stream stops the reply direction; the server's next
//! write sees the caller gone.

use crate::client::{CallEvidence, CallOptions, NodeRpcClient, Opened, Phase};
use crate::resolve::NodeTarget;
use crate::server::{BoxFut, Erased, HandlerFault, PeerContext, Refusal, ServerBuilder, ServerStats};
use iroh::endpoint::{ReadError, RecvStream, SendStream, VarInt};
use rafka_node_rpc_contract::catalog::{CatalogEntry, Shape, OpOwner};
use rafka_node_rpc_contract::codes::ResetCode;
use rafka_node_rpc_contract::framing::{decode_frame, encode_frame, FrameError};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, NotSentReason, PreCommit, RpcOutcome};
use rafka_node_rpc_contract::streaming::{FrameKind, FrameOrder, OrderState, StreamingProtocol};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::{timeout_at, Instant};

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
    send: Option<SendStream>,
    order: FrameOrder,
    max: usize,
    frames: u64,
    bytes: u64,
    caller_gone: Option<String>,
}

/// The server side of one stream: the reply direction and its order. Shared by the sink and the
/// runtime, which takes the stream back to write the terminal frame.
#[derive(Clone)]
pub(crate) struct StreamOut(Arc<tokio::sync::Mutex<OutInner>>);

impl StreamOut {
    pub(crate) fn new(send: SendStream, max: usize) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(OutInner {
            send: Some(send),
            order: FrameOrder::new(),
            max,
            frames: 0,
            bytes: 0,
            caller_gone: None,
        })))
    }

    pub(crate) async fn take(&self) -> Option<SendStream> {
        self.0.lock().await.send.take()
    }

    async fn write(&self, kind: FrameKind, payload: Vec<u8>) -> Result<(), SinkError> {
        let mut g = self.0.lock().await;
        if let Some(reason) = &g.caller_gone {
            return Err(SinkError::CallerGone(reason.clone()));
        }
        if payload.len() > g.max {
            return Err(SinkError::TooLarge { size: payload.len(), max: g.max });
        }
        g.order.admit(kind).map_err(SinkError::Order)?;
        let frame = encode_frame(&payload);
        let Some(send) = g.send.as_mut() else {
            return Err(SinkError::CallerGone("the reply direction is closed".into()));
        };
        // Waits on QUIC flow control: a slow consumer holds the producer here.
        if let Err(e) = send.write_all(&frame).await {
            let reason = e.to_string();
            g.caller_gone = Some(reason.clone());
            return Err(SinkError::CallerGone(reason));
        }
        g.frames += 1;
        g.bytes += frame.len() as u64;
        Ok(())
    }

    /// Write the handler's result: its terminal (or pre-`Started` refusal) frame, then finish.
    pub(crate) async fn finish(&self, joined: Result<StreamEnd, tokio::task::JoinError>, stats: &Arc<ServerStats>) {
        let mut g = self.0.lock().await;
        let (frames, bytes, gone) = (g.frames, g.bytes, g.caller_gone.clone());
        let Some(mut send) = g.send.take() else { return };
        let code = |c: ResetCode| VarInt::from_u32(c.code());
        match joined {
            Ok(StreamEnd::Final { kind, payload }) if payload.len() > g.max => {
                stats.violations.fetch_add(1, Ordering::SeqCst);
                let _ = send.reset(code(ResetCode::ProtocolViolation));
                tracing::info_span!("rdm.node_rpc.stream.reject.via-protocol-violation", reason = "final frame over the reply limit", ?kind)
                    .in_scope(|| tracing::info!("424"));
            }
            Ok(StreamEnd::Final { kind, payload }) => match final_frame(&mut g.order, kind) {
                Ok(()) => {
                    let _ = send.write_all(&encode_frame(&payload)).await;
                    let _ = send.finish();
                    tracing::info_span!(
                        "rdm.node_rpc.stream.serve.via-terminal",
                        frames = frames + 1,
                        bytes,
                        caller_cancelled = gone.is_some(),
                        cancellation_reason = gone.as_deref().unwrap_or("")
                    )
                    .in_scope(|| tracing::info!("stream ended"));
                }
                Err(reason) => {
                    stats.violations.fetch_add(1, Ordering::SeqCst);
                    let _ = send.reset(code(ResetCode::ProtocolViolation));
                    tracing::info_span!("rdm.node_rpc.stream.reject.via-protocol-violation", reason = %reason)
                        .in_scope(|| tracing::info!("424"));
                }
            },
            Ok(StreamEnd::Fault(fault)) => {
                stats.faults.fetch_add(1, Ordering::SeqCst);
                let _ = send.reset(code(ResetCode::InternalRpcFailure));
                tracing::info_span!("rdm.node_rpc.handler.reject.via-internal-rpc-failure", fault = fault.reason())
                    .in_scope(|| tracing::info!("423"));
            }
            Err(_panic) => {
                stats.faults.fetch_add(1, Ordering::SeqCst);
                let _ = send.reset(code(ResetCode::InternalRpcFailure));
                tracing::info_span!("rdm.node_rpc.handler.reject.via-internal-rpc-failure", fault = "panic")
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
    Final { kind: FrameKind, payload: Vec<u8> },
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
    fn call(&self, _peer: PeerContext, _payload: Vec<u8>) -> BoxFut<Result<Vec<u8>, HandlerFault>> {
        Box::pin(async { Err(HandlerFault::invariant_broken("a streaming protocol is served through its stream path")) })
    }

    fn refusal(&self, r: Refusal) -> Vec<u8> {
        let reply = match r {
            Refusal::NotReady(s) => P::not_ready(s),
            Refusal::Busy(s) => P::busy(s),
            Refusal::Draining(s) => P::draining(s),
            Refusal::Malformed(k) => P::malformed(k),
        };
        P::encode_reply(&reply).unwrap_or_default()
    }

    fn max_reply(&self) -> usize {
        P::MAX_REPLY_FRAME_BYTES
    }

    fn is_stream(&self) -> bool {
        true
    }

    fn stream(&self, peer: PeerContext, payload: Vec<u8>, out: StreamOut) -> Option<BoxFut<StreamEnd>> {
        let f = self.f.clone();
        Some(Box::pin(async move {
            let req = match P::decode_request(&payload) {
                Ok(r) => r,
                Err(d) => {
                    let refusal = P::malformed(match d {
                        rafka_node_rpc_contract::protocol::DecodeFailure::UnknownVariant => {
                            rafka_node_rpc_contract::outcome::MalformedKind::UnknownVariant
                        }
                        rafka_node_rpc_contract::protocol::DecodeFailure::Corrupt => rafka_node_rpc_contract::outcome::MalformedKind::Corrupt,
                    });
                    return StreamEnd::Final { kind: P::frame_kind(&refusal), payload: P::encode_reply(&refusal).unwrap_or_default() };
                }
            };
            let span = tracing::info_span!(
                "rdm.node_rpc.stream.serve.via-direct",
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
            let sink = ReplySink::<P, NotStarted> { out, _p: PhantomData };
            match tracing::Instrument::instrument(f(peer, req, sink), span).await {
                Ok(last) => StreamEnd::Final { kind: P::frame_kind(&last), payload: P::encode_reply(&last).unwrap_or_default() },
                Err(fault) => StreamEnd::Fault(fault),
            }
        }))
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

/// `f` under `deadline`, or unbounded when there is none (a `Budget::Stream` call).
pub(crate) async fn bounded<F: Future>(deadline: Option<Instant>, f: F) -> Result<F::Output, tokio::time::error::Elapsed> {
    match deadline {
        Some(d) => timeout_at(d, f).await,
        None => Ok(f.await),
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

/// One received item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamItem<R> {
    /// A frame of the stream: its kind and its decoded body.
    Frame(FrameKind, R),
    /// The stream ended in a failure.
    Failed(StreamFailure),
}

/// The caller side of one committed stream.
pub struct ReplyStream<P: StreamingProtocol> {
    recv: RecvStream,
    buf: Vec<u8>,
    order: FrameOrder,
    deadline: Option<Instant>,
    done: bool,
    _p: PhantomData<fn() -> P>,
}

impl<P: StreamingProtocol> ReplyStream<P> {
    /// The next frame in order, a failure, or `None` once the stream has ended.
    pub async fn next(&mut self) -> Option<StreamItem<P::Reply>> {
        if self.done {
            return None;
        }
        loop {
            match decode_frame(&self.buf, P::MAX_REPLY_FRAME_BYTES) {
                Ok((payload, used)) => {
                    let decoded = P::decode_reply(payload);
                    self.buf.drain(..used);
                    let frame = match decoded {
                        Ok(f) => f,
                        Err(e) => return Some(self.fail(StreamFailure::Violation(format!("a frame does not decode: {e:?}")))),
                    };
                    let kind = P::frame_kind(&frame);
                    if let Err(reason) = self.order.admit(kind) {
                        return Some(self.fail(StreamFailure::Violation(reason)));
                    }
                    if self.order.state() == OrderState::Done {
                        self.done = true;
                    }
                    return Some(StreamItem::Frame(kind, frame));
                }
                Err(FrameError::Truncated) => {}
                Err(e) => return Some(self.fail(StreamFailure::Violation(format!("reply frame: {e:?}")))),
            }
            let mut chunk = vec![0u8; 16 * 1024];
            match bounded(self.deadline, self.recv.read(&mut chunk)).await {
                Err(_) => return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::ReplyDeadline))),
                Ok(Ok(Some(n))) => self.buf.extend_from_slice(&chunk[..n]),
                Ok(Ok(None)) => {
                    let why = if self.buf.is_empty() { "the stream ended before its terminal" } else { "the stream ended inside a frame" };
                    return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::ReplyLost(why.into()))));
                }
                Ok(Err(ReadError::Reset(c))) => return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::Reset(c.into_inner())))),
                Ok(Err(e)) => return Some(self.fail(StreamFailure::Indeterminate(IndeterminateReason::ReplyLost(e.to_string())))),
            }
        }
    }

    fn fail(&mut self, f: StreamFailure) -> StreamItem<P::Reply> {
        self.done = true;
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
        let span = tracing::info_span!("rdm.node_rpc.request.update.via-call", target = ?target, op = P::OP, outcome = tracing::field::Empty, elapsed_ms = tracing::field::Empty);
        let started = tokio::time::Instant::now();
        let opened = tracing::Instrument::instrument(
            self.open::<P::Reply, _>(target, P::OP, crate::client::Payload::Ready(payload), P::MAX_REPLY_FRAME_BYTES, opts, |early, bytes| early.reply::<P>(bytes)),
            span.clone(),
        )
        .await;
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        match opened {
            Phase::Done(out, evidence) => {
                span.record("outcome", out.name());
                span.in_scope(|| tracing::info!("node rpc stream call finished"));
                Err((out, evidence))
            }
            Phase::Committed(Opened { recv, evidence, reply_deadline, .. }) => {
                span.record("outcome", "stream-open");
                span.in_scope(|| tracing::info!("node rpc stream call opened"));
                Ok((ReplyStream { recv, buf: Vec::new(), order: FrameOrder::new(), deadline: reply_deadline, done: false, _p: PhantomData }, evidence))
            }
        }
    }
}
