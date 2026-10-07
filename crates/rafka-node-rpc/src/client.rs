//! The Node RPC client (node-rpc.md §20–§24; ownership amendment §6).
//!
//! One call is one attempt against one exact target. Pre-commit work —
//! resolve, dial, `open_bi`, writing the request —
//! spends the send budget and can only end `NotSent`. The commit cut is the
//! complete request written *and* the request direction finished cleanly; an
//! unfinished request is reset with `499 FRAME_NOT_SENT`. After the cut the
//! call ends `Reply`, `Unserved` (a `421`) or `Indeterminate`. Node RPC never
//! dials a second peer or target inside one call and never replays.

use crate::pool::{DialError, DialSpec, Failpoint, Pool, PoolKey};
use crate::resolve::{NodeResolver, NodeTarget, ResolvedNode};
use iroh::endpoint::{ReadError, ReadToEndError, VarInt, WriteError};
use iroh::Endpoint;
use rafka_mesh_entity::NodeId;
use rafka_node_rpc_contract::codes::ResetCode;
use rafka_node_rpc_contract::context::{dropped_as_str, CallContext};
use rafka_node_rpc_contract::framing::{decode_single_frame, encode_request, Fence, RequestHeader, MAX_VARINT_LEN};
use rafka_node_rpc_contract::outcome::{EarlyRefusal, IndeterminateReason, NotSentReason, PreCommit, RequestFinished, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

/// The two budget shapes (ownership amendment §6.1).
#[derive(Debug, Clone, Copy)]
pub enum Budget {
    /// One deadline over resolve, dial, send and reply; never reset between phases.
    Overall(Duration),
    /// A connect + complete-send bound, then a reply budget that starts at the commit cut.
    Split { send: Duration, reply: Duration },
}

#[derive(Debug, Clone)]
pub struct CallOptions {
    pub budget: Budget,
    /// Failpoint: write part of the request, then reset it with 499.
    pub cut_before_finish: bool,
    /// The caller's execution scope: part of the pool identity, so two
    /// scopes never share a pooled connection.
    pub scope: Option<String>,
    /// Failpoint: a dial this call starts stops after connecting, before it
    /// is checked against the resolver and pooled.
    pub after_connect: Option<Arc<Failpoint>>,
    /// The observability context to carry, verbatim (a carrier hands the original through).
    /// `None`: the current span's trace context under the client's `caller_system`.
    pub context: Option<CallContext>,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            budget: Budget::Overall(Duration::from_secs(10)),
            cut_before_finish: false,
            scope: None,
            after_connect: None,
            context: None,
        }
    }
}

/// Which exact leg a call used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallEvidence {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub committed: bool,
    /// The connection the call rode (`stable_id`), once it had one.
    pub connection: Option<usize>,
    /// The connection came from the pool rather than a dial this call waited on.
    pub reused: bool,
}

/// What a source observes of its own Direct connections (connections.md section 9 and
/// section 10): the pooled connection to an exact birth opened, a dial to it ended without a
/// connection, or a pooled connection broke. The observer is the source-owned connections
/// writer; the client reports facts and decides nothing.
pub trait ConnectionObserver: Send + Sync {
    /// A new pooled connection to `node` opened (a reused one is not reported).
    fn direct_connected(&self, node: &ResolvedNode);
    /// A dial to `node` ended with no connection: `reason` is the NotSent reason's text.
    fn direct_failed(&self, node: &ResolvedNode, reason: &str);
    /// The pooled connection to `node` broke after it had opened.
    fn direct_broken(&self, node: &ResolvedNode, reason: &str);
    /// A connection from `node` was accepted by this process's server: the same Direct
    /// Connected evidence as a dial of its own (connections.md §10), reported once per accepted
    /// connection. The default is the dial's own report.
    fn direct_accepted(&self, node: &ResolvedNode) {
        self.direct_connected(node)
    }
}

pub struct NodeRpcClient {
    endpoint: Endpoint,
    resolver: Arc<dyn NodeResolver>,
    pool: Pool,
    /// The source-owned connections writer, when the process has one.
    observer: Option<Arc<dyn ConnectionObserver>>,
    /// The system this process originates calls for (`rdm`, `rafka`); observability only.
    caller_system: Option<String>,
}

/// How the reply direction is decoded: after the commit cut, or after an
/// early refusal that stopped the request before it finished.
pub enum Decode<'a> {
    Committed(rafka_node_rpc_contract::outcome::Committed, &'a [u8]),
    Early(EarlyRefusal, Option<&'a [u8]>),
}

/// Where an invocation stands after the commit-cut phase.
pub(crate) enum Phase<R> {
    Done(RpcOutcome<R>, Option<CallEvidence>),
    Committed(Opened),
}

/// A committed invocation: the commit proof and the reply direction.
pub(crate) struct Opened {
    pub(crate) committed: rafka_node_rpc_contract::outcome::Committed,
    pub(crate) recv: iroh::endpoint::RecvStream,
    pub(crate) evidence: CallEvidence,
    pub(crate) key: PoolKey,
    pub(crate) conn: iroh::endpoint::Connection,
    pub(crate) reply_deadline: Instant,
    /// The fence the request named.
    pub(crate) target: Fence,
}

/// What came back after the commit cut, before protocol decoding.
enum Committed {
    Reply(Vec<u8>),
    Reset(u64),
    Lost(IndeterminateReason),
}

impl NodeRpcClient {
    pub fn new(endpoint: Endpoint, resolver: Arc<dyn NodeResolver>) -> Self {
        Self { endpoint, resolver, pool: Pool::default(), observer: None, caller_system: None }
    }

    /// Hand this client its source-owned connections writer: every Direct fact the client
    /// observes about its own pooled connections is reported there.
    pub fn with_connection_observer(mut self, observer: Arc<dyn ConnectionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Name the originating system every call of this client carries (`rdm`, `rafka`).
    pub fn with_caller_system(mut self, system: impl Into<String>) -> Self {
        self.caller_system = Some(system.into());
        self
    }

    /// The context a call carries: the one the caller handed in, or the current span's trace
    /// context under this client's `caller_system`. Anything malformed or over its bound is
    /// dropped here, named on a local span, so a conforming client never sends it.
    fn context_for(&self, opts: &CallOptions) -> CallContext {
        let ctx = opts.context.clone().unwrap_or_else(|| CallContext {
            caller_system: self.caller_system.clone(),
            traceparent: rafka_mesh_telemetry::current_traceparent(),
            tracestate: rafka_mesh_telemetry::current_tracestate(),
            baggage: None,
        });
        let (ctx, dropped) = ctx.sanitized();
        if !dropped.is_empty() {
            tracing::info_span!("rdm.node_rpc.request.update.via-context-dropped", decided_by = "caller", context_dropped = %dropped_as_str(&dropped))
                .in_scope(|| tracing::info!("observability context dropped before the request was sent; the call proceeds"));
        }
        ctx
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Every pooled connection's key.
    pub fn pooled(&self) -> Vec<PoolKey> {
        self.pool.keys()
    }

    /// Every key with a dial in flight.
    pub fn dialing(&self) -> Vec<PoolKey> {
        self.pool.dialing()
    }

    /// Invoke protocol `P` on `target`.
    pub async fn call<P: NodeProtocol>(
        &self,
        target: &NodeTarget,
        req: &P::Request,
        opts: &CallOptions,
    ) -> (RpcOutcome<P::Reply>, Option<CallEvidence>) {
        let pre = PreCommit::begin(P::OP);
        let payload = match P::encode_request(req) {
            Ok(p) => p,
            Err(e) => return (pre.not_sent(NotSentReason::Connection(format!("request does not encode: {}", e.0))), None),
        };
        self.invoke_raw::<P::Reply, _>(target, P::OP, payload, P::MAX_REPLY_FRAME_BYTES, opts, |d| match d {
            Decode::Committed(c, bytes) => c.reply::<P>(bytes),
            Decode::Early(e, bytes) => e.reply::<P>(bytes),
        })
        .await
    }

    /// Everything up to the commit cut: resolve, dial, open_bi, the request
    /// write and the clean finish. Ends `Done` with a pre-commit or early-refusal outcome, or
    /// `Committed` holding the reply direction.
    pub(crate) async fn open<R, E>(
        &self,
        target: &NodeTarget,
        op: u8,
        payload: Vec<u8>,
        max_reply: usize,
        opts: &CallOptions,
        early_decode: E,
    ) -> Phase<R>
    where
        E: FnOnce(EarlyRefusal, Option<&[u8]>) -> RpcOutcome<R>,
    {
        let start = Instant::now();
        let (send_deadline, overall) = match opts.budget {
            Budget::Overall(d) => (start + d, Some(start + d)),
            Budget::Split { send, .. } => (start + send, None),
        };
        let pre = PreCommit::begin(op);
        let node = match self.resolver.resolve(target) {
            Ok(n) => n,
            Err(f) => return Phase::Done(pre.not_sent(NotSentReason::Resolve(f)), None),
        };
        tracing::info!(step = "resolved", node = %node.name, addr = %node.transport_addr, incarnation = %node.incarnation.0, "the target resolved to one birth");
        // Whatever this node's record no longer names leaves the pool now.
        self.pool.purge_stale(&node);
        // The fence: the node the caller resolved and the op, never a dial target.
        let request_target = Fence { target_node_id: node.node_id.to_string(), op: op };
        let mut evidence = CallEvidence {
            node_id: node.node_id.clone(),
            addr: node.transport_addr,
            committed: false,
            connection: None,
            reused: false,
        };
        let key = PoolKey { scope: opts.scope.clone(), peer: node.endpoint_id, incarnation: node.incarnation.clone() };
        let spec = DialSpec {
            endpoint: self.endpoint.clone(),
            resolver: self.resolver.clone(),
            target: target.clone(),
            addr: node.transport_addr,
            deadline: send_deadline,
            failpoint: opts.after_connect.clone(),
        };
        let dial_started = Instant::now();
        let conn = match self.pool.get_or_dial(&key, spec).await {
            Ok((c, reused)) => {
                tracing::info!(step = if reused { "pooled" } else { "dialed" }, waited_ms = dial_started.elapsed().as_millis() as u64, "a connection to the target is held");
                evidence.connection = Some(c.stable_id());
                evidence.reused = reused;
                if !reused {
                    if let Some(o) = &self.observer {
                        o.direct_connected(&node);
                    }
                }
                c
            }
            Err(DialError::Superseded) => {
                // The birth moved while this dial was in flight: the target is stale.
                let now = self.resolver.resolve(target).ok().map(|n| n.incarnation.0).unwrap_or_default();
                stale_span("caller", &request_target, &format!("incarnation {} superseded by {now}", node.incarnation.0));
                return Phase::Done(pre.stale_before_finish(&request_target), Some(evidence));
            }
            Err(DialError::Deadline) => {
                tracing::info!(step = "dial-deadline", waited_ms = dial_started.elapsed().as_millis() as u64, "no connection to the target within the send budget");
                if let Some(o) = &self.observer {
                    o.direct_failed(&node, "dial deadline");
                }
                return Phase::Done(pre.not_sent(NotSentReason::Deadline), Some(evidence));
            }
            Err(DialError::Failed(e)) => {
                tracing::info!(step = "dial-failed", waited_ms = dial_started.elapsed().as_millis() as u64, error = %e, "the dial to the target failed");
                if let Some(o) = &self.observer {
                    o.direct_failed(&node, &e);
                }
                return Phase::Done(pre.not_sent(NotSentReason::Connection(e)), Some(evidence));
            }
        };
        let opened_at = Instant::now();
        let (mut send, mut recv) = match timeout_at(send_deadline, conn.open_bi()).await {
            Ok(Ok(s)) => {
                tracing::info!(step = "stream-opened", waited_ms = opened_at.elapsed().as_millis() as u64, "a request stream is open");
                s
            }
            Ok(Err(e)) => {
                self.pool.broken(&key, &conn);
                if let Some(o) = &self.observer {
                    o.direct_broken(&node, &e.to_string());
                }
                return Phase::Done(pre.not_sent(NotSentReason::Connection(e.to_string())), Some(evidence));
            }
            Err(_) => return Phase::Done(pre.not_sent(NotSentReason::Deadline), Some(evidence)),
        };
        let header = RequestHeader { fence: request_target.clone(), context: self.context_for(opts) };
        let frame = encode_request(&header, &payload);
        let frame_not_sent = VarInt::from_u32(ResetCode::FrameNotSent.code());
        if opts.cut_before_finish {
            let half = (frame.len() / 2).max(1);
            let _ = send.write_all(&frame[..half]).await;
            let _ = send.reset(frame_not_sent);
            let (_, out) = pre.cut_before_finish();
            return Phase::Done(out, Some(evidence));
        }
        let written = timeout_at(send_deadline, send.write_all(&frame)).await;
        if let Ok(Err(WriteError::Stopped(stop))) = &written {
            // The server refused before this request finished (node-rpc.md §27):
            // a healthy answer on a healthy connection.
            self.pool.healthy(&key);
            if stop.into_inner() == u64::from(ResetCode::UnservedOp.code()) {
                return Phase::Done(pre.unserved_before_finish(), Some(evidence));
            }
            if stop.into_inner() == u64::from(ResetCode::StaleTarget.code()) {
                // The target refused the fence; the connection to the process stays pooled.
                stale_span("target", &request_target, "");
                return Phase::Done(pre.stale_before_finish(&request_target), Some(evidence));
            }
            let bytes = timeout_at(send_deadline, recv.read_to_end(max_reply + MAX_VARINT_LEN)).await;
            let early = pre.stopped_before_finish();
            let out = match bytes {
                Ok(Ok(b)) => match decode_single_frame(&b, max_reply) {
                    Ok(payload) => early_decode(early, Some(payload)),
                    Err(_) => early_decode(early, None),
                },
                _ => early_decode(early, None),
            };
            return Phase::Done(out, Some(evidence));
        }
        if !matches!(written, Ok(Ok(()))) {
            if let Ok(Err(WriteError::ConnectionLost(_))) = &written {
                self.pool.broken(&key, &conn);
            }
            let _ = send.reset(frame_not_sent);
            let (_, out) = pre.cut_before_finish();
            return Phase::Done(out, Some(evidence));
        }
        let finished = send.finish().is_ok();
        let Some(proof) = RequestFinished::after_clean_finish(frame.len(), frame.len(), finished) else {
            let _ = send.reset(frame_not_sent);
            let (_, out) = pre.cut_before_finish();
            return Phase::Done(out, Some(evidence));
        };
        let committed = pre.commit(proof);
        evidence.committed = true;
        let reply_deadline = match (opts.budget, overall) {
            (_, Some(d)) => d,
            (Budget::Split { reply, .. }, None) => Instant::now() + reply,
            (Budget::Overall(_), None) => unreachable!(),
        };
        Phase::Committed(Opened { committed, recv, evidence, key, conn, reply_deadline, target: request_target })
    }

    /// Invoke a raw op (any op, served or not) — the unknown-op cell uses it.
    pub async fn invoke_raw<R, D>(
        &self,
        target: &NodeTarget,
        op: u8,
        payload: Vec<u8>,
        max_reply: usize,
        opts: &CallOptions,
        decode: D,
    ) -> (RpcOutcome<R>, Option<CallEvidence>)
    where
        D: FnOnce(Decode<'_>) -> RpcOutcome<R>,
    {
        use tracing::Instrument;
        // The caller's side of one call: its outcome, why, and how long it took; the steps it
        // reached are the log lines inside it (resolved, connection pooled or dialed, stream
        // opened, request written, reply read).
        let span = tracing::info_span!(
            "rdm.node_rpc.request.update.via-call",
            target = ?target,
            op,
            outcome = tracing::field::Empty,
            reason = tracing::field::Empty,
            reused = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
        );
        let started = Instant::now();
        let (out, evidence) = self.invoke_raw_steps(target, op, payload, max_reply, opts, decode).instrument(span.clone()).await;
        span.record("outcome", out.name());
        let reason = match &out {
            RpcOutcome::NotSent(n) => format!("{:?}", n.reason()),
            RpcOutcome::Indeterminate(i) => format!("{:?}", i.reason()),
            RpcOutcome::RejectedStale(r) => format!("stale target {}", r.target_node_id()),
            RpcOutcome::Unserved(u) => format!("op {} unserved", u.op()),
            RpcOutcome::Reply(_) => String::new(),
        };
        span.record("reason", reason.as_str());
        if let Some(e) = &evidence {
            span.record("reused", e.reused);
        }
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        span.in_scope(|| tracing::info!("node rpc call finished"));
        (out, evidence)
    }

    async fn invoke_raw_steps<R, D>(
        &self,
        target: &NodeTarget,
        op: u8,
        payload: Vec<u8>,
        max_reply: usize,
        opts: &CallOptions,
        decode: D,
    ) -> (RpcOutcome<R>, Option<CallEvidence>)
    where
        D: FnOnce(Decode<'_>) -> RpcOutcome<R>,
    {
        let decode = std::sync::Mutex::new(Some(decode));
        let take = || decode.lock().unwrap().take().expect("decode runs once");
        let Opened { committed, mut recv, evidence, key, conn, reply_deadline, target: request_target } =
            match self.open(target, op, payload, max_reply, opts, |early, bytes| take()(Decode::Early(early, bytes))).await {
                Phase::Done(out, evidence) => return (out, evidence),
                Phase::Committed(o) => o,
            };
        let decode = take();
        tracing::info!(step = "request-written", "the request is written and finished; awaiting the reply");
        let read_at = Instant::now();
        let got = match timeout_at(reply_deadline, recv.read_to_end(max_reply + MAX_VARINT_LEN)).await {
            Err(_) => Committed::Lost(IndeterminateReason::ReplyDeadline),
            Ok(Ok(bytes)) => Committed::Reply(bytes),
            Ok(Err(ReadToEndError::Read(ReadError::Reset(c)))) => Committed::Reset(c.into_inner()),
            Ok(Err(ReadToEndError::TooLong)) => Committed::Lost(IndeterminateReason::ProtocolViolation("reply longer than the protocol ceiling".into())),
            Ok(Err(e)) => Committed::Lost(IndeterminateReason::ReplyLost(e.to_string())),
        };
        tracing::info!(step = "reply-read", waited_ms = read_at.elapsed().as_millis() as u64, "the reply direction ended");
        // Pool health from what came back (never reachability).
        match &got {
            Committed::Reply(_) | Committed::Reset(_) => self.pool.healthy(&key),
            Committed::Lost(IndeterminateReason::ReplyDeadline) => self.pool.timed_out(&key, &conn),
            Committed::Lost(IndeterminateReason::ReplyLost(_)) => self.pool.broken(&key, &conn),
            Committed::Lost(_) => {}
        }
        let out = match got {
            Committed::Reply(bytes) => match decode_single_frame(&bytes, max_reply) {
                Ok(payload) => decode(Decode::Committed(committed, payload)),
                Err(e) => committed.indeterminate(IndeterminateReason::ProtocolViolation(format!("reply frame: {e:?}"))),
            },
            Committed::Reset(c) => {
                if c == u64::from(ResetCode::StaleTarget.code()) {
                    stale_span("target", &request_target, "");
                }
                committed.reset(c, &request_target)
            }
            Committed::Lost(r) => committed.indeterminate(r),
        };
        (out, Some(evidence))
    }
}

/// One span family for every stale-target refusal, whoever decided it: the
/// caller (its resolver no longer names the target) or the target (`425`).
/// `current` is what the decider holds instead, in its own words.
fn stale_span(decided_by: &'static str, asked: &Fence, current: &str) {
    tracing::info_span!(
        "rdm.node_rpc.connection.reject.via-stale-target",
        decided_by,
        node_id = %asked.target_node_id,
        op = asked.op,
        current,
    )
    .in_scope(|| tracing::info!("the fence is stale: RejectedStale, never dispatched"));
}
