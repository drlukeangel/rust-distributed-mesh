//! The Node RPC client (node-rpc.md §20–§24, §43; ownership amendment §6).
//!
//! One call is one owed request to one exact target (node-rpc.md §43): its identity is minted once,
//! and the runtime resends it with that identity on the schedule until it is acked, its receiver's
//! exit is proven, or its owner ends it. A call's budget is its owner's bound: when it lapses the
//! call ends and its request is owed no longer. Each attempt is resolve, the ordinary dial,
//! `open_bi` and the request frame; pre-commit work can only end `NotSent`. The commit cut is the
//! complete request frame written: the frame is self-delimited, and the request direction stays
//! open for the caller's acks of the response. An unfinished request is reset with
//! `499 FRAME_NOT_SENT`. After the cut the call ends `Reply`, `Unserved` (a `421`) or
//! `Indeterminate`. A resend is the same invocation, never a new execution; Node RPC never dials a
//! second target inside one call.

use crate::delivery::{AckOf, AckTo, CallGuard, Delivery, FrameOut, FrameReader, ReadEnd, ResponseVerdict, SlotEvent, SlotState};
use crate::pool::{DialError, DialSpec, Failpoint, Pool, PoolKey};
use crate::resolve::{NodeResolver, NodeTarget, ResolvedNode};
use iroh::endpoint::{VarInt, WriteError};
use iroh::Endpoint;
use rdm_core::NodeId;
use rdm_core::codes::ResetCode;
use rdm_core::context::{dropped_as_str, CallContext};
use rdm_core::framing::{encode_header, encode_request, DeliveryId, Target, RequestHeader, TransportHeader};
use rdm_core::outcome::{EarlyRefusal, IndeterminateReason, NotSentReason, PreCommit, RequestFinished, ResolveFailure, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout_at, Instant};

/// The two budget shapes (ownership amendment §6.1).
#[derive(Debug, Clone, Copy)]
pub enum Budget {
    /// One deadline over resolve, dial, send and reply; never reset between phases.
    Overall(Duration),
    /// A connect + complete-send bound, then a reply budget that starts at the commit cut.
    /// A connect and complete-send bound, then a reply budget that starts at the commit cut.
    Split {
        /// The bound on connecting and sending the complete request.
        send: Duration,
        /// The bound on the reply, counted from the commit cut.
        reply: Duration,
    },
    /// A connect + complete-send bound, then no reply bound: the stream lives as long as the
    /// transport does (idle and keepalive are the endpoint's liveness, R-T8). Only a server stream
    /// has a reply direction that outlasts any one budget; a unary reply is read unbounded too.
    Stream {
        /// The bound on connecting and sending the complete request.
        send: Duration,
    },
}

/// How one call is made: its budget, its scope, its observability context and its failpoints.
#[derive(Debug, Clone)]
pub struct CallOptions {
    /// The time bound of the call.
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
    /// A carried call's origin identity: the carrier's inner call keeps it, so the final target's
    /// duplicate window is the origin's, never the carrier's. `None`: this client mints the identity.
    pub carried: Option<CarriedIdentity>,
    /// The owner asks once (a reachability probe: node-recovery.md step 2, "one QUIC connect"): the
    /// request is never resent; the call ends when its one connect fails or its stream is lost, and
    /// otherwise waits for its answer within its budget. Its owner retargets, never this runtime.
    pub once: bool,
    /// The owner answers from its first unsent attempt (a carrier's one inner call: node-rpc.md
    /// §36.1, and the envelope's Forward, "CarrierEdgeLost directly from its failed dial"): a dial
    /// or reach that fails before the commit cut ends the call `NotSent`, never resent. Once
    /// committed the request is owed as any other.
    pub unsent_is_final: bool,
}

/// The origin's delivery identity a carrier's inner call keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarriedIdentity {
    /// The origin's identity of the request.
    pub id: DeliveryId,
    /// The origin's `forget_below`, valid for the final target too.
    pub forget_below: u64,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            budget: Budget::Overall(Duration::from_secs(10)),
            cut_before_finish: false,
            scope: None,
            after_connect: None,
            context: None,
            carried: None,
            once: false,
            unsent_is_final: false,
        }
    }
}

/// The request bytes of one call: encoded up front, or built at the write from the caller's
/// remaining budget.
pub(crate) enum Payload {
    Ready(Vec<u8>),
    AtWrite(Box<dyn FnOnce(Duration) -> Result<Vec<u8>, String> + Send>),
}

/// Which exact leg a call used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallEvidence {
    /// The node the call reached, as its resolver named it; `None` for a call to an endpoint the
    /// caller holds no record of ([`NodeTarget::Endpoint`]).
    pub node_id: Option<NodeId>,
    /// The address of the node's Iroh endpoint the call used.
    pub addr: SocketAddr,
    /// Whether the request reached the commit cut.
    pub committed: bool,
    /// The connection the call rode (`stable_id`), once it had one.
    pub connection: Option<usize>,
    /// The connection came from the pool rather than a dial this call waited on.
    pub reused: bool,
    /// The dial to the node ended with no connection, and why: the very fact the connection
    /// observer was handed (`direct_failed`). A carrier answers `CarrierEdgeLost` from this.
    pub dial_failure: Option<String>,
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
}

/// A connection this process's server accepted from a live node (connections.md section 10,
/// R-CN1): a recovery trigger for this node's own direct dial toward that exact birth, never
/// Direct evidence. The acceptor writes nothing to its connections log.
pub trait AcceptTrigger: Send + Sync {
    /// The server accepted a connection from `node`, the live birth the resolver names.
    fn accepted(&self, node: &ResolvedNode);
}

/// A Node RPC client: dials the exact target a resolver names, pools one connection per peer, scope
/// and incarnation, and returns a typed outcome per call. A clone shares its pool, its admission
/// block and its delivery state.
#[derive(Clone)]
pub struct NodeRpcClient {
    endpoint: Endpoint,
    pub(crate) resolver: Arc<dyn NodeResolver>,
    pool: Pool,
    /// The source-owned connections writer, when the process has one.
    observer: Option<Arc<dyn ConnectionObserver>>,
    /// The system this process originates calls for (`rdm`, `rafka`); observability only.
    caller_system: Option<String>,
    /// Why this client opens no new connection now, while its source admits none
    /// ([`Self::block_new_connections`]); `None` admits.
    admission: crate::pool::Admission,
    /// The process's delivery state: its birth, its window, its open invocations.
    delivery: Delivery,
}

/// How the reply direction is decoded: after the commit cut, or after an
/// early refusal that stopped the request before it finished.
pub enum Decode<'a> {
    /// The request committed: the commit cut and the reply bytes.
    Committed(rdm_core::outcome::Committed, &'a [u8]),
    /// The request stopped before it finished: the early refusal and the reply bytes, when any.
    Early(EarlyRefusal, Option<&'a [u8]>),
}

/// Where an invocation stands after the commit-cut phase.
pub(crate) enum Phase<R> {
    /// The attempt ended before the commit cut.
    Done(RpcOutcome<R>),
    /// The complete request frame is written: the frame's length.
    Committed(Opened, usize),
    /// The receiver stopped the request direction before the frame was complete: its refusal
    /// follows on the other direction.
    Stopped(Opened),
}

/// Whom one call reaches: the birth its resolver named, or the process at an endpoint it holds no
/// record of ([`NodeTarget::Endpoint`]).
#[derive(Debug, Clone)]
pub(crate) struct Reach {
    /// The birth the resolver named; `None` for an endpoint target.
    pub(crate) node: Option<ResolvedNode>,
    /// The birth the call's connection facts are recorded under (connections.md §10): the resolved
    /// birth, or for an endpoint target the birth the resolver names at that endpoint, if any.
    pub(crate) observed: Option<ResolvedNode>,
    /// The process's Iroh key.
    pub(crate) peer: iroh::PublicKey,
    /// The address the call dials.
    pub(crate) addr: SocketAddr,
}

/// One attempt past its commit cut: both directions of its stream.
pub(crate) struct Opened {
    pub(crate) send: iroh::endpoint::SendStream,
    pub(crate) recv: iroh::endpoint::RecvStream,
    pub(crate) key: PoolKey,
    pub(crate) conn: iroh::endpoint::Connection,
}

/// What the attempt task tells the call's owner.
#[derive(Debug)]
pub(crate) enum Attempted {
    /// An attempt crossed the commit cut (the first one counts).
    Committed { frame_len: usize },
    /// An attempt ended before the cut, and the request is resent: the latest reason.
    NotSent(NotSentReason),
    /// The request ended before any attempt committed, definitively: no resend.
    EndedPreCommit(RpcOutcome<()>),
    /// The receiver stopped the request before it was complete; its refusal reached the invocation.
    StoppedBeforeComplete,
    /// The stream was reset with this code after the request: definitive, no resend.
    Reset(u64),
    /// The receiving birth's exit or removal is proven: the owed request ends.
    ReceiverGone(String),
    /// No attempt committed within the send budget: the latest attempt's reason. The attempt task
    /// alone bounds every pre-commit step by the send budget, so the outcome of a dial cut by it is
    /// recorded before the call ends.
    SendDeadline(NotSentReason),
    /// A `Budget::Stream` call, committed, lost its connection to the responder and no fresh dial
    /// connected within its send bound: the transport's liveness, its only reply bound, ended.
    TransportLost(String),
}

impl NodeRpcClient {
    /// A client on `endpoint` that resolves its targets through `resolver`. It is a process of its
    /// own: its delivery identities are a birth it mints once ([`Self::with_delivery`] gives it a
    /// node's).
    pub fn new(endpoint: Endpoint, resolver: Arc<dyn NodeResolver>) -> Self {
        let me = Self {
            endpoint,
            resolver,
            pool: Pool::default(),
            observer: None,
            caller_system: None,
            admission: crate::pool::Admission::default(),
            delivery: Delivery::new(crate::delivery::minted_birth()),
        };
        me.attach();
        me
    }

    /// Send as the process `delivery` is, sharing its window and its open invocations with the
    /// process's server.
    pub fn with_delivery(mut self, delivery: Delivery) -> Self {
        self.delivery = delivery;
        self.attach();
        self
    }

    /// The process's delivery state.
    pub fn delivery(&self) -> &Delivery {
        &self.delivery
    }

    fn attach(&self) {
        let (endpoint, resolver, pool, admission, delivery) = (self.endpoint.clone(), self.resolver.clone(), self.pool.clone(), self.admission.clone(), self.delivery.clone());
        // A resent response whose caller holds no live connection here takes the ordinary dial
        // to the birth the resolver names at that endpoint.
        let dial = Arc::new(move |peer: iroh::PublicKey| {
            let (endpoint, resolver, pool, admission, delivery) = (endpoint.clone(), resolver.clone(), pool.clone(), admission.clone(), delivery.clone());
            Box::pin(async move {
                let node = resolver.by_endpoint(&peer)?;
                let key = PoolKey { scope: None, peer, incarnation: Some(node.incarnation.clone()) };
                let spec = DialSpec {
                    endpoint,
                    resolver: resolver.clone(),
                    target: Some(NodeTarget::ExactNode(node.node_id.clone())),
                    addr: node.transport_addr,
                    failpoint: None,
                    opened: None,
                    admission,
                    delivery: Some(delivery),
                };
                pool.get_or_dial(&key, spec, Instant::now() + rdm_core::cadence::staleness_floor()).await.ok().map(|(c, _)| c)
            }) as crate::delivery::BoxFut<Option<iroh::endpoint::Connection>>
        });
        self.delivery.attach_client(self.resolver.clone(), dial);
    }

    /// Open no new connection from now on, naming `why`, until [`Self::admit_new_connections`]: a
    /// call that would dial ends `NotSent` `AdmissionBlocked`, and a dial already running closes the
    /// connection it opens instead of pooling it. A pooled connection is untouched. The admission
    /// block of the client a node originates its logged connections on (proposal
    /// node-properties-own-commands.md §2.1 Precondition, §2.4): set when its
    /// `node.connections.delete { All }` closes them, lifted when its name apply resolves or it
    /// starts.
    pub fn block_new_connections(&self, why: impl Into<String>) {
        self.admission.block(why.into());
    }

    /// Open new connections again: the block [`Self::block_new_connections`] set is lifted.
    pub fn admit_new_connections(&self) {
        self.admission.lift();
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
            traceparent: crate::current_traceparent(),
            tracestate: crate::current_tracestate(),
            baggage: None,
        });
        let (ctx, dropped) = ctx.sanitized();
        if !dropped.is_empty() {
            tracing::info_span!("rdm.node_rpc.request.update.via-context-dropped", decided_by = "caller", context_dropped = %dropped_as_str(&dropped))
                .in_scope(|| tracing::info!("observability context dropped before the request was sent; the call proceeds"));
        }
        ctx
    }

    /// The Iroh endpoint the client dials from.
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

    /// Close the connections this client holds to `peer`: the next call to it dials afresh. A caller
    /// that has just been answered `left` by a node closes the connection the stop rode, so its start
    /// reaches the parked node on a fresh dial.
    pub fn close_pooled_to(&self, peer: &iroh::PublicKey, reason: &'static str) -> usize {
        self.pool.close_peer(peer, reason.as_bytes())
    }

    /// Close the connections this client holds to `peer` for `reason` and hand back each one closed,
    /// with no span of the pool's own: the caller names why on each connection's span (a
    /// `node.connections.delete` closing a data-plane connection is no hard cut of a pool).
    pub fn close_connections_to(&self, peer: &iroh::PublicKey, reason: &'static str) -> Vec<crate::pool::ClosedConnection> {
        self.pool.close_to(peer, reason.as_bytes())
    }

    /// The endpoint the resolver names for the node `node` now, if it resolves: the key its pooled
    /// connections to that node are held under.
    pub fn endpoint_of(&self, node: &rdm_core::NodeId) -> Option<iroh::PublicKey> {
        self.resolver.resolve(&NodeTarget::ExactNode(node.clone())).ok().map(|n| n.endpoint_id)
    }

    /// Hard-cut every connection this client holds (`node.connections.delete`): dials in flight are
    /// cancelled and every pooled connection is closed, so no call rides one and the next call dials
    /// afresh. Returns how many pooled connections were closed.
    pub fn close_pooled(&self, reason: &'static str) -> usize {
        self.pool.close_all(reason.as_bytes())
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

    /// Where `target` is now, or the reason the call cannot be sent.
    pub(crate) fn reach(&self, target: &NodeTarget) -> Result<Reach, NotSentReason> {
        Ok(match target {
            // The process at an endpoint the caller holds no record of: dialled as named, its target
            // naming no node.
            NodeTarget::Endpoint { endpoint_id, addr } => {
                tracing::info!(step = "endpoint", peer = %endpoint_id.fmt_short(), addr = %addr, "the target is the process at an endpoint, dialled as named");
                let observed = self.resolver.by_endpoint(endpoint_id).filter(|n| n.transport_addr == *addr);
                if let Some(n) = &observed {
                    self.pool.purge_stale(n);
                }
                Reach { node: None, observed, peer: *endpoint_id, addr: *addr }
            }
            _ => match self.resolver.resolve(target) {
                Ok(n) => {
                    tracing::info!(step = "resolved", node = %n.name, node_id = %n.node_id, peer = %n.endpoint_id.fmt_short(), addr = %n.transport_addr, incarnation = %n.incarnation.as_str(), "the target resolved to one birth");
                    // Whatever this node's record no longer names leaves the pool now.
                    self.pool.purge_stale(&n);
                    Reach { peer: n.endpoint_id, addr: n.transport_addr, observed: Some(n.clone()), node: Some(n) }
                }
                Err(f) => return Err(NotSentReason::Resolve(f)),
            },
        })
    }

    /// One attempt up to the commit cut: the ordinary dial, open_bi and the complete request frame
    /// `header | sections`. Ends `Done` with a pre-commit outcome, or `Committed` holding both
    /// directions of the stream; the request direction is not finished.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn attempt(
        &self,
        target: &NodeTarget,
        node: &Reach,
        op: u8,
        header: &TransportHeader,
        payload: &mut Payload,
        opts: &CallOptions,
        send_deadline: Instant,
        evidence: &Mutex<CallEvidence>,
    ) -> Phase<()> {
        let pre = PreCommit::begin(op);
        // The request's target: the node the caller resolved and the op, never a dial target. A call to an
        // endpoint names no node; no receiver reads the node id to refuse the call (R-UC1a).
        let request_target = Target { node_id: node.node.as_ref().map(|n| n.node_id.to_string()).unwrap_or_default(), op };
        let key = PoolKey { scope: opts.scope.clone(), peer: node.peer, incarnation: node.observed.as_ref().map(|n| n.incarnation.clone()) };
        let spec = DialSpec {
            endpoint: self.endpoint.clone(),
            resolver: self.resolver.clone(),
            target: node.node.as_ref().map(|_| target.clone()),
            addr: node.addr,
            failpoint: opts.after_connect.clone(),
            // The dial that opens the connection reports it, once, whoever is waiting for it.
            opened: match (self.observer.clone(), node.observed.clone()) {
                (Some(o), Some(n)) => Some(Arc::new(move || o.direct_connected(&n)) as Arc<dyn Fn() + Send + Sync>),
                _ => None,
            },
            admission: self.admission.clone(),
            delivery: Some(self.delivery.clone()),
        };
        let dial_started = Instant::now();
        let conn = match self.pool.get_or_dial(&key, spec, send_deadline).await {
            Ok((c, reused)) => {
                tracing::info!(step = if reused { "pooled" } else { "dialed" }, waited_ms = dial_started.elapsed().as_millis() as u64, "a connection to the target is held");
                let mut ev = evidence.lock().unwrap();
                ev.connection = Some(c.stable_id());
                ev.reused = reused;
                c
            }
            Err(DialError::Superseded) => {
                // The birth moved while this dial was in flight: the target is stale.
                let now = self.resolver.resolve(target).ok().map(|n| n.incarnation.to_string()).unwrap_or_default();
                stale_span(&request_target, &format!("incarnation {} superseded by {now}", key.birth()));
                return Phase::Done(pre.stale_before_finish(&request_target));
            }
            Err(DialError::Deadline) => {
                tracing::info!(step = "dial-deadline", waited_ms = dial_started.elapsed().as_millis() as u64, "no connection to the target within the send budget");
                evidence.lock().unwrap().dial_failure = Some("dial deadline".to_string());
                if let (Some(o), Some(n)) = (&self.observer, &node.observed) {
                    o.direct_failed(n, "dial deadline");
                }
                return Phase::Done(pre.not_sent(NotSentReason::Deadline));
            }
            Err(DialError::Blocked { at, why }) => {
                // Not a dial that failed: the source admits no new connection, so no Direct fact
                // is reported (nothing was tried, or what opened was closed before it was pooled).
                tracing::info_span!(
                    "rdm.node_rpc.connection.reject.via-admission-block",
                    peer = %key.peer.fmt_short(),
                    incarnation_id = %key.birth(),
                    destination = %node.node.as_ref().map(|n| n.name.to_string()).unwrap_or_default(),
                    destination_node_id = %node.node.as_ref().map(|n| n.node_id.to_string()).unwrap_or_default(),
                    at,
                    reason = %why,
                    waited_ms = dial_started.elapsed().as_millis() as u64,
                )
                .in_scope(|| tracing::info!("the source admits no new connection: the call is not sent"));
                return Phase::Done(pre.not_sent(NotSentReason::AdmissionBlocked(why)));
            }
            Err(DialError::Failed(e)) => {
                tracing::info!(step = "dial-failed", waited_ms = dial_started.elapsed().as_millis() as u64, error = %e, "the dial to the target failed");
                evidence.lock().unwrap().dial_failure = Some(e.clone());
                if let (Some(o), Some(n)) = (&self.observer, &node.observed) {
                    o.direct_failed(n, &e);
                }
                return Phase::Done(pre.not_sent(NotSentReason::Connection(e)));
            }
        };
        let opened_at = Instant::now();
        let (mut send, recv) = match timeout_at(send_deadline, conn.open_bi()).await {
            Ok(Ok(s)) => {
                tracing::info!(step = "stream-opened", waited_ms = opened_at.elapsed().as_millis() as u64, "a request stream is open");
                s
            }
            Ok(Err(e)) => {
                self.pool.broken(&key, &conn);
                if let (Some(o), Some(n)) = (&self.observer, &node.observed) {
                    o.direct_broken(n, &e.to_string());
                }
                return Phase::Done(pre.not_sent(NotSentReason::Connection(e.to_string())));
            }
            Err(_) => {
                tracing::info!(step = "stream-open-deadline", waited_ms = opened_at.elapsed().as_millis() as u64, "no request stream opened within the send budget");
                return Phase::Done(pre.not_sent(NotSentReason::Deadline));
            }
        };
        if let Payload::AtWrite(_) = payload {
            let Payload::AtWrite(build) = std::mem::replace(payload, Payload::Ready(Vec::new())) else { unreachable!("matched above") };
            // The remaining budget is measured here, after resolve, dial and stream open,
            // immediately before the request is first written; a resend carries the same bytes.
            let remaining = match opts.budget {
                Budget::Overall(_) => send_deadline.saturating_duration_since(Instant::now()),
                Budget::Split { reply, .. } => reply,
                Budget::Stream { send } => send,
            };
            tracing::info!(step = "remaining-measured", remaining_ms = remaining.as_millis() as u64, "the remaining budget at the request write");
            match build(remaining) {
                Ok(bytes) => *payload = Payload::Ready(bytes),
                Err(e) => {
                    let _ = send.reset(VarInt::from_u32(ResetCode::FrameNotSent.code()));
                    return Phase::Done(pre.not_sent(NotSentReason::Connection(format!("request does not encode: {e}"))));
                }
            }
        }
        let Payload::Ready(payload) = &*payload else { unreachable!("built above") };
        let sections = RequestHeader { target: request_target.clone(), context: self.context_for(opts) };
        let mut frame = Vec::new();
        encode_header(header, &mut frame);
        frame.extend(encode_request(&sections, payload));
        let frame_not_sent = VarInt::from_u32(ResetCode::FrameNotSent.code());
        if opts.cut_before_finish {
            let half = (frame.len() / 2).max(1);
            let _ = send.write_all(&frame[..half]).await;
            let _ = send.reset(frame_not_sent);
            let (_, out) = pre.cut_before_finish();
            return Phase::Done(out);
        }
        let written = timeout_at(send_deadline, send.write_all(&frame)).await;
        if let Ok(Err(WriteError::Stopped(stop))) = &written {
            // The server refused before this request was complete (node-rpc.md §27): a healthy
            // answer on a healthy connection.
            if stop.into_inner() == u64::from(ResetCode::UnservedOp.code()) {
                return Phase::Done(pre.unserved_before_finish());
            }
            // Its refusal is a response frame on the stream's other direction.
            return Phase::Stopped(Opened { send, recv, key, conn });
        }
        if !matches!(written, Ok(Ok(()))) {
            if let Ok(Err(WriteError::ConnectionLost(_))) = &written {
                self.pool.broken(&key, &conn);
            }
            let _ = send.reset(frame_not_sent);
            let (_, out) = pre.cut_before_finish();
            return Phase::Done(out);
        }
        evidence.lock().unwrap().committed = true;
        Phase::Committed(Opened { send, recv, key, conn }, frame.len())
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
        self.invoke_payload(target, op, Payload::Ready(payload), max_reply, opts, decode).await
    }

    pub(crate) async fn invoke_payload<R, D>(
        &self,
        target: &NodeTarget,
        op: u8,
        payload: Payload,
        max_reply: usize,
        opts: &CallOptions,
        decode: D,
    ) -> (RpcOutcome<R>, Option<CallEvidence>)
    where
        D: FnOnce(Decode<'_>) -> RpcOutcome<R>,
    {
        self.invoke_via(target, None, op, payload, max_reply, opts, decode).await
    }

    /// [`Self::invoke_payload`] for a request that also reaches `final_target` (a carried call's
    /// final target): its frames are held toward the carrier and the final target both.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invoke_via<R, D>(
        &self,
        target: &NodeTarget,
        final_target: Option<&NodeId>,
        op: u8,
        payload: Payload,
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
        // opened, request written, reply read), and its delivery: the identity, how many attempts
        // it took and how its owed request ended.
        let span = tracing::info_span!(
            "rdm.node_rpc.request.update.via-call",
            target = ?target,
            op,
            outcome = tracing::field::Empty,
            reason = tracing::field::Empty,
            reused = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
            delivery_id = tracing::field::Empty,
            attempts = tracing::field::Empty,
            delivery = tracing::field::Empty,
        );
        let started = Instant::now();
        let (out, evidence) = self.owe_steps(target, final_target, op, payload, max_reply, opts, decode, &span).instrument(span.clone()).await;
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

    /// Open one owed request (node-rpc.md §43): resolve once, mint its identity, open its invocation
    /// and start the task that sends and resends it. `Err` is an outcome before anything was owed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn owe(
        &self,
        target: &NodeTarget,
        final_target: Option<&NodeId>,
        op: u8,
        payload: Payload,
        max_reply: usize,
        opts: &CallOptions,
        unary: bool,
        deadlines: (Instant, Option<Instant>),
        span: &tracing::Span,
    ) -> Result<OwedCall, (RpcOutcome<()>, Option<CallEvidence>)> {
        let node = match self.reach(target) {
            Ok(n) => n,
            Err(r) => return Err((PreCommit::begin(op).not_sent(r), None)),
        };
        let mut receivers = vec![node.peer];
        if let Some(f) = final_target {
            if let Ok(n) = self.resolver.resolve(&NodeTarget::ExactNode(f.clone())) {
                receivers.push(n.endpoint_id);
            }
        }
        let (id, carried_floor) = match &opts.carried {
            Some(c) => (c.id.clone(), Some(c.forget_below)),
            None => (self.delivery.mint(&receivers), None),
        };
        span.record("delivery_id", tracing::field::display(&id));
        let (guard, frames, state) = self.delivery.open_call(id.clone(), max_reply, unary);
        let evidence = Arc::new(Mutex::new(CallEvidence {
            node_id: node.node.as_ref().map(|n| n.node_id.clone()),
            addr: node.addr,
            committed: false,
            connection: None,
            reused: false,
            dial_failure: None,
        }));
        let (tx, attempted) = mpsc::unbounded_channel();
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let task = Attempts {
            client: self.clone(),
            target: target.clone(),
            birth: node.observed.as_ref().filter(|_| node.node.is_some()).map(|n| n.incarnation.clone()),
            op,
            id: id.clone(),
            receivers,
            carried_floor,
            payload,
            opts: opts.clone(),
            deadlines,
            evidence: evidence.clone(),
            tx,
            state: state.clone(),
            attempts: attempts.clone(),
            committed_on: None,
        };
        // The attempts run on the dispatcher of the call's span, as the pool's dial does: a worker that
        // holds no dispatcher still delivers their events to the call's subscriber.
        let run = tracing::Instrument::instrument(task.run(), span.clone());
        let handle = match tracing::Span::with_subscriber(span, |(_, d): (&tracing::Id, &tracing::Dispatch)| d.clone()) {
            Some(d) => tokio::spawn(UnderDispatch { fut: Some(Box::pin(run)), dispatch: d }),
            None => tokio::spawn(run),
        };
        Ok(OwedCall { guard, frames, attempted, evidence, attempts, _task: AbortOnDrop(Some(handle)) })
    }

    #[allow(clippy::too_many_arguments)]
    async fn owe_steps<R, D>(
        &self,
        target: &NodeTarget,
        final_target: Option<&NodeId>,
        op: u8,
        payload: Payload,
        max_reply: usize,
        opts: &CallOptions,
        decode: D,
        span: &tracing::Span,
    ) -> (RpcOutcome<R>, Option<CallEvidence>)
    where
        D: FnOnce(Decode<'_>) -> RpcOutcome<R>,
    {
        let start = Instant::now();
        let (send_deadline, overall) = match opts.budget {
            Budget::Overall(d) => (start + d, Some(start + d)),
            Budget::Split { send, .. } | Budget::Stream { send } => (start + send, None),
        };
        let after_commit = |at: Instant| match (opts.budget, overall) {
            (_, Some(d)) => Some(d),
            (Budget::Split { reply, .. }, None) => Some(at + reply),
            (Budget::Stream { .. }, None) => None,
            (Budget::Overall(_), None) => unreachable!(),
        };
        let mut owed = match self.owe(target, final_target, op, payload, max_reply, opts, true, (send_deadline, overall), span) {
            Ok(o) => o,
            Err((out, ev)) => return (out.retyped().expect("no reply before anything is owed"), ev),
        };
        let mut pre = Some(PreCommit::begin(op));
        let mut committed: Option<rdm_core::outcome::Committed> = None;
        let mut stopped: Option<EarlyRefusal> = None;
        let mut last = NotSentReason::Deadline;
        // Before the commit the attempt task bounds every step by the send budget and says when it
        // lapsed; after it, the reply budget is this owner's.
        let mut deadline: Option<Instant> = None;
        let out = loop {
            let wait = async {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                // A frame already delivered is taken before anything the attempt task said after it.
                biased;
                f = owed.frames.recv() => match f {
                    Some(SlotEvent::Frame(bytes, _)) => {
                        span.record("delivery", "acked");
                        break match (committed.take(), stopped.take(), pre.take()) {
                            (Some(c), _, _) => decode(Decode::Committed(c, &bytes)),
                            (None, Some(e), _) => decode(Decode::Early(e, Some(&bytes))),
                            // A frame before the commit event: the event is still queued behind it.
                            (None, None, Some(p)) => decode(Decode::Committed(p.commit(RequestFinished::after_complete_write(0, 0).expect("complete")), &bytes)),
                            (None, None, None) => unreachable!("a call is committed, stopped or pre-commit"),
                        };
                    }
                    Some(SlotEvent::Violation(why)) => {
                        break match (committed.take(), pre.take()) {
                            (Some(c), _) => c.indeterminate(IndeterminateReason::ProtocolViolation(why)),
                            (None, Some(p)) => p.not_sent(NotSentReason::Connection(why)),
                            (None, None) => PreCommit::begin(op).not_sent(NotSentReason::Connection(why)),
                        };
                    }
                    None => continue,
                },
                ev = owed.attempted.recv() => match ev {
                    Some(Attempted::Committed { frame_len }) => {
                        if let Some(p) = pre.take() {
                            committed = Some(p.commit(RequestFinished::after_complete_write(frame_len, frame_len).expect("a complete write")));
                            deadline = after_commit(Instant::now());
                        }
                    }
                    Some(Attempted::NotSent(r)) => last = r,
                    Some(Attempted::TransportLost(why)) => {
                        break match (committed.take(), pre.take()) {
                            (Some(c), _) => c.indeterminate(IndeterminateReason::ReplyLost(why)),
                            (None, Some(p)) => p.not_sent(NotSentReason::Connection(why)),
                            (None, None) => PreCommit::begin(op).not_sent(NotSentReason::Connection(why)),
                        };
                    }
                    Some(Attempted::SendDeadline(r)) => {
                        span.record("delivery", "cancelled");
                        break match (committed.take(), stopped.take(), pre.take()) {
                            (Some(c), _, _) => c.indeterminate(IndeterminateReason::ReplyDeadline),
                            (None, Some(e), _) => decode(Decode::Early(e, None)),
                            (None, None, Some(p)) => p.not_sent(r),
                            (None, None, None) => unreachable!("a call is committed, stopped or pre-commit"),
                        };
                    }
                    Some(Attempted::StoppedBeforeComplete) => {
                        if let Some(p) = pre.take() {
                            stopped = Some(p.stopped_before_finish());
                        }
                    }
                    Some(Attempted::EndedPreCommit(o)) => {
                        // Definitive before anything was dispatched (unserved, stale, the owner's cut).
                        break match committed.take() {
                            None => o.retyped().expect("no reply before the commit cut"),
                            Some(c) => c.indeterminate(IndeterminateReason::ReplyLost("a resend ended before its commit cut".into())),
                        };
                    }
                    Some(Attempted::Reset(code)) => {
                        break match (committed.take(), pre.take()) {
                            (Some(c), _) => c.reset(code),
                            (None, Some(p)) if code == u64::from(ResetCode::UnservedOp.code()) => p.unserved_before_finish(),
                            (None, Some(p)) => p.not_sent(NotSentReason::Connection(format!("reset {code} before the commit cut"))),
                            (None, None) => PreCommit::begin(op).not_sent(NotSentReason::Connection(format!("reset {code}"))),
                        };
                    }
                    Some(Attempted::ReceiverGone(why)) => {
                        span.record("delivery", "receiver-gone");
                        break match (committed.take(), pre.take()) {
                            (Some(c), _) => c.indeterminate(IndeterminateReason::ReceiverGone(why)),
                            (None, Some(p)) => p.not_sent(NotSentReason::Resolve(ResolveFailure::Gone)),
                            (None, None) => PreCommit::begin(op).not_sent(NotSentReason::Resolve(ResolveFailure::Gone)),
                        };
                    }
                    None => {
                        break match (committed.take(), stopped.take(), pre.take()) {
                            (Some(c), _, _) => c.indeterminate(IndeterminateReason::ReplyLost("the request's attempts ended".into())),
                            (None, Some(e), _) => decode(Decode::Early(e, None)),
                            (None, None, Some(p)) => p.not_sent(last.clone()),
                            (None, None, None) => PreCommit::begin(op).not_sent(last.clone()),
                        };
                    }
                },
                () = wait => {
                    // The owner's budget lapsed: the call ends, and its request is owed no longer.
                    span.record("delivery", "cancelled");
                    break match (committed.take(), stopped.take(), pre.take()) {
                        (Some(c), _, _) => c.indeterminate(IndeterminateReason::ReplyDeadline),
                        (None, Some(e), _) => decode(Decode::Early(e, None)),
                        (None, None, Some(p)) => p.not_sent(last.clone()),
                        (None, None, None) => unreachable!("a call is committed, stopped or pre-commit"),
                    };
                }
            }
        };
        span.record("attempts", owed.attempts.load(std::sync::atomic::Ordering::SeqCst));
        let evidence = owed.evidence.lock().unwrap().clone();
        owed.close(out.reply().is_none()).await;
        (out, Some(evidence))
    }
}

/// One owed request, held by its owner: dropped, the invocation ends and its request is owed no
/// longer (the owner's cancellation).
pub(crate) struct OwedCall {
    pub(crate) guard: CallGuard,
    pub(crate) frames: mpsc::UnboundedReceiver<SlotEvent>,
    pub(crate) attempted: mpsc::UnboundedReceiver<Attempted>,
    pub(crate) evidence: Arc<Mutex<CallEvidence>>,
    pub(crate) attempts: Arc<std::sync::atomic::AtomicU32>,
    /// The attempt task: aborted with the owner's hold.
    pub(crate) _task: AbortOnDrop,
}

/// A task that ends when its handle is dropped.
/// An earlier attempt's stream, kept after a resend: its acks and response frames reach the
/// invocation as any other stream's do, and its response frames are acked on it. It ends with the
/// invocation, with the stream, or on a frame that breaks the framing.
async fn keep_reading(d: Delivery, out: FrameOut, mut reader: FrameReader, mut state: watch::Receiver<SlotState>) {
    loop {
        tokio::select! {
            biased;
            changed = state.changed() => {
                if changed.is_err() || state.borrow().complete {
                    out.finish().await;
                    return;
                }
            }
            h = reader.header() => match h {
                Ok(Some(h @ TransportHeader::Ack { .. })) => d.on_ack(&h),
                Ok(Some(h @ TransportHeader::Response { .. })) => {
                    let TransportHeader::Response { invocation_id, .. } = &h else { unreachable!() };
                    let Ok(body) = reader.body(d.reply_bound(invocation_id)).await else { return };
                    match d.on_response(&h, body, AckTo::Stream(out.clone())) {
                        ResponseVerdict::Ack(acks) => {
                            for a in acks {
                                d.write_ack_header(&out, &a).await;
                            }
                        }
                        ResponseVerdict::Held | ResponseVerdict::Drop => {}
                        ResponseVerdict::Violation(_) => {
                            out.reset(ResetCode::ProtocolViolation).await;
                            reader.stop(ResetCode::ProtocolViolation);
                            return;
                        }
                    }
                }
                _ => return,
            },
        }
    }
}

/// A task's future polled, and dropped, under one dispatcher. A span the future holds is closed through
/// its own dispatcher, but the registry releases that span's parent through the dispatcher of the thread
/// the close happens on: a runtime that drops a finished or aborted future on a worker holding none would
/// leave the parent (the caller's own span, and every span above it) open for good.
struct UnderDispatch {
    fut: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
    dispatch: tracing::Dispatch,
}

impl std::future::Future for UnderDispatch {
    type Output = ();
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        let dispatch = self.dispatch.clone();
        let _current = tracing::dispatcher::set_default(&dispatch);
        let Some(fut) = self.fut.as_mut() else { return std::task::Poll::Ready(()) };
        let polled = fut.as_mut().poll(cx);
        if polled.is_ready() {
            self.fut = None;
        }
        polled
    }
}

impl Drop for UnderDispatch {
    fn drop(&mut self) {
        if self.fut.is_some() {
            let _current = tracing::dispatcher::set_default(&self.dispatch);
            self.fut = None;
        }
    }
}

pub(crate) struct AbortOnDrop(pub(crate) Option<tokio::task::JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(h) = &self.0 {
            h.abort();
        }
    }
}

impl OwedCall {
    /// End the owner's hold and wait for the attempt task to end, so nothing of the call (its span
    /// above all) outlives it: aborted when the owner ends it early, else it ends on its own once
    /// the invocation is complete (its last ack written, its request direction finished).
    pub(crate) async fn close(&mut self, abort: bool) {
        if abort {
            self.guard.complete();
        }
        if let Some(h) = self._task.0.take() {
            if abort {
                h.abort();
            }
            let _ = h.await;
        }
    }
}

/// How one attempt's stream ended.
enum StreamEnd {
    /// The invocation is complete.
    Complete,
    /// Reset with a code that ends the request (`421`, `423`, `424`).
    Definitive(u64),
    /// The stream broke the framing; the invocation was told.
    Violation,
    /// No ack in time: the attempt is abandoned, the request resent.
    NoAck,
    /// The stream (or its connection) is gone.
    Lost,
}

/// The task that sends one owed request and resends it, with the same identity, until it is acked,
/// its receiver's exit is proven, or its owner drops it.
struct Attempts {
    client: NodeRpcClient,
    target: NodeTarget,
    /// The receiving birth the identity was minted toward: a resolver naming another incarnation
    /// proves this one ended. `None` for a call to an endpoint no record names.
    birth: Option<rdm_core::IncarnationId>,
    op: u8,
    id: DeliveryId,
    receivers: Vec<iroh::PublicKey>,
    carried_floor: Option<u64>,
    payload: Payload,
    opts: CallOptions,
    deadlines: (Instant, Option<Instant>),
    evidence: Arc<Mutex<CallEvidence>>,
    tx: mpsc::UnboundedSender<Attempted>,
    state: watch::Receiver<SlotState>,
    attempts: Arc<std::sync::atomic::AtomicU32>,
    /// The connection a `Budget::Stream` request committed on: the call lives as long as it does.
    committed_on: Option<iroh::endpoint::Connection>,
}

impl Attempts {
    fn receiver_gone(&self) -> Option<String> {
        let held = self.birth.as_ref()?;
        match self.client.resolver.resolve(&self.target) {
            Ok(n) if &n.incarnation == held => None,
            Ok(n) => Some(format!("incarnation {} superseded by {}", held.as_str(), n.incarnation.as_str())),
            Err(ResolveFailure::Gone) => Some(format!("incarnation {} exited or removed", held.as_str())),
            Err(_) => None,
        }
    }

    fn lowest_unacked(&self) -> u64 {
        let own = self.client.delivery.lowest_unacked(&self.receivers);
        match self.carried_floor {
            Some(r) => r,
            None => own,
        }
    }

    async fn pause(&mut self, until: Instant) {
        let mut changes = self.client.resolver.changes();
        let resolver_moved = async {
            match changes.as_mut() {
                Some(c) => {
                    let _ = c.changed().await;
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            () = tokio::time::sleep_until(until) => {}
            () = resolver_moved => {}
            _ = self.state.changed() => {}
        }
    }

    async fn run(mut self) {
        let pacing = self.client.delivery.pacing();
        let mut attempt = 0u32;
        let mut committed = false;
        let mut last = NotSentReason::Deadline;
        loop {
            if self.state.borrow().complete {
                return;
            }
            if !committed && Instant::now() >= self.deadlines.0 {
                let _ = self.tx.send(Attempted::SendDeadline(last));
                return;
            }
            // A stream budget's only reply bound is the transport: the call lives as long as the
            // connection its request committed on.
            if let Some(why) = self.committed_on.as_ref().and_then(|c| c.close_reason()) {
                let _ = self.tx.send(Attempted::TransportLost(format!("the connection the request committed on closed ({why})")));
                return;
            }
            if let Some(why) = self.receiver_gone() {
                tracing::info_span!("rdm.node_rpc.delivery.remove.via-receiver-gone", delivery_id = %self.id, attempts = attempt, reason = %why)
                    .in_scope(|| tracing::info!("the receiving birth ended: the request is owed no longer"));
                let _ = self.tx.send(Attempted::ReceiverGone(why));
                return;
            }
            if self.state.borrow().acked {
                // Acked, its response not complete: hold a connection the responder can resend on.
                if let Err(why) = self.hold_connection(attempt).await {
                    let _ = self.tx.send(Attempted::TransportLost(why));
                    return;
                }
                attempt += 1;
                continue;
            }
            attempt += 1;
            self.attempts.store(attempt, std::sync::atomic::Ordering::SeqCst);
            let started = Instant::now();
            let next_at = started + pacing.after(attempt);
            // Before the commit no pause outlasts the send budget.
            let resume_at = if committed { next_at } else { next_at.min(self.deadlines.0) };
            if attempt > 1 {
                self.client.delivery.stats().resends.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.delivery.update.via-resend", delivery_id = %self.id, attempt, backoff_ms = pacing.after(attempt - 1).as_millis() as u64)
                    .in_scope(|| tracing::info!("no ack: the request is resent with the same identity"));
            }
            let stream_send = match self.opts.budget {
                Budget::Stream { send } => Some(send),
                _ => None,
            };
            let deadline = if committed { self.deadlines.1.unwrap_or(started + pacing.floor) } else { self.deadlines.0 };
            let node = match self.client.reach(&self.target) {
                Ok(n) => n,
                Err(r) if self.opts.unsent_is_final && !committed => {
                    let _ = self.tx.send(Attempted::EndedPreCommit(PreCommit::begin(self.op).not_sent(r)));
                    return;
                }
                Err(r) => {
                    last = r.clone();
                    let _ = self.tx.send(Attempted::NotSent(r));
                    self.pause(resume_at).await;
                    continue;
                }
            };
            let header = TransportHeader::Request { sender: self.id.sender.clone(), delivery_seq: self.id.delivery_seq, forget_below: self.lowest_unacked() };
            let phase = self.client.attempt(&self.target, &node, self.op, &header, &mut self.payload, &self.opts, deadline, &self.evidence).await;
            match phase {
                Phase::Done(out) => {
                    // The owner's cut, a source that admits no new connection (nothing was sent, and
                    // nothing is owed for a request this source refuses to originate), a request
                    // asked once and an owner that answers from its first unsent attempt end the
                    // call; any other unsent attempt is resent.
                    let resend = !self.opts.once
                        && !(self.opts.unsent_is_final && !committed)
                        && matches!(&out, RpcOutcome::NotSent(n) if !matches!(n.reason(), NotSentReason::FrameNotSent | NotSentReason::AdmissionBlocked(_)));
                    if resend {
                        if let RpcOutcome::NotSent(n) = out {
                            last = n.reason().clone();
                            let _ = self.tx.send(Attempted::NotSent(last.clone()));
                        }
                        self.pause(resume_at).await;
                        continue;
                    }
                    let _ = self.tx.send(Attempted::EndedPreCommit(out));
                    return;
                }
                Phase::Stopped(opened) => {
                    let _ = self.tx.send(Attempted::StoppedBeforeComplete);
                    self.read_refusal(opened).await;
                    return;
                }
                Phase::Committed(opened, frame_len) => {
                    if !committed {
                        committed = true;
                        let _ = self.tx.send(Attempted::Committed { frame_len });
                    }
                    if stream_send.is_some() {
                        self.committed_on = Some(opened.conn.clone());
                    }
                    // The wait for the ack runs from the write: a slow dial spends none of it. A
                    // request asked once is never resent: it waits on its stream for its answer.
                    let ack_by = (!self.state.borrow().acked && !self.opts.once).then(|| Instant::now() + pacing.after(attempt));
                    let resend_at = ack_by.unwrap_or(next_at);
                    match self.read(opened, ack_by).await {
                        StreamEnd::Complete | StreamEnd::Violation => return,
                        StreamEnd::Definitive(code) => {
                            let _ = self.tx.send(Attempted::Reset(code));
                            return;
                        }
                        StreamEnd::NoAck => continue,
                        StreamEnd::Lost if self.opts.once => {
                            let _ = self.tx.send(Attempted::TransportLost("asked once: its stream was lost before an answer".into()));
                            return;
                        }
                        StreamEnd::Lost => {
                            if !self.state.borrow().acked {
                                self.pause(resend_at).await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Acked, response not complete, its stream gone: keep a pooled connection to the responder,
    /// on the schedule, until the response completes there.
    /// `Err` for a call asked once, or a `Budget::Stream` call, whose stream was lost: neither waits
    /// for a resent response (the transport's liveness is a stream budget's reply bound).
    async fn hold_connection(&mut self, attempt: u32) -> Result<(), String> {
        let pacing = self.client.delivery.pacing();
        if self.opts.once {
            return Err("asked once: its stream was lost before an answer".into());
        }
        if let Some(why) = self.committed_on.as_ref().and_then(|c| c.close_reason()) {
            return Err(format!("the connection the request committed on closed ({why})"));
        }
        if let Ok(node) = self.client.reach(&self.target) {
            let key = PoolKey { scope: self.opts.scope.clone(), peer: node.peer, incarnation: node.observed.as_ref().map(|n| n.incarnation.clone()) };
            let spec = DialSpec {
                endpoint: self.client.endpoint.clone(),
                resolver: self.client.resolver.clone(),
                target: node.node.as_ref().map(|_| self.target.clone()),
                addr: node.addr,
                failpoint: None,
                opened: None,
                admission: self.client.admission.clone(),
                delivery: Some(self.client.delivery.clone()),
            };
            let dial = self.client.pool.get_or_dial(&key, spec, Instant::now() + pacing.floor);
            tokio::select! {
                _ = dial => {}
                // The invocation completed on a resent frame: nothing to hold.
                _ = self.state.changed() => {}
            }
        }
        let until = Instant::now() + pacing.after(attempt.max(1));
        self.pause(until).await;
        Ok(())
    }

    /// The refusal of a request the receiver stopped before it was complete: its response frame is
    /// delivered, and acked on an ack stream of the connection (this stream's request direction is
    /// stopped).
    async fn read_refusal(&mut self, opened: Opened) {
        let d = self.client.delivery.clone();
        let mut reader = FrameReader::new(opened.recv, Vec::new());
        while let Ok(Some(h)) = reader.header().await {
            match &h {
                TransportHeader::Ack { .. } => d.on_ack(&h),
                TransportHeader::Response { invocation_id, .. } => {
                    let Ok(body) = reader.body(d.reply_bound(invocation_id)).await else { return };
                    if let ResponseVerdict::Ack(acks) = d.on_response(&h, body, AckTo::NewStream(opened.conn.clone())) {
                        for a in acks {
                            d.ack_on_new_stream(&opened.conn, &a, AckOf::Response).await;
                        }
                    }
                }
                TransportHeader::Request { .. } => return,
            }
        }
    }

    /// Read one attempt's stream: acks of the request and response frames, delivered to the
    /// invocation and acked on the request direction. `ack_by`: when an unacked request is resent.
    async fn read(&mut self, opened: Opened, ack_by: Option<Instant>) -> StreamEnd {
        let d = self.client.delivery.clone();
        let Opened { send, recv, key, conn } = opened;
        let out = FrameOut::new(send);
        let mut reader = FrameReader::new(recv, Vec::new());
        let mut ack_by = ack_by;
        let violation = |why: &'static str| {
            tracing::info_span!("rdm.node_rpc.stream.reject.via-protocol-violation", reason = why, delivery_id = %self.id).in_scope(|| tracing::info!("424"));
            d.fail_call(&self.id, why);
        };
        loop {
            let resend_at = async {
                match ack_by {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                _ = self.state.changed() => {
                    let s = *self.state.borrow();
                    if s.complete {
                        out.finish().await;
                        return StreamEnd::Complete;
                    }
                    if s.acked {
                        ack_by = None;
                    }
                }
                h = reader.header() => match h {
                    Ok(Some(h @ TransportHeader::Ack { .. })) => d.on_ack(&h),
                    Ok(Some(h @ TransportHeader::Response { .. })) => {
                        let TransportHeader::Response { invocation_id, .. } = &h else { unreachable!() };
                        let body = match reader.body(d.reply_bound(invocation_id)).await {
                            Ok(b) => b,
                            Err(ReadEnd::Violation(why)) => {
                                violation(why);
                                out.reset(ResetCode::ProtocolViolation).await;
                                reader.stop(ResetCode::ProtocolViolation);
                                return StreamEnd::Violation;
                            }
                            Err(_) => return self.lost(&key, &conn),
                        };
                        match d.on_response(&h, body, AckTo::Stream(out.clone())) {
                            ResponseVerdict::Ack(acks) => {
                                for a in acks {
                                    d.write_ack_header(&out, &a).await;
                                }
                            }
                            ResponseVerdict::Held | ResponseVerdict::Drop => {}
                            ResponseVerdict::Violation(_) => {
                                violation("a response frame broke its invocation's order");
                                out.reset(ResetCode::ProtocolViolation).await;
                                reader.stop(ResetCode::ProtocolViolation);
                                return StreamEnd::Violation;
                            }
                        }
                    }
                    Ok(Some(TransportHeader::Request { .. })) => {
                        violation("a request frame on a request's reply direction");
                        out.reset(ResetCode::ProtocolViolation).await;
                        reader.stop(ResetCode::ProtocolViolation);
                        return StreamEnd::Violation;
                    }
                    // The responder ended its direction: after a repeat's ack, or after its terminal.
                    Ok(None) => {
                        if self.state.borrow().complete {
                            out.finish().await;
                            return StreamEnd::Complete;
                        }
                        out.finish().await;
                        return StreamEnd::Lost;
                    }
                    Err(ReadEnd::Reset(code)) if [ResetCode::UnservedOp, ResetCode::InternalRpcFailure, ResetCode::ProtocolViolation].iter().any(|c| u64::from(c.code()) == code) => {
                        return StreamEnd::Definitive(code);
                    }
                    Err(ReadEnd::Violation(why)) => {
                        violation(why);
                        out.reset(ResetCode::ProtocolViolation).await;
                        reader.stop(ResetCode::ProtocolViolation);
                        return StreamEnd::Violation;
                    }
                    Err(_) => return self.lost(&key, &conn),
                },
                () = resend_at => {
                    // The request goes out again on a new stream; this one stays open to carry the
                    // invocation's response, read beside the next attempt until the invocation
                    // completes or this stream ends. A handler slower than the ack wait answers here.
                    let keep = keep_reading(d.clone(), out, reader, self.state.clone());
                    match tracing::dispatcher::get_default(|d| d.clone()) {
                        d if d.is::<tracing::subscriber::NoSubscriber>() => drop(tokio::spawn(keep)),
                        d => drop(tokio::spawn(UnderDispatch { fut: Some(Box::pin(keep)), dispatch: d })),
                    }
                    return StreamEnd::NoAck;
                }
            }
        }
    }

    fn lost(&self, key: &PoolKey, conn: &iroh::endpoint::Connection) -> StreamEnd {
        if conn.close_reason().is_some() && self.client.pool.broken(key, conn) {
            if let (Some(o), Ok(node)) = (&self.client.observer, self.client.reach(&self.target)) {
                if let Some(n) = &node.observed {
                    o.direct_broken(n, "connection lost");
                }
            }
        }
        StreamEnd::Lost
    }
}

/// The caller's stale-target refusal: its resolver no longer names the birth the call was dialled
/// to. Only a caller decides it; no receiver refuses a call for the node id it names (R-UC1a).
/// `current` is what the resolver holds instead, in its own words.
fn stale_span(asked: &Target, current: &str) {
    tracing::info_span!(
        "rdm.node_rpc.connection.reject.via-stale-target",
        decided_by = "caller",
        node_id = %asked.node_id,
        op = asked.op,
        current,
    )
    .in_scope(|| tracing::info!("the target is stale: RejectedStale, never dispatched"));
}
