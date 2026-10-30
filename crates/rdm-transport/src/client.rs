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

/// The budget shapes (node-rpc.md §34).
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
    /// No send or reply deadline (i146 Q44): the call ends only on its reply, on the runtime's proof
    /// that the receiver is gone (exited, removed or superseded), or when its owner drops it; the
    /// resend schedule runs unchanged underneath it. For a report or completion its owner owes with
    /// no bound of its own; never a default.
    Owed,
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
    /// §36.1, and the envelope's Forward, "ConnectionLost directly from its failed dial"): a dial
    /// or reach that fails before the commit cut ends the call `NotSent`, never resent. Once
    /// committed the request is owed as any other.
    pub unsent_is_final: bool,
    /// The route an Owed call's resends take once an attempt sent nothing (node-rpc.md §43, §45; i146
    /// Q52): its owner's carrier policy, answered per attempt by the route projection. `None`: every
    /// attempt is direct.
    pub route: Option<RoutePolicy>,
}

/// An owner's carrier policy for an Owed call (node-rpc.md §43, §45; i146 Q52): given the attempt
/// number, the route the projection answers now for the call's one receiver. The receiver is the same
/// on every leg; only the leg changes.
#[derive(Clone)]
pub struct RoutePolicy(pub Arc<dyn Fn(u32) -> crate::route::RouteChoice + Send + Sync>);

impl std::fmt::Debug for RoutePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RoutePolicy")
    }
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
            route: None,
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
    /// observer was handed (`direct_failed`). A carrier answers `ConnectionLost` from this.
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
    /// An Owed dial learned that its exact receiver ended, before any commit.
    ReceiverGone(String),
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
    /// The attempt task's last word: the path it ended on, after any outcome above (i146 Q38). The
    /// call names it when its attempts end with no reply delivered.
    Ended(AttemptEnd),
}

/// The path an attempt task ended on: one per return of it (i146 Q38).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttemptEnd {
    /// The invocation was complete when the task looked, at the top of its turn.
    CompleteAtTurn,
    /// No attempt committed within the send budget (`Attempted::SendDeadline`).
    SendDeadline,
    /// The connection a `Budget::Stream` request committed on closed (`Attempted::TransportLost`).
    CommittedConnectionClosed,
    /// The receiving birth's exit or removal is proven (`Attempted::ReceiverGone`).
    ReceiverGone,
    /// Acked, its stream lost, and no resent response is waited for: asked once, or a
    /// `Budget::Stream` call whose connection closed (`Attempted::TransportLost`).
    HoldLost,
    /// The target did not resolve and an unsent attempt ends the call (`Attempted::EndedPreCommit`).
    UnreachedFinal,
    /// An attempt ended definitively before its commit cut (`Attempted::EndedPreCommit`).
    EndedPreCommit,
    /// The receiver stopped the request before it was complete, and its refusal was read.
    RefusalRead,
    /// The stream the attempt read ended with the invocation complete.
    StreamComplete,
    /// The stream the attempt read broke the protocol, and the violation was handed to the call.
    StreamViolation,
    /// The stream was reset with a code that ends the request (`Attempted::Reset`).
    Reset,
    /// A request asked once lost its stream before an answer (`Attempted::TransportLost`).
    LostAskedOnce,
}

impl AttemptEnd {
    /// The path's name, as spans and reasons carry it.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::CompleteAtTurn => "complete-at-turn",
            Self::SendDeadline => "send-deadline",
            Self::CommittedConnectionClosed => "committed-connection-closed",
            Self::ReceiverGone => "receiver-gone",
            Self::HoldLost => "hold-lost",
            Self::UnreachedFinal => "unreached-final",
            Self::EndedPreCommit => "ended-pre-commit",
            Self::RefusalRead => "refusal-read",
            Self::StreamComplete => "stream-complete",
            Self::StreamViolation => "stream-violation",
            Self::Reset => "reset",
            Self::LostAskedOnce => "lost-asked-once",
        }
    }
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
    /// block of the client a node originates its logged connections on (node-name-update.md
    /// Precondition): set when its `node.connection.delete { All }` closes them, lifted when it
    /// starts or joins its mesh again.
    pub fn block_new_connections(&self, why: impl Into<String>) {
        self.admission.block(why.into());
    }

    /// Open new connections again: the block [`Self::block_new_connections`] set is lifted.
    pub fn admit_new_connections(&self) {
        self.admission.lift();
    }

    /// Testkit seam: block this client's direct dials to the process at `peer` (its Iroh key, the
    /// pool's dial key) until [`Self::unblock_direct_dial`]. A call that would dial it ends as an
    /// unreachable dial ends, `NotSent` `Connection`; a connection already pooled to it is closed.
    /// A carried call through another peer, and every dial to any other peer, is untouched.
    /// Returns how many pooled connections were closed.
    #[cfg(feature = "testkit-gossip")]
    pub fn block_direct_dial(&self, peer: iroh::PublicKey) -> usize {
        self.pool.block_direct(peer)
    }

    /// Testkit seam: release [`Self::block_direct_dial`] for `peer`.
    #[cfg(feature = "testkit-gossip")]
    pub fn unblock_direct_dial(&self, peer: &iroh::PublicKey) {
        self.pool.unblock_direct(peer);
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
    /// reaches the stopped node on a fresh dial.
    pub fn close_pooled_to(&self, peer: &iroh::PublicKey, reason: &'static str) -> usize {
        self.pool.close_peer(peer, reason.as_bytes())
    }

    /// Close the connections this client holds to `peer` for `reason` and hand back each one closed,
    /// with no span of the pool's own: the caller names why on each connection's span (a
    /// `node.connection.delete` closing a data-plane connection is no hard cut of a pool).
    pub fn close_connections_to(&self, peer: &iroh::PublicKey, reason: &'static str) -> Vec<crate::pool::ClosedConnection> {
        self.pool.close_to(peer, reason.as_bytes())
    }

    /// The endpoint the resolver names for the node `node` now, if it resolves: the key its pooled
    /// connections to that node are held under.
    pub fn endpoint_of(&self, node: &rdm_core::NodeId) -> Option<iroh::PublicKey> {
        self.resolver.resolve(&NodeTarget::ExactNode(node.clone())).ok().map(|n| n.endpoint_id)
    }

    /// Hard-cut every connection this client holds (`node.connection.delete`): dials in flight are
    /// cancelled and every pooled connection is closed, so no call rides one and the next call dials
    /// afresh. Returns how many pooled connections were closed.
    pub fn close_pooled(&self, reason: &'static str) -> usize {
        self.pool.close_all(reason.as_bytes())
    }

    /// [`Self::close_pooled`], sparing every connection and dial to a peer in `spare`: a stop's cut
    /// keeps the connection to its mesh-primary its owed declarations ride, and the connections of
    /// the callers it answers (node-stop.md "Transport cut").
    pub fn close_pooled_except(&self, spare: &[iroh::PublicKey], reason: &'static str) -> usize {
        self.pool.close_all_except(spare, reason.as_bytes())
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

    /// The exact receiving incarnation ended; an endpoint-slot change alone is not that proof.
    fn receiver_gone(&self, target: &NodeTarget, held: &rdm_core::IncarnationId) -> Option<String> {
        match self.resolver.resolve(target) {
            Ok(n) if &n.incarnation == held => None,
            Ok(n) => Some(format!("incarnation {} superseded by {}", held.as_str(), n.incarnation.as_str())),
            Err(ResolveFailure::Gone) => Some(format!("incarnation {} removed", held.as_str())),
            Err(ResolveFailure::Unavailable) => Some(format!("incarnation {} exited: its exit is proven", held.as_str())),
            Err(_) => None,
        }
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
                let now = self.resolver.resolve(target).ok();
                // What moved under the dial: the birth, or only its endpoint slot.
                let moved = match &now {
                    Some(n) if n.incarnation.as_str() == key.birth() => format!("the endpoint slot of incarnation {} moved from {} to {}", key.birth(), key.peer.fmt_short(), n.endpoint_id.fmt_short()),
                    Some(n) => format!("incarnation {} superseded by {}", key.birth(), n.incarnation.as_str()),
                    None => format!("incarnation {} no longer resolves", key.birth()),
                };
                // The pool also invalidates a dial when its receiving birth ends. For an Owed
                // call that proof ends the obligation as receiver-gone; a moved slot of a living
                // birth is neither the holder's answer nor its exited verdict (i146 Q52), so the
                // attempt sent nothing and the next one dials the slot the view names then.
                if matches!(opts.budget, Budget::Owed) {
                    if let Some(why) = node.observed.as_ref().and_then(|n| self.receiver_gone(target, &n.incarnation)) {
                        return Phase::ReceiverGone(why);
                    }
                    tracing::info!(step = "dial-superseded", moved = %moved, "the dial's slot moved under a living birth: the owed request is resent");
                    return Phase::Done(pre.not_sent(NotSentReason::Connection(format!("the dial was superseded: {moved}"))));
                }
                // A pinned endpoint slot on a living birth remains stale.
                stale_span(&request_target, &moved);
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
        // How many of this process's calls wait on a stream open with this one: a wait shared by many
        // is the peer's stream credit (each stream is held until its reply is acked), one alone is not.
        let opening = OpeningStream::enter();
        let (mut send, recv) = match timeout_at(send_deadline, conn.open_bi()).await {
            Ok(Ok(s)) => {
                tracing::info!(step = "stream-opened", waited_ms = opened_at.elapsed().as_millis() as u64, waiting_with = opening.with, "a request stream is open");
                drop(opening);
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
                // An Owed call's attempt is bounded by the floor: it stamps what remains of that.
                Budget::Overall(_) | Budget::Owed => send_deadline.saturating_duration_since(Instant::now()),
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
            code = tracing::field::Empty,
            detail = tracing::field::Empty,
            reason = tracing::field::Empty,
            reused = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
            delivery_id = tracing::field::Empty,
            attempts = tracing::field::Empty,
            delivery = tracing::field::Empty,
        );
        let started = Instant::now();
        let mut guard = CallSpanGuard::new(&span);
        let (out, evidence) = self.owe_steps(target, final_target, op, payload, max_reply, opts, decode, &span).instrument(span.clone()).await;
        guard.complete();
        span.record("outcome", out.name());
        span.record("code", out.code().name());
        span.record("detail", out.detail_name());
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
        deadlines: (Option<Instant>, Option<Instant>),
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

    /// [`Self::owe_steps`]'s wait for an Owed request's receiver this caller's view does not hold yet
    /// (`Unknown`, node-rpc.md §43: "never on absence from a view"): read again at the resend pacing
    /// (first at the publish cadence, then backoff to the floor) and on every change of the view.
    /// Returns once the receiver resolves, or once the view holds it proven gone or exited, which ends
    /// the request as before.
    async fn until_resolved(&self, target: &NodeTarget, span: &tracing::Span) {
        let pacing = self.delivery.pacing();
        let mut changes = self.resolver.changes();
        let mut waits = 0u32;
        loop {
            if let Some(c) = changes.as_mut() {
                c.borrow_and_update();
            }
            if !matches!(self.reach(target), Err(NotSentReason::Resolve(ResolveFailure::Unknown))) {
                return;
            }
            if waits == 0 {
                span.in_scope(|| tracing::info!(step = "unresolved", "the owed request's receiver is not in this caller's view yet: it stays owed and is opened once it resolves"));
            }
            waits += 1;
            let moved = async {
                // A view that no longer reports changes leaves the pacing alone to read it again.
                let changed = match changes.as_mut() {
                    Some(c) => c.changed().await.is_ok(),
                    None => false,
                };
                if !changed {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                () = tokio::time::sleep(pacing.after(waits)) => {}
                () = moved => {}
            }
        }
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
        let (send_deadline, overall) = deadlines_of(opts.budget, start);
        let after_commit = |at: Instant| match (opts.budget, overall) {
            (_, Some(d)) => Some(d),
            (Budget::Split { reply, .. }, None) => Some(at + reply),
            (Budget::Stream { .. } | Budget::Owed, None) => None,
            (Budget::Overall(_), None) => unreachable!(),
        };
        // node-rpc.md §43: an Owed request is never ended by its receiver's absence from this caller's
        // view. Its delivery identity is minted against the receiver's endpoint, so a receiver the view
        // does not hold yet is waited for before anything is minted; only the owner's drop ends the wait.
        if matches!(opts.budget, Budget::Owed) {
            self.until_resolved(target, span).await;
        }
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
        // The path the attempt task ended on, and whether the invocation's frame channel closed with no
        // frame: named when the attempts end (i146 Q38).
        let mut attempt_end: Option<AttemptEnd> = None;
        let mut slot_closed = false;
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
                f = owed.frames.recv(), if !slot_closed => match f {
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
                    // Closed with no frame: not polled again, so the attempts' end is heard.
                    None => {
                        slot_closed = true;
                        continue;
                    }
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
                    Some(Attempted::Ended(path)) => attempt_end = Some(path),
                    None => {
                        let path = match (attempt_end, slot_closed) {
                            (Some(p), false) => p.name().to_string(),
                            (Some(p), true) => format!("{}, the invocation's frame channel closed with no frame", p.name()),
                            (None, true) => "slot-closed: the invocation's frame channel closed with no frame".to_string(),
                            (None, false) => "no path reported: the attempt task ended without naming one".to_string(),
                        };
                        break match (committed.take(), stopped.take(), pre.take()) {
                            (Some(c), _, _) => c.indeterminate(IndeterminateReason::ReplyLost(format!("the request's attempts ended ({path})"))),
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

/// The send deadline (`None` for an Owed call) and the overall deadline of a call made at `start`
/// under `budget`.
pub(crate) fn deadlines_of(budget: Budget, start: Instant) -> (Option<Instant>, Option<Instant>) {
    match budget {
        Budget::Overall(d) => (Some(start + d), Some(start + d)),
        Budget::Split { send, .. } | Budget::Stream { send } => (Some(start + send), None),
        Budget::Owed => (None, None),
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
    /// A carried attempt's carrier answered that it did not reach the call's receiver (its
    /// `ForwardReply` names why): the request is not acked and is resent (i146 Q52).
    CarrierFailed(String),
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
    /// The send deadline (`None`: an Owed call has none) and the overall one.
    deadlines: (Option<Instant>, Option<Instant>),
    evidence: Arc<Mutex<CallEvidence>>,
    tx: mpsc::UnboundedSender<Attempted>,
    state: watch::Receiver<SlotState>,
    attempts: Arc<std::sync::atomic::AtomicU32>,
    /// The connection a `Budget::Stream` request committed on: the call lives as long as it does.
    committed_on: Option<iroh::endpoint::Connection>,
}

/// A carrier's response frame `h` as the answer to `invocation`, the request it carried (i146 Q52):
/// the frame keeps its own identity, so its ack is the carrier's.
fn answering(h: TransportHeader, invocation: DeliveryId) -> TransportHeader {
    match h {
        TransportHeader::Response { sender, delivery_seq, forget_below, seq, .. } => TransportHeader::Response { sender, delivery_seq, forget_below, invocation_id: invocation, seq },
        other => other,
    }
}

impl Attempts {
    fn receiver_gone(&self) -> Option<String> {
        self.client.receiver_gone(&self.target, self.birth.as_ref()?)
    }

    /// The request as one carrier's Forward to the call's receiver, `remaining` the bound its inner
    /// call is told, the inner call keeping the request's own identity (i146 Q52). A request built at
    /// its write, or a target that names no exact receiver, is not carried.
    fn forward_payload(&self, remaining: Duration) -> Result<Payload, String> {
        let NodeTarget::ExactNode(receiver) = &self.target else { return Err("a carried attempt needs an exact receiver".into()) };
        let Payload::Ready(inner) = &self.payload else { return Err("the request is built at its write".into()) };
        let owed = rdm_core::forward::OwedDelivery { delivery_seq: self.id.delivery_seq, forget_below: self.lowest_unacked() };
        let forward = rdm_core::forward::ForwardRequest::Forward { target: receiver.clone(), inner_op: self.op, inner: inner.clone(), remaining_ms: remaining.as_millis() as u64, owed: Some(owed) };
        <rdm_core::forward::Forward as rdm_core::protocol::NodeProtocol>::encode_request(&forward).map(Payload::Ready).map_err(|e| e.0)
    }

    fn end_receiver_gone(&self, why: String, attempts: u32) -> AttemptEnd {
        tracing::info_span!("rdm.node_rpc.delivery.remove.via-receiver-gone", delivery_id = %self.id, attempts, reason = %why)
            .in_scope(|| tracing::info!("the receiving birth ended: the request is owed no longer"));
        let _ = self.tx.send(Attempted::ReceiverGone(why));
        AttemptEnd::ReceiverGone
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

    /// The request's attempts, then the path they ended on: every return is spanned and told to the
    /// call, after any outcome of its own (i146 Q38).
    async fn run(mut self) {
        let end = self.until_end().await;
        let attempts = self.attempts.load(std::sync::atomic::Ordering::SeqCst);
        tracing::info_span!("rdm.node_rpc.delivery.update.via-attempt-ended", delivery_id = %self.id, attempts, path = end.name())
            .in_scope(|| tracing::info!("the request's attempt task ended"));
        let _ = self.tx.send(Attempted::Ended(end));
    }

    async fn until_end(&mut self) -> AttemptEnd {
        let pacing = self.client.delivery.pacing();
        let mut attempt = 0u32;
        let mut committed = false;
        let mut last = NotSentReason::Deadline;
        // The last attempt sent nothing to the receiver: the next one asks the owner's carrier policy
        // (i146 Q52).
        let mut sent_nothing = false;
        loop {
            if self.state.borrow().complete {
                return AttemptEnd::CompleteAtTurn;
            }
            if !committed && self.deadlines.0.is_some_and(|d| Instant::now() >= d) {
                let _ = self.tx.send(Attempted::SendDeadline(last));
                return AttemptEnd::SendDeadline;
            }
            // A stream budget's only reply bound is the transport: the call lives as long as the
            // connection its request committed on.
            if let Some(why) = self.committed_on.as_ref().and_then(|c| c.close_reason()) {
                let _ = self.tx.send(Attempted::TransportLost(format!("the connection the request committed on closed ({why})")));
                return AttemptEnd::CommittedConnectionClosed;
            }
            if let Some(why) = self.receiver_gone() {
                return self.end_receiver_gone(why, attempt);
            }
            if self.state.borrow().acked {
                // Acked, its response not complete: hold a connection the responder can resend on.
                if let Err(why) = self.hold_connection(attempt).await {
                    let _ = self.tx.send(Attempted::TransportLost(why));
                    return AttemptEnd::HoldLost;
                }
                attempt += 1;
                continue;
            }
            attempt += 1;
            self.attempts.store(attempt, std::sync::atomic::Ordering::SeqCst);
            let started = Instant::now();
            let next_at = started + pacing.after(attempt);
            // Before the commit no pause outlasts the send budget.
            let resume_at = match self.deadlines.0 {
                Some(d) if !committed => next_at.min(d),
                _ => next_at,
            };
            if attempt > 1 {
                self.client.delivery.stats().resends.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.delivery.update.via-resend", delivery_id = %self.id, attempt, backoff_ms = pacing.after(attempt - 1).as_millis() as u64)
                    .in_scope(|| tracing::info!("no ack: the request is resent with the same identity"));
            }
            let stream_send = match self.opts.budget {
                Budget::Stream { send } => Some(send),
                _ => None,
            };
            // One attempt of an Owed call, as one after the commit, is bounded by the floor.
            let deadline = match (committed, self.deadlines) {
                (false, (Some(send), _)) => send,
                (true, (_, Some(overall))) => overall,
                _ => started + pacing.floor,
            };
            // The leg of this attempt (node-rpc.md §43, §45; i146 Q52): direct first; once an attempt sent
            // nothing, the owner's carrier policy answers per attempt, every leg aimed at the same receiver
            // under the same delivery identity.
            let leg = match (&self.opts.route, sent_nothing) {
                (Some(policy), true) => (policy.0)(attempt),
                _ => crate::route::RouteChoice::Direct,
            };
            let mut carried = match &leg {
                crate::route::RouteChoice::Direct => None,
                crate::route::RouteChoice::ViaPeer { carrier, path } => match self.forward_payload(pacing.floor) {
                    Ok(p) => Some((NodeTarget::ExactNode(carrier.clone()), path.to_string(), p)),
                    Err(why) => {
                        tracing::info_span!("rdm.node_rpc.delivery.reject.via-carried-leg", delivery_id = %self.id, attempt, carrier = %path, reason = %why)
                            .in_scope(|| tracing::info!("the attempt is not carried: it goes direct"));
                        None
                    }
                },
                crate::route::RouteChoice::NoActiveRoute => {
                    last = NotSentReason::NoActiveRoute;
                    let _ = self.tx.send(Attempted::NotSent(last.clone()));
                    sent_nothing = true;
                    self.pause(resume_at).await;
                    continue;
                }
            };
            let target = carried.as_ref().map(|(t, _, _)| t.clone()).unwrap_or_else(|| self.target.clone());
            let node = match self.client.reach(&target) {
                Ok(n) => n,
                Err(r) if self.opts.unsent_is_final && !committed => {
                    let _ = self.tx.send(Attempted::EndedPreCommit(PreCommit::begin(self.op).not_sent(r)));
                    return AttemptEnd::UnreachedFinal;
                }
                Err(r) => {
                    last = r.clone();
                    let _ = self.tx.send(Attempted::NotSent(r));
                    sent_nothing = true;
                    self.pause(resume_at).await;
                    continue;
                }
            };
            // A carried attempt is a Forward invocation of its own toward the carrier, written once: a
            // carrier that answered one attempt carries the next (i146 Q52).
            let outer = carried.as_ref().map(|_| self.client.delivery.mint_once(&[node.peer]));
            let header = match &outer {
                Some(o) => TransportHeader::Request { sender: o.id().sender.clone(), delivery_seq: o.id().delivery_seq, forget_below: self.client.delivery.lowest_unacked(&[node.peer]) },
                None => TransportHeader::Request { sender: self.id.sender.clone(), delivery_seq: self.id.delivery_seq, forget_below: self.lowest_unacked() },
            };
            let phase = match carried.as_mut() {
                Some((carrier_target, carrier, payload)) => {
                    tracing::info_span!("rdm.node_rpc.delivery.update.via-carried-attempt", delivery_id = %self.id, attempt, carrier = %carrier, forward_id = %header.delivery_id())
                        .in_scope(|| tracing::info!("the owed request is carried to its receiver through a live peer"));
                    self.client.attempt(carrier_target, &node, <rdm_core::forward::Forward as rdm_core::protocol::NodeProtocol>::OP, &header, payload, &self.opts, deadline, &self.evidence).await
                }
                None => self.client.attempt(&self.target, &node, self.op, &header, &mut self.payload, &self.opts, deadline, &self.evidence).await,
            };
            match phase {
                Phase::ReceiverGone(why) => return self.end_receiver_gone(why, attempt),
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
                        sent_nothing = true;
                        self.pause(resume_at).await;
                        continue;
                    }
                    let _ = self.tx.send(Attempted::EndedPreCommit(out));
                    return AttemptEnd::EndedPreCommit;
                }
                Phase::Stopped(opened) => {
                    let _ = self.tx.send(Attempted::StoppedBeforeComplete);
                    self.read_refusal(opened).await;
                    return AttemptEnd::RefusalRead;
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
                    // A carried attempt waits for its carrier's answer, which the carrier gives inside
                    // the `remaining_ms` it was told.
                    let ack_by = match &outer {
                        Some(_) => Some(Instant::now() + pacing.floor),
                        None => (!self.state.borrow().acked && !self.opts.once).then(|| Instant::now() + pacing.after(attempt)),
                    };
                    let resend_at = ack_by.unwrap_or(next_at);
                    sent_nothing = false;
                    match self.read(opened, ack_by, outer.as_ref().map(|o| o.id())).await {
                        StreamEnd::Complete => return AttemptEnd::StreamComplete,
                        StreamEnd::Violation => return AttemptEnd::StreamViolation,
                        StreamEnd::Definitive(code) => {
                            let _ = self.tx.send(Attempted::Reset(code));
                            return AttemptEnd::Reset;
                        }
                        StreamEnd::NoAck => continue,
                        StreamEnd::Lost if self.opts.once => {
                            let _ = self.tx.send(Attempted::TransportLost("asked once: its stream was lost before an answer".into()));
                            return AttemptEnd::LostAskedOnce;
                        }
                        StreamEnd::Lost => {
                            if !self.state.borrow().acked {
                                self.pause(resend_at).await;
                            }
                        }
                        // A carrier failure never ends the call (i146 Q52): the request is unacked, and the
                        // next attempt asks the owner's carrier policy again.
                        StreamEnd::CarrierFailed(why) => {
                            tracing::info_span!("rdm.node_rpc.delivery.update.via-carrier-failed", delivery_id = %self.id, attempt, reason = %why)
                                .in_scope(|| tracing::info!("the carrier did not reach the receiver: the request is resent"));
                            sent_nothing = true;
                            self.pause(resend_at).await;
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
    /// `carried`: the attempt's own Forward invocation toward its carrier, whose answer wraps the
    /// receiver's.
    async fn read(&mut self, opened: Opened, ack_by: Option<Instant>, carried: Option<&DeliveryId>) -> StreamEnd {
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
                        // A carried attempt's answer is its carrier's Forward reply around the receiver's.
                        let forward = carried.filter(|o| *o == invocation_id);
                        let bound = if forward.is_some() { d.reply_bound(&self.id).max(<rdm_core::forward::Forward as rdm_core::protocol::NodeProtocol>::MAX_REPLY_FRAME_BYTES) } else { d.reply_bound(invocation_id) };
                        let body = match reader.body(bound).await {
                            Ok(b) => b,
                            Err(ReadEnd::Violation(why)) => {
                                violation(why);
                                out.reset(ResetCode::ProtocolViolation).await;
                                reader.stop(ResetCode::ProtocolViolation);
                                return StreamEnd::Violation;
                            }
                            Err(_) => return self.lost(&key, &conn),
                        };
                        // The receiver's answer is handed on as the call's response, its frame acked to the
                        // carrier; anything else is the carrier's failure to reach it: acked to the carrier,
                        // the call's request left unacked (i146 Q52).
                        let (h, body) = if forward.is_some() {
                            match <rdm_core::forward::Forward as rdm_core::protocol::NodeProtocol>::decode_reply(&body) {
                                Ok(rdm_core::forward::ForwardReply::Forwarded { inner }) => (answering(h, self.id.clone()), inner),
                                Ok(other) => {
                                    d.write_ack_header(&out, &h.ack()).await;
                                    return StreamEnd::CarrierFailed(format!("{other:?}"));
                                }
                                Err(e) => {
                                    d.write_ack_header(&out, &h.ack()).await;
                                    return StreamEnd::CarrierFailed(format!("the carrier's reply does not decode: {e:?}"));
                                }
                            }
                        } else {
                            (h, body)
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
                        // A frame this stream delivered and its owner has not recovered is acked on
                        // this direction (node-rpc.md §31, §43), and the responder reads the
                        // direction's finish as the caller done with the invocation: it stays open
                        // until the owner has recovered every frame and the invocation completes, or
                        // the connection closes. A direction that delivered no such frame (a repeat's
                        // ack) is finished at once, the stream lost.
                        if !self.state.borrow().complete && d.unrecovered_on(&self.id, &out) {
                            let mut state = self.state.clone();
                            tokio::select! {
                                _ = state.wait_for(|s| s.complete) => {}
                                _ = conn.closed() => {}
                            }
                        }
                        if self.state.borrow().complete {
                            out.finish().await;
                            return StreamEnd::Complete;
                        }
                        out.finish().await;
                        if conn.close_reason().is_some() {
                            return self.lost(&key, &conn);
                        }
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

/// Records the caller's typed cancellation when its future is dropped before an outcome exists.
pub(crate) struct CallSpanGuard { span: tracing::Span, started: Instant, done: bool }
impl CallSpanGuard {
    pub(crate) fn new(span: &tracing::Span) -> Self { Self { span: span.clone(), started: Instant::now(), done: false } }
    pub(crate) fn complete(&mut self) { self.done = true; }
}
impl Drop for CallSpanGuard {
    fn drop(&mut self) {
        if !self.done {
            self.span.record("code", "CANCELLED").record("detail", "caller-gone").record("elapsed_ms", self.started.elapsed().as_millis() as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::framing::Birth;
    use rdm_core::{IncarnationId, NodeId};

    fn birth() -> Birth {
        Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }
    }

    /// CONTRACT (i146 Q52): the carrier's response frame to a carried attempt answers the request it
    /// carried: only its `invocation_id` becomes the request's, and the frame keeps its own identity, so
    /// its ack names the carrier's frame and the carrier settles it; any frame that is not a response is
    /// unchanged. Must NOT happen: the ack naming the carried request, or the frame's sequence moved.
    #[test]
    fn a_carriers_response_answers_the_carried_request_and_keeps_its_own_identity() {
        let (origin, carrier) = (birth(), birth());
        let attempt = DeliveryId { sender: origin.clone(), delivery_seq: 7 };
        let request = DeliveryId { sender: origin, delivery_seq: 3 };
        let frame = TransportHeader::Response { sender: carrier.clone(), delivery_seq: 11, forget_below: 5, invocation_id: attempt, seq: 0 };
        let answered = answering(frame.clone(), request.clone());
        assert_eq!(answered, TransportHeader::Response { sender: carrier.clone(), delivery_seq: 11, forget_below: 5, invocation_id: request.clone(), seq: 0 }, "it answers the carried request");
        assert_eq!(answered.ack(), frame.ack(), "its ack names the carrier's frame");
        let ack = TransportHeader::Ack { sender: carrier, delivery_seq: 11 };
        assert_eq!(answering(ack.clone(), request), ack, "an ack is unchanged");
    }
}

/// The calls of this process waiting on a stream open now (`stream-opened`'s `waiting_with`).
static OPENING: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// One call's wait on a stream open: counted while it lasts.
struct OpeningStream {
    /// The calls waiting when this one began, itself included.
    with: usize,
}

impl OpeningStream {
    fn enter() -> Self {
        Self { with: OPENING.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 }
    }
}

impl Drop for OpeningStream {
    fn drop(&mut self) {
        OPENING.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
