//! The Node RPC client (node-rpc.md §20–§24, §43; ownership amendment §6).
//!
//! One call is one request to one exact target (node-rpc.md §43): its identity is minted once, and
//! the runtime resends it with that identity on the schedule until it is acked, its receiver's exit
//! is proven, its owner ends it, or its attempts are spent. A call's `Send` is its owner's bound:
//! when its attempts are spent the call ends `NotSent` with the last reason if no attempt crossed
//! the commit cut, `Indeterminate` otherwise (Q311). Each attempt is resolve, the ordinary dial,
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

/// How long one attempt of a call may take (node-rpc.md §34; Q311): its resolve, dial and complete
/// send, at most [`TRANSPORT_LIMIT`] of it, and once the request is acked its reply, counted from the
/// ack. Ten seconds for a call its receiver answers at once; the receiver's own bound plus ten seconds
/// where it has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeLimit(pub Duration);

/// How a call is resent (node-rpc.md §34, §43; Q311, Q315): how many attempts it makes in all, the
/// first send included, and how long one attempt may take. Set by whoever opens the call;
/// [`Send::default`] is RDM's values, three attempts and ten seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Send {
    /// Attempts in all, the first send included (a call addressed to a primary: per birth the view
    /// names for the role, Q313).
    pub attempts: u8,
    /// How long one attempt may take.
    pub time_limit: TimeLimit,
}

impl Default for Send {
    fn default() -> Self {
        Self { attempts: 3, time_limit: TimeLimit(Duration::from_secs(10)) }
    }
}

impl Send {
    /// The default attempts with the time limit `d`.
    pub fn with_time_limit(d: Duration) -> Self {
        Self { time_limit: TimeLimit(d), ..Self::default() }
    }

    /// `call` made under this Send, a call that is not a Node RPC call (a store's read or write; Q344:
    /// one `Send` on every call, the only retry mechanism): at most `attempts` attempts, each bounded by
    /// the time limit, each after the first after its delay on [`crate::Pacing::from_cadences`]
    /// (node-rpc.md §43). An error `answered` reads as an answer (asked again, it answers the same) ends
    /// the call at once and is never sent again.
    pub async fn run<T, E, F, Fut>(&self, answered: impl Fn(&E) -> bool, call: F) -> Result<T, SendEnd<E>>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        self.run_paced(crate::delivery::Pacing::from_cadences(), answered, call).await
    }

    /// [`Send::run`] on the schedule `pacing` (a cell turns its jitter off, as a [`crate::Delivery`]'s).
    pub async fn run_paced<T, E, F, Fut>(&self, pacing: crate::delivery::Pacing, answered: impl Fn(&E) -> bool, mut call: F) -> Result<T, SendEnd<E>>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let attempts = self.attempts.max(1);
        let mut last = None;
        for n in 1..=attempts {
            if n > 1 {
                tokio::time::sleep(pacing.after(u32::from(n - 1))).await;
            }
            match tokio::time::timeout(self.time_limit.0, call()).await {
                Ok(Ok(v)) => return Ok(v),
                Ok(Err(e)) if answered(&e) => return Err(SendEnd::Answered(e)),
                Ok(Err(e)) => last = Some(e),
                Err(_) => last = None,
            }
        }
        Err(SendEnd::Spent { attempts, time_limit: self.time_limit, last })
    }
}

/// How a call made under a [`Send`] ([`Send::run`]) ended without its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendEnd<E> {
    /// The call answered with an error that is an answer: it was not sent again.
    Answered(E),
    /// Its attempts are spent: the last attempt's error, `None` when that attempt passed its time limit.
    Spent {
        /// The attempts made.
        attempts: u8,
        /// The time limit of each.
        time_limit: TimeLimit,
        /// The last attempt's error.
        last: Option<E>,
    },
}

impl<E: std::fmt::Display> std::fmt::Display for SendEnd<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Answered(e) => write!(f, "{e}"),
            Self::Spent { attempts, last: Some(e), .. } => write!(f, "{e} (its Send spent: {attempts} attempts)"),
            Self::Spent { attempts, time_limit, last: None } => write!(f, "its last attempt passed its time limit of {} ms (its Send spent: {attempts} attempts)", time_limit.0.as_millis()),
        }
    }
}

/// The transport's share of an attempt (Q311, "plus 10 s for the transport"): the most an attempt's
/// resolve, dial and complete send take, whatever its time limit. A limit under it bounds them too.
pub const TRANSPORT_LIMIT: Duration = Duration::from_secs(10);

/// How one call is made: its resend, its scope, its observability context and its failpoints.
#[derive(Debug, Clone)]
pub struct CallOptions {
    /// How the call is resent: its attempts and the time limit of one attempt.
    pub send: Send,
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
    /// The owner answers from its first unsent attempt (a carrier's one inner call: node-rpc.md
    /// §36.1, and the envelope's Forward, "ConnectionLost directly from its failed dial"): a dial
    /// or reach that fails before the commit cut ends the call `NotSent`, never resent. Once
    /// committed the request is resent as any other.
    pub unsent_is_final: bool,
    /// The route a call's resends take once an attempt sent nothing (node-rpc.md §43, §45; i146
    /// Q52): its owner's carrier policy, answered per attempt by the route projection. `None`: every
    /// attempt is direct.
    pub route: Option<RoutePolicy>,
}

/// An owner's carrier policy for a call (node-rpc.md §43, §45; i146 Q52): given the attempt
/// number, the route the projection answers now for the call's one receiver. The receiver is the same
/// on every leg; only the leg changes.
#[derive(Clone)]
pub struct RoutePolicy(pub Arc<dyn Fn(u32) -> crate::route::RouteChoice + std::marker::Send + Sync>);

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
            send: Send::default(),
            cut_before_finish: false,
            scope: None,
            after_connect: None,
            context: None,
            carried: None,
            unsent_is_final: false,
            route: None,
        }
    }
}

/// The request bytes of one call: encoded up front, or built at the write from what remains of the
/// attempt's time limit.
pub(crate) enum Payload {
    Ready(Vec<u8>),
    AtWrite(Box<dyn FnOnce(Duration) -> Result<Vec<u8>, String> + std::marker::Send>),
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
pub trait ConnectionObserver: std::marker::Send + Sync {
    /// A new pooled connection to `node` opened (a reused one is not reported).
    fn direct_connected(&self, node: &ResolvedNode);
    /// A dial to `node` ended with no connection: `reason` is the NotSent reason's text.
    fn direct_failed(&self, node: &ResolvedNode, reason: &str);
    /// The pooled connection to `node` broke after it had opened.
    fn direct_broken(&self, node: &ResolvedNode, reason: &str);
}

/// Why a call this client opened ended with its attempts spent (fabric-node-hooks.md
/// `on_call_failed`; Q314): the end its span stores, and nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallFailedReason {
    /// Nothing reached the receiver: the last attempt's reason.
    NotSent(NotSentReason),
    /// The request crossed the commit cut and no answer came.
    Indeterminate(IndeterminateReason),
}

impl CallFailedReason {
    /// The end's name, as the call's span carries it: `NotSent` or `Indeterminate`.
    pub fn name(&self) -> &'static str {
        match self {
            Self::NotSent(_) => "NotSent",
            Self::Indeterminate(_) => "Indeterminate",
        }
    }
}

/// How a call this client opened ended, as its [`CallEndObserver`] is told it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallEnd {
    /// Its attempts are spent: its stored end (node-rpc.md §43).
    Spent(CallFailedReason),
    /// The birth it was addressed to answered it: a typed reply (node-recovery.md step 1, Q340: a
    /// spent-call mark ends when the exact birth answers a call of the primary that holds it).
    Answered,
}

impl CallEnd {
    /// The end's name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Spent(r) => r.name(),
            Self::Answered => "Answered",
        }
    }
}

/// What a node is told of how its own calls end (node-rpc.md §43; fabric-node-hooks.md
/// `on_call_failed`): the client reports the fact, once per call, and decides nothing. A unary call is
/// told when its attempts are spent or when its receiver answered it; a stream call when its attempts
/// are spent. A carried call is the origin's, never the carrier's: it is not reported.
pub trait CallEndObserver: std::marker::Send + Sync {
    /// A call this client opened ended `end`, its outcome already its owner's: `receiver` is the birth
    /// it was addressed to (none for a call that names no birth, or whose receiver never resolved).
    /// Told on the call's own task for every answered call: an observer does its `Answered` work at
    /// once, without a task of its own.
    fn call_ended(&self, receiver: Option<&ResolvedNode>, end: CallEnd);
}

/// Where a client tells how its calls end so the primary above a receiver marks or is reported a birth
/// whose call's attempts are spent, and ends a spent-call mark on a birth that answered (node-rpc.md
/// §43; Q311, Q340): set once, when the process holds what it decides with, and shared by every clone
/// of the client and every client handed the same slot. A spent call is told at the same point as the
/// application's `on_call_failed` ([`NodeRpcClient::with_call_failed_observer`]).
pub type CallEndsSlot = Arc<std::sync::OnceLock<Arc<dyn CallEndObserver>>>;

/// A connection this process's server accepted from a live node (connections.md section 10,
/// R-CN1): a recovery trigger for this node's own direct dial toward that exact birth, never
/// Direct evidence. The acceptor writes nothing to its connections log.
pub trait AcceptTrigger: std::marker::Send + Sync {
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
    /// Told of each call this client opened that ends with its attempts spent, and of no other end,
    /// when the node registered `on_call_failed`.
    call_failed: Option<Arc<dyn CallEndObserver>>,
    /// Where how a call of this client ends is told, for the primary above its receiver (node-rpc.md
    /// §43; Q311, Q340): empty for a client whose calls' ends start and end nothing
    /// ([`Self::without_call_ends`]).
    call_ends: CallEndsSlot,
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
    /// The receiving birth's exit or removal is proven, or the primary role the call is addressed
    /// to has no birth: the request is resent no longer.
    ReceiverGone(String),
    /// The call's attempts are spent with no answer (Q311): it ends `NotSent` with the last reason
    /// when no attempt committed, `Indeterminate` otherwise.
    Spent(Option<String>),
    /// A stream op, acked, lost the stream its frames rode, and its invocation did not complete
    /// within the call's time limit from that loss: the stream ends.
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
    /// The call's attempts are spent with no answer (`Attempted::Spent`).
    AttemptsSpent,
    /// The connection a stream's request committed on closed, its protocol bounding it by that
    /// connection (`Attempted::TransportLost`).
    CommittedConnectionClosed,
    /// The receiving birth's exit or removal is proven (`Attempted::ReceiverGone`).
    ReceiverGone,
    /// Acked, its stream lost, and the invocation not complete within the call's time limit from the
    /// loss: a stream op held past its limit (`Attempted::TransportLost`).
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
}

impl AttemptEnd {
    /// The path's name, as spans and reasons carry it.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::CompleteAtTurn => "complete-at-turn",
            Self::AttemptsSpent => "attempts-spent",
            Self::CommittedConnectionClosed => "committed-connection-closed",
            Self::ReceiverGone => "receiver-gone",
            Self::HoldLost => "hold-lost",
            Self::UnreachedFinal => "unreached-final",
            Self::EndedPreCommit => "ended-pre-commit",
            Self::RefusalRead => "refusal-read",
            Self::StreamComplete => "stream-complete",
            Self::StreamViolation => "stream-violation",
            Self::Reset => "reset",
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
            call_failed: None,
            call_ends: CallEndsSlot::default(),
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
                    closed: None,
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

    /// Hand this client what its node is told of a call that ends with its attempts spent
    /// (fabric-node-hooks.md `on_call_failed`): each such call of this client is reported there once.
    pub fn with_call_failed_observer(mut self, observer: Arc<dyn CallEndObserver>) -> Self {
        self.call_failed = Some(observer);
        self
    }

    /// Tell how the call ended, once, never for a carried call (the origin's, not this node's):
    /// `NotSent` and `Indeterminate` are spent, told to the call-ends slot and to `on_call_failed`; a
    /// reply is `Answered`, told to the call-ends slot alone. Any other end is told nowhere.
    pub(crate) fn tell_call_end<R>(&self, opts: &CallOptions, receiver: Option<&ResolvedNode>, out: &RpcOutcome<R>) {
        if opts.carried.is_some() {
            return;
        }
        let end = match out {
            RpcOutcome::NotSent(n) => CallEnd::Spent(CallFailedReason::NotSent(n.reason().clone())),
            RpcOutcome::Indeterminate(i) => CallEnd::Spent(CallFailedReason::Indeterminate(i.reason().clone())),
            RpcOutcome::Reply(_) => CallEnd::Answered,
            _ => return,
        };
        self.tell_end(receiver, end);
    }

    /// Tell a call's end to the primary above its receiver and, when spent, to `on_call_failed`.
    pub(crate) fn tell_end(&self, receiver: Option<&ResolvedNode>, end: CallEnd) {
        // The primary above the receiver marks it or is reported it, or ends its mark (node-rpc.md §43;
        // Q311, Q340).
        if let Some(ends) = self.call_ends.get() {
            ends.call_ended(receiver, end.clone());
        }
        if let (CallEnd::Spent(_), Some(observer)) = (&end, self.call_failed.as_ref()) {
            observer.call_ended(receiver, end);
        }
    }

    /// Tell how a call ends where `slot` tells it: a process whose clients share one slot sets it once.
    pub fn with_call_ends(mut self, slot: CallEndsSlot) -> Self {
        self.call_ends = slot;
        self
    }

    /// Where this client tells how a call ends; set it once the process holds what it decides with.
    pub fn call_ends(&self) -> &CallEndsSlot {
        &self.call_ends
    }

    /// This client, its pool and delivery shared, whose calls' ends start and end nothing at the primary
    /// above their receiver: node investigation's own probes, which never mark the birth they
    /// investigate (node-rpc.md §43); the investigation ends a mark on its own answer.
    pub fn without_call_ends(&self) -> Self {
        Self { call_ends: CallEndsSlot::default(), ..self.clone() }
    }

    /// [`Self::without_call_ends`], telling no `on_call_failed` either: the client a
    /// `ReportCallFailed` is sent on, since nothing reports a report and RDM's own report is not an
    /// application's call (node-rpc.md §43).
    pub fn for_reports(&self) -> Self {
        Self { call_failed: None, ..self.without_call_ends() }
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

    /// The exact receiving incarnation ended; an endpoint-slot change alone is not that proof. For a
    /// call addressed to a primary the role's next birth is no end (Q313): only a role the view
    /// names no birth for is.
    fn receiver_gone(&self, target: &NodeTarget, held: &rdm_core::IncarnationId) -> Option<String> {
        if let NodeTarget::Primary { mesh } = target {
            return self.resolver.resolve(target).err().map(|f| role_has_no_birth(mesh.as_deref(), &f));
        }
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
                    tracing::info!(step = "resolved", node_path = %n.path, node_name = %n.name, node_id = %n.node_id, peer = %n.endpoint_id.fmt_short(), addr = %n.transport_addr, incarnation = %n.incarnation.as_str(), "the target resolved to one birth");
                    // Whatever this node's record no longer names leaves the pool now.
                    self.pool.purge_stale(&n);
                    Reach { peer: n.endpoint_id, addr: n.transport_addr, observed: Some(n.clone()), node: Some(n) }
                }
                Err(f) => return Err(NotSentReason::Resolve(f)),
            },
        })
    }

    /// One attempt up to the commit cut: the ordinary dial, open_bi and the complete request frame
    /// `header | sections`, bounded by `send_deadline`. Ends `Done` with a pre-commit outcome, or
    /// `Committed` holding both directions of the stream; the request direction is not finished. A
    /// request built at its write is told what remains until `limit_at`, the attempt's time limit.
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
        limit_at: Instant,
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
                (Some(o), Some(n)) => Some(Arc::new(move || o.direct_connected(&n)) as Arc<dyn Fn() + std::marker::Send + Sync>),
                _ => None,
            },
            // The same connection's close, told once, whether or not a call rides it then.
            closed: match (self.observer.clone(), node.observed.clone()) {
                (Some(o), Some(n)) => Some(Arc::new(move || o.direct_broken(&n, "connection lost")) as Arc<dyn Fn() + std::marker::Send + Sync>),
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
                // The pool also invalidates a dial when its receiving birth ends: the caller's
                // stale-target refusal, decided before anything is sent (node-rpc.md §20.1). A moved
                // slot of a living birth (or, for a call addressed to a primary, the role's next
                // birth) is neither the receiver's answer nor its exited verdict (i146 Q52, Q313), so
                // the attempt sent nothing and the next one dials what the view names then.
                if node.observed.as_ref().and_then(|n| self.receiver_gone(target, &n.incarnation)).is_some() {
                    stale_span(&request_target, &moved);
                    return Phase::Done(pre.stale_before_finish(&request_target));
                }
                tracing::info!(step = "dial-superseded", moved = %moved, "the dial's slot moved under a living birth: the request is resent");
                return Phase::Done(pre.not_sent(NotSentReason::Connection(format!("the dial was superseded: {moved}"))));
            }
            Err(DialError::Deadline) => {
                tracing::info!(step = "dial-deadline", waited_ms = dial_started.elapsed().as_millis() as u64, "no connection to the target within the attempt's send bound");
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
                    destination = %node.node.as_ref().map(|n| n.node_id.to_string()).unwrap_or_default(),
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
                tracing::info!(step = "stream-open-deadline", waited_ms = opened_at.elapsed().as_millis() as u64, "no request stream opened within the attempt's send bound");
                return Phase::Done(pre.not_sent(NotSentReason::Deadline));
            }
        };
        if let Payload::AtWrite(_) = payload {
            let Payload::AtWrite(build) = std::mem::replace(payload, Payload::Ready(Vec::new())) else { unreachable!("matched above") };
            // What remains of the attempt's time limit is measured here, after resolve, dial and
            // stream open, immediately before the request is first written; a resend carries the
            // same bytes.
            let remaining = limit_at.saturating_duration_since(Instant::now());
            tracing::info!(step = "remaining-measured", remaining_ms = remaining.as_millis() as u64, "what remains of the attempt's time limit at the request write");
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
            // The unfinished request is reset with 499, so the receiver dispatches no partial frame.
            // The attempt sent nothing, and it is not the owner's cut: it is resent while the call has
            // attempts left, and told once they are spent (node-rpc.md §43).
            let _ = send.reset(frame_not_sent);
            let why = match written {
                Ok(Err(e)) => e.to_string(),
                _ => "its send bound passed".to_string(),
            };
            tracing::info!(step = "write-incomplete", reason = %why, "the request write did not complete: the attempt sent nothing");
            return Phase::Done(pre.not_sent(NotSentReason::Connection(format!("the request write did not complete: {why}"))));
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

    /// Open one request (node-rpc.md §43): resolve its receiver, mint its identity, open its
    /// invocation and start the task that sends and resends it. A receiver this view does not hold
    /// yet spends an attempt, never a wait (Q311): it is resolved again at the next attempt's
    /// instant, or sooner when the view changes. `Err` is an outcome before anything was sent: the
    /// attempts spent unresolved, a receiver proven gone, or a primary role with no birth.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn owe(
        &self,
        target: &NodeTarget,
        final_target: Option<&NodeId>,
        op: u8,
        payload: Payload,
        max_reply: usize,
        opts: &CallOptions,
        shape: ReplyShape,
        span: &tracing::Span,
    ) -> Result<SendCall, (RpcOutcome<()>, Option<CallEvidence>)> {
        let unary = shape == ReplyShape::Unary;
        let allowed = u32::from(opts.send.attempts.max(1));
        let pacing = self.delivery.pacing();
        let mut spent = 0u32;
        let node = loop {
            let started = Instant::now();
            match self.reach(target) {
                Ok(n) => break n,
                // A call addressed to a primary whose role names no birth ends receiver-gone (Q313).
                Err(NotSentReason::Resolve(f)) if matches!(target, NodeTarget::Primary { .. }) => {
                    let NodeTarget::Primary { mesh } = target else { unreachable!("matched above") };
                    let why = role_has_no_birth(mesh.as_deref(), &f);
                    span.in_scope(|| tracing::info_span!("rdm.node_rpc.delivery.remove.via-receiver-gone", attempts = spent, reason = %why).in_scope(|| tracing::info!("the primary role names no birth: the request is not sent")));
                    span.record("delivery", "receiver-gone");
                    return Err((PreCommit::begin(op).not_sent(NotSentReason::Resolve(ResolveFailure::Gone)), None));
                }
                Err(r @ NotSentReason::Resolve(ResolveFailure::Unknown)) => {
                    spent += 1;
                    span.in_scope(|| tracing::info!(step = "unresolved", attempt = spent, "the receiver is not in this caller's view: the attempt sent nothing"));
                    if spent >= allowed {
                        span.record("attempts", spent);
                        let out = PreCommit::begin(op).not_sent(r);
                        span.in_scope(|| self.tell_call_end(opts, None, &out));
                        return Err((out, None));
                    }
                    // The next attempt at its delay (node-rpc.md §43), or sooner when a change names
                    // the receiver: a change that does not spends no attempt.
                    let next_attempt = tokio::time::sleep_until(started + pacing.after(spent));
                    tokio::pin!(next_attempt);
                    let mut changes = self.resolver.changes();
                    loop {
                        let moved = async {
                            let changed = match changes.as_mut() {
                                Some(c) => c.changed().await.is_ok(),
                                None => false,
                            };
                            if !changed {
                                std::future::pending::<()>().await;
                            }
                        };
                        tokio::select! {
                            () = &mut next_attempt => break,
                            () = moved => {
                                if !matches!(self.resolver.resolve(target), Err(ResolveFailure::Unknown)) {
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(r) => return Err((PreCommit::begin(op).not_sent(r), None)),
            }
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
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(spent));
        let task = Attempts {
            client: self.clone(),
            target: target.clone(),
            birth: node.observed.as_ref().filter(|_| node.node.is_some()).map(|n| n.incarnation.clone()),
            receiver: node.node.as_ref().map(|n| n.node_id.clone()),
            op,
            id: id.clone(),
            receivers,
            carried_floor,
            payload,
            opts: opts.clone(),
            unary,
            ends_with_its_connection: matches!(shape, ReplyShape::Stream { ends_with_its_connection: true, .. }),
            resent_until_acked: !matches!(shape, ReplyShape::Stream { resent_until_acked: false, .. }),
            lost_last: None,
            committed_on: None,
            spent,
            evidence: evidence.clone(),
            tx,
            state: state.clone(),
            attempts: attempts.clone(),
        };
        // The attempts run on the dispatcher of the call's span, as the pool's dial does: a worker that
        // holds no dispatcher still delivers their events to the call's subscriber.
        let run = tracing::Instrument::instrument(task.run(), span.clone());
        let handle = match tracing::Span::with_subscriber(span, |(_, d): (&tracing::Id, &tracing::Dispatch)| d.clone()) {
            Some(d) => tokio::spawn(UnderDispatch { fut: Some(Box::pin(run)), dispatch: d }),
            None => tokio::spawn(run),
        };
        Ok(SendCall { guard, frames, attempted, state, evidence, attempts, receiver: node.node.clone(), _task: AbortOnDrop(Some(handle)) })
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
        let limit = opts.send.time_limit.0;
        let mut owed = match self.owe(target, final_target, op, payload, max_reply, opts, ReplyShape::Unary, span).await {
            Ok(o) => o,
            Err((out, ev)) => return (out.retyped().expect("no reply before anything is sent"), ev),
        };
        let mut pre = Some(PreCommit::begin(op));
        let mut committed: Option<rdm_core::outcome::Committed> = None;
        let mut stopped: Option<EarlyRefusal> = None;
        let mut last = NotSentReason::Deadline;
        // Once the request is acked its reply is bounded by the time limit, counted from the ack
        // (node-rpc.md §34): an acked request is never resent.
        let mut deadline: Option<Instant> = None;
        let mut state_closed = false;
        // The path the attempt task ended on, and whether the invocation's frame channel closed with no
        // frame: named when the attempts end (i146 Q38).
        let mut attempt_end: Option<AttemptEnd> = None;
        let mut slot_closed = false;
        // The call ended because its attempts are spent: its node is told once it has ended.
        let mut spent = false;
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
                changed = owed.state.changed(), if deadline.is_none() && !state_closed => {
                    if changed.is_err() {
                        state_closed = true;
                    } else if owed.state.borrow().acked {
                        deadline = Some(Instant::now() + limit);
                    }
                }
                ev = owed.attempted.recv() => match ev {
                    Some(Attempted::Committed { frame_len }) => {
                        if let Some(p) = pre.take() {
                            committed = Some(p.commit(RequestFinished::after_complete_write(frame_len, frame_len).expect("a complete write")));
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
                    Some(Attempted::Spent(lost)) => {
                        span.record("delivery", "attempts-spent");
                        spent = true;
                        break match (committed.take(), stopped.take(), pre.take()) {
                            // Its last attempt's stream was lost, or it waited out its time limit.
                            (Some(c), _, _) => c.indeterminate(lost.map_or(IndeterminateReason::ReplyDeadline, IndeterminateReason::ReplyLost)),
                            (None, Some(e), _) => decode(Decode::Early(e, None)),
                            (None, None, Some(p)) => p.not_sent(last.clone()),
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
                    // Acked and no reply within the time limit from the ack: the call ends, and its
                    // request is resent no longer.
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
        if spent || out.reply().is_some() {
            self.tell_call_end(opts, owed.receiver.as_ref(), &out);
        }
        (out, Some(evidence))
    }
}

/// Why a call addressed to a primary is not sent: the role names no birth in this caller's view.
fn role_has_no_birth(mesh: Option<&str>, f: &ResolveFailure) -> String {
    match mesh {
        Some(m) => format!("the mesh-primary of {m} names no birth in this caller's view ({f:?})"),
        None => format!("the fabric-primary names no birth in this caller's view ({f:?})"),
    }
}

/// What the op answers with: one frame, or a stream (node-rpc.md §34).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyShape {
    /// One reply frame.
    Unary,
    /// A stream of frames; `ends_with_its_connection` as its protocol declares
    /// (`StreamingProtocol::ENDS_WITH_ITS_CONNECTION`).
    Stream {
        /// The connection its request committed on is its bound.
        ends_with_its_connection: bool,
        /// `StreamingProtocol::REQUEST_RESENT_UNTIL_ACKED`.
        resent_until_acked: bool,
    },
}

/// One request, held by its owner: dropped, the invocation ends and its request is resent no
/// longer (the owner's cancellation).
pub(crate) struct SendCall {
    pub(crate) guard: CallGuard,
    pub(crate) frames: mpsc::UnboundedReceiver<SlotEvent>,
    pub(crate) attempted: mpsc::UnboundedReceiver<Attempted>,
    /// The invocation's state: acked, complete.
    pub(crate) state: watch::Receiver<SlotState>,
    pub(crate) evidence: Arc<Mutex<CallEvidence>>,
    pub(crate) attempts: Arc<std::sync::atomic::AtomicU32>,
    /// The birth the call is addressed to, as its resolver named it at the open; `None` for a call
    /// to an endpoint no record names.
    pub(crate) receiver: Option<ResolvedNode>,
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
    fut: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + std::marker::Send>>>,
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

impl SendCall {
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

/// The task that sends one request and resends it, with the same identity, until it is acked, its
/// receiver's exit is proven, its owner drops it, or its attempts are spent (node-rpc.md §43; Q311,
/// Q313).
struct Attempts {
    client: NodeRpcClient,
    target: NodeTarget,
    /// The receiving birth the call is sent to now: a resolver naming another incarnation proves
    /// this one ended, or for a call addressed to a primary names the role's next birth. `None` for
    /// a call to an endpoint no record names.
    birth: Option<rdm_core::IncarnationId>,
    /// The node the call is sent to now, as its resolver named it: the receiver a carried attempt
    /// names. `None` for a call to an endpoint no record names.
    receiver: Option<NodeId>,
    op: u8,
    id: DeliveryId,
    receivers: Vec<iroh::PublicKey>,
    carried_floor: Option<u64>,
    payload: Payload,
    opts: CallOptions,
    /// The op answers with one frame; a stream op's frames have no time limit while its stream
    /// lives, and once its stream is lost it is held for its time limit at most.
    unary: bool,
    /// A stream whose protocol ends it with the connection its request committed on.
    ends_with_its_connection: bool,
    /// An unacked attempt waits for its ack until the next send, and the last until its time limit
    /// (node-rpc.md §34).
    resent_until_acked: bool,
    /// Why the last attempt ended with no answer when its stream was lost, not its time limit: the
    /// call ends `ReplyLost`, never `ReplyDeadline`.
    lost_last: Option<String>,
    /// That connection, once committed.
    committed_on: Option<iroh::endpoint::Connection>,
    /// The attempts spent before the task started: a receiver the view did not hold yet.
    spent: u32,
    evidence: Arc<Mutex<CallEvidence>>,
    tx: mpsc::UnboundedSender<Attempted>,
    state: watch::Receiver<SlotState>,
    attempts: Arc<std::sync::atomic::AtomicU32>,
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

    /// For a call addressed to a primary: the birth the view names for the role now, when it is not
    /// the one the call is sent to (Q313). The call takes it as its receiver: the same message under
    /// the same delivery identity, toward that birth's endpoint too.
    fn next_birth(&mut self) -> Option<ResolvedNode> {
        if !matches!(self.target, NodeTarget::Primary { .. }) {
            return None;
        }
        let n = self.client.resolver.resolve(&self.target).ok()?;
        if self.birth.as_ref() == Some(&n.incarnation) {
            return None;
        }
        self.birth = Some(n.incarnation.clone());
        self.receiver = Some(n.node_id.clone());
        if !self.receivers.contains(&n.endpoint_id) {
            self.receivers.push(n.endpoint_id);
        }
        Some(n)
    }

    /// The request as one carrier's Forward to the call's receiver, `remaining` the bound its inner
    /// call is told, the inner call keeping the request's own identity (i146 Q52). A request built at
    /// its write, or a target that names no node, is not carried.
    fn forward_payload(&self, remaining: Duration) -> Result<Payload, String> {
        let receiver = match (&self.target, &self.receiver) {
            (NodeTarget::ExactNode(r), _) | (NodeTarget::Primary { .. }, Some(r)) => r,
            _ => return Err("a carried attempt needs an exact receiver".into()),
        };
        let Payload::Ready(inner) = &self.payload else { return Err("the request is built at its write".into()) };
        let owed = rdm_core::forward::SendDelivery { delivery_seq: self.id.delivery_seq, forget_below: self.lowest_unacked() };
        let forward = rdm_core::forward::ForwardRequest::Forward { target: receiver.clone(), inner_op: self.op, inner: inner.clone(), remaining_ms: remaining.as_millis() as u64, owed: Some(owed) };
        <rdm_core::forward::Forward as rdm_core::protocol::NodeProtocol>::encode_request(&forward).map(Payload::Ready).map_err(|e| e.0)
    }

    fn end_receiver_gone(&self, why: String, attempts: u32) -> AttemptEnd {
        tracing::info_span!("rdm.node_rpc.delivery.remove.via-receiver-gone", delivery_id = %self.id, attempts, reason = %why)
            .in_scope(|| tracing::info!("the receiving birth ended: the request is resent no longer"));
        let _ = self.tx.send(Attempted::ReceiverGone(why));
        AttemptEnd::ReceiverGone
    }

    fn end_spent(&self, attempts: u32) -> AttemptEnd {
        tracing::info_span!("rdm.node_rpc.delivery.update.via-attempts-spent", delivery_id = %self.id, attempts, allowed = self.opts.send.attempts)
            .in_scope(|| tracing::info!("the call's attempts are spent with no answer: it ends to its owner"));
        let _ = self.tx.send(Attempted::Spent(self.lost_last.clone()));
        AttemptEnd::AttemptsSpent
    }

    fn lowest_unacked(&self) -> u64 {
        // Never above the request's own identity: a receiver the call took on after it was minted
        // (a primary's next birth) holds nothing of this sender below it.
        let own = self.client.delivery.lowest_unacked(&self.receivers).min(self.id.delivery_seq);
        match self.carried_floor {
            Some(r) => r,
            None => own,
        }
    }

    /// Wait until `until`, or sooner when the resolver's answer for this call's target changes or
    /// the invocation's state does. A change of the view that leaves this target's answer as it was
    /// wakes nothing: each wake may start an attempt.
    async fn pause(&mut self, until: Instant) {
        let mut changes = self.client.resolver.changes();
        let (resolver, target) = (self.client.resolver.clone(), self.target.clone());
        let resolver_moved = async {
            let Some(c) = changes.as_mut() else { return std::future::pending::<()>().await };
            let before = resolver.resolve(&target);
            loop {
                if c.changed().await.is_err() {
                    return std::future::pending::<()>().await;
                }
                if resolver.resolve(&target) != before {
                    return;
                }
            }
        };
        tokio::select! {
            () = tokio::time::sleep_until(until) => {}
            () = resolver_moved => {}
            _ = self.state.changed() => {}
        }
    }

    /// A call addressed to a primary whose attempts on the birth the view names are spent waits
    /// for the view to name another birth for the role (Q313): `Some` ends the call (the role names
    /// no birth, or the owner's end), `None` takes the next birth.
    async fn until_next_birth(&mut self, attempts: u32) -> Option<AttemptEnd> {
        tracing::info_span!("rdm.node_rpc.delivery.update.via-attempts-spent", delivery_id = %self.id, attempts, allowed = self.opts.send.attempts, birth = %self.birth.as_ref().map(|b| b.as_str().to_string()).unwrap_or_default())
            .in_scope(|| tracing::info!("the attempts on this birth are spent: the call waits for the view to name the role's next birth"));
        let mut changes = self.client.resolver.changes();
        loop {
            if self.state.borrow().complete {
                return Some(AttemptEnd::CompleteAtTurn);
            }
            if let Some(why) = self.receiver_gone() {
                return Some(self.end_receiver_gone(why, attempts));
            }
            let held = self.birth.clone();
            if self.client.resolver.resolve(&self.target).ok().map(|n| n.incarnation) != held {
                return None;
            }
            match changes.as_mut() {
                Some(c) => tokio::select! {
                    r = c.changed() => if r.is_err() { changes = None },
                    _ = self.state.changed() => {}
                },
                None => {
                    let _ = self.state.changed().await;
                }
            }
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

    /// The attempts (node-rpc.md §43; Q311, Q313): each one sends the request with its identity,
    /// its resolve, dial and complete send bounded by its time limit (at most [`TRANSPORT_LIMIT`]);
    /// an unacked request is sent again at the schedule's next delay from the attempt's start, and
    /// the last attempt waits for its ack until its time limit. An acked request is never resent and
    /// spends no attempt. A call addressed to a primary counts its attempts per birth the view
    /// names for the role; any other keeps its count across every attempt.
    async fn until_end(&mut self) -> AttemptEnd {
        let pacing = self.client.delivery.pacing();
        let limit = self.opts.send.time_limit.0;
        let allowed = u32::from(self.opts.send.attempts.max(1));
        // Attempts in all, and on the birth the call is sent to now (the same, but for a call
        // addressed to a primary, whose count restarts on each birth the view names).
        let mut attempt = self.spent;
        let mut on_birth = self.spent;
        let mut held = 0u32;
        // Since when a stream op has held its lost stream.
        let mut held_since: Option<Instant> = None;
        // The last attempt on a birth committed: its wait for the ack runs to its time limit.
        let mut last_until: Option<Instant> = None;
        let mut committed = false;
        let mut last = NotSentReason::Deadline;
        // The last attempt sent nothing to the receiver: the next one asks the owner's carrier policy
        // (i146 Q52).
        let mut sent_nothing = false;
        loop {
            if self.state.borrow().complete {
                return AttemptEnd::CompleteAtTurn;
            }
            // A stream its protocol bounds by its connection lives as long as that connection.
            if let Some(why) = self.committed_on.as_ref().and_then(|c| c.close_reason()) {
                let _ = self.tx.send(Attempted::TransportLost(format!("the connection the request committed on closed ({why})")));
                return AttemptEnd::CommittedConnectionClosed;
            }
            if let Some(why) = self.receiver_gone() {
                return self.end_receiver_gone(why, attempt);
            }
            if let Some(n) = self.next_birth() {
                tracing::info_span!("rdm.node_rpc.delivery.update.via-next-birth", delivery_id = %self.id, attempts = attempt, node_id = %n.node_id, incarnation = %n.incarnation.as_str())
                    .in_scope(|| tracing::info!("the view names another birth for the primary role: its attempts count from one"));
                on_birth = 0;
                last_until = None;
            }
            if self.state.borrow().acked {
                // Acked, its response not complete: hold a connection the responder can resend on,
                // within the reply's time limit, which its owner keeps.
                held += 1;
                if let Err(why) = self.hold_connection(held, &mut held_since).await {
                    let _ = self.tx.send(Attempted::TransportLost(why));
                    return AttemptEnd::HoldLost;
                }
                continue;
            }
            if on_birth >= allowed {
                // The last attempt waits for its ack until its time limit, waking on the
                // receiver's end and on the invocation's state.
                if let Some(until) = last_until.filter(|u| Instant::now() < *u) {
                    self.pause(until).await;
                    continue;
                }
                if matches!(self.target, NodeTarget::Primary { .. }) {
                    match self.until_next_birth(attempt).await {
                        Some(end) => return end,
                        None => continue,
                    }
                }
                return self.end_spent(attempt);
            }
            attempt += 1;
            on_birth += 1;
            let last_on_birth = on_birth >= allowed;
            self.attempts.store(attempt, std::sync::atomic::Ordering::SeqCst);
            let started = Instant::now();
            let next_at = started + pacing.after(on_birth);
            // The attempt's resolve, dial and complete send: its time limit, at most the transport's
            // share of it.
            let send_deadline = started + limit.min(TRANSPORT_LIMIT);
            if attempt > 1 {
                self.client.delivery.stats().resends.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.delivery.update.via-resend", delivery_id = %self.id, attempt, backoff_ms = pacing.after(on_birth.saturating_sub(1).max(1)).as_millis() as u64)
                    .in_scope(|| tracing::info!("no ack: the request is resent with the same identity"));
            }
            // The leg of this attempt (node-rpc.md §43, §45; i146 Q52): direct first; once an attempt sent
            // nothing, the owner's carrier policy answers per attempt, every leg aimed at the same receiver
            // under the same delivery identity.
            let leg = match (&self.opts.route, sent_nothing) {
                (Some(policy), true) => (policy.0)(attempt),
                _ => crate::route::RouteChoice::Direct,
            };
            let mut carried = match &leg {
                crate::route::RouteChoice::Direct => None,
                crate::route::RouteChoice::ViaPeer { carrier } => match self.forward_payload(pacing.floor) {
                    Ok(p) => Some((NodeTarget::ExactNode(carrier.clone()), carrier.to_string(), p)),
                    Err(why) => {
                        tracing::info_span!("rdm.node_rpc.delivery.reject.via-carried-leg", delivery_id = %self.id, attempt, carrier = %carrier, reason = %why)
                            .in_scope(|| tracing::info!("the attempt is not carried: it goes direct"));
                        None
                    }
                },
                crate::route::RouteChoice::NoActiveRoute => {
                    last = NotSentReason::NoActiveRoute;
                    let _ = self.tx.send(Attempted::NotSent(last.clone()));
                    sent_nothing = true;
                    if !last_on_birth {
                        self.pause(next_at).await;
                    }
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
                // A receiver this view does not hold spends the attempt, never a wait (Q311).
                Err(r) => {
                    last = r.clone();
                    let _ = self.tx.send(Attempted::NotSent(r));
                    sent_nothing = true;
                    if !last_on_birth {
                        self.pause(next_at).await;
                    }
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
                        .in_scope(|| tracing::info!("the request is carried to its receiver through a live peer"));
                    self.client.attempt(carrier_target, &node, <rdm_core::forward::Forward as rdm_core::protocol::NodeProtocol>::OP, &header, payload, &self.opts, send_deadline, started + limit, &self.evidence).await
                }
                None => self.client.attempt(&self.target, &node, self.op, &header, &mut self.payload, &self.opts, send_deadline, started + limit, &self.evidence).await,
            };
            match phase {
                Phase::ReceiverGone(why) => return self.end_receiver_gone(why, attempt),
                Phase::Done(out) => {
                    // The owner's cut, a source that admits no new connection (nothing was sent, and
                    // nothing is resent for a request this source refuses to originate) and an owner
                    // that answers from its first unsent attempt end the call; any other unsent
                    // attempt is resent while the call has attempts left.
                    let resend = !(self.opts.unsent_is_final && !committed)
                        && matches!(&out, RpcOutcome::NotSent(n) if !matches!(n.reason(), NotSentReason::FrameNotSent | NotSentReason::AdmissionBlocked(_)));
                    if resend {
                        if let RpcOutcome::NotSent(n) = out {
                            last = n.reason().clone();
                            let _ = self.tx.send(Attempted::NotSent(last.clone()));
                        }
                        sent_nothing = true;
                        if !last_on_birth {
                            self.pause(next_at).await;
                        }
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
                    if self.ends_with_its_connection {
                        self.committed_on = Some(opened.conn.clone());
                    }
                    // The wait for the ack runs from the write: a slow dial spends none of it. An
                    // unacked request is sent again at the schedule's next delay; the last attempt
                    // waits for its ack until its time limit. A carried attempt waits for its
                    // carrier's answer, which the carrier gives inside the `remaining_ms` it was
                    // told. A stream op's request is owed the same; only its frames carry no time
                    // limit. The Build family and the handover are resent only when their stream is
                    // lost (`StreamingProtocol::REQUEST_RESENT_UNTIL_ACKED`).
                    let ack_by = match &outer {
                        Some(_) => Some(Instant::now() + pacing.floor),
                        None if !self.resent_until_acked => None,
                        None if self.state.borrow().acked => None,
                        None if last_on_birth => {
                            let until = (started + limit).max(Instant::now());
                            last_until = Some(until);
                            Some((Instant::now() + pacing.after(on_birth)).min(until))
                        }
                        None => Some(Instant::now() + pacing.after(on_birth)),
                    };
                    let resend_at = ack_by.unwrap_or(next_at);
                    sent_nothing = false;
                    self.lost_last = None;
                    match self.read(opened, ack_by, outer.as_ref().map(|o| o.id())).await {
                        StreamEnd::Complete => return AttemptEnd::StreamComplete,
                        StreamEnd::Violation => return AttemptEnd::StreamViolation,
                        StreamEnd::Definitive(code) => {
                            let _ = self.tx.send(Attempted::Reset(code));
                            return AttemptEnd::Reset;
                        }
                        StreamEnd::NoAck => continue,
                        StreamEnd::Lost => {
                            if !self.state.borrow().acked {
                                if last_on_birth {
                                    // Its last attempt's stream is gone unacked: no answer can come,
                                    // and no time limit lapsed.
                                    last_until = None;
                                    self.lost_last = Some("its last attempt's stream was lost before an ack".into());
                                } else {
                                    self.pause(resend_at).await;
                                }
                            }
                        }
                        // A carrier failure never ends the call (i146 Q52): the request is unacked, and the
                        // next attempt asks the owner's carrier policy again.
                        StreamEnd::CarrierFailed(why) => {
                            tracing::info_span!("rdm.node_rpc.delivery.update.via-carrier-failed", delivery_id = %self.id, attempt, reason = %why)
                                .in_scope(|| tracing::info!("the carrier did not reach the receiver: the request is resent"));
                            sent_nothing = true;
                            if !last_on_birth {
                                self.pause(resend_at).await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Acked, response not complete, its stream gone: keep a pooled connection to the responder,
    /// on the schedule, so the responder resends what it holds there, until the response completes.
    /// A unary call's reply limit, counted from the ack, is its owner's and ends it. A stream op's
    /// frames have no limit while its stream lives; once lost, `Err` when the invocation has not
    /// completed within the call's time limit from the loss (`held_since`).
    async fn hold_connection(&mut self, turn: u32, held_since: &mut Option<Instant>) -> Result<(), String> {
        let pacing = self.client.delivery.pacing();
        let limit = self.opts.send.time_limit.0;
        if !self.unary {
            let since = *held_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= limit {
                return Err(format!("the stream was lost and its invocation did not complete within {} ms", since.elapsed().as_millis()));
            }
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
                closed: None,
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
        let mut until = Instant::now() + pacing.after(turn.max(1));
        if let Some(since) = held_since.filter(|_| !self.unary) {
            until = until.min(since + limit);
        }
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
