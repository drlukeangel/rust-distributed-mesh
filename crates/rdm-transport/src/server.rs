//! The Node RPC server (node-rpc.md §12–§19, §26–§33, §43; ownership §6.3).
//!
//! Per bi-stream: read the transport header first. A `Response` or `Ack` first frame goes to the
//! delivery demultiplexer before any admission. A `Request` is checked against its sender's
//! duplicate window: a repeat is held pending (unacked) while the original has not reached its
//! acceptance boundary, and acked without dispatch after it. A first arrival reads the target (an
//! unserved op → `421`; the node id it names is never a reason to refuse, R-UC1a), the context, the
//! declared length (oversize → typed `Malformed(TooLarge)`), takes admission (full → typed `Busy`),
//! reads the body, and dispatches once the request frame is complete. A request the sender reset
//! (`499`) or left unfinished is dropped and never reaches a handler. Every response frame, a named
//! refusal included, is owed until the caller acks it. Handler work runs supervised: a panic or
//! `HandlerFault` resets with `423 INTERNAL_RPC_FAILURE`.

use crate::admission::{Admission, Limits, Permit};
use crate::client::CarriedIdentity;
use crate::delivery::{Acceptance, AckOf, Delivery, FrameOut, FrameReader, ReadEnd, RequestArrival, Settled};
use iroh::endpoint::{Connection, ReadError, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use rdm_core::catalog::{CatalogBuilder, CatalogEntry, CatalogError, Catalog, Shape, OpOwner};
use rdm_core::codes::ResetCode;
use rdm_core::dispatch::{RequestAssembly, ServerAction};
use rdm_core::framing::TransportHeader;
use rdm_core::outcome::MalformedKind;
use rdm_core::protocol::NodeProtocol;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use rdm_core::context::{dropped_as_str, CallContext};
use std::sync::Arc;
use tracing::Instrument;

/// Who is calling, as the transport proved it.
#[derive(Debug, Clone)]
pub struct PeerContext {
    /// The calling node's endpoint id, as the transport proved it.
    pub endpoint_id: iroh::PublicKey,
    /// The observability context the request carried, already sanitized: what a carrier
    /// hands into its inner call unchanged.
    pub context: CallContext,
    /// The request's delivery identity and its sender's `forget_below`: what a carrier's inner call
    /// keeps, so the final target suppresses a resend through any carrier.
    pub delivery: Option<CarriedIdentity>,
    /// The request's acceptance boundary (node-rpc.md §43): a handler that records the accepted
    /// work says so here, and the request is acknowledged at once.
    pub acceptance: Acceptance,
}

/// An unexpected execution fault. Created only by named constructors, so
/// every fault site is deliberate (node-rpc.md §32.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerFault(String);

impl HandlerFault {
    /// A fault for a broken invariant, naming `what` was broken.
    pub fn invariant_broken(what: impl Into<String>) -> Self {
        Self(what.into())
    }
    /// The named reason the fault carries.
    pub fn reason(&self) -> &str {
        &self.0
    }
}

pub(crate) type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A typed refusal encoded with the protocol's own constructors.
pub(crate) enum Refusal {
    NotReady(String),
    Busy(String),
    ConnectionsDeleting(String),
    Malformed(MalformedKind),
}

pub(crate) trait Erased: Send + Sync {
    /// The handler's future, with the serve span it runs under: the invocation names what follows the
    /// handler (its fault or panic) under that span. `Span::none()` for a request that does not decode,
    /// which is refused and opens no serve span.
    fn call(&self, peer: PeerContext, payload: Vec<u8>) -> (tracing::Span, BoxFut<Result<Vec<u8>, HandlerFault>>);
    fn refusal(&self, r: Refusal) -> Vec<u8>;
    fn max_reply(&self) -> usize;
    /// Whether the request `payload` only reads: accepted as it is dispatched.
    fn read_only(&self, payload: &[u8]) -> bool;
    /// A server-streaming handler: frames go out through `out` as the handler produces them.
    fn is_stream(&self) -> bool {
        false
    }
    /// The streaming handler's future, with its serve span as [`Erased::call`] gives it: the stream's
    /// end (its terminal or refusal) is written under that span.
    fn stream(&self, _peer: PeerContext, _payload: Vec<u8>, _out: crate::stream::StreamOut) -> Option<(tracing::Span, BoxFut<crate::stream::StreamEnd>)> {
        None
    }
}

struct Typed<P, F> {
    f: Arc<F>,
    _p: std::marker::PhantomData<fn() -> P>,
}

impl<P, F, Fut> Erased for Typed<P, F>
where
    P: NodeProtocol,
    F: Fn(PeerContext, P::Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<P::Reply, HandlerFault>> + Send + 'static,
{
    fn call(&self, peer: PeerContext, payload: Vec<u8>) -> (tracing::Span, BoxFut<Result<Vec<u8>, HandlerFault>>) {
        let f = self.f.clone();
        let req = match P::decode_request(&payload) {
            Ok(r) => r,
            Err(d) => {
                let refused = Box::pin(async move {
                    tracing::info_span!("rdm.node_rpc.request.reject.via-malformed", protocol = P::NAME, kind = ?d, code = rdm_core::outcome::ReplyKind::Malformed(d.into()).code().name(), detail = "malformed")
                        .in_scope(|| tracing::info!(?d, "request does not decode"));
                    P::encode_reply(&P::malformed(d.into())).map_err(|e| HandlerFault::invariant_broken(e.0))
                });
                return (tracing::Span::none(), refused);
            }
        };
        let span = tracing::info_span!(
            "rdm.node_rpc.request.serve.via-direct",
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
        apply_context(&span, &peer);
        let serve = span.clone();
        let handler = Box::pin(async move {
            let reply = match f(peer, req).instrument(serve.clone()).await {
                Ok(reply) => reply,
                Err(fault) => {
                    serve.record("code", "INTERNAL");
                    serve.record("detail", "handler-fault");
                    return Err(fault);
                }
            };
            serve.record("code", P::classify_reply(&reply).code().name());
            serve.record("detail", P::reply_detail(&reply));
            P::encode_reply(&reply).map_err(|e| HandlerFault::invariant_broken(e.0))
        });
        (span, handler)
    }

    fn refusal(&self, r: Refusal) -> Vec<u8> {
        let reply = match r {
            Refusal::NotReady(s) => P::not_ready(s),
            Refusal::Busy(s) => P::busy(s),
            Refusal::ConnectionsDeleting(s) => P::connections_deleting(s),
            Refusal::Malformed(k) => P::malformed(k),
        };
        tracing::info_span!("rdm.node_rpc.request.reject.via-refusal", protocol = P::NAME, code = P::classify_reply(&reply).code().name(), detail = P::reply_detail(&reply)).in_scope(|| {});
        P::encode_reply(&reply).unwrap_or_default()
    }

    fn max_reply(&self) -> usize {
        P::MAX_REPLY_FRAME_BYTES
    }

    fn read_only(&self, payload: &[u8]) -> bool {
        P::decode_request(payload).is_ok_and(|r| P::read_only(&r))
    }
}

/// Parent `span` on the request's W3C context and record its caller identity and the
/// allowlisted baggage keys. The context never reaches the handler's decision: it is
/// evidence on the span and, through `peer`, what a carrier hands onward.
pub(crate) fn apply_context(span: &tracing::Span, peer: &PeerContext) {
    let c = &peer.context;
    if let Some(tp) = c.traceparent.as_deref() {
        crate::set_remote_parent(span, tp, c.tracestate.as_deref());
    }
    if let Some(s) = c.caller_system.as_deref() {
        span.record("caller_system", s);
    }
    for (k, v) in c.span_baggage() {
        span.record(k, v.as_str());
    }
}

/// Counters a functional test or a caller can read (pre-body refusals are
/// counted, not root-spanned per call; node-rpc.md §38.1).
#[derive(Debug, Default)]
pub struct ServerStats {
    /// Requests that reached a handler.
    pub dispatched: AtomicU64,
    /// Requests dropped because the sender finished or reset before the request was complete.
    pub dropped_unfinished: AtomicU64,
    /// Requests refused because no handler serves the op (`421 UNSERVED_OP`).
    pub unserved: AtomicU64,
    /// Requests refused because a frame exceeded its op's size bound.
    pub too_large: AtomicU64,
    /// Requests refused typed `Busy` by admission.
    pub busy: AtomicU64,
    /// Requests refused with `ConnectionsDeleting` while admission is closed.
    pub connections_deleting: AtomicU64,
    /// Requests refused typed `NotReady` by a closed ready gate.
    pub not_ready: AtomicU64,
    /// Requests reset as protocol violations.
    pub violations: AtomicU64,
    /// Handlers that failed or panicked.
    pub faults: AtomicU64,
    /// Handlers dispatched and not yet finished (a connections-deleted node waits for it to reach zero).
    pub in_flight: AtomicU64,
}

/// Counts one dispatched handler for as long as it lives, however its
/// invocation ends.
struct InFlight(Arc<ServerStats>);

impl InFlight {
    fn enter(stats: &Arc<ServerStats>) -> Self {
        stats.in_flight.fetch_add(1, Ordering::SeqCst);
        Self(stats.clone())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The calls this server dispatched whose reply is not yet settled: written and acknowledged by
/// the caller, or the caller or the connection gone. A node that closes its endpoint with a reply
/// unsettled takes the reply down with the connection, so the caller reads `connection lost`
/// where the node answered `Applied`.
#[derive(Default)]
struct UnsettledReplies {
    next: AtomicU64,
    /// Each unsettled call by its dispatch order, named `op <op> from <peer>`.
    open: std::sync::Mutex<std::collections::BTreeMap<u64, String>>,
    changed: tokio::sync::watch::Sender<u64>,
}

/// One dispatched call's place in [`UnsettledReplies`]: held from the dispatch until the reply is
/// settled, and released on every way out of the call.
struct Unsettled {
    replies: Arc<UnsettledReplies>,
    seq: u64,
}

impl Unsettled {
    fn enter(replies: &Arc<UnsettledReplies>, op: u8, peer: &iroh::PublicKey) -> Self {
        let mut open = replies.open.lock().unwrap();
        let seq = replies.next.fetch_add(1, Ordering::SeqCst);
        open.insert(seq, format!("op {op} from {}", peer.fmt_short()));
        Self { replies: replies.clone(), seq }
    }
}

impl Drop for Unsettled {
    fn drop(&mut self) {
        self.replies.open.lock().unwrap().remove(&self.seq);
        self.replies.changed.send_modify(|v| *v += 1);
    }
}

impl ServerStats {
    /// Read `c` with sequential consistency.
    pub fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::SeqCst)
    }
}

/// Registration happens at birth; `build` consumes the builder.
#[derive(Default)]
pub struct ServerBuilder {
    pub(crate) catalog: CatalogBuilder,
    pub(crate) handlers: HashMap<u8, Arc<dyn Erased>>,
    admission: Admission,
    /// Closed (`false`) until the owner opens it: every op, ping included, is refused with the
    /// protocol's typed `NotReady`.
    ready: Option<Arc<AtomicBool>>,
    /// Forwardable protocols this node carries, op -> max inner reply (node-rpc.md §36.1).
    pub(crate) carried: HashMap<u8, usize>,
    /// Filled at build for the forward handler, when this node serves it.
    pub(crate) carried_table: Option<Arc<std::sync::OnceLock<HashMap<u8, usize>>>>,
    /// What this carrier does with each inner answer it carries back, when it was given one.
    pub(crate) carried_replies: Arc<std::sync::OnceLock<Arc<dyn crate::forward::CarriedReplies>>>,
    /// The accept trigger and the resolver that names an accepted peer, when the process runs
    /// direct recovery (R-CN1).
    inbound: Option<Inbound>,
    /// Test seam: withholds the next reply after the handler has applied it.
    withhold: Option<Arc<ReplyWithhold>>,
    /// The process's delivery state, shared with its client; `None`: the served birth's own.
    delivery: Option<Delivery>,
}

/// An accepted connection handed to the process's accept trigger once the resolver names the
/// peer's live birth: a recovery trigger, never Direct evidence (R-CN1).
#[derive(Clone)]
struct Inbound {
    resolver: Arc<crate::LiveNodeResolver>,
    trigger: Arc<dyn crate::AcceptTrigger>,
}

impl ServerBuilder {
    /// An empty builder: no handlers, default admission, no ready gate.
    pub fn new() -> Self {
        Self {
            catalog: CatalogBuilder::new(),
            handlers: HashMap::new(),
            admission: Admission::default(),
            ready: None,
            carried: HashMap::new(),
            carried_table: None,
            carried_replies: Arc::default(),
            inbound: None,
            withhold: None,
            delivery: None,
        }
    }

    /// Serve as the process `delivery` is: the duplicate windows, the owed responses and the
    /// open invocations of its client are one.
    pub fn with_delivery(mut self, delivery: Delivery) -> Self {
        self.delivery = Some(delivery);
        self
    }

    /// Test seam: let `withhold` hold back the reply of the next call it is armed for. An unarmed
    /// seam changes nothing.
    pub fn withhold_replies(mut self, withhold: Arc<ReplyWithhold>) -> Self {
        self.withhold = Some(withhold);
        self
    }

    /// Hand every accepted connection whose peer `resolver` names as a live node to `trigger`
    /// (R-CN1: an accept is a recovery trigger for this node's own direct dial toward that birth,
    /// never Direct evidence; the acceptor writes nothing). A peer the resolver does not hold (a
    /// probe's ephemeral key) is handed to nothing.
    pub fn with_accept_trigger(mut self, resolver: Arc<crate::LiveNodeResolver>, trigger: Arc<dyn crate::AcceptTrigger>) -> Self {
        self.inbound = Some(Inbound { resolver, trigger });
        self
    }

    /// Serve unary protocol `P` (owned by `owner`) with handler `f`.
    pub fn serve<P, F, Fut>(mut self, owner: OpOwner, f: F) -> Self
    where
        P: NodeProtocol,
        F: Fn(PeerContext, P::Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<P::Reply, HandlerFault>> + Send + 'static,
    {
        self.catalog = self.catalog.serve(CatalogEntry::canonical::<P>(owner, Shape::Unary));
        self.handlers.insert(P::OP, Arc::new(Typed::<P, F> { f: Arc::new(f), _p: std::marker::PhantomData }));
        self
    }

    /// Add the ledger rows that reserve ops for other products; each op is named once in the built
    /// catalog.
    pub fn ledger(mut self, rows: impl IntoIterator<Item = rdm_core::catalog::LedgerEntry>) -> Self {
        self.catalog = self.catalog.ledger(rows);
        self
    }

    /// Compose a product's transitional adapter (`CatalogEntry::transitional`) into this
    /// process's one built catalog (node-rpc-envelope.md §12). The server runs no handler
    /// for it: the product's own dispatcher serves the op on its legacy framing and consults
    /// [`NodeRpcServer::catalog`] to refuse what the catalog does not hold. On this server's
    /// own ALPN the op is unserved (`421`), as any catalogued op without a handler is.
    pub fn adapter(mut self, entry: CatalogEntry) -> Self {
        self.catalog = self.catalog.serve(entry);
        self
    }

    /// Serve nothing until `ready` is true: while it is false every op, `Ping` included, is
    /// answered with its protocol's typed `NotReady` and no handler runs. A node whose handlers
    /// read state filled after its server is already accepting closes the gate until the state
    /// exists, so no peer is answered from a half-built node.
    pub fn with_ready_gate(mut self, ready: Arc<AtomicBool>) -> Self {
        self.ready = Some(ready);
        self
    }

    /// Bound admission for `op` to `limits`.
    pub fn limits(mut self, op: u8, limits: Limits) -> Self {
        self.admission.set(op, limits);
        self
    }

    /// Build the catalog before the first stream is accepted, as `birth`: the
    /// exact node and incarnation this process is. A request is dispatched only
    /// when its target names this node and a served op.
    pub fn build(self, birth: ServedBirth) -> Result<NodeRpcServer, Vec<CatalogError>> {
        let catalog = self.catalog.build()?;
        if let Some(table) = &self.carried_table {
            let _ = table.set(self.carried.clone());
        }
        let delivery = self.delivery.unwrap_or_else(|| {
            let me = match (rdm_core::NodeId::parse(&birth.node_id), rdm_core::IncarnationId::parse(&birth.incarnation)) {
                (Ok(node_id), Ok(incarnation)) => rdm_core::framing::Birth { node_id, incarnation },
                _ => crate::delivery::minted_birth(),
            };
            Delivery::new(me)
        });
        Ok(NodeRpcServer {
            inner: Arc::new(Inner {
                catalog,
                handlers: self.handlers,
                admission: self.admission,
                ready: self.ready,
                connections_deleting: AtomicBool::new(false),
                stats: Arc::new(ServerStats::default()),
                birth,
                inbound: self.inbound,
                withhold: self.withhold,
                replies: Arc::new(UnsettledReplies::default()),
                connections: std::sync::Mutex::default(),
                delivery,
            }),
        })
    }
}

/// Test seam (R-I3): withholds the reply of the next call after its handler has run, until the test
/// releases it, then writes the reply and records what the caller's side of the stream did with it;
/// every release is recorded, a reply with no stream left to write as a failed one.
/// The server, its wire format and the reply bytes are the production ones; only the moment of the
/// write moves. Unarmed, it is inert.
#[derive(Default)]
pub struct ReplyWithhold {
    armed: AtomicBool,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
    landed: tokio::sync::Notify,
    late: std::sync::Mutex<Option<LateReply>>,
}

/// What became of a withheld reply once it was released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LateReply {
    /// The caller's `STOP_SENDING` for the call's reply stream, as the server observed it
    /// (`Ok(Some(code))`), or why it observed none.
    pub stopped: Result<Option<u64>, String>,
    /// The late reply's write: `Err` when the caller's side had already abandoned the stream, or
    /// when the reply's direction had ended before the release (named: a write on it that failed).
    pub written: Result<(), String>,
}

impl ReplyWithhold {
    /// Withhold the reply of the next call that reaches its write.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Resolves once a handler has finished and its reply is being withheld.
    pub async fn reached(&self) {
        self.reached.notified().await;
    }

    /// Let the withheld reply go.
    pub fn release(&self) {
        self.release.notify_one();
    }

    /// Resolves with what became of the released reply.
    pub async fn late_reply(&self) -> LateReply {
        self.landed.notified().await;
        self.late.lock().unwrap().take().expect("a landed late reply is recorded")
    }

    fn take_armed(&self) -> bool {
        self.armed.swap(false, Ordering::SeqCst)
    }

    async fn wait_release(&self) {
        self.reached.notify_one();
        self.release.notified().await;
    }

    async fn land(send: &mut SendStream, bytes: &[u8]) -> LateReply {
        // The caller's abandonment of the reply stream is waited for, not slept for: it is the
        // fact the release is ordered after.
        let stopped = send.stopped().await.map(|c| c.map(|v| v.into_inner())).map_err(|e| e.to_string());
        let written = send.write_all(bytes).await.map_err(|e| e.to_string());
        LateReply { stopped, written }
    }

    fn record(&self, late: LateReply) {
        *self.late.lock().unwrap() = Some(late);
        self.landed.notify_one();
    }
}

impl LateReply {
    /// A released reply that had no stream to be written on: `why` names what ended it.
    fn unwritten(why: String) -> Self {
        Self { stopped: Err(why.clone()), written: Err(why) }
    }
}

/// The exact birth a server is, named in its spans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedBirth {
    /// The served node's id.
    pub node_id: String,
    /// The served node's incarnation id.
    pub incarnation: String,
}

struct Inner {
    catalog: Catalog,
    handlers: HashMap<u8, Arc<dyn Erased>>,
    admission: Admission,
    ready: Option<Arc<AtomicBool>>,
    connections_deleting: AtomicBool,
    stats: Arc<ServerStats>,
    birth: ServedBirth,
    inbound: Option<Inbound>,
    withhold: Option<Arc<ReplyWithhold>>,
    replies: Arc<UnsettledReplies>,
    /// The connections this server accepted and still holds open, by their stable id.
    connections: std::sync::Mutex<HashMap<usize, Connection>>,
    delivery: Delivery,
}

/// One built server for the process's one endpoint. The socket a request
/// arrives on names nothing: the request's target does.
#[derive(Clone)]
pub struct NodeRpcServer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for NodeRpcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRpcServer").field("birth", &self.inner.birth).finish()
    }
}

fn code(c: ResetCode) -> VarInt {
    VarInt::from_u32(c.code())
}

impl NodeRpcServer {
    /// The one built effective catalog of this process: what every dispatcher in it serves.
    pub fn catalog(&self) -> &Catalog {
        &self.inner.catalog
    }

    /// The server's counters.
    pub fn stats(&self) -> Arc<ServerStats> {
        self.inner.stats.clone()
    }

    /// Resolves once every call dispatched before this one has its reply settled: written, and
    /// acknowledged by the caller or abandoned by it or its connection. A node that is about to
    /// close its endpoint waits here first, so no admitted answer is taken down with the
    /// connection. Calls dispatched after this one do not extend the wait.
    pub async fn settled(&self) {
        let replies = &self.inner.replies;
        let upto = replies.next.load(Ordering::SeqCst);
        let mut changed = replies.changed.subscribe();
        loop {
            if replies.open.lock().unwrap().first_key_value().is_none_or(|(first, _)| *first >= upto) {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }

    /// The calls this server dispatched whose reply is not settled now, in dispatch order, each named
    /// `op <op> from <peer>`: what a leave that stopped waiting at its bound names.
    pub fn unsettled(&self) -> Vec<String> {
        self.inner.replies.open.lock().unwrap().values().cloned().collect()
    }

    /// Hard-cut the connections this server accepted (`node.connections.delete`), closing each with
    /// `reason`, except those from `spare` (the peers whose calls are in flight and answered on their
    /// connections). The endpoint stays bound and fresh dials are accepted. Returns how many were
    /// closed.
    pub fn close_connections_except(&self, spare: &[iroh::PublicKey], reason: &'static str) -> usize {
        let cut: Vec<Connection> = {
            let held = self.inner.connections.lock().unwrap();
            held.values().filter(|c| !spare.contains(&c.remote_id())).cloned().collect()
        };
        for c in &cut {
            tracing::info_span!("rdm.node_rpc.connection.delete.via-hard-cut", peer = %c.remote_id().fmt_short(), connection = c.stable_id(), direction = "accepted")
                .in_scope(|| tracing::info!("an accepted connection was closed"));
            c.close(0u32.into(), reason.as_bytes());
        }
        cut.len()
    }

    /// Hard-cut the connections this server accepted from `peer` (`node.connections.delete` naming
    /// one exact connection), closing each with `reason`, unless `peer` is `spare` (the peer whose
    /// call is in flight and answered on its connection). Returns how many were closed.
    pub fn close_connections_from(&self, peer: iroh::PublicKey, spare: Option<iroh::PublicKey>, reason: &'static str) -> usize {
        if Some(peer) == spare {
            return 0;
        }
        let cut: Vec<Connection> = {
            let held = self.inner.connections.lock().unwrap();
            held.values().filter(|c| c.remote_id() == peer).cloned().collect()
        };
        for c in &cut {
            tracing::info_span!("rdm.node_rpc.connection.delete.via-hard-cut", peer = %c.remote_id().fmt_short(), connection = c.stable_id(), direction = "accepted")
                .in_scope(|| tracing::info!("an accepted connection was closed"));
            c.close(0u32.into(), reason.as_bytes());
        }
        cut.len()
    }

    /// Enter connections-deleting: new calls receive a typed `ConnectionsDeleting` refusal.
    pub fn begin_connections_delete(&self) {
        self.inner.connections_deleting.store(true, Ordering::SeqCst);
    }

    /// Leave connections-deleting: a stopped birth that starts again takes new calls.
    pub fn resume(&self) {
        self.inner.connections_deleting.store(false, Ordering::SeqCst);
    }

    /// The process's delivery state.
    pub fn delivery(&self) -> &Delivery {
        &self.inner.delivery
    }

    /// One accepted bi-stream: its first frame's header decides what it carries.
    async fn stream(self, conn: Connection, send: SendStream, recv: RecvStream) {
        let peer = conn.remote_id();
        let mut reader = FrameReader::new(recv, Vec::new());
        let first = match reader.header().await {
            Ok(Some(h)) => h,
            Ok(None) => return,
            Err(ReadEnd::Violation(why)) => {
                self.inner.stats.violations.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.request.reject.via-protocol-violation", op = ?Option::<u8>::None, reason = why, code = "INTERNAL", detail = "protocol-violation")
                    .in_scope(|| tracing::info!(reason = why, "protocol violation: 424"));
                let mut send = send;
                let _ = send.reset(code(ResetCode::ProtocolViolation));
                reader.stop(ResetCode::ProtocolViolation);
                return;
            }
            Err(e) => {
                let reason = match e {
                    ReadEnd::Reset(c) if ResetCode::from_code(c) == Some(ResetCode::FrameNotSent) => "sender reset an unfinished request (499 FRAME_NOT_SENT)",
                    ReadEnd::Reset(_) => "sender reset the request direction",
                    ReadEnd::Lost(_) => "connection lost",
                    _ => "the stream ended inside the transport header",
                };
                self.dropped(peer, reason);
                let mut send = send;
                let _ = send.reset(code(ResetCode::RequestStop));
                return;
            }
        };
        match first {
            TransportHeader::Request { .. } => self.invocation(peer, conn, first, send, reader).await,
            // Resent responses and acks reach the demultiplexer before dispatch and before
            // lifecycle admission: an endpoint that admits no new command still receives them.
            _ => self.inner.delivery.serve_reverse_stream(first, send, reader).await,
        }
    }

    fn dropped(&self, peer: iroh::PublicKey, reason: &'static str) {
        self.inner.stats.dropped_unfinished.fetch_add(1, Ordering::SeqCst);
        tracing::info_span!("rdm.node_rpc.request.reject.via-frame-not-sent", reason, peer = %peer, code = "UNAVAILABLE", detail = "frame-not-sent")
            .in_scope(|| tracing::info!(reason, "unfinished request dropped; never dispatched"));
    }

    /// A repeat of a request this server holds: acked once the original is accepted, without
    /// dispatch, then its stream ends; reset as the original ended; or stopped when the original was
    /// never dispatched.
    async fn answer_repeat(&self, peer: iroh::PublicKey, header: &TransportHeader, settled: Settled, out: FrameOut, mut reader: FrameReader, parent: Option<(String, Option<String>)>) {
        let d = &self.inner.delivery;
        let id = header.delivery_id();
        match settled {
            Settled::Accepted => {
                let span = tracing::info_span!("rdm.node_rpc.delivery.serve.via-ack", delivery_id = %id, acceptance = "duplicate", peer = %peer);
                crate::delivery::parent_on(&span, parent.as_ref());
                span.in_scope(|| tracing::info!("a repeated request is acked without dispatch"));
                d.write_ack(&out, header, AckOf::Request).await;
                out.finish().await;
                // The caller is reachable from here: what of the original's response is resent goes
                // to it.
                d.answer_to(&id, peer);
            }
            Settled::Ended(c) => {
                out.reset(ResetCode::from_code(c).unwrap_or(ResetCode::InternalRpcFailure)).await;
                reader.stop(ResetCode::RequestStop);
            }
            Settled::Dropped => {
                out.reset(ResetCode::RequestStop).await;
                reader.stop(ResetCode::RequestStop);
            }
        }
    }

    /// The remote context a repeated request carried, read from its sections (bounded as any request's);
    /// the reader comes back holding the bytes after them.
    async fn repeat_context(&self, reader: FrameReader) -> (FrameReader, Option<(String, Option<String>)>) {
        let (mut recv, rest) = reader.into_parts();
        let mut asm = RequestAssembly::new(&self.inner.catalog);
        let mut action = asm.push(&rest);
        let mut buf = vec![0u8; 16 * 1024];
        while action == ServerAction::Continue {
            action = match recv.read(&mut buf).await {
                Ok(Some(n)) => asm.push(&buf[..n]),
                _ => break,
            };
        }
        let parent = match &action {
            ServerAction::Dispatch { header, .. } => crate::delivery::parent_of(&header.context.clone().sanitized().0),
            _ => None,
        };
        (FrameReader::new(recv, asm.take_rest()), parent)
    }

    /// Read the caller's acks of `id`'s response on the request direction until the response is
    /// complete and acked, or the direction ends. While the direction lives its frames are delivered
    /// (QUIC loses nothing on a live stream): an ack that has not come is a caller whose owner has not
    /// read the frame yet, never a loss. A caller finishes its request direction only once it is done
    /// with the invocation, so a clean finish with a frame unacked is a caller that ended it: nothing is
    /// owed. A direction reset or lost with a frame unacked is a lost ack: the frame goes out again on
    /// a new stream.
    async fn read_acks(d: Delivery, id: rdm_core::framing::DeliveryId, mut reader: FrameReader) -> String {
        let Some(mut acked) = d.acked_watch(&id) else { return "nothing owed".into() };
        let ended = loop {
            tokio::select! {
                h = reader.header() => match h {
                    Ok(Some(h @ TransportHeader::Ack { .. })) => d.on_ack(&h),
                    Ok(Some(other)) => break format!("a {} frame where acks belong", other.kind()),
                    Ok(None) => break "finished".to_string(),
                    Err(e) => break format!("{e:?}"),
                },
                r = acked.changed() => if r.is_err() {
                    // The response is complete and every frame acked.
                    return "settled".into();
                },
            }
        };
        if !d.response_settled(&id) {
            if ended == "finished" {
                tracing::debug_span!("rdm.node_rpc.delivery.remove.via-caller-finished", delivery_id = %id)
                    .in_scope(|| tracing::debug!("the caller finished the invocation's request direction: its response is owed no longer"));
                d.drop_response(&id);
            } else {
                d.push(&id, None, &format!("the request direction ended ({ended}) with a response frame unacked"));
            }
        }
        ended
    }

    /// A named refusal of request `id`: a response frame at `seq` 0, owed until the caller acks it.
    /// It answers the request (a repeat is acked without dispatch).
    async fn answer_refusal(&self, op: u8, r: Refusal, id: &rdm_core::framing::DeliveryId, peer: iroh::PublicKey, out: FrameOut, acks: Option<FrameReader>) {
        let d = &self.inner.delivery;
        let Some(h) = self.inner.handlers.get(&op) else {
            out.reset(ResetCode::UnservedOp).await;
            d.settle_request(id, Settled::Ended(u64::from(ResetCode::UnservedOp.code())));
            return;
        };
        d.owe_response(id, peer);
        d.settle_request(id, Settled::Accepted);
        if let Some(frame) = d.owe_frame(id, 0, h.refusal(r), true) {
            let _ = out.write(&frame).await;
        }
        out.finish().await;
        match acks {
            Some(reader) => drop(Self::read_acks(d.clone(), id.clone(), reader).await),
            // The request direction was stopped: the caller acks on an ack stream, and the frame is
            // resent if no ack arrives within the first resend's delay.
            None => d.push_after(id, d.pacing().after(1)),
        }
    }

    async fn invocation(self, peer: iroh::PublicKey, conn: Connection, header: TransportHeader, send: SendStream, reader: FrameReader) {
        let stats = self.inner.stats.clone();
        let d = self.inner.delivery.clone();
        let id = header.delivery_id();
        let TransportHeader::Request { forget_below, .. } = &header else { unreachable!("an invocation begins with a request header") };
        let forget_below = *forget_below;
        let out = FrameOut::new(send);
        let arrival = d.arrive_request(&header);
        if !matches!(arrival, RequestArrival::New) {
            // A repeat: its own sections are read for the context it carries (its span joins the
            // caller's trace), never dispatched.
            let (reader, parent) = self.repeat_context(reader).await;
            let settled = match arrival {
                // A repeat while the original executes: pending and unacked until it settles.
                RequestArrival::Pending(settled) => settled.await.unwrap_or(Settled::Dropped),
                RequestArrival::Accepted => Settled::Accepted,
                RequestArrival::Ended(c) => Settled::Ended(c),
                RequestArrival::New => unreachable!("matched above"),
            };
            return self.answer_repeat(peer, &header, settled, out, reader, parent).await;
        }
        let (mut recv, rest) = reader.into_parts();
        let mut asm = RequestAssembly::new(&self.inner.catalog);
        let mut buf = vec![0u8; 16 * 1024];
        let mut permit: Option<Permit> = None;
        let mut refusal: Option<Refusal> = None;
        let mut first = Some(rest);
        let action = loop {
            let action = match first.take() {
                Some(b) if !b.is_empty() => asm.push(&b),
                Some(_) => ServerAction::Continue,
                None => match recv.read(&mut buf).await {
                    Ok(Some(n)) => asm.push(&buf[..n]),
                    Ok(None) => asm.finish(),
                    Err(ReadError::Reset(c)) => asm.reset(c.into_inner()),
                    Err(e) => break ServerAction::Drop { reason: if matches!(e, ReadError::ConnectionLost(_)) { "connection lost" } else { "stream closed" } },
                },
            };
            // Constant-cost guards once the op is known, before the body; a frame that arrived whole is
            // guarded the same before it is dispatched.
            if permit.is_none() && refusal.is_none() && matches!(action, ServerAction::Continue | ServerAction::Dispatch { .. }) {
                if let Some(op) = asm.head_op() {
                    // A closed gate refuses every op, ping included, before anything else.
                    if self.inner.ready.as_ref().is_some_and(|r| !r.load(Ordering::SeqCst)) {
                        stats.not_ready.fetch_add(1, Ordering::SeqCst);
                        tracing::info_span!("rdm.node_rpc.request.reject.via-not-ready", op, peer = %peer, code = "UNAVAILABLE", detail = "not-ready")
                            .in_scope(|| tracing::info!(op, "the node's ready gate is closed: typed NotReady"));
                        refusal = Some(Refusal::NotReady("this node is not ready: its authority is not filled yet".into()));
                    } else if self.inner.connections_deleting.load(Ordering::SeqCst) && !self.catalog().lookup(op).is_some_and(|e| e.served_while_connections_deleting) {
                        // Connections-deleting refuses every new call except the lifecycle control the catalog
                        // marks served while connections-deleting: the authority's probe and apply still land.
                        stats.connections_deleting.fetch_add(1, Ordering::SeqCst);
                        refusal = Some(Refusal::ConnectionsDeleting("node is connections-deleting".into()));
                    } else {
                        match self.inner.admission.try_admit(op, &peer.to_string()) {
                            Ok(p) => permit = Some(p),
                            Err(reason) => {
                                stats.busy.fetch_add(1, Ordering::SeqCst);
                                tracing::debug_span!("rdm.node_rpc.request.reject.via-busy", op, %reason).in_scope(|| tracing::debug!("busy"));
                                refusal = Some(Refusal::Busy(reason));
                            }
                        }
                    }
                }
            }
            if action != ServerAction::Continue {
                break action;
            }
        };
        match action {
            ServerAction::Continue => unreachable!("loop breaks only on a decision"),
            ServerAction::ResetUnserved { op } => {
                stats.unserved.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.request.reject.via-unserved-op", op, peer = %peer, code = "UNIMPLEMENTED", detail = "unserved-op")
                    .in_scope(|| tracing::info!(op, "unserved op: 421"));
                out.reset(ResetCode::UnservedOp).await;
                let _ = recv.stop(code(ResetCode::UnservedOp));
                d.settle_request(&id, Settled::Ended(u64::from(ResetCode::UnservedOp.code())));
            }
            ServerAction::RefuseMalformed { op, kind } => {
                stats.too_large.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.request.reject.via-malformed", op, kind = ?kind, code = rdm_core::outcome::ReplyKind::Malformed(kind).code().name(), detail = "malformed")
                    .in_scope(|| tracing::info!(?kind, "malformed request refused before the body"));
                let _ = recv.stop(code(ResetCode::RequestStop));
                self.answer_refusal(op, Refusal::Malformed(kind), &id, peer, out, None).await;
            }
            ServerAction::ResetViolation { op, reason } => {
                stats.violations.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.request.reject.via-protocol-violation", op = ?op, reason, code = "INTERNAL", detail = "protocol-violation")
                    .in_scope(|| tracing::info!(reason, "protocol violation: 424"));
                out.reset(ResetCode::ProtocolViolation).await;
                let _ = recv.stop(code(ResetCode::ProtocolViolation));
                d.settle_request(&id, Settled::Ended(u64::from(ResetCode::ProtocolViolation.code())));
            }
            ServerAction::Drop { reason } => {
                self.dropped(peer, reason);
                out.reset(ResetCode::RequestStop).await;
                d.settle_request(&id, Settled::Dropped);
            }
            ServerAction::Dispatch { op, header: request, payload } => {
                let acks = FrameReader::new(recv, asm.take_rest());
                if crate::delivery::DeliverySeams::take_cut(d.seams()) {
                    tracing::info_span!("rdm.node_rpc.connection.delete.via-seam", delivery_id = %id, peer = %peer)
                        .in_scope(|| tracing::info!("the test seam closes the connection after the request frame, before dispatch"));
                    conn.close(0u32.into(), b"seam: cut after the request frame");
                    d.settle_request(&id, Settled::Dropped);
                    return;
                }
                if let Some(r) = refusal {
                    return self.answer_refusal(op, r, &id, peer, out, Some(acks)).await;
                }
                let Some(h) = self.inner.handlers.get(&op).cloned() else {
                    out.reset(ResetCode::UnservedOp).await;
                    d.settle_request(&id, Settled::Ended(u64::from(ResetCode::UnservedOp.code())));
                    return;
                };
                stats.dispatched.fetch_add(1, Ordering::SeqCst);
                let _unsettled = Unsettled::enter(&self.inner.replies, op, &peer);
                // Bad or over-bound context is dropped here, named, and the call proceeds.
                let (context, dropped) = request.context.sanitized();
                if !dropped.is_empty() {
                    tracing::info_span!("rdm.node_rpc.request.update.via-context-dropped", decided_by = "target", op, peer = %peer, context_dropped = %dropped_as_str(&dropped))
                        .in_scope(|| tracing::info!("observability context dropped; the request is dispatched unchanged"));
                }
                let parent = crate::delivery::parent_of(&context);
                d.owe_response_in(&id, peer, parent.clone());
                let (acceptance, accepted) = Acceptance::new();
                let gate = Accepting::new(d.clone(), id.clone());
                // An acceptance the handler declares acks the request at once; otherwise its first
                // response frame does. A read-only request is accepted at dispatch: its ack is written
                // here, before its handler runs, so it always precedes the response on the stream (a
                // fast reply finishing the stream before a spawned ack is written left the ack on a
                // finished direction: rdm#216, `via-ack-not-written` of a request).
                let read_only = h.read_only(&payload);
                if read_only {
                    acceptance.accepted();
                    if gate.explicit() {
                        gate.d.write_ack(&out, &header, AckOf::Request).await;
                    }
                } else {
                    let (gate, out, header, mut accepted) = (gate.clone(), out.clone(), header.clone(), accepted);
                    let parent = parent.clone();
                    tokio::spawn(async move {
                        if accepted.wait_for(|a| *a).await.is_ok() && gate.explicit() {
                            let span = tracing::info_span!("rdm.node_rpc.delivery.serve.via-ack", delivery_id = %header.delivery_id(), acceptance = "recorded");
                            crate::delivery::parent_on(&span, parent.as_ref());
                            span.in_scope(|| tracing::info!("the handler recorded the accepted work: the request is acked"));
                            gate.d.write_ack(&out, &header, AckOf::Request).await;
                        }
                    });
                }
                let ctx = PeerContext { endpoint_id: peer, context, delivery: Some(CarriedIdentity { id: id.clone(), forget_below }), acceptance };
                let acks = tokio::spawn(Self::read_acks(d.clone(), id.clone(), acks));
                if h.is_stream() {
                    let stream_out = crate::stream::StreamOut::new(out.clone(), h.max_reply(), gate.clone());
                    let Some((serve, streaming)) = h.stream(ctx, payload, stream_out.clone()) else { return };
                    let counted = InFlight::enter(&stats);
                    let joined = tokio::spawn(async move {
                        let _counted = counted;
                        streaming.await
                    })
                    .await;
                    drop(permit);
                    // The stream's end is a step of its serve: its terminal (or refusal) is written and
                    // named under the serve span, which ends once it is written.
                    stream_out.finish(joined, &stats).instrument(serve).await;
                    // The stream held past its terminal until the caller's acks end the request direction.
                    let leg = tracing::info_span!("rdm.node_rpc.delivery.update.via-reply-held", delivery_id = %id, peer = %peer, held_ms = tracing::field::Empty, ended = tracing::field::Empty);
                    crate::delivery::parent_on(&leg, parent.as_ref());
                    let held_from = std::time::Instant::now();
                    let ended = acks.await.unwrap_or_else(|e| format!("{e}"));
                    leg.record("held_ms", held_from.elapsed().as_millis() as u64).record("ended", ended.as_str());
                    leg.in_scope(|| tracing::info!("the stream's terminal is written and the request direction ended: the stream is released"));
                    drop(leg);
                    if !d.response_settled(&id) {
                        d.push(&id, None, &format!("the stream ended with frames unacked; its request direction {ended}"));
                    }
                    return;
                }
                // Supervised: a panic is caught at this boundary.
                let (serve, handler) = h.call(ctx, payload);
                let counted = InFlight::enter(&stats);
                let joined = tokio::spawn(async move {
                    let _counted = counted;
                    handler.await
                })
                .await;
                drop(permit);
                // A fault or a panic is the end of the serve: named under the serve span, which ends here.
                if matches!(&joined, Ok(Err(_)) | Err(_)) {
                    serve.record("code", "INTERNAL");
                    serve.record("detail", "handler-fault");
                }
                serve.in_scope(|| match &joined {
                    Ok(Err(fault)) => tracing::info_span!("rdm.node_rpc.handler.reject.via-internal-rpc-failure", code = "INTERNAL", detail = "handler-fault", fault = fault.reason())
                        .in_scope(|| tracing::info!("handler fault: 423")),
                    Err(_panic) => tracing::info_span!("rdm.node_rpc.handler.reject.via-internal-rpc-failure", code = "INTERNAL", detail = "handler-fault", fault = "panic")
                        .in_scope(|| tracing::info!("handler panicked: 423")),
                    Ok(Ok(_)) => {}
                });
                drop(serve);
                match joined {
                    Ok(Ok(reply)) if reply.len() <= h.max_reply() => {
                        if let Some(w) = self.inner.withhold.as_ref().filter(|w| w.take_armed()) {
                            // The handler ran; its reply is not yet a frame: until the release the
                            // request stays unaccepted and a repeat of it pending.
                            w.wait_release().await;
                            gate.implicit();
                            // Every release is recorded: a reply whose direction ended before it (a
                            // read-only request's ack write that lost to a cut) is a failed late
                            // reply naming that end, and the frame stays owed.
                            let late = match d.owe_frame(&id, 0, reply, true) {
                                None => LateReply::unwritten("no reply is owed: the caller already finished the invocation".into()),
                                Some(frame) => match out.take().await {
                                    Ok(mut send) => {
                                        let late = ReplyWithhold::land(&mut send, &frame).await;
                                        let _ = send.finish();
                                        late
                                    }
                                    Err(why) => LateReply::unwritten(format!("the reply's direction ended before the release: {why}")),
                                },
                            };
                            let written = match &late.written {
                                Ok(()) => "written".to_string(),
                                Err(why) => why.clone(),
                            };
                            let span = tracing::info_span!("rdm.node_rpc.delivery.update.via-withheld-reply-released", delivery_id = %id, peer = %peer, written);
                            crate::delivery::parent_on(&span, parent.as_ref());
                            span.in_scope(|| tracing::info!("the test seam released the withheld reply"));
                            w.record(late);
                            let ended = acks.await.unwrap_or_else(|e| format!("{e}"));
                            if !d.response_settled(&id) {
                                d.push(&id, None, &format!("a withheld reply; the request direction {ended}"));
                            }
                            return;
                        }
                        // The reply leg, past the serve span's end: the reply's write and the stream held
                        // until the caller's ack of it ends the request direction (its credit to the
                        // caller's next stream open is only then returned).
                        let leg = tracing::info_span!("rdm.node_rpc.delivery.update.via-reply-held", delivery_id = %id, peer = %peer, write_ms = tracing::field::Empty, written = tracing::field::Empty, held_ms = tracing::field::Empty, ended = tracing::field::Empty);
                        crate::delivery::parent_on(&leg, parent.as_ref());
                        let reply_from = std::time::Instant::now();
                        gate.implicit();
                        // A caller that already finished the invocation is owed no reply.
                        let written = match d.owe_frame(&id, 0, reply, true) {
                            Some(frame) => out.send(&frame).await,
                            None => crate::delivery::Written::Ok,
                        };
                        out.finish().await;
                        // The reply leg, past the serve span's end: the stream stays held until the
                        // caller's ack of the reply ends the request direction.
                        leg.record("write_ms", reply_from.elapsed().as_millis() as u64).record("written", tracing::field::debug(&written));
                        let held_from = std::time::Instant::now();
                        // Settled when the caller acks the reply; when its direction ends first, the
                        // reply is resent on a new stream. A caller that dropped the invocation (it
                        // stopped the stream with code 0) is owed nothing.
                        if written == crate::delivery::Written::Stopped(0) {
                            d.drop_response(&id);
                        }
                        let ended = acks.await.unwrap_or_else(|e| format!("{e}"));
                        leg.record("held_ms", held_from.elapsed().as_millis() as u64).record("ended", ended.as_str());
                        leg.in_scope(|| tracing::info!("the reply is written and the request direction ended: the stream is released"));
                        drop(leg);
                        if !d.response_settled(&id) {
                            d.push(&id, None, &format!("the reply's write {written:?}; the request direction {ended}"));
                        }
                    }
                    Ok(Ok(_)) => {
                        stats.violations.fetch_add(1, Ordering::SeqCst);
                        out.reset(ResetCode::ProtocolViolation).await;
                        d.settle_request(&id, Settled::Ended(u64::from(ResetCode::ProtocolViolation.code())));
                        d.drop_response(&id);
                    }
                    Ok(Err(_)) | Err(_) => {
                        stats.faults.fetch_add(1, Ordering::SeqCst);
                        out.reset(ResetCode::InternalRpcFailure).await;
                        d.settle_request(&id, Settled::Ended(u64::from(ResetCode::InternalRpcFailure.code())));
                        d.drop_response(&id);
                    }
                }
            }
        }
    }
}

/// The first acceptance of one dispatched request, whichever comes first: the handler's declared
/// boundary (or a read-only request's, at dispatch), which writes an `Ack`; or the first response
/// frame, which acks the request by itself.
#[derive(Clone)]
pub(crate) struct Accepting {
    d: Delivery,
    id: rdm_core::framing::DeliveryId,
    done: Arc<AtomicBool>,
}

impl Accepting {
    fn new(d: Delivery, id: rdm_core::framing::DeliveryId) -> Self {
        Self { d, id, done: Arc::new(AtomicBool::new(false)) }
    }

    /// The handler declared acceptance: `true` when this is the first, and an `Ack` is to be written.
    fn explicit(&self) -> bool {
        if self.done.swap(true, Ordering::SeqCst) {
            return false;
        }
        self.d.settle_request(&self.id, Settled::Accepted);
        true
    }

    /// A response frame is about to be written: it accepts the request, if nothing did before.
    pub(crate) fn implicit(&self) {
        if !self.done.swap(true, Ordering::SeqCst) {
            self.d.settle_request(&self.id, Settled::Accepted);
        }
    }

    /// The delivery state and the invocation this acceptance is of.
    pub(crate) fn owed(&self) -> (&Delivery, &rdm_core::framing::DeliveryId) {
        (&self.d, &self.id)
    }
}

impl ProtocolHandler for NodeRpcServer {
    /// The handshake's end, as iroh's default awaits it, written to the transport diagnostic file
    /// (R-QH1): the remote's address, how long the handshake took and, on failure, the full error
    /// chain. Observation only: the outcome is returned as is.
    async fn on_accepting(&self, accepting: iroh::endpoint::Accepting) -> Result<Connection, AcceptError> {
        let remote_addr = accepting.remote_addr();
        let started = std::time::Instant::now();
        let r = accepting.await;
        let elapsed_ms = started.elapsed().as_millis() as u64;
        match &r {
            Ok(c) => tracing::info!(target: crate::TRANSPORT_DIAG_TARGET, step = "accept-completed", ?remote_addr, peer = %c.remote_id(), connection = c.stable_id(), elapsed_ms, "the accepted handshake completed"),
            Err(e) => tracing::info!(target: crate::TRANSPORT_DIAG_TARGET, step = "accept-failed", ?remote_addr, elapsed_ms, error = %format_args!("{e:#}"), error_debug = ?e, "the accepted handshake failed"),
        }
        Ok(r?)
    }

    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        let id = connection.stable_id();
        // The moment the handshake completed here, as its own span (it ends once the connection is
        // held, before its streams are served): matched by `peer` against the caller's `dialed` /
        // `dial-deadline` line, it says which side a slow connect waited on.
        tracing::info_span!("rdm.node_rpc.connection.create.via-accept", peer = %peer.fmt_short(), connection = id).in_scope(|| {
            tracing::info!(step = "accepted", "a node rpc connection was accepted");
            // Once per accepted connection, never per stream: the peer's live birth, as this
            // process resolves it now, is a recovery trigger (R-CN1), never a Direct fact.
            if let Some(inbound) = &self.inner.inbound {
                if let Some(node) = inbound.resolver.by_endpoint(&peer) {
                    inbound.trigger.accepted(&node);
                }
            }
            self.inner.connections.lock().unwrap().insert(id, connection.clone());
        });
        self.inner.delivery.register(&connection);
        while let Ok((send, recv)) = connection.accept_bi().await {
            tokio::spawn(self.clone().stream(connection.clone(), send, recv));
        }
        self.inner.delivery.unregister(&connection);
        self.inner.connections.lock().unwrap().remove(&id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ServedBirth, ServerBuilder};
    use rdm_core::ping::{Ping, PingReply, PingRequest};
    use std::net::SocketAddr;

    /// CONTRACT: docs/architecture/node-rpc-envelope.md:38
    /// "Every request carries a `CallContext` in its outer envelope"
    /// CONTRACT: docs/architecture/connections.md:384
    /// "node.connections.delete hard-cuts the live transport connection(s) it names"
    #[tokio::test]
    async fn apply_context_records_peer_metadata_and_close_connections_from_cuts_matching_peer_unless_spared() {
        // 1. `apply_context` records `caller_system` and allowlisted span baggage onto `span`.
        let secret = iroh::SecretKey::from_bytes(&[29u8; 32]);
        let mut ctx = CallContext::default();
        ctx.traceparent = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into());
        ctx.caller_system = Some("rdm-cli".into());
        ctx.baggage = Some("test_case=ctx-1".into());
        let peer_ctx = PeerContext {
            endpoint_id: secret.public(),
            context: ctx,
            delivery: None,
            acceptance: Acceptance::detached(),
        };
        let span = tracing::info_span!(
            "rdm.node_rpc.request.serve.via-direct",
            caller_system = tracing::field::Empty,
            test_case = tracing::field::Empty,
            scenario = tracing::field::Empty,
            operation = tracing::field::Empty,
        );
        apply_context(&span, &peer_ctx);

        // 2. `close_connections_from` spares the connection when `spare == Some(peer)` and cuts it otherwise.
        let server_ep = crate::endpoint::bind(
            iroh::SecretKey::from_bytes(&[31u8; 32]),
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        )
        .await
        .unwrap();
        let client_ep = crate::endpoint::bind(
            iroh::SecretKey::from_bytes(&[32u8; 32]),
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        )
        .await
        .unwrap();
        let client_peer = client_ep.id();

        let server = ServerBuilder::new()
            .serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
                let PingRequest::Ping { payload } = req;
                Ok(PingReply::Pong { payload })
            })
            .build(ServedBirth {
                node_id: "node-1".into(),
                incarnation: "inc-1".into(),
            })
            .unwrap();

        let server_addr = iroh::EndpointAddr::new(server_ep.id())
            .with_ip_addr(server_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap());

        let server_clone = server.clone();
        let server_ep_clone = server_ep.clone();
        let accept_task = tokio::spawn(async move {
            let incoming = server_ep_clone.accept().await.unwrap();
            let conn = server_clone.on_accepting(incoming.accept().unwrap()).await.unwrap();
            server_clone.inner.connections.lock().unwrap().insert(conn.stable_id(), conn.clone());
            conn
        });

        let client_conn = client_ep.connect(server_addr, crate::ALPN).await.unwrap();
        let _accepted_conn = accept_task.await.unwrap();

        // Spared peer is not cut:
        assert_eq!(server.close_connections_from(client_peer, Some(client_peer), "spared"), 0);
        assert!(client_conn.close_reason().is_none());

        // Unspared peer is hard-cut:
        assert_eq!(server.close_connections_from(client_peer, None, "cut-by-test"), 1);
        client_conn.closed().await;
        assert!(client_conn.close_reason().is_some());

        client_ep.close().await;
        server_ep.close().await;
    }
}

