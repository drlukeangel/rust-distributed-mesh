//! The Node RPC server (node-rpc.md §12–§19, §26–§32; ownership §6.3).
//!
//! Per bi-stream: read the fence (an unserved op → `421`; another node → `425`),
//! the context, the declared length
//! (oversize → typed `Malformed(TooLarge)`), take admission (full → typed
//! `Busy`), read the body, and dispatch only after the request direction
//! finished cleanly. A request the sender reset (`499`) or left unfinished is
//! dropped and never reaches a handler. Handler work runs supervised: a panic
//! or `HandlerFault` resets with `423 INTERNAL_RPC_FAILURE`.

use crate::admission::{Admission, Limits, Permit};
use iroh::endpoint::{Connection, ReadError, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use rafka_node_rpc_contract::catalog::{CatalogBuilder, CatalogEntry, SealError, SealedCatalog, Shape, OpOwner};
use rafka_node_rpc_contract::codes::ResetCode;
use rafka_node_rpc_contract::dispatch::{RequestAssembly, ServerAction};
use rafka_node_rpc_contract::framing::encode_frame;
use rafka_node_rpc_contract::outcome::MalformedKind;
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use rafka_node_rpc_contract::context::{dropped_as_str, CallContext};
use rafka_node_rpc_contract::framing::Fence;
use std::sync::Arc;
use tracing::Instrument;

/// Who is calling, as the transport proved it.
#[derive(Debug, Clone)]
pub struct PeerContext {
    pub endpoint_id: iroh::PublicKey,
    /// The observability context the request carried, already sanitized: what a carrier
    /// hands into its inner call unchanged.
    pub context: CallContext,
}

/// An unexpected execution fault. Created only by named constructors, so
/// every fault site is deliberate (node-rpc.md §32.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerFault(String);

impl HandlerFault {
    pub fn invariant_broken(what: impl Into<String>) -> Self {
        Self(what.into())
    }
    pub fn reason(&self) -> &str {
        &self.0
    }
}

pub(crate) type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A typed refusal encoded with the protocol's own constructors.
pub(crate) enum Refusal {
    Busy(String),
    Draining(String),
    Malformed(MalformedKind),
}

pub(crate) trait Erased: Send + Sync {
    fn call(&self, peer: PeerContext, payload: Vec<u8>) -> BoxFut<Result<Vec<u8>, HandlerFault>>;
    fn refusal(&self, r: Refusal) -> Vec<u8>;
    fn max_reply(&self) -> usize;
    /// A server-streaming handler: frames go out through `out` as the handler produces them.
    fn is_stream(&self) -> bool {
        false
    }
    fn stream(&self, _peer: PeerContext, _payload: Vec<u8>, _out: crate::stream::StreamOut) -> Option<BoxFut<crate::stream::StreamEnd>> {
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
    fn call(&self, peer: PeerContext, payload: Vec<u8>) -> BoxFut<Result<Vec<u8>, HandlerFault>> {
        let f = self.f.clone();
        Box::pin(async move {
            let req = match P::decode_request(&payload) {
                Ok(r) => r,
                Err(d) => {
                    tracing::info_span!("rdm.node_rpc.request.reject.via-malformed", protocol = P::NAME, kind = ?d)
                        .in_scope(|| tracing::info!(?d, "request does not decode"));
                    return P::encode_reply(&P::malformed(d.into())).map_err(|e| HandlerFault::invariant_broken(e.0));
                }
            };
            let span = tracing::info_span!(
                "rdm.node_rpc.request.serve.via-direct",
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
            let reply = f(peer, req).instrument(span).await?;
            P::encode_reply(&reply).map_err(|e| HandlerFault::invariant_broken(e.0))
        })
    }

    fn refusal(&self, r: Refusal) -> Vec<u8> {
        let reply = match r {
            Refusal::Busy(s) => P::busy(s),
            Refusal::Draining(s) => P::draining(s),
            Refusal::Malformed(k) => P::malformed(k),
        };
        P::encode_reply(&reply).unwrap_or_default()
    }

    fn max_reply(&self) -> usize {
        P::MAX_REPLY_FRAME_BYTES
    }
}

/// Parent `span` on the request's W3C context and record its caller identity and the
/// allowlisted baggage keys. The context never reaches the handler's decision: it is
/// evidence on the span and, through `peer`, what a carrier hands onward.
pub(crate) fn apply_context(span: &tracing::Span, peer: &PeerContext) {
    let c = &peer.context;
    if let Some(tp) = c.traceparent.as_deref() {
        rafka_mesh_telemetry::set_remote_parent(span, tp, c.tracestate.as_deref());
    }
    if let Some(s) = c.caller_system.as_deref() {
        span.record("caller_system", s);
    }
    for (k, v) in c.span_baggage() {
        span.record(k, v.as_str());
    }
}

/// Counters a functional test or an operator can read (pre-body refusals are
/// counted, not root-spanned per call; node-rpc.md §38.1).
#[derive(Debug, Default)]
pub struct ServerStats {
    pub dispatched: AtomicU64,
    pub dropped_unfinished: AtomicU64,
    pub unserved: AtomicU64,
    pub too_large: AtomicU64,
    pub busy: AtomicU64,
    pub draining: AtomicU64,
    pub violations: AtomicU64,
    /// Requests refused `425 STALE_TARGET` (another node).
    pub stale: AtomicU64,
    pub faults: AtomicU64,
    /// Handlers dispatched and not yet finished (WaitForDrain reads it).
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

impl ServerStats {
    pub fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::SeqCst)
    }
}

/// Registration happens at birth; `seal` consumes the builder.
#[derive(Default)]
pub struct ServerBuilder {
    pub(crate) catalog: CatalogBuilder,
    pub(crate) handlers: HashMap<u8, Arc<dyn Erased>>,
    admission: Admission,
    /// Forwardable protocols this node carries, op -> max inner reply (node-rpc.md §36.1).
    pub(crate) carried: HashMap<u8, usize>,
    /// Filled at seal for the forward handler, when this node serves it.
    pub(crate) carried_table: Option<Arc<std::sync::OnceLock<HashMap<u8, usize>>>>,
    /// The source-owned connections observer and the resolver that names an accepted peer,
    /// when the process writes its own connection facts (connections.md §10).
    inbound: Option<Inbound>,
}

/// An accepted connection reported to the process's connections observer as Direct Connected
/// from this node to the peer, once the resolver names the peer's live birth.
#[derive(Clone)]
struct Inbound {
    resolver: Arc<crate::LiveNodeResolver>,
    observer: Arc<dyn crate::ConnectionObserver>,
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self {
            catalog: CatalogBuilder::new(),
            handlers: HashMap::new(),
            admission: Admission::default(),
            carried: HashMap::new(),
            carried_table: None,
            inbound: None,
        }
    }

    /// Report every accepted connection whose peer `resolver` names as a live node to
    /// `observer` as `direct_accepted` (connections.md §10: a connection accepted from the
    /// destination is Direct Connected evidence like a dial of this node's own). A peer the
    /// resolver does not hold (a probe's ephemeral key) is reported to nothing.
    pub fn with_connection_observer(mut self, resolver: Arc<crate::LiveNodeResolver>, observer: Arc<dyn crate::ConnectionObserver>) -> Self {
        self.inbound = Some(Inbound { resolver, observer });
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

    pub fn ledger(mut self, rows: impl IntoIterator<Item = rafka_node_rpc_contract::catalog::LedgerEntry>) -> Self {
        self.catalog = self.catalog.ledger(rows);
        self
    }

    /// Compose a product's transitional adapter (`CatalogEntry::transitional`) into this
    /// process's one sealed catalog (node-rpc-rdm-ownership.md §12). The server runs no handler
    /// for it: the product's own dispatcher serves the op on its legacy framing and consults
    /// [`NodeRpcServer::catalog`] to refuse what the catalog does not hold. On this server's
    /// own ALPN the op is unserved (`421`), as any catalogued op without a handler is.
    pub fn adapter(mut self, entry: CatalogEntry) -> Self {
        self.catalog = self.catalog.serve(entry);
        self
    }

    pub fn limits(mut self, op: u8, limits: Limits) -> Self {
        self.admission.set(op, limits);
        self
    }

    /// Seal the catalog before the first stream is accepted, as `birth`: the
    /// exact node and incarnation this process is. A request is dispatched only
    /// when its fence names this node and a served op.
    pub fn seal(self, birth: ServedBirth) -> Result<NodeRpcServer, Vec<SealError>> {
        let catalog = self.catalog.seal()?;
        if let Some(table) = &self.carried_table {
            let _ = table.set(self.carried.clone());
        }
        Ok(NodeRpcServer {
            inner: Arc::new(Inner {
                catalog,
                handlers: self.handlers,
                admission: self.admission,
                draining: AtomicBool::new(false),
                stats: Arc::new(ServerStats::default()),
                birth,
                inbound: self.inbound,
            }),
        })
    }
}

/// The exact birth a server is: what every request's fence is checked against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedBirth {
    pub node_id: String,
    pub incarnation: String,
}

/// Which part of a request's fence this node is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceMismatch {
    NodeId,
}

impl FenceMismatch {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NodeId => "node_id",
        }
    }
}

struct Inner {
    catalog: SealedCatalog,
    handlers: HashMap<u8, Arc<dyn Erased>>,
    admission: Admission,
    draining: AtomicBool,
    stats: Arc<ServerStats>,
    birth: ServedBirth,
    inbound: Option<Inbound>,
}

/// One sealed server for the process's one endpoint. The socket a request
/// arrives on names nothing: the request's fence does.
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
    /// Is `fence` this node? `Err` names what is not. The op is the catalog's
    /// to check.
    pub fn check_fence(&self, fence: &Fence) -> Result<(), FenceMismatch> {
        if fence.target_node_id != self.inner.birth.node_id {
            return Err(FenceMismatch::NodeId);
        }
        Ok(())
    }

    pub fn is_current(&self, fence: &Fence) -> bool {
        self.check_fence(fence).is_ok()
    }

    /// The one sealed effective catalog of this process: what every dispatcher in it serves.
    pub fn catalog(&self) -> &SealedCatalog {
        &self.inner.catalog
    }

    pub fn stats(&self) -> Arc<ServerStats> {
        self.inner.stats.clone()
    }

    /// Enter `Draining`: new calls get a typed `Draining` refusal.
    pub fn drain(&self) {
        self.inner.draining.store(true, Ordering::SeqCst);
    }

    async fn refuse(&self, op: u8, r: Refusal, mut send: SendStream, mut recv: RecvStream) {
        if let Some(h) = self.inner.handlers.get(&op) {
            let bytes = encode_frame(&h.refusal(r));
            let _ = send.write_all(&bytes).await;
            let _ = send.finish();
        }
        let _ = recv.stop(code(ResetCode::RequestStop));
    }

    async fn invocation(self, peer: iroh::PublicKey, mut send: SendStream, mut recv: RecvStream) {
        let stats = self.inner.stats.clone();
        let me = self.clone();
        let current = move |f: &Fence| me.is_current(f);
        let mut asm = RequestAssembly::new(&self.inner.catalog, &current);
        let mut buf = vec![0u8; 16 * 1024];
        let mut permit: Option<Permit> = None;
        let action = loop {
            let action = match recv.read(&mut buf).await {
                Ok(Some(n)) => asm.push(&buf[..n]),
                Ok(None) => asm.finish(),
                Err(ReadError::Reset(c)) => asm.reset(c.into_inner()),
                Err(e) => break ServerAction::Drop { reason: if matches!(e, ReadError::ConnectionLost(_)) { "connection lost" } else { "stream closed" } },
            };
            if action != ServerAction::Continue {
                break action;
            }
            // Constant-cost guards once the op is known, before the body.
            if permit.is_none() {
                if let Some(op) = asm.head_op() {
                    // Draining refuses every new call except the lifecycle control the catalog
                    // marks served while draining: the authority's probe and apply still land.
                    if self.inner.draining.load(Ordering::SeqCst) && !self.catalog().lookup(op).is_some_and(|e| e.served_while_draining) {
                        stats.draining.fetch_add(1, Ordering::SeqCst);
                        return self.refuse(op, Refusal::Draining("node is draining".into()), send, recv).await;
                    }
                    match self.inner.admission.try_admit(op, &peer.to_string()) {
                        Ok(p) => permit = Some(p),
                        Err(reason) => {
                            stats.busy.fetch_add(1, Ordering::SeqCst);
                            tracing::debug_span!("rdm.node_rpc.request.reject.via-busy", op, %reason).in_scope(|| tracing::debug!("busy"));
                            return self.refuse(op, Refusal::Busy(reason), send, recv).await;
                        }
                    }
                }
            }
        };
        match action {
            ServerAction::Continue => unreachable!("loop breaks only on a decision"),
            ServerAction::ResetUnserved { op } => {
                stats.unserved.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.request.reject.via-unserved-op", op, peer = %peer)
                    .in_scope(|| tracing::info!(op, "unserved op: 421"));
                let _ = send.reset(code(ResetCode::UnservedOp));
                let _ = recv.stop(code(ResetCode::UnservedOp));
            }
            ServerAction::ResetStale { op, fence } => {
                stats.stale.fetch_add(1, Ordering::SeqCst);
                let mismatch = self.check_fence(&fence).err().map(FenceMismatch::as_str).unwrap_or("");
                let span = tracing::info_span!(
                    "rdm.node_rpc.connection.reject.via-stale-target",
                    decided_by = "target",
                    op,
                    peer = %peer,
                    node_id = %fence.target_node_id,
                    op = fence.op,
                    mismatch,
                    receiver_node_id = %self.inner.birth.node_id,
                );
                // Decided on the fence alone: the context was never read, so the refusal carries
                // no caller trace and no caller_system (node-rpc-envelope.md, the fence checks).
                span.in_scope(|| tracing::info!("the request's fence is not this node: 425"));
                let _ = send.reset(code(ResetCode::StaleTarget));
                let _ = recv.stop(code(ResetCode::StaleTarget));
            }
            ServerAction::RefuseMalformed { op, kind } => {
                stats.too_large.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.request.reject.via-malformed", op, kind = ?kind)
                    .in_scope(|| tracing::info!(?kind, "malformed request refused before the body"));
                self.refuse(op, Refusal::Malformed(kind), send, recv).await;
            }
            ServerAction::ResetViolation { op, reason } => {
                stats.violations.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.request.reject.via-protocol-violation", op = ?op, reason)
                    .in_scope(|| tracing::info!(reason, "protocol violation: 424"));
                let _ = send.reset(code(ResetCode::ProtocolViolation));
                let _ = recv.stop(code(ResetCode::ProtocolViolation));
            }
            ServerAction::Drop { reason } => {
                stats.dropped_unfinished.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rdm.node_rpc.request.reject.via-frame-not-sent", reason, peer = %peer)
                    .in_scope(|| tracing::info!(reason, "unfinished request dropped; never dispatched"));
                let _ = send.reset(code(ResetCode::RequestStop));
            }
            ServerAction::Dispatch { op, header, payload } => {
                let Some(h) = self.inner.handlers.get(&op).cloned() else {
                    let _ = send.reset(code(ResetCode::UnservedOp));
                    return;
                };
                stats.dispatched.fetch_add(1, Ordering::SeqCst);
                // Bad or over-bound context is dropped here, named, and the call proceeds.
                let (context, dropped) = header.context.sanitized();
                if !dropped.is_empty() {
                    tracing::info_span!("rdm.node_rpc.request.update.via-context-dropped", decided_by = "target", op, peer = %peer, context_dropped = %dropped_as_str(&dropped))
                        .in_scope(|| tracing::info!("observability context dropped; the request is dispatched unchanged"));
                }
                let ctx = PeerContext { endpoint_id: peer, context };
                let out = crate::stream::StreamOut::new(send, h.max_reply());
                if h.is_stream() {
                    let Some(streaming) = h.stream(ctx, payload, out.clone()) else { return };
                    let counted = InFlight::enter(&stats);
                    let joined = tokio::spawn(async move {
                        let _counted = counted;
                        streaming.await
                    })
                    .await;
                    drop(permit);
                    out.finish(joined, &stats).await;
                    return;
                }
                let Some(mut send) = out.take().await else { return };
                // Supervised: a panic is caught at this boundary.
                let handler = h.call(ctx, payload);
                let counted = InFlight::enter(&stats);
                let joined = tokio::spawn(async move {
                    let _counted = counted;
                    handler.await
                })
                .await;
                drop(permit);
                match joined {
                    Ok(Ok(reply)) if reply.len() <= h.max_reply() => {
                        let _ = send.write_all(&encode_frame(&reply)).await;
                        let _ = send.finish();
                    }
                    Ok(Ok(_)) => {
                        stats.violations.fetch_add(1, Ordering::SeqCst);
                        let _ = send.reset(code(ResetCode::ProtocolViolation));
                    }
                    Ok(Err(fault)) => {
                        stats.faults.fetch_add(1, Ordering::SeqCst);
                        tracing::info_span!("rdm.node_rpc.handler.reject.via-internal-rpc-failure", fault = fault.reason())
                            .in_scope(|| tracing::info!("handler fault: 423"));
                        let _ = send.reset(code(ResetCode::InternalRpcFailure));
                    }
                    Err(_panic) => {
                        stats.faults.fetch_add(1, Ordering::SeqCst);
                        tracing::info_span!("rdm.node_rpc.handler.reject.via-internal-rpc-failure", fault = "panic")
                            .in_scope(|| tracing::info!("handler panicked: 423"));
                        let _ = send.reset(code(ResetCode::InternalRpcFailure));
                    }
                }
            }
        }
    }
}

impl ProtocolHandler for NodeRpcServer {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        // Once per accepted connection, never per stream: the peer's live birth, as this
        // process resolves it now, is Direct Connected from this node (connections.md §10).
        if let Some(inbound) = &self.inner.inbound {
            if let Some(node) = inbound.resolver.by_endpoint(&peer) {
                inbound.observer.direct_accepted(&node);
            }
        }
        while let Ok((send, recv)) = connection.accept_bi().await {
            tokio::spawn(self.clone().invocation(peer, send, recv));
        }
        Ok(())
    }
}
