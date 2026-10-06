//! The Node RPC server (node-rpc.md §12–§19, §26–§32; ownership §6.3).
//!
//! Per bi-stream: read the tag (unserved → `421`), the target slot and its
//! freshness (not this birth's current one → `425`), the declared length
//! (oversize → typed `Malformed(TooLarge)`), take admission (full → typed
//! `Busy`), read the body, and dispatch only after the request direction
//! finished cleanly. A request the sender reset (`499`) or left unfinished is
//! dropped and never reaches a handler. Handler work runs supervised: a panic
//! or `HandlerFault` resets with `423 INTERNAL_RPC_FAILURE`.

use crate::admission::{Admission, Limits, Permit};
use iroh::endpoint::{Connection, ReadError, RecvStream, SendStream, VarInt};
use iroh::protocol::{AcceptError, ProtocolHandler};
use rafka_node_rpc_contract::catalog::{CatalogBuilder, CatalogEntry, SealError, SealedCatalog, Shape, TagOwner};
use rafka_node_rpc_contract::codes::ResetCode;
use rafka_node_rpc_contract::dispatch::{RequestAssembly, ServerAction};
use rafka_node_rpc_contract::framing::encode_frame;
use rafka_node_rpc_contract::outcome::MalformedKind;
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use rafka_mesh_entity::{EndpointSlot, FreshnessToken};
use rafka_node_rpc_contract::framing::RequestTarget;
use std::sync::{Arc, RwLock};
use tracing::Instrument;

/// Who is calling, as the transport proved it.
#[derive(Debug, Clone)]
pub struct PeerContext {
    pub transport_id: iroh::PublicKey,
    /// The endpoint slot the request named (and the server held under its token).
    pub slot: String,
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
                    tracing::info_span!("rafka.node_rpc.request.reject.via-malformed", protocol = P::NAME, kind = ?d)
                        .in_scope(|| tracing::info!(?d, "request does not decode"));
                    return P::encode_reply(&P::malformed(d.into())).map_err(|e| HandlerFault::invariant_broken(e.0));
                }
            };
            let span = tracing::info_span!(
                "rafka.node_rpc.request.serve.via-direct",
                protocol = P::NAME,
                tag = P::TAG,
                peer = %peer.transport_id,
                slot = %peer.slot,
            );
            if let Some(tp) = P::traceparent(&req) {
                rafka_mesh_telemetry::set_parent(&span, tp);
            }
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
    /// Requests refused `425 STALE_SLOT`.
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
    /// Forwardable protocols this node carries, tag -> max inner reply (node-rpc.md §36.1).
    pub(crate) carried: HashMap<u8, usize>,
    /// Filled at seal for the forward handler, when this node serves it.
    pub(crate) carried_table: Option<Arc<std::sync::OnceLock<HashMap<u8, usize>>>>,
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self {
            catalog: CatalogBuilder::new(),
            handlers: HashMap::new(),
            admission: Admission::default(),
            carried: HashMap::new(),
            carried_table: None,
        }
    }

    /// Serve unary protocol `P` (owned by `owner`) with handler `f`.
    pub fn serve<P, F, Fut>(mut self, owner: TagOwner, f: F) -> Self
    where
        P: NodeProtocol,
        F: Fn(PeerContext, P::Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<P::Reply, HandlerFault>> + Send + 'static,
    {
        self.catalog = self.catalog.serve(CatalogEntry::canonical::<P>(owner, Shape::Unary));
        self.handlers.insert(P::TAG, Arc::new(Typed::<P, F> { f: Arc::new(f), _p: std::marker::PhantomData }));
        self
    }

    pub fn ledger(mut self, rows: impl IntoIterator<Item = rafka_node_rpc_contract::catalog::LedgerEntry>) -> Self {
        self.catalog = self.catalog.ledger(rows);
        self
    }

    pub fn limits(mut self, tag: u8, limits: Limits) -> Self {
        self.admission.set(tag, limits);
        self
    }

    /// Seal the catalog before the first stream is accepted, as `birth`: the
    /// exact node and incarnation this process is, serving `slots` under their
    /// freshness tokens. A request is dispatched only when it names this birth
    /// and one of its slots under the slot's current token.
    pub fn seal(self, birth: ServedBirth, slots: impl IntoIterator<Item = EndpointSlot>) -> Result<NodeRpcServer, Vec<SealError>> {
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
                slots: RwLock::new(slots.into_iter().map(|s| (s.slot, s.freshness)).collect()),
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

/// Which part of a request's fence was not current.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceMismatch {
    NodeId,
    Incarnation,
    Slot,
    Freshness,
}

impl FenceMismatch {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NodeId => "node_id",
            Self::Incarnation => "incarnation",
            Self::Slot => "slot",
            Self::Freshness => "freshness",
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
    /// Every slot this birth serves, under its current freshness token.
    slots: RwLock<HashMap<String, FreshnessToken>>,
}

/// One sealed server for every endpoint slot of the process's one endpoint.
/// The socket a request arrives on names no slot: the request's target does.
#[derive(Clone)]
pub struct NodeRpcServer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for NodeRpcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRpcServer").field("slots", &*self.inner.slots.read().unwrap()).finish()
    }
}

fn code(c: ResetCode) -> VarInt {
    VarInt::from_u32(c.code())
}

impl NodeRpcServer {
    /// Assign `slot` (a new freshness token, or a slot this birth did not
    /// hold): a request naming the slot's previous token is `425` from now.
    pub fn assign(&self, slot: EndpointSlot) {
        self.inner.slots.write().unwrap().insert(slot.slot, slot.freshness);
    }

    /// Is `target` this exact birth, at one of its slots under exactly the
    /// slot's current token? `Err` names the first part that is not.
    pub fn check_fence(&self, target: &RequestTarget) -> Result<(), FenceMismatch> {
        if target.node_id != self.inner.birth.node_id {
            return Err(FenceMismatch::NodeId);
        }
        if target.incarnation != self.inner.birth.incarnation {
            return Err(FenceMismatch::Incarnation);
        }
        match self.inner.slots.read().unwrap().get(&target.slot) {
            None => Err(FenceMismatch::Slot),
            Some(f) if f.to_string() != target.freshness => Err(FenceMismatch::Freshness),
            Some(_) => Ok(()),
        }
    }

    pub fn is_current(&self, target: &RequestTarget) -> bool {
        self.check_fence(target).is_ok()
    }

    fn current_token(&self, slot: &str) -> Option<String> {
        self.inner.slots.read().unwrap().get(slot).map(|f| f.to_string())
    }

    pub fn stats(&self) -> Arc<ServerStats> {
        self.inner.stats.clone()
    }

    /// Enter `Draining`: new calls get a typed `Draining` refusal.
    pub fn drain(&self) {
        self.inner.draining.store(true, Ordering::SeqCst);
    }

    async fn refuse(&self, tag: u8, r: Refusal, mut send: SendStream, mut recv: RecvStream) {
        if let Some(h) = self.inner.handlers.get(&tag) {
            let bytes = encode_frame(&h.refusal(r));
            let _ = send.write_all(&bytes).await;
            let _ = send.finish();
        }
        let _ = recv.stop(code(ResetCode::RequestStop));
    }

    async fn invocation(self, peer: iroh::PublicKey, mut send: SendStream, mut recv: RecvStream) {
        let stats = self.inner.stats.clone();
        let me = self.clone();
        let current = move |t: &RequestTarget| me.is_current(t);
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
            // Constant-cost guards once the tag is known, before the body.
            if permit.is_none() {
                if let Some(tag) = asm.head_tag() {
                    if self.inner.draining.load(Ordering::SeqCst) {
                        stats.draining.fetch_add(1, Ordering::SeqCst);
                        return self.refuse(tag, Refusal::Draining("node is draining".into()), send, recv).await;
                    }
                    match self.inner.admission.try_admit(tag, &peer.to_string()) {
                        Ok(p) => permit = Some(p),
                        Err(reason) => {
                            stats.busy.fetch_add(1, Ordering::SeqCst);
                            tracing::debug_span!("rafka.node_rpc.request.reject.via-busy", tag, %reason).in_scope(|| tracing::debug!("busy"));
                            return self.refuse(tag, Refusal::Busy(reason), send, recv).await;
                        }
                    }
                }
            }
        };
        match action {
            ServerAction::Continue => unreachable!("loop breaks only on a decision"),
            ServerAction::ResetUnserved { tag } => {
                stats.unserved.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rafka.node_rpc.request.reject.via-unserved-tag", tag, peer = %peer)
                    .in_scope(|| tracing::info!(tag, "unserved tag: 421"));
                let _ = send.reset(code(ResetCode::UnservedTag));
                let _ = recv.stop(code(ResetCode::UnservedTag));
            }
            ServerAction::ResetStale { tag, target } => {
                stats.stale.fetch_add(1, Ordering::SeqCst);
                let mismatch = self.check_fence(&target).err().map(FenceMismatch::as_str).unwrap_or("");
                tracing::info_span!(
                    "rafka.node_rpc.connection.reject.via-stale-slot",
                    decided_by = "target",
                    tag,
                    peer = %peer,
                    node_id = %target.node_id,
                    incarnation_id = %target.incarnation,
                    slot = %target.slot,
                    freshness = %target.freshness,
                    mismatch,
                    current = self.current_token(&target.slot).unwrap_or_default(),
                )
                .in_scope(|| tracing::info!("the request's fence is not this birth's current state: 425"));
                let _ = send.reset(code(ResetCode::StaleSlot));
                let _ = recv.stop(code(ResetCode::StaleSlot));
            }
            ServerAction::RefuseMalformed { tag, kind } => {
                stats.too_large.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rafka.node_rpc.request.reject.via-malformed", tag, kind = ?kind)
                    .in_scope(|| tracing::info!(?kind, "malformed request refused before the body"));
                self.refuse(tag, Refusal::Malformed(kind), send, recv).await;
            }
            ServerAction::ResetViolation { tag, reason } => {
                stats.violations.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rafka.node_rpc.request.reject.via-protocol-violation", tag = ?tag, reason)
                    .in_scope(|| tracing::info!(reason, "protocol violation: 424"));
                let _ = send.reset(code(ResetCode::ProtocolViolation));
                let _ = recv.stop(code(ResetCode::ProtocolViolation));
            }
            ServerAction::Drop { reason } => {
                stats.dropped_unfinished.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rafka.node_rpc.request.reject.via-frame-not-sent", reason, peer = %peer)
                    .in_scope(|| tracing::info!(reason, "unfinished request dropped; never dispatched"));
                let _ = send.reset(code(ResetCode::RequestStop));
            }
            ServerAction::Dispatch { tag, target, payload } => {
                let Some(h) = self.inner.handlers.get(&tag).cloned() else {
                    let _ = send.reset(code(ResetCode::UnservedTag));
                    return;
                };
                stats.dispatched.fetch_add(1, Ordering::SeqCst);
                let ctx = PeerContext { transport_id: peer, slot: target.slot };
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
                        tracing::info_span!("rafka.node_rpc.handler.reject.via-internal-rpc-failure", fault = fault.reason())
                            .in_scope(|| tracing::info!("handler fault: 423"));
                        let _ = send.reset(code(ResetCode::InternalRpcFailure));
                    }
                    Err(_panic) => {
                        stats.faults.fetch_add(1, Ordering::SeqCst);
                        tracing::info_span!("rafka.node_rpc.handler.reject.via-internal-rpc-failure", fault = "panic")
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
        while let Ok((send, recv)) = connection.accept_bi().await {
            tokio::spawn(self.clone().invocation(peer, send, recv));
        }
        Ok(())
    }
}
