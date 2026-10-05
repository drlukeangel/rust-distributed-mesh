//! The Node RPC server (node-rpc.md §12–§19, §26–§32; ownership §6.3).
//!
//! Per bi-stream: read the tag (unserved → `421`), the declared length
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
use std::sync::Arc;
use tracing::Instrument;

/// Who is calling, as the transport proved it.
#[derive(Debug, Clone)]
pub struct PeerContext {
    pub transport_id: iroh::PublicKey,
    /// The local endpoint slot the call arrived on.
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

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A typed refusal encoded with the protocol's own constructors.
enum Refusal {
    Busy(String),
    Draining(String),
    Malformed(MalformedKind),
}

trait Erased: Send + Sync {
    fn call(&self, peer: PeerContext, payload: Vec<u8>) -> BoxFut<Result<Vec<u8>, HandlerFault>>;
    fn refusal(&self, r: Refusal) -> Vec<u8>;
    fn max_reply(&self) -> usize;
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
                rafka_telemetry::set_parent(&span, tp);
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
    catalog: CatalogBuilder,
    handlers: HashMap<u8, Arc<dyn Erased>>,
    admission: Admission,
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self { catalog: CatalogBuilder::new(), handlers: HashMap::new(), admission: Admission::default() }
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

    /// Seal the catalog before the first stream is accepted.
    pub fn seal(self, slot: impl Into<String>) -> Result<NodeRpcServer, Vec<SealError>> {
        let catalog = self.catalog.seal()?;
        Ok(NodeRpcServer {
            inner: Arc::new(Inner {
                catalog,
                handlers: self.handlers,
                admission: self.admission,
                draining: AtomicBool::new(false),
                stats: Arc::new(ServerStats::default()),
            }),
            slot: slot.into(),
        })
    }
}

struct Inner {
    catalog: SealedCatalog,
    handlers: HashMap<u8, Arc<dyn Erased>>,
    admission: Admission,
    draining: AtomicBool,
    stats: Arc<ServerStats>,
}

/// One sealed server, shareable across a node's endpoint slots.
#[derive(Clone)]
pub struct NodeRpcServer {
    inner: Arc<Inner>,
    slot: String,
}

impl std::fmt::Debug for NodeRpcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRpcServer").field("slot", &self.slot).finish()
    }
}

fn code(c: ResetCode) -> VarInt {
    VarInt::from_u32(c.code())
}

impl NodeRpcServer {
    /// The same server answering on another local endpoint slot.
    pub fn for_slot(&self, slot: impl Into<String>) -> Self {
        Self { inner: self.inner.clone(), slot: slot.into() }
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
        let mut asm = RequestAssembly::new(&self.inner.catalog);
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
                tracing::info_span!("rafka.node_rpc.request.reject.via-unserved-tag", tag, peer = %peer, slot = %self.slot)
                    .in_scope(|| tracing::info!(tag, "unserved tag: 421"));
                let _ = send.reset(code(ResetCode::UnservedTag));
                let _ = recv.stop(code(ResetCode::UnservedTag));
            }
            ServerAction::RefuseMalformed { tag, kind } => {
                stats.too_large.fetch_add(1, Ordering::SeqCst);
                tracing::info_span!("rafka.node_rpc.request.reject.via-malformed", tag, kind = ?kind, slot = %self.slot)
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
                tracing::info_span!("rafka.node_rpc.request.reject.via-frame-not-sent", reason, peer = %peer, slot = %self.slot)
                    .in_scope(|| tracing::info!(reason, "unfinished request dropped; never dispatched"));
                let _ = send.reset(code(ResetCode::RequestStop));
            }
            ServerAction::Dispatch { tag, payload } => {
                let Some(h) = self.inner.handlers.get(&tag).cloned() else {
                    let _ = send.reset(code(ResetCode::UnservedTag));
                    return;
                };
                stats.dispatched.fetch_add(1, Ordering::SeqCst);
                let ctx = PeerContext { transport_id: peer, slot: self.slot.clone() };
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
