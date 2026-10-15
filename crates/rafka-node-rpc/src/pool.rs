//! The scoped connection pool (i143 PRD §1.19–20, §14;
//! node-rpc-envelope.md §8–§9).
//!
//! A pooled connection is keyed by `(scope, peer, incarnation)`: the caller's
//! execution scope, the peer's authenticated Iroh key and the process birth it
//! was dialled to, or no birth for a call to an endpoint the caller holds no
//! record of (`NodeTarget::Endpoint`). A connection is to a process, and every
//! op of a birth shares the connection.
//!
//! Supersession is per birth:
//! - a dial in flight whose incarnation the resolver no longer names is
//!   cancelled the moment the resolver changes, and its waiters get
//!   `RejectedStale` at once;
//! - a dial that completes after its birth was superseded is never pooled;
//! - pooled connections of a superseded birth are evicted.
//!
//! One dial per key is in flight at a time; concurrent callers share it. A dial is bounded by no
//! caller's budget: each caller bounds its own wait, so a caller that joins a dial with a longer
//! budget is not ended by the starter's shorter one.
//!
//! Eviction removes the entry only; it never closes the connection, so a call
//! already riding it finishes. Eviction is connection health, not
//! reachability: nothing here marks a node unreachable.
//!
//! Eviction is structural (node-rpc.md §21): Iroh reports the connection closed
//! or lost (when it is taken from the pool, or on a stream open, the request
//! write or the reply read), or the resolver no longer names its birth. A reply
//! deadline is no evidence about the connection: the call ends `Indeterminate`
//! and the connection stays pooled.

use crate::resolve::{NodeResolver, NodeTarget, ResolvedNode};
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr};
use rdm_core::IncarnationId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Notify};
use tokio::time::Instant;
use tracing::instrument::WithSubscriber;

/// One pooled connection's identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    /// The caller's execution scope.
    pub scope: Option<String>,
    /// The peer's Iroh public key.
    pub peer: iroh::PublicKey,
    /// The incarnation of the peer's birth the connection reached; `None` for a call to an endpoint
    /// the caller holds no record of, which nothing supersedes.
    pub incarnation: Option<IncarnationId>,
}

impl PoolKey {
    /// Does `node` (the resolver's answer now) still name this exact birth? A key of no birth is
    /// the process at its endpoint, whatever a resolver says.
    pub(crate) fn is_current(&self, node: Option<&ResolvedNode>) -> bool {
        match &self.incarnation {
            None => true,
            Some(held) => node.is_some_and(|n| n.endpoint_id == self.peer && &n.incarnation == held),
        }
    }

    /// The birth the key names, as a span field: empty for a key of no birth.
    pub fn birth(&self) -> &str {
        self.incarnation.as_ref().map(|i| i.as_str()).unwrap_or_default()
    }

    fn superseded_by(&self, _node: Option<&ResolvedNode>) -> &'static str {
        "via-incarnation-superseded"
    }
}

/// A pooled connection a close ended: the key it was pooled under and its `stable_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedConnection {
    /// The key it was pooled under: the peer and the birth it reached.
    pub key: PoolKey,
    /// The connection's `stable_id`.
    pub connection: usize,
}

/// Test failpoint: a dial that connected stops here, before it is checked
/// against the resolver and pooled, until released.
#[derive(Default)]
pub struct Failpoint {
    /// Notified when the dial has connected and stopped at the failpoint.
    pub reached: Notify,
    /// Notified to let the stopped dial continue.
    pub release: Notify,
}

impl std::fmt::Debug for Failpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Failpoint")
    }
}

/// How a dial ended, for every caller waiting on it.
#[derive(Debug, Clone)]
pub(crate) enum DialError {
    /// The birth moved while the dial ran, or before it pooled.
    Superseded,
    /// The source admits no new connection ([`Admission`]): refused before a dial (`at = "dial"`),
    /// or the connection a dial opened was closed before it was pooled (`at = "pool"`).
    Blocked {
        /// Where the block refused it: `dial` or `pool`.
        at: &'static str,
        /// Why the source admits none.
        why: String,
    },
    /// This caller's own deadline passed first (the dial may run on for others).
    Deadline,
    Failed(String),
}

type Outcome = Option<Result<Connection, DialError>>;

struct Dial {
    cancel: watch::Sender<bool>,
    outcome: watch::Receiver<Outcome>,
    addr: SocketAddr,
}

struct Pooled {
    conn: Connection,
    addr: SocketAddr,
    reported: bool,
}

#[derive(Default)]
struct Inner {
    conns: Mutex<HashMap<PoolKey, Pooled>>,
    dials: Mutex<HashMap<PoolKey, Dial>>,
}

/// The pool a [`crate::NodeRpcClient`] owns.
#[derive(Default, Clone)]
pub struct Pool {
    inner: Arc<Inner>,
}

/// What a dial needs to run on its own.
pub(crate) struct DialSpec {
    pub endpoint: Endpoint,
    pub resolver: Arc<dyn NodeResolver>,
    /// What the resolver is asked about the dial while it runs and before it pools: `None` for an
    /// endpoint target, which no resolver answers and nothing supersedes.
    pub target: Option<NodeTarget>,
    pub addr: SocketAddr,
    pub failpoint: Option<Arc<Failpoint>>,
    /// Told once, when the dial's connection enters the pool, whoever is or is not waiting for it.
    pub opened: Option<Arc<dyn Fn() + Send + Sync>>,
    /// The client's admission block: read before a dial starts or is joined, and again before the
    /// connection it opens is pooled.
    pub admission: Admission,
}

/// Whether a client opens new connections: `None` admits, `Some(why)` refuses every new one,
/// naming why (`NodeRpcClient::block_new_connections`). Shared by the client and its dials.
#[derive(Clone, Default)]
pub(crate) struct Admission(Arc<std::sync::RwLock<Option<String>>>);

impl Admission {
    pub(crate) fn block(&self, why: String) {
        *self.0.write().unwrap() = Some(why);
    }

    pub(crate) fn lift(&self) {
        *self.0.write().unwrap() = None;
    }

    /// Why no new connection opens now, if the block is set.
    pub(crate) fn blocked(&self) -> Option<String> {
        self.0.read().unwrap().clone()
    }
}

fn evict_span(_reason: &'static str, key: &PoolKey, outcome: &str, elapsed_ms: u128) {
    tracing::info_span!(
        "rdm.node_rpc.connection.evict.via-incarnation-superseded",
        peer = %key.peer.fmt_short(), incarnation_id = %key.birth(), outcome, elapsed_ms = elapsed_ms as u64
    )
    .in_scope(|| tracing::info!("superseded birth: {outcome}"));
}

/// The dial's outcome in the transport diagnostic file (R-QH1): the peer key the dial expected, the
/// address it dialled, the endpoint's own socket, the dial's trace (its `traceparent`, so the line
/// and the flame name each other) and, on failure, the full error chain. Observation only.
fn diag_connect<E: std::error::Error>(key: &PoolKey, spec: &DialSpec, r: &Result<Connection, E>, started: Instant, span: &tracing::Span) {
    let trace = rafka_mesh_telemetry::traceparent_of(span).unwrap_or_default();
    let (peer, addr, local, elapsed_ms) = (&key.peer, spec.addr, spec.endpoint.bound_sockets(), started.elapsed().as_millis() as u64);
    let me = spec.endpoint.id();
    match r {
        Ok(c) => tracing::info!(target: rafka_mesh_telemetry::TRANSPORT_DIAG_TARGET, step = "connect-completed", %me, %peer, %addr, ?local, incarnation = %key.birth(), connection = c.stable_id(), elapsed_ms, %trace, "the dial's handshake completed"),
        Err(e) => tracing::info!(target: rafka_mesh_telemetry::TRANSPORT_DIAG_TARGET, step = "connect-failed", %me, %peer, %addr, ?local, incarnation = %key.birth(), elapsed_ms, %trace, error = %format_args!("{e:#}"), error_debug = ?e, "the dial's handshake failed"),
    }
}

impl Pool {
    /// Every pooled key (diagnostics and tests).
    pub fn keys(&self) -> Vec<PoolKey> {
        self.inner.conns.lock().unwrap().keys().cloned().collect()
    }

    /// Every key with a dial in flight (diagnostics and tests).
    pub fn dialing(&self) -> Vec<PoolKey> {
        self.inner.dials.lock().unwrap().keys().cloned().collect()
    }

    /// Evict every pooled connection and cancel every dial of `node`'s peer
    /// whose target `node` no longer names; adopt any birthless endpoint
    /// connection at `node.transport_addr` under `node`'s birth.
    pub(crate) fn purge_stale(&self, node: &ResolvedNode) {
        let gone: Vec<PoolKey> = {
            let mut conns = self.inner.conns.lock().unwrap();
            let peer_keys: Vec<PoolKey> = conns.keys().filter(|k| k.peer == node.endpoint_id).cloned().collect();
            let mut gone = Vec::new();
            for k in peer_keys {
                match &k.incarnation {
                    None => {
                        let Some(p) = conns.remove(&k) else { continue };
                        if p.addr == node.transport_addr {
                            if p.conn.close_reason().is_none() {
                                let promoted = PoolKey { scope: k.scope, peer: k.peer, incarnation: Some(node.incarnation.clone()) };
                                conns.entry(promoted).or_insert(p);
                            }
                        } else {
                            gone.push(k);
                        }
                    }
                    Some(held) if held != &node.incarnation => {
                        conns.remove(&k);
                        gone.push(k);
                    }
                    Some(_) => {}
                }
            }
            gone
        };
        for k in gone {
            evict_span(k.superseded_by(Some(node)), &k, "evicted", 0);
        }
        for (k, d) in self.inner.dials.lock().unwrap().iter() {
            if k.peer != node.endpoint_id {
                continue;
            }
            let stale = match &k.incarnation {
                None => d.addr != node.transport_addr,
                Some(held) => held != &node.incarnation,
            };
            if stale {
                let _ = d.cancel.send(true);
            }
        }
    }

    /// Hard-cut every connection this pool holds: cancel the dials in flight, remove the pooled
    /// connections and close them with `reason`. A call riding one ends where the connection ends;
    /// the next call dials afresh. Returns how many pooled connections were closed.
    pub fn close_all(&self, reason: &'static [u8]) -> usize {
        for (_, d) in self.inner.dials.lock().unwrap().iter() {
            let _ = d.cancel.send(true);
        }
        let closed: Vec<(PoolKey, Pooled)> = self.inner.conns.lock().unwrap().drain().collect();
        for (key, p) in &closed {
            p.conn.close(0u32.into(), reason);
            tracing::info_span!("rdm.node_rpc.connection.delete.via-hard-cut", peer = %key.peer.fmt_short(), incarnation_id = %key.birth())
                .in_scope(|| tracing::info!("a pooled connection was closed"));
        }
        closed.len()
    }

    /// Close the connections this pool holds to `peer`, whatever their scope or birth, and cancel the
    /// dials in flight to it: the next call to it dials afresh. Returns how many were closed.
    pub fn close_peer(&self, peer: &iroh::PublicKey, reason: &'static [u8]) -> usize {
        let closed = self.close_to(peer, reason);
        for c in &closed {
            tracing::info_span!("rdm.node_rpc.connection.delete.via-hard-cut", peer = %c.key.peer.fmt_short(), incarnation_id = %c.key.birth())
                .in_scope(|| tracing::info!("a pooled connection was closed"));
        }
        closed.len()
    }

    /// Close the connections this pool holds to `peer`, whatever their scope or birth, and cancel the
    /// dials in flight to it, handing back each connection closed; the caller names why on that
    /// connection's own span. The next call to it dials afresh.
    pub fn close_to(&self, peer: &iroh::PublicKey, reason: &'static [u8]) -> Vec<ClosedConnection> {
        for (k, d) in self.inner.dials.lock().unwrap().iter() {
            if &k.peer == peer {
                let _ = d.cancel.send(true);
            }
        }
        let closed: Vec<(PoolKey, Pooled)> = {
            let mut conns = self.inner.conns.lock().unwrap();
            let keys: Vec<PoolKey> = conns.keys().filter(|k| &k.peer == peer).cloned().collect();
            keys.into_iter().filter_map(|k| conns.remove(&k).map(|p| (k, p))).collect()
        };
        closed
            .into_iter()
            .map(|(key, p)| {
                p.conn.close(0u32.into(), reason);
                ClosedConnection { connection: p.conn.stable_id(), key }
            })
            .collect()
    }

    /// A live pooled connection for `key`.
    pub fn pooled(&self, key: &PoolKey) -> Option<Connection> {
        let mut conns = self.inner.conns.lock().unwrap();
        match conns.get(key) {
            Some(p) if p.conn.close_reason().is_none() => Some(p.conn.clone()),
            Some(_) => {
                conns.remove(key);
                None
            }
            None => None,
        }
    }

    /// The pooled connection for `key`, or the outcome of the one dial for
    /// it (started here or already in flight). `true` when reused.
    pub(crate) async fn get_or_dial(&self, key: &PoolKey, spec: DialSpec, deadline: Instant) -> Result<(Connection, bool), DialError> {
        let existing = {
            let mut conns = self.inner.conns.lock().unwrap();
            match conns.get_mut(key) {
                Some(p) if p.conn.close_reason().is_none() => {
                    let report = !p.reported && spec.opened.is_some();
                    if report {
                        p.reported = true;
                    }
                    Some((p.conn.clone(), report))
                }
                Some(_) => {
                    conns.remove(key);
                    None
                }
                None => None,
            }
        };
        if let Some((c, report)) = existing {
            if report {
                if let Some(opened) = &spec.opened {
                    opened();
                }
            }
            return Ok((c, true));
        }
        // No new connection while the source admits none: neither a dial of its own nor one
        // already running for the key that this call would wait on.
        if let Some(why) = spec.admission.blocked() {
            return Err(DialError::Blocked { at: "dial", why });
        }
        let mut rx = {
            let mut dials = self.inner.dials.lock().unwrap();
            match dials.get(key) {
                Some(d) => d.outcome.clone(),
                None => {
                    let (cancel, cancelled) = watch::channel(false);
                    let (tx, rx) = watch::channel(None);
                    dials.insert(key.clone(), Dial { cancel, outcome: rx.clone(), addr: spec.addr });
                    let (pool, key) = (self.clone(), key.clone());
                    // The one dial for `key`, under the call that started it: its steps (the superseded
                    // paths retired, the connect) and how it ended, however many callers wait on it. It is
                    // never the current span while the dial runs: the transport's connect spawns the
                    // connection's driver task with the span current at that moment, and that task lives as
                    // long as the pooled connection, so a current dial span would stay open (and keep the
                    // call span above it open) until the connection closes. Its events name it as parent.
                    // The task runs on the dispatcher of the dial span: its events reach that span's
                    // subscriber, and the span ends there. A span's end releases its parent's reference
                    // through the dispatcher of the thread it ends on, so a dial span ending on a worker that
                    // holds another dispatcher would leave the call span (and every span above it) open
                    // for good. A dial span no subscriber took has no parent to release and no subscriber
                    // for its events: its task carries no dispatcher, so it never pins "none" over one
                    // installed while it runs (a callsite first reached under a pinned "none" is cached as
                    // never enabled, process-wide).
                    let span = tracing::info_span!(
                        "rdm.node_rpc.connection.create.via-dial",
                        peer = %key.peer.fmt_short(),
                        incarnation_id = %key.birth(),
                        addr = %spec.addr,
                        local = ?spec.endpoint.bound_sockets(),
                        outcome = tracing::field::Empty,
                        elapsed_ms = tracing::field::Empty,
                    );
                    let dispatch = tracing::Span::with_subscriber(&span, |(_, d): (&tracing::Id, &tracing::Dispatch)| d.clone());
                    let dial = async move {
                        let started = Instant::now();
                        let out = pool.run_dial(&key, spec, cancelled, &span).await;
                        span.record("outcome", match &out { Ok(_) => "connected".to_string(), Err(e) => format!("{e:?}") }.as_str());
                        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
                        // The dial span ends with the dial, before any waiter hears the outcome.
                        drop(span);
                        pool.inner.dials.lock().unwrap().remove(&key);
                        let _ = tx.send(Some(out));
                    };
                    match dispatch {
                        Some(d) => drop(tokio::spawn(dial.with_subscriber(d))),
                        None => drop(tokio::spawn(dial)),
                    }
                    rx
                }
            }
        };
        let waited = match tokio::time::timeout_at(deadline, rx.wait_for(Option::is_some)).await {
            Ok(Ok(o)) => o.clone(),
            Ok(Err(_)) => return Err(DialError::Failed("the dial ended without an outcome".into())),
            Err(_) => return Err(DialError::Deadline),
        };
        waited.expect("waited for an outcome").map(|c| (c, false))
    }

    async fn run_dial(&self, key: &PoolKey, spec: DialSpec, mut cancelled: watch::Receiver<bool>, span: &tracing::Span) -> Result<Connection, DialError> {
        let started = Instant::now();
        let resolver = spec.resolver.clone();
        let target = spec.target.clone();
        // Released the moment the resolver stops naming this exact target: subscribed first, then
        // checked, so a move between the dial's registration and this subscription is seen too.
        let moved = async {
            let (Some(target), Some(mut changes)) = (target.as_ref(), resolver.changes()) else { return std::future::pending::<()>().await };
            loop {
                if !key.is_current(resolver.resolve(target).ok().as_ref()) {
                    return;
                }
                if changes.changed().await.is_err() {
                    return std::future::pending::<()>().await;
                }
            }
        };
        // The resolver names this one socket for the peer: every other direct address the endpoint
        // holds for the key is a superseded birth's, and its paths (open ones too) are retired
        // before the dial so none of them takes part in it.
        let connect = async {
            spec.endpoint.replace_direct_addrs(key.peer, [spec.addr]).await;
            tracing::info!(parent: span, step = "direct-addrs-replaced", elapsed_ms = started.elapsed().as_millis() as u64, "every other direct address for the peer is retired");
            let r = spec.endpoint.connect(EndpointAddr::new(key.peer).with_ip_addr(spec.addr), crate::ALPN).await;
            tracing::info!(parent: span, step = "connect-returned", ok = r.is_ok(), elapsed_ms = started.elapsed().as_millis() as u64, "the endpoint's connect returned");
            diag_connect(key, &spec, &r, started, span);
            r
        };
        let res = tokio::select! {
            biased;
            _ = cancelled.wait_for(|c| *c) => None,
            () = moved => None,
            r = connect => Some(r),
        };
        let Some(res) = res else {
            let now = target.as_ref().and_then(|t| resolver.resolve(t).ok());
            span.in_scope(|| evict_span(key.superseded_by(now.as_ref()), key, "cancelled", started.elapsed().as_millis()));
            return Err(DialError::Superseded);
        };
        let conn = match res {
            Ok(c) => c,
            Err(e) => return Err(DialError::Failed(e.to_string())),
        };
        if let Some(fp) = &spec.failpoint {
            fp.reached.notify_one();
            fp.release.notified().await;
        }
        // A dial that began before the source blocked admission opens no connection after it:
        // the connection is closed, never pooled, and no Direct fact is written for it.
        if let Some(why) = spec.admission.blocked() {
            conn.close(0u32.into(), b"admission blocked");
            return Err(DialError::Blocked { at: "pool", why });
        }
        // A connection to a target the resolver no longer names is evidence
        // about nothing current: it is never pooled.
        if let Some(t) = &target {
            let now = resolver.resolve(t).ok();
            if !key.is_current(now.as_ref()) {
                conn.close(0u32.into(), b"superseded");
                span.in_scope(|| evict_span(key.superseded_by(now.as_ref()), key, "late-connect-dropped", started.elapsed().as_millis()));
                return Err(DialError::Superseded);
            }
        }
        let reported = spec.opened.is_some();
        self.inner.conns.lock().unwrap().insert(key.clone(), Pooled { conn: conn.clone(), addr: spec.addr, reported });
        if let Some(opened) = &spec.opened {
            opened();
        }
        Ok(conn)
    }

    /// A structural failure (open, write or read on the connection): evict now. `true` when this
    /// call removed the pooled connection.
    pub fn broken(&self, key: &PoolKey, conn: &Connection) -> bool {
        let mut conns = self.inner.conns.lock().unwrap();
        if conns.get(key).is_some_and(|p| p.conn.stable_id() == conn.stable_id()) {
            conns.remove(key);
            return true;
        }
        false
    }
}
