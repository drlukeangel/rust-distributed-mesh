//! The scoped connection pool (i143 PRD §1.19–20, §14;
//! node-rpc-rdm-ownership.md §8–§9).
//!
//! A pooled connection is keyed by `(scope, peer, incarnation)`: the caller's
//! execution scope, the peer's authenticated Iroh key and the process birth it
//! was dialled to. A connection is to a process: the fence in each request's
//! framing names the node, and every op of a birth shares the connection.
//!
//! Supersession is per birth:
//! - a dial in flight whose incarnation the resolver no longer names is
//!   cancelled the moment the resolver changes, and its waiters get
//!   `RejectedStale` at once;
//! - a dial that completes after its birth was superseded is never pooled;
//! - pooled connections of a superseded birth are evicted.
//!
//! One dial per key is in flight at a time; concurrent callers share it.
//!
//! Eviction removes the entry only; it never closes the connection, so a call
//! already riding it finishes. Eviction is connection health, not
//! reachability: nothing here marks a node unreachable.
//!
//! Timeout strikes (two consecutive reply-deadline expiries on one entry)
//! evict a poisoned connection. They are transitional restart detection
//! (ownership §9): the incarnation key above detects a restart with no failed
//! call, and the strikes are deleted once that cut is proven end to end.

use crate::resolve::{NodeResolver, NodeTarget, ResolvedNode};
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr};
use rafka_mesh_entity::IncarnationId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Notify};
use tokio::time::Instant;

/// Consecutive reply-deadline expiries on one pooled entry that evict it.
pub const TIMEOUT_STRIKES_BEFORE_EVICT: u32 = 2;

/// One pooled connection's identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    pub scope: Option<String>,
    pub peer: iroh::PublicKey,
    pub incarnation: IncarnationId,
}

impl PoolKey {
    /// Does `node` (the resolver's answer now) still name this exact birth?
    pub fn is_current(&self, node: Option<&ResolvedNode>) -> bool {
        node.is_some_and(|n| n.endpoint_id == self.peer && n.incarnation == self.incarnation)
    }

    fn superseded_by(&self, _node: Option<&ResolvedNode>) -> &'static str {
        "via-incarnation-superseded"
    }
}

/// Test failpoint: a dial that connected stops here, before it is checked
/// against the resolver and pooled, until released.
#[derive(Default)]
pub struct Failpoint {
    pub reached: Notify,
    pub release: Notify,
}

impl std::fmt::Debug for Failpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Failpoint")
    }
}

/// How a dial ended, for every caller waiting on it.
#[derive(Debug, Clone)]
pub enum DialError {
    /// The birth moved while the dial ran, or before it pooled.
    Superseded,
    /// This caller's own deadline passed first (the dial may run on for others).
    Deadline,
    Failed(String),
}

type Outcome = Option<Result<Connection, DialError>>;

struct Dial {
    cancel: watch::Sender<bool>,
    outcome: watch::Receiver<Outcome>,
}

#[derive(Default)]
struct Inner {
    conns: Mutex<HashMap<PoolKey, Connection>>,
    dials: Mutex<HashMap<PoolKey, Dial>>,
    strikes: Mutex<HashMap<PoolKey, u32>>,
}

/// The pool a [`crate::NodeRpcClient`] owns.
#[derive(Default, Clone)]
pub struct Pool {
    inner: Arc<Inner>,
}

/// What a dial needs to run on its own.
pub struct DialSpec {
    pub endpoint: Endpoint,
    pub resolver: Arc<dyn NodeResolver>,
    pub target: NodeTarget,
    pub addr: SocketAddr,
    pub deadline: Instant,
    pub failpoint: Option<Arc<Failpoint>>,
}

fn evict_span(_reason: &'static str, key: &PoolKey, outcome: &str, elapsed_ms: u128) {
    tracing::info_span!(
        "rafka.node_rpc.connection.evict.via-incarnation-superseded",
        peer = %key.peer.fmt_short(), incarnation_id = %key.incarnation.0, outcome, elapsed_ms = elapsed_ms as u64
    )
    .in_scope(|| tracing::info!("superseded birth: {outcome}"));
}

impl Pool {
    /// Every pooled key (diagnostics and tests).
    pub fn keys(&self) -> Vec<PoolKey> {
        self.inner.conns.lock().unwrap().keys().cloned().collect()
    }

    /// Evict every pooled connection and cancel every dial of `node`'s peer
    /// whose target `node` no longer names.
    pub fn purge_stale(&self, node: &ResolvedNode) {
        let stale = |k: &PoolKey| k.peer == node.endpoint_id && !k.is_current(Some(node));
        let gone: Vec<PoolKey> = {
            let mut conns = self.inner.conns.lock().unwrap();
            let gone: Vec<PoolKey> = conns.keys().filter(|k| stale(k)).cloned().collect();
            for k in &gone {
                conns.remove(k);
            }
            gone
        };
        for k in gone {
            self.inner.strikes.lock().unwrap().remove(&k);
            evict_span(k.superseded_by(Some(node)), &k, "evicted", 0);
        }
        for (_, d) in self.inner.dials.lock().unwrap().iter().filter(|(k, _)| stale(k)) {
            let _ = d.cancel.send(true);
        }
    }

    /// A live pooled connection for `key`.
    pub fn pooled(&self, key: &PoolKey) -> Option<Connection> {
        let mut conns = self.inner.conns.lock().unwrap();
        match conns.get(key) {
            Some(c) if c.close_reason().is_none() => Some(c.clone()),
            Some(_) => {
                conns.remove(key);
                None
            }
            None => None,
        }
    }

    /// The pooled connection for `key`, or the outcome of the one dial for
    /// it (started here or already in flight). `true` when reused.
    pub async fn get_or_dial(&self, key: &PoolKey, spec: DialSpec) -> Result<(Connection, bool), DialError> {
        if let Some(c) = self.pooled(key) {
            return Ok((c, true));
        }
        let deadline = spec.deadline;
        let mut rx = {
            let mut dials = self.inner.dials.lock().unwrap();
            match dials.get(key) {
                Some(d) => d.outcome.clone(),
                None => {
                    let (cancel, cancelled) = watch::channel(false);
                    let (tx, rx) = watch::channel(None);
                    dials.insert(key.clone(), Dial { cancel, outcome: rx.clone() });
                    let (pool, key) = (self.clone(), key.clone());
                    tokio::spawn(async move {
                        let out = pool.run_dial(&key, spec, cancelled).await;
                        pool.inner.dials.lock().unwrap().remove(&key);
                        let _ = tx.send(Some(out));
                    });
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

    async fn run_dial(&self, key: &PoolKey, spec: DialSpec, mut cancelled: watch::Receiver<bool>) -> Result<Connection, DialError> {
        let started = Instant::now();
        let resolver = spec.resolver.clone();
        let target = spec.target.clone();
        // Released the moment the resolver stops naming this exact target.
        let moved = async {
            let Some(mut changes) = resolver.changes() else { return std::future::pending::<()>().await };
            loop {
                if changes.changed().await.is_err() {
                    return std::future::pending::<()>().await;
                }
                if !key.is_current(resolver.resolve(&target).ok().as_ref()) {
                    return;
                }
            }
        };
        let connect = spec.endpoint.connect(EndpointAddr::new(key.peer).with_ip_addr(spec.addr), crate::ALPN);
        let res = tokio::select! {
            biased;
            _ = cancelled.wait_for(|c| *c) => None,
            () = moved => None,
            r = tokio::time::timeout_at(spec.deadline, connect) => Some(r),
        };
        let Some(res) = res else {
            let now = resolver.resolve(&target).ok();
            evict_span(key.superseded_by(now.as_ref()), key, "cancelled", started.elapsed().as_millis());
            return Err(DialError::Superseded);
        };
        let conn = match res {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => return Err(DialError::Failed(e.to_string())),
            Err(_) => return Err(DialError::Deadline),
        };
        if let Some(fp) = &spec.failpoint {
            fp.reached.notify_one();
            fp.release.notified().await;
        }
        // A connection to a target the resolver no longer names is evidence
        // about nothing current: it is never pooled.
        let now = resolver.resolve(&target).ok();
        if !key.is_current(now.as_ref()) {
            conn.close(0u32.into(), b"superseded");
            evict_span(key.superseded_by(now.as_ref()), key, "late-connect-dropped", started.elapsed().as_millis());
            return Err(DialError::Superseded);
        }
        self.inner.conns.lock().unwrap().insert(key.clone(), conn.clone());
        Ok(conn)
    }

    /// The connection proved itself (any reply or refusal): clear its strikes.
    pub fn healthy(&self, key: &PoolKey) {
        self.inner.strikes.lock().unwrap().remove(key);
    }

    /// A structural failure (open, write or read on the connection): evict now.
    pub fn broken(&self, key: &PoolKey, conn: &Connection) {
        self.inner.strikes.lock().unwrap().remove(key);
        self.remove_if_same(key, conn);
    }

    /// A reply deadline expired on `conn`. The second in a row evicts it.
    pub fn timed_out(&self, key: &PoolKey, conn: &Connection) {
        let strikes = {
            let mut s = self.inner.strikes.lock().unwrap();
            let n = s.entry(key.clone()).or_insert(0);
            *n += 1;
            *n
        };
        if strikes >= TIMEOUT_STRIKES_BEFORE_EVICT {
            self.inner.strikes.lock().unwrap().remove(key);
            if self.remove_if_same(key, conn) {
                tracing::info_span!(
                    "rafka.node_rpc.connection.evict.via-timeout-strikes",
                    peer = %key.peer.fmt_short(), incarnation_id = %key.incarnation.0, strikes
                )
                .in_scope(|| tracing::info!("poisoned connection evicted; the node is not marked unreachable"));
            }
        }
    }

    fn remove_if_same(&self, key: &PoolKey, conn: &Connection) -> bool {
        let mut conns = self.inner.conns.lock().unwrap();
        if conns.get(key).is_some_and(|c| c.stable_id() == conn.stable_id()) {
            conns.remove(key);
            return true;
        }
        false
    }
}
