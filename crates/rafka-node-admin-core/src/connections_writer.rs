//! The source-owned connections writer (connections.md sections 3, 4.2, 9 and 10; i143.e6.s11):
//! every node writes its own Direct and Proxy facts, and holds its own projection of them.
//!
//! The Node RPC client reports what it observes of this node's pooled connections through
//! [`ConnectionObserver`], and the server reports every accepted connection the same way; this
//! writer turns each observation into one row, applies it to the held projection the route
//! resolver reads, appends it to the raw log and keeps it as the latest of its pair in the index.
//! At birth the held projection is hydrated from the index, so a restart keeps its latest Proxy
//! and the backoff of its latest Direct failure. The writer decides no route and chooses no
//! carrier: it records facts.
//!
//! One obligation is reconciled here (connections.md §10): whenever this node's latest Direct to
//! a destination is Connected beside its own active Proxy to it, the Proxy's retirement
//! (`Disconnected(direct-restored)`) is owed. It is written durably first and applied to the held
//! projection only once the write lands, so new calls keep the proven Proxy until then. A refused
//! write leaves the obligation standing: it is derived again from the two latest rows at the next
//! observation, at the next settlement a caller asks for, and every [`RETIREMENT_RETRY`] while
//! owed. Nothing is queued; a restart reconstructs the same obligation from the same rows.

use crate::storage::ConnectionsStorage;
use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
use rafka_mesh_entity::reconnect::{first_failure, owed_retirements};
use rafka_node_rpc::{ConnectionObserver, ResolvedNode};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How often an owed Proxy retirement whose write was refused is attempted again.
pub const RETIREMENT_RETRY: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct ConnectionsWriter {
    inner: Arc<Inner>,
}

/// Makes an observation's row from the held projection, inside the write gate.
type Build = Box<dyn FnOnce(&ConnectionsWriter) -> NodeConnection + Send>;

struct Inner {
    own: ConnectionEnd,
    storage: Arc<dyn ConnectionsStorage>,
    held: Arc<Mutex<ConnectionsHeld>>,
    runtime: tokio::runtime::Handle,
    /// The last stamp this writer gave a row: every row it writes carries a later one, so two
    /// facts in one millisecond never tie (the held projection keeps one stamp per key).
    last_stamp: Mutex<u64>,
    /// Observation writes spawned and not yet landed or refused; [`ConnectionsWriter::drain`]
    /// waits for this to reach zero, `idle` wakes it when a write finishes.
    in_flight: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
    /// The observations in the order they were observed: one consumer builds, stamps, writes and
    /// applies them one at a time, so the index member of a pair is always the last observed.
    queue: tokio::sync::mpsc::UnboundedSender<(Build, &'static str)>,
    /// The one write gate: held across every build, durable write and apply, whether it is an
    /// observation, a [`ConnectionsWriter::record`] or a retirement settlement.
    write_gate: tokio::sync::Mutex<()>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn end_of(node: &ResolvedNode) -> ConnectionEnd {
    ConnectionEnd { name: node.name.clone(), node_id: node.node_id.clone(), incarnation: Some(node.incarnation.clone()) }
}

fn kind_name(k: ConnectionKind) -> &'static str {
    match k {
        ConnectionKind::Direct => "direct",
        ConnectionKind::Proxy => "proxy",
    }
}

fn state_name(s: ConnectionState) -> &'static str {
    match s {
        ConnectionState::Connected => "connected",
        ConnectionState::Disconnected => "disconnected",
        ConnectionState::Failed => "failed",
    }
}

impl ConnectionsWriter {
    /// A writer for `own`, over its storage, holding `held` (shared with the route resolver).
    pub fn new(own: ConnectionEnd, storage: Arc<dyn ConnectionsStorage>, held: Arc<Mutex<ConnectionsHeld>>) -> Self {
        let runtime = tokio::runtime::Handle::current();
        let (queue, mut observations) = tokio::sync::mpsc::unbounded_channel::<(Build, &'static str)>();
        let inner = Arc::new(Inner {
            own,
            storage,
            held,
            runtime: runtime.clone(),
            last_stamp: Mutex::new(0),
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            idle: tokio::sync::Notify::new(),
            queue,
            write_gate: tokio::sync::Mutex::new(()),
        });
        let weak = Arc::downgrade(&inner);
        runtime.spawn(async move {
            while let Some((build, origin)) = observations.recv().await {
                let Some(inner) = weak.upgrade() else { break };
                ConnectionsWriter { inner }.write_observed(build, origin).await;
            }
        });
        Self { inner }
    }

    pub fn held(&self) -> Arc<Mutex<ConnectionsHeld>> {
        self.inner.held.clone()
    }

    /// This node, as the source every row it writes names.
    pub fn own(&self) -> &ConnectionEnd {
        &self.inner.own
    }

    /// Observation writes spawned by [`ConnectionObserver`] calls that have not yet landed or
    /// been refused. The raw log, the index, the held projection and the observation's span follow
    /// when its write completes.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Resolves once every observation write spawned before the call has completed: the moment
    /// after which the index and the evidence hold every fact this writer was handed.
    pub async fn drain(&self) {
        loop {
            let mut notified = std::pin::pin!(self.inner.idle.notified());
            notified.as_mut().enable();
            if self.in_flight() == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Hydrate the held projection from the index: every latest row this node wrote (its own
    /// Direct and Proxy facts) is applied, so the first call after a restart resolves over the
    /// latest Proxy and the reconnect series continues from the latest failure. A retirement the
    /// two latest rows of a pair owe is settled right after, from those rows (connections.md §10).
    pub async fn hydrate(&self) -> Result<usize, String> {
        let rows = self.inner.storage.connections().await.map_err(|e| e.to_string())?;
        let applied = {
            let mut held = self.inner.held.lock().unwrap();
            held.set_own_source(self.inner.own.name.clone());
            let mut applied = 0;
            for row in rows {
                if held.apply(row).is_ok() {
                    applied += 1;
                }
            }
            held.mark_complete();
            applied
        };
        tracing::info_span!("rdm.node_admin.connection.update.via-hydrate", node = %self.inner.own.name, applied)
            .in_scope(|| tracing::info!("the held projection is hydrated from this node's own index"));
        let _ = self.settle_owed().await;
        Ok(applied)
    }

    /// Record one fact: the raw log first, then the index, then the held projection.
    pub async fn record(&self, row: NodeConnection) -> Result<(), String> {
        let _gate = self.inner.write_gate.lock().await;
        self.persist(&row).await?;
        let _ = self.inner.held.lock().unwrap().apply(row);
        Ok(())
    }

    /// The raw log first, then the index; an error names the storage's own refusal.
    async fn persist(&self, row: &NodeConnection) -> Result<(), String> {
        self.inner.storage.append_history(row).await.map_err(|e| e.to_string())?;
        self.inner.storage.put_connection(row).await.map_err(|e| e.to_string())
    }

    /// The latest Direct row this node holds toward `destination`, if any.
    fn latest_direct(&self, destination: &ConnectionEnd) -> Option<NodeConnection> {
        self.inner.held.lock().unwrap().own_latest_directs().into_iter().find(|d| d.destination.name == destination.name).cloned()
    }

    fn direct(&self, destination: ConnectionEnd, state: ConnectionState, reason: Option<String>) -> NodeConnection {
        NodeConnection {
            source: self.inner.own.clone(),
            destination,
            kind: ConnectionKind::Direct,
            state,
            carrier: None,
            recovery: None,
            reason,
            logged_at_ms: now_ms(),
        }
    }

    /// The Proxy retirements this node owes now (connections.md §10), derived from the held
    /// projection's two latest rows per pair: never a queue.
    pub fn owed(&self) -> Vec<NodeConnection> {
        owed_retirements(&self.inner.held.lock().unwrap(), now_ms())
    }

    /// Settle every Proxy retirement this node owes: each is written durably as
    /// `Disconnected(direct-restored)` and applied to the held projection once its write lands,
    /// so new calls cut back to Direct only then. A refused write is named and leaves the
    /// obligation standing for the next attempt. Returns how many landed.
    pub async fn settle_owed(&self) -> Result<usize, String> {
        let _gate = self.inner.write_gate.lock().await;
        let at = self.stamp();
        let owed = owed_retirements(&self.inner.held.lock().unwrap(), at);
        let mut landed = 0;
        for row in owed {
            let span = tracing::info_span!(
                "rdm.node_admin.connection.update.via-retirement",
                source = %row.source.name,
                destination = %row.destination.name,
                carrier = %row.carrier.as_ref().map(|c| c.name.to_string()).unwrap_or_default(),
                outcome = tracing::field::Empty,
                reason = tracing::field::Empty,
            );
            match self.persist(&row).await {
                Ok(()) => {
                    let _ = self.inner.held.lock().unwrap().apply(row);
                    span.record("outcome", "landed");
                    span.in_scope(|| tracing::info!("the owed Proxy retirement landed; new calls cut back to Direct"));
                    landed += 1;
                }
                Err(e) => {
                    span.record("outcome", "refused");
                    span.record("reason", e.as_str());
                    span.in_scope(|| tracing::info!("the owed Proxy retirement write was refused; the Proxy stays effective and the obligation stands"));
                    return Err(e);
                }
            }
        }
        Ok(landed)
    }

    /// [`Self::settle_owed`], as the integration cells name it.
    pub async fn retire_owed(&self) -> Result<usize, String> {
        self.settle_owed().await
    }

    /// Attempt every owed retirement again each `every` while one is owed: the bounded retry
    /// connections.md §10 asks for, derived from the rows each time. The task ends with the
    /// runtime.
    pub fn spawn_retirement_reconciler(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        let w = self.clone();
        self.inner.runtime.spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if !w.owed().is_empty() {
                    let _ = w.settle_owed().await;
                }
            }
        })
    }

    /// A strictly increasing stamp for this writer's rows.
    fn stamp(&self) -> u64 {
        let mut last = self.inner.last_stamp.lock().unwrap();
        *last = now_ms().max(*last + 1);
        *last
    }

    /// An observed fact: `build` makes the row from the held projection, the row is written to the
    /// raw log and the index, and it is applied to the held projection only once that write has
    /// landed. A refused write leaves the fact unheld and names the refusal; the next observation
    /// of the connection writes it afresh. Observations take the write gate one at a time, so a
    /// row built from the held projection (a failing series' next ordinal) sees every earlier
    /// observation already applied. A Direct Connected settles the Proxy retirement it may owe
    /// once its own write has landed.
    fn observed(&self, build: impl FnOnce(&Self) -> NodeConnection + Send + 'static, origin: &'static str) {
        self.inner.in_flight.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let _ = self.inner.queue.send((Box::new(build), origin));
    }

    /// Write one queued observation inside the write gate, then settle the retirement it may owe.
    async fn write_observed(&self, build: Build, origin: &'static str) {
        let w = self;
        {
            let gate = w.inner.write_gate.lock().await;
            let mut row = build(w);
            row.logged_at_ms = w.stamp();
            let settles = row.kind == ConnectionKind::Direct && row.state == ConnectionState::Connected;
            let span = tracing::info_span!(
                "rdm.node_admin.connection.update.via-observed",
                source = %row.source.name,
                destination = %row.destination.name,
                kind = kind_name(row.kind),
                state = state_name(row.state),
                reason = %row.reason.clone().unwrap_or_default(),
                origin,
                outcome = tracing::field::Empty,
            );
            let landed = match w.persist(&row).await {
                Ok(()) => {
                    let _ = w.inner.held.lock().unwrap().apply(row.clone());
                    span.record("outcome", "landed");
                    true
                }
                Err(e) => {
                    span.record("outcome", format!("refused: {e}").as_str());
                    tracing::info_span!(
                        "rdm.node_admin.connection.reject.via-refused-write",
                        node = %w.inner.own.name,
                        source = %row.source.name,
                        destination = %row.destination.name,
                        destination_node_id = %row.destination.node_id,
                        kind = kind_name(row.kind),
                        state = state_name(row.state),
                        reason = %row.reason.clone().unwrap_or_default(),
                        origin,
                        logged_at_ms = row.logged_at_ms,
                        refusal = %e,
                    )
                    .in_scope(|| tracing::info!("the observed connection fact was refused by storage; it is not held, and the next observation of this connection writes it afresh"));
                    false
                }
            };
            span.in_scope(|| tracing::info!("one connection fact this node observed"));
            drop(gate);
            if settles && landed {
                let _ = w.settle_owed().await;
            }
            drop(span);
            w.inner.in_flight.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            w.inner.idle.notify_waiters();
        }
    }
}

impl ConnectionObserver for ConnectionsWriter {
    fn direct_connected(&self, node: &ResolvedNode) {
        let destination = end_of(node);
        self.observed(move |w| w.direct(destination, ConnectionState::Connected, None), "dial");
    }

    fn direct_accepted(&self, node: &ResolvedNode) {
        let destination = end_of(node);
        self.observed(move |w| w.direct(destination, ConnectionState::Connected, None), "accept");
    }

    fn direct_failed(&self, node: &ResolvedNode, reason: &str) {
        let destination = end_of(node);
        let reason = reason.to_string();
        self.observed(
            move |w| {
                let previous = w.latest_direct(&destination);
                // A failing series continues in its epoch, one ordinal up; anything else (never
                // tried, connected, dropped) is a new incident: a new epoch at ordinal one.
                match previous.as_ref() {
                    Some(p) if p.state == ConnectionState::Failed => match p.recovery {
                        Some(r) => NodeConnection {
                            recovery: Some(rafka_mesh_entity::connections::DirectRecovery { recovery_epoch: r.recovery_epoch, attempt_ordinal: r.attempt_ordinal.saturating_add(1) }),
                            reason: Some(reason),
                            logged_at_ms: now_ms(),
                            ..p.clone()
                        },
                        None => first_failure(w.inner.own.clone(), destination, previous.as_ref(), &reason, now_ms()),
                    },
                    _ => first_failure(w.inner.own.clone(), destination, previous.as_ref(), &reason, now_ms()),
                }
            },
            "dial",
        );
    }

    fn direct_broken(&self, node: &ResolvedNode, reason: &str) {
        let destination = end_of(node);
        let reason = reason.to_string();
        self.observed(move |w| w.direct(destination, ConnectionState::Disconnected, Some(reason)), "dial");
    }
}

#[async_trait::async_trait]
impl rafka_node_rpc::CarrierEdges for ConnectionsWriter {
    async fn edge_not_active(&self, target: &rafka_mesh_entity::NodeId) -> Option<String> {
        // The dial that just failed handed its Failed fact to this writer; it is held once its write
        // has landed. The carrier answers from the facts, so it waits for them.
        self.drain().await;
        let held = self.inner.held.lock().unwrap();
        let latest = held.own_latest_directs().into_iter().find(|d| &d.destination.node_id == target)?;
        (latest.state != ConnectionState::Connected).then(|| {
            format!(
                "{} -> {} Direct {}{}",
                latest.source.name,
                latest.destination.name,
                state_name(latest.state),
                latest.reason.as_deref().map(|r| format!(" ({r})")).unwrap_or_default()
            )
        })
    }
}
