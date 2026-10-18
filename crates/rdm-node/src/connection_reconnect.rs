//! Per-node Direct reconnect scheduler and Proxy retirement reconciler
//! (`docs/architecture/connections.md` §9, §10; i148 e3).
//!
//! Every node runs the same reconnect mechanism internally in `rdm-node`. Node-admin does not
//! monitor Proxy routes and does not send reconnect commands.
//!
//! # Reconnect obligation (§9.1)
//!
//! A reconnect series exists when the latest Direct record for `(source, destination, alpn)` is
//! `Failed` for the current destination incarnation and no newer `Direct Connected` on that ALPN
//! supersedes it.
//!
//! # Durable backoff (§9.3)
//!
//! Every failed reconnect attempt appends a new `Direct Failed` event in the same recovery round at
//! `attempt_ordinal + 1`. The next due time is reconstructed deterministically from the latest
//! indexed `Failed` record (`rdm_core::reconnect::next_due`):
//! `logged_at_ms + reconnect_backoff(attempt_ordinal) + deterministic_jitter(source, destination, attempt_ordinal)`.
//! A process restart reads its latest Direct-per-destination index rows and reconstructs the
//! schedule without replaying the raw log and without resetting exponential backoff or attempting
//! before `next_due`.
//!
//! # One scheduler, one attempt at a time (§9.4, §9.5)
//!
//! Each node runs one scheduler/reconciler with at most one pending item per
//! `(destination, destination_incarnation, alpn)` and executes at most one reconnect dial attempt at
//! a time on the node. Two paths arm that one pending item: a latest `Direct Failed` row in `held`,
//! and an inbound connection accepted from the exact destination birth this node reaches through an
//! active Proxy (§10, §13 G).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdm_core::connections::{
    ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery, NodeConnection,
};
use rdm_core::reconnect::next_due;
use rdm_core::{ConnectionId, IncarnationId, NodeId, PathName};
use rdm_transport::ResolvedNode;
use rdm_transport::connections::{Birth, ConnectError, ConnectionsRegistry};

use crate::connections_writer::ConnectionsWriter;

/// How often the reconciler checks what is owed without an explicit wake: an owed Proxy retirement
/// whose write was refused is retried, and a pending recovery attempt whose backoff has ended is
/// dialled.
pub const RECONCILER_TICK: Duration = Duration::from_millis(500);

/// One direct dial of this node's default data plane toward an exact destination birth
/// (`connections.md` §9.5 step 3), answering the call outcome's name for the attempt's span.
pub type RecoveryDial =
    Arc<dyn Fn(ConnectionEnd) -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync>;

/// One direct dial of this node's data plane toward an exact destination birth on a specific
/// `alpn` (`connections.md` §9.4, §9.5 step 3), answering the outcome's name for the attempt's span.
pub type AlpnRecoveryDial = Arc<
    dyn Fn(ConnectionEnd, Vec<u8>) -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync,
>;

/// One pending direct-recovery attempt (`connections.md` §9.4): toward an exact destination birth,
/// due at `due_ms` on the local clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryAttempt {
    /// The destination birth the attempt dials.
    pub destination: ConnectionEnd,
    /// When the attempt is due.
    pub due_ms: u64,
}

/// One pending reconnect obligation reconstructed from this node's latest `Direct Failed` index row
/// for `(source, destination, alpn)` (`connections.md` §9.1, §9.3, §9.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectObligation {
    /// The destination birth the obligation dials.
    pub destination: ConnectionEnd,
    /// The exact destination process incarnation.
    pub destination_incarnation: IncarnationId,
    /// The protocol ALPN of the failing series.
    pub alpn: Vec<u8>,
    /// The recovery round and attempt ordinal of the latest `Direct Failed` row.
    pub recovery: DirectRecovery,
    /// When the next direct attempt is due on the local clock (ms).
    pub due_ms: u64,
    /// The `ConnectionId` of the latest `Direct Failed` row this obligation was derived from.
    pub row_id: ConnectionId,
}

/// One pending recovery item in the node's schedule.
#[derive(Clone)]
pub(crate) struct Pending {
    pub(crate) destination: ConnectionEnd,
    pub(crate) alpn: Vec<u8>,
    pub(crate) traceparent: Option<String>,
}

/// Per-node reconnect scheduler state owned by [`ConnectionsWriter`].
pub(crate) struct ReconnectState {
    pub(crate) recovery: Mutex<HashMap<(NodeId, IncarnationId, Vec<u8>), Pending>>,
    pub(crate) wake: tokio::sync::Notify,
    pub(crate) attempt_gate: tokio::sync::Mutex<()>,
    pub(crate) in_flight: AtomicUsize,
    pub(crate) max_concurrent: AtomicUsize,
}

impl Default for ReconnectState {
    fn default() -> Self {
        Self {
            recovery: Mutex::new(HashMap::new()),
            wake: tokio::sync::Notify::new(),
            attempt_gate: tokio::sync::Mutex::new(()),
            in_flight: AtomicUsize::new(0),
            max_concurrent: AtomicUsize::new(0),
        }
    }
}

enum Step {
    Wait,
    Spent(&'static str),
    Dial(u64, Option<ConnectionId>),
}

fn local_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn state_name(s: ConnectionState) -> &'static str {
    match s {
        ConnectionState::Connected => "connected",
        ConnectionState::Disconnected => "disconnected",
        ConnectionState::Failed => "failed",
        ConnectionState::Refused => "refused",
    }
}

/// When an attempt toward `destination` on `alpn` is due, and the latest Direct row it is scheduled
/// from: the series' backoff from this node's latest `Direct Failed` row toward that birth
/// (`connections.md` §9.3), else at once.
pub(crate) fn recovery_due(
    held: &ConnectionsHeld,
    destination: &ConnectionEnd,
    alpn: &[u8],
) -> (u64, Option<ConnectionId>) {
    match held
        .own_latest_directs()
        .into_iter()
        .find(|d| d.destination.name == destination.name && d.alpn.as_slice() == alpn)
    {
        Some(row)
            if (&row.destination.node_id, &row.destination.incarnation)
                == (&destination.node_id, &destination.incarnation) =>
        {
            (next_due(row).unwrap_or(0), Some(row.id.clone()))
        }
        Some(row) => (0, Some(row.id.clone())),
        None => (0, None),
    }
}

/// Adapt a legacy single-ALPN [`RecoveryDial`] into an [`AlpnRecoveryDial`].
pub fn adapt_recovery_dial(dial: RecoveryDial) -> AlpnRecoveryDial {
    Arc::new(move |destination: ConnectionEnd, _alpn: Vec<u8>| dial(destination))
}

/// Build an [`AlpnRecoveryDial`] that dials registered data-plane ALPNs through `registry`
/// ([`ConnectionsRegistry::probe_direct`]) and falls back to `fallback` for the control/default ALPN.
pub fn registry_recovery_dial(
    registry: &ConnectionsRegistry,
    fallback: Option<RecoveryDial>,
) -> AlpnRecoveryDial {
    let reg = registry.clone();
    Arc::new(move |destination: ConnectionEnd, alpn: Vec<u8>| {
        let reg = reg.clone();
        let fallback = fallback.clone();
        Box::pin(async move {
            if reg.has_alpn(&alpn) {
                let Some(inc) = destination.incarnation.clone() else {
                    return "NotSent".to_string();
                };
                let birth = Birth {
                    name: destination.name,
                    node_id: destination.node_id,
                    incarnation: inc,
                };
                match reg.probe_direct(&birth, &alpn).await {
                    Ok(_) => "Reply".to_string(),
                    Err(ConnectError::Refused { .. }) => "Refused".to_string(),
                    Err(_) => "NotSent".to_string(),
                }
            } else if let Some(fb) = fallback {
                fb(destination).await
            } else {
                "NotSent".to_string()
            }
        })
    })
}

/// Read the pending reconnect obligations currently held by `writer`, sorted deterministically by
/// `(destination.name, alpn)`.
pub fn obligations(writer: &ConnectionsWriter) -> Vec<ReconnectObligation> {
    let pending: Vec<(ConnectionEnd, Vec<u8>)> = writer
        .reconnect_state()
        .recovery
        .lock()
        .unwrap()
        .values()
        .map(|p| (p.destination.clone(), p.alpn.clone()))
        .collect();
    let held_arc = writer.held();
    let held = held_arc.lock().unwrap();
    let mut out = Vec::new();
    for (destination, alpn) in pending {
        let Some(inc) = destination.incarnation.clone() else {
            continue;
        };
        let latest = held
            .own_latest_directs()
            .into_iter()
            .find(|d| d.destination.name == destination.name && d.alpn == alpn);
        let (due_ms, recovery, row_id) = match latest {
            Some(row)
                if (&row.destination.node_id, &row.destination.incarnation)
                    == (&destination.node_id, &destination.incarnation) =>
            {
                (
                    next_due(row).unwrap_or(0),
                    row.recovery.unwrap_or(DirectRecovery {
                        recovery_round: 1,
                        attempt_ordinal: 1,
                    }),
                    row.id.clone(),
                )
            }
            Some(row) => (
                0,
                DirectRecovery {
                    recovery_round: 1,
                    attempt_ordinal: 1,
                },
                row.id.clone(),
            ),
            None => (
                0,
                DirectRecovery {
                    recovery_round: 1,
                    attempt_ordinal: 1,
                },
                ConnectionId::mint(),
            ),
        };
        out.push(ReconnectObligation {
            destination,
            destination_incarnation: inc,
            alpn,
            recovery,
            due_ms,
            row_id,
        });
    }
    out.sort_by_key(|o| (o.destination.name.to_string(), o.alpn.clone()));
    out
}

/// The pending recovery attempts as [`RecoveryAttempt`]s.
pub(crate) fn recovery_pending(writer: &ConnectionsWriter) -> Vec<RecoveryAttempt> {
    let pending: Vec<(ConnectionEnd, Vec<u8>)> = writer
        .reconnect_state()
        .recovery
        .lock()
        .unwrap()
        .values()
        .map(|p| (p.destination.clone(), p.alpn.clone()))
        .collect();
    let held_arc = writer.held();
    let held = held_arc.lock().unwrap();
    pending
        .into_iter()
        .map(|(destination, alpn)| RecoveryAttempt {
            due_ms: recovery_due(&held, &destination, &alpn).0,
            destination,
        })
        .collect()
}

/// Arm the one pending recovery attempt per `(destination, destination_incarnation, alpn)` from each
/// own latest `Direct Failed` row in `held` (`connections.md` §9.4, §9.5, Q7). A series for a
/// superseded birth at the same path and ALPN is replaced by the current birth's series; a series
/// whose latest Direct row is `Connected` is satisfied.
pub(crate) fn sync_recovery(writer: &ConnectionsWriter) {
    let (directs, own_source): (Vec<NodeConnection>, Option<PathName>) = {
        let held_arc = writer.held();
        let held = held_arc.lock().unwrap();
        (
            held.own_latest_directs()
                .into_iter()
                .filter(|r| held.end_superseded(&r.destination) != Some(true))
                .cloned()
                .collect(),
            held.own_source().cloned(),
        )
    };
    let mut recovery = writer.reconnect_state().recovery.lock().unwrap();
    for row in directs {
        if row.state == ConnectionState::Connected {
            if let Some(inc) = row.destination.incarnation.as_ref() {
                recovery.remove(&(row.destination.node_id.clone(), inc.clone(), row.alpn.clone()));
            }
        } else if row.state == ConnectionState::Failed && row.recovery.is_some() {
            let superseded_by_active = own_source.as_ref().is_some_and(|src| {
                let held_arc = writer.held();
                let held = held_arc.lock().unwrap();
                matches!(
                    held.active_direct_for(src, &row.destination.name, &row.alpn),
                    Some(Some(edge)) if edge.destination.incarnation == row.destination.incarnation
                )
            });
            if !superseded_by_active {
                if let Some(inc) = row.destination.incarnation.clone() {
                    recovery.retain(|(id, i, a), p| {
                        p.destination.name != row.destination.name
                            || a != &row.alpn
                            || (id == &row.destination.node_id && i == &inc)
                    });
                    recovery
                        .entry((row.destination.node_id.clone(), inc, row.alpn.clone()))
                        .or_insert_with(|| Pending {
                            destination: row.destination.clone(),
                            alpn: row.alpn.clone(),
                            traceparent: None,
                        });
                }
            }
        }
    }
}

fn spend(
    writer: &ConnectionsWriter,
    key: &(NodeId, IncarnationId, Vec<u8>),
    source: &PathName,
    destination: &ConnectionEnd,
    reason: &'static str,
) {
    writer.reconnect_state().recovery.lock().unwrap().remove(key);
    tracing::info_span!(
        "rdm.node_admin.connection.remove.via-recovery-attempt",
        source = %source,
        destination = %destination.name,
        destination_node_id = %destination.node_id,
        destination_incarnation = %destination.incarnation.as_ref().map(ToString::to_string).unwrap_or_default(),
        reason,
    )
    .in_scope(|| tracing::info!("the pending recovery attempt is spent"));
}

/// The node's one reconnect scheduler and reconciler (`connections.md` §9.4, §9.5).
#[derive(Clone)]
pub struct Reconciler {
    writer: Arc<ConnectionsWriter>,
}

impl std::fmt::Debug for Reconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reconciler").finish()
    }
}

impl Reconciler {
    /// Create a reconciler over `writer`.
    pub fn new(writer: Arc<ConnectionsWriter>) -> Self {
        Self { writer }
    }

    /// Reconnect attempts currently executing on this node.
    pub fn in_flight_attempts(&self) -> usize {
        self.writer
            .reconnect_state()
            .in_flight
            .load(Ordering::SeqCst)
    }

    /// Peak concurrent reconnect attempts observed on this node.
    pub fn max_concurrent_attempts(&self) -> usize {
        self.writer
            .reconnect_state()
            .max_concurrent
            .load(Ordering::SeqCst)
    }

    /// Run all due reconnect obligations at the current clock, one attempt at a time on this node.
    pub async fn run_due(&self, dial: &AlpnRecoveryDial) {
        run_due_at(&self.writer, dial, local_now_ms()).await;
    }
}

/// Run all pending reconnect obligations on `writer` whose `due_ms <= now_ms`, executing at most
/// one reconnect attempt at a time on the node (`connections.md` §9.4, §9.5).
pub async fn run_due_at(
    writer: &ConnectionsWriter,
    dial: &AlpnRecoveryDial,
    now_ms: u64,
) {
    let state = writer.reconnect_state();
    let keys: Vec<(NodeId, IncarnationId, Vec<u8>)> =
        state.recovery.lock().unwrap().keys().cloned().collect();
    for key in keys {
        // Acquire the node-wide single-attempt gate so at most one reconnect dial runs at a time on
        // this node, even if multiple obligations are due or multiple passes overlap.
        let _attempt_guard = state.attempt_gate.lock().await;
        let Some(Pending {
            destination,
            alpn,
            traceparent,
        }) = state.recovery.lock().unwrap().get(&key).cloned()
        else {
            continue;
        };
        let source = writer.own().name;
        let step = {
            let held_arc = writer.held();
            let held = held_arc.lock().unwrap();
            if held.end_superseded(&destination) == Some(true) {
                Step::Spent("destination-superseded")
            } else if matches!(
                held.active_direct_for(&source, &destination.name, &alpn),
                Some(Some(edge)) if edge.destination.incarnation == destination.incarnation
            ) {
                Step::Spent("direct-connected")
            } else {
                let (due_ms, basis) = recovery_due(&held, &destination, &alpn);
                let current_now = local_now_ms().max(now_ms);
                if current_now < due_ms {
                    Step::Wait
                } else {
                    Step::Dial(due_ms, basis)
                }
            }
        };
        match step {
            Step::Wait => {}
            Step::Spent(reason) => spend(writer, &key, &source, &destination, reason),
            Step::Dial(due_ms, basis) => {
                let cur = state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                state.max_concurrent.fetch_max(cur, Ordering::SeqCst);

                let span = tracing::info_span!(
                    "rdm.node_admin.connection.create.via-recovery-dial",
                    source = %source,
                    destination = %destination.name,
                    destination_node_id = %destination.node_id,
                    destination_incarnation = %destination.incarnation.as_ref().map(ToString::to_string).unwrap_or_default(),
                    due_ms,
                    outcome = tracing::field::Empty,
                    fact = tracing::field::Empty,
                    entry = tracing::field::Empty,
                );
                if let Some(tp) = &traceparent {
                    rdm_transport::set_parent(&span, tp);
                }
                let outcome = tracing::Instrument::instrument(
                    dial(destination.clone(), alpn.clone()),
                    span.clone(),
                )
                .await;
                writer.observations_landed().await;
                state.in_flight.fetch_sub(1, Ordering::SeqCst);

                let latest = writer.latest_direct_for(&destination, &alpn);
                span.record("outcome", outcome.as_str());
                let fact = latest.as_ref().filter(|r| Some(&r.id) != basis.as_ref());
                span.record("fact", fact.map_or("none", |r| state_name(r.state)));
                if let Some(r) = fact {
                    span.record("entry", r.id.to_string().as_str());
                }
                span.in_scope(|| {
                    tracing::info!(
                        "this node dialled its own connection toward the destination birth"
                    )
                });
                match latest {
                    Some(r)
                        if Some(&r.id) != basis.as_ref()
                            && r.state == ConnectionState::Connected =>
                    {
                        spend(writer, &key, &source, &destination, "direct-connected");
                    }
                    Some(r) if Some(&r.id) != basis.as_ref() => {
                        if let Some(p) = state.recovery.lock().unwrap().get_mut(&key) {
                            p.traceparent = None;
                        }
                    }
                    _ => spend(writer, &key, &source, &destination, "no-fact"),
                }
            }
        }
    }
}

/// Spawn the node's one connections reconciler (`connections.md` §9.4, §10) with an
/// [`AlpnRecoveryDial`].
pub fn spawn_alpn_reconciler(
    writer: &ConnectionsWriter,
    every: Duration,
    dial: AlpnRecoveryDial,
) -> tokio::task::JoinHandle<()> {
    spawn_reconciler_inner(writer, every, Some(dial))
}

/// Spawn the node's one connections reconciler (`connections.md` §9.4, §10) with an optional
/// single-ALPN [`RecoveryDial`].
pub fn spawn_reconciler(
    writer: &ConnectionsWriter,
    every: Duration,
    dial: Option<RecoveryDial>,
) -> tokio::task::JoinHandle<()> {
    spawn_reconciler_inner(writer, every, dial.map(adapt_recovery_dial))
}

fn spawn_reconciler_inner(
    writer: &ConnectionsWriter,
    every: Duration,
    dial: Option<AlpnRecoveryDial>,
) -> tokio::task::JoinHandle<()> {
    let w = writer.clone();
    writer.runtime_handle().spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(every) => {}
                _ = w.reconnect_state().wake.notified() => {}
            }
            if !w.owed().is_empty() {
                let _ = w.settle_owed().await;
            }
            if let Some(dial) = &dial {
                run_due_at(&w, dial, local_now_ms()).await;
            }
        }
    })
}

/// Handle an inbound connection accepted from `peer` on `alpn` (`connections.md` §9.4, §10, §13 G).
pub(crate) fn accepted_on_alpn(
    writer: &ConnectionsWriter,
    peer: &Birth,
    alpn: &[u8],
    peer_short: &str,
) {
    let destination = peer.to_end();
    let (source, proxied) = {
        let held_arc = writer.held();
        let held = held_arc.lock().unwrap();
        let Some(proxy) = held
            .own_active_proxies()
            .into_iter()
            .find(|p| p.destination.name == destination.name && p.alpn.as_slice() == alpn)
        else {
            return;
        };
        let direct = matches!(
            held.active_direct_for(&proxy.source.name, &destination.name, &proxy.alpn),
            Some(Some(_))
        );
        (
            proxy.source.name.clone(),
            (proxy.destination.clone(), proxy.alpn.clone(), direct),
        )
    };
    let (named, proxy_alpn, direct) = proxied;
    let key = (
        destination.node_id.clone(),
        peer.incarnation.clone(),
        proxy_alpn.clone(),
    );
    let span = tracing::info_span!(
        "rdm.node_admin.connection.update.via-accept-trigger",
        source = %source,
        destination = %destination.name,
        destination_node_id = %destination.node_id,
        destination_incarnation = %peer.incarnation,
        proxy_destination_node_id = %named.node_id,
        proxy_destination_incarnation = %named.incarnation.as_ref().map(ToString::to_string).unwrap_or_default(),
        peer = %peer_short,
        outcome = tracing::field::Empty,
    );
    let outcome = if (&named.node_id, &named.incarnation)
        != (&destination.node_id, &destination.incarnation)
    {
        "another-birth"
    } else if direct {
        "direct-connected"
    } else {
        match writer.reconnect_state().recovery.lock().unwrap().entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if e.get().traceparent.is_none() {
                    e.get_mut().traceparent = rdm_transport::traceparent_of(&span);
                }
                "coalesced"
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(Pending {
                    destination: destination.clone(),
                    alpn: proxy_alpn,
                    traceparent: rdm_transport::traceparent_of(&span),
                });
                "pending"
            }
        }
    };
    span.record("outcome", outcome);
    span.in_scope(|| {
        tracing::info!(
            "an accept from the destination: a recovery trigger for this node's own dial, never a Direct entry"
        )
    });
    if matches!(outcome, "pending" | "coalesced") {
        writer.reconnect_state().wake.notify_one();
    }
}

/// Handle an inbound connection accepted from `node` across any active Proxy to `node.name`.
pub(crate) fn accepted_resolved(writer: &ConnectionsWriter, node: &ResolvedNode) {
    let alpns: Vec<Vec<u8>> = {
        let held_arc = writer.held();
        let held = held_arc.lock().unwrap();
        held.own_active_proxies()
            .into_iter()
            .filter(|p| p.destination.name == node.name)
            .map(|p| p.alpn.clone())
            .collect()
    };
    let birth = Birth {
        name: node.name.clone(),
        node_id: node.node_id.clone(),
        incarnation: node.incarnation.clone(),
    };
    let peer_short = node.endpoint_id.fmt_short().to_string();
    for alpn in alpns {
        accepted_on_alpn(writer, &birth, &alpn, &peer_short);
    }
}
