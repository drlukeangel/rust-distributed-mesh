//! Per-node Direct reconnect scheduler and Proxy delete reconciler
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

/// How often the reconciler checks what is owed without an explicit wake: an owed Proxy delete
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
    /// The latest Direct row the item's last dial was scheduled from, once it has dialled and
    /// failed: the item is next scheduled from the Failed row that dial owes, once it lands (§9.5
    /// step 4), and is never dialled again from this one (Q169: the dial's result is read in
    /// memory, never by waiting for its row).
    pub(crate) attempted: Option<Option<ConnectionId>>,
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
                    Ok(conn) => {
                        // The dial names the entry it opened, its row owed (Q169).
                        tracing::Span::current().record("entry", conn.id().to_string().as_str());
                        "Reply".to_string()
                    }
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

/// Arm the one pending recovery attempt per `(destination, destination_incarnation, alpn)` from this
/// node's own latest `Direct Failed` row toward `destination` on `alpn` in `held`, the pair of the
/// row just applied (`connections.md` §9.4, §9.5, Q7). A series for a superseded birth at the same
/// path and ALPN is replaced by the current birth's series; a series whose latest Direct row is
/// `Connected` is satisfied. Another pair's attempt is never touched: a row landing for one
/// destination never arms again an attempt another destination's dial spent on its Reply while
/// that destination's own Direct Connected row is still owed (§9.4: a reconnect on one pair never
/// touches another's; Q169: the dial's own result decides, in memory).
pub(crate) fn sync_recovery(writer: &ConnectionsWriter, destination: &PathName, alpn: &[u8]) {
    let (directs, own_source): (Vec<NodeConnection>, Option<PathName>) = {
        let held_arc = writer.held();
        let held = held_arc.lock().unwrap();
        (
            held.own_latest_directs()
                .into_iter()
                .filter(|r| &r.destination.name == destination && r.alpn.as_slice() == alpn)
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
                if recovery.remove(&(row.destination.node_id.clone(), inc.clone(), row.alpn.clone())).is_some() {
                    if let Some(source) = own_source.as_ref() {
                        spent_span(source, &row.destination, "direct-connected");
                    }
                }
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
                    let superseded: Vec<(NodeId, IncarnationId, Vec<u8>)> = recovery
                        .iter()
                        .filter(|((id, i, a), p)| p.destination.name == row.destination.name && a == &row.alpn && !(id == &row.destination.node_id && i == &inc))
                        .map(|(k, _)| k.clone())
                        .collect();
                    for key in superseded {
                        if let (Some(p), Some(source)) = (recovery.remove(&key), own_source.as_ref()) {
                            spent_span(source, &p.destination, "destination-superseded");
                        }
                    }
                    recovery
                        .entry((row.destination.node_id.clone(), inc, row.alpn.clone()))
                        .or_insert_with(|| Pending {
                            destination: row.destination.clone(),
                            alpn: row.alpn.clone(),
                            traceparent: None,
                            attempted: None,
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
    // One span per pending attempt: an attempt a landed Direct Connected already ended
    // (`sync_recovery`) is not ended again.
    if writer.reconnect_state().recovery.lock().unwrap().remove(key).is_some() {
        spent_span(source, destination, reason);
    }
}

/// A pending attempt ended without a dial of its own, or after it (`connections.md` §12: the
/// reconnect attempt span records whether it was superseded before dial or already satisfied by
/// Direct Connected).
fn spent_span(source: &PathName, destination: &ConnectionEnd, reason: &'static str) {
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
            attempted,
        }) = state.recovery.lock().unwrap().get(&key).cloned()
        else {
            continue;
        };
        let source = writer.own().name;
        let (step, series) = {
            let held_arc = writer.held();
            let held = held_arc.lock().unwrap();
            // The position this attempt takes in its series: the latest Failed row's round, one
            // ordinal above it (connections.md §9.3, §12).
            let series = held
                .own_latest_directs()
                .into_iter()
                .find(|d| d.destination.name == destination.name && d.alpn == alpn && d.state == ConnectionState::Failed && (&d.destination.node_id, &d.destination.incarnation) == (&destination.node_id, &destination.incarnation))
                .and_then(|d| d.recovery);
            let step = if held.end_superseded(&destination) == Some(true) {
                Step::Spent("destination-superseded")
            } else if matches!(
                held.active_direct_for(&source, &destination.name, &alpn),
                Some(Some(edge)) if edge.destination.incarnation == destination.incarnation
            ) {
                Step::Spent("direct-connected")
            } else {
                let (due_ms, basis) = recovery_due(&held, &destination, &alpn);
                let current_now = local_now_ms().max(now_ms);
                if attempted.as_ref() == Some(&basis) {
                    // The last dial failed and no newer row is held: its Failed row is still owed,
                    // and the item is scheduled from it once it lands; with nothing owed, the dial
                    // owed no fact and the item is spent.
                    if writer.in_flight() == 0 { Step::Spent("no-fact") } else { Step::Wait }
                } else if current_now < due_ms {
                    Step::Wait
                } else {
                    Step::Dial(due_ms, basis)
                }
            };
            (step, series)
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
                    recovery_round = tracing::field::Empty,
                    attempt_ordinal = tracing::field::Empty,
                    outcome = tracing::field::Empty,
                    fact = tracing::field::Empty,
                    entry = tracing::field::Empty,
                );
                if let Some(r) = series {
                    span.record("recovery_round", r.recovery_round).record("attempt_ordinal", r.attempt_ordinal.saturating_add(1));
                }
                if let Some(tp) = &traceparent {
                    rdm_transport::set_parent(&span, tp);
                }
                let outcome = tracing::Instrument::instrument(
                    dial(destination.clone(), alpn.clone()),
                    span.clone(),
                )
                .await;
                state.in_flight.fetch_sub(1, Ordering::SeqCst);

                // The dial's own result decides, in memory (Q169): its row is owed and lands
                // behind it, and nothing here waits for it. The entry is named by the dial.
                let connected = outcome == "Reply";
                span.record("outcome", outcome.as_str());
                span.record("fact", if connected { "connected" } else { "failed" });
                span.in_scope(|| {
                    tracing::info!(
                        "this node dialled its own connection toward the destination birth"
                    )
                });
                drop(span);
                if connected {
                    spend(writer, &key, &source, &destination, "direct-connected");
                } else if let Some(p) = state.recovery.lock().unwrap().get_mut(&key) {
                    p.traceparent = None;
                    p.attempted = Some(basis);
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
                    attempted: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryConnectionsStorage;
    use rdm_core::NodeKind;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct FieldVisitor(BTreeMap<String, String>);

    impl tracing::field::Visit for FieldVisitor {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}").trim_matches('"').to_string());
        }
    }

    #[derive(Clone, Default)]
    struct SpanRecorder {
        next_id: Arc<AtomicUsize>,
        spans: Arc<Mutex<BTreeMap<u64, (String, BTreeMap<String, String>)>>>,
    }

    impl tracing::Subscriber for SpanRecorder {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed) as u64 + 1;
            let mut v = FieldVisitor::default();
            attrs.record(&mut v);
            self.spans
                .lock()
                .unwrap()
                .insert(id, (attrs.metadata().name().to_string(), v.0));
            tracing::span::Id::from_u64(id)
        }
        fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
            let mut v = FieldVisitor::default();
            values.record(&mut v);
            if let Some((_, fields)) = self.spans.lock().unwrap().get_mut(&span.into_u64()) {
                fields.extend(v.0);
            }
        }
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, _event: &tracing::Event<'_>) {}
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    // @feature: connections-reconnect
    #[test]
    fn reconciler_reconstructs_due_obligation_coalesces_accept_triggers_and_spends_on_connected_dial() {
        // docs/architecture/connections.md:332,358 — "A reconnect series exists when the latest
        // Direct record for `(source, destination, alpn)` is `Failed` for the current destination
        // incarnation and no newer `Direct Connected` on that ALPN supersedes it... Each node runs
        // one scheduler/reconciler with at most one pending item per
        // `(destination, destination_incarnation, alpn)` and executes at most one reconnect dial
        // attempt at a time on the node."
        let recorder = SpanRecorder::default();
        let dispatch = tracing::Dispatch::new(recorder.clone());
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");

        rt.block_on(async {
            let src = ConnectionEnd {
                name: PathName::new("mesh1", NodeKind::Gateway, 1),
                node_id: NodeId::mint(),
                incarnation: Some(IncarnationId::mint()),
            };
            let carrier = ConnectionEnd {
                name: PathName::new("mesh1", NodeKind::Gateway, 2),
                node_id: NodeId::mint(),
                incarnation: Some(IncarnationId::mint()),
            };
            let dst_inc = IncarnationId::mint();
            let dst = ConnectionEnd {
                name: PathName::new("mesh1", NodeKind::Broker, 1),
                node_id: NodeId::mint(),
                incarnation: Some(dst_inc.clone()),
            };
            let alpn = b"app-reconnect/1".to_vec();

            let storage = Arc::new(MemoryConnectionsStorage::default());
            let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
            let writer = Arc::new(ConnectionsWriter::new(src.clone(), storage, held));

            let base_ms = local_now_ms() + 60_000;
            let failed_id = ConnectionId::mint();
            let failed_row = NodeConnection {
                id: failed_id.clone(),
                source: src.clone(),
                destination: dst.clone(),
                kind: ConnectionKind::Direct,
                alpn: alpn.clone(),
                state: ConnectionState::Failed,
                carrier: None,
                recovery: Some(DirectRecovery {
                    recovery_round: 1,
                    attempt_ordinal: 1,
                }),
                reason: Some("NotSent".into()),
                logged_at_ms: base_ms,
            };
            writer.record(failed_row.clone()).await.expect("record failed");
            writer
                .record(NodeConnection {
                    id: ConnectionId::mint(),
                    source: src.clone(),
                    destination: dst.clone(),
                    kind: ConnectionKind::Proxy,
                    alpn: alpn.clone(),
                    state: ConnectionState::Connected,
                    carrier: Some(carrier),
                    recovery: None,
                    reason: None,
                    logged_at_ms: base_ms + 1,
                })
                .await
                .expect("record proxy");

            let obs = obligations(&writer);
            assert_eq!(obs.len(), 1);
            let due_ms = next_due(&failed_row).expect("due_ms");
            assert_eq!(obs[0].destination, dst);
            assert_eq!(obs[0].destination_incarnation, dst_inc);
            assert_eq!(obs[0].alpn, alpn);
            assert_eq!(obs[0].due_ms, due_ms);
            assert_eq!(obs[0].row_id, failed_id);

            // Accept from the exact destination birth coalesces into the existing pending item;
            // accept from another birth is ignored with outcome="another-birth".
            accepted_on_alpn(
                &writer,
                &Birth {
                    name: dst.name.clone(),
                    node_id: dst.node_id.clone(),
                    incarnation: dst_inc.clone(),
                },
                &alpn,
                "peer-cur",
            );
            accepted_on_alpn(
                &writer,
                &Birth {
                    name: dst.name.clone(),
                    node_id: dst.node_id.clone(),
                    incarnation: IncarnationId::mint(),
                },
                &alpn,
                "peer-stale",
            );
            assert_eq!(obligations(&writer).len(), 1);

            let dial_calls = Arc::new(AtomicUsize::new(0));
            let dial_calls_cl = dial_calls.clone();
            let dial: AlpnRecoveryDial = Arc::new(move |_dest, _alpn| {
                let calls = dial_calls_cl.clone();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    "Reply".to_string()
                })
            });

            // Before due_ms, run_due_at waits and performs no dial.
            run_due_at(&writer, &dial, due_ms - 1).await;
            assert_eq!(dial_calls.load(Ordering::SeqCst), 0);
            assert_eq!(obligations(&writer).len(), 1);

            // At due_ms, run_due_at dials once, records max_concurrent_attempts == 1, and spends the obligation.
            let reconciler = Reconciler::new(writer.clone());
            run_due_at(&writer, &dial, due_ms).await;
            assert_eq!(dial_calls.load(Ordering::SeqCst), 1);
            assert_eq!(reconciler.in_flight_attempts(), 0);
            assert_eq!(reconciler.max_concurrent_attempts(), 1);
            assert!(obligations(&writer).is_empty());
        });

        drop(rt);

        let recorded: Vec<(String, BTreeMap<String, String>)> =
            recorder.spans.lock().unwrap().values().cloned().collect();
        assert!(
            recorded.iter().any(|(name, attrs)| {
                name == "rdm.node_admin.connection.update.via-accept-trigger"
                    && attrs.get("outcome").map(String::as_str) == Some("coalesced")
            }),
            "expected via-accept-trigger span with outcome=coalesced"
        );
        assert!(
            recorded.iter().any(|(name, attrs)| {
                name == "rdm.node_admin.connection.update.via-accept-trigger"
                    && attrs.get("outcome").map(String::as_str) == Some("another-birth")
            }),
            "expected via-accept-trigger span with outcome=another-birth"
        );
        assert!(
            recorded.iter().any(|(name, attrs)| {
                name == "rdm.node_admin.connection.create.via-recovery-dial"
                    && attrs.get("outcome").map(String::as_str) == Some("Reply")
                    && attrs.get("fact").map(String::as_str) == Some("connected")
            }),
            "expected via-recovery-dial span with outcome=Reply and fact=connected"
        );
        assert!(
            recorded.iter().any(|(name, attrs)| {
                name == "rdm.node_admin.connection.remove.via-recovery-attempt"
                    && attrs.get("reason").map(String::as_str) == Some("direct-connected")
            }),
            "expected via-recovery-attempt span with reason=direct-connected"
        );
    }
}

