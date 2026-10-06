//! The source-owned Direct reconnect reconciler and ordered Proxy retirement (i143.e4.s3).
//!
//! A reconnect series exists while this source's latest Direct entry to a destination is `Failed`
//! for the destination's current process. Its schedule is reconstructed from that one entry —
//! `recovery_epoch`, `attempt_ordinal` and when it was written — so a restart neither resets the
//! backoff nor attempts early. One item per `(destination, destination incarnation)`; nothing here
//! is driven by request traffic.
//!
//! Any Direct `Connected` for a pair whose own Proxy is still `Connected` owes a
//! `Proxy Disconnected(reason=direct-restored)`. The Proxy stays the effective route until that
//! write is applied, and a failed write is owed again: cutback follows durable retirement.

use crate::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery, NodeConnection};
use crate::connections::spread;
use crate::ids::IncarnationId;
use crate::path::PathName;

/// The backoff of a series' first attempt, ms.
pub const RECONNECT_BACKOFF_BASE_MS: u64 = 4_000;
/// The backoff a series is held at once doubling reaches it, ms.
pub const RECONNECT_BACKOFF_MAX_MS: u64 = 120_000;
/// The reason a Proxy retired because its Direct came back carries.
pub const DIRECT_RESTORED: &str = "direct-restored";

/// The backoff after attempt `attempt_ordinal`, ms: the base doubled per ordinal after the first,
/// held at the maximum. Ordinal 0 reads as 1.
pub fn reconnect_backoff(attempt_ordinal: u32) -> u64 {
    let doublings = attempt_ordinal.max(1) - 1;
    if doublings >= 20 {
        return RECONNECT_BACKOFF_MAX_MS;
    }
    RECONNECT_BACKOFF_BASE_MS.saturating_mul(1u64 << doublings).min(RECONNECT_BACKOFF_MAX_MS)
}

/// The jitter added to attempt `attempt_ordinal`'s backoff for this pair, ms: FNV-1a over the
/// source, destination and ordinal, below a quarter of the backoff; the same in every process.
pub fn deterministic_jitter(source: &PathName, destination: &PathName, attempt_ordinal: u32) -> u64 {
    let span = reconnect_backoff(attempt_ordinal) / 4;
    if span == 0 {
        return 0;
    }
    spread(&source.to_string(), &destination.to_string(), &attempt_ordinal.to_string()) % span
}

/// When the attempt after `row` is due, ms: `Some` only for a Direct `Failed` entry carrying its
/// recovery.
pub fn next_due(row: &NodeConnection) -> Option<u64> {
    if row.kind != ConnectionKind::Direct || row.state != ConnectionState::Failed {
        return None;
    }
    let ordinal = row.recovery?.attempt_ordinal;
    Some(
        row.logged_at_ms
            .saturating_add(reconnect_backoff(ordinal))
            .saturating_add(deterministic_jitter(&row.source.name, &row.destination.name, ordinal)),
    )
}

/// One pending reconnect: this source's series to one destination process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectItem {
    pub destination: ConnectionEnd,
    pub destination_incarnation: IncarnationId,
    pub recovery: DirectRecovery,
    pub due_ms: u64,
    /// The latest Failed entry the item was reconstructed from.
    pub failed: NodeConnection,
}

/// This source's pending reconnects, reconstructed from its latest Direct entries: one per
/// destination whose latest Direct is `Failed` for the destination process `current_incarnation`
/// names now. A series for a superseded process is no longer owed; a destination whose current
/// process is unknown keeps its series.
pub fn reconnect_plan(held: &ConnectionsHeld, current_incarnation: impl Fn(&PathName) -> Option<IncarnationId>) -> Vec<ReconnectItem> {
    held.own_latest_directs()
        .into_iter()
        .filter_map(|row| {
            let due_ms = next_due(row)?;
            let recovery = row.recovery?;
            let recorded = row.destination.incarnation.clone()?;
            if current_incarnation(&row.destination.name).is_some_and(|now| now != recorded) {
                return None;
            }
            Some(ReconnectItem { destination: row.destination.clone(), destination_incarnation: recorded, recovery, due_ms, failed: row.clone() })
        })
        .collect()
}

/// The items due at `now_ms`, earliest first.
pub fn due(plan: &[ReconnectItem], now_ms: u64) -> Vec<&ReconnectItem> {
    let mut out: Vec<&ReconnectItem> = plan.iter().filter(|i| i.due_ms <= now_ms).collect();
    out.sort_by_key(|i| i.due_ms);
    out
}

/// The Direct `Failed` entry an attempt that failed at `now_ms` appends: the next ordinal of the
/// same epoch. The caller writes it and reschedules from it.
pub fn next_failure(item: &ReconnectItem, reason: impl Into<String>, now_ms: u64) -> NodeConnection {
    NodeConnection {
        state: ConnectionState::Failed,
        recovery: Some(DirectRecovery {
            recovery_epoch: item.recovery.recovery_epoch,
            attempt_ordinal: item.recovery.attempt_ordinal.saturating_add(1),
        }),
        reason: Some(reason.into()),
        logged_at_ms: now_ms,
        ..item.failed.clone()
    }
}

/// The first Direct `Failed` entry of a new incident: a new epoch, ordinal 1. `previous` is the
/// source's latest Direct entry to this destination, if any.
pub fn first_failure(
    source: ConnectionEnd,
    destination: ConnectionEnd,
    previous: Option<&NodeConnection>,
    reason: impl Into<String>,
    now_ms: u64,
) -> NodeConnection {
    let epoch = previous.and_then(|p| p.recovery).map_or(1, |r| r.recovery_epoch.saturating_add(1));
    NodeConnection {
        source,
        destination,
        kind: ConnectionKind::Direct,
        state: ConnectionState::Failed,
        carrier: None,
        recovery: Some(DirectRecovery { recovery_epoch: epoch, attempt_ordinal: 1 }),
        reason: Some(reason.into()),
        logged_at_ms: now_ms,
    }
}

/// The Proxy retirements this source owes now: each own active Proxy whose pair has an active
/// Direct, as the `Proxy Disconnected(direct-restored)` entry to write at `now_ms`.
pub fn owed_retirements(held: &ConnectionsHeld, now_ms: u64) -> Vec<NodeConnection> {
    held.own_active_proxies()
        .into_iter()
        .filter(|p| matches!(held.active_direct(&p.source.name, &p.destination.name), Some(Some(_))))
        .map(|p| NodeConnection {
            state: ConnectionState::Disconnected,
            reason: Some(DIRECT_RESTORED.into()),
            recovery: None,
            logged_at_ms: now_ms.max(p.logged_at_ms.saturating_add(1)),
            ..p.clone()
        })
        .collect()
}
