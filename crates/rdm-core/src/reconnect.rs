//! A source's Direct failure rows and the ordered Proxy delete (connections.md §7, §9, §10).
//!
//! A lost Direct is dialled on `Send` and nothing else (Q344, Q348): when its attempts are spent the
//! dial ends to the application by name, and the first Direct `Failed` row of the incident records
//! it. No schedule is derived from that row and nothing dials again from it.
//!
//! Any Direct `Connected` for a pair whose own Proxy is still `Connected` owes a
//! `Proxy Disconnected(reason=direct-restored)`. The Proxy stays the effective route until that
//! write is applied, and a failed write is owed again: cutback follows the durable write.

use crate::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery, NodeConnection};

/// The reason a Proxy deleted because its Direct came back carries.
pub const DIRECT_RESTORED: &str = "direct-restored";

/// The first Direct `Failed` entry of a new incident on the default ALPN (or `previous`'s ALPN): a
/// new round, ordinal 1. `previous` is the source's latest Direct entry to this destination, if any.
pub fn first_failure(
    source: ConnectionEnd,
    destination: ConnectionEnd,
    previous: Option<&NodeConnection>,
    reason: impl Into<String>,
    now_ms: u64,
) -> NodeConnection {
    let alpn = previous.map_or_else(|| crate::connections::ALPN.to_vec(), |p| p.alpn.clone());
    first_failure_for(source, destination, alpn, previous, reason, now_ms)
}

/// [`first_failure`] on `alpn`.
pub fn first_failure_for(
    source: ConnectionEnd,
    destination: ConnectionEnd,
    alpn: Vec<u8>,
    previous: Option<&NodeConnection>,
    reason: impl Into<String>,
    now_ms: u64,
) -> NodeConnection {
    let round = previous.and_then(|p| p.recovery).map_or(1, |r| r.recovery_round.saturating_add(1));
    NodeConnection {
        id: crate::ConnectionId::mint(),
        source,
        destination,
        kind: ConnectionKind::Direct,
        alpn,
        state: ConnectionState::Failed,
        carrier: None,
        recovery: Some(DirectRecovery { recovery_round: round, attempt_ordinal: 1 }),
        reason: Some(reason.into()),
        logged_at_ms: now_ms,
    }
}

/// The Proxy deletes this source owes now: each own active Proxy whose `(source, destination, alpn)`
/// has an active Direct on that same ALPN, as the `Proxy Disconnected(direct-restored)` entry to write at `now_ms`.
pub fn owed_direct_restored(held: &ConnectionsHeld, now_ms: u64) -> Vec<NodeConnection> {
    held.own_active_proxies()
        .into_iter()
        .filter(|p| matches!(held.active_direct_for(&p.source.node_id, &p.destination.node_id, &p.alpn), Some(Some(connection)) if !held.names_superseded_destination(connection)))
        .map(|p| NodeConnection {
            id: crate::ConnectionId::mint(),
            state: ConnectionState::Disconnected,
            reason: Some(DIRECT_RESTORED.into()),
            recovery: None,
            logged_at_ms: now_ms.max(p.logged_at_ms.saturating_add(1)),
            ..p.clone()
        })
        .collect()
}
