//! A source's Direct failure rows and the ordered Proxy delete (connections.md §7, §9, §10).
//!
//! A lost Direct is dialled on `Send` and nothing else (Q344, Q348): when its attempts are spent the
//! dial ends to the application by name, and one Direct `Failed` row records it (connections.md
//! §9.3: one dial is one row). No schedule is derived from that row and nothing dials again from it.
//!
//! Any Direct `Connected` for a pair whose own Proxy is still `Connected` owes a
//! `Proxy Disconnected(reason=direct-restored)`. The Proxy stays the effective route until that
//! write is applied, and a failed write is owed again: cutback follows the durable write.

use crate::connections::{ConnectionState, ConnectionsHeld, NodeConnection};

/// The reason a Proxy deleted because its Direct came back carries.
pub const DIRECT_RESTORED: &str = "direct-restored";

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
            logged_at_ms: now_ms.max(p.logged_at_ms.saturating_add(1)),
            ..p.clone()
        })
        .collect()
}
