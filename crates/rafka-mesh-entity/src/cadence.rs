//! The fabric's three configured cadences, read once from the environment. They live here, the
//! lowest crate that membership and Node RPC both depend on, so the transport configuration
//! (`rafka_node_rpc::endpoint`) and the membership channel read the same values.

use std::time::Duration;

fn env_ms(key: &str, default_ms: u64) -> Duration {
    Duration::from_millis(std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default_ms))
}

/// The staleness floor, `RDM_STALENESS_MS` (default 30 s): how long a member stays heard without
/// a fresh word, local receipt age (fabric-node-lifecycle.md §7.3, i77 PRD row 18). Past it a
/// member is `PendingReconnect`: soft and reversible, never death.
pub fn staleness_floor() -> Duration {
    static FLOOR: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *FLOOR.get_or_init(|| env_ms("RDM_STALENESS_MS", 30_000))
}

/// The mesh channel's gossip interval, `RDM_GOSSIP_INTERVAL_MS` (default 2 s; never slower,
/// gossip.md §3.2): how often a node publishes its digest. Each gossip topic has its own interval.
pub fn gossip_interval() -> Duration {
    static EVERY: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *EVERY.get_or_init(|| env_ms("RDM_GOSSIP_INTERVAL_MS", 2_000))
}

/// The backbone topic's gossip interval, `RDM_BACKBONE_INTERVAL_MS` (default 2 s,
/// fabric-node-lifecycle.md:355): how often a mesh's primary publishes its mesh on the backbone.
pub fn backbone_gossip_interval() -> Duration {
    static EVERY: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *EVERY.get_or_init(|| env_ms("RDM_BACKBONE_INTERVAL_MS", 2_000))
}
