use crate::traits::CacheTier;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HydrationSource {
    Peer,
    TopicReplay,
}

#[derive(Debug, Clone)]
pub enum CacheEvent {
    Hydrating {
        entity_kind: &'static str,
        tier: CacheTier,
        source: HydrationSource,
        binding_count: u64,
    },
    Ready {
        entity_kind: &'static str,
        tier: CacheTier,
    },
    Degraded {
        entity_kind: &'static str,
        tier: CacheTier,
        reason: String,
    },
}

static CACHE_HEALTH_TX: OnceLock<tokio::sync::broadcast::Sender<CacheEvent>> = OnceLock::new();

/// Current status per entity_kind — written on every state change, readable at any time.
/// Fixes the broadcast-late-subscriber race: mark_ready() fires before
/// spawn_local_health_tracker subscribes, so channel events are lost.
static CACHE_STATUS_SNAPSHOT: std::sync::LazyLock<
    dashmap::DashMap<&'static str, (&'static str, &'static str)>
> = std::sync::LazyLock::new(dashmap::DashMap::new);
// Value: (status, tier) where status ∈ {"ready","hydrating","degraded"}, tier ∈ {"functional","feature"}

pub fn record_cache_status(entity_kind: &'static str, status: &'static str, tier: &'static str) {
    CACHE_STATUS_SNAPSHOT.insert(entity_kind, (status, tier));
}

pub fn get_cache_status_snapshot() -> Vec<(&'static str, &'static str, &'static str)> {
    CACHE_STATUS_SNAPSHOT.iter().map(|r| (*r.key(), r.value().0, r.value().1)).collect()
}

pub fn cache_event_sender() -> &'static tokio::sync::broadcast::Sender<CacheEvent> {
    CACHE_HEALTH_TX.get_or_init(|| {
        let (tx, _) = tokio::sync::broadcast::channel(256);
        tx
    })
}

pub fn subscribe_cache_events() -> tokio::sync::broadcast::Receiver<CacheEvent> {
    cache_event_sender().subscribe()
}

/// Simple cache status record for broker health reporting.
#[derive(Debug, Clone)]
pub struct BrokerCacheStatus {
    pub entity_kind: &'static str,
    pub tier: &'static str,
    pub status: &'static str,
}

/// Returns current cache statuses from the channel snapshot.
/// Used by the broker's Op 60 handler to respond to HealthRefreshRequest.
pub fn broker_cache_statuses() -> Vec<BrokerCacheStatus> {
    let mut rx = subscribe_cache_events();
    let mut statuses = std::collections::HashMap::new();
    while let Ok(event) = rx.try_recv() {
        match event {
            CacheEvent::Ready { entity_kind, tier } => {
                statuses.insert(entity_kind, ("ready", match tier { CacheTier::Functional => "functional", CacheTier::Feature => "feature" }));
            }
            CacheEvent::Hydrating { entity_kind, tier, .. } => {
                statuses.insert(entity_kind, ("hydrating", match tier { CacheTier::Functional => "functional", CacheTier::Feature => "feature" }));
            }
            CacheEvent::Degraded { entity_kind, tier, .. } => {
                statuses.insert(entity_kind, ("degraded", match tier { CacheTier::Functional => "functional", CacheTier::Feature => "feature" }));
            }
        }
    }
    statuses
        .into_iter()
        .map(|(kind, (status, tier))| BrokerCacheStatus { entity_kind: kind, tier, status })
        .collect()
}

// Exposed for integration tests.
pub fn test_channel() -> (
    tokio::sync::broadcast::Sender<CacheEvent>,
    tokio::sync::broadcast::Receiver<CacheEvent>,
) {
    tokio::sync::broadcast::channel(256)
}
