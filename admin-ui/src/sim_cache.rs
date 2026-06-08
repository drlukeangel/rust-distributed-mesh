// Generic multi-cache, multi-channel simulator.
// This module is intentionally free of axum/reqwest/AppState so the generic
// core is importable to v2-mesh. The admin-ui wires the AppState glue in main.rs.

use dashmap::DashMap;
use serde::Serialize;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex as StdMutex,
};
use std::collections::VecDeque;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// Rides the shared membership channel ("main").
    Main,
    /// Has its own dedicated topic channel named by the given string.
    Dedicated(&'static str),
}

impl Channel {
    /// Flat string name — used in event payloads so the UI sees "vt" not
    /// `{"Dedicated":"vt"}`.
    pub fn name(&self) -> &'static str {
        match self {
            Channel::Main => "main",
            Channel::Dedicated(s) => s,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteModel {
    /// Only the elected leader (admin-ui) may publish; non-leader writes are rejected.
    LeaderOnly,
    /// Each node writes only its own key (key == publisher). No cross-key conflicts;
    /// keys are partitioned by writer.
    SelfKey,
    /// Any node may write any key. Conflict resolution: accept only if the incoming
    /// epoch is strictly greater than the stored epoch (monotonic last-writer-wins).
    /// Wall-clock timestamps are NOT safe here.
    SharedKey,
}

/// Describes a logical cache in the system.
#[derive(Debug, Clone)]
pub struct CacheSpec {
    pub entity_kind: &'static str,
    pub channel: Channel,
    pub write_model: WriteModel,
    /// Node types that hold this cache. admin-ui implicitly holds ALL caches.
    pub owners: &'static [&'static str],
}

// ---------------------------------------------------------------------------
// The 9-cache registry (matches the spec table exactly)
// ---------------------------------------------------------------------------

pub const CACHE_REGISTRY: &[CacheSpec] = &[
    CacheSpec {
        entity_kind: "topology",
        channel: Channel::Main,
        write_model: WriteModel::SelfKey,
        owners: &["admin-ui", "gateway", "broker", "compute", "registry"],
    },
    CacheSpec {
        entity_kind: "vt",
        channel: Channel::Dedicated("vt"),
        write_model: WriteModel::SharedKey,
        owners: &["gateway", "compute"],
    },
    CacheSpec {
        entity_kind: "gateway-cache-1",
        channel: Channel::Dedicated("gateway-cache-1"),
        write_model: WriteModel::LeaderOnly,
        owners: &["gateway"],
    },
    CacheSpec {
        entity_kind: "gateway-cache-2",
        channel: Channel::Dedicated("gateway-cache-2"),
        write_model: WriteModel::SelfKey,
        owners: &["gateway"],
    },
    CacheSpec {
        entity_kind: "compute-cache-1",
        channel: Channel::Dedicated("compute-cache-1"),
        write_model: WriteModel::SharedKey,
        owners: &["compute"],
    },
    CacheSpec {
        entity_kind: "compute-cache-2",
        channel: Channel::Dedicated("compute-cache-2"),
        write_model: WriteModel::SelfKey,
        owners: &["compute"],
    },
    CacheSpec {
        entity_kind: "compute-cache-3",
        channel: Channel::Dedicated("compute-cache-3"),
        write_model: WriteModel::LeaderOnly,
        owners: &["compute"],
    },
    CacheSpec {
        entity_kind: "registry-cache-1",
        channel: Channel::Dedicated("registry-cache-1"),
        write_model: WriteModel::SelfKey,
        owners: &["registry"],
    },
    CacheSpec {
        entity_kind: "broker-cache-1",
        channel: Channel::Dedicated("broker-cache-1"),
        write_model: WriteModel::SelfKey,
        owners: &["broker"],
    },
];

// ---------------------------------------------------------------------------
// SimEntry and SimCache
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct SimEntry {
    pub value: u64,
    pub epoch: u64,
    pub publisher: String,
    pub updated_ms: u64,
}

pub struct SimCache {
    pub entity_kind: &'static str,
    pub write_model: WriteModel,
    /// The leader node name for LeaderOnly caches.
    pub leader: String,
    entries: DashMap<String, SimEntry>,
}

/// Result of an `apply` call — tells the caller what happened.
#[derive(Debug, PartialEq, Eq)]
pub enum ApplyResult {
    Accepted,
    /// Write-model rule rejected this write (e.g. wrong publisher, stale epoch).
    Rejected { reason: &'static str },
}

/// Result of an `apply_remove` call.
#[derive(Debug, PartialEq, Eq)]
pub enum RemoveResult {
    Removed,
    Rejected { reason: &'static str },
    NotFound,
}

impl SimCache {
    pub fn new(spec: &CacheSpec, leader: impl Into<String>) -> Self {
        Self {
            entity_kind: spec.entity_kind,
            write_model: spec.write_model,
            leader: leader.into(),
            entries: DashMap::new(),
        }
    }

    /// Apply a upsert according to this cache's write model.
    ///
    /// - `SelfKey`: key is forced to `publisher` — each node only writes its own row.
    /// - `LeaderOnly`: accept only if `publisher == self.leader`.
    /// - `SharedKey`: accept only if `epoch > existing.epoch` (strict monotonic).
    ///   Equal epochs are REJECTED (last-writer-wins requires a strict ordering;
    ///   ties are indeterminate on the wire).
    pub fn apply(
        &self,
        publisher: &str,
        key: &str,
        value: u64,
        epoch: u64,
    ) -> ApplyResult {
        let now_ms = now_ms();
        match self.write_model {
            WriteModel::LeaderOnly => {
                if publisher != self.leader {
                    return ApplyResult::Rejected {
                        reason: "LeaderOnly: publisher is not the leader",
                    };
                }
                self.entries.insert(
                    key.to_string(),
                    SimEntry { value, epoch, publisher: publisher.to_string(), updated_ms: now_ms },
                );
                ApplyResult::Accepted
            }
            WriteModel::SelfKey => {
                // Key is forced to publisher — cross-key writes are a structural
                // impossibility in this model.
                let canonical_key = publisher.to_string();
                self.entries.insert(
                    canonical_key,
                    SimEntry { value, epoch, publisher: publisher.to_string(), updated_ms: now_ms },
                );
                ApplyResult::Accepted
            }
            WriteModel::SharedKey => {
                // Monotonic last-writer-wins: reject if incoming epoch is not
                // strictly greater than the existing one.
                if let Some(existing) = self.entries.get(key) {
                    if epoch <= existing.epoch {
                        return ApplyResult::Rejected {
                            reason: "SharedKey: epoch is not strictly greater than existing",
                        };
                    }
                }
                self.entries.insert(
                    key.to_string(),
                    SimEntry { value, epoch, publisher: publisher.to_string(), updated_ms: now_ms },
                );
                ApplyResult::Accepted
            }
        }
    }

    /// Remove an entry according to the write model (same publisher rules).
    pub fn apply_remove(&self, publisher: &str, key: &str) -> RemoveResult {
        match self.write_model {
            WriteModel::LeaderOnly => {
                if publisher != self.leader {
                    return RemoveResult::Rejected {
                        reason: "LeaderOnly: publisher is not the leader",
                    };
                }
                if self.entries.remove(key).is_some() {
                    RemoveResult::Removed
                } else {
                    RemoveResult::NotFound
                }
            }
            WriteModel::SelfKey => {
                // Can only remove own row (key == publisher)
                let canonical_key = publisher.to_string();
                if self.entries.remove(&canonical_key).is_some() {
                    RemoveResult::Removed
                } else {
                    RemoveResult::NotFound
                }
            }
            WriteModel::SharedKey => {
                // Any owner may remove any key
                if self.entries.remove(key).is_some() {
                    RemoveResult::Removed
                } else {
                    RemoveResult::NotFound
                }
            }
        }
    }

    /// Snapshot the entire cache state — used for birth-hydration.
    pub fn snapshot(&self) -> Vec<(String, SimEntry)> {
        self.entries
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Hydrate from a previously taken snapshot. Existing entries with higher
    /// epoch are preserved (snapshot is additive-via-write-model).
    pub fn hydrate_from(&self, snapshot: Vec<(String, SimEntry)>) {
        for (key, entry) in snapshot {
            // Route through apply so write model rules are respected.
            let _ = self.apply(&entry.publisher, &key, entry.value, entry.epoch);
        }
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Return up to `n` sample entries as serialisable values.
    pub fn sample_entries(&self, n: usize) -> Vec<serde_json::Value> {
        self.entries
            .iter()
            .take(n)
            .map(|e| {
                serde_json::json!({
                    "key": e.key(),
                    "value": e.value().value,
                    "epoch": e.value().epoch,
                    "publisher": e.value().publisher,
                    "updated_ms": e.value().updated_ms,
                })
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Channel event buffer (rolling, capped at 40)
// ---------------------------------------------------------------------------

/// One event in a channel buffer.
#[derive(Debug, Clone, Serialize)]
pub struct ChannelEvent {
    pub ts_ms: u64,
    /// The channel this event was observed on (flat string: "main", "vt", …).
    pub channel: String,
    pub entity_kind: String,
    pub publisher: String,
    /// "upsert" or "remove"
    pub op: String,
    pub key: String,
    pub value: u64,
    pub epoch: u64,
}

/// A rolling, capped ring buffer of `ChannelEvent`s for one channel.
pub struct ChannelBuffer {
    pub channel_name: String,
    events: StdMutex<VecDeque<ChannelEvent>>,
    cap: usize,
}

impl ChannelBuffer {
    pub fn new(channel_name: impl Into<String>, cap: usize) -> Self {
        Self {
            channel_name: channel_name.into(),
            events: StdMutex::new(VecDeque::with_capacity(cap)),
            cap,
        }
    }

    pub fn push(&self, event: ChannelEvent) {
        let mut g = self.events.lock().unwrap();
        if g.len() >= self.cap {
            g.pop_front();
        }
        g.push_back(event);
    }

    /// Newest-first snapshot.
    pub fn snapshot(&self) -> Vec<ChannelEvent> {
        let g = self.events.lock().unwrap();
        g.iter().rev().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// The 10-channel set (main + backbone + 8 dedicated)
// ---------------------------------------------------------------------------

/// Names of all 10 channels.
pub const ALL_CHANNELS: &[&str] = &[
    "main",
    "backbone",
    "vt",
    "gateway-cache-1",
    "gateway-cache-2",
    "compute-cache-1",
    "compute-cache-2",
    "compute-cache-3",
    "registry-cache-1",
    "broker-cache-1",
];

// ---------------------------------------------------------------------------
// Chatter generator helpers
// ---------------------------------------------------------------------------

/// A small keyset for SharedKey caches so different publishers can write
/// the same keys and trigger epoch-monotonic conflict resolution.
pub const SHARED_KEYSETS: &[(&str, &[&str])] = &[
    ("vt", &["vt-a", "vt-b", "vt-c", "vt-d"]),
    ("compute-cache-1", &["cc1-x", "cc1-y", "cc1-z"]),
];

/// Pick the keyset for a SharedKey cache, or fall back to the publisher name.
pub fn keyset_for(entity_kind: &str) -> &'static [&'static str] {
    for (ek, keys) in SHARED_KEYSETS {
        if *ek == entity_kind {
            return keys;
        }
    }
    &["key-a", "key-b"]
}

// ---------------------------------------------------------------------------
// Monotonic epoch counter — one per cache, shared across all chatter ticks.
// Using a global AtomicU64 array indexed by cache position ensures the epoch
// only ever increases for a given cache, regardless of which "publisher" tick
// emits it.
// ---------------------------------------------------------------------------

static EPOCH_COUNTERS: [AtomicU64; 9] = [
    AtomicU64::new(1),
    AtomicU64::new(1),
    AtomicU64::new(1),
    AtomicU64::new(1),
    AtomicU64::new(1),
    AtomicU64::new(1),
    AtomicU64::new(1),
    AtomicU64::new(1),
    AtomicU64::new(1),
];

/// Fetch-and-increment the epoch for cache at `index` (0-based into CACHE_REGISTRY).
pub fn next_epoch(index: usize) -> u64 {
    EPOCH_COUNTERS[index % 9].fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Unit tests — conflict-resolution proof
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_shared_cache() -> SimCache {
        SimCache::new(
            &CacheSpec {
                entity_kind: "test-shared",
                channel: Channel::Dedicated("test"),
                write_model: WriteModel::SharedKey,
                owners: &["compute"],
            },
            "admin-ui",
        )
    }

    #[test]
    fn shared_key_higher_epoch_accepted() {
        let cache = make_shared_cache();
        assert_eq!(
            cache.apply("publisher-a", "key1", 100, 5),
            ApplyResult::Accepted
        );
        assert_eq!(
            cache.apply("publisher-b", "key1", 200, 6),
            ApplyResult::Accepted
        );
        // Value should be from the epoch-6 write
        let snap = cache.snapshot();
        let entry = snap.iter().find(|(k, _)| k == "key1").unwrap();
        assert_eq!(entry.1.value, 200);
        assert_eq!(entry.1.epoch, 6);
    }

    #[test]
    fn shared_key_lower_epoch_rejected() {
        let cache = make_shared_cache();
        // Write epoch=5 first
        assert_eq!(
            cache.apply("publisher-a", "key1", 100, 5),
            ApplyResult::Accepted
        );
        // Lower epoch — must be rejected
        let result = cache.apply("publisher-b", "key1", 999, 3);
        assert_eq!(
            result,
            ApplyResult::Rejected {
                reason: "SharedKey: epoch is not strictly greater than existing"
            }
        );
        // Value must still be from epoch-5 write
        let snap = cache.snapshot();
        let entry = snap.iter().find(|(k, _)| k == "key1").unwrap();
        assert_eq!(entry.1.value, 100);
        assert_eq!(entry.1.epoch, 5);
    }

    #[test]
    fn shared_key_equal_epoch_rejected() {
        let cache = make_shared_cache();
        assert_eq!(
            cache.apply("publisher-a", "key1", 42, 7),
            ApplyResult::Accepted
        );
        // Equal epoch — ties are indeterminate; rejected
        let result = cache.apply("publisher-b", "key1", 99, 7);
        assert_eq!(
            result,
            ApplyResult::Rejected {
                reason: "SharedKey: epoch is not strictly greater than existing"
            }
        );
        // Original value preserved
        let snap = cache.snapshot();
        let entry = snap.iter().find(|(k, _)| k == "key1").unwrap();
        assert_eq!(entry.1.value, 42);
    }

    #[test]
    fn leader_only_non_leader_rejected() {
        let cache = SimCache::new(
            &CacheSpec {
                entity_kind: "test-leader",
                channel: Channel::Dedicated("test"),
                write_model: WriteModel::LeaderOnly,
                owners: &["gateway"],
            },
            "admin-ui",
        );
        let result = cache.apply("gateway-abc123", "any-key", 1, 1);
        assert_eq!(
            result,
            ApplyResult::Rejected {
                reason: "LeaderOnly: publisher is not the leader"
            }
        );
    }

    #[test]
    fn leader_only_leader_accepted() {
        let cache = SimCache::new(
            &CacheSpec {
                entity_kind: "test-leader",
                channel: Channel::Dedicated("test"),
                write_model: WriteModel::LeaderOnly,
                owners: &["gateway"],
            },
            "admin-ui",
        );
        assert_eq!(
            cache.apply("admin-ui", "any-key", 1, 1),
            ApplyResult::Accepted
        );
    }

    #[test]
    fn self_key_key_forced_to_publisher() {
        let cache = SimCache::new(
            &CacheSpec {
                entity_kind: "test-self",
                channel: Channel::Main,
                write_model: WriteModel::SelfKey,
                owners: &["broker"],
            },
            "admin-ui",
        );
        // Key "some-other-key" is passed but must be stored as the publisher name
        cache.apply("broker-abc", "some-other-key", 77, 1);
        let snap = cache.snapshot();
        // There must be exactly one entry keyed as "broker-abc" (the publisher)
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].0, "broker-abc");
    }

    #[test]
    fn birth_hydration_snapshot_roundtrip() {
        let cache = make_shared_cache();
        cache.apply("publisher-a", "k1", 10, 1);
        cache.apply("publisher-b", "k2", 20, 2);
        let snap = cache.snapshot();
        assert_eq!(snap.len(), 2);

        // Create a new empty cache and hydrate from the snapshot
        let cache2 = make_shared_cache();
        cache2.hydrate_from(snap);
        let snap2 = cache2.snapshot();
        assert_eq!(snap2.len(), 2);
        // Check values are present
        let has_k1 = snap2.iter().any(|(k, e)| k == "k1" && e.value == 10);
        let has_k2 = snap2.iter().any(|(k, e)| k == "k2" && e.value == 20);
        assert!(has_k1);
        assert!(has_k2);
    }
}
