// Generic per-node cache engine.
//
// Every node can own one or more named caches, each governed by a CacheType
// write model. Caches on a DEDICATED channel subscribe to an iroh-gossip topic
// keyed by blake3("cache:" + name) and publish/receive updates on that topic.
// The KeyGossip type rides `main` via live_digests() — no dedicated channel.
//
// Public API (used by run_node + admin-ui):
//   - node_caches() -> &'static Arc<DashMap<String, NodeCache>>
//   - channel_events() -> &'static Arc<DashMap<String, ChannelEventRing>>
//   - parse_cache_specs_from_env() -> Vec<CacheSpec>
//   - run_cache_task(...)  — spawned per cache inside run_node
//   - run_keygossip_fill() — spawned for the KeyGossip projection

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Write-model for a cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheType {
    /// Epoch-monotonic last-write-wins across ANY publisher.
    /// Accept iff incoming epoch > existing epoch.
    Shared,
    /// Only the LEADER may mutate entries.
    /// Accept iff publisher == leader (see NodeCache.leader field).
    Leader,
    /// Each node writes only its own row (key == publisher).
    /// Force key = publisher; ignore announced key.
    Key,
    /// Same as Key but fed from the main membership gossip (live_digests).
    /// NOT subscribed on a dedicated channel — rides `main`.
    KeyGossip,
}

/// Which gossip channel a cache is published on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Channel {
    /// Rides the main mesh gossip topic (KeyGossip only).
    Main,
    /// Dedicated iroh-gossip topic: blake3("cache:" + name).
    Dedicated(String),
}

impl Channel {
    pub fn name(&self) -> &str {
        match self {
            Channel::Main => "main",
            Channel::Dedicated(s) => s.as_str(),
        }
    }
}

/// Static description of a cache: immutable after creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSpec {
    pub name: String,
    pub cache_type: CacheType,
    pub channel: Channel,
    /// Optional per-cache durability. `false` = Ephemeral (rebuilt from birth +
    /// gossip on restart). `true` = Disk (snapshot to file + reload on boot,
    /// before gossip). In v2-mesh this maps to Topic-backed; here (no topics)
    /// disk IS the durable backing.
    pub durable: bool,
}

/// One entry in a cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub value: u64,
    pub epoch: u64,
    pub publisher: String,
    pub updated_ms: u64,
}

/// Result of trying to apply an incoming update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResult {
    Accepted,
    Rejected,
}

/// A named cache with its spec and a concurrent entry map.
pub struct NodeCache {
    pub spec: CacheSpec,
    /// The leader node_id for Leader-type caches. Set at cache creation.
    pub leader: Option<String>,
    entries: DashMap<String, CacheEntry>,
    /// Counts updates rejected by this cache's write model.
    rejected_count: std::sync::atomic::AtomicU64,
}

impl NodeCache {
    pub fn new(spec: CacheSpec) -> Self {
        NodeCache {
            spec,
            leader: None,
            entries: DashMap::new(),
            rejected_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn with_leader(mut self, leader: impl Into<String>) -> Self {
        self.leader = Some(leader.into());
        self
    }

    /// Apply an incoming update according to this cache's write model.
    /// Returns Accepted if the entry was written, Rejected otherwise.
    pub fn apply(&self, publisher: &str, key: &str, value: u64, epoch: u64) -> ApplyResult {
        let now_ms = now_ms();
        // Decide per the write model: (accepted, reject_reason, stored_key).
        let (accepted, reason, stored_key): (bool, &'static str, String) = match self.spec.cache_type {
            CacheType::Shared => {
                // Accept iff epoch > existing epoch (monotonic last-write-wins).
                let ok = match self.entries.get(key) {
                    Some(existing) => epoch > existing.epoch,
                    None => true,
                };
                (ok, if ok { "" } else { "stale-epoch" }, key.to_string())
            }
            CacheType::Leader => {
                // Accept iff publisher == leader.
                let ok = matches!(&self.leader, Some(l) if publisher == l);
                (ok, if ok { "" } else { "not-leader" }, key.to_string())
            }
            CacheType::Key | CacheType::KeyGossip => {
                // Force key = publisher; each node writes only its own row.
                (true, "", publisher.to_string())
            }
        };
        let type_str = match self.spec.cache_type {
            CacheType::Shared => "shared",
            CacheType::Leader => "leader",
            CacheType::Key => "key",
            CacheType::KeyGossip => "key-gossip",
        };
        if accepted {
            self.entries.insert(stored_key.clone(), CacheEntry {
                value,
                epoch,
                publisher: publisher.to_string(),
                updated_ms: now_ms,
            });
            tracing::info_span!(
                "rafka.cache.apply",
                cache = %self.spec.name,
                cache_type = %type_str,
                channel = %self.spec.channel.name(),
                publisher = %publisher,
                key = %stored_key,
                epoch = epoch,
                result = "accepted",
                "otel.kind" = "internal",
            )
            .in_scope(|| {});
            ApplyResult::Accepted
        } else {
            self.rejected_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // The observable proof of write-model enforcement.
            tracing::info_span!(
                "rafka.cache.reject",
                cache = %self.spec.name,
                cache_type = %type_str,
                publisher = %publisher,
                reason = %reason,
                "otel.kind" = "internal",
            )
            .in_scope(|| {
                tracing::info!(cache = %self.spec.name, reason = %reason, "cache update rejected by write model");
            });
            ApplyResult::Rejected
        }
    }

    /// All entries as a vec of (key, entry) for serialization.
    pub fn all_entries(&self) -> Vec<(String, CacheEntry)> {
        self.entries.iter().map(|e| (e.key().clone(), e.value().clone())).collect()
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn rejected_count(&self) -> u64 {
        self.rejected_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Distinct publishers currently in this cache.
    pub fn distinct_publishers(&self) -> Vec<String> {
        let mut pubs: Vec<String> = self.entries.iter()
            .map(|e| e.value().publisher.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        pubs.sort();
        pubs
    }

    /// Snapshot all entries for birth-hydration.
    pub fn snapshot(&self) -> Vec<(String, CacheEntry)> {
        self.all_entries()
    }

    /// Hydrate from a snapshot (from another node's snapshot).
    pub fn hydrate_from(&self, snapshot: Vec<(String, CacheEntry)>) {
        for (key, entry) in snapshot {
            self.apply(&entry.publisher, &key, entry.value, entry.epoch);
        }
    }

    /// Disk path for a durable cache's snapshot (under RAFKA_DATA_DIR).
    pub fn disk_path(cache_name: &str) -> std::path::PathBuf {
        let dir = std::env::var("RAFKA_DATA_DIR").unwrap_or_else(|_| ".".to_string());
        std::path::Path::new(&dir).join(format!("cache-{}.postcard", cache_name))
    }

    /// Persist all entries to disk (no-op unless `spec.durable`).
    pub fn persist_to_disk(&self) {
        if !self.spec.durable {
            return;
        }
        match postcard::to_allocvec(&self.all_entries()) {
            Ok(bytes) => {
                let path = Self::disk_path(&self.spec.name);
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let _ = std::fs::write(path, bytes);
            }
            Err(e) => eprintln!("[node_cache] persist {} failed: {}", self.spec.name, e),
        }
    }

    /// Load entries from disk on boot (no-op unless `spec.durable`). Returns count loaded.
    pub fn load_from_disk(&self) -> usize {
        if !self.spec.durable {
            return 0;
        }
        let path = Self::disk_path(&self.spec.name);
        let Ok(bytes) = std::fs::read(&path) else { return 0; };
        let Ok(snap) = postcard::from_bytes::<Vec<(String, CacheEntry)>>(&bytes) else { return 0; };
        let n = snap.len();
        self.hydrate_from(snap);
        n
    }
}

// ---------------------------------------------------------------------------
// Per-channel event ring (for /api/channels)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelEvent {
    pub ts_ms: u64,
    pub publisher: String,
    pub op: String,
    pub key: String,
    pub value: u64,
    pub epoch: u64,
    pub cache_name: String,
}

const CHANNEL_RING_CAP: usize = 40;

pub struct ChannelEventRing {
    pub channel_name: String,
    events: std::sync::Mutex<std::collections::VecDeque<ChannelEvent>>,
}

impl ChannelEventRing {
    pub fn new(channel_name: impl Into<String>) -> Self {
        ChannelEventRing {
            channel_name: channel_name.into(),
            events: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(CHANNEL_RING_CAP)),
        }
    }

    pub fn push(&self, evt: ChannelEvent) {
        let mut g = self.events.lock().unwrap();
        if g.len() >= CHANNEL_RING_CAP {
            g.pop_front();
        }
        g.push_back(evt);
    }

    pub fn snapshot(&self) -> Vec<ChannelEvent> {
        self.events.lock().unwrap().iter().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Process-global stores
// ---------------------------------------------------------------------------

static NODE_CACHES: OnceLock<Arc<DashMap<String, NodeCache>>> = OnceLock::new();

/// Process-global cache registry: name → NodeCache.
/// Admin reads this directly for API responses.
pub fn node_caches() -> &'static Arc<DashMap<String, NodeCache>> {
    NODE_CACHES.get_or_init(|| Arc::new(DashMap::new()))
}

static CHANNEL_EVENTS: OnceLock<Arc<DashMap<String, ChannelEventRing>>> = OnceLock::new();

/// Process-global per-channel event rings: channel_name → ring.
pub fn channel_events() -> &'static Arc<DashMap<String, ChannelEventRing>> {
    CHANNEL_EVENTS.get_or_init(|| Arc::new(DashMap::new()))
}

// ---------------------------------------------------------------------------
// Wire format for gossip messages
// ---------------------------------------------------------------------------

/// The message broadcast on a cache's dedicated gossip channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheGossipMsg {
    pub cache_name: String,
    pub publisher: String,
    pub key: String,
    pub value: u64,
    pub epoch: u64,
}

// ---------------------------------------------------------------------------
// Env-var spec parser: RAFKA_NODE_CACHES
//
// Format: <name>:<type>:<channel>[,<name>:<type>:<channel>,...]
//   type: shared | leader | key | keygossip
//   channel: main | dedicated
// ---------------------------------------------------------------------------

pub fn parse_cache_specs_from_env() -> Vec<CacheSpec> {
    let raw = std::env::var("RAFKA_NODE_CACHES").unwrap_or_default();
    let raw = raw.trim();
    if raw.is_empty() {
        return Vec::new();
    }
    raw.split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() { return None; }
            let parts: Vec<&str> = s.splitn(4, ':').collect();
            if parts.len() < 3 {
                eprintln!("RAFKA_NODE_CACHES: invalid spec {:?} (expected name:type:channel[:disk])", s);
                return None;
            }
            // Optional 4th field: "disk" → durable; absent/anything else → ephemeral.
            let durable = parts.get(3).map(|d| d.trim() == "disk").unwrap_or(false);
            let name = parts[0].trim().to_string();
            let cache_type = match parts[1].trim() {
                "shared" => CacheType::Shared,
                "leader" => CacheType::Leader,
                "key" => CacheType::Key,
                "keygossip" => CacheType::KeyGossip,
                other => {
                    eprintln!("RAFKA_NODE_CACHES: unknown cache_type {:?}", other);
                    return None;
                }
            };
            let channel = match parts[2].trim() {
                "main" => Channel::Main,
                "dedicated" => Channel::Dedicated(name.clone()),
                other => {
                    eprintln!("RAFKA_NODE_CACHES: unknown channel {:?}", other);
                    return None;
                }
            };
            Some(CacheSpec { name, cache_type, channel, durable })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Per-cache gossip task
//
// For each dedicated-channel cache:
//   - Subscribe to gossip topic blake3("cache:" + name)
//   - Join peers from the shared peer_registry (same pattern as run_gossip)
//   - If publisher=true: ~1-1.5s chatter loop mutates value and broadcasts
//   - Always: receive + apply incoming messages
// ---------------------------------------------------------------------------

/// Type alias matching PeerRegistry in lib.rs.
pub type PeerRegistry = Arc<DashMap<String, iroh::endpoint::Connection>>;

pub async fn run_cache_task(
    gossip: iroh_gossip::net::Gossip,
    cache_name: String,
    publisher: bool,   // true = this node is an owner and should publish
    publisher_id: String,  // node_id or node_name used as publisher key
    peer_registry: PeerRegistry,
    leader: Option<String>,
) {
    let topic_key = format!("cache:{}", cache_name);
    let topic_bytes: [u8; 32] = *blake3::hash(topic_key.as_bytes()).as_bytes();
    let topic_id = iroh_gossip::proto::TopicId::from_bytes(topic_bytes);

    use futures_lite::StreamExt;
    use iroh_gossip::api::Event;

    // Subscribe with empty bootstrap — the join_peers loop seeds the swarm
    // from the shared peer_registry (same proven pattern as run_gossip).
    let topic = match gossip.subscribe(topic_id, Vec::new()).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("[node_cache] subscribe failed for cache {}: {}", cache_name, e);
            return;
        }
    };
    let (sender, mut receiver) = topic.split();

    // Ensure the NodeCache + ChannelEventRing exist in the globals.
    {
        let caches = node_caches();
        if !caches.contains_key(&cache_name) {
            eprintln!("[node_cache] WARN: cache {} not in node_caches() at task start — was it registered?", cache_name);
        }
        let events = channel_events();
        events.entry(format!("cache:{}", cache_name)).or_insert_with(|| {
            ChannelEventRing::new(format!("cache:{}", cache_name))
        });
    }

    // Set the leader on this cache if provided.
    if let Some(ref l) = leader {
        let caches = node_caches();
        if let Some(mut entry) = caches.get_mut(&cache_name) {
            entry.leader = Some(l.clone());
        }
    }

    let mut joined_peers: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(1200));
    let mut chatter_tick: u64 = 0;
    let mut epoch: u64 = now_ms(); // use time as initial epoch to minimize collisions
    // Shared caches must RACE on a shared keyset so the epoch resolver actually
    // fires (and losers emit rafka.cache.reject reason=stale-epoch). Key/Leader
    // caches keep key == publisher.
    let is_shared = node_caches()
        .get(&cache_name)
        .map(|c| matches!(c.spec.cache_type, CacheType::Shared))
        .unwrap_or(false);

    // Durable caches: hydrate from disk BEFORE gossip, so a restarted node has
    // its persisted state at t=0 (no-op for ephemeral caches).
    if let Some(cache) = node_caches().get(&cache_name) {
        let n = cache.load_from_disk();
        if n > 0 {
            tracing::info_span!(
                "rafka.cache.hydrate.from-disk",
                cache = %cache_name,
                loaded = n as i64,
                "otel.kind" = "internal",
            )
            .in_scope(|| {
                tracing::info!(cache = %cache_name, loaded = n, "durable cache hydrated from disk (pre-gossip)");
            });
        }
    }

    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Persist durable caches each tick (no-op for ephemeral).
                if let Some(cache) = node_caches().get(&cache_name) {
                    cache.persist_to_disk();
                }
                // Join any new peers from the shared QUIC registry.
                let mut new_peers = Vec::new();
                for peer in peer_registry.iter() {
                    if !joined_peers.contains(peer.key()) {
                        if let Ok(id) = peer.key().parse::<iroh::PublicKey>() {
                            new_peers.push(iroh::EndpointId::from(id));
                            joined_peers.insert(peer.key().clone());
                        }
                    }
                }
                // Remove peers that dropped off the registry.
                joined_peers.retain(|p| peer_registry.contains_key(p));
                if !new_peers.is_empty() {
                    let _ = sender.join_peers(new_peers).await;
                }

                if !publisher { continue; }

                // Chatter: alternate ~1000/1500ms by tick parity.
                chatter_tick = chatter_tick.wrapping_add(1);
                epoch = epoch.wrapping_add(1);

                let value: u64 = (now_ms() % 10000) + chatter_tick * 7;
                let key = if is_shared {
                    // Publishers race on a small shared keyset → epoch resolves;
                    // the loser's apply() emits rafka.cache.reject (stale-epoch).
                    const SHARED_KEYS: [&str; 3] = ["row-a", "row-b", "row-c"];
                    SHARED_KEYS[(chatter_tick as usize) % SHARED_KEYS.len()].to_string()
                } else {
                    publisher_id.clone() // Key/Leader/KeyGossip: key == publisher
                };

                let msg = CacheGossipMsg {
                    cache_name: cache_name.clone(),
                    publisher: publisher_id.clone(),
                    key: key.clone(),
                    value,
                    epoch,
                };

                match postcard::to_allocvec(&msg) {
                    Ok(bytes) => {
                        let _ = sender.broadcast(bytes.into()).await;
                        tracing::info_span!(
                            "rafka.cache.publish",
                            cache = %cache_name,
                            channel = %cache_name,
                            key = %key,
                            epoch = epoch,
                            "otel.kind" = "internal",
                        )
                        .in_scope(|| {});
                        // Apply to our own cache (self-write).
                        let caches = node_caches();
                        if let Some(cache) = caches.get(&cache_name) {
                            cache.apply(&publisher_id, &key, value, epoch);
                        }
                        // Log to channel ring.
                        let events = channel_events();
                        if let Some(ring) = events.get(&format!("cache:{}", cache_name)) {
                            ring.push(ChannelEvent {
                                ts_ms: now_ms(),
                                publisher: publisher_id.clone(),
                                op: "publish".to_string(),
                                key: key.clone(),
                                value,
                                epoch,
                                cache_name: cache_name.clone(),
                            });
                        }
                    }
                    Err(e) => {
                        eprintln!("[node_cache] encode failed for cache {}: {}", cache_name, e);
                    }
                }
            }

            event = receiver.next() => {
                let Some(event) = event else { break };
                let event = match event {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::trace!("cache {} gossip receive error: {}", cache_name, e);
                        continue;
                    }
                };
                if let Event::Received(msg) = event {
                    let Ok(gossip_msg) = postcard::from_bytes::<CacheGossipMsg>(&msg.content) else {
                        continue;
                    };
                    // Only apply if the message is for this cache.
                    if gossip_msg.cache_name != cache_name {
                        continue;
                    }
                    let caches = node_caches();
                    if let Some(cache) = caches.get(&cache_name) {
                        let result = cache.apply(&gossip_msg.publisher, &gossip_msg.key, gossip_msg.value, gossip_msg.epoch);
                        tracing::info_span!(
                            "rafka.cache.receive",
                            cache = %cache_name,
                            channel = %cache_name,
                            publisher = %gossip_msg.publisher,
                            "otel.kind" = "internal",
                        )
                        .in_scope(|| {});
                        // Log to channel ring.
                        let events = channel_events();
                        if let Some(ring) = events.get(&format!("cache:{}", cache_name)) {
                            ring.push(ChannelEvent {
                                ts_ms: now_ms(),
                                publisher: gossip_msg.publisher.clone(),
                                op: if result == ApplyResult::Accepted { "received" } else { "rejected" }.to_string(),
                                key: gossip_msg.key.clone(),
                                value: gossip_msg.value,
                                epoch: gossip_msg.epoch,
                                cache_name: cache_name.clone(),
                            });
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// KeyGossip fill task — rides main gossip, no dedicated channel
// ---------------------------------------------------------------------------

/// Periodically projects live_digests() into the "topology-key-gossip" cache.
/// Each live node's entry uses node_id as both key and publisher.
pub async fn run_keygossip_fill(cache_name: String) {
    // Ensure the cache exists.
    {
        let caches = node_caches();
        if !caches.contains_key(&cache_name) {
            eprintln!("[node_cache] WARN: keygossip cache {} not registered", cache_name);
        }
    }

    let mut interval = tokio::time::interval(Duration::from_millis(1500));
    loop {
        interval.tick().await;
        let caches = node_caches();
        if let Some(cache) = caches.get(&cache_name) {
            // Project live_digests into the KeyGossip cache.
            let digests = crate::live_digests();
            for entry in digests.iter() {
                let d = entry.value();
                let epoch = d.wall_time_ms;
                cache.apply(&d.node_id, &d.node_id, epoch % 100_000, epoch);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Unit tests — the engine's write-model logic (apply), hydration, durability.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(name: &str, ty: CacheType, durable: bool) -> NodeCache {
        NodeCache::new(CacheSpec {
            name: name.to_string(),
            cache_type: ty,
            channel: Channel::Dedicated(name.to_string()),
            durable,
        })
    }

    #[test]
    fn key_forces_key_to_publisher() {
        let c = cache("k", CacheType::Key, false);
        // announce a DIFFERENT key; the engine must store under the publisher.
        assert_eq!(c.apply("nodeA", "ANNOUNCED", 1, 100), ApplyResult::Accepted);
        let entries = c.all_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "nodeA"); // key == publisher, not "ANNOUNCED"
    }

    #[test]
    fn shared_higher_accepted_lower_and_equal_rejected() {
        let c = cache("s", CacheType::Shared, false);
        assert_eq!(c.apply("A", "row", 5, 100), ApplyResult::Accepted);
        assert_eq!(c.apply("B", "row", 3, 99), ApplyResult::Rejected); // lower epoch
        assert_eq!(c.apply("B", "row", 3, 100), ApplyResult::Rejected); // equal epoch
        assert_eq!(c.apply("B", "row", 9, 101), ApplyResult::Accepted); // higher epoch
        assert_eq!(c.rejected_count(), 2);
    }

    #[test]
    fn leader_only_leader_accepted_others_rejected() {
        let c = cache("l", CacheType::Leader, false).with_leader("LEADER");
        assert_eq!(c.apply("LEADER", "x", 1, 1), ApplyResult::Accepted);
        assert_eq!(c.apply("rogue", "x", 1, 2), ApplyResult::Rejected);
        assert_eq!(c.rejected_count(), 1);
    }

    #[test]
    fn leader_with_no_leader_rejects_all() {
        let c = cache("l", CacheType::Leader, false); // no leader configured
        assert_eq!(c.apply("anyone", "x", 1, 1), ApplyResult::Rejected);
    }

    #[test]
    fn birth_hydration_roundtrip() {
        let src = cache("s", CacheType::Key, false);
        src.apply("A", "A", 7, 10);
        src.apply("B", "B", 8, 11);
        let snap = src.snapshot();
        let dst = cache("s", CacheType::Key, false);
        dst.hydrate_from(snap);
        assert_eq!(dst.entry_count(), 2);
    }

    #[test]
    fn durable_persist_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("rafka-cache-test-{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RAFKA_DATA_DIR", &dir);
        let spec = CacheSpec {
            name: "dur".to_string(),
            cache_type: CacheType::Key,
            channel: Channel::Dedicated("dur".to_string()),
            durable: true,
        };
        let c = NodeCache::new(spec.clone());
        c.apply("A", "A", 42, 100);
        c.persist_to_disk();
        // A fresh cache with the same spec reloads the persisted entry.
        let c2 = NodeCache::new(spec);
        assert_eq!(c2.load_from_disk(), 1);
        assert_eq!(c2.entry_count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ephemeral_persist_load_are_noops() {
        let c = cache("eph", CacheType::Key, false); // durable = false
        c.apply("A", "A", 1, 1);
        c.persist_to_disk(); // no-op
        assert_eq!(c.load_from_disk(), 0); // no-op
    }
}
