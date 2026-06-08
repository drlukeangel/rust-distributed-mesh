//! Trait surface consumed by `EntityCache<E, B, M>`.
//!
//! Native `async fn` on `BrokerRpc` / `MeshGossip` keeps the hot path
//! allocation-free. Static dispatch via the factory's generics avoids
//! object-safety problems with AFIT methods that are themselves generic.

use std::borrow::Cow;
use std::hash::Hash;
use std::time::Duration;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

pub enum TailScope {
    Global,                                  // single topic, no org sweep
    PerOrg { discover: Arc<dyn OrgDiscovery> }, // sweep over orgs, one tailer per org
}

pub trait OrgDiscovery: Send + Sync {
    fn current_org_ids(&self) -> Vec<u64>;
    fn subscribe_org_changes(&self) -> tokio::sync::broadcast::Receiver<OrgEvent>;
}

#[derive(Debug, Clone)]
pub enum OrgEvent { Added(u64), Removed(u64) }

pub type DecodeError = String;

pub enum TailerEvent<E> {
    Value(E),
    Tombstone,
    Skip,
}

pub trait TailerSideEffect<E: CachedEntity>: Send + Sync {
    fn before_apply(&self, _key: &E::Key, _value: Option<&E>) {}
    fn after_apply(&self, _key: &E::Key, _value: Option<&E>) {}
}

/// Provenance discriminator passed to `EntityCache::apply_update`.
///
/// Crucial for correctness:
/// * `Tail` is the **only** source allowed to advance the cache's global
///   `tail_offset` watermark.
/// * `Gossip` may arrive ahead of or behind tail; per-key offset CAS
///   resolves the race.
/// * `ReadThrough` populates demand-driven entries on Bounded caches and
///   bypasses stale-vacant checks.
/// * `LocalMutation` is the RYOW path applied immediately after the
///   broker confirms a write on the local node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateSource {
    Tail,
    Gossip,
    ReadThrough,
    LocalMutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheTier { Functional, Feature }

/// Marker for an entity type cached via `EntityCache<E, B, M>`.
///
/// Implementors describe the Stage 1 topic (compacted, single-partition),
/// broker RPC op-codes (u16 to avoid namespace exhaustion), wall-clock
/// retention horizon for zombie-tombstone prevention, and optional
/// boundedness / on-disk snapshot configuration.
pub trait CachedEntity: Serialize + DeserializeOwned + Send + Sync + 'static {
    fn cache_tier() -> CacheTier { CacheTier::Feature }

    type Key: Hash + Eq + Clone + Serialize + DeserializeOwned + ToString + Send + Sync + 'static;

    /// Compute the cache key for this entity. Returns owned so
    /// composite keys built from multiple fields (e.g. `(org_id, name)`)
    /// don't have to be stashed on the struct or threaded through
    /// `unsafe`. The factory's `insert` / `update` / `upsert` callers
    /// clone immediately, so the owned form is at-cost-parity with the
    /// prior borrow-then-clone shape.
    fn key(&self) -> Self::Key;

    /// Stage 1 compacted topic RRL. **MUST** be configured with `num_partitions: 1` —
    /// offsets are only monotonic per-partition and the offset CAS in
    /// `apply_update` relies on a single global ordering.
    ///
    /// `Cow<'static, str>` so per-org topic names (`rrl:<org>:...`) can be
    /// computed without allocating for the common static case.
    fn topic_path(org: Option<&str>) -> Cow<'static, str>;

    fn entity_kind() -> &'static str;

    /// Broker RPC op-codes. `put_op`/`delete_op` MUST return the
    /// broker-assigned offset for RYOW caching; `get_op` MUST return
    /// `(Option<E>, offset)` to allow negative caching at the topic's
    /// LATEST offset (anti-DDoS).
    fn put_op() -> u16;
    fn delete_op() -> u16;
    fn get_op() -> u16;
    fn snapshot_op() -> u16;

    /// Wall-clock horizon for tombstone validity. Snapshots older than
    /// this MUST be rejected on hot-restart to avoid zombie resurrections
    /// after broker compaction has dropped the relevant tombstones.
    fn delete_retention() -> Duration;

    /// `None` = Resident `DashMap` (security-critical, full-resident).
    /// `Some(N)` = Bounded `moka::future::Cache` (high-cardinality with
    /// W-TinyLFU eviction + request coalescing).
    fn max_capacity() -> Option<u64> {
        None
    }

    /// Optional on-disk snapshot path. When `Some`, hot-restart writes /
    /// reads `SnapshotDiskFormat<Self>` here; the wall-clock validity
    /// check in `Entity-Cache.md` §4.3 applies.
    fn snapshot_path() -> Option<&'static str> {
        None
    }

    /// Decode key bytes from broker/gossip frame.
    fn decode_key(bytes: &[u8]) -> Result<Self::Key, DecodeError>
    where
        Self::Key: DeserializeOwned,
    {
        postcard::from_bytes(bytes).map_err(|e| e.to_string())
    }

    /// Decode value bytes from broker/gossip frame.
    fn decode_value(bytes: &[u8]) -> Result<Self, DecodeError>
    where
        Self: DeserializeOwned,
    {
        postcard::from_bytes(bytes).map_err(|e| e.to_string())
    }

    /// Optional namespace override for tailer span emission.
    fn span_namespace() -> &'static str {
        Self::entity_kind()
    }

    /// Decode the full record (key + value bytes) into a TailerEvent.
    /// Default implementation uses empty `value_bytes` as a tombstone marker.
    fn decode_record(key_bytes: &[u8], value_bytes: &[u8]) -> Result<(Self::Key, TailerEvent<Self>), DecodeError> {
        let key = Self::decode_key(key_bytes)?;
        let event = if value_bytes.is_empty() {
            TailerEvent::Tombstone
        } else {
            TailerEvent::Value(Self::decode_value(value_bytes)?)
        };
        Ok((key, event))
    }

    /// Value-driven conflict resolver. Override to encode value-level
    /// CAS semantics that should win over offset-CAS — useful when the
    /// entity carries its own monotonic publisher field (e.g.
    /// `VtAssignment.epoch`, `HandoffSignal.from_epoch`) that the
    /// protocol guarantees is globally agreed across racing publishers.
    ///
    /// * `Some(Ordering::Greater)` → incoming wins over existing.
    /// * `Some(Ordering::Less)` → existing wins; incoming dropped.
    /// * `Some(Ordering::Equal)` or `None` (default) → fall through to
    ///   the factory's offset-CAS in `apply_update`.
    ///
    /// Default returns `None`, preserving the offset-only behavior for
    /// every existing impl. Caches WITH broker-stamped offsets keep
    /// the default; caches WITHOUT (where the publisher stamps a
    /// value-level monotonic field) override.
    ///
    /// **Order of precedence:** value resolver runs first; if it
    /// returns `None` or `Some(Equal)`, offset-CAS resolves the rest.
    /// This means an override CAN escalate "existing has lower offset
    /// but the value semantics say it wins" (e.g., an `Ack` beating a
    /// later-offset `Ready` on tied epoch). Implementors are
    /// responsible for making the resolver total order — a wrong
    /// `resolve_conflict` produces silent divergence, same risk class
    /// as a wrong `key()`.
    fn resolve_conflict(_existing: &Self, _incoming: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

pub struct BrokerRecord {
    pub payload: Vec<u8>,
    pub offset: u64,
}

/// Consumer-provided broker transport.
///
/// Generic over `E: CachedEntity` per-method so a single transport
/// implementation (e.g. `QuicBrokerRpc`) handles every cached entity
/// type by op-code dispatch + serde.
///
/// **Not object-safe.** Used statically through `EntityCache<E, B, M>`'s
/// `B` parameter — exactly the design that lets the trait stay native
/// `async fn` without `dyn`/Box overhead.
pub trait BrokerRpc: Send + Sync + 'static {
    fn next_record(
        &self,
        topic_path: &str,
    ) -> impl std::future::Future<Output = Result<BrokerRecord, String>> + Send;

    /// Writes through the Stage 1 topic. `expect_present = true` makes
    /// a CAS-style update; `false` is upsert. Returns the broker-assigned
    /// offset for RYOW.
    fn put_entity<E: CachedEntity>(
        &self,
        op: u16,
        entity: &E,
        expect_present: bool,
    ) -> impl std::future::Future<Output = Result<u64, String>> + Send;

    /// Tombstone-write through the Stage 1 topic. Returns the
    /// broker-assigned offset for RYOW.
    fn delete_entity<E: CachedEntity>(
        &self,
        op: u16,
        key: &E::Key,
    ) -> impl std::future::Future<Output = Result<u64, String>> + Send;
}

/// Consumer-provided mesh-gossip transport (Stage 4).
///
/// Generic over `E` so a single mesh implementation handles every
/// cached entity by op-code dispatch + serde, mirroring `BrokerRpc`.
pub trait MeshGossip: Send + Sync + 'static {
    /// Broadcast a per-key update stamped with the broker-assigned
    /// topic offset. Receivers apply via `EntityCache::apply_update`
    /// with `UpdateSource::Gossip`. `data = None` is a tombstone.
    fn broadcast_update<E: CachedEntity>(
        &self,
        key: &E::Key,
        offset: u64,
        data: Option<&E>,
    ) -> impl std::future::Future<Output = ()> + Send;
}
