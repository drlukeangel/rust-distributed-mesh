//! Shared value-types: `VersionedValue`, `SnapshotDiskFormat`, `CacheBackend`.

use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::traits::CachedEntity;

/// Per-key cache slot.
///
/// `data: None` is an explicit tombstone — *never* physically removed
/// from the cache by `apply_update`. Tombstones are pruned by the
/// `O(log N)` background reaper after the offset crosses
/// `tail_offset - SAFE_REORDER_WINDOW`. Physical removal would forget
/// the offset watermark and let a delayed `Put` resurrect the entity
/// (Ghost Resurrection).
#[derive(Serialize, Deserialize)]
pub struct VersionedValue<E> {
    pub offset: u64,
    pub data: Option<Arc<E>>,
}

// Manual Clone — `Arc<E>` clones regardless of `E: Clone`, so we
// don't want `derive(Clone)` to over-constrain the generic. moka's
// cache requires `V: Clone` and would otherwise reject any `E` that
// isn't itself Clone (which most entity records are not).
impl<E> Clone for VersionedValue<E> {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset,
            data: self.data.clone(),
        }
    }
}

/// Hot-restart snapshot wire format.
///
/// `timestamp_secs` is wall-clock UTC. On hot-restart the node MUST
/// reject the snapshot if `now() - timestamp_secs > delete_retention()`,
/// because Kafka's `low_water_mark` does not reliably advance during
/// log compaction — only wall-clock can prove the snapshot still
/// reflects all tombstones the broker still retains.
#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "E::Key: Serialize, E: Serialize",
    deserialize = "E::Key: serde::de::DeserializeOwned, E: serde::de::DeserializeOwned",
))]
pub struct SnapshotDiskFormat<E: CachedEntity> {
    pub timestamp_secs: u64,
    pub snapshot_offset: u64,
    pub entries: Vec<(E::Key, VersionedValue<E>)>,
}

/// Storage backend selector.
///
/// `Resident`: `DashMap`, no eviction. Used for security-critical
/// caches that must hold every entity (e.g. compiled ACLs).
///
/// `Bounded`: `moka::future::Cache` with W-TinyLFU eviction and request
/// coalescing via `try_get_with_by_ref`. Used for high-cardinality
/// caches where the working set is much smaller than the global set
/// (e.g. webhook callback registry).
pub enum CacheBackend<E: CachedEntity> {
    Resident(Arc<DashMap<E::Key, VersionedValue<E>>>),
    Bounded(Arc<moka::future::Cache<E::Key, VersionedValue<E>>>),
}

impl<E: CachedEntity> Clone for CacheBackend<E> {
    fn clone(&self) -> Self {
        match self {
            Self::Resident(m) => Self::Resident(m.clone()),
            Self::Bounded(m) => Self::Bounded(m.clone()),
        }
    }
}
