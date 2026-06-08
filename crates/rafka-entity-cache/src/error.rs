//! Failure modes for the entity-cache factory.
//!
//! REST middleware translates these into HTTP responses
//! (e.g. `NotReady` → 503 + Retry-After). See
//! `docs/architecture/Entity-Cache.md` §6.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    /// Mutating insert collided with an existing entry. REST → 409.
    #[error("entity already exists at key")]
    Conflict,

    /// Mutating update or delete addressed a non-existent key. REST → 404.
    #[error("entity not found at key")]
    NotFound,

    /// Cache is still booting; retry after hydration. REST → 503 + Retry-After.
    /// On `Ordering::Acquire` load of state, a value of 0 indicates Booting.
    #[error("cache not ready (booting); retry after hydration")]
    NotReady,

    /// Underlying broker RPC failed (network, decode, etc).
    #[error("broker RPC failed: {0}")]
    BrokerRpc(String),

    /// Snapshot wall-clock age exceeds the entity's `delete_retention()`.
    /// Hot restart MUST fall back to cold boot to avoid zombie tombstones.
    #[error("snapshot age {age_secs}s exceeds retention {retention_secs}s; cold boot required")]
    SnapshotStale { age_secs: u64, retention_secs: u64 },

    /// `read_local` was called on a Bounded backend that may need to issue
    /// a broker RPC on miss; caller must use `read_or_fetch` instead.
    #[error("bounded cache requires async read-through; call read_or_fetch instead of read_local")]
    RequiresAsyncReadThrough,

    /// `stream_snapshot` was called on a Bounded backend. Bounded caches
    /// hold partial post-eviction state; warm-boot receivers must cold-boot.
    #[error("bounded caches cannot serve as snapshot donors (fragmented post-eviction state)")]
    BoundedSnapshotForbidden,
}
