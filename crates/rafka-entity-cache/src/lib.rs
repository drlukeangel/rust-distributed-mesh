//! # rafka-entity-cache
//!
//! Multi-stage RAM cache factory for Rafka entity state. See
//! `docs/architecture/Entity-Cache.md` for the full pattern, including the
//! five stages (durable topic, broker primitive, node hot cache, mesh
//! gossip), entity / node lifecycle events, and acceptance criteria for
//! net-new caches.
//!
//! The factory is `EntityCache<E: CachedEntity, B: BrokerRpc, M: MeshGossip>`,
//! statically dispatched so consumer-provided transport traits can use
//! native `async fn` without boxing or `dyn`-object-safety contortions.
//!
//! Phase 1 (this crate): factory, traits, error, types. No migrations.
//! Subsequent sprints migrate Gateway, Compute, and Broker caches per
//! `Entity-Cache.md` §9.

pub mod error;
pub mod factory;
pub mod health;
pub mod traits;
pub mod types;

pub use error::CacheError;
pub use factory::{EntityCache, SnapshotChunk, SAFE_REORDER_WINDOW};
pub use health::{CacheEvent, cache_event_sender, subscribe_cache_events};
pub use traits::{BrokerRpc, CacheTier, CachedEntity, MeshGossip, UpdateSource};
pub use types::{CacheBackend, SnapshotDiskFormat, VersionedValue};
