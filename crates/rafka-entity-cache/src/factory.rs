//! `EntityCache<E, B, M>` — the factory.
//!
//! See `docs/architecture/Entity-Cache.md` §6 for the full design.
//!
//! Phase 1 (this crate) ships:
//! * `new` — constructs a Booting cache with a chosen `CacheBackend` and
//!   the consumer's `BrokerRpc` / `MeshGossip` transports.
//! * `mark_ready` — the hydration code path flips the gate when boot finishes.
//! * `read_local` / `read_or_fetch` — fast-path reads.
//! * `insert` / `update` / `upsert` / `delete` — write-through with detached
//!   RYOW spawn for cancellation safety.
//! * `apply_update` — the core conflict-resolution kernel (offset CAS,
//!   tombstone preservation, ghost-resurrection barrier).
//! * `stream_snapshot` — Resident-only donor stream with `spawn_blocking`
//!   key extraction.
//! * `reap_old_tombstones` — `O(log N)` tombstone pruning via
//!   `BTreeMap::split_off`.
//!
//! Hydration (cold / warm / hot-restart per Entity-Cache.md §4) is left to
//! the migration sprints: they wire the broker tail-subscription stream
//! into `apply_update(_, _, _, UpdateSource::Tail)` and call
//! `mark_ready` once the offset reaches the broker's latest committed.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use async_stream::stream;
use dashmap::DashMap;
use futures::Stream;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use tokio::task::JoinHandle;

use crate::error::CacheError;
use crate::traits::{BrokerRpc, CachedEntity, MeshGossip, UpdateSource, TailScope, TailerSideEffect};use crate::types::{CacheBackend, VersionedValue};

use std::sync::LazyLock;
use tokio::time::Instant;

static DEGRADED_DEBOUNCE: LazyLock<dashmap::DashMap<&'static str, Instant>> = LazyLock::new(dashmap::DashMap::new);

/// Window of offsets behind the global tail watermark within which a
/// vacant-key insert is still considered safe (i.e. not a delayed
/// resurrection of an already-reaped tombstone). Tuning this trades
/// reaper memory for tolerance to mesh-reorder pathologies.
pub const SAFE_REORDER_WINDOW: u64 = 100_000;

/// One element of a warm-boot snapshot stream. Receivers apply via
/// `apply_update(_, _, _, UpdateSource::Tail)` to seed the cache, then
/// open a tail subscription starting from `donor_offset` to drain the
/// delta. See `Entity-Cache.md` §4.2.
#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "E::Key: Serialize, E: Serialize",
    deserialize = "E::Key: serde::de::DeserializeOwned, E: serde::de::DeserializeOwned",
))]
pub struct SnapshotChunk<E: CachedEntity> {
    pub donor_offset: u64,
    pub entries: Vec<(E::Key, VersionedValue<E>)>,
}

/// Internal Booting/Ready/Draining gate.
///
/// `0 = Booting`, `1 = Ready`, `2 = Draining`. Acquire/Release ensures
/// the hydration writes are visible on ARM64 before a reader sees Ready.
const STATE_BOOTING: u8 = 0;
const STATE_READY: u8 = 1;
#[allow(dead_code)]
const STATE_DRAINING: u8 = 2;

pub struct EntityCache<E: CachedEntity, B: BrokerRpc, M: MeshGossip> {
    cache: CacheBackend<E>,

    /// Strict chronological tombstone registry, keyed by broker offset.
    /// `BTreeMap::split_off` gives `O(log N)` reaper sweeps without
    /// scanning the cache. `std::sync::Mutex` is fine — no `.await`
    /// while the guard is held.
    tombstone_registry: Arc<Mutex<BTreeMap<u64, Vec<E::Key>>>>,

    /// Global durable watermark. Advanced ONLY by `UpdateSource::Tail`.
    /// Read with `Acquire`; written with `Release`. Used by
    /// `stream_snapshot` to pin a donor offset, and by the ghost-
    /// resurrection barrier in `apply_update`.
    tail_offset: Arc<AtomicU64>,

    topic_path: String,
    broker_rpc: Arc<B>,
    mesh: Arc<M>,

    state: Arc<AtomicU8>,
}

impl<E: CachedEntity, B: BrokerRpc, M: MeshGossip> Clone for EntityCache<E, B, M> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            tombstone_registry: self.tombstone_registry.clone(),
            tail_offset: self.tail_offset.clone(),
            topic_path: self.topic_path.clone(),
            broker_rpc: self.broker_rpc.clone(),
            mesh: self.mesh.clone(),
            state: self.state.clone(),
        }
    }
}

macro_rules! tailer_span {
    ($state:expr, $ns:expr, $kind:expr, $offset:expr) => {
        match ($state, $ns) {
            ("starting", "compiled_acl") => tracing::info_span!("rafka.gateway.compiled_acl.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "compiled_acl") => tracing::info_span!("rafka.gateway.compiled_acl.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "compiled_acl") => tracing::info_span!("rafka.gateway.compiled_acl.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "service_account") | ("starting", "service_account_credential") => tracing::info_span!("rafka.gateway.service_account.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "service_account") | ("upserted", "service_account_credential") => tracing::info_span!("rafka.gateway.service_account.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "service_account") | ("decode_failed", "service_account_credential") => tracing::info_span!("rafka.gateway.service_account.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "topic_ownership") => tracing::info_span!("rafka.gateway.topic_ownership.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "topic_ownership") => tracing::info_span!("rafka.gateway.topic_ownership.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "topic_ownership") => tracing::info_span!("rafka.gateway.topic_ownership.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "schema_binding") | ("starting", "schema_bindings") => tracing::info_span!("rafka.gateway.schema_bindings.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "schema_binding") | ("upserted", "schema_bindings") => tracing::info_span!("rafka.gateway.schema_bindings.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "schema_binding") | ("decode_failed", "schema_bindings") => tracing::info_span!("rafka.gateway.schema_bindings.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "callback_entry") | ("starting", "callbacks") => tracing::info_span!("rafka.gateway.callbacks.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "callback_entry") | ("upserted", "callbacks") => tracing::info_span!("rafka.gateway.callbacks.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "callback_entry") | ("decode_failed", "callbacks") => tracing::info_span!("rafka.gateway.callbacks.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "principal_scope") => tracing::info_span!("rafka.gateway.principal_scope.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "principal_scope") => tracing::info_span!("rafka.gateway.principal_scope.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "principal_scope") => tracing::info_span!("rafka.gateway.principal_scope.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "auth.jti_denylist") => tracing::info_span!("rafka.gateway.auth.jti_denylist.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "auth.jti_denylist") => tracing::info_span!("rafka.gateway.auth.jti_denylist.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "auth.jti_denylist") => tracing::info_span!("rafka.gateway.auth.jti_denylist.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "auth.refresh_token") => tracing::info_span!("rafka.gateway.auth.refresh_token.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "auth.refresh_token") => tracing::info_span!("rafka.gateway.auth.refresh_token.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "auth.refresh_token") => tracing::info_span!("rafka.gateway.auth.refresh_token.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "job_record") | ("starting", "jobs") => tracing::info_span!("rafka.gateway.jobs.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "job_record") | ("upserted", "jobs") => tracing::info_span!("rafka.gateway.jobs.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "job_record") | ("decode_failed", "jobs") => tracing::info_span!("rafka.gateway.jobs.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "jobs_failed_record") => tracing::info_span!("rafka.gateway.jobs_failed_record.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "jobs_failed_record") => tracing::info_span!("rafka.gateway.jobs_failed_record.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "jobs_failed_record") => tracing::info_span!("rafka.gateway.jobs_failed_record.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "iam_group") => tracing::info_span!("rafka.gateway.iam_group.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "iam_group") => tracing::info_span!("rafka.gateway.iam_group.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "iam_group") => tracing::info_span!("rafka.gateway.iam_group.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "user_credential") | ("starting", "user") => tracing::info_span!("rafka.gateway.user.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "user_credential") | ("upserted", "user") => tracing::info_span!("rafka.gateway.user.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "user_credential") | ("decode_failed", "user") => tracing::info_span!("rafka.gateway.user.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "slug_mapping") | ("starting", "rrl.slug_registry") => tracing::info_span!("rafka.gateway.rrl.slug_registry.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "slug_mapping") | ("upserted", "rrl.slug_registry") => tracing::info_span!("rafka.gateway.rrl.slug_registry.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "slug_mapping") | ("decode_failed", "rrl.slug_registry") => tracing::info_span!("rafka.gateway.rrl.slug_registry.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", "entity_map") | ("starting", "entity_map_entry") => tracing::info_span!("rafka.gateway.entity_map.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", "entity_map") | ("upserted", "entity_map_entry") => tracing::info_span!("rafka.gateway.entity_map.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", "entity_map") | ("decode_failed", "entity_map_entry") => tracing::info_span!("rafka.gateway.entity_map.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),

            ("starting", _) => tracing::info_span!("rafka.gateway.unknown.tailer.starting", entity_kind = $kind).in_scope(|| {}),
            ("upserted", _) => tracing::info_span!("rafka.gateway.unknown.tailer.upserted", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            ("decode_failed", _) => tracing::info_span!("rafka.gateway.unknown.tailer.decode_failed", entity_kind = $kind, offset = $offset).in_scope(|| {}),
            _ => tracing::info_span!("rafka.gateway.unknown.tailer", entity_kind = $kind).in_scope(|| {}),
        }
    }
}

impl<E: CachedEntity, B: BrokerRpc, M: MeshGossip> EntityCache<E, B, M> {
    pub fn spawn_tailer(
        self: Arc<Self>,
        scope: TailScope,
        side_effect: Option<Arc<dyn TailerSideEffect<E>>>,
    ) -> JoinHandle<()> {
        match scope {
            TailScope::Global => self.spawn_global_tailer(side_effect),
            TailScope::PerOrg { discover } => self.spawn_per_org_tailer(discover, side_effect),
        }
    }

    fn spawn_global_tailer(
        self: Arc<Self>,
        side_effect: Option<Arc<dyn TailerSideEffect<E>>>,
    ) -> JoinHandle<()> {
        let topic_path = E::topic_path(None).into_owned();
        tokio::spawn(async move {
            tailer_span!("starting", E::span_namespace(), E::entity_kind(), 0);

            loop {
                let frame = match self.broker_rpc.next_record(&topic_path).await {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::warn!("broker_rpc next_record failed: {}", e);
                        if self.is_ready() {
                            let now = tokio::time::Instant::now();
                            let should_publish = match DEGRADED_DEBOUNCE.get(E::entity_kind()) {
                                Some(last_time) if now.duration_since(*last_time).as_secs() <= 10 => false,
                                _ => {
                                    DEGRADED_DEBOUNCE.insert(E::entity_kind(), now);
                                    true
                                }
                            };
                            if should_publish {
                                let tier_str = match E::cache_tier() {
                                    crate::traits::CacheTier::Functional => "functional",
                                    crate::traits::CacheTier::Feature => "feature",
                                };
                                crate::health::record_cache_status(E::entity_kind(), "degraded", tier_str);
                                let _ = crate::health::cache_event_sender().send(crate::health::CacheEvent::Degraded {
                                    entity_kind: E::entity_kind(),
                                    tier: E::cache_tier(),
                                    reason: e.to_string(),
                                });
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue;
                    }
                };

                let (key, event) = match E::decode_record(&frame.payload, &frame.payload) { // NOTE: Key and value bytes are the same here since payload carries the full record
                    Ok((k, ev)) => (k, ev),
                    Err(e) => {
                        tracing::error!("decode_record failed: {}", e);
                        tailer_span!("decode_failed", E::span_namespace(), E::entity_kind(), frame.offset);
                        continue;
                    }
                };

                match event {
                    crate::traits::TailerEvent::Value(v) => {
                        let value = Some(Arc::new(v));
                        if let Some(ref se) = side_effect { se.before_apply(&key, value.as_deref()); }
                        self.apply_tail_update(key.clone(), frame.offset, value.clone()).await;
                        if let Some(ref se) = side_effect { se.after_apply(&key, value.as_deref()); }
                        tailer_span!("upserted", E::span_namespace(), E::entity_kind(), frame.offset);
                    }
                    crate::traits::TailerEvent::Tombstone => {
                        if let Some(ref se) = side_effect { se.before_apply(&key, None); }
                        self.apply_tail_update(key.clone(), frame.offset, None).await;
                        if let Some(ref se) = side_effect { se.after_apply(&key, None); }
                        tailer_span!("tombstoned", E::span_namespace(), E::entity_kind(), frame.offset);
                    }
                    crate::traits::TailerEvent::Skip => {
                        // Just advance the watermark without applying.
                        self.tail_offset.store(frame.offset, std::sync::atomic::Ordering::Release);
                        tailer_span!("skipped", E::span_namespace(), E::entity_kind(), frame.offset);
                    }
                }
            }
        })
    }

    fn spawn_per_org_tailer(
        self: Arc<Self>,
        discover: Arc<dyn crate::traits::OrgDiscovery>,
        side_effect: Option<Arc<dyn TailerSideEffect<E>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut org_tasks = std::collections::HashMap::new();
            let current_orgs = discover.current_org_ids();

            for org_id in current_orgs {
                let cache_clone = self.clone();
                let se_clone = side_effect.clone();
                let handle = tokio::spawn(async move {
                    cache_clone.spawn_org_tailer(org_id, se_clone).await;
                });
                org_tasks.insert(org_id, handle);
            }

            let mut sub = discover.subscribe_org_changes();
            while let Ok(event) = sub.recv().await {
                match event {
                    crate::traits::OrgEvent::Added(org_id) => {
                        if !org_tasks.contains_key(&org_id) {
                            let cache_clone = self.clone();
                            let se_clone = side_effect.clone();
                            let handle = tokio::spawn(async move {
                                cache_clone.spawn_org_tailer(org_id, se_clone).await;
                            });
                            org_tasks.insert(org_id, handle);
                        }
                    }
                    crate::traits::OrgEvent::Removed(org_id) => {
                        if let Some(handle) = org_tasks.remove(&org_id) {
                            handle.abort();
                        }
                    }
                }
            }
        })
    }

    async fn spawn_org_tailer(
        self: Arc<Self>,
        org_id: u64,
        side_effect: Option<Arc<dyn TailerSideEffect<E>>>,
    ) {
        let topic_path = E::topic_path(Some(&org_id.to_string())).into_owned();
        tailer_span!("starting", E::span_namespace(), E::entity_kind(), 0);

        loop {
            let frame = match self.broker_rpc.next_record(&topic_path).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!("broker_rpc next_record failed: {}", e);
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
            };

            let (key, event) = match E::decode_record(&frame.payload, &frame.payload) {
                Ok((k, ev)) => (k, ev),
                Err(e) => {
                    tracing::error!("decode_record failed: {}", e);
                    tailer_span!("decode_failed", E::span_namespace(), E::entity_kind(), frame.offset);
                    continue;
                }
            };

            match event {
                crate::traits::TailerEvent::Value(v) => {
                    let value = Some(Arc::new(v));
                    if let Some(ref se) = side_effect { se.before_apply(&key, value.as_deref()); }
                    self.apply_tail_update(key.clone(), frame.offset, value.clone()).await;
                    if let Some(ref se) = side_effect { se.after_apply(&key, value.as_deref()); }
                    tailer_span!("upserted", E::span_namespace(), E::entity_kind(), frame.offset);
                }
                crate::traits::TailerEvent::Tombstone => {
                    if let Some(ref se) = side_effect { se.before_apply(&key, None); }
                    self.apply_tail_update(key.clone(), frame.offset, None).await;
                    if let Some(ref se) = side_effect { se.after_apply(&key, None); }
                    tailer_span!("tombstoned", E::span_namespace(), E::entity_kind(), frame.offset);
                }
                crate::traits::TailerEvent::Skip => {
                    self.tail_offset.store(frame.offset, std::sync::atomic::Ordering::Release);
                    tailer_span!("skipped", E::span_namespace(), E::entity_kind(), frame.offset);
                }
            }
        }
    }

    /// Construct a not-yet-Ready cache. The composer must drive
    /// hydration (cold / warm / hot-restart per `Entity-Cache.md` §4)
    /// and call `mark_ready` when the cache has caught up to the
    /// broker's latest committed offset.
    pub fn new(broker_rpc: Arc<B>, mesh: Arc<M>, topic_path: String) -> Self {
        let cache = match E::max_capacity() {
            None => CacheBackend::Resident(Arc::new(DashMap::new())),
            Some(cap) => CacheBackend::Bounded(Arc::new(
                moka::future::Cache::builder().max_capacity(cap).build(),
            )),
        };

        Self {
            cache,
            tombstone_registry: Arc::new(Mutex::new(BTreeMap::new())),
            tail_offset: Arc::new(AtomicU64::new(0)),
            topic_path,
            broker_rpc,
            mesh,
            state: Arc::new(AtomicU8::new(STATE_BOOTING)),
        }
    }

    /// Flip the gate from Booting → Ready. Hydration must have:
    /// (a) drained the Stage 1 topic to the broker's latest committed
    /// offset (Resident) or (b) opened a from-LATEST tail and accepted
    /// that the cache starts empty (Bounded).
    pub fn mark_ready(&self) {
        self.state.store(STATE_READY, Ordering::Release);
        let tier_str = match E::cache_tier() {
            crate::traits::CacheTier::Functional => "functional",
            crate::traits::CacheTier::Feature => "feature",
        };
        crate::health::record_cache_status(E::entity_kind(), "ready", tier_str);
        let _ = crate::health::cache_event_sender().send(crate::health::CacheEvent::Ready {
            entity_kind: E::entity_kind(),
            tier: E::cache_tier(),
        });
    }

    pub fn publish_hydrating(&self, source: crate::health::HydrationSource, binding_count: u64) {
        let tier_str = match E::cache_tier() {
            crate::traits::CacheTier::Functional => "functional",
            crate::traits::CacheTier::Feature => "feature",
        };
        crate::health::record_cache_status(E::entity_kind(), "hydrating", tier_str);
        let _ = crate::health::cache_event_sender().send(crate::health::CacheEvent::Hydrating {
            entity_kind: E::entity_kind(),
            tier: E::cache_tier(),
            source,
            binding_count,
        });
    }

    pub fn topic_path(&self) -> &str {
        &self.topic_path
    }

    pub fn broker_rpc(&self) -> &Arc<B> {
        &self.broker_rpc
    }

    pub fn mesh(&self) -> &Arc<M> {
        &self.mesh
    }

    pub fn tail_offset(&self) -> u64 {
        self.tail_offset.load(Ordering::Acquire)
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_READY
    }

    /// Sync fast-path. Resident only. Returns `RequiresAsyncReadThrough`
    /// for Bounded backends because a miss may need a broker RPC.
    pub fn read_local(&self, key: &E::Key) -> Result<Option<Arc<E>>, CacheError> {
        if self.state.load(Ordering::Acquire) != STATE_READY {
            return Err(CacheError::NotReady);
        }
        match &self.cache {
            CacheBackend::Resident(m) => Ok(m.get(key).and_then(|r| r.value().data.clone())),
            CacheBackend::Bounded(_) => Err(CacheError::RequiresAsyncReadThrough),
        }
    }

    /// Resident-only `O(N)` snapshot of the live (non-tombstoned)
    /// entries. Returns `Err(BoundedSnapshotForbidden)` for Bounded
    /// backends — same posture as `stream_snapshot`: bounded caches
    /// hold partial post-eviction state, listing them would lie.
    ///
    /// Intended for low-frequency callers (operator listings,
    /// assignment-recompute loops) — NOT a hot-path read. For per-key
    /// reads use `read_local`. The returned `Vec` is a point-in-time
    /// copy; the cache may have moved on by the time it's consumed.
    pub fn current_entries(&self) -> Result<Vec<(E::Key, Arc<E>)>, CacheError> {
        if self.state.load(Ordering::Acquire) != STATE_READY {
            return Err(CacheError::NotReady);
        }
        match &self.cache {
            CacheBackend::Resident(m) => Ok(m
                .iter()
                .filter_map(|kv| {
                    kv.value()
                        .data
                        .as_ref()
                        .map(|arc| (kv.key().clone(), arc.clone()))
                })
                .collect()),
            CacheBackend::Bounded(_) => Err(CacheError::BoundedSnapshotForbidden),
        }
    }

    /// Like `current_entries` but bypasses the STATE_READY gate.
    ///
    /// Safe ONLY for disk-export snapshots taken before boot hydration completes
    /// (e.g. `persist_state_to_disk` called during ceremony before the boot
    /// sequencer has run). Bounded backends return an empty vec — they cannot
    /// be fully snapshotted regardless of ready state.
    pub fn snapshot_all_raw(&self) -> Vec<(E::Key, Arc<E>)> {
        match &self.cache {
            CacheBackend::Resident(m) => m
                .iter()
                .filter_map(|kv| {
                    kv.value()
                        .data
                        .as_ref()
                        .map(|arc| (kv.key().clone(), arc.clone()))
                })
                .collect(),
            CacheBackend::Bounded(_) => Vec::new(),
        }
    }

    /// CREATE — write-through with `expect_present = true` (CAS-style
    /// conflict on existing). Detaches RYOW apply via `tokio::spawn` so
    /// the local cache update completes even if the request future is
    /// dropped on client disconnect.
    pub async fn insert(&self, entity: E) -> Result<(), CacheError> {
        let key = entity.key();
        let offset = self
            .broker_rpc
            .put_entity::<E>(E::put_op(), &entity, true)
            .await
            .map_err(map_put_err)?;

        let cache_clone = self.clone();
        let entity_arc = Arc::new(entity);
        tokio::spawn(async move {
            cache_clone
                .apply_update(key, offset, Some(entity_arc), UpdateSource::LocalMutation)
                .await;
        });
        Ok(())
    }

    /// UPDATE — write-through with `expect_present = true`; broker
    /// returns `NotFound` on missing key.
    pub async fn update(&self, entity: E) -> Result<(), CacheError> {
        let key = entity.key();
        let offset = self
            .broker_rpc
            .put_entity::<E>(E::put_op(), &entity, true)
            .await
            .map_err(map_put_err)?;

        let cache_clone = self.clone();
        let entity_arc = Arc::new(entity);
        tokio::spawn(async move {
            cache_clone
                .apply_update(key, offset, Some(entity_arc), UpdateSource::LocalMutation)
                .await;
        });
        Ok(())
    }

    /// UPSERT — write-through with `expect_present = false`.
    pub async fn upsert(&self, entity: E) -> Result<(), CacheError> {
        let key = entity.key();
        let offset = self
            .broker_rpc
            .put_entity::<E>(E::put_op(), &entity, false)
            .await
            .map_err(CacheError::BrokerRpc)?;

        let cache_clone = self.clone();
        let entity_arc = Arc::new(entity);
        tokio::spawn(async move {
            cache_clone
                .apply_update(key, offset, Some(entity_arc), UpdateSource::LocalMutation)
                .await;
        });
        Ok(())
    }

    /// DELETE — write-through tombstone with detached RYOW.
    pub async fn delete(&self, key: &E::Key) -> Result<(), CacheError> {
        let offset = self
            .broker_rpc
            .delete_entity::<E>(E::delete_op(), key)
            .await
            .map_err(CacheError::BrokerRpc)?;

        let cache_clone = self.clone();
        let k = key.clone();
        tokio::spawn(async move {
            cache_clone
                .apply_update(k, offset, None, UpdateSource::LocalMutation)
                .await;
        });
        Ok(())
    }

    /// Resident-only snapshot donor for warm-boot peers. Pins the donor
    /// offset BEFORE iterating so receivers can resume tail from a
    /// known watermark. O(N) key extraction is offloaded to
    /// `spawn_blocking` to avoid starving the Tokio worker.
    pub fn stream_snapshot(
        &self,
    ) -> Result<impl Stream<Item = SnapshotChunk<E>> + Send + 'static, CacheError>
    where
        E::Key: Send + 'static,
        E: Send + Sync + 'static,
    {
        let m = match &self.cache {
            CacheBackend::Resident(m) => m.clone(),
            CacheBackend::Bounded(_) => return Err(CacheError::BoundedSnapshotForbidden),
        };

        let pinned_offset = self.tail_offset.load(Ordering::Acquire);
        let map_for_keys = m.clone();
        let keys_future = tokio::task::spawn_blocking(move || {
            map_for_keys
                .iter()
                .map(|kv| kv.key().clone())
                .collect::<Vec<_>>()
        });

        const CHUNK_SIZE: usize = 1024;
        let map_for_lookup = m;

        Ok(stream! {
            let keys = match keys_future.await {
                Ok(k) => k,
                Err(_) => return,
            };

            for chunk_keys in keys.chunks(CHUNK_SIZE) {
                let mut entries = Vec::with_capacity(chunk_keys.len());
                for k in chunk_keys {
                    if let Some(v) = map_for_lookup.get(k) {
                        entries.push((k.clone(), v.value().clone()));
                    }
                }
                yield SnapshotChunk { donor_offset: pinned_offset, entries };
            }
        })
    }

    /// Atomic in-place mutation wrapper for Resident caches.
    ///
    /// Exposes `dashmap::mapref::entry::Entry` exclusively for `Resident`
    /// topologies to allow callers (like `jobs::dispatcher::claim_job`) to perform
    /// TOCTOU-safe CAS logic without full-table locks.
    ///
    /// Panics if invoked on a `Bounded` cache.
    pub fn with_entry_mut<F, R>(&self, key: E::Key, f: F) -> Result<R, CacheError>
    where
        F: FnOnce(dashmap::mapref::entry::Entry<'_, E::Key, crate::types::VersionedValue<E>>) -> R,
    {
        match &self.cache {
            CacheBackend::Resident(m) => Ok(f(m.entry(key))),
            CacheBackend::Bounded(_) => panic!("with_entry_mut requires Resident topology"),
        }
    }

    /// Conflict-resolution kernel. Handles:
    /// * Per-key offset CAS (incoming-newer wins; ties favor Put over
    ///   Tombstone for offset-0 init races).
    /// * Tombstone preservation (`data: None`) — physical removal would
    ///   forget the watermark and admit ghost resurrections.
    /// * Delayed-vacant ghost-resurrection barrier (offset within
    ///   `safe_horizon` is dropped for non-ReadThrough sources).
    /// * Bounded firehose protection (Tail/Gossip on Vacant ⇒ Nop).
    /// * Watermark advance gated to `UpdateSource::Tail`.
    pub async fn apply_update(
        &self,
        key: E::Key,
        offset: u64,
        value: Option<Arc<E>>,
        source: UpdateSource,
    ) {
        let span = tracing::info_span!(
            "rafka.entity_cache.apply_update",
            entity_kind = E::entity_kind(),
            topic_rrl = %self.topic_path,
            offset = offset,
            source = ?source
        );
        
        async move {
            let is_tombstone = value.is_none();
            let key_for_registry = key.clone();
            let key_for_gossip = key.clone();
            let value_for_gossip = value.clone();

            let current_tail = self.tail_offset.load(Ordering::Acquire);
            let safe_horizon = current_tail.saturating_sub(SAFE_REORDER_WINDOW);
            let is_stale_vacant = !matches!(source, UpdateSource::ReadThrough) && offset <= safe_horizon;

            let resident_applied = match &self.cache {
                CacheBackend::Resident(m) => {
                    use dashmap::mapref::entry::Entry;
                    let mut applied = false;
                    match m.entry(key) {
                        Entry::Occupied(mut existing) => {
                            let current = existing.get_mut();
                            let value_says = match (current.data.as_ref(), value.as_ref()) {
                                (Some(ex), Some(inc)) => E::resolve_conflict(ex, inc),
                                _ => None,
                            };
                            let is_fresher = match value_says {
                                Some(std::cmp::Ordering::Greater) => true,
                                Some(std::cmp::Ordering::Less) => false,
                                Some(std::cmp::Ordering::Equal) | None => {
                                    offset > current.offset
                                        || (offset == current.offset
                                            && current.data.is_none()
                                            && value.is_some())
                                }
                            };

                            if is_fresher {
                                current.data = value;
                                current.offset = offset;
                                applied = true;
                            }
                        }
                        Entry::Vacant(vacant) => {
                            if !is_stale_vacant {
                                vacant.insert(VersionedValue { offset, data: value });
                                applied = true;
                            }
                        }
                    }
                    applied
                }
                CacheBackend::Bounded(m) => {
                    let applied_flag = Arc::new(AtomicBool::new(false));
                    let applied_for_closure = applied_flag.clone();
                    let value_for_closure = value.clone();
                    let source_copy = source;

                    m.entry(key.clone())
                        .and_compute_with(move |maybe| {
                            let applied_for_closure = applied_for_closure.clone();
                            let value = value_for_closure;
                            async move {
                                use moka::ops::compute::Op;
                                match maybe {
                                    Some(existing) => {
                                        let cur = existing.into_value();
                                        let value_says = match (cur.data.as_ref(), value.as_ref()) {
                                            (Some(ex), Some(inc)) => E::resolve_conflict(ex, inc),
                                            _ => None,
                                        };
                                        let is_fresher = match value_says {
                                            Some(std::cmp::Ordering::Greater) => true,
                                            Some(std::cmp::Ordering::Less) => false,
                                            Some(std::cmp::Ordering::Equal) | None => {
                                                offset > cur.offset
                                                    || (offset == cur.offset
                                                        && cur.data.is_none()
                                                        && value.is_some())
                                            }
                                        };

                                        if is_fresher {
                                            applied_for_closure.store(true, Ordering::Relaxed);
                                            Op::Put(VersionedValue { offset, data: value })
                                        } else {
                                            Op::Nop
                                        }
                                    }
                                    None => {
                                        if matches!(
                                            source_copy,
                                            UpdateSource::Tail | UpdateSource::Gossip
                                        ) || is_stale_vacant
                                        {
                                            Op::Nop
                                        } else {
                                            applied_for_closure.store(true, Ordering::Relaxed);
                                            Op::Put(VersionedValue { offset, data: value })
                                        }
                                    }
                                }
                            }
                        })
                        .await;

                    applied_flag.load(Ordering::Relaxed)
                }
            };

            if resident_applied && is_tombstone && matches!(source, UpdateSource::Tail) {
                if let CacheBackend::Resident(_) = &self.cache {
                    let mut registry = self
                        .tombstone_registry
                        .lock()
                        .expect("tombstone_registry poisoned");
                    registry.entry(offset).or_default().push(key_for_registry);
                }
            }

            if let UpdateSource::Tail = source {
                self.tail_offset.store(offset, Ordering::Release);
            }

            if resident_applied && matches!(source, UpdateSource::LocalMutation) {
                self.mesh
                    .broadcast_update(&key_for_gossip, offset, value_for_gossip.as_deref())
                    .await;
            }
        }.instrument(span).await;
    }

    /// Tail-specific apply: always overwrites an existing entry (no CAS).
    ///
    /// LocalMutation entries carry nanosecond-epoch offsets (~1.7e18) that
    /// always beat sequential broker offsets in the CAS comparison inside
    /// `apply_update`. This method bypasses that comparison so Tail records
    /// are the authoritative source of truth regardless of what LocalMutation
    /// wrote optimistically.
    ///
    /// Ghost-resurrection barrier (is_stale_vacant) and the tombstone registry
    /// are preserved verbatim. Mesh broadcast is NOT performed — that is
    /// LocalMutation territory.
    async fn apply_tail_update(
        &self,
        key: E::Key,
        offset: u64,
        value: Option<Arc<E>>,
    ) {
        let span = tracing::info_span!(
            "rafka.entity_cache.apply_tail_update",
            entity_kind = E::entity_kind(),
            topic_rrl = %self.topic_path,
            offset = offset,
        );

        async move {
            let is_tombstone = value.is_none();
            let key_for_registry = key.clone();

            let current_tail = self.tail_offset.load(Ordering::Acquire);
            let safe_horizon = current_tail.saturating_sub(SAFE_REORDER_WINDOW);
            let is_stale_vacant = offset <= safe_horizon;

            let resident_applied = match &self.cache {
                CacheBackend::Resident(m) => {
                    use dashmap::mapref::entry::Entry;
                    let mut applied = false;
                    match m.entry(key) {
                        Entry::Occupied(mut existing) => {
                            let current = existing.get_mut();
                            current.data = value;
                            current.offset = offset;
                            applied = true;
                        }
                        Entry::Vacant(vacant) => {
                            if !is_stale_vacant {
                                vacant.insert(VersionedValue { offset, data: value });
                                applied = true;
                            }
                        }
                    }
                    applied
                }
                CacheBackend::Bounded(m) => {
                    let applied_flag = Arc::new(AtomicBool::new(false));
                    let applied_for_closure = applied_flag.clone();
                    let value_for_closure = value.clone();

                    m.entry(key.clone())
                        .and_compute_with(move |maybe| {
                            let applied_for_closure = applied_for_closure.clone();
                            let value = value_for_closure;
                            async move {
                                use moka::ops::compute::Op;
                                match maybe {
                                    Some(_existing) => {
                                        applied_for_closure.store(true, Ordering::Relaxed);
                                        Op::Put(VersionedValue { offset, data: value })
                                    }
                                    None => {
                                        if is_stale_vacant {
                                            Op::Nop
                                        } else {
                                            applied_for_closure.store(true, Ordering::Relaxed);
                                            Op::Put(VersionedValue { offset, data: value })
                                        }
                                    }
                                }
                            }
                        })
                        .await;

                    applied_flag.load(Ordering::Relaxed)
                }
            };

            if resident_applied && is_tombstone {
                if let CacheBackend::Resident(_) = &self.cache {
                    let mut registry = self
                        .tombstone_registry
                        .lock()
                        .expect("tombstone_registry poisoned");
                    registry.entry(offset).or_default().push(key_for_registry);
                }
            }

            self.tail_offset.store(offset, Ordering::Release);
        }.instrument(span).await;
    }

    pub async fn apply_update_with<F>(
        self: Arc<Self>,
        key: E::Key,
        offset: u64,
        source: UpdateSource,
        f: F,
    )
    where
        F: FnOnce(Option<&E>) -> Option<E> + Send,
    {
        let span = tracing::info_span!(
            "rafka.entity_cache.apply_update_with",
            entity_kind = E::entity_kind(),
            topic_rrl = %self.topic_path,
            offset = offset,
            source = ?source
        );

        async move {
            let key_for_registry = key.clone();
            let key_for_gossip = key.clone();

            let current_tail = self.tail_offset.load(Ordering::Acquire);
            let safe_horizon = current_tail.saturating_sub(SAFE_REORDER_WINDOW);
            let is_stale_vacant = !matches!(source, UpdateSource::ReadThrough) && offset <= safe_horizon;

            let (resident_applied, value) = match &self.cache {
                CacheBackend::Resident(m) => {
                    use dashmap::mapref::entry::Entry;
                    let mut applied = false;
                    #[allow(unused_assignments)]
                    let mut value: Option<Arc<E>> = None;
                    match m.entry(key) {
                        Entry::Occupied(mut existing) => {
                            let current = existing.get_mut();
                            let new_value = f(current.data.as_deref());
                            let value_arc = new_value.map(Arc::new);
                            
                            let is_fresher = true;
                            
                            if is_fresher {
                                current.data = value_arc.clone();
                                current.offset = offset;
                                applied = true;
                            }
                            value = value_arc;
                        }
                        Entry::Vacant(vacant) => {
                            let new_value = f(None);
                            let value_arc = new_value.map(Arc::new);
                            if !is_stale_vacant {
                                vacant.insert(VersionedValue { offset, data: value_arc.clone() });
                                applied = true;
                            }
                            value = value_arc;
                        }
                    }
                    (applied, value)
                }
                CacheBackend::Bounded(_) => panic!("apply_update_with requires Resident topology"),
            };

            let is_tombstone = value.is_none();

            if resident_applied && is_tombstone && matches!(source, UpdateSource::Tail) {
                if let CacheBackend::Resident(_) = &self.cache {
                    let mut registry = self
                        .tombstone_registry
                        .lock()
                        .expect("tombstone_registry poisoned");
                    registry.entry(offset).or_default().push(key_for_registry);
                }
            }

            if let UpdateSource::Tail = source {
                self.tail_offset.store(offset, Ordering::Release);
            }

            if resident_applied && matches!(source, UpdateSource::LocalMutation) {
                self.mesh
                    .broadcast_update(&key_for_gossip, offset, value.as_deref())
                    .await;
            }
        }.instrument(span).await;
    }

    /// O(log N) reaper. Splits the registry at `safe_horizon + 1` so any
    /// tombstone offset ≤ safe_horizon is reaped, and uses
    /// compare-and-delete to skip keys that have since been overwritten
    /// by a fresher Put at a higher offset.
    pub async fn reap_old_tombstones(&self, safe_reorder_window: u64) {
        let current_tail = self.tail_offset.load(Ordering::Acquire);
        let safe_horizon = current_tail.saturating_sub(safe_reorder_window);

        let expired_tombstones = {
            let mut registry = self
                .tombstone_registry
                .lock()
                .expect("tombstone_registry poisoned");
            // Everything strictly greater than safe_horizon stays; the
            // returned `keep` is the tail. The original map (now the
            // expired prefix) is what we want to drain.
            let keep = registry.split_off(&(safe_horizon + 1));
            std::mem::replace(&mut *registry, keep)
        };

        for (tombstone_offset, keys) in expired_tombstones {
            for key in keys {
                match &self.cache {
                    CacheBackend::Resident(m) => {
                        m.remove_if(&key, |_, v| {
                            v.offset == tombstone_offset && v.data.is_none()
                        });
                    }
                    CacheBackend::Bounded(m) => {
                        m.entry(key.clone())
                            .and_compute_with(move |maybe| async move {
                                use moka::ops::compute::Op;
                                match maybe {
                                    Some(existing) => {
                                        let v = existing.into_value();
                                        if v.offset == tombstone_offset && v.data.is_none() {
                                            Op::Remove
                                        } else {
                                            Op::Nop
                                        }
                                    }
                                    None => Op::Nop,
                                }
                            })
                            .await;
                    }
                }
            }
        }
    }
}

/// Map a `put`/`update` broker error into the typed cache error.
/// The broker returns the literal string "Conflict" on `expect_present`
/// CAS failure; everything else is id transport.
fn map_put_err(s: String) -> CacheError {
    if s == "Conflict" {
        CacheError::Conflict
    } else if s == "NotFound" {
        CacheError::NotFound
    } else {
        CacheError::BrokerRpc(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::BrokerRecord;
    use std::borrow::Cow;
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct ToyEntity {
        id: String,
        payload: u64,
    }

    impl CachedEntity for ToyEntity {
        type Key = String;
        fn key(&self) -> String {
            self.id.clone()
        }
        fn topic_path(_org: Option<&str>) -> Cow<'static, str> {
            Cow::Borrowed("rrl:org-rafka:env-global:clu-system:topics:toy")
        }
        fn entity_kind() -> &'static str {
            "toy"
        }
        fn put_op() -> u16 {
            1
        }
        fn delete_op() -> u16 {
            2
        }
        fn get_op() -> u16 {
            3
        }
        fn snapshot_op() -> u16 {
            4
        }
        fn delete_retention() -> Duration {
            Duration::from_secs(3600)
        }
    }

    struct NoopBroker;
    impl BrokerRpc for NoopBroker {
        fn next_record(
            &self,
            _topic_path: &str,
        ) -> impl std::future::Future<Output = Result<BrokerRecord, String>> + Send {
            async { Err("unimplemented".into()) }
        }
        fn put_entity<E: CachedEntity>(
            &self,
            _op: u16,
            _entity: &E,
            _expect_present: bool,
        ) -> impl std::future::Future<Output = Result<u64, String>> + Send {
            async { Ok(1) }
        }
        fn delete_entity<E: CachedEntity>(
            &self,
            _op: u16,
            _key: &E::Key,
        ) -> impl std::future::Future<Output = Result<u64, String>> + Send {
            async { Ok(2) }
        }
    }

    struct NoopMesh;
    impl MeshGossip for NoopMesh {
        fn broadcast_update<E: CachedEntity>(
            &self,
            _key: &E::Key,
            _offset: u64,
            _data: Option<&E>,
        ) -> impl std::future::Future<Output = ()> + Send {
            async {}
        }
    }

    fn fresh() -> EntityCache<ToyEntity, NoopBroker, NoopMesh> {
        EntityCache::new(
            Arc::new(NoopBroker),
            Arc::new(NoopMesh),
            "rrl:org-rafka:env-global:clu-system:topics:toy".into(),
        )
    }

    #[tokio::test]
    async fn read_local_returns_not_ready_before_mark_ready() {
        let cache = fresh();
        let err = cache.read_local(&"k".to_string()).unwrap_err();
        assert!(matches!(err, CacheError::NotReady));
    }

    #[tokio::test]
    async fn apply_update_then_read_local() {
        let cache = fresh();
        cache.mark_ready();
        let entity = ToyEntity {
            id: "a".into(),
            payload: 7,
        };
        cache
            .apply_update("a".into(), 5, Some(Arc::new(entity)), UpdateSource::Tail)
            .await;

        let got = cache.read_local(&"a".to_string()).unwrap();
        assert_eq!(got.unwrap().payload, 7);
        assert_eq!(cache.tail_offset(), 5);
    }

    #[tokio::test]
    async fn older_offset_is_dropped_by_cas() {
        let cache = fresh();
        cache.mark_ready();

        cache
            .apply_update(
                "a".into(),
                10,
                Some(Arc::new(ToyEntity {
                    id: "a".into(),
                    payload: 100,
                })),
                UpdateSource::Tail,
            )
            .await;
        cache
            .apply_update(
                "a".into(),
                5,
                Some(Arc::new(ToyEntity {
                    id: "a".into(),
                    payload: 50,
                })),
                UpdateSource::Gossip,
            )
            .await;

        let got = cache.read_local(&"a".to_string()).unwrap().unwrap();
        assert_eq!(got.payload, 100);
    }

    #[tokio::test]
    async fn tombstone_is_preserved_not_removed() {
        let cache = fresh();
        cache.mark_ready();

        cache
            .apply_update(
                "a".into(),
                5,
                Some(Arc::new(ToyEntity {
                    id: "a".into(),
                    payload: 1,
                })),
                UpdateSource::Tail,
            )
            .await;
        cache
            .apply_update("a".into(), 6, None, UpdateSource::Tail)
            .await;

        // Read returns None (tombstoned) but the slot is retained so a
        // delayed older Put cannot resurrect.
        assert!(cache.read_local(&"a".to_string()).unwrap().is_none());

        cache
            .apply_update(
                "a".into(),
                4,
                Some(Arc::new(ToyEntity {
                    id: "a".into(),
                    payload: 999,
                })),
                UpdateSource::Gossip,
            )
            .await;
        // Still tombstoned; offset 4 < 6 was rejected by CAS.
        assert!(cache.read_local(&"a".to_string()).unwrap().is_none());
    }

    #[tokio::test]
    async fn gossip_does_not_advance_tail_watermark() {
        let cache = fresh();
        cache.mark_ready();

        cache
            .apply_update(
                "a".into(),
                100,
                Some(Arc::new(ToyEntity {
                    id: "a".into(),
                    payload: 1,
                })),
                UpdateSource::Gossip,
            )
            .await;
        assert_eq!(cache.tail_offset(), 0, "gossip must not advance tail");

        cache
            .apply_update(
                "b".into(),
                50,
                Some(Arc::new(ToyEntity {
                    id: "b".into(),
                    payload: 2,
                })),
                UpdateSource::Tail,
            )
            .await;
        assert_eq!(cache.tail_offset(), 50, "tail must advance");
    }

    #[tokio::test]
    async fn stream_snapshot_pins_donor_offset() {
        use futures::StreamExt;

        let cache = fresh();
        cache.mark_ready();
        for i in 0..3u64 {
            cache
                .apply_update(
                    format!("k{i}"),
                    i + 1,
                    Some(Arc::new(ToyEntity {
                        id: format!("k{i}"),
                        payload: i,
                    })),
                    UpdateSource::Tail,
                )
                .await;
        }

        let pinned_at = cache.tail_offset();
        let mut stream = Box::pin(cache.stream_snapshot().expect("resident snapshot ok"));
        let mut total = 0;
        while let Some(chunk) = stream.next().await {
            assert_eq!(chunk.donor_offset, pinned_at);
            total += chunk.entries.len();
        }
        assert_eq!(total, 3);
    }

    /// Variant of ToyEntity that uses a value-driven resolver:
    /// later `payload` wins, with `id`-string as a tie-break (mirrors
    /// `VtAssignment`'s `(epoch, coordinator_peer_id)` shape).
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct ResolverToy {
        id: String,
        payload: u64,
        tiebreak: u32,
    }
    impl CachedEntity for ResolverToy {
        type Key = String;
        fn key(&self) -> String {
            self.id.clone()
        }
        fn topic_path(_: Option<&str>) -> Cow<'static, str> {
            Cow::Borrowed("rrl:org-rafka:env-global:clu-system:topics:resolver_toy")
        }
        fn entity_kind() -> &'static str {
            "resolver_toy"
        }
        fn put_op() -> u16 {
            10
        }
        fn delete_op() -> u16 {
            11
        }
        fn get_op() -> u16 {
            12
        }
        fn snapshot_op() -> u16 {
            13
        }
        fn delete_retention() -> Duration {
            Duration::from_secs(60)
        }

        fn resolve_conflict(ex: &Self, inc: &Self) -> Option<std::cmp::Ordering> {
            match inc.payload.cmp(&ex.payload) {
                std::cmp::Ordering::Equal => Some(inc.tiebreak.cmp(&ex.tiebreak)),
                o => Some(o),
            }
        }
    }

    fn fresh_resolver() -> EntityCache<ResolverToy, NoopBroker, NoopMesh> {
        EntityCache::new(
            Arc::new(NoopBroker),
            Arc::new(NoopMesh),
            "rrl:org-rafka:env-global:clu-system:topics:resolver_toy".into(),
        )
    }

    #[tokio::test]
    async fn resolver_value_wins_over_lower_offset_when_value_says_greater() {
        let cache = fresh_resolver();
        cache.mark_ready();

        // First write: high offset, low payload.
        cache
            .apply_update(
                "k".into(),
                100,
                Some(Arc::new(ResolverToy {
                    id: "k".into(),
                    payload: 1,
                    tiebreak: 0,
                })),
                UpdateSource::Tail,
            )
            .await;
        // Second write: LOWER offset, but HIGHER payload — resolver wins.
        cache
            .apply_update(
                "k".into(),
                50,
                Some(Arc::new(ResolverToy {
                    id: "k".into(),
                    payload: 9,
                    tiebreak: 0,
                })),
                UpdateSource::Gossip,
            )
            .await;

        let got = cache.read_local(&"k".to_string()).unwrap().unwrap();
        assert_eq!(got.payload, 9, "resolver Greater must override offset CAS");
    }

    #[tokio::test]
    async fn resolver_tiebreak_resolves_equal_payload() {
        let cache = fresh_resolver();
        cache.mark_ready();

        cache
            .apply_update(
                "k".into(),
                10,
                Some(Arc::new(ResolverToy {
                    id: "k".into(),
                    payload: 5,
                    tiebreak: 1,
                })),
                UpdateSource::Tail,
            )
            .await;
        // Same payload, higher tiebreak — resolver says Greater.
        cache
            .apply_update(
                "k".into(),
                11,
                Some(Arc::new(ResolverToy {
                    id: "k".into(),
                    payload: 5,
                    tiebreak: 7,
                })),
                UpdateSource::Tail,
            )
            .await;
        let got = cache.read_local(&"k".to_string()).unwrap().unwrap();
        assert_eq!(got.tiebreak, 7);

        // Same payload, LOWER tiebreak — resolver says Less; existing wins
        // even though incoming offset is higher.
        cache
            .apply_update(
                "k".into(),
                100,
                Some(Arc::new(ResolverToy {
                    id: "k".into(),
                    payload: 5,
                    tiebreak: 2,
                })),
                UpdateSource::Tail,
            )
            .await;
        let got = cache.read_local(&"k".to_string()).unwrap().unwrap();
        assert_eq!(got.tiebreak, 7, "resolver Less must override offset CAS");
    }

    #[tokio::test]
    async fn default_resolver_preserves_offset_cas_for_toy_entity() {
        // ToyEntity does NOT override resolve_conflict — verify the
        // existing offset-CAS test still holds. Regression guard: the
        // resolver enhancement must be no-op for entities that don't
        // opt in.
        let cache = fresh();
        cache.mark_ready();
        cache
            .apply_update(
                "a".into(),
                10,
                Some(Arc::new(ToyEntity {
                    id: "a".into(),
                    payload: 100,
                })),
                UpdateSource::Tail,
            )
            .await;
        cache
            .apply_update(
                "a".into(),
                5,
                Some(Arc::new(ToyEntity {
                    id: "a".into(),
                    payload: 50,
                })),
                UpdateSource::Tail,
            )
            .await;
        let got = cache.read_local(&"a".to_string()).unwrap().unwrap();
        assert_eq!(got.payload, 100, "default resolver must fall through to offset CAS");
    }

    #[tokio::test]
    async fn current_entries_skips_tombstones_and_not_ready() {
        let cache = fresh();
        // Booting → NotReady
        let err = cache.current_entries().unwrap_err();
        assert!(matches!(err, CacheError::NotReady));

        cache.mark_ready();
        cache
            .apply_update(
                "live".into(),
                1,
                Some(Arc::new(ToyEntity {
                    id: "live".into(),
                    payload: 11,
                })),
                UpdateSource::Tail,
            )
            .await;
        cache
            .apply_update(
                "dead".into(),
                2,
                Some(Arc::new(ToyEntity {
                    id: "dead".into(),
                    payload: 22,
                })),
                UpdateSource::Tail,
            )
            .await;
        cache
            .apply_update("dead".into(), 3, None, UpdateSource::Tail)
            .await;

        let entries = cache.current_entries().unwrap();
        assert_eq!(entries.len(), 1, "tombstoned key must be filtered out");
        assert_eq!(entries[0].0, "live");
        assert_eq!(entries[0].1.payload, 11);
    }
}
