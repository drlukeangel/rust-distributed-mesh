// Process-global topology-node entity cache for every node type
// (gateway / broker / compute / registry / observer / admin-ui).
//
// Phase 2: ported from `admin-ui/src/topology_cache.rs` into `rafka-node-base`
// so the cache is available to every node that calls `NodeRuntime::run()`.
//
// The cache is filled by `run_topology_cache_fill` which is spawned inside
// `run_node` — every node running NodeRuntime gets a topology view automatically.
//
// The admin-ui re-exports these types and reads the process-global via
// `topology_nodes()` instead of maintaining its own separate cache.

use std::borrow::Cow;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rafka_entity_cache::{BrokerRpc, CachedEntity, EntityCache, MeshGossip, UpdateSource};
use rafka_entity_cache::traits::BrokerRecord;

// ── Entity ──────────────────────────────────────────────────────────────────

/// One node in the mesh topology, projected from a `GossipDigest`.
/// Stored in the entity-cache; served by `GET /api/topology/node`.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct TopologyNode {
    pub node_id: String,
    pub node_name: String,
    pub mesh_id: String,
    pub node_type: String,
    /// The reachable bind address ("ip:port") from GossipDigest.location.
    pub ip_port: String,
    /// `format!("{:?}", digest.state)` — e.g. "Alive", "Joining".
    pub state: String,
    pub stateful: bool,
    pub cpu_used: f32,
    pub cpu_budget: f32,
    pub ram_used: f32,
    pub ram_budget: f32,
}

impl CachedEntity for TopologyNode {
    type Key = String; // node_id

    fn key(&self) -> String {
        self.node_id.clone()
    }

    fn topic_path(_org: Option<&str>) -> Cow<'static, str> {
        Cow::Borrowed("system/system/ops/topology-nodes")
    }

    fn entity_kind() -> &'static str {
        "topology_node"
    }

    // Op-codes: keep the same values as the admin-ui version to avoid confusion.
    fn put_op() -> u16 { 900 }
    fn delete_op() -> u16 { 901 }
    fn get_op() -> u16 { 902 }
    fn snapshot_op() -> u16 { 903 }

    fn delete_retention() -> Duration {
        Duration::from_secs(3600)
    }

    fn max_capacity() -> Option<u64> {
        None // Resident DashMap — full-resident, no eviction
    }
}

// ── Stub transports ──────────────────────────────────────────────────────────
//
// The cache is filled exclusively from gossip (live_digests() → apply_update).
// We never call insert/update/upsert/delete on this cache, so the broker and
// mesh-gossip transport impls never fire. They exist only to satisfy the
// EntityCache<E, B, M> type bounds.

/// Never returns — the gossip-fill path never drives a broker tail.
pub struct StubBrokerRpc;

impl BrokerRpc for StubBrokerRpc {
    fn next_record(
        &self,
        _topic_path: &str,
    ) -> impl std::future::Future<Output = Result<BrokerRecord, String>> + Send {
        // Pending forever — the tailer is never spawned for this cache.
        async { std::future::pending::<Result<BrokerRecord, String>>().await }
    }

    fn put_entity<E: CachedEntity>(
        &self,
        _op: u16,
        _entity: &E,
        _expect_present: bool,
    ) -> impl std::future::Future<Output = Result<u64, String>> + Send {
        async { Ok(0) }
    }

    fn delete_entity<E: CachedEntity>(
        &self,
        _op: u16,
        _key: &E::Key,
    ) -> impl std::future::Future<Output = Result<u64, String>> + Send {
        async { Ok(0) }
    }
}

/// No-op — the node never broadcasts entity gossip; live_digests() is the
/// source, not the destination.
pub struct StubMeshGossip;

impl MeshGossip for StubMeshGossip {
    fn broadcast_update<E: CachedEntity>(
        &self,
        _key: &E::Key,
        _offset: u64,
        _data: Option<&E>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }
}

// ── Concrete cache type ──────────────────────────────────────────────────────

pub type TopologyCache = EntityCache<TopologyNode, StubBrokerRpc, StubMeshGossip>;

/// Build the topology-node cache and immediately mark it ready.
/// (No broker hydration — the gossip fill task drives it directly.)
pub fn build_topology_cache() -> Arc<TopologyCache> {
    let cache = TopologyCache::new(
        Arc::new(StubBrokerRpc),
        Arc::new(StubMeshGossip),
        "system/system/ops/topology-nodes".to_string(),
    );
    cache.mark_ready();
    Arc::new(cache)
}

// ── Process-global singleton ─────────────────────────────────────────────────

static TOPOLOGY_NODES: OnceLock<Arc<TopologyCache>> = OnceLock::new();

/// Returns (or lazily builds) the process-global topology-node cache.
/// Safe to call from any thread/task after `NodeRuntime::run()` starts.
pub fn topology_nodes() -> Arc<TopologyCache> {
    TOPOLOGY_NODES.get_or_init(build_topology_cache).clone()
}

// ── Gossip-fill task ─────────────────────────────────────────────────────────
//
// Every ~1s: iterate live_digests() → project each digest to a TopologyNode →
// apply_update(..., UpdateSource::Gossip). Emits a per-upsert tracing span so
// each update is visible in stdout/OTLP telemetry.
//
// Every ~10s: emit a SUMMARY span with the cache count and this node's identity
// so the soak can confirm each node's cache size over time.

/// Project a gossip digest into the data-plane TopologyNode shape. Shared by
/// the fill loop and birth-injection so they stay in lockstep.
fn digest_to_topology_node(d: &crate::GossipDigest) -> TopologyNode {
    TopologyNode {
        node_id: d.node_id.clone(),
        node_name: d.node_name.clone(),
        mesh_id: d.mesh_id.clone(),
        node_type: d.node_type.clone(),
        ip_port: d.location.clone(),
        state: format!("{:?}", d.state),
        stateful: d.stateful,
        cpu_used: d.cpu_used,
        cpu_budget: d.cpu_budget,
        ram_used: d.ram_used,
        ram_budget: d.ram_budget,
    }
}

/// Birth-injection ("born knowing"): if node-admin handed this child a topology
/// snapshot at spawn via `RAFKA_TOPOLOGY_BIRTH_FILE`, hydrate BOTH `live_digests()`
/// AND the data-plane `topology_nodes()` cache from it BEFORE gossip starts — so
/// the node can route from t=0 instead of waiting ~1s for gossip to warm up.
///
/// Seeding `live_digests()` lets the existing fill loop / eviction / staleness
/// pruner treat the injected view as real (confirmed by gossip → kept; never
/// confirmed → aged out after the keep-alive window). Populating the cache
/// directly makes the data plane ready immediately, not on the first fill tick.
pub async fn inject_birth_topology(self_node_id: &str, self_node_name: &str) {
    let path = match std::env::var("RAFKA_TOPOLOGY_BIRTH_FILE") {
        Ok(p) if !p.is_empty() => p,
        _ => return, // first node into an empty mesh, or not admin-spawned
    };
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(path = %path, error = %e, "birth-topology file unreadable; skipping");
            return;
        }
    };
    let digests: Vec<crate::GossipDigest> = match postcard::from_bytes(&bytes) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "birth-topology decode failed; skipping");
            return;
        }
    };

    let live = crate::live_digests();
    let cache = topology_nodes();
    let count = digests.len();
    // Local receive-time stamped on EVERY injected node so the staleness pruner
    // treats the born-knowing view as freshly-seen. The pruner compares
    // `now - last_seen_ms` (NOT the digest's wall_time_ms); without an entry it
    // reads `unwrap_or(0)`, so an un-stamped birth node looks infinitely stale and
    // is evicted on the very first 5s sweep — collapsing the injected topology
    // before gossip can confirm it and forcing a full re-discovery storm at boot.
    // Stamping `now` gives each injected node the same keep-alive window a
    // gossip-received digest gets: confirmed by gossip → refreshed and kept; never
    // confirmed → aged out after RAFKA_STALENESS_MS, exactly as the doc-comment promises.
    let now_recv = crate::now_unix_ms();
    for d in &digests {
        live.insert(d.node_id.clone(), d.clone());
        // Temporary guard dropped at the `;` — never held across the await below.
        crate::last_seen_ms()
            .lock()
            .unwrap()
            .insert(d.node_id.clone(), now_recv);
        cache
            .apply_update(
                d.node_id.clone(),
                d.wall_time_ms,
                Some(Arc::new(digest_to_topology_node(d))),
                UpdateSource::Gossip,
            )
            .await;
    }

    tracing::info_span!(
        "rafka.node.topology-nodes.add.from-birth-injection",
        self_node = %self_node_name,
        self_node_id = %self_node_id,
        injected = count as i64,
        "otel.kind" = "internal",
    )
    .in_scope(|| {
        tracing::info!(
            self_node = %self_node_name,
            injected = count,
            "topology hydrated at birth from admin snapshot (cache + live_digests, pre-gossip)"
        );
    });
}

pub async fn run_topology_cache_fill(self_node_id: String, self_node_name: String) {
    let cache = topology_nodes();
    let mut tick_count: u64 = 0;

    loop {
        let digests = crate::live_digests();
        let mut live_ids = std::collections::HashSet::new();
        for entry in digests.iter() {
            let d = entry.value();
            live_ids.insert(d.node_id.clone());

            let node = TopologyNode {
                node_id: d.node_id.clone(),
                node_name: d.node_name.clone(),
                mesh_id: d.mesh_id.clone(),
                node_type: d.node_type.clone(),
                ip_port: d.location.clone(),
                state: format!("{:?}", d.state),
                stateful: d.stateful,
                cpu_used: d.cpu_used,
                cpu_budget: d.cpu_budget,
                ram_used: d.ram_used,
                ram_budget: d.ram_budget,
            };

            // Use wall_time_ms as the offset — monotonically increasing millisecond
            // timestamp so the Occupied-path CAS lets later digests overwrite earlier ones.
            let offset = d.wall_time_ms;

            cache
                .apply_update(
                    d.node_id.clone(),
                    offset,
                    Some(Arc::new(node)),
                    UpdateSource::Gossip,
                )
                .await;

            tracing::info_span!(
                "rafka.node.topology-nodes.add.from-gossip",
                node_id = %d.node_id,
                node_name = %d.node_name,
                mesh_id = %d.mesh_id,
                ip_port = %d.location,
                "otel.kind" = "internal",
            )
            .in_scope(|| {
                tracing::info!(
                    node_id = %d.node_id,
                    node_name = %d.node_name,
                    "topology-node upserted from gossip"
                );
            });
        }

        // Reconcile (eviction): tombstone any cache entry whose node_id is no
        // longer in live_digests(). live_digests() already applies the mesh
        // ~30s keep-alive (staleness pruning), so mirroring it gives the cache
        // the same keep-alive for free — a killed node drops from the cache
        // ~30s after death instead of lingering forever (prevents unbounded
        // growth + cross-node divergence under churn).
        let now = crate::now_unix_ms();
        for (key, _) in cache.snapshot_all_raw() {
            if !live_ids.contains(&key) {
                cache
                    .apply_update(key.clone(), now, None, UpdateSource::Gossip)
                    .await;
                tracing::info_span!(
                    "rafka.node.topology-nodes.remove.from-gossip",
                    node_id = %key,
                    "otel.kind" = "internal",
                )
                .in_scope(|| {
                    tracing::info!(node_id = %key, "topology-node evicted — left live_digests");
                });
            }
        }

        tick_count += 1;

        // Every ~10s (10 × 1s ticks) emit a snapshot span for soak observability.
        // Lets the team-lead confirm each node's cache size growing over time.
        if tick_count % 10 == 0 {
            let count = cache.snapshot_all_raw().len() as i64;
            tracing::info_span!(
                "rafka.node.topology-nodes.snapshot",
                self_node = %self_node_name,
                self_node_id = %self_node_id,
                count = count,
                "otel.kind" = "internal",
            )
            .in_scope(|| {
                tracing::info!(
                    self_node = %self_node_name,
                    count,
                    "topology-node cache snapshot"
                );
            });
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
