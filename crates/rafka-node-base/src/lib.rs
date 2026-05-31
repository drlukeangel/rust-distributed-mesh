use anyhow::Result;
use dashmap::DashMap;
use iroh::{EndpointAddr, endpoint::Connection, PublicKey, SecretKey};
use rafka_mesh_ops::{framer, InternalMeshFrame};
use rafka_mesh_transport::{IrohMeshTransport, ALPN};

/// Tag for the dedicated bidirectional QUIC stream echo handler — the data
/// plane sanity check. Sender writes a varint-length-prefixed postcard payload,
/// receiver decodes, sends it back identically. Round-trip proves the
/// bi-stream substrate works end-to-end before any real compute lands.
pub const TAG_BI_ECHO: u8 = 0x11;

use serde::{Deserialize, Serialize};
use std::{
    net::{SocketAddr, SocketAddrV4},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tracing::{info, instrument, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod deployment;
pub use deployment::Deployment;

mod load;
pub use load::{
    announce_dev_state,
    load_env_dev_from,
    parse_budget_cli_args,
    read_dev_cpu_budget,
    read_dev_ram_budget,
    BudgetCliArgs,
    LoadSampler,
    NodeLoad,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Gateway,
    Broker,
    Compute,
    Registry,
    /// Bridges multiple mesh_ids. Reads `RAFKA_BRIDGE_TARGET_MESHES` (comma-separated)
    /// to know which meshes it's expected to peer into. Emits per-mesh aggregate
    /// heartbeats (one `rafka.mesh.heartbeat` span per observed peer mesh_id) and
    /// boot-time `rafka.mesh.bridge.boot_announced` listing target meshes.
    Bridge,
    /// Operator console node. Joins the mesh just like any other node —
    /// subscribes to gossip, emits its own digest + heartbeat, accepts
    /// peer.connected from siblings. Does NOT run `run_ping_sender` because
    /// it isn't producing data-plane traffic; it's there to watch. Used by
    /// the rafka-topology-ui binary.
    Observer,
}

pub struct NodeRuntime {
    node_type: String,
    role: Role,
    cpu_budget: Option<f32>,
    ram_budget: Option<f32>,
}

impl NodeRuntime {
    pub fn new(node_type: impl Into<String>) -> Self {
        Self {
            node_type: node_type.into(),
            role: Role::Broker,
            cpu_budget: None,
            ram_budget: None,
        }
    }

    pub fn with_role(mut self, role: Role) -> Self {
        self.role = role;
        self
    }

    /// Set the node's programmatic CPU budget in cores. When set, this
    /// value flows directly into `GossipDigest.cpu_budget`. When left
    /// unset (None), `LoadSampler` falls back to sysinfo measurement
    /// (cgroup-aware on Linux, host total elsewhere).
    pub fn with_cpu_budget(mut self, cores: f32) -> Self {
        self.cpu_budget = Some(cores);
        self
    }

    /// Same shape for RAM budget in GB.
    pub fn with_ram_budget(mut self, gb: f32) -> Self {
        self.ram_budget = Some(gb);
        self
    }

    pub async fn run(self) -> Result<()> {
        let _guard = rafka_telemetry::init_telemetry(&self.node_type);
        run_node(self.node_type, self.role, self.cpu_budget, self.ram_budget).await
    }
}

struct SeedNode {
    id: PublicKey,
    addr: SocketAddr,
}

#[derive(Serialize, Deserialize)]
struct NodeIdentity {
    secret_key_hex: String,
}

type PeerRegistry = Arc<DashMap<String, Connection>>;
/// Parallel registry: peer_id → peer_mesh_id, populated from Hello frames. Used by
/// Role::Bridge to emit per-mesh aggregate heartbeats; used by all roles to make
/// peer→mesh associations observable from any code path that has the peer_id.
type MeshIdRegistry = Arc<DashMap<String, String>>;

async fn run_node(
    node_type: String,
    role: Role,
    cpu_budget: Option<f32>,
    ram_budget: Option<f32>,
) -> Result<()> {
    // mesh_id is a logical cluster identifier. Multiple physical nodes with the
    // same mesh_id form one mesh; cross-mesh peering (see feature mesh-to-mesh)
    // requires a Role::Bridge node that joins multiple mesh_ids. Defaults to
    // "default" so single-mesh dev/test work uninstrumented.
    let mesh_id = std::env::var("RAFKA_MESH_ID").unwrap_or_else(|_| "default".to_string());
    let mesh_id: &'static str = Box::leak(mesh_id.into_boxed_str());

    // node_name is the topology-ui-assigned spawn name (e.g. "broker-abc123").
    // Surfaces as a span attribute so topology-ui can show ONE NODE PER SPAWNED
    // SUBPROCESS in the Topology tab instead of collapsing all of a type into
    // a single entry. Defaults to "<unspawned>" for direct/manual launches.
    let node_name = std::env::var("RAFKA_NODE_NAME").unwrap_or_else(|_| "<unspawned>".to_string());
    let node_name: &'static str = Box::leak(node_name.into_boxed_str());

    let data_dir = std::env::var("RAFKA_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let id: u32 = rand::random();
            PathBuf::from(format!("./data/node-{id:08x}"))
        });

    let bind_addr: SocketAddrV4 = std::env::var("RAFKA_NODE_BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:0".to_string())
        .parse()
        .expect("RAFKA_NODE_BIND_ADDR must be a valid IPv4 socket address (e.g. 0.0.0.0:0)");

    let gossip_interval_ms: u64 = std::env::var("RAFKA_GOSSIP_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);

    let mdns_enable: bool = std::env::var("RAFKA_MDNS_ENABLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(true);

    let seed_nodes: Vec<SeedNode> = std::env::var("RAFKA_SEED_NODES")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .filter_map(|s| {
            let s = s.trim();
            let (id_str, addr_str) = match s.split_once('@') {
                Some(parts) => parts,
                None => {
                    eprintln!("RAFKA_SEED_NODES: expected <node_id>@<addr>, got {:?}", s);
                    return None;
                }
            };
            let id = match PublicKey::from_str(id_str) {
                Ok(pk) => pk,
                Err(e) => {
                    eprintln!("RAFKA_SEED_NODES: bad node_id {:?}: {e}", id_str);
                    return None;
                }
            };
            let addr = match addr_str.parse::<SocketAddr>() {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("RAFKA_SEED_NODES: bad addr {:?}: {e}", addr_str);
                    return None;
                }
            };
            Some(SeedNode { id, addr })
        })
        .collect();

    let node_type_str: &'static str = Box::leak(node_type.into_boxed_str());

    // Load identity before creating the iroh endpoint.
    // All boot steps run under node.ready. create_endpoint is called OUTSIDE the span
    // so iroh's background tasks don't inherit node.ready context — that would keep
    // node.ready open indefinitely and prevent it from exporting.
    let identity_path = data_dir.join("node-identity.json");
    let secret_key = if identity_path.exists() {
        load_or_mint_identity(&data_dir)
            .instrument(tracing::info_span!(
                "rafka.mesh.boot.identity_loaded",
                node_id = tracing::field::Empty,
                path = ?identity_path,
            ))
            .await?
    } else {
        load_or_mint_identity(&data_dir)
            .instrument(tracing::info_span!(
                "rafka.mesh.boot.identity_minted",
                node_id = tracing::field::Empty,
                path = ?identity_path,
            ))
            .await?
    };
    let node_id = secret_key.public().to_string();

    // Create iroh endpoint: no tracing span active here so iroh background tasks
    // are NOT attached to node.ready.
    let mut transport = create_endpoint(secret_key, bind_addr, mdns_enable).await?;

    let sockets = transport.endpoint.bound_sockets();
    let actual_bind_addr = if sockets.is_empty() {
        bind_addr.to_string()
    } else {
        sockets.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ")
    };

    // Emit all boot-chain observation spans under node.ready (all in_scope = close immediately)
    tracing::info_span!(
        "rafka.mesh.node.ready",
        node_id = %node_id,
        node_name = node_name,
        node_type = node_type_str,
        mesh_id = mesh_id,
        bind_addr = %actual_bind_addr,
        version = env!("CARGO_PKG_VERSION"),
    )
    .in_scope(|| {
        info!(gossip_interval_ms, bind_addr = %actual_bind_addr, data_dir = ?data_dir, seed_count = seed_nodes.len(), node_id = %node_id, "boot config");

        tracing::info_span!(
            "rafka.mesh.boot.endpoint_created",
            node_id = %node_id,
            bind_addr = %actual_bind_addr,
        )
        .in_scope(|| info!(node_id = %node_id, bind_addr = %actual_bind_addr, "iroh endpoint bound"));

        tracing::info_span!(
            "rafka.mesh.boot.alpn_registered",
            node_id = %node_id,
            alpn = "rafka-mesh-v1",
        )
        .in_scope(|| info!(alpn = ?std::str::from_utf8(ALPN).unwrap_or("<binary>"), "ALPN registered"));

        tracing::info_span!("rafka.mesh.boot.gossip_started", node_id = %node_id)
            .in_scope(|| info!(gossip_interval_ms, mdns_enable, "gossip discovery started"));

        tracing::info_span!("rafka.mesh.boot.accept_loop_started", node_id = %node_id)
            .in_scope(|| info!("accept loop running"));

        info!(node_id = %node_id, "boot complete, idling");
    });
    // node.ready closes here — tiny span, exports immediately, no iroh internals inside

    let peer_registry: PeerRegistry = Arc::new(DashMap::new());
    let mesh_id_registry: MeshIdRegistry = Arc::new(DashMap::new());
    // Initialize the process-wide MeshCounters singleton. Every send/recv site
    // reads via mesh_counters() so we don't have to thread an Arc through 3
    // layers of accept loops. run_gossip reads it when assembling each digest.
    let _ = mesh_counters();

    // Background pruner: drops stale entries from the process-global
    // `live_digests` and `topic_membership` maps. Without it both grow
    // monotonically with the count of unique node_ids ever observed. See
    // `run_staleness_pruner` for the TTL semantics.
    tokio::spawn(run_staleness_pruner());

    // Bootstrap iroh-gossip on the existing endpoint. Topic ID = blake3(mesh_id)
    // so every node in the same mesh joins the same gossip topic. Real gossip
    // plane — replaces the previously-lying rafka.mesh.boot.gossip_started span
    // that was just mdns discovery in disguise.
    let gossip = iroh_gossip::net::Gossip::builder().spawn(transport.endpoint.clone());
    let topic_bytes: [u8; 32] = *blake3::hash(mesh_id.as_bytes()).as_bytes();
    let topic_id = iroh_gossip::proto::TopicId::from_bytes(topic_bytes);
    tracing::info_span!(
        "rafka.mesh.gossip.subscribed",
        node_id = %node_id,
        mesh_id = mesh_id,
        topic_id = %hex::encode(topic_bytes),
    )
    .in_scope(|| {
        info!(mesh_id, topic_id = %hex::encode(topic_bytes), "iroh-gossip subscribed (HyParView+Plumtree)");
    });

    let gossip_handle = {
        let node_id_g = node_id.clone();
        let registry_for_digest = Arc::clone(&peer_registry);
        let gossip_clone = gossip.clone();
        let node_type_g = node_type_str.to_string();
        tokio::spawn(run_gossip(
            gossip_clone,
            topic_id,
            node_id_g,
            mesh_id,
            node_name,
            node_type_g,
            gossip_interval_ms,
            registry_for_digest,
            mesh_id, // primary task: topic_label = mesh_id (digests filed under our own mesh)
            cpu_budget,
            ram_budget,
        ))
    };

    // RAFKA_OBSERVER_MESHES (admin-ui) AND RAFKA_BRIDGE_TARGET_MESHES (bridges):
    // comma-separated list of ADDITIONAL meshes to subscribe to (beyond our
    // primary RAFKA_MESH_ID). Both env vars feed the same multi-topic-join
    // path — observer/bridge is just a labeling distinction. Each extra
    // topic gets its own run_gossip task that writes into the process-wide
    // live_digests() map and broadcasts our own digest on that topic too,
    // so bridges genuinely appear as members of every mesh they bridge.
    let extra_meshes_combined = {
        let observer = std::env::var("RAFKA_OBSERVER_MESHES").unwrap_or_default();
        let bridge_targets = std::env::var("RAFKA_BRIDGE_TARGET_MESHES").unwrap_or_default();
        let combined = if observer.is_empty() {
            bridge_targets
        } else if bridge_targets.is_empty() {
            observer
        } else {
            format!("{observer},{bridge_targets}")
        };
        if combined.is_empty() { None } else { Some(combined) }
    };
    if let Some(extra) = extra_meshes_combined {
        for extra_mesh in extra.split(',') {
            let extra_mesh = extra_mesh.trim();
            if extra_mesh.is_empty() || extra_mesh == mesh_id {
                continue;
            }
            let extra_mesh_static: &'static str =
                Box::leak(extra_mesh.to_string().into_boxed_str());
            let extra_topic_bytes: [u8; 32] =
                *blake3::hash(extra_mesh_static.as_bytes()).as_bytes();
            let extra_topic_id =
                iroh_gossip::proto::TopicId::from_bytes(extra_topic_bytes);
            tracing::info_span!(
                "rafka.mesh.gossip.subscribed_extra",
                node_id = %node_id,
                extra_mesh_id = extra_mesh_static,
                topic_id = %hex::encode(extra_topic_bytes),
            )
            .in_scope(|| {
                info!(extra_mesh = extra_mesh_static, "iroh-gossip extra topic subscribed (observer mode)");
            });
            let node_id_g = node_id.clone();
            let registry_for_digest = Arc::clone(&peer_registry);
            let gossip_clone = gossip.clone();
            let node_type_g = node_type_str.to_string();
            // CRITICAL: pass our PRIMARY mesh_id, not the extra topic's mesh_id.
            // The digest describes the node's identity (primary mesh); the
            // topic is just the broadcast channel. Without this, multiple
            // run_gossip tasks for the same node race to overwrite
            // live_digests[node_id] with conflicting mesh_id values.
            tokio::spawn(run_gossip(
                gossip_clone,
                extra_topic_id,
                node_id_g,
                mesh_id,          // digest's mesh_id stays primary (node's identity)
                node_name,
                node_type_g,
                gossip_interval_ms,
                registry_for_digest,
                extra_mesh_static, // topic_label = actual subscription topic (NOT primary)
                cpu_budget,
                ram_budget,
            ));
        }
    }

    let mdns_rx = std::mem::replace(
        &mut transport.mdns_discovered,
        tokio::sync::mpsc::channel(1).1,
    );

    // Role::Bridge: emit boot-announced span listing target meshes so operators see
    // immediately which meshes this bridge is supposed to span.
    let is_bridge = matches!(role, Role::Bridge);
    if is_bridge {
        let target_meshes = std::env::var("RAFKA_BRIDGE_TARGET_MESHES")
            .unwrap_or_else(|_| "".to_string());
        tracing::info_span!(
            "rafka.mesh.bridge.boot_announced",
            node_id = %node_id,
            mesh_id = mesh_id,
            target_meshes = %target_meshes,
        )
        .in_scope(|| {
            info!(target_meshes = %target_meshes, "bridge boot announced");
        });
    }

    let accept_handle =
        start_accept_loop(&transport, node_id.clone(), mesh_id, node_type_str, Arc::clone(&peer_registry), Arc::clone(&mesh_id_registry), gossip.clone()).await;

    // Dedicated bidirectional QUIC stream echo accept loop — the data plane
    // sanity surface for the new framed wire grammar (tag 0x11). Lives in its
    // own task because it accepts NEW bi-streams as they arrive, independent
    // of the per-peer frame readers.
    let bi_echo_handle = {
        let endpoint = transport.endpoint.clone();
        let node_id_be = node_id.clone();
        tokio::spawn(run_bi_echo_acceptor(endpoint, node_id_be))
    };

    let dial_handle = if !seed_nodes.is_empty() {
        let node_id_dial = node_id.clone();
        let endpoint = transport.endpoint.clone();
        let registry = Arc::clone(&peer_registry);
        let mesh_reg = Arc::clone(&mesh_id_registry);
        Some(tokio::spawn(dial_seeds(endpoint, seed_nodes, node_id_dial, mesh_id, node_type_str, registry, mesh_reg)))
    } else {
        None
    };

    let mdns_handle = {
        let node_id_mdns = node_id.clone();
        let endpoint = transport.endpoint.clone();
        let registry = Arc::clone(&peer_registry);
        let mesh_reg = Arc::clone(&mesh_id_registry);
        tokio::spawn(watch_mdns(mdns_rx, endpoint, node_id_mdns, mesh_id, node_type_str, registry, mesh_reg))
    };

    let heartbeat_handle = {
        let registry = Arc::clone(&peer_registry);
        let mesh_reg = Arc::clone(&mesh_id_registry);
        tokio::spawn(run_heartbeat(node_id.clone(), mesh_id, node_name, is_bridge, registry, mesh_reg))
    };

    // No application-level ping/pong: iroh-quinn owns connection liveness
    // (keep-alive + idle timeout). rafka does not hand-roll a heartbeat
    // (Golden Principle #1). run_ping_sender removed entirely.

    let stopping_reason = wait_for_signal().await;

    tracing::info_span!(
        "rafka.mesh.node.stopping",
        node_id = %node_id,
        reason = stopping_reason,
    )
    .in_scope(|| {
        info!("node stopping");
    });

    accept_handle.abort();
    heartbeat_handle.abort();
    mdns_handle.abort();
    gossip_handle.abort();
    bi_echo_handle.abort();
    if let Some(h) = dial_handle {
        h.abort();
    }

    Ok(())
}

/// Placeholder no-op — per-connection bi-stream reader is now spawned per peer
/// connection inside start_accept_loop alongside run_frame_reader. Kept as a
/// long-sleeping task so the parent abort handle stays valid; remove on next
/// cleanup pass.
async fn run_bi_echo_acceptor(endpoint: iroh::Endpoint, node_id: String) {
    let _ = &endpoint;
    let _ = &node_id;
    std::future::pending::<()>().await;
}

/// Per-connection bi-stream echo reader. Loops on `conn.accept_bi()`, reads a
/// complete framed payload (tag + varint length + postcard bytes), and if tag
/// is `TAG_BI_ECHO` (0x11), echoes the same bytes back on the send half and
/// finishes. Other tags are dropped with a span. This is the data-plane sanity
/// surface — proves bi-stream open + read + write + close cycle works end-to-
/// end before any real broker/compute logic lands.
async fn run_bi_echo_reader(conn: iroh::endpoint::Connection, own_node_id: String, peer_id_str: String) {
    loop {
        let (mut send, mut recv) = match conn.accept_bi().await {
            Ok(pair) => pair,
            Err(_) => break, // connection closed
        };
        let bytes = match recv.read_to_end(64 * 1024).await {
            Ok(b) => b,
            Err(e) => {
                tracing::trace_span!(
                    "rafka.mesh.bi.read_failed",
                    node_id = %own_node_id,
                    peer_id = %peer_id_str,
                    error = %e,
                )
                .in_scope(|| tracing::trace!(error = %e, "bi-stream read failed"));
                continue;
            }
        };
        if bytes.is_empty() {
            continue;
        }
        let tag = bytes[0];
        if tag != TAG_BI_ECHO {
            tracing::trace_span!(
                "rafka.mesh.bi.unknown_tag",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                tag = tag as i64,
                size_bytes = bytes.len() as i64,
            )
            .in_scope(|| tracing::trace!(tag, "bi-stream unknown tag — dropping"));
            continue;
        }
        let recv_span = tracing::trace_span!(
            "rafka.mesh.bi.echo_received",
            node_id = %own_node_id,
            peer_id = %peer_id_str,
            size_bytes = bytes.len() as i64,
        );
        recv_span.in_scope(|| tracing::trace!(size = bytes.len(), "bi echo received"));

        // Echo back identical bytes (tag + varint + payload).
        if let Err(e) = send.write_all(&bytes).await {
            tracing::trace_span!(
                "rafka.mesh.bi.write_failed",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                error = %e,
            )
            .in_scope(|| tracing::trace!(error = %e, "bi echo write failed"));
            continue;
        }
        if let Err(e) = send.finish() {
            tracing::trace_span!(
                "rafka.mesh.bi.finish_failed",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                error = %e,
            )
            .in_scope(|| tracing::trace!(error = %e, "bi echo finish failed"));
            continue;
        }
        tracing::trace_span!(
            "rafka.mesh.bi.echo_sent",
            node_id = %own_node_id,
            peer_id = %peer_id_str,
            size_bytes = bytes.len() as i64,
        )
        .in_scope(|| tracing::trace!(size = bytes.len(), "bi echo sent"));
    }
}

/// Standalone bi-stream client: open a bi-stream to `peer`, write a framed
/// payload (tag 0x11), read echo back, return the round-trip bytes. Used by
/// the bi-stream-echo test in the CLI test runner — proves the dedicated data
/// plane works without needing live broker / compute / message types yet.
pub async fn bi_echo_roundtrip(
    endpoint: &iroh::Endpoint,
    peer: impl Into<iroh::EndpointAddr>,
    payload: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let conn = endpoint.connect(peer, ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    let frame = framer::encode(TAG_BI_ECHO, &payload);
    send.write_all(&frame).await?;
    send.finish()?;
    let echoed = recv.read_to_end(64 * 1024).await?;
    Ok(echoed)
}

/// State digest broadcast over iroh-gossip every gossip_interval_ms. Tiny
/// payload (≤200 bytes after postcard) so it fits well under QUIC datagram MTU
/// (~1200 bytes safe). Plumtree's spanning tree disseminates these efficiently
/// across the mesh; HyParView keeps membership churn graceful.
///
/// `frames_sent_total` + `frames_recv_total` are monotonic counters. The
/// operator UI subscribes to these via gossip and computes throughput as
/// (delta between two consecutive digests) / (delta wall_time_ms).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GossipDigest {
    pub node_id: String,
    pub node_name: String,
    pub mesh_id: String,
    pub node_type: String,
    pub peer_count: u64,
    /// Hex-encoded NodeIds of every peer this node has an active iroh
    /// connection to. The operator UI builds real edges from this list
    /// (cross-referenced against other digests' node_ids).
    pub peer_ids: Vec<String>,
    pub frames_sent_total: u64,
    pub frames_recv_total: u64,
    pub wall_time_ms: u64,
    /// Process CPU usage in cores (e.g. 2.4 = 2.4 cores' worth of work).
    /// Measured via sysinfo; may be overridden by RAFKA_DEV_CPU_USED in dev.
    pub cpu_used: f32,
    /// Process CPU budget in cores (cgroup-aware on Linux, host cpus
    /// elsewhere). May be overridden by RAFKA_DEV_CPU_BUDGET in dev.
    pub cpu_budget: f32,
    /// Resident memory in GB. Measured via sysinfo (this is the same number
    /// `top` shows as RES). May be overridden by RAFKA_DEV_RAM_USED in dev.
    pub ram_used: f32,
    /// RAM budget in GB (cgroup-aware on Linux, host total elsewhere).
    /// May be overridden by RAFKA_DEV_RAM_BUDGET in dev.
    pub ram_budget: f32,
}

/// Process-wide monotonic counters. Incremented at every uni-stream / bi-stream
/// send + receive site. Read by `run_gossip` when assembling each digest so the
/// operator UI sees live throughput without ever querying Jaeger.
#[derive(Default)]
pub struct MeshCounters {
    pub frames_sent: std::sync::atomic::AtomicU64,
    pub frames_recv: std::sync::atomic::AtomicU64,
}

/// Process-wide singleton. Avoids threading `Arc<MeshCounters>` through every
/// reader/sender helper, which would cascade through 3+ levels of accept loops.
static MESH_COUNTERS: std::sync::OnceLock<Arc<MeshCounters>> = std::sync::OnceLock::new();

pub fn mesh_counters() -> &'static Arc<MeshCounters> {
    MESH_COUNTERS.get_or_init(|| Arc::new(MeshCounters::default()))
}

/// Process-global map of every GossipDigest this node has received from
/// peers via iroh-gossip. Keyed by node_id (hex). Written by `run_gossip`
/// on every Event::Received. topology-ui reads from this directly to
/// render /api/topology + /api/heartbeats — ZERO Jaeger dependency, the
/// mesh IS the topology source of truth.
static LIVE_DIGESTS: std::sync::OnceLock<Arc<DashMap<String, GossipDigest>>> =
    std::sync::OnceLock::new();

pub fn live_digests() -> &'static Arc<DashMap<String, GossipDigest>> {
    LIVE_DIGESTS.get_or_init(|| Arc::new(DashMap::new()))
}

/// Process-local receive timestamps keyed by node_id. Updated at every site
/// that inserts into `live_digests` — once on our own self-injection and once
/// per inbound gossip event. The staleness pruner compares `now - last_seen_ms`
/// rather than `now - digest.wall_time_ms` to avoid clock-skew false positives:
/// a peer whose wall clock is drifted by 2 minutes would otherwise appear stale
/// even though we just received a live digest from it.
///
/// Uses a Mutex<HashMap> rather than DashMap to avoid holding a DashMap shard
/// lock on live_digests while also needing a lock on last_seen_ms — a two-map
/// DashMap access pattern that can deadlock under DashMap's custom RwLock impl.
static LAST_SEEN_MS: std::sync::OnceLock<
    Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>>,
> = std::sync::OnceLock::new();

pub fn last_seen_ms() -> &'static Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>> {
    LAST_SEEN_MS.get_or_init(|| Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())))
}

/// Per-topic membership ledger. For each gossip topic we subscribe to,
/// records the node_ids whose digests we've actually received on that
/// topic in the last gossip cycle. This is the AUTHORITATIVE answer to
/// "which nodes belong to mesh X" — built from observed traffic, not
/// from inferred peer_ids (which conflate iroh-mdns connections with
/// gossip topic membership).
static TOPIC_MEMBERSHIP: std::sync::OnceLock<
    Arc<DashMap<String, std::collections::HashSet<String>>>,
> = std::sync::OnceLock::new();

pub fn topic_membership(
) -> &'static Arc<DashMap<String, std::collections::HashSet<String>>> {
    TOPIC_MEMBERSHIP.get_or_init(|| Arc::new(DashMap::new()))
}

/// Default staleness window for the process-global mesh-state pruner. A
/// `GossipDigest` whose `wall_time_ms` is older than this is treated as
/// "the source node is gone" and removed from `live_digests` +
/// `topic_membership`. At the default 2s gossip cadence this is 15 missed
/// cycles — well past any reasonable flake. Override with `RAFKA_STALENESS_MS`.
const DEFAULT_STALENESS_MS: u64 = 30_000;

/// Background staleness pruner for the process-global `live_digests` +
/// `topic_membership` maps. Without it, both grow monotonically with the
/// count of unique node_ids ever observed — every cluster restart, peer
/// churn, or admin-ui respawn adds entries that never leave.
///
/// Sweeps every 5 seconds, removes digests older than `RAFKA_STALENESS_MS`
/// (default 30s), then drops the same node_ids from every topic's
/// membership set. The receiving node's OWN digest is refreshed every
/// gossip tick via the self-injection at the bottom of `run_gossip`, so
/// it never goes stale and is never pruned.
async fn run_staleness_pruner() {
    let staleness_ms: u64 = std::env::var("RAFKA_STALENESS_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_STALENESS_MS);

    let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(5_000));
    loop {
        tick.tick().await;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Collect all known node_ids first (releasing shard locks before the
        // staleness check to avoid holding DashMap shard locks across a second
        // DashMap lookup, which can deadlock with parking_lot's non-reentrant
        // RwLock). Compare against local receive-time (from last_seen_ms) rather
        // than the sender's wall_time_ms to avoid clock-skew false positives.
        let all_keys: Vec<String> = live_digests()
            .iter()
            .map(|e| e.key().clone())
            .collect();

        let stale: Vec<String> = {
            let seen = last_seen_ms().lock().unwrap();
            all_keys
                .into_iter()
                .filter(|node_id| {
                    let received = seen.get(node_id).copied().unwrap_or(0);
                    now_ms.saturating_sub(received) > staleness_ms
                })
                .collect()
        };

        if stale.is_empty() {
            continue;
        }

        for node_id in &stale {
            live_digests().remove(node_id);
            last_seen_ms().lock().unwrap().remove(node_id);
        }

        // Topic membership is the dependent view — same node_ids that
        // disappeared from live_digests must also leave every topic.
        for mut topic_entry in topic_membership().iter_mut() {
            for node_id in &stale {
                topic_entry.value_mut().remove(node_id);
            }
        }

        tracing::info_span!(
            "rafka.mesh.staleness.pruned",
            removed = stale.len() as i64,
            staleness_ms = staleness_ms as i64,
            "otel.kind" = "internal",
        )
        .in_scope(|| {
            info!(
                removed = stale.len(),
                staleness_ms,
                "pruned stale digests + topic membership"
            );
        });
    }
}

/// Last N frames this node received over its data plane. Pushed by
/// `run_frame_reader` after each successful decode. Bounded to 1000 entries
/// (oldest dropped on overflow). Powers the admin-ui Messages tab — live
/// view of mesh traffic flowing through THIS node.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MeshMessage {
    pub ts_ms: u64,
    pub from_peer_id: String,
    pub frame_kind: String,
    pub bytes: usize,
    /// Human-readable decoded summary of the frame contents — e.g.
    /// "Ping{org_id=0}", "Hello{mesh_id=mesh-a, node_type=broker}".
    /// `<decode_failed>` if postcard couldn't parse the bytes.
    pub summary: String,
}

static MESSAGE_RING: std::sync::OnceLock<
    Arc<std::sync::Mutex<std::collections::VecDeque<MeshMessage>>>,
> = std::sync::OnceLock::new();

pub fn message_ring(
) -> &'static Arc<std::sync::Mutex<std::collections::VecDeque<MeshMessage>>> {
    MESSAGE_RING.get_or_init(|| {
        Arc::new(std::sync::Mutex::new(
            std::collections::VecDeque::with_capacity(1024),
        ))
    })
}

fn push_message(from_peer_id: &str, frame_kind: &str, bytes: usize, summary: String) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut g = message_ring().lock().unwrap();
    if g.len() >= 1000 {
        g.pop_front();
    }
    g.push_back(MeshMessage {
        ts_ms: now_ms,
        from_peer_id: from_peer_id.to_string(),
        frame_kind: frame_kind.to_string(),
        bytes,
        summary,
    });
}

async fn run_gossip(
    gossip: iroh_gossip::net::Gossip,
    topic_id: iroh_gossip::proto::TopicId,
    node_id: String,
    mesh_id: &'static str,
    node_name: &'static str,
    node_type: String,
    interval_ms: u64,
    registry: PeerRegistry,
    // topic_label is the key under which received digests get filed in
    // topic_membership(). For PRIMARY-topic tasks this MUST equal mesh_id
    // (so a broker's mesh-a digests live under topic_membership["mesh-a"]).
    // For EXTRA-topic tasks (observer/bridge subscribers) this MUST equal
    // the extra topic's mesh-name, NOT the node's primary mesh_id —
    // otherwise admin-ui (primary "admin-ui") would file every received
    // digest under topic_membership["admin-ui"] and the edge-builder
    // would produce O(n²) spurious cross-mesh pairs (red-team R2 2026-05-21).
    topic_label: &'static str,
    cpu_budget: Option<f32>,
    ram_budget: Option<f32>,
) {
    let counters = mesh_counters();
    let load_sampler = LoadSampler::new(cpu_budget, ram_budget, None, None);
    use futures_lite::StreamExt;
    use iroh_gossip::api::Event;
    // Subscribe with no bootstrap peers — peers self-discover via the iroh
    // endpoint's mdns. Once peers connect via the underlying QUIC, gossip
    // forms its active spanning tree organically.
    let topic = match gossip.subscribe(topic_id, Vec::new()).await {
        Ok(t) => t,
        Err(e) => {
            tracing::info_span!(
                "rafka.mesh.gossip.subscribe_failed",
                node_id = %node_id,
                error = %e,
            )
            .in_scope(|| info!(error = %e, "gossip subscribe failed; gossip plane disabled for this node"));
            return;
        }
    };
    let (sender, mut receiver) = topic.split();
    let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(interval_ms));
    let mut last_digest: Option<GossipDigest> = None;
    let mut last_broadcast_time: u64 = 0;
    let mut ticks_since_sample = 10;
    let mut current_load = load_sampler.sample();
    let mut joined_peers = std::collections::HashSet::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Feed mdns-discovered peers to gossip so the swarm forms.
                // join_peers triggers QUIC handshakes, so doing it every 100ms
                // for already-connected peers creates a massive CPU storm.
                let mut new_peers = Vec::new();
                for peer in registry.iter() {
                    if !joined_peers.contains(peer.key()) {
                        if let Ok(id) = iroh::EndpointId::from_str(peer.key()) {
                            new_peers.push(id);
                            joined_peers.insert(peer.key().clone());
                        }
                    }
                }
                joined_peers.retain(|p| registry.contains_key(p));
                if !new_peers.is_empty() {
                    let _ = sender.join_peers(new_peers).await;
                }
                use std::sync::atomic::Ordering;
                let peer_ids: Vec<String> = registry.iter().map(|e| e.key().clone()).collect();
                
                ticks_since_sample += 1;
                if ticks_since_sample >= 10 {
                    current_load = load_sampler.sample();
                    ticks_since_sample = 0;
                }
                let load = current_load;
                let digest = GossipDigest {
                    node_id: node_id.clone(),
                    node_name: node_name.to_string(),
                    mesh_id: mesh_id.to_string(),
                    node_type: node_type.clone(),
                    peer_count: registry.len() as u64,
                    peer_ids,
                    frames_sent_total: counters.frames_sent.load(Ordering::Relaxed),
                    frames_recv_total: counters.frames_recv.load(Ordering::Relaxed),
                    wall_time_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                    cpu_used: load.cpu_used,
                    cpu_budget: load.cpu_budget,
                    ram_used: load.ram_used,
                    ram_budget: load.ram_budget,
                };
                
                let mut should_broadcast = false;
                if let Some(last) = &last_digest {
                    if last.peer_count != digest.peer_count || last.peer_ids != digest.peer_ids {
                        should_broadcast = true;
                    }
                    if last.frames_sent_total != digest.frames_sent_total || last.frames_recv_total != digest.frames_recv_total {
                        should_broadcast = true;
                    }
                    if (last.cpu_used - digest.cpu_used).abs() > 0.05 * last.cpu_budget.max(1.0) {
                        should_broadcast = true;
                    }
                    if (last.ram_used - digest.ram_used).abs() > 0.05 * last.ram_budget.max(1.0) {
                        should_broadcast = true;
                    }
                    if digest.wall_time_ms.saturating_sub(last_broadcast_time) >= 30_000 {
                        should_broadcast = true;
                    }
                } else {
                    should_broadcast = true;
                }

                // Red-team R3 fix: file our own digest into live_digests +
                // topic_membership so we appear in /api/topology and
                // /api/heartbeats from our own perspective. iroh-gossip does
                // NOT echo broadcasts back to the sender, so without this
                // self-injection an admin-ui observer (or any node) is
                // invisible in its own UI.
                live_digests().insert(digest.node_id.clone(), digest.clone());
                // Track local receive-time (not sender wall-clock) so the
                // staleness pruner is immune to clock skew between nodes.
                {
                    let now_recv = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    last_seen_ms().lock().unwrap().insert(digest.node_id.clone(), now_recv);
                }
                topic_membership()
                    .entry(topic_label.to_string())
                    .or_insert_with(std::collections::HashSet::new)
                    .insert(digest.node_id.clone());

                if !should_broadcast {
                    continue;
                }

                last_digest = Some(digest.clone());
                last_broadcast_time = digest.wall_time_ms;

                let payload = match postcard::to_allocvec(&digest) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let size = payload.len();
                if let Err(e) = sender.broadcast(payload.into()).await {
                    tracing::info_span!(
                        "rafka.mesh.gossip.broadcast_failed",
                        node_id = %node_id,
                        error = %e,
                    )
                    .in_scope(|| info!(error = %e, "gossip broadcast failed"));
                } else {
                    tracing::info_span!(
                        "rafka.mesh.gossip.broadcast",
                        node_id = %node_id,
                        mesh_id = mesh_id,
                        size_bytes = size as i64,
                    )
                    .in_scope(|| info!(size_bytes = size, "gossip digest broadcast"));
                }
            }
            event = receiver.next() => {
                let Some(event) = event else { break };
                let event = match event {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::info_span!(
                            "rafka.mesh.gossip.receive_failed",
                            node_id = %node_id,
                            error = %e,
                        )
                        .in_scope(|| info!(error = %e, "gossip receive event errored"));
                        continue;
                    }
                };
                if let Event::Received(msg) = event {
                    let size = msg.content.len();
                    let from = msg.delivered_from.to_string();
                    let digest: Option<GossipDigest> = postcard::from_bytes(&msg.content).ok();
                    // Insert into the process-global digest map so the operator
                    // UI (or anyone in-process) can read live mesh state with
                    // zero Jaeger dependency. The mesh IS the topology.
                    if let Some(d) = &digest {
                        live_digests().insert(d.node_id.clone(), d.clone());
                        // Record local receive-time for clock-skew-safe staleness pruning.
                        {
                            let now_recv = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                            last_seen_ms().lock().unwrap().insert(d.node_id.clone(), now_recv);
                        }
                        // Authoritative topic-membership: we received d's digest
                        // ON THIS TOPIC (= the `mesh_id` param of this run_gossip
                        // task), so d is a member of that topic's swarm.
                        // Edges in /api/topology are built from this map's
                        // intersections rather than from peer_ids (mdns junk).
                        topic_membership()
                            .entry(topic_label.to_string())
                            .or_insert_with(std::collections::HashSet::new)
                            .insert(d.node_id.clone());
                    }
                    let summary = digest
                        .as_ref()
                        .map(|d| format!("node={}/peers={}", d.node_name, d.peer_count))
                        .unwrap_or_else(|| "<decode_failed>".to_string());
                    // TRACE not INFO: this span fires per-frame. At 18 nodes
                    // with Plumtree eager-push, each digest arrives ~17 times
                    // (once per peer in the spanning-tree fanout), so logging
                    // here at INFO produces ~2600 events/sec per node and was
                    // the dominant CPU cost (host pegged at 95-99% during
                    // bootstrap-2-mesh). State transitions stay at INFO; this
                    // per-message event lives at TRACE.
                    tracing::trace_span!(
                        "rafka.mesh.gossip.received",
                        node_id = %node_id,
                        from_peer = %from,
                        size_bytes = size as i64,
                        digest = %summary,
                    )
                    .in_scope(|| tracing::trace!(from = %from, size_bytes = size, digest = %summary, "gossip digest received"));
                }
            }
        }
    }
}

#[instrument(skip_all)]
async fn dial_seeds(
    endpoint: iroh::Endpoint,
    seeds: Vec<SeedNode>,
    own_node_id: String,
    own_mesh_id: &'static str,
    own_node_type: &'static str,
    registry: PeerRegistry,
    mesh_id_registry: MeshIdRegistry,
) {
    const MAX_ATTEMPTS: u32 = 10;
    const BASE_DELAY_MS: u64 = 1_000;
    const MAX_DELAY_MS: u64 = 30_000;

    for seed in seeds {
        let peer_id_str = seed.id.to_string();
        let endpoint = endpoint.clone();
        let own_node_id = own_node_id.clone();
        let registry = Arc::clone(&registry);
        let mesh_id_registry = Arc::clone(&mesh_id_registry);

        // Each seed dials in its own task so a slow/down seed doesn't block
        // subsequent seeds from connecting.
        tokio::spawn(async move {
            tracing::info_span!(
                "rafka.mesh.peer.discovered",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                peer_node_type = "unknown",
                source = "seed",
            )
            .in_scope(|| {
                info!(peer_id = %peer_id_str, addr = %seed.addr, "peer discovered via seed list");
            });

            let endpoint_addr = EndpointAddr::new(seed.id).with_ip_addr(seed.addr);
            let mut attempt = 0u32;
            loop {
                if attempt >= MAX_ATTEMPTS {
                    tracing::info_span!(
                        "rafka.mesh.seed.giveup",
                        node_id = %own_node_id,
                        peer_id = %peer_id_str,
                        attempts = attempt as i64,
                    )
                    .in_scope(|| {
                        info!(peer_id = %peer_id_str, attempts = attempt, "seed gave up after max attempts");
                    });
                    break;
                }

                match endpoint.connect(endpoint_addr.clone(), ALPN).await {
                    Ok(conn) => {
                        tracing::info_span!(
                            "rafka.mesh.peer.connected",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            peer_node_type = "unknown",
                            direction = "outbound",
                        )
                        .in_scope(|| {
                            info!(peer_id = %peer_id_str, "peer connected (outbound)");
                        });

                        if let Some((_, old_conn)) = registry.remove(&peer_id_str) {
                            old_conn.close(0u32.into(), b"superseded by new connection");
                        }
                        registry.insert(peer_id_str.clone(), conn.clone());

                        send_hello(&conn, &own_node_id, own_mesh_id, own_node_type, &peer_id_str).await;

                        let conn_bi = conn.clone();
                        let own_bi = own_node_id.clone();
                        let peer_bi = peer_id_str.clone();
                        tokio::spawn(run_bi_echo_reader(conn_bi, own_bi, peer_bi));

                        let own = own_node_id.clone();
                        let reg = Arc::clone(&registry);
                        let mesh_reg = Arc::clone(&mesh_id_registry);
                        tokio::spawn(run_frame_reader(own, own_mesh_id, peer_id_str.clone(), conn, reg, mesh_reg));
                        break;
                    }
                    Err(e) => {
                        attempt += 1;
                        let delay_ms = (BASE_DELAY_MS * 2u64.pow(attempt.min(5))).min(MAX_DELAY_MS);
                        tracing::info_span!(
                            "rafka.mesh.seed.retry",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            attempt = attempt as i64,
                            delay_ms = delay_ms as i64,
                        )
                        .in_scope(|| {
                            info!(peer_id = %peer_id_str, attempt, delay_ms, error = %e, "seed dial failed, retrying");
                        });
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        });
    }
}

async fn watch_mdns(
    mut rx: tokio::sync::mpsc::Receiver<String>,
    endpoint: iroh::Endpoint,
    own_node_id: String,
    own_mesh_id: &'static str,
    own_node_type: &'static str,
    registry: PeerRegistry,
    mesh_id_registry: MeshIdRegistry,
) {
    while let Some(peer_id_str) = rx.recv().await {
        let peer_id = match peer_id_str.parse::<PublicKey>() {
            Ok(pk) => pk,
            Err(_) => continue,
        };

        if registry.contains_key(&peer_id_str) {
            continue;
        }

        tracing::info_span!(
            "rafka.mesh.peer.discovered",
            node_id = %own_node_id,
            peer_id = %peer_id_str,
            peer_node_type = "unknown",
            source = "mdns",
        )
        .in_scope(|| {
            info!(peer_id = %peer_id_str, "peer discovered via mdns");
        });

        let endpoint_clone = endpoint.clone();
        let own = own_node_id.clone();
        let reg = Arc::clone(&registry);
        let mesh_reg = Arc::clone(&mesh_id_registry);
        tokio::spawn(async move {
            match endpoint_clone.connect(peer_id, ALPN).await {
                Ok(conn) => {
                    tracing::info_span!(
                        "rafka.mesh.peer.connected",
                        node_id = %own,
                        peer_id = %peer_id_str,
                        peer_node_type = "unknown",
                        direction = "outbound",
                    )
                    .in_scope(|| {
                        info!(peer_id = %peer_id_str, "peer connected via mdns (outbound)")
                    });

                    if let Some((_, old_conn)) = reg.remove(&peer_id_str) {
                        old_conn.close(0u32.into(), b"superseded by new connection");
                    }
                    reg.insert(peer_id_str.clone(), conn.clone());

                    send_hello(&conn, &own, own_mesh_id, own_node_type, &peer_id_str).await;

                    let conn_bi = conn.clone();
                    let own_bi = own.clone();
                    let peer_bi = peer_id_str.clone();
                    tokio::spawn(run_bi_echo_reader(conn_bi, own_bi, peer_bi));

                    run_frame_reader(own, own_mesh_id, peer_id_str.clone(), conn, reg, mesh_reg).await;
                }
                Err(e) => {
                    info!(peer_id = %peer_id_str, error = %e, "mdns dial failed");
                }
            }
        });
    }
}

#[instrument(skip_all)]
async fn start_accept_loop(
    transport: &IrohMeshTransport,
    own_node_id: String,
    own_mesh_id: &'static str,
    own_node_type: &'static str,
    registry: PeerRegistry,
    mesh_id_registry: MeshIdRegistry,
    gossip: iroh_gossip::net::Gossip,
) -> tokio::task::JoinHandle<()> {
    let endpoint = transport.endpoint.clone();
    tokio::spawn(async move {
        loop {
            match endpoint.accept().await {
                Some(incoming) => {
                    let own_id = own_node_id.clone();
                    let reg = Arc::clone(&registry);
                    let mesh_reg = Arc::clone(&mesh_id_registry);
                    let gossip = gossip.clone();
                    tokio::spawn(async move {
                        let conn = match incoming.await {
                            Ok(c) => c,
                            Err(e) => {
                                info!(error = %e, "accept: incoming await failed");
                                return;
                            }
                        };
                        let alpn = conn.alpn();
                        if alpn == iroh_gossip::ALPN {
                            // Route to gossip — its handle_connection drives the
                            // HyParView state machine on this connection.
                            let peer_id = conn.remote_id().to_string();
                            tracing::info_span!(
                                "rafka.mesh.gossip.accept",
                                node_id = %own_id,
                                peer_id = %peer_id,
                            )
                            .in_scope(|| info!(peer_id = %peer_id, "gossip accept"));
                            if let Err(e) = gossip.handle_connection(conn).await {
                                info!(peer_id = %peer_id, error = %e, "gossip handle_connection failed");
                            }
                            return;
                        }
                        // Default: our rafka-mesh-v1 ALPN
                        {
                            let peer_id = conn.remote_id().to_string();
                            tracing::info_span!(
                                "rafka.mesh.peer.connected",
                                node_id = %own_id,
                                peer_id = %peer_id,
                                peer_node_type = "unknown",
                                direction = "inbound",
                            )
                            .in_scope(|| {
                                info!(peer_id = %peer_id, "peer connected (inbound)");
                            });

                            if let Some((_, old_conn)) = reg.remove(&peer_id) {
                                old_conn.close(0u32.into(), b"superseded by new connection");
                            }
                            reg.insert(peer_id.clone(), conn.clone());

                            send_hello(&conn, &own_id, own_mesh_id, own_node_type, &peer_id).await;

                            // Per-connection bi-stream echo reader (data plane sanity).
                            let conn_bi = conn.clone();
                            let own_bi = own_id.clone();
                            let peer_bi = peer_id.clone();
                            tokio::spawn(run_bi_echo_reader(conn_bi, own_bi, peer_bi));

                            run_frame_reader(own_id, own_mesh_id, peer_id.clone(), conn, reg, mesh_reg).await;
                        }
                    });
                }
                None => {
                    info!("accept loop: endpoint closed");
                    break;
                }
            }
        }
    })
}

/// Send a `Hello` frame to a freshly-connected peer carrying our mesh_id + node_type.
/// Peer's run_frame_reader handles it: emits a `rafka.mesh.peer.hello_received` span,
/// plus a `rafka.mesh.cross.peer_connected` span if the mesh_ids differ — the substrate
/// signal for cross-mesh peering per feature `mesh-to-mesh`.
async fn send_hello(
    conn: &Connection,
    own_node_id: &str,
    own_mesh_id: &str,
    own_node_type: &str,
    peer_id_str: &str,
) {
    let frame = InternalMeshFrame::Hello {
        mesh_id: own_mesh_id.to_string(),
        node_type: own_node_type.to_string(),
    };
    let sent_span = tracing::info_span!(
        "rafka.mesh.frame.sent",
        node_id = %own_node_id,
        peer_id = %peer_id_str,
        frame_kind = "hello",
        mesh_id = own_mesh_id,
        otel.kind = "producer",
    );
    let _enter = sent_span.enter();
    let ctx = Span::current().context();
    let encoded = frame.encode_with_context(&ctx);
    drop(_enter);

    match conn.open_uni().await {
        Ok(mut send) => {
            if send.write_all(&encoded).await.is_err() || send.finish().is_err() {
                tracing::info_span!(
                    "rafka.mesh.frame.sent_failed",
                    node_id = %own_node_id,
                    peer_id = %peer_id_str,
                    frame_kind = "hello",
                    otel.kind = "producer",
                )
                .in_scope(|| info!(peer_id = %peer_id_str, "hello write/finish failed"));
            } else {
                sent_span.in_scope(|| info!(peer_id = %peer_id_str, "hello sent"));
            }
        }
        Err(e) => {
            tracing::info_span!(
                "rafka.mesh.frame.sent_failed",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                frame_kind = "hello",
                error = %e,
                otel.kind = "producer",
            )
            .in_scope(|| info!(peer_id = %peer_id_str, "open_uni failed for hello"));
        }
    }
}

/// Handles incoming uni streams. Gateway expects Pong; others expect Ping and reply with Pong.
async fn run_frame_reader(
    own_node_id: String,
    own_mesh_id: &'static str,
    peer_id_str: String,
    conn: Connection,
    registry: PeerRegistry,
    mesh_id_registry: MeshIdRegistry,
) {
    let counters = mesh_counters();
    loop {
        match conn.accept_uni().await {
            Ok(mut recv) => {
                let bytes = match recv.read_to_end(4096).await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::trace!(peer_id = %peer_id_str, error = %e, "frame read error");
                        continue;
                    }
                };

                // Count EVERY received frame regardless of variant. This is the
                // ground-truth recv counter the operator UI reads via gossip.
                counters
                    .frames_recv
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Decode for the Messages tab — capture variant + fields so
                // the operator sees actual payload content, not just kind.
                let frame_size = bytes.len();
                // Red-team R6 fix: prefix the summary with an 8-char
                // peer-ID prefix so /api/messages renders self-describing
                // entries — was "Ping{org_id=0}" with from_peer_id in a
                // separate field; now "[3f120f46] Ping{org_id=0}" in the
                // summary itself so a UI table column rendering only
                // `summary` still shows the source. from_peer_id remains
                // available as the full 64-char NodeId.
                let peer_prefix: String = peer_id_str.chars().take(8).collect();
                let (kind_tag, summary) = match InternalMeshFrame::decode_with_context(&bytes) {
                    Ok((_, InternalMeshFrame::Hello { mesh_id: m, node_type: nt })) => (
                        "hello",
                        format!("[{peer_prefix}] Hello{{mesh_id={m}, node_type={nt}}}"),
                    ),
                    Ok((_, InternalMeshFrame::Ping { org_id })) => (
                        "ping",
                        format!("[{peer_prefix}] Ping{{org_id={org_id}}}"),
                    ),
                    Ok((_, InternalMeshFrame::Pong { org_id })) => (
                        "pong",
                        format!("[{peer_prefix}] Pong{{org_id={org_id}}}"),
                    ),
                    Err(e) => ("decode_failed", format!("[{peer_prefix}] <decode_failed: {e}>")),
                };
                push_message(&peer_id_str, kind_tag, frame_size, summary);

                match InternalMeshFrame::decode_with_context(&bytes) {
                    Ok((parent_ctx, InternalMeshFrame::Hello { mesh_id: peer_mesh_id, node_type: peer_node_type })) => {
                        // Record peer's mesh_id so heartbeat (Role::Bridge especially)
                        // can aggregate per-mesh peer counts.
                        mesh_id_registry.insert(peer_id_str.clone(), peer_mesh_id.clone());
                        let recv_span = tracing::trace_span!(
                            "rafka.mesh.peer.hello_received",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            peer_mesh_id = %peer_mesh_id,
                            peer_node_type = %peer_node_type,
                            otel.kind = "consumer",
                        );
                        recv_span.set_parent(parent_ctx);
                        recv_span.in_scope(|| {
                            tracing::trace!(peer_id = %peer_id_str, peer_mesh_id = %peer_mesh_id, peer_node_type = %peer_node_type, "hello received");
                        });
                        // Cross-mesh: peer is in a different mesh_id than ours. Emit
                        // dedicated span so operators can filter Jaeger for cross-mesh
                        // links and Role::Bridge gateway flows.
                        if peer_mesh_id != own_mesh_id {
                            tracing::info_span!(
                                "rafka.mesh.cross.peer_connected",
                                node_id = %own_node_id,
                                peer_id = %peer_id_str,
                                own_mesh_id = own_mesh_id,
                                peer_mesh_id = %peer_mesh_id,
                                peer_node_type = %peer_node_type,
                                otel.kind = "internal",
                            )
                            .in_scope(|| info!(peer_id = %peer_id_str, own_mesh_id, peer_mesh_id = %peer_mesh_id, "cross-mesh peer connected"));
                        }
                    }
                    Ok((parent_ctx, InternalMeshFrame::Pong { org_id })) => {
                        let span = tracing::trace_span!(
                            "rafka.mesh.frame.received",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            frame_kind = "pong",
                            org_id = org_id,
                            otel.kind = "consumer",
                        );
                        span.set_parent(parent_ctx);
                        span.in_scope(|| {
                            tracing::trace!(peer_id = %peer_id_str, "pong received");
                        });
                    }
                    Ok((parent_ctx, InternalMeshFrame::Ping { org_id })) => {
                        // Nodes that aren't the ping sender receive pings and reply with pong.
                        let recv_span = tracing::trace_span!(
                            "rafka.mesh.frame.received",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            frame_kind = "ping",
                            org_id = org_id,
                            otel.kind = "consumer",
                        );
                        recv_span.set_parent(parent_ctx);
                        recv_span.in_scope(|| {
                            tracing::trace!(peer_id = %peer_id_str, "ping received");
                        });

                        let pong = InternalMeshFrame::Pong { org_id };
                        let sent_span = recv_span.in_scope(|| {
                            tracing::trace_span!(
                                "rafka.mesh.frame.sent",
                                node_id = %own_node_id,
                                peer_id = %peer_id_str,
                                frame_kind = "pong",
                                org_id = org_id,
                                otel.kind = "producer",
                            )
                        });
                        let _enter = sent_span.enter();
                        let ctx = Span::current().context();
                        let encoded = pong.encode_with_context(&ctx);
                        drop(_enter);

                        match conn.open_uni().await {
                            Ok(mut send) => {
                                if let Err(e) = send.write_all(&encoded).await {
                                    tracing::trace_span!(
                                        "rafka.mesh.frame.sent_failed",
                                        node_id = %own_node_id,
                                        peer_id = %peer_id_str,
                                        frame_kind = "pong",
                                        error = %e,
                                        otel.kind = "producer",
                                    )
                                    .in_scope(|| tracing::trace!(peer_id = %peer_id_str, "pong write failed"));
                                    continue;
                                }
                                if let Err(e) = send.finish() {
                                    tracing::trace_span!(
                                        "rafka.mesh.frame.sent_failed",
                                        node_id = %own_node_id,
                                        peer_id = %peer_id_str,
                                        frame_kind = "pong",
                                        error = %e,
                                        otel.kind = "producer",
                                    )
                                    .in_scope(|| tracing::trace!(peer_id = %peer_id_str, "pong finish failed"));
                                    continue;
                                }
                                sent_span.in_scope(|| {
                                    tracing::trace!(peer_id = %peer_id_str, "pong sent");
                                });
                            }
                            Err(e) => {
                                tracing::trace_span!(
                                    "rafka.mesh.frame.sent_failed",
                                    node_id = %own_node_id,
                                    peer_id = %peer_id_str,
                                    frame_kind = "pong",
                                    error = %e,
                                    otel.kind = "producer",
                                )
                                .in_scope(|| tracing::trace!(peer_id = %peer_id_str, "open_uni failed for pong"));
                            }
                        }
                    }
                    Err(e) => {
                        let byte_len = bytes.len();
                        tracing::trace_span!(
                            "rafka.mesh.frame.decode_failed",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            error = %e,
                            byte_len = byte_len,
                            otel.kind = "consumer",
                        )
                        .in_scope(|| tracing::trace!(peer_id = %peer_id_str, "frame decode failed"));
                    }
                }
            }
            Err(_) => {
                registry.remove(&peer_id_str);
                mesh_id_registry.remove(&peer_id_str);
                tracing::info_span!(
                    "rafka.mesh.peer.disconnected",
                    node_id = %own_node_id,
                    peer_id = %peer_id_str,
                    reason = "connection_closed",
                )
                .in_scope(|| info!(peer_id = %peer_id_str, "peer disconnected"));
                break;
            }
        }
    }
}

#[instrument(skip_all)]
async fn load_or_mint_identity(data_dir: &PathBuf) -> Result<SecretKey> {
    tokio::fs::create_dir_all(data_dir).await?;
    let identity_path = data_dir.join("node-identity.json");

    if identity_path.exists() {
        let raw = tokio::fs::read_to_string(&identity_path).await?;
        let stored: NodeIdentity = serde_json::from_str(&raw)?;
        let bytes = hex::decode(&stored.secret_key_hex)?;
        let key_bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid key length in identity file"))?;
        let secret_key = SecretKey::from_bytes(&key_bytes);
        info!(path = ?identity_path, node_id = %secret_key.public(), "loaded existing identity");
        Ok(secret_key)
    } else {
        let secret_key = SecretKey::generate();
        let identity = NodeIdentity {
            secret_key_hex: hex::encode(secret_key.to_bytes()),
        };
        let json = serde_json::to_string_pretty(&identity)?;
        tokio::fs::write(&identity_path, json).await?;
        info!(path = ?identity_path, node_id = %secret_key.public(), event = "identity_minted", "minted new identity");
        Ok(secret_key)
    }
}

#[instrument(skip_all)]
async fn create_endpoint(
    secret_key: SecretKey,
    bind_addr: SocketAddrV4,
    mdns_enable: bool,
) -> Result<IrohMeshTransport> {
    let transport = IrohMeshTransport::new(secret_key, bind_addr, mdns_enable).await?;
    info!(node_id = %transport.endpoint.id(), mdns_enable = mdns_enable, "iroh endpoint bound");
    Ok(transport)
}

// NO #[instrument] here — this is an infinite loop that emits child spans per tick.
// Wrapping the whole loop in a root span would keep that root open forever; child
// heartbeat spans pile up in the OTel batch waiting for parent close (which never
// happens until shutdown), so only the first few export. Each tick must be its own
// independent root span.
async fn run_heartbeat(
    node_id: String,
    mesh_id: &'static str,
    node_name: &'static str,
    is_bridge: bool,
    registry: PeerRegistry,
    mesh_id_registry: MeshIdRegistry,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    // Read clock skew once at boot. Chaos `clock_skew` primitive restarts the
    // subprocess with this env var; heartbeat surfaces it as an observable
    // attribute so chaos detection can verify the skew was applied.
    let skew_ms: i64 = std::env::var("RAFKA_CLOCK_SKEW_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    loop {
        interval.tick().await;
        let total_peer_count = registry.len() as i64;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let wall_time_ms = now_ms + skew_ms;

        // Always emit the aggregate heartbeat (preserves existing telemetry contract).
        tracing::info_span!(
            "rafka.mesh.heartbeat",
            node_id = %node_id,
            node_name = node_name,
            mesh_id = mesh_id,
            peer_count = total_peer_count,
            wall_time_ms = wall_time_ms,
            clock_skew_ms = skew_ms,
        )
        .in_scope(|| {
            info!("heartbeat");
        });

        // Role::Bridge: also emit per-target-mesh aggregate spans grouped by the
        // peer_mesh_id observed in each peer's Hello frame. Operators get a
        // per-mesh peer count for the bridge in a single Jaeger filter.
        if is_bridge {
            let mut by_mesh: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            for entry in mesh_id_registry.iter() {
                *by_mesh.entry(entry.value().clone()).or_insert(0) += 1;
            }
            for (target_mesh, count) in by_mesh {
                tracing::info_span!(
                    "rafka.mesh.bridge.per_mesh_heartbeat",
                    node_id = %node_id,
                    mesh_id = mesh_id,
                    target_mesh_id = %target_mesh,
                    peer_count = count,
                    wall_time_ms = wall_time_ms,
                )
                .in_scope(|| {
                    info!(target_mesh_id = %target_mesh, peer_count = count, "bridge per-mesh heartbeat");
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{Endpoint, endpoint::presets};
    use rafka_mesh_transport::ALPN;

    /// End-to-end bi-stream echo: two in-process iroh endpoints, A accepts +
    /// echoes via run_bi_echo_reader, B opens bi-stream + writes + reads back.
    /// Proves the data plane wire format (tag 0x11 + varint + postcard) makes
    /// the full round trip across the QUIC bi-stream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bi_stream_echo_e2e() {
        // Endpoint A (server)
        let secret_a = iroh::SecretKey::generate();
        let endpoint_a = Endpoint::builder(presets::N0DisableRelay)
            .secret_key(secret_a)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr("127.0.0.1:0").unwrap()
            .bind()
            .await
            .expect("endpoint A");
        let addr_a = endpoint_a.addr();
        let node_id_a = endpoint_a.id();

        // Accept loop on A: any incoming connection gets bi_echo_reader.
        let _accept_handle = {
            let endpoint = endpoint_a.clone();
            let own = node_id_a.to_string();
            tokio::spawn(async move {
                if let Some(incoming) = endpoint.accept().await {
                    let conn = incoming.await.expect("A accept");
                    let peer = conn.remote_id().to_string();
                    run_bi_echo_reader(conn, own, peer).await;
                }
            })
        };

        // Endpoint B (client)
        let secret_b = iroh::SecretKey::generate();
        let endpoint_b = Endpoint::builder(presets::N0DisableRelay)
            .secret_key(secret_b)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr("127.0.0.1:0").unwrap()
            .bind()
            .await
            .expect("endpoint B");
        // In iroh 0.98 there is no add_node_addr; pass the full EndpointAddr
        // directly to bi_echo_roundtrip (it accepts impl Into<EndpointAddr>).

        // Round-trip via the public helper
        let payload = b"bi-stream-echo-test-payload".to_vec();
        let echoed = bi_echo_roundtrip(&endpoint_b, addr_a, payload.clone())
            .await
            .expect("bi_echo_roundtrip");

        // Echo bytes are the FULL framed envelope (tag + varint + payload)
        // because the reader echoes raw bytes. Decode to verify the inner
        // payload survived intact.
        let (tag, inner, _consumed): (u8, Vec<u8>, usize) =
            framer::decode(&echoed).expect("decode echo");
        assert_eq!(tag, TAG_BI_ECHO, "echoed tag must be 0x11");
        assert_eq!(inner, payload, "echoed payload must equal sent");
    }

    /// Backpressure / sustained-throughput test: open 32 concurrent bi-streams
    /// from B → A, each pushing 1 KiB payloads in a tight loop for 10 seconds.
    /// Records total round-trips + measured throughput; passes if:
    ///   - >= 200 round-trips total complete (sanity floor on a 10s window)
    ///   - zero errors (means the accept loop's read_to_end didn't stall on
    ///     any single stream — i.e. the data plane back-pressured smoothly
    ///     instead of OOM-ing or hanging).
    /// This proves the bi-stream plane survives a sustained burst that's well
    /// beyond what a single broker handshake demands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn backpressure_bi_stream_flood() {
        let secret_a = iroh::SecretKey::generate();
        let endpoint_a = Endpoint::builder(presets::N0DisableRelay)
            .secret_key(secret_a)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr("127.0.0.1:0").unwrap()
            .bind()
            .await
            .expect("endpoint A");
        let addr_a = endpoint_a.addr();

        // Accept loop on A: keep accepting incoming connections; each gets a
        // run_bi_echo_reader spawned. Runs until the test drops the handle.
        let _accept_handle = {
            let endpoint = endpoint_a.clone();
            let own = endpoint_a.id().to_string();
            tokio::spawn(async move {
                while let Some(incoming) = endpoint.accept().await {
                    let own = own.clone();
                    tokio::spawn(async move {
                        if let Ok(conn) = incoming.await {
                            let peer = conn.remote_id().to_string();
                            run_bi_echo_reader(conn, own, peer).await;
                        }
                    });
                }
            })
        };

        let secret_b = iroh::SecretKey::generate();
        let endpoint_b = Endpoint::builder(presets::N0DisableRelay)
            .secret_key(secret_b)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr("127.0.0.1:0").unwrap()
            .bind()
            .await
            .expect("endpoint B");
        // In iroh 0.98, addr_a (EndpointAddr) is passed directly to connect()
        // instead of using the removed add_node_addr API.
        let addr_a_clone = addr_a.clone();

        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let errors = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut workers = Vec::new();
        for _ in 0..32 {
            let endpoint_b = endpoint_b.clone();
            let total = total.clone();
            let errors = errors.clone();
            let addr = addr_a_clone.clone();
            workers.push(tokio::spawn(async move {
                let payload = vec![0xAB_u8; 1024]; // 1 KiB per round-trip
                while tokio::time::Instant::now() < deadline {
                    match bi_echo_roundtrip(&endpoint_b, addr.clone(), payload.clone()).await {
                        Ok(_) => {
                            total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(_) => {
                            errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }));
        }
        for w in workers {
            let _ = w.await;
        }
        let total_ops = total.load(std::sync::atomic::Ordering::Relaxed);
        let err_ops = errors.load(std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "backpressure_bi_stream_flood: round_trips={total_ops} errors={err_ops} \
             over 10s across 32 concurrent streams"
        );
        assert!(err_ops == 0, "data plane errored under flood (errors={err_ops})");
        assert!(
            total_ops >= 200,
            "sustained throughput too low: only {total_ops} round-trips in 10s"
        );
    }
}

async fn wait_for_signal() -> &'static str {
    let timer = std::env::var("RAFKA_AUTO_SHUTDOWN_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(std::time::Duration::from_secs);
    tokio::select! {
        _ = async { while !std::path::Path::new("E:\\evidence-soak\\STOP").exists() { tokio::time::sleep(std::time::Duration::from_secs(2)).await; } } => {
            info!("ctrl_c received, shutting down");
            "signal"
        }
        _ = async {
            match timer {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending::<()>().await,
            }
        } => {
            info!("auto-shutdown timer fired");
            "auto_shutdown_timer"
        }
    }
}

#[cfg(test)]
mod gossip_digest_schema_tests {
    use super::*;

    #[test]
    fn digest_carries_load_fields_through_postcard_roundtrip() {
        let original = GossipDigest {
            node_id: "abc123".into(),
            node_name: "broker-1".into(),
            mesh_id: "mesh-a".into(),
            node_type: "broker".into(),
            peer_count: 3,
            peer_ids: vec!["peer1".into()],
            frames_sent_total: 100,
            frames_recv_total: 200,
            wall_time_ms: 1_700_000_000_000,
            cpu_used: 2.4,
            cpu_budget: 4.0,
            ram_used: 0.31,
            ram_budget: 2.0,
        };
        let bytes = postcard::to_allocvec(&original).expect("encode");
        let decoded: GossipDigest = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.cpu_used, 2.4);
        assert_eq!(decoded.cpu_budget, 4.0);
        assert_eq!(decoded.ram_used, 0.31_f32);
        assert_eq!(decoded.ram_budget, 2.0);
        // Wire-size budget: digest must remain under 200 bytes for typical
        // small-mesh values to fit inside QUIC datagram MTU comfortably.
        assert!(bytes.len() < 200, "digest is {} bytes, must stay under 200", bytes.len());
    }
}

#[cfg(test)]
mod staleness_pruner_tests {
    use super::*;

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn mk_digest(node_id: &str, age_ms: u64) -> GossipDigest {
        GossipDigest {
            node_id: node_id.into(),
            node_name: format!("test-{node_id}"),
            mesh_id: "mesh-test".into(),
            node_type: "broker".into(),
            peer_count: 0,
            peer_ids: vec![],
            frames_sent_total: 0,
            frames_recv_total: 0,
            wall_time_ms: now_ms().saturating_sub(age_ms),
            cpu_used: 0.0,
            cpu_budget: 0.0,
            ram_used: 0.0,
            ram_budget: 0.0,
        }
    }

    /// The pruner's actual logic, factored out of the loop so tests can
    /// call it once instead of waiting 5 seconds. Mirrors the body of
    /// `run_staleness_pruner` exactly (using last_seen_ms, not wall_time_ms).
    /// Returns the number of entries pruned.
    fn prune_once(staleness_ms: u64) -> usize {
        let now = now_ms();
        // Collect keys first (dropping shard locks) before looking up
        // last_seen_ms to avoid holding DashMap shard locks across a second
        // DashMap lookup (non-reentrant parking_lot RwLock would deadlock).
        let all_keys: Vec<String> = live_digests()
            .iter()
            .map(|e| e.key().clone())
            .collect();
        let stale: Vec<String> = {
            let seen = last_seen_ms().lock().unwrap();
            all_keys
                .into_iter()
                .filter(|node_id| {
                    let received = seen.get(node_id).copied().unwrap_or(0);
                    now.saturating_sub(received) > staleness_ms
                })
                .collect()
        };
        for node_id in &stale {
            live_digests().remove(node_id);
            last_seen_ms().lock().unwrap().remove(node_id);
        }
        for mut topic_entry in topic_membership().iter_mut() {
            for node_id in &stale {
                topic_entry.value_mut().remove(node_id);
            }
        }
        stale.len()
    }

    #[test]
    fn prunes_stale_digest_keeps_fresh() {
        // Use unique node_ids so this test doesn't conflict with anything
        // else the process-global maps might hold during the run.
        let fresh_id = format!("test-fresh-{}", std::process::id());
        let stale_id = format!("test-stale-{}", std::process::id());

        let now = now_ms();
        live_digests().insert(fresh_id.clone(), mk_digest(&fresh_id, 1_000)); // 1s old
        // Fresh entry was "received" 1s ago (local clock).
        last_seen_ms().lock().unwrap().insert(fresh_id.clone(), now.saturating_sub(1_000));

        live_digests().insert(stale_id.clone(), mk_digest(&stale_id, 60_000)); // 60s old
        // Stale entry was "received" 60s ago (local clock).
        last_seen_ms().lock().unwrap().insert(stale_id.clone(), now.saturating_sub(60_000));

        topic_membership()
            .entry("test-topic".into())
            .or_insert_with(std::collections::HashSet::new)
            .insert(fresh_id.clone());
        topic_membership()
            .entry("test-topic".into())
            .or_insert_with(std::collections::HashSet::new)
            .insert(stale_id.clone());

        // Threshold = 30s; stale entry (60s old) goes, fresh entry (1s) stays.
        let pruned = prune_once(30_000);
        assert!(pruned >= 1, "expected at least 1 prune, got {pruned}");

        assert!(live_digests().contains_key(&fresh_id), "fresh entry was pruned");
        assert!(!live_digests().contains_key(&stale_id), "stale entry was kept");

        {
            let topic_set = topic_membership().get("test-topic").unwrap();
            assert!(topic_set.contains(&fresh_id), "fresh node missing from topic");
            assert!(!topic_set.contains(&stale_id), "stale node still in topic");
        } // topic_set (Ref holding read lock) dropped here

        // Cleanup so we don't pollute other tests.
        live_digests().remove(&fresh_id);
        last_seen_ms().lock().unwrap().remove(&fresh_id);
        topic_membership().alter("test-topic", |_, mut set| {
            set.remove(&fresh_id);
            set
        });
    }

    #[test]
    fn empty_maps_no_panic() {
        // Calling prune on no stale entries returns 0 and doesn't crash.
        // Use a threshold so high that NOTHING in the maps qualifies.
        let pruned = prune_once(u64::MAX);
        assert_eq!(pruned, 0);
    }
}

#[cfg(test)]
mod node_runtime_builder_tests {
    use super::*;

    #[test]
    fn default_runtime_has_no_budget() {
        let rt = NodeRuntime::new("broker");
        assert_eq!(rt.cpu_budget, None);
        assert_eq!(rt.ram_budget, None);
    }

    #[test]
    fn with_cpu_budget_sets_value() {
        let rt = NodeRuntime::new("broker").with_cpu_budget(4.0);
        assert_eq!(rt.cpu_budget, Some(4.0));
        assert_eq!(rt.ram_budget, None);
    }

    #[test]
    fn with_ram_budget_sets_value() {
        let rt = NodeRuntime::new("broker").with_ram_budget(2.0);
        assert_eq!(rt.cpu_budget, None);
        assert_eq!(rt.ram_budget, Some(2.0));
    }

    #[test]
    fn both_budgets_chain() {
        let rt = NodeRuntime::new("broker")
            .with_cpu_budget(4.0)
            .with_ram_budget(2.0);
        assert_eq!(rt.cpu_budget, Some(4.0));
        assert_eq!(rt.ram_budget, Some(2.0));
    }

    #[test]
    fn with_role_preserves_budgets() {
        let rt = NodeRuntime::new("bridge")
            .with_cpu_budget(1.0)
            .with_ram_budget(0.5)
            .with_role(Role::Bridge);
        assert_eq!(rt.cpu_budget, Some(1.0));
        assert_eq!(rt.ram_budget, Some(0.5));
        assert!(matches!(rt.role, Role::Bridge));
    }
}
