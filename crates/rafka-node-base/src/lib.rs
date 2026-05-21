use anyhow::Result;
use dashmap::DashMap;
use iroh::{endpoint::Connection, NodeAddr, PublicKey, SecretKey};
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
use tokio::signal;
use tracing::{info, instrument, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
}

impl NodeRuntime {
    pub fn new(node_type: impl Into<String>) -> Self {
        Self {
            node_type: node_type.into(),
            role: Role::Broker,
        }
    }

    pub fn with_role(mut self, role: Role) -> Self {
        self.role = role;
        self
    }

    pub async fn run(self) -> Result<()> {
        let _guard = rafka_telemetry::init_telemetry(&self.node_type);
        run_node(self.node_type, self.role).await
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

async fn run_node(node_type: String, role: Role) -> Result<()> {
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
        .unwrap_or(500);

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
    let mut transport = create_endpoint(secret_key, bind_addr).await?;

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
            .in_scope(|| info!(gossip_interval_ms, "gossip discovery started via iroh mdns"));

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
                mesh_id,          // primary, NOT extra_mesh_static
                node_name,
                node_type_g,
                gossip_interval_ms,
                registry_for_digest,
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

    // EVERY role runs the ping sender. topology-ui (Role::Observer) is just
    // another node — it pings like everything else.
    let ping_handle = {
        let registry = Arc::clone(&peer_registry);
        Some(tokio::spawn(run_ping_sender(node_id.clone(), registry)))
    };

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
    if let Some(h) = ping_handle {
        h.abort();
    }
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
                tracing::info_span!(
                    "rafka.mesh.bi.read_failed",
                    node_id = %own_node_id,
                    peer_id = %peer_id_str,
                    error = %e,
                )
                .in_scope(|| info!(error = %e, "bi-stream read failed"));
                continue;
            }
        };
        if bytes.is_empty() {
            continue;
        }
        let tag = bytes[0];
        if tag != TAG_BI_ECHO {
            tracing::info_span!(
                "rafka.mesh.bi.unknown_tag",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                tag = tag as i64,
                size_bytes = bytes.len() as i64,
            )
            .in_scope(|| info!(tag, "bi-stream unknown tag — dropping"));
            continue;
        }
        let recv_span = tracing::info_span!(
            "rafka.mesh.bi.echo_received",
            node_id = %own_node_id,
            peer_id = %peer_id_str,
            size_bytes = bytes.len() as i64,
        );
        recv_span.in_scope(|| info!(size = bytes.len(), "bi echo received"));

        // Echo back identical bytes (tag + varint + payload).
        if let Err(e) = send.write_all(&bytes).await {
            tracing::info_span!(
                "rafka.mesh.bi.write_failed",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                error = %e,
            )
            .in_scope(|| info!(error = %e, "bi echo write failed"));
            continue;
        }
        if let Err(e) = send.finish() {
            tracing::info_span!(
                "rafka.mesh.bi.finish_failed",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                error = %e,
            )
            .in_scope(|| info!(error = %e, "bi echo finish failed"));
            continue;
        }
        tracing::info_span!(
            "rafka.mesh.bi.echo_sent",
            node_id = %own_node_id,
            peer_id = %peer_id_str,
            size_bytes = bytes.len() as i64,
        )
        .in_scope(|| info!(size = bytes.len(), "bi echo sent"));
    }
}

/// Standalone bi-stream client: open a bi-stream to `peer`, write a framed
/// payload (tag 0x11), read echo back, return the round-trip bytes. Used by
/// the bi-stream-echo test in the CLI test runner — proves the dedicated data
/// plane works without needing live broker / compute / message types yet.
pub async fn bi_echo_roundtrip(
    endpoint: &iroh::Endpoint,
    peer: iroh::NodeId,
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
) {
    let counters = mesh_counters();
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
    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Feed mdns-discovered peers to gossip so the swarm forms. Subscribe
                // with empty bootstrap only finds peers if they're explicitly added.
                // join_peers is idempotent — calling every tick with the current
                // peer registry is cheap.
                let peer_node_ids: Vec<iroh::NodeId> = registry
                    .iter()
                    .filter_map(|e| iroh::NodeId::from_str(e.key()).ok())
                    .collect();
                if !peer_node_ids.is_empty() {
                    let _ = sender.join_peers(peer_node_ids).await;
                }
                use std::sync::atomic::Ordering;
                let peer_ids: Vec<String> = registry.iter().map(|e| e.key().clone()).collect();
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
                };
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
                let Some(event) = event else { continue };
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
                        // Authoritative topic-membership: we received d's digest
                        // ON THIS TOPIC (= the `mesh_id` param of this run_gossip
                        // task), so d is a member of that topic's swarm.
                        // Edges in /api/topology are built from this map's
                        // intersections rather than from peer_ids (mdns junk).
                        topic_membership()
                            .entry(mesh_id.to_string())
                            .or_insert_with(std::collections::HashSet::new)
                            .insert(d.node_id.clone());
                    }
                    let summary = digest
                        .as_ref()
                        .map(|d| format!("node={}/peers={}", d.node_name, d.peer_count))
                        .unwrap_or_else(|| "<decode_failed>".to_string());
                    tracing::info_span!(
                        "rafka.mesh.gossip.received",
                        node_id = %node_id,
                        from_peer = %from,
                        size_bytes = size as i64,
                        digest = %summary,
                    )
                    .in_scope(|| info!(from = %from, size_bytes = size, digest = %summary, "gossip digest received"));
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
    for seed in seeds {
        let peer_id_str = seed.id.to_string();

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

        let endpoint_addr = NodeAddr::new(seed.id).with_direct_addresses([seed.addr]);
        match endpoint.connect(endpoint_addr, ALPN).await {
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
            }
            Err(e) => {
                info!(peer_id = %peer_id_str, error = %e, "seed dial failed");
            }
        }
    }
}

#[instrument(skip_all)]
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
                        let alpn = conn.alpn().unwrap_or_default();
                        if alpn == iroh_gossip::ALPN {
                            // Route to gossip — its handle_connection drives the
                            // HyParView state machine on this connection.
                            let peer_id = conn
                                .remote_node_id()
                                .map(|id| id.to_string())
                                .unwrap_or_else(|_| "unknown".into());
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
                            let peer_id = conn
                                .remote_node_id()
                                .map(|id| id.to_string())
                                .unwrap_or_else(|_| "unknown".into());
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
                        info!(peer_id = %peer_id_str, error = %e, "frame read error");
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
                let (kind_tag, summary) = match InternalMeshFrame::decode_with_context(&bytes) {
                    Ok((_, InternalMeshFrame::Hello { mesh_id: m, node_type: nt })) => (
                        "hello",
                        format!("Hello{{mesh_id={m}, node_type={nt}}}"),
                    ),
                    Ok((_, InternalMeshFrame::Ping { org_id })) => (
                        "ping",
                        format!("Ping{{org_id={org_id}}}"),
                    ),
                    Ok((_, InternalMeshFrame::Pong { org_id })) => (
                        "pong",
                        format!("Pong{{org_id={org_id}}}"),
                    ),
                    Err(e) => ("decode_failed", format!("<decode_failed: {e}>")),
                };
                push_message(&peer_id_str, kind_tag, frame_size, summary);

                match InternalMeshFrame::decode_with_context(&bytes) {
                    Ok((parent_ctx, InternalMeshFrame::Hello { mesh_id: peer_mesh_id, node_type: peer_node_type })) => {
                        // Record peer's mesh_id so heartbeat (Role::Bridge especially)
                        // can aggregate per-mesh peer counts.
                        mesh_id_registry.insert(peer_id_str.clone(), peer_mesh_id.clone());
                        let recv_span = tracing::info_span!(
                            "rafka.mesh.peer.hello_received",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            peer_mesh_id = %peer_mesh_id,
                            peer_node_type = %peer_node_type,
                            otel.kind = "consumer",
                        );
                        recv_span.set_parent(parent_ctx);
                        recv_span.in_scope(|| {
                            info!(peer_id = %peer_id_str, peer_mesh_id = %peer_mesh_id, peer_node_type = %peer_node_type, "hello received");
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
                        let span = tracing::info_span!(
                            "rafka.mesh.frame.received",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            frame_kind = "pong",
                            org_id = org_id,
                            otel.kind = "consumer",
                        );
                        span.set_parent(parent_ctx);
                        span.in_scope(|| {
                            info!(peer_id = %peer_id_str, "pong received");
                        });
                    }
                    Ok((parent_ctx, InternalMeshFrame::Ping { org_id })) => {
                        // Nodes that aren't the ping sender receive pings and reply with pong.
                        let recv_span = tracing::info_span!(
                            "rafka.mesh.frame.received",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            frame_kind = "ping",
                            org_id = org_id,
                            otel.kind = "consumer",
                        );
                        recv_span.set_parent(parent_ctx);
                        recv_span.in_scope(|| {
                            info!(peer_id = %peer_id_str, "ping received");
                        });

                        let pong = InternalMeshFrame::Pong { org_id };
                        let sent_span = recv_span.in_scope(|| {
                            tracing::info_span!(
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
                                    tracing::info_span!(
                                        "rafka.mesh.frame.sent_failed",
                                        node_id = %own_node_id,
                                        peer_id = %peer_id_str,
                                        frame_kind = "pong",
                                        error = %e,
                                        otel.kind = "producer",
                                    )
                                    .in_scope(|| info!(peer_id = %peer_id_str, "pong write failed"));
                                    continue;
                                }
                                if let Err(e) = send.finish() {
                                    tracing::info_span!(
                                        "rafka.mesh.frame.sent_failed",
                                        node_id = %own_node_id,
                                        peer_id = %peer_id_str,
                                        frame_kind = "pong",
                                        error = %e,
                                        otel.kind = "producer",
                                    )
                                    .in_scope(|| info!(peer_id = %peer_id_str, "pong finish failed"));
                                    continue;
                                }
                                sent_span.in_scope(|| {
                                    info!(peer_id = %peer_id_str, "pong sent");
                                });
                            }
                            Err(e) => {
                                tracing::info_span!(
                                    "rafka.mesh.frame.sent_failed",
                                    node_id = %own_node_id,
                                    peer_id = %peer_id_str,
                                    frame_kind = "pong",
                                    error = %e,
                                    otel.kind = "producer",
                                )
                                .in_scope(|| info!(peer_id = %peer_id_str, "open_uni failed for pong"));
                            }
                        }
                    }
                    Err(e) => {
                        let byte_len = bytes.len();
                        tracing::info_span!(
                            "rafka.mesh.frame.decode_failed",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            error = %e,
                            byte_len = byte_len,
                            otel.kind = "consumer",
                        )
                        .in_scope(|| info!(peer_id = %peer_id_str, "frame decode failed"));
                    }
                }
            }
            Err(_) => {
                registry.remove(&peer_id_str);
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
async fn run_ping_sender(own_node_id: String, registry: PeerRegistry) {
    let counters = mesh_counters();
    // 10s cadence: faster (3s) overloads the Jaeger ES backend with ~100
    // spans/sec across 18 nodes and stalls /api/topology resolution. 10s
    // gives ~30 spans/sec sustained which Jaeger handles fine. The UI's
    // 60s lookback still accumulates ~6 frame.sent spans per peer pair —
    // enough to weight edges visibly.
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    // Link fault-injection envs read once at boot. Chaos primitives slow_link/
    // lossy_link restart the node with these set; node-base applies them on
    // outbound ping sends so the substrate behaves as if the link were degraded.
    let link_slow_ms: u64 = std::env::var("RAFKA_LINK_SLOW_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let link_loss_pct: u8 = std::env::var("RAFKA_LINK_LOSS_PCT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    loop {
        interval.tick().await;

        let peer_ids: Vec<String> = registry.iter().map(|e| e.key().clone()).collect();

        for peer_id_str in peer_ids {
            let conn = match registry.get(&peer_id_str) {
                Some(c) => c.clone(),
                None => continue,
            };

            // lossy_link: roll dice; if loss, emit `dropped` span and skip the send.
            if link_loss_pct > 0 {
                let roll: u8 = rand::random::<u8>() % 100;
                if roll < link_loss_pct {
                    tracing::info_span!(
                        "rafka.mesh.frame.dropped_by_fault_inject",
                        node_id = %own_node_id,
                        peer_id = %peer_id_str,
                        frame_kind = "ping",
                        link_loss_pct = link_loss_pct as i64,
                        otel.kind = "producer",
                    )
                    .in_scope(|| info!(peer_id = %peer_id_str, link_loss_pct, "ping dropped by lossy_link fault inject"));
                    continue;
                }
            }
            // slow_link: sleep before write to simulate latency.
            if link_slow_ms > 0 {
                tokio::time::sleep(Duration::from_millis(link_slow_ms)).await;
            }

            let frame = InternalMeshFrame::Ping { org_id: 0 };

            // Enter frame.sent span BEFORE encoding — the embedded context must carry
            // THIS span's trace_id/span_id, not its parent's.
            let sent_span = tracing::info_span!(
                "rafka.mesh.frame.sent",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                frame_kind = "ping",
                org_id = 0u64,
                otel.kind = "producer",
            );
            let _enter = sent_span.enter();
            let ctx = Span::current().context();
            let encoded = frame.encode_with_context(&ctx);
            drop(_enter);

            match conn.open_uni().await {
                Ok(mut send) => {
                    if let Err(e) = send.write_all(&encoded).await {
                        tracing::info_span!(
                            "rafka.mesh.frame.sent_failed",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            frame_kind = "ping",
                            error = %e,
                            otel.kind = "producer",
                        )
                        .in_scope(|| info!(peer_id = %peer_id_str, "ping write failed"));
                        continue;
                    }
                    if let Err(e) = send.finish() {
                        tracing::info_span!(
                            "rafka.mesh.frame.sent_failed",
                            node_id = %own_node_id,
                            peer_id = %peer_id_str,
                            frame_kind = "ping",
                            error = %e,
                            otel.kind = "producer",
                        )
                        .in_scope(|| info!(peer_id = %peer_id_str, "ping finish failed"));
                        continue;
                    }
                    sent_span.in_scope(|| {
                        info!(peer_id = %peer_id_str, "ping sent");
                    });
                    counters
                        .frames_sent
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::info_span!(
                        "rafka.mesh.frame.sent_failed",
                        node_id = %own_node_id,
                        peer_id = %peer_id_str,
                        frame_kind = "ping",
                        error = %e,
                        otel.kind = "producer",
                    )
                    .in_scope(|| info!(peer_id = %peer_id_str, "open_uni failed for ping"));
                }
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
        let secret_key = SecretKey::generate(rand::rngs::OsRng);
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
) -> Result<IrohMeshTransport> {
    let transport = IrohMeshTransport::new(secret_key, bind_addr).await?;
    info!(node_id = %transport.endpoint.node_id(), "iroh endpoint bound");
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
    use iroh::{Endpoint, Watcher};
    use rafka_mesh_transport::ALPN;

    /// End-to-end bi-stream echo: two in-process iroh endpoints, A accepts +
    /// echoes via run_bi_echo_reader, B opens bi-stream + writes + reads back.
    /// Proves the data plane wire format (tag 0x11 + varint + postcard) makes
    /// the full round trip across the QUIC bi-stream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bi_stream_echo_e2e() {
        // Endpoint A (server)
        let secret_a = iroh::SecretKey::generate(rand::rngs::OsRng);
        let endpoint_a = Endpoint::builder()
            .secret_key(secret_a)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr_v4("127.0.0.1:0".parse().unwrap())
            .bind()
            .await
            .expect("endpoint A");
        let addr_a = endpoint_a.node_addr().initialized().await;
        let node_id_a = endpoint_a.node_id();

        // Accept loop on A: any incoming connection gets bi_echo_reader.
        let _accept_handle = {
            let endpoint = endpoint_a.clone();
            let own = node_id_a.to_string();
            tokio::spawn(async move {
                if let Some(incoming) = endpoint.accept().await {
                    let conn = incoming.await.expect("A accept");
                    let peer = conn.remote_node_id().map(|i| i.to_string()).unwrap_or_default();
                    run_bi_echo_reader(conn, own, peer).await;
                }
            })
        };

        // Endpoint B (client)
        let secret_b = iroh::SecretKey::generate(rand::rngs::OsRng);
        let endpoint_b = Endpoint::builder()
            .secret_key(secret_b)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr_v4("127.0.0.1:0".parse().unwrap())
            .bind()
            .await
            .expect("endpoint B");
        // Add A as known peer so the dial resolves without DHT/mdns.
        endpoint_b.add_node_addr(addr_a).expect("add node addr");

        // Round-trip via the public helper
        let payload = b"bi-stream-echo-test-payload".to_vec();
        let echoed = bi_echo_roundtrip(&endpoint_b, node_id_a, payload.clone())
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
        let secret_a = iroh::SecretKey::generate(rand::rngs::OsRng);
        let endpoint_a = Endpoint::builder()
            .secret_key(secret_a)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr_v4("127.0.0.1:0".parse().unwrap())
            .bind()
            .await
            .expect("endpoint A");
        let addr_a = endpoint_a.node_addr().initialized().await;
        let node_id_a = endpoint_a.node_id();

        // Accept loop on A: keep accepting incoming connections; each gets a
        // run_bi_echo_reader spawned. Runs until the test drops the handle.
        let _accept_handle = {
            let endpoint = endpoint_a.clone();
            let own = node_id_a.to_string();
            tokio::spawn(async move {
                while let Some(incoming) = endpoint.accept().await {
                    let own = own.clone();
                    tokio::spawn(async move {
                        if let Ok(conn) = incoming.await {
                            let peer = conn
                                .remote_node_id()
                                .map(|i| i.to_string())
                                .unwrap_or_default();
                            run_bi_echo_reader(conn, own, peer).await;
                        }
                    });
                }
            })
        };

        let secret_b = iroh::SecretKey::generate(rand::rngs::OsRng);
        let endpoint_b = Endpoint::builder()
            .secret_key(secret_b)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(iroh::RelayMode::Disabled)
            .bind_addr_v4("127.0.0.1:0".parse().unwrap())
            .bind()
            .await
            .expect("endpoint B");
        endpoint_b.add_node_addr(addr_a).expect("add node addr");

        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let errors = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut workers = Vec::new();
        for _ in 0..32 {
            let endpoint_b = endpoint_b.clone();
            let total = total.clone();
            let errors = errors.clone();
            workers.push(tokio::spawn(async move {
                let payload = vec![0xAB_u8; 1024]; // 1 KiB per round-trip
                while tokio::time::Instant::now() < deadline {
                    match bi_echo_roundtrip(&endpoint_b, node_id_a, payload.clone()).await {
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
        _ = signal::ctrl_c() => {
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
