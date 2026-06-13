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

/// Tag for the cold-pull (warm re-hydration) request bi-stream. A node whose mesh
/// view has collapsed (lost its peers, `live_digests` aged out) opens a bi-stream
/// to the **node-admin** (the always-available hub — see `RAFKA_NODE_ADMIN_ADDR`),
/// writes this single tag byte, and the donor replies with a postcard
/// `Vec<GossipDigest>` snapshot of ITS `live_digests`. The requester hydrates
/// immediately instead of waiting for gossip to re-flood. This is the §4 "warm
/// pull" principle (Entity-Cache.md) realized over QUIC against the node-admin,
/// NOT the chunked broker-log snapshot RPC (that tailer doesn't apply to a
/// gossip-only mesh). Rides the dial_seeds connection, so it costs no extra dial.
pub const TAG_SNAPSHOT_REQ: u8 = 0x12;

use serde::{Deserialize, Serialize};
use std::{
    net::{SocketAddr, SocketAddrV4},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tracing::{info, instrument, Span};
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
    read_dev_cpu_used,
    read_dev_ram_used,
    BudgetCliArgs,
    LoadSampler,
    NodeLoad,
};

pub mod topology_cache;
pub use topology_cache::{TopologyNode, topology_nodes};

pub mod node_cache;
pub mod cert;
pub use node_cache::{
    CacheType, Channel, CacheSpec, CacheEntry, NodeCache, ApplyResult,
    CacheGossipMsg, ChannelEvent, ChannelEventRing,
    node_caches, channel_events,
    parse_cache_specs_from_env,
    run_cache_task, run_keygossip_fill,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Gateway,
    Broker,
    Compute,
    Registry,
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
        // Sprint-13 B1: the node SELF-NAMES from its own node_id, and its OTel
        // identity (service.name / .namespace / .instance.id) must be in place
        // BEFORE init_telemetry builds the resource. node_id is only known after
        // identity load, so we load it HERE (early) — before telemetry init —
        // derive the three OTel fields, and export them via the standard OTel env
        // vars that rafka-telemetry::build_resource reads.
        //
        // mesh_id is REQUIRED (no "default"): fail fast so a misconfigured node
        // never joins a phantom mesh.
        let mesh_id = resolve_required_mesh_id()?;

        // Pin RAFKA_DATA_DIR to the resolved path so the identity is loaded from
        // ONE file across this early load AND run_node's later load — otherwise an
        // unset data_dir would mint two different random keys (two node_ids).
        let data_dir = resolve_data_dir();
        // SAFETY: single-threaded startup, before any tasks spawn.
        std::env::set_var("RAFKA_DATA_DIR", &data_dir);

        let secret_key = load_or_mint_identity(&data_dir).await?;
        let node_id = secret_key.public().to_string();

        // service.name = <mesh>.<type> (the Jaeger graph node); namespace = mesh;
        // instance.id = the FULL node_id (the iroh public key). EVERY node owns
        // these — sprint-14 B6 normalized the admin-ui: it is a node like any other
        // (RAFKA_MESH_ID=mesh1, service.name="mesh1.admin-ui"), NO flat special case.
        let service_name = format!("{}.{}", mesh_id, self.node_type);
        std::env::set_var("OTEL_SERVICE_NAME", &service_name);
        std::env::set_var(
            "OTEL_RESOURCE_ATTRIBUTES",
            format!("service.namespace={mesh_id},service.instance.id={node_id}"),
        );

        let _guard = rafka_telemetry::init_telemetry(&service_name);
        run_node(self.node_type, self.role, self.cpu_budget, self.ram_budget).await
    }
}

/// Sprint-13 B1: number of hex chars of the node_id used in the self-derived
/// node_name (`<mesh>.<type>.<first N hex>`). Tunable in one place.
pub const NODE_NAME_HEX_LEN: usize = 6;

/// Milliseconds since the UNIX epoch (0 on the impossible clock-before-1970 case).
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Resolve `RAFKA_MESH_ID` or fail fast (sprint-13 B1 — no silent "default").
fn resolve_required_mesh_id() -> Result<&'static str> {
    let mesh_id = std::env::var("RAFKA_MESH_ID").unwrap_or_default();
    let mesh_id = mesh_id.trim();
    if mesh_id.is_empty() {
        anyhow::bail!(
            "RAFKA_MESH_ID is required and must be non-empty — refusing to boot. \
             Every node must explicitly declare its mesh (no 'default' fallback)."
        );
    }
    Ok(Box::leak(mesh_id.to_string().into_boxed_str()))
}

/// Resolve the data dir (identity storage). Default `./data/node-<random>` only
/// when unset — admin-ui always sets it explicitly.
fn resolve_data_dir() -> PathBuf {
    std::env::var("RAFKA_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let id: u32 = rand::random();
            PathBuf::from(format!("./data/node-{id:08x}"))
        })
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
/// Parallel registry: peer_id → peer_mesh_id, populated from Hello frames. Makes
/// peer→mesh associations observable from any code path that has the peer_id.
type MeshIdRegistry = Arc<DashMap<String, String>>;

async fn run_node(
    node_type: String,
    role: Role,
    cpu_budget: Option<f32>,
    ram_budget: Option<f32>,
) -> Result<()> {
    // mesh_id is a logical cluster identifier. REQUIRED (no "default" — sprint-13
    // B1): cross-mesh awareness comes from a node observing additional meshes via
    // RAFKA_OBSERVER_MESHES. Fail fast if unset (run() already validated, but
    // run_node can be reached on a manual/test launch too).
    let mesh_id = resolve_required_mesh_id()?;

    // data_dir resolved identically to run()'s early load. run() pins
    // RAFKA_DATA_DIR before init_telemetry, so this read returns the SAME path and
    // load_or_mint_identity below loads the SAME identity → consistent node_id.
    let data_dir = resolve_data_dir();

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

    let mut seed_nodes: Vec<SeedNode> = std::env::var("RAFKA_SEED_NODES")
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
    // The REAL identity load MUST happen here (before node.ready) because we need
    // node_id to populate node.ready's attributes. But the identity OBSERVATION
    // span is emitted later as an in_scope child of node.ready (see below) so the
    // full boot chain — including identity — lives in ONE trace per CLAUDE.md §10.
    // create_endpoint is called OUTSIDE node.ready so iroh's background tasks don't
    // inherit its context (which would keep node.ready open and block its export).
    let identity_path = data_dir.join("node-identity.json");
    let identity_existed = identity_path.exists();
    let secret_key = load_or_mint_identity(&data_dir).await?;
    let node_id = secret_key.public().to_string();

    // node_name is the node's STABLE LOGICAL identity, IMMUTABLE across its lifecycle
    // (restarts, identity/node_id rotations). When admin-ui spawns the node it assigns
    // a 1-based per-(mesh,type) counter name `<mesh>.<type>.<N>` via RAFKA_NODE_NAME —
    // the durable handle partition routing + lifecycle logic tie to, decoupled from
    // the MUTABLE node_id. A node launched WITHOUT RAFKA_NODE_NAME (manual/dev) falls
    // back to the self-derived `<mesh>.<type>.<first 6 hex of node_id>` form.
    let node_name: &'static str = match std::env::var("RAFKA_NODE_NAME") {
        Ok(n) if !n.trim().is_empty() => Box::leak(n.trim().to_string().into_boxed_str()),
        _ => {
            let hex_suffix: String = node_id.chars().take(NODE_NAME_HEX_LEN).collect();
            Box::leak(format!("{mesh_id}.{node_type_str}.{hex_suffix}").into_boxed_str())
        }
    };

    // ── Node-admin anchor seed + self-seed guard ──
    // RAFKA_NODE_ADMIN_ADDR carries the node-admin's `node_id@ip:port` — the bootstrap
    // ANCHOR (a stable known-id seed every node can always dial; the admin's identity
    // does NOT rotate). We ensure it is in the seed list so dial_seeds keeps a
    // connection to it. The cold-pull itself is NOT admin-specific: a collapsed node
    // re-hydrates from WHICHEVER seed it reaches (every node carries the topology).
    // Guards:
    //   * never self-dial: drop any seed whose id == our node_id.
    //   * keep the admin dialable: prepend it to the seed list if missing (and not us).
    let admin_seed: Option<SeedNode> = std::env::var("RAFKA_NODE_ADMIN_ADDR")
        .ok()
        .and_then(|s| {
            let s = s.trim();
            let (id_str, addr_str) = s.split_once('@')?;
            let id = PublicKey::from_str(id_str).ok()?;
            let addr = addr_str.parse::<SocketAddr>().ok()?;
            Some(SeedNode { id, addr })
        });
    seed_nodes.retain(|s| s.id.to_string() != node_id);
    if let Some(a) = admin_seed {
        if a.id.to_string() != node_id && !seed_nodes.iter().any(|s| s.id == a.id) {
            seed_nodes.insert(0, a);
        }
    }

    // Create iroh endpoint: no tracing span active here so iroh background tasks
    // are NOT attached to node.ready.
    let mut transport = create_endpoint(secret_key, bind_addr, mdns_enable).await?;

    // Publish this process's iroh endpoint to a process global so the (same-process)
    // control plane — admin-ui's kill is a CONTROL OP, not OS TerminateProcess — can
    // dial ANY node by node_id and send it a Shutdown frame. See send_shutdown().
    let _ = MESH_ENDPOINT.set(transport.endpoint.clone());

    // Register iroh's built-in manual address book on the endpoint. The gossip
    // receive path feeds it each peer's `location`, so join_peers/connect resolve
    // by node_id without a discovery lookup (mDNS stays off). See register_peer_location().
    {
        let book = iroh::address_lookup::memory::MemoryLookup::with_provenance("rafka-gossip-location");
        if let Ok(services) = transport.endpoint.address_lookup() {
            services.add(book.clone());
        }
        // Seed addresses are explicit + known at boot. Register them NOW so any
        // join_peers/connect for a seed resolves immediately — critical for the
        // BACKBONE: a cross-mesh seed peer is in a different mesh, so its
        // GossipDigest never arrives on our per-mesh gossip and register_peer_location
        // (the digest path) never sees it. Without this, the backbone topic's
        // join_peers(seed_id) has no address for the cross-mesh console and the
        // two meshes' backbone swarms never link (each shows only its own mesh).
        for seed in &seed_nodes {
            book.add_endpoint_info(EndpointAddr::new(seed.id).with_ip_addr(seed.addr));
        }
        let _ = MESH_ADDR_BOOK.set(book);
    }

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

        // Identity observation span — child of node.ready so the locked §10 boot
        // chain (6 spans incl. identity) lives in ONE trace. The real identity
        // load already happened above (we needed node_id); this is the in-trace
        // marker, emitted identity_loaded vs identity_minted by whether the
        // node-identity.json file existed at boot.
        let identity_span = if identity_existed {
            tracing::info_span!("rafka.mesh.boot.identity_loaded", node_id = %node_id, path = ?identity_path)
        } else {
            tracing::info_span!("rafka.mesh.boot.identity_minted", node_id = %node_id, path = ?identity_path)
        };
        identity_span.in_scope(|| info!(node_id = %node_id, path = ?identity_path, existed = identity_existed, "identity loaded"));

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

    // Phase 2b: birth-injection — if node-admin handed this child a topology
    // snapshot at spawn, hydrate live_digests() + the topology cache from it
    // BEFORE gossip starts, so the node has the mesh view at t=0 ("born knowing").
    topology_cache::inject_birth_topology(&node_id, node_name).await;

    // Background pruner: drops stale entries from the process-global
    // `live_digests` and `topic_membership` maps. Without it both grow
    // monotonically with the count of unique node_ids ever observed. See
    // `run_staleness_pruner` for the TTL semantics.
    tokio::spawn(run_staleness_pruner());

    // Phase 2: fill the process-global topology-node entity-cache from gossip.
    // Runs on EVERY node type (gateway / broker / compute / registry / observer /
    // admin-ui) — each node gets its own topology view automatically.
    // self_node_id + self_node_name are passed so the ~10s snapshot span can
    // identify which node emitted it for soak observability.
    let topo_fill_handle = tokio::spawn(topology_cache::run_topology_cache_fill(
        node_id.clone(),
        node_name.to_string(),
    ));

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

    // Build the location string once — reused by every run_gossip task.
    // Prefer `127.0.0.1:<port>` over `0.0.0.0:<port>` so peers can
    // actually dial back on loopback; on a real routable interface the
    // bind_addr already contains the right host.
    let location_str: String = {
        let raw = bind_addr.to_string();
        if bind_addr.ip().is_unspecified() && bind_addr.port() != 0 {
            format!("127.0.0.1:{}", bind_addr.port())
        } else {
            raw
        }
    };

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
            location_str.clone(),
            true, // primary mesh-topic task: self-publishes immediate state changes
        ))
    };

    // RAFKA_OBSERVER_MESHES: comma-separated list of ADDITIONAL meshes to
    // subscribe to (beyond our primary RAFKA_MESH_ID). Feeds the multi-topic-join
    // path. Each extra topic gets its own run_gossip task that writes into the
    // process-wide live_digests() map and broadcasts our own digest on that
    // topic too — so an observing node (e.g. a gateway watching both meshes)
    // genuinely appears as a member of every mesh it observes. This is how
    // cross-mesh awareness works post-bridge (PRD §2): the gateway observes the
    // other mesh's gossip, so its live_digests() spans both meshes.
    let extra_meshes_combined = {
        let observer = std::env::var("RAFKA_OBSERVER_MESHES").unwrap_or_default();
        if observer.is_empty() { None } else { Some(observer) }
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
                location_str.clone(),
                false, // extra observer-topic task: does not self-publish
            ));
        }
    }

    let mdns_rx = std::mem::replace(
        &mut transport.mdns_discovered,
        tokio::sync::mpsc::channel(1).1,
    );

    let accept_handle =
        start_accept_loop(&transport, node_id.clone(), mesh_id, node_type_str, node_name, Arc::clone(&peer_registry), Arc::clone(&mesh_id_registry), gossip.clone()).await;

    // Dedicated bidirectional QUIC stream echo accept loop — the data plane
    // sanity surface for the new framed wire grammar (tag 0x11). Lives in its
    // own task because it accepts NEW bi-streams as they arrive, independent
    // of the per-peer frame readers.
    let bi_echo_handle = {
        let endpoint = transport.endpoint.clone();
        let node_id_be = node_id.clone();
        tokio::spawn(run_bi_echo_acceptor(endpoint, node_id_be))
    };

    // Seed ids captured BEFORE dial_seeds consumes seed_nodes. Used to bootstrap
    // the backbone gossip swarm: a remote console's backbone subscription must JOIN
    // the existing swarm across the cross-seed bridge. Subscribing with an empty
    // bootstrap + relying on later join_peers did NOT reliably bridge two fleets'
    // backbone swarms (a mesh1 console never received mesh2's MeshSummary even
    // though hop-1 gateway->same-mesh-console worked).
    let backbone_seed_ids: Vec<PublicKey> = seed_nodes.iter().map(|s| s.id).collect();

    let dial_handle = if !seed_nodes.is_empty() {
        let node_id_dial = node_id.clone();
        let endpoint = transport.endpoint.clone();
        let registry = Arc::clone(&peer_registry);
        let mesh_reg = Arc::clone(&mesh_id_registry);
        Some(tokio::spawn(dial_seeds(endpoint, seed_nodes, node_id_dial, mesh_id, node_type_str, node_name, registry, mesh_reg)))
    } else {
        None
    };

    let mdns_handle = {
        let node_id_mdns = node_id.clone();
        let endpoint = transport.endpoint.clone();
        let registry = Arc::clone(&peer_registry);
        let mesh_reg = Arc::clone(&mesh_id_registry);
        tokio::spawn(watch_mdns(mdns_rx, endpoint, node_id_mdns, mesh_id, node_type_str, node_name, registry, mesh_reg))
    };

    let heartbeat_handle = {
        let registry = Arc::clone(&peer_registry);
        tokio::spawn(run_heartbeat(node_id.clone(), mesh_id, node_name, registry))
    };

    // No application-level ping/pong: iroh-quinn owns connection liveness
    // (keep-alive + idle timeout). rafka does not hand-roll a heartbeat
    // (Golden Principle #1). run_ping_sender removed entirely.

    // Write-sim (sprint-13 B4+B5): gateways periodically resolve target brokers
    // from the gossiped topology cache (live_digests → location) and send a Write
    // over a BIDIRECTIONAL stream (request/response). Targets: mesh1.broker.br1
    // (intra) + mesh2.broker.br1 (cross-mesh). The broker continues the W3C trace
    // (produce.handle) and writes an ACK back carrying ITS span context; the
    // gateway reads the ACK as a child of the broker's ack span → the dependency
    // graph shows BOTH arrows. The admin-ui CC is GONE (B4) — no observer special-
    // case; the Messages tab derives from gossiped frames_recv_total instead.
    let write_sim_handle = if matches!(role, Role::Gateway) {
        let endpoint = transport.endpoint.clone();
        let node_name_ws = node_name.to_string();
        let node_id_ws = node_id.clone();
        Some(tokio::spawn(run_write_sim(endpoint, node_id_ws, node_name_ws)))
    } else {
        None
    };

    // Cross-mesh backbone (sprint-14, PRD 03; multi-mesh: node-admin is the SOLE
    // backbone listener). ONLY the admin-ui (Role::Observer) subscribes to the
    // single backbone topic blake3("rafka.backbone") and publishes its mesh's
    // summary. node-admin is the cross-mesh authority in the settled model:
    // gateways/brokers/computes/registries are pure intra-mesh participants and
    // never touch the backbone — cross-mesh traffic is the repeater's job, not the
    // gateway's. (Dropping gateways here removes their cross-mesh write-sim target
    // resolution via backbone_summaries(); that arrow is intentionally retired —
    // the repeater phase reintroduces cross-mesh delivery on the shared-root trust.)
    let backbone_interval_ms: u64 = std::env::var("RAFKA_BACKBONE_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let backbone_handle = if matches!(role, Role::Observer) {
        let backbone_bytes: [u8; 32] = *blake3::hash(BACKBONE_TOPIC_NAME.as_bytes()).as_bytes();
        let backbone_topic = iroh_gossip::proto::TopicId::from_bytes(backbone_bytes);
        // The admin-ui is a NODE in its mesh and the sole backbone participant: it
        // advertises its mesh's summary (aggregated from live_digests()) so the mesh
        // is visible cross-mesh even with no gateway. One admin per mesh ⇒ it is
        // unconditionally the publisher (the soft-lease machinery still applies but
        // there is only ever one candidate now).
        let is_publisher = true;
        let gossip_bb = gossip.clone();
        let node_id_bb = node_id.clone();
        let registry_bb = Arc::clone(&peer_registry);
        Some(tokio::spawn(run_backbone(
            gossip_bb,
            backbone_topic,
            node_id_bb,
            mesh_id,
            is_publisher,
            backbone_interval_ms,
            registry_bb,
            backbone_seed_ids,
        )))
    } else {
        None
    };

    // Per-node cache engine: parse RAFKA_NODE_CACHES and spawn one task per
    // dedicated-channel cache. Admin assigns caches via this env var at spawn.
    // KeyGossip caches ride `main` via a separate projection fill task.
    let cache_specs = node_cache::parse_cache_specs_from_env();
    // Register all caches in the process-global registry.
    for spec in &cache_specs {
        let caches = node_cache::node_caches();
        if !caches.contains_key(&spec.name) {
            caches.insert(spec.name.clone(), node_cache::NodeCache::new(spec.clone()));
        }
    }
    // Determine leader for Leader-type caches.
    // In this spike: the admin (Observer role) is the leader.
    let is_leader = matches!(role, Role::Observer);
    // publisher_id: use node_id for Key-type caches so entries are keyed uniquely.
    let publisher_id = node_id.clone();
    let mut cache_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    for spec in &cache_specs {
        match &spec.channel {
            node_cache::Channel::Dedicated(_) => {
                let cache_name = spec.name.clone();
                let _is_publisher_not_keygossip = !matches!(spec.cache_type, node_cache::CacheType::KeyGossip);
                let leader: Option<String> = if matches!(spec.cache_type, node_cache::CacheType::Leader) {
                    // In Leader caches: only the Observer/admin publishes.
                    // Encode "admin is leader" as: leader = node_id if Observer, else no leader.
                    if is_leader { Some(node_id.clone()) } else {
                        // Non-leaders learn the leader (admin) node_id from the env
                        // var the admin sets at spawn, so they ACCEPT the leader's
                        // updates and reject anyone else.
                        std::env::var("RAFKA_LEADER_NODE_ID").ok()
                    }
                } else {
                    None
                };
                let publisher_flag = match spec.cache_type {
                    node_cache::CacheType::Leader => is_leader,
                    node_cache::CacheType::Shared => !matches!(role, Role::Observer),
                    node_cache::CacheType::Key => !matches!(role, Role::Observer),
                    node_cache::CacheType::KeyGossip => false, // KeyGossip never on dedicated channel
                };
                let gossip_task = gossip.clone();
                let peer_reg = Arc::clone(&peer_registry);
                let pub_id = publisher_id.clone();
                let h = tokio::spawn(node_cache::run_cache_task(
                    gossip_task,
                    cache_name,
                    publisher_flag,
                    pub_id,
                    peer_reg,
                    leader,
                ));
                cache_handles.push(h);
            }
            node_cache::Channel::Main => {
                // KeyGossip: project from live_digests() into the cache.
                let cache_name = spec.name.clone();
                let h = tokio::spawn(node_cache::run_keygossip_fill(cache_name));
                cache_handles.push(h);
            }
        }
    }

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
    topo_fill_handle.abort();
    if let Some(h) = dial_handle {
        h.abort();
    }
    if let Some(h) = write_sim_handle {
        h.abort();
    }
    if let Some(h) = backbone_handle {
        h.abort();
    }
    for h in cache_handles {
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

/// Write-sim sender (sprint-13 B4+B5). Runs only on gateways. Every
/// `RAFKA_WRITE_SIM_INTERVAL_MS` (default 5000) it resolves the target brokers
/// from the gossiped topology cache (`live_digests()` → `location`) and, for each,
/// opens a **bidirectional** iroh stream (request/response):
///   1. writes a `Write` (produce) frame carrying the gateway's W3C context;
///   2. reads the broker's `Ack` back on the response half;
///   3. opens an `ack-receive` span PARENTED to the broker's ack span context
///      (extracted from the ack frame) — that child-of-broker link is what draws
///      the reverse `broker → gateway` edge in Jaeger's System Architecture graph.
/// No admin-ui CC (B4): the Messages tab now derives from gossiped frame counters.
async fn run_write_sim(endpoint: iroh::Endpoint, own_node_id: String, own_node_name: String) {
    let interval_ms: u64 = std::env::var("RAFKA_WRITE_SIM_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);
    // Own mesh (intra) is resolved from live_digests(); the OTHER mesh (cross) is
    // resolved from the BACKBONE DIRECTORY (sprint-14, PRD 03 §5) — the gateway no
    // longer joins the other mesh's full gossip, so its live_digests() does not hold
    // remote nodes. We learn this gateway's own mesh from its own digest.
    let own_mesh: String = live_digests()
        .get(&own_node_id)
        .map(|e| e.value().mesh_id.clone())
        .unwrap_or_default();
    // Demo targets: a broker in mesh1 + a broker in mesh2. The mesh that is NOT our
    // own resolves via the backbone; our own resolves via local gossip.
    const TARGET_MESHES: &[&str] = &["mesh1", "mesh2"];
    const TARGET_TYPE: &str = "broker";

    // Resolve the first live broker in `mesh`. If `mesh` is our own, use
    // live_digests(); otherwise use the backbone directory.
    let resolve = |mesh: &str| -> Option<(String, String, EndpointAddr)> {
        let (name, nid, location) = if mesh == own_mesh {
            // Intra-mesh: full per-node detail is in our own gossip.
            live_digests()
                .iter()
                .find(|e| {
                    let d = e.value();
                    d.mesh_id == mesh && d.node_type == TARGET_TYPE && d.node_id != own_node_id
                })
                .map(|e| {
                    let d = e.value();
                    (d.node_name.clone(), d.node_id.clone(), d.location.clone())
                })?
        } else {
            // Cross-mesh: resolve from the backbone directory (control plane).
            let summary = backbone_summaries().get(mesh)?;
            let entry = summary
                .value()
                .directory
                .iter()
                .find(|e| e.node_type == TARGET_TYPE)?;
            (entry.node_name.clone(), entry.node_id.clone(), entry.location.clone())
        };
        if location.is_empty() {
            return None;
        }
        let pk = PublicKey::from_str(&nid).ok()?;
        let addr = location.parse::<SocketAddr>().ok()?;
        Some((name, nid, EndpointAddr::new(pk).with_ip_addr(addr)))
    };

    let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(interval_ms));
    let mut seq: u64 = 0;
    // Startup delay so the gossip cache has populated before the first send.
    tokio::time::sleep(tokio::time::Duration::from_secs(8)).await;
    loop {
        tick.tick().await;
        seq += 1;
        for mesh in TARGET_MESHES {
            let Some((target_name, dest_id, dest_addr)) = resolve(mesh) else { continue };
            produce_once(
                &endpoint,
                &own_node_id,
                &own_node_name,
                &dest_id,
                dest_addr,
                &target_name,
                seq,
            )
            .await;
        }
    }
}

/// One produce request/response round-trip (sprint-13 B5). Opens a bi-stream,
/// sends a `Write` frame (op_kind="produce") with the gateway's W3C context, then
/// reads the broker's `Ack` and opens an `ack-receive` span parented to the
/// broker's ack context. Bounded by a read timeout so a dead broker can't wedge
/// the loop.
async fn produce_once(
    endpoint: &iroh::Endpoint,
    own_node_id: &str,
    own_node_name: &str,
    dest_node_id: &str,
    dest_addr: EndpointAddr,
    target_name: &str,
    seq: u64,
) {
    let frame = InternalMeshFrame::Write {
        from: own_node_name.to_string(),
        to: target_name.to_string(),
        seq,
    };
    // produce frame.sent — op_kind="produce" (locked §10). This span is the trace
    // root for the whole produce→handle→ack→ack-receive chain; its context is what
    // the broker extracts and parents produce.handle onto.
    let sent_span = tracing::info_span!(
        "rafka.mesh.frame.sent",
        node_id = %own_node_id,
        peer_id = %dest_node_id,
        frame_kind = "write",
        op_kind = "produce",
        write_to = %target_name,
        seq = seq,
        otel.kind = "producer",
    );
    let _enter = sent_span.enter();
    let ctx = Span::current().context();
    let encoded = frame.encode_with_context(&ctx);
    drop(_enter);

    let conn = match endpoint.connect(dest_addr, ALPN).await {
        Ok(c) => c,
        Err(e) => {
            sent_span.in_scope(|| info!(target = %target_name, error = %e, "produce connect failed"));
            return;
        }
    };
    let (mut send, mut recv) = match conn.open_bi().await {
        Ok(pair) => pair,
        Err(e) => {
            sent_span.in_scope(|| info!(target = %target_name, error = %e, "produce open_bi failed"));
            return;
        }
    };
    if send.write_all(&encoded).await.is_err() || send.finish().is_err() {
        sent_span.in_scope(|| info!(target = %target_name, "produce write/finish failed"));
        return;
    }
    mesh_counters().frames_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    sent_span.in_scope(|| info!(target = %target_name, seq, "produce frame sent (bi)"));

    // Read the broker's ACK on the response half, bounded so a silent broker
    // can't stall the write-sim loop.
    let ack_bytes = match tokio::time::timeout(
        tokio::time::Duration::from_secs(3),
        recv.read_to_end(4096),
    )
    .await
    {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            sent_span.in_scope(|| info!(target = %target_name, error = %e, "produce ack read failed"));
            return;
        }
        Err(_) => {
            sent_span.in_scope(|| info!(target = %target_name, "produce ack timed out"));
            return;
        }
    };
    match InternalMeshFrame::decode_with_context(&ack_bytes) {
        Ok((broker_ack_ctx, InternalMeshFrame::Ack { from, seq: ack_seq })) => {
            mesh_counters().frames_recv.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // ack-receive parented to the BROKER's ack span (extracted) — this is
            // the cross-service link that produces the broker→gateway edge.
            let ack_span = tracing::info_span!(
                "rafka.mesh.frame.received",
                node_id = %own_node_id,
                peer_id = %dest_node_id,
                frame_kind = "ack",
                op_kind = "ack",
                ack_from = %from,
                seq = ack_seq,
                otel.kind = "consumer",
            );
            ack_span.set_parent(broker_ack_ctx);
            ack_span.in_scope(|| info!(target = %target_name, ack_from = %from, seq = ack_seq, "produce ack received"));
        }
        Ok((_, other)) => {
            sent_span.in_scope(|| info!(target = %target_name, ?other, "produce got non-ack response"));
        }
        Err(e) => {
            sent_span.in_scope(|| info!(target = %target_name, error = %e, "produce ack decode failed"));
        }
    }
}

/// Cross-mesh backbone task (sprint-14, PRD 03; extended sprint-18).
/// ONE task per gateway + admin-ui.
/// Subscribes to the single backbone topic `blake3("rafka.backbone")` and, each
/// interval:
///   - stores received `BackboneMessage::Summary` into `backbone_summaries()` +
///     emits `rafka.mesh.backbone.received` (SEPARATE from `topic_membership`).
///   - if `is_publisher`: runs the SOFT LEASE (§4) and, when it holds or wins the
///     lease, aggregates its own mesh from `live_digests()`, publishes a
///     `BackboneMessage::Summary`, and emits `rafka.mesh.backbone.published`.
///     Self-injects its own claim (iroh-gossip does not echo the sender's own
///     broadcast). A departed node simply drops out of `live_digests()` and thus
///     the next Summary — cross-mesh eviction needs no separate message.
///
/// NOT an election: leadership is the lease on the wire. `min(node_id)` over live
/// gateways breaks a tie ONLY for a vacant/expired seat; a live claim is never
/// preempted by a lower-id joiner.
async fn run_backbone(
    gossip: iroh_gossip::net::Gossip,
    backbone_topic: iroh_gossip::proto::TopicId,
    node_id: String,
    mesh_id: &'static str,
    is_publisher: bool,
    interval_ms: u64,
    registry: PeerRegistry,
    bootstrap_peers: Vec<PublicKey>,
) {
    use futures_lite::StreamExt;
    use iroh_gossip::api::Event;

    // Lease TTL = LEASE_MULTIPLIER × interval, so one missed renew does NOT trigger
    // failover but a dead publisher does within ~TTL.
    const LEASE_MULTIPLIER: u64 = 3;
    let ttl_ms = interval_ms.saturating_mul(LEASE_MULTIPLIER).max(3_000);

    let topic = match gossip.subscribe(backbone_topic, bootstrap_peers).await {
        Ok(t) => t,
        Err(e) => {
            tracing::info_span!("rafka.mesh.backbone.subscribe_failed", node_id = %node_id, error = %e)
                .in_scope(|| info!(error = %e, "backbone subscribe failed; backbone disabled for this node"));
            return;
        }
    };
    let (sender, mut receiver) = topic.split();

    tracing::info_span!(
        "rafka.mesh.backbone.subscribed",
        node_id = %node_id,
        mesh_id = mesh_id,
        is_publisher = is_publisher,
    )
    .in_scope(|| info!(mesh_id, is_publisher, "backbone topic subscribed"));

    // Track which peers we've fed to the gossip swarm (same join-peers discipline
    // as run_gossip — join only NEW peers, not every tick).
    let mut joined_peers: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(interval_ms));
    // Warm-up: let live_digests() + the backbone swarm populate before first publish.
    tokio::time::sleep(tokio::time::Duration::from_secs(8)).await;

    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Feed any peers reachable on our iroh endpoint into the backbone
                // swarm so it forms across meshes (the underlay is shared; the
                // topic is what's mesh-spanning here). Same join-only-NEW-peers
                // discipline as run_gossip.
                let mut new_peers: Vec<iroh::EndpointId> = Vec::new();
                for entry in registry.iter() {
                    let key = entry.key();
                    if !joined_peers.contains(key) {
                        if let Ok(id) = iroh::EndpointId::from_str(key) {
                            new_peers.push(id);
                            joined_peers.insert(key.clone());
                        }
                    }
                }
                joined_peers.retain(|p| registry.contains_key(p));
                if !new_peers.is_empty() {
                    let _ = sender.join_peers(new_peers).await;
                }

                if !is_publisher {
                    continue;
                }

                let now_ms = now_unix_ms();

                // ---- SOFT LEASE (PRD 03 §4) ----
                let claim = backbone_summaries().get(mesh_id).map(|e| {
                    (e.value().published_by.clone(), e.value().expires_at_ms)
                });
                let should_publish = match claim {
                    // Live claim held by SOMEONE ELSE → stay quiet (no preemption).
                    Some((holder, exp)) if now_ms < exp && holder != node_id => false,
                    // Live claim held by US → renew.
                    Some((holder, exp)) if now_ms < exp && holder == node_id => true,
                    // Vacant/expired seat → contend: lowest live ADMIN id wins.
                    // Post sole-listener (multi-mesh): only the node-admin (Observer)
                    // subscribes to + publishes on the backbone, so the publisher
                    // candidate set is the mesh's admin(s), NOT its gateways. With one
                    // admin per mesh this is always self → the admin always publishes
                    // its own mesh aggregate. (Was filtered on "gateway", which left
                    // an admin silent whenever a gateway's node_id sorted below it.)
                    _ => {
                        let mut live_admin_ids: Vec<String> = live_digests()
                            .iter()
                            .filter(|e| {
                                let d = e.value();
                                d.mesh_id == mesh_id && d.node_type == "node-admin"
                            })
                            .map(|e| e.value().node_id.clone())
                            .collect();
                        live_admin_ids.push(node_id.clone()); // include self
                        live_admin_ids.sort();
                        live_admin_ids.first().map(|m| m == &node_id).unwrap_or(true)
                    }
                };

                if !should_publish {
                    continue;
                }

                // ---- AGGREGATE our own mesh from live_digests() (local sum) ----
                let mut agg = MeshAggregate::default();
                let mut directory: Vec<MeshDirectoryEntry> = Vec::new();
                for e in live_digests().iter() {
                    let d = e.value();
                    if d.mesh_id != mesh_id { continue; }
                    agg.node_count += 1;
                    agg.cpu_used += d.cpu_used;
                    agg.cpu_budget += d.cpu_budget;
                    agg.ram_used += d.ram_used;
                    agg.ram_budget += d.ram_budget;
                    if !d.location.is_empty() {
                        directory.push(MeshDirectoryEntry {
                            node_name: d.node_name.clone(),
                            node_type: d.node_type.clone(),
                            node_id: d.node_id.clone(),
                            location: d.location.clone(),
                            state: d.state,
                            cpu_used: d.cpu_used,
                            cpu_budget: d.cpu_budget,
                            ram_used: d.ram_used,
                            ram_budget: d.ram_budget,
                        });
                    }
                }
                directory.sort_by(|a, b| a.node_name.cmp(&b.node_name));

                let summary = MeshSummary {
                    mesh_id: mesh_id.to_string(),
                    directory,
                    aggregate: agg.clone(),
                    published_by: node_id.clone(),
                    wall_time_ms: now_ms,
                    expires_at_ms: now_ms + ttl_ms,
                };

                // Self-inject (iroh-gossip does not echo our own broadcast) so our
                // own lease claim is visible to our next-tick lease check.
                backbone_summaries().insert(mesh_id.to_string(), summary.clone());

                // Sprint-18: wrap in BackboneMessage enum before encoding.
                let msg = BackboneMessage::Summary(summary);
                match postcard::to_allocvec(&msg) {
                    Ok(bytes) => {
                        if let Err(e) = sender.broadcast(bytes.into()).await {
                            tracing::info_span!("rafka.mesh.backbone.broadcast_failed", node_id = %node_id, error = %e)
                                .in_scope(|| info!(error = %e, "backbone broadcast failed"));
                        } else {
                            // PRD 03 §7 span: aggregate metrics ride as attributes
                            // (spans-only stack; no separate metrics SDK).
                            tracing::info_span!(
                                "rafka.mesh.backbone.published",
                                node_id = %node_id,
                                mesh_id = mesh_id,
                                publisher = %node_id,
                                node_count = agg.node_count as i64,
                                cpu_used = agg.cpu_used as f64,
                                cpu_budget = agg.cpu_budget as f64,
                                ram_used = agg.ram_used as f64,
                                ram_budget = agg.ram_budget as f64,
                            )
                            .in_scope(|| info!(mesh_id, node_count = agg.node_count, "backbone summary published"));
                        }
                    }
                    Err(e) => {
                        tracing::info_span!("rafka.mesh.backbone.encode_failed", node_id = %node_id, error = %e)
                            .in_scope(|| info!(error = %e, "backbone summary encode failed"));
                    }
                }
            }
            // Cross-mesh eviction needs NO separate message: when a node leaves,
            // its mesh evicts it (Leaving state), so this mesh's next MeshSummary
            // simply omits it → remote consumers replace their directory and the
            // departed node disappears (≤ one backbone interval). One state, one path.
            event = receiver.next() => {
                let Some(event) = event else { break };
                let event = match event { Ok(e) => e, Err(_) => continue };
                if let Event::Received(msg) = event {
                    match postcard::from_bytes::<BackboneMessage>(&msg.content) {
                        Ok(BackboneMessage::Summary(mut summary)) => {
                            let summary_mesh = summary.mesh_id.clone();
                            let publisher = summary.published_by.clone();
                            // Resurrection guard (sprint-17): a Summary published
                            // just BEFORE a kill can arrive AFTER we applied the
                            // backbone tombstone, re-inserting the dead node for up
                            // to one backbone interval. Drop any directory entry for
                            // a recently-tombstoned node_id (same window + map the
                            // gossip-digest receive path uses) so cross-mesh eviction
                            // is monotonic, not a flicker.
                            {
                                let guard = recently_evicted().lock().unwrap();
                                let now_ms = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_millis() as u64)
                                    .unwrap_or(0);
                                summary.directory.retain(|e| match guard.get(&e.node_id) {
                                    Some(&ts_ms) => now_ms.saturating_sub(ts_ms) >= EVICTION_GUARD_MS,
                                    None => true,
                                });
                            }
                            // Teach iroh each cross-mesh peer's address so a
                            // cross-mesh connect (write-sim / control op) can
                            // resolve by node_id alone — same fix as the per-mesh
                            // gossip receive, applied to the backbone directory.
                            for e in &summary.directory {
                                register_peer_location(&e.node_id, &e.location);
                            }
                            let node_count = summary.aggregate.node_count;
                            backbone_summaries().insert(summary_mesh.clone(), summary);
                            tracing::info_span!(
                                "rafka.mesh.backbone.received",
                                node_id = %node_id,
                                mesh_id = %summary_mesh,
                                publisher = %publisher,
                                node_count = node_count as i64,
                            )
                            .in_scope(|| info!(mesh_id = %summary_mesh, publisher = %publisher, "backbone summary received"));
                        }
                        Err(e) => {
                            // A decode failure here means a peer on an OLDER build is
                            // broadcasting a pre-BackboneMessage format (bare MeshSummary).
                            // Surfaced at debug to flag version skew without spamming;
                            // the staleness pruner is the fallback. (Caught a stale
                            // target/debug node binary during sprint-19 verification.)
                            tracing::debug!(size = msg.content.len(), error = %e, "backbone message decode failed (build/version skew?)");
                        }
                    }
                }
            }
        }
    }
}

/// Per-connection bi-stream reader. Loops on `conn.accept_bi()`, reads a complete
/// framed payload, and demuxes by tag:
///   - `0x11` (`TAG_BI_ECHO`) → echo the bytes back (data-plane sanity, unchanged).
///   - `0x10` (`TAG_LEGACY_FRAME`) carrying a `Write` → the **produce handler**
///     (sprint-13 B5): extract the gateway's W3C context, open `produce.handle`
///     parented to it, then `produce.ack` as a child, and write an `Ack` back with
///     produce.ack's context injected so the gateway's ack-receive is a child of
///     the broker → reverse `broker → gateway` edge.
async fn run_bi_reader(
    conn: iroh::endpoint::Connection,
    own_node_id: String,
    own_node_name: &'static str,
    peer_id_str: String,
) {
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
        // Demux by tag. (if/else, not match: framer::TAG_LEGACY_FRAME is a path
        // const, which a match arm would treat as a catch-all binding.)
        let tag = bytes[0];
        if tag == TAG_BI_ECHO {
            let recv_span = tracing::trace_span!(
                "rafka.mesh.bi.echo_received",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                size_bytes = bytes.len() as i64,
            );
            recv_span.in_scope(|| tracing::trace!(size = bytes.len(), "bi echo received"));
            if send.write_all(&bytes).await.is_err() || send.finish().is_err() {
                tracing::trace_span!(
                    "rafka.mesh.bi.write_failed",
                    node_id = %own_node_id,
                    peer_id = %peer_id_str,
                )
                .in_scope(|| tracing::trace!("bi echo write/finish failed"));
                continue;
            }
        } else if tag == framer::TAG_LEGACY_FRAME {
            // A Shutdown control op self-terminates this node; anything else
            // (a Write produce) goes to the produce handler. (The Shutdown branch
            // only matches that variant; a Write falls through to handle_produce_bi.)
            match InternalMeshFrame::decode_with_context(&bytes) {
                Ok((_ctx, InternalMeshFrame::Shutdown { reason })) => {
                    handle_shutdown_op(&own_node_id, &peer_id_str, &reason).await;
                    break;
                }
                // Sprint-21 lifecycle op — NON-terminal: apply the self-state
                // override and keep serving (the node lives on, unlike Shutdown).
                Ok((_ctx, InternalMeshFrame::SetState { state })) => {
                    handle_set_state_op(&own_node_id, &peer_id_str, &state);
                }
                // B5: a produce arriving on a bi-stream. Decode + handle + ACK.
                _ => {
                    handle_produce_bi(&own_node_id, own_node_name, &peer_id_str, &bytes, &mut send).await;
                }
            }
        } else if tag == TAG_SNAPSHOT_REQ {
            // Cold-pull DONOR: a peer whose view collapsed wants our live mesh view.
            // The request carries the requester's mesh_id after the tag byte; we
            // serve ONLY that mesh's digests. Critical with MANY node-admins (one
            // per mesh): a node must never hydrate another mesh's nodes into its
            // live_digests, even if it somehow dialed the wrong admin. Empty mesh_id
            // (legacy/unscoped request) → serve all. Non-terminal entries only —
            // never ship a Leaving/Dead state as if it were live.
            let req_mesh = String::from_utf8_lossy(&bytes[1..]).to_string();
            let snapshot: Vec<GossipDigest> = live_digests()
                .iter()
                .map(|e| e.value().clone())
                .filter(|d| !matches!(d.state, NodeState::Leaving | NodeState::Dead))
                .filter(|d| req_mesh.is_empty() || d.mesh_id == req_mesh)
                .collect();
            let count = snapshot.len();
            let payload = postcard::to_allocvec(&snapshot).unwrap_or_default();
            if send.write_all(&payload).await.is_err() || send.finish().is_err() {
                tracing::trace!(peer_id = %peer_id_str, "cold-pull donor write failed");
                continue;
            }
            tracing::info_span!(
                "rafka.mesh.coldpull.served",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                count = count as i64,
                "otel.kind" = "producer",
            )
            .in_scope(|| info!(peer_id = %peer_id_str, count, "cold-pull snapshot served to re-joining peer"));
        } else {
            tracing::trace_span!(
                "rafka.mesh.bi.unknown_tag",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                tag = tag as i64,
                size_bytes = bytes.len() as i64,
            )
            .in_scope(|| tracing::trace!(tag, "bi-stream unknown tag — dropping"));
        }
    }
}

/// Broker-side produce handler (sprint-13 B5). Given the raw `Write` frame bytes
/// read off a bi-stream, this:
///   1. extracts the gateway's W3C context from the frame;
///   2. opens `rafka.mesh.produce.handle` PARENTED to it (the broker's own work
///      becomes visible in the gateway's trace — `gateway → broker` edge);
///   3. opens `rafka.mesh.produce.ack` as a child of handle, and writes an `Ack`
///      frame back carrying produce.ack's OWN context (NOT the echoed gateway
///      context) so the gateway's ack-receive parents onto the broker → the
///      reverse `broker → gateway` edge appears.
async fn handle_produce_bi(
    own_node_id: &str,
    own_node_name: &str,
    peer_id_str: &str,
    bytes: &[u8],
    send: &mut iroh::endpoint::SendStream,
) {
    let (parent_ctx, frame) = match InternalMeshFrame::decode_with_context(bytes) {
        Ok(pair) => pair,
        Err(e) => {
            tracing::info_span!(
                "rafka.mesh.frame.decode_failed",
                node_id = %own_node_id,
                peer_id = %peer_id_str,
                error = %e,
                otel.kind = "consumer",
            )
            .in_scope(|| info!(peer_id = %peer_id_str, "produce decode failed"));
            return;
        }
    };
    let (write_from, write_to, seq) = match frame {
        InternalMeshFrame::Write { from, to, seq } => (from, to, seq),
        other => {
            tracing::trace!(peer_id = %peer_id_str, ?other, "bi 0x10 frame not a Write — ignoring");
            return;
        }
    };
    mesh_counters().frames_recv.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Surface in the (now CC-free) Messages ring too — this is a REAL received frame.
    let peer_prefix: String = peer_id_str.chars().take(8).collect();
    push_message(
        peer_id_str,
        "write",
        bytes.len(),
        format!("[{peer_prefix}] produce {write_from}→{write_to} #{seq}"),
    );

    // produce.handle — the broker's own work, parented to the gateway's context.
    let handle_span = tracing::info_span!(
        "rafka.mesh.produce.handle",
        node_id = %own_node_id,
        peer_id = %peer_id_str,
        op_kind = "produce",
        write_from = %write_from,
        write_to = %write_to,
        seq = seq,
        otel.kind = "consumer",
    );
    handle_span.set_parent(parent_ctx);

    // Build + send the ACK from WITHIN a produce.ack span that is a child of
    // produce.handle. Inject produce.ack's OWN context into the ack frame.
    let encoded_ack = handle_span.in_scope(|| {
        info!(write_from = %write_from, seq, "produce handled");
        let ack_span = tracing::info_span!(
            "rafka.mesh.produce.ack",
            node_id = %own_node_id,
            peer_id = %peer_id_str,
            op_kind = "ack",
            seq = seq,
            otel.kind = "producer",
        );
        ack_span.in_scope(|| {
            let ack = InternalMeshFrame::Ack { from: own_node_name.to_string(), seq };
            // inject THIS span (produce.ack) so the gateway parents onto the broker.
            let ctx = Span::current().context();
            ack.encode_with_context(&ctx)
        })
    });

    if send.write_all(&encoded_ack).await.is_err() || send.finish().is_err() {
        tracing::info_span!(
            "rafka.mesh.frame.sent_failed",
            node_id = %own_node_id,
            peer_id = %peer_id_str,
            frame_kind = "ack",
            otel.kind = "producer",
        )
        .in_scope(|| info!(peer_id = %peer_id_str, "ack write/finish failed"));
        return;
    }
    mesh_counters().frames_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    handle_span.in_scope(|| info!(seq, "produce ack sent"));
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
    /// Reachable bind address for this node (e.g. "127.0.0.1:15820").
    /// Sourced from RAFKA_NODE_BIND_ADDR at startup. Used by the topology
    /// cache as the `location` field: a peer that wants to connect to this
    /// node resolves its name → location and dials that address directly.
    /// Added in mesh-v2 Phase 1; absent in old digests (postcard compat:
    /// only nodes on the same gossip topic = same build = same struct layout).
    #[serde(default)]
    pub location: String,
    /// Node lifecycle/health state (sprint-20) — published as an EVENT on every
    /// transition; generalizes the sprint-15 `leaving` tombstone flag. Self-
    /// published for Joining/Alive/Degraded/Updating/Draining/Leaving; `Dead` is
    /// observer-assigned (a node that vanished without `Leaving`). Receivers evict
    /// on `Leaving`/`Dead` (the old fast-delete); other states upsert + render.
    #[serde(default)]
    pub state: NodeState,
    /// True if this node was spawned with `RAFKA_STATEFUL=true`, meaning its data
    /// dir is NOT auto-wiped on kill/crash — it will carry the same node identity
    /// (node-identity.json) across restarts. Appended last so the postcard layout
    /// stays append-only; `#[serde(default)]` = false for old digests.
    #[serde(default)]
    pub stateful: bool,
    /// Hex-encoded node-admin-signed cert (cert::SignedCert). Empty if the node
    /// was spawned without one. Receivers verify it (CA sig + node_id + expiry)
    /// before admitting the node to live_digests — the trust boundary.
    #[serde(default)]
    pub cert: String,
}

/// Node lifecycle/health, published as an event on every transition (sprint-20;
/// generalizes the sprint-15 tombstone). LOCKED, append-only enum (CLAUDE.md §10):
/// never remove or repurpose a variant; new variants append at the END (the
/// postcard discriminant is positional). Self-published: Joining/Alive/Degraded/
/// Updating/Draining/Leaving. Observer-inferred: Dead (a node that vanished with
/// no Leaving — the staleness/crash fallback, now a visible state).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum NodeState {
    Joining,
    #[default]
    Alive,
    Degraded,
    Updating,
    Draining,
    Leaving,
    Dead,
}

impl NodeState {
    /// Parse a NodeState from its `{:?}` variant name (the form sent over the
    /// SetState control op and rendered in the UI). Returns None on unknown.
    pub fn from_name(s: &str) -> Option<NodeState> {
        match s {
            "Joining" => Some(NodeState::Joining),
            "Alive" => Some(NodeState::Alive),
            "Degraded" => Some(NodeState::Degraded),
            "Updating" => Some(NodeState::Updating),
            "Draining" => Some(NodeState::Draining),
            "Leaving" => Some(NodeState::Leaving),
            "Dead" => Some(NodeState::Dead),
            _ => None,
        }
    }
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

// ===========================================================================
// Node-state eviction (terminal states) + the immediate self-publish trigger
// ===========================================================================
// There is ONE departure mechanism: a node publishes its state, and observers
// act on the received state — Leaving (graceful) and Dead (observer-inferred via
// staleness) both mean "evict." No separate "tombstone" subsystem and no
// third-party broadcast: a node only ever publishes its OWN state. A leaving node
// sets state=Leaving and triggers an immediate self-publish (publish_now) so the
// departure doesn't wait for the next periodic tick.

/// Fired by a node when it wants its current state broadcast NOW (e.g. it set
/// state=Leaving on shutdown) instead of waiting for the periodic gossip tick.
/// `run_gossip`'s primary-topic task selects on it and publishes the real digest.
static SELF_PUBLISH_NOW: std::sync::OnceLock<std::sync::Arc<tokio::sync::Notify>> =
    std::sync::OnceLock::new();

fn publish_now() -> &'static std::sync::Arc<tokio::sync::Notify> {
    SELF_PUBLISH_NOW.get_or_init(|| std::sync::Arc::new(tokio::sync::Notify::new()))
}

/// Recently-evicted node_ids: `node_id → unix_ms of eviction`. Resurrection guard:
/// a live digest that was in-flight when a node departed can arrive AFTER the
/// Leaving/Dead state and re-insert it. The receive path checks this set; within
/// EVICTION_GUARD_MS a stale digest for an evicted node is dropped. This is
/// state-receive correctness (don't act on an older state), not a separate system.
static RECENTLY_EVICTED: std::sync::OnceLock<
    Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>>,
> = std::sync::OnceLock::new();

/// The resurrection guard is honoured for this long after an eviction. Long enough
/// to outlast in-flight Plumtree relays (< 2 s on loopback), short enough not to
/// block a legitimate respawn (which re-mints a new identity / node_id anyway).
const EVICTION_GUARD_MS: u64 = 10_000;

fn recently_evicted(
) -> &'static Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>> {
    RECENTLY_EVICTED.get_or_init(|| Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())))
}

/// Evict `node_id` because we observed it in a terminal state — either a received
/// `Leaving`/`Dead` digest (`source="gossip_receive"`) or the staleness pruner
/// inferring `Dead` (`source="staleness_dead"`). Removes it from all three
/// process-global maps and records it in the resurrection guard. Emits
/// `rafka.mesh.node.evicted` on the first eviction (the UI reflects the removal 1:1).
pub fn evict_node(node_id: &str, source: &str) {
    // Gate the span on whether the node was actually present: Plumtree fanout
    // delivers the same state ~17× per observer; the FIRST removal returns Some,
    // the rest are no-ops.
    let was_present = live_digests().remove(node_id).is_some();
    last_seen_ms().lock().unwrap().remove(node_id);
    for mut entry in topic_membership().iter_mut() {
        entry.value_mut().remove(node_id);
    }
    // The resurrection guard applies ONLY to a DEFINITIVE departure (a received
    // `Leaving` — source "gossip_receive"): there, a stale in-flight digest must
    // not re-add the node. A `staleness_dead` eviction is an INFERENCE — the node
    // went quiet but may be alive-but-slow. Guarding it would block its own
    // re-published digests and trap a live node permanently evicted. So do NOT
    // guard staleness: a still-alive node recovers on its next digest; a truly
    // dead one is simply re-evicted on the next sweep.
    if source != "staleness_dead" {
        recently_evicted().lock().unwrap().insert(node_id.to_string(), now_unix_ms());
    }

    if was_present {
        tracing::info_span!(
            "rafka.mesh.node.evicted",
            node_id = node_id,
            source = source,
            "otel.kind" = "internal",
        )
        .in_scope(|| info!(node_id, source, "node evicted (terminal state) from all gossip maps"));
    }
}

// ===========================================================================
// Cross-mesh backbone (sprint-14, PRD 03)
// ===========================================================================

/// One directory entry in a `MeshSummary`: how to reach a node in that mesh.
///
/// PRD 03 §2 lists `{node_name, node_type, location}`. We ALSO carry `node_id`:
/// iroh is identity-based — `endpoint.connect` needs the target's PublicKey, and
/// `node_name` only embeds 6 hex of the id (not reconstructable). Without it a
/// cross-mesh write resolved from the backbone could not actually connect. Still
/// low-churn summary data (changes only on spawn/kill), consistent with §2's intent.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MeshDirectoryEntry {
    pub node_name: String,
    pub node_type: String,
    pub node_id: String,
    pub location: String,
    /// Sprint-20: the node's lifecycle/health state so a cross-mesh console can
    /// render it (green=Alive, amber=Degraded, …). Appended LAST — postcard
    /// discriminant is positional; `#[serde(default)]` decodes pre-sprint-20
    /// summaries (which lacked it) as the enum default (Alive).
    #[serde(default)]
    pub state: NodeState,
    /// PER-NODE CPU/RAM carried over the backbone so a cross-mesh console renders
    /// each remote node's real metrics (cores / GB), not blank boxes. (Appended,
    /// serde(default)=0.) The mesh-level rollup still rides `MeshAggregate`; this
    /// is the per-node detail.
    #[serde(default)] pub cpu_used: f32,
    #[serde(default)] pub cpu_budget: f32,
    #[serde(default)] pub ram_used: f32,
    #[serde(default)] pub ram_budget: f32,
}

/// Mesh-level rollup carried on the backbone (PRD 03 §2). The heavy per-node
/// churn (every broker's CPU each tick) never leaves its mesh — only this sum does.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MeshAggregate {
    pub node_count: u64,
    pub cpu_used: f32,
    pub cpu_budget: f32,
    pub ram_used: f32,
    pub ram_budget: f32,
    pub frames_per_sec: f32,
}

/// One record per mesh, published to the backbone topic each interval by that
/// mesh's elected (soft-lease) gateway. Consumers key by `mesh_id` (last-writer-
/// wins). `published_by` + `expires_at_ms` carry the soft lease (PRD 03 §4).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MeshSummary {
    pub mesh_id: String,
    pub directory: Vec<MeshDirectoryEntry>,
    pub aggregate: MeshAggregate,
    /// node_id of the publishing gateway (the soft-lease holder).
    pub published_by: String,
    pub wall_time_ms: u64,
    /// Soft-lease expiry: a claim is LIVE while `now < expires_at_ms`. A live
    /// claim is NOT preempted by a lower-id gateway; leadership changes only when
    /// the claim expires (dead-man's switch).
    pub expires_at_ms: u64,
}

/// Process-global map of the latest `MeshSummary` per mesh_id, received over the
/// backbone topic (and self-injected by the publisher, since iroh-gossip does not
/// echo a node's own broadcast). This is the CONTROL-PLANE view: every gateway +
/// admin-ui reads it to resolve cross-mesh writes (directory) and render the
/// global view (aggregate). It is SEPARATE from `topic_membership` — a backbone
/// summary received from mesh2 does NOT make this node a member of mesh2's gossip.
static BACKBONE_SUMMARIES: std::sync::OnceLock<Arc<DashMap<String, MeshSummary>>> =
    std::sync::OnceLock::new();

pub fn backbone_summaries() -> &'static Arc<DashMap<String, MeshSummary>> {
    BACKBONE_SUMMARIES.get_or_init(|| Arc::new(DashMap::new()))
}

/// The fixed backbone topic name. `blake3("rafka.backbone")` is the gossip topic
/// id — one additional iroh-gossip topic shared by ALL meshes (PRD 03 §1). Not a
/// DHT, not a new mesh primitive: the same gossip plane, a new topic.
pub const BACKBONE_TOPIC_NAME: &str = "rafka.backbone";

// ===========================================================================
// Sprint-18: Backbone tombstone — cross-mesh fast eviction
// ===========================================================================

/// Wire message type for the backbone topic. A single tagged enum (kept as an
/// enum for forward-compatibility even though only Summary exists now). Cross-mesh
/// departure rides the Summary itself: a departed node simply drops out of its
/// mesh's next directory — no separate eviction message.
///
/// All backbone nodes must be on the same build (same binary version) so
/// postcard cross-version compat is not a concern.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum BackboneMessage {
    /// Normal per-mesh aggregate + directory, published each interval by the
    /// soft-lease publisher (sprint-14, PRD 03 §3/§4).
    Summary(MeshSummary),
}

/// Default staleness window for the process-global mesh-state pruner. A
/// `GossipDigest` whose `wall_time_ms` is older than this is treated as
/// "the source node is gone" and removed from `live_digests` +
/// `topic_membership`. At the default 2s gossip cadence this is 15 missed
/// cycles — well past any reasonable flake. Override with `RAFKA_STALENESS_MS`.
///
/// NOTE (2026-06-13): this EQUALS the 30s gossip re-broadcast floor, which causes a
/// cosmetic evict/re-add flicker of stable nodes in the topology view (a keepalive
/// can land a beat after the prune deadline). Kept at 30s deliberately — crisp
/// crash-detection is preferred over smoothing the harmless flicker; the mesh holds
/// either way. If smoothing is ever wanted, raise THIS above the re-broadcast floor
/// (never raise the floor above this, or live nodes get pruned before their keepalive).
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

        // Sprint-15: expire old recently_evicted entries every sweep so
        // the map doesn't grow unbounded under churn. Entries older than
        // EVICTION_GUARD_MS are past the resurrection window and can be
        // removed without risk of allowing a re-insert of the dead node.
        {
            let mut guard = recently_evicted().lock().unwrap();
            guard.retain(|_, &mut ts_ms| now_ms.saturating_sub(ts_ms) < EVICTION_GUARD_MS);
        }

        if stale.is_empty() {
            continue;
        }

        // Sprint-20: a node that went stale WITHOUT announcing Leaving is
        // observer-inferred DEAD (crash/vanish, no graceful departure). Route
        // each eviction through evict_node with source="staleness_dead" so
        // it (a) clears all three maps + the resurrection guard uniformly and
        // (b) emits a tombstone.applied span whose `source` DISTINGUISHES Dead
        // (staleness) from Leaving (the graceful gossip tombstone). Both are
        // removed from the directory; the eviction event is what tells them apart.
        for node_id in &stale {
            evict_node(node_id, "staleness_dead");
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
    // Reachable bind address string broadcast in GossipDigest.location
    // so peers can look up this node's address from the topology cache.
    location: String,
    // Sprint-15: tombstone outbox. Only the PRIMARY-topic task holds
    // a receiver (extra-topic tasks pass None). When a node_id arrives
    // here, run_gossip broadcasts a leaving=true digest on this topic.
    // Only the primary mesh-topic task self-publishes immediate state changes
    // (e.g. Leaving on shutdown); extra observer-topic tasks do not.
    is_primary: bool,
) {
    let counters = mesh_counters();
    // cpu_used/ram_used overrides come from RAFKA_DEV_CPU_USED / RAFKA_DEV_RAM_USED
    // (dev-gated). These let a test put a node deterministically over budget so it
    // self-reports NodeState::Degraded — the documented override, now wired.
    let load_sampler = LoadSampler::new(
        cpu_budget,
        ram_budget,
        read_dev_cpu_used(),
        read_dev_ram_used(),
    );
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
    // Sprint-20 self-state: a node is Joining until it has published its first
    // digest (booting/joining), then Alive — or Degraded while over its own
    // CPU/RAM budget. Self-published states ride the normal periodic digest;
    // a state CHANGE forces the next tick's broadcast (see should_broadcast),
    // so transitions propagate in ≤1 gossip interval with NO new channel.
    let mut published_once = false;
    // ONE place that turns "my current load + lifecycle state" into a digest. Both
    // the periodic tick AND an immediate state-change publish (a node announcing
    // Leaving as it shuts down) build the SAME digest through this — there is no
    // separate "tombstone" wire shape. A node publishes its state; observers act on
    // the received state (Leaving/Dead → evict, else upsert). 1:1 with what the UI
    // shows.
    let make_digest = |published_once: bool, load: NodeLoad| -> GossipDigest {
        use std::sync::atomic::Ordering;
        let over_budget = (load.cpu_budget > 0.0 && load.cpu_used > load.cpu_budget)
            || (load.ram_budget > 0.0 && load.ram_used > load.ram_budget);
        let auto = if !published_once {
            NodeState::Joining
        } else if over_budget {
            NodeState::Degraded
        } else {
            NodeState::Alive
        };
        // Operator override (Updating/Draining) or self-departure (Leaving) wins over
        // the auto health state; otherwise the auto Joining/Degraded/Alive applies.
        let state = match *requested_state().lock().unwrap() {
            Some(req @ (NodeState::Updating | NodeState::Draining | NodeState::Leaving)) => req,
            _ => auto,
        };
        let peer_ids: Vec<String> = registry.iter().map(|e| e.key().clone()).collect();
        let stateful = std::env::var("RAFKA_STATEFUL").as_deref() == Ok("true");
        // This node's cert (node-admin-signed). Presented in every digest so
        // peers can verify the node belongs on the mesh.
        let cert = std::env::var("RAFKA_NODE_CERT").unwrap_or_default();
        GossipDigest {
            node_id: node_id.clone(),
            node_name: node_name.to_string(),
            mesh_id: mesh_id.to_string(),
            node_type: node_type.clone(),
            peer_count: registry.len() as u64,
            peer_ids,
            frames_sent_total: counters.frames_sent.load(Ordering::Relaxed),
            frames_recv_total: counters.frames_recv.load(Ordering::Relaxed),
            wall_time_ms: now_unix_ms(),
            cpu_used: load.cpu_used,
            cpu_budget: load.cpu_budget,
            ram_used: load.ram_used,
            ram_budget: load.ram_budget,
            location: location.clone(),
            state,
            stateful,
            cert,
        }
    };
    // CA pubkey for cert enforcement: every received digest's cert is verified
    // against this before the node is admitted to live_digests. Unset (no CA)
    // => enforcement OFF (open mesh, backward compatible).
    let ca_pubkey: Option<iroh::PublicKey> = std::env::var("RAFKA_CA_PUBKEY")
        .ok()
        .and_then(|s| iroh::PublicKey::from_str(s.trim()).ok());
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
                ticks_since_sample += 1;
                if ticks_since_sample >= 10 {
                    current_load = load_sampler.sample();
                    ticks_since_sample = 0;
                }
                let digest = make_digest(published_once, current_load);
                published_once = true;
                
                let mut should_broadcast = false;
                if let Some(last) = &last_digest {
                    if last.peer_count != digest.peer_count || last.peer_ids != digest.peer_ids {
                        should_broadcast = true;
                    }
                    // Sprint-20: a lifecycle/health transition (e.g. Joining→Alive,
                    // Alive→Degraded) forces the next publish so state changes
                    // propagate in ≤1 interval rather than waiting for the 30s floor.
                    if last.state != digest.state {
                        should_broadcast = true;
                        // Durable per-transition lifecycle trace (the §10
                        // node.state_changed span the sprint-20 config promised).
                        // One span per self-state transition for this node_id, so
                        // Jaeger holds the FULL lifecycle (Joining→Alive→Degraded→
                        // Updating→Draining→…) even for states too fleeting to catch
                        // in the UI. source="self" (observer-inferred Dead + the
                        // Leaving evict are recorded by tombstone.applied instead).
                        tracing::info_span!(
                            "rafka.mesh.node.state_changed",
                            node_id = %node_id,
                            node_name = %node_name,
                            from = ?last.state,
                            to = ?digest.state,
                            source = "self",
                            "otel.kind" = "internal",
                        )
                        .in_scope(|| info!(node = %node_name, from = ?last.state, to = ?digest.state, "node state changed"));
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
                    // Feed the REAL membership gossip into the `main` channel so the
                    // Channels view shows main chatting continuously (state/load digests).
                    node_cache::channel_events()
                        .entry("main".to_string())
                        .or_insert_with(|| node_cache::ChannelEventRing::new("main"))
                        .push(node_cache::ChannelEvent {
                            ts_ms: now_unix_ms(),
                            publisher: node_id.clone(),
                            op: "publish".to_string(),
                            key: format!("{} {:?}", node_name, digest.state),
                            value: digest.peer_count,
                            epoch: digest.wall_time_ms,
                            cache_name: "main".to_string(),
                        });
                }
            }
            // Immediate self-publish: a node announcing a state change NOW instead of
            // waiting for the next periodic tick — used when it sets state=Leaving as
            // it shuts down. It publishes its OWN real digest through `make_digest`
            // (the SAME path as the tick); observers act on the received state
            // (Leaving → evict). There is no separate "tombstone" wire shape and no
            // third-party broadcast — a node only ever publishes its own state.
            // Only the primary-topic task self-publishes.
            _ = async {
                if is_primary { publish_now().notified().await } else { std::future::pending().await }
            } => {
                let digest = make_digest(true, current_load);
                // Lifecycle trace: emit the transition (e.g. →Leaving) like the tick.
                if last_digest.as_ref().map(|l| l.state) != Some(digest.state) {
                    tracing::info_span!(
                        "rafka.mesh.node.state_changed",
                        node_id = %node_id,
                        node_name = %node_name,
                        from = ?last_digest.as_ref().map(|l| l.state),
                        to = ?digest.state,
                        source = "self",
                        "otel.kind" = "internal",
                    )
                    .in_scope(|| info!(node = %node_name, to = ?digest.state, "node state changed (immediate publish)"));
                }
                last_digest = Some(digest.clone());
                last_broadcast_time = digest.wall_time_ms;
                match postcard::to_allocvec(&digest) {
                    Ok(payload) => {
                        let size = payload.len();
                        if let Err(e) = sender.broadcast(payload.into()).await {
                            tracing::info_span!("rafka.mesh.gossip.broadcast_failed", node_id = %node_id, error = %e)
                                .in_scope(|| info!(error = %e, "immediate state publish failed"));
                        } else {
                            tracing::info_span!(
                                "rafka.mesh.gossip.broadcast",
                                node_id = %node_id,
                                mesh_id = mesh_id,
                                size_bytes = size as i64,
                            )
                            .in_scope(|| info!(size_bytes = size, state = ?digest.state, "immediate state publish"));
                        }
                    }
                    Err(e) => info!(error = %e, "immediate digest encode failed"),
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
                    if let Some(d) = &digest {
                        // ── TRUST BOUNDARY: verify the node's cert before admitting it ──
                        // If a CA is configured, the digest must carry a cert signed by
                        // that CA, binding the sender's NodeId, not expired. No valid
                        // cert → drop the digest entirely: the node never enters
                        // live_digests, so it is invisible and cannot communicate.
                        if let Some(ca_pub) = &ca_pubkey {
                            let ok = crate::cert::decode_cert(&d.cert)
                                .map(|signed| crate::cert::verify_cert(&signed, ca_pub, &d.node_name, now_unix_ms()))
                                .unwrap_or(Err(crate::cert::CertError::BadSignature));
                            if let Err(reason) = ok {
                                tracing::info_span!(
                                    "rafka.cert.reject",
                                    rejected_node = %d.node_name,
                                    rejected_node_id = %d.node_id,
                                    reason = %reason.as_str(),
                                    "otel.kind" = "internal",
                                )
                                .in_scope(|| tracing::warn!(node = %d.node_name, reason = %reason.as_str(), "digest REJECTED — invalid/missing cert; node not admitted to mesh"));
                                continue;
                            }
                        }
                        // Feed the received membership digest into the `main` channel
                        // (the Channels view's real-gossip stream).
                        node_cache::channel_events()
                            .entry("main".to_string())
                            .or_insert_with(|| node_cache::ChannelEventRing::new("main"))
                            .push(node_cache::ChannelEvent {
                                ts_ms: now_unix_ms(),
                                publisher: d.node_id.clone(),
                                op: "received".to_string(),
                                key: format!("{} {:?}", d.node_name, d.state),
                                value: d.peer_count,
                                epoch: d.wall_time_ms,
                                cache_name: "main".to_string(),
                            });
                        // Sprint-15/20: terminal-state path — FAST eviction.
                        // A digest in a terminal state (Leaving = graceful departure,
                        // Dead = observer-inferred crash) means the node is gone.
                        // Immediately evict from all three process-global maps. This
                        // is the gossip-native, uniform mechanism: every subscriber's
                        // receive path runs this code, so all observers (home-mesh
                        // admin-ui, gateways, backbone publisher) evict simultaneously
                        // — NOT via a local hand-edit. Non-terminal states (Joining/
                        // Alive/Degraded/Updating/Draining) fall through and upsert.
                        if matches!(d.state, NodeState::Leaving | NodeState::Dead) {
                            evict_node(&d.node_id, "gossip_receive");
                            continue;
                        }

                        // Resurrection guard: if a live digest arrives for a
                        // recently-tombstoned node_id, drop it silently.
                        // In-flight Plumtree relay hops can deliver a normal
                        // digest that was in transit just before the kill — we
                        // must not let it re-insert the dead node.
                        {
                            let guard = recently_evicted().lock().unwrap();
                            if let Some(&ts_ms) = guard.get(&d.node_id) {
                                let now_ms = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_millis() as u64)
                                    .unwrap_or(0);
                                if now_ms.saturating_sub(ts_ms) < EVICTION_GUARD_MS {
                                    // Still within the guard window — drop silently.
                                    continue;
                                }
                            }
                        }

                        // ── NAME-REBIND (identity rotation) ──
                        // node_name is the STABLE logical identity; node_id is the
                        // MUTABLE transport id. If this name already lives under a
                        // DIFFERENT node_id, the node rotated its id (a worker restart),
                        // so evict the stale old id(s) — the topology shows ONE entry per
                        // name, bound to the CURRENT id. evict_node records the old id in
                        // recently_evicted, so late Plumtree relays still carrying it are
                        // dropped → freshest-wins, no X/Y flip-flop. (Names are unique
                        // per (mesh,type), so a same-name/different-id collision can ONLY
                        // be a rotation, never two distinct live nodes.) Skip when the
                        // digest is our own self-injection.
                        if d.node_id != node_id && !d.node_name.is_empty() {
                            let stale_ids: Vec<String> = live_digests()
                                .iter()
                                .filter(|e| {
                                    let o = e.value();
                                    o.node_name == d.node_name && o.node_id != d.node_id
                                })
                                .map(|e| e.key().clone())
                                .collect();
                            for stale in stale_ids {
                                tracing::info_span!(
                                    "rafka.mesh.node.rebound",
                                    node_name = %d.node_name,
                                    old_node_id = %stale,
                                    new_node_id = %d.node_id,
                                    "otel.kind" = "internal",
                                )
                                .in_scope(|| info!(node_name = %d.node_name, old = %stale, new = %d.node_id, "name rebound to a new identity — evicting stale node_id"));
                                evict_node(&stale, "name_rebind");
                            }
                        }

                        // Normal live digest — insert into the process-global maps.
                        live_digests().insert(d.node_id.clone(), d.clone());
                        // Teach iroh this peer's address so a later join_peers/connect
                        // resolves by node_id without a failed discovery lookup.
                        register_peer_location(&d.node_id, &d.location);
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
                        .map(|d| {
                            if matches!(d.state, NodeState::Leaving | NodeState::Dead) {
                                format!("{:?} node={}", d.state, d.node_id)
                            } else {
                                format!("node={}/peers={}/state={:?}", d.node_name, d.peer_count, d.state)
                            }
                        })
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

/// Cold-pull REQUESTER (warm re-hydration). Given an already-established data-plane
/// connection to the node-admin (the dial_seeds connection — so this costs NO extra
/// dial and dodges iroh's ~60s stale-path reconnect delay), open a bi-stream, ask
/// for a snapshot (`TAG_SNAPSHOT_REQ`), and hydrate our process-global maps from the
/// donor's `live_digests`. Fires on every (re)connect to the node-admin: at boot it
/// seeds the view immediately (like birth-injection, but live); after a view-collapse
/// it re-acquires the mesh deterministically instead of waiting on gossip re-flood.
async fn pull_topology_snapshot(conn: &Connection, own_node_id: &str, own_mesh: &str, donor_id: &str) {
    let (mut send, mut recv) = match conn.open_bi().await {
        Ok(p) => p,
        Err(e) => {
            tracing::trace!(donor = %donor_id, error = %e, "cold-pull open_bi failed");
            return;
        }
    };
    // Request = tag byte + our mesh_id, so the donor scopes the snapshot to OUR mesh
    // (it may be one of several node-admins / host a different mesh's view).
    let mut req = Vec::with_capacity(1 + own_mesh.len());
    req.push(TAG_SNAPSHOT_REQ);
    req.extend_from_slice(own_mesh.as_bytes());
    if send.write_all(&req).await.is_err() || send.finish().is_err() {
        tracing::trace!(donor = %donor_id, "cold-pull request write failed");
        return;
    }
    // Bound the read so a silent donor can't wedge the dial task.
    let bytes = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        recv.read_to_end(4 * 1024 * 1024),
    )
    .await
    {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            tracing::trace!(donor = %donor_id, error = %e, "cold-pull response read failed");
            return;
        }
        Err(_) => {
            tracing::trace!(donor = %donor_id, "cold-pull response timed out");
            return;
        }
    };
    let digests: Vec<GossipDigest> = match postcard::from_bytes(&bytes) {
        Ok(d) => d,
        Err(e) => {
            tracing::trace!(donor = %donor_id, error = %e, "cold-pull decode failed");
            return;
        }
    };
    let count = hydrate_from_snapshot(digests);
    if count > 0 {
        tracing::info_span!(
            "rafka.mesh.coldpull.hydrated",
            node_id = %own_node_id,
            peer_id = %donor_id,
            count = count as i64,
            "otel.kind" = "consumer",
        )
        .in_scope(|| info!(donor = %donor_id, count, "cold-pull: hydrated live_digests from node-admin snapshot"));
    }
}

/// Apply a cold-pull snapshot into the process-global maps, mirroring the gossip
/// receive path's discipline: skip terminal states, honour the resurrection guard
/// (don't re-add a just-evicted node), stamp local `last_seen` so the staleness
/// pruner treats each entry as freshly-seen, and teach iroh each peer's address so
/// the node can dial/join them. Returns the number of entries applied.
fn hydrate_from_snapshot(digests: Vec<GossipDigest>) -> usize {
    let now = now_unix_ms();
    // Same trust boundary as the gossip receive path: if a CA is configured, every
    // snapshot digest must carry a cert signed by it binding the sender's node_id —
    // otherwise the cold-pull would be a hole AROUND cert enforcement. (Each node's
    // cert binds its CURRENT node_id, so a rotated identity carries a fresh matching
    // cert and verifies cleanly; a stale/forged one is dropped.)
    let ca_pubkey: Option<iroh::PublicKey> = std::env::var("RAFKA_CA_PUBKEY")
        .ok()
        .and_then(|s| iroh::PublicKey::from_str(s.trim()).ok());
    let mut applied = 0usize;
    for d in digests {
        if matches!(d.state, NodeState::Leaving | NodeState::Dead) {
            continue;
        }
        if let Some(ca_pub) = &ca_pubkey {
            let ok = crate::cert::decode_cert(&d.cert)
                .map(|signed| crate::cert::verify_cert(&signed, ca_pub, &d.node_name, now))
                .unwrap_or(Err(crate::cert::CertError::BadSignature));
            if ok.is_err() {
                continue; // reject an uncertified/invalid digest pulled in a snapshot
            }
        }
        {
            let guard = recently_evicted().lock().unwrap();
            if let Some(&ts) = guard.get(&d.node_id) {
                if now.saturating_sub(ts) < EVICTION_GUARD_MS {
                    continue;
                }
            }
        }
        register_peer_location(&d.node_id, &d.location);
        last_seen_ms().lock().unwrap().insert(d.node_id.clone(), now);
        live_digests().insert(d.node_id.clone(), d);
        applied += 1;
    }
    applied
}

#[instrument(skip_all)]
async fn dial_seeds(
    endpoint: iroh::Endpoint,
    seeds: Vec<SeedNode>,
    own_node_id: String,
    own_mesh_id: &'static str,
    own_node_type: &'static str,
    own_node_name: &'static str,
    registry: PeerRegistry,
    mesh_id_registry: MeshIdRegistry,
) {
    const BASE_DELAY_MS: u64 = 1_000;
    // Capped LOW (was 30_000): when re-acquiring a seed after a drop, the seed is
    // usually back up and failures are transient iroh stale-path timeouts — keep
    // retrying briskly rather than backing off into a 30s sleep.
    const MAX_DELAY_MS: u64 = 5_000;
    // Bound every connect attempt (proven pattern from the iroh-poc reference,
    // which scales to 100 nodes). After a hard kill the peer's stale path state
    // makes iroh's connect() hang ~30s before it errors — even though the peer is
    // reachable (the connection actually establishes on the peer's accept side).
    // Bounding at 10s turns each wasted attempt from 30s → 10s so the retry loop
    // cycles ~3× faster and re-acquisition completes in tens of seconds, not minutes.
    const CONNECT_TIMEOUT_MS: u64 = 10_000;

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
            // Persistent seed maintenance (node-drop fix). A seed connection is the
            // ONLY discovery path for a node spawned with mdns off (the soak default).
            // The original dial was ONE-SHOT: it broke out of the loop after the first
            // successful connect AND gave up permanently after MAX_ATTEMPTS failures.
            // So when chaos suspended a node or a link blipped and the seed connection
            // dropped, NOTHING re-dialed it — the node's registry stayed empty,
            // run_gossip's join_peers had no peer to feed, peer_count stuck at 0, and
            // peers evicted it after staleness → the mesh eroded to the never-displaced
            // seed core (~3). This OUTER loop owns the connection lifecycle and NEVER
            // gives up: (re)connect → run the frame reader to completion (returns when
            // the connection drops) → re-dial. Re-establishing the seed connection
            // repopulates the registry, which feeds the gossip swarm again (join_peers)
            // → digests re-flood → the node rejoins instead of being evicted.
            loop {
                // ── connect, retrying forever with capped exponential backoff ──
                let mut attempt = 0u32;
                let conn = loop {
                    let dialed = tokio::time::timeout(
                        std::time::Duration::from_millis(CONNECT_TIMEOUT_MS),
                        endpoint.connect(endpoint_addr.clone(), ALPN),
                    )
                    .await;
                    // Outer Err = our 10s bound elapsed (iroh stuck on a stale path);
                    // inner Err = iroh returned a connect error. Both → retry.
                    let err: String = match dialed {
                        Ok(Ok(conn)) => break conn,
                        Ok(Err(e)) => e.to_string(),
                        Err(_) => "connect bound (10s) elapsed".to_string(),
                    };
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
                        info!(peer_id = %peer_id_str, attempt, delay_ms, error = %err, "seed dial failed, retrying");
                    });
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                };

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

                // Adopt the newest connection for the data plane WITHOUT force-closing
                // the prior one (a CONNECTION_CLOSE here would tear down an iroh-gossip
                // backbone neighbor riding a duplicate connection). The stale connection
                // idle-times-out on its own (max_idle_timeout).
                registry.insert(peer_id_str.clone(), conn.clone());

                send_hello(&conn, &own_node_id, own_mesh_id, own_node_type, &peer_id_str).await;

                let conn_bi = conn.clone();
                let own_bi = own_node_id.clone();
                let peer_bi = peer_id_str.clone();
                tokio::spawn(run_bi_reader(conn_bi, own_bi, own_node_name, peer_bi));

                // Cold-pull on VIEW-COLLAPSE, from ANY same-mesh node. Every node keeps
                // the full topology, so whichever seed we just (re)connected to is a
                // valid donor — we don't depend on the node-admin being reachable. Gate
                // on "we currently know no peers" (live_digests holds only our own
                // self-injection, or nothing) so a healthy mesh doesn't re-pull on
                // routine reconnects; this fires at boot (cold) and after a collapse.
                // The donor mesh-scopes the snapshot to OUR mesh, and self-pull is
                // impossible (we never seed to ourselves). Rides this fresh connection,
                // so it costs no extra dial.
                let view_collapsed = live_digests().iter().all(|e| e.key() == &own_node_id);
                if view_collapsed && peer_id_str != own_node_id {
                    pull_topology_snapshot(&conn, &own_node_id, own_mesh_id, &peer_id_str).await;
                }

                // Run the frame reader INLINE (await) — unlike the original detached
                // spawn — so this task learns WHEN the connection drops. run_frame_reader
                // returns on disconnect, having already removed the peer from the
                // registry + emitted peer.disconnected. Same lifecycle discipline as
                // watch_mdns. On return we loop and re-dial the seed.
                {
                    let own = own_node_id.clone();
                    let reg = Arc::clone(&registry);
                    let mesh_reg = Arc::clone(&mesh_id_registry);
                    run_frame_reader(own, own_mesh_id, peer_id_str.clone(), conn, reg, mesh_reg).await;
                }

                // Connection dropped — run_frame_reader has ALREADY removed this peer
                // from the registry + emitted peer.disconnected on the disconnect path.
                // Do NOT remove it again here: in the mutually-seeded case (admin-ui ↔
                // admin-ui) a fresh INBOUND connection from the same peer may have been
                // re-inserted under the same key by the accept loop, and a second remove
                // would drop that live entry. Just re-dial to re-acquire the seed.
                info!(peer_id = %peer_id_str, "seed connection dropped — re-dialing to re-acquire mesh view");
                tokio::time::sleep(tokio::time::Duration::from_millis(BASE_DELAY_MS)).await;
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
    own_node_name: &'static str,
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

                    // Adopt newest without force-closing the prior (see the dial-path
                    // note above): a CONNECTION_CLOSE would drop an iroh-gossip neighbor
                    // riding a duplicate connection. Stale conn idle-times-out.
                    reg.insert(peer_id_str.clone(), conn.clone());

                    send_hello(&conn, &own, own_mesh_id, own_node_type, &peer_id_str).await;

                    let conn_bi = conn.clone();
                    let own_bi = own.clone();
                    let peer_bi = peer_id_str.clone();
                    tokio::spawn(run_bi_reader(conn_bi, own_bi, own_node_name, peer_bi));

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
    own_node_name: &'static str,
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

                            // Adopt newest without force-closing the prior (see the
                            // dial-path note): the accept side is where a mutually-seeded
                            // peer's inbound would otherwise supersede+close our live
                            // outbound and drop the backbone gossip neighbor. Stale conn
                            // idle-times-out.
                            reg.insert(peer_id.clone(), conn.clone());

                            send_hello(&conn, &own_id, own_mesh_id, own_node_type, &peer_id).await;

                            // Per-connection bi-stream reader. Demuxes by tag:
                            // 0x11 = echo (data-plane sanity); 0x10 Write = produce
                            // handler that continues the W3C trace + ACKs (B5).
                            let conn_bi = conn.clone();
                            let own_bi = own_id.clone();
                            let peer_bi = peer_id.clone();
                            tokio::spawn(run_bi_reader(conn_bi, own_bi, own_node_name, peer_bi));

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
                    Ok((_, InternalMeshFrame::Write { from, to, seq })) => (
                        "write",
                        format!("[{peer_prefix}] write-sim {from}→{to} #{seq}"),
                    ),
                    Ok((_, InternalMeshFrame::Ack { from, seq })) => (
                        "ack",
                        format!("[{peer_prefix}] ack {from} #{seq}"),
                    ),
                    Ok((_, InternalMeshFrame::Shutdown { reason })) => (
                        "control",
                        format!("[{peer_prefix}] shutdown reason={reason}"),
                    ),
                    Ok((_, InternalMeshFrame::SetState { state })) => (
                        "control",
                        format!("[{peer_prefix}] set-state {state}"),
                    ),
                    Err(e) => ("decode_failed", format!("[{peer_prefix}] <decode_failed: {e}>")),
                };
                push_message(&peer_id_str, kind_tag, frame_size, summary);

                match InternalMeshFrame::decode_with_context(&bytes) {
                    Ok((parent_ctx, InternalMeshFrame::Hello { mesh_id: peer_mesh_id, node_type: peer_node_type })) => {
                        // Record peer's mesh_id so any code path with the peer_id
                        // can resolve its mesh association.
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
                        // links (e.g. a gateway observing both meshes).
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
                    Ok((_parent_ctx, InternalMeshFrame::Write { from, to, seq })) => {
                        // Sprint-13 B5: produce now travels on BI-streams (handled
                        // by handle_produce_bi). A Write on a UNI stream is legacy /
                        // unexpected — record at trace, don't emit a produce span
                        // (that would be a duplicate handle without an ack path).
                        tracing::trace!(peer_id = %peer_id_str, %from, %to, seq, "unexpected Write on uni stream (produce is bi now)");
                    }
                    Ok((_parent_ctx, InternalMeshFrame::Ack { from, seq })) => {
                        // Acks travel on the produce bi-stream response half, not uni.
                        tracing::trace!(peer_id = %peer_id_str, %from, seq, "unexpected Ack on uni stream");
                    }
                    Ok((_parent_ctx, InternalMeshFrame::Shutdown { reason })) => {
                        // Control-plane kill. Normally arrives on a bi-stream
                        // (run_bi_reader); handle here too so the op works regardless
                        // of which stream the caller used.
                        handle_shutdown_op(&own_node_id, &peer_id_str, &reason).await;
                    }
                    Ok((_parent_ctx, InternalMeshFrame::SetState { state })) => {
                        // Sprint-21 lifecycle op — also handled on uni for parity
                        // with the bi-stream path (send_set_state uses bi).
                        handle_set_state_op(&own_node_id, &peer_id_str, &state);
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

    // 2026-06-01 fix: a pre-minted key passed via RAFKA_NODE_SECRET_KEY is the SOURCE
    // OF TRUTH and wins over any file. admin-ui spawns children with the EXACT key it
    // used to derive the node_name + seed entry, so the booted identity can never
    // diverge from the recorded one. This eliminates the concurrent-spawn file
    // round-trip race where a child loaded a DIFFERENT identity than admin-ui recorded
    // -> duplicate node_id -> seed dial TLS "invalid peer certificate: UnknownIssuer"
    // -> ghost. Both this early load AND run_node's later load read the same env key,
    // so the dual-load stays consistent. DEV-SPAWN ONLY: the key rides the child env
    // block; NOT a production identity-provisioning pattern.
    if let Ok(hex_key) = std::env::var("RAFKA_NODE_SECRET_KEY") {
        let hex_key = hex_key.trim();
        if !hex_key.is_empty() {
            let bytes = hex::decode(hex_key)
                .map_err(|e| anyhow::anyhow!("RAFKA_NODE_SECRET_KEY not valid hex: {e}"))?;
            let key_bytes: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("RAFKA_NODE_SECRET_KEY must be 32 bytes"))?;
            let secret_key = SecretKey::from_bytes(&key_bytes);
            // Persist for restart stability if nothing is on disk yet (don't clobber).
            if !identity_path.exists() {
                let identity = NodeIdentity { secret_key_hex: hex::encode(secret_key.to_bytes()) };
                let _ = tokio::fs::write(&identity_path, serde_json::to_string_pretty(&identity)?).await;
            }
            info!(path = ?identity_path, node_id = %secret_key.public(), source = "env",
                  "loaded identity from RAFKA_NODE_SECRET_KEY (race-free spawn identity)");
            return Ok(secret_key);
        }
    }

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
/// The cross-mesh REPEATER: a dumb, trust-agnostic relay between two (or more) mesh
/// gossip swarms. It subscribes to each mesh's topic and re-broadcasts every ORIGIN
/// digest it hears onto the OTHER meshes' topics — VERBATIM. It reads only `mesh_id`
/// (to route + prevent loops); it NEVER inspects or validates the cert. Trust lives
/// entirely at the RECEIVER: a node on the far mesh runs its normal run_gossip cert
/// check (shared-root CA) on the relayed digest and admits or rejects it. So a
/// compromised repeater cannot inject foreign-cert nodes — it has no trust to grant.
///
/// Loop prevention: a digest is re-broadcast only if its `mesh_id` equals the mesh of
/// the topic it arrived on (it ORIGINATED there). A relayed mesh1 digest arriving on
/// mesh2's topic carries mesh_id=mesh1 != mesh2, so it is never relayed onward — the
/// chain terminates in exactly one hop (and the repeater's own echo is dropped the
/// same way).
///
/// Discovery is mDNS (localhost): like the admins, the repeater subscribes empty and
/// join_peers from what mDNS surfaces. Cross-HOST would need explicit backbone seeds.
/// Deliberately NO RAFKA_CA_PUBKEY here — the repeater is trust-agnostic by design.
pub async fn run_repeater(meshes: Vec<String>, bind_addr: SocketAddrV4) -> Result<()> {
    use futures_lite::StreamExt;
    use iroh_gossip::api::{Event, GossipSender};
    if meshes.len() < 2 {
        anyhow::bail!("repeater needs >=2 meshes to bridge, got {meshes:?}");
    }
    let secret_key = SecretKey::generate();
    let repeater_id = secret_key.public().to_string();
    info!(repeater_id = %repeater_id, ?meshes, "repeater starting (trust-agnostic cross-mesh relay)");

    let mut transport = create_endpoint(secret_key, bind_addr, true).await?;
    let endpoint = transport.endpoint.clone();
    let peer_registry: PeerRegistry = Arc::new(DashMap::new());
    let mesh_id_registry: MeshIdRegistry = Arc::new(DashMap::new());
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());

    // Reuse node machinery: inbound conns -> gossip; mDNS dials -> peer_registry.
    let _accept = start_accept_loop(
        &transport, repeater_id.clone(), "repeater", "repeater", "repeater",
        Arc::clone(&peer_registry), Arc::clone(&mesh_id_registry), gossip.clone(),
    ).await;
    let mdns_rx = std::mem::replace(
        &mut transport.mdns_discovered,
        tokio::sync::mpsc::channel(1).1,
    );
    tokio::spawn(watch_mdns(
        mdns_rx, endpoint.clone(), repeater_id.clone(),
        "repeater", "repeater", "repeater",
        Arc::clone(&peer_registry), Arc::clone(&mesh_id_registry),
    ));

    // Subscribe every bridged mesh; collect all senders so each relay task can
    // broadcast onto the OTHER meshes' topics.
    let mut senders: std::collections::HashMap<String, Arc<GossipSender>> =
        std::collections::HashMap::new();
    let mut receivers = Vec::new();
    for mesh in &meshes {
        let topic_bytes: [u8; 32] = *blake3::hash(mesh.as_bytes()).as_bytes();
        let topic_id = iroh_gossip::proto::TopicId::from_bytes(topic_bytes);
        let topic = gossip.subscribe(topic_id, Vec::new()).await?;
        let (sender, receiver) = topic.split();
        info!(mesh = %mesh, topic_id = %hex::encode(topic_bytes), "repeater subscribed to mesh topic");
        senders.insert(mesh.clone(), Arc::new(sender));
        receivers.push((mesh.clone(), receiver));
    }

    let mut handles = Vec::new();
    for (mesh, mut receiver) in receivers {
        let own_sender = Arc::clone(senders.get(&mesh).expect("own sender"));
        let others: Vec<(String, Arc<GossipSender>)> = senders
            .iter()
            .filter(|(m, _)| *m != &mesh)
            .map(|(m, s)| (m.clone(), Arc::clone(s)))
            .collect();
        let registry = Arc::clone(&peer_registry);
        let rid = repeater_id.clone();
        handles.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            let mut joined: std::collections::HashSet<String> = std::collections::HashSet::new();
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        // Join whatever mDNS surfaced onto THIS topic so the swarm forms.
                        let mut new_peers = Vec::new();
                        for peer in registry.iter() {
                            if joined.insert(peer.key().clone()) {
                                if let Ok(id) = iroh::EndpointId::from_str(peer.key()) {
                                    new_peers.push(id);
                                }
                            }
                        }
                        joined.retain(|p| registry.contains_key(p));
                        if !new_peers.is_empty() {
                            let _ = own_sender.join_peers(new_peers).await;
                        }
                    }
                    ev = receiver.next() => {
                        let Some(ev) = ev else { break };
                        let Ok(Event::Received(msg)) = ev else { continue };
                        // Read mesh_id ONLY — never the cert. Routing, not trust.
                        let Ok(digest) = postcard::from_bytes::<GossipDigest>(&msg.content) else { continue };
                        // Loop guard: relay only digests that ORIGINATED on this mesh.
                        if digest.mesh_id != mesh { continue; }
                        for (other_mesh, sender) in &others {
                            tracing::info_span!(
                                "rafka.repeater.relay",
                                repeater_id = %rid,
                                from_mesh = %mesh,
                                to_mesh = %other_mesh,
                                relayed_node = %digest.node_name,
                                "otel.kind" = "internal",
                            ).in_scope(|| info!(from = %mesh, to = %other_mesh, node = %digest.node_name, "relayed digest cross-mesh"));
                            let _ = sender.broadcast(msg.content.clone()).await;
                        }
                    }
                }
            }
        }));
    }
    for h in handles { let _ = h.await; }
    Ok(())
}

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
    registry: PeerRegistry,
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
                    run_bi_reader(conn, own, "test.node.t1", peer).await;
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
                            run_bi_reader(conn, own, "test.node.t1", peer).await;
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

// ===========================================================================
// Control-plane shutdown (self-aware-fleet kill). A node is killed by SENDING it
// a Shutdown control op over the mesh — NOT by owning + TerminateProcess-ing its
// OS handle. Any mesh participant (an admin-ui console, even one in another mesh)
// dials the target by node_id and sends InternalMeshFrame::Shutdown; the target
// self-tombstones on its own mesh gossip and runs the standard graceful shutdown.
// The caller (admin-ui) separately broadcasts the BACKBONE tombstone for the
// cross-mesh eviction. No OS-process ownership anywhere.
// ===========================================================================

/// This process's iroh endpoint, published at boot (run_node) so the same-process
/// admin-ui HTTP layer can dial arbitrary nodes to send control ops.
static MESH_ENDPOINT: std::sync::OnceLock<iroh::Endpoint> = std::sync::OnceLock::new();

/// Operator-requested lifecycle override (sprint-21). Set by a SetState control op
/// (Updating/Draining to mark the node; Alive to resume → cleared). The periodic
/// digest-builder reads this: a Some(Updating|Draining) overrides the auto health
/// state so the operator's intent propagates + renders mesh-wide. Alive/None means
/// "no override — use the auto Joining/Degraded/Alive logic".
static REQUESTED_STATE: std::sync::OnceLock<std::sync::Mutex<Option<NodeState>>> =
    std::sync::OnceLock::new();

fn requested_state() -> &'static std::sync::Mutex<Option<NodeState>> {
    REQUESTED_STATE.get_or_init(|| std::sync::Mutex::new(None))
}

/// iroh's BUILT-IN manual address book (`MemoryLookup`), registered on the
/// endpoint at boot. We feed it every peer's `location` learned via gossip so
/// `join_peers`/`connect` (which take only an `EndpointId`) can resolve the
/// address WITHOUT a discovery lookup. mDNS stays off; this is the
/// topology-independent fix for the ~40s "Address Lookup failed" join latency.
/// NOT a custom AddressLookup — iroh ships this provider; we only populate it.
static MESH_ADDR_BOOK: std::sync::OnceLock<iroh::address_lookup::memory::MemoryLookup> =
    std::sync::OnceLock::new();

/// Register a peer's gossiped `location` into the iroh address book so the
/// endpoint can dial it by `node_id` alone. No-op on empty/self/unparseable.
fn register_peer_location(node_id_hex: &str, location: &str) {
    if location.is_empty() {
        return;
    }
    let Some(book) = MESH_ADDR_BOOK.get() else { return };
    let Ok(pk) = node_id_hex.parse::<PublicKey>() else { return };
    let Ok(sock) = location.parse::<std::net::SocketAddr>() else { return };
    book.add_endpoint_info(EndpointAddr::new(pk).with_ip_addr(sock));
}

/// Fired when a Shutdown control op is received. `wait_for_signal` selects on it,
/// so the kill reuses the EXISTING graceful-shutdown path (node.stopping + task
/// aborts + telemetry flush) — no hard process::exit, no duplicate teardown.
static SHUTDOWN_NOTIFY: std::sync::OnceLock<std::sync::Arc<tokio::sync::Notify>> =
    std::sync::OnceLock::new();

fn shutdown_notify() -> &'static std::sync::Arc<tokio::sync::Notify> {
    SHUTDOWN_NOTIFY.get_or_init(|| std::sync::Arc::new(tokio::sync::Notify::new()))
}

/// Trigger this node's graceful shutdown (called by the Shutdown control-op
/// handler). `wait_for_signal` then returns "control_op".
pub fn trigger_self_shutdown() {
    shutdown_notify().notify_waiters();
}

/// Dial `target_node_id` at `location` and send a Shutdown control op. The target
/// self-terminates. Works for ANY node the caller can address (same mesh via its
/// gossiped location, other mesh via the backbone directory) — no OS-process
/// ownership. The caller handles the cross-mesh backbone tombstone separately.
pub async fn send_shutdown(target_node_id: &str, location: &str, reason: &str) -> anyhow::Result<()> {
    let endpoint = MESH_ENDPOINT
        .get()
        .ok_or_else(|| anyhow::anyhow!("mesh endpoint not initialized"))?;
    let pk = PublicKey::from_str(target_node_id)?;
    let dest = if location.starts_with("http://") || location.starts_with("https://") {
        EndpointAddr::new(pk).with_relay_url(iroh::RelayUrl::from_str(location)?)
    } else {
        EndpointAddr::new(pk).with_ip_addr(location.parse::<SocketAddr>()?)
    };
    let conn = endpoint.connect(dest, ALPN).await?;
    let (mut send, _recv) = conn.open_bi().await?;
    let span = tracing::info_span!(
        "rafka.mesh.control.shutdown_sent",
        node_id = %endpoint.id(),
        peer_id = %target_node_id,
        op_kind = "control",
        reason = %reason,
        otel.kind = "producer",
    );
    let frame = InternalMeshFrame::Shutdown { reason: reason.to_string() };
    let bytes = span.in_scope(|| frame.encode_with_context(&Span::current().context()));
    send.write_all(&bytes).await?;
    send.finish()?;
    // Hold the connection open until the target reads the frame and acts on it —
    // it closes the connection when it self-terminates. Returning here immediately
    // would drop `conn`, resetting the stream before the target's accept_bi reads it.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), conn.closed()).await;
    span.in_scope(|| info!(target = %target_node_id, "shutdown control op sent"));
    Ok(())
}

/// Handle an inbound Shutdown control op (from run_bi_reader). Announce our own
/// departure on our mesh gossip (so same-mesh peers evict us even though the
/// killer may be in another mesh), let it flush, then trigger graceful shutdown.
async fn handle_shutdown_op(own_node_id: &str, peer_id_str: &str, reason: &str) {
    tracing::info_span!(
        "rafka.mesh.control.shutdown_received",
        node_id = %own_node_id,
        peer_id = %peer_id_str,
        op_kind = "control",
        reason = %reason,
        otel.kind = "consumer",
    )
    .in_scope(|| info!(reason = %reason, "shutdown control op received — self-terminating"));
    // Announce our own departure as a STATE: set state=Leaving and publish it NOW
    // (not a separate tombstone). Observers receive the Leaving digest and evict us;
    // the resurrection guard keeps any in-flight older digest from re-adding us.
    let _ = own_node_id; // identity is implicit — we publish our OWN state
    *requested_state().lock().unwrap() = Some(NodeState::Leaving);
    publish_now().notify_one();
    // Let the Leaving digest reach the wire before we tear the endpoint down.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    trigger_self_shutdown();
}

/// Dial `target_node_id` at `location` and send a SetState lifecycle control op
/// (sprint-21). The target applies `state` as a self-state override its next gossip
/// digest publishes. Mirrors `send_shutdown` but is NON-terminal — the connection
/// is not held open for a self-terminate; we just deliver and return.
pub async fn send_set_state(target_node_id: &str, location: &str, state: &str) -> anyhow::Result<()> {
    let endpoint = MESH_ENDPOINT
        .get()
        .ok_or_else(|| anyhow::anyhow!("mesh endpoint not initialized"))?;
    let pk = PublicKey::from_str(target_node_id)?;
    let dest = if location.starts_with("http://") || location.starts_with("https://") {
        EndpointAddr::new(pk).with_relay_url(iroh::RelayUrl::from_str(location)?)
    } else {
        EndpointAddr::new(pk).with_ip_addr(location.parse::<SocketAddr>()?)
    };
    let conn = endpoint.connect(dest, ALPN).await?;
    let (mut send, _recv) = conn.open_bi().await?;
    let span = tracing::info_span!(
        "rafka.mesh.control.state_change_sent",
        node_id = %endpoint.id(),
        peer_id = %target_node_id,
        op_kind = "control",
        state = %state,
        otel.kind = "producer",
    );
    let frame = InternalMeshFrame::SetState { state: state.to_string() };
    let bytes = span.in_scope(|| frame.encode_with_context(&Span::current().context()));
    send.write_all(&bytes).await?;
    send.finish()?;
    // Brief hold so the target's accept_bi reads the frame before we drop conn.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), conn.closed()).await;
    span.in_scope(|| info!(target = %target_node_id, state, "set-state control op sent"));
    Ok(())
}

/// Handle an inbound SetState control op (from run_bi_reader). Parse the requested
/// state and store it as this node's self-state override; the periodic digest
/// builder picks it up on the next tick and a state CHANGE forces an immediate
/// re-publish, so the new lifecycle state propagates mesh-wide in ≤1 interval.
/// Terminal states (Leaving/Dead) are rejected — Leaving is the kill path, Dead is
/// observer-inferred.
fn handle_set_state_op(own_node_id: &str, peer_id_str: &str, state_str: &str) {
    let parsed = NodeState::from_name(state_str);
    let accepted = matches!(
        parsed,
        Some(NodeState::Updating | NodeState::Draining | NodeState::Alive)
    );
    tracing::info_span!(
        "rafka.mesh.control.state_change_received",
        node_id = %own_node_id,
        peer_id = %peer_id_str,
        op_kind = "control",
        state = %state_str,
        accepted = accepted,
        otel.kind = "consumer",
    )
    .in_scope(|| info!(state = %state_str, accepted, "set-state control op received"));
    if !accepted {
        return;
    }
    // Alive resumes (clears the override → back to auto health); Updating/Draining set it.
    let mut guard = requested_state().lock().unwrap();
    *guard = match parsed {
        Some(NodeState::Alive) => None,
        other => other,
    };
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
        _ = shutdown_notify().notified() => {
            info!("control-op shutdown received");
            "control_op"
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
            location: "127.0.0.1:14820".into(),
            state: NodeState::Alive,
            stateful: false,
            cert: String::new(),
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

    #[test]
    fn backbone_message_summary_postcard_roundtrip() {
        let summary = MeshSummary {
            mesh_id: "mesh2".into(),
            directory: vec![
                MeshDirectoryEntry { node_name: "mesh2.gateway.aaaaaa".into(), node_type: "gateway".into(), node_id: "a".repeat(64), location: "127.0.0.1:15920".into(), state: NodeState::Alive, cpu_used: 0.05, cpu_budget: 5.0, ram_used: 0.1, ram_budget: 2.5 },
                MeshDirectoryEntry { node_name: "mesh2.broker.bbbbbb".into(), node_type: "broker".into(), node_id: "b".repeat(64), location: "127.0.0.1:15921".into(), state: NodeState::Degraded, cpu_used: 0.05, cpu_budget: 5.0, ram_used: 0.1, ram_budget: 2.5 },
            ],
            aggregate: MeshAggregate { node_count: 2, cpu_used: 0.1, cpu_budget: 10.0, ram_used: 0.2, ram_budget: 5.0, frames_per_sec: 3.0 },
            published_by: "c".repeat(64),
            wall_time_ms: 1_780_000_000_000,
            expires_at_ms: 1_780_000_006_000,
        };
        let msg = BackboneMessage::Summary(summary);
        let bytes = postcard::to_allocvec(&msg).expect("encode BackboneMessage");
        let decoded: BackboneMessage = postcard::from_bytes(&bytes)
            .unwrap_or_else(|e| panic!("DECODE FAILED ({} bytes): {e}", bytes.len()));
        let BackboneMessage::Summary(s) = decoded;
        assert_eq!(s.mesh_id, "mesh2");
        assert_eq!(s.directory.len(), 2);
        assert_eq!(s.aggregate.node_count, 2);
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
            location: String::new(),
            state: NodeState::Alive,
            stateful: false,
            cert: String::new(),
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
        let rt = NodeRuntime::new("gateway")
            .with_cpu_budget(1.0)
            .with_ram_budget(0.5)
            .with_role(Role::Gateway);
        assert_eq!(rt.cpu_budget, Some(1.0));
        assert_eq!(rt.ram_budget, Some(0.5));
        assert!(matches!(rt.role, Role::Gateway));
    }
}
