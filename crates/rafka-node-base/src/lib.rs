use anyhow::Result;
use dashmap::DashMap;
use iroh::{endpoint::Connection, NodeAddr, PublicKey, SecretKey};
use rafka_mesh_ops::InternalMeshFrame;
use rafka_mesh_transport::{IrohMeshTransport, ALPN};
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

async fn run_node(node_type: String, role: Role) -> Result<()> {
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
        node_type = node_type_str,
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
    let mdns_rx = std::mem::replace(
        &mut transport.mdns_discovered,
        tokio::sync::mpsc::channel(1).1,
    );

    let accept_handle =
        start_accept_loop(&transport, node_id.clone(), Arc::clone(&peer_registry)).await;

    let dial_handle = if !seed_nodes.is_empty() {
        let node_id_dial = node_id.clone();
        let endpoint = transport.endpoint.clone();
        let registry = Arc::clone(&peer_registry);
        Some(tokio::spawn(dial_seeds(endpoint, seed_nodes, node_id_dial, registry)))
    } else {
        None
    };

    let mdns_handle = {
        let node_id_mdns = node_id.clone();
        let endpoint = transport.endpoint.clone();
        let registry = Arc::clone(&peer_registry);
        tokio::spawn(watch_mdns(mdns_rx, endpoint, node_id_mdns, registry))
    };

    let heartbeat_handle = {
        let registry = Arc::clone(&peer_registry);
        tokio::spawn(run_heartbeat(node_id.clone(), registry))
    };

    let ping_handle = match role {
        Role::Gateway => {
            let registry = Arc::clone(&peer_registry);
            Some(tokio::spawn(run_ping_sender(node_id.clone(), registry)))
        }
        _ => None,
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
    if let Some(h) = ping_handle {
        h.abort();
    }
    if let Some(h) = dial_handle {
        h.abort();
    }

    Ok(())
}

#[instrument(skip_all)]
async fn dial_seeds(
    endpoint: iroh::Endpoint,
    seeds: Vec<SeedNode>,
    own_node_id: String,
    registry: PeerRegistry,
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

                let own = own_node_id.clone();
                let reg = Arc::clone(&registry);
                tokio::spawn(run_frame_reader(own, peer_id_str.clone(), conn, reg));
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
    registry: PeerRegistry,
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

                    run_frame_reader(own, peer_id_str.clone(), conn, reg).await;
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
    registry: PeerRegistry,
) -> tokio::task::JoinHandle<()> {
    let endpoint = transport.endpoint.clone();
    tokio::spawn(async move {
        loop {
            match endpoint.accept().await {
                Some(incoming) => {
                    let own_id = own_node_id.clone();
                    let reg = Arc::clone(&registry);
                    tokio::spawn(async move {
                        if let Ok(conn) = incoming.await {
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

                            run_frame_reader(own_id, peer_id.clone(), conn, reg).await;
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

/// Handles incoming uni streams. Gateway expects Pong; others expect Ping and reply with Pong.
async fn run_frame_reader(
    own_node_id: String,
    peer_id_str: String,
    conn: Connection,
    registry: PeerRegistry,
) {
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

                match InternalMeshFrame::decode_with_context(&bytes) {
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
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;

        let peer_ids: Vec<String> = registry.iter().map(|e| e.key().clone()).collect();

        for peer_id_str in peer_ids {
            let conn = match registry.get(&peer_id_str) {
                Some(c) => c.clone(),
                None => continue,
            };

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
async fn run_heartbeat(node_id: String, registry: PeerRegistry) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        let peer_count = registry.len() as i64;
        tracing::info_span!(
            "rafka.mesh.heartbeat",
            node_id = %node_id,
            peer_count = peer_count,
        )
        .in_scope(|| {
            info!("heartbeat");
        });
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
