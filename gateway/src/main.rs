use anyhow::Result;
use iroh::{NodeAddr, PublicKey, SecretKey};
use rafka_mesh_transport::{IrohMeshTransport, ALPN};
use serde::{Deserialize, Serialize};
use std::{net::{SocketAddr, SocketAddrV4}, path::PathBuf, str::FromStr, time::Duration};
use tokio::signal;
use tracing::{info, instrument, Instrument, Span};

struct SeedNode {
    id: PublicKey,
    addr: SocketAddr,
}

const NODE_TYPE: &str = "data-gateway";

#[derive(Serialize, Deserialize)]
struct NodeIdentity {
    secret_key_hex: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = rafka_telemetry::init_telemetry("data-gateway");

    // All config from env vars — no config files, no magic numbers.
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

    // Format: <node_id_hex>@<host>:<port> per CLAUDE.md Principle #8 env var table.
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
                Err(e) => { eprintln!("RAFKA_SEED_NODES: bad node_id {:?}: {e}", id_str); return None; }
            };
            let addr = match addr_str.parse::<SocketAddr>() {
                Ok(a) => a,
                Err(e) => { eprintln!("RAFKA_SEED_NODES: bad addr {:?}: {e}", addr_str); return None; }
            };
            Some(SeedNode { id, addr })
        })
        .collect();

    // Root span — node_id and bind_addr are late-bound after identity + endpoint creation.
    let root_span = tracing::info_span!(
        "rafka.mesh.node.ready",
        node_id = tracing::field::Empty,
        node_type = NODE_TYPE,
        bind_addr = tracing::field::Empty,
        version = env!("CARGO_PKG_VERSION"),
    );

    async {
        info!(
            gossip_interval_ms,
            bind_addr = %bind_addr,
            data_dir = ?data_dir,
            seed_count = seed_nodes.len(),
            "boot config"
        );

        // Child 1: identity — span name reflects load vs mint
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
        info!(node_id = %node_id, "identity ready");

        // Late-bind node_id onto the root span now that identity is known.
        Span::current().record("node_id", &node_id.as_str());

        // Child 2: endpoint
        let transport = create_endpoint(secret_key, bind_addr)
            .instrument(tracing::info_span!(
                "rafka.mesh.boot.endpoint_created",
                node_id = %node_id,
                bind_addr = tracing::field::Empty,
            ))
            .await?;

        // Resolve actual bound address and late-bind on root span.
        // Join all sockets (IPv4 + IPv6) — on Windows iroh 0.91 often binds IPv6-only,
        // so an is_ipv4() filter would fall back to the literal "0.0.0.0:0" config string.
        let sockets = transport.endpoint.bound_sockets();
        let actual_bind_addr = if sockets.is_empty() {
            bind_addr.to_string()
        } else {
            sockets.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ")
        };
        Span::current().record("bind_addr", &actual_bind_addr.as_str());

        // Child 3: ALPN
        tracing::info_span!(
            "rafka.mesh.boot.alpn_registered",
            node_id = %node_id,
            alpn = "rafka-mesh-v1",
        )
        .in_scope(|| {
            info!(alpn = ?std::str::from_utf8(ALPN).unwrap_or("<binary>"), "ALPN registered");
        });

        // Child 4: gossip
        tracing::info_span!("rafka.mesh.boot.gossip_started", node_id = %node_id).in_scope(|| {
            info!(gossip_interval_ms, "gossip discovery started via iroh mdns");
        });

        // Child 5: accept loop
        let accept_handle = start_accept_loop(&transport).await;
        tracing::info_span!("rafka.mesh.boot.accept_loop_started", node_id = %node_id)
            .in_scope(|| {
                info!("accept loop running");
            });

        info!(node_id = %node_id, "boot complete, idling");

        // Spawn seed-dial task — one attempt per seed, peer.discovered + peer.connected spans.
        let dial_handle = if !seed_nodes.is_empty() {
            let node_id_dial = node_id.clone();
            let endpoint = transport.endpoint.clone();
            Some(tokio::spawn(dial_seeds(endpoint, seed_nodes, node_id_dial)))
        } else {
            None
        };

        let node_id_hb = node_id.clone();
        let heartbeat_handle = tokio::spawn(run_heartbeat(node_id_hb));

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
        if let Some(h) = dial_handle { h.abort(); }

        anyhow::Ok(())
    }
    .instrument(root_span)
    .await?;

    Ok(())
}

/// Dial each seed node by NodeId + direct addr: emit peer.discovered then peer.connected.
async fn dial_seeds(endpoint: iroh::Endpoint, seeds: Vec<SeedNode>, own_node_id: String) {
    for seed in seeds {
        let peer_id_str = seed.id.to_string();

        tracing::info_span!(
            "rafka.mesh.peer.discovered",
            node_id = %own_node_id,
            peer_id = %peer_id_str,
            peer_node_type = "unknown",
        )
        .in_scope(|| {
            info!(peer_id = %peer_id_str, addr = %seed.addr, source = "seed", "peer discovered via seed list");
        });

        let endpoint_addr = NodeAddr::new(seed.id).with_direct_addresses([seed.addr]);
        match endpoint.connect(endpoint_addr, rafka_mesh_transport::ALPN).await {
            Ok(conn) => {
                tracing::info_span!(
                    "rafka.mesh.peer.connected",
                    node_id = %own_node_id,
                    peer_id = %peer_id_str,
                    peer_node_type = "unknown",
                )
                .in_scope(|| {
                    info!(peer_id = %peer_id_str, "peer connected");
                });
                // Sprint 03: drop connection immediately — sprint 04 will hold it.
                drop(conn);
            }
            Err(e) => {
                info!(peer_id = %peer_id_str, error = %e, "seed dial failed");
            }
        }
    }
}

/// Load an existing node identity or mint a new one and persist it.
/// The caller instruments this with the correct boot span name.
#[instrument(skip_all, fields(data_dir = ?data_dir))]
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

/// Create the iroh Endpoint. Caller instruments with the correct boot span name.
#[instrument(skip_all)]
async fn create_endpoint(secret_key: SecretKey, bind_addr: SocketAddrV4) -> Result<IrohMeshTransport> {
    let transport = IrohMeshTransport::new(secret_key, bind_addr).await?;
    info!(node_id = %transport.endpoint.node_id(), "iroh endpoint bound");
    Ok(transport)
}

/// Accept incoming connections and complete the QUIC handshake.
/// Sprint 03: accept + immediately drop — dialer-side peer.connected needs the handshake to complete.
#[instrument(skip_all)]
async fn start_accept_loop(transport: &IrohMeshTransport) -> tokio::task::JoinHandle<()> {
    let endpoint = transport.endpoint.clone();
    tokio::spawn(async move {
        loop {
            match endpoint.accept().await {
                Some(incoming) => {
                    tokio::spawn(async move {
                        if let Ok(conn) = incoming.await {
                            let peer = conn.remote_node_id().map(|id| id.to_string()).unwrap_or_else(|_| "unknown".into());
                            info!(peer_id = %peer, "accepted connection");
                            drop(conn);
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

#[instrument(skip_all)]
async fn run_heartbeat(node_id: String) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        tracing::info_span!(
            "rafka.mesh.heartbeat",
            node_id = %node_id,
            peer_count = 0u32,
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
