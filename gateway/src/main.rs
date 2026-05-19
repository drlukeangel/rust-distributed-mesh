use anyhow::Result;
use iroh::SecretKey;
use rafka_mesh_transport::{IrohMeshTransport, ALPN};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
use tokio::signal;
use tracing::{info, instrument, Instrument};

#[derive(Serialize, Deserialize)]
struct NodeIdentity {
    secret_key_hex: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = rafka_telemetry::init_telemetry("rafka-gateway");

    let data_dir = std::env::var("RAFKA_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data/node"));

    // Root span — all boot child spans nest under this.
    let root_span = tracing::info_span!("rafka.mesh.node.ready");

    async {
        // Child 1: identity — span name reflects load vs mint
        let identity_path = data_dir.join("node-identity.json");
        let secret_key = if identity_path.exists() {
            load_or_mint_identity(&data_dir)
                .instrument(tracing::info_span!("rafka.mesh.boot.identity_loaded"))
                .await?
        } else {
            load_or_mint_identity(&data_dir)
                .instrument(tracing::info_span!("rafka.mesh.boot.identity_minted"))
                .await?
        };
        let node_id = secret_key.public().to_string();
        info!(node_id = %node_id, "identity ready");

        // Child 2: endpoint
        let transport = create_endpoint(secret_key)
            .instrument(tracing::info_span!("rafka.mesh.boot.endpoint_created"))
            .await?;

        // Child 3: ALPN (already done inside endpoint builder, emit the span)
        tracing::info_span!("rafka.mesh.boot.alpn_registered").in_scope(|| {
            info!(alpn = ?std::str::from_utf8(ALPN).unwrap_or("<binary>"), "ALPN registered");
        });

        // Child 4: gossip
        tracing::info_span!("rafka.mesh.boot.gossip_started").in_scope(|| {
            info!("gossip discovery started via iroh mdns");
        });

        // Child 5: accept loop
        let accept_handle = start_accept_loop(&transport).await;
        tracing::info_span!("rafka.mesh.boot.accept_loop_started").in_scope(|| {
            info!("accept loop running");
        });

        info!(node_id = %node_id, "boot complete, idling");

        let heartbeat_handle = tokio::spawn(run_heartbeat());

        wait_for_signal().await;

        tracing::info_span!("rafka.mesh.node.stopping").in_scope(|| {
            info!("node stopping");
        });

        accept_handle.abort();
        heartbeat_handle.abort();

        anyhow::Ok(())
    }
    .instrument(root_span)
    .await?;

    Ok(())
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
        // Rename the span retroactively with an event attribute so Jaeger shows "minted"
        info!(path = ?identity_path, node_id = %secret_key.public(), event = "identity_minted", "minted new identity");
        Ok(secret_key)
    }
}

/// Create the iroh Endpoint. Caller instruments with the correct boot span name.
#[instrument(skip_all)]
async fn create_endpoint(secret_key: SecretKey) -> Result<IrohMeshTransport> {
    let transport = IrohMeshTransport::new(secret_key).await?;
    info!(node_id = %transport.endpoint.node_id(), "iroh endpoint bound");
    Ok(transport)
}

/// Start the no-op accept loop in a background task.
#[instrument(skip_all)]
async fn start_accept_loop(transport: &IrohMeshTransport) -> tokio::task::JoinHandle<()> {
    let endpoint = transport.endpoint.clone();
    tokio::spawn(async move {
        loop {
            match endpoint.accept().await {
                Some(incoming) => drop(incoming),
                None => {
                    info!("accept loop: endpoint closed");
                    break;
                }
            }
        }
    })
}

#[instrument]
async fn run_heartbeat() {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        tracing::info_span!("rafka.mesh.heartbeat").in_scope(|| {
            info!("heartbeat");
        });
    }
}

async fn wait_for_signal() {
    let _ = signal::ctrl_c().await;
    info!("signal received, shutting down");
}
