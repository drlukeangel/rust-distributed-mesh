use anyhow::Result;
use iroh::{
    discovery::mdns::MdnsDiscovery,
    endpoint::{Connection, ConnectionError},
    Endpoint, RelayMode, SecretKey,
};
use std::net::SocketAddrV4;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tracing::instrument;

pub const ALPN: &[u8] = b"rafka-mesh-v1";

pub struct IrohMeshTransport {
    pub endpoint: Endpoint,
    /// Receiver for peers passively discovered via mdns. Each item is a NodeId string.
    pub mdns_discovered: mpsc::Receiver<String>,
}

impl IrohMeshTransport {
    /// Create an iroh endpoint with local mdns discovery on the caller's tokio runtime.
    #[instrument(skip(secret_key))]
    pub async fn new(secret_key: SecretKey, bind_addr: SocketAddrV4) -> Result<Self> {
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(RelayMode::Disabled)
            .bind_addr_v4(bind_addr)
            .discovery(MdnsDiscovery::builder())
            .bind()
            .await?;

        // Subscribe to passively discovered peers from mdns.
        let (tx, rx) = mpsc::channel::<String>(64);
        if let Some(stream) = endpoint.discovery().and_then(|d| d.subscribe()) {
            let mut stream = Box::pin(stream);
            tokio::spawn(async move {
                while let Some(item) = stream.next().await {
                    let node_id: String = item.node_id().to_string();
                    if tx.send(node_id).await.is_err() {
                        break;
                    }
                }
            });
        }

        Ok(Self { endpoint, mdns_discovered: rx })
    }
}

/// Await connection close and return a human-readable reason string.
pub async fn await_disconnect(conn: Connection) -> String {
    match conn.closed().await {
        ConnectionError::ApplicationClosed(close) => {
            if close.error_code == 0u32.into() {
                "graceful_close".into()
            } else {
                format!("app_close:{}", close.error_code)
            }
        }
        ConnectionError::ConnectionClosed(_) => "graceful_close".into(),
        ConnectionError::Reset => "connection_reset".into(),
        ConnectionError::TimedOut => "timed_out".into(),
        ConnectionError::LocallyClosed => "locally_closed".into(),
        _ => "connection_lost".into(),
    }
}
