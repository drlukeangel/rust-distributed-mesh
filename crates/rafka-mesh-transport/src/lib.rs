use anyhow::Result;
use futures_lite::StreamExt as _;
use iroh::{
    Endpoint, RelayMode, SecretKey,
    address_lookup::{DiscoveryEvent, MdnsAddressLookup},
    endpoint::{Connection, ConnectionError, presets},
};
use std::net::SocketAddrV4;
use tokio::sync::mpsc;
use tracing::instrument;

pub const ALPN: &[u8] = b"rafka-mesh-v1";

pub struct IrohMeshTransport {
    pub endpoint: Endpoint,
    /// Receiver for peers passively discovered via mDNS-style address
    /// lookup. Each item is the discovered endpoint's id as a hex string
    /// (i.e. the public-key short form).
    pub mdns_discovered: mpsc::Receiver<String>,
}

impl IrohMeshTransport {
    /// Create an iroh endpoint with local-network mDNS address lookup on
    /// the caller's tokio runtime.
    ///
    /// iroh 0.98 API: the builder takes a `presets::Preset` value;
    /// address lookup services are added AFTER `bind()` rather than
    /// during construction.
    #[instrument(skip(secret_key))]
    pub async fn new(secret_key: SecretKey, bind_addr: SocketAddrV4, mdns_enable: bool) -> Result<Self> {
        let endpoint = Endpoint::builder(presets::N0DisableRelay)
            .secret_key(secret_key)
            .alpns(vec![ALPN.to_vec(), iroh_gossip::ALPN.to_vec()])
            .relay_mode(RelayMode::Disabled)
            .bind_addr(std::net::SocketAddr::V4(bind_addr))?
            .bind()
            .await?;

        let (tx, rx) = mpsc::channel::<String>(64);

        if mdns_enable {
            // Attach mDNS-style local-network address lookup. In 0.98 this is
            // registered against the endpoint AFTER bind, unlike the 0.91 API
            // which baked it into the builder.
            let mdns = MdnsAddressLookup::builder()
                .build(endpoint.id())
                .map_err(|e| anyhow::anyhow!("MdnsAddressLookup build failed: {e}"))?;
            endpoint
                .address_lookup()
                .map_err(|e| anyhow::anyhow!("endpoint.address_lookup() failed: {e}"))?
                .add(mdns.clone());

            // Subscribe to discovery events and forward node_ids over a channel
            // so consumers (rafka-node-base's peer registry) can react without
            // pulling in the iroh subscriber API directly.
            let mut events = mdns.subscribe().await;
            tokio::spawn(async move {
                while let Some(event) = events.next().await {
                    if let DiscoveryEvent::Discovered { endpoint_info, .. } = event {
                        let node_id = endpoint_info.endpoint_id.to_string();
                        if tx.send(node_id).await.is_err() {
                            break;
                        }
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
