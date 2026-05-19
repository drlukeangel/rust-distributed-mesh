use anyhow::Result;
use iroh::{endpoint::Connection, Endpoint, RelayMode, SecretKey};
use std::net::SocketAddrV4;
use tracing::instrument;

pub const ALPN: &[u8] = b"rafka-mesh-v1";

pub struct IrohMeshTransport {
    pub endpoint: Endpoint,
}

impl IrohMeshTransport {
    /// Create an iroh endpoint directly on the caller's tokio runtime.
    ///
    /// iroh 0.91 on Windows no longer deadlocks on the multi-thread tokio runtime —
    /// the COM apartment conflict that required a dedicated thread in iroh 0.35 was
    /// fixed upstream (tested 2026-05-19, bound in <200ms on Windows 11 multi-thread).
    /// Keeping the simple path; dedicated-thread workaround is deleted.
    #[instrument(skip(secret_key))]
    pub async fn new(secret_key: SecretKey, bind_addr: SocketAddrV4) -> Result<Self> {
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(RelayMode::Disabled)
            .bind_addr_v4(bind_addr)
            .bind()
            .await?;

        Ok(Self { endpoint })
    }

    /// Dial a peer by its PublicKey (EndpointId). Returns the live Connection.
    /// Caller is responsible for dropping the connection when done.
    #[instrument(skip(self), fields(peer_id = %peer_id))]
    pub async fn connect_seed(&self, peer_id: iroh::PublicKey) -> Result<Connection> {
        let conn = self.endpoint.connect(peer_id, ALPN).await?;
        Ok(conn)
    }

    /// No-op accept loop. Drives the iroh endpoint's accept future so it
    /// doesn't stall. Real frame dispatch lands in a later sprint.
    #[instrument(skip(self))]
    pub async fn run_accept_loop(&self) {
        loop {
            match self.endpoint.accept().await {
                Some(incoming) => {
                    drop(incoming);
                }
                None => {
                    tracing::info!("accept loop: endpoint closed");
                    break;
                }
            }
        }
    }
}
