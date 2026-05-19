use anyhow::Result;
use iroh::{Endpoint, RelayMode, SecretKey};
use std::sync::Arc;
use tokio::sync::{oneshot, Notify};
use tracing::instrument;

pub const ALPN: &[u8] = b"rafka-mesh-v1";

pub struct IrohMeshTransport {
    pub endpoint: Endpoint,
    /// Signals the iroh sub-runtime thread to shut down when this transport drops.
    _shutdown: Arc<Notify>,
}

impl IrohMeshTransport {
    /// Create an iroh endpoint on a dedicated tokio runtime thread.
    ///
    /// On Windows, iroh 0.35's `netmon::Monitor::new()` calls `COMLibrary::new()`
    /// synchronously. On the multi-thread tokio runtime this deadlocks due to
    /// COM apartment conflicts. A dedicated `current_thread` runtime on its own
    /// OS thread avoids this: COM is always initialized from that single thread
    /// with no inter-thread conflict.
    ///
    /// The dedicated thread keeps the `current_thread` runtime spinning via
    /// `block_on(shutdown_signal)` so iroh's internal tasks (magicsock actor,
    /// relay actor) remain alive for the lifetime of `IrohMeshTransport`.
    #[instrument(skip(secret_key))]
    pub async fn new(secret_key: SecretKey) -> Result<Self> {
        let (ep_tx, ep_rx) = oneshot::channel::<anyhow::Result<Endpoint>>();
        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();

        std::thread::Builder::new()
            .name("iroh-runtime".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("iroh sub-runtime");

                rt.block_on(async move {
                    let result = Endpoint::builder()
                        .secret_key(secret_key)
                        .alpns(vec![ALPN.to_vec()])
                        .relay_mode(RelayMode::Disabled)
                        .bind()
                        .await
                        .map_err(Into::into);

                    let _ = ep_tx.send(result);

                    // Keep the runtime alive (and all iroh actors running)
                    // until the transport is dropped.
                    shutdown_clone.notified().await;
                });
            })
            .expect("spawn iroh-runtime thread");

        let endpoint = ep_rx.await??;
        Ok(Self { endpoint, _shutdown: shutdown })
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

impl Drop for IrohMeshTransport {
    fn drop(&mut self) {
        self._shutdown.notify_one();
    }
}
