//! The entry pull: a launched node's first act on the mesh.
//!
//! A node takes its identity at launch; everything it needs to know about
//! the fabric it takes from the node-admin that launched it, over a direct
//! QUIC connection on [`ENTRY_ALPN`] (never HTTP: an admin's HTTP API is for
//! tests and people). The answer is what that admin holds now: its topology
//! projection (fabric, meshes, nodes) and the membership digests it is
//! projected from. The node records those digests as heard, so its first
//! view is its admin's, and only then marks itself ready. There is no join
//! mode: a member the answer names but the node never hears again is
//! inferred dead by silence, like any other.

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointAddr};
use rafka_mesh_entity::digest::MeshDigest;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// The entry protocol.
pub const ENTRY_ALPN: &[u8] = b"rafka-mesh-entry/1";

/// The largest answer a node reads.
const MAX_ANSWER: usize = 4 * 1024 * 1024;

/// What a launched node asks: its own path name (for evidence only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryRequest {
    pub node: String,
}

/// What node-admin holds now.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EntryAnswer {
    /// The answering admin's path name.
    pub served_by: String,
    /// The admin's topology projection (`fabric`, `meshes`, `nodes`), as the
    /// admin serialises it; this crate does not interpret it.
    pub topology: serde_json::Value,
    /// The membership digests of every member the admin hears.
    pub members: Vec<MeshDigest>,
}

type Answer = dyn Fn(&EntryRequest) -> EntryAnswer + Send + Sync;

/// Serves [`ENTRY_ALPN`]: one bi-stream per pull, the request then the answer.
#[derive(Clone)]
pub struct EntryServer {
    answer: Arc<Answer>,
}

impl std::fmt::Debug for EntryServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EntryServer")
    }
}

impl EntryServer {
    pub fn new(answer: impl Fn(&EntryRequest) -> EntryAnswer + Send + Sync + 'static) -> Self {
        Self { answer: Arc::new(answer) }
    }
}

impl ProtocolHandler for EntryServer {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let Ok(bytes) = recv.read_to_end(64 * 1024).await else { continue };
            let Ok(req) = serde_json::from_slice::<EntryRequest>(&bytes) else { continue };
            let answer = (self.answer)(&req);
            tracing::info_span!(
                "rafka.mesh.entry.serve.via-pull",
                node = %req.node,
                served_by = %answer.served_by,
                members = answer.members.len(),
            )
            .in_scope(|| tracing::info!("entry answered"));
            let _ = send.write_all(&serde_json::to_vec(&answer).unwrap_or_default()).await;
            let _ = send.finish();
            // Let the answer leave before the stream is dropped.
            let _ = send.stopped().await;
        }
        Ok(())
    }
}

/// One pull from `anchor`, within `within`.
pub async fn pull_once(endpoint: &Endpoint, anchor: EndpointAddr, node: &str, within: Duration) -> Result<EntryAnswer, String> {
    let work = async {
        let answered = |a: EntryAnswer| if a.served_by.is_empty() { Err("the admin is not ready to answer yet".to_string()) } else { Ok(a) };
        let conn = endpoint.connect(anchor, ENTRY_ALPN).await.map_err(|e| format!("connecting: {e}"))?;
        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| format!("opening a stream: {e}"))?;
        let req = serde_json::to_vec(&EntryRequest { node: node.to_string() }).map_err(|e| e.to_string())?;
        send.write_all(&req).await.map_err(|e| format!("sending: {e}"))?;
        send.finish().map_err(|e| format!("finishing: {e}"))?;
        let bytes = recv.read_to_end(MAX_ANSWER).await.map_err(|e| format!("reading the answer: {e}"))?;
        conn.close(0u32.into(), b"entry pulled");
        serde_json::from_slice::<EntryAnswer>(&bytes).map_err(|e| format!("undecodable answer: {e}")).and_then(answered)
    };
    tokio::time::timeout(within, work).await.map_err(|_| format!("no answer within {within:?}"))?
}

/// Pull from `anchor`, retrying an unreachable admin up to `attempts` times.
/// The evidence names the outcome either way.
pub async fn pull(endpoint: &Endpoint, anchor: EndpointAddr, node: &str, attempts: u32) -> Result<EntryAnswer, String> {
    let mut last = String::new();
    for attempt in 1..=attempts.max(1) {
        match pull_once(endpoint, anchor.clone(), node, Duration::from_secs(5)).await {
            Ok(a) => {
                tracing::info_span!(
                    "rafka.mesh.entry.update.via-membership-pulled",
                    node,
                    served_by = %a.served_by,
                    members = a.members.len(),
                    attempt,
                )
                .in_scope(|| tracing::info!("entry pulled"));
                return Ok(a);
            }
            Err(e) => {
                last = e;
                tokio::time::sleep(Duration::from_millis(200 * attempt as u64)).await;
            }
        }
    }
    tracing::info_span!("rafka.mesh.entry.reject.via-membership-pull-failed", node, reason = %last, attempts)
        .in_scope(|| tracing::warn!("entry pull failed"));
    Err(last)
}
