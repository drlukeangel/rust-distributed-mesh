//! Binding an Iroh endpoint at an exact, node-admin-assigned address.

use anyhow::Result;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode, SecretKey};
use std::net::SocketAddr;

/// Bind an endpoint for `secret` at exactly `addr` (relay off, no discovery):
/// the address is node-admin's assignment, never chosen here.
///
/// A connection to a peer that died closes within the membership silence
/// window (a 1 s keep-alive, a 3 s idle timeout). Gossip shares this endpoint,
/// and the gossip actor waits on a dead peer's full send queue until its
/// connection closes: a 30 s idle timeout stalled every topic of the node.
pub async fn bind(secret: SecretKey, addr: SocketAddr) -> Result<Endpoint> {
    let transport = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(1))
        .max_idle_timeout(Some(std::time::Duration::from_secs(3).try_into()?))
        .build();
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(secret)
        .alpns(vec![crate::ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .transport_config(transport)
        .bind_addr(addr)?
        .bind()
        .await?;
    Ok(ep)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_bound_endpoint_publishes_to_no_address_lookup_service() {
        let ep = bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        assert!(ep.address_lookup().unwrap().is_empty(), "Node RPC endpoints are addressed by node-admin, never by a discovery service");
        ep.close().await;
    }
}
