//! Binding an Iroh endpoint at an exact, node-admin-assigned address.

use anyhow::Result;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode, SecretKey};
use std::net::SocketAddr;

/// The process's one endpoint: one identity, one physical UDP socket, at
/// exactly the transport address node-admin assigned (relay off, no
/// discovery, Iroh's default transport configuration). Node RPC and gossip
/// share it by ALPN. A Node RPC slot is a fence a request names in its
/// framing; it owns no socket, so the socket decides nothing and there is
/// never a second one.
pub async fn bind(secret: SecretKey, addr: SocketAddr) -> Result<Endpoint> {
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(secret)
        .alpns(vec![crate::ALPN.to_vec(), iroh_gossip::ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr(addr)?
        .bind()
        .await?;
    Ok(ep)
}

/// Bind an endpoint holding exactly one socket, at `addr`. Iroh's builder
/// otherwise also binds the IPv4 and IPv6 wildcards (`0.0.0.0`, `[::]`) on
/// ports nobody assigned; those are cleared, so the node is reachable only
/// at the address node-admin advertises for it.
pub async fn bind_exact(secret: SecretKey, addr: SocketAddr, alpns: Vec<Vec<u8>>, transport: iroh::endpoint::QuicTransportConfig) -> Result<Endpoint> {
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(secret)
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .transport_config(transport)
        .clear_ip_transports()
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

    /// node-admin assigns the address; the endpoint binds that one socket and
    /// nothing else. Iroh's builder otherwise also binds `[::]` on a port
    /// nobody assigned (RED wherever the host has IPv6, as CI does), and peers
    /// can reach the node through it.
    #[tokio::test]
    async fn a_bound_endpoint_holds_exactly_its_assigned_socket() {
        let ep = bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let socks = ep.bound_sockets();
        assert_eq!(socks.len(), 1, "only the assigned socket: {socks:?}");
        assert_eq!(socks[0].ip(), std::net::IpAddr::from([127, 0, 0, 1]), "{socks:?}");
        ep.close().await;
    }
}
