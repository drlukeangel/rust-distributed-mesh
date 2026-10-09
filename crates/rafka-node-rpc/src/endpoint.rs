//! Binding an Iroh endpoint at an exact, node-admin-assigned address.

use anyhow::Result;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode, SecretKey};
use std::net::SocketAddr;

/// The one QUIC transport configuration every node kind binds with (node-admin, rpc nodes, the
/// probe, role binaries): a connection idles out after `staleness_floor()` (`RDM_STALENESS_MS`,
/// 30 s by default) and is kept alive every `gossip_interval() / 2` (`RDM_GOSSIP_INTERVAL_MS`,
/// 2 s by default, so 1 s). A pooled connection to a dead peer therefore closes within the
/// staleness floor, which the node-admin holds below the investigation's second probe
/// (`check_idle_below_probe2`), so the second probe's dial fails before writing.
pub fn transport_config() -> iroh::endpoint::QuicTransportConfig {
    let idle = rafka_mesh_entity::cadence::staleness_floor();
    let keep_alive = rafka_mesh_entity::cadence::gossip_interval() / 2;
    iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(keep_alive)
        .max_idle_timeout(Some(idle.try_into().expect("the staleness floor fits a QUIC idle timeout")))
        .build()
}

/// The process's one endpoint: one identity, one physical UDP socket, at
/// exactly the transport address node-admin assigned (relay off, no
/// discovery, the shared `transport_config`). Node RPC and gossip
/// share it by ALPN. A request names its target in the fence of its framing;
/// the socket decides nothing and there is never a second one.
pub async fn bind(secret: SecretKey, addr: SocketAddr) -> Result<Endpoint> {
    let builder = Endpoint::builder(presets::Minimal)
        .secret_key(secret)
        .alpns(vec![crate::ALPN.to_vec(), iroh_gossip::ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .portmapper_config(iroh::endpoint::PortmapperConfig::Disabled)
        .transport_config(transport_config())
        .clear_ip_transports()
        .bind_addr(addr)?;
    Ok(builder.bind().await?)
}

/// Bind an endpoint holding exactly one socket, at `addr`. Iroh's builder
/// otherwise also binds the IPv4 and IPv6 wildcards (`0.0.0.0`, `[::]`) on
/// ports nobody assigned, and its port mapper probes the gateway (UPnP,
/// NAT-PMP, PCP) from wildcard sockets and advertises what it maps; both are
/// off, so the node is reachable only at the address node-admin advertises.
pub async fn bind_exact(secret: SecretKey, addr: SocketAddr, alpns: Vec<Vec<u8>>, transport: iroh::endpoint::QuicTransportConfig) -> Result<Endpoint> {
    let builder = Endpoint::builder(presets::Minimal)
        .secret_key(secret)
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .portmapper_config(iroh::endpoint::PortmapperConfig::Disabled)
        .transport_config(transport)
        .clear_ip_transports()
        .bind_addr(addr)?;
    Ok(builder.bind().await?)
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
