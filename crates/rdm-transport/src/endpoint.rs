//! Binding an Iroh endpoint at an exact, node-admin-assigned address.

use anyhow::Result;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode, SecretKey};
use std::net::SocketAddr;
use tracing::instrument::WithSubscriber;

/// The one QUIC transport configuration every node kind binds with (node-admin, rpc nodes, the
/// probe, role binaries): a connection idles out after `staleness_floor()` (`RDM_STALENESS_MS`,
/// 30 s by default) and is kept alive every `gossip_interval() / 2` (`RDM_GOSSIP_INTERVAL_MS`,
/// 2 s by default, so 1 s). A pooled connection to a dead peer therefore closes within the
/// staleness floor, before the peer-mesh investigation's second probe (four thirds of the floor
/// unheard, `rdm_admin::investigate::Rungs::at`), so the second probe's dial fails
/// before writing.
pub fn transport_config() -> iroh::endpoint::QuicTransportConfig {
    let idle = rdm_core::cadence::staleness_floor();
    let keep_alive = rdm_core::cadence::gossip_interval() / 2;
    iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(keep_alive)
        .max_idle_timeout(Some(idle.try_into().expect("the staleness floor fits a QUIC idle timeout")))
        .build()
}

/// The process's one endpoint: one identity, one physical UDP socket, at
/// exactly the transport address node-admin assigned (relay_mode Disabled, no
/// discovery, the shared `transport_config`). Node RPC and gossip
/// share it by ALPN. A request names its target in the first section of its framing;
/// the socket decides nothing and there is never a second one.
pub async fn bind(secret: SecretKey, addr: SocketAddr) -> Result<Endpoint> {
    bind_with_alpns(secret, addr, &[]).await
}

/// [`bind`], also registering each application ALPN in `app_alpns` on the process's one endpoint.
pub async fn bind_with_alpns(secret: SecretKey, addr: SocketAddr, app_alpns: &[Vec<u8>]) -> Result<Endpoint> {
    let mut alpns = vec![crate::ALPN.to_vec(), iroh_gossip::ALPN.to_vec()];
    for a in app_alpns {
        if !alpns.iter().any(|existing| existing == a) {
            alpns.push(a.clone());
        }
    }
    let builder = Endpoint::builder(presets::Minimal)
        .secret_key(secret)
        .alpns(alpns)
        .relay_mode(RelayMode::Disabled)
        .portmapper_config(iroh::endpoint::PortmapperConfig::Disabled)
        // One explicit address, relay_mode Disabled, no portmapper: there is no default route to learn and no
        // interface list to expand, so the bind starts no interface enumeration (R-B2).
        .network_monitor(false)
        .transport_config(transport_config())
        .clear_ip_transports()
        .bind_addr(addr)?;
    bound(addr, bind_under_no_span(builder).await)
}

/// Iroh's bind, run where no span is current. Iroh opens its `endpoint` span at the bind under whatever
/// span is current and hangs the endpoint's tasks (its socket actor and the rest) from it for as long as
/// the endpoint lives, so a span current at the bind would be held open by the transport until the
/// endpoint ends, and on a worker thread that holds another dispatcher never end at all. On a task of its
/// own nothing is current: the endpoint's spans are a root of their own, on the caller's dispatcher.
/// Dropping the returned future cancels the bind, as awaiting it in place would.
async fn bind_under_no_span(builder: iroh::endpoint::Builder) -> Result<Endpoint> {
    struct CancelOnDrop(tokio::task::AbortHandle);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let task = tokio::spawn(builder.bind().with_current_subscriber());
    let _cancel = CancelOnDrop(task.abort_handle());
    match task.await {
        Ok(r) => Ok(r?),
        Err(e) => match e.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(e) => Err(anyhow::anyhow!("the endpoint's bind task ended before it bound: {e}")),
        },
    }
}

/// The bind's outcome in the transport diagnostic file (R-QH1): the endpoint's identity and the
/// UDP sockets it holds, or the full error chain. Observation only; the outcome is returned as is.
fn bound(requested: SocketAddr, r: Result<Endpoint>) -> Result<Endpoint> {
    match &r {
        Ok(ep) => tracing::info!(target: crate::TRANSPORT_DIAG_TARGET, step = "endpoint-bound", endpoint_id = %ep.id(), %requested, bound = ?ep.bound_sockets(), "the endpoint is bound"),
        Err(e) => tracing::info!(target: crate::TRANSPORT_DIAG_TARGET, step = "endpoint-bind-failed", %requested, error = %format_args!("{e:#}"), error_debug = ?e, "the endpoint did not bind"),
    }
    r
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
        // One explicit address, relay_mode Disabled, no portmapper: there is no default route to learn and no
        // interface list to expand, so the bind starts no interface enumeration (R-B2).
        .network_monitor(false)
        .transport_config(transport)
        .clear_ip_transports()
        .bind_addr(addr)?;
    bound(addr, bind_under_no_span(builder).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (R-B2): both binds set the endpoint's network monitor off, so no bind enumerates
    /// the host's interfaces. A disabled monitor has no interface list to expand an unspecified
    /// address with, and iroh refuses that bind by name: the refusal is the proof the option is set.
    #[tokio::test]
    async fn both_binds_run_with_the_network_monitor_off() {
        let unspecified: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let refused = bind(SecretKey::generate(), unspecified).await.unwrap_err();
        assert!(format!("{refused:#}").contains("network monitor is disabled"), "{refused:#}");
        let refused = bind_exact(SecretKey::generate(), unspecified, vec![crate::ALPN.to_vec()], transport_config()).await.unwrap_err();
        assert!(format!("{refused:#}").contains("network monitor is disabled"), "{refused:#}");
    }

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
