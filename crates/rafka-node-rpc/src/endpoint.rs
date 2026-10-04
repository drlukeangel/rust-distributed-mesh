//! Binding an Iroh endpoint at an exact, node-admin-assigned address.

use anyhow::Result;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode, SecretKey};
use std::net::SocketAddr;

/// Bind an endpoint for `secret` at exactly `addr` (relay off, no discovery):
/// the address is node-admin's assignment, never chosen here.
pub async fn bind(secret: SecretKey, addr: SocketAddr) -> Result<Endpoint> {
    let transport = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(15))
        .max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into()?))
        .build();
    let ep = Endpoint::builder(presets::N0DisableRelay)
        .secret_key(secret)
        .alpns(vec![crate::ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .transport_config(transport)
        .bind_addr(addr)?
        .bind()
        .await?;
    Ok(ep)
}
