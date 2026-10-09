//! iroh's local transport observations of one remote, sampled once for a span.
//!
//! These fields represent iroh's local transport observations at the time of sampling. They are
//! not proof of node liveness, reachability, or runtime exit. Nothing decides on them: they are
//! recorded on spans beside an existing event and read only by a person or a test.

use iroh::endpoint::TransportAddrUsage;
use iroh::{Endpoint, EndpointId};

/// What `Endpoint::remote_info` said about one remote, rendered for span fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IrohObservation {
    /// Every transport address iroh knows for the remote, comma separated, or `unavailable`.
    pub known_addrs: String,
    /// The addresses iroh currently uses for the remote, comma separated, or `unavailable`.
    pub active_addrs: String,
}

impl IrohObservation {
    /// No sample: the remote's key is unknown here, or iroh holds no record of it. Unavailable
    /// information, never zero connectivity.
    pub fn unavailable() -> Self {
        Self { known_addrs: "unavailable".into(), active_addrs: "unavailable".into() }
    }
}

/// One `remote_info` read of `key` on `endpoint`: no network call, no wait beyond the read.
pub async fn observe_remote(endpoint: &Endpoint, key: Option<EndpointId>) -> IrohObservation {
    let Some(key) = key else { return IrohObservation::unavailable() };
    let Some(info) = endpoint.remote_info(key).await else { return IrohObservation::unavailable() };
    let (mut known, mut active) = (Vec::new(), Vec::new());
    for a in info.addrs() {
        known.push(a.addr().to_string());
        if matches!(a.usage(), TransportAddrUsage::Active) {
            active.push(a.addr().to_string());
        }
    }
    IrohObservation { known_addrs: known.join(","), active_addrs: active.join(",") }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: a key with no record on the endpoint, or no key at all, is unavailable
    /// information, never an empty (zero-connectivity) observation.
    #[tokio::test]
    async fn an_unknown_remote_is_unavailable_never_zero_connectivity() {
        let ep = Endpoint::builder(iroh::endpoint::presets::Minimal).relay_mode(iroh::RelayMode::Disabled).bind().await.unwrap();
        assert_eq!(observe_remote(&ep, None).await, IrohObservation::unavailable());
        let stranger = iroh::SecretKey::generate().public();
        assert_eq!(observe_remote(&ep, Some(stranger)).await, IrohObservation::unavailable());
        ep.close().await;
    }
}
