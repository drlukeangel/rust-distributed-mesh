//! Deployment: the runtime hand (PRD §8; mesh-control-plane.md §5–§6).
//!
//! Node-admin owns topology, identity, endpoints, readiness and restart vs
//! replacement; a provider only realises or retires one runtime.

pub mod container;
pub mod endpoint;
pub mod pipeline;
pub mod process;
pub mod provider;

use crate::model::ProviderKind;
use std::net::IpAddr;
use std::sync::Arc;

/// The deployment hand of one fabric, as its policy selects it.
pub struct Prepared {
    pub provider: Arc<dyn provider::DeploymentProvider>,
    /// The address node-admin advertises to its runtimes (membership seed):
    /// loopback for processes, the fabric network's gateway for containers.
    pub admin_ip: IpAddr,
    /// Set for a container fabric, so its network can be removed with it.
    pub container: Option<Arc<container::ContainerDeploymentProvider>>,
}

/// Prepare the provider the fabric's `MESH_SPAWN_TYPE` policy names. A host
/// that cannot run it is refused by name (`DeployError::Unsupported`).
/// `fabric` keys the provider's host-wide resources (a container fabric's Docker network and
/// labels): the Fabric's id, never its name, which another Fabric on the host may share.
pub async fn prepare(policy: provider::FabricPolicy, fabric: &str) -> Result<Prepared, provider::DeployError> {
    match policy.provider {
        ProviderKind::Process => Ok(Prepared {
            provider: Arc::new(process::ProcessDeploymentProvider::new()),
            admin_ip: IpAddr::from([127, 0, 0, 1]),
            container: None,
        }),
        ProviderKind::Container => {
            let c = Arc::new(container::ContainerDeploymentProvider::prepare(fabric).await?);
            Ok(Prepared {
                admin_ip: IpAddr::V4(c.network().gateway),
                provider: c.clone(),
                container: Some(c),
            })
        }
    }
}
