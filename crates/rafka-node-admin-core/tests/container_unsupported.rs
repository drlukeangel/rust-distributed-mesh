//! i143.e2.s4: a host that cannot run containers is refused by name — the
//! container provider never passes silently.
//!
//! Its own test binary: it points the container runtime client at a socket
//! that does not exist, which is process-wide.

use rafka_node_admin_core::deployment::container::ContainerDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeployError, FabricPolicy};
use rafka_node_admin_core::model::ProviderKind;

#[tokio::test]
async fn an_unreachable_container_runtime_is_refused_by_name() {
    std::env::set_var("DOCKER_HOST", "unix:///nonexistent/i143-e2-s4/docker.sock");
    match ContainerDeploymentProvider::prepare("fabric-unsupported").await {
        Err(DeployError::Unsupported { provider, reason }) => {
            assert_eq!(provider, ProviderKind::Container);
            assert!(reason.contains("no reachable container runtime"), "{reason}");
        }
        Err(e) => panic!("refused with the wrong reason: {e}"),
        Ok(_) => panic!("prepared a container provider against a runtime that does not exist"),
    }
    let policy = FabricPolicy::bootstrap(Some("container")).unwrap();
    let err = rafka_node_admin_core::deployment::prepare(policy, "fabric-unsupported").await.err().expect("refused");
    assert!(err.to_string().contains("Container provider is unsupported on this host"), "{err}");
}
