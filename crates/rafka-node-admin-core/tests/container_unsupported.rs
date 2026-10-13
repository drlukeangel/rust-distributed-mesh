//! i143.e2.s4: a host that cannot run containers is refused by name — the
//! container provider never passes silently.
//!
//! Its own process: the container runtime client reads `DOCKER_HOST` from the process
//! environment, so the cell's child is started with it naming a socket that does not exist.

use rafka_node_admin_core::deployment::container::ContainerDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeployError, FabricPolicy};
use rafka_node_admin_core::model::ProviderKind;

#[tokio::test]
async fn an_unreachable_container_runtime_is_refused_by_name() {
    // Its own process, started with the container runtime client pointed at a socket that does not exist.
    if crate::own_process::delegated_with_env(module_path!(), "an_unreachable_container_runtime_is_refused_by_name", &[("DOCKER_HOST", Some(std::ffi::OsStr::new("unix:///nonexistent/i143-e2-s4/docker.sock")))]) {
        return;
    }
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
