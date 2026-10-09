//! i143.e2.s4 smoke: the e2.s3 scenario with `MESH_SPAWN_TYPE=container`.
//! Each node runs in its own container and network namespace on the fabric's
//! bridge network, at the address node-admin assigned.
//!
//! Opt-in: it runs only with `RDM_CONTAINER_PROOF=1` (the container-proof step); otherwise it
//! skips by name and nothing container-shaped is started. With `RDM_REQUIRE_CONTAINER=1` a host
//! that cannot run containers fails instead of skipping.

use crate::common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_deploys_a_container_node_through_every_pipeline_step() {
    if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
        eprintln!("SKIP container smoke: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step); the process provider is the proving ground until then");
        return;
    }
    match common::deploy_through_every_step("container").await {
        common::Smoke::Passed => {}
        common::Smoke::Unsupported(reason) if std::env::var("RDM_REQUIRE_CONTAINER").as_deref() == Ok("1") => {
            panic!("RDM_REQUIRE_CONTAINER=1 but the container provider is unsupported here: {reason}")
        }
        common::Smoke::Unsupported(reason) => eprintln!("SKIP container smoke: the container provider is unsupported on this host: {reason}"),
    }
}
