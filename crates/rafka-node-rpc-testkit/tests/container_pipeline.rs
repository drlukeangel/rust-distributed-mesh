//! i143.e2.s4 smoke: the e2.s3 scenario with `MESH_SPAWN_TYPE=container`.
//! Each node runs in its own container and network namespace on the fabric's
//! bridge network, at the address node-admin assigned.
//!
//! A host that cannot run containers skips with the named reason. With
//! `RAFKA_REQUIRE_CONTAINER=1` (CI) that skip is a failure instead.

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_deploys_a_container_node_through_every_pipeline_step() {
    match common::deploy_through_every_step("container").await {
        common::Smoke::Passed => {}
        common::Smoke::Unsupported(reason) if std::env::var("RAFKA_REQUIRE_CONTAINER").as_deref() == Ok("1") => {
            panic!("RAFKA_REQUIRE_CONTAINER=1 but the container provider is unsupported here: {reason}")
        }
        common::Smoke::Unsupported(reason) => eprintln!("SKIP container smoke: the container provider is unsupported on this host: {reason}"),
    }
}
