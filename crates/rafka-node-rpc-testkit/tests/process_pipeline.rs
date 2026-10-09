//! i143.e2.s3 functional: a process node is deployed through the create
//! `DeploymentPipeline` with every step's receipt and span (`common`).

use crate::common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_deploys_a_process_node_through_every_pipeline_step() {
    match common::deploy_through_every_step("process").await {
        common::Smoke::Passed => {}
        common::Smoke::Unsupported(reason) => panic!("the process provider runs on every host: {reason}"),
    }
}
