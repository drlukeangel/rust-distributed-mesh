//! i143.e2.s3 functional: a process node is deployed through the create
//! `DeploymentPipeline` with every step's receipt and span (`common`).

use crate::common;

#[test]
fn admin_deploys_a_process_node_through_every_pipeline_step() {
    let cap = crate::common::capture("admin_deploys_a_process_node_through_every_pipeline_step");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        match common::deploy_through_every_step("process", &spans).await {
            common::Smoke::Passed => {}
            common::Smoke::Unsupported(reason) => panic!("the process provider runs on every host: {reason}"),
        }
    })
}
