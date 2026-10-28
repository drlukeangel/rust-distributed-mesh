//! The functional rig owns the teardown of every node launched through it. A create that fails
//! leaves its runtime running for the Build's re-run (the pipeline reuses a live runtime), and the
//! process provider starts a node detached so it outlives its launcher: a cell that ends without
//! stopping a node it launched (a panic after a create, a failed step) must still leave no
//! `rdm-rpc-node` running once its rig is gone, or the node is orphaned to the init process.

use crate::common;

use common::{add_node, admin_side, publish_build, template, Published};
use rdm_core::FabricId;
use rdm_admin::build_state::MemoryBuildStateAdapter;
use rdm_admin::deployment::endpoint::RPC_NODE;
use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, NoLifecycleEvents, Timeouts};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use std::net::IpAddr;

/// `pid` runs as the process started at `start`: its `/proc` entry carries that start token and one
/// of its tasks has not exited. A zombie has exited (only its parent's wait is outstanding); an
/// absent entry or another start token is another process, or none.
fn runs(pid: u32, start: u64) -> bool {
    if rdm_core::runtime::process_start_token(pid) != Some(start) {
        return false;
    }
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else { return false };
    tasks.flatten().any(|t| std::fs::read_to_string(t.path().join("stat")).is_ok_and(|stat| stat.rsplit(')').next().is_some_and(|rest| !rest.trim_start().starts_with('Z'))))
}

/// CONTRACT: a node the cell launched through the rig's template and never stopped is stopped when
/// the rig drops: once the drop returns, the node's process no longer runs. What must NOT happen: a
/// launched node outliving its rig, so that a cell failing after a launch orphans its node.
#[test]
fn a_rig_dropped_with_its_launched_node_running_leaves_no_node_running() {
    let cap = common::capture("a_rig_dropped_with_its_launched_node_running_leaves_no_node_running");
    cap.run(common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
        let template = template(&admin);
        let builds = MemoryBuildStateAdapter::new();
        let sink = Published::default();
        let provider = ProcessDeploymentProvider::new();
        let pipeline = DeploymentPipeline {
            provider: &provider,
            joins: &admin.joins,
            observer: &admin.observer,
            sink: &sink,
            lifecycle: &NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts::default(),
        };
        let created = pipeline
            .create(&CreateRequest { build_id: publish_build(&builds, add_node()).await, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None })
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        let pid = created.handle.pid.expect("a process handle names its pid");
        let start = created.handle.start.expect("a process handle carries its start token");
        assert!(runs(pid, start), "the launched node runs before its rig drops (pid {pid})");

        // The cell ends here without stopping its node, as a failed cell does.
        drop(pipeline);
        drop(admin);
        assert!(!runs(pid, start), "rdm-rpc-node pid {pid} still runs after its rig dropped: the cell orphans it");
    })
}
