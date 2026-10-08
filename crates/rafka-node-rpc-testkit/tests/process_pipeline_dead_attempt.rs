//! A create re-run reuses the receipts of an earlier attempt only while that attempt's runtime
//! runs. An attempt that died before `DeployRuntime` left nothing held anywhere: its
//! `AllocateEndpoints` receipt was a claim of its executor's allocator, gone with that executor,
//! and by now another allocator may have handed the same port to a live birth. The re-run
//! decides its endpoints afresh and never binds the dead attempt's port.

mod common;

use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::{Assignment, EndpointAllocator, HeldSockets, RPC_NODE};
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, CreateStep, DeploymentPipeline, LaunchTemplate, NodeObserver, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::model::Node;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;

struct Discard;
impl TopologySink for Discard {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &rafka_node_admin_core::model::PathName) {}
}

struct Never;
#[async_trait::async_trait]
impl NodeObserver for Never {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<rafka_node_admin_core::deployment::pipeline::Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Err("never".into())
    }
    async fn drain(&self, _: &Node) -> rafka_node_admin_core::deployment::pipeline::DrainOutcome {
        rafka_node_admin_core::deployment::pipeline::DrainOutcome::NotSent { reason: "this observer has no Node RPC".into() }
    }
    async fn drained(&self, _: &Node) -> bool {
        false
    }
    async fn admission_closed(&self, _: &Node) -> Result<(), String> {
        Err("never".into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_attempt_that_died_before_its_runtime_leaves_no_endpoint_to_reuse() {
    let data_root = std::env::temp_dir().join(format!("i143-dead-attempt-{}", NodeId::mint()));
    let template = LaunchTemplate {
        fabric: String::new(),
        fabric_id: FabricId::mint(),
        executable: env!("CARGO_BIN_EXE_rafka-rpc-node").into(),
        seeds: vec![],
        env: BTreeMap::new(),
        data_root: data_root.clone(),
    };
    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&rafka_node_admin_core::build_state::BuildAccepted {
            build_id: build_id.clone(),
            topology: common::add_node(),
            submitted_change: None,
            submitted_at_ms: 0,
        })
        .await
        .unwrap();
    let node: rafka_node_admin_core::model::PathName = "mesh1.rpc.1".parse().unwrap();
    let operation = format!("create-node:{node}");
    // Attempt 1, by an executor that is gone: it allocated identity and endpoints (the block's first port) and
    // died before DeployRuntime. Its receipts are the only trace of it.
    let (first, last) = rafka_node_admin_core::deployment::endpoint::port_range_from_env();
    // The dead attempt's port: one port from the lane's range, taken as a port is taken everywhere.
    let mut dead_attempt_allocator = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), first, last);
    let dead_port = dead_attempt_allocator.assign(&"mesh1.rpc.9".parse().unwrap(), &RPC_NODE).unwrap().transport;
    let dead_assignment = Assignment { transport: dead_port, listeners: vec![] };
    for (step, output) in [
        (CreateStep::AllocateIdentity.name(), serde_json::json!({"node_id": NodeId::mint(), "incarnation": IncarnationId::mint(), "supersedes": null, "deployment_id": rafka_node_admin_core::model::DeploymentId::mint()})),
        (CreateStep::AllocateEndpoints.name(), serde_json::to_value(&dead_assignment).unwrap()),
    ] {
        builds
            .append_step_receipt(&BuildStepReceipt { build_id: build_id.clone(), attempt: 1, operation: operation.clone(), step: step.into(), outcome: StepOutcome::Complete, output: Some(output), executor: None })
            .await
            .unwrap();
    }
    // Meanwhile another executor's live birth, mesh1.admin.2, holds that port: its record in the
    // topology names it (the record check), and the process is bound on it.
    let squatter = std::net::UdpSocket::bind(dead_port).unwrap();
    dead_attempt_allocator.release(&"mesh1.rpc.9".parse().unwrap());
    let other: rafka_node_admin_core::model::PathName = "mesh1.admin.2".parse().unwrap();
    let allocator = tokio::sync::Mutex::new(
        EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), first, last).with_held_sockets(HeldSockets::new(move || vec![(other.clone(), dead_port)])),
    );
    let provider = ProcessDeploymentProvider::new();
    let pipeline = DeploymentPipeline {
        provider: &provider,
        allocator: &allocator,
        observer: &Never,
        sink: &Discard,
        lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { bind: Duration::from_secs(10), ..Timeouts::default() },
    };
    // Attempt 2 by this executor: the runtime refuses to start (no fabric), which is fine — the
    // decision under test is made before it. Nothing of attempt 1 is reused.
    let _ = pipeline.create(&CreateRequest { build_id: build_id.clone(), attempt: 2, node: node.clone(), spec: &RPC_NODE, restart_of: None }).await;
    let view = builds.read_build(&build_id).await.unwrap();
    let second: Vec<&BuildStepReceipt> = view.steps.iter().filter(|r| r.attempt == 2).collect();
    let endpoints = second.iter().find(|r| r.step == CreateStep::AllocateEndpoints.name()).expect("attempt 2 allocated its own endpoints");
    let assigned: Assignment = serde_json::from_value(endpoints.output.clone().unwrap()).unwrap();
    assert_ne!(assigned.transport, dead_port, "the dead attempt's port is never reused: another birth holds it");
    assert!((first..=last).contains(&assigned.transport.port()), "a port of the lane's range");
    assert!(second.iter().all(|r| r.step != CreateStep::AllocateIdentity.name()), "the identity receipt is reused (no new receipt): only the endpoints another node holds are decided afresh");
    drop(squatter);
    let _ = std::fs::remove_dir_all(&data_root);
}
