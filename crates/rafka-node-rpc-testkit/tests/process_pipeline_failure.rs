//! i143.e2.s3 functional: a runtime that exits before binding fails the
//! create pipeline at `WaitForBind`, with the runtime's own reason carried in
//! the error and the failed receipt; no later step runs.
//!
//! Its own test binary: the span-asserting pipeline test installs a scoped
//! subscriber, and tracing's process-wide callsite interest cache must not be
//! shared with a test that runs the same callsites without one.

use crate::common;

use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateStep, DeploymentPipeline, CreateRequest, LaunchTemplate, NodeObserver, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::model::Node;
use std::collections::BTreeMap;
use std::time::Duration;

struct Discard;
impl TopologySink for Discard {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &rafka_node_admin_core::model::PathName) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_runtime_that_dies_before_binding_fails_wait_for_bind_with_its_reason() {
    let data_root = std::env::temp_dir().join(format!("i143-e2s3-{}", NodeId::mint()));
    // No seeds and no fabric: the node refuses to start (exit 2) before binding.
    let template = LaunchTemplate {
        fabric: String::new(),
        fabric_id: FabricId::mint(),
        executable: env!("CARGO_BIN_EXE_rafka-rpc-node").into(),
        seeds: vec![],
        launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: BTreeMap::new(),
        data_root: data_root.clone(),
    };
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
    let joins = rafka_node_admin_core::join::Joins::default();
    let provider = ProcessDeploymentProvider::new();
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &joins,
        observer: &Never,
        sink: &Discard,
        lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { bind: Duration::from_secs(10), ..Timeouts::default() },
    };
    let err = pipeline
        .create(&CreateRequest { build_id: build_id.clone(), attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() })
        .await
        .unwrap_err();
    assert_eq!(err.step, "WaitForBind");
    assert!(err.reason.contains("runtime exited"), "{}", err.reason);
    assert!(err.reason.contains("refusing to start"), "the runtime's own reason is carried: {}", err.reason);
    let view = builds.read_build(&build_id).await.unwrap();
    let last = view.steps.last().unwrap();
    assert_eq!(last.step, "WaitForBind");
    assert!(matches!(&last.outcome, StepOutcome::Failed { reason } if reason.contains("runtime exited")));
    let ran: Vec<&str> = view.steps.iter().map(|r| r.step.as_str()).collect();
    let upto = CreateStep::ORDER.iter().position(|s| *s == CreateStep::WaitForBind).unwrap();
    let want: Vec<&str> = CreateStep::ORDER[..=upto].iter().map(|s| s.name()).collect();
    assert_eq!(ran, want, "nothing after a failed step runs");
    let _ = std::fs::remove_dir_all(&data_root);
}
