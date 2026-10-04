//! i143.e2.s5 functional: the retire pipeline runs its steps in order —
//! MarkDraining, WaitForDrain, PublishLeaving, CloseRpcAdmission,
//! TerminateRuntime, ReleaseEndpoints, ReleaseStorage (permanent),
//! RemoveTopologyMembership, Complete — each with a receipt and a step span,
//! and a retire then create on the same node leaks no port.
//!
//! The allocator holds exactly the two ports one rpc node needs, so the
//! second create can only succeed if the retirement released both.
//! Its own test binary: it asserts spans (see `process_pipeline_failure.rs`).

mod common;

use common::{add_node, admin_side, publish_build, template, Published, Spans};
use rafka_mesh_entity::{MemberStatus, NodeId};
use rafka_node_admin_core::build::BuildIntent;
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::{EndpointAllocator, RPC_NODE_SLOTS};
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, DeploymentPipeline, RetireRequest, RetireStep, Timeouts};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeploymentProvider, DeploymentStatus};
use rafka_node_admin_core::model::NodeStatus;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::Mutex;
use tracing_subscriber::layer::SubscriberExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retire_runs_every_step_in_order_and_a_new_create_reuses_the_released_ports() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));

    let fabric = format!("fab-{}", NodeId::mint());
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone());
    let builds = MemoryBuildStateAdapter::new();
    // Exactly one rpc node's worth of ports.
    let allocator = Mutex::new(EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 58600, 58601));
    let sink = Published::default();
    let provider = ProcessDeploymentProvider::new();
    let pipeline = DeploymentPipeline {
        provider: &provider,
        allocator: &allocator,
        observer: &admin.observer,
        sink: &sink,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };
    let node: rafka_node_admin_core::model::PathName = "mesh1.rpc.1".parse().unwrap();
    let create = |build_id| CreateRequest { build_id, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), slots: RPC_NODE_SLOTS, restart_of: None };

    let first = pipeline.create(&create(publish_build(&builds, add_node()).await)).await.unwrap_or_else(|e| panic!("create: {e}"));
    let ports: Vec<SocketAddr> = first.node.endpoints.iter().map(|e| e.addr).collect();

    let retire_build = publish_build(&builds, BuildIntent::RemoveNode { node: node.clone() }).await;
    pipeline
        .retire(&RetireRequest { build_id: retire_build.clone(), attempt: 1, node: first.node.clone(), handle: first.handle.clone(), permanent: true })
        .await
        .unwrap_or_else(|e| panic!("retire: {e}"));

    // Every retire step, in order, Complete once.
    let view = builds.read_build(&retire_build).await.unwrap();
    let got: Vec<(&str, &StepOutcome, &str)> = view.steps.iter().map(|r| (r.step.as_str(), &r.outcome, r.operation.as_str())).collect();
    let want: Vec<(&str, &StepOutcome, &str)> =
        RetireStep::ORDER.iter().map(|s| (s.name(), &StepOutcome::Complete, "retire-node:mesh1.rpc.1")).collect();
    assert_eq!(got, want);

    // What each step did, from outside.
    let statuses: Vec<NodeStatus> = sink.nodes.lock().unwrap().iter().map(|n| n.status).collect();
    assert_eq!(statuses, vec![NodeStatus::Pending, NodeStatus::ReadyForTraffic, NodeStatus::Draining, NodeStatus::Leaving]);
    let (digest, _) = admin.observer.membership.book.get(&first.node.node_id.0).expect("the node's digest");
    assert_eq!(digest.status, MemberStatus::Leaving, "the node said Leaving on the fabric before it went");
    assert!(matches!(provider.inspect(&first.handle).await, DeploymentStatus::Exited { .. }));
    assert!(ports.iter().all(|a| UdpSocket::bind(a).is_ok()), "the runtime released its ports");
    assert_eq!(allocator.lock().unwrap().held(&node), None);
    assert_eq!(allocator.lock().unwrap().in_use_count(), 0, "no port leaked");
    assert!(!std::path::Path::new(first.node.data_dir.as_deref().unwrap()).exists(), "permanent: the data dir is gone");
    assert_eq!(*sink.removed.lock().unwrap(), vec![node.clone()]);

    // Each retire step ran as a child of the retire pipeline span.
    let all = spans.0.lock().unwrap().clone();
    for step in RetireStep::ORDER {
        let (_, parent, f) = all
            .values()
            .find(|(n, _, f)| {
                n == "rafka.node_admin.deployment.update.via-step"
                    && f.get("step").map(String::as_str) == Some(step.name())
                    && f.get("build_id") == Some(&retire_build.0)
            })
            .unwrap_or_else(|| panic!("no span for retire step {}", step.name()));
        assert_eq!(parent.as_deref(), Some("rafka.node_admin.deployment.update.via-pipeline"));
        assert_eq!(f.get("outcome").map(String::as_str), Some("complete"), "{}", step.name());
    }
    assert!(all.values().any(|(n, _, f)| n == "rafka.node_admin.deployment.update.via-pipeline"
        && f.get("pipeline").map(String::as_str) == Some("retire")));

    // Create again on the same node: only possible on the released ports.
    let again = pipeline.create(&create(publish_build(&builds, add_node()).await)).await.unwrap_or_else(|e| panic!("re-create: {e}"));
    let mut reused: Vec<SocketAddr> = again.node.endpoints.iter().map(|e| e.addr).collect();
    let mut before = ports.clone();
    reused.sort();
    before.sort();
    assert_eq!(reused, before, "the new node took the released ports");
    assert_ne!(again.node.node_id, first.node.node_id, "a new node, not the retired one");

    let again_build = publish_build(&builds, BuildIntent::RemoveNode { node: node.clone() }).await;
    pipeline
        .retire(&RetireRequest { build_id: again_build, attempt: 1, node: again.node.clone(), handle: again.handle.clone(), permanent: true })
        .await
        .unwrap();
    assert_eq!(allocator.lock().unwrap().in_use_count(), 0);
    let _ = std::fs::remove_dir_all(&template.data_root);
}
