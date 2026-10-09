//! i143.e2.s5 functional: the retire pipeline runs its steps in order —
//! MarkDraining, WaitForDrain, PublishLeaving, CloseRpcAdmission,
//! TerminateRuntime, ReleaseStorage (permanent),
//! RemoveTopologyMembership, Complete — each with a receipt and a step span,
//! and a retire then create on the same node succeeds.
//!
//! Its own test binary: it asserts spans (see `process_pipeline_failure.rs`).

use crate::common;

use common::{add_node, admin_side, publish_build, template, Published, Spans};
use rafka_mesh_entity::{FabricId, MemberStatus};
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, DeploymentPipeline, RetireRequest, RetireStep, Timeouts};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeploymentProvider, DeploymentStatus};
use rafka_node_admin_core::model::NodeStatus;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use tracing_subscriber::layer::SubscriberExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retire_runs_every_step_in_order_and_the_ports_it_held_are_released() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));

    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone(), admin.launcher.clone());
    let builds = MemoryBuildStateAdapter::new();
    let sink = Published::default();
    let provider = ProcessDeploymentProvider::new();
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &admin.joins,
        observer: &admin.observer,
        sink: &sink,
        lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };
    let node: rafka_node_admin_core::model::PathName = "mesh1.rpc.1".parse().unwrap();
    let create = |build_id| CreateRequest { build_id, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() };

    let first = pipeline.create(&create(publish_build(&builds, add_node()).await)).await.unwrap_or_else(|e| panic!("create: {e}"));
    let port: SocketAddr = first.node.transport_addr.expect("a created node carries its transport address");

    let retire_build = publish_build(&builds, add_node()).await; // the Build names the path: an rpc node's meta is Ephemeral, so a permanent retirement releases its storage
    pipeline
        .retire(&RetireRequest { build_id: retire_build.clone(), attempt: 1, node: first.node.clone(), handle: first.handle.clone(), kind: rafka_node_admin_core::deployment::pipeline::RetireKind::Removal, observe_departure: false })
        .await
        .unwrap_or_else(|e| panic!("retire: {e}"));

    // Every retire step, in order, Complete once.
    let view = builds.read_build(&retire_build).await.unwrap();
    let got: Vec<(&str, &StepOutcome, &str)> = view.steps.iter().map(|r| (r.step.as_str(), &r.outcome, r.operation.as_str())).collect();
    let want: Vec<(&str, &StepOutcome, &str)> =
        RetireStep::ORDER.iter().map(|s| (s.name(), &StepOutcome::Complete, "retire-node:mesh1.rpc.1")).collect();
    assert_eq!(got, want);
    // MarkDraining is the typed Node RPC drain: its receipt carries what the call established.
    let mark = view.steps.iter().find(|r| r.step == RetireStep::MarkDraining.name()).expect("MarkDraining receipted");
    let drain: rafka_node_admin_core::deployment::pipeline::DrainOutcome =
        serde_json::from_value(mark.output.clone().expect("the drain outcome is the step's output")).expect("a DrainOutcome");
    assert!(matches!(drain, rafka_node_admin_core::deployment::pipeline::DrainOutcome::Established { .. }), "{drain:?}");

    // What each step did, from outside.
    let statuses: Vec<NodeStatus> = sink.nodes.lock().unwrap().iter().map(|n| n.status).collect();
    assert_eq!(statuses, vec![NodeStatus::Pending, NodeStatus::ReadyForTraffic, NodeStatus::Draining, NodeStatus::Leaving]);
    let (digest, _) = admin.observer.membership.book.get(first.node.node_id.as_str()).expect("the node's digest");
    assert_eq!(digest.status, MemberStatus::Leaving, "the node said Leaving on the fabric before it went");
    assert!(matches!(provider.inspect(&first.handle).await, DeploymentStatus::Exited { .. }));
    assert!(UdpSocket::bind(port).is_ok(), "the runtime released its port");
    assert!(!std::path::Path::new(first.node.data_dir.as_deref().unwrap()).exists(), "permanent: the data dir is gone");
    assert_eq!(*sink.removed.lock().unwrap(), vec![node.clone()]);

    // Each retire step ran as a child of the retire pipeline span.
    let all = spans.0.lock().unwrap().clone();
    for step in RetireStep::ORDER {
        let (_, parent, f) = all
            .values()
            .find(|(n, _, f)| {
                n == "rdm.node_admin.deployment.update.via-step"
                    && f.get("step").map(String::as_str) == Some(step.name())
                    && f.get("build_id") == Some(&retire_build.0)
            })
            .unwrap_or_else(|| panic!("no span for retire step {}", step.name()));
        assert_eq!(parent.as_deref(), Some("rdm.node_admin.deployment.update.via-pipeline"));
        assert_eq!(f.get("outcome").map(String::as_str), Some("complete"), "{}", step.name());
    }
    assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline"
        && f.get("pipeline").map(String::as_str) == Some("retire")));

    // Create again on the same node.
    let again = pipeline.create(&create(publish_build(&builds, add_node()).await)).await.unwrap_or_else(|e| panic!("re-create: {e}"));
    assert_ne!(again.node.node_id, first.node.node_id, "a new node, not the retired one");

    // The Build names this path's storage Persistent { on_retire: Preserve }: a permanent
    // retirement terminates the runtime and leaves the data dir, and the receipt says so.
    let mut keep = common::add_node();
    let preserve = rafka_mesh_entity::meta::NodeMeta {
        storage: rafka_mesh_entity::meta::StorageMeta::Persistent { on_retire: rafka_mesh_entity::meta::PersistentRetireDisposition::Preserve },
        placement: Default::default(),
    };
    keep.meshes.get_mut("mesh1").unwrap().set_meta(&again.node.name, preserve).unwrap();
    let again_build = publish_build(&builds, keep).await;
    pipeline
        .retire(&RetireRequest { build_id: again_build.clone(), attempt: 1, node: again.node.clone(), handle: again.handle.clone(), kind: rafka_node_admin_core::deployment::pipeline::RetireKind::Removal, observe_departure: false })
        .await
        .unwrap();
    assert!(std::path::Path::new(again.node.data_dir.as_deref().unwrap()).exists(), "Persistent/Preserve: the data dir stays on a permanent retirement");
    let kept = builds.read_build(&again_build).await.unwrap().steps;
    let disposition: rafka_node_admin_core::deployment::pipeline::StorageDisposition = serde_json::from_value(
        kept.iter().find(|r| r.step == RetireStep::ReleaseStorage.name()).and_then(|r| r.output.clone()).expect("ReleaseStorage receipted"),
    )
    .unwrap();
    assert_eq!(disposition, rafka_node_admin_core::deployment::pipeline::StorageDisposition::Preserved);
    let _ = std::fs::remove_dir_all(&template.data_root);
}
