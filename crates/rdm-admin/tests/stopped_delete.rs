//! A node.delete of a stopped node (lifecycles/node-delete.md step 1, node-stop.md step 6): the stopped
//! birth already ran its whole stop (drain receipt, connection cut, `stopped`, `NodeStopped`), so
//! its delete composes nothing of that stop again. It goes from the `NodeDeleting` pre-notice
//! straight to the provider's exact runtime exit proof, then `NodeDeleted`, and releases no storage
//! (node-delete.md "Storage": "Retain data dir and endpoint key"). The cell drives the retire
//! pipeline over a recording provider and a recording observer: nothing here depends on timing.

use rdm_core::{IncarnationId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::deployment::pipeline::{
    CommandAdmission, CommandContext, Completion, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, RetireRequest, RetireStep, Timeouts, TopologySink,
};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::{DeploymentId, Node, NodeStatus, PathName, ProviderKind};
use rdm_admin::node_commands::NodeCommand;
use std::sync::Mutex;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

/// The provider: terminating the exact runtime succeeds and its inspection then says it exited.
#[derive(Default)]
struct Exits {
    terminated: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for Exits {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "i143-3072-delete".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this cell births nothing".into() })
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        self.terminated.lock().unwrap().push(h.deployment_id.to_string());
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        if self.terminated.lock().unwrap().contains(&h.deployment_id.to_string()) {
            DeploymentStatus::Exited { code: Some(0) }
        } else {
            DeploymentStatus::Running
        }
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// The stopped birth: every command or seat transfer sent to it is recorded. A stopped birth has
/// cut its connections; anything sent to it is a defect of the delete.
#[derive(Default)]
struct Recorded {
    sent: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl NodeObserver for Recorded {
    async fn send_command(&self, node: &Node, cmd: NodeCommand, _: &CommandContext) -> CommandAdmission {
        self.sent.lock().unwrap().push(format!("{cmd:?} to {}", node.name));
        CommandAdmission::Admitted {}
    }
    async fn await_completion(&self, node: &Node, cmd: NodeCommand, _: &CommandContext, _: Duration) -> Completion {
        self.sent.lock().unwrap().push(format!("await {cmd:?} from {}", node.name));
        Completion::Received {}
    }
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

struct Discard;
impl TopologySink for Discard {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, _: &PathName, _: &NodeId, _: &IncarnationId) {}
}

/// CONTRACT (lifecycles/node-delete.md 1-2: "For a crashed process those stop steps are already done; do
/// not fabricate node replies or receipts. Stop and inspect the exact runtime through provider
/// control"; node-stop.md 6: "Delete proceeds to external exact runtime exit proof instead"): the
/// delete of a stopped birth (Leaving, stopped) records `NodeDeleting`, then `TerminateRuntime` (the
/// provider's stop and its `Exited` inspection of the exact runtime), then `NodeDeleted`, and
/// `Complete`. Must NOT happen: a stop sent again to the stopped birth, or any of its receipts
/// (`StopNode`, `AwaitNodeLeft`), or a step that releases storage (`ReleaseStorage`: node-delete.md step 3,
/// "Retain the data dir, node-identity.json and endpoint key").
#[tokio::test]
async fn a_stopped_nodes_delete_goes_straight_to_the_exact_runtimes_exit_proof() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("i143-3072-stopped-delete", exporter.clone());
    let _g = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
    node.node_id = NodeId::mint();
    node.incarnation_id = Some(IncarnationId::mint());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::Leaving;
    node.stopped = true;
    let template = LaunchTemplate { fabric: "fabric1".into(),
        fabric_id: rdm_core::FabricId::mint(),
        executable: "/nonexistent/stopped-delete".into(),
        seeds: vec![],
        launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-3072-delete-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-3072-delete-store-{}", NodeId::mint())),
        issuing: None,
        roots: Default::default(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
        binding: None,
    };
    let (provider, observer, joins) = (Exits::default(), Recorded::default(), rdm_admin::join::Joins::default());
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &joins,
        observer: &observer,
        sink: &Discard,
        lifecycle: &NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { completion: Duration::from_millis(300), stop_grace: Duration::from_millis(300), ..Timeouts::default() },
    };
    let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: None, start: None, container: None, domain: Some("i143-3072-delete".into()) };
    let req = RetireRequest { build_id: build_id.clone(), attempt: 1, node, handle: handle.clone(), unreached: false };
    let out = pipeline.delete_node(&req).await;
    // The trace first: a red below still names the steps the delete ran, with their trace id.
    capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
    let spans = exporter.get_finished_spans().unwrap();
    for s in spans.iter().filter(|s| s.name.starts_with("rdm.node_admin.deployment.update.via-")) {
        let step = s.attributes.iter().find(|kv| kv.key.as_str() == "step").map(|kv| kv.value.to_string()).unwrap_or_default();
        eprintln!("span {} trace_id={} step={step}", s.name, s.span_context.trace_id());
    }

    let view = builds.read_build(&build_id).await.unwrap();
    let order: Vec<&str> = view.steps.iter().map(|s| s.step.as_str()).collect();
    assert_eq!(*observer.sent.lock().unwrap(), Vec::<String>::new(), "nothing is sent to a stopped birth: its stop is done; steps {order:?}");
    assert_eq!(
        order,
        [RetireStep::NodeDeleting, RetireStep::TerminateRuntime, RetireStep::NodeDeleted, RetireStep::RemoveTopologyMembership, RetireStep::Complete].map(|s| s.name()),
        "the pre-notice, the exact runtime's exit proof, the departure, and no storage release: {out:?}"
    );
    assert!(out.is_ok(), "{out:?}");
    assert_eq!(*provider.terminated.lock().unwrap(), vec![handle.deployment_id.to_string()], "the exact runtime is stopped through provider control, once");

    let steps: Vec<String> = spans
        .iter()
        .filter(|s| s.name == "rdm.node_admin.deployment.update.via-step")
        .filter_map(|s| s.attributes.iter().find(|kv| kv.key.as_str() == "step").map(|kv| kv.value.to_string()))
        .collect();
    assert!(!steps.iter().any(|s| s == RetireStep::StopNode.name()), "no stop step span: {steps:?}");
    assert!(steps.iter().any(|s| s == RetireStep::TerminateRuntime.name()), "{steps:?}");
    assert!(!steps.iter().any(|s| s == "ReleaseStorage"), "no step span releases storage: {steps:?}");
}
