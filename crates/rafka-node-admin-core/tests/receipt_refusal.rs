//! i143.e12.s33 functional: a step whose receipt the Build state refuses is not complete. The
//! receipt is the step's durable commit (node-recovery.md "Failure and receipts": durable step
//! outcomes are `Complete` or `Failed { step, reason }`), and a receipt that cannot be recorded or
//! broadcast (a fact with no wire shape, a fenced seat, an I/O failure) fails the step by name: its
//! span, and the pipeline's error naming the step and the refusal. Nothing after it runs.

use opentelemetry::trace::TracerProvider as _;
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome, MemoryBuildStateAdapter,
};
use rafka_node_admin_core::deployment::pipeline::{CommandAdmission, CommandContext, Completion, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, RetireKind, RetireRequest, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::model::{IncarnationId, Node, NodeId, NodeStatus, PathName, ProviderKind};
use rafka_node_admin_core::node_commands::NodeCommand;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

struct Running;
#[async_trait::async_trait]
impl DeploymentProvider for Running {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "i143-3051-refusal".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this cell births nothing".into() })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        DeploymentStatus::Running
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        None
    }
}

/// The birth admits the drain and calls its completion back at once.
struct Drains;
#[async_trait::async_trait]
impl NodeObserver for Drains {
    async fn send_command(&self, _: &Node, _: NodeCommand, _: &CommandContext) -> CommandAdmission {
        CommandAdmission::Admitted
    }
    async fn await_completion(&self, _: &Node, _: NodeCommand, _: &CommandContext, _: Duration) -> Completion {
        Completion::Received
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

/// The Build state, refusing the receipt of one step as the fabric's adapter refuses a fact with no
/// wire shape.
struct Refuses {
    inner: MemoryBuildStateAdapter,
    step: &'static str,
}

#[async_trait::async_trait]
impl BuildStateAdapter for Refuses {
    async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(a).await
    }
    async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(o).await
    }
    async fn read_build(&self, id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(id).await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &BuildStepReceipt) -> Result<(), BuildStateError> {
        if r.step == self.step {
            return Err(BuildStateError::Unencodable(format!("the output of step {} of {} attempt {} has no wire shape: this cell refuses it", r.step, r.build_id, r.attempt)));
        }
        self.inner.append_step_receipt(r).await
    }
    async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(r).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
}

/// CONTRACT (node-recovery.md "Failure and receipts"; CLAUDE.md "an error state carries every
/// non-leaking detail"): a standalone drain whose `DrainNode` receipt the Build state refuses (here
/// as a fact with no wire shape) fails at `DrainNode`, the error naming the step and the refusal,
/// under a `rdm.node_admin.deployment.reject.via-receipt-refused` span naming the Build, attempt,
/// operation, step and refusal. Must NOT happen: the refusal swallowed and the run going on to
/// `AwaitNodeDrained` and `Complete` as if the step were committed.
#[tokio::test]
async fn a_step_whose_receipt_is_refused_fails_by_name_and_the_run_stops() {
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", "i143-3051-receipt-refusal")]))
        .build();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-3051"))));

    let builds = Refuses { inner: MemoryBuildStateAdapter::new(), step: "DrainNode" };
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
    node.incarnation_id = Some(IncarnationId::mint());
    node.status = NodeStatus::ReadyForTraffic;
    let template = LaunchTemplate {
        fabric: "fabric1".into(),
        fabric_id: rafka_mesh_entity::FabricId::mint(),
        executable: "/nonexistent/rshape-none".into(),
        seeds: vec![],
        launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-3051-refusal-{}", NodeId::mint())),
        mesh_issuer: None,
    };
    let joins = rafka_node_admin_core::join::Joins::default();
    let pipeline = DeploymentPipeline { provider: &Running, joins: &joins, observer: &Drains, sink: &Discard, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
    let handle = DeploymentHandle { deployment_id: rafka_node_admin_core::model::DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("i143-3051-refusal".into()) };
    let req = RetireRequest { build_id: build_id.clone(), attempt: 1, node, handle, kind: RetireKind::Removal };

    let err = match pipeline.drain(&req).await {
        Err(e) => e,
        Ok(()) => panic!("the DrainNode receipt was refused and the drain still reported complete: the refusal was swallowed"),
    };
    assert_eq!(err.step, "DrainNode", "the step whose receipt was refused: {err:?}");
    assert!(err.reason.contains("has no wire shape: this cell refuses it"), "the refusal is named in the error: {err:?}");
    let steps: Vec<String> = builds.inner.read_build(&build_id).await.unwrap().steps.iter().map(|s| s.step.clone()).collect();
    assert!(steps.is_empty(), "nothing after the refused step ran: {steps:?}");

    let _ = provider.force_flush();
    let spans: Vec<_> = exporter.get_finished_spans().unwrap().into_iter().filter(|s| s.name == "rdm.node_admin.deployment.reject.via-receipt-refused").collect();
    assert_eq!(spans.len(), 1, "one refusal span");
    let attrs: std::collections::BTreeMap<String, String> = spans[0].attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect();
    assert_eq!(attrs.get("step").map(String::as_str), Some("DrainNode"), "{attrs:?}");
    assert_eq!(attrs.get("operation").map(String::as_str), Some("drain-node:mesh1.rpc.1"), "{attrs:?}");
    assert_eq!(attrs.get("build_id"), Some(&build_id.to_string()), "{attrs:?}");
    assert!(attrs.get("detail").is_some_and(|d| d.contains("has no wire shape")), "{attrs:?}");
    eprintln!("span rdm.node_admin.deployment.reject.via-receipt-refused trace_id={} {attrs:?}", spans[0].span_context.trace_id());
}
