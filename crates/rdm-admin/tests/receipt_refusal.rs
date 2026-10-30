//! i143.e12.s33 functional: a step whose receipt the Build state refuses is not complete. The
//! receipt is the step's durable commit (node-recovery.md "Failure and receipts": durable step
//! outcomes are `Complete` or `Failed { step, reason }`), and a receipt that is not written to the
//! executor's own log (a fact with no wire shape, a targeted role, a failed local write) fails the
//! step by name: its span, and the pipeline's error naming the step and the refusal. Nothing after
//! it runs. A Build
//! the run cannot read at its start fails the run by name before any step (CLAUDE.md: "never written"
//! and "could not be read" are different errors, and neither is an empty answer).

use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome, MemoryBuildStateAdapter,
};
use rdm_admin::deployment::pipeline::{CommandAdmission, CommandContext, Completion, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, DeleteRequest, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::{IncarnationId, Node, NodeId, NodeStatus, PathName, ProviderKind};
use rdm_admin::node_commands::NodeCommand;
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
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// The birth answers every command at once: `node.connection.delete { All }` with its
/// `node.connections.deleted`, any other command admitted.
struct Answers;
#[async_trait::async_trait]
impl NodeObserver for Answers {
    async fn send_command(&self, _: &Node, cmd: NodeCommand, _: &CommandContext) -> CommandAdmission {
        match cmd {
            NodeCommand::DeleteConnections => CommandAdmission::ConnectionsDeleted { receipt: rdm_core::status::ConnectionsReceipt::Established { in_flight: 0, closed: vec![] } },
            _ => CommandAdmission::Admitted {},
        }
    }
    async fn await_completion(&self, _: &Node, _: NodeCommand, _: &CommandContext, _: Duration) -> Completion {
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

/// The Build state, refusing the receipt of one step: as the fabric's adapter refuses a fact with no
/// wire shape (`local: false`), or as a local log whose write fails (`local: true`).
struct Refuses {
    inner: MemoryBuildStateAdapter,
    step: &'static str,
    local: bool,
    /// The Build cannot be read: the journal read fails with this.
    unreadable: Option<&'static str>,
}

#[async_trait::async_trait]
impl rdm_admin::build_state::LocalBuildLog for Refuses {
    async fn absorb_facts(&self, facts: &[BuildFact]) {
        self.inner.absorb_facts(facts).await
    }
    async fn derive_on_every_fact(&self, derive: rdm_admin::build_state::FactDerivation) {
        self.inner.derive_on_every_fact(derive).await
    }
    async fn put_attempt_context(&self, build_id: &rdm_admin::build::BuildId, lane: Option<&str>, attempt: u32, context: &rdm_core::context::CallContext) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.put_attempt_context(build_id, lane, attempt, context).await
    }
    async fn attempt_context(&self, build_id: &rdm_admin::build::BuildId, lane: Option<&str>, attempt: u32) -> Result<Option<rdm_core::context::CallContext>, rdm_admin::storage_error::StorageError> {
        self.inner.attempt_context(build_id, lane, attempt).await
    }
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
        if let Some(why) = self.unreadable {
            return Err(BuildStateError::Io(format!("journal read of Build {id}: {why}")));
        }
        self.inner.read_build(id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
        self.inner.newest_accepted_at_ms().await
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
        if r.step == self.step && self.local {
            return Err(BuildStateError::Io(format!("journal write of step {} of {} attempt {}: this cell's disk refuses it", r.step, r.build_id, r.attempt)));
        }
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
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &rdm_admin::build::BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
}

/// CONTRACT (node-recovery.md "Failure and receipts"; CLAUDE.md "an error state carries every
/// non-leaking detail"): a standalone node.connection.delete whose `DeleteConnections` receipt the Build state refuses (here
/// as a fact with no wire shape) fails at `DeleteConnections`, the error naming the step and the refusal,
/// under a `rdm.node_admin.deployment.reject.via-receipt-refused` span naming the Build, attempt,
/// operation, step and refusal. Must NOT happen: the refusal swallowed and the run going on to
/// `Complete` as if the step were committed.
#[tokio::test]
async fn a_step_whose_receipt_is_refused_fails_by_name_and_the_run_stops() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (spans_pipeline, layer) = rdm_transport::span_capture("i143-3051-receipt-refusal", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let builds = Refuses { inner: MemoryBuildStateAdapter::new(), step: "DeleteConnections", local: false, unreadable: None };
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
    node.incarnation_id = Some(IncarnationId::mint());
    node.status = NodeStatus::Ready;
    let template = LaunchTemplate { fabric: "fabric1".into(),
        fabric_id: rdm_core::FabricId::mint(),
        executable: "/nonexistent/rshape-none".into(),
        seeds: vec![],
        launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-3051-refusal-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-3051-refusal-{}", NodeId::mint())),
        issuing: None,
        roots: Default::default(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
        binding: None,
    };
    let joins = rdm_admin::join::Joins::default();
    let pipeline = DeploymentPipeline { provider: &Running, joins: &joins, observer: &Answers, sink: &Discard, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
    let handle = DeploymentHandle { deployment_id: rdm_admin::model::DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("i143-3051-refusal".into()) };
    let req = DeleteRequest { build_id: build_id.clone(), attempt: 1, node, handle, unreached: false };

    let err = match pipeline.delete_connections(&req).await {
        Err(e) => e,
        Ok(()) => panic!("the DeleteConnections receipt was refused and the node.connection.delete still reported complete: the refusal was swallowed"),
    };
    assert_eq!(err.step, "DeleteConnections", "the step whose receipt was refused: {err:?}");
    assert!(err.reason.contains("has no wire shape: this cell refuses it"), "the refusal is named in the error: {err:?}");
    let steps: Vec<String> = builds.inner.read_build(&build_id).await.unwrap().steps.iter().map(|s| s.step.clone()).collect();
    assert!(steps.is_empty(), "nothing after the refused step ran: {steps:?}");

    spans_pipeline.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
    let spans: Vec<_> = exporter.get_finished_spans().unwrap().into_iter().filter(|s| s.name == "rdm.node_admin.deployment.reject.via-receipt-refused").collect();
    assert_eq!(spans.len(), 1, "one refusal span");
    let attrs: std::collections::BTreeMap<String, String> = spans[0].attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect();
    assert_eq!(attrs.get("step").map(String::as_str), Some("DeleteConnections"), "{attrs:?}");
    assert_eq!(attrs.get("operation").map(String::as_str), Some("node.connection.delete:mesh1.rpc.1"), "{attrs:?}");
    assert_eq!(attrs.get("build_id"), Some(&build_id.log().to_string()), "{attrs:?}");
    assert!(attrs.get("detail").is_some_and(|d| d.contains("has no wire shape")), "{attrs:?}");
    eprintln!("span rdm.node_admin.deployment.reject.via-receipt-refused trace_id={} {attrs:?}", spans[0].span_context.trace_id());
}

/// CONTRACT (CLAUDE.md "an error state carries every non-leaking detail": "Never written" and "could
/// not be read" are different errors, and neither is an empty answer; node-recovery.md "Failure and
/// receipts": a run hands on the decisions its earlier attempts made): a standalone node.connection.delete of a Build
/// whose journal cannot be read fails the run at its start by name, the error naming the node, the
/// Build, the operation and the read failure, under a
/// `rdm.node_admin.deployment.reject.via-build-unreadable` span, and no step runs or writes a receipt.
/// A Build this node holds no fact of is refused the same way, its own error. Must NOT happen: the
/// unreadable Build taken for one with no earlier receipts, so that the run goes on from its first
/// step and decides afresh what an earlier attempt had already decided.
#[tokio::test]
async fn a_run_whose_build_cannot_be_read_fails_by_name_before_any_step() {
    let (capture, _guard) = Capture::new("i143-s42-build-unreadable");
    let builds = Refuses { inner: MemoryBuildStateAdapter::new(), step: "", local: false, unreadable: Some("this cell's disk refuses it") };
    let build_id = BuildId::mint();
    accepted_and_claimed(&builds.inner, &build_id).await;

    let err = match standalone_through(&builds, &build_id, &Answers, false).await {
        Err(e) => e,
        Ok(()) => panic!("the Build could not be read and the node.connection.delete still reported complete: the read failure was taken for no earlier receipts"),
    };
    assert_eq!(err.step, "ReadBuild", "the run stops at its start: {err:?}");
    for named in ["mesh1.rpc.1", &build_id.to_string(), "node.connection.delete:mesh1.rpc.1", "this cell's disk refuses it"] {
        assert!(err.reason.contains(named), "the error names {named}: {err:?}");
    }
    let steps: Vec<String> = builds.inner.read_build(&build_id).await.unwrap().steps.iter().map(|s| s.step.clone()).collect();
    assert!(steps.is_empty(), "no step ran and no receipt was written: {steps:?}");
    let spans = capture.spans("rdm.node_admin.deployment.reject.via-build-unreadable");
    assert_eq!(spans.len(), 1, "one rejection span: {spans:?}");
    assert_eq!(spans[0].1.get("build_id"), Some(&build_id.log().to_string()), "{spans:?}");
    assert_eq!(spans[0].1.get("operation").map(String::as_str), Some("node.connection.delete:mesh1.rpc.1"), "{spans:?}");
    assert!(spans[0].1.get("detail").is_some_and(|d| d.contains("this cell's disk refuses it")), "{spans:?}");
    eprintln!("span rdm.node_admin.deployment.reject.via-build-unreadable trace_id={} {:?}", spans[0].0, spans[0].1);

    let never_written = MemoryBuildStateAdapter::new();
    let unwritten = BuildId::mint();
    let err = match standalone_through(&never_written, &unwritten, &Answers, false).await {
        Err(e) => e,
        Ok(()) => panic!("the Build was never written on this node and the node.connection.delete still reported complete"),
    };
    assert_eq!(err.step, "ReadBuild", "{err:?}");
    assert!(err.reason.contains(&format!("no Build {unwritten}")), "a Build never written is named as such, not as an unreadable journal: {err:?}");
    assert!(!err.reason.contains("journal"), "{err:?}");
}

/// The birth is never reached: every command ends `NotSent` (the client's dial deadline).
struct NeverReached;
#[async_trait::async_trait]
impl NodeObserver for NeverReached {
    async fn send_command(&self, _: &Node, _: NodeCommand, _: &CommandContext) -> CommandAdmission {
        CommandAdmission::NotSent { reason: "dial-deadline: no connection to the target within the send budget".into() , code: rdm_core::outcome::Code::FailedPrecondition }
    }
    async fn await_completion(&self, _: &Node, _: NodeCommand, _: &CommandContext, _: Duration) -> Completion {
        panic!("a command that was never sent has no completion to await")
    }
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

/// CONTRACT (node-connections-delete.md "Admission is not completion"; "never infer completion from
/// silence"; node-recovery.md "An exit during a workflow belongs to that workflow: its Build ends
/// Failed"): a standalone node.connection.delete whose `node.connection.delete` never reaches the birth (`NotSent`, as when the
/// birth exited or is held) fails at `DeleteConnections` by name, the admission and its reason in
/// the error, and writes no `Complete`. A standalone stop never sent fails at `AwaitNodeLeft` the
/// same way. Must NOT happen: the node.connection.delete or stop recorded complete for a birth never told of it.
#[tokio::test]
async fn a_standalone_connections_delete_or_stop_whose_command_is_never_sent_fails_by_name() {
    for (stop, step) in [(false, "DeleteConnections"), (true, "AwaitNodeLeft")] {
        let local = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        accepted_and_claimed(&local, &build_id).await;
        let err = match standalone_through(&local, &build_id, &NeverReached, stop).await {
            Err(e) => e,
            Ok(()) => panic!("the {} command never reached the birth, yet the operation reported complete", if stop { "stop-node" } else { "node.connection.delete" }),
        };
        assert_eq!(err.step, step, "{err:?}");
        assert!(err.reason.contains("NotSent") && err.reason.contains("dial-deadline"), "the admission and its reason are named: {err:?}");
        let steps: Vec<String> = local.read_build(&build_id).await.unwrap().steps.iter().map(|s| s.step.clone()).collect();
        assert!(!steps.iter().any(|s| s == "Complete"), "no Complete for an operation never sent: {steps:?}");
    }
}

/// A capture of every finished span, by name.
pub(crate) struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    pipeline: rdm_transport::SpanCapture,
}

impl Capture {
    pub(crate) fn new(service: &'static str) -> (Self, tracing::subscriber::DefaultGuard) {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (pipeline, layer) = rdm_transport::span_capture(service, exporter.clone());
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        (Self { exporter, pipeline }, guard)
    }

    pub(crate) fn spans(&self, name: &str) -> Vec<(String, std::collections::BTreeMap<String, String>)> {
        self.pipeline.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| (s.span_context.trace_id().to_string(), s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect()))
            .collect()
    }
}

/// A node-admin's real Build state over `local`, and its endpoint.
async fn fabric_builds(name: &str, local: std::sync::Arc<dyn rdm_admin::build_state::LocalBuildLog>) -> (iroh::Endpoint, rdm_admin::fabric_builds::FabricBuildStateAdapter) {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(iroh::SecretKey::generate())
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    let builds = rdm_admin::fabric_builds::FabricBuildStateAdapter::new(local, &rdm_transport::topology::LocationView::new(&endpoint, name).unwrap(), name.into(), rdm_admin::fabric_builds::PrimarySource::unrecorded());
    (endpoint, builds)
}

/// A standalone node.connection.delete of a ready `mesh1.rpc.1` as attempt 1 of `build_id`, through `builds`.
async fn connections_delete_through(builds: &dyn BuildStateAdapter, build_id: &BuildId) -> Result<(), rdm_admin::deployment::pipeline::PipelineError> {
    standalone_through(builds, build_id, &Answers, false).await
}

/// A standalone node.connection.delete (or, with `stop`, a standalone stop) of a ready `mesh1.rpc.1` as attempt 1 of
/// `build_id`, through `builds`, its commands answered by `observer`.
async fn standalone_through(builds: &dyn BuildStateAdapter, build_id: &BuildId, observer: &dyn NodeObserver, stop: bool) -> Result<(), rdm_admin::deployment::pipeline::PipelineError> {
    let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
    node.incarnation_id = Some(IncarnationId::mint());
    node.status = NodeStatus::Ready;
    let template = LaunchTemplate { fabric: "fabric1".into(),
        fabric_id: rdm_core::FabricId::mint(),
        executable: "/nonexistent/rshape-none".into(),
        seeds: vec![],
        launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-3051-refusal-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-3051-refusal-{}", NodeId::mint())),
        issuing: None,
        roots: Default::default(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
        binding: None,
    };
    let joins = rdm_admin::join::Joins::default();
    let pipeline = DeploymentPipeline { provider: &Running, joins: &joins, observer, sink: &Discard, lifecycle: &NoLifecycleEvents, builds, template: &template, timeouts: Timeouts::default() };
    let handle = DeploymentHandle { deployment_id: rdm_admin::model::DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("i143-3051-refusal".into()) };
    let req = DeleteRequest { build_id: build_id.clone(), attempt: 1, node, handle, unreached: false };
    if stop {
        pipeline.stop(&req).await
    } else {
        pipeline.delete_connections(&req).await
    }
}

/// Build `build_id` accepted and its attempt 1 claimed by `mesh1.admin.1`, in `builds`.
async fn accepted_and_claimed(builds: &dyn BuildStateAdapter, build_id: &BuildId) {
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
}

/// CONTRACT (node-recovery.md "Failure and receipts"): a standalone node.connection.delete run through the fabric's
/// real Build state whose local log refuses the `DeleteConnections` receipt (a failed journal write) fails
/// at `DeleteConnections`, the error naming the step and the failed write, under
/// `rdm.node_admin.deployment.reject.via-receipt-refused`. Must NOT happen: the run going on as if
/// the step were committed, or the receipt broadcast with nothing written locally.
#[tokio::test]
async fn a_step_whose_local_receipt_write_fails_fails_by_name() {
    let (capture, _guard) = Capture::new("i143-3051-local-write-refused");
    let local = std::sync::Arc::new(Refuses { inner: MemoryBuildStateAdapter::new(), step: "DeleteConnections", local: true, unreadable: None });
    let (_endpoint, builds) = fabric_builds("mesh1.admin.1", local.clone()).await;
    let build_id = BuildId::mint();
    accepted_and_claimed(&builds, &build_id).await;

    let err = match connections_delete_through(&builds, &build_id).await {
        Err(e) => e,
        Ok(()) => panic!("the DeleteConnections receipt's local write failed and the node.connection.delete still reported complete"),
    };
    assert_eq!(err.step, "DeleteConnections", "{err:?}");
    assert!(err.reason.contains("this cell's disk refuses it"), "the failed write is named in the error: {err:?}");
    assert!(local.inner.read_build(&build_id).await.unwrap().steps.is_empty(), "nothing after the refused step ran");
    let spans = capture.spans("rdm.node_admin.deployment.reject.via-receipt-refused");
    assert_eq!(spans.len(), 1, "one refusal span: {spans:?}");
    assert_eq!(spans[0].1.get("step").map(String::as_str), Some("DeleteConnections"), "{spans:?}");
    assert!(capture.spans("rdm.node_admin.build.reject.via-broadcast-refused").is_empty(), "nothing was broadcast for an unwritten receipt");
    eprintln!("span rdm.node_admin.deployment.reject.via-receipt-refused trace_id={} {:?}", spans[0].0, spans[0].1);
}
