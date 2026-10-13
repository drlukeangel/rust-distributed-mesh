//! node.restart is one operation (node-restart.md): `restart-node:<path>` holds every step of the
//! restart, its `NodeRestarting` pre-event, the stop of the live process (its connections deleted,
//! it stops, its process alive) and the start of that same process, and ends with one `Complete`. No step of it
//! is receipted under `retire-node:` (the delete's operation), nothing is deployed, nothing exits.
//! The cell drives the restart pipeline over a recording provider, observer and lifecycle: nothing
//! here depends on timing.

use rafka_mesh_entity::{IncarnationId, LifecycleOp, NodeId};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::deployment::pipeline::{
    CommandAdmission, CommandContext, Completion, DeploymentPipeline, LaunchTemplate, LifecycleEvents, MeshLeaveEvent, NodeObserver, Publication, StartRequest, Timeouts, TopologySink,
};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::model::{DeploymentId, EndpointId, Node, NodeStatus, PathName, ProviderKind};
use rafka_node_admin_core::node_commands::NodeCommand;
use std::sync::Mutex;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

const NODE: &str = "mesh1.rpc.1";

/// The provider: the runtime runs throughout; a terminate is recorded (a restart asks none).
#[derive(Default)]
struct Running {
    terminated: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for Running {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "i143-3093-restart".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.to_string(), reason: "a restart deploys nothing".into() })
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        self.terminated.lock().unwrap().push(h.deployment_id.to_string());
        Ok(())
    }
    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        DeploymentStatus::Running
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
        Ok(rafka_node_admin_core::deployment::provider::Found::NeverLaunched)
    }
}

/// The live birth: it admits the stop, answers it, stays stopped, and answers the start
/// `Started`; or (`refuses_stop`) it refuses the stop by name and so never stops. Every command and
/// every completion wait is recorded with the operation it carried.
#[derive(Default)]
struct StopsThenStarts {
    sent: Mutex<Vec<String>>,
    refuses_stop: bool,
    /// The birth answers the stop `Stopped` on its call (node-stop.md: the reply is the stop's
    /// completion), and nothing else ever signals it: a wait for a separate completion never ends.
    answers_stopped: bool,
    /// The birth answers the start `AlreadyApplied`: this start command, reissued by a later attempt
    /// of its Build, already brought it back (node-rpc-envelope.md: a reissued idempotent step settles
    /// with AlreadyApplied).
    start_already_applied: bool,
}

#[async_trait::async_trait]
impl NodeObserver for StopsThenStarts {
    async fn send_command(&self, node: &Node, cmd: NodeCommand, ctx: &CommandContext) -> CommandAdmission {
        self.sent.lock().unwrap().push(format!("{cmd:?} {} to {}", ctx.operation, node.name));
        match cmd {
            NodeCommand::Start if self.start_already_applied => CommandAdmission::AlreadyAdmitted,
            NodeCommand::Start => CommandAdmission::Started,
            NodeCommand::Stop if self.refuses_stop => CommandAdmission::Refused { reply: "RejectedStaleIncarnation".into() },
            NodeCommand::Stop if self.answers_stopped => CommandAdmission::Stopped { receipt: rafka_node_rpc_contract::status::ConnectionsReceipt::Established { in_flight: 0, closed: vec![] } },
            _ => CommandAdmission::Admitted,
        }
    }
    async fn await_completion(&self, node: &Node, cmd: NodeCommand, ctx: &CommandContext, _: Duration) -> Completion {
        self.sent.lock().unwrap().push(format!("await {cmd:?} {} from {}", ctx.operation, node.name));
        if self.answers_stopped {
            return Completion::Deadline;
        }
        Completion::Received
    }
    async fn transfer_seats(&self, node: &Node, _: &CommandContext) -> Result<rafka_node_admin_core::fabric_handover::SeatTransferOutcome, String> {
        Err(format!("{} is an rpc node: it holds no seat", node.name))
    }
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

/// Every lifecycle event the restart publishes, with the operation its op carries.
#[derive(Default)]
struct Events {
    published: Mutex<Vec<(&'static str, String)>>,
}

impl Events {
    fn note(&self, event: &'static str, op: &LifecycleOp) {
        self.published.lock().unwrap().push((event, op.operation.clone()));
    }
}

#[async_trait::async_trait]
impl LifecycleEvents for Events {
    async fn deleting(&self, op: &LifecycleOp) {
        self.note("deleting", op);
    }
    async fn deleted(&self, op: &LifecycleOp) {
        self.note("deleted", op);
    }
    async fn restarting(&self, op: &LifecycleOp) {
        self.note("restarting", op);
    }
    async fn stopped(&self, op: &LifecycleOp) {
        self.note("stopped", op);
    }
    async fn joined(&self, op: &LifecycleOp) {
        self.note("joined", op);
    }
    async fn connections_deleting(&self, op: &LifecycleOp) {
        self.note("connections-deleting", op);
    }
    async fn leaving(&self, op: &LifecycleOp) {
        self.note("leaving", op);
    }
    async fn mesh_leaving(&self, _: &MeshLeaveEvent) {
        self.published.lock().unwrap().push(("mesh-leaving", String::new()));
    }
    async fn mesh_leave(&self, _: &MeshLeaveEvent) {
        self.published.lock().unwrap().push(("mesh-leave", String::new()));
    }
    async fn mesh_left(&self, _: &MeshLeaveEvent) {
        self.published.lock().unwrap().push(("mesh-left", String::new()));
    }
    fn now_rafka_ms(&self) -> u64 {
        1_000_000
    }
}

struct Discard;
impl TopologySink for Discard {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, _: &PathName, _: &NodeId, _: &IncarnationId) {}
}

/// CONTRACT (node-restart.md "node.restart is a composed workflow: NodeRestarting + the entire
/// node.stop + the entire node.start + node.restarted. It runs on the stopped process, which never
/// exits"; "No supersedes lineage or new incarnation is introduced"): the restart of a live rpc node
/// is ONE Build operation, `restart-node:<path>`. Its receipts, in order: `NodeRestarting`, the stop
/// of the live process (`StopNode`, `AwaitNodeLeft`: it stays, stopped), the
/// start of the same process (`StartNode`, `WaitForNodeReady`), and one `Complete`, last. The
/// `NodeRestarting` pre-event carries that same operation, published once; the stop's `stopped` is
/// not published as `NodeStopped`; nothing is deleted. Must NOT happen: any receipt under
/// `retire-node:` (the delete's operation) or any other operation, a `Complete` before the start
/// (it would end the restart's hold while the birth is still stopped), a `TerminateRuntime` or a
/// provider terminate, a `NodeDeleting`/`NodeDeleted`.
#[tokio::test]
async fn a_restart_is_one_operation_whose_steps_stop_and_start_the_same_process() {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture("i143-3093-restart-one-op", exporter.clone());
    let _g = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let name: PathName = NODE.parse().unwrap();
    let incarnation = IncarnationId::mint();
    let mut node = Node::allocated(name.clone());
    node.node_id = NodeId::mint();
    node.incarnation_id = Some(incarnation.clone());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::ReadyForTraffic;
    node.endpoint_id = Some(EndpointId("i143-3093-endpoint".into()));
    node.data_dir = Some("/nonexistent/i143-3093/mesh1.rpc.1".into());
    let template = LaunchTemplate {
        fabric: "fabric1".into(),
        fabric_id: rafka_mesh_entity::FabricId::mint(),
        executable: "/nonexistent/restart-one-op".into(),
        seeds: vec![],
        launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-3093-restart-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-3093-restart-store-{}", NodeId::mint())),
        mesh_issuer: None,
        connections_delete_deadline: rafka_mesh_entity::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let (provider, observer, events, joins) = (Running::default(), StopsThenStarts::default(), Events::default(), rafka_node_admin_core::join::Joins::default());
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &joins,
        observer: &observer,
        sink: &Discard,
        lifecycle: &events,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { completion: Duration::from_millis(300), stop_grace: Duration::from_millis(300), ready: Duration::from_millis(300), ..Timeouts::default() },
    };
    let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(4242), start: Some(1), container: None, domain: Some("i143-3093-restart".into()) };
    let deployed = rafka_node_admin_core::join::Deployed {
        name: name.clone(),
        node_id: node.node_id.clone(),
        incarnation: incarnation.clone(),
        supersedes: None,
        endpoint_id: node.endpoint_id.clone().unwrap(),
        runtime: handle.fact().expect("the handle names its exact runtime"),
        data_dir: node.data_dir.clone().unwrap(),
        last_accepted_name: Some(name.clone()),
        held: true,
    };
    let out = pipeline.restart(&StartRequest { build_id: build_id.clone(), attempt: 1, node, handle, deployed }).await;

    let view = builds.read_build(&build_id).await.unwrap();
    let receipts: Vec<(String, String)> = view.steps.iter().map(|s| (s.operation.clone(), s.step.clone())).collect();
    let restart_op = format!("restart-node:{NODE}");
    let foreign: Vec<&(String, String)> = receipts.iter().filter(|(op, _)| *op != restart_op).collect();
    assert!(foreign.is_empty(), "every receipt of the restart is under {restart_op}; these are not: {foreign:?} (all: {receipts:?})");
    let order: Vec<&str> = receipts.iter().map(|(_, s)| s.as_str()).collect();
    assert_eq!(
        order,
        ["NodeRestarting", "StopNode", "AwaitNodeLeft", "StartNode", "WaitForNodeReady", "Complete"],
        "the pre-event, the stop of the live process, the start of the same process, one Complete: {out:?}"
    );
    assert!(out.is_ok(), "{out:?}");
    assert_eq!(
        *observer.sent.lock().unwrap(),
        [
            format!("Stop stop-node:{NODE} to {NODE}"),
            format!("await Stop stop-node:{NODE} from {NODE}"),
            format!("Start start-node:{NODE} to {NODE}"),
        ],
        "the restart calls the stop and the start of the same birth, once each"
    );
    assert_eq!(*provider.terminated.lock().unwrap(), Vec::<String>::new(), "a restart asks the provider for no exit");
    let published = events.published.lock().unwrap().clone();
    assert_eq!(published.iter().filter(|(e, _)| *e == "restarting").map(|(_, op)| op.as_str()).collect::<Vec<_>>(), [restart_op.as_str()], "NodeRestarting carries the restart's own operation, once: {published:?}");
    assert!(!published.iter().any(|(e, _)| matches!(*e, "stopped" | "deleting" | "deleted")), "the restart's stop is not published as NodeStopped and nothing is deleted: {published:?}");

    capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
    let spans = exporter.get_finished_spans().unwrap();
    crate::assert_own_story(spans.iter().map(|s| s.name.as_ref()));
    let attr = |s: &opentelemetry_sdk::export::trace::SpanData, k: &str| s.attributes.iter().find(|kv| kv.key.as_str() == k).map(|kv| kv.value.to_string()).unwrap_or_default();
    let pipelines: Vec<&opentelemetry_sdk::export::trace::SpanData> = spans.iter().filter(|s| s.name == "rdm.node_admin.deployment.update.via-pipeline").collect();
    assert_eq!(pipelines.iter().map(|s| attr(s, "pipeline")).collect::<Vec<_>>(), ["restart"], "one pipeline span: the restart");
    let steps: Vec<&opentelemetry_sdk::export::trace::SpanData> = spans.iter().filter(|s| s.name == "rdm.node_admin.deployment.update.via-step").collect();
    for s in &steps {
        eprintln!("span {} trace_id={} parent={} step={} outcome={}", s.name, s.span_context.trace_id(), s.parent_span_id, attr(s, "step"), attr(s, "outcome"));
    }
    eprintln!("span {} trace_id={} span_id={}", pipelines[0].name, pipelines[0].span_context.trace_id(), pipelines[0].span_context.span_id());
    assert!(steps.iter().all(|s| s.parent_span_id == pipelines[0].span_context.span_id()), "every step is a child of the one restart pipeline span");
    assert_eq!(steps.iter().map(|s| attr(s, "step")).collect::<Vec<_>>(), order, "the step spans are the receipts, in order");
}

/// CONTRACT (node-rpc-envelope.md: "Reissuing a naturally idempotent uncertain step can settle it with
/// AlreadyApplied"; node-status.md: a mesh-primary sets the status from the node's own report): a start
/// the birth answers `AlreadyApplied` is the reissue of the start that already brought it back, whose
/// node.joined and node.started its mesh-primary took then. The restart takes it as started and goes on
/// to WaitForNodeReady. Must NOT happen: the step taking node.joined again, which sets the birth back to
/// Pending at its mesh-primary with no node.started ever to follow; or the step failing the start as
/// "not answered Started".
#[tokio::test]
async fn a_restart_whose_reissued_start_is_already_applied_takes_no_new_join() {
    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let name: PathName = NODE.parse().unwrap();
    let incarnation = IncarnationId::mint();
    let mut node = Node::allocated(name.clone());
    node.node_id = NodeId::mint();
    node.incarnation_id = Some(incarnation.clone());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::ReadyForTraffic;
    node.endpoint_id = Some(EndpointId("i143-3093-endpoint".into()));
    node.data_dir = Some("/nonexistent/i143-3093/mesh1.rpc.1".into());
    let template = LaunchTemplate {
        fabric: "fabric1".into(),
        fabric_id: rafka_mesh_entity::FabricId::mint(),
        executable: "/nonexistent/restart-one-op".into(),
        seeds: vec![],
        launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-3093-restart-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-3093-restart-store-{}", NodeId::mint())),
        mesh_issuer: None,
        connections_delete_deadline: rafka_mesh_entity::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let (provider, observer, events, joins) = (Running::default(), StopsThenStarts { answers_stopped: true, start_already_applied: true, ..StopsThenStarts::default() }, Events::default(), rafka_node_admin_core::join::Joins::default());
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &joins,
        observer: &observer,
        sink: &Discard,
        lifecycle: &events,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { completion: Duration::from_millis(300), stop_grace: Duration::from_millis(300), ready: Duration::from_millis(300), ..Timeouts::default() },
    };
    let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(4242), start: Some(1), container: None, domain: Some("i143-3093-restart".into()) };
    let deployed = rafka_node_admin_core::join::Deployed {
        name: name.clone(),
        node_id: node.node_id.clone(),
        incarnation: incarnation.clone(),
        supersedes: None,
        endpoint_id: node.endpoint_id.clone().unwrap(),
        runtime: handle.fact().expect("the handle names its exact runtime"),
        data_dir: node.data_dir.clone().unwrap(),
        last_accepted_name: Some(name.clone()),
        held: true,
    };
    let out = pipeline.restart(&StartRequest { build_id: build_id.clone(), attempt: 1, node, handle, deployed }).await;
    assert!(out.is_ok(), "the start was already applied: the restart goes on and completes: {out:?}");
    let view = builds.read_build(&build_id).await.unwrap();
    let order: Vec<String> = view.steps.iter().map(|s| s.step.clone()).collect();
    assert_eq!(order, ["NodeRestarting", "StopNode", "AwaitNodeLeft", "StartNode", "WaitForNodeReady", "Complete"], "{out:?}");
    let published = events.published.lock().unwrap().clone();
    assert!(!published.iter().any(|(e, _)| *e == "joined"), "an already-applied start takes no node.joined: {published:?}");
}

/// CONTRACT (node-stop.md: stop-node answers `Stopped` on its own call, and that reply is the stop's
/// completion; node-rpc-envelope.md: "Reissuing a naturally idempotent uncertain step can settle it"):
/// a restart whose stop the birth answered `Stopped` takes that reply as its left reply: `AwaitNodeLeft`
/// completes from the `StopNode` step's own recorded answer and the start follows. Must NOT happen: the
/// wait looking for a second, process-local completion signal, which a successor executor reusing a
/// lost predecessor's `StopNode` receipt never holds (the restart then failing at `AwaitNodeLeft` with
/// `Deadline` on a birth that is stopped).
#[tokio::test]
async fn a_restart_takes_its_stops_stopped_reply_as_the_left_reply() {
    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let name: PathName = NODE.parse().unwrap();
    let incarnation = IncarnationId::mint();
    let mut node = Node::allocated(name.clone());
    node.node_id = NodeId::mint();
    node.incarnation_id = Some(incarnation.clone());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::ReadyForTraffic;
    node.endpoint_id = Some(EndpointId("i143-3093-endpoint".into()));
    node.data_dir = Some("/nonexistent/i143-3093/mesh1.rpc.1".into());
    let template = LaunchTemplate {
        fabric: "fabric1".into(),
        fabric_id: rafka_mesh_entity::FabricId::mint(),
        executable: "/nonexistent/restart-one-op".into(),
        seeds: vec![],
        launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-3093-restart-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-3093-restart-store-{}", NodeId::mint())),
        mesh_issuer: None,
        connections_delete_deadline: rafka_mesh_entity::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let (provider, observer, events, joins) = (Running::default(), StopsThenStarts { answers_stopped: true, ..StopsThenStarts::default() }, Events::default(), rafka_node_admin_core::join::Joins::default());
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &joins,
        observer: &observer,
        sink: &Discard,
        lifecycle: &events,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { completion: Duration::from_millis(300), stop_grace: Duration::from_millis(300), ready: Duration::from_millis(300), ..Timeouts::default() },
    };
    let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(4242), start: Some(1), container: None, domain: Some("i143-3093-restart".into()) };
    let deployed = rafka_node_admin_core::join::Deployed {
        name: name.clone(),
        node_id: node.node_id.clone(),
        incarnation: incarnation.clone(),
        supersedes: None,
        endpoint_id: node.endpoint_id.clone().unwrap(),
        runtime: handle.fact().expect("the handle names its exact runtime"),
        data_dir: node.data_dir.clone().unwrap(),
        last_accepted_name: Some(name.clone()),
        held: true,
    };
    let out = pipeline.restart(&StartRequest { build_id: build_id.clone(), attempt: 1, node, handle, deployed }).await;
    assert!(out.is_ok(), "the stop answered Stopped, so it is complete and the start follows: {out:?}");
    let view = builds.read_build(&build_id).await.unwrap();
    let order: Vec<String> = view.steps.iter().map(|s| s.step.clone()).collect();
    assert_eq!(order, ["NodeRestarting", "StopNode", "AwaitNodeLeft", "StartNode", "WaitForNodeReady", "Complete"], "{out:?}");
    assert_eq!(
        *observer.sent.lock().unwrap(),
        [format!("Stop stop-node:{NODE} to {NODE}"), format!("Start start-node:{NODE} to {NODE}")],
        "the Stopped reply is the completion: nothing waits for another"
    );
}

/// CONTRACT (node-restart.md: the restart composes "the entire node.stop"; node-stop.md "Completion:
/// The matching node.stopped on the stop stream and the recorded stop receipt"): a restart whose
/// stop the live birth refuses by name has no `stopped` to follow, so its stop is not complete: the
/// restart fails at `AwaitNodeLeft`, naming the refusal, and runs nothing after it. Must NOT happen:
/// the restart taking the refused stop as done and sending `start-node` to a birth that never
/// stopped, or receipting a `Complete`.
#[tokio::test]
async fn a_restart_whose_stop_is_refused_fails_at_its_stop_and_starts_nothing() {
    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let name: PathName = NODE.parse().unwrap();
    let incarnation = IncarnationId::mint();
    let mut node = Node::allocated(name.clone());
    node.node_id = NodeId::mint();
    node.incarnation_id = Some(incarnation.clone());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::ReadyForTraffic;
    node.endpoint_id = Some(EndpointId("i143-3093-endpoint".into()));
    node.data_dir = Some("/nonexistent/i143-3093/mesh1.rpc.1".into());
    let template = LaunchTemplate {
        fabric: "fabric1".into(),
        fabric_id: rafka_mesh_entity::FabricId::mint(),
        executable: "/nonexistent/restart-one-op".into(),
        seeds: vec![],
        launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-3093-restart-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-3093-restart-store-{}", NodeId::mint())),
        mesh_issuer: None,
        connections_delete_deadline: rafka_mesh_entity::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let (provider, observer, events, joins) = (Running::default(), StopsThenStarts { refuses_stop: true, ..StopsThenStarts::default() }, Events::default(), rafka_node_admin_core::join::Joins::default());
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &joins,
        observer: &observer,
        sink: &Discard,
        lifecycle: &events,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { completion: Duration::from_millis(300), stop_grace: Duration::from_millis(300), ready: Duration::from_millis(300), ..Timeouts::default() },
    };
    let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(4242), start: Some(1), container: None, domain: Some("i143-3093-restart".into()) };
    let deployed = rafka_node_admin_core::join::Deployed {
        name: name.clone(),
        node_id: node.node_id.clone(),
        incarnation,
        supersedes: None,
        endpoint_id: node.endpoint_id.clone().unwrap(),
        runtime: handle.fact().expect("the handle names its exact runtime"),
        data_dir: node.data_dir.clone().unwrap(),
        last_accepted_name: Some(name.clone()),
        held: true,
    };
    let out = pipeline.restart(&StartRequest { build_id: build_id.clone(), attempt: 1, node, handle, deployed }).await;

    let view = builds.read_build(&build_id).await.unwrap();
    let receipts: Vec<(String, bool)> = view.steps.iter().map(|s| (s.step.clone(), s.outcome == rafka_node_admin_core::build_state::StepOutcome::Complete)).collect();
    let sent = observer.sent.lock().unwrap().clone();
    assert!(!sent.iter().any(|c| c.starts_with("Start ")), "no start-node to a birth that never stopped: {sent:?} (receipts {receipts:?})");
    let err = out.expect_err("a restart whose stop was refused is not complete");
    assert_eq!(err.step, "AwaitNodeLeft", "{err:?}");
    assert!(err.reason.contains("RejectedStaleIncarnation"), "the failure names the refusal: {err:?}");
    assert_eq!(
        receipts,
        [("NodeRestarting", true), ("StopNode", true), ("AwaitNodeLeft", false)].map(|(s, ok)| (s.to_string(), ok)),
        "the restart stops at its stop's wait, Failed, and receipts nothing after it"
    );
    assert_eq!(*provider.terminated.lock().unwrap(), Vec::<String>::new(), "a failed restart asks the provider for no exit either");
}

/// The live birth whose start runs its whole sequence before it answers (a node-admin's hook runs
/// inside its start): on `start-node` it joins its commander (the join door admits its digest), then
/// reports node.started, and only then answers `Started`.
struct StartsWhole<'a> {
    joins: &'a rafka_node_admin_core::join::Joins,
    events: &'a Events,
    digest: rafka_mesh_entity::MeshDigest,
}

#[async_trait::async_trait]
impl NodeObserver for StartsWhole<'_> {
    async fn send_command(&self, _: &Node, cmd: NodeCommand, ctx: &CommandContext) -> CommandAdmission {
        match cmd {
            NodeCommand::Start => {
                // The join, admitted at the commander's door, then the start's own steps and its report.
                self.joins.report(&self.digest);
                tokio::task::yield_now().await;
                self.events.published.lock().unwrap().push(("node.started", ctx.operation.clone()));
                CommandAdmission::Started
            }
            _ => CommandAdmission::Admitted,
        }
    }
    async fn await_completion(&self, _: &Node, _: NodeCommand, _: &CommandContext, _: Duration) -> Completion {
        Completion::Received
    }
    async fn transfer_seats(&self, node: &Node, _: &CommandContext) -> Result<rafka_node_admin_core::fabric_handover::SeatTransferOutcome, String> {
        Err(format!("{} is an rpc node: it holds no seat", node.name))
    }
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

/// CONTRACT (node-start.md "End-to-end sequence": the start emits `node.joined` after the accepted
/// join, then runs its hook, topology, Build floor and rafka-time, then emits `node.started`; states.md
/// "Who sets each state": node pending from node.joined, node ReadyForTraffic from node.started): the
/// executor takes a start's node.joined when the birth's join is admitted, before the birth can report
/// node.started, whatever moment the birth's `Started` answer reaches it. A birth that runs its whole
/// start before it answers (a node-admin's hook runs inside its start) has node.joined taken first and
/// node.started after it. Must NOT happen: node.joined taken at the `Started` answer, after the birth's
/// node.started, so that the Pending node.joined sets stands over the ReadyForTraffic node.started set.
#[tokio::test]
async fn a_starts_node_joined_is_taken_at_its_admitted_join_before_its_node_started() {
    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let name: PathName = NODE.parse().unwrap();
    let incarnation = IncarnationId::mint();
    let mut node = Node::allocated(name.clone());
    node.node_id = NodeId::mint();
    node.incarnation_id = Some(incarnation.clone());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::ReadyForTraffic;
    node.endpoint_id = Some(EndpointId("i143-s52-endpoint".into()));
    node.data_dir = Some("/nonexistent/i143-s52/mesh1.rpc.1".into());
    let template = LaunchTemplate {
        fabric: "fabric1".into(),
        fabric_id: rafka_mesh_entity::FabricId::mint(),
        executable: "/nonexistent/start-order".into(),
        seeds: vec![],
        launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-s52-start-order-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-s52-start-order-store-{}", NodeId::mint())),
        mesh_issuer: None,
        connections_delete_deadline: rafka_mesh_entity::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(4242), start: Some(1), container: None, domain: Some("i143-s52-start-order".into()) };
    let runtime = handle.fact().expect("the handle names its exact runtime");
    let deployed = rafka_node_admin_core::join::Deployed {
        name: name.clone(),
        node_id: node.node_id.clone(),
        incarnation: incarnation.clone(),
        supersedes: None,
        endpoint_id: node.endpoint_id.clone().unwrap(),
        runtime: runtime.clone(),
        data_dir: node.data_dir.clone().unwrap(),
        last_accepted_name: Some(name.clone()),
        held: true,
    };
    let digest = rafka_mesh_entity::MeshDigest {
        fabric_id: template.fabric_id.clone(),
        node: rafka_mesh_entity::MeshNode { node_id: node.node_id.clone(), name: name.clone(), endpoint_id: node.endpoint_id.clone().unwrap(), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: incarnation.clone(), supersedes: None, runtime: Some(runtime) },
        status: rafka_mesh_entity::MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 9,
        emitted_at_rafka_ms: 9,
        data_dir: node.data_dir.clone(),
        mesh_id: None,
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
    };
    let (provider, events, joins) = (Running::default(), Events::default(), rafka_node_admin_core::join::Joins::default());
    let observer = StartsWhole { joins: &joins, events: &events, digest };
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &joins,
        observer: &observer,
        sink: &Discard,
        lifecycle: &events,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { completion: Duration::from_millis(300), stop_grace: Duration::from_millis(300), ready: Duration::from_millis(300), ..Timeouts::default() },
    };
    let out = pipeline.restart(&StartRequest { build_id: build_id.clone(), attempt: 1, node, handle, deployed }).await;
    assert!(out.is_ok(), "{out:?}");
    let published = events.published.lock().unwrap().clone();
    let order: Vec<&str> = published.iter().map(|(e, _)| *e).filter(|e| matches!(*e, "joined" | "node.started")).collect();
    assert_eq!(order, ["joined", "node.started"], "node.joined is taken at the admitted join, before the birth's node.started: {published:?}");
}
