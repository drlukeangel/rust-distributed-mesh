//! i143.e12.s33 functional: a step whose receipt the Build state refuses is not complete. The
//! receipt is the step's durable commit (node-recovery.md "Failure and receipts": durable step
//! outcomes are `Complete` or `Failed { step, reason }`), and a receipt that is not written to the
//! executor's own log (a fact with no wire shape, a fenced seat, a failed local write) fails the
//! step by name: its span, and the pipeline's error naming the step and the refusal. Nothing after
//! it runs. A receipt written to the local log whose Build-topic broadcast fails is committed: the
//! step completes and the broadcast failure is named (node-rpc-envelope.md "Build carrier":
//! "Progress waits on no gossip; Build-topic facts remain durability and recovery evidence through
//! the existing publication path"); the neighbour-up catch-up and `FetchBuildFacts` repair it.

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
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<Option<DeploymentHandle>, DeployError> {
        Ok(None)
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

/// The Build state, refusing the receipt of one step: as the fabric's adapter refuses a fact with no
/// wire shape (`local: false`), or as a local log whose write fails (`local: true`).
struct Refuses {
    inner: MemoryBuildStateAdapter,
    step: &'static str,
    local: bool,
}

#[async_trait::async_trait]
impl rafka_node_admin_core::build_state::LocalBuildLog for Refuses {
    async fn absorb_facts(&self, facts: &[BuildFact]) {
        self.inner.absorb_facts(facts).await
    }
    fn derive_on_every_fact(&self, derive: rafka_node_admin_core::build_state::FactDerivation) {
        self.inner.derive_on_every_fact(derive)
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
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
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
    let (spans_pipeline, layer) = rafka_mesh_telemetry::span_capture("i143-3051-receipt-refusal", exporter.clone());
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let builds = Refuses { inner: MemoryBuildStateAdapter::new(), step: "DrainNode", local: false };
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
        store_dir: std::env::temp_dir().join(format!("i143-3051-refusal-{}", NodeId::mint())),
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

    spans_pipeline.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
    let spans: Vec<_> = exporter.get_finished_spans().unwrap().into_iter().filter(|s| s.name == "rdm.node_admin.deployment.reject.via-receipt-refused").collect();
    assert_eq!(spans.len(), 1, "one refusal span");
    let attrs: std::collections::BTreeMap<String, String> = spans[0].attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect();
    assert_eq!(attrs.get("step").map(String::as_str), Some("DrainNode"), "{attrs:?}");
    assert_eq!(attrs.get("operation").map(String::as_str), Some("drain-node:mesh1.rpc.1"), "{attrs:?}");
    assert_eq!(attrs.get("build_id"), Some(&build_id.to_string()), "{attrs:?}");
    assert!(attrs.get("detail").is_some_and(|d| d.contains("has no wire shape")), "{attrs:?}");
    eprintln!("span rdm.node_admin.deployment.reject.via-receipt-refused trace_id={} {attrs:?}", spans[0].span_context.trace_id());
}

/// The birth is never reached: every command ends `NotSent` (the client's dial deadline).
struct NeverReached;
#[async_trait::async_trait]
impl NodeObserver for NeverReached {
    async fn send_command(&self, _: &Node, _: NodeCommand, _: &CommandContext) -> CommandAdmission {
        CommandAdmission::NotSent { reason: "dial-deadline: no connection to the target within the send budget".into() }
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

/// CONTRACT (node-drain.md "Admission is not drain completion"; "never infer completion from
/// silence"; node-recovery.md "An exit during a workflow belongs to that workflow: its Build ends
/// Failed"): a standalone drain whose `drain-node` never reaches the birth (`NotSent`, as when the
/// birth exited or is held) fails at `AwaitNodeDrained` by name, the admission and its reason in
/// the error, and writes no `Complete`. A standalone stop never sent fails at `AwaitNodeLeft` the
/// same way. Must NOT happen: the drain or stop recorded complete for a birth never told of it.
#[tokio::test]
async fn a_standalone_drain_or_stop_whose_command_is_never_sent_fails_by_name() {
    for (stop, step) in [(false, "AwaitNodeDrained"), (true, "AwaitNodeLeft")] {
        let local = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        accepted_and_claimed(&local, &build_id).await;
        let err = match standalone_through(&local, &build_id, &NeverReached, stop).await {
            Err(e) => e,
            Ok(()) => panic!("the {} command never reached the birth, yet the operation reported complete", if stop { "stop-node" } else { "drain-node" }),
        };
        assert_eq!(err.step, step, "{err:?}");
        assert!(err.reason.contains("NotSent") && err.reason.contains("dial-deadline"), "the admission and its reason are named: {err:?}");
        let steps: Vec<String> = local.read_build(&build_id).await.unwrap().steps.iter().map(|s| s.step.clone()).collect();
        assert!(!steps.iter().any(|s| s == "Complete"), "no Complete for an operation never sent: {steps:?}");
    }
}

/// A capture of every finished span, by name.
pub(crate) struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    pipeline: rafka_mesh_telemetry::SpanCapture,
}

impl Capture {
    pub(crate) fn new(service: &'static str) -> (Self, tracing::subscriber::DefaultGuard) {
        crate::enable_callsites();
        let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
        let (pipeline, layer) = rafka_mesh_telemetry::span_capture(service, exporter.clone());
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

/// A node-admin's real Build state on the fabric's Build topic, over `local`: its endpoint, its
/// gossip (to shut down) and its router.
async fn fabric_builds(name: &str, local: std::sync::Arc<dyn rafka_node_admin_core::build_state::LocalBuildLog>) -> (iroh::Endpoint, iroh_gossip::net::Gossip, iroh::protocol::Router, rafka_node_admin_core::fabric_builds::FabricBuildStateAdapter) {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(iroh::SecretKey::generate())
        .alpns(vec![iroh_gossip::ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    let router = iroh::protocol::Router::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let builds = rafka_node_admin_core::fabric_builds::FabricBuildStateAdapter::join(
        &gossip,
        &rafka_mesh_transport::membership::LocationBook::new(&endpoint, name).unwrap(),
        &rafka_mesh_entity::FabricId::mint(),
        vec![],
        local,
        std::sync::Arc::new(rafka_node_admin_core::accepted::AcceptedStore::new(std::sync::Arc::new(rafka_node_admin_core::fabric_storage::MemoryFabricStorage::new()), name)),
        rafka_node_admin_core::shutdown::ShutdownControl::memory(name).await,
        name.into(),
        rafka_node_admin_core::fabric_builds::SeatSource::unrecorded(),
        None,
    )
    .await
    .unwrap();
    (endpoint, gossip, router, builds)
}

/// A standalone drain of a ready `mesh1.rpc.1` as attempt 1 of `build_id`, through `builds`.
async fn drain_through(builds: &dyn BuildStateAdapter, build_id: &BuildId) -> Result<(), rafka_node_admin_core::deployment::pipeline::PipelineError> {
    standalone_through(builds, build_id, &Drains, false).await
}

/// A standalone drain (or, with `stop`, a standalone stop) of a ready `mesh1.rpc.1` as attempt 1 of
/// `build_id`, through `builds`, its commands answered by `observer`.
async fn standalone_through(builds: &dyn BuildStateAdapter, build_id: &BuildId, observer: &dyn NodeObserver, stop: bool) -> Result<(), rafka_node_admin_core::deployment::pipeline::PipelineError> {
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
        store_dir: std::env::temp_dir().join(format!("i143-3051-refusal-{}", NodeId::mint())),
        mesh_issuer: None,
    };
    let joins = rafka_node_admin_core::join::Joins::default();
    let pipeline = DeploymentPipeline { provider: &Running, joins: &joins, observer, sink: &Discard, lifecycle: &NoLifecycleEvents, builds, template: &template, timeouts: Timeouts::default() };
    let handle = DeploymentHandle { deployment_id: rafka_node_admin_core::model::DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("i143-3051-refusal".into()) };
    let req = RetireRequest { build_id: build_id.clone(), attempt: 1, node, handle, kind: RetireKind::Removal };
    if stop {
        pipeline.stop(&req).await
    } else {
        pipeline.drain(&req).await
    }
}

/// Build `build_id` accepted and its attempt 1 claimed by `mesh1.admin.1`, in `builds`.
async fn accepted_and_claimed(builds: &dyn BuildStateAdapter, build_id: &BuildId) {
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
}

/// CONTRACT (node-rpc-envelope.md "Build carrier": "Progress waits on no gossip; Build-topic facts
/// remain durability and recovery evidence through the existing publication path"; R-ON8: Build-
/// topic gossip is for recovery only): a standalone drain run through the fabric's real Build state
/// whose gossip is shut down (every broadcast fails) completes: each step's receipt is written to the
/// admin's own log, the run goes on to `Complete`, and each failed broadcast is named under
/// `rdm.node_admin.build.reject.via-broadcast-refused` with the Build, attempt, step and the send
/// error. Must NOT happen: the step failed (a `via-receipt-refused` span, the run stopped at
/// `DrainNode`) because the broadcast after its durable local write failed.
#[tokio::test]
async fn a_step_whose_receipt_is_written_locally_completes_when_its_broadcast_fails() {
    let (capture, _guard) = Capture::new("i143-3051-broadcast-refused");
    let local = std::sync::Arc::new(MemoryBuildStateAdapter::new());
    let (_endpoint, gossip, _router, builds) = fabric_builds("mesh1.admin.1", local.clone()).await;
    let build_id = BuildId::mint();
    accepted_and_claimed(&builds, &build_id).await;
    gossip.shutdown().await.unwrap();

    if let Err(e) = drain_through(&builds, &build_id).await {
        panic!("every receipt was written to the local log and only its broadcast failed, yet the drain failed: {e:?}");
    }
    let steps: Vec<String> = local.read_build(&build_id).await.unwrap().steps.iter().map(|s| s.step.clone()).collect();
    assert!(steps.iter().any(|s| s == "DrainNode") && steps.last().map(String::as_str) == Some("Complete"), "every step's receipt is in the local log: {steps:?}");
    assert!(capture.spans("rdm.node_admin.deployment.reject.via-receipt-refused").is_empty(), "no step was refused for a failed broadcast");
    let refused = capture.spans("rdm.node_admin.build.reject.via-broadcast-refused");
    let drain = refused.iter().find(|(_, a)| a.get("step").map(String::as_str) == Some("DrainNode")).unwrap_or_else(|| panic!("the DrainNode receipt's failed broadcast is named: {refused:?}"));
    assert_eq!(drain.1.get("build_id"), Some(&build_id.to_string()), "{drain:?}");
    assert_eq!(drain.1.get("attempt").map(String::as_str), Some("1"), "{drain:?}");
    assert_eq!(drain.1.get("fact").map(String::as_str), Some("step"), "{drain:?}");
    assert!(drain.1.get("detail").is_some_and(|d| !d.is_empty()), "the send error is carried: {drain:?}");
    eprintln!("span rdm.node_admin.build.reject.via-broadcast-refused trace_id={} {:?}", drain.0, drain.1);
}

/// CONTRACT (node-recovery.md "Failure and receipts"): a standalone drain run through the fabric's
/// real Build state whose local log refuses the `DrainNode` receipt (a failed journal write) fails
/// at `DrainNode`, the error naming the step and the failed write, under
/// `rdm.node_admin.deployment.reject.via-receipt-refused`. Must NOT happen: the run going on as if
/// the step were committed, or the receipt broadcast with nothing written locally.
#[tokio::test]
async fn a_step_whose_local_receipt_write_fails_fails_by_name() {
    let (capture, _guard) = Capture::new("i143-3051-local-write-refused");
    let local = std::sync::Arc::new(Refuses { inner: MemoryBuildStateAdapter::new(), step: "DrainNode", local: true });
    let (_endpoint, _gossip, _router, builds) = fabric_builds("mesh1.admin.1", local.clone()).await;
    let build_id = BuildId::mint();
    accepted_and_claimed(&builds, &build_id).await;

    let err = match drain_through(&builds, &build_id).await {
        Err(e) => e,
        Ok(()) => panic!("the DrainNode receipt's local write failed and the drain still reported complete"),
    };
    assert_eq!(err.step, "DrainNode", "{err:?}");
    assert!(err.reason.contains("this cell's disk refuses it"), "the failed write is named in the error: {err:?}");
    assert!(local.inner.read_build(&build_id).await.unwrap().steps.is_empty(), "nothing after the refused step ran");
    let spans = capture.spans("rdm.node_admin.deployment.reject.via-receipt-refused");
    assert_eq!(spans.len(), 1, "one refusal span: {spans:?}");
    assert_eq!(spans[0].1.get("step").map(String::as_str), Some("DrainNode"), "{spans:?}");
    assert!(capture.spans("rdm.node_admin.build.reject.via-broadcast-refused").is_empty(), "nothing was broadcast for an unwritten receipt");
    eprintln!("span rdm.node_admin.deployment.reject.via-receipt-refused trace_id={} {:?}", spans[0].0, spans[0].1);
}

/// CONTRACT (node-rpc-envelope.md "Build carrier": "Progress waits on no gossip; Build-topic facts
/// remain durability and recovery evidence through the existing publication path"): a fabric-primary
/// whose seat record moved hands its committed facts on while its gossip is shut down: each send
/// that fails is named under `rdm.node_admin.build.reject.via-broadcast-refused` (fact `hand-over`,
/// sent by the handing admin, the send error), and the hand-over reports the facts it held. Must NOT
/// happen: the hand-over answered as failed because a broadcast failed.
#[tokio::test]
async fn a_seat_hand_over_whose_broadcast_fails_names_each_refused_send() {
    let (capture, _guard) = Capture::new("i143-3051-yield-broadcast-refused");
    let local = std::sync::Arc::new(MemoryBuildStateAdapter::new());
    let (_endpoint, gossip, _router, builds) = fabric_builds("mesh1.admin.1", local).await;
    let build_id = BuildId::mint();
    accepted_and_claimed(&builds, &build_id).await;
    gossip.shutdown().await.unwrap();

    let handed = builds.hand_over_facts("the seat is now mesh2 test", Some("mesh2.admin.1".into())).await;
    assert!(handed >= 2, "the accepted Build and its claim were held and handed on: {handed}");
    let refused = capture.spans("rdm.node_admin.build.reject.via-broadcast-refused");
    let hand = refused.iter().find(|(_, a)| a.get("fact").map(String::as_str) == Some("hand-over")).unwrap_or_else(|| panic!("the hand-over's failed send is named: {refused:?}"));
    assert_eq!(hand.1.get("sent_by").map(String::as_str), Some("mesh1.admin.1"), "{hand:?}");
    assert!(hand.1.get("detail").is_some_and(|d| d.contains("hand")), "the send error is carried: {hand:?}");
    eprintln!("span rdm.node_admin.build.reject.via-broadcast-refused trace_id={} {:?}", hand.0, hand.1);
}
