//! node.start of a stopped birth (node-start.md): the order the executor takes the start's
//! `node.joined` and the birth's `node.started` in. The cell drives the start pipeline over a
//! recording provider, observer and lifecycle: nothing here depends on timing.

use rdm_core::{IncarnationId, LifecycleOp, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::deployment::pipeline::{
    CommandAdmission, CommandContext, Completion, DeploymentPipeline, LaunchTemplate, LifecycleEvents, NodeObserver, Publication, StartRequest, Timeouts, TopologySink,
};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::{DeploymentId, EndpointId, Node, NodeStatus, PathName, ProviderKind};
use rdm_admin::node_commands::NodeCommand;
use std::sync::Mutex;
use std::time::Duration;



/// The provider: the runtime runs throughout; a terminate is recorded (a start asks none).
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
        "start-join-order".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { path: spec.path.clone(), name: spec.name.clone(), reason: "a start deploys nothing".into() })
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        self.terminated.lock().unwrap().push(h.deployment_id.to_string());
        Ok(())
    }
    async fn inspect(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        DeploymentStatus::Running
    }
    async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// Every lifecycle event the start publishes, with the operation its op carries.
#[derive(Default)]
struct Events {
    published: Mutex<Vec<(&'static str, rdm_core::OperationId)>>,
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
    async fn stopped(&self, op: &LifecycleOp) {
        self.note("stopped", op);
    }
    async fn joined(&self, op: &LifecycleOp) {
        self.note("joined", op);
    }
    async fn leaving(&self, op: &LifecycleOp) {
        self.note("leaving", op);
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

/// The stopped birth whose start runs its whole sequence before it answers (a node-admin's hook runs
/// inside its start): on `start-node` it joins its commander (the `JoinService` admits its digest), then
/// reports node.started, and only then answers `Started`.
struct StartsWhole<'a> {
    joins: &'a rdm_admin::join::Joins,
    events: &'a Events,
    digest: rdm_core::MeshDigest,
}

#[async_trait::async_trait]
impl NodeObserver for StartsWhole<'_> {
    async fn send_command(&self, _: &Node, cmd: NodeCommand, ctx: &CommandContext) -> CommandAdmission {
        match cmd {
            NodeCommand::Start => {
                // This observer models the command-stream owner: NodeJoined is recorded
                // after the admitted join, before NodeStarted. The pipeline records neither.
                self.joins.report(&rdm_admin::join::JoinReport { digest: self.digest.clone(), listeners: Vec::new() });
                self.events.published.lock().unwrap().push(("joined", ctx.operation.clone()));
                tokio::task::yield_now().await;
                self.events.published.lock().unwrap().push(("node.started", ctx.operation.clone()));
                CommandAdmission::Started {}
            }
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

/// CONTRACT (node-start.md "End-to-end sequence": the start emits `node.joined` after the accepted
/// join, then runs its hook, topology, Build floor and rafka-time, then emits `node.started`; states.md
/// "Who sets each state": node pending from node.joined, node Ready from node.started): the
/// executor takes a start's node.joined when the birth's join is admitted, before the birth can report
/// node.started, whatever moment the birth's `Started` answer reaches it. A birth that runs its whole
/// start before it answers (a node-admin's hook runs inside its start) has node.joined taken first and
/// node.started after it. Must NOT happen: node.joined taken at the `Started` answer, after the birth's
/// node.started, so that the Pending node.joined sets stands over the Ready node.started set.
#[tokio::test]
async fn a_starts_node_joined_is_taken_at_its_admitted_join_before_its_node_started() {
    let executor_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let builds = crate::stepping(&rdm_core::NodeId::mint(), "mesh1");
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 })
        .await
        .unwrap();
    let name: PathName = PathName::new("mesh1.node", "1").unwrap();
    let incarnation = IncarnationId::mint();
    let mut node = Node::allocated(name.clone(), "mesh1", "node");
    node.node_id = NodeId::mint();
    node.incarnation_id = Some(incarnation.clone());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::Leaving;
    node.stopped = true;
    node.endpoint_id = Some(EndpointId("i143-s52-endpoint".into()));
    node.data_dir = Some("/nonexistent/i143-s52/mesh1.node.1".into());
    let template = LaunchTemplate { fabric: "fabric1".into(),
        fabric_id: rdm_core::FabricId::mint(),
        executable: "/nonexistent/start-order".into(),
        seeds: vec![],
        launcher: rdm_core::launch::Launcher { mesh: "mesh1".into(), path: "mesh1.node-admin".into(), name: "1".into(), node_id: executor_birth.node_id.clone(), incarnation: executor_birth.incarnation.clone() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-s52-start-order-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-s52-start-order-store-{}", NodeId::mint())),
        issuing: None,
        roots: Default::default(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
        binding: None,
    };
    let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(4242), start: Some(1), container: None, domain: Some("i143-s52-start-order".into()) };
    let runtime = handle.fact().expect("the handle names its exact runtime");
    let deployed = rdm_admin::join::Deployed { mesh: node.mesh.clone(), r#type: node.r#type.clone(),
        path: name.path.clone(), name: name.name.clone(),
        node_id: node.node_id.clone(),
        incarnation: incarnation.clone(),
        supersedes: None,
        endpoint_id: node.endpoint_id.clone().unwrap(),
        runtime: runtime.clone(),
        data_dir: node.data_dir.clone().unwrap(),
        last_accepted_name: Some(name.clone()),
        held: true,
        listeners: Vec::new(),
    };
    let digest = rdm_core::MeshDigest { 
        fabric_id: template.fabric_id.clone(),
        node: rdm_core::MeshNode { mesh: node.mesh.clone(), r#type: node.r#type.clone(), node_id: node.node_id.clone(), path: name.path.clone(), name: name.name.clone(), endpoint_id: node.endpoint_id.clone().unwrap(), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: incarnation.clone(), supersedes: None, runtime: Some(runtime) },
        status: rdm_core::MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 9,
        emitted_at_rafka_ms: 9,
        data_dir: node.data_dir.clone(),
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    };
    let (provider, events, joins) = (Running::default(), Events::default(), rdm_admin::join::Joins::default());
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
    let operation = rdm_core::OperationId::node("node.start", &node.node_id);
    let out = pipeline.start(&StartRequest { operation: rdm_core::OperationId::node("node.start", &(node).node_id), node, handle, deployed }).await;
    assert!(out.is_ok(), "{out:?}");
    let published = events.published.lock().unwrap().clone();
    assert!(published.iter().all(|(_, recorded)| recorded == &operation), "each event retains the admitted node ID: {published:?}");
    let order: Vec<&str> = published.iter().map(|(e, _)| *e).filter(|e| matches!(*e, "joined" | "node.started")).collect();
    assert_eq!(order, ["joined", "node.started"], "node.joined is taken at the admitted join, before the birth's node.started: {published:?}");
}

/// CONTRACT (docs/architecture/lifecycles/node-start.md:22-23, 29-32, 61-66, 88-96, 121-122, 126-127):
/// "1. A stopped process remains the same exact runtime and keeps its endpoint/port; it does not start a
/// new process or load a new executable."
/// "node.start clears NodeStopped at NodeJoined, or, for an executor whose replayed start of a birth
/// its view holds stopped is answered `AlreadyApplied`, at that answer: the join it did not see ...
/// 5. Emit `NodeStarted` (Terminal): its hook passed, with nothing pending. Neither a partial snapshot
/// nor a Blocked hook permits it. That frame is the node's report: the mesh-primary sets Ready on that
/// exact birth through the node status apply, not a caller op. The node also declares Ready to its
/// mesh-primary as an owed message, as it declares Stopped (Q36, Q87); frame and declaration apply once
/// (Q49). A typed refusal of that apply completes no readiness."
/// "If join fails after start: CommandAccepted / Failed { step: \"node.join\", reason: \"...\", code } (Terminal).
/// No node.joined, node.started or downstream success follows that failure."
/// "Failures and stream certainty follow the caller execution contract."
/// "Trace propagation, step spans and correlation follow the runtime spans."
/// Injects one fault per step of `node.start` (lost reply at `StartNode`, join failure at `StartNode`,
/// a cut at `WaitForNodeReady` while the birth is suspended) and asserts the page's failure or recovery
/// outcome on its step spans. node.start is a node command, never a Build step (inbox 0008: a node
/// command keys on its node.id and operation): each call runs every step, so the call after the cut
/// sends `start-node` again, and the birth answers that replayed start `AlreadyAdmitted`; the start
/// completes with the same exact runtime, nothing terminated or spawned.
#[test]
fn node_start_step_faults_recover_or_fail_as_the_page_specifies() {
    use rdm_admin::deployment::pipeline::{CreateStep, StartStep};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing_subscriber::layer::SubscriberExt as _;

    struct FaultStartObserver {
        admissions: Mutex<std::collections::VecDeque<CommandAdmission>>,
        send_calls: AtomicUsize,
        ready_err: Mutex<Option<String>>,
        ready_entered: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl NodeObserver for FaultStartObserver {
        async fn send_command(&self, _: &Node, cmd: NodeCommand, _: &CommandContext) -> CommandAdmission {
            assert_eq!(cmd, NodeCommand::Start);
            self.send_calls.fetch_add(1, Ordering::SeqCst);
            self.admissions.lock().unwrap().pop_front().unwrap_or(CommandAdmission::Started {})
        }
        async fn await_completion(&self, _: &Node, _: NodeCommand, _: &CommandContext, _: Duration) -> Completion {
            Completion::Received {}
        }
        async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
            None
        }
        async fn ready(&self, _: &Node) -> Result<(), String> {
            self.ready_entered.notify_waiters();
            match self.ready_err.lock().unwrap().clone() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }
    }

    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("node_start_step_faults", exporter.clone());
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    rt.block_on(async {
        let executor_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let builds = crate::stepping(&rdm_core::NodeId::mint(), "mesh1");

        let name: PathName = PathName::new("mesh1.node", "1").unwrap();
        let incarnation = IncarnationId::mint();
        let mut node = Node::allocated(name.clone(), "mesh1", "node");
        node.node_id = NodeId::mint();
        node.incarnation_id = Some(incarnation.clone());
        node.provider = Some(ProviderKind::Process);
        node.status = NodeStatus::Leaving;
        node.stopped = true;
        node.endpoint_id = Some(EndpointId("start-fault-endpoint".into()));
        node.data_dir = Some("/nonexistent/start-fault/mesh1.node.1".into());

        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: rdm_core::FabricId::mint(),
            executable: "/nonexistent/start-fault".into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { mesh: "mesh1".into(), path: "mesh1.node-admin".into(), name: "1".into(), node_id: executor_birth.node_id.clone(), incarnation: executor_birth.incarnation.clone() },
            env: Default::default(),
            data_root: std::env::temp_dir().join(format!("start-fault-{}", NodeId::mint())),
            store_dir: std::env::temp_dir().join(format!("start-fault-store-{}", NodeId::mint())),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let handle = DeploymentHandle {
            deployment_id: DeploymentId::mint(),
            provider: ProviderKind::Process,
            pid: Some(4242),
            start: Some(1),
            container: None,
            domain: Some("start-fault-domain".into()),
        };
        let runtime = handle.fact().expect("exact runtime");
        let deployed = rdm_admin::join::Deployed { mesh: node.mesh.clone(), r#type: node.r#type.clone(),
            path: name.path.clone(), name: name.name.clone(),
            node_id: node.node_id.clone(),
            incarnation: incarnation.clone(),
            supersedes: None,
            endpoint_id: node.endpoint_id.clone().unwrap(),
            runtime,
            data_dir: node.data_dir.clone().unwrap(),
            last_accepted_name: Some(name.clone()),
            held: true,
            listeners: Vec::new(),
        };

        let (provider, events, joins) = (Running::default(), Events::default(), rdm_admin::join::Joins::default());
        let observer = FaultStartObserver {
            admissions: Mutex::new(std::collections::VecDeque::from([
                // Call 1: lost reply at StartNode.
                CommandAdmission::Indeterminate {
                    reason: "stream reset before NodeStarted".into(),
                    code: rdm_core::outcome::Code::Unavailable,
                },
                // Call 2: join fails after start.
                CommandAdmission::StartFailed {
                    step: "node.join".into(),
                    reason: "join service cut".into(),
                    code: rdm_core::outcome::Code::Unavailable,
                },
                // Call 3: StartNode succeeds, then cut while suspended at WaitForNodeReady.
                CommandAdmission::Started {},
                // Call 4: the replayed start-node, answered AlreadyAdmitted.
                CommandAdmission::AlreadyAdmitted {},
            ])),
            send_calls: AtomicUsize::new(0),
            ready_err: Mutex::new(None),
            ready_entered: tokio::sync::Notify::new(),
        };

        let pipeline = DeploymentPipeline {
            provider: &provider,
            joins: &joins,
            observer: &observer,
            sink: &Discard,
            lifecycle: &events,
            builds: &builds,
            template: &template,
            timeouts: Timeouts {
                completion: Duration::from_millis(80),
                stop_grace: Duration::from_millis(80),
                ready: Duration::from_millis(80),
                ..Timeouts::default()
            },
        };

        // 1. Call 1: lost reply at `StartNode` -> fails at `StartNode` with no downstream steps.
        let req = || StartRequest { operation: rdm_core::OperationId::node("node.start", &(node).node_id), node: node.clone(), handle: handle.clone(), deployed: deployed.clone() };
        let err1 = pipeline.start(&req()).await.expect_err("lost reply at StartNode must fail");
        assert_eq!(err1.step, StartStep::StartNode.name());
        assert_eq!(err1.code, rdm_core::outcome::Code::Unavailable);
        assert!(err1.reason.contains("start-node was not answered Started"), "unexpected reason: {}", err1.reason);

        // 2. Call 2: join failure after `CommandAccepted` -> fails at `StartNode` naming `node.join`;
        // nothing past it runs.
        let err2 = pipeline.start(&req()).await.expect_err("join failure at StartNode must fail");
        assert_eq!(err2.step, StartStep::StartNode.name());
        assert_eq!(err2.code, rdm_core::outcome::Code::Unavailable);
        assert!(err2.reason.contains("node.start failed at node.join"), "unexpected reason: {}", err2.reason);

        // 3. Call 3: `StartNode` succeeds (`Started`), then the birth is suspended (`os-stopped`) and
        // the call is cut at `WaitForNodeReady`.
        *observer.ready_err.lock().unwrap() = Some("birth is os-stopped before readiness".into());
        let req3 = req();
        tokio::select! {
            res = pipeline.start(&req3) => panic!("call 3 should be cut while suspended at WaitForNodeReady, got {res:?}"),
            _ = observer.ready_entered.notified() => {}
        }
        assert_eq!(observer.send_calls.load(Ordering::SeqCst), 3);

        // 4. Call 4: the birth is resumed; the call sends `start-node` again, the birth answers the
        // replayed start `AlreadyAdmitted`, and `WaitForNodeReady` and `Complete` follow.
        *observer.ready_err.lock().unwrap() = None;
        pipeline.start(&req()).await.expect("the replayed start answered AlreadyAdmitted completes once resumed");
        assert_eq!(observer.send_calls.load(Ordering::SeqCst), 4, "a node command holds no receipt: the call after the cut sends start-node again");
        assert!(
            provider.terminated.lock().unwrap().is_empty(),
            "node.start keeps the same exact runtime and terminates or spawns nothing"
        );
    });

    drop(rt);
    capture.flush(Duration::from_secs(10)).unwrap();
    let step_spans: Vec<BTreeMap<String, String>> = exporter
        .get_finished_spans()
        .unwrap()
        .into_iter()
        .filter(|s| s.name == "rdm.node_admin.deployment.update.via-step")
        .map(|s| s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect())
        .collect();
    let of = |step: &str, outcome: &str| step_spans.iter().filter(|s| s.get("step").map(String::as_str) == Some(step) && s.get("outcome").map(String::as_str) == Some(outcome)).count();
    assert_eq!(of(StartStep::StartNode.name(), "failed"), 2, "calls 1 and 2 fail at StartNode: {step_spans:?}");
    assert_eq!(of(StartStep::StartNode.name(), "complete"), 2, "calls 3 and 4 complete StartNode: {step_spans:?}");
    assert_eq!(of(CreateStep::WaitForNodeReady.name(), "cancelled"), 1, "call 3 is cut at WaitForNodeReady: {step_spans:?}");
    assert_eq!(of(CreateStep::WaitForNodeReady.name(), "complete"), 1, "call 4 completes WaitForNodeReady: {step_spans:?}");
}
