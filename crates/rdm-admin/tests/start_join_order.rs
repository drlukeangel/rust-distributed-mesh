//! node.start of a stopped birth (node-start.md): the order the executor takes the start's
//! `node.joined` and the birth's `node.started` in. The cell drives the start pipeline over a
//! recording provider, observer and lifecycle: nothing here depends on timing.

use rdm_core::{IncarnationId, LifecycleOp, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::deployment::pipeline::{
    CommandAdmission, CommandContext, Completion, DeploymentPipeline, LaunchTemplate, LifecycleEvents, NodeObserver, Publication, StartRequest, Timeouts, TopologySink,
};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::{DeploymentId, EndpointId, Node, NodeStatus, PathName, ProviderKind};
use rdm_admin::node_commands::NodeCommand;
use std::sync::Mutex;
use std::time::Duration;

const NODE: &str = "mesh1.rpc.1";

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
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.to_string(), reason: "a start deploys nothing".into() })
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
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// Every lifecycle event the start publishes, with the operation its op carries.
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
/// inside its start): on `start-node` it joins its commander (the join door admits its digest), then
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
    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let name: PathName = NODE.parse().unwrap();
    let incarnation = IncarnationId::mint();
    let mut node = Node::allocated(name.clone());
    node.node_id = NodeId::mint();
    node.incarnation_id = Some(incarnation.clone());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::Leaving;
    node.stopped = true;
    node.endpoint_id = Some(EndpointId("i143-s52-endpoint".into()));
    node.data_dir = Some("/nonexistent/i143-s52/mesh1.rpc.1".into());
    let template = LaunchTemplate { fabric: "fabric1".into(),
        fabric_id: rdm_core::FabricId::mint(),
        executable: "/nonexistent/start-order".into(),
        seeds: vec![],
        launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
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
    let deployed = rdm_admin::join::Deployed {
        name: name.clone(),
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
    let digest = rdm_core::MeshDigest { held_hash: Default::default(),
        fabric_id: template.fabric_id.clone(),
        node: rdm_core::MeshNode { node_id: node.node_id.clone(), name: name.clone(), endpoint_id: node.endpoint_id.clone().unwrap(), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: incarnation.clone(), supersedes: None, runtime: Some(runtime) },
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
    let out = pipeline.start(&StartRequest { build_id: build_id.clone(), attempt: 1, node, handle, deployed }).await;
    assert!(out.is_ok(), "{out:?}");
    let published = events.published.lock().unwrap().clone();
    let order: Vec<&str> = published.iter().map(|(e, _)| *e).filter(|e| matches!(*e, "joined" | "node.started")).collect();
    assert_eq!(order, ["joined", "node.started"], "node.joined is taken at the admitted join, before the birth's node.started: {published:?}");
}
