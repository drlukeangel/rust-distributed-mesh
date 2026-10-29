//! A standalone node.connection.delete or stop of a seat-holding node-admin, run by the
//! product's pipeline (`DeploymentPipeline`): the pipeline moves no seat (R-MT1: the fabric-primary's
//! drive moves it by node.primary.update before the operation is dispatched), and a command the birth
//! never admitted completes nothing. The provider, the observer and the Build state are fixtures.

use rdm_core::{FabricId, IncarnationId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildFact, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::deployment::pipeline::{CommandAdmission, CommandContext, DeploymentPipeline, LaunchTemplate, NodeObserver, NoLifecycleEvents, Publication, DeleteRequest, DeleteStep, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::join::Joins;
use rdm_admin::model::{DeploymentId, Node, NodeStatus, PathName, ProviderKind};
use rdm_admin::node_commands::NodeCommand;
use std::collections::BTreeMap;
use std::sync::Mutex;

const DOMAIN: &str = "seat-transfer-step";
const HOLDER: &str = "mesh1.admin.1";

/// The holder's runtime: running, never stopped here; nothing is launched.
struct Running;

#[async_trait::async_trait]
impl DeploymentProvider for Running {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, launch: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        // A node.connection.delete or a stop launches nothing: refused here so the cell's assertion names it.
        Err(DeployError::Spawn { node: format!("{launch:?}").chars().take(80).collect(), reason: "this cell launches nothing".into() })
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        Err(DeployError::Terminate { deployment: h.deployment_id.to_string(), reason: "this cell terminates nothing".into() })
    }
    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        DeploymentStatus::Running
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        unreachable!("a node.connection.delete stops nothing")
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// The executor's observer: every command it is asked to send is recorded and never sent.
struct Executor {
    sent: Mutex<Vec<NodeCommand>>,
}

#[async_trait::async_trait]
impl NodeObserver for Executor {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
    async fn send_command(&self, node: &Node, cmd: NodeCommand, _: &CommandContext) -> CommandAdmission {
        self.sent.lock().unwrap().push(cmd);
        CommandAdmission::NotSent { reason: format!("this cell sends {} nothing", node.name) , code: rdm_core::outcome::Code::FailedPrecondition }
    }
}

/// The executor's view: publishes nothing; records every predecessor that gave up its path.name.
#[derive(Default)]
struct NoView;
impl TopologySink for NoView {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, _: &PathName, _: &NodeId, _: &IncarnationId) {}
}

fn primary() -> Node {
    let mut n = Node::allocated(HOLDER.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = true;
    n.is_fabric_primary = true;
    n
}

/// The standalone workflow `workflow` (a node.connection.delete or a stop) of the seat-holding admin:
/// the pipeline's result, the commands the observer was asked to send, the Build's step receipts and
/// the trace id.
fn run_with(workflow: NodeCommand) -> (Result<(), rdm_admin::deployment::pipeline::PipelineError>, Vec<NodeCommand>, Vec<(String, StepOutcome)>, String) {
    run_workflow(workflow)
}

/// Run `workflow` on the primary (a node.connection.delete or a stop): the result, the
/// commands sent, the step receipts and the trace id.
fn run_workflow(workflow: NodeCommand) -> (Result<(), rdm_admin::deployment::pipeline::PipelineError>, Vec<NodeCommand>, Vec<(String, StepOutcome)>, String) {
    let cap = crate::mesh_delete_resume::capture();
    let observer = Executor { sent: Mutex::new(Vec::new()) };
    let view = NoView;
    let (r, steps, trace_id) = cap.run(async {
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rdm-node-admin".into(),
            seeds: vec![("executorkey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: "mesh2.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: std::env::temp_dir().join(format!("seat-transfer-step-{}", NodeId::mint())),
            store_dir: std::env::temp_dir().join(format!("seat-transfer-step-{}", NodeId::mint())),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let pipeline = DeploymentPipeline { provider: &Running, joins: &joins, observer: &observer, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let handle = DeploymentHandle { deployment_id: DeploymentId::parse(&rdm_core::fixture_crockford60(&format!("dep-{HOLDER}"))).unwrap(), provider: ProviderKind::Process, pid: Some(42_001), start: Some(1), container: None, domain: Some(DOMAIN.into()) };
        let root = tracing::info_span!("cell.seat-transfer-step", build_id = %build_id.log(), node = HOLDER);
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let r = {
            use tracing::Instrument;
            let req = DeleteRequest { build_id: build_id.clone(), attempt: 1, node: primary(), handle, unreached: false };
            match workflow {
                NodeCommand::Stop => pipeline.stop(&req).instrument(root).await,
                _ => pipeline.delete_connections(&req).instrument(root).await,
            }
        };
        let steps = builds
            .facts()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|f| match f {
                BuildFact::Step(s) if s.build_id == build_id => Some((s.step, s.outcome)),
                _ => None,
            })
            .collect::<Vec<_>>();
        (r, steps, trace_id)
    });
    let names: Vec<String> = cap.spans().iter().filter(|s| s["trace_id"] == trace_id.as_str()).map(|s| s["name"].as_str().unwrap_or("").to_string()).collect();
    crate::assert_own_story(names.iter().map(String::as_str).filter(|n| *n != "cell.seat-transfer-step"));
    println!("trace_id={trace_id} spans={names:?}");
    (r, observer.sent.into_inner().unwrap(), steps, trace_id)
}

/// CONTRACT (lifecycles/node-connections-delete.md "Admission is not connections delete completion"; node-stop.md, the complete stop): a
/// standalone connections delete or stop whose command never reached the birth (`NotSent`) established nothing:
/// its wait step fails by name with the admission it got, and no `Complete` receipt names the birth.
#[test]
fn a_standalone_connections_delete_or_stop_whose_command_was_never_admitted_fails_by_name_and_completes_nothing() {
    for (workflow, command, wait) in [(NodeCommand::DeleteConnections, "node.connection.delete", DeleteStep::DeleteConnections.name()), (NodeCommand::Stop, "stop-node", DeleteStep::AwaitNodeLeft.name())] {
        let (r, sent, steps, _) = run_with(workflow);
        assert_eq!(sent, vec![workflow], "the {command} was asked for");
        let e = r.expect_err("a command that never reached the birth established nothing");
        assert_eq!(e.step, wait, "{e}");
        assert!(e.reason.contains(HOLDER) && e.reason.contains(command) && e.reason.contains("NotSent"), "the failure names the birth, the command and the admission it got: {e}");
        assert!(!steps.iter().any(|(s, _)| s == "Complete"), "no Complete receipt: {steps:?}");
        assert_eq!(steps.first().map(|(s, _)| s.as_str()), Some(if workflow == NodeCommand::DeleteConnections { DeleteStep::DeleteConnections.name() } else { DeleteStep::StopNode.name() }), "the pipeline moves no seat: its first step is the command's (R-MT1): {steps:?}");
    }
}
