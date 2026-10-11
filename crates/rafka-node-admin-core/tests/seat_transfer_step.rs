//! The SeatTransfer step's outcome decides whether the drain goes on (node-drain.md step 0;
//! node-rpc-envelope.md "TakeSeats: Build-driven seat transfer"): a target holding a seat is
//! drained by its same-mesh successor only once the holder committed its seats. TakeSeats moves
//! seats inside the holder's mesh only, so an executor of another mesh cannot take them: the step
//! fails by name and nothing is drained. A holder's refusal fails the step the same way. The
//! pipeline is the product's (`DeploymentPipeline::drain`); the provider, the observer and the
//! Build state are fixtures.

use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildAccepted, BuildFact, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::pipeline::{CommandAdmission, CommandContext, DeploymentPipeline, LaunchTemplate, NodeObserver, NoLifecycleEvents, Publication, RetireRequest, RetireStep, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::fabric_handover::SeatTransferOutcome;
use rafka_node_admin_core::join::Joins;
use rafka_node_admin_core::model::{DeploymentId, Node, NodeStatus, PathName, ProviderKind};
use rafka_node_admin_core::node_commands::NodeCommand;
use std::collections::BTreeMap;
use std::sync::Mutex;

const DOMAIN: &str = "seat-transfer-step";
const HOLDER: &str = "mesh1.admin.1";

/// The holder's runtime: running, never stopped here.
struct Running;

#[async_trait::async_trait]
impl DeploymentProvider for Running {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, _: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        unreachable!("a drain launches nothing")
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        unreachable!("a drain terminates nothing")
    }
    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        DeploymentStatus::Running
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        unreachable!("a drain stops nothing")
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        None
    }
}

/// The executor's observer: its SeatTransfer answers `transfer`; every command it is asked to send
/// is recorded and never sent.
struct Executor {
    transfer: Result<SeatTransferOutcome, String>,
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
    async fn transfer_seats(&self, _: &Node, _: &CommandContext) -> Result<SeatTransferOutcome, String> {
        self.transfer.clone()
    }
    async fn send_command(&self, node: &Node, cmd: NodeCommand, _: &CommandContext) -> CommandAdmission {
        self.sent.lock().unwrap().push(cmd);
        CommandAdmission::NotSent { reason: format!("this cell sends {} nothing", node.name) }
    }
}

struct NoView;
impl TopologySink for NoView {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, _: &PathName, _: &NodeId, _: &IncarnationId) {}
}

fn holder() -> Node {
    let mut n = Node::allocated(HOLDER.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = true;
    n.is_fabric_primary = true;
    n
}

/// Drain the seat-holding admin with `transfer` as its SeatTransfer answer: the pipeline's result,
/// the commands the observer was asked to send, the Build's step receipts, and the cell's trace id.
fn drain_with(transfer: Result<SeatTransferOutcome, String>) -> (Result<(), rafka_node_admin_core::deployment::pipeline::PipelineError>, Vec<NodeCommand>, Vec<(String, StepOutcome)>, String) {
    run_with(transfer, NodeCommand::Drain)
}

/// [`drain_with`] for the standalone workflow `workflow` (a drain or a stop).
fn run_with(transfer: Result<SeatTransferOutcome, String>, workflow: NodeCommand) -> (Result<(), rafka_node_admin_core::deployment::pipeline::PipelineError>, Vec<NodeCommand>, Vec<(String, StepOutcome)>, String) {
    let cap = crate::mesh_leave_resume::capture();
    let observer = Executor { transfer, sent: Mutex::new(Vec::new()) };
    let (r, steps, trace_id) = cap.run(async {
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let joins = Joins::default();
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rafka-node-admin".into(),
            seeds: vec![("executorkey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rafka_mesh_entity::launch::Launcher { name: "mesh2.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: std::env::temp_dir().join(format!("seat-transfer-step-{}", NodeId::mint())),
            store_dir: std::env::temp_dir().join(format!("seat-transfer-step-{}", NodeId::mint())),
            mesh_issuer: None,
        };
        let pipeline = DeploymentPipeline { provider: &Running, joins: &joins, observer: &observer, sink: &NoView, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let handle = DeploymentHandle { deployment_id: DeploymentId(format!("dep-{HOLDER}")), provider: ProviderKind::Process, pid: Some(42_001), start: Some(1), container: None, domain: Some(DOMAIN.into()) };
        let root = tracing::info_span!("cell.seat-transfer-step", build_id = %build_id, node = HOLDER);
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        let r = {
            use tracing::Instrument;
            let req = RetireRequest { build_id: build_id.clone(), attempt: 1, node: holder(), handle };
            match workflow {
                NodeCommand::Stop => pipeline.stop(&req).instrument(root).await,
                _ => pipeline.drain(&req).instrument(root).await,
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

/// CONTRACT: a refused seat transfer fails the SeatTransfer step by name and the drain never starts:
/// no drain-node is sent and no DrainNode step is receipted (a fabric seat an executor of another
/// mesh asks for is refused so by `take_seats`). A target that holds no seat, or holds only its own
/// mesh's seat with no other admin in that mesh (`OtherMesh`), transfers nothing and the drain goes on.
#[test]
fn a_refused_seat_transfer_fails_the_step_by_name_and_drains_nothing() {
    let seat_transfer = RetireStep::SeatTransfer.name();
    let failed = |steps: &[(String, StepOutcome)]| steps.iter().find(|(s, _)| s == seat_transfer).and_then(|(_, o)| match o {
        StepOutcome::Failed { reason } => Some(reason.clone()),
        _ => None,
    });

    let refusal = "mesh2.admin.1 cannot take the fabric seat of mesh1.admin.1: TakeSeats moves seats inside the holder's mesh (mesh1) only";
    let (r, sent, steps, _) = drain_with(Err(refusal.into()));
    let e = r.expect_err("a refused TakeSeats ends the drain");
    assert_eq!((e.step, e.reason.as_str()), (seat_transfer, refusal), "{e}");
    assert_eq!(failed(&steps).as_deref(), Some(refusal), "the SeatTransfer receipt is Failed with the same reason: {steps:?}");
    assert!(sent.is_empty(), "no drain-node is sent: {sent:?}");
    assert!(!steps.iter().any(|(s, _)| s == RetireStep::DrainNode.name()), "no DrainNode step: {steps:?}");

    for outcome in [SeatTransferOutcome::NotAHolder, SeatTransferOutcome::OtherMesh { holder_mesh: "mesh1".into() }] {
        let (_, sent, steps, _) = drain_with(Ok(outcome.clone()));
        assert!(steps.iter().any(|(s, o)| s == seat_transfer && *o == StepOutcome::Complete), "{outcome:?}: {steps:?}");
        assert_eq!(sent, vec![NodeCommand::Drain], "{outcome:?}: the drain goes on");
    }
}

/// CONTRACT (node-drain.md "Admission is not drain completion"; node-stop.md, the complete stop): a
/// standalone drain or stop whose command never reached the birth (`NotSent`) established nothing:
/// its wait step fails by name with the admission it got, and no `Complete` receipt names the birth.
#[test]
fn a_standalone_drain_or_stop_whose_command_was_never_admitted_fails_by_name_and_completes_nothing() {
    for (workflow, command, wait) in [(NodeCommand::Drain, "drain-node", RetireStep::AwaitNodeDrained.name()), (NodeCommand::Stop, "stop-node", RetireStep::AwaitNodeLeft.name())] {
        let (r, sent, steps, _) = run_with(Ok(SeatTransferOutcome::NotAHolder), workflow);
        assert_eq!(sent, vec![workflow], "the {command} was asked for");
        let e = r.expect_err("a command that never reached the birth established nothing");
        assert_eq!(e.step, wait, "{e}");
        assert!(e.reason.contains(HOLDER) && e.reason.contains(command) && e.reason.contains("NotSent"), "the failure names the birth, the command and the admission it got: {e}");
        assert!(!steps.iter().any(|(s, _)| s == "Complete"), "no Complete receipt: {steps:?}");
    }
}
