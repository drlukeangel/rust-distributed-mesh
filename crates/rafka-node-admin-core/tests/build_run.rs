//! The executor's run of a claimed attempt is a turn of its admin's park gate (park_gate.rs: a task
//! that takes the view lock begins each turn at its own wait point; a stop's quiesce waits for every
//! turn already past it). A run the admin serves for a remote fabric-primary (`build.attempt.run`,
//! op `0x20`) is its own task: it begins at its wait point and the stop's quiesce waits for it, so
//! nothing it does follows the stop's cut (node-stop.md steps 3-4, R-ON1). A stopped admin keeps
//! answering the call, and the run it starts waits at the gate until the admin starts.

use rafka_node_admin_core::accepted::FabricTopology;
use rafka_node_admin_core::build::{BuildId, BuildOperation};
use rafka_node_admin_core::build_run::{AttemptRuns, Attached, FramedBuilds, RunDoor};
use rafka_node_admin_core::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::executor::{BuildExecutor, OperationRunner, RunEnd, RunPlan};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::park_gate::ParkGate;
use rafka_node_rpc_contract::build::{BuildReply, Disposition};
use rafka_node_rpc_contract::context::CallContext;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::Notify;
use tracing_subscriber::layer::SubscriberExt;

/// The cell's own span capture: the product span pipeline in front of an in-memory exporter.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    pipeline: rafka_mesh_telemetry::SpanCapture,
}

impl Capture {
    fn spans(&self) -> Vec<Value> {
        self.pipeline.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let ns = |t: std::time::SystemTime| t.duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({ "name": s.name, "trace_id": s.span_context.trace_id().to_string(), "end_ns": ns(s.end_time), "attributes": attributes })
            })
            .collect()
    }
}

fn capture(service: &str) -> (Capture, tracing::subscriber::DefaultGuard) {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture(service, exporter.clone());
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    (Capture { exporter, pipeline }, guard)
}

fn build_id() -> BuildId {
    BuildId::parse("vhpqxdbt5w2g").unwrap()
}

/// What the runner did, in order, beside the instants the cell marks (the stop's cut).
#[derive(Default)]
struct Calls {
    log: Mutex<Vec<String>>,
    /// The first operation waits here once it has been entered, while `hold_first` is set.
    entered: Notify,
    release: Notify,
}

impl Calls {
    fn push(&self, what: String) {
        self.log.lock().unwrap().push(what);
    }
    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

/// The executor's runner: each operation is a call out of the admin, recorded with whether the
/// admin's gate was parked when it was made. The first operation holds until the cell releases it
/// when `hold_first` is set.
struct CallingOut {
    calls: Arc<Calls>,
    gate: ParkGate,
    hold_first: bool,
}

#[async_trait::async_trait]
impl OperationRunner for CallingOut {
    async fn run(&self, _: &BuildId, _: Option<&str>, _: u32, op: &BuildOperation) -> Result<(), RunEnd> {
        let first = self.calls.log().iter().all(|c| !c.starts_with("call "));
        self.calls.push(format!("call {}{}", op.key(), if self.gate.is_parked() { " (gate parked)" } else { "" }));
        if first && self.hold_first {
            self.calls.entered.notify_one();
            self.calls.release.notified().await;
        }
        Ok(())
    }
}

/// `mesh1.admin.1`'s executor door with its own gate, attempt 1 of the cell's Build claimed for it by
/// a fabric-primary elsewhere, and a run of two operations it plans.
async fn executor(calls: &Arc<Calls>, hold_first: bool) -> (RunDoor, ParkGate, RunPlan) {
    let memory = Arc::new(MemoryBuildStateAdapter::new());
    let runs = Arc::new(AttemptRuns::default());
    let builds: Arc<dyn BuildStateAdapter> = Arc::new(FramedBuilds::new(memory.clone(), runs.clone()));
    let mut admin = Node::allocated("mesh1.admin.1".parse().unwrap());
    admin.status = NodeStatus::ReadyForTraffic;
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    let topology = Arc::new(tokio::sync::RwLock::new(rafka_node_admin_core::topology::Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![admin],
    }));
    let gate = ParkGate::new();
    let runner = Arc::new(CallingOut { calls: calls.clone(), gate: gate.clone(), hold_first });
    let exec = Arc::new(BuildExecutor { executor: "mesh1.admin.1".into(), builds: builds.clone(), topology, runner });
    memory
        .publish_accepted(&BuildAccepted { build_id: build_id(), topology: FabricTopology::root("fabric1", "mesh1", MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    memory.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let door = RunDoor { me: "mesh1.admin.1".parse().unwrap(), builds, exec, runs, local: memory.clone(), gate: gate.clone() };
    let plan = RunPlan { operations: vec![BuildOperation::CreateMesh { mesh: "mesh2".into(), mesh_id: MeshId::mint() }, BuildOperation::CreateMesh { mesh: "mesh3".into(), mesh_id: MeshId::mint() }], hand_off_to: None };
    (door, gate, plan)
}

/// CONTRACT (node-stop.md steps 3-4, R-ON1; park_gate.rs:5-11): `mesh1.admin.1` serves
/// `build.attempt.run` for a fabric-primary elsewhere and its run is inside its first operation when
/// the admin's stop parks its gate. The stop's quiesce names the run (`build-run`) among the turns it
/// waits for and resolves only once the run has ended: every call the run makes (its second
/// operation included) and its reconcile span end before the stop's cut. Must NOT happen: a quiesce
/// that resolves while the run is mid-operation, so the stopped admin's run calls out after its cut.
#[tokio::test(start_paused = true)]
async fn a_run_served_for_a_remote_fabric_primary_ends_before_its_admins_stop_cut_and_calls_out_no_more() {
    let (capture, _guard) = capture("mesh1.admin.1");
    let calls = Arc::new(Calls::default());
    let (door, gate, plan) = executor(&calls, true).await;
    let Attached::Live { disposition, mut reader, .. } = door.attempt_run(&build_id(), 1, "mesh1.admin.1", &CallContext::default(), &[], &plan).await else { panic!("the claimed attempt is started") };
    assert_eq!(disposition, Disposition::Started);
    calls.entered.notified().await;

    // The admin's stop: park, wait for the turns in progress, then the cut.
    let cut = tokio::spawn({
        let (gate, calls) = (gate.clone(), calls.clone());
        async move {
            let at = gate.quiesce().await;
            calls.push("stop cut".into());
            (at, std::time::SystemTime::now())
        }
    });
    // Every task runs until it waits on something (the paused clock advances only then): a quiesce
    // that does not wait for the run has cut by now.
    tokio::time::sleep(Duration::from_secs(1)).await;
    calls.release.notify_one();
    let mut terminal = None;
    while let Some(f) = reader.next().await {
        terminal = Some(f);
    }
    let (at, cut_at) = cut.await.unwrap();
    assert_eq!(calls.log(), vec!["call create-mesh:mesh2".to_string(), "call create-mesh:mesh3".into(), "stop cut".into()], "every call of the run precedes the stop's cut: none follows it");
    assert!(matches!(terminal, Some(BuildReply::Complete { attempt: 1, .. })), "the run ran to its terminal before the cut: {terminal:?}");
    assert_eq!(at.named(), "build-run", "the quiesce waited for the run's turn, by name");

    let spans = capture.spans();
    crate::assert_own_story(spans.iter().map(|s| s["name"].as_str().unwrap()));
    let reconcile = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.update.via-reconcile").expect("the run's reconcile span was exported");
    let cut_ns = cut_at.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    assert!(reconcile["end_ns"].as_u64().unwrap() <= cut_ns, "the run's reconcile ended before the stop's cut: {reconcile}");
    println!("trace {} reconcile {}", reconcile["trace_id"], reconcile["attributes"]);
}

/// CONTRACT (node-stop: a stopped node does nothing outward until it is started; park_gate.rs:5-11):
/// `mesh1.admin.1` is stopped (its gate parked) when a fabric-primary elsewhere calls
/// `build.attempt.run` for the attempt it claimed for it. The stopped admin answers the call (the run
/// is started, so a later call reattaches to it) and its run waits at the gate: it makes no call while
/// the admin is stopped. Once the admin starts, the run makes its calls and reaches its terminal.
/// Must NOT happen: a run of a stopped admin calling out.
#[tokio::test(start_paused = true)]
async fn a_run_served_while_its_admin_is_stopped_calls_out_only_once_the_admin_starts() {
    let (capture, _guard) = capture("mesh1.admin.1");
    let calls = Arc::new(Calls::default());
    let (door, gate, plan) = executor(&calls, false).await;
    let _ = gate.quiesce().await;
    let Attached::Live { disposition, mut reader, .. } = door.attempt_run(&build_id(), 1, "mesh1.admin.1", &CallContext::default(), &[], &plan).await else { panic!("the stopped admin answers the claimed attempt") };
    assert_eq!(disposition, Disposition::Started);
    // Every task runs until it waits on something: a run that does not wait at the gate has called out by now.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(calls.log(), Vec::<String>::new(), "the stopped admin's run made no call");
    gate.resume();
    calls.push("start".into());
    let mut terminal = None;
    while let Some(f) = reader.next().await {
        terminal = Some(f);
    }
    assert_eq!(calls.log(), vec!["start".to_string(), "call create-mesh:mesh2".into(), "call create-mesh:mesh3".into()], "the run called out only once its admin started");
    assert!(matches!(terminal, Some(BuildReply::Complete { attempt: 1, .. })), "{terminal:?}");
    let spans = capture.spans();
    crate::assert_own_story(spans.iter().map(|s| s["name"].as_str().unwrap()));
    let reconcile = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.update.via-reconcile").expect("the run's reconcile span was exported");
    println!("trace {} reconcile {}", reconcile["trace_id"], reconcile["attributes"]);
}
