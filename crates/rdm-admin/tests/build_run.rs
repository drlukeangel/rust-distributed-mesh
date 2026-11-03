//! A mesh-primary's run of its mesh's section is a turn of its admin's stop gate (stop_gate.rs: a task
//! that takes the view lock begins each turn at its own wait point; a stop's quiesce waits for every
//! turn already past it). A run the admin serves for a remote fabric-primary (`build.attempt.run`,
//! op `0x20`) is its own task: it begins at its wait point and the stop's quiesce waits for it, so
//! nothing it does follows the stop's cut (node-stop.md steps 3-4, R-ON1). A stopped admin keeps
//! answering the call, and the run it starts waits at the gate until the admin starts.

use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::{BuildId, BuildOperation};
use rdm_admin::build_run::{Attached, FramedBuilds, RunDoor, SectionRuns};
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::executor::{BuildExecutor, OperationRunner, RunEnd};
use rdm_admin::model::*;
use rdm_admin::stop_gate::StopGate;
use rdm_core::build::{BuildReply, Disposition};
use rdm_core::context::CallContext;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::Notify;
use tracing_subscriber::layer::SubscriberExt;

/// The cell's own span capture: the product span pipeline in front of an in-memory exporter.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    pipeline: rdm_transport::SpanCapture,
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
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture(service, exporter.clone());
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    (Capture { exporter, pipeline }, guard)
}

fn build_id() -> BuildId {
    BuildId::parse("vhpqxdbt5w2g").unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CallEvent { Operation(rdm_core::OperationId, bool), StopCut, Start }

/// What the runner did, in order, beside the instants the cell marks (the stop's cut).
#[derive(Default)]
struct Calls {
    log: Mutex<Vec<CallEvent>>,
    /// The first operation waits here once it has been entered, while `hold_first` is set.
    entered: Notify,
    release: Notify,
}

impl Calls {
    fn push(&self, what: CallEvent) {
        self.log.lock().unwrap().push(what);
    }
    fn log(&self) -> Vec<CallEvent> {
        self.log.lock().unwrap().clone()
    }
}

/// The executor's runner: each operation is a call out of the admin, recorded with whether the
/// admin's gate was stopped when it was made. The first operation holds until the cell releases it
/// when `hold_first` is set.
struct CallingOut {
    calls: Arc<Calls>,
    gate: StopGate,
    hold_first: bool,
}

#[async_trait::async_trait]
impl OperationRunner for CallingOut {
    async fn run(&self, _: &BuildId, op: &BuildOperation) -> Result<(), RunEnd> {
        let first = self.calls.log().iter().all(|c| !matches!(c, CallEvent::Operation(..)));
        self.calls.push(CallEvent::Operation(op.key(), self.gate.is_stopped()));
        if first && self.hold_first {
            self.calls.entered.notify_one();
            self.calls.release.notified().await;
        }
        Ok(())
    }
}

/// A ready node of `mesh1` at `path`.`name`, with a birth; `primary` holds mesh1's primary.
fn ready(path: &str, name: &str, r#type: &str, primary: bool) -> Node {
    let mut n = Node::allocated(rdm_core::PathName::new(path, name).unwrap(), "mesh1", r#type);
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// mesh1 as `admin`, its primary, and two nodes it serves; the cell's Build deletes both nodes: its
/// acceptance pins their births (build.md §4.4), so mesh1's section is their two deletes, one after the
/// other, run by mesh1's primary.
async fn mesh1_section(admin: Node, memory: &MemoryBuildStateAdapter) -> (Arc<tokio::sync::RwLock<rdm_admin::topology::Topology>>, Vec<BuildOperation>) {
    let me = admin.node_id.clone();
    let fabric = Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process };
    let meshes = vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }];
    let deleted = [ready("mesh1.node", "1", "node", false), ready("mesh1.node", "2", "node", false)];
    let after = rdm_admin::topology::Topology { fabric: fabric.clone(), meshes: meshes.clone(), nodes: vec![admin.clone()] };
    let mut topology = FabricTopology::of_observed(&after);
    topology.pinned = deleted.iter().map(|n| (n.location(), rdm_admin::build::PinnedBirth::of(n).unwrap())).collect();
    memory.publish_accepted(&BuildAccepted { build_id: build_id(), topology, change: None, source: None, time: 0 }).await.unwrap();
    let view = rdm_admin::topology::Topology { fabric, meshes, nodes: [vec![admin], deleted.to_vec()].concat() };
    let section = rdm_admin::build_run::section_of(&memory.read_build(&build_id()).await.unwrap(), "mesh1", &view, &me);
    assert_eq!(section.len(), 2, "mesh1's section is the two deletes: {section:?}");
    (Arc::new(tokio::sync::RwLock::new(view)), section)
}

/// `mesh1.node-admin.1`'s door with its own gate and its step log, as mesh1's primary, for the cell's
/// Build; and mesh1's section of it, which the door plans.
async fn executor(calls: &Arc<Calls>, hold_first: bool) -> (RunDoor, StopGate, Vec<BuildOperation>) {
    let memory = Arc::new(MemoryBuildStateAdapter::new());
    let admin = ready("mesh1.node-admin", "1", "node-admin", true);
    let executor_birth = admin.birth().unwrap();
    let (topology, section) = mesh1_section(admin, &memory).await;
    let runs = Arc::new(SectionRuns::default());
    let steps = rdm_admin::events_storage::EventsWriter::on(memory.events(), &topology.read().await.fabric.id, &executor_birth.node_id);
    let builds: Arc<dyn BuildStateAdapter> = Arc::new(FramedBuilds::new(memory.clone(), runs.clone()).writing_steps(steps, "mesh1"));
    let gate = StopGate::new();
    let runner = Arc::new(crate::loopback::Steps::new(Arc::new(CallingOut { calls: calls.clone(), gate: gate.clone(), hold_first }), builds.clone()));
    let exec = Arc::new(BuildExecutor { executor: executor_birth.clone(), builds: builds.clone(), topology, runner });
    let door = RunDoor { me_path: "mesh1.node-admin".into(), me_name: "1".into(), builds, exec, runs, local: memory.clone(), gate: gate.clone() };
    (door, gate, section)
}

/// CONTRACT (node-stop.md steps 3-4, R-ON1; stop_gate.rs:5-11): `mesh1.node-admin.1` serves
/// `build.attempt.run` (mesh1's section) for a fabric-primary elsewhere and its run is inside its first operation when
/// the admin's stop holds its gate. The stop's quiesce names the run (`build-run`) among the turns it
/// waits for and resolves only once the run has ended: every call the run makes (its second
/// operation, on the stopped gate, included) and its reconcile span end before the stop's cut. Must NOT happen: a quiesce
/// that resolves while the run is mid-operation, so the stopped admin's run calls out after its cut.
#[tokio::test(start_paused = true)]
async fn a_run_served_for_a_remote_fabric_primary_ends_before_its_admins_stop_cut_and_calls_out_no_more() {
    let (capture, _guard) = capture("mesh1.node-admin.1");
    let calls = Arc::new(Calls::default());
    let (door, gate, plan) = executor(&calls, true).await;
    let Attached::Live { disposition, mut reader, .. } = door.attempt_run(&build_id(), "mesh1", &CallContext::default()).await else { panic!("mesh1's section is started") };
    assert_eq!(disposition, Disposition::Started);
    calls.entered.notified().await;

    // The admin's stop: stop, wait for the turns in progress, then the cut.
    let cut = tokio::spawn({
        let (gate, calls) = (gate.clone(), calls.clone());
        async move {
            let at = gate.quiesce().await;
            calls.push(CallEvent::StopCut);
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
    // The run was past its wait point at the stop: its second operation runs on the stopped gate, before the cut.
    assert_eq!(calls.log(), vec![CallEvent::Operation(plan[0].key(), false), CallEvent::Operation(plan[1].key(), true), CallEvent::StopCut], "every call of the run precedes the stop's cut: none follows it");
    assert!(matches!(terminal, Some(BuildReply::Complete { .. })), "the run ran to its terminal before the cut: {terminal:?}");
    assert_eq!(at.named(), "build-run", "the quiesce waited for the run's turn, by name");

    let spans = capture.spans();
    crate::assert_own_story(spans.iter().map(|s| s["name"].as_str().unwrap()));
    let reconcile = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.update.via-reconcile").expect("the run's reconcile span was exported");
    let cut_ns = cut_at.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    assert!(reconcile["end_ns"].as_u64().unwrap() <= cut_ns, "the run's reconcile ended before the stop's cut: {reconcile}");
    let turn = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.serve.via-run-turn").expect("the run's turn span was exported");
    assert_eq!(turn["attributes"]["turn"], "begun", "the served run began its own turn at its wait point: {turn}");
    println!("trace {} reconcile {} turn {}", reconcile["trace_id"], reconcile["attributes"], turn["attributes"]);
}

/// CONTRACT (node-stop: a stopped node does nothing outward until it is started; stop_gate.rs:5-11):
/// `mesh1.node-admin.1` is stopped (its gate stopped) when a fabric-primary elsewhere calls
/// `build.attempt.run` for mesh1's section. The stopped admin answers the call (the run
/// is started, so a later call reattaches to it) and its run waits at the gate: it makes no call while
/// the admin is stopped. Once the admin starts, the run makes its calls and reaches its terminal.
/// Must NOT happen: a run of a stopped admin calling out.
#[tokio::test(start_paused = true)]
async fn a_run_served_while_its_admin_is_stopped_calls_out_only_once_the_admin_starts() {
    let (capture, _guard) = capture("mesh1.node-admin.1");
    let calls = Arc::new(Calls::default());
    let (door, gate, plan) = executor(&calls, false).await;
    let _ = gate.quiesce().await;
    let Attached::Live { disposition, mut reader, .. } = door.attempt_run(&build_id(), "mesh1", &CallContext::default()).await else { panic!("the stopped admin answers the call for mesh1's section") };
    assert_eq!(disposition, Disposition::Started);
    // Every task runs until it waits on something: a run that does not wait at the gate has called out by now.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(calls.log(), Vec::<CallEvent>::new(), "the stopped admin's run made no call");
    gate.resume();
    calls.push(CallEvent::Start);
    let mut terminal = None;
    while let Some(f) = reader.next().await {
        terminal = Some(f);
    }
    assert_eq!(calls.log(), vec![CallEvent::Start, CallEvent::Operation(plan[0].key(), false), CallEvent::Operation(plan[1].key(), false)], "the run called out only once its admin started");
    assert!(matches!(terminal, Some(BuildReply::Complete { .. })), "{terminal:?}");
    let spans = capture.spans();
    crate::assert_own_story(spans.iter().map(|s| s["name"].as_str().unwrap()));
    let reconcile = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.update.via-reconcile").expect("the run's reconcile span was exported");
    let turn = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.serve.via-run-turn").expect("the run's turn span was exported");
    assert_eq!(turn["attributes"]["turn"], "waited-for-start", "the stopped admin's run waited at the gate until the start: {turn}");
    println!("trace {} reconcile {} turn {}", reconcile["trace_id"], reconcile["attributes"], turn["attributes"]);
}

/// CONTRACT (stop_gate.rs: a turn already past its wait point when the gate stops runs to its end;
/// build_drive.rs, a pass is one turn and runs to its end, the section it runs included): the
/// fabric-primary's own drive pass is past its wait point when its admin's stop holds the gate, and it
/// runs its own mesh's section on its own admin. The run begins its turn under the pass's at once, runs to
/// its terminal, and the stop's quiesce resolves once the pass and the run have both ended: nothing of
/// either follows the cut. Must NOT happen: the pass's own run waiting at the gate for the pass that
/// waits on it, so neither ends and the stop never cuts.
#[tokio::test(start_paused = true)]
async fn a_drive_pass_dispatching_to_its_own_admin_after_its_stop_began_runs_under_its_turn_and_never_waits_at_the_gate() {
    let (capture, _guard) = capture("mesh1.node-admin.1");
    let calls = Arc::new(Calls::default());
    let (door, gate, plan) = executor(&calls, false).await;
    let pass = gate.turn("build-drive").await;
    let cut = tokio::spawn({
        let (gate, calls) = (gate.clone(), calls.clone());
        async move {
            let at = gate.quiesce().await;
            calls.push(CallEvent::StopCut);
            at
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(gate.is_stopped(), "the stop held the gate while the pass was in its turn");
    let Attached::Live { disposition, mut reader, .. } = door.run_under(&pass, &build_id(), "mesh1", &CallContext::default(), plan.clone()).await else { panic!("the pass's own section is started") };
    assert_eq!(disposition, Disposition::Started);
    let terminal = tokio::time::timeout(Duration::from_secs(20), async {
        let mut terminal = None;
        while let Some(f) = reader.next().await {
            terminal = Some(f);
        }
        terminal
    })
    .await
    .expect("the pass's own run reached its terminal: it did not wait at the gate for the pass");
    assert!(matches!(terminal, Some(BuildReply::Complete { .. })), "{terminal:?}");
    assert_eq!(calls.log(), vec![CallEvent::Operation(plan[0].key(), true), CallEvent::Operation(plan[1].key(), true)], "the run of a pass past its wait point ran, and the stop has not cut");
    drop(pass);
    let at = cut.await.unwrap();
    assert_eq!(at.named(), "build-drive", "at the stop only the pass was in a turn");
    assert_eq!(calls.log().last(), Some(&CallEvent::StopCut), "the stop cut once the pass and its run had ended");
    let spans = capture.spans();
    crate::assert_own_story(spans.iter().map(|s| s["name"].as_str().unwrap()));
    let turn = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.serve.via-run-turn").expect("the run's turn span was exported");
    assert_eq!(turn["attributes"]["turn"], "under-drive-pass", "the run's turn was begun under the pass's: {turn}");
    let reconcile = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.update.via-reconcile").expect("the run's reconcile span was exported");
    println!("trace {} reconcile {} turn {}", reconcile["trace_id"], reconcile["attributes"], turn["attributes"]);
}


/// A rename changes the name at the same path while retaining the node ID and incarnation. The
/// renamed admin, mesh1's primary by its birth in its view, runs mesh1's section under its new name,
/// and its steps' events are its exact birth's. An admin of another node ID that presents the same
/// incarnation is not mesh1's primary in that view: its door refuses the call `NotExecutor`, naming the
/// primary and itself.
#[tokio::test(start_paused = true)]
async fn a_renamed_admin_runs_its_meshs_section_under_its_new_name() {
    let (capture, _guard) = capture("mesh1.node-admin.1");
    let calls = Arc::new(Calls::default());
    let memory = Arc::new(MemoryBuildStateAdapter::new());
    let admin = ready("mesh1.node-admin", "9", "node-admin", true);
    let executor_birth = admin.birth().unwrap();
    let (topology, section) = mesh1_section(admin, &memory).await;
    let fabric_id = topology.read().await.fabric.id.clone();
    let door_of = |birth: rdm_core::framing::Birth, me_name: &str| {
        let runs = Arc::new(SectionRuns::default());
        let mut steps = rdm_admin::events_storage::EventsWriter::on(memory.events(), &fabric_id, &birth.node_id);
        steps.source.incarnation = birth.incarnation.clone();
        let builds: Arc<dyn BuildStateAdapter> = Arc::new(FramedBuilds::new(memory.clone(), runs.clone()).writing_steps(steps, "mesh1"));
        let runner = Arc::new(crate::loopback::Steps::new(Arc::new(CallingOut { calls: calls.clone(), gate: StopGate::new(), hold_first: false }), builds.clone()));
        let exec = Arc::new(BuildExecutor { executor: birth, builds: builds.clone(), topology: topology.clone(), runner });
        RunDoor { me_path: "mesh1.node-admin".into(), me_name: me_name.into(), builds, exec, runs, local: memory.clone(), gate: StopGate::new() }
    };
    let door = door_of(executor_birth.clone(), "9");

    // Only the name changes; the birth is the same.
    {
        let at = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let mut t = topology.write().await;
        t.nodes[0].path = at.path;
        t.nodes[0].name = at.name;
    }
    let Attached::Live { disposition, mut reader, .. } = door.attempt_run(&build_id(), "mesh1", &CallContext::default()).await else {
        panic!("the renamed admin, mesh1's primary by its birth, starts mesh1's section")
    };
    assert_eq!(disposition, Disposition::Started);
    let mut terminal = None;
    while let Some(f) = reader.next().await {
        terminal = Some(f);
    }
    assert!(matches!(terminal, Some(BuildReply::Complete { .. })), "the renamed admin ran both operations to Complete: {terminal:?}");
    assert_eq!(calls.log(), vec![CallEvent::Operation(section[0].key(), false), CallEvent::Operation(section[1].key(), false)]);
    let folded = memory.read_build(&build_id()).await.unwrap();
    assert!(!folded.steps.is_empty() && folded.steps.iter().all(|s| s.source == format!("rdm://{fabric_id}/{}/{}", executor_birth.node_id, executor_birth.incarnation)), "the rename keeps the birth every step event names: {:?}", folded.steps);

    // Another node ID with the same incarnation is not mesh1's primary in the view: refused.
    let other_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: executor_birth.incarnation.clone() };
    let other = door_of(other_birth.clone(), "1");
    let Attached::Refused(refused) = other.attempt_run(&build_id(), "mesh1", &CallContext::default()).await else {
        panic!("a call to an admin that is not mesh1's primary must be refused")
    };
    assert!(
        matches!(&refused, BuildReply::NotExecutor { named, recipient } if named == &executor_birth && recipient == &other_birth),
        "the refusal names mesh1's primary and the recipient's birth: {refused:?}"
    );

    let spans = capture.spans();
    crate::assert_own_story(spans.iter().map(|s| s["name"].as_str().unwrap()));
    for name in ["rdm.node_admin.build.serve.via-attempt-run", "rdm.node_admin.build.serve.via-run-turn"] {
        for s in spans.iter().filter(|s| s["name"] == name && s["attributes"]["outcome"] != "not-executor") {
            assert_eq!((s["attributes"]["node_path"].as_str(), s["attributes"]["node_name"].as_str()), (Some("mesh1.node-admin"), Some("1")), "{name} names the admin's held name: {s}");
        }
    }
    let reconcile = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.update.via-reconcile").expect("the run's reconcile span was exported");
    assert_eq!(reconcile["attributes"]["executor"], executor_birth.to_string(), "via-reconcile names the admin's birth: {reconcile}");
    println!("trace {} reconcile {}", reconcile["trace_id"], reconcile["attributes"]);
}
