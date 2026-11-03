//! An admin's own run commanding a stop of the admin it runs on, beside the stops that are not its
//! own run's: the functional rows of the matrix whose e2e rows are
//! `crates/rdm-test/tests/fabric/own_delete_matrix.rs`.
//!
//! Why missed: until f982ad9fa no cell had an admin's own run command a stop of that admin. The
//! control plane names a node-admin the executor of its own stop, connections delete or
//! delete in exactly one place, `executor.rs` `node_op_executor`'s `last_mesh_gone` (every mesh Dead
//! and the target its mesh's only node: the fabric-primary after fabric.deleted, fabric-delete.md step
//! 4); everywhere else a primary hands over first (node.primary.update) and another admin executes it, or `last_admin`
//! refuses it by name (R-MT1, Q7). So every node_stop, node_delete, connections-delete
//! and mesh_delete cell has a different admin execute. The one cell that reached the branch,
//! `rdm-admin/tests/fabric_delete.rs` crit:e12.s59.1, asserted `executor_for` names the fabric-primary
//! for its own node.delete and then ran `pipeline.delete_node` over a stub fleet: no admin gate, no run
//! turn, no caller stream and a provider that ends no process, so the run's turn the stop's quiesce
//! waited on, the cut of the caller's connection and the provider ending the executor's own process
//! before node.deleted had nowhere to happen. Every e2e fabric_delete cell stopped at "only the
//! fabric-primary is left, for its separate node.delete" and ended with `Estate::stop`, SIGTERM: the
//! signal's stop (no commander, `quiesce()`, no run in progress), never step 4's call. And the
//! build_run rig's stops were all commanded from outside the run (`gate.quiesce()` on a task of the
//! cell's own), so a run commanding the stop of the gate it holds a turn of had no row.
//!
//! The rows. Each runs on the build_run rig: `mesh1.node-admin.1`'s executor door with its own gate, the
//! fabric-primary and mesh-primary of a one-mesh fabric, and mesh1's section of the cell's Build, one
//! node.delete, whose runner holds the admin's tasks as the row names. The gate does not tell a stop, a
//! connections delete and a delete apart: each holds the admin's tasks through the one
//! `Session::hold_tasks(commanded_by)`, so the hold the runner takes stands for each of them.
//! - its own run commands a hold of itself, the run served for a fabric-primary elsewhere and the run
//!   its own drive pass dispatched: the hold resolves inside the connections-delete bound without that
//!   run's turns, and the run reaches its terminal;
//! - the hold every other stop takes, `quiesce()`, commanded from inside that run: it waits on the run
//!   that waits on it, and the bound ends it (the defect f982ad9fa removed, kept as the rig's control);
//! - a stop commanded by another Build's executor waits for the run in progress, and resolves once it ends;
//! - a stop of itself from a signal waits for the run in progress, and resolves once it ends;
//! - a stop its own run commands still waits for every turn that is not that Build's;
//! - the callers that stop spares are exactly the readers of that Build's drive, and the end of its
//!   runtime (`Drive::until_read`, `AdminRunner::delete_own`) waits until each read its last reply.

use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::{BuildId, BuildOperation};
use rdm_admin::build_drive::{Drive, Read};
use rdm_admin::build_run::{Attached, FramedBuilds, RunDoor, SectionRuns};
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::executor::{BuildExecutor, OperationRunner, RunEnd};
use rdm_admin::model::*;
use rdm_admin::stop_gate::StopGate;
use rdm_core::build::{BuildReply, Disposition};
use rdm_core::context::CallContext;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// The bound of a stop's hold: the node's connections-delete bound (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`,
/// 5 s by default), inside which every in-flight handler finishes before the node leaves.
const BOUND: Duration = rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE;

fn build_id() -> BuildId {
    BuildId::parse("vhpqxdbt5w2g").unwrap()
}

/// Another Build, whose executor commands a stop of this admin from elsewhere.
const OTHER_BUILD: &str = "7kq2m9vx4pzr";

/// How the run's operation holds its own admin's tasks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Hold {
    /// As a stop this admin's own run commands holds them (`node_self.rs` `stop_steps`: the commander
    /// is this node): every turn but the Build's.
    CommandedBy,
    /// As every other stop holds them: every turn.
    EveryTurn,
    /// The run does not stop its admin: its operation waits until the cell releases it.
    Held,
}

/// What the run did, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CallEvent { Operation(rdm_core::OperationId), Held(String), TimedOut }

#[derive(Default)]
struct Calls {
    log: Mutex<Vec<CallEvent>>,
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

/// The executor's runner: its one operation commands, in process, the stop its op names of the admin it
/// runs on and waits on it (the executor's own node.delete composes the whole node.stop), bounded by
/// the connections-delete bound.
struct OwnAdmin {
    calls: Arc<Calls>,
    gate: StopGate,
    hold: Hold,
}

#[async_trait::async_trait]
impl OperationRunner for OwnAdmin {
    async fn run(&self, build: &BuildId, op: &BuildOperation) -> Result<(), RunEnd> {
        self.calls.push(CallEvent::Operation(op.key()));
        let held = match self.hold {
            Hold::Held => {
                self.calls.entered.notify_one();
                self.calls.release.notified().await;
                return Ok(());
            }
            Hold::CommandedBy => tokio::time::timeout(BOUND, self.gate.quiesce_commanded_by(&build.to_string())).await,
            Hold::EveryTurn => tokio::time::timeout(BOUND, self.gate.quiesce()).await,
        };
        match held {
            Ok(at) => self.calls.push(CallEvent::Held(at.named())),
            Err(_) => {
                self.calls.push(CallEvent::TimedOut);
                return Err(RunEnd::failed(format!("{:?}: the stop's hold waited on the run that commanded it past {BOUND:?}", op.key())));
            }
        }
        Ok(())
    }
}

/// A ready node of `mesh1` at `path`.`name`, with a birth; `primary` holds mesh1's and the fabric's primary.
fn ready(path: &str, name: &str, r#type: &str, primary: bool) -> Node {
    let mut n = Node::allocated(rdm_core::PathName::new(path, name).unwrap(), "mesh1", r#type);
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// `mesh1.node-admin.1`, the fabric-primary and mesh-primary of a one-mesh fabric, its executor door and
/// gate, its step log, and the cell's Build, which deletes `mesh1.node.1`: its acceptance pins that
/// node's birth (build.md §4.4), so mesh1's section is that one node.delete, which the door plans.
async fn executor(calls: &Arc<Calls>, hold: Hold) -> (RunDoor, StopGate, Vec<BuildOperation>) {
    let memory = Arc::new(MemoryBuildStateAdapter::new());
    let admin = ready("mesh1.node-admin", "1", "node-admin", true);
    let executor_birth = admin.birth().unwrap();
    let me = admin.node_id.clone();
    let fabric = Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process };
    let meshes = vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }];
    let deleted = ready("mesh1.node", "1", "node", false);
    let after = rdm_admin::topology::Topology { fabric: fabric.clone(), meshes: meshes.clone(), nodes: vec![admin.clone()] };
    let mut accepted = FabricTopology::of_observed(&after);
    accepted.pinned = [(deleted.location(), rdm_admin::build::PinnedBirth::of(&deleted).unwrap())].into_iter().collect();
    memory.publish_accepted(&BuildAccepted { build_id: build_id(), topology: accepted, change: None, source: None, time: 0 }).await.unwrap();
    let view = rdm_admin::topology::Topology { fabric, meshes, nodes: vec![admin, deleted] };
    let section = rdm_admin::build_run::section_of(&memory.read_build(&build_id()).await.unwrap(), "mesh1", &view, &me);
    assert_eq!(section.len(), 1, "mesh1's section is the one delete: {section:?}");
    let topology = Arc::new(tokio::sync::RwLock::new(view));
    let runs = Arc::new(SectionRuns::default());
    let steps = rdm_admin::events_storage::EventsWriter::on(memory.events(), &topology.read().await.fabric.id, &executor_birth.node_id);
    let builds: Arc<dyn BuildStateAdapter> = Arc::new(FramedBuilds::new(memory.clone(), runs.clone()).writing_steps(steps, "mesh1"));
    let gate = StopGate::new();
    let runner = Arc::new(crate::loopback::Steps::new(Arc::new(OwnAdmin { calls: calls.clone(), gate: gate.clone(), hold }), builds.clone()));
    let exec = Arc::new(BuildExecutor { executor: executor_birth, builds: builds.clone(), topology, runner });
    let door = RunDoor { me_path: "mesh1.node-admin".into(), me_name: "1".into(), builds, exec, runs, local: memory.clone(), gate: gate.clone() };
    (door, gate, section)
}

/// Every frame of `reader` to its end, bounded: a run that never reaches its terminal fails the cell by name.
async fn terminal(what: &str, reader: &mut rdm_admin::build_run::RunReader) -> BuildReply {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut last = None;
        while let Some(f) = reader.next().await {
            last = Some(f);
        }
        last.unwrap_or_else(|| panic!("{what}: the run's stream ended with no frame"))
    })
    .await
    .unwrap_or_else(|_| panic!("{what}: the run reached no terminal within 60 s"))
}

/// CONTRACT (fabric-delete.md step 4: the fabric-primary executes its own node.delete; node-delete.md
/// step 1: a live target's delete composes the whole node.stop; node-stop.md steps 2-3; stop_gate.rs:
/// "a stop of this node that a turn commands and waits on does not wait for that turn"): an admin's own
/// run commands a hold of the admin it runs on (the one `Session::hold_tasks(commanded_by)` its stop,
/// connections delete and delete take), from mesh1's section served for a fabric-primary elsewhere
/// (`build.attempt.run`) and from the section its own drive pass runs (the pass's turn of the Build, as
/// `DriveEnv` takes it). The hold the run commands
/// resolves inside the connections-delete bound, waiting for no turn (the run's and the pass's are the
/// Build's), and the run reaches `Complete`. What must NOT happen: the hold waiting on the run that
/// waits on it, so the step idles to its reply budget and the delete never reaches node.deleted.
#[tokio::test(start_paused = true)]
async fn its_own_runs_hold_of_itself_holds_inside_the_bound_without_that_runs_turns() {
    for dispatched in ["served", "under-its-own-pass"] {
        let what = dispatched.to_string();
        let calls = Arc::new(Calls::default());
        let (door, gate, section) = executor(&calls, Hold::CommandedBy).await;
        let op = section[0].clone();
        // The drive pass that runs the section on its own admin holds a turn of the Build, as
        // `DriveEnv` takes it (build_drive.rs: the pass's turn `of_build`).
        let pass = if dispatched == "under-its-own-pass" { Some(gate.turn("build-drive").await.of_build(&build_id().to_string())) } else { None };
        let attached = match &pass {
            Some(pass) => door.run_under(pass, &build_id(), "mesh1", &CallContext::default(), section.clone()).await,
            None => door.attempt_run(&build_id(), "mesh1", &CallContext::default()).await,
        };
        let Attached::Live { disposition, mut reader, .. } = attached else { panic!("{what}: mesh1's section is started") };
        assert_eq!(disposition, Disposition::Started, "{what}");
        let end = terminal(&what, &mut reader).await;
        assert!(matches!(end, BuildReply::Complete { .. }), "{what}: the run reached its terminal: {end:?} after {:?}", calls.log());
        let want = vec![CallEvent::Operation(op.key()), CallEvent::Held(String::new())];
        assert_eq!(calls.log(), want, "{what}: the hold its own run commanded waited for no turn of that run");
        drop(pass);
        assert_eq!(gate.state().borrow().in_progress(), 0, "{what}: every turn of the Build ended with it");
    }
}

/// CONTRACT (the rig's control; stop_gate.rs: every other stop "waits for every turn"): the same run,
/// its stop of its own admin held as every other stop holds (`quiesce()`, the hold before f982ad9fa):
/// the hold waits for the run's own turn, which waits on it, until the connections-delete bound ends
/// it, and the run fails naming the hold. Must NOT happen: the rig reading this self-wait as a hold
/// that resolved, which would leave the row above proving nothing.
#[tokio::test(start_paused = true)]
async fn the_hold_every_other_stop_takes_commanded_from_inside_the_run_waits_on_that_run_until_the_bound() {
    let calls = Arc::new(Calls::default());
    let (door, _gate, section) = executor(&calls, Hold::EveryTurn).await;
    let Attached::Live { mut reader, .. } = door.attempt_run(&build_id(), "mesh1", &CallContext::default()).await else { panic!("mesh1's section is started") };
    let begun = tokio::time::Instant::now();
    let end = terminal("quiesce() from inside the run", &mut reader).await;
    assert!(matches!(&end, BuildReply::Incomplete { sections, .. } if sections.len() == 1 && sections[0].reason.contains("waited on the run that commanded it")), "the section ends incomplete naming the hold (Q346): {end:?}");
    assert!(begun.elapsed() >= BOUND, "the hold ended only at the bound: {:?}", begun.elapsed());
    assert_eq!(calls.log(), vec![CallEvent::Operation(section[0].key()), CallEvent::TimedOut]);
}

/// The run of the cell's Build held inside its operation, and the gate it holds a turn of.
async fn run_in_progress(calls: &Arc<Calls>) -> (StopGate, rdm_admin::build_run::RunReader) {
    let (door, gate, _) = executor(calls, Hold::Held).await;
    let Attached::Live { reader, .. } = door.attempt_run(&build_id(), "mesh1", &CallContext::default()).await else { panic!("mesh1's section is started") };
    calls.entered.notified().await;
    (gate, reader)
}

/// A stop that is not this Build's run's own (`hold`, given the admin's gate), while that run is inside
/// its operation: it does not resolve while the run is in progress, and resolves once the run reached
/// its terminal, naming the run's turn.
async fn waits_for_the_run<F, Fut>(what: &str, hold: F)
where
    F: FnOnce(StopGate) -> Fut,
    Fut: std::future::Future<Output = rdm_admin::stop_gate::GateState> + Send + 'static,
{
    let calls = Arc::new(Calls::default());
    let (gate, mut reader) = run_in_progress(&calls).await;
    let hold = tokio::spawn(hold(gate));
    // Every task runs until it waits on something (the paused clock advances only then): a hold that
    // does not wait for the run has resolved by now.
    tokio::time::sleep(BOUND).await;
    assert!(!hold.is_finished(), "{what}: the hold resolved while the run was inside its operation");
    calls.release.notify_one();
    let end = terminal(what, &mut reader).await;
    assert!(matches!(end, BuildReply::Complete { .. }), "{what}: {end:?}");
    let at = tokio::time::timeout(BOUND, hold).await.unwrap_or_else(|_| panic!("{what}: the hold did not resolve once the run ended")).unwrap();
    assert_eq!(at.named(), "build-run", "{what}: the hold waited for the run's turn, by name");
}

/// CONTRACT (node-stop.md: the mesh-primary's StopNode to the exact birth; stop_gate.rs: a stop another
/// node commands waits for every turn): a stop of `mesh1.node-admin.1` that another Build's executor
/// commands (`commanded_by` names that Build, not the one running here) while this admin's run of the
/// cell's Build is inside its operation waits for that run, and resolves once it ends. Must NOT happen:
/// a stop that excludes the turns of a Build other than its commander's.
#[tokio::test(start_paused = true)]
async fn a_stop_commanded_by_another_builds_executor_waits_for_the_run_in_progress_and_resolves_once_it_ends() {
    waits_for_the_run("another Build's stop", |gate| async move { gate.quiesce_commanded_by(OTHER_BUILD).await }).await;
}

/// CONTRACT (node-stop.md steps 2-4 with the node its own subject, i146 Q55: a signal's stop; stop_gate.rs):
/// a stop of `mesh1.node-admin.1` from its own signal (no commander: `quiesce()`) while its run is inside its
/// operation waits for that run, and resolves once it ends. Must NOT happen: a signal's stop that cuts
/// while a run is mid-operation.
#[tokio::test(start_paused = true)]
async fn a_stop_of_itself_from_a_signal_waits_for_the_run_in_progress_and_resolves_once_it_ends() {
    waits_for_the_run("the signal's stop", |gate| async move { gate.quiesce().await }).await;
}

/// CONTRACT (stop_gate.rs: `quiesce_commanded_by` "waits for every turn but that Build's"): a stop
/// `mesh1.node-admin.1`'s own run commands, while another task of the admin (`backbone`) is in its turn,
/// waits for that turn and resolves once it ends, inside the bound. Must NOT happen: a stop its own run
/// commands that skips every turn, so the admin's other work runs on past its cut.
#[tokio::test(start_paused = true)]
async fn a_stop_its_own_run_commands_still_waits_for_every_turn_that_is_not_that_builds() {
    let calls = Arc::new(Calls::default());
    let (door, gate, section) = executor(&calls, Hold::CommandedBy).await;
    let other = gate.turn("backbone").await;
    let Attached::Live { mut reader, .. } = door.attempt_run(&build_id(), "mesh1", &CallContext::default()).await else { panic!("mesh1's section is started") };
    tokio::time::sleep(BOUND / 2).await;
    assert_eq!(calls.log(), vec![CallEvent::Operation(section[0].key())], "the hold waits for the backbone's turn");
    drop(other);
    let end = terminal("a stop its own run commands beside another turn", &mut reader).await;
    assert!(matches!(end, BuildReply::Complete { .. }), "the hold resolved inside the bound once the other turn ended: {end:?} {:?}", calls.log());
    assert_eq!(calls.log(), vec![CallEvent::Operation(section[0].key()), CallEvent::Held("backbone".into())], "the hold named the other turn it waited for");
}

/// CONTRACT (node-stop.md "Transport cut": "never ... the stop call's stream"; fabric-delete.md "After
/// completion": node.deleted "is its last reply and the caller takes its word"): the connections a stop
/// its own run commands spares are exactly the callers reading that Build's drive (`Drive::callers`,
/// what `AdminSession::callers_of` hands the cut): a reader of a caller adds its key while it reads, a
/// reader of no caller adds none, and another Build's caller is not this Build's. The executor ends
/// its own runtime only once that drive has ended and every caller took its terminal
/// (`Drive::until_read`, `AdminRunner::delete_own`): not while the drive runs, not after the terminal
/// while a caller has yet to read it, and at once when the last caller has. Must NOT happen: the cut
/// closing the caller's stream, or the runtime ending before the caller read node.deleted.
#[tokio::test(start_paused = true)]
async fn the_callers_its_own_stop_spares_are_that_builds_readers_and_its_runtime_ends_only_after_their_last_reply() {
    let caller = iroh::SecretKey::generate().public();
    let elsewhere = iroh::SecretKey::generate().public();
    let drive = Drive::detached(build_id());
    let other = Drive::detached(BuildId::parse(OTHER_BUILD).unwrap());
    let mut reading = drive.reader().of_caller(Some(caller));
    let unnamed = drive.reader().of_caller(None);
    let _theirs = other.reader().of_caller(Some(elsewhere));
    assert_eq!(drive.callers(), vec![caller], "the Build's one caller is spared, and only it");
    assert_eq!(other.callers(), vec![elsewhere]);
    drop(unnamed);

    let ended = tokio::spawn({
        let drive = drive.clone();
        async move { drive.until_read().await }
    });
    tokio::time::sleep(BOUND).await;
    assert!(!ended.is_finished(), "the runtime would end while the drive still runs");
    drive.finish(BuildReply::Complete { build_id: build_id().to_string() });
    tokio::time::sleep(BOUND).await;
    assert!(!ended.is_finished(), "the runtime would end before the caller read the terminal");
    assert!(matches!(reading.next().await, Some(Read::Terminal(BuildReply::Complete { .. }))), "the caller reads the terminal");
    drop(reading);
    tokio::time::timeout(Duration::from_millis(1), ended).await.expect("the runtime ends at once when its last caller read its last reply").unwrap();
    assert_eq!(drive.callers(), Vec::<iroh::PublicKey>::new(), "no caller is left to spare");
}
