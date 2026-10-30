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
//! The rows. Each runs on the build_run rig: `mesh1.admin.1`'s executor door with its own gate, the
//! fabric-primary of a one-mesh fabric, and a run whose one operation is the cell's. The gate does not
//! tell a stop, a connections delete and a delete apart: each holds the admin's tasks
//! through the one `Session::hold_tasks(commanded_by)`, so the first row runs every one of them.
//! - its own run commands a stop, connections delete or node delete of itself, the run served for a
//!   fabric-primary elsewhere and the run its own drive pass dispatched: the hold resolves inside the
//!   connections-delete bound without that run's turns, and the run reaches its terminal;
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
use rdm_admin::build_run::{AttemptRuns, Attached, FramedBuilds, RunDoor};
use rdm_admin::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::executor::{BuildExecutor, OperationRunner, RunEnd, RunPlan};
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
#[derive(Default)]
struct Calls {
    log: Mutex<Vec<String>>,
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
    async fn run(&self, build: &BuildId, _: Option<&str>, _: u32, op: &BuildOperation) -> Result<(), RunEnd> {
        self.calls.push(format!("call {}", op.key()));
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
            Ok(at) => self.calls.push(format!("held [{}]", at.named())),
            Err(_) => {
                self.calls.push("the hold waited on its own run past the bound".into());
                return Err(RunEnd::failed(format!("{}: the stop's hold waited on the run that commanded it past {BOUND:?}", op.key())));
            }
        }
        Ok(())
    }
}

/// `mesh1.admin.1`, the fabric-primary of a one-mesh fabric, its executor door and gate, attempt 1 of
/// the cell's Build claimed for it, and a run of `op`.
async fn executor(calls: &Arc<Calls>, hold: Hold, op: BuildOperation) -> (RunDoor, StopGate, RunPlan) {
    let memory = Arc::new(MemoryBuildStateAdapter::new());
    let runs = Arc::new(AttemptRuns::default());
    let builds: Arc<dyn BuildStateAdapter> = Arc::new(FramedBuilds::new(memory.clone(), runs.clone()));
    let mut admin = Node::allocated("mesh1.admin.1".parse().unwrap());
    admin.status = NodeStatus::Ready;
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    let topology = Arc::new(tokio::sync::RwLock::new(rdm_admin::topology::Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![admin],
    }));
    let gate = StopGate::new();
    let runner = Arc::new(OwnAdmin { calls: calls.clone(), gate: gate.clone(), hold });
    let exec = Arc::new(BuildExecutor { executor: "mesh1.admin.1".into(), builds: builds.clone(), topology, runner });
    memory
        .publish_accepted(&BuildAccepted { build_id: build_id(), topology: FabricTopology::root("fabric1", "mesh1", MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    memory.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let door = RunDoor { me: "mesh1.admin.1".parse().unwrap(), builds, exec, runs, local: memory.clone(), gate: gate.clone() };
    (door, gate, RunPlan { operations: vec![op], hand_off_to: None })
}

/// The three operations of `mesh1.admin.1` on itself whose stop its own run commands.
fn own_operations() -> Vec<BuildOperation> {
    let node: PathName = "mesh1.admin.1".parse().unwrap();
    let (node_id, birth) = (NodeId::mint(), IncarnationId::mint());
    let pinned = rdm_admin::build::PinnedBirth { node_id: node_id.clone(), incarnation: birth.clone() };
    vec![
        BuildOperation::StopNode { node: node.clone(), node_id: node_id.clone(), from_incarnation: birth.clone() },
        BuildOperation::DeleteConnections { node: node.clone(), node_id, from_incarnation: birth },
        BuildOperation::DeleteLiveNode { node, exited: None, birth: Some(pinned) },
    ]
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
/// run commands a stop, a connections delete and a delete of the admin it runs on, each from
/// a run served for a fabric-primary elsewhere (`build.attempt.run`) and from the run its own drive
/// pass dispatched (the pass's turn of the Build, as `DriveEnv` takes it). The hold the run commands
/// resolves inside the connections-delete bound, waiting for no turn (the run's and the pass's are the
/// Build's), and the run reaches `Complete`. What must NOT happen: the hold waiting on the run that
/// waits on it, so the step idles to its reply budget and the delete never reaches node.deleted.
#[tokio::test(start_paused = true)]
async fn its_own_runs_stop_connections_delete_or_delete_of_itself_holds_inside_the_bound_without_that_runs_turns() {
    for op in own_operations() {
        for dispatched in ["served", "under-its-own-pass"] {
            let what = format!("{} {dispatched}", op.key());
            let calls = Arc::new(Calls::default());
            let (door, gate, plan) = executor(&calls, Hold::CommandedBy, op.clone()).await;
            // The drive pass that dispatched the attempt to its own admin holds a turn of the Build, as
            // `DriveEnv` takes it (build_drive.rs: the pass's turn `of_build`).
            let pass = if dispatched == "under-its-own-pass" { Some(gate.turn("build-drive").await.of_build(&build_id().to_string())) } else { None };
            let attached = match &pass {
                Some(pass) => door.attempt_run_under(pass, &build_id(), 1, "mesh1.admin.1", &CallContext::default(), &plan).await,
                None => door.attempt_run(&build_id(), 1, "mesh1.admin.1", &CallContext::default(), &plan).await,
            };
            let Attached::Live { disposition, mut reader, .. } = attached else { panic!("{what}: the claimed attempt is started") };
            assert_eq!(disposition, Disposition::Started, "{what}");
            let end = terminal(&what, &mut reader).await;
            assert!(matches!(end, BuildReply::Complete { attempt: 1, .. }), "{what}: the run reached its terminal: {end:?} after {:?}", calls.log());
            let want = vec![format!("call {}", op.key()), "held []".to_string()];
            assert_eq!(calls.log(), want, "{what}: the hold its own run commanded waited for no turn of that run");
            drop(pass);
            assert_eq!(gate.state().borrow().in_progress(), 0, "{what}: every turn of the Build ended with it");
        }
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
    let op = BuildOperation::DeleteLiveNode { node: "mesh1.admin.1".parse().unwrap(), exited: None, birth: Some(rdm_admin::build::PinnedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }) };
    let (door, _gate, plan) = executor(&calls, Hold::EveryTurn, op).await;
    let Attached::Live { mut reader, .. } = door.attempt_run(&build_id(), 1, "mesh1.admin.1", &CallContext::default(), &plan).await else { panic!("the claimed attempt is started") };
    let begun = tokio::time::Instant::now();
    let end = terminal("quiesce() from inside the run", &mut reader).await;
    assert!(matches!(&end, BuildReply::Failed { reason, .. } if reason.contains("waited on the run that commanded it")), "the run fails naming the hold: {end:?}");
    assert!(begun.elapsed() >= BOUND, "the hold ended only at the bound: {:?}", begun.elapsed());
    assert_eq!(calls.log(), vec!["call node.delete:mesh1.admin.1".to_string(), "the hold waited on its own run past the bound".into()]);
}

/// The run of the cell's Build held inside its operation, and the gate it holds a turn of.
async fn run_in_progress(calls: &Arc<Calls>) -> (StopGate, rdm_admin::build_run::RunReader) {
    let op = BuildOperation::CreateMesh { mesh: "mesh2".into(), mesh_id: MeshId::mint() };
    let (door, gate, plan) = executor(calls, Hold::Held, op).await;
    let Attached::Live { reader, .. } = door.attempt_run(&build_id(), 1, "mesh1.admin.1", &CallContext::default(), &plan).await else { panic!("the claimed attempt is started") };
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
    assert!(matches!(end, BuildReply::Complete { attempt: 1, .. }), "{what}: {end:?}");
    let at = tokio::time::timeout(BOUND, hold).await.unwrap_or_else(|_| panic!("{what}: the hold did not resolve once the run ended")).unwrap();
    assert_eq!(at.named(), "build-run", "{what}: the hold waited for the run's turn, by name");
}

/// CONTRACT (node-stop.md: the mesh-primary's StopNode to the exact birth; stop_gate.rs: a stop another
/// node commands waits for every turn): a stop of `mesh1.admin.1` that another Build's executor
/// commands (`commanded_by` names that Build, not the one running here) while this admin's run of the
/// cell's Build is inside its operation waits for that run, and resolves once it ends. Must NOT happen:
/// a stop that excludes the turns of a Build other than its commander's.
#[tokio::test(start_paused = true)]
async fn a_stop_commanded_by_another_builds_executor_waits_for_the_run_in_progress_and_resolves_once_it_ends() {
    waits_for_the_run("another Build's stop", |gate| async move { gate.quiesce_commanded_by(OTHER_BUILD).await }).await;
}

/// CONTRACT (node-stop.md steps 2-4 with the node its own subject, i146 Q55: a signal's stop; stop_gate.rs):
/// a stop of `mesh1.admin.1` from its own signal (no commander: `quiesce()`) while its run is inside its
/// operation waits for that run, and resolves once it ends. Must NOT happen: a signal's stop that cuts
/// while a run is mid-operation.
#[tokio::test(start_paused = true)]
async fn a_stop_of_itself_from_a_signal_waits_for_the_run_in_progress_and_resolves_once_it_ends() {
    waits_for_the_run("the signal's stop", |gate| async move { gate.quiesce().await }).await;
}

/// CONTRACT (stop_gate.rs: `quiesce_commanded_by` "waits for every turn but that Build's"): a stop
/// `mesh1.admin.1`'s own run commands, while another task of the admin (`backbone`) is in its turn,
/// waits for that turn and resolves once it ends, inside the bound. Must NOT happen: a stop its own run
/// commands that skips every turn, so the admin's other work runs on past its cut.
#[tokio::test(start_paused = true)]
async fn a_stop_its_own_run_commands_still_waits_for_every_turn_that_is_not_that_builds() {
    let calls = Arc::new(Calls::default());
    let op = BuildOperation::DeleteLiveNode { node: "mesh1.admin.1".parse().unwrap(), exited: None, birth: Some(rdm_admin::build::PinnedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }) };
    let (door, gate, plan) = executor(&calls, Hold::CommandedBy, op).await;
    let other = gate.turn("backbone").await;
    let Attached::Live { mut reader, .. } = door.attempt_run(&build_id(), 1, "mesh1.admin.1", &CallContext::default(), &plan).await else { panic!("the claimed attempt is started") };
    tokio::time::sleep(BOUND / 2).await;
    assert_eq!(calls.log(), vec!["call node.delete:mesh1.admin.1".to_string()], "the hold waits for the backbone's turn");
    drop(other);
    let end = terminal("a stop its own run commands beside another turn", &mut reader).await;
    assert!(matches!(end, BuildReply::Complete { attempt: 1, .. }), "the hold resolved inside the bound once the other turn ended: {end:?} {:?}", calls.log());
    assert_eq!(calls.log(), vec!["call node.delete:mesh1.admin.1".to_string(), "held [backbone]".into()], "the hold named the other turn it waited for");
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
    let mut reading = drive.reader(1).of_caller(Some(caller));
    let unnamed = drive.reader(1).of_caller(None);
    let _theirs = other.reader(1).of_caller(Some(elsewhere));
    assert_eq!(drive.callers(), vec![caller], "the Build's one caller is spared, and only it");
    assert_eq!(other.callers(), vec![elsewhere]);
    drop(unnamed);

    let ended = tokio::spawn({
        let drive = drive.clone();
        async move { drive.until_read().await }
    });
    tokio::time::sleep(BOUND).await;
    assert!(!ended.is_finished(), "the runtime would end while the drive still runs");
    drive.finish(BuildReply::Complete { build_id: build_id().to_string(), attempt: 1 });
    tokio::time::sleep(BOUND).await;
    assert!(!ended.is_finished(), "the runtime would end before the caller read the terminal");
    assert!(matches!(reading.next().await, Some(Read::Terminal(BuildReply::Complete { .. }))), "the caller reads the terminal");
    drop(reading);
    tokio::time::timeout(Duration::from_millis(1), ended).await.expect("the runtime ends at once when its last caller read its last reply").unwrap();
    assert_eq!(drive.callers(), Vec::<iroh::PublicKey>::new(), "no caller is left to spare");
}
