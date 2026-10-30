// Why missed: Rafka 141 found a Build that stalled for ever (Pending, attempt 1, no step) after a
// Reconcile that differed from the accepted topology only by naming, for the Day-0 admin's path, the
// executable its row already records (day 0's entry names none). Two gaps met there, and no cell held
// either:
// (c) No cell submitted a Reconcile naming an already-recorded executable on an existing path as the
//     only difference. The harness's requests (rdm-test `executables::Current::held`, the demo UI's
//     `Admin::current`) fill a held path that names no executable with the one its row records, and the
//     topology they read (the current Build's facts copy, which carries no executables) names none at
//     any path, so every held path is named from its row. Only the Day-0 admin's accepted entry truly
//     names none, so its None -> Some is the one delta the compiler sees, in every first request after
//     day 0; but every cell's first
//     request after day 0 also grows the fabric (`reconcile_shape`, `submit_named` with `resized`,
//     `create_node`), a resize that rightly puts the fabric back to pending, so the reset that delta
//     alone caused (`fabric_status::resized` compares the whole topology) was never told apart. After
//     that first request every entry names its executable, and a later re-read request (build/
//     executables.rs, "the re-read request ... changes nothing") is the accepted topology itself. In
//     rdm-admin, `named_request` (tests/main.rs) submits a change that creates no path untouched, so
//     only a create ever named an executable, and the drive fixture's rows (`rows_of`, build_drive.rs)
//     record a sha only where the entry names one: no fixture held a row recording what an entry that
//     names none runs.
// (a) No cell had the fabric reach ready while the held run was in flight. The drive cell
//     for a Build with nothing to execute (build_drive.rs `FabricChange`) flips readiness between
//     passes only, never during a dispatch; the e2e resizes (fabric/fabric_states.rs) run real
//     creates, whose births check in after the run handed back, so the fabric was still changing when
//     the drive read `fabric_ready()` after the run. Only a Build with nothing to execute over a fabric
//     whose every member is already ready (the Day-0 admin alone, or any reset with no work) lets the
//     fabric's change finish inside the run's own hand-back, where the drive read it as "ended
//     incomplete with nothing moved" and idled on a world that never moves.
//
//! The Reconcile variant matrix, the drive's rows (the acceptance rows, which variants put the fabric
//! back to pending, are `fabric_status::tests::a_reconcile_puts_the_fabric_back_to_pending_only_for_a_resize_or_an_executable_replacement`;
//! the estate's rows are rdm-test build/reconcile_variants.rs).
//!
//! Variants (each a complete-topology request, `Reconcile { based_on, nodes }`, through the control
//! plane's door): the Day-0 admin named on the executable its row records (its entry names none), every
//! held path named on the executable its entry names, the unchanged request (every held path naming
//! none), tags only, storage only, a new path (one create), a removed path (one delete), another
//! executable (a replacement: delete then create at the path). A placement-only request is the unchanged
//! one: `PlacementMeta` declares no constraint.
//!
//! Timings, the fabric's ready state as the drive's gate reads it: already set before the run,
//! set while the run is in flight, set after the run handed back, held by the application and then
//! answered, held and then answered with a failure.
//!
//! Every row (states.md "Resize and executable replacement"; Q154; Q158): the Build is Complete only once
//! the fabric holds ready, reports Blocked naming `fabric-change` while it does not, and is
//! never blocked for ever; a failure the application reports fails it on `fabric-change` by name. Every
//! operation the request owes runs once, and a request that owes none runs nothing. Must NOT happen: a
//! held run read as "ended incomplete" because the fabric's change finished while it was in flight, the
//! drive idle on a world that never moves.
//!
//! Which timings a variant meets in the product: one that is neither a resize nor a replacement leaves
//! the fabric ready (before the run); the drive's rule does not depend on why the fabric
//! moves, so every variant runs every timing.

use rdm_admin::accepted::{AcceptedStore, FabricTopology, Recorded, RecordedRow, TopologyChange};
use rdm_admin::build::{BuildId, BuildOperation};
use rdm_admin::build_claim::{AttemptContexts, ClaimDoor};
use rdm_admin::build_drive::{Drive, DriveEnd, DriveEnv, Gate, Read};
use rdm_admin::build_state::{BuildState, BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::control_plane::ControlPlane;
use rdm_admin::executor::OperationRunner;
use rdm_admin::model::*;
use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::topology::Topology;
use rdm_core::build::BuildReply;
use rdm_core::meta::{NodeSpec, PersistentDisposition, StorageMeta};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// The fixture view: `mesh1.admin.1`, the Day-0 admin and fabric-primary; its mesh primary
/// `mesh1.admin.2`; `mesh1.rpc.1`.
fn view() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node("mesh1.admin.1", false, true), node("mesh1.admin.2", true, false), node("mesh1.rpc.1", false, false)],
    }
}

fn path(p: &str) -> PathName {
    p.parse().unwrap()
}

/// A binding no launch reads: a held path's, which the door hashes only when the request creates the path
/// or replaces its node.
fn recorded_binding(kind: NodeKind, sha: &str) -> rdm_core::binding::Binding {
    rdm_core::binding::Binding { launch_id: kind.name().into(), executable: format!("/b/{}-{sha}", kind.name()).into(), sha256: sha.into(), image: None }
}

/// What each node's row records: the Day-0 admin `a1`, `mesh1.admin.2` `a2`, `mesh1.rpc.1` `r1`.
const ROWS: [(&str, &str); 3] = [("mesh1.admin.1", "a1"), ("mesh1.admin.2", "a2"), ("mesh1.rpc.1", "r1")];

#[derive(Clone, Copy, Debug)]
enum Variant {
    Day0AdminNamedOnItsRecordedExecutable,
    EveryPathNamedOnItsEntrysExecutable,
    Unchanged,
    TagsOnly,
    StorageOnly,
    NewPath,
    RemovedPath,
    AnotherExecutable,
}

impl Variant {
    /// The request, from the accepted topology's entries.
    fn request(self, held: &FabricTopology) -> Vec<NodeSpec> {
        let mut r: Vec<NodeSpec> = held.meshes.values().flat_map(|m| m.spec_by_path.values().cloned()).collect();
        let at = |r: &mut Vec<NodeSpec>, p: &str| -> usize { r.iter().position(|s| s.path.to_string() == p).expect("the request lists the path") };
        match self {
            Self::Day0AdminNamedOnItsRecordedExecutable => {
                let i = at(&mut r, "mesh1.admin.1");
                r[i].executable = Some(recorded_binding(NodeKind::NodeAdmin, "a1"));
            }
            Self::EveryPathNamedOnItsEntrysExecutable => {}
            Self::Unchanged => r.iter_mut().for_each(|s| s.executable = None),
            Self::TagsOnly => {
                let i = at(&mut r, "mesh1.rpc.1");
                r[i].tags = ["zone_a".to_string()].into();
            }
            Self::StorageOnly => {
                let i = at(&mut r, "mesh1.rpc.1");
                r[i].storage_persistence = StorageMeta::Persistent { disposition: PersistentDisposition::Preserve };
            }
            Self::NewPath => {
                let mut s = NodeSpec::default_for(path("mesh1.rpc.2"));
                s.executable = Some(crate::test_executable(NodeKind::RpcNode));
                r.push(s);
            }
            Self::RemovedPath => r.retain(|s| s.path.to_string() != "mesh1.rpc.1"),
            Self::AnotherExecutable => {
                let i = at(&mut r, "mesh1.rpc.1");
                r[i].executable = Some(crate::test_executable(NodeKind::RpcNode));
            }
        }
        r
    }

    /// The operations the request owes, by key.
    fn owes(self) -> Vec<&'static str> {
        match self {
            Self::NewPath => vec!["node.create:mesh1.rpc.2"],
            Self::RemovedPath => vec!["node.delete:mesh1.rpc.1"],
            Self::AnotherExecutable => vec!["node.create:mesh1.rpc.1", "node.delete:mesh1.rpc.1"],
            _ => vec![],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Timing {
    /// Ready is set before the run.
    Before,
    /// It is set while the run is in flight: after the drive planned it, before the drive reads how it ended.
    During,
    /// It is set once the run handed back and the drive reported the Build Blocked on the fabric's change.
    After,
    /// The fabric's change holds for two passes, then the fabric is ready.
    HeldThenAnswered,
}

/// The fabric as the drive's gate reads it, and what each node's row records.
struct Readiness {
    ready: AtomicBool,
    recorded: Arc<Mutex<Recorded>>,
}

#[async_trait::async_trait]
impl Gate for Readiness {
    fn fabric_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
    async fn recorded_executables(&self, _topology: &FabricTopology) -> Result<Recorded, String> {
        Ok(self.recorded.lock().unwrap().clone())
    }
}

/// Records what ran and commits each operation's `Complete` receipt, as the create pipeline does; a create
/// records on its row the executable its entry names, a delete leaves no live row (build-deployment.md §2.3).
struct Runs {
    ran: Mutex<Vec<String>>,
    builds: Arc<MemoryBuildStateAdapter>,
    recorded: Arc<Mutex<Recorded>>,
}

#[async_trait::async_trait]
impl OperationRunner for Runs {
    async fn run(&self, build: &BuildId, _lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.lock().unwrap().push(op.key());
        match op {
            BuildOperation::CreateNode { node, .. } => {
                let topology = self.builds.read_build(build).await.map_err(|e| e.to_string())?.topology;
                let sha = rdm_admin::accepted::target_executable(&topology, node).map(|b| b.sha256.clone());
                self.recorded.lock().unwrap().insert(node.clone(), RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: NodeId::mint(), sha256: sha });
            }
            BuildOperation::DeleteNode { node, .. } | BuildOperation::DeleteLiveNode { node, .. } => {
                self.recorded.lock().unwrap().remove(node);
            }
            _ => {}
        }
        let receipt = BuildStepReceipt { mesh: None, build_id: build.clone(), attempt, operation: op.key(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None };
        self.builds.append_step_receipt(&receipt).await.map_err(|e| e.to_string().into())
    }
}

/// The fixture's dispatcher: the loopback, which, for [`Timing::During`], sets the fabric's
/// ready state once the attempt it calls has run and before the drive reads how it ended.
struct Dispatch {
    inner: Arc<crate::loopback::Loopback>,
    during: Option<Arc<Readiness>>,
}

#[async_trait::async_trait]
impl rdm_admin::build_drive::Dispatcher for Dispatch {
    async fn dispatch(&self, turn: &rdm_admin::stop_gate::Turn, executor: &PathName, build_id: &BuildId, attempt: u32, context: rdm_core::context::CallContext, plan: rdm_admin::executor::RunPlan) -> rdm_admin::build_drive::Dispatched {
        let run = self.inner.dispatch(turn, executor, build_id, attempt, context, plan).await;
        if let Some(fabric) = &self.during {
            fabric.ready.store(true, Ordering::SeqCst);
        }
        run
    }
    fn hold(&self, me: &PathName, build_id: &BuildId, attempt: u32) -> Option<rdm_admin::build_run::HeldRun> {
        self.inner.hold(me, build_id, attempt)
    }
}

struct Row {
    builds: Arc<MemoryBuildStateAdapter>,
    env: Arc<DriveEnv>,
    fabric: Arc<Readiness>,
    runs: Arc<Runs>,
    build: BuildId,
}

/// The Build of `variant`'s request over the fixture, accepted at the control plane's door, and the drive
/// that runs it under `timing`.
async fn row(variant: Variant, timing: Timing) -> Row {
    let t = view();
    let mut held = FabricTopology::of_observed(&t);
    // Day 0's entry for the Day-0 admin names no executable; every later path's names what it runs.
    for (p, sha) in ROWS.iter().skip(1) {
        let p = path(p);
        let mesh = held.meshes.get_mut("mesh1").unwrap();
        let mut c = mesh.spec(&p).cloned().unwrap();
        c.executable = Some(recorded_binding(p.kind, sha));
        mesh.set_spec(c).unwrap();
    }
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), held.clone(), "mesh1.admin.1").await.unwrap();
    let topology = Arc::new(RwLock::new(t.clone()));
    let store = Arc::new(rdm_admin::storage::MemoryNodesStorage::default());
    let mut recorded = Recorded::new();
    for (p, sha) in ROWS {
        let n = t.node(&path(p)).expect("the view holds the node");
        let mut row = rdm_admin::storage::NodeRow::new(n.name.clone(), n.node_id.clone(), NodeStatus::Ready);
        row.executable = Some(recorded_binding(n.name.kind, sha));
        store.write_record(&row).await.unwrap();
        recorded.insert(n.name.clone(), RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: n.node_id.clone(), sha256: Some(sha.into()) });
    }
    let mut cp = ControlPlane::new(builds.clone(), accepted.clone(), path("mesh1.admin.1"), t.clone(), crate::adopted_time());
    cp.topology = topology.clone();
    let _ = cp.node_rows.set(store);
    let change = TopologyChange::Reconcile { topology_storage_provider: None, based_on: builds.current().await.unwrap().expect("day 0's Build is current").build_id, nodes: variant.request(&held) };
    let opened = cp.submit("test", change).await.unwrap_or_else(|e| panic!("{variant:?}: the door accepts the request: {e:?}"));
    let recorded = Arc::new(Mutex::new(recorded));
    let fabric = Arc::new(Readiness { ready: AtomicBool::new(timing == Timing::Before), recorded: recorded.clone() });
    let runs = Arc::new(Runs { ran: Mutex::new(Vec::new()), builds: builds.clone(), recorded });
    let loopback = crate::loopback::Loopback::new(builds.clone(), builds.clone(), topology.clone(), runs.clone());
    let me = path("mesh1.admin.1");
    let fp_id = t.node(&me).unwrap().node_id.clone();
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: topology.clone(),
        builds: builds.clone(),
        door: Arc::new(ClaimDoor {
            node_id: fp_id,
            me,
            topology,
            builds: builds.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            claims: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
            claimed: Default::default(),
        }),
        dispatcher: Arc::new(Dispatch { inner: loopback, during: (timing == Timing::During).then(|| fabric.clone()) }),
        gate: fabric.clone(),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        notices: Default::default(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    Row { builds, env, fabric, runs, build: opened.build_id }
}

/// One pass of the drive, bounded: a pass that does not end is the defect, named.
async fn pass(r: &Row, drive: &Arc<Drive>, what: &str) -> DriveEnd {
    tokio::time::timeout(Duration::from_secs(10), r.env.run(drive)).await.unwrap_or_else(|_| panic!("{what}: a drive pass ends within 10 s"))
}

/// Passes until the drive ends, at most `max`: the ends, the last one first-class.
async fn until_terminal(r: &Row, drive: &Arc<Drive>, max: usize, what: &str) -> Vec<DriveEnd> {
    let mut ends = Vec::new();
    for _ in 0..max {
        let end = pass(r, drive, what).await;
        let done = end == DriveEnd::Terminal;
        ends.push(end);
        if done {
            break;
        }
    }
    ends
}

/// The frames the drive's readers hold now.
async fn frames(reader: &mut rdm_admin::build_drive::DriveReader) -> Vec<Read> {
    let mut out = Vec::new();
    while let Ok(Some(read)) = tokio::time::timeout(Duration::from_millis(50), reader.next()).await {
        out.push(read);
    }
    out
}

/// A pass that waits on the fabric's change: Stalled naming the fabric's ready state, never
/// "ended incomplete" and never an end of the Build.
fn waits_on_the_fabric(end: &DriveEnd, what: &str) {
    match end {
        DriveEnd::Stalled(why) if why.contains("ready") && !why.contains("ended incomplete") => {}
        other => panic!("{what}: while the fabric changes the drive waits on its change, never ends the Build nor idles as ended incomplete: {other:?}"),
    }
}

async fn check(variant: Variant, timing: Timing) {
    let what = format!("{variant:?} with ready {timing:?}");
    let r = row(variant, timing).await;
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    let ends = match timing {
        Timing::Before | Timing::During => until_terminal(&r, &drive, 4, &what).await,
        Timing::After | Timing::HeldThenAnswered => {
            let held = if timing == Timing::HeldThenAnswered { 2 } else { 1 };
            for _ in 0..held {
                waits_on_the_fabric(&pass(&r, &drive, &what).await, &what);
            }
            let seen = frames(&mut reader).await;
            assert!(seen.iter().any(|f| matches!(f, Read::Frame(BuildReply::Blocked { step, .. }) if step == "fabric-change")), "{what}: the Build reports Blocked naming the fabric's change: {seen:?}");
            assert!(!seen.iter().any(|f| matches!(f, Read::Terminal(_) | Read::Frame(BuildReply::Complete { .. } | BuildReply::Failed { .. }))), "{what}: never an end while the fabric changes: {seen:?}");
            assert_ne!(r.builds.read_build(&r.build).await.unwrap().state, BuildState::Complete, "{what}: never Complete while the fabric changes");
            r.fabric.ready.store(true, Ordering::SeqCst);
            until_terminal(&r, &drive, 3, &what).await
        }
    };
    let done = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!(ends.last(), Some(&DriveEnd::Terminal), "{what}: the drive ends the Build, never idles on a fabric that already moved: passes ended {ends:?}; the Build is {:?} at attempt {} with steps {:?}", done.state, done.attempt, done.steps.iter().map(|s| (&s.operation, &s.step)).collect::<Vec<_>>());
    let ran = r.runs.ran.lock().unwrap().clone();
    let mut once = ran.clone();
    once.sort();
    once.dedup();
    assert_eq!(once.len(), ran.len(), "{what}: no operation runs twice: {ran:?}");
    let mut owed: Vec<String> = variant.owes().into_iter().map(String::from).collect();
    owed.sort();
    assert_eq!(once, owed, "{what}: the request's operations ran, and only those");
    assert_eq!(done.state, BuildState::Complete, "{what}: the Build is Complete after its own ready: {done:?}");
}

macro_rules! rows {
    ($($name:ident: $variant:ident, $timing:ident;)*) => {
        $(
            #[tokio::test]
            async fn $name() {
                check(Variant::$variant, Timing::$timing).await;
            }
        )*
    };
}

rows! {
    day0_admin_named_on_its_recorded_executable_ready_before_the_run: Day0AdminNamedOnItsRecordedExecutable, Before;
    day0_admin_named_on_its_recorded_executable_ready_during_the_run: Day0AdminNamedOnItsRecordedExecutable, During;
    day0_admin_named_on_its_recorded_executable_ready_after_the_run: Day0AdminNamedOnItsRecordedExecutable, After;
    day0_admin_named_on_its_recorded_executable_held_then_answered: Day0AdminNamedOnItsRecordedExecutable, HeldThenAnswered;
    every_path_on_its_entrys_executable_ready_before_the_run: EveryPathNamedOnItsEntrysExecutable, Before;
    every_path_on_its_entrys_executable_ready_during_the_run: EveryPathNamedOnItsEntrysExecutable, During;
    every_path_on_its_entrys_executable_ready_after_the_run: EveryPathNamedOnItsEntrysExecutable, After;
    every_path_on_its_entrys_executable_held_then_answered: EveryPathNamedOnItsEntrysExecutable, HeldThenAnswered;
    unchanged_request_ready_before_the_run: Unchanged, Before;
    unchanged_request_ready_during_the_run: Unchanged, During;
    unchanged_request_ready_after_the_run: Unchanged, After;
    unchanged_request_held_then_answered: Unchanged, HeldThenAnswered;
    tags_only_ready_before_the_run: TagsOnly, Before;
    tags_only_ready_during_the_run: TagsOnly, During;
    tags_only_ready_after_the_run: TagsOnly, After;
    tags_only_held_then_answered: TagsOnly, HeldThenAnswered;
    storage_only_ready_before_the_run: StorageOnly, Before;
    storage_only_ready_during_the_run: StorageOnly, During;
    storage_only_ready_after_the_run: StorageOnly, After;
    storage_only_held_then_answered: StorageOnly, HeldThenAnswered;
    new_path_ready_before_the_run: NewPath, Before;
    new_path_ready_during_the_run: NewPath, During;
    new_path_ready_after_the_run: NewPath, After;
    new_path_held_then_answered: NewPath, HeldThenAnswered;
    removed_path_ready_before_the_run: RemovedPath, Before;
    removed_path_ready_during_the_run: RemovedPath, During;
    removed_path_ready_after_the_run: RemovedPath, After;
    removed_path_held_then_answered: RemovedPath, HeldThenAnswered;
    another_executable_ready_before_the_run: AnotherExecutable, Before;
    another_executable_ready_during_the_run: AnotherExecutable, During;
    another_executable_ready_after_the_run: AnotherExecutable, After;
    another_executable_held_then_answered: AnotherExecutable, HeldThenAnswered;
}
