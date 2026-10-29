//! The fabric-primary's drive of a Build (op `0x20`; node-rpc-envelope.md "Build, op `0x20`").
//!
//! The fabric-primary is the one driver of every Build, whether a caller is attached or not (a
//! drift Build has none). For each active Build it:
//!
//! 1. claims the next attempt on its own log through the claim door, for the admin
//!    [`crate::executor::lead_for`] names, or finds the attempt already claimed;
//! 2. calls that admin with `build.attempt.run` (in process when the executor is itself) and passes on
//!    the run's frames into the Build's [`Drive`];
//! 3. on `HandedOff { to }` claims the next attempt for `to`; on `Complete` or `Failed` ends.
//!
//! Callers (`build.create`) read the [`Drive`]'s frames; a cut caller cancels nothing, because the
//! drive is its own task and the caller is a view of its frames. The drive's state is the Build
//! projection: a successor fabric-primary resumes from it, and the attempt it finds claimed it
//! dispatches again (start-or-reattach), so no completed step runs twice.
//!
//! Before every claim and every dispatch the drive passes two gates: the Build log's target is not set
//! (a fabric-primary whose view, by the election's result it installed, names another live birth
//! fabric-primary dispatches nothing), and this admin is the fabric-primary in its view. A loss names
//! the fabric-primary the installed result names.
//! An executor that cannot be reached is replaced only on proof of its departure, never on
//! silence.
//!
//! Each pass of a drive (its gates, its claim, its dispatch and the run's terminal) is one turn of
//! its admin's stop gate ([`crate::stop_gate`]): a stopped admin begins none, and a stop waits for
//! the pass in progress before it cuts.

use crate::build::BuildId;
use crate::build_claim::ClaimDoor;
use crate::build_state::{AttemptOutcome, BuildAttemptReceipt, BuildProjection, BuildState, BuildStateAdapter, BuildStateError};
use crate::executor::{lead_for, run_of, RunPlan};
use crate::build::BuildOperation;
use crate::model::{NodeKind, PathName};
use crate::topology::Topology;
use rdm_core::build::BuildReply;
use rdm_core::build_claim::BuildClaimReply;
use rdm_core::context::CallContext;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, RwLock};
use tracing::instrument::WithSubscriber as _;
use tracing::Instrument as _;

/// The gates every claim and dispatch passes.
#[async_trait::async_trait]
pub trait Gate: Send + Sync {
    /// The target of the Build log, read from the fabric-primary this admin's view holds (the election's
    /// result it installed): refused by name once it names another live birth; otherwise the path.name
    /// of the birth it names when that is this birth, `None` when it names none or a birth proven gone.
    async fn target(&self) -> Result<Option<String>, BuildStateError> {
        Ok(None)
    }
    /// Resolves once the view names another live birth fabric-primary (never, for a gate with none):
    /// a drive waiting on an executor's run ends there, not when that run's stream does.
    async fn target_set(&self) {
        std::future::pending::<()>().await
    }
    /// What the peer-mesh investigation holds of `mesh` now (`crate::investigate::Ladder::peer_mesh`),
    /// read when asked: a node of a peer mesh held unheard is not claimed or dispatched for (R-D1),
    /// except the mesh's first recovering node-admin once the investigation decided its recovery.
    fn peer_mesh(&self, _mesh: &str) -> Option<crate::investigate::PeerMesh> {
        None
    }
    /// node.primary.update before an operation on a seat holder runs (R-MT1, Luke 2026-10-12: "only a
    /// FP should issue commands to a MP"; "this same primitive should be called on MP stop or
    /// delete"): this fabric-primary commands the holder's mesh seat to move, or moves its own seats
    /// (its mesh seat, then the fabric seat), so the operation's executor is the holder's successor.
    /// `operation` names the Build operation the update precedes; `run` is the first attempt of the
    /// run `attempt` belongs to, the update's transfer key with the Build id (ruling R-PU2,
    /// [`crate::fabric_handover::update_operation`]). `Err` names why the seat did not move, and
    /// whether the update was refused before the call or left incomplete after it ([`SeatMoveEnd`]). A
    /// gate with no primaries moves nothing.
    async fn update_primaries(&self, _m: &crate::executor::SeatMove, _build_id: &BuildId, _attempt: u32, _run: u32, _operation: &str) -> Result<(), SeatMoveEnd> {
        Ok(())
    }
    /// node.primary.updated for every update that precedes an operation the drive is about to dispatch
    /// on that update's primary (lifecycles/node-primary-update.md failure contract: "The workflow
    /// continues only after the result is installed and acknowledged: `node.primary.updated` answered
    /// to the resuming executor's call, never a read alone"). An update this admin holds acknowledged
    /// is acknowledged again; otherwise what it came to is discovered from this admin's view and the
    /// successor's node.primary.updated obtained on a call and installed
    /// ([`crate::fabric_handover::HandoverDoor::row_acknowledged`]). `Err` names why an update was not
    /// acknowledged. A gate with no primaries has none to acknowledge.
    async fn primaries_updated(&self, _updates: &[PrecedingUpdate], _build_id: &BuildId, _attempt: u32) -> Result<(), String> {
        Ok(())
    }
    /// Take the completed lifecycle receipts of `build_id` into the fabric-primary's atlas
    /// before `BuildReply::Complete` goes out on the caller's stream (fabric-atlas.md R-FA2).
    async fn attempt_completed(&self, _build_id: &BuildId, _attempt: u32) {}
    /// Take the `Complete` receipt of `operation` of attempt `attempt` of `build_id` into the
    /// fabric-primary's atlas as its frame streams up `build.attempt.run`, before the frame is
    /// passed on (fabric-atlas.md R-FA2), whichever admin executed it.
    async fn receipt_streamed(&self, _build_id: &BuildId, _attempt: u32, _operation: &str) {}
    /// [`Self::receipt_streamed`] of an attempt numbered in mesh `lane`'s lane (a section, Q250).
    async fn receipt_streamed_in(&self, _lane: &str, _build_id: &BuildId, _attempt: u32, _operation: &str) {}
    /// Now on the admin's clock, in milliseconds, for an attempt this drive opens (a section, Q250).
    fn now_ms(&self) -> u64 {
        0
    }
    /// Attempt `attempt` of `build_id` ended `Failed`: what it accepted is no longer executing.
    async fn attempt_failed(&self, _build_id: &BuildId, _attempt: u32) {}
    /// What the node rows record of each member of `topology`'s executable, for a replacement's plan
    /// ([`crate::accepted::recorded_executables`]; build-deployment.md §3). A gate with no rows
    /// records none: every member is owed its replacement.
    async fn recorded_executables(&self, _topology: &crate::accepted::FabricTopology) -> Result<crate::accepted::Recorded, String> {
        Ok(crate::accepted::Recorded::new())
    }
    /// The meshes of `observed` whose Mesh record holds them Dead (mesh-delete.md: the mesh row goes Dead
    /// at mesh.deleted; a `dead` record is final), read from the Mesh records, never from the view: a view
    /// that has not heard a mesh's Dead yet still shows it live. A gate with no Mesh records holds none.
    async fn dead_meshes(&self, _observed: &Topology) -> Result<std::collections::HashSet<crate::model::MeshId>, String> {
        Ok(Default::default())
    }
    /// Every mesh record the store holds, for a fabric.delete's plan: the delete plans from the Mesh rows
    /// on the store, never only from the view, which drops a mesh once its members are gone (fabric-delete.md
    /// "Failure": a runtime inside the workflow whose delete has no receipt is never reported deleted;
    /// mesh-delete.md "Resume after the primary's loss"). A gate with no mesh store holds none: the view's
    /// meshes are planned.
    async fn mesh_rows(&self) -> Result<Vec<crate::storage::MeshRecord>, String> {
        Ok(Vec::new())
    }
    /// The birth the fabric-primary's atlas row holds at `path` now, the record Build acceptance read
    /// (fabric-atlas.md R-FA1, R-FA3), for the plan of a Build whose attempt carries a standalone stop,
    /// start, connections delete or delete of it: planned from the atlas, never from the gossip view.
    /// `Ok(None)` for a gate with no atlas: the view's birth is planned.
    async fn atlas_birth(&self, _path: &PathName, _observed: &Topology) -> Result<Option<crate::accepted::AtlasBirth>, String> {
        Ok(None)
    }
    /// Whether the fabric holds ready now: a Build that put the fabric back to pending is
    /// `Complete` only once the fabric's ready state is set from its change (Q154; states.md
    /// "Resize and executable replacement"). A gate with no fabric change holds it.
    fn fabric_ready(&self) -> bool {
        true
    }
}

/// The node.primary.update that precedes an operation on a primary (R-MT1), named by its operation
/// (`crate::fabric_handover::update_operation`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PrecedingUpdate {
    /// The primary's mesh.
    pub mesh: String,
    /// The update's own operation.
    pub operation: String,
    /// The node the operation acts on, when it names one: the update's primary.
    pub holder: Option<PathName>,
}

/// The node.primary.updates that may precede `op` of Build `build_id` in the run whose first attempt
/// is `run` (R-MT1, ruling R-PU2, [`crate::executor::seat_move`]): for a stop, delete, replace
/// or node.connection.delete of a node-admin, the mesh-primary's update and the fabric-primary's
/// alone; of a node of any other type, its type's primary's update; for a mesh delete, the
/// fabric-primary's under that mesh. None for any other operation.
pub fn preceding_updates(op: &BuildOperation, build_id: &BuildId, run: u32) -> Vec<PrecedingUpdate> {
    use rdm_core::build::Step;
    use crate::fabric_handover::update_operation;
    use rdm_core::Seat;
    let key = op.key();
    match op {
        BuildOperation::ShutdownMesh { mesh, .. } => vec![PrecedingUpdate { mesh: mesh.clone(), operation: update_operation(Seat::FabricPrimary, build_id.as_str(), run, &key), holder: None }],
        BuildOperation::DeleteLiveNode { node, exited: None, .. }
        | BuildOperation::DeleteConnections { node, .. }
        | BuildOperation::StopNode { node, .. }
        | BuildOperation::Step { step: Step::Stop { node } | Step::Delete { node } | Step::DeleteConnections { node }, .. }
            if node.kind == NodeKind::NodeAdmin =>
        {
            [Seat::MeshPrimary, Seat::FabricPrimary].into_iter().map(|seat| PrecedingUpdate { mesh: node.mesh.clone(), operation: update_operation(seat, build_id.as_str(), run, &key), holder: Some(node.clone()) }).collect()
        }
        // Any other node type's primary, commanded by its mesh-primary (node-primary-update.md, "Commander").
        BuildOperation::DeleteLiveNode { node, exited: None, .. }
        | BuildOperation::DeleteConnections { node, .. }
        | BuildOperation::StopNode { node, .. }
        | BuildOperation::Step { step: Step::Stop { node } | Step::Delete { node } | Step::DeleteConnections { node }, .. } => {
            vec![PrecedingUpdate { mesh: node.mesh.clone(), operation: update_operation(Seat::Cohort(node.kind), build_id.as_str(), run, &key), holder: Some(node.clone()) }]
        }
        _ => Vec::new(),
    }
}

/// A gate that is always open: a fixture with no topology view and no seat target.
pub struct OpenGate;

#[async_trait::async_trait]
impl Gate for OpenGate {
    async fn target(&self) -> Result<Option<String>, BuildStateError> {
        Ok(None)
    }
}

/// Proof that an executor's birth deleted: the exact runtime inspected and found exited or
/// os-stopped (build.md, Q239). Running, unproven and silence are not proof.
#[async_trait::async_trait]
pub trait DepartureProof: Send + Sync {
    /// `Ok` when the provider reads the birth `executor` names in this admin's view exited or
    /// os-stopped; `Err` names why it is not.
    async fn proven(&self, executor: &PathName) -> Result<(), String>;
    /// The fabric primary has claimed attempt `attempt` of `build_id`, whose create of the first
    /// node-admin of `mesh`, no member of which answers, was admitted (mesh-investigation.md step 5),
    /// `from` being the instant before the claim: at that same moment it gives the
    /// mesh a new node-admin and authors the fabric `degraded` (step 6, R-DG1), until the mesh's new
    /// primary reports (step 7). A proof that holds no health records nothing.
    async fn mesh_given_node_admin(&self, _mesh: &str, _build_id: &BuildId, _attempt: u32, _from: std::time::Instant) {}
}

/// Where the verdict an executor reported is recorded on the fabric-primary's own log, so the
/// Build's state there is current when the terminal frame goes out.
#[async_trait::async_trait]
pub trait VerdictSink: Send + Sync {
    /// Record `receipt`, which the executor wrote on its own log, here without gossiping it again.
    async fn record(&self, receipt: BuildAttemptReceipt);
}

/// What a dispatch of `build.attempt.run` came to before any frame.
pub enum Dispatched {
    /// The executor accepted: its frames follow.
    Stream(Box<dyn InnerStream>),
    /// The executor refused, by name.
    Refused(BuildReply),
    /// The request provably never reached the executor (`NotSent`): the reason.
    Unreached(String),
}

/// One item of an inner stream.
pub enum InnerItem {
    /// A frame (never `Started`).
    Frame(BuildReply),
    /// The stream ended without a terminal.
    Ended,
    /// The stream broke after the request committed (`Indeterminate`): the reason.
    Broke(String),
}

/// The frames of one `build.attempt.run`.
#[async_trait::async_trait]
pub trait InnerStream: Send {
    /// The next item.
    async fn next(&mut self) -> InnerItem;
    /// The frames handed over so far are recorded: their acks are owed now. A stream read as its
    /// own recovery acks each frame as it reads it, and this does nothing.
    async fn accepted(&mut self) {}
}

/// Calls the executor `executor` names for one claimed attempt.
#[async_trait::async_trait]
pub trait Dispatcher: Send + Sync {
    /// `build.attempt.run` for `attempt` of `build_id` at `executor`, carrying the won claim, the
    /// Build's intent (`intent`: postcard frames of a Build message) and the run the
    /// fabric-primary planned (`plan`): the operations the executor runs and nothing else. `turn` is
    /// the drive pass's turn of the admin's gate: a run this admin makes itself begins its own turn
    /// under it, never waiting at the gate for the pass that waits on it.
    async fn dispatch(
        &self,
        turn: &crate::stop_gate::Turn,
        executor: &PathName,
        build_id: &BuildId,
        attempt: u32,
        context: CallContext,
        intent: Vec<Vec<u8>>,
        plan: RunPlan,
    ) -> Dispatched;
    /// [`Self::dispatch`] of a mesh's drift attempt, numbered in `lane` (Q118): `build.attempt.run`
    /// naming the mesh. A dispatcher that runs no drift attempt refuses it by name.
    #[allow(clippy::too_many_arguments)]
    async fn dispatch_in(
        &self,
        turn: &crate::stop_gate::Turn,
        executor: &PathName,
        lane: &str,
        build_id: &BuildId,
        attempt: u32,
        context: CallContext,
        intent: Vec<Vec<u8>>,
        plan: RunPlan,
    ) -> Dispatched {
        let _ = (turn, build_id, attempt, context, intent, plan);
        Dispatched::Unreached(format!(
            "this dispatcher runs no drift attempt: {executor} in mesh {lane}"
        ))
    }
    /// Hold the run of `attempt` of `build_id` on the admin `me` (the drive's own) while a step of the
    /// Build runs in the drive itself (the holder's own node.primary.update): a dispatch of the attempt
    /// to `me` reattaches to it ([`crate::build_run::HeldRun`]). `None` when it holds no run here.
    fn hold(&self, _me: &PathName, _build_id: &BuildId, _attempt: u32) -> Option<crate::build_run::HeldRun> {
        None
    }
}

/// What a pass's sections did (Q250).
enum Sections {
    /// No section was owed: the fabric lane goes on as it is.
    None,
    /// Every owed section ran (or failed for the first time): the pass plans again.
    Ran,
    /// The drive ends here.
    End(DriveEnd),
}

/// How a section ended short of complete.
enum SectionEnd {
    /// Its verdict is `Failed`.
    Failed(SectionFailure),
    /// It holds its mesh, or could not be opened, claimed or ended: why.
    Held(String),
}

/// How a section failed (Q250, Q252): the operation and step its `Failed` terminal named, with the
/// step's own reason and code; `operation` empty and `step` `section` when it ended without a step
/// failing (refused, its executor exited).
struct SectionFailure {
    operation: String,
    step: String,
    reason: String,
    code: rdm_core::outcome::Code,
}

impl SectionFailure {
    /// What the same failure twice running is keyed on: the operation alone, whatever the reason
    /// (Q252); a section that failed no operation, its reason.
    fn key(&self) -> String {
        if self.operation.is_empty() { self.reason.clone() } else { self.operation.clone() }
    }
}

/// Why a drive ended without the seat: what the old seat says, and the admin the committed fabric
/// seat record names as the seat's holder now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatLoss {
    /// Why this admin may not act as the fabric-primary.
    pub why: String,
    /// The admin (path.name) the fabric-primary record this admin holds names, when it
    /// names a live birth whose path.name is known here.
    pub to: Option<String>,
}

#[derive(Default)]
struct DriveState {
    events: Vec<BuildReply>,
    seen: HashSet<String>,
    terminal: Option<BuildReply>,
    lost: Option<SeatLoss>,
    stall: Option<String>,
    /// The attempt that ended handed back to this fabric-primary with nothing moved, and what its
    /// resume decides on then ([`DriveEnv::world`]): no next attempt is claimed until that moves.
    idle: Option<Idle>,
}

/// What a drive waits on after an attempt it handed back to itself ended with nothing moved.
#[derive(Clone)]
struct Idle {
    attempt: u32,
    world: String,
    why: String,
}

/// One Build's drive: the frames its callers read. The task that makes them is [`DriveEnv::run`].
pub struct Drive {
    /// The Build.
    pub build_id: BuildId,
    /// The first attempt this drive dispatched (0 until it has): the receipts of the attempts from
    /// there to the one running are replayed as frames when a later attempt takes over.
    first_attempt: std::sync::atomic::AtomicU32,
    state: Mutex<DriveState>,
    wake: Notify,
    running: AtomicBool,
    /// An ensure found this drive's run in progress: the run is made once more when it ends waiting
    /// ([`Drives::ensure`]).
    again: AtomicBool,
    /// What the node rows recorded of each member's executable when this drive's pass last read them:
    /// a replacement's plan ([`crate::accepted::plan_with_replacements`]); `None` until a pass of a Build
    /// that plans replacements read them.
    recorded: Mutex<Option<crate::accepted::Recorded>>,
    /// The meshes the Mesh records held Dead when this drive's pass last read them ([`Gate::dead_meshes`]):
    /// planned as Dead whatever the view shows.
    dead: Mutex<std::collections::HashSet<crate::model::MeshId>>,
    /// The endpoint keys of the callers reading this drive's frames now, one per reader
    /// ([`DriveReader::of_caller`]).
    callers: Mutex<Vec<iroh::PublicKey>>,
    /// The Mesh rows the store held when this drive's pass last read them, for a fabric.delete's plan
    /// ([`Gate::mesh_rows`]); `None` for any other Build.
    mesh_rows: Mutex<Option<Vec<crate::storage::MeshRecord>>>,
    /// The birth the fabric-primary's atlas row held at the path of the Build's standalone action when
    /// this drive's pass last read it ([`Gate::atlas_birth`]); `None` for any other attempt, or a gate
    /// with no atlas.
    atlas: Mutex<Option<crate::accepted::AtlasBirth>>,
    /// Each mesh's last section failure (Q250), keyed on its operation ([`SectionFailure::key`]): the
    /// same operation failing again ends the Build `Failed` rather than sending the section once more (Q252).
    section_failed: Mutex<HashMap<String, String>>,
    /// The operations a section this drive sent completed, its run reported by its verdict (Q250), with the
    /// fabric run they were sent in (`BuildProjection::opened`): done in that run only, as a reported
    /// fabric-lane run's are ([`left`]); a later run (a recovery of the same Build) plans from the world.
    section_done: Mutex<HashSet<(u32, String)>>,
}

fn frame_key(f: &BuildReply) -> Option<String> {
    match f {
        BuildReply::Step { attempt, operation, step, .. } => Some(format!("step|{attempt}|{operation}|{step}")),
        BuildReply::Blocked { attempt, operation, step, reason, .. } => Some(format!("blocked|{attempt}|{operation}|{step}|{reason}")),
        _ => None,
    }
}

fn frame_attempt(f: &BuildReply) -> u32 {
    match f {
        BuildReply::Step { attempt, .. } | BuildReply::Blocked { attempt, .. } => *attempt,
        _ => u32::MAX,
    }
}

impl Drive {
    fn new(build_id: BuildId) -> Arc<Self> {
        Arc::new(Self { build_id, first_attempt: std::sync::atomic::AtomicU32::new(0), state: Mutex::new(DriveState::default()), wake: Notify::new(), running: AtomicBool::new(false), again: AtomicBool::new(false), recorded: Mutex::new(None), dead: Mutex::new(Default::default()), callers: Mutex::new(Vec::new()), mesh_rows: Mutex::new(None), atlas: Mutex::new(None), section_failed: Mutex::new(HashMap::new()), section_done: Mutex::new(HashSet::new()) })
    }

    /// A drive that no task makes frames for: the frames are pushed by the caller.
    pub fn detached(build_id: BuildId) -> Arc<Self> {
        Self::new(build_id)
    }

    /// Make a progress frame readable. A frame whose receipt key is already held is dropped, so a
    /// reattached run's replay adds nothing twice. Returns whether it was new.
    pub fn push(&self, frame: BuildReply) -> bool {
        if !self.take_key(&frame) {
            return false;
        }
        self.publish(frame);
        true
    }

    /// Whether `frame` is new to this drive: its receipt key is taken now, so the frame is handled
    /// once whichever way it reached the drive (the live run, the executor's replay of a reattached
    /// run, a receipt read). Nothing is new after the terminal.
    fn take_key(&self, frame: &BuildReply) -> bool {
        let mut s = self.state.lock().unwrap();
        s.terminal.is_none() && frame_key(frame).is_none_or(|key| s.seen.insert(key))
    }

    fn publish(&self, frame: BuildReply) {
        let mut s = self.state.lock().unwrap();
        if s.terminal.is_some() {
            return;
        }
        s.events.push(frame);
        drop(s);
        self.wake.notify_waiters();
    }

    /// The drive ended with `terminal`.
    pub fn finish(&self, terminal: BuildReply) {
        let mut s = self.state.lock().unwrap();
        if s.terminal.is_none() {
            s.terminal = Some(terminal);
        }
        drop(s);
        self.wake.notify_waiters();
    }

    /// The fabric-primary moved: the callers' streams end, and a caller re-submits by build id at the
    /// new fabric-primary.
    pub fn lose_fabric_primary(&self, loss: SeatLoss) {
        let mut s = self.state.lock().unwrap();
        if s.lost.is_none() && s.terminal.is_none() {
            s.lost = Some(loss);
        }
        drop(s);
        self.wake.notify_waiters();
    }

    /// The first attempt this drive dispatched.
    fn note_first_attempt(&self, attempt: u32) -> u32 {
        let _ = self.first_attempt.compare_exchange(0, attempt, Ordering::SeqCst, Ordering::SeqCst);
        self.first_attempt.load(Ordering::SeqCst)
    }

    /// The `Step` frame of every `Complete` receipt of `steps` from attempt `from` to before attempt
    /// `to`: a read of receipts, never a run.
    fn receipt_frames(&self, steps: &[crate::build_state::BuildStepReceipt], from: u32, to: u32) -> Vec<BuildReply> {
        steps
            .iter()
            .filter(|s| {
                s.outcome == crate::build_state::StepOutcome::Complete
                    && s.attempt >= from
                    && s.attempt < to
            })
            .map(|s| BuildReply::Step {
                build_id: self.build_id.to_string(),
                attempt: s.attempt,
                operation: s.operation.clone(),
                step: s.step.clone(),
                receipt: None,
            })
            .collect()
    }

    /// Whether the drive ended with the Build complete: its frames are the record a re-submit is
    /// answered from.
    pub fn completed(&self) -> bool {
        self.completed_attempt().is_some()
    }

    /// The attempt the drive completed the Build at, when it ended with the Build complete.
    pub fn completed_attempt(&self) -> Option<u32> {
        match self.state.lock().unwrap().terminal {
            Some(BuildReply::Complete { attempt, .. }) => Some(attempt),
            _ => None,
        }
    }

    /// Whether this drive dispatched an attempt: it holds the frames of the calls it made.
    fn has_run_log(&self) -> bool {
        self.first_attempt.load(Ordering::SeqCst) != 0
    }

    /// Whether the drive has ended.
    pub fn ended(&self) -> bool {
        let s = self.state.lock().unwrap();
        s.terminal.is_some() || s.lost.is_some()
    }

    /// The frames from attempt `from_attempt` on, from the first.
    pub fn reader(self: &Arc<Self>, from_attempt: u32) -> DriveReader {
        DriveReader { drive: self.clone(), cursor: 0, from_attempt, done: false, caller: None }
    }

    /// The endpoint keys of the callers reading this drive's frames now: the calls this Build's run
    /// answers. A stop of this admin that the run commands spares their connections, as every stop
    /// spares the call it answers on (node-stop.md "Transport cut").
    pub fn callers(&self) -> Vec<iroh::PublicKey> {
        self.callers.lock().unwrap().clone()
    }

    /// Resolves once this drive has ended and no caller reads its frames: every caller took its
    /// terminal.
    pub async fn until_read(&self) {
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.ended() && self.callers.lock().unwrap().is_empty() {
                return;
            }
            notified.await;
        }
    }


    /// Whether a stall reason is new (the drive names a stall once, not on every pass).
    fn stall_is_new(&self, reason: &str) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.stall.as_deref() == Some(reason) {
            return false;
        }
        s.stall = Some(reason.to_string());
        true
    }

    fn clear_stall(&self) {
        self.state.lock().unwrap().stall = None;
    }

    fn idle(&self) -> Option<Idle> {
        self.state.lock().unwrap().idle.clone()
    }

    fn set_idle(&self, idle: Option<Idle>) {
        self.state.lock().unwrap().idle = idle;
    }

    /// Whether this drive holds the `Complete` frame of the operation `key` in an attempt `counts`
    /// admits: an executor's stream reply (a `Step` frame goes out only once its receipt is durable on
    /// the executor that wrote it) or a receipt read said the operation completed.
    fn completed_in(&self, key: &str, counts: impl Fn(u32) -> bool) -> bool {
        self.state.lock().unwrap().seen.iter().any(|k| {
            let mut parts = k.splitn(4, '|');
            matches!(
                (parts.next(), parts.next().and_then(|a| a.parse::<u32>().ok()), parts.next(), parts.next()),
                (Some("step"), Some(attempt), Some(operation), Some("Complete")) if counts(attempt) && operation == key
            )
        })
    }
}

/// What a caller reads from a drive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    /// A progress frame.
    Frame(BuildReply),
    /// The terminal frame.
    Terminal(BuildReply),
    /// The fabric-primary moved; the stream ends `FabricPrimaryMoved`, and the caller re-submits.
    SeatLost(SeatLoss),
}

/// A caller's read position in a [`Drive`].
pub struct DriveReader {
    drive: Arc<Drive>,
    cursor: usize,
    from_attempt: u32,
    done: bool,
    /// The caller this reader streams to, named on its drive while it reads ([`Drive::callers`]).
    caller: Option<iroh::PublicKey>,
}

impl Drop for DriveReader {
    fn drop(&mut self) {
        if let Some(key) = self.caller.take() {
            let mut callers = self.drive.callers.lock().unwrap();
            if let Some(i) = callers.iter().position(|k| *k == key) {
                callers.remove(i);
            }
            drop(callers);
            self.drive.wake.notify_waiters();
        }
    }
}

impl DriveReader {
    /// This reader as the one of the caller at `caller` (its endpoint key), named on its drive until
    /// the reader is dropped.
    pub fn of_caller(mut self, caller: Option<iroh::PublicKey>) -> Self {
        if let Some(key) = caller {
            self.drive.callers.lock().unwrap().push(key);
            self.caller = Some(key);
        }
        self
    }

    /// The Build this reader follows.
    pub fn build_id(&self) -> &BuildId {
        &self.drive.build_id
    }

    /// The next frame, the terminal, or the seat loss; `None` after the end.
    pub async fn next(&mut self) -> Option<Read> {
        if self.done {
            return None;
        }
        loop {
            let notified = self.drive.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let s = self.drive.state.lock().unwrap();
                while let Some(frame) = s.events.get(self.cursor) {
                    self.cursor += 1;
                    if frame_attempt(frame) >= self.from_attempt {
                        return Some(Read::Frame(frame.clone()));
                    }
                }
                if let Some(t) = &s.terminal {
                    self.done = true;
                    return Some(Read::Terminal(t.clone()));
                }
                if let Some(loss) = &s.lost {
                    self.done = true;
                    return Some(Read::SeatLost(loss.clone()));
                }
            }
            notified.await;
        }
    }
}

/// How a node.primary.update that did not complete ended ([`Gate::update_primaries`]), as
/// lifecycles/node-primary-update.md's failure contract tells its branches apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeatMoveEnd {
    /// Refused before the call: "refused by name, nothing changed, the Build's step fails by name".
    Refused(String),
    /// Not answered by node.primary.updated: the end is uncertain, and the next attempt discovers what
    /// the update came to from its view.
    Incomplete(String),
    /// The successor is unheard after the update, node investigation's verdict (ruling R-PU2, Q10): the
    /// step ends `Failed { step, reason }`, the Build is Failed (R-BI4), the election names whom it
    /// names, and the workflow never goes on to the primary's operation.
    Failed {
        /// The step that failed (`primary-update`).
        step: String,
        /// The failure reason recording the five facts.
        reason: String,
        /// The original failed terminal code.
        code: rdm_core::outcome::Code,
    },
}

impl std::fmt::Display for SeatMoveEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(why) | Self::Incomplete(why) => f.write_str(why),
            Self::Failed { step, reason, .. } => write!(f, "{step}: {reason}"),
        }
    }
}

/// Where a Build a fabric.delete interrupts stands ([`DriveEnv::end_interrupted`]).
enum Interrupted {
    /// Its attempts ended and its terminal is `Failed` at `attempt`, for `reason`.
    Ended { attempt: u32, reason: String },
    /// Its last admitted operation completed it at `attempt`.
    Converged { attempt: u32 },
    /// An attempt of it still runs, named.
    Open(String),
}

/// How one pass of a drive ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriveEnd {
    /// The Build reached a terminal: nothing more to drive.
    Terminal,
    /// The drive cannot go on now; the next pass of the reconcile loop tries again.
    Stalled(String),
    /// This admin does not hold the seat: the callers' streams end.
    SeatLost(String),
}

/// The application's notices of a Build's openings and endings (fabric-node-hooks.md
/// `on_build_started`, `on_build_ended`; build.md §4.1, Q256), as the fabric-primary sends them: each
/// fire and forget, after the claim or the terminal it follows is recorded, gating nothing and writing
/// no receipt. [`Default`]: the application registered none.
#[derive(Clone, Default)]
pub struct BuildNotices {
    fabric_id: Option<rdm_core::FabricId>,
    on_build_started: Option<crate::wiring::BuildStartedNotice>,
    on_build_ended: Option<crate::wiring::BuildEndedNotice>,
}

impl BuildNotices {
    /// The notices of fabric `fabric_id` the application registered in `hooks`.
    pub fn of(fabric_id: rdm_core::FabricId, hooks: &crate::wiring::FabricHooks) -> Self {
        Self { fabric_id: Some(fabric_id), on_build_started: hooks.on_build_started.clone(), on_build_ended: hooks.on_build_ended.clone() }
    }

    /// `on_build_started` of attempt `attempt` of `build_id`, whose claim opened the Build, under its
    /// own span (`rdm.node_admin.build.update.via-on-build-started`) parented to the attempt's context.
    fn started(&self, me: &PathName, contexts: &Arc<crate::build_claim::AttemptContexts>, build_id: &BuildId, attempt: u32, reason: crate::build_state::AttemptReason, context: &CallContext) {
        let hook = self.fabric_id.clone().zip(self.on_build_started.clone());
        let span = tracing::info_span!(parent: None, "rdm.node_admin.build.update.via-on-build-started", node = %me, build_id = %build_id.log(), attempt, reason = reason.as_str(), outcome = if hook.is_some() { "sent" } else { "no-hook" }, "otel.kind" = "internal");
        if let Some(tp) = &context.traceparent {
            rdm_transport::set_remote_parent(&span, tp, context.tracestate.as_deref());
        }
        let (contexts, build_id) = (contexts.clone(), build_id.clone());
        tokio::spawn(
            async move {
                let Some((fabric_id, hook)) = hook else {
                    tracing::info!("no on_build_started hook is registered");
                    return;
                };
                let context = contexts.get(&build_id, attempt).await.ok().flatten().unwrap_or_default();
                hook(crate::wiring::BuildStartedCtx { fabric_id, build_id: build_id.to_string(), attempt, reason, context }).await;
                tracing::info!("the application was told the Build started");
            }
            .instrument(span),
        );
    }

    /// `on_build_ended` of attempt `attempt` of `build_id`, whose terminal `state` is recorded, under its
    /// own span (`rdm.node_admin.build.update.via-on-build-ended`) in the trace of `parent`, the attempt's
    /// span: parented through its trace context, never as a local child, so a notice not yet sent holds
    /// the attempt's span open for nothing.
    fn ended(&self, me: &PathName, contexts: &Arc<crate::build_claim::AttemptContexts>, build_id: &BuildId, attempt: u32, reason: crate::build_state::AttemptReason, state: crate::wiring::BuildEndedState, parent: &tracing::Span) {
        let hook = self.fabric_id.clone().zip(self.on_build_ended.clone());
        let span = tracing::info_span!(parent: None, "rdm.node_admin.build.update.via-on-build-ended", node = %me, build_id = %build_id.log(), attempt, reason = reason.as_str(), state = state.as_str(), outcome = if hook.is_some() { "sent" } else { "no-hook" }, "otel.kind" = "internal");
        let under = parent.in_scope(crate::build_claim::current_context);
        if let Some(tp) = &under.traceparent {
            rdm_transport::set_remote_parent(&span, tp, under.tracestate.as_deref());
        }
        let (contexts, build_id) = (contexts.clone(), build_id.clone());
        tokio::spawn(
            async move {
                let Some((fabric_id, hook)) = hook else {
                    tracing::info!("no on_build_ended hook is registered");
                    return;
                };
                let context = contexts.get(&build_id, attempt).await.ok().flatten().unwrap_or_default();
                hook(crate::wiring::BuildEndedCtx { fabric_id, build_id: build_id.to_string(), attempt, reason, state, context }).await;
                tracing::info!("the application was told the Build ended");
            }
            .instrument(span),
        );
    }
}

/// The ended state of the terminal `t`: `Failed` naming its step, reason and code; `Complete` otherwise.
fn ended_state(t: &BuildReply) -> crate::wiring::BuildEndedState {
    match t {
        BuildReply::Failed { step, reason, code, .. } => crate::wiring::BuildEndedState::Failed { step: step.clone(), reason: reason.clone(), code: *code },
        _ => crate::wiring::BuildEndedState::Complete,
    }
}

/// What the fabric-primary drives a Build with.
pub struct DriveEnv {
    /// This admin's `path.name`.
    pub me: PathName,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// The Build state.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// The claim door: the one place an attempt's claim is decided.
    pub door: Arc<ClaimDoor>,
    /// Calls executors.
    pub dispatcher: Arc<dyn Dispatcher>,
    /// The cut-off rule and the seat target.
    pub gate: Arc<dyn Gate>,
    /// Proof of an executor's departure.
    pub departure: Arc<dyn DepartureProof>,
    /// Records an executor's verdict on this admin's log.
    pub verdicts: Arc<dyn VerdictSink>,
    /// The application's Build notices this fabric-primary sends (fabric-node-hooks.md, Q256).
    pub notices: BuildNotices,
    /// The admin's stop gate (`crate::stop_gate`): a drive takes the view lock and calls executors,
    /// so each of its passes is a turn begun at the gate's wait point, and a stop's quiesce waits for
    /// the pass in progress.
    pub turns: crate::stop_gate::StopGate,
}

/// The drives this admin holds, by Build.
#[derive(Default)]
pub struct Drives {
    drives: Mutex<HashMap<BuildId, Arc<Drive>>>,
    /// The task making each drive's frames, by Build, and whether this admin drives Builds no longer
    /// ([`Self::stop`]): one lock, so no drive's task starts after the stop.
    tasks: Mutex<(HashMap<BuildId, tokio::task::JoinHandle<()>>, bool)>,
    /// Moves each time a drive's task ends its Build ([`DriveEnd::Terminal`]): what the reconcile loop
    /// waits on, whose drift check opens nothing while a Build is in progress.
    ended: tokio::sync::watch::Sender<u64>,
}

impl Drives {
    /// The drives' ends as they happen: a waiter arms on it to see a Build ended by its drive.
    pub fn subscribe_ended(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ended.subscribe()
    }

    /// The drive of `build_id` for `attempt` (the attempt opened, or
    /// [`BuildProjection::attempt_to_drive`] of the caller's read), with a task making its frames
    /// running: the one held, or a new one. A drive that stalled or paused is picked up again here.
    /// A drive that completed the Build at `attempt` or later is the record of that run and is handed
    /// back as it is, with nothing run: a caller whose read predates the verdict (the reconcile
    /// loop's `list_active`, a re-submit's `read_build`, showing that attempt running) gets the
    /// drive that ended it, never a second one in its place. A drive that ended otherwise (it
    /// completed an earlier attempt and the Build was reopened, it failed, its seat was lost) is
    /// replaced by a new one.
    pub fn ensure(self: &Arc<Self>, env: &Arc<DriveEnv>, build_id: &BuildId, attempt: u32) -> Arc<Drive> {
        let drive = {
            let mut drives = self.drives.lock().unwrap();
            match drives.get(build_id) {
                Some(d) if d.completed_attempt().is_some_and(|done| done >= attempt) => return d.clone(),
                Some(d) if d.ended() => {
                    drives.remove(build_id);
                }
                _ => {}
            }
            drives.entry(build_id.clone()).or_insert_with(|| Drive::new(build_id.clone())).clone()
        };
        let mut tasks = self.tasks.lock().unwrap();
        if tasks.1 {
            return drive;
        }
        if !drive.running.swap(true, Ordering::SeqCst) {
            let (env, d, drives) = (env.clone(), drive.clone(), self.clone());
            let parent = tracing::Span::current();
            let task = tokio::spawn(
                async move {
                    let end = loop {
                        d.again.store(false, Ordering::SeqCst);
                        let end = env.run(&d).await;
                        // An ensure that found this run in progress (an event's pass, a submit) is run
                        // now: the run may have read the world before that event and ended waiting on
                        // it. Decided under the tasks' lock, which every ensure decides under.
                        let again = {
                            let tasks = drives.tasks.lock().unwrap();
                            let again = matches!(end, DriveEnd::Stalled(_)) && !tasks.1 && d.again.swap(false, Ordering::SeqCst);
                            if !again {
                                d.running.store(false, Ordering::SeqCst);
                            }
                            again
                        };
                        if !again {
                            break end;
                        }
                    };
                    // The Build's end is what the drift check waits on: the reconcile loop runs its next
                    // pass on it.
                    if end == DriveEnd::Terminal {
                        drives.ended.send_modify(|v| *v += 1);
                    }
                    // A Build that completed keeps its drive: the frames it recorded are what a
                    // re-submit is answered from.
                    if matches!(end, DriveEnd::Terminal | DriveEnd::SeatLost(_)) && !d.completed() {
                        let mut held = drives.drives.lock().unwrap();
                        if held.get(&d.build_id).is_some_and(|h| Arc::ptr_eq(h, &d)) {
                            held.remove(&d.build_id);
                        }
                    }
                }
                .instrument(parent)
                .with_current_subscriber(),
            );
            tasks.0.insert(drive.build_id.clone(), task);
        } else {
            // Its run is in progress and may have read the world before what this ensure was asked
            // for: it runs once more when it ends waiting.
            drive.again.store(true, Ordering::SeqCst);
        }
        drive
    }

    /// This admin drives Builds no longer (`Running::stop_reconciling`): every drive's task ends where
    /// it is, and none starts after this, so no attempt of any Build begins here. It returns once each
    /// task it ended has ended (an abort lands at the task's next await, so a task in the middle of a
    /// poll runs that poll to its end first). Returns how many drives' tasks were ended.
    pub async fn stop(&self) -> usize {
        let ending: Vec<tokio::task::JoinHandle<()>> = {
            let mut tasks = self.tasks.lock().unwrap();
            tasks.1 = true;
            tasks.0.drain().map(|(_, t)| t).filter(|t| !t.is_finished()).collect()
        };
        let ended = ending.len();
        for t in &ending {
            t.abort();
        }
        for t in ending {
            let _ = t.await;
        }
        tracing::info_span!("rdm.node_admin.build.update.via-reconciling-stopped", drives = ended as u64)
            .in_scope(|| tracing::info!("this admin drives no Build from now on: every drive's task is ended and none starts"));
        ended
    }

    /// The drive a re-submit of a Build that ended at `attempt` is answered from: the drive this
    /// admin kept that completed it, else the one running, else a new one (a fabric-primary that
    /// took the seat after the Build ended, whose drive attaches to the last attempt's executor).
    pub fn replay_of(self: &Arc<Self>, env: &Arc<DriveEnv>, build_id: &BuildId, attempt: u32) -> Arc<Drive> {
        self.ensure(env, build_id, attempt)
    }

    /// The Build left history: its drive's record goes with it.
    pub fn forget(&self, build_id: &BuildId) {
        self.drives.lock().unwrap().remove(build_id);
    }

    /// The Builds whose drive this admin holds and has not ended.
    pub fn unended(&self) -> Vec<BuildId> {
        self.drives.lock().unwrap().iter().filter(|(_, d)| !d.ended()).map(|(id, _)| id.clone()).collect()
    }

    /// The drive of `build_id` when one is held (running, stalled, paused or completed).
    pub fn get(&self, build_id: &BuildId) -> Option<Arc<Drive>> {
        self.drives.lock().unwrap().get(build_id).cloned()
    }

    /// The endpoint keys of the callers reading `build_id`'s frames now ([`Drive::callers`]).
    pub fn callers_of(&self, build_id: &BuildId) -> Vec<iroh::PublicKey> {
        self.get(build_id).map(|d| d.callers()).unwrap_or_default()
    }

    /// [`Drive::until_read`] of `build_id`'s drive; at once when this admin holds none.
    pub async fn until_read(&self, build_id: &BuildId) {
        if let Some(d) = self.get(build_id) {
            d.until_read().await;
        }
    }
}

fn parse_path(s: &str) -> Option<PathName> {
    s.parse().ok()
}

/// The node an operation key names (`node.create:mesh2.admin.1`), when it names one.
fn node_of_key(key: &str) -> Option<PathName> {
    key.split_once(':').and_then(|(_, node)| parse_path(node))
}

/// What is left of `p` in view `t`: the plan, less every operation a run of the Build's current run
/// completed whose executor reported the run's end ([`BuildProjection::reported_in_run`]), by its
/// `Complete` receipt on this admin's log ([`BuildProjection::completed_in_run`]) or by the
/// `Complete` frame the executor's stream reply carried to `drive` ([`Drive::completed_in`]). What a
/// run completed is read from those, never from the view: a view that has not absorbed a completed
/// operation yet still lists it, and it is not planned, claimed or dispatched again. The stream reply
/// is the authority: a remote executor's receipt reaches this admin's log from the store root, and
/// nothing waits for that copy. A lost executor's run reported nothing: what it left is planned from
/// the view again, and the create pipeline's target decides against the world.
/// Whether an attempt ended incomplete: `me` ran it as its executor and it was handed back to `me`
/// having moved nothing. A hand-off to `me` that another executor reported is that executor's end,
/// and the drive attaching to the finished run of it replays the same terminal.
fn ended_incomplete(me: &PathName, executor: &PathName, to: &PathName, before: &str, after: &str) -> bool {
    executor == me && to == me && before == after
}

/// Whether a `Step` frame's reported receipt is a failed step's.
fn failed_receipt(receipt: Option<&[u8]>) -> bool {
    receipt.and_then(|b| serde_json::from_slice::<crate::build_state::BuildStepReceipt>(b).ok()).is_some_and(|r| matches!(r.outcome, crate::build_state::StepOutcome::Failed { .. }))
}

/// What the next claim of a Build decides on: the first operation left, the receipts the Build
/// holds, the members of the mesh that operation acts on, by birth, status and primary, and whether the
/// fabric holds ready (`fabric_ready`, [`Gate::fabric_ready`]: a Build that put the fabric
/// back to pending is claimed again once it does). An attempt that ended with this unchanged would end
/// the same way again.
fn world(drive: &Drive, p: &BuildProjection, t: &Topology, fabric_ready: bool) -> String {
    let first = left(drive, p, t).into_iter().next();
    let mesh = first.as_ref().and_then(|op| match op {
        BuildOperation::CreateMesh { mesh, .. } | BuildOperation::ShutdownMesh { mesh, .. } => Some(mesh.clone()),
        other => other.node().map(|n| n.mesh.clone()),
    });
    let mut members: Vec<String> = t
        .members()
        .filter(|n| Some(&n.mesh) == mesh.as_ref())
        .map(|n| format!("{}:{}:{:?}:{}", n.name, n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("-"), n.status, n.is_primary))
        .collect();
    members.sort();
    format!("{} receipts={} members=[{}] fabric_ready={fabric_ready}", first.map(|op| op.key()).unwrap_or_default(), p.steps.len(), members.join(","))
}

fn left(drive: &Drive, p: &BuildProjection, t: &Topology) -> Vec<BuildOperation> {
    // A mesh the Mesh records hold Dead is planned as Dead, whatever the view shows ([`Gate::dead_meshes`]).
    let dead = drive.dead.lock().unwrap().clone();
    let with_dead;
    let t = if t.meshes.iter().any(|m| m.status != crate::model::ScopeStatus::Dead && m.id.as_ref().is_some_and(|id| dead.contains(id))) {
        let mut v = t.clone();
        for m in v.meshes.iter_mut().filter(|m| m.id.as_ref().is_some_and(|id| dead.contains(id))) {
            m.status = crate::model::ScopeStatus::Dead;
        }
        with_dead = v;
        &with_dead
    } else {
        t
    };
    // A replacement plans from what the rows recorded at this pass ([`Gate::recorded_executables`]).
    // A fabric.delete plans every mesh the store holds a row of beside the view's ([`Gate::mesh_rows`]).
    let stored = drive.mesh_rows.lock().unwrap().as_ref().map(|rows| crate::accepted::with_mesh_rows(t, rows));
    let t = stored.as_ref().unwrap_or(t);
    // A standalone action plans from the birth the atlas row held at this pass ([`Gate::atlas_birth`]).
    let atlas = drive.atlas.lock().unwrap().clone();
    let plan = crate::accepted::plan_for_build_with(p, t, drive.recorded.lock().unwrap().as_ref(), atlas.as_ref());
    let sections: HashSet<String> = drive.section_done.lock().unwrap().iter().filter(|(run, _)| *run == p.opened).map(|(_, key)| key.clone()).collect();
    plan.operations.into_iter().filter(|op| !p.completed_in_run(&op.key()) && !drive.completed_in(&op.key(), |a| p.reported_in_run(a)) && !sections.contains(&op.key())).collect()
}

/// Whether `p` is a Build that owes the replacements its rows say are left (a complete-topology
/// request's, proposal executable-bindings-at-create.md rule 5): its rows are read at every pass.
fn plans_replacements(p: &BuildProjection) -> bool {
    p.submitted_change.as_ref().is_some_and(crate::accepted::TopologyChange::plans_replacements)
}

/// Whether `p`'s Build changes the fabric to ready (states.md "Resize and executable
/// replacement": "Its Build is Complete only when the fabric is ready again from this Build's
/// own change, as fabric.create's is"; Q154): the Day-0 Build, fabric.create's, which carries no submitted
/// change and whose fabric is pending from its mint (fabric-create.md, Completion: "`fabric.created` when
/// the fabric is ready"), and every Build a change was submitted for, a resize and an
/// executable replacement alike, except fabric.delete, after which the fabric is never ready
/// again. A Build whose topology the change left as it was puts nothing back, and the fabric it finds
/// ready holds it.
fn changes_the_fabric(p: &BuildProjection) -> bool {
    !p.submitted_change.as_ref().is_some_and(|c| matches!(c, crate::accepted::TopologyChange::DeleteFabric))
}

/// Whether `p`'s attempt waits for the fabric's ready before its Build is Complete: the
/// Build's own change of the fabric, Day 0's or a submitted topology change's (Q154; states.md "Resize and
/// executable replacement"), up to the Build's
/// first Complete, which comes only from its own ready. Every later attempt of the Build
/// (node.stop, node.start, a primary change, node recovery, drift: attempts of an
/// unchanged topology, build.md §1) ends at its own completion and never waits on the fabric's change
/// (states.md "The check-in checklist": "Individual lifecycle status changes do not wait for the entire
/// check-in checklist").
fn waits_for_the_fabric_change(p: &BuildProjection) -> bool {
    changes_the_fabric(p) && p.completed_at.is_none()
}

/// The node-admin of `mesh` the Build already carried across the R-D1 target: an operation on it has a
/// step receipt in an attempt held by an admin outside `mesh`, later than the last attempt an admin of
/// `mesh` held (a mesh that has executed its own work since stands on its own again). That admin is
/// the mesh's first recovering node-admin (mesh-investigation.md step 6: the restore runs on the SAME
/// accepted Build), read from the Build's facts whichever drive dispatched it and whichever admin
/// outside the mesh held its attempt.
pub(crate) fn crossed_admin(p: &BuildProjection, mesh: &str) -> Option<PathName> {
    let holder = |attempt: u32| p.attempt_executors.get(&attempt).and_then(|e| parse_path(e));
    let own = own_attempt(p, mesh);
    let mut steps: Vec<&crate::build_state::BuildStepReceipt> = p.steps.iter().filter(|s| s.attempt > own).collect();
    steps.sort_by_key(|s| s.attempt);
    steps.into_iter().find_map(|s| {
        let node = node_of_key(&s.operation).filter(|n| n.kind == NodeKind::NodeAdmin && n.mesh == mesh)?;
        holder(s.attempt).filter(|h| h.mesh != mesh).map(|_| node)
    })
}

/// The last attempt of the Build an admin of `mesh` held; `0` when none did. A completed attempt of
/// the mesh's own lane its own admin ran (a section, Q250) is the mesh's own work at the fabric attempt
/// the Build stood at when it was claimed.
fn own_attempt(p: &BuildProjection, mesh: &str) -> u32 {
    let fabric = p.attempt_executors.iter().filter(|(_, e)| parse_path(e).is_some_and(|e| e.mesh == mesh)).map(|(a, _)| *a).max().unwrap_or(0);
    let lane = p.lanes.get(mesh).filter(|l| l.state == BuildState::Complete && l.attempt > 0 && l.executor.as_deref().and_then(parse_path).is_some_and(|e| e.mesh == mesh)).map(|l| l.at_fabric_attempt).unwrap_or(0);
    fabric.max(lane)
}

/// The node-admin the carrier-edge-lost verdict of `mesh` still owes a node.create (mesh-investigation.md
/// step 6), when the investigation decided the mesh's recovery: the node-admin the Build already carried
/// across the target ([`crossed_admin`]) until its create completed since the mesh last executed its own
/// work, and `first` (the mesh's first node-admin in path order) while the Build carried none.
pub(crate) fn first_admin_owed(p: &BuildProjection, mesh: &str, first: &PathName) -> Option<PathName> {
    let Some(crossed) = crossed_admin(p, mesh) else { return Some(first.clone()) };
    let (own, key) = (own_attempt(p, mesh), format!("node.create:{crossed}"));
    let created = p.steps.iter().any(|s| s.attempt > own && s.operation == key && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete);
    (!created).then_some(crossed)
}

enum Inner {
    Terminal(BuildReply),
    Refused(BuildReply),
    Unreached(String),
    Broke(String),
    /// The Build log's target was set while the executor's run was open: this admin is no longer the
    /// fabric-primary. The drive ends there without waiting for the run's stream, except for the
    /// executor the target names, whose stream ends `FabricPrimaryMoved` once it is fabric-primary.
    SeatYielded,
}

impl DriveEnv {
    /// This admin's currently held `path.name`: what its Build state or its view names its birth,
    /// falling back to `self.me` before its birth is in the view.
    pub async fn held_name(&self) -> PathName {
        if let Some(held) = self.builds.held_name() {
            return held;
        }
        self.topology.read().await.held_name_of(&self.me, Some(&self.door.node_id))
    }

    /// Drive `drive`'s Build until it ends, stalls or loses the seat.
    pub async fn run(self: &Arc<Self>, drive: &Arc<Drive>) -> DriveEnd {
        let id = drive.build_id.clone();
        let me = self.held_name().await;
        let span = tracing::info_span!("rdm.node_admin.build.update.via-drive", build_id = %id.log(), node = %me, outcome = tracing::field::Empty, detail = tracing::field::Empty, dispatches = tracing::field::Empty);
        let mut dispatches = 0u32;
        let mut last_turn = None;
        let end = self.pass(drive, &mut dispatches, &mut last_turn).instrument(span.clone()).await;
        span.record("dispatches", dispatches);
        match &end {
            DriveEnd::Terminal => span.record("outcome", "terminal"),
            DriveEnd::Stalled(w) => span.record("outcome", "stalled").record("detail", w.as_str()),
            DriveEnd::SeatLost(w) => span.record("outcome", "seat-lost").record("detail", w.as_str()),
        };
        drop(span);
        drop(last_turn);
        end
    }

    /// The fabric-primary and the target: `Some` when this admin may not claim or dispatch now. The
    /// target is read first: the election's result this admin installed names the fabric-primary every
    /// decision is judged against (lifecycles/node-primary-update.md step 4: the successor "becomes
    /// primary" when it installs it).
    async fn may_act(&self, drive: &Arc<Drive>) -> Option<DriveEnd> {
        let me = self.held_name().await;
        let record_holder = match self.gate.target().await {
            Ok(holder) => holder,
            Err(e) => {
                let why = e.to_string();
                // The record names the holder (its path.name from the digest view); none only when no
                // digest of that birth is held, and the reason says so.
                let to = match e {
                    BuildStateError::SeatYielded { to, .. } => to,
                    _ => None,
                };
                tracing::info_span!("rdm.node_admin.build.reject.via-fabric-primary-changed", build_id = %drive.build_id.log(), node = %me, seat_yielded = true, detail = %why, to = to.as_deref().unwrap_or("none")).in_scope(|| tracing::info!("dispatch refused: the seat record names another holder"));
                drive.lose_fabric_primary(SeatLoss { why: why.clone(), to });
                return Some(DriveEnd::SeatLost(why));
            }
        };
        // The seat is this admin's when the view's holder is the birth it was born as (its node id):
        // another birth at the same path.name (a successor, a deleted birth) is not this admin.
        let (held, seat) = {
            let t = self.topology.read().await;
            (t.is_fabric_primary(&self.door.node_id), t.fabric_primary().map(|n| (n.name.clone(), n.node_id.clone())))
        };
        if !held {
            // Past the target the installed result names this birth, none, or a birth proven gone
            // (no fabric-primary).
            let shows = seat.as_ref().map(|(name, id)| format!("{name} ({id})")).unwrap_or_else(|| "none".into());
            let names = record_holder.as_deref().unwrap_or("none");
            let why = format!("{me} ({}) does not hold the fabric-primary seat in its view (it shows {shows}); the fabric-primary record it holds names {names}", self.door.node_id);
            tracing::info_span!("rdm.node_admin.build.reject.via-not-fabric-primary", build_id = %drive.build_id.log(), node = %me, detail = %why, to = names).in_scope(|| tracing::info!("dispatch refused: the seat is not held"));
            drive.lose_fabric_primary(SeatLoss { why: why.clone(), to: record_holder });
            return Some(DriveEnd::SeatLost(why));
        }
        None
    }

    /// `Some` when the Build's current attempt is past `attempt` and was claimed by another admin: the end
    /// of `attempt` decides nothing, and the drive ends `SeatLost` naming that claimant, the fabric-primary
    /// that decided the claim of the current fabric-lane attempt ([`BuildProjection::claimant`]). A Build
    /// that cannot be read decides nothing here: the end is handled as before.
    async fn superseded(&self, me: &PathName, drive: &Arc<Drive>, id: &BuildId, attempt: u32) -> Option<DriveEnd> {
        let p = self.builds.read_build(id).await.ok()?;
        let claimant = p.claimant.clone().or_else(|| p.executor.clone());
        if p.attempt <= attempt || claimant.as_deref() == Some(me.to_string().as_str()) {
            return None;
        }
        let to = claimant.as_deref().unwrap_or("none");
        let why = format!("{me}: attempt {attempt} of Build {id} ended after {to} claimed attempt {}: the Build's current attempt is not this admin's, and the end of attempt {attempt} decides nothing", p.attempt);
        tracing::info_span!("rdm.node_admin.build.reject.via-superseded-attempt", build_id = %id.log(), attempt, current_attempt = p.attempt, node = %me, to = %to, detail = %why)
            .in_scope(|| tracing::info!("the end of a superseded attempt is not recorded: the drive stops on its read of the Build"));
        drive.lose_fabric_primary(SeatLoss { why: why.clone(), to: claimant });
        Some(DriveEnd::SeatLost(why))
    }

    fn stalled(&self, me: &PathName, drive: &Arc<Drive>, why: String) -> DriveEnd {
        if drive.stall_is_new(&why) {
            tracing::info_span!("rdm.node_admin.build.update.via-stalled", build_id = %drive.build_id.log(), node = %me, reason = %why).in_scope(|| tracing::info!("the drive cannot go on until the world changes"));
        }
        DriveEnd::Stalled(why)
    }

    /// The end of the fabric lane's `attempt` (its terminal, or its take-over from a proven-exited
    /// executor) releases the in-flight row it took (R-BI7 caveat 3), while the row still names that
    /// attempt: its executor's mesh's, and, for a node recovery, the recovered node's mesh's, which the
    /// recovery takes whoever executes it (`ControlPlane`'s recovery claim). A release that cannot be
    /// written is named: the mesh stays held, and the next claim on it is refused naming this attempt.
    async fn release_mesh(&self, id: &BuildId, attempt: u32, executor: &PathName) {
        let me = self.held_name().await;
        let row = crate::mesh_claims::InFlight { build_id: id.clone(), lane: None, attempt, claimant: me.to_string(), claimant_node: None };
        let others: Vec<String> = self.topology.read().await.meshes.iter().map(|m| m.name.clone()).filter(|m| *m != executor.mesh).collect();
        let mut taken = vec![executor.mesh.clone()];
        for mesh in others {
            if self.door.claims.in_flight(&mesh).await.ok().flatten().is_some_and(|held| held.same_attempt(&row)) {
                taken.push(mesh);
            }
        }
        for mesh in taken {
            if let Err(e) = self.door.claims.release_in_flight(&mesh, &row).await {
                tracing::info_span!("rdm.node_admin.build.reject.via-in-flight-unreleased", build_id = %id.log(), attempt, mesh = %mesh, node = %me, error = %e)
                    .in_scope(|| tracing::info!("the attempt ended and its mesh's in-flight row could not be released: the mesh stays held by it"));
            }
        }
    }

    /// The fabric's change the Build waits on (states.md "Resize and executable replacement": "while the
    /// fabric changes, the Build reports Blocked naming the change"): framed once for a new reason. Every
    /// other wait of the drive is named on its span, never framed to the caller (node-rpc-envelope.md:
    /// Blocked's only source is the subject's Status `Blocked` reply to the executor's ProbeNodeState).
    fn blocked(&self, drive: &Arc<Drive>, attempt: u32, operation: &str, step: &str, reason: &str) {
        drive.push(BuildReply::Blocked { build_id: drive.build_id.to_string(), attempt, operation: operation.into(), step: step.into(), reason: crate::build_run::bounded_reason(reason.into()) });
    }

    /// What the peer-mesh investigation holds of the mesh of `op`'s node when that mesh is a peer
    /// mesh it holds unheard (R-D1: loss of peer-mesh control-plane visibility targets lifecycle
    /// mutation for every node in that mesh): the mesh, how long it is unheard, and whether its
    /// recovery is decided. `None` for a mesh operation, a node of this admin's own mesh, or a mesh
    /// the investigation does not hold unheard.
    fn unheard_peer(&self, op: &BuildOperation) -> Option<(PathName, u64, bool)> {
        let node = op.node()?;
        if node.mesh == self.me.mesh {
            return None;
        }
        let peer = self.gate.peer_mesh(&node.mesh)?;
        Some((node.clone(), peer.unheard_ms?, peer.recovery_decided))
    }

    /// The restore the investigation's recovery verdict authorizes (R-D1; mesh-investigation.md steps
    /// 5-6; Q221): `op` is the decision step's operation on a node-admin of a peer mesh held unheard, the
    /// node.create of that node-admin on a decided recovery (carrier-edge-lost) or on the fully crashed
    /// mesh's exited verdict ([`Self::crash_admits`]), and the Build carried no other admin of that mesh
    /// across the target ([`crossed_admin`]); no other operation of the mesh is exempt. The
    /// fabric-primary executes it, whatever else the mesh's view holds (an admin held silent
    /// included): it is the mesh's first recovering node-admin.
    fn recovery_admits(&self, p: &BuildProjection, op: &BuildOperation, crashed: &HashSet<String>) -> bool {
        mesh_primary_recovery_admits(p, op)
            || self.unheard_peer(op).is_some_and(|(node, _, decided)| ((decided && op.created_node() == Some(&node)) || self.crash_admits(p, op, &node, crashed)) && node.kind == NodeKind::NodeAdmin && crossed_admin(p, &node.mesh).is_none_or(|crossed| crossed == node))
    }

    /// The node.create of the node-admin a mesh no member of which answers is owed (mesh-investigation.md
    /// step 5, "No member answers"): `op` creates `node`, the mesh's first node-admin in path order (or
    /// the one the Build already carried across the target, [`first_admin_owed`]), and `crashed` (this
    /// pass's [`Self::unanswered_meshes`]) holds its mesh: the investigation's 60 s decision on it came
    /// out `unreachable`. Nothing else of that mesh crosses the target, and the create's own target decides
    /// the birth it finds at the path.
    fn crash_admits(&self, p: &BuildProjection, op: &BuildOperation, node: &PathName, crashed: &HashSet<String>) -> bool {
        op.created_node() == Some(node)
            && crashed.contains(&node.mesh)
            && p.topology.meshes.get(&node.mesh).and_then(|m| m.nodes_of_type(NodeKind::NodeAdmin).min()).and_then(|first| first_admin_owed(p, &node.mesh, first)).as_ref() == Some(node)
    }

    /// The peer meshes of `p` no member of which answers (mesh-investigation.md step 5, "No member
    /// answers"): each mesh the investigation decided `unreachable`. Its first node-admin's create may be
    /// admitted on that verdict (Q111); the create's own target decides the birth at the path.
    fn unanswered_meshes(&self, p: &BuildProjection) -> HashSet<String> {
        p.topology.meshes.values().filter(|m| m.name != self.me.mesh && !m.nodes.is_empty() && self.gate.peer_mesh(&m.name).is_some_and(|pm| pm.unreachable_decided)).map(|m| m.name.clone()).collect()
    }

    /// The target on `op` (R-D1): a node of a peer mesh held unheard is not claimed or dispatched for,
    /// except the mesh's first recovering node-admin once the investigation decided its recovery
    /// ([`Self::recovery_admits`]). `Some` names the
    /// target.
    fn peer_mesh_held(&self, p: &BuildProjection, op: &BuildOperation, crashed: &HashSet<String>) -> Option<(PathName, u64, bool)> {
        if p.topology.meshes.is_empty() || self.recovery_admits(p, op, crashed) {
            return None;
        }
        self.unheard_peer(op)
    }

    /// The drive stops at the target on `op` by name: once per mesh, operation and verdict on its own
    /// span, never framed to the caller, and stalled until the investigation's state changes.
    fn stop_at_peer_mesh_held(&self, me: &PathName, drive: &Arc<Drive>, attempt: u32, op: &BuildOperation, (node, unheard_ms, decided): (PathName, u64, bool), crashed: &HashSet<String>) -> DriveEnd {
        let unreachable = self.gate.peer_mesh(&node.mesh).is_some_and(|m| m.unreachable_decided);
        let why = format!(
            "{me}: {} acts on {node} of peer mesh {mesh}, which the peer-mesh investigation holds unheard ({unheard_ms} ms){}: nothing is claimed or dispatched for that mesh until it is heard again (R-D1)",
            op.key(),
            if decided {
                "; its decided recovery admits only the mesh's first recovering node-admin"
            } else if unreachable && crashed.contains(&node.mesh) {
                "; its decision is unreachable: no member answers, which admits only the create of the mesh's first node-admin"
            } else if unreachable {
                "; its decision is unreachable and the accepted topology names no node of it, so nothing of it is admitted"
            } else {
                ", with no recovery decided"
            },
            mesh = node.mesh,
        );
        if drive.stall_is_new(&format!("unheard-mesh|{}|{}|{decided}", node.mesh, op.key())) {
            tracing::info_span!("rdm.node_admin.build.reject.via-unheard-mesh-drive", build_id = %drive.build_id.log(), attempt, node = %me, operation = %op.key(), target = %node, mesh = %node.mesh, unheard_ms, recovery_decided = decided)
                .in_scope(|| tracing::info!("the drive claims and dispatches nothing for a node of a peer mesh held unheard"));
        }
        DriveEnd::Stalled(why)
    }

    /// What this admin dispatches to `executor` for what is left of `p` in view `t` ([`left`]). The
    /// mesh's first recovering node-admin under a recovery verdict runs alone, on this admin (the
    /// fabric-primary); its receipts are what target every other admin of that mesh. Otherwise the executor's run
    /// ([`run_of`]) is cut before the first operation on a peer mesh held unheard (R-D1) and before
    /// an operation no admin executes in this view (it waits for its mesh's own seat, never for the
    /// run's executor); a cut run hands back to this admin, whose next claim re-reads the view and
    /// the investigation. `Err` names the target on the first operation. With the plan, whether the run
    /// hands back at the hold for the fabric's change (Q154): the state it was planned on, which decides
    /// how its hand-back is read, whatever the fabric's state when the run ends.
    fn plan_dispatch(&self, me: &PathName, drive: &Drive, p: &BuildProjection, t: &Topology, executor: &PathName, crashed: &HashSet<String>) -> Result<(RunPlan, bool), (BuildOperation, (PathName, u64, bool))> {
        let ops = left(drive, p, t);
        if let Some(op) = ops.first().filter(|op| *executor == *me && self.recovery_admits(p, op, crashed)) {
            return Ok((RunPlan { operations: vec![op.clone()], hand_off_to: (ops.len() > 1).then(|| me.to_string()) }, false));
        }
        let mut plan = run_of(&ops, t, executor);
        let mut cut = None;
        for (i, op) in plan.operations.iter().enumerate() {
            if let Some(f) = self.peer_mesh_held(p, op, crashed) {
                if i == 0 {
                    return Err((op.clone(), f));
                }
                cut = Some(i);
                break;
            }
            if i > 0 && ((self.unheard_peer(op).is_some() && !p.topology.meshes.is_empty()) || crate::executor::executor_for(op, t).is_none()) {
                cut = Some(i);
                break;
            }
            if p.topology.meshes.is_empty() && matches!(op, BuildOperation::ShutdownMesh { .. }) && plan.operations.len() > i + 1 {
                cut = Some(i + 1);
                break;
            }
        }
        if let Some(i) = cut {
            plan.operations.truncate(i);
            plan.hand_off_to = Some(me.to_string());
        }
        // The last run of a Build that changes the fabric (Day 0's, or one that put it back to pending) hands back here rather than
        // ending `Complete` while the fabric's ready state is not set (Q154; states.md "Resize
        // and executable replacement"): the next pass waits for it. A later attempt of a Build that completed
        // once ends at its own completion ([`waits_for_the_fabric_change`]).
        let held = waits_for_the_fabric_change(p) && plan.hand_off_to.is_none() && !self.gate.fabric_ready();
        if held {
            plan.hand_off_to = Some(me.to_string());
        }
        Ok((plan, held))
    }

    async fn pass(self: &Arc<Self>, drive: &Arc<Drive>, dispatches: &mut u32, last_turn: &mut Option<crate::stop_gate::Turn>) -> DriveEnd {
        let id = drive.build_id.clone();
        let mut handed_to: Option<PathName> = None;
        let mut deleted: Option<String> = None;
        loop {
            // Each pass is one turn of the admin's gate (stop_gate.rs): it begins here, holding
            // nothing, and while the admin is stopped it begins only once the admin starts. A pass in
            // progress when a stop holds the gate runs to its end (the attempt it dispatched, its
            // terminal handled) and the stop's quiesce waits for it, so nothing of it follows the cut.
            let _ = last_turn.take();
            // Of its Build: a stop of this admin that the Build's own run commands does not wait for
            // the pass that waits on that run (stop_gate.rs).
            let turn = last_turn.insert(self.turns.turn("build-drive").await.of_build(&id.to_string()));
            if let Some(end) = self.may_act(drive).await {
                return end;
            }
            let me = self.held_name().await;
            let p = match self.builds.read_build(&id).await {
                Ok(p) => p,
                Err(e) => return self.stalled(&me, drive, format!("{me}: Build {id} could not be read: {e}")),
            };
            if p.state == BuildState::Complete {
                if !drive.has_run_log() {
                    // This drive made no call of the Build: a fabric-primary that took over after
                    // the Build ended holds no frame of it. The executor of the last attempt replays
                    // it from the receipts it wrote itself, and the facts of earlier attempts are
                    // read; the step facts on the store root are never the answer for the last attempt.
                    if let Some(end) = self.reattach_to_ended(&me, &turn, drive, &p).await {
                        return end;
                    }
                }
                // The attempt that completed the Build holds its executor's mesh no longer (a
                // fabric-primary that took the seat after the verdict releases what its predecessor
                // could not).
                if let Some(executor) = p.executor.as_deref().and_then(parse_path) {
                    self.release_mesh(&id, p.attempt, &executor).await;
                }
                drive.finish(BuildReply::Complete { build_id: id.to_string(), attempt: p.attempt });
                return DriveEnd::Terminal;
            }
            match crate::build_state::drivable(&*self.builds, &p).await {
                Ok(true) => {}
                Ok(false) => {
                    // A fabric.delete accepted after this Build interrupts it (build.md §1): its run receives
                    // `Failed` naming the delete once its attempts ended, and nothing of it runs again.
                    if let Ok(Some(delete)) = crate::build_state::interrupted_by(&*self.builds, &p).await {
                        return self.end_of_interrupted(&me, drive, &p, &delete).await;
                    }
                    let current = self.builds.current_build().await.ok().and_then(|c| c.into_build_id()).map(|c| c.to_string()).unwrap_or_else(|| "none".into());
                    return self.stalled(&me, drive, format!("{me}: Build {id} ({:?} at attempt {}, completed {:?}) is neither the current Build ({current}) nor a failed Build accepted after it: nothing drives it", p.state, p.attempt, p.completed_at.is_some()));
                }
                Err(e) => return self.stalled(&me, drive, format!("{me}: the current Build could not be read to admit Build {id}: {e}")),
            }
            // A replacement's remaining work is the members whose row records another sha than their
            // entry names (build-deployment.md §3): the rows are read at every pass, before it plans.
            if plans_replacements(&p) {
                match self.gate.recorded_executables(&p.topology).await {
                    Ok(recorded) => *drive.recorded.lock().unwrap() = Some(recorded),
                    Err(e) => return self.stalled(&me, drive, format!("{me}: Build {id} plans replacements and the node rows could not be read for what each member runs: {e}")),
                }
            }
            // A mesh the Mesh records hold Dead is done whatever the view shows (a view that has not heard
            // its Dead yet): the plan reads mesh death from the records at every pass, before it plans.
            let observed = self.topology.read().await.clone();
            match self.gate.dead_meshes(&observed).await {
                Ok(dead) => *drive.dead.lock().unwrap() = dead,
                Err(e) => return self.stalled(&me, drive, format!("{me}: Build {id}: the Mesh records could not be read for which meshes are Dead: {e}")),
            }
            // A fabric.delete's remaining meshes are the Mesh rows on the store (fabric-delete.md "Failure";
            // mesh-delete.md "Resume after the primary's loss"): read at every pass, before it plans.
            if p.deletes_fabric() {
                match self.gate.mesh_rows().await {
                    Ok(rows) => *drive.mesh_rows.lock().unwrap() = Some(rows),
                    Err(e) => return self.stalled(&me, drive, format!("{me}: Build {id} is a fabric.delete and the Mesh rows could not be read for the meshes it deletes: {e}")),
                }
            }
            // A standalone stop, start, connections delete or delete plans from the birth the atlas row
            // holds at its path (fabric-atlas.md R-FA1, R-FA3): read at every pass, before it plans.
            let atlas = match crate::accepted::action_path(&p) {
                Some(path) => match self.gate.atlas_birth(&path, &observed).await {
                    Ok(birth) => birth,
                    Err(e) => return self.stalled(&me, drive, format!("{me}: Build {id}: the atlas row at {path} could not be read for its action's birth: {e}")),
                },
                None => None,
            };
            *drive.atlas.lock().unwrap() = atlas;
            let crashed = self.unanswered_meshes(&p);
            // The claim's holder was proven exited by this pass: the next attempt is claimed without
            // calling the lost holder again.
            let take_over = deleted.as_deref().is_some_and(|d| p.executor.as_deref() == Some(d));
            let (attempt, executor, context) = if p.state == BuildState::Running && !take_over {
                let holder = p.executor.clone().unwrap_or_default();
                let Some(holder) = parse_path(&holder) else {
                    return self.stalled(&me, drive, format!("{me}: attempt {} of Build {id} is claimed by {holder:?}, which is not a path.name", p.attempt));
                };
                (p.attempt, holder, self.door.contexts.get(&id, p.attempt).await.ok().flatten().unwrap_or_default())
            } else {
                // The teardown opens once every open attempt of the Builds it interrupts has ended and
                // each of them has its terminal (build.md §1).
                if p.deletes_fabric() {
                    if let Some(end) = self.interrupted_ended(&me, drive, &p).await {
                        return end;
                    }
                }
                if let Some(end) = self.idle_until_moved(&me, drive, &p).await {
                    return end;
                }
                // A Build that changes the fabric (Day 0's, or one that put it back to pending) and never completed, its
                // work all done, is Complete only once the fabric's ready state is set from its change (Q154; states.md "Resize and
                // executable replacement": "while the fabric changes, the Build reports Blocked naming the
                // change, never Complete"): no further attempt is claimed until the fabric holds
                // ready, and the reconcile loop drives the Build again. Its first attempt is
                // always claimed, even with nothing left to execute (build.md §1: every attempt "computes
                // remaining work"): the fabric's change names the accepted Build's attempt
                // (states.md, "The fabric's ready"), and that attempt's run hands back at the hold of `plan_dispatch`.
                // Waiting is not failure.
                if p.attempt > 0 && waits_for_the_fabric_change(&p) && !self.gate.fabric_ready() && left(drive, &p, &self.topology.read().await.clone()).is_empty() {
                    let why = format!("{me}: every operation of Build {id} is done, and the Build is Complete only once the fabric's ready state is set");
                    self.blocked(drive, p.attempt, "", "fabric-change", &why);
                    return self.stalled(&me, drive, why);
                }
                match self.update_primaries_first(&me, drive, &p).await {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(end) => return end,
                }
                // A taken-over attempt ends with its proven-exited executor: its hold on that
                // executor's mesh goes before the next attempt takes the mesh.
                if take_over {
                    if let Some(lost) = p.executor.as_deref().and_then(parse_path) {
                        self.release_mesh(&id, p.attempt, &lost).await;
                    }
                }
                // The meshes whose own mesh-primary executes what is left of them are sent their sections,
                // all at once (Q250); the fabric lane then plans what is left again.
                match self.run_sections(&me, &turn, drive, &p).await {
                    Sections::None => {}
                    Sections::Ran => continue,
                    Sections::End(end) => return end,
                }
                match self.claim_next(&me, drive, &p, handed_to.take(), &crashed).await {
                    Ok(c) => c,
                    Err(end) => return end,
                }
            };
            // The claim may have taken time: the gates again, immediately before the dispatch.
            if let Some(end) = self.may_act(drive).await {
                return end;
            }
            // An attempt that takes over from an earlier one of this drive: the frames of the steps
            // the earlier attempts completed are read from the receipts held (a frame the lost
            // executor never sent is not lost), and never run again.
            // A drive that dispatches for the first time (a fabric-primary that took the seat
            // mid-Build) holds no frame of any earlier attempt: every receipt before this attempt
            // is read, as for a Build that ended (`reattach_to_ended`).
            let fresh = !drive.has_run_log();
            let first = drive.note_first_attempt(attempt);
            self.replay(drive, &p.steps, if fresh { 0 } else { first }, attempt).await;
            let (plan, held) = {
                let t = self.topology.read().await.clone();
                match self.plan_dispatch(&me, drive, &p, &t, &executor, &crashed) {
                    Ok(plan) => plan,
                    Err((op, f)) => return self.stop_at_peer_mesh_held(&me, drive, attempt, &op, f, &crashed),
                }
            };
            // A node.primary.update that precedes an operation of this run is acknowledged first,
            // whether this admin ran it, resumes after another admin's, or is its successor:
            // node.primary.updated answered to this executor, never the view alone
            // (node-primary-update.md failure contract), in the claim's attempt and in a re-dispatch.
            if let Some(end) = self.updates_acknowledged(&me, drive, &plan, attempt, p.run_of(attempt)).await {
                return end;
            }
            *dispatches += 1;
            let dspan = tracing::info_span!("rdm.node_admin.build.update.via-dispatch", build_id = %id.log(), attempt, executor = %executor, node = %me, outcome = tracing::field::Empty, reason = tracing::field::Empty);
            if let Some(tp) = &context.traceparent {
                rdm_transport::set_remote_parent(&dspan, tp, context.tracestate.as_deref());
            }
            // What the attempt decides on, as claimed: an attempt handed back to this admin with this
            // unchanged would end the same way again.
            let before = match self.builds.read_build(&id).await {
                Ok(claimed) => Some(world(drive, &claimed, &self.topology.read().await.clone(), self.gate.fabric_ready())),
                Err(_) => None,
            };
            let inner = self.run_inner(&turn, drive, &id, attempt, &executor, context, plan).instrument(dspan.clone()).await;
            drive.clear_stall();
            // The end of a run is decided only for the Build's current attempt, the folded maximum/winning
            // claim (build.md §5.2): the Build is read before anything of the end is written (Q109). A later
            // attempt another admin claimed while this one ran supersedes it: its end decides nothing, and
            // this admin stops on that read (fabric-recovery.md step 4, Q160).
            if matches!(inner, Inner::Terminal(_)) {
                if let Some(end) = self.superseded(&me, drive, &id, attempt).await {
                    dspan.record("outcome", "superseded");
                    return end;
                }
            }
            match inner {
                Inner::Terminal(t @ BuildReply::Complete { .. }) => {
                    dspan.record("outcome", "complete");
                    self.verdict(&me, &executor, BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }, None).await;
                    self.release_mesh(&id, attempt, &executor).await;
                    self.gate.attempt_completed(&id, attempt).await;
                    // The Build's Complete is recorded: the ending's last step (Q256).
                    self.notices.ended(&me, &self.door.contexts, &id, attempt, p.reason, crate::wiring::BuildEndedState::Complete, &dspan);
                    drive.finish(t);
                    return DriveEnd::Terminal;
                }
                Inner::Terminal(t @ BuildReply::Failed { .. }) => {
                    dspan.record("outcome", "failed");
                    if let BuildReply::Failed { operation, step, reason, code, .. } = &t {
                        let receipt = BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Failed { reason: reason.clone(), code: *code } };
                        if executor != me {
                            self.verdict(&me, &executor, receipt, Some((operation, step, reason, *code))).await;
                        } else if !operation.is_empty() && !reason.contains("(reason cut:") {
                            self.verdicts.record(receipt).await;
                        }
                    }
                    self.release_mesh(&id, attempt, &executor).await;
                    self.gate.attempt_failed(&id, attempt).await;
                    // The Build's Failed is recorded: the ending's last step (Q256).
                    self.notices.ended(&me, &self.door.contexts, &id, attempt, p.reason, ended_state(&t), &dspan);
                    drive.finish(t);
                    return DriveEnd::Terminal;
                }
                Inner::Terminal(BuildReply::HandedOff { to, .. }) => {
                    dspan.record("outcome", "handed-off");
                    self.verdict(&me, &executor, BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::HandedOff { to: to.clone() } }, None).await;
                    self.release_mesh(&id, attempt, &executor).await;
                    let Some(next) = parse_path(&to) else {
                        return self.stalled(&me, drive, format!("attempt {attempt} of Build {id} handed off to {to:?}, which is not a path.name"));
                    };
                    // An attempt handed back to this admin that moved nothing (it ended incomplete: a
                    // participant lost, a proof not in) would be claimed again and end the same way: the
                    // Build stays in progress, and the next attempt waits for what it decides on to move.
                    // The wait is named on its span, never framed to the caller (node-rpc-envelope.md:
                    // Blocked's only source is the subject's Status `Blocked`).
                    if next == me {
                        if let (Some(before), Ok(ended)) = (before, self.builds.read_build(&id).await) {
                            let t = self.topology.read().await.clone();
                            let after = world(drive, &ended, &t, self.gate.fabric_ready());
                            // A run with nothing left, handed back by the hold of a Build that put the
                            // fabric back to pending ([`Self::plan_dispatch`]), did not end incomplete: it
                            // waits on the fabric's change, and the next pass reports it Blocked naming it,
                            // or claims the next attempt once the fabric is ready. The hold is
                            // the one the run was planned with, never the fabric's state read after it.
                            let held_for_the_fabric_change = held && left(drive, &ended, &t).is_empty();
                            if !held_for_the_fabric_change && ended_incomplete(&me, &executor, &next, &before, &after) {
                                let why = format!(
                                    "{me}: attempt {attempt} of Build {id} ended incomplete and was handed back to this fabric-primary with nothing moved ({after}): the Build stays in progress and no next attempt is claimed until what it decides on moves"
                                );
                                drive.set_idle(Some(Idle { attempt, world: after, why: why.clone() }));
                                return self.stalled(&me, drive, why);
                            }
                        }
                    }
                    handed_to = Some(next);
                    deleted = None;
                }
                Inner::Terminal(other) => {
                    dspan.record("outcome", "protocol-violation");
                    return self.stalled(&me, drive, format!("{executor} ended attempt {attempt} with {}, which is not a terminal of the attempt", other.name()));
                }
                Inner::Refused(r) => {
                    dspan.record("outcome", r.name());
                    let why = format!("{executor} refused attempt {attempt} of Build {id}: {}", describe(&r));
                    return self.stalled(&me, drive, why);
                }
                Inner::SeatYielded => {
                    dspan.record("outcome", "seat-yielded");
                    // The target is kept: `may_act` ends the drive SeatLost naming the new holder.
                    return match self.may_act(drive).await {
                        Some(end) => end,
                        None => self.stalled(&me, drive, format!("{me}: attempt {attempt} of Build {id} stopped at a seat target that the gate no longer reports")),
                    };
                }
                Inner::Unreached(why) | Inner::Broke(why) => {
                    let (class, detail) = ("lost", why);
                    dspan.record("outcome", "executor-lost");
                    // Why the inner call to the executor ended: what the next step (the departure proof) answers.
                    dspan.record("reason", detail.as_str());
                    // Only a proven departure lets another admin take the attempt; silence never does.
                    if deleted.is_some() {
                        return self.stalled(&me, drive, format!("{executor} was proven exited and the attempt claimed after it could not be dispatched either: {detail}"));
                    }
                    match self.departure.proven(&executor).await {
                        Ok(()) => {
                            tracing::info_span!("rdm.node_admin.build.update.via-departure-proven", build_id = %id.log(), attempt, executor = %executor, node = %me)
                                .in_scope(|| tracing::info!("the executor's exact runtime is proven exited or os-stopped: the next attempt may be claimed"));
                            deleted = Some(executor.to_string());
                        }
                        Err(not_proven) => {
                            let why = format!("executor {executor} is {class} ({detail}); its departure is not proven ({not_proven}); the attempt is not claimed for another admin");
                            return self.stalled(&me, drive, why);
                        }
                    }
                }
            }
        }
    }

    /// The run of `p`, which the fabric.delete `delete` interrupts, ends at the Build's terminal: `Failed`
    /// naming the delete once its attempts ended ([`Self::end_interrupted`]), or the `Complete` its last
    /// admitted operation reached. While an attempt of it still runs, the drive stalls naming it.
    async fn end_of_interrupted(&self, me: &PathName, drive: &Arc<Drive>, p: &BuildProjection, delete: &BuildProjection) -> DriveEnd {
        match self.end_interrupted(me, p, delete).await {
            Interrupted::Ended { attempt, reason } => {
                drive.finish(rdm_core::reply::build::failed(p.build_id.to_string(), attempt, String::new(), "fabric.delete".into(), crate::build_run::bounded_reason(reason), rdm_core::outcome::Code::Aborted));
                DriveEnd::Terminal
            }
            Interrupted::Converged { attempt } => {
                drive.finish(BuildReply::Complete { build_id: p.build_id.to_string(), attempt });
                DriveEnd::Terminal
            }
            Interrupted::Open(why) => self.stalled(me, drive, why),
        }
    }

    /// The fabric.delete `delete` opens its teardown only once every Build it interrupts has its
    /// terminal (build.md §1): `Some` stalls the delete's drive, naming the attempt it waits for on its span.
    async fn interrupted_ended(&self, me: &PathName, drive: &Arc<Drive>, delete: &BuildProjection) -> Option<DriveEnd> {
        let active = match self.builds.list_active().await {
            Ok(active) => active,
            Err(e) => return Some(self.stalled(me, drive, format!("{me}: the Builds fabric.delete {} interrupts could not be read: {e}", delete.build_id))),
        };
        for p in active.iter().filter(|b| (b.submitted_at_ms, &b.build_id) < (delete.submitted_at_ms, &delete.build_id)) {
            if let Interrupted::Open(why) = self.end_interrupted(me, p, delete).await {
                return Some(self.stalled(me, drive, why));
            }
        }
        None
    }

    /// Where `p`, which the fabric.delete `delete` interrupts, stands, and its terminal written once its
    /// attempts ended (build.md §1). An attempt running at its executor ends within its bound: the run
    /// stops at its next operation and hands back, and one whose executor deleted ends on that
    /// departure's exact proof, the attempt failing then. With no attempt running, this fabric-primary
    /// claims the next attempt for itself and ends it `Failed` naming the delete, running nothing: the
    /// Build's terminal, after which nothing reconciles it again.
    async fn end_interrupted(&self, me: &PathName, p: &BuildProjection, delete: &BuildProjection) -> Interrupted {
        let id = &p.build_id;
        let p = match self.builds.read_build(id).await {
            Ok(p) => p,
            Err(e) => return Interrupted::Open(format!("{me}: Build {id}, which fabric.delete {} interrupts, could not be read: {e}", delete.build_id)),
        };
        match p.state {
            BuildState::Complete => return Interrupted::Converged { attempt: p.attempt },
            BuildState::Failed => return Interrupted::Ended { attempt: p.failed_at.unwrap_or(p.attempt), reason: p.last_failure.clone().unwrap_or_default() },
            BuildState::Pending | BuildState::Running => {}
        }
        if let Some((mesh, lane)) = p.lanes.iter().find(|(_, l)| l.state == BuildState::Running) {
            return Interrupted::Open(format!(
                "{me}: attempt {} of mesh {mesh}'s drift lane of Build {id}, which fabric.delete {} interrupts, runs at {}: the teardown waits for it to end",
                lane.attempt,
                delete.build_id,
                lane.executor.as_deref().unwrap_or("no recorded executor")
            ));
        }
        let span = tracing::info_span!("rdm.node_admin.build.update.via-interrupted-terminal", build_id = %id.log(), delete_build_id = %delete.build_id.log(), node = %me, attempt = tracing::field::Empty, executor = tracing::field::Empty);
        let attempt = if p.state == BuildState::Running {
            let Some(executor) = p.executor.as_deref().and_then(parse_path) else {
                return Interrupted::Open(format!("{me}: attempt {} of Build {id} is claimed by {:?}, which is not a path.name", p.attempt, p.executor));
            };
            // An attempt this fabric-primary executes is in this process: its runtime is the one running the
            // delete, whose exit is never proven while it runs it. With no run of the attempt live here, it
            // ends the attempt itself now, its verdict written through the held run (no later verdict of
            // a run of it follows); a live run stops at its next operation and hands back (build.md §1).
            if executor == *me {
                let Some(held) = self.dispatcher.hold(me, id, p.attempt) else {
                    return Interrupted::Open(format!(
                        "{me}: attempt {} of Build {id}, which fabric.delete {} interrupts, runs in this process: the teardown waits for its run to stop at its next operation",
                        p.attempt, delete.build_id
                    ));
                };
                span.record("executor", executor.to_string().as_str());
                span.record("attempt", p.attempt);
                let reason = crate::build_state::interrupted_reason(id, &delete.build_id);
                let terminal = rdm_core::reply::build::failed(id.to_string(), p.attempt, String::new(), "fabric.delete".into(), crate::build_run::bounded_reason(reason.clone()), rdm_core::outcome::Code::Aborted);
                let state = ended_state(&terminal);
                held.finish(terminal, self.release_mesh(id, p.attempt, &executor)).instrument(span.clone()).await;
                return match self.builds.read_build(id).await {
                    Ok(ended) if ended.state == BuildState::Failed && ended.failed_at == Some(p.attempt) => {
                        span.in_scope(|| tracing::info!(%reason, "the interrupted Build's attempt ran at this fabric-primary with no run live here: it ends Failed naming the fabric.delete"));
                        // The Build's Failed is recorded: the ending's last step (Q256).
                        self.notices.ended(me, &self.door.contexts, id, p.attempt, p.reason, state, &span);
                        Interrupted::Ended { attempt: p.attempt, reason }
                    }
                    Ok(other) => Interrupted::Open(format!("{me}: the terminal of attempt {} of Build {id}, which fabric.delete {} interrupts, was not recorded: the Build reads {:?} at attempt {}", p.attempt, delete.build_id, other.state, other.attempt)),
                    Err(e) => Interrupted::Open(format!("{me}: Build {id}, which fabric.delete {} interrupts, could not be read after its terminal: {e}", delete.build_id)),
                };
            }
            if let Err(not_proven) = self.departure.proven(&executor).await {
                return Interrupted::Open(format!(
                    "{me}: attempt {} of Build {id}, which fabric.delete {} interrupts, runs at {executor} ({not_proven}): the teardown waits for it to end within its bound",
                    p.attempt, delete.build_id
                ));
            }
            span.record("executor", executor.to_string().as_str());
            p.attempt
        } else {
            let claim = crate::build_state::BuildAttemptClaim { build_id: id.clone(), attempt: p.attempt + 1, executor: me.to_string(), claimant: Some(me.to_string()), mesh: None };
            match self.builds.claim_attempt(&claim).await {
                Ok(crate::build_state::ClaimOutcome::Won) => p.attempt + 1,
                Ok(other) => return Interrupted::Open(format!("{me}: attempt {} of Build {id}, which fabric.delete {} interrupts, could not be taken to end it: {other:?}", p.attempt + 1, delete.build_id)),
                Err(e) => return Interrupted::Open(format!("{me}: attempt {} of Build {id}, which fabric.delete {} interrupts, could not be taken to end it: {e}", p.attempt + 1, delete.build_id)),
            }
        };
        span.record("attempt", attempt);
        let reason = crate::build_state::interrupted_reason(id, &delete.build_id);
        let verdict = BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Failed { reason: reason.clone(), code: rdm_core::outcome::Code::Aborted } };
        if let Err(e) = self.builds.append_attempt_receipt(&verdict).await {
            return Interrupted::Open(format!("{me}: the terminal of Build {id}, which fabric.delete {} interrupts, could not be written: {e}", delete.build_id));
        }
        // The ended attempt holds its executor's mesh no longer (R-BI7 caveat 3).
        if let Some(executor) = p.executor.as_deref().and_then(parse_path) {
            let row = crate::mesh_claims::InFlight { build_id: id.clone(), lane: None, attempt: p.attempt, claimant: me.to_string(), claimant_node: None };
            if let Err(e) = self.door.claims.release_in_flight(&executor.mesh, &row).await {
                span.in_scope(|| tracing::info!(error = %e, mesh = %executor.mesh, "the ended attempt's in-flight row could not be released: the mesh stays held by it"));
            }
        }
        span.in_scope(|| tracing::info!(%reason, "the interrupted Build's attempts ended: its run ends Failed naming the fabric.delete"));
        // The Build's Failed is recorded: the ending's last step (Q256). An attempt claimed here only to
        // end it opened nothing, and sends no on_build_started.
        let state = crate::wiring::BuildEndedState::Failed { step: "fabric.delete".into(), reason: crate::build_run::bounded_reason(reason.clone()), code: rdm_core::outcome::Code::Aborted };
        self.notices.ended(me, &self.door.contexts, id, attempt, p.reason, state, &span);
        Interrupted::Ended { attempt, reason }
    }

    /// Every node.primary.update that precedes an operation of `plan`, acknowledged through the gate
    /// ([`Gate::primaries_updated`]); `Some` ends the drive stalled, naming the updates and why on its
    /// span, when one was not.
    async fn updates_acknowledged(&self, me: &PathName, drive: &Arc<Drive>, plan: &RunPlan, attempt: u32, run: u32) -> Option<DriveEnd> {
        let mut updates: Vec<(String, PrecedingUpdate)> = plan.operations.iter().flat_map(|op| preceding_updates(op, &drive.build_id, run).into_iter().map(move |u| (op.key(), u))).collect();
        updates.dedup();
        if updates.is_empty() {
            return None;
        }
        let asked: Vec<PrecedingUpdate> = updates.into_iter().map(|(_, u)| u).collect();
        let why = self.gate.primaries_updated(&asked, &drive.build_id, attempt).await.err()?;
        let names = asked.iter().map(|u| format!("{} under mesh {}", u.operation, u.mesh)).collect::<Vec<_>>().join(", ");
        let why = format!("{me}: attempt {attempt} of Build {} dispatches nothing until node.primary.updated is acknowledged for the update that precedes it (one of {names}): {why}", drive.build_id);
        Some(self.stalled(me, drive, why))
    }

    /// The seats the first operation left moves before it runs (R-MT1, [`crate::executor::seat_move`],
    /// proposal Part C1 step 1: "The holder is commanded from above, as a step of the Build", Q10:
    /// "The FP claims the attempt at the claim door BEFORE it commands"): attempt `attempt` is claimed
    /// for this fabric-primary first and its run is held here while node.primary.update runs (whether
    /// this admin moves its own primaries or commands another mesh-primary's), so a successor that is
    /// fabric-primary from its answer reattaches to this attempt and goes on only once it ends (step 6:
    /// "The commanding Build continues on the successor's direct acknowledgement"; build.md: "A live run
    /// is reattached, never re-claimed"). Refused before the call or ended `Failed { step, reason }`
    /// after it (ruling R-PU2, Q10): the attempt ends `Failed` and the Build is Failed (R-BI4).
    /// Every other end hands the attempt to the view's fabric-primary, and when the seat moved
    /// (`Ok(true)`) the loop passes the gates again.
    async fn update_primaries_first(&self, me: &PathName, drive: &Arc<Drive>, p: &BuildProjection) -> Result<bool, DriveEnd> {
        let t = self.topology.read().await.clone();
        let Some(op) = left(drive, p, &t).into_iter().next() else { return Ok(false) };
        let Some(m) = crate::executor::seat_move(&op, &t) else { return Ok(false) };
        let attempt = p.attempt + 1;
        // The update's transfer key: the first attempt of the run this attempt belongs to, so the
        // next run a resubmit opens makes a new transfer (ruling R-PU2, Q9).
        let run = p.run_of(attempt);
        let id = &drive.build_id;
        let context = match self.door.claim(&me.to_string(), id, attempt).await {
            BuildClaimReply::Won { context } => {
                self.started_if_opening(me, p, attempt, &context).await;
                context
            }
            other => {
                let why = format!("{me}: the seats of {} move before {} as a step of attempt {attempt} of Build {id}, and that attempt was not claimed for {me}: {}", m.holder.name, op.key(), claim_refusal(&other));
                return Err(self.stalled(me, drive, why));
            }
        };
        let held = self.dispatcher.hold(me, id, attempt);
        let dspan = tracing::info_span!("rdm.node_admin.build.update.via-dispatch", build_id = %id.log(), attempt, executor = %me, node = %me, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        if let Some(tp) = &context.traceparent {
            rdm_transport::set_remote_parent(&dspan, tp, context.tracestate.as_deref());
        }
        let moved = self.gate.update_primaries(&m, id, attempt, run, &op.key()).instrument(dspan.clone()).await;
        let after = self.topology.read().await.clone();
        let still = still_holds(&op, &m, &after);
        let to = match after.fabric_primary() {
            Some(fp) if !still => fp.name.clone(),
            _ => me.clone(),
        };
        let failed = match &moved {
            Err(SeatMoveEnd::Refused(why)) => Some(("primary-update".to_string(), format!("{me}: the seats of {} did not move before {}: node.primary.update was refused before its row: {why}", m.holder.name, op.key()), rdm_core::outcome::Code::FailedPrecondition)),
            Err(SeatMoveEnd::Failed { step, reason, code }) => Some((step.clone(), reason.clone(), *code)),
            _ => None,
        };
        let terminal = match &failed {
            Some((step, why, code)) => rdm_core::reply::build::failed(id.to_string(), attempt, op.key(), step.clone(), crate::build_run::bounded_reason(why.clone()), *code),
            None => BuildReply::HandedOff { build_id: id.to_string(), attempt, to: to.to_string() },
        };
        // The verdict's writer releases the mesh's in-flight row the claim took (R-BI7 caveat 3), at
        // the verdict and before a reader of the held run learns the attempt ended: a successor that
        // reads the verdict claims its next attempt on the same mesh rows.
        let outcome = match &terminal {
            BuildReply::Failed { reason, code, .. } => AttemptOutcome::Failed { reason: reason.clone(), code: *code },
            _ => AttemptOutcome::HandedOff { to: to.to_string() },
        };
        let recorded = match held {
            Some(held) => {
                held.finish(terminal.clone(), self.release_mesh(id, attempt, me)).await;
                true
            }
            None => match self.builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt, outcome, mesh: None }).await {
                Ok(()) => {
                    self.release_mesh(id, attempt, me).await;
                    true
                }
                Err(e) => {
                    dspan.record("reason", format!("the verdict of attempt {attempt} could not be recorded: {e}").as_str());
                    false
                }
            },
        };
        if let Some((_, why, _)) = failed {
            dspan.record("outcome", "failed").record("reason", why.as_str());
            // The Build's Failed is recorded: the ending's last step (Q256).
            if recorded {
                self.notices.ended(me, &self.door.contexts, id, attempt, p.reason, ended_state(&terminal), &dspan);
            }
            drive.finish(terminal);
            return Err(DriveEnd::Terminal);
        }
        dspan.record("outcome", "handed-off");
        match (moved, still) {
            (Ok(()), false) => Ok(true),
            (Err(why), false) => {
                // The update ended after its call: the drive goes on to the gates, which end it where
                // the election's installed result names another fabric-primary.
                let why = format!("{me}: node.primary.update to {to} before {} did not complete: {why}", op.key());
                dspan.record("reason", why.as_str());
                Ok(true)
            }
            (moved, true) => {
                let why = match moved {
                    Err(why) => format!("{me}: the seat of {} did not move before {}: {why}", m.holder.name, op.key()),
                    Ok(()) => format!("{me}: node.primary.update of {} answered, and this view still names it the holder of the seat {} moves", m.holder.name, op.key()),
                };
                dspan.record("reason", why.as_str());
                if let Ok(ended) = self.builds.read_build(id).await {
                    drive.set_idle(Some(Idle { attempt, world: world(drive, &ended, &after, self.gate.fabric_ready()), why: why.clone() }));
                }
                Err(self.stalled(me, drive, why))
            }
        }
    }

    /// The drive waits, named, while the attempt it handed back to itself with nothing moved has not
    /// seen what its resume decides on move ([`world`]); the stall is the same reason, named once.
    async fn idle_until_moved(&self, me: &PathName, drive: &Arc<Drive>, p: &BuildProjection) -> Option<DriveEnd> {
        let idle = drive.idle()?;
        if idle.attempt != p.attempt {
            drive.set_idle(None);
            return None;
        }
        let now = world(drive, p, &self.topology.read().await.clone(), self.gate.fabric_ready());
        if now == idle.world {
            return Some(self.stalled(me, drive, idle.why));
        }
        drive.set_idle(None);
        None
    }

    /// `on_build_started` of `attempt` of `p`, just claimed here, when that claim opened the Build
    /// (fabric-node-hooks.md `on_build_started`, Q256): its first attempt, or an attempt after one that
    /// ended the Build (`Complete` or `Failed`: a drift repair, a stop, a start, a caller's resubmit). An
    /// attempt that continues a Build in progress (a takeover, the next attempt after a hand-back) sends none.
    async fn started_if_opening(&self, me: &PathName, p: &BuildProjection, attempt: u32, context: &CallContext) {
        let opening = attempt == 1
            || matches!(p.state, BuildState::Failed)
            || (p.opened == attempt && self.builds.facts().await.is_ok_and(|facts| {
                facts
                    .iter()
                    .find_map(|f| match f {
                        crate::build_state::BuildFact::Attempt(a) if a.build_id == p.build_id && a.mesh.is_none() && a.attempt == p.attempt => Some(matches!(a.outcome, AttemptOutcome::Converged | AttemptOutcome::Failed { .. })),
                        _ => None,
                    })
                    .unwrap_or(false)
            }));
        if opening {
            self.notices.started(me, &self.door.contexts, &p.build_id, attempt, p.reason, context);
        }
    }

    /// Claim the next attempt of `p` for the admin that runs what is left (or `forced`, the admin a
    /// run just handed off to), on this admin's own log.
    async fn claim_next(&self, me: &PathName, drive: &Arc<Drive>, p: &BuildProjection, forced: Option<PathName>, crashed: &HashSet<String>) -> Result<(u32, PathName, CallContext), DriveEnd> {
        let id = &drive.build_id;
        let attempt = p.attempt + 1;
        let view = self.topology.read().await.clone();
        let first = left(drive, p, &view).into_iter().next();
        if let Some((op, f)) = first.as_ref().and_then(|op| self.peer_mesh_held(p, op, crashed).map(|f| (op, f))) {
            return Err(self.stop_at_peer_mesh_held(me, drive, attempt, op, f, crashed));
        }
        // The mesh's first recovering node-admin under the investigation's recovery verdict is this
        // admin's to execute (R-D1). A run handed back to this admin (its own cut, or an executor whose
        // view names this admin) is claimed for the admin this view names for what is left: an
        // operation no admin executes waits for its mesh's own seat, and one another admin executes is
        // claimed for that admin, never for this one to hand on unrun.
        let recover = first.as_ref().is_some_and(|op| self.recovery_admits(p, op, crashed));
        // The create admitted for a mesh no member of which answers (not the ladder's decided recovery,
        // which the ladder told when it decided): the mesh this claim gives a node-admin.
        let crash_mesh = first.as_ref().filter(|_| recover).and_then(|op| self.unheard_peer(op)).filter(|(_, _, decided)| !*decided).map(|(node, _, _)| node.mesh);
        let from = std::time::Instant::now();
        // An unheard mesh-primary's recovery, its delete and the create over its birth, is claimed for the
        // admin its mesh seats in the birth's place once one is seated: "the fabric-primary opens the
        // attempt ...; the mesh-primary claims it through BuildClaim, and the fabric-primary calls
        // build.attempt.run naming the mesh; the mesh-primary runs it and reports each receipt up that
        // stream, and the fabric-primary writes every Build record" (node-recovery.md "Execution"), and
        // "The fabric-primary writes a row only as acting primary of a mesh with no primary. No other
        // process writes a Node row" (node-identity-and-state.md §4.3). It is this admin's only while no
        // admin of that mesh executes it: the acting primary of a mesh with no primary.
        // A node-admin's is claimed for the mesh-primary the election named in its place, read from this
        // view as the claim at its open reads it ([`crate::executor::unheard_mesh_primary_executor`]):
        // while the view names none and the mesh holds another node-admin, nothing is claimed and the
        // attempt waits, named, for the election's result.
        let recovery = first.as_ref().filter(|op| matches!(op, BuildOperation::CreateNode { .. } | BuildOperation::DeleteLiveNode { .. }) && mesh_primary_recovery_admits(p, op));
        let seated = match (recovery, p.action.as_ref()) {
            (Some(op), Some(crate::accepted::AttemptAction::Recover { path, node_id, .. })) if path.kind == NodeKind::NodeAdmin => match crate::executor::unheard_mesh_primary_executor(path, node_id, &view) {
                Some(to) => Some(to),
                None => {
                    let admins: Vec<String> = crate::executor::held_admins(&path.mesh, Some(path), &view).into_iter().map(|n| format!("{} ({:?})", n.name, n.status)).collect();
                    let why = format!("{me}: attempt {attempt} of Build {id} recovers {path}, mesh {}'s unheard mesh-primary: this view names no mesh-primary of mesh {} in its place and the mesh holds node-admin(s) {}, so the election has not named it here; nothing is claimed for {}", path.mesh, path.mesh, admins.join(", "), op.key());
                    return Err(self.stalled(me, drive, why));
                }
            },
            (Some(op), _) => crate::executor::executor_for(op, &view),
            (None, _) => None,
        }
        .filter(|to| to != me);
        let forced = match seated {
            Some(to) => Some(to),
            None if recover => Some(me.clone()),
            None => forced.filter(|to| to != me),
        };
        let lead = match forced {
            Some(to) => Some(to),
            None => {
                let t = self.topology.read().await.clone();
                let ops = left(drive, p, &t);
                let lead = lead_for(&ops, &t);
                // A mesh shutdown with no admin outside the mesh has no owner: no one executes it. The
                // fabric-primary says so, once per Build, rather than leaving the Build to stall unnamed.
                if lead.is_none() {
                    if let Some(op @ crate::build::BuildOperation::ShutdownMesh { mesh, .. }) = ops.first() {
                        if drive.stall_is_new(&format!("no-owner|{}", op.key())) {
                            tracing::info_span!("rdm.node_admin.build.reject.via-no-owner-outside-mesh", build_id = %id.log(), operation = %op.key(), mesh = %mesh)
                                .in_scope(|| tracing::info!("no ready admin primary exists outside the leaving mesh: nothing owns its shutdown, and the mesh is not dismantled"));
                        }
                    }
                }
                lead
            }
        };
        let Some(lead) = lead else {
            // Named: the operation, its mesh, and the same-mesh admins the attempt waits for.
            let t = self.topology.read().await.clone();
            let why = match left(drive, p, &t).first() {
                Some(op) => format!("{me}: no admin can execute what is left of Build {id} in this view: {}", crate::executor::why_no_executor(op, &t)),
                None => format!("{me}: no admin can execute what is left of Build {id} in this view"),
            };
            return Err(self.stalled(me, drive, why));
        };
        match self.door.claim(&lead.to_string(), id, attempt).await {
            BuildClaimReply::Won { context } => {
                if let Some(mesh) = crash_mesh {
                    self.departure.mesh_given_node_admin(&mesh, id, attempt, from).await;
                }
                self.started_if_opening(me, p, attempt, &context).await;
                Ok((attempt, lead, context))
            }
            BuildClaimReply::Lost { holder } => match parse_path(&holder) {
                Some(h) => {
                    let context = self.door.contexts.get(id, attempt).await.ok().flatten().unwrap_or_default();
                    Ok((attempt, h, context))
                }
                None => Err(self.stalled(me, drive, format!("attempt {attempt} of Build {id} is held by {holder:?}, which is not a path.name"))),
            },
            BuildClaimReply::NotOpen { next } => Err(self.stalled(me, drive, format!("{me}: attempt {attempt} of Build {id} is not open (the open attempt is {next:?})"))),
            // Another attempt holds the mesh this one acts in (R-BI7 caveat 3): nothing is claimed or
            // dispatched; the next pass claims once its verdict releases the mesh.
            BuildClaimReply::BuildInProgress { current_build_id, mesh, held_by } => {
                let why = format!("{me}: attempt {attempt} of Build {id} acts in mesh {mesh}, which an attempt of Build {current_build_id} holds (claimed by {}): one attempt in flight per mesh", held_by.as_deref().unwrap_or("no recorded claimant"));
                Err(self.stalled(me, drive, why))
            }
            other => Err(self.stalled(me, drive, format!("{me}: the claim of attempt {attempt} of Build {id} was not decided: {}", other.name()))),
        }
    }

    /// The Build as the claim was decided on it: its acceptance and every attempt opened, claimed and
    /// ended, packed into Build messages, but not its step receipts (those are
    /// the recovery contract of the projection). What an executor needs to fold the same current
    /// attempt and to plan, carried by the call, so a run never waits for the store root.
    /// The sections of what is left of `p` (Q250): by mesh, in plan order, the plain creates whose
    /// executor is that mesh's own mesh-primary, never this fabric-primary. A mesh with any other
    /// operation left, its first node-admin owed, or held unheard (R-D1, whatever its recovery's
    /// verdict) sends no section: the fabric lane keeps its order, its waits and its recovery.
    fn sections(&self, me: &PathName, drive: &Drive, p: &BuildProjection, t: &Topology) -> Vec<(PathName, Vec<BuildOperation>)> {
        if p.deletes_fabric() || p.action.is_some() {
            return Vec::new();
        }
        let mut by: std::collections::BTreeMap<String, (PathName, Vec<BuildOperation>)> = Default::default();
        let mut kept: HashSet<String> = HashSet::new();
        for op in left(drive, p, t) {
            let Some(node) = op.node().cloned() else { continue };
            let to = crate::executor::executor_for(&op, t);
            match to {
                Some(to) if to != *me && !kept.contains(&node.mesh) && crate::executor::section_create(&op, t, &to) && !t.first_admin_of(&node) && self.unheard_peer(&op).is_none() => {
                    by.entry(node.mesh.clone()).or_insert_with(|| (to, Vec::new())).1.push(op);
                }
                _ => {
                    by.remove(&node.mesh);
                    kept.insert(node.mesh.clone());
                }
            }
        }
        by.into_values().collect()
    }

    /// Send every section of what is left of `p` to its mesh-primary at once, when two meshes or more
    /// are owed one or a mesh's section failed before, and wait for each to end (Q250). A section that fails stops none of the others; its mesh's operations are planned again
    /// on the next pass, and the same operation failing twice running ends the Build Failed (Q252).
    async fn run_sections(self: &Arc<Self>, me: &PathName, turn: &crate::stop_gate::Turn, drive: &Arc<Drive>, p: &BuildProjection) -> Sections {
        let sections = {
            let t = self.topology.read().await.clone();
            self.sections(me, drive, p, &t)
        };
        // One mesh's section gains nothing over the fabric lane's own attempt at its mesh-primary: sections
        // are sent when two meshes or more are owed one, or to a mesh whose section failed, which is claimed
        // again for that mesh alone (Q250).
        let retried = sections.iter().any(|(executor, _)| drive.section_failed.lock().unwrap().contains_key(&executor.mesh));
        if sections.len() < 2 && !retried {
            return Sections::None;
        }
        let ended = futures_util::future::join_all(sections.into_iter().map(|(executor, ops)| {
            async move { (executor.mesh.clone(), self.run_section(me, turn, drive, p, &executor, ops).await) }
        }))
        .await;
        let mut held = Vec::new();
        let mut twice = None;
        for (mesh, end) in ended {
            match end {
                Ok(()) => {
                    drive.section_failed.lock().unwrap().remove(&mesh);
                }
                Err(SectionEnd::Failed(failure)) => {
                    let key = failure.key();
                    let again = drive.section_failed.lock().unwrap().insert(mesh.clone(), key.clone()).as_deref() == Some(key.as_str());
                    if again && twice.is_none() {
                        twice = Some((mesh, failure));
                    }
                }
                Err(SectionEnd::Held(why)) => held.push(why),
            }
        }
        // The same operation failing twice running ends the Build Failed with the step and reason
        // of its second failure (Q250, Q252), once no section holds its mesh.
        if let (true, Some((mesh, failure))) = (held.is_empty(), twice) {
            return Sections::End(self.section_failed_twice(me, drive, p, &mesh, failure).await);
        }
        if held.is_empty() {
            return Sections::Ran;
        }
        let why = format!("{me}: Build {}: {}", p.build_id, held.join("; "));
        Sections::End(self.stalled(me, drive, why))
    }

    /// Mesh `mesh`'s section failed the same operation twice running (Q250, Q252): the Build ends
    /// `Failed` with the step and reason of the second failure, never a stall (node-rpc-envelope.md:
    /// "Only a fabric-primary change or a failed step ends an attempt"). No fabric-lane attempt runs
    /// while sections are sent: this fabric-primary claims the next one for itself and ends it `Failed`,
    /// running nothing, the failed step's receipt and the verdict written (as an interrupted Build's
    /// terminal is, build.md §1).
    async fn section_failed_twice(&self, me: &PathName, drive: &Arc<Drive>, p: &BuildProjection, mesh: &str, f: SectionFailure) -> DriveEnd {
        let id = &p.build_id;
        let attempt = p.attempt + 1;
        let span = tracing::info_span!("rdm.node_admin.build.update.via-section-failed", build_id = %id.log(), lane = %mesh, attempt, node = %me, operation = %f.operation, step = %f.step, reason = %f.reason);
        let claim = crate::build_state::BuildAttemptClaim { build_id: id.clone(), attempt, executor: me.to_string(), claimant: Some(me.to_string()), mesh: None };
        match self.builds.claim_attempt(&claim).await {
            Ok(crate::build_state::ClaimOutcome::Won) => {}
            other => return self.stalled(me, drive, format!("{me}: mesh {mesh}'s section of Build {id} failed {} twice running and attempt {attempt}, which ends the Build Failed, could not be taken: {other:?}", f.key())),
        }
        if !f.operation.is_empty() {
            let receipt = crate::build_state::BuildStepReceipt { build_id: id.clone(), attempt, operation: f.operation.clone(), step: f.step.clone(), outcome: crate::build_state::StepOutcome::Failed { reason: f.reason.clone(), code: f.code }, output: None, executor: Some(me.to_string()), mesh: None };
            if let Err(e) = self.builds.append_step_receipt(&receipt).await {
                span.in_scope(|| tracing::info!(error = %e, "the failed step's receipt could not be written"));
            }
        }
        let reason = if f.operation.is_empty() { f.reason.clone() } else { format!("{}: {}", f.operation, f.reason) };
        let verdict = BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Failed { reason, code: f.code } };
        if let Err(e) = self.builds.append_attempt_receipt(&verdict).await {
            return self.stalled(me, drive, format!("{me}: the Failed verdict of attempt {attempt} of Build {id} (mesh {mesh}'s section failed {} twice running) could not be written: {e}", f.key()));
        }
        self.gate.attempt_failed(id, attempt).await;
        span.in_scope(|| tracing::info!("a section failed the same operation twice running: the Build ends Failed with the second failure's step and reason"));
        drive.finish(rdm_core::reply::build::failed(id.to_string(), attempt, f.operation, f.step, crate::build_run::bounded_reason(f.reason), f.code));
        DriveEnd::Terminal
    }

    /// One section (Q250): its attempt opened in `executor`'s mesh's lane and claimed by this
    /// fabric-primary, dispatched to the mesh-primary, every receipt it reports written in the lane, its
    /// verdict written, and the mesh's in-flight row released.
    #[allow(clippy::too_many_arguments)]
    async fn run_section(&self, me: &PathName, turn: &crate::stop_gate::Turn, drive: &Arc<Drive>, p: &BuildProjection, executor: &PathName, operations: Vec<BuildOperation>) -> Result<(), SectionEnd> {
        let id = &p.build_id;
        use crate::build_state::{AttemptOpened, AttemptReason, BuildStepReceipt, StepOutcome};
        let mesh = executor.mesh.clone();
        let lane = crate::build_run::LaneBuilds::of(&self.builds, Some(&mesh));
        let keys = operations.iter().map(BuildOperation::key).collect::<Vec<_>>().join(",");
        let span = tracing::info_span!("rdm.node_admin.build.update.via-section", build_id = %id.log(), lane = %mesh, attempt = tracing::field::Empty, executor = %executor, node = %me, operations = %keys, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        let ended = async {
            let attempt = match lane.read_build(id).await {
                Ok(l) => l.attempt + 1,
                Err(e) => return Err(SectionEnd::Held(format!("mesh {mesh}'s lane of {id} could not be read: {e}"))),
            };
            tracing::Span::current().record("attempt", attempt);
            let opened = AttemptOpened { build_id: id.clone(), attempt, reason: AttemptReason::Requested, action: None, opened_by: me.to_string(), opened_at_ms: self.gate.now_ms(), mesh: Some(mesh.clone()), context: None };
            if let Err(e) = lane.open_attempt(&opened).await {
                return Err(SectionEnd::Held(format!("attempt {attempt} of mesh {mesh}'s lane of {id} could not be opened: {e}")));
            }
            match self.door.claim_in(&executor.to_string(), id, Some(&mesh), attempt).await {
                BuildClaimReply::Won { .. } => {}
                other => return Err(SectionEnd::Held(format!("attempt {attempt} of mesh {mesh}'s lane of {id} was not claimed for {executor}: {}", claim_refusal(&other)))),
            }
            let context = self.door.contexts.get_lane(id, Some(&mesh), attempt).await.ok().flatten().unwrap_or_default();
            // The intent is read after the open and the claim, so the mesh-primary holds the attempt it runs.
            let intent = self.intent_of(id, &operations).await;
            let keys: Vec<String> = operations.iter().map(BuildOperation::key).collect();
            let plan = RunPlan { operations, hand_off_to: None };
            let mut completed: Vec<String> = Vec::new();
            let mut failed_step: Option<(String, String, String)> = None;
            let outcome = match self.dispatcher.dispatch_in(turn, executor, &mesh, id, attempt, context, intent, plan).await {
                Dispatched::Refused(r) => AttemptOutcome::Failed { reason: format!("{executor} refused the section: {}", describe(&r)), code: rdm_core::outcome::Code::Unavailable },
                Dispatched::Unreached(why) => match self.departure.proven(executor).await {
                    Ok(()) => AttemptOutcome::Failed { reason: format!("{executor} exited with the section unfinished: {why}"), code: rdm_core::outcome::Code::Unavailable },
                    Err(not) => return Err(SectionEnd::Held(format!("the section of mesh {mesh} at {executor} was not reached ({why}) and its departure is not proven ({not}): attempt {attempt} holds the mesh"))),
                },
                Dispatched::Stream(mut stream) => loop {
                    match stream.next().await {
                        InnerItem::Frame(BuildReply::Step { operation, step, attempt: a, receipt, .. }) => {
                            // A failed step's receipt is written from the section's Failed terminal.
                            if failed_receipt(receipt.as_deref()) {
                                stream.accepted().await;
                                continue;
                            }
                            if let Some(r) = receipt.as_deref().and_then(|b| serde_json::from_slice::<BuildStepReceipt>(b).ok()) {
                                if let Err(e) = lane.append_step_receipt(&BuildStepReceipt { build_id: id.clone(), attempt, mesh: Some(mesh.clone()), ..r }).await {
                                    tracing::info!(error = %e, operation = %operation, step = %step, "a reported step receipt could not be written: a later attempt runs the step again");
                                }
                            }
                            stream.accepted().await;
                            if step == "Complete" {
                                self.gate.receipt_streamed_in(&mesh, id, attempt, &operation).await;
                                completed.push(operation.clone());
                            }
                            self.step(drive, BuildReply::Step { build_id: id.to_string(), attempt: a, operation, step, receipt: None }).await;
                        }
                        InnerItem::Frame(f @ BuildReply::Blocked { .. }) => {
                            stream.accepted().await;
                            drive.push(f);
                        }
                        InnerItem::Frame(BuildReply::Complete { .. }) => break AttemptOutcome::Converged,
                        InnerItem::Frame(BuildReply::HandedOff { to, .. }) => break AttemptOutcome::HandedOff { to },
                        InnerItem::Frame(BuildReply::Failed { operation, step, reason, code, .. }) => {
                            let _ = lane.append_step_receipt(&BuildStepReceipt { build_id: id.clone(), attempt, operation: operation.clone(), step: step.clone(), outcome: StepOutcome::Failed { reason: reason.clone(), code }, output: None, executor: Some(executor.to_string()), mesh: Some(mesh.clone()) }).await;
                            failed_step = Some((operation.clone(), step, reason.clone()));
                            break AttemptOutcome::Failed { reason: format!("{operation}: {reason}"), code };
                        }
                        InnerItem::Frame(other) => break AttemptOutcome::Failed { reason: format!("{executor}'s section ended {}", other.name()), code: rdm_core::outcome::Code::Unavailable },
                        InnerItem::Ended | InnerItem::Broke(_) => match self.departure.proven(executor).await {
                            Ok(()) => break AttemptOutcome::Failed { reason: format!("{executor} exited with the section unfinished"), code: rdm_core::outcome::Code::Unavailable },
                            Err(not) => return Err(SectionEnd::Held(format!("the section of mesh {mesh} at {executor} was lost and its departure is not proven ({not}): attempt {attempt} holds the mesh"))),
                        },
                    }
                },
            };
            let failed = match &outcome {
                AttemptOutcome::Failed { reason, code } => Some(match failed_step {
                    Some((operation, step, reason)) => SectionFailure { operation, step, reason, code: *code },
                    None => SectionFailure { operation: String::new(), step: "section".into(), reason: reason.clone(), code: *code },
                }),
                _ => None,
            };
            // A section that converged ran every one of its operations (`Reconciled::Converged`); one that
            // ended otherwise completed what its Complete frames named.
            if matches!(outcome, AttemptOutcome::Converged) {
                completed = keys;
            }
            if let Err(e) = lane.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt, outcome, mesh: Some(mesh.clone()) }).await {
                return Err(SectionEnd::Held(format!("the verdict of attempt {attempt} of mesh {mesh}'s lane of {id} could not be written: {e}")));
            }
            // Reported by its verdict: what the section completed is done.
            drive.section_done.lock().unwrap().extend(completed.into_iter().map(|key| (p.opened, key)));
            let row = crate::mesh_claims::InFlight { build_id: id.clone(), lane: Some(mesh.clone()), attempt, claimant: me.to_string(), claimant_node: None };
            if let Err(e) = self.door.claims.release_in_flight(&mesh, &row).await {
                tracing::info!(error = %e, "the section ended and its mesh's in-flight row could not be released: the mesh stays held by it");
            }
            match failed {
                Some(failure) => Err(SectionEnd::Failed(failure)),
                None => Ok(()),
            }
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", match &ended { Ok(()) => "complete", Err(SectionEnd::Failed(_)) => "failed", Err(SectionEnd::Held(_)) => "held" });
        match &ended {
            Err(SectionEnd::Failed(f)) => {
                span.record("reason", format!("{}: {}", f.operation, f.reason).as_str());
            }
            Err(SectionEnd::Held(why)) => {
                span.record("reason", why.as_str());
            }
            Ok(()) => {}
        }
        span.in_scope(|| tracing::info!("a section of the Build ran at its mesh's mesh-primary"));
        ended
    }

    async fn intent_of(&self, id: &BuildId, run: &[BuildOperation]) -> Vec<Vec<u8>> {
        intent_frames(&*self.builds, id, run).await
    }

    /// A complete Build this drive made no call of: attach to the last attempt's executor and read
    /// its frames. `None` when the attached run ended `Complete`; otherwise the end of the drive.
    async fn reattach_to_ended(&self, me: &PathName, turn: &crate::stop_gate::Turn, drive: &Arc<Drive>, p: &BuildProjection) -> Option<DriveEnd> {
        let id = &drive.build_id;
        self.replay(drive, &p.steps, 0, p.attempt).await;
        let named = p.executor.clone().unwrap_or_default();
        let Some(holder) = parse_path(&named) else {
            drive.finish(rdm_core::reply::build::not_ready(format!("{me}: attempt {} of Build {id} completed on {named:?}, which is not a path.name", p.attempt)));
            return Some(DriveEnd::Terminal);
        };
        drive.note_first_attempt(p.attempt);
        let context = self.door.contexts.get(id, p.attempt).await.ok().flatten().unwrap_or_default();
        let span = tracing::info_span!("rdm.node_admin.build.update.via-dispatch", build_id = %id.log(), attempt = p.attempt, executor = %holder, node = %me, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        let why = match self.run_inner(turn, drive, id, p.attempt, &holder, context, RunPlan::default()).instrument(span.clone()).await {
            Inner::Terminal(BuildReply::Complete { .. }) => {
                span.record("outcome", "complete");
                return None;
            }
            Inner::SeatYielded => {
                span.record("outcome", "seat-yielded");
                return Some(match self.may_act(drive).await {
                    Some(end) => end,
                    None => self.stalled(me, drive, format!("{me}: the replay of Build {id} stopped at a seat target that the gate no longer reports")),
                });
            }
            Inner::Terminal(t) => format!("it answered {}", t.name()),
            Inner::Refused(r) => format!("it refused: {}", describe(&r)),
            Inner::Unreached(w) | Inner::Broke(w) => w,
        };
        span.record("outcome", "executor-lost");
        span.record("reason", why.as_str());
        drive.finish(rdm_core::reply::build::not_ready(format!("{me}: Build {id} is complete, and {holder}, which ran its last attempt {}, could not replay it: {why}", p.attempt)));
        Some(DriveEnd::Terminal)
    }

    /// A `Step` frame new to `drive`, live from the executor or read from a receipt (a completed
    /// step's receipt is the same event as its frame), published once.
    async fn step(&self, drive: &Drive, frame: BuildReply) {
        if !drive.take_key(&frame) {
            return;
        }
        drive.publish(frame);
    }

    /// The frame of every `Complete` receipt of `steps` from attempt `from` to before attempt `to`,
    /// each through [`Self::step`]: a read of receipts, never a run.
    async fn replay(&self, drive: &Drive, steps: &[crate::build_state::BuildStepReceipt], from: u32, to: u32) {
        for frame in drive.receipt_frames(steps, from, to) {
            self.step(drive, frame).await;
        }
    }

    /// The verdict of a run that ended: an executor on another admin reported it, and this
    /// fabric-primary writes it, with the step receipt of the step it failed at (Q118; build.md: the
    /// fabric-primary writes every Build record); this admin's own run wrote it and it is taken here.
    async fn verdict(&self, me: &PathName, executor: &PathName, receipt: BuildAttemptReceipt, failed: Option<(&String, &String, &String, rdm_core::outcome::Code)>) {
        if executor == me {
            self.verdicts.record(receipt).await;
            return;
        }
        // An attempt has one verdict: one its executor wrote itself (a run served when it was the
        // fabric-primary) is the attempt's, and nothing is written again.
        let decided = self.builds.facts().await.map(|facts| facts.iter().any(|f| matches!(f, crate::build_state::BuildFact::Attempt(a) if a.build_id == receipt.build_id && a.attempt == receipt.attempt && a.mesh.is_none())));
        if matches!(decided, Ok(true)) {
            return;
        }
        if let Some((operation, step, reason, code)) = failed.filter(|(operation, ..)| !operation.is_empty()) {
            // A failed step whose receipt the executor reported up the stream is on the record already.
            let held = self.builds.read_build(&receipt.build_id).await.is_ok_and(|p| p.steps.iter().any(|s| s.mesh.is_none() && s.attempt == receipt.attempt && s.operation == *operation && s.step == *step && matches!(s.outcome, crate::build_state::StepOutcome::Failed { .. })));
            if held {
                if let Err(e) = self.builds.append_attempt_receipt(&receipt).await {
                    tracing::info!(error = %e, attempt = receipt.attempt, "the reported verdict could not be written: the attempt holds no verdict until the next pass decides it");
                }
                return;
            }
            let failed = crate::build_state::BuildStepReceipt { build_id: receipt.build_id.clone(), attempt: receipt.attempt, operation: operation.clone(), step: step.clone(), outcome: crate::build_state::StepOutcome::Failed { reason: reason.clone(), code }, output: None, executor: Some(executor.to_string()), mesh: None };
            if let Err(e) = self.builds.append_step_receipt(&failed).await {
                tracing::info!(error = %e, operation = %operation, step = %step, "the failed step's reported receipt could not be written");
            }
        }
        if let Err(e) = self.builds.append_attempt_receipt(&receipt).await {
            tracing::info!(error = %e, attempt = receipt.attempt, "the reported verdict could not be written: the attempt holds no verdict until the next pass decides it");
        }
    }

    /// Write the step receipt `bytes` an executor reported for attempt `attempt` of `id` (Q118): only
    /// while the claim record this admin holds names that attempt and `executor` (build.md: "a call
    /// carrying a claim that is not the current folded attempt of the receiver's projection is refused
    /// `StaleClaim`"); a receipt of any other attempt is refused by name and written nowhere.
    async fn write_reported(&self, id: &BuildId, attempt: u32, executor: &PathName, operation: &str, step: &str, bytes: &[u8]) {
        let me = self.held_name().await;
        let r = match serde_json::from_slice::<crate::build_state::BuildStepReceipt>(bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::info!(error = %e, operation, step, "a reported step receipt does not decode: a later attempt runs the step again");
                return;
            }
        };
        let p = match self.builds.read_build(id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::info!(error = %e, operation, step, "the Build could not be read to decide a reported step receipt: a later attempt runs the step again");
                return;
            }
        };
        if p.attempt != attempt || p.executor.as_deref() != Some(executor.to_string().as_str()) {
            tracing::info_span!("rdm.node_admin.build.reject.via-stale-claim", node = %me, build_id = %id.log(), attempt, held_attempt = p.attempt, held_executor = p.executor.as_deref().unwrap_or(""), executor = %executor, operation, step)
                .in_scope(|| tracing::info!("a reported receipt of an attempt the claim record does not hold is written nowhere"));
            return;
        }
        if let Err(e) = self.builds.append_step_receipt(&crate::build_state::BuildStepReceipt { build_id: id.clone(), attempt, mesh: None, ..r }).await {
            tracing::info!(error = %e, operation, step, "a reported step receipt could not be written: a later attempt runs the step again");
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_inner(&self, turn: &crate::stop_gate::Turn, drive: &Arc<Drive>, id: &BuildId, attempt: u32, executor: &PathName, context: CallContext, plan: RunPlan) -> Inner {
        let intent = self.intent_of(id, &plan.operations).await;
        let mut stream = match self.dispatcher.dispatch(turn, executor, id, attempt, context, intent, plan).await {
            Dispatched::Stream(s) => s,
            Dispatched::Refused(r) => return Inner::Refused(r),
            Dispatched::Unreached(why) => return Inner::Unreached(why),
        };
        // Set once the target names this run's executor: the run's own node.primary.update made it
        // fabric-primary.
        let mut seated_executor = false;
        loop {
            // Only the wait for the next item races the target: a frame in hand is handled whole.
            let item = tokio::select! {
                biased;
                _ = self.gate.target_set(), if !seated_executor => {
                    // This run's executor is fabric-primary once it has installed the election's result,
                    // and it says so on this stream by ending it with FabricPrimaryMoved naming itself. The callers are sent to it only
                    // then: before, its door would answer them as an admin that is not the
                    // fabric-primary. Any other holder ends the drive at the target at once.
                    match self.gate.target().await {
                        Err(BuildStateError::SeatYielded { to: Some(to), .. }) if parse_path(&to).as_ref() == Some(executor) => {
                            seated_executor = true;
                            continue;
                        }
                        _ => return Inner::SeatYielded,
                    }
                }
                item = stream.next() => item,
            };
            match item {
                InnerItem::Frame(f) => match f {
                    BuildReply::Step { ref operation, ref step, attempt: a, ref receipt, .. } => {
                        // A reported receipt (Q118): this fabric-primary writes it, decided by the claim
                        // record it holds, never by the view (build.md, Q239).
                        if let Some(bytes) = receipt {
                            self.write_reported(id, a, executor, operation, step, bytes).await;
                        }
                        stream.accepted().await;
                        // A failed step's receipt is the record's, never progress: the attempt's terminal
                        // names a failure, and a create's heal hands back without one (Q245, Q252).
                        if failed_receipt(receipt.as_deref()) {
                            continue;
                        }
                        if step == "Complete" {
                            self.gate.receipt_streamed(id, a, operation).await;
                        }
                        self.step(drive, BuildReply::Step { build_id: id.to_string(), attempt: a, operation: operation.clone(), step: step.clone(), receipt: None }).await
                    }
                    BuildReply::Blocked { .. } => {
                        stream.accepted().await;
                        drive.push(f);
                    }
                    // Past the target this admin decides nothing of the attempt: the executor, now the
                    // fabric-primary, reports its end to its own callers.
                    _ if seated_executor => return Inner::SeatYielded,
                    t @ (BuildReply::Complete { .. } | BuildReply::Failed { .. } | BuildReply::HandedOff { .. }) => return Inner::Terminal(t),
                    // The executor became fabric-primary during the run: the target names it, and the
                    // drive ends there.
                    BuildReply::FabricPrimaryMoved { .. } => return Inner::SeatYielded,
                    other => return Inner::Refused(other),
                },
                // The target is the answer whatever became of the stream.
                InnerItem::Ended | InnerItem::Broke(_) if seated_executor => return Inner::SeatYielded,
                InnerItem::Ended => return Inner::Broke("the inner stream ended without its terminal".into()),
                InnerItem::Broke(why) => return Inner::Broke(why),
            }
        }
    }
}

/// The Build's intent as the executor absorbs it before it plans (`build.attempt.run`'s `intent`):
/// postcard frames of a Build message, every fact of `id` the fabric-primary holds, its step receipts
/// with them: the executor reads no Build record (Q227), so an earlier attempt's receipts a step reads
/// (a successor's create reads its delete's `TerminateRuntime`) reach it here.
pub(crate) async fn intent_frames(builds: &dyn BuildStateAdapter, id: &BuildId, run: &[BuildOperation]) -> Vec<Vec<u8>> {
    let facts = match builds.facts().await {
        Ok(f) => intent_facts(f, id, run),
        Err(_) => return Vec::new(),
    };
    let (frames, refused) = crate::fabric_builds::encode_chunks(facts);
    for e in refused {
        tracing::info_span!("rdm.node_admin.build.reject.via-oversized-fact", build_id = %id.log(), detail = %e).in_scope(|| tracing::info!("a fact of the Build fits no frame: the executor does not hold it"));
    }
    frames.into_iter().map(|b| b.to_vec()).collect()
}

/// The facts of Build `id` an attempt run carries ([`intent_frames`]): every fact of the Build but its
/// forgetting, with only the step receipts the executor reads: those of every operation on a path the
/// run acts on (an operation run again reuses what its completed steps decided, and a replacement's
/// create reads its delete's receipts at the same path) and those of the steps a run writes under one
/// of them (a seat move's `node.primary.update:<build>:<attempt>:<operation>`). Never every receipt the
/// Build ever wrote, which grows with every attempt the Build opens until the request outgrows its frame.
fn intent_facts(facts: Vec<crate::build_state::BuildFact>, id: &BuildId, run: &[BuildOperation]) -> Vec<crate::build_state::BuildFact> {
    let keys: Vec<String> = run.iter().map(BuildOperation::key).collect();
    // An operation key is `<op>:<target>`; the target is the path (or `<path>@<incarnation>`) it acts on.
    let target = |key: &str| key.split_once(':').map(|(_, t)| t.to_string()).unwrap_or_default();
    let targets: Vec<String> = keys.iter().map(|k| target(k)).filter(|t| !t.is_empty()).collect();
    let of_this_run = |operation: &str| keys.iter().any(|k| operation.ends_with(&format!(":{k}"))) || targets.iter().any(|t| target(operation) == *t);
    facts
        .into_iter()
        .filter(|f| f.build_id() == id && !matches!(f, crate::build_state::BuildFact::Forget { .. }))
        .filter(|f| match f {
            crate::build_state::BuildFact::Step(s) => of_this_run(&s.operation),
            _ => true,
        })
        .collect()
}

/// Whether view `t` still names `m`'s holder the holder of the seats `m` moves before `op`.
fn still_holds(op: &BuildOperation, m: &crate::executor::SeatMove, t: &Topology) -> bool {
    crate::executor::seat_move(op, t).is_some_and(|still| still.holder.node_id == m.holder.node_id && still.mesh_seat == m.mesh_seat && still.fabric_seat == m.fabric_seat && still.cohort_seat == m.cohort_seat)
}

/// A claim not won, in words that name what the claim door said.
fn claim_refusal(r: &BuildClaimReply) -> String {
    match r {
        BuildClaimReply::Lost { holder } => format!("lost (the attempt is held by {holder})"),
        BuildClaimReply::NotOpen { next } => format!("not-open (the open attempt is {next:?})"),
        BuildClaimReply::NotClaimant { mesh, fabric_primary, mesh_primary } => format!("not-claimant (mesh {mesh:?}; its view shows fabric-primary {fabric_primary:?}, mesh-primary {mesh_primary:?})"),
        BuildClaimReply::BuildInProgress { current_build_id, mesh, held_by } => format!("build-in-progress (mesh {mesh} is held by Build {current_build_id}, claimed by {held_by:?})"),
        BuildClaimReply::NotReady { reason } => format!("not-ready ({reason})"),
        other => other.name().to_string(),
    }
}

/// A refusal in words that name what it is and what it carries.
pub fn describe(r: &BuildReply) -> String {
    match r {
        BuildReply::NotExecutor { named, recipient } => format!("not-executor (the claim names {named}, the recipient is {recipient})"),
        BuildReply::StaleClaim { held_attempt, held_executor, carried_attempt } => {
            format!("stale-claim (the recipient holds attempt {held_attempt} for {}, the call carried attempt {carried_attempt})", held_executor.as_deref().unwrap_or("no executor"))
        }
        BuildReply::UnknownBuild { build_id } => format!("unknown-build ({build_id})"),
        BuildReply::NotReady { reason } | BuildReply::Busy { reason } | BuildReply::ConnectionsDeleting { reason } | BuildReply::PeerUnresolved { reason } | BuildReply::Unauthorized { reason } => format!("{} ({reason})", r.name()),
        other => other.name().to_string(),
    }
}

/// An unheard mesh-primary's recovery (node-recovery.md sequence step 1: "for an unheard mesh-primary,
/// the fabric-primary opens it", in the fabric lane): `op` is one of the attempt's own operations on the
/// exact birth its `Recover` action names, the delete of that birth (`TerminateRuntime` first when its
/// runtime runs, nothing sent to it) or the create over it at the same path.name. They act on node
/// investigation's verdict, so R-D1's target does not hold them; nothing else of the unheard mesh
/// crosses it.
fn mesh_primary_recovery_admits(p: &BuildProjection, op: &BuildOperation) -> bool {
    let Some(crate::accepted::AttemptAction::Recover { path, node_id, from_incarnation, .. }) = p.action.as_ref() else { return false };
    let birth = |b: &crate::build::ExitedBirth| &b.node_id == node_id && &b.incarnation == from_incarnation;
    match op {
        BuildOperation::DeleteLiveNode { node, exited: Some(b), .. } | BuildOperation::CreateNode { node, over: Some(b), .. } => node == path && birth(b),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (R-BI4; proposal node-primary-update Part C1 step 8: the commanding Build continues
    /// at the new fabric-primary, which resumes the same attempt): an attempt ended incomplete only when this admin ran it itself and it came
    /// back to this admin having moved nothing. A hand-off to this admin that another executor
    /// reported (the old fabric-primary's attempt, handed to the new seat before its operation ran, and
    /// replayed from its run when the new seat's drive attaches to it) is that executor's end: the
    /// drive claims the next attempt for the admin the view names, it does not idle.
    /// Must NOT happen: the new seat's drive idling on the replay of an attempt it never ran, and
    /// never claiming the attempt the old seat handed it.
    #[test]
    fn an_attempt_another_executor_handed_to_this_admin_is_not_an_attempt_that_ended_incomplete() {
        let path = |p: &str| p.parse::<PathName>().unwrap();
        let (me, old_seat) = (path("mesh2.admin.1"), path("mesh1.admin.1"));
        assert!(!ended_incomplete(&me, &old_seat, &me, "w", "w"), "the old seat's attempt, replayed to the new seat's drive, moved nothing in this dispatch and is still not incomplete");
        assert!(ended_incomplete(&me, &me, &me, "w", "w"), "an attempt this admin ran, handed back to it with nothing moved, ended incomplete");
        assert!(!ended_incomplete(&me, &me, &me, "w", "w + a receipt"), "an attempt that moved something is progress, the next is claimed at once");
        assert!(!ended_incomplete(&me, &me, &old_seat, "w", "w"), "an attempt handed to another admin is that admin's");
    }

    /// CONTRACT (the AttemptRun request's `intent`: the Build's acceptance, every attempt opened, claimed
    /// and ended, and the step receipts of the operations this run carries; red in the soaks at 9244b8cf8,
    /// routing: attempt 71 of a formation Build with 70 attempts of receipts refused `Malformed(TooLarge)`
    /// past the Build op's 256 KiB request frame, the restart never dispatched): the receipts of the run's
    /// operations' paths and of the steps written under them are carried, the Build's other receipts are not, and
    /// every attempt fact is. Must NOT happen: the intent growing with every receipt the Build ever wrote.
    #[test]
    fn an_attempt_run_carries_the_receipts_of_its_own_operations_only() {
        use crate::build_state::{AttemptOutcome, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
        let id = BuildId::parse(&rdm_core::fixture_crockford60("intent")).unwrap();
        let other = BuildId::parse(&rdm_core::fixture_crockford60("other")).unwrap();
        let step = |build: &BuildId, attempt: u32, operation: &str| BuildFact::Step(BuildStepReceipt { mesh: None, build_id: build.clone(), attempt, operation: operation.into(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None });
        let mut facts = Vec::new();
        for attempt in 1..=70 {
            facts.push(step(&id, attempt, "node.stop:mesh1.rpc.1"));
            facts.push(step(&id, attempt, "node.start:mesh1.rpc.1"));
            facts.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }));
        }
        facts.push(step(&id, 69, "node.delete:mesh2.broker.3"));
        facts.push(step(&id, 71, "node.stop:mesh2.broker.3"));
        facts.push(step(&id, 71, &format!("node.primary.update:{id}:71:node.stop:mesh2.broker.3")));
        facts.push(step(&other, 1, "node.stop:mesh2.broker.3"));
        let run = vec![BuildOperation::StopNode { node: "mesh2.broker.3".parse().unwrap(), node_id: rdm_core::NodeId::mint(), from_incarnation: rdm_core::IncarnationId::mint() }];
        let carried = intent_facts(facts, &id, &run);
        let steps: Vec<String> = carried.iter().filter_map(|f| match f { BuildFact::Step(s) => Some(s.operation.clone()), _ => None }).collect();
        assert_eq!(steps, vec!["node.delete:mesh2.broker.3".to_string(), "node.stop:mesh2.broker.3".to_string(), format!("node.primary.update:{id}:71:node.stop:mesh2.broker.3")], "the receipts of every operation at the run's path, its seat move's included, and none of another path's or another Build's");
        assert_eq!(carried.iter().filter(|f| matches!(f, BuildFact::Attempt(_))).count(), 70, "every attempt fact is carried");
    }

    // @feature: on-build-started-ended
    #[test]
    fn build_notices_deliver_started_and_ended_contexts_with_attempt_trace_and_emit_no_hook_when_unregistered() {
        // docs/architecture/fabric-node-hooks.md:139 — "Each time a Build's first attempt is
        // claimed, or an attempt that reopens a Build that has ended is claimed, the
        // fabric-primary sends the application one `on_build_started` notice ... with
        // `BuildStartedCtx { fabric_id, build_id, attempt, reason, context }`"
        // docs/architecture/fabric-node-hooks.md:143 — "Each time a Build's terminal state is
        // recorded, the fabric-primary sends the application one `on_build_ended` notice ... with
        // `BuildEndedCtx { fabric_id, build_id, attempt, reason, state, context }`"
        let capture = crate::span_capture::Capture::new("build-notices-unit");
        let dispatch = capture.dispatch.clone();
        let d = dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let fabric_id = rdm_core::FabricId::mint();
        let build_id = BuildId::parse("amrrv6tggykq").unwrap();
        let build_id_for_assert = build_id.clone();
        let fabric_id_for_assert = fabric_id.clone();

        let (started_ctx, ended_complete, ended_failed) = rt.block_on(async move {
            let me: PathName = "mesh1.admin.1".parse().unwrap();
            let contexts = Arc::new(crate::build_claim::AttemptContexts::in_memory());
            let call_ctx = CallContext {
                caller_system: Some("test-caller".into()),
                traceparent: Some(
                    "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".into(),
                ),
                tracestate: None,
                baggage: Some("k=v".into()),
            };
            contexts.put(&build_id, 1, &call_ctx).await.unwrap();
            contexts.put(&build_id, 2, &call_ctx).await.unwrap();

            // 1. Unregistered `BuildNotices::default()` emits `outcome = "no-hook"` spans.
            let unbound = BuildNotices::default();
            unbound.started(
                &me,
                &contexts,
                &build_id,
                1,
                crate::build_state::AttemptReason::Requested,
                &call_ctx,
            );
            unbound.ended(
                &me,
                &contexts,
                &build_id,
                1,
                crate::build_state::AttemptReason::Requested,
                crate::wiring::BuildEndedState::Complete,
                &tracing::Span::none(),
            );

            // 2. Registered `BuildNotices::of(fabric_id, &hooks)` delivers `BuildStartedCtx` and
            //    `BuildEndedCtx` (`Complete` and `Failed`) with `outcome = "sent"`.
            let (started_tx, mut started_rx) =
                tokio::sync::mpsc::unbounded_channel::<crate::wiring::BuildStartedCtx>();
            let (ended_tx, mut ended_rx) =
                tokio::sync::mpsc::unbounded_channel::<crate::wiring::BuildEndedCtx>();
            let hooks = crate::wiring::FabricHooks {
                on_build_started: Some(Arc::new(move |ctx| {
                    let _ = started_tx.send(ctx);
                    Box::pin(async {})
                })),
                on_build_ended: Some(Arc::new(move |ctx| {
                    let _ = ended_tx.send(ctx);
                    Box::pin(async {})
                })),
                ..crate::wiring::FabricHooks::no_app_work()
            };
            let bound = BuildNotices::of(fabric_id, &hooks);
            bound.started(
                &me,
                &contexts,
                &build_id,
                1,
                crate::build_state::AttemptReason::Requested,
                &call_ctx,
            );
            let parent = tracing::info_span!("rdm.node_admin.build.update.via-dispatch");
            bound.ended(
                &me,
                &contexts,
                &build_id,
                1,
                crate::build_state::AttemptReason::Requested,
                crate::wiring::BuildEndedState::Complete,
                &parent,
            );
            bound.ended(
                &me,
                &contexts,
                &build_id,
                2,
                crate::build_state::AttemptReason::ProvenDrift,
                crate::wiring::BuildEndedState::Failed {
                    step: "node.create".into(),
                    reason: "spawn refused".into(),
                    code: rdm_core::outcome::Code::FailedPrecondition,
                },
                &parent,
            );

            let started_ctx = started_rx.recv().await.expect("started notice delivered");
            let mut ended = vec![
                ended_rx.recv().await.expect("first ended notice delivered"),
                ended_rx.recv().await.expect("second ended notice delivered"),
            ];
            ended.sort_by_key(|c| c.attempt);
            let ended_failed = ended.pop().unwrap();
            let ended_complete = ended.pop().unwrap();
            (started_ctx, ended_complete, ended_failed)
        });
        drop(rt);

        assert_eq!(started_ctx.fabric_id, fabric_id_for_assert);
        assert_eq!(started_ctx.build_id, build_id_for_assert.to_string());
        assert_eq!(started_ctx.attempt, 1);
        assert_eq!(
            started_ctx.reason,
            crate::build_state::AttemptReason::Requested
        );
        assert_eq!(started_ctx.context.baggage.as_deref(), Some("k=v"));

        assert_eq!(ended_complete.fabric_id, fabric_id_for_assert);
        assert_eq!(ended_complete.build_id, build_id_for_assert.to_string());
        assert_eq!(ended_complete.attempt, 1);
        assert_eq!(
            ended_complete.state,
            crate::wiring::BuildEndedState::Complete
        );
        assert_eq!(ended_complete.context.baggage.as_deref(), Some("k=v"));

        assert_eq!(ended_failed.attempt, 2);
        assert_eq!(ended_failed.reason, crate::build_state::AttemptReason::ProvenDrift);
        assert_eq!(
            ended_failed.state,
            crate::wiring::BuildEndedState::Failed {
                step: "node.create".into(),
                reason: "spawn refused".into(),
                code: rdm_core::outcome::Code::FailedPrecondition,
            }
        );

        let started_spans = capture.named("rdm.node_admin.build.update.via-on-build-started");
        assert!(
            started_spans
                .iter()
                .any(|s| s.attributes.get("outcome").map(String::as_str) == Some("no-hook")),
            "expected via-on-build-started with outcome=no-hook"
        );
        assert!(
            started_spans.iter().any(|s| {
                s.attributes.get("outcome").map(String::as_str) == Some("sent")
                    && s.attributes.get("reason").map(String::as_str) == Some("requested")
            }),
            "expected via-on-build-started with outcome=sent and reason=requested"
        );

        let ended_spans = capture.named("rdm.node_admin.build.update.via-on-build-ended");
        assert!(
            ended_spans
                .iter()
                .any(|s| s.attributes.get("outcome").map(String::as_str) == Some("no-hook")),
            "expected via-on-build-ended with outcome=no-hook"
        );
        assert!(
            ended_spans.iter().any(|s| {
                s.attributes.get("outcome").map(String::as_str) == Some("sent")
                    && s.attributes.get("state").map(String::as_str) == Some("complete")
            }),
            "expected via-on-build-ended with outcome=sent and state=complete"
        );
        assert!(
            ended_spans.iter().any(|s| {
                s.attributes.get("outcome").map(String::as_str) == Some("sent")
                    && s.attributes.get("state").map(String::as_str) == Some("failed")
                    && s.attributes.get("reason").map(String::as_str) == Some("proven-drift")
            }),
            "expected via-on-build-ended with outcome=sent, state=failed, and reason=proven-drift"
        );
    }
}
