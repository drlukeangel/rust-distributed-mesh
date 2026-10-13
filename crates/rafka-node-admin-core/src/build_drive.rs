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
//! Before every claim and every dispatch the drive passes three gates: the Build log's sticky seat
//! fence is not set (a fabric-primary whose committed seat record names another live birth
//! dispatches nothing), this admin holds the seat in its view, and its view may authorize anything
//! at all (a cut-off view dispatches nothing). A seat loss names the holder the committed seat
//! record names, never the view's.
//! An executor that cannot be reached is replaced only on proof of its departure, never on
//! silence.
//!
//! Each pass of a drive (its gates, its claim, its dispatch and the run's terminal) is one turn of
//! its admin's park gate ([`crate::park_gate`]): a stopped admin begins none, and a stop waits for
//! the pass in progress before it cuts.

use crate::build::BuildId;
use crate::build_claim::ClaimDoor;
use crate::build_state::{AttemptOutcome, BuildAttemptReceipt, BuildProjection, BuildState, BuildStateAdapter, BuildStateError};
use crate::executor::{lead_for, run_of, RunPlan};
use crate::build::BuildOperation;
use crate::model::{NodeKind, PathName};
use crate::topology::Topology;
use rafka_node_rpc_contract::build::BuildReply;
use rafka_node_rpc_contract::build_claim::BuildClaimReply;
use rafka_node_rpc_contract::context::CallContext;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, RwLock};
use tracing::instrument::WithSubscriber as _;
use tracing::Instrument as _;

/// The gates every claim and dispatch passes.
#[async_trait::async_trait]
pub trait Gate: Send + Sync {
    /// Whether this admin's view may authorize anything now: not cut off, and not within one
    /// silence window of healing (`CutOff::authorizes`).
    fn authorizes(&self) -> bool;
    /// The seat fence of the Build log, read from the committed fabric seat record: refused by name
    /// once the record names another live birth; otherwise the path.name of the birth it names when
    /// that is this birth, `None` when it names none or a birth proven gone.
    async fn fence(&self) -> Result<Option<String>, BuildStateError>;
    /// Why reconciliation is frozen (a fabric shutdown is in force), when it is: no claim and no
    /// dispatch from here on.
    fn frozen(&self) -> Option<String> {
        None
    }
    /// Resolves once the seat record ends this admin's authority (never, for a gate with no seat):
    /// a drive waiting on an executor's run ends there, not when that run's stream does.
    async fn fence_set(&self) {
        std::future::pending::<()>().await
    }
    /// What the peer-mesh investigation holds of `mesh` now (`crate::investigate::Ladder::peer_mesh`),
    /// read when asked: a node of a peer mesh held unheard is not claimed or dispatched for (R-D1),
    /// except the mesh's first recovering node-admin once the investigation decided its rebirth.
    fn peer_mesh(&self, _mesh: &str) -> Option<crate::investigate::PeerMesh> {
        None
    }
    /// node.primary.update before an operation on a seat holder runs (R-MT1, Luke 2026-10-12: "only a
    /// FP should issue commands to a MP"; "this same primitive should be called on MP stop or
    /// delete"): this fabric-primary commands the holder's mesh seat to move, or moves its own seats
    /// (its mesh seat, then the fabric seat), so the operation's executor is the holder's successor.
    /// `operation` names the update's own operation. `Err` names why the seat did not move. A gate
    /// with no seats moves nothing.
    async fn move_seats(&self, _m: &crate::executor::SeatMove, _build_id: &BuildId, _attempt: u32, _operation: &str) -> Result<(), String> {
        Ok(())
    }
    /// node.primary.updated for every seat update a committed row records before an operation the
    /// drive is about to dispatch acts on that update's holder (the approved proposal
    /// node-primary-update-fabric-mesh-and-node-type.md, Part C1 failure contract: "The workflow
    /// continues only after the result is installed and acknowledged: node.primary.updated answered
    /// to the resuming executor's call, never a read alone"; "the next writer to read the row
    /// publishes 'updated'"). `updates` names each update's row; a row not written is no update. A
    /// row naming this birth the successor is installed from the row and this admin says "primary
    /// updated"; a row naming another successor is that successor's node.primary.updated, obtained
    /// on a call and installed. `Err` names why an update was not acknowledged. A gate with no seats
    /// has none to acknowledge.
    async fn seats_acknowledged(&self, _updates: &[PrecedingUpdate], _build_id: &BuildId, _attempt: u32) -> Result<(), String> {
        Ok(())
    }
}

/// The seat update that precedes an operation on a seat holder (R-MT1), named as its committed row
/// is on the fabric's store root: `meshes/<mesh>/seat-transfers/<operation>`
/// (`crate::fabric_handover::update_operation`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PrecedingUpdate {
    /// The holder's mesh, under which its row is written.
    pub mesh: String,
    /// The update's own operation.
    pub operation: String,
    /// The node the operation acts on, when it names one: the update's holder.
    pub holder: Option<PathName>,
}

/// The seat updates a committed row may record before `op` of Build `build_id` (R-MT1,
/// [`crate::executor::seat_move`]): for a stop, delete, restart, replace or node.connections.delete of
/// a node-admin, the mesh-primary seat's update and the fabric seat's alone; for a mesh retire, the
/// fabric seat's under that mesh. None for any other operation.
pub fn preceding_updates(op: &BuildOperation, build_id: &BuildId) -> Vec<PrecedingUpdate> {
    use crate::fabric_handover::update_operation;
    use rafka_mesh_entity::Seat;
    let key = op.key();
    match op {
        BuildOperation::ShutdownMesh { mesh, .. } => vec![PrecedingUpdate { mesh: mesh.clone(), operation: update_operation(Seat::FabricPrimary, build_id.as_str(), &key), holder: None }],
        BuildOperation::RetireNode { node, exited: None } | BuildOperation::RestartNode { node } | BuildOperation::ReplaceNode { node, .. } | BuildOperation::DeleteConnections { node, .. } | BuildOperation::StopNode { node, .. }
            if node.kind == NodeKind::NodeAdmin =>
        {
            [Seat::MeshPrimary, Seat::FabricPrimary].into_iter().map(|seat| PrecedingUpdate { mesh: node.mesh.clone(), operation: update_operation(seat, build_id.as_str(), &key), holder: Some(node.clone()) }).collect()
        }
        _ => Vec::new(),
    }
}

/// A gate that is always open: a fixture with no membership and no seat fence.
pub struct OpenGate;

#[async_trait::async_trait]
impl Gate for OpenGate {
    fn authorizes(&self) -> bool {
        true
    }
    async fn fence(&self) -> Result<Option<String>, BuildStateError> {
        Ok(None)
    }
}

/// Proof that an executor's birth departed: the exact runtime inspected and found exited. Silence
/// is not proof.
#[async_trait::async_trait]
pub trait DepartureProof: Send + Sync {
    /// `Ok` when the birth `executor` names in this admin's view is proven exited; `Err` names why
    /// it is not.
    async fn proven(&self, executor: &PathName) -> Result<(), String>;
    /// Whether the exact birth `executor` names in this admin's view was already proven exited (a
    /// departure an earlier pass, or the drift reconcile, recorded): a drive that finds its claim's
    /// holder recorded so claims the next attempt without calling or proving it again.
    async fn recorded(&self, _executor: &PathName) -> bool {
        false
    }
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
}

/// Calls the executor `executor` names for one claimed attempt.
#[async_trait::async_trait]
pub trait Dispatcher: Send + Sync {
    /// `build.attempt.run` for `attempt` of `build_id` at `executor`, carrying the won claim, the
    /// Build's intent (`intent`: postcard frames of the Build topic's message) and the run the
    /// fabric-primary planned (`plan`): the operations the executor runs and nothing else.
    async fn dispatch(&self, executor: &PathName, build_id: &BuildId, attempt: u32, context: CallContext, intent: Vec<Vec<u8>>, plan: RunPlan) -> Dispatched;
    /// Hold the run of `attempt` of `build_id` on the admin `me` (the drive's own) while a step of the
    /// Build runs in the drive itself (the holder's own node.primary.update): a dispatch of the attempt
    /// to `me` reattaches to it ([`crate::build_run::HeldRun`]). `None` when it holds no run here.
    fn hold(&self, _me: &PathName, _build_id: &BuildId, _attempt: u32) -> Option<crate::build_run::HeldRun> {
        None
    }
}

/// Why a drive ended without the seat: what the old seat says, and the admin the committed fabric
/// seat record names as the seat's holder now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatLoss {
    /// Why this admin may not act as the fabric-primary.
    pub why: String,
    /// The admin (path.name) the committed fabric seat record names as the holder, when it names a
    /// live birth whose path.name is known here.
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
        Arc::new(Self { build_id, first_attempt: std::sync::atomic::AtomicU32::new(0), state: Mutex::new(DriveState::default()), wake: Notify::new(), running: AtomicBool::new(false) })
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

    /// The seat moved: the callers' streams end, and a caller re-submits by build id at the new seat.
    pub fn lose_seat(&self, loss: SeatLoss) {
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
    /// `to`, with the node the receipt names (the lifecycle op a `NodeDeleting` or `NodeDeleted`
    /// decided): a read of receipts, never a run.
    fn receipt_frames(&self, steps: &[crate::build_state::BuildStepReceipt], from: u32, to: u32) -> Vec<(BuildReply, Option<crate::model::NodeId>)> {
        steps
            .iter()
            .filter(|s| s.outcome == crate::build_state::StepOutcome::Complete && s.attempt >= from && s.attempt < to)
            .map(|s| {
                let subject = s.output.clone().and_then(|o| serde_json::from_value::<rafka_mesh_entity::LifecycleOp>(o).ok()).map(|op| op.node_id);
                (BuildReply::Step { build_id: self.build_id.to_string(), attempt: s.attempt, operation: s.operation.clone(), step: s.step.clone() }, subject)
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
        DriveReader { drive: self.clone(), cursor: 0, from_attempt, done: false }
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
    /// The seat moved; the stream ends with the typed new-seat frame, and the caller re-submits.
    SeatLost(SeatLoss),
}

/// A caller's read position in a [`Drive`].
pub struct DriveReader {
    drive: Arc<Drive>,
    cursor: usize,
    from_attempt: u32,
    done: bool,
}

impl DriveReader {
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

/// How one pass of a drive ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriveEnd {
    /// The Build reached a terminal: nothing more to drive.
    Terminal,
    /// The drive cannot go on now; the next pass of the reconcile loop tries again.
    Stalled(String),
    /// The view may not authorize anything now (cut off); the next pass tries again.
    Paused(String),
    /// This admin does not hold the seat: the callers' streams end.
    SeatLost(String),
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
    /// The cut-off rule and the seat fence.
    pub gate: Arc<dyn Gate>,
    /// Proof of an executor's departure.
    pub departure: Arc<dyn DepartureProof>,
    /// Records an executor's verdict on this admin's log.
    pub verdicts: Arc<dyn VerdictSink>,
    /// The record of each parked predecessor a replace left at `<path>.old`, which admits its
    /// `node.delete`: the live `Step` frames of a replace and a delete feed it, and write no row
    /// (`crate::node_rows::NodeRows::on_step`).
    pub rows: Arc<crate::node_rows::NodeRows>,
    /// The admin's park gate (`crate::park_gate`): a drive takes the view lock and calls executors,
    /// so each of its passes is a turn begun at the gate's wait point, and a stop's quiesce waits for
    /// the pass in progress.
    pub turns: crate::park_gate::ParkGate,
}

/// The drives this admin holds, by Build.
#[derive(Default)]
pub struct Drives {
    drives: Mutex<HashMap<BuildId, Arc<Drive>>>,
}

impl Drives {
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
        if !drive.running.swap(true, Ordering::SeqCst) {
            let (env, d, drives) = (env.clone(), drive.clone(), self.clone());
            let parent = tracing::Span::current();
            tokio::spawn(
                async move {
                    let end = env.run(&d).await;
                    d.running.store(false, Ordering::SeqCst);
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
        }
        drive
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

    /// The drive of `build_id` when one is held (running, stalled, paused or completed).
    pub fn get(&self, build_id: &BuildId) -> Option<Arc<Drive>> {
        self.drives.lock().unwrap().get(build_id).cloned()
    }
}

fn parse_path(s: &str) -> Option<PathName> {
    s.parse().ok()
}

/// The node an operation key names (`create-node:mesh2.admin.1`), when it names one.
fn node_of_key(key: &str) -> Option<PathName> {
    key.split_once(':').and_then(|(_, node)| parse_path(node))
}

/// What is left of `p` in view `t`: the plan, less every operation a run of the Build's current run
/// completed whose executor reported the run's end ([`BuildProjection::reported_in_run`]), by its
/// `Complete` receipt on this admin's log ([`BuildProjection::completed_in_run`]) or by the
/// `Complete` frame the executor's stream reply carried to `drive` ([`Drive::completed_in`]). What a
/// run completed is read from those, never from the view: a view that has not absorbed a completed
/// operation yet still lists it, and it is not planned, claimed or dispatched again. The stream reply
/// is the authority: a remote executor's receipt reaches this admin's log by the Build topic, and
/// nothing waits for that copy. A lost executor's run reported nothing: what it left is planned from
/// the view again, and the create pipeline's fence decides against the world.
/// Whether an attempt ended incomplete: `me` ran it as its executor and it was handed back to `me`
/// having moved nothing. A hand-off to `me` that another executor reported is that executor's end,
/// and the drive attaching to the finished run of it replays the same terminal.
fn ended_incomplete(me: &PathName, executor: &PathName, to: &PathName, before: &str, after: &str) -> bool {
    executor == me && to == me && before == after
}

/// What the next claim of a Build decides on: the first operation left, the receipts the Build
/// holds, and the members of the mesh that operation acts on, by birth, status and seat. An attempt
/// that ended with this unchanged would end the same way again.
fn world(drive: &Drive, p: &BuildProjection, t: &Topology) -> String {
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
    format!("{} receipts={} members=[{}]", first.map(|op| op.key()).unwrap_or_default(), p.steps.len(), members.join(","))
}

fn left(drive: &Drive, p: &BuildProjection, t: &Topology) -> Vec<BuildOperation> {
    crate::accepted::plan_for_build(p, t).operations.into_iter().filter(|op| !p.completed_in_run(&op.key()) && !drive.completed_in(&op.key(), |a| p.reported_in_run(a))).collect()
}

/// The node-admin of `mesh` the Build already carried across the R-D1 fence: an operation on it has a
/// step receipt in an attempt held by an admin outside `mesh`, later than the last attempt an admin of
/// `mesh` held (a mesh that has executed its own work since stands on its own again). That admin is
/// the mesh's first recovering node-admin (mesh-investigation.md step 6: the restore runs on the SAME
/// accepted Build), read from the Build's facts whichever drive dispatched it and whichever admin
/// outside the mesh held its attempt.
pub(crate) fn crossed_admin(p: &BuildProjection, mesh: &str) -> Option<PathName> {
    let holder = |attempt: u32| p.attempt_executors.get(&attempt).and_then(|e| parse_path(e));
    let own = p.attempt_executors.iter().filter(|(_, e)| parse_path(e).is_some_and(|e| e.mesh == mesh)).map(|(a, _)| *a).max().unwrap_or(0);
    let mut steps: Vec<&crate::build_state::BuildStepReceipt> = p.steps.iter().filter(|s| s.attempt > own).collect();
    steps.sort_by_key(|s| s.attempt);
    steps.into_iter().find_map(|s| {
        let node = node_of_key(&s.operation).filter(|n| n.kind == NodeKind::NodeAdmin && n.mesh == mesh)?;
        holder(s.attempt).filter(|h| h.mesh != mesh).map(|_| node)
    })
}

enum Inner {
    Terminal(BuildReply),
    Refused(BuildReply),
    Unreached(String),
    Broke(String),
    /// The Build log's seat fence was set while the executor's run was open: this admin's authority
    /// ended at the fence, and the drive ends there without waiting for the run's stream.
    Fenced,
}

impl DriveEnv {
    /// Drive `drive`'s Build until it ends, stalls or loses the seat.
    pub async fn run(self: &Arc<Self>, drive: &Arc<Drive>) -> DriveEnd {
        let id = drive.build_id.clone();
        let span = tracing::info_span!("rdm.node_admin.build.update.via-drive", build_id = %id, node = %self.me, outcome = tracing::field::Empty, detail = tracing::field::Empty, dispatches = tracing::field::Empty);
        let mut dispatches = 0u32;
        let end = self.pass(drive, &mut dispatches).instrument(span.clone()).await;
        span.record("dispatches", dispatches);
        match &end {
            DriveEnd::Terminal => span.record("outcome", "terminal"),
            DriveEnd::Stalled(w) => span.record("outcome", "stalled").record("detail", w.as_str()),
            DriveEnd::Paused(w) => span.record("outcome", "paused").record("detail", w.as_str()),
            DriveEnd::SeatLost(w) => span.record("outcome", "seat-lost").record("detail", w.as_str()),
        };
        end
    }

    /// The seat, the cut-off rule and the target: `Some` when this admin may not claim or dispatch now.
    /// The committed fabric seat record is read first: it is the target every decision is judged
    /// against (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 5:
    /// "From this write the holder is no longer primary"), and it names the holder this admin's
    /// authority ended at, while the view follows it.
    async fn may_act(&self, drive: &Arc<Drive>) -> Option<DriveEnd> {
        let record_holder = match self.gate.fence().await {
            Ok(holder) => holder,
            Err(e) => {
                let why = e.to_string();
                // The record names the holder (its path.name from the digest book); none only when no
                // digest of that birth is held, and the reason says so.
                let to = match e {
                    BuildStateError::Fenced { to, .. } => to,
                    _ => None,
                };
                tracing::info_span!("rdm.node_admin.build.reject.via-fenced", build_id = %drive.build_id, node = %self.me, fenced = true, detail = %why, to = to.as_deref().unwrap_or("none")).in_scope(|| tracing::info!("dispatch refused: the seat record names another holder"));
                drive.lose_seat(SeatLoss { why: why.clone(), to });
                return Some(DriveEnd::SeatLost(why));
            }
        };
        // The seat is this admin's when the view's holder is the birth it was born as (its node id):
        // another birth at the same path.name (a successor, a retired birth) is not this admin.
        let (held, seat) = {
            let t = self.topology.read().await;
            (t.holds_fabric_seat(&self.door.node_id), t.fabric_primary().map(|n| (n.name.clone(), n.node_id.clone())))
        };
        if !held {
            // Where the seat is comes from the committed seat record, never the view: past the
            // fence it names this birth, none, or a birth proven gone (no holder).
            let shows = seat.as_ref().map(|(name, id)| format!("{name} ({id})")).unwrap_or_else(|| "none".into());
            let names = record_holder.as_deref().unwrap_or("none");
            let why = format!("{} ({}) does not hold the fabric-primary seat in its view (it shows {shows}); the committed fabric seat record names {names}", self.me, self.door.node_id);
            tracing::info_span!("rdm.node_admin.build.reject.via-not-fabric-primary", build_id = %drive.build_id, node = %self.me, detail = %why, to = names).in_scope(|| tracing::info!("dispatch refused: the seat is not held"));
            drive.lose_seat(SeatLoss { why: why.clone(), to: record_holder });
            return Some(DriveEnd::SeatLost(why));
        }
        if let Some(why) = self.gate.frozen() {
            return Some(DriveEnd::Paused(why));
        }
        if !self.gate.authorizes() {
            let why = format!("{}: its view is cut off or within one silence window of healing, and authorizes nothing", self.me);
            if drive.stall_is_new(&why) {
                tracing::info_span!("rdm.node_admin.build.reject.via-cut-off", build_id = %drive.build_id, node = %self.me).in_scope(|| tracing::info!("dispatch refused: a cut-off view authorizes nothing"));
            }
            return Some(DriveEnd::Paused(why));
        }
        None
    }

    fn stalled(&self, drive: &Arc<Drive>, why: String) -> DriveEnd {
        if drive.stall_is_new(&why) {
            tracing::info_span!("rdm.node_admin.build.update.via-stalled", build_id = %drive.build_id, node = %self.me, reason = %why).in_scope(|| tracing::info!("the drive cannot go on until the world changes"));
        }
        DriveEnd::Stalled(why)
    }

    /// The end of the fabric lane's `attempt` (its terminal, or its take-over from a proven-departed
    /// executor) releases its executor's mesh's in-flight row (R-BI7 caveat 3), while the row still
    /// names that attempt. A release that cannot be written is named: the mesh stays held, and the
    /// next claim on it is refused naming this attempt.
    async fn release_mesh(&self, id: &BuildId, attempt: u32, executor: &PathName) {
        let row = crate::mesh_claims::InFlight { build_id: id.clone(), lane: None, attempt, claimant: self.me.to_string() };
        if let Err(e) = self.door.claims.release(&executor.mesh, &row).await {
            tracing::info_span!("rdm.node_admin.build.reject.via-in-flight-unreleased", build_id = %id, attempt, mesh = %executor.mesh, node = %self.me, error = %e)
                .in_scope(|| tracing::info!("the attempt ended and its mesh's in-flight row could not be released: the mesh stays held by it"));
        }
    }

    /// A blocker the drive itself holds: framed once for a new reason.
    fn blocked(&self, drive: &Arc<Drive>, attempt: u32, operation: &str, step: &str, reason: &str) {
        drive.push(BuildReply::Blocked { build_id: drive.build_id.to_string(), attempt, operation: operation.into(), step: step.into(), reason: crate::build_run::bounded_reason(reason.into()) });
    }

    /// What the peer-mesh investigation holds of the mesh of `op`'s node when that mesh is a peer
    /// mesh it holds unheard (R-D1: loss of peer-mesh control-plane visibility fences lifecycle
    /// mutation for every node in that mesh): the mesh, how long it is unheard, and whether its
    /// rebirth is decided. `None` for a mesh operation, a node of this admin's own mesh, or a mesh
    /// the investigation does not hold unheard.
    fn unheard_peer(&self, op: &BuildOperation) -> Option<(PathName, u64, bool)> {
        let node = op.node()?;
        if node.mesh == self.me.mesh {
            return None;
        }
        let peer = self.gate.peer_mesh(&node.mesh)?;
        Some((node.clone(), peer.unheard_ms?, peer.rebirth_decided))
    }

    /// The restore the investigation's rebirth verdict authorizes (R-D1; mesh-investigation.md steps
    /// 5-6): `op` acts on a node-admin of a peer mesh held unheard whose rebirth is decided, and the
    /// Build carried no other admin of that mesh across the fence ([`crossed_admin`]). The
    /// fabric-primary executes it, whatever else the mesh's view holds (an admin held silent
    /// included): it is the mesh's first recovering node-admin.
    fn rebirth_admits(&self, p: &BuildProjection, op: &BuildOperation) -> bool {
        self.unheard_peer(op).is_some_and(|(node, _, decided)| decided && node.kind == NodeKind::NodeAdmin && crossed_admin(p, &node.mesh).is_none_or(|crossed| crossed == node))
    }

    /// The fence on `op` (R-D1): a node of a peer mesh held unheard is not claimed or dispatched for,
    /// except the mesh's first recovering node-admin once the investigation decided its rebirth
    /// ([`Self::rebirth_admits`]). `Some` names the fence.
    fn fenced(&self, p: &BuildProjection, op: &BuildOperation) -> Option<(PathName, u64, bool)> {
        if self.rebirth_admits(p, op) {
            return None;
        }
        self.unheard_peer(op)
    }

    /// The drive stops at the fence on `op` by name: once per mesh, operation and verdict on its own
    /// span, framed as the attempt's blocker, and stalled until the investigation's state changes.
    fn stop_at_fence(&self, drive: &Arc<Drive>, attempt: u32, op: &BuildOperation, (node, unheard_ms, decided): (PathName, u64, bool)) -> DriveEnd {
        let why = format!(
            "{}: {} acts on {node} of peer mesh {mesh}, which the peer-mesh investigation holds unheard ({unheard_ms} ms){}: nothing is claimed or dispatched for that mesh until it is heard again (R-D1)",
            self.me,
            op.key(),
            if decided { "; its decided rebirth admits only the mesh's first recovering node-admin" } else { ", with no rebirth decided" },
            mesh = node.mesh,
        );
        if drive.stall_is_new(&format!("unheard-mesh|{}|{}|{decided}", node.mesh, op.key())) {
            tracing::info_span!("rdm.node_admin.build.reject.via-unheard-mesh-drive", build_id = %drive.build_id, attempt, node = %self.me, operation = %op.key(), target = %node, mesh = %node.mesh, unheard_ms, rebirth_decided = decided)
                .in_scope(|| tracing::info!("the drive claims and dispatches nothing for a node of a peer mesh held unheard"));
        }
        self.blocked(drive, attempt, &op.key(), "unheard-mesh", &why);
        DriveEnd::Stalled(why)
    }

    /// What this admin dispatches to `executor` for what is left of `p` in view `t` ([`left`]). The
    /// mesh's first recovering node-admin under a rebirth verdict runs alone, on this admin (the
    /// fabric-primary); its receipts are what fence every other admin of that mesh. Otherwise the executor's run
    /// ([`run_of`]) is cut before the first operation on a peer mesh held unheard (R-D1) and before
    /// an operation no admin executes in this view (it waits for its mesh's own seat, never for the
    /// run's executor); a cut run hands back to this admin, whose next claim re-reads the view and
    /// the investigation. `Err` names the fence on the first operation.
    fn plan_dispatch(&self, drive: &Drive, p: &BuildProjection, t: &Topology, executor: &PathName) -> Result<RunPlan, (BuildOperation, (PathName, u64, bool))> {
        let ops = left(drive, p, t);
        if let Some(op) = ops.first().filter(|op| *executor == self.me && self.rebirth_admits(p, op)) {
            return Ok(RunPlan { operations: vec![op.clone()], hand_off_to: (ops.len() > 1).then(|| self.me.to_string()) });
        }
        let mut plan = run_of(&ops, t, executor);
        let mut cut = None;
        for (i, op) in plan.operations.iter().enumerate() {
            if let Some(f) = self.fenced(p, op) {
                if i == 0 {
                    return Err((op.clone(), f));
                }
                cut = Some(i);
                break;
            }
            if i > 0 && (self.unheard_peer(op).is_some() || crate::executor::executor_for(op, t).is_none()) {
                cut = Some(i);
                break;
            }
        }
        if let Some(i) = cut {
            plan.operations.truncate(i);
            plan.hand_off_to = Some(self.me.to_string());
        }
        Ok(plan)
    }

    async fn pass(self: &Arc<Self>, drive: &Arc<Drive>, dispatches: &mut u32) -> DriveEnd {
        let id = drive.build_id.clone();
        let mut handed_to: Option<PathName> = None;
        let mut departed: Option<String> = None;
        loop {
            // Each pass is one turn of the admin's gate (park_gate.rs): it begins here, holding
            // nothing, and while the admin is stopped it begins only once the admin starts. A pass in
            // progress when a stop parks the gate runs to its end (the attempt it dispatched, its
            // terminal handled) and the stop's quiesce waits for it, so nothing of it follows the cut.
            let _turn = self.turns.turn("build-drive").await;
            if let Some(end) = self.may_act(drive).await {
                return end;
            }
            let p = match self.builds.read_build(&id).await {
                Ok(p) => p,
                Err(e) => return self.stalled(drive, format!("{}: Build {id} could not be read: {e}", self.me)),
            };
            if p.state == BuildState::Complete {
                if !drive.has_run_log() {
                    // This drive made no call of the Build: a fabric-primary that took over after
                    // the Build ended holds no frame of it. The executor of the last attempt replays
                    // it from the receipts it wrote itself, and the facts of earlier attempts are
                    // read; the Build topic's step facts are never the answer for the last attempt.
                    if let Some(end) = self.reattach_to_ended(drive, &p).await {
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
                Ok(false) => return self.stalled(drive, format!("{}: Build {id} is neither the current Build nor a failed Build accepted after it: nothing drives it", self.me)),
                Err(e) => return self.stalled(drive, format!("{}: the current Build could not be read to admit Build {id}: {e}", self.me)),
            }
            // The claim's holder was proven departed: by this pass, or by an earlier pass or the
            // drift reconcile, whose proof this admin recorded for its exact birth. Either way the
            // next attempt is claimed without calling the lost holder or proving it again.
            let recorded = match (&departed, p.state == BuildState::Running, p.executor.as_deref().and_then(parse_path)) {
                (None, true, Some(holder)) => self.departure.recorded(&holder).await,
                _ => false,
            };
            let take_over = recorded || departed.as_deref().is_some_and(|d| p.executor.as_deref() == Some(d));
            if recorded {
                departed = p.executor.clone();
            }
            let (attempt, executor, context) = if p.state == BuildState::Running && !take_over {
                let holder = p.executor.clone().unwrap_or_default();
                let Some(holder) = parse_path(&holder) else {
                    return self.stalled(drive, format!("{}: attempt {} of Build {id} is claimed by {holder:?}, which is not a path.name", self.me, p.attempt));
                };
                (p.attempt, holder, self.door.contexts.get(&id, p.attempt).await.ok().flatten().unwrap_or_default())
            } else {
                if let Some(end) = self.idle_until_moved(drive, &p).await {
                    return end;
                }
                match self.seats_first(drive, &p).await {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(end) => return end,
                }
                // A taken-over attempt ends with its proven-departed executor: its hold on that
                // executor's mesh goes before the next attempt takes the mesh.
                if take_over {
                    if let Some(lost) = p.executor.as_deref().and_then(parse_path) {
                        self.release_mesh(&id, p.attempt, &lost).await;
                    }
                }
                match self.claim_next(drive, &p, handed_to.take()).await {
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
            let plan = {
                let t = self.topology.read().await.clone();
                match self.plan_dispatch(drive, &p, &t, &executor) {
                    Ok(plan) => plan,
                    Err((op, f)) => return self.stop_at_fence(drive, attempt, &op, f),
                }
            };
            // A seat update a committed row records before an operation of this run is acknowledged
            // first, whether this admin moved the seat, resumes after another admin's row, or holds the
            // seat from that row already: node.primary.updated answered to this executor, never the
            // view alone (proposal Part C1 failure contract), in the claim's attempt and in a re-dispatch.
            if let Some(end) = self.updates_acknowledged(drive, &plan, attempt).await {
                return end;
            }
            *dispatches += 1;
            let dspan = tracing::info_span!("rdm.node_admin.build.update.via-dispatch", build_id = %id, attempt, executor = %executor, node = %self.me, outcome = tracing::field::Empty, reason = tracing::field::Empty);
            if let Some(tp) = &context.traceparent {
                rafka_mesh_telemetry::set_remote_parent(&dspan, tp, context.tracestate.as_deref());
            }
            // What the attempt decides on, as claimed: an attempt handed back to this admin with this
            // unchanged would end the same way again.
            let before = match self.builds.read_build(&id).await {
                Ok(claimed) => Some(world(drive, &claimed, &self.topology.read().await.clone())),
                Err(_) => None,
            };
            let inner = self.run_inner(drive, &id, attempt, &executor, context, plan).instrument(dspan.clone()).await;
            drive.clear_stall();
            match inner {
                Inner::Terminal(t @ BuildReply::Complete { .. }) => {
                    dspan.record("outcome", "complete");
                    self.verdicts.record(BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await;
                    self.release_mesh(&id, attempt, &executor).await;
                    drive.finish(t);
                    return DriveEnd::Terminal;
                }
                Inner::Terminal(t @ BuildReply::Failed { .. }) => {
                    dspan.record("outcome", "failed");
                    if let BuildReply::Failed { operation, reason, .. } = &t {
                        if !operation.is_empty() && !reason.contains("(reason cut:") {
                            self.verdicts.record(BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Failed { reason: reason.clone() } }).await;
                        }
                    }
                    self.release_mesh(&id, attempt, &executor).await;
                    drive.finish(t);
                    return DriveEnd::Terminal;
                }
                Inner::Terminal(BuildReply::HandedOff { to, .. }) => {
                    dspan.record("outcome", "handed-off");
                    self.verdicts.record(BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::HandedOff { to: to.clone() } }).await;
                    self.release_mesh(&id, attempt, &executor).await;
                    let Some(next) = parse_path(&to) else {
                        return self.stalled(drive, format!("attempt {attempt} of Build {id} handed off to {to:?}, which is not a path.name"));
                    };
                    // An attempt handed back to this admin that moved nothing (it ended incomplete: a
                    // participant lost, a proof not in) would be claimed again and end the same way: the
                    // Build stays in progress, and the next attempt waits for what it decides on to move.
                    if next == self.me {
                        if let (Some(before), Ok(ended)) = (before, self.builds.read_build(&id).await) {
                            let after = world(drive, &ended, &self.topology.read().await.clone());
                            if ended_incomplete(&self.me, &executor, &next, &before, &after) {
                                let why = format!(
                                    "{}: attempt {attempt} of Build {id} ended incomplete and was handed back to this fabric-primary with nothing moved ({after}): the Build stays in progress and no next attempt is claimed until what it decides on moves",
                                    self.me
                                );
                                self.blocked(drive, attempt, "", "incomplete", &why);
                                drive.set_idle(Some(Idle { attempt, world: after, why: why.clone() }));
                                return self.stalled(drive, why);
                            }
                        }
                    }
                    handed_to = Some(next);
                    departed = None;
                }
                Inner::Terminal(other) => {
                    dspan.record("outcome", "protocol-violation");
                    return self.stalled(drive, format!("{executor} ended attempt {attempt} with {}, which is not a terminal of the attempt", other.name()));
                }
                Inner::Refused(r) => {
                    dspan.record("outcome", r.name());
                    let why = format!("{executor} refused attempt {attempt} of Build {id}: {}", describe(&r));
                    self.blocked(drive, attempt, "", "dispatch", &why);
                    return self.stalled(drive, why);
                }
                Inner::Fenced => {
                    dspan.record("outcome", "fenced");
                    // The fence is sticky: `may_act` ends the drive SeatLost naming the new holder.
                    return match self.may_act(drive).await {
                        Some(end) => end,
                        None => self.stalled(drive, format!("{}: attempt {attempt} of Build {id} stopped at a seat fence that the gate no longer reports", self.me)),
                    };
                }
                Inner::Unreached(why) | Inner::Broke(why) => {
                    let (class, detail) = ("lost", why);
                    dspan.record("outcome", "executor-lost");
                    // Why the inner call to the executor ended: what the next step (the departure proof) answers.
                    dspan.record("reason", detail.as_str());
                    // Only a proven departure lets another admin take the attempt; silence never does.
                    if departed.is_some() {
                        return self.stalled(drive, format!("{executor} was proven departed and the attempt claimed after it could not be dispatched either: {detail}"));
                    }
                    match self.departure.proven(&executor).await {
                        Ok(()) => {
                            tracing::info_span!("rdm.node_admin.build.update.via-departure-proven", build_id = %id, attempt, executor = %executor, node = %self.me)
                                .in_scope(|| tracing::info!("the executor's exact runtime is proven exited: the next attempt may be claimed"));
                            departed = Some(executor.to_string());
                        }
                        Err(not_proven) => {
                            let why = format!("executor {executor} is {class} ({detail}); its departure is not proven ({not_proven}); the attempt is not claimed for another admin");
                            self.blocked(drive, attempt, "", "departure-proof", &why);
                            return self.stalled(drive, why);
                        }
                    }
                }
            }
        }
    }

    /// Every seat update a committed row records before an operation of `plan`, acknowledged through
    /// the gate ([`Gate::seats_acknowledged`]); `Some` ends the drive stalled, the attempt blocked
    /// naming the updates and why, when one was not.
    async fn updates_acknowledged(&self, drive: &Arc<Drive>, plan: &RunPlan, attempt: u32) -> Option<DriveEnd> {
        let mut updates: Vec<(String, PrecedingUpdate)> = plan.operations.iter().flat_map(|op| preceding_updates(op, &drive.build_id).into_iter().map(move |u| (op.key(), u))).collect();
        updates.dedup();
        let Some((first, _)) = updates.first().cloned() else { return None };
        let asked: Vec<PrecedingUpdate> = updates.into_iter().map(|(_, u)| u).collect();
        let why = self.gate.seats_acknowledged(&asked, &drive.build_id, attempt).await.err()?;
        let names = asked.iter().map(|u| format!("{} under mesh {}", u.operation, u.mesh)).collect::<Vec<_>>().join(", ");
        let why = format!("{}: attempt {attempt} of Build {} dispatches nothing until node.primary.updated is acknowledged for the seat update a committed row records before it (one of {names}): {why}", self.me, drive.build_id);
        self.blocked(drive, attempt, &first, "primary-update", &why);
        Some(self.stalled(drive, why))
    }

    /// The seats the first operation left moves before it runs (R-MT1, [`crate::executor::seat_move`]):
    /// node.primary.update through the gate. A mesh-primary's seat is commanded from here, and the
    /// fabric-primary keeps its own. `Ok(true)` once a seat moved (the loop passes the gates again: a
    /// moved fabric seat ends this drive there), `Ok(false)` when no seat is owed, and the end, named,
    /// when the seat did not move.
    async fn seats_first(&self, drive: &Arc<Drive>, p: &BuildProjection) -> Result<bool, DriveEnd> {
        let t = self.topology.read().await.clone();
        let Some(op) = left(drive, p, &t).into_iter().next() else { return Ok(false) };
        let Some(m) = crate::executor::seat_move(&op, &t) else { return Ok(false) };
        let attempt = p.attempt + 1;
        if m.holder.node_id == self.door.node_id {
            return self.own_seats_first(drive, &op, &m, attempt).await;
        }
        if let Err(why) = self.gate.move_seats(&m, &drive.build_id, attempt, &op.key()).await {
            let why = format!("{}: the seat of {} did not move before {}: {why}", self.me, m.holder.name, op.key());
            self.blocked(drive, attempt, &op.key(), "primary-update", &why);
            return Err(self.stalled(drive, why));
        }
        if still_holds(&op, &m, &self.topology.read().await.clone()) {
            let why = format!("{}: node.primary.update of {} answered, and this view still names it the holder of the seat {} moves", self.me, m.holder.name, op.key());
            self.blocked(drive, attempt, &op.key(), "primary-update", &why);
            return Err(self.stalled(drive, why));
        }
        Ok(true)
    }

    /// This admin's own seats move before `op`, as a step of the Build (the approved proposal
    /// node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 1: "The holder is commanded from
    /// above, as a step of the Build"): attempt `attempt` is claimed for this admin first and its run is
    /// held here while node.primary.update runs, so a successor that holds the seats from the row
    /// reattaches to this attempt and goes on only once it ends, after this holder said "primary
    /// updated" (step 8: "Only then does the commanding Build continue"; build.md: "A live run is
    /// reattached, never re-claimed"). The attempt ends `HandedOff` to the fabric-primary of the view the
    /// update ends with: the successor once the row moved the seats, this admin when nothing moved, and
    /// then the drive waits for what its next attempt decides on to move ([`world`], R-BI4).
    async fn own_seats_first(&self, drive: &Arc<Drive>, op: &BuildOperation, m: &crate::executor::SeatMove, attempt: u32) -> Result<bool, DriveEnd> {
        let id = &drive.build_id;
        let context = match self.door.claim(&self.me.to_string(), id, attempt).await {
            BuildClaimReply::Won { context } => context,
            other => {
                let why = format!("{}: its seats move before {} as a step of attempt {attempt} of Build {id}, and that attempt was not claimed for it: {}", self.me, op.key(), claim_refusal(&other));
                self.blocked(drive, attempt, &op.key(), "primary-update", &why);
                return Err(self.stalled(drive, why));
            }
        };
        let held = self.dispatcher.hold(&self.me, id, attempt);
        let dspan = tracing::info_span!("rdm.node_admin.build.update.via-dispatch", build_id = %id, attempt, executor = %self.me, node = %self.me, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        if let Some(tp) = &context.traceparent {
            rafka_mesh_telemetry::set_remote_parent(&dspan, tp, context.tracestate.as_deref());
        }
        let moved = self.gate.move_seats(m, id, attempt, &op.key()).instrument(dspan.clone()).await;
        let after = self.topology.read().await.clone();
        let still = still_holds(op, m, &after);
        let to = match after.fabric_primary() {
            Some(fp) if !still => fp.name.clone(),
            _ => self.me.clone(),
        };
        let terminal = BuildReply::HandedOff { build_id: id.to_string(), attempt, to: to.to_string() };
        // The verdict's writer releases the mesh's in-flight row the claim took (R-BI7 caveat 3), at
        // the verdict and before a reader of the held run learns the attempt ended: a successor that
        // reads the verdict claims its next attempt on the same mesh rows.
        match held {
            Some(held) => held.finish(terminal, self.release_mesh(id, attempt, &self.me)).await,
            None => match self.builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::HandedOff { to: to.to_string() }, mesh: None }).await {
                Ok(()) => self.release_mesh(id, attempt, &self.me).await,
                Err(e) => {
                    dspan.record("reason", format!("the verdict of attempt {attempt} could not be recorded: {e}").as_str());
                }
            },
        }
        dspan.record("outcome", "handed-off");
        match (moved, still) {
            (Ok(()), false) => Ok(true),
            (Err(why), false) => {
                // After the row the seats are the successor's whatever the call returned (C1 failure
                // contract): the drive goes on to the gates, which end it at the moved seat.
                let why = format!("{}: its seats moved to {to} by the committed row before {}, and node.primary.update did not complete: {why}", self.me, op.key());
                dspan.record("reason", why.as_str());
                self.blocked(drive, attempt, &op.key(), "primary-update", &why);
                Ok(true)
            }
            (moved, true) => {
                let why = match moved {
                    Err(why) => format!("{}: the seat of {} did not move before {}: {why}", self.me, m.holder.name, op.key()),
                    Ok(()) => format!("{}: node.primary.update of {} answered, and this view still names it the holder of the seat {} moves", self.me, m.holder.name, op.key()),
                };
                dspan.record("reason", why.as_str());
                self.blocked(drive, attempt, &op.key(), "primary-update", &why);
                if let Ok(ended) = self.builds.read_build(id).await {
                    drive.set_idle(Some(Idle { attempt, world: world(drive, &ended, &after), why: why.clone() }));
                }
                Err(self.stalled(drive, why))
            }
        }
    }

    /// The drive waits, named, while the attempt it handed back to itself with nothing moved has not
    /// seen what its resume decides on move ([`world`]); the stall is the same reason, named once.
    async fn idle_until_moved(&self, drive: &Arc<Drive>, p: &BuildProjection) -> Option<DriveEnd> {
        let idle = drive.idle()?;
        if idle.attempt != p.attempt {
            drive.set_idle(None);
            return None;
        }
        let now = world(drive, p, &self.topology.read().await.clone());
        if now == idle.world {
            return Some(self.stalled(drive, idle.why));
        }
        drive.set_idle(None);
        None
    }

    /// Claim the next attempt of `p` for the admin that runs what is left (or `forced`, the admin a
    /// run just handed off to), on this admin's own log.
    async fn claim_next(&self, drive: &Arc<Drive>, p: &BuildProjection, forced: Option<PathName>) -> Result<(u32, PathName, CallContext), DriveEnd> {
        let id = &drive.build_id;
        let attempt = p.attempt + 1;
        let first = {
            let t = self.topology.read().await.clone();
            left(drive, p, &t).into_iter().next()
        };
        if let Some((op, f)) = first.as_ref().and_then(|op| self.fenced(p, op).map(|f| (op, f))) {
            return Err(self.stop_at_fence(drive, attempt, op, f));
        }
        // The mesh's first recovering node-admin under the investigation's rebirth verdict is this
        // admin's to execute (R-D1). A run handed back to this admin (its own cut, or an executor whose
        // view names this admin) is claimed for the admin this view names for what is left: an
        // operation no admin executes waits for its mesh's own seat, and one another admin executes is
        // claimed for that admin, never for this one to hand on unrun.
        let rebirth = first.as_ref().is_some_and(|op| self.rebirth_admits(p, op));
        let forced = if rebirth { Some(self.me.clone()) } else { forced.filter(|to| *to != self.me) };
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
                            tracing::info_span!("rdm.node_admin.build.reject.via-no-owner-outside-mesh", build_id = %id, operation = %op.key(), mesh = %mesh)
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
                Some(op) => format!("{}: no admin can execute what is left of Build {id} in this view: {}", self.me, crate::executor::why_no_executor(op, &t)),
                None => format!("{}: no admin can execute what is left of Build {id} in this view", self.me),
            };
            self.blocked(drive, attempt, "", "successor-held", &why);
            return Err(self.stalled(drive, why));
        };
        match self.door.claim(&lead.to_string(), id, attempt).await {
            BuildClaimReply::Won { context } => Ok((attempt, lead, context)),
            BuildClaimReply::Lost { holder } => match parse_path(&holder) {
                Some(h) => {
                    let context = self.door.contexts.get(id, attempt).await.ok().flatten().unwrap_or_default();
                    Ok((attempt, h, context))
                }
                None => Err(self.stalled(drive, format!("attempt {attempt} of Build {id} is held by {holder:?}, which is not a path.name"))),
            },
            BuildClaimReply::NotOpen { next } => Err(self.stalled(drive, format!("{}: attempt {attempt} of Build {id} is not open (the open attempt is {next:?})", self.me))),
            // Another attempt holds the mesh this one acts in (R-BI7 caveat 3): nothing is claimed or
            // dispatched; the next pass claims once its verdict releases the mesh.
            BuildClaimReply::BuildInProgress { current_build_id, mesh, held_by } => {
                let why = format!("{}: attempt {attempt} of Build {id} acts in mesh {mesh}, which an attempt of Build {current_build_id} holds (claimed by {}): one attempt in flight per mesh", self.me, held_by.as_deref().unwrap_or("no recorded claimant"));
                self.blocked(drive, attempt, "", "mesh-in-flight", &why);
                Err(self.stalled(drive, why))
            }
            other => Err(self.stalled(drive, format!("{}: the claim of attempt {attempt} of Build {id} was not decided: {}", self.me, other.name()))),
        }
    }

    /// The Build as the claim was decided on it: its acceptance and every attempt opened, claimed and
    /// ended, packed as the Build topic's catch-up packs facts, but not its step receipts (those are
    /// the recovery contract of the projection). What an executor needs to fold the same current
    /// attempt and to plan, carried by the call, so a run never waits for the Build topic.
    async fn intent_of(&self, id: &BuildId) -> Vec<Vec<u8>> {
        let facts = match self.builds.facts().await {
            Ok(f) => f.into_iter().filter(|f| f.build_id() == id && !matches!(f, crate::build_state::BuildFact::Step(_) | crate::build_state::BuildFact::Forget { .. })).collect::<Vec<_>>(),
            Err(_) => return Vec::new(),
        };
        let (frames, refused) = crate::fabric_builds::encode_chunks(facts);
        for e in refused {
            tracing::info_span!("rdm.node_admin.build.reject.via-oversized-fact", build_id = %id, detail = %e).in_scope(|| tracing::info!("a fact of the Build fits no frame: the executor holds it only if the Build topic delivered it"));
        }
        frames.into_iter().map(|b| b.to_vec()).collect()
    }

    /// A complete Build this drive made no call of: attach to the last attempt's executor and read
    /// its frames. `None` when the attached run ended `Complete`; otherwise the end of the drive.
    async fn reattach_to_ended(&self, drive: &Arc<Drive>, p: &BuildProjection) -> Option<DriveEnd> {
        let id = &drive.build_id;
        self.replay(drive, &p.steps, 0, p.attempt).await;
        let named = p.executor.clone().unwrap_or_default();
        let Some(holder) = parse_path(&named) else {
            drive.finish(BuildReply::NotReady { reason: format!("{}: attempt {} of Build {id} completed on {named:?}, which is not a path.name", self.me, p.attempt) });
            return Some(DriveEnd::Terminal);
        };
        drive.note_first_attempt(p.attempt);
        let context = self.door.contexts.get(id, p.attempt).await.ok().flatten().unwrap_or_default();
        let span = tracing::info_span!("rdm.node_admin.build.update.via-dispatch", build_id = %id, attempt = p.attempt, executor = %holder, node = %self.me, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        let why = match self.run_inner(drive, id, p.attempt, &holder, context, RunPlan::default()).instrument(span.clone()).await {
            Inner::Terminal(BuildReply::Complete { .. }) => {
                span.record("outcome", "complete");
                return None;
            }
            Inner::Fenced => {
                span.record("outcome", "fenced");
                return Some(match self.may_act(drive).await {
                    Some(end) => end,
                    None => self.stalled(drive, format!("{}: the replay of Build {id} stopped at a seat fence that the gate no longer reports", self.me)),
                });
            }
            Inner::Terminal(t) => format!("it answered {}", t.name()),
            Inner::Refused(r) => format!("it refused: {}", describe(&r)),
            Inner::Unreached(w) | Inner::Broke(w) => w,
        };
        span.record("outcome", "executor-lost");
        span.record("reason", why.as_str());
        drive.finish(BuildReply::NotReady { reason: format!("{}: Build {id} is complete, and {holder}, which ran its last attempt {}, could not replay it: {why}", self.me, p.attempt) });
        Some(DriveEnd::Terminal)
    }

    /// A `Step` frame new to `drive`, live from the executor or read from a receipt (a completed
    /// step's receipt is the same event as its frame): the parked-predecessor record is fed from it
    /// once, from what this fabric-primary holds, before any reader of the drive sees the frame.
    async fn step(&self, drive: &Drive, frame: BuildReply, subject: Option<crate::model::NodeId>) {
        if !drive.take_key(&frame) {
            return;
        }
        if let BuildReply::Step { operation, step, .. } = &frame {
            self.rows.on_step(&self.topology, operation, step, subject.as_ref()).await;
        }
        drive.publish(frame);
    }

    /// The frame of every `Complete` receipt of `steps` from attempt `from` to before attempt `to`,
    /// each through [`Self::step`]: a read of receipts, never a run.
    async fn replay(&self, drive: &Drive, steps: &[crate::build_state::BuildStepReceipt], from: u32, to: u32) {
        for (frame, subject) in drive.receipt_frames(steps, from, to) {
            self.step(drive, frame, subject).await;
        }
    }

    async fn run_inner(&self, drive: &Arc<Drive>, id: &BuildId, attempt: u32, executor: &PathName, context: CallContext, plan: RunPlan) -> Inner {
        let intent = self.intent_of(id).await;
        let mut stream = match self.dispatcher.dispatch(executor, id, attempt, context, intent, plan).await {
            Dispatched::Stream(s) => s,
            Dispatched::Refused(r) => return Inner::Refused(r),
            Dispatched::Unreached(why) => return Inner::Unreached(why),
        };
        loop {
            // Only the wait for the next item races the fence: a frame in hand is handled whole.
            let item = tokio::select! {
                biased;
                _ = self.gate.fence_set() => return Inner::Fenced,
                item = stream.next() => item,
            };
            match item {
                InnerItem::Frame(f) => match f {
                    BuildReply::Step { .. } => self.step(drive, f, None).await,
                    BuildReply::Blocked { .. } => {
                        drive.push(f);
                    }
                    t @ (BuildReply::Complete { .. } | BuildReply::Failed { .. } | BuildReply::HandedOff { .. }) => return Inner::Terminal(t),
                    // The executor took the fabric-primary seat during the run: this admin's seat row
                    // fenced it, and the drive ends at that fence.
                    BuildReply::SeatMoved { .. } => return Inner::Fenced,
                    other => return Inner::Refused(other),
                },
                InnerItem::Ended => return Inner::Broke("the inner stream ended without its terminal".into()),
                InnerItem::Broke(why) => return Inner::Broke(why),
            }
        }
    }
}

/// Whether view `t` still names `m`'s holder the holder of the seats `m` moves before `op`.
fn still_holds(op: &BuildOperation, m: &crate::executor::SeatMove, t: &Topology) -> bool {
    crate::executor::seat_move(op, t).is_some_and(|still| still.holder.node_id == m.holder.node_id && still.mesh_seat == m.mesh_seat && still.fabric_seat == m.fabric_seat)
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
        BuildReply::NotReady { reason } | BuildReply::Busy { reason } | BuildReply::Draining { reason } | BuildReply::PeerUnresolved { reason } | BuildReply::Unauthorized { reason } => format!("{} ({reason})", r.name()),
        other => other.name().to_string(),
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
}
