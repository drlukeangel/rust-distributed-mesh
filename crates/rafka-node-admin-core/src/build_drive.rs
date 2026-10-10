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
//! Before every claim and every dispatch the drive passes three gates: this admin holds the seat
//! in its view, its view may authorize anything at all (a cut-off view dispatches nothing), and
//! the Build log's sticky seat fence is not set (a fabric-primary that yielded dispatches nothing).
//! An executor that cannot be reached is replaced only on proof of its departure, never on
//! silence.

use crate::build::BuildId;
use crate::build_claim::ClaimDoor;
use crate::build_state::{AttemptOutcome, BuildAttemptReceipt, BuildProjection, BuildState, BuildStateAdapter, BuildStateError};
use crate::executor::lead_for;
use crate::model::PathName;
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
    /// The seat fence of the Build log: refused by name once this admin yielded the seat.
    async fn fence(&self) -> Result<(), BuildStateError>;
    /// Why reconciliation is frozen (a fabric shutdown is in force), when it is: no claim and no
    /// dispatch from here on.
    fn frozen(&self) -> Option<String> {
        None
    }
}

/// A gate that is always open: a fixture with no membership and no seat fence.
pub struct OpenGate;

#[async_trait::async_trait]
impl Gate for OpenGate {
    fn authorizes(&self) -> bool {
        true
    }
    async fn fence(&self) -> Result<(), BuildStateError> {
        Ok(())
    }
}

/// Proof that an executor's birth departed: the exact runtime inspected and found exited. Silence
/// is not proof.
#[async_trait::async_trait]
pub trait DepartureProof: Send + Sync {
    /// `Ok` when the birth `executor` names in this admin's view is proven exited; `Err` names why
    /// it is not.
    async fn proven(&self, executor: &PathName) -> Result<(), String>;
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
    /// `build.attempt.run` for `attempt` of `build_id` at `executor`, carrying the won claim and the
    /// Build's intent (`intent`: postcard frames of the Build topic's message).
    async fn dispatch(&self, executor: &PathName, build_id: &BuildId, attempt: u32, context: CallContext, intent: Vec<Vec<u8>>) -> Dispatched;
}

#[derive(Default)]
struct DriveState {
    events: Vec<BuildReply>,
    seen: HashSet<String>,
    terminal: Option<BuildReply>,
    lost: Option<String>,
    stall: Option<String>,
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
        let mut s = self.state.lock().unwrap();
        if s.terminal.is_some() {
            return false;
        }
        if let Some(key) = frame_key(&frame) {
            if !s.seen.insert(key) {
                return false;
            }
        }
        s.events.push(frame);
        drop(s);
        self.wake.notify_waiters();
        true
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
    pub fn lose_seat(&self, reason: String) {
        let mut s = self.state.lock().unwrap();
        if s.lost.is_none() && s.terminal.is_none() {
            s.lost = Some(reason);
        }
        drop(s);
        self.wake.notify_waiters();
    }

    /// The first attempt this drive dispatched.
    fn note_first_attempt(&self, attempt: u32) -> u32 {
        let _ = self.first_attempt.compare_exchange(0, attempt, Ordering::SeqCst, Ordering::SeqCst);
        self.first_attempt.load(Ordering::SeqCst)
    }

    /// Make a `Step` frame readable for every `Complete` receipt of `steps` from attempt `from` to
    /// before attempt `to`: a read of receipts, never a run. A frame whose key is held is dropped.
    pub fn replay_receipts(&self, steps: &[crate::build_state::BuildStepReceipt], from: u32, to: u32) {
        for s in steps.iter().filter(|s| s.outcome == crate::build_state::StepOutcome::Complete && s.attempt >= from && s.attempt < to) {
            self.push(BuildReply::Step { build_id: self.build_id.0.clone(), attempt: s.attempt, operation: s.operation.clone(), step: s.step.clone() });
        }
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
}

/// What a caller reads from a drive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    /// A progress frame.
    Frame(BuildReply),
    /// The terminal frame.
    Terminal(BuildReply),
    /// The seat moved; the stream ends, and the caller re-submits.
    SeatLost(String),
}

/// A caller's read position in a [`Drive`].
pub struct DriveReader {
    drive: Arc<Drive>,
    cursor: usize,
    from_attempt: u32,
    done: bool,
}

impl DriveReader {
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
                if let Some(why) = &s.lost {
                    self.done = true;
                    return Some(Read::SeatLost(why.clone()));
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
    /// `Fabric.build_id` as this admin holds it: only the accepted Build is driven.
    pub accepted: Arc<crate::accepted::AcceptedStore>,
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
}

/// The drives this admin holds, by Build.
#[derive(Default)]
pub struct Drives {
    drives: Mutex<HashMap<BuildId, Arc<Drive>>>,
}

impl Drives {
    /// The drive of `build_id`, with a task making its frames running: the one held, or a new one.
    /// A drive that stalled or paused is picked up again here.
    pub fn ensure(self: &Arc<Self>, env: &Arc<DriveEnv>, build_id: &BuildId) -> Arc<Drive> {
        let drive = {
            let mut drives = self.drives.lock().unwrap();
            if drives.get(build_id).is_some_and(|d| d.ended()) {
                drives.remove(build_id);
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
                    if matches!(end, DriveEnd::Terminal | DriveEnd::SeatLost(_)) {
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

    /// The drive of `build_id` when one is held (running, stalled or paused).
    pub fn get(&self, build_id: &BuildId) -> Option<Arc<Drive>> {
        self.drives.lock().unwrap().get(build_id).cloned()
    }
}

fn parse_path(s: &str) -> Option<PathName> {
    s.parse().ok()
}

enum Inner {
    Terminal(BuildReply),
    Refused(BuildReply),
    Unreached(String),
    Broke(String),
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

    /// The seat, the cut-off rule and the fence: `Some` when this admin may not claim or dispatch now.
    async fn may_act(&self, drive: &Arc<Drive>) -> Option<DriveEnd> {
        let seat = self.topology.read().await.fabric_primary().map(|n| n.name.clone());
        if seat.as_ref() != Some(&self.me) {
            let why = format!("{} does not hold the fabric-primary seat in its view (it shows {})", self.me, seat.map(|s| s.to_string()).unwrap_or_else(|| "none".into()));
            tracing::info_span!("rdm.node_admin.build.reject.via-not-fabric-primary", build_id = %drive.build_id, node = %self.me, detail = %why).in_scope(|| tracing::info!("dispatch refused: the seat is not held"));
            drive.lose_seat(why.clone());
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
        if let Err(e) = self.gate.fence().await {
            let why = e.to_string();
            tracing::info_span!("rdm.node_admin.build.reject.via-fenced", build_id = %drive.build_id, node = %self.me, fenced = true, detail = %why).in_scope(|| tracing::info!("dispatch refused: the seat was yielded"));
            drive.lose_seat(why.clone());
            return Some(DriveEnd::SeatLost(why));
        }
        None
    }

    fn stalled(&self, drive: &Arc<Drive>, why: String) -> DriveEnd {
        if drive.stall_is_new(&why) {
            tracing::info_span!("rdm.node_admin.build.update.via-stalled", build_id = %drive.build_id, node = %self.me, reason = %why).in_scope(|| tracing::info!("the drive cannot go on until the world changes"));
        }
        DriveEnd::Stalled(why)
    }

    /// A blocker the drive itself holds: framed once for a new reason.
    fn blocked(&self, drive: &Arc<Drive>, attempt: u32, operation: &str, step: &str, reason: &str) {
        drive.push(BuildReply::Blocked { build_id: drive.build_id.0.clone(), attempt, operation: operation.into(), step: step.into(), reason: crate::build_run::bounded_reason(reason.into()) });
    }

    async fn pass(self: &Arc<Self>, drive: &Arc<Drive>, dispatches: &mut u32) -> DriveEnd {
        let id = drive.build_id.clone();
        let mut handed_to: Option<PathName> = None;
        let mut departed: Option<String> = None;
        loop {
            if let Some(end) = self.may_act(drive).await {
                return end;
            }
            let p = match self.builds.read_build(&id).await {
                Ok(p) => p,
                Err(e) => return self.stalled(drive, format!("{}: Build {id} could not be read: {e}", self.me)),
            };
            if p.state == BuildState::Complete {
                drive.finish(BuildReply::Complete { build_id: id.0.clone(), attempt: p.attempt });
                return DriveEnd::Terminal;
            }
            if self.accepted.build_id().await.as_ref() != Some(&id) {
                return self.stalled(drive, format!("{}: Build {id} is not the accepted Build (Fabric.build_id): nothing drives it", self.me));
            }
            let take_over = departed.as_deref().is_some_and(|d| p.executor.as_deref() == Some(d));
            let (attempt, executor, context) = if p.state == BuildState::Running && !take_over {
                let holder = p.executor.clone().unwrap_or_default();
                let Some(holder) = parse_path(&holder) else {
                    return self.stalled(drive, format!("{}: attempt {} of Build {id} is claimed by {holder:?}, which is not a path.name", self.me, p.attempt));
                };
                (p.attempt, holder, self.door.contexts.get(&id, p.attempt).ok().flatten().unwrap_or_default())
            } else {
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
            let first = drive.note_first_attempt(attempt);
            drive.replay_receipts(&p.steps, first, attempt);
            *dispatches += 1;
            let dspan = tracing::info_span!("rdm.node_admin.build.update.via-dispatch", build_id = %id, attempt, executor = %executor, node = %self.me, outcome = tracing::field::Empty);
            if let Some(tp) = &context.traceparent {
                rafka_mesh_telemetry::set_remote_parent(&dspan, tp, context.tracestate.as_deref());
            }
            let inner = self.run_inner(drive, &id, attempt, &executor, context).instrument(dspan.clone()).await;
            drive.clear_stall();
            match inner {
                Inner::Terminal(t @ BuildReply::Complete { .. }) => {
                    dspan.record("outcome", "complete");
                    self.verdicts.record(BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await;
                    drive.finish(t);
                    return DriveEnd::Terminal;
                }
                Inner::Terminal(t @ BuildReply::Failed { .. }) => {
                    dspan.record("outcome", "failed");
                    if let BuildReply::Failed { operation, reason, .. } = &t {
                        if !operation.is_empty() && !reason.contains("(reason cut:") {
                            self.verdicts.record(BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::Failed { reason: reason.clone() } }).await;
                        }
                    }
                    drive.finish(t);
                    return DriveEnd::Terminal;
                }
                Inner::Terminal(BuildReply::HandedOff { to, .. }) => {
                    dspan.record("outcome", "handed-off");
                    self.verdicts.record(BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::HandedOff { to: to.clone() } }).await;
                    let Some(next) = parse_path(&to) else {
                        return self.stalled(drive, format!("attempt {attempt} of Build {id} handed off to {to:?}, which is not a path.name"));
                    };
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
                Inner::Unreached(why) | Inner::Broke(why) => {
                    let (class, detail) = ("lost", why);
                    dspan.record("outcome", "executor-lost");
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
                            let why = format!("executor {executor} is {class}; its departure is not proven ({not_proven}); the attempt is not claimed for another admin");
                            self.blocked(drive, attempt, "", "departure-proof", &why);
                            return self.stalled(drive, why);
                        }
                    }
                }
            }
        }
    }

    /// Claim the next attempt of `p` for the admin that runs what is left (or `forced`, the admin a
    /// run just handed off to), on this admin's own log.
    async fn claim_next(&self, drive: &Arc<Drive>, p: &BuildProjection, forced: Option<PathName>) -> Result<(u32, PathName, CallContext), DriveEnd> {
        let id = &drive.build_id;
        let attempt = p.attempt + 1;
        let lead = match forced {
            Some(to) => Some(to),
            None => {
                let t = self.topology.read().await.clone();
                let ops = crate::accepted::plan_for_build(p, &t).operations;
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
            return Err(self.stalled(drive, format!("{}: no admin can execute what is left of Build {id} in this view", self.me)));
        };
        match self.door.claim(&lead.to_string(), id, attempt).await {
            BuildClaimReply::Won { context } => Ok((attempt, lead, context)),
            BuildClaimReply::Lost { holder } => match parse_path(&holder) {
                Some(h) => {
                    let context = self.door.contexts.get(id, attempt).ok().flatten().unwrap_or_default();
                    Ok((attempt, h, context))
                }
                None => Err(self.stalled(drive, format!("attempt {attempt} of Build {id} is held by {holder:?}, which is not a path.name"))),
            },
            BuildClaimReply::NotOpen { next } => Err(self.stalled(drive, format!("{}: attempt {attempt} of Build {id} is not open (the open attempt is {next:?})", self.me))),
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

    async fn run_inner(&self, drive: &Arc<Drive>, id: &BuildId, attempt: u32, executor: &PathName, context: CallContext) -> Inner {
        let intent = self.intent_of(id).await;
        let mut stream = match self.dispatcher.dispatch(executor, id, attempt, context, intent).await {
            Dispatched::Stream(s) => s,
            Dispatched::Refused(r) => return Inner::Refused(r),
            Dispatched::Unreached(why) => return Inner::Unreached(why),
        };
        loop {
            match stream.next().await {
                InnerItem::Frame(f) => match f {
                    BuildReply::Step { .. } | BuildReply::Blocked { .. } => {
                        drive.push(f);
                    }
                    t @ (BuildReply::Complete { .. } | BuildReply::Failed { .. } | BuildReply::HandedOff { .. }) => return Inner::Terminal(t),
                    other => return Inner::Refused(other),
                },
                InnerItem::Ended => return Inner::Broke("the inner stream ended without its terminal".into()),
                InnerItem::Broke(why) => return Inner::Broke(why),
            }
        }
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
