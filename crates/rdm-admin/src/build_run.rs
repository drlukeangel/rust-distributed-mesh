//! The executor's side of `build.attempt.run` (op `0x20`; node-rpc-envelope.md "Build, op `0x20`").
//!
//! The fabric-primary claims an attempt on its own log and calls the admin `executor_for` names
//! with the won claim. This module is what that admin answers with:
//!
//! - **start-or-reattach on `(build_id, attempt)`.** A call for an attempt this admin is running
//!   attaches to that run: it is replayed a `Step` frame per `Complete` receipt the run holds
//!   (read, never run again), then the current `Blocked` if the run is still blocked, then the
//!   run's live frames. A call for the claimed attempt with no run here starts it: the steps
//!   already receipted are reused by the pipelines' own keys, so nothing runs twice.
//! - **refusals by name.** A call whose `executor` is not this admin is `NotExecutor`; a claim that
//!   is not the current folded attempt of this admin's projection is `StaleClaim`. A replaced
//!   mesh primary gets a new claim from the fabric-primary, never the stale one.
//! - **frames after durability.** A `Step` frame is produced only when the step's `Complete`
//!   receipt has been appended on this admin ([`FramedBuilds`] sits on the append). `Blocked` is
//!   kept in memory per attempt, sent on a new blocker or a changed reason and never on a timer,
//!   and is never a step outcome.
//! - **a cut call cancels nothing.** The run is its own task; the stream is a view of its
//!   [`RunLog`]. Only a failed step or a target ends an attempt.
//! - **a run is a turn of the admin's stop gate** (stop_gate.rs). A run served for a call begins
//!   its turn at its own wait point, before it runs anything: while the admin is stopped it waits
//!   there, and a stop's quiesce waits for a run already past it, so nothing a run does follows the
//!   stop's cut. A run the admin's own drive pass dispatches to itself begins its turn under the
//!   pass's ([`crate::stop_gate::Turn::under`]): it never waits at the gate for the pass that waits
//!   on it.

use crate::build::BuildId;
use crate::build_state::{AttemptOutcome, BuildFact, BuildProjection, BuildState, BuildStateAdapter, BuildStateError, BuildStepReceipt, LocalBuildLog, StepOutcome};
use crate::executor::{BuildExecutor, Reconciled, RunPlan};
use crate::model::PathName;
use rdm_core::build::{BuildReply, Disposition};
use rdm_core::context::CallContext;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tracing::instrument::WithSubscriber as _;
use tracing::Instrument as _;

/// The longest step reason a frame carries; a longer one is cut with the cut named.
pub const MAX_REASON_BYTES: usize = 16 * 1024;

/// `reason` within [`MAX_REASON_BYTES`]; the bytes left out are counted in the text.
pub fn bounded_reason(reason: String) -> String {
    if reason.len() <= MAX_REASON_BYTES {
        return reason;
    }
    let mut end = MAX_REASON_BYTES;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}... (reason cut: {} more bytes)", &reason[..end], reason.len() - end)
}

#[derive(Default)]
struct RunState {
    /// Every `Step` and `Blocked` frame, in the order they were produced.
    events: Vec<BuildReply>,
    /// The `Step` frames alone: what a reattaching call is replayed.
    steps: Vec<BuildReply>,
    /// The blocker the run holds now, `(operation, step, reason)`.
    blocked: Option<(String, String, String)>,
    /// The last step to fail, `(operation, step, reason)`: what the terminal `Failed` names.
    failed: Option<(String, String, String)>,
    /// The attempt's terminal frame.
    terminal: Option<BuildReply>,
}

/// Whether the admin a run reports from is still not the fabric-primary ([`RunLog::report`]).
pub type NotFabricPrimary = Arc<dyn Fn() -> bool + Send + Sync>;

/// What one run of one attempt has produced: the frames every call attached to it reads.
pub struct RunLog {
    build_id: String,
    attempt: u32,
    state: Mutex<RunState>,
    wake: Notify,
    /// A run served to a remote fabric-primary (Q118, build.md: the fabric-primary writes every Build
    /// record): its receipts go up the stream in each `Step`, and the fabric-primary writes them; this
    /// admin writes no Build record of it while it is not the fabric-primary itself.
    reporting: std::sync::atomic::AtomicBool,
    /// Whether this admin is still not the fabric-primary: a run whose executor becomes the fabric-primary
    /// during it (its own node.primary.update) writes its own records from then on.
    not_fabric_primary: Mutex<Option<NotFabricPrimary>>,
}

impl RunLog {
    fn new(build_id: &BuildId, attempt: u32) -> Arc<Self> {
        Arc::new(Self {
            build_id: build_id.to_string(),
            attempt,
            state: Mutex::new(RunState::default()),
            wake: Notify::new(),
            reporting: Default::default(),
            not_fabric_primary: Mutex::new(None),
        })
    }

    /// This run reports its receipts up the stream (an attempt served to a remote fabric-primary) while
    /// `not_fabric_primary` holds: while this admin is not the fabric-primary.
    pub fn report(&self, not_fabric_primary: Option<NotFabricPrimary>) {
        *self.not_fabric_primary.lock().unwrap() = not_fabric_primary;
        self.reporting
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether this run reports its receipts up the stream instead of writing them.
    pub fn reporting(&self) -> bool {
        self.reporting.load(std::sync::atomic::Ordering::SeqCst) && self.not_fabric_primary.lock().unwrap().as_ref().is_none_or(|still| still())
    }

    /// A step's `Complete` receipt is durable: its frame. A repeat of a step already framed is
    /// dropped. A step that completes clears the blocker that waited on it.
    pub fn step(&self, operation: &str, step: &str) {
        self.step_with(operation, step, None)
    }

    /// [`Self::step`], the frame carrying the step's `receipt` (a reporting run's, JSON-encoded).
    pub fn step_with(&self, operation: &str, step: &str, receipt: Option<Vec<u8>>) {
        let mut s = self.state.lock().unwrap();
        let held = s.steps.iter().any(|f| matches!(f, BuildReply::Step { operation: o, step: t, .. } if o == operation && t == step));
        if held {
            return;
        }
        let frame = BuildReply::Step {
            build_id: self.build_id.clone(),
            attempt: self.attempt,
            operation: operation.into(),
            step: step.into(),
            receipt,
        };
        s.steps.push(frame.clone());
        s.events.push(frame);
        if s.blocked.as_ref().is_some_and(|(o, t, _)| o == operation && t == step) {
            s.blocked = None;
        }
        drop(s);
        self.wake.notify_waiters();
    }

    /// The run is blocked at `step` of `operation`. A frame goes out only for a new blocker or a
    /// changed reason; the blocker stays in memory for a call that attaches later.
    pub fn blocked(&self, operation: &str, step: &str, reason: &str) {
        let mut s = self.state.lock().unwrap();
        let now = (operation.to_string(), step.to_string(), reason.to_string());
        if s.blocked.as_ref() == Some(&now) {
            return;
        }
        s.blocked = Some(now);
        s.events.push(BuildReply::Blocked { build_id: self.build_id.clone(), attempt: self.attempt, operation: operation.into(), step: step.into(), reason: bounded_reason(reason.into()) });
        drop(s);
        self.wake.notify_waiters();
    }

    /// A step failed: the terminal `Failed` of the attempt names it.
    pub fn failed_step(&self, operation: &str, step: &str, reason: &str) {
        self.state.lock().unwrap().failed = Some((operation.into(), step.into(), reason.into()));
    }

    /// The last step to fail, when one did.
    pub fn last_failed_step(&self) -> Option<(String, String, String)> {
        self.state.lock().unwrap().failed.clone()
    }

    /// The attempt ended: `terminal` follows the last frame.
    pub fn finish(&self, terminal: BuildReply) {
        let mut s = self.state.lock().unwrap();
        if s.terminal.is_none() {
            s.blocked = None;
            s.terminal = Some(terminal);
        }
        drop(s);
        self.wake.notify_waiters();
    }

    /// A call attached to the run after it began: the frames it is replayed (a `Step` per
    /// `Complete` receipt held, then the current `Blocked` if the run is still blocked) and the
    /// live reader after them.
    pub fn attach(self: &Arc<Self>) -> (Vec<BuildReply>, RunReader) {
        let s = self.state.lock().unwrap();
        let mut replay = s.steps.clone();
        if let Some((operation, step, reason)) = &s.blocked {
            replay.push(BuildReply::Blocked { build_id: self.build_id.clone(), attempt: self.attempt, operation: operation.clone(), step: step.clone(), reason: bounded_reason(reason.clone()) });
        }
        (replay, RunReader { log: self.clone(), cursor: s.events.len(), done: false })
    }

    /// The reader of a run from its first frame: the call that started it.
    pub fn reader(self: &Arc<Self>) -> RunReader {
        RunReader { log: self.clone(), cursor: 0, done: false }
    }
}

/// A call's read position in a [`RunLog`].
pub struct RunReader {
    log: Arc<RunLog>,
    cursor: usize,
    done: bool,
}

impl RunReader {
    /// The next frame in order: every `Step` and `Blocked` frame, then the terminal, then `None`.
    pub async fn next(&mut self) -> Option<BuildReply> {
        if self.done {
            return None;
        }
        loop {
            let notified = self.log.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let s = self.log.state.lock().unwrap();
                if let Some(frame) = s.events.get(self.cursor) {
                    self.cursor += 1;
                    return Some(frame.clone());
                }
                if let Some(t) = &s.terminal {
                    self.done = true;
                    return Some(t.clone());
                }
            }
            notified.await;
        }
    }
}

/// The runs this admin holds, by `(build_id, lane, attempt)`: `None` the fabric lane, `Some(mesh)`
/// that mesh's drift lane (R-BI7), whose numbering is its own.
#[derive(Default)]
pub struct AttemptRuns {
    runs: Mutex<HashMap<(String, Option<String>, u32), Arc<RunLog>>>,
    /// Each run's task, and whether this admin executes attempts no longer ([`Self::stop`]): one lock,
    /// so no run's task starts after the stop.
    tasks: Mutex<(Vec<tokio::task::JoinHandle<()>>, bool)>,
}

impl AttemptRuns {
    /// The run of `attempt` of `build_id` in `lane`, when this admin is running it.
    pub fn get(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> Option<Arc<RunLog>> {
        self.runs.lock().unwrap().get(&(build_id.to_string(), lane.map(str::to_string), attempt)).cloned()
    }

    /// The run of `attempt` in `lane`: the one held, or a new one (`true`).
    fn begin(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> (Arc<RunLog>, bool) {
        let mut runs = self.runs.lock().unwrap();
        let key = (build_id.to_string(), lane.map(str::to_string), attempt);
        match runs.get(&key) {
            Some(log) => (log.clone(), false),
            None => {
                let log = RunLog::new(build_id, attempt);
                runs.insert(key, log.clone());
                (log, true)
            }
        }
    }

    fn end(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) {
        self.runs.lock().unwrap().remove(&(build_id.to_string(), lane.map(str::to_string), attempt));
    }

    /// Run `run` as a task of its own, unless this admin executes attempts no longer ([`Self::stop`]):
    /// `false`, and nothing is spawned.
    fn spawn(&self, run: impl std::future::Future<Output = ()> + Send + 'static) -> bool {
        let mut tasks = self.tasks.lock().unwrap();
        if tasks.1 {
            return false;
        }
        tasks.0.retain(|t| !t.is_finished());
        tasks.0.push(tokio::spawn(run));
        true
    }

    /// This admin executes attempts no longer (`Running::stop_reconciling`): every run it holds ends
    /// where it is, a run a drive dispatched before the stop included, and none starts after this. It
    /// returns once each run it ended has ended (an abort lands at the run's next await, so a run in
    /// the middle of a poll runs that poll to its end first). Returns how many runs were ended.
    pub async fn stop(&self) -> usize {
        let ending: Vec<tokio::task::JoinHandle<()>> = {
            let mut tasks = self.tasks.lock().unwrap();
            tasks.1 = true;
            tasks.0.drain(..).filter(|t| !t.is_finished()).collect()
        };
        let ended = ending.len();
        for t in &ending {
            t.abort();
        }
        for t in ending {
            let _ = t.await;
        }
        tracing::info_span!("rdm.node_admin.build.update.via-runs-stopped", runs = ended as u64)
            .in_scope(|| tracing::info!("this admin executes no attempt from now on: every run's task is ended and none starts"));
        ended
    }
}

/// This admin's Build state, with a frame produced whenever a step's `Complete` receipt has been
/// appended (and a reporting run's `Failed` one, which it writes nowhere else). Every pipeline writes its receipts through the one `builds` handle, so this is the
/// single place a step frame is made, and it is made after the append returned: the frame never
/// precedes the durable receipt on the admin that wrote it.
pub struct FramedBuilds {
    inner: Arc<dyn BuildStateAdapter>,
    runs: Arc<AttemptRuns>,
    /// The receipts of reporting runs (Q118): never written here, read back by the run that made them.
    reported: Mutex<Vec<BuildStepReceipt>>,
}

impl FramedBuilds {
    /// `inner` with frames into `runs`.
    pub fn new(inner: Arc<dyn BuildStateAdapter>, runs: Arc<AttemptRuns>) -> Self {
        Self {
            inner,
            runs,
            reported: Mutex::default(),
        }
    }

    /// Whether the run of `(build_id, lane, attempt)` reports its records up the stream.
    fn reporting(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> bool {
        self.runs
            .get(build_id, lane, attempt)
            .is_some_and(|r| r.reporting())
    }
}

#[async_trait::async_trait]
impl BuildStateAdapter for FramedBuilds {
    async fn publish_accepted(&self, accepted: &crate::build_state::BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await
    }
    async fn open_attempt(&self, opened: &crate::build_state::AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(opened).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        // A reporting run reads the receipts it reported as it reads its own (a later step reuses an
        // earlier step's decision) until the fabric-primary's are read back.
        let mut p = self.inner.read_build(build_id).await?;
        for r in self
            .reported
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.build_id == *build_id)
        {
            // A drift lane's receipts are its lane's own steps.
            let steps = match r.mesh.as_deref().and_then(|m| p.lanes.get_mut(m)) {
                Some(lane) => &mut lane.steps,
                None => &mut p.steps,
            };
            if !steps.iter().any(|s| {
                s.attempt == r.attempt
                    && s.mesh == r.mesh
                    && s.operation == r.operation
                    && s.step == r.step
            }) {
                steps.push(r.clone());
            }
        }
        Ok(p)
    }
    async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, crate::build_state::BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        self.inner.accepted_newest_first().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, claim: &crate::build_state::BuildAttemptClaim) -> Result<crate::build_state::ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(claim).await
    }
    async fn claim_in_flight(&self, claim: &crate::build_state::BuildAttemptClaim, claims: &dyn crate::storage::MeshStorage, mesh: &str, row: &crate::mesh_claims::InFlight) -> Result<crate::build_state::InFlightClaim, BuildStateError> {
        self.inner.claim_in_flight(claim, claims, mesh, row).await
    }
    async fn in_flight_live(&self, held: &crate::mesh_claims::InFlight) -> Result<bool, BuildStateError> {
        self.inner.in_flight_live(held).await
    }
    async fn adopt_claim(
        &self,
        claim: &crate::build_state::BuildAttemptClaim,
    ) -> Result<(), BuildStateError> {
        // The fabric-primary holds a reporting run's claim (it decided it).
        if self.reporting(&claim.build_id, claim.mesh.as_deref(), claim.attempt) {
            return Ok(());
        }
        self.inner.adopt_claim(claim).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        let run = self
            .runs
            .get(&receipt.build_id, receipt.mesh.as_deref(), receipt.attempt);
        // A reporting run's receipt goes up its stream in its `Step`, and the fabric-primary writes it.
        let reporting = run.as_ref().is_some_and(|r| r.reporting());
        if reporting {
            self.reported.lock().unwrap().push(receipt.clone());
        } else {
            self.inner.append_step_receipt(receipt).await?;
        }
        if let Some(run) = run {
            match &receipt.outcome {
                StepOutcome::Complete => run.step_with(
                    &receipt.operation,
                    &receipt.step,
                    reporting.then(|| serde_json::to_vec(receipt).unwrap_or_default()),
                ),
                StepOutcome::Failed { reason, .. } => {
                    // A reporting run writes no record of its own: its failed step's receipt goes up
                    // the stream too, the step's result (node-rpc-envelope.md, Step), so the
                    // fabric-primary's record holds it when the attempt is handed back rather than
                    // failed (a create's heal, Q245, Q252).
                    if reporting {
                        run.step_with(&receipt.operation, &receipt.step, Some(serde_json::to_vec(receipt).unwrap_or_default()));
                    }
                    run.failed_step(&receipt.operation, &receipt.step, reason)
                }
            }
        }
        Ok(())
    }
    async fn append_attempt_receipt(
        &self,
        receipt: &crate::build_state::BuildAttemptReceipt,
    ) -> Result<(), BuildStateError> {
        // A reporting run's verdict is its terminal frame; the fabric-primary writes it.
        if self.reporting(&receipt.build_id, receipt.mesh.as_deref(), receipt.attempt) {
            return Ok(());
        }
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.committed_facts(build).await
    }
    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(crate::build::BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(crate::build::BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }
    fn held_name(&self) -> Option<crate::model::PathName> {
        self.inner.held_name()
    }
    async fn note_blocked(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, operation: &str, step: &str, reason: &str) {
        if let Some(run) = self.runs.get(build_id, lane, attempt) {
            run.blocked(operation, step, reason);
        }
    }
}

/// The Build state as one mesh's drift lane writes and reads it (R-BI7 caveat 2: attempts are
/// scoped per mesh; the claim key is `(build_id, mesh, attempt)`): every open, claim and receipt
/// written through it carries the lane (`mesh: Some(mesh)`), and a Build read through it is read in
/// the lane ([`BuildProjection::in_lane`]). The executor and its pipelines write and read an
/// attempt's facts through one handle; a drift attempt's run hands them this one, so they keep the
/// lane's numbering and receipts and never touch the fabric lane's. Every other read is the Build's.
pub struct LaneBuilds {
    inner: Arc<dyn BuildStateAdapter>,
    lane: String,
}

impl LaneBuilds {
    /// `inner` as `mesh`'s drift lane.
    pub fn new(inner: Arc<dyn BuildStateAdapter>, mesh: &str) -> Self {
        Self { inner, lane: mesh.to_string() }
    }

    /// `builds` for an attempt of `lane`: itself for the fabric lane, else as that mesh's drift lane.
    pub fn of(builds: &Arc<dyn BuildStateAdapter>, lane: Option<&str>) -> Arc<dyn BuildStateAdapter> {
        match lane {
            None => builds.clone(),
            Some(mesh) => Arc::new(Self::new(builds.clone(), mesh)),
        }
    }

    fn lane(&self) -> Option<String> {
        Some(self.lane.clone())
    }
}

#[async_trait::async_trait]
impl BuildStateAdapter for LaneBuilds {
    async fn publish_accepted(&self, accepted: &crate::build_state::BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await
    }
    async fn open_attempt(&self, opened: &crate::build_state::AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(&crate::build_state::AttemptOpened { mesh: self.lane(), ..opened.clone() }).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        Ok(self.inner.read_build(build_id).await?.in_lane(Some(&self.lane)))
    }
    async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        self.inner.accepted_newest_first().await
    }
    async fn current(&self) -> Result<Option<BuildProjection>, BuildStateError> {
        self.inner.current().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, claim: &crate::build_state::BuildAttemptClaim) -> Result<crate::build_state::ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(&crate::build_state::BuildAttemptClaim { mesh: self.lane(), ..claim.clone() }).await
    }
    async fn claim_in_flight(&self, claim: &crate::build_state::BuildAttemptClaim, claims: &dyn crate::storage::MeshStorage, mesh: &str, row: &crate::mesh_claims::InFlight) -> Result<crate::build_state::InFlightClaim, BuildStateError> {
        let lane = self.lane();
        self.inner.claim_in_flight(&crate::build_state::BuildAttemptClaim { mesh: lane.clone(), ..claim.clone() }, claims, mesh, &crate::mesh_claims::InFlight { lane, ..row.clone() }).await
    }
    async fn in_flight_live(&self, held: &crate::mesh_claims::InFlight) -> Result<bool, BuildStateError> {
        self.inner.in_flight_live(held).await
    }
    /// A drift attempt's claimant is its executor, the mesh's own primary (R-BI7 caveat 2): an adopted
    /// claim that names none names it.
    async fn adopt_claim(&self, claim: &crate::build_state::BuildAttemptClaim) -> Result<(), BuildStateError> {
        let claimant = claim.claimant.clone().or_else(|| Some(claim.executor.clone()));
        self.inner.adopt_claim(&crate::build_state::BuildAttemptClaim { mesh: self.lane(), claimant, ..claim.clone() }).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.inner.append_step_receipt(&BuildStepReceipt { mesh: self.lane(), ..receipt.clone() }).await
    }
    async fn append_attempt_receipt(&self, receipt: &crate::build_state::BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(&crate::build_state::BuildAttemptReceipt { mesh: self.lane(), ..receipt.clone() }).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.committed_facts(build).await
    }
    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(crate::build::BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(crate::build::BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }
    fn held_name(&self) -> Option<crate::model::PathName> {
        self.inner.held_name()
    }
    async fn note_blocked(&self, build_id: &BuildId, _lane: Option<&str>, attempt: u32, operation: &str, step: &str, reason: &str) {
        self.inner.note_blocked(build_id, Some(&self.lane), attempt, operation, step, reason).await
    }
}

/// A run of an attempt this admin holds itself while it acts as a step of the Build outside the
/// executor (the primary's own node.primary.update, run from its drive: the approved proposal
/// node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 1, "as a step of the Build"; build.md
/// "A live run is reattached, never re-claimed"). A call for the attempt while it is held reattaches to
/// it and reads its terminal once [`HeldRun::finish`] says it.
pub struct HeldRun {
    build_id: BuildId,
    attempt: u32,
    log: Arc<RunLog>,
    runs: Arc<AttemptRuns>,
    builds: Arc<dyn BuildStateAdapter>,
    finished: bool,
}

impl HeldRun {
    /// The attempt ended with `terminal`: its verdict receipt first, on this admin's own Build state as
    /// an executor's run writes it, so a call that reads the attempt after the run ends finds the
    /// verdict; then `at_verdict`, what the verdict's writer does at the verdict (the release of the
    /// mesh's in-flight row the attempt's claim took, R-BI7 caveat 3), before any reader learns the
    /// attempt ended; then the terminal frame to every reader; then the run is no longer held. A
    /// verdict that cannot be recorded ends the attempt `Failed` naming why, with no verdict and no
    /// `at_verdict` (the mesh stays held by the attempt), as an executor's run does.
    pub async fn finish(mut self, terminal: BuildReply, at_verdict: impl std::future::Future<Output = ()>) {
        let outcome = match &terminal {
            BuildReply::HandedOff { to, .. } => Some(AttemptOutcome::HandedOff { to: to.clone() }),
            BuildReply::Complete { .. } => Some(AttemptOutcome::Converged),
            BuildReply::Failed { reason, code, .. } => Some(AttemptOutcome::Failed { reason: reason.clone(), code: *code }),
            _ => None,
        };
        let terminal = match outcome {
            Some(outcome) => match self.builds.append_attempt_receipt(&crate::build_state::BuildAttemptReceipt { build_id: self.build_id.clone(), attempt: self.attempt, outcome, mesh: None }).await {
                Ok(()) => {
                    at_verdict.await;
                    terminal
                }
                Err(e) => rdm_core::reply::build::failed(self.build_id.to_string(), self.attempt, String::new(), "attempt".into(), bounded_reason(format!("recording attempt {}: {e}", self.attempt)), rdm_core::outcome::Code::FailedPrecondition),
            },
            None => terminal,
        };
        self.log.finish(terminal);
        self.runs.end(&self.build_id, None, self.attempt);
        self.finished = true;
    }
}

impl Drop for HeldRun {
    /// A held run dropped unfinished (its task ended or was aborted): every reader reads a named
    /// `NotReady`, never a stream that waits forever, and the attempt has no verdict, so a later call
    /// starts it here.
    fn drop(&mut self) {
        if !self.finished {
            self.log.finish(rdm_core::reply::build::not_ready(format!("attempt {} of Build {} was held by its run on this admin, which ended before it said the attempt's terminal", self.attempt, self.build_id)));
            self.runs.end(&self.build_id, None, self.attempt);
        }
    }
}

/// What `build.attempt.run` came to on this admin.
pub enum Attached {
    /// The attempt is running here: `replay` first, then the reader.
    Live {
        /// How the call came to the run.
        disposition: Disposition,
        /// The frames the call is replayed before the live ones.
        replay: Vec<BuildReply>,
        /// The live frames.
        reader: RunReader,
    },
    /// The attempt already has its verdict: its receipts, then the terminal.
    Ended {
        /// A `Step` frame per `Complete` receipt of the attempt.
        replay: Vec<BuildReply>,
        /// The attempt's terminal frame.
        terminal: BuildReply,
    },
    /// The call is refused, by name.
    Refused(BuildReply),
}

/// The door an executor answers `build.attempt.run` through.
pub struct RunDoor {
    /// This admin's `path.name`.
    pub me: PathName,
    /// This admin's Build state, as the pipelines write it (through [`FramedBuilds`]).
    pub builds: Arc<dyn BuildStateAdapter>,
    /// The executor that runs a claimed attempt.
    pub exec: Arc<BuildExecutor>,
    /// The runs this admin holds.
    pub runs: Arc<AttemptRuns>,
    /// This admin's own Build log, where the intent a call carries is absorbed before the attempt
    /// is planned.
    pub local: Arc<dyn LocalBuildLog>,
    /// This admin's stop gate (stop_gate.rs): a stopped admin's tasks are held at it.
    pub gate: crate::stop_gate::StopGate,
}

impl RunDoor {
    /// The path name this admin holds now (`builds.held_name()` when joined to the fabric Build
    /// topic, else the name its executor resolves from the topology view, or `self.me`).
    pub async fn held_name(&self) -> PathName {
        if let Some(held) = self.builds.held_name() {
            return held;
        }
        self.exec.held_name().await.parse().unwrap_or_else(|_| self.me.clone())
    }

    /// Answer `build.attempt.run` for `attempt` of `build_id`, claimed for `executor`. `intent` is
    /// the Build's intent the fabric-primary carried (empty for a run it makes itself) and `plan`
    /// the run it planned: the operations this admin runs.
    pub async fn attempt_run(&self, build_id: &BuildId, attempt: u32, executor: &str, context: &CallContext, intent: &[Vec<u8>], plan: &RunPlan) -> Attached {
        self.attempt_run_in(None, build_id, attempt, executor, context, intent, plan).await
    }

    /// [`Self::attempt_run`] for `attempt` of `lane`: `None` the fabric lane (the fabric-primary's
    /// call on `0x20`), `Some(mesh)` that mesh's drift lane, which this admin runs as the mesh's own
    /// primary (R-BI7): the attempt is read, run and receipted in the lane, numbered in the lane.
    #[allow(clippy::too_many_arguments)]
    pub async fn attempt_run_in(
        &self,
        lane: Option<&str>,
        build_id: &BuildId,
        attempt: u32,
        executor: &str,
        context: &CallContext,
        intent: &[Vec<u8>],
        plan: &RunPlan,
    ) -> Attached {
        self.serve(
            None, lane, None, build_id, attempt, executor, context, intent, plan,
        )
        .await
    }

    /// [`Self::attempt_run_in`] for an attempt served to a remote fabric-primary, a topology attempt
    /// (`lane` `None`) or a mesh's drift attempt (Q118; build.md: the fabric-primary writes every Build
    /// record): the run reports its receipts up the stream and writes no Build record of its own while
    /// `not_fabric_primary` holds.
    #[allow(clippy::too_many_arguments)]
    pub async fn attempt_run_reporting(
        &self,
        lane: Option<&str>,
        build_id: &BuildId,
        attempt: u32,
        executor: &str,
        context: &CallContext,
        intent: &[Vec<u8>],
        plan: &RunPlan,
        not_fabric_primary: NotFabricPrimary,
    ) -> Attached {
        self.serve(
            None,
            lane,
            Some(not_fabric_primary),
            build_id,
            attempt,
            executor,
            context,
            intent,
            plan,
        )
        .await
    }

    /// [`Self::attempt_run`] for the fabric-primary's own drive pass dispatching the attempt to this
    /// admin, in the pass's `turn`: the run begins its own turn under it, at once, stopped gate or not
    /// (the pass waits on the run; a run waiting at the gate for it would hold the stop's quiesce).
    pub async fn attempt_run_under(
        &self,
        turn: &crate::stop_gate::Turn,
        build_id: &BuildId,
        attempt: u32,
        executor: &str,
        context: &CallContext,
        intent: &[Vec<u8>],
        plan: &RunPlan,
    ) -> Attached {
        self.serve(
            Some(turn),
            None,
            None,
            build_id,
            attempt,
            executor,
            context,
            intent,
            plan,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn serve(
        &self,
        under: Option<&crate::stop_gate::Turn>,
        lane: Option<&str>,
        reporting: Option<NotFabricPrimary>,
        build_id: &BuildId,
        attempt: u32,
        executor: &str,
        context: &CallContext,
        intent: &[Vec<u8>],
        plan: &RunPlan,
    ) -> Attached {
        let me = self.held_name().await;
        let span = tracing::info_span!(
            "rdm.node_admin.build.serve.via-attempt-run",
            node = %me,
            build_id = %build_id.log(),
            lane = lane.unwrap_or("fabric"),
            attempt,
            executor,
            outcome = tracing::field::Empty,
        );
        let attached = self
            .attach_or_start(
                under, lane, reporting, build_id, attempt, executor, context, intent, plan,
            )
            .instrument(span.clone())
            .await;
        span.record(
            "outcome",
            match &attached {
                Attached::Live { disposition: Disposition::Reattached, .. } => "reattached",
                Attached::Live { .. } => "started",
                Attached::Ended { .. } => "ended",
                Attached::Refused(r) => r.name(),
            },
        );
        span.in_scope(|| match &attached {
            Attached::Refused(r) => tracing::info!(reply = r.name(), "build.attempt.run refused"),
            _ => tracing::info!("build.attempt.run accepted"),
        });
        attached
    }

    /// Hold the run of `attempt` of `build_id` on this admin while the Build's own step runs outside
    /// the executor ([`HeldRun`]): a call for the attempt reattaches to it. The holder's role update
    /// is a topology attempt, so the run is the fabric lane's (`None`). `None` when a run of the
    /// attempt is already held here.
    pub fn hold(&self, build_id: &BuildId, attempt: u32) -> Option<HeldRun> {
        let (log, created) = self.runs.begin(build_id, None, attempt);
        created.then(|| HeldRun { build_id: build_id.clone(), attempt, log, runs: self.runs.clone(), builds: self.builds.clone(), finished: false })
    }

    #[allow(clippy::too_many_arguments)]
    async fn attach_or_start(
        &self,
        under: Option<&crate::stop_gate::Turn>,
        lane: Option<&str>,
        reporting: Option<NotFabricPrimary>,
        build_id: &BuildId,
        attempt: u32,
        executor: &str,
        context: &CallContext,
        intent: &[Vec<u8>],
        plan: &RunPlan,
    ) -> Attached {
        let me = self.held_name().await;
        if executor != me.to_string() {
            tracing::info_span!("rdm.node_admin.build.reject.via-not-executor", node = %me, build_id = %build_id.log(), attempt, named = executor)
                .in_scope(|| tracing::info!("the claim names another admin as the executor"));
            return Attached::Refused(rdm_core::reply::build::not_executor(executor.to_string(), me.to_string()));
        }
        if let Some(log) = self.runs.get(build_id, lane, attempt) {
            let (replay, reader) = log.attach();
            return Attached::Live { disposition: Disposition::Reattached, replay, reader };
        }
        // The intent the fabric-primary decided the claim on is absorbed before anything is planned:
        // insert-and-fail on the facts' own keys, so what the store root holds too changes nothing.
        let mut facts = Vec::new();
        for (i, frame) in intent.iter().enumerate() {
            match crate::fabric_builds::BuildMessage::from_bytes(frame) {
                Ok(m) => facts.extend(m.facts),
                Err(e) => return Attached::Refused(rdm_core::reply::build::rejected("intent-undecodable".into(), format!("{me}: intent frame {i} of Build {build_id} does not decode: {e}"))),
            }
        }
        self.local.absorb_facts(&facts).await;
        // The attempt is read in its lane: a drift attempt's number, executor and state are its
        // mesh's lane's, never the fabric lane's.
        let p = match self.builds.read_build(build_id).await {
            Ok(p) => p.in_lane(lane),
            Err(BuildStateError::UnknownBuild(_)) => return Attached::Refused(rdm_core::reply::build::unknown_build(build_id.to_string())),
            Err(e) => return Attached::Refused(rdm_core::reply::build::not_ready(format!("{me}: Build {build_id} could not be read: {e}"))),
        };
        let held = p.attempt == attempt && p.executor.as_deref() == Some(executor);
        let next = p.attempt + 1 == attempt && p.state != BuildState::Complete;
        if held && p.state != BuildState::Running {
            return self.ended(&me, build_id, lane, attempt, &p).await;
        }
        if !(held || next) {
            tracing::info_span!("rdm.node_admin.build.reject.via-stale-claim", node = %me, build_id = %build_id.log(), attempt, held_attempt = p.attempt, held_executor = p.executor.as_deref().unwrap_or(""))
                .in_scope(|| tracing::info!("the claim is not the current folded attempt of this admin's projection"));
            return Attached::Refused(rdm_core::reply::build::stale_claim(p.attempt, p.executor.clone(), attempt));
        }
        let (log, created) = self.runs.begin(build_id, lane, attempt);
        if !created {
            let (replay, reader) = log.attach();
            return Attached::Live { disposition: Disposition::Reattached, replay, reader };
        }
        if let Some(not_fabric_primary) = reporting {
            log.report(Some(not_fabric_primary));
        }
        let reader = log.reader();
        // The run's turn of the admin's gate: under the drive pass that dispatched it here, begun now;
        // for a served call, begun by the run itself at its wait point, before it runs anything.
        let gated = tracing::info_span!("rdm.node_admin.build.serve.via-run-turn", node = %me, build_id = %build_id.log(), lane = lane.unwrap_or("fabric"), attempt, turn = tracing::field::Empty);
        let held = under.map(|t| t.under("build-run"));
        let gate = self.gate.clone();
        let of_run = (build_id.clone(), lane.map(str::to_string));
        let (exec, runs, build_id, context, plan, lane) = (self.exec.clone(), self.runs.clone(), build_id.clone(), context.clone(), plan.clone(), lane.map(str::to_string));
        let run = log.clone();
        let spawned = self.runs.spawn(
            {
                let runs = runs.clone();
                async move {
                    let _turn = match held {
                        Some(t) => {
                            gated.record("turn", "under-drive-pass");
                            t
                        }
                        None => {
                            let waited = gate.is_stopped();
                            let t = gate.turn("build-run").instrument(gated.clone()).await;
                            gated.record("turn", if waited { "waited-for-start" } else { "begun" });
                            t
                        }
                    };
                    drop(gated);
                    // Of its Build: a stop of this admin that this run commands and waits on (its own
                    // node.delete) does not wait for this turn (stop_gate.rs).
                    let _turn = _turn.of_build(&build_id.to_string());
                    let r = exec.run_attempt(&build_id, lane.as_deref(), attempt, &context, &plan).await;
                    run.finish(terminal_of(&build_id, &r, run.last_failed_step()));
                    runs.end(&build_id, lane.as_deref(), attempt);
                }
            }
            .with_current_subscriber(),
        );
        if !spawned {
            runs.end(&of_run.0, of_run.1.as_deref(), attempt);
            return Attached::Refused(rdm_core::reply::build::not_ready(format!("{me}: this admin stopped reconciling: it executes no attempt of Build {}", of_run.0)));
        }
        Attached::Live { disposition: Disposition::Started, replay: Vec::new(), reader }
    }

    /// The attempt of `lane` has its verdict and no run here: its `Complete` receipts, then its
    /// terminal, read from the facts of that lane alone.
    async fn ended(&self, me: &PathName, build_id: &BuildId, lane: Option<&str>, attempt: u32, p: &BuildProjection) -> Attached {
        let facts = match self.builds.facts().await {
            Ok(f) => f,
            Err(e) => return Attached::Refused(rdm_core::reply::build::not_ready(format!("{me}: the facts of Build {build_id} could not be read: {e}"))),
        };
        let mut replay = Vec::new();
        let mut failed = None;
        let mut verdict = None;
        for f in facts.iter().filter(|f| f.build_id() == build_id) {
            match f {
                BuildFact::Step(s) if s.attempt == attempt && s.mesh.as_deref() == lane => match &s.outcome {
                    StepOutcome::Complete => {
                        let frame = BuildReply::Step { build_id: build_id.to_string(), attempt, operation: s.operation.clone(), step: s.step.clone(), receipt: None };
                        if !replay.contains(&frame) {
                            replay.push(frame);
                        }
                    }
                    StepOutcome::Failed { reason, .. } => failed = Some((s.operation.clone(), s.step.clone(), reason.clone())),
                },
                BuildFact::Attempt(a) if a.attempt == attempt && a.mesh.as_deref() == lane && verdict.is_none() => verdict = Some(a.outcome.clone()),
                _ => {}
            }
        }
        let terminal = match verdict {
            Some(AttemptOutcome::Converged) => BuildReply::Complete { build_id: build_id.to_string(), attempt },
            Some(AttemptOutcome::HandedOff { to }) => BuildReply::HandedOff { build_id: build_id.to_string(), attempt, to },
            Some(AttemptOutcome::Failed { reason, code }) => {
                let (operation, step, _) = failed.unwrap_or_else(|| (String::new(), "attempt".into(), String::new()));
                rdm_core::reply::build::failed(build_id.to_string(), attempt, operation, step, bounded_reason(reason), code)
            }
            None => {
                return Attached::Refused(rdm_core::reply::build::not_ready(format!("{me}: attempt {attempt} of Build {build_id} is {:?} with no verdict receipt and no run here", p.state)));
            }
        };
        Attached::Ended { replay, terminal }
    }
}

/// What a mesh-primary's own recovery of its member came to (R-BI7; node-recovery.md as R-BI7 moves
/// it: "the mesh-primary opens the recovery attempt of the current Build itself: claim on its mesh's
/// rows, then run; no call to the fabric-primary").
#[derive(Debug)]
pub enum Recovered {
    /// The recovery was opened and claimed at this admin's door and ran here to `terminal`
    /// (`Complete`, or `Failed` naming the step).
    Ran {
        /// The attempt the recovery is.
        opened: crate::control_plane::Opened,
        /// The attempt's terminal frame.
        terminal: BuildReply,
    },
    /// The exited birth was recovered already, by this attempt: nothing was opened or run.
    AlreadyApplied(crate::control_plane::Opened),
}

/// Why a mesh-primary's own recovery did not run to a terminal.
#[derive(Debug)]
pub enum RecoveryNotRun {
    /// This admin's door refused the open, by name.
    Refused(crate::control_plane::Refusal),
    /// The attempt was opened and claimed, and its run was refused, by name.
    RunRefused {
        /// The attempt.
        opened: crate::control_plane::Opened,
        /// The refusal.
        reply: BuildReply,
    },
    /// The attempt's run ended without its terminal.
    Broke {
        /// The attempt.
        opened: crate::control_plane::Opened,
        /// Why.
        why: String,
    },
}

/// The mesh-primary's own recovery of the exited birth `(node_id, incarnation)` at `path` (R-BI7;
/// node-recovery.md as R-BI7 moves it: "the mesh-primary opens the recovery attempt of the current
/// Build itself: claim on its mesh's rows, then run; no call to the fabric-primary"): opened and
/// claimed at `control` (this admin's door) on the mesh's rows, planned from the lane's receipts
/// (only what is left of node.delete, then node.create), run through `door` (this admin's
/// executor) in the mesh's lane and followed to its terminal. Its verdict is the executor's receipt,
/// on the fabric's Build journal on the store root, where the fabric-primary reads it; at the verdict
/// this admin, which took the mesh's in-flight row for the attempt, releases it (R-BI7 caveat 3:
/// "one attempt in flight per mesh"). An attempt with no verdict (its run refused or broken) keeps
/// the mesh held, named in the error.
pub async fn run_recovery(
    control: &crate::control_plane::ControlPlane,
    door: &RunDoor,
    route: &'static str,
    path: PathName,
    node_id: crate::model::NodeId,
    incarnation: crate::model::IncarnationId,
) -> Result<Recovered, RecoveryNotRun> {
    match control
        .open_recovery(route, path, node_id, incarnation)
        .await
    {
        Ok(
            crate::control_plane::Recovery::Opened(o)
            | crate::control_plane::Recovery::Reported { opened: o, .. },
        ) => run_opened_recovery(control, door, o).await,
        Ok(crate::control_plane::Recovery::AlreadyApplied(o)) => Ok(Recovered::AlreadyApplied(o)),
        Err(r) => Err(RecoveryNotRun::Refused(r)),
    }
}

/// The run of the recovery attempt `opened`, which `control` opened and claimed in its mesh's lane, or
/// in the fabric lane for a hung mesh-primary ([`run_recovery`] after the open): planned from the
/// lane's receipts, run through `door`, followed to its terminal, and the mesh's in-flight row
/// released at the verdict.
pub async fn run_opened_recovery(control: &crate::control_plane::ControlPlane, door: &RunDoor, opened: crate::control_plane::Opened) -> Result<Recovered, RecoveryNotRun> {
    let me = door.held_name().await;
    let lane = opened.mesh.as_deref();
    let p = match control.builds.read_build(&opened.build_id).await {
        Ok(p) => p.in_lane(lane),
        Err(e) => return Err(RecoveryNotRun::Broke { why: format!("{me}: Build {} could not be read to plan attempt {} of {}'s lane: {e}; the attempt holds its mesh", opened.build_id, opened.attempt, lane.unwrap_or("the fabric")), opened }),
    };
    // A drift attempt acts in its lane's mesh. A fabric-lane recovery is an unheard mesh-primary's, which
    // the fabric-primary opens (node-recovery.md "Execution"; its mesh's elected primary runs it): it acts
    // in the mesh of the birth its open names.
    let mesh = match (&opened.mesh, p.attempt_actions.get(&opened.attempt)) {
        (Some(mesh), _) => mesh.clone(),
        (None, Some(crate::accepted::AttemptAction::Recover { path, .. })) => path.mesh.clone(),
        (None, _) => return Err(RecoveryNotRun::Broke { why: format!("{me}: attempt {} of {} is a fabric-lane attempt that names no recovered birth: a recovery runs in its mesh's drift lane, or in the fabric lane for a hung mesh-primary", opened.attempt, opened.build_id), opened }),
    };
    let observed = control.topology.read().await.clone();
    let plan = RunPlan { operations: crate::accepted::plan_for_build(&p, &observed).operations, hand_off_to: None };
    // An unheard mesh-primary's recovery the fabric-primary opened and claimed for its mesh's elected
    // primary (`ControlPlane::open_recovery`, node-recovery.md "Execution") is run by that primary, on
    // the fabric-primary's build.attempt.run: nothing of it runs here, and its terminal is that run's.
    if let Some(executor) = p.attempt_executors.get(&opened.attempt).filter(|e| **e != me.to_string()) {
        let terminal = BuildReply::HandedOff { build_id: opened.build_id.to_string(), attempt: opened.attempt, to: executor.clone() };
        return Ok(Recovered::Ran { opened, terminal });
    }
    let context = crate::build_claim::current_context();
    let terminal = match door.attempt_run_in(lane, &opened.build_id, opened.attempt, &me.to_string(), &context, &[], &plan).await {
        Attached::Refused(reply) => return Err(RecoveryNotRun::RunRefused { opened, reply }),
        Attached::Ended { terminal, .. } => terminal,
        Attached::Live { mut reader, .. } => loop {
            match reader.next().await {
                Some(t @ (BuildReply::Complete { .. } | BuildReply::Failed { .. } | BuildReply::HandedOff { .. })) => break t,
                Some(_) => continue,
                None => return Err(RecoveryNotRun::Broke { why: format!("{me}: the run of attempt {} of {} in mesh {mesh}'s lane ended without its terminal; the attempt holds mesh {mesh}", opened.attempt, opened.build_id), opened }),
            }
        },
    };
    let row = crate::mesh_claims::InFlight { build_id: opened.build_id.clone(), lane: opened.mesh.clone(), attempt: opened.attempt, claimant: me.to_string(), claimant_node: None };
    if let Err(e) = control.claims.release_in_flight(&mesh, &row).await {
        tracing::info_span!("rdm.node_admin.build.reject.via-in-flight-unreleased", build_id = %opened.build_id.log(), lane = %mesh, attempt = opened.attempt, mesh = %mesh, node = %me, error = %e)
            .in_scope(|| tracing::info!("the attempt ended and its mesh's in-flight row could not be released: the mesh stays held by it"));
    }
    // A mesh's drift attempt ended on this admin: the reconcile loop runs its next pass on it
    // (`Woken::Submitted`), so a drive refused for this mesh's in-flight row (R-BI7) claims again
    // now, never on whatever event happens next (the returned birth's termination, held behind this
    // recovery, waited forever in the soak at 9244b8cf8).
    control.build_submitted.notify_waiters();
    Ok(Recovered::Ran { opened, terminal })
}

/// The terminal frame an attempt's end is: `Complete`, `Failed` naming the step, or `HandedOff`.
pub fn terminal_of(build_id: &BuildId, r: &Reconciled, failed: Option<(String, String, String)>) -> BuildReply {
    match r {
        Reconciled::Converged { attempt, .. } => BuildReply::Complete { build_id: build_id.to_string(), attempt: *attempt },
        Reconciled::HandedOff { attempt, to, .. } => BuildReply::HandedOff { build_id: build_id.to_string(), attempt: *attempt, to: to.clone() },
        Reconciled::Failed { attempt, operation, reason, code } => {
            // The step a receipt names is the one that failed when its operation is the attempt's.
            // The reason is the attempt's own, word for word the verdict receipt's.
            let step = match failed {
                Some((op, step, _)) if operation.is_empty() || op == *operation => step,
                _ => "attempt".to_string(),
            };
            rdm_core::reply::build::failed(build_id.to_string(), *attempt, operation.clone(), step, bounded_reason(reason.clone()), *code)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> BuildId {
        BuildId::parse("vhpqxdbt5w2g").unwrap()
    }

    fn names(frames: &[BuildReply]) -> Vec<&'static str> {
        frames.iter().map(BuildReply::name).collect()
    }

    /// CONTRACT: a call that attaches to a run is replayed a frame per step already framed and the
    /// blocker the run holds now, in that order, and then reads the run's live frames; a repeat of
    /// a framed step makes no second frame, and a blocker is framed only when it is new or its
    /// reason changed. A step that completes clears the blocker that waited on it.
    #[tokio::test]
    async fn a_reattached_call_is_replayed_the_framed_steps_then_the_current_blocker_then_live() {
        let log = RunLog::new(&id(), 3);
        log.step("node.create:mesh2.rpc.1", "AllocateIdentity");
        log.step("node.create:mesh2.rpc.1", "AllocateIdentity");
        log.blocked("node.create:mesh2.rpc.1", "WaitForNodeReady", "authority not ready");
        log.blocked("node.create:mesh2.rpc.1", "WaitForNodeReady", "authority not ready");
        log.blocked("node.create:mesh2.rpc.1", "WaitForNodeReady", "pulling the accepting authority");
        let mut first = log.reader();
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(first.next().await.unwrap());
        }
        assert_eq!(names(&seen), vec!["step", "blocked", "blocked"], "a repeated step and a repeated blocker make no frame");

        let (replay, mut reader) = log.attach();
        assert_eq!(names(&replay), vec!["step", "blocked"], "a Step per Complete receipt, then only the current Blocked");
        assert!(matches!(&replay[1], BuildReply::Blocked { reason, .. } if reason == "pulling the accepting authority"));

        log.step("node.create:mesh2.rpc.1", "WaitForNodeReady");
        assert!(matches!(reader.next().await.unwrap(), BuildReply::Step { step, .. } if step == "WaitForNodeReady"), "live frames follow the replay");
        let (replay, _) = log.attach();
        assert_eq!(names(&replay), vec!["step", "step"], "the step that completed cleared its blocker");

        log.finish(BuildReply::Complete { build_id: id().to_string(), attempt: 3 });
        assert_eq!(reader.next().await.unwrap().name(), "complete");
        assert!(reader.next().await.is_none(), "nothing follows the terminal");
    }

    #[test]
    fn a_failed_terminal_retains_its_typed_cause_without_reclassifying_the_explanation() {
        use rdm_core::outcome::Code;
        for code in [Code::Unknown, Code::DeadlineExceeded, Code::Unavailable] {
            let r = Reconciled::Failed { attempt: 1, operation: "node.start:mesh1.rpc.2".into(), reason: "same explanation".into(), code };
            let receipt = Some(("node.start:mesh1.rpc.2".into(), "StartNode".into(), "same explanation".into()));
            assert!(matches!(terminal_of(&id(), &r, receipt), BuildReply::Failed { code: held, .. } if held == code));
        }
    }

    /// CONTRACT: the terminal frame of a failed attempt names the step the failed receipt names
    /// when that receipt belongs to the failed operation, and carries the attempt's own reason.
    #[test]
    fn a_failed_attempt_names_the_step_its_failed_receipt_names() {
        let r = Reconciled::Failed { attempt: 2, operation: "node.create:mesh1.rpc.2".into(), reason: "node.create:mesh1.rpc.2: WaitForMeshJoin failed: no join".into() , code: rdm_core::outcome::Code::FailedPrecondition };
        let failed = Some(("node.create:mesh1.rpc.2".to_string(), "WaitForMeshJoin".to_string(), "no join".to_string()));
        assert_eq!(
            terminal_of(&id(), &r, failed),
            BuildReply::Failed { build_id: "vhpqxdbt5w2g".into(), attempt: 2, operation: "node.create:mesh1.rpc.2".into(), step: "WaitForMeshJoin".into(), reason: "node.create:mesh1.rpc.2: WaitForMeshJoin failed: no join".into() , code: rdm_core::outcome::Code::FailedPrecondition }
        );
        let other = Some(("node.create:mesh1.rpc.9".to_string(), "DeployRuntime".to_string(), "other".to_string()));
        assert!(matches!(terminal_of(&id(), &r, other), BuildReply::Failed { step, .. } if step == "attempt"), "a failed receipt of another operation is not this attempt's step");
    }

    use crate::build_state::{AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, ClaimOutcome, MemoryBuildStateAdapter};

    /// A Build state whose step appends fail: nothing is durable, so no frame may exist.
    struct FailingAppend(Arc<MemoryBuildStateAdapter>);

    #[async_trait::async_trait]
    impl BuildStateAdapter for FailingAppend {
        async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
            self.0.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
            self.0.open_attempt(o).await
        }
        async fn read_build(&self, id: &BuildId) -> Result<BuildProjection, BuildStateError> {
            self.0.read_build(id).await
        }
        async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
            self.0.current_build().await
        }
        async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, crate::build_state::BuildStateError> {
            self.0.newest_accepted_at_ms().await
        }
        async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
            self.0.list_active().await
        }
        async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
            self.0.claim_attempt(c).await
        }
        async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), BuildStateError> {
            self.0.adopt_claim(c).await
        }
        async fn append_step_receipt(&self, _: &BuildStepReceipt) -> Result<(), BuildStateError> {
            Err(BuildStateError::Io("the disk refused the receipt".into()))
        }
        async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
            self.0.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
            self.0.facts().await
        }
        async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
            self.0.committed_facts(build).await
        }
        async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(crate::build::BuildId, Option<String>, u32, String)>, BuildStateError> {
            self.0.recovery_of(path, node_id, incarnation).await
        }
        async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(crate::build::BuildId, u32, String)>, BuildStateError> {
            self.0.mesh_deleted_of(node_id, incarnation).await
        }
        async fn exits_owed(&self, build_id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, BuildStateError> {
            self.0.exits_owed(build_id, mesh).await
        }
        async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
            self.0.forget(id).await
        }
    }

    /// A Build state whose fact log may not be scanned: every lookup is a point read by key.
    struct NoScan(Arc<MemoryBuildStateAdapter>);

    #[async_trait::async_trait]
    impl BuildStateAdapter for NoScan {
        async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
            self.0.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
            self.0.open_attempt(o).await
        }
        async fn read_build(&self, id: &BuildId) -> Result<BuildProjection, BuildStateError> {
            self.0.read_build(id).await
        }
        async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
            self.0.current_build().await
        }
        async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, crate::build_state::BuildStateError> {
            self.0.newest_accepted_at_ms().await
        }
        async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
            self.0.list_active().await
        }
        async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
            self.0.claim_attempt(c).await
        }
        async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), BuildStateError> {
            self.0.adopt_claim(c).await
        }
        async fn append_step_receipt(&self, r: &BuildStepReceipt) -> Result<(), BuildStateError> {
            self.0.append_step_receipt(r).await
        }
        async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
            self.0.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
            panic!("the whole fact log was read for a lookup")
        }
        async fn committed_facts(&self, _build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
            panic!("the Build storage was read for a lookup")
        }
        async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
            self.0.recovery_of(path, node_id, incarnation).await
        }
        async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
            self.0.mesh_deleted_of(node_id, incarnation).await
        }
        async fn exits_owed(&self, build_id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, BuildStateError> {
            self.0.exits_owed(build_id, mesh).await
        }
        async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
            self.0.forget(id).await
        }
    }

    /// CONTRACT (CLAUDE.md read rule: a lookup is a point read through an index; node-recovery.md
    /// "A repeat submit answers AlreadyApplied"): the recovery dedup of an exited birth is read by
    /// its key `(node_id, incarnation)` through every decorator of the Build state, never by
    /// scanning the fact log.
    /// Must NOT happen: a decorator answering the lookup by reading every fact.
    #[tokio::test]
    async fn a_recovery_lookup_through_the_framed_builds_is_a_point_read_by_the_exited_birth() {
        let memory = Arc::new(MemoryBuildStateAdapter::new());
        let (path, node_id, incarnation): (crate::model::PathName, _, _) = ("mesh1.rpc.1".parse().unwrap(), crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let deleted = rdm_core::LifecycleOp { build_id: id().to_string(), attempt: 2, operation: format!("node.delete:{path}"), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        for (operation, step, output) in [("node.delete", "NodeDeleted", Some(serde_json::to_value(&deleted).unwrap())), ("node.delete", "Complete", None)] {
            memory
                .append_step_receipt(&BuildStepReceipt { mesh: None, build_id: id(), attempt: 2, operation: format!("{operation}:{path}"), step: step.into(), outcome: StepOutcome::Complete, output, executor: Some("mesh1.admin.1".into()) })
                .await
                .unwrap();
        }
        let framed = FramedBuilds::new(Arc::new(NoScan(memory)), Arc::new(AttemptRuns::default()));
        assert_eq!(framed.recovery_of(&path, &node_id, &incarnation).await.unwrap(), Some((id(), None, 2, format!("node.delete:{path}"))), "the exited birth's deleting workflow, by its key");
        assert_eq!(framed.recovery_of(&path, &node_id, &crate::model::IncarnationId::mint()).await.unwrap(), None, "another birth of the node was not recovered");
    }

    fn receipt(attempt: u32, step: &str, outcome: StepOutcome) -> BuildStepReceipt {
        BuildStepReceipt { mesh: None, build_id: id(), attempt, operation: "node.create:mesh1.rpc.2".into(), step: step.into(), outcome, output: None, executor: Some("mesh1.admin.1".into()) }
    }

    /// CONTRACT: a step frame is made only after the step's `Complete` receipt has been appended on
    /// the admin that wrote it. An append that fails makes no frame; a failed step makes none either
    /// (it is the terminal's step, never a step frame).
    #[tokio::test]
    async fn a_step_frame_follows_its_appended_receipt_and_never_precedes_or_replaces_it() {
        let runs = Arc::new(AttemptRuns::default());
        let (log, _) = runs.begin(&id(), None, 1);
        let mut reader = log.reader();
        let memory = Arc::new(MemoryBuildStateAdapter::new());
        let ok = FramedBuilds::new(memory.clone(), runs.clone());
        ok.append_step_receipt(&receipt(1, "AllocateIdentity", StepOutcome::Complete)).await.unwrap();
        assert!(matches!(reader.next().await.unwrap(), BuildReply::Step { step, .. } if step == "AllocateIdentity"));
        assert_eq!(memory.facts().await.unwrap().len(), 1, "the receipt was appended before the frame was readable");

        let failing = FramedBuilds::new(Arc::new(FailingAppend(memory.clone())), runs.clone());
        assert!(failing.append_step_receipt(&receipt(1, "PrepareStorage", StepOutcome::Complete)).await.is_err());
        ok.append_step_receipt(&receipt(1, "PrepareNetwork", StepOutcome::failed("no port"))).await.unwrap();
        log.finish(BuildReply::Complete { build_id: id().to_string(), attempt: 1 });
        assert_eq!(reader.next().await.unwrap().name(), "complete", "an append that failed and a failed step made no step frame");
        assert_eq!(log.last_failed_step(), Some(("node.create:mesh1.rpc.2".into(), "PrepareNetwork".into(), "no port".into())), "the failed step is kept for the terminal to name");
    }

    struct Nothing;

    #[async_trait::async_trait]
    impl crate::executor::OperationRunner for Nothing {
        async fn run(&self, _: &BuildId, _lane: Option<&str>, _: u32, _: &crate::build::BuildOperation) -> Result<(), crate::executor::RunEnd> {
            Ok(())
        }
    }

    async fn door(me: &str) -> (RunDoor, Arc<MemoryBuildStateAdapter>) {
        use crate::model::*;
        let memory = Arc::new(MemoryBuildStateAdapter::new());
        let runs = Arc::new(AttemptRuns::default());
        let builds: Arc<dyn BuildStateAdapter> = Arc::new(FramedBuilds::new(memory.clone(), runs.clone()));
        let mut admin = Node::allocated("mesh1.admin.1".parse().unwrap());
        admin.status = NodeStatus::Ready;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let topology = Arc::new(tokio::sync::RwLock::new(crate::topology::Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![admin],
        }));
        let exec = Arc::new(BuildExecutor { executor: me.into(), builds: builds.clone(), topology, runner: Arc::new(Nothing) });
        (RunDoor { me: me.parse().unwrap(), builds, exec, runs, local: memory.clone(), gate: crate::stop_gate::StopGate::new() }, memory)
    }

    async fn accepted(memory: &MemoryBuildStateAdapter) {
        memory
            .publish_accepted(&BuildAccepted { build_id: id(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
    }

    /// CONTRACT (acceptance 6): a call whose claim names another admin is refused `NotExecutor`
    /// naming both; a claim that is not the current folded attempt is refused `StaleClaim` naming the
    /// attempt and executor the projection holds; a Build the admin holds nothing of is
    /// `UnknownBuild`. Nothing is recorded or run by a refusal.
    #[tokio::test]
    async fn a_call_for_another_executor_or_a_stale_claim_is_refused_by_name_and_records_nothing() {
        let (door, memory) = door("mesh1.admin.1").await;
        let ctx = CallContext::default();
        assert!(matches!(door.attempt_run(&id(), 1, "mesh1.admin.1", &ctx, &[], &RunPlan::default()).await, Attached::Refused(BuildReply::UnknownBuild { .. })));
        accepted(&memory).await;
        assert!(matches!(
            door.attempt_run(&id(), 1, "mesh2.admin.1", &ctx, &[], &RunPlan::default()).await,
            Attached::Refused(BuildReply::NotExecutor { named, recipient }) if named == "mesh2.admin.1" && recipient == "mesh1.admin.1"
        ));
        // Attempt 1 is claimed and converged; attempt 4 is ahead of the Build, attempt 0 behind it.
        memory.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
        memory.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
        for carried in [4u32, 0] {
            assert_eq!(
                match door.attempt_run(&id(), carried, "mesh1.admin.1", &ctx, &[], &RunPlan::default()).await {
                    Attached::Refused(r) => r,
                    _ => panic!("attempt {carried} is not the current folded attempt"),
                },
                BuildReply::StaleClaim { held_attempt: 1, held_executor: Some("mesh1.admin.1".into()), carried_attempt: carried }
            );
        }
        assert_eq!(memory.facts().await.unwrap().len(), 3, "a refusal recorded nothing: acceptance, claim, verdict");
    }

    /// CONTRACT (fabric-recovery.md step 4, Q160: "a returning fabric-primary's writes carry its superseded
    /// revision and every reader ignores them, its Build calls are refused StaleClaim"; Q109's fold): under a
    /// fabric.delete, the lost fabric-primary mesh1.admin.1 held attempt 1 and recorded mesh2's roster; the
    /// executor mesh2.admin.1 won attempt 2 and recorded the transition, holding that one receipt of
    /// attempt 1. The lost fabric-primary returns and, before it learns the transfer, writes more of
    /// attempt 1 (mesh2 deleted, fabric.deleted, the attempt converged) and calls attempt 1 at an executor.
    /// Every reader's fold keeps attempt 2 running at mesh2.admin.1 with only the roster of attempt 1, and
    /// the call is refused StaleClaim naming attempt 2. Must NOT happen: a stale receipt read as the
    /// delete's progress (a mesh or the fabric reported deleted that the executor never deleted), the stale
    /// verdict deciding the Build, or the stale call run.
    #[tokio::test]
    async fn a_returning_fabric_primarys_writes_before_it_learns_the_transfer_are_ignored_and_its_call_refused_stale() {
        let (door, memory) = door("mesh2.admin.1").await;
        memory.publish_accepted(&BuildAccepted { build_id: id(), topology: crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: Default::default() }, submitted_change: Some(crate::accepted::TopologyChange::DeleteFabric), submitted_at_ms: 0 }).await.unwrap();
        let step = |attempt: u32, operation: &str, step: &str, executor: &str, output: Option<serde_json::Value>| BuildStepReceipt { mesh: None, build_id: id(), attempt, operation: operation.into(), step: step.into(), outcome: StepOutcome::Complete, output, executor: Some(executor.into()) };
        let (lost, lost_inc, exec, exec_inc) = (crate::model::NodeId::mint(), crate::model::IncarnationId::mint(), crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        memory.claim_attempt(&BuildAttemptClaim { claimant: Some("mesh1.admin.1".into()), mesh: None, build_id: id(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
        memory.append_step_receipt(&step(1, "mesh.delete:meshd0000002", "AcceptedRoster", "mesh1.admin.1", None)).await.unwrap();
        // The executor's winning claim of the next attempt, then its recorded transition.
        memory.claim_attempt(&BuildAttemptClaim { claimant: Some("mesh2.admin.1".into()), mesh: None, build_id: id(), attempt: 2, executor: "mesh2.admin.1".into() }).await.unwrap();
        let transition = serde_json::json!({
            "lost_node_id": lost, "lost_incarnation": lost_inc, "executor": "mesh2.admin.1", "executor_node_id": exec, "executor_incarnation": exec_inc,
            "fabric_revision": 2, "mesh": "mesh1", "mesh_revision": 2, "superseded_attempt": 1, "held": ["mesh.delete:meshd0000002 AcceptedRoster"],
        });
        memory.append_step_receipt(&step(2, &format!("fabric-recovery:{lost}"), "PrimariesTaken", "mesh2.admin.1", Some(transition))).await.unwrap();
        // The returning fabric-primary's writes, before it learns the transfer.
        memory.append_step_receipt(&step(1, "mesh.delete:meshd0000002", "MeshDeleted", "mesh1.admin.1", None)).await.unwrap();
        memory.append_step_receipt(&step(1, "fabric.delete:fabric1", "Complete", "mesh1.admin.1", None)).await.unwrap();
        memory.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
        let p = memory.read_build(&id()).await.unwrap();
        assert_eq!((p.state, p.attempt, p.executor.as_deref()), (crate::build_state::BuildState::Running, 2, Some("mesh2.admin.1")), "the executor's attempt runs; the stale verdict decides nothing");
        let of_first: Vec<String> = p.steps.iter().filter(|r| r.attempt == 1).map(|r| format!("{} {}", r.operation, r.step)).collect();
        assert_eq!(of_first, vec!["mesh.delete:meshd0000002 AcceptedRoster".to_string()], "only the receipt of attempt 1 the transition held is read: {:?}", p.steps);
        let ctx = CallContext::default();
        match door.attempt_run(&id(), 1, "mesh2.admin.1", &ctx, &[], &RunPlan::default()).await {
            Attached::Refused(r) => assert_eq!(r, BuildReply::StaleClaim { held_attempt: 2, held_executor: Some("mesh2.admin.1".into()), carried_attempt: 1 }),
            _ => panic!("the returning fabric-primary's call of attempt 1 is refused StaleClaim"),
        }
    }

    /// CONTRACT: a claimed attempt with no run on the admin is started (a fabric-primary that died
    /// after claiming, or an executor that was re-born); the run ends with its terminal; a call for
    /// the same attempt after its end reads the receipts it holds, replaying a frame per `Complete`
    /// receipt and the terminal, and runs nothing again.
    #[tokio::test]
    async fn a_claimed_attempt_is_started_and_a_call_after_its_end_replays_its_receipts_and_runs_nothing() {
        let (door, memory) = door("mesh1.admin.1").await;
        accepted(&memory).await;
        let ctx = CallContext::default();
        // The fabric-primary won attempt 1 for this admin and died before calling it.
        memory.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
        let Attached::Live { disposition, mut reader, .. } = door.attempt_run(&id(), 1, "mesh1.admin.1", &ctx, &[], &RunPlan::default()).await else { panic!("the claimed attempt is started") };
        assert_eq!(disposition, Disposition::Started);
        let mut last = None;
        while let Some(f) = reader.next().await {
            last = Some(f);
        }
        assert!(matches!(last, Some(BuildReply::Complete { attempt: 1, .. })), "{last:?}");
        // A recorded step of that attempt, then a call after the end.
        memory.append_step_receipt(&receipt(1, "AllocateIdentity", StepOutcome::Complete)).await.unwrap();
        memory.append_step_receipt(&receipt(1, "PrepareStorage", StepOutcome::failed("x"))).await.unwrap();
        match door.attempt_run(&id(), 1, "mesh1.admin.1", &ctx, &[], &RunPlan::default()).await {
            Attached::Ended { replay, terminal } => {
                assert_eq!(names(&replay), vec!["step"], "a frame per Complete receipt, none for a failed one");
                assert!(matches!(terminal, BuildReply::Complete { attempt: 1, .. }));
            }
            _ => panic!("an attempt with its verdict is read, never run again"),
        }
    }

    /// CONTRACT: docs/architecture/build.md:289 "the mesh-primary of the member's mesh opens the next attempt of B123 on its mesh's rows, claims and runs it itself (R-BI7); the Build stays completed, and the fabric-primary reads the attempt's verdict from the Build journal"
    #[tokio::test]
    async fn mesh_self_heal_attempt_runs_in_mesh_scope_while_build_stays_completed() {
        use crate::accepted::AttemptAction;
        use crate::build_state::AttemptReason;

        let (door, memory) = door("mesh2.admin.1").await;
        accepted(&memory).await;
        memory
            .claim_attempt(&BuildAttemptClaim {
                claimant: Some("mesh1.admin.1".into()),
                mesh: None,
                build_id: id(),
                attempt: 1,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();
        memory
            .append_attempt_receipt(&BuildAttemptReceipt {
                mesh: None,
                build_id: id(),
                attempt: 1,
                outcome: AttemptOutcome::Converged,
            })
            .await
            .unwrap();

        let victim_path: crate::model::PathName = "mesh2.rpc.1".parse().unwrap();
        let victim_id = crate::model::NodeId::mint();
        let victim_inc = crate::model::IncarnationId::mint();

        // Mesh-primary opens and claims attempt 1 of mesh2 on the completed Build.
        memory
            .open_attempt(&AttemptOpened {
                mesh: Some("mesh2".into()),
                build_id: id(),
                attempt: 1,
                reason: AttemptReason::Recovery,
                action: Some(AttemptAction::Recover {
                    path: victim_path.clone(),
                    node_id: victim_id,
                    from_incarnation: victim_inc,
                    backbone: false,
                }),
                opened_by: "mesh2.admin.1".into(),
                opened_at_ms: 100,
                context: None,
            })
            .await
            .unwrap();
        memory
            .claim_attempt(&BuildAttemptClaim {
                claimant: Some("mesh2.admin.1".into()),
                mesh: Some("mesh2".into()),
                build_id: id(),
                attempt: 1,
                executor: "mesh2.admin.1".into(),
            })
            .await
            .unwrap();

        let while_running = memory.read_build(&id()).await.unwrap();
        assert_eq!(
            while_running.state,
             crate::build_state::BuildState::Complete,
            "the Build stays completed while the mesh recovery attempt is running"
        );
        assert_eq!(while_running.completed_at, Some(1));
        assert_eq!(
            while_running.lanes.get("mesh2").map(|l| (l.state, l.attempt)),
            Some((crate::build_state::BuildState::Running, 1))
        );

        let ctx = CallContext::default();
        let Attached::Live {
            disposition,
            mut reader,
            ..
        } = door
            .attempt_run_in(
                Some("mesh2"),
                &id(),
                1,
                "mesh2.admin.1",
                &ctx,
                &[],
                &RunPlan::default(),
            )
            .await
        else {
            panic!("mesh2.admin.1 starts its claimed mesh recovery attempt");
        };
        assert_eq!(disposition, Disposition::Started);
        let mut last = None;
        while let Some(f) = reader.next().await {
            last = Some(f);
        }
        assert!(matches!(last, Some(BuildReply::Complete { attempt: 1, .. })));

        let after = memory.read_build(&id()).await.unwrap();
        assert_eq!(after.state, crate::build_state::BuildState::Complete);
        assert_eq!(
            after.lanes.get("mesh2").map(|l| (l.state, l.attempt)),
            Some((crate::build_state::BuildState::Complete, 1))
        );
    }
}
