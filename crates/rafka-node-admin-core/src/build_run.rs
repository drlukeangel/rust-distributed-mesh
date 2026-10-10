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
//!   [`RunLog`]. Only a failed step or a fence ends an attempt.

use crate::build::BuildId;
use crate::build_state::{AttemptOutcome, BuildFact, BuildProjection, BuildState, BuildStateAdapter, BuildStateError, BuildStepReceipt, LocalBuildLog, StepOutcome};
use crate::executor::{BuildExecutor, Reconciled, RunPlan};
use crate::model::PathName;
use rafka_node_rpc_contract::build::{BuildReply, Disposition};
use rafka_node_rpc_contract::context::CallContext;
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

/// What one run of one attempt has produced: the frames every call attached to it reads.
pub struct RunLog {
    build_id: String,
    attempt: u32,
    state: Mutex<RunState>,
    wake: Notify,
}

impl RunLog {
    fn new(build_id: &BuildId, attempt: u32) -> Arc<Self> {
        Arc::new(Self { build_id: build_id.0.clone(), attempt, state: Mutex::new(RunState::default()), wake: Notify::new() })
    }

    /// A step's `Complete` receipt is durable: its frame. A repeat of a step already framed is
    /// dropped. A step that completes clears the blocker that waited on it.
    pub fn step(&self, operation: &str, step: &str) {
        let mut s = self.state.lock().unwrap();
        let held = s.steps.iter().any(|f| matches!(f, BuildReply::Step { operation: o, step: t, .. } if o == operation && t == step));
        if held {
            return;
        }
        let frame = BuildReply::Step { build_id: self.build_id.clone(), attempt: self.attempt, operation: operation.into(), step: step.into() };
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

/// The runs this admin holds, by `(build_id, attempt)`.
#[derive(Default)]
pub struct AttemptRuns {
    runs: Mutex<HashMap<(String, u32), Arc<RunLog>>>,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl AttemptRuns {
    /// The run of `attempt` of `build_id`, when this admin is running it.
    pub fn get(&self, build_id: &BuildId, attempt: u32) -> Option<Arc<RunLog>> {
        self.runs.lock().unwrap().get(&(build_id.0.clone(), attempt)).cloned()
    }

    /// The run of `attempt`: the one held, or a new one (`true`).
    fn begin(&self, build_id: &BuildId, attempt: u32) -> (Arc<RunLog>, bool) {
        let mut runs = self.runs.lock().unwrap();
        match runs.get(&(build_id.0.clone(), attempt)) {
            Some(log) => (log.clone(), false),
            None => {
                let log = RunLog::new(build_id, attempt);
                runs.insert((build_id.0.clone(), attempt), log.clone());
                (log, true)
            }
        }
    }

    fn end(&self, build_id: &BuildId, attempt: u32) {
        self.runs.lock().unwrap().remove(&(build_id.0.clone(), attempt));
    }

    fn track(&self, task: tokio::task::AbortHandle) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|t| !t.is_finished());
        tasks.push(task);
    }

    /// A fabric shutdown froze reconciliation: every run this admin holds stops where it is, as the
    /// executor loop's own abort stopped it before runs were tasks of their own.
    pub fn abort_all(&self) {
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }
}

/// This admin's Build state, with a frame produced whenever a step's `Complete` receipt has been
/// appended. Every pipeline writes its receipts through the one `builds` handle, so this is the
/// single place a step frame is made, and it is made after the append returned: the frame never
/// precedes the durable receipt on the admin that wrote it.
pub struct FramedBuilds {
    inner: Arc<dyn BuildStateAdapter>,
    runs: Arc<AttemptRuns>,
}

impl FramedBuilds {
    /// `inner` with frames into `runs`.
    pub fn new(inner: Arc<dyn BuildStateAdapter>, runs: Arc<AttemptRuns>) -> Self {
        Self { inner, runs }
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
    async fn publish_fabric(&self, record: &crate::fabric_storage::FabricRecord) -> Result<(), BuildStateError> {
        self.inner.publish_fabric(record).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, claim: &crate::build_state::BuildAttemptClaim) -> Result<crate::build_state::ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(claim).await
    }
    async fn adopt_claim(&self, claim: &crate::build_state::BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(claim).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.inner.append_step_receipt(receipt).await?;
        if let Some(run) = self.runs.get(&receipt.build_id, receipt.attempt) {
            match &receipt.outcome {
                StepOutcome::Complete => run.step(&receipt.operation, &receipt.step),
                StepOutcome::Failed { reason } => run.failed_step(&receipt.operation, &receipt.step, reason),
            }
        }
        Ok(())
    }
    async fn append_attempt_receipt(&self, receipt: &crate::build_state::BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }
    async fn note_blocked(&self, build_id: &BuildId, attempt: u32, operation: &str, step: &str, reason: &str) {
        if let Some(run) = self.runs.get(build_id, attempt) {
            run.blocked(operation, step, reason);
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
}

impl RunDoor {
    /// Answer `build.attempt.run` for `attempt` of `build_id`, claimed for `executor`. `intent` is
    /// the Build's intent the fabric-primary carried (empty for a run it makes itself) and `plan`
    /// the run it planned: the operations this admin runs.
    pub async fn attempt_run(&self, build_id: &BuildId, attempt: u32, executor: &str, context: &CallContext, intent: &[Vec<u8>], plan: &RunPlan) -> Attached {
        let span = tracing::info_span!(
            "rdm.node_admin.build.serve.via-attempt-run",
            node = %self.me,
            build_id = %build_id,
            attempt,
            executor,
            outcome = tracing::field::Empty,
        );
        let attached = self.attach_or_start(build_id, attempt, executor, context, intent, plan).instrument(span.clone()).await;
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

    async fn attach_or_start(&self, build_id: &BuildId, attempt: u32, executor: &str, context: &CallContext, intent: &[Vec<u8>], plan: &RunPlan) -> Attached {
        if executor != self.me.to_string() {
            tracing::info_span!("rdm.node_admin.build.reject.via-not-executor", node = %self.me, build_id = %build_id, attempt, named = executor)
                .in_scope(|| tracing::info!("the claim names another admin as the executor"));
            return Attached::Refused(BuildReply::NotExecutor { named: executor.to_string(), recipient: self.me.to_string() });
        }
        if let Some(log) = self.runs.get(build_id, attempt) {
            let (replay, reader) = log.attach();
            return Attached::Live { disposition: Disposition::Reattached, replay, reader };
        }
        // The intent the fabric-primary decided the claim on is absorbed before anything is planned:
        // insert-and-fail on the facts' own keys, so what the Build topic delivers too changes nothing.
        let mut facts = Vec::new();
        for (i, frame) in intent.iter().enumerate() {
            match crate::fabric_builds::BuildMessage::from_bytes(frame) {
                Ok(m) => facts.extend(m.facts),
                Err(e) => return Attached::Refused(BuildReply::Rejected { reason: "intent-undecodable".into(), detail: format!("{}: intent frame {i} of Build {build_id} does not decode: {e}", self.me) }),
            }
        }
        self.local.absorb_facts(&facts).await;
        let p = match self.builds.read_build(build_id).await {
            Ok(p) => p,
            Err(BuildStateError::UnknownBuild(_)) => return Attached::Refused(BuildReply::UnknownBuild { build_id: build_id.0.clone() }),
            Err(e) => return Attached::Refused(BuildReply::NotReady { reason: format!("{}: Build {build_id} could not be read: {e}", self.me) }),
        };
        let held = p.attempt == attempt && p.executor.as_deref() == Some(executor);
        let next = p.attempt + 1 == attempt && p.state != BuildState::Complete;
        if held && p.state != BuildState::Running {
            return self.ended(build_id, attempt, &p).await;
        }
        if !(held || next) {
            tracing::info_span!("rdm.node_admin.build.reject.via-stale-claim", node = %self.me, build_id = %build_id, attempt, held_attempt = p.attempt, held_executor = p.executor.as_deref().unwrap_or(""))
                .in_scope(|| tracing::info!("the claim is not the current folded attempt of this admin's projection"));
            return Attached::Refused(BuildReply::StaleClaim { held_attempt: p.attempt, held_executor: p.executor.clone(), carried_attempt: attempt });
        }
        let (log, created) = self.runs.begin(build_id, attempt);
        if !created {
            let (replay, reader) = log.attach();
            return Attached::Live { disposition: Disposition::Reattached, replay, reader };
        }
        let reader = log.reader();
        let (exec, runs, build_id, context, plan) = (self.exec.clone(), self.runs.clone(), build_id.clone(), context.clone(), plan.clone());
        let run = log.clone();
        let task = tokio::spawn(
            {
                let runs = runs.clone();
                async move {
                    let r = exec.run_attempt(&build_id, attempt, &context, &plan).await;
                    run.finish(terminal_of(&build_id, &r, run.last_failed_step()));
                    runs.end(&build_id, attempt);
                }
            }
            .with_current_subscriber(),
        );
        runs.track(task.abort_handle());
        Attached::Live { disposition: Disposition::Started, replay: Vec::new(), reader }
    }

    /// The attempt has its verdict and no run here: its `Complete` receipts, then its terminal.
    async fn ended(&self, build_id: &BuildId, attempt: u32, p: &BuildProjection) -> Attached {
        let facts = match self.builds.facts().await {
            Ok(f) => f,
            Err(e) => return Attached::Refused(BuildReply::NotReady { reason: format!("{}: the facts of Build {build_id} could not be read: {e}", self.me) }),
        };
        let mut replay = Vec::new();
        let mut failed = None;
        let mut verdict = None;
        for f in facts.iter().filter(|f| f.build_id() == build_id) {
            match f {
                BuildFact::Step(s) if s.attempt == attempt => match &s.outcome {
                    StepOutcome::Complete => {
                        let frame = BuildReply::Step { build_id: build_id.0.clone(), attempt, operation: s.operation.clone(), step: s.step.clone() };
                        if !replay.contains(&frame) {
                            replay.push(frame);
                        }
                    }
                    StepOutcome::Failed { reason } => failed = Some((s.operation.clone(), s.step.clone(), reason.clone())),
                },
                BuildFact::Attempt(a) if a.attempt == attempt && verdict.is_none() => verdict = Some(a.outcome.clone()),
                _ => {}
            }
        }
        let terminal = match verdict {
            Some(AttemptOutcome::Converged) => BuildReply::Complete { build_id: build_id.0.clone(), attempt },
            Some(AttemptOutcome::HandedOff { to }) => BuildReply::HandedOff { build_id: build_id.0.clone(), attempt, to },
            Some(AttemptOutcome::Failed { reason }) => {
                let (operation, step, _) = failed.unwrap_or_else(|| (String::new(), "attempt".into(), String::new()));
                BuildReply::Failed { build_id: build_id.0.clone(), attempt, operation, step, reason: bounded_reason(reason) }
            }
            None => {
                return Attached::Refused(BuildReply::NotReady { reason: format!("{}: attempt {attempt} of Build {build_id} is {:?} with no verdict receipt and no run here", self.me, p.state) });
            }
        };
        Attached::Ended { replay, terminal }
    }
}

/// The terminal frame an attempt's end is: `Complete`, `Failed` naming the step, or `HandedOff`.
pub fn terminal_of(build_id: &BuildId, r: &Reconciled, failed: Option<(String, String, String)>) -> BuildReply {
    match r {
        Reconciled::Converged { attempt, .. } => BuildReply::Complete { build_id: build_id.0.clone(), attempt: *attempt },
        Reconciled::HandedOff { attempt, to, .. } => BuildReply::HandedOff { build_id: build_id.0.clone(), attempt: *attempt, to: to.clone() },
        Reconciled::Failed { attempt, operation, reason } => {
            // The step a receipt names is the one that failed when its operation is the attempt's.
            // The reason is the attempt's own, word for word the verdict receipt's.
            let step = match failed {
                Some((op, step, _)) if operation.is_empty() || op == *operation => step,
                _ => "attempt".to_string(),
            };
            BuildReply::Failed { build_id: build_id.0.clone(), attempt: *attempt, operation: operation.clone(), step, reason: bounded_reason(reason.clone()) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> BuildId {
        BuildId("bld-run".into())
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
        log.step("create-node:mesh2.rpc.1", "AllocateIdentity");
        log.step("create-node:mesh2.rpc.1", "AllocateIdentity");
        log.blocked("create-node:mesh2.rpc.1", "WaitForNodeReady", "authority not ready");
        log.blocked("create-node:mesh2.rpc.1", "WaitForNodeReady", "authority not ready");
        log.blocked("create-node:mesh2.rpc.1", "WaitForNodeReady", "pulling the accepting authority");
        let mut first = log.reader();
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(first.next().await.unwrap());
        }
        assert_eq!(names(&seen), vec!["step", "blocked", "blocked"], "a repeated step and a repeated blocker make no frame");

        let (replay, mut reader) = log.attach();
        assert_eq!(names(&replay), vec!["step", "blocked"], "a Step per Complete receipt, then only the current Blocked");
        assert!(matches!(&replay[1], BuildReply::Blocked { reason, .. } if reason == "pulling the accepting authority"));

        log.step("create-node:mesh2.rpc.1", "WaitForNodeReady");
        assert!(matches!(reader.next().await.unwrap(), BuildReply::Step { step, .. } if step == "WaitForNodeReady"), "live frames follow the replay");
        let (replay, _) = log.attach();
        assert_eq!(names(&replay), vec!["step", "step"], "the step that completed cleared its blocker");

        log.finish(BuildReply::Complete { build_id: id().0, attempt: 3 });
        assert_eq!(reader.next().await.unwrap().name(), "complete");
        assert!(reader.next().await.is_none(), "nothing follows the terminal");
    }

    /// CONTRACT: the terminal frame of a failed attempt names the step the failed receipt names
    /// when that receipt belongs to the failed operation, and carries the attempt's own reason.
    #[test]
    fn a_failed_attempt_names_the_step_its_failed_receipt_names() {
        let r = Reconciled::Failed { attempt: 2, operation: "create-node:mesh1.rpc.2".into(), reason: "create-node:mesh1.rpc.2: WaitForMeshJoin failed: no join".into() };
        let failed = Some(("create-node:mesh1.rpc.2".to_string(), "WaitForMeshJoin".to_string(), "no join".to_string()));
        assert_eq!(
            terminal_of(&id(), &r, failed),
            BuildReply::Failed { build_id: "bld-run".into(), attempt: 2, operation: "create-node:mesh1.rpc.2".into(), step: "WaitForMeshJoin".into(), reason: "create-node:mesh1.rpc.2: WaitForMeshJoin failed: no join".into() }
        );
        let other = Some(("create-node:mesh1.rpc.9".to_string(), "DeployRuntime".to_string(), "other".to_string()));
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
        async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
            self.0.forget(id).await
        }
    }

    fn receipt(attempt: u32, step: &str, outcome: StepOutcome) -> BuildStepReceipt {
        BuildStepReceipt { build_id: id(), attempt, operation: "create-node:mesh1.rpc.2".into(), step: step.into(), outcome, output: None, executor: Some("mesh1.admin.1".into()) }
    }

    /// CONTRACT: a step frame is made only after the step's `Complete` receipt has been appended on
    /// the admin that wrote it. An append that fails makes no frame; a failed step makes none either
    /// (it is the terminal's step, never a step frame).
    #[tokio::test]
    async fn a_step_frame_follows_its_appended_receipt_and_never_precedes_or_replaces_it() {
        let runs = Arc::new(AttemptRuns::default());
        let (log, _) = runs.begin(&id(), 1);
        let mut reader = log.reader();
        let memory = Arc::new(MemoryBuildStateAdapter::new());
        let ok = FramedBuilds::new(memory.clone(), runs.clone());
        ok.append_step_receipt(&receipt(1, "AllocateIdentity", StepOutcome::Complete)).await.unwrap();
        assert!(matches!(reader.next().await.unwrap(), BuildReply::Step { step, .. } if step == "AllocateIdentity"));
        assert_eq!(memory.facts().await.unwrap().len(), 1, "the receipt was appended before the frame was readable");

        let failing = FramedBuilds::new(Arc::new(FailingAppend(memory.clone())), runs.clone());
        assert!(failing.append_step_receipt(&receipt(1, "PrepareStorage", StepOutcome::Complete)).await.is_err());
        ok.append_step_receipt(&receipt(1, "PrepareNetwork", StepOutcome::Failed { reason: "no port".into() })).await.unwrap();
        log.finish(BuildReply::Complete { build_id: id().0, attempt: 1 });
        assert_eq!(reader.next().await.unwrap().name(), "complete", "an append that failed and a failed step made no step frame");
        assert_eq!(log.last_failed_step(), Some(("create-node:mesh1.rpc.2".into(), "PrepareNetwork".into(), "no port".into())), "the failed step is kept for the terminal to name");
    }

    struct Nothing;

    #[async_trait::async_trait]
    impl crate::executor::OperationRunner for Nothing {
        async fn run(&self, _: &BuildId, _: u32, _: &crate::build::BuildOperation) -> Result<(), String> {
            Ok(())
        }
    }

    async fn door(me: &str) -> (RunDoor, Arc<MemoryBuildStateAdapter>) {
        use crate::model::*;
        let memory = Arc::new(MemoryBuildStateAdapter::new());
        let runs = Arc::new(AttemptRuns::default());
        let builds: Arc<dyn BuildStateAdapter> = Arc::new(FramedBuilds::new(memory.clone(), runs.clone()));
        let mut admin = Node::allocated("mesh1.admin.1".parse().unwrap());
        admin.status = NodeStatus::ReadyForTraffic;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let topology = Arc::new(tokio::sync::RwLock::new(crate::topology::Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
            nodes: vec![admin],
        }));
        let exec = Arc::new(BuildExecutor { executor: me.into(), builds: builds.clone(), topology, runner: Arc::new(Nothing) });
        (RunDoor { me: me.parse().unwrap(), builds, exec, runs, local: memory.clone() }, memory)
    }

    async fn accepted(memory: &MemoryBuildStateAdapter) {
        memory
            .publish_accepted(&BuildAccepted { build_id: id(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
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
        memory.claim_attempt(&BuildAttemptClaim { build_id: id(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
        memory.append_attempt_receipt(&BuildAttemptReceipt { build_id: id(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
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

    /// CONTRACT: a claimed attempt with no run on the admin is started (a fabric-primary that died
    /// after claiming, or an executor that was reborn); the run ends with its terminal; a call for
    /// the same attempt after its end reads the receipts it holds, replaying a frame per `Complete`
    /// receipt and the terminal, and runs nothing again.
    #[tokio::test]
    async fn a_claimed_attempt_is_started_and_a_call_after_its_end_replays_its_receipts_and_runs_nothing() {
        let (door, memory) = door("mesh1.admin.1").await;
        accepted(&memory).await;
        let ctx = CallContext::default();
        // The fabric-primary won attempt 1 for this admin and died before calling it.
        memory.claim_attempt(&BuildAttemptClaim { build_id: id(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
        let Attached::Live { disposition, mut reader, .. } = door.attempt_run(&id(), 1, "mesh1.admin.1", &ctx, &[], &RunPlan::default()).await else { panic!("the claimed attempt is started") };
        assert_eq!(disposition, Disposition::Started);
        let mut last = None;
        while let Some(f) = reader.next().await {
            last = Some(f);
        }
        assert!(matches!(last, Some(BuildReply::Complete { attempt: 1, .. })), "{last:?}");
        // A recorded step of that attempt, then a call after the end.
        memory.append_step_receipt(&receipt(1, "AllocateIdentity", StepOutcome::Complete)).await.unwrap();
        memory.append_step_receipt(&receipt(1, "PrepareStorage", StepOutcome::Failed { reason: "x".into() })).await.unwrap();
        match door.attempt_run(&id(), 1, "mesh1.admin.1", &ctx, &[], &RunPlan::default()).await {
            Attached::Ended { replay, terminal } => {
                assert_eq!(names(&replay), vec!["step"], "a frame per Complete receipt, none for a failed one");
                assert!(matches!(terminal, BuildReply::Complete { attempt: 1, .. }));
            }
            _ => panic!("an attempt with its verdict is read, never run again"),
        }
    }
}
