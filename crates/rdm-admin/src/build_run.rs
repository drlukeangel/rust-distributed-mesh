//! The mesh-primary's side of `build.attempt.run` (op `0x20`; node-rpc-envelope.md "Build, op `0x20`").
//!
//! The fabric-primary dispatches each mesh's section of a Build to that mesh's primary, all at once
//! (Q250), with `AttemptRun { build_id, mesh, context }`, and runs its own section in process. This
//! module is what the admin running a section answers with:
//!
//! - **start-or-reattach on `(build_id, mesh)`.** A call for a section this admin is running
//!   attaches to that run: it is replayed a `Step` frame per step the run wrote, then the current
//!   `Pending` if the run is still pending, then the run's live frames. A call with no run here
//!   starts one: the admin reads the Build and its events from storage (Q292) and plans its own
//!   mesh's section; a step whose `rdm.build.step` event ended `complete` is never run again.
//! - **refusals by name.** A call to an admin that is not the mesh's primary in its view is
//!   `NotExecutor { named, recipient }`.
//! - **frames after durability.** A `Step` frame is produced only once the step's `rdm.build.step`
//!   event is written on this admin ([`FramedBuilds`] writes it). `Pending` is kept in memory per
//!   run, sent when its reason changes, and is never a step outcome.
//! - **a cut call cancels nothing.** The run is its own task; the stream is a view of its
//!   [`RunLog`]. Only a step that ends failed ends a section `Failed`.
//! - **a run is a turn of the admin's stop gate** (stop_gate.rs). A run served for a call begins
//!   its turn at its own wait point, before it runs anything; a section the admin's own drive pass
//!   runs in process begins its turn under the pass's ([`crate::stop_gate::Turn::under`]).

use crate::build::{BuildId, BuildOperation};
use crate::build_state::{BuildProjection, BuildStateAdapter, BuildStateError, LocalBuildLog, StepOutcome, StepWrite};
use crate::executor::{BuildExecutor, SectionEnd};
use crate::model::PathName;
use rdm_core::build::{BuildReply, Disposition, StepResult};
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

/// A step's result on the wire.
pub fn result_of(outcome: &StepOutcome) -> StepResult {
    match outcome {
        StepOutcome::Complete => StepResult::Complete,
        StepOutcome::Failed { reason, code } => StepResult::Failed { reason: bounded_reason(reason.clone()), code: *code },
    }
}

#[derive(Default)]
struct RunState {
    /// Every `Step` and `Pending` frame, in the order they were produced.
    events: Vec<BuildReply>,
    /// The `Step` frames alone: what a reattaching call is replayed.
    steps: Vec<BuildReply>,
    /// The blocker the run holds now, `(operation, step, reason)`.
    pending: Option<(String, String, String)>,
    /// The last step to fail, `(operation, step, reason)`: what the terminal `Failed` names.
    failed: Option<(String, String, String)>,
    /// The section's terminal frame.
    terminal: Option<BuildReply>,
}

/// What one run of one mesh's section has produced: the frames every call attached to it reads.
pub struct RunLog {
    build_id: String,
    mesh: String,
    state: Mutex<RunState>,
    wake: Notify,
    /// Whether the fabric-primary runs it in its own drive pass: its step writes are Build calls of the
    /// fabric-primary, refused once the role moved (fabric-recovery.md step 4).
    fabric_primarys: std::sync::atomic::AtomicBool,
}

impl RunLog {
    fn new(build_id: &BuildId, mesh: &str) -> Arc<Self> {
        Arc::new(Self { build_id: build_id.to_string(), mesh: mesh.to_string(), state: Mutex::new(RunState::default()), wake: Notify::new(), fabric_primarys: Default::default() })
    }

    /// A step's `rdm.build.step` event is written: its frame. A repeat of a step already framed with
    /// the same result is dropped. A step that ends clears the blocker that waited on it; a failed step
    /// is what the section's `Failed` names.
    pub fn step(&self, operation: &str, step: &str, result: StepResult) {
        let mut s = self.state.lock().unwrap();
        let held = s.steps.iter().any(|f| matches!(f, BuildReply::Step { operation: o, step: t, result: r, .. } if o == operation && t == step && r == &result));
        if held {
            return;
        }
        if let StepResult::Failed { reason, .. } = &result {
            s.failed = Some((operation.to_string(), step.to_string(), reason.clone()));
        }
        let frame = BuildReply::Step { build_id: self.build_id.clone(), mesh: self.mesh.clone(), operation: operation.into(), step: step.into(), result };
        s.steps.push(frame.clone());
        s.events.push(frame);
        if s.pending.as_ref().is_some_and(|(o, t, _)| o == operation && t == step) {
            s.pending = None;
        }
        drop(s);
        self.wake.notify_waiters();
    }

    /// The run is pending at `step` of `operation` (its node's `pre_node_ready` answered `Pending`). A
    /// frame goes out only when the reason changes; it stays in memory for a call that attaches later.
    pub fn pending(&self, operation: &str, step: &str, reason: &str) {
        let mut s = self.state.lock().unwrap();
        let now = (operation.to_string(), step.to_string(), reason.to_string());
        if s.pending.as_ref() == Some(&now) {
            return;
        }
        s.pending = Some(now);
        s.events.push(BuildReply::Pending { build_id: self.build_id.clone(), mesh: self.mesh.clone(), operation: operation.into(), step: step.into(), reason: bounded_reason(reason.into()) });
        drop(s);
        self.wake.notify_waiters();
    }

    /// The last step to fail, when one did: `(operation, step, reason)`.
    pub fn last_failed_step(&self) -> Option<(String, String, String)> {
        self.state.lock().unwrap().failed.clone()
    }

    /// The section ended: `terminal` follows the last frame.
    pub fn finish(&self, terminal: BuildReply) {
        let mut s = self.state.lock().unwrap();
        if s.terminal.is_none() {
            s.pending = None;
            s.terminal = Some(terminal);
        }
        drop(s);
        self.wake.notify_waiters();
    }

    /// A call attached to the run after it began: the frames it is replayed (a `Step` per step
    /// written, then the current `Pending` if the run is still pending) and the live reader after them.
    pub fn attach(self: &Arc<Self>) -> (Vec<BuildReply>, RunReader) {
        let s = self.state.lock().unwrap();
        let mut replay = s.steps.clone();
        if let Some((operation, step, reason)) = &s.pending {
            replay.push(BuildReply::Pending { build_id: self.build_id.clone(), mesh: self.mesh.clone(), operation: operation.clone(), step: step.clone(), reason: bounded_reason(reason.clone()) });
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
    /// The next frame in order: every `Step` and `Pending` frame, then the terminal, then `None`.
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

/// The section runs this admin holds, by `(build_id, mesh)`.
#[derive(Default)]
pub struct SectionRuns {
    runs: Mutex<HashMap<(String, String), Arc<RunLog>>>,
    /// Each run's task, and whether this admin runs sections no longer ([`Self::stop`]): one lock,
    /// so no run's task starts after the stop.
    tasks: Mutex<(Vec<tokio::task::JoinHandle<()>>, bool)>,
}

impl SectionRuns {
    /// The run of `mesh`'s section of `build_id`, when this admin is running it.
    pub fn get(&self, build_id: &BuildId, mesh: &str) -> Option<Arc<RunLog>> {
        self.runs.lock().unwrap().get(&(build_id.to_string(), mesh.to_string())).cloned()
    }

    /// The run of `mesh`'s section: the one held, or a new one (`true`).
    fn begin(&self, build_id: &BuildId, mesh: &str) -> (Arc<RunLog>, bool) {
        let mut runs = self.runs.lock().unwrap();
        let key = (build_id.to_string(), mesh.to_string());
        match runs.get(&key) {
            Some(log) => (log.clone(), false),
            None => {
                let log = RunLog::new(build_id, mesh);
                runs.insert(key, log.clone());
                (log, true)
            }
        }
    }

    fn end(&self, build_id: &BuildId, mesh: &str) {
        self.runs.lock().unwrap().remove(&(build_id.to_string(), mesh.to_string()));
    }

    /// Whether the fabric-primary runs a section of `build_id` here in its own drive pass.
    fn run_by_the_fabric_primary(&self, build_id: &BuildId) -> bool {
        let id = build_id.to_string();
        self.runs.lock().unwrap().iter().any(|((b, _), log)| *b == id && log.fabric_primarys.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// Run `run` as a task of its own, unless this admin runs sections no longer ([`Self::stop`]):
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

    /// This admin runs sections no longer (`Running::stop_reconciling`): every run it holds ends where
    /// it is, and none starts after this. It returns once each run it ended has ended. Returns how many
    /// runs were ended.
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
            .in_scope(|| tracing::info!("this admin runs no section from now on: every run's task is ended and none starts"));
        ended
    }
}

/// This admin's Build state as its runs write it: each step's `rdm.build.step` event written by this
/// admin (build.md §5: the mesh's mesh-primary writes its mesh's steps), then its frame in the run of
/// `(build_id, mesh)`. Every pipeline writes its steps through the one `builds` handle, so this is the
/// single place a step frame is made, and it is made after the event is written.
pub struct FramedBuilds {
    inner: Arc<dyn BuildStateAdapter>,
    runs: Arc<SectionRuns>,
    /// Where this admin writes each step's event, and this admin's own mesh (a step that names no mesh
    /// is its own); none in a rig with no events store, where steps are framed only.
    steps: Option<(crate::events_storage::EventsWriter, String)>,
    /// The fabric-primary check a step of the fabric-primary's own run passes before it is written
    /// (node-rpc-envelope.md "Refusals and bounds"): refused `RoleYielded` once the record names
    /// another live birth; none for an admin that drives no Build.
    role: Option<Arc<crate::fabric_builds::FabricBuildStateAdapter>>,
}

impl FramedBuilds {
    /// `inner` with frames into `runs`.
    pub fn new(inner: Arc<dyn BuildStateAdapter>, runs: Arc<SectionRuns>) -> Self {
        Self { inner, runs, steps: None, role: None }
    }

    /// [`Self::new`], a step of a run the fabric-primary runs in its own drive pass written only while
    /// `role`'s record still names this admin (fabric-recovery.md step 4: a returning fabric-primary's
    /// Build calls are refused, and it stops on its first read after the heal).
    pub fn checked_by(self, role: Arc<crate::fabric_builds::FabricBuildStateAdapter>) -> Self {
        Self { role: Some(role), ..self }
    }

    /// [`Self::new`], each step this admin runs written as its `rdm.build.step` event through `events`,
    /// `mesh` the admin's own mesh.
    pub fn writing_steps(self, events: crate::events_storage::EventsWriter, mesh: impl Into<String>) -> Self {
        Self { steps: Some((events, mesh.into())), ..self }
    }

    /// `inner`, each step written as its `rdm.build.step` event into `inner`'s events store by a birth of
    /// `node_id` in `fabric_id`, `mesh` its own mesh: a Build store with no admin around it, as a cell
    /// runs a step.
    pub fn stepping(inner: Arc<dyn BuildStateAdapter>, fabric_id: &rdm_core::FabricId, node_id: &rdm_core::NodeId, mesh: &str) -> Self {
        let events = crate::events_storage::EventsWriter::on(inner.events(), fabric_id, node_id);
        Self::new(inner, Arc::new(SectionRuns::default())).writing_steps(events, mesh)
    }

    /// The mesh `step` belongs to: its own, else this admin's.
    fn mesh_of(&self, step: &StepWrite) -> String {
        step.mesh.clone().or_else(|| self.steps.as_ref().map(|(_, own)| own.clone())).unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl BuildStateAdapter for FramedBuilds {
    async fn publish_accepted(&self, accepted: &crate::build_state::BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
    }

    async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, BuildStateError> {
        self.inner.current_build().await
    }

    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }

    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        self.inner.accepted_newest_first().await
    }

    fn events(&self) -> Arc<dyn crate::events_storage::EventsStorage> {
        self.inner.events()
    }

    fn held_name(&self) -> Option<PathName> {
        self.inner.held_name()
    }

    /// The `rdm.build.step` event of `step`, by this admin (build.md §5): its subject the node the
    /// operation acts on, none for a mesh or fabric operation; then its frame.
    async fn write_step(&self, step: &StepWrite) -> Result<(), BuildStateError> {
        let mesh = self.mesh_of(step);
        let op = step.operation.to_string();
        if let Some(role) = self.role.as_ref().filter(|_| self.runs.run_by_the_fabric_primary(&step.build_id)) {
            role.target_check().await?;
        }
        if let Some((events, _)) = &self.steps {
            let state = step.outcome.state();
            let span = tracing::info_span!("rdm.node_admin.build.update.via-step-event", build_id = %step.build_id.log(), mesh = %mesh, op = %op, step = %step.step, state = ?state, event_id = tracing::field::Empty);
            let written = events.append(|source, time_ms| crate::events_storage::Event::build_step(source, time_ms, step.operation.node_id(), &step.build_id, &mesh, &step.step, &op, &state)).instrument(span.clone()).await;
            match written {
                Ok(id) => {
                    span.record("event_id", id.as_str());
                }
                Err(e) => return Err(BuildStateError::Io(format!("the rdm.build.step event of {op} step {} could not be written: {e}", step.step))),
            }
        }
        if let Some(run) = self.runs.get(&step.build_id, &mesh) {
            run.step(&op, &step.step, result_of(&step.outcome));
        }
        Ok(())
    }

    async fn note_pending(&self, build_id: &BuildId, operation: &rdm_core::OperationId, step: &str, reason: &str) {
        let mesh = self.steps.as_ref().map(|(_, own)| own.clone()).unwrap_or_default();
        let held = self.runs.runs.lock().unwrap().iter().filter(|((b, _), _)| b == build_id.as_str()).map(|(_, r)| r.clone()).collect::<Vec<_>>();
        for run in held.iter().filter(|r| r.mesh == mesh || held.len() == 1) {
            run.pending(&operation.to_string(), step, reason);
        }
    }
}

/// What `build.attempt.run` came to on this admin.
pub enum Attached {
    /// The section is running here: `replay` first, then the reader.
    Live {
        /// How the call came to the run.
        disposition: Disposition,
        /// The frames the call is replayed before the live ones.
        replay: Vec<BuildReply>,
        /// The live frames.
        reader: RunReader,
    },
    /// The section has nothing left to run: the steps of it whose event ended `complete`, then the
    /// terminal.
    Ended {
        /// A `Step` frame per step of the mesh whose event ended `complete`.
        replay: Vec<BuildReply>,
        /// The section's terminal frame.
        terminal: BuildReply,
    },
    /// The call is refused, by name.
    Refused(BuildReply),
}

/// The door an admin answers `build.attempt.run` through.
pub struct RunDoor {
    /// This admin's path.
    pub me_path: String,
    /// This node's name, beside its path.
    pub me_name: String,
    /// This admin's Build state, as the pipelines write it (through [`FramedBuilds`]).
    pub builds: Arc<dyn BuildStateAdapter>,
    /// The executor that runs a section.
    pub exec: Arc<BuildExecutor>,
    /// The runs this admin holds.
    pub runs: Arc<SectionRuns>,
    /// This admin's own Build log, where the Build's committed records read from storage are absorbed
    /// before the section is planned (Q292).
    pub local: Arc<dyn LocalBuildLog>,
    /// This admin's stop gate (stop_gate.rs): a stopped admin's tasks are held at it.
    pub gate: crate::stop_gate::StopGate,
}

impl RunDoor {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName {
        rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() }
    }

    /// The path name this admin holds now (`builds.held_name()` when joined to the fabric Build
    /// topic, else the name its executor resolves from the topology view, or `self.me`).
    pub async fn held_name(&self) -> PathName {
        if let Some(held) = self.builds.held_name() {
            return held;
        }
        if let Some(held) = self.exec.builds.held_name() {
            return held;
        }
        // The name the view holds for this executor's own birth, whatever name it booted under.
        if let Some(n) = self.exec.topology.read().await.own(&self.exec.executor.node_id) {
            return n.location();
        }
        self.me()
    }

    /// Answer `build.attempt.run` for `mesh`'s section of `build_id`: this admin plans the section
    /// itself from the Build and its events, read from storage (Q292).
    pub async fn attempt_run(&self, build_id: &BuildId, mesh: &str, context: &CallContext) -> Attached {
        let me = self.held_name().await;
        let span = tracing::info_span!("rdm.node_admin.build.serve.via-attempt-run", node_path = %me.path, node_name = %me.name, build_id = %build_id.log(), mesh, outcome = tracing::field::Empty);
        let attached = self.attach_or_start(None, build_id, mesh, context, None).instrument(span.clone()).await;
        span.record("outcome", outcome_of(&attached));
        span.in_scope(|| match &attached {
            Attached::Refused(r) => tracing::info!(reply = r.name(), "build.attempt.run refused"),
            _ => tracing::info!("build.attempt.run accepted"),
        });
        attached
    }

    /// The fabric-primary's own drive pass running `operations`, `mesh`'s section of `build_id`, on
    /// this admin in the pass's `turn`: the run begins its own turn under it, at once, stopped gate or
    /// not (the pass waits on the run).
    pub async fn run_under(&self, turn: &crate::stop_gate::Turn, build_id: &BuildId, mesh: &str, context: &CallContext, operations: Vec<BuildOperation>) -> Attached {
        let me = self.held_name().await;
        let span = tracing::info_span!("rdm.node_admin.build.serve.via-own-section", node_path = %me.path, node_name = %me.name, build_id = %build_id.log(), mesh, outcome = tracing::field::Empty);
        let attached = self.attach_or_start(Some(turn), build_id, mesh, context, Some(operations)).instrument(span.clone()).await;
        span.record("outcome", outcome_of(&attached));
        attached
    }

    async fn attach_or_start(&self, under: Option<&crate::stop_gate::Turn>, build_id: &BuildId, mesh: &str, context: &CallContext, planned: Option<Vec<BuildOperation>>) -> Attached {
        let me = self.held_name().await;
        let me_id = &self.exec.executor.node_id;
        if let Some(log) = self.runs.get(build_id, mesh) {
            let (replay, reader) = log.attach();
            return Attached::Live { disposition: Disposition::Reattached, replay, reader };
        }
        let operations = match planned {
            Some(ops) => ops,
            None => {
                // A section dispatched by the fabric-primary: this admin must be the mesh's primary.
                let t = self.exec.topology.read().await.clone();
                let primary = crate::executor::mesh_primary(mesh, &t);
                if primary.as_ref().map(|b| &b.node_id) != Some(me_id) {
                    tracing::info_span!("rdm.node_admin.build.reject.via-not-executor", node_path = %me.path, node_name = %me.name, build_id = %build_id.log(), mesh, named = ?primary)
                        .in_scope(|| tracing::info!("this admin is not the mesh's primary in its view"));
                    let named = primary.unwrap_or_else(|| self.exec.executor.clone());
                    return Attached::Refused(rdm_core::reply::build::not_executor(named, self.exec.executor.clone()));
                }
                // The Build is read from its committed records before anything is planned (Q292: a
                // dispatch names the Build and the mesh, never a copy of either). An unreadable store
                // refuses the run by name; it is never an empty Build.
                let facts = match self.local.committed_facts(Some(build_id)).await {
                    Ok(facts) => facts,
                    Err(BuildStateError::UnknownBuild(_)) => return Attached::Refused(rdm_core::reply::build::unknown_build(build_id.to_string())),
                    Err(e) => return Attached::Refused(rdm_core::reply::build::not_ready(format!("node_id={me_id}: the committed records of Build {build_id} could not be read: {e}"))),
                };
                self.local.absorb_facts(&facts).await;
                let p = match self.builds.read_build(build_id).await {
                    Ok(p) => p,
                    Err(BuildStateError::UnknownBuild(_)) => return Attached::Refused(rdm_core::reply::build::unknown_build(build_id.to_string())),
                    Err(e) => return Attached::Refused(rdm_core::reply::build::not_ready(format!("node_id={me_id}: Build {build_id} could not be read: {e}"))),
                };
                let ops = section_of(&p, mesh, &t, me_id);
                if ops.is_empty() {
                    return ended(build_id, mesh, &p);
                }
                ops
            }
        };
        let (log, created) = self.runs.begin(build_id, mesh);
        if !created {
            let (replay, reader) = log.attach();
            return Attached::Live { disposition: Disposition::Reattached, replay, reader };
        }
        if under.is_some() {
            log.fabric_primarys.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let reader = log.reader();
        // The run's turn of the admin's gate: under the drive pass that runs it here, begun now; for a
        // served call, begun by the run itself at its wait point, before it runs anything.
        let gated = tracing::info_span!("rdm.node_admin.build.serve.via-run-turn", node_path = %me.path, node_name = %me.name, build_id = %build_id.log(), mesh, turn = tracing::field::Empty);
        let held = under.map(|t| t.under("build-run"));
        let gate = self.gate.clone();
        let (exec, runs, id, context, mesh_name) = (self.exec.clone(), self.runs.clone(), build_id.clone(), context.clone(), mesh.to_string());
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
                    let _turn = _turn.of_build(&id.to_string());
                    let end = exec.run_section(&id, &mesh_name, &context, &operations).await;
                    run.finish(terminal_of(&id, &mesh_name, &end, run.last_failed_step()));
                    runs.end(&id, &mesh_name);
                }
            }
            .with_current_subscriber(),
        );
        if !spawned {
            runs.end(build_id, mesh);
            return Attached::Refused(rdm_core::reply::build::not_ready(format!("node_id={me_id}: this admin stopped reconciling: it runs no section of Build {build_id}")));
        }
        Attached::Live { disposition: Disposition::Started, replay: Vec::new(), reader }
    }
}

fn outcome_of(attached: &Attached) -> &'static str {
    match attached {
        Attached::Live { disposition: Disposition::Reattached, .. } => "reattached",
        Attached::Live { .. } => "started",
        Attached::Ended { .. } => "ended",
        Attached::Refused(r) => r.name(),
    }
}

/// `mesh`'s section of `p` as admin `me` plans it in view `t` (Q292): the operations left of the
/// Build that `me` executes on `mesh`, in plan order. A step whose event ended `complete` is done.
pub fn section_of(p: &BuildProjection, mesh: &str, t: &crate::topology::Topology, me: &rdm_core::NodeId) -> Vec<BuildOperation> {
    let plan = crate::accepted::plan_for_build_created(p, t, &Default::default());
    plan.operations
        .into_iter()
        .filter(|op| op.mesh() == Some(mesh) && crate::executor::executor_for(op, t).as_ref() == Some(me) && !p.operation_completed(&op.key()))
        .collect()
}

/// A section with nothing left here: the steps of `mesh` whose event ended `complete`, then
/// `Complete`, or `Failed` naming the step whose event ended failed when the Build failed there.
fn ended(build_id: &BuildId, mesh: &str, p: &BuildProjection) -> Attached {
    let mut replay: Vec<BuildReply> = Vec::new();
    for s in p.steps_in(mesh).filter(|s| s.outcome == StepOutcome::Complete) {
        let frame = BuildReply::Step { build_id: build_id.to_string(), mesh: mesh.to_string(), operation: s.operation.clone(), step: s.step.clone(), result: StepResult::Complete };
        if !replay.contains(&frame) {
            replay.push(frame);
        }
    }
    let failed = p.steps_in(mesh).rev().find(|s| matches!(s.outcome, StepOutcome::Failed { .. }) && !p.completed(&s.operation, &s.step));
    let terminal = match failed {
        Some(s) => {
            let StepOutcome::Failed { reason, code } = &s.outcome else { unreachable!("a failed step") };
            rdm_core::reply::build::incomplete(build_id.to_string(), vec![rdm_core::build::UnfinishedSection { mesh: mesh.to_string(), operation: s.operation.clone(), step: s.step.clone(), reason: bounded_reason(reason.clone()), code: code.clone() }])
        }
        None => BuildReply::Complete { build_id: build_id.to_string() },
    };
    Attached::Ended { replay, terminal }
}

/// The terminal frame a section's end is: `Complete`, or `Failed` naming the step. A section that
/// ended incomplete, interrupted or for a replan ends `NotReady` naming why: the fabric-primary plans
/// again, and the Build stays in progress.
pub fn terminal_of(build_id: &BuildId, mesh: &str, end: &SectionEnd, failed: Option<(String, String, String)>) -> BuildReply {
    // A section that ended short ends `Incomplete`, naming its mesh's one section (Q346).
    let short = |operation: String, step: String, reason: String, code| rdm_core::reply::build::incomplete(build_id.to_string(), vec![rdm_core::build::UnfinishedSection { mesh: mesh.to_string(), operation, step, reason: bounded_reason(reason), code }]);
    match end {
        SectionEnd::Complete { .. } => BuildReply::Complete { build_id: build_id.to_string() },
        // The view names another admin for what follows: what this section ran stands, and the
        // fabric-primary plans again from the events.
        SectionEnd::Replan { .. } => BuildReply::Complete { build_id: build_id.to_string() },
        SectionEnd::Failed { operation, reason, code } => {
            // The step a failed event names is the one that failed when its operation is the section's.
            let op = operation.to_string();
            let step = match failed {
                Some((o, step, _)) if o == op => step,
                _ => "section".to_string(),
            };
            short(op, step, reason.clone(), *code)
        }
        SectionEnd::Incomplete { operation, reason } => short(operation.to_string(), "section".into(), reason.clone(), rdm_core::outcome::Code::Unavailable),
        SectionEnd::Interrupted { delete } => short(String::new(), "fabric.delete".into(), crate::build_state::interrupted_reason(&BuildId::parse(build_id.as_str()).unwrap_or_else(|_| delete.clone()), delete), rdm_core::outcome::Code::Aborted),
    }
}
