//! The fabric-primary's drive of a Build (op `0x20`; node-rpc-envelope.md "Build, op `0x20`").
//!
//! The fabric-primary is the one driver of every Build, whether a caller is attached or not. For each
//! active Build, each pass:
//!
//! 1. reads the Build and its events; a Build not yet started gets its `rdm.build.started` event and
//!    the application its `on_build_started` notice;
//! 2. plans what is left from the Build's topology, the records and the steps whose
//!    `rdm.build.step` event ended `complete`;
//! 3. runs its own section in process (mesh creation and deletion, and the members of any mesh with
//!    no mesh-primary) and dispatches every other mesh's section to that mesh's primary with
//!    `build.attempt.run`, each after its `rdm.build.dispatched` event, all at once (Q250), passing
//!    each section's frames into the Build's [`Drive`];
//! 4. a step that ends `failed { reason }` ends the Build: `rdm.build.ended` `failed`, and the
//!    application's `on_build_ended`; nothing left ends it `complete` the same way. A section lost
//!    with its mesh-primary leaves the Build in progress: the next pass sends it to the mesh's new
//!    primary, which runs the steps its mesh owes (elections §4.1).
//!
//! Callers (`build.create`) read the [`Drive`]'s frames; a cut caller cancels nothing, because the
//! drive is its own task and the caller is a view of its frames. A successor fabric-primary resumes
//! from the Build and its events: no step whose event ended `complete` runs twice.
//!
//! Before every dispatch the drive passes two gates: the Build log's target is not set (a
//! fabric-primary whose view names another live birth fabric-primary dispatches nothing), and this
//! admin is the fabric-primary in its view. A loss names the fabric-primary the installed result
//! names.
//!
//! Each pass of a drive is one turn of its admin's stop gate ([`crate::stop_gate`]): a stopped admin
//! begins none, and a stop waits for the pass in progress before it cuts.

use crate::build::BuildId;
use crate::build::BuildOperation;
use crate::build_state::{BuildProjection, BuildState, BuildStateAdapter, BuildStateError};
use crate::model::PathName;
use crate::topology::Topology;
use rdm_core::build::{BuildReply, StepResult, UnfinishedSection};
use rdm_core::context::CallContext;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, RwLock};
use tracing::instrument::WithSubscriber as _;
use tracing::Instrument as _;

/// The gates every dispatch passes, and what the drive reads beside the Build.
#[async_trait::async_trait]
pub trait Gate: Send + Sync {
    /// The paths this Build's own creates hold in the atlas: never surplus (Luke 2026-10-30).
    async fn atlas_created(&self, _build: &BuildProjection) -> std::collections::BTreeSet<PathName> {
        std::collections::BTreeSet::new()
    }
    /// The fabric-primary the Build log's target names, when it names another; an error ends the
    /// drive `RoleLost`.
    async fn target(&self) -> Result<Option<rdm_core::NodeId>, BuildStateError> {
        Ok(None)
    }
    /// Resolves once the target is set.
    async fn target_set(&self) {
        std::future::pending::<()>().await
    }
    /// How long `mesh` has been unheard on the backbone, when it is held unheard (R-D1).
    fn mesh_unheard_ms(&self, _mesh: &str) -> Option<u64> {
        None
    }
    /// Move the roles `m` names before `operation`, a step of `build_id` (node.primary.update).
    async fn update_primaries(&self, _m: &crate::executor::RoleMove, _build_id: &BuildId, _operation: &rdm_core::OperationId) -> Result<(), RoleMoveEnd> {
        Ok(())
    }
    /// Whether every update in `updates` was acknowledged (node.primary.updated), before the
    /// operations they precede are dispatched.
    async fn primaries_updated(&self, _updates: &[PrecedingUpdate], _build_id: &BuildId) -> Result<(), String> {
        Ok(())
    }
    /// The Build's `rdm.build.started` is written: the fabric's change, which names the accepted Build
    /// once it runs, decides again now.
    async fn build_started(&self, _build_id: &BuildId) {}
    /// The Build completed.
    async fn build_completed(&self, _build_id: &BuildId) {}
    /// The Build failed.
    async fn build_failed(&self, _build_id: &BuildId) {}
    /// A step of `operation` was passed on to the Build's callers from `mesh`'s section.
    async fn step_streamed(&self, _mesh: &str, _build_id: &BuildId, _operation: &str) {}
    /// Rafka-time now, in milliseconds.
    fn now_ms(&self) -> u64 {
        0
    }
    /// The meshes the Mesh records hold Dead.
    async fn deleted_meshes(&self, _observed: &Topology) -> Result<std::collections::HashSet<crate::model::MeshId>, String> {
        Ok(Default::default())
    }
    /// The Mesh rows on the store (a fabric.delete's remaining meshes).
    async fn mesh_rows(&self) -> Result<Vec<crate::storage::MeshRecord>, String> {
        Ok(Vec::new())
    }
    /// The node records of `ids` on the store (the births a fabric.delete's begun node.deletes act on).
    async fn node_rows(&self, _ids: &[rdm_core::NodeId]) -> Result<Vec<crate::storage::NodeRow>, String> {
        Ok(Vec::new())
    }
    /// Whether the fabric holds ready.
    fn fabric_ready(&self) -> bool {
        true
    }
}

/// One node.primary.update that precedes an operation of a Build (node-primary-update.md): the mesh
/// whose primary acknowledges it, its operation, and the primary it moves.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PrecedingUpdate {
    /// The mesh whose primary answers node.primary.updated.
    pub mesh: String,
    /// The update's operation, `node.primary.update:<build_id>:<op>` by its step.
    pub operation: rdm_core::OperationId,
    /// The primary it moves, when it names one.
    pub primary: Option<rdm_core::OperationSubject>,
}

/// The node.primary.update operations that precede `op` (node-primary-update.md, "Commander").
pub fn preceding_updates(op: &BuildOperation) -> Vec<PrecedingUpdate> {
    use crate::primary_update::update_operation;
    use rdm_core::PrimaryRole;
    let key = op.key();
    match op {
        BuildOperation::ShutdownMesh { mesh, .. } => vec![PrecedingUpdate { mesh: mesh.clone(), operation: update_operation(PrimaryRole::FabricPrimary, &key), primary: None }],
        BuildOperation::DeleteLiveNode { exited: None, .. } if op.r#type().as_deref().is_some_and(rdm_core::is_node_admin) => {
            [PrimaryRole::MeshPrimary, PrimaryRole::FabricPrimary].into_iter().map(|role| PrecedingUpdate { mesh: op.mesh().expect("node operation has mesh").to_owned(), operation: update_operation(role, &key), primary: op.subject() }).collect()
        }
        // Any other node type's primary, commanded by its mesh-primary (node-primary-update.md, "Commander").
        BuildOperation::DeleteLiveNode { exited: None, .. } => {
            vec![PrecedingUpdate { mesh: op.mesh().expect("node operation has mesh").to_owned(), operation: update_operation(PrimaryRole::of_type(&op.r#type().expect("a node operation has a type")), &key), primary: op.subject() }]
        }
        _ => Vec::new(),
    }
}

/// A gate that holds nothing: every dispatch passes.
pub struct OpenGate;

#[async_trait::async_trait]
impl Gate for OpenGate {
    async fn target(&self) -> Result<Option<rdm_core::NodeId>, BuildStateError> {
        Ok(None)
    }
}

/// What a dispatch of a section came to.
pub enum Dispatched {
    /// The section is running: its stream.
    Stream(Box<dyn InnerStream>),
    /// The call was refused, by name.
    Refused(BuildReply),
    /// The mesh-primary could not be reached.
    Unreached(String),
}

/// One item of a section's stream.
pub enum InnerItem {
    /// A frame.
    Frame(BuildReply),
    /// The stream ended.
    Ended,
    /// The stream broke (the mesh-primary lost): Indeterminate.
    Broke(String),
}

/// A section's stream.
#[async_trait::async_trait]
pub trait InnerStream: Send {
    /// The next item.
    async fn next(&mut self) -> InnerItem;
    /// The stream was accepted (Started read).
    async fn accepted(&mut self) {}
}

/// How the drive reaches the admin that runs a section.
#[async_trait::async_trait]
pub trait Dispatcher: Send + Sync {
    /// Send `mesh`'s section of `build_id` to the mesh's primary (`build.attempt.run`): one call,
    /// addressed to the role under the call's one retry mechanism (Q344).
    async fn dispatch(&self, build_id: &BuildId, mesh: &str, context: CallContext) -> Dispatched;
    /// Run `operations`, `mesh`'s section of `build_id` that this admin executes, in process under the
    /// drive pass's `turn`.
    async fn run_own(&self, turn: &crate::stop_gate::Turn, build_id: &BuildId, mesh: &str, context: CallContext, operations: Vec<BuildOperation>) -> Dispatched;
}

/// The fabric-primary role moved off this admin while it drove a Build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleLoss {
    /// Why, in words.
    pub why: String,
    /// The fabric-primary the installed result names, when it names one.
    pub to: Option<rdm_core::NodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StallKey {
    Reason(String),
    UnheardMesh { mesh: String, operation: rdm_core::OperationId },
}

#[derive(Default)]
struct DriveState {
    events: Vec<BuildReply>,
    seen: HashSet<FrameKey>,
    terminal: Option<BuildReply>,
    lost: Option<RoleLoss>,
    stall: Option<StallKey>,
    idle: Option<String>,
}

/// One Build's drive on the fabric-primary: the frames its sections produced, its terminal, and the
/// state a pass reads beside the Build.
pub struct Drive {
    /// The Build.
    pub build_id: BuildId,
    state: Mutex<DriveState>,
    wake: Notify,
    running: AtomicBool,
    again: AtomicBool,
    dead: Mutex<std::collections::HashSet<crate::model::MeshId>>,
    callers: Mutex<Vec<iroh::PublicKey>>,
    mesh_rows: Mutex<Option<Vec<crate::storage::MeshRecord>>>,
    /// A fabric.delete's begun node.deletes' node records, for each birth the view no longer holds.
    node_rows: Mutex<Vec<crate::storage::NodeRow>>,
    created: Mutex<std::collections::BTreeSet<PathName>>,
    /// Whether this drive has passed on any frame of the Build: a drive that has not (a fabric-primary
    /// that took the role mid-Build, or after it ended) replays the steps whose event ended `complete`.
    replayed: AtomicBool,
    /// The sections of this run that ended short of their end, by mesh (Q346): nothing of a mesh here
    /// is dispatched again in this run, and the Build ends `incomplete` naming each.
    unfinished: Mutex<std::collections::BTreeMap<String, UnfinishedSection>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum FrameKey {
    Step { mesh: String, operation: String, step: String },
    Pending { mesh: String, operation: String, step: String, reason: String },
}

fn frame_key(f: &BuildReply) -> Option<FrameKey> {
    match f {
        BuildReply::Step { mesh, operation, step, .. } => Some(FrameKey::Step { mesh: mesh.clone(), operation: operation.clone(), step: step.clone() }),
        BuildReply::Pending { mesh, operation, step, reason, .. } => Some(FrameKey::Pending { mesh: mesh.clone(), operation: operation.clone(), step: step.clone(), reason: reason.clone() }),
        _ => None,
    }
}

impl Drive {
    fn new(build_id: BuildId) -> Arc<Self> {
        Arc::new(Self {
            build_id,
            state: Mutex::new(DriveState::default()),
            wake: Notify::new(),
            running: AtomicBool::new(false),
            again: AtomicBool::new(false),
            dead: Mutex::new(Default::default()),
            callers: Mutex::new(Vec::new()),
            mesh_rows: Mutex::new(None),
            node_rows: Mutex::new(Vec::new()),
            created: Mutex::new(Default::default()),
            replayed: AtomicBool::new(false),
            unfinished: Mutex::new(Default::default()),
        })
    }

    /// A drive no task runs: a fixture's.
    pub fn detached(build_id: BuildId) -> Arc<Self> {
        Self::new(build_id)
    }

    /// Pass `frame` on to the Build's callers, unless its key was passed on already. Answers whether
    /// it was.
    pub fn push(&self, frame: BuildReply) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.terminal.is_some() || frame_key(&frame).is_some_and(|key| !s.seen.insert(key)) {
            return false;
        }
        s.events.push(frame);
        drop(s);
        self.replayed.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
        true
    }

    /// The Build ended: `terminal` follows the last frame.
    pub fn finish(&self, terminal: BuildReply) {
        let mut s = self.state.lock().unwrap();
        if s.terminal.is_none() {
            s.terminal = Some(terminal);
        }
        drop(s);
        self.wake.notify_waiters();
    }

    /// The fabric-primary role moved off this admin: every reader ends naming it.
    pub fn lose_fabric_primary(&self, loss: RoleLoss) {
        let mut s = self.state.lock().unwrap();
        if s.lost.is_none() && s.terminal.is_none() {
            s.lost = Some(loss);
        }
        drop(s);
        self.wake.notify_waiters();
    }

    /// Whether the Build completed under this drive.
    pub fn completed(&self) -> bool {
        matches!(self.state.lock().unwrap().terminal, Some(BuildReply::Complete { .. }))
    }

    /// Whether the drive ended: a terminal, or the role lost.
    pub fn ended(&self) -> bool {
        let s = self.state.lock().unwrap();
        s.terminal.is_some() || s.lost.is_some()
    }

    /// A reader of every frame the drive passed on, then its end.
    pub fn reader(self: &Arc<Self>) -> DriveReader {
        DriveReader { drive: self.clone(), cursor: 0, done: false, caller: None }
    }

    /// The callers reading the drive now.
    pub fn callers(&self) -> Vec<iroh::PublicKey> {
        self.callers.lock().unwrap().clone()
    }

    /// Resolves once the drive ended and no caller reads it.
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

    fn stall_is_new(&self, reason: StallKey) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.stall.as_ref() == Some(&reason) {
            return false;
        }
        s.stall = Some(reason);
        true
    }

    fn clear_stall(&self) {
        self.state.lock().unwrap().stall = None;
    }
}

/// What a reader of a drive reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    /// A frame.
    Frame(BuildReply),
    /// The Build's terminal.
    Terminal(BuildReply),
    /// The fabric-primary role moved off this admin.
    RoleLost(RoleLoss),
}

/// A caller's read position in a [`Drive`].
pub struct DriveReader {
    drive: Arc<Drive>,
    cursor: usize,
    done: bool,
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
    /// This reader is `caller`'s: the drive keeps its callers while they read.
    pub fn of_caller(mut self, caller: Option<iroh::PublicKey>) -> Self {
        if let Some(key) = caller {
            self.drive.callers.lock().unwrap().push(key);
            self.caller = Some(key);
        }
        self
    }

    /// The Build read.
    pub fn build_id(&self) -> &BuildId {
        &self.drive.build_id
    }

    /// The next frame, then the end, then `None`.
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
                if let Some(frame) = s.events.get(self.cursor) {
                    self.cursor += 1;
                    return Some(Read::Frame(frame.clone()));
                }
                if let Some(t) = &s.terminal {
                    self.done = true;
                    return Some(Read::Terminal(t.clone()));
                }
                if let Some(loss) = &s.lost {
                    self.done = true;
                    return Some(Read::RoleLost(loss.clone()));
                }
            }
            notified.await;
        }
    }
}

/// How a role move before an operation ended short of complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleMoveEnd {
    /// node.primary.update was refused before its row.
    Refused(String),
    /// It did not complete.
    Incomplete(String),
    /// A step of it failed.
    Failed {
        /// The step.
        step: String,
        /// Why.
        reason: String,
        /// The captured standard code.
        code: rdm_core::outcome::Code,
    },
}

impl std::fmt::Display for RoleMoveEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(why) | Self::Incomplete(why) => f.write_str(why),
            Self::Failed { step, reason, .. } => write!(f, "{step}: {reason}"),
        }
    }
}

/// How one drive of a Build ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriveEnd {
    /// The Build has its terminal.
    Terminal,
    /// The drive cannot go on until the world changes; the Build stays in progress.
    Stalled(String),
    /// The fabric-primary role moved off this admin.
    RoleLost(String),
}

/// The fabric-primary's Build-level events (build.md §5) and the application's notices from their
/// writes (fabric-node-hooks.md): `rdm.build.started` and `on_build_started`, `rdm.build.dispatched`,
/// `rdm.build.ended` and `on_build_ended`.
#[derive(Clone, Default)]
pub struct BuildNotices {
    fabric_id: Option<rdm_core::FabricId>,
    on_build_started: Option<crate::wiring::BuildStartedNotice>,
    on_build_ended: Option<crate::wiring::BuildEndedNotice>,
    events: Option<crate::events_storage::EventsWriter>,
}

impl BuildNotices {
    /// The notices `hooks` registers, for `fabric_id`.
    pub fn of(fabric_id: rdm_core::FabricId, hooks: &crate::wiring::FabricHooks) -> Self {
        Self { fabric_id: Some(fabric_id), on_build_started: hooks.on_build_started.clone(), on_build_ended: hooks.on_build_ended.clone(), events: None }
    }

    /// These notices, their events written through `events`.
    pub fn writing_to(self, events: crate::events_storage::EventsWriter) -> Self {
        Self { events: Some(events), ..self }
    }

    async fn write(&self, span: &tracing::Span, make: impl FnOnce(&crate::events_storage::EventSource, u64) -> crate::events_storage::Event) -> Result<(), String> {
        let Some(events) = &self.events else { return Ok(()) };
        let event_id = events.append(make).instrument(span.clone()).await?;
        span.record("event_id", event_id.as_str());
        Ok(())
    }

    /// The Build's `rdm.build.started`, then `on_build_started` from that write. `Err` when the event
    /// was not written.
    pub async fn started(&self, me: &PathName, build_id: &BuildId, context: &CallContext) -> Result<(), String> {
        let hook = self.fabric_id.clone().zip(self.on_build_started.clone());
        let span = tracing::info_span!(parent: None, "rdm.node_admin.build.update.via-on-build-started", node_path = %me.path, node_name = %me.name, build_id = %build_id.log(), event_id = tracing::field::Empty, outcome = if hook.is_some() { "sent" } else { "no-hook" }, "otel.kind" = "internal");
        if let Some(tp) = &context.traceparent {
            rdm_transport::set_remote_parent(&span, tp, context.tracestate.as_deref());
        }
        if let Err(cause) = self.write(&span, |source, time_ms| crate::events_storage::Event::build_started(source, time_ms, build_id)).await {
            span.record("outcome", "event-not-written");
            span.in_scope(|| tracing::info!(cause = %cause, "the Build's rdm.build.started event was not written: the notice is not sent"));
            return Err(cause);
        }
        let (build_id, context) = (build_id.clone(), context.clone());
        tokio::spawn(
            async move {
                let Some((fabric_id, hook)) = hook else {
                    tracing::info!("no on_build_started hook is registered");
                    return;
                };
                hook(crate::wiring::BuildStartedCtx { fabric_id, build_id: build_id.to_string(), context }).await;
                tracing::info!("the application was told the Build started");
            }
            .instrument(span),
        );
        Ok(())
    }

    /// `mesh`'s section of the Build was dispatched: `rdm.build.dispatched`.
    pub async fn dispatched(&self, me: &PathName, build_id: &BuildId, mesh: &str) {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-dispatched", node_path = %me.path, node_name = %me.name, build_id = %build_id.log(), mesh, event_id = tracing::field::Empty, outcome = "written");
        if let Err(cause) = self.write(&span, |source, time_ms| crate::events_storage::Event::build_dispatched(source, time_ms, build_id, mesh)).await {
            span.record("outcome", "event-not-written");
            span.in_scope(|| tracing::info!(cause = %cause, "the section's rdm.build.dispatched event was not written"));
        }
    }

    /// The Build's `rdm.build.ended` `state`, then `on_build_ended` from that write. `Err` when the
    /// event was not written.
    pub async fn ended(&self, me: &PathName, build_id: &BuildId, state: crate::wiring::BuildEndedState, parent: &tracing::Span) -> Result<(), String> {
        let hook = self.fabric_id.clone().zip(self.on_build_ended.clone());
        let span = tracing::info_span!(parent: None, "rdm.node_admin.build.update.via-on-build-ended", node_path = %me.path, node_name = %me.name, build_id = %build_id.log(), state = state.as_str(), event_id = tracing::field::Empty, outcome = if hook.is_some() { "sent" } else { "no-hook" }, "otel.kind" = "internal");
        let under = parent.in_scope(crate::build::current_context);
        if let Some(tp) = &under.traceparent {
            rdm_transport::set_remote_parent(&span, tp, under.tracestate.as_deref());
        }
        let ended = match &state {
            crate::wiring::BuildEndedState::Complete => crate::events_storage::EndedState::Complete,
            crate::wiring::BuildEndedState::Incomplete { sections } => crate::events_storage::EndedState::Incomplete {
                sections: sections.iter().map(|u| crate::events_storage::EndedSection { mesh: u.mesh.clone(), op: u.operation.clone(), step: u.step.clone(), reason: u.reason.clone(), code: u.code.name().to_string() }).collect(),
            },
            crate::wiring::BuildEndedState::Failed { reason, code } => crate::events_storage::EndedState::Failed { reason: reason.clone(), code: code.name().to_string() },
        };
        if let Err(cause) = self.write(&span, |source, time_ms| crate::events_storage::Event::build_ended(source, time_ms, build_id, &ended)).await {
            span.record("outcome", "event-not-written");
            span.in_scope(|| tracing::info!(cause = %cause, "the Build's rdm.build.ended event was not written: the notice is not sent"));
            return Err(cause);
        }
        let build_id = build_id.clone();
        tokio::spawn(
            async move {
                let Some((fabric_id, hook)) = hook else {
                    tracing::info!("no on_build_ended hook is registered");
                    return;
                };
                hook(crate::wiring::BuildEndedCtx { fabric_id, build_id: build_id.to_string(), state, context: under }).await;
                tracing::info!("the application was told the Build ended");
            }
            .instrument(span),
        );
        Ok(())
    }
}

/// What a terminal frame is as `on_build_ended`'s state.
fn ended_state(t: &BuildReply) -> crate::wiring::BuildEndedState {
    match t {
        BuildReply::Failed { reason, code, .. } => crate::wiring::BuildEndedState::Failed { reason: reason.clone(), code: *code },
        BuildReply::Incomplete { sections, .. } => crate::wiring::BuildEndedState::Incomplete { sections: sections.clone() },
        _ => crate::wiring::BuildEndedState::Complete,
    }
}

/// What every drive of this admin reads and writes through.
pub struct DriveEnv {
    /// This admin's mesh.
    pub me_mesh: String,
    /// This admin's path.
    pub me_path: String,
    /// This admin's name.
    pub me_name: String,
    /// This admin's exact birth.
    pub birth: rdm_core::framing::Birth,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// The Build store.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// How sections reach the admins that run them.
    pub dispatcher: Arc<dyn Dispatcher>,
    /// The gates.
    pub gate: Arc<dyn Gate>,
    /// The Build-level events and the application's notices.
    pub notices: BuildNotices,
    /// This admin's stop gate.
    pub turns: crate::stop_gate::StopGate,
}

impl DriveEnv {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName {
        rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() }
    }
}

/// The drives of this admin, one per Build.
#[derive(Default)]
pub struct Drives {
    drives: Mutex<HashMap<BuildId, Arc<Drive>>>,
    tasks: Mutex<(HashMap<BuildId, tokio::task::JoinHandle<()>>, bool)>,
    ended: tokio::sync::watch::Sender<u64>,
}

impl Drives {
    /// A receiver that changes each time a Build's drive ends at its terminal.
    pub fn subscribe_ended(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ended.subscribe()
    }

    /// The drive of `build_id`, its task started when it runs none. A drive whose Build completed is
    /// kept: its frames answer a re-submit.
    pub fn ensure(self: &Arc<Self>, env: &Arc<DriveEnv>, build_id: &BuildId) -> Arc<Drive> {
        let drive = {
            let mut drives = self.drives.lock().unwrap();
            match drives.get(build_id) {
                Some(d) if d.completed() => return d.clone(),
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
            // Each pass of the drive is its own root, joined by build_id (telemetry.md §4), never a
            // child of the event or submit that ensured it.
            let task = tokio::spawn(
                async move {
                    let end = loop {
                        d.again.store(false, Ordering::SeqCst);
                        let end = env.run(&d).await;
                        // An ensure that found this run in progress (an event's pass, a submit) is run
                        // now: the run may have read the world before that event and ended waiting on it.
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
                    if end == DriveEnd::Terminal {
                        drives.ended.send_modify(|v| *v += 1);
                    }
                    if matches!(end, DriveEnd::Terminal | DriveEnd::RoleLost(_)) && !d.completed() {
                        let mut held = drives.drives.lock().unwrap();
                        if held.get(&d.build_id).is_some_and(|h| Arc::ptr_eq(h, &d)) {
                            held.remove(&d.build_id);
                        }
                    }
                }
                .instrument(tracing::Span::none())
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

    /// This admin drives no Build from now on: every drive's task is ended and none starts.
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

    /// The drive a re-submit of an ended Build is answered from.
    pub fn replay_of(self: &Arc<Self>, env: &Arc<DriveEnv>, build_id: &BuildId) -> Arc<Drive> {
        self.ensure(env, build_id)
    }

    /// Forget `build_id`'s drive.
    pub fn forget(&self, build_id: &BuildId) {
        self.drives.lock().unwrap().remove(build_id);
    }

    /// The Builds whose drive has not ended.
    pub fn unended(&self) -> Vec<BuildId> {
        self.drives.lock().unwrap().iter().filter(|(_, d)| !d.ended()).map(|(id, _)| id.clone()).collect()
    }

    /// `build_id`'s drive, when one is held.
    pub fn get(&self, build_id: &BuildId) -> Option<Arc<Drive>> {
        self.drives.lock().unwrap().get(build_id).cloned()
    }

    /// The callers reading `build_id`'s drive.
    pub fn callers_of(&self, build_id: &BuildId) -> Vec<iroh::PublicKey> {
        self.get(build_id).map(|d| d.callers()).unwrap_or_default()
    }

    /// Resolves once `build_id`'s drive ended and no caller reads it.
    pub async fn until_read(&self, build_id: &BuildId) {
        if let Some(d) = self.get(build_id) {
            d.until_read().await;
        }
    }
}

/// What is left of `p` in view `t`: the plan from the Build's topology, the meshes the records hold
/// Dead, a fabric.delete's Mesh rows and this Build's own creates, less each operation whose `Complete`
/// step's event ended `complete`.
fn left(drive: &Drive, p: &BuildProjection, t: &Topology) -> Vec<BuildOperation> {
    // A mesh the Mesh records hold Dead is planned as Dead, whatever the view shows ([`Gate::deleted_meshes`]).
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
    // A fabric.delete plans every mesh the store holds a row of beside the view's ([`Gate::mesh_rows`]).
    let stored = drive.mesh_rows.lock().unwrap().as_ref().map(|rows| crate::accepted::with_mesh_rows(t, rows));
    let t = stored.as_ref().unwrap_or(t);
    // And the births of the node.deletes it began that the view no longer holds ([`Gate::node_rows`]).
    let owed = drive.node_rows.lock().unwrap().clone();
    let with_owed = (!owed.is_empty()).then(|| crate::accepted::with_node_rows(t, &owed));
    let t = with_owed.as_ref().unwrap_or(t);
    let created = drive.created.lock().unwrap().clone();
    let plan = crate::accepted::plan_for_build_created(p, t, &created);
    plan.operations.into_iter().filter(|op| !p.operation_completed(&op.key())).collect()
}

/// The nodes whose node.delete `p` began (a step event of `node.delete:<node_id>`) and has not completed.
fn begun_deletes(p: &BuildProjection) -> Vec<rdm_core::NodeId> {
    let mut ids: Vec<rdm_core::NodeId> = Vec::new();
    for id in p.steps.iter().filter_map(|s| s.subject.as_ref()) {
        let op = rdm_core::OperationId::node("node.delete", id);
        if !ids.contains(id) && p.steps.iter().any(|s| s.operation == op.to_string()) && !p.operation_completed(&op) {
            ids.push(id.clone());
        }
    }
    ids
}

/// Whether `p`'s Build changes the fabric to ready (states.md "Resize and executable
/// replacement": "Its Build is Complete only when the fabric is ready again from this Build's
/// own change"; Q154): every Build but one that leaves no fabric (fabric.delete, and the
/// fabric-primary's own delete after it), after which the fabric is never ready again.
fn changes_the_fabric(p: &BuildProjection) -> bool {
    !p.leaves_no_fabric()
}

/// How one section of a pass ended, as the drive reads it.
enum Section {
    /// It ran what it was sent: the drive plans again.
    Ran,
    /// It ended short of its end (Q346): a step failed, or its call ended without the section's
    /// terminal. Its mesh is dispatched nothing more in this run.
    Unfinished(UnfinishedSection),
    /// The fabric-primary role moved off this admin.
    RoleYielded,
}

/// The section of `mesh` that ended short at `operation` because its call to the mesh's primary
/// (`build.attempt.run`) ended without the section's terminal.
fn call_ended_short(mesh: &str, operation: &str, reason: String, code: rdm_core::outcome::Code) -> UnfinishedSection {
    UnfinishedSection { mesh: mesh.to_string(), operation: operation.to_string(), step: "build.attempt.run".into(), reason: crate::build_run::bounded_reason(reason), code }
}

impl DriveEnv {
    /// The path name this admin holds now.
    pub async fn held_name(&self) -> PathName {
        if let Some(held) = self.builds.held_name() {
            return held;
        }
        self.topology.read().await.held_name_of(&self.me(), Some(&self.birth.node_id))
    }

    /// Drive `drive`'s Build until it ends, stalls or the role moves.
    pub async fn run(self: &Arc<Self>, drive: &Arc<Drive>) -> DriveEnd {
        let id = drive.build_id.clone();
        let me = self.held_name().await;
        let span = tracing::info_span!("rdm.node_admin.build.update.via-drive", build_id = %id.log(), node_path = %me.path, node_name = %me.name, outcome = tracing::field::Empty, detail = tracing::field::Empty, dispatches = tracing::field::Empty);
        let mut dispatches = 0u32;
        let mut last_turn = None;
        let end = self.pass(drive, &mut dispatches, &mut last_turn).instrument(span.clone()).await;
        span.record("dispatches", dispatches);
        match &end {
            DriveEnd::Terminal => span.record("outcome", "terminal"),
            DriveEnd::Stalled(w) => span.record("outcome", "stalled").record("detail", w.as_str()),
            DriveEnd::RoleLost(w) => span.record("outcome", "role-lost").record("detail", w.as_str()),
        };
        drop(span);
        drop(last_turn);
        end
    }

    async fn may_act(&self, drive: &Arc<Drive>) -> Option<DriveEnd> {
        let me = self.held_name().await;
        let recorded_primary = match self.gate.target().await {
            Ok(primary) => primary,
            Err(e) => {
                let why = e.to_string();
                let to = match e {
                    BuildStateError::RoleYielded { to, .. } => to.map(|birth| birth.node_id),
                    _ => None,
                };
                tracing::info_span!("rdm.node_admin.build.reject.via-fabric-primary-changed", build_id = %drive.build_id.log(), node_path = %me.path, node_name = %me.name, role_yielded = true, detail = %why, to = to.as_ref().map(|id| id.as_str()).unwrap_or("none")).in_scope(|| tracing::info!("dispatch refused: the primary record names another primary"));
                drive.lose_fabric_primary(RoleLoss { why: why.clone(), to });
                return Some(DriveEnd::RoleLost(why));
            }
        };
        // The role is this admin's when the view's holder is the birth it was born as (its node id). A
        // fabric-primary whose own node.delete removed it from its own view, the one Build left after
        // fabric.deleted, still holds it while its primary record names it: nobody is left in the fabric
        // to take it, and the delete's node.deleted is its last reply (fabric-delete.md step 4).
        let (held, role) = {
            let t = self.topology.read().await;
            let deleted_itself = t.own(&self.birth.node_id).is_none() && t.fabric_primary().is_none() && recorded_primary.as_ref() == Some(&self.birth.node_id);
            (t.is_fabric_primary(&self.birth.node_id) || deleted_itself, t.fabric_primary().map(|n| (n.location(), n.node_id.clone())))
        };
        if !held {
            let shows = role.as_ref().map(|(name, id)| format!("path={} name={} node_id={id}", name.path, name.name)).unwrap_or_else(|| "none".into());
            let names = recorded_primary.as_ref().map(|id| id.as_str()).unwrap_or("none");
            let why = format!("node_id={} does not hold the fabric-primary role in its view (it shows {shows}); the fabric-primary record it holds names {names}", self.birth.node_id);
            tracing::info_span!("rdm.node_admin.build.reject.via-not-fabric-primary", build_id = %drive.build_id.log(), node_path = %me.path, node_name = %me.name, detail = %why, to = names).in_scope(|| tracing::info!("dispatch refused: the role is not held"));
            drive.lose_fabric_primary(RoleLoss { why: why.clone(), to: recorded_primary });
            return Some(DriveEnd::RoleLost(why));
        }
        None
    }

    fn stalled(&self, me: &PathName, drive: &Arc<Drive>, why: String) -> DriveEnd {
        if drive.stall_is_new(StallKey::Reason(why.clone())) {
            tracing::info_span!("rdm.node_admin.build.update.via-stalled", build_id = %drive.build_id.log(), node_path = %me.path, node_name = %me.name, reason = %why).in_scope(|| tracing::info!("the drive cannot go on until the world changes"));
        }
        DriveEnd::Stalled(why)
    }

    /// The peer mesh `op` acts on, held unheard on the backbone (R-D1), with how long.
    fn unheard_peer(&self, op: &BuildOperation) -> Option<(PathName, u64)> {
        let node = op.node()?;
        if op.mesh()? == self.me_mesh {
            return None;
        }
        Some((node.clone(), self.gate.mesh_unheard_ms(op.mesh()?)?))
    }

    /// The Build ends `terminal`: its `rdm.build.ended` and `on_build_ended`, then every reader.
    async fn end(&self, me: &PathName, drive: &Arc<Drive>, terminal: BuildReply, span: &tracing::Span) -> DriveEnd {
        if let Err(cause) = self.notices.ended(me, &drive.build_id, ended_state(&terminal), span).await {
            return self.stalled(me, drive, format!("path {me_path:?}, name {me_name:?}: the rdm.build.ended event of Build {} could not be written: {cause}", drive.build_id, me_path = &me.path, me_name = &me.name));
        }
        match &terminal {
            BuildReply::Complete { .. } => self.gate.build_completed(&drive.build_id).await,
            _ => self.gate.build_failed(&drive.build_id).await,
        }
        drive.finish(terminal);
        DriveEnd::Terminal
    }

    /// Pass on, once, a `Step` frame for each step whose event ended `complete`: a drive that holds no
    /// frame of the Build (a fabric-primary that took the role mid-Build, or after it ended).
    fn replay(&self, drive: &Drive, p: &BuildProjection) {
        if drive.replayed.swap(true, Ordering::SeqCst) {
            return;
        }
        for s in p.steps.iter().filter(|s| s.outcome == crate::build_state::StepOutcome::Complete) {
            drive.push(BuildReply::Step { build_id: p.build_id.to_string(), mesh: s.mesh.clone(), operation: s.operation.clone(), step: s.step.clone(), result: StepResult::Complete });
        }
    }

    async fn pass(self: &Arc<Self>, drive: &Arc<Drive>, dispatches: &mut u32, last_turn: &mut Option<crate::stop_gate::Turn>) -> DriveEnd {
        let id = drive.build_id.clone();
        // Whether the turn held now is the one whose sections just ran: it reads how they left the Build
        // and records the Build's end, and dispatches nothing more (stop_gate.rs: a pass in progress
        // when a stop holds the gate runs to its end, its sections' terminals handled).
        let mut after_sections = false;
        loop {
            // Each pass is one turn of the admin's gate (stop_gate.rs), and a dispatch begins a new one.
            let carried = std::mem::take(&mut after_sections) && last_turn.is_some();
            if !carried {
                let _ = last_turn.take();
                last_turn.insert(self.turns.turn("build-drive").await.of_build(&id.to_string()));
            }
            let turn = last_turn.as_ref().expect("the pass holds a turn");
            if let Some(end) = self.may_act(drive).await {
                return end;
            }
            let me = self.held_name().await;
            let span = tracing::Span::current();
            let p = match self.builds.read_build(&id).await {
                Ok(p) => p,
                Err(e) => return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: Build {id} could not be read: {e}", me_path = &me.path, me_name = &me.name)),
            };
            self.replay(drive, &p);
            match p.state {
                BuildState::Complete => {
                    drive.finish(BuildReply::Complete { build_id: id.to_string() });
                    return DriveEnd::Terminal;
                }
                BuildState::Failed if !matches!(crate::build_state::drivable(&*self.builds, &p).await, Ok(true)) => {
                    let f = p.failure.clone().unwrap_or(crate::build_state::BuildFailure { reason: String::new(), code: "UNKNOWN".into() });
                    drive.finish(rdm_core::reply::build::failed(id.to_string(), crate::build_run::bounded_reason(f.reason), crate::build_state::code_of_name(&f.code)));
                    return DriveEnd::Terminal;
                }
                BuildState::Incomplete if !matches!(crate::build_state::drivable(&*self.builds, &p).await, Ok(true)) => {
                    drive.finish(rdm_core::reply::build::incomplete(id.to_string(), p.unfinished.clone()));
                    return DriveEnd::Terminal;
                }
                _ => {}
            }
            match crate::build_state::drivable(&*self.builds, &p).await {
                Ok(true) => {}
                Ok(false) => {
                    // A fabric.delete accepted after this Build interrupts it (build.md §1): it ends
                    // `incomplete`, each section it did not finish naming the delete, and nothing of it
                    // runs again.
                    if let Ok(Some(delete)) = crate::build_state::interrupted_by(&*self.builds, &p).await {
                        let reason = crate::build_state::interrupted_reason(&id, &delete.build_id);
                        let t = self.topology.read().await.clone();
                        let mut sections: std::collections::BTreeMap<String, UnfinishedSection> = Default::default();
                        for op in left(drive, &p, &t) {
                            let mesh = op.mesh().unwrap_or(&self.me_mesh).to_string();
                            sections.entry(mesh.clone()).or_insert_with(|| UnfinishedSection { mesh, operation: op.key().to_string(), step: "fabric.delete".into(), reason: crate::build_run::bounded_reason(reason.clone()), code: rdm_core::outcome::Code::Aborted });
                        }
                        // Every operation ran, and the fabric change it waits on never completes: the
                        // fabric is never ready again under the delete (Q154).
                        if sections.is_empty() && changes_the_fabric(&p) {
                            sections.insert(self.me_mesh.clone(), UnfinishedSection { mesh: self.me_mesh.clone(), operation: "fabric.change".into(), step: "fabric.delete".into(), reason: crate::build_run::bounded_reason(reason.clone()), code: rdm_core::outcome::Code::Aborted });
                        }
                        let terminal = if p.state == BuildState::Pending { rdm_core::reply::build::failed(id.to_string(), crate::build_run::bounded_reason(reason), rdm_core::outcome::Code::Aborted) } else { rdm_core::reply::build::incomplete(id.to_string(), sections.into_values().collect()) };
                        return self.end(&me, drive, terminal, &span).await;
                    }
                    let current = self.builds.current_build().await.ok().and_then(|c| c.into_build_id()).map(|c| c.to_string()).unwrap_or_else(|| "none".into());
                    return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: Build {id} ({:?}) is neither the current Build ({current}) nor a failed Build accepted after it: nothing drives it", p.state, me_path = &me.path, me_name = &me.name));
                }
                Err(e) => return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: the current Build could not be read to admit Build {id}: {e}", me_path = &me.path, me_name = &me.name)),
            }
            // A Build not yet started, or a failed one a caller resubmitted, starts: `rdm.build.started`
            // and the application's on_build_started from that write (build.md §4.1).
            if matches!(p.state, BuildState::Pending | BuildState::Failed | BuildState::Incomplete) {
                if let Err(cause) = self.notices.started(&me, &id, &crate::build::current_context()).await {
                    return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: the rdm.build.started event of Build {id} could not be written: {cause}", me_path = &me.path, me_name = &me.name));
                }
                self.gate.build_started(&id).await;
                // A new run: no section of it has ended yet.
                drive.unfinished.lock().unwrap().clear();
                continue;
            }
            // A fabric.delete's teardown runs once every Build it interrupts has ended, each by its own
            // drive once its steps in flight have ended (build.md §1).
            if p.deletes_fabric() {
                match crate::build_state::interrupted_open(&*self.builds, &p).await {
                    Ok(open) if open.is_empty() => {}
                    Ok(open) => {
                        let waits: Vec<String> = open.iter().map(ToString::to_string).collect();
                        return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: fabric.delete {id} interrupts Build {}, which has not ended: the teardown runs once its steps in flight have ended and it has ended incomplete", waits.join(", "), me_path = &me.path, me_name = &me.name));
                    }
                    Err(e) => return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: the Builds fabric.delete {id} interrupts could not be read: {e}", me_path = &me.path, me_name = &me.name)),
                }
            }
            // A mesh the Mesh records hold Dead is done whatever the view shows: read at every pass.
            let observed = self.topology.read().await.clone();
            match self.gate.deleted_meshes(&observed).await {
                Ok(dead) => *drive.dead.lock().unwrap() = dead,
                Err(e) => return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: Build {id}: the Mesh records could not be read for which meshes are Dead: {e}", me_path = &me.path, me_name = &me.name)),
            }
            // A fabric.delete's remaining meshes are the Mesh rows on the store: read at every pass.
            if p.deletes_fabric() {
                match self.gate.mesh_rows().await {
                    Ok(rows) => *drive.mesh_rows.lock().unwrap() = Some(rows),
                    Err(e) => return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: Build {id} is a fabric.delete and the Mesh rows could not be read for the meshes it deletes: {e}", me_path = &me.path, me_name = &me.name)),
                }
                // A node.delete it began and has not completed acts on a birth the view may have let go:
                // the birth is its node record's (fabric-delete.md "Failure"; D67).
                let owed: Vec<rdm_core::NodeId> = begun_deletes(&p).into_iter().filter(|id| !observed.nodes.iter().any(|n| &n.node_id == id)).collect();
                let rows = if owed.is_empty() { Ok(Vec::new()) } else { self.gate.node_rows(&owed).await };
                match rows {
                    Ok(rows) => *drive.node_rows.lock().unwrap() = rows,
                    Err(e) => return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: Build {id} is a fabric.delete and the node records of the node.deletes it began could not be read: {e}", me_path = &me.path, me_name = &me.name)),
                }
            }
            // The creates this Build's own ops made, as the atlas holds them: never surplus.
            let created = self.gate.atlas_created(&p).await;
            *drive.created.lock().unwrap() = created;
            let t = self.topology.read().await.clone();
            // A mesh whose section ended short in this run is dispatched nothing more (Q344, Q346).
            let unfinished = drive.unfinished.lock().unwrap().clone();
            let ops: Vec<BuildOperation> = left(drive, &p, &t).into_iter().filter(|op| !unfinished.contains_key(op.mesh().unwrap_or(&self.me_mesh))).collect();
            if ops.is_empty() && !unfinished.is_empty() {
                // It ran and not every section finished: what ran stands (Q346).
                return self.end(&me, drive, rdm_core::reply::build::incomplete(id.to_string(), unfinished.into_values().collect()), &span).await;
            }
            if ops.is_empty() {
                // A Build that changes the fabric is Complete only once the fabric's ready state is set
                // from its change (Q154; states.md "Resize and a new executable").
                if changes_the_fabric(&p) && !self.gate.fabric_ready() {
                    let why = format!("path {me_path:?}, name {me_name:?}: every operation of Build {id} is done, and the Build is Complete only once the fabric's ready state is set", me_path = &me.path, me_name = &me.name);
                    return self.stalled(&me, drive, why);
                }
                return self.end(&me, drive, BuildReply::Complete { build_id: id.to_string() }, &span).await;
            }
            // More to run after the sections this turn ran: it runs in a turn of its own.
            if carried {
                continue;
            }
            // A role the first operation moves moves first, a step of the Build (node.primary.update).
            match self.update_primaries_first(&me, drive, &ops, &t, &span).await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(end) => return end,
            }
            if let Some(end) = self.updates_acknowledged(&me, drive, &ops).await {
                return end;
            }
            // The sections: this admin's own (the leading operations it executes or no admin does yet),
            // and every other mesh's, by the mesh's primary.
            let mut own: Vec<BuildOperation> = Vec::new();
            let mut remote: std::collections::BTreeMap<String, String> = Default::default();
            let mut held: Vec<String> = Vec::new();
            let mut own_open = true;
            for op in &ops {
                if let Some((node, unheard_ms)) = self.unheard_peer(op).filter(|_| !p.topology.meshes.is_empty()) {
                    let mesh = op.mesh().unwrap_or_default().to_string();
                    if drive.stall_is_new(StallKey::UnheardMesh { mesh: mesh.clone(), operation: op.key() }) {
                        tracing::info_span!("rdm.node_admin.build.reject.via-unheard-mesh-drive", build_id = %id.log(), node_path = %me.path, node_name = %me.name, operation = %op.key(), target_path = %node.path, target_name = %node.name, mesh = %mesh, unheard_ms)
                            .in_scope(|| tracing::info!("the drive dispatches nothing for a node of a peer mesh held unheard"));
                    }
                    held.push(format!("operation {} acts on path {:?}, name {:?} of peer mesh {mesh}, unheard on the backbone for {unheard_ms} ms: nothing is dispatched for that mesh until it is heard again (R-D1)", op.key(), node.path, node.name));
                    own_open = false;
                    continue;
                }
                match crate::executor::executor_for(op, &t) {
                    Some(to) if to == self.birth.node_id => {
                        if own_open {
                            own.push(op.clone());
                        }
                    }
                    Some(_) => {
                        let mesh = op.mesh().unwrap_or_default().to_string();
                        remote.entry(mesh).or_insert_with(|| op.key().to_string());
                        own_open = false;
                    }
                    None if op.mesh().is_some_and(|m| m != self.me_mesh) => {
                        let mesh = op.mesh().unwrap_or_default().to_string();
                        remote.entry(mesh).or_insert_with(|| op.key().to_string());
                        own_open = false;
                    }
                    None => {
                        if own.is_empty() && remote.is_empty() {
                            held.push(crate::executor::why_no_executor(op, &t));
                        }
                        own_open = false;
                    }
                }
            }
            if own.is_empty() && remote.is_empty() {
                return self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: Build {id} dispatches nothing now: {}", held.join("; "), me_path = &me.path, me_name = &me.name));
            }
            drive.clear_stall();
            let context = crate::build::current_context();
            let mut runs: Vec<futures_util::future::BoxFuture<'_, Section>> = Vec::new();
            if !own.is_empty() {
                *dispatches += 1;
                let mesh = own.iter().find_map(|op| op.mesh().map(str::to_string)).unwrap_or_else(|| self.me_mesh.clone());
                let first = own[0].key().to_string();
                let (env, context) = (self.clone(), context.clone());
                runs.push(Box::pin(async move {
                    let d = env.dispatcher.run_own(turn, &drive.build_id, &mesh, context, own).await;
                    let runs_at = Some(env.birth.node_id.clone());
                    env.follow(drive, &mesh, &first, runs_at, d).await
                }));
            }
            for (mesh, first) in remote {
                *dispatches += 1;
                let (env, context) = (self.clone(), context.clone());
                let me = me.clone();
                let runs_at = crate::executor::mesh_primary(&mesh, &t).map(|b| b.node_id);
                runs.push(Box::pin(async move {
                    env.notices.dispatched(&me, &drive.build_id, &mesh).await;
                    let d = env.dispatcher.dispatch(&drive.build_id, &mesh, context).await;
                    env.follow(drive, &mesh, &first, runs_at, d).await
                }));
            }
            let ended = futures_util::future::join_all(runs).await;
            if ended.iter().any(|s| matches!(s, Section::RoleYielded)) {
                return match self.may_act(drive).await {
                    Some(end) => end,
                    None => self.stalled(&me, drive, format!("path {me_path:?}, name {me_name:?}: a section of Build {id} stopped at a role target that the gate no longer reports", me_path = &me.path, me_name = &me.name)),
                };
            }
            // A section that ended short ends its mesh's part of this run, never the Build (Q346): the
            // drive plans again, and the Build ends once no section is left to run.
            for s in ended {
                if let Section::Unfinished(u) = s {
                    tracing::info_span!("rdm.node_admin.build.update.via-section-unfinished", build_id = %id.log(), node_path = %me.path, node_name = %me.name, mesh = %u.mesh, operation = %u.operation, step = %u.step, reason = %u.reason, code = u.code.name())
                        .in_scope(|| tracing::info!("the mesh's section ended short of its end: nothing more of that mesh runs in this run, and the other sections run on"));
                    drive.unfinished.lock().unwrap().insert(u.mesh.clone(), u);
                }
            }
            drive.state.lock().unwrap().idle = None;
            after_sections = true;
        }
    }

    /// Pass on the frames of one dispatched section and read how it ended. `first` is the first
    /// operation the section was sent, which a call that ends short names; `runs_at` the admin that runs
    /// it. A target set while the section runs ends it here `RoleYielded`, unless the target names
    /// `runs_at`: that admin took the role during its run (a step's own node.primary.update), and it
    /// ends the section's stream with FabricPrimaryMoved naming itself; the callers are sent to it only
    /// then (node-rpc-envelope.md: a fabric-primary change ends the outer stream with
    /// FabricPrimaryMoved).
    async fn follow(&self, drive: &Arc<Drive>, mesh: &str, first: &str, runs_at: Option<rdm_core::NodeId>, d: Dispatched) -> Section {
        use rdm_core::outcome::Code;
        let mut inner = match d {
            Dispatched::Stream(s) => s,
            Dispatched::Refused(BuildReply::RoleYielded { .. }) => return Section::RoleYielded,
            Dispatched::Refused(r) => return Section::Unfinished(call_ended_short(mesh, first, format!("mesh {mesh}'s section was refused: {}", describe(&r)), <rdm_core::build::Build as rdm_core::protocol::NodeProtocol>::classify_reply(&r).code())),
            Dispatched::Unreached(why) => return Section::Unfinished(call_ended_short(mesh, first, format!("mesh {mesh}'s primary was not reached: {why}"), Code::Unavailable)),
        };
        inner.accepted().await;
        let mut last = first.to_string();
        let mut runs_at_holds = false;
        loop {
            let item = tokio::select! {
                biased;
                _ = self.gate.target_set(), if !runs_at_holds => {
                    match self.gate.target().await {
                        Err(BuildStateError::RoleYielded { to: Some(to), .. }) if Some(&to.node_id) == runs_at.as_ref() => {
                            runs_at_holds = true;
                            continue;
                        }
                        _ => return Section::RoleYielded,
                    }
                }
                item = inner.next() => item,
            };
            // Each frame is the drive's once it is pushed or read as the section's end, and it is
            // recovered then (`ReplyStream::accepted`): the mesh-primary's held frames are acked as they
            // are taken, never only the first, so a section of any length streams to its terminal.
            match item {
                InnerItem::Frame(f @ BuildReply::Step { .. }) => {
                    if let BuildReply::Step { operation, .. } = &f {
                        self.gate.step_streamed(mesh, &drive.build_id, operation).await;
                        last = operation.clone();
                    }
                    drive.push(f);
                    inner.accepted().await;
                }
                InnerItem::Frame(f @ BuildReply::Pending { .. }) => {
                    drive.push(f);
                    inner.accepted().await;
                }
                InnerItem::Frame(BuildReply::Started { .. }) => {}
                InnerItem::Frame(BuildReply::Complete { .. }) => {
                    inner.accepted().await;
                    return Section::Ran
                }
                InnerItem::Frame(BuildReply::Incomplete { sections, .. }) => {
                    inner.accepted().await;
                    let u = sections.into_iter().find(|s| s.mesh == mesh).unwrap_or_else(|| call_ended_short(mesh, &last, format!("mesh {mesh}'s section ended incomplete naming no section of {mesh}"), Code::Internal));
                    return Section::Unfinished(u);
                }
                InnerItem::Frame(BuildReply::RoleYielded { .. } | BuildReply::FabricPrimaryMoved { .. }) => {
                    inner.accepted().await;
                    return Section::RoleYielded
                }
                InnerItem::Frame(other) => {
                    inner.accepted().await;
                    return Section::Unfinished(call_ended_short(mesh, &last, format!("mesh {mesh}'s section ended with {}", describe(&other)), <rdm_core::build::Build as rdm_core::protocol::NodeProtocol>::classify_reply(&other).code()));
                },
                InnerItem::Ended => return Section::Unfinished(call_ended_short(mesh, &last, format!("mesh {mesh}'s section stream ended without its terminal"), Code::Unavailable)),
                InnerItem::Broke(why) => return Section::Unfinished(call_ended_short(mesh, &last, format!("mesh {mesh}'s primary was lost mid-section (Indeterminate): {why}"), Code::Unavailable)),
            }
        }
    }

    /// node.primary.updated is acknowledged for every update that precedes an operation left, before
    /// any is dispatched (node-primary-update.md failure contract).
    async fn updates_acknowledged(&self, me: &PathName, drive: &Arc<Drive>, ops: &[BuildOperation]) -> Option<DriveEnd> {
        let mut asked: Vec<PrecedingUpdate> = ops.iter().flat_map(preceding_updates).collect();
        asked.dedup();
        if asked.is_empty() {
            return None;
        }
        let why = self.gate.primaries_updated(&asked, &drive.build_id).await.err()?;
        let names = asked.iter().map(|u| format!("{} under mesh {}", u.operation, u.mesh)).collect::<Vec<_>>().join(", ");
        let why = format!("path {me_path:?}, name {me_name:?}: Build {} dispatches nothing until node.primary.updated is acknowledged for the update that precedes it (one of {names}): {why}", drive.build_id, me_path = &me.path, me_name = &me.name);
        Some(self.stalled(me, drive, why))
    }

    /// The roles the first operation left moves, moved first as a step of the Build
    /// (node.primary.update): `Ok(true)` when they moved and the drive plans again, `Ok(false)` when
    /// none moves.
    async fn update_primaries_first(&self, me: &PathName, drive: &Arc<Drive>, ops: &[BuildOperation], t: &Topology, span: &tracing::Span) -> Result<bool, DriveEnd> {
        let Some(op) = ops.first() else { return Ok(false) };
        let Some(m) = crate::executor::role_move(op, t) else { return Ok(false) };
        let id = &drive.build_id;
        let moved = self.gate.update_primaries(&m, id, &op.key()).instrument(span.clone()).await;
        let after = self.topology.read().await.clone();
        let still = still_holds(op, &m, &after);
        match moved {
            Err(RoleMoveEnd::Refused(why)) => {
                let reason = format!("path {me_path:?}, name {me_name:?}: the roles of node {} did not move before operation {}: node.primary.update was refused before its row: {why}", m.primary.node_id, op.key(), me_path = &me.path, me_name = &me.name);
                // The role move is a step of the operation's mesh section: its end ends that section (Q346).
                role_move_unfinished(drive, &self.me_mesh, op, "primary-update".into(), reason, rdm_core::outcome::Code::FailedPrecondition);
                Ok(true)
            }
            Err(RoleMoveEnd::Failed { step, reason, code }) => {
                role_move_unfinished(drive, &self.me_mesh, op, step, reason, code);
                Ok(true)
            }
            Ok(()) if !still => Ok(true),
            Err(RoleMoveEnd::Incomplete(_)) if !still => Ok(true),
            other => {
                let why = match other {
                    Err(why) => format!("path {me_path:?}, name {me_name:?}: the role of node {} did not move before operation {}: {why}", m.primary.node_id, op.key(), me_path = &me.path, me_name = &me.name),
                    Ok(()) => format!("path {me_path:?}, name {me_name:?}: node.primary.update of node {} answered, and this view still names it the primary operation {} moves", m.primary.node_id, op.key(), me_path = &me.path, me_name = &me.name),
                };
                Err(self.stalled(me, drive, why))
            }
        }
    }
}

/// The section of `op`'s mesh ended short at its role move (Q346): nothing more of that mesh runs in
/// this run.
fn role_move_unfinished(drive: &Drive, me_mesh: &str, op: &BuildOperation, step: String, reason: String, code: rdm_core::outcome::Code) {
    let mesh = op.mesh().unwrap_or(me_mesh).to_string();
    let u = UnfinishedSection { mesh: mesh.clone(), operation: op.key().to_string(), step, reason: crate::build_run::bounded_reason(reason), code };
    drive.unfinished.lock().unwrap().insert(mesh, u);
}

/// Whether view `t` still names the primary `m` moves before `op`.
fn still_holds(op: &BuildOperation, m: &crate::executor::RoleMove, t: &Topology) -> bool {
    crate::executor::role_move(op, t).is_some_and(|now| now.primary.node_id == m.primary.node_id)
}

/// A refusal or terminal in words, for a span or a stall.
pub fn describe(r: &BuildReply) -> String {
    match r {
        BuildReply::NotExecutor { named, recipient } => format!("not-executor: the mesh's primary is {named}, the recipient is {recipient}"),
        BuildReply::UnknownBuild { build_id } => format!("unknown-build: {build_id}"),
        BuildReply::NotReady { reason } | BuildReply::Busy { reason } | BuildReply::PeerUnresolved { reason } | BuildReply::ConnectionsDeleting { reason } | BuildReply::Unauthorized { reason } => format!("{}: {reason}", r.name()),
        BuildReply::RoleYielded { node, by } => format!("role-yielded: {node} is no longer the fabric-primary: {by}"),
        BuildReply::Failed { reason, .. } => format!("failed: {reason}"),
        BuildReply::Incomplete { sections, .. } => format!("incomplete: {}", sections.iter().map(|u| format!("mesh {} at {} step {}: {}", u.mesh, u.operation, u.step, u.reason)).collect::<Vec<_>>().join("; ")),
        other => other.name().to_string(),
    }
}
