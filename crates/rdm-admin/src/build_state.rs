//! `BuildStateAdapter`: where Builds live (build.md §5).
//!
//! A Build is one record, [`BuildAccepted`], written once by the fabric-primary. What happened while
//! executing it is the events store's ([`crate::events_storage::EventsStorage`]): the fabric-primary's
//! `rdm.build.started`, `rdm.build.dispatched` and `rdm.build.ended`, and each mesh-primary's
//! `rdm.build.step` for its own mesh's steps. The Build's view, [`BuildProjection`], is the record
//! with its events folded: its steps from the `rdm.build.step` events (a step whose event ended
//! `complete` is never run again), its state from `rdm.build.started` and `rdm.build.ended`.
//!
//! RDM adapters:
//! - [`MemoryBuildStateAdapter`]: the in-memory Build log that backs the live fabric control
//!   projection;
//! - [`FileJournal`]: the file adapter's journal on the fabric's shared store root, its `builds.jsonl`
//!   (R-BI6, R-SR1): one line per Build record, written by the fabric-primary, so a successor reads
//!   it there.
//!
//! Every adapter reaches the events store: [`BuildStateAdapter::events`].

use crate::accepted::FabricTopology;
use rdm_core::build::BuildChange;
use crate::build::BuildId;
use crate::events_storage::{EndedState, Event, EventsStorage, StepState};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[cfg(feature = "file-backend")]
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// A Build accepted by the fabric-primary: the complete topology it realizes, and the change that
/// produced it (history only; planning reads `topology`, never `change`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildAccepted {
    /// The Build's id.
    pub build_id: BuildId,
    /// The complete topology the Build realizes.
    pub topology: FabricTopology,
    /// The change as it came in (build.md §1): history only. Every Build is a Create, a Delete or a
    /// DeleteFabric (Q342); Day 0's is the Create of its mesh and its one node-admin.
    pub change: BuildChange,
    /// Who wrote the record: the accepting fabric-primary's exact birth,
    /// `rdm://<fabric_id>/<node_id>/<incarnation>` (gossip.md §3.4's `source`). A record written before
    /// the field names none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// When the Build was accepted, in milliseconds since the Unix epoch.
    pub time: u64,
}

/// How a step ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepOutcome {
    /// The step completed.
    Complete,
    /// The step failed.
    Failed {
        /// Why it failed.
        reason: String,
        /// The standard code captured where the failure occurred.
        #[serde(default = "rdm_core::outcome::legacy_failed_code")]
        code: rdm_core::outcome::Code,
    },
}

impl StepOutcome {
    /// A Failed outcome.
    pub fn failed(reason: impl Into<String>) -> Self {
        Self::Failed { reason: reason.into(), code: rdm_core::outcome::Code::FailedPrecondition }
    }

    /// The event's `state` this outcome is.
    pub fn state(&self) -> StepState {
        match self {
            Self::Complete => StepState::Complete,
            Self::Failed { reason, code } => StepState::Failed { reason: reason.clone(), code: code.name().to_string() },
        }
    }

    /// The outcome an event's `state` names.
    pub fn of_state(state: &StepState) -> Self {
        match state {
            StepState::Complete => Self::Complete,
            StepState::Failed { reason, code } => Self::Failed { reason: reason.clone(), code: code_of_name(code) },
        }
    }
}

/// The standard code an event's `code` names (its gRPC name, as `Code::name` writes it). RDM never
/// writes a permission code on an event; a name it does not write reads as `UNKNOWN`.
pub fn code_of_name(name: &str) -> rdm_core::outcome::Code {
    use rdm_core::outcome::Code;
    match name {
        "OK" => Code::Ok,
        "CANCELLED" => Code::Cancelled,
        "INVALID_ARGUMENT" => Code::InvalidArgument,
        "DEADLINE_EXCEEDED" => Code::DeadlineExceeded,
        "NOT_FOUND" => Code::NotFound,
        "ALREADY_EXISTS" => Code::AlreadyExists,
        "RESOURCE_EXHAUSTED" => Code::ResourceExhausted,
        "FAILED_PRECONDITION" => Code::FailedPrecondition,
        "ABORTED" => Code::Aborted,
        "OUT_OF_RANGE" => Code::OutOfRange,
        "UNIMPLEMENTED" => Code::Unimplemented,
        "INTERNAL" => Code::Internal,
        "UNAVAILABLE" => Code::Unavailable,
        "DATA_LOSS" => Code::DataLoss,
        _ => Code::Unknown,
    }
}

/// One step of a Build, as its `rdm.build.step` event says it (build.md §5): the mesh whose section it
/// belongs to, its operation (`noun.verb:<node_id>`), the step, and how it ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildStep {
    /// The Build.
    pub build_id: BuildId,
    /// The mesh whose section the step belongs to.
    pub mesh: String,
    /// The step's operation, `noun.verb:<node_id>` (the event's `op`).
    pub operation: String,
    /// The step.
    pub step: String,
    /// How the step ended.
    pub outcome: StepOutcome,
    /// The node the step acted on (the event's `subject`), when it acted on one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<rdm_core::NodeId>,
    /// The writer's exact birth (the event's `source`).
    #[serde(default)]
    pub source: String,
}

impl BuildStep {
    /// The step of `event` when it is an `rdm.build.step`.
    pub fn of_event(event: &Event) -> Option<Self> {
        if event.r#type != crate::events_storage::BUILD_STEP {
            return None;
        }
        let field = |k: &str| event.data.get(k).and_then(|v| v.as_str()).map(str::to_string);
        let state: StepState = serde_json::from_value(event.data.get("state")?.clone()).ok()?;
        Some(Self {
            build_id: BuildId::parse(&field("build_id")?).ok()?,
            mesh: field("mesh")?,
            operation: field("op")?,
            step: field("step")?,
            outcome: StepOutcome::of_state(&state),
            subject: event.subject.clone(),
            source: event.source.clone(),
        })
    }
}

/// A step's outcome as the run that ran it reports it: what its `rdm.build.step` event says (build.md
/// §5). The event is written by the run's step log ([`BuildStateAdapter::write_step`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepWrite {
    /// The Build.
    pub build_id: BuildId,
    /// The mesh whose section the step belongs to; `None` the writer's own mesh.
    pub mesh: Option<String>,
    /// The step's operation.
    pub operation: rdm_core::OperationId,
    /// The step.
    pub step: String,
    /// How the step ended.
    pub outcome: StepOutcome,
}

/// One appended record of the Build log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "fact", rename_all = "snake_case")]
pub enum BuildFact {
    /// The Build was accepted.
    Accepted(BuildAccepted),
    /// Build-history administration (`build.delete`): a finished Build leaves the views. Never a
    /// topology mutation.
    Forget {
        /// The Build forgotten.
        build_id: BuildId,
    },
}

impl BuildFact {
    /// The Build the fact belongs to.
    pub fn build_id(&self) -> &BuildId {
        match self {
            Self::Accepted(f) => &f.build_id,
            Self::Forget { build_id } => build_id,
        }
    }
}

/// Where a Build is in its life, as its events say it (`docs/architecture/build.md` §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    /// Accepted, and no `rdm.build.started` written yet.
    ///
    /// Serialized as `pending`.
    Pending,
    /// `rdm.build.started` written, and no `rdm.build.ended` after it.
    ///
    /// Serialized as `running`.
    Running,
    /// `rdm.build.ended` `complete`.
    ///
    /// Serialized as `complete`.
    Complete,
    /// `rdm.build.ended` `failed`: the Build could not start (Q346).
    ///
    /// Serialized as `failed`.
    Failed,
    /// `rdm.build.ended` `incomplete`: it ran and not every section finished (Q346).
    ///
    /// Serialized as `incomplete`.
    Incomplete,
}

impl BuildState {
    /// Whether the Build ended short of `complete`, `incomplete` or `failed`: skipped by the current
    /// Build's read and resubmittable by its build_id (build.md §4).
    pub fn ended_short(self) -> bool {
        matches!(self, Self::Failed | Self::Incomplete)
    }
}

/// Why a failed Build could not start, as its `rdm.build.ended` says it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildFailure {
    /// Why.
    pub reason: String,
    /// The gRPC code name of the failure.
    pub code: String,
}

/// The Build's view (`docs/architecture/build.md` §5): its record and its events folded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildProjection {
    /// The Build.
    pub build_id: BuildId,
    /// The complete topology the Build realizes.
    pub topology: FabricTopology,
    /// The change as it came in (build.md §1): history only.
    pub change: BuildChange,
    /// When the Build was accepted, in milliseconds since the Unix epoch.
    pub time: u64,
    /// Where the Build is in its life.
    pub state: BuildState,
    /// The Build's steps, one per `rdm.build.step` event, in event.id order.
    pub steps: Vec<BuildStep>,
    /// The meshes the fabric-primary dispatched a section to (`rdm.build.dispatched`).
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub dispatched: std::collections::BTreeSet<String>,
    /// Why the Build could not start, when its last `rdm.build.ended` is `failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<BuildFailure>,
    /// Each section it did not finish, when its last `rdm.build.ended` is `incomplete`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unfinished: Vec<rdm_core::build::UnfinishedSection>,
}

impl BuildProjection {
    /// The Build `accepted` with `events` folded: its events in event.id order. Only the events of this
    /// Build are read. A run is the events since the last `rdm.build.started`: a resubmission of a
    /// failed Build writes `rdm.build.started` again and its steps whose event ended `complete` stand.
    pub fn of(accepted: &BuildAccepted, events: &[Event]) -> Self {
        let mut p = Self {
            build_id: accepted.build_id.clone(),
            topology: accepted.topology.clone(),
            change: accepted.change.clone(),
            time: accepted.time,
            state: BuildState::Pending,
            steps: Vec::new(),
            dispatched: Default::default(),
            failure: None,
            unfinished: Vec::new(),
        };
        for e in events.iter().filter(|e| e.build_id().as_ref() == Some(&accepted.build_id)) {
            match e.r#type.as_str() {
                crate::events_storage::BUILD_STARTED => {
                    p.state = BuildState::Running;
                    p.failure = None;
                    p.unfinished.clear();
                }
                crate::events_storage::BUILD_DISPATCHED => {
                    if let Some(mesh) = e.data.get("mesh").and_then(|m| m.as_str()) {
                        p.dispatched.insert(mesh.to_string());
                    }
                }
                crate::events_storage::BUILD_STEP => p.steps.extend(BuildStep::of_event(e)),
                crate::events_storage::BUILD_ENDED => match e.data.get("state").cloned().map(serde_json::from_value::<EndedState>) {
                    Some(Ok(EndedState::Complete)) => p.state = BuildState::Complete,
                    Some(Ok(EndedState::Failed { reason, code })) => {
                        p.state = BuildState::Failed;
                        p.failure = Some(BuildFailure { reason, code });
                    }
                    Some(Ok(EndedState::Incomplete { sections })) => {
                        p.state = BuildState::Incomplete;
                        p.unfinished = sections.into_iter().map(|s| rdm_core::build::UnfinishedSection { mesh: s.mesh, operation: s.op, step: s.step, reason: s.reason, code: code_of_name(&s.code) }).collect();
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        p
    }

    /// Whether the change that produced this Build is fabric.delete.
    pub fn deletes_fabric(&self) -> bool {
        matches!(self.change, BuildChange::DeleteFabric)
    }

    /// Whether this Build leaves the fabric no mesh: a fabric.delete, or the fabric-primary's own delete
    /// after fabric.deleted (fabric-delete.md step 4). The fabric is never made ready again from it.
    pub fn leaves_no_fabric(&self) -> bool {
        self.deletes_fabric() || self.topology.meshes.is_empty()
    }

    /// The latest event of `operation`'s `step`, when one is.
    pub fn step_of(&self, operation: &str, step: &str) -> Option<&BuildStep> {
        self.steps.iter().rev().find(|s| s.operation == operation && s.step == step)
    }

    /// Whether `operation`'s `step` ended `complete`: never run again (build.md §1).
    pub fn completed(&self, operation: &str, step: &str) -> bool {
        self.steps.iter().any(|s| s.operation == operation && s.step == step && s.outcome == StepOutcome::Complete)
    }

    /// Whether `operation` is done: its `Complete` step ended `complete`.
    pub fn operation_completed(&self, operation: &rdm_core::OperationId) -> bool {
        self.completed(&operation.to_string(), "Complete")
    }

    /// The steps of `operation` whose event ended `complete`, by step name.
    pub fn completed_steps(&self, operation: &str) -> std::collections::BTreeSet<String> {
        self.steps.iter().filter(|s| s.operation == operation && s.outcome == StepOutcome::Complete).map(|s| s.step.clone()).collect()
    }

    /// The steps of `mesh`'s section, in event.id order.
    pub fn steps_in<'a>(&'a self, mesh: &'a str) -> impl DoubleEndedIterator<Item = &'a BuildStep> + 'a {
        self.steps.iter().filter(move |s| s.mesh == mesh)
    }
}

/// The current Build (R-BI3, rulings 2026-10-12): the Builds read last in, first out through the
/// acceptance-order index, the first that is in progress or completed by its events. A failed Build is
/// skipped and never current; nothing resumes it on its own, and a caller resubmits it by its build_id.
/// No row points at it: the Fabric row carries no pointer to the newest Build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentBuild {
    /// The newest Build not failed is in progress: it is resumed.
    InProgress(BuildId),
    /// The newest Build not failed completed: it is the known state.
    Completed(BuildId),
    /// No Build is in progress or completed.
    None,
}

impl CurrentBuild {
    /// The current Build's id, when one is.
    pub fn build_id(&self) -> Option<&BuildId> {
        match self {
            Self::InProgress(id) | Self::Completed(id) => Some(id),
            Self::None => None,
        }
    }

    /// The current Build's id, when one is.
    pub fn into_build_id(self) -> Option<BuildId> {
        match self {
            Self::InProgress(id) | Self::Completed(id) => Some(id),
            Self::None => None,
        }
    }
}

/// What `p` is to the LIFO read: in progress, completed, or skipped (failed).
pub(crate) fn current_of(p: &BuildProjection) -> Option<CurrentBuild> {
    match p.state {
        BuildState::Pending | BuildState::Running => Some(CurrentBuild::InProgress(p.build_id.clone())),
        BuildState::Complete => Some(CurrentBuild::Completed(p.build_id.clone())),
        BuildState::Failed | BuildState::Incomplete => None,
    }
}

/// The current Build of `builds`, read newest first (build.md §4): the first Build in progress or
/// completed by its events.
pub async fn current_of_builds<S: BuildStateAdapter + ?Sized>(builds: &S) -> Result<CurrentBuild, BuildStateError> {
    for id in builds.accepted_newest_first().await? {
        let p = match builds.read_build(&id).await {
            Ok(p) => p,
            Err(BuildStateError::UnknownBuild(_)) => continue,
            Err(e) => return Err(e),
        };
        if let Some(c) = current_of(&p) {
            return Ok(c);
        }
    }
    Ok(CurrentBuild::None)
}

/// Whether the fabric-primary drives `p` now: it is the current Build, or it failed and a caller's
/// resubmission runs it again while no Build is in progress and it is newer than the completed one.
pub async fn drivable(builds: &dyn BuildStateAdapter, p: &BuildProjection) -> Result<bool, BuildStateError> {
    let current = builds.current_build().await?;
    if current.build_id() == Some(&p.build_id) {
        return Ok(true);
    }
    if !p.state.ended_short() {
        return Ok(false);
    }
    Ok(match current {
        CurrentBuild::InProgress(_) => false,
        CurrentBuild::None => true,
        CurrentBuild::Completed(id) => {
            let c = builds.read_build(&id).await?;
            (p.time, &p.build_id) > (c.time, &c.build_id)
        }
    })
}

/// The fabric.delete accepted after `p` that interrupts it (build.md §1), when one is.
pub async fn interrupted_by(builds: &dyn BuildStateAdapter, p: &BuildProjection) -> Result<Option<BuildProjection>, BuildStateError> {
    let Some(id) = builds.current_build().await?.into_build_id() else { return Ok(None) };
    if id == p.build_id {
        return Ok(None);
    }
    let current = builds.read_build(&id).await?;
    Ok((current.deletes_fabric() && (current.time, &current.build_id) > (p.time, &p.build_id)).then_some(current))
}

/// The Builds the fabric.delete `delete` interrupts that have not ended (build.md §1): each accepted
/// before it and neither complete, incomplete nor failed, newest first. A Build that completed is the
/// last one any older Build ran before, so the read stops there.
pub async fn interrupted_open(builds: &dyn BuildStateAdapter, delete: &BuildProjection) -> Result<Vec<BuildId>, BuildStateError> {
    let mut open = Vec::new();
    for id in builds.accepted_newest_first().await? {
        if id == delete.build_id {
            continue;
        }
        let p = builds.read_build(&id).await?;
        if (p.time, &p.build_id) > (delete.time, &delete.build_id) {
            continue;
        }
        match p.state {
            BuildState::Complete => break,
            BuildState::Pending | BuildState::Running => open.push(id),
            BuildState::Failed | BuildState::Incomplete => {}
        }
    }
    Ok(open)
}

/// The fabric.delete in force, when one is: the current Build when it is one (`false`), or the newest
/// accepted Build when it is a failed fabric.delete (`true`: it stays in force until resubmitted).
pub async fn fabric_delete_in_force(builds: &dyn BuildStateAdapter) -> Result<Option<(BuildProjection, bool)>, BuildStateError> {
    let current = builds.current().await?;
    if let Some(c) = current.as_ref().filter(|c| c.deletes_fabric()) {
        return Ok(Some((c.clone(), false)));
    }
    let Some(newest) = builds.accepted_newest_first().await?.into_iter().next() else { return Ok(None) };
    if current.as_ref().is_some_and(|c| c.build_id == newest) {
        return Ok(None);
    }
    let p = builds.read_build(&newest).await?;
    Ok((p.deletes_fabric() && p.state.ended_short()).then_some((p, true)))
}

/// Why a Build a fabric.delete interrupts ends `Failed`.
pub fn interrupted_reason(interrupted: &BuildId, delete: &BuildId) -> String {
    format!("Build {interrupted} is interrupted by fabric.delete Build {delete}, accepted after it: its dispatched sections have ended, it runs no further step and is never resumed")
}

/// Write `accepted` (the Build record, once) under its span.
pub async fn accept(builds: &dyn BuildStateAdapter, accepted: &BuildAccepted, previous: Option<&BuildId>, node: &str, via: &str) -> Result<(), BuildStateError> {
    use tracing::Instrument as _;
    // The span opens BEFORE the write: the Build is current to a reader the moment it is in the
    // store, which is before `publish_accepted` returns.
    let span = tracing::info_span!(
        "rdm.node_admin.build.create.via-accepted",
        node = %node,
        build_id = %accepted.build_id.log(),
        previous_build_id = %previous.map(|b| b.as_str()).unwrap_or(""),
        time = accepted.time,
        via,
    );
    let put = builds.publish_accepted(accepted).instrument(span.clone()).await;
    match &put {
        Ok(()) => span.in_scope(|| tracing::info!("the Build is in the builds store: it is the newest accepted, and the current Build")),
        Err(e) => span.in_scope(|| tracing::info!(error = %e, "the Build could not be written to the builds store: nothing is accepted")),
    }
    put
}

/// Why a Build store call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildStateError {
    /// The store holds no record of the Build.
    UnknownBuild(BuildId),
    /// The Build was written before with another record.
    ConflictingIntent(BuildId),
    /// The store could not be read or written.
    Io(String),
    /// The fabric-primary record this admin holds names another live birth: it writes nothing.
    RoleYielded {
        /// This admin.
        node: rdm_core::NodeId,
        /// The record that moved the role, in words.
        by: String,
        /// The birth that holds the role now, when one is named.
        to: Option<rdm_core::framing::Birth>,
    },
}

impl std::fmt::Display for BuildStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownBuild(id) => write!(f, "no Build {id}"),
            Self::ConflictingIntent(id) => write!(f, "Build {id} was published with a different intent"),
            Self::Io(e) => write!(f, "build journal I/O: {e}"),
            Self::RoleYielded { node, by, .. } => write!(f, "{node} is no longer the fabric-primary: {by}; its Build log decides nothing any more"),
        }
    }
}

impl From<crate::storage_error::StorageError> for BuildStateError {
    fn from(e: crate::storage_error::StorageError) -> Self {
        Self::Io(e.to_string())
    }
}

/// The Builds of `facts` by id, forgotten ones left out.
pub fn fold(facts: &[BuildFact]) -> BTreeMap<BuildId, BuildAccepted> {
    let mut out = BTreeMap::new();
    for f in facts {
        match f {
            BuildFact::Accepted(a) => {
                out.entry(a.build_id.clone()).or_insert_with(|| a.clone());
            }
            BuildFact::Forget { build_id } => {
                out.remove(build_id);
            }
        }
    }
    out
}

/// The Build store (build.md §5). Every adapter reaches the events store ([`Self::events`]): the
/// Build's view is its record with its events folded.
#[async_trait]
pub trait BuildStateAdapter: Send + Sync {
    /// Write the Build record, once.
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError>;
    /// The Build's view: its record and its events.
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError>;
    /// The current Build, read newest first by its events.
    async fn current_build(&self) -> Result<CurrentBuild, BuildStateError> {
        current_of_builds(self).await
    }
    /// The current Build's view, when one is.
    async fn current(&self) -> Result<Option<BuildProjection>, BuildStateError> {
        match self.current_build().await?.into_build_id() {
            Some(id) => self.read_build(&id).await.map(Some),
            None => Ok(None),
        }
    }
    /// The newest acceptance's time: the acceptance-order index's high-water mark.
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError>;
    /// A finished Build leaves history.
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError>;
    /// The Builds held, newest accepted first: the acceptance-order index.
    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError>;
    /// The events store the Build's events are in.
    fn events(&self) -> Arc<dyn EventsStorage>;
    /// The path name this admin holds, when the adapter knows it.
    fn held_name(&self) -> Option<crate::model::PathName> {
        None
    }
    /// Write `step`'s `rdm.build.step` event. Only a run's step log writes one
    /// ([`crate::build_run::FramedBuilds`]): it knows the writer's birth and its mesh. Any other
    /// adapter refuses by name.
    async fn write_step(&self, step: &StepWrite) -> Result<(), BuildStateError> {
        Err(BuildStateError::Io(format!("this Build store writes no step event: {} of {} is written by a run's step log", step.step, step.operation)))
    }
    /// The step in flight is pending for `reason` (its node's `pre_node_ready` answered `Pending`): live progress, never durable.
    async fn note_pending(&self, _build_id: &BuildId, _operation: &rdm_core::OperationId, _step: &str, _reason: &str) {}
}

#[derive(Debug, Default)]
struct FactLog {
    /// The facts readers see, one immutable snapshot: a reader takes it with one `Arc` clone and
    /// folds it outside this lock; a writer replaces it whole once the derivation ran over the facts
    /// it adds ([`publish`]).
    facts: Arc<Vec<BuildFact>>,
    /// The acceptance-order index (R-BI3): every Build accepted, by its own acceptance stamp
    /// `(time, build_id)`, entered once by its `Accepted` fact and never rewritten, in
    /// whatever order the facts arrive. Read newest first by [`current_build`].
    accepted_order: std::collections::BTreeSet<(u64, BuildId)>,
    /// Each Build's own facts, by their position in `facts`: a Build is read by folding its facts
    /// alone ([`read_own`]).
    by_build: std::collections::HashMap<BuildId, Vec<usize>>,
}

/// A derivation over the Build facts, run before any reader sees the facts a writer adds
/// ([`LocalBuildLog::derive_on_every_fact`]): `derive` over the facts of the Builds it reads, never
/// the whole log. It reads the Builds the new facts belong to, the current Build, and the Builds
/// `held` names (those whose derived state its caller still holds). Only the current Build runs
/// (build.md: a second topology change is refused while the current Build is in progress), so an
/// adapter that keeps no log in memory serves it with per-Build reads.
#[derive(Clone)]
pub struct FactDerivation {
    /// The Builds whose derived state the caller holds now.
    held: Arc<dyn Fn() -> Vec<BuildId> + Send + Sync>,
    /// The derivation, over the facts of the Builds it reads, in log order.
    derive: Arc<dyn Fn(&[BuildFact]) + Send + Sync>,
}

impl FactDerivation {
    /// `derive` over the facts of the Builds it reads, `held` naming the Builds its caller holds
    /// derived state of.
    pub fn new(held: impl Fn() -> Vec<BuildId> + Send + Sync + 'static, derive: impl Fn(&[BuildFact]) + Send + Sync + 'static) -> Self {
        Self { held: Arc::new(held), derive: Arc::new(derive) }
    }

    /// The Builds the derivation reads when `new` enters the log: theirs, the current Build and the
    /// held ones.
    pub fn reads(&self, new: &[BuildFact], current: Option<&BuildId>) -> std::collections::HashSet<BuildId> {
        let mut reads: std::collections::HashSet<BuildId> = new.iter().map(|f| f.build_id().clone()).collect();
        reads.extend((self.held)());
        reads.extend(current.cloned());
        reads
    }

    /// Run the derivation over `facts`, the facts of the Builds it reads ([`FactDerivation::reads`]).
    pub fn derive(&self, facts: &[BuildFact]) {
        (self.derive)(facts)
    }

    /// Run it on `log`, whose facts will stand as `all` once `new` enters: over the facts of the
    /// Builds it reads, in log order.
    fn run(&self, log: &Mutex<FactLog>, all: &[BuildFact], new: &[BuildFact]) {
        let current = accepted_newest_first(log).into_iter().next();
        let reads = self.reads(new, current.as_ref());
        let facts: Vec<BuildFact> = all.iter().filter(|f| reads.contains(f.build_id())).cloned().collect();
        self.derive(&facts);
    }
}

/// The derivation a log runs on every publication, when one is installed: held with the writer's turn.
#[derive(Default)]
struct Derived(Option<FactDerivation>);

impl std::fmt::Debug for Derived {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() { "Derived(installed)" } else { "Derived(none)" })
    }
}

impl FactLog {
    /// Fold `fact`, which sits at position `at` of the facts, into the indexes (own appends, absorbed
    /// facts and a journal's replay alike).
    fn index(&mut self, fact: &BuildFact, at: usize) {
        if let BuildFact::Accepted(a) = fact {
            self.accepted_order.insert((a.time, a.build_id.clone()));
        }
        self.by_build.entry(fact.build_id().clone()).or_default().push(at);
    }
}

/// What readers see of `log` now: one `Arc` clone under its lock.
fn snapshot(log: &Mutex<FactLog>) -> Arc<Vec<BuildFact>> {
    log.lock().unwrap().facts.clone()
}

/// Publish `new` (facts the log does not hold yet) to readers. The derivation runs first, over the
/// log with them, while readers keep reading the log as it stood: no reader sees a fact before what
/// is derived from it reflects it, and no reader waits on the derivation. Then the indexes take the
/// facts and the snapshot is swapped, under one brief hold of the readers' lock. The caller holds the
/// writer's turn, so nothing else publishes between the read and the swap.
fn publish(log: &Mutex<FactLog>, derive: Option<&FactDerivation>, new: Vec<BuildFact>) {
    if new.is_empty() {
        return;
    }
    let held = snapshot(log);
    let mut next = Vec::with_capacity(held.len() + new.len());
    next.extend(held.iter().cloned());
    next.extend(new.iter().cloned());
    drop(held);
    let next = Arc::new(next);
    if let Some(derive) = derive {
        derive.run(log, &next, &new);
    }
    let mut log = log.lock().unwrap();
    let base = log.facts.len();
    for (i, f) in new.iter().enumerate() {
        log.index(f, base + i);
    }
    log.facts = next;
}

/// The accepted Builds `stored`, as the store root records them, in place of the copies `log` holds of
/// them, each at its own position (build.md §5.3: the Build facts are "written by the fabric-primary,
/// so a successor reads them there"): an admin holds each Build as the store root records it once it
/// reads it there. The derivation runs over the log as it will stand, as for any publication.
#[cfg(feature = "file-backend")]
fn take_stored_acceptances(log: &Mutex<FactLog>, derive: Option<&FactDerivation>, stored: Vec<BuildAccepted>) {
    if stored.is_empty() {
        return;
    }
    let held = snapshot(log);
    let next: Vec<BuildFact> = held
        .iter()
        .map(|f| match f {
            BuildFact::Accepted(a) => stored.iter().find(|s| s.build_id == a.build_id).map(|s| BuildFact::Accepted(s.clone())).unwrap_or_else(|| f.clone()),
            other => other.clone(),
        })
        .collect();
    drop(held);
    let next = Arc::new(next);
    if let Some(derive) = derive {
        let taken: Vec<BuildFact> = stored.into_iter().map(BuildFact::Accepted).collect();
        derive.run(log, &next, &taken);
    }
    log.lock().unwrap().facts = next;
}

/// Whether `accepted` is held already (`Ok(true)`), free (`Ok(false)`), or held with another intent.
fn accepted_conflict(facts: &[BuildFact], accepted: &BuildAccepted) -> Result<bool, BuildStateError> {
    for f in facts {
        if let BuildFact::Accepted(i) = f {
            if i.build_id == accepted.build_id {
                return if i == accepted { Ok(true) } else { Err(BuildStateError::ConflictingIntent(accepted.build_id.clone())) };
            }
        }
    }
    Ok(false)
}

/// Whether `f` adds anything to `facts`: acceptances, opens and claims are insert-and-fail on
/// their key, everything else deduplicates.
fn is_new(facts: &[BuildFact], f: &BuildFact) -> bool {
    match f {
        BuildFact::Accepted(i) => !known(facts, &i.build_id),
        BuildFact::Forget { .. } => !facts.contains(f),
    }
}

fn known(facts: &[BuildFact], build_id: &BuildId) -> bool {
    facts.iter().any(|f| matches!(f, BuildFact::Accepted(i) if &i.build_id == build_id))
}

fn read(facts: &[BuildFact], build_id: &BuildId) -> Result<BuildAccepted, BuildStateError> {
    fold(facts).remove(build_id).ok_or_else(|| BuildStateError::UnknownBuild(build_id.clone()))
}

/// The folded view of `build_id`, from its own facts alone: its positions and the snapshot are taken
/// together under the readers' lock (one `Arc` clone and one small point read), and the Build's facts
/// are folded outside it.
fn read_own(log: &Mutex<FactLog>, build_id: &BuildId) -> Result<BuildAccepted, BuildStateError> {
    let (facts, at) = {
        let log = log.lock().unwrap();
        (log.facts.clone(), log.by_build.get(build_id).cloned().unwrap_or_default())
    };
    let own: Vec<BuildFact> = at.iter().map(|i| facts[*i].clone()).collect();
    read(&own, build_id)
}

/// The point read of [`BuildStateAdapter::accepted_newest_first`]: the index, newest first.
fn accepted_newest_first(log: &Mutex<FactLog>) -> Vec<BuildId> {
    log.lock().unwrap().accepted_order.iter().rev().map(|(_, id)| id.clone()).collect()
}

/// The point read of [`BuildStateAdapter::newest_accepted_at_ms`]: the index's head.
fn newest_accepted_at_ms(log: &Mutex<FactLog>) -> Option<u64> {
    log.lock().unwrap().accepted_order.last().map(|(at, _)| *at)
}

/// In-memory Build log, beside an events store.
pub struct MemoryBuildStateAdapter {
    log: Mutex<FactLog>,
    /// The writer's turn: one writer at a time decides on the log and publishes to it, holding the
    /// derivation each publication runs. An async lock: a writer waiting on another's derivation
    /// yields its worker. Readers never take it.
    writer: tokio::sync::Mutex<Derived>,
    /// The Build storage this log's admin reads committed records from (Q292), when it is another log:
    /// an admin of a fabric whose fabric-primary's log stands for the shared store. None: this log is
    /// its own storage.
    storage: std::sync::OnceLock<Storage>,
    /// The events store the Builds' events are in.
    events: Arc<dyn EventsStorage>,
}

impl std::fmt::Debug for MemoryBuildStateAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBuildStateAdapter").field("log", &self.log).finish_non_exhaustive()
    }
}

impl Default for MemoryBuildStateAdapter {
    fn default() -> Self {
        Self { log: Default::default(), writer: Default::default(), storage: Default::default(), events: Arc::new(crate::events_storage::MemoryEventsStorage::new()) }
    }
}

/// The Build storage a [`MemoryBuildStateAdapter`] reads committed records from (Q292).
#[derive(Clone)]
struct Storage(Arc<dyn LocalBuildLog>);

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Storage")
    }
}

impl MemoryBuildStateAdapter {
    /// An empty store, with its own empty events store.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store whose Builds' events are in `events`.
    pub fn with_events(events: Arc<dyn EventsStorage>) -> Self {
        Self { events, ..Self::default() }
    }

    /// This log, its admin reading the Build's committed records from `storage` (Q292): an admin's own
    /// log beside the fabric's shared Build storage, as every admin but the fabric-primary keeps one.
    /// Its events are the storage's.
    pub fn persisted_in(mut self, storage: Arc<dyn LocalBuildLog>) -> Self {
        self.events = storage.events();
        let _ = self.storage.set(Storage(storage));
        self
    }

    /// Merge records received from storage (append the unseen ones; a record is insert-and-fail).
    pub async fn absorb(&self, facts: &[BuildFact]) {
        let w = self.writer.lock().await;
        let held = snapshot(&self.log);
        let mut new: Vec<BuildFact> = Vec::new();
        for f in facts {
            if is_new(&held, f) && is_new(&new, f) {
                new.push(f.clone());
            }
        }
        drop(held);
        publish(&self.log, w.0.as_ref(), new);
    }

    /// Publish `fact` under the writer's turn `w`.
    fn put(&self, w: &Derived, fact: BuildFact) {
        publish(&self.log, w.0.as_ref(), vec![fact]);
    }
}

/// The Build log an admin's Build state is over: in memory for runs that need no restart survival,
/// or the fabric's Build journal on the shared store root (`builds.storage`).
#[async_trait]
pub trait LocalBuildLog: BuildStateAdapter {
    /// Merge the committed records read from storage (Q292: [`Self::committed_facts`]): append the
    /// unseen ones; a record is insert-and-fail.
    async fn absorb_facts(&self, facts: &[BuildFact]);
    /// Run `derive` over the records of the Builds it reads ([`FactDerivation::reads`]: the newest
    /// Build and the held ones, then also those the new records belong to), once now over the records
    /// it holds, reading no store, and again each time records enter the log, whoever put them there,
    /// before any reader sees them and before the append returns. It runs under the writer's turn,
    /// never under the lock readers take.
    async fn derive_on_every_fact(&self, derive: FactDerivation);
    /// Merge one Build's whole committed records, as a read of storage answered them (Q292), into what
    /// this log holds. A log that reads no store merges them as [`Self::absorb_facts`] does.
    async fn absorb_build(&self, facts: &[BuildFact]) {
        self.absorb_facts(facts).await
    }
    /// The records this log holds.
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError>;
    /// The committed records of `build` (every Build's when `None`) as storage holds them (Q292); a
    /// Build the storage holds no record of is [`BuildStateError::UnknownBuild`].
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError>;
    /// `build_id` as the records this log holds now, reading no store: what a launched node-admin's
    /// Build floor is checked against at its start (node-start.md step 4; Q227).
    async fn held_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.read_build(build_id).await
    }
    /// The current Build as the records this log holds now, reading no store ([`Self::held_build`]).
    async fn held_current_build(&self) -> Result<CurrentBuild, BuildStateError> {
        self.current_build().await
    }
    /// From now on this log reads the Build records on the store only while `reader` names this admin
    /// (Q227: "only FP reads the builds"; build.md §5): while it is the fabric-primary, or the
    /// executor fabric recovery names in its place (Q122). A log that reads no store has nothing to
    /// switch.
    async fn read_records_while(&self, _reader: RecordsReader) {}
    /// Read the Build records now, for `op`, as admin `node`, whatever [`Self::read_records_while`]
    /// says: the re-born fabric-primary's start over an inherited root with nobody to pull from (Q227).
    /// A log that reads no store has nothing to read.
    async fn read_records_now(&self, _op: &'static str, _node: &str) -> Result<(), BuildStateError> {
        Ok(())
    }
}

/// Whether this admin reads the Build records now ([`LocalBuildLog::read_records_while`]): its
/// path and name while it does, `None` while it holds only what reached it.
pub type RecordsReader = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// `accepted`'s view: its record with the events `events` holds of it.
async fn projection_of(events: &dyn EventsStorage, accepted: &BuildAccepted) -> Result<BuildProjection, BuildStateError> {
    let of = events.of_build(&accepted.build_id).await?;
    Ok(BuildProjection::of(accepted, &of))
}

#[async_trait]
impl LocalBuildLog for MemoryBuildStateAdapter {
    async fn absorb_facts(&self, facts: &[BuildFact]) {
        self.absorb(facts).await
    }

    async fn derive_on_every_fact(&self, derive: FactDerivation) {
        let mut w = self.writer.lock().await;
        derive.run(&self.log, &snapshot(&self.log), &[]);
        w.0 = Some(derive);
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        Ok(snapshot(&self.log).as_ref().clone())
    }

    /// A log held in one process persists nowhere: its committed records are the ones it holds, or the
    /// storage it was given's ([`MemoryBuildStateAdapter::persisted_in`]).
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
        if let Some(Storage(storage)) = self.storage.get() {
            return storage.committed_facts(build).await;
        }
        committed_of(self.facts().await?, build)
    }
}

#[async_trait]
impl BuildStateAdapter for MemoryBuildStateAdapter {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        if !accepted_conflict(&snapshot(&self.log), accepted)? {
            self.put(&w, BuildFact::Accepted(accepted.clone()));
        }
        Ok(())
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        let accepted = read_own(&self.log, build_id)?;
        projection_of(&*self.events, &accepted).await
    }

    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        Ok(newest_accepted_at_ms(&self.log))
    }

    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        Ok(accepted_newest_first(&self.log))
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.put(&w, BuildFact::Forget { build_id: build_id.clone() });
        Ok(())
    }

    fn events(&self) -> Arc<dyn EventsStorage> {
        self.events.clone()
    }
}


#[cfg(feature = "file-backend")]
#[async_trait]
impl LocalBuildLog for FileJournal {
    async fn derive_on_every_fact(&self, derive: FactDerivation) {
        let mut w = self.turn("derive_on_every_fact").await;
        // Over the facts the log holds now, reading no store (Q227): every fact a later read of the
        // store root takes is derived as it is published, as an own append's is.
        derive.run(&self.log, &snapshot(&self.log), &[]);
        w.0 = Some(derive);
    }

    async fn absorb_facts(&self, facts: &[BuildFact]) {
        // Under the writer's turn; the store root is not read (Q227): a fact a later read of the store
        // takes again is deduplicated there.
        let w = self.turn("absorb_facts").await;
        let held = snapshot(&self.log);
        let mut new: Vec<BuildFact> = Vec::new();
        for f in facts {
            if is_new(&held, f) && is_new(&new, f) {
                new.push(f.clone());
            }
        }
        drop(held);
        publish(&self.log, w.0.as_ref(), new);
    }

    async fn absorb_build(&self, facts: &[BuildFact]) {
        // Under the writer's turn, which the derivation runs under; the store root is not read.
        let w = self.turn("absorb_build").await;
        let held = snapshot(&self.log);
        let mut new: Vec<BuildFact> = Vec::new();
        for f in facts {
            if is_new(&held, f) && is_new(&new, f) {
                new.push(f.clone());
            }
        }
        drop(held);
        publish(&self.log, w.0.as_ref(), new);
    }

    async fn held_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        let accepted = read_own(&self.log, build_id)?;
        projection_of(&*self.events, &accepted).await
    }


    async fn read_records_while(&self, reader: RecordsReader) {
        let _ = self.reader.set(reader);
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.sync_for_read("facts").await?;
        Ok(snapshot(&self.log).as_ref().clone())
    }

    /// A fresh read of `builds.jsonl` whole (Q292), on the blocking pool: neither the shared cursor
    /// nor the writer's turn is touched, so what is answered is exactly what this read obtained. Its
    /// records are folded as a take folds them: each write (`id`) once, in the order they were
    /// committed (`seq`); an object another writer is still writing is left out by the read.
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
        let path = self.file.path().to_path_buf();
        let build = build.cloned();
        tokio::task::spawn_blocking(move || read_committed(&path, build.as_ref()))
            .await
            .map_err(|e| BuildStateError::Io(format!("the read of the Build records did not finish: {e}")))?
    }

    async fn read_records_now(&self, op: &'static str, node: &str) -> Result<(), BuildStateError> {
        let w = self.turn(op).await;
        let read = match self.store_call(self.read_new(op)).await {
            Ok(read) => read,
            Err(e) => {
                self.name_take(op, node, Err(&e));
                return Err(e);
            }
        };
        let records = read.len();
        self.take_read(&w, read);
        self.name_take(op, node, Ok(records));
        Ok(())
    }

}

#[cfg(feature = "file-backend")]
/// The `dataschema` of one Build record's line in `builds.jsonl`, its `subject` its Build's id
/// ([`FileJournal`]).
// durable format moved: the journal holds Builds only; attempts, claims and receipts are gone (the
// steps are the events store's `rdm.build.step`).
pub const RECORD_FORMAT: &str = "build-record/3";

#[cfg(feature = "file-backend")]
/// The `type` of [`RECORD_FORMAT`]'s lines.
pub const RECORD_KIND: &str = "build-record";

#[cfg(feature = "file-backend")]
/// One Build record as it is stored: the fact, and `seq`, its place in the order its writer wrote the
/// records, which a reader replays them in (the fold breaks its ties by that order: a step's receipts
/// replay in the order they were written).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredFact {
    seq: u64,
    fact: BuildFact,
}

/// `facts` of `build` (every Build's when `None`), `Err(UnknownBuild)` when `build` has none (Q292).
fn committed_of(facts: Vec<BuildFact>, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
    let Some(build) = build else { return Ok(facts) };
    let of: Vec<BuildFact> = facts.into_iter().filter(|f| f.build_id() == build).collect();
    if of.is_empty() {
        return Err(BuildStateError::UnknownBuild(build.clone()));
    }
    Ok(of)
}

#[cfg(feature = "file-backend")]
/// The committed records of `build` (every Build's when `None`) in the Build journal `file` (a fabric
/// store root's `builds.jsonl`), read whole now (Q292): each write (`id`) once, in the order they were
/// committed (`seq`), as a take folds them; an object another writer is still writing is left out by the read.
/// `Err(UnknownBuild)` when it holds none of `build`, an I/O error when the file cannot be read, never
/// an empty Build. Blocking: the journal calls it on the blocking pool.
pub fn read_committed(file: &std::path::Path, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
    let lines = crate::store_log::read_file(file).map_err(store_io)?;
    let mut seen = std::collections::HashSet::new();
    let mut stored: Vec<StoredFact> = Vec::new();
    for line in lines.iter().filter(|l| l.attributes.dataschema == RECORD_FORMAT && build.is_none_or(|b| l.subject() == b.as_str())) {
        if seen.insert(line.attributes.id.clone()) {
            stored.push(line.decode::<StoredFact>(file).map_err(store_io)?);
        }
    }
    stored.sort_by_key(|s| s.seq);
    committed_of(stored.into_iter().map(|s| s.fact).collect(), build)
}

#[cfg(feature = "file-backend")]
#[cfg(feature = "file-backend")]
/// `builds.storage`: the Build journal on the fabric's shared store root, one line of its `builds.jsonl`
/// per fact, its `subject` its Build's id (build.md §5.3; R-SR1; Q365), appended when it is committed
/// (Q61). A successor folds the lines, each write (`id`) once.
pub struct FileJournal {
    /// The root's `builds.jsonl`.
    file: crate::store_log::StoreLog,
    /// The lines the file's fold has read and no turn has taken yet, in file order, each by its `id`.
    unread: Arc<Mutex<Vec<(crate::store_log::EventId, StoredFact)>>>,
    /// The process writing through this journal.
    log_writer: crate::store_log::LogWriter,
    /// Every record this journal took or wrote, by its line's `id`: a read takes only the others.
    seen: Arc<Mutex<std::collections::HashSet<crate::store_log::EventId>>>,
    /// The `seq` the next record is written at: past every record read or written.
    next_seq: std::sync::atomic::AtomicU64,
    /// The events store the Builds' events are in.
    events: Arc<dyn EventsStorage>,
    root_dir: PathBuf,
    /// The replayed facts and their indexes. Held only for a snapshot, a point read or a
    /// publication's swap, never across the disk, a fold or the derivation.
    log: Mutex<FactLog>,
    /// The writer's turn: decide on a snapshot, write the fact's record on the blocking pool, then
    /// publish, holding the derivation each publication runs. An async lock, so a writer waiting on
    /// another's write or derivation yields its worker. A reader takes it only when no turn holds it,
    /// to take what it read from the store root, and holds it across no disk.
    writer: tokio::sync::Mutex<Derived>,
    /// The turn holding `writer` and since when: what a turn that waits names as its holder.
    turn_of: Mutex<Option<(&'static str, std::time::Instant)>>,
    /// The time the holding turn spent in the store root's own calls (its reads and writes), in
    /// nanoseconds: what its span names apart from its wait.
    turn_store_ns: std::sync::atomic::AtomicU64,
    /// The blocking calls of the holder of `writer` (a turn or a reader's sync), from its store calls:
    /// how many, how long they queued for a blocking thread, how long they ran.
    holder_io: Mutex<Option<crate::blocking::IoTime>>,
    /// The facts the holder of `writer` read from the store root.
    holder_facts: std::sync::atomic::AtomicU64,
    /// What the last holder of `writer` did with it, written when it let it go: what a turn that
    /// waited names as the holder it waited behind.
    released: Mutex<Option<String>>,
    /// Whether this admin reads the records now ([`LocalBuildLog::read_records_while`]); unset, it
    /// reads (a bootstrap or adoption start, before its views exist: Q227's re-born fabric-primary).
    reader: std::sync::OnceLock<RecordsReader>,
    /// This tenure's take read is made: the next reads are tail reads.
    took: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "file-backend")]
/// One writer's turn of a [`FileJournal`], on its own span
/// (`rdm.node_admin.build.update.via-journal-turn`): `waited_ms`, how long it waited for the writer,
/// and `held_by`, the turn holding it when the wait began; then `held_ms`, how long it held the
/// writer, and `store_ms`, how much of that was the store root's own calls. A turn that waited on
/// another is told apart from one that waited on the store root.
struct Turn<'a> {
    guard: tokio::sync::MutexGuard<'a, Derived>,
    /// The operation the turn is for: what its reads of the store root are named by.
    op: &'static str,
    journal: &'a FileJournal,
    span: tracing::Span,
    began: std::time::Instant,
}

#[cfg(feature = "file-backend")]
impl std::ops::Deref for Turn<'_> {
    type Target = Derived;
    fn deref(&self) -> &Derived {
        &self.guard
    }
}

#[cfg(feature = "file-backend")]
/// A reader taking what it read from the store root, holding the writer, named as its holder until it
/// ends ([`FileJournal::sync_for_read`]).
struct ReadHolder<'a> {
    journal: &'a FileJournal,
    op: &'static str,
    began: std::time::Instant,
}

#[cfg(feature = "file-backend")]
impl<'a> ReadHolder<'a> {
    fn name(journal: &'a FileJournal, op: &'static str) -> Self {
        *journal.turn_of.lock().unwrap() = Some((op, std::time::Instant::now()));
        journal.hold_begins();
        Self { journal, op, began: std::time::Instant::now() }
    }
}

#[cfg(feature = "file-backend")]
impl Drop for ReadHolder<'_> {
    fn drop(&mut self) {
        self.journal.hold_ends(self.op, self.began.elapsed());
        *self.journal.turn_of.lock().unwrap() = None;
    }
}

#[cfg(feature = "file-backend")]
impl std::ops::DerefMut for Turn<'_> {
    fn deref_mut(&mut self) -> &mut Derived {
        &mut self.guard
    }
}

#[cfg(feature = "file-backend")]
impl Drop for Turn<'_> {
    fn drop(&mut self) {
        let store_ns = self.journal.turn_store_ns.swap(0, std::sync::atomic::Ordering::Relaxed);
        let op = (*self.journal.turn_of.lock().unwrap()).map(|(op, _)| op).unwrap_or_default();
        let (calls, queued, ran, facts) = self.journal.hold_ends(op, self.began.elapsed());
        *self.journal.turn_of.lock().unwrap() = None;
        self.span.record("held_ms", self.began.elapsed().as_millis() as u64).record("store_ms", store_ns / 1_000_000);
        self.span.record("io_calls", calls).record("io_queued_ms", queued).record("io_ms", ran).record("facts_read", facts);
    }
}

#[cfg(feature = "file-backend")]
impl std::fmt::Debug for FileJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileJournal").field("root_dir", &self.root_dir).finish()
    }
}

#[cfg(feature = "file-backend")]
fn store_io(e: crate::storage_error::StorageError) -> BuildStateError {
    BuildStateError::Io(e.to_string())
}

#[cfg(feature = "file-backend")]
impl FileJournal {
    /// Open the Build journal on the fabric's shared `root`, written by `writer` (R-BI6): its
    /// `builds.jsonl`, appended straight to the file and folded on its first read or append, never at
    /// the open (rdm#89, Q227). The Builds' events are in `events`.
    pub async fn open_on_root(root: &crate::store_root::StoreRoot, writer: &crate::store_log::LogWriter, events: Arc<dyn EventsStorage>) -> Result<Self, BuildStateError> {
        let unread: Arc<Mutex<Vec<(crate::store_log::EventId, StoredFact)>>> = Arc::default();
        let queue = unread.clone();
        let observe: crate::store_log::Observer = Arc::new(move |l: &crate::store_log::Line| {
            if let Ok(stored) = StoredFact::deserialize(&l.data) {
                queue.lock().unwrap_or_else(|e| e.into_inner()).push((l.attributes.id.clone(), stored));
            }
        });
        let file = crate::store_log::StoreLog::open_observed_on_first_read(&root.dir().join(crate::store_root::BUILDS_FILE), crate::store_log::Formats::new().with::<StoredFact>(RECORD_FORMAT), observe).await.map_err(store_io)?;
        Ok(Self {
            file,
            unread,
            log_writer: writer.clone(),
            seen: Arc::default(),
            next_seq: std::sync::atomic::AtomicU64::new(1),
            events,
            root_dir: root.dir().to_path_buf(),
            log: Mutex::new(FactLog::default()),
            writer: tokio::sync::Mutex::new(Derived::default()),
            turn_of: Mutex::new(None),
            turn_store_ns: std::sync::atomic::AtomicU64::new(0),
            holder_io: Mutex::new(None),
            holder_facts: std::sync::atomic::AtomicU64::new(0),
            released: Mutex::new(None),
            reader: std::sync::OnceLock::new(),
            took: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The writer's turn for `op`, spanned ([`Turn`]).
    async fn turn(&self, op: &'static str) -> Turn<'_> {
        use tracing::Instrument;
        let held_by = (*self.turn_of.lock().unwrap()).map(|(holder, since)| format!("{holder} ({} ms in)", since.elapsed().as_millis())).unwrap_or_default();
        let span = tracing::info_span!(
            "rdm.node_admin.build.update.via-journal-turn",
            op,
            held_by = %held_by,
            waited_ms = tracing::field::Empty,
            waited_behind = tracing::field::Empty,
            held_ms = tracing::field::Empty,
            store_ms = tracing::field::Empty,
            io_calls = tracing::field::Empty,
            io_queued_ms = tracing::field::Empty,
            io_ms = tracing::field::Empty,
            facts_read = tracing::field::Empty,
            "otel.kind" = "internal"
        );
        let asked = std::time::Instant::now();
        let guard = self.writer.lock().instrument(span.clone()).await;
        let waited = asked.elapsed();
        span.record("waited_ms", waited.as_millis() as u64);
        if waited.as_millis() > 0 {
            if let Some(behind) = self.released.lock().unwrap().clone() {
                span.record("waited_behind", behind.as_str());
            }
        }
        *self.turn_of.lock().unwrap() = Some((op, std::time::Instant::now()));
        self.turn_store_ns.store(0, std::sync::atomic::Ordering::Relaxed);
        self.hold_begins();
        Turn { guard, op, journal: self, span, began: std::time::Instant::now() }
    }

    /// The holder of `writer` begins: its store calls' blocking I/O and the facts it reads are its own.
    fn hold_begins(&self) {
        *self.holder_io.lock().unwrap() = Some(crate::blocking::IoTime::default());
        self.holder_facts.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// The holder `op` lets `writer` go after `held`: what it did is what a turn waiting behind it names.
    /// Returns its blocking calls, their queue and run time in ms, and the facts it read.
    fn hold_ends(&self, op: &str, held: std::time::Duration) -> (u64, u64, u64, u64) {
        let io = self.holder_io.lock().unwrap().take().unwrap_or_default();
        let facts = self.holder_facts.swap(0, std::sync::atomic::Ordering::Relaxed);
        let (calls, queued, ran) = (io.calls(), io.queued_ms(), io.ran_ms());
        *self.released.lock().unwrap() = Some(format!("{op} held {} ms: {calls} store calls, queued {queued} ms, ran {ran} ms, {facts} facts read", held.as_millis()));
        (calls, queued, ran, facts)
    }

    /// `call`, one of the store root's own, its time counted to the holding turn ([`Turn`]).
    async fn store_call<T>(&self, call: impl std::future::Future<Output = T>) -> T {
        let at = std::time::Instant::now();
        let io = self.holder_io.lock().unwrap().clone();
        let out = match io {
            Some(io) => crate::blocking::io_time_with(io, call).await,
            None => call.await,
        };
        self.turn_store_ns.fetch_add(at.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
        out
    }

    /// The records on the store root this journal has not taken yet, each with its line's `id`: the file's
    /// lines past the fold's offset, read now for `op` (the turn or read it is for, what a read that
    /// waits on the file names), and any a read could not take before. Nothing is marked taken here.
    async fn read_new(&self, op: &'static str) -> Result<Vec<(crate::store_log::EventId, StoredFact)>, BuildStateError> {
        self.file.advance_for(op).await.map_err(store_io)?;
        Ok(std::mem::take(&mut *self.unread.lock().unwrap_or_else(|e| e.into_inner())))
    }

    /// Take the records `read` from the store root into the log under the writer's turn `w`, in the
    /// order they were written: each is marked taken, and the facts the log does not hold yet are
    /// published.
    fn take_read(&self, w: &Derived, mut read: Vec<(crate::store_log::EventId, StoredFact)>) {
        if read.is_empty() {
            return;
        }
        read.sort_by_key(|(_, s)| s.seq);
        let held = snapshot(&self.log);
        let mut new: Vec<BuildFact> = Vec::new();
        let mut stored: Vec<BuildAccepted> = Vec::new();
        {
            let mut seen = self.seen.lock().unwrap();
            for (id, StoredFact { seq, fact }) in read {
                if !seen.insert(id) {
                    continue;
                }
                self.holder_facts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.next_seq.fetch_max(seq + 1, std::sync::atomic::Ordering::Relaxed);
                if let BuildFact::Accepted(a) = &fact {
                    if held.iter().any(|f| matches!(f, BuildFact::Accepted(h) if h.build_id == a.build_id && h != a)) {
                        stored.push(a.clone());
                        continue;
                    }
                }
                if is_new(&held, &fact) && is_new(&new, &fact) {
                    new.push(fact);
                }
            }
        }
        drop(held);
        take_stored_acceptances(&self.log, w.0.as_ref(), stored);
        publish(&self.log, w.0.as_ref(), new);
    }

    /// Read the records not taken yet, under the writer's turn `w`: a turn writes a record, and the
    /// one writer of the Build records is the fabric-primary, or fabric recovery's executor in its
    /// place (build.md §5.3, Q118, Q122), the admins that read them (Q227).
    async fn sync(&self, w: &Turn<'_>) -> Result<(), BuildStateError> {
        // The first read of a tenure under the writer is its take ([`Self::name_take`]).
        let take = self.reads_now().filter(|_| !self.took.load(std::sync::atomic::Ordering::Acquire));
        let read = match self.store_call(self.read_new(w.op)).await {
            Ok(read) => read,
            Err(e) => {
                if let Some(node) = &take {
                    self.name_take(w.op, node, Err(&e));
                }
                return Err(e);
            }
        };
        let records = read.len();
        self.take_read(w, read);
        if let Some(node) = take {
            self.took.store(true, std::sync::atomic::Ordering::Release);
            self.name_take(w.op, &node, Ok(records));
        }
        Ok(())
    }

    /// Sync from the store root for a read: the records are read from the disk holding no lock, so a
    /// writer's turn never waits behind a reader's disk; what was read is taken under the writer, with
    /// no disk, only when no turn holds it (a turn that holds it reads the root itself), so a reader
    /// never waits behind a writer either. While it takes, `op` (the reader) is the holder a waiting
    /// turn names. Records a read could not take stay untaken for the next read.
    async fn sync_for_read(&self, op: &'static str) -> Result<(), BuildStateError> {
        // Only the fabric-primary (or fabric recovery's executor) reads the records (Q227); every
        // other admin answers from what reached it.
        if self.reads_now().is_none() {
            self.took.store(false, std::sync::atomic::Ordering::Release);
            return Ok(());
        }
        if !self.took.load(std::sync::atomic::Ordering::Acquire) {
            return self.read_at_take(op).await;
        }
        let read = self.read_new(op).await?;
        match self.writer.try_lock() {
            Ok(w) => {
                let _holder = ReadHolder::name(self, op);
                self.take_read(&w, read);
            }
            // Untaken: the next read takes them, before the lines it reads.
            Err(_) => {
                let mut unread = self.unread.lock().unwrap_or_else(|e| e.into_inner());
                let later = std::mem::take(&mut *unread);
                *unread = read.into_iter().chain(later).collect();
            }
        }
        Ok(())
    }

    /// Whether this admin reads the Build records now ([`LocalBuildLog::read_records_while`]): its
    /// path and name while it does; unset, it reads (`""`), as a bootstrap or adoption start does.
    fn reads_now(&self) -> Option<String> {
        match self.reader.get() {
            Some(reader) => reader(),
            None => Some(String::new()),
        }
    }

    /// The take (Q227: the re-born or newly taking fabric-primary "will need to read the last build
    /// record to determine if anything is in flight"; build.md §5 "new fabric-primary -> reads the
    /// Builds newest first"), when a read comes first: this tenure's first read of the records, for
    /// `op`, made under the writer's turn so the records it reads are in the log before any read
    /// answers ([`Self::sync`] names it). A turn of this tenure that read already made it.
    async fn read_at_take(&self, op: &'static str) -> Result<(), BuildStateError> {
        let w = self.turn(op).await;
        if self.took.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        self.sync(&w).await
    }

    /// This tenure's first read of the records under the writer's turn (`op`), `read` of them taken:
    /// named once on `rdm.node_admin.build.resolve.via-records-read` with how many it read and the
    /// current Build the log holds after it, or refused naming why (`error`; the next read reads again).
    fn name_take(&self, op: &str, node: &str, read: Result<usize, &BuildStateError>) {
        let span = tracing::info_span!("rdm.node_admin.build.resolve.via-records-read", node, reader = op, store = "builds.storage", records = tracing::field::Empty, current_build_id = tracing::field::Empty, state = tracing::field::Empty, outcome = tracing::field::Empty, error = tracing::field::Empty, "otel.kind" = "internal");
        match read {
            Ok(records) => {
                span.record("records", records as u64).record("outcome", "read");
                if let Some(id) = accepted_newest_first(&self.log).first() {
                    span.record("current_build_id", id.as_str());
                }
                span.in_scope(|| tracing::info!("this admin reads the Build records: the ones the store holds are read once, to learn what is in flight, and every read after it reads on from there"));
            }
            Err(e) => {
                span.record("outcome", "refused").record("error", e.to_string().as_str());
                span.in_scope(|| tracing::info!("the Build records could not be read at the take: the next read reads them again"));
            }
        }
    }

    /// Append `fact` at its Build's id and publish it, under the writer's turn `w` that has read the store
    /// root. Nothing is written when the log holds the fact already: a record is written once.
    async fn append(&self, w: &Derived, fact: BuildFact) -> Result<(), BuildStateError> {
        if !is_new(&snapshot(&self.log), &fact) {
            return Ok(());
        }
        let stored = StoredFact { seq: self.next_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed), fact };
        let attributes = crate::store_log::Attributes::of(&self.log_writer, RECORD_KIND, RECORD_FORMAT, stored.fact.build_id().as_str());
        let id = attributes.id.clone();
        self.store_call(self.file.append_with(attributes, &stored)).await.map_err(store_io)?;
        self.seen.lock().unwrap().insert(id);
        publish(&self.log, w.0.as_ref(), vec![stored.fact]);
        Ok(())
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl BuildStateAdapter for FileJournal {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        let w = self.turn("publish_accepted").await;
        self.sync(&w).await?;
        if accepted_conflict(&snapshot(&self.log), accepted)? {
            return Ok(());
        }
        self.append(&w, BuildFact::Accepted(accepted.clone())).await
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.sync_for_read("read_build").await?;
        let accepted = read_own(&self.log, build_id)?;
        projection_of(&*self.events, &accepted).await
    }

    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.sync_for_read("newest_accepted_at_ms").await?;
        Ok(newest_accepted_at_ms(&self.log))
    }

    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        self.sync_for_read("accepted_newest_first").await?;
        Ok(accepted_newest_first(&self.log))
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let w = self.turn("forget").await;
        self.sync(&w).await?;
        self.append(&w, BuildFact::Forget { build_id: build_id.clone() }).await
    }

    fn events(&self) -> Arc<dyn EventsStorage> {
        self.events.clone()
    }
}

#[cfg(test)]
mod outcome_code_receipt_cells {
    #[test]
    fn a_failed_step_receipt_carries_its_standard_code_beside_its_reason() {
        let receipt = super::StepOutcome::failed("same explanation");
        let value = serde_json::to_value(receipt).unwrap();
        assert_eq!(value["failed"]["code"], serde_json::json!(9), "the failed-state receipt carries FAILED_PRECONDITION, not just explanation text");
        assert_eq!(value["failed"]["reason"], "same explanation");
    }
}

#[cfg(test)]
mod typed_failure_receipt_cells {
    use super::*;
    use rdm_core::outcome::Code;
    #[test]
    fn failed_receipt_fields_keep_their_positions_before_the_appended_code() {
        let step = StepOutcome::Failed { reason: "r".into(), code: Code::Unavailable };
        assert_eq!(rdm_transport::wire::encode(&step).unwrap(), vec![1, 1, b'r', 14]);
    }
    #[test]
    fn replay_retains_typed_failure_codes_and_defaults_only_legacy_json_failed_state() {
        for code in [Code::Unknown, Code::DeadlineExceeded, Code::Unavailable] {
            let outcome = StepOutcome::Failed { reason: "the same explanation".into(), code };
            let json = serde_json::to_vec(&outcome).unwrap();
            assert_eq!(serde_json::from_slice::<StepOutcome>(&json).unwrap(), outcome);
            let wire = rdm_transport::wire::encode(&outcome).unwrap();
            assert_eq!(rdm_transport::wire::decode::<StepOutcome>(&wire).unwrap(), outcome);
            assert_eq!(wire.last(), Some(&code.number()));
        }
        assert_eq!(serde_json::from_str::<StepOutcome>(r#"{"failed":{"reason":"the same explanation","stage":null}}"#).unwrap(), StepOutcome::failed("the same explanation"));
    }
}

#[cfg(test)]
mod ended_fold_cells {
    use super::*;
    use crate::events_storage::{EndedSection, EndedState, Event, EventSource};

    fn accepted() -> BuildAccepted {
        BuildAccepted { build_id: BuildId::mint(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::mint()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 1 }
    }

    fn by() -> EventSource {
        EventSource { fabric_id: rdm_core::FabricId::mint(), node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() }
    }

    /// CONTRACT (build.md §5: the Build's state is "from `rdm.build.started` and `rdm.build.ended`";
    /// Q346: a Build ends `complete`, `incomplete` or `failed`): each `rdm.build.ended` folds to its state,
    /// `incomplete` naming each section it did not finish and `failed` why it could not start; a later
    /// `rdm.build.started` (a resubmit) makes the Build running again and clears the last run's end, and a
    /// step whose event ended `complete` stands across the runs. An ended event of another Build folds
    /// nothing. Must NOT happen: a resubmitted Build read as still incomplete, or a completed step lost.
    #[test]
    fn each_build_ended_folds_to_its_state_and_a_resubmit_runs_from_the_steps_that_stand() {
        let a = accepted();
        let id = a.build_id.clone();
        let fp = by();
        let short = EndedState::Incomplete { sections: vec![EndedSection { mesh: "mesh2".into(), op: "node.create:000000000002".into(), step: "WaitForBind".into(), reason: "did not report where it bound".into(), code: "UNAVAILABLE".into() }] };
        let step = |at: u64, state: &crate::events_storage::StepState| Event::build_step(&fp, at, None, &id, "mesh1", "Complete", "node.create:000000000001", state);
        let mut events = vec![Event::build_started(&fp, 1, &id), step(2, &crate::events_storage::StepState::Complete), Event::build_ended(&fp, 3, &id, &short)];
        let p = BuildProjection::of(&a, &events);
        assert_eq!(p.state, BuildState::Incomplete);
        assert!(p.state.ended_short(), "an incomplete Build is skipped by the current Build's read");
        assert!(matches!(p.unfinished.as_slice(), [u] if u.mesh == "mesh2" && u.step == "WaitForBind" && u.code == rdm_core::outcome::Code::Unavailable), "{:?}", p.unfinished);
        assert_eq!(current_of(&p), None, "the current Build's read skips it");
        // Another Build's end folds nothing into this one.
        let other = BuildId::mint();
        events.push(Event::build_ended(&fp, 4, &other, &EndedState::Complete));
        assert_eq!(BuildProjection::of(&a, &events).state, BuildState::Incomplete);
        // The resubmit: running again, the last run's end cleared, the completed step standing.
        events.push(Event::build_started(&fp, 5, &id));
        let p = BuildProjection::of(&a, &events);
        assert_eq!((p.state, p.unfinished.len(), p.failure.as_ref()), (BuildState::Running, 0, None), "the resubmit runs: {p:?}");
        assert!(p.completed("node.create:000000000001", "Complete"), "the step that ended complete stands: {:?}", p.steps);
        events.push(Event::build_ended(&fp, 6, &id, &EndedState::Complete));
        assert_eq!(BuildProjection::of(&a, &events).state, BuildState::Complete);
        // A Build that could not start ends failed, naming why.
        let b = accepted();
        let failed = vec![Event::build_ended(&fp, 1, &b.build_id, &EndedState::Failed { reason: "interrupted by a fabric.delete".into(), code: "ABORTED".into() })];
        let p = BuildProjection::of(&b, &failed);
        assert_eq!(p.state, BuildState::Failed);
        assert_eq!(p.failure, Some(BuildFailure { reason: "interrupted by a fabric.delete".into(), code: "ABORTED".into() }));
    }
}

#[cfg(all(test, feature = "file-backend"))]
mod store_layout_cells {
    use super::*;

    /// CONTRACT (build.md §5.3; R-SR1; Q365): one Build record's line in the store root's `builds.jsonl`
    /// is frozen byte for byte: a CloudEvents event whose `source` is its writer, `type` `build-record`,
    /// `dataschema` `build-record/3`, `subject` its Build's id and `data` the record `{"seq", "fact"}`
    /// as the journal writes it, its `id` and `time` the write's. A layout that moves moves this cell
    /// with it (`durable format moved`). Must NOT happen: a line whose attributes or record bytes moved
    /// while this cell still passes.
    #[tokio::test]
    async fn the_store_layout_is_frozen_byte_for_byte() {
        let d = std::env::temp_dir().join(format!("rdm-store-layout-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&d).unwrap();
        let root = crate::store_root::StoreRoot::at(&d);
        let events: Arc<dyn EventsStorage> = Arc::new(crate::events_storage::MemoryEventsStorage::new());
        let writer = crate::store_log::LogWriter::mint();
        let j = FileJournal::open_on_root(&root, &writer, events).await.unwrap();
        let b1 = rdm_core::fixture_crockford60("b1");
        j.forget(&BuildId::parse(&b1).unwrap()).await.unwrap();
        let file = d.join(crate::store_root::BUILDS_FILE);
        let lines = crate::store_log::read_file(&file).unwrap();
        assert_eq!(lines.len(), 1);
        let written = &lines[0].attributes;
        let attributes = crate::store_log::Attributes { id: written.id.clone(), source: writer.source(), r#type: "build-record".into(), subject: Some(b1.clone()), time: None, dataschema: "build-record/3".into(), traceparent: None, tracestate: None };
        let record = format!(r#"{{"seq":1,"fact":{{"fact":"forget","build_id":"{b1}"}}}}"#);
        let line = crate::store_log::encode_line(&attributes, record.as_bytes());
        assert_eq!(std::fs::read(&file).unwrap(), line);
        let _ = std::fs::remove_dir_all(&d);
    }
}
