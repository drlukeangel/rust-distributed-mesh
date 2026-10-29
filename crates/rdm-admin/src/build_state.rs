//! `BuildStateAdapter`: where Build intent and evidence live (PRD §1.5–6, §7;
//! mesh-control-plane.md §4).
//!
//! The vocabulary is append/fold: an intent fact is put once, an attempt is
//! taken by an insert-and-fail claim on `(build_id, attempt)`, step and attempt
//! receipts are appended, and the current Build view is a fold over the facts.
//! Nothing is read-modify-written: two executors claiming the same attempt
//! cannot both win, and the current attempt is the folded maximum winning claim. The log that
//! decides a claim is the fabric-primary's (`build_claim`); every other admin records the claim
//! the fabric-primary decided (`adopt_claim`) and never decides one from its own log.
//!
//! RDM adapters:
//! - [`MemoryBuildStateAdapter`]: the in-memory fact log that backs the live
//!   fabric control projection;
//! - [`FileJournal`]: the file adapter's journal on the fabric's shared store
//!   root, its `builds.jsonl` (R-BI6, R-SR1): one line per fact, written by the
//!   fabric-primary for a topology attempt and by a mesh's primary for its drift
//!   attempt, the mesh in the key (R-BI7), so a successor reads it there.

use crate::accepted::{AttemptAction, FabricTopology, TopologyChange};
use crate::build::BuildId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[cfg(feature = "file-backend")]
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// A Build accepted by the fabric-primary: the complete topology it realizes, and the change that
/// produced it (history only; planning reads `topology`, never `submitted_change`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildAccepted {
    /// The Build's id.
    pub build_id: BuildId,
    /// The complete topology the Build realizes.
    pub topology: FabricTopology,
    /// The change that produced the topology: history only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_change: Option<TopologyChange>,
    /// When the Build was accepted, in milliseconds since the Unix epoch.
    pub submitted_at_ms: u64,
}

/// Why an attempt of a Build was opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AttemptReason {
    /// The first attempt: the Build was just accepted.
    Requested,
    /// A birth the topology names was proven exited; the same Build repairs it.
    ProvenDrift,
    /// Reserved position: the removed node.restart's reason (Q183; callers run node.stop then
    /// node.start). Never opened; kept so every later reason keeps its position.
    Reserved2,
    /// The previous attempt's executor is gone; another admin continues the Build.
    #[serde(rename = "authority-moved")] PrimaryMoved,
    /// `POST /api/nodes/{name}/connections/delete`: node.connection.delete { All }.
    DeleteConnections,
    /// A standalone stop of an exact birth was requested (`node.stop:<path>`).
    Stop,
    /// A standalone start of a stopped birth was requested (`node.start:<path>`).
    Start,
    /// A mesh-primary proved a member's exact birth exited and submitted its recovery
    /// (`node.delete:<path>`, then `node.create:<path>`, node-recovery.md "Steps").
    Recovery,
    /// The delete of a stopped birth at its `.old` path, or of a deleted mesh's last admin, was
    /// requested (`node.delete:<path>`, node.delete's one key).
    Delete,
    /// Steps were submitted: one Build of node primitives (build.md §1). Its journal name stays
    /// `recipe`, the name `builds.jsonl` already holds (Q211 renamed the variant only, as `roll` before it).
    #[serde(rename = "recipe")]
    Steps,
    /// An adoption launch opened it (Q156; fabric-recovery.md "Nothing alive"): the fabric-recovery attempt
    /// a node-admin a person launched at the lost fabric-primary's path.name claims, `node.delete:<path>`
    /// then `node.create:<path>` as a recovery's. Abandoned because a live admin answered, it owes no exit
    /// (node-recovery.md "Failure and receipts", Q159).
    Adoption,
}

impl AttemptReason {
    /// The reason's name as it appears in views and spans.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::ProvenDrift => "proven-drift",
            Self::Reserved2 => "reserved-2",
            Self::PrimaryMoved => "primary-moved",
            Self::DeleteConnections => "node.connection.delete",
            Self::Stop => "stop",
            Self::Start => "start",
            Self::Recovery => "recovery",
            Self::Delete => "delete",
            Self::Steps => "steps",
            Self::Adoption => "adoption",
        }
    }
}

/// An attempt opened on a Build by the authority (the first one by acceptance itself): what it
/// is for, and the targeted action it carries. Insert-and-fail on `(build_id, attempt)`: two
/// authorities proving the same drift open one attempt, never two.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptOpened {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt number.
    pub attempt: u32,
    /// Why the attempt was opened.
    pub reason: AttemptReason,
    /// The targeted action the attempt carries, when it carries one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<AttemptAction>,
    /// The opener's `path.name`.
    pub opened_by: String,
    /// When the attempt was opened, in milliseconds since the Unix epoch.
    pub opened_at_ms: u64,
    /// The lane the attempt is numbered in (R-BI7, proposal Part C caveat 2): `None` the fabric lane
    /// (a topology Build's attempts and the fabric-primary's workflow attempts, numbered across the
    /// Build); `Some(mesh)` that mesh's drift lane (its mesh-primary's attempts, numbered per Build
    /// and mesh from 1). A line written before lanes existed carries none: the fabric lane.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh: Option<String>,
    /// The opening request's call context, written with the open in this one record (build.md §4.4:
    /// a refused open writes nothing): the context the attempt's claim takes, so no claim of it ever
    /// reads a context written after its open. It is written to the journal with the open; a Build
    /// message never carries it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<rdm_core::context::CallContext>,
}

/// A claim of one attempt by an executor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildAttemptClaim {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt claimed.
    pub attempt: u32,
    /// The claiming executor (a node-admin's path.name).
    pub executor: String,
    /// The admin that decided the claim (a node-admin's path.name): the fabric-primary for a fabric-lane
    /// attempt, the mesh's own mesh-primary for a drift attempt. What a refusal names as the holder of
    /// the attempt's mesh, as the claim recorded it (never re-read from a later seat view). A line
    /// written before claimants were recorded carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimant: Option<String>,
    /// The lane the attempt is numbered in (R-BI7, proposal Part C caveat 2): `None` the fabric lane
    /// (a topology Build's attempts and the fabric-primary's workflow attempts, numbered across the
    /// Build); `Some(mesh)` that mesh's drift lane (its mesh-primary's attempts, numbered per Build
    /// and mesh from 1). A line written before lanes existed carries none: the fabric lane.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh: Option<String>,
}

/// The receipt of one step of an attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildStepReceipt {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
    /// The operation's idempotency key (`node.create:mesh1.rpc.2`, ...).
    pub operation: String,
    /// The step.
    pub step: String,
    /// How the step ended.
    pub outcome: StepOutcome,
    /// What a completed step decided (ids, endpoints, the runtime handle), so
    /// a re-run reuses it instead of deciding again. Absent on older lines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    /// The executing admin that decided this step: the endpoint (`key@addr`) every launch it made
    /// carries as its seed. A re-run by another executor reads which of these decisions name a
    /// lost admin. Absent on lines written by an executor that did not record it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    /// The lane of the attempt the step belongs to (`None` the fabric lane, `Some(mesh)` that mesh's
    /// drift lane; see [`AttemptOpened::mesh`]). A line written before lanes existed carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh: Option<String>,
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
}

/// The receipt of an attempt's end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildAttemptReceipt {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
    /// How the attempt ended.
    pub outcome: AttemptOutcome,
    /// The lane of the attempt (`None` the fabric lane, `Some(mesh)` that mesh's drift lane; see
    /// [`AttemptOpened::mesh`]). A line written before lanes existed carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh: Option<String>,
}

/// How an attempt ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcome {
    /// Desired state reached: the Build is complete.
    Converged,
    /// This attempt stopped; a later attempt may continue the same Build.
    Failed {
        /// Why the attempt stopped.
        reason: String,
        /// The standard code captured where the failure occurred.
        #[serde(default = "rdm_core::outcome::legacy_failed_code")]
        code: rdm_core::outcome::Code,
    },
    /// This attempt ran what its executor was eligible for and stopped at an
    /// operation another admin executes (`to`); that admin claims the next
    /// attempt of the same Build.
    HandedOff {
        /// The admin that executes the operation the attempt stopped at.
        to: String,
    },
}

/// One appended fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "fact", rename_all = "snake_case")]
pub enum BuildFact {
    /// The Build was accepted.
    Accepted(BuildAccepted),
    /// An attempt was opened.
    Opened(AttemptOpened),
    /// An attempt was claimed.
    Claim(BuildAttemptClaim),
    /// A step receipt was appended.
    Step(BuildStepReceipt),
    /// An attempt receipt was appended.
    Attempt(BuildAttemptReceipt),
    /// Build-history administration (`DELETE /api/builds?id=`): a finished
    /// Build leaves the views. Never a topology mutation.
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
            Self::Opened(f) => &f.build_id,
            Self::Claim(f) => &f.build_id,
            Self::Step(f) => &f.build_id,
            Self::Attempt(f) => &f.build_id,
            Self::Forget { build_id } => build_id,
        }
    }
}

/// Where a Build is in its life (`docs/architecture/build.md` §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    /// No executor is running an attempt: the Build is accepted, or its next attempt is open.
    ///
    /// Serialized as `pending`.
    Pending,
    /// An executor holds and runs an attempt.
    ///
    /// Serialized as `running`.
    Running,
    /// Every step is complete.
    ///
    /// Serialized as `complete`.
    Complete,
    /// The last attempt failed.
    ///
    /// Serialized as `failed`.
    Failed,
}

/// The folded Build view (`docs/architecture/build.md` §5.2–§5.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildProjection {
    /// The Build.
    pub build_id: BuildId,
    /// The complete topology the Build realizes.
    pub topology: FabricTopology,
    /// The change that produced the topology: history only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_change: Option<TopologyChange>,
    /// When the Build was accepted, in milliseconds since the Unix epoch.
    pub submitted_at_ms: u64,
    /// Where the Build is in its life.
    pub state: BuildState,
    /// The current attempt: the highest attempt claimed. An opened attempt is `attempt + 1`.
    pub attempt: u32,
    /// The executor of the current attempt, when one is.
    pub executor: Option<String>,
    /// The step receipts of the Build.
    pub steps: Vec<BuildStepReceipt>,
    /// The reason the last attempt failed, when one did.
    pub last_failure: Option<String>,
    /// Why the current attempt exists, and what it carries.
    pub reason: AttemptReason,
    /// The targeted action the current attempt carries, when it carries one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<AttemptAction>,
    /// The targeted action each claimed attempt carried, by attempt (from its `Opened` fact). A
    /// receipt names its attempt; this names the birth that attempt acted on, so a standalone
    /// operation's `Complete` needs no output of its own to say which birth it completed for.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attempt_actions: BTreeMap<u32, AttemptAction>,
    /// The executor each claimed attempt was won for, by attempt (from its `Claim` fact): the
    /// admin a later attempt follows.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attempt_executors: BTreeMap<u32, String>,
    /// The attempt the latest `Opened` fact opened (0 while none did: the first attempt follows the
    /// acceptance): the first attempt of the Build's current run, which the attempts after it
    /// continue by hand-off or take-over. A step receipt from it on is this run's.
    #[serde(default)]
    pub opened: u32,
    /// The attempt whose `Failed` verdict last failed the Build, when one did: the attempt after it,
    /// which a caller's resubmit opens (R-BI3), begins a new run ([`Self::run_of`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<u32>,
    /// The attempt whose `Converged` verdict first completed the Build, when one did. Kept when a
    /// later attempt reopens it (a stop, a start, a recovery, proven drift: attempts of an unchanged
    /// topology, build.md §4.1), so a Build that once realized its topology stays the known state
    /// whatever such an attempt ends with ([`CurrentBuild`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u32>,
    /// The attempts whose executor reported its run's end by the attempt's verdict (`HandedOff` or
    /// `Converged`): what such a run completed is done. A run whose executor was lost reported nothing,
    /// and its successor re-checks the world for what it left.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub reported: std::collections::BTreeSet<u32>,
    /// The admin that decided the current fabric-section attempt's claim, as its claim recorded it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimant: Option<String>,
    /// Each mesh's per-mesh drift section, by mesh (`docs/architecture/build.md` §5.3): the attempts
    /// its mesh-primary opened, claimed and ended on this Build, numbered per mesh from 1. The fields
    /// above are the fabric section's alone, and nothing of a per-mesh drift section moves them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub lanes: BTreeMap<String, Lane>,
}

/// One mesh's drift lane of a Build (R-BI7): its mesh-primary's attempts of the Build inside its
/// mesh, folded from the facts whose `mesh` names it, with the fabric lane's rules (an open moves the
/// lane to its next attempt, the first claim of that attempt runs it, the attempt's first verdict
/// ends it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lane {
    /// Where the lane's current attempt is.
    pub state: BuildState,
    /// The lane's current attempt: the highest claimed. An opened attempt is `attempt + 1`.
    pub attempt: u32,
    /// The executor of the current attempt, when one is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    /// The admin that decided the current attempt's claim (the mesh's mesh-primary).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimant: Option<String>,
    /// Why the current attempt exists.
    pub reason: AttemptReason,
    /// The action the current attempt carries, targeted at one exact birth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<AttemptAction>,
    /// The attempt the latest open opened: the first attempt of the lane's current run.
    pub opened: u32,
    /// The reason the lane's last attempt failed, when one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<String>,
    /// The step receipts of the lane's attempts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<BuildStepReceipt>,
    /// The attempts whose executor reported its run's end (`HandedOff` or `Converged`).
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub reported: std::collections::BTreeSet<u32>,
    /// The action each claimed attempt carried, by attempt.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attempt_actions: BTreeMap<u32, AttemptAction>,
    /// The fabric lane's attempt when the lane's current attempt was claimed (folded in journal order):
    /// where the mesh's own work in its lane stands among the fabric lane's attempts (Q250 sections).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub at_fabric_attempt: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl Lane {
    /// Whether the lane holds an attempt open: opened and not yet claimed, or claimed and running.
    pub fn is_open(&self) -> bool {
        matches!(self.state, BuildState::Pending | BuildState::Running)
    }
}

impl BuildProjection {
    /// Whether the change that produced this Build is fabric.delete.
    pub fn deletes_fabric(&self) -> bool {
        matches!(self.submitted_change, Some(TopologyChange::DeleteFabric))
    }

    /// Whether any mesh's drift lane holds an attempt open.
    pub fn lane_open(&self) -> bool {
        self.lanes.values().any(Lane::is_open)
    }

    /// The Build as an attempt of `lane` reads it (R-BI7): `None` the fabric lane, the Build itself;
    /// `Some(mesh)` that mesh's drift lane, whose state, attempt, executor, claimant, reason, action,
    /// steps and verdicts stand where the fabric lane's do, so the executor, its pipelines and the
    /// plan read the lane's own numbering and receipts and never the fabric lane's. The accepted
    /// topology and the Build's identity are the Build's. A lane never opened reads as no attempt yet.
    pub fn in_lane(&self, lane: Option<&str>) -> BuildProjection {
        let Some(mesh) = lane else { return self.clone() };
        let mut p = self.clone();
        let l = self.lanes.get(mesh).cloned().unwrap_or(Lane {
            state: BuildState::Complete,
            attempt: 0,
            executor: None,
            claimant: None,
            reason: AttemptReason::Recovery,
            action: None,
            opened: 0,
            last_failure: None,
            steps: Vec::new(),
            reported: Default::default(),
            attempt_actions: BTreeMap::new(),
            at_fabric_attempt: 0,
        });
        p.state = l.state;
        p.attempt = l.attempt;
        p.attempt_executors = l.executor.iter().map(|e| (l.attempt, e.clone())).collect();
        p.executor = l.executor;
        p.claimant = l.claimant;
        p.reason = l.reason;
        p.action = l.action;
        p.opened = l.opened;
        p.last_failure = l.last_failure;
        p.steps = l.steps;
        p.reported = l.reported;
        p.attempt_actions = l.attempt_actions;
        p.completed_at = (l.state == BuildState::Complete && l.attempt > 0).then_some(l.attempt);
        // A drift lane numbers its attempts per mesh: the fabric lane's failed attempt is not one of them.
        p.failed_at = None;
        p
    }

    /// The attempt a drive of this Build covers as it reads now: the current attempt while it runs or
    /// once it completed; the next one while it waits for its claim (an attempt opened on it, a
    /// complete Build reopened included, or the hand-off of the current one) or after it failed.
    pub fn attempt_to_drive(&self) -> u32 {
        match self.state {
            BuildState::Running | BuildState::Complete => self.attempt,
            BuildState::Pending | BuildState::Failed => self.attempt + 1,
        }
    }

    /// The first attempt of the run `attempt` belongs to (ruling R-PU2): the attempt the latest
    /// `Opened` fact opened ([`Self::opened`]), or the first attempt of the Build. A seat update's
    /// transfer key is `(build_id, run)` ([`crate::fabric_handover::update_operation`]): the attempts
    /// of one run (hand-offs and take-overs) share it and replay its row; a new run makes a new one.
    pub fn run_of(&self, attempt: u32) -> u32 {
        // The attempt after a Failed verdict begins a new run: only a caller's resubmit drives a failed
        // Build on (R-BI3), and that is never the continuation of the failed run.
        let after_failure = self.failed_at.filter(|f| *f < attempt).map_or(1, |f| f + 1);
        self.opened.max(after_failure)
    }

    /// Whether `attempt` carried exactly `action`.
    pub fn carried(&self, attempt: u32, action: &AttemptAction) -> bool {
        self.attempt_actions.get(&attempt) == Some(action)
    }

    /// Whether `attempt` belongs to the Build's current run (from [`Self::opened`] on) and its executor
    /// reported the run's end ([`Self::reported`]).
    pub fn reported_in_run(&self, attempt: u32) -> bool {
        attempt >= self.opened && self.reported.contains(&attempt)
    }

    /// Whether the operation `key` completed in the Build's current run: its `Complete` receipt in an
    /// attempt of the run whose executor reported the run's end ([`Self::reported_in_run`]). A plan
    /// read from a view that has not absorbed the operation yet still lists it; the receipt says it is
    /// done.
    pub fn completed_in_run(&self, key: &str) -> bool {
        self.steps.iter().any(|s| self.reported_in_run(s.attempt) && s.operation == key && s.step == "Complete" && s.outcome == StepOutcome::Complete)
    }
}

/// The current Build (R-BI3, rulings 2026-10-12): the Build entities read last in, first out through
/// the acceptance-order index, the first that is in progress or completed. A failed Build is skipped
/// and never current; nothing resumes it on its own, and a caller resubmits it. No row points at it:
/// the Fabric row carries no pointer to the newest Build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentBuild {
    /// The newest Build not failed is being driven (an attempt open or running): it is resumed, its
    /// completed steps replayed and never run again.
    InProgress(BuildId),
    /// The newest Build not failed completed (it realized its topology once): the known state.
    Completed(BuildId),
    /// No Build held is in progress or completed.
    None,
}

impl CurrentBuild {
    /// The current Build's id, when there is one.
    pub fn build_id(&self) -> Option<&BuildId> {
        match self {
            Self::InProgress(id) | Self::Completed(id) => Some(id),
            Self::None => None,
        }
    }

    /// The current Build's id, when there is one.
    pub fn into_build_id(self) -> Option<BuildId> {
        match self {
            Self::InProgress(id) | Self::Completed(id) => Some(id),
            Self::None => None,
        }
    }
}

/// What a read of `p`, one entry of the acceptance-order index, makes of it: in progress, completed,
/// or skipped (`None`: failed, and it never completed).
pub(crate) fn current_of(p: &BuildProjection) -> Option<CurrentBuild> {
    // A drift lane's open attempt keeps its Build in progress (R-BI7 caveat 5: the LIFO read is
    // otherwise untouched).
    if p.lane_open() {
        return Some(CurrentBuild::InProgress(p.build_id.clone()));
    }
    match p.state {
        BuildState::Pending | BuildState::Running => Some(CurrentBuild::InProgress(p.build_id.clone())),
        BuildState::Complete => Some(CurrentBuild::Completed(p.build_id.clone())),
        BuildState::Failed if p.completed_at.is_some() => Some(CurrentBuild::Completed(p.build_id.clone())),
        BuildState::Failed => None,
    }
}

/// Whether `p` may be driven now: it is the current Build, or it is a failed Build accepted after the
/// current one (each Build between them failed too) while the current one is not in progress. The
/// resubmit that resumes it opens its next attempt, which makes it the current Build (R-BI3: "a
/// caller resubmits it"); one Build is in flight at a time.
pub async fn drivable(builds: &dyn BuildStateAdapter, p: &BuildProjection) -> Result<bool, BuildStateError> {
    let current = builds.current_build().await?;
    if current.build_id() == Some(&p.build_id) {
        return Ok(true);
    }
    if p.state != BuildState::Failed || p.completed_at.is_some() {
        return Ok(false);
    }
    Ok(match current {
        CurrentBuild::InProgress(_) => false,
        CurrentBuild::None => true,
        CurrentBuild::Completed(id) => {
            let c = builds.read_build(&id).await?;
            (p.submitted_at_ms, &p.build_id) > (c.submitted_at_ms, &c.build_id)
        }
    })
}

/// The fabric.delete that interrupts `p` (build.md §1: "The one exception is fabric.delete, which may
/// interrupt the Build in progress"): the current Build, when it is a fabric.delete accepted after `p`.
/// Its acceptance record is the durable intent: from it on, `p` opens no further operation and is never
/// resumed. `None` while `p` is the current Build, or the current Build is no fabric.delete accepted
/// after it.
pub async fn interrupted_by(builds: &dyn BuildStateAdapter, p: &BuildProjection) -> Result<Option<BuildProjection>, BuildStateError> {
    let Some(id) = builds.current_build().await?.into_build_id() else { return Ok(None) };
    if id == p.build_id {
        return Ok(None);
    }
    let current = builds.read_build(&id).await?;
    Ok((current.deletes_fabric() && (current.submitted_at_ms, &current.build_id) > (p.submitted_at_ms, &p.build_id)).then_some(current))
}

/// The fabric.delete in force (build.md §1: a successor fabric-primary reads the delete as the current
/// Build and continues the teardown): the current Build when it is a fabric.delete, or else the newest
/// Build accepted when it is a fabric.delete whose attempt failed, its intent retained (a failed Build is
/// never current, R-BI3). With the delete, `true` when its attempt failed. `None`: no fabric.delete is in
/// force.
pub async fn fabric_delete_in_force(builds: &dyn BuildStateAdapter) -> Result<Option<(BuildProjection, bool)>, BuildStateError> {
    let current = builds.current().await?;
    if let Some(c) = current.as_ref().filter(|c| c.deletes_fabric()) {
        return Ok(Some((c.clone(), false)));
    }
    let Some(newest) = builds.newest_accepted_at_ms().await? else { return Ok(None) };
    if current.as_ref().is_some_and(|c| c.submitted_at_ms >= newest) {
        return Ok(None);
    }
    let facts = builds.facts().await?;
    let Some(id) = facts.iter().rev().find_map(|f| match f {
        BuildFact::Accepted(a) if a.submitted_at_ms == newest && matches!(a.submitted_change, Some(TopologyChange::DeleteFabric)) => Some(a.build_id.clone()),
        _ => None,
    }) else {
        return Ok(None);
    };
    let p = builds.read_build(&id).await?;
    Ok((p.state == BuildState::Failed).then_some((p, true)))
}

/// Why the run of `interrupted` ends: the fabric.delete `delete` accepted after it (build.md §1).
pub fn interrupted_reason(interrupted: &BuildId, delete: &BuildId) -> String {
    format!("Build {interrupted} is interrupted by fabric.delete Build {delete}, accepted after it: its open attempts have ended, it opens no further operation and is never resumed")
}

/// Accept `accepted` (build.md §5.2, acceptance): the Build entity is written once into the builds
/// store, and it is current from the moment it is there (an index read finds it newest), so nothing
/// is moved after it and nothing can read it as current before it is durable. `previous` is the Build
/// that was current, which the span names; `via` names the surface (`accepted`, `day-0`, `seeded`).
pub async fn accept(builds: &dyn BuildStateAdapter, accepted: &BuildAccepted, previous: Option<&BuildId>, node: &str, via: &str) -> Result<(), BuildStateError> {
    use tracing::Instrument as _;
    // The span opens BEFORE the write: the Build is current to a reader the moment it is in the
    // store, which is before `publish_accepted` returns.
    let span = tracing::info_span!(
        "rdm.node_admin.build.create.via-accepted",
        node = %node,
        build_id = %accepted.build_id.log(),
        previous_build_id = %previous.map(|b| b.as_str()).unwrap_or(""),
        submitted_at_ms = accepted.submitted_at_ms,
        via,
    );
    let put = builds.publish_accepted(accepted).instrument(span.clone()).await;
    match &put {
        Ok(()) => span.in_scope(|| tracing::info!("the Build is in the builds store: it is the newest accepted, and the current Build")),
        Err(e) => span.in_scope(|| tracing::info!(error = %e, "the Build could not be written to the builds store: nothing is accepted")),
    }
    put
}

/// The result of claiming an attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The attempt is the claimant's.
    Won,
    /// The attempt is already claimed by `holder`.
    Lost {
        /// The executor holding the attempt.
        holder: String,
    },
    /// Not the Build's next attempt, or the Build is complete and no attempt was opened on it:
    /// nothing to run, nothing recorded.
    NotOpen {
        /// The attempt that is open, when one is.
        next: Option<u32>,
    },
}

/// What a claim decided with its mesh's in-flight row came to ([`BuildStateAdapter::claim_in_flight`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InFlightClaim {
    /// Another attempt's row holds the mesh: nothing was written.
    Held(crate::mesh_claims::InFlight),
    /// The row names this attempt, and the claim was decided.
    Decided(ClaimOutcome),
}

/// Why a Build state operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildStateError {
    /// No fact of the Build is held.
    UnknownBuild(BuildId),
    /// An intent was published twice for one id with different content.
    ConflictingIntent(BuildId),
    /// The journal could not be read or written.
    Io(String),
    /// A fact that has no wire shape (a step output of a step that commits none, or of another
    /// type than its step's): named with the Build, attempt and step.
    Unencodable(String),
    /// The attempt number is held by another opener's action: `held` is what the number carries,
    /// `wanted` what this open asked for. The number is never shared by two actions.
    AttemptTaken {
        /// The Build.
        build_id: BuildId,
        /// The attempt number.
        attempt: u32,
        /// What the number carries.
        held: String,
        /// What this open asked for.
        wanted: String,
    },
    /// The fabric-primary record this admin holds names another live birth: this admin's Build log
    /// decides nothing from that record's revision on (the target stays set for this birth).
    SeatYielded {
        /// The admin whose Build log yielded the seat.
        node: String,
        /// The record that moved the seat, in words.
        by: String,
        /// The admin (path.name) the record names, when a digest of it is held.
        to: Option<String>,
    },
}

impl std::fmt::Display for BuildStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownBuild(id) => write!(f, "no Build {id}"),
            Self::ConflictingIntent(id) => write!(f, "Build {id} was published with a different intent"),
            Self::Io(e) => write!(f, "build journal I/O: {e}"),
            Self::Unencodable(e) => write!(f, "Build fact cannot be sent: {e}"),
            Self::SeatYielded { node, by, .. } => write!(f, "{node} is no longer the fabric-primary: {by}; its Build log decides nothing any more"),
            Self::AttemptTaken { build_id, attempt, held, wanted } => {
                write!(f, "attempt {attempt} of Build {build_id} is already open for {held}; this open asked for {wanted} and lost the number")
            }
        }
    }
}

/// The step of the recorded transition under an in-force fabric.delete (fabric-recovery.md step 4, Q160):
/// the executor that won the delete's next attempt takes, at the next revision, the fabric-primary role
/// and the lost mesh's mesh-primary role, keeping its own mesh's. Its receipt, in the attempt it won, is
/// the record every reader folds by.
pub const STEP_PRIMARIES_TAKEN: &str = "PrimariesTaken";

/// The operation a [`PrimariesTaken`] receipt is recorded under: fabric recovery of the lost birth.
pub fn primaries_taken_operation(lost: &crate::model::NodeId) -> String {
    format!("fabric-recovery:{lost}")
}

/// The recorded transition under an in-force fabric.delete (Q160): who lost the roles, who took them at
/// which revisions, and the attempt the taking supersedes with the receipts of it held then. A receipt of
/// the superseded attempt not among `held` was written by a writer that had not yet learned the transfer:
/// every reader ignores it (Q109's fold).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimariesTaken {
    /// The lost fabric-primary's birth.
    pub lost_node_id: crate::model::NodeId,
    /// The lost fabric-primary's incarnation.
    pub lost_incarnation: crate::model::IncarnationId,
    /// The executor that took the roles: its path.name and birth.
    pub executor: String,
    /// The executor's node id.
    pub executor_node_id: crate::model::NodeId,
    /// The executor's incarnation.
    pub executor_incarnation: crate::model::IncarnationId,
    /// The fabric-primary record's revision the executor holds from the transition.
    pub fabric_revision: u64,
    /// The lost mesh, and the revision of its mesh-primary record the executor holds from the transition.
    pub mesh: String,
    /// The revision of the lost mesh's mesh-primary record the executor holds from the transition.
    pub mesh_revision: u64,
    /// The attempt the transition supersedes: the delete's attempt the lost fabric-primary held.
    pub superseded_attempt: u32,
    /// The receipts of the superseded attempt the executor held at the transition, by [`receipt_key`].
    pub held: Vec<String>,
}

/// Every [`PrimariesTaken`] transition recorded in `steps`, in step order.
pub fn primaries_taken(steps: &[BuildStepReceipt]) -> Vec<PrimariesTaken> {
    steps
        .iter()
        .filter(|r| r.mesh.is_none() && r.step == STEP_PRIMARIES_TAKEN && r.outcome == StepOutcome::Complete)
        .filter_map(|r| r.output.clone().and_then(|v| serde_json::from_value::<PrimariesTaken>(v).ok()))
        .collect()
}

/// A step receipt's key within its attempt: its operation and step.
pub fn receipt_key(r: &BuildStepReceipt) -> String {
    format!("{} {}", r.operation, r.step)
}

/// Fold an ordered fact log into Build views (`docs/architecture/build.md` §5.2–§5.3). Deterministic
/// and idempotent: the same log gives the same views, and a duplicated fact changes nothing. The
/// winning claim of an attempt is the first one appended. A fabric-section receipt of an attempt a
/// recorded transition supersedes ([`PrimariesTaken`], Q160) is ignored unless the transition held
/// it, and that attempt's verdict decides nothing: the superseded writer wrote them after the
/// transfer.
pub fn fold(facts: &[BuildFact]) -> BTreeMap<BuildId, BuildProjection> {
    let superseded: BTreeMap<(BuildId, u32), std::collections::BTreeSet<String>> = facts
        .iter()
        .filter_map(|f| match f {
            BuildFact::Step(s) if s.mesh.is_none() && s.step == STEP_PRIMARIES_TAKEN => s
                .output
                .clone()
                .and_then(|v| serde_json::from_value::<PrimariesTaken>(v).ok())
                .filter(|t| t.superseded_attempt < s.attempt)
                .map(|t| ((s.build_id.clone(), t.superseded_attempt), t.held.into_iter().collect())),
            _ => None,
        })
        .collect();
    // Facts arrive in any order (gossip, catch-up); the fold reads them in their one logical order:
    // per Build, acceptance first, then by attempt with its open before its claim, steps and
    // verdict, and forget last. Arrival order breaks ties, so the first-appended claim still wins.
    let rank = |f: &BuildFact| -> (u32, u8) {
        match f {
            BuildFact::Accepted(_) => (0, 0),
            BuildFact::Opened(o) => (o.attempt, 1),
            BuildFact::Claim(c) => (c.attempt, 2),
            BuildFact::Step(s) => (s.attempt, 3),
            BuildFact::Attempt(a) => (a.attempt, 4),
            BuildFact::Forget { .. } => (u32::MAX, 5),
        }
    };
    let mut ordered: Vec<&BuildFact> = facts.iter().collect();
    ordered.sort_by(|a, b| a.build_id().cmp(b.build_id()).then(rank(a).cmp(&rank(b))));
    let mut out: BTreeMap<BuildId, BuildProjection> = BTreeMap::new();
    // Keyed by lane (`None` the fabric lane, `Some(mesh)` that mesh's drift lane, R-BI7): one
    // Build's attempt number names one attempt per lane.
    let mut winners: BTreeMap<(BuildId, Option<String>, u32), String> = BTreeMap::new();
    let mut opened: std::collections::BTreeSet<(BuildId, Option<String>, u32)> = std::collections::BTreeSet::new();
    // An attempt has one verdict: the first appended. A stale second receipt changes nothing.
    let mut decided: std::collections::BTreeSet<(BuildId, Option<String>, u32)> = std::collections::BTreeSet::new();
    for f in ordered {
        match f {
            // A Build's sections (Q284) fold into one topology; its whole acceptance replaces them.
            BuildFact::Accepted(i) if out.contains_key(&i.build_id) => {
                let p = out.get_mut(&i.build_id).expect("held");
                if p.topology.section_of.is_some() && i.topology.section_of.is_none() {
                    p.submitted_change = i.submitted_change.clone();
                }
                p.topology.absorb(&i.topology);
            }
            BuildFact::Accepted(i) => {
                out.entry(i.build_id.clone()).or_insert_with(|| BuildProjection {
                    build_id: i.build_id.clone(),
                    topology: i.topology.clone(),
                    submitted_change: i.submitted_change.clone(),
                    submitted_at_ms: i.submitted_at_ms,
                    state: BuildState::Pending,
                    attempt: 0,
                    executor: None,
                    steps: Vec::new(),
                    last_failure: None,
                    reason: AttemptReason::Requested,
                    action: None,
                    attempt_actions: BTreeMap::new(),
                    attempt_executors: BTreeMap::new(),
                    opened: 0,
                    failed_at: None,
                    completed_at: None,
                    reported: Default::default(),
                    claimant: None,
                    lanes: BTreeMap::new(),
                });
            }
            BuildFact::Opened(o) => {
                let key = (o.build_id.clone(), o.mesh.clone(), o.attempt);
                if opened.contains(&key) {
                    continue;
                }
                opened.insert(key);
                if let (Some(mesh), Some(p)) = (&o.mesh, out.get_mut(&o.build_id)) {
                    // A drift lane comes into being with its first open; it moves as the fabric lane does.
                    let lane = p.lanes.entry(mesh.clone()).or_insert_with(|| Lane {
                        state: BuildState::Complete,
                        attempt: 0,
                        executor: None,
                        claimant: None,
                        reason: o.reason,
                        action: None,
                        opened: 0,
                        last_failure: None,
                        steps: Vec::new(),
                        reported: Default::default(),
                        attempt_actions: BTreeMap::new(),
                        at_fabric_attempt: 0,
                    });
                    if o.attempt == lane.attempt + 1 && !matches!(lane.state, BuildState::Running) {
                        lane.state = BuildState::Pending;
                        lane.executor = None;
                        lane.claimant = None;
                        lane.reason = o.reason;
                        lane.action = o.action.clone();
                        lane.opened = o.attempt;
                    }
                } else if let Some(p) = out.get_mut(&o.build_id) {
                    // The authority opens the attempt after the current one, on a Build not in
                    // flight; it reopens a complete Build. The executor that leads then claims it.
                    if o.attempt == p.attempt + 1 && !matches!(p.state, BuildState::Running) {
                        p.state = BuildState::Pending;
                        p.executor = None;
                        p.reason = o.reason;
                        p.action = o.action.clone();
                        p.opened = o.attempt;
                    }
                }
            }
            BuildFact::Claim(c) => {
                let key = (c.build_id.clone(), c.mesh.clone(), c.attempt);
                if winners.contains_key(&key) {
                    continue;
                }
                winners.insert(key, c.executor.clone());
                if let Some(mesh) = &c.mesh {
                    // A drift lane's attempt exists only by its open: a claim of a lane never opened moves nothing.
                    let fabric_attempt = out.get(&c.build_id).map(|p| p.attempt).unwrap_or(0);
                    if let Some(lane) = out.get_mut(&c.build_id).and_then(|p| p.lanes.get_mut(mesh)) {
                        if c.attempt == lane.attempt + 1 && !matches!(lane.state, BuildState::Complete) {
                            lane.attempt = c.attempt;
                            lane.at_fabric_attempt = fabric_attempt;
                            lane.executor = Some(c.executor.clone());
                            lane.claimant = c.claimant.clone();
                            lane.state = BuildState::Running;
                            if let Some(a) = &lane.action {
                                lane.attempt_actions.insert(c.attempt, a.clone());
                            }
                        }
                    }
                } else if let Some(p) = out.get_mut(&c.build_id) {
                    // The next attempt only, and never of a complete Build: a complete Build runs
                    // again only through an opened attempt.
                    if c.attempt == p.attempt + 1 && !matches!(p.state, BuildState::Complete) {
                        p.attempt = c.attempt;
                        p.executor = Some(c.executor.clone());
                        p.claimant = c.claimant.clone();
                        p.attempt_executors.insert(c.attempt, c.executor.clone());
                        p.state = BuildState::Running;
                        if let Some(a) = &p.action {
                            p.attempt_actions.insert(c.attempt, a.clone());
                        }
                    }
                }
            }
            BuildFact::Step(s) => {
                // A receipt the superseded writer wrote after the recorded transition: ignored (Q160).
                if s.mesh.is_none() && superseded.get(&(s.build_id.clone(), s.attempt)).is_some_and(|held| !held.contains(&receipt_key(s))) {
                    continue;
                }
                // A drift lane's step is its lane's, never the fabric lane's: every reader of the
                // fabric lane's steps by attempt reads its own numbering only.
                let steps = match (&s.mesh, out.get_mut(&s.build_id)) {
                    (None, Some(p)) => Some(&mut p.steps),
                    (Some(mesh), Some(p)) => p.lanes.get_mut(mesh).map(|l| &mut l.steps),
                    (_, None) => None,
                };
                if let Some(steps) = steps {
                    if !steps.contains(s) {
                        steps.push(s.clone());
                    }
                }
            }
            BuildFact::Attempt(a) => {
                // The verdict of an attempt a recorded transition superseded decides nothing (Q160): its
                // writer had lost the roles.
                if a.mesh.is_none() && superseded.contains_key(&(a.build_id.clone(), a.attempt)) {
                    continue;
                }
                if !decided.insert((a.build_id.clone(), a.mesh.clone(), a.attempt)) {
                    continue; // the attempt already has its verdict
                }
                if let Some(mesh) = &a.mesh {
                    if let Some(lane) = out.get_mut(&a.build_id).and_then(|p| p.lanes.get_mut(mesh)) {
                        if matches!(a.outcome, AttemptOutcome::Converged | AttemptOutcome::HandedOff { .. }) {
                            lane.reported.insert(a.attempt);
                        }
                        if a.attempt != lane.attempt || lane.state == BuildState::Complete {
                            continue;
                        }
                        match &a.outcome {
                            AttemptOutcome::Converged => lane.state = BuildState::Complete,
                            AttemptOutcome::Failed { reason , .. } => {
                                lane.state = BuildState::Failed;
                                lane.last_failure = Some(reason.clone());
                            }
                            AttemptOutcome::HandedOff { .. } => lane.state = BuildState::Pending,
                        }
                    }
                } else if let Some(p) = out.get_mut(&a.build_id) {
                    if matches!(a.outcome, AttemptOutcome::Converged | AttemptOutcome::HandedOff { .. }) {
                        p.reported.insert(a.attempt);
                    }
                    if a.attempt != p.attempt || p.state == BuildState::Complete {
                        continue; // a superseded attempt's verdict does not decide the Build
                    }
                    match &a.outcome {
                        AttemptOutcome::Converged => {
                            p.state = BuildState::Complete;
                            p.completed_at.get_or_insert(a.attempt);
                        }
                        AttemptOutcome::Failed { reason , .. } => {
                            p.state = BuildState::Failed;
                            p.last_failure = Some(reason.clone());
                            p.failed_at = Some(a.attempt);
                        }
                        // No attempt is in flight: the Build waits for the next claim.
                        AttemptOutcome::HandedOff { .. } => p.state = BuildState::Pending,
                    }
                }
            }
            BuildFact::Forget { build_id } => {
                if out.get(build_id).is_some_and(|p| matches!(p.state, BuildState::Complete | BuildState::Failed) && !p.lane_open()) {
                    out.remove(build_id);
                }
            }
        }
    }
    out
}

/// The lifecycle operations open in `builds` (the fold of `facts`): every `NodeDeleting` step a
/// delete operation completed with no `NodeDeleted` step of the same operation after it whose attempt
/// has no terminal verdict. Derived from the Build facts as each fact enters the log, never stored: a
/// successor primary publishes the same overlays from the same facts.
pub(crate) fn in_flight_ops(facts: &[BuildFact], builds: &BTreeMap<BuildId, BuildProjection>) -> InFlight {
    let mut out = InFlight::default();
    // An attempt has one verdict, the first appended (as `fold` reads it), in its own lane: the
    // terminal verdicts by (Build, lane, attempt). A removal's close is handed on by the removal's own
    // key, decided here in its own lane.
    let mut decided: std::collections::BTreeSet<(&BuildId, Option<&str>, u32)> = std::collections::BTreeSet::new();
    let mut converged: std::collections::BTreeSet<(String, Option<String>, u32)> = std::collections::BTreeSet::new();
    let mut failed: std::collections::BTreeSet<(String, Option<String>, u32)> = std::collections::BTreeSet::new();
    for f in facts {
        if let BuildFact::Attempt(a) = f {
            if decided.insert((&a.build_id, a.mesh.as_deref(), a.attempt)) && builds.contains_key(&a.build_id) {
                let key = (a.build_id.to_string(), a.mesh.clone(), a.attempt);
                match a.outcome {
                    AttemptOutcome::Converged => {
                        converged.insert(key);
                    }
                    AttemptOutcome::Failed { .. } => {
                        failed.insert(key);
                    }
                    _ => {}
                }
            }
        }
    }
    for p in builds.values() {
        // The fabric lane's steps, then each drift lane's: a removal in any lane holds its overlay.
        for lane_steps in std::iter::once(&p.steps).chain(p.lanes.values().map(|l| &l.steps)) {
        for s in lane_steps {
            if s.outcome != StepOutcome::Complete || s.step != "NodeDeleting" {
                continue;
            }
            // A pre-event step this build cannot read is refused by name, never skipped as if it
            // carried no operation.
            let unrecognised = |reason: String| UnrecognisedStep { build_id: s.build_id.clone(), attempt: s.attempt, operation: s.operation.clone(), step: s.step.clone(), reason };
            let op = match s.output.as_ref() {
                None => {
                    out.unrecognised.push(unrecognised("the step recorded no operation".into()));
                    continue;
                }
                Some(v) => match serde_json::from_value::<rdm_core::LifecycleOp>(v.clone()) {
                    Ok(op) => op,
                    Err(e) => {
                        out.unrecognised.push(unrecognised(format!("its recorded operation is not a LifecycleOp this build reads: {e}")));
                        continue;
                    }
                },
            };
            // A removal's overlay is keyed by its attempt (gossip.md `NodeDeleting`): it ends with the
            // departure, or when that attempt's verdict is terminal (Converged or Failed: the attempt
            // no longer claims the operation).
            let verdict = (op.build_id.clone(), s.mesh.clone(), op.attempt);
            let by = if failed.contains(&verdict) {
                Some("attempt-failed")
            } else if converged.contains(&verdict) {
                Some("attempt-converged")
            } else {
                None
            };
            if let Some(by) = by {
                out.removals_ended.insert(op.key(), by);
            }
            let closed = lane_steps.iter().any(|t| t.step == "NodeDeleted" && t.operation == s.operation && t.outcome == StepOutcome::Complete) || by.is_some();
            if !closed {
                out.ops.push(op);
            }
        }
        }
    }
    out
}

/// The open lifecycle operations of the Build facts, and every pre-event step whose recorded
/// operation this build does not recognise.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InFlight {
    /// The operations in flight.
    pub ops: Vec<rdm_core::LifecycleOp>,
    /// Completed steps whose recorded operation cannot be read.
    pub unrecognised: Vec<UnrecognisedStep>,
    /// The removals (`NodeDeleting`) whose own attempt, in its own lane, has a terminal verdict, by
    /// the removal's key (`LifecycleOp::key`), each `attempt-failed` or `attempt-converged`: the
    /// facts end that hold.
    pub removals_ended: std::collections::BTreeMap<(String, u32, String), &'static str>,
}

/// The Builds the facts forgot: each a `Forget` fact names whose Build the fold no longer holds
/// (`fold` drops a Build on its `Forget` once it is complete or failed). An overlay of such a Build
/// is no longer named by any fact.
pub(crate) fn forgotten_builds(facts: &[BuildFact], builds: &BTreeMap<BuildId, BuildProjection>) -> std::collections::BTreeSet<String> {
    facts
        .iter()
        .filter_map(|f| match f {
            BuildFact::Forget { build_id } if !builds.contains_key(build_id) => Some(build_id.to_string()),
            _ => None,
        })
        .collect()
}

/// A completed `NodeDeleting` step whose recorded operation cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnrecognisedStep {
    /// The Build of the step.
    pub build_id: BuildId,
    /// The attempt of the step.
    pub attempt: u32,
    /// The recorded operation.
    pub operation: String,
    /// The step.
    pub step: String,
    /// Why the operation cannot be read.
    pub reason: String,
}

/// The durable record of Builds, attempts, claims and step receipts: appended facts and their
/// folded views.
#[async_trait]
pub trait BuildStateAdapter: Send + Sync {
    /// Record an accepted Build. Insert-and-fail on its id.
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError>;
    /// Open an attempt of a Build. Insert-and-fail on `(build_id, attempt)`: the first opener wins.
    /// A second open of the same attempt that asks for the same action (two authorities proving the
    /// same drift) changes nothing and succeeds; one that asks for another action is refused
    /// `AttemptTaken`, so no caller is told an attempt is open for an action it does not carry.
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError>;
    /// The folded view of `build_id`.
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError>;
    /// The current Build ([`CurrentBuild`], R-BI3): the acceptance-order index read last in, first
    /// out, each Build read from its own facts, until the first that is in progress or completed. A
    /// failed Build is skipped; no Build accepted before the current one is read. Every adapter
    /// answers it from its index (or delegates to the adapter it decorates).
    async fn current_build(&self) -> Result<CurrentBuild, BuildStateError>;
    /// The acceptance stamp (`submitted_at_ms`) of the newest Build accepted, whatever its state: the
    /// head of the acceptance-order index. A Build accepted next is stamped after it.
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError>;
    /// The current Build's folded view: [`Self::current_build`], then a point read of that Build.
    /// `Ok(None)`: no Build in the store is in progress or completed (none was ever accepted here, or
    /// every one failed). `Err`: the store could not be read, named; never answered as `None`, so a
    /// caller never takes "could not be read" for "never written".
    async fn current(&self) -> Result<Option<BuildProjection>, BuildStateError> {
        match self.current_build().await?.into_build_id() {
            None => Ok(None),
            Some(id) => self.read_build(&id).await.map(Some),
        }
    }
    /// Builds that are neither complete nor failed.
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError>;
    /// Every Build accepted, newest first: the acceptance-order index (R-BI3) read whole, each Build by
    /// its own acceptance stamp `(submitted_at_ms, build_id)`. What `build.get` streams in order (Q167
    /// R-RD5). RDM's adapters answer it from their index (or delegate to the adapter they decorate); an
    /// adapter that keeps none answers it from its facts' acceptances.
    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        let mut accepted: Vec<(u64, BuildId)> = self.facts().await?.into_iter().filter_map(|f| match f {
            BuildFact::Accepted(a) => Some((a.submitted_at_ms, a.build_id)),
            _ => None,
        }).collect();
        accepted.sort();
        accepted.dedup();
        Ok(accepted.into_iter().rev().map(|(_, id)| id).collect())
    }
    /// Insert-and-fail on `(build_id, attempt)`. Only the fabric-primary calls this on its own
    /// log to decide a claim (`build_claim::ClaimDoor`); an executor never claims from its own.
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError>;
    /// Whether `held`, an in-flight row standing for another attempt, still holds its mesh (Q35): a
    /// log on the store root reads the lane key the row names (void when it holds another entry); a
    /// log held in one process keys no lane, and every row it holds is live.
    async fn in_flight_live(&self, _held: &crate::mesh_claims::InFlight) -> Result<bool, BuildStateError> {
        Ok(true)
    }
    /// Decide `claim` with `mesh`'s in-flight row taken first (R-BI7 caveat 3): `row` names the
    /// attempt and its claimant, and a log on the store root names the claim's lane key in it (Q35),
    /// taking the row and writing the claim in one writer's turn. `Held` when another attempt's row
    /// holds the mesh (nothing written); else the claim's outcome. A `NotOpen` claim leaves the row
    /// taken for it to its caller to release. A log held in one process has no lane keys: it takes
    /// the row, then claims.
    async fn claim_in_flight(&self, claim: &BuildAttemptClaim, claims: &dyn crate::storage::MeshStorage, mesh: &str, row: &crate::mesh_claims::InFlight) -> Result<InFlightClaim, BuildStateError> {
        match claims.take_in_flight(mesh, row).await.map_err(|e| BuildStateError::Io(format!("mesh {mesh}'s in-flight row: {e}")))? {
            crate::mesh_claims::Taken::Held(held) => Ok(InFlightClaim::Held(held)),
            crate::mesh_claims::Taken::Won => self.claim_attempt(claim).await.map(InFlightClaim::Decided),
        }
    }
    /// Record, on this admin's own log, a claim the fabric-primary decided `Won`: the same fact
    /// the primary broadcast, taken now so this admin's view of the Build moves with its own
    /// attempt. Idempotent; decides nothing and broadcasts nothing.
    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError>;
    /// Append a step receipt.
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError>;
    /// Append an attempt receipt.
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError>;
    /// Every fact, in append order (what the fabric projection carries).
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError>;
    /// Drop a finished Build from the views (history administration).
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError>;
    /// The step in flight of `operation` in `attempt` of `lane` (`None` the fabric lane, `Some(mesh)`
    /// that mesh's drift lane) waits on `reason`: live progress for a call attached to the run, never
    /// a fact, never durable. A state with no call attached to its runs
    /// has nobody to tell.
    async fn note_blocked(&self, _build_id: &BuildId, _lane: Option<&str>, _attempt: u32, _operation: &str, _step: &str, _reason: &str) {}
    /// The Build, lane (`None` the fabric lane, `Some(mesh)` that mesh's drift lane), attempt and
    /// operation of the node workflow at `path` that deleted the exited birth `(node_id, incarnation)`
    /// and completed: a recovery of that birth (its `node.delete:<path>`
    /// in an attempt the recovery opened, answered at the attempt its last operation,
    /// `node.create:<path>`, completed), or a `node.delete:<path>` (the birth
    /// exited inside that workflow, node-recovery.md "Failure and receipts", so it is no drift to
    /// recover). Read from every
    /// Build's step receipts in the log (a forgotten Build's receipts included: forgetting leaves
    /// the views, never the receipts). The first such attempt. A point read by the exited birth:
    /// every adapter answers it from its index (or delegates to the adapter it decorates), never by
    /// reading the fact log.
    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError>;
    /// The Build, attempt and operation of the mesh.delete whose completion receipt
    /// (`mesh_delete::STEP_MESH_DELETED`) names the exact birth `(node_id, incarnation)` its final
    /// primary: the mesh.delete that ended with that birth alive, whose mesh-left follows that birth's
    /// exited verdict. The first such attempt. A point read by the birth: every adapter answers it from
    /// its index (or delegates to the adapter it decorates), never by reading the fact log.
    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError>;
    /// The exited births of `mesh` whose node workflow in `build_id` stopped at a Failed attempt and
    /// is not done (node-recovery.md "Failure and receipts": the mesh-primary submits each again):
    /// for each birth an attempt of `build_id` was opened for with `AttemptAction::Recover`, the run
    /// that its latest claimed such open began (up to the next attempt opened in the Build, whatever it
    /// carries) ended with its last claimed attempt's verdict `Failed`, and
    /// [`BuildStateAdapter::recovery_of`] does not answer the birth. An open no one claimed ran nothing
    /// and ends no run: a resubmit whose opener was lost between its open and its claim leaves the
    /// birth owed, and the submit claims that open attempt as it stands (build.md :365, claims are
    /// insert-and-fail on the attempt). A run claimed with no verdict yet, or handed off, is not owed.
    /// A point read by the Build: every adapter answers it from its index (or delegates to the adapter
    /// it decorates), never by reading or folding the fact log.
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError>;
    /// The path name this admin's topology view holds (`Some` once its location view holds one).
    fn held_name(&self) -> Option<crate::model::PathName> {
        None
    }
}

/// An exited birth whose node workflow stopped at a Failed attempt and is not done
/// ([`BuildStateAdapter::exits_owed`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwedExit {
    /// The exited birth's path.name.
    pub path: crate::model::PathName,
    /// The exited birth's node.id.
    pub node_id: crate::model::NodeId,
    /// The exited birth's incarnation.
    pub incarnation: crate::model::IncarnationId,
    /// The attempt whose `Failed` verdict ended the birth's run.
    pub attempt: u32,
    /// Why that attempt failed.
    pub reason: String,
}

/// The node workflows whose operation deletes a birth at its path.name: `<kind>:<path>`. A birth
/// one of them deleted is answered by it in the recovery index ([`BuildStateAdapter::recovery_of`]).
const DELETING_WORKFLOWS: [&str; 1] = ["node.delete:"];

/// The last operation of a node recovery (node-recovery.md "Steps"): node.create of the new node at
/// the exited birth's path.name over its data dir, whose claim seeds config and tags from the
/// Deleted row (proposal node-properties-own-commands.md §8). A recovery's deletion is answered once
/// it completes at or after the deletion's attempt, in the deletion's lane: a create from before
/// the deletion (the path's original create) is not the recovery's.
const RECOVERY_LAST: &str = "node.create:";

/// The path of a deleting workflow's operation `<kind>:<path>`, as text.
fn deleting_workflow_path(operation: &str) -> Option<&str> {
    DELETING_WORKFLOWS.iter().find_map(|kind| operation.strip_prefix(kind))
}

/// The birth a deleting workflow's (`retire-node`) `NodeDeleted`
/// receipt names: its output is the lifecycle operation of the node.delete of that birth,
/// `(path, node_id, incarnation)`.
pub(crate) fn recovery_deleted(s: &BuildStepReceipt) -> Option<(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId)> {
    delete_step_birth(s, "NodeDeleted")
}

/// The birth a deleting workflow's completed `step` receipt names, for the two steps whose output is
/// the lifecycle operation of the node.delete of that birth (`NodeDeleting`, `NodeDeleted`):
/// `(path, node_id, incarnation)`.
pub(crate) fn delete_step_birth(s: &BuildStepReceipt, step: &str) -> Option<(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId)> {
    if s.step != step || s.outcome != StepOutcome::Complete {
        return None;
    }
    let path = deleting_workflow_path(&s.operation)?.parse().ok()?;
    let output = s.output.as_ref()?;
    let node_id = serde_json::from_value(output.get("node_id")?.clone()).ok()?;
    let incarnation = serde_json::from_value(output.get("incarnation")?.clone()).ok()?;
    Some((path, node_id, incarnation))
}

/// Whether `s` is the `Complete` receipt of a deleting workflow's operation, or of a recovery's last.
fn recovery_completed(s: &BuildStepReceipt) -> bool {
    s.step == "Complete" && s.outcome == StepOutcome::Complete && (deleting_workflow_path(&s.operation).is_some() || s.operation.starts_with(RECOVERY_LAST))
}

/// The final primary a mesh.delete's completion receipt (`MeshDeleted`) names: its manifest's
/// `final_primary`, `(node_id, incarnation)`.
fn mesh_deleted_final(s: &BuildStepReceipt) -> Option<(crate::model::NodeId, crate::model::IncarnationId)> {
    if s.step != crate::mesh_delete::STEP_MESH_DELETED || s.outcome != StepOutcome::Complete {
        return None;
    }
    let last = s.output.as_ref()?.get("final_primary")?;
    let node_id = crate::model::NodeId::parse(last.get("node_id")?.as_str()?).ok()?;
    let incarnation = crate::model::IncarnationId::parse(last.get("incarnation_id")?.as_str()?).ok()?;
    Some((node_id, incarnation))
}

/// What an opened attempt carries, for a refusal: its reason, its action and who opened it.
fn describe_open(o: &AttemptOpened) -> String {
    let action = match &o.action {
        Some(crate::accepted::AttemptAction::DeleteConnections { path, node_id, from_incarnation }) => format!("node.connection.delete of {path} from {node_id} at {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Stop { path, node_id, from_incarnation }) => format!("stop of {path} from {node_id} at {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Start { path, node_id, from_incarnation }) => format!("start of {path} from {node_id} at {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Recover { path, node_id, from_incarnation, .. }) => format!("recovery of {path} from exited {node_id} at {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Delete { path, node_id, from_incarnation }) => format!("delete of {path} from {node_id} at {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Steps { steps, .. }) => format!("{} step(s): {}", steps.len(), steps.iter().map(crate::build::step_key).collect::<Vec<_>>().join(", ")),
        Some(crate::accepted::AttemptAction::Terminate { path, node_id, from_incarnation }) => format!("termination of the deleted birth {node_id} at {from_incarnation} of {path}"),
        None => "no targeted action".to_string(),
    };
    let lane = o.mesh.as_ref().map(|m| format!(", in {m}'s drift lane")).unwrap_or_default();
    format!("{} ({action}, opened by {}{lane})", o.reason.as_str(), o.opened_by)
}

/// The shared core of both RDM adapters: what readers see of the log, and its indexes, behind one
/// std lock taken only for an `Arc` clone, an index point read or a publication's swap. Readers fold
/// the snapshot outside it; a writer decides on a snapshot under its own turn (each adapter's async
/// `writer`) and publishes with one swap ([`publish`]). No whole-log fold and no derivation runs
/// while it is held, so no reader on an async worker waits on a writer's work.
#[derive(Debug, Default)]
struct FactLog {
    /// The facts readers see, one immutable snapshot: a reader takes it with one `Arc` clone and
    /// folds it outside this lock; a writer replaces it whole once the derivation ran over the facts
    /// it adds ([`publish`]).
    facts: Arc<Vec<BuildFact>>,
    /// The indexes the birth reads answer from ([`BirthIndex`]), filled by [`FactLog::index`].
    births: BirthIndex,
    /// The acceptance-order index (R-BI3): every Build accepted, by its own acceptance stamp
    /// `(submitted_at_ms, build_id)`, entered once by its `Accepted` fact and never rewritten, in
    /// whatever order the facts arrive. Read newest first by [`current_build`].
    accepted_order: std::collections::BTreeSet<(u64, BuildId)>,
    /// Each Build's own facts, by their position in `facts`: a Build is read by folding its facts
    /// alone ([`read_own`]).
    by_build: std::collections::HashMap<BuildId, Vec<usize>>,
}

/// The indexes [`BuildStateAdapter::recovery_of`], [`BuildStateAdapter::mesh_deleted_of`],
/// and [`BuildStateAdapter::exits_owed`] answer from,
/// derived fact by fact in whatever order the facts arrive. Every adapter keeps one: RDM's file and
/// memory adapters inside their log, an application's adapter beside its own store, filled by
/// [`BirthIndex::index`] with every fact it appends, absorbs or reads back on open, so the four reads
/// are point reads of it and never a fold of the fact log.
#[derive(Debug, Default, Clone)]
pub struct BirthIndex {
    /// The recovery index, in three parts the facts fill in whatever order they arrive (gossip and
    /// catch-up deliver in any order; once all three are held the answer is the same in every
    /// order, while a log that holds a recovery's deletion and its `Complete` but not yet its
    /// `Opened` answers it as a standalone delete's): every deleting workflow's (`retire-node`)
    /// `NodeDeleted` by the birth `(node_id, incarnation)` it names, to its path, operation, Build
    /// and attempt...
    recovery_deletes: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), Vec<(crate::model::PathName, String, LaneOf, u32)>>,
    /// ...the attempts of each `(Build, operation)` whose `Complete` is receipted, for the deleting
    /// operations and a recovery's last (`create-node`)...
    recovery_completes: std::collections::HashMap<(LaneOf, String), std::collections::BTreeSet<u32>>,
    /// ...and the attempts each recovery of a birth was opened at, by that birth, with its Build.
    /// A birth is answered once its `NodeDeleted` and a `Complete` at that attempt or later are both
    /// held: of the recovery's `node.create:<path>` when a recovery of the birth was opened in that
    /// Build at or before the deletion's attempt (its `retire-node` is the recovery's node.delete),
    /// else of the deleting operation itself. Filled by [`BirthIndex::index`], the one place a fact
    /// enters the log (own appends, absorbed facts and a journal's replay alike); forgetting a Build
    /// leaves all three, as it leaves the receipts.
    recoveries_opened: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), Vec<(LaneOf, u32)>>,
    /// The mesh.delete completion index: every `MeshDeleted` receipt by the final primary its
    /// manifest names, to its Build, attempt and operation ([`BuildStateAdapter::mesh_deleted_of`]).
    mesh_deleted: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), Vec<(BuildId, u32, String)>>,
    /// The index of [`BuildStateAdapter::exits_owed`], per Build, filled by [`BirthIndex::index`] in
    /// whatever order the facts arrive: the exited birth each attempt opened with
    /// `AttemptAction::Recover` names, by attempt (the first open of an attempt)...
    exit_opens: std::collections::HashMap<LaneOf, BTreeMap<u32, (crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId)>>,
    /// The recovery attempts of each lane an adoption launch opened (`AttemptReason::Adoption`).
    adoption_opens: std::collections::HashMap<LaneOf, std::collections::BTreeSet<u32>>,
    /// ...every attempt opened, whatever it carries (the next one ends the run of the one before)...
    opens: std::collections::HashMap<LaneOf, std::collections::BTreeSet<u32>>,
    /// ...and every attempt claimed or decided, with its verdict once one is held: the first
    /// appended, as [`fold`] reads it.
    verdicts: std::collections::HashMap<LaneOf, BTreeMap<u32, Option<AttemptOutcome>>>,
}

/// A lane of a Build, as the attempt-keyed indexes key it: the Build, and `None` for its fabric lane
/// or `Some(mesh)` for that mesh's drift lane (R-BI7). One attempt number names one attempt per lane.
type LaneOf = (BuildId, Option<String>);

/// A derivation over the Build facts, run before any reader sees the facts a writer adds
/// ([`LocalBuildLog::derive_on_every_fact`]): `derive` over the facts of the Builds it reads, never
/// the whole log. It reads the Builds the new facts belong to, the current Build, and the Builds
/// `held` names (those whose derived state its caller still holds). Only the current Build carries
/// open attempts (build.md: a second topology change is refused while the current Build reconciles,
/// and every stop, start, recovery and drift is an attempt of the current Build), so an
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
        let current = current_build(log);
        let reads = self.reads(new, current.build_id());
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
        self.births.index(fact);
        if let BuildFact::Accepted(a) = fact {
            self.accepted_order.insert((a.submitted_at_ms, a.build_id.clone()));
        }
        self.by_build.entry(fact.build_id().clone()).or_default().push(at);
    }
}

/// Where an adapter that keeps no log in memory keeps a [`BirthEntry`] ([`BirthEntry::keys`]): under
/// the birth it names, or under the lane it belongs to. A birth read reads the entries of one key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BirthKey {
    /// A birth `(node_id, incarnation)`.
    Birth(crate::model::NodeId, crate::model::IncarnationId),
    /// A lane of a Build: `None` for its fabric lane, `Some(mesh)` for that mesh's drift lane.
    Lane(BuildId, Option<String>),
}

/// One entry of a [`BirthIndex`], derived from one fact ([`birth_entries`]). An adapter that keeps
/// no log in memory keeps each entry under every key of [`BirthEntry::keys`], by its
/// [`BirthEntry::slot`] within that key: the first entry written in a slot stands and a later one in
/// the same slot is not written. The four birth reads then read those keys back ([`BirthRows`]) and
/// answer from the same derivation ([`BirthIndex::recovery_of_rows`] and its three siblings).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BirthEntry {
    /// A deleting workflow's `NodeDeleted` receipt, under the birth it names.
    Deleted {
        /// The node id of the birth the receipt names.
        node_id: crate::model::NodeId,
        /// The incarnation of the birth the receipt names.
        incarnation: crate::model::IncarnationId,
        /// The path.name it was deleted at.
        path: crate::model::PathName,
        /// The operation, `<kind>:<path>`.
        operation: String,
        /// The Build the entry belongs to.
        build_id: BuildId,
        /// Its lane: `None` for the fabric lane, `Some(mesh)` for that mesh's drift lane.
        mesh: Option<String>,
        /// The attempt.
        attempt: u32,
    },
    /// The `Complete` receipt of a deleting operation or of a recovery's last, under its lane.
    Completed {
        /// The Build the entry belongs to.
        build_id: BuildId,
        /// Its lane: `None` for the fabric lane, `Some(mesh)` for that mesh's drift lane.
        mesh: Option<String>,
        /// The operation, `<kind>:<path>`.
        operation: String,
        /// The attempt.
        attempt: u32,
    },
    /// A `MeshDeleted` receipt, under the final primary its manifest names.
    MeshDeleted {
        /// The node id of the birth the manifest names its final primary.
        node_id: crate::model::NodeId,
        /// The incarnation of the birth the manifest names its final primary.
        incarnation: crate::model::IncarnationId,
        /// The Build the receipt belongs to.
        build_id: BuildId,
        /// The attempt.
        attempt: u32,
        /// The operation, `<kind>:<path>`.
        operation: String,
    },
    /// An attempt opened with `AttemptAction::Recover`, under the exited birth and under its lane.
    RecoveryOpened {
        /// The node id of the birth the recovery is of.
        node_id: crate::model::NodeId,
        /// The incarnation of the birth the recovery is of.
        incarnation: crate::model::IncarnationId,
        /// The exited birth's path.name.
        path: crate::model::PathName,
        /// The Build the entry belongs to.
        build_id: BuildId,
        /// Its lane: `None` for the fabric lane, `Some(mesh)` for that mesh's drift lane.
        mesh: Option<String>,
        /// The attempt.
        attempt: u32,
        /// When the attempt was opened.
        opened_at_ms: u64,
        /// Whether an adoption launch opened it (`AttemptReason::Adoption`, Q156, Q159).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        adoption: bool,
    },
    /// An attempt opened, whatever it carries, under its lane.
    Opened {
        /// The Build the entry belongs to.
        build_id: BuildId,
        /// Its lane: `None` for the fabric lane, `Some(mesh)` for that mesh's drift lane.
        mesh: Option<String>,
        /// The attempt.
        attempt: u32,
    },
    /// An attempt claimed, under its lane.
    Claimed {
        /// The Build the entry belongs to.
        build_id: BuildId,
        /// Its lane: `None` for the fabric lane, `Some(mesh)` for that mesh's drift lane.
        mesh: Option<String>,
        /// The attempt.
        attempt: u32,
    },
    /// An attempt's verdict, under its lane.
    Decided {
        /// The Build the entry belongs to.
        build_id: BuildId,
        /// Its lane: `None` for the fabric lane, `Some(mesh)` for that mesh's drift lane.
        mesh: Option<String>,
        /// The attempt.
        attempt: u32,
        /// The verdict.
        outcome: AttemptOutcome,
    },
}

impl BirthEntry {
    /// The keys the entry is kept under.
    pub fn keys(&self) -> Vec<BirthKey> {
        let lane = |build_id: &BuildId, mesh: &Option<String>| BirthKey::Lane(build_id.clone(), mesh.clone());
        match self {
            Self::Deleted { node_id, incarnation, .. } | Self::MeshDeleted { node_id, incarnation, .. } => vec![BirthKey::Birth(node_id.clone(), incarnation.clone())],
            Self::RecoveryOpened { node_id, incarnation, build_id, mesh, .. } => vec![BirthKey::Birth(node_id.clone(), incarnation.clone()), lane(build_id, mesh)],
            Self::Completed { build_id, mesh, .. } | Self::Opened { build_id, mesh, .. } | Self::Claimed { build_id, mesh, .. } | Self::Decided { build_id, mesh, .. } => vec![lane(build_id, mesh)],
        }
    }

    /// The entry's slot within a key: two entries of one slot are one entry, the first written.
    pub fn slot(&self) -> String {
        let lane = |build_id: &BuildId, mesh: &Option<String>| format!("{build_id}/{}", mesh.as_deref().unwrap_or(""));
        match self {
            Self::Deleted { path, operation, build_id, mesh, attempt, .. } => format!("deleted/{}/{attempt}/{operation}/{path}", lane(build_id, mesh)),
            Self::Completed { build_id, mesh, operation, attempt } => format!("completed/{}/{attempt}/{operation}", lane(build_id, mesh)),
            Self::MeshDeleted { build_id, attempt, operation, .. } => format!("mesh-deleted/{build_id}/{attempt}/{operation}"),
            Self::RecoveryOpened { build_id, mesh, attempt, .. } => format!("recovery-opened/{}/{attempt}", lane(build_id, mesh)),
            Self::Opened { build_id, mesh, attempt } => format!("opened/{}/{attempt}", lane(build_id, mesh)),
            Self::Claimed { build_id, mesh, attempt } => format!("claimed/{}/{attempt}", lane(build_id, mesh)),
            Self::Decided { build_id, mesh, attempt, .. } => format!("decided/{}/{attempt}", lane(build_id, mesh)),
        }
    }
}

/// The entries [`BirthIndex::index`] derives from `fact`: none for a fact no birth read answers from.
pub fn birth_entries(fact: &BuildFact) -> Vec<BirthEntry> {
    let mut out = Vec::new();
    if let BuildFact::Step(s) = fact {
        let (build_id, mesh, attempt) = (s.build_id.clone(), s.mesh.clone(), s.attempt);
        if let Some((path, node_id, incarnation)) = recovery_deleted(s) {
            out.push(BirthEntry::Deleted { node_id, incarnation, path, operation: s.operation.clone(), build_id, mesh, attempt });
        } else if recovery_completed(s) {
            out.push(BirthEntry::Completed { build_id, mesh, operation: s.operation.clone(), attempt });
        } else if let Some((node_id, incarnation)) = mesh_deleted_final(s) {
            out.push(BirthEntry::MeshDeleted { node_id, incarnation, build_id, attempt, operation: s.operation.clone() });
        }
    }
    match fact {
        BuildFact::Opened(o) => {
            if let Some(crate::accepted::AttemptAction::Recover { path, node_id, from_incarnation, .. }) = &o.action {
                out.push(BirthEntry::RecoveryOpened { node_id: node_id.clone(), incarnation: from_incarnation.clone(), path: path.clone(), build_id: o.build_id.clone(), mesh: o.mesh.clone(), attempt: o.attempt, opened_at_ms: o.opened_at_ms, adoption: o.reason == AttemptReason::Adoption });
            }
            out.push(BirthEntry::Opened { build_id: o.build_id.clone(), mesh: o.mesh.clone(), attempt: o.attempt });
        }
        BuildFact::Claim(c) => out.push(BirthEntry::Claimed { build_id: c.build_id.clone(), mesh: c.mesh.clone(), attempt: c.attempt }),
        BuildFact::Attempt(a) => out.push(BirthEntry::Decided { build_id: a.build_id.clone(), mesh: a.mesh.clone(), attempt: a.attempt, outcome: a.outcome.clone() }),
        _ => {}
    }
    out
}

/// The entries an adapter keeps under a [`BirthKey`]: a read of that key's rows (a prefix read).
#[async_trait]
pub trait BirthRows: Send + Sync {
    /// Every entry kept under `key`, in any order.
    async fn entries_at(&self, key: &BirthKey) -> Result<Vec<BirthEntry>, BuildStateError>;
}

impl BirthIndex {
    /// Fold `fact` into the indexes: each entry it derives ([`birth_entries`]). A fact folded twice
    /// changes nothing.
    pub fn index(&mut self, fact: &BuildFact) {
        for entry in birth_entries(fact) {
            self.absorb(&entry);
        }
    }

    /// Fold one entry into the indexes, in whatever order the entries arrive. The first entry of a
    /// slot ([`BirthEntry::slot`]) stands; an entry folded twice changes nothing.
    pub fn absorb(&mut self, entry: &BirthEntry) {
        match entry.clone() {
            BirthEntry::Deleted { node_id, incarnation, path, operation, build_id, mesh, attempt } => {
                let lane: LaneOf = (build_id, mesh);
                let held = self.recovery_deletes.entry((node_id, incarnation)).or_default();
                if !held.iter().any(|(p, o, l, a)| *p == path && *o == operation && *l == lane && *a == attempt) {
                    held.push((path, operation, lane, attempt));
                }
            }
            BirthEntry::Completed { build_id, mesh, operation, attempt } => {
                self.recovery_completes.entry(((build_id, mesh), operation)).or_default().insert(attempt);
            }
            BirthEntry::MeshDeleted { node_id, incarnation, build_id, attempt, operation } => {
                let held = self.mesh_deleted.entry((node_id, incarnation)).or_default();
                if !held.iter().any(|(b, a, o)| *b == build_id && *a == attempt && *o == operation) {
                    held.push((build_id, attempt, operation));
                }
            }
            BirthEntry::RecoveryOpened { node_id, incarnation, path, build_id, mesh, attempt, adoption, .. } => {
                let lane: LaneOf = (build_id, mesh);
                if adoption {
                    self.adoption_opens.entry(lane.clone()).or_default().insert(attempt);
                }
                let opened = self.recoveries_opened.entry((node_id.clone(), incarnation.clone())).or_default();
                if !opened.contains(&(lane.clone(), attempt)) {
                    opened.push((lane.clone(), attempt));
                }
                self.exit_opens.entry(lane).or_default().entry(attempt).or_insert((path, node_id, incarnation));
            }
            BirthEntry::Opened { build_id, mesh, attempt } => {
                self.opens.entry((build_id, mesh)).or_default().insert(attempt);
            }
            BirthEntry::Claimed { build_id, mesh, attempt } => {
                self.verdicts.entry((build_id, mesh)).or_default().entry(attempt).or_insert(None);
            }
            BirthEntry::Decided { build_id, mesh, attempt, outcome } => {
                let held = self.verdicts.entry((build_id, mesh)).or_default().entry(attempt).or_insert(None);
                if held.is_none() {
                    *held = Some(outcome);
                }
            }
        }
    }

    /// The entries kept under the birth `(node_id, incarnation)` and under every lane they name.
    async fn birth_rows(rows: &dyn BirthRows, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Vec<BirthEntry>, BuildStateError> {
        let mut out = rows.entries_at(&BirthKey::Birth(node_id.clone(), incarnation.clone())).await?;
        let lanes: std::collections::BTreeSet<(BuildId, Option<String>)> = out
            .iter()
            .filter_map(|e| match e {
                BirthEntry::Deleted { build_id, mesh, .. } | BirthEntry::RecoveryOpened { build_id, mesh, .. } => Some((build_id.clone(), mesh.clone())),
                _ => None,
            })
            .collect();
        for (build_id, mesh) in lanes {
            out.extend(rows.entries_at(&BirthKey::Lane(build_id, mesh)).await?);
        }
        Ok(out)
    }

    /// The index of `entries`.
    fn of(entries: &[BirthEntry]) -> Self {
        let mut index = Self::default();
        for entry in entries {
            index.absorb(entry);
        }
        index
    }

    /// The index of one birth's entries ([`BirthIndex::birth_rows`]).
    async fn of_birth(rows: &dyn BirthRows, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Self, BuildStateError> {
        Ok(Self::of(&Self::birth_rows(rows, node_id, incarnation).await?))
    }

    /// [`BirthIndex::recovery_of`] over the entries `rows` keeps: the birth's, then its lanes'.
    pub async fn recovery_of_rows(rows: &dyn BirthRows, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        Ok(Self::of_birth(rows, node_id, incarnation).await?.recovery_of(path, node_id, incarnation))
    }

    /// [`BirthIndex::mesh_deleted_of`] over the entries `rows` keeps.
    pub async fn mesh_deleted_of_rows(rows: &dyn BirthRows, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        Ok(Self::of_birth(rows, node_id, incarnation).await?.mesh_deleted_of(node_id, incarnation))
    }

    /// [`BirthIndex::exits_owed`] over the entries `rows` keeps: the Build's fabric lane and `mesh`'s
    /// drift lane, then each exited birth they open a recovery of, with its lanes.
    pub async fn exits_owed_rows(rows: &dyn BirthRows, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError> {
        let mut index = Self::default();
        let mut births = std::collections::HashSet::new();
        for lane in [None, Some(mesh.to_string())] {
            for entry in rows.entries_at(&BirthKey::Lane(build_id.clone(), lane)).await? {
                if let BirthEntry::RecoveryOpened { node_id, incarnation, path, .. } = &entry {
                    if path.mesh == mesh {
                        births.insert((node_id.clone(), incarnation.clone()));
                    }
                }
                index.absorb(&entry);
            }
        }
        for (node_id, incarnation) in births {
            for entry in Self::birth_rows(rows, &node_id, &incarnation).await? {
                index.absorb(&entry);
            }
        }
        Ok(index.exits_owed(build_id, mesh))
    }

    /// The point read of [`BuildStateAdapter::recovery_of`]: the birth's `NodeDeleted` receipts at
    /// `path`, each answered by the first `Complete` at its attempt or later of the operation that
    /// ends its workflow: a recovery's `node.create:<path>` when a recovery of the birth was opened in
    /// that Build at or before the deletion's attempt, else the deleting operation itself. The first
    /// such attempt, with the deleting operation.
    pub fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Option<(BuildId, Option<String>, u32, String)> {
        let birth = (node_id.clone(), incarnation.clone());
        let opened = self.recoveries_opened.get(&birth);
        self.recovery_deletes
            .get(&birth)?
            .iter()
            .filter(|(p, _, _, _)| p == path)
            .filter_map(|(_, operation, lane, deleted)| {
                // In the deletion's own lane: an attempt number names one attempt per lane.
                let recovering = opened.is_some_and(|o| o.iter().any(|(l, at)| l == lane && at <= deleted));
                let ends = if recovering { format!("{RECOVERY_LAST}{path}") } else { operation.clone() };
                let completed = self.recovery_completes.get(&(lane.clone(), ends))?.range(*deleted..).next()?;
                Some((lane.0.clone(), lane.1.clone(), *completed, operation.clone()))
            })
            .min_by_key(|(_, _, a, _)| *a)
    }

    /// The point read of [`BuildStateAdapter::mesh_deleted_of`]: the first attempt's.
    pub fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Option<(BuildId, u32, String)> {
        self.mesh_deleted.get(&(node_id.clone(), incarnation.clone()))?.iter().min_by_key(|(_, a, _)| *a).cloned()
    }

    /// The point read of [`BuildStateAdapter::exits_owed`]: per exited birth of `mesh`, the run its
    /// latest claimed `Recover` open in `build_id` began, up to the next attempt opened in the Build;
    /// owed when the run's last claimed attempt holds the verdict `Failed` and [`BirthIndex::recovery_of`]
    /// does not answer the birth. An open with no claim (no entry in `verdicts`) starts no run.
    pub fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Vec<OwedExit> {
        // The fabric lane's recoveries of the mesh's births, and the mesh's own drift lane's.
        let mut owed: Vec<OwedExit> = [None, Some(mesh.to_string())].into_iter().flat_map(|lane| self.exits_owed_in(&(build_id.clone(), lane), mesh)).collect();
        owed.sort_by(|a, b| a.path.cmp(&b.path).then(a.attempt.cmp(&b.attempt)));
        owed
    }

    /// [`BirthIndex::exits_owed`] in one lane of the Build.
    fn exits_owed_in(&self, lane: &LaneOf, mesh: &str) -> Vec<OwedExit> {
        let Some(exit_opens) = self.exit_opens.get(lane) else { return Vec::new() };
        let claimed = |attempt: &u32| self.verdicts.get(lane).is_some_and(|v| v.contains_key(attempt));
        // The latest claimed open per birth: the attempts ascend, so the last one seen stands. An open
        // no one claimed ran nothing, so the run before it stands.
        let mut latest: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), (u32, crate::model::PathName)> = std::collections::HashMap::new();
        for (attempt, (path, node_id, incarnation)) in exit_opens {
            if path.mesh == mesh && claimed(attempt) {
                latest.insert((node_id.clone(), incarnation.clone()), (*attempt, path.clone()));
            }
        }
        let mut owed: Vec<OwedExit> = latest
            .into_iter()
            .filter_map(|((node_id, incarnation), (from, path))| {
                let until = self.opens.get(lane).and_then(|o| o.range(from + 1..).next().copied()).unwrap_or(u32::MAX);
                let (attempt, verdict) = self.verdicts.get(lane)?.range(from..until).next_back()?;
                let Some(AttemptOutcome::Failed { reason, code }) = verdict else { return None };
                // An adoption abandoned because the birth or a live node-admin answered (an adoption
                // launch's checks after its claim, Q156, NameHeld's code) owes no exit: the birth is
                // alive. Only that: a recovery failed on any other name conflict owes it still
                // (node-recovery.md "Failure and receipts", Q159).
                let adopted = self.adoption_opens.get(lane).is_some_and(|a| a.contains(&from));
                if adopted && *code == rdm_core::outcome::Code::AlreadyExists {
                    return None;
                }
                if self.recovery_of(&path, &node_id, &incarnation).is_some() {
                    return None;
                }
                Some(OwedExit { path, node_id, incarnation, attempt: *attempt, reason: reason.clone() })
            })
            .collect();
        owed.sort_by(|a, b| a.path.cmp(&b.path).then(a.attempt.cmp(&b.attempt)));
        owed
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
/// so a successor reads them there"). A Build message's copy (`FetchBuildFacts` 0x1F, a
/// build.attempt.run's intent) carries no executable (proposal executable-bindings-at-create.md rule
/// 7): an admin that absorbed a Build from one holds the Build as the store root records it once it
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

/// A claim that is not its lane's next attempt, or is on a complete lane (a drift lane never opened
/// included): refused by name, never recorded (the fold would ignore it, but the attempt number
/// stays free).
fn not_open(facts: &[BuildFact], claim: &BuildAttemptClaim) -> Option<ClaimOutcome> {
    let p = fold(facts).remove(&claim.build_id)?;
    let (state, attempt) = match &claim.mesh {
        None => (p.state, p.attempt),
        Some(mesh) => p.lanes.get(mesh).map(|l| (l.state, l.attempt)).unwrap_or((BuildState::Complete, 0)),
    };
    if matches!(state, BuildState::Complete) || claim.attempt != attempt + 1 {
        return Some(ClaimOutcome::NotOpen { next: (!matches!(state, BuildState::Complete)).then_some(attempt + 1) });
    }
    None
}

/// Whether `f` adds anything to `facts`: acceptances, opens and claims are insert-and-fail on
/// their key, everything else deduplicates.
fn is_new(facts: &[BuildFact], f: &BuildFact) -> bool {
    match f {
        // The whole acceptance is insert-and-fail on its Build; a section (Q284) on its Build and
        // meshes, and the whole holds every section.
        BuildFact::Accepted(i) => !facts.iter().any(|f| matches!(f, BuildFact::Accepted(h) if h.build_id == i.build_id && (h.topology.section_of.is_none() || (i.topology.section_of.is_some() && h.topology.meshes.keys().eq(i.topology.meshes.keys()))))),
        BuildFact::Opened(o) => opened(facts, &o.build_id, o.mesh.as_deref(), o.attempt).is_none(),
        BuildFact::Claim(c) => claim_holder(facts, &c.build_id, c.mesh.as_deref(), c.attempt).is_none(),
        _ => !facts.contains(f),
    }
}

/// The call context the open of `attempt` of `build_id` in the lane `mesh` (`None` the fabric lane)
/// was written with ([`AttemptOpened::context`]); `None` for an attempt opened with none or not open.
pub(crate) async fn opened_context(builds: &dyn BuildStateAdapter, build_id: &BuildId, mesh: Option<&str>, attempt: u32) -> Result<Option<rdm_core::context::CallContext>, BuildStateError> {
    Ok(opened(&builds.facts().await?, build_id, mesh, attempt).and_then(|o| o.context.clone()))
}

/// The open of `attempt` of `build_id` in the lane `mesh` (`None` the fabric lane).
fn opened<'a>(facts: &'a [BuildFact], build_id: &BuildId, mesh: Option<&str>, attempt: u32) -> Option<&'a AttemptOpened> {
    facts.iter().find_map(|f| match f {
        BuildFact::Opened(o) if &o.build_id == build_id && o.mesh.as_deref() == mesh && o.attempt == attempt => Some(o),
        _ => None,
    })
}

/// Whether `wanted` may be recorded: `Ok(true)` the number is free, `Ok(false)` it is held for the
/// same action (nothing to record), `Err(AttemptTaken)` it is held for another.
fn admit_open(facts: &[BuildFact], wanted: &AttemptOpened) -> Result<bool, BuildStateError> {
    match opened(facts, &wanted.build_id, wanted.mesh.as_deref(), wanted.attempt) {
        None => Ok(true),
        Some(held) if held.action == wanted.action => Ok(false),
        Some(held) => Err(BuildStateError::AttemptTaken {
            build_id: wanted.build_id.clone(),
            attempt: wanted.attempt,
            held: describe_open(held),
            wanted: describe_open(wanted),
        }),
    }
}

/// The executor holding `attempt` of `build_id` in the lane `mesh` (`None` the fabric lane): the
/// claim key `(build_id, mesh, attempt)`.
fn claim_holder(facts: &[BuildFact], build_id: &BuildId, mesh: Option<&str>, attempt: u32) -> Option<String> {
    facts.iter().find_map(|f| match f {
        BuildFact::Claim(c) if &c.build_id == build_id && c.mesh.as_deref() == mesh && c.attempt == attempt => Some(c.executor.clone()),
        _ => None,
    })
}

fn known(facts: &[BuildFact], build_id: &BuildId) -> bool {
    facts.iter().any(|f| matches!(f, BuildFact::Accepted(i) if &i.build_id == build_id))
}

fn read(facts: &[BuildFact], build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
    fold(facts).remove(build_id).ok_or_else(|| BuildStateError::UnknownBuild(build_id.clone()))
}

/// The folded view of `build_id`, from its own facts alone: its positions and the snapshot are taken
/// together under the readers' lock (one `Arc` clone and one small point read), and the Build's facts
/// are folded outside it.
fn read_own(log: &Mutex<FactLog>, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
    let (facts, at) = {
        let log = log.lock().unwrap();
        (log.facts.clone(), log.by_build.get(build_id).cloned().unwrap_or_default())
    };
    let own: Vec<BuildFact> = at.iter().map(|i| facts[*i].clone()).collect();
    read(&own, build_id)
}

/// The point read of [`BuildStateAdapter::current_build`]: the acceptance-order index newest first,
/// each Build read from its own facts, until the first in progress or completed. A Build forgotten
/// has left the views and is passed over, as a failed one is.
fn current_build(log: &Mutex<FactLog>) -> CurrentBuild {
    let newest_first: Vec<BuildId> = log.lock().unwrap().accepted_order.iter().rev().map(|(_, id)| id.clone()).collect();
    newest_first.iter().find_map(|id| read_own(log, id).ok().as_ref().and_then(current_of)).unwrap_or(CurrentBuild::None)
}

/// The point read of [`BuildStateAdapter::accepted_newest_first`]: the index, newest first.
fn accepted_newest_first(log: &Mutex<FactLog>) -> Vec<BuildId> {
    log.lock().unwrap().accepted_order.iter().rev().map(|(_, id)| id.clone()).collect()
}

/// The point read of [`BuildStateAdapter::newest_accepted_at_ms`]: the index's head.
fn newest_accepted_at_ms(log: &Mutex<FactLog>) -> Option<u64> {
    log.lock().unwrap().accepted_order.last().map(|(at, _)| *at)
}

fn active(facts: &[BuildFact]) -> Vec<BuildProjection> {
    fold(facts).into_values().filter(|p| matches!(p.state, BuildState::Pending | BuildState::Running)).collect()
}

/// In-memory fact log.
#[derive(Debug, Default)]
pub struct MemoryBuildStateAdapter {
    log: Mutex<FactLog>,
    /// The writer's turn: one writer at a time decides on the log and publishes to it, holding the
    /// derivation each publication runs. An async lock: a writer waiting on another's derivation
    /// yields its worker. Readers never take it.
    writer: tokio::sync::Mutex<Derived>,
    /// Each attempt's context, at its key.
    contexts: Mutex<BTreeMap<String, rdm_core::context::CallContext>>,
}

impl MemoryBuildStateAdapter {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish `fact` under the writer's turn `w`.
    async fn put_admitted(&self, w: &Derived, fact: BuildFact) -> Result<(), BuildStateError> {
        self.put(w, fact);
        Ok(())
    }

    /// Merge facts received from the fabric projection (append the unseen
    /// ones, keeping the receiver's own order deterministic: intents and
    /// claims are insert-and-fail, receipts are deduplicated).
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

/// The fact log an admin's Build state is over, which absorbs the facts a Build message carries: in
/// memory for runs that need no restart survival, or the fabric's Build journal on the shared store
/// root (`builds.storage`).
#[async_trait]
pub trait LocalBuildLog: BuildStateAdapter {
    /// Merge the facts a Build message carried (`FetchBuildFacts` 0x1F, a build.attempt.run's intent):
    /// append the unseen ones; intents, opens and claims are insert-and-fail, receipts deduplicate.
    async fn absorb_facts(&self, facts: &[BuildFact]);
    /// Run `derive` over the facts of the Builds it reads ([`FactDerivation::reads`]: the current
    /// Build and the held ones, then also those the new facts belong to), once now over the facts it
    /// holds, reading no store, and again each time facts enter the log, whoever put them there (an
    /// own append, an absorbed fact or a fact its read of the store took), before
    /// any reader sees them and before the append returns: what is derived from the facts never lags
    /// a reader of them. Never the whole log: an adapter that keeps none in memory reads those Builds
    /// one by one. It runs under the writer's turn, never under the lock readers take. A log whose
    /// facts live on a plane other admins write (no copy in memory) cannot order reads after the
    /// derivation: a fact there is readable before this admin derives over it (another admin's
    /// fact until this admin absorbs it; an own fact between its claim, which can lose, and the
    /// derivation over it), and the overlays converge at that derivation.
    async fn derive_on_every_fact(&self, derive: FactDerivation);
    /// Put the context attempt `(build_id, lane, attempt)` carries ([`crate::build_claim`]): a blind
    /// put of its own key. Local to this admin: never a Build fact, never gossiped, never read by
    /// another admin.
    async fn put_attempt_context(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, context: &rdm_core::context::CallContext) -> Result<(), crate::storage_error::StorageError>;
    /// The context of `(build_id, lane, attempt)`, when one was put: a point read of its key.
    async fn attempt_context(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> Result<Option<rdm_core::context::CallContext>, crate::storage_error::StorageError>;
    /// Merge one Build's whole facts, as `FetchBuildFacts` carried them complete (op 0x1F), into what
    /// this log holds, reading no store (Q227: "only FP reads the builds"). A whole Build's facts are
    /// ordered among themselves, so a later read of the store takes only the facts they lack. A log
    /// that reads no store merges them as [`Self::absorb_facts`] does.
    async fn absorb_build(&self, facts: &[BuildFact]) {
        self.absorb_facts(facts).await
    }
    /// `build_id` as the facts this log holds now, reading no store: what a launched node-admin's Build
    /// floor is checked against at its start (node-start.md step 4; Q227).
    async fn held_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.read_build(build_id).await
    }
    /// The current Build as the facts this log holds now, reading no store ([`Self::held_build`]).
    async fn held_current_build(&self) -> Result<CurrentBuild, BuildStateError> {
        self.current_build().await
    }
    /// From now on this log reads the Build records on the store only while `reader` names this admin
    /// (Q227: "only FP reads the builds"; build.md §5): while it is the fabric-primary, or the
    /// executor fabric recovery names in its place (Q122). Its first read of such a tenure is the take,
    /// made under the writer's turn so every record the store holds is in the log before the read
    /// answers; any other admin answers every read from what reached it (its pulls, 0x1F, an attempt's
    /// intent) and reads nothing. A log that reads no store has nothing to switch.
    async fn read_records_while(&self, _reader: RecordsReader) {}
    /// Read the Build records now, for `op`, as admin `node`, whatever [`Self::read_records_while`]
    /// says: the re-born fabric-primary's start over an inherited root with nobody to pull from (Q227:
    /// "re-born, it reads the latest Build records to learn what is in flight"; fabric-recovery.md
    /// "Nothing alive"), before any record names it fabric-primary. Made under the writer's turn and
    /// named as the take is. A log that reads no store has nothing to read.
    async fn read_records_now(&self, _op: &'static str, _node: &str) -> Result<(), BuildStateError> {
        Ok(())
    }
}

/// Whether this admin reads the Build records now ([`LocalBuildLog::read_records_while`]): its
/// path.name while it does, `None` while it holds only what reached it.
pub type RecordsReader = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// The key of attempt `(build_id, lane, attempt)`'s context: `<build_id>-<attempt>` for the fabric
/// lane, `<build_id>-<mesh>-<attempt>` for a mesh's drift lane, so an attempt number names one
/// attempt per lane.
pub(crate) fn attempt_context_key(build_id: &BuildId, lane: Option<&str>, attempt: u32) -> String {
    match lane {
        None => format!("{build_id}-{attempt}"),
        Some(mesh) => format!("{build_id}-{mesh}-{attempt}"),
    }
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

    async fn put_attempt_context(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, context: &rdm_core::context::CallContext) -> Result<(), crate::storage_error::StorageError> {
        self.contexts.lock().unwrap().insert(attempt_context_key(build_id, lane, attempt), context.clone());
        Ok(())
    }

    async fn attempt_context(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> Result<Option<rdm_core::context::CallContext>, crate::storage_error::StorageError> {
        Ok(self.contexts.lock().unwrap().get(&attempt_context_key(build_id, lane, attempt)).cloned())
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
        read_own(&self.log, build_id)
    }

    async fn held_current_build(&self) -> Result<CurrentBuild, BuildStateError> {
        Ok(current_build(&self.log))
    }

    async fn read_records_while(&self, reader: RecordsReader) {
        let _ = self.reader.set(reader);
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

    async fn put_attempt_context(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, context: &rdm_core::context::CallContext) -> Result<(), crate::storage_error::StorageError> {
        self.contexts.write(&attempt_context_key(build_id, lane, attempt), CONTEXT_FORMAT, context).await
    }

    async fn attempt_context(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32) -> Result<Option<rdm_core::context::CallContext>, crate::storage_error::StorageError> {
        self.contexts.read(&attempt_context_key(build_id, lane, attempt), CONTEXT_FORMAT).await
    }
}

#[async_trait]
impl BuildStateAdapter for MemoryBuildStateAdapter {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        if !accepted_conflict(&snapshot(&self.log), accepted)? {
            self.put_admitted(&w, BuildFact::Accepted(accepted.clone())).await?;
        }
        Ok(())
    }

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        let held = snapshot(&self.log);
        if !known(&held, &opened.build_id) {
            return Err(BuildStateError::UnknownBuild(opened.build_id.clone()));
        }
        if admit_open(&held, opened)? {
            self.put_admitted(&w, BuildFact::Opened(opened.clone())).await?;
        }
        Ok(())
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        read_own(&self.log, build_id)
    }

    async fn current_build(&self) -> Result<CurrentBuild, BuildStateError> {
        Ok(current_build(&self.log))
    }

    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        Ok(newest_accepted_at_ms(&self.log))
    }

    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        Ok(accepted_newest_first(&self.log))
    }

    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        Ok(active(&snapshot(&self.log)))
    }

    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        let w = self.writer.lock().await;
        let held = snapshot(&self.log);
        if !known(&held, &claim.build_id) {
            return Err(BuildStateError::UnknownBuild(claim.build_id.clone()));
        }
        if let Some(holder) = claim_holder(&held, &claim.build_id, claim.mesh.as_deref(), claim.attempt) {
            return Ok(if holder == claim.executor { ClaimOutcome::Won } else { ClaimOutcome::Lost { holder } });
        }
        if let Some(not_open) = not_open(&held, claim) {
            return Ok(not_open);
        }
        drop(held);
        self.put_admitted(&w, BuildFact::Claim(claim.clone())).await?;
        Ok(ClaimOutcome::Won)
    }

    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.absorb(&[BuildFact::Claim(claim.clone())]).await;
        Ok(())
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.put_admitted(&w, BuildFact::Step(receipt.clone())).await?;
        Ok(())
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.put_admitted(&w, BuildFact::Attempt(receipt.clone())).await?;
        Ok(())
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        Ok(snapshot(&self.log).as_ref().clone())
    }

    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        Ok(self.log.lock().unwrap().births.recovery_of(path, node_id, incarnation))
    }

    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        Ok(self.log.lock().unwrap().births.mesh_deleted_of(node_id, incarnation))
    }

    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError> {
        Ok(self.log.lock().unwrap().births.exits_owed(build_id, mesh))
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.put_admitted(&w, BuildFact::Forget { build_id: build_id.clone() }).await?;
        Ok(())
    }
}

#[cfg(feature = "file-backend")]
/// Where a node-admin keeps its attempts' contexts in its own data dir, and their format.
pub(crate) const CONTEXT_DIR: &str = "attempt-context";
#[cfg(feature = "file-backend")]
const CONTEXT_FORMAT: &str = "attempt-context/1";

#[cfg(feature = "file-backend")]
/// The format of one Build record's line in `builds.jsonl`, at `<build_id>/<key>` ([`FileJournal`]).
pub const RECORD_FORMAT: &str = "build-record/1";

#[cfg(feature = "file-backend")]
/// One Build record as it is stored: the fact, and `seq`, its place in the order its writer wrote the
/// records, which a reader replays them in (the fold breaks its ties by that order: a step's receipts
/// replay in the order they were written).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredFact {
    seq: u64,
    fact: BuildFact,
}

#[cfg(feature = "file-backend")]
/// The key `fact` is written at, after its Build's id (`<build_id>/<key>`): one record per fact (build.md §5.3): the
/// acceptance at `accepted`, an attempt's open, claim and result at `<attempt>-opened`, `-claim` and
/// `-result` (`<mesh>-<attempt>` for an attempt in a mesh), one receipt per step at
/// `<attempt>-step-<operation>-<step>`, and a forget at `forget`.
fn record_key(fact: &BuildFact) -> Result<String, BuildStateError> {
    let attempt = |mesh: &Option<String>, attempt: u32| match mesh {
        None => attempt.to_string(),
        Some(mesh) => format!("{mesh}-{attempt}"),
    };
    let key = match fact {
        BuildFact::Accepted(_) => "accepted".to_string(),
        BuildFact::Forget { .. } => "forget".to_string(),
        BuildFact::Opened(o) => format!("{}-opened", attempt(&o.mesh, o.attempt)),
        BuildFact::Claim(c) => format!("{}-claim", attempt(&c.mesh, c.attempt)),
        BuildFact::Attempt(a) => format!("{}-result", attempt(&a.mesh, a.attempt)),
        BuildFact::Step(s) => format!("{}-step-{}-{}", attempt(&s.mesh, s.attempt), s.operation, s.step),
    };
    if key.contains('/') || key.starts_with('.') {
        return Err(BuildStateError::Io(format!("the Build record key {key:?} of Build {} is not a file name", fact.build_id())));
    }
    Ok(key)
}

#[cfg(feature = "file-backend")]
/// `builds.storage`: the Build journal on the fabric's shared store root, one line of its `builds.jsonl`
/// per fact at its own key (`<build_id>/<key>`, `record_key`; build.md §5.3; R-SR1), appended when
/// it is committed (Q61). A successor folds the lines.
pub struct FileJournal {
    /// The root's `builds.jsonl`.
    file: crate::store_log::StoreLog,
    /// The lines the file's fold has read and no turn has taken yet, in file order.
    unread: Arc<Mutex<Vec<(String, StoredFact)>>>,
    /// The process writing through this journal.
    log_writer: crate::store_log::LogWriter,
    /// Every record this journal took or wrote, `<build_id>/<key>`: a read takes only the others.
    seen: Arc<Mutex<std::collections::HashSet<String>>>,
    /// The `seq` the next record is written at: past every record read or written.
    next_seq: std::sync::atomic::AtomicU64,
    /// `<own data dir>/attempt-context/`: each attempt's context, local to this admin.
    contexts: crate::record_store::FileRecords,
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
    /// the open (rdm#89, Q227). Its attempt contexts are this admin's own, in `own_data_dir`.
    pub async fn open_on_root(root: &crate::store_root::StoreRoot, own_data_dir: &Path, writer: &crate::store_log::LogWriter) -> Result<Self, BuildStateError> {
        let contexts = crate::record_store::FileRecords::open(own_data_dir, CONTEXT_DIR, &writer.incarnation_id).await.map_err(|e| BuildStateError::Io(format!("attempt contexts: {e}")))?;
        let unread: Arc<Mutex<Vec<(String, StoredFact)>>> = Arc::default();
        let queue = unread.clone();
        let observe: crate::store_log::Observer = Arc::new(move |l: &crate::store_log::Line| {
            if let Ok(stored) = StoredFact::deserialize(&l.record) {
                queue.lock().unwrap_or_else(|e| e.into_inner()).push((l.key.clone(), stored));
            }
        });
        let file = crate::store_log::StoreLog::open_observed_on_first_read(&root.dir().join(crate::store_root::BUILDS_FILE), crate::store_log::Formats::new().with::<StoredFact>(RECORD_FORMAT), observe).await.map_err(store_io)?;
        Ok(Self {
            file,
            unread,
            log_writer: writer.clone(),
            seen: Arc::default(),
            next_seq: std::sync::atomic::AtomicU64::new(1),
            contexts,
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

    /// The records on the store root this journal has not taken yet, each with its key: the file's
    /// lines past the fold's offset, read now for `op` (the turn or read it is for, what a read that
    /// waits on the file names), and any a read could not take before. Nothing is marked taken here.
    async fn read_new(&self, op: &'static str) -> Result<Vec<(String, StoredFact)>, BuildStateError> {
        self.file.advance_for(op).await.map_err(store_io)?;
        Ok(std::mem::take(&mut *self.unread.lock().unwrap_or_else(|e| e.into_inner())))
    }

    /// Take the records `read` from the store root into the log under the writer's turn `w`, in the
    /// order they were written: each is marked taken, and the facts the log does not hold yet are
    /// published.
    fn take_read(&self, w: &Derived, mut read: Vec<(String, StoredFact)>) {
        if read.is_empty() {
            return;
        }
        read.sort_by_key(|(_, s)| s.seq);
        let held = snapshot(&self.log);
        let mut new: Vec<BuildFact> = Vec::new();
        let mut stored: Vec<BuildAccepted> = Vec::new();
        {
            let mut seen = self.seen.lock().unwrap();
            for (key, StoredFact { seq, fact }) in read {
                if !seen.insert(key) {
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
    /// path.name while it does; unset, it reads (`""`), as a bootstrap or adoption start does.
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
                if let Some(id) = current_build(&self.log).build_id() {
                    span.record("current_build_id", id.as_str());
                    if let Ok(p) = read_own(&self.log, id) {
                        span.record("state", format!("{:?}", p.state).as_str());
                    }
                }
                span.in_scope(|| tracing::info!("this admin reads the Build records: the ones the store holds are read once, to learn what is in flight, and every read after it reads on from there"));
            }
            Err(e) => {
                span.record("outcome", "refused").record("error", e.to_string().as_str());
                span.in_scope(|| tracing::info!("the Build records could not be read at the take: the next read reads them again"));
            }
        }
    }

    /// Append `fact` at its own key and publish it, under the writer's turn `w` that has read the store
    /// root. Nothing is written when the log holds the fact already: a record is written once.
    async fn append(&self, w: &Derived, fact: BuildFact) -> Result<(), BuildStateError> {
        if !is_new(&snapshot(&self.log), &fact) {
            return Ok(());
        }
        let key = format!("{}/{}", fact.build_id(), record_key(&fact)?);
        let stored = StoredFact { seq: self.next_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed), fact };
        self.store_call(self.file.append(&self.log_writer, RECORD_FORMAT, &key, &stored)).await.map_err(store_io)?;
        self.seen.lock().unwrap().insert(key);
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

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        let w = self.turn("open_attempt").await;
        self.sync(&w).await?;
        let free = {
            let held = snapshot(&self.log);
            if !known(&held, &opened.build_id) {
                return Err(BuildStateError::UnknownBuild(opened.build_id.clone()));
            }
            admit_open(&held, opened)?
        };
        if !free {
            return Ok(());
        }
        self.append(&w, BuildFact::Opened(opened.clone())).await
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.sync_for_read("read_build").await?;
        read_own(&self.log, build_id)
    }


    async fn current_build(&self) -> Result<CurrentBuild, BuildStateError> {
        self.sync_for_read("current_build").await?;
        Ok(current_build(&self.log))
    }

    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.sync_for_read("newest_accepted_at_ms").await?;
        Ok(newest_accepted_at_ms(&self.log))
    }

    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        self.sync_for_read("accepted_newest_first").await?;
        Ok(accepted_newest_first(&self.log))
    }

    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.sync_for_read("list_active").await?;
        Ok(active(&snapshot(&self.log)))
    }

    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        let w = self.turn("claim_attempt").await;
        self.sync(&w).await?;
        {
            let held = snapshot(&self.log);
            if !known(&held, &claim.build_id) {
                return Err(BuildStateError::UnknownBuild(claim.build_id.clone()));
            }
            if let Some(holder) = claim_holder(&held, &claim.build_id, claim.mesh.as_deref(), claim.attempt) {
                return Ok(if holder == claim.executor { ClaimOutcome::Won } else { ClaimOutcome::Lost { holder } });
            }
            if let Some(not_open) = not_open(&held, claim) {
                return Ok(not_open);
            }
        }
        self.append(&w, BuildFact::Claim(claim.clone())).await.map(|()| ClaimOutcome::Won)
    }

    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        let w = self.turn("adopt_claim").await;
        self.sync(&w).await?;
        let fact = BuildFact::Claim(claim.clone());
        if is_new(&snapshot(&self.log), &fact) {
            publish(&self.log, w.0.as_ref(), vec![fact]);
        }
        Ok(())
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        let w = self.turn("append_step_receipt").await;
        self.sync(&w).await?;
        self.append(&w, BuildFact::Step(receipt.clone())).await
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let w = self.turn("append_attempt_receipt").await;
        self.sync(&w).await?;
        self.append(&w, BuildFact::Attempt(receipt.clone())).await
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.sync_for_read("facts").await?;
        Ok(snapshot(&self.log).as_ref().clone())
    }

    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.sync_for_read("recovery_of").await?;
        Ok(self.log.lock().unwrap().births.recovery_of(path, node_id, incarnation))
    }

    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.sync_for_read("mesh_deleted_of").await?;
        Ok(self.log.lock().unwrap().births.mesh_deleted_of(node_id, incarnation))
    }

    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError> {
        self.sync_for_read("exits_owed").await?;
        Ok(self.log.lock().unwrap().births.exits_owed(build_id, mesh))
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let w = self.turn("forget").await;
        self.sync(&w).await?;
        self.append(&w, BuildFact::Forget { build_id: build_id.clone() }).await
    }
}

#[cfg(all(test, feature = "file-backend"))]
mod tests {
    /// The birth-index rows an adapter that keeps no log in memory keeps of `adapter`'s facts: each
    /// entry under each of its keys, the first of a slot standing.
    #[derive(Default)]
    struct Rows(std::sync::Mutex<std::collections::HashMap<super::BirthKey, std::collections::BTreeMap<String, super::BirthEntry>>>);

    #[async_trait::async_trait]
    impl super::BirthRows for Rows {
        async fn entries_at(&self, key: &super::BirthKey) -> Result<Vec<super::BirthEntry>, super::BuildStateError> {
            Ok(self.0.lock().unwrap().get(key).map(|r| r.values().cloned().collect()).unwrap_or_default())
        }
    }

    async fn rows_of(adapter: &impl super::BuildStateAdapter) -> Rows {
        let rows = Rows::default();
        for fact in adapter.facts().await.unwrap() {
            for entry in super::birth_entries(&fact) {
                for key in entry.keys() {
                    rows.0.lock().unwrap().entry(key).or_default().entry(entry.slot()).or_insert_with(|| entry.clone());
                }
            }
        }
        rows
    }

    #[test]
    fn a_step_line_written_before_receipts_carried_output_still_decodes() {
        let old = format!(r#"{{"fact":"step","build_id":"{}","attempt":1,"operation":"node.create:mesh1.rpc.1","step":"DeployRuntime","outcome":"complete"}}"#, rdm_core::fixture_crockford60("b1"));
        let fact: BuildFact = serde_json::from_str(&old).unwrap();
        assert!(matches!(fact, BuildFact::Step(BuildStepReceipt { output: None, .. })));
        let with = BuildStepReceipt { output: Some(serde_json::json!({"pid": 7})), ..step("b1", 1, "node.create:mesh1.rpc.1") };
        let back: BuildStepReceipt = serde_json::from_str(&serde_json::to_string(&with).unwrap()).unwrap();
        assert_eq!(back, with);
    }

    use super::*;

    fn intent(id: &str) -> BuildAccepted {
        BuildAccepted {
            build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(),
            topology: FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()),
            submitted_change: None,
            submitted_at_ms: 1,
        }
    }

    fn opened(id: &str, attempt: u32, reason: AttemptReason) -> AttemptOpened {
        AttemptOpened { mesh: None, build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, reason, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 5, context: None }
    }

    fn claim(id: &str, attempt: u32, who: &str) -> BuildAttemptClaim {
        BuildAttemptClaim { claimant: None, mesh: None, build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, executor: who.into() }
    }

    fn step(id: &str, attempt: u32, op: &str) -> BuildStepReceipt {
        BuildStepReceipt { mesh: None, build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, operation: op.into(), step: "DeployRuntime".into(), outcome: StepOutcome::Complete, output: None, executor: None }
    }

    fn attempt(id: &str, attempt: u32, outcome: AttemptOutcome) -> BuildAttemptReceipt {
        BuildAttemptReceipt { mesh: None, build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, outcome }
    }

    fn attempt_of(id: &str, n: u32, outcome: AttemptOutcome) -> BuildAttemptReceipt {
        attempt(id, n, outcome)
    }

    fn log() -> Vec<BuildFact> {
        vec![
            BuildFact::Accepted(intent("b1")),
            BuildFact::Claim(claim("b1", 1, "mesh1.admin.1")),
            BuildFact::Step(step("b1", 1, "node.stop:mesh1.rpc.2")),
            BuildFact::Attempt(attempt("b1", 1, AttemptOutcome::Failed { reason: "executor lost".into() , code: rdm_core::outcome::Code::FailedPrecondition })),
            BuildFact::Claim(claim("b1", 2, "mesh1.admin.2")),
            BuildFact::Claim(claim("b1", 2, "mesh1.admin.1")), // loses: attempt 2 already claimed
            BuildFact::Attempt(attempt("b1", 1, AttemptOutcome::Converged)), // stale attempt: ignored
            BuildFact::Attempt(attempt("b1", 2, AttemptOutcome::Converged)),
            BuildFact::Accepted(intent("b2")),
        ]
    }

    #[test]
    fn the_fold_is_deterministic_and_idempotent() {
        let a = fold(&log());
        assert_eq!(a, fold(&log()));
        let b1 = &a[&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()];
        assert_eq!(b1.state, BuildState::Complete);
        assert_eq!(b1.attempt, 2);
        assert_eq!(b1.executor.as_deref(), Some("mesh1.admin.2"), "the first claim of an attempt wins");
        assert_eq!(b1.steps.len(), 1);
        assert_eq!(a[&BuildId::parse(&rdm_core::fixture_crockford60("b2")).unwrap()].state, BuildState::Pending);
        // Duplicating every fact changes nothing.
        let doubled: Vec<BuildFact> = log().into_iter().flat_map(|f| [f.clone(), f]).collect();
        assert_eq!(fold(&doubled), a);
    }

    #[test]
    fn a_failed_attempt_leaves_the_build_failed_until_a_later_claim() {
        let v = fold(&log()[..4]);
        let b1 = &v[&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()];
        assert_eq!(b1.state, BuildState::Failed);
        assert_eq!(b1.last_failure.as_deref(), Some("executor lost"));
        let v = fold(&log()[..5]);
        assert_eq!(v[&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()].state, BuildState::Running, "a successor's claim resumes the same build id");
    }

    #[test]
    fn open_delete_overlays_are_derived_from_the_build_facts() {
        let op = |node: &str| rdm_core::LifecycleOp {
            build_id: rdm_core::fixture_crockford60("b1"),
            attempt: 1,
            operation: format!("node.delete:{node}"),
            node_id: rdm_core::NodeId::mint(),
            incarnation: rdm_core::IncarnationId::mint(),
            name: node.parse().unwrap(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        };
        let (a, b) = (op("mesh1.rpc.1"), op("mesh1.rpc.2"));
        let with = |operation: &str, step: &str, output: &rdm_core::LifecycleOp| {
            BuildFact::Step(BuildStepReceipt {
                mesh: None,
                build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
                attempt: 1,
                operation: operation.into(),
                step: step.into(),
                outcome: StepOutcome::Complete,
                output: Some(serde_json::to_value(output).unwrap()),
                executor: None,
            })
        };
        let facts = vec![
            BuildFact::Accepted(intent("b1")),
            with(&a.operation, "NodeDeleting", &a),
            with(&a.operation, "NodeDeleted", &a),
            with(&b.operation, "NodeDeleting", &b),
            with(&b.operation, "DeleteConnections", &b),
        ];
        let open = in_flight_ops(&facts, &fold(&facts)).ops;
        assert_eq!(open, vec![b.clone()], "a delete with its pre-notice and no departure is open; a completed one is not");

        assert!(in_flight_ops(&facts, &fold(&facts)).unrecognised.is_empty());
        let mut facts = facts;

        // A Build is forgotten once the fold drops it on its `Forget`; a `Forget` of a Build still in
        // flight leaves it held, so it names nothing forgotten.
        facts.push(BuildFact::Forget { build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap() });
        facts.push(BuildFact::Forget { build_id: BuildId::parse(&rdm_core::fixture_crockford60("b9")).unwrap() });
        assert_eq!(forgotten_builds(&facts, &fold(&facts)), [rdm_core::fixture_crockford60("b9")].into(), "only a Build the fold no longer holds is forgotten");
    }

    /// CONTRACT: a completed pre-event step whose recorded operation this build cannot read (a row
    /// another build wrote, a field it renamed) is refused by name (build, attempt, operation, step,
    /// why) and opens no overlay; it is never skipped as if it carried nothing.
    #[test]
    fn a_pre_event_step_this_build_cannot_read_is_refused_by_name() {
        let step = |output: Option<serde_json::Value>| {
            BuildFact::Step(BuildStepReceipt {
                mesh: None,
                build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
                attempt: 2,
                operation: "node.delete:mesh1.rpc.1".into(),
                step: "NodeDeleting".into(),
                outcome: StepOutcome::Complete,
                output,
                executor: None,
            })
        };
        let old_shape = serde_json::json!({"build_id": rdm_core::fixture_crockford60("b1"), "attempt": 2, "operation": "node.delete:mesh1.rpc.1", "event_at_ms": 1});
        let facts = vec![BuildFact::Accepted(intent("b1")), step(Some(old_shape))];
        let read = in_flight_ops(&facts, &fold(&facts));
        assert!(read.ops.is_empty(), "an unreadable step opens no overlay");
        assert_eq!(read.unrecognised.len(), 1, "{read:?}");
        let u = &read.unrecognised[0];
        assert_eq!((u.attempt, u.operation.as_str(), u.step.as_str()), (2, "node.delete:mesh1.rpc.1", "NodeDeleting"));
        assert!(u.reason.contains("not a LifecycleOp this build reads"), "{}", u.reason);
        let held = [BuildFact::Accepted(intent("b1")), step(None)];
        let none = in_flight_ops(&held, &fold(&held));
        assert_eq!(none.unrecognised[0].reason, "the step recorded no operation");
    }

    #[tokio::test]
    async fn claims_are_insert_and_fail() {
        let m = MemoryBuildStateAdapter::new();
        m.publish_accepted(&intent("b1")).await.unwrap();
        assert_eq!(m.claim_attempt(&claim("b1", 1, "a")).await.unwrap(), ClaimOutcome::Won);
        assert_eq!(m.claim_attempt(&claim("b1", 1, "b")).await.unwrap(), ClaimOutcome::Lost { holder: "a".into() });
        assert_eq!(m.claim_attempt(&claim("b1", 1, "a")).await.unwrap(), ClaimOutcome::Won, "re-claiming your own attempt is idempotent");
        assert_eq!(m.claim_attempt(&claim("b9", 1, "a")).await, Err(BuildStateError::UnknownBuild(BuildId::parse(&rdm_core::fixture_crockford60("b9")).unwrap())));
        let mut other = intent("b1");
        other.submitted_at_ms = 2;
        assert_eq!(m.publish_accepted(&other).await, Err(BuildStateError::ConflictingIntent(BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap())));
        m.publish_accepted(&intent("b1")).await.unwrap();
        assert_eq!(m.list_active().await.unwrap().len(), 1);
    }


    #[test]
    fn a_hand_off_chain_arriving_out_of_order_still_reaches_its_converged_attempt() {
        // Gossip delivers facts in any order: a later claim can arrive first.
        let chain = vec![
            BuildFact::Claim(claim("b7", 3, "c")),
            BuildFact::Attempt(attempt("b7", 3, AttemptOutcome::Converged)),
            BuildFact::Attempt(attempt("b7", 2, AttemptOutcome::HandedOff { to: "c".into() })),
            BuildFact::Claim(claim("b7", 2, "b")),
            BuildFact::Attempt(attempt("b7", 1, AttemptOutcome::HandedOff { to: "b".into() })),
            BuildFact::Claim(claim("b7", 1, "a")),
            BuildFact::Accepted(intent("b7")),
        ];
        let b = fold(&chain).remove(&BuildId::parse(&rdm_core::fixture_crockford60("b7")).unwrap()).unwrap();
        assert_eq!((b.state, b.attempt, b.executor.as_deref()), (BuildState::Complete, 3, Some("c")));
    }

    #[tokio::test]
    async fn an_attempt_is_opened_once_and_reopens_a_complete_build() {
        let m = MemoryBuildStateAdapter::new();
        m.publish_accepted(&intent("b1")).await.unwrap();
        assert_eq!(m.claim_attempt(&claim("b1", 1, "a")).await.unwrap(), ClaimOutcome::Won);
        m.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        assert_eq!(m.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap().state, BuildState::Complete);
        // A claim on a complete Build runs nothing and records nothing: a complete Build runs
        // again only once an attempt is opened on it.
        assert_eq!(m.claim_attempt(&claim("b1", 2, "a")).await.unwrap(), ClaimOutcome::NotOpen { next: None });
        assert_eq!(m.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap().state, BuildState::Complete, "a stray claim does not reopen");
        // Two authorities proving the same drift open one attempt: the first opener wins.
        m.open_attempt(&opened("b1", 2, AttemptReason::ProvenDrift)).await.unwrap();
        let mut again = opened("b1", 2, AttemptReason::Stop);
        again.opened_by = "mesh1.admin.2".into();
        m.open_attempt(&again).await.unwrap();
        let b = m.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap();
        assert_eq!((b.state, b.attempt, b.reason, b.executor), (BuildState::Pending, 1, AttemptReason::ProvenDrift, None));
        assert_eq!(m.facts().await.unwrap().iter().filter(|f| matches!(f, BuildFact::Opened(_))).count(), 1);
        assert_eq!(m.list_active().await.unwrap().len(), 1, "the reopened Build is active again");
        assert_eq!(m.claim_attempt(&claim("b1", 3, "b")).await.unwrap(), ClaimOutcome::NotOpen { next: Some(2) }, "only the next attempt");
        assert_eq!(m.claim_attempt(&claim("b1", 2, "b")).await.unwrap(), ClaimOutcome::Won);
        let b = m.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap();
        assert_eq!((b.state, b.attempt, b.executor.as_deref()), (BuildState::Running, 2, Some("b")));
        assert_eq!(m.open_attempt(&opened("b9", 1, AttemptReason::Stop)).await, Err(BuildStateError::UnknownBuild(BuildId::parse(&rdm_core::fixture_crockford60("b9")).unwrap())));
    }

    /// Two openers that computed one attempt number from one settled Build: the first append holds it.
    /// The second asking for the same action is the same attempt (two authorities proving one drift);
    /// asking for another action it is refused by name, naming what holds the number, and appends nothing.
    #[tokio::test]
    async fn an_attempt_number_held_for_one_action_is_refused_to_another() {
        use crate::accepted::AttemptAction;
        let m = MemoryBuildStateAdapter::new();
        m.publish_accepted(&intent("b1")).await.unwrap();
        m.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        let (inc, other) = (crate::model::IncarnationId::mint(), crate::model::IncarnationId::mint());
        let node_id = crate::model::NodeId::mint();
        let act = |path: &str, from: &crate::model::IncarnationId| Some(AttemptAction::Stop { path: path.parse().unwrap(), node_id: node_id.clone(), from_incarnation: from.clone() });
        let mut first = opened("b1", 2, AttemptReason::ProvenDrift);
        first.action = act("mesh1.rpc.2", &inc);
        m.open_attempt(&first).await.unwrap();
        let mut same = opened("b1", 2, AttemptReason::Stop);
        same.action = act("mesh1.rpc.2", &inc);
        same.opened_by = "mesh2.admin.1".into();
        m.open_attempt(&same).await.expect("the same action asked again is the same attempt");
        let mut other_node = opened("b1", 2, AttemptReason::Stop);
        other_node.action = Some(AttemptAction::Stop { path: "mesh2.rpc.1".parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: other.clone() });
        let err = m.open_attempt(&other_node).await.expect_err("another action asked for the held number");
        match &err {
            BuildStateError::AttemptTaken { build_id, attempt, held, wanted } => {
                assert_eq!((build_id.to_string(), *attempt), (rdm_core::fixture_crockford60("b1"), 2));
                assert!(held.contains("stop of mesh1.rpc.2") && held.contains("proven-drift"), "the refusal names what holds the number: {held}");
                assert!(wanted.contains("stop of mesh2.rpc.1"), "the refusal names what was asked: {wanted}");
            }
            other => panic!("not AttemptTaken: {other:?}"),
        }
        let mut other_birth = opened("b1", 2, AttemptReason::Stop);
        other_birth.action = act("mesh1.rpc.2", &other);
        assert!(matches!(m.open_attempt(&other_birth).await, Err(BuildStateError::AttemptTaken { .. })), "the same path targeted to another birth is another action");
        assert_eq!(m.facts().await.unwrap().iter().filter(|f| matches!(f, BuildFact::Opened(_))).count(), 1, "the refused opens appended nothing");
    }

    #[tokio::test]
    async fn absorbing_projection_facts_is_idempotent() {
        let a = MemoryBuildStateAdapter::new();
        a.absorb(&log()).await;
        a.absorb(&log()).await;
        assert_eq!(fold(&a.facts().await.unwrap()), fold(&log()));
    }

    /// A Build log on a store root at `dir`.
    async fn open_at(dir: &Path) -> Result<FileJournal, BuildStateError> {
        FileJournal::open_on_root(&crate::store_root::StoreRoot::at(dir), &crate::test_support::tempdir("attempt-context"), &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &writer())).await
    }

    fn writer() -> crate::model::IncarnationId {
        crate::model::IncarnationId::mint()
    }

    /// Every Build record on the store root at `root`, sorted: `<build_id>/<key>` under `fabric/builds/`.
    fn record_files(root: &Path) -> Vec<String> {
        crate::store_log::keys_in(&root.join(crate::store_root::BUILDS_FILE), "build-record").unwrap()
    }

    fn tmp() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!("journal-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// CONTRACT (the Build log is read on async workers: the fabric-primary's change reads its Build
    /// through `AcceptedStore::current` -> `read_build` (accepted.rs `current`, fabric_status.rs
    /// `step_*`); `LocalBuildLog::derive_on_every_fact`: what is derived from the facts never lags a
    /// reader of them): while a push into the journal is held inside the installed derivation, a
    /// reader of the log on another thread (`read_build`, `list_active`) answers at once, from the log
    /// as it stood before the held fact: it never sees that fact before its derivation ran. Once the
    /// derivation ends, the push returns and the reader sees the fact. Must NOT happen: a reader
    /// waiting on a std lock behind a writer's whole-log derivation (a worker thread taken from the
    /// runtime for as long as the fold and its view locks take).
    /// The derivation reads the Builds the new facts belong to, the current Build and the Builds its
    /// caller holds derived state of, never another Build of the log: an adapter that keeps no log
    /// in memory serves it with per-Build reads.
    #[tokio::test]
    async fn the_derivation_reads_the_new_facts_builds_the_current_build_and_the_held_ones_only() {
        let m = MemoryBuildStateAdapter::new();
        m.absorb(&[BuildFact::Accepted(intent("b1"))]).await;
        let seen: Arc<Mutex<Vec<std::collections::BTreeSet<String>>>> = Arc::default();
        let held: Arc<Mutex<Vec<BuildId>>> = Arc::default();
        let (s2, h2) = (seen.clone(), held.clone());
        m.derive_on_every_fact(FactDerivation::new(move || h2.lock().unwrap().clone(), move |facts: &[BuildFact]| {
            s2.lock().unwrap().push(facts.iter().map(|f| f.build_id().to_string()).collect());
        }))
        .await;
        let (b1, b2) = (BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap().to_string(), BuildId::parse(&rdm_core::fixture_crockford60("b2")).unwrap().to_string());
        assert_eq!(seen.lock().unwrap().last().unwrap(), &[b1.clone()].into_iter().collect(), "installed: the current Build, b1");
        m.absorb(&[BuildFact::Accepted(BuildAccepted { submitted_at_ms: 2, ..intent("b2") })]).await;
        assert_eq!(seen.lock().unwrap().last().unwrap(), &[b1.clone(), b2.clone()].into_iter().collect(), "b2's acceptance: b2's own, and b1 still current until it is indexed");
        m.absorb(&[BuildFact::Claim(claim("b2", 1, "mesh1.admin.1"))]).await;
        assert_eq!(seen.lock().unwrap().last().unwrap(), &[b2.clone()].into_iter().collect(), "a fact of the current Build b2: b2 alone, b1 is neither touched, current nor held");
        held.lock().unwrap().push(BuildId::parse(&b1).unwrap());
        m.absorb(&[BuildFact::Claim(claim("b2", 2, "mesh1.admin.1"))]).await;
        assert_eq!(seen.lock().unwrap().last().unwrap(), &[b1, b2].into_iter().collect(), "b1 held: read with b2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reader_of_the_journal_never_waits_behind_a_push_held_mid_derivation() {
        let dir = tmp();
        let j = Arc::new(open_at(&dir).await.unwrap());
        let b1 = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        j.publish_accepted(&intent("b1")).await.unwrap();
        j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
        let held_op = "node.stop:mesh1.rpc.9";
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (entered_tx, release_rx) = (Mutex::new(entered_tx), Mutex::new(release_rx));
        j.derive_on_every_fact(FactDerivation::new(Vec::new, move |facts: &[BuildFact]| {
            if facts.iter().any(|f| matches!(f, BuildFact::Step(s) if s.operation == held_op)) {
                let _ = entered_tx.lock().unwrap().send(());
                let _ = release_rx.lock().unwrap().recv();
            }
        }))
        .await;
        let writer = tokio::spawn({
            let j = j.clone();
            async move { j.append_step_receipt(&step("b1", 1, held_op)).await }
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(std::time::Duration::from_secs(30)).expect("the push enters the derivation")).await.unwrap();
        // The reader, on a thread of its own: a reader blocked on the log's lock cannot answer.
        let (read_tx, read_rx) = std::sync::mpsc::channel();
        let reader = {
            let (j, b1) = (j.clone(), b1.clone());
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                let read = rt.block_on(async { (j.read_build(&b1).await, j.list_active().await) });
                let _ = read_tx.send(read);
            })
        };
        let answered = read_rx.recv_timeout(std::time::Duration::from_secs(2));
        let (read, active) = answered.expect("the reader answered while the push was held mid-derivation");
        let read = read.unwrap();
        assert!(read.steps.iter().all(|s| s.operation != held_op), "the held fact is not seen before its derivation ran: {:?}", read.steps);
        assert_eq!(active.unwrap().iter().map(|p| p.build_id.clone()).collect::<Vec<_>>(), vec![b1.clone()]);
        release_tx.send(()).unwrap();
        writer.await.unwrap().unwrap();
        reader.join().unwrap();
        assert!(j.read_build(&b1).await.unwrap().steps.iter().any(|s| s.operation == held_op), "once the derivation ran, the reader sees the fact");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_same_admin_restart_replays_the_journal_to_the_same_view() {
        let dir = tmp();
        let before = {
            let j = open_at(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
            j.append_step_receipt(&step("b1", 1, "node.stop:mesh1.rpc.2")).await.unwrap();
            j.publish_accepted(&intent("b2")).await.unwrap();
            (j.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap(), j.list_active().await.unwrap())
        };
        let j = open_at(&dir).await.unwrap(); // the admin restarted
        assert_eq!(j.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap(), before.0);
        assert_eq!(j.list_active().await.unwrap(), before.1);
        assert_eq!(j.claim_attempt(&claim("b1", 1, "mesh1.admin.2")).await.unwrap(), ClaimOutcome::Lost { holder: "mesh1.admin.1".into() });
        j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        let again = open_at(&dir).await.unwrap();
        assert_eq!(again.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap().state, BuildState::Complete);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 1: "a Build decoded from the journal whose
    /// paths name none ... is valid history with its births' rows as the record, and an admin restarted over
    /// an old store reads its current Build as it is, never as malformed"; durable format moved): a store
    /// whose current Build was written before `NodeSpec.executable` (its records carry no such field) is
    /// read by a restarted admin as its current Build, complete, every entry naming none; the next
    /// complete-topology request compiles over it. Must NOT happen: the restart refusing the record as
    /// unrecognised, or the old Build read as anything but current.
    #[tokio::test]
    async fn an_admin_restarted_over_a_store_written_before_the_executable_field_reads_its_current_build() {
        let dir = tmp();
        {
            let j = open_at(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
            j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        }
        // The records as a writer before the fields wrote them: no `executable` and no `tags` on any entry,
        // and, before Q166, the map named `path_config_by_path`, each entry without its `path` and its
        // `config`.
        // Each line's record is rewritten as its writer then wrote it and the line framed again around it
        // (its len and crc32), its op, format and key kept.
        let mut rewritten = 0;
        let file = dir.join(crate::store_root::BUILDS_FILE);
        let mut out = Vec::new();
        for line in std::fs::read_to_string(&file).unwrap().split('\n').filter(|l| !l.is_empty()) {
            let parsed = crate::store_log::read_record_bytes(line.as_bytes()).expect("a whole line");
            let mut text = parsed.record_json.clone();
            if text.contains("\"executable\":null") {
                text = text.replace(",\"executable\":null", "").replace(",\"tags\":[]", "").replace(",\"config\":{}", "").replace("\"spec_by_path\"", "\"path_config_by_path\"");
                while let Some(at) = text.find("{\"path\":\"") {
                    let end = text[at + 9..].find("\",").expect("the path's closing quote") + at + 9;
                    text.replace_range(at + 1..end + 2, "");
                }
                assert!(!text.contains("\"path\"") && !text.contains("\"config\"") && text.contains("\"path_config_by_path\""), "{text}");
                rewritten += 1;
            }
            out.extend(crate::store_log::encode_line(&parsed.op, &parsed.format, &parsed.key, text.as_bytes()));
        }
        std::fs::write(&file, out).unwrap();
        assert_eq!(rewritten, 1, "the accepted Build's record carries the field, once");
        let j = open_at(&dir).await.unwrap(); // the admin restarted over the old store
        let current = j.current().await.unwrap().expect("the old Build is current");
        assert_eq!((current.build_id.clone(), current.state), (BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), BuildState::Complete));
        assert_eq!(current.topology, intent("b1").topology, "read as it is: every entry names none");
        let admin = rdm_core::meta::NodeSpec { path: "mesh1.admin.1".parse().unwrap(), storage_persistence: rdm_core::meta::StorageMeta::Persistent { disposition: rdm_core::meta::PersistentDisposition::Preserve }, placement: Default::default(), executable: None, tags: Default::default(), config: Default::default() };
        let mut rpc = rdm_core::meta::NodeSpec { path: "mesh1.rpc.1".parse().unwrap(), storage_persistence: rdm_core::meta::StorageMeta::Ephemeral, placement: Default::default(), executable: None, tags: Default::default(), config: Default::default() };
        rpc.executable = Some(rdm_core::binding::Binding { launch_id: "rpc_node".into(), executable: "/b/rpc".into(), sha256: "b".into(), image: None });
        let change = crate::accepted::TopologyChange::Reconcile { topology_storage_provider: None, based_on: current.build_id.clone(), nodes: vec![admin, rpc] };
        let observed = crate::topology::Topology {
            fabric: crate::model::Fabric { id: crate::model::FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: vec![],
            nodes: vec![],
        };
        let next = crate::accepted::compile(&current.topology, &change, &observed).expect("the next request compiles over it");
        assert_eq!(next.meshes["mesh1"].nodes.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (build.md §5.3: "The Build facts are the Build journal on the fabric's shared store root
    /// (§5.3), each record at its own key, written by the fabric-primary, so a successor reads them
    /// there"; proposal executable-bindings-at-create.md rule 7, Luke 2026-10-24: "the executable is not
    /// gossiped, it's only in the node storage"): an admin that absorbed an accepted Build from a Build
    /// message (`FetchBuildFacts` 0x1F, a build.attempt.run's intent), which carries no executable, holds
    /// the accepted Build as the fabric-primary wrote it on the store root, every entry's executable
    /// with it, at the same place in its log. Must NOT happen: the message's copy kept over the stored
    /// record (a successor planning a Build's replacements against entries that name no executable).
    #[tokio::test]
    async fn an_admin_holds_the_accepted_build_as_the_store_root_records_it_not_as_a_build_message_carried_it() {
        let dir = tmp();
        let mut stored = intent("b1");
        let rpc: crate::model::PathName = "mesh1.rpc.1".parse().unwrap();
        let m = stored.topology.meshes.get_mut("mesh1").unwrap();
        m.nodes.insert(rpc.clone());
        let mut spec = rdm_core::meta::NodeSpec { path: rpc.clone(), storage_persistence: rdm_core::meta::StorageMeta::Ephemeral, placement: Default::default(), executable: None, tags: Default::default(), config: Default::default() };
        spec.executable = Some(rdm_core::binding::Binding { launch_id: "rpc_node".into(), executable: "/b/rpc".into(), sha256: "b".into(), image: None });
        m.set_spec(spec).unwrap();
        let writer = open_at(&dir).await.unwrap();
        writer.publish_accepted(&stored).await.unwrap();
        writer.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();

        // Another admin absorbs the Build as a Build message carries it: no executable.
        let other = open_at(&dir).await.unwrap();
        let mut heard = stored.clone();
        for c in heard.topology.meshes.get_mut("mesh1").unwrap().spec_by_path.values_mut() {
            c.executable = None;
        }
        other.absorb_facts(&[BuildFact::Accepted(heard.clone()), BuildFact::Claim(claim("b1", 1, "mesh1.admin.1"))]).await;
        let id = stored.build_id.clone();
        let read = other.read_build(&id).await.unwrap();
        assert_eq!(read.topology, stored.topology, "the accepted Build as the store root records it, its entries' executables with it");
        assert_eq!((read.attempt, read.executor.as_deref()), (1, Some("mesh1.admin.1")), "the rest of its log as it was");
        assert_eq!(other.current().await.unwrap().map(|c| c.topology), Some(stored.topology.clone()), "the current Build likewise");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn accepted_at(id: &str, at_ms: u64) -> BuildAccepted {
        BuildAccepted { submitted_at_ms: at_ms, ..intent(id) }
    }

    /// CONTRACT (R-BI3: "it reads the builds till it finds the last committed/completed build id and
    /// then uses that as known state"; "if we're LIFO it would be in-progress || completed"; "failed
    /// Builds are skipped. A failed Build is never current"): the acceptance-order index is read
    /// newest first and the first Build in progress or completed is current. A Build that realized
    /// its topology once stays the known state when a later attempt of it (a restart, proven drift:
    /// build.md §4.1, the topology unchanged) fails; a Build that never completed and failed is
    /// skipped; a forgotten one has left the views. Memory and the journal answer alike, and the
    /// journal answers the same after a restart replays it.
    #[tokio::test]
    async fn the_current_build_is_the_newest_in_progress_or_completed_and_a_failed_one_is_skipped() {
        let dir = tmp();
        let journal = open_at(&dir).await.unwrap();
        for s in [&MemoryBuildStateAdapter::new() as &dyn BuildStateAdapter, &journal] {
            let id = |b: &str| BuildId::parse(&rdm_core::fixture_crockford60(b)).unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::None, "an empty store holds no current Build");
            s.publish_accepted(&accepted_at("b0", 10)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b0")), "an accepted Build is current, in progress");
            s.claim_attempt(&claim("b0", 1, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt("b0", 1, AttemptOutcome::Converged)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b0")));
            s.publish_accepted(&accepted_at("b1", 20)).await.unwrap();
            s.claim_attempt(&claim("b1", 1, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Failed { reason: "x".into() , code: rdm_core::outcome::Code::FailedPrecondition })).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b0")), "the failed b1 is skipped: b0 is the known state");
            assert_eq!(s.newest_accepted_at_ms().await.unwrap(), Some(20), "the index's head is the failed b1, whatever its state");
            // A stop of b0's birth is an attempt of it: in progress while it runs, and b0 stays the known
            // state when it fails.
            s.open_attempt(&opened("b0", 2, AttemptReason::Stop)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b0")));
            s.claim_attempt(&claim("b0", 2, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt("b0", 2, AttemptOutcome::Failed { reason: "stop failed".into() , code: rdm_core::outcome::Code::FailedPrecondition })).await.unwrap();
            assert_eq!(s.read_build(&id("b0")).await.unwrap().state, BuildState::Failed);
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b0")), "an attempt of an unchanged topology that failed never makes an older Build current");
            // The failed b1 resubmitted: its next attempt makes it the Build in progress.
            s.open_attempt(&opened("b1", 2, AttemptReason::PrimaryMoved)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b1")));
            s.claim_attempt(&claim("b1", 2, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt("b1", 2, AttemptOutcome::Converged)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b1")));
            // A newer Build that failed and was forgotten leaves the views: b1 stays current.
            s.publish_accepted(&accepted_at("b2", 30)).await.unwrap();
            s.claim_attempt(&claim("b2", 1, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt("b2", 1, AttemptOutcome::Failed { reason: "y".into() , code: rdm_core::outcome::Code::FailedPrecondition })).await.unwrap();
            s.forget(&id("b2")).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b1")));
        }
        let replayed = open_at(&dir).await.unwrap(); // the admin restarted
        assert_eq!(replayed.current_build().await.unwrap(), CurrentBuild::Completed(BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()), "the journal's replay answers the same current Build");
        assert_eq!(replayed.newest_accepted_at_ms().await.unwrap(), Some(30));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (R-BI3: "Each accepted Build is its own entity in the builds store, written once"):
    /// the acceptance order is each Build's own stamp, written once in its accepted fact, so facts
    /// absorbed in any order (gossip, catch-up) give the same current Build: an older Build's facts
    /// arriving after a newer one's never make the older one current.
    #[tokio::test]
    async fn the_acceptance_order_is_each_builds_own_stamp_whatever_order_the_facts_arrive() {
        let newer = [BuildFact::Accepted(accepted_at("bnew", 20)), BuildFact::Claim(claim("bnew", 1, "a")), BuildFact::Attempt(attempt("bnew", 1, AttemptOutcome::Converged))];
        let older = [BuildFact::Accepted(accepted_at("bold", 10)), BuildFact::Claim(claim("bold", 1, "a")), BuildFact::Attempt(attempt("bold", 1, AttemptOutcome::Converged))];
        let (a, b) = (MemoryBuildStateAdapter::new(), MemoryBuildStateAdapter::new());
        a.absorb(&newer).await;
        a.absorb(&older).await;
        b.absorb(&older).await;
        b.absorb(&newer).await;
        for s in [&a, &b] {
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(BuildId::parse(&rdm_core::fixture_crockford60("bnew")).unwrap()));
        }
    }

    /// CONTRACT (ruling R-PU2: "The caller's resubmit opens the next attempt of the same Build ...
    /// that attempt's node.primary.update is a new transfer key, (build_id, attempt), never the Build id
    /// alone"): attempt 1 of b1 hands off and attempt 2 continues its run (run 1); attempt 2 fails,
    /// and the attempt a resubmit opens after it, attempt 3, begins run 3, as does attempt 4 continuing
    /// it. Must NOT happen: a resubmitted attempt in the failed run, or a hand-off starting a new one.
    #[tokio::test]
    async fn a_resubmit_after_a_failed_attempt_opens_a_new_run_and_a_hand_off_continues_its_run() {
        let m = MemoryBuildStateAdapter::new();
        let b1 = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        m.absorb(&[BuildFact::Accepted(intent("b1")), BuildFact::Claim(claim("b1", 1, "mesh1.admin.1")), BuildFact::Attempt(attempt("b1", 1, AttemptOutcome::HandedOff { to: "mesh1.admin.2".into() }))]).await;
        let p = m.read_build(&b1).await.unwrap();
        assert_eq!(p.run_of(p.attempt_to_drive()), 1, "the hand-off's next attempt continues run 1");
        m.absorb(&[BuildFact::Claim(claim("b1", 2, "mesh1.admin.2")), BuildFact::Attempt(attempt("b1", 2, AttemptOutcome::Failed { reason: "successor proven exited after committed transfer".into() , code: rdm_core::outcome::Code::FailedPrecondition }))]).await;
        let p = m.read_build(&b1).await.unwrap();
        assert_eq!((p.state, p.attempt_to_drive(), p.run_of(p.attempt_to_drive())), (BuildState::Failed, 3, 3), "the resubmit's attempt opens run 3");
        m.absorb(&[BuildFact::Claim(claim("b1", 3, "mesh1.admin.1")), BuildFact::Attempt(attempt("b1", 3, AttemptOutcome::HandedOff { to: "mesh1.admin.3".into() }))]).await;
        let p = m.read_build(&b1).await.unwrap();
        assert_eq!(p.run_of(p.attempt_to_drive()), 3, "attempt 4 continues run 3");
    }

    /// CONTRACT (node-recovery.md "Failure and receipts", Q159: the no-retry exception is scoped to an
    /// adoption abandoned because a live admin answered, never a generic name conflict): in mesh1's lane of
    /// one Build, the recovery of mesh1.rpc.1's exited birth fails on a name conflict (NameHeld's code), and
    /// an adoption launch's attempt for mesh1.admin.1's lost birth is abandoned because a live admin
    /// answered (the same code). The recovery's exit is still owed, so the mesh-primary submits it again;
    /// the abandoned adoption owes none. Must NOT happen: a recovery that failed on a name conflict dropped
    /// from the owed exits, or the abandoned adoption retried against a live admin.
    #[tokio::test]
    async fn only_an_adoption_abandoned_because_a_live_admin_answered_owes_no_exit() {
        let m = MemoryBuildStateAdapter::new();
        let b1 = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        let (rpc, admin): (crate::model::PathName, crate::model::PathName) = ("mesh1.rpc.1".parse().unwrap(), "mesh1.admin.1".parse().unwrap());
        let (rpc_birth, admin_birth) = ((crate::model::NodeId::mint(), crate::model::IncarnationId::mint()), (crate::model::NodeId::mint(), crate::model::IncarnationId::mint()));
        let lane = || Some("mesh1".to_string());
        let open = |attempt: u32, reason: AttemptReason, path: &crate::model::PathName, birth: &(crate::model::NodeId, crate::model::IncarnationId)| {
            BuildFact::Opened(AttemptOpened {
                mesh: lane(),
                build_id: b1.clone(),
                attempt,
                reason,
                action: Some(crate::accepted::AttemptAction::Recover { path: path.clone(), node_id: birth.0.clone(), from_incarnation: birth.1.clone(), backbone: false }),
                opened_by: "mesh1.admin.2".into(),
                opened_at_ms: 2,
                context: None,
            })
        };
        let claimed = |attempt: u32| BuildFact::Claim(BuildAttemptClaim { mesh: lane(), ..claim("b1", attempt, "mesh1.admin.2") });
        let name_held = |attempt: u32| BuildFact::Attempt(BuildAttemptReceipt { mesh: lane(), ..attempt_of("b1", attempt, AttemptOutcome::Failed { reason: "NameHeld".into(), code: rdm_core::outcome::Code::AlreadyExists }) });
        m.absorb(&[BuildFact::Accepted(intent("b1")), open(1, AttemptReason::Recovery, &rpc, &rpc_birth), claimed(1), name_held(1), open(2, AttemptReason::Adoption, &admin, &admin_birth), claimed(2), name_held(2)]).await;
        let owed: Vec<(String, u32)> = m.exits_owed(&b1, "mesh1").await.unwrap().into_iter().map(|o| (o.path.to_string(), o.attempt)).collect();
        assert_eq!(owed, vec![("mesh1.rpc.1".to_string(), 1)], "the recovery failed on a name conflict is owed again; the abandoned adoption is not");
    }

    /// CONTRACT (R-BI3: "a caller resubmits it"; one Build in flight): a failed Build may be driven
    /// again while it is newer than the current Build and the current Build is not in progress; a
    /// Build older than the current one, or any Build while another is in progress, is not.
    #[tokio::test]
    async fn a_failed_build_newer_than_the_current_one_is_drivable_and_an_older_one_is_not() {
        let s = MemoryBuildStateAdapter::new();
        let id = |b: &str| BuildId::parse(&rdm_core::fixture_crockford60(b)).unwrap();
        for (b, at, outcome) in [("b0", 10, AttemptOutcome::Failed { reason: "x".into() , code: rdm_core::outcome::Code::FailedPrecondition }), ("b1", 20, AttemptOutcome::Converged), ("b2", 30, AttemptOutcome::Failed { reason: "y".into() , code: rdm_core::outcome::Code::FailedPrecondition })] {
            s.publish_accepted(&accepted_at(b, at)).await.unwrap();
            s.claim_attempt(&claim(b, 1, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt(b, 1, outcome)).await.unwrap();
        }
        let held = &s;
        let p = |b: &'static str| async move { held.read_build(&BuildId::parse(&rdm_core::fixture_crockford60(b)).unwrap()).await.unwrap() };
        assert!(drivable(&s, &p("b1").await).await.unwrap(), "the current Build");
        assert!(drivable(&s, &p("b2").await).await.unwrap(), "a failed Build accepted after the current one");
        assert!(!drivable(&s, &p("b0").await).await.unwrap(), "a failed Build older than the current one");
        s.open_attempt(&opened("b1", 2, AttemptReason::Stop)).await.unwrap();
        assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b1")));
        assert!(!drivable(&s, &p("b2").await).await.unwrap(), "one Build in flight: not while the current one is in progress");
    }

    /// CONTRACT (node-recovery.md "A repeat submit answers AlreadyApplied ... never a second
    /// recovery"; "Steps"; proposal node-properties-own-commands.md §5 item 8: recovery is node.delete
    /// then node.create, no step 6): a recovery's exited birth is answered by the recovery index once
    /// the recovery's last operation, its node.create (`node.create:<path>`), completed at or after
    /// the deletion's attempt: its node.delete's `NodeDeleted` and `Complete` alone are not a
    /// recovery, nor is a create's `Complete` from before the deletion. The index is folded wherever a
    /// fact enters the log, so a restarted fabric-primary's journal replay and absorbed facts, in any
    /// order, answer the point read as an own append does, and the first recovering attempt stays the
    /// answer, naming the operation that deleted the birth.
    /// Must NOT happen: a recovery answered before its create completed (a resubmit then answers
    /// AlreadyApplied for a path no node was created at), a recovery left unanswered after its create
    /// completed (waiting on a node.update no plan runs), or a restarted admin answering None for a
    /// birth its journal holds recovered.
    #[tokio::test]
    async fn the_recovery_index_is_folded_on_append_replay_and_absorb() {
        let (path, node_id, incarnation): (crate::model::PathName, _, _) = ("mesh1.rpc.1".parse().unwrap(), crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        // A recovery's receipts as its two operations write them: the delete's NodeDeleted names
        // the exited birth (its lifecycle operation), and each operation's Complete commits no output.
        let receipt = |build: &str, attempt: u32, operation: &str, step: &str| BuildStepReceipt {
            mesh: None,
            build_id: BuildId::parse(&rdm_core::fixture_crockford60(build)).unwrap(),
            attempt,
            operation: format!("{operation}:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: (step == "NodeDeleted").then(|| {
                serde_json::to_value(rdm_core::LifecycleOp { build_id: build.into(), attempt, operation: format!("{operation}:{path}"), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }).unwrap()
            }),
            executor: Some("mesh1.admin.1".into()),
        };
        let opened = |build: &str, attempt: u32| AttemptOpened {
            mesh: None,
            build_id: BuildId::parse(&rdm_core::fixture_crockford60(build)).unwrap(),
            attempt,
            reason: AttemptReason::Recovery,
            action: Some(crate::accepted::AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone(), backbone: false }),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 1,
            context: None,
        };
        let deleted = format!("node.delete:{path}");
        let dir = tmp();
        {
            let j = open_at(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
            j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
            j.open_attempt(&opened("b1", 2)).await.unwrap();
            j.append_step_receipt(&receipt("b1", 2, "node.delete", "NodeDeleted")).await.unwrap();
            j.append_step_receipt(&receipt("b1", 2, "node.delete", "Complete")).await.unwrap();
            assert_eq!(j.recovery_of(&path, &node_id, &incarnation).await.unwrap(), None, "deleted, the create owed: not recovered");
            j.append_step_receipt(&receipt("b1", 1, "node.create", "Complete")).await.unwrap();
            assert_eq!(j.recovery_of(&path, &node_id, &incarnation).await.unwrap(), None, "the path's create from before the deletion: not recovered");
            j.append_step_receipt(&receipt("b1", 3, "node.create", "Complete")).await.unwrap();
            j.append_step_receipt(&receipt("b1", 4, "node.create", "Complete")).await.unwrap();
            assert_eq!(j.recovery_of(&path, &node_id, &incarnation).await.unwrap(), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 3, deleted.clone())), "own append: the first attempt that completed the recovery");
            assert_eq!(super::BirthIndex::recovery_of_rows(&rows_of(&j).await, &path, &node_id, &incarnation).await.unwrap(), j.recovery_of(&path, &node_id, &incarnation).await.unwrap(), "kept as keyed rows and read back per birth and lane");
        }
        let j = open_at(&dir).await.unwrap(); // the admin restarted
        assert_eq!(j.recovery_of(&path, &node_id, &incarnation).await.unwrap(), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 3, deleted.clone())), "journal replay");
        assert_eq!(j.recovery_of(&"mesh1.rpc.2".parse().unwrap(), &node_id, &incarnation).await.unwrap(), None, "another path's operation");
        let m = MemoryBuildStateAdapter::new();
        // Gossip delivers in any order: the create's Complete heard first, the attempt's opening last.
        m.absorb(&[BuildFact::Accepted(intent("b1")), BuildFact::Step(receipt("b1", 2, "node.create", "Complete"))]).await;
        assert_eq!(m.recovery_of(&path, &node_id, &incarnation).await.unwrap(), None, "a Complete alone names no birth");
        m.absorb(&[BuildFact::Step(receipt("b1", 2, "node.delete", "NodeDeleted")), BuildFact::Step(receipt("b1", 2, "node.delete", "Complete")), BuildFact::Opened(opened("b1", 2))]).await;
        assert_eq!(m.recovery_of(&path, &node_id, &incarnation).await.unwrap(), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 2, deleted.clone())), "absorbed, in any order");
        assert_eq!(super::BirthIndex::recovery_of_rows(&rows_of(&m).await, &path, &node_id, &incarnation).await.unwrap(), m.recovery_of(&path, &node_id, &incarnation).await.unwrap(), "absorbed, kept as keyed rows");
        // An application's adapter keeps its own index beside its store, fed every fact it holds.
        let mut index = BirthIndex::default();
        for fact in [BuildFact::Opened(opened("b1", 2)), BuildFact::Step(receipt("b1", 2, "node.delete", "Complete")), BuildFact::Step(receipt("b1", 2, "node.create", "Complete")), BuildFact::Step(receipt("b1", 2, "node.delete", "NodeDeleted")), BuildFact::Step(receipt("b1", 2, "node.delete", "NodeDeleted"))] {
            index.index(&fact);
        }
        assert_eq!(index.recovery_of(&path, &node_id, &incarnation), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 2, deleted.clone())), "a standalone index, fed in any order and one fact twice, answers as the adapter does");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": the mesh-primary submits again an exited
    /// birth whose workflow ended Failed; CLAUDE.md read rule: a point read through the index its
    /// caller uses, nothing held in memory between reads): `exits_owed(build, mesh)` answers an exited
    /// birth of `mesh` once the run its latest `Recover` open began ends with its last claimed attempt
    /// `Failed` and the birth is not done. A run in flight (opened and not claimed, or claimed with no
    /// verdict) is not owed; the next open of another birth bounds the run before it; a birth of
    /// another mesh or another Build is not answered; once the birth's `create-node`, the recovery's
    /// last operation, completes it is owed no longer. Folded wherever a fact enters the log, so a restarted admin's journal replay
    /// and facts absorbed in reverse order answer as own appends do. Must NOT happen: an owed exit
    /// lost by a restart, a run in flight answered owed, or a done birth answered owed.
    #[tokio::test]
    async fn an_exit_whose_workflow_ended_failed_is_owed_until_it_is_done_on_append_replay_and_absorb() {
        let birth = |path: &str| (path.parse::<crate::model::PathName>().unwrap(), crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let (r, q) = (birth("mesh2.rpc.1"), birth("mesh1.rpc.1"));
        let receipt = |b: &(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId), attempt: u32, operation: &str, step: &str| BuildStepReceipt {
            mesh: None,
            build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("{operation}:{}", b.0),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: (step == "NodeDeleted").then(|| {
                serde_json::to_value(rdm_core::LifecycleOp { build_id: rdm_core::fixture_crockford60("b1"), attempt, operation: format!("{operation}:{}", b.0), node_id: b.1.clone(), incarnation: b.2.clone(), name: b.0.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }).unwrap()
            }),
            executor: Some("mesh2.admin.1".into()),
        };
        let open = |b: &(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId), attempt: u32| AttemptOpened {
            mesh: None,
            build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            reason: AttemptReason::Recovery,
            action: Some(crate::accepted::AttemptAction::Recover { path: b.0.clone(), node_id: b.1.clone(), from_incarnation: b.2.clone(), backbone: false }),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 1,
            context: None,
        };
        let failed = |n: u32, why: &str| attempt("b1", n, AttemptOutcome::Failed { reason: why.into() , code: rdm_core::outcome::Code::FailedPrecondition });
        let owed = |b: &(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId), attempt: u32, reason: &str| OwedExit { path: b.0.clone(), node_id: b.1.clone(), incarnation: b.2.clone(), attempt, reason: reason.into() };
        let b1 = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        let mut facts: Vec<BuildFact> = Vec::new();
        let dir = tmp();
        {
            let j = open_at(&dir).await.unwrap();
            let mut put = async |f: BuildFact| {
                match &f {
                    BuildFact::Accepted(a) => j.publish_accepted(a).await.unwrap(),
                    BuildFact::Opened(o) => j.open_attempt(o).await.unwrap(),
                    BuildFact::Claim(c) => assert_eq!(j.claim_attempt(c).await.unwrap(), ClaimOutcome::Won),
                    BuildFact::Step(s) => j.append_step_receipt(s).await.unwrap(),
                    BuildFact::Attempt(a) => j.append_attempt_receipt(a).await.unwrap(),
                    BuildFact::Forget { .. } => unreachable!(),
                }
                facts.push(f);
            };
            for f in [BuildFact::Accepted(intent("b1")), BuildFact::Accepted(intent("b2")), BuildFact::Claim(claim("b1", 1, "mesh1.admin.1")), BuildFact::Attempt(attempt("b1", 1, AttemptOutcome::Converged)), BuildFact::Opened(open(&r, 2))] {
                put(f).await;
            }
            assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![], "opened, not claimed: in flight");
            put(BuildFact::Claim(claim("b1", 2, "mesh2.admin.1"))).await;
            put(BuildFact::Step(receipt(&r, 2, "node.delete", "NodeDeleted"))).await;
            put(BuildFact::Step(receipt(&r, 2, "node.delete", "Complete"))).await;
            assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![], "claimed, no verdict: in flight");
            put(BuildFact::Attempt(failed(2, "node.create: PrepareStorage"))).await;
            assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![owed(&r, 2, "node.create: PrepareStorage")], "deleted, its create failed: owed");
            // Another mesh's birth opens the next attempt and fails: it bounds the run before it.
            for f in [BuildFact::Opened(open(&q, 3)), BuildFact::Claim(claim("b1", 3, "mesh1.admin.1")), BuildFact::Attempt(failed(3, "q's create"))] {
                put(f).await;
            }
            assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![owed(&r, 2, "node.create: PrepareStorage")], "the next open bounds the run; the other mesh's birth is not answered");
            assert_eq!(j.exits_owed(&b1, "mesh1").await.unwrap(), vec![owed(&q, 3, "q's create")], "the other mesh answers its own");
            let rows = rows_of(&j).await;
            assert_eq!(super::BirthIndex::exits_owed_rows(&rows, &b1, "mesh2").await.unwrap(), j.exits_owed(&b1, "mesh2").await.unwrap(), "mesh2's, kept as keyed rows");
            assert_eq!(super::BirthIndex::exits_owed_rows(&rows, &b1, "mesh1").await.unwrap(), j.exits_owed(&b1, "mesh1").await.unwrap(), "mesh1's, kept as keyed rows");
            assert_eq!(j.exits_owed(&BuildId::parse(&rdm_core::fixture_crockford60("b2")).unwrap(), "mesh2").await.unwrap(), vec![], "another Build holds none");
            for f in [BuildFact::Opened(open(&r, 4)), BuildFact::Claim(claim("b1", 4, "mesh2.admin.2"))] {
                put(f).await;
            }
            assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![], "submitted again and claimed: in flight");
            put(BuildFact::Attempt(failed(4, "node.create: PrepareStorage again"))).await;
        }
        let j = open_at(&dir).await.unwrap(); // the admin restarted, or another took the seat
        assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![owed(&r, 4, "node.create: PrepareStorage again")], "journal replay: the latest run's Failed attempt");
        for f in [
            BuildFact::Opened(open(&r, 5)),
            BuildFact::Claim(claim("b1", 5, "mesh2.admin.2")),
            BuildFact::Step(receipt(&r, 5, "node.create", "Complete")),
            BuildFact::Attempt(attempt("b1", 5, AttemptOutcome::Converged)),
        ] {
            match &f {
                BuildFact::Opened(o) => j.open_attempt(o).await.unwrap(),
                BuildFact::Claim(c) => assert_eq!(j.claim_attempt(c).await.unwrap(), ClaimOutcome::Won),
                BuildFact::Step(s) => j.append_step_receipt(s).await.unwrap(),
                BuildFact::Attempt(a) => j.append_attempt_receipt(a).await.unwrap(),
                _ => unreachable!(),
            }
            facts.push(f);
        }
        assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![], "created: done, owed no longer");
        assert_eq!(j.exits_owed(&b1, "mesh1").await.unwrap(), vec![owed(&q, 3, "q's create")], "the other mesh's run is untouched");
        let m = MemoryBuildStateAdapter::new();
        facts.reverse();
        m.absorb(&facts).await;
        assert_eq!(m.exits_owed(&b1, "mesh2").await.unwrap(), vec![], "absorbed in reverse order: done");
        assert_eq!(m.exits_owed(&b1, "mesh1").await.unwrap(), vec![owed(&q, 3, "q's create")], "absorbed in reverse order: the other mesh's owed exit");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-recovery.md:5-7, recovery is for "a member that exits outside any workflow";
    /// "Failure and receipts": "An exit during a workflow belongs to that workflow"): a birth a
    /// `retire-node` operation deleted (its `NodeDeleted` receipt names the exact
    /// birth, and the operation's `Complete` at that attempt or later is held) is answered by the
    /// recovery index as that workflow's attempt, folded on append, journal replay and absorb alike.
    /// Must NOT happen: `None` for a birth a workflow deleted, which admits a recovery of it.
    #[tokio::test]
    async fn a_birth_a_delete_deleted_is_answered_by_that_workflow() {
        let path: crate::model::PathName = "mesh2.rpc.2".parse().unwrap();
        let receipt = |kind: &str, attempt: u32, step: &str, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId| BuildStepReceipt {
            mesh: None,
            build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            operation: format!("{kind}:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: (step == "NodeDeleted").then(|| {
                serde_json::to_value(rdm_core::LifecycleOp { build_id: rdm_core::fixture_crockford60("b1"), attempt, operation: format!("{kind}:{path}"), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }).unwrap()
            }),
            executor: Some("mesh2.admin.1".into()),
        };
        let replaced = (crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let deleted = (crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let dir = tmp();
        {
            let j = open_at(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.append_step_receipt(&receipt("node.delete", 2, "NodeDeleted", &replaced.0, &replaced.1)).await.unwrap();
            assert_eq!(j.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), None, "deleted, the delete not complete: its next attempt finishes it");
            j.append_step_receipt(&receipt("node.delete", 2, "Complete", &replaced.0, &replaced.1)).await.unwrap();
            j.append_step_receipt(&receipt("node.delete", 3, "NodeDeleted", &deleted.0, &deleted.1)).await.unwrap();
            j.append_step_receipt(&receipt("node.delete", 3, "Complete", &deleted.0, &deleted.1)).await.unwrap();
            assert_eq!(j.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 2, format!("node.delete:{path}"))), "own append: the first delete that deleted its birth");
            assert_eq!(j.recovery_of(&path, &deleted.0, &deleted.1).await.unwrap(), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 3, format!("node.delete:{path}"))), "own append: the delete that deleted the birth");
        }
        let j = open_at(&dir).await.unwrap(); // the admin restarted
        assert_eq!(j.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 2, format!("node.delete:{path}"))), "journal replay");
        assert_eq!(j.recovery_of(&"mesh2.rpc.1".parse().unwrap(), &replaced.0, &replaced.1).await.unwrap(), None, "another path's operation");
        let m = MemoryBuildStateAdapter::new();
        m.absorb(&[BuildFact::Accepted(intent("b1")), BuildFact::Step(receipt("node.delete", 2, "Complete", &replaced.0, &replaced.1))]).await;
        assert_eq!(m.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), None, "a Complete alone names no birth");
        m.absorb(&[BuildFact::Step(receipt("node.delete", 2, "NodeDeleted", &replaced.0, &replaced.1))]).await;
        assert_eq!(m.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 2, format!("node.delete:{path}"))), "absorbed, in either order");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (Q28 A: "a mesh-audit ratchet for the store layout"; R-SR1, R-SR3): the store root's
    /// layout, byte for byte: a Build record is one line of `builds.jsonl` at `<build_id>/<key>`
    /// (`build-record/1`, build.md §5.3), its object exactly the protocol's (`len`, `op`, `format`,
    /// `key`, the record nested, `crc32`); and a mesh's in-flight lock (`mesh-in-flight/2`, naming the
    /// birth that took it, R-SR5). A layout
    /// that moves moves this cell with it (`durable format moved`).
    #[tokio::test]
    async fn the_store_layout_is_frozen_byte_for_byte() {
        let d = crate::test_support::tempdir("store-layout");
        let root = crate::store_root::StoreRoot::at(&d);
        let read = |rel: &str| String::from_utf8(std::fs::read(d.join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))).unwrap();
        let b1 = rdm_core::fixture_crockford60("b1");
        let j = open_at(&d).await.unwrap();
        j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        let lines = crate::store_log::read_file(&d.join(crate::store_root::BUILDS_FILE)).unwrap();
        assert_eq!(lines.len(), 1);
        let op = lines[0].op.clone().unwrap();
        let record = format!(r#"{{"seq":1,"fact":{{"fact":"attempt","build_id":"{b1}","attempt":1,"outcome":"converged"}}}}"#);
        let line = crate::store_log::encode_line(&op, "build-record/1", &format!("{b1}/1-result"), record.as_bytes());
        assert_eq!(read(crate::store_root::BUILDS_FILE), String::from_utf8(line).unwrap());
        use crate::storage::MeshStorage as _;
        let claims = crate::storage::FileMeshStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap();
        let row = crate::mesh_claims::InFlight { build_id: BuildId::parse(&b1).unwrap(), lane: None, attempt: 1, claimant: "mesh1.admin.1".into(), claimant_node: None };
        assert_eq!(claims.take_in_flight("mesh2", &row).await.unwrap(), crate::mesh_claims::Taken::Won);
        let lock = read("locks/in-flight/mesh2");
        let by: serde_json::Value = serde_json::from_str::<serde_json::Value>(&lock).unwrap()["by"].clone();
        assert_eq!(lock, format!(r#"{{"format":"mesh-in-flight/2","record":{{"build_id":"{b1}","attempt":1,"claimant":"mesh1.admin.1"}},"by":{by}}}"#));
        assert!(by.as_array().is_some_and(|b| b.len() == 2), "the lock names the birth that took it: {by}");
    }

    /// CONTRACT (R-SR3; rdm#89: the journal opens without folding): a line of `builds.jsonl` that is not
    /// a Build record refuses the journal's read by name, the file and the offset named, never read as
    /// no record; a journal opened over it opens, reading nothing, and its first read is refused the
    /// same way. Must NOT happen: the facts before it answered as the journal.
    #[tokio::test]
    async fn a_corrupt_build_line_is_refused_by_name() {
        let dir = tmp();
        let j = open_at(&dir).await.unwrap();
        j.publish_accepted(&intent("b1")).await.unwrap();
        let file = dir.join(crate::store_root::BUILDS_FILE);
        let at = std::fs::metadata(&file).unwrap().len() + 1;
        std::io::Write::write_all(&mut std::fs::OpenOptions::new().append(true).open(&file).unwrap(), b"\nnot json\n").unwrap();
        let reopened = open_at(&dir).await.expect("the open reads nothing");
        let err = reopened.list_active().await.unwrap_err();
        assert!(matches!(&err, BuildStateError::Io(e) if e.contains(&file.display().to_string()) && e.contains(&format!("byte offset {at}"))), "{err}");
        let err = j.list_active().await.unwrap_err();
        assert!(matches!(&err, BuildStateError::Io(e) if e.contains(&format!("byte offset {at}"))), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// PRD §1.4 / §7: no test, and no code outside this module and the file backend's construction
    /// (`storage_backend.rs`), opens a Build journal; the one exception is store_root_boot's
    /// preparation of a store root before
    /// an admin starts on it (R-SB1, Luke 2026-10-13: each cell "prepares a durable store
    /// root through the storage adapters, with no previous process"): it writes the
    /// accepted Build the admin then reloads, and decides nothing from it.
    #[test]
    fn nothing_but_this_module_opens_a_build_journal() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().parent().unwrap();
        let mut offenders = Vec::new();
        let mut stack = vec![root.join("crates"), root.join("tools")];
        while let Some(dir) = stack.pop() {
            for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let p = e.path();
                if p.is_dir() {
                    if !p.ends_with("target") {
                        stack.push(p);
                    }
                } else if p.extension().is_some_and(|x| x == "rs") && !p.ends_with("build_state.rs") && !p.ends_with("rdm-admin/src/storage_backend.rs") && !p.ends_with("rdm-admin/tests/store_root_boot.rs") {
                    let text = std::fs::read_to_string(&p).unwrap_or_default();
                    if text.contains("FileJournal::open") || text.contains("build-journal.jsonl") {
                        offenders.push(p);
                    }
                }
            }
        }
        assert!(offenders.is_empty(), "only build_state.rs, the file backend's construction and store_root_boot's prepared store root may open a journal: {offenders:?}");
    }

    /// An attempt of `id` opened on `mesh`'s drift lane (R-BI7): the recovery of `path`'s exited birth.
    fn lane_open(id: &str, mesh: &str, attempt: u32, path: &str) -> AttemptOpened {
        AttemptOpened {
            mesh: Some(mesh.into()),
            build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(),
            attempt,
            reason: AttemptReason::Recovery,
            action: Some(crate::accepted::AttemptAction::Recover { path: path.parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint(), backbone: false }),
            opened_by: format!("{mesh}.admin.1"),
            opened_at_ms: 6,
            context: None,
        }
    }

    /// A claim of `mesh`'s drift-lane attempt for `who`, decided by `who` (a mesh-primary claims its own).
    fn lane_claim(id: &str, mesh: &str, attempt: u32, who: &str) -> BuildAttemptClaim {
        BuildAttemptClaim { claimant: Some(who.into()), mesh: Some(mesh.into()), build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, executor: who.into() }
    }

    /// CONTRACT (R-BI7 caveat 2, proposal Part C: "The claim key becomes `(build_id, mesh, attempt)`.
    /// Each mesh-primary claims drift attempts for its own mesh by insert-and-fail on its own mesh's
    /// rows"): attempt 1 of one Build opened on mesh1's lane and on mesh2's lane are two attempts, and
    /// each mesh-primary's claim of its own is `Won`; a second claimant of mesh2's attempt 1 is `Lost`
    /// naming the first. Must NOT happen: mesh2's claim refused because mesh1 holds the Build's
    /// attempt number, or two winners of one mesh's attempt.
    #[tokio::test]
    async fn claims_of_one_attempt_number_in_two_meshes_both_win_and_a_second_claimant_in_one_mesh_loses() {
        let m = MemoryBuildStateAdapter::new();
        m.publish_accepted(&intent("b1")).await.unwrap();
        m.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
        m.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        m.open_attempt(&lane_open("b1", "mesh1", 1, "mesh1.rpc.2")).await.expect("mesh1's drift lane opens its attempt 1");
        m.open_attempt(&lane_open("b1", "mesh2", 1, "mesh2.rpc.2")).await.expect("mesh2's drift lane opens its own attempt 1");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh1", 1, "mesh1.admin.1")).await.unwrap(), ClaimOutcome::Won, "mesh1's mesh-primary claims its attempt");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh2", 1, "mesh2.admin.1")).await.unwrap(), ClaimOutcome::Won, "mesh2's attempt 1 is another key than mesh1's");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh2", 1, "mesh2.admin.2")).await.unwrap(), ClaimOutcome::Lost { holder: "mesh2.admin.1".into() }, "one winner per (build, mesh, attempt)");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh2", 1, "mesh2.admin.1")).await.unwrap(), ClaimOutcome::Won, "re-claiming your own attempt is idempotent");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh3", 1, "mesh3.admin.1")).await.unwrap(), ClaimOutcome::NotOpen { next: None }, "a lane with no attempt opened has nothing to claim");
    }

    /// CONTRACT (R-BI7 caveats 2 and 3; proposal Part C: "A recovery in mesh 2 is no longer refused
    /// because a node.create runs in mesh 1"): the fold reads the fabric lane (the topology Build's own
    /// attempts) and each mesh's drift lane apart. With the fabric lane's attempt running in mesh1 and
    /// mesh2's drift attempt claimed, the Build view shows both running, each with its own attempt,
    /// executor and claimant, and the drift lane's steps never enter the fabric lane's. A Build whose
    /// fabric lane completed while a drift lane is open is in progress (the LIFO read's
    /// `InProgress`), and once the drift attempt converges it is completed again. Must NOT happen:
    /// mesh2's attempt 1 read as a stale fabric attempt (ignored) or as the fabric lane's next attempt.
    #[tokio::test]
    async fn the_fold_reads_the_fabric_attempts_and_each_meshs_drift_attempts_apart() {
        let m = MemoryBuildStateAdapter::new();
        m.publish_accepted(&intent("b1")).await.unwrap();
        let id = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        let mut fabric = claim("b1", 1, "mesh1.admin.1");
        fabric.claimant = Some("mesh1.admin.1".into());
        m.claim_attempt(&fabric).await.unwrap();
        m.open_attempt(&lane_open("b1", "mesh2", 1, "mesh2.rpc.2")).await.unwrap();
        m.claim_attempt(&lane_claim("b1", "mesh2", 1, "mesh2.admin.1")).await.unwrap();
        let mut drift_step = step("b1", 1, "node.delete:mesh2.rpc.2");
        drift_step.mesh = Some("mesh2".into());
        m.append_step_receipt(&drift_step).await.unwrap();
        let p = m.read_build(&id).await.unwrap();
        assert_eq!((p.state, p.attempt, p.executor.as_deref()), (BuildState::Running, 1, Some("mesh1.admin.1")), "the fabric lane's attempt runs in mesh1: {p:?}");
        assert!(p.steps.is_empty(), "the drift lane's step is not the fabric lane's: {:?}", p.steps);
        let view = serde_json::to_value(&p).unwrap();
        assert_eq!(view["claimant"], "mesh1.admin.1", "the fabric lane's claimant: {view}");
        let lane = &view["lanes"]["mesh2"];
        assert_eq!((lane["state"].as_str(), lane["attempt"].as_u64(), lane["executor"].as_str(), lane["claimant"].as_str()), (Some("running"), Some(1), Some("mesh2.admin.1"), Some("mesh2.admin.1")), "mesh2's drift lane runs its own attempt 1: {view}");
        assert_eq!(lane["steps"].as_array().map(Vec::len), Some(1), "the drift step is the lane's: {view}");

        // The fabric lane converges; the drift lane keeps the Build in progress until its verdict.
        m.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        assert_eq!(m.read_build(&id).await.unwrap().state, BuildState::Complete);
        assert_eq!(m.current_build().await.unwrap(), CurrentBuild::InProgress(id.clone()), "an open drift lane keeps the Build in progress");
        let mut verdict = attempt("b1", 1, AttemptOutcome::Converged);
        verdict.mesh = Some("mesh2".into());
        m.append_attempt_receipt(&verdict).await.unwrap();
        assert_eq!(m.current_build().await.unwrap(), CurrentBuild::Completed(id.clone()), "both lanes settled: the known state");
        assert_eq!(serde_json::to_value(m.read_build(&id).await.unwrap()).unwrap()["lanes"]["mesh2"]["state"], "complete");
    }

    /// CONTRACT (durable format moved, R-BI7): a journal line written before lanes and claimants
    /// existed (no `mesh`, no `claimant`) decodes as the fabric lane, with no claimant recorded; a
    /// line with them round-trips them.
    #[test]
    fn a_claim_line_written_with_no_mesh_and_no_claimant_decodes_with_neither() {
        let id = rdm_core::fixture_crockford60("b1");
        let old: BuildFact = serde_json::from_str(&format!(r#"{{"fact":"claim","build_id":"{id}","attempt":2,"executor":"mesh1.admin.1"}}"#)).unwrap();
        assert!(matches!(&old, BuildFact::Claim(c) if c.mesh.is_none() && c.claimant.is_none()), "{old:?}");
        let new = BuildFact::Claim(lane_claim("b1", "mesh2", 1, "mesh2.admin.1"));
        assert_eq!(serde_json::from_str::<BuildFact>(&serde_json::to_string(&new).unwrap()).unwrap(), new);
        let fabric = serde_json::to_value(BuildFact::Claim(claim("b1", 2, "mesh1.admin.1"))).unwrap();
        assert!(fabric.get("mesh").is_none() && fabric.get("claimant").is_none(), "a fabric-lane line is written as before: {fabric}");
    }

    /// CONTRACT (Q211: the Build's ordered list is steps; the rename moves no durable format): an attempt
    /// that carries steps is written to `builds.jsonl` under the tags the journal already holds,
    /// `"reason":"recipe"` and `"action":"recipe"`, and a line written before the rename reads back as
    /// `AttemptReason::Steps` carrying `AttemptAction::Steps`. Views and spans name the reason `steps`.
    #[test]
    fn an_attempt_of_steps_keeps_the_journal_tags_written_before_the_rename() {
        let node: crate::model::PathName = "mesh1.rpc.1".parse().unwrap();
        let mut steps = opened("b1", 2, AttemptReason::Steps);
        steps.action = Some(crate::accepted::AttemptAction::Steps { steps: vec![rdm_core::build::Step::Stop { node }], pinned: vec![] });
        let line = serde_json::to_value(BuildFact::Opened(steps.clone())).unwrap();
        assert_eq!((line["reason"].as_str(), line["action"]["action"].as_str()), (Some("recipe"), Some("recipe")), "{line}");
        let old = line.to_string();
        assert!(old.contains(r#""reason":"recipe""#) && old.contains(r#""action":"recipe""#), "{old}");
        assert_eq!(serde_json::from_str::<BuildFact>(&old).unwrap(), BuildFact::Opened(steps));
        assert_eq!(AttemptReason::Steps.as_str(), "steps");
    }

    /// CONTRACT (i146 Q61 A; build.md §5.2: "Restarting every node-admin at once must not erase the facts
    /// needed to reconstruct the Fabric and continue an active Build", §5.3: "a successor reads them
    /// there"): a fabric-primary's committed Build facts (the accepted Build, its attempt claim, a step
    /// receipt, the attempt's completion) stand on the shared root before the fabric's first
    /// ready, so the same fabric-primary restarted (a new process of its birth on the root)
    /// and a successor opened on the root each read every one of them, and the Build complete. Must NOT
    /// happen: a committed fact lost with the process that held it.
    #[tokio::test]
    async fn every_build_fact_committed_before_the_first_ready_is_recovered_by_a_restart_and_by_a_successor() {
        let d = crate::test_support::tempdir("q61-recovery");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric_id = crate::model::FabricId::mint();
        let birth = writer();
        {
            let root = crate::store_root::StoreRoot::open(&store, &fabric_id).unwrap();
            let j = FileJournal::open_on_root(&root, &crate::test_support::tempdir("attempt-context"), &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &birth)).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
            j.append_step_receipt(&step("b1", 1, "admit-node:mesh1.admin.1")).await.unwrap();
            j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
            // The process ends here.
        }
        let id = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        for (who, writer) in [("the same fabric-primary restarted", birth.clone()), ("a successor", writer())] {
            let root = crate::store_root::StoreRoot::open(&store, &fabric_id).unwrap();
            let j = FileJournal::open_on_root(&root, &crate::test_support::tempdir("attempt-context"), &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &writer)).await.unwrap();
            let kinds: Vec<&str> = j.facts().await.unwrap().iter().map(|f| match f {
                BuildFact::Accepted(_) => "accepted",
                BuildFact::Claim(_) => "claim",
                BuildFact::Step(_) => "step",
                BuildFact::Attempt(_) => "completion",
                _ => "other",
            }).collect();
            assert_eq!(kinds, ["accepted", "claim", "step", "completion"], "{who} reads every committed fact on the root");
            assert_eq!(j.read_build(&id).await.unwrap().state, BuildState::Complete, "{who}: the Build is complete");
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (build.md §5.3: "The Build journal holds one record per fact at the fact's own key: one
    /// row at `build_id`, one at `(build_id, mesh, attempt)`, one claim at `(build_id, mesh, attempt)`,
    /// one receipt per step of an attempt. No record is written twice"; R-BI6, Q61): a `FileJournal`
    /// opened on a `StoreRoot` appends each fact as one line at its own key (`<build_id>/<key>`) of the
    /// root's `builds.jsonl`, when it is committed, never under a per-admin data dir; an attempt in a mesh is keyed with its mesh; a fact written
    /// again writes nothing; and a successor, open already or opened after a restart, reads every
    /// record from the root without gossip.
    #[tokio::test]
    async fn the_build_journal_is_one_record_per_fact_on_the_shared_store_root_and_a_successor_reads_it_there() {
        let d = crate::test_support::tempdir("build-records-on-root");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric_id = crate::model::FabricId::mint();
        let root = crate::store_root::StoreRoot::open(&store, &fabric_id).unwrap();
        let j = FileJournal::open_on_root(&root, &crate::test_support::tempdir("attempt-context"), &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &writer())).await.unwrap();
        j.publish_accepted(&intent("b1")).await.unwrap();
        j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
        j.append_step_receipt(&step("b1", 1, "admit-node:mesh1.admin.1")).await.unwrap();
        j.append_step_receipt(&step("b1", 1, "admit-node:mesh1.admin.1")).await.unwrap();
        let b1 = rdm_core::fixture_crockford60("b1");
        assert_eq!(
            record_files(root.dir()),
            [format!("{b1}/1-claim"), format!("{b1}/1-step-admit-node:mesh1.admin.1-DeployRuntime"), format!("{b1}/accepted")],
            "each committed fact is one record at its own key on the store root, and a receipt written again writes nothing"
        );
        assert_eq!(crate::store_log::read_file(&root.dir().join(crate::store_root::BUILDS_FILE)).unwrap().len(), 3, "one line per fact: the receipt written again is not appended");
        j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        j.open_attempt(&lane_open("b1", "mesh2", 1, "mesh2.rpc.2")).await.unwrap();
        assert_eq!(j.claim_attempt(&lane_claim("b1", "mesh2", 1, "mesh2.admin.1")).await.unwrap(), ClaimOutcome::Won);
        let mut in_mesh = step("b1", 1, "node.delete:mesh2.rpc.2");
        in_mesh.mesh = Some("mesh2".into());
        j.append_step_receipt(&in_mesh).await.unwrap();
        let mut result = attempt("b1", 1, AttemptOutcome::Converged);
        result.mesh = Some("mesh2".into());
        j.append_attempt_receipt(&result).await.unwrap();
        let in_mesh2: Vec<String> = record_files(root.dir()).into_iter().filter(|k| k.starts_with(&format!("{b1}/mesh2-1-"))).collect();
        assert_eq!(in_mesh2, [format!("{b1}/mesh2-1-claim"), format!("{b1}/mesh2-1-opened"), format!("{b1}/mesh2-1-result"), format!("{b1}/mesh2-1-step-node.delete:mesh2.rpc.2-DeployRuntime")], "the attempt in mesh2 is keyed (build_id, mesh, attempt)");
        let id = BuildId::parse(&b1).unwrap();
        let successor = FileJournal::open_on_root(&crate::store_root::StoreRoot::at(root.dir()), &crate::test_support::tempdir("attempt-context"), &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &writer())).await.unwrap();
        let read = serde_json::to_value(successor.read_build(&id).await.unwrap()).unwrap();
        assert_eq!(read["state"], "complete", "{read}");
        assert_eq!(read["lanes"]["mesh2"]["state"], "complete", "a successor reads the attempt in mesh2 from the store root: {read}");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn the_journal_replays_the_original_step_and_attempt_codes() {
        use rdm_core::outcome::Code;
        for code in [Code::Unknown, Code::DeadlineExceeded, Code::Unavailable] {
            let dir = tmp();
            let receipt = BuildStepReceipt { outcome: StepOutcome::Failed { reason: "same explanation".into(), code }, ..step("b1", 1, "node.stop:mesh1.rpc.2") };
            {
                let journal = open_at(&dir).await.unwrap();
                journal.publish_accepted(&intent("b1")).await.unwrap();
                journal.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
                journal.append_step_receipt(&receipt).await.unwrap();
                journal.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Failed { reason: "same explanation".into(), code })).await.unwrap();
            }
            let replay = open_at(&dir).await.unwrap();
            let facts = replay.facts().await.unwrap();
            assert!(facts.iter().any(|f| matches!(f, BuildFact::Step(s) if s == &receipt)));
            assert!(facts.iter().any(|f| matches!(f, BuildFact::Attempt(a) if a.outcome == AttemptOutcome::Failed { reason: "same explanation".into(), code })));
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// CONTRACT (the Build journal wait, i143 e12.s89; i144 e6): each writer's turn of the journal is on
    /// its own span. A turn that waits for another names how long it waited (`waited_ms`) and the turn
    /// holding the writer when its wait began (`held_by`); every turn names how long it held the writer
    /// (`held_ms`) and how much of that was the store root's own calls (`store_ms`). Must NOT happen: a
    /// wait on the writer no span names, or a holder unnamed.
    #[tokio::test]
    async fn a_journal_turn_that_waits_names_its_wait_and_the_turn_holding_the_writer() {
        let dir = tmp();
        let journal = Arc::new(open_at(&dir).await.unwrap());
        let capture = crate::span_capture::Capture::new("journal-turn-wait");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let first = journal.turn("publish_accepted").await;
        let waiter = {
            let journal = journal.clone();
            tokio::spawn(async move {
                let _turn = journal.turn("append_step_receipt").await;
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        drop(first);
        waiter.await.unwrap();
        let turns = capture.named("rdm.node_admin.build.update.via-journal-turn");
        let of = |op: &str| turns.iter().find(|t| t.attributes.get("op").map(String::as_str) == Some(op)).unwrap_or_else(|| panic!("no turn of {op}: {turns:?}")).attributes.clone();
        let (holder, waiter) = (of("publish_accepted"), of("append_step_receipt"));
        let ms = |a: &std::collections::BTreeMap<String, String>, k: &str| a.get(k).and_then(|v| v.parse::<u64>().ok()).unwrap_or_else(|| panic!("{k} unrecorded: {a:?}"));
        assert!(ms(&waiter, "waited_ms") >= 100, "the waiting turn names its wait: {waiter:?}");
        assert!(waiter.get("held_by").is_some_and(|h| h.starts_with("publish_accepted (")), "the waiting turn names the turn holding the writer: {waiter:?}");
        assert!(ms(&holder, "held_ms") >= 100 && ms(&holder, "waited_ms") < 100, "the holding turn names its hold, not a wait: {holder:?}");
        assert_eq!(ms(&holder, "store_ms"), 0, "a turn that made no store call names none: {holder:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (the Build journal's read path; i144 e6, `fabric_primary.rs:204`): a read of the journal
    /// reads the store root without holding the writer, so a writer's turn never waits behind a
    /// reader's disk I/O. With the blocking pool's one thread held, a reader's sync is queued on
    /// the disk; a writer's turn begun then takes the writer at once and names no holder. Must NOT
    /// happen: a writer's turn whose `held_by` names a reader, or a reader holding the writer across the
    /// disk.
    #[test]
    fn a_writers_turn_never_waits_behind_a_readers_store_io() {
        let dir = tmp();
        let capture = crate::span_capture::Capture::new("journal-read-holds-no-writer");
        let setup = {
            let dir = dir.clone();
            Box::pin(async move {
                let j = Arc::new(open_at(&dir).await.unwrap());
                j.publish_accepted(&intent("b1")).await.unwrap();
                j
            })
        };
        // The cell's runtime runs on this thread: every span of the journal is the capture's.
        tracing::dispatcher::with_default(&capture.dispatch, || {
            crate::test_support::op_waits_on_the_blocking_pool(setup, |j: Arc<FileJournal>| {
                Box::pin(async move {
                    // The reader runs first: its sync's first disk call queues behind the held thread.
                    let reader = tokio::spawn({
                        let j = j.clone();
                        async move { j.current_build().await.unwrap() }
                    });
                    tokio::task::yield_now().await;
                    j.publish_accepted(&intent("b2")).await.unwrap();
                    reader.await.unwrap();
                })
            })
        });
        let turns = capture.named("rdm.node_admin.build.update.via-journal-turn");
        let writes: Vec<_> = turns.iter().filter(|t| t.attributes.get("op").map(String::as_str) == Some("publish_accepted")).map(|t| t.attributes.clone()).collect();
        assert_eq!(writes.len(), 2, "the setup's acceptance and the one begun beside the reader: {turns:?}");
        assert!(writes.iter().all(|w| w.get("held_by").map(String::as_str) == Some("")), "no writer's turn waited behind a reader: {writes:#?}");
        let _ = std::fs::remove_dir_all(&dir);
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
        let attempt = AttemptOutcome::Failed { reason: "r".into(), code: Code::DeadlineExceeded };
        assert_eq!(rdm_transport::wire::encode(&attempt).unwrap(), vec![1, 1, b'r', 4]);
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
