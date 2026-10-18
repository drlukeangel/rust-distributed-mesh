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
//!   root (`fabric/builds/` written by the fabric-primary, `meshes/<mesh>/builds/`
//!   written by each mesh's primary under the writer ladder; R-BI6), so a
//!   successor reads it there.

use crate::accepted::{AttemptAction, FabricTopology, TopologyChange};
use crate::build::BuildId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
    /// A restart of an exact birth was requested.
    Restart,
    /// The previous attempt's executor is gone; another admin continues the Build.
    AuthorityMoved,
    /// `POST /api/nodes/{name}/connections/delete`: node.connections.delete { All }.
    DeleteConnections,
    /// A standalone stop of an exact birth was requested (`node.stop:<path>`).
    Stop,
    /// A standalone start of a parked birth was requested (`node.start:<path>`).
    Start,
    /// A mesh-primary proved a member's exact birth exited and submitted its recovery
    /// (`node.delete:<path>`, then `node.create:<path>`, node-recovery.md "Steps").
    Recovery,
    /// The delete of a stopped birth at its `.old` path, or of a deleted mesh's last admin, was
    /// requested (`node.delete:<path>`, node.delete's one key).
    Delete,
    /// A recipe was submitted: one Build of node primitives (build.md §1).
    Recipe,
}

impl AttemptReason {
    /// The reason's name as it appears in views and spans.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::ProvenDrift => "proven-drift",
            Self::Restart => "restart",
            Self::AuthorityMoved => "authority-moved",
            Self::DeleteConnections => "node.connections.delete",
            Self::Stop => "stop",
            Self::Start => "start",
            Self::Recovery => "recovery",
            Self::Delete => "delete",
            Self::Recipe => "recipe",
        }
    }
}

/// An attempt opened on a Build by the authority (the first one by acceptance itself): what it
/// is for, and the fenced action it carries. Insert-and-fail on `(build_id, attempt)`: two
/// authorities proving the same drift open one attempt, never two.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptOpened {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt number.
    pub attempt: u32,
    /// Why the attempt was opened.
    pub reason: AttemptReason,
    /// The fenced action the attempt carries, when it carries one.
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
        /// Where a failed `TerminateRuntime` stopped (node-recovery.md step 4): none for every other
        /// step, and for a terminate whose runtime exited while the operating system still held its
        /// addresses. Positional on the Build topic's wire (no skip): a line written before it carries
        /// none.
        #[serde(default)]
        stage: Option<TerminateStage>,
        /// The standard code captured where the failure occurred.
        #[serde(default = "rdm_core::outcome::legacy_failed_code")]
        code: rdm_core::outcome::Code,
    },
}

impl StepOutcome {
    /// A Failed outcome with no stage.
    pub fn failed(reason: impl Into<String>) -> Self {
        Self::Failed { reason: reason.into(), stage: None, code: rdm_core::outcome::Code::FailedPrecondition }
    }
}

/// Where a failed `TerminateRuntime` stopped (node-recovery.md step 4's end, i144 R21).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminateStage {
    /// The step's own inspection read the exact runtime `OsStopped` or `Unknown`: termination was
    /// not requested. It voids the hold.
    Inspection {
        /// What the inspection read.
        verdict: InspectionVerdict,
    },
    /// Termination was requested and the provider returned an error.
    Provider {
        /// The provider's error.
        error: String,
    },
    /// Termination was requested and exit was not proven; a later inspection may still find
    /// `Exited`.
    Unproven,
}

impl TerminateStage {
    /// Whether no hold submits the birth's terminate again (node-recovery.md step 4's end: "a
    /// person decides").
    pub fn stops_resubmission(&self) -> bool {
        matches!(self, Self::Provider { .. } | Self::Unproven)
    }

    /// The stage as a span names it.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Inspection { .. } => "inspection",
            Self::Provider { .. } => "provider",
            Self::Unproven => "unproven",
        }
    }
}

/// What a failed `TerminateRuntime`'s own inspection read: termination was not requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionVerdict {
    /// The operating system holds the exact runtime suspended.
    OsStopped,
    /// The provider could not tell the exact runtime's state.
    Unknown,
}

/// A recovery attempt of a birth whose `TerminateRuntime` failed with a stage
/// ([`BuildStateAdapter::terminate_failures_of`], i144 R21).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedTerminate {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt's lane (`None` the fabric lane, `Some(mesh)` that mesh's drift lane).
    pub mesh: Option<String>,
    /// The attempt.
    pub attempt: u32,
    /// When the attempt was opened, once its `Opened` fact is held.
    pub opened_at_ms: Option<u64>,
    /// Where its `TerminateRuntime` stopped.
    pub stage: TerminateStage,
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

/// Where a Build is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    /// No executor is running an attempt: the Build is accepted, or its next attempt is open.
    Pending,
    /// An executor holds and runs an attempt.
    Running,
    /// Every step is complete.
    Complete,
    /// The last attempt failed.
    Failed,
}

/// The folded Build view (`GET /api/builds?id=`).
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
    /// The fenced action the current attempt carries, when it carries one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<AttemptAction>,
    /// The fenced action each claimed attempt carried, by attempt (from its `Opened` fact). A
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
    /// later attempt reopens it (a restart, a recovery, proven drift: attempts of an unchanged
    /// topology, build.md §4.1), so a Build that once realized its topology stays the known state
    /// whatever such an attempt ends with ([`CurrentBuild`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u32>,
    /// The attempts whose executor reported its run's end by the attempt's verdict (`HandedOff` or
    /// `Converged`): what such a run completed is done. A run whose executor was lost reported nothing,
    /// and its successor re-checks the world for what it left.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub reported: std::collections::BTreeSet<u32>,
    /// The admin that decided the current fabric-lane attempt's claim, as its claim recorded it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimant: Option<String>,
    /// Each mesh's drift lane, by mesh (R-BI7 caveat 2): the attempts its mesh-primary opened,
    /// claimed and ended on this Build, numbered per mesh from 1. The fields above are the fabric
    /// lane's alone, and nothing of a drift lane moves them.
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
}

impl Lane {
    /// Whether the lane holds an attempt open: opened and not yet claimed, or claimed and running.
    pub fn is_open(&self) -> bool {
        matches!(self.state, BuildState::Pending | BuildState::Running)
    }
}

impl BuildProjection {
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
fn current_of(p: &BuildProjection) -> Option<CurrentBuild> {
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
    /// A lane write of an authority that no longer holds its lane's seat (Q28: `RejectedStaleSeat`
    /// from the store): the seat record names another live birth, or the revision is sealed. Nothing
    /// was written.
    StaleSeat {
        /// The lane key the write met (its seal's), or the lane when no key was reached.
        at: String,
        /// The revision the write was made under, when one was held.
        revision: Option<u64>,
        /// The record or seal that refused it, in words.
        detail: String,
    },
    /// Another writer's fact took the lane key first (insert-and-fail): nothing was written, that
    /// fact is read, and the write is decided again against it.
    LaneTaken {
        /// The key taken.
        at: String,
    },
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
    /// The committed fabric seat record names another live birth: this admin's Build log decides
    /// nothing from that record's revision (the fence is sticky for this birth).
    SeatYielded {
        /// The admin whose Build log yielded the seat.
        node: String,
        /// The record that moved the seat, in words.
        by: String,
        /// The admin (path.name) a committed seat transfer moved the seat to, when it named one.
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
            Self::StaleSeat { at, revision, detail } => write!(f, "stale seat at {at}{}: {detail}; nothing is written", revision.map(|r| format!(" (revision {r})")).unwrap_or_default()),
            Self::LaneTaken { at } => write!(f, "the lane key {at} holds another writer's fact"),
            Self::SeatYielded { node, by, .. } => write!(f, "{node} no longer holds the fabric-primary seat: the committed seat row moved it ({by}); its Build log decides nothing any more"),
            Self::AttemptTaken { build_id, attempt, held, wanted } => {
                write!(f, "attempt {attempt} of Build {build_id} is already open for {held}; this open asked for {wanted} and lost the number")
            }
        }
    }
}

/// Fold an ordered fact log into Build views. Deterministic and idempotent:
/// the same log gives the same views, and a duplicated fact changes nothing.
/// The winning claim of an attempt is the first one appended.
pub fn fold(facts: &[BuildFact]) -> BTreeMap<BuildId, BuildProjection> {
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
                    if let Some(lane) = out.get_mut(&c.build_id).and_then(|p| p.lanes.get_mut(mesh)) {
                        if c.attempt == lane.attempt + 1 && !matches!(lane.state, BuildState::Complete) {
                            lane.attempt = c.attempt;
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
/// retire operation completed with no `NodeDeleted` step of the same operation after it, and every
/// `NodeRestarting` step whose restart (`node.restart:<path>`, the op it carries) has no `Complete`
/// step yet and whose attempt is not `Converged`. An attempt's `Converged` verdict ends every
/// restart of that attempt: the fabric-primary records it from the executor's stream terminal, which
/// follows every step's `Complete` receipt durable on the executor (node-rpc-envelope.md: progress
/// waits on no gossip), so the restart's own `Complete` need not have reached this log. Derived from
/// the Build facts as each fact enters the log, never stored: a successor primary publishes the same
/// overlays from the same facts.
pub(crate) fn in_flight_ops(facts: &[BuildFact], builds: &BTreeMap<BuildId, BuildProjection>) -> InFlight {
    let mut out = InFlight::default();
    // An attempt has one verdict, the first appended (as `fold` reads it), in its own lane: the
    // terminal verdicts by (Build, lane, attempt). `out.attempts_converged` is the fabric lane's
    // alone (its reader keys a restart by (Build, attempt), and only the fabric lane restarts); a
    // removal's close is handed on by the removal's own key, decided here in its own lane.
    let mut decided: std::collections::BTreeSet<(&BuildId, Option<&str>, u32)> = std::collections::BTreeSet::new();
    let mut converged: std::collections::BTreeSet<(String, Option<String>, u32)> = std::collections::BTreeSet::new();
    let mut failed: std::collections::BTreeSet<(String, Option<String>, u32)> = std::collections::BTreeSet::new();
    for f in facts {
        if let BuildFact::Attempt(a) = f {
            if decided.insert((&a.build_id, a.mesh.as_deref(), a.attempt)) && builds.contains_key(&a.build_id) {
                let key = (a.build_id.to_string(), a.mesh.clone(), a.attempt);
                match a.outcome {
                    AttemptOutcome::Converged => {
                        if a.mesh.is_none() {
                            out.attempts_converged.insert((a.build_id.to_string(), a.attempt));
                        }
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
        // The fabric lane's steps, then each drift lane's: a removal or restart in any lane holds
        // its overlay.
        for lane_steps in std::iter::once(&p.steps).chain(p.lanes.values().map(|l| &l.steps)) {
        for s in lane_steps {
            if s.step == "Complete" && s.outcome == StepOutcome::Complete && s.operation.starts_with("node.restart:") && s.mesh.is_none() {
                out.restarts_completed.insert((s.build_id.to_string(), s.attempt, s.operation.clone()));
            }
            if s.outcome != StepOutcome::Complete || (s.step != "NodeDeleting" && s.step != "NodeRestarting") {
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
            let closed = if s.step == "NodeDeleting" {
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
                lane_steps.iter().any(|t| t.step == "NodeDeleted" && t.operation == s.operation && t.outcome == StepOutcome::Complete) || by.is_some()
            } else {
                converged.contains(&verdict) || lane_steps.iter().any(|t| t.step == "Complete" && t.attempt == op.attempt && t.operation == op.operation && t.outcome == StepOutcome::Complete)
            };
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
    /// The restarts whose `node.restart:<path>` `Complete` is receipted, by `(build_id, attempt, operation)`:
    /// the facts end that hold of that operation in that Build attempt.
    pub restarts_completed: std::collections::BTreeSet<(String, u32, String)>,
    /// The attempts whose verdict is `Converged`, by (Build, attempt): the facts end every restart
    /// hold of that attempt in that Build.
    pub attempts_converged: std::collections::BTreeSet<(String, u32)>,
    /// The removals (`NodeDeleting`) whose own attempt, in its own lane, has a terminal verdict, by
    /// the removal's key (`LifecycleOp::key`), each `attempt-failed` or `attempt-converged`: the
    /// facts end that hold. A restart's hold is not one.
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

/// A completed `NodeDeleting`/`NodeRestarting` step whose recorded operation cannot be read.
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
    /// Tell the fabric the Fabric record, its identity, on the Build topic. A local-only adapter has
    /// no fabric to tell.
    async fn publish_fabric(&self, _record: &crate::fabric_storage::FabricIdentity) -> Result<(), BuildStateError> {
        Ok(())
    }
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
    /// Insert-and-fail on `(build_id, attempt)`. Only the fabric-primary calls this on its own
    /// log to decide a claim (`build_claim::ClaimDoor`); an executor never claims from its own.
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError>;
    /// Seal, after the committed seat move `moved`, the Build lane of every seat it moved at the
    /// revision it moved from (Q35: "Whoever commits the move seals, after its seat-move row stands"),
    /// written by `by`, the move's writer. A log held in one process has no lanes to seal.
    async fn seal_moved(&self, _moved: &crate::fabric_storage::SeatMove, _by: &rdm_core::handover::HandoverBirth) -> Result<(), BuildStateError> {
        Ok(())
    }
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
    async fn claim_in_flight(&self, claim: &BuildAttemptClaim, claims: &crate::mesh_claims::MeshClaims, mesh: &str, row: &crate::mesh_claims::InFlight) -> Result<InFlightClaim, BuildStateError> {
        match claims.take(mesh, row).await.map_err(|e| BuildStateError::Io(format!("mesh {mesh}'s in-flight row: {e}")))? {
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
    /// exit proof. The first such attempt. A point read by the birth: every adapter answers it from
    /// its index (or delegates to the adapter it decorates), never by reading the fact log.
    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError>;
    /// Every recovery attempt of the birth `(node_id, incarnation)`, in any Build and lane, whose
    /// `TerminateRuntime` receipt is Failed with a stage, oldest first (node-recovery.md step 4's
    /// end, i144 R21: the hold reads it before it submits the birth's terminate). A point read by the
    /// birth: every adapter answers it from its index (or delegates to the adapter it decorates),
    /// never by reading the fact log.
    async fn terminate_failures_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Vec<FailedTerminate>, BuildStateError>;
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
    /// The path name this admin's membership view holds (`Some` once the fabric Build topic joined
    /// with the endpoint's location book).
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
    if s.step != "NodeDeleted" || s.outcome != StepOutcome::Complete {
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
        Some(crate::accepted::AttemptAction::Restart { path, from_incarnation }) => format!("restart of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::DeleteConnections { path, from_incarnation }) => format!("node.connections.delete of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Stop { path, from_incarnation }) => format!("stop of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Start { path, from_incarnation }) => format!("start of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Recover { path, node_id, from_incarnation }) => format!("recovery of {path} from exited {node_id} at {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Delete { path, from_incarnation }) => format!("delete of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Recipe { steps, .. }) => format!("recipe of {} step(s): {}", steps.len(), steps.iter().map(crate::build::recipe_step_key).collect::<Vec<_>>().join(", ")),
        None => "no fenced action".to_string(),
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
    /// The recovery index, in three parts the facts fill in whatever order they arrive (gossip and
    /// catch-up deliver in any order; once all three are held the answer is the same in every
    /// order, while a log that holds a recovery's deletion and its `Complete` but not yet its
    /// `Opened` answers it as a standalone retire's): every deleting workflow's (`retire-node`)
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
    /// else of the deleting operation itself. Filled by [`FactLog::index`], the one place a fact
    /// enters the log (own appends, absorbed facts and a journal's replay alike); forgetting a Build
    /// leaves all three, as it leaves the receipts.
    recoveries_opened: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), Vec<(LaneOf, u32)>>,
    /// The mesh.delete completion index: every `MeshDeleted` receipt by the final primary its
    /// manifest names, to its Build, attempt and operation ([`BuildStateAdapter::mesh_deleted_of`]).
    mesh_deleted: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), Vec<(BuildId, u32, String)>>,
    /// The index of [`BuildStateAdapter::exits_owed`], per Build, filled by [`FactLog::index`] in
    /// whatever order the facts arrive: the exited birth each attempt opened with
    /// `AttemptAction::Recover` names, by attempt (the first open of an attempt)...
    exit_opens: std::collections::HashMap<LaneOf, BTreeMap<u32, (crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId)>>,
    /// ...every attempt opened, whatever it carries (the next one ends the run of the one before)...
    opens: std::collections::HashMap<LaneOf, std::collections::BTreeSet<u32>>,
    /// ...and every attempt claimed or decided, with its verdict once one is held: the first
    /// appended, as [`fold`] reads it.
    verdicts: std::collections::HashMap<LaneOf, BTreeMap<u32, Option<AttemptOutcome>>>,
    /// The index of [`BuildStateAdapter::terminate_failures_of`] (i144 R21), filled by
    /// [`FactLog::index`] in whatever order the facts arrive: the stage of each attempt's Failed
    /// `TerminateRuntime` receipt, by its lane and attempt (the first appended)...
    terminate_failures: std::collections::HashMap<(LaneOf, u32), TerminateStage>,
    /// ...and when each recovery attempt was opened, by its lane and attempt. Forgetting a Build
    /// leaves both, as it leaves the receipts: a person's forget never lifts a refused terminate.
    recovery_opened_at: std::collections::HashMap<(LaneOf, u32), u64>,
    /// The acceptance-order index (R-BI3): every Build accepted, by its own acceptance stamp
    /// `(submitted_at_ms, build_id)`, entered once by its `Accepted` fact and never rewritten, in
    /// whatever order the facts arrive. Read newest first by [`current_build`].
    accepted_order: std::collections::BTreeSet<(u64, BuildId)>,
    /// Each Build's own facts, by their position in `facts`: a Build is read by folding its facts
    /// alone ([`read_own`]).
    by_build: std::collections::HashMap<BuildId, Vec<usize>>,
}

/// A lane of a Build, as the attempt-keyed indexes key it: the Build, and `None` for its fabric lane
/// or `Some(mesh)` for that mesh's drift lane (R-BI7). One attempt number names one attempt per lane.
type LaneOf = (BuildId, Option<String>);

/// A derivation over the whole fact log, run before any reader sees the facts a writer adds
/// ([`LocalBuildLog::derive_on_every_fact`]).
pub type FactDerivation = Arc<dyn Fn(&[BuildFact]) + Send + Sync>;

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
        if let BuildFact::Step(s) = fact {
            let lane: LaneOf = (s.build_id.clone(), s.mesh.clone());
            if let Some((path, node_id, incarnation)) = recovery_deleted(s) {
                let held = self.recovery_deletes.entry((node_id, incarnation)).or_default();
                if !held.iter().any(|(p, o, l, a)| *p == path && *o == s.operation && *l == lane && *a == s.attempt) {
                    held.push((path, s.operation.clone(), lane, s.attempt));
                }
            } else if recovery_completed(s) {
                self.recovery_completes.entry((lane, s.operation.clone())).or_default().insert(s.attempt);
            } else if let (true, StepOutcome::Failed { stage: Some(stage), .. }) = (s.step == crate::deployment::pipeline::RetireStep::TerminateRuntime.name(), &s.outcome) {
                self.terminate_failures.entry((lane, s.attempt)).or_insert_with(|| stage.clone());
            } else if let Some(birth) = mesh_deleted_final(s) {
                let held = self.mesh_deleted.entry(birth).or_default();
                if !held.iter().any(|(b, a, o)| *b == s.build_id && *a == s.attempt && *o == s.operation) {
                    held.push((s.build_id.clone(), s.attempt, s.operation.clone()));
                }
            }
        }
        if let BuildFact::Opened(AttemptOpened { build_id, attempt, mesh, opened_at_ms, action: Some(crate::accepted::AttemptAction::Recover { path, node_id, from_incarnation }), .. }) = fact {
            let lane: LaneOf = (build_id.clone(), mesh.clone());
            self.recovery_opened_at.entry((lane.clone(), *attempt)).or_insert(*opened_at_ms);
            let opened = self.recoveries_opened.entry((node_id.clone(), from_incarnation.clone())).or_default();
            if !opened.contains(&(lane.clone(), *attempt)) {
                opened.push((lane.clone(), *attempt));
            }
            self.exit_opens.entry(lane).or_default().entry(*attempt).or_insert_with(|| (path.clone(), node_id.clone(), from_incarnation.clone()));
        }
        match fact {
            BuildFact::Opened(o) => {
                self.opens.entry((o.build_id.clone(), o.mesh.clone())).or_default().insert(o.attempt);
            }
            BuildFact::Claim(c) => {
                self.verdicts.entry((c.build_id.clone(), c.mesh.clone())).or_default().entry(c.attempt).or_insert(None);
            }
            BuildFact::Attempt(a) => {
                let held = self.verdicts.entry((a.build_id.clone(), a.mesh.clone())).or_default().entry(a.attempt).or_insert(None);
                if held.is_none() {
                    *held = Some(a.outcome.clone());
                }
            }
            BuildFact::Accepted(a) => {
                self.accepted_order.insert((a.submitted_at_ms, a.build_id.clone()));
            }
            _ => {}
        }
        self.by_build.entry(fact.build_id().clone()).or_default().push(at);
    }


    /// The point read of [`BuildStateAdapter::recovery_of`]: the birth's `NodeDeleted` receipts at
    /// `path`, each answered by the first `Complete` at its attempt or later of the operation that
    /// ends its workflow: a recovery's `node.create:<path>` when a recovery of the birth was opened in
    /// that Build at or before the deletion's attempt, else the deleting operation itself. The first
    /// such attempt, with the deleting operation.
    fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Option<(BuildId, Option<String>, u32, String)> {
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

    /// The point read of [`BuildStateAdapter::terminate_failures_of`]: every recovery of the birth
    /// opened in any Build and lane whose `TerminateRuntime` failed with a stage, oldest first (by
    /// its opening time, then attempt).
    fn terminate_failures_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Vec<FailedTerminate> {
        let Some(opened) = self.recoveries_opened.get(&(node_id.clone(), incarnation.clone())) else { return Vec::new() };
        let mut failed: Vec<FailedTerminate> = opened
            .iter()
            .filter_map(|(lane, attempt)| {
                let stage = self.terminate_failures.get(&(lane.clone(), *attempt))?;
                Some(FailedTerminate { build_id: lane.0.clone(), mesh: lane.1.clone(), attempt: *attempt, opened_at_ms: self.recovery_opened_at.get(&(lane.clone(), *attempt)).copied(), stage: stage.clone() })
            })
            .collect();
        failed.sort_by_key(|f| (f.opened_at_ms.unwrap_or(0), f.attempt));
        failed
    }

    /// The point read of [`BuildStateAdapter::mesh_deleted_of`]: the first attempt's.
    fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Option<(BuildId, u32, String)> {
        self.mesh_deleted.get(&(node_id.clone(), incarnation.clone()))?.iter().min_by_key(|(_, a, _)| *a).cloned()
    }

    /// The point read of [`BuildStateAdapter::exits_owed`]: per exited birth of `mesh`, the run its
    /// latest claimed `Recover` open in `build_id` began, up to the next attempt opened in the Build;
    /// owed when the run's last claimed attempt holds the verdict `Failed` and [`FactLog::recovery_of`]
    /// does not answer the birth. An open with no claim (no entry in `verdicts`) starts no run.
    fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Vec<OwedExit> {
        // The fabric lane's recoveries of the mesh's births, and the mesh's own drift lane's.
        let mut owed: Vec<OwedExit> = [None, Some(mesh.to_string())].into_iter().flat_map(|lane| self.exits_owed_in(&(build_id.clone(), lane), mesh)).collect();
        owed.sort_by(|a, b| a.path.cmp(&b.path).then(a.attempt.cmp(&b.attempt)));
        owed
    }

    /// [`FactLog::exits_owed`] in one lane of the Build.
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
                let Some(AttemptOutcome::Failed { reason , .. }) = verdict else { return None };
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
        derive(&next);
    }
    let mut log = log.lock().unwrap();
    let base = log.facts.len();
    for (i, f) in new.iter().enumerate() {
        log.index(f, base + i);
    }
    log.facts = next;
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
        BuildFact::Accepted(i) => !known(facts, &i.build_id),
        BuildFact::Opened(o) => opened(facts, &o.build_id, o.mesh.as_deref(), o.attempt).is_none(),
        BuildFact::Claim(c) => claim_holder(facts, &c.build_id, c.mesh.as_deref(), c.attempt).is_none(),
        _ => !facts.contains(f),
    }
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
}

impl MemoryBuildStateAdapter {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
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

/// The local fact log a Build-topic adapter holds and absorbs the fabric's facts into: in memory
/// for runs that need no restart survival, or the admin's own journal (`builds.storage`).
#[async_trait]
pub trait LocalBuildLog: BuildStateAdapter {
    /// Merge facts heard from the fabric (append the unseen ones; intents, opens and claims are
    /// insert-and-fail, receipts deduplicate).
    async fn absorb_facts(&self, facts: &[BuildFact]);
    /// Run `derive` over the facts held now, then over the whole log each time facts enter it,
    /// whoever put them there (an own append or an absorbed fact), before any reader sees them and
    /// before the append returns: what is derived from the facts never lags a reader of them. It runs
    /// under the writer's turn, never under the lock readers take.
    async fn derive_on_every_fact(&self, derive: FactDerivation);
}

#[async_trait]
impl LocalBuildLog for MemoryBuildStateAdapter {
    async fn absorb_facts(&self, facts: &[BuildFact]) {
        self.absorb(facts).await
    }

    async fn derive_on_every_fact(&self, derive: FactDerivation) {
        let mut w = self.writer.lock().await;
        derive(&snapshot(&self.log));
        w.0 = Some(derive);
    }
}

#[async_trait]
impl LocalBuildLog for FileJournal {
    async fn derive_on_every_fact(&self, derive: FactDerivation) {
        let mut w = self.turn("derive_on_every_fact").await;
        let _ = self.sync(&w).await;
        derive(&snapshot(&self.log));
        w.0 = Some(derive);
    }

    async fn absorb_facts(&self, facts: &[BuildFact]) {
        let w = self.turn("absorb_facts").await;
        let _ = self.sync(&w).await;
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

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        let held = snapshot(&self.log);
        if !known(&held, &opened.build_id) {
            return Err(BuildStateError::UnknownBuild(opened.build_id.clone()));
        }
        if admit_open(&held, opened)? {
            self.put(&w, BuildFact::Opened(opened.clone()));
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
        self.put(&w, BuildFact::Claim(claim.clone()));
        Ok(ClaimOutcome::Won)
    }

    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.absorb(&[BuildFact::Claim(claim.clone())]).await;
        Ok(())
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.put(&w, BuildFact::Step(receipt.clone()));
        Ok(())
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.put(&w, BuildFact::Attempt(receipt.clone()));
        Ok(())
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        Ok(snapshot(&self.log).as_ref().clone())
    }

    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        Ok(self.log.lock().unwrap().recovery_of(path, node_id, incarnation))
    }

    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        Ok(self.log.lock().unwrap().mesh_deleted_of(node_id, incarnation))
    }

    async fn terminate_failures_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Vec<FailedTerminate>, BuildStateError> {
        Ok(self.log.lock().unwrap().terminate_failures_of(node_id, incarnation))
    }

    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError> {
        Ok(self.log.lock().unwrap().exits_owed(build_id, mesh))
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.put(&w, BuildFact::Forget { build_id: build_id.clone() });
        Ok(())
    }
}

/// The retired Build journal file (one JSON line per fact, one file per writer): a store root holding
/// one is refused by name. The facts are the lanes' keys ([`crate::build_lanes`]).
const REFUSED_JOURNAL_FILE: &str = "build-journal.jsonl";

/// The seat a birth writes each lane under (Q28: "`N` is the committed fabric seat revision the
/// writer holds"), from the lane's seat record as this birth holds it.
pub type LaneRevisions = Arc<dyn Fn(&crate::build_lanes::LaneId) -> LaneSeat + Send + Sync>;

/// A lane's seat record as one birth holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneSeat {
    /// The record's revision (0 while none is held): every write to the lane is made under it, so a
    /// write of a revision a seat move sealed is refused at the seal.
    pub revision: u64,
    /// `Ok` when the record names this birth, none, or a birth proven gone; `Err` naming the record
    /// when it names another live birth. A decision of the lane (an acceptance, an open, a claim, a
    /// forget) is this birth's to write only while it holds the seat; a receipt of an attempt it
    /// executes is written under the record's revision whoever holds it.
    pub holds: Result<(), String>,
}

/// The seals a birth owes on a lane before it appends under `revision` (Q35: "the successor appends
/// only once both stand"; a crash between the seat-move row and its seal is completed by the next
/// fold): one for each earlier revision a committed seat move moved the lane's seat from, naming that
/// move. A revision an election stepped holds no seal: its holder is proven gone.
pub type SealsOwed = Arc<dyn Fn(crate::build_lanes::LaneId, u64) -> futures_util::future::BoxFuture<'static, Result<Vec<crate::build_lanes::LaneSeal>, String>> + Send + Sync>;

/// `builds.storage`: the Build facts on the fabric's shared store root, one key per fact in the lane
/// of the seat that writes it ([`crate::build_lanes`]; R-BI6, Q28, Q35): `fabric/builds/` the
/// fabric-primary's, `meshes/<mesh>/builds/` each mesh-primary's. A fact counts once its key stands,
/// so a successor reads it there; a fact whose key another writer took first is decided again
/// against what that writer wrote.
pub struct FileJournal {
    lanes: crate::build_lanes::Lanes,
    root_dir: PathBuf,
    own_mesh: String,
    holds_fabric_seat: std::sync::OnceLock<Arc<dyn Fn() -> bool + Send + Sync>>,
    holds_mesh_seat: std::sync::OnceLock<Arc<dyn Fn() -> bool + Send + Sync>>,
    revisions: std::sync::OnceLock<LaneRevisions>,
    seals: std::sync::OnceLock<SealsOwed>,
    /// The lane key of every claim this log read or wrote, by its attempt: what an in-flight row
    /// names (Q35).
    claim_keys: Mutex<std::collections::HashMap<(BuildId, Option<String>, u32), crate::build_lanes::LaneKey>>,
    /// The replayed facts and their indexes. Held only for a snapshot, a point read or a
    /// publication's swap, never across the disk, a fold or the derivation.
    log: Mutex<FactLog>,
    /// The writer's turn: decide on a snapshot, insert the fact's key on the blocking pool, then
    /// publish, holding the derivation each publication runs. An async lock, so a writer waiting on
    /// another's insert or derivation yields its worker. A reader takes it only when no turn holds it,
    /// to take what it read from the lanes, and holds it across no disk.
    writer: tokio::sync::Mutex<Derived>,
    /// The turn holding `writer` and since when: what a turn that waits names as its holder.
    turn_of: Mutex<Option<(&'static str, std::time::Instant)>>,
    /// The time the holding turn spent in the lane store's own calls (its reads, appends, seals and
    /// the seals it owes), in nanoseconds: what its span names apart from its wait.
    turn_lane_ns: std::sync::atomic::AtomicU64,
    /// The blocking calls of the holder of `writer` (a turn or a reader's sync), from its lane calls:
    /// how many, how long they queued for a blocking thread, how long they ran.
    holder_io: Mutex<Option<crate::blocking::IoTime>>,
    /// The facts the holder of `writer` read from the lanes.
    holder_facts: std::sync::atomic::AtomicU64,
    /// What the last holder of `writer` did with it, written when it let it go: what a turn that
    /// waited names as the holder it waited behind.
    released: Mutex<Option<String>>,
}

/// One writer's turn of a [`FileJournal`], on its own span
/// (`rdm.node_admin.build.update.via-journal-turn`): `waited_ms`, how long it waited for the writer,
/// and `held_by`, the turn holding it when the wait began; then `held_ms`, how long it held the
/// writer, and `lane_ms`, how much of that was the lane store's own calls. A turn that waited on
/// another is told apart from one that waited on the store root.
struct Turn<'a> {
    guard: tokio::sync::MutexGuard<'a, Derived>,
    journal: &'a FileJournal,
    span: tracing::Span,
    began: std::time::Instant,
}

impl std::ops::Deref for Turn<'_> {
    type Target = Derived;
    fn deref(&self) -> &Derived {
        &self.guard
    }
}

/// A reader taking what it read from the lanes, holding the writer, named as its holder until it ends
/// ([`FileJournal::sync_for_read`]).
struct ReadHolder<'a> {
    journal: &'a FileJournal,
    op: &'static str,
    began: std::time::Instant,
}

impl<'a> ReadHolder<'a> {
    fn name(journal: &'a FileJournal, op: &'static str) -> Self {
        *journal.turn_of.lock().unwrap() = Some((op, std::time::Instant::now()));
        journal.hold_begins();
        Self { journal, op, began: std::time::Instant::now() }
    }
}

impl Drop for ReadHolder<'_> {
    fn drop(&mut self) {
        self.journal.hold_ends(self.op, self.began.elapsed());
        *self.journal.turn_of.lock().unwrap() = None;
    }
}

impl std::ops::DerefMut for Turn<'_> {
    fn deref_mut(&mut self) -> &mut Derived {
        &mut self.guard
    }
}

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        let lane_ns = self.journal.turn_lane_ns.swap(0, std::sync::atomic::Ordering::Relaxed);
        let op = (*self.journal.turn_of.lock().unwrap()).map(|(op, _)| op).unwrap_or_default();
        let (calls, queued, ran, facts) = self.journal.hold_ends(op, self.began.elapsed());
        *self.journal.turn_of.lock().unwrap() = None;
        self.span.record("held_ms", self.began.elapsed().as_millis() as u64).record("lane_ms", lane_ns / 1_000_000);
        self.span.record("io_calls", calls).record("io_queued_ms", queued).record("io_ms", ran).record("facts_read", facts);
    }
}

impl std::fmt::Debug for FileJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileJournal").field("root_dir", &self.root_dir).field("own_mesh", &self.own_mesh).finish()
    }
}

fn lane_io(e: crate::storage_error::StorageError) -> BuildStateError {
    BuildStateError::Io(e.to_string())
}

impl FileJournal {
    /// Open the Build lanes on the fabric's shared `root` for an admin of `own_mesh`, written by the
    /// process born as `writer` (R-BI6): `fabric/builds/` (the fabric-primary's) and every
    /// `meshes/<mesh>/builds/` (each mesh-primary's) are read when this admin holds the corresponding
    /// seat, and each fact is written to the root when it is committed, before the fabric's first
    /// state-commit too (Q61). A root holding the retired journal file is refused by name.
    pub async fn open_on_root(root: &crate::store_root::StoreRoot, own_mesh: &str, writer: &crate::model::IncarnationId) -> Result<Self, BuildStateError> {
        let dir = root.dir().to_path_buf();
        crate::blocking::blocking_io(move || refuse_journal_file(&dir)).await?;
        Ok(Self {
            lanes: crate::build_lanes::Lanes::new(root.clone(), writer.clone()),
            root_dir: root.dir().to_path_buf(),
            own_mesh: own_mesh.to_string(),
            holds_fabric_seat: std::sync::OnceLock::new(),
            holds_mesh_seat: std::sync::OnceLock::new(),
            revisions: std::sync::OnceLock::new(),
            seals: std::sync::OnceLock::new(),
            claim_keys: Mutex::new(std::collections::HashMap::new()),
            log: Mutex::new(FactLog::default()),
            writer: tokio::sync::Mutex::new(Derived::default()),
            turn_of: Mutex::new(None),
            turn_lane_ns: std::sync::atomic::AtomicU64::new(0),
            holder_io: Mutex::new(None),
            holder_facts: std::sync::atomic::AtomicU64::new(0),
            released: Mutex::new(None),
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
            lane_ms = tracing::field::Empty,
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
        self.turn_lane_ns.store(0, std::sync::atomic::Ordering::Relaxed);
        self.hold_begins();
        Turn { guard, journal: self, span, began: std::time::Instant::now() }
    }

    /// The holder of `writer` begins: its lane calls' blocking I/O and the facts it reads are its own.
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
        *self.released.lock().unwrap() = Some(format!("{op} held {} ms: {calls} lane calls, queued {queued} ms, ran {ran} ms, {facts} facts read", held.as_millis()));
        (calls, queued, ran, facts)
    }

    /// `call`, one of the lane store's own, its time counted to the holding turn ([`Turn`]).
    async fn lane<T>(&self, call: impl std::future::Future<Output = T>) -> T {
        let at = std::time::Instant::now();
        let io = self.holder_io.lock().unwrap().clone();
        let out = match io {
            Some(io) => crate::blocking::io_time_with(io, call).await,
            None => call.await,
        };
        self.turn_lane_ns.fetch_add(at.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
        out
    }

    /// Set how this journal reads whether this birth holds the fabric and mesh seats (set once at boot):
    /// `holds_fabric` routes fabric-lane executor receipts and reads `fabric/builds/` when `true`,
    /// and `holds_mesh` reads `meshes/<own_mesh>/builds/` when `true`.
    pub fn set_fabric_seat(&self, holds_fabric: Arc<dyn Fn() -> bool + Send + Sync>, holds_mesh: Arc<dyn Fn() -> bool + Send + Sync>) {
        let _ = self.holds_fabric_seat.set(holds_fabric);
        let _ = self.holds_mesh_seat.set(holds_mesh);
    }

    /// Set the seat revision this birth writes each lane under, and the seals it owes before it
    /// appends under a later revision (set once at boot). Unset: every lane is written under
    /// revision 0 and no seal is owed (a log no seat record governs).
    pub fn set_lane_seats(&self, revisions: LaneRevisions, seals: SealsOwed) {
        let _ = self.revisions.set(revisions);
        let _ = self.seals.set(seals);
    }

    /// The lanes this journal writes through: the seal of a moved seat's lane is written there.
    pub fn lanes(&self) -> &crate::build_lanes::Lanes {
        &self.lanes
    }

    fn is_fabric_primary(&self) -> bool {
        self.holds_fabric_seat.get().map(|f| f()).unwrap_or(true)
    }

    fn is_mesh_primary(&self) -> bool {
        self.holds_mesh_seat.get().map(|f| f()).unwrap_or(true)
    }

    /// The lane `fact` is written to: its lane's mesh, else the fabric lane, except an executor's
    /// receipt of a fabric-lane attempt, written to its own mesh's lane by an admin that does not
    /// hold the fabric seat.
    fn lane_of(&self, fact: &BuildFact) -> crate::build_lanes::LaneId {
        let own = || (!self.is_fabric_primary()).then_some(self.own_mesh.as_str());
        let mesh = match fact {
            BuildFact::Accepted(_) | BuildFact::Forget { .. } => None,
            BuildFact::Opened(o) => o.mesh.as_deref(),
            BuildFact::Claim(c) => c.mesh.as_deref(),
            BuildFact::Step(s) => s.mesh.as_deref().or_else(own),
            BuildFact::Attempt(a) => a.mesh.as_deref().or_else(own),
        };
        mesh.map(|m| crate::build_lanes::LaneId::Mesh(m.to_string())).unwrap_or(crate::build_lanes::LaneId::Fabric)
    }

    /// The revision this birth writes `fact` to `lane` under, or, for a decision of a lane whose
    /// seat record names another live birth, the stale refusal naming that record.
    fn revision_of(&self, lane: &crate::build_lanes::LaneId, fact: &BuildFact) -> Result<u64, BuildStateError> {
        let Some(of) = self.revisions.get() else { return Ok(0) };
        let seat = of(lane);
        match (fact, seat.holds) {
            (BuildFact::Step(_) | BuildFact::Attempt(_), _) | (_, Ok(())) => Ok(seat.revision),
            (_, Err(record)) => Err(stale(lane.name(), Some(seat.revision), record)),
        }
    }

    /// The lanes this admin reads: the fabric lane and every mesh's while it holds the fabric seat,
    /// its own mesh's while it holds that mesh's seat, none otherwise.
    async fn read_lanes(&self) -> Result<Vec<crate::build_lanes::LaneId>, BuildStateError> {
        if self.is_fabric_primary() {
            let meshes = self.root_dir.join("meshes");
            let mut lanes = vec![crate::build_lanes::LaneId::Fabric];
            let mut found = crate::blocking::blocking_io(move || -> Result<Vec<String>, BuildStateError> {
                match std::fs::read_dir(&meshes) {
                    Ok(entries) => Ok(entries.flatten().filter(|e| e.path().join("builds").is_dir()).map(|e| e.file_name().to_string_lossy().into_owned()).collect()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
                    Err(e) => Err(BuildStateError::Io(format!("{}: {e}", meshes.display()))),
                }
            })
            .await?;
            found.sort();
            lanes.extend(found.into_iter().map(crate::build_lanes::LaneId::Mesh));
            Ok(lanes)
        } else if self.is_mesh_primary() {
            Ok(vec![crate::build_lanes::LaneId::Mesh(self.own_mesh.clone())])
        } else {
            Ok(Vec::new())
        }
    }

    /// Take the facts `read` from the lanes into the log under the writer's turn `w`: each claim's
    /// key is noted, and the facts the log does not hold yet are published.
    fn take_read(&self, w: &Derived, read: Vec<(crate::build_lanes::LaneKey, BuildFact)>) {
        if read.is_empty() {
            return;
        }
        self.holder_facts.fetch_add(read.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let held = snapshot(&self.log);
        let mut new: Vec<BuildFact> = Vec::new();
        for (key, f) in read {
            self.note_key(&key, &f);
            if is_new(&held, &f) && is_new(&new, &f) {
                new.push(f);
            }
        }
        drop(held);
        publish(&self.log, w.0.as_ref(), new);
    }

    fn note_key(&self, key: &crate::build_lanes::LaneKey, f: &BuildFact) {
        if let BuildFact::Claim(c) = f {
            self.claim_keys.lock().unwrap().entry((c.build_id.clone(), c.mesh.clone(), c.attempt)).or_insert_with(|| key.clone());
        }
    }

    /// Read every lane this admin reads, from where it last read each, under the writer's turn `w`.
    async fn sync(&self, w: &Derived) -> Result<(), BuildStateError> {
        for lane in self.read_lanes().await? {
            let read = self.lane(self.lanes.read_new(&lane)).await.map_err(lane_io)?;
            self.take_read(w, read);
        }
        Ok(())
    }

    /// Sync from the store root for a read: the lanes are read from the disk holding no lock, so a
    /// writer's turn never waits behind a reader's disk; what was read is taken under the writer, with
    /// no disk, only when no turn holds it (a turn that holds it reads the lanes itself), so a reader
    /// never waits behind a writer either. While it takes, `op` (the reader) is the holder a waiting
    /// turn names.
    async fn sync_for_read(&self, op: &'static str) -> Result<(), BuildStateError> {
        let mut reads = Vec::new();
        for lane in self.read_lanes().await? {
            reads.push(self.lanes.peek_new(&lane).await.map_err(lane_io)?);
        }
        if let Ok(w) = self.writer.try_lock() {
            let _holder = ReadHolder::name(self, op);
            for read in reads {
                let read = self.lanes.take(read);
                self.take_read(&w, read);
            }
        }
        Ok(())
    }

    /// Read `lane` to its end and seal every earlier revision of it a committed seat move moved and no
    /// seal closes yet (the seal its mover owes, completed by this fold), so nothing of this birth's
    /// `revision` lands before them.
    async fn ready_lane(&self, w: &Derived, lane: &crate::build_lanes::LaneId, revision: u64) -> Result<(), BuildStateError> {
        let read = self.lane(self.lanes.read_new(lane)).await.map_err(lane_io)?;
        self.take_read(w, read);
        let Some(seals) = self.seals.get() else { return Ok(()) };
        for seal in self.lane(seals(lane.clone(), revision)).await.map_err(BuildStateError::Io)? {
            if self.lanes.seal_of(lane, seal.revision).is_none() {
                self.seal(w, lane, &seal, revision).await?;
            }
        }
        Ok(())
    }

    /// Write `seal` on `lane` (or meet the one standing), spanned, taking the facts it read past.
    async fn seal(&self, w: &Derived, lane: &crate::build_lanes::LaneId, seal: &crate::build_lanes::LaneSeal, writes_under: u64) -> Result<(), BuildStateError> {
        let sealed = self.lane(self.lanes.seal(lane, seal)).await.map_err(lane_io)?;
        tracing::info_span!("rdm.node_admin.build.update.via-lane-sealed", lane = %lane.name(), revision = seal.revision, at = %sealed.at, sealed_by = sealed.seal.sealed_by, by = %format!("{}:{}:{}", sealed.seal.by.mesh, sealed.seal.by.node_id, sealed.seal.by.incarnation.as_str()), passed = sealed.passed.len(), writes_under)
            .in_scope(|| tracing::info!("a revision a committed seat move moved is sealed: nothing of it commits past the seal"));
        self.take_read(w, sealed.passed);
        Ok(())
    }

    /// Insert `fact` at its lane's next key under this birth's revision, then publish it. Refused
    /// stale, named, when this birth no longer holds the lane's seat or the revision is sealed;
    /// `LaneTaken` when another writer's fact took the key first (that fact is read into the log, and
    /// the caller decides again).
    async fn append(&self, w: &Derived, fact: BuildFact) -> Result<crate::build_lanes::LaneKey, BuildStateError> {
        let lane = self.lane_of(&fact);
        let revision = self.revision_of(&lane, &fact)?;
        self.ready_lane(w, &lane, revision).await?;
        match self.lane(self.lanes.append(&lane, revision, &fact)).await.map_err(lane_io)? {
            crate::build_lanes::Appended::At(key) => {
                self.note_key(&key, &fact);
                if is_new(&snapshot(&self.log), &fact) {
                    publish(&self.log, w.0.as_ref(), vec![fact]);
                }
                Ok(key)
            }
            crate::build_lanes::Appended::Sealed { at, seal } => Err(stale(at.to_string(), Some(revision), format!("revision {revision} is sealed by seat move {} ({}:{}:{})", seal.sealed_by, seal.by.mesh, seal.by.node_id, seal.by.incarnation.as_str()))),
            crate::build_lanes::Appended::Taken(at) => {
                let read = self.lane(self.lanes.read_new(&lane)).await.map_err(lane_io)?;
                self.take_read(w, read);
                Err(BuildStateError::LaneTaken { at: at.to_string() })
            }
        }
    }

    /// Append a fact no one decides (a receipt, a forget) under the writer's turn `w`: written once,
    /// whoever's key it lands at; nothing when the log holds it already.
    async fn append_once(&self, w: &Derived, fact: BuildFact) -> Result<(), BuildStateError> {
        loop {
            self.sync(w).await?;
            if !is_new(&snapshot(&self.log), &fact) {
                return Ok(());
            }
            match self.append(w, fact.clone()).await {
                Err(BuildStateError::LaneTaken { .. }) => continue,
                r => return r.map(|_| ()),
            }
        }
    }

    /// Whether `held`, the in-flight row standing for another attempt, still holds its mesh (Q35):
    /// its key holds that attempt's claim, or holds nothing yet (its claimant may still write it: a
    /// key past a seal is never left empty, since the seal takes the first free key). Void when the
    /// key holds any other entry, the seal included.
    async fn row_live(&self, held: &crate::mesh_claims::InFlight) -> Result<bool, BuildStateError> {
        let Some(key) = held.lane_key.as_deref() else { return Ok(false) };
        let key: crate::build_lanes::LaneKey = key.parse().map_err(BuildStateError::Io)?;
        Ok(match self.lane(self.lanes.entry(&key)).await.map_err(lane_io)? {
            None => true,
            Some(crate::build_lanes::Entry::Fact(BuildFact::Claim(c))) => c.build_id == held.build_id && c.mesh == held.lane && c.attempt == held.attempt,
            Some(_) => false,
        })
    }

    /// Take `mesh`'s in-flight row for `row` (naming its claim's lane key): a row of another attempt
    /// that is void is released and the take made again; a row of this attempt naming another key is
    /// moved to `row`'s.
    async fn take_in_flight(&self, claims: &crate::mesh_claims::MeshClaims, mesh: &str, row: &crate::mesh_claims::InFlight) -> Result<crate::mesh_claims::Taken, BuildStateError> {
        let io = |e: crate::storage_error::StorageError| BuildStateError::Io(format!("mesh {mesh}'s in-flight row: {e}"));
        match claims.take(mesh, row).await.map_err(io)? {
            crate::mesh_claims::Taken::Won => {
                claims.repoint(mesh, row).await.map_err(io)?;
                Ok(crate::mesh_claims::Taken::Won)
            }
            crate::mesh_claims::Taken::Held(held) => {
                if self.row_live(&held).await? {
                    return Ok(crate::mesh_claims::Taken::Held(held));
                }
                tracing::info_span!("rdm.node_admin.build.update.via-in-flight-void", mesh, build_id = %held.build_id.log(), lane = held.lane.as_deref().unwrap_or("fabric"), attempt = held.attempt, claimant = %held.claimant, lane_key = held.lane_key.as_deref().unwrap_or(""), taker_build_id = %row.build_id.log(), taker_attempt = row.attempt)
                    .in_scope(|| tracing::info!("the mesh's in-flight row names a lane key that holds another entry: its claim never committed below the seal, and the row is released"));
                claims.release(mesh, &held).await.map_err(io)?;
                match claims.take(mesh, row).await.map_err(io)? {
                    crate::mesh_claims::Taken::Won => {
                        claims.repoint(mesh, row).await.map_err(io)?;
                        Ok(crate::mesh_claims::Taken::Won)
                    }
                    held => Ok(held),
                }
            }
        }
    }
}

/// A store root an older build left a Build journal file on (`fabric/builds/` or any
/// `meshes/<mesh>/builds/`) is refused by name, the file named.
fn refuse_journal_file(root: &Path) -> Result<(), BuildStateError> {
    let refuse = |file: PathBuf| BuildStateError::Io(format!("{}: the Build journal format {REFUSED_JOURNAL_FILE} is retired: this build reads the Build lanes, one key per fact under rev-<R>/<seq> (build-fact/1, lane-seal/1)", file.display()));
    let fabric = root.join("fabric").join("builds").join(REFUSED_JOURNAL_FILE);
    if fabric.exists() {
        return Err(refuse(fabric));
    }
    let meshes = match std::fs::read_dir(root.join("meshes")) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(BuildStateError::Io(format!("{}: {e}", root.join("meshes").display()))),
    };
    let mut files: Vec<PathBuf> = meshes.flatten().map(|e| e.path().join("builds").join(REFUSED_JOURNAL_FILE)).filter(|f| f.exists()).collect();
    files.sort();
    match files.into_iter().next() {
        Some(f) => Err(refuse(f)),
        None => Ok(()),
    }
}

/// The stale refusal of a lane write (Q28: "`RejectedStaleSeat` from the store, spanned").
fn stale(at: String, revision: Option<u64>, detail: String) -> BuildStateError {
    tracing::info_span!("rdm.node_admin.build.reject.via-stale-seat", at = %at, revision = revision.map(|r| r.to_string()).unwrap_or_default(), detail = %detail)
        .in_scope(|| tracing::info!("a Build fact of an authority that no longer holds its lane's seat is refused, and nothing is written"));
    BuildStateError::StaleSeat { at, revision, detail }
}

#[async_trait]
impl BuildStateAdapter for FileJournal {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        let w = self.turn("publish_accepted").await;
        loop {
            self.sync(&w).await?;
            if accepted_conflict(&snapshot(&self.log), accepted)? {
                return Ok(());
            }
            match self.append(&w, BuildFact::Accepted(accepted.clone())).await {
                Err(BuildStateError::LaneTaken { .. }) => continue,
                r => return r.map(|_| ()),
            }
        }
    }

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        let w = self.turn("open_attempt").await;
        loop {
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
            match self.append(&w, BuildFact::Opened(opened.clone())).await {
                Err(BuildStateError::LaneTaken { .. }) => continue,
                r => return r.map(|_| ()),
            }
        }
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

    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.sync_for_read("list_active").await?;
        Ok(active(&snapshot(&self.log)))
    }

    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        let w = self.turn("claim_attempt").await;
        loop {
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
            match self.append(&w, BuildFact::Claim(claim.clone())).await {
                Err(BuildStateError::LaneTaken { .. }) => continue,
                r => return r.map(|_| ClaimOutcome::Won),
            }
        }
    }

    async fn claim_in_flight(&self, claim: &BuildAttemptClaim, claims: &crate::mesh_claims::MeshClaims, mesh: &str, row: &crate::mesh_claims::InFlight) -> Result<InFlightClaim, BuildStateError> {
        let w = self.turn("claim_in_flight").await;
        loop {
            self.sync(&w).await?;
            if !known(&snapshot(&self.log), &claim.build_id) {
                return Err(BuildStateError::UnknownBuild(claim.build_id.clone()));
            }
            let fact = BuildFact::Claim(claim.clone());
            let lane = self.lane_of(&fact);
            let revision = self.revision_of(&lane, &fact)?;
            self.ready_lane(&w, &lane, revision).await?;
            let held = snapshot(&self.log);
            let holder = claim_holder(&held, &claim.build_id, claim.mesh.as_deref(), claim.attempt);
            // The claim's lane key: where it stands, or where it is written next.
            let standing = holder.as_ref().and_then(|_| self.claim_keys.lock().unwrap().get(&(claim.build_id.clone(), claim.mesh.clone(), claim.attempt)).cloned());
            let key = match standing {
                Some(k) => k,
                None => self.lane(self.lanes.next_key(&lane, revision)).await.map_err(lane_io)?,
            };
            let named = crate::mesh_claims::InFlight { lane_key: Some(key.to_string()), ..row.clone() };
            if let crate::mesh_claims::Taken::Held(other) = self.take_in_flight(claims, mesh, &named).await? {
                return Ok(InFlightClaim::Held(other));
            }
            if let Some(holder) = holder {
                return Ok(InFlightClaim::Decided(if holder == claim.executor { ClaimOutcome::Won } else { ClaimOutcome::Lost { holder } }));
            }
            if let Some(not_open) = not_open(&held, claim) {
                return Ok(InFlightClaim::Decided(not_open));
            }
            drop(held);
            match self.append(&w, BuildFact::Claim(claim.clone())).await {
                Ok(at) if at == key => return Ok(InFlightClaim::Decided(ClaimOutcome::Won)),
                // The claim stands at another key than the row names: the next turn points the row
                // at it.
                Ok(_) => continue,
                // Another writer's fact took the key: the row names a key that holds another entry,
                // so it goes, and the claim is decided again against that fact.
                Err(BuildStateError::LaneTaken { .. }) => {
                    claims.release(mesh, &named).await.map_err(|e| BuildStateError::Io(format!("mesh {mesh}'s in-flight row: {e}")))?;
                    continue;
                }
                Err(e) => {
                    let _ = claims.release(mesh, &named).await;
                    return Err(e);
                }
            }
        }
    }

    async fn in_flight_live(&self, held: &crate::mesh_claims::InFlight) -> Result<bool, BuildStateError> {
        self.row_live(held).await
    }

    async fn seal_moved(&self, moved: &crate::fabric_storage::SeatMove, by: &rdm_core::handover::HandoverBirth) -> Result<(), BuildStateError> {
        let w = self.turn("seal_moved").await;
        for s in &moved.row.seats {
            let lane = match &s.seat {
                rdm_core::seat::transfer::Seat::FabricPrimary => crate::build_lanes::LaneId::Fabric,
                rdm_core::seat::transfer::Seat::MeshPrimary { mesh } => crate::build_lanes::LaneId::Mesh(mesh.clone()),
                rdm_core::seat::transfer::Seat::Cohort { .. } => continue,
            };
            let read = self.lane(self.lanes.read_new(&lane)).await.map_err(lane_io)?;
            self.take_read(&w, read);
            self.seal(&w, &lane, &crate::build_lanes::LaneSeal { revision: s.from_revision, sealed_by: moved.t, by: by.clone() }, s.revision).await?;
        }
        Ok(())
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
        self.append_once(&w, BuildFact::Step(receipt.clone())).await
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let w = self.turn("append_attempt_receipt").await;
        self.append_once(&w, BuildFact::Attempt(receipt.clone())).await
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.sync_for_read("facts").await?;
        Ok(snapshot(&self.log).as_ref().clone())
    }

    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.sync_for_read("recovery_of").await?;
        Ok(self.log.lock().unwrap().recovery_of(path, node_id, incarnation))
    }

    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.sync_for_read("mesh_deleted_of").await?;
        Ok(self.log.lock().unwrap().mesh_deleted_of(node_id, incarnation))
    }

    async fn terminate_failures_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Vec<FailedTerminate>, BuildStateError> {
        self.sync_for_read("terminate_failures_of").await?;
        Ok(self.log.lock().unwrap().terminate_failures_of(node_id, incarnation))
    }

    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError> {
        self.sync_for_read("exits_owed").await?;
        Ok(self.log.lock().unwrap().exits_owed(build_id, mesh))
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let w = self.turn("forget").await;
        self.append_once(&w, BuildFact::Forget { build_id: build_id.clone() }).await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_step_line_written_before_receipts_carried_output_still_decodes() {
        let old = format!(r#"{{"fact":"step","build_id":"{}","attempt":1,"operation":"node.create:mesh1.rpc.1","step":"DeployRuntime","outcome":"complete"}}"#, rdm_core::fixture_crockford60("b1"));
        let fact: BuildFact = serde_json::from_str(&old).unwrap();
        assert!(matches!(fact, BuildFact::Step(BuildStepReceipt { output: None, .. })));
        let with = BuildStepReceipt { output: Some(serde_json::json!({"pid": 7})), ..step("b1", 1, "node.create:mesh1.rpc.1") };
        let back: BuildStepReceipt = serde_json::from_str(&serde_json::to_string(&with).unwrap()).unwrap();
        assert_eq!(back, with);
        // i144 R21: a Failed line written before the stage existed replays with none.
        let old_failed = format!(r#"{{"fact":"step","build_id":"{}","attempt":1,"operation":"node.delete:mesh1.rpc.1","step":"TerminateRuntime","outcome":{{"failed":{{"reason":"x"}}}}}}"#, rdm_core::fixture_crockford60("b1"));
        let fact: BuildFact = serde_json::from_str(&old_failed).unwrap();
        assert!(matches!(fact, BuildFact::Step(BuildStepReceipt { outcome: StepOutcome::Failed { stage: None, .. }, .. })), "{fact:?}");
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
        AttemptOpened { mesh: None, build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, reason, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 5 }
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

    fn log() -> Vec<BuildFact> {
        vec![
            BuildFact::Accepted(intent("b1")),
            BuildFact::Claim(claim("b1", 1, "mesh1.admin.1")),
            BuildFact::Step(step("b1", 1, "node.restart:mesh1.rpc.2")),
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
    fn open_retire_and_restart_overlays_are_derived_from_the_build_facts() {
        let op = |node: &str| rdm_core::LifecycleOp {
            build_id: rdm_core::fixture_crockford60("b1"),
            attempt: 1,
            operation: format!("node.delete:{node}"),
            node_id: rdm_core::NodeId::mint(),
            incarnation: rdm_core::IncarnationId::mint(),
            name: node.parse().unwrap(),
            event_at_rafka_ms: 1,
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
        assert_eq!(open, vec![b.clone()], "a retire with its pre-notice and no departure is open; a completed one is not");

        // A restart is one operation (`node.restart:<path>`): its pre-event opens it, the stop of the
        // live process leaves it open, and only its `Complete`, after the start, closes it, whoever
        // reads the facts.
        let c = rdm_core::LifecycleOp { operation: "node.restart:mesh1.rpc.3".into(), name: "mesh1.rpc.3".parse().unwrap(), ..op("mesh1.rpc.3") };
        let mut facts = facts;
        facts.push(with("node.restart:mesh1.rpc.3", "NodeRestarting", &c));
        facts.push(with("node.restart:mesh1.rpc.3", "AwaitNodeLeft", &c));
        assert_eq!(in_flight_ops(&facts, &fold(&facts)).ops, vec![b.clone(), c.clone()], "a successor holds the restart from the Build facts alone");
        facts.push(with("node.restart:mesh1.rpc.3", "Complete", &c));
        assert_eq!(in_flight_ops(&facts, &fold(&facts)).ops, vec![b], "the restart's Complete closes it");
        assert!(in_flight_ops(&facts, &fold(&facts)).unrecognised.is_empty());
        assert_eq!(
            in_flight_ops(&facts, &fold(&facts)).restarts_completed,
            [(rdm_core::fixture_crockford60("b1"), 1, "node.restart:mesh1.rpc.3".to_string())].into(),
            "the restart's Complete is named by Build, attempt and operation: it ends that restart's hold and no other"
        );

        // A Build is forgotten once the fold drops it on its `Forget`; a `Forget` of a Build still in
        // flight leaves it held, so it names nothing forgotten.
        facts.push(BuildFact::Forget { build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap() });
        facts.push(BuildFact::Forget { build_id: BuildId::parse(&rdm_core::fixture_crockford60("b9")).unwrap() });
        assert_eq!(forgotten_builds(&facts, &fold(&facts)), [rdm_core::fixture_crockford60("b9")].into(), "only a Build the fold no longer holds is forgotten");
    }

    /// CONTRACT (node-rpc-envelope.md: "Progress waits on no gossip"): a restart is open until its
    /// `node.restart:<path>` `Complete` is receipted or its attempt's verdict is `Converged` (the
    /// fabric-primary's own fact from the stream terminal); an attempt's first verdict is its one, so
    /// a `Converged` after a `Failed` ends nothing, and a verdict ends only its own attempt's restarts.
    #[test]
    fn a_restart_is_closed_by_its_attempts_converged_verdict_and_by_no_other_attempts() {
        let restart = |node: &str, attempt: u32| rdm_core::LifecycleOp {
            build_id: rdm_core::fixture_crockford60("b1"),
            attempt,
            operation: format!("node.restart:{node}"),
            node_id: rdm_core::NodeId::mint(),
            incarnation: rdm_core::IncarnationId::mint(),
            name: node.parse().unwrap(),
            event_at_rafka_ms: 1,
        };
        let pre_event = |op: &rdm_core::LifecycleOp| {
            BuildFact::Step(BuildStepReceipt {
                mesh: None,
                build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
                attempt: op.attempt,
                operation: op.operation.clone(),
                step: "NodeRestarting".into(),
                outcome: StepOutcome::Complete,
                output: Some(serde_json::to_value(op).unwrap()),
                executor: None,
            })
        };
        let (failed, converged, later) = (restart("mesh1.rpc.1", 2), restart("mesh1.rpc.2", 3), restart("mesh1.rpc.3", 4));
        let mut facts = vec![BuildFact::Accepted(intent("b1")), pre_event(&failed), pre_event(&converged), pre_event(&later)];
        assert_eq!(in_flight_ops(&facts, &fold(&facts)).ops, vec![failed.clone(), converged.clone(), later.clone()]);
        facts.push(BuildFact::Attempt(attempt("b1", 2, AttemptOutcome::Failed { reason: "lost".into() , code: rdm_core::outcome::Code::FailedPrecondition })));
        facts.push(BuildFact::Attempt(attempt("b1", 2, AttemptOutcome::Converged)));
        facts.push(BuildFact::Attempt(attempt("b1", 3, AttemptOutcome::Converged)));
        let read = in_flight_ops(&facts, &fold(&facts));
        assert_eq!(read.ops, vec![failed, later], "attempt 3's Converged ends its restart; attempt 2's first verdict is Failed and attempt 4 has none");
        assert_eq!(read.attempts_converged, [(rdm_core::fixture_crockford60("b1"), 3)].into(), "the converged attempts are named by Build and attempt");
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
        let mut again = opened("b1", 2, AttemptReason::Restart);
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
        assert_eq!(m.open_attempt(&opened("b9", 1, AttemptReason::Restart)).await, Err(BuildStateError::UnknownBuild(BuildId::parse(&rdm_core::fixture_crockford60("b9")).unwrap())));
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
        let act = |path: &str, from: &crate::model::IncarnationId| Some(AttemptAction::Restart { path: path.parse().unwrap(), from_incarnation: from.clone() });
        let mut first = opened("b1", 2, AttemptReason::ProvenDrift);
        first.action = act("mesh1.rpc.2", &inc);
        m.open_attempt(&first).await.unwrap();
        let mut same = opened("b1", 2, AttemptReason::Restart);
        same.action = act("mesh1.rpc.2", &inc);
        same.opened_by = "mesh2.admin.1".into();
        m.open_attempt(&same).await.expect("the same action asked again is the same attempt");
        let mut other_node = opened("b1", 2, AttemptReason::Restart);
        other_node.action = Some(AttemptAction::Restart { path: "mesh2.rpc.1".parse().unwrap(), from_incarnation: other.clone() });
        let err = m.open_attempt(&other_node).await.expect_err("another action asked for the held number");
        match &err {
            BuildStateError::AttemptTaken { build_id, attempt, held, wanted } => {
                assert_eq!((build_id.to_string(), *attempt), (rdm_core::fixture_crockford60("b1"), 2));
                assert!(held.contains("restart of mesh1.rpc.2") && held.contains("proven-drift"), "the refusal names what holds the number: {held}");
                assert!(wanted.contains("restart of mesh2.rpc.1"), "the refusal names what was asked: {wanted}");
            }
            other => panic!("not AttemptTaken: {other:?}"),
        }
        let mut other_birth = opened("b1", 2, AttemptReason::Restart);
        other_birth.action = act("mesh1.rpc.2", &other);
        assert!(matches!(m.open_attempt(&other_birth).await, Err(BuildStateError::AttemptTaken { .. })), "the same path fenced to another birth is another action");
        assert_eq!(m.facts().await.unwrap().iter().filter(|f| matches!(f, BuildFact::Opened(_))).count(), 1, "the refused opens appended nothing");
    }

    #[tokio::test]
    async fn absorbing_projection_facts_is_idempotent() {
        let a = MemoryBuildStateAdapter::new();
        a.absorb(&log()).await;
        a.absorb(&log()).await;
        assert_eq!(fold(&a.facts().await.unwrap()), fold(&log()));
    }

    /// A Build log on a store root at `dir` (no scratchpad: written straight to the disk), its seats
    /// governed by no record (every lane written under revision 0).
    async fn open_at(dir: &Path) -> Result<FileJournal, BuildStateError> {
        FileJournal::open_on_root(&crate::store_root::StoreRoot::at(dir), "mesh1", &writer()).await
    }

    fn writer() -> crate::model::IncarnationId {
        crate::model::IncarnationId::mint()
    }

    /// Every lane key on the store root at `root`, in order: `<lane>/rev-<R>/<seq>`.
    fn lane_files(root: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|x| x == "json") && p.components().any(|c| c.as_os_str().to_string_lossy().starts_with("rev-")) {
                    out.push(p.strip_prefix(root).unwrap().with_extension("").display().to_string());
                }
            }
        }
        out.sort();
        out
    }

    fn tmp() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!("journal-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// CONTRACT (the Build log is read on async workers: the fabric-primary's climb reads its Build
    /// through `AcceptedStore::current` -> `read_build` (accepted.rs `current`, fabric_rounds.rs
    /// `step_*`); `LocalBuildLog::derive_on_every_fact`: what is derived from the facts never lags a
    /// reader of them): while a push into the journal is held inside the installed derivation, a
    /// reader of the log on another thread (`read_build`, `list_active`) answers at once, from the log
    /// as it stood before the held fact: it never sees that fact before its derivation ran. Once the
    /// derivation ends, the push returns and the reader sees the fact. Must NOT happen: a reader
    /// waiting on a std lock behind a writer's whole-log derivation (a worker thread taken from the
    /// runtime for as long as the fold and its book locks take).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reader_of_the_journal_never_waits_behind_a_push_held_mid_derivation() {
        let dir = tmp();
        let j = Arc::new(open_at(&dir).await.unwrap());
        let b1 = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        j.publish_accepted(&intent("b1")).await.unwrap();
        j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
        let held_op = "node.restart:mesh1.rpc.9";
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (entered_tx, release_rx) = (Mutex::new(entered_tx), Mutex::new(release_rx));
        j.derive_on_every_fact(Arc::new(move |facts: &[BuildFact]| {
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
            j.append_step_receipt(&step("b1", 1, "node.restart:mesh1.rpc.2")).await.unwrap();
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
            // A restart of b0 is an attempt of it: in progress while it runs, and b0 stays the known
            // state when it fails.
            s.open_attempt(&opened("b0", 2, AttemptReason::Restart)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b0")));
            s.claim_attempt(&claim("b0", 2, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt("b0", 2, AttemptOutcome::Failed { reason: "restart failed".into() , code: rdm_core::outcome::Code::FailedPrecondition })).await.unwrap();
            assert_eq!(s.read_build(&id("b0")).await.unwrap().state, BuildState::Failed);
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b0")), "an attempt of an unchanged topology that failed never makes an older Build current");
            // The failed b1 resubmitted: its next attempt makes it the Build in progress.
            s.open_attempt(&opened("b1", 2, AttemptReason::AuthorityMoved)).await.unwrap();
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
    /// alone"; proposal Part C1: "A later attempt that reads the committed row has discovered the
    /// commit"): attempt 1 of b1 hands off and attempt 2 continues its run (run 1); attempt 2 fails,
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
        s.open_attempt(&opened("b1", 2, AttemptReason::Restart)).await.unwrap();
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
                serde_json::to_value(rdm_core::LifecycleOp { build_id: build.into(), attempt, operation: format!("{operation}:{path}"), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1 }).unwrap()
            }),
            executor: Some("mesh1.admin.1".into()),
        };
        let opened = |build: &str, attempt: u32| AttemptOpened {
            mesh: None,
            build_id: BuildId::parse(&rdm_core::fixture_crockford60(build)).unwrap(),
            attempt,
            reason: AttemptReason::Recovery,
            action: Some(crate::accepted::AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone() }),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 1,
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
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (i144 R21; node-recovery.md step 4's end: "no hold submits that birth's terminate
    /// again: a person decides", read from the birth's durable Failed receipt): `terminate_failures_of`
    /// answers every recovery attempt of the birth, in any Build and lane, whose `TerminateRuntime`
    /// receipt is Failed with a stage, oldest first, with the attempt's opening time. It is folded
    /// wherever a fact enters the log (own appends, a restarted admin's journal replay, facts absorbed
    /// from the Build topic in any order), it holds after a later Build is accepted, and forgetting the
    /// Build leaves it. Must NOT happen: a refused terminate lost to a successor, to a later Build or
    /// to a forget, so a hold submits the birth's terminate again; another birth's or a stageless
    /// failure answered.
    #[tokio::test]
    async fn a_birth_s_failed_terminates_are_folded_on_append_replay_and_absorb_across_builds_and_forgets() {
        let (path, node_id, incarnation): (crate::model::PathName, _, _) = ("mesh2.rpc.2".parse().unwrap(), crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let bid = |b: &str| BuildId::parse(&rdm_core::fixture_crockford60(b)).unwrap();
        let opened = |build: &str, attempt: u32, at: u64| AttemptOpened {
            mesh: Some("mesh2".into()),
            build_id: bid(build),
            attempt,
            reason: AttemptReason::Recovery,
            action: Some(crate::accepted::AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone() }),
            opened_by: "mesh2.admin.1".into(),
            opened_at_ms: at,
        };
        let terminate = |build: &str, attempt: u32, stage: Option<TerminateStage>| BuildStepReceipt {
            mesh: Some("mesh2".into()),
            build_id: bid(build),
            attempt,
            operation: format!("node.delete:{path}"),
            step: "TerminateRuntime".into(),
            outcome: StepOutcome::Failed { reason: "the cell's terminate".into(), stage , code: rdm_core::outcome::Code::FailedPrecondition },
            output: None,
            executor: Some("mesh2.admin.1".into()),
        };
        let refused = TerminateStage::Provider { error: "the daemon refused".into() };
        let voided = TerminateStage::Inspection { verdict: InspectionVerdict::OsStopped };
        let want = vec![
            FailedTerminate { build_id: bid("b1"), mesh: Some("mesh2".into()), attempt: 1, opened_at_ms: Some(10), stage: voided.clone() },
            FailedTerminate { build_id: bid("b1"), mesh: Some("mesh2".into()), attempt: 2, opened_at_ms: Some(20), stage: refused.clone() },
        ];
        let dir = tmp();
        {
            let j = open_at(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.open_attempt(&opened("b1", 1, 10)).await.unwrap();
            j.append_step_receipt(&terminate("b1", 1, Some(voided.clone()))).await.unwrap();
            j.open_attempt(&opened("b1", 2, 20)).await.unwrap();
            j.append_step_receipt(&terminate("b1", 2, Some(refused.clone()))).await.unwrap();
            // A stageless failure (the runtime exited, its addresses still held) is no failed terminate.
            j.open_attempt(&opened("b1", 3, 30)).await.unwrap();
            j.append_step_receipt(&terminate("b1", 3, None)).await.unwrap();
            assert_eq!(j.terminate_failures_of(&node_id, &incarnation).await.unwrap(), want, "own append");
            // A later Build is accepted and becomes current: the earlier Build's receipts still answer.
            j.publish_accepted(&BuildAccepted { submitted_at_ms: 2, ..intent("b2") }).await.unwrap();
            assert_eq!(j.terminate_failures_of(&node_id, &incarnation).await.unwrap(), want, "after a later Build");
            j.forget(&bid("b1")).await.unwrap();
            assert_eq!(j.terminate_failures_of(&node_id, &incarnation).await.unwrap(), want, "a forget leaves it");
            assert_eq!(j.terminate_failures_of(&crate::model::NodeId::mint(), &incarnation).await.unwrap(), vec![], "another birth");
        }
        let j = open_at(&dir).await.unwrap(); // the admin restarted
        assert_eq!(j.terminate_failures_of(&node_id, &incarnation).await.unwrap(), want, "journal replay");
        let m = MemoryBuildStateAdapter::new();
        // The Build topic delivers in any order: the refused receipt heard before its attempt's opening.
        m.absorb(&[BuildFact::Accepted(intent("b1")), BuildFact::Step(terminate("b1", 2, Some(refused.clone())))]).await;
        assert_eq!(m.terminate_failures_of(&node_id, &incarnation).await.unwrap(), vec![], "a receipt alone names no birth");
        m.absorb(&[BuildFact::Opened(opened("b1", 2, 20)), BuildFact::Step(terminate("b1", 1, Some(voided.clone()))), BuildFact::Opened(opened("b1", 1, 10))]).await;
        assert_eq!(m.terminate_failures_of(&node_id, &incarnation).await.unwrap(), want, "absorbed, in any order");
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
                serde_json::to_value(rdm_core::LifecycleOp { build_id: rdm_core::fixture_crockford60("b1"), attempt, operation: format!("{operation}:{}", b.0), node_id: b.1.clone(), incarnation: b.2.clone(), name: b.0.clone(), event_at_rafka_ms: 1 }).unwrap()
            }),
            executor: Some("mesh2.admin.1".into()),
        };
        let open = |b: &(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId), attempt: u32| AttemptOpened {
            mesh: None,
            build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt,
            reason: AttemptReason::Recovery,
            action: Some(crate::accepted::AttemptAction::Recover { path: b.0.clone(), node_id: b.1.clone(), from_incarnation: b.2.clone() }),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 1,
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
                serde_json::to_value(rdm_core::LifecycleOp { build_id: rdm_core::fixture_crockford60("b1"), attempt, operation: format!("{kind}:{path}"), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1 }).unwrap()
            }),
            executor: Some("mesh2.admin.1".into()),
        };
        let replaced = (crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let retired = (crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let dir = tmp();
        {
            let j = open_at(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.append_step_receipt(&receipt("node.delete", 2, "NodeDeleted", &replaced.0, &replaced.1)).await.unwrap();
            assert_eq!(j.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), None, "deleted, the retire not complete: its next attempt finishes it");
            j.append_step_receipt(&receipt("node.delete", 2, "Complete", &replaced.0, &replaced.1)).await.unwrap();
            j.append_step_receipt(&receipt("node.delete", 3, "NodeDeleted", &retired.0, &retired.1)).await.unwrap();
            j.append_step_receipt(&receipt("node.delete", 3, "Complete", &retired.0, &retired.1)).await.unwrap();
            assert_eq!(j.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 2, format!("node.delete:{path}"))), "own append: the first retire that deleted its birth");
            assert_eq!(j.recovery_of(&path, &retired.0, &retired.1).await.unwrap(), Some((BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), None, 3, format!("node.delete:{path}"))), "own append: the retire that deleted the birth");
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

    /// CONTRACT: opening the journal (its replay of every fact line in the file) never waits on
    /// the disk on a runtime worker: it runs on the blocking pool, as every append does. The admin
    /// opens it inside its boot, on the runtime. The cell opens a journal whose file holds the
    /// read: the runtime's only thread keeps running, and the replay answers the logged Build.
    #[test]
    fn opening_the_journal_waits_on_the_disk_off_the_runtime_thread() {
        let dir = crate::record_store::tempdir("journal-offload");
        let line = serde_json::to_vec(&serde_json::json!({"format": crate::build_lanes::FACT_FORMAT, "record": BuildFact::Accepted(intent("b1"))})).unwrap();
        let rev = dir.join("fabric").join("builds").join("rev-00000000000000000000");
        std::fs::create_dir_all(&rev).unwrap();
        let fifo = rev.join("00000000000000000001.json");
        let (ran, active) = crate::record_store::runtime_runs_while_a_fifo_is_read(fifo, line, Box::pin(async move { open_at(&dir).await.unwrap().list_active().await.unwrap().len() }));
        assert!(ran, "the runtime thread ran while the journal's replay waited on the disk");
        assert_eq!(active, 1);
    }

    /// A lane entry that does not decode is refused by name, the key named, never read as no entry;
    /// a store root holding the retired journal file is refused by name at the open.
    #[tokio::test]
    async fn a_corrupt_lane_entry_and_a_build_journal_file_are_refused_by_name() {
        let dir = tmp();
        let rev = dir.join("fabric").join("builds").join("rev-00000000000000000000");
        std::fs::create_dir_all(&rev).unwrap();
        std::fs::write(rev.join("00000000000000000001.json"), serde_json::to_vec(&serde_json::json!({"format": crate::build_lanes::FACT_FORMAT, "record": BuildFact::Accepted(intent("b1"))})).unwrap()).unwrap();
        std::fs::write(rev.join("00000000000000000002.json"), b"not json").unwrap();
        let err = open_at(&dir).await.unwrap().list_active().await.unwrap_err();
        assert!(matches!(&err, BuildStateError::Io(e) if e.contains("rev-00000000000000000000/00000000000000000002.json")), "{err}");
        let retired = tmp();
        std::fs::create_dir_all(retired.join("meshes").join("mesh2").join("builds")).unwrap();
        std::fs::write(retired.join("meshes").join("mesh2").join("builds").join("build-journal.jsonl"), b"").unwrap();
        let err = open_at(&retired).await.unwrap_err();
        assert!(matches!(&err, BuildStateError::Io(e) if e.contains("meshes/mesh2/builds/build-journal.jsonl") && e.contains("retired")), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&retired);
    }

    /// PRD §1.4 / §7: no test, and no code outside this module, opens a Build
    /// journal; the one exception is store_root_boot's preparation of a store root before
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
                } else if p.extension().is_some_and(|x| x == "rs") && !p.ends_with("build_state.rs") && !p.ends_with("rdm-admin/src/admin.rs") && !p.ends_with("rdm-admin/src/admin/boot.rs") && !p.ends_with("rdm-admin/tests/store_root_boot.rs") {
                    let text = std::fs::read_to_string(&p).unwrap_or_default();
                    if text.contains("FileJournal::open") || text.contains("build-journal.jsonl") {
                        offenders.push(p);
                    }
                }
            }
        }
        assert!(offenders.is_empty(), "only build_state.rs, the admin's own boot and store_root_boot's prepared store root may open a journal: {offenders:?}");
    }

    /// An attempt of `id` opened on `mesh`'s drift lane (R-BI7): the recovery of `path`'s exited birth.
    fn lane_open(id: &str, mesh: &str, attempt: u32, path: &str) -> AttemptOpened {
        AttemptOpened {
            mesh: Some(mesh.into()),
            build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(),
            attempt,
            reason: AttemptReason::Recovery,
            action: Some(crate::accepted::AttemptAction::Recover { path: path.parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() }),
            opened_by: format!("{mesh}.admin.1"),
            opened_at_ms: 6,
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
    async fn the_fold_reads_the_fabric_lane_and_each_drift_lane_apart() {
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
    fn a_fact_line_written_before_lanes_decodes_as_the_fabric_lane() {
        let id = rdm_core::fixture_crockford60("b1");
        let old: BuildFact = serde_json::from_str(&format!(r#"{{"fact":"claim","build_id":"{id}","attempt":2,"executor":"mesh1.admin.1"}}"#)).unwrap();
        assert!(matches!(&old, BuildFact::Claim(c) if c.mesh.is_none() && c.claimant.is_none()), "{old:?}");
        let new = BuildFact::Claim(lane_claim("b1", "mesh2", 1, "mesh2.admin.1"));
        assert_eq!(serde_json::from_str::<BuildFact>(&serde_json::to_string(&new).unwrap()).unwrap(), new);
        let fabric = serde_json::to_value(BuildFact::Claim(claim("b1", 2, "mesh1.admin.1"))).unwrap();
        assert!(fabric.get("mesh").is_none() && fabric.get("claimant").is_none(), "a fabric-lane line is written as before: {fabric}");
    }

    /// CONTRACT (i146 Q61 A; build.md §5.2: "Restarting every node-admin at once must not erase the facts
    /// needed to reconstruct the Fabric and continue an active Build", §5.3: "a successor reads them
    /// there"): a fabric-primary's committed Build facts (the accepted Build, its attempt claim, a step
    /// receipt, the attempt's completion) stand on the shared root before the fabric's first
    /// state-commit, so the same fabric-primary restarted (a new process of its birth on the root, its
    /// scratchpad empty) and a successor opened on the root each read every one of them, and the
    /// Build complete. Must NOT happen: a committed fact lost with the process that held it, or read
    /// only after a state-commit.
    #[tokio::test]
    async fn every_build_fact_committed_before_the_first_state_commit_is_recovered_by_a_restart_and_by_a_successor() {
        let d = crate::record_store::tempdir("q61-recovery");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric_id = crate::model::FabricId::mint();
        let birth = writer();
        {
            let root = crate::store_root::StoreRoot::open(&store, &fabric_id).unwrap();
            assert_eq!(root.writes_to(), "scratchpad", "the fabric has had no state-commit");
            let j = FileJournal::open_on_root(&root, "mesh1", &birth).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
            j.append_step_receipt(&step("b1", 1, "admit-node:mesh1.admin.1")).await.unwrap();
            j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
            // The process ends here, its scratchpad never published.
        }
        let id = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        for (who, mesh, writer) in [("the same fabric-primary restarted", "mesh1", birth.clone()), ("a successor", "mesh2", writer())] {
            let root = crate::store_root::StoreRoot::open(&store, &fabric_id).unwrap();
            assert_eq!(root.writes_to(), "scratchpad", "{who}: the fabric has still had no state-commit");
            let j = FileJournal::open_on_root(&root, mesh, &writer).await.unwrap();
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

    /// CONTRACT (R-BI6, Luke 2026-10-12: "the Build journal's location is the adapter's and
    /// provider's choice; RDM's file adapter keeps it on the fabric's shared store root
    /// (`fabric/builds/` under the writer ladder, the fabric-primary its writer) so a successor reads
    /// it there; the per-admin journal and `store_root.rs`'s exclusion of it go"): a `FileJournal`
    /// opened on a `StoreRoot` writes fabric-lane facts under `fabric/builds/rev-<R>/<seq>` and
    /// mesh-lane facts under `meshes/<mesh>/builds/rev-<R>/<seq>`, one key per fact (never under a per-admin data
    /// dir), each on the root when it is committed, before the first state-commit too (Q61), and a successor
    /// (whether already open on the same `StoreRoot` or freshly opened after a restart) reads both
    /// fabric-lane and mesh-lane facts from the shared store root without gossip.
    #[tokio::test]
    async fn the_build_journal_lives_on_the_shared_store_root_under_the_writer_ladder_and_a_successor_reads_it_there() {
        let d = crate::record_store::tempdir("rbi6-store-root-journal");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric_id = crate::model::FabricId::mint();

        // 1. Before the first state-commit, a root opened on `store` holds RDM's rows in its
        // scratchpad; each Build fact stands on the root when it is committed (Q61).
        let root_pre = crate::store_root::StoreRoot::open(&store, &fabric_id).unwrap();
        assert_eq!(root_pre.writes_to(), "scratchpad");
        let j_mesh1 = FileJournal::open_on_root(&root_pre, "mesh1", &writer()).await.unwrap();
        j_mesh1.publish_accepted(&intent("b1")).await.unwrap();
        j_mesh1.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
        j_mesh1.append_step_receipt(&step("b1", 1, "admit-node:mesh1.admin.1")).await.unwrap();
        assert_eq!(
            lane_files(root_pre.dir()),
            ["fabric/builds/rev-00000000000000000000/00000000000000000001", "fabric/builds/rev-00000000000000000000/00000000000000000002", "fabric/builds/rev-00000000000000000000/00000000000000000003"],
            "before state-commit, each committed fact is its own key of the fabric lane, in order, on the store root"
        );
        assert!(!root_pre.dir().join("fabric/identity.json").exists(), "nothing else of RDM's rows is on the root before state-commit");
        let pub_out = root_pre.scratchpad().unwrap().publish().await.unwrap();
        assert!(!pub_out.stores.iter().any(|s| s.store.starts_with("fabric/builds")), "state-commit publishes no lane key: none was held: {pub_out:?}");

        // 2. A second admin in mesh2 opens the shared store root; a successor in mesh1 also opens it.
        // Without gossip (`absorb_facts`), mesh2's journal already sees the accepted Build from the
        // shared store root, opens and runs a drift-lane attempt for mesh2 (written under
        // `meshes/mesh2/builds/rev-<R>/`), and the live `j_mesh1` plus a freshly opened
        // successor `j_successor` both read the fabric-lane and mesh2-lane facts from the store root.
        let id = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        let root_mesh2 = crate::store_root::StoreRoot::at(root_pre.dir());
        let j_mesh2 = FileJournal::open_on_root(&root_mesh2, "mesh2", &writer()).await.unwrap();
        assert_eq!(j_mesh2.read_build(&id).await.unwrap().attempt, 1);

        // Fabric-primary finishes fabric lane attempt 1 on `j_mesh1`; `j_mesh2` sees it on its next read/write
        // without `absorb_facts`.
        j_mesh1.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        assert_eq!(j_mesh2.read_build(&id).await.unwrap().state, BuildState::Complete);

        // Mesh2's primary opens, claims, steps, and converges a drift attempt on mesh2's lane.
        j_mesh2.open_attempt(&lane_open("b1", "mesh2", 1, "mesh2.rpc.2")).await.unwrap();
        assert_eq!(j_mesh2.claim_attempt(&lane_claim("b1", "mesh2", 1, "mesh2.admin.1")).await.unwrap(), ClaimOutcome::Won);
        let mut drift_step = step("b1", 1, "node.delete:mesh2.rpc.2");
        drift_step.mesh = Some("mesh2".into());
        j_mesh2.append_step_receipt(&drift_step).await.unwrap();
        let mut drift_verdict = attempt("b1", 1, AttemptOutcome::Converged);
        drift_verdict.mesh = Some("mesh2".into());
        j_mesh2.append_attempt_receipt(&drift_verdict).await.unwrap();
        assert_eq!(
            lane_files(&root_pre.mesh_builds("mesh2")).len(),
            4,
            "mesh2's primary writes its lane's facts under meshes/mesh2/builds/rev-<R>/: {:?}",
            lane_files(&root_pre.mesh_builds("mesh2"))
        );

        // Both the already-open `j_mesh1` and a newly booted successor read mesh2's converged lane from the shared store root.
        let view_live = serde_json::to_value(j_mesh1.read_build(&id).await.unwrap()).unwrap();
        assert_eq!(view_live["lanes"]["mesh2"]["state"], "complete", "live fabric-primary reads mesh2's lane from the store root: {view_live}");
        let j_successor = FileJournal::open_on_root(&crate::store_root::StoreRoot::at(root_pre.dir()), "mesh1", &writer()).await.unwrap();
        let view_succ = serde_json::to_value(j_successor.read_build(&id).await.unwrap()).unwrap();
        assert_eq!(view_succ["state"], "complete");
        assert_eq!(view_succ["lanes"]["mesh2"]["state"], "complete", "fresh successor reads both fabric/builds and meshes/mesh2/builds: {view_succ}");

        // 3. One fact gets one source (R-BI6): `holds_fabric` and `holds_mesh` read only the seat
        // record in `book.seats` (`SeatRecord::Open { holder: Some(_) }`), never `!launched` or the
        // backbone publisher flag. An admin booting on an inherited root while a primary lives
        // elsewhere (`sole_node_root == false`) has no seat record naming its birth in `book.seats`
        // (or the seat record names the primary living elsewhere), so it does NOT sync a journal it
        // does not own. Once the seat record in `book.seats` names its birth, it syncs the journal
        // it owns.
        let book = rdm_transport::membership::DigestBook::default();
        let elsewhere_node = crate::model::NodeId::mint();
        let elsewhere_inc = crate::model::IncarnationId::mint();
        book.seats.take(
            rdm_core::Seat::FabricPrimary,
            &rdm_core::SeatHolder { mesh: "mesh1".into(), node_id: elsewhere_node.clone(), incarnation: elsewhere_inc.clone(), revision: 1 },
        );
        book.seats.take(
            rdm_core::Seat::MeshPrimary,
            &rdm_core::SeatHolder { mesh: "mesh1".into(), node_id: elsewhere_node, incarnation: elsewhere_inc, revision: 1 },
        );
        let booting_me = (crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let booting_name: crate::model::PathName = "mesh1.admin.2".parse().unwrap();
        let records = std::sync::Arc::new(crate::admin::Records::default());
        records.set_me(booting_me.0.clone());
        records.set_book(book.clone());
        let f_seat = crate::admin::fabric_seat_source(&book, &records, booting_me.clone(), &booting_name);
        let m_seat = crate::admin::mesh_seat_source(&book, &records, booting_me.clone(), &booting_name, "mesh1");
        let j_unseated = FileJournal::open_on_root(&crate::store_root::StoreRoot::at(root_pre.dir()), "mesh1", &writer()).await.unwrap();
        j_unseated.set_fabric_seat(
            std::sync::Arc::new(move || matches!((f_seat.read)(), crate::fabric_builds::SeatRecord::Open { holder: Some(_) })),
            std::sync::Arc::new(move || matches!((m_seat.read)(), crate::fabric_builds::SeatRecord::Open { holder: Some(_) })),
        );
        assert_eq!(
            j_unseated.current_build().await.unwrap(),
            CurrentBuild::None,
            "an admin booting on an inherited root while a primary lives elsewhere holds no seat record in book.seats and does not sync fabric/builds or meshes/mesh1/builds"
        );
        assert!(
            j_unseated.facts().await.unwrap().is_empty(),
            "no journal lines synced when book.seats names another primary"
        );
        // Once this birth holds the seat record in `book.seats` (at revision 2), the same single
        // source reports `holds_fabric == true` and `holds_mesh == true`, and `j_unseated` syncs the
        // journal it now owns.
        book.seats.take(
            rdm_core::Seat::FabricPrimary,
            &rdm_core::SeatHolder { mesh: "mesh1".into(), node_id: booting_me.0.clone(), incarnation: booting_me.1.clone(), revision: 2 },
        );
        book.seats.take(
            rdm_core::Seat::MeshPrimary,
            &rdm_core::SeatHolder { mesh: "mesh1".into(), node_id: booting_me.0, incarnation: booting_me.1, revision: 2 },
        );
        assert_eq!(
            j_unseated.current_build().await.unwrap(),
            CurrentBuild::Completed(id),
            "once book.seats records this birth as holder, it syncs the journal from the shared store root"
        );
    }

    #[tokio::test]
    async fn the_journal_replays_the_original_step_and_attempt_codes() {
        use rdm_core::outcome::Code;
        for code in [Code::Unknown, Code::DeadlineExceeded, Code::Unavailable] {
            let dir = tmp();
            let receipt = BuildStepReceipt { outcome: StepOutcome::Failed { reason: "same explanation".into(), stage: None, code }, ..step("b1", 1, "node.restart:mesh1.rpc.2") };
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

    fn birth(mesh: &str, node: &str, inc: &str) -> rdm_core::handover::HandoverBirth {
        rdm_core::handover::HandoverBirth { mesh: mesh.into(), node_id: crate::model::NodeId::parse(node).unwrap(), incarnation: crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60(inc)).unwrap() }
    }

    /// A Build log on the store root at `dir` writing every lane under `revision`, owing the seals
    /// `storage`'s seat moves name, written by `by`.
    async fn seated_at(dir: &Path, revision: u64, storage: &Arc<dyn crate::fabric_storage::FabricStorage>, by: rdm_core::handover::HandoverBirth) -> FileJournal {
        let j = open_at(dir).await.unwrap();
        j.set_lane_seats(Arc::new(move |_| LaneSeat { revision, holds: Ok(()) }), crate::fabric_storage::seals_owed(storage.clone(), by));
        j
    }

    /// CONTRACT (Q35 x cells: "a crash between the row and the seal: the successor does not append, the
    /// next fold completes the seal"; Q28 A: "An old-authority writer's next append under N meets the
    /// seal and is refused by name (`RejectedStaleSeat` from the store, spanned)"): the holder writes
    /// facts under mesh1's seat revision 0 and commits the seat move 0 -> 1, and its process is gone
    /// before it seals. The successor, writing under revision 1, does not append before revision 0 is
    /// sealed: its next fold writes the seal the move owes (naming the move and the successor) after
    /// the holder's last fact, then appends at revision 1's first key. The old authority, still writing
    /// under revision 0, is refused stale by name and writes nothing; a fresh reader replays the
    /// holder's facts, then the successor's, and nothing of revision 0 past its seal. Must NOT happen:
    /// the successor's fact before revision 0's seal, or an old-authority fact committing after it.
    #[tokio::test]
    async fn a_crash_between_the_seat_move_and_its_seal_is_sealed_by_the_successors_next_fold_and_the_old_writer_is_refused_stale() {
        let dir = tmp();
        let storage: Arc<dyn crate::fabric_storage::FabricStorage> = Arc::new(crate::fabric_storage::FileFabricStorage::open(&crate::store_root::StoreRoot::at(&dir), &crate::model::IncarnationId::mint()).await.unwrap());
        let (holder, successor) = (birth("mesh1", "100000000000", "p"), birth("mesh1", "200000000000", "n"));
        let old = seated_at(&dir, 0, &storage, holder.clone()).await;
        old.publish_accepted(&intent("b1")).await.unwrap();
        old.open_attempt(&lane_open("b1", "mesh1", 1, "mesh1.rpc.2")).await.unwrap();
        // The holder commits the move of mesh1's seat 0 -> 1; its process is gone before its seal.
        let row = crate::fabric_storage::SeatTransferRow {
            operation: "node.primary.update:b1:1:node.stop:mesh1.admin.1".into(),
            build_id: "b1".into(),
            attempt: 1,
            holder: holder.clone(),
            successor: successor.clone(),
            expected: vec![rdm_core::handover::SeatRevision { seat: rdm_core::seat::transfer::Seat::MeshPrimary { mesh: "mesh1".into() }, expected_revision: 0 }],
            seats: vec![rdm_core::seat::transfer::CommittedSeat { seat: rdm_core::seat::transfer::Seat::MeshPrimary { mesh: "mesh1".into() }, holder: successor.clone(), revision: 1 }],
            by: holder.clone(),
        };
        assert!(matches!(storage.commit_seat_move(&row.seat_move()).await.unwrap(), crate::fabric_storage::MoveCommit::Committed(crate::fabric_storage::SeatMove { t: 1, .. })));
        let capture = crate::span_capture::Capture::new("e6-1-crash-between-row-and-seal");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let new = seated_at(&dir, 1, &storage, successor.clone()).await;
        new.claim_attempt(&lane_claim("b1", "mesh1", 1, "mesh1.admin.2")).await.unwrap();
        assert_eq!(
            lane_files(&dir),
            [
                "fabric/builds/rev-00000000000000000000/00000000000000000001",
                "meshes/mesh1/builds/rev-00000000000000000000/00000000000000000001",
                "meshes/mesh1/builds/rev-00000000000000000000/00000000000000000002",
                "meshes/mesh1/builds/rev-00000000000000000001/00000000000000000001",
            ],
            "the seal stands after the holder's last fact, and the successor's claim at revision 1's first key"
        );
        let seal: serde_json::Value = serde_json::from_slice(&std::fs::read(dir.join("meshes/mesh1/builds/rev-00000000000000000000/00000000000000000002.json")).unwrap()).unwrap();
        assert_eq!((seal["format"].as_str(), seal["record"]["revision"].as_u64(), seal["record"]["sealed_by"].as_u64()), (Some(crate::build_lanes::SEAL_FORMAT), Some(0), Some(1)), "{seal}");
        let sealed = capture.named("rdm.node_admin.build.update.via-lane-sealed");
        assert_eq!(sealed.len(), 1, "{sealed:?}");
        eprintln!("span rdm.node_admin.build.update.via-lane-sealed trace_id={} {:?}", sealed[0].trace_id, sealed[0].attributes);
        let refused = old.append_step_receipt(&{ let mut s = step("b1", 1, "node.delete:mesh1.rpc.2"); s.mesh = Some("mesh1".into()); s }).await.unwrap_err();
        assert!(matches!(&refused, BuildStateError::StaleSeat { at, revision: Some(0), detail } if at.ends_with("rev-00000000000000000000/00000000000000000002") && detail.contains("sealed by seat move 1")), "{refused}");
        let stale = capture.named("rdm.node_admin.build.reject.via-stale-seat");
        assert_eq!(stale.len(), 1, "{stale:?}");
        eprintln!("span rdm.node_admin.build.reject.via-stale-seat trace_id={} {:?}", stale[0].trace_id, stale[0].attributes);
        assert_eq!(lane_files(&dir).len(), 4, "nothing written past the seal");
        let replay = open_at(&dir).await.unwrap().facts().await.unwrap();
        assert_eq!(replay.iter().map(|f| match f { BuildFact::Accepted(_) => "accepted", BuildFact::Opened(_) => "opened", BuildFact::Claim(_) => "claim", _ => "other" }).collect::<Vec<_>>(), vec!["accepted", "opened", "claim"], "{replay:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (the Build journal wait, i143 e12.s89; i144 e6): each writer's turn of the journal is on
    /// its own span. A turn that waits for another names how long it waited (`waited_ms`) and the turn
    /// holding the writer when its wait began (`held_by`); every turn names how long it held the writer
    /// (`held_ms`) and how much of that was the lane store's own calls (`lane_ms`). Must NOT happen: a
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
        assert_eq!(ms(&holder, "lane_ms"), 0, "a turn that made no lane call names none: {holder:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (the Build journal's read path; i144 e6, `fabric_primary.rs:204`): a read of the journal
    /// reads the lanes from the store root without holding the writer, so a writer's turn never waits
    /// behind a reader's lane I/O. With the blocking pool's one thread held, a reader's sync is queued on
    /// the disk; a writer's turn begun then takes the writer at once and names no holder. Must NOT
    /// happen: a writer's turn whose `held_by` names a reader, or a reader holding the writer across the
    /// disk.
    #[test]
    fn a_writers_turn_never_waits_behind_a_readers_lane_io() {
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
            crate::record_store::op_waits_on_the_blocking_pool(setup, |j: Arc<FileJournal>| {
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

    /// CONTRACT (Q35 x: "`in-flight`: the one shared key names its claim's lane key and is void when
    /// that key is not in the lane below the seal"): a claim decided with its mesh's in-flight row names
    /// the claim's lane key there. A row of another attempt whose key holds nothing yet still holds the
    /// mesh (its claimant may still write it): `Held`, nothing written. A row whose key holds another
    /// entry (the seal that closed its claimant's revision) is void: it is released, spanned, and the
    /// claim takes the mesh. Must NOT happen: a void row holding the mesh forever, or a live one taken.
    #[tokio::test]
    async fn an_in_flight_row_names_its_claims_lane_key_and_is_void_once_that_key_holds_another_entry() {
        let dir = tmp();
        let root = crate::store_root::StoreRoot::at(&dir);
        let claims = crate::mesh_claims::MeshClaims::on_root(root.clone(), crate::model::IncarnationId::mint());
        let j = open_at(&dir).await.unwrap();
        j.publish_accepted(&intent("b1")).await.unwrap();
        j.publish_accepted(&intent("b2")).await.unwrap();
        j.open_attempt(&opened("b1", 1, AttemptReason::Requested)).await.unwrap();
        let row = |id: &str, attempt: u32| crate::mesh_claims::InFlight { build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), lane: None, attempt, claimant: "mesh1.admin.1".into(), lane_key: None };
        assert_eq!(j.claim_in_flight(&claim("b1", 1, "mesh1.admin.1"), &claims, "mesh2", &row("b1", 1)).await.unwrap(), InFlightClaim::Decided(ClaimOutcome::Won));
        let held = claims.held("mesh2").await.unwrap().unwrap();
        assert_eq!(held.lane_key.as_deref(), Some("fabric/builds/rev-00000000000000000000/00000000000000000004"), "the row names the claim's lane key: {held:?}");
        assert!(matches!(j.lanes().entry(&held.lane_key.as_deref().unwrap().parse().unwrap()).await.unwrap(), Some(crate::build_lanes::Entry::Fact(BuildFact::Claim(_)))));
        // Another attempt's row naming a key that holds nothing yet holds the mesh.
        claims.release("mesh2", &row("b1", 1)).await.unwrap();
        let pending = crate::mesh_claims::InFlight { lane_key: Some("fabric/builds/rev-00000000000000000000/00000000000000000009".into()), ..row("b9", 1) };
        assert_eq!(claims.take("mesh2", &pending).await.unwrap(), crate::mesh_claims::Taken::Won);
        j.open_attempt(&opened("b2", 1, AttemptReason::Requested)).await.unwrap();
        assert_eq!(j.claim_in_flight(&claim("b2", 1, "mesh1.admin.1"), &claims, "mesh2", &row("b2", 1)).await.unwrap(), InFlightClaim::Held(pending.clone()), "a row whose key holds nothing yet is live");
        // Its claimant's revision is sealed at that key: the row is void.
        j.lanes().read_new(&crate::build_lanes::LaneId::Fabric).await.unwrap();
        let void = crate::mesh_claims::InFlight { lane_key: Some("fabric/builds/rev-00000000000000000000/00000000000000000006".into()), ..row("b9", 1) };
        claims.release("mesh2", &pending).await.unwrap();
        assert_eq!(claims.take("mesh2", &void).await.unwrap(), crate::mesh_claims::Taken::Won);
        j.lanes().seal(&crate::build_lanes::LaneId::Fabric, &crate::build_lanes::LaneSeal { revision: 0, sealed_by: 1, by: birth("mesh1", "200000000000", "n") }).await.unwrap();
        assert!(!j.in_flight_live(&void).await.unwrap(), "the key holds the seal");
        // The successor, writing under revision 1, claims: the void row is released and the mesh is its.
        let capture = crate::span_capture::Capture::new("e6-1-in-flight-void");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let none: Arc<dyn crate::fabric_storage::FabricStorage> = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        let successor = seated_at(&dir, 1, &none, birth("mesh1", "200000000000", "n")).await;
        assert_eq!(successor.claim_in_flight(&claim("b2", 1, "mesh1.admin.2"), &claims, "mesh2", &row("b2", 1)).await.unwrap(), InFlightClaim::Decided(ClaimOutcome::Won));
        assert_eq!(claims.held("mesh2").await.unwrap().and_then(|h| h.lane_key), Some("fabric/builds/rev-00000000000000000001/00000000000000000001".into()));
        let voided = capture.named("rdm.node_admin.build.update.via-in-flight-void");
        assert_eq!(voided.len(), 1, "{voided:?}");
        eprintln!("span rdm.node_admin.build.update.via-in-flight-void trace_id={} {:?}", voided[0].trace_id, voided[0].attributes);
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
        let step = StepOutcome::Failed { reason: "r".into(), stage: None, code: Code::Unavailable };
        assert_eq!(rdm_transport::wire::encode(&step).unwrap(), vec![1, 1, b'r', 0, 14]);
        let attempt = AttemptOutcome::Failed { reason: "r".into(), code: Code::DeadlineExceeded };
        assert_eq!(rdm_transport::wire::encode(&attempt).unwrap(), vec![1, 1, b'r', 4]);
    }
    #[test]
    fn replay_retains_typed_failure_codes_and_defaults_only_legacy_json_failed_state() {
        for code in [Code::Unknown, Code::DeadlineExceeded, Code::Unavailable] {
            let outcome = StepOutcome::Failed { reason: "the same explanation".into(), stage: None, code };
            let json = serde_json::to_vec(&outcome).unwrap();
            assert_eq!(serde_json::from_slice::<StepOutcome>(&json).unwrap(), outcome);
            let wire = rdm_transport::wire::encode(&outcome).unwrap();
            assert_eq!(rdm_transport::wire::decode::<StepOutcome>(&wire).unwrap(), outcome);
            assert_eq!(wire.last(), Some(&code.number()));
        }
        assert_eq!(serde_json::from_str::<StepOutcome>(r#"{"failed":{"reason":"the same explanation","stage":null}}"#).unwrap(), StepOutcome::failed("the same explanation"));
    }
}
