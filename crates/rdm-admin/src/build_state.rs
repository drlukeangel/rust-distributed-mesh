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

use crate::accepted::{AttemptAction, FabricTopology};
use rdm_core::build::BuildChange;
use crate::build::BuildId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[cfg(feature = "file-backend")]
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// A Build accepted by the fabric-primary: the complete topology it realizes, and the change that
/// produced it (history only; planning reads `topology`, never `change`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildAccepted {
    /// The Build's id.
    pub build_id: BuildId,
    /// The complete topology the Build realizes.
    pub topology: FabricTopology,
    /// The change as it came in (build.md §1): history only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change: Option<BuildChange>,
    /// Who wrote the record: the accepting fabric-primary's exact birth,
    /// `rdm://<fabric_id>/<node_id>/<incarnation>` (gossip.md §3.4's `source`). A record written before
    /// the field names none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// When the Build was accepted, in milliseconds since the Unix epoch.
    pub time: u64,
}

/// Why an attempt of a Build was opened: what a caller asked for (build.md §1, Q342). A node command,
/// a repair and drift are never a Build's attempt; their positions are reserved, so every later reason
/// keeps its position on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AttemptReason {
    /// The first attempt: the Build was just accepted.
    Requested,
    /// Reserved position: the removed drift repair's reason (Q342). Never opened.
    Reserved1,
    /// Reserved position: the removed node.restart's reason (Q183; callers run node.stop then
    /// node.start). Never opened; kept so every later reason keeps its position.
    Reserved2,
    /// Reserved position: the removed primary-moved reason (Q342). Never opened.
    Reserved3,
    /// Reserved position: the removed connections delete's reason; it is a node command (Q342). Never opened.
    Reserved4,
    /// Reserved position: the removed stop's reason; it is a node command (Q342). Never opened.
    Reserved5,
    /// Reserved position: the removed start's reason; it is a node command (Q342). Never opened.
    Reserved6,
    /// Reserved position: the removed node recovery's reason; it is the primary's own workflow (Q342). Never opened.
    Reserved7,
    /// The delete of a stopped birth at its retained predecessor location, or of a deleted mesh's last admin, was
    /// requested (`node.delete:<path>`, node.delete's one key).
    Delete,
    /// Reserved position: the removed steps' reason (Q342: a replace is node commands, then a Build
    /// that adds; node-replace.md). Never opened.
    Reserved9,
    /// Reserved position: the removed adoption launch's reason; it is fabric recovery, the primary's own
    /// workflow (Q342). Never opened.
    Reserved10,
}

impl AttemptReason {
    /// The reason's name as it appears in views and spans.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Reserved1 => "reserved-1",
            Self::Reserved2 => "reserved-2",
            Self::Reserved3 => "reserved-3",
            Self::Reserved4 => "reserved-4",
            Self::Reserved5 => "reserved-5",
            Self::Reserved6 => "reserved-6",
            Self::Reserved7 => "reserved-7",
            Self::Delete => "delete",
            Self::Reserved9 => "reserved-9",
            Self::Reserved10 => "reserved-10",
        }
    }

    /// Whether the reason is a reserved position, which no attempt is opened with.
    pub fn is_reserved(self) -> bool {
        !matches!(self, Self::Requested | Self::Delete)
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
    /// The opener's globally unique node ID.
    pub opened_by: rdm_core::NodeId,
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
    /// The actual in-flight mesh claimed; None for a fact that took no in-flight row.
    pub claimed_mesh: Option<String>,
    /// The executor's immutable owning mesh when this claim was admitted, distinct from the lane.
    pub executor_mesh: String,
    /// The Build.
    pub build_id: BuildId,
    /// The attempt claimed.
    pub attempt: u32,
    /// The exact executing birth fixed at admission; renaming does not change it.
    pub executor: rdm_core::framing::Birth,
    /// The globally unique ID of the admin that decided the claim: the fabric-primary for a fabric-lane
    /// attempt, the mesh's own mesh-primary for a drift attempt. What a refusal names as the holder of
    /// the attempt's mesh, as the claim recorded it (never re-read from a later role view). A line
    /// written before claimants were recorded carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimant: Option<crate::model::NodeId>,
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
    /// The operation's idempotency key (`node.create:mesh1.node.2`, ...).
    pub operation: rdm_core::OperationId,
    /// The step.
    pub step: String,
    /// How the step ended.
    pub outcome: StepOutcome,
    /// What a completed step decided (ids, endpoints, the runtime handle), so
    /// a re-run reuses it instead of deciding again. Absent on older lines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    /// The exact executing admin birth that decided this step. A re-run compares IDs and
    /// incarnations independently of mutable names and network endpoints. Synthetic receipts omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<rdm_core::framing::Birth>,
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
        to: crate::model::NodeId,
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
    /// The change as it came in (build.md §1): history only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change: Option<BuildChange>,
    /// When the Build was accepted, in milliseconds since the Unix epoch.
    pub time: u64,
    /// Where the Build is in its life.
    pub state: BuildState,
    /// The current attempt: the highest attempt claimed. An opened attempt is `attempt + 1`.
    pub attempt: u32,
    /// The executor of the current attempt, when one is.
    pub executor: Option<rdm_core::framing::Birth>,
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
    pub attempt_executors: BTreeMap<u32, rdm_core::framing::Birth>,
    /// Explicit executor ownership from each winning claim.
    pub attempt_executor_meshes: BTreeMap<u32, String>,
    /// Actual in-flight scope recorded by each winning claim.
    pub attempt_claimed_meshes: BTreeMap<u32, String>,
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
    pub claimant: Option<crate::model::NodeId>,
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
    /// Actual in-flight scope recorded by the current claim.
    pub claimed_mesh: Option<String>,
    /// Ownership recorded by the current winning claim.
    pub executor_mesh: Option<String>,
    /// Where the lane's current attempt is.
    pub state: BuildState,
    /// The lane's current attempt: the highest claimed. An opened attempt is `attempt + 1`.
    pub attempt: u32,
    /// The executor of the current attempt, when one is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<rdm_core::framing::Birth>,
    /// The admin that decided the current attempt's claim (the mesh's mesh-primary).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimant: Option<crate::model::NodeId>,
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
        matches!(self.change, Some(BuildChange::DeleteFabric))
    }

    /// Whether this Build leaves the fabric no mesh: a fabric.delete, or the fabric-primary's own delete
    /// after fabric.deleted (fabric-delete.md step 4). The fabric is never made ready again from it.
    pub fn leaves_no_fabric(&self) -> bool {
        self.deletes_fabric() || self.topology.meshes.is_empty()
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
        let l = self.lanes.get(mesh).cloned().unwrap_or(Lane { claimed_mesh: None, executor_mesh: None,
            state: BuildState::Complete,
            attempt: 0,
            executor: None,
            claimant: None,
            reason: AttemptReason::Requested,
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
        p.attempt_claimed_meshes = l.claimed_mesh.iter().map(|m| (l.attempt, m.clone())).collect();
        p.attempt_executor_meshes = l.executor_mesh.iter().map(|m| (l.attempt, m.clone())).collect();
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
    /// `Opened` fact opened ([`Self::opened`]), or the first attempt of the Build. A role update's
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
    pub fn completed_in_run(&self, key: &rdm_core::OperationId) -> bool {
        self.steps.iter().any(|s| self.reported_in_run(s.attempt) && &s.operation == key && s.step == "Complete" && s.outcome == StepOutcome::Complete)
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
            (p.time, &p.build_id) > (c.time, &c.build_id)
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
    Ok((current.deletes_fabric() && (current.time, &current.build_id) > (p.time, &p.build_id)).then_some(current))
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
    if current.as_ref().is_some_and(|c| c.time >= newest) {
        return Ok(None);
    }
    let facts = builds.facts().await?;
    let Some(id) = facts.iter().rev().find_map(|f| match f {
        BuildFact::Accepted(a) if a.time == newest && matches!(a.change, Some(BuildChange::DeleteFabric)) => Some(a.build_id.clone()),
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

/// Read the winning committed claim for exactly one Build, scope and attempt.
pub async fn committed_claim<S: BuildStateAdapter + ?Sized>(builds: &S, build_id: &BuildId, mesh: Option<&str>, attempt: u32) -> Result<Option<BuildAttemptClaim>, BuildStateError> {
    let facts = builds.committed_facts(Some(build_id)).await?;
    Ok(claim_holder(&facts, build_id, mesh, attempt).cloned())
}

/// The result of claiming an attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The attempt is the claimant's.
    Won,
    /// The attempt is already claimed by `holder`.
    Lost {
        /// The executor holding the attempt.
        holder: crate::model::NodeId,
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
    RoleYielded {
        /// The admin whose Build log yielded the role.
        node: rdm_core::NodeId,
        /// The record that moved the role, in words.
        by: String,
        /// The exact birth named directly by the primary record.
        to: Option<rdm_core::framing::Birth>,
    },
}

impl std::fmt::Display for BuildStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownBuild(id) => write!(f, "no Build {id}"),
            Self::ConflictingIntent(id) => write!(f, "Build {id} was published with a different intent"),
            Self::Io(e) => write!(f, "build journal I/O: {e}"),
            Self::Unencodable(e) => write!(f, "Build fact cannot be sent: {e}"),
            Self::RoleYielded { node, by, .. } => write!(f, "{node} is no longer the fabric-primary: {by}; its Build log decides nothing any more"),
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
pub fn primaries_taken_operation(lost: &crate::model::NodeId) -> rdm_core::OperationId {
    rdm_core::OperationId::node("fabric-recovery", lost)
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
    pub held: Vec<ReceiptKey>,
}

/// Every [`PrimariesTaken`] transition recorded in `steps`, in step order.
pub fn primaries_taken(steps: &[BuildStepReceipt]) -> Vec<PrimariesTaken> {
    steps
        .iter()
        .filter(|r| r.mesh.is_none() && r.step == STEP_PRIMARIES_TAKEN && r.outcome == StepOutcome::Complete)
        .filter_map(|r| r.output.clone().and_then(|v| serde_json::from_value::<PrimariesTaken>(v).ok()))
        .collect()
}

/// A step receipt's identity within its attempt. Its operation fields and step remain
/// separate in retained-receipt lists, including through primary transfer and replay.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptKey {
    /// The admitted structured operation.
    pub operation: rdm_core::OperationId,
    /// The workflow step, independent of the operation identity.
    pub step: String,
}

/// The same identity for comparison and retained-receipt serialization.
pub fn receipt_key(r: &BuildStepReceipt) -> ReceiptKey {
    ReceiptKey { operation: r.operation.clone(), step: r.step.clone() }
}

/// Fold an ordered fact log into Build views (`docs/architecture/build.md` §5.2–§5.3). Deterministic
/// and idempotent: the same log gives the same views, and a duplicated fact changes nothing. The
/// winning claim of an attempt is the first one appended. A fabric-section receipt of an attempt a
/// recorded transition supersedes ([`PrimariesTaken`], Q160) is ignored unless the transition held
/// it, and that attempt's verdict decides nothing: the superseded writer wrote them after the
/// transfer.
pub fn fold(facts: &[BuildFact]) -> BTreeMap<BuildId, BuildProjection> {
    let superseded: BTreeMap<(BuildId, u32), std::collections::BTreeSet<ReceiptKey>> = facts
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
    let mut winners: BTreeMap<(BuildId, Option<String>, u32), rdm_core::framing::Birth> = BTreeMap::new();
    let mut opened: std::collections::BTreeSet<(BuildId, Option<String>, u32)> = std::collections::BTreeSet::new();
    // An attempt has one verdict: the first appended. A stale second receipt changes nothing.
    let mut decided: std::collections::BTreeSet<(BuildId, Option<String>, u32)> = std::collections::BTreeSet::new();
    for f in ordered {
        match f {
            // A Build's sections (Q284) fold into one topology; its whole acceptance replaces them.
            BuildFact::Accepted(i) if out.contains_key(&i.build_id) => {
                let p = out.get_mut(&i.build_id).expect("held");
                if p.topology.section_of.is_some() && i.topology.section_of.is_none() {
                    p.change = i.change.clone();
                }
                p.topology.absorb(&i.topology);
            }
            BuildFact::Accepted(i) => {
                out.entry(i.build_id.clone()).or_insert_with(|| BuildProjection {
                    build_id: i.build_id.clone(),
                    topology: i.topology.clone(),
                    change: i.change.clone(),
                    time: i.time,
                    state: BuildState::Pending,
                    attempt: 0,
                    executor: None,
                    steps: Vec::new(),
                    last_failure: None,
                    reason: AttemptReason::Requested,
                    action: None,
                    attempt_actions: BTreeMap::new(),
                    attempt_executors: BTreeMap::new(),
                    attempt_executor_meshes: BTreeMap::new(),
                    attempt_claimed_meshes: BTreeMap::new(),
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
                    let lane = p.lanes.entry(mesh.clone()).or_insert_with(|| Lane { claimed_mesh: None, executor_mesh: None,
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
                            lane.executor_mesh = Some(c.executor_mesh.clone());
                            lane.claimed_mesh = c.claimed_mesh.clone();
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
                        p.attempt_executor_meshes.insert(c.attempt, c.executor_mesh.clone());
                        if let Some(mesh) = &c.claimed_mesh { p.attempt_claimed_meshes.insert(c.attempt, mesh.clone()); }
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
    pub removals_ended: std::collections::BTreeMap<(String, u32, rdm_core::OperationId), &'static str>,
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
    pub operation: rdm_core::OperationId,
    /// The step.
    pub step: String,
    /// Why the operation cannot be read.
    pub reason: String,
}

/// The durable record of Builds, attempts, claims and step receipts: appended facts and their
/// folded views.
#[async_trait]
pub trait BuildStateAdapter: Send + Sync {
    /// The explicit receipt lane this adapter projects and writes; `None` is the fabric lane.
    fn receipt_mesh(&self) -> Option<String> { None }

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
    /// The acceptance stamp (`time`) of the newest Build accepted, whatever its state: the
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
    /// its own acceptance stamp `(time, build_id)`. What `build.get` streams in order (Q167
    /// R-RD5). RDM's adapters answer it from their index (or delegate to the adapter they decorate); an
    /// adapter that keeps none answers it from its facts' acceptances.
    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        let mut accepted: Vec<(u64, BuildId)> = self.facts().await?.into_iter().filter_map(|f| match f {
            BuildFact::Accepted(a) => Some((a.time, a.build_id)),
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
        let admitted = BuildAttemptClaim { claimed_mesh: Some(mesh.to_owned()), ..claim.clone() };
        let claim = &admitted;
        self.read_build(&claim.build_id).await?;
        if let Some(held) = committed_claim(self, &claim.build_id, claim.mesh.as_deref(), claim.attempt).await? {
            return Ok(InFlightClaim::Decided(if held == *claim { ClaimOutcome::Won } else { ClaimOutcome::Lost { holder: held.executor.node_id } }));
        }
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
    /// The committed facts of `build`, every Build's when `None`, read fresh from the Build storage
    /// this adapter persists to (Q292: an executor, a starting admin and an admin taking a primary
    /// role read the named Build's committed records, never a copy a command carried): the records the
    /// read obtained, in commit order, or an error naming why, never a cached projection. `Err(UnknownBuild)`
    /// when the storage holds no record of `build`; an unreadable store is `Err(Io)`, never an empty
    /// Build. Required: an adapter that persists nowhere answers its own facts, by name.
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError>;
    /// Drop a finished Build from the views (history administration).
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError>;
    /// The step in flight of `operation` in `attempt` of `lane` (`None` the fabric lane, `Some(mesh)`
    /// that mesh's drift lane) waits on `reason`: live progress for a call attached to the run, never
    /// a fact, never durable. A state with no call attached to its runs
    /// has nobody to tell.
    async fn note_blocked(&self, _build_id: &BuildId, _lane: Option<&str>, _attempt: u32, _operation: &rdm_core::OperationId, _step: &str, _reason: &str) {}
    /// The Build, attempt and operation of the mesh.delete whose completion receipt
    /// (`mesh_delete::STEP_MESH_DELETED`) names the exact birth `(node_id, incarnation)` its final
    /// primary: the mesh.delete that ended with that birth alive, whose mesh-left follows that birth's
    /// exited verdict. The first such attempt. A point read by the birth: every adapter answers it from
    /// its index (or delegates to the adapter it decorates), never by reading the fact log.
    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, rdm_core::OperationId)>, BuildStateError>;
    /// The path name this admin's topology view holds (`Some` once its location view holds one).
    fn held_name(&self) -> Option<crate::model::PathName> {
        None
    }
}

/// The birth a deleting workflow's completed `step` receipt names, for the two steps whose output is
/// the lifecycle operation of the node.delete of that birth (`NodeDeleting`, `NodeDeleted`):
/// `(path, node_id, incarnation)`.
/// Extract a committed receipt assertion; this does not validate cross-attempt reuse provenance.
pub(crate) fn delete_step_birth(s: &BuildStepReceipt, step: &str) -> Option<(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId)> {
    if s.step != step || s.outcome != StepOutcome::Complete || s.operation.action != "node.delete" { return None; }
    let op: rdm_core::LifecycleOp = serde_json::from_value(s.output.as_ref()?.clone()).ok()?;
    if op.build_id != s.build_id.as_str() || op.operation != s.operation { return None; }
    let matches_subject = match &s.operation.subject {
        Some(rdm_core::OperationSubject::Node(id)) => *id == op.node_id,
        Some(rdm_core::OperationSubject::Birth { node_id, incarnation }) => *node_id == op.node_id && *incarnation == op.incarnation,
        _ => false,
    };
    matches_subject.then_some((op.location(), op.node_id, op.incarnation))
}

/// Last completed or failed operation attempt before a new execution, in the same Build and lane.
/// This is the pipeline's existing boundary for handing completed step outputs to a retry.
pub(crate) fn operation_reuse_boundary(receipts: &[&BuildStepReceipt], build_id: &BuildId, mesh: &Option<String>, operation: &rdm_core::OperationId, attempt: u32) -> u32 {
    receipts.iter().filter(|r| r.build_id == *build_id && r.mesh == *mesh && r.operation == *operation && r.attempt < attempt)
        .filter(|r| matches!(r.outcome, StepOutcome::Failed { .. }) || (r.step == "Complete" && r.outcome == StepOutcome::Complete))
        .map(|r| r.attempt).max().unwrap_or(0)
}

/// Validate a lifecycle payload against its receipt and the exact expected birth. An older
/// payload must come from an eligible completed step, after the same boundary the pipeline uses.
/// Returns the attempt that originally produced the acknowledgement.
pub(crate) fn validate_carried_lifecycle(projection: &BuildProjection, receipt: &BuildStepReceipt, payload: &rdm_core::LifecycleOp, expected: (&crate::model::NodeId, &crate::model::IncarnationId)) -> Option<u32> {
    if &payload.node_id != expected.0 || &payload.incarnation != expected.1 { return None; }
    if receipt.build_id != projection.build_id || payload.build_id != receipt.build_id.as_str() || payload.operation != receipt.operation || payload.attempt > receipt.attempt { return None; }
    if payload.attempt == receipt.attempt { return Some(payload.attempt); }
    let receipts: Vec<_> = projection.steps.iter().collect();
    let ended = operation_reuse_boundary(&receipts, &receipt.build_id, &receipt.mesh, &receipt.operation, receipt.attempt);
    if payload.attempt <= ended { return None; }
    projection.steps.iter().any(|source| {
        if source.build_id != receipt.build_id || source.mesh != receipt.mesh || source.operation != receipt.operation || source.attempt != payload.attempt || source.outcome != StepOutcome::Complete { return false; }
        let Some(value) = &source.output else { return false };
        let Ok(original) = serde_json::from_value::<rdm_core::LifecycleOp>(value.clone()) else { return false };
        original.build_id == payload.build_id && original.attempt == payload.attempt && original.operation == payload.operation
            && &original.node_id == expected.0 && &original.incarnation == expected.1
    }).then_some(payload.attempt)
}

/// Whether a node's stored acknowledgement names this receipt's operation: the declaration carries
/// the operation alone (its identity), so it is matched on that.
pub(crate) fn declaration_names_receipt(projection: &BuildProjection, receipt: &BuildStepReceipt, declaration: &rdm_core::status::DeclaredOperation) -> bool {
    receipt.build_id == projection.build_id && declaration.operation == receipt.operation
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
        Some(crate::accepted::AttemptAction::Delete { path, node_id, from_incarnation, .. }) => format!("delete of path {path_field:?}, name {name_field:?} from {node_id} at {from_incarnation}", path_field = &path.path, name_field = &path.name),
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
    /// `(time, build_id)`, entered once by its `Accepted` fact and never rewritten, in
    /// whatever order the facts arrive. Read newest first by [`current_build`].
    accepted_order: std::collections::BTreeSet<(u64, BuildId)>,
    /// Each Build's own facts, by their position in `facts`: a Build is read by folding its facts
    /// alone ([`read_own`]).
    by_build: std::collections::HashMap<BuildId, Vec<usize>>,
}

/// The index [`BuildStateAdapter::mesh_deleted_of`] answers from, derived fact by fact in whatever order
/// the facts arrive. Every adapter keeps one: RDM's file and memory adapters inside their log, an
/// application's adapter beside its own store, filled by [`BirthIndex::index`] with every fact it
/// appends, absorbs or reads back on open, so the read is a point read of it and never a fold of the
/// fact log.
#[derive(Debug, Default, Clone)]
pub struct BirthIndex {
    /// The mesh.delete completion index: every `MeshDeleted` receipt by the final primary its
    /// manifest names, to its Build, attempt and operation ([`BuildStateAdapter::mesh_deleted_of`]).
    mesh_deleted: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), Vec<(BuildId, u32, rdm_core::OperationId)>>,
}

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
            self.accepted_order.insert((a.time, a.build_id.clone()));
        }
        self.by_build.entry(fact.build_id().clone()).or_default().push(at);
    }
}

/// Where an adapter that keeps no log in memory keeps a [`BirthEntry`] ([`BirthEntry::keys`]): under
/// the birth it names. A birth read reads the entries of one key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BirthKey {
    /// A birth `(node_id, incarnation)`.
    Birth(crate::model::NodeId, crate::model::IncarnationId),
}

/// One entry of a [`BirthIndex`], derived from one fact ([`birth_entries`]). An adapter that keeps
/// no log in memory keeps each entry under every key of [`BirthEntry::keys`], by its
/// [`BirthEntry::slot`] within that key: the first entry written in a slot stands and a later one in
/// the same slot is not written. The birth read then reads those keys back ([`BirthRows`]) and
/// answers from the same derivation ([`BirthIndex::mesh_deleted_of_rows`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BirthEntry {
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
        /// The structured operation and its admitted subject.
        operation: rdm_core::OperationId,
    },
}

/// A composite receipt-index slot. Operation identity remains a structured field.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BirthSlot {
    build_id: BuildId,
    attempt: u32,
    operation: rdm_core::OperationId,
}

impl BirthEntry {
    /// The keys the entry is kept under.
    pub fn keys(&self) -> Vec<BirthKey> {
        match self {
            Self::MeshDeleted { node_id, incarnation, .. } => vec![BirthKey::Birth(node_id.clone(), incarnation.clone())],
        }
    }

    /// The entry's slot within a key: two entries of one slot are one entry, the first written.
    pub fn slot(&self) -> BirthSlot {
        match self {
            Self::MeshDeleted { build_id, attempt, operation, .. } => BirthSlot { build_id: build_id.clone(), attempt: *attempt, operation: operation.clone() },
        }
    }
}

/// The entries [`BirthIndex::index`] derives from `fact`: none for a fact no birth read answers from.
pub fn birth_entries(fact: &BuildFact) -> Vec<BirthEntry> {
    match fact {
        BuildFact::Step(s) => mesh_deleted_final(s).map(|(node_id, incarnation)| BirthEntry::MeshDeleted { node_id, incarnation, build_id: s.build_id.clone(), attempt: s.attempt, operation: s.operation.clone() }).into_iter().collect(),
        _ => Vec::new(),
    }
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
            BirthEntry::MeshDeleted { node_id, incarnation, build_id, attempt, operation } => {
                let held = self.mesh_deleted.entry((node_id, incarnation)).or_default();
                if !held.iter().any(|(b, a, o)| *b == build_id && *a == attempt && *o == operation) {
                    held.push((build_id, attempt, operation));
                }
            }
        }
    }

    /// The entries kept under the birth `(node_id, incarnation)`.
    async fn birth_rows(rows: &dyn BirthRows, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Vec<BirthEntry>, BuildStateError> {
        rows.entries_at(&BirthKey::Birth(node_id.clone(), incarnation.clone())).await
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

    /// [`BirthIndex::mesh_deleted_of`] over the entries `rows` keeps.
    pub async fn mesh_deleted_of_rows(rows: &dyn BirthRows, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, rdm_core::OperationId)>, BuildStateError> {
        Ok(Self::of_birth(rows, node_id, incarnation).await?.mesh_deleted_of(node_id, incarnation))
    }

    /// The point read of [`BuildStateAdapter::mesh_deleted_of`]: the first attempt's.
    pub fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Option<(BuildId, u32, rdm_core::OperationId)> {
        self.mesh_deleted.get(&(node_id.clone(), incarnation.clone()))?.iter().min_by_key(|(_, a, _)| *a).cloned()
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
fn claim_holder<'a>(facts: &'a [BuildFact], build_id: &BuildId, mesh: Option<&str>, attempt: u32) -> Option<&'a BuildAttemptClaim> {
    facts.iter().find_map(|f| match f {
        BuildFact::Claim(c) if &c.build_id == build_id && c.mesh.as_deref() == mesh && c.attempt == attempt => Some(c),
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
    /// The Build storage this log's admin reads committed records from (Q292), when it is another log:
    /// an admin of a fabric whose fabric-primary's log stands for the shared store. None: this log is
    /// its own storage.
    storage: std::sync::OnceLock<Storage>,
}

/// The Build storage a [`MemoryBuildStateAdapter`] reads committed records from (Q292).
#[derive(Clone)]
struct Storage(Arc<dyn BuildStateAdapter>);

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Storage")
    }
}

impl MemoryBuildStateAdapter {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// This log, its admin reading the Build's committed records from `storage` (Q292): an admin's own
    /// log beside the fabric's shared Build storage, as every admin but the fabric-primary keeps one.
    pub fn persisted_in(self, storage: Arc<dyn BuildStateAdapter>) -> Self {
        self.persist_in(storage);
        self
    }

    /// [`Self::persisted_in`] for a log already shared: the first storage it is given holds.
    pub fn persist_in(&self, storage: Arc<dyn BuildStateAdapter>) {
        let _ = self.storage.set(Storage(storage));
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
    /// Merge the committed records read from storage (Q292: [`BuildStateAdapter::committed_facts`]):
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
    /// Merge one Build's whole committed records, as a read of storage answered them (Q292), into what
    /// this log holds. A whole Build's facts are
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
    /// answers; any other admin answers every read from what it absorbed, its reads of the Build's
    /// committed records (Q292), and takes nothing. A log that reads no store has nothing to switch.
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
/// path and name while it does, `None` while it holds only what reached it.
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
            return Ok(if holder == claim { ClaimOutcome::Won } else { ClaimOutcome::Lost { holder: holder.executor.node_id.clone() } });
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
    /// A log held in one process persists nowhere: its committed facts are the ones it holds, or the
    /// storage it was given's ([`MemoryBuildStateAdapter::persisted_in`]).
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
        if let Some(Storage(storage)) = self.storage.get() {
            return storage.committed_facts(build).await;
        }
        committed_of(self.facts().await?, build)
    }

    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, rdm_core::OperationId)>, BuildStateError> {
        Ok(self.log.lock().unwrap().births.mesh_deleted_of(node_id, incarnation))
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
// durable format moved: structured operations, separate path/name, exact executor births.
pub const RECORD_FORMAT: &str = "build-record/2";

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
/// store root's `builds.jsonl`), read whole now (Q292): each key once, in the order they were committed
/// (`seq`), as a take folds them; an object another writer is still writing is left out by the read.
/// `Err(UnknownBuild)` when it holds none of `build`, an I/O error when the file cannot be read, never
/// an empty Build. Blocking: the journal calls it on the blocking pool.
pub fn read_committed(file: &std::path::Path, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
    let lines = crate::store_log::read_file(file).map_err(store_io)?;
    let prefix = build.map(|b| format!("{b}/"));
    let mut seen = std::collections::HashSet::new();
    let mut stored: Vec<StoredFact> = Vec::new();
    for line in lines.iter().filter(|l| l.format == RECORD_FORMAT && prefix.as_deref().is_none_or(|p| l.key.starts_with(p))) {
        if seen.insert(line.key.clone()) {
            stored.push(line.decode::<StoredFact>(file).map_err(store_io)?);
        }
    }
    stored.sort_by_key(|s| s.seq);
    committed_of(stored.into_iter().map(|s| s.fact).collect(), build)
}

#[cfg(feature = "file-backend")]
/// The key `fact` is written at, after its Build's id (`<build_id>/<key>`): one record per fact (build.md §5.3): the
/// acceptance at `accepted`, an attempt's open, claim and result at `<attempt>-opened`, `-claim` and
/// `-result` (`<mesh>-<attempt>` for an attempt in a mesh), one receipt per step at
/// `<attempt>-step-<opaque receipt hash>`, and a forget at `forget`. The record keeps all fields.
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
        BuildFact::Step(s) => {
            // An opaque physical journal index only. The record retains the structured
            // operation and step; nothing parses this hash to recover either field.
            let bytes = serde_json::to_vec(&receipt_key(s)).map_err(|e| BuildStateError::Io(format!("encoding receipt index: {e}")))?;
            let mut hash = blake3::Hasher::new();
            hash.update(b"rdm-build-receipt-index/1\0");
            hash.update(&bytes);
            format!("{}-step-{}", attempt(&s.mesh, s.attempt), hash.finalize().to_hex())
        },
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
                return Ok(if holder == claim { ClaimOutcome::Won } else { ClaimOutcome::Lost { holder: holder.executor.node_id.clone() } });
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
    /// A fresh read of `builds.jsonl` whole (Q292), on the blocking pool: neither the shared cursor
    /// nor the writer's turn is touched, so what is answered is exactly what this read obtained. Its
    /// records are folded as a take folds them: each key once, in the order they were committed
    /// (`seq`); an object another writer is still writing is left out by the read.
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
        let path = self.file.path().to_path_buf();
        let build = build.cloned();
        tokio::task::spawn_blocking(move || read_committed(&path, build.as_ref()))
            .await
            .map_err(|e| BuildStateError::Io(format!("the read of the Build records did not finish: {e}")))?
    }

    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, rdm_core::OperationId)>, BuildStateError> {
        self.sync_for_read("mesh_deleted_of").await?;
        Ok(self.log.lock().unwrap().births.mesh_deleted_of(node_id, incarnation))
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
    struct Rows(std::sync::Mutex<std::collections::HashMap<super::BirthKey, std::collections::BTreeMap<super::BirthSlot, super::BirthEntry>>>);

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
    fn a_structured_step_without_optional_output_decodes() {
        let mut without_output = serde_json::to_value(step("b1", 1, rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())))).unwrap();
        without_output.as_object_mut().unwrap().remove("output");
        let receipt: BuildStepReceipt = serde_json::from_value(without_output).unwrap();
        assert_eq!(receipt.output, None);
        let with = BuildStepReceipt { output: Some(serde_json::json!({"pid": 7})), ..step("b1", 1, rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap()))) };
        let back: BuildStepReceipt = serde_json::from_str(&serde_json::to_string(&with).unwrap()).unwrap();
        assert_eq!(back, with);
    }

    use super::*;

    /// CONTRACT: retained receipts preserve operation/step boundaries and both location fields.
    #[test]
    fn receipt_identity_preserves_structured_fields() {
        let location = rdm_core::PathName::new("elsewhere.gateway", "north").unwrap();
        let first = step("b1", 1, rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&location)));
        let mut variants = vec![first.clone()];
        let mut changed = first.clone();
        changed.operation.step = Some(2);
        variants.push(changed);
        let mut changed = first.clone();
        changed.step = "Complete".into();
        variants.push(changed);
        let mut changed = first.clone();
        changed.operation = rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("elsewhere", "north").unwrap()));
        variants.push(changed);
        let mut changed = first.clone();
        changed.operation = rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("elsewhere.gateway", "south").unwrap()));
        variants.push(changed);
        let mut nested = first.clone();
        nested.operation = rdm_core::OperationId::primary_update(rdm_core::PrimaryRole::FabricPrimary, &first.operation);
        variants.push(nested.clone());
        nested.operation = rdm_core::OperationId::primary_update(rdm_core::PrimaryRole::FabricPrimary, &first.operation.clone().at_step(Some(3)));
        variants.push(nested);
        let keys: std::collections::BTreeSet<_> = variants.iter().map(receipt_key).collect();
        assert_eq!(keys.len(), variants.len());
        let encoded = serde_json::to_value(receipt_key(&first)).unwrap();
        assert_eq!(serde_json::from_value::<ReceiptKey>(encoded).unwrap(), receipt_key(&first));
        assert!(serde_json::from_value::<ReceiptKey>(serde_json::json!("node.create:elsewhere.gateway.north|DeployRuntime")).is_err());
        let indexes: std::collections::BTreeSet<_> = variants.into_iter().map(|r| record_key(&BuildFact::Step(r)).unwrap()).collect();
        assert_eq!(indexes.len(), keys.len());
        let mut same_identity = first.clone();
        same_identity.output = Some(serde_json::json!({"ack": true}));
        assert_eq!(record_key(&BuildFact::Step(first.clone())).unwrap(), record_key(&BuildFact::Step(same_identity)).unwrap());
        let mut other_attempt = first.clone();
        other_attempt.attempt += 1;
        assert_ne!(record_key(&BuildFact::Step(first.clone())).unwrap(), record_key(&BuildFact::Step(other_attempt)).unwrap());
        let mut other_mesh = first.clone();
        other_mesh.mesh = Some("explicit-mesh".into());
        assert_ne!(record_key(&BuildFact::Step(first)).unwrap(), record_key(&BuildFact::Step(other_mesh)).unwrap());
    }

    #[tokio::test]
    async fn a_committed_claim_keeps_its_exact_executor_and_claimant() {
        let builds = MemoryBuildStateAdapter::new();
        let accepted = intent("exact-claim");
        builds.publish_accepted(&accepted).await.unwrap();
        let claim = BuildAttemptClaim {
            claimed_mesh: Some("execution-mesh".into()),
            executor_mesh: "owner-mesh".into(),
            build_id: accepted.build_id.clone(), attempt: 1,
            executor: rdm_core::framing::Birth { node_id: crate::model::NodeId::mint(), incarnation: crate::model::IncarnationId::mint() },
            claimant: Some(crate::model::NodeId::mint()), mesh: None,
        };
        assert_eq!(builds.claim_attempt(&claim).await.unwrap(), ClaimOutcome::Won);
        assert_eq!(builds.claim_attempt(&claim).await.unwrap(), ClaimOutcome::Won);
        let mut another = claim.clone();
        another.executor.incarnation = crate::model::IncarnationId::mint();
        assert_eq!(builds.claim_attempt(&another).await.unwrap(), ClaimOutcome::Lost { holder: claim.executor.node_id.clone() });
        another = claim.clone();
        another.claimant = Some(crate::model::NodeId::mint());
        assert_eq!(builds.claim_attempt(&another).await.unwrap(), ClaimOutcome::Lost { holder: claim.executor.node_id.clone() });
        let held = builds.read_build(&accepted.build_id).await.unwrap();
        assert_eq!(held.executor, Some(claim.executor));
        assert_eq!(held.claimant, claim.claimant);
        assert_eq!(held.attempt_claimed_meshes.get(&1).map(String::as_str), Some("execution-mesh"));
    }

    fn intent(id: &str) -> BuildAccepted {
        BuildAccepted {
            build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(),
            topology: FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()),
            change: None,
            source: None, time: 1,
        }
    }

    fn opened(id: &str, attempt: u32, reason: AttemptReason) -> AttemptOpened {
        AttemptOpened { mesh: None, build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, reason, action: None, opened_by: rdm_core::NodeId::parse("100000000000").unwrap(), opened_at_ms: 5, context: None }
    }

    fn fixture_birth(id: &str) -> rdm_core::framing::Birth {
        rdm_core::framing::Birth {
            node_id: rdm_core::NodeId::parse(id).unwrap(),
            incarnation: rdm_core::IncarnationId::parse("300000000001").unwrap(),
        }
    }

    fn claim(id: &str, attempt: u32, who: &str, owner: &str) -> BuildAttemptClaim {
        BuildAttemptClaim { claimed_mesh: None, executor_mesh: owner.to_owned(), claimant: None, mesh: None, build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, executor: fixture_birth(who) }
    }

    fn step(id: &str, attempt: u32, op: rdm_core::OperationId) -> BuildStepReceipt {
        BuildStepReceipt { mesh: None, build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, operation: op, step: "DeployRuntime".into(), outcome: StepOutcome::Complete, output: None, executor: None }
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
            BuildFact::Claim(claim("b1", 1, "100000000001", "mesh1")),
            BuildFact::Step(step("b1", 1, rdm_core::OperationId::node("node.stop", &rdm_core::NodeId::parse("200000000002").unwrap()))),
            BuildFact::Attempt(attempt("b1", 1, AttemptOutcome::Failed { reason: "executor lost".into() , code: rdm_core::outcome::Code::FailedPrecondition })),
            BuildFact::Claim(claim("b1", 2, "100000000002", "mesh1")),
            BuildFact::Claim(claim("b1", 2, "100000000001", "mesh1")), // loses: attempt 2 already claimed
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
        assert_eq!(b1.executor.as_ref().map(|birth| birth.node_id.as_str()), Some("100000000002"), "the first claim of an attempt wins");
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
        let op = |name: rdm_core::PathName| { let node_id = rdm_core::NodeId::mint(); rdm_core::LifecycleOp { mesh: "mesh1".into(), r#type: "node".into(),
            build_id: rdm_core::fixture_crockford60("b1"),
            attempt: 1,
            operation: rdm_core::OperationId::node("node.delete", &node_id),
            node_id,
            incarnation: rdm_core::IncarnationId::mint(),
            path: name.path.clone(), name: name.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        }};
        let (a, b) = (op(rdm_core::PathName::new("mesh1.node", "1").unwrap()), op(rdm_core::PathName::new("mesh1.node", "2").unwrap()));
        let with = |operation: &rdm_core::OperationId, step: &str, output: &rdm_core::LifecycleOp| {
            BuildFact::Step(BuildStepReceipt {
                mesh: None,
                build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
                attempt: 1,
                operation: operation.clone(),
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
                operation: rdm_core::OperationId::node("node.delete", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())),
                step: "NodeDeleting".into(),
                outcome: StepOutcome::Complete,
                output,
                executor: None,
            })
        };
        let old_shape = serde_json::json!({"build_id": rdm_core::fixture_crockford60("b1"), "attempt": 2, "operation": "node.delete:mesh1.node.1", "event_at_ms": 1});
        let facts = vec![BuildFact::Accepted(intent("b1")), step(Some(old_shape))];
        let read = in_flight_ops(&facts, &fold(&facts));
        assert!(read.ops.is_empty(), "an unreadable step opens no overlay");
        assert_eq!(read.unrecognised.len(), 1, "{read:?}");
        let u = &read.unrecognised[0];
        assert_eq!((u.attempt, &u.operation, u.step.as_str()), (2, &rdm_core::OperationId::node("node.delete", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())), "NodeDeleting"));
        assert!(u.reason.contains("not a LifecycleOp this build reads"), "{}", u.reason);
        let held = [BuildFact::Accepted(intent("b1")), step(None)];
        let none = in_flight_ops(&held, &fold(&held));
        assert_eq!(none.unrecognised[0].reason, "the step recorded no operation");
    }

    #[tokio::test]
    async fn claims_are_insert_and_fail() {
        let m = MemoryBuildStateAdapter::new();
        m.publish_accepted(&intent("b1")).await.unwrap();
        assert_eq!(m.claim_attempt(&claim("b1", 1, "100000000007", "mesh1")).await.unwrap(), ClaimOutcome::Won);
        assert_eq!(m.claim_attempt(&claim("b1", 1, "100000000008", "mesh1")).await.unwrap(), ClaimOutcome::Lost { holder: rdm_core::NodeId::parse("100000000007").unwrap() });
        assert_eq!(m.claim_attempt(&claim("b1", 1, "100000000007", "mesh1")).await.unwrap(), ClaimOutcome::Won, "re-claiming your own attempt is idempotent");
        assert_eq!(m.claim_attempt(&claim("b9", 1, "100000000007", "mesh1")).await, Err(BuildStateError::UnknownBuild(BuildId::parse(&rdm_core::fixture_crockford60("b9")).unwrap())));
        let mut other = intent("b1");
        other.time = 2;
        assert_eq!(m.publish_accepted(&other).await, Err(BuildStateError::ConflictingIntent(BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap())));
        m.publish_accepted(&intent("b1")).await.unwrap();
        assert_eq!(m.list_active().await.unwrap().len(), 1);
    }


    /// A replay of the same executor cannot claim another mesh for one accepted attempt.
    /// Ownership and claimed scope are independent: an admin of mesh1 may execute mesh2's work.
    #[tokio::test]
    async fn a_claim_replay_preserves_its_actual_mesh_without_taking_a_second_row() {
        use crate::storage::MeshStorage;
        let builds = MemoryBuildStateAdapter::new();
        let rows = crate::storage::MemoryMeshStorage::default();
        builds.publish_accepted(&intent("b1")).await.unwrap();
        let first = claim("b1", 1, "100000000006", "mesh1");
        let row = crate::mesh_claims::InFlight { build_id: first.build_id.clone(), lane: None, attempt: 1, claimant: first.executor.node_id.clone(), };
        assert_eq!(builds.claim_in_flight(&first, &rows, "mesh2", &row).await.unwrap(), InFlightClaim::Decided(ClaimOutcome::Won));
        assert_eq!(builds.claim_in_flight(&first, &rows, "mesh2", &row).await.unwrap(), InFlightClaim::Decided(ClaimOutcome::Won));
        assert_eq!(builds.claim_in_flight(&first, &rows, "mesh3", &row).await.unwrap(), InFlightClaim::Decided(ClaimOutcome::Lost { holder: first.executor.node_id.clone() }));
        assert_eq!(rows.in_flight("mesh3").await.unwrap(), None, "the conflicting replay takes no second scope");
        assert_eq!(rows.in_flight("mesh2").await.unwrap(), Some(row));
        let changed_owner = BuildAttemptClaim { executor_mesh: "mesh9".into(), ..first.clone() };
        assert_eq!(builds.claim_in_flight(&changed_owner, &rows, "mesh2", &crate::mesh_claims::InFlight { build_id: first.build_id.clone(), lane: None, attempt: 1, claimant: first.executor.node_id.clone(), }).await.unwrap(), InFlightClaim::Decided(ClaimOutcome::Lost { holder: first.executor.node_id }));
        let accepted = builds.read_build(&first.build_id).await.unwrap();
        assert_eq!(accepted.attempt_executor_meshes.get(&1).map(String::as_str), Some("mesh1"));
        assert_eq!(accepted.attempt_claimed_meshes.get(&1).map(String::as_str), Some("mesh2"));
    }

    #[test]
    fn a_hand_off_chain_arriving_out_of_order_still_reaches_its_converged_attempt() {
        // Gossip delivers facts in any order: a later claim can arrive first.
        let chain = vec![
            BuildFact::Claim(claim("b7", 3, "100000000009", "mesh1")),
            BuildFact::Attempt(attempt("b7", 3, AttemptOutcome::Converged)),
            BuildFact::Attempt(attempt("b7", 2, AttemptOutcome::HandedOff { to: rdm_core::NodeId::parse("100000000009").unwrap() })),
            BuildFact::Claim(claim("b7", 2, "100000000008", "mesh1")),
            BuildFact::Attempt(attempt("b7", 1, AttemptOutcome::HandedOff { to: rdm_core::NodeId::parse("100000000008").unwrap() })),
            BuildFact::Claim(claim("b7", 1, "100000000007", "mesh1")),
            BuildFact::Accepted(intent("b7")),
        ];
        let b = fold(&chain).remove(&BuildId::parse(&rdm_core::fixture_crockford60("b7")).unwrap()).unwrap();
        assert_eq!((b.state, b.attempt, b.executor.as_ref().map(|birth| birth.node_id.as_str())), (BuildState::Complete, 3, Some("100000000009")));
    }

    #[tokio::test]
    async fn an_attempt_is_opened_once_and_reopens_a_complete_build() {
        let m = MemoryBuildStateAdapter::new();
        m.publish_accepted(&intent("b1")).await.unwrap();
        assert_eq!(m.claim_attempt(&claim("b1", 1, "100000000007", "mesh1")).await.unwrap(), ClaimOutcome::Won);
        m.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        assert_eq!(m.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap().state, BuildState::Complete);
        // A claim on a complete Build runs nothing and records nothing: a complete Build runs
        // again only once an attempt is opened on it.
        assert_eq!(m.claim_attempt(&claim("b1", 2, "100000000007", "mesh1")).await.unwrap(), ClaimOutcome::NotOpen { next: None });
        assert_eq!(m.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap().state, BuildState::Complete, "a stray claim does not reopen");
        // Two authorities proving the same drift open one attempt: the first opener wins.
        m.open_attempt(&opened("b1", 2, AttemptReason::Delete)).await.unwrap();
        let mut again = opened("b1", 2, AttemptReason::Delete);
        again.opened_by = rdm_core::NodeId::parse("100000000002").unwrap();
        m.open_attempt(&again).await.unwrap();
        let b = m.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap();
        assert_eq!((b.state, b.attempt, b.reason, b.executor), (BuildState::Pending, 1, AttemptReason::Delete, None));
        assert_eq!(m.facts().await.unwrap().iter().filter(|f| matches!(f, BuildFact::Opened(_))).count(), 1);
        assert_eq!(m.list_active().await.unwrap().len(), 1, "the reopened Build is active again");
        assert_eq!(m.claim_attempt(&claim("b1", 3, "100000000008", "mesh1")).await.unwrap(), ClaimOutcome::NotOpen { next: Some(2) }, "only the next attempt");
        assert_eq!(m.claim_attempt(&claim("b1", 2, "100000000008", "mesh1")).await.unwrap(), ClaimOutcome::Won);
        let b = m.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap();
        assert_eq!((b.state, b.attempt, b.executor.as_ref().map(|birth| birth.node_id.as_str())), (BuildState::Running, 2, Some("100000000008")));
        assert_eq!(m.open_attempt(&opened("b9", 1, AttemptReason::Delete)).await, Err(BuildStateError::UnknownBuild(BuildId::parse(&rdm_core::fixture_crockford60("b9")).unwrap())));
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
        let act = |path: rdm_core::PathName, from: &crate::model::IncarnationId| Some(AttemptAction::Delete { r#type: "node".into(), mesh: "mesh1".into(), path, node_id: node_id.clone(), from_incarnation: from.clone() });
        let mut first = opened("b1", 2, AttemptReason::Delete);
        first.action = act(rdm_core::PathName::new("mesh1.node", "2").unwrap(), &inc);
        m.open_attempt(&first).await.unwrap();
        let mut same = opened("b1", 2, AttemptReason::Delete);
        same.action = act(rdm_core::PathName::new("mesh1.node", "2").unwrap(), &inc);
        same.opened_by = rdm_core::NodeId::parse("100000000003").unwrap();
        m.open_attempt(&same).await.expect("the same action asked again is the same attempt");
        let mut other_node = opened("b1", 2, AttemptReason::Delete);
        other_node.action = Some(AttemptAction::Delete { r#type: "node".into(), mesh: "mesh2".into(), path: rdm_core::PathName::new("mesh2.node", "1").unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: other.clone() });
        let err = m.open_attempt(&other_node).await.expect_err("another action asked for the held number");
        match &err {
            BuildStateError::AttemptTaken { build_id, attempt, held, wanted } => {
                assert_eq!((build_id.to_string(), *attempt), (rdm_core::fixture_crockford60("b1"), 2));
                assert!(held.contains("delete of path \"mesh1.node\", name \"2\"") && held.contains("delete ("), "the refusal names what holds the number: {held}");
                assert!(wanted.contains("delete of path \"mesh2.node\", name \"1\""), "the refusal names what was asked: {wanted}");
            }
            other => panic!("not AttemptTaken: {other:?}"),
        }
        let mut other_birth = opened("b1", 2, AttemptReason::Delete);
        other_birth.action = act(rdm_core::PathName::new("mesh1.node", "2").unwrap(), &other);
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
        m.absorb(&[BuildFact::Accepted(BuildAccepted { source: None, time: 2, ..intent("b2") })]).await;
        assert_eq!(seen.lock().unwrap().last().unwrap(), &[b1.clone(), b2.clone()].into_iter().collect(), "b2's acceptance: b2's own, and b1 still current until it is indexed");
        m.absorb(&[BuildFact::Claim(claim("b2", 1, "100000000001", "mesh1"))]).await;
        assert_eq!(seen.lock().unwrap().last().unwrap(), &[b2.clone()].into_iter().collect(), "a fact of the current Build b2: b2 alone, b1 is neither touched, current nor held");
        held.lock().unwrap().push(BuildId::parse(&b1).unwrap());
        m.absorb(&[BuildFact::Claim(claim("b2", 2, "100000000001", "mesh1"))]).await;
        assert_eq!(seen.lock().unwrap().last().unwrap(), &[b1, b2].into_iter().collect(), "b1 held: read with b2");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reader_of_the_journal_never_waits_behind_a_push_held_mid_derivation() {
        let dir = tmp();
        let j = Arc::new(open_at(&dir).await.unwrap());
        let b1 = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        j.publish_accepted(&intent("b1")).await.unwrap();
        j.claim_attempt(&claim("b1", 1, "100000000001", "mesh1")).await.unwrap();
        let held_op = rdm_core::OperationId::node("node.stop", &rdm_core::NodeId::parse("200000000009").unwrap());
        let derived_op = held_op.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (entered_tx, release_rx) = (Mutex::new(entered_tx), Mutex::new(release_rx));
        j.derive_on_every_fact(FactDerivation::new(Vec::new, move |facts: &[BuildFact]| {
            if facts.iter().any(|f| matches!(f, BuildFact::Step(s) if s.operation == derived_op)) {
                let _ = entered_tx.lock().unwrap().send(());
                let _ = release_rx.lock().unwrap().recv();
            }
        }))
        .await;
        let writer = tokio::spawn({
            let j = j.clone();
            let held_op = held_op.clone();
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
            j.claim_attempt(&claim("b1", 1, "100000000001", "mesh1")).await.unwrap();
            j.append_step_receipt(&step("b1", 1, rdm_core::OperationId::node("node.stop", &rdm_core::NodeId::parse("200000000002").unwrap()))).await.unwrap();
            j.publish_accepted(&intent("b2")).await.unwrap();
            (j.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap(), j.list_active().await.unwrap())
        };
        let j = open_at(&dir).await.unwrap(); // the admin restarted
        assert_eq!(j.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap(), before.0);
        assert_eq!(j.list_active().await.unwrap(), before.1);
        assert_eq!(j.claim_attempt(&claim("b1", 1, "100000000002", "mesh1")).await.unwrap(), ClaimOutcome::Lost { holder: rdm_core::NodeId::parse("100000000001").unwrap() });
        j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        let again = open_at(&dir).await.unwrap();
        assert_eq!(again.read_build(&BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap()).await.unwrap().state, BuildState::Complete);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: an omitted optional executable field in the current structured format
    /// retains its default through journal replay. Required identity fields remain intact.
    #[tokio::test]
    async fn an_admin_replays_a_current_record_without_optional_executable_fields() {
        let dir = tmp();
        {
            let j = open_at(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "100000000001", "mesh1")).await.unwrap();
            j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        }
        // Omit optional fields while preserving all structured identity fields.
        let mut rewritten = 0;
        let file = dir.join(crate::store_root::BUILDS_FILE);
        let mut out = Vec::new();
        for line in std::fs::read_to_string(&file).unwrap().split('\n').filter(|l| !l.is_empty()) {
            let parsed = crate::store_log::read_record_bytes(line.as_bytes()).expect("a whole line");
            let mut text = parsed.record_json.clone();
            if text.contains("\"executable\":null") {
                text = text.replace(",\"executable\":null", "").replace(",\"tags\":[]", "").replace(",\"config\":{}", "");
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
        let rpc = rdm_core::build::Nodes { path: "mesh1.node".into(), name: "1".into(), mesh: "mesh1".into(), r#type: "node".into(), storage_persistence: None, executable: Some(rdm_core::binding::Binding { launch_id: "node".into(), executable: "/b/rpc".into(), sha256: "b".into(), image: None }), tags: Default::default(), config: Default::default() };
        let change = rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: vec![rpc] };
        let observed = crate::test_support::records_of(&current.topology);
        let next = crate::accepted::compile(&current.topology, &change, &observed).expect("the next request compiles over it");
        assert_eq!(next.meshes["mesh1"].nodes.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (build.md §5.3: "The Build facts are the Build journal on the fabric's shared store root
    /// (§5.3), each record at its own key, written by the fabric-primary, so a successor reads them
    /// there"; proposal executable-bindings-at-create.md rule 7, Luke 2026-10-24: "the executable is not
    /// gossiped, it's only in the node storage"): an admin that absorbed an accepted Build from a Build
    /// message, which carries no executable, holds
    /// the accepted Build as the fabric-primary wrote it on the store root, every entry's executable
    /// with it, at the same place in its log. Must NOT happen: the message's copy kept over the stored
    /// record (a successor planning a Build's replacements against entries that name no executable).
    #[tokio::test]
    async fn an_admin_holds_the_accepted_build_as_the_store_root_records_it_not_as_a_build_message_carried_it() {
        let dir = tmp();
        let mut stored = intent("b1");
        let rpc: crate::model::PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let m = stored.topology.meshes.get_mut("mesh1").unwrap();
        m.nodes.insert(rpc.clone());
        let mut spec = rdm_core::meta::NodeSpec { mesh: "mesh1".into(), r#type: "node".into(), path: rpc.path.clone(), name: rpc.name.clone(), storage_persistence: rdm_core::meta::StoragePersistence::Ephemeral, executable: None, tags: Default::default(), config: Default::default() };
        spec.executable = Some(rdm_core::binding::Binding { launch_id: "node".into(), executable: "/b/rpc".into(), sha256: "b".into(), image: None });
        m.set_spec(spec).unwrap();
        let writer = open_at(&dir).await.unwrap();
        writer.publish_accepted(&stored).await.unwrap();
        writer.claim_attempt(&claim("b1", 1, "100000000001", "mesh1")).await.unwrap();

        // Another admin absorbs the Build as a Build message carries it: no executable.
        let other = open_at(&dir).await.unwrap();
        let mut heard = stored.clone();
        for c in heard.topology.meshes.get_mut("mesh1").unwrap().spec_by_path.values_mut() {
            c.executable = None;
        }
        other.absorb_facts(&[BuildFact::Accepted(heard.clone()), BuildFact::Claim(claim("b1", 1, "100000000001", "mesh1"))]).await;
        let id = stored.build_id.clone();
        let read = other.read_build(&id).await.unwrap();
        assert_eq!(read.topology, stored.topology, "the accepted Build as the store root records it, its entries' executables with it");
        assert_eq!((read.attempt, read.executor.as_ref().map(|birth| birth.node_id.as_str())), (1, Some("100000000001")), "the rest of its log as it was");
        assert_eq!(other.current().await.unwrap().map(|c| c.topology), Some(stored.topology.clone()), "the current Build likewise");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn accepted_at(id: &str, at_ms: u64) -> BuildAccepted {
        BuildAccepted { source: None, time: at_ms, ..intent(id) }
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
            s.claim_attempt(&claim("b0", 1, "100000000007", "mesh1")).await.unwrap();
            s.append_attempt_receipt(&attempt("b0", 1, AttemptOutcome::Converged)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b0")));
            s.publish_accepted(&accepted_at("b1", 20)).await.unwrap();
            s.claim_attempt(&claim("b1", 1, "100000000007", "mesh1")).await.unwrap();
            s.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Failed { reason: "x".into() , code: rdm_core::outcome::Code::FailedPrecondition })).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b0")), "the failed b1 is skipped: b0 is the known state");
            assert_eq!(s.newest_accepted_at_ms().await.unwrap(), Some(20), "the index's head is the failed b1, whatever its state");
            // A stop of b0's birth is an attempt of it: in progress while it runs, and b0 stays the known
            // state when it fails.
            s.open_attempt(&opened("b0", 2, AttemptReason::Delete)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b0")));
            s.claim_attempt(&claim("b0", 2, "100000000007", "mesh1")).await.unwrap();
            s.append_attempt_receipt(&attempt("b0", 2, AttemptOutcome::Failed { reason: "stop failed".into() , code: rdm_core::outcome::Code::FailedPrecondition })).await.unwrap();
            assert_eq!(s.read_build(&id("b0")).await.unwrap().state, BuildState::Failed);
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b0")), "an attempt of an unchanged topology that failed never makes an older Build current");
            // The failed b1 resubmitted: its next attempt makes it the Build in progress.
            s.open_attempt(&opened("b1", 2, AttemptReason::Delete)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b1")));
            s.claim_attempt(&claim("b1", 2, "100000000007", "mesh1")).await.unwrap();
            s.append_attempt_receipt(&attempt("b1", 2, AttemptOutcome::Converged)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b1")));
            // A newer Build that failed and was forgotten leaves the views: b1 stays current.
            s.publish_accepted(&accepted_at("b2", 30)).await.unwrap();
            s.claim_attempt(&claim("b2", 1, "100000000007", "mesh1")).await.unwrap();
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
        let newer = [BuildFact::Accepted(accepted_at("bnew", 20)), BuildFact::Claim(claim("bnew", 1, "100000000007", "mesh1")), BuildFact::Attempt(attempt("bnew", 1, AttemptOutcome::Converged))];
        let older = [BuildFact::Accepted(accepted_at("bold", 10)), BuildFact::Claim(claim("bold", 1, "100000000007", "mesh1")), BuildFact::Attempt(attempt("bold", 1, AttemptOutcome::Converged))];
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
        m.absorb(&[BuildFact::Accepted(intent("b1")), BuildFact::Claim(claim("b1", 1, "100000000001", "mesh1")), BuildFact::Attempt(attempt("b1", 1, AttemptOutcome::HandedOff { to: rdm_core::NodeId::parse("100000000002").unwrap() }))]).await;
        let p = m.read_build(&b1).await.unwrap();
        assert_eq!(p.run_of(p.attempt_to_drive()), 1, "the hand-off's next attempt continues run 1");
        m.absorb(&[BuildFact::Claim(claim("b1", 2, "100000000002", "mesh1")), BuildFact::Attempt(attempt("b1", 2, AttemptOutcome::Failed { reason: "successor proven exited after committed transfer".into() , code: rdm_core::outcome::Code::FailedPrecondition }))]).await;
        let p = m.read_build(&b1).await.unwrap();
        assert_eq!((p.state, p.attempt_to_drive(), p.run_of(p.attempt_to_drive())), (BuildState::Failed, 3, 3), "the resubmit's attempt opens run 3");
        m.absorb(&[BuildFact::Claim(claim("b1", 3, "100000000001", "mesh1")), BuildFact::Attempt(attempt("b1", 3, AttemptOutcome::HandedOff { to: rdm_core::NodeId::parse("100000000010").unwrap() }))]).await;
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
            s.claim_attempt(&claim(b, 1, "100000000007", "mesh1")).await.unwrap();
            s.append_attempt_receipt(&attempt(b, 1, outcome)).await.unwrap();
        }
        let held = &s;
        let p = |b: &'static str| async move { held.read_build(&BuildId::parse(&rdm_core::fixture_crockford60(b)).unwrap()).await.unwrap() };
        assert!(drivable(&s, &p("b1").await).await.unwrap(), "the current Build");
        assert!(drivable(&s, &p("b2").await).await.unwrap(), "a failed Build accepted after the current one");
        assert!(!drivable(&s, &p("b0").await).await.unwrap(), "a failed Build older than the current one");
        s.open_attempt(&opened("b1", 2, AttemptReason::Delete)).await.unwrap();
        assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b1")));
        assert!(!drivable(&s, &p("b2").await).await.unwrap(), "one Build in flight: not while the current one is in progress");
    }


    /// CONTRACT (Q28 A: "a mesh-audit ratchet for the store layout"; R-SR1, R-SR3): the store root's
    /// layout, byte for byte: a Build record is one line of `builds.jsonl` at `<build_id>/<key>`
    /// (`build-record/2`, build.md §5.3), its object exactly the protocol's (`len`, `op`, `format`,
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
        let line = crate::store_log::encode_line(&op, "build-record/2", &format!("{b1}/1-result"), record.as_bytes());
        assert_eq!(read(crate::store_root::BUILDS_FILE), String::from_utf8(line).unwrap());
        use crate::storage::MeshStorage as _;
        let claims = crate::storage::FileMeshStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap();
        let row = crate::mesh_claims::InFlight { build_id: BuildId::parse(&b1).unwrap(), lane: None, attempt: 1, claimant: rdm_core::NodeId::parse("100000000001").unwrap(), };
        assert_eq!(claims.take_in_flight("mesh2", &row).await.unwrap(), crate::mesh_claims::Taken::Won);
        let lock = read("locks/in-flight/mesh2");
        let by: serde_json::Value = serde_json::from_str::<serde_json::Value>(&lock).unwrap()["by"].clone();
        assert_eq!(lock, format!(r#"{{"format":"mesh-in-flight/2","record":{{"build_id":"{b1}","attempt":1,"claimant":"100000000001"}},"by":{by}}}"#));
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

    /// CONTRACT: an incompatible journal format is refused, never interpreted as an empty Build.
    #[tokio::test]
    async fn journal_rejects_pre_structured_identity_format() {
        let dir = tmp();
        let journal = open_at(&dir).await.unwrap();
        journal.publish_accepted(&intent("b1")).await.unwrap();
        let file = dir.join(crate::store_root::BUILDS_FILE);
        let mut old = Vec::new();
        for line in std::fs::read_to_string(&file).unwrap().lines().filter(|line| !line.is_empty()) {
            let record = crate::store_log::read_record_bytes(line.as_bytes()).unwrap();
            old.extend(crate::store_log::encode_line(&record.op, "build-record/1", &record.key, record.record_json.as_bytes()));
        }
        std::fs::write(&file, old).unwrap();
        let reopened = open_at(&dir).await.unwrap();
        let error = reopened.list_active().await.unwrap_err();
        assert!(matches!(&error, BuildStateError::Io(reason) if reason.contains("build-record/1") && reason.contains(crate::store_root::BUILDS_FILE)), "{error}");
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

    /// An attempt of `id` opened on `mesh`'s lane: the delete of a birth at `path`.
    fn lane_open(id: &str, mesh: &str, attempt: u32, path: rdm_core::PathName) -> AttemptOpened {
        AttemptOpened {
            mesh: Some(mesh.into()),
            build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(),
            attempt,
            reason: AttemptReason::Delete,
            action: Some(crate::accepted::AttemptAction::Delete { r#type: "node".into(), mesh: mesh.into(), path, node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() }),
            opened_by: rdm_core::NodeId::parse("100000000000").unwrap(),
            opened_at_ms: 6,
            context: None,
        }
    }

    /// A claim of `mesh`'s drift-lane attempt for `who`, decided by `who` (a mesh-primary claims its own).
    fn lane_claim(id: &str, mesh: &str, attempt: u32, who: &str) -> BuildAttemptClaim {
        BuildAttemptClaim { executor_mesh: mesh.to_owned(), claimed_mesh: None, claimant: Some(rdm_core::NodeId::parse(who).unwrap()), mesh: Some(mesh.into()), build_id: BuildId::parse(&rdm_core::fixture_crockford60(id)).unwrap(), attempt, executor: fixture_birth(who) }
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
        m.claim_attempt(&claim("b1", 1, "100000000001", "mesh1")).await.unwrap();
        m.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        m.open_attempt(&lane_open("b1", "mesh1", 1, rdm_core::PathName::new("mesh1.node", "2").unwrap())).await.expect("mesh1's drift lane opens its attempt 1");
        m.open_attempt(&lane_open("b1", "mesh2", 1, rdm_core::PathName::new("mesh2.node", "2").unwrap())).await.expect("mesh2's drift lane opens its own attempt 1");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh1", 1, "100000000001")).await.unwrap(), ClaimOutcome::Won, "mesh1's mesh-primary claims its attempt");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh2", 1, "100000000003")).await.unwrap(), ClaimOutcome::Won, "mesh2's attempt 1 is another key than mesh1's");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh2", 1, "100000000004")).await.unwrap(), ClaimOutcome::Lost { holder: rdm_core::NodeId::parse("100000000003").unwrap() }, "one winner per (build, mesh, attempt)");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh2", 1, "100000000003")).await.unwrap(), ClaimOutcome::Won, "re-claiming your own attempt is idempotent");
        assert_eq!(m.claim_attempt(&lane_claim("b1", "mesh3", 1, "100000000005")).await.unwrap(), ClaimOutcome::NotOpen { next: None }, "a lane with no attempt opened has nothing to claim");
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
        let mut fabric = claim("b1", 1, "100000000001", "mesh1");
        fabric.claimant = Some( rdm_core::NodeId::parse("100000000001").unwrap());
        m.claim_attempt(&fabric).await.unwrap();
        m.open_attempt(&lane_open("b1", "mesh2", 1, rdm_core::PathName::new("mesh2.node", "2").unwrap())).await.unwrap();
        m.claim_attempt(&lane_claim("b1", "mesh2", 1, "100000000003")).await.unwrap();
        let mut drift_step = step("b1", 1, rdm_core::OperationId::node("node.delete", &rdm_core::NodeId::parse("200000000022").unwrap()));
        drift_step.mesh = Some("mesh2".into());
        m.append_step_receipt(&drift_step).await.unwrap();
        let p = m.read_build(&id).await.unwrap();
        assert_eq!((p.state, p.attempt, p.executor.as_ref().map(|birth| birth.node_id.as_str())), (BuildState::Running, 1, Some("100000000001")), "the fabric lane's attempt runs in mesh1: {p:?}");
        assert!(p.steps.is_empty(), "the drift lane's step is not the fabric lane's: {:?}", p.steps);
        let view = serde_json::to_value(&p).unwrap();
        assert_eq!(view["claimant"], "100000000001", "the fabric lane's claimant: {view}");
        let lane = &view["lanes"]["mesh2"];
        assert_eq!((lane["state"].as_str(), lane["attempt"].as_u64(), lane["executor"]["node_id"].as_str(), lane["claimant"].as_str()), (Some("running"), Some(1), Some("100000000003"), Some("100000000003")), "mesh2's drift lane runs its own attempt 1: {view}");
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

    /// CONTRACT: a structured claim without optional mesh and claimant fields decodes
    /// as a fabric attempt; a claim carrying both fields round-trips them.
    #[test]
    fn a_claim_line_written_with_no_mesh_and_no_claimant_decodes_with_neither() {
        let mut record = serde_json::to_value(BuildFact::Claim(claim("b1", 2, "100000000001", "mesh1"))).unwrap();
        record.as_object_mut().unwrap().remove("mesh");
        record.as_object_mut().unwrap().remove("claimant");
        let old: BuildFact = serde_json::from_value(record).unwrap();
        assert!(matches!(&old, BuildFact::Claim(c) if c.mesh.is_none() && c.claimant.is_none()), "{old:?}");
        let new = BuildFact::Claim(lane_claim("b1", "mesh2", 1, "100000000003"));
        assert_eq!(serde_json::from_str::<BuildFact>(&serde_json::to_string(&new).unwrap()).unwrap(), new);
        let fabric = serde_json::to_value(BuildFact::Claim(claim("b1", 2, "100000000001", "mesh1"))).unwrap();
        assert!(fabric.get("mesh").is_none() && fabric.get("claimant").is_none(), "a fabric-lane line is written as before: {fabric}");
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
            j.claim_attempt(&claim("b1", 1, "100000000001", "mesh1")).await.unwrap();
            j.append_step_receipt(&step("b1", 1, rdm_core::OperationId::node("admit-node", &rdm_core::NodeId::parse("100000000001").unwrap()))).await.unwrap();
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

    /// CONTRACT (Q292: an executor, a starting admin and an admin taking a primary role read the named
    /// Build's committed records from the Build storage, never a copy a command carried; the read is
    /// fresh, an unreadable store is an error and a Build the store holds no record of is `UnknownBuild`):
    /// an admin that is not the fabric-primary (its records reader answers none) reads, through
    /// `committed_facts`, every record the fabric-primary committed, a failed step whose reason is far
    /// past one 4,032-byte Build message included, whole; its cached log holds none of them.
    /// Must NOT happen: the read answering the cached log, a fact cut or dropped for its size, or an
    /// absent Build answered as empty.
    #[tokio::test]
    async fn an_admin_that_is_not_the_fabric_primary_reads_a_builds_committed_records_whole() {
        let d = crate::test_support::tempdir("q292-committed");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric_id = crate::model::FabricId::mint();
        let root = crate::store_root::StoreRoot::open(&store, &fabric_id).unwrap();
        let fp = FileJournal::open_on_root(&root, &crate::test_support::tempdir("attempt-context"), &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &writer())).await.unwrap();
        let mp = FileJournal::open_on_root(&root, &crate::test_support::tempdir("attempt-context"), &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &writer())).await.unwrap();
        mp.read_records_while(Arc::new(|| None)).await;
        let id = BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        let long = "x".repeat(9_000);
        fp.publish_accepted(&intent("b1")).await.unwrap();
        fp.claim_attempt(&claim("b1", 1, "100000000001", "mesh1")).await.unwrap();
        let failed = BuildStepReceipt { outcome: StepOutcome::Failed { reason: long.clone(), code: rdm_core::outcome::Code::DeadlineExceeded }, ..step("b1", 1, rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap()))) };
        fp.append_step_receipt(&failed).await.unwrap();

        assert!(mp.facts().await.unwrap().is_empty(), "the reader's cached log holds nothing: it is not the fabric-primary");
        let read = mp.committed_facts(Some(&id)).await.expect("the committed records are read");
        let kinds: Vec<&str> = read.iter().map(|f| match f {
            BuildFact::Accepted(_) => "accepted",
            BuildFact::Claim(_) => "claim",
            BuildFact::Step(_) => "step",
            _ => "other",
        }).collect();
        assert_eq!(kinds, ["accepted", "claim", "step"], "every committed record, in commit order");
        assert!(read.iter().any(|f| matches!(f, BuildFact::Step(s) if s.outcome == failed.outcome)), "the 9,000-byte reason arrives whole");
        assert_eq!(fold(&read).get(&id).map(|p| p.attempt), Some(1), "the read folds to the Build's claimed attempt");

        // A record committed after the first read is in the next one: nothing is cached.
        fp.append_step_receipt(&step("b1", 1, rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "2").unwrap())))).await.unwrap();
        assert_eq!(mp.committed_facts(Some(&id)).await.unwrap().len(), 4, "the next read holds the record committed since");

        let absent = BuildId::parse(&rdm_core::fixture_crockford60("b9")).unwrap();
        assert!(matches!(mp.committed_facts(Some(&absent)).await, Err(BuildStateError::UnknownBuild(b)) if b == absent), "a Build the store holds no record of is named unknown, never empty");
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
        j.claim_attempt(&claim("b1", 1, "100000000001", "mesh1")).await.unwrap();
        j.append_step_receipt(&step("b1", 1, rdm_core::OperationId::node("admit-node", &rdm_core::NodeId::parse("100000000001").unwrap()))).await.unwrap();
        j.append_step_receipt(&step("b1", 1, rdm_core::OperationId::node("admit-node", &rdm_core::NodeId::parse("100000000001").unwrap()))).await.unwrap();
        let b1 = rdm_core::fixture_crockford60("b1");
        assert_eq!(
            record_files(root.dir()),
            [format!("{b1}/1-claim"), format!("{b1}/{}", record_key(&BuildFact::Step(step("b1", 1, rdm_core::OperationId::node("admit-node", &rdm_core::NodeId::parse("100000000001").unwrap())))).unwrap()), format!("{b1}/accepted")],
            "each committed fact is one record at its own key on the store root, and a receipt written again writes nothing"
        );
        assert_eq!(crate::store_log::read_file(&root.dir().join(crate::store_root::BUILDS_FILE)).unwrap().len(), 3, "one line per fact: the receipt written again is not appended");
        j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        j.open_attempt(&lane_open("b1", "mesh2", 1, rdm_core::PathName::new("mesh2.node", "2").unwrap())).await.unwrap();
        assert_eq!(j.claim_attempt(&lane_claim("b1", "mesh2", 1, "100000000003")).await.unwrap(), ClaimOutcome::Won);
        let mut in_mesh = step("b1", 1, rdm_core::OperationId::node("node.delete", &rdm_core::NodeId::parse("200000000022").unwrap()));
        in_mesh.mesh = Some("mesh2".into());
        j.append_step_receipt(&in_mesh).await.unwrap();
        let mut result = attempt("b1", 1, AttemptOutcome::Converged);
        result.mesh = Some("mesh2".into());
        j.append_attempt_receipt(&result).await.unwrap();
        let in_mesh2: Vec<String> = record_files(root.dir()).into_iter().filter(|k| k.starts_with(&format!("{b1}/mesh2-1-"))).collect();
        assert_eq!(in_mesh2, [format!("{b1}/mesh2-1-claim"), format!("{b1}/mesh2-1-opened"), format!("{b1}/mesh2-1-result"), format!("{b1}/{}", record_key(&BuildFact::Step(in_mesh.clone())).unwrap())], "the attempt in mesh2 is keyed (build_id, mesh, attempt)");
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
            let receipt = BuildStepReceipt { outcome: StepOutcome::Failed { reason: "same explanation".into(), code }, ..step("b1", 1, rdm_core::OperationId::node("node.stop", &rdm_core::NodeId::parse("200000000002").unwrap())) };
            {
                let journal = open_at(&dir).await.unwrap();
                journal.publish_accepted(&intent("b1")).await.unwrap();
                journal.claim_attempt(&claim("b1", 1, "100000000001", "mesh1")).await.unwrap();
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
    /// the disk; a writer's turn begun then takes the writer at once and names no primary. Must NOT
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
