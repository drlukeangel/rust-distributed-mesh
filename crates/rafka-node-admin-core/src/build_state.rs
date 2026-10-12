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
//! - [`FileJournal`]: an optional local journal of the same facts for a
//!   same-admin restart and replay evidence. It is never cross-mesh authority:
//!   a successor in another mesh continues a Build from the live projection,
//!   never from a dead admin's disk.

use crate::accepted::{AttemptAction, FabricTopology, TopologyChange};
use crate::build::BuildId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::BufRead;
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
    /// A replacement of a birth that does not answer was requested.
    Replace,
    /// The previous attempt's executor is gone; another admin continues the Build.
    AuthorityMoved,
    /// A standalone drain of an exact birth was requested (`drain-node:<path>`).
    Drain,
    /// A standalone stop of an exact birth was requested (`stop-node:<path>`).
    Stop,
    /// A standalone start of a parked birth was requested (`start-node:<path>`).
    Start,
    /// A mesh-primary proved a member's exact birth exited and submitted its recovery
    /// (`retire-node:<path>`, `create-node:<path>`, `update-node:<path>`, node-recovery.md "Steps").
    Recovery,
    /// The delete of a parked birth at its `.old` path was requested (`delete-node:<path>.old`).
    Delete,
}

impl AttemptReason {
    /// The reason's name as it appears in views and spans.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::ProvenDrift => "proven-drift",
            Self::Restart => "restart",
            Self::Replace => "replace",
            Self::AuthorityMoved => "authority-moved",
            Self::Drain => "drain",
            Self::Stop => "stop",
            Self::Start => "start",
            Self::Recovery => "recovery",
            Self::Delete => "delete",
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
}

/// The receipt of one step of an attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildStepReceipt {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
    /// The operation's idempotency key (`create-node:mesh1.rpc.2`, ...).
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
    },
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
}

impl BuildProjection {
    /// The attempt a drive of this Build covers as it reads now: the current attempt while it runs or
    /// once it completed; the next one while it waits for its claim (an attempt opened on it, a
    /// complete Build reopened included, or the hand-off of the current one) or after it failed.
    pub fn attempt_to_drive(&self) -> u32 {
        match self.state {
            BuildState::Running | BuildState::Complete => self.attempt,
            BuildState::Pending | BuildState::Failed => self.attempt + 1,
        }
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
        build_id = %accepted.build_id,
        previous_build_id = %previous.map(|b| b.0.as_str()).unwrap_or(""),
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
    /// A journal line that does not decode, named with its line number.
    CorruptJournal {
        /// The journal line number.
        line: usize,
        /// Why it does not decode.
        reason: String,
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
    /// nothing from that record's epoch (the fence is sticky for this birth).
    Fenced {
        /// The admin whose Build log is fenced.
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
            Self::CorruptJournal { line, reason } => write!(f, "build journal line {line} does not decode: {reason}"),
            Self::Fenced { node, by, .. } => write!(f, "{node} no longer holds the fabric-primary seat: the committed seat row moved it ({by}); its Build log decides nothing any more"),
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
    let mut winners: BTreeMap<(BuildId, u32), String> = BTreeMap::new();
    let mut opened: std::collections::BTreeSet<(BuildId, u32)> = std::collections::BTreeSet::new();
    // An attempt has one verdict: the first appended. A stale second receipt changes nothing.
    let mut decided: std::collections::BTreeSet<(BuildId, u32)> = std::collections::BTreeSet::new();
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
                    completed_at: None,
                    reported: Default::default(),
                });
            }
            BuildFact::Opened(o) => {
                let key = (o.build_id.clone(), o.attempt);
                if opened.contains(&key) {
                    continue;
                }
                opened.insert(key);
                if let Some(p) = out.get_mut(&o.build_id) {
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
                let key = (c.build_id.clone(), c.attempt);
                if winners.contains_key(&key) {
                    continue;
                }
                winners.insert(key, c.executor.clone());
                if let Some(p) = out.get_mut(&c.build_id) {
                    // The next attempt only, and never of a complete Build: a complete Build runs
                    // again only through an opened attempt.
                    if c.attempt == p.attempt + 1 && !matches!(p.state, BuildState::Complete) {
                        p.attempt = c.attempt;
                        p.executor = Some(c.executor.clone());
                        p.attempt_executors.insert(c.attempt, c.executor.clone());
                        p.state = BuildState::Running;
                        if let Some(a) = &p.action {
                            p.attempt_actions.insert(c.attempt, a.clone());
                        }
                    }
                }
            }
            BuildFact::Step(s) => {
                if let Some(p) = out.get_mut(&s.build_id) {
                    if !p.steps.contains(s) {
                        p.steps.push(s.clone());
                    }
                }
            }
            BuildFact::Attempt(a) => {
                if !decided.insert((a.build_id.clone(), a.attempt)) {
                    continue; // the attempt already has its verdict
                }
                if let Some(p) = out.get_mut(&a.build_id) {
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
                        AttemptOutcome::Failed { reason } => {
                            p.state = BuildState::Failed;
                            p.last_failure = Some(reason.clone());
                        }
                        // No attempt is in flight: the Build waits for the next claim.
                        AttemptOutcome::HandedOff { .. } => p.state = BuildState::Pending,
                    }
                }
            }
            BuildFact::Forget { build_id } => {
                if out.get(build_id).is_some_and(|p| matches!(p.state, BuildState::Complete | BuildState::Failed)) {
                    out.remove(build_id);
                }
            }
        }
    }
    out
}

/// The lifecycle operations open in `builds` (the fold of `facts`): every `NodeDeleting` step a
/// retire operation completed with no `NodeDeleted` step of the same operation after it, and every
/// `NodeRestarting` step whose restart (`restart-node:<path>`, the op it carries) has no `Complete`
/// step yet and whose attempt is not `Converged`. An attempt's `Converged` verdict ends every
/// restart of that attempt: the fabric-primary records it from the executor's stream terminal, which
/// follows every step's `Complete` receipt durable on the executor (node-rpc-envelope.md: progress
/// waits on no gossip), so the restart's own `Complete` need not have reached this log. Derived from
/// the Build facts as each fact enters the log, never stored: a successor primary publishes the same
/// overlays from the same facts.
pub(crate) fn in_flight_ops(facts: &[BuildFact], builds: &BTreeMap<BuildId, BuildProjection>) -> InFlight {
    let mut out = InFlight::default();
    // An attempt has one verdict, the first appended (as `fold` reads it).
    let mut decided: std::collections::BTreeSet<(&BuildId, u32)> = std::collections::BTreeSet::new();
    for f in facts {
        if let BuildFact::Attempt(a) = f {
            if decided.insert((&a.build_id, a.attempt)) && builds.contains_key(&a.build_id) {
                match a.outcome {
                    AttemptOutcome::Converged => {
                        out.attempts_converged.insert((a.build_id.0.clone(), a.attempt));
                    }
                    AttemptOutcome::Failed { .. } => {
                        out.attempts_failed.insert((a.build_id.0.clone(), a.attempt));
                    }
                    _ => {}
                }
            }
        }
    }
    for p in builds.values() {
        for s in &p.steps {
            if s.step == "Complete" && s.outcome == StepOutcome::Complete && s.operation.starts_with("restart-node:") {
                out.restarts_completed.insert((s.build_id.0.clone(), s.operation.clone()));
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
                Some(v) => match serde_json::from_value::<rafka_mesh_entity::LifecycleOp>(v.clone()) {
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
            let closed = if s.step == "NodeDeleting" {
                p.steps.iter().any(|t| t.step == "NodeDeleted" && t.operation == s.operation && t.outcome == StepOutcome::Complete)
                    || out.attempts_converged.contains(&(op.build_id.clone(), op.attempt))
                    || out.attempts_failed.contains(&(op.build_id.clone(), op.attempt))
            } else {
                out.attempts_converged.contains(&(op.build_id.clone(), op.attempt))
                    || p.steps.iter().any(|t| t.step == "Complete" && t.operation == op.operation && t.outcome == StepOutcome::Complete)
            };
            if !closed {
                out.ops.push(op);
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
    pub ops: Vec<rafka_mesh_entity::LifecycleOp>,
    /// Completed steps whose recorded operation cannot be read.
    pub unrecognised: Vec<UnrecognisedStep>,
    /// The restarts whose `restart-node:<path>` `Complete` is receipted, by (Build, operation): the
    /// facts end every hold of that operation in that Build.
    pub restarts_completed: std::collections::BTreeSet<(String, String)>,
    /// The attempts whose verdict is `Converged`, by (Build, attempt): the facts end every restart
    /// hold of that attempt in that Build.
    pub attempts_converged: std::collections::BTreeSet<(String, u32)>,
    /// The attempts whose verdict is `Failed`, by (Build, attempt): the facts end every removal
    /// (`NodeDeleting`) hold of that attempt in that Build; a restart's hold stays open.
    pub attempts_failed: std::collections::BTreeSet<(String, u32)>,
}

/// The Builds the facts forgot: each a `Forget` fact names whose Build the fold no longer holds
/// (`fold` drops a Build on its `Forget` once it is complete or failed). An overlay of such a Build
/// is no longer named by any fact.
pub(crate) fn forgotten_builds(facts: &[BuildFact], builds: &BTreeMap<BuildId, BuildProjection>) -> std::collections::BTreeSet<String> {
    facts
        .iter()
        .filter_map(|f| match f {
            BuildFact::Forget { build_id } if !builds.contains_key(build_id) => Some(build_id.0.clone()),
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
    /// The step in flight of `operation` in `attempt` waits on `reason`: live progress for a call
    /// attached to the run, never a fact, never durable. A state with no call attached to its runs
    /// has nobody to tell.
    async fn note_blocked(&self, _build_id: &BuildId, _attempt: u32, _operation: &str, _step: &str, _reason: &str) {}
    /// The Build, attempt and operation of the node workflow at `path` that deleted the exited
    /// birth `(node_id, incarnation)` and completed: a recovery of that birth (its `retire-node:<path>`
    /// in an attempt the recovery opened, answered at the attempt its last operation,
    /// `update-node:<path>`, completed), or a `replace-node:<path>` or `retire-node:<path>` (the birth
    /// exited inside that workflow, node-recovery.md "Failure and receipts", so it is no drift to
    /// recover). Read from every
    /// Build's step receipts in the log (a forgotten Build's receipts included: forgetting leaves
    /// the views, never the receipts). The first such attempt. A point read by the exited birth:
    /// every adapter answers it from its index (or delegates to the adapter it decorates), never by
    /// reading the fact log.
    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError>;
    /// The Build, attempt and operation of the mesh.delete whose completion receipt
    /// (`mesh_leave::STEP_MESH_DELETED`) names the exact birth `(node_id, incarnation)` its final
    /// primary: the mesh.delete that ended with that birth alive, whose mesh-left follows that birth's
    /// exit proof. The first such attempt. A point read by the birth: every adapter answers it from
    /// its index (or delegates to the adapter it decorates), never by reading the fact log.
    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError>;
    /// The exited births of `mesh` whose node workflow in `build_id` stopped at a Failed attempt and
    /// is not done (node-recovery.md "Failure and receipts": the mesh-primary submits each again):
    /// for each birth an attempt of `build_id` was opened for with `AttemptAction::Recover`, the run
    /// that its latest such open began (up to the next attempt opened in the Build, whatever it
    /// carries) ended with its last claimed attempt's verdict `Failed`, and
    /// [`BuildStateAdapter::recovery_of`] does not answer the birth. A run still in flight (opened and
    /// not claimed, or claimed with no verdict) or handed off is not owed. A point read by the Build:
    /// every adapter answers it from its index (or delegates to the adapter it decorates), never by
    /// reading or folding the fact log.
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError>;
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
const DELETING_WORKFLOWS: [&str; 2] = ["replace-node:", "retire-node:"];

/// The last operation of a node recovery (node-recovery.md "Steps"): node.update of the new node at
/// the exited birth's path.name. A recovery's deletion is answered once it completes.
const RECOVERY_LAST: &str = "update-node:";

/// The path of a deleting workflow's operation `<kind>:<path>`, as text.
fn deleting_workflow_path(operation: &str) -> Option<&str> {
    DELETING_WORKFLOWS.iter().find_map(|kind| operation.strip_prefix(kind))
}

/// The birth a deleting workflow's (`replace-node`, `retire-node`) `NodeDeleted`
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
    if s.step != crate::mesh_leave::STEP_MESH_DELETED || s.outcome != StepOutcome::Complete {
        return None;
    }
    let last = s.output.as_ref()?.get("final_primary")?;
    let node_id = crate::model::NodeId::parse(last.get("node_id")?.as_str()?).ok()?;
    let incarnation = crate::model::IncarnationId(last.get("incarnation_id")?.as_str()?.to_string());
    Some((node_id, incarnation))
}

/// What an opened attempt carries, for a refusal: its reason, its action and who opened it.
fn describe_open(o: &AttemptOpened) -> String {
    let action = match &o.action {
        Some(crate::accepted::AttemptAction::Restart { path, from_incarnation }) => format!("restart of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Replace { path, from_incarnation }) => format!("replace of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Drain { path, from_incarnation }) => format!("drain of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Stop { path, from_incarnation }) => format!("stop of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Start { path, from_incarnation }) => format!("start of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Recover { path, node_id, from_incarnation }) => format!("recovery of {path} from exited {node_id} at {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Delete { path, from_incarnation }) => format!("delete of {path} from {from_incarnation}"),
        None => "no fenced action".to_string(),
    };
    format!("{} ({action}, opened by {})", o.reason.as_str(), o.opened_by)
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
    /// `Opened` answers it as a standalone retire's): every deleting workflow's (`replace-node`, `retire-node`)
    /// `NodeDeleted` by the birth `(node_id, incarnation)` it names, to its path, operation, Build
    /// and attempt...
    recovery_deletes: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), Vec<(crate::model::PathName, String, BuildId, u32)>>,
    /// ...the attempts of each `(Build, operation)` whose `Complete` is receipted, for the deleting
    /// operations and a recovery's last (`update-node`)...
    recovery_completes: std::collections::HashMap<(BuildId, String), std::collections::BTreeSet<u32>>,
    /// ...and the attempts each recovery of a birth was opened at, by that birth, with its Build.
    /// A birth is answered once its `NodeDeleted` and a `Complete` at that attempt or later are both
    /// held: of the recovery's `update-node:<path>` when a recovery of the birth was opened in that
    /// Build at or before the deletion's attempt (its `retire-node` is the recovery's node.delete),
    /// else of the deleting operation itself. Filled by [`FactLog::index`], the one place a fact
    /// enters the log (own appends, absorbed facts and a journal's replay alike); forgetting a Build
    /// leaves all three, as it leaves the receipts.
    recoveries_opened: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), Vec<(BuildId, u32)>>,
    /// The mesh.delete completion index: every `MeshDeleted` receipt by the final primary its
    /// manifest names, to its Build, attempt and operation ([`BuildStateAdapter::mesh_deleted_of`]).
    mesh_deleted: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), Vec<(BuildId, u32, String)>>,
    /// The index of [`BuildStateAdapter::exits_owed`], per Build, filled by [`FactLog::index`] in
    /// whatever order the facts arrive: the exited birth each attempt opened with
    /// `AttemptAction::Recover` names, by attempt (the first open of an attempt)...
    exit_opens: std::collections::HashMap<BuildId, BTreeMap<u32, (crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId)>>,
    /// ...every attempt opened, whatever it carries (the next one ends the run of the one before)...
    opens: std::collections::HashMap<BuildId, std::collections::BTreeSet<u32>>,
    /// ...and every attempt claimed or decided, with its verdict once one is held: the first
    /// appended, as [`fold`] reads it.
    verdicts: std::collections::HashMap<BuildId, BTreeMap<u32, Option<AttemptOutcome>>>,
    /// The acceptance-order index (R-BI3): every Build accepted, by its own acceptance stamp
    /// `(submitted_at_ms, build_id)`, entered once by its `Accepted` fact and never rewritten, in
    /// whatever order the facts arrive. Read newest first by [`current_build`].
    accepted_order: std::collections::BTreeSet<(u64, BuildId)>,
    /// Each Build's own facts, by their position in `facts`: a Build is read by folding its facts
    /// alone ([`read_own`]).
    by_build: std::collections::HashMap<BuildId, Vec<usize>>,
}

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
            if let Some((path, node_id, incarnation)) = recovery_deleted(s) {
                let held = self.recovery_deletes.entry((node_id, incarnation)).or_default();
                if !held.iter().any(|(p, o, b, a)| *p == path && *o == s.operation && *b == s.build_id && *a == s.attempt) {
                    held.push((path, s.operation.clone(), s.build_id.clone(), s.attempt));
                }
            } else if recovery_completed(s) {
                self.recovery_completes.entry((s.build_id.clone(), s.operation.clone())).or_default().insert(s.attempt);
            } else if let Some(birth) = mesh_deleted_final(s) {
                let held = self.mesh_deleted.entry(birth).or_default();
                if !held.iter().any(|(b, a, o)| *b == s.build_id && *a == s.attempt && *o == s.operation) {
                    held.push((s.build_id.clone(), s.attempt, s.operation.clone()));
                }
            }
        }
        if let BuildFact::Opened(AttemptOpened { build_id, attempt, action: Some(crate::accepted::AttemptAction::Recover { path, node_id, from_incarnation }), .. }) = fact {
            let opened = self.recoveries_opened.entry((node_id.clone(), from_incarnation.clone())).or_default();
            if !opened.contains(&(build_id.clone(), *attempt)) {
                opened.push((build_id.clone(), *attempt));
            }
            self.exit_opens.entry(build_id.clone()).or_default().entry(*attempt).or_insert_with(|| (path.clone(), node_id.clone(), from_incarnation.clone()));
        }
        match fact {
            BuildFact::Opened(o) => {
                self.opens.entry(o.build_id.clone()).or_default().insert(o.attempt);
            }
            BuildFact::Claim(c) => {
                self.verdicts.entry(c.build_id.clone()).or_default().entry(c.attempt).or_insert(None);
            }
            BuildFact::Attempt(a) => {
                let held = self.verdicts.entry(a.build_id.clone()).or_default().entry(a.attempt).or_insert(None);
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
    /// ends its workflow: a recovery's `update-node:<path>` when a recovery of the birth was opened in
    /// that Build at or before the deletion's attempt, else the deleting operation itself. The first
    /// such attempt, with the deleting operation.
    fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Option<(BuildId, u32, String)> {
        let birth = (node_id.clone(), incarnation.clone());
        let opened = self.recoveries_opened.get(&birth);
        self.recovery_deletes
            .get(&birth)?
            .iter()
            .filter(|(p, _, _, _)| p == path)
            .filter_map(|(_, operation, build, deleted)| {
                let recovering = opened.is_some_and(|o| o.iter().any(|(b, at)| b == build && at <= deleted));
                let ends = if recovering { format!("{RECOVERY_LAST}{path}") } else { operation.clone() };
                let completed = self.recovery_completes.get(&(build.clone(), ends))?.range(*deleted..).next()?;
                Some((build.clone(), *completed, operation.clone()))
            })
            .min_by_key(|(_, a, _)| *a)
    }

    /// The point read of [`BuildStateAdapter::mesh_deleted_of`]: the first attempt's.
    fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Option<(BuildId, u32, String)> {
        self.mesh_deleted.get(&(node_id.clone(), incarnation.clone()))?.iter().min_by_key(|(_, a, _)| *a).cloned()
    }

    /// The point read of [`BuildStateAdapter::exits_owed`]: per exited birth of `mesh`, the run its
    /// latest `Recover` open in `build_id` began, up to the next attempt opened in the Build; owed when
    /// the run's last claimed attempt holds the verdict `Failed` and [`FactLog::recovery_of`] does not
    /// answer the birth.
    fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Vec<OwedExit> {
        let Some(exit_opens) = self.exit_opens.get(build_id) else { return Vec::new() };
        // The latest open per birth: the attempts ascend, so the last one seen stands.
        let mut latest: std::collections::HashMap<(crate::model::NodeId, crate::model::IncarnationId), (u32, crate::model::PathName)> = std::collections::HashMap::new();
        for (attempt, (path, node_id, incarnation)) in exit_opens {
            if path.mesh == mesh {
                latest.insert((node_id.clone(), incarnation.clone()), (*attempt, path.clone()));
            }
        }
        let mut owed: Vec<OwedExit> = latest
            .into_iter()
            .filter_map(|((node_id, incarnation), (from, path))| {
                let until = self.opens.get(build_id).and_then(|o| o.range(from + 1..).next().copied()).unwrap_or(u32::MAX);
                let (attempt, verdict) = self.verdicts.get(build_id)?.range(from..until).next_back()?;
                let Some(AttemptOutcome::Failed { reason }) = verdict else { return None };
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

/// A claim that is not the Build's next attempt, or is on a complete Build: refused by name,
/// never recorded (the fold would ignore it, but the attempt number stays free).
fn not_open(facts: &[BuildFact], claim: &BuildAttemptClaim) -> Option<ClaimOutcome> {
    let p = fold(facts).remove(&claim.build_id)?;
    if matches!(p.state, BuildState::Complete) || claim.attempt != p.attempt + 1 {
        return Some(ClaimOutcome::NotOpen { next: (!matches!(p.state, BuildState::Complete)).then_some(p.attempt + 1) });
    }
    None
}

/// Whether `f` adds anything to `facts`: acceptances, opens and claims are insert-and-fail on
/// their key, everything else deduplicates.
fn is_new(facts: &[BuildFact], f: &BuildFact) -> bool {
    match f {
        BuildFact::Accepted(i) => !known(facts, &i.build_id),
        BuildFact::Opened(o) => opened(facts, &o.build_id, o.attempt).is_none(),
        BuildFact::Claim(c) => claim_holder(facts, &c.build_id, c.attempt).is_none(),
        _ => !facts.contains(f),
    }
}

fn opened<'a>(facts: &'a [BuildFact], build_id: &BuildId, attempt: u32) -> Option<&'a AttemptOpened> {
    facts.iter().find_map(|f| match f {
        BuildFact::Opened(o) if &o.build_id == build_id && o.attempt == attempt => Some(o),
        _ => None,
    })
}

/// Whether `wanted` may be recorded: `Ok(true)` the number is free, `Ok(false)` it is held for the
/// same action (nothing to record), `Err(AttemptTaken)` it is held for another.
fn admit_open(facts: &[BuildFact], wanted: &AttemptOpened) -> Result<bool, BuildStateError> {
    match opened(facts, &wanted.build_id, wanted.attempt) {
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

fn claim_holder(facts: &[BuildFact], build_id: &BuildId, attempt: u32) -> Option<String> {
    facts.iter().find_map(|f| match f {
        BuildFact::Claim(c) if &c.build_id == build_id && c.attempt == attempt => Some(c.executor.clone()),
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
        let mut w = self.writer.lock().await;
        derive(&snapshot(&self.log));
        w.0 = Some(derive);
    }

    async fn absorb_facts(&self, facts: &[BuildFact]) {
        let w = self.writer.lock().await;
        for f in facts {
            if is_new(&snapshot(&self.log), f) {
                if let Err(e) = self.append(&w, f.clone()).await {
                    tracing::info_span!("rdm.node_admin.build.reject.via-journal-unwritten", path = %self.path.display(), error = %e)
                        .in_scope(|| tracing::info!("a Build fact heard from the fabric could not be journaled"));
                }
            }
        }
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
        if let Some(holder) = claim_holder(&held, &claim.build_id, claim.attempt) {
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

    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        Ok(self.log.lock().unwrap().recovery_of(path, node_id, incarnation))
    }

    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        Ok(self.log.lock().unwrap().mesh_deleted_of(node_id, incarnation))
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

/// The journal file name inside an admin's own data dir.
pub(crate) const JOURNAL_FILE: &str = "build-journal.jsonl";

/// `builds.storage`: the same facts, one JSON line each, appended and fsynced in the admin's own
/// data dir before the fact counts. Re-opening it replays to the same view, so a Build accepted
/// before an all-admin restart is still held after it.
#[derive(Debug)]
pub(crate) struct FileJournal {
    path: PathBuf,
    /// The replayed facts and their indexes. Held only for a snapshot, a point read or a
    /// publication's swap, never across the disk, a fold or the derivation.
    log: Mutex<FactLog>,
    /// The writer's turn: decide on a snapshot, append + fsync on the blocking pool, then publish,
    /// holding the derivation each publication runs. An async lock, so a writer waiting on another's
    /// fsync or derivation yields its worker; readers never take it.
    writer: tokio::sync::Mutex<Derived>,
}

impl FileJournal {
    /// Open (or create) the journal in `own_data_dir` and replay it: the directory, the file and
    /// every line read on the blocking pool, as every append is, so no worker thread waits on the
    /// disk. A journal never written replays to no facts; one that cannot be opened is an error.
    pub async fn open(own_data_dir: &Path) -> Result<Self, BuildStateError> {
        let path = own_data_dir.join(JOURNAL_FILE);
        let (dir, file) = (own_data_dir.to_path_buf(), path.clone());
        let log = crate::record_store::blocking_io(move || {
            std::fs::create_dir_all(&dir).map_err(|e| BuildStateError::Io(format!("{}: {e}", dir.display())))?;
            let mut log = FactLog::default();
            let f = match std::fs::File::open(&file) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(log),
                Err(e) => return Err(BuildStateError::Io(format!("{}: {e}", file.display()))),
            };
            let mut facts = Vec::new();
            for (i, line) in std::io::BufReader::new(f).lines().enumerate() {
                let line = line.map_err(|e| BuildStateError::Io(format!("{}: {e}", file.display())))?;
                if line.trim().is_empty() {
                    continue;
                }
                let fact: BuildFact = serde_json::from_str(&line)
                    .map_err(|e| BuildStateError::CorruptJournal { line: i + 1, reason: e.to_string() })?;
                log.index(&fact, facts.len());
                facts.push(fact);
            }
            log.facts = Arc::new(facts);
            Ok(log)
        })
        .await?;
        Ok(Self { path, log: Mutex::new(log), writer: tokio::sync::Mutex::new(Derived::default()) })
    }

    /// Append `fact` as one fsynced line, then publish it ([`publish`]). The caller holds the
    /// writer's turn `w`.
    async fn append(&self, w: &Derived, fact: BuildFact) -> Result<(), BuildStateError> {
        use std::io::Write as _;
        let line = serde_json::to_string(&fact).map_err(|e| BuildStateError::Io(e.to_string()))?;
        let path = self.path.clone();
        crate::record_store::blocking_io(move || {
            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path).map_err(|e| BuildStateError::Io(e.to_string()))?;
            writeln!(f, "{line}").and_then(|_| f.sync_data()).map_err(|e| BuildStateError::Io(e.to_string()))
        })
        .await?;
        publish(&self.log, w.0.as_ref(), vec![fact]);
        Ok(())
    }
}

#[async_trait]
impl BuildStateAdapter for FileJournal {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        if !accepted_conflict(&snapshot(&self.log), accepted)? {
            self.append(&w, BuildFact::Accepted(accepted.clone())).await?;
        }
        Ok(())
    }

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        let free = {
            let held = snapshot(&self.log);
            if !known(&held, &opened.build_id) {
                return Err(BuildStateError::UnknownBuild(opened.build_id.clone()));
            }
            admit_open(&held, opened)?
        };
        if free {
            self.append(&w, BuildFact::Opened(opened.clone())).await?;
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
        {
            let held = snapshot(&self.log);
            if !known(&held, &claim.build_id) {
                return Err(BuildStateError::UnknownBuild(claim.build_id.clone()));
            }
            if let Some(holder) = claim_holder(&held, &claim.build_id, claim.attempt) {
                return Ok(if holder == claim.executor { ClaimOutcome::Won } else { ClaimOutcome::Lost { holder } });
            }
            if let Some(not_open) = not_open(&held, claim) {
                return Ok(not_open);
            }
        }
        self.append(&w, BuildFact::Claim(claim.clone())).await?;
        Ok(ClaimOutcome::Won)
    }

    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        if is_new(&snapshot(&self.log), &BuildFact::Claim(claim.clone())) {
            self.append(&w, BuildFact::Claim(claim.clone())).await?;
        }
        Ok(())
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.append(&w, BuildFact::Step(receipt.clone())).await
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.append(&w, BuildFact::Attempt(receipt.clone())).await
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        Ok(snapshot(&self.log).as_ref().clone())
    }

    async fn recovery_of(&self, path: &crate::model::PathName, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        Ok(self.log.lock().unwrap().recovery_of(path, node_id, incarnation))
    }

    async fn mesh_deleted_of(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        Ok(self.log.lock().unwrap().mesh_deleted_of(node_id, incarnation))
    }

    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<OwedExit>, BuildStateError> {
        Ok(self.log.lock().unwrap().exits_owed(build_id, mesh))
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let w = self.writer.lock().await;
        self.append(&w, BuildFact::Forget { build_id: build_id.clone() }).await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_step_line_written_before_receipts_carried_output_still_decodes() {
        let old = r#"{"fact":"step","build_id":"b1","attempt":1,"operation":"create-node:mesh1.rpc.1","step":"DeployRuntime","outcome":"complete"}"#;
        let fact: BuildFact = serde_json::from_str(old).unwrap();
        assert!(matches!(fact, BuildFact::Step(BuildStepReceipt { output: None, .. })));
        let with = BuildStepReceipt { output: Some(serde_json::json!({"pid": 7})), ..step("b1", 1, "create-node:mesh1.rpc.1") };
        let back: BuildStepReceipt = serde_json::from_str(&serde_json::to_string(&with).unwrap()).unwrap();
        assert_eq!(back, with);
    }

    use super::*;

    fn intent(id: &str) -> BuildAccepted {
        BuildAccepted {
            build_id: BuildId(id.into()),
            topology: FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()),
            submitted_change: None,
            submitted_at_ms: 1,
        }
    }

    fn opened(id: &str, attempt: u32, reason: AttemptReason) -> AttemptOpened {
        AttemptOpened { build_id: BuildId(id.into()), attempt, reason, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 5 }
    }

    fn claim(id: &str, attempt: u32, who: &str) -> BuildAttemptClaim {
        BuildAttemptClaim { build_id: BuildId(id.into()), attempt, executor: who.into() }
    }

    fn step(id: &str, attempt: u32, op: &str) -> BuildStepReceipt {
        BuildStepReceipt { build_id: BuildId(id.into()), attempt, operation: op.into(), step: "DeployRuntime".into(), outcome: StepOutcome::Complete, output: None, executor: None }
    }

    fn attempt(id: &str, attempt: u32, outcome: AttemptOutcome) -> BuildAttemptReceipt {
        BuildAttemptReceipt { build_id: BuildId(id.into()), attempt, outcome }
    }

    fn log() -> Vec<BuildFact> {
        vec![
            BuildFact::Accepted(intent("b1")),
            BuildFact::Claim(claim("b1", 1, "mesh1.admin.1")),
            BuildFact::Step(step("b1", 1, "restart-node:mesh1.rpc.2")),
            BuildFact::Attempt(attempt("b1", 1, AttemptOutcome::Failed { reason: "executor lost".into() })),
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
        let b1 = &a[&BuildId("b1".into())];
        assert_eq!(b1.state, BuildState::Complete);
        assert_eq!(b1.attempt, 2);
        assert_eq!(b1.executor.as_deref(), Some("mesh1.admin.2"), "the first claim of an attempt wins");
        assert_eq!(b1.steps.len(), 1);
        assert_eq!(a[&BuildId("b2".into())].state, BuildState::Pending);
        // Duplicating every fact changes nothing.
        let doubled: Vec<BuildFact> = log().into_iter().flat_map(|f| [f.clone(), f]).collect();
        assert_eq!(fold(&doubled), a);
    }

    #[test]
    fn a_failed_attempt_leaves_the_build_failed_until_a_later_claim() {
        let v = fold(&log()[..4]);
        let b1 = &v[&BuildId("b1".into())];
        assert_eq!(b1.state, BuildState::Failed);
        assert_eq!(b1.last_failure.as_deref(), Some("executor lost"));
        let v = fold(&log()[..5]);
        assert_eq!(v[&BuildId("b1".into())].state, BuildState::Running, "a successor's claim resumes the same build id");
    }

    #[test]
    fn open_retire_and_restart_overlays_are_derived_from_the_build_facts() {
        let op = |node: &str| rafka_mesh_entity::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: format!("retire-node:{node}"),
            node_id: rafka_mesh_entity::NodeId::mint(),
            incarnation: rafka_mesh_entity::IncarnationId::mint(),
            name: node.parse().unwrap(),
            event_at_rafka_ms: 1,
        };
        let (a, b) = (op("mesh1.rpc.1"), op("mesh1.rpc.2"));
        let with = |operation: &str, step: &str, output: &rafka_mesh_entity::LifecycleOp| {
            BuildFact::Step(BuildStepReceipt {
                build_id: BuildId("b1".into()),
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
            with(&b.operation, "DrainNode", &b),
        ];
        let open = in_flight_ops(&facts, &fold(&facts)).ops;
        assert_eq!(open, vec![b.clone()], "a retire with its pre-notice and no departure is open; a completed one is not");

        // A restart is one operation (`restart-node:<path>`): its pre-event opens it, the stop of the
        // live process leaves it open, and only its `Complete`, after the start, closes it, whoever
        // reads the facts.
        let c = rafka_mesh_entity::LifecycleOp { operation: "restart-node:mesh1.rpc.3".into(), name: "mesh1.rpc.3".parse().unwrap(), ..op("mesh1.rpc.3") };
        let mut facts = facts;
        facts.push(with("restart-node:mesh1.rpc.3", "NodeRestarting", &c));
        facts.push(with("restart-node:mesh1.rpc.3", "AwaitNodeLeft", &c));
        assert_eq!(in_flight_ops(&facts, &fold(&facts)).ops, vec![b.clone(), c.clone()], "a successor holds the restart from the Build facts alone");
        facts.push(with("restart-node:mesh1.rpc.3", "Complete", &c));
        assert_eq!(in_flight_ops(&facts, &fold(&facts)).ops, vec![b], "the restart's Complete closes it");
        assert!(in_flight_ops(&facts, &fold(&facts)).unrecognised.is_empty());
        assert_eq!(
            in_flight_ops(&facts, &fold(&facts)).restarts_completed,
            [("b1".to_string(), "restart-node:mesh1.rpc.3".to_string())].into(),
            "the restart's Complete is named by Build and operation: it ends that restart's hold and no other"
        );

        // A Build is forgotten once the fold drops it on its `Forget`; a `Forget` of a Build still in
        // flight leaves it held, so it names nothing forgotten.
        facts.push(BuildFact::Forget { build_id: BuildId("b1".into()) });
        facts.push(BuildFact::Forget { build_id: BuildId("b9".into()) });
        assert_eq!(forgotten_builds(&facts, &fold(&facts)), ["b9".to_string()].into(), "only a Build the fold no longer holds is forgotten");
    }

    /// CONTRACT (node-rpc-envelope.md: "Progress waits on no gossip"): a restart is open until its
    /// `restart-node:<path>` `Complete` is receipted or its attempt's verdict is `Converged` (the
    /// fabric-primary's own fact from the stream terminal); an attempt's first verdict is its one, so
    /// a `Converged` after a `Failed` ends nothing, and a verdict ends only its own attempt's restarts.
    #[test]
    fn a_restart_is_closed_by_its_attempts_converged_verdict_and_by_no_other_attempts() {
        let restart = |node: &str, attempt: u32| rafka_mesh_entity::LifecycleOp {
            build_id: "b1".into(),
            attempt,
            operation: format!("restart-node:{node}"),
            node_id: rafka_mesh_entity::NodeId::mint(),
            incarnation: rafka_mesh_entity::IncarnationId::mint(),
            name: node.parse().unwrap(),
            event_at_rafka_ms: 1,
        };
        let pre_event = |op: &rafka_mesh_entity::LifecycleOp| {
            BuildFact::Step(BuildStepReceipt {
                build_id: BuildId("b1".into()),
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
        facts.push(BuildFact::Attempt(attempt("b1", 2, AttemptOutcome::Failed { reason: "lost".into() })));
        facts.push(BuildFact::Attempt(attempt("b1", 2, AttemptOutcome::Converged)));
        facts.push(BuildFact::Attempt(attempt("b1", 3, AttemptOutcome::Converged)));
        let read = in_flight_ops(&facts, &fold(&facts));
        assert_eq!(read.ops, vec![failed, later], "attempt 3's Converged ends its restart; attempt 2's first verdict is Failed and attempt 4 has none");
        assert_eq!(read.attempts_converged, [("b1".to_string(), 3)].into(), "the converged attempts are named by Build and attempt");
    }

    /// CONTRACT: a completed pre-event step whose recorded operation this build cannot read (a row
    /// another build wrote, a field it renamed) is refused by name (build, attempt, operation, step,
    /// why) and opens no overlay; it is never skipped as if it carried nothing.
    #[test]
    fn a_pre_event_step_this_build_cannot_read_is_refused_by_name() {
        let step = |output: Option<serde_json::Value>| {
            BuildFact::Step(BuildStepReceipt {
                build_id: BuildId("b1".into()),
                attempt: 2,
                operation: "retire-node:mesh1.rpc.1".into(),
                step: "NodeDeleting".into(),
                outcome: StepOutcome::Complete,
                output,
                executor: None,
            })
        };
        let old_shape = serde_json::json!({"build_id": "b1", "attempt": 2, "operation": "retire-node:mesh1.rpc.1", "event_at_ms": 1});
        let facts = vec![BuildFact::Accepted(intent("b1")), step(Some(old_shape))];
        let read = in_flight_ops(&facts, &fold(&facts));
        assert!(read.ops.is_empty(), "an unreadable step opens no overlay");
        assert_eq!(read.unrecognised.len(), 1, "{read:?}");
        let u = &read.unrecognised[0];
        assert_eq!((u.attempt, u.operation.as_str(), u.step.as_str()), (2, "retire-node:mesh1.rpc.1", "NodeDeleting"));
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
        assert_eq!(m.claim_attempt(&claim("b9", 1, "a")).await, Err(BuildStateError::UnknownBuild(BuildId("b9".into()))));
        let mut other = intent("b1");
        other.submitted_at_ms = 2;
        assert_eq!(m.publish_accepted(&other).await, Err(BuildStateError::ConflictingIntent(BuildId("b1".into()))));
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
        let b = fold(&chain).remove(&BuildId("b7".into())).unwrap();
        assert_eq!((b.state, b.attempt, b.executor.as_deref()), (BuildState::Complete, 3, Some("c")));
    }

    #[tokio::test]
    async fn an_attempt_is_opened_once_and_reopens_a_complete_build() {
        let m = MemoryBuildStateAdapter::new();
        m.publish_accepted(&intent("b1")).await.unwrap();
        assert_eq!(m.claim_attempt(&claim("b1", 1, "a")).await.unwrap(), ClaimOutcome::Won);
        m.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        assert_eq!(m.read_build(&BuildId("b1".into())).await.unwrap().state, BuildState::Complete);
        // A claim on a complete Build runs nothing and records nothing: a complete Build runs
        // again only once an attempt is opened on it.
        assert_eq!(m.claim_attempt(&claim("b1", 2, "a")).await.unwrap(), ClaimOutcome::NotOpen { next: None });
        assert_eq!(m.read_build(&BuildId("b1".into())).await.unwrap().state, BuildState::Complete, "a stray claim does not reopen");
        // Two authorities proving the same drift open one attempt: the first opener wins.
        m.open_attempt(&opened("b1", 2, AttemptReason::ProvenDrift)).await.unwrap();
        let mut again = opened("b1", 2, AttemptReason::Restart);
        again.opened_by = "mesh1.admin.2".into();
        m.open_attempt(&again).await.unwrap();
        let b = m.read_build(&BuildId("b1".into())).await.unwrap();
        assert_eq!((b.state, b.attempt, b.reason, b.executor), (BuildState::Pending, 1, AttemptReason::ProvenDrift, None));
        assert_eq!(m.facts().await.unwrap().iter().filter(|f| matches!(f, BuildFact::Opened(_))).count(), 1);
        assert_eq!(m.list_active().await.unwrap().len(), 1, "the reopened Build is active again");
        assert_eq!(m.claim_attempt(&claim("b1", 3, "b")).await.unwrap(), ClaimOutcome::NotOpen { next: Some(2) }, "only the next attempt");
        assert_eq!(m.claim_attempt(&claim("b1", 2, "b")).await.unwrap(), ClaimOutcome::Won);
        let b = m.read_build(&BuildId("b1".into())).await.unwrap();
        assert_eq!((b.state, b.attempt, b.executor.as_deref()), (BuildState::Running, 2, Some("b")));
        assert_eq!(m.open_attempt(&opened("b9", 1, AttemptReason::Restart)).await, Err(BuildStateError::UnknownBuild(BuildId("b9".into()))));
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
        let act = |path: &str, from: &crate::model::IncarnationId| Some(AttemptAction::Replace { path: path.parse().unwrap(), from_incarnation: from.clone() });
        let mut first = opened("b1", 2, AttemptReason::ProvenDrift);
        first.action = act("mesh1.rpc.2", &inc);
        m.open_attempt(&first).await.unwrap();
        let mut same = opened("b1", 2, AttemptReason::Replace);
        same.action = act("mesh1.rpc.2", &inc);
        same.opened_by = "mesh2.admin.1".into();
        m.open_attempt(&same).await.expect("the same action asked again is the same attempt");
        let mut other_node = opened("b1", 2, AttemptReason::Restart);
        other_node.action = Some(AttemptAction::Restart { path: "mesh2.rpc.1".parse().unwrap(), from_incarnation: other.clone() });
        let err = m.open_attempt(&other_node).await.expect_err("another action asked for the held number");
        match &err {
            BuildStateError::AttemptTaken { build_id, attempt, held, wanted } => {
                assert_eq!((build_id.0.as_str(), *attempt), ("b1", 2));
                assert!(held.contains("replace of mesh1.rpc.2") && held.contains("proven-drift"), "the refusal names what holds the number: {held}");
                assert!(wanted.contains("restart of mesh2.rpc.1"), "the refusal names what was asked: {wanted}");
            }
            other => panic!("not AttemptTaken: {other:?}"),
        }
        let mut other_birth = opened("b1", 2, AttemptReason::Replace);
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
        let j = Arc::new(FileJournal::open(&dir).await.unwrap());
        let b1 = BuildId("b1".into());
        j.publish_accepted(&intent("b1")).await.unwrap();
        j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
        let held_op = "restart-node:mesh1.rpc.9";
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
            let j = FileJournal::open(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
            j.append_step_receipt(&step("b1", 1, "restart-node:mesh1.rpc.2")).await.unwrap();
            j.publish_accepted(&intent("b2")).await.unwrap();
            (j.read_build(&BuildId("b1".into())).await.unwrap(), j.list_active().await.unwrap())
        };
        let j = FileJournal::open(&dir).await.unwrap(); // the admin restarted
        assert_eq!(j.read_build(&BuildId("b1".into())).await.unwrap(), before.0);
        assert_eq!(j.list_active().await.unwrap(), before.1);
        assert_eq!(j.claim_attempt(&claim("b1", 1, "mesh1.admin.2")).await.unwrap(), ClaimOutcome::Lost { holder: "mesh1.admin.1".into() });
        j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        let again = FileJournal::open(&dir).await.unwrap();
        assert_eq!(again.read_build(&BuildId("b1".into())).await.unwrap().state, BuildState::Complete);
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
        let journal = FileJournal::open(&dir).await.unwrap();
        for s in [&MemoryBuildStateAdapter::new() as &dyn BuildStateAdapter, &journal] {
            let id = |b: &str| BuildId(b.into());
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::None, "an empty store holds no current Build");
            s.publish_accepted(&accepted_at("b0", 10)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b0")), "an accepted Build is current, in progress");
            s.claim_attempt(&claim("b0", 1, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt("b0", 1, AttemptOutcome::Converged)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b0")));
            s.publish_accepted(&accepted_at("b1", 20)).await.unwrap();
            s.claim_attempt(&claim("b1", 1, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Failed { reason: "x".into() })).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b0")), "the failed b1 is skipped: b0 is the known state");
            assert_eq!(s.newest_accepted_at_ms().await.unwrap(), Some(20), "the index's head is the failed b1, whatever its state");
            // A restart of b0 is an attempt of it: in progress while it runs, and b0 stays the known
            // state when it fails.
            s.open_attempt(&opened("b0", 2, AttemptReason::Restart)).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b0")));
            s.claim_attempt(&claim("b0", 2, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt("b0", 2, AttemptOutcome::Failed { reason: "restart failed".into() })).await.unwrap();
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
            s.append_attempt_receipt(&attempt("b2", 1, AttemptOutcome::Failed { reason: "y".into() })).await.unwrap();
            s.forget(&id("b2")).await.unwrap();
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(id("b1")));
        }
        let replayed = FileJournal::open(&dir).await.unwrap(); // the admin restarted
        assert_eq!(replayed.current_build().await.unwrap(), CurrentBuild::Completed(BuildId("b1".into())), "the journal's replay answers the same current Build");
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
            assert_eq!(s.current_build().await.unwrap(), CurrentBuild::Completed(BuildId("bnew".into())));
        }
    }

    /// CONTRACT (R-BI3: "a caller resubmits it"; one Build in flight): a failed Build may be driven
    /// again while it is newer than the current Build and the current Build is not in progress; a
    /// Build older than the current one, or any Build while another is in progress, is not.
    #[tokio::test]
    async fn a_failed_build_newer_than_the_current_one_is_drivable_and_an_older_one_is_not() {
        let s = MemoryBuildStateAdapter::new();
        let id = |b: &str| BuildId(b.into());
        for (b, at, outcome) in [("b0", 10, AttemptOutcome::Failed { reason: "x".into() }), ("b1", 20, AttemptOutcome::Converged), ("b2", 30, AttemptOutcome::Failed { reason: "y".into() })] {
            s.publish_accepted(&accepted_at(b, at)).await.unwrap();
            s.claim_attempt(&claim(b, 1, "a")).await.unwrap();
            s.append_attempt_receipt(&attempt(b, 1, outcome)).await.unwrap();
        }
        let held = &s;
        let p = |b: &'static str| async move { held.read_build(&BuildId(b.into())).await.unwrap() };
        assert!(drivable(&s, &p("b1").await).await.unwrap(), "the current Build");
        assert!(drivable(&s, &p("b2").await).await.unwrap(), "a failed Build accepted after the current one");
        assert!(!drivable(&s, &p("b0").await).await.unwrap(), "a failed Build older than the current one");
        s.open_attempt(&opened("b1", 2, AttemptReason::Restart)).await.unwrap();
        assert_eq!(s.current_build().await.unwrap(), CurrentBuild::InProgress(id("b1")));
        assert!(!drivable(&s, &p("b2").await).await.unwrap(), "one Build in flight: not while the current one is in progress");
    }

    /// CONTRACT (node-recovery.md "A repeat submit answers AlreadyApplied ... never a second
    /// recovery"; "Steps"): a recovery's exited birth is answered by the recovery index only once the
    /// recovery's last operation (`update-node:<path>`) completed at or after the deletion's attempt:
    /// its node.delete's `NodeDeleted` and `Complete` alone, or with the create's, are not a recovery.
    /// The index is folded wherever a fact enters the log, so a restarted fabric-primary's journal
    /// replay and absorbed facts, in any order, answer the point read as an own append does, and the
    /// first recovering attempt stays the answer, naming the operation that deleted the birth.
    /// Must NOT happen: a recovery answered before its create and update completed (a resubmit then
    /// answers AlreadyApplied for a path no node was created at), or a restarted admin answering None
    /// for a birth its journal holds recovered.
    #[tokio::test]
    async fn the_recovery_index_is_folded_on_append_replay_and_absorb() {
        let (path, node_id, incarnation): (crate::model::PathName, _, _) = ("mesh1.rpc.1".parse().unwrap(), crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        // A recovery's receipts as its three operations write them: the delete's NodeDeleted names
        // the exited birth (its lifecycle operation), and each operation's Complete commits no output.
        let receipt = |build: &str, attempt: u32, operation: &str, step: &str| BuildStepReceipt {
            build_id: BuildId(build.into()),
            attempt,
            operation: format!("{operation}:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: (step == "NodeDeleted").then(|| {
                serde_json::to_value(rafka_mesh_entity::LifecycleOp { build_id: build.into(), attempt, operation: format!("{operation}:{path}"), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1 }).unwrap()
            }),
            executor: Some("mesh1.admin.1".into()),
        };
        let opened = |build: &str, attempt: u32| AttemptOpened {
            build_id: BuildId(build.into()),
            attempt,
            reason: AttemptReason::Recovery,
            action: Some(crate::accepted::AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone() }),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 1,
        };
        let deleted = format!("retire-node:{path}");
        let dir = tmp();
        {
            let j = FileJournal::open(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
            j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
            j.open_attempt(&opened("b1", 2)).await.unwrap();
            j.append_step_receipt(&receipt("b1", 2, "retire-node", "NodeDeleted")).await.unwrap();
            j.append_step_receipt(&receipt("b1", 2, "retire-node", "Complete")).await.unwrap();
            assert_eq!(j.recovery_of(&path, &node_id, &incarnation).await.unwrap(), None, "deleted, the create and the update owed: not recovered");
            j.append_step_receipt(&receipt("b1", 2, "create-node", "Complete")).await.unwrap();
            assert_eq!(j.recovery_of(&path, &node_id, &incarnation).await.unwrap(), None, "created, the update owed: not recovered");
            j.append_step_receipt(&receipt("b1", 3, "update-node", "Complete")).await.unwrap();
            j.append_step_receipt(&receipt("b1", 4, "update-node", "Complete")).await.unwrap();
            assert_eq!(j.recovery_of(&path, &node_id, &incarnation).await.unwrap(), Some((BuildId("b1".into()), 3, deleted.clone())), "own append: the first attempt that completed the recovery");
        }
        let j = FileJournal::open(&dir).await.unwrap(); // the admin restarted
        assert_eq!(j.recovery_of(&path, &node_id, &incarnation).await.unwrap(), Some((BuildId("b1".into()), 3, deleted.clone())), "journal replay");
        assert_eq!(j.recovery_of(&"mesh1.rpc.2".parse().unwrap(), &node_id, &incarnation).await.unwrap(), None, "another path's operation");
        let m = MemoryBuildStateAdapter::new();
        // Gossip delivers in any order: the update's Complete heard first, the attempt's opening last.
        m.absorb(&[BuildFact::Accepted(intent("b1")), BuildFact::Step(receipt("b1", 2, "update-node", "Complete"))]).await;
        assert_eq!(m.recovery_of(&path, &node_id, &incarnation).await.unwrap(), None, "a Complete alone names no birth");
        m.absorb(&[BuildFact::Step(receipt("b1", 2, "retire-node", "NodeDeleted")), BuildFact::Step(receipt("b1", 2, "retire-node", "Complete")), BuildFact::Step(receipt("b1", 2, "create-node", "Complete")), BuildFact::Opened(opened("b1", 2))]).await;
        assert_eq!(m.recovery_of(&path, &node_id, &incarnation).await.unwrap(), Some((BuildId("b1".into()), 2, deleted.clone())), "absorbed, in any order");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": the mesh-primary submits again an exited
    /// birth whose workflow ended Failed; CLAUDE.md read rule: a point read through the index its
    /// caller uses, nothing held in memory between reads): `exits_owed(build, mesh)` answers an exited
    /// birth of `mesh` once the run its latest `Recover` open began ends with its last claimed attempt
    /// `Failed` and the birth is not done. A run in flight (opened and not claimed, or claimed with no
    /// verdict) is not owed; the next open of another birth bounds the run before it; a birth of
    /// another mesh or another Build is not answered; once the birth's `update-node` completes it is
    /// owed no longer. Folded wherever a fact enters the log, so a restarted admin's journal replay
    /// and facts absorbed in reverse order answer as own appends do. Must NOT happen: an owed exit
    /// lost by a restart, a run in flight answered owed, or a done birth answered owed.
    #[tokio::test]
    async fn an_exit_whose_workflow_ended_failed_is_owed_until_it_is_done_on_append_replay_and_absorb() {
        let birth = |path: &str| (path.parse::<crate::model::PathName>().unwrap(), crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let (r, q) = (birth("mesh2.rpc.1"), birth("mesh1.rpc.1"));
        let receipt = |b: &(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId), attempt: u32, operation: &str, step: &str| BuildStepReceipt {
            build_id: BuildId("b1".into()),
            attempt,
            operation: format!("{operation}:{}", b.0),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: (step == "NodeDeleted").then(|| {
                serde_json::to_value(rafka_mesh_entity::LifecycleOp { build_id: "b1".into(), attempt, operation: format!("{operation}:{}", b.0), node_id: b.1.clone(), incarnation: b.2.clone(), name: b.0.clone(), event_at_rafka_ms: 1 }).unwrap()
            }),
            executor: Some("mesh2.admin.1".into()),
        };
        let open = |b: &(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId), attempt: u32| AttemptOpened {
            build_id: BuildId("b1".into()),
            attempt,
            reason: AttemptReason::Recovery,
            action: Some(crate::accepted::AttemptAction::Recover { path: b.0.clone(), node_id: b.1.clone(), from_incarnation: b.2.clone() }),
            opened_by: "mesh1.admin.1".into(),
            opened_at_ms: 1,
        };
        let failed = |n: u32, why: &str| attempt("b1", n, AttemptOutcome::Failed { reason: why.into() });
        let owed = |b: &(crate::model::PathName, crate::model::NodeId, crate::model::IncarnationId), attempt: u32, reason: &str| OwedExit { path: b.0.clone(), node_id: b.1.clone(), incarnation: b.2.clone(), attempt, reason: reason.into() };
        let b1 = BuildId("b1".into());
        let mut facts: Vec<BuildFact> = Vec::new();
        let dir = tmp();
        {
            let j = FileJournal::open(&dir).await.unwrap();
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
            put(BuildFact::Step(receipt(&r, 2, "retire-node", "NodeDeleted"))).await;
            put(BuildFact::Step(receipt(&r, 2, "retire-node", "Complete"))).await;
            assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![], "claimed, no verdict: in flight");
            put(BuildFact::Attempt(failed(2, "create-node: PrepareStorage"))).await;
            assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![owed(&r, 2, "create-node: PrepareStorage")], "deleted, its create failed: owed");
            // Another mesh's birth opens the next attempt and fails: it bounds the run before it.
            for f in [BuildFact::Opened(open(&q, 3)), BuildFact::Claim(claim("b1", 3, "mesh1.admin.1")), BuildFact::Attempt(failed(3, "q's create"))] {
                put(f).await;
            }
            assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![owed(&r, 2, "create-node: PrepareStorage")], "the next open bounds the run; the other mesh's birth is not answered");
            assert_eq!(j.exits_owed(&b1, "mesh1").await.unwrap(), vec![owed(&q, 3, "q's create")], "the other mesh answers its own");
            assert_eq!(j.exits_owed(&BuildId("b2".into()), "mesh2").await.unwrap(), vec![], "another Build holds none");
            for f in [BuildFact::Opened(open(&r, 4)), BuildFact::Claim(claim("b1", 4, "mesh2.admin.2"))] {
                put(f).await;
            }
            assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![], "submitted again and claimed: in flight");
            put(BuildFact::Attempt(failed(4, "create-node: PrepareStorage again"))).await;
        }
        let j = FileJournal::open(&dir).await.unwrap(); // the admin restarted, or another took the seat
        assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![owed(&r, 4, "create-node: PrepareStorage again")], "journal replay: the latest run's Failed attempt");
        for f in [
            BuildFact::Opened(open(&r, 5)),
            BuildFact::Claim(claim("b1", 5, "mesh2.admin.2")),
            BuildFact::Step(receipt(&r, 5, "create-node", "Complete")),
            BuildFact::Step(receipt(&r, 5, "update-node", "Complete")),
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
        assert_eq!(j.exits_owed(&b1, "mesh2").await.unwrap(), vec![], "created and updated: done, owed no longer");
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
    /// `replace-node` or `retire-node` operation deleted (its `NodeDeleted` receipt names the exact
    /// birth, and the operation's `Complete` at that attempt or later is held) is answered by the
    /// recovery index as that workflow's attempt, folded on append, journal replay and absorb alike.
    /// Must NOT happen: `None` for a birth a workflow deleted, which admits a recovery of it.
    #[tokio::test]
    async fn a_birth_a_replace_or_a_retire_deleted_is_answered_by_that_workflow() {
        let path: crate::model::PathName = "mesh2.rpc.2".parse().unwrap();
        let receipt = |kind: &str, attempt: u32, step: &str, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId| BuildStepReceipt {
            build_id: BuildId("b1".into()),
            attempt,
            operation: format!("{kind}:{path}"),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: (step == "NodeDeleted").then(|| {
                serde_json::to_value(rafka_mesh_entity::LifecycleOp { build_id: "b1".into(), attempt, operation: format!("{kind}:{path}"), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1 }).unwrap()
            }),
            executor: Some("mesh2.admin.1".into()),
        };
        let replaced = (crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let retired = (crate::model::NodeId::mint(), crate::model::IncarnationId::mint());
        let dir = tmp();
        {
            let j = FileJournal::open(&dir).await.unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.append_step_receipt(&receipt("replace-node", 2, "NodeDeleted", &replaced.0, &replaced.1)).await.unwrap();
            assert_eq!(j.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), None, "deleted, the replace not complete: its next attempt finishes it");
            j.append_step_receipt(&receipt("replace-node", 2, "Complete", &replaced.0, &replaced.1)).await.unwrap();
            j.append_step_receipt(&receipt("retire-node", 3, "NodeDeleted", &retired.0, &retired.1)).await.unwrap();
            j.append_step_receipt(&receipt("retire-node", 3, "Complete", &retired.0, &retired.1)).await.unwrap();
            assert_eq!(j.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), Some((BuildId("b1".into()), 2, format!("replace-node:{path}"))), "own append: the replace that deleted the birth");
            assert_eq!(j.recovery_of(&path, &retired.0, &retired.1).await.unwrap(), Some((BuildId("b1".into()), 3, format!("retire-node:{path}"))), "own append: the retire that deleted the birth");
        }
        let j = FileJournal::open(&dir).await.unwrap(); // the admin restarted
        assert_eq!(j.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), Some((BuildId("b1".into()), 2, format!("replace-node:{path}"))), "journal replay");
        assert_eq!(j.recovery_of(&"mesh2.rpc.1".parse().unwrap(), &replaced.0, &replaced.1).await.unwrap(), None, "another path's operation");
        let m = MemoryBuildStateAdapter::new();
        m.absorb(&[BuildFact::Accepted(intent("b1")), BuildFact::Step(receipt("replace-node", 2, "Complete", &replaced.0, &replaced.1))]).await;
        assert_eq!(m.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), None, "a Complete alone names no birth");
        m.absorb(&[BuildFact::Step(receipt("replace-node", 2, "NodeDeleted", &replaced.0, &replaced.1))]).await;
        assert_eq!(m.recovery_of(&path, &replaced.0, &replaced.1).await.unwrap(), Some((BuildId("b1".into()), 2, format!("replace-node:{path}"))), "absorbed, in either order");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: opening the journal (its replay of every fact line in the file) never waits on
    /// the disk on a runtime worker: it runs on the blocking pool, as every append does. The admin
    /// opens it inside its boot, on the runtime. The cell opens a journal whose file holds the
    /// read: the runtime's only thread keeps running, and the replay answers the logged Build.
    #[test]
    fn opening_the_journal_waits_on_the_disk_off_the_runtime_thread() {
        let dir = crate::record_store::tempdir("journal-offload");
        let line = format!("{}\n", serde_json::to_string(&BuildFact::Accepted(intent("b1"))).unwrap()).into_bytes();
        let fifo = dir.join(JOURNAL_FILE);
        let (ran, active) = crate::record_store::runtime_runs_while_a_fifo_is_read(fifo, line, Box::pin(async move { FileJournal::open(&dir).await.unwrap().list_active().await.unwrap().len() }));
        assert!(ran, "the runtime thread ran while the journal's replay waited on the disk");
        assert_eq!(active, 1);
    }

    #[tokio::test]
    async fn a_corrupt_journal_line_is_named() {
        let dir = tmp();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(JOURNAL_FILE), format!("{}\nnot json\n", serde_json::to_string(&BuildFact::Accepted(intent("b1"))).unwrap())).unwrap();
        assert!(matches!(FileJournal::open(&dir).await, Err(BuildStateError::CorruptJournal { line: 2, .. })));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// PRD §1.4 / §7: no test, and no code outside this module, opens a Build
    /// journal; cross-mesh authority is the live projection, never a disk.
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
                } else if p.extension().is_some_and(|x| x == "rs") && !p.ends_with("build_state.rs") && !p.ends_with("rafka-node-admin-core/src/admin.rs") {
                    let text = std::fs::read_to_string(&p).unwrap_or_default();
                    if text.contains("FileJournal::open") || text.contains("build-journal.jsonl") || text.contains("JOURNAL_FILE") {
                        offenders.push(p);
                    }
                }
            }
        }
        assert!(offenders.is_empty(), "only build_state.rs and the admin's own boot may open a journal: {offenders:?}");
    }
}
