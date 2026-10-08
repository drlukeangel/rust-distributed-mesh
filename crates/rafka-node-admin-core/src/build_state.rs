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
use std::sync::Mutex;

/// A Build accepted by the fabric-primary: the complete topology it realizes, and the change that
/// produced it (history only; planning reads `topology`, never `submitted_change`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildAccepted {
    pub build_id: BuildId,
    pub topology: FabricTopology,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_change: Option<TopologyChange>,
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
    Restart,
    Replace,
    /// The previous attempt's executor is gone; another admin continues the Build.
    AuthorityMoved,
}

impl AttemptReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::ProvenDrift => "proven-drift",
            Self::Restart => "restart",
            Self::Replace => "replace",
            Self::AuthorityMoved => "authority-moved",
        }
    }
}

/// An attempt opened on a Build by the authority (the first one by acceptance itself): what it
/// is for, and the fenced action it carries. Insert-and-fail on `(build_id, attempt)`: two
/// authorities proving the same drift open one attempt, never two.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptOpened {
    pub build_id: BuildId,
    pub attempt: u32,
    pub reason: AttemptReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<AttemptAction>,
    pub opened_by: String,
    pub opened_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildAttemptClaim {
    pub build_id: BuildId,
    pub attempt: u32,
    /// The claiming executor (a node-admin's path.name).
    pub executor: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildStepReceipt {
    pub build_id: BuildId,
    pub attempt: u32,
    /// The operation's idempotency key (`create-node:mesh1.rpc.2`, ...).
    pub operation: String,
    pub step: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepOutcome {
    Complete,
    Failed { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildAttemptReceipt {
    pub build_id: BuildId,
    pub attempt: u32,
    pub outcome: AttemptOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcome {
    /// Desired state reached: the Build is complete.
    Converged,
    /// This attempt stopped; a later attempt may continue the same Build.
    Failed { reason: String },
    /// This attempt ran what its executor was eligible for and stopped at an
    /// operation another admin executes (`to`); that admin claims the next
    /// attempt of the same Build.
    HandedOff { to: String },
}

/// One appended fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "fact", rename_all = "snake_case")]
pub enum BuildFact {
    Accepted(BuildAccepted),
    Opened(AttemptOpened),
    Claim(BuildAttemptClaim),
    Step(BuildStepReceipt),
    Attempt(BuildAttemptReceipt),
    /// Build-history administration (`DELETE /api/builds?id=`): a finished
    /// Build leaves the views. Never a topology mutation.
    Forget { build_id: BuildId },
}

impl BuildFact {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    Pending,
    Running,
    Complete,
    Failed,
}

/// The folded Build view (`GET /api/builds?id=`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildProjection {
    pub build_id: BuildId,
    pub topology: FabricTopology,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_change: Option<TopologyChange>,
    pub submitted_at_ms: u64,
    pub state: BuildState,
    /// The current attempt: the highest attempt claimed. An opened attempt is `attempt + 1`.
    pub attempt: u32,
    pub executor: Option<String>,
    pub steps: Vec<BuildStepReceipt>,
    pub last_failure: Option<String>,
    /// Why the current attempt exists, and what it carries.
    pub reason: AttemptReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<AttemptAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    Won,
    /// The attempt is already claimed by `holder`.
    Lost { holder: String },
    /// Not the Build's next attempt, or the Build is complete and no attempt was opened on it:
    /// nothing to run, nothing recorded.
    NotOpen { next: Option<u32> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildStateError {
    UnknownBuild(BuildId),
    /// An intent was published twice for one id with different content.
    ConflictingIntent(BuildId),
    Io(String),
    /// A fact that has no wire shape (a step output of a step that commits none, or of another
    /// type than its step's): named with the Build, attempt and step.
    Unencodable(String),
    /// A journal line that does not decode, named with its line number.
    CorruptJournal { line: usize, reason: String },
    /// The attempt number is held by another opener's action: `held` is what the number carries,
    /// `wanted` what this open asked for. The number is never shared by two actions.
    AttemptTaken { build_id: BuildId, attempt: u32, held: String, wanted: String },
}

impl std::fmt::Display for BuildStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownBuild(id) => write!(f, "no Build {id}"),
            Self::ConflictingIntent(id) => write!(f, "Build {id} was published with a different intent"),
            Self::Io(e) => write!(f, "build journal I/O: {e}"),
            Self::Unencodable(e) => write!(f, "Build fact cannot be sent: {e}"),
            Self::CorruptJournal { line, reason } => write!(f, "build journal line {line} does not decode: {reason}"),
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
                        p.state = BuildState::Running;
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
                    if a.attempt != p.attempt || p.state == BuildState::Complete {
                        continue; // a superseded attempt's verdict does not decide the Build
                    }
                    match &a.outcome {
                        AttemptOutcome::Converged => p.state = BuildState::Complete,
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

/// The lifecycle operations open in `builds`: every `NodeDeleting` step a retire operation
/// completed with no `NodeDeleted` step of the same operation after it, and every
/// `NodeRestarting` step whose restart (`restart-node:<path>`, the op it carries) has no
/// `Complete` step yet. Derived from the Build facts on every round, never stored: a successor
/// primary publishes the same overlays from the same facts.
pub fn in_flight_ops(builds: &BTreeMap<BuildId, BuildProjection>) -> InFlight {
    let mut out = InFlight::default();
    for p in builds.values() {
        for s in &p.steps {
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
            let closed = if s.step == "NodeDeleting" {
                p.steps.iter().any(|t| t.step == "NodeDeleted" && t.operation == s.operation && t.outcome == StepOutcome::Complete)
            } else {
                p.steps.iter().any(|t| t.step == "Complete" && t.operation == op.operation && t.outcome == StepOutcome::Complete)
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
    pub ops: Vec<rafka_mesh_entity::LifecycleOp>,
    pub unrecognised: Vec<UnrecognisedStep>,
}

/// A completed `NodeDeleting`/`NodeRestarting` step whose recorded operation cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnrecognisedStep {
    pub build_id: BuildId,
    pub attempt: u32,
    pub operation: String,
    pub step: String,
    pub reason: String,
}

#[async_trait]
pub trait BuildStateAdapter: Send + Sync {
    /// Record an accepted Build. Insert-and-fail on its id.
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError>;
    /// Open an attempt of a Build. Insert-and-fail on `(build_id, attempt)`: the first opener wins.
    /// A second open of the same attempt that asks for the same action (two authorities proving the
    /// same drift) changes nothing and succeeds; one that asks for another action is refused
    /// `AttemptTaken`, so no caller is told an attempt is open for an action it does not carry.
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError>;
    /// Tell the fabric `Fabric.build_id` moved (the record it names). A local-only adapter has no
    /// fabric to tell.
    async fn publish_fabric(&self, _record: &crate::fabric_storage::FabricRecord) -> Result<(), BuildStateError> {
        Ok(())
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError>;
    /// Builds that are neither complete nor failed.
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError>;
    /// Insert-and-fail on `(build_id, attempt)`. Only the fabric-primary calls this on its own
    /// log to decide a claim (`build_claim::ClaimDoor`); an executor never claims from its own.
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError>;
    /// Record, on this admin's own log, a claim the fabric-primary decided `Won`: the same fact
    /// the primary broadcast, taken now so this admin's view of the Build moves with its own
    /// attempt. Idempotent; decides nothing and broadcasts nothing.
    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError>;
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError>;
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError>;
    /// Every fact, in append order (what the fabric projection carries).
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError>;
    /// Drop a finished Build from the views (history administration).
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError>;
}

/// What an opened attempt carries, for a refusal: its reason, its action and who opened it.
fn describe_open(o: &AttemptOpened) -> String {
    let action = match &o.action {
        Some(crate::accepted::AttemptAction::Restart { path, from_incarnation }) => format!("restart of {path} from {from_incarnation}"),
        Some(crate::accepted::AttemptAction::Replace { path, from_incarnation }) => format!("replace of {path} from {from_incarnation}"),
        None => "no fenced action".to_string(),
    };
    format!("{} ({action}, opened by {})", o.reason.as_str(), o.opened_by)
}

/// The shared append-and-fold core of both RDM adapters.
#[derive(Debug, Default)]
struct FactLog {
    facts: Vec<BuildFact>,
}

impl FactLog {
    fn accepted_conflict(&self, accepted: &BuildAccepted) -> Result<bool, BuildStateError> {
        for f in &self.facts {
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
    fn not_open(&self, claim: &BuildAttemptClaim) -> Option<ClaimOutcome> {
        let p = fold(&self.facts).remove(&claim.build_id)?;
        if matches!(p.state, BuildState::Complete) || claim.attempt != p.attempt + 1 {
            return Some(ClaimOutcome::NotOpen { next: (!matches!(p.state, BuildState::Complete)).then_some(p.attempt + 1) });
        }
        None
    }

    /// Whether `f` adds anything to this log: acceptances, opens and claims are insert-and-fail on
    /// their key, everything else deduplicates.
    fn is_new(&self, f: &BuildFact) -> bool {
        match f {
            BuildFact::Accepted(i) => !self.known(&i.build_id),
            BuildFact::Opened(o) => self.opened(&o.build_id, o.attempt).is_none(),
            BuildFact::Claim(c) => self.claim_holder(&c.build_id, c.attempt).is_none(),
            _ => !self.facts.contains(f),
        }
    }

    fn opened(&self, build_id: &BuildId, attempt: u32) -> Option<&AttemptOpened> {
        self.facts.iter().find_map(|f| match f {
            BuildFact::Opened(o) if &o.build_id == build_id && o.attempt == attempt => Some(o),
            _ => None,
        })
    }

    /// Whether `wanted` may be recorded: `Ok(true)` the number is free, `Ok(false)` it is held for the
    /// same action (nothing to record), `Err(AttemptTaken)` it is held for another.
    fn admit_open(&self, wanted: &AttemptOpened) -> Result<bool, BuildStateError> {
        match self.opened(&wanted.build_id, wanted.attempt) {
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

    fn claim_holder(&self, build_id: &BuildId, attempt: u32) -> Option<String> {
        self.facts.iter().find_map(|f| match f {
            BuildFact::Claim(c) if &c.build_id == build_id && c.attempt == attempt => Some(c.executor.clone()),
            _ => None,
        })
    }

    fn known(&self, build_id: &BuildId) -> bool {
        self.facts.iter().any(|f| matches!(f, BuildFact::Accepted(i) if &i.build_id == build_id))
    }

    fn read(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        fold(&self.facts).remove(build_id).ok_or_else(|| BuildStateError::UnknownBuild(build_id.clone()))
    }

    fn active(&self) -> Vec<BuildProjection> {
        fold(&self.facts).into_values().filter(|p| matches!(p.state, BuildState::Pending | BuildState::Running)).collect()
    }
}

/// In-memory fact log.
#[derive(Debug, Default)]
pub struct MemoryBuildStateAdapter {
    log: Mutex<FactLog>,
}

impl MemoryBuildStateAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge facts received from the fabric projection (append the unseen
    /// ones, keeping the receiver's own order deterministic: intents and
    /// claims are insert-and-fail, receipts are deduplicated).
    pub fn absorb(&self, facts: &[BuildFact]) {
        let mut log = self.log.lock().unwrap();
        for f in facts {
            if log.is_new(f) {
                log.facts.push(f.clone());
            }
        }
    }
}

/// The local fact log a Build-topic adapter holds and absorbs the fabric's facts into: in memory
/// for runs that need no restart survival, or the admin's own journal (`builds.storage`).
#[async_trait]
pub trait LocalBuildLog: BuildStateAdapter {
    /// Merge facts heard from the fabric (append the unseen ones; intents, opens and claims are
    /// insert-and-fail, receipts deduplicate).
    async fn absorb_facts(&self, facts: &[BuildFact]);
}

#[async_trait]
impl LocalBuildLog for MemoryBuildStateAdapter {
    async fn absorb_facts(&self, facts: &[BuildFact]) {
        self.absorb(facts)
    }
}

#[async_trait]
impl LocalBuildLog for FileJournal {
    async fn absorb_facts(&self, facts: &[BuildFact]) {
        let _w = self.writer.lock().await;
        for f in facts {
            let new = self.log.lock().unwrap().is_new(f);
            if new {
                if let Err(e) = self.append(f.clone()).await {
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
        let mut log = self.log.lock().unwrap();
        if !log.accepted_conflict(accepted)? {
            log.facts.push(BuildFact::Accepted(accepted.clone()));
        }
        Ok(())
    }

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        let mut log = self.log.lock().unwrap();
        if !log.known(&opened.build_id) {
            return Err(BuildStateError::UnknownBuild(opened.build_id.clone()));
        }
        if log.admit_open(opened)? {
            log.facts.push(BuildFact::Opened(opened.clone()));
        }
        Ok(())
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.log.lock().unwrap().read(build_id)
    }

    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        Ok(self.log.lock().unwrap().active())
    }

    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        let mut log = self.log.lock().unwrap();
        if !log.known(&claim.build_id) {
            return Err(BuildStateError::UnknownBuild(claim.build_id.clone()));
        }
        if let Some(holder) = log.claim_holder(&claim.build_id, claim.attempt) {
            return Ok(if holder == claim.executor { ClaimOutcome::Won } else { ClaimOutcome::Lost { holder } });
        }
        if let Some(not_open) = log.not_open(&claim) {
            return Ok(not_open);
        }
        log.facts.push(BuildFact::Claim(claim.clone()));
        Ok(ClaimOutcome::Won)
    }

    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.absorb(&[BuildFact::Claim(claim.clone())]);
        Ok(())
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.log.lock().unwrap().facts.push(BuildFact::Step(receipt.clone()));
        Ok(())
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.log.lock().unwrap().facts.push(BuildFact::Attempt(receipt.clone()));
        Ok(())
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        Ok(self.log.lock().unwrap().facts.clone())
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.log.lock().unwrap().facts.push(BuildFact::Forget { build_id: build_id.clone() });
        Ok(())
    }
}

/// The journal file name inside an admin's own data dir.
pub const JOURNAL_FILE: &str = "build-journal.jsonl";

/// `builds.storage`: the same facts, one JSON line each, appended and fsynced in the admin's own
/// data dir before the fact counts. Re-opening it replays to the same view, so a Build accepted
/// before an all-admin restart is still held after it.
#[derive(Debug)]
pub struct FileJournal {
    path: PathBuf,
    /// The replayed facts. Held only for in-memory reads and pushes, never across the disk.
    log: Mutex<FactLog>,
    /// Serializes writers: decide on the log, append + fsync on the blocking pool, then push. An
    /// async lock, so a writer waiting on another's fsync yields its worker and readers never wait.
    writer: tokio::sync::Mutex<()>,
}

impl FileJournal {
    /// Open (or create) the journal in `own_data_dir` and replay it.
    pub fn open(own_data_dir: &Path) -> Result<Self, BuildStateError> {
        std::fs::create_dir_all(own_data_dir).map_err(|e| BuildStateError::Io(e.to_string()))?;
        let path = own_data_dir.join(JOURNAL_FILE);
        let mut log = FactLog::default();
        if path.exists() {
            let f = std::fs::File::open(&path).map_err(|e| BuildStateError::Io(e.to_string()))?;
            for (i, line) in std::io::BufReader::new(f).lines().enumerate() {
                let line = line.map_err(|e| BuildStateError::Io(e.to_string()))?;
                if line.trim().is_empty() {
                    continue;
                }
                let fact: BuildFact = serde_json::from_str(&line)
                    .map_err(|e| BuildStateError::CorruptJournal { line: i + 1, reason: e.to_string() })?;
                log.facts.push(fact);
            }
        }
        Ok(Self { path, log: Mutex::new(log), writer: tokio::sync::Mutex::new(()) })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append `fact` as one fsynced line, then count it. The caller holds `writer`.
    async fn append(&self, fact: BuildFact) -> Result<(), BuildStateError> {
        use std::io::Write as _;
        let line = serde_json::to_string(&fact).map_err(|e| BuildStateError::Io(e.to_string()))?;
        let path = self.path.clone();
        crate::record_store::blocking_io(move || {
            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path).map_err(|e| BuildStateError::Io(e.to_string()))?;
            writeln!(f, "{line}").and_then(|_| f.sync_data()).map_err(|e| BuildStateError::Io(e.to_string()))
        })
        .await?;
        self.log.lock().unwrap().facts.push(fact);
        Ok(())
    }
}

#[async_trait]
impl BuildStateAdapter for FileJournal {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        let _w = self.writer.lock().await;
        let conflict = self.log.lock().unwrap().accepted_conflict(accepted)?;
        if !conflict {
            self.append(BuildFact::Accepted(accepted.clone())).await?;
        }
        Ok(())
    }

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        let _w = self.writer.lock().await;
        let free = {
            let log = self.log.lock().unwrap();
            if !log.known(&opened.build_id) {
                return Err(BuildStateError::UnknownBuild(opened.build_id.clone()));
            }
            log.admit_open(opened)?
        };
        if free {
            self.append(BuildFact::Opened(opened.clone())).await?;
        }
        Ok(())
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.log.lock().unwrap().read(build_id)
    }

    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        Ok(self.log.lock().unwrap().active())
    }

    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        let _w = self.writer.lock().await;
        {
            let log = self.log.lock().unwrap();
            if !log.known(&claim.build_id) {
                return Err(BuildStateError::UnknownBuild(claim.build_id.clone()));
            }
            if let Some(holder) = log.claim_holder(&claim.build_id, claim.attempt) {
                return Ok(if holder == claim.executor { ClaimOutcome::Won } else { ClaimOutcome::Lost { holder } });
            }
            if let Some(not_open) = log.not_open(claim) {
                return Ok(not_open);
            }
        }
        self.append(BuildFact::Claim(claim.clone())).await?;
        Ok(ClaimOutcome::Won)
    }

    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        let _w = self.writer.lock().await;
        let new = self.log.lock().unwrap().is_new(&BuildFact::Claim(claim.clone()));
        if new {
            self.append(BuildFact::Claim(claim.clone())).await?;
        }
        Ok(())
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        let _w = self.writer.lock().await;
        self.append(BuildFact::Step(receipt.clone())).await
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let _w = self.writer.lock().await;
        self.append(BuildFact::Attempt(receipt.clone())).await
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        Ok(self.log.lock().unwrap().facts.clone())
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let _w = self.writer.lock().await;
        self.append(BuildFact::Forget { build_id: build_id.clone() }).await
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
            topology: FabricTopology::root("fabric1", "mesh1"),
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
            with(&b.operation, "MarkDraining", &b),
        ];
        let open = in_flight_ops(&fold(&facts)).ops;
        assert_eq!(open, vec![b.clone()], "a retire with its pre-notice and no departure is open; a completed one is not");

        // A restart's pre-event is a step of its retire half; the restart is open until its
        // create half (`restart-node:<path>`) completes, whoever reads the facts.
        let c = rafka_mesh_entity::LifecycleOp { operation: "restart-node:mesh1.rpc.3".into(), name: "mesh1.rpc.3".parse().unwrap(), ..op("mesh1.rpc.3") };
        let mut facts = facts;
        facts.push(with("retire-node:mesh1.rpc.3", "NodeRestarting", &c));
        facts.push(with("retire-node:mesh1.rpc.3", "TerminateRuntime", &c));
        assert_eq!(in_flight_ops(&fold(&facts)).ops, vec![b.clone(), c.clone()], "a successor holds the restart from the Build facts alone");
        facts.push(with("restart-node:mesh1.rpc.3", "Complete", &c));
        assert_eq!(in_flight_ops(&fold(&facts)).ops, vec![b], "the restart's create half completing closes it");
        assert!(in_flight_ops(&fold(&facts)).unrecognised.is_empty());
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
        let read = in_flight_ops(&fold(&facts));
        assert!(read.ops.is_empty(), "an unreadable step opens no overlay");
        assert_eq!(read.unrecognised.len(), 1, "{read:?}");
        let u = &read.unrecognised[0];
        assert_eq!((u.attempt, u.operation.as_str(), u.step.as_str()), (2, "retire-node:mesh1.rpc.1", "NodeDeleting"));
        assert!(u.reason.contains("not a LifecycleOp this build reads"), "{}", u.reason);
        let none = in_flight_ops(&fold(&[BuildFact::Accepted(intent("b1")), step(None)]));
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
        a.absorb(&log());
        a.absorb(&log());
        assert_eq!(fold(&a.facts().await.unwrap()), fold(&log()));
    }

    fn tmp() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!("journal-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    #[tokio::test]
    async fn a_same_admin_restart_replays_the_journal_to_the_same_view() {
        let dir = tmp();
        let before = {
            let j = FileJournal::open(&dir).unwrap();
            j.publish_accepted(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
            j.append_step_receipt(&step("b1", 1, "restart-node:mesh1.rpc.2")).await.unwrap();
            j.publish_accepted(&intent("b2")).await.unwrap();
            (j.read_build(&BuildId("b1".into())).await.unwrap(), j.list_active().await.unwrap())
        };
        let j = FileJournal::open(&dir).unwrap(); // the admin restarted
        assert_eq!(j.read_build(&BuildId("b1".into())).await.unwrap(), before.0);
        assert_eq!(j.list_active().await.unwrap(), before.1);
        assert_eq!(j.claim_attempt(&claim("b1", 1, "mesh1.admin.2")).await.unwrap(), ClaimOutcome::Lost { holder: "mesh1.admin.1".into() });
        j.append_attempt_receipt(&attempt("b1", 1, AttemptOutcome::Converged)).await.unwrap();
        let again = FileJournal::open(&dir).unwrap();
        assert_eq!(again.read_build(&BuildId("b1".into())).await.unwrap().state, BuildState::Complete);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_corrupt_journal_line_is_named() {
        let dir = tmp();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(JOURNAL_FILE), format!("{}\nnot json\n", serde_json::to_string(&BuildFact::Accepted(intent("b1"))).unwrap())).unwrap();
        assert!(matches!(FileJournal::open(&dir), Err(BuildStateError::CorruptJournal { line: 2, .. })));
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
