//! `BuildStateAdapter`: where Build intent and evidence live (PRD §1.5–6, §7;
//! mesh-control-plane.md §4).
//!
//! The vocabulary is append/fold: an intent fact is put once, an attempt is
//! taken by an insert-and-fail claim on `(build_id, attempt)`, step and attempt
//! receipts are appended, and the current Build view is a fold over the facts.
//! Nothing is read-modify-written: two executors claiming the same attempt
//! cannot both win, and the current attempt is the folded maximum winning claim.
//!
//! RDM adapters:
//! - [`MemoryBuildStateAdapter`]: the in-memory fact log that backs the live
//!   fabric control projection;
//! - [`FileJournal`]: an optional local journal of the same facts for a
//!   same-admin restart and replay evidence. It is never cross-mesh authority:
//!   a successor in another mesh continues a Build from the live projection,
//!   never from a dead admin's disk.

use crate::build::{BuildId, BuildIntent};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildIntentFact {
    pub build_id: BuildId,
    pub intent: BuildIntent,
    /// W3C traceparent of the accepting span, so execution parents to it.
    pub traceparent: Option<String>,
    pub submitted_at_ms: u64,
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
}

/// One appended fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "fact", rename_all = "snake_case")]
pub enum BuildFact {
    Intent(BuildIntentFact),
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
            Self::Intent(f) => &f.build_id,
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
    pub intent: BuildIntent,
    pub traceparent: Option<String>,
    pub state: BuildState,
    /// The current attempt: the highest attempt with a winning claim.
    pub attempt: u32,
    pub executor: Option<String>,
    pub steps: Vec<BuildStepReceipt>,
    pub last_failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    Won,
    /// The attempt is already claimed by `holder`.
    Lost { holder: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildStateError {
    UnknownBuild(BuildId),
    /// An intent was published twice for one id with different content.
    ConflictingIntent(BuildId),
    Io(String),
    /// A journal line that does not decode, named with its line number.
    CorruptJournal { line: usize, reason: String },
}

impl std::fmt::Display for BuildStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownBuild(id) => write!(f, "no Build {id}"),
            Self::ConflictingIntent(id) => write!(f, "Build {id} was published with a different intent"),
            Self::Io(e) => write!(f, "build journal I/O: {e}"),
            Self::CorruptJournal { line, reason } => write!(f, "build journal line {line} does not decode: {reason}"),
        }
    }
}

/// Fold an ordered fact log into Build views. Deterministic and idempotent:
/// the same log gives the same views, and a duplicated fact changes nothing.
/// The winning claim of an attempt is the first one appended.
pub fn fold(facts: &[BuildFact]) -> BTreeMap<BuildId, BuildProjection> {
    let mut out: BTreeMap<BuildId, BuildProjection> = BTreeMap::new();
    let mut winners: BTreeMap<(BuildId, u32), String> = BTreeMap::new();
    for f in facts {
        match f {
            BuildFact::Intent(i) => {
                out.entry(i.build_id.clone()).or_insert_with(|| BuildProjection {
                    build_id: i.build_id.clone(),
                    intent: i.intent.clone(),
                    traceparent: i.traceparent.clone(),
                    state: BuildState::Pending,
                    attempt: 0,
                    executor: None,
                    steps: Vec::new(),
                    last_failure: None,
                });
            }
            BuildFact::Claim(c) => {
                let key = (c.build_id.clone(), c.attempt);
                if winners.contains_key(&key) {
                    continue;
                }
                winners.insert(key, c.executor.clone());
                if let Some(p) = out.get_mut(&c.build_id) {
                    if c.attempt > p.attempt && !matches!(p.state, BuildState::Complete) {
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

#[async_trait]
pub trait BuildStateAdapter: Send + Sync {
    async fn publish_intent(&self, intent: &BuildIntentFact) -> Result<(), BuildStateError>;
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError>;
    /// Builds that are neither complete nor failed.
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError>;
    /// Insert-and-fail on `(build_id, attempt)`.
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError>;
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError>;
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError>;
    /// Every fact, in append order (what the fabric projection carries).
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError>;
    /// Drop a finished Build from the views (history administration).
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError>;
}

/// The shared append-and-fold core of both RDM adapters.
#[derive(Debug, Default)]
struct FactLog {
    facts: Vec<BuildFact>,
}

impl FactLog {
    fn intent_conflict(&self, intent: &BuildIntentFact) -> Result<bool, BuildStateError> {
        for f in &self.facts {
            if let BuildFact::Intent(i) = f {
                if i.build_id == intent.build_id {
                    return if i == intent { Ok(true) } else { Err(BuildStateError::ConflictingIntent(intent.build_id.clone())) };
                }
            }
        }
        Ok(false)
    }

    fn claim_holder(&self, build_id: &BuildId, attempt: u32) -> Option<String> {
        self.facts.iter().find_map(|f| match f {
            BuildFact::Claim(c) if &c.build_id == build_id && c.attempt == attempt => Some(c.executor.clone()),
            _ => None,
        })
    }

    fn known(&self, build_id: &BuildId) -> bool {
        self.facts.iter().any(|f| matches!(f, BuildFact::Intent(i) if &i.build_id == build_id))
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
            let new = match f {
                BuildFact::Intent(i) => !log.known(&i.build_id),
                BuildFact::Claim(c) => log.claim_holder(&c.build_id, c.attempt).is_none(),
                _ => !log.facts.contains(f),
            };
            if new {
                log.facts.push(f.clone());
            }
        }
    }
}

#[async_trait]
impl BuildStateAdapter for MemoryBuildStateAdapter {
    async fn publish_intent(&self, intent: &BuildIntentFact) -> Result<(), BuildStateError> {
        let mut log = self.log.lock().unwrap();
        if !log.intent_conflict(intent)? {
            log.facts.push(BuildFact::Intent(intent.clone()));
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
        log.facts.push(BuildFact::Claim(claim.clone()));
        Ok(ClaimOutcome::Won)
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

/// Optional local journal: the same facts, one JSON line each, in the
/// admin's own data dir. Re-opening it replays to the same view.
#[derive(Debug)]
pub struct FileJournal {
    path: PathBuf,
    log: Mutex<FactLog>,
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
        Ok(Self { path, log: Mutex::new(log) })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn append(&self, log: &mut FactLog, fact: BuildFact) -> Result<(), BuildStateError> {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| BuildStateError::Io(e.to_string()))?;
        let line = serde_json::to_string(&fact).map_err(|e| BuildStateError::Io(e.to_string()))?;
        writeln!(f, "{line}").and_then(|_| f.sync_data()).map_err(|e| BuildStateError::Io(e.to_string()))?;
        log.facts.push(fact);
        Ok(())
    }
}

#[async_trait]
impl BuildStateAdapter for FileJournal {
    async fn publish_intent(&self, intent: &BuildIntentFact) -> Result<(), BuildStateError> {
        let mut log = self.log.lock().unwrap();
        if !log.intent_conflict(intent)? {
            self.append(&mut log, BuildFact::Intent(intent.clone()))?;
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
        self.append(&mut log, BuildFact::Claim(claim.clone()))?;
        Ok(ClaimOutcome::Won)
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        let mut log = self.log.lock().unwrap();
        self.append(&mut log, BuildFact::Step(receipt.clone()))
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let mut log = self.log.lock().unwrap();
        self.append(&mut log, BuildFact::Attempt(receipt.clone()))
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        Ok(self.log.lock().unwrap().facts.clone())
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        let mut log = self.log.lock().unwrap();
        self.append(&mut log, BuildFact::Forget { build_id: build_id.clone() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::BuildIntent;

    fn intent(id: &str) -> BuildIntentFact {
        BuildIntentFact {
            build_id: BuildId(id.into()),
            intent: BuildIntent::RestartNode { node: "mesh1.rpc.2".parse().unwrap() },
            traceparent: None,
            submitted_at_ms: 1,
        }
    }

    fn claim(id: &str, attempt: u32, who: &str) -> BuildAttemptClaim {
        BuildAttemptClaim { build_id: BuildId(id.into()), attempt, executor: who.into() }
    }

    fn step(id: &str, attempt: u32, op: &str) -> BuildStepReceipt {
        BuildStepReceipt { build_id: BuildId(id.into()), attempt, operation: op.into(), step: "DeployRuntime".into(), outcome: StepOutcome::Complete }
    }

    fn attempt(id: &str, attempt: u32, outcome: AttemptOutcome) -> BuildAttemptReceipt {
        BuildAttemptReceipt { build_id: BuildId(id.into()), attempt, outcome }
    }

    fn log() -> Vec<BuildFact> {
        vec![
            BuildFact::Intent(intent("b1")),
            BuildFact::Claim(claim("b1", 1, "mesh1.admin.1")),
            BuildFact::Step(step("b1", 1, "restart-node:mesh1.rpc.2")),
            BuildFact::Attempt(attempt("b1", 1, AttemptOutcome::Failed { reason: "executor lost".into() })),
            BuildFact::Claim(claim("b1", 2, "mesh1.admin.2")),
            BuildFact::Claim(claim("b1", 2, "mesh1.admin.1")), // loses: attempt 2 already claimed
            BuildFact::Attempt(attempt("b1", 1, AttemptOutcome::Converged)), // stale attempt: ignored
            BuildFact::Attempt(attempt("b1", 2, AttemptOutcome::Converged)),
            BuildFact::Intent(intent("b2")),
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

    #[tokio::test]
    async fn claims_are_insert_and_fail() {
        let m = MemoryBuildStateAdapter::new();
        m.publish_intent(&intent("b1")).await.unwrap();
        assert_eq!(m.claim_attempt(&claim("b1", 1, "a")).await.unwrap(), ClaimOutcome::Won);
        assert_eq!(m.claim_attempt(&claim("b1", 1, "b")).await.unwrap(), ClaimOutcome::Lost { holder: "a".into() });
        assert_eq!(m.claim_attempt(&claim("b1", 1, "a")).await.unwrap(), ClaimOutcome::Won, "re-claiming your own attempt is idempotent");
        assert_eq!(m.claim_attempt(&claim("b9", 1, "a")).await, Err(BuildStateError::UnknownBuild(BuildId("b9".into()))));
        let mut other = intent("b1");
        other.submitted_at_ms = 2;
        assert_eq!(m.publish_intent(&other).await, Err(BuildStateError::ConflictingIntent(BuildId("b1".into()))));
        m.publish_intent(&intent("b1")).await.unwrap();
        assert_eq!(m.list_active().await.unwrap().len(), 1);
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
            j.publish_intent(&intent("b1")).await.unwrap();
            j.claim_attempt(&claim("b1", 1, "mesh1.admin.1")).await.unwrap();
            j.append_step_receipt(&step("b1", 1, "restart-node:mesh1.rpc.2")).await.unwrap();
            j.publish_intent(&intent("b2")).await.unwrap();
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
        std::fs::write(dir.join(JOURNAL_FILE), format!("{}\nnot json\n", serde_json::to_string(&BuildFact::Intent(intent("b1"))).unwrap())).unwrap();
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
                } else if p.extension().is_some_and(|x| x == "rs") && !p.ends_with("build_state.rs") {
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
