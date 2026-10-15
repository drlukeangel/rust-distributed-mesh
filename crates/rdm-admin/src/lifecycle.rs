//! `LifecycleTransitionPipeline` (PRD §1.10–11, §9; mesh-control-plane.md §7).
//!
//! Node, Mesh and Fabric status changes are explicit transitions with
//! registered hooks, never incidental field writes:
//!
//! ```text
//! transition(scope, from, to)
//!   -> BeforeEligibility hooks          (BeforeDrain when entering Draining)
//!   -> evaluate desired/observed eligibility
//!   -> AfterEligibilityBeforeCommit hooks
//!   -> commit                           (only when every required hook is satisfied)
//!   -> AfterTransition hooks            (AfterDrain when entering Draining)
//! ```
//!
//! Hooks are registered before the registry seals. Each run appends a receipt
//! keyed by `(transition_id, hook_id)`; a retry of the same transition reuses
//! every `Complete` receipt instead of re-running the hook. A failed blocking
//! hook leaves the target in its old state, never pretending it reached the
//! new one.

use async_trait::async_trait;
use tracing::Instrument as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex};

/// What a transition changes the status of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleScope {
    /// A node.
    Node,
    /// A mesh.
    Mesh,
    /// The fabric.
    Fabric,
}

/// A node, mesh or fabric status a transition moves between.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LifecycleState {
    /// Created and not yet ready for traffic.
    Pending,
    /// Ready: it takes traffic.
    ReadyForTraffic,
    /// Draining: it takes no new work.
    Draining,
    /// Leaving: it has announced its departure.
    Leaving,
    /// Dead: it is gone.
    Dead,
    /// Retired: it has been taken out of service.
    Retired,
}

/// Which transition a hook attaches to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransitionKey {
    /// The scope of the status.
    pub scope: LifecycleScope,
    /// The state the status moves from.
    pub from: LifecycleState,
    /// The state the status moves to.
    pub to: LifecycleState,
}

/// The semantic cuts (mesh-control-plane.md §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookPhase {
    /// Before the transition's eligibility is evaluated.
    BeforeEligibility,
    /// Before eligibility, when the transition enters `Draining`.
    BeforeDrain,
    /// After eligibility holds and before the commit.
    AfterEligibilityBeforeCommit,
    /// After the commit.
    AfterTransition,
    /// After the commit, when the transition entered `Draining`.
    AfterDrain,
}

impl HookPhase {
    /// The phase's name as it appears in receipts and spans.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BeforeEligibility => "before_eligibility",
            Self::BeforeDrain => "before_drain",
            Self::AfterEligibilityBeforeCommit => "after_eligibility_before_commit",
            Self::AfterTransition => "after_transition",
            Self::AfterDrain => "after_drain",
        }
    }
}

/// The fabric's desired shape, as hook predicates see it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShapeFacts {
    /// The number of meshes the fabric's desired state names.
    pub desired_meshes: u32,
}

/// When a hook applies. Topology-derived gates come from desired state;
/// explicit predicates exist for genuinely shape-specific hooks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapePredicate {
    /// Every shape.
    Always,
    /// A fabric of exactly one mesh.
    SingleMesh,
    /// A fabric of more than one mesh.
    MultiMesh,
    /// A fabric of at least this many meshes.
    MinMeshes(u32),
}

impl ShapePredicate {
    /// Whether the predicate holds for `shape`.
    pub fn holds(&self, shape: &ShapeFacts) -> bool {
        match self {
            Self::Always => true,
            Self::SingleMesh => shape.desired_meshes == 1,
            Self::MultiMesh => shape.desired_meshes >= 2,
            Self::MinMeshes(n) => shape.desired_meshes >= *n,
        }
    }
}

/// A registered hook: where it runs and how it behaves.
#[derive(Debug, Clone)]
pub struct LifecycleHookSpec {
    /// The hook's id, unique in the registry.
    pub hook_id: String,
    /// The transition the hook attaches to.
    pub transition: TransitionKey,
    /// The phase the hook runs at.
    pub phase: HookPhase,
    /// When the hook applies.
    pub applies_when: ShapePredicate,
    /// A failed blocking hook prevents the commit.
    pub blocking: bool,
    /// Attempts per transition run before the hook counts as failed.
    pub max_attempts: u32,
}

/// What a hook sees.
#[derive(Debug, Clone)]
pub struct HookContext {
    /// The transition's stable id.
    pub transition_id: String,
    /// The node, mesh or fabric the transition is for.
    pub target: String,
    /// The transition.
    pub key: TransitionKey,
    /// The phase the hook runs at.
    pub phase: HookPhase,
    /// Which attempt of the hook this is.
    pub attempt: u32,
}

/// Work a hook does for one transition.
#[async_trait]
pub trait LifecycleHook: Send + Sync {
    /// Idempotent work for one transition; `Err` names what failed.
    async fn run(&self, ctx: &HookContext) -> Result<(), String>;
}

/// How a hook run ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookOutcome {
    /// The hook completed.
    Complete,
    /// The hook failed.
    Failed {
        /// Why it failed.
        reason: String,
    },
}

/// The receipt of one hook run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookReceipt {
    /// The transition the run belongs to.
    pub transition_id: String,
    /// The hook.
    pub hook_id: String,
    /// The phase.
    pub phase: HookPhase,
    /// The attempt.
    pub attempt: u32,
    /// How the run ended.
    pub outcome: HookOutcome,
}

/// Where hook receipts are appended.
pub trait ReceiptLog: Send + Sync {
    /// Append a receipt.
    fn append(&self, r: HookReceipt);
    /// The receipts of `transition_id`.
    fn receipts(&self, transition_id: &str) -> Vec<HookReceipt>;
}

/// A receipt log held in memory.
#[derive(Debug, Default)]
pub struct MemoryReceiptLog(Mutex<Vec<HookReceipt>>);

impl ReceiptLog for MemoryReceiptLog {
    fn append(&self, r: HookReceipt) {
        self.0.lock().unwrap().push(r);
    }
    fn receipts(&self, transition_id: &str) -> Vec<HookReceipt> {
        self.0.lock().unwrap().iter().filter(|r| r.transition_id == transition_id).cloned().collect()
    }
}

/// Why a hook registry does not seal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    /// Two hooks share an id.
    DuplicateHook(String),
    /// A hook allows zero attempts.
    ZeroAttempts(String),
    /// A drain cut on a transition that does not enter `Draining`.
    DrainPhaseOffDrain {
        /// The hook.
        hook_id: String,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateHook(h) => write!(f, "hook `{h}` registered twice"),
            Self::ZeroAttempts(h) => write!(f, "hook `{h}` allows zero attempts"),
            Self::DrainPhaseOffDrain { hook_id } => write!(f, "hook `{hook_id}`: drain phases only attach to a transition into Draining"),
        }
    }
}

/// The hooks registered before the registry seals.
#[derive(Default)]
pub struct HookRegistry {
    hooks: Vec<(LifecycleHookSpec, Arc<dyn LifecycleHook>)>,
}

impl HookRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `hook` under `spec`.
    pub fn register(mut self, spec: LifecycleHookSpec, hook: Arc<dyn LifecycleHook>) -> Self {
        self.hooks.push((spec, hook));
        self
    }

    /// Validate and seal; nothing registers afterwards.
    pub fn seal(self) -> Result<SealedHooks, Vec<RegistryError>> {
        let mut errors = Vec::new();
        let mut ids = BTreeSet::new();
        for (s, _) in &self.hooks {
            if !ids.insert(s.hook_id.clone()) {
                errors.push(RegistryError::DuplicateHook(s.hook_id.clone()));
            }
            if s.max_attempts == 0 {
                errors.push(RegistryError::ZeroAttempts(s.hook_id.clone()));
            }
            if matches!(s.phase, HookPhase::BeforeDrain | HookPhase::AfterDrain) && s.transition.to != LifecycleState::Draining {
                errors.push(RegistryError::DrainPhaseOffDrain { hook_id: s.hook_id.clone() });
            }
        }
        if errors.is_empty() {
            Ok(SealedHooks { hooks: self.hooks })
        } else {
            Err(errors)
        }
    }
}

/// The sealed registry. Hook order is deterministic: by phase, then
/// registration order.
pub struct SealedHooks {
    hooks: Vec<(LifecycleHookSpec, Arc<dyn LifecycleHook>)>,
}

impl SealedHooks {
    fn for_phase(&self, key: TransitionKey, phase: HookPhase, shape: &ShapeFacts) -> Vec<&(LifecycleHookSpec, Arc<dyn LifecycleHook>)> {
        self.hooks
            .iter()
            .filter(|(s, _)| s.transition == key && s.phase == phase && s.applies_when.holds(shape))
            .collect()
    }
}

/// The result of one transition run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionResult {
    /// The new state was committed.
    Committed,
    /// Eligibility does not hold yet; nothing committed.
    NotEligible {
        /// Why the transition is not eligible.
        reason: String,
    },
    /// A blocking hook failed; the target stays in `from`.
    HookFailed {
        /// The failed hook.
        hook_id: String,
        /// The phase it failed at.
        phase: HookPhase,
        /// Why it failed.
        reason: String,
    },
}

/// One transition request.
pub struct Transition<'a> {
    /// The transition's stable id.
    pub transition_id: String,
    /// The node, mesh or fabric the transition is for.
    pub target: String,
    /// The transition.
    pub key: TransitionKey,
    /// The desired shape the hooks' predicates see.
    pub shape: &'a ShapeFacts,
}

impl Transition<'_> {
    /// The stable id a retry reuses: `<scope>:<target>:<from>-><to>`.
    pub fn id_for(key: TransitionKey, target: &str) -> String {
        format!("{:?}:{target}:{:?}->{:?}", key.scope, key.from, key.to).to_ascii_lowercase()
    }

    /// The id of a node's transition for ONE birth: its path (the target) and the exact incarnation.
    /// A transition id keys once-only hook receipts, and a re-birth at the same path is a new
    /// execution of the transition: its hooks run again. A retry of the same birth reuses its receipts.
    pub(crate) fn id_for_birth(key: TransitionKey, path: &str, incarnation: &str) -> String {
        format!("{}@{incarnation}", Self::id_for(key, path))
    }
}

/// Runs a transition through its hooks, its eligibility check and its commit.
pub struct LifecycleTransitionPipeline {
    hooks: SealedHooks,
    receipts: Arc<dyn ReceiptLog>,
}

impl LifecycleTransitionPipeline {
    /// A pipeline over the sealed `hooks` that appends its receipts to `receipts`.
    pub fn new(hooks: SealedHooks, receipts: Arc<dyn ReceiptLog>) -> Self {
        Self { hooks, receipts }
    }

    async fn run_phase(&self, t: &Transition<'_>, phase: HookPhase) -> Result<(), (String, String)> {
        let done = self.receipts.receipts(&t.transition_id);
        for (spec, hook) in self.hooks.for_phase(t.key, phase, t.shape) {
            if done.iter().any(|r| r.hook_id == spec.hook_id && r.outcome == HookOutcome::Complete) {
                continue; // a retry reuses the receipt
            }
            let prior = done.iter().filter(|r| r.hook_id == spec.hook_id).map(|r| r.attempt).max().unwrap_or(0);
            let mut last_err = String::new();
            let mut ok = false;
            for n in 1..=spec.max_attempts {
                let attempt = prior + n;
                let ctx = HookContext {
                    transition_id: t.transition_id.clone(),
                    target: t.target.clone(),
                    key: t.key,
                    phase,
                    attempt,
                };
                let span = tracing::info_span!(
                    "rdm.node_admin.lifecycle_hook.update.via-transition",
                    hook_id = %spec.hook_id,
                    phase = phase.as_str(),
                    transition_id = %t.transition_id,
                    blocking = spec.blocking,
                    attempt,
                    outcome = tracing::field::Empty,
                );
                let r = hook.run(&ctx).instrument(span.clone()).await;
                let outcome = match &r {
                    Ok(()) => HookOutcome::Complete,
                    Err(e) => HookOutcome::Failed { reason: e.clone() },
                };
                span.record("outcome", if r.is_ok() { "complete" } else { "failed" });
                self.receipts.append(HookReceipt {
                    transition_id: t.transition_id.clone(),
                    hook_id: spec.hook_id.clone(),
                    phase,
                    attempt,
                    outcome,
                });
                match r {
                    Ok(()) => {
                        ok = true;
                        break;
                    }
                    Err(e) => last_err = e,
                }
            }
            if !ok && spec.blocking {
                return Err((spec.hook_id.clone(), last_err));
            }
        }
        Ok(())
    }

    /// Run one transition. `eligible` evaluates desired/observed eligibility;
    /// `commit` writes the new state and runs only when every required hook is
    /// satisfied.
    pub async fn transition<E, C>(&self, t: Transition<'_>, eligible: E, commit: C) -> TransitionResult
    where
        E: FnOnce() -> Result<(), String>,
        C: FnOnce(),
    {
        let span = tracing::info_span!(
            "rdm.node_admin.lifecycle.update.via-transition",
            transition_id = %t.transition_id,
            target = %t.target,
            from = ?t.key.from,
            to = ?t.key.to,
        );
        // The span is entered by the future, on each poll, never by a guard held across an await: a
        // guard stays entered on the worker thread while the task is parked, and the next span any
        // task creates there takes the closed span as its parent.
        async move {
            let draining = t.key.to == LifecycleState::Draining;
            let pre = if draining { [HookPhase::BeforeEligibility, HookPhase::BeforeDrain].as_slice() } else { &[HookPhase::BeforeEligibility] };
            for &phase in pre {
                if let Err((hook_id, reason)) = self.run_phase(&t, phase).await {
                    return TransitionResult::HookFailed { hook_id, phase, reason };
                }
            }
            if let Err(reason) = eligible() {
                return TransitionResult::NotEligible { reason };
            }
            if let Err((hook_id, reason)) = self.run_phase(&t, HookPhase::AfterEligibilityBeforeCommit).await {
                return TransitionResult::HookFailed { hook_id, phase: HookPhase::AfterEligibilityBeforeCommit, reason };
            }
            commit();
            let post = if draining { [HookPhase::AfterTransition, HookPhase::AfterDrain].as_slice() } else { &[HookPhase::AfterTransition] };
            for &phase in post {
                // The state is committed; a failing post hook leaves a Failed receipt to retry, not a lie about state.
                let _ = self.run_phase(&t, phase).await;
            }
            TransitionResult::Committed
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    const READY: TransitionKey = TransitionKey { scope: LifecycleScope::Fabric, from: LifecycleState::Pending, to: LifecycleState::ReadyForTraffic };
    const DRAIN: TransitionKey = TransitionKey { scope: LifecycleScope::Node, from: LifecycleState::ReadyForTraffic, to: LifecycleState::Draining };

    /// Records its name into a shared call log; fails while `fail_left > 0`.
    struct Probe {
        name: &'static str,
        log: Arc<Mutex<Vec<&'static str>>>,
        fail_left: AtomicU32,
        runs: AtomicU32,
    }

    #[async_trait]
    impl LifecycleHook for Probe {
        async fn run(&self, _: &HookContext) -> Result<(), String> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            self.log.lock().unwrap().push(self.name);
            if self.fail_left.load(Ordering::SeqCst) > 0 {
                self.fail_left.fetch_sub(1, Ordering::SeqCst);
                return Err(format!("{} not yet", self.name));
            }
            Ok(())
        }
    }

    fn probe(name: &'static str, log: &Arc<Mutex<Vec<&'static str>>>, fails: u32) -> Arc<Probe> {
        Arc::new(Probe { name, log: log.clone(), fail_left: AtomicU32::new(fails), runs: AtomicU32::new(0) })
    }

    fn spec(id: &str, key: TransitionKey, phase: HookPhase, blocking: bool) -> LifecycleHookSpec {
        LifecycleHookSpec { hook_id: id.into(), transition: key, phase, applies_when: ShapePredicate::Always, blocking, max_attempts: 1 }
    }

    fn run<'a>(key: TransitionKey, shape: &'a ShapeFacts) -> Transition<'a> {
        Transition { transition_id: Transition::id_for(key, "fabric1"), target: "fabric1".into(), key, shape }
    }

    #[tokio::test]
    async fn a_failing_blocking_hook_prevents_the_commit() {
        let log = Arc::new(Mutex::new(vec![]));
        let hooks = HookRegistry::new()
            .register(spec("control-plane", READY, HookPhase::AfterEligibilityBeforeCommit, true), probe("control-plane", &log, 0))
            .register(spec("app-bootstrap", READY, HookPhase::AfterEligibilityBeforeCommit, true), probe("app-bootstrap", &log, 1))
            .seal()
            .unwrap();
        let receipts = Arc::new(MemoryReceiptLog::default());
        let p = LifecycleTransitionPipeline::new(hooks, receipts.clone());
        let shape = ShapeFacts { desired_meshes: 1 };
        let mut committed = false;
        let r = p.transition(run(READY, &shape), || Ok(()), || committed = true).await;
        assert_eq!(
            r,
            TransitionResult::HookFailed { hook_id: "app-bootstrap".into(), phase: HookPhase::AfterEligibilityBeforeCommit, reason: "app-bootstrap not yet".into() }
        );
        assert!(!committed, "the Fabric does not lie and report ready");
        let rs = receipts.receipts(&Transition::id_for(READY, "fabric1"));
        assert_eq!(rs.iter().map(|r| (r.hook_id.as_str(), r.outcome == HookOutcome::Complete)).collect::<Vec<_>>(), vec![("control-plane", true), ("app-bootstrap", false)]);
    }

    #[tokio::test]
    async fn a_retry_reuses_receipts_and_reruns_only_the_failed_hook() {
        let log = Arc::new(Mutex::new(vec![]));
        let first = probe("control-plane", &log, 0);
        let second = probe("app-bootstrap", &log, 1);
        let hooks = HookRegistry::new()
            .register(spec("control-plane", READY, HookPhase::AfterEligibilityBeforeCommit, true), first.clone())
            .register(spec("app-bootstrap", READY, HookPhase::AfterEligibilityBeforeCommit, true), second.clone())
            .seal()
            .unwrap();
        let receipts = Arc::new(MemoryReceiptLog::default());
        let p = LifecycleTransitionPipeline::new(hooks, receipts.clone());
        let shape = ShapeFacts { desired_meshes: 1 };
        let mut committed = 0;
        let _ = p.transition(run(READY, &shape), || Ok(()), || committed += 1).await;
        let r = p.transition(run(READY, &shape), || Ok(()), || committed += 1).await;
        assert_eq!(r, TransitionResult::Committed);
        assert_eq!(committed, 1);
        assert_eq!(first.runs.load(Ordering::SeqCst), 1, "the completed hook's receipt was reused");
        assert_eq!(second.runs.load(Ordering::SeqCst), 2);
        let last = receipts.receipts(&Transition::id_for(READY, "fabric1")).last().cloned().unwrap();
        assert_eq!((last.hook_id.as_str(), last.attempt, last.outcome), ("app-bootstrap", 2, HookOutcome::Complete));
    }

    #[tokio::test]
    async fn hook_order_is_deterministic_by_phase_then_registration() {
        let log = Arc::new(Mutex::new(vec![]));
        let hooks = HookRegistry::new()
            .register(spec("after-b", DRAIN, HookPhase::AfterDrain, false), probe("after-drain", &log, 0))
            .register(spec("post", DRAIN, HookPhase::AfterTransition, false), probe("after-transition", &log, 0))
            .register(spec("commit-2", DRAIN, HookPhase::AfterEligibilityBeforeCommit, true), probe("pre-commit-2", &log, 0))
            .register(spec("commit-1", DRAIN, HookPhase::AfterEligibilityBeforeCommit, true), probe("pre-commit-1", &log, 0))
            .register(spec("drain", DRAIN, HookPhase::BeforeDrain, true), probe("before-drain", &log, 0))
            .register(spec("elig", DRAIN, HookPhase::BeforeEligibility, true), probe("before-eligibility", &log, 0))
            .seal()
            .unwrap();
        let shape = ShapeFacts { desired_meshes: 1 };
        let mut order_at_commit = Vec::new();
        for i in 0..2 {
            log.lock().unwrap().clear();
            // A fresh receipt log per run, so nothing is reused between the two runs.
            let pipeline = LifecycleTransitionPipeline::new(hooks_clone(&hooks), Arc::new(MemoryReceiptLog::default()));
            let l = log.clone();
            let r = pipeline
                .transition(
                    Transition { transition_id: format!("t{i}"), target: "mesh1.rpc.1".into(), key: DRAIN, shape: &shape },
                    || Ok(()),
                    || l.lock().unwrap().push("COMMIT"),
                )
                .await;
            assert_eq!(r, TransitionResult::Committed);
            order_at_commit.push(log.lock().unwrap().clone());
        }
        assert_eq!(
            order_at_commit[0],
            vec!["before-eligibility", "before-drain", "pre-commit-2", "pre-commit-1", "COMMIT", "after-transition", "after-drain"]
        );
        assert_eq!(order_at_commit[0], order_at_commit[1]);
    }

    fn hooks_clone(h: &SealedHooks) -> SealedHooks {
        SealedHooks { hooks: h.hooks.iter().map(|(s, k)| (s.clone(), k.clone())).collect() }
    }

    /// CONTRACT: a node's Pending -> ReadyForTraffic transition has one id per birth. A retry of
    /// the same birth keeps its id (so its Complete hook receipts are reused); a re-birth at the
    /// same path is another execution (so its hooks run). Must NOT happen: two births of one path
    /// sharing a transition id, which would skip the second birth's hooks.
    #[test]
    fn a_re_birth_at_the_same_path_is_another_transition_execution() {
        let key = TransitionKey { scope: LifecycleScope::Node, from: LifecycleState::Pending, to: LifecycleState::ReadyForTraffic };
        let first = Transition::id_for_birth(key, "mesh1.rpc.1", "inc-a");
        assert_eq!(first, Transition::id_for_birth(key, "mesh1.rpc.1", "inc-a"), "a retry of one birth keeps its id");
        assert_ne!(first, Transition::id_for_birth(key, "mesh1.rpc.1", "inc-b"), "a re-birth at the path is another execution");
        assert_ne!(first, Transition::id_for_birth(key, "mesh1.rpc.2", "inc-a"));
        assert!(first.starts_with(&Transition::id_for(key, "mesh1.rpc.1")), "{first}");
    }

    #[tokio::test]
    async fn ineligible_targets_do_not_commit_or_run_pre_commit_hooks() {
        let log = Arc::new(Mutex::new(vec![]));
        let hooks = HookRegistry::new()
            .register(spec("pre", READY, HookPhase::AfterEligibilityBeforeCommit, true), probe("pre", &log, 0))
            .seal()
            .unwrap();
        let p = LifecycleTransitionPipeline::new(hooks, Arc::new(MemoryReceiptLog::default()));
        let shape = ShapeFacts { desired_meshes: 2 };
        let mut committed = false;
        let r = p.transition(run(READY, &shape), || Err("mesh2 is not ready-for-traffic".into()), || committed = true).await;
        assert_eq!(r, TransitionResult::NotEligible { reason: "mesh2 is not ready-for-traffic".into() });
        assert!(!committed && log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_non_blocking_failure_is_recorded_but_does_not_block() {
        let log = Arc::new(Mutex::new(vec![]));
        let hooks = HookRegistry::new()
            .register(spec("advisory", READY, HookPhase::AfterEligibilityBeforeCommit, false), probe("advisory", &log, 5))
            .seal()
            .unwrap();
        let receipts = Arc::new(MemoryReceiptLog::default());
        let p = LifecycleTransitionPipeline::new(hooks, receipts.clone());
        let shape = ShapeFacts { desired_meshes: 1 };
        let mut committed = false;
        assert_eq!(p.transition(run(READY, &shape), || Ok(()), || committed = true).await, TransitionResult::Committed);
        assert!(committed);
        assert!(matches!(receipts.receipts(&Transition::id_for(READY, "fabric1"))[0].outcome, HookOutcome::Failed { .. }));
    }

    #[tokio::test]
    async fn retry_policy_allows_bounded_attempts_within_one_run() {
        let log = Arc::new(Mutex::new(vec![]));
        let flaky = probe("flaky", &log, 2);
        let mut s = spec("flaky", READY, HookPhase::BeforeEligibility, true);
        s.max_attempts = 3;
        let p = LifecycleTransitionPipeline::new(HookRegistry::new().register(s, flaky.clone()).seal().unwrap(), Arc::new(MemoryReceiptLog::default()));
        let shape = ShapeFacts { desired_meshes: 1 };
        assert_eq!(p.transition(run(READY, &shape), || Ok(()), || ()).await, TransitionResult::Committed);
        assert_eq!(flaky.runs.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn the_registry_refuses_bad_registrations_by_name() {
        let log = Arc::new(Mutex::new(vec![]));
        let mut zero = spec("z", READY, HookPhase::BeforeEligibility, true);
        zero.max_attempts = 0;
        let errs = HookRegistry::new()
            .register(spec("a", READY, HookPhase::BeforeEligibility, true), probe("a", &log, 0))
            .register(spec("a", READY, HookPhase::AfterTransition, true), probe("a", &log, 0))
            .register(zero, probe("z", &log, 0))
            .register(spec("d", READY, HookPhase::BeforeDrain, true), probe("d", &log, 0))
            .seal()
            .err()
            .unwrap();
        assert_eq!(
            errs,
            vec![
                RegistryError::DuplicateHook("a".into()),
                RegistryError::ZeroAttempts("z".into()),
                RegistryError::DrainPhaseOffDrain { hook_id: "d".into() },
            ]
        );
    }
}
