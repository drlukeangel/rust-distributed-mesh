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
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleScope {
    Node,
    Mesh,
    Fabric,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LifecycleState {
    Pending,
    ReadyForTraffic,
    Draining,
    Leaving,
    Dead,
    Retired,
}

/// Which transition a hook attaches to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransitionKey {
    pub scope: LifecycleScope,
    pub from: LifecycleState,
    pub to: LifecycleState,
}

/// The semantic cuts (mesh-control-plane.md §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookPhase {
    BeforeEligibility,
    BeforeDrain,
    AfterEligibilityBeforeCommit,
    AfterTransition,
    AfterDrain,
}

impl HookPhase {
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
    pub desired_meshes: u32,
}

/// When a hook applies. Topology-derived gates come from desired state;
/// explicit predicates exist for genuinely shape-specific hooks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapePredicate {
    Always,
    SingleMesh,
    MultiMesh,
    MinMeshes(u32),
}

impl ShapePredicate {
    pub fn holds(&self, shape: &ShapeFacts) -> bool {
        match self {
            Self::Always => true,
            Self::SingleMesh => shape.desired_meshes == 1,
            Self::MultiMesh => shape.desired_meshes >= 2,
            Self::MinMeshes(n) => shape.desired_meshes >= *n,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LifecycleHookSpec {
    pub hook_id: String,
    pub transition: TransitionKey,
    pub phase: HookPhase,
    pub applies_when: ShapePredicate,
    /// A failed blocking hook prevents the commit.
    pub blocking: bool,
    /// Attempts per transition run before the hook counts as failed.
    pub max_attempts: u32,
}

/// What a hook sees.
#[derive(Debug, Clone)]
pub struct HookContext {
    pub transition_id: String,
    pub target: String,
    pub key: TransitionKey,
    pub phase: HookPhase,
    pub attempt: u32,
}

#[async_trait]
pub trait LifecycleHook: Send + Sync {
    /// Idempotent work for one transition; `Err` names what failed.
    async fn run(&self, ctx: &HookContext) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookOutcome {
    Complete,
    Failed { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookReceipt {
    pub transition_id: String,
    pub hook_id: String,
    pub phase: HookPhase,
    pub attempt: u32,
    pub outcome: HookOutcome,
}

/// Where hook receipts are appended.
pub trait ReceiptLog: Send + Sync {
    fn append(&self, r: HookReceipt);
    fn receipts(&self, transition_id: &str) -> Vec<HookReceipt>;
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    DuplicateHook(String),
    ZeroAttempts(String),
    /// A drain cut on a transition that does not enter `Draining`.
    DrainPhaseOffDrain { hook_id: String },
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

#[derive(Default)]
pub struct HookRegistry {
    hooks: Vec<(LifecycleHookSpec, Arc<dyn LifecycleHook>)>,
}

impl HookRegistry {
    pub fn new() -> Self {
        Self::default()
    }

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
    NotEligible { reason: String },
    /// A blocking hook failed; the target stays in `from`.
    HookFailed { hook_id: String, phase: HookPhase, reason: String },
}

/// One transition request.
pub struct Transition<'a> {
    pub transition_id: String,
    pub target: String,
    pub key: TransitionKey,
    pub shape: &'a ShapeFacts,
}

impl Transition<'_> {
    /// The stable id a retry reuses: `<scope>:<target>:<from>-><to>`.
    pub fn id_for(key: TransitionKey, target: &str) -> String {
        format!("{:?}:{target}:{:?}->{:?}", key.scope, key.from, key.to).to_ascii_lowercase()
    }
}

pub struct LifecycleTransitionPipeline {
    hooks: SealedHooks,
    receipts: Arc<dyn ReceiptLog>,
}

impl LifecycleTransitionPipeline {
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
                let r = {
                    let _g = span.enter();
                    hook.run(&ctx).await
                };
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
        let _g = span.enter();
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
