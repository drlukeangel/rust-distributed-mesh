//! The Build executor (PRD §1.2, §1.4, §7; mesh-control-plane.md §1, §3).
//!
//! A Build is not owned by the admin that accepted it. Whichever node-admin
//! executes takes the next attempt by an insert-and-fail claim, re-reads the
//! Build's pinned intent and the observed topology, plans what is left and
//! runs it; it never resumes from a saved instruction pointer and never
//! mints a new build id. A successor after the executor's loss does exactly
//! the same from its own fabric projection of the Build.

use crate::build::{plan, BuildId, BuildOperation};
use crate::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildProjection, BuildState, BuildStateAdapter, ClaimOutcome};
use crate::topology::Topology;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Realises one planned operation (the deployment and lifecycle pipelines).
#[async_trait::async_trait]
pub trait OperationRunner: Send + Sync {
    async fn run(&self, build_id: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String>;
}

/// How one reconcile of one Build ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciled {
    /// Every remaining operation ran: the Build is complete.
    Converged { attempt: u32, operations: Vec<BuildOperation> },
    /// The attempt stopped at an operation; a later attempt continues.
    Failed { attempt: u32, reason: String },
    /// Another executor holds the attempt this one tried to claim.
    Lost { attempt: u32, holder: String },
    /// The Build is already complete: nothing to do.
    Finished,
}

pub struct BuildExecutor {
    /// This node-admin's path.name: the claimant on every attempt it takes.
    pub executor: String,
    pub builds: Arc<dyn BuildStateAdapter>,
    pub topology: Arc<RwLock<Topology>>,
    pub runner: Arc<dyn OperationRunner>,
}

impl BuildExecutor {
    /// Continue every active Build in this admin's projection.
    pub async fn reconcile_active(&self) -> Vec<(BuildId, Reconciled)> {
        let active = match self.builds.list_active().await {
            Ok(a) => a,
            Err(e) => {
                tracing::info!(error = %e, "Build state unreadable; nothing reconciled");
                return Vec::new();
            }
        };
        let mut out = Vec::new();
        for b in active {
            let r = self.reconcile(&b).await;
            out.push((b.build_id, r));
        }
        out
    }

    /// Claim the next attempt of `build`, plan what is left and run it.
    pub async fn reconcile(&self, build: &BuildProjection) -> Reconciled {
        use tracing::Instrument;
        let attempt = build.attempt + 1;
        let span = tracing::info_span!(
            "rafka.node_admin.build.update.via-reconcile",
            build_id = %build.build_id,
            attempt,
            executor = %self.executor,
            previous_executor = build.executor.as_deref().unwrap_or(""),
            operations = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let r = self.reconcile_attempt(build, attempt, &span).instrument(span.clone()).await;
        span.record(
            "outcome",
            match &r {
                Reconciled::Converged { .. } => "converged",
                Reconciled::Failed { .. } => "failed",
                Reconciled::Lost { .. } => "lost",
                Reconciled::Finished => "finished",
            },
        );
        r
    }

    async fn reconcile_attempt(&self, build: &BuildProjection, attempt: u32, span: &tracing::Span) -> Reconciled {
        // A failed attempt does not end a Build: a later attempt continues it.
        if build.state == BuildState::Complete {
            return Reconciled::Finished;
        }
        let claim = BuildAttemptClaim { build_id: build.build_id.clone(), attempt, executor: self.executor.clone() };
        match self.builds.claim_attempt(&claim).await {
            Ok(ClaimOutcome::Won) => {}
            Ok(ClaimOutcome::Lost { holder }) => return Reconciled::Lost { attempt, holder },
            Err(e) => return Reconciled::Failed { attempt, reason: format!("claiming attempt {attempt}: {e}") },
        }
        // Desired is the pinned intent; observed is read now, never remembered.
        let observed = self.topology.read().await.clone();
        let operations = match plan(&build.intent, &observed) {
            Ok(p) => p.operations,
            Err(reject) => return self.finish(build, attempt, Err(format!("re-plan refused: {reject}"))).await,
        };
        span.record("operations", operations.iter().map(BuildOperation::key).collect::<Vec<_>>().join(",").as_str());
        for op in &operations {
            if let Err(e) = self.runner.run(&build.build_id, attempt, op).await {
                return self.finish(build, attempt, Err(format!("{}: {e}", op.key()))).await;
            }
        }
        self.finish(build, attempt, Ok(operations)).await
    }

    async fn finish(&self, build: &BuildProjection, attempt: u32, r: Result<Vec<BuildOperation>, String>) -> Reconciled {
        let outcome = match &r {
            Ok(_) => AttemptOutcome::Converged,
            Err(reason) => AttemptOutcome::Failed { reason: reason.clone() },
        };
        let receipt = BuildAttemptReceipt { build_id: build.build_id.clone(), attempt, outcome };
        if let Err(e) = self.builds.append_attempt_receipt(&receipt).await {
            return Reconciled::Failed { attempt, reason: format!("recording attempt {attempt}: {e}") };
        }
        match r {
            Ok(operations) => Reconciled::Converged { attempt, operations },
            Err(reason) => Reconciled::Failed { attempt, reason },
        }
    }
}
