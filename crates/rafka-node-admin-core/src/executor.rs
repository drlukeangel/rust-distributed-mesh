//! The Build executor (PRD §1.2, §1.4, §7; mesh-control-plane.md §1, §3).
//!
//! A Build is not owned by the admin that accepted it. Whichever node-admin
//! executes takes the next attempt by an insert-and-fail claim, re-reads the
//! Build's pinned intent and the observed topology, plans what is left and
//! runs it; it never resumes from a saved instruction pointer and never
//! mints a new build id. A successor after the executor's loss does exactly
//! the same from its own fabric projection of the Build.
//!
//! Which admin executes an operation (PRD §12.1, `executor_for`): a mesh's
//! admin primary runs the operations on that mesh's members; the fabric
//! primary runs everything else (mesh creation and retirement, every
//! node-admin cohort) and a mesh's members while that mesh has no admin
//! primary. An admin claims a Build's next attempt only when it executes the
//! first operation left; an attempt that reaches an operation another admin
//! executes ends `HandedOff`, and that admin claims the next attempt of the
//! same Build.

use crate::build::{plan, BuildId, BuildOperation};
use crate::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildProjection, BuildState, BuildStateAdapter, ClaimOutcome};
use crate::model::{NodeKind, PathName};
use crate::topology::Topology;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Realises one planned operation (the deployment and lifecycle pipelines).
#[async_trait::async_trait]
pub trait OperationRunner: Send + Sync {
    async fn run(&self, build_id: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String>;
}

/// The admin that executes `op` in view `t`; `None` while no admin can.
pub fn executor_for(op: &BuildOperation, t: &Topology) -> Option<PathName> {
    let fabric = || t.fabric_primary().map(|n| n.name.clone());
    match op {
        BuildOperation::CreateNode { node } | BuildOperation::RestartNode { node } | BuildOperation::RetireNode { node, .. }
            if node.kind == NodeKind::RpcNode =>
        {
            t.cohort_primary(&node.mesh, NodeKind::NodeAdmin).map(|n| n.name.clone()).or_else(fabric)
        }
        // A whole-mesh retire runs outside the mesh it removes: an executor inside M cannot see a
        // member's departure leave M, and would retire itself mid-plan. The fabric primary keeps
        // authority; when it sits in M, execution goes to the lowest-NodeId ready mesh-primary
        // admin of another mesh (the Build's ordinary hand-off). No such admin: none.
        BuildOperation::RetireMesh { mesh } => match t.fabric_primary() {
            Some(fp) if fp.mesh != *mesh => Some(fp.name.clone()),
            _ => retire_mesh_executor_outside(mesh, t),
        },
        _ => fabric(),
    }
}

/// The admin that executes `retire-mesh:mesh` when the fabric primary is inside `mesh`: the
/// lowest-NodeId ready-for-traffic admin primary of another mesh.
fn retire_mesh_executor_outside(mesh: &str, t: &Topology) -> Option<PathName> {
    t.nodes
        .iter()
        .filter(|n| n.kind == NodeKind::NodeAdmin && n.is_primary && n.mesh != mesh && n.status == crate::model::NodeStatus::ReadyForTraffic)
        .min_by(|a, b| a.node_id.as_str().cmp(b.node_id.as_str()))
        .map(|n| n.name.clone())
}

/// The admin that executes what is left of a plan: the first operation's
/// executor, or the fabric primary when nothing is left (it closes the Build).
pub fn lead_for(ops: &[BuildOperation], t: &Topology) -> Option<PathName> {
    match ops.first() {
        Some(op) => executor_for(op, t),
        None => t.fabric_primary().map(|n| n.name.clone()),
    }
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
    /// The attempt ran `operations` and stopped where admin `to` executes.
    HandedOff { attempt: u32, operations: Vec<BuildOperation>, to: String },
    /// The Build is already complete: nothing to do.
    Finished,
}

pub struct BuildExecutor {
    /// This node-admin's path.name: the claimant on every attempt it takes.
    pub executor: String,
    pub builds: Arc<dyn BuildStateAdapter>,
    pub topology: Arc<RwLock<Topology>>,
    pub runner: Arc<dyn OperationRunner>,
    /// This admin's desired topology: a Build whose desired revision lost a
    /// fork is refused, never executed.
    pub desired: Arc<crate::desired::DesiredStore>,
}

impl BuildExecutor {
    /// Continue every active Build in this admin's projection whose next
    /// operation this admin executes.
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
            // A Build waits for the desired revision it names to be heard.
            if b.desired.as_ref().is_some_and(|m| matches!(self.desired.standing(m), crate::desired::Standing::Ahead | crate::desired::Standing::Unhydrated)) {
                continue;
            }
            if !self.leads(&b).await {
                continue;
            }
            let r = self.reconcile(&b).await;
            out.push((b.build_id, r));
        }
        out
    }

    /// Does this admin execute what is left of `build` in its view now? A
    /// Build that no longer plans (its re-plan refuses) is closed by whoever
    /// would execute its first operation: the fabric primary.
    async fn leads(&self, build: &BuildProjection) -> bool {
        let t = self.topology.read().await;
        match build.state {
            BuildState::Complete => return false,
            // An attempt is in flight: only its holder's loss lets another
            // admin take the Build over (an admin runs its own attempts to the
            // end, so its own open attempt is one a previous birth left).
            BuildState::Running => {
                let holder_live = build.executor.as_deref().is_some_and(|h| {
                    h != self.executor && t.nodes.iter().any(|n| n.name.to_string() == h && n.status.is_live())
                });
                if holder_live {
                    return false;
                }
            }
            BuildState::Pending | BuildState::Failed => {}
        }
        let ops = plan(&build.intent, &t).map(|p| p.operations).unwrap_or_default();
        lead_for(&ops, &t).is_some_and(|l| l.to_string() == self.executor)
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
            desired_revision = build.desired.as_ref().map(|m| m.revision).unwrap_or(0),
            reason = build.reason.map(|r| r.as_str()).unwrap_or(""),
            operations = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        // The accepting span, on whichever admin took the request, is the parent.
        if let Some(tp) = &build.traceparent {
            rafka_telemetry::set_parent(&span, tp);
        }
        let r = self.reconcile_attempt(build, attempt, &span).instrument(span.clone()).await;
        span.record(
            "outcome",
            match &r {
                Reconciled::Converged { .. } => "converged",
                Reconciled::Failed { .. } => "failed",
                Reconciled::Lost { .. } => "lost",
                Reconciled::HandedOff { .. } => "handed-off",
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
        if let Some(mark) = build.desired.as_ref().filter(|m| self.desired.standing(m) == crate::desired::Standing::Lost) {
            let reason = format!("desired-revision-conflict: {mark} lost to a concurrent update from the same base; resubmit against the current revision");
            tracing::info_span!("rafka.node_admin.build.reject.via-desired-revision-conflict", build_id = %build.build_id, desired_revision = mark.revision, detail = %reason)
                .in_scope(|| tracing::info!("Build refused: its desired revision lost"));
            return self.finish(build, attempt, Err(reason)).await;
        }
        // Desired is the pinned intent; observed is read now, never remembered.
        let observed = self.topology.read().await.clone();
        let operations = match plan(&build.intent, &observed) {
            Ok(p) => p.operations,
            Err(reject) => return self.finish(build, attempt, Err(format!("re-plan refused: {reject}"))).await,
        };
        span.record("operations", operations.iter().map(BuildOperation::key).collect::<Vec<_>>().join(",").as_str());
        for (i, op) in operations.iter().enumerate() {
            // The view moves as operations run (a new mesh's admins become
            // its primary): eligibility is decided on the view as it is now.
            let to = executor_for(op, &*self.topology.read().await);
            if let Some(to) = to.filter(|to| to.to_string() != self.executor) {
                return self.hand_off(build, attempt, operations[..i].to_vec(), to.to_string()).await;
            }
            if let Err(e) = self.runner.run(&build.build_id, attempt, op).await {
                return self.finish(build, attempt, Err(format!("{}: {e}", op.key()))).await;
            }
        }
        self.finish(build, attempt, Ok(operations)).await
    }

    async fn hand_off(&self, build: &BuildProjection, attempt: u32, operations: Vec<BuildOperation>, to: String) -> Reconciled {
        let receipt = BuildAttemptReceipt { build_id: build.build_id.clone(), attempt, outcome: AttemptOutcome::HandedOff { to: to.clone() } };
        if let Err(e) = self.builds.append_attempt_receipt(&receipt).await {
            return Reconciled::Failed { attempt, reason: format!("recording attempt {attempt}: {e}") };
        }
        tracing::info!(to = %to, "the next operation belongs to another admin; handed off");
        Reconciled::HandedOff { attempt, operations, to }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};

    fn node(name: &str, primary: bool, fabric: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = NodeStatus::ReadyForTraffic;
        n.is_primary = primary;
        n.is_fabric_primary = fabric;
        n
    }

    fn mm(mesh2_admin: bool) -> Topology {
        let mut nodes = vec![node("mesh1.admin.1", true, true), node("mesh1.rpc.1", true, false)];
        if mesh2_admin {
            nodes.push(node("mesh2.admin.1", true, false));
        }
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic },
            ],
            nodes,
        }
    }

    fn create(n: &str) -> BuildOperation {
        BuildOperation::CreateNode { node: n.parse().unwrap() }
    }

    fn who(op: &BuildOperation, t: &Topology) -> String {
        executor_for(op, t).map(|p| p.to_string()).unwrap_or_default()
    }

    #[test]
    fn a_meshs_members_are_its_own_primarys_and_everything_else_the_fabric_primarys() {
        let t = mm(true);
        assert_eq!(who(&create("mesh2.rpc.1"), &t), "mesh2.admin.1");
        assert_eq!(who(&BuildOperation::RetireNode { node: "mesh2.rpc.1".parse().unwrap(), permanent: true }, &t), "mesh2.admin.1");
        assert_eq!(who(&create("mesh1.rpc.2"), &t), "mesh1.admin.1");
        assert_eq!(who(&create("mesh2.admin.2"), &t), "mesh1.admin.1", "admin cohorts are the fabric primary's");
        assert_eq!(who(&BuildOperation::CreateMesh { mesh: "mesh3".into() }, &t), "mesh1.admin.1");
        assert_eq!(who(&BuildOperation::RetireMesh { mesh: "mesh2".into() }, &t), "mesh1.admin.1");
    }

    /// CONTRACT (Luke 2026-10-05, mesh retire runs outside the mesh): `retire-mesh:M` runs on the
    /// fabric primary when it is outside M; when the fabric primary sits inside M, on the
    /// lowest-NodeId ready admin primary of another mesh; with no admin outside M, on none.
    #[test]
    fn a_mesh_retire_runs_outside_the_mesh_it_removes() {
        let t = mm(true);
        assert_eq!(who(&BuildOperation::RetireMesh { mesh: "mesh2".into() }, &t), "mesh1.admin.1", "the fabric primary, outside mesh2");
        let mut inside = mm(true);
        for n in inside.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh2.admin.1";
        }
        assert_eq!(who(&BuildOperation::RetireMesh { mesh: "mesh2".into() }, &inside), "mesh1.admin.1", "the fabric primary is in mesh2: another mesh's admin primary runs it");
        let mut alone = mm(true);
        alone.nodes.retain(|n| n.mesh == "mesh2");
        alone.nodes[0].is_fabric_primary = true;
        assert_eq!(who(&BuildOperation::RetireMesh { mesh: "mesh2".into() }, &alone), "", "no admin outside mesh2: no executor");
    }

    #[test]
    fn a_mesh_without_an_admin_primary_has_its_members_run_by_the_fabric_primary() {
        let t = mm(false);
        assert_eq!(who(&create("mesh2.rpc.1"), &t), "mesh1.admin.1");
        assert_eq!(lead_for(&[], &t).map(|p| p.to_string()).as_deref(), Some("mesh1.admin.1"), "an empty plan is closed by the fabric primary");
        assert_eq!(lead_for(&[create("mesh2.rpc.1")], &mm(true)).map(|p| p.to_string()).as_deref(), Some("mesh2.admin.1"));
    }

    #[test]
    fn a_handed_off_attempt_leaves_the_build_waiting_for_its_next_claim() {
        use crate::build_state::{fold, BuildFact, BuildIntentFact};
        let id = crate::build::BuildId("bld-x".into());
        let facts = vec![
            BuildFact::Intent(BuildIntentFact {
                build_id: id.clone(),
                intent: crate::build::BuildIntent::RemoveMesh { mesh: "mesh2".into() },
                traceparent: None,
                submitted_at_ms: 0,
                desired: None,
                reason: None,
            }),
            BuildFact::Claim(BuildAttemptClaim { build_id: id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }),
            BuildFact::Attempt(BuildAttemptReceipt { build_id: id.clone(), attempt: 1, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }),
        ];
        let p = fold(&facts).remove(&id).unwrap();
        assert_eq!((p.state, p.attempt, p.last_failure), (BuildState::Pending, 1, None));
    }
}
