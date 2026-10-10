//! The Build executor (PRD §1.2, §1.4, §7; mesh-control-plane.md §1, §3; node-rpc-envelope.md "Build,
//! op `0x20`").
//!
//! A Build is driven by the fabric-primary alone (`build_drive`): it claims each attempt on its own
//! log and calls the admin that [`executor_for`] names with `build.attempt.run` (`build_run`). The
//! executor here runs what that call carries: it records the won claim and runs exactly the
//! operations the fabric-primary planned for the run ([`RunPlan`]), never planning the Build again
//! from its own view.

use crate::build::{BuildId, BuildOperation};
use crate::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildProjection, BuildStateAdapter};
use crate::model::{NodeKind, PathName};
use crate::topology::Topology;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Realises one planned operation (the deployment and lifecycle pipelines).
#[async_trait::async_trait]
pub trait OperationRunner: Send + Sync {
    /// Realise `op` of `attempt` of `build_id`; `Err` names why it failed.
    async fn run(&self, build_id: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String>;

    /// What `op` owes before its executor is decided, once its turn comes: a mesh retire whose
    /// fabric-primary sits inside the leaving mesh hands the fabric seat to an outside
    /// mesh-primary first, so the executor is the new fabric-primary. `Err` ends the attempt by
    /// name and nothing of `op` has run.
    async fn prepare(&self, _build_id: &BuildId, _attempt: u32, _op: &BuildOperation) -> Result<(), String> {
        Ok(())
    }
}

/// The admin that executes `op` in view `t`; `None` while no admin can.
pub fn executor_for(op: &BuildOperation, t: &Topology) -> Option<PathName> {
    let fabric = || t.fabric_primary().map(|n| n.name.clone());
    match op {
        // A member node of any kind is born, restarted and retired by its own mesh's primary
        // admin (the fabric primary when the mesh has none): the rule names no role, so a
        // product's kinds get the same executor as the proof product's rpc node.
        BuildOperation::CreateNode { node } | BuildOperation::RestartNode { node } | BuildOperation::RetireNode { node } | BuildOperation::ReplaceNode { node, .. } | BuildOperation::DrainNode { node, .. } | BuildOperation::StopNode { node, .. }
            if node.kind != NodeKind::NodeAdmin =>
        {
            t.cohort_primary(&node.mesh, NodeKind::NodeAdmin).map(|n| n.name.clone()).or_else(fabric)
        }
        // A mesh's missing node-admin is created by that mesh's primary node-admin, its claim
        // fenced at the fabric primary (node-admin-lifecycle.md §4.3); the fabric primary creates
        // only the first admin of a mesh that has none.
        BuildOperation::CreateNode { node } if node.kind == NodeKind::NodeAdmin => {
            t.cohort_primary(&node.mesh, NodeKind::NodeAdmin).map(|n| n.name.clone()).or_else(fabric)
        }
        // The fabric primary is never the executor of its own retire, restart or replace: the
        // operation's terminate step would stop the executor mid-step. It is a hand-off: the admin
        // the election seats once the target drains (a Draining birth is no candidate,
        // fabric-node-lifecycle-elections.md section 2) executes it, and drains the target first.
        BuildOperation::RetireNode { node } | BuildOperation::RestartNode { node } | BuildOperation::ReplaceNode { node, .. } | BuildOperation::DrainNode { node, .. } | BuildOperation::StopNode { node, .. }
            if t.fabric_primary().is_some_and(|fp| fp.name == *node) =>
        {
            successor_of(node, t)
        }
        // A node-admin's restart, retire and replace belong to its mesh's primary admin, like every
        // other member of the mesh (Master Event Matrix: the executing Mesh admin is the owning
        // mesh-admin). The mesh primary never executes its own: the admin its mesh seats once it
        // drains does. A mesh with no other admin has no mesh admin to execute it: the fabric
        // primary, which owns what happens to a mesh that has none.
        BuildOperation::RetireNode { node } | BuildOperation::RestartNode { node } | BuildOperation::ReplaceNode { node, .. } | BuildOperation::DrainNode { node, .. } | BuildOperation::StopNode { node, .. } => {
            if t.cohort_primary(&node.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.name == *node) {
                mesh_successor_of(node, t).or_else(fabric)
            } else {
                t.cohort_primary(&node.mesh, NodeKind::NodeAdmin).map(|n| n.name.clone()).or_else(fabric)
            }
        }
        _ => fabric(),
    }
}

/// The admin primary of `target`'s mesh once `target` drains; `None` when no other admin of the
/// mesh can hold the seat.
fn mesh_successor_of(target: &PathName, t: &Topology) -> Option<PathName> {
    let mut nodes: Vec<_> = t.members().cloned().collect();
    for n in nodes.iter_mut().filter(|n| n.name == *target) {
        n.status = crate::model::NodeStatus::Draining;
    }
    crate::election::resolve(&mut nodes);
    nodes.into_iter().find(|n| n.kind == NodeKind::NodeAdmin && n.mesh == target.mesh && n.is_primary && n.name != *target).map(|n| n.name)
}

/// The fabric primary the election resolves in `t` once `target` drains; `None` when no other
/// admin can hold the seat.
fn successor_of(target: &PathName, t: &Topology) -> Option<PathName> {
    let mut nodes: Vec<_> = t.members().cloned().collect();
    for n in nodes.iter_mut().filter(|n| n.name == *target) {
        n.status = crate::model::NodeStatus::Draining;
    }
    crate::election::resolve(&mut nodes);
    nodes.into_iter().find(|n| n.is_fabric_primary && n.name != *target).map(|n| n.name)
}

/// The admin that executes what is left of a plan: the first operation's
/// executor, or the fabric primary when nothing is left (it closes the Build).
pub fn lead_for(ops: &[BuildOperation], t: &Topology) -> Option<PathName> {
    match ops.first() {
        Some(op) => executor_for(op, t),
        None => t.fabric_primary().map(|n| n.name.clone()),
    }
}

/// What the fabric-primary planned for one run of an attempt: the contiguous leading operations of
/// what is left that one executor executes in the fabric-primary's view, and who executes what
/// follows them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RunPlan {
    /// The operations to run, in plan order. Each is idempotent by its natural key.
    pub operations: Vec<BuildOperation>,
    /// The executor of the operation after this run; `None` when the run is the rest of the plan.
    pub hand_off_to: Option<String>,
}

/// The run of `executor` within `ops` in view `t`: the leading operations it executes, and the
/// executor of the first one it does not (an operation no admin can execute stays in the run).
pub fn run_of(ops: &[BuildOperation], t: &Topology, executor: &PathName) -> RunPlan {
    let mut run = RunPlan::default();
    for op in ops {
        match executor_for(op, t) {
            Some(other) if other != *executor => {
                run.hand_off_to = Some(other.to_string());
                return run;
            }
            _ => run.operations.push(op.clone()),
        }
    }
    run
}

/// How one attempt ended on its executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciled {
    /// Every remaining operation ran: the Build is complete.
    Converged {
        /// The attempt that completed.
        attempt: u32,
        /// The operations it ran.
        operations: Vec<BuildOperation>,
    },
    /// The attempt stopped at an operation; the fabric-primary claims a later attempt.
    Failed {
        /// The attempt that stopped.
        attempt: u32,
        /// The operation key the attempt stopped at (empty when it stopped before any operation).
        operation: String,
        /// Why it stopped.
        reason: String,
    },
    /// The attempt ran `operations` and stopped where admin `to` executes.
    HandedOff {
        /// The attempt that ran.
        attempt: u32,
        /// The operations it ran.
        operations: Vec<BuildOperation>,
        /// The admin that executes the operation it stopped at.
        to: String,
    },
}

/// Runs the attempt of an accepted Build that the fabric-primary claimed for this admin.
///
/// An executor never claims and never picks a Build up from its projection: the fabric-primary
/// decides the claim on its own log, plans the run (the contiguous operations this executor
/// executes) and calls [`BuildExecutor::run_attempt`] with the won claim and that [`RunPlan`]
/// (`build_run::RunDoor`, op `0x20` `build.attempt.run`). The executor records the claim and runs
/// exactly the operations the call carries; it does not plan, it re-reads no topology to decide what
/// is left, it never resumes from a saved instruction pointer and never mints a build id.
///
/// Which admin executes an operation (PRD §12.1, `executor_for`): a mesh's admin primary runs the
/// operations on that mesh's members, its node-admins included; the fabric primary runs everything
/// else (mesh creation and retirement) and a mesh's members while that mesh has no admin primary.
/// An attempt that reaches an operation another admin executes ends `HandedOff`, and the
/// fabric-primary claims the next attempt of the same Build for that admin.
pub struct BuildExecutor {
    /// This node-admin's path.name: the executor on every attempt it runs.
    pub executor: String,
    /// The Build state.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// The runner that realises each operation.
    pub runner: Arc<dyn OperationRunner>,
}

impl BuildExecutor {
    /// Run attempt `attempt` of `build_id`, which the fabric-primary won for this admin. The span is
    /// parented to the attempt's context the claim returned, so a restart reads under its request
    /// and a drift repair under the span that proved it.
    pub async fn run_attempt(&self, build_id: &BuildId, attempt: u32, context: &rafka_node_rpc_contract::context::CallContext, plan: &RunPlan) -> Reconciled {
        use tracing::Instrument;
        let previous = self.builds.read_build(build_id).await.ok();
        let span = tracing::info_span!(
            "rdm.node_admin.build.update.via-reconcile",
            build_id = %build_id,
            attempt,
            executor = %self.executor,
            previous_executor = previous.as_ref().and_then(|b| b.executor.as_deref()).unwrap_or(""),
            reason = previous.as_ref().map(|b| b.reason.as_str()).unwrap_or(""),
            action = %previous.as_ref().and_then(|b| b.action.as_ref()).map(|a| serde_json::to_string(a).unwrap_or_default()).unwrap_or_default(),
            operations = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        if let Some(tp) = &context.traceparent {
            rafka_mesh_telemetry::set_remote_parent(&span, tp, context.tracestate.as_deref());
        }
        let r = self.run_claimed(build_id, attempt, plan, &span).instrument(span.clone()).await;
        span.record(
            "outcome",
            match &r {
                Reconciled::Converged { .. } => "converged",
                Reconciled::Failed { .. } => "failed",
                Reconciled::HandedOff { .. } => "handed-off",
            },
        );
        r
    }

    async fn run_claimed(&self, build_id: &BuildId, attempt: u32, plan: &RunPlan, span: &tracing::Span) -> Reconciled {
        // The fabric-primary decided the claim; this admin's own log takes the fact with it.
        let claim = BuildAttemptClaim { build_id: build_id.clone(), attempt, executor: self.executor.clone() };
        if let Err(e) = self.builds.adopt_claim(&claim).await {
            return Reconciled::Failed { attempt, operation: String::new(), reason: format!("recording the claim of attempt {attempt}: {e}") };
        }
        let build = match self.builds.read_build(build_id).await {
            Ok(b) => b,
            Err(e) => return Reconciled::Failed { attempt, operation: String::new(), reason: format!("reading Build {build_id} after the claim of attempt {attempt}: {e}") },
        };
        let operations = &plan.operations;
        span.record("operations", operations.iter().map(BuildOperation::key).collect::<Vec<_>>().join(",").as_str());
        for (i, op) in operations.iter().enumerate() {
            // An operation's executor can move while it is prepared (the seat handover of a mesh
            // retire): the operation then belongs to the admin the view now names, and it is passed
            // to it unrun. Nothing else hands an operation off from here; the run is the
            // fabric-primary's plan.
            let named = executor_for(op, &*self.topology.read().await);
            if let Err(e) = self.runner.prepare(&build.build_id, attempt, op).await {
                return self.finish(&build, attempt, Err((op.key(), e))).await;
            }
            let to = executor_for(op, &*self.topology.read().await);
            if to != named {
                if let Some(to) = to.filter(|to| to.to_string() != self.executor) {
                    return self.hand_off(&build, attempt, operations[..i].to_vec(), to.to_string()).await;
                }
            }
            if let Err(e) = self.runner.run(&build.build_id, attempt, op).await {
                return self.finish(&build, attempt, Err((op.key(), e))).await;
            }
        }
        match &plan.hand_off_to {
            Some(to) => self.hand_off(&build, attempt, operations.clone(), to.clone()).await,
            None => self.finish(&build, attempt, Ok(operations.clone())).await,
        }
    }

    async fn hand_off(&self, build: &BuildProjection, attempt: u32, operations: Vec<BuildOperation>, to: String) -> Reconciled {
        let receipt = BuildAttemptReceipt { build_id: build.build_id.clone(), attempt, outcome: AttemptOutcome::HandedOff { to: to.clone() } };
        if let Err(e) = self.builds.append_attempt_receipt(&receipt).await {
            return Reconciled::Failed { attempt, operation: String::new(), reason: format!("recording attempt {attempt}: {e}") };
        }
        tracing::info!(to = %to, "the next operation belongs to another admin; handed off");
        Reconciled::HandedOff { attempt, operations, to }
    }

    async fn finish(&self, build: &BuildProjection, attempt: u32, r: Result<Vec<BuildOperation>, (String, String)>) -> Reconciled {
        let outcome = match &r {
            Ok(_) => AttemptOutcome::Converged,
            Err((op, reason)) => AttemptOutcome::Failed { reason: format!("{op}: {reason}") },
        };
        let receipt = BuildAttemptReceipt { build_id: build.build_id.clone(), attempt, outcome };
        if let Err(e) = self.builds.append_attempt_receipt(&receipt).await {
            return Reconciled::Failed { attempt, operation: String::new(), reason: format!("recording attempt {attempt}: {e}") };
        }
        match r {
            Ok(operations) => Reconciled::Converged { attempt, operations },
            Err((operation, reason)) => Reconciled::Failed { attempt, reason: format!("{operation}: {reason}"), operation },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::BuildState;
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
        assert_eq!(who(&BuildOperation::RetireNode { node: "mesh2.rpc.1".parse().unwrap() }, &t), "mesh2.admin.1");
        assert_eq!(who(&create("mesh1.rpc.2"), &t), "mesh1.admin.1");
        // The rule names no role: a product's kinds get the mesh's own primary admin too.
        assert_eq!(who(&create("mesh2.broker.1"), &t), "mesh2.admin.1");
        assert_eq!(who(&BuildOperation::RestartNode { node: "mesh2.gateway.1".parse().unwrap() }, &t), "mesh2.admin.1");
        assert_eq!(who(&BuildOperation::RetireNode { node: "mesh2.compute.1".parse().unwrap() }, &t), "mesh2.admin.1");
        assert_eq!(who(&create("mesh2.admin.2"), &t), "mesh2.admin.1", "a mesh's missing admin is created by that mesh's primary");
        assert_eq!(who(&create("mesh1.admin.2"), &t), "mesh1.admin.1", "its own mesh's primary, which is the fabric primary here");
        assert_eq!(who(&BuildOperation::RetireNode { node: "mesh2.admin.1".parse().unwrap() }, &t), "mesh1.admin.1", "mesh2.admin.1 is mesh2's only admin: no successor, the fabric primary");
        let mut t3 = mm(true);
        t3.nodes.push(node("mesh2.admin.2", false, false));
        let (p2, m2) = ("mesh2.admin.1", "mesh2.admin.2");
        assert_eq!(who(&BuildOperation::RetireNode { node: m2.parse().unwrap() }, &t3), p2, "a non-primary admin's retire is its mesh primary's");
        assert_eq!(who(&BuildOperation::RestartNode { node: m2.parse().unwrap() }, &t3), p2, "a non-primary admin's restart is its mesh primary's");
        assert_eq!(who(&BuildOperation::RestartNode { node: p2.parse().unwrap() }, &t3), m2, "a mesh primary's own restart is handed to the admin its mesh seats once it drains");
        assert_eq!(who(&BuildOperation::RetireNode { node: p2.parse().unwrap() }, &t3), m2, "a mesh primary's own retire is handed to the admin its mesh seats once it drains");
        let replace = |n: &str| BuildOperation::ReplaceNode { node: n.parse().unwrap(), from_incarnation: crate::model::IncarnationId::mint() };
        assert_eq!(who(&replace("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's replace is its mesh primary's");
        assert_eq!(who(&replace(m2), &t3), p2, "a non-primary admin's replace is its mesh primary's");
        assert_eq!(who(&replace(p2), &t3), m2, "a mesh primary's own replace is handed to the admin its mesh seats once it drains");
        let drain = |n: &str| BuildOperation::DrainNode { node: n.parse().unwrap(), from_incarnation: crate::model::IncarnationId::mint() };
        let stop = |n: &str| BuildOperation::StopNode { node: n.parse().unwrap(), from_incarnation: crate::model::IncarnationId::mint() };
        assert_eq!(who(&drain("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's drain is its mesh primary's");
        assert_eq!(who(&stop("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's stop is its mesh primary's");
        assert_eq!(who(&drain(m2), &t3), p2, "a non-primary admin's drain is its mesh primary's");
        assert_eq!(who(&stop(p2), &t3), m2, "a mesh primary's own stop is handed to the admin its mesh seats once it drains");
        assert_eq!(who(&BuildOperation::CreateMesh { mesh: "mesh3".into() }, &t), "mesh1.admin.1");
        assert_eq!(who(&shutdown("mesh2"), &t), "mesh1.admin.1");
    }

    /// CONTRACT (Luke 2026-10-08, the fabric primary is handed off, never wiped out): a retire or
    /// restart of the fabric primary is executed by the admin the election seats once the target
    /// drains, never by the target itself; with no other admin, by none. Another admin's retire or
    /// restart is its own mesh primary's (mesh2's only admin has no mesh primary beside it: the
    /// fabric primary).
    #[test]
    fn the_fabric_primary_never_executes_its_own_retire_or_restart() {
        let mut t = mm(true);
        crate::election::resolve(&mut t.nodes);
        let fp = t.fabric_primary().expect("a fabric primary").name.clone();
        let other = t.nodes.iter().find(|n| n.kind == NodeKind::NodeAdmin && n.name != fp).unwrap().name.clone();
        for op in [BuildOperation::RetireNode { node: fp.clone() }, BuildOperation::RestartNode { node: fp.clone() }] {
            assert_eq!(executor_for(&op, &t), Some(other.clone()), "{op:?}: executed by the successor, never by {fp}");
        }
        assert_eq!(executor_for(&BuildOperation::RetireNode { node: other.clone() }, &t), Some(fp.clone()), "mesh2's only admin has no mesh primary beside it: the fabric primary retires it");
        // Alone, the fabric primary has no successor: nobody executes its retire.
        let mut alone = mm(false);
        crate::election::resolve(&mut alone.nodes);
        let fp = alone.fabric_primary().unwrap().name.clone();
        assert_eq!(executor_for(&BuildOperation::RetireNode { node: fp }, &alone), None);
    }

    fn shutdown(mesh: &str) -> BuildOperation {
        BuildOperation::ShutdownMesh { mesh: mesh.into(), mesh_id: rafka_mesh_entity::MeshId::mint() }
    }

    /// CONTRACT (R-ST2): `shutdown-mesh:<mesh_id>` is the fabric primary's, whatever mesh holds the
    /// seat. When the fabric primary is inside the leaving mesh it hands the seat over first
    /// (`OperationRunner::prepare`), and the executor of the operation is then the new fabric
    /// primary.
    #[test]
    fn a_mesh_retire_is_the_fabric_primarys() {
        let t = mm(true);
        assert_eq!(who(&shutdown("mesh2"), &t), "mesh1.admin.1", "the fabric primary, outside mesh2");
        assert_eq!(who(&shutdown("mesh1"), &t), "mesh1.admin.1", "the fabric primary, inside mesh1: it hands the seat over before it runs");
        let mut moved = mm(true);
        for n in moved.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh2.admin.1";
        }
        assert_eq!(who(&shutdown("mesh1"), &moved), "mesh2.admin.1", "once the seat moved, the new fabric primary is the executor");
    }

    #[test]
    fn a_mesh_without_an_admin_primary_has_its_members_run_by_the_fabric_primary() {
        let t = mm(false);
        assert_eq!(who(&create("mesh2.rpc.1"), &t), "mesh1.admin.1");
        assert_eq!(who(&create("mesh2.admin.1"), &t), "mesh1.admin.1", "the first admin of a mesh that has none is the fabric primary's");
        assert_eq!(lead_for(&[], &t).map(|p| p.to_string()).as_deref(), Some("mesh1.admin.1"), "an empty plan is closed by the fabric primary");
        assert_eq!(lead_for(&[create("mesh2.rpc.1")], &mm(true)).map(|p| p.to_string()).as_deref(), Some("mesh2.admin.1"));
    }

    #[test]
    fn a_handed_off_attempt_leaves_the_build_waiting_for_its_next_claim() {
        use crate::build_state::{fold, BuildAccepted, BuildFact};
        let id = crate::build::BuildId("bld-x".into());
        let facts = vec![
            BuildFact::Accepted(BuildAccepted {
                build_id: id.clone(),
                topology: crate::accepted::FabricTopology::root("fabric1", "mesh1"),
                submitted_change: None,
                submitted_at_ms: 0,
            }),
            BuildFact::Claim(BuildAttemptClaim { build_id: id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }),
            BuildFact::Attempt(BuildAttemptReceipt { build_id: id.clone(), attempt: 1, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }),
        ];
        let p = fold(&facts).remove(&id).unwrap();
        assert_eq!((p.state, p.attempt, p.last_failure), (BuildState::Pending, 1, None));
    }
}
