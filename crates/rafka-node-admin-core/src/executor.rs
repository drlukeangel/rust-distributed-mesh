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
        // Every operation of a node recovery (its node.delete, node.create and node.update) is
        // executed by the exited birth's recoverer (node-recovery.md "Reconciler", R-ON8): a member's
        // mesh-primary; for a lost node-admin of a mesh with a live mesh-primary, that mesh-primary,
        // and a mesh primary's own recovery by the admin its mesh seats in its place
        // (mesh-recovery.md:8-9). A mesh with no other live admin lost its mesh-primary: that is no
        // node recovery, and no admin executes one. Its node-admin is created by a recipe's node.create
        // (below), the fabric-primary's once the mesh holds no admin that is not proven gone
        // (mesh-investigation.md "A node-admin joins the live mesh").
        BuildOperation::RetireNode { node, exited: Some(_) } | BuildOperation::CreateNode { node, over: Some(_) } | BuildOperation::UpdateNode { node, .. } => {
            if node.kind == NodeKind::NodeAdmin {
                node_admin_recoverer(node, t)
            } else {
                mesh_executor(&node.mesh, None, t)
            }
        }
        // A recipe step is executed as the op it calls: a create as node.create, every other step as
        // the node op on its node (the mesh-primary of the node's mesh writes its row, R13).
        BuildOperation::RecipeStep { step: rafka_node_rpc_contract::build::RecipeStep::Create { node }, .. } => mesh_executor(&node.mesh, None, t),
        BuildOperation::RecipeStep { step, .. } => node_op_executor(step.node(), t),
        // A member node of any kind is born, restarted and retired by its own mesh's primary
        // admin (the fabric primary when the mesh has none): the rule names no role, so a
        // product's kinds get the same executor as the proof product's rpc node.
        // A member node of any kind is born by its own mesh's primary admin (the fabric primary when
        // the mesh has none): the rule names no role, so a product's kinds get the same executor as
        // the proof product's rpc node. A mesh's missing node-admin is created by that mesh's primary
        // node-admin, its claim fenced at the fabric primary (node-admin-lifecycle.md §4.3); the
        // fabric primary creates only the first admin of a mesh that has none.
        BuildOperation::CreateNode { node, .. } => mesh_executor(&node.mesh, None, t),
        BuildOperation::RetireNode { node, .. } | BuildOperation::RestartNode { node } | BuildOperation::DrainNode { node, .. } | BuildOperation::StopNode { node, .. } | BuildOperation::StartNode { node, .. } | BuildOperation::DeleteNode { node, .. } => node_op_executor(node, t),
        BuildOperation::CreateMesh { .. } | BuildOperation::ShutdownMesh { .. } => fabric(),
    }
}

/// The executor of a node op (restart, retire, drain, stop, start, delete, and a recipe's update) on
/// `node` in view `t`.
fn node_op_executor(node: &PathName, t: &Topology) -> Option<PathName> {
    let fabric = || t.fabric_primary().map(|n| n.name.clone());
    // A member node of any kind belongs to its own mesh's primary admin (the fabric primary when the
    // mesh has none).
    if node.kind != NodeKind::NodeAdmin {
        return mesh_executor(&node.mesh, None, t);
    }
    // The fabric primary is never the executor of its own op: the operation's terminate step would
    // stop the executor mid-step. It is a hand-off: the admin the election seats once the target
    // drains (a Draining birth is no candidate, fabric-node-lifecycle-elections.md section 2)
    // executes it, and drains the target first.
    if t.fabric_primary().is_some_and(|fp| fp.name == *node) {
        return successor_of(node, t);
    }
    // A node-admin's ops belong to its mesh's primary admin, like every other member of the mesh
    // (Master Event Matrix: the executing Mesh admin is the owning mesh-admin). The mesh primary never
    // executes its own: the admin its mesh seats once it drains does. A mesh with no other admin has
    // no mesh admin to execute it: the fabric primary, which owns what happens to a mesh that has none.
    if t.cohort_primary(&node.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.name == *node) {
        mesh_successor_of(node, t).or_else(|| held_admins(&node.mesh, Some(node), t).is_empty().then(fabric).flatten())
    } else {
        mesh_executor(&node.mesh, Some(node), t)
    }
}

/// The executor of an operation on a node of `mesh` (not `target` itself): the mesh's primary
/// admin; the fabric primary only when the mesh has no node-admin left that is not proven gone
/// (R-A2: cross-mesh only when the whole cohort is proven lost; node-rpc-envelope.md TakeSeats: the
/// executor is the same-mesh successor the election seats). An admin held but not seated (silent,
/// or ready and waiting on its seat) gives `None`: the drive waits for that admin, named by
/// [`held_admins`], and never acts on the mesh from outside it. A `Pending` admin is a birth, not
/// a held seat: the fabric primary carries the mesh until it stands.
fn mesh_executor(mesh: &str, target: Option<&PathName>, t: &Topology) -> Option<PathName> {
    if let Some(p) = t.cohort_primary(mesh, NodeKind::NodeAdmin) {
        return Some(p.name.clone());
    }
    if held_admins(mesh, target, t).is_empty() {
        t.fabric_primary().map(|n| n.name.clone())
    } else {
        None
    }
}

/// The node-admins of `mesh` (other than `target`) that hold no seat and are not proven gone: the
/// same-mesh successors an operation on the mesh waits for. Proven gone in this view is `Dead` (an
/// exit proven for the exact birth; the offline mark is only for an admin's own mesh, which is
/// never the fallback case), `Leaving` and `Deleted`; a departed birth has left the view.
pub fn held_admins<'a>(mesh: &str, target: Option<&PathName>, t: &'a Topology) -> Vec<&'a crate::model::Node> {
    use crate::model::NodeStatus;
    t.cohort(mesh, NodeKind::NodeAdmin).filter(|n| Some(&n.name) != target && matches!(n.status, NodeStatus::ReadyForTraffic | NodeStatus::Draining | NodeStatus::PendingReconnect | NodeStatus::Restarting)).collect()
}

/// Why no admin executes `op` in view `t`: the mesh's held node-admins by name and status, or
/// that the view seats no admin at all.
pub fn why_no_executor(op: &BuildOperation, t: &Topology) -> String {
    let node = match op {
        BuildOperation::CreateNode { node, .. } | BuildOperation::RestartNode { node } | BuildOperation::RetireNode { node, .. } | BuildOperation::DrainNode { node, .. } | BuildOperation::StopNode { node, .. } | BuildOperation::StartNode { node, .. } | BuildOperation::UpdateNode { node, .. } | BuildOperation::DeleteNode { node, .. } => node,
        BuildOperation::RecipeStep { step, .. } => step.node(),
        _ => return format!("{}: no admin holds the seat that executes it in this view", op.key()),
    };
    let held: Vec<String> = held_admins(&node.mesh, Some(node), t).into_iter().map(|n| format!("{} ({:?})", n.name, n.status)).collect();
    if held.is_empty() {
        format!("{}: mesh {} seats no node-admin and no fabric primary is seated in this view", op.key(), node.mesh)
    } else {
        format!("{}: mesh {} seats no node-admin, and its node-admin(s) {} are not proven gone: the fabric primary does not act on the mesh; the attempt waits for the mesh's own seat", op.key(), node.mesh, held.join(", "))
    }
}

/// The mesh-primary that recovers node-admin `node` (node-recovery.md, mesh-recovery.md:8-9): its
/// mesh's live primary admin, or, when `node` itself holds the seat, the admin its mesh seats in its
/// place. `None` when no other live admin of the mesh is left: the mesh lost its mesh-primary, which
/// no node recovery restores.
///
/// The seat is read from the view's election, not from liveness: a silent holder keeps its seat until
/// it is proven gone (`crate::election`), and its own recovery is still its mesh's to run.
pub fn node_admin_recoverer(node: &PathName, t: &Topology) -> Option<PathName> {
    match t.cohort(&node.mesh, NodeKind::NodeAdmin).find(|n| n.is_primary) {
        Some(p) if p.name == *node => mesh_successor_of(node, t),
        Some(p) if p.status.is_live() => Some(p.name.clone()),
        _ => None,
    }
}

/// The admin primary of `target`'s mesh once `target`'s birth is gone; `None` when no other admin
/// of the mesh can hold the seat.
fn mesh_successor_of(target: &PathName, t: &Topology) -> Option<PathName> {
    seat_successor(target, t).map(|n| n.name)
}

/// The admin the election seats as `target`'s own mesh primary once `target`'s exact birth is gone:
/// the same-mesh successor that executes a seat holder's drain, stop, restart or replace and takes
/// its seats by TakeSeats (node-rpc-envelope.md, "TakeSeats: Build-driven seat transfer"). `None`
/// when no other admin of the mesh can hold the seat.
pub fn seat_successor(target: &PathName, t: &Topology) -> Option<crate::model::Node> {
    seated_once_gone(target, t).into_iter().find(|n| n.kind == NodeKind::NodeAdmin && n.mesh == target.mesh && n.is_primary && n.name != *target)
}

/// The fabric primary the election seats once `target`'s birth is gone: the admin its own mesh
/// seats while any node-admin of that mesh survives, another mesh's primary only when none does
/// (fabric-node-lifecycle-elections.md sections 4.3 and 5); `None` when no other admin can hold
/// the seat yet.
fn successor_of(target: &PathName, t: &Topology) -> Option<PathName> {
    seated_once_gone(target, t).into_iter().find(|n| n.is_fabric_primary && n.name != *target).map(|n| n.name)
}

/// The members of `t` as the election seats them once `target`'s exact birth is gone. The seat
/// records are the holders `t` was decided against (its fabric primary and each mesh's admin
/// primary: `t` is the projection of this admin's held records), and `target`'s birth is the one
/// proven gone, so every seat it holds is filled by its own mesh's rule and every other seat
/// stays with its holder (election.rs `resolve_with`).
fn seated_once_gone(target: &PathName, t: &Topology) -> Vec<crate::model::Node> {
    let mut nodes: Vec<_> = t.members().cloned().collect();
    let record = |n: &crate::model::Node| n.incarnation_id.clone().map(|incarnation| rafka_mesh_entity::SeatHolder { mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation, epoch: 0 });
    let mut inc = crate::election::Incumbency { fabric: t.fabric_primary().and_then(record), ..Default::default() };
    for mesh in &t.meshes {
        if let Some(h) = t.cohort_primary(&mesh.name, NodeKind::NodeAdmin).and_then(record) {
            inc.meshes.insert(mesh.name.clone(), h);
        }
    }
    for n in nodes.iter_mut().filter(|n| n.name == *target) {
        match n.incarnation_id.clone() {
            Some(birth) => {
                inc.lost.insert(birth);
            }
            // A birth with no incarnation names no record to yield; it is no candidate either.
            None => n.status = crate::model::NodeStatus::Leaving,
        }
    }
    crate::election::resolve_with(&mut nodes, &inc);
    nodes
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
    /// `Some` when more of the plan follows this run (the planner's executor of the next operation,
    /// in the planner's view); `None` when the run is the rest of the plan. It only marks that more
    /// follows: the executor hands back to the fabric primary of the view its run ends with, since
    /// the run's own operations can move a seat or stop the planner ([`BuildExecutor`]).
    pub hand_off_to: Option<String>,
}

/// The run of `executor` within `ops` in view `t`: the leading operations it executes, and the
/// executor of the first one it does not. An operation no admin can execute stays in the run here;
/// the fabric-primary's drive cuts the run before it when it is not the first (`build_drive`), so it
/// waits for its mesh's own seat.
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
/// fabric-primary claims the next attempt of the same Build for that admin. A run that ends with
/// more of the plan to follow hands back to the fabric primary of the view it ends with, which
/// claims the next attempt for the admin its view names for what is left.
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
            // The admin that held the attempt before this one. The fabric-primary's claim of this
            // attempt is already folded (the executor absorbs its intent first), so `executor` here
            // names this admin.
            previous_executor = previous.as_ref().and_then(|b| b.attempt_executors.get(&attempt.saturating_sub(1))).map(String::as_str).unwrap_or(""),
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
            // An operation's executor can move while the run goes: at prepare (the seat handover
            // of a mesh retire) or once an earlier operation of the run has seated a mesh's first
            // admin. The operation then belongs to the admin the view now names, and it is passed
            // to it unrun. The operations themselves are always the fabric-primary's plan.
            let named = executor_for(op, &*self.topology.read().await);
            if let Err(e) = self.runner.prepare(&build.build_id, attempt, op).await {
                return self.finish(&build, attempt, Err((op.key(), e))).await;
            }
            let to = executor_for(op, &*self.topology.read().await);
            // After the first operation the view can name another executor for the rest: the run
            // seated a mesh's first admin, which executes that mesh's members from then on. An
            // operation a view does not place stays with this admin.
            if to != named || i > 0 {
                if let Some(to) = to.filter(|to| to.to_string() != self.executor) {
                    return self.hand_off(&build, attempt, operations[..i].to_vec(), to.to_string()).await;
                }
            }
            if let Err(e) = self.runner.run(&build.build_id, attempt, op).await {
                return self.finish(&build, attempt, Err((op.key(), e))).await;
            }
        }
        // More of the plan follows the run. The planner named the next executor from its own view,
        // taken before this run's operations, and they can move a seat or stop the planner itself
        // (the fabric primary's replace: its same-mesh successor runs the stop that moves its seats
        // and parks it). So the run hands back to the fabric primary of the view it ends with, the
        // one driver of the Build, whose next claim names the executor of what is left in its own
        // view (`build_drive` claim_next).
        if plan.hand_off_to.is_some() {
            let driver = self.topology.read().await.fabric_primary().map(|n| n.name.to_string());
            return match driver {
                Some(to) => self.hand_off(&build, attempt, operations.clone(), to).await,
                // No operation failed: the receipt names none, so no step is blamed for it.
                None => {
                    let last = operations.last().map(BuildOperation::key).unwrap_or_else(|| "no operation".into());
                    let reason = format!("{}: the run of attempt {attempt} ended after {last} with more of the plan to follow, and this admin's view seats no fabric primary to hand it back to", self.executor);
                    self.finish(&build, attempt, Err((String::new(), reason))).await
                }
            };
        }
        self.finish(&build, attempt, Ok(operations.clone())).await
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
            Err((op, reason)) if op.is_empty() => AttemptOutcome::Failed { reason: reason.clone() },
            Err((op, reason)) => AttemptOutcome::Failed { reason: format!("{op}: {reason}") },
        };
        let receipt = BuildAttemptReceipt { build_id: build.build_id.clone(), attempt, outcome };
        if let Err(e) = self.builds.append_attempt_receipt(&receipt).await {
            return Reconciled::Failed { attempt, operation: String::new(), reason: format!("recording attempt {attempt}: {e}") };
        }
        match r {
            Ok(operations) => Reconciled::Converged { attempt, operations },
            Err((operation, reason)) if operation.is_empty() => Reconciled::Failed { attempt, reason, operation },
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
        n.incarnation_id = Some(crate::model::IncarnationId::mint());
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
        BuildOperation::CreateNode { node: n.parse().unwrap(), over: None }
    }

    fn who(op: &BuildOperation, t: &Topology) -> String {
        executor_for(op, t).map(|p| p.to_string()).unwrap_or_default()
    }

    /// CONTRACT (#3051, #3088, node-recovery.md "Execution" and "Steps": the fabric-primary dispatches
    /// the recovery Build to the mesh-primary as executor; mesh-recovery.md: "A lost member of a mesh
    /// with a live mesh-primary is node recovery, driven by that mesh-primary"): every operation of a
    /// member's recovery (its node.delete, node.create and node.update), a secondary node-admin's
    /// included, is executed by its own mesh's primary admin, never by the fabric-primary of another
    /// mesh. A mesh primary's own recovery is executed by the admin its mesh seats in its place, the
    /// view still seating the exited primary included. A node-admin with no other admin in its mesh is
    /// no node recovery (its mesh lost its mesh-primary: its node-admin is created by the investigation's
    /// node.create, never recovered), so no admin executes one.
    /// Must NOT happen: a recovery's operation routed to the fabric-primary by a fall-through arm, or
    /// its create routed to the exited mesh primary the view still seats.
    #[test]
    fn a_members_recovery_is_executed_by_its_mesh_primary_a_secondary_node_admins_included() {
        let recovery = |n: &str| {
            let (node, from) = (n.parse::<PathName>().unwrap(), crate::build::ExitedBirth { node_id: crate::model::NodeId::mint(), incarnation: crate::model::IncarnationId::mint() });
            [BuildOperation::RetireNode { node: node.clone(), exited: Some(from.clone()) }, BuildOperation::CreateNode { node: node.clone(), over: Some(from.clone()) }, BuildOperation::UpdateNode { node, from }]
        };
        let recovered_by = |n: &str, t: &Topology| {
            let who: Vec<String> = recovery(n).iter().map(|op| who(op, t)).collect();
            assert!(who.windows(2).all(|w| w[0] == w[1]), "{n}: one executor runs all three operations: {who:?}");
            who[0].clone()
        };
        let t = mm(true);
        assert_eq!(recovered_by("mesh2.rpc.1", &t), "mesh2.admin.1", "a member of mesh2 is recovered by mesh2's primary");
        assert_eq!(recovered_by("mesh1.rpc.1", &t), "mesh1.admin.1", "a member of the fabric-primary's own mesh is recovered by that mesh's primary");
        assert_eq!(recovered_by("mesh2.admin.1", &t), "", "mesh2's only node-admin: its loss is mesh recovery, never node recovery");
        let mut t3 = mm(true);
        t3.nodes.push(node("mesh2.admin.2", false, false));
        t3.nodes.push(node("mesh1.admin.2", false, false));
        assert_eq!(recovered_by("mesh2.admin.2", &t3), "mesh2.admin.1", "a secondary node-admin is recovered by its mesh's primary");
        assert_eq!(recovered_by("mesh1.admin.2", &t3), "mesh1.admin.1", "the fabric-primary's own secondary admin, by its mesh-primary seat");
        assert_eq!(recovered_by("mesh2.admin.1", &t3), "mesh2.admin.2", "a mesh primary's own recovery is executed by the admin its mesh seats in its place");
        let mut silent = t3.clone();
        silent.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.1").for_each(|n| n.status = NodeStatus::PendingReconnect);
        assert_eq!(recovered_by("mesh2.admin.2", &silent), "", "no live node-admin of mesh2 is left to recover it: mesh recovery");
    }

    #[test]
    fn a_meshs_members_are_its_own_primarys_and_everything_else_the_fabric_primarys() {
        let t = mm(true);
        assert_eq!(who(&create("mesh2.rpc.1"), &t), "mesh2.admin.1");
        assert_eq!(who(&BuildOperation::RetireNode { node: "mesh2.rpc.1".parse().unwrap(), exited: None }, &t), "mesh2.admin.1");
        assert_eq!(who(&create("mesh1.rpc.2"), &t), "mesh1.admin.1");
        // The rule names no role: a product's kinds get the mesh's own primary admin too.
        assert_eq!(who(&create("mesh2.broker.1"), &t), "mesh2.admin.1");
        assert_eq!(who(&BuildOperation::RestartNode { node: "mesh2.gateway.1".parse().unwrap() }, &t), "mesh2.admin.1");
        assert_eq!(who(&BuildOperation::RetireNode { node: "mesh2.compute.1".parse().unwrap(), exited: None }, &t), "mesh2.admin.1");
        assert_eq!(who(&create("mesh2.admin.2"), &t), "mesh2.admin.1", "a mesh's missing admin is created by that mesh's primary");
        assert_eq!(who(&create("mesh1.admin.2"), &t), "mesh1.admin.1", "its own mesh's primary, which is the fabric primary here");
        assert_eq!(who(&BuildOperation::RetireNode { node: "mesh2.admin.1".parse().unwrap(), exited: None }, &t), "mesh1.admin.1", "mesh2.admin.1 is mesh2's only admin: no successor, the fabric primary");
        let mut t3 = mm(true);
        t3.nodes.push(node("mesh2.admin.2", false, false));
        let (p2, m2) = ("mesh2.admin.1", "mesh2.admin.2");
        assert_eq!(who(&BuildOperation::RetireNode { node: m2.parse().unwrap(), exited: None }, &t3), p2, "a non-primary admin's retire is its mesh primary's");
        assert_eq!(who(&BuildOperation::RestartNode { node: m2.parse().unwrap() }, &t3), p2, "a non-primary admin's restart is its mesh primary's");
        assert_eq!(who(&BuildOperation::RestartNode { node: p2.parse().unwrap() }, &t3), m2, "a mesh primary's own restart is handed to the admin its mesh seats once it drains");
        assert_eq!(who(&BuildOperation::RetireNode { node: p2.parse().unwrap(), exited: None }, &t3), m2, "a mesh primary's own retire is handed to the admin its mesh seats once it drains");
        let rename = |n: &str| {
            let node: PathName = n.parse().unwrap();
            BuildOperation::RecipeStep { step: rafka_node_rpc_contract::build::RecipeStep::Update { name: node.renamed(), node }, birth: None }
        };
        let make = |n: &str| BuildOperation::RecipeStep { step: rafka_node_rpc_contract::build::RecipeStep::Create { node: n.parse().unwrap() }, birth: None };
        assert_eq!(who(&rename("mesh2.rpc.1"), &t), "mesh2.admin.1", "a recipe's update of a member is its mesh primary's, the writer of its row");
        assert_eq!(who(&rename(m2), &t3), p2, "a recipe's update of a non-primary admin is its mesh primary's");
        assert_eq!(who(&rename(p2), &t3), m2, "a recipe's update of a mesh primary is the admin its mesh seats once it is stopped");
        assert_eq!(who(&make("mesh2.rpc.1.new"), &t), "mesh2.admin.1", "a recipe's create is node.create's executor");
        let drain = |n: &str| BuildOperation::DrainNode { node: n.parse().unwrap(), from_incarnation: crate::model::IncarnationId::mint() };
        let stop = |n: &str| BuildOperation::StopNode { node: n.parse().unwrap(), from_incarnation: crate::model::IncarnationId::mint() };
        assert_eq!(who(&drain("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's drain is its mesh primary's");
        assert_eq!(who(&stop("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's stop is its mesh primary's");
        assert_eq!(who(&drain(m2), &t3), p2, "a non-primary admin's drain is its mesh primary's");
        assert_eq!(who(&stop(p2), &t3), m2, "a mesh primary's own stop is handed to the admin its mesh seats once it drains");
        assert_eq!(who(&BuildOperation::CreateMesh { mesh: "mesh3".into(), mesh_id: MeshId::mint() }, &t), "mesh1.admin.1");
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
        for op in [BuildOperation::RetireNode { node: fp.clone(), exited: None }, BuildOperation::RestartNode { node: fp.clone() }] {
            assert_eq!(executor_for(&op, &t), Some(other.clone()), "{op:?}: executed by the successor, never by {fp}");
        }
        assert_eq!(executor_for(&BuildOperation::RetireNode { node: other.clone(), exited: None }, &t), Some(fp.clone()), "mesh2's only admin has no mesh primary beside it: the fabric primary retires it");
        // Alone, the fabric primary has no successor: nobody executes its retire.
        let mut alone = mm(false);
        crate::election::resolve(&mut alone.nodes);
        let fp = alone.fabric_primary().unwrap().name.clone();
        assert_eq!(executor_for(&BuildOperation::RetireNode { node: fp, exited: None }, &alone), None);
    }

    /// CONTRACT (#3016; node-rpc-envelope.md TakeSeats: the executor is "the same-mesh successor
    /// N"; R-A2: cross-mesh only when the whole cohort is proven lost): mesh2's seated admin is
    /// proven exited and its other admin is held but silent (`PendingReconnect`, not proven gone).
    /// No admin of mesh2 holds the seat yet, and the fabric primary of mesh1 executes nothing of
    /// mesh2: the answer is `None` and the reason names the held admin. With every mesh2 admin
    /// proven gone the fabric primary executes; a `Pending` admin is a birth, carried by the fabric
    /// primary until it stands.
    #[test]
    fn a_mesh_whose_admin_is_held_but_not_seated_is_never_executed_by_the_fabric_primary() {
        let mut t = mm(true);
        let mut second = node("mesh2.admin.2", false, false);
        second.status = NodeStatus::PendingReconnect;
        t.nodes.push(second);
        t.nodes.push(node("mesh2.rpc.1", true, false));
        for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.1") {
            n.status = NodeStatus::Dead;
            n.is_primary = false;
        }
        let ops = [BuildOperation::RestartNode { node: "mesh2.rpc.1".parse().unwrap() }, create("mesh2.rpc.2"), create("mesh2.admin.3"), BuildOperation::RetireNode { node: "mesh2.admin.1".parse().unwrap(), exited: None }];
        for op in &ops {
            assert_eq!(executor_for(op, &t), None, "{op:?}: mesh2.admin.2 is held, not proven gone: never the fabric primary");
            let why = why_no_executor(op, &t);
            assert!(why.contains("mesh2.admin.2") && why.contains("PendingReconnect"), "{why}");
        }
        for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.2") {
            n.status = NodeStatus::Dead;
        }
        assert_eq!(who(&ops[0], &t), "mesh1.admin.1", "every mesh2 admin proven gone: the fabric primary");
        for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.2") {
            n.status = NodeStatus::Pending;
        }
        assert_eq!(who(&ops[0], &t), "mesh1.admin.1", "a Pending admin is a birth the fabric primary carries");
        for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.2") {
            n.status = NodeStatus::ReadyForTraffic;
            n.is_primary = true;
        }
        assert_eq!(who(&ops[0], &t), "mesh2.admin.2", "once seated, the same-mesh successor executes");
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

    /// CONTRACT (fabric-node-lifecycle-elections.md section 5: "a holder that yields or whose exact
    /// birth is proven gone hands the seat to the lowest ReadyForTraffic NodeId of its own mesh";
    /// section 4.3: the fabric seat leaves its holder's mesh only when every node-admin birth of that
    /// mesh is gone): the fabric primary P holds both seats of mesh1; mesh1.admin.2 is Ready with a
    /// HIGHER NodeId than mesh2's primary. P's replace, retire, restart, drain and stop are executed
    /// by mesh1.admin.2, the admin its own mesh seats once P's birth is gone, never by the
    /// lowest-NodeId mesh primary of another mesh. With no other admin in mesh1, the seat leaves the
    /// mesh: mesh2's primary executes.
    #[test]
    fn the_fabric_primarys_own_operations_go_to_its_own_meshs_successor_whatever_the_node_ids() {
        let with = |path: &str, id: &str, primary: bool, fabric: bool| {
            let mut n = node(path, primary, fabric);
            n.node_id = rafka_mesh_entity::ids::NodeId::parse(id).unwrap();
            n
        };
        let mut t = mm(true);
        t.nodes = vec![
            with("mesh1.admin.1", "5000000000aa", true, true),
            with("mesh1.admin.2", "9000000000aa", false, false),
            with("mesh1.rpc.1", "6000000000aa", true, false),
            with("mesh2.admin.1", "1000000000aa", true, false),
        ];
        let p: PathName = "mesh1.admin.1".parse().unwrap();
        let birth = || crate::model::IncarnationId::mint();
        for op in [
            BuildOperation::RetireNode { node: p.clone(), exited: None },
            BuildOperation::RestartNode { node: p.clone() },
            BuildOperation::DrainNode { node: p.clone(), from_incarnation: birth() },
            BuildOperation::StopNode { node: p.clone(), from_incarnation: birth() },
        ] {
            assert_eq!(who(&op, &t), "mesh1.admin.2", "{op:?}: P's own mesh seats mesh1.admin.2 once P is gone; mesh2.admin.1's lower NodeId is no input");
        }
        // mesh1's only admin: no birth of mesh1 survives, so the seat leaves the mesh.
        t.nodes.retain(|n| n.name.to_string() != "mesh1.admin.2");
        assert_eq!(who(&BuildOperation::StopNode { node: p.clone(), from_incarnation: birth() }, &t), "mesh2.admin.1");
    }

    #[test]
    fn a_mesh_without_an_admin_primary_has_its_members_run_by_the_fabric_primary() {
        let t = mm(false);
        assert_eq!(who(&create("mesh2.rpc.1"), &t), "mesh1.admin.1");
        assert_eq!(who(&create("mesh2.admin.1"), &t), "mesh1.admin.1", "the first admin of a mesh that has none is the fabric primary's");
        assert_eq!(lead_for(&[], &t).map(|p| p.to_string()).as_deref(), Some("mesh1.admin.1"), "an empty plan is closed by the fabric primary");
        assert_eq!(lead_for(&[create("mesh2.rpc.1")], &mm(true)).map(|p| p.to_string()).as_deref(), Some("mesh2.admin.1"));
    }

    /// A runner that realises the replace recipe's stop and rename of the seat holder P in the shared
    /// view, as the real ops do: the stop moves P's seats to the executor by TakeSeats and parks P;
    /// the rename lists P at `<P>.old`.
    struct RecipeRunner {
        view: Arc<RwLock<Topology>>,
        ran: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl OperationRunner for RecipeRunner {
        async fn run(&self, _build_id: &BuildId, _attempt: u32, op: &BuildOperation) -> Result<(), String> {
            use rafka_node_rpc_contract::build::RecipeStep;
            self.ran.lock().unwrap().push(op.key());
            let BuildOperation::RecipeStep { step, .. } = op else { return Ok(()) };
            let mut t = self.view.write().await;
            match step {
                RecipeStep::Stop { node } => {
                    let heir = node.successor();
                    for n in t.nodes.iter_mut() {
                        if n.name == *node {
                            (n.parked, n.status, n.is_primary, n.is_fabric_primary) = (true, NodeStatus::Leaving, false, false);
                        } else if n.name == heir {
                            (n.is_primary, n.is_fabric_primary) = (true, true);
                        }
                    }
                }
                RecipeStep::Update { node, name } => {
                    for n in t.nodes.iter_mut().filter(|n| n.name == *node) {
                        n.name = name.clone();
                    }
                }
                _ => {}
            }
            Ok(())
        }
    }

    /// CONTRACT (#3087 red 4; node-rpc-envelope.md "Build, op 0x20": "a run that stops at another
    /// executor's operation ends HandedOff { to }" and "Hand-off is the fabric-primary's next claim
    /// and call"; node-admin-lifecycle.md §1.3: the fabric primary's replace "is executed by its
    /// same-mesh successor, which takes its seats"): the fabric primary P plans attempt 5 of its
    /// own replace recipe for the successor it elects, `mesh1.admin.1.new` (lowest Ready NodeId of
    /// P's mesh once P is gone): the run is P's stop and rename, and in P's view the step after it,
    /// the stop of `.new`, is P's own (P is `.new`'s mesh primary). The run's own stop moves both
    /// seats to `.new` and parks P, and its rename lists P at `mesh1.admin.1.old`. The run's trailing
    /// hand-off is resolved from the view it ends with: it hands back to the fabric primary that view
    /// seats, `.new`, whose next claim names the admin that executes what is left there,
    /// `mesh1.admin.2`. Must NOT happen: the run handing off to P, the stopped and renamed seat
    /// holder that planned it (the drive then claims attempt 6 for a birth that can never run it).
    #[tokio::test]
    async fn a_run_that_stops_the_seat_holder_that_planned_it_hands_off_from_the_view_it_ends_with() {
        use rafka_node_rpc_contract::build::RecipeStep;
        let with = |path: &str, id: &str, primary: bool, fabric: bool| {
            let mut n = node(path, primary, fabric);
            n.node_id = rafka_mesh_entity::ids::NodeId::parse(id).unwrap();
            n
        };
        let mut t = mm(false);
        t.nodes = vec![
            with("mesh1.admin.1", "5000000000aa", true, true),
            with("mesh1.admin.2", "9000000000aa", false, false),
            with("mesh1.admin.1.new", "2000000000aa", false, false),
            with("mesh1.rpc.1", "6000000000aa", false, false),
        ];
        let p: PathName = "mesh1.admin.1".parse().unwrap();
        let step = |step: RecipeStep| BuildOperation::RecipeStep { step, birth: None };
        let ops = [
            step(RecipeStep::Stop { node: p.clone() }),
            step(RecipeStep::Update { node: p.clone(), name: p.renamed() }),
            step(RecipeStep::Stop { node: p.successor() }),
            step(RecipeStep::Update { node: p.successor(), name: p.clone() }),
            step(RecipeStep::Start { node: p.clone() }),
            step(RecipeStep::Delete { node: p.renamed() }),
        ];
        // P's plan for attempt 5, as P's drive makes it (`build_drive` plan_dispatch -> run_of).
        let plan = run_of(&ops, &t, &p.successor());
        assert_eq!(plan.operations, ops[..2].to_vec(), "the run is P's stop and rename, executed by .new");
        assert_eq!(plan.hand_off_to.as_deref(), Some("mesh1.admin.1"), "in P's view the step after the run is P's: the plan says more follows");
        let view = Arc::new(RwLock::new(t));
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        let id = BuildId("bld-recipe".into());
        builds
            .publish_accepted(&crate::build_state::BuildAccepted { build_id: id.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let runner = Arc::new(RecipeRunner { view: view.clone(), ran: Default::default() });
        let exec = BuildExecutor { executor: "mesh1.admin.1.new".into(), builds: builds.clone(), topology: view.clone(), runner: runner.clone() };
        let r = exec.run_attempt(&id, 5, &rafka_node_rpc_contract::context::CallContext::default(), &plan).await;
        assert_eq!(*runner.ran.lock().unwrap(), vec!["stop-node:mesh1.admin.1".to_string(), "update-node:mesh1.admin.1".to_string()], "the run ran P's stop and rename");
        let Reconciled::HandedOff { attempt: 5, to, .. } = &r else { panic!("the run hands off: {r:?}") };
        assert_ne!(to, "mesh1.admin.1", "the run handed off to P, which it stopped and renamed: {r:?}");
        assert_eq!(to, "mesh1.admin.1.new", "the run hands back to the fabric primary its ending view seats: {r:?}");
        let end = view.read().await.clone();
        assert_eq!(lead_for(&ops[2..], &end).map(|n| n.to_string()).as_deref(), Some("mesh1.admin.2"), "that fabric primary's next claim names .new's same-mesh successor for what is left");
        let p_after = builds.read_build(&id).await.unwrap();
        assert_eq!(p_after.state, BuildState::Pending, "the Build waits for the next claim");
    }

    #[test]
    fn a_handed_off_attempt_leaves_the_build_waiting_for_its_next_claim() {
        use crate::build_state::{fold, BuildAccepted, BuildFact};
        let id = crate::build::BuildId("bld-x".into());
        let facts = vec![
            BuildFact::Accepted(BuildAccepted {
                build_id: id.clone(),
                topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()),
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
