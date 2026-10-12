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

/// How a run of one operation ended short of complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunEnd {
    /// The operation's own logic ended it with a named failure. The attempt ends `Failed`, and a
    /// Build whose attempt ended so is failed: the current-Build read skips it and a caller
    /// resubmits it (R-BI3, R-BI4).
    Failed(String),
    /// A participant of the operation was lost, or the proof its resume needs is not in yet: the
    /// operation is incomplete and its receipts stand. The attempt ends handed back to the
    /// fabric-primary, no terminal verdict is recorded, the Build stays in progress and the next
    /// attempt resumes from the receipts (R-BI4; mesh-delete.md, "Failure and receipts").
    Incomplete(String),
}

impl RunEnd {
    /// Why the run ended short of complete.
    pub fn reason(&self) -> &str {
        match self {
            Self::Failed(r) | Self::Incomplete(r) => r,
        }
    }
}

impl From<String> for RunEnd {
    fn from(reason: String) -> Self {
        Self::Failed(reason)
    }
}

impl std::fmt::Display for RunEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}

/// Realises one planned operation (the deployment and lifecycle pipelines).
#[async_trait::async_trait]
pub trait OperationRunner: Send + Sync {
    /// Realise `op` of `attempt` of `build_id`; `Err` names how it ended short of complete
    /// ([`RunEnd`]).
    async fn run(&self, build_id: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), RunEnd>;

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
        // (mesh-recovery.md:8-9). A mesh with no other live admin lost its mesh-primary: that is the
        // fabric-primary's mesh recovery, no node recovery, and no admin executes one.
        BuildOperation::RetireNode { node, exited: Some(_) } | BuildOperation::CreateNode { node, over: Some(_) } | BuildOperation::UpdateNode { node, .. } => {
            if node.kind == NodeKind::NodeAdmin {
                node_admin_recoverer(node, t)
            } else {
                mesh_executor(&node.mesh, None, t)
            }
        }
        // A member node of any kind is born, restarted and retired by its own mesh's primary
        // admin (the fabric primary when the mesh has none): the rule names no role, so a
        // product's kinds get the same executor as the proof product's rpc node.
        BuildOperation::CreateNode { node, .. } | BuildOperation::RestartNode { node } | BuildOperation::RetireNode { node, .. } | BuildOperation::ReplaceNode { node, .. } | BuildOperation::DeleteConnections { node, .. } | BuildOperation::StopNode { node, .. } | BuildOperation::StartNode { node, .. } | BuildOperation::DeleteNode { node, .. }
            if node.kind != NodeKind::NodeAdmin =>
        {
            mesh_executor(&node.mesh, None, t)
        }
        // A mesh's missing node-admin is created by that mesh's primary node-admin, its claim
        // fenced at the fabric primary (node-admin-lifecycle.md §4.3); the fabric primary creates
        // only the first admin of a mesh that has none.
        BuildOperation::CreateNode { node, .. } if node.kind == NodeKind::NodeAdmin => mesh_executor(&node.mesh, None, t),
        // A node-admin that holds a seat is never the executor of its own stop, delete, restart,
        // replace or connections delete, and no admin of its mesh commands it (R-MT1, Luke 2026-10-12:
        // "a node-admin in the mesh shouldn't issue a command to the mesh-primary"; "only a FP should
        // issue commands to a MP"). The fabric-primary's drive moves the seat first by
        // node.primary.update ([`seat_move`]): until it has, no admin executes the operation. Once the
        // seat moved the target is a member like any other, executed by its mesh's primary. The delete
        // of a mesh's only node moves no seat: the fabric primary, which owns what happens to a mesh
        // that has no other admin.
        BuildOperation::RetireNode { node, .. } | BuildOperation::RestartNode { node } | BuildOperation::ReplaceNode { node, .. } | BuildOperation::DeleteConnections { node, .. } | BuildOperation::StopNode { node, .. } | BuildOperation::StartNode { node, .. } | BuildOperation::DeleteNode { node, .. } => {
            if seat_move(op, t).is_some() {
                None
            } else if holds_seat(node, t) {
                held_admins(&node.mesh, Some(node), t).is_empty().then(fabric).flatten().filter(|fp| fp != node)
            } else {
                mesh_executor(&node.mesh, Some(node), t)
            }
        }
        BuildOperation::CreateNode { .. } | BuildOperation::CreateMesh { .. } | BuildOperation::ShutdownMesh { .. } => fabric(),
    }
}

/// The executor of an operation on a node of `mesh` (not `target` itself): the mesh's primary
/// admin; the fabric primary only when the mesh has no node-admin left that is not proven gone
/// (R-A2: cross-mesh only when the whole cohort is proven lost). An admin held but not seated (silent,
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
        BuildOperation::CreateNode { node, .. } | BuildOperation::RestartNode { node } | BuildOperation::RetireNode { node, .. } | BuildOperation::ReplaceNode { node, .. } | BuildOperation::DeleteConnections { node, .. } | BuildOperation::StopNode { node, .. } | BuildOperation::StartNode { node, .. } | BuildOperation::UpdateNode { node, .. } | BuildOperation::DeleteNode { node, .. } => node,
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
/// place once its exited birth is gone (a proven loss vacates the seat, elections §5). `None` when no
/// other live admin of the mesh is left: the mesh lost its mesh-primary, which is the
/// fabric-primary's mesh recovery.
///
/// The seat is read from the view's election, not from liveness: a silent holder keeps its seat until
/// it is proven gone (`crate::election`), and its own recovery is still its mesh's to run.
pub fn node_admin_recoverer(node: &PathName, t: &Topology) -> Option<PathName> {
    match t.cohort(&node.mesh, NodeKind::NodeAdmin).find(|n| n.is_primary) {
        Some(p) if p.name == *node => seated_after_loss(node, t),
        Some(p) if p.status.is_live() => Some(p.name.clone()),
        _ => None,
    }
}

/// The admin the election seats as `target`'s mesh primary once `target`'s exited birth is proven
/// gone (elections §5: "a holder ... whose exact birth is proven gone hands the seat to the lowest
/// ReadyForTraffic NodeId of its own mesh"). A proven loss only: a planned move is node.primary.update.
fn seated_after_loss(target: &PathName, t: &Topology) -> Option<PathName> {
    let holder = t.node(target)?;
    let nodes: Vec<crate::model::Node> = t.members().cloned().collect();
    let mut inc = crate::election::Incumbency::default();
    inc.lost.extend(holder.incarnation_id.clone());
    crate::election::cohort_successor(&nodes, &inc, holder).map(|n| n.name)
}

/// Whether `node` holds a seat in view `t`: its mesh's node-admin primary or the fabric primary.
fn holds_seat(node: &PathName, t: &Topology) -> bool {
    node.kind == NodeKind::NodeAdmin && (t.cohort_primary(&node.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.name == *node) || t.fabric_primary().is_some_and(|fp| fp.name == *node))
}

/// The seats `op` moves before it runs (R-MT1: node.primary.update "should be called on MP stop or
/// delete. Caveat: delete is skipped if it's the only node on the mesh"): the node-admin it stops,
/// deletes, restarts, replaces or deletes the connections of, when that node holds its mesh's primary
/// seat or the fabric seat in view `t`. `None` for any other operation, a target that holds no seat,
/// and the delete of a mesh's only node. A holder with no other node-admin in its mesh is still named:
/// node.primary.update refuses it by name (the Build door refuses it first, `http::last_admin`).
pub fn seat_move(op: &BuildOperation, t: &Topology) -> Option<SeatMove> {
    let (node, delete) = match op {
        BuildOperation::RetireNode { node, exited: None } => (node, true),
        BuildOperation::RestartNode { node } | BuildOperation::ReplaceNode { node, .. } | BuildOperation::DeleteConnections { node, .. } | BuildOperation::StopNode { node, .. } => (node, false),
        _ => return None,
    };
    if node.kind != NodeKind::NodeAdmin {
        return None;
    }
    let holder = t.node(node)?.clone();
    let mesh_seat = t.cohort_primary(&node.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.node_id == holder.node_id);
    let fabric_seat = t.fabric_primary().is_some_and(|fp| fp.node_id == holder.node_id);
    if !mesh_seat && !fabric_seat {
        return None;
    }
    let only_node = t.members().filter(|n| n.mesh == node.mesh).all(|n| n.node_id == holder.node_id);
    if delete && only_node {
        return None;
    }
    Some(SeatMove { holder, mesh_seat, fabric_seat })
}

/// What the fabric-primary's drive moves before an operation on a seat holder runs ([`seat_move`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatMove {
    /// The node-admin that holds the seats.
    pub holder: crate::model::Node,
    /// It holds its mesh's primary seat: node.primary.update moves it.
    pub mesh_seat: bool,
    /// It holds the fabric seat: node.fabric-primary.update moves it, after its mesh seat.
    pub fabric_seat: bool,
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
            // The admin that held the attempt before this one. The fabric-primary's claim of this
            // attempt is already folded (the executor absorbs its intent first), so `executor` here
            // names this admin.
            previous_executor = previous.as_ref().and_then(|b| b.attempt_executors.get(&attempt.saturating_sub(1))).map(String::as_str).unwrap_or(""),
            reason = previous.as_ref().map(|b| b.reason.as_str()).unwrap_or(""),
            action = %previous.as_ref().and_then(|b| b.action.as_ref()).map(|a| serde_json::to_string(a).unwrap_or_default()).unwrap_or_default(),
            operations = tracing::field::Empty,
            incomplete = tracing::field::Empty,
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
            match self.runner.run(&build.build_id, attempt, op).await {
                Ok(()) => {}
                Err(RunEnd::Failed(reason)) => return self.finish(&build, attempt, Err((op.key(), reason))).await,
                Err(RunEnd::Incomplete(reason)) => return self.hand_back(&build, attempt, operations[..i].to_vec(), op, reason, span).await,
            }
        }
        match &plan.hand_off_to {
            Some(to) => self.hand_off(&build, attempt, operations.clone(), to.clone()).await,
            None => self.finish(&build, attempt, Ok(operations.clone())).await,
        }
    }

    /// `op` ended incomplete (R-BI4): a participant of it was lost, or the proof its resume needs is
    /// not in. Its receipts stand and no terminal verdict is recorded. The attempt ends the way a run
    /// that stops at another executor's operation ends: handed to the fabric-primary of the view the
    /// run ends with, which claims the next attempt, and that attempt resumes from the receipts. The
    /// Build stays in progress. The reason is the attempt's blocker, framed to the caller.
    async fn hand_back(&self, build: &BuildProjection, attempt: u32, ran: Vec<BuildOperation>, op: &BuildOperation, reason: String, span: &tracing::Span) -> Reconciled {
        span.record("incomplete", reason.as_str());
        let to = self.topology.read().await.fabric_primary().map(|n| n.name.to_string()).unwrap_or_else(|| self.executor.clone());
        self.builds.note_blocked(&build.build_id, attempt, &op.key(), "incomplete", &reason).await;
        tracing::info!(operation = %op.key(), reason = %reason, to = %to, "the operation ended incomplete: its receipts stand, the attempt is handed to the fabric-primary and the Build stays in progress");
        self.hand_off(build, attempt, ran, to).await
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
    /// no node recovery (its mesh lost its mesh-primary: the fabric-primary's mesh recovery), so no
    /// admin executes one.
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
        // R-MT1: a mesh primary's own operation waits for node.primary.update; no admin executes it
        // while it holds the seat, and once the seat moved its mesh's new primary does.
        assert_eq!(who(&BuildOperation::RestartNode { node: p2.parse().unwrap() }, &t3), "", "a mesh primary's own restart waits for its seat to move");
        assert_eq!(who(&BuildOperation::RetireNode { node: p2.parse().unwrap(), exited: None }, &t3), "", "a mesh primary's own retire waits for its seat to move");
        let mut moved = t3.clone();
        for n in moved.nodes.iter_mut().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin) {
            n.is_primary = n.name.to_string() == m2;
        }
        assert_eq!(who(&BuildOperation::RestartNode { node: p2.parse().unwrap() }, &moved), m2, "once the seat moved, the new mesh primary executes the former holder's restart");
        let replace = |n: &str| BuildOperation::ReplaceNode { node: n.parse().unwrap(), from_incarnation: crate::model::IncarnationId::mint() };
        assert_eq!(who(&replace("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's replace is its mesh primary's");
        assert_eq!(who(&replace(m2), &t3), p2, "a non-primary admin's replace is its mesh primary's");
        assert_eq!(who(&replace(p2), &t3), "", "a mesh primary's own replace waits for its seat to move");
        assert_eq!(who(&replace(p2), &moved), m2, "once the seat moved, the new mesh primary executes the replace");
        let connections = |n: &str| BuildOperation::DeleteConnections { node: n.parse().unwrap(), from_incarnation: crate::model::IncarnationId::mint() };
        let stop = |n: &str| BuildOperation::StopNode { node: n.parse().unwrap(), from_incarnation: crate::model::IncarnationId::mint() };
        assert_eq!(who(&connections("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's node.connections.delete is its mesh primary's");
        assert_eq!(who(&stop("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's stop is its mesh primary's");
        assert_eq!(who(&connections(m2), &t3), p2, "a non-primary admin's node.connections.delete is its mesh primary's");
        assert_eq!(who(&stop(p2), &t3), "", "a mesh primary's own stop waits for its seat to move");
        assert_eq!(who(&stop(p2), &moved), m2, "once the seat moved, the new mesh primary executes the stop");
        assert!(seat_move(&stop(p2), &t3).is_some_and(|m| m.mesh_seat && !m.fabric_seat), "the stop of mesh2's primary moves its mesh seat first");
        assert_eq!(who(&BuildOperation::CreateMesh { mesh: "mesh3".into(), mesh_id: MeshId::mint() }, &t), "mesh1.admin.1");
        assert_eq!(who(&shutdown("mesh2"), &t), "mesh1.admin.1");
    }

    /// CONTRACT (Luke 2026-10-08, the fabric primary is handed off, never wiped out; R-MT1): a retire
    /// or restart of the fabric primary is never executed by the target itself: no admin executes it
    /// until its seats moved by node.primary.update and node.fabric-primary.update. Another admin's
    /// retire or restart is its own mesh primary's (mesh2's only admin, its mesh's only node, has no
    /// mesh primary beside it: the fabric primary).
    #[test]
    fn the_fabric_primary_never_executes_its_own_retire_or_restart() {
        let mut t = mm(true);
        crate::election::resolve(&mut t.nodes);
        let fp = t.fabric_primary().expect("a fabric primary").name.clone();
        let other = t.nodes.iter().find(|n| n.kind == NodeKind::NodeAdmin && n.name != fp).unwrap().name.clone();
        for op in [BuildOperation::RetireNode { node: fp.clone(), exited: None }, BuildOperation::RestartNode { node: fp.clone() }] {
            assert_eq!(executor_for(&op, &t), None, "{op:?}: never {fp}; no admin until its seats moved");
            assert!(seat_move(&op, &t).is_some_and(|m| m.fabric_seat), "{op:?}: the fabric seat moves first");
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

    /// CONTRACT (R-MT1: the holder computes its successor; fabric-node-lifecycle-elections.md section
    /// 5: "a holder that yields ... hands the seat to the lowest ReadyForTraffic NodeId of its own
    /// mesh"; section 4.3: the fabric seat leaves its holder's mesh only when no node-admin of that
    /// mesh can take it): the fabric primary P holds both seats of mesh1; mesh1.admin.2 is Ready with
    /// a HIGHER NodeId than mesh2's primary. Both of P's seats go to mesh1.admin.2, the mesh seat by
    /// the cohort election and the fabric seat once mesh1.admin.2 holds the mesh seat, never to the
    /// lowest-NodeId mesh primary of another mesh. With no other admin in mesh1 the mesh seat has no
    /// successor (node.primary.update refuses it) and the fabric seat would leave the mesh.
    #[test]
    fn the_fabric_primarys_seats_go_to_its_own_meshs_successor_whatever_the_node_ids() {
        let with = |path: &str, id: &str, primary: bool, fabric: bool| {
            let mut n = node(path, primary, fabric);
            n.node_id = rafka_mesh_entity::ids::NodeId::parse(id).unwrap();
            n
        };
        let mut nodes = vec![
            with("mesh1.admin.1", "5000000000aa", true, true),
            with("mesh1.admin.2", "9000000000aa", false, false),
            with("mesh1.rpc.1", "6000000000aa", true, false),
            with("mesh2.admin.1", "1000000000aa", true, false),
        ];
        let inc = crate::election::Incumbency::default();
        let p = nodes[0].clone();
        let name = |n: Option<crate::model::Node>| n.map(|n| n.name.to_string()).unwrap_or_default();
        assert_eq!(name(crate::election::cohort_successor(&nodes, &inc, &p)), "mesh1.admin.2", "the mesh seat: mesh1's other admin; mesh2.admin.1's lower NodeId is no input");
        assert_eq!(name(crate::election::fabric_successor_yielding(&nodes, &inc, &p)), "mesh1.admin.2", "the fabric seat stays in mesh1");
        // The mesh seat moved first: mesh1.admin.2 is mesh1's primary and takes the fabric seat.
        nodes[0].is_primary = false;
        nodes[1].is_primary = true;
        assert_eq!(name(crate::election::fabric_successor_yielding(&nodes, &inc, &p)), "mesh1.admin.2");
        // mesh1's only admin: no successor for the mesh seat; the fabric seat would leave mesh1.
        nodes.retain(|n| n.name.to_string() != "mesh1.admin.2");
        nodes[0].is_primary = true;
        assert_eq!(name(crate::election::cohort_successor(&nodes, &inc, &p)), "", "no other node-admin of mesh1");
        assert_eq!(name(crate::election::fabric_successor_yielding(&nodes, &inc, &p)), "mesh2.admin.1");
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
        let id = crate::build::BuildId::parse("b7swg3jj790z").unwrap();
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
