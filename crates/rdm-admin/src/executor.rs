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
    /// Realise `op` of `attempt` of `build_id` in `lane` (`None` the fabric lane, `Some(mesh)` that
    /// mesh's drift lane, R-BI7: its receipts carry the lane); `Err` names how it ended short of
    /// complete ([`RunEnd`]).
    async fn run(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), RunEnd>;
}

/// The admin that executes `op` in view `t`; `None` while no admin can.
pub fn executor_for(op: &BuildOperation, t: &Topology) -> Option<PathName> {
    let fabric = || t.fabric_primary().map(|n| n.name.clone());
    match op {
        // Every operation of a node recovery (its node.delete, then its node.create) is
        // executed by the exited birth's recoverer (node-recovery.md "Reconciler", R-ON8): a member's
        // mesh-primary; for a lost node-admin of a mesh with a live mesh-primary, that mesh-primary,
        // and a mesh primary's own recovery by the admin its mesh seats in its place
        // (mesh-recovery.md:8-9). A mesh with no other live admin lost its mesh-primary: that is no
        // node recovery, and no admin executes one. Its node-admin is created by a recipe's node.create
        // (below), the fabric-primary's once the mesh holds no admin that is not proven gone
        // (mesh-investigation.md "A node-admin joins the live mesh").
        BuildOperation::RetireNode { node, exited: Some(_) } | BuildOperation::CreateNode { node, over: Some(_) } => {
            if node.kind == NodeKind::NodeAdmin {
                node_admin_recoverer(node, t)
            } else {
                mesh_executor(&node.mesh, None, t)
            }
        }
        // A recipe step is executed as the op it calls: a create as node.create, every other step as
        // the node op on its node (the mesh-primary of the node's mesh writes its row, R13).
        BuildOperation::RecipeStep { step: rdm_core::build::RecipeStep::Create { node }, .. } => mesh_executor(&node.mesh, None, t),
        BuildOperation::RecipeStep { step, .. } => node_op_executor(step.node(), op, t),
        // A member node of any kind is born by its own mesh's primary admin (the fabric primary when
        // the mesh has none): the rule names no role, so a product's kinds get the same executor as
        // the proof product's rpc node. A mesh's missing node-admin is created by that mesh's primary
        // node-admin, its claim fenced at the fabric primary (node-admin-lifecycle.md §4.3); the
        // fabric primary creates only the first admin of a mesh that has none.
        BuildOperation::CreateNode { node, .. } => mesh_executor(&node.mesh, None, t),
        BuildOperation::RetireNode { node, .. }
        | BuildOperation::RestartNode { node }
        | BuildOperation::DeleteConnections { node, .. }
        | BuildOperation::StopNode { node, .. }
        | BuildOperation::StartNode { node, .. }
        | BuildOperation::DeleteNode { node, .. } => node_op_executor(node, op, t),
        // A mesh retire whose mesh holds the fabric seat waits for node.fabric-primary.update ([`seat_move`]):
        // the new fabric-primary, outside the leaving mesh, drives the rest (mesh-delete.md step 1).
        BuildOperation::ShutdownMesh { .. } if seat_move(op, t).is_some() => None,
        BuildOperation::CreateMesh { .. } | BuildOperation::ShutdownMesh { .. } | BuildOperation::DeleteFabric { .. } => fabric(),
    }
}

/// The executor of a node op (restart, retire, connections delete, stop, start, delete, and a
/// recipe's step) on `node` in view `t`.
fn node_op_executor(node: &PathName, op: &BuildOperation, t: &Topology) -> Option<PathName> {
    let fabric = || t.fabric_primary().map(|n| n.name.clone());
    // The holder of its cohort's seat moves that seat first, by node.primary.update its mesh-primary
    // commands and writes (proposal Part C1, Q3; [`seat_move`]): until it has, no admin executes the
    // operation.
    if node.kind != NodeKind::NodeAdmin {
        return if seat_move(op, t).is_some() { None } else { mesh_executor(&node.mesh, None, t) };
    }
    // A node-admin that holds a seat is never the executor of its own stop, delete, restart or
    // connections delete, and no admin of its mesh commands it (R-MT1, Luke 2026-10-12: "a
    // node-admin in the mesh shouldn't issue a command to the mesh-primary"; "only a FP should
    // issue commands to a MP"). The fabric-primary's drive moves the seat first by
    // node.primary.update ([`seat_move`]): until it has, no admin executes the operation. Once the
    // seat moved the target is a member like any other, executed by its mesh's primary. The delete
    // of a mesh's only node moves no mesh seat (a fabric seat on it moves out of its mesh first):
    // the fabric primary, which owns what happens to a mesh that has no other admin, executes it.
    if seat_move(op, t).is_some() {
        None
    } else if holds_seat(node, t) {
        let last_mesh_gone = !t.meshes.is_empty() && t.meshes.iter().all(|m| m.status == crate::model::ScopeStatus::Dead) && t.node(node).is_some_and(|n| t.only_node(&node.mesh, &n.node_id));
        held_admins(&node.mesh, Some(node), t).is_empty().then(fabric).flatten().filter(|fp| fp != node || last_mesh_gone)
    } else {
        mesh_executor(&node.mesh, Some(node), t)
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
        BuildOperation::CreateNode { node, .. }
        | BuildOperation::RestartNode { node }
        | BuildOperation::RetireNode { node, .. }
        | BuildOperation::DeleteConnections { node, .. }
        | BuildOperation::StopNode { node, .. }
        | BuildOperation::StartNode { node, .. }
        | BuildOperation::DeleteNode { node, .. } => node,
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
/// place once its exited birth is gone (a proven loss vacates the seat, elections §5). `None` when no
/// other live admin of the mesh is left: the mesh lost its mesh-primary, which no node recovery
/// restores.
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
/// delete. Caveat: delete is skipped if it's the only node on the mesh"): the node of any other type it
/// stops, deletes, restarts or deletes the connections of, when that node holds its cohort's
/// seat (Part C1, Q3: its mesh-primary commands the update); the node-admin it stops,
/// deletes, restarts or deletes the connections of, when that node holds its mesh's primary
/// seat or the fabric seat in view `t`; and for a mesh retire of the mesh that holds the fabric seat,
/// the fabric seat alone (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part
/// C1 "The last node-admin of a mesh": "mesh.delete never updates the mesh primary ... If the mesh
/// holds the fabric seat, that seat moves first by `node.fabric-primary.update` with the departing mesh
/// excluded (mesh-delete.md step 1); nothing else moves"). The delete of a mesh's only node moves no
/// mesh seat; when that node holds the fabric seat, the fabric seat alone moves out of its mesh (C2
/// "Successor search for the commanded transfer", step 2: "only when that mesh has none ... the lowest
/// NodeId among the other ReadyForTraffic mesh-primaries"). `None` for any other operation, a target
/// that holds no seat, and the delete of a mesh's only node that holds no fabric seat. A holder with no
/// other node-admin in its mesh is still named: node.primary.update refuses it by name (the Build door
/// refuses it first, `control_plane::last_admin`).
pub fn seat_move(op: &BuildOperation, t: &Topology) -> Option<SeatMove> {
    use rdm_core::build::RecipeStep;
    if let BuildOperation::ShutdownMesh { mesh, .. } = op {
        if !t.meshes.iter().any(|m| m.name != *mesh && m.status != crate::model::ScopeStatus::Dead) {
            return None;
        }
        let holder = t.fabric_primary().filter(|fp| fp.mesh == *mesh)?.clone();
        return Some(SeatMove { holder, mesh_seat: false, fabric_seat: true, cohort_seat: false });
    }
    let (node, delete) = match op {
        BuildOperation::RetireNode { node, exited: None } | BuildOperation::RecipeStep { step: RecipeStep::Delete { node }, .. } => (node, true),
        BuildOperation::RestartNode { node }
        | BuildOperation::DeleteConnections { node, .. }
        | BuildOperation::StopNode { node, .. }
        | BuildOperation::RecipeStep { step: RecipeStep::Stop { node }, .. } => (node, false),
        _ => return None,
    };
    if node.kind != NodeKind::NodeAdmin {
        // Any other cohort's seat: its holder's seat moves first, by node.primary.update its mesh-primary
        // commands and writes (proposal Part C1: "One primitive moves the primary of any (mesh,
        // node_type) cohort"; Q3), except when retiring the cohort's only live node. A holder with
        // no other node of its type is still named for non-delete actions: the update refuses it by Q7's
        // name (the Build door refuses it first, `control_plane::last_admin`).
        let holder = t.node(node)?.clone();
        let only_of_cohort = t.cohort(&node.mesh, node.kind).filter(|n| !n.exit_proven).all(|n| n.node_id == holder.node_id);
        if !t.cohort_primary(&node.mesh, node.kind).is_some_and(|p| p.node_id == holder.node_id) || (delete && only_of_cohort) {
            return None;
        }
        return Some(SeatMove { holder, mesh_seat: false, fabric_seat: false, cohort_seat: true });
    }
    let holder = t.node(node)?.clone();
    let mesh_seat = t.cohort_primary(&node.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.node_id == holder.node_id);
    let fabric_seat = t.fabric_primary().is_some_and(|fp| fp.node_id == holder.node_id);
    if !mesh_seat && !fabric_seat {
        return None;
    }
    // The delete of a mesh's only node skips node.primary.update; a fabric seat on it still moves, out
    // of its mesh, which has no other node-admin to take it (C2 search step 2), unless every mesh of
    // the fabric is already Dead (fabric-delete.md step 4: the separate node.delete of the fabric-primary).
    if delete && t.only_node(&node.mesh, &holder.node_id) {
        if !t.meshes.is_empty() && t.meshes.iter().all(|m| m.status == crate::model::ScopeStatus::Dead) {
            return None;
        }
        return fabric_seat.then_some(SeatMove { holder, mesh_seat: false, fabric_seat: true, cohort_seat: false });
    }
    Some(SeatMove { holder, mesh_seat, fabric_seat, cohort_seat: false })
}

/// What a drive moves before an operation on a seat holder runs ([`seat_move`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatMove {
    /// The node-admin that holds the seats.
    pub holder: crate::model::Node,
    /// It holds its mesh's primary seat: node.primary.update moves it.
    pub mesh_seat: bool,
    /// It holds the fabric seat: node.fabric-primary.update moves it, in the mesh seat's one row when
    /// both move; alone (the mesh seat stays) it moves out of the holder's mesh (C2 search step 2).
    pub fabric_seat: bool,
    /// It holds the seat of its (mesh, node type) cohort, a type other than node-admin:
    /// node.primary.update moves it, commanded and written by its mesh-primary (proposal Part C1, Q3).
    pub cohort_seat: bool,
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
    /// The path name this admin holds now (`builds.held_name()` when joined to the fabric Build
    /// topic, else the name its topology view holds for this admin, or its birth `executor`).
    pub async fn held_name(&self) -> String {
        if let Some(held) = self.builds.held_name() {
            return held.to_string();
        }
        if let Ok(birth) = self.executor.parse::<PathName>() {
            let t = self.topology.read().await;
            if t.node(&birth).is_none() {
                if birth.new && t.node(&birth.base()).is_some() {
                    return birth.base().to_string();
                }
                if let Some(p) = t.cohort_primary(&birth.mesh, NodeKind::NodeAdmin) {
                    return p.name.to_string();
                }
                let mut admins = t.nodes.iter().filter(|n| n.name.mesh == birth.mesh && n.name.kind == NodeKind::NodeAdmin);
                if let (Some(only), None) = (admins.next(), admins.next()) {
                    return only.name.to_string();
                }
            }
        }
        self.executor.clone()
    }

    /// Run attempt `attempt` of `build_id`, which the fabric-primary won for this admin. The span is
    /// parented to the attempt's context the claim returned, so a restart reads under its request
    /// and a drift repair under the span that proved it.
    ///
    /// `lane` is the attempt's lane: `None` the fabric lane, `Some(mesh)` that mesh's drift lane
    /// (R-BI7), which the mesh's own primary claimed and runs: every fact the run writes carries the
    /// lane and every read of the Build is the lane's ([`crate::build_run::LaneBuilds`]).
    pub async fn run_attempt(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, context: &rdm_core::context::CallContext, plan: &RunPlan) -> Reconciled {
        use tracing::Instrument;
        let builds = crate::build_run::LaneBuilds::of(&self.builds, lane);
        let previous = builds.read_build(build_id).await.ok();
        let me = self.held_name().await;
        let span = tracing::info_span!(
            "rdm.node_admin.build.update.via-reconcile",
            build_id = %build_id.log(),
            // The attempt's lane: "" the fabric lane, else the mesh whose drift lane numbers it (R-BI7:
            // an attempt is `(build_id, lane, attempt)`, so the number alone names two attempts).
            lane = lane.unwrap_or(""),
            attempt,
            executor = %me,
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
            rdm_transport::set_remote_parent(&span, tp, context.tracestate.as_deref());
        }
        let r = self.run_claimed(&*builds, build_id, lane, attempt, &me, plan, &span).instrument(span.clone()).await;
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

    #[allow(clippy::too_many_arguments)]
    async fn run_claimed(&self, builds: &dyn BuildStateAdapter, build_id: &BuildId, lane: Option<&str>, attempt: u32, me: &str, plan: &RunPlan, span: &tracing::Span) -> Reconciled {
        // The claim was decided (by the fabric-primary for the fabric lane, by this mesh-primary for
        // its mesh's drift lane); this admin's own log takes the fact with it.
        let claim = BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt, executor: me.to_string() };
        if let Err(e) = builds.adopt_claim(&claim).await {
            return Reconciled::Failed { attempt, operation: String::new(), reason: format!("recording the claim of attempt {attempt}: {e}") };
        }
        let build = match builds.read_build(build_id).await {
            Ok(b) => b,
            Err(e) => return Reconciled::Failed { attempt, operation: String::new(), reason: format!("reading Build {build_id} after the claim of attempt {attempt}: {e}") },
        };
        let operations = &plan.operations;
        span.record("operations", operations.iter().map(BuildOperation::key).collect::<Vec<_>>().join(",").as_str());
        for (i, op) in operations.iter().enumerate() {
            // After the first operation the view can name another executor for the rest: the run
            // seated a mesh's first admin, which executes that mesh's members from then on. The
            // operation then belongs to the admin the view now names, and it is passed to it unrun;
            // an operation a view does not place stays with this admin. The operations themselves are
            // always the fabric-primary's plan.
            if i > 0 && lane.is_none() {
                let now = self.held_name().await;
                if let Some(to) = executor_for(op, &*self.topology.read().await).filter(|to| to.to_string() != now) {
                    return self.hand_off(builds, &build, attempt, operations[..i].to_vec(), to.to_string()).await;
                }
            }
            match self.runner.run(&build.build_id, lane, attempt, op).await {
                Ok(()) => {}
                Err(RunEnd::Failed(reason)) => return self.finish(builds, &build, attempt, Err((op.key(), reason))).await,
                Err(RunEnd::Incomplete(reason)) => return self.hand_back(builds, &build, attempt, operations[..i].to_vec(), op, reason, span).await,
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
                Some(to) => self.hand_off(builds, &build, attempt, operations.clone(), to).await,
                // No operation failed: the receipt names none, so no step is blamed for it.
                None => {
                    let last = operations.last().map(BuildOperation::key).unwrap_or_else(|| "no operation".into());
                    let reason = format!("{me}: the run of attempt {attempt} ended after {last} with more of the plan to follow, and this admin's view seats no fabric primary to hand it back to");
                    self.finish(builds, &build, attempt, Err((String::new(), reason))).await
                }
            };
        }
        self.finish(builds, &build, attempt, Ok(operations.clone())).await
    }

    /// `op` ended incomplete (R-BI4): a participant of it was lost, or the proof its resume needs is
    /// not in. Its receipts stand and no terminal verdict is recorded. The attempt ends the way a run
    /// that stops at another executor's operation ends: handed to the fabric-primary of the view the
    /// run ends with, which claims the next attempt, and that attempt resumes from the receipts. The
    /// Build stays in progress. The reason is the attempt's blocker, framed to the caller.
    #[allow(clippy::too_many_arguments)]
    async fn hand_back(&self, builds: &dyn BuildStateAdapter, build: &BuildProjection, attempt: u32, ran: Vec<BuildOperation>, op: &BuildOperation, reason: String, span: &tracing::Span) -> Reconciled {
        span.record("incomplete", reason.as_str());
        let me = self.held_name().await;
        let to = self.topology.read().await.fabric_primary().map(|n| n.name.to_string()).unwrap_or(me);
        builds.note_blocked(&build.build_id, None, attempt, &op.key(), "incomplete", &reason).await;
        tracing::info!(operation = %op.key(), reason = %reason, to = %to, "the operation ended incomplete: its receipts stand, the attempt is handed to the fabric-primary and the Build stays in progress");
        self.hand_off(builds, build, attempt, ran, to).await
    }

    async fn hand_off(&self, builds: &dyn BuildStateAdapter, build: &BuildProjection, attempt: u32, operations: Vec<BuildOperation>, to: String) -> Reconciled {
        let receipt = BuildAttemptReceipt { mesh: None, build_id: build.build_id.clone(), attempt, outcome: AttemptOutcome::HandedOff { to: to.clone() } };
        if let Err(e) = builds.append_attempt_receipt(&receipt).await {
            return Reconciled::Failed { attempt, operation: String::new(), reason: format!("recording attempt {attempt}: {e}") };
        }
        tracing::info!(to = %to, "the next operation belongs to another admin; handed off");
        Reconciled::HandedOff { attempt, operations, to }
    }

    async fn finish(&self, builds: &dyn BuildStateAdapter, build: &BuildProjection, attempt: u32, r: Result<Vec<BuildOperation>, (String, String)>) -> Reconciled {
        let outcome = match &r {
            Ok(_) => AttemptOutcome::Converged,
            Err((op, reason)) if op.is_empty() => AttemptOutcome::Failed { reason: reason.clone() },
            Err((op, reason)) => AttemptOutcome::Failed { reason: format!("{op}: {reason}") },
        };
        let receipt = BuildAttemptReceipt { mesh: None, build_id: build.build_id.clone(), attempt, outcome };
        if let Err(e) = builds.append_attempt_receipt(&receipt).await {
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
    /// member's recovery (its node.delete, then its node.create), a secondary node-admin's
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
            [BuildOperation::RetireNode { node: node.clone(), exited: Some(from.clone()) }, BuildOperation::CreateNode { node, over: Some(from) }]
        };
        let recovered_by = |n: &str, t: &Topology| {
            let who: Vec<String> = recovery(n).iter().map(|op| who(op, t)).collect();
            assert!(who.windows(2).all(|w| w[0] == w[1]), "{n}: one executor runs both operations: {who:?}");
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
        let rename = |n: &str| {
            let node: PathName = n.parse().unwrap();
            BuildOperation::RecipeStep { step: rdm_core::build::RecipeStep::Update { name: node.renamed(), node }, birth: None }
        };
        let make = |n: &str| BuildOperation::RecipeStep { step: rdm_core::build::RecipeStep::Create { node: n.parse().unwrap() }, birth: None };
        assert_eq!(who(&rename("mesh2.rpc.1"), &t), "mesh2.admin.1", "a recipe's update of a member is its mesh primary's, the writer of its row");
        assert_eq!(who(&rename(m2), &t3), p2, "a recipe's update of a non-primary admin is its mesh primary's");
        assert_eq!(who(&rename(p2), &moved), m2, "a recipe's update of a former mesh primary after its stop moved the seat is the new mesh primary's");
        assert_eq!(who(&make("mesh2.rpc.1.new"), &t), "mesh2.admin.1", "a recipe's create is node.create's executor");
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

    /// `mm(true)` with pinned NodeIds: `fp`, the lower, is the node-admin the election seats as the
    /// fabric primary; the other admin and `mesh1.rpc.1` are higher.
    fn seated(fp: &str) -> Topology {
        let mut t = mm(true);
        for n in t.nodes.iter_mut() {
            let id = if n.name.to_string() == fp {
                "1000000000aa"
            } else if n.kind == NodeKind::NodeAdmin {
                "5000000000aa"
            } else {
                "9000000000aa"
            };
            n.node_id = rdm_core::ids::NodeId::parse(id).unwrap();
        }
        crate::election::resolve(&mut t.nodes);
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()), Some(fp.to_string()), "the pinned ids seat {fp}");
        t
    }

    /// CONTRACT (Luke 2026-10-08, the fabric primary is handed off, never wiped out; R-MT1; the
    /// approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C2 "Successor search
    /// for the commanded transfer": "1. the holder's own mesh ... 2. only when that mesh has none, or
    /// it is the departing mesh under mesh.delete: the lowest NodeId among the other ReadyForTraffic
    /// mesh-primaries"): a retire or restart of the fabric primary is never executed by the target
    /// itself: no admin executes it until its seats moved. Beside another node of its mesh, both seats
    /// move (node.primary.update's one row, Q1). As its mesh's only node, its retire skips
    /// node.primary.update (R-MT1: the delete of a mesh's only node) and moves the fabric seat alone,
    /// out of its mesh (C2 step 2), after which the new fabric primary executes it; its restart still
    /// moves both (refused by Q7's text, no secondary). Another admin's retire is its own mesh
    /// primary's (mesh2's only admin, its mesh's only node, has no mesh primary beside it: the fabric
    /// primary). The NodeIds are pinned: which admin holds the fabric seat is the case, never a draw.
    /// Must NOT happen: the fabric primary named the executor of its own retire, or its retire as its
    /// mesh's only node left waiting with no seat to move and no admin to execute it.
    #[test]
    fn the_fabric_primary_never_executes_its_own_retire_or_restart() {
        let retire = |n: &str| BuildOperation::RetireNode { node: n.parse().unwrap(), exited: None };
        let restart = |n: &str| BuildOperation::RestartNode { node: n.parse().unwrap() };
        // mesh1.admin.1 holds the fabric seat, mesh1.rpc.1 beside it.
        let t = seated("mesh1.admin.1");
        for op in [retire("mesh1.admin.1"), restart("mesh1.admin.1")] {
            assert_eq!(executor_for(&op, &t), None, "{op:?}: never the fabric primary; no admin until its seats moved");
            assert!(seat_move(&op, &t).is_some_and(|m| m.mesh_seat && m.fabric_seat), "{op:?}: both seats move first: {:?}", seat_move(&op, &t));
        }
        assert_eq!(who(&retire("mesh2.admin.1"), &t), "mesh1.admin.1", "mesh2's only admin has no mesh primary beside it: the fabric primary retires it");
        // mesh2.admin.1 holds the fabric seat and is its mesh's only node.
        let t = seated("mesh2.admin.1");
        assert_eq!(executor_for(&retire("mesh2.admin.1"), &t), None, "never the fabric primary; no admin until its fabric seat moved");
        assert!(
            seat_move(&retire("mesh2.admin.1"), &t).is_some_and(|m| !m.mesh_seat && m.fabric_seat),
            "the retire of its mesh's only node moves the fabric seat alone: {:?}",
            seat_move(&retire("mesh2.admin.1"), &t)
        );
        assert!(seat_move(&restart("mesh2.admin.1"), &t).is_some_and(|m| m.mesh_seat && m.fabric_seat), "its restart moves both seats");
        assert_eq!(who(&retire("mesh1.admin.1"), &t), "", "mesh1's primary, beside mesh1.rpc.1, waits for its own seat to move");
        let mut moved = t.clone();
        for n in moved.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh1.admin.1";
        }
        assert_eq!(seat_move(&retire("mesh2.admin.1"), &moved), None, "once the fabric seat moved, nothing is owed");
        assert_eq!(who(&retire("mesh2.admin.1"), &moved), "mesh1.admin.1", "the new fabric primary executes it");
        // Alone, the fabric primary has no successor: nobody executes its retire.
        let mut alone = mm(false);
        crate::election::resolve(&mut alone.nodes);
        let fp = alone.fabric_primary().unwrap().name.clone();
        assert_eq!(executor_for(&BuildOperation::RetireNode { node: fp, exited: None }, &alone), None);
    }

    /// CONTRACT (#3016; elections §5: a holder proven gone "hands the seat to the lowest
    /// ReadyForTraffic NodeId of its own mesh"; R-A2: cross-mesh only when the whole cohort is proven lost): mesh2's seated admin is
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
        // mesh2.rpc.1 holds mesh2's rpc seat; the restart is of mesh2.rpc.3, which holds none, so no seat
        // moves before it and its executor alone is the question.
        t.nodes.push(node("mesh2.rpc.1", true, false));
        t.nodes.push(node("mesh2.rpc.3", false, false));
        for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.1") {
            n.status = NodeStatus::Dead;
            n.is_primary = false;
        }
        let ops = [BuildOperation::RestartNode { node: "mesh2.rpc.3".parse().unwrap() }, create("mesh2.rpc.2"), create("mesh2.admin.3"), BuildOperation::RetireNode { node: "mesh2.admin.1".parse().unwrap(), exited: None }];
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
        BuildOperation::ShutdownMesh { mesh: mesh.into(), mesh_id: rdm_core::MeshId::mint() }
    }

    /// CONTRACT (R-ST2; the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1
    /// "The last node-admin of a mesh": "If the mesh holds the fabric seat, that seat moves first by
    /// `node.fabric-primary.update` with the departing mesh excluded (mesh-delete.md step 1); nothing
    /// else moves"): `mesh.delete:<mesh_id>` is the fabric primary's. When the fabric primary is
    /// inside the leaving mesh, its fabric seat alone moves first ([`seat_move`]: the mesh seat stays),
    /// no admin executes the retire until it has, and the executor is then the new fabric primary.
    /// Must NOT happen: the leaving mesh's admin named the executor of its own mesh's retire.
    #[test]
    fn a_mesh_retire_is_the_fabric_primarys() {
        let t = mm(true);
        assert_eq!(who(&shutdown("mesh2"), &t), "mesh1.admin.1", "the fabric primary, outside mesh2");
        assert_eq!(seat_move(&shutdown("mesh2"), &t), None, "a retire of a mesh the fabric seat is not in moves no seat");
        assert_eq!(who(&shutdown("mesh1"), &t), "", "the fabric primary, inside mesh1: no admin executes the retire until its seat moved");
        assert!(
            seat_move(&shutdown("mesh1"), &t).is_some_and(|m| m.holder.name.to_string() == "mesh1.admin.1" && !m.mesh_seat && m.fabric_seat),
            "the fabric seat alone moves first: {:?}",
            seat_move(&shutdown("mesh1"), &t)
        );
        let mut moved = mm(true);
        for n in moved.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh2.admin.1";
        }
        assert_eq!(seat_move(&shutdown("mesh1"), &moved), None, "once the seat moved, nothing is owed");
        assert_eq!(who(&shutdown("mesh1"), &moved), "mesh2.admin.1", "once the seat moved, the new fabric primary is the executor");
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C2
    /// "Successor search for the commanded transfer": "1. the holder's own mesh: the lowest
    /// ReadyForTraffic node-admin NodeId there ... 2. only when that mesh has none, or it is the
    /// departing mesh under mesh.delete: the lowest NodeId among the other ReadyForTraffic
    /// mesh-primaries; 3. neither found: refused by name"; Q1: both seats go to the same-mesh
    /// successor in one row): P, mesh1's primary and the fabric-primary, has a HIGHER NodeId than
    /// mesh2's primary. Both seats go to mesh1.admin.2, never to the lowest-NodeId mesh primary of
    /// another mesh. Under mesh.delete the fabric seat alone goes to mesh2's primary. With no other
    /// admin in mesh1 both seats are refused with Q7's mesh text (mesh2's primary could take the fabric
    /// seat, the mesh seat has no secondary), and with mesh2 gone too with the fabric seat's text.
    #[test]
    fn the_fabric_primarys_seats_go_to_its_own_meshs_successor_whatever_the_node_ids() {
        use crate::election::{commanded_successor, Moving};
        let with = |path: &str, id: &str, primary: bool, fabric: bool| {
            let mut n = node(path, primary, fabric);
            n.node_id = rdm_core::ids::NodeId::parse(id).unwrap();
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
        let name = |n: Result<crate::model::Node, String>| n.map(|n| n.name.to_string()).unwrap_or_else(|e| e);
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::BothSeats)), "mesh1.admin.2", "both seats: mesh1's other admin; mesh2.admin.1's lower NodeId is no input");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::MeshSeat)), "mesh1.admin.2");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::FabricSeatOutside)), "mesh2.admin.1", "mesh.delete: the fabric seat leaves the departing mesh");
        // mesh1's only admin: the mesh seat has no secondary though mesh2's primary is Ready.
        nodes.retain(|n| n.name.to_string() != "mesh1.admin.2");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::BothSeats)), "cannot transfer: no secondary of type node-admin in mesh mesh1");
        // And no other mesh-primary either: both searches named.
        nodes.retain(|n| n.mesh != "mesh2");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::BothSeats)), "cannot transfer fabric seat: no other node-admin ReadyForTraffic in mesh mesh1, no other mesh-primary ReadyForTraffic");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::FabricSeatOutside)), "cannot transfer fabric seat: no other node-admin ReadyForTraffic in mesh mesh1, no other mesh-primary ReadyForTraffic");
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
        async fn run(&self, _build_id: &BuildId, _lane: Option<&str>, _attempt: u32, op: &BuildOperation) -> Result<(), RunEnd> {
            use rdm_core::build::RecipeStep;
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
        use rdm_core::build::RecipeStep;
        let with = |path: &str, id: &str, primary: bool, fabric: bool| {
            let mut n = node(path, primary, fabric);
            n.node_id = rdm_core::ids::NodeId::parse(id).unwrap();
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
        let id = BuildId::mint();
        builds
            .publish_accepted(&crate::build_state::BuildAccepted { build_id: id.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let runner = Arc::new(RecipeRunner { view: view.clone(), ran: Default::default() });
        let exec = BuildExecutor { executor: "mesh1.admin.1.new".into(), builds: builds.clone(), topology: view.clone(), runner: runner.clone() };
        let r = exec.run_attempt(&id, None, 5, &rdm_core::context::CallContext::default(), &plan).await;
        assert_eq!(*runner.ran.lock().unwrap(), vec!["node.stop:mesh1.admin.1".to_string(), "node.update:mesh1.admin.1".to_string()], "the run ran P's stop and rename");
        let Reconciled::HandedOff { attempt: 5, to, .. } = &r else { panic!("the run hands off: {r:?}") };
        assert_ne!(to, "mesh1.admin.1", "the run handed off to P, which it stopped and renamed: {r:?}");
        assert_eq!(to, "mesh1.admin.1.new", "the run hands back to the fabric primary its ending view seats: {r:?}");
        let mut end = view.read().await.clone();
        let m = seat_move(&ops[2], &end).expect("the stop of .new moves its seats first");
        assert_eq!((m.holder.name.to_string().as_str(), m.mesh_seat, m.fabric_seat), ("mesh1.admin.1.new", true, true));
        for n in end.nodes.iter_mut() {
            (n.is_primary, n.is_fabric_primary) = (n.name.to_string() == "mesh1.admin.2", n.name.to_string() == "mesh1.admin.2");
        }
        assert_eq!(lead_for(&ops[2..], &end).map(|n| n.to_string()).as_deref(), Some("mesh1.admin.2"), "that fabric primary's next claim names .new's same-mesh successor for what is left");
        let p_after = builds.read_build(&id).await.unwrap();
        assert_eq!(p_after.state, BuildState::Pending, "the Build waits for the next claim");
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
            BuildFact::Claim(BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }),
            BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt: 1, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }),
        ];
        let p = fold(&facts).remove(&id).unwrap();
        assert_eq!((p.state, p.attempt, p.last_failure), (BuildState::Pending, 1, None));
    }
}
