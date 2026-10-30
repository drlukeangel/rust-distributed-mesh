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
    Failed {
        /// Explanation of the failure.
        reason: String,
        /// The captured standard code.
        code: rdm_core::outcome::Code,
    },
    /// A participant of the operation was lost, or the proof its resume needs is not in yet: the
    /// operation is incomplete and its receipts stand. The attempt ends handed back to the
    /// fabric-primary, no terminal verdict is recorded, the Build stays in progress and the next
    /// attempt resumes from the receipts (R-BI4; mesh-delete.md, "Failure and receipts").
    Incomplete(String),
}

impl RunEnd {
    /// A named failed state without a remote delivery cause.
    pub fn failed(reason: String) -> Self { Self::Failed { reason, code: rdm_core::outcome::Code::FailedPrecondition } }
    /// Preserve a step failure at the operation boundary.
    pub fn coded(code: rdm_core::outcome::Code, reason: String) -> Self { Self::Failed { reason, code } }
    /// Why the run ended short of complete.
    pub fn reason(&self) -> &str {
        match self {
            Self::Failed { reason: r, .. } | Self::Incomplete(r) => r,
        }
    }
}

impl From<crate::deployment::pipeline::PipelineError> for RunEnd {
    fn from(e: crate::deployment::pipeline::PipelineError) -> Self { Self::coded(e.code, e.to_string()) }
}
impl From<String> for RunEnd {
    fn from(reason: String) -> Self {
        Self::failed(reason)
    }
}

impl std::fmt::Display for RunEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}

/// The most member creates of one attempt that run at once (Q245).
const CREATES_AT_ONCE: usize = 8;

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
        // and a mesh primary's own recovery by the admin its mesh elects in its place
        // (mesh-recovery.md:8-9). A mesh with no other live admin lost its mesh-primary: that is no
        // node recovery, and no admin executes one. Its node-admin is created by a Build's node.create
        // (below), the fabric-primary's once the mesh holds no admin that is not proven gone
        // (mesh-investigation.md "A node-admin joins the live mesh").
        BuildOperation::DeleteLiveNode { node, exited: Some(_), .. } | BuildOperation::CreateNode { node, over: Some(_), .. } => {
            if node.kind == NodeKind::NodeAdmin {
                node_admin_recoverer(node, t)
            } else {
                mesh_executor(&node.mesh, None, t)
            }
        }
        // A step is executed as the op it calls: a create as node.create, every other step as
        // the node op on its node (the mesh-primary of the node's mesh writes its row, R13).
        BuildOperation::Step { step: rdm_core::build::Step::Create { node, .. }, .. } => mesh_executor(&node.mesh, None, t),
        BuildOperation::Step { step, .. } => node_op_executor(step.node(), op, t),
        // A member node of any kind is born by its own mesh's primary admin (the fabric primary when
        // the mesh has none): the rule names no role, so a product's kinds get the same executor as
        // the proof product's rpc node. A mesh's missing node-admin is created by that mesh's primary
        // node-admin, its claim targeted at the fabric primary (node-admin-lifecycle.md §4.3); the
        // fabric primary creates only the first admin of a mesh that has none.
        BuildOperation::CreateNode { node, .. } => mesh_executor(&node.mesh, None, t),
        BuildOperation::DeleteLiveNode { node, .. }
        | BuildOperation::DeleteConnections { node, .. }
        | BuildOperation::StopNode { node, .. }
        | BuildOperation::StartNode { node, .. }
        | BuildOperation::DeleteNode { node, .. } => node_op_executor(node, op, t),
        // A mesh delete whose mesh holds the fabric-primary role waits for node.fabric-primary.update ([`role_move`]):
        // the new fabric-primary, outside the leaving mesh, drives the rest (mesh-delete.md step 1).
        BuildOperation::ShutdownMesh { .. } if role_move(op, t).is_some() => None,
        BuildOperation::CreateMesh { .. } | BuildOperation::ShutdownMesh { .. } | BuildOperation::DeleteFabric { .. } => fabric(),
        // The fabric-primary terminates a deleted birth heard again (node-investigation.md, Q128).
        BuildOperation::TerminateNode { .. } => fabric(),
        // A storage change's block and unblock of a mesh are its mesh-primary's; its switch and its
        // verdict the fabric-primary's (storage.md §2: fabric-primary → mesh-primary → node).
        BuildOperation::BlockStorage { mesh, .. } | BuildOperation::UnblockStorage { mesh, .. } => mesh_executor(mesh, None, t),
        BuildOperation::SwitchStorage { .. } | BuildOperation::FinishStorage { .. } => fabric(),
        // A property applied in place is written by the node's mesh-primary, the one writer of its
        // mesh's rows (node-rpc-envelope.md "Node properties"; build.md §1), its own row included.
        BuildOperation::UpdateConfig { node, .. } | BuildOperation::CreateTags { node, .. } | BuildOperation::DeleteTags { node, .. } => mesh_executor(&node.mesh, None, t),
    }
}

/// Whether `op`, executed by `to` in view `t`, is a section's (Q250): a plain node create (no node
/// recovery's, no replacement's) of a mesh whose own mesh-primary is `to`.
pub fn section_create(op: &BuildOperation, t: &Topology, to: &PathName) -> bool {
    matches!(op, BuildOperation::CreateNode { node, over: None, .. } if node.mesh == to.mesh && t.node_type_primary(&node.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.name == *to))
}

/// The executor of a node op (delete, connections delete, stop, start, delete, and a
/// Build's step) on `node` in view `t`.
fn node_op_executor(node: &PathName, op: &BuildOperation, t: &Topology) -> Option<PathName> {
    let fabric = || t.fabric_primary().map(|n| n.name.clone());
    // The node-type primary moves its role first, by node.primary.update its mesh-primary
    // commands and writes (proposal Part C1, Q3; [`role_move`]): until it has, no admin executes the
    // operation.
    if node.kind != NodeKind::NodeAdmin {
        return if role_move(op, t).is_some() { None } else { mesh_executor(&node.mesh, None, t) };
    }
    // A node-admin that holds a role is never the executor of its own stop, delete or
    // connections delete, and no admin of its mesh commands it (R-MT1, Luke 2026-10-12: "a
    // node-admin in the mesh shouldn't issue a command to the mesh-primary"; "only a FP should
    // issue commands to a MP"). The fabric-primary's drive moves the role first by
    // node.primary.update ([`role_move`]): until it has, no admin executes the operation. Once the
    // role moved the target is a member like any other, executed by its mesh's primary. The delete
    // of a mesh's only node moves no mesh-primary role (a fabric-primary role on it moves out of its mesh first):
    // the fabric primary, which owns what happens to a mesh that has no other admin, executes it.
    if role_move(op, t).is_some() {
        None
    } else if is_primary(node, t) {
        let last_mesh_gone = !t.meshes.is_empty() && t.meshes.iter().all(|m| m.status == crate::model::ScopeStatus::Dead) && t.node(node).is_some_and(|n| t.only_node(&node.mesh, &n.node_id));
        held_admins(&node.mesh, Some(node), t).is_empty().then(fabric).flatten().filter(|fp| fp != node || last_mesh_gone)
    } else {
        mesh_executor(&node.mesh, Some(node), t)
    }
}

/// The executor of an operation on a node of `mesh` (not `target` itself): the mesh's primary
/// admin; the fabric primary only when the mesh has no node-admin left that is not proven gone
/// (R-A2: cross-mesh only when every node-admin of the mesh is proven lost). An admin held but not elected (silent,
/// or ready and waiting on its role) gives `None`: the drive waits for that admin, named by
/// [`held_admins`], and never acts on the mesh from outside it. A `Pending` admin is a birth, not
/// a held role: the fabric primary carries the mesh until it stands.
fn mesh_executor(mesh: &str, target: Option<&PathName>, t: &Topology) -> Option<PathName> {
    if let Some(p) = t.node_type_primary(mesh, NodeKind::NodeAdmin) {
        return Some(p.name.clone());
    }
    if held_admins(mesh, target, t).is_empty() {
        t.fabric_primary().map(|n| n.name.clone())
    } else {
        None
    }
}

/// The node-admins of `mesh` (other than `target`) that hold no role and are not proven gone: the
/// same-mesh successors an operation on the mesh waits for. Proven gone in this view is `Dead` (an
/// exit proven for the exact birth; the offline mark is only for an admin's own mesh, which is
/// never the fallback case), `Leaving` and `Deleted`; a deleted birth has left the view.
pub fn held_admins<'a>(mesh: &str, target: Option<&PathName>, t: &'a Topology) -> Vec<&'a crate::model::Node> {
    use crate::model::NodeStatus;
    t.nodes_of_type(mesh, NodeKind::NodeAdmin).filter(|n| Some(&n.name) != target && matches!(n.status, NodeStatus::Ready | NodeStatus::ConnectionsDeleting | NodeStatus::PendingReconnect)).collect()
}

/// Why no admin executes `op` in view `t`: the mesh's held node-admins by name and status, or
/// that the view names no admin at all.
pub fn why_no_executor(op: &BuildOperation, t: &Topology) -> String {
    let node = match op {
        BuildOperation::CreateNode { node, .. }
        | BuildOperation::DeleteLiveNode { node, .. }
        | BuildOperation::DeleteConnections { node, .. }
        | BuildOperation::StopNode { node, .. }
        | BuildOperation::StartNode { node, .. }
        | BuildOperation::DeleteNode { node, .. }
        | BuildOperation::UpdateConfig { node, .. }
        | BuildOperation::CreateTags { node, .. }
        | BuildOperation::DeleteTags { node, .. } => node,
        BuildOperation::Step { step, .. } => step.node(),
        _ => return format!("{}: no admin holds the primary role that executes it in this view", op.key()),
    };
    let held: Vec<String> = held_admins(&node.mesh, Some(node), t).into_iter().map(|n| format!("{} ({:?})", n.name, n.status)).collect();
    if held.is_empty() {
        format!("{}: mesh {} has no primary node-admin and no fabric primary is named in this view", op.key(), node.mesh)
    } else {
        format!("{}: mesh {} has no primary node-admin, and its node-admin(s) {} are not proven gone: the fabric primary does not act on the mesh; the attempt waits for the mesh's own role", op.key(), node.mesh, held.join(", "))
    }
}

/// The mesh-primary that recovers node-admin `node` (node-recovery.md, mesh-recovery.md:8-9): its
/// mesh's live primary admin, or, when `node` itself holds the role, the admin its mesh elects in its
/// place once its exited birth is gone (a proven loss vacates the role, elections §5). `None` when no
/// other live admin of the mesh is left: the mesh lost its mesh-primary, which no node recovery
/// restores.
///
/// The role is read from the view's election, not from liveness: a silent primary keeps its role until
/// it is proven gone (`crate::election`), and its own recovery is still its mesh's to run.
pub fn node_admin_recoverer(node: &PathName, t: &Topology) -> Option<PathName> {
    match t.nodes_of_type(&node.mesh, NodeKind::NodeAdmin).find(|n| n.is_primary) {
        Some(p) if p.name == *node => elected_after_loss(node, t),
        Some(p) if p.status.is_live() => Some(p.name.clone()),
        _ => None,
    }
}

/// The admin that runs the recovery of an unheard mesh-primary, the birth `node_id` at `node`, and
/// writes its mesh's rows (node-recovery.md step 2: "For an unheard mesh-primary the fabric-primary runs
/// it ... after the election has named the new mesh-primary"; "Execution": "the mesh-primary claims it
/// ... the mesh-primary runs it"; node-identity-and-state.md: "The fabric-primary writes a row only as
/// acting primary of a mesh with no primary"), decided once, at the attempt's claim, from view `t`, the
/// view its rows are written against: the mesh-primary the view names in the unheard birth's place,
/// read from the election and not from liveness, as `mesh_executor` reads it; the fabric-primary,
/// the acting primary, while the mesh holds no other node-admin that is not proven gone
/// ([`held_admins`]); `None` while the view names no mesh-primary in its place and the mesh holds one:
/// the election has not named it here yet.
pub fn unheard_mesh_primary_executor(node: &PathName, node_id: &crate::model::NodeId, t: &Topology) -> Option<PathName> {
    match t.node_type_primary(&node.mesh, NodeKind::NodeAdmin) {
        Some(p) if p.node_id != *node_id => Some(p.name.clone()),
        _ if held_admins(&node.mesh, Some(node), t).is_empty() => t.fabric_primary().map(|n| n.name.clone()),
        _ => None,
    }
}

/// The admin the election names as `target`'s mesh primary once `target`'s exited birth is proven
/// gone (elections §5: "a primary ... whose exact birth is proven gone hands the role to the lowest
/// Ready NodeId of its own mesh"). A proven loss only: a planned move is node.primary.update.
fn elected_after_loss(target: &PathName, t: &Topology) -> Option<PathName> {
    let primary = t.node(target)?;
    let nodes: Vec<crate::model::Node> = t.members().cloned().collect();
    let mut inc = crate::election::Incumbency::default();
    inc.lost.extend(primary.incarnation_id.clone());
    crate::election::node_type_successor(&nodes, &inc, primary).map(|n| n.name)
}

/// Whether `node` holds a role in view `t`: its mesh's node-admin primary or the fabric primary.
fn is_primary(node: &PathName, t: &Topology) -> bool {
    node.kind == NodeKind::NodeAdmin && (t.node_type_primary(&node.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.name == *node) || t.fabric_primary().is_some_and(|fp| fp.name == *node))
}

/// The roles `op` moves before it runs (R-MT1: node.primary.update "should be called on MP stop or
/// delete. Caveat: delete is skipped if it's the only node on the mesh"): the node of any other type it
/// stops or deletes, or deletes the connections of, when that node holds its node type's
/// role (Part C1, Q3: its mesh-primary commands the update); the node-admin it stops,
/// deletes, or deletes the connections of, when that node holds its mesh's primary
/// role or the fabric-primary role in view `t`; and for a mesh delete of the mesh that holds the fabric-primary role,
/// the fabric-primary role alone (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part
/// C1 "The last node-admin of a mesh": "mesh.delete never updates the mesh primary ... If the mesh
/// holds the fabric-primary role, that role moves first by `node.fabric-primary.update` with the departing mesh
/// excluded (mesh-delete.md step 1); nothing else moves"). The delete of a mesh's only node moves no
/// mesh-primary role; when that node holds the fabric-primary role, the fabric-primary role alone moves out of its mesh (C2
/// "Successor search for the commanded transfer", step 2: "only when that mesh has none ... the lowest
/// NodeId among the other Ready mesh-primaries"). `None` for any other operation, a target
/// that holds no role, and the delete of a mesh's only node that holds no fabric-primary role. A primary with no
/// other node-admin in its mesh is still named: node.primary.update refuses it by name (the Build door
/// refuses it first, `control_plane::last_admin`).
pub fn role_move(op: &BuildOperation, t: &Topology) -> Option<RoleMove> {
    use rdm_core::build::Step;
    if let BuildOperation::ShutdownMesh { mesh, .. } = op {
        if !t.meshes.iter().any(|m| m.name != *mesh && m.status != crate::model::ScopeStatus::Dead) {
            return None;
        }
        let primary = t.fabric_primary().filter(|fp| fp.mesh == *mesh)?.clone();
        return Some(RoleMove { primary, mesh_role: false, fabric_role: true, node_type_role: false, successor: None });
    }
    let (node, delete) = match op {
        BuildOperation::DeleteLiveNode { node, exited: None, .. } | BuildOperation::Step { step: Step::Delete { node }, .. } => (node, true),
        BuildOperation::DeleteConnections { node, .. }
        | BuildOperation::StopNode { node, .. }
        | BuildOperation::Step { step: Step::Stop { node } | Step::DeleteConnections { node }, .. } => (node, false),
        _ => return None,
    };
    if node.kind != NodeKind::NodeAdmin {
        // Any other node type's role: its primary's role moves first, by node.primary.update its mesh-primary
        // commands and writes (proposal Part C1, Q3), except when deleting the node type's only live node. A primary with
        // no other node of its type is still named for non-delete actions: the update refuses it by Q7's
        // name (the Build door refuses it first, `control_plane::last_admin`).
        let primary = t.node(node)?.clone();
        let only_of_node_type = t.nodes_of_type(&node.mesh, node.kind).all(|n| n.node_id == primary.node_id);
        if !t.node_type_primary(&node.mesh, node.kind).is_some_and(|p| p.node_id == primary.node_id) || (delete && only_of_node_type) {
            return None;
        }
        return Some(RoleMove { primary, mesh_role: false, fabric_role: false, node_type_role: true, successor: None });
    }
    let primary = t.node(node)?.clone();
    let mesh_role = t.node_type_primary(&node.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.node_id == primary.node_id);
    let fabric_role = t.fabric_primary().is_some_and(|fp| fp.node_id == primary.node_id);
    if !mesh_role && !fabric_role {
        return None;
    }
    // The delete of a mesh's only node skips node.primary.update; a fabric-primary role on it still moves, out
    // of its mesh, which has no other node-admin to take it (C2 search step 2), unless every mesh of
    // the fabric is already Dead (fabric-delete.md step 4: the separate node.delete of the fabric-primary).
    // The final primary of a mesh whose delete was accepted (its mesh leaving or dead) is deleted by the
    // separate node.delete after its mesh.deleted (mesh-delete.md step 7): no other member is left to
    // take its mesh-primary role, which goes with the mesh.
    let mesh_deleted = t.meshes.iter().any(|m| m.name == node.mesh && matches!(m.status, crate::model::ScopeStatus::Leaving | crate::model::ScopeStatus::Dead));
    if delete && (t.only_node(&node.mesh, &primary.node_id) || mesh_deleted) {
        if !t.meshes.is_empty() && t.meshes.iter().all(|m| m.status == crate::model::ScopeStatus::Dead) {
            return None;
        }
        return fabric_role.then_some(RoleMove { primary, mesh_role: false, fabric_role: true, node_type_role: false, successor: None });
    }
    Some(RoleMove { primary, mesh_role, fabric_role, node_type_role: false, successor: None })
}

/// What a drive moves before an operation on a primary runs ([`role_move`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleMove {
    /// The node-admin that holds the roles.
    pub primary: crate::model::Node,
    /// It holds its mesh's primary role: node.primary.update moves it.
    pub mesh_role: bool,
    /// It holds the fabric-primary role: node.fabric-primary.update moves it, in the mesh-primary role's one row when
    /// both move; alone (the mesh-primary role stays) it moves out of the primary's mesh (C2 search step 2).
    pub fabric_role: bool,
    /// It holds the node-type primary role of its (mesh, node type), a type other than node-admin:
    /// node.primary.update moves it, commanded and written by its mesh-primary (proposal Part C1, Q3).
    pub node_type_role: bool,
    /// The successor the primary names (elections §4.4: the re-created node-admin of the mesh that lost
    /// the fabric-primary role); `None` when the search of node-primary-update.md picks it.
    pub successor: Option<PathName>,
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
    /// the run's own operations can move a role or stop the planner ([`BuildExecutor`]).
    pub hand_off_to: Option<String>,
}

/// The run of `executor` within `ops` in view `t`: the leading operations it executes, and the
/// executor of the first one it does not. An operation no admin can execute stays in the run here;
/// the fabric-primary's drive cuts the run before it when it is not the first (`build_drive`), so it
/// waits for its mesh's own role.
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
        /// The captured standard code.
        code: rdm_core::outcome::Code,
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
/// else (mesh creation and deletion) and a mesh's members while that mesh has no admin primary.
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
                if let Some(p) = t.node_type_primary(&birth.mesh, NodeKind::NodeAdmin) {
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
    /// parented to the attempt's context the claim returned, so a stop reads under its request
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
            return Reconciled::Failed { attempt, operation: String::new(), reason: format!("recording the claim of attempt {attempt}: {e}") , code: rdm_core::outcome::Code::FailedPrecondition };
        }
        let build = match builds.read_build(build_id).await {
            Ok(b) => b,
            Err(e) => return Reconciled::Failed { attempt, operation: String::new(), reason: format!("reading Build {build_id} after the claim of attempt {attempt}: {e}") , code: rdm_core::outcome::Code::FailedPrecondition },
        };
        let operations = &plan.operations;
        span.record("operations", operations.iter().map(BuildOperation::key).collect::<Vec<_>>().join(",").as_str());
        // The plain creates of a mesh whose own mesh-primary executes them are that mesh's section
        // (Q250): this fabric-lane run passes them over, and the fabric-primary sends each mesh-primary
        // its section, all at once.
        let mut skipped: std::collections::BTreeSet<usize> = Default::default();
        let mut skipped_meshes: std::collections::HashSet<String> = Default::default();
        let ran = |upto: usize, skipped: &std::collections::BTreeSet<usize>| -> Vec<BuildOperation> { operations[..upto].iter().enumerate().filter(|(k, _)| !skipped.contains(k)).map(|(_, op)| op.clone()).collect() };
        let mut i = 0;
        while i < operations.len() {
            let op = &operations[i];
            if lane.is_none() && op.created_node().is_some_and(|n| skipped_meshes.contains(&n.mesh)) {
                skipped.insert(i);
                i += 1;
                continue;
            }
            // A fabric.delete accepted after this Build interrupts it (build.md §1): the operation admitted
            // before ended within its bound and its receipts stand, and nothing further is opened. The run
            // hands back to the fabric-primary, which writes the Build's terminal naming the delete.
            if let Ok(Some(delete)) = crate::build_state::interrupted_by(builds, &build).await {
                return self.interrupted(builds, &build, attempt, ran(i, &skipped), op, None, &delete.build_id, span).await;
            }
            // After the first operation the view can name another executor for the rest: the run
            // in its role a mesh's first admin, which executes that mesh's members from then on. The
            // operation then belongs to the admin the view now names, and it is passed to it unrun;
            // an operation a view does not place stays with this admin. The operations themselves are
            // always the fabric-primary's plan.
            if i > 0 && lane.is_none() {
                let now = self.held_name().await;
                let t = self.topology.read().await.clone();
                if let Some(to) = executor_for(op, &t).filter(|to| to.to_string() != now) {
                    if section_create(op, &t, &to) {
                        skipped_meshes.insert(to.mesh.clone());
                        skipped.insert(i);
                        i += 1;
                        continue;
                    }
                    return self.hand_off(builds, &build, attempt, ran(i, &skipped), to.to_string()).await;
                }
            }
            // A run of member creates this admin executes runs concurrently (Q245): the nodes of a
            // mesh are born together, never one after another, a further node-admin with them. A mesh's
            // first node-admin is created on its own: it is the birth the view hands the mesh's members to. A create that ends
            // short of complete stops none of the others; the attempt ends once every one has ended.
            let group = self.create_group(&operations[i..], lane).await;
            if group > 1 {
                let ended = self.run_group(&build.build_id, lane, attempt, &operations[i..i + group]).await;
                let ran: Vec<BuildOperation> = ran(i, &skipped).into_iter().chain(ended.iter().filter(|(_, r)| r.is_ok()).map(|(op, _)| (*op).clone())).collect();
                let failed = |heals: bool| ended.iter().find_map(|(op, r)| match r { Err(RunEnd::Failed { reason, code }) if (heals_when_failed(op, lane, false) && failed_the_run_before(&build, op, attempt).is_none()) == heals => Some((*op, reason.clone(), *code)), _ => None });
                if let Some((op, reason, _)) = failed(false).or_else(|| failed(true)) {
                    if let Ok(Some(delete)) = crate::build_state::interrupted_by(builds, &build).await {
                        return self.interrupted(builds, &build, attempt, ran, op, Some(&reason), &delete.build_id, span).await;
                    }
                }
                if let Some((op, reason, code)) = failed(false) {
                    return self.finish(builds, &build, attempt, Err((op.key(), reason, code))).await;
                }
                if let Some((op, reason, _)) = failed(true) {
                    return self.create_failed(builds, &build, attempt, ran, op, reason, span).await;
                }
                if let Some((op, reason)) = ended.iter().find_map(|(op, r)| match r { Err(RunEnd::Incomplete(reason)) => Some((*op, reason.clone())), _ => None }) {
                    return self.hand_back(builds, &build, attempt, ran, op, reason, span).await;
                }
                i += group;
                continue;
            }
            // Read before the run: once it ran, a first node-admin's create is no longer the mesh's first.
            let first_admin = matches!(op, BuildOperation::CreateNode { node, .. } if self.topology.read().await.first_admin_of(node));
            match self.runner.run(&build.build_id, lane, attempt, op).await {
                Ok(()) => {}
                Err(RunEnd::Failed { reason, code }) => {
                    // An operation admitted before a fabric.delete accepted after this Build fails within its
                    // bound (build.md §1): its failure stands in its receipts and the run hands back, so the
                    // Build's `Failed` names the delete.
                    if let Ok(Some(delete)) = crate::build_state::interrupted_by(builds, &build).await {
                        return self.interrupted(builds, &build, attempt, ran(i, &skipped), op, Some(&reason), &delete.build_id, span).await;
                    }
                    if heals_when_failed(op, lane, first_admin) && failed_the_run_before(&build, op, attempt).is_none() {
                        return self.create_failed(builds, &build, attempt, ran(i, &skipped), op, reason, span).await;
                    }
                    return self.finish(builds, &build, attempt, Err((op.key(), reason, code))).await;
                }
                Err(RunEnd::Incomplete(reason)) => return self.hand_back(builds, &build, attempt, ran(i, &skipped), op, reason, span).await,
            }
            i += 1;
        }
        // More of the plan follows the run. The planner named the next executor from its own view,
        // taken before this run's operations, and they can move a role or stop the planner itself
        // (the fabric primary's replace: its same-mesh successor runs the stop that moves its roles
        // and stops it). So the run hands back to the fabric primary of the view it ends with, the
        // one driver of the Build, whose next claim names the executor of what is left in its own
        // view (`build_drive` claim_next).
        if plan.hand_off_to.is_some() || !skipped.is_empty() {
            let driver = self.topology.read().await.fabric_primary().map(|n| n.name.to_string());
            return match driver {
                Some(to) => self.hand_off(builds, &build, attempt, ran(operations.len(), &skipped), to).await,
                // No operation failed: the receipt names none, so no step is blamed for it.
                None => {
                    let last = operations.last().map(BuildOperation::key).unwrap_or_else(|| "no operation".into());
                    let reason = format!("{me}: the run of attempt {attempt} ended after {last} with more of the plan to follow, and this admin's view names no fabric primary to hand it back to");
                    self.finish(builds, &build, attempt, Err((String::new(), reason, rdm_core::outcome::Code::FailedPrecondition))).await
                }
            };
        }
        self.finish(builds, &build, attempt, Ok(operations.clone())).await
    }

    /// How many of `ops`, from the first, are member creates this admin executes in its view now
    /// (Q245): node creates, each of whose executor is this admin, but a mesh's first node-admin, the
    /// birth the view hands the mesh's members to.
    async fn create_group(&self, ops: &[BuildOperation], lane: Option<&str>) -> usize {
        let now = self.held_name().await;
        let t = self.topology.read().await;
        ops.iter()
            .take_while(|op| matches!(op, BuildOperation::CreateNode { node, .. } if !t.first_admin_of(node)) && (lane.is_some() || executor_for(op, &t).is_none_or(|to| to.to_string() == now)))
            .count()
    }

    /// Runs `ops` concurrently, at most [`CREATES_AT_ONCE`] at a time, each to its own end; answers
    /// each with how it ended, in plan order.
    async fn run_group<'a>(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, ops: &'a [BuildOperation]) -> Vec<(&'a BuildOperation, Result<(), RunEnd>)> {
        use futures_util::StreamExt;
        // Boxed where each is made: the creates' futures are the runner's, sent to the worker that runs the attempt.
        let runs: Vec<futures_util::future::BoxFuture<'_, (usize, Result<(), RunEnd>)>> = ops.iter().enumerate().map(|(k, op)| -> futures_util::future::BoxFuture<'_, _> { Box::pin(async move { (k, self.runner.run(build_id, lane, attempt, op).await) }) }).collect();
        let mut ended: Vec<(usize, Result<(), RunEnd>)> = futures_util::stream::iter(runs).buffer_unordered(CREATES_AT_ONCE).collect().await;
        ended.sort_by_key(|(k, _)| *k);
        ended.into_iter().map(|(k, r)| (&ops[k], r)).collect()
    }

    /// `op` ended incomplete (R-BI4): a participant of it was lost, or the proof its resume needs is
    /// not in. Its receipts stand and no terminal verdict is recorded. The attempt ends the way a run
    /// that stops at another executor's operation ends: handed to the fabric-primary of the view the
    /// run ends with, which claims the next attempt, and that attempt resumes from the receipts. The
    /// Build stays in progress. Nothing is framed to the caller (node-rpc-envelope.md: Blocked's only
    /// source is the subject's Status `Blocked`): the reason is on the run's span.
    #[allow(clippy::too_many_arguments)]
    async fn hand_back(&self, builds: &dyn BuildStateAdapter, build: &BuildProjection, attempt: u32, ran: Vec<BuildOperation>, op: &BuildOperation, reason: String, span: &tracing::Span) -> Reconciled {
        span.record("incomplete", reason.as_str());
        let me = self.held_name().await;
        let to = self.topology.read().await.fabric_primary().map(|n| n.name.to_string()).unwrap_or(me);
        tracing::info!(operation = %op.key(), reason = %reason, to = %to, "the operation ended incomplete: its receipts stand, the attempt is handed to the fabric-primary and the Build stays in progress");
        self.hand_off(builds, build, attempt, ran, to).await
    }

    /// `op`, a plain create of the fabric lane ([`heals_when_failed`]), ended failed, and the attempt
    /// before that ran it did not fail it (Q245, Q252: one heal attempt). Its failed step's receipt is
    /// on the record, the receipts of what ran stand, and the attempt is handed to the fabric-primary of
    /// the view the run ends with, the Build in progress: the next attempt it claims creates it again.
    /// Nothing is framed to the caller (node-rpc-envelope.md: Blocked's only source is the subject's
    /// Status `Blocked`). The same create failing on that next attempt ends it `Failed` with its step
    /// and reason ([`failed_the_run_before`]).
    #[allow(clippy::too_many_arguments)]
    async fn create_failed(&self, builds: &dyn BuildStateAdapter, build: &BuildProjection, attempt: u32, ran: Vec<BuildOperation>, op: &BuildOperation, reason: String, span: &tracing::Span) -> Reconciled {
        span.record("incomplete", reason.as_str());
        let me = self.held_name().await;
        let to = self.topology.read().await.fabric_primary().map(|n| n.name.to_string()).unwrap_or(me);
        tracing::info_span!("rdm.node_admin.build.update.via-create-failed", build_id = %build.build_id.log(), attempt, operation = %op.key(), reason = %reason, to = %to)
            .in_scope(|| tracing::info!("a create of the fabric lane ended failed: the attempt is handed back and the next attempt creates it again; the Build stays in progress"));
        self.hand_off(builds, build, attempt, ran, to).await
    }

    /// The run stops at `next` because the fabric.delete `delete` interrupts its Build (build.md §1): what
    /// it ran stands, and `next` is either never opened (`failed` is `None`) or ended within its bound by
    /// failing (`failed` names why, its receipts standing). The attempt is handed to the fabric-primary of
    /// the view the run ends with, which ends the Build `Failed` naming the delete once its attempts ended.
    /// Nothing is framed to the caller (node-rpc-envelope.md: Blocked's only source is the subject's
    /// Status `Blocked`): the interruption is on the run's span and its `via-interrupted` span.
    #[allow(clippy::too_many_arguments)]
    async fn interrupted(&self, builds: &dyn BuildStateAdapter, build: &BuildProjection, attempt: u32, ran: Vec<BuildOperation>, next: &BuildOperation, failed: Option<&str>, delete: &BuildId, span: &tracing::Span) -> Reconciled {
        let reason = crate::build_state::interrupted_reason(&build.build_id, delete);
        span.record("incomplete", reason.as_str());
        let me = self.held_name().await;
        let to = self.topology.read().await.fabric_primary().map(|n| n.name.to_string()).unwrap_or(me);
        tracing::info_span!("rdm.node_admin.build.update.via-interrupted", build_id = %build.build_id.log(), attempt, delete_build_id = %delete.log(), operation = %next.key(), failed = failed.unwrap_or(""), to = %to)
            .in_scope(|| match failed {
                None => tracing::info!("a fabric.delete accepted after this Build interrupts it: the next operation is not opened and the attempt is handed to the fabric-primary"),
                Some(_) => tracing::info!("a fabric.delete accepted after this Build interrupts it: the operation admitted before it failed within its bound, and the attempt is handed to the fabric-primary"),
            });
        self.hand_off(builds, build, attempt, ran, to).await
    }

    async fn hand_off(&self, builds: &dyn BuildStateAdapter, build: &BuildProjection, attempt: u32, operations: Vec<BuildOperation>, to: String) -> Reconciled {
        let receipt = BuildAttemptReceipt { mesh: None, build_id: build.build_id.clone(), attempt, outcome: AttemptOutcome::HandedOff { to: to.clone() } };
        if let Err(e) = builds.append_attempt_receipt(&receipt).await {
            return Reconciled::Failed { attempt, operation: String::new(), reason: format!("recording attempt {attempt}: {e}") , code: rdm_core::outcome::Code::FailedPrecondition };
        }
        tracing::info!(to = %to, "the next operation belongs to another admin; handed off");
        Reconciled::HandedOff { attempt, operations, to }
    }

    async fn finish(&self, builds: &dyn BuildStateAdapter, build: &BuildProjection, attempt: u32, r: Result<Vec<BuildOperation>, (String, String, rdm_core::outcome::Code)>) -> Reconciled {
        let outcome = match &r {
            Ok(_) => AttemptOutcome::Converged,
            Err((op, reason, code)) if op.is_empty() => AttemptOutcome::Failed { reason: reason.clone(), code: *code },
            Err((op, reason, code)) => AttemptOutcome::Failed { reason: format!("{op}: {reason}"), code: *code },
        };
        let receipt = BuildAttemptReceipt { mesh: None, build_id: build.build_id.clone(), attempt, outcome };
        if let Err(e) = builds.append_attempt_receipt(&receipt).await {
            return Reconciled::Failed { attempt, operation: String::new(), reason: format!("recording attempt {attempt}: {e}") , code: rdm_core::outcome::Code::FailedPrecondition };
        }
        match r {
            Ok(operations) => Reconciled::Converged { attempt, operations },
            Err((operation, reason, code)) if operation.is_empty() => Reconciled::Failed { attempt, reason, operation, code },
            Err((operation, reason, code)) => Reconciled::Failed { attempt, reason: format!("{operation}: {reason}"), operation, code },
        }
    }
}

/// The attempt before `attempt` that ran `op` in the fabric lane, when it failed it: the receipts the
/// record holds of `op` (Q252: the bound is the same operation failing twice running, whatever the
/// reason). A create that fails again after it ends the attempt `Failed`, never a second heal.
fn failed_the_run_before(build: &BuildProjection, op: &BuildOperation, attempt: u32) -> Option<u32> {
    let key = op.key();
    let ours = || build.steps.iter().filter(|s| s.mesh.is_none() && s.operation == key && s.attempt < attempt);
    let last = ours().map(|s| s.attempt).max()?;
    ours().any(|s| s.attempt == last && matches!(s.outcome, crate::build_state::StepOutcome::Failed { .. })).then_some(last)
}

/// Whether `op`, ended failed, hands its attempt back rather than failing it (Q245): a plain create
/// (`over: None`) of the fabric lane (`lane` `None`) that is not its mesh's first node-admin
/// (`first_admin`). Every other operation's failure, a node recovery's create over an exited birth
/// among them, fails the attempt.
fn heals_when_failed(op: &BuildOperation, lane: Option<&str>, first_admin: bool) -> bool {
    lane.is_none() && !first_admin && matches!(op, BuildOperation::CreateNode { over: None, .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::BuildState;
    use crate::model::{Fabric, FabricId, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};

    fn node(name: &str, primary: bool, fabric: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = NodeStatus::Ready;
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
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes,
        }
    }

    fn create(n: &str) -> BuildOperation {
        BuildOperation::CreateNode { node: n.parse().unwrap(), over: None, executable: None, entry: None }
    }

    fn who(op: &BuildOperation, t: &Topology) -> String {
        executor_for(op, t).map(|p| p.to_string()).unwrap_or_default()
    }

    /// CONTRACT (#3051, #3088, node-recovery.md "Execution" and "Steps": the fabric-primary dispatches
    /// the recovery Build to the mesh-primary as executor; mesh-recovery.md: "A lost member of a mesh
    /// with a live mesh-primary is node recovery, driven by that mesh-primary"): every operation of a
    /// member's recovery (its node.delete, then its node.create), a secondary node-admin's
    /// included, is executed by its own mesh's primary admin, never by the fabric-primary of another
    /// mesh. A mesh primary's own recovery is executed by the admin its mesh elects in its place, the
    /// view still naming as primary the exited primary included. A node-admin with no other admin in its mesh is
    /// no node recovery (its mesh lost its mesh-primary: its node-admin is created by the investigation's
    /// node.create, never recovered), so no admin executes one.
    /// Must NOT happen: a recovery's operation routed to the fabric-primary by a fall-through arm, or
    /// its create routed to the exited mesh primary the view still names.
    #[test]
    fn a_members_recovery_is_executed_by_its_mesh_primary_a_secondary_node_admins_included() {
        let recovery = |n: &str| {
            let (node, from) = (n.parse::<PathName>().unwrap(), crate::build::ExitedBirth { node_id: crate::model::NodeId::mint(), incarnation: crate::model::IncarnationId::mint() });
            [BuildOperation::DeleteLiveNode { node: node.clone(), exited: Some(from.clone()), birth: None }, BuildOperation::CreateNode { node, over: Some(from), executable: None, entry: None }]
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
        assert_eq!(recovered_by("mesh1.admin.2", &t3), "mesh1.admin.1", "the fabric-primary's own secondary admin, by its mesh-primary role");
        assert_eq!(recovered_by("mesh2.admin.1", &t3), "mesh2.admin.2", "a mesh primary's own recovery is executed by the admin its mesh elects in its place");
        let mut silent = t3.clone();
        silent.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.1").for_each(|n| n.status = NodeStatus::PendingReconnect);
        assert_eq!(recovered_by("mesh2.admin.2", &silent), "", "no live node-admin of mesh2 is left to recover it: mesh recovery");
    }

    #[test]
    fn a_meshs_members_are_its_own_primarys_and_everything_else_the_fabric_primarys() {
        let t = mm(true);
        assert_eq!(who(&create("mesh2.rpc.1"), &t), "mesh2.admin.1");
        assert_eq!(who(&BuildOperation::DeleteLiveNode { node: "mesh2.rpc.1".parse().unwrap(), exited: None, birth: None }, &t), "mesh2.admin.1");
        assert_eq!(who(&create("mesh1.rpc.2"), &t), "mesh1.admin.1");
        // The rule names no role: a product's kinds get the mesh's own primary admin too.
        assert_eq!(who(&create("mesh2.broker.1"), &t), "mesh2.admin.1");
        assert_eq!(who(&BuildOperation::StopNode { node: "mesh2.gateway.1".parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() }, &t), "mesh2.admin.1");
        assert_eq!(who(&BuildOperation::DeleteLiveNode { node: "mesh2.compute.1".parse().unwrap(), exited: None, birth: None }, &t), "mesh2.admin.1");
        assert_eq!(who(&create("mesh2.admin.2"), &t), "mesh2.admin.1", "a mesh's missing admin is created by that mesh's primary");
        assert_eq!(who(&create("mesh1.admin.2"), &t), "mesh1.admin.1", "its own mesh's primary, which is the fabric primary here");
        assert_eq!(who(&BuildOperation::DeleteLiveNode { node: "mesh2.admin.1".parse().unwrap(), exited: None, birth: None }, &t), "mesh1.admin.1", "mesh2.admin.1 is mesh2's only admin: no successor, the fabric primary");
        let mut t3 = mm(true);
        t3.nodes.push(node("mesh2.admin.2", false, false));
        let (p2, m2) = ("mesh2.admin.1", "mesh2.admin.2");
        assert_eq!(who(&BuildOperation::DeleteLiveNode { node: m2.parse().unwrap(), exited: None, birth: None }, &t3), p2, "a non-primary admin's delete is its mesh primary's");
        assert_eq!(who(&BuildOperation::StopNode { node: m2.parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() }, &t3), p2, "a non-primary admin's stop is its mesh primary's");
        // R-MT1: a mesh primary's own operation waits for node.primary.update; no admin executes it
        // while it holds the role, and once the role moved its mesh's new primary does.
        assert_eq!(who(&BuildOperation::StopNode { node: p2.parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() }, &t3), "", "a mesh primary's own stop waits for its role to move");
        assert_eq!(who(&BuildOperation::DeleteLiveNode { node: p2.parse().unwrap(), exited: None, birth: None }, &t3), "", "a mesh primary's own delete waits for its role to move");
        let mut moved = t3.clone();
        for n in moved.nodes.iter_mut().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin) {
            n.is_primary = n.name.to_string() == m2;
        }
        assert_eq!(who(&BuildOperation::StopNode { node: p2.parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() }, &moved), m2, "once the role moved, the new mesh primary executes the former primary's stop");
        let rename = |n: &str| {
            let node: PathName = n.parse().unwrap();
            BuildOperation::Step { step: rdm_core::build::Step::Update { name: node.renamed(), node }, birth: None, entry: None }
        };
        let make = |n: &str| BuildOperation::Step { step: rdm_core::build::Step::Create { node: n.parse().unwrap(), executable: None }, birth: None, entry: None };
        assert_eq!(who(&rename("mesh2.rpc.1"), &t), "mesh2.admin.1", "a Build's update of a member is its mesh primary's, the writer of its row");
        assert_eq!(who(&rename(m2), &t3), p2, "a Build's update of a non-primary admin is its mesh primary's");
        assert_eq!(who(&rename(p2), &moved), m2, "a Build's update of a former mesh primary after its node.connection.delete moved the role is the new mesh primary's");
        assert_eq!(who(&make("mesh2.rpc.1"), &t), "mesh2.admin.1", "a Build's create, a replace successor's at the name its predecessor released, is node.create's executor");
        let steps_connections = |n: &str| BuildOperation::Step { step: rdm_core::build::Step::DeleteConnections { node: n.parse().unwrap() }, birth: None, entry: None };
        assert_eq!(who(&steps_connections(p2), &t3), "", "a Build's node.connection.delete of a mesh primary waits for its role to move");
        assert!(role_move(&steps_connections(p2), &t3).is_some_and(|m| m.mesh_role && !m.fabric_role), "a Build's node.connection.delete of mesh2's primary moves its mesh-primary role first");
        assert_eq!(who(&steps_connections(p2), &moved), m2, "once the role moved, the new mesh primary executes the Build's node.connection.delete");
        let connections = |n: &str| BuildOperation::DeleteConnections { node: n.parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() };
        let stop = |n: &str| BuildOperation::StopNode { node: n.parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() };
        assert_eq!(who(&connections("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's node.connection.delete is its mesh primary's");
        assert_eq!(who(&stop("mesh2.rpc.1"), &t), "mesh2.admin.1", "a member's stop is its mesh primary's");
        assert_eq!(who(&connections(m2), &t3), p2, "a non-primary admin's node.connection.delete is its mesh primary's");
        assert_eq!(who(&stop(p2), &t3), "", "a mesh primary's own stop waits for its role to move");
        assert_eq!(who(&stop(p2), &moved), m2, "once the role moved, the new mesh primary executes the stop");
        assert!(role_move(&stop(p2), &t3).is_some_and(|m| m.mesh_role && !m.fabric_role), "the stop of mesh2's primary moves its mesh-primary role first");
        assert_eq!(who(&BuildOperation::CreateMesh { mesh: "mesh3".into(), mesh_id: MeshId::mint() }, &t), "mesh1.admin.1");
        assert_eq!(who(&shutdown("mesh2"), &t), "mesh1.admin.1");
    }

    /// `mm(true)` with pinned NodeIds: `fp`, the lower, is the node-admin the election names as the
    /// fabric primary; the other admin and `mesh1.rpc.1` are higher.
    fn elected(fp: &str) -> Topology {
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
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()), Some(fp.to_string()), "the pinned ids role {fp}");
        t
    }

    /// CONTRACT (Luke 2026-10-08, the fabric primary is handed off, never wiped out; R-MT1; the
    /// approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C2 "Successor search
    /// for the commanded transfer": "1. the primary's own mesh ... 2. only when that mesh has none, or
    /// it is the departing mesh under mesh.delete: the lowest NodeId among the other Ready
    /// mesh-primaries"): a delete or stop of the fabric primary is never executed by the target
    /// itself: no admin executes it until its roles moved. Beside another node of its mesh, both roles
    /// move (node.primary.update's one row, Q1). As its mesh's only node, its delete skips
    /// node.primary.update (R-MT1: the delete of a mesh's only node) and moves the fabric-primary role alone,
    /// out of its mesh (C2 step 2), after which the new fabric primary executes it; its stop still
    /// moves both (refused by Q7's text, no secondary). Another admin's delete is its own mesh
    /// primary's (mesh2's only admin, its mesh's only node, has no mesh primary beside it: the fabric
    /// primary). The NodeIds are pinned: which admin holds the fabric-primary role is the case, never a draw.
    /// Must NOT happen: the fabric primary named the executor of its own delete, or its delete as its
    /// mesh's only node left waiting with no role to move and no admin to execute it.
    #[test]
    fn the_fabric_primary_never_executes_its_own_delete_or_stop() {
        let delete = |n: &str| BuildOperation::DeleteLiveNode { node: n.parse().unwrap(), exited: None, birth: None };
        let stop = |n: &str| BuildOperation::StopNode { node: n.parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() };
        // mesh1.admin.1 holds the fabric-primary role, mesh1.rpc.1 beside it.
        let t = elected("mesh1.admin.1");
        for op in [delete("mesh1.admin.1"), stop("mesh1.admin.1")] {
            assert_eq!(executor_for(&op, &t), None, "{op:?}: never the fabric primary; no admin until its roles moved");
            assert!(role_move(&op, &t).is_some_and(|m| m.mesh_role && m.fabric_role), "{op:?}: both roles move first: {:?}", role_move(&op, &t));
        }
        assert_eq!(who(&delete("mesh2.admin.1"), &t), "mesh1.admin.1", "mesh2's only admin has no mesh primary beside it: the fabric primary deletes it");
        // mesh2.admin.1 holds the fabric-primary role and is its mesh's only node.
        let t = elected("mesh2.admin.1");
        assert_eq!(executor_for(&delete("mesh2.admin.1"), &t), None, "never the fabric primary; no admin until its fabric-primary role moved");
        assert!(
            role_move(&delete("mesh2.admin.1"), &t).is_some_and(|m| !m.mesh_role && m.fabric_role),
            "the delete of its mesh's only node moves the fabric-primary role alone: {:?}",
            role_move(&delete("mesh2.admin.1"), &t)
        );
        assert!(role_move(&stop("mesh2.admin.1"), &t).is_some_and(|m| m.mesh_role && m.fabric_role), "its stop moves both roles");
        assert_eq!(who(&delete("mesh1.admin.1"), &t), "", "mesh1's primary, beside mesh1.rpc.1, waits for its own role to move");
        let mut moved = t.clone();
        for n in moved.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh1.admin.1";
        }
        assert_eq!(role_move(&delete("mesh2.admin.1"), &moved), None, "once the fabric-primary role moved, nothing is owed");
        assert_eq!(who(&delete("mesh2.admin.1"), &moved), "mesh1.admin.1", "the new fabric primary executes it");
        // Alone, the fabric primary has no successor: nobody executes its delete.
        let mut alone = mm(false);
        crate::election::resolve(&mut alone.nodes);
        let fp = alone.fabric_primary().unwrap().name.clone();
        assert_eq!(executor_for(&BuildOperation::DeleteLiveNode { node: fp, exited: None, birth: None }, &alone), None);
    }

    /// CONTRACT (#3016; elections §5: a primary proven gone "hands the role to the lowest
    /// Ready NodeId of its own mesh"; R-A2: cross-mesh only when every node-admin of the mesh is proven lost): mesh2's in its role admin is
    /// proven exited and its other admin is held but silent (`PendingReconnect`, not proven gone).
    /// No admin of mesh2 holds the role yet, and the fabric primary of mesh1 executes nothing of
    /// mesh2: the answer is `None` and the reason names the held admin. With every mesh2 admin
    /// proven gone the fabric primary executes; a `Pending` admin is a birth, carried by the fabric
    /// primary until it stands.
    #[test]
    fn a_mesh_whose_admin_is_held_but_not_elected_is_never_executed_by_the_fabric_primary() {
        let mut t = mm(true);
        let mut second = node("mesh2.admin.2", false, false);
        second.status = NodeStatus::PendingReconnect;
        t.nodes.push(second);
        // mesh2.rpc.1 holds mesh2's rpc role; the stop is of mesh2.rpc.3, which holds none, so no role
        // moves before it and its executor alone is the question.
        t.nodes.push(node("mesh2.rpc.1", true, false));
        t.nodes.push(node("mesh2.rpc.3", false, false));
        for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.1") {
            n.status = NodeStatus::Dead;
            n.is_primary = false;
        }
        let ops = [BuildOperation::StopNode { node: "mesh2.rpc.3".parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() }, create("mesh2.rpc.2"), create("mesh2.admin.3"), BuildOperation::DeleteLiveNode { node: "mesh2.admin.1".parse().unwrap(), exited: None, birth: None }];
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
            n.status = NodeStatus::Ready;
            n.is_primary = true;
        }
        assert_eq!(who(&ops[0], &t), "mesh2.admin.2", "once primary, the same-mesh successor executes");
    }

    fn shutdown(mesh: &str) -> BuildOperation {
        BuildOperation::ShutdownMesh { mesh: mesh.into(), mesh_id: rdm_core::MeshId::mint() }
    }

    /// CONTRACT (R-ST2; the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1
    /// "The last node-admin of a mesh": "If the mesh holds the fabric-primary role, that role moves first by
    /// `node.fabric-primary.update` with the departing mesh excluded (mesh-delete.md step 1); nothing
    /// else moves"): `mesh.delete:<mesh_id>` is the fabric primary's. When the fabric primary is
    /// inside the leaving mesh, its fabric-primary role alone moves first ([`role_move`]: the mesh-primary role stays),
    /// no admin executes the delete until it has, and the executor is then the new fabric primary.
    /// Must NOT happen: the leaving mesh's admin named the executor of its own mesh's delete.
    #[test]
    fn a_mesh_delete_is_the_fabric_primarys() {
        let t = mm(true);
        assert_eq!(who(&shutdown("mesh2"), &t), "mesh1.admin.1", "the fabric primary, outside mesh2");
        assert_eq!(role_move(&shutdown("mesh2"), &t), None, "a delete of a mesh the fabric-primary role is not in moves no role");
        assert_eq!(who(&shutdown("mesh1"), &t), "", "the fabric primary, inside mesh1: no admin executes the delete until its role moved");
        assert!(
            role_move(&shutdown("mesh1"), &t).is_some_and(|m| m.primary.name.to_string() == "mesh1.admin.1" && !m.mesh_role && m.fabric_role),
            "the fabric-primary role alone moves first: {:?}",
            role_move(&shutdown("mesh1"), &t)
        );
        let mut moved = mm(true);
        for n in moved.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh2.admin.1";
        }
        assert_eq!(role_move(&shutdown("mesh1"), &moved), None, "once the role moved, nothing is owed");
        assert_eq!(who(&shutdown("mesh1"), &moved), "mesh2.admin.1", "once the role moved, the new fabric primary is the executor");
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C2
    /// "Successor search for the commanded transfer": "1. the primary's own mesh: the lowest
    /// Ready node-admin NodeId there ... 2. only when that mesh has none, or it is the
    /// departing mesh under mesh.delete: the lowest NodeId among the other Ready
    /// mesh-primaries; 3. neither found: refused by name"; Q1: both roles go to the same-mesh
    /// successor in one row): P, mesh1's primary and the fabric-primary, has a HIGHER NodeId than
    /// mesh2's primary. Both roles go to mesh1.admin.2, never to the lowest-NodeId mesh primary of
    /// another mesh. Under mesh.delete the fabric-primary role alone goes to mesh2's primary. With no other
    /// admin in mesh1 both roles are refused with Q7's mesh text (mesh2's primary could take the fabric
    /// role, the mesh-primary role has no secondary), and with mesh2 gone too with the fabric-primary role's text.
    #[test]
    fn the_fabric_primary_hands_both_primaries_to_its_own_meshs_successor_whatever_the_node_ids() {
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
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::BothRoles)), "mesh1.admin.2", "both roles: mesh1's other admin; mesh2.admin.1's lower NodeId is no input");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::MeshRole)), "mesh1.admin.2");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::FabricRoleOutside)), "mesh2.admin.1", "mesh.delete: the fabric-primary role leaves the departing mesh");
        // mesh1's only admin: the mesh-primary role has no secondary though mesh2's primary is Ready.
        nodes.retain(|n| n.name.to_string() != "mesh1.admin.2");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::BothRoles)), "cannot transfer: no secondary of type node-admin in mesh mesh1");
        // And no other mesh-primary either: both searches named.
        nodes.retain(|n| n.mesh != "mesh2");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::BothRoles)), "cannot transfer fabric-primary: no other node-admin Ready in mesh mesh1, no other mesh-primary Ready");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::FabricRoleOutside)), "cannot transfer fabric-primary: no other node-admin Ready in mesh mesh1, no other mesh-primary Ready");
    }

    /// CONTRACT (node-primary-update.md:159-160, "Successor search for the commanded transfer": "2. only
    /// when that mesh has none, or it is the departing mesh under mesh.delete: the lowest NodeId among the
    /// other Ready mesh-primaries"; mesh-delete.md step 7: a deleted mesh's final primary stays
    /// alive until its own node.delete): the fabric-primary leaving its mesh under mesh.delete hands the
    /// fabric-primary to mesh2's primary, the one other Ready mesh. mesh3 is Dead (its final
    /// primary still a Ready member of the view, with the lowest NodeId) and mesh4's mesh.delete runs
    /// (Leaving, the next lowest): neither is chosen; with mesh2 gone the transfer is refused by name.
    /// Must NOT happen: the fabric-primary handed to the admin of a Dead or deleting mesh.
    #[test]
    fn the_fabric_primary_is_never_handed_to_the_admin_of_a_dead_or_deleting_mesh() {
        use crate::election::{commanded_successor_in, Moving};
        let with = |path: &str, id: &str, fabric: bool| {
            let mut n = node(path, true, fabric);
            n.node_id = rdm_core::ids::NodeId::parse(id).unwrap();
            n
        };
        let mesh = |name: &str, status: ScopeStatus| Mesh { id: Some(MeshId::mint()), name: name.into(), status };
        let mut t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![mesh("mesh1", ScopeStatus::Leaving), mesh("mesh2", ScopeStatus::Ready), mesh("mesh3", ScopeStatus::Dead), mesh("mesh4", ScopeStatus::Leaving)],
            nodes: vec![with("mesh1.admin.1", "5000000000aa", true), with("mesh2.admin.1", "9000000000aa", false), with("mesh3.admin.1", "1000000000aa", false), with("mesh4.admin.1", "2000000000aa", false)],
        };
        let p = t.nodes[0].clone();
        let inc = crate::election::Incumbency::default();
        let name = |n: Result<crate::model::Node, String>| n.map(|n| n.name.to_string()).unwrap_or_else(|e| e);
        assert_eq!(name(commanded_successor_in(&t, &inc, &p, Moving::FabricRoleOutside)), "mesh2.admin.1", "the other Ready mesh's primary, never the Dead mesh3's or the deleting mesh4's");
        t.nodes.retain(|n| n.mesh != "mesh2");
        t.meshes.retain(|m| m.name != "mesh2");
        assert_eq!(name(commanded_successor_in(&t, &inc, &p, Moving::FabricRoleOutside)), "cannot transfer fabric-primary: no other node-admin Ready in mesh mesh1, no other mesh-primary Ready");
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
            BuildFact::Claim(BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }),
            BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt: 1, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }),
        ];
        let p = fold(&facts).remove(&id).unwrap();
        assert_eq!((p.state, p.attempt, p.last_failure), (BuildState::Pending, 1, None));
    }
}
