//! The Build executor (PRD §1.2, §1.4, §7; mesh-control-plane.md §1, §3; node-rpc-envelope.md "Build,
//! op `0x20`").
//!
//! A Build is driven by the fabric-primary alone (`build_drive`): it dispatches each mesh's section to
//! the admin that [`executor_for`] names, that mesh's primary, with `build.section.run` (`build_run`),
//! and runs its own section in process. The executor here runs one section's operations
//! ([`BuildExecutor::run_section`]).

use crate::build::{BuildId, BuildOperation};
use crate::build_state::{BuildProjection, BuildStateAdapter};
use crate::model::{NodeId, PathName};
use crate::topology::Topology;
use std::sync::Arc;
use tokio::sync::RwLock;

/// How a run of one operation ended short of complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunEnd {
    /// The operation's own logic ended it with a named failure. Its section ends failed, never the
    /// Build (Q346): the Build ends `incomplete`, the current-Build read skips it and a caller
    /// resubmits it (R-BI3, R-BI4).
    Failed {
        /// Explanation of the failure.
        reason: String,
        /// The captured standard code.
        code: rdm_core::outcome::Code,
    },
    /// A participant of the operation was lost, or the proof its resume needs is not in yet: the
    /// operation is incomplete and its receipts stand. Its section ends incomplete, no terminal
    /// verdict of the operation is recorded, and a resubmit of the Build resumes it from the receipts
    /// (Q346; R-BI4; mesh-delete.md, "Failure and receipts").
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

/// The most member creates of one section that run at once (Q245).
const CREATES_AT_ONCE: usize = 8;

/// Realises one planned operation (the deployment and lifecycle pipelines).
#[async_trait::async_trait]
pub trait OperationRunner: Send + Sync {
    /// Realise `op` of `build_id`; `Err` names how it ended short of complete ([`RunEnd`]).
    async fn run(&self, build_id: &BuildId, op: &BuildOperation) -> Result<(), RunEnd>;
}

/// The primary of `mesh` in view `t`, its exact birth, when the view names one.
pub fn mesh_primary(mesh: &str, t: &Topology) -> Option<rdm_core::framing::Birth> {
    t.nodes_of_type(mesh, "node-admin").find(|n| n.is_primary).and_then(|n| Some(rdm_core::framing::Birth { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone()? }))
}

/// The admin that executes `op` in view `t`; `None` while no admin can. A drive or a section's plan
/// decides from its view with its atlas's primaries set (`crate::atlas::Atlas::primaries`, Q355): after
/// a primary update it commanded, the next command goes to the node's mesh-primary as the answer
/// streamed back names it, never waiting on gossip (node-primary-update.md step 6).
pub fn executor_for(op: &BuildOperation, t: &Topology) -> Option<NodeId> {
    let fabric = || t.fabric_primary().map(|n| n.node_id.clone());
    match op {
        // Every operation of a node recovery (its node.delete, then its node.create) is
        // executed by the exited birth's recoverer (node-recovery.md "Reconciler", R-ON8): a member's
        // mesh-primary; for a lost node-admin of a mesh with a live mesh-primary, that mesh-primary,
        // and a mesh primary's own recovery by the admin its mesh elects in its place
        // (mesh-recovery.md:8-9). A mesh-primary lost with no node-admin of its mesh Ready for the
        // election to name is the fabric-primary's, the acting primary of a mesh with none (Q335 item 4,
        // [`unheard_mesh_primary_executor`]).
        BuildOperation::DeleteLiveNode { path: node_path, name: node_name, exited: Some(_), .. } | BuildOperation::CreateNode { path: node_path, name: node_name, over: Some(_), .. } => { let node = &rdm_core::PathName { path: node_path.clone(), name: node_name.clone() };
            if rdm_core::is_node_admin(&op.r#type()?) {
                let mesh = op.mesh()?;
                node_admin_recoverer(node, mesh, t).or_else(|| (!t.nodes_of_type(mesh, "node-admin").any(|n| n.location() != *node && n.status == crate::model::NodeStatus::Ready)).then(fabric).flatten())
            } else {
                mesh_executor(op.mesh()?, None, t)
            }
        }
        // A step is executed as the op it calls: a create as node.create, every other step as
        // the node op on its node (the mesh-primary of the node's mesh writes its row, R13).
        // A member node of any kind is born by its own mesh's primary admin (the fabric primary when
        // the mesh has none): the rule names no role, so a product's kinds get the same executor as
        // the proof product's node. A mesh's missing node-admin is created by that mesh's primary
        // node-admin, its claim targeted at the fabric primary (node-admin-lifecycle.md §4.3); the
        // fabric primary creates only the first admin of a mesh that has none.
        BuildOperation::CreateNode { .. } => mesh_executor(op.mesh()?, None, t),
        BuildOperation::DeleteLiveNode { .. } => node_op_executor(&op.node()?, op, t),
        // A mesh delete whose mesh holds the fabric-primary role waits for node.fabric-primary.update ([`role_move`]):
        // the new fabric-primary, outside the leaving mesh, drives the rest (mesh-delete.md step 1).
        BuildOperation::ShutdownMesh { .. } if role_move(op, t).is_some() => None,
        BuildOperation::CreateMesh { .. } | BuildOperation::ShutdownMesh { .. } | BuildOperation::DeleteFabric { .. } => fabric(),
        // A property applied in place is written by the node's mesh-primary, the one writer of its
        // mesh's rows (node-rpc-envelope.md "Node properties"; build.md §1), its own row included.
    }
}

/// Whether `op`, executed by `to` in view `t`, is a section's (Q250): a plain node create (no node
/// recovery's, no replacement's) of a mesh whose own mesh-primary is `to`.
pub fn section_create(op: &BuildOperation, t: &Topology, to: &NodeId) -> bool {
    matches!(op, BuildOperation::CreateNode { mesh, over: None, .. } if t.node_type_primary(mesh, "node-admin").is_some_and(|p| p.node_id == *to))
}

/// The executor of a node op (delete, connections delete, stop, start, delete, and a
/// Build's step) on `node` in view `t`.
fn node_op_executor(node: &PathName, op: &BuildOperation, t: &Topology) -> Option<NodeId> {
    let mesh = op.mesh()?;
    let fabric = || t.fabric_primary().map(|n| n.node_id.clone());
    // The node-type primary moves its role first, by node.primary.update its mesh-primary
    // commands and writes (proposal Part C1, Q3; [`role_move`]): until it has, no admin executes the
    // operation.
    if !rdm_core::is_node_admin(&op.r#type()?) {
        return if role_move(op, t).is_some() { None } else { mesh_executor(mesh, None, t) };
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
    } else if let Some(now) = t.nodes_of_type(mesh, "node-admin").find(|n| n.location() != *node && n.is_primary).filter(|_| is_primary(node, t)) {
        // Every command for a mesh goes to its primary of the moment (Q373): once the role moved, the
        // node-admin that holds it runs the target's delete, while this view still names the target
        // primary too (Q369's window: step 1 the primary update, step 2 the first primary's delete,
        // sent to the second).
        Some(now.node_id.clone())
    } else if is_primary(node, t) {
        let last_mesh_gone = !t.meshes.is_empty() && t.meshes.iter().all(|m| m.status == crate::model::ScopeStatus::Dead) && t.node(node).is_some_and(|n| t.only_node(mesh, &n.node_id));
        held_admins(mesh, Some(node), t).is_empty().then(fabric).flatten().filter(|fp| !t.node(node).is_some_and(|target| &target.node_id == fp) || last_mesh_gone)
    } else {
        mesh_executor(mesh, Some(node), t)
    }
}

/// The executor of an operation on a node of `mesh` (not `target` itself): the mesh's primary
/// admin; the fabric primary only when the mesh has no node-admin left that is not proven gone
/// (R-A2: cross-mesh only when every node-admin of the mesh is proven lost). An admin held but not elected (silent,
/// or ready and waiting on its role) gives `None`: the drive waits for that admin, named by
/// [`held_admins`], and never acts on the mesh from outside it. A `Pending` admin is a birth, not
/// a held role: the fabric primary carries the mesh until it stands.
fn mesh_executor(mesh: &str, target: Option<&PathName>, t: &Topology) -> Option<NodeId> {
    if let Some(p) = t.node_type_primary(mesh, "node-admin") {
        return Some(p.node_id.clone());
    }
    if held_admins(mesh, target, t).is_empty() {
        t.fabric_primary().map(|n| n.node_id.clone())
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
    t.nodes_of_type(mesh, "node-admin").filter(|n| Some(&n.location()) != target && matches!(n.status, NodeStatus::Ready | NodeStatus::ConnectionsDeleting | NodeStatus::PendingReconnect)).collect()
}

/// Why no admin executes `op` in view `t`: the mesh's held node-admins by name and status, or
/// that the view names no admin at all.
pub fn why_no_executor(op: &BuildOperation, t: &Topology) -> String {
    let Some(node) = op.node() else {
        return format!("{:?}: no admin holds the primary role that executes it in this view", op.key());
    };
    let Some(mesh) = op.mesh() else { return format!("{:?} has no subject mesh", op.key()) };
    let held: Vec<String> = held_admins(mesh, Some(&node), t).into_iter().map(|n| format!("node {} ({:?})", n.node_id, n.status)).collect();
    if held.is_empty() {
        format!("{:?}: mesh {} has no primary node-admin and no fabric primary is named in this view", op.key(), mesh)
    } else {
        format!("{:?}: mesh {} has no primary node-admin, and its node-admin(s) {} are not proven gone: the fabric primary does not act on the mesh; the operation waits for the mesh's own role", op.key(), mesh, held.join(", "))
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
pub fn node_admin_recoverer(node: &PathName, mesh: &str, t: &Topology) -> Option<NodeId> {
    match t.nodes_of_type(mesh, "node-admin").find(|n| n.is_primary) {
        Some(p) if p.location() == *node => elected_after_loss(node, t),
        Some(p) if p.status.is_live() => Some(p.node_id.clone()),
        _ => None,
    }
}

/// The admin that runs the recovery of an unheard mesh-primary, the birth `node_id` at `node`, and
/// writes its mesh's rows (node-recovery.md step 2: "For a lost mesh-primary the successor its election
/// names runs it as for any member ... the fabric-primary runs it ... only when the mesh's aggregate stays
/// unheard for twice the staleness floor"; "Execution": "The mesh-primary runs the recovery itself";
/// node-identity-and-state.md: "The fabric-primary writes a row only as acting primary of a mesh with no
/// primary"), decided once, as the recovery starts, from view `t`, the
/// view its rows are written against: the mesh-primary the view names in the unheard birth's place,
/// read from the election and not from liveness, as `mesh_executor` reads it; the fabric-primary,
/// the acting primary, while the mesh holds no other node-admin the election can name, none Ready
/// (elections §4.1: "the lowest Ready node-admin"; Q335 item 4: with no successor "the election names
/// nobody", and the fabric-primary runs the recovery); `None` while the view names no mesh-primary in
/// its place and the mesh holds a Ready node-admin: the election has not named it here yet.
pub fn unheard_mesh_primary_executor(node: &PathName, mesh: &str, node_id: &crate::model::NodeId, t: &Topology) -> Option<NodeId> {
    match t.node_type_primary(mesh, "node-admin") {
        Some(p) if p.node_id != *node_id => Some(p.node_id.clone()),
        _ if !t.nodes_of_type(mesh, "node-admin").any(|n| n.location() != *node && n.status == crate::model::NodeStatus::Ready) => t.fabric_primary().map(|n| n.node_id.clone()),
        _ => None,
    }
}

/// The admin the election names as `target`'s mesh primary once `target`'s exited birth is proven
/// gone (elections §5: "a primary ... whose exact birth is proven gone hands the role to the lowest
/// Ready NodeId of its own mesh"). A proven loss only: a planned move is node.primary.update.
fn elected_after_loss(target: &PathName, t: &Topology) -> Option<NodeId> {
    let primary = t.node(target)?;
    let nodes: Vec<crate::model::Node> = t.births().cloned().collect();
    let mut inc = crate::election::Incumbency::default();
    inc.lost.extend(primary.incarnation_id.clone());
    crate::election::node_type_successor(&nodes, &inc, primary).map(|n| n.node_id)
}

/// Whether `node` holds a role in view `t`: its mesh's node-admin primary or the fabric primary.
fn is_primary(node: &PathName, t: &Topology) -> bool {
    t.node(node).is_some_and(|n| rdm_core::is_node_admin(&n.r#type) && (n.is_primary || n.is_fabric_primary))
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
/// other node-admin in its mesh is still named: node.primary.update refuses it by name (the `BuildService`
/// refuses it first, `control_plane::last_admin`).
pub fn role_move(op: &BuildOperation, t: &Topology) -> Option<RoleMove> {
    if let BuildOperation::ShutdownMesh { mesh, .. } = op {
        if !t.meshes.iter().any(|m| m.name != *mesh && m.status != crate::model::ScopeStatus::Dead) {
            return None;
        }
        let primary = t.fabric_primary().filter(|fp| fp.mesh == *mesh)?.clone();
        return Some(RoleMove { primary, mesh_role: false, fabric_role: true, node_type_role: false, successor: None });
    }
    match op {
        BuildOperation::DeleteLiveNode { exited: None, birth: Some(birth), .. } => node_role_move(birth, true, t),
        _ => None,
    }
}

/// The roles a node command moves before it runs, or a live delete ([`role_move`]): `birth` is the
/// exact birth it acts on (its node.id, incarnation, mesh and type), and `delete` whether it deletes
/// the node; a stop (node.stop) or an `All` connections delete (node.connection.delete) is not a delete.
pub fn node_role_move(birth: &crate::build::PinnedBirth, delete: bool, t: &Topology) -> Option<RoleMove> {
    let subject = t.node_subject(&rdm_core::OperationSubject::Birth { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone() })?;
    if !rdm_core::is_node_admin(&subject.r#type) {
        // Any other node type's role: its primary's role moves first, by node.primary.update its mesh-primary
        // commands and writes (proposal Part C1, Q3), except when deleting the node type's only live node. A primary with
        // no other node of its type is still named for non-delete actions: the update refuses it by Q7's
        // name (the `BuildService` refuses it first, `control_plane::last_admin`).
        let primary = subject.clone();
        let only_of_node_type = t.nodes_of_type(&subject.mesh, &subject.r#type).all(|n| n.node_id == primary.node_id);
        if !t.node_type_primary(&subject.mesh, &subject.r#type).is_some_and(|p| p.node_id == primary.node_id) || (delete && only_of_node_type) {
            return None;
        }
        return Some(RoleMove { primary, mesh_role: false, fabric_role: false, node_type_role: true, successor: None });
    }
    let primary = subject.clone();
    let mesh_role = t.node_type_primary(&subject.mesh, "node-admin").is_some_and(|p| p.node_id == primary.node_id);
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
    let mesh_deleted = t.meshes.iter().any(|m| m.name == subject.mesh && matches!(m.status, crate::model::ScopeStatus::Leaving | crate::model::ScopeStatus::Dead));
    if delete && (t.only_node(&subject.mesh, &primary.node_id) || mesh_deleted) {
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
pub fn lead_for(ops: &[BuildOperation], t: &Topology) -> Option<NodeId> {
    match ops.first() {
        Some(op) => executor_for(op, t),
        None => t.fabric_primary().map(|n| n.node_id.clone()),
    }
}

/// How one section of a Build ended on the admin that ran it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SectionEnd {
    /// Every operation of the section ran.
    Complete {
        /// The operations it ran.
        operations: Vec<BuildOperation>,
    },
    /// A step ended `failed { reason }`: the section ends, never the Build (Q346).
    Failed {
        /// The operation the failed step belongs to.
        operation: rdm_core::OperationId,
        /// Why it failed.
        reason: String,
        /// The captured standard code.
        code: rdm_core::outcome::Code,
    },
    /// An operation ended short of complete without failing (a participant lost, a proof not in,
    /// R-BI4): what ran stands and the section ends; a resubmit of the Build runs what is left (Q346).
    Incomplete {
        /// The operation it stopped at.
        operation: rdm_core::OperationId,
        /// Why.
        reason: String,
    },
    /// A fabric.delete accepted after the Build interrupts it (build.md §1): the operation admitted
    /// before ended within its bound and nothing further is opened.
    Interrupted {
        /// The fabric.delete.
        delete: BuildId,
    },
    /// After the operations it ran, the view names another admin for what follows (a mesh's first
    /// node-admin was created: its members are its own primary's): the fabric-primary plans again.
    Replan {
        /// The operations it ran.
        operations: Vec<BuildOperation>,
    },
}

/// Runs a section of an accepted Build: the operations one admin executes, in plan order.
///
/// The fabric-primary runs its own section in process (mesh creation and deletion, and the members
/// of any mesh with no mesh-primary); a mesh's primary runs its mesh's section when the
/// fabric-primary dispatches it (`build.section.run`, `build_run`), planning the section itself from
/// the Build and its events (Q292). A step whose `rdm.build.step` event ended `complete` is never
/// run again. It never mints a build id and never resumes from a saved instruction pointer.
///
/// Which admin executes an operation (PRD §12.1, `executor_for`): a mesh's admin primary runs the
/// operations on that mesh's members, its node-admins included; the fabric primary runs everything
/// else (mesh creation and deletion) and a mesh's members while that mesh has no admin primary.
pub struct BuildExecutor {
    /// This process's established birth.
    pub executor: rdm_core::framing::Birth,
    /// The Build state.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// The runner that realises each operation.
    pub runner: Arc<dyn OperationRunner>,
}

impl BuildExecutor {
    /// Run `operations`, `mesh`'s section of `build_id`, on this admin. The span is parented to the
    /// dispatch's `context`.
    pub async fn run_section(&self, build_id: &BuildId, mesh: &str, context: &rdm_core::context::CallContext, operations: &[BuildOperation]) -> SectionEnd {
        use tracing::Instrument;
        let me = self.executor.clone();
        let span = tracing::info_span!(
            "rdm.node_admin.build.update.via-reconcile",
            build_id = %build_id.log(),
            mesh,
            executor = %me,
            executor_node_id = %me.node_id,
            executor_incarnation = %me.incarnation,
            operations = tracing::field::Empty,
            incomplete = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        if let Some(tp) = &context.traceparent {
            rdm_transport::set_remote_parent(&span, tp, context.tracestate.as_deref());
        }
        let r = self.run_operations(build_id, operations, &span).instrument(span.clone()).await;
        span.record(
            "outcome",
            match &r {
                SectionEnd::Complete { .. } => "complete",
                SectionEnd::Failed { .. } => "failed",
                SectionEnd::Incomplete { .. } => "incomplete",
                SectionEnd::Interrupted { .. } => "interrupted",
                SectionEnd::Replan { .. } => "replan",
            },
        );
        r
    }

    async fn run_operations(&self, build_id: &BuildId, operations: &[BuildOperation], span: &tracing::Span) -> SectionEnd {
        let build = match self.builds.read_build(build_id).await {
            Ok(b) => b,
            Err(e) => return SectionEnd::Incomplete { operation: rdm_core::OperationId::unscoped("build.section.run"), reason: format!("reading Build {build_id}: {e}") },
        };
        span.record("operations", tracing::field::debug(operations.iter().map(BuildOperation::key).collect::<Vec<_>>()));
        let ran = |upto: usize| -> Vec<BuildOperation> { operations[..upto].to_vec() };
        let mut i = 0;
        while i < operations.len() {
            let op = &operations[i];
            // A fabric.delete accepted after this Build interrupts it (build.md §1): the operation admitted
            // before ended within its bound and its events stand, and nothing further is opened.
            if let Ok(Some(delete)) = crate::build_state::interrupted_by(&*self.builds, &build).await {
                return self.interrupted(&build, op, None, &delete.build_id, span);
            }
            // After the first operation the view can name another executor for the rest: a mesh's first
            // admin, which executes that mesh's members from then on. The fabric-primary plans again.
            if i > 0 {
                let now = &self.executor.node_id;
                let t = self.topology.read().await.clone();
                if executor_for(op, &t).is_some_and(|to| &to != now) {
                    tracing::info!(operation = %op.key(), "the next operation belongs to another admin: the fabric-primary plans again");
                    return SectionEnd::Replan { operations: ran(i) };
                }
            }
            // A run of member creates this admin executes runs concurrently (Q245): the nodes of a
            // mesh are born together, never one after another, a further node-admin with them. A mesh's
            // first node-admin is created on its own: it is the birth the view hands the mesh's members
            // to. A create that ends short of complete stops none of the others; the section ends once
            // every one has ended.
            let group = self.create_group(&operations[i..], &build.topology).await;
            if group > 1 {
                let ended = self.run_group(&build.build_id, &operations[i..i + group]).await;
                if let Some((op, reason, code)) = ended.iter().find_map(|(op, r)| match r {
                    Err(RunEnd::Failed { reason, code }) => Some((*op, reason.clone(), *code)),
                    _ => None,
                }) {
                    if let Ok(Some(delete)) = crate::build_state::interrupted_by(&*self.builds, &build).await {
                        return self.interrupted(&build, op, Some(&reason), &delete.build_id, span);
                    }
                    return SectionEnd::Failed { operation: op.key(), reason, code };
                }
                if let Some((op, reason)) = ended.iter().find_map(|(op, r)| match r {
                    Err(RunEnd::Incomplete(reason)) => Some((*op, reason.clone())),
                    _ => None,
                }) {
                    return self.incomplete(op, reason, span);
                }
                i += group;
                continue;
            }
            match self.runner.run(&build.build_id, op).await {
                Ok(()) => {}
                Err(RunEnd::Failed { reason, code }) => {
                    // An operation admitted before a fabric.delete accepted after this Build fails within its
                    // bound (build.md §1): its failure stands in its events, and the Build's `Failed` names the
                    // delete.
                    if let Ok(Some(delete)) = crate::build_state::interrupted_by(&*self.builds, &build).await {
                        return self.interrupted(&build, op, Some(&reason), &delete.build_id, span);
                    }
                    return SectionEnd::Failed { operation: op.key(), reason, code };
                }
                Err(RunEnd::Incomplete(reason)) => return self.incomplete(op, reason, span),
            }
            i += 1;
        }
        SectionEnd::Complete { operations: operations.to_vec() }
    }

    /// How many of `ops`, from the first, are member creates this admin executes in its view now
    /// (Q245): node creates, each of whose executor is this admin, but a mesh's first node-admin, the
    /// birth the view hands the mesh's members to, and a node's executable replacement, which runs on
    /// its own (build-deployment.md §3).
    async fn create_group(&self, ops: &[BuildOperation], accepted: &crate::accepted::FabricTopology) -> usize {
        let now = &self.executor.node_id;
        let t = self.topology.read().await;
        ops.iter()
            .take_while(|op| matches!(op, BuildOperation::CreateNode { path, name, mesh, .. } if { let node = &PathName { path: path.clone(), name: name.clone() }; accepted.replaced(node).is_none() && accepted.meshes.get(mesh).and_then(|m| m.spec(node)).is_some_and(|spec| !t.first_admin_of(node, mesh, &spec.r#type)) }) && executor_for(op, &t).is_none_or(|to| &to == now))
            .count()
    }

    /// Runs `ops` concurrently, at most [`CREATES_AT_ONCE`] at a time, each to its own end; answers
    /// each with how it ended, in plan order.
    async fn run_group<'a>(&self, build_id: &BuildId, ops: &'a [BuildOperation]) -> Vec<(&'a BuildOperation, Result<(), RunEnd>)> {
        use futures_util::StreamExt;
        // Boxed where each is made: the creates' futures are the runner's, sent to the worker that runs the section.
        let runs: Vec<futures_util::future::BoxFuture<'_, (usize, Result<(), RunEnd>)>> = ops.iter().enumerate().map(|(k, op)| -> futures_util::future::BoxFuture<'_, _> { Box::pin(async move { (k, self.runner.run(build_id, op).await) }) }).collect();
        let mut ended: Vec<(usize, Result<(), RunEnd>)> = futures_util::stream::iter(runs).buffer_unordered(CREATES_AT_ONCE).collect().await;
        ended.sort_by_key(|(k, _)| *k);
        ended.into_iter().map(|(k, r)| (&ops[k], r)).collect()
    }

    /// `op` ended incomplete (R-BI4): a participant of it was lost, or the proof its resume needs is
    /// not in. Its events stand and the Build stays in progress. Nothing is framed to the caller
    /// (node-rpc-envelope.md: Blocked's only source is the subject's Status `Blocked`): the reason is
    /// on the run's span.
    fn incomplete(&self, op: &BuildOperation, reason: String, span: &tracing::Span) -> SectionEnd {
        span.record("incomplete", reason.as_str());
        tracing::info!(operation = %op.key(), reason = %reason, "the operation ended incomplete: its events stand and the Build stays in progress");
        SectionEnd::Incomplete { operation: op.key(), reason }
    }

    /// The section stops at `next` because the fabric.delete `delete` interrupts its Build (build.md
    /// §1): what it ran stands, and `next` is either never opened (`failed` is `None`) or ended within
    /// its bound by failing (`failed` names why, its events standing).
    fn interrupted(&self, build: &BuildProjection, next: &BuildOperation, failed: Option<&str>, delete: &BuildId, span: &tracing::Span) -> SectionEnd {
        let reason = crate::build_state::interrupted_reason(&build.build_id, delete);
        span.record("incomplete", reason.as_str());
        tracing::info_span!("rdm.node_admin.build.update.via-interrupted", build_id = %build.build_id.log(), delete_build_id = %delete.log(), operation = %next.key(), failed = failed.unwrap_or(""))
            .in_scope(|| match failed {
                None => tracing::info!("a fabric.delete accepted after this Build interrupts it: the next operation is not opened"),
                Some(_) => tracing::info!("a fabric.delete accepted after this Build interrupts it: the operation admitted before it failed within its bound"),
            });
        SectionEnd::Interrupted { delete: delete.clone() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::BuildState;
    use crate::model::{Fabric, FabricId, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};

    fn node(name: PathName, mesh: &str, r#type: &str, primary: bool, fabric: bool) -> Node {
        let mut n = Node::allocated(name.clone(), mesh, r#type);
        n.status = NodeStatus::Ready;
        n.incarnation_id = Some(crate::model::IncarnationId::mint());
        n.is_primary = primary;
        n.is_fabric_primary = fabric;
        n
    }

    fn mm(mesh2_admin: bool) -> Topology {
        let mut nodes = vec![node(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true), node(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", true, false)];
        if mesh2_admin {
            nodes.push(node(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false));
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

    fn create(n: PathName, mesh: &str, r#type: &str) -> BuildOperation {
        BuildOperation::CreateNode { mesh: mesh.to_owned(), r#type: r#type.to_string(), node_id: rdm_core::fixture_node_id(&n.clone()), path: n.path.clone(), name: n.name.clone(), over: None, executable: None, entry: None }
    }

    fn expected(t: &Topology, location: &PathName) -> String {
        t.node(location).expect("fixture target exists").node_id.to_string()
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
    /// view still naming as primary the exited primary included. A node-admin of a mesh with no other
    /// node-admin Ready is recovered by the fabric-primary, the acting primary of a mesh with none (Q335
    /// item 4: the election names nobody).
    /// Must NOT happen: a recovery's operation routed to the fabric-primary by a fall-through arm, or
    /// its create routed to the exited mesh primary the view still names.
    #[test]
    fn a_members_recovery_is_executed_by_its_mesh_primary_a_secondary_node_admins_included() {
        let recovery = |n: PathName, mesh: &str, r#type: &str| {
            let (node, from) = (n.clone(), crate::build::ExitedBirth { node_id: crate::model::NodeId::mint(), incarnation: crate::model::IncarnationId::mint() });
            [BuildOperation::DeleteLiveNode { mesh: mesh.to_owned(), r#type: r#type.to_string(), path: node.path.clone(), name: node.name.clone(), exited: Some(from.clone()), birth: None }, BuildOperation::CreateNode { mesh: mesh.to_owned(), r#type: r#type.to_string(), node_id: rdm_core::fixture_node_id(&node), path: node.path.clone(), name: node.name.clone(), over: Some(from), executable: None, entry: None }]
        };
        let recovered_by = |n: PathName, mesh: &str, r#type: &str, t: &Topology| {
            let who: Vec<String> = recovery(n.clone(), mesh, r#type).iter().map(|op| who(op, t)).collect();
            assert!(who.windows(2).all(|w| w[0] == w[1]), "{n:?}: one executor runs both operations: {who:?}");
            who[0].clone()
        };
        let t = mm(true);
        assert_eq!(recovered_by(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", &t), expected(&t, &PathName::new("mesh2.node-admin", "1").unwrap()), "a member of mesh2 is recovered by mesh2's primary");
        assert_eq!(recovered_by(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()), "a member of the fabric-primary's own mesh is recovered by that mesh's primary");
        let fabric_primary = t.fabric_primary().expect("a fabric-primary").node_id.to_string();
        assert_eq!(recovered_by(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", &t), fabric_primary, "mesh2's only node-admin: no successor, the fabric-primary recovers it (Q335)");
        let mut t3 = mm(true);
        t3.nodes.push(node(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", false, false));
        t3.nodes.push(node(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", false, false));
        assert_eq!(recovered_by(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", &t3), expected(&t3, &PathName::new("mesh2.node-admin", "1").unwrap()), "a secondary node-admin is recovered by its mesh's primary");
        assert_eq!(recovered_by(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", &t3), expected(&t3, &PathName::new("mesh1.node-admin", "1").unwrap()), "the fabric-primary's own secondary admin, by its mesh-primary role");
        assert_eq!(recovered_by(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", &t3), expected(&t3, &PathName::new("mesh2.node-admin", "2").unwrap()), "a mesh primary's own recovery is executed by the admin its mesh elects in its place");
        let mut silent = t3.clone();
        silent.nodes.iter_mut().filter(|n| n.location() == PathName::new("mesh2.node-admin", "1").unwrap()).for_each(|n| n.status = NodeStatus::PendingReconnect);
        assert_eq!(recovered_by(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", &silent), silent.fabric_primary().expect("a fabric-primary").node_id.to_string(), "no node-admin of mesh2 Ready: the fabric-primary, the acting primary of a mesh with none (Q335)");
    }

    #[test]
    fn a_meshs_members_are_its_own_primarys_and_everything_else_the_fabric_primarys() {
        let t = mm(true);
        assert_eq!(who(&create(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node"), &t), expected(&t, &PathName::new("mesh2.node-admin", "1").unwrap()));
        assert_eq!(who(&BuildOperation::DeleteLiveNode { r#type: "node".into(), mesh: "mesh2".into(), path: "mesh2.node".into(), name: "1".into(), exited: None, birth: t.node(&rdm_core::PathName::new("mesh2.node", "1").unwrap()).and_then(crate::build::PinnedBirth::of) }, &t), expected(&t, &PathName::new("mesh2.node-admin", "1").unwrap()));
        assert_eq!(who(&create(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()));
        // The rule names no role: a product's kinds get the mesh's own primary admin too.
        assert_eq!(who(&create(PathName::new("mesh2.broker", "1").unwrap(), "mesh2", "broker"), &t), expected(&t, &PathName::new("mesh2.node-admin", "1").unwrap()));
        assert_eq!(who(&BuildOperation::DeleteLiveNode { r#type: "compute".into(), mesh: "mesh2".into(), path: "mesh2.compute".into(), name: "1".into(), exited: None, birth: t.node(&rdm_core::PathName::new("mesh2.compute", "1").unwrap()).and_then(crate::build::PinnedBirth::of) }, &t), expected(&t, &PathName::new("mesh2.node-admin", "1").unwrap()));
        assert_eq!(who(&create(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin"), &t), expected(&t, &PathName::new("mesh2.node-admin", "1").unwrap()), "a mesh's missing admin is created by that mesh's primary");
        assert_eq!(who(&create(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin"), &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()), "its own mesh's primary, which is the fabric primary here");
        assert_eq!(who(&BuildOperation::DeleteLiveNode { r#type: "node-admin".into(), mesh: "mesh2".into(), path: "mesh2.node-admin".into(), name: "1".into(), exited: None, birth: t.node(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).and_then(crate::build::PinnedBirth::of) }, &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()), "mesh2.node-admin.1 is mesh2's only admin: no successor, the fabric primary");
        let mut t3 = mm(true);
        t3.nodes.push(node(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", false, false));
        let (p2, m2) = (PathName::new("mesh2.node-admin", "1").unwrap(), PathName::new("mesh2.node-admin", "2").unwrap());
        assert_eq!(who(&BuildOperation::DeleteLiveNode { mesh: "mesh2".to_owned(), r#type: "node-admin".into(), path: m2.path.clone(), name: m2.name.clone(), exited: None, birth: t3.node(&m2).and_then(crate::build::PinnedBirth::of) }, &t3), expected(&t3, &p2), "a non-primary admin's delete is its mesh primary's");
        assert_eq!(who(&BuildOperation::DeleteLiveNode { mesh: "mesh2".to_owned(), r#type: "node-admin".into(), path: p2.path.clone(), name: p2.name.clone(), exited: None, birth: t3.node(&p2).and_then(crate::build::PinnedBirth::of) }, &t3), "", "a mesh primary's own delete waits for its role to move");
        let mut moved = t3.clone();
        for n in moved.nodes.iter_mut().filter(|n| n.mesh == "mesh2" && rdm_core::is_node_admin(&n.r#type)) {
            n.is_primary = n.location() == m2;
        }
        let stop = |t: &Topology, n: PathName| crate::build::PinnedBirth::of(t.node(&n).expect("the stopped node")).expect("its birth");
        assert!(node_role_move(&stop(&t3, p2.clone()), false, &t3).is_some_and(|m| m.mesh_role && !m.fabric_role), "the stop of mesh2's primary moves its mesh-primary role first");
        assert_eq!(who(&BuildOperation::CreateMesh { mesh: "mesh3".into(), mesh_id: MeshId::mint() }, &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()));
        assert_eq!(who(&shutdown("mesh2"), &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()));
    }

    /// CONTRACT (Q373, Q369, build.md §4 "the mesh's primary of the moment"): every command for a mesh
    /// goes to that mesh's primary of the moment. After the Build's primary update moved mesh2's primary
    /// from mesh2.node-admin.1 to mesh2.node-admin.2, while the view still names the first primary too,
    /// the delete of mesh2.node-admin.1 is executed by mesh2.node-admin.2. Must NOT happen: the
    /// fabric-primary executing it (fabric_atlas's red at dec57eef9: its NodeDeleting refused as not
    /// mesh2's writer).
    #[test]
    fn a_delete_of_the_first_primary_goes_to_the_second_primary() {
        let mut t = mm(true);
        let (first, second) = (PathName::new("mesh2.node-admin", "1").unwrap(), PathName::new("mesh2.node-admin", "2").unwrap());
        t.nodes.push(node(second.clone(), "mesh2", "node-admin", true, false));
        assert!(t.node(&first).is_some_and(|n| n.is_primary), "the view still names the first primary");
        let delete = BuildOperation::DeleteLiveNode { mesh: "mesh2".to_owned(), r#type: "node-admin".into(), path: first.path.clone(), name: first.name.clone(), exited: None, birth: t.node(&first).and_then(crate::build::PinnedBirth::of) };
        assert_eq!(who(&delete, &t), expected(&t, &second));
    }

    /// `mm(true)` with pinned NodeIds: `fp`, the lower, is the node-admin the election names as the
    /// fabric primary; the other admin and `mesh1.node.1` are higher.
    fn elected(fp: PathName) -> Topology {
        let mut t = mm(true);
        for n in t.nodes.iter_mut() {
            let id = if n.location() == fp {
                "1000000000aa"
            } else if rdm_core::is_node_admin(&n.r#type) {
                "5000000000aa"
            } else {
                "9000000000aa"
            };
            n.node_id = rdm_core::ids::NodeId::parse(id).unwrap();
        }
        crate::election::resolve(&mut t.nodes);
        assert_eq!(t.fabric_primary().map(|n| n.node_id.clone()), Some(t.node(&fp).unwrap().node_id.clone()), "the pinned ids role {fp:?}");
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
        let delete = |t: &Topology, n: PathName, mesh: &str, r#type: &str| BuildOperation::DeleteLiveNode { mesh: mesh.to_owned(), r#type: r#type.to_string(), path: n.path.clone(), name: n.name.clone(), exited: None, birth: t.node(&n).and_then(crate::build::PinnedBirth::of) };
        let stop = |t: &Topology, n: PathName| crate::build::PinnedBirth::of(t.node(&n).expect("the stopped node")).expect("its birth");
        // mesh1.node-admin.1 holds the fabric-primary role, mesh1.node.1 beside it.
        let t = elected(PathName::new("mesh1.node-admin", "1").unwrap());
        let op = delete(&t, PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        assert_eq!(executor_for(&op, &t), None, "{op:?}: never the fabric primary; no admin until its roles moved");
        assert!(role_move(&op, &t).is_some_and(|m| m.mesh_role && m.fabric_role), "{op:?}: both roles move first: {:?}", role_move(&op, &t));
        let fp_stop = node_role_move(&stop(&t, PathName::new("mesh1.node-admin", "1").unwrap()), false, &t);
        assert!(fp_stop.as_ref().is_some_and(|m| m.mesh_role && m.fabric_role), "its stop moves both roles first: {fp_stop:?}");
        assert_eq!(who(&delete(&t, PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin"), &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()), "mesh2's only admin has no mesh primary beside it: the fabric primary deletes it");
        // mesh2.node-admin.1 holds the fabric-primary role and is its mesh's only node.
        let t = elected(PathName::new("mesh2.node-admin", "1").unwrap());
        assert_eq!(executor_for(&delete(&t, PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin"), &t), None, "never the fabric primary; no admin until its fabric-primary role moved");
        assert!(
            role_move(&delete(&t, PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin"), &t).is_some_and(|m| !m.mesh_role && m.fabric_role),
            "the delete of its mesh's only node moves the fabric-primary role alone: {:?}",
            role_move(&delete(&t, PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin"), &t)
        );
        assert!(node_role_move(&stop(&t, PathName::new("mesh2.node-admin", "1").unwrap()), false, &t).is_some_and(|m| m.mesh_role && m.fabric_role), "its stop moves both roles");
        assert_eq!(who(&delete(&t, PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), &t), "", "mesh1's primary, beside mesh1.node.1, waits for its own role to move");
        let mut moved = t.clone();
        for n in moved.nodes.iter_mut() {
            n.is_fabric_primary = n.location() == PathName::new("mesh1.node-admin", "1").unwrap();
        }
        assert_eq!(role_move(&delete(&moved, PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin"), &moved), None, "once the fabric-primary role moved, nothing is owed");
        assert_eq!(who(&delete(&moved, PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin"), &moved), expected(&moved, &PathName::new("mesh1.node-admin", "1").unwrap()), "the new fabric primary executes it");
        // Alone, the fabric primary has no successor: nobody executes its delete.
        let mut alone = mm(false);
        crate::election::resolve(&mut alone.nodes);
        let fp = alone.fabric_primary().unwrap().location();
        assert_eq!(executor_for(&BuildOperation::DeleteLiveNode { mesh: "mesh1".to_owned(), r#type: "node-admin".into(), path: fp.path.clone(), name: fp.name.clone(), exited: None, birth: alone.node(&fp).and_then(crate::build::PinnedBirth::of) }, &alone), None);
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
        let mut second = node(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", false, false);
        second.status = NodeStatus::PendingReconnect;
        t.nodes.push(second);
        // mesh2.node.1 holds mesh2's node role; the delete is of mesh2.node.3, which holds none, so no role
        // moves before it and its executor alone is the question.
        t.nodes.push(node(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", true, false));
        t.nodes.push(node(PathName::new("mesh2.node", "3").unwrap(), "mesh2", "node", false, false));
        for n in t.nodes.iter_mut().filter(|n| n.location() == PathName::new("mesh2.node-admin", "1").unwrap()) {
            n.status = NodeStatus::Dead;
            n.is_primary = false;
        }
        let ops = [BuildOperation::DeleteLiveNode { r#type: "node".into(), mesh: "mesh2".into(), path: "mesh2.node".into(), name: "3".into(), exited: None, birth: t.node(&PathName::new("mesh2.node", "3").unwrap()).and_then(crate::build::PinnedBirth::of) }, create(PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), create(PathName::new("mesh2.node-admin", "3").unwrap(), "mesh2", "node-admin"), BuildOperation::DeleteLiveNode { r#type: "node-admin".into(), mesh: "mesh2".into(), path: "mesh2.node-admin".into(), name: "1".into(), exited: None, birth: None }];
        for op in &ops {
            assert_eq!(executor_for(op, &t), None, "{op:?}: mesh2.node-admin.2 is held, not proven gone: never the fabric primary");
            let why = why_no_executor(op, &t);
            assert!(why.contains(&t.node(&PathName::new("mesh2.node-admin", "2").unwrap()).unwrap().node_id.to_string()) && why.contains("PendingReconnect"), "{why}");
        }
        for n in t.nodes.iter_mut().filter(|n| n.location() == PathName::new("mesh2.node-admin", "2").unwrap()) {
            n.status = NodeStatus::Dead;
        }
        assert_eq!(who(&ops[0], &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()), "every mesh2 admin proven gone: the fabric primary");
        for n in t.nodes.iter_mut().filter(|n| n.location() == PathName::new("mesh2.node-admin", "2").unwrap()) {
            n.status = NodeStatus::Pending;
        }
        assert_eq!(who(&ops[0], &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()), "a Pending admin is a birth the fabric primary carries");
        for n in t.nodes.iter_mut().filter(|n| n.location() == PathName::new("mesh2.node-admin", "2").unwrap()) {
            n.status = NodeStatus::Ready;
            n.is_primary = true;
        }
        assert_eq!(who(&ops[0], &t), expected(&t, &PathName::new("mesh2.node-admin", "2").unwrap()), "once primary, the same-mesh successor executes");
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
        assert_eq!(who(&shutdown("mesh2"), &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()), "the fabric primary, outside mesh2");
        assert_eq!(role_move(&shutdown("mesh2"), &t), None, "a delete of a mesh the fabric-primary role is not in moves no role");
        assert_eq!(who(&shutdown("mesh1"), &t), "", "the fabric primary, inside mesh1: no admin executes the delete until its role moved");
        assert!(
            role_move(&shutdown("mesh1"), &t).is_some_and(|m| m.primary.location() == PathName::new("mesh1.node-admin", "1").unwrap() && !m.mesh_role && m.fabric_role),
            "the fabric-primary role alone moves first: {:?}",
            role_move(&shutdown("mesh1"), &t)
        );
        let mut moved = mm(true);
        for n in moved.nodes.iter_mut() {
            n.is_fabric_primary = n.location() == PathName::new("mesh2.node-admin", "1").unwrap();
        }
        assert_eq!(role_move(&shutdown("mesh1"), &moved), None, "once the role moved, nothing is owed");
        assert_eq!(who(&shutdown("mesh1"), &moved), expected(&moved, &PathName::new("mesh2.node-admin", "1").unwrap()), "once the role moved, the new fabric primary is the executor");
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C2
    /// "Successor search for the commanded transfer": "1. the primary's own mesh: the lowest
    /// Ready node-admin NodeId there ... 2. only when that mesh has none, or it is the
    /// departing mesh under mesh.delete: the lowest NodeId among the other Ready
    /// mesh-primaries; 3. neither found: refused by name"; Q1: both roles go to the same-mesh
    /// successor in one row): P, mesh1's primary and the fabric-primary, has a HIGHER NodeId than
    /// mesh2's primary. Both roles go to mesh1.node-admin.2, never to the lowest-NodeId mesh primary of
    /// another mesh. Under mesh.delete the fabric-primary role alone goes to mesh2's primary. With no other
    /// admin in mesh1 both roles are refused with Q7's mesh text (mesh2's primary could take the fabric
    /// role, the mesh-primary role has no secondary), and with mesh2 gone too with the fabric-primary role's text.
    #[test]
    fn the_fabric_primary_hands_both_primaries_to_its_own_meshs_successor_whatever_the_node_ids() {
        use crate::election::{commanded_successor, Moving};
        let with = |path: PathName, mesh: &str, r#type: &str, id: &str, primary: bool, fabric: bool| {
            let mut n = node(path.clone(), mesh, r#type, primary, fabric);
            n.node_id = rdm_core::ids::NodeId::parse(id).unwrap();
            n
        };
        let mut nodes = vec![
            with(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "5000000000aa", true, true),
            with(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", "9000000000aa", false, false),
            with(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", "6000000000aa", true, false),
            with(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", "1000000000aa", true, false),
        ];
        let inc = crate::election::Incumbency::default();
        let p = nodes[0].clone();
        let name = |n: Result<crate::model::Node, String>| n.map(|n| n.location());
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::BothRoles)), Ok(PathName::new("mesh1.node-admin", "2").unwrap()), "both roles: mesh1's other admin; mesh2.node-admin.1's lower NodeId is no input");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::MeshRole)), Ok(PathName::new("mesh1.node-admin", "2").unwrap()));
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::FabricRoleOutside)), Ok(PathName::new("mesh2.node-admin", "1").unwrap()), "mesh.delete: the fabric-primary role leaves the departing mesh");
        // mesh1's only admin: the mesh-primary role has no secondary though mesh2's primary is Ready.
        nodes.retain(|n| n.location() != PathName::new("mesh1.node-admin", "2").unwrap());
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::BothRoles)), Err("cannot transfer: no secondary of type node-admin in mesh mesh1".to_string()));
        // And no other mesh-primary either: both searches named.
        nodes.retain(|n| n.mesh != "mesh2");
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::BothRoles)), Err("cannot transfer fabric-primary: no other node-admin Ready in mesh mesh1, no other mesh-primary Ready".to_string()));
        assert_eq!(name(commanded_successor(&nodes, &inc, &p, Moving::FabricRoleOutside)), Err("cannot transfer fabric-primary: no other node-admin Ready in mesh mesh1, no other mesh-primary Ready".to_string()));
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
        let with = |path: PathName, mesh: &str, r#type: &str, id: &str, fabric: bool| {
            let mut n = node(path.clone(), mesh, r#type, true, fabric);
            n.node_id = rdm_core::ids::NodeId::parse(id).unwrap();
            n
        };
        let mesh = |name: &str, status: ScopeStatus| Mesh { id: Some(MeshId::mint()), name: name.into(), status };
        let mut t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![mesh("mesh1", ScopeStatus::Leaving), mesh("mesh2", ScopeStatus::Ready), mesh("mesh3", ScopeStatus::Dead), mesh("mesh4", ScopeStatus::Leaving)],
            nodes: vec![with(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "5000000000aa", true), with(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", "9000000000aa", false), with(PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin", "1000000000aa", false), with(PathName::new("mesh4.node-admin", "1").unwrap(), "mesh4", "node-admin", "2000000000aa", false)],
        };
        let p = t.nodes[0].clone();
        let inc = crate::election::Incumbency::default();
        let name = |n: Result<crate::model::Node, String>| n.map(|n| n.location());
        assert_eq!(name(commanded_successor_in(&t, &inc, &p, Moving::FabricRoleOutside)), Ok(PathName::new("mesh2.node-admin", "1").unwrap()), "the other Ready mesh's primary, never the Dead mesh3's or the deleting mesh4's");
        t.nodes.retain(|n| n.mesh != "mesh2");
        t.meshes.retain(|m| m.name != "mesh2");
        assert_eq!(name(commanded_successor_in(&t, &inc, &p, Moving::FabricRoleOutside)), Err("cannot transfer fabric-primary: no other node-admin Ready in mesh mesh1, no other mesh-primary Ready".to_string()));
    }

    #[test]
    fn a_mesh_without_an_admin_primary_has_its_members_run_by_the_fabric_primary() {
        let t = mm(false);
        assert_eq!(who(&create(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node"), &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()));
        assert_eq!(who(&create(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin"), &t), expected(&t, &PathName::new("mesh1.node-admin", "1").unwrap()), "the first admin of a mesh that has none is the fabric primary's");
        assert_eq!(lead_for(&[], &t), t.fabric_primary().map(|n| n.node_id.clone()), "an empty plan is closed by the fabric primary");
        assert_eq!(lead_for(&[create(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node")], &t), t.fabric_primary().map(|n| n.node_id.clone()));
    }
}
