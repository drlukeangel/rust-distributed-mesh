//! node.primary.update and node.fabric-primary.update on op `0x21` at a node-admin
//! (lifecycles/node-primary-update.md; node-rpc-envelope.md "PrimaryUpdate, op `0x21`, rdm").
//!
//! ```text
//! fabric-primary (commander)     NA1 (mesh-primary)                          NA2 (successor)
//!   UpdatePrimary --------------> <- PrimaryUpdating (Started)
//!                                 2 the election over its own view, NA1 excluded,
//!                                   names the successor; none -> Failed by name
//!                                 3 finish the writes it has issued (Q109)
//!                                 4 PrimaryUpdate (the election's result) ---> PrimaryUpdating; installs
//!                                   <--------------------------------------- PrimaryUpdated { NA2 }
//!   <---------------------------- PrimaryUpdated { NA2 } (Terminal)          5 its digest carries it;
//!   NA1's next command goes to NA2                                            it records its own row
//! ```
//!
//! The same primitive serves every node type (`rdm_node::member_handover`): the fabric-primary commands
//! a mesh-primary, a mesh-primary commands the primaries of its mesh's other node types. Who is primary
//! is the election's result; the write records the role and does not grant it, and the record follows
//! the role through the mesh rows writer (`crate::admin::rows_writer::keep_mesh_rows`). No row and no
//! frame of its own record the change. The fabric-primary, the commander of a mesh-primary, records the
//! answer in its atlas (Q355): `is_primary` on the `node_id` it names, removed from the node it sent the
//! update to, read wherever it decides which node runs a node command (`crate::atlas::Atlas::primaries`);
//! a mesh-primary, the commander of its mesh's other node types' primaries, records their answers in its
//! atlas the same way, and the fabric-primary records the answer the mesh-primary streams back to it;
//! every other node learns the new primary from gossip. The Build's next command goes to the node's
//! mesh-primary as it stands after the update, read from the atlas, never waiting on gossip.

pub use crate::election::Moving;
use crate::model::{IncarnationId, NodeId, PathName};
use crate::topology::Topology;
use rdm_core::framing::Birth;
use rdm_core::handover::{HandoverBirth, NodePrimaryUpdate, NodePrimaryUpdateReply as Reply, NodePrimaryUpdateRequest as Request, FABRIC};
use rdm_core::outcome::RpcOutcome;
use rdm_core::PrimaryRole;
use rdm_core::catalog::OpOwner;
use rdm_node::member_handover::{after_updating, call_successor, call_update, entries_named, updating, Answered, Entry, STEP};
use rdm_transport::stream::{NotStarted, ReplySink};
use rdm_transport::topology::{Backbone, DigestView, TopologyView};
use rdm_transport::{CallOptions, NodeRpcClient, PeerContext, ServerBuilder};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use tracing::Instrument as _;

/// The primary update that precedes an admitted operation. Its role and the complete
/// preceding operation are structured fields; Build and run are carried separately.
pub fn update_operation(role: PrimaryRole, operation: &rdm_core::OperationId) -> rdm_core::OperationId {
    rdm_core::OperationId::primary_update(role, operation)
}

/// The time limit of the command (`UpdatePrimary`): its reply is the primary's whole update, so it
/// is the primary's own bound plus the transport's 10 s (Q311): its call to the successor, node
/// investigation of the successor when that call is not answered, and the call made again once
/// investigation reaches it (`call_and_look`). Each call to the successor is `rdm_transport::Send::default()`: its
/// attempts' time limits and the schedule's delays between them.
fn command_limit() -> std::time::Duration {
    let r = rdm_transport::Send::default();
    let delays = rdm_core::cadence::gossip_interval() * ((1u32 << (r.attempts.max(1) - 1)) - 1);
    let successor_call = r.time_limit.0 * u32::from(r.attempts) + delays;
    successor_call * 2 + crate::offline::tickle_ladder_at_default_budget() + rdm_transport::TRANSPORT_LIMIT
}

/// Finishes every write this admin's stores have issued (node-primary-update.md step 3, Q109): the
/// primary runs it before it calls its successor, so no write it issued lands after it.
pub type FinishWrites = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// What projects an admin's view from its held records now (`crate::admin::view_projector`).
pub type ProjectView = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// One update at a time at each side.
#[derive(Default)]
pub struct UpdateLocks {
    /// The successor's install.
    serving: tokio::sync::Mutex<()>,
    /// The primary's update.
    updating: tokio::sync::Mutex<()>,
}

/// The updates this admin holds node.primary.updated for, by operation: it answered the call as the
/// successor, or obtained the successor's answer as the primary or the commander.
#[derive(Default)]
pub struct UpdateMemo {
    /// Each update held acknowledged, and whether this admin is its successor.
    acknowledged: std::sync::Mutex<std::collections::BTreeMap<(String, rdm_core::OperationId), bool>>,
    /// What this admin answered as the successor, by operation.
    answered: Answered,
}

impl UpdateMemo {
    fn acknowledge(&self, build_id: &str, operation: &rdm_core::OperationId, successor: bool) {
        self.acknowledged.lock().unwrap().insert((build_id.to_owned(), operation.clone()), successor);
    }

    /// `Some(whether this admin is the successor)` once `operation` is acknowledged here.
    fn acknowledged(&self, build_id: &str, operation: &rdm_core::OperationId) -> Option<bool> {
        self.acknowledged.lock().unwrap().get(&(build_id.to_owned(), operation.clone())).copied()
    }
}

/// Every side of node.primary.update on one node-admin: the primary's update, the successor's
/// install, the commander's command and its resume.
pub struct PrimaryUpdate {
    /// The fabric's name.
    pub fabric: String,
    /// This admin's path.
    pub me_path: String,
    /// This node's name, beside its path.
    pub me_name: String,
    /// This admin's node id.
    pub node_id: NodeId,
    /// This admin's incarnation.
    pub incarnation: IncarnationId,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// This admin's mesh channel (the primaries' records, its clock).
    pub topology_view: TopologyView,
    /// This admin's place on the backbone.
    pub backbone: Backbone,
    /// The Node RPC client every call is made through.
    pub client: Arc<NodeRpcClient>,
    /// What this admin holds of the digests (the election's inputs).
    pub digest_view: DigestView,
    /// The records the election's inputs come from, and whether the initial fabric first reached
    /// ready.
    pub records: Arc<crate::admin::Records>,
    /// One update at a time at each side.
    pub locks: UpdateLocks,
    /// The updates this admin holds node.primary.updated for.
    pub memo: UpdateMemo,
    /// The lock of this admin's Build-log decisions (`FabricBuildStateAdapter::decisions`): held across
    /// an install, so a decision in flight completes before the primaries move.
    pub decisions: Arc<tokio::sync::Mutex<()>>,
    /// Projects this admin's view from the records it holds now. Run once an install is held, before
    /// the call that depends on it is answered: every reader of the view (the `BuildService`,
    /// the drive, the executor plan, the rows writers) decides from it.
    pub project_view: ProjectView,
    /// Finishes every write this admin issued, before its call (node-primary-update.md step 3).
    pub finish_writes: FinishWrites,
    /// node investigation on one exact birth (node-investigation.md), run on a successor that did not
    /// answer the call.
    pub looker: crate::primary_watch::Looker,
    /// This admin's atlas, which records each update's answer as the commander (Q355): `is_primary`
    /// (`is_fabric_primary`) on the `node_id` it names, removed from the node the update was sent to.
    pub atlas: Arc<crate::atlas::Atlas>,
}

impl PrimaryUpdate {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
}

/// The node id of the successor an update's `entries` name (one successor for every entry).
fn successor_of(entries: &[Entry]) -> Option<NodeId> {
    entries.first().map(|(_, _, successor, _)| successor.node_id.clone())
}

fn birth_of(n: &crate::model::Node) -> Option<HandoverBirth> {
    Some(HandoverBirth { mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone()? })
}

fn exact(b: &HandoverBirth) -> Birth {
    Birth { node_id: b.node_id.clone(), incarnation: b.incarnation.clone() }
}

fn births(b: &HandoverBirth) -> String {
    format!("{}:{}:{}", b.mesh, b.node_id, b.incarnation.as_str())
}

/// The refusal span of node.primary.update at `side` (`primary`, `successor`, `commander`, `winner`),
/// with `reason` and its detail.
fn refused(side: &'static str, reason: &'static str, detail: &str, operation: &rdm_core::OperationId) {
    tracing::info_span!("rdm.node_admin.primary.reject.via-primary-update", side, reason, detail, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step)
        .in_scope(|| tracing::info!("node.primary.update was refused"));
}

impl PrimaryUpdate {
    fn held_name(&self) -> PathName {
        self.digest_view.held_path_name().unwrap_or_else(|| self.me())
    }

    fn incumbency(&self) -> crate::election::Incumbency {
        crate::admin::incumbency_of(&self.digest_view, &self.records)
    }

    fn me_birth(&self) -> HandoverBirth {
        HandoverBirth { mesh: self.topology_view.mesh_name().to_string(), node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }
    }

    /// Install `entries`, an update whose successor is of `mesh`, in this admin's own view under the
    /// decision lock, and project the view from them: the successor installing itself (step 4), or the
    /// primary whose own roles moved to the successor its election named. A commander installs nothing.
    async fn install(&self, entries: &[Entry], mesh: &str) -> &'static str {
        let outcome = {
            let _decisions = self.decisions.clone().lock_owned().await;
            rdm_node::member_handover::install(self.topology_view.primaries(), entries, mesh)
        };
        (self.project_view)().await;
        outcome
    }

    /// Record in this admin's atlas the answer `node_id` to the update of `primary` it sent, or the
    /// answer streamed back to it (Q355): `is_primary` on `node_id`, removed from `primary`.
    fn record_answer(&self, primary: &crate::model::Node, node_id: &NodeId) {
        self.atlas.primary_updated(self.topology_view.primaries(), &primary.mesh, &primary.r#type, &primary.node_id, node_id);
    }

    // ---- the successor (step 4) ----

    /// node.primary.update at its receiver: it answers node.primary.updating, installs the election's
    /// result the call carries in its own view (every entry names it, of its own mesh) and answers
    /// node.primary.updated with its own node id; as a node-admin it then records its own row (the mesh
    /// rows writer follows its view). It refuses only a conflicting payload, never the birth the call
    /// names (R-UC1a, R-RX5).
    async fn serve_primary_update(&self, primaries: Vec<Entry>, build_id: String, operation: rdm_core::OperationId, sink: ReplySink<NodePrimaryUpdate, NotStarted>) -> Reply {
        let answered = &self.memo.answered;
        let _one_at_a_time = self.locks.serving.lock().await;
        let payload = rdm_node::member_handover::AnswerPayload::Install(primaries.clone());
        let before = answered.before(&build_id, &operation, &payload);
        if let Some(r @ Reply::RejectedConflictingPayload { .. }) = before {
            return r;
        }
        updating(sink).await;
        if let Some(r) = before {
            return r;
        }
        // The span covers the install: its start is the moment the successor begins to install.
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-primary-updated", node_path = %self.held_name().path, node_name = %self.held_name().name, fabric = %self.fabric, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, build_id = %build_id, primaries = %entries_named(&primaries), outcome = tracing::field::Empty);
        let outcome = self.install(&primaries, self.topology_view.mesh_name()).instrument(span.clone()).await;
        span.record("outcome", outcome);
        span.in_scope(|| tracing::info!("the successor installed the election's result the call carries and answers node.primary.updated"));
        if primaries.iter().any(|(_, _, s, _)| s.node_id == self.node_id) {
            self.memo.acknowledge(&build_id, &operation, true);
        }
        let reply = Reply::PrimaryUpdated { node_id: self.node_id.clone() };
        answered.keep(&build_id, &operation, payload, reply.clone());
        reply
    }

    // ---- the primary (steps 2-4) ----

    /// node.primary.update commanded from above, on this admin: `node-admin` moves its mesh-primary (and
    /// the fabric-primary with it when it is both), `fabric` the fabric-primary, any other node type is
    /// the primary of that type in this admin's mesh, which this admin, its mesh-primary, commands. Once
    /// it is the primary the command names it answers node.primary.updating, then node.primary.updated
    /// with the new primary's node id, or `Failed`.
    async fn serve_update_primary(self: &Arc<Self>, r#type: String, build_id: String, operation: rdm_core::OperationId, sink: ReplySink<NodePrimaryUpdate, NotStarted>) -> Reply {
        let me = self.held_name();
        let primary_view = self.topology_view.primaries();
        let mesh_primary = primary_view.mesh(self.topology_view.mesh_name()).is_some_and(|h| h.is_birth(&self.node_id, &self.incarnation));
        let fabric_primary = primary_view.fabric().is_some_and(|h| h.is_birth(&self.node_id, &self.incarnation));
        let moving = if rdm_core::is_node_admin(&r#type) {
            if !mesh_primary {
                let reason = format!("path {me_path:?}, name {me_name:?} is not the mesh-primary of {}: its view names {}", self.topology_view.mesh_name(), primary_view.mesh(self.topology_view.mesh_name()).map(|h| h.to_string()).unwrap_or_else(|| "no primary".into()), me_path = &me.path, me_name = &me.name);
                refused("primary", "not-primary", &reason, &operation);
                return rdm_core::reply::handover::bounded_not_primary(&reason);
            }
            if fabric_primary { Moving::BothRoles } else { Moving::MeshRole }
        } else if r#type == FABRIC {
            if !fabric_primary {
                let reason = format!("path {me_path:?}, name {me_name:?} is not the fabric-primary: its view names {}", primary_view.fabric().map(|h| h.to_string()).unwrap_or_else(|| "no primary".into()), me_path = &me.path, me_name = &me.name);
                refused("primary", "not-primary", &reason, &operation);
                return rdm_core::reply::handover::bounded_not_primary(&reason);
            }
            let leaving = matches!(&operation.subject, Some(rdm_core::OperationSubject::PrimaryUpdate { preceding, .. }) if preceding.action == "mesh.delete");
            let same_mesh_successor = !leaving && self.topology.read().await.births().any(|n| n.mesh == self.topology_view.mesh_name() && rdm_core::is_node_admin(&n.r#type) && n.node_id != self.node_id && n.status == crate::model::NodeStatus::Ready);
            if mesh_primary && same_mesh_successor { Moving::BothRoles } else { Moving::FabricRoleOutside }
        } else {
            let view = self.topology.read().await.clone();
            let primary = view.node_type_primary(self.topology_view.mesh_name(), &r#type).cloned();
            let Some(primary) = primary.filter(|_| mesh_primary) else {
                let reason = format!("path {me_path:?}, name {me_name:?} is not the mesh-primary of {} naming a {} primary", self.topology_view.mesh_name(), r#type, me_path = &me.path, me_name = &me.name);
                refused("commander", "not-primary", &reason, &operation);
                return rdm_core::reply::handover::bounded_not_primary(&reason);
            };
            updating(sink).await;
            return after_updating(match self.update_node_type_primary(&primary, None, &build_id, &operation).await {
                Ok(node_id) => Reply::PrimaryUpdated { node_id },
                Err(refusal) => refusal,
            });
        };
        updating(sink).await;
        after_updating(match self.update_primary(moving, None, &build_id, &operation, None).await {
            Ok(node_id) => Reply::PrimaryUpdated { node_id },
            Err(refusal) => refusal,
        })
    }

    /// node.primary.update at this admin, the primary of `moving` (steps 2-4): the election over its own
    /// view with itself excluded names the successor (`named` names it for the hand-back); none is
    /// `Failed` by name. It finishes the writes it has issued, calls the successor carrying the election's
    /// result, installs that result in its own view once the successor answered (its own roles left it),
    /// and answers with the successor's node id. A successor that does not answer is looked at by node
    /// investigation: reached, the call is made again; any other verdict is `Failed`, the successor
    /// unheard after the update. An identical repeat is answered as the first was.
    pub async fn update_primary(&self, moving: Moving, _expected: Option<u64>, build_id: &str, operation: &rdm_core::OperationId, named: Option<&PathName>) -> Result<NodeId, Reply> {
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-update-primary", node_path = %me.path, node_name = %me.name, moving = moving.name(), build_id, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, successor = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let r = self.update_primary_in(moving, build_id, operation, named).instrument(span.clone()).await;
        match &r {
            Ok(_) => {
                self.memo.acknowledge(build_id, operation, false);
                span.record("outcome", "updated")
            }
            Err(Reply::Failed { reason, .. }) => span.record("outcome", "failed").record("detail", reason.as_str()),
            Err(other) => span.record("outcome", other.name()).record("detail", format!("{other:?}").as_str()),
        };
        span.in_scope(|| tracing::info!("node.primary.update ran at the primary"));
        r
    }

    /// [`Self::update_primary`] moving this admin's own primaries as a step of its Build (the
    /// fabric-primary has nothing above it), its end told apart as the step needs it: the new
    /// primary's node id, recorded in this admin's atlas (Q355): `is_primary` on it in this admin's mesh
    /// when the mesh-primary role moved, `is_fabric_primary` when the fabric-primary role moved, each
    /// removed from this admin, while its records still name this admin (its install has moved them
    /// already when it installed the successor's answer).
    pub async fn update_own_primaries(&self, moving: Moving, build_id: &str, operation: &rdm_core::OperationId, named: Option<&PathName>) -> Result<NodeId, crate::build_drive::RoleMoveEnd> {
        use crate::build_drive::RoleMoveEnd;
        let node_id = match self.update_primary(moving, None, build_id, operation, named).await {
            Ok(node_id) => node_id,
            Err(Reply::AlreadyApplied { revisions }) => successor_of(&revisions).ok_or_else(|| RoleMoveEnd::Refused(format!("{}: node.primary.update {operation:?} answered already-applied naming no successor", self.node_id)))?,
            Err(Reply::Failed { step, reason, code }) => return Err(RoleMoveEnd::Failed { step, reason, code }),
            Err(other) => return Err(RoleMoveEnd::Refused(format!("{}: {other:?}", other.name()))),
        };
        if moving.moves_mesh_primary() {
            self.atlas.primary_updated(self.topology_view.primaries(), self.topology_view.mesh_name(), rdm_core::NODE_ADMIN, &self.node_id, &node_id);
        }
        if moving.moves_fabric_primary() {
            self.atlas.fabric_primary_updated(self.topology_view.primaries(), &self.node_id, &node_id);
        }
        Ok(node_id)
    }

    async fn update_primary_in(&self, moving: Moving, build_id: &str, operation: &rdm_core::OperationId, named: Option<&PathName>) -> Result<NodeId, Reply> {
        let _one_at_a_time = self.locks.updating.lock().await;
        let me_name = self.held_name();
        let me = self.me_birth();
        let primary_view = self.topology_view.primaries();
        // The hand-back repeated once it applied (fabric-recovery.md step 4): the fabric-primary is the
        // birth this admin's view holds at `named` already.
        if let (true, Some(name)) = (moving.moves_fabric_primary(), named) {
            let to = self.topology.read().await.node(name).and_then(birth_of);
            if let (Some(to), Some(h)) = (to, primary_view.fabric()) {
                if h.is_birth(&to.node_id, &to.incarnation) {
                    return Err(rdm_core::reply::handover::already_applied(vec![(FABRIC.to_string(), exact(&me), exact(&to), h.revision)]));
                }
            }
        }
        // The primaries this admin is, each at the revision its record holds.
        let mut held = Vec::new();
        if moving.moves_mesh_primary() {
            let Some(h) = primary_view.mesh(self.topology_view.mesh_name()).filter(|h| h.is_birth(&self.node_id, &self.incarnation)) else {
                let reason = format!("path {me_name_path:?}, name {me_name_name:?} is not the mesh-primary of {}: its view names {}", self.topology_view.mesh_name(), primary_view.mesh(self.topology_view.mesh_name()).map(|h| h.to_string()).unwrap_or_else(|| "no primary".into()), me_name_path = &me_name.path, me_name_name = &me_name.name);
                refused("primary", "not-primary", &reason, operation);
                return Err(rdm_core::reply::handover::bounded_not_primary(&reason));
            };
            held.push(("node-admin".to_string(), h.revision));
        }
        if moving.moves_fabric_primary() {
            let Some(h) = primary_view.fabric().filter(|h| h.is_birth(&self.node_id, &self.incarnation)) else {
                let reason = format!("path {me_name_path:?}, name {me_name_name:?} is not the fabric-primary: its view names {}", primary_view.fabric().map(|h| h.to_string()).unwrap_or_else(|| "no primary".into()), me_name_path = &me_name.path, me_name_name = &me_name.name);
                refused("primary", "not-primary", &reason, operation);
                return Err(rdm_core::reply::handover::bounded_not_primary(&reason));
            };
            held.push((FABRIC.to_string(), h.revision));
        }
        // Step 2: the election over this admin's own view, itself excluded.
        let view = self.topology.read().await.clone();
        let nodes: Vec<crate::model::Node> = view.births().cloned().collect();
        let Some(me_node) = nodes.iter().find(|n| n.node_id == self.node_id).cloned() else {
            let reason = format!("path {me_name_path:?}, name {me_name_name:?} is not a member of its own view", me_name_path = &me_name.path, me_name_name = &me_name.name);
            refused("primary", "not-primary", &reason, operation);
            return Err(rdm_core::reply::handover::bounded_not_primary(&reason));
        };
        let chosen = match named {
            Some(name) => crate::election::named_fabric_successor(&nodes, &self.incumbency(), &me_node, name),
            None => crate::election::commanded_successor_in(&view, &self.incumbency(), &me_node, moving),
        };
        let successor = match chosen.map(|n| (birth_of(&n), n)) {
            Ok((Some(to), n)) => (to, n),
            Ok((None, n)) => {
                let text = format!("path {me_name_path:?}, name {me_name_name:?}'s view holds {} with no birth", n.node_id, me_name_path = &me_name.path, me_name_name = &me_name.name);
                refused("primary", "no-successor", &text, operation);
                return Err(rdm_core::reply::handover::bounded_failed(STEP, &text, rdm_core::outcome::Code::FailedPrecondition));
            }
            Err(text) => {
                refused("primary", "no-successor", &text, operation);
                return Err(rdm_core::reply::handover::bounded_failed(STEP, &text, rdm_core::outcome::Code::FailedPrecondition));
            }
        };
        let (to, to_node) = successor;
        tracing::Span::current().record("successor", to_node.node_id.as_str());
        let entries: Vec<Entry> = held.into_iter().map(|(t, r)| (t, exact(&me), exact(&to), r + 1)).collect();
        // Step 3: the primary finishes the writes it has issued; from the successor's answer it writes the
        // mesh's rows no more.
        (self.finish_writes)().await;
        tracing::info_span!("rdm.node_admin.primary.update.via-issued-writes-finished", node_path = %me_name.path, node_name = %me_name.name, fabric = %self.fabric, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, build_id, primary = %births(&me), successor = %births(&to))
            .in_scope(|| tracing::info!("the primary finished the writes it issued before its call"));
        // Step 4: the call on the successor's exact birth. Answered, this admin's own roles left it: it
        // installs the election's result it sent, of the successor's mesh its own election chose.
        let node_id = self.call_and_look(&to_node, &entries, build_id, operation).await?;
        self.install(&entries, &to.mesh).await;
        Ok(node_id)
    }

    /// The call carrying `entries` on `successor`, its answer, or, when it does not answer, node
    /// investigation on the successor's exact birth: reached, the call is made again; any other verdict
    /// is `Failed`, `the successor is unheard after the update: <verdict>`.
    async fn call_and_look(&self, successor: &crate::model::Node, entries: &[Entry], build_id: &str, operation: &rdm_core::OperationId) -> Result<NodeId, Reply> {
        let how = match call_successor(&self.client, &successor.node_id, entries, build_id, operation).await {
            Ok(node_id) => return Ok(node_id),
            Err(how) => how,
        };
        let view = self.topology.read().await.clone();
        let finding = self.looker.look(&view, successor).await;
        let verdict = finding.name();
        let span = tracing::info_span!("rdm.node_admin.primary.resolve.via-successor-investigation", node_path = %self.held_name().path, node_name = %self.held_name().name, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, successor_path = %successor.path, successor_name = %successor.name, call = %how, verdict, detail = %finding.detail(), retry = tracing::field::Empty);
        if matches!(finding, crate::primary_watch::Finding::Reached { .. }) {
            match call_successor(&self.client, &successor.node_id, entries, build_id, operation).await {
                Ok(node_id) => {
                    span.record("retry", "primary-updated");
                    return Ok(node_id);
                }
                Err(again) => {
                    span.record("retry", again.as_str());
                }
            }
        }
        span.in_scope(|| tracing::info!("the successor did not answer node.primary.updated: node investigation decides"));
        let reason = match finding {
            crate::primary_watch::Finding::Reached { .. } => format!("the successor did not answer node.primary.updated: {} ({}), reached by node investigation; the call {how}", successor.node_id, successor.node_id),
            _ => format!("the successor is unheard after the update: {} ({}) {verdict}; the call {how}", successor.node_id, successor.node_id),
        };
        refused("primary", "successor-unheard", &reason, operation);
        Err(rdm_core::reply::handover::bounded_failed(STEP, &reason, rdm_core::outcome::Code::Unavailable))
    }

    // ---- fabric recovery's restore (fabric-recovery.md step 4, Q122) ----

    /// Fabric recovery's restore of `to`, the node-admin Ready at the lost fabric-primary's
    /// path and name, as mesh-primary and fabric-primary: node.primary.update's call on it carrying both
    /// records at their next revisions, from `target` (the fabric record) and `mesh` (its mesh's record),
    /// installed here on its answer under `to`'s mesh. The executor takes neither role: the node installs
    /// both, acknowledges, and records them on its own row as the mesh's writer.
    pub async fn restore(&self, target: &rdm_core::Primary, mesh: Option<&rdm_core::Primary>, to: &crate::model::Node) -> Result<Vec<Entry>, Reply> {
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-fabric-recovery", node_path = %me.path, node_name = %me.name, target = %target, to_path = %to.path, to_name = %to.name, to_node_id = %to.node_id, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let Some(incarnation) = to.incarnation_id.clone() else {
            return Err(rdm_core::reply::handover::not_ready(format!("{} holds no incarnation: no birth to restore", to.node_id)));
        };
        let to_birth = Birth { node_id: to.node_id.clone(), incarnation };
        let from = |h: &rdm_core::Primary| Birth { node_id: h.node_id.clone(), incarnation: h.incarnation.clone() };
        let mesh_entry = match mesh {
            Some(m) => (from(m), m.revision + 1),
            None => (from(target), 1),
        };
        let entries = vec![
            ("node-admin".to_string(), mesh_entry.0, to_birth.clone(), mesh_entry.1),
            (FABRIC.to_string(), from(target), to_birth, target.revision + 1),
        ];
        let operation = update_operation(PrimaryRole::FabricPrimary, &rdm_core::OperationId::new("node.create", rdm_core::OperationSubject::Birth { node_id: to.node_id.clone(), incarnation: to.incarnation_id.clone().expect("validated above") }));
        let r: Result<Vec<Entry>, Reply> = async {
            // The node restoring itself (the admin a person launched at the path and name) installs the records
            // as node.primary.update's receiver does, with no call.
            if to.node_id == self.node_id {
                self.install(&entries, &to.mesh).await;
                self.memo.acknowledge("fabric-recovery", &operation, true);
                return Ok(entries.clone());
            }
            self.call_and_look(to, &entries, "fabric-recovery", &operation).await?;
            self.install(&entries, &to.mesh).await;
            Ok(entries.clone())
        }
        .instrument(span.clone())
        .await;
        match &r {
            Ok(revisions) => span.record("outcome", "restored").record("detail", entries_named(revisions).as_str()),
            Err(other) => span.record("outcome", other.name()).record("detail", format!("{other:?}").as_str()),
        };
        span.in_scope(|| tracing::info!("fabric recovery restored the node at the fabric-primary's path and name as mesh-primary and fabric-primary"));
        r
    }

    /// Under an in-force fabric.delete (fabric-recovery.md step 4, Q160), the executor `to` of another mesh
    /// than the lost fabric-primary `target`'s holds, at the revisions of the recorded transition, the
    /// fabric-primary role (`target`'s revision plus one) and the lost mesh's mesh-primary role at
    /// `mesh_revision`, keeping its own mesh's: installed on its own node with no call, all or none.
    pub async fn take_under_delete(&self, target: &rdm_core::Primary, mesh_revision: u64, to: &crate::model::Node) -> Result<Vec<Entry>, Reply> {
        let Some(incarnation) = to.incarnation_id.clone() else {
            return Err(rdm_core::reply::handover::not_ready(format!("{} holds no incarnation: no birth takes the roles", to.node_id)));
        };
        let held = |mesh: &str, revision: u64| rdm_core::Primary { mesh: mesh.to_string(), node_id: to.node_id.clone(), incarnation: incarnation.clone(), revision };
        let records = vec![(rdm_core::PrimaryRole::FabricPrimary, held(&to.mesh, target.revision + 1)), (rdm_core::PrimaryRole::of_type("node-admin"), held(&target.mesh, mesh_revision))];
        let taken = {
            let _decisions = self.decisions.clone().lock_owned().await;
            self.topology_view.primaries().take_together(&records)
        };
        (self.project_view)().await;
        if let Some(rdm_transport::topology::RoleTaken::Refused { held }) = taken.iter().find(|t| matches!(t, rdm_transport::topology::RoleTaken::Refused { .. })) {
            return Err(rdm_core::reply::handover::not_ready(format!("{}: a record held here is newer than the transition's: {held:?}", to.node_id)));
        }
        let to_birth = Birth { node_id: to.node_id.clone(), incarnation };
        let lost = Birth { node_id: target.node_id.clone(), incarnation: target.incarnation.clone() };
        Ok(vec![(FABRIC.to_string(), lost.clone(), to_birth.clone(), target.revision + 1), ("node-admin".to_string(), lost, to_birth, mesh_revision)])
    }

    // ---- the commander ----

    /// The fabric-primary commands the primary `primary` before the Build's stop, delete or
    /// node.connection.delete of it runs: a mesh-primary directly, another node type's primary through
    /// its mesh-primary (commands go down). The answer is the primary's node.primary.updating, then its
    /// node.primary.updated naming the new primary's node id. The answer, a mesh-primary's or the one a
    /// mesh-primary streams back for another node type's primary, this admin records in its atlas (Q355):
    /// `is_primary` on that node id, removed from `primary`, read wherever it decides which node runs a
    /// node command, never waiting on gossip; every other node learns it from gossip. What the update
    /// preceded goes to the node's mesh-primary as it stands now, read from the atlas. A `Failed` answer fails the
    /// step on the variant, never on its text; a command that does not answer leaves the step
    /// incomplete, and the next attempt sends the identical command.
    pub async fn command_update_primary(&self, primary: &crate::model::Node, build_id: &str, operation: &rdm_core::OperationId) -> Result<NodeId, crate::build_drive::RoleMoveEnd> {
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-command", node_path = %me.path, node_name = %me.name, primary_path = %primary.path, primary_name = %primary.name, primary_node_id = %primary.node_id, "type" = primary.r#type, to = tracing::field::Empty, build_id, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let r = self.command_update_primary_in(primary, build_id, operation).instrument(span.clone()).await;
        match &r {
            Ok(node_id) => {
                self.memo.acknowledge(build_id, operation, *node_id == self.node_id);
                // The answer streamed back is recorded in this admin's atlas (Q355), a node type's primary's
                // included: its next command goes to its mesh-primary, which the update did not move.
                self.record_answer(primary, node_id);
                span.record("outcome", "updated").record("detail", format!("node_id={node_id}").as_str())
            }
            Err(e) => span.record("outcome", "ended").record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("the commander's node.primary.update ended: the answer streamed back is recorded in its atlas"));
        r
    }

    async fn command_update_primary_in(&self, primary: &crate::model::Node, build_id: &str, operation: &rdm_core::OperationId) -> Result<NodeId, crate::build_drive::RoleMoveEnd> {
        use crate::build_drive::RoleMoveEnd;
        let me = self.held_name();
        let Some(hb) = birth_of(primary) else { return Err(RoleMoveEnd::Refused(format!("{} has no known birth: it cannot be commanded", primary.node_id))) };
        // The mesh-primary of a non-admin primary is its commander, read from this admin's atlas (Q355);
        // this admin is when it is that mesh-primary.
        let to = if rdm_core::is_node_admin(&primary.r#type) {
            primary.clone()
        } else {
            let mut view = self.topology.read().await.clone();
            self.atlas.primaries(&mut view);
            let Some(mp) = view.node_type_primary(&primary.mesh, "node-admin").cloned() else {
                return Err(RoleMoveEnd::Refused(format!("path {me_path:?}, name {me_name:?}'s view names no mesh-primary of {}: the {} primary {} is commanded through its mesh-primary", primary.mesh, primary.r#type, primary.node_id, me_path = &me.path, me_name = &me.name)));
            };
            if mp.node_id == self.node_id {
                return match self.update_node_type_primary(primary, None, build_id, operation).await {
                    Ok(node_id) => Ok(node_id),
                    Err(Reply::Failed { step, reason, code }) => Err(RoleMoveEnd::Failed { step, reason, code }),
                    Err(refusal) => Err(RoleMoveEnd::Refused(format!("path {me_path:?}, name {me_name:?}: node.primary.update of {} was refused: {} ({refusal:?})", primary.node_id, refusal.name(), me_path = &me.path, me_name = &me.name))),
                };
            }
            mp
        };
        tracing::Span::current().record("to", to.node_id.as_str());
        // A primary the election has superseded answers nothing: what the update came to is discovered,
        // and the successor's node.primary.updated is obtained.
        if self.incumbency().lost.contains(&hb.incarnation) {
            return match self.discovered(&hb, &primary.r#type, build_id, operation).await {
                Ok(Some(node_id)) => Ok(node_id),
                Ok(None) => Err(RoleMoveEnd::Incomplete(format!("path {me_path:?}, name {me_name:?}: {} is gone and its view names it the {} primary still: no update happened, and the ordinary election names its successor", primary.node_id, primary.r#type, me_path = &me.path, me_name = &me.name))),
                Err(why) => Err(RoleMoveEnd::Incomplete(why)),
            };
        }
        let req = Request::UpdatePrimary { r#type: primary.r#type.to_string(), build_id: build_id.to_string(), operation: operation.clone(), successor: None };
        match call_update(&self.client, &to.node_id, &req, &CallOptions { send: rdm_transport::Send::with_time_limit(command_limit()), ..CallOptions::default() }).await {
            Ok(Reply::PrimaryUpdated { node_id }) => Ok(node_id),
            Ok(Reply::Failed { step, reason, code }) => Err(RoleMoveEnd::Failed { step, reason, code }),
            Ok(refusal) => Err(RoleMoveEnd::Refused(format!("{} refused node.primary.update of {}: {} ({refusal:?})", to.node_id, primary.node_id, refusal.name()))),
            Err(why) => Err(RoleMoveEnd::Incomplete(format!("path {me_path:?}, name {me_name:?}: node.primary.update {operation:?} of {} is uncertain and the step is incomplete; a later attempt sends the identical command: {why}", primary.node_id, me_path = &me.path, me_name = &me.name))),
        }
    }

    /// What an update of the primary `primary` (of `kind`, in its mesh) came to, discovered from this
    /// admin's view: when the record of that type's primary names another birth now, the update to it
    /// happened, and that successor's node.primary.updated is obtained on the identical call; this
    /// admin installs nothing ("Discovery is not installation"). `None`: the view names `primary` still,
    /// or `primary` was never that type's primary in this view, so no update moved it.
    async fn discovered(&self, primary: &HandoverBirth, r#type: &str, build_id: &str, operation: &rdm_core::OperationId) -> Result<Option<NodeId>, String> {
        let primary_view = self.topology_view.primaries();
        // The record the operation moves: the fabric-primary's for node.fabric-primary.update, its type's
        // otherwise.
        let fabric_update = matches!(&operation.subject, Some(rdm_core::OperationSubject::PrimaryUpdate { role: PrimaryRole::FabricPrimary, .. }));
        let (role, record) = if fabric_update { (PrimaryRole::FabricPrimary, primary_view.fabric()) } else { (PrimaryRole::of_type(r#type), primary_view.node_type_primary(&primary.mesh, r#type)) };
        let Some(record) = record.filter(|r| !r.is_birth(&primary.node_id, &primary.incarnation)) else { return Ok(None) };
        if !primary_view.replaced(role, &primary.mesh, &primary.incarnation) {
            return Ok(None);
        }
        let successor = Birth { node_id: record.node_id.clone(), incarnation: record.incarnation.clone() };
        let entry_type = if fabric_update { FABRIC.to_string() } else { r#type.to_string() };
        let mut entries = vec![(entry_type, exact(primary), successor.clone(), record.revision)];
        // A same-mesh successor became both: the fabric-primary moved from `primary` with the mesh-primary.
        if let Some(f) = primary_view.fabric().filter(|f| f.is_birth(&record.node_id, &record.incarnation)) {
            if !fabric_update && rdm_core::is_node_admin(r#type) && primary_view.replaced(PrimaryRole::FabricPrimary, &primary.mesh, &primary.incarnation) {
                entries.push((FABRIC.to_string(), exact(primary), successor.clone(), f.revision));
            }
        }
        if successor.node_id == self.node_id {
            return Ok(Some(successor.node_id));
        }
        let node_id = call_successor(&self.client, &successor.node_id, &entries, build_id, operation).await.map_err(|how| format!("{}: the successor {successor} of {operation:?} did not answer node.primary.updated: {how}", self.node_id))?;
        Ok(Some(node_id))
    }

    /// node.primary.updated for the update `operation` that preceded an operation on `named` in `mesh`,
    /// obtained by the executor that resumes a Build (node-primary-update.md: "The workflow continues only
    /// after the result is installed and acknowledged: node.primary.updated answered to the resuming
    /// executor's call, never a read alone"). An update this admin holds acknowledged is acknowledged
    /// again with nothing installed. Otherwise the update is discovered from this admin's view
    /// (`Self::discovered`). `false`: the view names the primary still, or holds no node at the name
    /// the update names (no primary is there, so no update is owed): no update happened.
    pub async fn row_acknowledged(&self, mesh: &str, operation: &rdm_core::OperationId, named: Option<&rdm_core::OperationSubject>, build_id: &str) -> Result<bool, String> {
        if self.memo.acknowledged(build_id, operation).is_some() {
            return Ok(true);
        }
        let me = self.held_name();
        let view = self.topology.read().await.clone();
        let Some(node) = named.and_then(|subject| view.node_subject(subject)).cloned() else {
            tracing::info_span!("rdm.node_admin.primary.update.via-acknowledged", node_path = %me.path, node_name = %me.name, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, build_id, primary_subject = ?named, outcome = "no-node")
                .in_scope(|| tracing::info!("the view holds no node at the name the update names: no primary there, no update owed"));
            return Ok(false);
        };
        let Some(hb) = birth_of(&node) else { return Err(format!("path {me_path:?}, name {me_name:?}: {} has no known birth", node.node_id, me_path = &me.path, me_name = &me.name)) };
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-acknowledged", node_path = %me.path, node_name = %me.name, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, build_id, primary = %births(&hb), outcome = tracing::field::Empty);
        match self.discovered(&hb, &node.r#type, build_id, operation).instrument(span.clone()).await? {
            Some(_) => {
                self.memo.acknowledge(build_id, operation, false);
                span.record("outcome", "acknowledged");
                Ok(true)
            }
            None => {
                span.record("outcome", "no-update");
                Ok(false)
            }
        }
    }

    /// The gate of a mesh.delete's leave on its owner, the fabric-primary (mesh-delete.md step 1): the
    /// fabric-primary is no node-admin of the departing mesh in this admin's view before anything is
    /// dismantled. A view that names one of the departing mesh fabric-primary still refuses, named.
    pub async fn fabric_primary_updated(&self, departing: &str, operation: &rdm_core::OperationId) -> Result<(), String> {
        let me = self.held_name();
        match self.topology_view.primaries().fabric() {
            Some(h) if h.mesh == departing => Err(format!("path {me_path:?}, name {me_name:?}: the fabric-primary is {h}, a node-admin of the departing mesh {departing}: node.fabric-primary.update {operation:?} has not moved it", me_path = &me.path, me_name = &me.name)),
            held => {
                tracing::info_span!("rdm.node_admin.primary.update.via-fabric-primary-left", node_path = %me.path, node_name = %me.name, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, departing, fabric_primary = %held.map(|h| h.to_string()).unwrap_or_default())
                    .in_scope(|| tracing::info!("the fabric-primary is no node-admin of the departing mesh"));
                Ok(())
            }
        }
    }

    /// A request off the wire, decided on this admin's own view, whoever calls (node-rpc.md §14), on its
    /// stream `sink`.
    pub async fn serve(self: &Arc<Self>, req: Request, sink: ReplySink<NodePrimaryUpdate, NotStarted>) -> Reply {
        match req {
            Request::PrimaryUpdate { primaries, build_id, operation } => self.serve_primary_update(primaries, build_id, operation, sink).await,
            Request::UpdatePrimary { r#type, build_id, operation, .. } => self.serve_update_primary(r#type, build_id, operation, sink).await,
        }
    }

    // ---- the mesh-primary commanding another node type's primary of its mesh ----

    /// node.primary.update of `primary`, the primary of a node type other than node-admin, at this admin
    /// as its mesh's mesh-primary, the commander and the writer: its pre-check (step 2) from its own view
    /// names a Ready successor, none is `Failed` by name; then the command to the primary, which
    /// calls the successor it names, answered with node.primary.updated naming the successor's node id,
    /// which this admin records in its atlas (Q355): `is_primary` on that node id, removed from
    /// `primary`, read wherever it decides which node runs a node command (a section's plan included),
    /// never waiting on gossip. It records the successor's row from the successor's digest (step 5), and
    /// it runs the operation the update preceded itself.
    pub async fn update_node_type_primary(&self, primary: &crate::model::Node, _expected: Option<u64>, build_id: &str, operation: &rdm_core::OperationId) -> Result<NodeId, Reply> {
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-command-member", node_path = %me.path, node_name = %me.name, primary_path = %primary.path, primary_name = %primary.name, "type" = primary.r#type, build_id, operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let r = self.update_node_type_primary_in(primary, build_id, operation).instrument(span.clone()).await;
        match &r {
            Ok(node_id) => {
                self.memo.acknowledge(build_id, operation, false);
                self.record_answer(primary, node_id);
                span.record("outcome", "updated").record("detail", format!("node_id={node_id}").as_str())
            }
            Err(other) => span.record("outcome", other.name()).record("detail", format!("{other:?}").as_str()),
        };
        span.in_scope(|| tracing::info!("the mesh-primary's command of a node type's primary ended: the answer is recorded in its atlas"));
        r
    }

    async fn update_node_type_primary_in(&self, primary: &crate::model::Node, build_id: &str, operation: &rdm_core::OperationId) -> Result<NodeId, Reply> {
        let me = self.held_name();
        let mesh = primary.mesh.clone();
        let primary_view = self.topology_view.primaries();
        if self.topology_view.mesh_name() != mesh || !primary_view.mesh(&mesh).is_some_and(|h| h.is_birth(&self.node_id, &self.incarnation)) {
            let reason = format!("path {me_path:?}, name {me_name:?} is not the mesh-primary of {mesh}, the commander of its {} primary", primary.r#type, me_path = &me.path, me_name = &me.name);
            refused("commander", "not-primary", &reason, operation);
            return Err(rdm_core::reply::handover::bounded_not_primary(&reason));
        }
        // Step 2: the writer's pre-check, from its own view, names the successor the command carries: the
        // primary calls it and runs no election of its own (Q129).
        let view = self.topology.read().await.clone();
        let nodes: Vec<crate::model::Node> = view.births().cloned().collect();
        let successor = match crate::election::commanded_node_type_successor(&nodes, &self.incumbency(), primary) {
            Ok(n) => n,
            Err(text) => {
                refused("commander", "no-successor", &text, operation);
                return Err(rdm_core::reply::handover::bounded_failed(STEP, &text, rdm_core::outcome::Code::FailedPrecondition));
            }
        };
        let Some(named) = birth_of(&successor) else {
            let text = format!("path {me_path:?}, name {me_name:?}: the successor {} of {operation:?} has no known birth", successor.node_id, me_path = &me.path, me_name = &me.name);
            refused("commander", "no-successor", &text, operation);
            return Err(rdm_core::reply::handover::bounded_failed(STEP, &text, rdm_core::outcome::Code::FailedPrecondition));
        };
        let req = Request::UpdatePrimary { r#type: primary.r#type.to_string(), build_id: build_id.to_string(), operation: operation.clone(), successor: Some(exact(&named)) };
        match call_update(&self.client, &primary.node_id, &req, &CallOptions { send: rdm_transport::Send::with_time_limit(command_limit()), ..CallOptions::default() }).await {
            Ok(Reply::PrimaryUpdated { node_id }) => Ok(node_id),
            Ok(refusal) => Err(refusal),
            Err(why) => {
                let reason = format!("the command to the {} primary {} did not answer: {why}", primary.r#type, primary.node_id);
                refused("commander", "unanswered", &reason, operation);
                Err(rdm_core::reply::handover::bounded_failed(STEP, &reason, rdm_core::outcome::Code::Unavailable))
            }
        }
    }
}

// ---- a node of a type that is not node-admin (re-exported from rdm-node) ----

pub use rdm_node::member_handover::{serve_node_primary_update, NodePrimaryUpdateService, NodePrimaryUpdateSlot};

/// Where a running admin sets its [`PrimaryUpdate`] once it holds a view.
pub type PrimaryUpdateSlot = Arc<OnceLock<Arc<PrimaryUpdate>>>;

/// Serve node.primary.update on this admin, server-streaming. Until `slot` is filled a call is
/// `NotReady`.
pub fn serve(b: ServerBuilder, slot: PrimaryUpdateSlot) -> ServerBuilder {
    b.serve_stream::<NodePrimaryUpdate, _, _>(OpOwner::Product("rdm".into()), move |_peer: PeerContext, req: Request, sink| {
        let slot = slot.clone();
        async move {
            let Some(primary_update) = slot.get().cloned() else {
                return Ok(rdm_core::reply::handover::not_ready("this admin holds no view yet".into()));
            };
            Ok(primary_update.serve(req, sink).await)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::process::ProcessDeploymentProvider;
    use crate::model::{Fabric, FabricId, MeshId, Node, ProviderKind, ScopeStatus};
    use rdm_core::Primary;
    use rdm_transport::StaticResolver;
    use std::net::SocketAddr;

    /// CONTRACT: retry acknowledgements are scoped by the explicit Build, submission run,
    /// role and preceding operation, never a parsed operation string.
    // @feature: node-primary-update
    #[test]
    fn acknowledged_update_keeps_build_and_preceding_operation_separate() {
        let memo = UpdateMemo::default();
        let stop = rdm_core::OperationId::node("node.stop", &NodeId::mint()).at_step(Some(1));
        let update = update_operation(PrimaryRole::MeshPrimary, &stop);
        memo.acknowledge("build-a", &update, true);
        assert_eq!(memo.acknowledged("build-a", &update), Some(true));
        assert_eq!(memo.acknowledged("build-b", &update), None);
        assert_eq!(memo.acknowledged("build-a", &update_operation(PrimaryRole::FabricPrimary, &stop)), None);
        let mut delete = stop.clone();
        delete.action = "node.delete".into();
        assert_eq!(memo.acknowledged("build-a", &update_operation(PrimaryRole::MeshPrimary, &delete)), None);
    }

    /// CONTRACT: docs/architecture/lifecycles/fabric-recovery.md:87
    /// "The executor takes the fabric-primary role and the lost mesh's mesh-primary role by one winning claim and one recorded transition at the next revision, keeping its own mesh's"
    #[tokio::test]
    async fn take_under_delete_installs_fabric_and_lost_mesh_primary_roles_all_or_none() {
        let ep = rdm_transport::endpoint::bind(
            iroh::SecretKey::from_bytes(&[23u8; 32]),
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        )
        .await
        .unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(5_000_000);
        let fabric_id = FabricId::mint();
        let mesh_id = MeshId::mint();
        let me: PathName = rdm_core::PathName::new("mesh2.node-admin", "1").unwrap();
        let node_id = NodeId::mint();
        let incarnation = IncarnationId::mint();

        let topology_view = TopologyView::join(
            &gossip,
            &ep,
            &fabric_id,
            "mesh2",
            &mesh_id,
            "mesh2.node-admin.1",
            Arc::new(time),
            vec![],
        )
        .await
        .unwrap();
        let backbone = Backbone::join(
            &gossip,
            &ep,
            &topology_view,
            "mesh2",
            rdm_core::PublisherId {
                incarnation: incarnation.clone(),
                node_id: node_id.clone(),
            },
            vec![],
        )
        .await
        .unwrap();
        let resolver = Arc::new(StaticResolver::new());
        let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver));
        let provider = Arc::new(ProcessDeploymentProvider::new());
        let looker = crate::primary_watch::Looker {
            me_path: me.path.clone(), me_name: me.name.clone(),
            me_id: node_id.clone(),
            topology_view: topology_view.clone(),
            client: client.clone(),
            provider,
            nodes: None,
            heard_again: Default::default(),
            connections: None,
        };
        let primary_update = PrimaryUpdate {
            fabric: "fab".into(),
            me_path: me.path.clone(), me_name: me.name.clone(),
            node_id: node_id.clone(),
            incarnation: incarnation.clone(),
            topology: Arc::new(RwLock::new(Topology {
                fabric: Fabric {
                    id: FabricId::mint(),
                    name: "fab".into(),
                    status: ScopeStatus::Ready,
                    provider: ProviderKind::Process,
                },
                meshes: Vec::new(),
                nodes: Vec::new(),
            })),
            digest_view: topology_view.digest_view.clone(),
            topology_view: topology_view.clone(),
            backbone,
            client,
            records: Arc::new(crate::admin::Records::default()),
            locks: Default::default(),
            memo: Default::default(),
            decisions: Arc::default(),
            project_view: Arc::new(|| Box::pin(async {})),
            finish_writes: Arc::new(|| Box::pin(async {})),
            looker,
            atlas: Arc::default(),
        };

        let lost_fp = Primary {
            mesh: "mesh1".into(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            revision: 3,
        };

        // 1. A target node with no incarnation is refused `NotReady`.
        let mut to_node = Node::allocated(me.clone(), "mesh2", "node-admin");
        to_node.node_id = node_id.clone();
        assert!(matches!(
            primary_update.take_under_delete(&lost_fp, 2, &to_node).await,
            Err(Reply::NotReady { .. })
        ));

        // 2. With a valid incarnation, `take_under_delete` installs both `FabricPrimary` (at `target.revision + 1`)
        //    and `mesh1`'s `NodeAdmin` primary (at `mesh_revision`) onto `to_node`.
        to_node.incarnation_id = Some(incarnation.clone());
        let entries = primary_update.take_under_delete(&lost_fp, 2, &to_node).await.expect("take_under_delete succeeds");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, FABRIC);
        assert_eq!(entries[0].3, 4);
        assert_eq!(entries[1].0, "node-admin");
        assert_eq!(entries[1].3, 2);

        let held_fp = topology_view.primaries().fabric().expect("fabric primary installed");
        assert_eq!(held_fp.node_id, node_id);
        assert_eq!(held_fp.revision, 4);

        // 3. If a newer revision is already held, `take_under_delete` refuses `NotReady` all-or-none.
        assert!(matches!(
            primary_update.take_under_delete(&lost_fp, 1, &to_node).await,
            Err(Reply::NotReady { .. })
        ));
        ep.close().await;
    }
}

