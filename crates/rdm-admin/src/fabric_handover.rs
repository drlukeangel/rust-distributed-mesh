//! node.primary.update and node.fabric-primary.update on op `0x21` at a node-admin
//! (lifecycles/node-primary-update.md; node-rpc-envelope.md "PrimaryUpdate, op `0x21`, rdm").
//!
//! ```text
//! fabric-primary (commander)     NA1 (mesh-primary)                          NA2 (successor)
//!   UpdatePrimary --------------> 2 the election over its own view, NA1 excluded,
//!                                   names the successor; none -> Failed by name
//!                                 3 finish the writes it has issued (Q109)
//!                                 4 PrimaryUpdate (the election's result) ---> installs it in its view
//!                                   <--------------------------------------- PrimaryUpdated
//!   <---------------------------- PrimaryUpdated                              5 its digest carries it;
//!   the Build continues under NA2                                              it records its own row
//! ```
//!
//! The same primitive serves every node type (`rdm_node::member_handover`): the fabric-primary commands
//! a mesh-primary, a mesh-primary commands the primaries of its mesh's other node types. Who is primary
//! is the election's result; the write records the role and does not grant it, and the record follows
//! the role through the mesh rows writer (`crate::admin::rows_writer::keep_mesh_rows`). No row and no
//! frame of its own record the change.

pub use crate::election::Moving;
use crate::model::{IncarnationId, NodeId, PathName};
use crate::topology::Topology;
use rdm_core::framing::Birth;
use rdm_core::handover::{HandoverBirth, NodePrimaryUpdate, NodePrimaryUpdateReply as Reply, NodePrimaryUpdateRequest as Request, FABRIC};
use rdm_core::outcome::RpcOutcome;
use rdm_core::PrimaryRole;
use rdm_core::catalog::OpOwner;
use rdm_node::member_handover::{call_successor, entries_named, install, Answered, Entry, STEP};
use rdm_transport::topology::{Backbone, DigestView, TopologyView};
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use tracing::Instrument as _;

/// The operation identity of the primary update that precedes the Build operation `op_key` of Build
/// `build_id` (R-MT1): node.primary.update for a mesh-primary or another node type's primary (and the
/// fabric-primary with the mesh-primary when one node-admin is both), node.fabric-primary.update for the
/// fabric-primary alone. Keyed `(build_id, run)` (`node.primary.update:<build_id>:<run>:<op>`), never the
/// Build id alone (ruling R-PU2, Q9): `run` is the first attempt of the series a submission opened
/// ([`crate::build_state::BuildProjection::run_of`]); a resume continues it, and a resubmit after
/// `Failed` starts a new one, so a new update.
pub fn update_operation(role: PrimaryRole, build_id: &str, run: u32, op_key: &str) -> String {
    match role {
        PrimaryRole::MeshPrimary | PrimaryRole::NodeTypePrimary(_) => format!("node.primary.update:{build_id}:{run}:{op_key}"),
        PrimaryRole::FabricPrimary => format!("node.fabric-primary.update:{build_id}:{run}:{op_key}"),
    }
}

/// The run an operation names (`<kind>:<build_id>:<run>:<op>`), else `attempt`.
fn run_of(operation: &str, attempt: u32) -> u32 {
    operation.split(':').nth(2).and_then(|r| r.parse().ok()).unwrap_or(attempt)
}

/// The commander's bound on reaching the primary and sending it the command (`UpdatePrimary`). The
/// command's reply is the primary's whole update, its own call to the successor included, so nothing
/// but the primary's liveness bounds the reply: the transport's idle timeout and keepalive, as for the
/// Build carrier (node-rpc.md §34).
const COMMAND_SEND: std::time::Duration = std::time::Duration::from_secs(10);

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
    acknowledged: std::sync::Mutex<std::collections::BTreeMap<String, bool>>,
    /// What this admin answered as the successor, by operation.
    answered: Answered,
}

impl UpdateMemo {
    fn acknowledge(&self, operation: &str, successor: bool) {
        self.acknowledged.lock().unwrap().insert(operation.to_string(), successor);
    }

    /// `Some(whether this admin is the successor)` once `operation` is acknowledged here.
    fn acknowledged(&self, operation: &str) -> Option<bool> {
        self.acknowledged.lock().unwrap().get(operation).copied()
    }
}

/// Every side of node.primary.update on one node-admin: the primary's update, the successor's
/// install, the commander's command and its resume.
pub struct HandoverDoor {
    /// The fabric's name.
    pub fabric: String,
    /// This admin's path.
    pub me: PathName,
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
    /// the call that depends on it is answered: every reader of the view (the Build door, the claim
    /// door, the drive, the executor plan, the rows writers) decides from it.
    pub project_view: ProjectView,
    /// Finishes every write this admin issued, before its call (node-primary-update.md step 3).
    pub finish_writes: FinishWrites,
    /// node investigation on one exact birth (node-investigation.md), run on a successor that did not
    /// answer the call.
    pub looker: crate::primary_watch::Looker,
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
fn refused(side: &'static str, reason: &'static str, detail: &str, operation: &str) {
    tracing::info_span!("rdm.node_admin.primary.reject.via-primary-update", side, reason, detail, operation)
        .in_scope(|| tracing::info!("node.primary.update was refused"));
}

impl HandoverDoor {
    fn held_name(&self) -> PathName {
        self.digest_view.held_path_name().unwrap_or_else(|| self.me.clone())
    }

    fn incumbency(&self) -> crate::election::Incumbency {
        crate::admin::incumbency_of(&self.digest_view, &self.records)
    }

    fn me_birth(&self) -> HandoverBirth {
        HandoverBirth { mesh: self.me.mesh.clone(), node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }
    }

    /// The mesh this admin's view gives the birth `b`, else this admin's own.
    async fn mesh_of(&self) -> impl Fn(&Birth) -> String {
        let view = self.topology.read().await.clone();
        let own = self.me.mesh.clone();
        move |b: &Birth| view.births().find(|n| n.node_id == b.node_id).map(|n| n.mesh.clone()).unwrap_or_else(|| own.clone())
    }

    /// Install the entries an answer named, under the decision lock, and project the view from them.
    async fn take(&self, entries: &[Entry]) -> &'static str {
        let mesh_of = self.mesh_of().await;
        let outcome = {
            let _decisions = self.decisions.clone().lock_owned().await;
            install(self.topology_view.primaries(), entries, mesh_of)
        };
        (self.project_view)().await;
        outcome
    }

    /// One call to `target`: its reply, or how it ended without one.
    async fn call_once(&self, target: &NodeId, req: &Request, opts: &CallOptions) -> Result<Reply, String> {
        let (out, _) = self.client.call::<NodePrimaryUpdate>(&NodeTarget::ExactNode(target.clone()), req, opts).await;
        match out {
            RpcOutcome::Reply(r) => Ok(r.value().clone()),
            o => Err(format!("the {} call to {target} ended {} ({o:?})", req.op(), o.name())),
        }
    }

    // ---- the successor (step 4) ----

    /// node.primary.update at its receiver: it installs the election's result the call carries in its own
    /// view and answers node.primary.updated; as a node-admin it then records its own row (the mesh rows
    /// writer follows its view). It refuses only a conflicting payload, never the birth the call names
    /// (R-UC1a, R-RX5).
    async fn serve_primary_update(&self, primaries: Vec<Entry>, build_id: String, run: u32, operation: String) -> Reply {
        let answered = &self.memo.answered;
        let _one_at_a_time = self.locks.serving.lock().await;
        let payload = (primaries.clone(), build_id.clone());
        if let Some(r) = answered.before(&operation, &payload) {
            return r;
        }
        // The span covers the install: its start is the moment the successor begins to install.
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-primary-updated", node = %self.held_name(), fabric = %self.fabric, operation = %operation, build_id = %build_id, run, primaries = %entries_named(&primaries), outcome = tracing::field::Empty);
        let outcome = self.take(&primaries).instrument(span.clone()).await;
        span.record("outcome", outcome);
        span.in_scope(|| tracing::info!("the successor installed the election's result the call carries and answers node.primary.updated"));
        if primaries.iter().any(|(_, _, s, _)| s.node_id == self.node_id) {
            self.memo.acknowledge(&operation, true);
        }
        let reply = Reply::PrimaryUpdated { revisions: primaries };
        answered.keep(&operation, payload, reply.clone());
        reply
    }

    // ---- the primary (steps 2-4) ----

    /// node.primary.update commanded from above, on this admin: `node-admin` moves its mesh-primary (and
    /// the fabric-primary with it when it is both), `fabric` the fabric-primary, any other node type is
    /// the primary of that type in this admin's mesh, which this admin, its mesh-primary, commands.
    async fn serve_update_primary(self: &Arc<Self>, node_type: String, build_id: String, run: u32, operation: String) -> Reply {
        let me = self.held_name();
        let primary_view = self.topology_view.primaries();
        let mesh_primary = primary_view.mesh(&self.me.mesh).is_some_and(|h| h.is_birth(&self.node_id, &self.incarnation));
        let fabric_primary = primary_view.fabric().is_some_and(|h| h.is_birth(&self.node_id, &self.incarnation));
        let moving = if node_type == crate::model::NodeKind::NodeAdmin.node_type() {
            if !mesh_primary {
                let reason = format!("{me} is not the mesh-primary of {}: its view names {}", self.me.mesh, primary_view.mesh(&self.me.mesh).map(|h| h.to_string()).unwrap_or_else(|| "no primary".into()));
                refused("primary", "not-primary", &reason, &operation);
                return rdm_core::reply::handover::bounded_not_primary(&reason);
            }
            if fabric_primary { Moving::BothRoles } else { Moving::MeshRole }
        } else if node_type == FABRIC {
            if !fabric_primary {
                let reason = format!("{me} is not the fabric-primary: its view names {}", primary_view.fabric().map(|h| h.to_string()).unwrap_or_else(|| "no primary".into()));
                refused("primary", "not-primary", &reason, &operation);
                return rdm_core::reply::handover::bounded_not_primary(&reason);
            }
            let leaving = operation.contains("mesh.delete:");
            let same_mesh_successor = !leaving && self.topology.read().await.members().any(|n| n.mesh == self.me.mesh && n.kind == crate::model::NodeKind::NodeAdmin && n.node_id != self.node_id && n.status == crate::model::NodeStatus::Ready);
            if mesh_primary && same_mesh_successor { Moving::BothRoles } else { Moving::FabricRoleOutside }
        } else {
            let view = self.topology.read().await.clone();
            let kind = crate::model::NodeKind::ALL.into_iter().find(|k| k.node_type() == node_type);
            let primary = kind.and_then(|k| view.node_type_primary(&self.me.mesh, k).cloned());
            let Some(primary) = primary.filter(|_| mesh_primary) else {
                let reason = format!("{me} is not the mesh-primary of {} naming a {node_type} primary", self.me.mesh);
                refused("commander", "not-primary", &reason, &operation);
                return rdm_core::reply::handover::bounded_not_primary(&reason);
            };
            return match self.update_node_type_primary(&primary, None, &build_id, run, &operation).await {
                Ok(revisions) => Reply::PrimaryUpdated { revisions },
                Err(refusal) => refusal,
            };
        };
        match self.update_primary(moving, None, &build_id, run, &operation, None).await {
            Ok(revisions) => Reply::PrimaryUpdated { revisions },
            Err(refusal) => refusal,
        }
    }

    /// node.primary.update at this admin, the primary of `moving` (steps 2-4): the election over its own
    /// view with itself excluded names the successor (`named` names it for the hand-back); none is
    /// `Failed` by name. It finishes the writes it has issued, calls the successor carrying the election's
    /// result, installs the answer in its own view and answers with it. A successor that does not answer
    /// is looked at by node investigation: reached, the call is made again; any other verdict is `Failed`,
    /// the successor unheard after the update. An identical repeat is answered as the first was.
    pub async fn update_primary(&self, moving: Moving, _expected: Option<u64>, build_id: &str, attempt: u32, operation: &str, named: Option<&PathName>) -> Result<Vec<Entry>, Reply> {
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-update-primary", node = %me, moving = moving.name(), build_id, attempt, operation, successor = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let r = self.update_primary_in(moving, build_id, run_of(operation, attempt), operation, named).instrument(span.clone()).await;
        match &r {
            Ok(_) => {
                self.memo.acknowledge(operation, false);
                span.record("outcome", "updated")
            }
            Err(Reply::Failed { reason, .. }) => span.record("outcome", "failed").record("detail", reason.as_str()),
            Err(other) => span.record("outcome", other.name()).record("detail", format!("{other:?}").as_str()),
        };
        span.in_scope(|| tracing::info!("node.primary.update ran at the primary"));
        r
    }

    /// [`Self::update_primary`] moving this admin's own primaries as a step of its Build (the
    /// fabric-primary has nothing above it), its end told apart as the step needs it.
    pub async fn update_own_primaries(&self, moving: Moving, build_id: &str, attempt: u32, operation: &str, named: Option<&PathName>) -> Result<Vec<Entry>, crate::build_drive::RoleMoveEnd> {
        use crate::build_drive::RoleMoveEnd;
        match self.update_primary(moving, None, build_id, attempt, operation, named).await {
            Ok(revisions) | Err(Reply::AlreadyApplied { revisions }) => Ok(revisions),
            Err(Reply::Failed { step, reason, code }) => Err(RoleMoveEnd::Failed { step, reason, code }),
            Err(other) => Err(RoleMoveEnd::Refused(format!("{}: {other:?}", other.name()))),
        }
    }

    async fn update_primary_in(&self, moving: Moving, build_id: &str, run: u32, operation: &str, named: Option<&PathName>) -> Result<Vec<Entry>, Reply> {
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
            let Some(h) = primary_view.mesh(&self.me.mesh).filter(|h| h.is_birth(&self.node_id, &self.incarnation)) else {
                let reason = format!("{me_name} is not the mesh-primary of {}: its view names {}", self.me.mesh, primary_view.mesh(&self.me.mesh).map(|h| h.to_string()).unwrap_or_else(|| "no primary".into()));
                refused("primary", "not-primary", &reason, operation);
                return Err(rdm_core::reply::handover::bounded_not_primary(&reason));
            };
            held.push((crate::model::NodeKind::NodeAdmin.node_type().to_string(), h.revision));
        }
        if moving.moves_fabric_primary() {
            let Some(h) = primary_view.fabric().filter(|h| h.is_birth(&self.node_id, &self.incarnation)) else {
                let reason = format!("{me_name} is not the fabric-primary: its view names {}", primary_view.fabric().map(|h| h.to_string()).unwrap_or_else(|| "no primary".into()));
                refused("primary", "not-primary", &reason, operation);
                return Err(rdm_core::reply::handover::bounded_not_primary(&reason));
            };
            held.push((FABRIC.to_string(), h.revision));
        }
        // Step 2: the election over this admin's own view, itself excluded.
        let view = self.topology.read().await.clone();
        let nodes: Vec<crate::model::Node> = view.members().cloned().collect();
        let Some(me_node) = nodes.iter().find(|n| n.node_id == self.node_id).cloned() else {
            let reason = format!("{me_name} is not a member of its own view");
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
                let text = format!("{me_name}'s view holds {} with no birth", n.name);
                refused("primary", "no-successor", &text, operation);
                return Err(rdm_core::reply::handover::bounded_failed(STEP, &text, rdm_core::outcome::Code::FailedPrecondition));
            }
            Err(text) => {
                refused("primary", "no-successor", &text, operation);
                return Err(rdm_core::reply::handover::bounded_failed(STEP, &text, rdm_core::outcome::Code::FailedPrecondition));
            }
        };
        let (to, to_node) = successor;
        tracing::Span::current().record("successor", to_node.name.to_string().as_str());
        let entries: Vec<Entry> = held.into_iter().map(|(t, r)| (t, exact(&me), exact(&to), r + 1)).collect();
        // Step 3: the primary finishes the writes it has issued; from the successor's answer it writes the
        // mesh's rows no more.
        (self.finish_writes)().await;
        tracing::info_span!("rdm.node_admin.primary.update.via-issued-writes-finished", node = %me_name, fabric = %self.fabric, operation, build_id, run, primary = %births(&me), successor = %births(&to))
            .in_scope(|| tracing::info!("the primary finished the writes it issued before its call"));
        // Step 4: the call on the successor's exact birth.
        let revisions = self.call_and_look(&to_node, &entries, build_id, run, operation).await?;
        self.take(&revisions).await;
        Ok(revisions)
    }

    /// The call carrying `entries` on `successor`, its answer, or, when it does not answer, node
    /// investigation on the successor's exact birth: reached, the call is made again; any other verdict
    /// is `Failed`, `the successor is unheard after the update: <verdict>`.
    async fn call_and_look(&self, successor: &crate::model::Node, entries: &[Entry], build_id: &str, run: u32, operation: &str) -> Result<Vec<Entry>, Reply> {
        let how = match call_successor(&self.client, &successor.node_id, entries, build_id, run, operation).await {
            Ok(revisions) => return Ok(revisions),
            Err(how) => how,
        };
        let view = self.topology.read().await.clone();
        let finding = self.looker.look(&view, successor).await;
        let verdict = finding.name();
        let span = tracing::info_span!("rdm.node_admin.primary.resolve.via-successor-investigation", node = %self.held_name(), operation, successor = %successor.name, call = %how, verdict, detail = %finding.detail(), retry = tracing::field::Empty);
        if matches!(finding, crate::primary_watch::Finding::Reached { .. }) {
            match call_successor(&self.client, &successor.node_id, entries, build_id, run, operation).await {
                Ok(revisions) => {
                    span.record("retry", "primary-updated");
                    return Ok(revisions);
                }
                Err(again) => {
                    span.record("retry", again.as_str());
                }
            }
        }
        span.in_scope(|| tracing::info!("the successor did not answer node.primary.updated: node investigation decides"));
        let reason = match finding {
            crate::primary_watch::Finding::Reached { .. } => format!("the successor did not answer node.primary.updated: {} ({}), reached by node investigation; the call {how}", successor.name, successor.node_id),
            _ => format!("the successor is unheard after the update: {} ({}) {verdict}; the call {how}", successor.name, successor.node_id),
        };
        refused("primary", "successor-unheard", &reason, operation);
        Err(rdm_core::reply::handover::bounded_failed(STEP, &reason, rdm_core::outcome::Code::Unavailable))
    }

    // ---- fabric recovery's restore (fabric-recovery.md step 4, Q122) ----

    /// Fabric recovery's restore of `to`, the node-admin Ready at the lost fabric-primary's
    /// path.name, as mesh-primary and fabric-primary: node.primary.update's call on it carrying both
    /// records at their next revisions, from `target` (the fabric record) and `mesh` (its mesh's record),
    /// installed here on its answer. The executor takes neither role: the node installs both,
    /// acknowledges, and records them on its own row as the mesh's writer.
    pub async fn restore(&self, target: &rdm_core::Primary, mesh: Option<&rdm_core::Primary>, to: &crate::model::Node) -> Result<Vec<Entry>, Reply> {
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-fabric-recovery", node = %me, target = %target, to = %to.name, to_node_id = %to.node_id, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let Some(incarnation) = to.incarnation_id.clone() else {
            return Err(rdm_core::reply::handover::not_ready(format!("{} holds no incarnation: no birth to restore", to.name)));
        };
        let to_birth = Birth { node_id: to.node_id.clone(), incarnation };
        let from = |h: &rdm_core::Primary| Birth { node_id: h.node_id.clone(), incarnation: h.incarnation.clone() };
        let mesh_entry = match mesh {
            Some(m) => (from(m), m.revision + 1),
            None => (from(target), 1),
        };
        let entries = vec![
            (crate::model::NodeKind::NodeAdmin.node_type().to_string(), mesh_entry.0, to_birth.clone(), mesh_entry.1),
            (FABRIC.to_string(), from(target), to_birth, target.revision + 1),
        ];
        let operation = update_operation(PrimaryRole::FabricPrimary, "fabric-recovery", 0, &format!("node.create:{}", to.name));
        let r: Result<Vec<Entry>, Reply> = async {
            // The node restoring itself (the admin a person launched at the path.name) installs the records
            // as node.primary.update's receiver does, with no call.
            if to.node_id == self.node_id {
                self.take(&entries).await;
                self.memo.acknowledge(&operation, true);
                return Ok(entries.clone());
            }
            let revisions = self.call_and_look(to, &entries, "fabric-recovery", 0, &operation).await?;
            self.take(&revisions).await;
            Ok(revisions)
        }
        .instrument(span.clone())
        .await;
        match &r {
            Ok(revisions) => span.record("outcome", "restored").record("detail", entries_named(revisions).as_str()),
            Err(other) => span.record("outcome", other.name()).record("detail", format!("{other:?}").as_str()),
        };
        span.in_scope(|| tracing::info!("fabric recovery restored the node at the fabric-primary's path.name as mesh-primary and fabric-primary"));
        r
    }

    /// Under an in-force fabric.delete (fabric-recovery.md step 4, Q160), the executor `to` of another mesh
    /// than the lost fabric-primary `target`'s holds, at the revisions of the recorded transition, the
    /// fabric-primary role (`target`'s revision plus one) and the lost mesh's mesh-primary role at
    /// `mesh_revision`, keeping its own mesh's: installed on its own node with no call, all or none.
    pub async fn take_under_delete(&self, target: &rdm_core::Primary, mesh_revision: u64, to: &crate::model::Node) -> Result<Vec<Entry>, Reply> {
        let Some(incarnation) = to.incarnation_id.clone() else {
            return Err(rdm_core::reply::handover::not_ready(format!("{} holds no incarnation: no birth takes the roles", to.name)));
        };
        let held = |mesh: &str, revision: u64| rdm_core::Primary { mesh: mesh.to_string(), node_id: to.node_id.clone(), incarnation: incarnation.clone(), revision };
        let records = vec![(rdm_core::PrimaryRole::FabricPrimary, held(&to.mesh, target.revision + 1)), (rdm_core::PrimaryRole::of_node_type(crate::model::NodeKind::NodeAdmin), held(&target.mesh, mesh_revision))];
        let taken = {
            let _decisions = self.decisions.clone().lock_owned().await;
            self.topology_view.primaries().take_together(&records)
        };
        (self.project_view)().await;
        if let Some(rdm_transport::topology::RoleTaken::Refused { held }) = taken.iter().find(|t| matches!(t, rdm_transport::topology::RoleTaken::Refused { .. })) {
            return Err(rdm_core::reply::handover::not_ready(format!("{}: a record held here is newer than the transition's: {held:?}", to.name)));
        }
        let to_birth = Birth { node_id: to.node_id.clone(), incarnation };
        let lost = Birth { node_id: target.node_id.clone(), incarnation: target.incarnation.clone() };
        Ok(vec![(FABRIC.to_string(), lost.clone(), to_birth.clone(), target.revision + 1), (crate::model::NodeKind::NodeAdmin.node_type().to_string(), lost, to_birth, mesh_revision)])
    }

    // ---- the commander ----

    /// The fabric-primary commands the primary `primary` before the Build's stop, delete, restart, replace
    /// or node.connection.delete of it runs: a mesh-primary directly, another node type's primary through
    /// its mesh-primary (commands go down). The answer is the successor's node.primary.updated, installed
    /// here so this admin's view names the successor before it dispatches what the update preceded. A
    /// `Failed` answer fails the step on the variant, never on its text; a command that does not answer
    /// leaves the step incomplete, and the next attempt sends the identical command.
    pub async fn command_update_primary(&self, primary: &crate::model::Node, build_id: &str, attempt: u32, operation: &str) -> Result<Vec<Entry>, crate::build_drive::RoleMoveEnd> {
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-command", node = %me, primary = %primary.name, primary_node_id = %primary.node_id, node_type = primary.kind.node_type(), to = tracing::field::Empty, build_id, attempt, operation, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let r = self.command_update_primary_in(primary, build_id, run_of(operation, attempt), operation).instrument(span.clone()).await;
        match &r {
            Ok(revisions) => {
                self.memo.acknowledge(operation, revisions.iter().any(|(_, _, s, _)| s.node_id == self.node_id));
                span.record("outcome", "updated").record("detail", entries_named(revisions).as_str())
            }
            Err(e) => span.record("outcome", "ended").record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("the commander's node.primary.update ended"));
        r
    }

    async fn command_update_primary_in(&self, primary: &crate::model::Node, build_id: &str, run: u32, operation: &str) -> Result<Vec<Entry>, crate::build_drive::RoleMoveEnd> {
        use crate::build_drive::RoleMoveEnd;
        let me = self.held_name();
        let Some(hb) = birth_of(primary) else { return Err(RoleMoveEnd::Refused(format!("{} has no known birth: it cannot be commanded", primary.name))) };
        // The mesh-primary of a non-admin primary is its commander; this admin is when it is that
        // mesh-primary.
        let to = if primary.kind == crate::model::NodeKind::NodeAdmin {
            primary.clone()
        } else {
            let view = self.topology.read().await.clone();
            let Some(mp) = view.node_type_primary(&primary.mesh, crate::model::NodeKind::NodeAdmin).cloned() else {
                return Err(RoleMoveEnd::Refused(format!("{me}'s view names no mesh-primary of {}: the {} primary {} is commanded through its mesh-primary", primary.mesh, primary.kind.node_type(), primary.name)));
            };
            if mp.node_id == self.node_id {
                return match self.update_node_type_primary(primary, None, build_id, run, operation).await {
                    Ok(revisions) => Ok(revisions),
                    Err(Reply::Failed { step, reason, code }) => Err(RoleMoveEnd::Failed { step, reason, code }),
                    Err(refusal) => Err(RoleMoveEnd::Refused(format!("{me}: node.primary.update of {} was refused: {} ({refusal:?})", primary.name, refusal.name()))),
                };
            }
            mp
        };
        tracing::Span::current().record("to", to.name.to_string().as_str());
        // A primary the election has superseded answers nothing: what the update came to is discovered,
        // and the successor's node.primary.updated is obtained before anything is installed.
        if self.incumbency().lost.contains(&hb.incarnation) {
            return match self.discovered(&hb, primary.kind, build_id, run, operation).await {
                Ok(Some(revisions)) => Ok(revisions),
                Ok(None) => Err(RoleMoveEnd::Incomplete(format!("{me}: {} is gone and its view names it the {} primary still: no update happened, and the ordinary election names its successor", primary.name, primary.kind.node_type()))),
                Err(why) => Err(RoleMoveEnd::Incomplete(why)),
            };
        }
        let req = Request::UpdatePrimary { node_type: primary.kind.node_type().to_string(), build_id: build_id.to_string(), run, operation: operation.to_string(), successor: None };
        match self.call_once(&to.node_id, &req, &CallOptions { budget: Budget::Stream { send: COMMAND_SEND }, ..CallOptions::default() }).await {
            Ok(Reply::PrimaryUpdated { revisions }) if !revisions.is_empty() => {
                self.take(&revisions).await;
                Ok(revisions)
            }
            Ok(Reply::Failed { step, reason, code }) => Err(RoleMoveEnd::Failed { step, reason, code }),
            Ok(refusal) => Err(RoleMoveEnd::Refused(format!("{} refused node.primary.update of {}: {} ({refusal:?})", to.name, primary.name, refusal.name()))),
            Err(why) => Err(RoleMoveEnd::Incomplete(format!("{me}: node.primary.update {operation} of {} is uncertain and the step is incomplete; a later attempt sends the identical command: {why}", primary.name))),
        }
    }

    /// What an update of the primary `primary` (of `kind`, in its mesh) came to, discovered from this
    /// admin's view: when the record of that type's primary names another birth now, the update to it
    /// happened, and that successor's node.primary.updated is obtained on the identical call before
    /// anything is installed ("Discovery is not installation"). `None`: the view names `primary` still,
    /// or `primary` was never that type's primary in this view, so no update moved it.
    async fn discovered(&self, primary: &HandoverBirth, kind: crate::model::NodeKind, build_id: &str, run: u32, operation: &str) -> Result<Option<Vec<Entry>>, String> {
        let primary_view = self.topology_view.primaries();
        // The record the operation moves: the fabric-primary's for node.fabric-primary.update, its type's
        // otherwise.
        let fabric_update = operation.starts_with("node.fabric-primary.update:");
        let (role, record) = if fabric_update { (PrimaryRole::FabricPrimary, primary_view.fabric()) } else { (PrimaryRole::of_node_type(kind), primary_view.node_type_primary(&primary.mesh, kind)) };
        let Some(record) = record.filter(|r| !r.is_birth(&primary.node_id, &primary.incarnation)) else { return Ok(None) };
        if !primary_view.replaced(role, &primary.mesh, &primary.incarnation) {
            return Ok(None);
        }
        let successor = Birth { node_id: record.node_id.clone(), incarnation: record.incarnation.clone() };
        let node_type = if fabric_update { FABRIC.to_string() } else { kind.node_type().to_string() };
        let mut entries = vec![(node_type, exact(primary), successor.clone(), record.revision)];
        // A same-mesh successor became both: the fabric-primary moved from `primary` with the mesh-primary.
        if let Some(f) = primary_view.fabric().filter(|f| f.is_birth(&record.node_id, &record.incarnation)) {
            if !fabric_update && kind == crate::model::NodeKind::NodeAdmin && primary_view.replaced(PrimaryRole::FabricPrimary, &primary.mesh, &primary.incarnation) {
                entries.push((FABRIC.to_string(), exact(primary), successor.clone(), f.revision));
            }
        }
        if successor.node_id == self.node_id {
            return Ok(Some(entries));
        }
        let revisions = call_successor(&self.client, &successor.node_id, &entries, build_id, run, operation).await.map_err(|how| format!("{}: the successor {successor} of {operation} did not answer node.primary.updated: {how}", self.held_name()))?;
        self.take(&revisions).await;
        Ok(Some(revisions))
    }

    /// node.primary.updated for the update `operation` that preceded an operation on `named` in `mesh`,
    /// obtained by the executor that resumes a Build (node-primary-update.md: "The workflow continues only
    /// after the result is installed and acknowledged: node.primary.updated answered to the resuming
    /// executor's call, never a read alone"). An update this admin holds acknowledged is acknowledged
    /// again with nothing installed. Otherwise the update is discovered from this admin's view
    /// (`Self::discovered`). `false`: the view names the primary still, or holds no node at the name
    /// the update names (no primary is there, so no update is owed): no update happened.
    pub async fn row_acknowledged(&self, mesh: &str, operation: &str, named: Option<&PathName>, build_id: &str, attempt: u32) -> Result<bool, String> {
        if self.memo.acknowledged(operation).is_some() {
            return Ok(true);
        }
        let me = self.held_name();
        let view = self.topology.read().await.clone();
        let Some(node) = named.and_then(|n| view.node(n)).cloned() else {
            tracing::info_span!("rdm.node_admin.primary.update.via-acknowledged", node = %me, operation, build_id, attempt, primary = %named.map(|n| n.to_string()).unwrap_or_default(), outcome = "no-node")
                .in_scope(|| tracing::info!("the view holds no node at the name the update names: no primary there, no update owed"));
            return Ok(false);
        };
        let Some(hb) = birth_of(&node) else { return Err(format!("{me}: {} has no known birth", node.name)) };
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-acknowledged", node = %me, operation, build_id, attempt, primary = %births(&hb), outcome = tracing::field::Empty);
        match self.discovered(&hb, node.kind, build_id, run_of(operation, attempt), operation).instrument(span.clone()).await? {
            Some(_) => {
                self.memo.acknowledge(operation, false);
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
    pub async fn fabric_primary_updated(&self, departing: &str, operation: &str) -> Result<(), String> {
        let me = self.held_name();
        match self.topology_view.primaries().fabric() {
            Some(h) if h.mesh == departing => Err(format!("{me}: the fabric-primary is {h}, a node-admin of the departing mesh {departing}: node.fabric-primary.update {operation} has not moved it")),
            held => {
                tracing::info_span!("rdm.node_admin.primary.update.via-fabric-primary-left", node = %me, operation, departing, fabric_primary = %held.map(|h| h.to_string()).unwrap_or_default())
                    .in_scope(|| tracing::info!("the fabric-primary is no node-admin of the departing mesh"));
                Ok(())
            }
        }
    }

    /// A request off the wire, decided on this admin's own view, whoever calls (node-rpc.md §14).
    pub async fn serve(self: &Arc<Self>, req: Request) -> Reply {
        match req {
            Request::PrimaryUpdate { primaries, build_id, run, operation } => self.serve_primary_update(primaries, build_id, run, operation).await,
            Request::UpdatePrimary { node_type, build_id, run, operation, .. } => self.serve_update_primary(node_type, build_id, run, operation).await,
        }
    }

    // ---- the mesh-primary commanding another node type's primary of its mesh ----

    /// node.primary.update of `primary`, the primary of a node type other than node-admin, at this admin
    /// as its mesh's mesh-primary, the commander and the writer: its pre-check (step 2) from its own view
    /// names a Ready successor, none is `Failed` by name; then the command to the primary, which
    /// computes its successor by the same election and calls it, answered with the successor's
    /// node.primary.updated, installed here. The successor's row is recorded from its digest (step 5).
    pub async fn update_node_type_primary(&self, primary: &crate::model::Node, _expected: Option<u64>, build_id: &str, run: u32, operation: &str) -> Result<Vec<Entry>, Reply> {
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-command-member", node = %me, primary = %primary.name, node_type = primary.kind.node_type(), build_id, run, operation, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let r = self.update_node_type_primary_in(primary, build_id, run, operation).instrument(span.clone()).await;
        match &r {
            Ok(revisions) => {
                self.memo.acknowledge(operation, false);
                span.record("outcome", "updated").record("detail", entries_named(revisions).as_str())
            }
            Err(other) => span.record("outcome", other.name()).record("detail", format!("{other:?}").as_str()),
        };
        span.in_scope(|| tracing::info!("the mesh-primary's command of a node type's primary ended"));
        r
    }

    async fn update_node_type_primary_in(&self, primary: &crate::model::Node, build_id: &str, run: u32, operation: &str) -> Result<Vec<Entry>, Reply> {
        let me = self.held_name();
        let mesh = primary.mesh.clone();
        let primary_view = self.topology_view.primaries();
        if self.me.mesh != mesh || !primary_view.mesh(&mesh).is_some_and(|h| h.is_birth(&self.node_id, &self.incarnation)) {
            let reason = format!("{me} is not the mesh-primary of {mesh}, the commander of its {} primary", primary.kind.node_type());
            refused("commander", "not-primary", &reason, operation);
            return Err(rdm_core::reply::handover::bounded_not_primary(&reason));
        }
        // Step 2: the writer's pre-check, from its own view, names the successor the command carries: the
        // primary calls it and runs no election of its own (Q129).
        let view = self.topology.read().await.clone();
        let nodes: Vec<crate::model::Node> = view.members().cloned().collect();
        let successor = match crate::election::commanded_node_type_successor(&nodes, &self.incumbency(), primary) {
            Ok(n) => n,
            Err(text) => {
                refused("commander", "no-successor", &text, operation);
                return Err(rdm_core::reply::handover::bounded_failed(STEP, &text, rdm_core::outcome::Code::FailedPrecondition));
            }
        };
        let Some(named) = birth_of(&successor) else {
            let text = format!("{me}: the successor {} of {operation} has no known birth", successor.name);
            refused("commander", "no-successor", &text, operation);
            return Err(rdm_core::reply::handover::bounded_failed(STEP, &text, rdm_core::outcome::Code::FailedPrecondition));
        };
        let req = Request::UpdatePrimary { node_type: primary.kind.node_type().to_string(), build_id: build_id.to_string(), run, operation: operation.to_string(), successor: Some(exact(&named)) };
        match self.call_once(&primary.node_id, &req, &CallOptions { budget: Budget::Stream { send: COMMAND_SEND }, ..CallOptions::default() }).await {
            Ok(Reply::PrimaryUpdated { revisions }) if !revisions.is_empty() => {
                self.take(&revisions).await;
                Ok(revisions)
            }
            Ok(refusal) => Err(refusal),
            Err(why) => {
                let reason = format!("the command to the {} primary {} did not answer: {why}", primary.kind.node_type(), primary.name);
                refused("commander", "unanswered", &reason, operation);
                Err(rdm_core::reply::handover::bounded_failed(STEP, &reason, rdm_core::outcome::Code::Unavailable))
            }
        }
    }
}

// ---- a node of a type that is not node-admin (re-exported from rdm-node) ----

pub use rdm_node::member_handover::{serve_member, MemberDoor, MemberSlot};

/// The door a running admin fills once it holds a view.
pub type HandoverSlot = Arc<OnceLock<Arc<HandoverDoor>>>;

/// Serve node.primary.update on this admin. Until `slot` is filled a call is `NotReady`.
pub fn serve(b: ServerBuilder, slot: HandoverSlot) -> ServerBuilder {
    b.serve::<NodePrimaryUpdate, _, _>(OpOwner::Product("rdm".into()), move |_peer: PeerContext, req: Request| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(rdm_core::reply::handover::not_ready("this admin holds no view yet".into()));
            };
            Ok(door.serve(req).await)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::process::ProcessDeploymentProvider;
    use crate::model::{Fabric, FabricId, MeshId, Node, NodeKind, ProviderKind, ScopeStatus};
    use rdm_core::Primary;
    use rdm_transport::StaticResolver;
    use std::net::SocketAddr;

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
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let node_id = NodeId::mint();
        let incarnation = IncarnationId::mint();

        let topology_view = TopologyView::join(
            &gossip,
            &ep,
            &fabric_id,
            "mesh2",
            &mesh_id,
            "mesh2.admin.1",
            Arc::new(time),
            vec![],
            &[],
        )
        .await
        .unwrap();
        let backbone = Backbone::join(
            &gossip,
            &ep,
            &topology_view,
            "mesh2",
            rdm_core::PublisherId {
                node: "mesh2.admin.1".to_string(),
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
            me: me.clone(),
            me_id: node_id.clone(),
            topology_view: topology_view.clone(),
            client: client.clone(),
            provider,
            nodes: None,
            heard_again: Default::default(),
            connections: None,
            commands: None,
        };
        let door = HandoverDoor {
            fabric: "fab".into(),
            me: me.clone(),
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
        };

        let lost_fp = Primary {
            mesh: "mesh1".into(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            revision: 3,
        };

        // 1. A target node with no incarnation is refused `NotReady`.
        let mut to_node = Node::allocated(me.clone());
        to_node.node_id = node_id.clone();
        assert!(matches!(
            door.take_under_delete(&lost_fp, 2, &to_node).await,
            Err(Reply::NotReady { .. })
        ));

        // 2. With a valid incarnation, `take_under_delete` installs both `FabricPrimary` (at `target.revision + 1`)
        //    and `mesh1`'s `NodeAdmin` primary (at `mesh_revision`) onto `to_node`.
        to_node.incarnation_id = Some(incarnation.clone());
        let entries = door.take_under_delete(&lost_fp, 2, &to_node).await.expect("take_under_delete succeeds");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, FABRIC);
        assert_eq!(entries[0].3, 4);
        assert_eq!(entries[1].0, NodeKind::NodeAdmin.node_type());
        assert_eq!(entries[1].3, 2);

        let held_fp = topology_view.primaries().fabric().expect("fabric primary installed");
        assert_eq!(held_fp.node_id, node_id);
        assert_eq!(held_fp.revision, 4);

        // 3. If a newer revision is already held, `take_under_delete` refuses `NotReady` all-or-none.
        assert!(matches!(
            door.take_under_delete(&lost_fp, 1, &to_node).await,
            Err(Reply::NotReady { .. })
        ));
        ep.close().await;
    }
}

