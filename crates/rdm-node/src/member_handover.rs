//! `node.primary.update` on op `0x21`, one primitive for every node type
//! (lifecycles/node-primary-update.md; node-rpc-envelope.md "PrimaryUpdate, op `0x21`, rdm").
//!
//! The primary, commanded from above with `UpdatePrimary`, computes its successor by the election over
//! its own view with itself excluded, calls `PrimaryUpdate` on the successor's exact birth carrying the
//! election's result, and answers its commander with the successor's `PrimaryUpdated`. The successor
//! installs the result in its own view and answers. The only difference between node types is what a
//! node-admin records afterwards (`is_primary`, `is_fabric_primary` on its own row, as its mesh's
//! writer): the shared parts are here, and `rdm-admin`'s door adds the node-admin's.

use std::sync::Arc;

use rdm_core::catalog::OpOwner;
use rdm_core::framing::Birth;
use rdm_core::handover::{NodePrimaryUpdate, NodePrimaryUpdateReply as Reply, NodePrimaryUpdateRequest as Request, FABRIC};
use rdm_core::outcome::RpcOutcome;
use rdm_core::{PrimaryRole};
use rdm_transport::topology::{TopologyView, PrimaryView, RoleTaken};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use tracing::Instrument as _;

use crate::model::{IncarnationId, NodeId, PathName};

/// One entry of `PrimaryUpdate`'s `primaries`: `(type, primary, successor, revision)`.
pub type Entry = (String, Birth, Birth, u64);

/// The step a failed commanded update names (`Failed { step, .. }`).
pub const STEP: &str = "primary-update";

/// The primary record the type `type` of an entry names: `fabric` the fabric-primary, `node-admin` the
/// mesh-primary, every other type its node-type primary. `None` for a name outside the type grammar.
pub fn primary_of_type(r#type: &str) -> Option<PrimaryRole> {
    if r#type == FABRIC {
        return Some(PrimaryRole::FabricPrimary);
    }
    rdm_core::status::type_refusal(r#type).is_none().then(|| PrimaryRole::of_type(r#type))
}

/// The records `entries` name, each its successor at its revision, of the mesh `mesh_of` gives that
/// successor. An entry of a type RDM does not know names no record.
pub fn records_of(entries: &[Entry], mesh_of: impl Fn(&Birth) -> String) -> Vec<(PrimaryRole, rdm_core::Primary)> {
    entries
        .iter()
        .filter_map(|(r#type, _, successor, revision)| {
            let role = primary_of_type(r#type)?;
            Some((role, rdm_core::Primary { mesh: mesh_of(successor), node_id: successor.node_id.clone(), incarnation: successor.incarnation.clone(), revision: *revision }))
        })
        .collect()
}

/// Install the records `entries` name in `digest_view`, all or none: how the digest view took them (`held`, `same`,
/// or `refused-not-newer` when one is older than the record held).
pub fn install(primary_view: &PrimaryView, entries: &[Entry], mesh_of: impl Fn(&Birth) -> String) -> &'static str {
    let taken = primary_view.take_together(&records_of(entries, mesh_of));
    if taken.iter().any(|t| matches!(t, RoleTaken::Refused { .. })) {
        "refused-not-newer"
    } else if taken.iter().all(|t| matches!(t, RoleTaken::Same)) {
        "same"
    } else {
        "held"
    }
}

/// The entries as a span names them: `<type> <primary> -> <successor> @<revision>`, comma-separated.
pub fn entries_named(entries: &[Entry]) -> String {
    entries.iter().map(|(t, p, s, r)| format!("{t} {p} -> {s} @{r}")).collect::<Vec<_>>().join(", ")
}

/// The complete request content compared when an operation is retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnswerPayload {
    /// Install the supplied election results.
    Install(Vec<Entry>),
    /// Transfer this primary role to the explicitly selected birth.
    Update {
        /// The explicitly requested primary role's node type.
        r#type: String,
        /// The exact successor birth supplied by the command.
        successor: Option<Birth>,
    },
}

/// What a door answered, by operation: an identical repeat is answered as the first was, a different
/// payload for the same operation is `RejectedConflictingPayload` (node-rpc-envelope.md: "An identical
/// retry answers the same PrimaryUpdated before any revision check").
#[derive(Default)]
pub struct Answered(std::sync::Mutex<std::collections::BTreeMap<(String, u32, rdm_core::OperationId), (AnswerPayload, Reply)>>);

#[cfg(test)]
mod answered_tests {
    use super::*;

    /// CONTRACT: retries compare the exact successor within one Build attempt; other
    /// Builds and attempts never inherit that attempt's cached answer.
    // @feature: node-primary-update
    #[test]
    fn handover_retry_checks_successor_and_build_attempt() {
        let answered = Answered::default();
        let id = NodeId::mint();
        let operation = rdm_core::OperationId::node("node.primary.update", &id);
        let successor = Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let payload = AnswerPayload::Update { r#type: "node".into(), successor: Some(successor.clone()) };
        let reply = Reply::PrimaryUpdated { revisions: Vec::new() };
        answered.keep("build-a", 1, &operation, payload.clone(), reply.clone());
        assert_eq!(answered.before("build-a", 1, &operation, &payload), Some(reply));
        assert_eq!(answered.before("build-b", 1, &operation, &payload), None);
        assert_eq!(answered.before("build-a", 2, &operation, &payload), None);
        let changed = AnswerPayload::Update {
            r#type: "node".into(),
            successor: Some(Birth { incarnation: IncarnationId::mint(), ..successor }),
        };
        assert!(matches!(answered.before("build-a", 1, &operation, &changed), Some(Reply::RejectedConflictingPayload { .. })));
        let absent = AnswerPayload::Update { r#type: "node".into(), successor: None };
        assert!(matches!(answered.before("build-a", 1, &operation, &absent), Some(Reply::RejectedConflictingPayload { .. })));
    }
}

impl Answered {
    /// The answer to a repeat of `operation` carrying `payload`: the first answer, or a conflict.
    pub fn before(&self, build_id: &str, run: u32, operation: &rdm_core::OperationId, payload: &AnswerPayload) -> Option<Reply> {
        let held = self.0.lock().unwrap().get(&(build_id.to_owned(), run, operation.clone())).cloned()?;
        if &held.0 == payload {
            return Some(held.1);
        }
        tracing::info_span!("rdm.node_admin.primary.reject.via-conflicting-payload", operation = ?operation, held = ?held.0, offered = ?payload)
            .in_scope(|| tracing::info!("the operation was answered for another payload"));
        Some(rdm_core::reply::handover::rejected_conflicting_payload(operation.clone()))
    }

    /// Keep `reply` as the answer to `operation` carrying `payload`.
    pub fn keep(&self, build_id: &str, run: u32, operation: &rdm_core::OperationId, payload: AnswerPayload, reply: Reply) {
        self.0.lock().unwrap().insert((build_id.to_owned(), run, operation.clone()), (payload, reply));
    }
}

/// How a `PrimaryUpdate` call ended when it did not answer `PrimaryUpdated`.
pub fn ended(out: &RpcOutcome<Reply>) -> String {
    match out {
        RpcOutcome::Reply(r) => format!("answered {} ({:?})", r.value().name(), r.value()),
        o => format!("ended {} ({o:?})", o.name()),
    }
}

/// The primary's call on its successor (step 4): `PrimaryUpdate` carrying `entries`, made once and,
/// when it answers nothing, once more, identical, at the one backoff rule's first delay (the primary
/// never takes NotSent or Indeterminate for an answer). `Err` names how the last call ended.
pub async fn call_successor(client: &NodeRpcClient, successor: &NodeId, entries: &[Entry], build_id: &str, run: u32, operation: &rdm_core::OperationId) -> Result<Vec<Entry>, String> {
    let req = Request::PrimaryUpdate { primaries: entries.to_vec(), build_id: build_id.to_string(), run, operation: operation.clone() };
    let mut last = String::new();
    for attempt in 0..2u32 {
        if attempt > 0 {
            tokio::time::sleep(rdm_transport::delivery::Pacing::from_cadences().after(1)).await;
        }
        let (out, _) = client.call::<NodePrimaryUpdate>(&NodeTarget::ExactNode(successor.clone()), &req, &CallOptions::default()).await;
        match &out {
            RpcOutcome::Reply(r) => match r.value() {
                Reply::PrimaryUpdated { revisions } if !revisions.is_empty() => return Ok(revisions.clone()),
                // A refusal is the receiver's answer: calling again changes nothing.
                _ => return Err(ended(&out)),
            },
            _ => last = ended(&out),
        }
    }
    Err(last)
}

// ---- a node of a type that is not node-admin ----

/// The 0x21 door of a node that is not a node-admin. As the successor it installs the election's result
/// the call carries and answers node.primary.updated (step 4). As its type's primary, commanded by its
/// mesh-primary, it calls the successor the command names (the mesh-primary's pre-check, step 2; no
/// election of its own, Q129) and answers with its node.primary.updated. It writes no row: its
/// mesh-primary records the change from the successor's digest (step 5).
pub struct MemberDoor {
    me: PathName,
    r#type: String,
    node_id: NodeId,
    incarnation: IncarnationId,
    topology_view: TopologyView,
    client: Arc<NodeRpcClient>,
    /// What this node answered as the successor, by operation.
    answered: Answered,
    /// What this node answered its commander as the primary, by operation.
    commanded: Answered,
    /// One call at a time: a repeat arriving while its first runs is answered from it.
    serving: tokio::sync::Mutex<()>,
}

impl MemberDoor {
    /// The door of the birth `node_id`/`incarnation` at `me`, on its mesh channel `topology_view`, calling
    /// through `client`.
    pub fn new(me: PathName, r#type: &str, node_id: NodeId, incarnation: IncarnationId, topology_view: TopologyView, client: Arc<NodeRpcClient>) -> Self {
        Self { me, r#type: r#type.to_string(), node_id, incarnation, topology_view, client, answered: Answered::default(), commanded: Answered::default(), serving: Default::default() }
    }

    fn held_name(&self) -> PathName {
        self.topology_view.digest_view.held_path_name().unwrap_or_else(|| self.me.clone())
    }

    fn me_birth(&self) -> Birth {
        Birth { node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }
    }

    /// This node's mesh channel, which holds the primaries' records.
    pub fn topology_view(&self) -> &TopologyView {
        &self.topology_view
    }

    /// The mesh of the birth `b`: every entry this node installs or calls is of its own mesh.
    fn mesh_of(&self) -> impl Fn(&Birth) -> String + '_ {
        move |_| self.topology_view.mesh_name().to_string()
    }

    /// Serve one node.primary.update call.
    pub async fn serve(&self, req: Request) -> Reply {
        let _one_at_a_time = self.serving.lock().await;
        match req {
            Request::PrimaryUpdate { primaries, build_id, run, operation } => self.install(primaries, build_id, run, operation),
            Request::UpdatePrimary { r#type, build_id, run, operation, successor } => self.update(r#type, build_id, run, operation, successor).await,
        }
    }

    /// The successor installs the election's result the call carries and answers node.primary.updated
    /// (step 4). It never refuses the call for the birth it names (R-UC1a, R-RX5).
    fn install(&self, primaries: Vec<Entry>, build_id: String, run: u32, operation: rdm_core::OperationId) -> Reply {
        let payload = AnswerPayload::Install(primaries.clone());
        if let Some(r) = self.answered.before(&build_id, run, &operation, &payload) {
            return r;
        }
        // The span covers the install: its start is the moment the successor begins to install.
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-primary-updated", node_location = ?self.held_name(), operation = %operation.action, operation_subject = ?operation.subject, build_id = %build_id, run, primaries = %entries_named(&primaries), outcome = tracing::field::Empty);
        let outcome = span.in_scope(|| install(self.topology_view.primaries(), &primaries, self.mesh_of()));
        span.record("outcome", outcome);
        span.in_scope(|| tracing::info!("the successor installed the election's result the call carries and answers node.primary.updated"));
        let reply = Reply::PrimaryUpdated { revisions: primaries };
        self.answered.keep(&build_id, run, &operation, payload, reply.clone());
        reply
    }

    /// The primary of its node type, commanded by its mesh-primary, runs step 4: it calls the successor
    /// the command names (the mesh-primary's pre-check, step 2), runs no election of its own (Q129), and
    /// answers with the successor's node.primary.updated, installed in its own view first. A command that
    /// names none is `Failed` by name; a successor that does not answer is `Failed`: the successor is
    /// unheard after the update.
    async fn update(&self, r#type: String, build_id: String, run: u32, operation: rdm_core::OperationId, successor: Option<Birth>) -> Reply {
        let payload = AnswerPayload::Update { r#type: r#type.clone(), successor: successor.clone() };
        if let Some(r) = self.commanded.before(&build_id, run, &operation, &payload) {
            return r;
        }
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.primary.update.via-update-primary", node_path = %me.path, node_name = %me.name, "type" = %r#type, build_id = %build_id, run, operation = %operation.action, operation_subject = ?operation.subject, successor = tracing::field::Empty, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let reply = self.update_in(&me, &r#type, &build_id, run, &operation, successor).instrument(span.clone()).await;
        span.record("outcome", reply.name());
        if let Reply::Failed { reason, .. } = &reply {
            span.record("detail", reason.as_str());
        }
        span.in_scope(|| tracing::info!("node.primary.update ran at the primary"));
        if matches!(reply, Reply::PrimaryUpdated { .. } | Reply::Failed { .. }) {
            self.commanded.keep(&build_id, run, &operation, payload, reply.clone());
        }
        reply
    }

    async fn update_in(&self, me: &PathName, r#type: &str, build_id: &str, run: u32, operation: &rdm_core::OperationId, successor: Option<Birth>) -> Reply {
        let mesh = self.topology_view.mesh_name().to_string();
        if r#type != self.r#type {
            return rdm_core::reply::handover::bounded_failed(STEP, &format!("{me:?} is a {}, not of the type {} the command names", self.r#type, r#type), rdm_core::outcome::Code::FailedPrecondition);
        }
        // Commanded by its mesh-primary, it runs the update (a receiver never refuses a call for the birth it
        // names, R-RX5). The revision the successor's entry takes is the next after the record of its type
        // this node holds; holding none, it reads its mesh-primary's topology first, as a starting node does.
        let primary_view = self.topology_view.primaries();
        if primary_view.node_type_primary(&mesh, &self.r#type).is_none() {
            self.read_own_mesh(me, &mesh).await;
        }
        let revision = primary_view.node_type_primary(&mesh, &self.r#type).map_or(0, |r| r.revision) + 1;
        // Step 4 on the successor the mesh-primary's pre-check named (step 2): no election here (Q129).
        let Some(successor) = successor else {
            return rdm_core::reply::handover::bounded_failed(STEP, &format!("path {:?}, name {:?} is a {} primary: its mesh-primary's command names no successor", me.path, me.name, r#type), rdm_core::outcome::Code::FailedPrecondition);
        };
        tracing::Span::current().record("successor", successor.node_id.as_str());
        let entries = vec![(r#type.to_string(), self.me_birth(), successor.clone(), revision)];
        // Step 4: the call on the successor's exact birth.
        match call_successor(&self.client, &successor.node_id, &entries, build_id, run, operation).await {
            Ok(revisions) => {
                install(primary_view, &revisions, self.mesh_of());
                Reply::PrimaryUpdated { revisions }
            }
            Err(how) => rdm_core::reply::handover::bounded_failed(STEP, &format!("the successor is unheard after the update: {successor} {how}"), rdm_core::outcome::Code::Unavailable),
        }
    }
}

impl MemberDoor {
    /// One `GetTopology` of `mesh` from the mesh-primary this node holds, installed into its view: the
    /// primary records the projection carries come with it.
    async fn read_own_mesh(&self, me: &PathName, mesh: &str) {
        let Some(mp) = self.topology_view.primaries().mesh(mesh) else { return };
        let read = crate::topology_read::get_topology(&self.client, &NodeTarget::ExactNode(mp.node_id.clone()), mesh, &self.topology_view, Some(mesh), None).await;
        tracing::info_span!("rdm.node_admin.primary.resolve.via-topology-read", node_path = %me.path, node_name = %me.name, mesh, mesh_primary = %mp, outcome = %read.as_ref().map(|_| "installed".to_string()).unwrap_or_else(|e| e.to_string()))
            .in_scope(|| tracing::info!("the commanded primary read its mesh-primary's topology: its own record was not held yet"));
    }
}

/// Where a member's door is set once its process holds its mesh channel.
pub type MemberSlot = Arc<std::sync::OnceLock<Arc<MemberDoor>>>;

/// Serve node.primary.update on a node that is not a node-admin ([`MemberDoor`]). Until `slot` is
/// filled a call is `NotReady`.
pub fn serve_member(b: ServerBuilder, slot: MemberSlot) -> ServerBuilder {
    b.serve::<NodePrimaryUpdate, _, _>(OpOwner::Product("rdm".into()), move |_peer: PeerContext, req: Request| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(rdm_core::reply::handover::not_ready("this node has not joined its mesh yet".into()));
            };
            Ok(door.serve(req).await)
        }
    })
}
