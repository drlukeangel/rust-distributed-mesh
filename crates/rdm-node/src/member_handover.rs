//! Member-side `node.primary.update` door (`rdm-node`).

use std::sync::Arc;

use rdm_transport::membership::{Membership, SeatTaken};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, PeerContext, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::handover::{
    CommittedSeat, HandoverBirth, NodePrimaryUpdate, NodePrimaryUpdateReply as Reply,
    NodePrimaryUpdateRequest as Request, SeatUpdate,
};
use rdm_core::outcome::RpcOutcome;
use rdm_core::LifecycleOp;
use tracing::Instrument as _;

use crate::model::{IncarnationId, NodeId, PathName};

fn births(b: &HandoverBirth) -> String {
    format!("{}:{}:{}", b.mesh, b.node_id, b.incarnation.as_str())
}

struct Refusing<'a> {
    operation: &'a str,
    build_id: &'a str,
    attempt: u32,
    holder: Option<&'a HandoverBirth>,
    successor: Option<&'a HandoverBirth>,
}

fn refused(side: &'static str, reason: &'static str, detail: &str, of: &Refusing<'_>) {
    let span = tracing::info_span!(
        "rdm.node_admin.seat.reject.via-primary-update",
        side,
        reason,
        detail,
        operation = of.operation,
        build_id = of.build_id,
        attempt = of.attempt,
        holder = %of.holder.map(births).unwrap_or_default(),
        successor = %of.successor.map(births).unwrap_or_default(),
        source = tracing::field::Empty,
        op = tracing::field::Empty,
        cause = tracing::field::Empty,
    );
    span.in_scope(|| tracing::info!("node.primary.update was refused"));
}

// ---- a node of a cohort that is not node-admin: the holder's call and the successor's install ----

/// The 0x21 door of a node of a cohort that is not node-admin (proposal Part C1: "For a `broker` cohort
/// the mesh-primary is commander and writer, and the holder B1 only makes the call"). As the holder, its
/// mesh-primary commands it after the committed row, carrying the row's content
/// ([`Request::UpdatePrimaryFromRow`]): it calls node.primary.update on the successor's exact birth with
/// that content and answers with the successor's node.primary.updated (step 6: "A non-admin holder then
/// answers its commander's command"). As the successor, it installs itself as primary from the row's
/// content the call carries and answers node.primary.updated (step 6). It writes no row and announces
/// nothing: its mesh-primary is the writer and the sole author of member status. Its one refusal is a
/// conflicting payload for an operation it answered (C3 edit 5): an identical repeat is answered as the
/// first was, and nothing checks who calls (R-RX1) or the birth a call names (R-UC1a, R-RX5).
pub struct MemberDoor {
    me: PathName,
    membership: Membership,
    client: Arc<NodeRpcClient>,
    /// What this node answered node.primary.updated for, by operation: the seats the call carried and
    /// the answer.
    answered: std::sync::Mutex<std::collections::BTreeMap<String, (Vec<SeatUpdate>, Reply)>>,
    /// One call at a time: a repeat arriving while its first runs is answered from it.
    serving: tokio::sync::Mutex<()>,
}

impl MemberDoor {
    /// The door of the birth `node_id`/`incarnation` at `me`, on its mesh channel `membership`, calling
    /// through `client`.
    pub fn new(me: PathName, _node_id: NodeId, _incarnation: IncarnationId, membership: Membership, client: Arc<NodeRpcClient>) -> Self {
        Self { me, membership, client, answered: Default::default(), serving: Default::default() }
    }

    fn held_name(&self) -> PathName {
        self.membership.book.held_path_name().unwrap_or_else(|| self.me.clone())
    }

    /// This node's mesh channel, which holds its seat records.
    pub fn membership(&self) -> &Membership {
        &self.membership
    }

    /// The answer to a repeat of `operation`: the first answer for the same seats, a conflict for others.
    fn answered_before(&self, side: &'static str, operation: &str, seats: &[SeatUpdate], build_id: &str, attempt: u32) -> Option<Reply> {
        let held = self.answered.lock().unwrap().get(operation).cloned()?;
        if held.0 == seats {
            return Some(held.1);
        }
        let me = self.held_name();
        let of = Refusing { operation, build_id, attempt, holder: seats.first().map(|u| &u.holder), successor: seats.first().map(|u| &u.successor) };
        refused(side, "conflict", &format!("{me}: operation {operation} was answered for seats {:?}; this call carries {seats:?}", held.0), &of);
        Some(Reply::RejectedConflict { operation: operation.to_string() })
    }

    /// Serve one node.primary.update call.
    pub async fn serve(&self, req: Request) -> Reply {
        let _one_at_a_time = self.serving.lock().await;
        match req {
            Request::PrimaryUpdate { seats, build_id, attempt, operation } => self.install(seats, build_id, attempt, operation).await,
            Request::UpdatePrimaryFromRow { fabric, seats, build_id, attempt, operation } => self.call_successor(fabric, seats, build_id, attempt, operation).await,
            Request::UpdatePrimary { build_id, attempt, operation, .. } => {
                let me = self.held_name();
                let reason = format!("{me} holds no node-admin or fabric seat: the seat of its cohort is moved by its mesh-primary, which writes the row and commands it with the row's content");
                refused("holder", "not-authority", &reason, &Refusing { operation: &operation, build_id: &build_id, attempt, holder: None, successor: None });
                Reply::not_authority(&reason)
            }
        }
    }

    /// The successor installs itself as its cohort's primary from the row's content the call carries
    /// and answers node.primary.updated (C1 step 6).
    async fn install(&self, seats: Vec<SeatUpdate>, build_id: String, attempt: u32, operation: String) -> Reply {
        if let Some(r) = self.answered_before("successor", &operation, &seats, &build_id, attempt) {
            return r;
        }
        let me = self.held_name();
        let Some(first) = seats.first().cloned() else {
            refused("successor", "conflict", &format!("{me}: the call names no seat, so it is no committed row's content"), &Refusing { operation: &operation, build_id: &build_id, attempt, holder: None, successor: None });
            return Reply::RejectedConflict { operation };
        };
        let committed: Vec<CommittedSeat> = seats.iter().map(SeatUpdate::committed).collect();
        // The holder by the name this node's mesh channel holds for its birth.
        let holder_name = self.membership.book.all().into_iter().find(|d| d.node.node_id == first.holder.node_id).map(|d| d.node.name.clone()).unwrap_or_else(|| me.clone());
        let op = LifecycleOp { build_id: build_id.clone(), attempt, operation: operation.clone(), node_id: first.holder.node_id.clone(), incarnation: first.holder.incarnation.clone(), name: holder_name, event_at_rafka_ms: self.membership.clock().now_rafka_ms() };
        let taken = self.membership.seats().take_together(&op, &committed);
        let outcome = if taken.iter().any(|t| matches!(t, SeatTaken::Refused { .. })) {
            "refused-not-newer"
        } else if taken.iter().all(|t| matches!(t, SeatTaken::Same)) {
            "same"
        } else {
            "held"
        };
        tracing::info_span!("rdm.node_admin.seat.update.via-primary-updated", node = %me, operation = %operation, build_id = %build_id, attempt, holder = %births(&first.holder), successor = %births(&first.successor), seat = first.seat.name(), revision = first.revision, replay = outcome != "held", outcome)
            .in_scope(|| tracing::info!("the successor installed itself from the committed row's content and answers node.primary.updated"));
        let reply = Reply::PrimaryUpdated { revisions: committed };
        self.answered.lock().unwrap().insert(operation, (seats, reply.clone()));
        reply
    }

    /// The holder, commanded by its mesh-primary with the committed row's content, calls
    /// node.primary.update on the successor's exact birth carrying it and answers with the successor's
    /// node.primary.updated (C1 step 6). A call that does not answer it is named in an `Incomplete`
    /// answer: the seat is the successor's from the row, and the commander's next attempt sends the
    /// identical command.
    async fn call_successor(&self, fabric: String, seats: Vec<SeatUpdate>, build_id: String, attempt: u32, operation: String) -> Reply {
        if let Some(r) = self.answered_before("holder", &operation, &seats, &build_id, attempt) {
            return r;
        }
        let me = self.held_name();
        let Some(first) = seats.first().cloned() else {
            refused("holder", "conflict", &format!("{me}: the command names no seat, so it carries no committed row"), &Refusing { operation: &operation, build_id: &build_id, attempt, holder: None, successor: None });
            return Reply::RejectedConflict { operation };
        };
        let span = tracing::info_span!("rdm.node_admin.seat.update.via-holder-call", node = %me, fabric = %fabric, operation = %operation, build_id = %build_id, attempt, holder = %births(&first.holder), successor = %births(&first.successor), seat = first.seat.name(), revision = first.revision, outcome = tracing::field::Empty, detail = tracing::field::Empty);
        let req = Request::PrimaryUpdate { seats: seats.clone(), build_id: build_id.clone(), attempt, operation: operation.clone() };
        let (out, _) = self.client.call::<NodePrimaryUpdate>(&NodeTarget::ExactNode(first.successor.node_id.clone()), &req, &CallOptions::default()).instrument(span.clone()).await;
        match out {
            RpcOutcome::Reply(r) if matches!(r.value(), Reply::PrimaryUpdated { revisions } if !revisions.is_empty()) => {
                let reply = r.value().clone();
                span.record("outcome", "primary-updated");
                self.answered.lock().unwrap().insert(operation, (seats, reply.clone()));
                reply
            }
            other => {
                let detail = format!("{me}: node.primary.update {operation} to {} did not answer node.primary.updated: the {} call ended {} ({other:?}); the seat is the successor's from the row", births(&first.successor), req.op(), other.name());
                span.record("outcome", other.name()).record("detail", detail.as_str());
                Reply::incomplete(&detail)
            }
        }
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
                return Ok(Reply::NotReady { reason: "this node has not joined its mesh yet".into() });
            };
            Ok(door.serve(req).await)
        }
    })
}

