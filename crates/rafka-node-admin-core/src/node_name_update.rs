//! `node.name.update` at the node's mesh-primary, the one writer of its mesh's rows (proposal
//! node-properties-own-commands.md §2; R13, R-CR1, R-MN1, R-OW1). The request carries the exact birth
//! and the name only: the path never travels, so it cannot move. From what the mesh-primary holds, in
//! order (§2.1 Validation): the exact birth matches its record (`RejectedStaleIncarnation`); it is the
//! node's writer, the mesh-primary or, before the mesh has one, the fabric-primary
//! (`ReceiverNotPrimary`); the node admits no connection, stopped, or holding the birth's
//! `node.connections.deleted` of an `All` made while it was not ready-for-traffic and leaving nothing
//! held (`RejectedAdmittingConnections { status, held_connections }`); the name is within the name
//! grammar and its join with the held record's path is a `path.name` this build keys
//! (`RejectedName`). Then, one change at a time with every other change to a node record: the new name
//! is claimed by insert-and-fail in the store adapter (`NameHeld { by }`, nothing written) and the row
//! moved through the one writer of a row's move (`crate::node_rows::move_row`): put at the new key
//! carrying `renamed_from`, then the old key released. A release that fails leaves both keys held and
//! ends the command incomplete naming it (`NameNotApplied`, `release-pending`); the next writer that
//! meets the row finishes it (`crate::node_rows::finish_release`). A stopped node is told nothing and
//! the command completes on the row; a live one is told `ApplyNodeName` (this admin itself in-process,
//! through its own name decision) and the command completes only on its `Applied` or `AlreadyApplied`
//! (§2.3). A repeat naming the name the row already holds finishes any pending release and then, for a
//! live node, resends `ApplyNodeName` (§2.1 Idempotency). A replace's cutover ([`Cutover`],
//! node-replace.md step 4) is the same command with the retained predecessor's row named: the
//! successor's row is put at the name with that row's config and tags, the old key released, then the
//! retained row marked inherited (`NodeRow::inherited_by`), its labels out of the tags index.

use crate::model::{Node, PathName};
use crate::node_rows::{Moved, RenameRefused};
use crate::storage::NodesStorage;
use crate::topology::Topology;
use rdm_core::ConnectionId;
use rdm_client::{ExactBirth, NodeRpc, Traffic};
use rdm_transport::{CallOptions, NodeTarget, RouteChoice};
use rdm_core::status::{NotAuthority, StatusReply, StatusRequest};
use tokio::sync::RwLock;

/// What a mesh-primary's `node.name.update` acts through: the door its record changes take turns at
/// (`crate::node_config::NodeConfigs`: the writer's held record and the client it tells births
/// through), its view's take of a moved row, and its own name decision for a rename of itself.
pub struct NameDoors<'a> {
    /// The config door: `None` until this admin's node-RPC client exists.
    pub configs: Option<&'a crate::node_config::NodeConfigs>,
    /// This admin's view taking a moved row.
    pub moved: &'a crate::status_rpc::RowMoved,
    /// This admin deciding a name its own row gives it.
    pub rename: &'a crate::status_rpc::Rename,
}

pub use crate::node_properties::FirstReadyOnRoot;

/// A replace's cutover (node-replace.md step 4: "the mesh-primary seeds the successor's config from
/// the `<name>.old` row"; proposal node-properties-own-commands.md §8, replace): the stopped successor
/// takes its name with the config and tags of the predecessor's retained row, in the put that claims
/// the name, and once the old key is released the retained row says it was inherited
/// (`NodeRow::inherited_by`). The recipe's step passes it to the writer; a caller's `UpdateName`
/// carries none, and any name stays renameable (§9 A).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cutover {
    /// The key of the predecessor's retained row (`<name>.old`).
    pub retained: PathName,
    /// The predecessor: the row at `retained` is inherited only while it is this node's.
    pub retained_id: rdm_core::NodeId,
}

/// Serve an `UpdateName` at the admin `me` (born `me_id`): one span holds the whole command, its
/// outcome the reply's name.
#[allow(clippy::too_many_arguments)]
pub async fn update(me: &PathName, me_id: &rdm_core::NodeId, view: &RwLock<Topology>, storage: &dyn NodesStorage, doors: NameDoors<'_>, deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>, first_ready: FirstReadyOnRoot<'_>, req: &StatusRequest, cutover: Option<&Cutover>) -> StatusReply {
    let StatusRequest::UpdateName { node_id, incarnation, name } = req else { unreachable!("update is called for UpdateName only") };
    let span = tracing::info_span!(
        "rdm.node_admin.node.name.update.via-writer",
        node = %me, node_id = %node_id, incarnation_id = %incarnation.as_str(), name = %name, from = tracing::field::Empty, to = tracing::field::Empty,
        inherited_from = tracing::field::Empty, keys = tracing::field::Empty, tags = tracing::field::Empty, traffic = tracing::field::Empty, connections = tracing::field::Empty, apply = tracing::field::Empty,
        outcome = tracing::field::Empty, answered = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
    );
    let reply = tracing::Instrument::instrument(
        async {
            match crate::node_properties::refused_before_first_ready(me, view, first_ready, req.op(), node_id, &span).await {
                Some(refused) => refused,
                None => {
                    // A snapshot of the view: no read lock is held across the row write's I/O.
                    let now = view.read().await.clone();
                    rename(me, me_id, &now, storage, doors, deleted, node_id, incarnation, name, cutover, &span).await
                }
            }
        },
        span.clone(),
    )
    .await;
    span.record("outcome", reply.name());
    span.in_scope(|| tracing::info!("node.name.update decided"));
    reply
}

/// Whether the held node admits a connection (R-CR1a, R-CR2), from what this mesh-primary holds.
struct Admission {
    /// The span's `(traffic, connections)`.
    labels: (String, &'static str),
    /// It admits none, and why; or it admits one: the status the refusal names, the connections it
    /// still held, and why in words.
    quiet: Result<Traffic, (crate::model::NodeStatus, Vec<ConnectionId>, String)>,
}

/// [`Admission`] of `held`: stopped, or its `node.connections.deleted` of an `All` cut made while it
/// was not ready-for-traffic, leaving nothing held.
fn admission_of(held: &Node, deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>) -> Admission {
    if held.parked {
        return Admission { labels: ("Stopped".into(), "stopped"), quiet: Ok(Traffic::Stopped) };
    }
    match deleted {
        None => Admission {
            labels: (format!("{:?}", held.status), "none-deleted"),
            quiet: Err((held.status, vec![], format!("is {:?} and its mesh-primary holds no node.connections.deleted for this birth", held.status))),
        },
        Some(r) if !r.all => Admission {
            labels: (format!("{:?}", r.status_at_cut), "partial"),
            quiet: Err((r.status_at_cut, vec![], format!("is {:?}, and its last node.connections.delete named exact connections, not All: it holds no node.connections.deleted of every connection", r.status_at_cut))),
        },
        Some(r) if !r.held.is_empty() => Admission {
            labels: (format!("{:?}", r.status_at_cut), "held"),
            quiet: Err((r.status_at_cut, r.held.clone(), format!("is {:?} and still held {} connection(s) after its node.connections.delete: {}", r.status_at_cut, r.held.len(), r.held.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ")))),
        },
        Some(r) => Admission { labels: (format!("{:?}", r.status_at_cut), "deleted"), quiet: Ok(Traffic::ConnectionsDeleted) },
    }
}

#[allow(clippy::too_many_arguments)]
async fn rename(
    me: &PathName,
    me_id: &rdm_core::NodeId,
    now: &Topology,
    storage: &dyn NodesStorage,
    doors: NameDoors<'_>,
    deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>,
    node_id: &rdm_core::NodeId,
    incarnation: &rdm_core::IncarnationId,
    name: &str,
    cutover: Option<&Cutover>,
    span: &tracing::Span,
) -> StatusReply {
    // A subject this receiver does not hold is the receiver's own fact (`SubjectUnknown`), never a
    // peer that did not resolve.
    let Some(held) = now.nodes.iter().find(|n| n.node_id == *node_id).cloned() else {
        span.record("detail", format!("{me}: node {node_id} is not in the mesh-primary's view; it cannot be renamed").as_str());
        return StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown };
    };
    span.record("from", held.name.to_string().as_str());
    // 1. The exact birth.
    match held.incarnation_id.as_ref() {
        Some(h) if h != incarnation => return StatusReply::RejectedStaleIncarnation { held: h.clone() },
        None => {
            span.record("detail", format!("{me}: {} has no known birth; it cannot be renamed", held.name).as_str());
            return StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown };
        }
        Some(_) => {}
    }
    // 2. The writer: the mesh-primary of the node's mesh, or the fabric-primary for a mesh with none (R-OW1).
    if !crate::node_rows::writes_for(now, me_id, &held.mesh) {
        let held_by = now.cohort_primary(&held.mesh, rdm_core::NodeKind::NodeAdmin).map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.name));
        let needed = format!("{}'s primary node-admin, the writer of {}'s row ({held_by}; {me} does not hold it)", held.mesh, held.name);
        span.record("detail", needed.as_str());
        return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } };
    }
    // 3. The node admits no connection.
    let admission = admission_of(&held, deleted);
    span.record("traffic", admission.labels.0.as_str()).record("connections", admission.labels.1);
    if let Err((status, held_connections, why)) = admission.quiet {
        span.record("detail", format!("{me}: {} {why}; a name moves only when the node admits no connection (stopped, or its node.connections.delete {{ All }} completed with nothing held): nothing is claimed or written", held.name).as_str());
        return StatusReply::rejected_admitting_connections(crate::views::node_status(status), &held_connections);
    }
    // 4. The name: within the grammar, and joined at the held record's path into a key this build keys.
    if let Some(reason) = rdm_core::status::name_refusal(name) {
        let why = format!("{me}: {reason}; nothing is claimed or written");
        span.record("detail", why.as_str());
        return StatusReply::rejected_name(&why);
    }
    let to = match held.name.with_name(name) {
        Ok(to) => to,
        Err(e) => {
            let why = format!("{me}: the name {name:?} at the path {} makes no path.name this build keys: {e}; nothing is claimed or written", held.name.path());
            span.record("detail", why.as_str());
            return StatusReply::rejected_name(&why);
        }
    };
    span.record("to", to.to_string().as_str());
    let Some(door) = doors.configs else {
        return StatusReply::NotReady { reason: format!("{me} has no node-RPC client yet, and so no door its record changes take turns at: the rename of {} is refused and nothing is written", held.name) };
    };
    // One change to a node record at a time, through the writer's held record: the row carries every
    // field the writer knows of it, the config and the tags included.
    let _one = door.one_at_a_time().await;
    let known = match door.written().known_row(storage, &held.mesh, node_id).await {
        Ok(row) => row,
        Err(why) => return StatusReply::node_row_refused(&held.name.to_string(), &format!("{me}: {why}: the rename of {} is refused and nothing is written", held.name)),
    };
    // The key the node's row stands at now: the row's own, else the name the view holds.
    let from = known.as_ref().map_or_else(|| held.name.clone(), |r| r.name.clone());
    if from == to {
        // A repeat naming the name the row already holds: any release still pending is finished first,
        // then a cutover's mark of the retained row, when it is still to be written.
        if let Some(row) = &known {
            if let Err(why) = crate::node_rows::finish_release(storage, me, row, "retry").await {
                return release_pending(me, node_id, &to, row.renamed_from.as_ref().unwrap_or(&from), &why, span);
            }
        }
        if let Some(c) = cutover {
            if let Err(why) = mark_inherited(storage, me, door.written(), c, node_id).await {
                return StatusReply::node_row_refused(&c.retained.to_string(), &why);
            }
        }
    } else {
        let Some(moved) = doors.moved.get() else {
            return StatusReply::NotReady { reason: format!("{me}: its view does not take a moved row yet; the rename of {} to {to} is refused and nothing is written", held.name) };
        };
        // A cutover's row carries the retained predecessor row's config and tags once; a repeated
        // cutover after the retained row already named this successor preserves the successor's own.
        let own = || known.map(|r| (r.config, r.tags)).unwrap_or_else(|| (held.config.clone(), Default::default()));
        let (config, tags) = match cutover {
            Some(c) => match retained_row(storage, me, c).await {
                Ok(r) if r.inherited_by.as_ref() == Some(node_id) => own(),
                Ok(r) => {
                    span.record("inherited_from", c.retained_id.as_str());
                    (r.config, r.tags)
                }
                Err(why) => {
                    let why = format!("{why}: the cutover of {} to {to} inherits nothing, and nothing is claimed or written", held.name);
                    span.record("detail", why.as_str());
                    return StatusReply::node_row_refused(&to.to_string(), &why);
                }
            },
            None => own(),
        };
        span.record("keys", config.len() as u64).record("tags", crate::node_rows::labels(&tags).as_str());
        let pending = match crate::node_rows::move_row(storage, me, &held, config, tags, &from, &to).await {
            Ok((m, written)) => {
                if let Some(row) = &written {
                    door.written().hold(row);
                }
                match m {
                    Moved::ReleasePending { why } => Some(why),
                    Moved::Renamed | Moved::AlreadyMoved => None,
                }
            }
            Err(RenameRefused::NameHeld(h)) => return StatusReply::NameHeld { name: h.name.to_string(), by: h.by },
            Err(RenameRefused::Unwritten(why)) => return StatusReply::node_row_refused(&to.to_string(), &why),
        };
        // The view reads the moved row: the birth is listed at the row's name from now on.
        moved(held.name.clone(), node_id.clone(), incarnation.clone(), to.clone()).await;
        if let Some(why) = pending {
            return release_pending(me, node_id, &to, &from, &why, span);
        }
        // The cutover's last write: the retained row's labels leave the tags index.
        if let Some(c) = cutover {
            if let Err(why) = mark_inherited(storage, me, door.written(), c, node_id).await {
                return StatusReply::node_row_refused(&c.retained.to_string(), &why);
            }
        }
    }
    let done = StatusReply::NameUpdated { node_id: node_id.clone(), incarnation: incarnation.clone() };
    if held.parked {
        // A stopped node is told nothing: it reads its name in its join answer at its next start.
        return done;
    }
    // The live birth takes the row's name. This admin itself takes it through its own name
    // decision in-process, as its config door applies its own config: it never calls itself.
    let applied = if held.node_id == *me_id {
        span.record("apply", "in-process");
        match doors.rename.get() {
            Some(rename) => {
                let (_, decision) = rename(to.clone()).await;
                Ok(crate::node_name::reply(&decision))
            }
            None => Ok(StatusReply::NotReady { reason: format!("{me}: its digest does not exist yet; the name {to} is not taken") }),
        }
    } else {
        span.record("apply", "direct");
        let birth = ExactBirth { target: NodeTarget::ExactNode(node_id.clone()), node_id: node_id.clone(), incarnation: incarnation.clone() };
        NodeRpc::new(door.client()).name_apply(&birth, &RouteChoice::Direct, &to, &CallOptions::default()).await
    };
    let (answered, detail) = match applied {
        Ok(r @ (StatusReply::Applied | StatusReply::AlreadyApplied)) => {
            span.record("answered", r.name());
            return done;
        }
        Ok(refused) => (refused.name(), format!("{me} moved the row to {to}; the birth answered {refused:?}; a resubmit resends the apply")),
        Err(end) => (end.outcome(), format!("{me} moved the row to {to}; apply-node-name ended {end:?}; a resubmit resends the apply")),
    };
    span.record("answered", answered).record("detail", detail.as_str());
    StatusReply::name_not_applied(&to.to_string(), answered, &detail)
}

/// The command ends incomplete after its put at `to`: the release of `from` is pending, both keys held
/// (proposal §2.4); the next writer that meets the row finishes it.
fn release_pending(me: &PathName, node_id: &rdm_core::NodeId, to: &PathName, from: &PathName, why: &str, span: &tracing::Span) -> StatusReply {
    let detail = format!("{me}: {node_id} holds {to} (its row carries renamed_from {from}) and the release of {from} is pending: {why}; a resubmit releases it and continues to the apply");
    span.record("answered", "release-pending").record("detail", detail.as_str());
    StatusReply::name_not_applied(&to.to_string(), "release-pending", &detail)
}

/// The predecessor's retained row a cutover inherits from: the row at `c.retained` while it is
/// `c.retained_id`'s, read through its `path.name` key; anything else is named.
async fn retained_row(storage: &dyn NodesStorage, me: &PathName, c: &Cutover) -> Result<crate::storage::NodeRow, String> {
    match storage.node_row_at(&c.retained).await {
        Ok(Some(r)) if r.node_id == c.retained_id => Ok(r),
        Ok(Some(r)) => Err(format!("{me}: the row at {} is node {}'s, not the retained predecessor {}'s", c.retained, r.node_id, c.retained_id)),
        Ok(None) => Err(format!("{me}: no row stands at {} for the retained predecessor {}", c.retained, c.retained_id)),
        Err(e) => Err(format!("{me}: the row at {} could not be read for the retained predecessor {}: {e}", c.retained, c.retained_id)),
    }
}

/// The cutover's last write (proposal node-properties-own-commands.md §8, replace: "the predecessor's
/// index entries gone"): the retained row at `c.retained` is put again at its own key carrying
/// `inherited_by: by`, its config and tags kept; the store takes every label of it out of the tags
/// index in that put (`NodeRow::tag_moves`). A row that says it already is not written again. One
/// `rdm.node_admin.node.update.via-inherited-row` span, its outcome `inherited`, `already-inherited` or
/// `row-refused`; answers the refusal's words.
async fn mark_inherited(storage: &dyn NodesStorage, me: &PathName, written: &crate::node_rows::WrittenRows, c: &Cutover, by: &rdm_core::NodeId) -> Result<(), String> {
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-inherited-row",
        node = %me,
        subject = %c.retained,
        node_id = %c.retained_id,
        inherited_by = %by,
        tags = tracing::field::Empty,
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let r = tracing::Instrument::instrument(
        async {
            let row = retained_row(storage, me, c).await?;
            span.record("tags", crate::node_rows::labels(&row.tags).as_str());
            if row.inherited_by.as_ref() == Some(by) {
                return Ok("already-inherited");
            }
            let row = crate::storage::NodeRow { inherited_by: Some(by.clone()), ..row };
            match storage.put_node_row(&row).await {
                Ok(Ok(())) => {
                    written.hold(&row);
                    Ok("inherited")
                }
                Ok(Err(held)) => Err(format!("{me}: nodes.storage refused the retained row at {}: {held}", c.retained)),
                Err(e) => Err(format!("{me}: nodes.storage refused the retained row at {}: {e}", c.retained)),
            }
        },
        span.clone(),
    )
    .await;
    match &r {
        Ok(outcome) => span.record("outcome", *outcome),
        Err(why) => span.record("outcome", "row-refused").record("detail", why.as_str()),
    };
    span.in_scope(|| tracing::info!("node.replace: the retained row's labels leave the tags index"));
    r.map(|_| ())
}
