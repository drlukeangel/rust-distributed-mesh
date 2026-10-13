//! `node.update` at the node's mesh-primary (R13, R-TG1; node-rpc-envelope.md "Meta and config";
//! node-update.md): the one write path of a node's record, its name and its tags. From what the
//! mesh-primary holds, in order: the exact birth matches its record; RDM-owned fields are unchanged
//! (`MetaFieldIsRdmOwned { field }`); the tags are a node's tag set (`RejectedTags`); a changed
//! name needs the node not ready-for-traffic and holding no connection (R-CR1a, R-CR2): the held
//! node is stopped, or this mesh-primary holds the birth's `node.connections.deleted` of an `All`
//! cut made while it was not ready-for-traffic, leaving nothing held; any other rename is refused
//! `Busy` naming the held status and the connections, nothing claimed (the node's own `Busy` on the
//! apply is the node-side half). Then, one change at a time with the config door, through the
//! writer's held record: a changed name is claimed by insert-and-fail in the store adapter
//! (`NameHeld { by }`, nothing written) and the old key released, through the one writer of a
//! row's move (`node_rows::move_row`), and the view is projected again and reads the moved row; a
//! put that keeps the name is a blind put of the row at it. Either row carries the tags, and the
//! store keeps the tags index with it: a label put, a dropped label tombstoned. A put that changes
//! no name completes once the row is written (node-update.md step 7). A live birth whose name
//! changed is told `ApplyNodeName` (this admin itself in-process, through its own name decision),
//! a stopped one nothing (its join answer hands it the name); `NodeUpdated` answers only after the
//! apply answered.

use crate::model::{Node, PathName};
use crate::node_rows::RenameRefused;
use crate::storage::{NodeRow, NodesStorage};
use crate::topology::Topology;
use rafka_node_admin_client::{ExactBirth, MetaField, NodeMeta, NodeRpc, Traffic};
use rafka_node_rpc::{CallOptions, NodeTarget, RouteChoice};
use rafka_node_rpc_contract::status::{NotAuthority, StatusReply, StatusRequest};
use std::collections::BTreeSet;
use tokio::sync::RwLock;

/// What a mesh-primary's `node.update` acts through: the door its record changes take turns at
/// (`crate::node_config::NodeConfigs`: the writer's held record and the client it tells births
/// through), its view's take of a moved row, and its own name decision for a rename of itself.
pub struct UpdateDoors<'a> {
    /// The config door: `None` until this admin's node-RPC client exists.
    pub configs: Option<&'a crate::node_config::NodeConfigs>,
    /// This admin's view taking a moved row.
    pub moved: &'a crate::status_rpc::RowMoved,
    /// This admin deciding a name its own row gives it.
    pub rename: &'a crate::status_rpc::Rename,
}

pub use crate::node_properties::FirstReadyOnRoot;

/// Serve an `UpdateNode` at the admin `me` (born `me_id`): one span holds the whole put.
#[allow(clippy::too_many_arguments)]
pub async fn update(me: &PathName, me_id: &rafka_mesh_entity::NodeId, view: &RwLock<Topology>, storage: &dyn NodesStorage, doors: UpdateDoors<'_>, deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>, first_ready: FirstReadyOnRoot<'_>, req: &StatusRequest) -> StatusReply {
    let StatusRequest::UpdateNode { node_id, incarnation, name, tags } = req else { unreachable!("update is called for UpdateNode only") };
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-update-node",
        node = %me, node_id = %node_id, incarnation_id = %incarnation.as_str(), from = tracing::field::Empty, to = %name, tags = %crate::node_rows::labels(tags),
        tags_arrived = tracing::field::Empty, tags_left = tracing::field::Empty, traffic = tracing::field::Empty, connections = tracing::field::Empty, apply = tracing::field::Empty,
        outcome = tracing::field::Empty, answered = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
    );
    let reply = tracing::Instrument::instrument(
        async {
            match crate::node_properties::refused_before_first_ready(me, view, first_ready, "node.update", node_id, &span).await {
                Some(refused) => refused,
                None => put(me, me_id, view, storage, doors, deleted, node_id, incarnation, name, tags, &span).await,
            }
        },
        span.clone(),
    )
    .await;
    span.record("outcome", reply.name());
    span.in_scope(|| tracing::info!("node.update decided"));
    reply
}

/// Whether the held node is quiet for a rename (R-CR1a, R-CR2), from what this mesh-primary holds:
/// stopped, or its `node.connections.deleted` of an `All` cut made while it was not
/// ready-for-traffic, leaving nothing held. Answers the span's `(traffic, connections)` and, when
/// it is not quiet, why, naming the held status and the connections.
fn traffic_of(held: &Node, deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>) -> ((String, &'static str), Result<Traffic, String>) {
    if held.parked {
        return (("Stopped".into(), "stopped"), Ok(Traffic::Stopped));
    }
    match deleted {
        None => ((format!("{:?}", held.status), "none-deleted"), Err(format!("is {:?} and its mesh-primary holds no node.connections.deleted for this birth", held.status))),
        Some(r) if !r.all => ((format!("{:?}", r.status_at_cut), "partial"), Err(format!("is {:?}, and its last node.connections.delete named exact connections, not All: it holds no node.connections.deleted of every connection", r.status_at_cut))),
        Some(r) if !r.held.is_empty() => (
            (format!("{:?}", r.status_at_cut), "held"),
            Err(format!("is {:?} and still held {} connection(s) after its node.connections.delete: {}", r.status_at_cut, r.held.len(), r.held.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", "))),
        ),
        Some(r) => ((format!("{:?}", r.status_at_cut), "deleted"), Ok(Traffic::ConnectionsDeleted)),
    }
}

/// The meta entity this admin holds of `n`, at its own name.
pub(crate) fn meta_of(n: &Node) -> NodeMeta {
    meta(n, n.name.clone())
}

fn meta(n: &Node, name: PathName) -> NodeMeta {
    NodeMeta { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone(), endpoint: None, transport_addr: n.transport_addr, exe: None, runtime: None, status: rafka_node_admin_client::NodeStatus::Pending, name, mesh: n.mesh.clone(), kind: n.kind, otel_debug: false, tags: Default::default() }
}

#[allow(clippy::too_many_arguments)]
async fn put(
    me: &PathName,
    me_id: &rafka_mesh_entity::NodeId,
    view: &RwLock<Topology>,
    storage: &dyn NodesStorage,
    doors: UpdateDoors<'_>,
    deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>,
    node_id: &rafka_mesh_entity::NodeId,
    incarnation: &rafka_mesh_entity::IncarnationId,
    name: &str,
    tags: &BTreeSet<String>,
    span: &tracing::Span,
) -> StatusReply {
    let now = view.read().await.clone();
    // A subject this receiver does not hold is the receiver's own fact (`SubjectUnknown`), never a
    // peer that did not resolve.
    let Some(held) = now.nodes.iter().find(|n| n.node_id == *node_id).cloned() else {
        span.record("detail", format!("{me}: node {node_id} is not in the mesh-primary's view; its record cannot be put").as_str());
        return StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown };
    };
    span.record("from", held.name.to_string().as_str());
    if !now.own(me_id).is_some_and(|n| n.is_primary && n.mesh == held.mesh) {
        return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("the mesh-primary of {}", held.mesh) } };
    }
    if held.incarnation_id.as_ref() != Some(incarnation) {
        return match held.incarnation_id.clone() {
            Some(h) => StatusReply::RejectedStaleIncarnation { held: h },
            None => {
                span.record("detail", format!("{me}: {} has no known birth; its record cannot be put", held.name).as_str());
                StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }
            }
        };
    }
    let Ok(to) = name.parse::<PathName>() else {
        return StatusReply::RejectedMetaFieldIsRdmOwned { field: MetaField::Name.name().into() };
    };
    // The shape first: only a node to or from its `.old` or `.new` form is a rename at all.
    if let Err(e) = meta(&held, to.clone()).check_update(&meta(&held, held.name.clone()), Traffic::Stopped) {
        return StatusReply::RejectedMetaFieldIsRdmOwned { field: e.field.name().into() };
    }
    if let Some(reason) = rafka_node_rpc_contract::status::tags_refusal(tags) {
        let why = format!("{me}: the tags put on {} are refused and nothing is written: {reason}", held.name);
        span.record("detail", why.as_str());
        return StatusReply::rejected_tags(&why);
    }
    let (traffic, quiet) = traffic_of(&held, deleted);
    span.record("traffic", traffic.0.as_str()).record("connections", traffic.1);
    if to != held.name {
        if let Err(why) = quiet {
            return StatusReply::Busy { reason: format!("{me}: {} {why}; a name moves only when the node is stopped, or its node.connections.delete {{ All }} has completed (node-update.md \"Rename precondition\"): the rename to {to} is refused and nothing is claimed or written", held.name) };
        }
    }
    let Some(door) = doors.configs else {
        return StatusReply::NotReady { reason: format!("{me} has no node-RPC client yet, and so no door its record changes take turns at: the put of {} is refused and nothing is written", held.name) };
    };
    // One change to a node record at a time, through the writer's held record (node-update.md
    // "Config update"): the row carries every field the writer knows of it, the config included.
    let _one = door.one_at_a_time().await;
    let known = match door.written().known_row(storage, &held.mesh, node_id).await {
        Ok(row) => row,
        Err(why) => return StatusReply::node_row_refused(&held.name.to_string(), &format!("{me}: {why}: the put of {} is refused and nothing is written", held.name)),
    };
    let was: BTreeSet<String> = known.as_ref().map(|r| r.tags.clone()).unwrap_or_default();
    let config = known.map(|r| r.config).unwrap_or_else(|| held.config.clone());
    span.record("tags_arrived", crate::node_rows::labels(&tags.difference(&was).cloned().collect()).as_str());
    span.record("tags_left", crate::node_rows::labels(&was.difference(tags).cloned().collect()).as_str());
    if to == held.name {
        let row = NodeRow { name: held.name.clone(), node_id: node_id.clone(), status: held.status, config, tags: tags.clone(), allocated: None };
        match storage.put_node_row(&row).await {
            Ok(Ok(())) => door.written().hold(&row),
            Ok(Err(h)) => return StatusReply::NameHeld { name: h.name.to_string(), by: h.by },
            Err(e) => return StatusReply::node_row_refused(&held.name.to_string(), &format!("{me}: nodes.storage refused the row of {} ({node_id}): {e}", held.name)),
        }
        // A change that touches no name completes once the row is written (node-update.md step 7).
        return StatusReply::NodeUpdated { node_id: node_id.clone(), incarnation: incarnation.clone() };
    }
    let Some(moved) = doors.moved.get() else {
        return StatusReply::NotReady { reason: format!("{me}: its view does not take a moved row yet; the rename of {} to {to} is refused and nothing is written", held.name) };
    };
    match crate::node_rows::move_row(storage, me, &held, config, tags.clone(), &held.name, &to).await {
        Ok((_, written)) => {
            if let Some(row) = written {
                door.written().hold(&row);
            }
        }
        Err(RenameRefused::NameHeld(h)) => return StatusReply::NameHeld { name: h.name.to_string(), by: h.by },
        Err(RenameRefused::Unwritten(why)) => return StatusReply::node_row_refused(&to.to_string(), &why),
    }
    // The view reads the moved row: a stopped birth is listed at the row's name from now on.
    moved(held.name.clone(), node_id.clone(), incarnation.clone(), to.clone()).await;
    if held.parked {
        return StatusReply::NodeUpdated { node_id: node_id.clone(), incarnation: incarnation.clone() };
    }
    // The live birth takes the row's name. This admin itself takes it through its own name
    // decision in-process, as its config door applies its own config: it never calls itself.
    let applied = if held.node_id == *me_id {
        span.record("apply", "in-process");
        match doors.rename.get() {
            Some(rename) => {
                let (holds, decision) = rename(to.clone()).await;
                Ok(crate::node_name::reply(&holds, &to, &decision))
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
            return StatusReply::NodeUpdated { node_id: node_id.clone(), incarnation: incarnation.clone() };
        }
        Ok(refused) => (refused.name(), format!("{me} wrote the row; the birth answered {refused:?}")),
        Err(end) => (end.outcome(), format!("{me} wrote the row; apply-node-name ended {end:?}")),
    };
    span.record("answered", answered).record("detail", detail.as_str());
    StatusReply::name_not_applied(&to.to_string(), answered, &detail)
}
