//! `node.update` at the node's mesh-primary (R13; node-rpc-envelope.md "Meta and config"): the one
//! write path of a node's record. From what the mesh-primary holds, in order: the exact birth
//! matches its record; RDM-owned fields are unchanged (`MetaFieldIsRdmOwned { field }`); a changed
//! name needs the node not ready-for-traffic and holding no connection (R-CR1a, R-CR2): the held
//! node is stopped, or this mesh-primary holds the birth's `node.connections.deleted` of an `All`
//! cut made while it was not ready-for-traffic, leaving nothing held; any other rename is refused
//! `Busy` naming the held status and the connections, nothing claimed (the node's own `Busy` on the
//! apply is the node-side half); the name is claimed by insert-and-fail in the store adapter (`NameHeld { by }`,
//! nothing written) and the old key released, through the one writer of a row's move
//! (`node_rows::move_row`); the view entry moves with the row; a live birth is
//! told `ApplyNodeName`, a stopped one nothing (its join answer hands it the name); `NodeUpdated`
//! answers only after the apply answered.

use crate::model::{Node, NodeStatus, PathName};
use crate::node_rows::RenameRefused;
use crate::storage::NodesStorage;
use crate::topology::Topology;
use rafka_node_admin_client::{ExactBirth, MetaField, NodeMeta, NodeRpc, Traffic};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, RouteChoice};
use rafka_node_rpc_contract::status::{NotAuthority, StatusReply, StatusRequest};
use tokio::sync::RwLock;

/// Serve an `UpdateNode` at the admin `me` (born `me_id`): one span holds the whole put.
#[allow(clippy::too_many_arguments)]
pub async fn update(me: &PathName, me_id: &rafka_mesh_entity::NodeId, view: &RwLock<Topology>, storage: &dyn NodesStorage, client: Option<&NodeRpcClient>, moved: &crate::status_rpc::RowMoved, deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>, req: &StatusRequest) -> StatusReply {
    let StatusRequest::UpdateNode { node_id, incarnation, name } = req else { unreachable!("update is called for UpdateNode only") };
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-update-node",
        node = %me, node_id = %node_id, incarnation_id = %incarnation.0, from = tracing::field::Empty, to = %name,
        traffic = tracing::field::Empty, connections = tracing::field::Empty, outcome = tracing::field::Empty, answered = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
    );
    let reply = tracing::Instrument::instrument(put(me, me_id, view, storage, client, moved, deleted, node_id, incarnation, name, &span), span.clone()).await;
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
        Some(r) if r.status_at_cut == NodeStatus::ReadyForTraffic => (("ReadyForTraffic".into(), "deleted"), Err("had its connections deleted while ReadyForTraffic: it was not drained first".into())),
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
    NodeMeta { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone(), endpoint: None, transport_addr: n.transport_addr, exe: None, runtime: None, status: rafka_node_admin_client::NodeStatus::Pending, name, mesh: n.mesh.clone(), kind: n.kind, otel_debug: false }
}

#[allow(clippy::too_many_arguments)]
async fn put(
    me: &PathName,
    me_id: &rafka_mesh_entity::NodeId,
    view: &RwLock<Topology>,
    storage: &dyn NodesStorage,
    client: Option<&NodeRpcClient>,
    moved: &crate::status_rpc::RowMoved,
    deleted: Option<&crate::status_rpc::ConnectionsDeletedReceipt>,
    node_id: &rafka_mesh_entity::NodeId,
    incarnation: &rafka_mesh_entity::IncarnationId,
    name: &str,
    span: &tracing::Span,
) -> StatusReply {
    let now = view.read().await.clone();
    let Some(held) = now.nodes.iter().find(|n| n.node_id == *node_id).cloned() else {
        return StatusReply::PeerUnresolved { reason: format!("{me}: node {node_id} is not in the mesh-primary's view; its record cannot be put") };
    };
    span.record("from", held.name.to_string().as_str());
    if !now.own(me_id).is_some_and(|n| n.is_primary && n.mesh == held.mesh) {
        return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("the mesh-primary of {}", held.mesh) } };
    }
    if held.incarnation_id.as_ref() != Some(incarnation) {
        return match held.incarnation_id.clone() {
            Some(h) => StatusReply::RejectedStaleIncarnation { held: h },
            None => StatusReply::PeerUnresolved { reason: format!("{me}: {} has no known birth; its record cannot be put", held.name) },
        };
    }
    let Ok(to) = name.parse::<PathName>() else {
        return StatusReply::RejectedMetaFieldIsRdmOwned { field: MetaField::Name.name().into() };
    };
    // The shape first: only a node to or from its `.old` or `.new` form is a rename at all.
    if let Err(e) = meta(&held, to.clone()).check_update(&meta(&held, held.name.clone()), Traffic::Stopped) {
        return StatusReply::RejectedMetaFieldIsRdmOwned { field: e.field.name().into() };
    }
    let (traffic, quiet) = traffic_of(&held, deleted);
    span.record("traffic", traffic.0.as_str()).record("connections", traffic.1);
    if to != held.name {
        if let Err(why) = quiet {
            return StatusReply::Busy { reason: format!("{me}: {} {why}; a name moves only when the node is stopped, or drained with node.connections.deleted (R-CR1a): the rename to {to} is refused and nothing is claimed or written", held.name) };
        }
    }
    // A live birth is told the name: without a node-RPC client nothing is written.
    let client = match (held.parked, client) {
        (false, None) => return StatusReply::NotReady { reason: format!("{me} has no node-RPC client yet to tell the live {} its name; the rename to {to} is refused and nothing is written", held.name) },
        (_, c) => c,
    };
    if to != held.name {
        let Some(moved) = moved.get() else {
            return StatusReply::NotReady { reason: format!("{me}: its view does not take a moved row yet; the rename of {} to {to} is refused and nothing is written", held.name) };
        };
        match crate::node_rows::move_row(storage, me, &held, &held.name, &to).await {
            Ok(_) => {}
            Err(RenameRefused::NameHeld(h)) => return StatusReply::NameHeld { name: h.name.to_string(), by: h.by },
            Err(RenameRefused::Unwritten(why)) => return StatusReply::node_row_refused(&to.to_string(), &why),
        }
        // The view lists the birth at the row's name from now on, on every projection.
        moved(held.name.clone(), node_id.clone(), incarnation.clone(), to.clone()).await;
    }
    if held.parked {
        return StatusReply::NodeUpdated { node_id: node_id.clone(), incarnation: incarnation.clone() };
    }
    let Some(client) = client else { unreachable!("a live birth's rename was refused above without a client") };
    let birth = ExactBirth { target: NodeTarget::ExactNode(node_id.clone()), node_id: node_id.clone(), incarnation: incarnation.clone() };
    let (answered, detail) = match NodeRpc::new(client).name_apply(&birth, &RouteChoice::Direct, &to, &CallOptions::default()).await {
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
