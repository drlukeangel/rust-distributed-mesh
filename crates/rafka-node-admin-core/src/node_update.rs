//! `node.update` at the node's mesh-primary (R13; node-rpc-envelope.md "Meta and config"): the one
//! write path of a node's record. From what the mesh-primary holds, in order: the exact birth
//! matches its record; RDM-owned fields are unchanged and a changed name has no traffic
//! (`MetaFieldIsRdmOwned { field }`): the held node is stopped, or it is a live `<name>.new`
//! taking `<name>` (nothing resolves a `.new` name; the node's own `Busy` on the apply is the
//! in-flight proof); the name is claimed by insert-and-fail in the store adapter (`NameHeld { by }`,
//! nothing written) and the old key released; the view entry moves with the row; a live birth is
//! told `ApplyNodeName`, a stopped one nothing (its join answer hands it the name); `NodeUpdated`
//! answers only after the apply answered.

use crate::model::{Node, PathName};
use crate::storage::{NodeRow, NodesStorage};
use crate::topology::Topology;
use rafka_node_admin_client::{ExactBirth, MetaField, NodeMeta, NodeRpc, Traffic};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, RouteChoice};
use rafka_node_rpc_contract::status::{NotAuthority, StatusReply, StatusRequest};
use tokio::sync::RwLock;

/// Serve an `UpdateNode` at the admin `me` (born `me_id`): one span holds the whole put.
pub async fn update(me: &PathName, me_id: &rafka_mesh_entity::NodeId, view: &RwLock<Topology>, storage: &dyn NodesStorage, client: Option<&NodeRpcClient>, moved: &crate::status_rpc::RowMoved, req: &StatusRequest) -> StatusReply {
    let StatusRequest::UpdateNode { node_id, incarnation, name } = req else { unreachable!("update is called for UpdateNode only") };
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-update-node",
        node = %me, node_id = %node_id, incarnation_id = %incarnation.0, from = tracing::field::Empty, to = %name,
        traffic = tracing::field::Empty, outcome = tracing::field::Empty, answered = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
    );
    let reply = tracing::Instrument::instrument(put(me, me_id, view, storage, client, moved, node_id, incarnation, name, &span), span.clone()).await;
    span.record("outcome", reply.name());
    span.in_scope(|| tracing::info!("node.update decided"));
    reply
}

fn meta(n: &Node, name: PathName) -> NodeMeta {
    NodeMeta { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone(), endpoint: None, transport_addr: n.transport_addr, exe: None, runtime: None, status: rafka_node_admin_client::NodeStatus::Pending, name, mesh: n.mesh.clone(), kind: n.kind, otel_debug: false }
}

#[allow(clippy::too_many_arguments)]
async fn put(me: &PathName, me_id: &rafka_mesh_entity::NodeId, view: &RwLock<Topology>, storage: &dyn NodesStorage, client: Option<&NodeRpcClient>, moved: &crate::status_rpc::RowMoved, node_id: &rafka_mesh_entity::NodeId, incarnation: &rafka_mesh_entity::IncarnationId, name: &str, span: &tracing::Span) -> StatusReply {
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
    let traffic = if held.parked { Traffic::Stopped } else if held.name.new { Traffic::NothingRouted } else { Traffic::Routed };
    span.record("traffic", format!("{traffic:?}").as_str());
    if let Err(e) = meta(&held, to.clone()).check_update(&meta(&held, held.name.clone()), traffic) {
        return StatusReply::RejectedMetaFieldIsRdmOwned { field: e.field.name().into() };
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
        let row = NodeRow { name: to.clone(), node_id: held.node_id.clone(), status: held.status, config: held.config.clone() };
        match storage.claim_node_row(&row).await {
            Ok(Ok(_)) => {}
            Ok(Err(h)) => return StatusReply::NameHeld { name: h.name.to_string(), by: h.by },
            Err(e) => return StatusReply::node_row_refused(&to.to_string(), &format!("{me}: nodes.storage refused the claim of {to} for {node_id}: {e}")),
        }
        if let Err(e) = storage.release_node_row(&held.name, &held.node_id).await {
            return StatusReply::node_row_refused(&held.name.to_string(), &format!("{me}: {node_id} holds {to} and nodes.storage refused the release of {}: {e}", held.name));
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
