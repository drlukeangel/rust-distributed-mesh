//! `node.update` at the node's mesh-primary (R13; node-rpc-envelope.md "Meta and config"): the one
//! write path of a node's record. From what the mesh-primary holds, in order: the exact birth
//! matches its record; RDM-owned fields are unchanged (`MetaFieldIsRdmOwned { field }`); a changed
//! name needs the node not ready-for-traffic and holding no connection (R-CR1a, R-CR2): the held
//! node is stopped, or this mesh-primary holds the birth's `node.connections.deleted` of an `All`
//! cut made while it was not ready-for-traffic, leaving nothing held; any other rename is refused
//! `Busy` naming the held status and the connections, nothing claimed (the node's own `Busy` on the
//! apply is the node-side half); the name is claimed by insert-and-fail in the store adapter (`NameHeld { by }`,
//! nothing written) and the old key released, through the one writer of a row's move
//! (`node_rows::move_row`); a node taking `<name>` from `<name>.new` carries the config of the
//! `<name>.old` row (lifecycles/node-replace.md step 4: "the mesh-primary seeds the successor's config
//! from the `<name>.old` row"); the view entry moves with the row; a live birth is
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
        traffic = tracing::field::Empty, connections = tracing::field::Empty, seeded_from = tracing::field::Empty, outcome = tracing::field::Empty, answered = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
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
        // A successor taking the name from `<name>.new` carries the predecessor's config, read from
        // the `<name>.old` row it left (lifecycles/node-replace.md step 4); any other rename keeps the
        // node's own config.
        let mut config = held.config.clone();
        if held.name.new && to == held.name.base() {
            let old = to.renamed();
            match storage.node_row_at(&old).await {
                Ok(Some(row)) => {
                    span.record("seeded_from", row.node_id.to_string().as_str());
                    config = row.config;
                }
                Ok(None) => {
                    span.record("seeded_from", "none");
                }
                Err(e) => return StatusReply::node_row_refused(&to.to_string(), &format!("{me}: the row at {old} could not be read to seed the config of {} taking {to}: {e}; nothing is written", held.node_id)),
            }
        }
        match crate::node_rows::move_row(storage, me, &held, config, &held.name, &to).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, NodeId, ProviderKind, ScopeStatus};
    use crate::storage::{MemoryNodesStorage, NodeRow};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn tier(t: &str) -> BTreeMap<String, String> {
        [("tier".to_string(), t.to_string())].into()
    }

    /// The mesh-primary `mesh1.admin.1`'s view holding `node` stopped, and a moved-row hook that
    /// takes the move.
    fn fixture(node: &Node) -> (PathName, NodeId, RwLock<Topology>, crate::status_rpc::RowMoved) {
        let mut me = Node::allocated("mesh1.admin.1".parse().unwrap());
        me.is_primary = true;
        me.is_fabric_primary = true;
        me.status = NodeStatus::ReadyForTraffic;
        me.incarnation_id = Some(IncarnationId::mint());
        let (name, id) = (me.name.clone(), me.node_id.clone());
        let view = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
            nodes: vec![me, node.clone()],
        };
        let moved: crate::status_rpc::RowMoved = Arc::new(std::sync::OnceLock::new());
        let _ = moved.set(Arc::new(|_, _, _, _| Box::pin(async {})));
        (name, id, RwLock::new(view), moved)
    }

    fn stopped(name: &str, config: BTreeMap<String, String>) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = NodeStatus::Leaving;
        n.parked = true;
        n.incarnation_id = Some(IncarnationId::mint());
        n.config = config;
        n
    }

    /// CONTRACT (lifecycles/node-replace.md step 4: "the mesh-primary seeds the successor's config
    /// from the `<name>.old` row"; R12/R13): node.update of the stopped successor from `<name>.new` to
    /// `<name>` at its mesh-primary writes its row at `<name>` carrying the config of the `<name>.old`
    /// row and lets go of `<name>.new`. A rename that is not a successor taking the name (the
    /// predecessor giving it up to `<name>.old`) keeps the node's own config. Must NOT happen: the
    /// successor taking the name with its own `.new` config while the predecessor's row holds the
    /// path's config.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_successor_taking_the_name_from_new_carries_the_old_rows_config() {
        let storage = MemoryNodesStorage::default();
        let successor = stopped("mesh1.rpc.1.new", tier("fresh"));
        let predecessor = NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::Leaving, config: tier("gold") };
        storage.put_node_row(&predecessor).await.unwrap().unwrap();
        storage.put_node_row(&NodeRow { name: successor.name.clone(), node_id: successor.node_id.clone(), status: NodeStatus::Leaving, config: tier("fresh") }).await.unwrap().unwrap();
        let (me, me_id, view, moved) = fixture(&successor);
        let req = StatusRequest::UpdateNode { node_id: successor.node_id.clone(), incarnation: successor.incarnation_id.clone().unwrap(), name: "mesh1.rpc.1".into() };
        let reply = update(&me, &me_id, &view, &storage, None, &moved, None, &req).await;
        assert_eq!(reply, StatusReply::NodeUpdated { node_id: successor.node_id.clone(), incarnation: successor.incarnation_id.clone().unwrap() });
        let row = storage.node_row_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap().expect("the successor holds the name");
        assert_eq!((row.node_id, row.config), (successor.node_id.clone(), tier("gold")), "the successor's row carries the .old row's config");
        assert_eq!(storage.node_row_at(&successor.name).await.unwrap(), None, "the successor let go of .new");

        // The predecessor giving up the name keeps its own config.
        let storage = MemoryNodesStorage::default();
        let held = stopped("mesh1.rpc.2", tier("silver"));
        storage.put_node_row(&NodeRow { name: held.name.clone(), node_id: held.node_id.clone(), status: NodeStatus::Leaving, config: tier("silver") }).await.unwrap().unwrap();
        let (me, me_id, view, moved) = fixture(&held);
        let req = StatusRequest::UpdateNode { node_id: held.node_id.clone(), incarnation: held.incarnation_id.clone().unwrap(), name: "mesh1.rpc.2.old".into() };
        assert!(matches!(update(&me, &me_id, &view, &storage, None, &moved, None, &req).await, StatusReply::NodeUpdated { .. }));
        let row = storage.node_row_at(&"mesh1.rpc.2.old".parse().unwrap()).await.unwrap().expect("the predecessor's row moved");
        assert_eq!(row.config, tier("silver"), "a rename that is not a successor taking the name keeps the node's own config");
    }
}
