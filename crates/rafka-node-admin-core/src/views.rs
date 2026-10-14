//! The reads over Node RPC (node-rpc-envelope.md "Typed objects mapped to wire and execution
//! owners"): `node.get`, `mesh.get` and `fabric.get` answered from a node-admin's view (a node's
//! tags read from its row, and `node.get`'s tag filter from the node store's tags index, R-TG1), and
//! `node.connections.get` answered by any node from the connections it holds as its own
//! (fabric-mesh-node-lifecycles.md: "Reading or cutting a node's held connections"). A read changes
//! nothing and publishes nothing; its answer is one Status reply, and an answer past the Status reply
//! bound is refused naming its size ([`StatusReply::within_reply_bound`]).
//!
//! The control API's routes answer from the same view and the same held connections
//! (`crate::control_plane`): the fold of what a node holds is written once, here.

use crate::connections_writer::HeldConnections;
use crate::model::{Node, PathName};
use crate::topology::Topology;
use rafka_mesh_entity::connections::{ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
use rafka_mesh_entity::{GossipStats, NodeLoad};
use crate::storage::NodesStorage;
use rafka_mesh_entity::NodeId;
use rafka_node_rpc_contract::status::{NodeSelect, StatusReply, StatusRequest, TagFilter};
use rafka_node_rpc_contract::views::{ConnectionView, ConnectionsView, FabricView, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Each member's CPU and RAM and mesh-channel counts from its latest digest, by node id: served
/// beside the view, never inside it (it changes every digest and never moves the topology).
pub type Loads = Arc<dyn Fn() -> BTreeMap<String, (Option<NodeLoad>, Option<GossipStats>)> + Send + Sync>;

pub(crate) fn node_status(s: crate::model::NodeStatus) -> NodeStatus {
    use crate::model::NodeStatus as S;
    match s {
        S::Pending => NodeStatus::Pending,
        S::ReadyForTraffic => NodeStatus::ReadyForTraffic,
        S::Draining => NodeStatus::Draining,
        S::Leaving => NodeStatus::Leaving,
        S::PendingReconnect => NodeStatus::PendingReconnect,
        S::Restarting => NodeStatus::Restarting,
        S::Dead => NodeStatus::Dead,
        S::Deleted => NodeStatus::Deleted,
    }
}

fn scope_status(s: crate::model::ScopeStatus) -> ScopeStatus {
    use crate::model::ScopeStatus as S;
    match s {
        S::Pending => ScopeStatus::Pending,
        S::ReadyForTraffic => ScopeStatus::ReadyForTraffic,
        S::Leaving => ScopeStatus::Leaving,
        S::Dead => ScopeStatus::Dead,
        S::Degraded => ScopeStatus::Degraded,
        S::StateSync => ScopeStatus::StateSync,
        S::StateCommit => ScopeStatus::StateCommit,
    }
}

fn provider(p: crate::model::ProviderKind) -> ProviderKind {
    match p {
        crate::model::ProviderKind::Process => ProviderKind::Process,
        crate::model::ProviderKind::Container => ProviderKind::Container,
    }
}

/// `n` as a `node.get` answer carries it, with the load and gossip stats its latest digest carried,
/// the tags its row carries and the config its digest carries (R-TG1: config rides the digest).
pub fn node_view(n: &Node, load: Option<NodeLoad>, gossip: Option<GossipStats>, tags: BTreeSet<String>) -> NodeView {
    NodeView {
        name: n.name.clone(),
        kind: n.kind,
        mesh: n.mesh.clone(),
        node_id: n.node_id.clone(),
        endpoint_id: n.endpoint_id.as_ref().map(|e| e.0.clone()),
        incarnation_id: n.incarnation_id.clone(),
        deployment_id: n.deployment_id.as_ref().map(|d| d.to_string()),
        provider: n.provider.map(provider),
        data_dir: n.data_dir.clone(),
        status: node_status(n.status),
        is_primary: n.is_primary,
        is_fabric_primary: n.is_fabric_primary,
        admin_api_base: n.admin_api_base.clone(),
        transport_addr: n.transport_addr,
        listeners: n.listeners.clone(),
        declared: n.declared.clone(),
        parked: n.parked,
        load,
        gossip,
        tags,
        config: n.config.clone(),
    }
}

fn mesh_view(m: crate::topology::MeshView) -> MeshView {
    MeshView { id: m.id, name: m.name, status: scope_status(m.status), primary_admin: m.primary_admin, admin_api_base: m.admin_api_base, nodes: m.nodes }
}

/// Which births a `node.get` selects, once a tag filter's index reads are made.
enum Chosen<'a> {
    /// The node with this minted id.
    Id(&'a NodeId),
    /// Every node of this mesh.
    Mesh(&'a str),
    /// The nodes the tags index named for the filter.
    Ids(BTreeSet<NodeId>),
}

/// Why a `node.get` read failed: what it read, and the store's error.
struct ReadFailed {
    source: String,
    op: String,
    cause: String,
}

/// The nodes a tag filter selects: one point read of the tags index per label (`NodesStorage::tagged`),
/// every label for `All` (the nodes in each), any for `Any` (the nodes in one at least). Never a read of
/// a node's row and never a pass over the view.
async fn tagged(me: &PathName, storage: &dyn NodesStorage, filter: &TagFilter) -> Result<BTreeSet<NodeId>, ReadFailed> {
    let mut picked: Option<BTreeSet<NodeId>> = None;
    for label in filter.labels() {
        let carrying = storage.tagged(label).await.map_err(|e| ReadFailed { source: format!("the tags index of {me}'s nodes store"), op: format!("read of the label {label}"), cause: e.to_string() })?;
        picked = Some(match (picked, filter) {
            (None, _) => carrying,
            (Some(p), TagFilter::All(_)) => p.intersection(&carrying).cloned().collect(),
            (Some(p), TagFilter::Any(_)) => p.union(&carrying).cloned().collect(),
        });
    }
    Ok(picked.unwrap_or_default())
}

/// The tags of the birth `n`: those of the row it holds, a point read through the node row's id
/// index and then the row's own key; none when it holds no row.
async fn tags_of(storage: &dyn NodesStorage, n: &Node) -> Result<BTreeSet<String>, ReadFailed> {
    let failed = |op: String, e: crate::record_store::StorageError| ReadFailed { source: format!("the nodes store's rows of {}", n.mesh), op, cause: e.to_string() };
    let Some(name) = storage.node_row_name(&n.mesh, &n.node_id).await.map_err(|e| failed(format!("read of the id index entry of {} ({})", n.name, n.node_id), e))? else { return Ok(BTreeSet::new()) };
    let row = storage.node_row_at(&name).await.map_err(|e| failed(format!("read of the row at {name}"), e))?;
    Ok(row.filter(|r| r.node_id == n.node_id).map(|r| r.tags).unwrap_or_default())
}

/// The births `view` holds that `chosen` selects, sorted by `path.name`, each with what `loads`
/// carries for it and the tags its row carries: every birth the view holds, replaced predecessors
/// included.
async fn nodes(view: &Topology, loads: &BTreeMap<String, (Option<NodeLoad>, Option<GossipStats>)>, storage: &dyn NodesStorage, chosen: &Chosen<'_>) -> Result<Vec<NodeView>, ReadFailed> {
    let births: Vec<&Node> = view
        .births()
        .filter(|n| match chosen {
            Chosen::Id(id) => &n.node_id == *id,
            Chosen::Mesh(mesh) => n.mesh == *mesh,
            Chosen::Ids(ids) => ids.contains(&n.node_id),
        })
        .collect();
    let mut out = Vec::with_capacity(births.len());
    for n in births {
        let (load, gossip) = loads.get(n.node_id.as_str()).cloned().unwrap_or((None, None));
        out.push(node_view(n, load, gossip, tags_of(storage, n).await?));
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// `node.get` answered by the node-admin `me` from its `view`, its members' `loads` and its nodes
/// `storage`: the births `select` selects, each with its tags. A tag filter is read from the tags
/// index, one point read per label; a filter that is not a tag set is `RejectedTags`, named; a read
/// that fails is `RejectedSourceFailed` naming the read. Its span names what was asked, the filter,
/// its labels, how many index reads were made and how many nodes were answered.
pub async fn nodes_answer(me: &PathName, view: &Topology, loads: &Loads, storage: &dyn NodesStorage, select: &NodeSelect) -> StatusReply {
    let span = tracing::info_span!(
        "rdm.node_admin.node.serve.via-get",
        node = %me,
        select = select.name(),
        filter = tracing::field::Empty,
        labels = tracing::field::Empty,
        index_reads = tracing::field::Empty,
        nodes = tracing::field::Empty,
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let read = async {
        let chosen = match select {
            NodeSelect::Id(id) => Chosen::Id(id),
            NodeSelect::Mesh(mesh) => Chosen::Mesh(mesh),
            NodeSelect::Tags(filter) => {
                span.record("filter", filter.name()).record("labels", crate::node_rows::labels(filter.labels()).as_str());
                if let Some(reason) = filter.refusal() {
                    return StatusReply::rejected_tags(&format!("{me}: {reason}"));
                }
                span.record("index_reads", filter.labels().len() as u64);
                match tagged(me, storage, filter).await {
                    Ok(ids) => Chosen::Ids(ids),
                    Err(f) => return StatusReply::source_failed(&f.source, &f.op, &f.cause),
                }
            }
        };
        match nodes(view, &loads(), storage, &chosen).await {
            Ok(nodes) => {
                let count = nodes.len();
                span.record("nodes", count as u64);
                let reply = StatusReply::within_reply_bound(StatusReply::Nodes { nodes }, "node.get");
                if let StatusReply::ReadOverBound { bytes, max, .. } = &reply {
                    tracing::info_span!("rdm.node_admin.node.reject.via-over-bound", node = %me, read = "node.get", select = select.name(), nodes = count, bytes, max, "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("the selected nodes encode past the Status reply bound; nothing of them is sent"));
                }
                reply
            }
            Err(f) => StatusReply::source_failed(&f.source, &f.op, &f.cause),
        }
    };
    let reply = tracing::Instrument::instrument(read, span.clone()).await;
    span.record("outcome", reply.name());
    match &reply {
        StatusReply::ReadOverBound { bytes, max, .. } => {
            span.record("detail", format!("the selected nodes encode to {bytes} bytes, past the {max}-byte Status reply bound").as_str());
        }
        StatusReply::RejectedTags { reason } | StatusReply::RejectedSourceFailed { cause: reason, .. } => {
            span.record("detail", reason.as_str());
            span.in_scope(|| tracing::info!("node.get refused by name"));
        }
        _ => span.in_scope(|| tracing::info!("the nodes the view holds answered")),
    }
    reply
}

/// The mesh `view` holds by the minted id or the name `mesh`, or every mesh it holds (`None`);
/// `None` when it holds no mesh by that id or name.
pub fn meshes(view: &Topology, mesh: Option<&str>) -> Option<Vec<MeshView>> {
    match mesh {
        Some(m) => view.mesh_view(m).map(|v| vec![mesh_view(v)]),
        None => Some(view.fabric_view().meshes.into_iter().map(mesh_view).collect()),
    }
}

/// The fabric as `view` holds it, with `build_id`, its `Fabric.build_id` as the answering admin's
/// fabric record holds it.
pub fn fabric(view: &Topology, build_id: Option<String>) -> FabricView {
    let f = view.fabric_view();
    FabricView {
        id: f.id,
        name: f.name,
        status: scope_status(f.status),
        provider: provider(f.provider),
        fabric_primary: f.fabric_primary,
        admin_api_base: f.admin_api_base,
        meshes: f.meshes.into_iter().map(mesh_view).collect(),
        build_id,
    }
}

/// Every connection fact a node-admin holds: the fleet's active Directs, with its own latest Directs
/// and Proxies over any older fact of the same index (the control API's connection list).
pub fn held_connections(held: &ConnectionsHeld) -> Vec<NodeConnection> {
    let mut facts: Vec<NodeConnection> = held.active_directs().into_iter().cloned().collect();
    for own in held.own_latest_directs().into_iter().chain(held.own_active_proxies()) {
        let key = own.index();
        match facts.iter().position(|f| f.index() == key) {
            Some(i) if facts[i].stamp() >= own.stamp() => {}
            Some(i) => facts[i] = own.clone(),
            None => facts.push(own.clone()),
        }
    }
    facts
}

/// One fact as a connection list carries it.
pub fn connection_view(f: &NodeConnection) -> ConnectionView {
    ConnectionView {
        id: f.id.to_string(),
        source: f.source.name.to_string(),
        destination: f.destination.name.to_string(),
        kind: match f.kind {
            ConnectionKind::Direct => "direct",
            ConnectionKind::Proxy => "proxy",
        }
        .into(),
        state: match f.state {
            ConnectionState::Connected => "connected",
            ConnectionState::Disconnected => "disconnected",
            ConnectionState::Failed => "failed",
        }
        .into(),
        carrier: f.carrier.as_ref().map(|c| c.name.to_string()),
        reason: f.reason.clone(),
        logged_at_ms: f.logged_at_ms,
    }
}

/// `node.connections.get` answered from `held`, one read of the node's connections writer: the
/// connections the node holds (its `connected` latest Directs and its active Proxies, each named by
/// the minted id `node.connections.delete` names it by), under the name it holds. Its span names the
/// node, how many it answered and whether its set is complete.
pub fn connections_answer(held: &HeldConnections) -> StatusReply {
    let view = ConnectionsView { node: held.name.to_string(), complete: held.complete, connections: held.routes.iter().map(connection_view).collect() };
    let (node, count, complete) = (&held.name, view.connections.len(), view.complete);
    let reply = StatusReply::within_reply_bound(StatusReply::Connections { connections: view }, "node.connections.get");
    match &reply {
        StatusReply::ReadOverBound { bytes, max, .. } => tracing::info_span!("rdm.node_admin.node.reject.via-over-bound", node = %node, read = "node.connections.get", connections = count, bytes, max, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the held connections encode past the Status reply bound; nothing of them is sent")),
        _ => tracing::info_span!("rdm.node_admin.node.serve.via-connections-get", node = %node, connections = count, complete, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the connections the node holds answered")),
    }
    reply
}

/// `mesh.get` answered by the node-admin `me` from its `view`; `None` for any other request
/// (`node.get` reads the nodes store too: [`nodes_answer`]; `fabric.get` its fabric record too:
/// [`fabric_answer`]). Each read's span names what was
/// asked and how much was answered; a mesh the view does not hold is `UnknownMesh`, named.
pub fn view_answer(me: &PathName, view: &Topology, req: &StatusRequest) -> Option<StatusReply> {
    let reply = match req {
        StatusRequest::GetMeshes { mesh } => {
            let select = if mesh.is_some() { "mesh" } else { "fabric" };
            match meshes(view, mesh.as_deref()) {
                None => {
                    let mesh = mesh.clone().unwrap_or_default();
                    tracing::info_span!("rdm.node_admin.mesh.reject.via-unknown-mesh", node = %me, mesh = %mesh, held = %view.meshes.iter().map(|m| m.name.as_str()).collect::<Vec<_>>().join(","), "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("the view holds no mesh by that id or name"));
                    StatusReply::UnknownMesh { mesh }
                }
                Some(meshes) => {
                    let count = meshes.len();
                    let reply = StatusReply::within_reply_bound(StatusReply::Meshes { meshes }, "mesh.get");
                    match &reply {
                        StatusReply::ReadOverBound { bytes, max, .. } => tracing::info_span!("rdm.node_admin.mesh.reject.via-over-bound", node = %me, read = "mesh.get", select, meshes = count, bytes, max, "otel.kind" = "internal")
                            .in_scope(|| tracing::info!("the selected meshes encode past the Status reply bound; nothing of them is sent")),
                        _ => tracing::info_span!("rdm.node_admin.mesh.serve.via-get", node = %me, select, meshes = count, "otel.kind" = "internal")
                            .in_scope(|| tracing::info!("the meshes the view holds answered")),
                    }
                    reply
                }
            }
        }
        _ => return None,
    };
    Some(reply)
}

/// `fabric.get` answered by the node-admin `me`: the fabric its `view` holds, with `build_id`, its
/// fabric record's `Fabric.build_id`. The span names the meshes and the Build answered.
pub fn fabric_answer(me: &PathName, view: &Topology, build_id: Option<String>) -> StatusReply {
    let fabric = fabric(view, build_id);
    let (count, build) = (fabric.meshes.len(), fabric.build_id.clone().unwrap_or_default());
    let reply = StatusReply::within_reply_bound(StatusReply::Fabric { fabric }, "fabric.get");
    match &reply {
        StatusReply::ReadOverBound { bytes, max, .. } => tracing::info_span!("rdm.node_admin.fabric.reject.via-over-bound", node = %me, read = "fabric.get", meshes = count, bytes, max, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the fabric encodes past the Status reply bound; nothing of it is sent")),
        _ => tracing::info_span!("rdm.node_admin.fabric.serve.via-get", node = %me, meshes = count, build_id = %build, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the fabric the view holds answered")),
    }
    reply
}
