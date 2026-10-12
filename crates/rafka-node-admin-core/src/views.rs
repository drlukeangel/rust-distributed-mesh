//! The reads over Node RPC (node-rpc-envelope.md "Typed objects mapped to wire and execution
//! owners"): `node.get`, `mesh.get` and `fabric.get` answered from a node-admin's view, and
//! `node.connections.get` answered by any node from the connections it holds as its own
//! (fabric-mesh-node-lifecycles.md: "Reading or cutting a node's held connections"). A read changes
//! nothing and publishes nothing; its answer is one Status reply, and an answer past the Status reply
//! bound is refused naming its size ([`StatusReply::within_reply_bound`]).
//!
//! The control API's routes answer from the same view and the same held connections
//! (`crate::http`): the fold of what a node holds is written once, here.

use crate::connections_writer::HeldConnections;
use crate::model::{Node, PathName};
use crate::topology::Topology;
use rafka_mesh_entity::connections::{ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
use rafka_mesh_entity::{GossipStats, NodeLoad};
use rafka_node_rpc_contract::status::{NodeSelect, StatusReply, StatusRequest};
use rafka_node_rpc_contract::views::{ConnectionView, ConnectionsView, FabricView, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Each member's CPU and RAM and mesh-channel counts from its latest digest, by node id: served
/// beside the view, never inside it (it changes every digest and never moves the topology).
pub type Loads = Arc<dyn Fn() -> BTreeMap<String, (Option<NodeLoad>, Option<GossipStats>)> + Send + Sync>;

fn node_status(s: crate::model::NodeStatus) -> NodeStatus {
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

/// `n` as a `node.get` answer carries it, with the load and gossip stats its latest digest carried.
pub fn node_view(n: &Node, load: Option<NodeLoad>, gossip: Option<GossipStats>) -> NodeView {
    NodeView {
        name: n.name.clone(),
        kind: n.kind,
        mesh: n.mesh.clone(),
        node_id: n.node_id.clone(),
        endpoint_id: n.endpoint_id.as_ref().map(|e| e.0.clone()),
        incarnation_id: n.incarnation_id.clone(),
        deployment_id: n.deployment_id.as_ref().map(|d| d.0.clone()),
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
        tags: Default::default(),
    }
}

fn mesh_view(m: crate::topology::MeshView) -> MeshView {
    MeshView { id: m.id, name: m.name, status: scope_status(m.status), primary_admin: m.primary_admin, admin_api_base: m.admin_api_base, nodes: m.nodes }
}

/// The births `view` holds that `select` selects, sorted by `path.name`, each with what `loads`
/// carries for it: every birth the control API's node list holds, replaced predecessors included.
pub fn nodes(view: &Topology, loads: &BTreeMap<String, (Option<NodeLoad>, Option<GossipStats>)>, select: &NodeSelect) -> Vec<NodeView> {
    let mut out: Vec<NodeView> = view
        .births()
        .filter(|n| match select {
            NodeSelect::Id(id) => &n.node_id == id,
            NodeSelect::Mesh(mesh) => &n.mesh == mesh,
            NodeSelect::Tags(_) => false,
        })
        .map(|n| {
            let (load, gossip) = loads.get(n.node_id.as_str()).cloned().unwrap_or((None, None));
            node_view(n, load, gossip)
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// The mesh `view` holds by the minted id or the name `mesh`, or every mesh it holds (`None`);
/// `None` when it holds no mesh by that id or name.
pub fn meshes(view: &Topology, mesh: Option<&str>) -> Option<Vec<MeshView>> {
    match mesh {
        Some(m) => view.mesh_view(m).map(|v| vec![mesh_view(v)]),
        None => Some(view.fabric_view().meshes.into_iter().map(mesh_view).collect()),
    }
}

/// The fabric as `view` holds it.
pub fn fabric(view: &Topology) -> FabricView {
    let f = view.fabric_view();
    FabricView {
        id: f.id,
        name: f.name,
        status: scope_status(f.status),
        provider: provider(f.provider),
        fabric_primary: f.fabric_primary,
        admin_api_base: f.admin_api_base,
        meshes: f.meshes.into_iter().map(mesh_view).collect(),
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

/// `node.get`, `mesh.get` or `fabric.get` answered by the node-admin `me` from its `view` and its
/// members' `loads`; `None` for any other request. Each read's span names what was asked and how much was
/// answered; a mesh the view does not hold is `UnknownMesh`, named.
pub fn view_answer(me: &PathName, view: &Topology, loads: &Loads, req: &StatusRequest) -> Option<StatusReply> {
    let reply = match req {
        StatusRequest::GetNodes { select } => {
            let nodes = nodes(view, &loads(), select);
            let count = nodes.len();
            let reply = StatusReply::within_reply_bound(StatusReply::Nodes { nodes }, "node.get");
            match &reply {
                StatusReply::ReadOverBound { bytes, max, .. } => tracing::info_span!("rdm.node_admin.node.reject.via-over-bound", node = %me, read = "node.get", select = select.name(), nodes = count, bytes, max, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the selected nodes encode past the Status reply bound; nothing of them is sent")),
                _ => tracing::info_span!("rdm.node_admin.node.serve.via-get", node = %me, select = select.name(), nodes = count, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the nodes the view holds answered")),
            }
            reply
        }
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
        StatusRequest::GetFabric => {
            let fabric = fabric(view);
            let count = fabric.meshes.len();
            let reply = StatusReply::within_reply_bound(StatusReply::Fabric { fabric }, "fabric.get");
            match &reply {
                StatusReply::ReadOverBound { bytes, max, .. } => tracing::info_span!("rdm.node_admin.fabric.reject.via-over-bound", node = %me, read = "fabric.get", meshes = count, bytes, max, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the fabric encodes past the Status reply bound; nothing of it is sent")),
                _ => tracing::info_span!("rdm.node_admin.fabric.serve.via-get", node = %me, meshes = count, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the fabric the view holds answered")),
            }
            reply
        }
        _ => return None,
    };
    Some(reply)
}
