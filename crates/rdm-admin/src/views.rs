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

use crate::model::{Node, PathName};
use crate::topology::Topology;
use rdm_core::connections::{ConnectionsHeld, NodeConnection};
use rdm_core::{GossipStats, NodeLoad};
use crate::storage::NodesStorage;
use rdm_core::NodeId;
use rdm_core::status::{NodeSelect, StatusReply, StatusRequest, TagFilter};
use rdm_core::views::{DataDirLost, DataDirUnreadable, FabricView, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
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
        S::ConnectionsDeleting => NodeStatus::ConnectionsDeleting,
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
    NodeView { cert: None, executable: None, types: Default::default(), held: Default::default(), held_hash: Default::default(),
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
        stopped: n.stopped,
        load,
        gossip,
        tags,
        config: n.config.clone(),
        data_dir_lost: None,
        data_dir_unreadable: None,
        // The fact the view folded for the silent birth (deployment-provider.md §2.5): node.get, the
        // node.delete door and the plan read one source.
    }
}

fn mesh_view(m: crate::topology::MeshView) -> MeshView {
    MeshView { renewals: None, id: m.id, name: m.name, status: scope_status(m.status), primary_admin: m.primary_admin, admin_api_base: m.admin_api_base, nodes: m.nodes }
}

/// The paths the current Build plans in each mesh, by mesh: where `node.get` names a birth its view
/// no longer holds (deleted by its recovery and held unrecovered, its data dir unreadable), one point
/// read per path no birth of the view holds. `Err` names why they could not be read.
pub type Planned = Result<BTreeMap<String, Vec<PathName>>, String>;

/// Which births a `node.get` selects, once a tag filter's index reads are made.
enum Chosen<'a> {
    /// The node with this minted id.
    Id(&'a NodeId),
    /// Every node of this mesh.
    Mesh(&'a str),
    /// Every node of every mesh (`fabric.nodes.get`).
    Fabric,
    /// The nodes the tags index named for the filter.
    Ids(BTreeSet<NodeId>),
    /// The nodes of this mesh the tags index named for the filter.
    MeshIds(&'a str, BTreeSet<NodeId>),
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

/// The record of the birth `n`'s node: a point read of its key. None when it holds no record.
async fn row_of(storage: &dyn NodesStorage, n: &Node) -> Result<Option<crate::storage::NodeRow>, ReadFailed> {
    storage.node(&n.node_id).await.map_err(|e| ReadFailed { source: format!("the nodes store's records of {}", n.mesh), op: format!("read of the record of {} ({})", n.name, n.node_id), cause: e.to_string() })
}

/// The `data-dir-lost` fact of the birth `n`: on its own birth's facts, else on the birth its record
/// names as its predecessor (the exited birth it was recovered over), a point read of that record.
async fn data_dir_lost_of(facts: Option<&dyn NodesStorage>, n: &Node, row: Option<&crate::storage::NodeRow>) -> Option<DataDirLost> {
    let row = row?;
    let own = n.incarnation_id.as_ref().and_then(|i| row.birth(i)).and_then(|b| b.data_dir_lost.clone());
    let lost = match own {
        Some(l) => l,
        None => {
            let p = row.predecessor.as_ref()?;
            facts?.node(&p.node_id).await.ok().flatten()?.birth(&p.incarnation_id)?.data_dir_lost.clone()?
        }
    };
    Some(DataDirLost { path: lost.path, since: lost.since })
}

fn data_dir_unreadable_of(n: &Node, row: Option<&crate::storage::NodeRow>) -> Option<DataDirUnreadable> {
    if n.status == crate::model::NodeStatus::ReadyForTraffic {
        return None;
    }
    let u = row?.birth(n.incarnation_id.as_ref()?)?.data_dir_unreadable.clone()?;
    Some(DataDirUnreadable { path: u.path, cause: u.cause })
}

/// The births `view` holds that `chosen` selects, sorted by `path.name`, each with what `loads`
/// carries for it and what its record carries (its tags, app types, cert dates and data-dir facts):
/// every birth the view holds, replaced predecessors included, its reads point reads made at once. A
/// birth the view no longer holds is answered at a path `planned` names and no birth holds, from the
/// data-dir-unreadable fact on the record the `path.name` index names there. No read lists a store.
async fn nodes(view: &Topology, loads: &BTreeMap<String, (Option<NodeLoad>, Option<GossipStats>)>, storage: &dyn NodesStorage, facts: Option<&dyn NodesStorage>, chosen: &Chosen<'_>, planned: &BTreeMap<String, Vec<PathName>>) -> Result<Vec<NodeView>, ReadFailed> {
    let births: Vec<&Node> = view
        .births()
        .filter(|n| match chosen {
            Chosen::Id(id) => &n.node_id == *id,
            Chosen::Mesh(mesh) => n.mesh == *mesh,
            Chosen::Ids(ids) => ids.contains(&n.node_id),
            Chosen::MeshIds(mesh, ids) => n.mesh == *mesh && ids.contains(&n.node_id),
            Chosen::Fabric => true,
        })
        .collect();
    let read = births.iter().map(|n| async move {
        let row = row_of(storage, n).await?;
        let (load, gossip) = loads.get(n.node_id.as_str()).cloned().unwrap_or((None, None));
        let mut v = node_view(n, load, gossip, row.as_ref().map(|r| r.tags.clone()).unwrap_or_default());
        // What this birth runs, as its row records it (proposal executable-bindings-at-create.md rule 7).
        v.executable = row.as_ref().and_then(|r| r.executable.clone());
        if let Some(declared) = row.as_ref().and_then(|r| r.app_types.clone()).filter(|t| Some(&t.incarnation) == n.incarnation_id.as_ref()) {
            v.types = declared.types;
            v.held = v.types.clone();
        }
        v.held_hash = rdm_core::app_types::hash(&v.held);
        if facts.is_some() {
            // fabric-certs.md §Tracking: `node.get` surfaces the birth's held cert as `cert { issued_at,
            // expires_at, issuer_expires_at }`.
            v.cert = n.incarnation_id.as_ref().and_then(|i| row.as_ref()?.birth(i)?.cert.clone()).map(|c| rdm_core::views::NodeCertView { issued_at: c.issued_at, expires_at: c.expires_at, issuer_expires_at: c.issuer_expires_at });
            v.data_dir_lost = data_dir_lost_of(facts, n, row.as_ref()).await;
            v.data_dir_unreadable = data_dir_unreadable_of(n, row.as_ref());
        }
        Ok::<_, ReadFailed>(v)
    });
    let mut out = futures_util::future::try_join_all(read).await?;
    if let Some(s) = facts {
        let meshes: Vec<&String> = match chosen {
            Chosen::Mesh(m) | Chosen::MeshIds(m, _) => planned.keys().filter(|k| k.as_str() == *m).collect(),
            _ => planned.keys().collect(),
        };
        let unheld: Vec<&PathName> = meshes.into_iter().flat_map(|m| planned[m].iter()).filter(|p| !view.births().any(|n| &n.name == *p)).collect();
        let found = futures_util::future::join_all(unheld.iter().map(|p| s.node_at(p))).await;
        for r in found.into_iter().filter_map(|r| r.ok().flatten()) {
            let Some((incarnation, unreadable)) = r.births.iter().rev().find_map(|b| Some((b.incarnation_id.clone(), b.data_dir_unreadable.clone()?))) else { continue };
            let matches_chosen = match chosen {
                Chosen::Id(id) => &r.node_id == *id,
                Chosen::Mesh(m) => r.name.mesh == *m,
                Chosen::Ids(ids) => ids.contains(&r.node_id),
                Chosen::MeshIds(m, ids) => r.name.mesh == *m && ids.contains(&r.node_id),
                Chosen::Fabric => true,
            };
            if !matches_chosen || out.iter().any(|v| v.node_id == r.node_id || v.name == r.name) {
                continue;
            }
            let mut n = Node::allocated(r.name.clone());
            n.node_id = r.node_id.clone();
            n.incarnation_id = Some(incarnation);
            n.provider = Some(view.fabric.provider);
            n.data_dir = Some(unreadable.path.clone());
            n.status = crate::model::NodeStatus::Dead;
            n.config = r.config.clone();
            let mut v = node_view(&n, None, None, r.tags.clone());
            v.executable = r.executable.clone();
            v.data_dir_unreadable = Some(DataDirUnreadable { path: unreadable.path, cause: unreadable.cause });
            out.push(v);
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// The node reads (`node.get`, `mesh.nodes.get`, `fabric.nodes.get`; Q165) answered by the node-admin
/// `me` from its `view`, its nodes' `loads` and its nodes `storage`: the births `select` selects,
/// each with its tags. A tag filter is read from the tags
/// index, one point read per label; a filter that is not a tag set is `RejectedTags`, named; a read
/// that fails is `RejectedSourceFailed` naming the read. Its span names what was asked, the filter,
/// its labels, how many index reads were made and how many nodes were answered.
pub async fn nodes_answer(me: &PathName, view: &Topology, loads: &Loads, storage: &dyn NodesStorage, select: &NodeSelect) -> StatusReply {
    nodes_answer_with_status(me, view, loads, storage, None, &Ok(BTreeMap::new()), select).await
}

/// [`nodes_answer`] with `facts` supplying the durable facts on each birth (R-FM1,
/// R-FM2), and `planned`, the current Build's paths, naming where a birth the view no longer holds is
/// read.
pub async fn nodes_answer_with_status(me: &PathName, view: &Topology, loads: &Loads, storage: &dyn NodesStorage, facts: Option<&dyn NodesStorage>, planned: &Planned, select: &NodeSelect) -> StatusReply {
    let span = tracing::info_span!(
        "rdm.node_admin.node.serve.via-get",
        node = %me,
        read = select.read(),
        select = select.name(),
        filter = tracing::field::Empty,
        labels = tracing::field::Empty,
        index_reads = tracing::field::Empty,
        nodes = tracing::field::Empty,
        io_calls = tracing::field::Empty,
        io_queued_ms = tracing::field::Empty,
        io_ms = tracing::field::Empty,
        planned = tracing::field::Empty,
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    // A Build that cannot be read names no path: the births the view holds are answered, and the
    // span says why no birth it no longer holds could be.
    let planned = match planned {
        Ok(p) => p.clone(),
        Err(why) => {
            span.record("planned", format!("unreadable: {why}").as_str());
            BTreeMap::new()
        }
    };
    let read = async {
        let chosen = match select {
            NodeSelect::Id(id) => Chosen::Id(id),
            NodeSelect::Mesh(mesh) => Chosen::Mesh(mesh),
            NodeSelect::Fabric => Chosen::Fabric,
            NodeSelect::Tags(filter) | NodeSelect::MeshTags(_, filter) => {
                span.record("filter", filter.name()).record("labels", crate::node_rows::labels(filter.labels()).as_str());
                if let Some(reason) = filter.refusal() {
                    return rdm_core::reply::status::bounded_rejected_tags(&format!("{me}: {reason}"));
                }
                span.record("index_reads", filter.labels().len() as u64);
                match (tagged(me, storage, filter).await, select) {
                    (Ok(ids), NodeSelect::MeshTags(mesh, _)) => Chosen::MeshIds(mesh, ids),
                    (Ok(ids), _) => Chosen::Ids(ids),
                    (Err(f), _) => return rdm_core::reply::status::bounded_source_failed(&f.source, &f.op, &f.cause),
                }
            }
        };
        match nodes(view, &loads(), storage, facts, &chosen, &planned).await {
            Ok(nodes) => {
                let count = nodes.len();
                span.record("nodes", count as u64);
                let reply = StatusReply::within_reply_bound(StatusReply::Nodes { nodes }, select.read());
                if let StatusReply::ReadOverBound { bytes, max, .. } = &reply {
                    tracing::info_span!("rdm.node_admin.node.reject.via-over-bound", node = %me, read = select.read(), select = select.name(), nodes = count, bytes, max, "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("the selected nodes encode past the Status reply bound; nothing of them is sent"));
                }
                reply
            }
            Err(f) => rdm_core::reply::status::bounded_source_failed(&f.source, &f.op, &f.cause),
        }
    };
    // What the read's store calls took (the blocking pool's queue and run).
    let io = crate::blocking::IoTime::default();
    let reply = crate::blocking::io_time_with(io.clone(), tracing::Instrument::instrument(read, span.clone())).await;
    span.record("io_calls", io.calls()).record("io_queued_ms", io.queued_ms()).record("io_ms", io.ran_ms());
    span.record("outcome", reply.name());
    match &reply {
        StatusReply::ReadOverBound { bytes, max, .. } => {
            span.record("detail", format!("the selected nodes encode to {bytes} bytes, past the {max}-byte Status reply bound").as_str());
        }
        StatusReply::RejectedTags { reason } | StatusReply::RejectedSourceFailed { cause: reason, .. } => {
            span.record("detail", reason.as_str());
            span.in_scope(|| tracing::info!(read = select.read(), "the node read refused by name"));
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
    FabricView { next_expiry: None,
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

pub use rdm_node::node_self::{connection_view, connections_answer};

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
                    rdm_core::reply::status::unknown_mesh(mesh)
                }
                Some(meshes) => {
                    let count = meshes.len();
                    let reply = StatusReply::within_reply_bound(StatusReply::Meshes { meshes }, req.op());
                    match &reply {
                        StatusReply::ReadOverBound { bytes, max, .. } => tracing::info_span!("rdm.node_admin.mesh.reject.via-over-bound", node = %me, read = req.op(), select, meshes = count, bytes, max, "otel.kind" = "internal")
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

/// [`fabric_answer`] with the cert rows giving the next expiry across the fabric and each mesh whose
/// renewals stall at `now_ms`, this admin's rafka-time (fabric-certs.md §Tracking).
pub async fn fabric_answer_with_status(me: &PathName, view: &Topology, build_id: Option<String>, facts: &dyn NodesStorage, now_ms: Option<u64>) -> StatusReply {
    let (next_expiry, stalled) = crate::cert_renewal::fabric_watch(view, facts, now_ms).await;
    let mut reply = fabric_answer(me, view, build_id);
    if let StatusReply::Fabric { fabric } = &mut reply {
        fabric.next_expiry = next_expiry;
        for m in fabric.meshes.iter_mut() {
            m.renewals = stalled.get(&m.name).map(|nodes| rdm_core::views::Renewals::Stalled { nodes: nodes.clone() });
        }
    }
    reply
}
