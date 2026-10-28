//! The reads over Node RPC (node-rpc-envelope.md "Typed objects mapped to wire and execution
//! owners"; Q167: one read per entity, the filter a property match): `nodes.get`, `meshes.get` and
//! `fabrics.get` answered from a node-admin's view (a node's record, its config and tags among it,
//! from its atlas, never the node store, Q203), each the rows that match every pair of the read's
//! filter ([`rdm_core::filter`]) by a pass over what the admin holds, and `connections.get { node }`
//! answered by any node from the connections it holds as its own (fabric-mesh-node-lifecycles.md:
//! "Reading or cutting a node's held connections"). A read changes nothing and publishes nothing;
//! its answer is one Status reply, a filter the entity's schema refuses is `RejectedFilter` naming
//! the pair, and an answer past the Status reply bound is refused naming its size
//! ([`StatusReply::within_reply_bound`]).
//!
//! The control API's routes answer from the same view and the same held connections
//! (`crate::control_plane`): the fold of what a node holds is written once, here.

use crate::model::{Node, PathName};
use crate::topology::Topology;
use rdm_core::connections::{ConnectionsHeld, NodeConnection};
use rdm_core::{GossipStats, NodeLoad};
use crate::atlas::Atlas;
use rdm_core::filter::{Entity, Filter};
use rdm_core::status::StatusReply;
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
        S::Reserved5 => NodeStatus::Reserved5,
        S::Dead => NodeStatus::Dead,
        S::Deleted => NodeStatus::Deleted,
        // A withdrawn create's node never ran and no view holds it; its record, read anyway, is a node
        // whose name is released, as a Deleted one's is. The view's vocabulary does not move.
        S::Withdrawn => NodeStatus::Deleted,
        S::Starting => NodeStatus::Starting,
        S::ConnectionsDeleted => NodeStatus::ConnectionsDeleted,
        S::Stopping => NodeStatus::Stopping,
        S::Stopped => NodeStatus::Stopped,
        S::Deleting => NodeStatus::Deleting,
        S::Left => NodeStatus::Left,
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
    }
}

fn provider(p: crate::model::ProviderKind) -> ProviderKind {
    match p {
        crate::model::ProviderKind::Process => ProviderKind::Process,
        crate::model::ProviderKind::Container => ProviderKind::Container,
    }
}

/// `n` as a `node.get` answer carries it, with the load and gossip stats its latest digest carried,
/// and the tags and config its record carries (Q203: config is never gossiped).
pub fn node_view(n: &Node, load: Option<NodeLoad>, gossip: Option<GossipStats>, tags: BTreeSet<String>, config: BTreeMap<String, String>) -> NodeView {
    NodeView { cert: None, executable: None, storage_persistence: None, topology_storage_provider: None, types: Default::default(), held: Default::default(), held_hash: Default::default(),
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
        config,
        data_dir_lost: None,
        data_dir_unreadable: None,
        // The fact the view folded for the silent birth (deployment-provider.md §2.5): node.get, the
        // node.delete door and the plan read one source.
    }
}

fn mesh_view(m: crate::topology::MeshView) -> MeshView {
    MeshView { renewals: None, id: m.id, name: m.name, status: scope_status(m.status), primary_admin: m.primary_admin, admin_api_base: m.admin_api_base, nodes: m.nodes }
}

/// The paths the current Build plans in each mesh, by mesh: where `nodes.get` names a birth its view
/// no longer holds (deleted by its recovery and held unrecovered, its data dir unreadable), one point
/// read per path no birth of the view holds. `Err` names why they could not be read.
pub type Planned = Result<BTreeMap<String, Vec<PathName>>, String>;

/// The `data-dir-lost` fact of the birth `n`: on its own birth's facts, else on the birth its record
/// names as its predecessor (the exited birth it was recovered over), that record as `records` holds it.
fn data_dir_lost_of(records: &Atlas, n: &Node, row: Option<&crate::storage::NodeRow>) -> Option<DataDirLost> {
    let row = row?;
    let own = n.incarnation_id.as_ref().and_then(|i| row.birth(i)).and_then(|b| b.data_dir_lost.clone());
    let lost = match own {
        Some(l) => l,
        None => {
            let p = row.predecessor.as_ref()?;
            records.record(&p.node_id)?.birth(&p.incarnation_id)?.data_dir_lost.clone()?
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

/// Every birth `view` holds, sorted by `path.name`, each with what `loads` carries for it and what its
/// record in `records` carries (its config, tags, app types, cert dates and data-dir facts), replaced
/// predecessors included. A birth the view no longer holds is answered at a path `planned` names and
/// no birth holds, from the data-dir-unreadable fact on the record `records` holds there. Nothing
/// reads the node store (Q203).
fn nodes(view: &Topology, loads: &BTreeMap<String, (Option<NodeLoad>, Option<GossipStats>)>, records: &Atlas, planned: &BTreeMap<String, Vec<PathName>>) -> Vec<NodeView> {
    // Each birth as the atlas holds it: the name its record holds and the status its row last took,
    // never the gossip view's word on either (Q197, Q203, Q233); a birth an op deleted is not answered.
    let births: Vec<Node> = view
        .births()
        .filter_map(|n| {
            let mut n = n.clone();
            records.read(&mut n).then_some(n)
        })
        .collect();
    let mut out: Vec<NodeView> = births.iter().map(|n| {
        let row = records.record(&n.node_id);
        let (load, gossip) = loads.get(n.node_id.as_str()).cloned().unwrap_or((None, None));
        let mut v = node_view(n, load, gossip, row.as_ref().map(|r| r.tags.clone()).unwrap_or_default(), row.as_ref().map(|r| r.config.clone()).unwrap_or_default());
        // What this birth runs, as its row records it (proposal executable-bindings-at-create.md rule 7).
        v.executable = row.as_ref().and_then(|r| r.executable.clone());
        // Its storage_persistence and the topology_storage_provider it writes, as its row records them
        // (storage.md §1, §2): two properties under two names.
        v.storage_persistence = row.as_ref().and_then(|r| r.storage_persistence);
        v.topology_storage_provider = row.as_ref().and_then(|r| r.topology_storage_provider.clone());
        if let Some(declared) = row.as_ref().and_then(|r| r.app_types.clone()).filter(|t| Some(&t.incarnation) == n.incarnation_id.as_ref()) {
            v.types = declared.types;
            v.held = v.types.clone();
        }
        v.held_hash = rdm_core::app_types::hash(&v.held);
        // fabric-certs.md §Tracking: `node.get` surfaces the birth's held cert as `cert { issued_at,
        // expires_at, issuer_expires_at }`.
        v.cert = n.incarnation_id.as_ref().and_then(|i| row.as_ref()?.birth(i)?.cert.clone()).map(|c| rdm_core::views::NodeCertView { issued_at: c.issued_at, expires_at: c.expires_at, issuer_expires_at: c.issuer_expires_at });
        v.data_dir_lost = data_dir_lost_of(records, n, row.as_ref());
        v.data_dir_unreadable = data_dir_unreadable_of(n, row.as_ref());
        v
    }).collect();
    {
        let unheld: Vec<&PathName> = planned.values().flatten().filter(|p| !view.births().any(|n| &n.name == *p)).collect();
        for r in unheld.into_iter().filter_map(|p| records.record_at(p)) {
            let Some((incarnation, unreadable)) = r.births.iter().rev().find_map(|b| Some((b.incarnation_id.clone(), b.data_dir_unreadable.clone()?))) else { continue };
            if out.iter().any(|v| v.node_id == r.node_id || v.name == r.name) {
                continue;
            }
            let mut n = Node::allocated(r.name.clone());
            n.node_id = r.node_id.clone();
            n.incarnation_id = Some(incarnation);
            n.provider = Some(view.fabric.provider);
            n.data_dir = Some(unreadable.path.clone());
            n.status = crate::model::NodeStatus::Dead;
            let mut v = node_view(&n, None, None, r.tags.clone(), r.config.clone());
            v.executable = r.executable.clone();
            v.storage_persistence = r.storage_persistence;
            v.topology_storage_provider = r.topology_storage_provider.clone();
            v.data_dir_unreadable = Some(DataDirUnreadable { path: unreadable.path, cause: unreadable.cause });
            out.push(v);
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// A node's own whole row as its mesh-primary's atlas holds it (Q234: every node property; the
/// hydration sync's `OwnRow`): what `node.get` of the node answers from `view` and `records`, never the
/// node store; a birth `view` does not hold yet is answered from its record alone.
pub fn own_row_view(view: &Topology, records: &Atlas, row: &crate::storage::NodeRow) -> NodeView {
    if let Some(v) = nodes(view, &BTreeMap::new(), records, &BTreeMap::new()).into_iter().find(|v| v.node_id == row.node_id) {
        return v;
    }
    let mut n = Node::allocated(row.name.clone());
    n.node_id = row.node_id.clone();
    n.status = row.status;
    n.provider = Some(view.fabric.provider);
    if let Some(b) = row.newest_birth() {
        n.incarnation_id = Some(b.incarnation_id.clone());
        n.endpoint_id = b.endpoint_id.clone();
        n.transport_addr = b.transport_addr;
        n.listeners = b.listeners.clone();
    }
    let mut v = node_view(&n, None, None, row.tags.clone(), row.config.clone());
    v.executable = row.executable.clone();
    v.storage_persistence = row.storage_persistence;
    v.topology_storage_provider = row.topology_storage_provider.clone();
    if let Some(declared) = row.app_types.clone().filter(|t| Some(&t.incarnation) == n.incarnation_id.as_ref()) {
        v.types = declared.types;
        v.held = v.types.clone();
    }
    v.held_hash = rdm_core::app_types::hash(&v.held);
    v.cert = n.incarnation_id.as_ref().and_then(|i| row.birth(i)?.cert.clone()).map(|c| rdm_core::views::NodeCertView { issued_at: c.issued_at, expires_at: c.expires_at, issuer_expires_at: c.issuer_expires_at });
    v.data_dir_lost = data_dir_lost_of(records, &n, Some(row));
    v.data_dir_unreadable = data_dir_unreadable_of(&n, Some(row));
    v
}

/// `nodes.get` (Q167) answered by the node-admin `me` from its `view`, its nodes' `loads` and the node
/// records its atlas holds (`records`; never the node store, Q203): every birth that matches every
/// pair of `filter`, each with its record's properties, by `path.name`. A filter the node schema
/// refuses is `RejectedFilter` naming the pair, and nothing is read. Its span names the read, the
/// pairs and how many nodes were answered.
pub async fn nodes_answer(me: &PathName, view: &Topology, loads: &Loads, records: &Atlas, filter: &Filter) -> StatusReply {
    nodes_answer_with_status(me, view, loads, records, &Ok(BTreeMap::new()), filter, &|_| {}).await
}

/// Whether `n` matches `filter` with the labels it holds in the active tags index (lifecycles/
/// node-replace.md "Inherited": "The successor's tag-index entries use its own id. Retained predecessor
/// labels are removed from the active index, not erased from its row"): a record marked `inherited_by`
/// a successor, the retained `<name>.old` row, carries no active label, so a `tags` pair never names it;
/// the row it is answered with keeps its labels. Every other pair is matched on the row as answered.
fn matches_active(filter: &Filter, records: &Atlas, n: &NodeView) -> bool {
    let names_tags = filter.pairs.iter().any(|(p, _)| p == "tags");
    if names_tags && !n.tags.is_empty() && records.record(&n.node_id).is_some_and(|r| r.inherited_by.is_some()) {
        let mut active = n.clone();
        active.tags.clear();
        return filter.matches(&active);
    }
    filter.matches(n)
}

/// [`nodes_answer`] with `planned`, the current Build's paths, naming where a birth the view no
/// longer holds is answered from its record (R-FM1, R-FM2), and `amend`, what the answering admin
/// adds to each row before it is matched (the app types its own channels hold).
pub async fn nodes_answer_with_status(me: &PathName, view: &Topology, loads: &Loads, records: &Atlas, planned: &Planned, filter: &Filter, amend: &(dyn Fn(&mut NodeView) + Sync)) -> StatusReply {
    let read = Entity::Nodes.read();
    let span = tracing::info_span!(
        "rdm.node_admin.node.serve.via-get",
        node = %me,
        read,
        properties = %filter.properties(),
        pairs = %pairs(filter),
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
    let answer = async {
        if let Some(reason) = filter.refusal(Entity::Nodes) {
            return rdm_core::reply::status::bounded_rejected_filter(&reason);
        }
        let mut nodes = nodes(view, &loads(), records, &planned);
        nodes.iter_mut().for_each(amend);
        nodes.retain(|n| matches_active(filter, records, n));
        let count = nodes.len();
        span.record("nodes", count as u64);
        let reply = StatusReply::within_reply_bound(StatusReply::Nodes { nodes }, read);
        if let StatusReply::ReadOverBound { bytes, max, .. } = &reply {
            tracing::info_span!("rdm.node_admin.node.reject.via-over-bound", node = %me, read, properties = %filter.properties(), nodes = count, bytes, max, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the matching nodes encode past the Status reply bound; nothing of them is sent"));
        }
        reply
    };
    // What the read's store calls took (the blocking pool's queue and run).
    let io = crate::blocking::IoTime::default();
    let reply = crate::blocking::io_time_with(io.clone(), tracing::Instrument::instrument(answer, span.clone())).await;
    span.record("io_calls", io.calls()).record("io_queued_ms", io.queued_ms()).record("io_ms", io.ran_ms());
    span.record("outcome", reply.name());
    match &reply {
        StatusReply::ReadOverBound { bytes, max, .. } => {
            span.record("detail", format!("the matching nodes encode to {bytes} bytes, past the {max}-byte Status reply bound").as_str());
        }
        StatusReply::RejectedFilter { reason } => {
            span.record("detail", reason.as_str());
            span.in_scope(|| tracing::info!(read, "the node read refused by name"));
        }
        _ => span.in_scope(|| tracing::info!("the nodes the view holds answered")),
    }
    reply
}

/// A filter's pairs as a span names them: `(mesh, [mesh1]) (kind, [gateway])`.
pub fn pairs(filter: &Filter) -> String {
    filter.pairs.iter().map(|(p, v)| format!("({p}, [{}])", v.join(", "))).collect::<Vec<_>>().join(" ")
}

/// Every mesh `view` holds, by name.
pub fn meshes(view: &Topology) -> Vec<MeshView> {
    let mut meshes: Vec<MeshView> = view.fabric_view().meshes.into_iter().map(mesh_view).collect();
    meshes.sort_by(|a, b| a.name.cmp(&b.name));
    meshes
}

/// The fabric as `view` holds it, with `build_id`, its `Fabric.build_id` as the answering admin's
/// fabric record holds it.
pub fn fabric(view: &Topology, build_id: Option<String>) -> FabricView {
    let f = view.fabric_view();
    FabricView { next_expiry: None, topology_storage_provider: None, previous_topology_storage_providers: Vec::new(),
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

/// `meshes.get` (Q167) answered by the node-admin `me` from its `view`: every mesh it holds that
/// matches every pair of `filter`, by name, the empty list when none does (a mesh it does not hold
/// included). A filter the mesh schema refuses is `RejectedFilter` naming the pair. The span names
/// the pairs and how many meshes were answered.
pub fn meshes_answer(me: &PathName, view: &Topology, filter: &Filter) -> StatusReply {
    let read = Entity::Meshes.read();
    if let Some(reason) = filter.refusal(Entity::Meshes) {
        tracing::info_span!("rdm.node_admin.mesh.reject.via-filter", node = %me, read, properties = %filter.properties(), detail = %reason, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the filter names a pair the mesh schema refuses; nothing is read"));
        return rdm_core::reply::status::bounded_rejected_filter(&reason);
    }
    let meshes: Vec<MeshView> = meshes(view).into_iter().filter(|m| filter.matches(m)).collect();
    let count = meshes.len();
    let reply = StatusReply::within_reply_bound(StatusReply::Meshes { meshes }, read);
    match &reply {
        StatusReply::ReadOverBound { bytes, max, .. } => tracing::info_span!("rdm.node_admin.mesh.reject.via-over-bound", node = %me, read, properties = %filter.properties(), meshes = count, bytes, max, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the matching meshes encode past the Status reply bound; nothing of them is sent")),
        _ => tracing::info_span!("rdm.node_admin.mesh.serve.via-get", node = %me, read, properties = %filter.properties(), pairs = %pairs(filter), meshes = count, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the meshes the view holds answered")),
    }
    reply
}

/// `fabrics.get` (Q167) answered by the node-admin `me`: the fabric its `view` holds, with `build_id`,
/// its fabric record's `Fabric.build_id`, when it matches every pair of `filter`; else the empty list.
/// A filter the fabric schema refuses is `RejectedFilter` naming the pair. The span names the pairs,
/// the meshes and the Build answered.
pub fn fabric_answer(me: &PathName, view: &Topology, build_id: Option<String>, filter: &Filter) -> StatusReply {
    fabrics_reply(me, vec![fabric(view, build_id)], filter)
}

/// The `Fabrics` answer of `fabrics`, the fabric the answering admin holds with every field it fills,
/// filtered by `filter`.
fn fabrics_reply(me: &PathName, fabrics: Vec<FabricView>, filter: &Filter) -> StatusReply {
    let read = Entity::Fabrics.read();
    if let Some(reason) = filter.refusal(Entity::Fabrics) {
        tracing::info_span!("rdm.node_admin.fabric.reject.via-filter", node = %me, read, properties = %filter.properties(), detail = %reason, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the filter names a pair the fabric schema refuses; nothing is read"));
        return rdm_core::reply::status::bounded_rejected_filter(&reason);
    }
    let fabrics: Vec<FabricView> = fabrics.into_iter().filter(|f| filter.matches(f)).collect();
    let (count, meshes, build) = (fabrics.len(), fabrics.first().map_or(0, |f| f.meshes.len()), fabrics.first().and_then(|f| f.build_id.clone()).unwrap_or_default());
    let reply = StatusReply::within_reply_bound(StatusReply::Fabrics { fabrics }, read);
    match &reply {
        StatusReply::ReadOverBound { bytes, max, .. } => tracing::info_span!("rdm.node_admin.fabric.reject.via-over-bound", node = %me, read, meshes, bytes, max, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the fabric encodes past the Status reply bound; nothing of it is sent")),
        _ => tracing::info_span!("rdm.node_admin.fabric.serve.via-get", node = %me, read, properties = %filter.properties(), pairs = %pairs(filter), fabrics = count, meshes, build_id = %build, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the fabric the view holds answered")),
    }
    reply
}

/// [`fabric_answer`] with the cert dates the node records of `records`, this admin's atlas, hold
/// giving the next expiry across the fabric and each mesh whose renewals stall at `now_ms`, this
/// admin's rafka-time (fabric-certs.md §Tracking; no store read, query.md §1; none without an atlas),
/// and `record`, what the Fabric row adds (its topology_storage_provider and the providers it left,
/// storage.md §2).
pub fn fabric_answer_with_status(me: &PathName, view: &Topology, build_id: Option<String>, records: Option<&Atlas>, now_ms: Option<u64>, record: Option<(Option<String>, Vec<String>)>, filter: &Filter) -> StatusReply {
    let (next_expiry, stalled) = records.map(|r| crate::cert_renewal::fabric_watch(view, r, now_ms)).unwrap_or_default();
    let mut f = fabric(view, build_id);
    f.next_expiry = next_expiry;
    for m in f.meshes.iter_mut() {
        m.renewals = stalled.get(&m.name).map(|nodes| rdm_core::views::Renewals::Stalled { nodes: nodes.clone() });
    }
    if let Some((provider, previous)) = record {
        f.topology_storage_provider = provider;
        f.previous_topology_storage_providers = previous;
    }
    fabrics_reply(me, vec![f], filter)
}
