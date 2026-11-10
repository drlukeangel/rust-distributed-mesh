//! The reads over Node RPC (node-rpc-envelope.md "Typed objects mapped to wire and execution
//! owners"; Q167: one read per entity, the filter a property match): `node.get`, `mesh.get` and
//! `fabric.get` answered from a node-admin's view (a node's lifecycle record read at its node
//! id and its config and tags from its own property record, query.md §1), each the rows that match every pair of the read's
//! filter ([`rdm_core::filter`]) by a pass over what the admin holds, and `node.connection.get { node }`
//! answered by any node from the connections it holds as its own (fabric-mesh-node-lifecycles.md:
//! "Reading or cutting a node's held connections"). A read changes nothing and publishes nothing;
//! a filter the entity's schema refuses is `RejectedFilter` naming the pair. The plural reads' rows
//! are answered whole, never bounded as one frame: op `0x23` streams them in frames (Q285,
//! [`crate::read_op`]).
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
use rdm_core::views::{FabricView, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Each member's CPU and RAM and mesh-channel counts from its latest digest, by node id: served
/// beside the view, never inside it (it changes every digest and never moves the topology).
pub type Loads = Arc<dyn Fn() -> BTreeMap<String, (Option<NodeLoad>, Option<GossipStats>)> + Send + Sync>;

pub(crate) fn node_status(s: crate::model::NodeStatus) -> NodeStatus {
    use crate::model::NodeStatus as S;
    match s {
        S::Pending => NodeStatus::Pending,
        S::Ready => NodeStatus::Ready,
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

/// `n` as a `node.get` answer carries it before its record is read, with the load and gossip stats its
/// latest digest carried and `tags` and `config` (Q203: config is never gossiped): the view's fields under
/// the record's names; a record overlays every field it holds ([`record_view`]).
pub fn node_view(n: &Node, load: Option<NodeLoad>, gossip: Option<GossipStats>, tags: BTreeSet<String>, config: BTreeMap<String, String>) -> NodeView {
    NodeView {
        node_id: n.node_id.clone(),
        path: n.path.clone(), name: n.name.clone(),
        r#type: n.r#type.clone(),
        incarnation: n.incarnation_id.clone(),
        state: node_status(n.status),
        endpoint_id: n.endpoint_id.as_ref().map(|e| e.0.clone()),
        transport_addr: n.transport_addr,
        listeners: n.listeners.clone(),
        runtime: None,
        is_primary: n.is_primary,
        is_fabric_primary: n.is_fabric_primary,
        cert: None,
        signing_cert: None,
        storage: rdm_core::meta::Storage { persistence: None, data_dir: n.data_dir.clone() },
        executable: None,
        config,
        tags,
        load,
        gossip,
    }
}

fn mesh_view(m: crate::topology::MeshView) -> MeshView {
    MeshView { renewals: None, id: m.id, name: m.name, status: m.status, primary_admin: m.primary_admin, nodes: m.nodes }
}

/// `n` with `load` and `gossip`, and every field its node record `row` holds (Q366), under the record's
/// names; the view's own word stands for a field the record does not hold (a node with no record yet).
/// `n`'s state is the one [`Atlas::read`] took from the record. Its role flags, `is_primary` and
/// `is_fabric_primary`, are the record's when the record is of `n`'s birth (query.md §1: "the status
/// and role flags the node writes in the same record"; Q323, Q356), [`RoleFrom::Record`]; a record of
/// another birth, or none, leaves the view's, [`RoleFrom::View`].
fn record_view(n: &Node, load: Option<NodeLoad>, gossip: Option<GossipStats>, row: Option<&crate::storage::NodeRecord>) -> (NodeView, RoleFrom) {
    let mut v = node_view(n, load, gossip, Default::default(), Default::default());
    let mut from = RoleFrom::View;
    if let Some(r) = row {
        if Some(&r.incarnation) == n.incarnation_id.as_ref() {
            v.is_primary = r.is_primary.is_some();
            v.is_fabric_primary = r.is_fabric_primary.is_some();
            from = RoleFrom::Record;
        }
        v.incarnation = Some(r.incarnation.clone());
        v.endpoint_id = Some(r.endpoint_id.0.clone());
        v.transport_addr = Some(r.transport_addr);
        v.listeners = r.listeners.clone();
        v.runtime = Some(r.runtime.clone());
        v.cert = r.cert.clone();
        v.signing_cert = r.signing_cert.clone();
        v.storage = r.storage.clone();
        v.executable = r.executable.clone();
        v.config = r.config.clone();
        v.tags = r.tags.clone();
    }
    (v, from)
}

/// Where an answered node's role flags came from: its record of its own birth, or the answering
/// admin's view (no record, a record of another birth, or a record claim the view's election settled
/// against, [`settle_claims`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoleFrom {
    /// The node's record of the birth answered.
    Record,
    /// The answering admin's view.
    View,
}

/// More than one record claiming one role, decided as a view decides between two claims (Q429: "by the
/// elections rule (heard, then lowest complete NodeId), never by a number"; elections.md: "A published
/// or recorded `is_primary` or `is_fabric_primary` bit is a projection of this result"): among the
/// answered nodes whose role flag is their record's, the claimants of `is_primary` of one node type in
/// one mesh, and of `is_fabric_primary`. One claimant keeps its record's flag. Of more, the claimant
/// `view`'s election names keeps it, else the lowest complete NodeId among them; the others answer
/// false, their role from the view. A primary killed before it could clear its own record is such a
/// claim beside the node elected after it.
fn settle_claims(view: &Topology, out: &mut [(NodeView, RoleFrom)]) {
    let named = |id: &rdm_core::NodeId, fabric: bool| view.births().any(|b| &b.node_id == id && if fabric { b.is_fabric_primary } else { b.is_primary });
    let mut roles: BTreeMap<Option<(String, String)>, Vec<usize>> = BTreeMap::new();
    for (i, (v, _)) in out.iter().enumerate().filter(|(_, (_, from))| *from == RoleFrom::Record) {
        if v.is_primary {
            roles.entry(Some((v.mesh(), v.r#type.clone()))).or_default().push(i);
        }
        if v.is_fabric_primary {
            roles.entry(None).or_default().push(i);
        }
    }
    for (role, claimants) in roles.into_iter().filter(|(_, c)| c.len() > 1) {
        let fabric = role.is_none();
        let keeps = claimants.iter().copied().find(|&i| named(&out[i].0.node_id, fabric)).unwrap_or_else(|| {
            let candidates: Vec<rdm_core::election::Candidate<'_>> = claimants.iter().map(|&i| rdm_core::election::Candidate { node_id: &out[i].0.node_id, ready: true }).collect();
            claimants[rdm_core::election::elect(&candidates).expect("more than one claimant")]
        });
        for i in claimants.into_iter().filter(|&i| i != keeps) {
            let (v, from) = &mut out[i];
            if fabric {
                v.is_fabric_primary = false;
            } else {
                v.is_primary = false;
            }
            *from = RoleFrom::View;
        }
    }
}

/// Every birth `view` holds, sorted by `(path, name)`, each with what `loads` carries for it and what its
/// record carries, stopped renamed nodes included: each record read through `current` (query.md §1: the
/// lifecycle record at its node id, never a copy an atlas holds), the lifecycle the ops
/// left each birth in from `atlas` ([`Atlas::read`]), and `is_primary` and `is_fabric_primary` as its
/// record holds them ([`record_view`]), more than one record claiming one role settled by the view's
/// election ([`settle_claims`]), each beside where its role flags came from. A node `ids` names that the view no longer holds is
/// answered from its record at that id, its latest line current, a Deleted one included (query.md §1:
/// "Historical records, including Deleted nodes, remain queryable by their own IDs"); an id with no
/// record is not answered. A `(path, name)` of `names` that no answered birth stands at is the
/// current-pair lookup's (query.md §1: "A current-pair lookup resolves the path and name to a node ID
/// from the node records"): the live record standing at it is answered from that record, whether the
/// view holds its birth or not; a pair no live record stands at is not answered. With no `current` (a
/// reader that holds no records) no record is read. `Err` names a record that could not be read
/// (query.md §1: "Record read errors remain named errors, not empty results or fabricated lifecycle
/// status").
async fn nodes(view: &Topology, loads: &BTreeMap<String, (Option<NodeLoad>, Option<GossipStats>)>, atlas: &Atlas, current: Option<&crate::current_record::CurrentRecords<'_>>, ids: &BTreeSet<crate::model::NodeId>, names: &BTreeSet<PathName>) -> Result<Vec<(NodeView, RoleFrom)>, String> {
    let mut out: Vec<(NodeView, RoleFrom)> = Vec::new();
    for b in view.births() {
        let row = match current {
            Some(c) => c.row(&b.node_id).await?,
            None => None,
        };
        // The name its record holds and the status its row last took, never the gossip view's word on
        // either (Q197, Q233); a birth an op deleted is not answered.
        let mut n = b.clone();
        if !atlas.read(&mut n, row.as_ref()) {
            continue;
        }
        let (load, gossip) = loads.get(n.node_id.as_str()).cloned().unwrap_or((None, None));
        out.push(record_view(&n, load, gossip, row.as_ref()));
    }
    if let Some(c) = current {
        // A node the filter names by id that the view no longer holds: its record at that id, never
        // the pair's claimant (query.md §1: "a history query does not redirect to the current pair's
        // claimant").
        for id in ids {
            if view.births().any(|n| &n.node_id == id) || out.iter().any(|(v, _)| &v.node_id == id) {
                continue;
            }
            let Some(row) = c.row(id).await? else { continue };
            out.push(row_view(view, atlas, &row));
        }
        // A (path, name) the filter names that no birth answered here stands at: the node the node
        // records' current-pair lookup names there, read at its record (query.md §1), never the gossip
        // view's word on whether that node is there.
        for at in names {
            if out.iter().any(|(v, _)| &v.location() == at) {
                continue;
            }
            let Some(row) = c.row_at(at).await? else { continue };
            if out.iter().any(|(v, _)| v.node_id == row.node_id) {
                continue;
            }
            out.push(row_view(view, atlas, &row));
        }
    }
    settle_claims(view, &mut out);
    out.sort_by(|(a, _), (b, _)| a.location().cmp(&b.location()));
    Ok(out)
}

/// A node's own whole record as its mesh-primary answers it (Q234: every node property; the hydration
/// sync's `OwnRow`): the birth `view` holds of `row`'s node, as `row`, the node's record, carries it and
/// the ops left it (`atlas`), its role flags the record's ([`record_view`]), as [`nodes`] reads them; a
/// birth `view` does not hold yet is answered from its record alone. No store read here: the caller
/// read `row`.
pub fn own_row_view(view: &Topology, atlas: &Atlas, row: &crate::storage::NodeRecord) -> NodeView {
    row_view(view, atlas, row).0
}

/// [`own_row_view`] of `row` in `view`, beside where its role flags came from.
fn row_view(view: &Topology, atlas: &Atlas, row: &crate::storage::NodeRecord) -> (NodeView, RoleFrom) {
    if let Some(b) = view.births().find(|n| n.node_id == row.node_id) {
        let mut n = b.clone();
        if atlas.read(&mut n, Some(row)) {
            return record_view(&n, None, None, Some(row));
        }
    }
    let mut n = Node::allocated(row.location(), row.mesh(), &row.r#type);
    n.node_id = row.node_id.clone();
    n.status = row.state;
    n.provider = Some(view.fabric.provider);
    n.is_primary = row.is_primary.is_some();
    n.is_fabric_primary = row.is_fabric_primary.is_some();
    (record_view(&n, None, None, Some(row)).0, RoleFrom::Record)
}

/// `node.get` (Q167) answered by the node-admin `me` from its `view`, its nodes' `loads` and the
/// lifecycle the ops left each birth in (`records`, its atlas): every birth that matches every pair of
/// `filter`, by `(path, name)`. A filter the node schema refuses is `RejectedFilter` naming the pair,
/// and nothing is read. Its span names the read, the pairs and how many nodes were answered. It takes
/// `current`, the reader each birth's record is read through (query.md §1: its
/// lifecycle record and its properties at its own node ID; never a copy an
/// atlas holds). A record that cannot be read answers `NotReady` naming it.
pub async fn nodes_answer_with_status(me: &PathName, view: &Topology, loads: &Loads, records: &Atlas, filter: &Filter, current: Option<crate::current_record::CurrentRecords<'_>>) -> StatusReply {
    let read = Entity::Nodes.read();
    let span = tracing::info_span!(
        "rdm.node_admin.node.serve.via-get",
        node_path = %me.path, node_name = %me.name,
        read,
        properties = %filter.properties(),
        pairs = %pairs(filter),
        nodes = tracing::field::Empty,
        io_calls = tracing::field::Empty,
        io_queued_ms = tracing::field::Empty,
        io_ms = tracing::field::Empty,
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let answer = async {
        if let Some(reason) = filter.refusal(Entity::Nodes) {
            return rdm_core::reply::status::bounded_rejected_filter(&reason);
        }
        // The ids the filter names (its `id` pairs, never a negated one): each read at its record when
        // the view no longer holds it.
        let ids: BTreeSet<crate::model::NodeId> = filter.pairs.iter().filter(|(property, _)| property == "id").flat_map(|(_, values)| values).filter_map(|v| crate::model::NodeId::parse(v).ok()).collect();
        // The (path, name) pairs the filter names (its `path` and `name` pairs, never a negated one):
        // each resolved at the node records' current-pair lookup when no birth the view holds stands there.
        let named = |property: &str| -> Vec<&String> { filter.pairs.iter().filter(|(p, _)| p == property).flat_map(|(_, values)| values).collect() };
        let names: BTreeSet<PathName> = named("path").into_iter().flat_map(|path| named("name").into_iter().filter_map(move |name| PathName::new(path, name).ok())).collect();
        let mut nodes = match nodes(view, &loads(), records, current.as_ref(), &ids, &names).await {
            Ok(nodes) => nodes,
            Err(why) => return rdm_core::reply::status::not_ready(format!("path {:?}, name {:?}: {why}", me.path, me.name)),
        };
        nodes.retain(|(n, _)| filter.matches(n));
        let count = nodes.len();
        span.record("nodes", count as u64);
        StatusReply::Nodes { nodes: nodes.into_iter().map(|(n, _)| n).collect() }
    };
    // What the read's store calls took (the blocking pool's queue and run).
    let io = crate::blocking::IoTime::default();
    let reply = crate::blocking::io_time_with(io.clone(), tracing::Instrument::instrument(answer, span.clone())).await;
    span.record("io_calls", io.calls()).record("io_queued_ms", io.queued_ms()).record("io_ms", io.ran_ms());
    span.record("outcome", reply.name());
    match &reply {
        StatusReply::RejectedFilter { reason } => {
            span.record("detail", reason.as_str());
            span.in_scope(|| tracing::info!(read, "the node read refused by name"));
        }
        _ => span.in_scope(|| tracing::info!("the nodes the view holds answered")),
    }
    reply
}

/// A filter's pairs as a span names them: `(mesh, [mesh1]) (type, [gateway])`.
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
    FabricView { next_expiry: None, topology_storage_provider: None, roots: None, ca: None, previous_topology_storage_providers: Vec::new(),
        id: f.id,
        name: f.name,
        status: f.status,
        provider: f.provider,
        fabric_primary: f.fabric_primary,
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

/// `mesh.get` (Q167) answered by the node-admin `me` from its `view`: every mesh it holds that
/// matches every pair of `filter`, by name, the empty list when none does (a mesh it does not hold
/// included). A filter the mesh schema refuses is `RejectedFilter` naming the pair. The span names
/// the pairs and how many meshes were answered.
pub fn meshes_answer(me: &PathName, view: &Topology, filter: &Filter) -> StatusReply {
    let read = Entity::Meshes.read();
    if let Some(reason) = filter.refusal(Entity::Meshes) {
        tracing::info_span!("rdm.node_admin.mesh.reject.via-filter", node_path = %me.path, node_name = %me.name, read, properties = %filter.properties(), detail = %reason, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the filter names a pair the mesh schema refuses; nothing is read"));
        return rdm_core::reply::status::bounded_rejected_filter(&reason);
    }
    let meshes: Vec<MeshView> = meshes(view).into_iter().filter(|m| filter.matches(m)).collect();
    let count = meshes.len();
    tracing::info_span!("rdm.node_admin.mesh.serve.via-get", node_path = %me.path, node_name = %me.name, read, properties = %filter.properties(), pairs = %pairs(filter), meshes = count, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the meshes the view holds answered"));
    StatusReply::Meshes { meshes }
}

/// `fabric.get` (Q167) answered by the node-admin `me`: the fabric its `view` holds, with `build_id`,
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
        tracing::info_span!("rdm.node_admin.fabric.reject.via-filter", node_path = %me.path, node_name = %me.name, read, properties = %filter.properties(), detail = %reason, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the filter names a pair the fabric schema refuses; nothing is read"));
        return rdm_core::reply::status::bounded_rejected_filter(&reason);
    }
    let fabrics: Vec<FabricView> = fabrics.into_iter().filter(|f| filter.matches(f)).collect();
    let (count, meshes, build) = (fabrics.len(), fabrics.first().map_or(0, |f| f.meshes.len()), fabrics.first().and_then(|f| f.build_id.clone()).unwrap_or_default());
    tracing::info_span!("rdm.node_admin.fabric.serve.via-get", node_path = %me.path, node_name = %me.name, read, properties = %filter.properties(), pairs = %pairs(filter), fabrics = count, meshes, build_id = %build, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the fabric the view holds answered"));
    StatusReply::Fabrics { fabrics }
}

/// [`fabric_answer`] with the cert dates `records` hold (the current records of the births of the meshes
/// this admin is primary for, read through the reader, query.md §1; none: no cert summary) giving the
/// next expiry across the fabric and each mesh whose renewals stall at `now_ms`, this admin's
/// rafka-time (fabric-certs.md §Tracking), and `record`, what the Fabric row adds (its
/// topology_storage_provider and the providers it left, storage.md §2).
pub fn fabric_answer_with_status(me: &PathName, view: &Topology, build_id: Option<String>, records: Option<&[crate::storage::NodeRecord]>, now_ms: Option<u64>, record: Option<&crate::fabric_storage::FabricRecord>, filter: &Filter) -> StatusReply {
    let (next_expiry, stalled) = records.map(|r| crate::cert_renewal::fabric_watch(view, r, now_ms)).unwrap_or_default();
    let mut f = fabric(view, build_id);
    f.next_expiry = next_expiry;
    for m in f.meshes.iter_mut() {
        m.renewals = stalled.get(&m.name).map(|nodes| rdm_core::views::Renewals::Stalled { nodes: nodes.clone() });
    }
    if let Some(record) = record {
        f.topology_storage_provider = record.topology_storage_provider.clone();
        f.previous_topology_storage_providers = record.previous_topology_storage_providers.clone();
        // The rotation and the CA as the Fabric row names them; a rotation not updated by the old
        // anchor's bound is stalled (fabric-ca-renew.md "fabric.root.update, the caller op").
        f.roots = record.roots.as_ref().map(|r| rdm_core::views::RootsView {
            phase: r.phase,
            expires_at: r.expires_at,
            stalled: r.phase != rdm_core::status::RootsPhase::Updated && now_ms.is_some_and(|now| r.expires_at > 0 && now >= r.expires_at),
        });
        f.ca = record.ca.as_ref().map(|c| rdm_core::views::CaView { expires_at: c.expires_at });
    }
    fabrics_reply(me, vec![f], filter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, Node, NodeId, NodeStatus as ModelNodeStatus, PathName, ProviderKind as ModelProviderKind, ScopeStatus as ModelScopeStatus};
    use crate::span_capture::Capture;
    use crate::storage::{CertDates, NodeRecord};
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:38
    /// "| Reply | `fabric.root.updated` once the rotation is accepted (the roots row `minted`); its progress is `fabric.get`'s `roots { phase, expires_at }`. Refused by name while a rotation is in flight (`rotation-in-progress`) and for a root already trusted (`root-already-trusted`) |"
    /// CONTRACT: docs/architecture/lifecycles/fabric-ca-renew.md:41
    /// "| Bound | The old anchor's own expiry: a rotation not updated by then is surfaced on `fabric.get` as `roots: stalled` |"
    /// fabric-ca-renew.md ("its progress is `fabric.get`'s `roots { phase, expires_at }`"; "a rotation
    /// not updated by then is surfaced on `fabric.get` as `roots: stalled`"): the Fabric row's roots and
    /// CA rows are answered; a rotation past its bound and not updated is stalled, an updated one never.
    #[test]
    fn fabrics_get_carries_the_rotation_and_the_ca_and_names_a_rotation_past_its_bound_stalled() {
        use rdm_core::status::RootsPhase;
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let view = Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".to_string(), status: ModelScopeStatus::Ready, provider: ModelProviderKind::Process }, meshes: Vec::new(), nodes: Vec::new() };
        let row = |phase: RootsPhase| crate::fabric_storage::FabricRecord {
            fabric_id: FabricId::mint(), name: "fabric1".into(), state: crate::fabric_state::FabricState::Pending, types: Vec::new(), provider: None, topology_storage_provider: None, previous_topology_storage_providers: Vec::new(),
            ca: Some(crate::fabric_storage::CaRow { expires_at: 9_000, previous_expires_at: 5_000, issuer_receipts: Default::default() }),
            roots: Some(crate::fabric_storage::RootsRow { phase, roots: vec!["aa".into(), "bb".into()], expires_at: 3_000 }),
        };
        let answer = |phase: RootsPhase, now: u64| match fabric_answer_with_status(&me, &view, None, None, Some(now), Some(&row(phase)), &Filter::new()) {
            StatusReply::Fabrics { mut fabrics } => fabrics.pop().expect("one fabric"),
            other => panic!("expected Fabrics, got {other:?}"),
        };
        let early = answer(RootsPhase::Distributed, 2_000);
        assert_eq!(early.roots, Some(rdm_core::views::RootsView { phase: RootsPhase::Distributed, expires_at: 3_000, stalled: false }));
        assert_eq!(early.ca, Some(rdm_core::views::CaView { expires_at: 9_000 }));
        assert!(answer(RootsPhase::CaRotated, 3_000).roots.unwrap().stalled, "past its bound and not updated: stalled");
        assert!(!answer(RootsPhase::Updated, 4_000).roots.unwrap().stalled, "an updated rotation is never stalled");
    }

    #[test]
    fn plural_reads_match_current_records_match_each_nodes_own_tags_and_emit_serve_and_reject_spans() {
        // docs/architecture/query.md:11,52 — "A caller outside the node sends `fabric.get`,
        // `mesh.get` and `node.get` to the fabric-primary, which answers from its atlas:
        // every mesh's node records, held in memory... Property: any public field of the row
        // by its page name... Refused: an unknown property, a value the field cannot take, an
        // empty values list or a field that is not public is `INVALID_ARGUMENT`, naming the pair."
        let capture = Capture::new("views-unit-reads");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");

        rt.block_on(async {
            let me = PathName::numbered("mesh1", "node-admin", 1);
            let admin_id = NodeId::mint();
            let admin_inc = IncarnationId::mint();
            let rpc_succ_id = NodeId::mint();
            let rpc_succ_inc = IncarnationId::mint();
            let rpc_pred_id = NodeId::mint();
            let rpc_pred_inc = IncarnationId::mint();

            let mut admin_node = Node::allocated(me.clone(), "mesh1", "node-admin");
            admin_node.node_id = admin_id.clone();
            admin_node.incarnation_id = Some(admin_inc.clone());
            admin_node.status = ModelNodeStatus::Ready;
            admin_node.is_primary = true;
            admin_node.is_fabric_primary = true;

            let rpc_path = PathName::numbered("mesh1", "node", 1);
            let mut rpc_succ = Node::allocated(rpc_path.clone(), "mesh1", "node");
            rpc_succ.node_id = rpc_succ_id.clone();
            rpc_succ.incarnation_id = Some(rpc_succ_inc.clone());
            rpc_succ.status = ModelNodeStatus::Ready;
            rpc_succ.is_primary = true;

            let rpc_pred_path = rpc_path.with_name("renamed").unwrap();
            let mut rpc_pred = Node::allocated(rpc_pred_path.clone(), "mesh1", "node");
            rpc_pred.node_id = rpc_pred_id.clone();
            rpc_pred.incarnation_id = Some(rpc_pred_inc.clone());
            rpc_pred.status = ModelNodeStatus::Ready;

            let view = Topology {
                fabric: Fabric {
                    id: FabricId::mint(),
                    name: "fabric1".to_string(),
                    status: ModelScopeStatus::Ready,
                    provider: ModelProviderKind::Process,
                },
                meshes: vec![Mesh {
                    id: Some(MeshId::mint()),
                    name: "mesh1".to_string(),
                    status: ModelScopeStatus::Ready,
                }],
                nodes: vec![admin_node, rpc_succ, rpc_pred],
            };

            let atlas = Atlas::default();
            // Each node's record is at its node id in the nodes store, written by the node itself (Q356):
            // its config and tags (node-config-update.md step 4) and its cert dates among its fields.
            let claims = crate::storage::MemoryNodesStorage::default();
            let admin_row = rdm_node::nodes_storage::fixture_record(&me, "node-admin", admin_id.clone(), admin_inc.clone(), ModelNodeStatus::Ready);
            let mut succ_row = rdm_node::nodes_storage::fixture_record(&rpc_path, "node", rpc_succ_id.clone(), rpc_succ_inc.clone(), ModelNodeStatus::Ready);
            succ_row.config = BTreeMap::from([("region".to_string(), "eu-west".to_string())]);
            succ_row.tags = BTreeSet::from(["tier-a".to_string()]);
            succ_row.cert = Some(CertDates {
                issued_at: 1_000,
                expires_at: 9_000,
                issuer_expires_at: 20_000,
                renew_at: 7_000,
            });
            // The earlier node's tags stay in its own record at its own id (node-records: a record is keyed by its node id).
            let mut pred_row = rdm_node::nodes_storage::fixture_record(&rpc_pred_path, "node", rpc_pred_id.clone(), rpc_pred_inc.clone(), ModelNodeStatus::Ready);
            pred_row.tags = BTreeSet::from(["tier-a".to_string()]);
            for row in [&admin_row, &succ_row, &pred_row] {
                crate::storage::NodesStorage::put_node(&claims, row).await.unwrap();
            }

            let loads: Loads = Arc::new(BTreeMap::new);

            // 1. A tag query reads each node's own labels: the new node and the stopped renamed node both carry tier-a.
            let read = |filter: Filter| {
                let (me, view, loads, atlas, claims) = (&me, &view, &loads, &atlas, &claims);
                let current = crate::current_record::CurrentRecords { nodes: claims };
                async move { nodes_answer_with_status(me, view, loads, atlas, &filter, Some(current)).await }
            };
            let tag_reply = read(Filter::new().eq("tags", ["tier-a"])).await;
            match tag_reply {
                StatusReply::Nodes { nodes } => {
                    assert_eq!(nodes.len(), 2);
                    let succ = nodes.iter().find(|n| n.node_id == rpc_succ_id).expect("the successor carries tier-a");
                    assert!(nodes.iter().any(|n| n.node_id == rpc_pred_id), "the stopped renamed node's own tier-a stays at its id");
                    assert_eq!(succ.config.get("region").map(String::as_str), Some("eu-west"));
                    assert_eq!(succ.cert.as_ref().map(|c| c.expires_at), Some(9_000));
                }
                other => panic!("expected StatusReply::Nodes, got {other:?}"),
            }

            // 2. Querying the predecessor by id returns its row with its retained tags intact.
            let pred_reply = read(Filter::new().eq("id", [rpc_pred_id.to_string()])).await;
            match pred_reply {
                StatusReply::Nodes { nodes } => {
                    assert_eq!(nodes.len(), 1);
                    assert_eq!(nodes[0].node_id, rpc_pred_id);
                    assert_eq!(nodes[0].tags, BTreeSet::from(["tier-a".to_string()]));
                }
                other => panic!("expected StatusReply::Nodes for predecessor, got {other:?}"),
            }

            // 3. Invalid filter on node.get is refused naming the pair.
            let bad_node_reply = nodes_answer_with_status(&me, &view, &loads, &atlas, &Filter::new().eq("unknown_field", ["x"]), None).await;
            assert!(matches!(bad_node_reply, StatusReply::RejectedFilter { ref reason } if reason.contains("(unknown_field, [x])")));

            // 4. mesh.get and fabric.get serve matching views and refuse invalid filters.
            let mesh_reply = meshes_answer(&me, &view, &Filter::new().eq("name", ["mesh1"]).eq("status", ["ready"]));
            match mesh_reply {
                StatusReply::Meshes { meshes } => {
                    assert_eq!(meshes.len(), 1);
                    assert_eq!(meshes[0].name, "mesh1");
                }
                other => panic!("expected StatusReply::Meshes, got {other:?}"),
            }
            let bad_mesh_reply = meshes_answer(&me, &view, &Filter::new().eq("status", ["not-a-status"]));
            assert!(matches!(bad_mesh_reply, StatusReply::RejectedFilter { ref reason } if reason.contains("(status, [not-a-status])")));

            let build_id = rdm_core::BuildId::mint().to_string();
            let fabric_reply = fabric_answer_with_status(
                &me,
                &view,
                Some(build_id.clone()),
                Some(&[]),
                Some(2_000),
                Some(&crate::fabric_storage::FabricRecord { fabric_id: FabricId::mint(), name: "fabric1".into(), state: crate::fabric_state::FabricState::Pending, types: Vec::new(), provider: None, topology_storage_provider: Some("file".to_string()), previous_topology_storage_providers: vec!["memory".to_string()], ca: None, roots: None }),
                &Filter::new().eq("name", ["fabric1"]).eq("build_id", [build_id.clone()]),
            );
            match fabric_reply {
                StatusReply::Fabrics { fabrics } => {
                    assert_eq!(fabrics.len(), 1);
                    assert_eq!(fabrics[0].build_id.as_deref(), Some(build_id.as_str()));
                    assert_eq!(fabrics[0].topology_storage_provider.as_deref(), Some("file"));
                    assert_eq!(fabrics[0].previous_topology_storage_providers, vec!["memory".to_string()]);
                }
                other => panic!("expected StatusReply::Fabrics, got {other:?}"),
            }
            let bad_fabric_reply = fabric_answer(&me, &view, None, &Filter::new().eq("provider", ["bogus"]));
            assert!(matches!(bad_fabric_reply, StatusReply::RejectedFilter { ref reason } if reason.contains("(provider, [bogus])")));
        });

        drop(rt);

        let node_spans = capture.named("rdm.node_admin.node.serve.via-get");
        assert!(
            node_spans.iter().any(|s| s.attributes.get("outcome").map(String::as_str) == Some("nodes")),
            "expected rdm.node_admin.node.serve.via-get with outcome=nodes"
        );
        assert!(
            node_spans.iter().any(|s| s.attributes.get("outcome").map(String::as_str) == Some("rejected-filter")),
            "expected rdm.node_admin.node.serve.via-get with outcome=rejected-filter"
        );
        assert!(
            !capture.named("rdm.node_admin.mesh.serve.via-get").is_empty(),
            "expected rdm.node_admin.mesh.serve.via-get"
        );
        assert!(
            !capture.named("rdm.node_admin.mesh.reject.via-filter").is_empty(),
            "expected rdm.node_admin.mesh.reject.via-filter"
        );
        assert!(
            !capture.named("rdm.node_admin.fabric.serve.via-get").is_empty(),
            "expected rdm.node_admin.fabric.serve.via-get"
        );
        assert!(
            !capture.named("rdm.node_admin.fabric.reject.via-filter").is_empty(),
            "expected rdm.node_admin.fabric.reject.via-filter"
        );
    }

    /// CONTRACT: docs/architecture/query.md:16
    /// "A current-pair lookup resolves the path and name to a node ID from the node records. The reader reads the node's record"
    /// A `node.get` naming a (path, name) whose node the answering admin's gossip view does not hold
    /// at that moment (a peer mesh's node-admin whose stop the backbone delivered after its start
    /// completed) answers the live record standing at the pair, read at its node ID: its incarnation and
    /// the state its record last took. Must NOT happen: the node not answered (the client's `unknown-node`)
    /// while its record stands at the pair; a pair no live record stands at answered.
    #[test]
    fn a_pair_the_view_does_not_hold_is_answered_from_the_record_standing_at_it() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("build current-thread runtime");
        rt.block_on(async {
            use crate::storage::NodesStorage as _;
            let me = PathName::numbered("mesh1", "node-admin", 1);
            let at = PathName::numbered("mesh2", "node-admin", 2);
            let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
            let view = Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".to_string(), status: ModelScopeStatus::Ready, provider: ModelProviderKind::Process }, meshes: Vec::new(), nodes: Vec::new() };
            let store = crate::storage::MemoryNodesStorage::default();
            let row = rdm_node::nodes_storage::fixture_record(&at, "node-admin", node_id.clone(), incarnation.clone(), ModelNodeStatus::Ready);
            store.put_node(&row).await.unwrap();
            let (atlas, loads): (Atlas, Loads) = (Atlas::default(), Arc::new(BTreeMap::new));
            let read = |filter: Filter| {
                let (me, view, loads, atlas, store) = (&me, &view, &loads, &atlas, &store);
                async move { nodes_answer_with_status(me, view, loads, atlas, &filter, Some(crate::current_record::CurrentRecords { nodes: store })).await }
            };
            match read(Filter::new().eq("path", [at.path.as_str()]).eq("name", [at.name.as_str()])).await {
                StatusReply::Nodes { nodes } => {
                    let answered: Vec<_> = nodes.iter().map(|n| (n.node_id.clone(), n.incarnation.clone(), n.state == rdm_core::views::NodeStatus::Ready)).collect();
                    assert_eq!(answered, vec![(node_id.clone(), Some(incarnation.clone()), true)], "the record standing at the pair is answered at its node ID, as its record last took it: {nodes:#?}");
                }
                other => panic!("expected StatusReply::Nodes, got {other:?}"),
            }
            match read(Filter::new().eq("path", [at.path.as_str()]).eq("name", ["3"])).await {
                StatusReply::Nodes { nodes } => assert!(nodes.is_empty(), "a pair no live record stands at answers nothing: {nodes:#?}"),
                other => panic!("expected StatusReply::Nodes, got {other:?}"),
            }
        });
    }
}
