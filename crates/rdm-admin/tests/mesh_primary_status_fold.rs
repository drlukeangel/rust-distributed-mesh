//! The status rows of a mesh live on the fabric's store root, written by the mesh's seat holder.
//! An admin holds them in its caches, hydrated at its create, start and join, and folds what its
//! caches hold into what it applied when it takes the mesh's seat, reading no storage (Q227, Q260,
//! node-identity-and-state.md §4.3); never at boot by an admin that does not hold the seat.

use rdm_admin::admin::Records;
use rdm_admin::model::*;
use rdm_admin::fabric_storage::{FabricStorage, FileFabricStorage};
use rdm_admin::storage::{FileMeshStorage, FileNodesStorage, MeshStorage, NodesStorage};

/// The stores an admin of mesh1 opens on the fabric's store root `root`, written as `writer`: the facts
/// on its mesh's births, on every mesh and on the fabric.
struct Stores {
    nodes: Arc<FileNodesStorage>,
    meshes: Arc<FileMeshStorage>,
    fabric: Arc<FileFabricStorage>,
}

async fn stores(root: &rdm_admin::store_root::StoreRoot, writer: &IncarnationId) -> Stores {
    let data_dir = root.dir().join(format!("data-{}", writer.as_str()));
    Stores {
        nodes: Arc::new(FileNodesStorage::open(&data_dir, root, "mesh1", &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &writer)).await.unwrap()),
        meshes: Arc::new(FileMeshStorage::open(root, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &writer)).await.unwrap()),
        fabric: Arc::new(FileFabricStorage::open(root, &rdm_admin::store_log::LogWriter::new(&rdm_core::NodeId::mint(), writer)).await.unwrap()),
    }
}
use rdm_admin::topology::Topology;
use rdm_core::status::{MeshState, NodeState};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

/// What an admin's create, start and join hydrate into its caches: its mesh's node rows, the Mesh
/// records and the Fabric record, as the cell's store holds them now (Q227: the seat taker reads no
/// storage; what it folds is what its caches hold).
async fn hydrate(records: &Records, nodes: &dyn rdm_admin::storage::NodesStorage, meshes: &dyn rdm_admin::storage::MeshStorage, fabric: &dyn rdm_admin::fabric_storage::FabricStorage) {
    for r in nodes.nodes("mesh1").await.unwrap() {
        records.caches.nodes.hold(&r);
    }
    for m in meshes.meshes().await.unwrap() {
        records.caches.meshes.hold(m.mesh_id.clone(), m);
    }
    if let Some(f) = fabric.fabric().await.unwrap() {
        records.caches.fabric.hold(f.fabric_id.clone(), f);
    }
}

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// mesh1 with two admins (`mesh1.admin.1` primary of mesh1 and the fabric) and one rpc node.
fn view(mesh_id: &MeshId, fabric_id: &FabricId) -> Topology {
    Topology {
        fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(mesh_id.clone()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node("mesh1.admin.1", true, true), node("mesh1.admin.2", false, false), node("mesh1.rpc.1", false, false)],
    }
}

/// What B held before and after it took mesh1's seat, and the cell's trace id.
struct Taken {
    before: rdm_admin::status_rpc::Declared,
    after: rdm_admin::status_rpc::Declared,
    rpc: (NodeId, IncarnationId),
    other: (NodeId, IncarnationId),
    mesh_id: MeshId,
    fabric_id: FabricId,
    trace_id: String,
}

async fn taken_over_one_root(trace_id: String) -> Taken {
    let dir = std::env::temp_dir().join(format!("status-seat-fold-{}", NodeId::mint()));
    let (mesh_id, fabric_id) = (MeshId::mint(), FabricId::mint());
    let mut before_view = view(&mesh_id, &fabric_id);
    before_view.nodes.push(node("mesh1.rpc.2", false, false));
    let rpc = before_view.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
    let rpc_birth = (rpc.node_id.clone(), rpc.incarnation_id.clone().unwrap());
    let other_node = before_view.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.2").unwrap();
    let other = (other_node.node_id.clone(), other_node.incarnation_id.clone().unwrap());
    // A, mesh1's primary, applied these as the mesh's authority: its rows on the shared root.
    let root = rdm_admin::store_root::StoreRoot::at(&dir);
    let a = stores(&root, &IncarnationId::mint()).await;
    { let mut r = a.meshes.mesh(&mesh_id.clone()).await.unwrap().unwrap_or_else(|| rdm_admin::storage::MeshRecord::new(mesh_id.clone(), "mesh1")); r.states.insert(MeshState::Ready); a.meshes.put_mesh(&r).await.unwrap(); }
    { let mut r = a.fabric.fabric().await.unwrap().unwrap_or(rdm_admin::fabric_storage::FabricRecord { fabric_id: fabric_id.clone(), name: "fabric1".into(), events: Vec::new(), provider: None, topology_storage_provider: None, previous_topology_storage_providers: Vec::new(), ca: None, roots: None}); r.events.push("shutdown-initiated:mesh1.admin.1".into()); a.fabric.put_fabric(&r).await.unwrap(); }
    { let (node_id, incarnation_id, name) = (rpc_birth.0.clone(), rpc_birth.1.clone(), rpc.name.clone()); rdm_admin::node_rows::WrittenRows::default().update(&*a.nodes, &node_id, Some(rdm_admin::storage::NodeRow::new(name.clone(), node_id.clone(), NodeStatus::Pending)), |r| { r.birth_mut(&incarnation_id, &name).declared.insert(NodeState::Ready); }).await.unwrap(); }
    { let (node_id, incarnation_id, name) = (other.0.clone(), other.1.clone(), "mesh1.rpc.2".parse::<PathName>().unwrap()); rdm_admin::node_rows::WrittenRows::default().update(&*a.nodes, &node_id, Some(rdm_admin::storage::NodeRow::new(name.clone(), node_id.clone(), NodeStatus::Pending)), |r| { r.birth_mut(&incarnation_id, &name).declared.insert(NodeState::Pending); }).await.unwrap(); }
    let fabric_records = rdm_admin::store_log::latest_in(&root.dir().join(rdm_admin::store_root::FABRIC_FILE)).unwrap().into_keys().filter(|(kind, _)| kind == "fabric-record").count();
    let mesh_records = rdm_admin::store_log::keys_in(&root.dir().join(rdm_admin::store_root::MESHES_FILE), "mesh-record").unwrap().len();
    let node_records = rdm_admin::store_log::keys_in(&root.dir().join(rdm_admin::store_root::NODES_FILE), "node-record").unwrap().len();
    assert_eq!((mesh_records, fabric_records, node_records), (1, 1, 2), "A's facts live on the fabric's store root, each on its object's record");
    // B, the other admin of mesh1, with its own store instance over the same root.
    let b_id = before_view.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.2").unwrap().node_id.clone();
    let records = Records::default();
    let b_stores = stores(&root, &IncarnationId::mint()).await;
    hydrate(&records, &*b_stores.nodes, &*b_stores.meshes, &*b_stores.fabric).await;
    records.set_status(b_stores.nodes, b_stores.meshes, b_stores.fabric, "mesh1".into(), "mesh1.admin.2".parse().unwrap(), b_id.clone());
    let topology = RwLock::new(Topology { nodes: Vec::new(), ..before_view.clone() });
    // B installs views naming A the primary: it holds no seat and folds none of A's rows.
    records.install_projected(&topology, || before_view.clone()).await;
    // What B applied itself meanwhile: a later state for the other birth and an event of its own.
    {
        let mut d = records.declared.lock().unwrap();
        d.nodes.insert(other.clone(), NodeState::ConnectionsDeleting);
        d.fabric.entry(fabric_id.clone()).or_default().push("b-own".into());
    }
    let before = records.declared.lock().unwrap().clone();
    // A's birth is gone and B takes mesh1's seat.
    let mut after_view = before_view.clone();
    after_view.nodes.retain(|n| n.name.to_string() != "mesh1.admin.1");
    for n in after_view.nodes.iter_mut() {
        if n.node_id == b_id {
            n.is_primary = true;
            n.is_fabric_primary = true;
        }
    }
    records.install_projected(&topology, || after_view.clone()).await;
    let after = records.declared.lock().unwrap().clone();
    let _ = std::fs::remove_dir_all(&dir);
    Taken { before, after, rpc: rpc_birth, other, mesh_id, fabric_id, trace_id }
}

/// CONTRACT (R14 :8 status is one of the interfaces "every writer and reader sees the same rows",
/// :34 layout of the shared root; Q227, Q260: the seat taker folds what its caches hold, reading no storage;
/// node-identity-and-state.md §4.3: the mesh-primary writes every status of its mesh): A, mesh1's
/// primary, applied a Mesh status, a Fabric event and two node declared states, each a row on the
/// fabric's store root. B, the other admin of mesh1 with its own store instance over that root,
/// folds none of them while A holds the seat: what B holds is only what B applied. When B takes the
/// seat, the view naming it mesh1's primary is installed with A's rows folded in, merged with what
/// B applied: the greatest state per key (B's own later state stands over A's earlier row), the
/// union of the events. One span, `rdm.node_admin.status.resolve.via-successor-fold`, names the mesh
/// and the counts folded.
#[test]
fn a_new_mesh_primary_folds_the_mesh_status_rows_and_a_secondary_folds_none() {
    let cap = crate::mesh_delete_resume::capture();
    let t = cap.run(async {
        let root_span = tracing::info_span!("rdm.test.status.resolve.via-successor-fold");
        let trace_id = crate::mesh_delete_resume::trace_id(&root_span);
        tracing::Instrument::instrument(taken_over_one_root(trace_id), root_span).await
    });
    println!("mesh_primary_status_fold trace_id={}", t.trace_id);
    assert_eq!(t.before.node(&t.rpc.0, &t.rpc.1), None, "a non-holder folds none of the primary's rows: {:#?}", t.before);
    assert!(t.before.meshes.is_empty(), "a non-holder folds no Mesh status row: {:#?}", t.before);
    assert_eq!(t.before.fabric.get(&t.fabric_id), Some(&vec!["b-own".to_string()]), "a non-holder holds only the events it applied: {:#?}", t.before);
    assert_eq!(t.after.node(&t.rpc.0, &t.rpc.1), Some(NodeState::Ready), "the seat taker folds the primary's declared-state row: {:#?}", t.after);
    assert_eq!(t.after.names.get(&t.rpc).map(|n| n.to_string()), Some("mesh1.rpc.1".to_string()), "and the path.name it was declared at");
    assert_eq!(t.after.node(&t.other.0, &t.other.1), Some(NodeState::ConnectionsDeleting), "the greatest state per birth: what B applied later stands over the row's Pending");
    assert_eq!(t.after.meshes.get(&t.mesh_id), Some(&MeshState::Ready), "the seat taker folds the Mesh status row");
    let mut events = t.after.fabric.get(&t.fabric_id).cloned().unwrap_or_default();
    events.sort();
    assert_eq!(events, vec!["b-own".to_string(), "shutdown-initiated:mesh1.admin.1".to_string()], "the union of the events, each once");
    let spans: Vec<Value> = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.status.resolve.via-successor-fold").collect();
    assert_eq!(spans.len(), 1, "one fold, at the seat take: {spans:#?}");
    let s = &spans[0];
    assert_eq!(s["trace_id"], t.trace_id.as_str(), "the fold is the cell's own story: {s:#}");
    for (k, v) in [("node", "mesh1.admin.2"), ("mesh", "mesh1"), ("node_declared_rows", "2"), ("mesh_status_rows", "1"), ("fabric_event_rows", "1")] {
        assert_eq!(s["attributes"][k], v, "{k}: {s:#}");
    }
}

/// The heartbeat `n`'s exact birth publishes: liveness only, no status (gossip.md §3.1).
fn heartbeat(fabric_id: &FabricId, n: &Node) -> rdm_core::MeshDigest {
    rdm_core::MeshDigest { held_hash: Default::default(),
        fabric_id: fabric_id.clone(),
        node: rdm_core::MeshNode {
            node_id: n.node_id.clone(),
            name: n.name.clone(),
            endpoint_id: EndpointId(format!("key-{}-{}", n.name, "x".repeat(40))),
            transport_addr: "127.0.0.1:20001".parse().unwrap(),
            incarnation: n.incarnation_id.clone().unwrap(),
            supersedes: None,
            runtime: None,
        },
        status: rdm_core::MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 7,
        emitted_at_rafka_ms: 7,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

/// CONTRACT (node-status-heartbeat-connections.md §A.3: "the successor acquires the held member
/// statuses and publishes a complete projection under its own revision before any delta"; fabric-authority.md
/// §7: a successor mesh primary hydrates the lifecycle state that already exists, adopts that exact
/// state and republishes the adopted projection, manufacturing no transition; mesh-investigation.md "A
/// node-admin joins the live mesh" step 4): B, an admin of mesh1 whose view holds `mesh1.rpc.1`'s
/// heartbeat and no status for it (an admin entering a live mesh: its topology read installs nothing,
/// reenter.rs), takes mesh1's seat. A, the primary before it, left a declared-state row for that exact
/// birth saying Ready, and one for `mesh1.rpc.2`, whose heartbeat B hears only after it took
/// the seat. The fold of A's rows reaches B's view: both births are Ready there and in B's
/// member projection. `mesh1.rpc.1` declaring Ready to its new primary (R-S2: a declaration is
/// owed again when the authority moved) is AlreadyApplied, and the view still says Ready. One
/// `rdm.node_admin.status.update.via-successor-fold` per adopted birth names it, its prior and its status.
/// Must NOT happen (i143_acceptance_2803, fabric recovery): the successor's projection says a surviving
/// member Pending while its record says Ready, so the member's declaration is AlreadyApplied
/// and the member stays Pending under the new primary for good.
#[test]
fn a_new_mesh_primary_adopts_the_folded_member_statuses_into_its_view_and_projection() {
    use rdm_core::MemberStatus;
    use rdm_transport::topology::{Backbone, TopologyView};
    use rdm_core::status::{StatusReply, StatusRequest};
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, after_take, rpc2_heard, projected, declared, after_declare) = cap.run(async {
        let root_span = tracing::info_span!("rdm.test.status.update.via-successor-fold");
        let trace_id = crate::mesh_delete_resume::trace_id(&root_span);
        tracing::Instrument::instrument(
            async move {
                let dir = std::env::temp_dir().join(format!("status-seat-adopt-{}", NodeId::mint()));
                let (mesh_id, fabric_id) = (MeshId::mint(), FabricId::mint());
                let mut before_view = view(&mesh_id, &fabric_id);
                before_view.nodes.push(node("mesh1.rpc.2", false, false));
                let by_name = |v: &Topology, name: &str| v.nodes.iter().find(|n| n.name.to_string() == name).cloned().unwrap();
                let (rpc1, rpc2, b) = (by_name(&before_view, "mesh1.rpc.1"), by_name(&before_view, "mesh1.rpc.2"), by_name(&before_view, "mesh1.admin.2"));
                // A's rows on the fabric's store root: both members Ready, as A applied them.
                let root = rdm_admin::store_root::StoreRoot::at(&dir);
                let a = stores(&root, &IncarnationId::mint()).await;
                for n in [&rpc1, &rpc2] {
                    { let (node_id, incarnation_id, name) = (n.node_id.clone(), n.incarnation_id.clone().unwrap(), n.name.clone()); rdm_admin::node_rows::WrittenRows::default().update(&*a.nodes, &node_id, Some(rdm_admin::storage::NodeRow::new(name.clone(), node_id.clone(), NodeStatus::Pending)), |r| { r.birth_mut(&incarnation_id, &name).declared.insert(NodeState::Ready); }).await.unwrap(); }
                }
                // B: its topology view's view, its seat fold over the same root.
                let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).await.unwrap();
                let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
                let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
                let topology_view = TopologyView::join(&gossip, &ep, &fabric_id, "mesh1", &mesh_id, "mesh1.admin.2", Arc::new(crate::adopted_time()), vec![], &[]).await.unwrap();
                let records = Records::default();
                records.set_digest_view(topology_view.digest_view.clone());
                let b_stores = stores(&root, &IncarnationId::mint()).await;
                hydrate(&records, &*b_stores.nodes, &*b_stores.meshes, &*b_stores.fabric).await;
                records.set_status(b_stores.nodes, b_stores.meshes, b_stores.fabric, "mesh1".into(), b.name.clone(), b.node_id.clone());
                assert!(topology_view.digest_view.record(heartbeat(&fabric_id, &rpc1)), "rpc.1's heartbeat is held");
                let status = |n: &Node| topology_view.digest_view.get(n.node_id.as_str()).map(|(d, _)| d.status);
                let topology = Arc::new(RwLock::new(Topology { nodes: Vec::new(), ..before_view.clone() }));
                records.install_projected(&topology, || before_view.clone()).await;
                assert_eq!(status(&rpc1), Some(MemberStatus::Pending), "a member whose status B was never told is Pending while A holds the seat");
                // A's birth is gone and B takes mesh1's seat.
                let mut after_view = before_view.clone();
                after_view.nodes.retain(|n| n.name.to_string() != "mesh1.admin.1");
                for n in after_view.nodes.iter_mut() {
                    if n.node_id == b.node_id {
                        n.is_primary = true;
                        n.is_fabric_primary = true;
                    }
                }
                records.install_projected(&topology, || after_view.clone()).await;
                topology_view.set_mesh_primary(true);
                let after_take = status(&rpc1);
                assert!(topology_view.digest_view.record(heartbeat(&fabric_id, &rpc2)), "rpc.2's heartbeat is held after the seat take");
                let rpc2_heard = status(&rpc2);
                let backbone = Backbone::join(&gossip, &ep, &topology_view, "mesh1", rdm_core::PublisherId { node: "mesh1.admin.2".to_string(), incarnation: b.incarnation_id.clone().unwrap(), node_id: b.node_id.clone() }, vec![]).await.unwrap();
                let mut projected: Vec<(String, MemberStatus)> = backbone.own_members(&topology_view).into_iter().filter(|d| d.node.name.kind == rdm_core::NodeKind::RpcNode).map(|d| (d.node.name.to_string(), d.status)).collect();
                projected.sort_by(|a, b| a.0.cmp(&b.0));
                // rpc.1 declares to its new primary.
                let door = rdm_admin::status_rpc::StatusDoor {
                    certs: None,
                    app_channels: None,
                    declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
                    stopped: Default::default(),
                    me: b.name.clone(),
                    node_id: b.node_id.clone(),
                    fabric_id: fabric_id.clone(),
                    topology: topology.clone(),
                    declared: records.declared.clone(),
                    nodes_storage: stores(&root, &IncarnationId::mint()).await.nodes,
                    caches: Default::default(),
                    meshes_storage: stores(&root, &IncarnationId::mint()).await.meshes,
                    fabric_storage: stores(&root, &IncarnationId::mint()).await.fabric,
                    mesh_ids: Arc::new(std::collections::BTreeMap::new),
                    durable: Default::default(),
                    commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
                    own: Arc::new(std::sync::OnceLock::new()),
                    mesh_deleter: Arc::new(std::sync::OnceLock::new()),
                    hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    wake: Default::default(),
                    fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
                    configs: Default::default(),
                    republish: Default::default(), configure: Default::default(),
                    pre_node_ready: Default::default(),
                    rename: Default::default(),
                    row_moved: Default::default(),
                    loads: None,
                    digest_view: Some(topology_view.digest_view.clone()),
                    builds: std::sync::Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
                };
                let declared = door.apply(Some(rpc1.clone()), &StatusRequest::DeclareNodeState { node_id: rpc1.node_id.clone(), incarnation: rpc1.incarnation_id.clone().unwrap(), state: NodeState::Ready, operation: None }).await;
                let after_declare = status(&rpc1);
                let _ = std::fs::remove_dir_all(&dir);
                (trace_id, after_take, rpc2_heard, projected, declared, after_declare)
            },
            root_span,
        )
        .await
    });
    println!("mesh_primary_status_fold adopt trace_id={trace_id}");
    assert_eq!(after_take, Some(MemberStatus::Ready), "the seat taker's view adopts the folded Ready of the birth it held Pending");
    assert_eq!(rpc2_heard, Some(MemberStatus::Ready), "a birth whose row was folded before its heartbeat arrived is held at the adopted status");
    assert_eq!(projected, vec![("mesh1.rpc.1".to_string(), MemberStatus::Ready), ("mesh1.rpc.2".to_string(), MemberStatus::Ready)], "the successor's member projection carries the adopted statuses");
    assert_eq!(declared, StatusReply::AlreadyApplied, "the member's re-declaration to its new primary is already held");
    assert_eq!(after_declare, Some(MemberStatus::Ready), "and the view agrees with the record");
    let adopted: Vec<Value> = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.status.update.via-successor-fold").collect();
    let mut members: Vec<String> = adopted.iter().map(|s| s["attributes"]["member"].as_str().unwrap_or_default().to_string()).collect();
    members.sort();
    assert_eq!(members, vec!["mesh1.rpc.1".to_string(), "mesh1.rpc.2".to_string()], "one adoption per folded birth of the mesh: {adopted:#?}");
    for s in &adopted {
        assert_eq!(s["trace_id"], trace_id.as_str(), "the adoption is the cell's own story: {s:#}");
        assert_eq!(s["attributes"]["node"], "mesh1.admin.2", "{s:#}");
        assert_eq!(s["attributes"]["status"], "Ready", "{s:#}");
    }
}

/// Q49: cycle state rows may be cleared, but an applied operation survives a disk reopen and
/// a successor writer. Attempts have separate identities; only forgetting the birth removes them.
#[tokio::test]
async fn applied_operations_survive_a_cycle_begin_and_a_successor_reopening_the_store() {
    use rdm_admin::status_rpc::OperationEffect;
    let dir = std::env::temp_dir().join(format!("operation-status-{}", NodeId::mint()));
    let root = rdm_admin::store_root::StoreRoot::at(&dir);
    let node_id = NodeId::mint();
    let incarnation = IncarnationId::mint();
    let row = OperationEffect {
        node_id: node_id.clone(), incarnation_id: incarnation.clone(), name: "mesh1.rpc.1".parse().unwrap(), state: NodeState::Stopped,
        operation: rdm_core::status::DeclaredOperation { build_id: "b".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() },
    };
    let next = OperationEffect { operation: rdm_core::status::DeclaredOperation { attempt: 2, ..row.operation.clone() }, ..row.clone() };
    {
        let store = stores(&root, &IncarnationId::mint()).await;
        let w = rdm_admin::node_rows::WrittenRows::default();
        let base = rdm_admin::storage::NodeRow::new(row.name.clone(), node_id.clone(), NodeStatus::Pending);
        w.update(&*store.nodes, &node_id, Some(base), |r| {
            let b = r.birth_mut(&incarnation, &row.name);
            b.declared.insert(NodeState::Stopped);
            b.apply_operation(&row.operation, row.state);
            b.apply_operation(&next.operation, next.state);
        }).await.unwrap();
        // A start begins the birth's next cycle: its declared states are cleared, its effects kept.
        w.update(&*store.nodes, &node_id, None, |r| r.birth_mut(&incarnation, &row.name).declared.clear()).await.unwrap();
    }
    let successor = stores(&root, &IncarnationId::mint()).await;
    let held = folded(&*successor.nodes, &*successor.meshes, &*successor.fabric, "mesh1").await;
    assert_eq!(held.node(&node_id, &incarnation), None, "only the current cycle's state was cleared");
    assert_eq!(held.operations.len(), 2, "both attempts are still applied");
    assert_eq!(held.operations.get(&row.key()), Some(&row));
    assert_eq!(held.operations.get(&next.key()), Some(&next));
    // Forgetting the exact birth lets its effects go with it.
    rdm_admin::node_rows::WrittenRows::default().update(&*successor.nodes, &node_id, None, |r| r.births.retain(|b| b.incarnation_id != incarnation)).await.unwrap();
    assert!(rdm_admin::status_rpc::OperationEffect::of_records(&successor.nodes.nodes("mesh1").await.unwrap()).is_empty(), "the operation rows are forgotten with the birth");
    let _ = std::fs::remove_dir_all(dir);
}

/// Q53: an acknowledged Ready for an exact birth survives acquisition by a new
/// mesh-primary and is in its first complete member projection, read from the store root its
/// predecessor wrote straight to the disk.
#[test]
fn an_acknowledged_ready_birth_is_in_its_successors_first_complete_projection() {
    use rdm_core::status::{StatusReply, StatusRequest};
    use rdm_core::MemberStatus;
    use rdm_transport::topology::{Backbone, TopologyView};
    let cap = crate::mesh_delete_resume::capture();
    let observed = cap.run(async {
        let mut observed = Vec::new();
        {
            let parent = tracing::info_span!("rdm.test.status.update.via-acknowledged-successor");
            let trace = crate::mesh_delete_resume::trace_id(&parent);
            let result = tracing::Instrument::instrument(async {
                let dir = std::env::temp_dir().join(format!("status-ack-successor-{}", NodeId::mint()));
                let (mesh_id, fabric_id) = (MeshId::mint(), FabricId::mint());
                let mut before = view(&mesh_id, &fabric_id);
                let a = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").unwrap().clone();
                let b = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.2").unwrap().clone();
                let rpc = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").unwrap().clone();
                let predecessor_root = rdm_admin::store_root::StoreRoot::at(&dir);
                let storage = stores(&predecessor_root, a.incarnation_id.as_ref().unwrap()).await;
                let door = rdm_admin::status_rpc::StatusDoor {
                    certs: None,
                    app_channels: None,
                    declaration_seam: None, command_frame_seam: None,
                    operation_effects: Default::default(), stopped: Default::default(),
                    me: a.name.clone(), node_id: a.node_id.clone(), fabric_id: fabric_id.clone(),
                    topology: Arc::new(RwLock::new(before.clone())), declared: Default::default(),
                    nodes_storage: storage.nodes.clone(), meshes_storage: storage.meshes.clone(), fabric_storage: storage.fabric.clone(),
                    caches: Default::default(),
                    mesh_ids: Arc::new(std::collections::BTreeMap::new), durable: Default::default(),
                    commands: Arc::new(rdm_admin::node_commands::CommandView::default()), own: Default::default(),
                    mesh_deleter: Default::default(), hold_next_reply: Default::default(), wake: Default::default(),
                    fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(), configs: Default::default(), republish: Default::default(), configure: Default::default(),
                    pre_node_ready: Default::default(), rename: Default::default(), row_moved: Default::default(),
                    loads: None, digest_view: None, builds: Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
                };
                let reply = door.apply(Some(rpc.clone()), &StatusRequest::DeclareNodeState {
                    node_id: rpc.node_id.clone(), incarnation: rpc.incarnation_id.clone().unwrap(),
                    state: NodeState::Ready, operation: None,
                }).await;
                assert_eq!(reply, StatusReply::Applied, "the predecessor acknowledged this exact birth's Ready");
                assert_eq!(rdm_admin::status_rpc::Declaration::of_records(&storage.nodes.nodes("mesh1").await.unwrap()).len(), 1, "the predecessor reads the acknowledged row");
                let disk = rdm_admin::store_root::StoreRoot::at(&dir);
                let independent = stores(&disk, &IncarnationId::mint()).await;
                assert_eq!(rdm_admin::status_rpc::Declaration::of_records(&independent.nodes.nodes("mesh1").await.unwrap()).len(), 1, "Applied made the row durable before the reply");
                { let mut r = storage.meshes.mesh(&mesh_id.clone()).await.unwrap().unwrap_or_else(|| rdm_admin::storage::MeshRecord::new(mesh_id.clone(), "mesh1")); r.states.insert(MeshState::Ready); storage.meshes.put_mesh(&r).await.unwrap(); }
                assert_eq!(independent.meshes.meshes().await.unwrap().into_iter().filter_map(|r| Some((r.mesh_id.clone(), r.state()?))).collect::<std::collections::HashMap<_, _>>().len(), 1, "the mesh row is on the root at its write");
                drop(independent);
                drop(door);
                drop(storage);
                drop(predecessor_root);
                // A successor is an independent process: it reads what its predecessor wrote to the root.
                let root = rdm_admin::store_root::StoreRoot::at(&dir);
                let successor = stores(&root, b.incarnation_id.as_ref().unwrap()).await;
                let rows = rdm_admin::status_rpc::Declaration::of_records(&successor.nodes.nodes("mesh1").await.unwrap()).len();
                let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).await.unwrap();
                let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
                let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
                let topology_view = TopologyView::join(&gossip, &ep, &fabric_id, "mesh1", &mesh_id, "mesh1.admin.2", Arc::new(crate::adopted_time()), vec![], &[]).await.unwrap();
                let records = Records::default();
                records.set_digest_view(topology_view.digest_view.clone());
                hydrate(&records, &*successor.nodes, &*successor.meshes, &*successor.fabric).await;
                records.set_status(successor.nodes, successor.meshes, successor.fabric, "mesh1".into(), b.name.clone(), b.node_id.clone());
                assert!(topology_view.digest_view.record(heartbeat(&fabric_id, &rpc)));
                before.nodes.retain(|n| n.node_id != a.node_id);
                for n in &mut before.nodes { if n.node_id == b.node_id { n.is_primary = true; n.is_fabric_primary = true; } }
                let topology = RwLock::new(Topology { nodes: Vec::new(), ..before.clone() });
                records.install_projected(&topology, || before.clone()).await;
                topology_view.set_mesh_primary(true);
                let backbone = Backbone::join(&gossip, &ep, &topology_view, "mesh1", rdm_core::PublisherId {
                    node: b.name.to_string(), incarnation: b.incarnation_id.clone().unwrap(), node_id: b.node_id.clone(),
                }, vec![]).await.unwrap();
                let first = backbone.own_members(&topology_view).into_iter().find(|d| d.node.node_id == rpc.node_id && Some(&d.node.incarnation) == rpc.incarnation_id.as_ref()).unwrap();
                let _ = std::fs::remove_dir_all(&dir);
                (rows, first.status)
            }, parent).await;
            observed.push((trace, result));
        }
        observed
    });
    for row in cap.spans() { println!("Q53_SPAN {}", serde_json::to_string(&row).unwrap()); }
    for (trace, (rows, status)) in observed {
        println!("Q53 trace={trace} rows={rows} first={status:?}");
        assert_eq!((rows, status), (1, MemberStatus::Ready), "Q53: acknowledged exact-birth Ready is in the successor's first full; trace={trace}");
    }
}

/// Q54: acquisition uses the incoming view's exact births, never a row's name.
#[test]
fn a_successor_ignores_declared_rows_for_births_it_does_not_hold() {
    crate::mesh_delete_resume::capture().run(async {
        let dir = std::env::temp_dir().join(format!("status-held-births-{}", NodeId::mint()));
        let (mesh_id, fabric_id) = (MeshId::mint(), FabricId::mint());
        let mut now = view(&mesh_id, &fabric_id);
        let b = now.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.2").unwrap().clone();
        let rpc = now.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").unwrap().clone();
        now.nodes.retain(|n| n.name.to_string() != "mesh1.admin.1");
        now.nodes.iter_mut().find(|n| n.node_id == b.node_id).unwrap().is_primary = true;
        let root = rdm_admin::store_root::StoreRoot::at(&dir);
        let storage = stores(&root, b.incarnation_id.as_ref().unwrap()).await;
        let exact = (rpc.node_id.clone(), rpc.incarnation_id.clone().unwrap());
        let stale = (rpc.node_id.clone(), IncarnationId::mint());
        let absent = (NodeId::mint(), IncarnationId::mint());
        // The records as a lost predecessor left them: the exact birth's, another birth of its node, and a
        // node the view never held, each written whole at its id.
        let mut held = rdm_admin::storage::NodeRow::new(rpc.name.clone(), rpc.node_id.clone(), NodeStatus::Ready);
        for (_, incarnation_id) in [&exact, &stale] {
            held.birth_mut(incarnation_id, &rpc.name).declared.insert(NodeState::Ready);
        }
        storage.nodes.put_node(&held).await.unwrap();
        let mut stranger = rdm_admin::storage::NodeRow::new(rpc.name.clone(), absent.0.clone(), NodeStatus::Ready);
        stranger.birth_mut(&absent.1, &rpc.name).declared.insert(NodeState::Ready);
        storage.nodes.put_node(&stranger).await.unwrap();
        let records = Records::default();
        hydrate(&records, &*storage.nodes, &*storage.meshes, &*storage.fabric).await;
        records.set_status(storage.nodes, storage.meshes, storage.fabric, "mesh1".into(), b.name.clone(), b.node_id.clone());
        let topology = RwLock::new(Topology { nodes: Vec::new(), ..now.clone() });
        records.install_projected(&topology, || now.clone()).await;
        let held = records.declared.lock().unwrap();
        assert_eq!(held.node(&exact.0, &exact.1), Some(NodeState::Ready));
        assert_eq!(held.node(&stale.0, &stale.1), None, "a previous incarnation is not held");
        assert_eq!(held.node(&absent.0, &absent.1), None, "a matching name does not establish a birth");
        drop(held);
        let _ = std::fs::remove_dir_all(dir);
    });
}

/// What a mesh-primary folds from the stores as it takes `mesh` (`SeatFold::on_install`'s read): the
/// facts on every mesh and on the fabric, and on every birth of `mesh`'s node records.
async fn folded(nodes: &dyn rdm_admin::storage::NodesStorage, meshes: &dyn rdm_admin::storage::MeshStorage, fabric: &dyn rdm_admin::fabric_storage::FabricStorage, mesh: &str) -> rdm_admin::status_rpc::Declared {
    let (m, f, n) = (meshes.meshes().await.unwrap(), fabric.fabric().await.unwrap(), nodes.nodes(mesh).await.unwrap());
    let mut declared = rdm_admin::status_rpc::Declared::default();
    declared.fold_records(&m, f.as_ref(), &n, |_, _| true);
    declared
}
