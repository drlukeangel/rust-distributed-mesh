//! The status rows of a mesh live on the fabric's store root, written by the mesh's primary.
//! An admin holds them in its caches, hydrated at its create, start and join, and folds what its
//! caches hold into what it applied when it takes the mesh's role, reading no storage (Q227, Q260,
//! node-identity-and-state.md §4.3); never at boot by an admin that does not hold the role.

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
        nodes: Arc::new(FileNodesStorage::open(&root, &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &rdm_admin::model::NodeId::mint(), &writer, &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap()),
        meshes: Arc::new(FileMeshStorage::open(root, &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &rdm_admin::model::NodeId::mint(), &writer, &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap()),
        fabric: Arc::new(FileFabricStorage::open(root, &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &rdm_core::NodeId::mint(), writer, &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap()),
    }
}
use rdm_admin::topology::Topology;
use rdm_core::status::{MeshState, NodeState};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

/// What an admin's create, start and join hydrate into its caches: its mesh's node rows, the Mesh
/// records and the Fabric record, as the cell's store holds them now (Q227: the role taker reads no
/// storage; what it folds is what its caches hold).
async fn hydrate(records: &Records, nodes: &dyn rdm_admin::storage::NodesStorage, meshes: &dyn rdm_admin::storage::MeshStorage, fabric: &dyn rdm_admin::fabric_storage::FabricStorage) {
    let _ = nodes;
    for m in meshes.meshes().await.unwrap() {
        records.caches.meshes.hold(m.mesh_id.clone(), m);
    }
    if let Some(f) = fabric.fabric().await.unwrap() {
        records.caches.fabric.hold(f.fabric_id.clone(), f);
    }
}

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// mesh1 with two admins (`mesh1.node-admin.1` primary of mesh1 and the fabric) and one node.
fn view(mesh_id: &MeshId, fabric_id: &FabricId) -> Topology {
    Topology {
        fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(mesh_id.clone()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true), node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", false, false), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", false, false)],
    }
}

/// What B held before and after it took mesh1's role, and the cell's trace id.
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
    let dir = std::env::temp_dir().join(format!("status-role-fold-{}", NodeId::mint()));
    let (mesh_id, fabric_id) = (MeshId::mint(), FabricId::mint());
    let mut before_view = view(&mesh_id, &fabric_id);
    before_view.nodes.push(node(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", false, false));
    let rpc = before_view.nodes.iter().find(|n| n.location() == rdm_core::PathName::new("mesh1.node", "1").unwrap()).cloned().unwrap();
    let rpc_birth = (rpc.node_id.clone(), rpc.incarnation_id.clone().unwrap());
    let other_node = before_view.nodes.iter().find(|n| n.location() == rdm_core::PathName::new("mesh1.node", "2").unwrap()).unwrap();
    let other = (other_node.node_id.clone(), other_node.incarnation_id.clone().unwrap());
    // A, mesh1's primary, applied these as the mesh's authority: its rows on the shared root.
    let root = rdm_admin::store_root::StoreRoot::at(&dir).unwrap();
    let a = stores(&root, &IncarnationId::mint()).await;
    { let mut r = a.meshes.mesh(&mesh_id.clone()).await.unwrap().unwrap_or_else(|| rdm_admin::storage::MeshRecord::new(mesh_id.clone(), "mesh1")); r.state = MeshState::Ready; a.meshes.put_mesh(&r).await.unwrap(); }
    { let mut r = a.fabric.fabric().await.unwrap().unwrap_or(rdm_admin::fabric_storage::FabricRecord { fabric_id: fabric_id.clone(), name: "fabric1".into(), state: rdm_admin::fabric_state::FabricState::Pending, types: Vec::new(), provider: None, topology_storage_provider: None, previous_topology_storage_providers: Vec::new(), ca: None, roots: None}); r.state = rdm_admin::fabric_state::FabricState::Ready; a.fabric.put_fabric(&r).await.unwrap(); }
    // Each node wrote its own record (Q356): rpc.1 Ready, rpc.2 Pending.
    a.nodes.put_node(&rdm_node::nodes_storage::fixture_record(&rpc.location(), "node", rpc_birth.0.clone(), rpc_birth.1.clone(), NodeStatus::Ready)).await.unwrap();
    a.nodes.put_node(&rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", other.0.clone(), other.1.clone(), NodeStatus::Pending)).await.unwrap();
    let fabric_records = rdm_admin::store_log::latest_in(&root.dir().join(rdm_admin::store_root::FABRIC_FILE)).unwrap().into_keys().filter(|(kind, _)| kind == "fabric-record").count();
    let mesh_records = rdm_admin::store_log::subjects_in(&root.dir().join(rdm_admin::store_root::MESHES_FILE), "mesh-record").unwrap().len();
    let node_records = rdm_admin::store_log::subjects_in(&root.dir().join(rdm_admin::store_root::NODES_FILE), "node-record").unwrap().len();
    assert_eq!((mesh_records, fabric_records, node_records), (1, 1, 2), "A's facts live on the fabric's store root, each on its object's record");
    // B, the other admin of mesh1, with its own store instance over the same root.
    let b_id = before_view.nodes.iter().find(|n| n.location() == rdm_core::PathName::new("mesh1.node-admin", "2").unwrap()).unwrap().node_id.clone();
    let records = Records::default();
    let b_stores = stores(&root, &IncarnationId::mint()).await;
    hydrate(&records, &*b_stores.nodes, &*b_stores.meshes, &*b_stores.fabric).await;
    records.set_status(b_stores.nodes, b_stores.meshes, b_stores.fabric, "mesh1".into(), rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), b_id.clone());
    let topology = RwLock::new(Topology { nodes: Vec::new(), ..before_view.clone() });
    // B installs views naming A the primary: it holds no role and folds none of A's rows.
    records.install_projected(&topology, || before_view.clone()).await;
    // What B applied itself meanwhile: a later state for the other birth and an event of its own.
    {
        let mut d = records.declared.lock().unwrap();
        d.nodes.insert(other.clone(), NodeState::ConnectionsDeleting);
        d.fabric.entry(fabric_id.clone()).or_default().push("b-own".into());
    }
    let before = records.declared.lock().unwrap().clone();
    // A's birth is gone and B takes mesh1's role.
    let mut after_view = before_view.clone();
    after_view.nodes.retain(|n| n.location() != rdm_core::PathName::new("mesh1.node-admin", "1").unwrap());
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
/// :34 layout of the shared root; Q227, Q260: the role taker folds what its caches hold, reading no storage;
/// decision B, Q260 + Q356: the role fold reads no node record): A, mesh1's primary, applied a Mesh
/// status and a Fabric event, each on its record on the fabric's store root, and two nodes wrote their
/// own records there. B, the other admin of mesh1 with its own store instance over that root, folds
/// none of them while A holds the role: what B holds is only what B applied. When B takes the role,
/// the view naming it mesh1's primary is installed with A's Mesh and Fabric records folded in, merged
/// with what B applied (the union of the events); no node's state is folded from its record: B's own
/// later state stands, and a member's state comes from its digest and its owed declaration. One span,
/// `rdm.node_admin.status.resolve.via-successor-fold`, names the mesh and the counts folded.
#[test]
fn a_new_mesh_primary_folds_the_mesh_status_rows_and_a_secondary_folds_none() {
    let cap = crate::mesh_delete_resume::capture();
    let t = cap.run(async {
        let root_span = tracing::info_span!("rdm.test.status.resolve.via-successor-fold");
        let trace_id = crate::mesh_delete_resume::trace_id(&root_span);
        tracing::Instrument::instrument(taken_over_one_root(trace_id), root_span).await
    });
    println!("mesh_primary_status_fold trace_id={}", t.trace_id);
    assert_eq!(t.before.node(&t.rpc.0, &t.rpc.1), None, "a non-primary folds none of the primary's rows: {:#?}", t.before);
    assert!(t.before.meshes.is_empty(), "a non-primary folds no Mesh status row: {:#?}", t.before);
    assert_eq!(t.before.fabric.get(&t.fabric_id), Some(&vec!["b-own".to_string()]), "a non-primary holds only the events it applied: {:#?}", t.before);
    assert_eq!(t.after.node(&t.rpc.0, &t.rpc.1), None, "the role taker folds no node record: {:#?}", t.after);
    assert_eq!(t.after.node(&t.other.0, &t.other.1), Some(NodeState::ConnectionsDeleting), "what B applied stands");
    assert_eq!(t.after.meshes.get(&t.mesh_id), Some(&MeshState::Ready), "the role taker folds the Mesh status row");
    let mut events = t.after.fabric.get(&t.fabric_id).cloned().unwrap_or_default();
    events.sort();
    assert_eq!(events, vec!["b-own".to_string(), "ready".to_string()], "the union of the events, each once: the Fabric record's ready among them");
    let spans: Vec<Value> = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.status.resolve.via-successor-fold").collect();
    assert_eq!(spans.len(), 1, "one fold, at the role take: {spans:#?}");
    let s = &spans[0];
    assert_eq!(s["trace_id"], t.trace_id.as_str(), "the fold is the cell's own story: {s:#}");
    for (k, v) in [("node_path", "mesh1.node-admin"), ("node_name", "2"), ("mesh", "mesh1"), ("mesh_status_rows", "1"), ("fabric_state", "ready")] {
        assert_eq!(s["attributes"][k], v, "{k}: {s:#}");
    }
}

/// The heartbeat `n`'s exact birth publishes: liveness only, no status (gossip.md §3.1).
fn heartbeat(fabric_id: &FabricId, n: &Node) -> rdm_core::MeshDigest {
    rdm_core::MeshDigest { 
        fabric_id: fabric_id.clone(),
        node: rdm_core::MeshNode { mesh: n.mesh.clone(), r#type: n.r#type.clone(),
            node_id: n.node_id.clone(),
            path: n.path.clone(), name: n.name.clone(),
            endpoint_id: EndpointId(format!("key-{}-{}", n.node_id, "x".repeat(40))),
            transport_addr: "127.0.0.1:20001".parse().unwrap(),
            incarnation: n.incarnation_id.clone().unwrap(),
            supersedes: None,
        },
        status: rdm_core::model::NodeStatus::Pending,
        digest_seq: 7,
        emitted_at_rafka_ms: 7,
        primary: Vec::new(),
    }
}
