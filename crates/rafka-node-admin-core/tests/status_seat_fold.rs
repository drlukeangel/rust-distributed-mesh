//! The status rows of a mesh live on the fabric's store root, written by the mesh's seat holder,
//! and are folded into what an admin applied when it takes the mesh's seat, never at boot by an
//! admin that does not hold it (R14 :8 and :34, R14 item 7, fabric-node-lifecycle.md §4.3).

use rafka_node_admin_core::admin::Records;
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::status_storage::{FabricEventRow, FileStatusStorage, MeshStatusRow, NodeDeclaredRow, StatusStorage};
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc_contract::status::{MeshState, NodeState};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// mesh1 with two admins (`mesh1.admin.1` primary of mesh1 and the fabric) and one rpc node.
fn view(mesh_id: &MeshId, fabric_id: &FabricId) -> Topology {
    Topology {
        fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(mesh_id.clone()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![node("mesh1.admin.1", true, true), node("mesh1.admin.2", false, false), node("mesh1.rpc.1", false, false)],
    }
}

/// What B held before and after it took mesh1's seat, and the cell's trace id.
struct Taken {
    before: rafka_node_admin_core::status_rpc::Declared,
    after: rafka_node_admin_core::status_rpc::Declared,
    rpc: (NodeId, IncarnationId),
    other: (NodeId, IncarnationId),
    mesh_id: MeshId,
    fabric_id: FabricId,
    trace_id: String,
}

async fn seat_taken_over_one_root(trace_id: String) -> Taken {
    let dir = std::env::temp_dir().join(format!("status-seat-fold-{}", NodeId::mint()));
    let (mesh_id, fabric_id) = (MeshId::mint(), FabricId::mint());
    let before_view = view(&mesh_id, &fabric_id);
    let rpc = before_view.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
    let rpc_birth = (rpc.node_id.clone(), rpc.incarnation_id.clone().unwrap());
    let other = (NodeId::mint(), IncarnationId::mint());
    // A, mesh1's primary, applied these as the mesh's authority: its rows on the shared root.
    let root = rafka_node_admin_core::store_root::StoreRoot::at(&dir);
    let a = FileStatusStorage::open(&root, "mesh1", &IncarnationId::mint()).unwrap();
    a.put_mesh_status(&MeshStatusRow { mesh_id: mesh_id.clone(), state: MeshState::ReadyForTraffic }).await.unwrap();
    a.put_fabric_event(&FabricEventRow { fabric_id: fabric_id.clone(), event: "shutdown-initiated:mesh1.admin.1".into() }).await.unwrap();
    a.put_node_declared(&NodeDeclaredRow { node_id: rpc_birth.0.clone(), incarnation_id: rpc_birth.1.clone(), name: rpc.name.clone(), state: NodeState::ReadyForTraffic }).await.unwrap();
    a.put_node_declared(&NodeDeclaredRow { node_id: other.0.clone(), incarnation_id: other.1.clone(), name: "mesh1.rpc.2".parse().unwrap(), state: NodeState::Pending }).await.unwrap();
    let on_root = |d: &str| std::fs::read_dir(root.status("mesh1").join(d)).map(|e| e.filter_map(|e| e.ok()).filter(|e| e.path().extension().is_some_and(|x| x == "json")).count()).unwrap_or(0);
    assert_eq!((on_root("meshes"), on_root("fabric"), on_root("declared")), (1, 1, 2), "A's rows live on the fabric's store root under status/mesh1/");
    // B, the other admin of mesh1, with its own store instance over the same root.
    let b_id = before_view.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.2").unwrap().node_id.clone();
    let records = Records::default();
    records.set_status(Arc::new(FileStatusStorage::open(&root, "mesh1", &IncarnationId::mint()).unwrap()), "mesh1".into(), b_id.clone());
    let topology = RwLock::new(Topology { nodes: Vec::new(), ..before_view.clone() });
    // B installs views naming A the primary: it holds no seat and folds none of A's rows.
    records.install_projected(&topology, || before_view.clone()).await;
    // What B applied itself meanwhile: a later state for the other birth and an event of its own.
    {
        let mut d = records.declared.lock().unwrap();
        d.nodes.insert(other.clone(), NodeState::Draining);
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
    records.install_projected(&topology, || after_view).await;
    let after = records.declared.lock().unwrap().clone();
    let _ = std::fs::remove_dir_all(&dir);
    Taken { before, after, rpc: rpc_birth, other, mesh_id, fabric_id, trace_id }
}

/// CONTRACT (R14 :8 status is one of the interfaces "every writer and reader sees the same rows",
/// :34 layout `status/<mesh>/`, item 7: the rebuild on a seat change reads the shared root;
/// fabric-node-lifecycle.md §4.3: the mesh-primary writes every status of its mesh): A, mesh1's
/// primary, applied a Mesh status, a Fabric event and two node declared states, each a row on the
/// fabric's store root. B, the other admin of mesh1 with its own store instance over that root,
/// folds none of them while A holds the seat: what B holds is only what B applied. When B takes the
/// seat, the view naming it mesh1's primary is installed with A's rows folded in, merged with what
/// B applied: the greatest state per key (B's own later state stands over A's earlier row), the
/// union of the events. One span, `rdm.node_admin.status.resolve.via-seat-taken`, names the mesh
/// and the counts folded.
#[test]
fn a_seat_taker_folds_the_mesh_status_rows_and_a_non_holder_folds_none() {
    let cap = crate::mesh_leave_resume::capture();
    let t = cap.run(async {
        let root_span = tracing::info_span!("rdm.test.status.resolve.via-seat-taken");
        let trace_id = crate::mesh_leave_resume::trace_id(&root_span);
        tracing::Instrument::instrument(seat_taken_over_one_root(trace_id), root_span).await
    });
    println!("trace_id={}", t.trace_id);
    assert_eq!(t.before.node(&t.rpc.0, &t.rpc.1), None, "a non-holder folds none of the primary's rows: {:#?}", t.before);
    assert!(t.before.meshes.is_empty(), "a non-holder folds no Mesh status row: {:#?}", t.before);
    assert_eq!(t.before.fabric.get(&t.fabric_id), Some(&vec!["b-own".to_string()]), "a non-holder holds only the events it applied: {:#?}", t.before);
    assert_eq!(t.after.node(&t.rpc.0, &t.rpc.1), Some(NodeState::ReadyForTraffic), "the seat taker folds the primary's declared-state row: {:#?}", t.after);
    assert_eq!(t.after.names.get(&t.rpc).map(|n| n.to_string()), Some("mesh1.rpc.1".to_string()), "and the path.name it was declared at");
    assert_eq!(t.after.node(&t.other.0, &t.other.1), Some(NodeState::Draining), "the greatest state per birth: what B applied later stands over the row's Pending");
    assert_eq!(t.after.meshes.get(&t.mesh_id), Some(&MeshState::ReadyForTraffic), "the seat taker folds the Mesh status row");
    let mut events = t.after.fabric.get(&t.fabric_id).cloned().unwrap_or_default();
    events.sort();
    assert_eq!(events, vec!["b-own".to_string(), "shutdown-initiated:mesh1.admin.1".to_string()], "the union of the events, each once");
    let spans: Vec<Value> = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.status.resolve.via-seat-taken").collect();
    assert_eq!(spans.len(), 1, "one fold, at the seat take: {spans:#?}");
    let s = &spans[0];
    assert_eq!(s["trace_id"], t.trace_id.as_str(), "the fold is the cell's own story: {s:#}");
    for (k, v) in [("mesh", "mesh1"), ("node_declared_rows", "2"), ("mesh_status_rows", "1"), ("fabric_event_rows", "1")] {
        assert_eq!(s["attributes"][k], v, "{k}: {s:#}");
    }
}
