//! The status rows of a mesh live on the fabric's store root, written by the mesh's seat holder,
//! and are folded into what an admin applied when it takes the mesh's seat, never at boot by an
//! admin that does not hold it (R14 :8 and :34, R14 item 7, node-identity-and-state.md §4.3).

use rdm_admin::admin::Records;
use rdm_admin::model::*;
use rdm_admin::status_storage::{FabricEventRow, FileStatusStorage, MeshStatusRow, NodeDeclaredRow, StatusStorage};
use rdm_admin::topology::Topology;
use rdm_core::status::{MeshState, NodeState};
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
    before: rdm_admin::status_rpc::Declared,
    after: rdm_admin::status_rpc::Declared,
    rpc: (NodeId, IncarnationId),
    other: (NodeId, IncarnationId),
    mesh_id: MeshId,
    fabric_id: FabricId,
    trace_id: String,
}

async fn seat_taken_over_one_root(trace_id: String) -> Taken {
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
    let a = FileStatusStorage::open(&root, "mesh1", &IncarnationId::mint()).await.unwrap();
    a.put_mesh_status(&MeshStatusRow { mesh_id: mesh_id.clone(), state: MeshState::ReadyForTraffic }).await.unwrap();
    a.put_fabric_event(&FabricEventRow { fabric_id: fabric_id.clone(), event: "shutdown-initiated:mesh1.admin.1".into() }).await.unwrap();
    a.put_node_declared(&NodeDeclaredRow { node_id: rpc_birth.0.clone(), incarnation_id: rpc_birth.1.clone(), name: rpc.name.clone(), state: NodeState::ReadyForTraffic }).await.unwrap();
    a.put_node_declared(&NodeDeclaredRow { node_id: other.0.clone(), incarnation_id: other.1.clone(), name: "mesh1.rpc.2".parse().unwrap(), state: NodeState::Pending }).await.unwrap();
    let on_root = |d: &str| std::fs::read_dir(root.status("mesh1").join(d)).map(|e| e.filter_map(|e| e.ok()).filter(|e| e.path().extension().is_some_and(|x| x == "json")).count()).unwrap_or(0);
    assert_eq!((on_root("meshes"), on_root("fabric"), on_root("declared")), (1, 1, 2), "A's rows live on the fabric's store root under status/mesh1/");
    // B, the other admin of mesh1, with its own store instance over the same root.
    let b_id = before_view.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.2").unwrap().node_id.clone();
    let records = Records::default();
    records.set_status(Arc::new(FileStatusStorage::open(&root, "mesh1", &IncarnationId::mint()).await.unwrap()), "mesh1".into(), "mesh1.admin.2".parse().unwrap(), b_id.clone());
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
/// :34 layout `status/<mesh>/`, item 7: the rebuild on a seat change reads the shared root;
/// node-identity-and-state.md §4.3: the mesh-primary writes every status of its mesh): A, mesh1's
/// primary, applied a Mesh status, a Fabric event and two node declared states, each a row on the
/// fabric's store root. B, the other admin of mesh1 with its own store instance over that root,
/// folds none of them while A holds the seat: what B holds is only what B applied. When B takes the
/// seat, the view naming it mesh1's primary is installed with A's rows folded in, merged with what
/// B applied: the greatest state per key (B's own later state stands over A's earlier row), the
/// union of the events. One span, `rdm.node_admin.status.resolve.via-seat-taken`, names the mesh
/// and the counts folded.
#[test]
fn a_seat_taker_folds_the_mesh_status_rows_and_a_non_holder_folds_none() {
    let cap = crate::mesh_delete_resume::capture();
    let t = cap.run(async {
        let root_span = tracing::info_span!("rdm.test.status.resolve.via-seat-taken");
        let trace_id = crate::mesh_delete_resume::trace_id(&root_span);
        tracing::Instrument::instrument(seat_taken_over_one_root(trace_id), root_span).await
    });
    println!("status_seat_fold trace_id={}", t.trace_id);
    assert_eq!(t.before.node(&t.rpc.0, &t.rpc.1), None, "a non-holder folds none of the primary's rows: {:#?}", t.before);
    assert!(t.before.meshes.is_empty(), "a non-holder folds no Mesh status row: {:#?}", t.before);
    assert_eq!(t.before.fabric.get(&t.fabric_id), Some(&vec!["b-own".to_string()]), "a non-holder holds only the events it applied: {:#?}", t.before);
    assert_eq!(t.after.node(&t.rpc.0, &t.rpc.1), Some(NodeState::ReadyForTraffic), "the seat taker folds the primary's declared-state row: {:#?}", t.after);
    assert_eq!(t.after.names.get(&t.rpc).map(|n| n.to_string()), Some("mesh1.rpc.1".to_string()), "and the path.name it was declared at");
    assert_eq!(t.after.node(&t.other.0, &t.other.1), Some(NodeState::ConnectionsDeleting), "the greatest state per birth: what B applied later stands over the row's Pending");
    assert_eq!(t.after.meshes.get(&t.mesh_id), Some(&MeshState::ReadyForTraffic), "the seat taker folds the Mesh status row");
    let mut events = t.after.fabric.get(&t.fabric_id).cloned().unwrap_or_default();
    events.sort();
    assert_eq!(events, vec!["b-own".to_string(), "shutdown-initiated:mesh1.admin.1".to_string()], "the union of the events, each once");
    let spans: Vec<Value> = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.status.resolve.via-seat-taken").collect();
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
        config: Default::default(),
        load: None,
        gossip: None,
    }
}

/// CONTRACT (node-status-heartbeat-connections.md §A.3: "the successor acquires the held member
/// statuses and publishes a complete projection under its own epoch before any delta"; fabric-authority.md
/// §7: a successor mesh primary hydrates the lifecycle state that already exists, adopts that exact
/// state and republishes the adopted projection, manufacturing no transition; mesh-investigation.md "A
/// node-admin joins the live mesh" step 4): B, an admin of mesh1 whose book holds `mesh1.rpc.1`'s
/// heartbeat and no status for it (an admin entering a live mesh: its topology read installs nothing,
/// reenter.rs), takes mesh1's seat. A, the primary before it, left a declared-state row for that exact
/// birth saying ReadyForTraffic, and one for `mesh1.rpc.2`, whose heartbeat B hears only after it took
/// the seat. The fold of A's rows reaches B's book: both births are ReadyForTraffic there and in B's
/// member projection. `mesh1.rpc.1` declaring ReadyForTraffic to its new primary (R-S2: a declaration is
/// owed again when the authority moved) is AlreadyApplied, and the book still says ReadyForTraffic. One
/// `rdm.node_admin.status.update.via-seat-fold` per adopted birth names it, its prior and its status.
/// Must NOT happen (i143_acceptance_2803, fabric recovery): the successor's projection says a surviving
/// member Pending while its record says ReadyForTraffic, so the member's declaration is AlreadyApplied
/// and the member stays Pending under the new primary for good.
#[test]
fn a_seat_taker_adopts_the_folded_member_statuses_into_its_book_and_projection() {
    use rdm_core::MemberStatus;
    use rdm_transport::membership::{Backbone, Membership};
    use rdm_core::status::{StatusReply, StatusRequest};
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, after_take, rpc2_heard, projected, declared, after_declare) = cap.run(async {
        let root_span = tracing::info_span!("rdm.test.status.update.via-seat-fold");
        let trace_id = crate::mesh_delete_resume::trace_id(&root_span);
        tracing::Instrument::instrument(
            async move {
                let dir = std::env::temp_dir().join(format!("status-seat-adopt-{}", NodeId::mint()));
                let (mesh_id, fabric_id) = (MeshId::mint(), FabricId::mint());
                let mut before_view = view(&mesh_id, &fabric_id);
                before_view.nodes.push(node("mesh1.rpc.2", false, false));
                let by_name = |v: &Topology, name: &str| v.nodes.iter().find(|n| n.name.to_string() == name).cloned().unwrap();
                let (rpc1, rpc2, b) = (by_name(&before_view, "mesh1.rpc.1"), by_name(&before_view, "mesh1.rpc.2"), by_name(&before_view, "mesh1.admin.2"));
                // A's rows on the fabric's store root: both members ReadyForTraffic, as A applied them.
                let root = rdm_admin::store_root::StoreRoot::at(&dir);
                let a = FileStatusStorage::open(&root, "mesh1", &IncarnationId::mint()).await.unwrap();
                for n in [&rpc1, &rpc2] {
                    a.put_node_declared(&NodeDeclaredRow { node_id: n.node_id.clone(), incarnation_id: n.incarnation_id.clone().unwrap(), name: n.name.clone(), state: NodeState::ReadyForTraffic }).await.unwrap();
                }
                // B: its membership's book, its seat fold over the same root.
                let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).await.unwrap();
                let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
                let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
                let membership = Membership::join(&gossip, &ep, &fabric_id, "mesh1", &mesh_id, "mesh1.admin.2", Arc::new(crate::adopted_time()), vec![], &[]).await.unwrap();
                let records = Records::default();
                records.set_book(membership.book.clone());
                records.set_status(Arc::new(FileStatusStorage::open(&root, "mesh1", &IncarnationId::mint()).await.unwrap()), "mesh1".into(), b.name.clone(), b.node_id.clone());
                assert!(membership.book.record(heartbeat(&fabric_id, &rpc1)), "rpc.1's heartbeat is held");
                let status = |n: &Node| membership.book.get(n.node_id.as_str()).map(|(d, _)| d.status);
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
                membership.set_mesh_primary(true);
                let after_take = status(&rpc1);
                assert!(membership.book.record(heartbeat(&fabric_id, &rpc2)), "rpc.2's heartbeat is held after the seat take");
                let rpc2_heard = status(&rpc2);
                let backbone = Backbone::join(&gossip, &ep, &membership, "mesh1", rdm_core::PublisherId { node: "mesh1.admin.2".to_string(), incarnation: b.incarnation_id.clone().unwrap(), node_id: b.node_id.clone() }, vec![]).await.unwrap();
                let mut projected: Vec<(String, MemberStatus)> = backbone.own_members(&membership).into_iter().filter(|d| d.node.name.kind == rdm_core::NodeKind::RpcNode).map(|d| (d.node.name.to_string(), d.status)).collect();
                projected.sort_by(|a, b| a.0.cmp(&b.0));
                // rpc.1 declares to its new primary.
                let authority = rdm_admin::status_rpc::StatusAuthority {
                    certs: None,
                    app_channels: None,
                    held_takeover: Default::default(),
                    declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
                    stopped: Default::default(),
                    me: b.name.clone(),
                    node_id: b.node_id.clone(),
                    fabric_id: fabric_id.clone(),
                    topology: topology.clone(),
                    declared: records.declared.clone(),
                    status_storage: records_status_storage(&root).await,
                    nodes_storage: Arc::new(rdm_admin::storage::MemoryNodesStorage::default()),
                    mesh_ids: Arc::new(std::collections::BTreeMap::new),
                    durable: Default::default(),
                    republish: Arc::new(std::sync::OnceLock::new()),
                    commands: Arc::new(rdm_admin::node_commands::CommandBook::default()),
                    own: Arc::new(std::sync::OnceLock::new()),
                    mesh_deleter: Arc::new(std::sync::OnceLock::new()),
                    hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    wake: Default::default(),
                    rounds: Default::default(),
                    configs: Default::default(),
                    configure: Default::default(),
                    before_ready: Default::default(),
                    rename: Default::default(),
                    row_moved: Default::default(),
                    loads: None,
                    book: Some(membership.book.clone()),
                    builds: std::sync::Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
                    first_ready: Default::default(),
                };
                let declared = authority.apply(Some(rpc1.clone()), &StatusRequest::DeclareNodeState { node_id: rpc1.node_id.clone(), incarnation: rpc1.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic, operation: None }).await;
                let after_declare = status(&rpc1);
                let _ = std::fs::remove_dir_all(&dir);
                (trace_id, after_take, rpc2_heard, projected, declared, after_declare)
            },
            root_span,
        )
        .await
    });
    println!("status_seat_fold adopt trace_id={trace_id}");
    assert_eq!(after_take, Some(MemberStatus::ReadyForTraffic), "the seat taker's book adopts the folded ReadyForTraffic of the birth it held Pending");
    assert_eq!(rpc2_heard, Some(MemberStatus::ReadyForTraffic), "a birth whose row was folded before its heartbeat arrived is held at the adopted status");
    assert_eq!(projected, vec![("mesh1.rpc.1".to_string(), MemberStatus::ReadyForTraffic), ("mesh1.rpc.2".to_string(), MemberStatus::ReadyForTraffic)], "the successor's member projection carries the adopted statuses");
    assert_eq!(declared, StatusReply::AlreadyApplied, "the member's re-declaration to its new primary is already held");
    assert_eq!(after_declare, Some(MemberStatus::ReadyForTraffic), "and the book agrees with the record");
    let adopted: Vec<Value> = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.status.update.via-seat-fold").collect();
    let mut members: Vec<String> = adopted.iter().map(|s| s["attributes"]["member"].as_str().unwrap_or_default().to_string()).collect();
    members.sort();
    assert_eq!(members, vec!["mesh1.rpc.1".to_string(), "mesh1.rpc.2".to_string()], "one adoption per folded birth of the mesh: {adopted:#?}");
    for s in &adopted {
        assert_eq!(s["trace_id"], trace_id.as_str(), "the adoption is the cell's own story: {s:#}");
        assert_eq!(s["attributes"]["node"], "mesh1.admin.2", "{s:#}");
        assert_eq!(s["attributes"]["status"], "ReadyForTraffic", "{s:#}");
    }
}

/// A status store over `root`'s mesh1 rows, as the seat holder's status authority writes through it.
async fn records_status_storage(root: &rdm_admin::store_root::StoreRoot) -> Arc<dyn StatusStorage> {
    Arc::new(FileStatusStorage::open(root, "mesh1", &IncarnationId::mint()).await.unwrap())
}

/// Q49: cycle state rows may be cleared, but an applied operation survives a disk reopen and
/// a successor writer. Attempts have separate identities; only forgetting the birth removes them.
#[tokio::test]
async fn applied_operations_survive_a_cycle_begin_and_a_successor_reopening_the_store() {
    use rdm_admin::status_storage::NodeOperationRow;
    let dir = std::env::temp_dir().join(format!("operation-status-{}", NodeId::mint()));
    let root = rdm_admin::store_root::StoreRoot::at(&dir);
    let node_id = NodeId::mint();
    let incarnation = IncarnationId::mint();
    let row = NodeOperationRow {
        node_id: node_id.clone(), incarnation_id: incarnation.clone(), name: "mesh1.rpc.1".parse().unwrap(), state: NodeState::Stopped,
        operation: rdm_core::status::DeclaredOperation { build_id: "b".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() },
    };
    let next = NodeOperationRow { operation: rdm_core::status::DeclaredOperation { attempt: 2, ..row.operation.clone() }, ..row.clone() };
    {
        let store = FileStatusStorage::open(&root, "mesh1", &IncarnationId::mint()).await.unwrap();
        store.put_node_declared(&NodeDeclaredRow { node_id: node_id.clone(), incarnation_id: incarnation.clone(), name: row.name.clone(), state: NodeState::Stopped }).await.unwrap();
        store.put_node_operation(&row).await.unwrap();
        store.put_node_operation(&next).await.unwrap();
        store.remove_node_declared(&node_id, &incarnation).await.unwrap();
    }
    let successor = FileStatusStorage::open(&root, "mesh1", &IncarnationId::mint()).await.unwrap();
    let held = rdm_admin::status_rpc::Declared::rehydrate(&successor).await.unwrap();
    assert_eq!(held.node(&node_id, &incarnation), None, "only the current cycle's state was cleared");
    assert_eq!(held.operations.len(), 2, "both attempts are still applied");
    assert_eq!(held.operations.get(&row.key()), Some(&row));
    assert_eq!(held.operations.get(&next.key()), Some(&next));
    for applied in held.operations.values() { successor.remove_node_operation(applied).await.unwrap(); }
    assert!(successor.node_operations().await.unwrap().is_empty(), "the operation rows are forgotten with the birth");
    let _ = std::fs::remove_dir_all(dir);
}

/// Q53: an acknowledged Ready for an exact birth survives acquisition by a new
/// mesh-primary and is in its first complete member projection. Exercise the
/// real pre-state-commit store as well as the already-published control.
#[test]
fn an_acknowledged_ready_birth_is_in_its_successors_first_complete_projection() {
    use rdm_core::status::{StatusReply, StatusRequest};
    use rdm_core::MemberStatus;
    use rdm_transport::membership::{Backbone, Membership};
    let cap = crate::mesh_delete_resume::capture();
    let observed = cap.run(async {
        let mut observed = Vec::new();
        for published in [true, false] {
            let parent = tracing::info_span!("rdm.test.status.update.via-acknowledged-successor", published);
            let trace = crate::mesh_delete_resume::trace_id(&parent);
            let result = tracing::Instrument::instrument(async {
                let dir = std::env::temp_dir().join(format!("status-ack-successor-{}", NodeId::mint()));
                let (mesh_id, fabric_id) = (MeshId::mint(), FabricId::mint());
                let mut before = view(&mesh_id, &fabric_id);
                let a = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").unwrap().clone();
                let b = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.2").unwrap().clone();
                let rpc = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").unwrap().clone();
                let pad = rdm_admin::record_store::Scratchpad::new();
                let held_root = rdm_admin::store_root::StoreRoot::held_at(&dir, pad.clone());
                let storage = Arc::new(FileStatusStorage::open(&held_root, "mesh1", a.incarnation_id.as_ref().unwrap()).await.unwrap());
                let authority = rdm_admin::status_rpc::StatusAuthority {
                    certs: None,
                    app_channels: None,
                    held_takeover: Default::default(), declaration_seam: None, command_frame_seam: None,
                    operation_effects: Default::default(), stopped: Default::default(),
                    me: a.name.clone(), node_id: a.node_id.clone(), fabric_id: fabric_id.clone(),
                    topology: Arc::new(RwLock::new(before.clone())), declared: Default::default(),
                    status_storage: storage.clone(), nodes_storage: Arc::new(rdm_admin::storage::MemoryNodesStorage::default()),
                    mesh_ids: Arc::new(std::collections::BTreeMap::new), durable: Default::default(), republish: Default::default(),
                    commands: Arc::new(rdm_admin::node_commands::CommandBook::default()), own: Default::default(),
                    mesh_deleter: Default::default(), hold_next_reply: Default::default(), wake: Default::default(),
                    rounds: Default::default(), configs: Default::default(), configure: Default::default(),
                    before_ready: Default::default(), rename: Default::default(), row_moved: Default::default(),
                    loads: None, book: None, builds: Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
                    first_ready: Default::default(),
                };
                let reply = authority.apply(Some(rpc.clone()), &StatusRequest::DeclareNodeState {
                    node_id: rpc.node_id.clone(), incarnation: rpc.incarnation_id.clone().unwrap(),
                    state: NodeState::ReadyForTraffic, operation: None,
                }).await;
                assert_eq!(reply, StatusReply::Applied, "the predecessor acknowledged this exact birth's Ready");
                assert_eq!(storage.node_declared().await.unwrap().len(), 1, "the predecessor reads the acknowledged row");
                let disk = rdm_admin::store_root::StoreRoot::at(&dir);
                let independent = FileStatusStorage::open(&disk, "mesh1", &IncarnationId::mint()).await.unwrap();
                assert_eq!(independent.node_declared().await.unwrap().len(), 1, "Applied already made the row durable before publication");
                storage.put_mesh_status(&MeshStatusRow { mesh_id: mesh_id.clone(), state: MeshState::ReadyForTraffic }).await.unwrap();
                assert!(independent.mesh_statuses().await.unwrap().is_empty(), "Q54 does not publish the other held rows");
                if published {
                    assert_eq!(pad.publish().await.unwrap().written(), 1, "only the held mesh row remains to publish");
                    assert_eq!(independent.mesh_statuses().await.unwrap().len(), 1);
                }
                drop(independent);
                drop(authority);
                drop(storage);
                drop(held_root);
                drop(pad);
                // A successor is an independent process: it cannot read the lost
                // predecessor's scratchpad. Its root has already been established.
                let root = rdm_admin::store_root::StoreRoot::at(&dir);
                let successor = Arc::new(FileStatusStorage::open(&root, "mesh1", b.incarnation_id.as_ref().unwrap()).await.unwrap());
                let rows = successor.node_declared().await.unwrap().len();
                let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).await.unwrap();
                let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
                let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
                let membership = Membership::join(&gossip, &ep, &fabric_id, "mesh1", &mesh_id, "mesh1.admin.2", Arc::new(crate::adopted_time()), vec![], &[]).await.unwrap();
                let records = Records::default();
                records.set_book(membership.book.clone());
                records.set_status(successor, "mesh1".into(), b.name.clone(), b.node_id.clone());
                assert!(membership.book.record(heartbeat(&fabric_id, &rpc)));
                before.nodes.retain(|n| n.node_id != a.node_id);
                for n in &mut before.nodes { if n.node_id == b.node_id { n.is_primary = true; n.is_fabric_primary = true; } }
                let topology = RwLock::new(Topology { nodes: Vec::new(), ..before.clone() });
                records.install_projected(&topology, || before.clone()).await;
                membership.set_mesh_primary(true);
                let backbone = Backbone::join(&gossip, &ep, &membership, "mesh1", rdm_core::PublisherId {
                    node: b.name.to_string(), incarnation: b.incarnation_id.clone().unwrap(), node_id: b.node_id.clone(),
                }, vec![]).await.unwrap();
                let first = backbone.own_members(&membership).into_iter().find(|d| d.node.node_id == rpc.node_id && Some(&d.node.incarnation) == rpc.incarnation_id.as_ref()).unwrap();
                let _ = std::fs::remove_dir_all(&dir);
                (rows, first.status)
            }, parent).await;
            observed.push((published, trace, result));
        }
        observed
    });
    for row in cap.spans() { println!("Q53_SPAN {}", serde_json::to_string(&row).unwrap()); }
    for (published, trace, (rows, status)) in observed {
        println!("Q53 published={published} trace={trace} rows={rows} first={status:?}");
        assert_eq!((rows, status), (1, MemberStatus::ReadyForTraffic), "Q53: acknowledged exact-birth Ready is in the successor's first full; published={published}, trace={trace}");
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
        let storage = Arc::new(FileStatusStorage::open(&root, "mesh1", b.incarnation_id.as_ref().unwrap()).await.unwrap());
        let exact = (rpc.node_id.clone(), rpc.incarnation_id.clone().unwrap());
        let stale = (rpc.node_id.clone(), IncarnationId::mint());
        let absent = (NodeId::mint(), IncarnationId::mint());
        for (node_id, incarnation_id) in [&exact, &stale, &absent] {
            storage.put_node_declared(&NodeDeclaredRow { node_id: node_id.clone(), incarnation_id: incarnation_id.clone(), name: rpc.name.clone(), state: NodeState::ReadyForTraffic }).await.unwrap();
        }
        let records = Records::default();
        records.set_status(storage, "mesh1".into(), b.name.clone(), b.node_id.clone());
        let topology = RwLock::new(Topology { nodes: Vec::new(), ..now.clone() });
        records.install_projected(&topology, || now.clone()).await;
        let held = records.declared.lock().unwrap();
        assert_eq!(held.node(&exact.0, &exact.1), Some(NodeState::ReadyForTraffic));
        assert_eq!(held.node(&stale.0, &stale.1), None, "a previous incarnation is not held");
        assert_eq!(held.node(&absent.0, &absent.1), None, "a matching name does not establish a birth");
        drop(held);
        let _ = std::fs::remove_dir_all(dir);
    });
}
