//! The node records of a mesh (node-identity-and-state.md §2.5, Q356): each node writes every field of
//! its own record; its mesh-primary writes a node's record only for a node that is lost, killed or
//! deleted (lifecycles/node-delete.md step 3, node-recovery.md step 3), and checks a create's path and
//! name against its view and the creates it has in flight, reading no store (node-create.md, Q227).
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.node-admin.1`, its mesh primary
//! `mesh1.node-admin.2` (the executor of every `mesh1` node) and `mesh1.node.1`.

use rdm_admin::node_rows::RecordAt as _;
use rdm_admin::model::*;
use rdm_admin::node_rows::{NodeRows, RowRefused};
use rdm_admin::storage::{MemoryNodesStorage, NodeRecord, NodesStorage};
use rdm_admin::topology::Topology;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::RwLock;

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// A cell's own span capture: the product span pipeline (`rdm_transport::span_capture`: its
/// span filter and `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher
/// installed on every thread of the cell's own runtime.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("node-rows", exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    fn spans_named(&self, name: &str) -> Vec<Value> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({"name": s.name, "trace_id": s.span_context.trace_id().to_string(), "attributes": attributes})
            })
            .collect()
    }
}

fn view() -> Topology {
    let rpc = node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", false, false);
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", false, true), node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", true, false), rpc],
    }
}

/// The node-admin `me` of the fixture view over `storage`: its record checks and its writes of the
/// records of lost, killed or deleted nodes.
fn admin(storage: &Arc<MemoryNodesStorage>, me: &str) -> (Arc<NodeRows>, Arc<RwLock<Topology>>) {
    let t = view();
    let me: PathName = rdm_core::PathName::new("mesh1.node-admin", me).unwrap();
    let me_id = t.node(&me).expect("the admin is in the view").node_id.clone();
    let view = Arc::new(RwLock::new(t));
    (Arc::new(NodeRows::new(me, me_id, view.clone(), storage.clone())), view)
}

/// The record the fixture view holds at `name`.
async fn held(view: &RwLock<Topology>, name: &PathName) -> Node {
    view.read().await.nodes.iter().find(|n| &n.location() == name).cloned().unwrap()
}

/// The record `n` wrote of itself at its start (Q356).
fn own_record(n: &Node) -> NodeRecord {
    rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), n.incarnation_id.clone().unwrap(), n.status)
}

/// CONTRACT (lifecycles/node-delete.md step 3; node-identity-and-state.md §2.5, Q356: the mesh-primary
/// writes a node's record only for a node that is lost, killed or deleted): `node.delete` leaves the
/// node's record `Deleted`, every other field as the node's latest line holds it, written by the
/// mesh-primary that executes it; the `Deleted` record releases the name. One `via-node-row` span
/// names the write.
#[test]
fn a_deleted_node_leaves_a_deleted_record_with_its_fields() {
    let cap = capture();
    let (written, victim) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let (rows, view) = admin(&storage, "2");
        let victim = held(&view, &PathName::new("mesh1.node", "1").unwrap()).await;
        let mut own = own_record(&victim);
        own.tags = ["kept".to_string()].into();
        storage.put_node(&own).await.unwrap();
        rows.deleted(&victim, &rdm_core::OperationId::node("node.delete", &victim.node_id)).await.expect("the mesh primary writes the Deleted record");
        assert_eq!(storage.record_at(&victim.location()).await.unwrap(), None, "the Deleted record released the name");
        (storage.node(&victim.node_id).await.unwrap().map(|l| l.record), own)
    });
    assert_eq!(written, Some(NodeRecord { state: NodeStatus::Deleted, ..victim.clone() }), "the record says Deleted and keeps every other field of the node's latest line");
    let spans = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!(((a["node_path"].as_str(), a["node_name"].as_str()), (a["subject_path"].as_str(), a["subject_name"].as_str()), a["node_id"].as_str(), a["outcome"].as_str()), ((Some("mesh1.node-admin"), Some("2")), (Some("mesh1.node"), Some("1")), Some(victim.node_id.as_str()), Some("row-deleted")), "{a:#?}");
}

/// CONTRACT (node-identity-and-state.md §2.5, Q356: "Its mesh-primary, or the fabric-primary for a mesh
/// with no mesh-primary, writes a node's record only when the node cannot"): an admin that is not the
/// node's mesh-primary (the fabric-primary of a mesh that has a primary, or another node-admin) writes
/// no record of the node: its delete is refused `NotMeshPrimary` and the node's record stands as the
/// node wrote it. A node that never wrote its record has none to delete: nothing is written and its
/// delete goes on. Must NOT happen: another admin writing a node's record.
#[test]
fn only_the_mesh_primary_writes_the_record_of_a_deleted_node() {
    let cap = capture();
    cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let (fp, view) = admin(&storage, "1");
        let victim = held(&view, &PathName::new("mesh1.node", "1").unwrap()).await;
        let delete = rdm_core::OperationId::node("node.delete", &victim.node_id);
        let refused = fp.deleted(&victim, &delete).await;
        assert!(matches!(refused, Err(RowRefused::NotMeshPrimary(_))), "the fabric-primary of a mesh that has a primary writes none of its records: {refused:?}");
        let (mp, _) = admin(&storage, "2");
        let none = mp.deleted(&victim, &delete).await;
        assert!(none.is_ok(), "a node that never wrote its record has none to delete, and its delete goes on: {none:?}");
        assert!(storage.node(&victim.node_id).await.unwrap().is_none(), "nothing is written for a node that never wrote its record");
        storage.put_node(&own_record(&victim)).await.unwrap();
        assert!(matches!(fp.deleted(&victim, &delete).await, Err(RowRefused::NotMeshPrimary(_))));
        assert_eq!(storage.node(&victim.node_id).await.unwrap().map(|l| l.record.state), Some(NodeStatus::Ready), "the node's record stands as the node wrote it");
    });
}

/// CONTRACT (node-recovery.md step 3: "the node's mesh-primary writes the row with status Deleted";
/// node-identity-and-state.md §2: "A current-pair lookup reads the node records by their path and
/// name"; §2.5, Q356): a recovery's `NodeDeleted` leaves the exited node's record `Deleted` and its name
/// released, so the new node created at the same `(path, name)` writes its own record there at its
/// start, and the pair resolves to it. Before the delete, the exited node's live record holds the
/// name. One `delete.via-node-row` span (`row-deleted`) names the mesh-primary's write.
#[test]
fn a_recovered_nodes_record_is_deleted_and_the_new_node_at_its_path_and_name_writes_its_own() {
    let cap = capture();
    let (exited, born) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let (mp, view) = admin(&storage, "2");
        let exited = held(&view, &PathName::new("mesh1.node", "1").unwrap()).await;
        storage.put_node(&own_record(&exited)).await.unwrap();
        let mut new = node(exited.location(), "mesh1", "node", false, false);
        new.status = NodeStatus::Starting;
        assert_eq!(rdm_admin::node_rows::check_name(&*storage, &exited.location(), &new.node_id).await.unwrap().map_err(|h| h.by), Err(exited.node_id.clone()), "before the delete the exited node's live record holds the name");
        mp.deleted(&exited, &rdm_core::OperationId::node("node.delete", &exited.node_id)).await.expect("the executor writes the Deleted record inside NodeDeleted");
        storage.put_node(&own_record(&new)).await.unwrap();
        let at = storage.record_at(&exited.location()).await.unwrap().expect("the new node's record");
        assert_eq!((at.node_id.clone(), at.state), (new.node_id.clone(), NodeStatus::Starting), "the new node holds the name, its own record written at its start");
        (exited.node_id, new.node_id)
    });
    let deleted = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(deleted.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(exited.as_str()), Some("row-deleted"))], "{deleted:#?}");
    assert_ne!(exited, born);
}

/// CONTRACT (fabric-recovery.md step 3, Q148: "a delete that finds another birth at the path writes
/// nothing"): a recovery's node.delete of the lost birth replayed after its re-created node wrote its
/// own record at the `(path, name)` is refused `name-held`: the new node keeps its record and its name,
/// and the lost birth's record is not written again. Must NOT happen: the replayed delete releases the
/// name the new node holds or writes its record.
#[test]
fn a_replayed_delete_of_the_lost_birth_writes_nothing_against_its_replacement() {
    let cap = capture();
    let (row, exited, born) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let (mp, view) = admin(&storage, "2");
        let exited = held(&view, &PathName::new("mesh1.node", "1").unwrap()).await;
        let delete = &rdm_core::OperationId::node("node.delete", &exited.node_id);
        storage.put_node(&own_record(&exited)).await.unwrap();
        mp.deleted(&exited, delete).await.expect("the recovery's delete writes the Deleted record");
        let mut new = node(exited.location(), "mesh1", "node", false, false);
        new.status = NodeStatus::Starting;
        storage.put_node(&own_record(&new)).await.unwrap();
        let replayed = mp.deleted(&exited, delete).await;
        assert!(matches!(replayed, Err(RowRefused::NameHeld(ref h)) if h.by == new.node_id), "the replayed delete is refused by the replacement's record: {replayed:?}");
        assert_eq!(storage.node(&exited.node_id).await.unwrap().map(|l| l.record.state), Some(NodeStatus::Deleted), "the lost birth's record is not written again");
        (storage.record_at(&exited.location()).await.unwrap(), exited.node_id, new.node_id)
    });
    assert_eq!(row.as_ref().map(|r| (&r.node_id, r.state)), Some((&born, NodeStatus::Starting)), "the replacement's record stands: {row:?}");
    let deleted = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(deleted.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(exited.as_str()), Some("row-deleted")), (Some(exited.as_str()), Some("name-held"))], "{deleted:#?}");
}

/// CONTRACT (lifecycles/node-create.md: "Before this step the mesh-primary checks the path and name
/// against its view and the creates it has in flight"; Q227: no store read at a create): a create at a
/// name another live node of the view holds is refused `NameHeld { by }` naming that node; so is one at
/// a name another create in flight deploys; the birth a recovery replaces does not hold its name
/// against its re-create; a free name passes; an admin that is not the mesh's primary is refused
/// `NotMeshPrimary`. Nothing is read from the store and nothing is written. Must NOT happen: a create
/// launched at a name a live node holds, or a record written by a create.
#[test]
fn a_create_is_checked_against_the_view_and_the_creates_in_flight_and_writes_nothing() {
    let cap = capture();
    cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let (mp, view) = admin(&storage, "2");
        let live = held(&view, &PathName::new("mesh1.node", "1").unwrap()).await;
        let new = NodeId::mint();
        let held_by = |r: Result<(), RowRefused>| match r { Err(RowRefused::NameHeld(h)) => Some(h.by), _ => None };
        assert_eq!(held_by(mp.check_create(&live.location(), "mesh1", "node", &new, None, None).await), Some(live.node_id.clone()), "a live node of the view holds the name");
        assert!(mp.check_create(&live.location(), "mesh1", "node", &new, Some(&live.node_id), None).await.is_ok(), "the birth a recovery replaces does not hold the name against its re-create");
        let free: PathName = rdm_core::PathName::new("mesh1.node", "2").unwrap();
        assert!(mp.check_create(&free, "mesh1", "node", &new, None, None).await.is_ok(), "a free name passes");
        let other = NodeId::mint();
        assert_eq!(held_by(mp.check_create(&free, "mesh1", "node", &new, None, Some(other.clone())).await), Some(other), "another create in flight at the name holds it");
        let (fp, _) = admin(&storage, "1");
        assert!(matches!(fp.check_create(&free, "mesh1", "node", &new, None, None).await, Err(RowRefused::NotMeshPrimary(_))), "an admin that is not the mesh's primary checks no create of it");
        assert!(storage.nodes("mesh1").await.unwrap().is_empty(), "a create's check writes nothing: the node writes its own record at its start");
    });
}

/// CONTRACT (e12.s41 / crit:e12.s41.2; proposal node-primary-update-fabric-mesh-and-node-type.md
/// §A.1.2, §A.2.3; node-identity-and-state.md §2):
/// 1. `type` at `node.create` normalizes `""` to `"node"` and refuses any value outside the shared
///    identifier grammar (`1..=64` of `a-z 0-9 - _ . ~`, no leading `.`, whitespace refused) by name
///    (`type`).
/// 2. `type` is immutable after creation: `node.update` of `type` is refused by name (`type`), leaving
///    the node untouched.
#[test]
fn node_update_of_type_and_an_invalid_type_are_refused_by_name() {
    use rdm_core::status::{normalize_type, type_refusal, type_update_refusal};
    // 1. Empty string normalizes to "node"; valid identifiers pass; invalid values are refused naming "type".
    assert_eq!(normalize_type(""), rdm_core::NODE);
    assert_eq!(normalize_type("broker"), "broker");
    for valid in ["node", "broker", "node-admin", "compute_1", "v1.2~rc"] {
        assert_eq!(type_refusal(valid), None, "{valid} is within the type grammar");
    }
    let over_cap = "a".repeat(65);
    for invalid in [" ", " broker", "broker ", "Broker", ".broker", "broker/1", over_cap.as_str()] {
        let reason = type_refusal(&normalize_type(invalid)).expect("an invalid type is refused");
        assert!(reason.contains("type"), "the refusal names type: {reason}");
    }
    let north_key: PathName = rdm_core::PathName::new("mesh1.broker", "north").expect("free-text node name parses");
    let ws_err = Node::allocated(north_key.clone(), "mesh1", "broker").with_type(" broker").expect_err("a whitespace type is refused");
    assert!(ws_err.contains("type"), "Node::with_type's refusal names type: {ws_err}");
    assert_eq!(Node::allocated(north_key.clone(), "mesh1", "broker").with_type("").unwrap().r#type, "node", "an empty type is `node`");

    // 2. `node.update` of `type` is refused by name (`type`), leaving the type untouched.
    let mut north_node = Node::allocated(north_key, "mesh1", "broker").with_type("broker").unwrap();
    for attempted in ["node", "broker", "Broker"] {
        let update_err = north_node.update_type(attempted).expect_err("Node::update_type is refused");
        assert!(update_err.contains("type") && update_err.contains("immutable") && update_err.contains("node.update"), "the refusal names type, immutable and node.update: {update_err}");
    }
    assert_eq!(north_node.r#type, "broker");
    let contract_msg = type_update_refusal("broker", "node");
    assert!(contract_msg.contains("type") && contract_msg.contains("node.update"), "{contract_msg}");
}

/// CONTRACT (node-identity-and-state.md §2: "the path and the name are free text set at node.create",
/// `type` immutable; §2.5, Q356: each node writes every field of its own record; Q366: a node's mesh is
/// the first location of its `path`; node-recovery.md "Retained": path and name): a node created at the
/// free-text name `north` under `mesh1.broker`, of type `broker`, keeps its `name`, `path` and `type`
/// on its record and in `node.get` (its mesh `mesh1`, read from the path) through:
/// 1. `create`: the mesh-primary checks the pair against its view, writing nothing, and the node writes
///    its own record at its start;
/// 2. `get`, by its id;
/// 3. `stop` and `start`: the node writes each state on its own record (stopped, then ready);
/// 4. `recovery`: the mesh-primary writes the lost node's record `Deleted`, and the new node at the
///    same path and name writes its own record there, the predecessor's config and tags its launch's;
/// 5. `replace`: the recovered node writes its own record under the name `north-old`, and a successor
///    created at the released name `north` writes its own.
/// Must NOT happen: a name, path or type changed by any of these, or a record written by a create.
#[test]
fn a_node_created_with_a_free_text_name_keeps_name_path_and_type_through_create_get_stop_start_recovery_and_replace() {
    use rdm_core::filter::Filter;
    use rdm_core::status::StatusReply;
    use rdm_node::node_records::NodePropertyWriter;
    use std::collections::BTreeMap;
    let cap = capture();
    cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let (mp, view) = admin(&storage, "2");
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        let get = |id: NodeId| {
            let (view, storage, me) = (view.clone(), storage.clone(), me.clone());
            async move {
                let view = view.read().await.clone();
                let meshes = rdm_admin::storage::MemoryMeshStorage::default();
                crate::write_meshes(&view, &meshes).await;
                let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
                let atlas = rdm_admin::atlas::Atlas::default();
                let current = rdm_admin::current_record::CurrentRecords { nodes: &*storage };
                let StatusReply::Nodes { nodes } = rdm_admin::views::nodes_answer_with_status(&me, &view, &loads, &atlas, &Filter::new().eq("id", [id.to_string()]), Some(current)).await else {
                    panic!("node.get answers Nodes");
                };
                assert_eq!(nodes.len(), 1, "node.get by id answers the one node: {nodes:#?}");
                let n = &nodes[0];
                ((n.path.clone(), n.name.clone(), n.r#type.clone(), n.mesh()), n.config.clone(), n.tags.clone())
            }
        };
        let kept = |name: &str| ("mesh1.broker".to_string(), name.to_string(), "broker".to_string(), "mesh1".to_string());
        let set = |id: NodeId, status: NodeStatus, stopped: bool| {
            let view = view.clone();
            async move {
                let mut v = view.write().await;
                let n = v.nodes.iter_mut().find(|n| n.node_id == id).unwrap();
                (n.status, n.stopped, n.routable) = (status, stopped, !stopped);
            }
        };
        let config: BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
        let tags: std::collections::BTreeSet<String> = ["blue".to_string()].into();

        // 1. Create: the free-text name `north` at path `mesh1.broker`, type `broker`.
        let north_at: PathName = rdm_core::PathName::new("mesh1.broker", "north").expect("a free-text node name parses");
        let north = node(north_at.clone(), "mesh1", "broker", false, false).with_type("broker").expect("a valid type");
        mp.check_create(&north_at, "mesh1", "broker", &north.node_id, None, None).await.expect("the free-text pair is free");
        assert!(storage.nodes("mesh1").await.unwrap().is_empty(), "the create's check writes nothing");
        view.write().await.nodes.push(north.clone());
        let writer = NodePropertyWriter::new(storage.clone(), || "memory".into());
        let mut start = own_record(&north);
        (start.state, start.config, start.tags) = (NodeStatus::Starting, config.clone(), tags.clone());
        writer.bound(start, |_| {}).await.expect("the node writes its own record at its start");
        writer.state(writer.start_record().unwrap(), NodeStatus::Ready).await.unwrap();
        let record = storage.record_at(&north_at).await.unwrap().expect("the node's own record at mesh1.broker.north");
        assert_eq!((record.path.clone(), record.name.clone(), record.r#type.clone(), record.mesh(), record.state), ("mesh1.broker".to_string(), "north".to_string(), "broker".to_string(), "mesh1".to_string(), NodeStatus::Ready));

        // 2. Get: name, path and type, the mesh from the path, the node's own config and tags.
        assert_eq!(get(north.node_id.clone()).await, (kept("north"), config.clone(), tags.clone()));

        // 3. Stop and start: the node writes each state on its own record.
        for to in [NodeStatus::Stopping, NodeStatus::ConnectionsDeleting, NodeStatus::ConnectionsDeleted, NodeStatus::Leaving, NodeStatus::Left, NodeStatus::Stopped] {
            writer.state(writer.start_record().unwrap(), to).await.unwrap();
        }
        set(north.node_id.clone(), NodeStatus::Leaving, true).await;
        assert_eq!(storage.node(&north.node_id).await.unwrap().map(|l| l.record.state), Some(NodeStatus::Stopped), "the node wrote stopped on its own record");
        assert_eq!(get(north.node_id.clone()).await.0, kept("north"), "a stopped node keeps its name, path and type");
        writer.state(writer.start_record().unwrap(), NodeStatus::Starting).await.unwrap();
        writer.state(writer.start_record().unwrap(), NodeStatus::Ready).await.unwrap();
        set(north.node_id.clone(), NodeStatus::Ready, false).await;
        assert_eq!(storage.node(&north.node_id).await.unwrap().map(|l| l.record.state), Some(NodeStatus::Ready), "the node wrote ready on its own record at its start");
        assert_eq!(get(north.node_id.clone()).await.0, kept("north"), "a started node keeps its name, path and type");

        // 4. Recovery: the mesh-primary writes the lost node's record Deleted; the new node at the same
        //    path and name writes its own, the predecessor's config and tags its launch's.
        mp.deleted(&north, &rdm_core::OperationId::node("node.delete", &north.node_id)).await.expect("the mesh-primary writes the lost node's record Deleted");
        let lost = storage.node(&north.node_id).await.unwrap().expect("the lost node's record").record;
        assert_eq!((lost.state, lost.path.clone(), lost.name.clone(), lost.r#type.clone()), (NodeStatus::Deleted, "mesh1.broker".to_string(), "north".to_string(), "broker".to_string()), "the Deleted record keeps name, path and type");
        view.write().await.nodes.retain(|n| n.node_id != north.node_id);
        let recovered = node(north_at.clone(), "mesh1", "broker", false, false).with_type("broker").unwrap();
        mp.check_create(&north_at, "mesh1", "broker", &recovered.node_id, Some(&north.node_id), None).await.expect("the lost birth does not hold the name against its re-create");
        view.write().await.nodes.push(recovered.clone());
        let recovered_writer = NodePropertyWriter::new(storage.clone(), || "memory".into());
        let mut start = own_record(&recovered);
        (start.state, start.config, start.tags) = (NodeStatus::Starting, lost.config.clone(), lost.tags.clone());
        recovered_writer.bound(start, |_| {}).await.expect("the recovered node writes its own record at its start");
        recovered_writer.state(recovered_writer.start_record().unwrap(), NodeStatus::Ready).await.unwrap();
        let at = storage.record_at(&north_at).await.unwrap().expect("the recovered node's record at mesh1.broker.north");
        assert_eq!((at.node_id.clone(), at.r#type.clone()), (recovered.node_id.clone(), "broker".to_string()), "the pair resolves to the recovered node, of its type");
        assert_eq!(get(recovered.node_id.clone()).await, (kept("north"), config.clone(), tags.clone()), "the recovered node keeps name, path and type, and the predecessor's config and tags");

        // 5. Replace: the recovered node writes its own record under `north-old`; a successor created at
        //    the released name `north` writes its own.
        recovered_writer.update(recovered_writer.start_record().unwrap(), |r| r.name = "north-old".into(), |_| {}).await.expect("the node writes its record under its new name");
        view.write().await.nodes.iter_mut().find(|n| n.node_id == recovered.node_id).unwrap().name = "north-old".into();
        let successor = node(north_at.clone(), "mesh1", "broker", false, false).with_type("broker").unwrap();
        mp.check_create(&north_at, "mesh1", "broker", &successor.node_id, None, None).await.expect("the released name is free");
        view.write().await.nodes.push(successor.clone());
        let successor_writer = NodePropertyWriter::new(storage.clone(), || "memory".into());
        let mut start = own_record(&successor);
        start.state = NodeStatus::Starting;
        successor_writer.bound(start, |_| {}).await.expect("the successor writes its own record at its start");
        let old_at: PathName = rdm_core::PathName::new("mesh1.broker", "north-old").unwrap();
        assert_eq!(storage.record_at(&old_at).await.unwrap().map(|r| (r.node_id, r.r#type)), Some((recovered.node_id.clone(), "broker".to_string())), "the renamed node's record at north-old, of its type");
        assert_eq!(storage.record_at(&north_at).await.unwrap().map(|r| (r.node_id, r.r#type)), Some((successor.node_id.clone(), "broker".to_string())), "the successor's record at north, of its type");
        assert_eq!(get(recovered.node_id.clone()).await.0, kept("north-old"), "the renamed node keeps its path and type under its new name");
        assert_eq!(get(successor.node_id.clone()).await.0, kept("north"), "the successor holds the free-text name at the same path, of the same type");
    });
}
