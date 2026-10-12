//! The node rows of a mesh, written by its primary node-admin (R13; ops-naming "Node data: meta,
//! config and exe"; lifecycles/node-create.md step 1; node-decommission.md step 3): the executor's
//! claim, move to `.old` and `Deleted` row, the rows of births no create claimed, and the
//! fabric-primary's record of a parked predecessor, which the drive of a Build feeds.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.admin.1`, its mesh primary
//! `mesh1.admin.2` (the executor of every `mesh1` node and the writer of its rows) and
//! `mesh1.rpc.1`, which carries config.

use rafka_node_admin_core::admin::Records;
use rafka_node_admin_core::accepted::FabricTopology;
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::MemoryBuildStateAdapter;
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::node_rows::NodeRows;
use rafka_node_admin_core::storage::{MemoryNodesStorage, NodeRow, NodesStorage};
use rafka_node_admin_core::topology::Topology;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A cell's own span capture: the product span pipeline (`rafka_mesh_telemetry::span_capture`: its
/// span filter and `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher
/// installed on every thread of the cell's own runtime.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rafka_mesh_telemetry::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture("node-rows", exporter.clone());
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

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn tier(t: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("tier".to_string(), t.to_string())])
}

fn view() -> Topology {
    let mut rpc = node("mesh1.rpc.1", false, false);
    rpc.config = tier("gold");
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![node("mesh1.admin.1", false, true), node("mesh1.admin.2", true, false), rpc],
    }
}

/// A view change as the product makes one: the projection installed through
/// `Records::install_projected`, the one door every view change of an admin goes through.
async fn install(records: &Records, view: &RwLock<Topology>, f: impl FnOnce(&mut Topology)) {
    let mut t = view.read().await.clone();
    f(&mut t);
    records.install_projected(view, || t.clone()).await;
}

/// The writer of `mesh1`'s rows, its primary `mesh1.admin.2`, over `storage`, its view the fixture
/// view, fed by the `Records` its view installs go through.
struct MeshPrimary {
    rows: Arc<NodeRows>,
    records: Records,
    view: Arc<RwLock<Topology>>,
}

fn mesh_primary(storage: &Arc<MemoryNodesStorage>) -> MeshPrimary {
    let t = view();
    let me: PathName = "mesh1.admin.2".parse().unwrap();
    let me_id = t.node(&me).expect("the mesh primary is in the view").node_id.clone();
    let view = Arc::new(RwLock::new(t));
    let rows = Arc::new(NodeRows::new(me, me_id, view.clone(), storage.clone()));
    let records = Records::default();
    records.set_rows(rows.clone());
    MeshPrimary { rows, records, view }
}

/// The record the fixture view holds at `name`.
async fn held(mp: &MeshPrimary, name: &str) -> Node {
    mp.view.read().await.nodes.iter().find(|n| n.name.to_string() == name).cloned().unwrap()
}

fn row_of(n: &Node) -> NodeRow {
    NodeRow { name: n.name.clone(), node_id: n.node_id.clone(), status: n.status, config: n.config.clone() }
}

/// CONTRACT: `node.delete` leaves the node's row at its `path.name` with status `Deleted`, its
/// node id and its config kept (ops-naming, the delete point; node-decommission.md step 3), written
/// by the mesh-primary that executes it, from the exact record it acted on. One `via-node-row` span
/// names the row written.
#[test]
fn a_deleted_node_leaves_a_deleted_row_with_its_record_and_config() {
    let cap = capture();
    let (row, victim) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let victim = held(&mp, "mesh1.rpc.1").await;
        storage.put_node_row(&row_of(&victim)).await.unwrap().unwrap();
        mp.rows.deleted(&victim, "retire-node:mesh1.rpc.1").await.expect("the mesh primary writes the Deleted row");
        (storage.node_row_at(&victim.name).await.unwrap(), victim.node_id)
    });
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: victim.clone(), status: NodeStatus::Deleted, config: tier("gold") }), "the row says Deleted and keeps the record and the config");
    let spans = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["node"].as_str(), a["subject"].as_str(), a["node_id"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some("mesh1.admin.2"), Some("mesh1.rpc.1"), Some(victim.as_str()), Some("1"), Some("row-deleted")), "{a:#?}");
}

/// CONTRACT: a node created at a `path.name` seeds its config from the previous row there (a
/// `Deleted` row): config belongs to the role, not the process (ops-naming "Node data"). The
/// mesh-primary's name claim writes the birth's row at the `path.name` before it is launched: the
/// new node id, `Pending`, the config the `Deleted` row kept. One `via-node-row` span names it.
#[test]
fn a_node_created_at_a_deleted_rows_name_carries_its_config() {
    let cap = capture();
    let (row, born, old) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let old = NodeId::mint();
        storage.put_node_row(&NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: old.clone(), status: NodeStatus::Deleted, config: tier("silver") }).await.unwrap().unwrap();
        let mp = mesh_primary(&storage);
        let born = NodeId::mint();
        mp.rows.claim(&"mesh1.rpc.2".parse().unwrap(), &born, "create-node:mesh1.rpc.2").await.expect("the name is free of live rows");
        (storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, old)
    });
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Pending, config: tier("silver") }), "the new birth holds the row and the Deleted row's config");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["moment"].as_str(), a["subject"].as_str(), a["node_id"].as_str(), a["seeded_from"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some("claim"), Some("mesh1.rpc.2"), Some(born.as_str()), Some(old.as_str()), Some("1"), Some("row-seeded")), "{a:#?}");
}

/// CONTRACT: a rewrite is lossless for every field its writer knows (fabric-node-lifecycle.md "A
/// rewrite is lossless"; node-recovery.md "Retained: ... config (seeded from the Deleted row)"). The
/// mesh-primary's claim seeds a birth's row with the config of the Deleted row at its `path.name`;
/// the executor's record of that birth (the create's own record) carries no config. When the birth
/// exits, its Deleted row carries the config the writer holds for it, never the record's empty
/// default, and the next birth at the name is seeded with it. The `via-node-row` delete span names
/// `config_from` `writer-record` and one key.
#[test]
fn a_deleted_row_carries_the_config_the_writer_holds_not_the_executors_empty_record() {
    let cap = capture();
    let (deleted, next_row, born) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let name: PathName = "mesh1.rpc.2".parse().unwrap();
        storage.put_node_row(&NodeRow { name: name.clone(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: tier("silver") }).await.unwrap().unwrap();
        let mp = mesh_primary(&storage);
        let born = NodeId::mint();
        mp.rows.claim(&name, &born, "create-node:mesh1.rpc.2").await.expect("the name is free of live rows");
        // The executor's record of the birth it created: no config on it.
        let mut record = Node::allocated(name.clone());
        record.node_id = born.clone();
        assert!(record.config.is_empty());
        mp.rows.deleted(&record, "retire-node:mesh1.rpc.2").await.expect("the Deleted row is written");
        let deleted = storage.node_row_at(&name).await.unwrap();
        let next = NodeId::mint();
        mp.rows.claim(&name, &next, "create-node:mesh1.rpc.2").await.expect("the Deleted row holds no name");
        (deleted, storage.node_row_at(&name).await.unwrap(), born)
    });
    assert_eq!(deleted.map(|r| (r.node_id, r.status, r.config)), Some((born.clone(), NodeStatus::Deleted, tier("silver"))), "the Deleted row keeps the config the writer holds for the birth");
    assert_eq!(next_row.map(|r| r.config), Some(tier("silver")), "the next birth at the name is seeded with it");
    let spans = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["config_from"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some("writer-record"), Some("1"), Some("row-deleted")), "{a:#?}");
    eprintln!("trace {}: rdm.node_admin.node.delete.via-node-row config_from=writer-record keys=1 outcome=row-deleted", spans[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT: two nodes never hold one name (R13 acceptance): a birth at a `path.name` whose row
/// another live node holds gets no row; the store adapter's insert-and-fail refuses it `NameHeld`
/// naming the holder, and the holder's row stays exactly as it was. One `via-node-row` span names
/// the refusal.
#[test]
fn a_birth_at_a_name_another_live_nodes_row_holds_is_refused_name_held_and_nothing_is_written() {
    let cap = capture();
    let (row, born, held) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let held = NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::ReadyForTraffic, config: tier("silver") };
        storage.put_node_row(&held).await.unwrap().unwrap();
        let birth = node("mesh1.rpc.2", false, false);
        let born = birth.node_id.clone();
        install(&mp.records, &mp.view, move |v| v.nodes.push(birth)).await;
        (storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, held)
    });
    assert_eq!(row, Some(held.clone()), "the holder's row stands; nothing of the birth is written");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let ours: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["node_id"] == born.as_str()).collect();
    assert_eq!(ours.len(), 1, "{spans:#?}");
    let a = &ours[0]["attributes"];
    assert_eq!(a["outcome"].as_str(), Some("name-held"), "{a:#?}");
    assert!(a["detail"].as_str().is_some_and(|d| d.contains(held.node_id.as_str())), "the refusal names the holder: {a:#?}");
    eprintln!("trace {}: rdm.node_admin.node.create.via-node-row subject=mesh1.rpc.2 outcome=name-held over another live node's row", ours[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT: a birth no create of the writer claimed (one made before the writer took its mesh's
/// seat) gets its row when the writer's view first holds it, from the record it holds (ops-naming
/// "Node data": "writes the whole row from that held record"), seeded from the `Deleted` row at its
/// `path.name`. One `via-node-row` span, `moment` `arrival`, at the view install, names it.
#[test]
fn a_birth_no_create_claimed_gets_its_row_seeded_when_the_writers_view_holds_it() {
    let cap = capture();
    let (row, born, old) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let old = NodeId::mint();
        storage.put_node_row(&NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: old.clone(), status: NodeStatus::Deleted, config: tier("silver") }).await.unwrap().unwrap();
        let mp = mesh_primary(&storage);
        let birth = node("mesh1.rpc.2", false, false);
        let born = birth.node_id.clone();
        install(&mp.records, &mp.view, move |v| v.nodes.push(birth)).await;
        (storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, old)
    });
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: born.clone(), status: NodeStatus::ReadyForTraffic, config: tier("silver") }), "the birth holds the row and the Deleted row's config");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let rpc2: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["subject"] == "mesh1.rpc.2").collect();
    assert_eq!(rpc2.len(), 1, "{spans:#?}");
    let a = &rpc2[0]["attributes"];
    assert_eq!((a["moment"].as_str(), a["node_id"].as_str(), a["seeded_from"].as_str(), a["outcome"].as_str()), (Some("arrival"), Some(born.as_str()), Some(old.as_str()), Some("row-seeded")), "{a:#?}");
}

/// CONTRACT: the store is read only to rebuild after a seat change (ops-naming "Node data"; R13:
/// "Rebuild after a seat change reads the mesh's own rows"), and that rebuild never overwrites a row
/// a node already holds: an admin that takes its mesh's primary seat writes the row of every member
/// of that mesh but itself, and the row of a member that already holds its name stays exactly as
/// written (a config the node has not applied yet is not wiped by the new holder's view). A member
/// with no row gets one. One `via-node-row` span per member: `row-held-by-node` for the held row,
/// `row-written` for the new one.
#[test]
fn a_seat_taken_rebuilds_the_rows_and_never_overwrites_a_row_its_node_holds() {
    let cap = capture();
    let (row, written) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let records = Records::default();
        let before = view();
        let topology = Arc::new(RwLock::new(before.clone()));
        records.set_rows(Arc::new(NodeRows::new(me.clone(), before.node(&me).expect("the admin is in the view").node_id.clone(), topology.clone(), storage.clone())));
        let rpc1 = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        let written = NodeRow { name: rpc1.name.clone(), node_id: rpc1.node_id.clone(), status: NodeStatus::Draining, config: tier("platinum") };
        storage.put_node_row(&written).await.unwrap().unwrap();
        install(&records, &topology, |t| {
            for n in t.nodes.iter_mut() {
                n.is_primary = n.name.to_string() == "mesh1.admin.1";
            }
        })
        .await;
        (storage.node_row_at(&rpc1.name).await.unwrap(), written)
    });
    assert_eq!(row, Some(written), "the row the node holds is untouched by the new holder's rebuild");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let outcome = |name: &str| spans.iter().filter(|s| s["attributes"]["subject"] == name).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
    assert_eq!(outcome("mesh1.rpc.1"), vec!["row-held-by-node"], "{spans:#?}");
    assert_eq!(outcome("mesh1.admin.2"), vec!["row-written"], "{spans:#?}");
}

type Rebuilt = (BTreeMap<String, Option<NodeRow>>, Option<NodeStatus>, NodeId, NodeId, NodeId, NodeId, String);

/// The rebuild cell's story: P's writes, then N's seat taken over one shared root.
async fn rebuild_over_one_root(trace_id: String) -> Rebuilt {
        let d = std::env::temp_dir().join(format!("node-rows-shared-root-{}", NodeId::mint()));
        let root = rafka_node_admin_core::store_root::StoreRoot::at(&d.join("store"));
        let open = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(rafka_node_admin_core::storage::FileNodesStorage::open(&data, &root, "mesh1", &IncarnationId::mint()).await.unwrap()) as Arc<dyn NodesStorage> }
        };
        let (p_store, n_store) = (open("mesh1.admin.2").await, open("mesh1.admin.1").await);
        let mut before = view();
        for n in before.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh1.admin.2";
        }
        let rpc1 = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        let (old_pred, deleted_pred) = (NodeId::mint(), NodeId::mint());
        // P's writes, as its seat made them.
        for row in [
            NodeRow { name: rpc1.name.clone(), node_id: rpc1.node_id.clone(), status: NodeStatus::Draining, config: tier("platinum") },
            NodeRow { name: "mesh1.rpc.2.old".parse().unwrap(), node_id: old_pred.clone(), status: NodeStatus::Deleted, config: tier("bronze") },
            NodeRow { name: "mesh1.rpc.3".parse().unwrap(), node_id: deleted_pred.clone(), status: NodeStatus::Deleted, config: tier("silver") },
        ] {
            p_store.put_node_row(&row).await.unwrap().unwrap();
        }
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let records = Records::default();
        let me_id = before.node(&me).unwrap().node_id.clone();
        let topology = Arc::new(RwLock::new(before));
        records.set_rows(Arc::new(NodeRows::new(me.clone(), me_id, topology.clone(), n_store.clone())));
        let (successor, fresh) = (node("mesh1.rpc.2", false, false), node("mesh1.rpc.3", false, false));
        let (succ, fresh_id) = (successor.node_id.clone(), fresh.node_id.clone());
        // N takes mesh1's seat (and the fabric seat P held with it): the mesh-primary writes its
        // mesh's rows (R13), so the rebuild is the mesh seat's.
        let after = {
            let mut t = topology.read().await.clone();
            for n in t.nodes.iter_mut() {
                n.is_primary = n.name == me;
                n.is_fabric_primary = n.name == me;
            }
            t.nodes.push(successor);
            t.nodes.push(fresh);
            t
        };
        records.install_projected(&topology, || after.clone()).await;
        let mut rows = BTreeMap::new();
        for name in ["mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.2.old", "mesh1.rpc.3"] {
            rows.insert(name.to_string(), n_store.node_row_at(&name.parse().unwrap()).await.unwrap());
        }
        let view_status = topology.read().await.node(&rpc1.name).map(|n| n.status);
        let _ = std::fs::remove_dir_all(&d);
        (rows, view_status, succ, fresh_id, old_pred, deleted_pred, trace_id)
}

/// CONTRACT (R14 item 7 and acceptance "a mesh seat change: the successor rebuilds and holds the
/// predecessor's rows (Node rows, `.old`, Deleted, config)"): the predecessor P and the successor N
/// are two admins with their own data dirs and their own store instances over one shared root. P
/// held the seat and wrote the rows: a live member's row (status Draining, config platinum), a
/// renamed predecessor's row moved to `mesh1.rpc.2.old` (Deleted, config bronze) and a deleted
/// node's row at `mesh1.rpc.3` (Deleted, config silver). N takes the seat over a view whose live
/// digests hold mesh1.rpc.1 ready, a successor at mesh1.rpc.2 and a new node at mesh1.rpc.3. N's
/// rebuild reads P's rows from the shared root: the live member's row stands untouched, the new
/// node at the Deleted row's name is seeded silver, the successor is seeded bronze from the `.old`
/// row, and N holds every one of P's rows. A row never overrides a live digest's status: N's view
/// holds mesh1.rpc.1 ready-for-traffic while its row says Draining.
#[test]
fn a_seat_taken_rebuilds_from_the_shared_root_and_holds_the_predecessors_rows() {
    let cap = crate::mesh_leave_resume::capture();
    let (rows, view_status, succ, fresh, old_pred, deleted_pred, trace_id) = cap.run(async {
        let root_span = tracing::info_span!("rdm.test.node.create.via-shared-root-rebuild");
        let trace_id = crate::mesh_leave_resume::trace_id(&root_span);
        tracing::Instrument::instrument(rebuild_over_one_root(trace_id), root_span).await
    });
    println!("trace_id={trace_id}");
    let row = |name: &str| rows[name].clone().unwrap_or_else(|| panic!("the successor holds no row at {name}: {rows:#?}"));
    assert_eq!((row("mesh1.rpc.1").status, row("mesh1.rpc.1").config), (NodeStatus::Draining, tier("platinum")), "the live member's row stands as P wrote it");
    assert_eq!((row("mesh1.rpc.2.old").node_id, row("mesh1.rpc.2.old").status), (old_pred.clone(), NodeStatus::Deleted), "N holds P's .old row");
    assert_eq!((row("mesh1.rpc.2").node_id, row("mesh1.rpc.2").config), (succ.clone(), tier("bronze")), "the successor is seeded from the .old row");
    assert_eq!((row("mesh1.rpc.3").node_id, row("mesh1.rpc.3").config), (fresh.clone(), tier("silver")), "the node created at the Deleted row's name is seeded from it, its row overwritten at that key");
    assert_eq!(view_status, Some(NodeStatus::ReadyForTraffic), "a row never overrides a live digest's status");
    let spans: Vec<Value> = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.node.create.via-node-row").collect();
    assert!(spans.iter().all(|s| s["trace_id"] == trace_id.as_str()), "every rebuild claim is the cell's own story: {spans:#?}");
    let outcome = |name: &str| spans.iter().filter(|s| s["attributes"]["subject"] == name).map(|s| (s["attributes"]["outcome"].as_str().unwrap_or("").to_string(), s["attributes"]["seeded_from"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>();
    assert_eq!(outcome("mesh1.rpc.1"), vec![("row-held-by-node".to_string(), String::new())], "{spans:#?}");
    assert_eq!(outcome("mesh1.rpc.2"), vec![("row-seeded".to_string(), old_pred.to_string())], "{spans:#?}");
    assert_eq!(outcome("mesh1.rpc.3"), vec![("row-seeded".to_string(), deleted_pred.to_string())], "{spans:#?}");
}

/// CONTRACT: a node lets go only of its own row: a release of a `path.name` by another node leaves
/// the row there exactly as it is, and the holder's release frees the key, in every nodes store.
#[test]
fn a_node_lets_go_only_of_its_own_row() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("node-rows-claim-{}", NodeId::mint()));
        let stores: Vec<(&str, Arc<dyn NodesStorage>)> = vec![("memory", Arc::new(MemoryNodesStorage::default())), ("file", Arc::new(rafka_node_admin_core::storage::FileNodesStorage::open(&dir, &rafka_node_admin_core::store_root::StoreRoot::at(&dir.join("store")), "mesh1", &rafka_node_admin_core::model::IncarnationId("test".into())).await.unwrap()))];
        for (kind, store) in stores {
            let id = NodeId::mint();
            let held = NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: id.clone(), status: NodeStatus::Draining, config: tier("platinum") };
            store.put_node_row(&held).await.unwrap().unwrap();
            assert!(!store.release_node_row(&held.name, &NodeId::mint()).await.unwrap(), "{kind}: another node lets go of nothing here");
            assert_eq!(store.node_row_at(&held.name).await.unwrap(), Some(held.clone()), "{kind}: a release by another node leaves the row");
            assert!(store.release_node_row(&held.name, &id).await.unwrap(), "{kind}: the holder lets go of its key");
            assert_eq!(store.node_row_at(&held.name).await.unwrap(), None, "{kind}: the holder's release frees the key");
        }
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// CONTRACT: the writer writes a birth's row when its view holds the birth for the first time in its
/// tenure (ops-naming "Node data": the row is written once, from the held record), never again on a
/// later view install. A birth that left the view (a departure the projection dropped) and is put
/// back by a later backbone copy of a peer mesh's view is the same birth, not a new arrival: one
/// `via-node-row` span for it, the row written once.
#[test]
fn a_birth_that_leaves_the_view_and_returns_is_written_once() {
    let cap = capture();
    let (row, born) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let birth = node("mesh1.rpc.2", false, false);
        let born = birth.node_id.clone();
        let again = birth.clone();
        install(&mp.records, &mp.view, move |v| v.nodes.push(birth)).await;
        install(&mp.records, &mp.view, |v| v.nodes.retain(|n| n.name.to_string() != "mesh1.rpc.2")).await;
        install(&mp.records, &mp.view, move |v| v.nodes.push(again)).await;
        (storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born)
    });
    assert_eq!(row.map(|r| r.node_id), Some(born.clone()), "the birth holds its row");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let claims: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["node_id"] == born.as_str()).collect();
    assert_eq!(claims.len(), 1, "one row write for one birth in one tenure: {claims:#?}");
    assert_eq!(claims[0]["attributes"]["outcome"], "row-written", "{claims:#?}");
}

// The writer of a mesh's rows (R13): the mesh's primary node-admin, the fabric-primary only as the
// acting primary of a mesh with no primary; the name claim of a `node.create` before anything is
// launched (lifecycles/node-create.md step 1).

mod writer {
    use super::*;
    use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
    use rafka_node_admin_core::deployment::pipeline::{CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts};
    use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
    use rafka_node_admin_core::join::Joins;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// The rows writer of the admin `me` in view `t`, over `storage`, fed by `records`.
    fn writer_of(me: &str, t: &Topology, storage: &Arc<MemoryNodesStorage>) -> (Arc<Records>, Arc<RwLock<Topology>>) {
        let me: PathName = me.parse().unwrap();
        let me_id = t.node(&me).expect("the admin is in the view").node_id.clone();
        let view = Arc::new(RwLock::new(t.clone()));
        let records = Arc::new(Records::default());
        records.set_rows(Arc::new(NodeRows::new(me, me_id, view.clone(), storage.clone())));
        (records, view)
    }

    /// Three meshes: `mesh1` has no node-admin (only `mesh1.rpc.1`), `mesh2.admin.1` is the
    /// fabric-primary and `mesh2`'s primary, `mesh3.admin.1` is `mesh3`'s primary.
    fn three_meshes() -> Topology {
        let mut t = view();
        t.meshes = ["mesh1", "mesh2", "mesh3"].iter().map(|m| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::ReadyForTraffic }).collect();
        t.nodes = vec![node("mesh1.rpc.1", false, false), node("mesh2.admin.1", true, true), node("mesh3.admin.1", true, false)];
        t
    }

    /// CONTRACT: no fabric-primary writes a row of a mesh that has a primary (R13 acceptance): a
    /// birth of `mesh1`, whose primary is `mesh1.admin.2`, entering the fabric-primary
    /// `mesh1.admin.1`'s view gets no row from it, and gets its row from `mesh1.admin.2` when its
    /// view holds it. One `via-node-row` span, the mesh primary's.
    #[test]
    fn a_fabric_primary_writes_no_row_of_a_mesh_that_has_a_primary_and_the_mesh_primary_does() {
        let cap = capture();
        let (fp_row, mp_row, born) = cap.run(async {
            let (fp_store, mp_store) = (Arc::new(MemoryNodesStorage::default()), Arc::new(MemoryNodesStorage::default()));
            let t = view();
            let (fp, fp_view) = writer_of("mesh1.admin.1", &t, &fp_store);
            let (mp, mp_view) = writer_of("mesh1.admin.2", &t, &mp_store);
            let birth = node("mesh1.rpc.2", false, false);
            let born = birth.node_id.clone();
            let again = birth.clone();
            install(&fp, &fp_view, move |v| v.nodes.push(birth)).await;
            install(&mp, &mp_view, move |v| v.nodes.push(again)).await;
            let name: PathName = "mesh1.rpc.2".parse().unwrap();
            (fp_store.node_row_at(&name).await.unwrap(), mp_store.node_row_at(&name).await.unwrap(), born)
        });
        assert_eq!(fp_row, None, "the fabric-primary writes no row of a mesh that has a primary");
        assert_eq!(mp_row.map(|r| r.node_id), Some(born.clone()), "the mesh's primary writes its member's row");
        let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let ours: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["node_id"] == born.as_str()).collect();
        assert_eq!(ours.len(), 1, "{spans:#?}");
        assert_eq!((ours[0]["attributes"]["node"].as_str(), ours[0]["attributes"]["outcome"].as_str()), (Some("mesh1.admin.2"), Some("row-written")), "{ours:#?}");
        eprintln!("trace {}: rdm.node_admin.node.create.via-node-row node=mesh1.admin.2 subject=mesh1.rpc.2 outcome=row-written; the fabric-primary mesh1.admin.1 wrote none", ours[0]["trace_id"].as_str().unwrap_or(""));
    }

    /// CONTRACT: no node writes its own row (R12): an admin that takes its mesh's seat (and the
    /// fabric seat) rebuilds the rows of its mesh's members from its view, every member but itself.
    #[test]
    fn an_admin_that_takes_its_meshs_seat_writes_every_members_row_but_its_own() {
        let cap = capture();
        let (own, others) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let mut before = view();
            for n in before.nodes.iter_mut() {
                n.is_fabric_primary = n.name.to_string() == "mesh1.admin.2";
            }
            let (records, topology) = writer_of("mesh1.admin.1", &before, &storage);
            install(&records, &topology, |v| {
                for n in v.nodes.iter_mut() {
                    let me = n.name.to_string() == "mesh1.admin.1";
                    n.is_fabric_primary = me;
                    n.is_primary = me;
                }
            })
            .await;
            let own = storage.node_row_at(&"mesh1.admin.1".parse().unwrap()).await.unwrap();
            let others = [storage.node_row_at(&"mesh1.admin.2".parse().unwrap()).await.unwrap(), storage.node_row_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap()];
            (own, others)
        });
        assert_eq!(own, None, "the admin writes no row of its own");
        assert!(others.iter().all(Option::is_some), "every other member's row is written: {others:?}");
        let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        assert!(spans.iter().all(|s| s["attributes"]["subject"] != "mesh1.admin.1"), "{spans:#?}");
    }

    /// Launches nothing: each spawn is counted and refused, so a create that reaches
    /// `DeployRuntime` fails there.
    #[derive(Default)]
    struct Counted {
        spawned: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for Counted {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            "node-rows".into()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            self.spawned.fetch_add(1, Ordering::SeqCst);
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this provider launches nothing".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
            DeploymentStatus::Unknown
        }
        async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch) -> Result<Option<DeploymentHandle>, DeployError> {
            Ok(None)
        }
    }

    struct Unheard;
    #[async_trait::async_trait]
    impl NodeObserver for Unheard {
        async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
            None
        }
        async fn ready(&self, _: &Node) -> Result<(), String> {
            Err("nothing is launched here".into())
        }
    }

    /// `create-node:<name>` run by the executor whose rows writer `records` feeds, over `provider`:
    /// its error, if it failed.
    async fn create(records: &Records, provider: &Counted, name: &str) -> Result<(), String> {
        let data_root = std::env::temp_dir().join(format!("node-rows-create-{}", NodeId::mint()));
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/node-rows".into(),
            seeds: vec![],
            launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.2".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.join("store"),
            mesh_issuer: None,
        };
        let (joins, builds) = (Joins::default(), MemoryBuildStateAdapter::new());
        let pipeline = DeploymentPipeline { provider, joins: &joins, observer: &Unheard, sink: records, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let req = CreateRequest { build_id: BuildId("build-node-rows".into()), attempt: 1, node: name.parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() };
        let r = pipeline.create(&req).await.map(|_| ()).map_err(|e| e.to_string());
        let _ = std::fs::remove_dir_all(&data_root);
        r
    }

    /// CONTRACT: a `node.create` whose name another live node's row holds launches nothing
    /// (lifecycles/node-create.md step 1): the mesh-primary claims the name at the start of
    /// `PrepareStorage`, before `DeployRuntime`, the store adapter refuses it `NameHeld` naming the
    /// holder, the create fails at `PrepareStorage` saying so, the provider is never asked to spawn,
    /// and the holder's row stands. One `via-node-row` span, `moment` `claim`, outcome `name-held`.
    #[test]
    fn a_create_whose_name_another_live_row_holds_launches_nothing() {
        let cap = capture();
        let (err, spawned, row, held) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let held = NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::ReadyForTraffic, config: tier("silver") };
            storage.put_node_row(&held).await.unwrap().unwrap();
            let (records, _view) = writer_of("mesh1.admin.2", &view(), &storage);
            let provider = Counted::default();
            let err = create(&records, &provider, "mesh1.rpc.2").await;
            (err, provider.spawned.load(Ordering::SeqCst), storage.node_row_at(&held.name).await.unwrap(), held)
        });
        let err = err.expect_err("the create is refused");
        assert!(err.contains("PrepareStorage") && err.contains(held.node_id.as_str()), "the create fails at PrepareStorage naming the holder: {err}");
        assert_eq!(spawned, 0, "nothing is launched");
        assert_eq!(row, Some(held.clone()), "the holder's row stands");
        let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        assert_eq!(spans.len(), 1, "{spans:#?}");
        let a = &spans[0]["attributes"];
        assert_eq!((a["moment"].as_str(), a["node"].as_str(), a["outcome"].as_str()), (Some("claim"), Some("mesh1.admin.2"), Some("name-held")), "{a:#?}");
        eprintln!("trace {}: rdm.node_admin.node.create.via-node-row moment=claim outcome=name-held; spawn never called", spans[0]["trace_id"].as_str().unwrap_or(""));
    }

    /// CONTRACT: a `node.create` that fails after its claim withdraws it (the holder's own key, a
    /// blind release), so the next birth at the name is not refused by it: the mesh-primary claims
    /// the free name (row written), the spawn fails at `DeployRuntime`, and no row is left at the
    /// name. A `via-node-row` span (`claim`, `row-written`) then a `via-claim-withdrawn` span
    /// (`withdrawn`).
    #[test]
    fn a_create_that_fails_after_its_claim_withdraws_it() {
        let cap = capture();
        let (err, spawned, row) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let (records, _view) = writer_of("mesh1.admin.2", &view(), &storage);
            let provider = Counted::default();
            let err = create(&records, &provider, "mesh1.rpc.2").await;
            (err, provider.spawned.load(Ordering::SeqCst), storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap())
        });
        assert!(err.expect_err("the spawn fails").contains("DeployRuntime"), "the create fails at DeployRuntime");
        assert_eq!(spawned, 1, "the claimed birth is launched");
        assert_eq!(row, None, "the failed create's claim is withdrawn");
        let claims = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        assert_eq!(claims.iter().map(|s| (s["attributes"]["moment"].as_str().unwrap_or(""), s["attributes"]["outcome"].as_str().unwrap_or(""))).collect::<Vec<_>>(), vec![("claim", "row-written")], "{claims:#?}");
        let withdrawn = cap.spans_named("rdm.node_admin.node.remove.via-claim-withdrawn");
        assert_eq!(withdrawn.len(), 1, "{withdrawn:#?}");
        assert_eq!(withdrawn[0]["attributes"]["outcome"].as_str(), Some("withdrawn"), "{withdrawn:#?}");
        eprintln!("trace {}: via-node-row moment=claim outcome=row-written; trace {}: rdm.node_admin.node.remove.via-claim-withdrawn outcome=withdrawn", claims[0]["trace_id"].as_str().unwrap_or(""), withdrawn[0]["trace_id"].as_str().unwrap_or(""));
    }

    /// The Build state of an executor lost while one step of attempt 1 ran: that step's work ran and
    /// its receipt is never written (the run stops there, as a killed executor's does, and the step's
    /// failure path never runs past it). Every other receipt reaches `inner`.
    struct LostAt<'a> {
        inner: &'a MemoryBuildStateAdapter,
        step: &'static str,
    }

    #[async_trait::async_trait]
    impl rafka_node_admin_core::build_state::BuildStateAdapter for LostAt<'_> {
        async fn publish_accepted(&self, a: &rafka_node_admin_core::build_state::BuildAccepted) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &rafka_node_admin_core::build_state::AttemptOpened) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.open_attempt(o).await
        }
        async fn read_build(&self, id: &BuildId) -> Result<rafka_node_admin_core::build_state::BuildProjection, rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.read_build(id).await
        }
        async fn list_active(&self) -> Result<Vec<rafka_node_admin_core::build_state::BuildProjection>, rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.list_active().await
        }
        async fn claim_attempt(&self, c: &rafka_node_admin_core::build_state::BuildAttemptClaim) -> Result<rafka_node_admin_core::build_state::ClaimOutcome, rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.claim_attempt(c).await
        }
        async fn adopt_claim(&self, c: &rafka_node_admin_core::build_state::BuildAttemptClaim) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.adopt_claim(c).await
        }
        async fn append_step_receipt(&self, r: &rafka_node_admin_core::build_state::BuildStepReceipt) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
            if r.step == self.step && r.attempt == 1 {
                return Err(rafka_node_admin_core::build_state::BuildStateError::Io(format!("the executor of {} attempt 1 was lost before its {} receipt was written", r.build_id, r.step)));
            }
            self.inner.append_step_receipt(r).await
        }
        async fn append_attempt_receipt(&self, r: &rafka_node_admin_core::build_state::BuildAttemptReceipt) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<rafka_node_admin_core::build_state::BuildFact>, rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.facts().await
        }
        async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.recovery_of(path, node_id, incarnation).await
        }
        async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.mesh_deleted_of(node_id, incarnation).await
        }
        async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<rafka_node_admin_core::build_state::OwedExit>, rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.exits_owed(build_id, mesh).await
        }
        async fn forget(&self, id: &BuildId) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
            self.inner.forget(id).await
        }
    }

    /// Attempt `attempt` of `create-node:<name>` in the Build `build`, its receipts in `builds`, run
    /// by the executor whose rows writer `records` feeds, over `provider`: its error.
    async fn create_attempt(records: &Records, provider: &Counted, builds: &dyn rafka_node_admin_core::build_state::BuildStateAdapter, build: &BuildId, name: &str, attempt: u32) -> Result<(), String> {
        let data_root = std::env::temp_dir().join(format!("node-rows-create-{}", NodeId::mint()));
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/node-rows".into(),
            seeds: vec![],
            launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.2".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.join("store"),
            mesh_issuer: None,
        };
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider, joins: &joins, observer: &Unheard, sink: records, lifecycle: &NoLifecycleEvents, builds, template: &template, timeouts: Timeouts::default() };
        let req = CreateRequest { build_id: build.clone(), attempt, node: name.parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() };
        let r = pipeline.create(&req).await.map(|_| ()).map_err(|e| e.to_string());
        let _ = std::fs::remove_dir_all(&data_root);
        r
    }

    /// CONTRACT (lifecycles/node-create.md step 1: the row writer claims the name before anything is
    /// launched; mesh_recover's lost mesh): a create whose executor is lost mid-create leaves no claim
    /// that no receipt names. The identity's receipt is written before the claim that names its node
    /// id, so a rerun of the same operation holds every claim an earlier attempt made:
    /// - lost after the identity was minted and before its `AllocateIdentity` receipt was written
    ///   (`mesh1.rpc.5`): attempt 2 claims the name for its own new node id, is not refused
    ///   `name-held`, and reaches `DeployRuntime`;
    /// - lost after the claim, with the `AllocateIdentity` receipt naming node X written and the
    ///   store holding X's `Pending` row (`mesh1.rpc.6`): attempt 2 reuses X, its claim finds the row
    ///   X holds (`row-held-by-node`), and it reaches `DeployRuntime`.
    /// Must NOT happen: attempt 2 refused `name-held` by a node no receipt of the Build names (the
    /// orphan claim mesh_recover's attempt 3 met: "mesh1.rpc.5 is held by node 0krc0z5weqyk").
    #[test]
    fn a_create_whose_executor_is_lost_mid_create_leaves_no_claim_a_rerun_cannot_hold() {
        let cap = capture();
        let (lost_before_receipt, rerun_after_loss, x, rerun_after_claim) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let (records, _view) = writer_of("mesh1.admin.2", &view(), &storage);
            let provider = Counted::default();
            let builds = MemoryBuildStateAdapter::new();
            let (build, other) = (BuildId("build-node-rows-5".into()), BuildId("build-node-rows-6".into()));
            for id in [&build, &other] {
                let accepted = rafka_node_admin_core::build_state::BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 };
                rafka_node_admin_core::build_state::BuildStateAdapter::publish_accepted(&builds, &accepted).await.unwrap();
            }
            // Lost before the AllocateIdentity receipt of attempt 1.
            let lost = LostAt { inner: &builds, step: "AllocateIdentity" };
            let lost_before_receipt = create_attempt(&records, &provider, &lost, &build, "mesh1.rpc.5", 1).await;
            let rerun_after_loss = create_attempt(&records, &provider, &builds, &build, "mesh1.rpc.5", 2).await;
            // Lost after the claim: the identity receipt names X, the store holds X's claim row.
            let x = NodeId::mint();
            let identity = serde_json::json!({ "node_id": x, "incarnation": IncarnationId::mint(), "supersedes": null, "deployment_id": rafka_node_admin_core::model::DeploymentId::mint() });
            rafka_node_admin_core::build_state::BuildStateAdapter::append_step_receipt(
                &builds,
                &rafka_node_admin_core::build_state::BuildStepReceipt { build_id: other.clone(), attempt: 1, operation: "create-node:mesh1.rpc.6".into(), step: "AllocateIdentity".into(), outcome: rafka_node_admin_core::build_state::StepOutcome::Complete, output: Some(identity), executor: None },
            )
            .await
            .unwrap();
            storage.put_node_row(&NodeRow { name: "mesh1.rpc.6".parse().unwrap(), node_id: x.clone(), status: NodeStatus::Pending, config: BTreeMap::new() }).await.unwrap().unwrap();
            let rerun_after_claim = create_attempt(&records, &provider, &builds, &other, "mesh1.rpc.6", 2).await;
            (lost_before_receipt, rerun_after_loss, x, rerun_after_claim)
        });
        assert!(lost_before_receipt.expect_err("attempt 1's executor is lost").contains("was not written"), "attempt 1 stops at its unwritten receipt");
        let rerun = rerun_after_loss.expect_err("attempt 2 fails where this provider launches nothing");
        assert!(rerun.contains("DeployRuntime") && !rerun.contains("name-held"), "attempt 2 is not refused by attempt 1's claim and reaches DeployRuntime: {rerun}");
        let claims = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let of = |subject: &str| claims.iter().filter(|s| s["attributes"]["subject"] == subject && s["attributes"]["moment"] == "claim").map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>();
        let rpc5 = of("mesh1.rpc.5");
        assert!(rpc5.iter().all(|(_, o)| o != "name-held"), "no claim of mesh1.rpc.5 is refused name-held: {rpc5:?}");
        assert_eq!(rpc5.iter().filter(|(_, o)| o == "row-written").count(), 1, "one claim of mesh1.rpc.5 is written, attempt 2's: {rpc5:?}");
        let rerun = rerun_after_claim.expect_err("attempt 2 fails where this provider launches nothing");
        assert!(rerun.contains("DeployRuntime"), "attempt 2 after a loss past the claim reaches DeployRuntime: {rerun}");
        assert_eq!(of("mesh1.rpc.6"), vec![(x.to_string(), "row-held-by-node".to_string())], "attempt 2 reuses X and its claim holds X's row");
        let trace = claims.iter().find(|s| s["attributes"]["subject"] == "mesh1.rpc.5").and_then(|s| s["trace_id"].as_str()).unwrap_or("");
        eprintln!("trace {trace}: via-node-row claims of mesh1.rpc.5 {rpc5:?}; mesh1.rpc.6 {:?}", of("mesh1.rpc.6"));
    }

    /// CONTRACT: another mesh's primary never writes a row of a mesh with no primary; the
    /// fabric-primary does, as that mesh's acting primary (R13). `mesh1` has no node-admin: a create
    /// of `mesh1.rpc.2` executed by `mesh3`'s primary is refused at its claim `not-mesh-primary`,
    /// naming the fabric-primary as the writer, and nothing is launched; executed by the
    /// fabric-primary `mesh2.admin.1`, the claim is written.
    #[test]
    fn another_meshs_primary_claims_no_name_in_a_mesh_with_no_primary_and_the_fabric_primary_does() {
        let cap = capture();
        let (refused, spawned_refused, fp_err, spawned_fp) = cap.run(async {
            let t = three_meshes();
            let (other, _v1) = writer_of("mesh3.admin.1", &t, &Arc::new(MemoryNodesStorage::default()));
            let (fp, _v2) = writer_of("mesh2.admin.1", &t, &Arc::new(MemoryNodesStorage::default()));
            let (p1, p2) = (Counted::default(), Counted::default());
            let refused = create(&other, &p1, "mesh1.rpc.2").await;
            let fp_err = create(&fp, &p2, "mesh1.rpc.2").await;
            (refused, p1.spawned.load(Ordering::SeqCst), fp_err, p2.spawned.load(Ordering::SeqCst))
        });
        let refused = refused.expect_err("another mesh's primary is refused");
        assert!(refused.contains("PrepareStorage") && refused.contains("mesh2.admin.1"), "refused at the claim, naming the fabric-primary as the writer: {refused}");
        assert_eq!(spawned_refused, 0, "nothing is launched");
        assert!(fp_err.expect_err("the spawn fails").contains("DeployRuntime"), "the fabric-primary's claim passes and its create reaches DeployRuntime");
        assert_eq!(spawned_fp, 1);
        let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let by = |n: &str| spans.iter().filter(|s| s["attributes"]["node"] == n).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
        assert_eq!(by("mesh3.admin.1"), vec!["not-mesh-primary"], "{spans:#?}");
        assert_eq!(by("mesh2.admin.1"), vec!["row-written"], "{spans:#?}");
        eprintln!("trace {}: via-node-row node=mesh3.admin.1 outcome=not-mesh-primary; node=mesh2.admin.1 outcome=row-written", spans[0]["trace_id"].as_str().unwrap_or(""));
    }
}

/// The recovery of `mesh1.rpc.1` (node-recovery.md steps 3-4) as its executor, `mesh1`'s primary
/// `mesh1.admin.2`, writes it (R13): its node.delete of the exited birth (`retire-node:<path>`)
/// writes the row `Deleted` inside its `NodeDeleted` step, and its node.create at the same
/// `path.name` (`create-node:<path>`) claims it, `before` or after that `Deleted` row. Returns the
/// row at `mesh1.rpc.1`, the exited node and the new one.
async fn recovered(new_claimed_first: bool) -> (Option<NodeRow>, NodeId, NodeId) {
    let (delete, create) = ("retire-node:mesh1.rpc.1", "create-node:mesh1.rpc.1");
    let storage = Arc::new(MemoryNodesStorage::default());
    let mp = mesh_primary(&storage);
    let exited = held(&mp, "mesh1.rpc.1").await;
    // The exited birth's row, as its create claimed it.
    storage.put_node_row(&NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: exited.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: tier("gold") }).await.unwrap().unwrap();
    let born = NodeId::mint();
    if new_claimed_first {
        mp.rows.claim(&exited.name, &born, create).await.expect_err("the exited node's live row holds the name");
    }
    mp.rows.deleted(&exited, delete).await.expect("the executor writes the Deleted row inside NodeDeleted");
    mp.rows.claim(&exited.name, &born, create).await.expect("the Deleted row holds no name");
    (storage.node_row_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap(), exited.node_id, born)
}

/// CONTRACT (node-recovery.md step 3: "the node's mesh-primary writes the row with status Deleted";
/// step 4: "Its config is seeded from the Deleted row at that path.name"): a recovery's
/// `NodeDeleted` leaves the exited node's row `Deleted`, so the new node created at the same
/// `path.name` claims it, seeded from it: the new node id, its status, the exited node's config.
/// One `delete.via-node-row` span (`row-deleted`) and one `create.via-node-row` span
/// (`row-seeded`, seeded from the exited node) name the two writes. The writer is the recovery's
/// executor, the mesh's primary (R13), and its claim writes the new node `Pending`.
#[test]
fn a_recovered_nodes_row_is_deleted_and_the_new_node_at_its_path_name_claims_it_seeded() {
    let cap = capture();
    let (row, exited, born) = cap.run(recovered(false));
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Pending, config: tier("gold") }), "the new node holds the row, seeded from the exited node's");
    let deleted = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(deleted.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(exited.as_str()), Some("row-deleted"))], "{deleted:#?}");
    let created = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    assert_eq!(created.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["seeded_from"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(born.as_str()), Some(exited.as_str()), Some("row-seeded"))], "{created:#?}");
}

/// CONTRACT: the new node of a recovery claimed before the exited node's `Deleted` row is written
/// finds the exited node's live row at its `path.name` (its claim is refused, `name-held`); once
/// the `NodeDeleted` step frees the name the claim takes it, through the one claim every birth
/// takes, seeded from the `Deleted` row.
#[test]
fn a_recovered_node_claimed_before_the_exited_ones_deleted_row_is_refused_name_held_then_seeded() {
    let cap = capture();
    let (row, exited, born) = cap.run(recovered(true));
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Pending, config: tier("gold") }), "the new node holds the row once the name is free");
    let created = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    assert_eq!(created.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(born.as_str()), Some("name-held")), (Some(born.as_str()), Some("row-seeded"))], "{created:#?}");
    assert_eq!(created[1]["attributes"]["seeded_from"].as_str(), Some(exited.as_str()));
}
