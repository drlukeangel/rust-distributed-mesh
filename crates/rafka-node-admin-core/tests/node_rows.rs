//! The node rows of a mesh, written by its primary node-admin (R13; ops-naming "Node data: meta,
//! config and exe"; lifecycles/node-create.md step 1; node-decommission.md step 3): the executor's
//! claim, move to `.old` and `Deleted` row, the rows of births no create claimed, and the
//! fabric-primary's record of a parked predecessor, which the drive of a Build feeds.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.admin.1`, its mesh primary
//! `mesh1.admin.2` (the executor of every `mesh1` node and the writer of its rows) and
//! `mesh1.rpc.1`, which carries config.

use rafka_node_admin_core::admin::Records;
use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_claim::{AttemptContexts, ClaimDoor};
use rafka_node_admin_core::build_drive::{Dispatched, Dispatcher, Drive, DriveEnd, DriveEnv, InnerItem, InnerStream, OpenGate};
use rafka_node_admin_core::build_state::MemoryBuildStateAdapter;
use rafka_node_admin_core::http::ControlPlane;
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::node_rows::NodeRows;
use rafka_node_admin_core::storage::{MemoryNodesStorage, NodeRow, NodesStorage};
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc_contract::build::BuildReply;
use rafka_node_rpc_contract::context::CallContext;
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
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

/// What the executor fixture does on its dispatch, in order: change the fabric-primary's view (a
/// departure heard, a birth heard) or stream a frame.
enum Act {
    Frame(&'static str, &'static str),
    View(Box<dyn FnOnce(&mut Topology) + Send>),
}

struct Script {
    acts: Mutex<Option<Vec<Act>>>,
    view: Arc<RwLock<Topology>>,
    records: Arc<Records>,
}

struct Played {
    acts: VecDeque<Act>,
    view: Arc<RwLock<Topology>>,
    records: Arc<Records>,
    build_id: String,
    attempt: u32,
    done: bool,
}

#[async_trait::async_trait]
impl InnerStream for Played {
    async fn next(&mut self) -> InnerItem {
        loop {
            match self.acts.pop_front() {
                Some(Act::View(f)) => install(&self.records, &self.view, f).await,
                Some(Act::Frame(operation, step)) => return InnerItem::Frame(BuildReply::Step { build_id: self.build_id.clone(), attempt: self.attempt, operation: operation.into(), step: step.into() }),
                None if !self.done => {
                    self.done = true;
                    return InnerItem::Frame(BuildReply::Complete { build_id: self.build_id.clone(), attempt: self.attempt });
                }
                None => return InnerItem::Ended,
            }
        }
    }
}

#[async_trait::async_trait]
impl Dispatcher for Script {
    async fn dispatch(&self, _executor: &PathName, build_id: &BuildId, attempt: u32, _context: CallContext, _intent: Vec<Vec<u8>>, _plan: rafka_node_admin_core::executor::RunPlan) -> Dispatched {
        let acts = self.acts.lock().unwrap().take().expect("one dispatch");
        Dispatched::Stream(Box::new(Played { acts: acts.into(), view: self.view.clone(), records: self.records.clone(), build_id: build_id.0.clone(), attempt, done: false }))
    }
}

struct Rig {
    env: Arc<DriveEnv>,
    storage: Arc<MemoryNodesStorage>,
    build: BuildId,
}

/// A view change as the product makes one: the projection installed through
/// `Records::install_projected`, the one door every view change of an admin goes through.
async fn install(records: &Records, view: &RwLock<Topology>, f: impl FnOnce(&mut Topology)) {
    let mut t = view.read().await.clone();
    f(&mut t);
    records.install_projected(view, || t.clone()).await;
}

async fn rig(change: TopologyChange, acts: Vec<Act>, storage: Arc<MemoryNodesStorage>) -> Rig {
    let t = view();
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let topology = Arc::new(RwLock::new(t.clone()));
    let mut cp = ControlPlane::new(builds.clone(), accepted.clone(), "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time());
    cp.topology = topology.clone();
    let opened = cp.submit("test", change).await.unwrap();
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let me_id = topology.read().await.node(&me).expect("the fabric-primary is in the view").node_id.clone();
    let rows = Arc::new(NodeRows::new(me.clone(), me_id.clone(), topology.clone(), storage.clone()));
    let records = Arc::new(Records::default());
    records.set_rows(rows.clone());
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: topology.clone(),
        accepted,
        builds: builds.clone(),
        door: Arc::new(ClaimDoor { me, node_id: me_id, topology: topology.clone(), builds: builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()) }),
        dispatcher: Arc::new(Script { acts: Mutex::new(Some(acts)), view: topology, records: records.clone() }),
        gate: Arc::new(OpenGate),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        rows,
    });
    Rig { env, storage, build: opened.build_id }
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
    NodeRow { name: n.name.clone(), node_id: n.node_id.clone(), status: n.status, config: n.config.clone(), tags: Default::default(), allocated: None, otel_debug: false }
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
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: victim.clone(), status: NodeStatus::Deleted, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false }), "the row says Deleted and keeps the record and the config");
    let spans = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["node"].as_str(), a["subject"].as_str(), a["node_id"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some("mesh1.admin.2"), Some("mesh1.rpc.1"), Some(victim.as_str()), Some("1"), Some("row-deleted")), "{a:#?}");
}

/// CONTRACT (R-TG1: tags are labels in the node's meta; node-decommission.md step 3: the `Deleted`
/// row keeps the record and its config; the node store's tags index names only nodes that carry a
/// label): a node whose row carries tags {rabbit, red} is deleted by its mesh's writer, which holds no
/// record of the row yet: the `Deleted` row keeps the tags with the record and the config, and the
/// tags index names the node under neither label. A node then created at that `path.name` is seeded
/// the config and carries no tag. What must NOT happen: a deleted node still selected by a label, the
/// tags dropped from the `Deleted` row, or a new node inheriting the deleted node's labels.
#[test]
fn a_deleted_nodes_row_keeps_its_tags_and_the_tags_index_names_it_under_none() {
    let cap = capture();
    let (row, born_row, victim, rabbit, red) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let victim = held(&mp, "mesh1.rpc.1").await;
        let tags: std::collections::BTreeSet<String> = ["rabbit".to_string(), "red".to_string()].into();
        storage.put_node_row(&NodeRow { tags: tags.clone(), ..row_of(&victim) }).await.unwrap().unwrap();
        assert_eq!(storage.tagged("rabbit").await.unwrap(), [victim.node_id.clone()].into(), "the store keeps the tags index with the row it writes");
        mp.rows.deleted(&victim, "retire-node:mesh1.rpc.1").await.expect("the mesh primary writes the Deleted row");
        let row = storage.node_row_at(&victim.name).await.unwrap();
        let (rabbit, red) = (storage.tagged("rabbit").await.unwrap(), storage.tagged("red").await.unwrap());
        let born = NodeId::mint();
        mp.rows.claim(&victim.name, &born, "create-node:mesh1.rpc.1", None).await.expect("a node created at the Deleted row's path.name claims it");
        (row, storage.node_row_at(&victim.name).await.unwrap(), victim.node_id, rabbit, red)
    });
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: victim.clone(), status: NodeStatus::Deleted, config: tier("gold"), tags: ["rabbit".to_string(), "red".to_string()].into(), allocated: None, otel_debug: false }), "the Deleted row keeps the record, the config and the tags");
    assert!(rabbit.is_empty() && red.is_empty(), "the tags index names the deleted node under neither label: rabbit {rabbit:?}, red {red:?}");
    let born_row = born_row.expect("the new node's row");
    assert_eq!((born_row.config, born_row.tags.len()), (tier("gold"), 0), "the new node is seeded the config and carries no tag");
    let spans = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["outcome"].as_str(), a["tags"].as_str()), (Some("row-deleted"), Some("rabbit,red")), "{a:#?}");
    eprintln!("trace {}: rdm.node_admin.node.delete.via-node-row subject=mesh1.rpc.1 tags=rabbit,red outcome=row-deleted", spans[0]["trace_id"].as_str().unwrap_or(""));
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
        storage.put_node_row(&NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: old.clone(), status: NodeStatus::Deleted, config: tier("silver"), tags: Default::default(), allocated: None, otel_debug: false }).await.unwrap().unwrap();
        let mp = mesh_primary(&storage);
        let born = NodeId::mint();
        mp.rows.claim(&"mesh1.rpc.2".parse().unwrap(), &born, "create-node:mesh1.rpc.2", None).await.expect("the name is free of live rows");
        (storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, old)
    });
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Pending, config: tier("silver"), tags: Default::default(), allocated: None, otel_debug: false }), "the new birth holds the row and the Deleted row's config");
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
        storage.put_node_row(&NodeRow { name: name.clone(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: tier("silver"), tags: Default::default(), allocated: None, otel_debug: false }).await.unwrap().unwrap();
        let mp = mesh_primary(&storage);
        let born = NodeId::mint();
        mp.rows.claim(&name, &born, "create-node:mesh1.rpc.2", None).await.expect("the name is free of live rows");
        // The executor's record of the birth it created: no config on it.
        let mut record = Node::allocated(name.clone());
        record.node_id = born.clone();
        assert!(record.config.is_empty());
        mp.rows.deleted(&record, "retire-node:mesh1.rpc.2").await.expect("the Deleted row is written");
        let deleted = storage.node_row_at(&name).await.unwrap();
        let next = NodeId::mint();
        mp.rows.claim(&name, &next, "create-node:mesh1.rpc.2", None).await.expect("the Deleted row holds no name");
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
        let held = NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::ReadyForTraffic, config: tier("silver"), tags: Default::default(), allocated: None, otel_debug: false };
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
        storage.put_node_row(&NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: old.clone(), status: NodeStatus::Deleted, config: tier("silver"), tags: Default::default(), allocated: None, otel_debug: false }).await.unwrap().unwrap();
        let mp = mesh_primary(&storage);
        let birth = node("mesh1.rpc.2", false, false);
        let born = birth.node_id.clone();
        install(&mp.records, &mp.view, move |v| v.nodes.push(birth)).await;
        (storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, old)
    });
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: born.clone(), status: NodeStatus::ReadyForTraffic, config: tier("silver"), tags: Default::default(), allocated: None, otel_debug: false }), "the birth holds the row and the Deleted row's config");
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
        let written = NodeRow { name: rpc1.name.clone(), node_id: rpc1.node_id.clone(), status: NodeStatus::Draining, config: tier("platinum"), tags: Default::default(), allocated: None, otel_debug: false };
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
            NodeRow { name: rpc1.name.clone(), node_id: rpc1.node_id.clone(), status: NodeStatus::Draining, config: tier("platinum"), tags: Default::default(), allocated: None, otel_debug: false },
            NodeRow { name: "mesh1.rpc.2.old".parse().unwrap(), node_id: old_pred.clone(), status: NodeStatus::Deleted, config: tier("bronze"), tags: Default::default(), allocated: None, otel_debug: false },
            NodeRow { name: "mesh1.rpc.3".parse().unwrap(), node_id: deleted_pred.clone(), status: NodeStatus::Deleted, config: tier("silver"), tags: Default::default(), allocated: None, otel_debug: false },
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
/// replaced predecessor's row moved to `mesh1.rpc.2.old` (Deleted, config bronze) and a deleted
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
            let held = NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: id.clone(), status: NodeStatus::Draining, config: tier("platinum"), tags: Default::default(), allocated: None, otel_debug: false };
            store.put_node_row(&held).await.unwrap().unwrap();
            assert!(!store.release_node_row(&held.name, &NodeId::mint()).await.unwrap(), "{kind}: another node lets go of nothing here");
            assert_eq!(store.node_row_at(&held.name).await.unwrap(), Some(held.clone()), "{kind}: a release by another node leaves the row");
            assert!(store.release_node_row(&held.name, &id).await.unwrap(), "{kind}: the holder lets go of its key");
            assert_eq!(store.node_row_at(&held.name).await.unwrap(), None, "{kind}: the holder's release frees the key");
        }
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// CONTRACT: a replace moves the predecessor's row to `<path.name>.old`, where it stays the parked
/// predecessor's row (node-replace.md: the predecessor stays parked as `<name>.old`; its delete is a
/// later step of the recipe), and the successor created at the `path.name` seeds its config from the
/// `.old` row (ops-naming "Node data": "a Deleted row, or the `<name>.old` row"). The mesh-primary
/// executing the replace writes `<path>.old` at `RenamePredecessor` from the exact predecessor record
/// it acts on and releases `<path>`; the successor's claim takes `<path>` seeded from the `.old` row.
/// No row says `Deleted`: the replace deletes nothing.
#[test]
fn a_replaced_nodes_row_moves_to_old_and_its_successor_is_seeded_from_it() {
    let cap = capture();
    let (old_row, row, pred, succ) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let pred = held(&mp, "mesh1.rpc.1").await;
        storage.put_node_row(&row_of(&pred)).await.unwrap().unwrap();
        let op = "replace-node:mesh1.rpc.1";
        mp.rows.moved_to_old(&pred, op).await.expect("the predecessor's row moves");
        let succ = NodeId::mint();
        mp.rows.claim(&pred.name, &succ, op, None).await.expect("the freed name is claimed");
        (storage.node_row_at(&pred.name.renamed()).await.unwrap(), storage.node_row_at(&pred.name).await.unwrap(), pred.node_id, succ)
    });
    assert_eq!(old_row, Some(NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), node_id: pred.clone(), status: NodeStatus::Leaving, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false }), "the predecessor's row moved to .old, parked, and stays its row, its config kept");
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: succ.clone(), status: NodeStatus::Pending, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false }), "the successor holds the path.name with the .old row's config");
    let renames = cap.spans_named("rdm.node_admin.node.update.via-rename");
    assert_eq!(renames.len(), 1, "{renames:#?}");
    let a = &renames[0]["attributes"];
    assert_eq!((a["node"].as_str(), a["from"].as_str(), a["to"].as_str(), a["outcome"].as_str()), (Some("mesh1.admin.2"), Some("mesh1.rpc.1"), Some("mesh1.rpc.1.old"), Some("renamed")), "{a:#?}");
    assert!(cap.spans_named("rdm.node_admin.node.delete.via-node-row").is_empty(), "a replace writes no Deleted row");
    let seeded: Vec<Value> = cap.spans_named("rdm.node_admin.node.create.via-node-row").into_iter().filter(|s| s["attributes"]["node_id"] == succ.as_str()).collect();
    assert_eq!(seeded.len(), 1, "{seeded:#?}");
    let a = &seeded[0]["attributes"];
    assert_eq!((a["subject"].as_str(), a["seeded_from"].as_str(), a["outcome"].as_str()), (Some("mesh1.rpc.1"), Some(pred.as_str()), Some("row-seeded")), "{a:#?}");
}

/// CONTRACT: the recovery of a proven-terminal birth (the replace operation's `NodeDeleting` branch)
/// moves the predecessor's row to `<path.name>.old`, the successor's claim takes the `path.name`
/// seeded from the `.old` row, and the predecessor's departure leaves a `Deleted` row at `.old`, its
/// config kept: the mesh-primary executing it writes each from the exact predecessor record.
#[test]
fn a_recovered_nodes_row_moves_to_old_ends_deleted_and_its_successor_is_seeded_from_it() {
    let cap = capture();
    let (old_row, row, pred, succ) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let pred = held(&mp, "mesh1.rpc.1").await;
        storage.put_node_row(&row_of(&pred)).await.unwrap().unwrap();
        let op = "replace-node:mesh1.rpc.1";
        mp.rows.moved_to_old(&pred, op).await.expect("the predecessor's row moves");
        let succ = NodeId::mint();
        mp.rows.claim(&pred.name, &succ, op, None).await.expect("the freed name is claimed");
        mp.rows.deleted(&Node { name: pred.name.renamed(), ..pred.clone() }, op).await.expect("the .old row says Deleted");
        (storage.node_row_at(&pred.name.renamed()).await.unwrap(), storage.node_row_at(&pred.name).await.unwrap(), pred.node_id, succ)
    });
    assert_eq!(old_row, Some(NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), node_id: pred.clone(), status: NodeStatus::Deleted, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false }), "the predecessor's row moved to .old and says Deleted, its config kept");
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: succ.clone(), status: NodeStatus::Pending, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false }), "the successor holds the path.name with the .old row's config");
    let deleted = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(deleted.len(), 1, "{deleted:#?}");
    let a = &deleted[0]["attributes"];
    assert_eq!((a["subject"].as_str(), a["node_id"].as_str(), a["outcome"].as_str()), (Some("mesh1.rpc.1.old"), Some(pred.as_str()), Some("row-deleted")), "{a:#?}");
}

/// CONTRACT: a successor never takes a name its predecessor's row still holds (R13: two nodes never
/// hold one name; the store adapter's insert-and-fail refuses `NameHeld`). Here the successor enters
/// the writer's view at the path.name before the replace's `RenamePredecessor`: its row is refused
/// `name-held` naming the predecessor, and nothing of it is written. `RenamePredecessor` then moves
/// the predecessor's row to `.old` and lets go of the path.name, which no row holds after.
#[test]
fn a_successor_heard_before_the_predecessor_lets_go_is_refused_name_held_and_the_rename_frees_the_name() {
    let cap = capture();
    let (old_row, row, pred, succ) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let pred = held(&mp, "mesh1.rpc.1").await;
        storage.put_node_row(&row_of(&pred)).await.unwrap().unwrap();
        let successor = node("mesh1.rpc.1", false, false);
        let succ = successor.node_id.clone();
        install(&mp.records, &mp.view, move |v| {
            v.nodes.retain(|n| n.name.to_string() != "mesh1.rpc.1");
            v.nodes.push(successor);
        })
        .await;
        mp.rows.moved_to_old(&Node { status: NodeStatus::Leaving, parked: true, ..pred.clone() }, "replace-node:mesh1.rpc.1").await.expect("the predecessor's row moves");
        (storage.node_row_at(&pred.name.renamed()).await.unwrap(), storage.node_row_at(&pred.name).await.unwrap(), pred.node_id, succ)
    });
    assert_eq!(old_row, Some(NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), node_id: pred.clone(), status: NodeStatus::Leaving, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false }), "the predecessor's row moved to .old, parked, its config kept");
    assert_eq!(row, None, "the successor was refused while the predecessor held the name, and the rename let it go");
    let renames = cap.spans_named("rdm.node_admin.node.update.via-rename");
    assert_eq!(renames.len(), 1, "{renames:#?}");
    assert_eq!(renames[0]["attributes"]["outcome"].as_str(), Some("renamed"), "{renames:#?}");
    let claims: Vec<Value> = cap.spans_named("rdm.node_admin.node.create.via-node-row").into_iter().filter(|s| s["attributes"]["node_id"] == succ.as_str()).collect();
    let outcomes: Vec<&str> = claims.iter().map(|s| s["attributes"]["outcome"].as_str().unwrap_or("")).collect();
    assert_eq!(outcomes, vec!["name-held"], "{claims:#?}");
    assert!(claims[0]["attributes"]["detail"].as_str().is_some_and(|d| d.contains(pred.as_str())), "the refusal names the predecessor: {claims:#?}");
    eprintln!("trace {}: rdm.node_admin.node.create.via-node-row subject=mesh1.rpc.1 outcome=name-held before the rename; trace {}: via-rename outcome=renamed", claims[0]["trace_id"].as_str().unwrap_or(""), renames[0]["trace_id"].as_str().unwrap_or(""));
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

/// CONTRACT: the rename is the predecessor letting go of its own key, never anyone else's: a
/// `RenamePredecessor` run again after the move (the predecessor's row at `<path>.old` already, the
/// successor holding `<path>` with a config of its own) leaves both rows exactly as they are. The
/// successor's row is not released and rewritten.
#[test]
fn a_rename_handled_again_after_the_successor_took_the_name_leaves_its_row() {
    let cap = capture();
    let (old_row, row, held_old, held_new) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let pred = held(&mp, "mesh1.rpc.1").await;
        let held_old = NodeRow { name: pred.name.renamed(), node_id: pred.node_id.clone(), status: pred.status, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false };
        let held_new = NodeRow { name: pred.name.clone(), node_id: NodeId::mint(), status: NodeStatus::ReadyForTraffic, config: tier("platinum"), tags: Default::default(), allocated: None, otel_debug: false };
        storage.put_node_row(&held_old).await.unwrap().unwrap();
        storage.put_node_row(&held_new).await.unwrap().unwrap();
        mp.rows.moved_to_old(&pred, "replace-node:mesh1.rpc.1").await.expect("a move made before is no refusal");
        (storage.node_row_at(&pred.name.renamed()).await.unwrap(), storage.node_row_at(&pred.name).await.unwrap(), held_old, held_new)
    });
    assert_eq!(old_row, Some(held_old), "the predecessor's .old row stands");
    assert_eq!(row, Some(held_new), "the successor's row, its own config, stands: the rename released no key the predecessor does not hold");
    let renames = cap.spans_named("rdm.node_admin.node.update.via-rename");
    let outcomes: Vec<&str> = renames.iter().map(|s| s["attributes"]["outcome"].as_str().unwrap_or("")).collect();
    assert_eq!(outcomes, vec!["already-moved"], "{renames:#?}");
}

/// CONTRACT: the fabric-primary holds the record of the parked predecessor a replace left at
/// `<path>.old` although its view never names that path (a fabric-primary in another mesh than the
/// replace's executor: its view drops the parked predecessor under the successor at `<path>`), fed
/// from the replace's frames its drive reads. From that held record it admits
/// `DELETE /api/nodes/<path>.old` as the next attempt fenced to the exact old birth; at the delete's
/// `NodeDeleted` it holds the record no more. It writes no row of `mesh1`, which has a primary: the
/// mesh primary that executes the delete writes the Deleted row (node-decommission.md 3; R13).
#[test]
fn a_fabric_primary_whose_view_never_names_the_old_path_admits_the_parked_predecessors_delete_and_writes_no_row() {
    let cap = capture();
    let (pred, held, opened, row, held_after) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let successor = node("mesh1.rpc.1", false, false);
        let op = "replace-node:mesh1.rpc.1";
        // This fabric-primary's view, as a peer's: the stop drops the predecessor, the successor arrives
        // at the path; nothing here names `<path>.old`.
        let heard = Act::View(Box::new(move |v: &mut Topology| {
            v.nodes.retain(|n| n.name.to_string() != "mesh1.rpc.1");
            v.nodes.push(successor);
        }));
        let acts = vec![
            Act::Frame(op, "DrainNode"),
            Act::Frame(op, "AwaitNodeDrained"),
            Act::Frame(op, "StopNode"),
            Act::Frame(op, "AwaitNodeLeft"),
            Act::Frame(op, "RenamePredecessor"),
            Act::Frame(op, "AllocateIdentity"),
            heard,
            Act::Frame(op, "WaitForNodeReady"),
            Act::Frame(op, "Complete"),
        ];
        let r = rig(TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, acts, storage).await;
        let pred = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        r.storage.put_node_row(&NodeRow { name: pred.name.clone(), node_id: pred.node_id.clone(), status: pred.status, config: pred.config.clone(), tags: Default::default(), allocated: None, otel_debug: false }).await.unwrap().unwrap();
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
        let old: PathName = "mesh1.rpc.1.old".parse().unwrap();
        assert!(r.env.topology.read().await.node(&old).is_none(), "the view never names {old}");
        let held = r.env.rows.parked_at(&old);
        // The replace's attempt converged, as its executor's attempt receipt says.
        let attempt = r.env.builds.read_build(&r.build).await.unwrap().attempt;
        let converged = rafka_node_admin_core::build_state::BuildAttemptReceipt { build_id: r.build.clone(), attempt, outcome: rafka_node_admin_core::build_state::AttemptOutcome::Converged };
        r.env.builds.append_attempt_receipt(&converged).await.unwrap();
        // The door: the next attempt of the accepted Build, fenced to the held record's exact birth.
        let mut cp = ControlPlane::new(r.env.builds.clone(), r.env.accepted.clone(), "mesh1.admin.1".parse().unwrap(), r.env.topology.read().await.clone(), crate::adopted_time());
        cp.topology = r.env.topology.clone();
        let rows = r.env.rows.clone();
        let _ = cp.parked_old.set(Arc::new(move |p: &PathName| rows.parked_at(p)));
        let opened = cp.open_attempt("DELETE /api/nodes/{name}", rafka_node_admin_core::build_state::AttemptReason::Delete, old.clone(), rafka_node_admin_core::http::ActionKind::Delete, None).await;
        let opened = opened.map(|o| o.build_id).map_err(|e| format!("{e:?}"));
        let read = r.env.builds.read_build(&r.build).await.unwrap();
        let opened = opened.map(|_| serde_json::to_value(&read.action).unwrap());
        // The delete's frames, as the executor streams them: the view still names no `.old` path.
        let del = "delete-node:mesh1.rpc.1.old";
        r.env.rows.on_step(&r.env.topology, del, "NodeDeleting", None).await;
        r.env.rows.on_step(&r.env.topology, del, "NodeDeleted", None).await;
        (pred, held, opened, r.storage.node_row_at(&old).await.unwrap(), r.env.rows.parked_at(&old))
    });
    let held = held.expect("the fabric-primary holds the parked predecessor's record at mesh1.rpc.1.old");
    assert_eq!((&held.node_id, &held.incarnation_id, held.parked, held.status), (&pred.node_id, &pred.incarnation_id, true, NodeStatus::Leaving), "the exact old birth, parked: {held:?}");
    let action = opened.unwrap_or_else(|e| panic!("the delete is admitted from the held record: {e}"));
    assert_eq!((action["action"].as_str(), action["path"].as_str()), (Some("delete"), Some("mesh1.rpc.1.old")), "{action}");
    assert_eq!(action["from_incarnation"].as_str(), pred.incarnation_id.as_ref().map(|i| i.0.as_str()), "fenced to the exact old birth: {action}");
    assert_eq!(row, None, "the fabric-primary writes no row at .old of a mesh that has a primary");
    assert!(held_after.is_none(), "the record is held no more once its delete's NodeDeleted is read");
    assert!(cap.spans_named("rdm.node_admin.node.delete.via-node-row").is_empty(), "the fabric-primary writes no Deleted row of mesh1");
    assert!(cap.spans_named("rdm.node_admin.node.update.via-rename").is_empty(), "the fabric-primary moves no row of mesh1");
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
        async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
            Ok(rafka_node_admin_core::deployment::provider::Found::NeverLaunched)
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
        // The Build the create runs for: a run over a Build its node holds no fact of is refused at its start.
        let build_id = BuildId("build-node-rows".into());
        let accepted = rafka_node_admin_core::build_state::BuildAccepted { build_id: build_id.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
        rafka_node_admin_core::build_state::BuildStateAdapter::publish_accepted(&builds, &accepted).await.unwrap();
        let pipeline = DeploymentPipeline { provider, joins: &joins, observer: &Unheard, sink: records, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let req = CreateRequest { build_id, attempt: 1, node: name.parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() };
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
            let held = NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::ReadyForTraffic, config: tier("silver"), tags: Default::default(), allocated: None, otel_debug: false };
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
                let accepted = rafka_node_admin_core::build_state::BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
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
            storage.put_node_row(&NodeRow { name: "mesh1.rpc.6".parse().unwrap(), node_id: x.clone(), status: NodeStatus::Pending, config: BTreeMap::new(), tags: Default::default(), allocated: None, otel_debug: false }).await.unwrap().unwrap();
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

    /// A provider that never answers `find`: the executor that runs on it stops inside
    /// `DeployRuntime` before anything is launched, its claim written, and is lost there. `entered`
    /// fires when it gets that far.
    struct LostBeforeLaunch {
        entered: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for LostBeforeLaunch {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            "node-rows".into()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "the lost executor launches nothing".into() })
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
        async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
            self.entered.notify_one();
            std::future::pending().await
        }
    }

    /// Launches nothing, and records the deployment of every `find` and every spawn it is asked for.
    /// Its `find` proves the deployment named `exited` launched and exited, and every other one never
    /// launched.
    #[derive(Default)]
    struct Proving {
        found: Mutex<Vec<DeploymentId>>,
        spawned: Mutex<Vec<DeploymentId>>,
        exited: Mutex<Option<DeploymentId>>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for Proving {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            "node-rows".into()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            self.spawned.lock().unwrap().push(spec.deployment_id.clone());
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
        async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
            self.found.lock().unwrap().push(spec.deployment_id.clone());
            Ok(if self.exited.lock().unwrap().as_ref() == Some(&spec.deployment_id) { rafka_node_admin_core::deployment::provider::Found::Exited } else { rafka_node_admin_core::deployment::provider::Found::NeverLaunched })
        }
    }

    /// Attempt `attempt` of `create-node:<name>` in the Build `build`, its receipts in `builds`, run by
    /// the executor whose rows writer `records` feeds, over `provider`, its births' data dirs under
    /// `data_root`: its error.
    async fn create_attempt_over(records: &Records, provider: &dyn DeploymentProvider, observer: &dyn NodeObserver, builds: &dyn rafka_node_admin_core::build_state::BuildStateAdapter, build: &BuildId, name: &str, attempt: u32, data_root: &std::path::Path) -> Result<(), String> {
        create_attempt_executing("/nonexistent/node-rows", records, provider, observer, builds, build, name, attempt, data_root).await
    }

    /// [`create_attempt_over`] for a template whose executable is `executable`.
    #[allow(clippy::too_many_arguments)]
    async fn create_attempt_executing(executable: &str, records: &Records, provider: &dyn DeploymentProvider, observer: &dyn NodeObserver, builds: &dyn rafka_node_admin_core::build_state::BuildStateAdapter, build: &BuildId, name: &str, attempt: u32, data_root: &std::path::Path) -> Result<(), String> {
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: executable.into(),
            seeds: vec![],
            launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.2".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.to_path_buf(),
            store_dir: data_root.join("store"),
            mesh_issuer: None,
        };
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider, joins: &joins, observer, sink: records, lifecycle: &NoLifecycleEvents, builds, template: &template, timeouts: Timeouts::default() };
        let req = CreateRequest { build_id: build.clone(), attempt, node: name.parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() };
        pipeline.create(&req).await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// CONTRACT (build.md §5.2: a step receipt is not committed until it is durable in storage that
    /// survives the loss of the admin that wrote it, and the Build journal is never copied into the
    /// shared store root; node-rpc-envelope.md Build carrier: "Completed steps are not rerun", an
    /// idempotent uncertain step is settled by its natural key; lifecycles/node-create.md step 1: the
    /// claim precedes the launch, and "Runtime exit requires independent exact provider proof, even for
    /// a deployment that never joined"): attempt 1 of `create-node:mesh1.rpc.7` runs on executor A
    /// through its name claim and A is lost inside `DeployRuntime`, before anything is launched. A's
    /// receipts are in A's own journal only and never reach B's. B runs attempt 2 holding only what
    /// the shared store holds: A's claim row. B's create takes the identity A's claim was made with
    /// (the same node id, incarnation and deployment), the provider proving no runtime of that exact
    /// deployment exists, its claim finds the row that birth holds (`row-held-by-node`) and it reaches
    /// `DeployRuntime` with A's deployment. Must NOT happen: B minting a new birth that the name claim
    /// refuses `name-held` by A's birth that never launched, attempt after attempt (gate
    /// gate-gates-e6s4-d7fcc329 node_replace: "name-held: mesh2.rpc.1 is held by node nrsm925mnyf8",
    /// 200 attempts, the path never recreated).
    #[test]
    fn a_create_whose_executor_is_lost_after_its_claim_is_continued_under_the_claimed_identity_by_an_executor_that_never_heard_its_receipts() {
        use rafka_node_admin_core::build_state::{BuildAccepted, BuildFact, BuildStateAdapter, StepOutcome};
        let cap = capture();
        let (a_identity, b_err, found, spawned, row) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-lost-after-claim-{}", NodeId::mint()));
            let build = BuildId("build-node-rows-7".into());
            let (a_builds, b_builds) = (Arc::new(MemoryBuildStateAdapter::new()), MemoryBuildStateAdapter::new());
            for builds in [&*a_builds, &b_builds] {
                let accepted = BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
                builds.publish_accepted(&accepted).await.unwrap();
            }
            // Executor A: attempt 1 through its claim, lost before it launches anything.
            let (a_records, _a_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let entered = Arc::new(tokio::sync::Notify::new());
            let a = {
                let (a_builds, data_root, build, entered) = (a_builds.clone(), data_root.clone(), build.clone(), entered.clone());
                tokio::spawn(async move {
                    let lost = LostBeforeLaunch { entered };
                    create_attempt_over(&a_records, &lost, &Unheard, &*a_builds, &build, "mesh1.rpc.7", 1, &data_root).await
                })
            };
            entered.notified().await;
            a.abort();
            let _ = a.await;
            let a_identity = a_builds
                .facts()
                .await
                .unwrap()
                .into_iter()
                .find_map(|f| match f {
                    BuildFact::Step(r) if r.step == "AllocateIdentity" && r.outcome == StepOutcome::Complete => r.output,
                    _ => None,
                })
                .expect("A's journal holds its AllocateIdentity receipt");
            // Executor B: attempt 2, its journal holding none of A's receipts.
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let proving = Proving::default();
            let b_err = create_attempt_over(&b_records, &proving, &Unheard, &b_builds, &build, "mesh1.rpc.7", 2, &data_root).await;
            let row = storage.node_row_at(&"mesh1.rpc.7".parse().unwrap()).await.unwrap();
            let _ = std::fs::remove_dir_all(&data_root);
            (a_identity, b_err, proving.found.into_inner().unwrap(), proving.spawned.into_inner().unwrap(), row)
        });
        let x = a_identity["node_id"].as_str().expect("A's identity names its node id").to_string();
        let deployment = a_identity["deployment_id"].as_str().expect("A's identity names its deployment").to_string();
        let b_err = b_err.expect_err("B's attempt fails where this provider launches nothing");
        assert!(b_err.contains("DeployRuntime") && !b_err.contains("name-held"), "B continues A's create past its claim and reaches DeployRuntime: {b_err}");
        assert_eq!(spawned.iter().map(|d| d.0.clone()).collect::<Vec<_>>(), vec![deployment.clone()], "B launches A's exact deployment, never a new one");
        assert!(found.iter().any(|d| d.0 == deployment), "the provider was asked for A's exact deployment before anything was launched: {found:?}");
        assert_eq!(row, None, "B's create failed after its claim (this provider launches nothing) and withdrew that birth's claim, as every create failing after its claim does: {row:?}");
        let claims = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let rpc7: Vec<(String, String)> = claims.iter().filter(|s| s["attributes"]["subject"] == "mesh1.rpc.7" && s["attributes"]["moment"] == "claim").map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
        assert_eq!(rpc7, vec![(x.clone(), "row-written".to_string()), (x.clone(), "row-held-by-node".to_string())], "A's claim is written and B's holds that same birth's row: {rpc7:?}");
        let settled = cap.spans_named("rdm.node_admin.node.resolve.via-claimed-allocation");
        assert_eq!(settled.len(), 1, "B settles the identity from A's claim once: {settled:#?}");
        let a = &settled[0]["attributes"];
        assert_eq!(
            (a["node_id"].as_str(), a["deployment_id"].as_str(), a["operation"].as_str(), a["attempt"].as_str(), a["outcome"].as_str()),
            (Some(x.as_str()), Some(deployment.as_str()), Some("create-node:mesh1.rpc.7"), Some("2"), Some("never-launched")),
            "the claim names the birth A allocated and the provider proves no runtime of its deployment exists: {a:#?}"
        );
        let withdrawn = cap.spans_named("rdm.node_admin.node.remove.via-claim-withdrawn");
        assert_eq!(withdrawn.iter().map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(x.clone(), "withdrawn".to_string())], "only B's failure withdraws, and the claim it withdraws is that same birth's: {withdrawn:#?}");
        let trace = claims.iter().find(|s| s["attributes"]["subject"] == "mesh1.rpc.7").and_then(|s| s["trace_id"].as_str()).unwrap_or("");
        eprintln!(
            "trace {trace}: via-node-row claims of mesh1.rpc.7 {rpc7:?}; trace {}: via-claimed-allocation node_id={x} deployment_id={deployment} outcome=never-launched; B launched deployment {deployment}",
            settled[0]["trace_id"].as_str().unwrap_or("")
        );
    }

    /// Answers `find` with a running runtime of the deployment asked for (the one the lost executor
    /// launched), records every terminate and spawn, and launches nothing itself.
    #[derive(Default)]
    struct LaunchedByTheLost {
        found: Mutex<Vec<DeploymentId>>,
        terminated: Mutex<Vec<DeploymentId>>,
        spawned: Mutex<Vec<DeploymentId>>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for LaunchedByTheLost {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            "node-rows".into()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            self.spawned.lock().unwrap().push(spec.deployment_id.clone());
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this provider launches nothing".into() })
        }
        async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
            self.terminated.lock().unwrap().push(h.deployment_id.clone());
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
            DeploymentStatus::Unknown
        }
        async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
            let first = { let mut f = self.found.lock().unwrap(); f.push(spec.deployment_id.clone()); f.len() == 1 };
            Ok(match first {
                true => rafka_node_admin_core::deployment::provider::Found::Running(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(4242), start: Some(7), container: None, domain: Some("node-rows".into()) }),
                false => rafka_node_admin_core::deployment::provider::Found::NeverLaunched,
            })
        }
    }

    /// CONTRACT (the lost-executor rule beside `create_to_ready_claimed`'s receipt check: a runtime
    /// launched by an executor that is not this one and never joined is stopped and the birth decided
    /// afresh; lifecycles/node-create.md: exact provider proof): A claims `mesh1.rpc.8` in attempt 1
    /// and is lost, and the provider holds a running runtime of the exact deployment A's claim names.
    /// B, holding none of A's receipts, stops that runtime through its exact handle, withdraws that
    /// birth's claim (its key only), mints a new birth, claims the free name for it and reaches
    /// `DeployRuntime` with a deployment of its own. Must NOT happen: B continuing a birth whose
    /// runtime the lost executor seeded, B refused `name-held` by it, or the runtime left running.
    #[test]
    fn a_create_whose_lost_executor_launched_its_claimed_birth_stops_that_runtime_and_decides_a_new_birth() {
        use rafka_node_admin_core::build_state::{BuildAccepted, BuildFact, BuildStateAdapter, StepOutcome};
        let cap = capture();
        let (a_identity, b_err, terminated, spawned) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-launched-by-the-lost-{}", NodeId::mint()));
            let build = BuildId("build-node-rows-8".into());
            let (a_builds, b_builds) = (Arc::new(MemoryBuildStateAdapter::new()), MemoryBuildStateAdapter::new());
            for builds in [&*a_builds, &b_builds] {
                let accepted = BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
                builds.publish_accepted(&accepted).await.unwrap();
            }
            let (a_records, _a_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let entered = Arc::new(tokio::sync::Notify::new());
            let a = {
                let (a_builds, data_root, build, entered) = (a_builds.clone(), data_root.clone(), build.clone(), entered.clone());
                tokio::spawn(async move { create_attempt_over(&a_records, &LostBeforeLaunch { entered }, &Unheard, &*a_builds, &build, "mesh1.rpc.8", 1, &data_root).await })
            };
            entered.notified().await;
            a.abort();
            let _ = a.await;
            let a_identity = a_builds
                .facts()
                .await
                .unwrap()
                .into_iter()
                .find_map(|f| match f {
                    BuildFact::Step(r) if r.step == "AllocateIdentity" && r.outcome == StepOutcome::Complete => r.output,
                    _ => None,
                })
                .expect("A's journal holds its AllocateIdentity receipt");
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let provider = LaunchedByTheLost::default();
            let b_err = create_attempt_over(&b_records, &provider, &Unheard, &b_builds, &build, "mesh1.rpc.8", 2, &data_root).await;
            let _ = std::fs::remove_dir_all(&data_root);
            (a_identity, b_err, provider.terminated.into_inner().unwrap(), provider.spawned.into_inner().unwrap())
        });
        let x = a_identity["node_id"].as_str().expect("A's identity names its node id").to_string();
        let deployment = a_identity["deployment_id"].as_str().expect("A's identity names its deployment").to_string();
        let b_err = b_err.expect_err("B's attempt fails where this provider launches nothing");
        assert!(b_err.contains("DeployRuntime") && !b_err.contains("name-held"), "B decides a new birth and reaches DeployRuntime: {b_err}");
        assert_eq!(terminated.iter().map(|d| d.0.clone()).collect::<Vec<_>>(), vec![deployment.clone()], "the lost executor's runtime is stopped through its exact handle");
        assert_eq!(spawned.len(), 1, "{spawned:?}");
        assert_ne!(spawned[0].0, deployment, "B launches a deployment of its own, never the lost executor's");
        let settled = cap.spans_named("rdm.node_admin.node.resolve.via-claimed-allocation");
        assert_eq!(settled.iter().map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(x.clone(), "launched-by-lost-executor".to_string())], "{settled:#?}");
        let claims = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let rpc8: Vec<(String, String)> = claims.iter().filter(|s| s["attributes"]["subject"] == "mesh1.rpc.8" && s["attributes"]["moment"] == "claim").map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
        assert_eq!(rpc8.len(), 2, "{rpc8:?}");
        assert_eq!(rpc8[0], (x.clone(), "row-written".to_string()), "A's claim: {rpc8:?}");
        assert!(rpc8[1].0 != x && rpc8[1].1 == "row-written", "B's claim is a new birth's, written once A's is withdrawn: {rpc8:?}");
        let withdrawn = cap.spans_named("rdm.node_admin.node.remove.via-claim-withdrawn");
        let w: Vec<(String, String)> = withdrawn.iter().map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
        assert_eq!(w, vec![(x.clone(), "withdrawn".to_string()), (rpc8[1].0.clone(), "withdrawn".to_string())], "A's birth's claim is withdrawn by B before it claims, and B's own by its failure: {w:?}");
        let stopped = cap.spans_named("rdm.node_admin.deployment.reject.via-lost-executor");
        assert_eq!(stopped.iter().map(|s| s["attributes"]["deployment_id"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>(), vec![deployment.clone()], "{stopped:#?}");
        eprintln!("trace {}: via-claimed-allocation node_id={x} outcome=launched-by-lost-executor; via-lost-executor deployment_id={deployment}; claims {rpc8:?}", settled[0]["trace_id"].as_str().unwrap_or(""));
    }

    /// Answers that every birth asked about has joined fabric membership.
    struct Joined;
    #[async_trait::async_trait]
    impl NodeObserver for Joined {
        async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
            Some(Publication { runtime: None, data_dir: None })
        }
        async fn ready(&self, _: &Node) -> Result<(), String> {
            Err("nothing is launched here".into())
        }
    }

    /// Executor A runs attempt 1 of `create-node:<name>` in `build` through its claim and is lost inside
    /// `DeployRuntime` before it launches anything; its births' data dirs under `data_root`. Returns
    /// the identity A's AllocateIdentity receipt names.
    async fn lost_after_claim(storage: &Arc<MemoryNodesStorage>, build: &BuildId, name: &str, data_root: &std::path::Path) -> Value {
        use rafka_node_admin_core::build_state::{BuildFact, BuildStateAdapter, StepOutcome};
        let a_builds = Arc::new(MemoryBuildStateAdapter::new());
        let accepted = rafka_node_admin_core::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
        a_builds.publish_accepted(&accepted).await.unwrap();
        let (a_records, _a_view) = writer_of("mesh1.admin.2", &view(), storage);
        let entered = Arc::new(tokio::sync::Notify::new());
        let a = {
            let (a_builds, data_root, build, entered, name) = (a_builds.clone(), data_root.to_path_buf(), build.clone(), entered.clone(), name.to_string());
            tokio::spawn(async move { create_attempt_over(&a_records, &LostBeforeLaunch { entered }, &Unheard, &*a_builds, &build, &name, 1, &data_root).await })
        };
        entered.notified().await;
        a.abort();
        let _ = a.await;
        a_builds
            .facts()
            .await
            .unwrap()
            .into_iter()
            .find_map(|f| match f {
                BuildFact::Step(r) if r.step == "AllocateIdentity" && r.outcome == StepOutcome::Complete => r.output,
                _ => None,
            })
            .expect("A's journal holds its AllocateIdentity receipt")
    }

    /// CONTRACT (lifecycles/node-create.md: exact provider proof of a runtime's state, even for a
    /// deployment that never joined; the receipt arm's rule: "the earlier attempt's runtime is gone: a
    /// fresh birth"; node-identity-and-state.md: an incarnation names one exact runtime): A claims
    /// `mesh1.rpc.9` in attempt 1, launches its birth, and is lost; that runtime has exited (the
    /// provider proves A's deployment launched and exited, `Found::Exited`). B, holding none of A's
    /// receipts, does not continue that birth: it withdraws its
    /// claim (that birth's key only), mints a new birth, claims the free name for it and reaches
    /// `DeployRuntime` with a deployment of its own. Must NOT happen: B relaunching the exited birth
    /// under A's node id, incarnation and deployment.
    #[test]
    fn a_create_whose_lost_executor_launched_its_claimed_birth_which_exited_decides_a_new_birth() {
        let cap = capture();
        let (a_identity, b_err, spawned) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-launched-exited-{}", NodeId::mint()));
            let build = BuildId("build-node-rows-9".into());
            let a_identity = lost_after_claim(&storage, &build, "mesh1.rpc.9", &data_root).await;
            let b_builds = MemoryBuildStateAdapter::new();
            let accepted = rafka_node_admin_core::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
            rafka_node_admin_core::build_state::BuildStateAdapter::publish_accepted(&b_builds, &accepted).await.unwrap();
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            // A launched the birth and it exited: the provider proves it.
            let proving = Proving { exited: Mutex::new(Some(DeploymentId(a_identity["deployment_id"].as_str().unwrap().to_string()))), ..Proving::default() };
            let b_err = create_attempt_over(&b_records, &proving, &Unheard, &b_builds, &build, "mesh1.rpc.9", 2, &data_root).await;
            let _ = std::fs::remove_dir_all(&data_root);
            (a_identity, b_err, proving.spawned.into_inner().unwrap())
        });
        let x = a_identity["node_id"].as_str().unwrap().to_string();
        let deployment = a_identity["deployment_id"].as_str().unwrap().to_string();
        let b_err = b_err.expect_err("B's attempt fails where this provider launches nothing");
        assert!(b_err.contains("DeployRuntime") && !b_err.contains("name-held"), "B decides a new birth and reaches DeployRuntime: {b_err}");
        assert_eq!(spawned.len(), 1, "{spawned:?}");
        assert_ne!(spawned[0].0, deployment, "B launches a deployment of its own, never the exited birth's again");
        let claims = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let rpc9: Vec<(String, String)> = claims.iter().filter(|s| s["attributes"]["subject"] == "mesh1.rpc.9" && s["attributes"]["moment"] == "claim").map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
        assert!(rpc9.len() == 2 && rpc9[0] == (x.clone(), "row-written".to_string()) && rpc9[1].0 != x && rpc9[1].1 == "row-written", "A's claim, then a new birth's once A's is withdrawn: {rpc9:?}");
        let settled = cap.spans_named("rdm.node_admin.node.resolve.via-claimed-allocation");
        assert_eq!(settled.iter().map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(x.clone(), "launched-exited".to_string())], "{settled:#?}");
        eprintln!("trace {}: via-claimed-allocation node_id={x} outcome=launched-exited; claims {rpc9:?}", settled[0]["trace_id"].as_str().unwrap_or(""));
    }

    /// The process provider's own `find` and `terminate` over the data dirs of the births it is asked
    /// about; it records every spawn asked of it and launches nothing.
    struct OnDisk {
        inner: rafka_node_admin_core::deployment::process::ProcessDeploymentProvider,
        spawned: Mutex<Vec<DeploymentId>>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for OnDisk {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            self.inner.control_domain()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            self.spawned.lock().unwrap().push(spec.deployment_id.clone());
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this provider launches nothing".into() })
        }
        async fn terminate(&self, h: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
            self.inner.terminate(h, mode).await
        }
        async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
            self.inner.inspect(h).await
        }
        async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
            self.inner.find(spec).await
        }
    }

    /// CONTRACT (lifecycles/node-create.md: exact provider proof of a runtime's state, even for a
    /// deployment that never joined; the receipt arm's rule: "the earlier attempt's runtime is gone: a
    /// fresh birth"; node-identity-and-state.md: an incarnation names one exact runtime): A claims
    /// `mesh1.rpc.11` in attempt 1, launches its birth and is lost, and that runtime exits before it
    /// writes its runtime record: the birth's data dir holds only the process provider's deployment
    /// record naming A's deployment, and its process is gone. B, holding none of A's receipts, asks the
    /// process provider, which tells a launch that exited from one never launched: B withdraws A's
    /// claim (that birth's key only), mints a new birth, claims the free name for it and reaches
    /// `DeployRuntime` with a deployment of its own. Must NOT happen: B reading the exited launch as
    /// never launched and relaunching it under A's node id, incarnation and deployment.
    #[test]
    fn a_create_whose_lost_executor_launched_a_birth_that_exited_before_its_runtime_record_decides_a_new_birth() {
        let cap = capture();
        let (a_identity, b_err, spawned) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-exited-before-record-{}", NodeId::mint()));
            let build = BuildId("build-node-rows-11".into());
            let a_identity = lost_after_claim(&storage, &build, "mesh1.rpc.11", &data_root).await;
            // A launched the birth and its process exited before it wrote its runtime record: the data
            // dir holds the provider's deployment record of A's deployment, naming a pid that is gone.
            let dir = data_root.join(format!("mesh1.rpc.11-{}", a_identity["node_id"].as_str().unwrap()));
            std::fs::create_dir_all(&dir).unwrap();
            let mut gone = std::process::Command::new("true").spawn().unwrap();
            let pid = gone.id();
            gone.wait().unwrap();
            std::fs::write(dir.join("deployment.json"), json!({ "deployment_id": a_identity["deployment_id"], "pid": pid }).to_string()).unwrap();
            let b_builds = MemoryBuildStateAdapter::new();
            let accepted = rafka_node_admin_core::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
            rafka_node_admin_core::build_state::BuildStateAdapter::publish_accepted(&b_builds, &accepted).await.unwrap();
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let on_disk = OnDisk { inner: rafka_node_admin_core::deployment::process::ProcessDeploymentProvider::new(), spawned: Mutex::default() };
            let b_err = create_attempt_over(&b_records, &on_disk, &Unheard, &b_builds, &build, "mesh1.rpc.11", 2, &data_root).await;
            let _ = std::fs::remove_dir_all(&data_root);
            (a_identity, b_err, on_disk.spawned.into_inner().unwrap())
        });
        let x = a_identity["node_id"].as_str().unwrap().to_string();
        let deployment = a_identity["deployment_id"].as_str().unwrap().to_string();
        let settled = cap.spans_named("rdm.node_admin.node.resolve.via-claimed-allocation");
        assert_eq!(settled.iter().map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(x.clone(), "launched-exited".to_string())], "{settled:#?}");
        let b_err = b_err.expect_err("B's attempt fails where this provider launches nothing");
        assert!(b_err.contains("DeployRuntime") && !b_err.contains("name-held"), "B decides a new birth and reaches DeployRuntime: {b_err}");
        assert_eq!(spawned.len(), 1, "{spawned:?}");
        assert_ne!(spawned[0].0, deployment, "B launches a deployment of its own, never the exited birth's again");
        let claims = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let rpc11: Vec<(String, String)> = claims.iter().filter(|s| s["attributes"]["subject"] == "mesh1.rpc.11" && s["attributes"]["moment"] == "claim").map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
        assert!(rpc11.len() == 2 && rpc11[0] == (x.clone(), "row-written".to_string()) && rpc11[1].0 != x && rpc11[1].1 == "row-written", "A's claim, then a new birth's once A's is withdrawn: {rpc11:?}");
        eprintln!("trace {}: via-claimed-allocation node_id={x} outcome=launched-exited; claims {rpc11:?}", settled[0]["trace_id"].as_str().unwrap_or(""));
    }

    /// CONTRACT (lifecycles/node-create.md step 1: the deployment is recorded before its executable
    /// runs, so a process that runs always has its record; the lost-executor rule: a runtime launched
    /// by an executor that is not this one and never joined is stopped and the birth decided afresh):
    /// A claims `mesh1.rpc.12` in attempt 1, launches its birth through the process provider (its
    /// process runs and its data dir holds the launch record naming A's deployment, pid and start
    /// token) and is lost. B, holding none of A's receipts, asks the process provider, which finds the
    /// exact running process from the record: B stops that process through the handle the record
    /// names, withdraws that birth's claim (its key only), mints a new birth, claims the free name for
    /// it and reaches `DeployRuntime` with a deployment of its own. Must NOT happen: B reading A's
    /// running process as never launched and continuing the same birth over it.
    #[test]
    fn a_create_whose_lost_executor_launched_a_birth_whose_process_runs_stops_that_process_from_its_record_and_decides_a_new_birth() {
        let cap = capture();
        let (a_identity, record, b_err, spawned, after, handle) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-launched-running-{}", NodeId::mint()));
            let build = BuildId("build-node-rows-12".into());
            let a_identity = lost_after_claim(&storage, &build, "mesh1.rpc.12", &data_root).await;
            // A launched the birth: its process runs, and the provider's launch record names it.
            let dir = data_root.join(format!("mesh1.rpc.12-{}", a_identity["node_id"].as_str().unwrap()));
            let spec = ResolvedNodeLaunch {
                node: "mesh1.rpc.12".parse().unwrap(),
                deployment_id: DeploymentId(a_identity["deployment_id"].as_str().unwrap().to_string()),
                executable: "/bin/sleep".into(),
                args: vec!["300".into()],
                env: Default::default(),
                data_dir: dir.clone(),
                transport: "127.0.0.1:0".parse().unwrap(),
                listeners: vec![],
            };
            let launcher = rafka_node_admin_core::deployment::process::ProcessDeploymentProvider::new();
            let handle = launcher.spawn(&spec).await.expect("A launches its birth");
            let record: Value = serde_json::from_str(&std::fs::read_to_string(dir.join("deployment.json")).expect("the launch record")).unwrap();
            let b_builds = MemoryBuildStateAdapter::new();
            let accepted = rafka_node_admin_core::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
            rafka_node_admin_core::build_state::BuildStateAdapter::publish_accepted(&b_builds, &accepted).await.unwrap();
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let on_disk = OnDisk { inner: rafka_node_admin_core::deployment::process::ProcessDeploymentProvider::new(), spawned: Mutex::default() };
            let b_err = create_attempt_executing("/bin/sleep", &b_records, &on_disk, &Unheard, &b_builds, &build, "mesh1.rpc.12", 2, &data_root).await;
            // The process B stopped is gone to every provider of the domain.
            let after = on_disk.inner.find(&spec).await;
            let _ = std::fs::remove_dir_all(&data_root);
            (a_identity, record, b_err, on_disk.spawned.into_inner().unwrap(), after, handle)
        });
        let x = a_identity["node_id"].as_str().unwrap().to_string();
        let deployment = a_identity["deployment_id"].as_str().unwrap().to_string();
        assert_eq!(record["deployment_id"], deployment.as_str(), "the launch record names A's deployment");
        assert_eq!(record["pid"], handle.pid.unwrap(), "and the pid A launched");
        assert!(record["start"].is_u64(), "and its start token: {record}");
        let settled = cap.spans_named("rdm.node_admin.node.resolve.via-claimed-allocation");
        assert_eq!(settled.iter().map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(x.clone(), "launched-by-lost-executor".to_string())], "{settled:#?}");
        let b_err = b_err.expect_err("B's attempt fails where this provider launches nothing");
        assert!(b_err.contains("DeployRuntime") && !b_err.contains("name-held"), "B decides a new birth and reaches DeployRuntime: {b_err}");
        assert_eq!(spawned.len(), 1, "{spawned:?}");
        assert_ne!(spawned[0].0, deployment, "B launches a deployment of its own, never the lost executor's");
        assert_eq!(after.unwrap(), rafka_node_admin_core::deployment::provider::Found::Exited, "A's process was stopped through its record");
        let stopped = cap.spans_named("rdm.node_admin.deployment.reject.via-lost-executor");
        assert_eq!(stopped.iter().map(|s| s["attributes"]["deployment_id"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>(), vec![deployment.clone()], "{stopped:#?}");
        eprintln!("trace {}: via-claimed-allocation node_id={x} outcome=launched-by-lost-executor; via-lost-executor deployment_id={deployment} pid={}", settled[0]["trace_id"].as_str().unwrap_or(""), handle.pid.unwrap());
    }

    /// CONTRACT (the lost-executor rule stops only a birth that never joined; a recoverable refusal
    /// over a terminated member): A claims `mesh1.rpc.10` in attempt 1 and is lost; the provider holds
    /// a running runtime of the exact deployment A's claim names, and that birth has joined fabric
    /// membership. B, holding none of A's receipts past the claim, refuses by name at
    /// `AllocateIdentity`: nothing is stopped, the claim is not withdrawn, nothing is minted or
    /// launched, and A's birth holds the row. Must NOT happen: B stopping a joined member on `find`
    /// alone.
    #[test]
    fn a_create_whose_lost_executor_launched_a_birth_that_joined_is_refused_by_name_and_stops_nothing() {
        let cap = capture();
        let (a_identity, b_err, terminated, spawned, row) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-launched-joined-{}", NodeId::mint()));
            let build = BuildId("build-node-rows-10".into());
            let a_identity = lost_after_claim(&storage, &build, "mesh1.rpc.10", &data_root).await;
            let b_builds = MemoryBuildStateAdapter::new();
            let accepted = rafka_node_admin_core::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
            rafka_node_admin_core::build_state::BuildStateAdapter::publish_accepted(&b_builds, &accepted).await.unwrap();
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let provider = LaunchedByTheLost::default();
            let b_err = create_attempt_over(&b_records, &provider, &Joined, &b_builds, &build, "mesh1.rpc.10", 2, &data_root).await;
            let row = storage.node_row_at(&"mesh1.rpc.10".parse().unwrap()).await.unwrap();
            let _ = std::fs::remove_dir_all(&data_root);
            (a_identity, b_err, provider.terminated.into_inner().unwrap(), provider.spawned.into_inner().unwrap(), row)
        });
        let x = a_identity["node_id"].as_str().unwrap().to_string();
        let b_err = b_err.expect_err("B's attempt is refused");
        assert!(b_err.contains("AllocateIdentity") && b_err.contains("joined fabric membership") && b_err.contains(&x), "refused by name at AllocateIdentity, naming the joined birth: {b_err}");
        assert!(terminated.is_empty(), "nothing is stopped: {terminated:?}");
        assert!(spawned.is_empty(), "nothing is launched: {spawned:?}");
        assert_eq!(row.map(|r| r.node_id.to_string()), Some(x.clone()), "A's birth holds the row: its claim is not withdrawn");
        let settled = cap.spans_named("rdm.node_admin.node.resolve.via-claimed-allocation");
        assert_eq!(settled.iter().map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(x.clone(), "launched-and-joined".to_string())], "{settled:#?}");
        assert!(cap.spans_named("rdm.node_admin.node.remove.via-claim-withdrawn").is_empty(), "no claim is withdrawn");
        eprintln!("trace {}: via-claimed-allocation node_id={x} outcome=launched-and-joined", settled[0]["trace_id"].as_str().unwrap_or(""));
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
    storage.put_node_row(&NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: exited.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false }).await.unwrap().unwrap();
    let born = NodeId::mint();
    if new_claimed_first {
        mp.rows.claim(&exited.name, &born, create, None).await.expect_err("the exited node's live row holds the name");
    }
    mp.rows.deleted(&exited, delete).await.expect("the executor writes the Deleted row inside NodeDeleted");
    mp.rows.claim(&exited.name, &born, create, None).await.expect("the Deleted row holds no name");
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
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Pending, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false }), "the new node holds the row, seeded from the exited node's");
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
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Pending, config: tier("gold"), tags: Default::default(), allocated: None, otel_debug: false }), "the new node holds the row once the name is free");
    let created = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    assert_eq!(created.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(born.as_str()), Some("name-held")), (Some(born.as_str()), Some("row-seeded"))], "{created:#?}");
    assert_eq!(created[1]["attributes"]["seeded_from"].as_str(), Some(exited.as_str()));
}
