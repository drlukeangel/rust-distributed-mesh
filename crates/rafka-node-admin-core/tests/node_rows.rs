//! The node row the fabric-primary writes (ops-naming "Node data: meta, config and exe"; the delete
//! point; lifecycles/node-decommission.md step 3): the drive of a Build is the product's, the
//! executor is a fixture that streams the frames a real run streams.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.admin.1`, its mesh primary
//! `mesh1.admin.2` (the executor of every `mesh1` node) and `mesh1.rpc.1`, which carries config.

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
                json!({"name": s.name, "attributes": attributes})
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
    records: Arc<Records>,
    build: BuildId,
}

/// A view change as the product makes one: the projection installed through
/// `Records::install_projected`, the one door every view change of an admin goes through.
async fn install(records: &Records, view: &RwLock<Topology>, f: impl FnOnce(&mut Topology)) {
    let mut t = view.read().await.clone();
    f(&mut t);
    records.install_projected(view, || t).await;
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
    let rows = Arc::new(NodeRows::new(me.clone(), me_id.clone(), storage.clone()));
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
    Rig { env, storage, records, build: opened.build_id }
}

fn gone(name: &'static str) -> Act {
    Act::View(Box::new(move |v: &mut Topology| v.nodes.retain(|n| n.name.to_string() != name)))
}

/// CONTRACT: `node.delete` leaves the node's row at its `path.name` with status `Deleted`, its
/// node id and its config kept (ops-naming, the delete point; node-decommission.md step 3). The
/// fabric-primary writes it at the executor's `NodeDeleted` from the record it held when the
/// removal was announced (`NodeDeleting`), so a departure that left the view before `NodeDeleted`
/// reached it loses nothing. One `via-node-row` span names the row written.
#[test]
fn a_deleted_node_leaves_a_deleted_row_with_its_record_and_config() {
    let cap = capture();
    let (row, victim) = cap.run(async {
        let op = "retire-node:mesh1.rpc.1";
        let acts = vec![Act::Frame(op, "NodeDeleting"), Act::Frame(op, "AwaitNodeLeft"), gone("mesh1.rpc.1"), Act::Frame(op, "NodeDeleted"), Act::Frame(op, "Complete")];
        let r = rig(TopologyChange::RemoveNode { node: "mesh1.rpc.1".parse().unwrap() }, acts, Arc::new(MemoryNodesStorage::default())).await;
        let victim = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").unwrap().node_id.clone();
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
        (r.storage.node_row_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap(), victim)
    });
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: victim.clone(), status: NodeStatus::Deleted, config: tier("gold") }), "the row says Deleted and keeps the record and the config");
    let spans = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["subject"].as_str(), a["node_id"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some("mesh1.rpc.1"), Some(victim.as_str()), Some("1"), Some("row-deleted")), "{a:#?}");
}

/// CONTRACT: a node created at a `path.name` seeds its config from the previous row there (a
/// `Deleted` row): config belongs to the role, not the process (ops-naming "Node data"). When the
/// fabric-primary's own view first holds the new birth it claims the birth's row at the
/// `path.name` from that held record: the new node id and its status, the config the `Deleted` row
/// kept. One `via-node-row` span names it.
#[test]
fn a_node_created_at_a_deleted_rows_name_carries_its_config() {
    let cap = capture();
    let (row, born, old) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let old = NodeId::mint();
        storage.put_node_row(&NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: old.clone(), status: NodeStatus::Deleted, config: tier("silver") }).await.unwrap();
        let birth = node("mesh1.rpc.2", false, false);
        let born = birth.node_id.clone();
        let op = "create-node:mesh1.rpc.2";
        let heard = Act::View(Box::new(move |v: &mut Topology| v.nodes.push(birth)));
        let acts = vec![Act::Frame(op, "AllocateIdentity"), heard, Act::Frame(op, "WaitForNodeReady"), Act::Frame(op, "Complete")];
        let r = rig(TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, acts, storage).await;
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
        (r.storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, old)
    });
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: born.clone(), status: NodeStatus::ReadyForTraffic, config: tier("silver") }), "the new birth holds the row and the Deleted row's config");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["subject"].as_str(), a["node_id"].as_str(), a["seeded_from"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some("mesh1.rpc.2"), Some(born.as_str()), Some(old.as_str()), Some("1"), Some("row-seeded")), "{a:#?}");
}

/// CONTRACT: the fabric-primary writes a new birth's row from the record it holds (ops-naming "Node
/// data": "writes the whole row from that held record"), so the row follows its own view, never the
/// executor's `Complete`: a birth that enters the view only after the create completed (a peer
/// mesh's birth reaches the fabric-primary through the forwarded aggregate, after the create's
/// last frame) still claims its row, seeded from the `Deleted` row at its `path.name`. One
/// `via-node-row` span, at the view install, names it.
#[test]
fn a_birth_the_view_holds_only_after_the_create_completed_claims_its_row_seeded() {
    let cap = capture();
    let (row, born, old) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let old = NodeId::mint();
        storage.put_node_row(&NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: old.clone(), status: NodeStatus::Deleted, config: tier("silver") }).await.unwrap();
        let birth = node("mesh1.rpc.2", false, false);
        let born = birth.node_id.clone();
        let op = "create-node:mesh1.rpc.2";
        let acts = vec![Act::Frame(op, "AllocateIdentity"), Act::Frame(op, "WaitForNodeReady"), Act::Frame(op, "Complete")];
        let r = rig(TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, acts, storage).await;
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
        install(&r.records, &r.env.topology, move |v| v.nodes.push(birth)).await;
        (r.storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, old)
    });
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: born.clone(), status: NodeStatus::ReadyForTraffic, config: tier("silver") }), "the birth holds the row and the Deleted row's config");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let rpc2: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["subject"] == "mesh1.rpc.2").collect();
    assert_eq!(rpc2.len(), 1, "{spans:#?}");
    let a = &rpc2[0]["attributes"];
    assert_eq!((a["node_id"].as_str(), a["seeded_from"].as_str(), a["outcome"].as_str()), (Some(born.as_str()), Some(old.as_str()), Some("row-seeded")), "{a:#?}");
}

/// CONTRACT: the store is read only to rebuild after a seat handover (ops-naming "Node data"), and
/// that rebuild never overwrites a row a node already holds: an admin that takes the
/// fabric-primary seat claims the row of every member its view holds, and the row of a member
/// that already holds its name stays exactly as written (a config the node has not applied yet is
/// not wiped by the new holder's view). A member with no row claims one. One `via-node-row` span
/// per member: `row-held-by-node` for the held row, `row-claimed` for the new one.
#[test]
fn a_seat_taken_rebuilds_the_rows_and_never_overwrites_a_row_its_node_holds() {
    let cap = capture();
    let (row, written) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let records = Records::default();
        let mut before = view();
        records.set_rows(Arc::new(NodeRows::new(me.clone(), before.node(&me).expect("the admin is in the view").node_id.clone(), storage.clone())));
        for n in before.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh1.admin.2";
        }
        let topology = RwLock::new(before);
        let rpc1 = view().nodes.into_iter().find(|n| n.name.to_string() == "mesh1.rpc.1").unwrap();
        let written = NodeRow { name: rpc1.name.clone(), node_id: rpc1.node_id.clone(), status: NodeStatus::Draining, config: tier("platinum") };
        storage.put_node_row(&written).await.unwrap();
        let after = {
            let mut t = topology.read().await.clone();
            for n in t.nodes.iter_mut() {
                n.is_fabric_primary = n.name == me;
                if n.name == rpc1.name {
                    n.node_id = rpc1.node_id.clone();
                }
            }
            t
        };
        records.install_projected(&topology, || after).await;
        (storage.node_row_at(&rpc1.name).await.unwrap(), written)
    });
    assert_eq!(row, Some(written), "the row the node holds is untouched by the new holder's rebuild");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let outcome = |name: &str| spans.iter().filter(|s| s["attributes"]["subject"] == name).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
    assert_eq!(outcome("mesh1.rpc.1"), vec!["row-held-by-node"], "{spans:#?}");
    assert_eq!(outcome("mesh1.admin.2"), vec!["row-claimed"], "{spans:#?}");
}

type Rebuilt = (BTreeMap<String, Option<NodeRow>>, Option<NodeStatus>, NodeId, NodeId, NodeId, NodeId, String);

/// The rebuild cell's story: P's writes, then N's seat taken over one shared root.
async fn rebuild_over_one_root(trace_id: String) -> Rebuilt {
        let d = std::env::temp_dir().join(format!("node-rows-shared-root-{}", NodeId::mint()));
        let root = rafka_node_admin_core::store_root::StoreRoot::at(&d.join("store"));
        let open = |data: &str| -> Arc<dyn NodesStorage> {
            Arc::new(rafka_node_admin_core::storage::FileNodesStorage::open(&d.join(data), &root, "mesh1", &IncarnationId::mint()).unwrap())
        };
        let (p_store, n_store) = (open("mesh1.admin.2"), open("mesh1.admin.1"));
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
            p_store.put_node_row(&row).await.unwrap();
        }
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let records = Records::default();
        records.set_rows(Arc::new(NodeRows::new(me.clone(), before.node(&me).unwrap().node_id.clone(), n_store.clone())));
        let topology = RwLock::new(before);
        let (successor, fresh) = (node("mesh1.rpc.2", false, false), node("mesh1.rpc.3", false, false));
        let (succ, fresh_id) = (successor.node_id.clone(), fresh.node_id.clone());
        let after = {
            let mut t = topology.read().await.clone();
            for n in t.nodes.iter_mut() {
                n.is_fabric_primary = n.name == me;
            }
            t.nodes.push(successor);
            t.nodes.push(fresh);
            t
        };
        records.install_projected(&topology, || after).await;
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

/// CONTRACT: a claim of a name the same node already holds writes nothing (CLAUDE.md: the claim is
/// insert-and-fail on the key): it is satisfied, and the row stays exactly as it was, in every
/// nodes store.
#[test]
fn a_claim_of_a_name_its_own_node_holds_writes_nothing() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("node-rows-claim-{}", NodeId::mint()));
        let stores: Vec<(&str, Arc<dyn NodesStorage>)> = vec![("memory", Arc::new(MemoryNodesStorage::default())), ("file", Arc::new(rafka_node_admin_core::storage::FileNodesStorage::open(&dir, &rafka_node_admin_core::store_root::StoreRoot::at(&dir.join("store")), "mesh1", &rafka_node_admin_core::model::IncarnationId("test".into())).unwrap()))];
        for (kind, store) in stores {
            let id = NodeId::mint();
            let held = NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: id.clone(), status: NodeStatus::Draining, config: tier("platinum") };
            store.put_node_row(&held).await.unwrap();
            let again = NodeRow { status: NodeStatus::ReadyForTraffic, config: tier("gold"), ..held.clone() };
            assert!(matches!(store.claim_node_row(&again).await, Ok(Ok(_))), "{kind}: the node's own name is never refused");
            assert_eq!(store.node_row_at(&held.name).await.unwrap(), Some(held.clone()), "{kind}: the claim wrote nothing over the node's own row");
            assert!(!store.release_node_row(&held.name, &NodeId::mint()).await.unwrap(), "{kind}: another node lets go of nothing here");
            assert_eq!(store.node_row_at(&held.name).await.unwrap(), Some(held.clone()), "{kind}: a release by another node leaves the row");
            assert!(store.release_node_row(&held.name, &id).await.unwrap(), "{kind}: the holder lets go of its key");
            assert_eq!(store.node_row_at(&held.name).await.unwrap(), None, "{kind}: the holder's release frees the key");
        }
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// CONTRACT: a replace moves the predecessor's row to `<path.name>.old` and its departure leaves a
/// `Deleted` row there, and the successor created at the `path.name` seeds its config from the
/// `.old` row (ops-naming "Node data": "a Deleted row, or the `<name>.old` row"; node-rpc-envelope.md
/// "Meta and config": name may change only to/from name.old, insert-and-fail). At the executor's
/// `RenamePredecessor` the fabric-primary claims `<path>.old` for the predecessor from the record it
/// held at `NodeDeleting` and releases `<path>`; at `NodeDeleted` it writes the `.old` row `Deleted`,
/// its config kept; the successor entering its view claims `<path>` seeded from the `.old` row.
#[test]
fn a_replaced_nodes_row_moves_to_old_and_its_successor_is_seeded_from_it() {
    let cap = capture();
    let (old_row, row, pred, succ) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let successor = node("mesh1.rpc.1", false, false);
        let succ = successor.node_id.clone();
        let op = "replace-node:mesh1.rpc.1";
        let renamed = Act::View(Box::new(|v: &mut Topology| {
            for n in v.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh1.rpc.1") {
                n.name = n.name.renamed();
            }
        }));
        let heard = Act::View(Box::new(move |v: &mut Topology| v.nodes.push(successor)));
        let acts = vec![
            Act::Frame(op, "NodeDeleting"),
            Act::Frame(op, "DrainNode"),
            Act::Frame(op, "RenamePredecessor"),
            renamed,
            Act::Frame(op, "AllocateIdentity"),
            heard,
            Act::Frame(op, "WaitForNodeReady"),
            Act::Frame(op, "NodeDeleted"),
            gone("mesh1.rpc.1.old"),
            Act::Frame(op, "Complete"),
        ];
        let r = rig(TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, acts, storage).await;
        let pred = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        r.storage.put_node_row(&NodeRow { name: pred.name.clone(), node_id: pred.node_id.clone(), status: pred.status, config: pred.config.clone() }).await.unwrap();
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
        (r.storage.node_row_at(&pred.name.renamed()).await.unwrap(), r.storage.node_row_at(&pred.name).await.unwrap(), pred.node_id.clone(), succ)
    });
    assert_eq!(old_row, Some(NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), node_id: pred.clone(), status: NodeStatus::Deleted, config: tier("gold") }), "the predecessor's row moved to .old and says Deleted, its config kept");
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: succ.clone(), status: NodeStatus::ReadyForTraffic, config: tier("gold") }), "the successor holds the path.name with the .old row's config");
    let renames = cap.spans_named("rdm.node_admin.node.update.via-rename");
    assert_eq!(renames.len(), 1, "{renames:#?}");
    let a = &renames[0]["attributes"];
    assert_eq!((a["from"].as_str(), a["to"].as_str(), a["outcome"].as_str()), (Some("mesh1.rpc.1"), Some("mesh1.rpc.1.old"), Some("renamed")), "{a:#?}");
    let deleted = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(deleted.len(), 1, "{deleted:#?}");
    let a = &deleted[0]["attributes"];
    assert_eq!((a["subject"].as_str(), a["node_id"].as_str(), a["outcome"].as_str()), (Some("mesh1.rpc.1.old"), Some(pred.as_str()), Some("row-deleted")), "{a:#?}");
    let seeded: Vec<Value> = cap.spans_named("rdm.node_admin.node.create.via-node-row").into_iter().filter(|s| s["attributes"]["node_id"] == succ.as_str()).collect();
    assert_eq!(seeded.len(), 1, "{seeded:#?}");
    let a = &seeded[0]["attributes"];
    assert_eq!((a["subject"].as_str(), a["seeded_from"].as_str(), a["outcome"].as_str()), (Some("mesh1.rpc.1"), Some(pred.as_str()), Some("row-seeded")), "{a:#?}");
}

/// CONTRACT: in a replace, the predecessor lets go of the `path.name` before the successor takes it
/// over (ops-naming "The name is claimed by insert-and-fail": "The new node can take the name over
/// only after the old node lets go, by being renamed to `<name>.old` or deleted"), and that order is
/// the code's, never the arrival order of two channels. Here the successor enters the
/// fabric-primary's view (a peer of the executor: its view takes the successor at the path.name, the
/// predecessor never renamed in it) BEFORE the executor's `RenamePredecessor` frame is handled. Its
/// claim at the view install meets the predecessor's row: it waits on the replace that holds the
/// name (`awaits-rename`), and the `RenamePredecessor` move, the rename's one writer, moves the
/// predecessor's row to `.old` and then claims the path.name for the successor the view holds,
/// seeded from the `.old` row. The end rows are the ones the in-order replace leaves.
#[test]
fn a_successor_heard_before_the_predecessor_lets_go_takes_the_name_at_the_rename() {
    let cap = capture();
    let (old_row, row, pred, succ) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let successor = node("mesh1.rpc.1", false, false);
        let succ = successor.node_id.clone();
        let op = "replace-node:mesh1.rpc.1";
        // Newest birth wins the path.name in a peer's view: the successor takes it.
        let heard = Act::View(Box::new(move |v: &mut Topology| {
            v.nodes.retain(|n| n.name.to_string() != "mesh1.rpc.1");
            v.nodes.push(successor);
        }));
        let acts = vec![
            Act::Frame(op, "NodeDeleting"),
            Act::Frame(op, "DrainNode"),
            heard,
            Act::Frame(op, "RenamePredecessor"),
            Act::Frame(op, "AllocateIdentity"),
            Act::Frame(op, "WaitForNodeReady"),
            Act::Frame(op, "NodeDeleted"),
            Act::Frame(op, "Complete"),
        ];
        let r = rig(TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, acts, storage).await;
        let pred = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        r.storage.put_node_row(&NodeRow { name: pred.name.clone(), node_id: pred.node_id.clone(), status: pred.status, config: pred.config.clone() }).await.unwrap();
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
        (r.storage.node_row_at(&pred.name.renamed()).await.unwrap(), r.storage.node_row_at(&pred.name).await.unwrap(), pred.node_id.clone(), succ)
    });
    assert_eq!(old_row, Some(NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), node_id: pred.clone(), status: NodeStatus::Deleted, config: tier("gold") }), "the predecessor's row moved to .old and says Deleted, its config kept");
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: succ.clone(), status: NodeStatus::ReadyForTraffic, config: tier("gold") }), "the successor holds the path.name with the .old row's config");
    let renames = cap.spans_named("rdm.node_admin.node.update.via-rename");
    assert_eq!(renames.len(), 1, "{renames:#?}");
    let a = &renames[0]["attributes"];
    assert_eq!((a["from"].as_str(), a["to"].as_str(), a["outcome"].as_str()), (Some("mesh1.rpc.1"), Some("mesh1.rpc.1.old"), Some("renamed")), "{a:#?}");
    let claims: Vec<Value> = cap.spans_named("rdm.node_admin.node.create.via-node-row").into_iter().filter(|s| s["attributes"]["node_id"] == succ.as_str()).collect();
    let outcomes: Vec<&str> = claims.iter().map(|s| s["attributes"]["outcome"].as_str().unwrap_or("")).collect();
    assert_eq!(outcomes, vec!["awaits-rename", "row-seeded"], "{claims:#?}");
    let a = &claims[1]["attributes"];
    assert_eq!((a["subject"].as_str(), a["seeded_from"].as_str()), (Some("mesh1.rpc.1"), Some(pred.as_str())), "{a:#?}");
    assert!(claims[0]["attributes"]["detail"].as_str().is_some_and(|d| d.contains(pred.as_str()) && d.contains("replace-node:mesh1.rpc.1")), "{:#?}", claims[0]);
}

/// CONTRACT: a fabric-primary that took the seat after the replace's `NodeDeleting` (it holds no
/// announced record) still moves the predecessor's row at `RenamePredecessor`, from the record its
/// view holds now. In the executor's own view the predecessor is already named `<path>.old` and
/// nothing holds `<path>`: the move reads the predecessor there, never only at `<path>`.
#[test]
fn a_rename_the_seat_holder_never_saw_announced_moves_the_row_from_the_old_name_its_view_holds() {
    let cap = capture();
    let (old_row, row, pred) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let op = "replace-node:mesh1.rpc.1";
        let renamed = Act::View(Box::new(|v: &mut Topology| {
            for n in v.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh1.rpc.1") {
                n.name = n.name.renamed();
            }
        }));
        let acts = vec![renamed, Act::Frame(op, "RenamePredecessor"), Act::Frame(op, "Complete")];
        let r = rig(TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, acts, storage).await;
        let pred = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        r.storage.put_node_row(&NodeRow { name: pred.name.clone(), node_id: pred.node_id.clone(), status: pred.status, config: pred.config.clone() }).await.unwrap();
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
        (r.storage.node_row_at(&pred.name.renamed()).await.unwrap(), r.storage.node_row_at(&pred.name).await.unwrap(), pred.node_id.clone())
    });
    assert_eq!(old_row, Some(NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), node_id: pred.clone(), status: NodeStatus::ReadyForTraffic, config: tier("gold") }), "the predecessor's row moved to .old");
    assert_eq!(row, None, "the path.name is let go");
    let renames = cap.spans_named("rdm.node_admin.node.update.via-rename");
    assert_eq!(renames.len(), 1, "{renames:#?}");
    let a = &renames[0]["attributes"];
    assert_eq!((a["node_id"].as_str(), a["outcome"].as_str()), (Some(pred.as_str()), Some("renamed")), "{a:#?}");
}

/// CONTRACT: the fabric-primary claims a birth's row when its view holds the birth for the first
/// time in its seat (ops-naming "Node data": the row is written once, from the held record), never
/// again on a later view install. A birth that left the view (a departure the projection dropped)
/// and is put back by a later backbone copy of a peer mesh's view is
/// the same birth, not a new arrival: one `via-node-row` span for it, the row written once.
#[test]
fn a_birth_that_leaves_the_view_and_returns_is_claimed_once() {
    let cap = capture();
    let (row, born) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let records = Records::default();
        let t = view();
        records.set_rows(Arc::new(NodeRows::new(me.clone(), t.node(&me).expect("the fabric-primary is in the view").node_id.clone(), storage.clone())));
        let topology = RwLock::new(t);
        let birth = node("mesh1.rpc.2", false, false);
        let born = birth.node_id.clone();
        let again = birth.clone();
        install(&records, &topology, move |v| v.nodes.push(birth)).await;
        install(&records, &topology, |v| v.nodes.retain(|n| n.name.to_string() != "mesh1.rpc.2")).await;
        install(&records, &topology, move |v| v.nodes.push(again)).await;
        (storage.node_row_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born)
    });
    assert_eq!(row.map(|r| r.node_id), Some(born.clone()), "the birth holds its row");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let claims: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["node_id"] == born.as_str()).collect();
    assert_eq!(claims.len(), 1, "one claim for one birth in one seat tenure: {claims:#?}");
    assert_eq!(claims[0]["attributes"]["outcome"], "row-claimed", "{claims:#?}");
}

/// The receipts an earlier attempt of the replace of `mesh1.rpc.1` wrote (`mesh1.admin.2` ran it up
/// to `RenamePredecessor` and was cut off), and the claim of the next attempt by the same executor:
/// the Build is running at attempt 2 when this fabric-primary's drive first reads it.
/// `NodeDeleting`'s receipt carries the lifecycle op that names the predecessor (`pred`), as the
/// replace pipeline writes it.
async fn earlier_attempt_renamed(r: &Rig, pred: &Node) {
    use rafka_node_admin_core::build_state::{BuildAttemptClaim, BuildStepReceipt, StepOutcome};
    let claim = |attempt| BuildAttemptClaim { build_id: r.build.clone(), attempt, executor: "mesh1.admin.2".into() };
    r.env.builds.claim_attempt(&claim(1)).await.unwrap();
    let operation = "replace-node:mesh1.rpc.1".to_string();
    let op = rafka_mesh_entity::LifecycleOp { build_id: r.build.0.clone(), attempt: 1, operation: operation.clone(), node_id: pred.node_id.clone(), incarnation: pred.incarnation_id.clone().unwrap(), name: pred.name.clone(), event_at_rafka_ms: 1 };
    for step in ["NodeDeleting", "DrainNode", "RenamePredecessor"] {
        let output = (step == "NodeDeleting").then(|| serde_json::to_value(&op).unwrap());
        let receipt = BuildStepReceipt { build_id: r.build.clone(), attempt: 1, operation: operation.clone(), step: step.into(), outcome: StepOutcome::Complete, output, executor: Some("mesh1.admin.2".into()) };
        r.env.builds.append_step_receipt(&receipt).await.unwrap();
    }
    r.env.builds.claim_attempt(&claim(2)).await.unwrap();
}

/// CONTRACT: a fabric-primary that takes the seat mid-replace moves the predecessor's row at the
/// replace's `RenamePredecessor` even though it reads that step only as a receipt of an earlier
/// attempt (the next attempt reuses the step and sends no frame for it): a completed step's receipt
/// is the same event as its live frame, and the row writer handles it once. The predecessor's row
/// moves to `<path>.old` and ends `Deleted`, and the successor takes the `path.name` seeded from it,
/// never refused by the predecessor's stale row.
#[test]
fn a_seat_taken_mid_replace_moves_the_row_from_the_rename_receipt() {
    let cap = capture();
    let (old_row, row, pred, succ) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let successor = node("mesh1.rpc.1", false, false);
        let succ = successor.node_id.clone();
        let op = "replace-node:mesh1.rpc.1";
        let heard = Act::View(Box::new(move |v: &mut Topology| {
            v.nodes.retain(|n| n.name.to_string() != "mesh1.rpc.1");
            v.nodes.push(successor);
        }));
        let acts = vec![Act::Frame(op, "AllocateIdentity"), heard, Act::Frame(op, "WaitForNodeReady"), Act::Frame(op, "NodeDeleted"), Act::Frame(op, "Complete")];
        let r = rig(TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, acts, storage).await;
        let pred = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        r.storage.put_node_row(&NodeRow { name: pred.name.clone(), node_id: pred.node_id.clone(), status: pred.status, config: pred.config.clone() }).await.unwrap();
        earlier_attempt_renamed(&r, &pred).await;
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
        (r.storage.node_row_at(&pred.name.renamed()).await.unwrap(), r.storage.node_row_at(&pred.name).await.unwrap(), pred.node_id.clone(), succ)
    });
    assert_eq!(old_row, Some(NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), node_id: pred.clone(), status: NodeStatus::Deleted, config: tier("gold") }), "the predecessor's row moved to .old at the receipt and says Deleted");
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: succ.clone(), status: NodeStatus::ReadyForTraffic, config: tier("gold") }), "the successor holds the path.name, seeded from the .old row");
    let renames = cap.spans_named("rdm.node_admin.node.update.via-rename");
    let outcomes: Vec<&str> = renames.iter().map(|s| s["attributes"]["outcome"].as_str().unwrap_or("")).collect();
    assert_eq!(outcomes, vec!["renamed"], "{renames:#?}");
    let deleted = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(deleted.len(), 1, "{deleted:#?}");
    assert_eq!((deleted[0]["attributes"]["subject"].as_str(), deleted[0]["attributes"]["held_from"].as_str()), (Some("mesh1.rpc.1.old"), Some("node-deleting")), "{deleted:#?}");
}

/// CONTRACT: the rename is the predecessor letting go of its own key, never anyone else's: a
/// `RenamePredecessor` handled again by a fabric-primary that took the seat after the move (the
/// predecessor's row at `<path>.old` already, the successor holding `<path>` with a config of its
/// own) leaves both rows exactly as they are. The successor's row is not released and rewritten.
#[test]
fn a_rename_handled_again_after_the_successor_took_the_name_leaves_its_row() {
    let cap = capture();
    let (old_row, row, held_old, held) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let successor = node("mesh1.rpc.1", false, false);
        let r = rig(TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, vec![Act::Frame("replace-node:mesh1.rpc.1", "Complete")], storage).await;
        let pred = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        // The executor's view: the predecessor under its .old path, the successor at the path.name.
        install(&r.records, &r.env.topology, {
            let successor = successor.clone();
            move |v| {
                for n in v.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh1.rpc.1") {
                    n.name = n.name.renamed();
                }
                v.nodes.push(successor);
            }
        })
        .await;
        let held_old = NodeRow { name: pred.name.renamed(), node_id: pred.node_id.clone(), status: pred.status, config: tier("gold") };
        let held = NodeRow { name: pred.name.clone(), node_id: successor.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: tier("platinum") };
        r.storage.put_node_row(&held_old).await.unwrap();
        r.storage.put_node_row(&held).await.unwrap();
        earlier_attempt_renamed(&r, &pred).await;
        assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
        (r.storage.node_row_at(&pred.name.renamed()).await.unwrap(), r.storage.node_row_at(&pred.name).await.unwrap(), held_old, held)
    });
    assert_eq!(old_row, Some(held_old), "the predecessor's .old row stands");
    assert_eq!(row, Some(held), "the successor's row, its own config, stands: the rename released no key the predecessor does not hold");
    let renames = cap.spans_named("rdm.node_admin.node.update.via-rename");
    let outcomes: Vec<&str> = renames.iter().map(|s| s["attributes"]["outcome"].as_str().unwrap_or("")).collect();
    assert_eq!(outcomes, vec!["already-moved"], "the rename is the predecessor's, read by its node id from the NodeDeleting receipt, and it was made: {renames:#?}");
}

/// The recovery of `mesh1.rpc.1` (node-recovery.md steps 3-4): the exited birth's `NodeDeleting`,
/// its departure, its `NodeDeleted`, and the new node at the same `path.name` heard `before` or
/// after the `NodeDeleted` frame reaches the fabric-primary. Returns the row at `mesh1.rpc.1`, the
/// exited node and the new one.
async fn recovered(new_heard_first: bool) -> (Option<NodeRow>, NodeId, NodeId) {
    let op = "recover-node:mesh1.rpc.1";
    let birth = node("mesh1.rpc.1", false, false);
    let born = birth.node_id.clone();
    let heard = Act::View(Box::new(move |v: &mut Topology| v.nodes.push(birth)));
    let mut acts = vec![Act::Frame(op, "NodeDeleting"), gone("mesh1.rpc.1")];
    if new_heard_first {
        acts.extend([heard, Act::Frame(op, "NodeDeleted")]);
    } else {
        acts.extend([Act::Frame(op, "NodeDeleted"), heard]);
    }
    acts.extend([Act::Frame(op, "WaitForNodeReady"), Act::Frame(op, "Complete")]);
    let storage = Arc::new(MemoryNodesStorage::default());
    let r = rig(TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, acts, storage).await;
    let exited = r.env.topology.read().await.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").unwrap().node_id.clone();
    // The exited birth's row, as its create claimed it.
    r.storage.put_node_row(&NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: exited.clone(), status: NodeStatus::ReadyForTraffic, config: tier("gold") }).await.unwrap();
    assert_eq!(r.env.run(&Drive::detached(r.build.clone())).await, DriveEnd::Terminal);
    (r.storage.node_row_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap(), exited, born)
}

/// CONTRACT (node-recovery.md step 3: "the node's mesh-primary writes the row with status Deleted";
/// step 4: "Its config is seeded from the Deleted row at that path.name"): a recovery's
/// `NodeDeleted` leaves the exited node's row `Deleted`, so the new node created at the same
/// `path.name` claims it, seeded from it: the new node id, its status, the exited node's config.
/// One `delete.via-node-row` span (`row-deleted`) and one `create.via-node-row` span
/// (`row-seeded`, seeded from the exited node) name the two writes.
#[test]
fn a_recovered_nodes_row_is_deleted_and_the_new_node_at_its_path_name_claims_it_seeded() {
    let cap = capture();
    let (row, exited, born) = cap.run(recovered(false));
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: born.clone(), status: NodeStatus::ReadyForTraffic, config: tier("gold") }), "the new node holds the row, seeded from the exited node's");
    let deleted = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(deleted.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(exited.as_str()), Some("row-deleted"))], "{deleted:#?}");
    let created = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    assert_eq!(created.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["seeded_from"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(born.as_str()), Some(exited.as_str()), Some("row-seeded"))], "{created:#?}");
}

/// CONTRACT: the new node of a recovery that enters the fabric-primary's view before the
/// recovery's `NodeDeleted` reaches it finds the exited node's live row at its `path.name` (its
/// claim is refused, `name-held`); the `NodeDeleted` that frees the name hands it to the birth the
/// view holds there, through the one claim every birth takes, seeded from the `Deleted` row.
#[test]
fn a_recovered_node_heard_before_the_exited_ones_node_deleted_takes_the_name_at_it() {
    let cap = capture();
    let (row, exited, born) = cap.run(recovered(true));
    assert_eq!(row, Some(NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: born.clone(), status: NodeStatus::ReadyForTraffic, config: tier("gold") }), "the new node holds the row once the name is free");
    let created = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    assert_eq!(created.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(born.as_str()), Some("name-held")), (Some(born.as_str()), Some("row-seeded"))], "{created:#?}");
    assert_eq!(created[1]["attributes"]["seeded_from"].as_str(), Some(exited.as_str()));
}
