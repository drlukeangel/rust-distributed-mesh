//! The node row the fabric-primary writes (ops-naming "Node data: meta, config and exe"; the delete
//! point; lifecycles/node-decommission.md step 3): the drive of a Build is the product's, the
//! executor is a fixture that streams the frames a real run streams.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.admin.1`, its mesh primary
//! `mesh1.admin.2` (the executor of every `mesh1` node) and `mesh1.rpc.1`, which carries config.

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

struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("node-rows"));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
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
        let _ = self.provider.force_flush();
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
}

struct Played {
    acts: VecDeque<Act>,
    view: Arc<RwLock<Topology>>,
    build_id: String,
    attempt: u32,
    done: bool,
}

#[async_trait::async_trait]
impl InnerStream for Played {
    async fn next(&mut self) -> InnerItem {
        loop {
            match self.acts.pop_front() {
                Some(Act::View(f)) => f(&mut *self.view.write().await),
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
        Dispatched::Stream(Box::new(Played { acts: acts.into(), view: self.view.clone(), build_id: build_id.0.clone(), attempt, done: false }))
    }
}

struct Rig {
    env: Arc<DriveEnv>,
    storage: Arc<MemoryNodesStorage>,
    build: BuildId,
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
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: topology.clone(),
        accepted,
        builds: builds.clone(),
        door: Arc::new(ClaimDoor { me, topology: topology.clone(), builds: builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()) }),
        dispatcher: Arc::new(Script { acts: Mutex::new(Some(acts)), view: topology }),
        gate: Arc::new(OpenGate),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        rows: Arc::new(NodeRows::new(storage.clone())),
    });
    Rig { env, storage, build: opened.build_id }
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
/// `Deleted` row): config belongs to the role, not the process (ops-naming "Node data"). At the
/// create's `Complete` the fabric-primary claims the new birth's row at the `path.name`, the new
/// node id and its status, the config the `Deleted` row kept. One `via-node-row` span names it.
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
