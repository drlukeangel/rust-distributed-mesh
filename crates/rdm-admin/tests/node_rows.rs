//! The node rows of a mesh, written by its primary node-admin (R13; ops-naming "Node data: meta,
//! config and exe"; lifecycles/node-create.md step 1; lifecycles/node-delete.md step 3): the executor's
//! claim, move to `.old` and `Deleted` row, the rows of births no create claimed, and the
//! fabric-primary's record of a stopped predecessor, which the drive of a Build feeds.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.admin.1`, its mesh primary
//! `mesh1.admin.2` (the executor of every `mesh1` node and the writer of its rows) and
//! `mesh1.rpc.1`, which carries config.

use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::node_rows::RecordAt as _;
use rdm_admin::admin::Records;
use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::MemoryBuildStateAdapter;
use rdm_admin::model::*;
use rdm_admin::node_rows::{ClaimFor, NodeRows, RowRefused};
use rdm_admin::storage::{MemoryNodesStorage, NodeRow, NodesStorage};
use rdm_admin::topology::Topology;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

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

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn tier(t: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("tier".to_string(), t.to_string())])
}

fn view() -> Topology {
    let rpc = node("mesh1.rpc.1", false, false);
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
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

/// `r` with its revision cleared: a row compared where its revision (the writer's count of its
/// writes, Q235 P1) is not what the cell is about.
fn sans_revision(r: Option<NodeRow>) -> Option<NodeRow> {
    r.map(|r| NodeRow { revision: 0, ..r })
}

/// `r` as its one writer wrote it once: the revision stepped once past the row handed to it (Q235 P1).
/// A row written again would carry a later one.
fn once(r: &NodeRow) -> NodeRow {
    NodeRow { revision: r.revision + 1, ..r.clone() }
}

/// The record of `n` as its create left it: the fixture's `mesh1.rpc.1` carries config `{tier: gold}`.
fn row_of(n: &Node) -> NodeRow {
    let config = if n.name.to_string() == "mesh1.rpc.1" { tier("gold") } else { Default::default() };
    NodeRow { topology_storage_provider: None, name: n.name.clone(), node_id: n.node_id.clone(), status: n.status, config, tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }
}

/// CONTRACT: `node.delete` leaves the node's row at its `path.name` with status `Deleted`, its
/// node id and its config kept (ops-naming, the delete point; lifecycles/node-delete.md step 3), written
/// by the mesh-primary that executes it, from the exact record it acted on. One `via-node-row` span
/// names the row written.
#[test]
fn a_deleted_node_leaves_a_deleted_row_with_its_record_and_config() {
    let cap = capture();
    let (row, victim) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let victim = held(&mp, "mesh1.rpc.1").await;
        storage.write_record(&row_of(&victim)).await.unwrap();
        mp.rows.deleted(&victim, "node.delete:mesh1.rpc.1").await.expect("the mesh primary writes the Deleted row");
        (storage.record_at(&victim.name).await.unwrap(), victim.node_id)
    });
    assert_eq!(sans_revision(row), Some(NodeRow { topology_storage_provider: None, name: "mesh1.rpc.1".parse().unwrap(), node_id: victim.clone(), status: NodeStatus::Deleted, config: tier("gold"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }), "the row says Deleted and keeps the record and the config");
    let spans = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["node"].as_str(), a["subject"].as_str(), a["node_id"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some("mesh1.admin.2"), Some("mesh1.rpc.1"), Some(victim.as_str()), Some("1"), Some("row-deleted")), "{a:#?}");
}

/// CONTRACT (R-TG1: tags are labels in the node's meta; lifecycles/node-delete.md step 3: the `Deleted`
/// row keeps the record and its config; proposal node-properties-own-commands.md §1, tags: "set by
/// the create spec (may be empty); inherited with the config on recover (from the Deleted row at the
/// claim) and on replace"): a node whose row carries tags {rabbit, red} is deleted by its mesh's
/// writer, which holds no record of the row yet: the `Deleted` row keeps the tags with the record and
/// the config, and the tags index names the node under neither label. A plain `node.create` at that
/// `path.name` (not a recovery: no exited birth taken over) carries its create spec's config and tags
/// (Q166), none: the labels index no node. What must NOT happen: a deleted node still selected by a
/// label, the tags dropped from the `Deleted` row, or a plain create inheriting the deleted node's
/// labels or config (only a recovery's and a successor's claim inherit them).
#[test]
fn a_deleted_nodes_row_keeps_its_tags_and_a_plain_create_at_its_name_inherits_none() {
    let cap = capture();
    let (row, born_row, victim, rabbit, red, born, after) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let victim = held(&mp, "mesh1.rpc.1").await;
        let tags: std::collections::BTreeSet<String> = ["rabbit".to_string(), "red".to_string()].into();
        storage.write_record(&NodeRow { tags: tags.clone(), ..row_of(&victim) }).await.unwrap();
        assert_eq!(storage.tagged("rabbit").await.unwrap(), [victim.node_id.clone()].into(), "the store keeps the tags index with the row it writes");
        mp.rows.deleted(&victim, "node.delete:mesh1.rpc.1").await.expect("the mesh primary writes the Deleted row");
        let row = storage.record_at(&victim.name).await.unwrap();
        let (rabbit, red) = (storage.tagged("rabbit").await.unwrap(), storage.tagged("red").await.unwrap());
        let born = NodeId::mint();
        mp.rows.claim(&victim.name, &born, "node.create:mesh1.rpc.1", None, None, ClaimFor::Create).await.expect("a node created at the Deleted row's path.name claims it");
        let after = (storage.tagged("rabbit").await.unwrap(), storage.tagged("red").await.unwrap());
        (row, storage.record_at(&victim.name).await.unwrap(), victim.node_id, rabbit, red, born, after)
    });
    assert_eq!(sans_revision(row), Some(NodeRow { topology_storage_provider: None, name: "mesh1.rpc.1".parse().unwrap(), node_id: victim.clone(), status: NodeStatus::Deleted, config: tier("gold"), tags: ["rabbit".to_string(), "red".to_string()].into(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }), "the Deleted row keeps the record, the config and the tags");
    assert!(rabbit.is_empty() && red.is_empty(), "the tags index names the deleted node under neither label: rabbit {rabbit:?}, red {red:?}");
    let born_row = born_row.expect("the new node's row");
    assert_eq!((born_row.config, born_row.tags), (Default::default(), Default::default()), "a plain create carries its create spec's config and tags, none");
    assert!(after.0.is_empty() && after.1.is_empty(), "the labels index no node after a plain create's claim: rabbit {:?}, red {:?}", after.0, after.1);
    let claims: Vec<Value> = cap.spans_named("rdm.node_admin.node.create.via-node-row").into_iter().filter(|s| s["attributes"]["node_id"] == born.as_str()).collect();
    assert_eq!(claims.len(), 1, "{claims:#?}");
    let c = &claims[0]["attributes"];
    assert_eq!((c["moment"].as_str(), c["seeded_from"].as_str(), c["tags"].as_str(), c["outcome"].as_str()), (Some("claim"), Some(victim.as_str()), Some(""), Some("row-seeded")), "{c:#?}");
    eprintln!("trace {}: rdm.node_admin.node.create.via-node-row moment=claim subject=mesh1.rpc.1 node_id={born} seeded_from={victim} tags= outcome=row-seeded", claims[0]["trace_id"].as_str().unwrap_or(""));
    let spans = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["outcome"].as_str(), a["tags"].as_str()), (Some("row-deleted"), Some("rabbit,red")), "{a:#?}");
    eprintln!("trace {}: rdm.node_admin.node.delete.via-node-row subject=mesh1.rpc.1 tags=rabbit,red outcome=row-deleted", spans[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (Q166; node-config-update.md: "A node's first config is its create spec's `config` in the
/// Build, written to its row at the claim and carried in its first join"; node-recovery.md "Retained":
/// config seeded from the Deleted row): a plain `node.create` at a `Deleted` row's `path.name` whose
/// create spec names config {tier: gold} and the tag `blue` claims the row with its spec's config and
/// tags, not the Deleted row's {tier: silver}: the new node id, `Pending`. A recovery's claim at the
/// same `path.name`, whatever spec it is handed, inherits the Deleted row's config and tags. One
/// `via-node-row` span per claim names it (keys 1). What must NOT happen: a plain create seeded with a
/// previous node's config over its own spec's, or a recovery losing the config its predecessor held.
#[test]
fn a_plain_create_carries_its_create_specs_config_and_a_recovery_inherits_the_deleted_rows() {
    let cap = capture();
    let (row, recovered, born, old) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let old = NodeId::mint();
        let deleted = NodeRow { topology_storage_provider: None, name: "mesh1.rpc.2".parse().unwrap(), node_id: old.clone(), status: NodeStatus::Deleted, config: tier("silver"), tags: ["red".to_string()].into(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
        storage.write_record(&deleted).await.unwrap();
        let mp = mesh_primary(&storage);
        let born = NodeId::mint();
        let blue: std::collections::BTreeSet<String> = ["blue".to_string()].into();
        mp.rows.claim_spec(&"mesh1.rpc.2".parse().unwrap(), &born, "node.create:mesh1.rpc.2", None, None, &blue, &tier("gold"), None, ClaimFor::Create).await.expect("the name is free of live rows");
        let row = storage.record_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap();
        let other = Arc::new(MemoryNodesStorage::default());
        other.write_record(&deleted).await.unwrap();
        let mp = mesh_primary(&other);
        mp.rows.claim_spec(&"mesh1.rpc.2".parse().unwrap(), &NodeId::mint(), "node.create:mesh1.rpc.2", None, None, &blue, &tier("gold"), None, ClaimFor::Recovery).await.expect("the Deleted row holds no name");
        (row, other.record_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, old)
    });
    assert_eq!(sans_revision(row), Some(NodeRow { topology_storage_provider: None, name: "mesh1.rpc.2".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Starting, config: tier("gold"), tags: ["blue".to_string()].into(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }), "the new birth holds the row with its create spec's config and tags");
    let recovered = recovered.expect("the recovered birth's row");
    assert_eq!((recovered.config, recovered.tags), (tier("silver"), ["red".to_string()].into()), "a recovery inherits the Deleted row's config and tags");
    let spans: Vec<Value> = cap.spans_named("rdm.node_admin.node.create.via-node-row").into_iter().filter(|s| s["attributes"]["node_id"] == born.as_str()).collect();
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["moment"].as_str(), a["subject"].as_str(), a["node_id"].as_str(), a["seeded_from"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some("claim"), Some("mesh1.rpc.2"), Some(born.as_str()), Some(old.as_str()), Some("1"), Some("row-seeded")), "{a:#?}");
}

/// CONTRACT (Q305; build-deployment.md §3: a replacement's "config and tags are its Build entry's,
/// written at its claim, never the predecessor's"; node-replace.md "Properties": "an entry naming no
/// ports names none until the node's own update"): a Build's replacement of a member at its own path
/// claims over the `Deleted` row at the key, as a recovery does, but its row takes the entry's config
/// and tags: an empty entry over a row holding the dead birth's two ports writes none of them, and an
/// entry naming {tier: gold} and `blue` writes those. Its predecessor is the Deleted row's birth. The
/// `via-node-row` span names the seed and the entry's key count (keys 0).
/// Must NOT happen: a replacement's row starting with the dead birth's ports (rafka #3400, Build
/// gp0027r3spsd: `row-seeded keys=2`).
#[test]
fn a_builds_replacement_at_its_own_path_takes_its_entrys_config_and_tags_not_the_deleted_rows() {
    let cap = capture();
    let ports: BTreeMap<String, String> = [("kafka_port".to_string(), "127.0.0.1:34003".to_string()), ("admin_port".to_string(), "127.0.0.1:38823".to_string())].into();
    let name: PathName = "mesh1.gateway.1".parse().unwrap();
    let (empty, named, born, old) = cap.run(async {
        let old = NodeId::mint();
        let births = vec![rdm_admin::storage::BirthFacts::new(IncarnationId::mint(), name.clone())];
        let deleted = NodeRow { topology_storage_provider: None, name: name.clone(), node_id: old.clone(), status: NodeStatus::Deleted, config: ports.clone(), tags: ["red".to_string()].into(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births, predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
        let storage = Arc::new(MemoryNodesStorage::default());
        storage.write_record(&deleted).await.unwrap();
        let born = NodeId::mint();
        mesh_primary(&storage).rows.claim_spec(&name, &born, "node.create:mesh1.gateway.1", None, None, &Default::default(), &BTreeMap::new(), None, ClaimFor::Replacement).await.expect("the Deleted row holds no name");
        let other = Arc::new(MemoryNodesStorage::default());
        other.write_record(&deleted).await.unwrap();
        let blue: std::collections::BTreeSet<String> = ["blue".to_string()].into();
        mesh_primary(&other).rows.claim_spec(&name, &NodeId::mint(), "node.create:mesh1.gateway.1", None, None, &blue, &tier("gold"), None, ClaimFor::Replacement).await.expect("the Deleted row holds no name");
        (storage.record_at(&name).await.unwrap(), other.record_at(&name).await.unwrap(), born, old)
    });
    let empty = empty.expect("the replacement's row");
    assert_eq!((empty.node_id.clone(), empty.config.clone(), empty.tags.clone()), (born.clone(), BTreeMap::new(), Default::default()), "an empty entry writes no config and no tags, never the dead birth's ports");
    assert_eq!(empty.predecessor.as_ref().map(|p| p.node_id.clone()), Some(old.clone()), "the predecessor is the Deleted row's birth, as a recovery's is");
    let named = named.expect("the replacement's row");
    assert_eq!((named.config, named.tags), (tier("gold"), ["blue".to_string()].into()), "the entry's config and tags are the row's");
    let spans: Vec<Value> = cap.spans_named("rdm.node_admin.node.create.via-node-row").into_iter().filter(|s| s["attributes"]["node_id"] == born.as_str()).collect();
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["seeded_from"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some(old.as_str()), Some("0"), Some("row-seeded")), "{a:#?}");
}

/// CONTRACT: a rewrite is lossless for every field its writer knows (node-identity-and-state.md "A
/// rewrite is lossless"; node-recovery.md "Retained: ... config (seeded from the Deleted row)"). The
/// mesh-primary's claim writes a birth's row with its create spec's config (Q166);
/// the executor's record of that birth (the create's own record) carries no config. When the birth
/// exits, its Deleted row carries the config the writer holds for it, never the record's empty
/// default, and the birth that recovers the name is seeded with it. The `via-node-row` delete span names
/// `config_from` `writer-record` and one key.
#[test]
fn a_deleted_row_carries_the_config_the_writer_holds_not_the_executors_empty_record() {
    let cap = capture();
    let (deleted, next_row, born) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let name: PathName = "mesh1.rpc.2".parse().unwrap();
        storage.write_record(&NodeRow { topology_storage_provider: None, name: name.clone(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: tier("silver"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        let mp = mesh_primary(&storage);
        let born = NodeId::mint();
        mp.rows.claim_spec(&name, &born, "node.create:mesh1.rpc.2", None, None, &Default::default(), &tier("silver"), None, ClaimFor::Create).await.expect("the name is free of live rows");
        // The executor's record of the birth it created: no config on it.
        let mut record = Node::allocated(name.clone());
        record.node_id = born.clone();
        mp.rows.deleted(&record, "node.delete:mesh1.rpc.2").await.expect("the Deleted row is written");
        let deleted = storage.record_at(&name).await.unwrap();
        let next = NodeId::mint();
        mp.rows.claim(&name, &next, "node.create:mesh1.rpc.2", None, None, ClaimFor::Recovery).await.expect("the Deleted row holds no name");
        (deleted, storage.record_at(&name).await.unwrap(), born)
    });
    assert_eq!(deleted.map(|r| (r.node_id, r.status, r.config)), Some((born.clone(), NodeStatus::Deleted, tier("silver"))), "the Deleted row keeps the config the writer holds for the birth");
    assert_eq!(next_row.map(|r| r.config), Some(tier("silver")), "the birth that recovers the name is seeded with it");
    let spans = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(spans.len(), 1, "{spans:#?}");
    let a = &spans[0]["attributes"];
    assert_eq!((a["config_from"].as_str(), a["keys"].as_str(), a["outcome"].as_str()), (Some("writer-record"), Some("1"), Some("row-deleted")), "{a:#?}");
    eprintln!("trace {}: rdm.node_admin.node.delete.via-node-row config_from=writer-record keys=1 outcome=row-deleted", spans[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT: two nodes never hold one name (R13 acceptance): a birth at a `path.name` whose row
/// another live node holds gets no row; the row writer's claim refuses it `NameHeld`
/// naming the holder, and the holder's row stays exactly as it was. One `via-node-row` span names
/// the refusal.
#[test]
fn a_birth_at_a_name_another_live_nodes_row_holds_is_refused_name_held_and_nothing_is_written() {
    let cap = capture();
    let (row, born, held) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let held = NodeRow { topology_storage_provider: None, name: "mesh1.rpc.2".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::Ready, config: tier("silver"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
        storage.write_record(&held).await.unwrap();
        let birth = node("mesh1.rpc.2", false, false);
        let born = birth.node_id.clone();
        install(&mp.records, &mp.view, move |v| v.nodes.push(birth)).await;
        (storage.record_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, held)
    });
    assert_eq!(row, Some(once(&held)), "the holder's row stands; nothing of the birth is written");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let ours: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["node_id"] == born.as_str()).collect();
    assert_eq!(ours.len(), 1, "{spans:#?}");
    let a = &ours[0]["attributes"];
    assert_eq!(a["outcome"].as_str(), Some("name-held"), "{a:#?}");
    assert!(a["detail"].as_str().is_some_and(|d| d.contains(held.node_id.as_str())), "the refusal names the holder: {a:#?}");
    eprintln!("trace {}: rdm.node_admin.node.create.via-node-row subject=mesh1.rpc.2 outcome=name-held over another live node's row", ours[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-OW1 item 2: "No bypass; a conflicting holder is NameHeld, never overwritten"): the
/// Day-0 claim of the bootstrap admin's own `path.name` is the create door's claim. A name
/// another live node's row holds is refused `NameHeld` naming the holder, and the holder's row stays
/// exactly as it was; a free name is written `Pending` under the admin's node.id. One `via-node-row`
/// span each, `moment` `claim`, operation `fabric-create`.
#[test]
fn a_day0_claim_of_a_name_another_live_nodes_row_holds_is_refused_name_held_and_nothing_is_written() {
    let cap = capture();
    let name: PathName = "mesh1.admin.1".parse().unwrap();
    let (refused, row, held, written, me) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let held = NodeRow { topology_storage_provider: None, name: name.clone(), node_id: NodeId::mint(), status: NodeStatus::Ready, config: tier("silver"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
        storage.write_record(&held).await.unwrap();
        let me = NodeId::mint();
        let refused = rdm_admin::node_rows::claim_day0_name(&*storage, &Default::default(), &name, &me, None).await;
        let row = storage.record_at(&name).await.unwrap();
        let free = Arc::new(MemoryNodesStorage::default());
        let written = rdm_admin::node_rows::claim_day0_name(&*free, &Default::default(), &name, &me, None).await;
        (refused, row, held, written, me)
    });
    assert!(matches!(&refused, Err(rdm_admin::node_rows::RowRefused::NameHeld(h)) if h.by == held.node_id), "{refused:?}");
    assert_eq!(row, Some(once(&held)), "the holder's row stands; nothing of the Day-0 admin is written");
    assert!(matches!(&written, Ok(rdm_admin::node_rows::Claimed::Written { row, seeded_from: None }) if row.node_id == me && row.status == NodeStatus::Pending), "{written:?}");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let ours: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["node_id"] == me.as_str()).collect();
    assert_eq!(ours.len(), 2, "{spans:#?}");
    let outcomes: Vec<(Option<&str>, Option<&str>, Option<&str>)> = ours.iter().map(|s| (s["attributes"]["moment"].as_str(), s["attributes"]["operation"].as_str(), s["attributes"]["outcome"].as_str())).collect();
    assert_eq!(outcomes, vec![(Some("claim"), Some("fabric-create"), Some("name-held")), (Some("claim"), Some("fabric-create"), Some("row-written"))], "{ours:#?}");
    assert!(ours[0]["attributes"]["detail"].as_str().is_some_and(|d| d.contains(held.node_id.as_str())), "the refusal names the holder: {:#?}", ours[0]);
    eprintln!("trace {}: rdm.node_admin.node.create.via-node-row subject=mesh1.admin.1 operation=fabric-create outcome=name-held over another live node's row", ours[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT: a birth no create of the writer claimed (one made before the writer took its mesh's
/// role) gets its row when the writer's view first holds it, from the record it holds (ops-naming
/// "Node data": "writes the whole row from that held record"), seeded from the `Deleted` row at its
/// `path.name`. One `via-node-row` span, `moment` `arrival`, at the view install, names it.
#[test]
fn a_birth_no_create_claimed_gets_its_row_seeded_when_the_writers_view_holds_it() {
    let cap = capture();
    let (row, born, old) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let old = NodeId::mint();
        storage.write_record(&NodeRow { topology_storage_provider: None, name: "mesh1.rpc.2".parse().unwrap(), node_id: old.clone(), status: NodeStatus::Deleted, config: tier("silver"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        let mp = mesh_primary(&storage);
        let birth = node("mesh1.rpc.2", false, false);
        let born = birth.node_id.clone();
        install(&mp.records, &mp.view, move |v| v.nodes.push(birth)).await;
        (storage.record_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born, old)
    });
    assert_eq!(sans_revision(row), Some(NodeRow { topology_storage_provider: None, name: "mesh1.rpc.2".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Ready, config: tier("silver"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }), "the birth holds the row and the Deleted row's config");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let rpc2: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["subject"] == "mesh1.rpc.2").collect();
    assert_eq!(rpc2.len(), 1, "{spans:#?}");
    let a = &rpc2[0]["attributes"];
    assert_eq!((a["moment"].as_str(), a["node_id"].as_str(), a["seeded_from"].as_str(), a["outcome"].as_str()), (Some("arrival"), Some(born.as_str()), Some(old.as_str()), Some("row-seeded")), "{a:#?}");
}

/// CONTRACT: the store is read only to rebuild after a role change (ops-naming "Node data"; R13:
/// "Rebuild after a role change reads the mesh's own rows"), and that rebuild never overwrites a row
/// a node already holds: an admin that takes its mesh's primary role writes the row of every member
/// of that mesh but itself, and the row of a member that already holds its name stays exactly as
/// written (a config the node has not applied yet is not wiped by the new primary's view). A member
/// with no row gets one. One `via-node-row` span per member: `row-held-by-node` for the held row,
/// `row-written` for the new one.
#[test]
fn a_take_over_rebuilds_the_rows_and_never_overwrites_a_row_its_node_holds() {
    let cap = capture();
    let (row, written) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let records = Records::default();
        let before = view();
        let topology = Arc::new(RwLock::new(before.clone()));
        records.set_rows(Arc::new(NodeRows::new(me.clone(), before.node(&me).expect("the admin is in the view").node_id.clone(), topology.clone(), storage.clone())));
        let rpc1 = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        let written = NodeRow { topology_storage_provider: None, name: rpc1.name.clone(), node_id: rpc1.node_id.clone(), status: NodeStatus::ConnectionsDeleting, config: tier("platinum"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
        storage.write_record(&written).await.unwrap();
        install(&records, &topology, |t| {
            for n in t.nodes.iter_mut() {
                n.is_primary = n.name.to_string() == "mesh1.admin.1";
            }
        })
        .await;
        (storage.record_at(&rpc1.name).await.unwrap(), written)
    });
    assert_eq!(row, Some(once(&written)), "the row the node holds is untouched by the new primary's rebuild");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let outcome = |name: &str| spans.iter().filter(|s| s["attributes"]["subject"] == name).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
    assert_eq!(outcome("mesh1.rpc.1"), vec!["row-held-by-node"], "{spans:#?}");
    assert_eq!(outcome("mesh1.admin.2"), vec!["row-written"], "{spans:#?}");
}

/// CONTRACT (R13: "Rebuild after a role change reads the mesh's own rows", and a rebuild never
/// overwrites a row a node holds; R12: a stopped node is told nothing of a rename): mesh1.rpc.1 was
/// stopped and renamed by node.update while stopped, so the one row it holds is at
/// `mesh1.rpc.1.old`. An admin that takes mesh1's primary role over a view that still lists that
/// stopped birth at the name it published (`mesh1.rpc.1`) writes no row for it: the node holds a
/// row, found through its id index, and that row stays the node's only one. No second row at the
/// bare name holds the name for a stopped birth, and the node's id index still names `.old`, where
/// the fabric-primary's view lists it for its delete. One `via-node-row` span, `row-held-by-node`.
#[test]
fn a_take_over_on_a_view_older_than_a_rename_leaves_the_renamed_nodes_row_its_only_one() {
    let cap = capture();
    let (old_row, bare_row, indexed, renamed) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let records = Records::default();
        let mut before = view();
        let rpc1 = before.nodes.iter_mut().find(|n| n.name.to_string() == "mesh1.rpc.1").unwrap();
        rpc1.status = NodeStatus::Leaving;
        rpc1.stopped = true;
        rpc1.routable = false;
        let rpc1 = rpc1.clone();
        let topology = Arc::new(RwLock::new(before.clone()));
        records.set_rows(Arc::new(NodeRows::new(me.clone(), before.node(&me).expect("the admin is in the view").node_id.clone(), topology.clone(), storage.clone())));
        // The rename its mesh's former primary wrote while the node was stopped: the row moved to `.old`.
        let renamed = NodeRow { topology_storage_provider: None, name: rpc1.name.renamed(), node_id: rpc1.node_id.clone(), status: NodeStatus::Leaving, config: tier("gold"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
        storage.write_record(&renamed).await.unwrap();
        install(&records, &topology, |t| {
            for n in t.nodes.iter_mut() {
                n.is_primary = n.name.to_string() == "mesh1.admin.1";
            }
        })
        .await;
        (storage.record_at(&rpc1.name.renamed()).await.unwrap(), storage.record_at(&rpc1.name).await.unwrap(), storage.node(&rpc1.node_id).await.unwrap().map(|r| r.name), renamed)
    });
    assert_eq!(old_row, Some(once(&renamed)), "the renamed node's row stands at .old as written");
    assert_eq!(bare_row, None, "no second row of the stopped node holds its former name");
    assert_eq!(indexed, Some(renamed.name.clone()), "the node's id index names its one row, at .old");
    let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let outcome = |name: &str| spans.iter().filter(|s| s["attributes"]["subject"] == name).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
    assert_eq!(outcome("mesh1.rpc.1"), vec!["row-held-by-node"], "{spans:#?}");
}

/// CONTRACT (proposal node-properties-own-commands.md §2.1 Claim and move: "a row whose
/// `renamed_from` key still holds the same node id has a pending release, and the writer releases it
/// ... It runs wherever a writer meets the row: ... a mesh-primary's rebuild from its mesh's rows after
/// a role change (R13)"; §2.4: "The writer that wrote the put may be gone; the row carries everything
/// the next one needs"; §7: "the writer lost between the put and the release: a successor
/// mesh-primary's rebuild finds the row's `renamed_from` still held by the same node id and releases
/// it"): a lost writer's rename of `mesh1.rpc.1` to `mesh1.rpc.9` put the row at `mesh1.rpc.9` carrying
/// `renamed_from: mesh1.rpc.1` and never released `mesh1.rpc.1`. An admin taking mesh1's role, whose
/// view still lists the node at `mesh1.rpc.1`, rebuilds: it reads the node's row through the id index
/// at `mesh1.rpc.9`, releases `mesh1.rpc.1`, and writes no row at the view's name. One
/// `rdm.node_admin.name.update.via-pending-release` span says `released` with `moment=rebuild`;
/// the rebuild's `via-node-row` span for the node says `row-held-by-node`. What must NOT happen: the old
/// key left held by the node, or a row put back at `mesh1.rpc.1`.
#[test]
fn a_take_over_rebuild_finishes_a_lost_writers_pending_release() {
    let cap = capture();
    let (old, new) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let records = Records::default();
        let before = view();
        let topology = Arc::new(RwLock::new(before.clone()));
        records.set_rows(Arc::new(NodeRows::new(me.clone(), before.node(&me).expect("the admin is in the view").node_id.clone(), topology.clone(), storage.clone())));
        let rpc1 = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        let to: PathName = "mesh1.rpc.9".parse().unwrap();
        // The lost writer's two writes: the row at its old key, then the put at the new key; no release.
        storage.write_record(&NodeRow { topology_storage_provider: None, name: rpc1.name.clone(), node_id: rpc1.node_id.clone(), status: NodeStatus::Leaving, config: tier("gold"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        storage.write_record(&NodeRow { topology_storage_provider: None, name: to.clone(), node_id: rpc1.node_id.clone(), status: NodeStatus::Leaving, config: tier("gold"), tags: Default::default(), allocated: None, renamed_from: Some(rpc1.name.clone()), app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        install(&records, &topology, |t| {
            for n in t.nodes.iter_mut() {
                n.is_primary = n.name.to_string() == "mesh1.admin.1";
            }
        })
        .await;
        (storage.record_at(&rpc1.name).await.unwrap(), storage.record_at(&to).await.unwrap())
    });
    assert_eq!(old, None, "the rebuild released the old key the lost writer left held");
    assert_eq!(new.map(|r| r.renamed_from), Some(Some("mesh1.rpc.1".parse().unwrap())), "the row at the new key stands as written");
    let releases = cap.spans_named("rdm.node_admin.name.update.via-pending-release");
    let seen: Vec<_> = releases.iter().map(|s| (s["attributes"]["from"].as_str().unwrap_or("").to_string(), s["attributes"]["moment"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
    assert_eq!(seen, vec![("mesh1.rpc.1".to_string(), "rebuild".to_string(), "released".to_string())], "{releases:#?}");
    let rows = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    let outcome = |name: &str| rows.iter().filter(|s| s["attributes"]["subject"] == name).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
    assert_eq!(outcome("mesh1.rpc.1"), vec!["row-held-by-node"], "{rows:#?}");
    println!("trace {}: rdm.node_admin.name.update.via-pending-release from=mesh1.rpc.1 to=mesh1.rpc.9 moment=rebuild outcome=released", releases[0]["trace_id"].as_str().unwrap_or(""));
}

type Rebuilt = (BTreeMap<String, Option<NodeRow>>, Option<NodeStatus>, NodeId, NodeId, NodeId, NodeId, String);

/// The rebuild cell's story: P's writes, then N's role taken over one shared root.
async fn rebuild_over_one_root(trace_id: String) -> Rebuilt {
        let d = std::env::temp_dir().join(format!("node-rows-shared-root-{}", NodeId::mint()));
        let root = rdm_admin::store_root::StoreRoot::at(&d.join("store"));
        let open = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(rdm_admin::storage::FileNodesStorage::open(&data, &root, "mesh1", &rdm_admin::store_log::LogWriter::mint()).await.unwrap()) as Arc<dyn NodesStorage> }
        };
        let (p_store, n_store) = (open("mesh1.admin.2").await, open("mesh1.admin.1").await);
        let mut before = view();
        for n in before.nodes.iter_mut() {
            n.is_fabric_primary = n.name.to_string() == "mesh1.admin.2";
        }
        let rpc1 = before.nodes.iter().find(|n| n.name.to_string() == "mesh1.rpc.1").cloned().unwrap();
        let (old_pred, deleted_pred) = (NodeId::mint(), NodeId::mint());
        // P's writes, as its role made them.
        for row in [
            NodeRow { topology_storage_provider: None, name: rpc1.name.clone(), node_id: rpc1.node_id.clone(), status: NodeStatus::ConnectionsDeleting, config: tier("platinum"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None },
            NodeRow { topology_storage_provider: None, name: "mesh1.rpc.2.old".parse().unwrap(), node_id: old_pred.clone(), status: NodeStatus::Deleted, config: tier("bronze"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None },
            NodeRow { topology_storage_provider: None, name: "mesh1.rpc.3".parse().unwrap(), node_id: deleted_pred.clone(), status: NodeStatus::Deleted, config: tier("silver"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None },
        ] {
            p_store.write_record(&row).await.unwrap();
        }
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let records = Records::default();
        let me_id = before.node(&me).unwrap().node_id.clone();
        let topology = Arc::new(RwLock::new(before));
        records.set_rows(Arc::new(NodeRows::new(me.clone(), me_id, topology.clone(), n_store.clone())));
        let (successor, fresh) = (node("mesh1.rpc.2", false, false), node("mesh1.rpc.3", false, false));
        let (succ, fresh_id) = (successor.node_id.clone(), fresh.node_id.clone());
        // N takes mesh1's role (and the fabric-primary role P held with it): the mesh-primary writes its
        // mesh's rows (R13), so the rebuild is the mesh-primary role's.
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
            rows.insert(name.to_string(), n_store.record_at(&name.parse().unwrap()).await.unwrap());
        }
        let view_status = topology.read().await.node(&rpc1.name).map(|n| n.status);
        let _ = std::fs::remove_dir_all(&d);
        (rows, view_status, succ, fresh_id, old_pred, deleted_pred, trace_id)
}

/// CONTRACT (R14 item 7 and acceptance "a mesh role change: the successor rebuilds and holds the
/// predecessor's rows (Node rows, `.old`, Deleted, config)"): the predecessor P and the successor N
/// are two admins with their own data dirs and their own store instances over one shared root. P
/// held the role and wrote the rows: a live member's row (status ConnectionsDeleting, config platinum), a
/// renamed predecessor's row moved to `mesh1.rpc.2.old` (Deleted, config bronze) and a deleted
/// node's row at `mesh1.rpc.3` (Deleted, config silver). N takes the role over a view whose live
/// digests hold mesh1.rpc.1 ready, a successor at mesh1.rpc.2 and a new node at mesh1.rpc.3. N's
/// rebuild reads P's rows from the shared root: the live member's row stands untouched, the new
/// node at the Deleted row's name is seeded silver, the successor is seeded bronze from the `.old`
/// row, and N holds every one of P's rows. A row never overrides a live digest's status: N's view
/// holds mesh1.rpc.1 ready while its row says ConnectionsDeleting.
#[test]
fn a_take_over_rebuilds_from_the_shared_root_and_holds_the_predecessors_rows() {
    let cap = crate::mesh_delete_resume::capture();
    let (rows, view_status, succ, fresh, old_pred, deleted_pred, trace_id) = cap.run(async {
        let root_span = tracing::info_span!("rdm.test.node.create.via-shared-root-rebuild");
        let trace_id = crate::mesh_delete_resume::trace_id(&root_span);
        tracing::Instrument::instrument(rebuild_over_one_root(trace_id), root_span).await
    });
    println!("trace_id={trace_id}");
    let row = |name: &str| rows[name].clone().unwrap_or_else(|| panic!("the successor holds no row at {name}: {rows:#?}"));
    assert_eq!((row("mesh1.rpc.1").status, row("mesh1.rpc.1").config), (NodeStatus::ConnectionsDeleting, tier("platinum")), "the live member's row stands as P wrote it");
    assert_eq!((row("mesh1.rpc.2.old").node_id, row("mesh1.rpc.2.old").status), (old_pred.clone(), NodeStatus::Deleted), "N holds P's .old row");
    assert_eq!((row("mesh1.rpc.2").node_id, row("mesh1.rpc.2").config), (succ.clone(), tier("bronze")), "the successor is seeded from the .old row");
    assert_eq!((row("mesh1.rpc.3").node_id, row("mesh1.rpc.3").config), (fresh.clone(), tier("silver")), "the node created at the Deleted row's name is seeded from it, its row overwritten at that key");
    assert_eq!(view_status, Some(NodeStatus::Ready), "a row never overrides a live digest's status");
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
        let stores: Vec<(&str, Arc<dyn NodesStorage>)> = vec![("memory", Arc::new(MemoryNodesStorage::default())), ("file", Arc::new(rdm_admin::storage::FileNodesStorage::open(&dir, &rdm_admin::store_root::StoreRoot::at(&dir.join("store")), "mesh1", &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap()))];
        for (kind, store) in stores {
            let id = NodeId::mint();
            let held = NodeRow { topology_storage_provider: None, name: "mesh1.rpc.1".parse().unwrap(), node_id: id.clone(), status: NodeStatus::ConnectionsDeleting, config: tier("platinum"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
            store.write_record(&held).await.unwrap();
            assert!(!rdm_admin::node_rows::release_record(&*store, &Default::default(), &held.name, &NodeId::mint()).await.unwrap(), "{kind}: another node lets go of nothing here");
            assert_eq!(store.record_at(&held.name).await.unwrap(), Some(once(&held)), "{kind}: a release by another node leaves the row");
            assert!(rdm_admin::node_rows::release_record(&*store, &Default::default(), &held.name, &id).await.unwrap(), "{kind}: the holder lets go of its key");
            assert_eq!(store.record_at(&held.name).await.unwrap(), None, "{kind}: the holder's release frees the key");
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
        (storage.record_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap(), born)
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
    use rdm_admin::deployment::endpoint::RPC_NODE;
    use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts};
    use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
    use rdm_admin::join::Joins;
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
        t.meshes = ["mesh1", "mesh2", "mesh3"].iter().map(|m| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::Ready }).collect();
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
            (fp_store.record_at(&name).await.unwrap(), mp_store.record_at(&name).await.unwrap(), born)
        });
        assert_eq!(fp_row, None, "the fabric-primary writes no row of a mesh that has a primary");
        assert_eq!(mp_row.map(|r| r.node_id), Some(born.clone()), "the mesh's primary writes its member's row");
        let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let ours: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["node_id"] == born.as_str()).collect();
        assert_eq!(ours.len(), 1, "{spans:#?}");
        assert_eq!((ours[0]["attributes"]["node"].as_str(), ours[0]["attributes"]["outcome"].as_str()), (Some("mesh1.admin.2"), Some("row-written")), "{ours:#?}");
        eprintln!("trace {}: rdm.node_admin.node.create.via-node-row node=mesh1.admin.2 subject=mesh1.rpc.2 outcome=row-written; the fabric-primary mesh1.admin.1 wrote none", ours[0]["trace_id"].as_str().unwrap_or(""));
    }

    /// CONTRACT (R-OW1, ruling text item 1: "The mesh-primary writes every row in its mesh, including its
    /// own"; item 5: a role transfer makes the successor the writer of every row of the mesh, the
    /// predecessor's included, and rewrites nothing): an admin that takes its mesh's role (and the
    /// fabric-primary role) rebuilds the rows of its mesh's members from its view, its own birth included: its
    /// own row, absent, is written (`subject = mesh1.admin.1`, `node = mesh1.admin.1`, `row-written`)
    /// under its own node.id. A row a member already holds is left as it is. What must NOT happen: a
    /// mesh-primary that writes every member's row but its own, so its mesh's primary holds no row.
    #[test]
    fn an_admin_that_takes_over_mesh_primary_writes_every_members_row_its_own_included() {
        let cap = capture();
        let (own, me_id, others, held_before, held_after) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let mut before = view();
            for n in before.nodes.iter_mut() {
                n.is_fabric_primary = n.name.to_string() == "mesh1.admin.2";
            }
            let rpc = before.node(&"mesh1.rpc.1".parse().unwrap()).unwrap().clone();
            let held = NodeRow { config: tier("silver"), ..row_of(&rpc) };
            storage.write_record(&held).await.unwrap();
            let me_id = before.node(&"mesh1.admin.1".parse().unwrap()).unwrap().node_id.clone();
            let (records, topology) = writer_of("mesh1.admin.1", &before, &storage);
            install(&records, &topology, |v| {
                for n in v.nodes.iter_mut() {
                    let me = n.name.to_string() == "mesh1.admin.1";
                    n.is_fabric_primary = me;
                    n.is_primary = me;
                }
            })
            .await;
            let own = storage.record_at(&"mesh1.admin.1".parse().unwrap()).await.unwrap();
            let others = storage.record_at(&"mesh1.admin.2".parse().unwrap()).await.unwrap();
            let after = storage.record_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap();
            (own, me_id, others, held, after)
        });
        assert_eq!(own.map(|r| r.node_id), Some(me_id.clone()), "the mesh-primary writes its own row");
        assert!(others.is_some(), "every other member's row is written");
        assert_eq!(held_after, Some(once(&held_before)), "a row its node holds is never rewritten");
        let spans = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let ours: Vec<&Value> = spans.iter().filter(|s| s["attributes"]["subject"] == "mesh1.admin.1").collect();
        assert_eq!(ours.len(), 1, "{spans:#?}");
        assert_eq!(
            (ours[0]["attributes"]["node"].as_str(), ours[0]["attributes"]["node_id"].as_str(), ours[0]["attributes"]["moment"].as_str(), ours[0]["attributes"]["outcome"].as_str()),
            (Some("mesh1.admin.1"), Some(me_id.as_str()), Some("arrival"), Some("row-written")),
            "{ours:#?}"
        );
        eprintln!("trace {}: rdm.node_admin.node.create.via-node-row node=mesh1.admin.1 subject=mesh1.admin.1 moment=arrival outcome=row-written", ours[0]["trace_id"].as_str().unwrap_or(""));
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
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
            self.spawned.fetch_add(1, Ordering::SeqCst);
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this provider launches nothing".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
            DeploymentStatus::Unknown
        }
        async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
            Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
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

    /// `node.create:<name>` run by the executor whose rows writer `records` feeds, over `provider`:
    /// its error, if it failed.
    async fn create(records: &Records, provider: &Counted, name: &str) -> Result<(), String> {
        let data_root = std::env::temp_dir().join(format!("node-rows-create-{}", NodeId::mint()));
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/node-rows".into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { name: "mesh1.admin.2".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.join("store"),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let (joins, builds) = (Joins::default(), MemoryBuildStateAdapter::new());
        // The Build the create runs for: a run over a Build its node holds no fact of is refused at its start.
        let build_id = BuildId::parse(&rdm_core::fixture_crockford60("build-node-rows")).unwrap();
        let accepted = rdm_admin::build_state::BuildAccepted { build_id: build_id.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
        rdm_admin::build_state::BuildStateAdapter::publish_accepted(&builds, &accepted).await.unwrap();
        let pipeline = DeploymentPipeline { provider, joins: &joins, observer: &Unheard, sink: records, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let req = CreateRequest { build_id, attempt: 1, node: name.parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None };
        let r = pipeline.create(&req).await.map(|_| ()).map_err(|e| e.to_string());
        let _ = std::fs::remove_dir_all(&data_root);
        r
    }

    /// CONTRACT: a `node.create` whose name another live node's row holds launches nothing
    /// (lifecycles/node-create.md step 1): the mesh-primary claims the name at the start of
    /// `PrepareStorage`, before `DeployRuntime`, the claim refuses it `NameHeld` naming the
    /// holder, the create fails at `PrepareStorage` saying so, the provider is never asked to spawn,
    /// and the holder's row stands. One `via-node-row` span, `moment` `claim`, outcome `name-held`.
    #[test]
    fn a_create_whose_name_another_live_row_holds_launches_nothing() {
        let cap = capture();
        let (err, spawned, row, held) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let held = NodeRow { topology_storage_provider: None, name: "mesh1.rpc.2".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::Ready, config: tier("silver"), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
            storage.write_record(&held).await.unwrap();
            let (records, _view) = writer_of("mesh1.admin.2", &view(), &storage);
            let provider = Counted::default();
            let err = create(&records, &provider, "mesh1.rpc.2").await;
            (err, provider.spawned.load(Ordering::SeqCst), storage.record_at(&held.name).await.unwrap(), held)
        });
        let err = err.expect_err("the create is refused");
        assert!(err.contains("PrepareStorage") && err.contains(held.node_id.as_str()), "the create fails at PrepareStorage naming the holder: {err}");
        assert_eq!(spawned, 0, "nothing is launched");
        assert_eq!(row, Some(once(&held)), "the holder's row stands");
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
            (err, provider.spawned.load(Ordering::SeqCst), storage.record_at(&"mesh1.rpc.2".parse().unwrap()).await.unwrap())
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
    impl rdm_admin::build_state::BuildStateAdapter for LostAt<'_> {
        async fn publish_accepted(&self, a: &rdm_admin::build_state::BuildAccepted) -> Result<(), rdm_admin::build_state::BuildStateError> {
            self.inner.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &rdm_admin::build_state::AttemptOpened) -> Result<(), rdm_admin::build_state::BuildStateError> {
            self.inner.open_attempt(o).await
        }
        async fn read_build(&self, id: &BuildId) -> Result<rdm_admin::build_state::BuildProjection, rdm_admin::build_state::BuildStateError> {
            self.inner.read_build(id).await
        }
        async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
            self.inner.current_build().await
        }
        async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
            self.inner.newest_accepted_at_ms().await
        }
        async fn list_active(&self) -> Result<Vec<rdm_admin::build_state::BuildProjection>, rdm_admin::build_state::BuildStateError> {
            self.inner.list_active().await
        }
        async fn claim_attempt(&self, c: &rdm_admin::build_state::BuildAttemptClaim) -> Result<rdm_admin::build_state::ClaimOutcome, rdm_admin::build_state::BuildStateError> {
            self.inner.claim_attempt(c).await
        }
        async fn adopt_claim(&self, c: &rdm_admin::build_state::BuildAttemptClaim) -> Result<(), rdm_admin::build_state::BuildStateError> {
            self.inner.adopt_claim(c).await
        }
        async fn append_step_receipt(&self, r: &rdm_admin::build_state::BuildStepReceipt) -> Result<(), rdm_admin::build_state::BuildStateError> {
            if r.step == self.step && r.attempt == 1 {
                return Err(rdm_admin::build_state::BuildStateError::Io(format!("the executor of {} attempt 1 was lost before its {} receipt was written", r.build_id, r.step)));
            }
            self.inner.append_step_receipt(r).await
        }
        async fn append_attempt_receipt(&self, r: &rdm_admin::build_state::BuildAttemptReceipt) -> Result<(), rdm_admin::build_state::BuildStateError> {
            self.inner.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
            self.inner.facts().await
        }
        async fn committed_facts(&self, build: Option<&rdm_admin::build::BuildId>) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
            let facts = self.facts().await?;
            Ok(match build { Some(b) => facts.into_iter().filter(|f| f.build_id() == b).collect(), None => facts })
        }
        async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, rdm_admin::build_state::BuildStateError> {
            self.inner.recovery_of(path, node_id, incarnation).await
        }
        async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, rdm_admin::build_state::BuildStateError> {
            self.inner.mesh_deleted_of(node_id, incarnation).await
        }
        async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, rdm_admin::build_state::BuildStateError> {
            self.inner.exits_owed(build_id, mesh).await
        }
        async fn forget(&self, id: &BuildId) -> Result<(), rdm_admin::build_state::BuildStateError> {
            self.inner.forget(id).await
        }
    }

    /// Attempt `attempt` of `node.create:<name>` in the Build `build`, its receipts in `builds`, run
    /// by the executor whose rows writer `records` feeds, over `provider`: its error.
    async fn create_attempt(records: &Records, provider: &Counted, builds: &dyn rdm_admin::build_state::BuildStateAdapter, build: &BuildId, name: &str, attempt: u32) -> Result<(), String> {
        let data_root = std::env::temp_dir().join(format!("node-rows-create-{}", NodeId::mint()));
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/node-rows".into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { name: "mesh1.admin.2".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.join("store"),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider, joins: &joins, observer: &Unheard, sink: records, lifecycle: &NoLifecycleEvents, builds, template: &template, timeouts: Timeouts::default() };
        let req = CreateRequest { build_id: build.clone(), attempt, node: name.parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None };
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
            let (build, other) = (BuildId::parse(&rdm_core::fixture_crockford60("build-node-rows-5")).unwrap(), BuildId::parse(&rdm_core::fixture_crockford60("build-node-rows-6")).unwrap());
            for id in [&build, &other] {
                let accepted = rdm_admin::build_state::BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
                rdm_admin::build_state::BuildStateAdapter::publish_accepted(&builds, &accepted).await.unwrap();
            }
            // Lost before the AllocateIdentity receipt of attempt 1.
            let lost = LostAt { inner: &builds, step: "AllocateIdentity" };
            let lost_before_receipt = create_attempt(&records, &provider, &lost, &build, "mesh1.rpc.5", 1).await;
            let rerun_after_loss = create_attempt(&records, &provider, &builds, &build, "mesh1.rpc.5", 2).await;
            // Lost after the claim: the identity receipt names X, the store holds X's claim row.
            let x = NodeId::mint();
            let identity = serde_json::json!({ "node_id": x, "incarnation": IncarnationId::mint(), "supersedes": null, "deployment_id": rdm_admin::model::DeploymentId::mint() });
            rdm_admin::build_state::BuildStateAdapter::append_step_receipt(
                &builds,
                &rdm_admin::build_state::BuildStepReceipt { mesh: None, build_id: other.clone(), attempt: 1, operation: "node.create:mesh1.rpc.6".into(), step: "AllocateIdentity".into(), outcome: rdm_admin::build_state::StepOutcome::Complete, output: Some(identity), executor: None },
            )
            .await
            .unwrap();
            storage.write_record(&NodeRow { topology_storage_provider: None, name: "mesh1.rpc.6".parse().unwrap(), node_id: x.clone(), status: NodeStatus::Pending, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
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
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "the lost executor launches nothing".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
            DeploymentStatus::Unknown
        }
        async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
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
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
            self.spawned.lock().unwrap().push(spec.deployment_id.clone());
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this provider launches nothing".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
            DeploymentStatus::Unknown
        }
        async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
            self.found.lock().unwrap().push(spec.deployment_id.clone());
            Ok(if self.exited.lock().unwrap().as_ref() == Some(&spec.deployment_id) { rdm_admin::deployment::provider::Found::Exited(None) } else { rdm_admin::deployment::provider::Found::NeverLaunched })
        }
    }

    /// Attempt `attempt` of `node.create:<name>` in the Build `build`, its receipts in `builds`, run by
    /// the executor whose rows writer `records` feeds, over `provider`, its births' data dirs under
    /// `data_root`: its error.
    async fn create_attempt_over(records: &Records, provider: &dyn DeploymentProvider, observer: &dyn NodeObserver, builds: &dyn rdm_admin::build_state::BuildStateAdapter, build: &BuildId, name: &str, attempt: u32, data_root: &std::path::Path) -> Result<(), String> {
        create_attempt_executing("/nonexistent/node-rows", records, provider, observer, builds, build, name, attempt, data_root).await
    }

    /// [`create_attempt_over`] for a template whose executable is `executable`.
    #[allow(clippy::too_many_arguments)]
    async fn create_attempt_executing(executable: &str, records: &Records, provider: &dyn DeploymentProvider, observer: &dyn NodeObserver, builds: &dyn rdm_admin::build_state::BuildStateAdapter, build: &BuildId, name: &str, attempt: u32, data_root: &std::path::Path) -> Result<(), String> {
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: executable.into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { name: "mesh1.admin.2".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.to_path_buf(),
            store_dir: data_root.join("store"),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider, joins: &joins, observer, sink: records, lifecycle: &NoLifecycleEvents, builds, template: &template, timeouts: Timeouts::default() };
        let req = CreateRequest { build_id: build.clone(), attempt, node: name.parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None };
        pipeline.create(&req).await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// CONTRACT (build.md §5.2: a step receipt is not committed until it is durable in storage that
    /// survives the loss of the admin that wrote it; node-rpc-envelope.md Build carrier: "Completed steps are not rerun", an
    /// idempotent uncertain step is settled by its natural key; lifecycles/node-create.md step 1: the
    /// claim precedes the launch, and "Runtime exit requires independent exact provider proof, even for
    /// a deployment that never joined"): attempt 1 of `node.create:mesh1.rpc.7` runs on executor A
    /// through its name claim and A is lost inside `DeployRuntime`, before anything is launched. A's
    /// receipts never reach B's. B runs attempt 2 holding only what
    /// the shared store holds: A's claim row. B's create takes the identity A's claim was made with
    /// (the same node id, incarnation and deployment), the provider proving no runtime of that exact
    /// deployment exists, its claim finds the row that birth holds (`row-held-by-node`) and it reaches
    /// `DeployRuntime` with A's deployment. Must NOT happen: B minting a new birth that the name claim
    /// refuses `name-held` by A's birth that never launched, attempt after attempt (gate
    /// gate-gates-e6s4-d7fcc329 node_replace: "name-held: mesh2.rpc.1 is held by node nrsm925mnyf8",
    /// 200 attempts, the path never recreated).
    #[test]
    fn a_create_whose_executor_is_lost_after_its_claim_is_continued_under_the_claimed_identity_by_an_executor_that_never_heard_its_receipts() {
        use rdm_admin::build_state::{BuildAccepted, BuildFact, BuildStateAdapter, StepOutcome};
        let cap = capture();
        let (a_identity, b_err, found, spawned, row) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-lost-after-claim-{}", NodeId::mint()));
            let build = BuildId::parse(&rdm_core::fixture_crockford60("build-node-rows-7")).unwrap();
            let (a_builds, b_builds) = (Arc::new(MemoryBuildStateAdapter::new()), MemoryBuildStateAdapter::new());
            for builds in [&*a_builds, &b_builds] {
                let accepted = BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
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
            let row = storage.record_at(&"mesh1.rpc.7".parse().unwrap()).await.unwrap();
            let _ = std::fs::remove_dir_all(&data_root);
            (a_identity, b_err, proving.found.into_inner().unwrap(), proving.spawned.into_inner().unwrap(), row)
        });
        let x = a_identity["node_id"].as_str().expect("A's identity names its node id").to_string();
        let deployment = a_identity["deployment_id"].as_str().expect("A's identity names its deployment").to_string();
        let b_err = b_err.expect_err("B's attempt fails where this provider launches nothing");
        assert!(b_err.contains("DeployRuntime") && !b_err.contains("name-held"), "B continues A's create past its claim and reaches DeployRuntime: {b_err}");
        assert_eq!(spawned.iter().map(|d| d.to_string()).collect::<Vec<_>>(), vec![deployment.clone()], "B launches A's exact deployment, never a new one");
        assert!(found.iter().any(|d| d.as_str() == deployment), "the provider was asked for A's exact deployment before anything was launched: {found:?}");
        assert_eq!(row, None, "B's create failed after its claim (this provider launches nothing) and withdrew that birth's claim, as every create failing after its claim does: {row:?}");
        let claims = cap.spans_named("rdm.node_admin.node.create.via-node-row");
        let rpc7: Vec<(String, String)> = claims.iter().filter(|s| s["attributes"]["subject"] == "mesh1.rpc.7" && s["attributes"]["moment"] == "claim").map(|s| (s["attributes"]["node_id"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
        assert_eq!(rpc7, vec![(x.clone(), "row-written".to_string()), (x.clone(), "row-held-by-node".to_string())], "A's claim is written and B's holds that same birth's row: {rpc7:?}");
        let settled = cap.spans_named("rdm.node_admin.node.resolve.via-claimed-allocation");
        assert_eq!(settled.len(), 1, "B settles the identity from A's claim once: {settled:#?}");
        let a = &settled[0]["attributes"];
        assert_eq!(
            (a["node_id"].as_str(), a["deployment_id"].as_str(), a["operation"].as_str(), a["attempt"].as_str(), a["outcome"].as_str()),
            (Some(x.as_str()), Some(deployment.as_str()), Some("node.create:mesh1.rpc.7"), Some("2"), Some("never-launched")),
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
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
            self.spawned.lock().unwrap().push(spec.deployment_id.clone());
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this provider launches nothing".into() })
        }
        async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            self.terminated.lock().unwrap().push(h.deployment_id.clone());
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
            DeploymentStatus::Unknown
        }
        async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
            let first = { let mut f = self.found.lock().unwrap(); f.push(spec.deployment_id.clone()); f.len() == 1 };
            Ok(match first {
                true => rdm_admin::deployment::provider::Found::Running(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(4242), start: Some(7), container: None, domain: Some("node-rows".into()) }),
                false => rdm_admin::deployment::provider::Found::NeverLaunched,
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
        use rdm_admin::build_state::{BuildAccepted, BuildFact, BuildStateAdapter, StepOutcome};
        let cap = capture();
        let (a_identity, b_err, terminated, spawned) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-launched-by-the-lost-{}", NodeId::mint()));
            let build = BuildId::parse(&rdm_core::fixture_crockford60("build-node-rows-8")).unwrap();
            let (a_builds, b_builds) = (Arc::new(MemoryBuildStateAdapter::new()), MemoryBuildStateAdapter::new());
            for builds in [&*a_builds, &b_builds] {
                let accepted = BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
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
        assert_eq!(terminated.iter().map(|d| d.to_string()).collect::<Vec<_>>(), vec![deployment.clone()], "the lost executor's runtime is stopped through its exact handle");
        assert_eq!(spawned.len(), 1, "{spawned:?}");
        assert_ne!(spawned[0].as_str(), deployment, "B launches a deployment of its own, never the lost executor's");
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

    /// Executor A runs attempt 1 of `node.create:<name>` in `build` through its claim and is lost inside
    /// `DeployRuntime` before it launches anything; its births' data dirs under `data_root`. Returns
    /// the identity A's AllocateIdentity receipt names.
    async fn lost_after_claim(storage: &Arc<MemoryNodesStorage>, build: &BuildId, name: &str, data_root: &std::path::Path) -> Value {
        use rdm_admin::build_state::{BuildFact, BuildStateAdapter, StepOutcome};
        let a_builds = Arc::new(MemoryBuildStateAdapter::new());
        let accepted = rdm_admin::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
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
    /// `DeployRuntime` with a deployment of its own. Must NOT happen: B starting the exited birth
    /// under A's node id, incarnation and deployment.
    #[test]
    fn a_create_whose_lost_executor_launched_its_claimed_birth_which_exited_decides_a_new_birth() {
        let cap = capture();
        let (a_identity, b_err, spawned) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-launched-exited-{}", NodeId::mint()));
            let build = BuildId::parse(&rdm_core::fixture_crockford60("build-node-rows-9")).unwrap();
            let a_identity = lost_after_claim(&storage, &build, "mesh1.rpc.9", &data_root).await;
            let b_builds = MemoryBuildStateAdapter::new();
            let accepted = rdm_admin::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
            rdm_admin::build_state::BuildStateAdapter::publish_accepted(&b_builds, &accepted).await.unwrap();
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            // A launched the birth and it exited: the provider proves it.
            let proving = Proving { exited: Mutex::new(Some(DeploymentId::parse(a_identity["deployment_id"].as_str().unwrap()).unwrap())), ..Proving::default() };
            let b_err = create_attempt_over(&b_records, &proving, &Unheard, &b_builds, &build, "mesh1.rpc.9", 2, &data_root).await;
            let _ = std::fs::remove_dir_all(&data_root);
            (a_identity, b_err, proving.spawned.into_inner().unwrap())
        });
        let x = a_identity["node_id"].as_str().unwrap().to_string();
        let deployment = a_identity["deployment_id"].as_str().unwrap().to_string();
        let b_err = b_err.expect_err("B's attempt fails where this provider launches nothing");
        assert!(b_err.contains("DeployRuntime") && !b_err.contains("name-held"), "B decides a new birth and reaches DeployRuntime: {b_err}");
        assert_eq!(spawned.len(), 1, "{spawned:?}");
        assert_ne!(spawned[0].as_str(), deployment, "B launches a deployment of its own, never the exited birth's again");
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
        inner: rdm_admin::deployment::process::ProcessDeploymentProvider,
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
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
            self.spawned.lock().unwrap().push(spec.deployment_id.clone());
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this provider launches nothing".into() })
        }
        async fn terminate(&self, h: &DeploymentHandle, mode: TerminationMode, deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            self.inner.terminate(h, mode, deadline).await
        }
        async fn inspect(&self, h: &DeploymentHandle, deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
            self.inner.inspect(h, deadline).await
        }
        async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, spec: &ResolvedNodeLaunch, deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
            self.inner.find(spec, deadline).await
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
    /// never launched and starting it under A's node id, incarnation and deployment.
    #[test]
    fn a_create_whose_lost_executor_launched_a_birth_that_exited_before_its_runtime_record_decides_a_new_birth() {
        let cap = capture();
        let (a_identity, b_err, spawned) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-exited-before-record-{}", NodeId::mint()));
            let build = BuildId::parse(&rdm_core::fixture_crockford60("build-node-rows-11")).unwrap();
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
            let accepted = rdm_admin::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
            rdm_admin::build_state::BuildStateAdapter::publish_accepted(&b_builds, &accepted).await.unwrap();
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let on_disk = OnDisk { inner: rdm_admin::deployment::process::ProcessDeploymentProvider::new(), spawned: Mutex::default() };
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
        assert_ne!(spawned[0].as_str(), deployment, "B launches a deployment of its own, never the exited birth's again");
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
            let build = BuildId::parse(&rdm_core::fixture_crockford60("build-node-rows-12")).unwrap();
            let a_identity = lost_after_claim(&storage, &build, "mesh1.rpc.12", &data_root).await;
            // A launched the birth: its process runs, and the provider's launch record names it.
            let dir = data_root.join(format!("mesh1.rpc.12-{}", a_identity["node_id"].as_str().unwrap()));
            let spec = ResolvedNodeLaunch { secrets: Default::default(), withhold: Vec::new(),
                node: "mesh1.rpc.12".parse().unwrap(),
                deployment_id: DeploymentId::parse(a_identity["deployment_id"].as_str().unwrap()).unwrap(),
                executable: "/bin/sleep".into(),
                args: vec!["300".into()],
                env: Default::default(),
                data_dir: dir.clone(),
                transport: "127.0.0.1:0".parse().unwrap(),
                listeners: vec![],
            };
            let launcher = rdm_admin::deployment::process::ProcessDeploymentProvider::new();
            let handle = launcher.spawn(&spec, rdm_admin::deployment::deadline::Deadline::step()).await.expect("A launches its birth");
            let record: Value = serde_json::from_str(&std::fs::read_to_string(dir.join("deployment.json")).expect("the launch record")).unwrap();
            let b_builds = MemoryBuildStateAdapter::new();
            let accepted = rdm_admin::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
            rdm_admin::build_state::BuildStateAdapter::publish_accepted(&b_builds, &accepted).await.unwrap();
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let on_disk = OnDisk { inner: rdm_admin::deployment::process::ProcessDeploymentProvider::new(), spawned: Mutex::default() };
            let b_err = create_attempt_executing("/bin/sleep", &b_records, &on_disk, &Unheard, &b_builds, &build, "mesh1.rpc.12", 2, &data_root).await;
            // The process B stopped is gone to every provider of the domain.
            let after = on_disk.inner.find(&spec, rdm_admin::deployment::deadline::Deadline::step()).await;
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
        assert_ne!(spawned[0].as_str(), deployment, "B launches a deployment of its own, never the lost executor's");
        assert!(matches!(after.unwrap(), rdm_admin::deployment::provider::Found::Exited(_)), "A's process was stopped through its record");
        let stopped = cap.spans_named("rdm.node_admin.deployment.reject.via-lost-executor");
        assert_eq!(stopped.iter().map(|s| s["attributes"]["deployment_id"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>(), vec![deployment.clone()], "{stopped:#?}");
        eprintln!("trace {}: via-claimed-allocation node_id={x} outcome=launched-by-lost-executor; via-lost-executor deployment_id={deployment} pid={}", settled[0]["trace_id"].as_str().unwrap_or(""), handle.pid.unwrap());
    }

    /// CONTRACT (the lost-executor rule stops only a birth that never joined; a recoverable refusal
    /// over a terminated member): A claims `mesh1.rpc.10` in attempt 1 and is lost; the provider holds
    /// a running runtime of the exact deployment A's claim names, and that birth has joined fabric
    /// the topology view. B, holding none of A's receipts past the claim, refuses by name at
    /// `AllocateIdentity`: nothing is stopped, the claim is not withdrawn, nothing is minted or
    /// launched, and A's birth holds the row. Must NOT happen: B stopping a joined member on `find`
    /// alone.
    #[test]
    fn a_create_whose_lost_executor_launched_a_birth_that_joined_is_refused_by_name_and_stops_nothing() {
        let cap = capture();
        let (a_identity, b_err, terminated, spawned, row) = cap.run(async {
            let storage = Arc::new(MemoryNodesStorage::default());
            let data_root = std::env::temp_dir().join(format!("node-rows-launched-joined-{}", NodeId::mint()));
            let build = BuildId::parse(&rdm_core::fixture_crockford60("build-node-rows-10")).unwrap();
            let a_identity = lost_after_claim(&storage, &build, "mesh1.rpc.10", &data_root).await;
            let b_builds = MemoryBuildStateAdapter::new();
            let accepted = rdm_admin::build_state::BuildAccepted { build_id: build.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 };
            rdm_admin::build_state::BuildStateAdapter::publish_accepted(&b_builds, &accepted).await.unwrap();
            let (b_records, _b_view) = writer_of("mesh1.admin.2", &view(), &storage);
            let provider = LaunchedByTheLost::default();
            let b_err = create_attempt_over(&b_records, &provider, &Joined, &b_builds, &build, "mesh1.rpc.10", 2, &data_root).await;
            let row = storage.record_at(&"mesh1.rpc.10".parse().unwrap()).await.unwrap();
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
/// `mesh1.admin.2`, writes it (R13): its node.delete of the exited birth (`node.delete:<path>`)
/// writes the row `Deleted` inside its `NodeDeleted` step, and its node.create at the same
/// `path.name` (`node.create:<path>`, over the exited birth: a recovery's claim) claims it, `before`
/// or after that `Deleted` row. Returns the
/// row at `mesh1.rpc.1`, the exited node, the new one, and the nodes the tags index names under the
/// exited row's label `blue` once the claim is written (nothing runs after it).
async fn recovered(new_claimed_first: bool) -> (Option<NodeRow>, NodeId, NodeId, std::collections::BTreeSet<NodeId>) {
    let (delete, create) = ("node.delete:mesh1.rpc.1", "node.create:mesh1.rpc.1");
    let storage = Arc::new(MemoryNodesStorage::default());
    let mp = mesh_primary(&storage);
    let exited = held(&mp, "mesh1.rpc.1").await;
    // The exited birth's row, as its create claimed it.
    storage.write_record(&NodeRow { topology_storage_provider: None, name: "mesh1.rpc.1".parse().unwrap(), node_id: exited.node_id.clone(), status: NodeStatus::Ready, config: tier("gold"), tags: ["blue".to_string()].into(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
    let born = NodeId::mint();
    if new_claimed_first {
        mp.rows.claim(&exited.name, &born, create, None, None, ClaimFor::Recovery).await.expect_err("the exited node's live row holds the name");
    }
    mp.rows.deleted(&exited, delete).await.expect("the executor writes the Deleted row inside NodeDeleted");
    mp.rows.claim(&exited.name, &born, create, None, None, ClaimFor::Recovery).await.expect("the Deleted row holds no name");
    (storage.record_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap(), exited.node_id, born, storage.tagged("blue").await.unwrap())
}

/// CONTRACT (node-recovery.md step 3: "the node's mesh-primary writes the row with status Deleted";
/// step 4: "Its config is seeded from the Deleted row at that path.name"; proposal
/// node-properties-own-commands.md §8: the claim "seeds config and tags from that Deleted row and
/// writes the tags index entries under the successor's id"): a recovery's `NodeDeleted` leaves the
/// exited node's row `Deleted`, so the new node created at the same `path.name` claims it, seeded
/// from it: the new node id, its status, the exited node's config and tags, the tags indexed under
/// the new id once the claim is written, nothing after it.
/// One `delete.via-node-row` span (`row-deleted`) and one `create.via-node-row` span
/// (`row-seeded`, seeded from the exited node) name the two writes. The writer is the recovery's
/// executor, the mesh's primary (R13), and its claim writes the new node `Pending`.
#[test]
fn a_recovered_nodes_row_is_deleted_and_the_new_node_at_its_path_name_claims_it_seeded() {
    let cap = capture();
    let (row, exited, born, blue) = cap.run(recovered(false));
    assert_eq!(sans_revision(row), Some(NodeRow { topology_storage_provider: None, name: "mesh1.rpc.1".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Starting, config: tier("gold"), tags: ["blue".to_string()].into(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }), "the new node holds the row, seeded from the exited node's config and tags");
    assert_eq!(blue, [born.clone()].into(), "the claim indexed the label under the new node's id, the exited node's left with its Deleted row");
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
    let (row, exited, born, blue) = cap.run(recovered(true));
    assert_eq!(sans_revision(row), Some(NodeRow { topology_storage_provider: None, name: "mesh1.rpc.1".parse().unwrap(), node_id: born.clone(), status: NodeStatus::Starting, config: tier("gold"), tags: ["blue".to_string()].into(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }), "the new node holds the row once the name is free, with the exited node's config and tags");
    assert_eq!(blue, [born.clone()].into(), "the label is indexed under the new node's id only");
    let created = cap.spans_named("rdm.node_admin.node.create.via-node-row");
    assert_eq!(created.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(born.as_str()), Some("name-held")), (Some(born.as_str()), Some("row-seeded"))], "{created:#?}");
    assert_eq!(created[1]["attributes"]["seeded_from"].as_str(), Some(exited.as_str()));
}

/// CONTRACT (fabric-recovery.md step 3, Q148: "a delete that finds another birth at the path writes
/// nothing"): a recovery's node.delete of the lost birth replayed after its re-create claimed the
/// `path.name` is refused `name-held`: the new node keeps its row, its name and its tags, and the lost
/// birth's record is not written again. Must NOT happen: the replayed delete releases the name the
/// new node holds or writes its row.
#[test]
fn a_replayed_delete_of_the_lost_birth_writes_nothing_against_its_replacement() {
    let cap = capture();
    let (row, exited, born, blue) = cap.run(async {
        let delete = "node.delete:mesh1.rpc.1";
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let exited = held(&mp, "mesh1.rpc.1").await;
        storage.write_record(&NodeRow { topology_storage_provider: None, name: "mesh1.rpc.1".parse().unwrap(), node_id: exited.node_id.clone(), status: NodeStatus::Ready, config: tier("gold"), tags: ["blue".to_string()].into(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        mp.rows.deleted(&exited, delete).await.expect("the recovery's delete writes the Deleted row");
        let born = NodeId::mint();
        mp.rows.claim(&exited.name, &born, "node.create:mesh1.rpc.1", None, None, ClaimFor::Recovery).await.expect("the re-create claims the name");
        let replayed = mp.rows.deleted(&exited, delete).await;
        assert!(matches!(replayed, Err(RowRefused::NameHeld(ref h)) if h.by == born), "the replayed delete is refused by the replacement's claim: {replayed:?}");
        assert_eq!(storage.name_claim(&exited.name).await.unwrap(), Some(born.clone()), "the replacement keeps the name");
        (storage.record_at(&exited.name).await.unwrap(), exited.node_id, born, storage.tagged("blue").await.unwrap())
    });
    assert_eq!(row.as_ref().map(|r| (&r.node_id, r.status)), Some((&born, NodeStatus::Starting)), "the replacement's row stands: {row:?}");
    assert_eq!(blue, [born.clone()].into(), "the label stays indexed under the replacement");
    let deleted = cap.spans_named("rdm.node_admin.node.delete.via-node-row");
    assert_eq!(deleted.iter().map(|s| (s["attributes"]["node_id"].as_str(), s["attributes"]["outcome"].as_str())).collect::<Vec<_>>(), vec![(Some(exited.as_str()), Some("row-deleted")), (Some(exited.as_str()), Some("name-held"))], "{deleted:#?}");
}

/// CONTRACT (e12.s41 / crit:e12.s41.1; proposal node-primary-update-fabric-mesh-and-node-type.md
/// Part A): a node created with a free-text name (`mesh1.broker.north`, where `path` is
/// `"mesh1.broker"`, `name` is `"north"`, and `node_type` is `"broker"`) round-trips `name`, `path`
/// and `node_type` through:
/// 1. `create` (claiming `mesh1.broker.north` and installing the birth into the view),
/// 2. `get` (`views::nodes_answer` by id and by mesh),
/// 3. `stop`/`start` (stopped on its row and started back to `Ready`),
/// 4. `recovery` (`NodeDeleted` writing the `Deleted` row at `mesh1.broker.north`, then the
///    recovered birth claiming `mesh1.broker.north` with `ClaimFor::Recovery` and inheriting
///    config, tags and `node_type`), and
/// 5. `replace` (the predecessor renamed to `mesh1.broker.north.old` with `name = "north.old"`, and
///    the successor created at `mesh1.broker.north`, the name it released, its claim a successor's
///    (`ClaimFor::Successor`), inheriting config, tags and `node_type` from `north.old`, whose row is
///    marked inherited and whose labels leave the active tags index).
#[test]
fn a_node_created_with_a_free_text_name_round_trips_name_path_and_node_type_through_create_get_stop_start_recovery_and_replace() {
    use rdm_core::filter::Filter;
    use rdm_core::status::StatusReply;
    let cap = capture();
    cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let loads: rdm_admin::views::Loads = Arc::new(BTreeMap::new);
        // The admin's atlas holds every record its writer holds (Q203).
        let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
        mp.rows.written().feeds(atlas.clone());

        // 1. Create: `mesh1.broker.north` with free-text name `north`, path `mesh1.broker`, node_type `broker`.
        let north_key: PathName = "mesh1.broker.north".parse().expect("free-text node name parses");
        assert_eq!((north_key.path().as_str(), north_key.name().as_str(), north_key.node_type()), ("mesh1.broker", "north", "broker"));
        let north = node("mesh1.broker.north", false, false).with_node_type("broker").expect("valid node_type");
        assert_eq!((north.path().as_str(), north.node_name().as_str(), north.node_type()), ("mesh1.broker", "north", "broker"));
        let (_, claimed_type) = mp
            .rows
            .claim_with_node_type(&north_key, &north.node_id, "broker", "node.create:mesh1.broker.north", None, None, ClaimFor::Create)
            .await
            .expect("the free-text name is claimed");
        assert_eq!(claimed_type, "broker");
        // The node runs and takes its config and tags: its record, written by its mesh-primary.
        mp.rows
            .written()
            .update(&*storage, &north.node_id, None, |r| {
                r.status = NodeStatus::Ready;
                r.config = tier("gold");
                r.tags = ["blue".to_string()].into();
            })
            .await
            .unwrap();
        let created_row = storage.record_at(&north_key).await.unwrap().expect("row at mesh1.broker.north");
        assert_eq!((created_row.path().as_str(), created_row.node_name().as_str(), created_row.node_type()), ("mesh1.broker", "north", "broker"));
        let north_for_view = north.clone();
        install(&mp.records, &mp.view, move |v| v.nodes.push(north_for_view)).await;

        // 2. Get: `nodes_answer` round-trips `name`, `path` and `node_type`.
        let StatusReply::Nodes { nodes } = rdm_admin::views::nodes_answer(&me, &*mp.view.read().await, &loads, &atlas, &Filter::new().eq("id", [north.node_id.to_string()])).await else {
            panic!("expected StatusReply::Nodes");
        };
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            (nodes[0].name.to_string().as_str(), nodes[0].path().as_str(), nodes[0].node_name().as_str(), nodes[0].node_type(), &nodes[0].config, &nodes[0].tags),
            ("mesh1.broker.north", "mesh1.broker", "north", "broker", &tier("gold"), &["blue".to_string()].into())
        );

        // 3. Stop / Start: stopping and starting preserve `name`, `path` and `node_type`; the read
        //    answers the status the row takes (Q233), written by its mesh-primary.
        let north_id = north.node_id.clone();
        install(&mp.records, &mp.view, move |v| {
            let n = v.nodes.iter_mut().find(|n| n.node_id == north_id).unwrap();
            n.status = NodeStatus::Leaving;
            n.stopped = true;
            n.routable = false;
        })
        .await;
        mp.rows.written().update(&*storage, &north.node_id, None, |r| r.status = NodeStatus::Stopped).await.unwrap();
        let StatusReply::Nodes { nodes: stopped } = rdm_admin::views::nodes_answer(&me, &*mp.view.read().await, &loads, &atlas, &Filter::new().eq("id", [north.node_id.to_string()])).await else {
            panic!("expected StatusReply::Nodes");
        };
        assert!(stopped[0].stopped);
        assert_eq!((stopped[0].path().as_str(), stopped[0].node_name().as_str(), stopped[0].node_type()), ("mesh1.broker", "north", "broker"));

        let north_id = north.node_id.clone();
        install(&mp.records, &mp.view, move |v| {
            let n = v.nodes.iter_mut().find(|n| n.node_id == north_id).unwrap();
            n.status = NodeStatus::Ready;
            n.stopped = false;
            n.routable = true;
        })
        .await;
        mp.rows.written().update(&*storage, &north.node_id, None, |r| r.status = NodeStatus::Ready).await.unwrap();
        let StatusReply::Nodes { nodes: started } = rdm_admin::views::nodes_answer(&me, &*mp.view.read().await, &loads, &atlas, &Filter::new().eq("id", [north.node_id.to_string()])).await else {
            panic!("expected StatusReply::Nodes");
        };
        assert!(!started[0].stopped);
        assert_eq!((started[0].path().as_str(), started[0].node_name().as_str(), started[0].node_type()), ("mesh1.broker", "north", "broker"));

        // 4. Recovery: `NodeDeleted` writes the Deleted row at `mesh1.broker.north`, and the
        //    recovered node claims `mesh1.broker.north` with `ClaimFor::Recovery`, copying
        //    `node_type`, `config` and `tags`.
        mp.rows.deleted(&north, "node.delete:mesh1.broker.north").await.expect("Deleted row written at mesh1.broker.north");
        let recovered_north = node("mesh1.broker.north", false, false);
        let (_, recovered_type) = mp
            .rows
            .claim_with_node_type(&north_key, &recovered_north.node_id, "", "node.create:mesh1.broker.north", None, None, ClaimFor::Recovery)
            .await
            .expect("recovery claims mesh1.broker.north");
        assert_eq!(recovered_type, "broker", "recovery copies the exited node's node_type");
        assert_eq!(mp.rows.node_type_of(&recovered_north.node_id, &north_key), "broker");
        let recovered_row = storage.record_at(&north_key).await.unwrap().expect("recovered row at mesh1.broker.north");
        assert_eq!(
            (recovered_row.node_id.clone(), recovered_row.path().as_str(), recovered_row.node_name().as_str(), recovered_row.node_type(), &recovered_row.config, &recovered_row.tags),
            (recovered_north.node_id.clone(), "mesh1.broker", "north", "broker", &tier("gold"), &["blue".to_string()].into())
        );
        let rec_for_view = recovered_north.clone();
        let exited_id = north.node_id.clone();
        install(&mp.records, &mp.view, move |v| {
            v.nodes.retain(|n| n.node_id != exited_id);
            v.nodes.push(rec_for_view);
        })
        .await;

        // 5. Replace (node-replace.md steps 2-3): the predecessor moves to `mesh1.broker.north.old`
        //    (`north.old`), and the successor is created at `mesh1.broker.north`, the name it released,
        //    its claim a successor's: it inherits `node_type` from the retained row, and its `config` and
    //    `tags` are its Build entry's (Q305: tier silver, never the retained row's gold).
        let old_key = north_key.renamed();
        assert_eq!((old_key.to_string().as_str(), old_key.path().as_str(), old_key.name().as_str(), old_key.node_type()), ("mesh1.broker.north.old", "mesh1.broker", "north.old", "broker"));
        assert_eq!(north_key.with_name("north.old").unwrap(), old_key);

        let mut pred_stopped = recovered_north.clone();
        pred_stopped.status = NodeStatus::Leaving;
        pred_stopped.stopped = true;
        let old_row = mp.rows.rename_row(&pred_stopped, &old_key).await.expect("predecessor moves to mesh1.broker.north.old");
        assert_eq!((old_row.path().as_str(), old_row.node_name().as_str(), old_row.node_type()), ("mesh1.broker", "north.old", "broker"));
        assert_eq!(storage.record_at(&north_key).await.unwrap(), None, "mesh1.broker.north is free for the successor's claim");

        let mut succ = node("mesh1.broker.north", false, false);
        succ.status = NodeStatus::Pending;
        let succ_tags: std::collections::BTreeSet<String> = ["blue".to_string()].into();
        mp.rows.claim_spec(&north_key, &succ.node_id, "node.create:mesh1.broker.north", None, None, &succ_tags, &tier("silver"), None, ClaimFor::Successor).await.expect("the successor claims mesh1.broker.north");
        let succ_row = storage.record_at(&north_key).await.unwrap().expect("successor row at mesh1.broker.north");
        assert_eq!(
            (succ_row.node_id.clone(), succ_row.path().as_str(), succ_row.node_name().as_str(), succ_row.node_type(), &succ_row.config, &succ_row.tags),
            (succ.node_id.clone(), "mesh1.broker", "north", "broker", &tier("silver"), &succ_tags)
        );
        assert_eq!(mp.rows.node_type_of(&succ.node_id, &north_key), "broker", "replace copies predecessor node_type to successor");
        let retained_old = storage.record_at(&old_key).await.unwrap().expect("retained predecessor row at mesh1.broker.north.old");
        assert_eq!((retained_old.node_name().as_str(), retained_old.inherited_by.as_ref()), ("north.old", Some(&succ.node_id)));
        assert_eq!(storage.tagged("blue").await.unwrap(), [succ.node_id.clone()].into(), "active tag index holds only the successor after its claim");

        let mut succ_ready = succ.clone();
        succ_ready.name = north_key.clone();
        succ_ready.status = NodeStatus::Ready;
        succ_ready.stopped = false;
        let mut pred_old = pred_stopped.clone();
        pred_old.name = old_key.clone();
        let pred_id = pred_old.node_id.clone();
        install(&mp.records, &mp.view, move |v| {
            v.nodes.retain(|n| n.node_id != pred_id);
            v.nodes.push(pred_old);
            v.nodes.push(succ_ready);
        })
        .await;

        let StatusReply::Nodes { nodes: mesh_nodes } = rdm_admin::views::nodes_answer(&me, &*mp.view.read().await, &loads, &atlas, &Filter::new().eq("mesh", ["mesh1"])).await else {
            panic!("expected StatusReply::Nodes");
        };
        let active = mesh_nodes.iter().find(|n| n.name == north_key).expect("active successor at mesh1.broker.north");
        assert_eq!((active.node_id.clone(), active.path().as_str(), active.node_name().as_str(), active.node_type()), (succ.node_id.clone(), "mesh1.broker", "north", "broker"));
        let retained = mesh_nodes.iter().find(|n| n.name == old_key).expect("retained predecessor at mesh1.broker.north.old");
        assert_eq!((retained.node_id.clone(), retained.path().as_str(), retained.node_name().as_str(), retained.node_type()), (pred_stopped.node_id.clone(), "mesh1.broker", "north.old", "broker"));
    });
}

/// CONTRACT (e12.s41 / crit:e12.s41.2; proposal node-primary-update-fabric-mesh-and-node-type.md
/// §A.1.2, §A.2.3):
/// 1. `node_type` at `node.create` normalizes `""` to `"node"` (`DEFAULT_NODE_TYPE`) and refuses
///    any value outside the shared identifier grammar (`1..=64` of `a-z 0-9 - _ . ~`, no leading
///    `.`, whitespace refused) by name (`node_type`), writing nothing.
/// 2. `node_type` is immutable after creation: `node.update` of `node_type` is refused by name
///    (`node_type`), leaving the node and its row untouched.
#[test]
fn node_update_of_node_type_and_invalid_node_type_are_refused_by_name() {
    use rdm_core::status::{normalize_node_type, node_type_refusal, node_type_update_refusal, DEFAULT_NODE_TYPE};
    let cap = capture();
    cap.run(async {
        // 1. Empty string normalizes to "node"; valid identifiers pass; invalid values are refused naming "node_type".
        assert_eq!(normalize_node_type(""), DEFAULT_NODE_TYPE);
        assert_eq!(normalize_node_type("broker"), "broker");
        for valid in ["node", "broker", "node-admin", "rpc-node", "compute_1", "v1.2~rc"] {
            assert_eq!(node_type_refusal(valid), None, "{valid} is within the node_type grammar");
        }
        let over_cap = "a".repeat(65);
        for invalid in [" ", " broker", "broker ", "Broker", ".broker", "broker/1", over_cap.as_str()] {
            let reason = node_type_refusal(&normalize_node_type(invalid)).expect("invalid node_type is refused");
            assert!(reason.contains("node_type"), "refusal names node_type: {reason}");
        }

        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let north_key: PathName = "mesh1.broker.north".parse().expect("free-text node name parses");

        // 2. Claiming a node with an invalid `node_type` is refused by name (`node_type`) and writes no row.
        let rejected_id = NodeId::mint();
        let err = mp
            .rows
            .claim_with_node_type(&north_key, &rejected_id, "Broker", "node.create:mesh1.broker.north", None, None, ClaimFor::Create)
            .await
            .expect_err("uppercase node_type is refused at create");
        assert!(err.to_string().contains("node_type"), "create refusal names node_type: {err}");
        assert_eq!(storage.record_at(&north_key).await.unwrap(), None, "nothing is written when node_type is refused");

        let ws_err = Node::allocated(north_key.clone()).with_node_type(" broker").expect_err("whitespace node_type is refused");
        assert!(ws_err.contains("node_type"), "Node::with_node_type refusal names node_type: {ws_err}");

        // 3. A node created with an empty `node_type` normalizes to `"node"`.
        let default_key: PathName = "mesh1.rpc.east".parse().expect("free-text node name parses");
        let default_id = NodeId::mint();
        let (_, normalized) = mp
            .rows
            .claim_with_node_type(&default_key, &default_id, "", "node.create:mesh1.rpc.east", None, None, ClaimFor::Create)
            .await
            .expect("empty node_type normalizes to default");
        assert_eq!(normalized, "node");
        assert_eq!(mp.rows.node_type_of(&default_id, &default_key), "node");

        // 4. `node.update` of `node_type` is refused by name (`node_type`), leaving the held `node_type` untouched.
        let north_id = NodeId::mint();
        mp.rows
            .claim_with_node_type(&north_key, &north_id, "broker", "node.create:mesh1.broker.north", None, None, ClaimFor::Create)
            .await
            .expect("valid node_type is claimed");
        for attempted in ["rpc-node", "broker", "node", "Broker"] {
            let err = mp
                .rows
                .update_node_type(&north_key, &north_id, attempted)
                .await
                .expect_err("node.update of node_type is refused by name");
            let msg = err.to_string();
            assert!(msg.contains("node_type") && msg.contains("node.update"), "refusal names node_type and node.update: {msg}");
        }
        assert_eq!(mp.rows.node_type_of(&north_id, &north_key), "broker", "held node_type is unchanged after refused node.update");

        let mut north_node = Node::allocated(north_key).with_node_type("broker").unwrap();
        let update_err = north_node.update_node_type("rpc-node").expect_err("Node::update_node_type is refused");
        assert!(update_err.contains("node_type") && update_err.contains("immutable"), "refusal names node_type and immutable: {update_err}");
        assert_eq!(north_node.node_type(), "broker");
        let contract_msg = node_type_update_refusal("broker", "rpc-node");
        assert!(contract_msg.contains("node_type") && contract_msg.contains("node.update"), "{contract_msg}");
    });
}


/// CONTRACT (build.md: an os-stopped executor that resumes after the next attempt is claimed "is a
/// deleted birth whose writes every reader refuses"; Q109: "a row that does not follow the fold is
/// ignored by every reader, refused by no storage", "the row's key names its writer's birth";
/// storage-whole-records.md: no write reads storage first): a birth that writes its own record (a
/// mesh-primary writes its own row) is held suspended while its mesh's writer `mesh1.admin.2` writes
/// that record `Deleted`; resumed, it puts the record whole from its older copy, and storage keeps the
/// version as written. Every record names the birth that wrote it. The writer's recovery claim at the
/// path.name reads that version, written by a birth it holds deleted, and ignores it for the `Deleted`
/// record it holds: the re-created birth names the deleted one its predecessor and inherits its tags.
/// A reader of the mesh's records ignores the deleted birth's own version, and keeps the record of
/// another node that birth wrote before its delete. Each ignored version is named on
/// `rdm.node_admin.node.reject.via-deleted-birth`.
/// Must NOT happen: the resumed birth's version read as its record (the re-created birth with no
/// predecessor and no tags, or the deleted birth read live), a write that reads storage first, or a
/// record of another node dropped because a since-deleted birth wrote it.
#[test]
fn a_resumed_deleted_births_own_record_write_is_ignored_on_read() {
    use rdm_admin::storage::BirthRef;
    let cap = capture();
    let (stored, successor, folded, other, x, other_id) = cap.run(async {
        let storage = Arc::new(MemoryNodesStorage::default());
        let mp = mesh_primary(&storage);
        let me = held(&mp, "mesh1.admin.2").await;
        mp.rows.written().writes_as(BirthRef { node_id: me.node_id.clone(), incarnation_id: me.incarnation_id.clone().unwrap() });
        let victim = held(&mp, "mesh1.rpc.1").await;
        let x = BirthRef { node_id: victim.node_id.clone(), incarnation_id: victim.incarnation_id.clone().unwrap() };
        // The birth writes its own record, and another node's, while it is its mesh's writer.
        let resumed = rdm_admin::node_rows::WrittenRows::default();
        resumed.writes_as(x.clone());
        let mut own = NodeRow { tags: ["red".to_string()].into(), ..row_of(&victim) };
        own.birth_mut(&x.incarnation_id, &victim.name);
        resumed.update(&*storage, &victim.node_id, Some(own), |_| ()).await.expect("the birth's own record is written");
        let other = NodeRow::new("mesh1.rpc.9".parse().unwrap(), NodeId::mint(), NodeStatus::Ready);
        resumed.update(&*storage, &other.node_id, Some(other.clone()), |_| ()).await.expect("another node's record is written");
        // Its mesh's writer deletes it; resumed, it puts its own record whole from its older copy.
        mp.rows.deleted(&victim, "node.delete:mesh1.rpc.1").await.expect("the mesh's writer writes the Deleted record");
        resumed.update(&*storage, &victim.node_id, None, |r| r.config.insert("k".into(), "v".into())).await.expect("storage stores what it is given");
        let stored = storage.node(&victim.node_id).await.unwrap().expect("the record");
        // The recovery's claim at the path.name.
        let born = NodeId::mint();
        mp.rows.claim(&victim.name, &born, "node.create:mesh1.rpc.1", None, None, ClaimFor::Recovery).await.expect("the Deleted record holds no name");
        let successor = storage.node(&born).await.unwrap().expect("the re-created birth's record");
        let folded = rdm_admin::node_rows::fold_records("mesh-records", storage.nodes("mesh1").await.unwrap());
        (stored, successor, folded, other.node_id.clone(), x, other.node_id)
    });
    let _ = other;
    assert_eq!((stored.status, stored.writer.as_ref()), (NodeStatus::Ready, Some(&x)), "storage keeps the resumed birth's version as written, naming its writer: {stored:?}");
    assert_eq!((successor.predecessor.as_ref(), successor.tags.clone()), (Some(&x), ["red".to_string()].into()), "the recovery's claim read the Deleted record its writer holds: the re-created birth names the deleted one its predecessor and inherits its tags: {successor:?}");
    assert!(folded.iter().all(|r| r.node_id != x.node_id), "a reader of the mesh's records ignores the deleted birth's own version: {folded:#?}");
    assert!(folded.iter().any(|r| r.node_id == other_id), "the record of another node the deleted birth wrote before its delete is read: {folded:#?}");
    let spans = cap.spans_named("rdm.node_admin.node.reject.via-deleted-birth");
    let writer = format!("{}@{}", x.node_id, x.incarnation_id.as_str());
    let reads: Vec<(Option<&str>, Option<&str>, Option<&str>, Option<&str>)> = spans.iter().map(|s| (s["attributes"]["read"].as_str(), s["attributes"]["subject"].as_str(), s["attributes"]["writer"].as_str(), s["attributes"]["outcome"].as_str())).collect();
    assert_eq!(reads, vec![(Some("writer-record"), Some("mesh1.rpc.1"), Some(writer.as_str()), Some("ignored")), (Some("mesh-records"), Some("mesh1.rpc.1"), Some(writer.as_str()), Some("ignored"))], "{spans:#?}");
    for s in &spans {
        eprintln!("trace {}: rdm.node_admin.node.reject.via-deleted-birth read={} writer={writer} outcome=ignored", s["trace_id"].as_str().unwrap_or(""), s["attributes"]["read"].as_str().unwrap_or(""));
    }
}
