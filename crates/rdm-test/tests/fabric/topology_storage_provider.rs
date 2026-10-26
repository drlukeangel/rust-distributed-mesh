//! product=mesh, feature=storage, subfeature=topology-storage-provider, rung=MM, provider=process.
//!
//! The fabric's `topology_storage_provider` (storage.md §2; i149): the registered name of the backend
//! RDM's five record stores (fabric, meshes, nodes, connections, builds) are written to. It is one value
//! for the fabric, on the Fabric row beside `previous_topology_storage_providers`; the same name on a
//! node's row is the provider that node writes. Day 0 takes it from `RDM_TOPOLOGY_STORAGE_PROVIDER`,
//! `file` when unset; a change is a Build of the complete topology naming the new value, and a Build
//! that omits it leaves it unchanged (Q166).
//!
//! Every estate here runs the testkit's faulted node-admin and rpc node, each registering RDM's `file`
//! backend and the testkit's second provider, [`TOPOLOGY_STORAGE_TARGET`], a file backend at a place of
//! its own beside the fabric's store dir (`rdm_test::test_app::topology_storage_dir`).

use rdm_client::{CallEnd, Change, FabricDesired, FabricView, MeshDesired, NodeStatus, NodeView, PathName};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use rdm_test::test_app::{topology_storage_dir, TOPOLOGY_STORAGE_TARGET};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(180);
/// How long a storage change's step is waited for once its Build is accepted.
const STEP: Duration = Duration::from_secs(120);
/// RDM's own file backend, registered by every RDM binary.
const FILE: &str = "file";
/// A name no executable of the testkit registers.
const UNREGISTERED: &str = "no-such-provider";
/// The five record stores, one append-only file each on a provider's fabric root (storage.md §2).
const STORES: [&str; 5] = [
    rdm_admin::store_root::FABRIC_FILE,
    rdm_admin::store_root::MESHES_FILE,
    rdm_admin::store_root::NODES_FILE,
    rdm_admin::store_root::CONNECTIONS_FILE,
    rdm_admin::store_root::BUILDS_FILE,
];

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "storage".into(), subfeature: "topology-storage-provider".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

/// A fabric of the faulted node-admin and the testkit rpc node, its Day-0 admin given `env`.
async fn estate_with(test: &str, env: &[(&str, &str)]) -> Estate {
    let sha = candidate_sha();
    Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &[], env).await.expect("the faulted-admin binding set is accepted")
}

/// Two meshes: mesh1 with two node-admins and an rpc node, mesh2 with one of each.
fn two_meshes() -> FabricDesired {
    FabricDesired {
        fabric: "fabric1".into(),
        meshes: vec![
            MeshDesired { name: "mesh1".into(), node_admin: 2, rpc_node: 1, broker: 0, gateway: 0, compute: 0 },
            MeshDesired { name: "mesh2".into(), node_admin: 1, rpc_node: 1, broker: 0, gateway: 0, compute: 0 },
        ],
    }
}

/// The names [`two_meshes`] holds.
fn two_mesh_names() -> BTreeSet<String> {
    ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh2.admin.1", "mesh2.rpc.1"].iter().map(|n| n.to_string()).collect()
}

/// The estate grown to [`two_meshes`], every node ready-for-traffic and the fabric ready-for-traffic.
async fn two_mesh_estate(test: &str, env: &[(&str, &str)]) -> (Estate, Vec<NodeView>) {
    let mut estate = estate_with(test, env).await;
    estate.reconcile(&two_meshes()).await;
    let views = estate.settles_to_named(&two_mesh_names(), SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    (estate, views)
}

/// Each node's birth: `path.name` → (node id, incarnation).
fn births(views: &[NodeView]) -> BTreeMap<String, (String, Option<String>)> {
    views.iter().map(|v| (v.name.to_string(), (v.node_id.to_string(), v.incarnation_id.as_ref().map(|i| i.as_str().to_string())))).collect()
}

/// The complete topology the fabric holds now, `topology_storage_provider` set to `provider`: the
/// storage change's request (storage.md §2 "Changing it": the same meshes and the new value).
async fn naming_provider(estate: &Estate, provider: Option<&str>) -> Change {
    match estate.shape_change(&two_meshes()).await {
        Change::Reconcile { based_on, nodes, .. } => Change::Reconcile { based_on, nodes, topology_storage_provider: provider.map(String::from) },
        other => panic!("the complete-topology request is a Reconcile: {other:?}"),
    }
}

/// The file provider's fabric root of `estate`, and the testkit target's root of the same fabric.
fn roots(estate: &Estate) -> (PathBuf, PathBuf) {
    let file_root = estate.fabric_store_root();
    let fabric_id = file_root.file_name().expect("the root is named by its fabric id").to_owned();
    let target = topology_storage_dir(file_root.parent().expect("the root is under the store dir")).join(fabric_id);
    (file_root, target)
}

/// Every line of each of the five stores on the provider root `root`, in file order.
fn store_lines(root: &Path) -> BTreeMap<&'static str, Vec<String>> {
    STORES.iter().map(|f| (*f, std::fs::read_to_string(root.join(f)).map(|t| t.lines().map(String::from).collect()).unwrap_or_default())).collect()
}

/// `source`'s lines, store by store, are a prefix of `destination`'s: every record and its history
/// copied in source order, nothing missing and nothing reordered (storage.md §2 "Copy").
fn history_carried(source: &BTreeMap<&'static str, Vec<String>>, destination: &BTreeMap<&'static str, Vec<String>>) {
    for (store, lines) in source {
        let copied = &destination[store];
        // A fabric whose nodes recorded no connection fact holds an empty connections store.
        assert!(*store == rdm_admin::store_root::CONNECTIONS_FILE || !lines.is_empty(), "{store}: the old provider holds history to carry");
        assert!(copied.len() >= lines.len() && copied[..lines.len()] == lines[..], "{store}: the new provider holds every line of the old one in source order ({} of {} lines; first difference at {:?})", copied.len(), lines.len(), lines.iter().zip(copied.iter()).position(|(a, b)| a != b));
    }
}

/// The spans of the storage step `name` for `build_id`, once one is recorded.
async fn step_spans(estate: &Estate, name: &str, build_id: &str, outcome: &str) -> Vec<Value> {
    wait_for(&format!("{name} outcome={outcome} for Build {build_id}"), STEP, || async {
        let spans: Vec<Value> = estate.spans().into_iter().filter(|s| s["name"] == name && s["attributes"]["build_id"] == build_id && s["attributes"]["outcome"] == outcome).collect();
        (!spans.is_empty()).then_some(spans)
    })
    .await
}

fn s(v: &Value) -> String {
    v.as_str().map(String::from).unwrap_or_else(|| v.to_string())
}

/// The fabric names `provider` and has left `previous`, as `fabric.get` answers it.
fn fabric_names(fabric: &FabricView, provider: &str, previous: &[&str]) {
    assert_eq!(fabric.topology_storage_provider.as_deref(), Some(provider), "fabric.get answers the Fabric row's topology_storage_provider: {fabric:?}");
    assert_eq!(fabric.previous_topology_storage_providers, previous.iter().map(|p| p.to_string()).collect::<Vec<_>>(), "fabric.get answers the providers the fabric left, in order: {fabric:?}");
}

/// Every node writes `provider`, as its row records it and `mesh.nodes.get` answers it.
fn nodes_write(views: &[NodeView], provider: &str) {
    for v in views {
        assert_eq!(v.topology_storage_provider.as_deref(), Some(provider), "{}: its row records the provider it writes: {v:?}", v.name);
    }
}

/// CONTRACT (storage.md §2 "The property": "`topology_storage_provider` is one value for the fabric,
/// on the Fabric row, beside `previous_topology_storage_providers` ... The same name on a node's row
/// is the provider that node writes"; "A caller reads the fabric's with `fabrics.get` and each node's
/// with `nodes.get`"; §1: `storage_persistence` "is a typed property of the node's create spec, carried
/// on the accepted Build's `NodeSpec` and the node's row"; Day 0: "`RDM_TOPOLOGY_STORAGE_PROVIDER`,
/// `file` when unset"; i149 R1): on a two-mesh fabric started with no provider named, `fabric.get`
/// answers `file` with no previous provider, and every node's row, as `mesh.nodes.get` answers it,
/// carries the provider it writes, `file`, beside its `storage_persistence`, two properties under two
/// names: a node-admin's `persistent { disposition: preserve }`, an rpc node's `ephemeral`. Must NOT
/// happen: a node or the fabric naming no provider, or the two properties read as one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_customer_reads_each_nodes_topology_provider() {
    let test = "a_customer_reads_each_nodes_topology_provider";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let fabric = estate.fabric_view().await;
    let views = estate.node_views().await;
    estate.artifact("fabric.json", &json!(fabric));
    estate.artifact("views.json", &json!(Estate::values(&views)));
    fabric_names(&fabric, FILE, &[]);
    assert_eq!(views.iter().map(|v| v.name.to_string()).collect::<BTreeSet<_>>(), two_mesh_names(), "the view holds both meshes' nodes");
    nodes_write(&views, FILE);
    for v in &views {
        let persistence = serde_json::to_value(&v.storage_persistence).unwrap();
        let want = match v.kind {
            rdm_client::LaunchKind::NodeAdmin => json!({"persistence": "persistent", "disposition": "preserve"}),
            _ => json!({"persistence": "ephemeral"}),
        };
        assert_eq!(persistence, want, "{}: its row records its storage_persistence, a property apart from the provider it writes: {v:?}", v.name);
    }
    estate.stop().await;
}

/// CONTRACT (storage.md §2 "Changing it": "Acceptance refuses by name a provider the fabric-primary's
/// wiring does not register or cannot open"; i149 R2: "refuse unsupported names without moving
/// data"; § Surfaces: "Unsupported provider is a named refusal, not an empty read or successful
/// no-op"): `build.create` of the complete topology naming a provider no executable registers is
/// refused by name, `topology-storage-provider-not-registered`, its detail naming the provider and the
/// ones the fabric-primary registers; nothing is accepted (the current Build stands, and no record on
/// the store root names the provider), the Fabric row still names `file` with no previous provider, and
/// every node is the same birth, writing `file`. Must NOT happen: a Build accepted, a block step run, or
/// the fabric's provider moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsupported_provider_changes_nothing() {
    let test = "an_unsupported_provider_changes_nothing";
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let before = births(&views);
    let current = estate.current().await.based_on;
    let change = naming_provider(&estate, Some(UNREGISTERED)).await;
    let refused = estate.carrier().await.create(change.clone()).await;
    let (reason, detail) = match refused {
        Err(CallEnd::Rejected { reason, detail }) => (reason, detail),
        Err(other) => panic!("the request naming {UNREGISTERED} ended otherwise than refused by name: {other}"),
        Ok(stream) => panic!("the request naming {UNREGISTERED} was accepted as Build {}", stream.build_id()),
    };
    estate.artifact("refusal.json", &json!({ "reason": reason, "detail": detail }));
    assert_eq!(reason, "topology-storage-provider-not-registered", "refused by name: {detail}");
    assert!(detail.contains(UNREGISTERED) && detail.contains(FILE), "the refusal names the provider and the ones the fabric-primary registers: {detail}");
    // Nothing accepted: the current Build stands and no record on the store root names the provider.
    assert_eq!(estate.current().await.based_on, current, "the current Build is the one before the request");
    let (file_root, _) = roots(&estate);
    for (store, lines) in store_lines(&file_root) {
        assert!(!lines.iter().any(|l| l.contains(UNREGISTERED)), "{store}: no record names {UNREGISTERED}");
    }
    fabric_names(&estate.fabric_view().await, FILE, &[]);
    let after = estate.node_views().await;
    assert_eq!(births(&after), before, "every node is the same birth");
    nodes_write(&after, FILE);
    // Every process's evidence is flushed as it leaves: the spans are read once the estate has stopped.
    estate.stop().await;
    let spans = estate.spans();
    let reject = named(&spans, "rdm.node_admin.build.reject.via-topology-storage-provider-not-registered");
    assert!(reject.iter().any(|sp| s(&sp["attributes"]["detail"]).contains(UNREGISTERED)), "the fabric-primary's refusal span names the provider: {reject:#?}");
    assert!(named(&spans, "rdm.node.build.update.via-storage-block").is_empty(), "no node blocked: no step of a change ran");
}

/// CONTRACT (storage.md §2 "The property": Day 0, "the bootstrap admin, on the Fabric row ... its
/// environment, `RDM_TOPOLOGY_STORAGE_PROVIDER`"; every launch, "the launching admin, into the node's
/// environment"; a change, "a Build that omits the property leaves it unchanged (Q166)"; i149 R3 and
/// "Provider omission never silently switches an established fabric to file"): a fabric started with
/// `RDM_TOPOLOGY_STORAGE_PROVIDER=test-target` writes its records there from Day 0, nothing on the file
/// provider's store dir; a Build that omits the property (the two-mesh reconcile, which creates four
/// nodes) completes, and the fabric still names `test-target` with no previous provider, every node it
/// launched writing `test-target`. Must NOT happen: the omitted value read as `file`, or a record of the
/// fabric written to the file provider.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_omitted_provider_preserves_the_current_provider() {
    let test = "an_omitted_provider_preserves_the_current_provider";
    let (mut estate, views) = two_mesh_estate(test, &[("RDM_TOPOLOGY_STORAGE_PROVIDER", TOPOLOGY_STORAGE_TARGET)]).await;
    let fabric = estate.fabric_view().await;
    estate.artifact("fabric.json", &json!(fabric));
    estate.artifact("views.json", &json!(Estate::values(&views)));
    fabric_names(&fabric, TOPOLOGY_STORAGE_TARGET, &[]);
    nodes_write(&estate.node_views().await, TOPOLOGY_STORAGE_TARGET);
    // Day 0 on the named provider: its place holds the fabric's root, the file provider's store dir none.
    let store_dir = estate.store_dir();
    let file_held: Vec<String> = std::fs::read_dir(&store_dir).map(|d| d.flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect()).unwrap_or_default();
    assert!(file_held.is_empty(), "the file provider's store dir {} holds nothing of the fabric: {file_held:?}", store_dir.display());
    let target = topology_storage_dir(&store_dir).join(fabric.id.to_string());
    assert!(target.join(rdm_admin::store_root::LAYOUT).is_file(), "the fabric's root is on {TOPOLOGY_STORAGE_TARGET}'s place {}", target.display());
    // Every store the fabric has written (a fabric whose nodes recorded no connection fact has written
    // none to its connections store) is on the named provider.
    assert!(store_lines(&target).iter().all(|(store, l)| *store == rdm_admin::store_root::CONNECTIONS_FILE || !l.is_empty()), "every store of the fabric is written on {TOPOLOGY_STORAGE_TARGET}");
    estate.stop().await;
}
