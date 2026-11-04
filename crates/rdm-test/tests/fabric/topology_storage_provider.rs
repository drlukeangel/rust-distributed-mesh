//! product=mesh, feature=storage, subfeature=topology-storage-provider, rung=MM, provider=process and
//! container (the container cells in [`container`], opt-in).
//!
//! The fabric's `topology_storage_provider` (storage.md §2; i149): the registered name of the backend
//! RDM's five record stores (fabric, meshes, nodes, connections, builds) are written to. It is one value
//! for the fabric, on the Fabric record beside `previous_topology_storage_providers` (Q361: never a
//! node's); a node opens the backend the Fabric record names. Day 0 takes it from `RDM_TOPOLOGY_STORAGE_PROVIDER`,
//! `file` when unset; a change is `fabric.storage.update` to the fabric-primary, its own workflow and
//! never a Build, recorded as its `rdm.storage.*` events; a Build never changes it.
//!
//! Every estate here runs the testkit's faulted node-admin and node, each registering RDM's `file`
//! backend and the testkit's second provider, [`TOPOLOGY_STORAGE_TARGET`], a file backend at a place of
//! its own beside the fabric's store dir (`rdm_test::test_app::topology_storage_dir`).

use rdm_client::{CallEnd, Change, FabricDesired, FabricView, MeshDesired, NodeStatus, NodeView, PathName};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use rdm_test::process_faults::{ExactRuntime, Fault};
use rdm_test::test_app::{refuse_list_knob, topology_storage_dir, TOPOLOGY_STORAGE_MEMORY, TOPOLOGY_STORAGE_REFUSING, TOPOLOGY_STORAGE_TARGET};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(180);
/// How long a storage change's step is waited for once the change is started.
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

/// A fabric of the faulted node-admin and the testkit node, its Day-0 admin given `env`.
async fn estate_with(test: &str, env: &[(&str, &str)]) -> Estate {
    let sha = candidate_sha();
    Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &[], env).await.expect("the faulted-admin binding set is accepted")
}

/// Two meshes: mesh1 with two node-admins and a node, mesh2 with one of each.
fn two_meshes() -> FabricDesired {
    FabricDesired {
        fabric: "fabric1".into(),
        meshes: vec![
            MeshDesired::of("mesh1", [("node-admin", 2), ("node", 1)]),
            MeshDesired::of("mesh2", [("node-admin", 1), ("node", 1)]),
        ],
    }
}

/// The names [`two_meshes`] holds.
fn two_mesh_names() -> BTreeSet<PathName> {
    [("mesh1.node-admin", "1"), ("mesh1.node-admin", "2"), ("mesh1.node", "1"), ("mesh2.node-admin", "1"), ("mesh2.node", "1")].into_iter().map(|(path, name)| PathName::new(path, name).unwrap()).collect()
}

/// The estate grown to [`two_meshes`], every node ready and the fabric ready.
async fn two_mesh_estate(test: &str, env: &[(&str, &str)]) -> (Estate, Vec<NodeView>) {
    let mut estate = estate_with(test, env).await;
    estate.resize(&two_meshes()).await;
    let views = estate.settles_to_named(&two_mesh_names(), SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    (estate, views)
}

/// Each node's birth: `(path, name)` → (node id, incarnation).
fn births(views: &[NodeView]) -> BTreeMap<PathName, (String, Option<String>)> {
    views.iter().map(|v| (v.location(), (v.node_id.to_string(), v.incarnation.as_ref().map(|i| i.as_str().to_string())))).collect()
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

/// The spans of the storage step `name` for `change_id`, once one is recorded.
async fn step_spans(estate: &Estate, name: &str, change_id: &str, outcome: &str) -> Vec<Value> {
    wait_for(&format!("{name} outcome={outcome} for storage change {change_id}"), STEP, || async {
        let spans: Vec<Value> = estate.spans().into_iter().filter(|s| s["name"] == name && s["attributes"]["change_id"] == change_id && s["attributes"]["outcome"] == outcome).collect();
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

/// CONTRACT (storage.md §2 "The property": `topology_storage_provider` is one value for the fabric, on
/// the Fabric record beside `previous_topology_storage_providers` (Q361: a fabric setting, never a
/// node's); §1: a node's own storage is its record's `storage { persistence, data_dir }` (Q364); Day 0:
/// "`RDM_TOPOLOGY_STORAGE_PROVIDER`, `file` when unset"; i149 R1): on a two-mesh fabric started with no
/// provider named, `fabric.get` answers `file` with no previous provider, and `node.get` answers each
/// node's `storage.persistence`: a node-admin's `persistent { disposition: preserve }`, a node's
/// `ephemeral`. Must NOT happen: the fabric naming no provider, or a node's storage persistence absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_customer_reads_the_fabrics_topology_provider_and_each_nodes_storage() {
    let test = "a_customer_reads_the_fabrics_topology_provider_and_each_nodes_storage";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let fabric = estate.fabric_view().await;
    let views = estate.node_views().await;
    estate.artifact("fabric.json", &json!(fabric));
    estate.artifact("views.json", &json!(Estate::values(&views)));
    fabric_names(&fabric, FILE, &[]);
    assert_eq!(views.iter().map(|v| v.location()).collect::<BTreeSet<_>>(), two_mesh_names(), "the view holds both meshes' nodes");
    for v in &views {
        let persistence = serde_json::to_value(&v.storage.persistence).unwrap();
        let want = match v.r#type.as_str() {
            "node-admin" => json!({"persistence": "persistent", "disposition": "preserve"}),
            _ => json!({"persistence": "ephemeral"}),
        };
        assert_eq!(persistence, want, "{:?}: its record's storage carries its persistence: {v:?}", v.name);
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
/// every node is the same birth, writing `file`. Must NOT happen: a Build accepted, a mesh-primary blocked, or
/// the fabric's provider moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsupported_provider_changes_nothing() {
    let test = "an_unsupported_provider_changes_nothing";
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let before = births(&views);
    let current = estate.current().await.build_id;
    let refused = estate.fabrics_obj().await.update_storage(UNREGISTERED).await;
    let (reason, detail) = match refused {
        Err(CallEnd::Rejected { reason, detail }) => (reason, detail),
        Err(other) => panic!("the request naming {UNREGISTERED} ended otherwise than refused by name: {other}"),
        Ok(change_id) => panic!("the request naming {UNREGISTERED} was started as storage change {change_id}"),
    };
    estate.artifact("refusal.json", &json!({ "reason": reason, "detail": detail }));
    assert_eq!(reason, "topology-storage-provider-not-registered", "refused by name: {detail}");
    assert!(detail.contains(UNREGISTERED) && detail.contains(FILE), "the refusal names the provider and the ones the fabric-primary registers: {detail}");
    // Nothing accepted: the current Build stands and no record on the store root names the provider.
    assert_eq!(estate.current().await.build_id, current, "the current Build is the one before the request");
    let (file_root, _) = roots(&estate);
    for (store, lines) in store_lines(&file_root) {
        assert!(!lines.iter().any(|l| l.contains(UNREGISTERED)), "{store}: no record names {UNREGISTERED}");
    }
    fabric_names(&estate.fabric_view().await, FILE, &[]);
    let after = estate.node_views().await;
    assert_eq!(births(&after), before, "every node is the same birth");
    // Every process's evidence is flushed as it leaves: the spans are read once the estate has stopped.
    estate.stop().await;
    let spans = estate.spans();
    let reject = named(&spans, "rdm.node_admin.storage.update.via-caller");
    assert!(reject.iter().any(|sp| attr(sp, "provider") == UNREGISTERED && attr(sp, "reason") == "topology-storage-provider-not-registered"), "the fabric-primary's refusal span names the provider and the rule: {reject:#?}");
    assert!(named(&spans, "rdm.node_admin.mesh.update.via-block").is_empty(), "no mesh-primary blocked: no step of a change ran");
}

/// CONTRACT (storage.md §2 "Changing it": "Acceptance refuses by name a provider the fabric-primary's
/// wiring does not register or cannot open"): `test-target` is registered by every executable of the
/// testkit, and its place cannot hold the fabric's root (a regular file stands at the root's path); a
/// request naming it is refused by name, `topology-storage-provider-not-opened`, naming the provider and
/// why it does not open. Nothing is accepted: the current Build stands, the Fabric row and every node
/// still name `file`, every node is the same birth, and no mesh-primary blocked. Must NOT happen: the
/// Build accepted and its copy meeting a provider that does not answer, read again without end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_that_does_not_open_changes_nothing() {
    let test = "a_provider_that_does_not_open_changes_nothing";
    let (estate, views) = two_mesh_estate(test, &[]).await;
    let before = births(&views);
    let current = estate.current().await.build_id;
    let (_, target_root) = roots(&estate);
    std::fs::create_dir_all(target_root.parent().expect("the root is under its provider's place")).expect("test-target's place");
    let _ = std::fs::remove_dir_all(&target_root);
    std::fs::write(&target_root, b"a regular file where the fabric's root would be").expect("a file at test-target's root");
    not_opened(estate, TOPOLOGY_STORAGE_TARGET, &before, &current, "").await;
}

/// The request naming `provider` is refused by name, `topology-storage-provider-not-opened`, its detail
/// naming the provider and `why`; nothing is accepted: the current Build is `current`, the Fabric row
/// and every node still name `file`, every node is the birth `before` holds, and no mesh-primary blocked.
async fn not_opened(mut estate: Estate, provider: &str, before: &BTreeMap<PathName, (String, Option<String>)>, current: &rdm_client::BuildId, why: &str) {
    let refused = estate.fabrics_obj().await.update_storage(provider).await;
    let (reason, detail) = match refused {
        Err(CallEnd::Rejected { reason, detail }) => (reason, detail),
        Err(other) => panic!("the request naming {provider} ended otherwise than refused by name: {other}"),
        Ok(change_id) => panic!("the request naming {provider}, which does not open, was started as storage change {change_id}"),
    };
    estate.artifact("refusal.json", &json!({ "reason": reason, "detail": detail }));
    assert_eq!(reason, "topology-storage-provider-not-opened", "refused by name: {detail}");
    assert!(detail.contains(provider) && detail.contains(why), "the refusal names the provider and why: {detail}");
    assert_eq!(&estate.current().await.build_id, current, "the current Build is the one before the request");
    fabric_names(&estate.fabric_view().await, FILE, &[]);
    let after = estate.node_views().await;
    assert_eq!(&births(&after), before, "every node is the same birth");
    estate.stop().await;
    let spans = estate.spans();
    let reject = named(&spans, "rdm.node_admin.storage.update.via-caller");
    assert!(reject.iter().any(|sp| attr(sp, "provider") == provider && attr(sp, "reason") == "topology-storage-provider-not-opened"), "the fabric-primary's refusal span names the provider and the rule: {reject:#?}");
    assert!(named(&spans, "rdm.node_admin.mesh.update.via-block").is_empty(), "no mesh-primary blocked: no step of a change ran");
}

/// CONTRACT (storage.md §2 "Changing it": "Acceptance refuses by name a provider the fabric-primary's
/// wiring does not register or cannot open"; step 2, the copy reads every store of the new provider):
/// `test-memory`, RDM's memory backend, is registered by every executable of the testkit, and its
/// boundaries open while it answers no lines to copy; a request naming it is refused by name,
/// `topology-storage-provider-not-opened`, naming the provider and that it answers no lines. Nothing is
/// accepted: the current Build stands, the Fabric row and every node still name `file`, every node is
/// the same birth, and no mesh-primary blocked. Must NOT happen: the Build accepted, its copy failing
/// on lines that do not open and its commit read on them read again without end, every mesh blocked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_whose_lines_do_not_open_changes_nothing() {
    let (estate, views) = two_mesh_estate("a_provider_whose_lines_do_not_open_changes_nothing", &[]).await;
    let current = estate.current().await.build_id;
    not_opened(estate, TOPOLOGY_STORAGE_MEMORY, &births(&views), &current, "answers no lines").await;
}

/// CONTRACT (storage.md §2 "Changing it": "Acceptance refuses by name a provider the fabric-primary's
/// wiring does not register or cannot open"; step 2, the copy reads every store of the new provider):
/// `test-refusing` with its knob standing answers every list of its store lines with a named refusal;
/// a request naming it is refused by name, `topology-storage-provider-not-opened`, naming the provider
/// and the refusal. Nothing is accepted and no mesh-primary blocked. Must NOT happen: the Build accepted
/// and its copy meeting the refusal with every mesh blocked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_that_refuses_its_list_at_acceptance_changes_nothing() {
    let (estate, views) = two_mesh_estate("a_provider_that_refuses_its_list_at_acceptance_changes_nothing", &[]).await;
    let current = estate.current().await.build_id;
    let knob = refuse_list_knob(estate.fabric_store_root().parent().expect("the root is under the store dir"));
    std::fs::create_dir_all(knob.parent().unwrap()).unwrap();
    std::fs::write(&knob, b"").unwrap();
    not_opened(estate, TOPOLOGY_STORAGE_REFUSING, &births(&views), &current, "lists no lines").await;
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

/// CONTRACT (i149 § Seed spec, `a_customer_changes_topology_storage_without_restarting_members`; storage.md
/// §2 "Changing it": "a property Build: the fabric, every mesh and every node stay ready, no
/// node restarts, and no node changes state"; "Copy" ... "dead births' and deleted nodes' records among
/// them"; "The previous provider is kept as it was at the switch ... written no more"): a
/// two-mesh fabric with history (a deleted birth, a deleted mesh) is moved to `test-target` by one Build
/// of its complete topology, and a caller's `node.config.update` of mesh2.node.1 reaches mesh2's
/// mesh-primary once it has replied `blocked` ("A mesh-primary that has replied `blocked` issues no new
/// command until it is unblocked"; node-config-update.md: "admitted at any time"): it is answered, and the
/// node applies the map only once its mesh-primary's `mesh.unblock` began; the row carrying it is on
/// `test-target` and not on `file`. The Build completes; the Fabric
/// row names `test-target` with `file` previous; every node writes `test-target` and is the same birth,
/// ready, carrying the config its last row names (the update and the Build's own config are
/// a serial order of the two, node-config-update.md); every line of the five stores is on
/// `test-target` in source order, the deleted birth's and the deleted mesh's included; `file` is written
/// no more; and each mesh-primary's `mesh.block` (its reply `blocked`), the copy, the switch and each
/// `mesh.unblock` (its reply `unblocked`) are spans on one trace (storage.md §2, Q198). Must NOT
/// happen: a node restarted or replaced, a record lost or reordered, the old provider written after
/// the commit, or the update's command issued by a blocked mesh-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_customer_changes_topology_storage_without_restarting_members() {
    let test = "a_customer_changes_topology_storage_without_restarting_members";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    // History: a deleted birth (mesh1.node.2, created and removed) and a deleted mesh (mesh3).
    let mut grown = two_meshes();
    grown.meshes[0].counts.insert("node".into(), 2);
    grown.meshes.push(MeshDesired::of("mesh3", [("node-admin", 1)]));
    estate.resize(&grown).await;
    let deleted = estate.node_view(&PathName::new("mesh1.node", "2").unwrap()).await;
    estate.resize(&two_meshes()).await;
    // A deleted mesh's last node-admin is a separate node.delete after mesh.deleted (mesh-delete.md step 7).
    Estate::workflow("node.delete of mesh3.node-admin.1", estate.delete_node(&Estate::path("mesh3.node-admin", "1")).await).await;
    let views = estate.settles_to_named(&two_mesh_names(), SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let before = births(&views);
    let (file_root, target_root) = roots(&estate);
    let history = store_lines(&file_root);
    assert!(history[rdm_admin::store_root::NODES_FILE].iter().any(|l| l.contains(deleted.node_id.as_str())) && history[rdm_admin::store_root::MESHES_FILE].iter().any(|l| l.contains("mesh3")), "the old provider holds the deleted birth's and the deleted mesh's records");

    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    // A caller's node.config.update of a mesh2 node once mesh2's mesh-primary replied `blocked`: sent to
    // the fabric-primary (node-config-update.md step 1), which sends it down to mesh2's mesh-primary;
    // admitted there, and issued only once it is unblocked (storage.md §2 step 1).
    let blocked = blocked_at(&estate, &change_id, "mesh2").await;
    let subject = views.iter().find(|v| v.location() == role()).cloned().expect("mesh2.node.1");
    let fabric_primary = views.iter().find(|v| v.is_fabric_primary).cloned().expect("the fabric-primary");
    let told: BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(subject.node_id.clone()), node_id: subject.node_id.clone(), incarnation: subject.incarnation.clone().expect("a live birth") };
    let carrier = estate.carrier().await;
    let (rpc, options) = (rdm_client::NodeRpc::new(carrier.client()), rdm_transport::CallOptions::default());
    let update = rpc.config_update(&fabric_primary.node_id, &rdm_transport::RouteChoice::Direct, &birth, told, &options);
    let (done, updated) = tokio::join!(ended(&estate, &run), update);
    updated.unwrap_or_else(|e| panic!("node.config.update during the change is answered: {e}"));
    estate.artifact("ended.json", &done);
    assert!(completed(&done), "the change ends complete: {done}");
    // The record line the update wrote is on the new provider, the one the node writes once its
    // mesh-primary is unblocked, never the old.
    assert!(!lines_naming(&target_root, "\"tier\":\"gold\"").is_empty() && lines_naming(&file_root, "\"tier\":\"gold\"").is_empty(), "the update's record line is on {TOPOLOGY_STORAGE_TARGET} and not on {FILE}");

    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    let after = estate.node_views().await;
    estate.artifact("views.json", &json!(Estate::values(&after)));
    assert_eq!(births(&after), before, "every node is the same birth: none restarted or replaced");
    assert!(after.iter().all(|v| v.state == NodeStatus::Ready), "every node is ready: {after:?}");
    let path: PathName = rdm_core::PathName::new("mesh2.node", "1").unwrap();
    // The update and the Build's own config of the node are a serial order of the two
    // (node-config-update.md): whichever was written last on its own record is the config the node
    // carries (the node writes its own record, Q356).
    let subject_after = after.iter().find(|v| v.location() == path).cloned().expect("mesh2.node.1 after the change");
    let last_record: Value = rdm_admin::store_log::read_record(&target_root.join(rdm_admin::store_root::NODES_FILE), rdm_node::nodes_storage::RECORD_KIND, Some(subject_after.node_id.as_str()))
        .expect("mesh2.node.1's record is on the new provider");
    let record_config: BTreeMap<String, String> = serde_json::from_value(last_record["data"]["config"].clone()).expect("a node record carries its whole config map");
    assert_eq!(subject_after.config, record_config, "mesh2.node.1 carries the config its last record line on {TOPOLOGY_STORAGE_TARGET} names");
    let old = store_lines(&file_root);
    history_carried(&old, &store_lines(&target_root));
    // storage.md §2 step 3, "Switch": the fabric-primary appends the change's `rdm.storage.committed`
    // event to the new provider's events store; the commit is that event, never a lock file.
    let events = std::fs::read_to_string(target_root.join(rdm_admin::store_root::EVENTS_FILE)).unwrap_or_default();
    let committed: Vec<&str> = events.lines().filter(|l| l.contains("\"rdm.storage.committed\"") && l.contains(&change_id)).collect();
    assert_eq!(committed.len(), 1, "the new provider's events store holds the change's one rdm.storage.committed event: {events}");
    assert!(!target_root.join("locks").join("commit").exists(), "the commit is no lock file on {TOPOLOGY_STORAGE_TARGET}");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(store_lines(&file_root), old, "the previous provider is written no more");

    // Every process's evidence is flushed as it leaves: the spans are read once the estate has stopped.
    estate.stop().await;
    let spans = estate.spans();
    let mut traces = BTreeSet::new();
    for (step, outcome) in [
        ("rdm.node_admin.mesh.update.via-block", "blocked"),
        ("rdm.node_admin.storage.update.via-copy", "copied"),
        ("rdm.node_admin.storage.update.via-switch", "inserted"),
        ("rdm.node_admin.mesh.update.via-unblock", "unblocked"),
    ] {
        let found: Vec<&Value> = named(&spans, step).into_iter().filter(|sp| s(&sp["attributes"]["change_id"]) == change_id && sp["attributes"]["outcome"] == outcome).collect();
        assert!(!found.is_empty(), "{step} outcome={outcome} is recorded for Build {change_id}");
        traces.extend(found.iter().map(|sp| s(&sp["trace_id"])));
    }
    assert_eq!(traces.len(), 1, "the storage steps are on one causal trace: {traces:?}");
    // The node applies the update's map only once its mesh-primary's `mesh.unblock` began.
    let unblocked = of_change(&spans, UNBLOCK, &change_id, Some("mesh2")).into_iter().filter(|sp| sp["attributes"]["outcome"] == "unblocked").map(start_ns).min().expect("mesh2's mesh-primary unblocked");
    let applied: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-apply-node-config").into_iter().filter(|sp| [subject.node_id.as_str(), subject_after.node_id.as_str()].contains(&attr(sp, "node").as_str()) && start_ns(sp) >= start_ns(&blocked)).collect();
    assert!(!applied.is_empty(), "mesh2.node.1 applied the update's map");
    assert!(applied.iter().all(|sp| start_ns(sp) >= unblocked), "mesh2.node.1 applies the map only once its mesh-primary is unblocked: {applied:#?}");
    estate.record_trace_url(traces.iter().next().unwrap());
}

// ---- e4: the mesh-primaries' block, the commit record, failure before the switch and lost nodes (i149
// R8–R12, R17–R19; storage.md §2 "Changing it", Q198). The spans: a mesh-primary's `mesh.block` and
// `mesh.unblock` (`rdm.node_admin.mesh.update.via-block` / `via-unblock`, its reply the outcome); the
// fabric-primary's switch, `commit_found` naming what it read of the `commit` record; a node's move
// onto the provider its change decided (`rdm.node.storage.update.via-move`).

/// A mesh-primary's `mesh.block` (storage.md §2 step 1): outcome `blocked`, its reply.
const BLOCK: &str = "rdm.node_admin.mesh.update.via-block";
/// The fabric-primary's switch: the `commit` record for `(change_id, change_id)` inserted, or read.
const SWITCH: &str = "rdm.node_admin.storage.update.via-switch";
/// A mesh-primary's `mesh.unblock` (step 4): outcome `unblocked`, its reply, `provider` where it moved
/// its nodes.
const UNBLOCK: &str = "rdm.node_admin.mesh.update.via-unblock";
/// A node's move onto the provider its change decided, commanded by its mesh-primary.
const MOVE: &str = "rdm.node.storage.update.via-move";
/// A mesh-primary's re-create, in its `mesh.unblock`, of a node that will not move: outcome `recreated`.
const MOVE_LOST: &str = "rdm.node_admin.node.update.via-storage-move-lost";
/// A node's connection event, its span open while its write lands (`connections_writer`).
const OBSERVED: &str = "rdm.node_admin.connection.update.via-observed";
/// The role node each cell restarts, freezes or reads.
fn role() -> PathName { PathName::new("mesh2.node", "1").unwrap() }
/// The fabric-primary, where the testkit's storage-switch cut holds a change between every
/// mesh-primary's `blocked` and the copy.
fn bootstrap() -> PathName { PathName::new("mesh1.node-admin", "1").unwrap() }

/// `name`'s exact runtime, frozen (SIGSTOP: alive and silent).
async fn freeze(estate: &Estate, name: &PathName) -> ExactRuntime {
    let rt = ExactRuntime::published(Path::new(&estate.data_dir(name).await)).unwrap_or_else(|r| panic!("{name:?}: its published runtime: {r:?}"));
    rt.apply(Fault::Stop).unwrap_or_else(|r| panic!("{name:?}: frozen: {r:?}"));
    rt
}

/// Continue a frozen runtime. One already ended (a re-created node's old birth, terminated) is left.
fn thaw(rt: &ExactRuntime) {
    let _ = rt.apply(Fault::Continue);
}

/// The fabric-primary's storage-switch cut armed: the next change is held after every mesh-primary
/// replied `blocked`, before its copy, until [`FaultClient::release`] of `"switch"`.
async fn hold_switch(estate: &Estate) -> rdm_test::faults::FaultClient {
    let fp = estate.node_views().await.into_iter().find(|n| n.is_fabric_primary).expect("fabric primary");
    let faults = rdm_test::faults::FaultClient::at(estate, &fp.location()).await;
    faults.arm("switch", json!({"kind": "storage-switch"})).await;
    faults
}

/// `fabric.storage.update` naming `provider` (storage.md §2 "Changing it"), accepted: the change's id,
/// twice (the one handed to [`ended`], the one the spans are filtered by).
async fn change_to(estate: &Estate, provider: &str) -> (String, String) {
    let id = estate.fabrics_obj().await.update_storage(provider).await.unwrap_or_else(|e| panic!("the storage change naming {provider} is accepted: {e}")).to_string();
    (id.clone(), id)
}

/// The change `change_id`'s `rdm.storage.ended` event, once it is written on either provider's events
/// store: the change's end (storage.md §2 "Changing it").
async fn ended(estate: &Estate, change_id: &str) -> Value {
    let (file_root, target_root) = roots(estate);
    let roots = [file_root, target_root, topology_storage_dir(estate.fabric_store_root().parent().expect("the root is under the store dir")).join("..").join("refusing")];
    wait_for(&format!("the rdm.storage.ended event of storage change {change_id}"), SETTLE, || {
        let roots = roots.clone();
        async move {
            roots.iter().flat_map(|r| std::fs::read_to_string(r.join(rdm_admin::store_root::EVENTS_FILE)).unwrap_or_default().lines().map(String::from).collect::<Vec<_>>())
                .filter_map(|l| serde_json::from_str::<Value>(&l).ok())
                .find(|e| e["type"] == "rdm.storage.ended" && e["data"]["change_id"] == change_id)
        }
    })
    .await
}

/// Whether the ended event `e` names the change complete.
fn completed(e: &Value) -> bool {
    e["data"]["state"] == "complete"
}

/// The reason the ended event `e` names the change failed with, when it failed.
fn failed_reason(e: &Value) -> Option<String> {
    e["data"]["state"]["failed"]["reason"].as_str().map(String::from)
}

/// A span's attribute as a string.
fn attr(sp: &Value, key: &str) -> String {
    s(&sp["attributes"][key])
}

/// Now, on the clock the spans' `start_unix_nano` is on.
fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// A boolean span attribute, recorded as a bool or its text.
fn yes(sp: &Value, key: &str) -> Option<bool> {
    match &sp["attributes"][key] {
        Value::Bool(b) => Some(*b),
        Value::String(t) => t.parse().ok(),
        _ => None,
    }
}

/// The spans `name` of the storage change `change_id` (every mesh's when `mesh` is `None`).
fn of_change<'a>(spans: &'a [Value], name: &str, change_id: &str, mesh: Option<&str>) -> Vec<&'a Value> {
    named(spans, name).into_iter().filter(|sp| attr(sp, "change_id") == change_id && mesh.is_none_or(|m| attr(sp, "mesh") == m)).collect()
}

/// `mesh`'s mesh-primary replied `blocked` to the storage change `change_id`'s `mesh.block`, once its span is recorded.
async fn blocked_at(estate: &Estate, change_id: &str, mesh: &str) -> Value {
    wait_for(&format!("{BLOCK} outcome=blocked for storage change {change_id} in {mesh}"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == BLOCK && attr(sp, "change_id") == change_id && attr(sp, "mesh") == mesh && sp["attributes"]["outcome"] == "blocked")
    })
    .await
}

/// The node id `name` holds now, as `mesh.nodes.get` answers it.
async fn node_id_of(estate: &Estate, name: &PathName) -> String {
    estate.node_view(name).await.node_id.to_string()
}

/// Every line of the five stores on `root` naming `needle`.
fn lines_naming(root: &Path, needle: &str) -> Vec<String> {
    store_lines(root).into_values().flatten().filter(|l| l.contains(needle)).collect()
}

/// One direct dial from `from` to `to` (a node's own connection event, Q169), answered.
fn dial(estate: &Estate, from: &str, to: &str) -> Value {
    let out = estate.probe(&["dial", "--target", &format!("exact:{from}"), "--destination", &format!("exact:{to}"), "--key", "storage-change"]);
    assert_eq!(out["outcome"], "Reply", "{from} answers the dial to {to} during the change (Q169: a connection event never blocks): {out}");
    out
}

/// A line on the destination root that the copy did not bring: on `target` and not on `source`. The
/// root is a provider's root of this build's layout (its `layout` marker), holding that one line.
fn foreign_line(target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    std::fs::write(target.join(rdm_admin::store_root::LAYOUT), format!("{}\n", rdm_admin::store_root::LAYOUT_VALUE)).unwrap();
    let writer = rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &rdm_core::NodeId::parse("zzzzzzzzzzz1").unwrap(), &rdm_core::IncarnationId::parse("zzzzzzzzzzz2").unwrap(), &rdm_transport::clock::RafkaTime::unadopted());
    // A whole node record (`RECORD_FORMAT`), as a writer of that store writes one.
    let record = serde_json::to_vec(&rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new("mesh9.node", "1").unwrap(), "node", rdm_core::NodeId::parse("zzzzzzzzzzz1").unwrap(), rdm_core::IncarnationId::parse("zzzzzzzzzzz2").unwrap(), rdm_core::model::NodeStatus::Ready)).unwrap();
    let line = rdm_admin::store_log::encode_line(&rdm_admin::store_log::Attributes::of(&writer, rdm_node::nodes_storage::RECORD_KIND, rdm_node::nodes_storage::RECORD_FORMAT, "zzzzzzzzzzz1"), &record);
    use std::io::Write;
    std::fs::OpenOptions::new().create(true).append(true).open(target.join(rdm_admin::store_root::NODES_FILE)).unwrap().write_all(&line).unwrap();
}

/// How a change in which a role node is restarted ends: switched, or failed at its copy before the
/// switch (the new provider holding a line the old one does not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ends {
    Switched,
    FailedBeforeTheSwitch,
}

/// One run of the role-node restart: the role fixture's mesh-primary has replied `blocked`, the change is held
/// before its copy, the role fixture dials (a connection event) and its process is killed (SIGKILL); node recovery
/// re-creates it at its `(path, name)` on a new data dir (a `node`'s storage is ephemeral, Q359) once its
/// mesh-primary issues commands again; the change ends `ends`. What is asserted is the
/// restarted birth's: it writes only the provider the `commit` record decided.
async fn role_node_restart(test: &str, ends: Ends) {
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    if ends == Ends::FailedBeforeTheSwitch {
        foreign_line(&target_root);
    }
    let (old_id, data_dir) = (node_id_of(&estate, &role()).await, estate.data_dir(&role()).await);
    let hold = hold_switch(&estate).await;
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &change_id, "mesh2").await;
    let change_id = attr(&blocked, "change_id");
    assert!(!change_id.is_empty(), "the block carries its change_id: {blocked}");
    // A connection event of the node while its mesh-primary is blocked.
    let dialled_ns = now_ns();
    let peer_id = node_id_of(&estate, &PathName::new("mesh2.node-admin", "1").unwrap()).await;
    dial(&estate, &old_id, &peer_id);
    estate.kill_node(&role()).await;
    hold.release("switch").await;
    let done = ended(&estate, &run).await;
    estate.artifact("ended.json", &done);
    let restarted = estate.until_node(&role(), "the role node re-created at its (path, name)", SETTLE, |v| v.filter(|v| v.node_id.to_string() != old_id && v.state == NodeStatus::Ready).cloned()).await;
    let new_id = restarted.node_id.to_string();
    assert_ne!(estate.data_dir(&role()).await, data_dir, "the re-created node is on a new data dir: a `node`'s storage is ephemeral (Q359)");
    let (chosen, other, previous): (&str, &Path, &[&str]) = match ends {
        Ends::Switched => (TOPOLOGY_STORAGE_TARGET, &file_root, &[FILE]),
        Ends::FailedBeforeTheSwitch => (FILE, &target_root, &[]),
    };
    match (ends, completed(&done)) {
        (Ends::Switched, true) | (Ends::FailedBeforeTheSwitch, false) => {}
        (_, last) => panic!("the change ends {ends:?}: {last:?}"),
    }
    fabric_names(&estate.fabric_view().await, chosen, previous);
    let chosen_root = if chosen == FILE { &file_root } else { &target_root };
    assert!(!lines_naming(chosen_root, &new_id).is_empty(), "the restarted birth's records are on {chosen}");
    // Nothing of a birth that started after the change's decision is on the provider not chosen.
    assert!(lines_naming(other, &new_id).is_empty(), "nothing of the restarted birth {new_id} is on the provider not chosen: {:#?}", lines_naming(other, &new_id));
    estate.stop().await;
    let spans = estate.spans();
    // The change was decided by the commit record.
    let decided: Vec<&Value> = of_change(&spans, SWITCH, &change_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found").is_some()).collect();
    assert!(!decided.is_empty(), "the fabric-primary decided the change by the commit record: {SWITCH} for ({change_id}, {change_id})");
    // The restarted birth only ever moves onto the provider the change decided, after it was decided.
    for sp in named(&spans, MOVE).into_iter().filter(|sp| attr(sp, "node_id") == new_id && attr(sp, "change_id") == change_id) {
        assert_eq!((attr(sp, "change_id"), attr(sp, "provider")), (change_id.clone(), chosen.to_string()), "the restarted birth moves onto {chosen} under the same change_id: {sp}");
        assert!(decided.iter().any(|d| start_ns(d) <= start_ns(sp)), "the restarted birth moves only once the commit record is read: {sp}");
    }
    // The connection event of the killed birth lands at most once on the provider chosen.
    let owed: Vec<&Value> = named(&spans, OBSERVED).into_iter().filter(|sp| attr(sp, "source_id") == old_id && attr(sp, "destination_id") == peer_id && attr(sp, "origin") == "dial" && start_ns(sp) >= dialled_ns).collect();
    for sp in &owed {
        let entry = attr(sp, "entry");
        assert!(lines_naming(chosen_root, &entry).len() <= 1, "the connection event {entry} lands at most once on {chosen}");
    }
    estate.record_trace_url(&s(&decided[0]["trace_id"]));
}

/// CONTRACT (i149 § Restart-recovery canary, second canary; storage.md §2 "Changing it" (Q198): "Once every
/// mesh-primary has replied `blocked`, the fabric-primary [copies]"; "Whoever resumes the change learns
/// the decision only from that record"; "A step that fails before the switch fails the Build only once
/// the fabric-primary has read that no `commit` exists ...; the fabric-primary then sends `mesh.unblock`
/// on the old provider"): a role process (mesh2.node.1) is killed while its mesh-primary has replied
/// `blocked` and the change is held before its copy, and comes back at its `(path, name)` as a new node on a
/// new data dir (a `node`'s storage is ephemeral, Q359). Run twice: a change that switches, and one whose copy is refused before the switch. Each time the
/// restarted birth writes only the provider the `commit` record decided: every record of it is on that
/// provider and none on the other, and it only ever moves onto that provider once the
/// record is read; the Fabric row names that provider. Must NOT happen: a write of the restarted birth on
/// the provider not chosen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_storage_state_survives_role_node_restart() {
    role_node_restart("topology_storage_state_survives_role_node_restart-switched", Ends::Switched).await;
    role_node_restart("topology_storage_state_survives_role_node_restart-failed", Ends::FailedBeforeTheSwitch).await;
}

/// CONTRACT (storage.md §2 "Changing it" (Q198): "A mesh-primary that has replied `blocked` issues no new
/// command until it is unblocked"; "Whoever resumes the change ... learns the decision only from that
/// record"; i149 R8, R12): a role node whose mesh-primary has replied `blocked` is killed while the change
/// is held, and node recovery re-creates it at its `(path, name)`, on a new data dir (Q359), once its
/// mesh-primary is unblocked. The restarted birth writes `test-target`, the provider the commit record
/// decided: every record of it is on `test-target`, none on `file`; the Build completes and the fabric
/// names `test-target` with `file` previous. Must NOT happen: the restarted birth writing to `file` (its
/// stale launch provider) or moving before the commit record is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read() {
    role_node_restart("a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read", Ends::Switched).await;
}

/// One mesh: its node-admin and a node, so the store names a node beside the admin's own birth
/// and the admin's loss is a fabric recovery, not Q200's Day 0.
fn one_mesh() -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1), ("node", 1)])] }
}

/// CONTRACT (storage.md §2 "The property": "a restart with no launcher (the adoption launch) | the node
/// itself | its own row in its data dir"): a one-mesh fabric started with no provider named moves to
/// `test-target` by one Build of its complete topology, which completes; its Day-0 node-admin is lost,
/// and a person starts a node-admin again on that data dir with no launcher and no
/// `RDM_TOPOLOGY_STORAGE_PROVIDER` (fabric recovery). The new birth writes `test-target`, the provider
/// its own row in its data dir names: the Fabric record still names `test-target` with `file` previous,
/// its records are on `test-target`, and nothing of it is on `file`. Must NOT happen: the start taking
/// `file` from its environment's default and opening the provider the fabric left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_without_a_launcher_writes_the_provider_its_own_row_names() {
    let test = "a_restart_without_a_launcher_writes_the_provider_its_own_row_names";
    let names: BTreeSet<PathName> = [bootstrap(), PathName::new("mesh1.node", "1").unwrap()].into_iter().collect();
    let mut estate = estate_with(test, &[]).await;
    estate.resize(&one_mesh()).await;
    estate.settles_to_named(&names, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let (file_root, target_root) = roots(&estate);
    let (run, _) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let done = ended(&estate, &run).await;
    assert!(completed(&done), "the change completes: {done}");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    let old_id = node_id_of(&estate, &bootstrap()).await;
    let data_dir = estate.bootstrap_data_dir("mesh1");
    estate.kill_bootstrap();
    let restarted = estate.restart_admin_with(&data_dir, &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    assert_eq!(restarted, bootstrap(), "the start on the data dir is at its own row's (path, name)");
    estate.enter(&restarted).await;
    let views = estate.settles_to_named(&names, SETTLE).await;
    estate.artifact("views.json", &json!(Estate::values(&views)));
    let admin = views.iter().find(|v| v.location() == bootstrap()).expect("the restarted node-admin");
    let new_id = admin.node_id.to_string();
    assert_ne!(new_id, old_id, "a new birth over the data dir");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    assert!(!lines_naming(&target_root, &new_id).is_empty(), "the new birth's records are on {TOPOLOGY_STORAGE_TARGET}");
    assert!(lines_naming(&file_root, &new_id).is_empty(), "nothing of the new birth {new_id} is on {FILE}, the provider the fabric left: {:#?}", lines_naming(&file_root, &new_id));
    estate.stop().await;
}

/// CONTRACT (storage.md §2 "Changing it" (Q198): "A step that fails before the switch fails the Build only
/// once the fabric-primary has read that no `commit` exists for `(change_id, change_id)`; the
/// fabric-primary then sends `mesh.unblock` on the old provider, the new provider closed and the Fabric
/// row unchanged"; step 2: "a new provider holding lines the old one does not is refused by name, and
/// nothing switches"; "No timer ends a block"; i149 R18): `test-target` holds a line `file` does not, so
/// the change's copy is refused before the switch (mesh2's mesh-primary frozen, then continued, holds
/// the change before every mesh-primary has replied `blocked`). The fabric-primary reads that no `commit`
/// record exists (`commit_found = false`) and only then fails the Build; every mesh-primary replies
/// `unblocked` onto `file` under the change's `change_id`, after that read; a connection event of a node
/// during the change lands once, on `file`; the Fabric row names `file` with no previous provider; every
/// node writes `file` and is the same birth. Must NOT happen: the Build failed before the record is read,
/// a mesh-primary left blocked or unblocked onto `test-target`, or the event lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider() {
    let test = "a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider";
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let before = births(&views);
    let (file_root, target_root) = roots(&estate);
    foreign_line(&target_root);
    // mesh2's mesh-primary holds the change; mesh1.node.1 dials mesh1.node-admin.2, a node it holds no
    // connection to, while mesh1's mesh-primary is blocked.
    let (source, peer, held) = (node_id_of(&estate, &PathName::new("mesh1.node", "1").unwrap()).await, node_id_of(&estate, &PathName::new("mesh1.node-admin", "2").unwrap()).await, PathName::new("mesh2.node-admin", "1").unwrap());
    let hold = freeze(&estate, &held).await;
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    blocked_at(&estate, &change_id, "mesh1").await;
    let dialled_ns = now_ns();
    dial(&estate, &source, &peer);
    thaw(&hold);
    let done = ended(&estate, &run).await;
    estate.artifact("ended.json", &done);
    assert!(failed_reason(&done).is_some(), "the change fails before its switch: {done}");
    fabric_names(&estate.fabric_view().await, FILE, &[]);
    let after = estate.node_views().await;
    let after_births = births(&after);
    for (name, birth) in before.iter().filter(|(n, _)| **n != held) {
        assert_eq!(after_births.get(name), Some(birth), "{name:?}: the same birth serves throughout");
    }
    // The estate's stop deletes every connection: the events it causes are not the change's.
    let stopped_ns = now_ns();
    estate.stop().await;
    let spans = estate.spans();
    let read: Vec<&Value> = of_change(&spans, SWITCH, &change_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found") == Some(false)).collect();
    assert!(!read.is_empty(), "the fabric-primary read that no commit record exists for ({change_id}, {change_id}) before it failed the Build");
    for mesh in ["mesh1", "mesh2"] {
        let own: Vec<&Value> = of_change(&spans, UNBLOCK, &change_id, Some(mesh)).into_iter().filter(|sp| sp["attributes"]["outcome"] == "unblocked").collect();
        assert!(!own.is_empty(), "{mesh}'s mesh-primary replied unblocked after the failure");
        for sp in own {
            assert_eq!((attr(sp, "change_id"), attr(sp, "provider")), (change_id.clone(), FILE.to_string()), "{mesh}: unblocked onto {FILE} under the change: {sp}");
            assert!(read.iter().any(|r| start_ns(r) <= start_ns(sp)), "{mesh}: unblocked only once no commit is read: {sp}");
        }
    }
    // The control: the connection event of the change lands once, on the old provider.
    let owed: Vec<&Value> = named(&spans, OBSERVED).into_iter().filter(|sp| attr(sp, "source_id") == source && attr(sp, "destination_id") == peer && attr(sp, "origin") == "dial" && (dialled_ns..stopped_ns).contains(&start_ns(sp))).collect();
    assert_eq!(owed.len(), 1, "the connection event during the change lands once: {owed:#?}");
    let entry = attr(owed[0], "entry");
    assert_eq!((lines_naming(&file_root, &entry).len(), lines_naming(&target_root, &entry).len()), (1, 0), "the connection event {entry} is on {FILE} once and not on {TOPOLOGY_STORAGE_TARGET}");
    estate.record_trace_url(&s(&read[0]["trace_id"]));
}

/// The copy's span (`rdm_node::storage_copy::COPY_SPAN`): `outcome` `copied`, `refused`, `failed` or
/// `indeterminate`, `cause` naming why.
const COPY: &str = "rdm.node_admin.storage.update.via-copy";

/// CONTRACT (storage.md §2 "Changing it" (Q198): step 2, "a new provider holding lines the old one does
/// not is refused by name, and nothing switches"; "A step that fails before the switch fails the Build
/// only once the fabric-primary has read that no `commit` exists for `(change_id, change_id)`; the
/// fabric-primary then sends `mesh.unblock` on the old provider, the new provider closed and the Fabric
/// row unchanged"; "No timer ends a block"): the change names `test-refusing`, held before its copy
/// (the storage-switch cut), and its knob is written: from there it answers a copy's list of its store
/// lines with a named refusal, its claims answered. The copy fails by name, once: its span ends
/// `failed`, its cause naming the provider's refusal, and it is not read again. The fabric-primary reads
/// that no `commit` record exists (`commit_found = false`) and only then fails the Build, its reason
/// naming the provider and the refusal; every mesh-primary replies `unblocked` onto `file` under the
/// change's `change_id`, after that read; the Fabric row names `file` with no previous provider; every
/// node writes `file` and is the same birth. Must NOT happen: a named refusal read as no answer and the
/// copy read again without end, every mesh left blocked; or the Build failed before the record is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_that_refuses_its_list_fails_the_copy_and_unblocks_on_the_old_provider() {
    let test = "a_provider_that_refuses_its_list_fails_the_copy_and_unblocks_on_the_old_provider";
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let before = births(&views);
    let hold = hold_switch(&estate).await;
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_REFUSING).await;
    blocked_at(&estate, &change_id, "mesh1").await;
    // Accepted, its lines answered: from now on the provider refuses its list by name.
    let knob = refuse_list_knob(estate.fabric_store_root().parent().expect("the root is under the store dir"));
    std::fs::create_dir_all(knob.parent().unwrap()).unwrap();
    std::fs::write(&knob, b"").unwrap();
    hold.release("switch").await;
    let done = match tokio::time::timeout(STEP, ended(&estate, &run)).await {
        Ok(done) => done,
        Err(_) => {
            estate.stop().await;
            let copies: Vec<String> = of_change(&estate.spans(), COPY, &change_id, None).into_iter().map(|sp| format!("{} {}", attr(sp, "outcome"), attr(sp, "cause"))).collect();
            panic!("Build {change_id} never ended within {STEP:?}: every mesh is still blocked; its copies: {copies:#?}");
        }
    };
    estate.artifact("ended.json", &done);
    match failed_reason(&done) {
        Some(reason) => assert!(reason.contains(TOPOLOGY_STORAGE_REFUSING) && reason.contains("lists no lines"), "the Build's failure names the provider and its refusal: {reason}"),
        last => panic!("the change fails before its switch: {last:?}"),
    }
    fabric_names(&estate.fabric_view().await, FILE, &[]);
    let after = estate.node_views().await;
    assert_eq!(births(&after), before, "every node is the same birth");
    estate.stop().await;
    let spans = estate.spans();
    let copies = of_change(&spans, COPY, &change_id, None);
    assert_eq!(copies.len(), 1, "the copy answered by name is not read again: {copies:#?}");
    assert!(attr(copies[0], "outcome") == "failed" && attr(copies[0], "cause").contains(TOPOLOGY_STORAGE_REFUSING), "the copy fails by name, naming the provider's refusal: {}", copies[0]);
    let read: Vec<&Value> = of_change(&spans, SWITCH, &change_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found") == Some(false)).collect();
    assert!(!read.is_empty(), "the fabric-primary read that no commit record exists for ({change_id}, {change_id}) before it failed the Build");
    for mesh in ["mesh1", "mesh2"] {
        let own: Vec<&Value> = of_change(&spans, UNBLOCK, &change_id, Some(mesh)).into_iter().filter(|sp| sp["attributes"]["outcome"] == "unblocked").collect();
        assert!(!own.is_empty(), "{mesh}'s mesh-primary replied unblocked after the failure");
        for sp in own {
            assert_eq!((attr(sp, "change_id"), attr(sp, "provider")), (change_id.clone(), FILE.to_string()), "{mesh}: unblocked onto {FILE} under the change: {sp}");
            assert!(read.iter().any(|r| start_ns(r) <= start_ns(sp)), "{mesh}: unblocked only once no commit is read: {sp}");
        }
    }
    estate.record_trace_url(&s(&read[0]["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "No Build is admitted while a change is in progress, a change
/// of `topology_storage_provider` included; `fabric.delete` alone is admitted"; "It is admitted and
/// refused as any Build is, one in progress at a time"; i149 R17, R18): while a change to `test-target`
/// is held after every mesh-primary replied `blocked`, a Create of one more node and a change back to
/// `file` are each refused `build-in-progress` naming the change's Build, and
/// nothing of them is accepted; the change then completes, the Fabric row naming `test-target` with
/// `file` previous. Must NOT happen: a Build accepted during the change, or the change ended by the
/// refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_other_than_fabric_delete_is_refused_during_a_change() {
    let test = "a_build_other_than_fabric_delete_is_refused_during_a_change";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let hold = hold_switch(&estate).await;
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &change_id, "mesh2").await;
    let mut refusals = Vec::new();
    let current = estate.current().await.build_id;
    // A Build beside the change: refused `build-in-progress`, naming no Build.
    let request = estate.create_node_request("mesh1", "node").await;
    let build = estate.carrier().await.create(rdm_client::Change::Create { meshes: Vec::new(), nodes: vec![request] }).await;
    match build {
        Err(CallEnd::Rejected { reason, detail }) if reason == "build-in-progress" => refusals.push(json!({"what": "a Build", "reason": reason, "detail": detail})),
        Err(other) => panic!("a Build during the change ended otherwise than build-in-progress: {other}"),
        Ok(accepted) => panic!("a Build was accepted during the change as Build {}", accepted.build_id()),
    }
    // A second change beside it: refused `build-in-progress`, naming the change in progress.
    match estate.fabrics_obj().await.update_storage(FILE).await {
        Err(CallEnd::Rejected { reason, detail }) if reason == "build-in-progress" => {
            assert!(detail.contains(&change_id), "the second change's refusal names the change in progress: {detail}");
            refusals.push(json!({"what": "a change back to file", "reason": reason, "detail": detail}));
        }
        Err(other) => panic!("a change back to file during the change ended otherwise than build-in-progress: {other}"),
        Ok(second) => panic!("a change back to file was started during the change as {second}"),
    }
    estate.artifact("refusals.json", &json!(refusals));
    hold.release("switch").await;
    let done = ended(&estate, &run).await;
    estate.artifact("ended.json", &done);
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    assert_eq!(estate.current().await.build_id, current, "no Build was accepted beside the change");
    assert!(completed(&done), "the change completes: {done}");
    estate.stop().await;
    let spans = estate.spans();
    assert_eq!(of_change(&spans, SWITCH, &change_id, None).into_iter().filter(|sp| sp["attributes"]["outcome"] == "inserted").count(), 1, "the change switched once");
    estate.record_trace_url(&s(&blocked["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "The change has a durable `change_id`, recorded with its Build
/// before its first command; its commands carry it, and a node refuses by name a command of an older
/// one"; i149 R11): a first change to `test-target` runs while mesh2's mesh-primary holds its first move
/// command to mesh2.node.1 on its way (the testkit's storage-step cut); the mesh-primary sends it again
/// and the change completes. A second change, back to `file`, completes, mesh2.node.1 moving under it, and
/// the held command of the first change is then released to mesh2.node.1. mesh2.node.1 refuses every
/// command of the first change it receives after its move under the second: its only move after that is
/// none, and it writes `file`. Must NOT happen: a move of the node by the first change's `change_id`
/// after its move under the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_change_cannot_unblock_writes_for_a_new_change() {
    let test = "an_old_change_cannot_unblock_writes_for_a_new_change";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let role_id = node_id_of(&estate, &role()).await;
    // mesh2's mesh-primary holds the first change's first move command of mesh2.node.1 on its way.
    let carrier = rdm_test::faults::FaultClient::at(&estate, &rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).await;
    carrier.arm("old-command", json!({"kind": "storage-step", "node": role()})).await;
    let (first, first_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    carrier.wait_held("old-command").await;
    ended(&estate, &first).await;
    let old_change = attr(&wait_for(&format!("{MOVE} of the role node under Build {first_id}"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == MOVE && attr(sp, "change_id") == first_id && attr(sp, "node_id") == role_id && sp["attributes"]["outcome"] == "moved")
    }).await, "change_id");
    let (second, second_id) = change_to(&estate, FILE).await;
    ended(&estate, &second).await;
    let moved = wait_for(&format!("{MOVE} of the role node under Build {second_id}"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == MOVE && attr(sp, "change_id") == second_id && attr(sp, "node_id") == role_id && sp["attributes"]["outcome"] == "moved")
    }).await;
    let new_change = attr(&moved, "change_id");
    assert_ne!(old_change, new_change, "each change has its own change_id");
    carrier.release("old-command").await;
    wait_for(&format!("the first change's held command reaches the role node"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == MOVE && attr(sp, "node_id") == role_id && attr(sp, "change_id") == old_change && start_ns(sp) > start_ns(&moved))
    }).await;
    fabric_names(&estate.fabric_view().await, FILE, &[FILE, TOPOLOGY_STORAGE_TARGET]);
    estate.stop().await;
    let spans = estate.spans();
    let switched = of_change(&spans, SWITCH, &second_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the second change switched");
    let at_node: Vec<&Value> = spans.iter().filter(|sp| sp["name"] == MOVE && attr(sp, "node_id") == role_id && start_ns(sp) > start_ns(&moved)).collect();
    let stale: Vec<&&Value> = at_node.iter().filter(|sp| attr(sp, "change_id") == old_change).collect();
    assert!(!stale.is_empty(), "the first change's held command reached the node after it moved under the second: {at_node:#?}");
    assert!(stale.iter().all(|sp| sp["attributes"]["outcome"] == "rejected-stale-change"), "every command of the older change is refused by name: {stale:#?}");
    assert!(at_node.iter().all(|sp| sp["attributes"]["outcome"] != "moved"), "the node does not move after its move under the second change: {at_node:#?}");
    assert!(start_ns(&moved) >= start_ns(switched) && attr(&moved, "provider") == FILE, "the node's move under the second change is onto {FILE}, after its switch: {moved}");
    estate.record_trace_url(&s(&moved["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it" (Q198): step 1, a mesh-primary that "has replied `blocked`
/// issues no new command until it is unblocked"; step 2, "Once every mesh-primary has replied `blocked`,
/// the fabric-primary appends every line of every store on the old provider to the new one in source
/// order"; step 4, "Each mesh-primary moves its nodes to the new provider"; Q169: a connection event
/// never blocks an open): mesh1.node.1, whose mesh-primary has replied `blocked`, dials mesh1.node-admin.2, a
/// node it holds no connection to, while the change is held before its copy; the dial is answered at
/// once and its connection event is written once, on `file`, the provider the node writes until its
/// mesh-primary moves it; the copy brings that line to `test-target` once; the node then writes
/// `test-target`. Must NOT happen: the open waiting on the write, or the event lost or landed twice on
/// either provider.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_event_during_a_change_lands_once_after_the_unblock() {
    let test = "a_connection_event_during_a_change_lands_once_after_the_unblock";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let source_view = estate.node_view(&PathName::new("mesh1.node", "1").unwrap()).await;
    let source = source_view.node_id.to_string();
    let destination = node_id_of(&estate, &PathName::new("mesh1.node-admin", "2").unwrap()).await;
    let (file_root, target_root) = roots(&estate);
    let hold = hold_switch(&estate).await;
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &change_id, "mesh2").await;
    let dialled_ns = now_ns();
    dial(&estate, &source, &destination);
    wait_for(&format!("{source}'s connection event is written"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == OBSERVED && attr(sp, "source_id") == source && attr(sp, "destination_id") == destination && start_ns(sp) >= dialled_ns)
    }).await;
    hold.release("switch").await;
    ended(&estate, &run).await;
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    // The estate's stop deletes every connection: the events it causes are not the change's.
    let stopped_ns = now_ns();
    estate.stop().await;
    let spans = estate.spans();
    let owed: Vec<&Value> = named(&spans, OBSERVED).into_iter().filter(|sp| attr(sp, "source_id") == source && attr(sp, "destination_id") == destination && attr(sp, "origin") == "dial" && (dialled_ns..stopped_ns).contains(&start_ns(sp))).collect();
    assert_eq!(owed.len(), 1, "the connection event during the change is written once: {owed:#?}");
    let entry = attr(owed[0], "entry");
    assert_eq!((lines_naming(&file_root, &entry).len(), lines_naming(&target_root, &entry).len()), (1, 1), "the connection event {entry} is on {FILE} once, where the node wrote it, and on {TOPOLOGY_STORAGE_TARGET} once, the copy's");
    let unblocked = of_change(&spans, UNBLOCK, &change_id, Some("mesh1")).into_iter().find(|sp| sp["attributes"]["outcome"] == "unblocked").expect("mesh1's mesh-primary unblocked");
    assert_eq!(attr(unblocked, "provider"), TOPOLOGY_STORAGE_TARGET, "mesh1's mesh-primary moves its nodes onto {TOPOLOGY_STORAGE_TARGET}: {unblocked}");
    estate.record_trace_url(&s(&blocked["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it" step 4: "Each mesh-primary moves its nodes to the new provider,
/// then replies `unblocked`"; a lost node "is investigated, and on any verdict but `reached` node
/// recovery re-creates it, the primary's own workflow, never part of the change. After the switch the
/// change completes, its `rdm.storage.ended` event written, once every mesh-primary has replied
/// `unblocked`; nothing else waits"; Q182: a node not replying to commands is silent): the role node is
/// frozen (alive and silent) after its mesh-primary replied `blocked`, so it leaves its move unanswered.
/// Its mesh-primary's node recovery, admitted once the mesh-primary has moved onto `test-target`,
/// re-creates it at mesh2.node.1 under a new node id launched on `test-target`; the mesh-primary replies
/// `unblocked` and the change ends: the new birth writes `test-target`, is ready, its records are on
/// `test-target` and none on `file`; the old birth never moves; the Fabric row names `test-target` with
/// `file` previous. Must NOT happen: the change waiting on the frozen birth forever (its recovery held
/// by the block while the unblock waits on it), the old birth moving, or the new birth launched on `file`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_never_unblocks_is_recreated_on_the_new_provider() {
    let test = "a_node_that_never_unblocks_is_recreated_on_the_new_provider";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let old_id = node_id_of(&estate, &role()).await;
    let hold = hold_switch(&estate).await;
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &change_id, "mesh2").await;
    let silent = freeze(&estate, &role()).await;
    hold.release("switch").await;
    let done = ended(&estate, &run).await;
    estate.artifact("ended.json", &done);
    let renewed = estate.until_node(&role(), "the role node re-created at its (path, name)", SETTLE, |v| v.filter(|v| v.node_id.to_string() != old_id && v.state == NodeStatus::Ready).cloned()).await;
    let new_id = renewed.node_id.to_string();
    assert!(!lines_naming(&target_root, &new_id).is_empty() && lines_naming(&file_root, &new_id).is_empty(), "the re-created birth's records are on {TOPOLOGY_STORAGE_TARGET} and none on {FILE}");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    thaw(&silent);
    estate.stop().await;
    let spans = estate.spans();
    assert!(named(&spans, MOVE).iter().filter(|sp| attr(sp, "node_id") == old_id).all(|sp| sp["attributes"]["outcome"] != "moved"), "the frozen birth never moves");
    let switched = of_change(&spans, SWITCH, &change_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the change switched");
    assert!(start_ns(switched) >= start_ns(&blocked), "the switch follows the mesh-primary's blocked");
    estate.record_trace_url(&s(&switched["trace_id"]));
}

// @feature: node-records
/// Before-copy nonresponse uses the existing command timer, investigation and recovery. The
/// test never invokes recovery; the same storage Build resumes when its affected mesh changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unanswered_storage_block_uses_normal_investigation_before_copy() {
    let test = "an_unanswered_storage_block_uses_normal_investigation_before_copy";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let old_id = node_id_of(&estate, &role()).await;
    let own = estate.probe(&["own-config", "--target", &format!("exact:{old_id}"), "--value", "source=before-pause"]);
    assert_eq!(own["own_config"], "config-updated", "{own}");
    let hold = hold_switch(&estate).await;
    let paused = freeze(&estate, &role()).await;
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    hold.wait_held("switch").await;
    let replacement = estate.node_view(&role()).await;
    assert_ne!(replacement.node_id.to_string(), old_id, "the normal investigation/recovery completes before the copy is entered");
    assert_eq!(replacement.config, BTreeMap::from([("source".into(), "before-pause".into())]));
    let successor = replacement.node_id.to_string();
    assert!(!lines_naming(&file_root, &successor).is_empty() && lines_naming(&target_root, &successor).is_empty(), "before copy the successor is created on the old provider, {FILE}");
    let blocked = blocked_at(&estate, &change_id, "mesh2").await;
    let before_copy = estate.spans();
    assert!(of_change(&before_copy, SWITCH, &change_id, None).iter().all(|sp| sp["attributes"]["outcome"] != "inserted"), "no switch before the normal lifecycle and all blocks finish");
    hold.release("switch").await;
    ended(&estate, &run).await;
    let after = estate.node_view(&role()).await;
    assert_eq!(after.node_id, replacement.node_id);
    assert_eq!(after.config, replacement.config);
    thaw(&paused);
    assert_eq!(estate.node_view(&role()).await.node_id, replacement.node_id, "a returning deleted birth cannot become the current node");
    estate.stop().await;
    let spans = estate.spans();
    assert!(named(&spans, "rdm.node_admin.node.update.via-true-offline").iter().any(|sp| attr(sp, "node_id") == old_id), "the existing investigation clock reaches its normal lifecycle verdict");
    assert!(named(&spans, MOVE).iter().any(|sp| attr(sp, "node_id") == successor && sp["attributes"]["outcome"] == "moved" && attr(sp, "provider") == TOPOLOGY_STORAGE_TARGET), "the successor moves onto {TOPOLOGY_STORAGE_TARGET} under the change");
    estate.record_trace_url(&s(&blocked["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it" step 4: "Each mesh-primary moves its nodes to the new provider,
/// then replies `unblocked`"; "A node that refuses its move is re-created on the new provider by its
/// mesh-primary's node recovery, never part of the change, before its mesh-primary replies
/// `unblocked`"): the role node refuses its move by name (the testkit's knob has its birth refuse to open
/// `test-target`) and keeps answering and gossiping. mesh2's mesh-primary hands it to its node recovery
/// (`via-storage-move-lost` `recovery-submitted`), which re-creates it at mesh2.node.1 under a new node id
/// launched on `test-target`: the new birth's row is the recovery's create, seeded from the refusing
/// birth and written by the mesh-primary, naming no storage change. The mesh-primary replies `unblocked`
/// only once the new birth has moved: when the change ends the path holds the new birth, ready, writing
/// `test-target`, its records on `test-target` and none on `file`; the old birth never moves; `file`
/// holds nothing the copy did not carry to `test-target`. Must NOT happen: `unblocked` with the refusing
/// birth standing, the re-create run as a step of the unblock or the change, the old birth left writing
/// `file` after the switch, or the new birth launched on `file`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_refuses_its_move_is_recreated_on_the_new_provider() {
    let test = "a_node_that_refuses_its_move_is_recreated_on_the_new_provider";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let old_id = node_id_of(&estate, &role()).await;
    let data_root = PathBuf::from(estate.data_dir(&role()).await).parent().expect("a data dir is under the estate's root").to_path_buf();
    std::fs::write(rdm_test::node::target_refused_knob(&data_root, &old_id), b"").unwrap();
    let hold = hold_switch(&estate).await;
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &change_id, "mesh2").await;
    hold.release("switch").await;
    let done = tokio::time::timeout(SETTLE, ended(&estate, &run)).await.unwrap_or_else(|_| panic!("storage change {change_id} did not end within {SETTLE:?}: mesh2's unblock waits on the refusing birth's re-create"));
    estate.artifact("ended.json", &done);
    // At the Build's completion, with no wait: the path holds the re-created birth.
    let at_completion = estate.node_view(&role()).await;
    assert_ne!(at_completion.node_id.to_string(), old_id, "mesh2's mesh-primary replied unblocked only once the refusing birth was re-created: {at_completion:?}");
    let renewed = estate.until_node(&role(), "the role node re-created at its (path, name), ready", SETTLE, |v| v.filter(|v| v.node_id.to_string() != old_id && v.state == NodeStatus::Ready).cloned()).await;
    let new_id = renewed.node_id.to_string();
    assert!(!lines_naming(&target_root, &new_id).is_empty() && lines_naming(&file_root, &new_id).is_empty(), "the re-created birth's records are on {TOPOLOGY_STORAGE_TARGET} and none on {FILE}");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    estate.stop().await;
    // Nothing is written to the old provider after the copy: every line it holds was carried.
    history_carried(&store_lines(&file_root), &store_lines(&target_root));
    let spans = estate.spans();
    let old_moves: Vec<&Value> = named(&spans, MOVE).into_iter().filter(|sp| attr(sp, "node_id") == old_id).collect();
    assert!(!old_moves.is_empty() && old_moves.iter().all(|sp| sp["attributes"]["outcome"] == "refused"), "the old birth refuses its move by name and never moves: {old_moves:#?}");
    let unblocked = of_change(&spans, UNBLOCK, &change_id, Some("mesh2")).into_iter().find(|sp| sp["attributes"]["outcome"] == "unblocked").expect("mesh2's mesh-primary replied unblocked");
    assert_eq!(attr(unblocked, "provider"), TOPOLOGY_STORAGE_TARGET, "mesh2's mesh-primary moves its nodes onto {TOPOLOGY_STORAGE_TARGET}: {unblocked}");
    assert_eq!(attr(unblocked, "recreated"), "1", "the mesh-primary that replied unblocked waited on the refusing birth's re-create: {unblocked}");
    let lost: Vec<&Value> = of_change(&spans, MOVE_LOST, &change_id, Some("mesh2")).into_iter().filter(|sp| attr(sp, "node_id") == old_id).collect();
    assert!(lost.iter().any(|sp| sp["attributes"]["outcome"] == "recovery-submitted"), "the refusing birth is handed to its mesh-primary's node recovery: {lost:#?}");
    let recreated = named(&spans, "rdm.node_admin.node.create.via-node-recovery").into_iter().find(|sp| attr(sp, "created_node_id") == new_id).unwrap_or_else(|| panic!("the re-created birth {new_id}'s create is spanned"));
    assert!(
        attr(recreated, "node_id") == old_id && attr(recreated, "node_path") == role().path && attr(recreated, "node_name") == role().name && recreated["attributes"]["change_id"].is_null(),
        "mesh2's mesh-primary's node recovery of the refusing birth re-creates it at its (path, name), never a step of the change: {recreated}"
    );
    // The re-created birth writes its own record (Q356) on the new provider, which node.get reads
    // (the file store root holds the records written before the move only).
    let reread = estate.node_view(&role()).await;
    assert_eq!(reread.node_id.to_string(), new_id, "the re-created birth's own record answers node.get at its (path, name): {reread:?}");
    let new_moved = named(&spans, MOVE).into_iter().find(|sp| attr(sp, "node_id") == new_id && attr(sp, "change_id") == change_id && sp["attributes"]["outcome"] == "moved").unwrap_or_else(|| panic!("the re-created birth {new_id} answered its move under Build {change_id}"));
    assert!(new_moved["end_unix_nano"].as_u64().unwrap_or(u64::MAX) <= unblocked["end_unix_nano"].as_u64().unwrap_or(0), "the re-created birth moved before its mesh-primary replied unblocked: {new_moved} {unblocked}");
    estate.record_trace_url(&s(&blocked["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "`fabric.delete` alone is admitted, and it interrupts the change
/// as it interrupts any Build, the records on both providers kept"; "Whoever resumes the change learns the
/// decision only from that record"; i149 R12: "fabric.delete resolve[s] the current migration before
/// issuing ordinary writes"): while a change to `test-target` is held after every mesh-primary replied
/// `blocked`, `fabric.delete` is admitted and completes. Before its teardown begins, the change's
/// `commit` record is read (`commit_found = false`: not switched), and the change is never switched (no
/// switch inserted); every line `file` held at the delete's acceptance is still on `file` and every line
/// `test-target` held then is still there. Must NOT happen: the delete refused, its teardown begun before
/// the change is resolved, the change switched under it, or a record of either provider removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_resolves_the_storage_change_before_teardown() {
    let test = "a_delete_resolves_the_storage_change_before_teardown";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let hold = hold_switch(&estate).await;
    let (_run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    blocked_at(&estate, &change_id, "mesh2").await;
    let accepted_ns = now_ns();
    let delete = estate.carrier().await.create(Change::DeleteFabric).await.unwrap_or_else(|end| panic!("fabric.delete during the change is admitted: {end}"));
    let delete_id = delete.build_id().to_string();
    let (file_at, target_at) = (store_lines(&file_root), store_lines(&target_root));
    let followed = estate.follow(delete).await;
    estate.artifact("frames.json", &json!(format!("{:?}", followed.frames)));
    // The change ended failed, interrupted by the delete, before the delete's teardown.
    let done = ended(&estate, &change_id).await;
    estate.artifact("ended.json", &done);
    assert_eq!(failed_reason(&done).as_deref(), Some("interrupted by fabric.delete"), "the delete ends the change failed: {done}");
    let (file_after, target_after) = (store_lines(&file_root), store_lines(&target_root));
    for (store, lines) in &file_at {
        assert!(file_after[store].len() >= lines.len() && file_after[store][..lines.len()] == lines[..], "{store}: every line {FILE} held at the delete's acceptance is kept");
    }
    for (store, lines) in &target_at {
        assert!(target_after[store].len() >= lines.len() && target_after[store][..lines.len()] == lines[..], "{store}: every line {TOPOLOGY_STORAGE_TARGET} held at the delete's acceptance is kept");
    }
    let _ = hold;
    estate.stop().await;
    let spans = estate.spans();
    let resolved = of_change(&spans, SWITCH, &change_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found") == Some(false) && start_ns(sp) >= accepted_ns).min_by_key(|sp| start_ns(sp)).unwrap_or_else(|| panic!("the delete's executor read the change's commit record (commit_found = false) for ({change_id}, {change_id})"));
    assert!(of_change(&spans, SWITCH, &change_id, None).iter().all(|sp| sp["attributes"]["outcome"] != "inserted"), "the change is never switched under the delete");
    let teardown = spans.iter().filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_admin.node.delete.") || n.starts_with("rdm.mesh.node.delete.")) && start_ns(sp) >= accepted_ns).map(start_ns).min().expect("the delete tore the nodes down");
    assert!(start_ns(resolved) <= teardown, "the change is resolved before the delete's teardown begins (delete {delete_id})");
    estate.record_trace_url(&s(&resolved["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "The change has a durable `change_id`, recorded as its
/// `rdm.storage.started` event before its first command"; "Whoever resumes the change learns the
/// decision only from that record"; build.md §5: the store answers "the storage changes' events
/// (`type` `rdm.storage.*`, in event.id order)"): a change to `test-target`, started by
/// `fabric.storage.update` and held after every mesh-primary replied `blocked`, loses its
/// fabric-primary (the Day-0 admin, SIGKILL). The successor fabric-primary reads the storage changes'
/// events, finds the started change with no `rdm.storage.ended` event and resumes it from its events:
/// it completes, its `rdm.storage.ended` event is on `test-target` naming it complete, and the Fabric
/// row names `test-target` with `file` previous. Must NOT happen: the change left blocked with its
/// fabric-primary, or resumed by the admin that started it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_fabric_primarys_successor_resumes_the_change_from_its_events() {
    let test = "a_lost_fabric_primarys_successor_resumes_the_change_from_its_events";
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let first = views.iter().find(|v| v.is_fabric_primary).cloned().expect("the fabric-primary");
    let (file_root, _) = roots(&estate);
    let hold = hold_switch(&estate).await;
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    blocked_at(&estate, &change_id, "mesh2").await;
    let started: Vec<String> = lines_naming(&file_root, &change_id);
    assert!(std::fs::read_to_string(file_root.join(rdm_admin::store_root::EVENTS_FILE)).unwrap_or_default().lines().any(|l| l.contains("\"rdm.storage.started\"") && l.contains(&change_id)), "the change's rdm.storage.started event is on {FILE} before its first command: {started:?}");
    estate.kill_bootstrap();
    let done = ended(&estate, &run).await;
    estate.artifact("ended.json", &done);
    assert!(completed(&done), "the successor completes the change: {done}");
    assert_eq!(done["data"]["topology_storage_provider"], TOPOLOGY_STORAGE_TARGET, "the change ends on the provider it decided: {done}");
    let _ = hold;
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    estate.stop().await;
    let spans = estate.spans();
    let resumed: Vec<&Value> = named(&spans, "rdm.node_admin.storage.update.via-started").into_iter().filter(|sp| attr(sp, "change_id") == change_id && sp["attributes"]["outcome"] == "resumed").collect();
    assert!(!resumed.is_empty() && resumed.iter().all(|sp| attr(sp, "node_id") != first.node_id.to_string()), "a successor fabric-primary resumed the change from its events: {resumed:#?}");
    let switched: Vec<&Value> = of_change(&spans, SWITCH, &change_id, None).into_iter().filter(|sp| sp["attributes"]["outcome"] == "inserted").collect();
    assert_eq!(switched.len(), 1, "the change switched once: {switched:#?}");
    estate.record_trace_url(&s(&resumed[0]["trace_id"]));
}

// ---- e5: the customer's change end to end (i149 R13–R15; storage.md §2 "Changing it"). The fast gate
// runs one cell of this stem (the serving births through a change, process provider); the release suite
// runs the whole stem, and its container-proof job the cells of [`container`].

/// The node store's file on a provider's root.
const NODES: &str = rdm_admin::store_root::NODES_FILE;
/// The node created after a change, by a Build that omits the provider (Q166).
fn grown_location() -> PathName { PathName::new("mesh1.node", "2").unwrap() }

/// A fabric of the faulted node-admin and the testkit node on the deployment provider `provider`
/// (`process` or `container`), grown to [`two_meshes`], every node ready.
async fn two_mesh_estate_on(test: &str, provider: &str) -> (Estate, Vec<NodeView>) {
    let sha = candidate_sha();
    let mut set = binding_set(&sha);
    if provider == "container" {
        for b in set.bindings.iter_mut() {
            b.image = Some(rdm_test::estate::RUNTIME_IMAGE.into());
        }
    }
    let owner = Owner { provider: provider.into(), ..owner(test) };
    let mut estate = Estate::bootstrap_on_with_env(owner, "fabric1", "mesh1", &set, &sha, &[], &[]).await.expect("the faulted-admin binding set is accepted");
    estate.resize(&two_meshes()).await;
    let views = estate.settles_to_named(&two_mesh_names(), SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    (estate, views)
}

/// [`two_meshes`] with mesh1 grown by one node, [`grown_location`].
fn grown() -> FabricDesired {
    let mut fabric = two_meshes();
    fabric.meshes[0].counts.insert("node".into(), 2);
    fabric
}

/// `node.get [id: x]` of every node of `views` by its minted id, at the fabric-primary: `(path, name)` →
/// (node id, incarnation, status).
async fn node_gets(estate: &Estate, views: &[NodeView]) -> BTreeMap<PathName, (String, Option<String>, NodeStatus)> {
    let nodes = estate.nodes_obj().await;
    let mut out = BTreeMap::new();
    for v in views {
        let mut got = nodes.get(&rdm_client::Filter::new().eq("id", [v.node_id.to_string()])).await.unwrap_or_else(|e| panic!("node.get [id: {}] ({:?}) is answered: {e}", v.node_id, v.name));
        assert_eq!(got.len(), 1, "node.get [id: {}] ({:?}) answers the one node: {got:?}", v.node_id, v.name);
        let got = got.remove(0);
        out.insert(got.location(), (got.node_id.to_string(), got.incarnation.as_ref().map(|i| i.as_str().to_string()), got.state));
    }
    out
}

/// Every birth of `before` is the node `node.get` answers at its `(path, name)` in `got`, ready.
fn serving(got: &BTreeMap<PathName, (String, Option<String>, NodeStatus)>, before: &BTreeMap<PathName, (String, Option<String>)>, when: &str) {
    for (name, (id, incarnation)) in before {
        let (got_id, got_incarnation, status) = got.get(name).unwrap_or_else(|| panic!("{when}: node.get answers no {name:?}: {got:?}"));
        assert_eq!((got_id, got_incarnation), (id, incarnation), "{when}: {name:?} is the same birth (node id, incarnation)");
        assert_eq!(*status, NodeStatus::Ready, "{when}: {name:?} is ready");
    }
}

/// The node store's lines on `root` from line `from` on, each as JSON (`{"subject", "data", ...}`).
fn node_lines_from(root_lines: &BTreeMap<&'static str, Vec<String>>, from: usize) -> Vec<Value> {
    root_lines[NODES].iter().skip(from).filter_map(|l| serde_json::from_str(l).ok()).collect()
}

/// `source` dials `destination`, a node it holds no connection to, and its connection event is written:
/// the event's entry, as its span names it.
async fn connection_event(estate: &Estate, source: &str, destination: &str) -> String {
    let dialled_ns = now_ns();
    dial(estate, source, destination);
    let sp = wait_for(&format!("{source}'s connection event to {destination} is written"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == OBSERVED && attr(sp, "source_id") == source && attr(sp, "destination_id") == destination && attr(sp, "origin") == "dial" && start_ns(sp) >= dialled_ns)
    })
    .await;
    attr(&sp, "entry")
}

/// The serving births through a change (i149 R13), on the deployment provider `provider`.
async fn every_node_serves_through_a_change(test: &str, provider: &str) {
    let (mut estate, views) = two_mesh_estate_on(test, provider).await;
    let before = births(&views);
    serving(&node_gets(&estate, &views).await, &before, "before the change");
    let (file_root, target_root) = roots(&estate);
    let file_before = store_lines(&file_root);
    let hold = hold_switch(&estate).await;
    let accepted_ns = now_ns();
    let (run, change_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    for mesh in ["mesh1", "mesh2"] {
        blocked_at(&estate, &change_id, mesh).await;
    }
    // Throughout: every mesh-primary has replied `blocked` and the change is held before its copy.
    serving(&node_gets(&estate, &views).await, &before, "every mesh-primary blocked");
    hold.release("switch").await;
    let done = ended(&estate, &run).await;
    estate.artifact("ended.json", &done);
    serving(&node_gets(&estate, &views).await, &before, "after the change");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    // The record: every line of a serving node's record written from the change's acceptance on, on
    // either provider, records the same node at its (path, name) in the state its record held before the
    // change (no node changes state). A move writes no line (Q361: the provider is the Fabric record's).
    let (file_after, target_after) = (store_lines(&file_root), store_lines(&target_root));
    history_carried(&file_after, &target_after);
    let held = node_lines_from(&file_before, 0);
    let written: Vec<Value> = node_lines_from(&file_after, file_before[NODES].len()).into_iter().chain(node_lines_from(&target_after, file_after[NODES].len())).collect();
    estate.artifact("rows-written.json", &json!(written));
    for (name, (id, _)) in &before {
        let status = &held.iter().rev().find(|l| l["subject"] == id.as_str()).unwrap_or_else(|| panic!("{name:?}: its record before the change"))["data"]["state"];
        let own: Vec<&Value> = written.iter().filter(|l| l["subject"] == id.as_str()).collect();
        assert!(own.iter().all(|l| l["data"]["state"] == *status && rdm_test::estate::is_location(&l["data"], name)), "{name:?}: every line of its record written during the change records the same node at its (path, name) in the state {status} its record held before: {own:#?}");
    }
    let stopped_ns = now_ns();
    estate.stop().await;
    let spans = estate.spans();
    // No process of a serving node started again, and no node was created or left during the change.
    for (name, (id, incarnation)) in &before {
        let boots: Vec<&Value> = named(&spans, "rdm.mesh.node.create.via-boot").into_iter().filter(|sp| attr(sp, "node_id") == *id).collect();
        assert_eq!(boots.len(), 1, "{name:?}: its birth's process started once: {boots:#?}");
        assert_eq!(Some(attr(boots[0], "incarnation_id")), incarnation.clone(), "{name:?}: the one start is its incarnation's");
        assert!(start_ns(boots[0]) < accepted_ns, "{name:?}: its process started before the change");
        let moved: Vec<&Value> = of_change(&spans, MOVE, &change_id, None).into_iter().filter(|sp| attr(sp, "node_id") == *id).collect();
        assert!(!moved.is_empty() && moved.iter().all(|sp| sp["attributes"]["outcome"] == "moved" && attr(sp, "provider") == TOPOLOGY_STORAGE_TARGET), "{name:?}: its birth moved onto {TOPOLOGY_STORAGE_TARGET} in place: {moved:#?}");
    }
    let lifecycle: Vec<&Value> = spans
        .iter()
        .filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.node.create.") || n.starts_with("rdm.mesh.node.delete.") || n.starts_with("rdm.node_admin.node.create.via-build") || n.starts_with("rdm.node_admin.node.delete.")))
        .filter(|sp| (accepted_ns..stopped_ns).contains(&start_ns(sp)))
        .collect();
    assert!(lifecycle.is_empty(), "no node was created, started or deleted during the change: {lifecycle:#?}");
    let switched = of_change(&spans, SWITCH, &change_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the change switched");
    estate.record_trace_url(&s(&switched["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": a storage change "is a property Build: the fabric, every mesh
/// and every node stay ready, no node restarts, and no node changes state"; i149 R13):
/// on a two-mesh fabric, `node.get` of every node by its id answers the same birth (node id and
/// incarnation), ready, before the change to `test-target`, while every mesh-primary has
/// replied `blocked` and the change is held before its copy, and after the Build completes; every line of
/// a serving node's record written during the change, on either provider, records the same node at its
/// (path, name) in the state its record held before the change; each birth's process started once, before the
/// change, and moved onto `test-target` in place; no node is created, started or deleted during the
/// change. Must NOT happen: a node restarted, re-born or replaced, a read unanswered while blocked, or a
/// record line in another state. The fast gate's cell of this stem.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_serving_node_keeps_its_birth_and_ready_through_a_change() {
    every_node_serves_through_a_change("every_serving_node_keeps_its_birth_and_ready_through_a_change", "process").await;
}

/// CONTRACT (storage.md §2 "Changing it": "The previous provider is kept as it was at the switch, named on
/// the Fabric row, written no more and never deleted by RDM"; step 3, the switch "appends the Fabric row's
/// line naming the new provider current" on the new provider; i149 R14): after a change to `test-target`
/// completes, every store on `file` reads whole (every line verified), its Fabric row still names `file`
/// with no previous provider while `test-target`'s names `test-target` with `file` previous, and every
/// serving node's record reads there. A Build that omits the provider then creates mesh1.node.2 and that node
/// dials mesh1.node-admin.2: its records and its connection event are on `test-target` and none on `file`, and
/// every line of `file` is the line it held at the cutover. Must NOT happen: a line of the previous
/// provider changed, added or removed, or a write after the cutover landing there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_previous_provider_remains_unchanged_after_cutover() {
    let test = "a_previous_provider_remains_unchanged_after_cutover";
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let (run, _) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    ended(&estate, &run).await;
    let cutover = store_lines(&file_root);
    estate.artifact("previous-at-cutover.json", &json!(cutover));
    // Readable: every store of the previous provider reads whole, every line verified.
    for store in STORES {
        let read = rdm_admin::store_log::read_file(&file_root.join(store)).unwrap_or_else(|e| panic!("{store} on {FILE} reads whole after the cutover: {e}"));
        assert_eq!(read.len(), cutover[store].iter().filter(|l| !l.is_empty()).count(), "{store} on {FILE}: every line reads");
    }
    let fabric_on = |root: &Path| rdm_admin::store_log::read_record(&root.join(rdm_admin::store_root::FABRIC_FILE), "fabric-record", None).unwrap_or_else(|| panic!("the Fabric row reads on {}", root.display()));
    let (old, new) = (fabric_on(&file_root), fabric_on(&target_root));
    assert_eq!((&old["data"]["topology_storage_provider"], old["data"]["previous_topology_storage_providers"].as_array().map_or(0, Vec::len)), (&json!(FILE), 0), "the Fabric row on {FILE} is the one before the switch: {old}");
    assert_eq!((&new["data"]["topology_storage_provider"], &new["data"]["previous_topology_storage_providers"]), (&json!(TOPOLOGY_STORAGE_TARGET), &json!([FILE])), "the Fabric row on {TOPOLOGY_STORAGE_TARGET} names it, {FILE} previous: {new}");
    for v in &views {
        assert!(rdm_admin::store_log::read_record(&file_root.join(NODES), "node-record", Some(v.node_id.as_str())).is_some(), "{:?}: its record reads on {FILE}", v.name);
    }
    // Writes after the cutover: a node created by a Build that omits the provider, and its connection event.
    estate.resize(&grown()).await;
    let mut names = two_mesh_names();
    names.insert(grown_location());
    let grown_id = estate.settles_to_named(&names, SETTLE).await.into_iter().find(|v| v.location() == grown_location()).expect("the grown node").node_id.to_string();
    let peer_id = node_id_of(&estate, &PathName::new("mesh1.node-admin", "2").unwrap()).await;
    let entry = connection_event(&estate, &grown_id, &peer_id).await;
    assert!(!lines_naming(&target_root, &grown_id).is_empty() && lines_naming(&file_root, &grown_id).is_empty(), "the grown node's records are on {TOPOLOGY_STORAGE_TARGET} and none on {FILE}");
    assert_eq!((lines_naming(&target_root, &entry).len(), lines_naming(&file_root, &entry).len()), (1, 0), "the connection event {entry} is on {TOPOLOGY_STORAGE_TARGET} once and not on {FILE}");
    assert_eq!(store_lines(&file_root), cutover, "every line of {FILE} is the one it held at the cutover");
    estate.stop().await;
    let spans = estate.spans();
    let created = named(&spans, "rdm.mesh.node.create.via-boot").into_iter().find(|sp| attr(sp, "node_id") == grown_id).expect("the grown node started");
    estate.record_trace_url(&s(&created["trace_id"]));
}

/// A change to `test-target` and back to `file` (i149 R14), on the deployment provider `provider`.
async fn returns_to_a_previous_provider(test: &str, provider: &str) {
    let (mut estate, views) = two_mesh_estate_on(test, provider).await;
    let before = births(&views);
    let (file_root, target_root) = roots(&estate);
    let (first, first_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    ended(&estate, &first).await;
    let at_switch = store_lines(&file_root);
    // A line `file` lacks: a connection event written on `test-target`.
    let source_id = node_id_of(&estate, &PathName::new("mesh1.node", "1").unwrap()).await;
    let peer_id = node_id_of(&estate, &PathName::new("mesh1.node-admin", "2").unwrap()).await;
    let entry = connection_event(&estate, &source_id, &peer_id).await;
    assert_eq!((lines_naming(&target_root, &entry).len(), lines_naming(&file_root, &entry).len()), (1, 0), "the connection event {entry} is on {TOPOLOGY_STORAGE_TARGET} alone");
    let (back, back_id) = change_to(&estate, FILE).await;
    ended(&estate, &back).await;
    let (file_back, target_back) = (store_lines(&file_root), store_lines(&target_root));
    estate.artifact("file-back.json", &json!(file_back));
    // Only the lines `file` lacks are copied into it: its lines at the first switch stand, then every line
    // of `test-target` in source order follows, none twice.
    for (store, lines) in &at_switch {
        assert!(file_back[store].len() >= lines.len() && file_back[store][..lines.len()] == lines[..], "{store}: {FILE}'s lines at the first switch stand unchanged");
    }
    history_carried(&target_back, &file_back);
    assert_eq!(lines_naming(&file_root, &entry).len(), 1, "the connection event {entry} is copied into {FILE} once");
    fabric_names(&estate.fabric_view().await, FILE, &[FILE, TOPOLOGY_STORAGE_TARGET]);
    let after = estate.node_views().await;
    assert_eq!(births(&after), before, "every node is the same birth through both changes");
    assert!(after.iter().all(|v| v.state == NodeStatus::Ready), "every node is ready: {after:?}");
    // The fabric serves on `file`: a Build that omits the provider creates a node, written there alone.
    estate.resize(&grown()).await;
    let mut names = two_mesh_names();
    names.insert(grown_location());
    let grown_view = estate.settles_to_named(&names, SETTLE).await.into_iter().find(|v| v.location() == grown_location()).expect("the grown node");
    let grown_id = grown_view.node_id.to_string();
    assert!(!lines_naming(&file_root, &grown_id).is_empty() && lines_naming(&target_root, &grown_id).is_empty(), "the grown node's records are on {FILE} and none on {TOPOLOGY_STORAGE_TARGET}");
    assert_eq!(store_lines(&target_root), target_back, "{TOPOLOGY_STORAGE_TARGET} is written no more once the fabric returned to {FILE}");
    estate.stop().await;
    let spans = estate.spans();
    for id in [&first_id, &back_id] {
        assert_eq!(of_change(&spans, SWITCH, id, None).into_iter().filter(|sp| sp["attributes"]["outcome"] == "inserted").count(), 1, "Build {id} switched once");
    }
    let switched = of_change(&spans, SWITCH, &back_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the change back switched");
    estate.record_trace_url(&s(&switched["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "A later Build naming it again copies into it only the lines it
/// lacks"; "The property": the Fabric row carries `previous_topology_storage_providers`, "each provider
/// the fabric left, in order"; i149 R14): a change to `test-target` completes and mesh1.node.1's connection
/// event is written there alone; a later Build naming `file` completes: `file`'s lines at the first switch
/// stand, then every line of `test-target` follows in source order, the event once; the Fabric row names
/// `file` with `file` and `test-target` previous; every node is the same birth, ready, writing
/// `file`; a Build that omits the provider then creates mesh1.node.2, written on `file` and none on
/// `test-target`, which is written no more. Must NOT happen: a line of `file` rewritten, reordered or
/// copied twice, or a write after the return landing on `test-target`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_customer_returns_to_a_previous_topology_provider() {
    returns_to_a_previous_provider("a_customer_returns_to_a_previous_topology_provider", "process").await;
}

/// The container provider's cells: opt-in, the container-proof step (`RDM_CONTAINER_PROOF=1`, or
/// `RDM_REQUIRE_CONTAINER=1`); otherwise each skips by name. Each is ignored by name until a role node's
/// container reaches the testkit's second provider: run opted in at 17fbf387e, every node-admin moved
/// onto `test-target` and each node refused its move, `refusing to start: <estate>/store-test-target:
/// the store_dir (RDM_STORE_DIR) cannot be used`, its mesh-primary replying `unblocked` with the node
/// left on `file` and writing there after the switch.
mod container {
    /// Whether the opt-in container step runs this cell; otherwise it skips by name.
    fn opted_in(what: &str) -> bool {
        if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
            eprintln!("SKIP container {what}: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
            return false;
        }
        true
    }

    /// CONTRACT (i149 R13, R15; container provider): every serving node keeps its birth and
    /// ready through a change, as the process provider's cell states it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "i149 e5 (R15): the testkit registers test-target beside RDM_STORE_DIR, a place no role node container mounts; a role node refuses its move, and each birth its mesh-primary re-creates on test-target refuses to start (store-test-target not in its container), so the unblock re-creates it without end (storage_change.rs move_until)"]
    async fn every_serving_node_keeps_its_birth_and_ready_through_a_change_containers() {
        if !opted_in("topology storage provider") {
            return;
        }
        super::every_node_serves_through_a_change("every_serving_node_keeps_its_birth_and_ready_through_a_change_containers", "container").await;
    }

    /// CONTRACT (i149 R14, R15; container provider): a later Build naming the previous provider returns
    /// the fabric to it, as the process provider's cell states it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "i149 e5 (R15): the testkit registers test-target beside RDM_STORE_DIR, a place no role node container mounts; a role node refuses its move, and each birth its mesh-primary re-creates on test-target refuses to start (store-test-target not in its container), so the unblock re-creates it without end (storage_change.rs move_until)"]
    async fn a_customer_returns_to_a_previous_topology_provider_containers() {
        if !opted_in("topology storage provider") {
            return;
        }
        super::returns_to_a_previous_provider("a_customer_returns_to_a_previous_topology_provider_containers", "container").await;
    }
}
