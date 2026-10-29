//! product=mesh, feature=storage, subfeature=topology-storage-provider, rung=MM, provider=process and
//! container (the container cells in [`container`], opt-in).
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

use rdm_client::{BuildFrame, BuildStream, CallEnd, Change, FabricDesired, FabricView, MeshDesired, NodeStatus, NodeView, PathName};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use rdm_test::process_faults::{ExactRuntime, Fault};
use rdm_test::test_app::{refuse_list_knob, topology_storage_dir, TOPOLOGY_STORAGE_MEMORY, TOPOLOGY_STORAGE_REFUSING, TOPOLOGY_STORAGE_TARGET};
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

/// The estate grown to [`two_meshes`], every node ready and the fabric ready.
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
/// is the provider that node writes"; "A caller reads the fabric's with `fabric.get` and each node's
/// with `node.get`"; §1: `storage_persistence` "is a typed property of the node's create spec, carried
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
/// every node is the same birth, writing `file`. Must NOT happen: a Build accepted, a mesh-primary blocked, or
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
    let current = estate.current().await.based_on;
    let (_, target_root) = roots(&estate);
    std::fs::create_dir_all(target_root.parent().expect("the root is under its provider's place")).expect("test-target's place");
    let _ = std::fs::remove_dir_all(&target_root);
    std::fs::write(&target_root, b"a regular file where the fabric's root would be").expect("a file at test-target's root");
    not_opened(estate, TOPOLOGY_STORAGE_TARGET, &before, &current, "").await;
}

/// The request naming `provider` is refused by name, `topology-storage-provider-not-opened`, its detail
/// naming the provider and `why`; nothing is accepted: the current Build is `current`, the Fabric row
/// and every node still name `file`, every node is the birth `before` holds, and no mesh-primary blocked.
async fn not_opened(mut estate: Estate, provider: &str, before: &BTreeMap<String, (String, Option<String>)>, current: &rdm_client::BuildId, why: &str) {
    let change = naming_provider(&estate, Some(provider)).await;
    let refused = estate.carrier().await.create(change).await;
    let (reason, detail) = match refused {
        Err(CallEnd::Rejected { reason, detail }) => (reason, detail),
        Err(other) => panic!("the request naming {provider} ended otherwise than refused by name: {other}"),
        Ok(stream) => panic!("the request naming {provider}, which does not open, was accepted as Build {}", stream.build_id()),
    };
    estate.artifact("refusal.json", &json!({ "reason": reason, "detail": detail }));
    assert_eq!(reason, "topology-storage-provider-not-opened", "refused by name: {detail}");
    assert!(detail.contains(provider) && detail.contains(why), "the refusal names the provider and why: {detail}");
    assert_eq!(&estate.current().await.based_on, current, "the current Build is the one before the request");
    fabric_names(&estate.fabric_view().await, FILE, &[]);
    let after = estate.node_views().await;
    assert_eq!(&births(&after), before, "every node is the same birth");
    nodes_write(&after, FILE);
    estate.stop().await;
    let spans = estate.spans();
    let reject = named(&spans, "rdm.node_admin.build.reject.via-topology-storage-provider-not-opened");
    assert!(reject.iter().any(|sp| s(&sp["attributes"]["detail"]).contains(provider)), "the fabric-primary's refusal span names the provider: {reject:#?}");
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
    let current = estate.current().await.based_on;
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
    let current = estate.current().await.based_on;
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

/// CONTRACT (i149 § Seed spec, `a_customer_changes_topology_storage_without_restarting_members`; storage.md
/// §2 "Changing it": "a property Build: the fabric, every mesh and every node stay ready, no
/// node restarts, and no node changes state"; "Copy" ... "dead births' and deleted nodes' records among
/// them"; "The previous provider is kept as it was at the switch ... written no more"): a
/// two-mesh fabric with history (a deleted birth, a deleted mesh) is moved to `test-target` by one Build
/// of its complete topology, and a caller's `node.config.update` of mesh2.rpc.1 reaches mesh2's
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
    // History: a deleted birth (mesh1.rpc.2, created and removed) and a deleted mesh (mesh3).
    let mut grown = two_meshes();
    grown.meshes[0].rpc_node = 2;
    grown.meshes.push(MeshDesired { name: "mesh3".into(), node_admin: 1, rpc_node: 0, broker: 0, gateway: 0, compute: 0 });
    estate.reconcile(&grown).await;
    estate.reconcile(&two_meshes()).await;
    // A deleted mesh's last node-admin is a separate node.delete after mesh.deleted (mesh-delete.md step 7).
    Estate::workflow("node.delete of mesh3.admin.1", estate.nodes_obj().await.delete(&Estate::path("mesh3.admin.1")).await).await;
    let views = estate.settles_to_named(&two_mesh_names(), SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let before = births(&views);
    let (file_root, target_root) = roots(&estate);
    let history = store_lines(&file_root);
    assert!(history[rdm_admin::store_root::NODES_FILE].iter().any(|l| l.contains("mesh1.rpc.2")) && history[rdm_admin::store_root::MESHES_FILE].iter().any(|l| l.contains("mesh3")), "the old provider holds the deleted birth's and the deleted mesh's records");

    let stream = estate.carrier().await.create(naming_provider(&estate, Some(TOPOLOGY_STORAGE_TARGET)).await).await.unwrap_or_else(|e| panic!("the storage change naming {TOPOLOGY_STORAGE_TARGET} is accepted: {e}"));
    let build_id = stream.build_id().to_string();
    // A caller's node.config.update of a mesh2 node once mesh2's mesh-primary replied `blocked`: sent to
    // the fabric-primary (node-config-update.md step 1), which sends it down to mesh2's mesh-primary;
    // admitted there, and issued only once it is unblocked (storage.md §2 step 1).
    let blocked = blocked_at(&estate, &build_id, "mesh2").await;
    let subject = views.iter().find(|v| v.name.to_string() == "mesh2.rpc.1").cloned().expect("mesh2.rpc.1");
    let fabric_primary = views.iter().find(|v| v.is_fabric_primary).cloned().expect("the fabric-primary");
    let told: BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(subject.node_id.clone()), node_id: subject.node_id.clone(), incarnation: subject.incarnation_id.clone().expect("a live birth") };
    let carrier = estate.carrier().await;
    let (rpc, options) = (rdm_client::NodeRpc::new(carrier.client()), rdm_transport::CallOptions::default());
    let update = rpc.config_update(&fabric_primary.node_id, &rdm_transport::RouteChoice::Direct, &birth, told, &options);
    let (done, updated) = tokio::join!(estate.follow(stream), update);
    updated.unwrap_or_else(|e| panic!("node.config.update during the change is answered: {e}"));
    estate.artifact("frames.json", &json!(format!("{:?}", done.frames)));
    // The row the update wrote is on the provider its mesh-primary writes once unblocked, never the old.
    assert!(!lines_naming(&target_root, "\"tier\":\"gold\"").is_empty() && lines_naming(&file_root, "\"tier\":\"gold\"").is_empty(), "the update's row is on {TOPOLOGY_STORAGE_TARGET} and not on {FILE}");

    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    let after = estate.node_views().await;
    estate.artifact("views.json", &json!(Estate::values(&after)));
    nodes_write(&after, TOPOLOGY_STORAGE_TARGET);
    assert_eq!(births(&after), before, "every node is the same birth: none restarted or replaced");
    assert!(after.iter().all(|v| v.status == NodeStatus::Ready), "every node is ready: {after:?}");
    let path: PathName = "mesh2.rpc.1".parse().unwrap();
    // The update and the Build's own config of the node are a serial order of the two
    // (node-config-update.md): whichever was written last on its row is the config the node carries.
    let subject_after = after.iter().find(|v| v.name == path).cloned().expect("mesh2.rpc.1 after the change");
    let last_row: Value = std::fs::read_to_string(target_root.join(rdm_admin::store_root::NODES_FILE))
        .unwrap()
        .lines()
        .rev()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find(|l| l["key"] == subject_after.node_id.to_string())
        .expect("mesh2.rpc.1's row is on the new provider");
    let row_config: BTreeMap<String, String> = serde_json::from_value(last_row["record"]["config"].clone()).unwrap_or_default();
    assert_eq!(subject_after.config, row_config, "mesh2.rpc.1 carries the config its last row on {TOPOLOGY_STORAGE_TARGET} names");
    let old = store_lines(&file_root);
    history_carried(&old, &store_lines(&target_root));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(store_lines(&file_root), old, "the previous provider is written no more");

    // Every process's evidence is flushed as it leaves: the spans are read once the estate has stopped.
    estate.stop().await;
    let spans = estate.spans();
    let mut traces = BTreeSet::new();
    for (step, outcome) in [
        ("rdm.node_admin.mesh.update.via-block", "blocked"),
        ("rdm.node_admin.build.update.via-storage-copy", "copied"),
        ("rdm.node_admin.build.update.via-storage-switch", "inserted"),
        ("rdm.node_admin.mesh.update.via-unblock", "unblocked"),
    ] {
        let found: Vec<&Value> = named(&spans, step).into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == build_id && sp["attributes"]["outcome"] == outcome).collect();
        assert!(!found.is_empty(), "{step} outcome={outcome} is recorded for Build {build_id}");
        traces.extend(found.iter().map(|sp| s(&sp["trace_id"])));
    }
    assert_eq!(traces.len(), 1, "the storage steps are on one causal trace: {traces:?}");
    // The node applies the update's map only once its mesh-primary's `mesh.unblock` began.
    let unblocked = of_build(&spans, UNBLOCK, &build_id, Some("mesh2")).into_iter().filter(|sp| sp["attributes"]["outcome"] == "unblocked").map(start_ns).min().expect("mesh2's mesh-primary unblocked");
    let applied: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-apply-node-config").into_iter().filter(|sp| attr(sp, "node") == "mesh2.rpc.1" && start_ns(sp) >= start_ns(&blocked)).collect();
    assert!(!applied.is_empty(), "mesh2.rpc.1 applied the update's map");
    assert!(applied.iter().all(|sp| start_ns(sp) >= unblocked), "mesh2.rpc.1 applies the map only once its mesh-primary is unblocked: {applied:#?}");
    estate.record_trace_url(traces.iter().next().unwrap());
}

// ---- e4: the mesh-primaries' block, the commit record, failure before the switch and lost nodes (i149
// R8–R12, R17–R19; storage.md §2 "Changing it", Q198). The spans: a mesh-primary's `mesh.block` and
// `mesh.unblock` (`rdm.node_admin.mesh.update.via-block` / `via-unblock`, its reply the outcome); the
// fabric-primary's switch, `commit_found` naming what it read of the `commit` record; a node's move
// onto the provider its change decided (`rdm.node.build.update.via-storage-move`).

/// A mesh-primary's `mesh.block` (storage.md §2 step 1): outcome `blocked`, its reply.
const BLOCK: &str = "rdm.node_admin.mesh.update.via-block";
/// The fabric-primary's switch: the `commit` record for `(build_id, change_id)` inserted, or read.
const SWITCH: &str = "rdm.node_admin.build.update.via-storage-switch";
/// A mesh-primary's `mesh.unblock` (step 4): outcome `unblocked`, its reply, `provider` where it moved
/// its nodes.
const UNBLOCK: &str = "rdm.node_admin.mesh.update.via-unblock";
/// A node's move onto the provider its change decided, commanded by its mesh-primary.
const MOVE: &str = "rdm.node.build.update.via-storage-move";
/// A mesh-primary's re-create, in its `mesh.unblock`, of a node that will not move: outcome `recreated`.
const MOVE_LOST: &str = "rdm.node_admin.node.update.via-storage-move-lost";
/// A node's connection event, its span open while its write lands (`connections_writer`).
const OBSERVED: &str = "rdm.node_admin.connection.update.via-observed";
/// The role node each cell restarts, freezes or reads.
const ROLE: &str = "mesh2.rpc.1";
/// The fabric-primary, where the testkit's storage-switch cut holds a change between every
/// mesh-primary's `blocked` and the copy.
const FP: &str = "mesh1.admin.1";

/// `name`'s exact runtime, frozen (SIGSTOP: alive and silent).
async fn freeze(estate: &Estate, name: &str) -> ExactRuntime {
    let rt = ExactRuntime::published(Path::new(&estate.data_dir(name).await)).unwrap_or_else(|r| panic!("{name}: its published runtime: {r:?}"));
    rt.apply(Fault::Stop).unwrap_or_else(|r| panic!("{name}: frozen: {r:?}"));
    rt
}

/// Continue a frozen runtime. One already ended (a re-created node's old birth, terminated) is left.
fn thaw(rt: &ExactRuntime) {
    let _ = rt.apply(Fault::Continue);
}

/// The fabric-primary's storage-switch cut armed: the next change is held after every mesh-primary
/// replied `blocked`, before its copy, until [`Door::release`] of `"switch"`.
async fn hold_switch(estate: &Estate) -> rdm_test::faults::Door {
    let door = rdm_test::faults::Door::at(estate, FP).await;
    door.arm("switch", json!({"kind": "storage-switch"})).await;
    door
}

/// `build.create` of the complete topology naming `provider`, accepted: the stream and its Build id.
async fn change_to(estate: &Estate, provider: &str) -> (BuildStream, String) {
    let stream = estate.carrier().await.create(naming_provider(estate, Some(provider)).await).await.unwrap_or_else(|e| panic!("the storage change naming {provider} is accepted: {e}"));
    let id = stream.build_id().to_string();
    (stream, id)
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

/// The spans `name` of Build `build_id` (every mesh's when `mesh` is `None`).
fn of_build<'a>(spans: &'a [Value], name: &str, build_id: &str, mesh: Option<&str>) -> Vec<&'a Value> {
    named(spans, name).into_iter().filter(|sp| attr(sp, "build_id") == build_id && mesh.is_none_or(|m| attr(sp, "mesh") == m)).collect()
}

/// `mesh`'s mesh-primary replied `blocked` to Build `build_id`'s `mesh.block`, once its span is recorded.
async fn blocked_at(estate: &Estate, build_id: &str, mesh: &str) -> Value {
    wait_for(&format!("{BLOCK} outcome=blocked for Build {build_id} in {mesh}"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == BLOCK && attr(sp, "build_id") == build_id && attr(sp, "mesh") == mesh && sp["attributes"]["outcome"] == "blocked")
    })
    .await
}

/// The node id `name` holds now, as `mesh.nodes.get` answers it.
async fn node_id_of(estate: &Estate, name: &str) -> String {
    estate.node_view(name).await.node_id.to_string()
}

/// Every line of the five stores on `root` naming `needle`.
fn lines_naming(root: &Path, needle: &str) -> Vec<String> {
    store_lines(root).into_values().flatten().filter(|l| l.contains(needle)).collect()
}

/// A Build stream to its terminal frame, reattached across a fabric-primary move.
async fn terminal(estate: &Estate, stream: BuildStream) -> Vec<BuildFrame> {
    let id = stream.build_id().clone();
    let followed = rdm_client::follow_with(stream, |id, _from| async move { estate.carrier().await.reattach(&id).await }).await.unwrap_or_else(|e| panic!("Build {id} could not be followed to its terminal: {e}"));
    followed.frames
}

/// One direct dial from `from` to `to` (a node's own connection event, Q169), answered.
fn dial(estate: &Estate, from: &str, to: &str) -> Value {
    let out = estate.probe(&["dial", "--target", &format!("path:{from}"), "--destination", &format!("path:{to}"), "--key", "storage-change"]);
    assert_eq!(out["outcome"], "Reply", "{from} answers the dial to {to} during the change (Q169: a connection event never blocks): {out}");
    out
}

/// A line on the destination root that the copy did not bring: on `target` and not on `source`. The
/// root is a provider's root of this build's layout (its `layout` marker), holding that one line.
fn foreign_line(target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    std::fs::write(target.join(rdm_admin::store_root::LAYOUT), format!("{}\n", rdm_admin::store_root::LAYOUT_VALUE)).unwrap();
    let op = rdm_admin::store_log::Op { node_id: "zzzzzzzzzzz1".into(), incarnation_id: "zzzzzzzzzzz2".into(), seq: 1 };
    // A whole node record (`node-record/1`), as a writer of that store writes one.
    let record = serde_json::to_vec(&rdm_admin::storage::NodeRow::new("mesh9.rpc.1".parse().unwrap(), rdm_core::NodeId::parse("zzzzzzzzzzz1").unwrap(), rdm_core::model::NodeStatus::Ready)).unwrap();
    let line = rdm_admin::store_log::encode_line(&op, "node-record/1", "zzzzzzzzzzz1", &record);
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

/// One run of the role-node restart: `ROLE`'s mesh-primary has replied `blocked`, the change is held
/// before its copy, `ROLE` dials (a connection event) and its process is killed (SIGKILL); node recovery
/// re-creates it at its `path.name` over the same data dir (node investigation's `exited` verdict, R-FM2)
/// once its mesh-primary issues commands again; the change ends `ends`. What is asserted is the
/// restarted birth's: it writes only the provider the `commit` record decided.
async fn role_node_restart(test: &str, ends: Ends) {
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    if ends == Ends::FailedBeforeTheSwitch {
        foreign_line(&target_root);
    }
    let (old_id, data_dir) = (node_id_of(&estate, ROLE).await, estate.data_dir(ROLE).await);
    let hold = hold_switch(&estate).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &build_id, "mesh2").await;
    let change_id = attr(&blocked, "change_id");
    assert!(!change_id.is_empty(), "the block carries its change_id: {blocked}");
    // A connection event of the node while its mesh-primary is blocked.
    let dialled_ns = now_ns();
    dial(&estate, ROLE, "mesh2.admin.1");
    estate.kill_node(ROLE).await;
    hold.release("switch").await;
    let frames = terminal(&estate, stream).await;
    estate.artifact("frames.json", &json!(format!("{frames:?}")));
    let restarted = estate.until_node(ROLE, "the role node re-created at its path.name", SETTLE, |v| v.filter(|v| v.node_id.to_string() != old_id && v.status == NodeStatus::Ready).cloned()).await;
    let new_id = restarted.node_id.to_string();
    assert_eq!(estate.data_dir(ROLE).await, data_dir, "the restart is over the same data dir: a new birth over a new one is not restart coverage");
    let (chosen, other, previous): (&str, &Path, &[&str]) = match ends {
        Ends::Switched => (TOPOLOGY_STORAGE_TARGET, &file_root, &[FILE]),
        Ends::FailedBeforeTheSwitch => (FILE, &target_root, &[]),
    };
    match (ends, frames.last()) {
        (Ends::Switched, Some(BuildFrame::Complete { .. })) | (Ends::FailedBeforeTheSwitch, Some(BuildFrame::Failed { .. })) => {}
        (_, last) => panic!("the change ends {ends:?}: {last:?}"),
    }
    fabric_names(&estate.fabric_view().await, chosen, previous);
    assert_eq!(restarted.topology_storage_provider.as_deref(), Some(chosen), "the restarted birth writes the provider its change decided: {restarted:?}");
    let chosen_root = if chosen == FILE { &file_root } else { &target_root };
    assert!(!lines_naming(chosen_root, &new_id).is_empty(), "the restarted birth's records are on {chosen}");
    // Nothing of a birth that started after the change's decision is on the provider not chosen.
    assert!(lines_naming(other, &new_id).is_empty(), "nothing of the restarted birth {new_id} is on the provider not chosen: {:#?}", lines_naming(other, &new_id));
    estate.stop().await;
    let spans = estate.spans();
    // The change was decided by the commit record.
    let decided: Vec<&Value> = of_build(&spans, SWITCH, &build_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found").is_some()).collect();
    assert!(!decided.is_empty(), "the fabric-primary decided the change by the commit record: {SWITCH} for ({build_id}, {change_id})");
    // The restarted birth only ever moves onto the provider the change decided, after it was decided.
    for sp in named(&spans, MOVE).into_iter().filter(|sp| attr(sp, "node_id") == new_id && attr(sp, "build_id") == build_id) {
        assert_eq!((attr(sp, "change_id"), attr(sp, "provider")), (change_id.clone(), chosen.to_string()), "the restarted birth moves onto {chosen} under the same change_id: {sp}");
        assert!(decided.iter().any(|d| start_ns(d) <= start_ns(sp)), "the restarted birth moves only once the commit record is read: {sp}");
    }
    // The connection event of the killed birth lands at most once on the provider chosen.
    let owed: Vec<&Value> = named(&spans, OBSERVED).into_iter().filter(|sp| attr(sp, "source") == ROLE && attr(sp, "destination") == "mesh2.admin.1" && attr(sp, "origin") == "dial" && start_ns(sp) >= dialled_ns).collect();
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
/// on the old provider"): a role process (mesh2.rpc.1) is killed while its mesh-primary has replied
/// `blocked` and the change is held before its copy, and comes back at its `path.name` over the same data
/// dir. Run twice: a change that switches, and one whose copy is refused before the switch. Each time the
/// restarted birth writes only the provider the `commit` record decided: its row names it, every record
/// of it is on that provider and none on the other, and it only ever moves onto that provider once the
/// record is read; the Fabric row names that provider. Must NOT happen: a write of the restarted birth on
/// the provider not chosen, or a new birth over a new data dir counted as the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_storage_state_survives_role_node_restart() {
    role_node_restart("topology_storage_state_survives_role_node_restart-switched", Ends::Switched).await;
    role_node_restart("topology_storage_state_survives_role_node_restart-failed", Ends::FailedBeforeTheSwitch).await;
}

/// CONTRACT (storage.md §2 "Changing it" (Q198): "A mesh-primary that has replied `blocked` issues no new
/// command until it is unblocked"; "Whoever resumes the change ... learns the decision only from that
/// record"; i149 R8, R12): a role node whose mesh-primary has replied `blocked` is killed while the change
/// is held, and node recovery re-creates it at its `path.name` over the same data dir once its
/// mesh-primary is unblocked. The restarted birth writes `test-target`, the provider the commit record
/// decided: every record of it is on `test-target`, none on `file`; the Build completes and the fabric
/// names `test-target` with `file` previous. Must NOT happen: the restarted birth writing to `file` (its
/// stale launch provider) or moving before the commit record is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read() {
    role_node_restart("a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read", Ends::Switched).await;
}

/// One mesh: its node-admin and an rpc node, so the store names a node beside the admin's own birth
/// and the admin's loss is a fabric recovery, not Q200's Day 0.
fn one_mesh() -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired { name: "mesh1".into(), node_admin: 1, rpc_node: 1, broker: 0, gateway: 0, compute: 0 }] }
}

/// CONTRACT (storage.md §2 "The property": "a restart with no launcher (the adoption launch) | the node
/// itself | its own row in its data dir"): a one-mesh fabric started with no provider named moves to
/// `test-target` by one Build of its complete topology, which completes; its Day-0 node-admin is lost,
/// and a person starts a node-admin again on that data dir with no launcher and no
/// `RDM_TOPOLOGY_STORAGE_PROVIDER` (fabric recovery). The new birth writes `test-target`, the provider
/// its own row names: its row says so, the Fabric row still names `test-target` with `file` previous,
/// its records are on `test-target`, and nothing of it is on `file`. Must NOT happen: the start taking
/// `file` from its environment's default and opening the provider the fabric left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_without_a_launcher_writes_the_provider_its_own_row_names() {
    let test = "a_restart_without_a_launcher_writes_the_provider_its_own_row_names";
    let names: BTreeSet<String> = [FP, "mesh1.rpc.1"].iter().map(|n| n.to_string()).collect();
    let mut estate = estate_with(test, &[]).await;
    estate.reconcile(&one_mesh()).await;
    estate.settles_to_named(&names, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let (file_root, target_root) = roots(&estate);
    let change = match estate.shape_change(&one_mesh()).await {
        Change::Reconcile { based_on, nodes, .. } => Change::Reconcile { based_on, nodes, topology_storage_provider: Some(TOPOLOGY_STORAGE_TARGET.into()) },
        other => panic!("the complete-topology request is a Reconcile: {other:?}"),
    };
    let stream = estate.carrier().await.create(change).await.unwrap_or_else(|e| panic!("the storage change naming {TOPOLOGY_STORAGE_TARGET} is accepted: {e}"));
    let frames = terminal(&estate, stream).await;
    assert!(matches!(frames.last(), Some(BuildFrame::Complete { .. })), "the change completes: {frames:?}");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    let old_id = node_id_of(&estate, FP).await;
    let data_dir = estate.bootstrap_data_dir("mesh1");
    estate.kill_bootstrap();
    let restarted = estate.restart_admin_with(&data_dir, &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    assert_eq!(restarted.to_string(), FP, "the start on the data dir is at its own row's path.name");
    estate.enter(&restarted).await;
    let views = estate.settles_to_named(&names, SETTLE).await;
    estate.artifact("views.json", &json!(Estate::values(&views)));
    let admin = views.iter().find(|v| v.name.to_string() == FP).expect("the restarted node-admin");
    let new_id = admin.node_id.to_string();
    assert_ne!(new_id, old_id, "a new birth over the data dir");
    assert_eq!(admin.topology_storage_provider.as_deref(), Some(TOPOLOGY_STORAGE_TARGET), "the start with no launcher writes the provider its own row names: {admin:?}");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    assert!(!lines_naming(&target_root, &new_id).is_empty(), "the new birth's records are on {TOPOLOGY_STORAGE_TARGET}");
    assert!(lines_naming(&file_root, &new_id).is_empty(), "nothing of the new birth {new_id} is on {FILE}, the provider the fabric left: {:#?}", lines_naming(&file_root, &new_id));
    estate.stop().await;
}

/// CONTRACT (storage.md §2 "Changing it" (Q198): "A step that fails before the switch fails the Build only
/// once the fabric-primary has read that no `commit` exists for `(build_id, change_id)`; the
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
    // mesh2's mesh-primary holds the change; mesh1.rpc.1 dials mesh1.admin.2, a node it holds no
    // connection to, while mesh1's mesh-primary is blocked.
    let (source, peer, held) = ("mesh1.rpc.1", "mesh1.admin.2", "mesh2.admin.1");
    let hold = freeze(&estate, held).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let change_id = attr(&blocked_at(&estate, &build_id, "mesh1").await, "change_id");
    let dialled_ns = now_ns();
    dial(&estate, source, peer);
    thaw(&hold);
    let frames = terminal(&estate, stream).await;
    estate.artifact("frames.json", &json!(format!("{frames:?}")));
    assert!(matches!(frames.last(), Some(BuildFrame::Failed { .. })), "the change fails before its switch: {frames:?}");
    fabric_names(&estate.fabric_view().await, FILE, &[]);
    let after = estate.node_views().await;
    nodes_write(&after, FILE);
    let after_births = births(&after);
    for (name, birth) in before.iter().filter(|(n, _)| n.as_str() != held) {
        assert_eq!(after_births.get(name), Some(birth), "{name}: the same birth serves throughout");
    }
    estate.stop().await;
    let spans = estate.spans();
    let read: Vec<&Value> = of_build(&spans, SWITCH, &build_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found") == Some(false)).collect();
    assert!(!read.is_empty(), "the fabric-primary read that no commit record exists for ({build_id}, {change_id}) before it failed the Build");
    for mesh in ["mesh1", "mesh2"] {
        let own: Vec<&Value> = of_build(&spans, UNBLOCK, &build_id, Some(mesh)).into_iter().filter(|sp| sp["attributes"]["outcome"] == "unblocked").collect();
        assert!(!own.is_empty(), "{mesh}'s mesh-primary replied unblocked after the failure");
        for sp in own {
            assert_eq!((attr(sp, "change_id"), attr(sp, "provider")), (change_id.clone(), FILE.to_string()), "{mesh}: unblocked onto {FILE} under the change: {sp}");
            assert!(read.iter().any(|r| start_ns(r) <= start_ns(sp)), "{mesh}: unblocked only once no commit is read: {sp}");
        }
    }
    // The control: the connection event of the change lands once, on the old provider.
    let owed: Vec<&Value> = named(&spans, OBSERVED).into_iter().filter(|sp| attr(sp, "source") == source && attr(sp, "destination") == peer && attr(sp, "origin") == "dial" && start_ns(sp) >= dialled_ns).collect();
    assert_eq!(owed.len(), 1, "the connection event during the change lands once: {owed:#?}");
    let entry = attr(owed[0], "entry");
    assert_eq!((lines_naming(&file_root, &entry).len(), lines_naming(&target_root, &entry).len()), (1, 0), "the connection event {entry} is on {FILE} once and not on {TOPOLOGY_STORAGE_TARGET}");
    estate.record_trace_url(&s(&read[0]["trace_id"]));
}

/// The copy's span (`rdm_node::storage_copy::COPY_SPAN`): `outcome` `copied`, `refused`, `failed` or
/// `indeterminate`, `cause` naming why.
const COPY: &str = "rdm.node_admin.build.update.via-storage-copy";

/// CONTRACT (storage.md §2 "Changing it" (Q198): step 2, "a new provider holding lines the old one does
/// not is refused by name, and nothing switches"; "A step that fails before the switch fails the Build
/// only once the fabric-primary has read that no `commit` exists for `(build_id, change_id)`; the
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
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_REFUSING).await;
    let change_id = attr(&blocked_at(&estate, &build_id, "mesh1").await, "change_id");
    // Accepted, its lines answered: from now on the provider refuses its list by name.
    let knob = refuse_list_knob(estate.fabric_store_root().parent().expect("the root is under the store dir"));
    std::fs::create_dir_all(knob.parent().unwrap()).unwrap();
    std::fs::write(&knob, b"").unwrap();
    hold.release("switch").await;
    let frames = match tokio::time::timeout(STEP, terminal(&estate, stream)).await {
        Ok(frames) => frames,
        Err(_) => {
            estate.stop().await;
            let copies: Vec<String> = of_build(&estate.spans(), COPY, &build_id, None).into_iter().map(|sp| format!("{} {}", attr(sp, "outcome"), attr(sp, "cause"))).collect();
            panic!("Build {build_id} never ended within {STEP:?}: every mesh is still blocked; its copies: {copies:#?}");
        }
    };
    estate.artifact("frames.json", &json!(format!("{frames:?}")));
    match frames.last() {
        Some(BuildFrame::Failed { reason, .. }) => assert!(reason.contains(TOPOLOGY_STORAGE_REFUSING) && reason.contains("lists no lines"), "the Build's failure names the provider and its refusal: {reason}"),
        last => panic!("the change fails before its switch: {last:?}"),
    }
    fabric_names(&estate.fabric_view().await, FILE, &[]);
    let after = estate.node_views().await;
    nodes_write(&after, FILE);
    assert_eq!(births(&after), before, "every node is the same birth");
    estate.stop().await;
    let spans = estate.spans();
    let copies = of_build(&spans, COPY, &build_id, None);
    assert_eq!(copies.len(), 1, "the copy answered by name is not read again: {copies:#?}");
    assert!(attr(copies[0], "outcome") == "failed" && attr(copies[0], "cause").contains(TOPOLOGY_STORAGE_REFUSING), "the copy fails by name, naming the provider's refusal: {}", copies[0]);
    let read: Vec<&Value> = of_build(&spans, SWITCH, &build_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found") == Some(false)).collect();
    assert!(!read.is_empty(), "the fabric-primary read that no commit record exists for ({build_id}, {change_id}) before it failed the Build");
    for mesh in ["mesh1", "mesh2"] {
        let own: Vec<&Value> = of_build(&spans, UNBLOCK, &build_id, Some(mesh)).into_iter().filter(|sp| sp["attributes"]["outcome"] == "unblocked").collect();
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
/// is held after every mesh-primary replied `blocked`, a Build of the complete topology that names no
/// provider and one naming `file` are each refused `build-in-progress` naming the change's Build, and
/// nothing of them is accepted; the change then completes, the Fabric row naming `test-target` with
/// `file` previous. Must NOT happen: a Build accepted during the change, or the change ended by the
/// refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_other_than_fabric_delete_is_refused_during_a_change() {
    let test = "a_build_other_than_fabric_delete_is_refused_during_a_change";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let hold = hold_switch(&estate).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &build_id, "mesh2").await;
    let mut refusals = Vec::new();
    for (what, provider) in [("a Build naming no provider", None), ("a change back to file", Some(FILE))] {
        let refused = estate.carrier().await.create(naming_provider(&estate, provider).await).await;
        match refused {
            Err(CallEnd::BuildInProgress { current }) => {
                assert_eq!(current.to_string(), build_id, "{what} is refused naming the change's Build");
                refusals.push(json!({"what": what, "current": current.to_string()}));
            }
            Err(other) => panic!("{what} during the change ended otherwise than build-in-progress: {other}"),
            Ok(accepted) => panic!("{what} was accepted during the change as Build {}", accepted.build_id()),
        }
    }
    estate.artifact("refusals.json", &json!(refusals));
    hold.release("switch").await;
    let done = estate.follow(stream).await;
    estate.artifact("frames.json", &json!(format!("{:?}", done.frames)));
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    assert_eq!(estate.current().await.based_on.to_string(), build_id, "the change is the current Build: nothing was accepted after it");
    estate.stop().await;
    let spans = estate.spans();
    assert_eq!(of_build(&spans, SWITCH, &build_id, None).into_iter().filter(|sp| sp["attributes"]["outcome"] == "inserted").count(), 1, "the change switched once");
    estate.record_trace_url(&s(&blocked["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "The change has a durable `change_id`, recorded with its Build
/// before its first command; its commands carry it, and a node refuses by name a command of an older
/// one"; i149 R11): a first change to `test-target` runs while mesh2's mesh-primary holds its first move
/// command to mesh2.rpc.1 on its way (the testkit's storage-step cut); the mesh-primary sends it again
/// and the change completes. A second change, back to `file`, completes, mesh2.rpc.1 moving under it, and
/// the held command of the first change is then released to mesh2.rpc.1. mesh2.rpc.1 refuses every
/// command of the first change it receives after its move under the second: its only move after that is
/// none, and it writes `file`. Must NOT happen: a move of the node by the first change's `change_id`
/// after its move under the second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_change_cannot_unblock_writes_for_a_new_change() {
    let test = "an_old_change_cannot_unblock_writes_for_a_new_change";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let role_id = node_id_of(&estate, ROLE).await;
    // mesh2's mesh-primary holds the first change's first move command of mesh2.rpc.1 on its way.
    let carrier = rdm_test::faults::Door::at(&estate, "mesh2.admin.1").await;
    carrier.arm("old-command", json!({"kind": "storage-step", "node": ROLE})).await;
    let (first, first_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    carrier.wait_held("old-command").await;
    estate.follow(first).await;
    let old_change = attr(&wait_for(&format!("{MOVE} of {ROLE} under Build {first_id}"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == MOVE && attr(sp, "build_id") == first_id && attr(sp, "node_id") == role_id && sp["attributes"]["outcome"] == "moved")
    }).await, "change_id");
    let (second, second_id) = change_to(&estate, FILE).await;
    estate.follow(second).await;
    let moved = wait_for(&format!("{MOVE} of {ROLE} under Build {second_id}"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == MOVE && attr(sp, "build_id") == second_id && attr(sp, "node_id") == role_id && sp["attributes"]["outcome"] == "moved")
    }).await;
    let new_change = attr(&moved, "change_id");
    assert_ne!(old_change, new_change, "each change has its own change_id");
    carrier.release("old-command").await;
    wait_for(&format!("the first change's held command reaches {ROLE}"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == MOVE && attr(sp, "node_id") == role_id && attr(sp, "change_id") == old_change && start_ns(sp) > start_ns(&moved))
    }).await;
    fabric_names(&estate.fabric_view().await, FILE, &[FILE, TOPOLOGY_STORAGE_TARGET]);
    assert_eq!(estate.node_view(ROLE).await.topology_storage_provider.as_deref(), Some(FILE), "{ROLE} writes {FILE}, the second change's provider");
    estate.stop().await;
    let spans = estate.spans();
    let switched = of_build(&spans, SWITCH, &second_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the second change switched");
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
/// never blocks an open): mesh1.rpc.1, whose mesh-primary has replied `blocked`, dials mesh1.admin.2, a
/// node it holds no connection to, while the change is held before its copy; the dial is answered at
/// once and its connection event is written once, on `file`, the provider the node writes until its
/// mesh-primary moves it; the copy brings that line to `test-target` once; the node then writes
/// `test-target`. Must NOT happen: the open waiting on the write, or the event lost or landed twice on
/// either provider.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_event_during_a_change_lands_once_after_the_unblock() {
    let test = "a_connection_event_during_a_change_lands_once_after_the_unblock";
    let (source, destination) = ("mesh1.rpc.1", "mesh1.admin.2");
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let hold = hold_switch(&estate).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &build_id, "mesh2").await;
    let dialled_ns = now_ns();
    dial(&estate, source, destination);
    wait_for(&format!("{source}'s connection event is written"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == OBSERVED && attr(sp, "source") == source && attr(sp, "destination") == destination && start_ns(sp) >= dialled_ns)
    }).await;
    hold.release("switch").await;
    estate.follow(stream).await;
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    nodes_write(&[estate.node_view(source).await], TOPOLOGY_STORAGE_TARGET);
    estate.stop().await;
    let spans = estate.spans();
    let owed: Vec<&Value> = named(&spans, OBSERVED).into_iter().filter(|sp| attr(sp, "source") == source && attr(sp, "destination") == destination && attr(sp, "origin") == "dial" && start_ns(sp) >= dialled_ns).collect();
    assert_eq!(owed.len(), 1, "the connection event during the change is written once: {owed:#?}");
    let entry = attr(owed[0], "entry");
    assert_eq!((lines_naming(&file_root, &entry).len(), lines_naming(&target_root, &entry).len()), (1, 1), "the connection event {entry} is on {FILE} once, where the node wrote it, and on {TOPOLOGY_STORAGE_TARGET} once, the copy's");
    let unblocked = of_build(&spans, UNBLOCK, &build_id, Some("mesh1")).into_iter().find(|sp| sp["attributes"]["outcome"] == "unblocked").expect("mesh1's mesh-primary unblocked");
    assert_eq!(attr(unblocked, "provider"), TOPOLOGY_STORAGE_TARGET, "mesh1's mesh-primary moves its nodes onto {TOPOLOGY_STORAGE_TARGET}: {unblocked}");
    estate.record_trace_url(&s(&blocked["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it" (Q198): step 4, "Each mesh-primary moves its nodes to the new
/// provider, then replies `unblocked`"; "After the switch the Build completes once every mesh-primary has
/// replied `unblocked`; nothing else waits"; a lost node "is investigated, and on any verdict but
/// `reached` node recovery re-creates it at its `path.name`"; Q182: a node not replying to commands is
/// silent; i149 R19): the role node is frozen (alive and silent) after its mesh-primary replied `blocked`,
/// so it leaves its move unanswered past the staleness floor. Its mesh-primary re-creates it at
/// mesh2.rpc.1 under a new node id, launched on `test-target`, steps of its unblock attempt, then replies
/// `unblocked` and the Build completes: the new birth writes
/// `test-target`, is ready, its records are on `test-target` and none on `file`; the old
/// birth never moves; the Fabric row names `test-target` with `file` previous. Must NOT happen: the
/// Build waiting on the frozen birth forever, the old birth moving, or the new birth launched on `file`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_never_unblocks_is_recreated_on_the_new_provider() {
    let test = "a_node_that_never_unblocks_is_recreated_on_the_new_provider";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let old_id = node_id_of(&estate, ROLE).await;
    let hold = hold_switch(&estate).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &build_id, "mesh2").await;
    let silent = freeze(&estate, ROLE).await;
    hold.release("switch").await;
    let done = estate.follow(stream).await;
    estate.artifact("frames.json", &json!(format!("{:?}", done.frames)));
    let renewed = estate.until_node(ROLE, "the role node re-created at its path.name", SETTLE, |v| v.filter(|v| v.node_id.to_string() != old_id && v.status == NodeStatus::Ready).cloned()).await;
    let new_id = renewed.node_id.to_string();
    assert_eq!(renewed.topology_storage_provider.as_deref(), Some(TOPOLOGY_STORAGE_TARGET), "the re-created birth is launched on the new provider: {renewed:?}");
    assert!(!lines_naming(&target_root, &new_id).is_empty() && lines_naming(&file_root, &new_id).is_empty(), "the re-created birth's records are on {TOPOLOGY_STORAGE_TARGET} and none on {FILE}");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    thaw(&silent);
    estate.stop().await;
    let spans = estate.spans();
    assert!(named(&spans, MOVE).iter().filter(|sp| attr(sp, "node_id") == old_id).all(|sp| sp["attributes"]["outcome"] != "moved"), "the frozen birth never moves");
    let switched = of_build(&spans, SWITCH, &build_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the change switched");
    assert!(start_ns(switched) >= start_ns(&blocked), "the switch follows the mesh-primary's blocked");
    estate.record_trace_url(&s(&switched["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it" (Q198): step 4, "Each mesh-primary moves its nodes to the new
/// provider, then replies `unblocked`"; "The previous provider is kept as it was at the switch, ...
/// written no more"; i149 R19: "A node that will not block or unblock is a lost node (node-investigation.md,
/// Q182): investigated and re-created at its path.name on the provider of its step; after the switch the
/// Build completes once every node has unblocked or been re-created"): the role node refuses its move by
/// name (the testkit's knob has its birth refuse to open `test-target`) and keeps answering and
/// gossiping. Its mesh-primary re-creates it at mesh2.rpc.1 under a new node id launched on
/// `test-target`, node.delete then node.create, steps of its unblock attempt of the change's own Build
/// (lead decision D1), and replies `unblocked` only once it is re-created: when the Build
/// completes the path holds the new birth, ready, writing `test-target`, its records on
/// `test-target` and none on `file`; the old birth never moves; `file` holds nothing the copy did not
/// carry to `test-target`. Must NOT happen: `unblocked` with the refusing birth standing, the old birth
/// left writing `file` after the switch, or the new birth launched on `file`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_refuses_its_move_is_recreated_on_the_new_provider() {
    let test = "a_node_that_refuses_its_move_is_recreated_on_the_new_provider";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let old_id = node_id_of(&estate, ROLE).await;
    let data_root = PathBuf::from(estate.data_dir(ROLE).await).parent().expect("a data dir is under the estate's root").to_path_buf();
    std::fs::write(rdm_test::node::target_refused_knob(&data_root, &old_id), b"").unwrap();
    let hold = hold_switch(&estate).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &build_id, "mesh2").await;
    hold.release("switch").await;
    let done = tokio::time::timeout(SETTLE, estate.follow(stream)).await.unwrap_or_else(|_| panic!("Build {build_id} did not complete within {SETTLE:?}: mesh2's unblock waits on the refusing birth's re-create"));
    estate.artifact("frames.json", &json!(format!("{:?}", done.frames)));
    // At the Build's completion, with no wait: the path holds the re-created birth.
    let at_completion = estate.node_view(ROLE).await;
    assert_ne!(at_completion.node_id.to_string(), old_id, "mesh2's mesh-primary replied unblocked only once the refusing birth was re-created: {at_completion:?}");
    let renewed = estate.until_node(ROLE, "the role node re-created at its path.name, ready", SETTLE, |v| v.filter(|v| v.node_id.to_string() != old_id && v.status == NodeStatus::Ready).cloned()).await;
    let new_id = renewed.node_id.to_string();
    assert_eq!(renewed.topology_storage_provider.as_deref(), Some(TOPOLOGY_STORAGE_TARGET), "the re-created birth is launched on the new provider: {renewed:?}");
    assert!(!lines_naming(&target_root, &new_id).is_empty() && lines_naming(&file_root, &new_id).is_empty(), "the re-created birth's records are on {TOPOLOGY_STORAGE_TARGET} and none on {FILE}");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    estate.stop().await;
    // Nothing is written to the old provider after the copy: every line it holds was carried.
    history_carried(&store_lines(&file_root), &store_lines(&target_root));
    let spans = estate.spans();
    let old_moves: Vec<&Value> = named(&spans, MOVE).into_iter().filter(|sp| attr(sp, "node_id") == old_id).collect();
    assert!(!old_moves.is_empty() && old_moves.iter().all(|sp| sp["attributes"]["outcome"] == "refused"), "the old birth refuses its move by name and never moves: {old_moves:#?}");
    let unblocked = of_build(&spans, UNBLOCK, &build_id, Some("mesh2")).into_iter().find(|sp| sp["attributes"]["outcome"] == "unblocked").expect("mesh2's mesh-primary replied unblocked");
    assert_eq!(attr(unblocked, "provider"), TOPOLOGY_STORAGE_TARGET, "mesh2's mesh-primary moves its nodes onto {TOPOLOGY_STORAGE_TARGET}: {unblocked}");
    assert_eq!(attr(unblocked, "recreated"), "1", "the mesh-primary that replied unblocked re-created the refusing birth in that unblock: {unblocked}");
    let lost: Vec<&Value> = of_build(&spans, MOVE_LOST, &build_id, Some("mesh2")).into_iter().filter(|sp| attr(sp, "node_id") == old_id).collect();
    assert!(lost.iter().any(|sp| sp["attributes"]["outcome"] == "recreated" && sp["trace_id"] == unblocked["trace_id"]), "the refusing birth is re-created, a step of the unblock that replied: {lost:#?}");
    let new_moved = named(&spans, MOVE).into_iter().find(|sp| attr(sp, "node_id") == new_id && attr(sp, "build_id") == build_id && sp["attributes"]["outcome"] == "moved").unwrap_or_else(|| panic!("the re-created birth {new_id} answered its move under Build {build_id}"));
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
    let (_change, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let change_id = attr(&blocked_at(&estate, &build_id, "mesh2").await, "change_id");
    let accepted_ns = now_ns();
    let delete = estate.carrier().await.create(Change::DeleteFabric).await.unwrap_or_else(|end| panic!("fabric.delete during the change is admitted: {end}"));
    let delete_id = delete.build_id().to_string();
    let (file_at, target_at) = (store_lines(&file_root), store_lines(&target_root));
    let frames = terminal(&estate, delete).await;
    estate.artifact("frames.json", &json!(format!("{frames:?}")));
    assert!(matches!(frames.last(), Some(BuildFrame::Complete { .. })), "the delete completes: {frames:?}");
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
    let resolved = of_build(&spans, SWITCH, &build_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found") == Some(false) && start_ns(sp) >= accepted_ns).min_by_key(|sp| start_ns(sp)).unwrap_or_else(|| panic!("the delete's executor read the change's commit record (commit_found = false) for ({build_id}, {change_id})"));
    assert!(of_build(&spans, SWITCH, &build_id, None).iter().all(|sp| sp["attributes"]["outcome"] != "inserted"), "the change is never switched under the delete");
    let teardown = spans.iter().filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_admin.node.delete.") || n.starts_with("rdm.mesh.node.delete.")) && start_ns(sp) >= accepted_ns).map(start_ns).min().expect("the delete tore the nodes down");
    assert!(start_ns(resolved) <= teardown, "the change is resolved before the delete's teardown begins (delete {delete_id})");
    estate.record_trace_url(&s(&resolved["trace_id"]));
}

// ---- e5: the customer's change end to end (i149 R13–R15; storage.md §2 "Changing it"). The fast gate
// runs one cell of this stem (the serving births through a change, process provider); the release suite
// runs the whole stem, and its container-proof job the cells of [`container`].

/// The node store's file on a provider's root.
const NODES: &str = rdm_admin::store_root::NODES_FILE;
/// The node created after a change, by a Build that omits the provider (Q166).
const GROWN: &str = "mesh1.rpc.2";

/// A fabric of the faulted node-admin and the testkit rpc node on the deployment provider `provider`
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
    estate.reconcile(&two_meshes()).await;
    let views = estate.settles_to_named(&two_mesh_names(), SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    (estate, views)
}

/// [`two_meshes`] with mesh1 grown by one rpc node, [`GROWN`].
fn grown() -> FabricDesired {
    let mut fabric = two_meshes();
    fabric.meshes[0].rpc_node = 2;
    fabric
}

/// `node.get [id: x]` of every node of `views` by its minted id, at the fabric-primary: `path.name` →
/// (node id, incarnation, status).
async fn node_gets(estate: &Estate, views: &[NodeView]) -> BTreeMap<String, (String, Option<String>, NodeStatus)> {
    let nodes = estate.nodes_obj().await;
    let mut out = BTreeMap::new();
    for v in views {
        let mut got = nodes.get(&rdm_client::Filter::new().eq("id", [v.node_id.to_string()])).await.unwrap_or_else(|e| panic!("node.get [id: {}] ({}) is answered: {e}", v.node_id, v.name));
        assert_eq!(got.len(), 1, "node.get [id: {}] ({}) answers the one node: {got:?}", v.node_id, v.name);
        let got = got.remove(0);
        out.insert(got.name.to_string(), (got.node_id.to_string(), got.incarnation_id.as_ref().map(|i| i.as_str().to_string()), got.status));
    }
    out
}

/// Every birth of `before` is the node `node.get` answers at its `path.name` in `got`, ready.
fn serving(got: &BTreeMap<String, (String, Option<String>, NodeStatus)>, before: &BTreeMap<String, (String, Option<String>)>, when: &str) {
    for (name, (id, incarnation)) in before {
        let (got_id, got_incarnation, status) = got.get(name).unwrap_or_else(|| panic!("{when}: node.get answers no {name}: {got:?}"));
        assert_eq!((got_id, got_incarnation), (id, incarnation), "{when}: {name} is the same birth (node id, incarnation)");
        assert_eq!(*status, NodeStatus::Ready, "{when}: {name} is ready");
    }
}

/// The node store's lines on `root` from line `from` on, each as JSON (`{"key", "record", ...}`).
fn node_lines_from(root_lines: &BTreeMap<&'static str, Vec<String>>, from: usize) -> Vec<Value> {
    root_lines[NODES].iter().skip(from).filter_map(|l| serde_json::from_str(l).ok()).collect()
}

/// `source` dials `destination`, a node it holds no connection to, and its connection event is written:
/// the event's entry, as its span names it.
async fn connection_event(estate: &Estate, source: &str, destination: &str) -> String {
    let dialled_ns = now_ns();
    dial(estate, source, destination);
    let sp = wait_for(&format!("{source}'s connection event to {destination} is written"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == OBSERVED && attr(sp, "source") == source && attr(sp, "destination") == destination && attr(sp, "origin") == "dial" && start_ns(sp) >= dialled_ns)
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
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    for mesh in ["mesh1", "mesh2"] {
        blocked_at(&estate, &build_id, mesh).await;
    }
    // Throughout: every mesh-primary has replied `blocked` and the change is held before its copy.
    serving(&node_gets(&estate, &views).await, &before, "every mesh-primary blocked");
    hold.release("switch").await;
    let done = estate.follow(stream).await;
    estate.artifact("frames.json", &json!(format!("{:?}", done.frames)));
    serving(&node_gets(&estate, &views).await, &before, "after the change");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    // The record: every row of a serving node written from the change's acceptance on, on either
    // provider, records the same node at its path.name with the status its row held before the change
    // (no node changes state), and the last records its move onto the new provider.
    let (file_after, target_after) = (store_lines(&file_root), store_lines(&target_root));
    history_carried(&file_after, &target_after);
    let held = node_lines_from(&file_before, 0);
    let written: Vec<Value> = node_lines_from(&file_after, file_before[NODES].len()).into_iter().chain(node_lines_from(&target_after, file_after[NODES].len())).collect();
    estate.artifact("rows-written.json", &json!(written));
    for (name, (id, _)) in &before {
        let status = &held.iter().rev().find(|l| l["key"] == id.as_str()).unwrap_or_else(|| panic!("{name}: its row before the change"))["record"]["status"];
        let own: Vec<&Value> = written.iter().filter(|l| l["key"] == id.as_str()).collect();
        assert!(own.iter().all(|l| l["record"]["status"] == *status && l["record"]["name"] == name.as_str()), "{name}: every row written during the change records the same node at its path.name with the status {status} its row held before: {own:#?}");
        let last = own.last().unwrap_or_else(|| panic!("{name}: its row records its move: no row of it written during the change"));
        assert_eq!(last["record"]["topology_storage_provider"], TOPOLOGY_STORAGE_TARGET, "{name}: its last row records the provider it writes: {last}");
    }
    let stopped_ns = now_ns();
    estate.stop().await;
    let spans = estate.spans();
    // No process of a serving node started again, and no node was created or left during the change.
    for (name, (id, incarnation)) in &before {
        let boots: Vec<&Value> = named(&spans, "rdm.mesh.node.create.via-boot").into_iter().filter(|sp| attr(sp, "node_id") == *id).collect();
        assert_eq!(boots.len(), 1, "{name}: its birth's process started once: {boots:#?}");
        assert_eq!(Some(attr(boots[0], "incarnation_id")), incarnation.clone(), "{name}: the one start is its incarnation's");
        assert!(start_ns(boots[0]) < accepted_ns, "{name}: its process started before the change");
        let moved: Vec<&Value> = of_build(&spans, MOVE, &build_id, None).into_iter().filter(|sp| attr(sp, "node_id") == *id).collect();
        assert!(!moved.is_empty() && moved.iter().all(|sp| sp["attributes"]["outcome"] == "moved" && attr(sp, "provider") == TOPOLOGY_STORAGE_TARGET), "{name}: its birth moved onto {TOPOLOGY_STORAGE_TARGET} in place: {moved:#?}");
    }
    let lifecycle: Vec<&Value> = spans
        .iter()
        .filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.node.create.") || n.starts_with("rdm.mesh.node.delete.") || n.starts_with("rdm.node_admin.node.create.via-build") || n.starts_with("rdm.node_admin.node.delete.")))
        .filter(|sp| (accepted_ns..stopped_ns).contains(&start_ns(sp)))
        .collect();
    assert!(lifecycle.is_empty(), "no node was created, started or deleted during the change: {lifecycle:#?}");
    let switched = of_build(&spans, SWITCH, &build_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the change switched");
    estate.record_trace_url(&s(&switched["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": a storage change "is a property Build: the fabric, every mesh
/// and every node stay ready, no node restarts, and no node changes state"; i149 R13):
/// on a two-mesh fabric, `node.get` of every node by its id answers the same birth (node id and
/// incarnation), ready, before the change to `test-target`, while every mesh-primary has
/// replied `blocked` and the change is held before its copy, and after the Build completes; every row of a
/// serving node written during the change, on either provider, records the same node at its path.name
/// with the status its row held before the change, the last naming `test-target`; each birth's process started once, before the
/// change, and moved onto `test-target` in place; no node is created, started or deleted during the
/// change. Must NOT happen: a node restarted, re-born or replaced, a read unanswered while blocked, or a
/// row recording another status. The fast gate's cell of this stem.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_serving_node_keeps_its_birth_and_ready_through_a_change() {
    every_node_serves_through_a_change("every_serving_node_keeps_its_birth_and_ready_through_a_change", "process").await;
}

/// CONTRACT (storage.md §2 "Changing it": "The previous provider is kept as it was at the switch, named on
/// the Fabric row, written no more and never deleted by RDM"; step 3, the switch "appends the Fabric row's
/// line naming the new provider current" on the new provider; i149 R14): after a change to `test-target`
/// completes, every store on `file` reads whole (every line verified), its Fabric row still names `file`
/// with no previous provider while `test-target`'s names `test-target` with `file` previous, and every
/// serving node's row reads there. A Build that omits the provider then creates mesh1.rpc.2 and that node
/// dials mesh1.admin.2: its records and its connection event are on `test-target` and none on `file`, and
/// every line of `file` is the line it held at the cutover. Must NOT happen: a line of the previous
/// provider changed, added or removed, or a write after the cutover landing there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_previous_provider_remains_unchanged_after_cutover() {
    let test = "a_previous_provider_remains_unchanged_after_cutover";
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let (stream, _) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    estate.follow(stream).await;
    let cutover = store_lines(&file_root);
    estate.artifact("previous-at-cutover.json", &json!(cutover));
    // Readable: every store of the previous provider reads whole, every line verified.
    for store in STORES {
        let read = rdm_admin::store_log::read_file(&file_root.join(store)).unwrap_or_else(|e| panic!("{store} on {FILE} reads whole after the cutover: {e}"));
        assert_eq!(read.len(), cutover[store].iter().filter(|l| !l.is_empty()).count(), "{store} on {FILE}: every line reads");
    }
    let fabric_on = |root: &Path| rdm_admin::store_log::read_record(&root.join(rdm_admin::store_root::FABRIC_FILE), "fabric-record", None).unwrap_or_else(|| panic!("the Fabric row reads on {}", root.display()));
    let (old, new) = (fabric_on(&file_root), fabric_on(&target_root));
    assert_eq!((&old["record"]["topology_storage_provider"], old["record"]["previous_topology_storage_providers"].as_array().map_or(0, Vec::len)), (&json!(FILE), 0), "the Fabric row on {FILE} is the one before the switch: {old}");
    assert_eq!((&new["record"]["topology_storage_provider"], &new["record"]["previous_topology_storage_providers"]), (&json!(TOPOLOGY_STORAGE_TARGET), &json!([FILE])), "the Fabric row on {TOPOLOGY_STORAGE_TARGET} names it, {FILE} previous: {new}");
    for v in &views {
        assert!(rdm_admin::store_log::read_record(&file_root.join(NODES), "node-record", Some(v.node_id.as_str())).is_some(), "{}: its row reads on {FILE}", v.name);
    }
    // Writes after the cutover: a node created by a Build that omits the provider, and its connection event.
    estate.reconcile(&grown()).await;
    let mut names = two_mesh_names();
    names.insert(GROWN.into());
    let grown_id = estate.settles_to_named(&names, SETTLE).await.into_iter().find(|v| v.name.to_string() == GROWN).expect("the grown node").node_id.to_string();
    let entry = connection_event(&estate, GROWN, "mesh1.admin.2").await;
    assert!(!lines_naming(&target_root, &grown_id).is_empty() && lines_naming(&file_root, &grown_id).is_empty(), "{GROWN}'s records are on {TOPOLOGY_STORAGE_TARGET} and none on {FILE}");
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
    estate.follow(first).await;
    let at_switch = store_lines(&file_root);
    // A line `file` lacks: a connection event written on `test-target`.
    let entry = connection_event(&estate, "mesh1.rpc.1", "mesh1.admin.2").await;
    assert_eq!((lines_naming(&target_root, &entry).len(), lines_naming(&file_root, &entry).len()), (1, 0), "the connection event {entry} is on {TOPOLOGY_STORAGE_TARGET} alone");
    let (back, back_id) = change_to(&estate, FILE).await;
    estate.follow(back).await;
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
    assert!(after.iter().all(|v| v.status == NodeStatus::Ready), "every node is ready: {after:?}");
    nodes_write(&after, FILE);
    // The fabric serves on `file`: a Build that omits the provider creates a node, written there alone.
    estate.reconcile(&grown()).await;
    let mut names = two_mesh_names();
    names.insert(GROWN.into());
    let grown_view = estate.settles_to_named(&names, SETTLE).await.into_iter().find(|v| v.name.to_string() == GROWN).expect("the grown node");
    let grown_id = grown_view.node_id.to_string();
    assert_eq!(grown_view.topology_storage_provider.as_deref(), Some(FILE), "{GROWN} writes {FILE}: {grown_view:?}");
    assert!(!lines_naming(&file_root, &grown_id).is_empty() && lines_naming(&target_root, &grown_id).is_empty(), "{GROWN}'s records are on {FILE} and none on {TOPOLOGY_STORAGE_TARGET}");
    assert_eq!(store_lines(&target_root), target_back, "{TOPOLOGY_STORAGE_TARGET} is written no more once the fabric returned to {FILE}");
    estate.stop().await;
    let spans = estate.spans();
    for id in [&first_id, &back_id] {
        assert_eq!(of_build(&spans, SWITCH, id, None).into_iter().filter(|sp| sp["attributes"]["outcome"] == "inserted").count(), 1, "Build {id} switched once");
    }
    let switched = of_build(&spans, SWITCH, &back_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the change back switched");
    estate.record_trace_url(&s(&switched["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "A later Build naming it again copies into it only the lines it
/// lacks"; "The property": the Fabric row carries `previous_topology_storage_providers`, "each provider
/// the fabric left, in order"; i149 R14): a change to `test-target` completes and mesh1.rpc.1's connection
/// event is written there alone; a later Build naming `file` completes: `file`'s lines at the first switch
/// stand, then every line of `test-target` follows in source order, the event once; the Fabric row names
/// `file` with `file` and `test-target` previous; every node is the same birth, ready, writing
/// `file`; a Build that omits the provider then creates mesh1.rpc.2, written on `file` and none on
/// `test-target`, which is written no more. Must NOT happen: a line of `file` rewritten, reordered or
/// copied twice, or a write after the return landing on `test-target`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_customer_returns_to_a_previous_topology_provider() {
    returns_to_a_previous_provider("a_customer_returns_to_a_previous_topology_provider", "process").await;
}

/// The container provider's cells: opt-in, the container-proof step (`RDM_CONTAINER_PROOF=1`, or
/// `RDM_REQUIRE_CONTAINER=1`); otherwise each skips by name. Each is ignored by name until a role node's
/// container reaches the testkit's second provider: run opted in at 17fbf387e, every node-admin moved
/// onto `test-target` and each rpc node refused its move, `refusing to start: <estate>/store-test-target:
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
