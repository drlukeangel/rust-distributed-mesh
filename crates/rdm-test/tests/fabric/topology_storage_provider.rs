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

use rdm_client::{BuildFrame, BuildStream, CallEnd, Change, FabricDesired, FabricView, MeshDesired, NodeStatus, NodeView, PathName};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use rdm_test::process_faults::{ExactRuntime, Fault};
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

/// Whether the fabric-primary's process is ended by a fault (SIGKILL) or by its own stop (SIGTERM).
#[derive(Debug, Clone, Copy)]
enum Ended {
    Forced,
    Clean,
}

/// One run of the restart-recovery canary, its fabric-primary ended `how`.
async fn admin_restart_resolves_the_same_change(test: &str, how: Ended) {
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let fabric_primary = estate.fabric_view().await.fabric_primary.expect("fabric.get names the fabric-primary").to_string();
    assert_eq!(fabric_primary, "mesh1.admin.1", "the Day-0 admin is the fabric-primary");
    let before = births(&views);
    let (file_root, target_root) = roots(&estate);
    let stream = estate.carrier().await.create(naming_provider(&estate, Some(TOPOLOGY_STORAGE_TARGET)).await).await.unwrap_or_else(|e| panic!("the storage change naming {TOPOLOGY_STORAGE_TARGET} is accepted: {e}"));
    let build_id = stream.build_id().to_string();
    // A node has blocked under the change: its block, recorded in its data dir, carries the change_id.
    let blocked = step_spans(&estate, "rdm.node.build.update.via-storage-block", &build_id, "blocked").await;
    let change_id = s(&blocked[0]["attributes"]["change_id"]);
    assert!(!change_id.is_empty(), "the block carries the change's change_id: {:#?}", blocked[0]);
    match how {
        Ended::Forced => estate.kill_bootstrap(),
        Ended::Clean => {
            let pid = estate.bootstrap_pid().expect("the Day-0 admin runs");
            assert!(std::process::Command::new("kill").args(["-TERM", &pid.to_string()]).status().unwrap().success(), "kill -TERM {pid}");
            wait_for(&format!("the fabric-primary's process {pid} leaves on its stop"), STEP, || async move { (!Path::new(&format!("/proc/{pid}/status")).exists() || std::fs::read_to_string(format!("/proc/{pid}/status")).is_ok_and(|t| t.contains("State:\tZ"))).then_some(()) }).await;
        }
    }
    // The same logical admin over the same data dir, its launch environment naming the old provider: a
    // stale environment: the restart reads its block from its data dir before it writes anything.
    let restarted = estate.restart_admin_with(&estate.bootstrap_data_dir("mesh1"), &[("RDM_TOPOLOGY_STORAGE_PROVIDER", FILE)]);
    // The estate enters the fabric again at the restarted birth.
    estate.enter(&restarted).await;
    // The Build is followed at the fabric-primary the fabric names after the restart.
    let holder = wait_for(&format!("{restarted}'s fabric names a fabric-primary"), SETTLE, || async { estate.fabric_view().await.fabric_primary }).await;
    estate.enter(&holder).await;
    let done = estate.follow(stream).await;
    estate.artifact("frames.json", &json!(format!("{:?}", done.frames)));
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    let after = estate.node_views().await;
    nodes_write(&after, TOPOLOGY_STORAGE_TARGET);
    let after_births = births(&after);
    for (name, birth) in before.iter().filter(|(n, _)| *n != &fabric_primary) {
        assert_eq!(after_births.get(name), Some(birth), "{name}: the same birth serves throughout");
    }
    assert!(after.iter().all(|v| v.status == NodeStatus::ReadyForTraffic), "every node is ready-for-traffic: {after:?}");
    // The complete captured dataset: every record and its history on the new provider, the old one
    // written no more.
    let old = store_lines(&file_root);
    history_carried(&old, &store_lines(&target_root));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(store_lines(&file_root), old, "the previous provider is written no more");
    // Every process's evidence is flushed as it leaves: the spans are read once the estate has stopped.
    estate.stop().await;
    let spans = estate.spans();
    let switched: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-storage-switch").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == build_id && sp["attributes"]["outcome"] == "inserted").collect();
    assert_eq!(switched.len(), 1, "exactly one `commit` is inserted for the change: {switched:#?}");
    assert_eq!(s(&switched[0]["attributes"]["change_id"]), change_id, "the resumed change is the same change_id");
    estate.record_trace_url(&s(&switched[0]["trace_id"]));
}

/// CONTRACT (i149 § Restart-recovery canary, FIRST test; storage.md §2 "Changing it": "The change has a
/// durable `change_id`"; "Block": the node "records the block and both providers in its data dir, which
/// a restart reads before it writes anything"; "Switch": "Whoever resumes the change, a successor or
/// the fabric-primary after a lost answer, learns the decision only by reading that record"): a storage
/// change to `test-target` on a two-mesh fabric has a node blocked, the fabric-primary is ended, once by
/// SIGKILL and once by its own stop, and started again over the same data dir with a launch environment
/// naming `file`. The change resumes under the same `change_id` with exactly one `commit` inserted for
/// `(build_id, change_id)`; the Fabric row names
/// `test-target` with `file` previous; every node writes `test-target`, every other node the same birth
/// and ready-for-traffic; every record of the five stores and its history is on `test-target` in source
/// order, and `file` is written no more. Must NOT happen: a second `commit`, a new `change_id` for the
/// resumed change, a write let through by the stale environment, or a record lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_storage_state_survives_admin_restart_at_same_data_dir() {
    admin_restart_resolves_the_same_change("topology_storage_state_survives_admin_restart_at_same_data_dir-forced", Ended::Forced).await;
    admin_restart_resolves_the_same_change("topology_storage_state_survives_admin_restart_at_same_data_dir-clean", Ended::Clean).await;
}

/// CONTRACT (i149 § Seed spec, `a_customer_changes_topology_storage_without_restarting_members`; storage.md
/// §2 "Changing it": "a property Build: the fabric, every mesh and every node stay ready-for-traffic, no
/// node restarts, and no node changes state"; "Copy" ... "dead births' and deleted nodes' records among
/// them"; "The previous provider is kept as it was at the switch ... written no more"): a
/// two-mesh fabric with history (a deleted birth, a deleted mesh) is moved to `test-target` by one Build
/// of its complete topology while a `node.config.update` is in flight. The Build completes; the Fabric
/// row names `test-target` with `file` previous; every node writes `test-target` and is the same birth,
/// ready-for-traffic, carrying the config the update set; every line of the five stores is on
/// `test-target` in source order, the deleted birth's and the deleted mesh's included; `file` is written
/// no more; and the block (its span closing on the node's acknowledgement once its issued writes
/// are finished: i149 § OTLP span-assertion bar), copy, switch and unblock steps are spans on one
/// trace. Must NOT
/// happen: a node restarted or replaced, a record lost or reordered, or the old provider written after
/// the commit.
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
    // A caller's command while the change runs: answered once its line has landed.
    let subject = views.iter().find(|v| v.name.to_string() == "mesh2.rpc.1").cloned().expect("mesh2.rpc.1");
    let primary = views.iter().find(|v| v.mesh == "mesh2" && v.is_primary).cloned().expect("mesh2's primary");
    let told: BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(subject.node_id.clone()), node_id: subject.node_id.clone(), incarnation: subject.incarnation_id.clone().expect("a live birth") };
    let carrier = estate.carrier().await;
    let (rpc, options) = (rdm_client::NodeRpc::new(carrier.client()), rdm_transport::CallOptions::default());
    let update = rpc.config_update(&primary.node_id, &rdm_transport::RouteChoice::Direct, &birth, told.clone(), &options);
    let (done, updated) = tokio::join!(estate.follow(stream), update);
    updated.unwrap_or_else(|e| panic!("node.config.update during the change is answered: {e}"));
    estate.artifact("frames.json", &json!(format!("{:?}", done.frames)));

    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    let after = estate.node_views().await;
    estate.artifact("views.json", &json!(Estate::values(&after)));
    nodes_write(&after, TOPOLOGY_STORAGE_TARGET);
    assert_eq!(births(&after), before, "every node is the same birth: none restarted or replaced");
    assert!(after.iter().all(|v| v.status == NodeStatus::ReadyForTraffic), "every node is ready-for-traffic: {after:?}");
    let path: PathName = "mesh2.rpc.1".parse().unwrap();
    assert_eq!(after.iter().find(|v| v.name == path).map(|v| v.config.clone()), Some(told), "the config the update set during the change stands");
    let old = store_lines(&file_root);
    history_carried(&old, &store_lines(&target_root));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(store_lines(&file_root), old, "the previous provider is written no more");

    // Every process's evidence is flushed as it leaves: the spans are read once the estate has stopped.
    estate.stop().await;
    let spans = estate.spans();
    let mut traces = BTreeSet::new();
    for (step, outcome) in [
        ("rdm.node.build.update.via-storage-block", "blocked"),
        ("rdm.node_admin.build.update.via-storage-copy", "copied"),
        ("rdm.node_admin.build.update.via-storage-switch", "inserted"),
        ("rdm.node.build.update.via-storage-unblock", "unblocked"),
    ] {
        let found: Vec<&Value> = named(&spans, step).into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == build_id && sp["attributes"]["outcome"] == outcome).collect();
        assert!(!found.is_empty(), "{step} outcome={outcome} is recorded for Build {build_id}");
        traces.extend(found.iter().map(|sp| s(&sp["trace_id"])));
    }
    assert_eq!(traces.len(), 1, "the storage steps are on one causal trace: {traces:?}");
    estate.record_trace_url(traces.iter().next().unwrap());
}

// ---- e4: blocked changes, the commit record, failure before the switch and lost nodes (i149 R8–R12,
// R17–R19). The step spans are the plan's (i149 § OTLP span-assertion bar): a node's block, its span
// closing on the node's acknowledgement once every write it issued before the block is finished; the
// fabric-primary's switch, `commit_found` naming what it read of the `commit` record; a node's unblock,
// `provider` naming where its owed writes land.

/// A node's block of a storage change (storage.md §2 steps 1 and 2).
const BLOCK: &str = "rdm.node.build.update.via-storage-block";
/// The fabric-primary's switch: the `commit` record for `(build_id, change_id)` inserted, or read.
const SWITCH: &str = "rdm.node_admin.build.update.via-storage-switch";
/// A node's unblock onto the provider its change decided.
const UNBLOCK: &str = "rdm.node.build.update.via-storage-unblock";
/// A node's connection event, its span open while its write lands (`connections_writer`).
const OBSERVED: &str = "rdm.node_admin.connection.update.via-observed";
/// The node whose silence holds a change between the block and the copy: frozen (SIGSTOP) before the
/// change is submitted and continued within the staleness floor. storage.md §2 step 2: "The copy waits
/// for every node's acknowledgement or node investigation's `exited` verdict on it; silence is neither."
const HOLD: &str = "mesh1.admin.2";
/// The role node each cell blocks, restarts, freezes or reads.
const ROLE: &str = "mesh2.rpc.1";

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

/// The spans `name` of Build `build_id` (every node's when `node_id` is `None`).
fn of_build<'a>(spans: &'a [Value], name: &str, build_id: &str, node_id: Option<&str>) -> Vec<&'a Value> {
    named(spans, name).into_iter().filter(|sp| attr(sp, "build_id") == build_id && node_id.is_none_or(|n| attr(sp, "node_id") == n)).collect()
}

/// `node_id`'s block under Build `build_id`, once its span is recorded: its acknowledgement.
async fn blocked_at(estate: &Estate, build_id: &str, node_id: &str) -> Value {
    wait_for(&format!("{BLOCK} outcome=blocked for Build {build_id} at node {node_id}"), STEP, || async {
        estate.spans().into_iter().find(|sp| sp["name"] == BLOCK && attr(sp, "build_id") == build_id && attr(sp, "node_id") == node_id && sp["attributes"]["outcome"] == "blocked")
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
    assert_eq!(out["outcome"], "Reply", "{from} answers the dial to {to} while its writes are owed (Q169: a connection event never blocks): {out}");
    out
}

/// A line on the destination root that the copy did not bring: on `target` and not on `source`. The
/// root is a provider's root of this build's layout (its `layout` marker), holding that one line.
fn foreign_line(target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    std::fs::write(target.join(rdm_admin::store_root::LAYOUT), format!("{}\n", rdm_admin::store_root::LAYOUT_VALUE)).unwrap();
    let op = rdm_admin::store_log::Op { node_id: "zzzzzzzzzzz1".into(), incarnation_id: "zzzzzzzzzzz2".into(), seq: 1 };
    // A whole node record (`node-record/1`), as a writer of that store writes one.
    let record = serde_json::to_vec(&rdm_admin::storage::NodeRow::new("mesh9.rpc.1".parse().unwrap(), rdm_core::NodeId::parse("zzzzzzzzzzz1").unwrap(), rdm_core::model::NodeStatus::ReadyForTraffic)).unwrap();
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

/// One run of the role-node restart: `ROLE` blocked under a change with a connection event owed, its
/// process killed (SIGKILL) while the change is held, and the node re-created at its `path.name` over
/// the same data dir (node investigation's `exited` verdict, R-FM2); the change ends `ends`. What is
/// asserted is the restarted birth's: it reads its block from its data dir before it writes anything,
/// and it writes only to the provider the `commit` record decided.
async fn role_node_restart(test: &str, ends: Ends) {
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    if ends == Ends::FailedBeforeTheSwitch {
        foreign_line(&target_root);
    }
    let (old_id, data_dir) = (node_id_of(&estate, ROLE).await, estate.data_dir(ROLE).await);
    let hold = freeze(&estate, HOLD).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &build_id, &old_id).await;
    let change_id = attr(&blocked, "change_id");
    assert!(!change_id.is_empty(), "the block carries its change_id: {blocked}");
    // A write owed: the node's own connection event, answered and not landed while it is blocked.
    dial(&estate, ROLE, "mesh2.admin.1");
    estate.kill_node(ROLE).await;
    thaw(&hold);
    let frames = terminal(&estate, stream).await;
    estate.artifact("frames.json", &json!(format!("{frames:?}")));
    let restarted = estate.until_node(ROLE, "the role node re-created at its path.name", SETTLE, |v| v.filter(|v| v.node_id.to_string() != old_id && v.status == NodeStatus::ReadyForTraffic).cloned()).await;
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
    match ends {
        // The old provider is written no more after the switch; nothing of a birth that started after
        // the block is on it.
        Ends::Switched => assert!(lines_naming(other, &new_id).is_empty(), "nothing of the restarted birth {new_id} is on {FILE}: {:#?}", lines_naming(other, &new_id)),
        // A change failed before its switch leaves the new provider as the copy left it.
        Ends::FailedBeforeTheSwitch => assert!(lines_naming(other, &new_id).is_empty(), "nothing of the restarted birth {new_id} is on {TOPOLOGY_STORAGE_TARGET}: {:#?}", lines_naming(other, &new_id)),
    }
    estate.stop().await;
    let spans = estate.spans();
    // The restarted birth read the block from its data dir before it wrote anything, and it unblocked
    // only onto the provider the `commit` record decided, after the fabric-primary read or inserted it.
    let decided: Vec<&Value> = of_build(&spans, SWITCH, &build_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found").is_some()).collect();
    assert!(!decided.is_empty(), "the fabric-primary decided the change by the commit record: {SWITCH} for ({build_id}, {change_id})");
    // A birth that came back before the decision read its block from its data dir and holds it until the
    // decision is read: its block and its unblock are spans under the same change. One launched after the
    // switch is launched on the new provider and owes no unblock (storage.md §2, lost node).
    let reblocked = of_build(&spans, BLOCK, &build_id, Some(&new_id));
    let unblocked = of_build(&spans, UNBLOCK, &build_id, Some(&new_id));
    assert!(reblocked.iter().all(|sp| attr(sp, "change_id") == change_id), "a block the restarted birth read from its data dir is the same change_id: {reblocked:#?}");
    assert!(reblocked.is_empty() || !unblocked.is_empty(), "the restarted birth {new_id} that came back blocked unblocks under the same change: {UNBLOCK}");
    for sp in &unblocked {
        assert_eq!((attr(sp, "change_id"), attr(sp, "provider")), (change_id.clone(), chosen.to_string()), "the restarted birth unblocks onto {chosen} under the same change_id: {sp}");
        assert!(decided.iter().any(|d| start_ns(d) <= start_ns(sp)), "the restarted birth unblocks only once the commit record is read: {sp}");
    }
    // The connection event owed when the process was killed lands at most once, and only on the
    // provider chosen (its queue is the node's writer queue, Q169).
    let owed: Vec<&Value> = named(&spans, OBSERVED).into_iter().filter(|sp| attr(sp, "source") == ROLE && attr(sp, "destination") == "mesh2.admin.1" && attr(sp, "origin") == "dial").collect();
    for sp in &owed {
        let entry = attr(sp, "entry");
        assert!(lines_naming(other, &entry).is_empty(), "the owed connection event {entry} is not on the provider the change did not choose");
        assert!(lines_naming(chosen_root, &entry).len() <= 1, "the owed connection event {entry} lands at most once on {chosen}");
    }
    estate.record_trace_url(&s(&decided[0]["trace_id"]));
}

/// CONTRACT (i149 § Restart-recovery canary, second canary; storage.md §2 "Block": "The node ... records
/// the block and both providers in its data dir, which a restart reads before it writes anything";
/// "Switch": "Whoever resumes the change ... learns the decision only by reading that record"; "A step
/// that fails before the switch fails the Build only once the fabric-primary has read that no `commit`
/// exists ...; each node then unblocks on the old provider, its owed writes landing there"): a role
/// process (mesh2.rpc.1) is killed while it is blocked under a storage change, with a connection event
/// owed, and comes back at its `path.name` over the same data dir. Run twice: a change that switches, and
/// one whose copy is refused before the switch. Each time the restarted birth unblocks only onto the
/// provider the `commit` record decided, once that record is read, under the same `change_id` (a birth
/// back before the decision holds the block it read from its data dir until then); every
/// record of it is on that provider and none on the other; the Fabric row names that provider. Must NOT
/// happen: a write of the restarted birth on the provider not chosen, an unblock before the decision is
/// read, or a new birth over a new data dir counted as the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_storage_state_survives_role_node_restart() {
    role_node_restart("topology_storage_state_survives_role_node_restart-switched", Ends::Switched).await;
    role_node_restart("topology_storage_state_survives_role_node_restart-failed", Ends::FailedBeforeTheSwitch).await;
}

/// CONTRACT (storage.md §2 "Block": the node "records the block and both providers in its data dir, which
/// a restart reads before it writes anything"; "Switch": "Whoever resumes the change ... learns the
/// decision only by reading that record: `commit` found is switched"; i149 R8, R12): a role node blocked
/// under a change to `test-target` is killed while the change is held, and node recovery re-creates it at
/// its `path.name` over the same data dir. The restarted birth stays blocked until the `commit` record is
/// read: any unblock of it is onto `test-target` under the same `change_id`, after the fabric-primary's
/// switch;
/// every record of it is on `test-target`, none on `file`; the Build completes and the fabric names
/// `test-target` with `file` previous. Must NOT happen: the restarted birth writing to `file` (its stale
/// launch provider) or unblocking before the commit record is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read() {
    role_node_restart("a_node_restart_keeps_changes_blocked_until_the_commit_record_is_read", Ends::Switched).await;
}

/// CONTRACT (storage.md §2 "Switch": "the fabric-primary inserts `commit` for `(build_id, change_id)` on
/// the new provider with the store's insert-and-fail primitive, then appends the Fabric row's line ...
/// Whoever resumes the change, a successor or the fabric-primary after a lost answer, learns the decision
/// only by reading that record: `commit` found is switched, and the row's line is written if missing";
/// i149 R10, R11): the fabric-primary is killed (SIGKILL) as soon as the `commit` record for the change
/// is on `test-target`, so the insert's answer is lost to it, and is started again over the same data dir.
/// It resumes the same `change_id`, reads the record (`commit_found = true`) and inserts nothing; exactly
/// one `commit` record for the change is on `test-target`; the Fabric row names `test-target` with `file`
/// previous; every node writes `test-target` and every node but the fabric-primary is the same birth.
/// Must NOT happen: a second `commit` record, the change resumed under a new `change_id`, or the lost
/// answer read as "not switched" (the change failed, or any node unblocked on `file`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_commit_answer_is_resolved_by_reading_the_commit_record() {
    let test = "a_lost_commit_answer_is_resolved_by_reading_the_commit_record";
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let before = births(&views);
    let (file_root, target_root) = roots(&estate);
    let role_id = node_id_of(&estate, ROLE).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let change_id = attr(&blocked_at(&estate, &build_id, &role_id).await, "change_id");
    // The commit record is on the new provider, read through the store's insert-and-fail primitive it is
    // inserted with: the claim of `commit` for (build_id, change_id) under the root's `locks/commit/`.
    let commits = || {
        std::fs::read_dir(target_root.join(rdm_admin::store_root::LOCKS_DIR).join("commit"))
            .map(|d| d.flatten().map(|e| e.file_name().to_string_lossy().into_owned()).filter(|n| !n.starts_with('.') && n.contains(&change_id)).collect::<Vec<_>>())
            .unwrap_or_default()
    };
    wait_for(&format!("the commit record of ({build_id}, {change_id}) is on {TOPOLOGY_STORAGE_TARGET}"), STEP, || async { (!commits().is_empty()).then_some(()) }).await;
    estate.kill_bootstrap();
    let restarted = estate.restart_admin(&estate.bootstrap_data_dir("mesh1"));
    // The estate enters the fabric again at the restarted birth.
    estate.enter(&restarted).await;
    // The Build is followed at the fabric-primary the fabric names after the restart.
    let holder = wait_for(&format!("{restarted}'s fabric names a fabric-primary"), SETTLE, || async { estate.fabric_view().await.fabric_primary }).await;
    estate.enter(&holder).await;
    let done = estate.follow(stream).await;
    estate.artifact("frames.json", &json!(format!("{:?}", done.frames)));
    assert_eq!(commits().len(), 1, "exactly one commit record for ({build_id}, {change_id}): {:#?}", commits());
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    let after = estate.node_views().await;
    nodes_write(&after, TOPOLOGY_STORAGE_TARGET);
    let after_births = births(&after);
    for (name, birth) in before.iter().filter(|(n, _)| n.as_str() != "mesh1.admin.1") {
        assert_eq!(after_births.get(name), Some(birth), "{name}: the same birth serves throughout");
    }
    estate.stop().await;
    let spans = estate.spans();
    let switch: Vec<&Value> = of_build(&spans, SWITCH, &build_id, None);
    assert!(switch.iter().all(|sp| attr(sp, "change_id") == change_id), "every switch of the Build is the same change_id: {switch:#?}");
    let read: Vec<&&Value> = switch.iter().filter(|sp| yes(sp, "commit_found") == Some(true) && sp["attributes"]["outcome"] != "inserted").collect();
    assert!(!read.is_empty(), "the restarted fabric-primary read the commit record (commit_found = true) and inserted none: {switch:#?}");
    assert!(of_build(&spans, UNBLOCK, &build_id, None).iter().all(|sp| attr(sp, "provider") == TOPOLOGY_STORAGE_TARGET), "no node unblocked on {FILE}: the lost answer is never read as not switched");
    estate.record_trace_url(&s(&read[0]["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "A step that fails before the switch fails the Build only once
/// the fabric-primary has read that no `commit` exists for `(build_id, change_id)`; each node then
/// unblocks on the old provider, its owed writes landing there, the new provider closed and the Fabric
/// row unchanged"; step 3: "a new provider holding lines the old one does not is refused by name, and
/// nothing switches"; "No timer ends a block"; i149 R18): `test-target` holds a line `file` does not, so
/// the change's copy is refused before the switch (mesh2's mesh-primary frozen, then continued, holds the
/// change between the block and the copy). The fabric-primary reads that no `commit` record
/// exists (`commit_found = false`) and only then fails the Build; every node unblocks onto `file` under
/// the change's `change_id`, the connection event owed while it was blocked landing on `file`; the Fabric
/// row names `file` with no previous provider; every node writes `file` and is the same birth. Must NOT
/// happen: the Build failed before the record is read, a node left blocked or unblocked onto
/// `test-target`, or an owed write lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider() {
    let test = "a_failed_step_before_the_switch_unblocks_every_node_on_the_old_provider";
    let (mut estate, views) = two_mesh_estate(test, &[]).await;
    let before = births(&views);
    let (file_root, target_root) = roots(&estate);
    foreign_line(&target_root);
    // mesh2's mesh-primary holds the change; mesh1.rpc.1 dials mesh1.admin.2, a node it holds no
    // connection to, while it is blocked.
    let (source, peer, held) = ("mesh1.rpc.1", "mesh1.admin.2", "mesh2.admin.1");
    let role_id = node_id_of(&estate, source).await;
    let hold = freeze(&estate, held).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let change_id = attr(&blocked_at(&estate, &build_id, &role_id).await, "change_id");
    // A write owed while blocked: answered, landed only at the unblock.
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
    let unblocked = of_build(&spans, UNBLOCK, &build_id, None);
    for v in after.iter().filter(|v| v.name.to_string() != held) {
        let id = v.node_id.to_string();
        let own: Vec<&&Value> = unblocked.iter().filter(|sp| attr(sp, "node_id") == id && sp["attributes"]["outcome"] == "unblocked").collect();
        assert!(!own.is_empty(), "{}: unblocked after the failure", v.name);
        for sp in own {
            assert_eq!((attr(sp, "change_id"), attr(sp, "provider")), (change_id.clone(), FILE.to_string()), "{}: unblocks onto {FILE} under the change: {sp}", v.name);
            assert!(read.iter().any(|r| start_ns(r) <= start_ns(sp)), "{}: unblocks only once no commit is read: {sp}", v.name);
        }
    }
    // The control: the owed connection event landed once, on the old provider, after the unblock.
    let owed: Vec<&Value> = named(&spans, OBSERVED).into_iter().filter(|sp| attr(sp, "source") == source && attr(sp, "destination") == peer && attr(sp, "origin") == "dial" && start_ns(sp) >= dialled_ns).collect();
    assert_eq!(owed.len(), 1, "the connection event owed while blocked lands once: {owed:#?}");
    let entry = attr(owed[0], "entry");
    assert_eq!((lines_naming(&file_root, &entry).len(), lines_naming(&target_root, &entry).len()), (1, 0), "the owed connection event {entry} is on {FILE} once and not on {TOPOLOGY_STORAGE_TARGET}");
    estate.record_trace_url(&s(&read[0]["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "No Build is admitted while a change is in progress, a change
/// of `topology_storage_provider` included; `fabric.delete` alone is admitted"; "It is admitted and
/// refused as any Build is, one in progress at a time"; i149 R17, R18): while a change to `test-target`
/// is held after its block, a Build of the complete topology that names no provider and one naming
/// `file` are each refused `build-in-progress` naming the change's Build, and nothing of them is
/// accepted; the change then completes, the Fabric row naming `test-target` with `file` previous. Must
/// NOT happen: a Build accepted during the change, or the change ended by the refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_other_than_fabric_delete_is_refused_during_a_change() {
    let test = "a_build_other_than_fabric_delete_is_refused_during_a_change";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let role_id = node_id_of(&estate, ROLE).await;
    let hold = freeze(&estate, HOLD).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &build_id, &role_id).await;
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
    thaw(&hold);
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
/// one"; i149 R11): a first change to `test-target` runs while mesh2's mesh-primary, the carrier of its
/// commands to mesh2.rpc.1, holds the first block command of that change on its way to mesh2.rpc.1 (the
/// testkit's storage-step cut); the fabric-primary sends it again and the change completes. A second
/// change, back to `file`, is held after mesh2.rpc.1 has blocked under it, and the held command of the
/// first change is released to mesh2.rpc.1.
/// mesh2.rpc.1 refuses every command of the first change it receives after its block under the second,
/// and stays blocked until the second change's switch: its only unblock after that block is the second
/// change's, onto `file`. Must NOT happen: an unblock of the node by the first change's `change_id`, or
/// an unblock before the second change's switch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_change_cannot_unblock_writes_for_a_new_change() {
    let test = "an_old_change_cannot_unblock_writes_for_a_new_change";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let role_id = node_id_of(&estate, ROLE).await;
    // mesh2's mesh-primary holds the first change's first block command of mesh2.rpc.1 on its way.
    let carrier = rdm_test::faults::Door::at(&estate, "mesh2.admin.1").await;
    carrier.arm("old-command", json!({"kind": "storage-step", "node": ROLE, "step": "block"})).await;
    let (first, first_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    carrier.wait_held("old-command").await;
    let old_change = attr(&blocked_at(&estate, &first_id, &role_id).await, "change_id");
    estate.follow(first).await;
    let hold = freeze(&estate, HOLD).await;
    let (second, second_id) = change_to(&estate, FILE).await;
    let blocked = blocked_at(&estate, &second_id, &role_id).await;
    let new_change = attr(&blocked, "change_id");
    assert_ne!(old_change, new_change, "each change has its own change_id");
    carrier.release("old-command").await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    thaw(&hold);
    estate.follow(second).await;
    fabric_names(&estate.fabric_view().await, FILE, &[FILE, TOPOLOGY_STORAGE_TARGET]);
    estate.stop().await;
    let spans = estate.spans();
    let switched = of_build(&spans, SWITCH, &second_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the second change switched");
    let at_node: Vec<&Value> = spans.iter().filter(|sp| (sp["name"] == BLOCK || sp["name"] == UNBLOCK) && attr(sp, "node_id") == role_id && start_ns(sp) > start_ns(&blocked)).collect();
    let stale: Vec<&&Value> = at_node.iter().filter(|sp| attr(sp, "change_id") == old_change).collect();
    assert!(!stale.is_empty(), "the old mesh-primary birth delivered a command of the first change after the node blocked under the second: {at_node:#?}");
    assert!(stale.iter().all(|sp| sp["attributes"]["outcome"] != "unblocked" && sp["attributes"]["outcome"] != "blocked"), "every command of the older change is refused by name: {stale:#?}");
    let unblocked: Vec<&&Value> = at_node.iter().filter(|sp| sp["name"] == UNBLOCK && sp["attributes"]["outcome"] == "unblocked").collect();
    assert!(!unblocked.is_empty() && unblocked.iter().all(|sp| attr(sp, "change_id") == new_change && attr(sp, "provider") == FILE && start_ns(sp) >= start_ns(switched)), "the node's only unblock after its block is the second change's, onto {FILE}, after its switch: {unblocked:#?}");
    estate.record_trace_url(&s(&blocked["trace_id"]));
}

/// CONTRACT (storage.md §2 "Block": "Every write it would make is held as owed, never refused ... a
/// connection event stays owed on the node's writer queue (Q169)"; "Unblock": "Each node moves to the new
/// provider, lands its owed writes there in its own order"; i149 R9): mesh1.rpc.1, blocked under a
/// change to `test-target` (held by mesh2's mesh-primary, frozen), dials mesh1.admin.2, a node it holds
/// no connection to; the dial is answered at once. While
/// the change is held no line of that connection event is on either provider; after the change completes
/// it is on `test-target` exactly once, its write begun after the node's unblock, and never on `file`.
/// Must NOT happen: the open waiting on the write, the event written to `file` after the block, lost, or
/// landed twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_event_during_a_change_lands_once_after_the_unblock() {
    let test = "a_connection_event_during_a_change_lands_once_after_the_unblock";
    let (source, destination) = ("mesh1.rpc.1", "mesh1.admin.2");
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let source_id = node_id_of(&estate, source).await;
    let hold = freeze(&estate, "mesh2.admin.1").await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &build_id, &source_id).await;
    let dialled_ns = now_ns();
    dial(&estate, source, destination);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let landed_while_blocked: Vec<Value> = estate.spans().into_iter().filter(|sp| sp["name"] == OBSERVED && attr(sp, "source") == source && start_ns(sp) >= dialled_ns).collect();
    assert!(landed_while_blocked.is_empty(), "no connection event of the blocked node is written while it is blocked: {landed_while_blocked:#?}");
    thaw(&hold);
    estate.follow(stream).await;
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    estate.stop().await;
    let spans = estate.spans();
    let owed: Vec<&Value> = named(&spans, OBSERVED).into_iter().filter(|sp| attr(sp, "source") == source && attr(sp, "destination") == destination && attr(sp, "origin") == "dial" && start_ns(sp) >= dialled_ns).collect();
    assert_eq!(owed.len(), 1, "the connection event owed while blocked is written once: {owed:#?}");
    let entry = attr(owed[0], "entry");
    assert_eq!((lines_naming(&target_root, &entry).len(), lines_naming(&file_root, &entry).len()), (1, 0), "the connection event {entry} is on {TOPOLOGY_STORAGE_TARGET} once and never on {FILE}");
    let unblock = of_build(&spans, UNBLOCK, &build_id, Some(&source_id)).into_iter().find(|sp| sp["attributes"]["outcome"] == "unblocked").expect("the node unblocked");
    assert_eq!(attr(unblock, "provider"), TOPOLOGY_STORAGE_TARGET, "the node unblocks onto {TOPOLOGY_STORAGE_TARGET}: {unblock}");
    assert!(start_ns(owed[0]) >= start_ns(unblock), "the owed write is begun after the unblock: {} vs {unblock}", owed[0]);
    estate.record_trace_url(&s(&blocked["trace_id"]));
}

/// CONTRACT (storage.md §2: "A node that does not acknowledge the block or does not unblock is a lost
/// node, as any node is: it is investigated, and on any verdict but `reached` node recovery re-creates it
/// at its `path.name`, an attempt of the change's own Build. The new birth is launched ... on the new
/// provider after [the switch]. After the switch the Build completes once every node has unblocked or
/// been re-created; nothing else waits"; Q182: a node not replying to commands is silent; Q188): the role
/// node acknowledges its block, then is frozen (alive and silent) so it never unblocks. Node
/// investigation re-creates it at mesh2.rpc.1 under a new node id, launched on `test-target`: it writes
/// `test-target`, is ready-for-traffic, its records are on `test-target` and none on `file`; the old
/// birth never unblocks; the Build completes and the Fabric row names `test-target` with `file` previous.
/// Must NOT happen: the Build waiting on the frozen birth forever, the old birth unblocking, or the new
/// birth launched on `file`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_never_unblocks_is_recreated_on_the_new_provider() {
    let test = "a_node_that_never_unblocks_is_recreated_on_the_new_provider";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let old_id = node_id_of(&estate, ROLE).await;
    let hold = freeze(&estate, HOLD).await;
    let (stream, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let blocked = blocked_at(&estate, &build_id, &old_id).await;
    let silent = freeze(&estate, ROLE).await;
    thaw(&hold);
    let done = estate.follow(stream).await;
    estate.artifact("frames.json", &json!(format!("{:?}", done.frames)));
    let renewed = estate.until_node(ROLE, "the role node re-created at its path.name", SETTLE, |v| v.filter(|v| v.node_id.to_string() != old_id && v.status == NodeStatus::ReadyForTraffic).cloned()).await;
    let new_id = renewed.node_id.to_string();
    assert_eq!(renewed.topology_storage_provider.as_deref(), Some(TOPOLOGY_STORAGE_TARGET), "the re-created birth is launched on the new provider: {renewed:?}");
    assert!(!lines_naming(&target_root, &new_id).is_empty() && lines_naming(&file_root, &new_id).is_empty(), "the re-created birth's records are on {TOPOLOGY_STORAGE_TARGET} and none on {FILE}");
    fabric_names(&estate.fabric_view().await, TOPOLOGY_STORAGE_TARGET, &[FILE]);
    thaw(&silent);
    estate.stop().await;
    let spans = estate.spans();
    assert!(of_build(&spans, UNBLOCK, &build_id, Some(&old_id)).iter().all(|sp| sp["attributes"]["outcome"] != "unblocked"), "the frozen birth never unblocks");
    let switched = of_build(&spans, SWITCH, &build_id, None).into_iter().find(|sp| sp["attributes"]["outcome"] == "inserted").expect("the change switched");
    assert!(start_ns(switched) >= start_ns(&blocked), "the switch follows the frozen birth's acknowledgement");
    estate.record_trace_url(&s(&switched["trace_id"]));
}

/// CONTRACT (storage.md §2 "Changing it": "`fabric.delete` alone is admitted, and it interrupts the change
/// as it interrupts any Build, the records on both providers kept"; "Whoever resumes the change ...
/// learns the decision only by reading that record"; i149 R12: "fabric.delete resolve[s] the current
/// migration before issuing ordinary writes"): while a change to `test-target` is held after its block,
/// `fabric.delete` is admitted and completes. Before its teardown begins, the delete's executor reads the
/// change's `commit` record (`commit_found = false`: not switched), and the change is never switched
/// (no switch inserted); every line `file` held at the delete's acceptance is still on `file` and every
/// line `test-target` held then is still there. Must NOT
/// happen: the delete refused, its teardown begun before the change is resolved, the change switched
/// under it, or a record of either provider removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_resolves_the_storage_change_before_teardown() {
    let test = "a_delete_resolves_the_storage_change_before_teardown";
    let (mut estate, _) = two_mesh_estate(test, &[]).await;
    let (file_root, target_root) = roots(&estate);
    let role_id = node_id_of(&estate, ROLE).await;
    let hold = freeze(&estate, HOLD).await;
    let (_change, build_id) = change_to(&estate, TOPOLOGY_STORAGE_TARGET).await;
    let change_id = attr(&blocked_at(&estate, &build_id, &role_id).await, "change_id");
    let accepted_ns = now_ns();
    let delete = estate.carrier().await.create(Change::DeleteFabric).await.unwrap_or_else(|end| panic!("fabric.delete during the change is admitted: {end}"));
    let delete_id = delete.build_id().to_string();
    let (file_at, target_at) = (store_lines(&file_root), store_lines(&target_root));
    thaw(&hold);
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
    estate.stop().await;
    let spans = estate.spans();
    let resolved = of_build(&spans, SWITCH, &build_id, None).into_iter().filter(|sp| attr(sp, "change_id") == change_id && yes(sp, "commit_found") == Some(false) && start_ns(sp) >= accepted_ns).min_by_key(|sp| start_ns(sp)).unwrap_or_else(|| panic!("the delete's executor read the change's commit record (commit_found = false) for ({build_id}, {change_id})"));
    assert!(of_build(&spans, SWITCH, &build_id, None).iter().all(|sp| sp["attributes"]["outcome"] != "inserted"), "the change is never switched under the delete");
    let teardown = spans.iter().filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_admin.node.delete.") || n.starts_with("rdm.mesh.node.delete.")) && start_ns(sp) >= accepted_ns).map(start_ns).min().expect("the delete tore the nodes down");
    assert!(start_ns(resolved) <= teardown, "the change is resolved before the delete's teardown begins (delete {delete_id})");
    estate.record_trace_url(&s(&resolved["trace_id"]));
}
