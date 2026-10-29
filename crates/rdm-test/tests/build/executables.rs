//! product=mesh, feature=build, subfeature=executables, rung=MM, provider=process.
//!
//! The executable a node runs, named per node at create (proposal executable-bindings-at-create.md, Q157):
//! `build.create { Change(Reconcile { based_on, nodes }) }` submits the complete topology, every node by
//! its create spec (its `path.name` and its properties, flat: Q166), the executable included. A node left out is removed, one added is
//! created on the executable its entry names (its row records it), one whose entry names another
//! executable than its row records is replaced at its path (node.delete, then node.create) in RDM's order,
//! and a mesh named for the first time is created with its first node-admin; a mesh left out is
//! mesh.delete. The request is refused by name when it was built from a Build that is no longer current,
//! when it lists no node, and when a path it creates names no executable.
//!
//! [`read_request`] is the harness's complete request: built, as a caller builds it, from the current
//! topology read and `fabric.nodes.get`.
//!
//! A new executable is the same program copied with one byte appended: another sha256, the same
//! behaviour, as a new build of it would be.

use crate::build_log;
use rdm_client::{BuildFrame, CallEnd, Change, FabricDesired, LaunchKind as NodeKind, MeshDesired, NodeView, PathName, ScopeStatus};
use rdm_core::binding::{sha256_file, Binding};
use rdm_core::meta::NodeSpec;
use rdm_test::estate::{Estate, Owner};
use rdm_test::faults::{binding_set, binding_set_with_roles, candidate_sha, Door};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(180);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "build".into(), subfeature: "executables".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// The complete-topology request a caller builds from what it reads now: `based_on` the current Build
/// (`fabric.get` at the fabric-primary), and every path of that Build's accepted topology with its
/// create spec, the executable its entry names, else the one `fabric.nodes.get` reads from the path's row,
/// and the tags and config that row carries.
pub async fn read_request(estate: &Estate) -> (rdm_core::BuildId, Vec<NodeSpec>) {
    let based_on = estate.accepted_build_id().await;
    let carrier = estate.carrier().await;
    let facts = build_log::log_read(&carrier, &based_on).await.unwrap_or_else(|e| panic!("the Build log of the fabric-primary holds no Build {based_on}: {e}"));
    let build = build_log::projection(&facts, &based_on).unwrap_or_else(|| panic!("the fabric-primary's log holds no acceptance of {based_on}"));
    let nodes = estate.nodes_obj().await.get(&rdm_client::Filter::new()).await.unwrap_or_else(|e| panic!("node.get: {e}"));
    let mut out = Vec::new();
    for m in build.topology.meshes.values() {
        for p in &m.nodes {
            let mut spec = m.spec(p).cloned().unwrap_or_else(|| panic!("{p} carries its create spec"));
            let row = nodes.iter().find(|n| &n.name == p);
            if spec.executable.is_none() {
                spec.executable = row.and_then(|n| n.executable.clone());
            }
            // The node's own tags and config as its row holds them: the Build applies every property that
            // differs from the row (build.md §1), so a request read now names what the node holds.
            if let Some(n) = row {
                (spec.tags, spec.config) = (n.tags.clone(), n.config.clone());
            }
            out.push(spec);
        }
    }
    (rdm_core::BuildId::parse(&based_on).expect("a Build id"), out)
}

/// `request` with the entry at `path` (added when it lists none) on `executable`.
fn naming(mut request: Vec<NodeSpec>, path: &str, executable: Option<&Binding>) -> Vec<NodeSpec> {
    let p: PathName = path.parse().unwrap();
    match request.iter_mut().find(|s| s.path == p) {
        Some(s) => s.executable = executable.cloned(),
        None => {
            let mut s = NodeSpec::default_for(p);
            s.executable = executable.cloned();
            request.push(s);
        }
    }
    request
}

/// `b` copied under the estate's root with one byte appended: the same program under another sha256.
fn another_build(estate: &Estate, b: &Binding, tag: &str) -> Binding {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    let dir = estate.root.join("executables");
    std::fs::create_dir_all(&dir).unwrap();
    let to = dir.join(format!("{}-{tag}", b.launch_id));
    std::fs::copy(&b.executable, &to).unwrap();
    std::fs::OpenOptions::new().append(true).open(&to).unwrap().write_all(&[0]).unwrap();
    std::fs::set_permissions(&to, std::fs::Permissions::from_mode(0o755)).unwrap();
    Binding { sha256: sha256_file(&to).unwrap(), executable: to, ..b.clone() }
}

/// The executable `node`'s process runs.
async fn exe_of(estate: &Estate, node: &str) -> std::path::PathBuf {
    let pid = estate.pid_of(node).await;
    std::fs::read_link(format!("/proc/{pid}/exe")).unwrap_or_else(|e| panic!("{node} (pid {pid}): /proc exe: {e}"))
}

/// The Node record of `node_id` on the estate's store root, as written.
fn record_of(estate: &Estate, node_id: &str) -> Value {
    estate.node_record(node_id).unwrap_or_else(|| panic!("the record of {node_id} is not in nodes.jsonl"))
}

/// Every node `fabric.nodes.get` answers, by `path.name`.
async fn fabric_nodes(estate: &Estate) -> BTreeMap<String, NodeView> {
    estate.nodes_obj().await.get(&rdm_client::Filter::new()).await.unwrap_or_else(|e| panic!("node.get: {e}")).into_iter().map(|n| (n.name.to_string(), n)).collect()
}

/// A Build stream followed to its `Complete` (across a fabric-primary move), bounded: a Build that does not
/// complete within the bound fails the cell naming it.
async fn followed(estate: &Estate, stream: rdm_client::BuildStream) -> rdm_test::typed::Reconciled {
    let id = stream.build_id().to_string();
    tokio::time::timeout(SETTLE * 3, estate.follow(stream)).await.unwrap_or_else(|_| panic!("Build {id} reached no terminal frame within {:?}", SETTLE * 3))
}

/// The refusal a call ended with.
fn rejected(what: &str, call: Result<rdm_client::BuildStream, CallEnd>) -> (String, String) {
    match call {
        Err(CallEnd::Rejected { reason, detail }) => (reason, detail),
        Err(other) => panic!("{what}: expected a refusal by name, got {other}"),
        Ok(s) => panic!("{what}: the fabric-primary accepted Build {}", s.build_id()),
    }
}

fn receipts_of<'a>(receipts: &'a [rdm_core::build::StepReceipt], operation: &str, step: &str) -> Vec<&'a rdm_core::build::StepReceipt> {
    receipts.iter().filter(|r| r.operation == operation && r.step == step).collect()
}

async fn receipts(estate: &Estate, build: &str) -> Vec<rdm_core::build::StepReceipt> {
    estate.carrier().await.build(&rdm_client::BuildId::parse(build).unwrap()).await.unwrap_or_else(|e| panic!("build.get [id: {build}]: {e}")).steps
}

/// The fabric-primary now, the fabric's status as it holds it, and the Build whose topology it accepted.
async fn fabric_at_its_primary(estate: &Estate) -> (String, ScopeStatus, Option<String>) {
    let fp = estate.node_views().await.into_iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    let view = rdm_test::typed::the_fabric(&rdm_client::Fabrics::new(estate.carrier_at(&Estate::path(&fp)).await)).await.unwrap_or_else(|e| panic!("fabric.get at {fp}: {e}"));
    (fp, view.status, view.build_id)
}

/// CONTRACT (proposal executable-bindings-at-create.md rule 1, "The request shape": "one field for the whole
/// request, `based_on`: the build_id of the current Build the caller read. RDM refuses the request by name
/// when that Build is no longer current, nothing accepted, and the caller re-reads"; "a request that ...
/// lists no nodes at all, is refused by name pointing to fabric.delete"; "a created path whose config names
/// no executable is refused by name at acceptance, nothing runs"; rule 3, "Recorded on the row at the claim";
/// rule 7, read from the records). Two callers read the same topology. B's request adds `mesh1.rpc.2` on a
/// new executable with the tag `zone_a` and the config {zone: a} (Q166; node-config-update.md: "A node's
/// first config is its create spec's `config` in the Build, written to its row at the claim and carried
/// in its first join") and completes: the node runs that executable, its row records it, the tag and the
/// config, its first join hands it the config (the join span's `config_keys` 1, never 0) and the node
/// carries it (`fabric.nodes.get` answers a live node's config from its digest), and `fabric.nodes.get`
/// returns them. A's request, built from the earlier read, is refused `based-on-not-current`, nothing
/// accepted and `mesh1.rpc.2` untouched. A re-reads: a request listing no node is refused `no-node-listed`
/// naming fabric.delete, one creating `mesh1.rpc.3` with no executable `executable-missing`, one creating it
/// on an executable whose bytes do not hash to the sha it names `executable-refused` (build-deployment.md
/// §2.4, "a differing hash is refused by name"), and a request at the reserved position of the count
/// form that created a node (`AddNode`'s, proposal §3.10(a)) is an unknown variant
/// (`Malformed(UnknownVariant)`, node-rpc.md §26), nothing accepted; the re-read request itself is accepted and
/// completes, changing nothing. The bootstrap admin's row records the hash of the executable it runs
/// (build-deployment.md §2.3: "the bootstrap admin's own included: it hashes the executable it is
/// running").
/// Must NOT happen: a stale list removing the node B added, an empty list read as delete everything, or a
/// created node launched on an executable no request named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_named_create_runs_its_executable_and_a_stale_or_incomplete_request_is_refused_by_name() {
    let estate = Estate::bootstrap(owner("a_named_create_runs_its_executable_and_a_stale_or_incomplete_request_is_refused_by_name"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1)]).await;
    estate.settles_to(&[("mesh1", 2, 1)], SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let (a_based_on, a_request) = read_request(&estate).await;
    let (b_based_on, b_request) = read_request(&estate).await;
    assert_eq!(a_based_on, b_based_on);
    let rpc = b_request.iter().find(|s| s.path.to_string() == "mesh1.rpc.1").and_then(|s| s.executable.clone()).expect("mesh1.rpc.1's row records its executable");
    let next = another_build(&estate, &rpc, "next");

    // B: mesh1.rpc.2, named on the new executable with its tags.
    let mut b_nodes = naming(b_request, "mesh1.rpc.2", Some(&next));
    let zone: std::collections::BTreeMap<String, String> = [("zone".to_string(), "a".to_string())].into();
    let entry = b_nodes.iter_mut().find(|s| s.path.to_string() == "mesh1.rpc.2").expect("the entry named");
    entry.tags = ["zone_a".to_string()].into();
    entry.config = zone.clone();
    let b = followed(&estate, estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on: b_based_on, nodes: b_nodes }).await).await.build_id.to_string();
    let created = estate.until_node("mesh1.rpc.2", "mesh1.rpc.2 ready", SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready).cloned()).await;
    assert_eq!(exe_of(&estate, "mesh1.rpc.2").await, next.executable, "the created node runs the executable its request named");
    let row = record_of(&estate, created.node_id.as_str());
    assert_eq!(row["record"]["executable"]["sha256"], json!(next.sha256), "its row records it: {row}");
    let read = fabric_nodes(&estate).await;
    assert_eq!(read["mesh1.rpc.2"].executable.as_ref(), Some(&next), "fabric.nodes.get returns it from the row");
    assert_eq!(read["mesh1.rpc.2"].tags, ["zone_a".to_string()].into(), "the node carries the tags its entry named, from its claim");
    assert_eq!(row["record"]["config"], json!({"zone": "a"}), "its row records its create spec's config from the claim: {row}");
    assert_eq!(read["mesh1.rpc.2"].config, zone, "the node carries the config its create spec named");
    // Spans reach the evidence folder in batches (rdm-transport telemetry), so the join the node made is read once it is there.
    let node_id = created.node_id.to_string();
    let joins: Vec<Value> = rdm_test::estate::wait_for("the join span of mesh1.rpc.2 in the evidence", SETTLE, || async {
        let joins: Vec<Value> = estate.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.node.update.via-join" && s["attributes"]["node_id"] == json!(node_id)).collect();
        (!joins.is_empty()).then_some(joins)
    })
    .await;
    let keys: Vec<&str> = joins.iter().filter_map(|j| j["attributes"]["config_keys"].as_str()).collect();
    assert!(keys.contains(&"1") && !keys.contains(&"0"), "its first join hands it the config, never none: {joins:#?}");
    assert_eq!(read["mesh1.rpc.1"].executable.as_ref(), Some(&rpc), "and every other node's");

    // A, from its earlier read: refused, nothing accepted, mesh1.rpc.2 untouched.
    let carrier = estate.carrier().await;
    let (reason, detail) = rejected("a request based on the Build before B's", carrier.create(Change::Reconcile { topology_storage_provider: None, based_on: a_based_on.clone(), nodes: a_request }).await);
    assert_eq!(reason, "based-on-not-current", "{detail}");
    assert!(detail.contains(&a_based_on.to_string()) && detail.contains(&b), "the detail names both Builds: {detail}");
    assert_eq!(estate.accepted_build_id().await, b, "nothing is accepted");

    // A re-reads.
    let (based_on, request) = read_request(&estate).await;
    assert_eq!(based_on.to_string(), b);
    assert!(request.iter().any(|s| s.path.to_string() == "mesh1.rpc.2"), "the read holds what B added");
    let (reason, detail) = rejected("a request listing no node", carrier.create(Change::Reconcile { topology_storage_provider: None, based_on: based_on.clone(), nodes: vec![] }).await);
    assert_eq!(reason, "no-node-listed", "{detail}");
    assert!(detail.contains("fabric.delete"), "the refusal names fabric.delete: {detail}");
    let (reason, detail) = rejected("a created path naming no executable", carrier.create(Change::Reconcile { topology_storage_provider: None, based_on: based_on.clone(), nodes: naming(request.clone(), "mesh1.rpc.3", None) }).await);
    assert_eq!(reason, "executable-missing", "{detail}");
    assert!(detail.contains("mesh1.rpc.3"), "{detail}");
    let differing = Binding { sha256: "0".repeat(64), ..next.clone() };
    let (reason, detail) = rejected("a created path on an executable whose bytes hash otherwise", carrier.create(Change::Reconcile { topology_storage_provider: None, based_on: based_on.clone(), nodes: naming(request.clone(), "mesh1.rpc.3", Some(&differing)) }).await);
    assert_eq!(reason, "executable-refused", "{detail}");
    assert!(detail.contains("mesh1.rpc.3"), "{detail}");
    let (reason, detail) = rejected("a request at a count form's reserved position", carrier.create(Change::Reserved2).await);
    assert_eq!((reason.as_str(), detail.as_str()), ("malformed", "UnknownVariant"), "the count forms are reserved positions: a request at one is an unknown variant");
    assert_eq!(estate.accepted_build_id().await, b, "nothing is accepted");
    assert!(estate.node_value_opt("mesh1.rpc.3").await.is_none(), "nothing runs at mesh1.rpc.3");

    followed(&estate, estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: request }).await).await;
    assert_eq!(estate.node_value("mesh1.rpc.2").await["node_id"], json!(created.node_id), "the re-read request changes nothing");
    // The bootstrap admin is the estate's own process, launched by no pipeline: its row records the binary it starts.
    let day0 = record_of(&estate, &s(&estate.node_value("mesh1.admin.1").await["node_id"]));
    let started = rdm_test::estate::binary("rdm-node-admin");
    assert_eq!(day0["record"]["executable"]["sha256"], json!(sha256_file(&started).unwrap()), "the bootstrap admin's row records the hash of {} it runs: {day0}", started.display());
    estate.shutdown().await;
}

/// A two-mesh fabric of the faulted node-admin with the role kinds bound, each mesh two node-admins, a
/// broker, two gateways and a compute node, ready.
async fn role_fabric(test: &str) -> Estate {
    let sha = candidate_sha();
    let estate = Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set_with_roles(&sha), &sha, &["broker", "gateway", "compute"], &[]).await.expect("the faulted-admin binding set with the role kinds is accepted");
    let mesh = |m: &str| MeshDesired::of(m, [(NodeKind::NodeAdmin, 2), (NodeKind::Broker, 1), (NodeKind::Gateway, 2), (NodeKind::Compute, 1)]);
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await;
    let want: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| ["admin.1", "admin.2", "broker.1", "gateway.1", "gateway.2", "compute.1"].map(|n| format!("{m}.{n}"))).collect();
    estate.settles_to_named(&want, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    estate
}

/// The request that names `next` for every gateway and leaves every other entry as read.
async fn gateways_on(estate: &Estate, next: &Binding) -> (rdm_core::BuildId, Vec<NodeSpec>) {
    let (based_on, mut request) = read_request(estate).await;
    for s in request.iter_mut().filter(|s| s.path.kind == NodeKind::Gateway) {
        assert!(s.executable.is_some(), "{}'s row records its executable", s.path);
        s.executable = Some(next.clone());
    }
    (based_on, request)
}

/// Every node of `before` but the gateways and `except` is the same birth, never stopped or restarted.
async fn untouched(estate: &Estate, before: &BTreeMap<String, NodeView>, build: &str, except: &[&str]) {
    let now = fabric_nodes(estate).await;
    let steps = receipts(estate, build).await;
    for (name, v) in before.iter().filter(|(n, v)| v.kind != NodeKind::Gateway && !except.contains(&n.as_str())) {
        let n = now.get(name).unwrap_or_else(|| panic!("{name} is still listed"));
        assert_eq!((&n.node_id, &n.incarnation_id), (&v.node_id, &v.incarnation_id), "{name}: the same node.id and incarnation");
        assert_eq!(n.status, rdm_client::NodeStatus::Ready, "{name}");
        let touched: Vec<String> = steps.iter().filter(|r| r.operation.ends_with(&format!(":{name}"))).map(|r| format!("{} {}", r.operation, r.step)).collect();
        assert!(touched.is_empty(), "{name}: no operation of the Build acts on it: {touched:?}");
    }
}

/// CONTRACT (proposal executable-bindings-at-create.md rule 2: "A live node whose row already records its
/// entry's config and executable is not deleted, created or restarted because another node changes. A
/// request that changes only the gateways' executables redeploys the gateways and leaves every broker,
/// compute and admin running as it was"; rule 5: "a type's non-primary members before its primary"; Q154:
/// the Build is Complete only once the fabric is ready again from it): a two-mesh fabric's request changes
/// only its gateways' executables. Every gateway comes back at its `path.name` as a new node.id and
/// incarnation running the new executable, deleted once and created once; in each mesh its non-primary
/// gateway before its primary. Every broker, compute node and node-admin is the same birth, and no
/// operation of the Build acts on it. At `Complete` the fabric-primary holds the fabric ready
/// under the Build. Must NOT happen: an untouched node stopped, restarted or re-created, or a gateway left on
/// its old executable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_changing_only_the_gateways_executables_replaces_every_gateway_and_nothing_else() {
    let estate = role_fabric("a_request_changing_only_the_gateways_executables_replaces_every_gateway_and_nothing_else").await;
    let before = fabric_nodes(&estate).await;
    let gateway = before["mesh1.gateway.1"].executable.clone().expect("a gateway's row records its executable");
    let next = another_build(&estate, &gateway, "next");
    let (based_on, request) = gateways_on(&estate, &next).await;
    let build = followed(&estate, estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: request }).await).await.build_id.to_string();
    let (fp, status, accepted) = fabric_at_its_primary(&estate).await;
    assert_eq!((status, accepted.as_deref()), (ScopeStatus::Ready, Some(build.as_str())), "Complete only once the fabric is ready again under it ({fp})");

    let steps = receipts(&estate, &build).await;
    for (name, v) in before.iter().filter(|(_, v)| v.kind == NodeKind::Gateway) {
        let now = estate.until_node(name, &format!("{name} ready as a new node"), SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready && n.node_id != v.node_id).cloned()).await;
        assert_ne!(now.incarnation_id, v.incarnation_id, "{name}: a new birth");
        assert_eq!(exe_of(&estate, name).await, next.executable, "{name} runs the new executable");
        assert_eq!(receipts_of(&steps, &format!("node.delete:{name}"), "NodeDeleted").len(), 1, "{name} is deleted once");
        assert_eq!(receipts_of(&steps, &format!("node.create:{name}"), "Complete").len(), 1, "{name} is created once");
    }
    for mesh in ["mesh1", "mesh2"] {
        let (primary, other): (Vec<&NodeView>, Vec<&NodeView>) = before.values().filter(|v| v.kind == NodeKind::Gateway && v.mesh == mesh).partition(|v| v.is_primary);
        let at = |v: &NodeView| steps.iter().position(|r| r.operation == format!("node.delete:{}", v.name) && r.step == "NodeDeleted").unwrap();
        assert!(at(other[0]) < at(primary[0]), "{mesh}: its non-primary gateway is replaced before its primary");
    }
    untouched(&estate, &before, &build, &[]).await;
    estate.shutdown().await;
}

/// CONTRACT (proposal executable-bindings-at-create.md rule 2: "Each attempt then computes remaining work
/// from the accepted topology, the current node rows, observed state and its receipts ... an attempt
/// interrupted halfway finishes the remaining gateways the same way"): the executor of mesh2's gateways'
/// replacement, mesh2's primary node-admin, is held by its fault door at the `Complete` of its first
/// gateway's create (the successor Ready, its receipt not written) and killed. The next attempt
/// finishes the rest: every gateway runs the new executable, each deleted once, and the successor born
/// before the kill is the one at its path when the Build completes, never replaced again. Every broker,
/// compute node and node-admin but the killed one is the same birth. Must NOT happen: a gateway replaced
/// twice, a gateway left on its old executable, or another node re-created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_killed_halfway_through_the_gateways_replacement_leaves_the_rest_to_the_next_attempt() {
    let estate = role_fabric("an_executor_killed_halfway_through_the_gateways_replacement_leaves_the_rest_to_the_next_attempt").await;
    let before = fabric_nodes(&estate).await;
    let p = before.values().find(|v| v.mesh == "mesh2" && v.kind == NodeKind::NodeAdmin && v.is_primary).map(|v| v.name.to_string()).expect("mesh2's primary admin");
    let first = before.values().find(|v| v.mesh == "mesh2" && v.kind == NodeKind::Gateway && !v.is_primary).map(|v| v.name.to_string()).expect("mesh2's non-primary gateway");
    let door = Door::at(&estate, &p).await;
    door.arm("gateway-created", json!({"kind": "receipt", "step": "Complete", "operation": format!("node.create:{first}")})).await;
    let gateway = before["mesh1.gateway.1"].executable.clone().expect("a gateway's row records its executable");
    let next = another_build(&estate, &gateway, "next");
    let (based_on, request) = gateways_on(&estate, &next).await;

    let stream = estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: request }).await;
    let build = stream.build_id().to_string();
    let held = door.wait_held("gateway-created").await;
    assert_eq!((s(&held["hit"]["build_id"]), s(&held["hit"]["operation"])), (build.clone(), format!("node.create:{first}")), "{p} is held at {first}'s create Complete: {held}");
    let successor = estate.until_node(&first, &format!("{first}'s successor ready"), SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready && n.node_id != before[&first].node_id).cloned()).await;
    let left: Vec<String> = before.values().filter(|v| v.kind == NodeKind::Gateway && v.name.to_string() != first && v.mesh == "mesh2").map(|v| v.name.to_string()).collect();
    assert_eq!(left.len(), 1);
    assert_eq!(estate.node_value(&left[0]).await["node_id"], json!(before[&left[0]].node_id), "halfway: mesh2's other gateway is not replaced yet");
    estate.kill_node(&p).await;

    followed(&estate, stream).await;
    let steps = receipts(&estate, &build).await;
    for (name, v) in before.iter().filter(|(_, v)| v.kind == NodeKind::Gateway) {
        estate.until_node(name, &format!("{name} ready as a new node"), SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready && n.node_id != v.node_id).map(|_| ())).await;
        assert_eq!(exe_of(&estate, name).await, next.executable, "{name} runs the new executable");
        assert_eq!(receipts_of(&steps, &format!("node.delete:{name}"), "NodeDeleted").len(), 1, "{name} is deleted once");
    }
    assert_eq!(estate.node_value(&first).await["node_id"], json!(successor.node_id), "{first}'s successor born before the kill is the member: never replaced again");
    untouched(&estate, &before, &build, &[p.as_str()]).await;
    estate.shutdown().await;
}

/// CONTRACT (proposal executable-bindings-at-create.md rule 5, R4 arm A: "a mesh whose only node-admin is to
/// be replaced first gets a second node-admin on the new executable at a new `path.name` of the node-admin
/// type in that mesh (the next free one, `mesh1.admin.2`), whose entry the compiler adds to the accepted
/// topology at acceptance, so every attempt and every read holds it and no attempt removes it as left out;
/// it is created, Ready and mesh-primary ... before the original is replaced, and stays"; rule 1:
/// "every node RDM originates ... appears in that read, so the next request lists it"): mesh2 has one
/// node-admin, and the request names a new executable for it. The accepted Build's topology holds
/// `mesh2.admin.2` from acceptance. Its executor, mesh2's admin, is held at the added admin's create
/// `Complete` and killed there. The next attempts keep the added admin (the same node) and finish: the
/// original is re-created at `mesh2.admin.1` on the new executable. `fabric.nodes.get` lists the added
/// admin on the new executable; the next complete request, read after, lists it and is accepted without
/// touching it, and a request without it, under the stale `based_on`, is refused.
/// Must NOT happen: the added admin removed as left out, or the original left on its old executable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_one_admin_meshs_second_admin_is_accepted_first_and_kept_when_its_executor_is_killed() {
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let estate = Estate::bootstrap_on(owner("a_one_admin_meshs_second_admin_is_accepted_first_and_kept_when_its_executor_is_killed"), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let shape = [("mesh1", 2, 1), ("mesh2", 1, 1)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let before = fabric_nodes(&estate).await;
    let admin = before["mesh2.admin.1"].executable.clone().expect("mesh2.admin.1's row records its executable");
    let next = another_build(&estate, &admin, "next");
    let door = Door::at(&estate, "mesh2.admin.1").await;
    door.arm("added-admin-created", json!({"kind": "receipt", "step": "Complete", "operation": "node.create:mesh2.admin.2"})).await;
    let (based_on, request) = read_request(&estate).await;
    let stale = request.clone();

    let stream = estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on: based_on.clone(), nodes: naming(request, "mesh2.admin.1", Some(&next)) }).await;
    let build = stream.build_id().to_string();
    let accepted = build_log::build_value(&estate.carrier().await, &build).await;
    assert!(accepted["topology"]["meshes"]["mesh2"]["nodes"].as_array().is_some_and(|n| n.iter().any(|p| p == "mesh2.admin.2")), "the added admin is in the accepted topology from acceptance: {}", accepted["topology"]);
    door.wait_held("added-admin-created").await;
    let added = estate.until_node("mesh2.admin.2", "mesh2.admin.2 ready", SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready).cloned()).await;
    assert_eq!(estate.node_value("mesh2.admin.1").await["node_id"], json!(before["mesh2.admin.1"].node_id), "the original is not replaced before its second admin is created");
    estate.kill_node("mesh2.admin.1").await;

    followed(&estate, stream).await;
    estate.until_node("mesh2.admin.1", "mesh2.admin.1 on the new executable", SETTLE + crate::node_recovery_due(), |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready && n.node_id != before["mesh2.admin.1"].node_id && n.executable.as_ref() == Some(&next)).map(|_| ())).await;
    assert_eq!(exe_of(&estate, "mesh2.admin.1").await, next.executable, "the original's path runs the new executable");
    let read = fabric_nodes(&estate).await;
    assert_eq!(read["mesh2.admin.2"].node_id, added.node_id, "the added admin is kept: the same node");
    assert_eq!(read["mesh2.admin.2"].executable.as_ref(), Some(&next), "fabric.nodes.get lists the added admin on the new executable");
    let receipts = receipts(&estate, &build).await;
    assert!(receipts_of(&receipts, "node.delete:mesh2.admin.2", "NodeDeleted").is_empty(), "the added admin is never deleted");

    // The next request, built from the read, lists the added admin and leaves it as it is.
    let (current, again) = read_request(&estate).await;
    assert!(again.iter().any(|s| s.path.to_string() == "mesh2.admin.2" && s.executable.as_ref() == Some(&next)), "the read lists the node RDM created");
    let (reason, detail) = rejected("a request without the added admin, under the stale based_on", estate.carrier().await.create(Change::Reconcile { topology_storage_provider: None, based_on, nodes: stale }).await);
    assert_eq!(reason, "based-on-not-current", "{detail}");
    followed(&estate, estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on: current, nodes: again }).await).await;
    assert_eq!(estate.node_value("mesh2.admin.2").await["node_id"], json!(added.node_id), "the next request keeps it");
    estate.shutdown().await;
}

/// CONTRACT (proposal executable-bindings-at-create.md rule 1: "A mesh named for the first time comes into
/// being with the create of its first node-admin (node-create.md, the mesh-absent arm); a mesh absent from
/// the request is mesh.delete, the leaf-inward lifecycle with its row going Dead, and when it is the
/// fabric-primary's mesh the fabric-primary hands over to another mesh's primary first, inside that
/// mesh.delete"): a request naming mesh2 and mesh3, which do not exist, creates each with its first
/// node-admin on the executables it names. A request leaving mesh1, the fabric-primary's own mesh, out
/// moves the fabric-primary to another mesh's primary first (the caller's stream ends `FabricPrimaryMoved`
/// before any step of mesh1's mesh.delete, and the same build_id completes at the new fabric-primary),
/// then mesh1 is deleted. In that order: a request leaving out mesh3, a mesh that does not hold the
/// fabric-primary, deletes it, its row Dead; then a request leaving out mesh1 moves the fabric-primary to
/// mesh2's primary, "the other Ready mesh-primaries" (node-primary-update.md), and the new
/// fabric-primary does not plan mesh3's mesh.delete again. Must NOT happen: a mesh left out kept running,
/// the fabric-primary's mesh deleted with the fabric-primary in it, the fabric-primary handed to the Dead
/// mesh's final primary, or a deleted mesh planned again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_named_first_is_created_with_its_first_admin_and_a_mesh_left_out_is_deleted() {
    let mut estate = Estate::bootstrap(owner("a_mesh_named_first_is_created_with_its_first_admin_and_a_mesh_left_out_is_deleted"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1)]).await;
    estate.settles_to(&[("mesh1", 2, 1)], SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let (based_on, request) = read_request(&estate).await;
    let of = |name: &str| request.iter().find(|s| s.path.to_string() == name).and_then(|s| s.executable.clone()).expect("the row records its executable");
    let (admin, rpc) = (of("mesh1.admin.1"), of("mesh1.rpc.1"));
    let mut grown = request.clone();
    for (p, b) in [("mesh2.admin.1", &admin), ("mesh2.admin.2", &admin), ("mesh2.rpc.1", &rpc), ("mesh3.admin.1", &admin), ("mesh3.rpc.1", &rpc)] {
        grown = naming(grown, p, Some(b));
    }
    followed(&estate, estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: grown }).await).await;
    estate.settles_to(&[("mesh1", 2, 1), ("mesh2", 2, 1), ("mesh3", 1, 1)], SETTLE).await;
    for m in ["mesh2", "mesh3"] {
        let view = estate.mesh_view(m).await;
        assert!(view.id.is_some() && view.status == ScopeStatus::Ready, "{m} is created with its first node-admin: {view:?}");
    }

    let mesh3_id = estate.mesh_view("mesh3").await.id.expect("mesh3 carries its id");
    // mesh3, a mesh that does not hold the fabric-primary, left out: its mesh.delete, its row Dead. Its
    // final primary stays alive until its own node.delete (mesh-delete.md step 7).
    let (fp, _, _) = fabric_at_its_primary(&estate).await;
    assert!(fp.starts_with("mesh1."), "the fabric-primary is mesh1's: {fp}");
    let (based_on, request) = read_request(&estate).await;
    let without3: Vec<_> = request.into_iter().filter(|s| s.path.mesh != "mesh3").collect();
    followed(&estate, estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: without3 }).await).await;
    rdm_client::Fabrics::new(estate.carrier().await)
        .fact("mesh3 deleted", SETTLE, |f| f.meshes.iter().any(|m| m.name == "mesh3" && m.status == ScopeStatus::Dead))
        .await
        .unwrap_or_else(|end| panic!("{end}"));

    // mesh1, the fabric-primary's own mesh, left out: the fabric-primary moves first, inside mesh1's
    // mesh.delete, to the other Ready mesh-primary, mesh2's (node-primary-update.md: "the lowest
    // NodeId among the other Ready mesh-primaries"), never to the Dead mesh3's final primary.
    let mesh2_admin = estate.mesh_node_views("mesh2").await.into_iter().find(|n| n.kind == NodeKind::NodeAdmin && n.status == rdm_client::NodeStatus::Ready).map(|n| n.name).expect("a ready mesh2 admin");
    estate.enter(&mesh2_admin).await;
    let (based_on, request) = read_request(&estate).await;
    let without1: Vec<_> = request.into_iter().filter(|s| s.path.mesh != "mesh1").collect();
    let done = followed(&estate, estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: without1 }).await).await;
    let build = done.build_id.to_string();
    let moved = done.frames.iter().position(|f| matches!(f, BuildFrame::FabricPrimaryMoved { .. })).unwrap_or_else(|| panic!("the caller's stream ended FabricPrimaryMoved and the same build_id completed: {:?}", done.frames));
    let first_step = done.frames.iter().position(|f| matches!(f, BuildFrame::Step { operation, .. } if operation.starts_with("mesh.delete:") || operation.starts_with("node.delete:mesh1."))).unwrap_or_else(|| panic!("the mesh.delete's steps: {:?}", done.frames));
    assert!(moved < first_step, "the hand-over comes first: the fabric-primary moved before any step of mesh1's mesh.delete: {:?}", done.frames);
    let (fp, _, _) = fabric_at_its_primary(&estate).await;
    assert!(fp.starts_with("mesh2."), "the fabric-primary is mesh2's primary now, never the Dead mesh3's: {fp}");
    rdm_client::Fabrics::new(estate.carrier().await)
        .fact("mesh1 deleted", SETTLE, |f| f.meshes.iter().any(|m| m.name == "mesh1" && m.status == ScopeStatus::Dead))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    let steps = receipts(&estate, &build).await;
    assert!(steps.iter().any(|r| r.operation.starts_with("mesh.delete:") && r.step == rdm_admin::mesh_delete::STEP_MESH_DELETED), "mesh1's mesh.deleted");
    assert!(!steps.iter().any(|r| r.operation == format!("mesh.delete:{mesh3_id}")), "mesh3, deleted by the Build before, is not deleted again: {steps:?}");
    estate.shutdown().await;
}

/// What a node was before a request replaced it: its view and its data dir.
#[derive(Debug, Clone)]
struct Member {
    view: NodeView,
    data_dir: String,
}

async fn members(estate: &Estate) -> BTreeMap<String, Member> {
    let mut out = BTreeMap::new();
    for (name, view) in fabric_nodes(estate).await {
        let data_dir = estate.data_dir(&name).await;
        out.insert(name, Member { view, data_dir });
    }
    out
}

/// The request that names, for every path the read lists, `next` of its kind, and the executables it
/// names: each kind's current executable copied with one byte appended.
async fn every_path_on_another_build(estate: &Estate) -> (rdm_core::BuildId, Vec<NodeSpec>, BTreeMap<NodeKind, Binding>) {
    let (based_on, mut request) = read_request(estate).await;
    let mut next: BTreeMap<NodeKind, Binding> = BTreeMap::new();
    for s in request.iter_mut() {
        let current = s.executable.clone().unwrap_or_else(|| panic!("{}'s row records its executable", s.path));
        let to = next.entry(s.path.kind).or_insert_with(|| another_build(estate, &current, "next")).clone();
        s.executable = Some(to);
    }
    (based_on, request, next)
}

/// `name` at its `path.name` again, Ready as a new node (new node.id and incarnation), running
/// `next`; a `Persistent` path's successor (a node-admin's, a broker's: their kinds' default storage) over
/// its predecessor's data dir with its endpoint key, an `Ephemeral` path's over a newly allocated data dir
/// with a new endpoint key (build-deployment.md §3).
async fn replaced(estate: &Estate, name: &str, was: &Member, next: &Binding) {
    let now = estate.until_node(name, &format!("{name} ready as a new node"), SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready && n.node_id != was.view.node_id).cloned()).await;
    assert_ne!(now.incarnation_id, was.view.incarnation_id, "{name}: a new birth");
    assert_eq!(exe_of(estate, name).await, next.executable, "{name} runs the executable its entry names");
    if matches!(NodeSpec::default_for(was.view.name.clone()).storage_persistence, rdm_core::meta::StorageMeta::Persistent { .. }) {
        assert_eq!(now.endpoint_id, was.view.endpoint_id, "{name}: its endpoint key kept, loaded from its kept data dir");
        assert_eq!(estate.data_dir(name).await, was.data_dir, "{name}: created over its kept data dir");
    } else {
        assert_ne!(now.endpoint_id, was.view.endpoint_id, "{name}: an Ephemeral path's successor has a new endpoint key");
        assert_ne!(estate.data_dir(name).await, was.data_dir, "{name}: an Ephemeral path's successor is created over a newly allocated data dir");
    }
}

/// CONTRACT (build-deployment.md §3: "The successor's storage follows its create spec's `storage`: `Persistent`
/// inherits the predecessor's data dir and endpoint key, `Ephemeral` allocates a fresh one"; proposal
/// executable-bindings-at-create.md §4: "of an `Ephemeral` gateway: a fresh data dir"): a mesh of two
/// node-admins and two rpc nodes (an rpc node's path `Ephemeral`, its kind's default) has one rpc node's
/// executable replaced by a request naming another. The rpc node comes back at its `path.name` as a new
/// node.id and incarnation on the new executable, over a newly allocated data dir with a new endpoint key;
/// its predecessor's data dir is left where it was. Must NOT happen: the successor created over its
/// predecessor's data dir or loading its endpoint key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ephemeral_paths_replacement_is_created_over_a_newly_allocated_data_dir_with_a_new_endpoint_key() {
    let estate = Estate::bootstrap(owner("an_ephemeral_paths_replacement_is_created_over_a_newly_allocated_data_dir_with_a_new_endpoint_key"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.settles_to(&[("mesh1", 2, 2)], SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let before = members(&estate).await;
    let (based_on, request) = read_request(&estate).await;
    assert_eq!(request.iter().find(|s| s.path.to_string() == "mesh1.rpc.1").map(|s| s.storage_persistence), Some(rdm_core::meta::StorageMeta::Ephemeral), "mesh1.rpc.1's path is Ephemeral");
    let current = before["mesh1.rpc.1"].view.executable.clone().expect("mesh1.rpc.1's row records its executable");
    let next = another_build(&estate, &current, "next");
    followed(&estate, estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: naming(request, "mesh1.rpc.1", Some(&next)) }).await).await;
    replaced(&estate, "mesh1.rpc.1", &before["mesh1.rpc.1"], &next).await;
    assert!(std::path::Path::new(&before["mesh1.rpc.1"].data_dir).is_dir(), "the predecessor's data dir is left where it was (node-delete.md \"Storage\")");
    estate.shutdown().await;
}

/// A Build's stream followed to its terminal across the fabric-primary's own replacement: a stream that
/// ends `FabricPrimaryMoved`, or breaks, is resubmitted by its build_id at the fabric-primary a node-admin
/// of `admins` still answering names (the entry admin may be the replaced one). Every frame, and how
/// many times the stream ended `FabricPrimaryMoved`.
async fn follow_across_replacement(estate: &Estate, stream: rdm_client::BuildStream, admins: &[&str]) -> (Vec<BuildFrame>, usize) {
    let build = stream.build_id().clone();
    let followed = tokio::time::timeout(SETTLE * 3, rdm_client::follow_with(stream, |id, _from| async move {
        let deadline = tokio::time::Instant::now() + SETTLE;
        loop {
            for admin in admins {
                // The admin's node.get first: a path the Build re-created holds a birth the estate learns
                // from a view that lists it, so each pass reaches the admins born since the last.
                if estate.node_views_at(&Estate::path(admin)).await.is_none() {
                    continue;
                }
                let Some(view) = estate.fabric_view_at(&Estate::path(admin)).await else { continue };
                let Some(fp) = view.fabric_primary else { continue };
                if estate.node_views_at(&fp).await.is_none() {
                    continue;
                }
                if let Ok(again) = estate.carrier_at(&fp).await.reattach(&id).await {
                    return Ok(again);
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "no node-admin of {admins:?} named a fabric-primary that resubmitted Build {id}");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }))
    .await
    .unwrap_or_else(|_| panic!("Build {build} reached no terminal frame within {:?}", SETTLE * 3))
    .unwrap_or_else(|e| panic!("Build {build} could not be followed to its terminal: {e}"));
    match followed.frames.last() {
        Some(BuildFrame::Complete { .. }) => {}
        other => panic!("Build {build} ended {other:?}: {:?}", followed.frames),
    }
    let moves = followed.frames.iter().filter(|f| matches!(f, BuildFrame::FabricPrimaryMoved { .. })).count();
    (followed.frames, moves)
}

/// CONTRACT (build-deployment.md §3: "RDM realizes it as node.delete then node.create at the path, a new
/// node.id and incarnation, in its order with its guards: a type's non-primary members before its primary,
/// the primary's move first by node.primary.update, the fabric-primary's move ending the caller's stream
/// `FabricPrimaryMoved` and the caller resubmitting the same build_id, a type's only node replaced with no
/// update ... A Build that puts the fabric back to pending completes only after its own ready
/// state is set"; proposal executable-bindings-at-create.md §4: "a request replacing every node of a
/// two-mesh fabric: Complete only after the ready state is set (Q154)"; "the fabric-primary's
/// own replacement moving it once"; "a one-broker mesh's broker replaced: one `node.delete` then the
/// create, no refusal at any step"): a fabric of two meshes, two node-admins and one rpc node in each, and a
/// request naming another build for every path. Every node comes back at its `path.name` as a new node.id
/// and incarnation running the executable its entry names, each node-admin over its kept data dir with its
/// endpoint key. In each mesh its non-primary node-admin is deleted before its primary one, and node-admins
/// before its rpc node; each node is created once. mesh1 holds the fabric-primary: the caller's stream ends
/// `FabricPrimaryMoved` once, and the same build_id completes at the new fabric-primary, which holds the
/// fabric ready under the Build. Each mesh's only rpc node is replaced by its `node.delete`, the
/// whole stop inside it, with no standalone connections delete or mesh leave and no `no-secondary` refusal.
/// Moved from the removed operation's `a_two_mesh_fabric_rolls_every_member_onto_the_set_at_its_path_over_its_kept_data_dir`
/// and `a_mesh_with_one_admin_gains_a_second_first_and_its_only_rpc_node_is_replaced_with_no_update` (its
/// one-admin half is `a_one_admin_meshs_second_admin_is_accepted_first_and_kept_when_its_executor_is_killed`).
/// Must NOT happen: a node left on its old executable, a primary replaced before its type's other members,
/// a type's only node refused for want of a successor, or the Build complete before the fabric is
/// ready under it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_replacing_every_nodes_executable_moves_the_fabric_primary_once_and_completes_after_ready() {
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let test = "a_request_replacing_every_nodes_executable_moves_the_fabric_primary_once_and_completes_after_ready";
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 1)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let before = members(&estate).await;
    let fp_before = before.iter().find(|(_, m)| m.view.is_fabric_primary).map(|(n, _)| n.clone()).expect("a fabric-primary");
    assert!(fp_before.starts_with("mesh1."), "the fabric-primary is mesh1's: {fp_before}");
    let (based_on, request, next) = every_path_on_another_build(&estate).await;

    let stream = estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: request }).await;
    let build = stream.build_id().to_string();
    let (frames, moves) = follow_across_replacement(&estate, stream, &["mesh2.admin.1", "mesh2.admin.2", "mesh1.admin.1", "mesh1.admin.2"]).await;
    assert_eq!(moves, 1, "the stream ended FabricPrimaryMoved once, at the fabric-primary's own replacement, and the same build_id completed at the new fabric-primary: {frames:?}");
    let (fp, status, accepted) = fabric_at_its_primary(&estate).await;
    assert_eq!((status, accepted.as_deref()), (ScopeStatus::Ready, Some(build.as_str())), "Complete only once the fabric is ready under it ({fp})");

    for (name, m) in &before {
        replaced(&estate, name, m, &next[&m.view.kind]).await;
    }
    let steps = receipts(&estate, &build).await;
    for mesh in ["mesh1", "mesh2"] {
        let admin = |primary: bool| before.iter().find(|(n, m)| n.starts_with(&format!("{mesh}.admin.")) && m.view.is_primary == primary).map(|(n, _)| n.clone()).expect("an admin");
        let (secondary, primary) = (admin(false), admin(true));
        let deleted = |n: &str| steps.iter().position(|r| r.operation == format!("node.delete:{n}") && r.step == "NodeDeleted").unwrap_or_else(|| panic!("{n} is deleted: {steps:?}"));
        assert!(deleted(&secondary) < deleted(&primary), "{mesh}: its non-primary admin {secondary} is replaced before its primary {primary}");
        assert!(deleted(&primary) < deleted(&format!("{mesh}.rpc.1")), "{mesh}: node-admins before the other types");
        for n in before.keys().filter(|n| n.starts_with(&format!("{mesh}."))) {
            assert_eq!(receipts_of(&steps, &format!("node.create:{n}"), "Complete").len(), 1, "{n} is created once");
        }
        let rpc_delete: Vec<&str> = steps.iter().filter(|r| r.operation == format!("node.delete:{mesh}.rpc.1")).map(|r| r.step.as_str()).collect();
        assert!(rpc_delete.contains(&"StopNode") && rpc_delete.contains(&"TerminateRuntime"), "{mesh}: its only rpc node's node.delete composes the whole stop: {rpc_delete:?}");
    }
    assert!(!steps.iter().any(|r| r.operation.starts_with("node.connections.delete:") || r.operation.starts_with("node.mesh.leave:")), "no standalone connections delete or mesh leave precedes a delete");
    estate.stop().await;
    let spans = estate.spans();
    assert!(rdm_test::estate::named(&spans, "rdm.node_admin.build.reject.via-no-secondary").is_empty(), "no step was refused no-secondary");
    assert!(!spans.iter().any(|sp| sp.to_string().contains("cannot transfer: no secondary")), "no step failed no-secondary");
}

/// The two-mesh fabric of `role_fabric` and the request that names another build for mesh2's broker,
/// its only broker, every other entry as read; the broker's executable it names.
async fn broker_on_another_build(estate: &Estate) -> (rdm_core::BuildId, Vec<NodeSpec>, Binding) {
    let (based_on, request) = read_request(estate).await;
    let current = request.iter().find(|s| s.path.to_string() == BROKER).and_then(|s| s.executable.clone()).expect("the broker's row records its executable");
    let next = another_build(estate, &current, "next");
    (based_on, naming(request, BROKER, Some(&next)), next)
}

const BROKER: &str = "mesh2.broker.1";

/// CONTRACT (proposal executable-bindings-at-create.md rule 2: "Each attempt then computes remaining work
/// from the accepted topology, the current node rows, observed state and its receipts ... an attempt
/// interrupted halfway finishes the remaining ... the same way"; rule 5: "`Persistent` inherits the
/// predecessor's data dir and its endpoint key"): the executor of mesh2's broker's replacement, mesh2's
/// primary node-admin, is held by its fault door at the broker's `node.deleted` and killed once the
/// fabric-primary's log holds that `NodeDeleted`. The Build goes on: mesh2's other admin takes the mesh,
/// the broker is created once at its path over its kept data dir with its endpoint key on the executable
/// its entry names, and the Build holds one `NodeDeleted` and one create `Complete` for it. Moved from the
/// removed operation's `an_executor_lost_after_a_members_delete_leaves_its_create_alone_to_the_next_attempt`.
/// Must NOT happen: the broker deleted twice, created twice, or created over a new data dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_lost_after_a_replaced_brokers_delete_leaves_its_create_alone_to_the_next_attempt() {
    let estate = role_fabric("an_executor_lost_after_a_replaced_brokers_delete_leaves_its_create_alone_to_the_next_attempt").await;
    let before = members(&estate).await;
    let p = before.iter().find(|(n, m)| n.starts_with("mesh2.admin.") && m.view.is_primary).map(|(n, _)| n.clone()).expect("mesh2's primary");
    let other = before.iter().find(|(n, m)| n.starts_with("mesh2.admin.") && !m.view.is_primary).map(|(n, _)| n.clone()).expect("mesh2's other admin");
    let door = Door::at(&estate, &p).await;
    door.arm("broker-deleted", json!({"kind": "event", "event": "deleted", "node": BROKER})).await;
    let (based_on, request, next) = broker_on_another_build(&estate).await;

    let stream = estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: request }).await;
    let build = stream.build_id().to_string();
    let held = door.wait_held("broker-deleted").await;
    assert_eq!((s(&held["hit"]["build_id"]), s(&held["hit"]["operation"])), (build.clone(), format!("node.delete:{BROKER}")), "{p} is held at the broker's node.deleted: {held}");
    let fp = estate.fabric_view_at(&Estate::path(&other)).await.and_then(|v| v.fabric_primary).expect("mesh2's other admin names the fabric-primary");
    estate.node_views_at(&fp).await.expect("the fabric-primary answers node.get");
    build_log::build_fact(&estate.carrier_at(&fp).await, &build, &format!("{BROKER}'s NodeDeleted"), SETTLE, |b| b.steps.iter().any(|r| r.operation == format!("node.delete:{BROKER}") && r.step == "NodeDeleted"))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    estate.kill_node(&p).await;

    followed(&estate, stream).await;
    replaced(&estate, BROKER, &before[BROKER], &next).await;
    let steps = receipts(&estate, &build).await;
    assert_eq!(receipts_of(&steps, &format!("node.delete:{BROKER}"), "NodeDeleted").len(), 1, "{BROKER} is deleted once");
    assert_eq!(receipts_of(&steps, &format!("node.create:{BROKER}"), "Complete").len(), 1, "{BROKER} is created once");
    estate.shutdown().await;
}

/// CONTRACT (proposal executable-bindings-at-create.md rule 2, as above; rule 5: "`Persistent` inherits the
/// predecessor's data dir and its endpoint key"): the executor of mesh2's broker's replacement is held by
/// its fault door inside the broker's `NodeDeleted` step, the runtime terminated and its row written
/// Deleted but the receipt not yet durable, and killed once the fabric-primary's log holds the delete's
/// `TerminateRuntime`. The next attempt reads the row and the receipts: the broker is created once at its
/// path over the data dir that `TerminateRuntime` names, with its endpoint key, on the executable its entry
/// names, and it is not deleted again. Moved from the removed operation's
/// `an_executor_lost_inside_a_members_delete_has_its_successor_created_over_the_kept_data_dir`.
/// Must NOT happen: the successor created over a newly allocated data dir or with a new endpoint key, the
/// broker deleted again, or created twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_lost_inside_a_replaced_brokers_delete_has_its_successor_created_over_the_kept_data_dir() {
    let estate = role_fabric("an_executor_lost_inside_a_replaced_brokers_delete_has_its_successor_created_over_the_kept_data_dir").await;
    let before = members(&estate).await;
    let p = before.iter().find(|(n, m)| n.starts_with("mesh2.admin.") && m.view.is_primary).map(|(n, _)| n.clone()).expect("mesh2's primary");
    let other = before.iter().find(|(n, m)| n.starts_with("mesh2.admin.") && !m.view.is_primary).map(|(n, _)| n.clone()).expect("mesh2's other admin");
    let door = Door::at(&estate, &p).await;
    door.arm("broker-deleted-row", json!({"kind": "receipt", "step": "NodeDeleted", "operation": format!("node.delete:{BROKER}")})).await;
    let (based_on, request, next) = broker_on_another_build(&estate).await;

    let stream = estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: request }).await;
    let build = stream.build_id().to_string();
    let held = door.wait_held("broker-deleted-row").await;
    assert_eq!((s(&held["hit"]["build_id"]), s(&held["hit"]["operation"])), (build.clone(), format!("node.delete:{BROKER}")), "{p} is held inside the broker's NodeDeleted step: {held}");
    let fp = estate.fabric_view_at(&Estate::path(&other)).await.and_then(|v| v.fabric_primary).expect("mesh2's other admin names the fabric-primary");
    estate.node_views_at(&fp).await.expect("the fabric-primary answers node.get");
    build_log::build_fact(&estate.carrier_at(&fp).await, &build, &format!("{BROKER}'s TerminateRuntime"), SETTLE, |b| b.steps.iter().any(|r| r.operation == format!("node.delete:{BROKER}") && r.step == "TerminateRuntime"))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    let record = record_of(&estate, before[BROKER].view.node_id.as_str());
    assert_eq!(record["record"]["status"], "deleted", "{BROKER}'s row is written Deleted inside the held step: {record}");
    estate.kill_node(&p).await;

    // The successor is read first: one on a new data dir fails `replaced`.
    replaced(&estate, BROKER, &before[BROKER], &next).await;
    followed(&estate, stream).await;
    let steps = receipts(&estate, &build).await;
    assert_eq!(receipts_of(&steps, &format!("node.delete:{BROKER}"), "NodeDeleting").len(), 1, "{BROKER} is deleted once: the next attempt does not delete it again");
    assert_eq!(receipts_of(&steps, &format!("node.create:{BROKER}"), "Complete").len(), 1, "{BROKER} is created once");
    estate.shutdown().await;
}

/// CONTRACT (proposal executable-bindings-at-create.md §2: "A containerized admin's executable directories
/// become its launch configuration, `RDM_EXECUTABLE_DIRS`, comma-separated host directories mounted
/// read-only at their host paths ...; an executable outside them is refused by name at acceptance. The
/// bootstrap admin reads it from its environment; the launching admin writes its own value into every
/// node-admin launch"; §4: "a containerized admin launched by another admin mounts the launching admin's
/// `RDM_EXECUTABLE_DIRS`"; "a container fabric: an executable outside the admin's configured directories
/// refused by name at acceptance; one inside, from a directory absent from day 0's input, launched";
/// deployment-provider.md §2.7 "Executables"): a container fabric whose bootstrap admin is given two
/// directories, the binaries' and an empty one. Every node-admin another admin launched mounts both at
/// their host paths, read-only, a write inside them refused. A request creating a node on an executable
/// outside them is refused `executable-refused` naming `RDM_EXECUTABLE_DIRS`, nothing accepted; one on
/// an executable copied into the second directory is accepted, and mesh2's admin, a launched one,
/// launches it: the node's row records it. Moved from the removed operation's
/// `a_container_fabrics_replacement_admin_launches_from_the_builds_set_and_reads_it_read_only`. Opt-in:
/// the container-proof step (`RDM_CONTAINER_PROOF=1`, or `RDM_REQUIRE_CONTAINER=1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_container_fabrics_launched_admins_mount_the_executable_dirs_and_an_executable_outside_them_is_refused() {
    if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
        eprintln!("SKIP container executable directories: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
        return;
    }
    let test = "a_container_fabrics_launched_admins_mount_the_executable_dirs_and_an_executable_outside_them_is_refused";
    let sha = candidate_sha();
    let mut set = binding_set(&sha);
    for b in set.bindings.iter_mut() {
        b.image = Some(rdm_test::estate::RUNTIME_IMAGE.into());
    }
    let bin = set.bindings[0].executable.parent().expect("the binaries' directory").to_path_buf();
    let added = std::env::temp_dir().join(format!("executable-dirs-{}-{}", std::process::id(), rdm_core::NodeId::mint()));
    std::fs::create_dir_all(&added).unwrap();
    let dirs = format!("{},{}", bin.display(), added.display());
    let estate = Estate::bootstrap_on_with_env(Owner { provider: "container".into(), ..owner(test) }, "fabric1", "mesh1", &set, &sha, &["rpc_node"], &[("RDM_EXECUTABLE_DIRS", dirs.as_str())]).await.expect("the container set is accepted");
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 0)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    for name in ["mesh1.admin.2", "mesh2.admin.1", "mesh2.admin.2"] {
        let id = estate.container_of(name).await.unwrap_or_else(|| panic!("{name} runs in a container"));
        let mounts = std::process::Command::new("docker").args(["inspect", "--format", "{{json .Mounts}}", &id]).output().unwrap();
        let mounts: Value = serde_json::from_slice(&mounts.stdout).unwrap();
        for dir in [&bin, &added] {
            let held = mounts.as_array().unwrap().iter().find(|m| m["Source"] == json!(dir.display().to_string())).unwrap_or_else(|| panic!("{name} mounts {}: {mounts}", dir.display()));
            assert_eq!((held["Destination"].clone(), held["RW"].clone()), (json!(dir.display().to_string()), json!(false)), "{name}: at its host path, read-only: {held}");
            let wrote = std::process::Command::new("docker").args(["exec", &id, "/bin/sh", "-c", &format!("echo x > {}/written", dir.display())]).output();
            assert!(wrote.map(|o| !o.status.success()).unwrap_or(true), "{name}: a write inside {} is refused", dir.display());
        }
    }
    let rpc = set.bindings.iter().find(|b| b.launch_id == "rpc_node").cloned().expect("the set binds rpc_node");
    let outside = another_build(&estate, &rpc, "outside");
    let (based_on, request) = read_request(&estate).await;
    let (reason, detail) = rejected("an executable outside RDM_EXECUTABLE_DIRS", estate.carrier().await.create(Change::Reconcile { topology_storage_provider: None, based_on: based_on.clone(), nodes: naming(request.clone(), "mesh2.rpc.1", Some(&outside)) }).await);
    assert_eq!(reason, "executable-refused", "{detail}");
    assert!(detail.contains("mesh2.rpc.1") && detail.contains("RDM_EXECUTABLE_DIRS"), "the refusal names the path and the directories: {detail}");
    assert_eq!(estate.accepted_build_id().await, based_on.to_string(), "nothing is accepted");
    let inside_path = added.join("rpc-node-inside");
    std::fs::copy(&outside.executable, &inside_path).unwrap();
    let inside = Binding { executable: inside_path, ..outside.clone() };
    followed(&estate, estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: naming(request, "mesh2.rpc.1", Some(&inside)) }).await).await;
    let created = estate.until_node("mesh2.rpc.1", "mesh2.rpc.1 ready", SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready).cloned()).await;
    let row = record_of(&estate, created.node_id.as_str());
    assert_eq!(row["record"]["executable"]["executable"], json!(inside.executable.display().to_string()), "mesh2's launched admin launched the executable from the added directory: {row}");
    estate.shutdown().await;
    let _ = std::fs::remove_dir_all(&added);
}

/// CONTRACT (build.md §4.4, closing rule: "The operation carries the exact birth the gates pinned (node id
/// and incarnation). The mesh-primary that runs it verifies that birth again against its own view at
/// execution, so a birth replaced between acceptance and execution is refused there, by name"; Q218; Q228,
/// decided by Q218 and Q221: a replacement's delete of a birth the Reconcile's acceptance saw carries the
/// acceptance pin): a two-mesh fabric's request changes its gateways' executables. mesh1's replacement is
/// held by its executor's fault door at its first gateway's create `Complete` (the attempt holds mesh1
/// alone). Meanwhile mesh2's non-primary gateway, pinned at acceptance, is killed and its own
/// mesh-primary recovers it at the same `path.name` under a new node id. Released, the Build reaches
/// mesh2: the gateway's replacement delete carries the birth the acceptance pinned, and mesh2's primary
/// refuses it `birth-replaced` (span `rdm.node_admin.node.reject.via-birth-replaced`, naming the pinned
/// and the observed birth). The recovered birth is untouched: the same node id and incarnation,
/// Ready, on the executable it was recovered on. Must NOT happen: the delete resolving the
/// birth now at the path and replacing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacements_delete_of_a_birth_replaced_after_acceptance_is_refused_birth_replaced_and_the_new_birth_is_untouched() {
    let estate = role_fabric("a_replacements_delete_of_a_birth_replaced_after_acceptance_is_refused_birth_replaced_and_the_new_birth_is_untouched").await;
    let before = fabric_nodes(&estate).await;
    let p1 = before.values().find(|v| v.mesh == "mesh1" && v.kind == NodeKind::NodeAdmin && v.is_primary).map(|v| v.name.to_string()).expect("mesh1's primary admin");
    let first = before.values().find(|v| v.mesh == "mesh1" && v.kind == NodeKind::Gateway && !v.is_primary).map(|v| v.name.to_string()).expect("mesh1's non-primary gateway");
    let target = before.values().find(|v| v.mesh == "mesh2" && v.kind == NodeKind::Gateway && !v.is_primary).map(|v| v.name.to_string()).expect("mesh2's non-primary gateway");
    let pinned = before[&target].clone();
    let door = Door::at(&estate, &p1).await;
    door.arm("mesh1-gateway-created", json!({"kind": "receipt", "step": "Complete", "operation": format!("node.create:{first}")})).await;
    let gateway = pinned.executable.clone().expect("a gateway's row records its executable");
    let next = another_build(&estate, &gateway, "next");
    let (based_on, request) = gateways_on(&estate, &next).await;

    // Accepted with every gateway's birth pinned; mesh1's replacement held at its first create's Complete.
    let stream = estate.submit_change(Change::Reconcile { topology_storage_provider: None, based_on, nodes: request }).await;
    let build = stream.build_id().to_string();
    let held = door.wait_held("mesh1-gateway-created").await;
    assert_eq!(s(&held["hit"]["build_id"]), build, "{p1} is held inside the Build's mesh1 replacement: {held}");
    assert_eq!(estate.node_value(&target).await["node_id"], json!(pinned.node_id), "{target} is the birth the acceptance pinned, not replaced yet");

    // The pinned birth is replaced outside the Build: killed, and recovered by mesh2's own primary.
    estate.kill_node(&target).await;
    let recovered = estate.until_node(&target, &format!("{target} recovered under a new node id"), SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready && n.node_id != pinned.node_id).cloned()).await;
    door.release("mesh1-gateway-created").await;

    // The Build reaches mesh2: the replacement's delete carries the acceptance pin and is refused there.
    let refused = rdm_test::estate::wait_for("the replacement's delete refused birth-replaced", SETTLE, || {
        let (spans, at) = (estate.spans(), target.clone());
        async move { spans.into_iter().find(|sp| sp["name"] == "rdm.node_admin.node.reject.via-birth-replaced" && s(&sp["attributes"]["node"]) == at) }
    })
    .await;
    let a = &refused["attributes"];
    assert_eq!((s(&a["operation"]), s(&a["node_id"]), s(&a["incarnation_id"])), (format!("node.delete:{target}"), pinned.node_id.to_string(), pinned.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default()), "the delete names the birth the acceptance pinned: {refused}");
    assert_eq!((s(&a["observed_node_id"]), s(&a["observed_incarnation_id"])), (recovered.node_id.to_string(), recovered.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default()), "and the birth mesh2's primary holds: {refused}");
    eprintln!("span rdm.node_admin.node.reject.via-birth-replaced trace_id={} service={} {a}", s(&refused["trace_id"]), s(&refused["service"]));

    // The recovered birth is untouched by the Build.
    let now = estate.until_node(&target, &format!("{target} as recovered"), SETTLE, |n| n.cloned()).await;
    assert_eq!((&now.node_id, &now.incarnation_id, now.status), (&recovered.node_id, &recovered.incarnation_id, rdm_client::NodeStatus::Ready), "{target}: the recovered birth stands");
    assert_eq!(exe_of(&estate, &target).await, gateway.executable, "{target} runs the executable it was recovered on");
    let steps = receipts(&estate, &build).await;
    assert!(receipts_of(&steps, &format!("node.delete:{target}"), "NodeDeleting").is_empty(), "the Build began no delete of {target}: {:?}", receipts_of(&steps, &format!("node.delete:{target}"), "NodeDeleting"));
    estate.shutdown().await;
}
