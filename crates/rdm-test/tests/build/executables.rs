//! product=mesh, feature=build, subfeature=executables, rung=MM, provider=process.
//!
//! The executable a node runs, named per node at create (proposal executable-bindings-at-create.md, Q157):
//! `build.create { Change(Reconcile { based_on, nodes }) }` submits the complete topology, every node by
//! its `path.name` with its config, the executable included. A node left out is removed, one added is
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
use rdm_core::meta::PathConfig;
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
/// config, the executable its entry names, else the one `fabric.nodes.get` reads from the path's row.
pub async fn read_request(estate: &Estate) -> (rdm_core::BuildId, Vec<(PathName, PathConfig)>) {
    let based_on = estate.accepted_build_id().await;
    let carrier = estate.carrier().await;
    let facts = build_log::log_read(&carrier, &based_on).await.unwrap_or_else(|e| panic!("the Build log of the fabric-primary holds no Build {based_on}: {e}"));
    let build = build_log::projection(&facts, &based_on).unwrap_or_else(|| panic!("the fabric-primary's log holds no acceptance of {based_on}"));
    let nodes = estate.fabrics_obj().await.nodes().await.unwrap_or_else(|e| panic!("fabric.nodes.get: {e}"));
    let mut out = Vec::new();
    for m in build.topology.meshes.values() {
        for p in &m.nodes {
            let mut c = m.path_config(p).cloned().unwrap_or_else(|| panic!("{p} carries its config"));
            if c.executable.is_none() {
                c.executable = nodes.iter().find(|n| &n.name == p).and_then(|n| n.executable.clone());
            }
            out.push((p.clone(), c));
        }
    }
    (rdm_core::BuildId::parse(&based_on).expect("a Build id"), out)
}

/// `request` with the entry at `path` (added when it lists none) on `executable`.
fn naming(mut request: Vec<(PathName, PathConfig)>, path: &str, executable: Option<&Binding>) -> Vec<(PathName, PathConfig)> {
    let p: PathName = path.parse().unwrap();
    match request.iter_mut().find(|(q, _)| *q == p) {
        Some((_, c)) => c.executable = executable.cloned(),
        None => {
            let mut c = PathConfig::default_for(p.kind);
            c.executable = executable.cloned();
            request.push((p, c));
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

/// The Node record of `node_id` in `mesh` on the estate's store root, as written.
fn record_of(estate: &Estate, mesh: &str, node_id: &str) -> Value {
    let file = estate.fabric_store_root().join(format!("nodes/{mesh}/records/{node_id}.json"));
    serde_json::from_slice(&std::fs::read(&file).unwrap_or_else(|e| panic!("{}: {e}", file.display()))).unwrap()
}

/// Every node `fabric.nodes.get` answers, by `path.name`.
async fn fabric_nodes(estate: &Estate) -> BTreeMap<String, NodeView> {
    estate.fabrics_obj().await.nodes().await.unwrap_or_else(|e| panic!("fabric.nodes.get: {e}")).into_iter().map(|n| (n.name.to_string(), n)).collect()
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
    estate.carrier().await.get(&rdm_client::BuildId::parse(build).unwrap()).await.unwrap_or_else(|e| panic!("build.get {build}: {e}")).steps
}

/// The fabric-primary now, the fabric's status as it holds it, and the Build whose topology it accepted.
async fn fabric_at_its_primary(estate: &Estate) -> (String, ScopeStatus, Option<String>) {
    let fp = estate.node_views().await.into_iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    let view = rdm_client::Fabrics::new(estate.carrier_at(&Estate::path(&fp)).await).get().await.unwrap_or_else(|e| panic!("fabric.get at {fp}: {e}"));
    (fp, view.status, view.build_id)
}

/// CONTRACT (proposal executable-bindings-at-create.md rule 1, "The request shape": "one field for the whole
/// request, `based_on`: the build_id of the current Build the caller read. RDM refuses the request by name
/// when that Build is no longer current, nothing accepted, and the caller re-reads"; "a request that ...
/// lists no nodes at all, is refused by name pointing to fabric.delete"; "a created path whose config names
/// no executable is refused by name at acceptance, nothing runs"; rule 3, "Recorded on the row at the claim";
/// rule 7, read from the records). Two callers read the same topology. B's request adds `mesh1.rpc.2` on a
/// new executable with the tag `zone_a` and completes: the node runs that executable, its row records it
/// and the tag, and `fabric.nodes.get` returns both. A's request, built from the earlier read, is refused `based-on-not-current`, nothing
/// accepted and `mesh1.rpc.2` untouched. A re-reads: a request listing no node is refused `no-node-listed`
/// naming fabric.delete, one creating `mesh1.rpc.3` with no executable `executable-missing`, nothing
/// accepted; the re-read request itself is accepted and completes, changing nothing.
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
    let rpc = b_request.iter().find(|(p, _)| p.to_string() == "mesh1.rpc.1").and_then(|(_, c)| c.executable.clone()).expect("mesh1.rpc.1's row records its executable");
    let next = another_build(&estate, &rpc, "next");

    // B: mesh1.rpc.2, named on the new executable with its tags.
    let mut b_nodes = naming(b_request, "mesh1.rpc.2", Some(&next));
    b_nodes.iter_mut().find(|(p, _)| p.to_string() == "mesh1.rpc.2").expect("the entry named").1.tags = ["zone_a".to_string()].into();
    let b = followed(&estate, estate.submit_change(Change::Reconcile { based_on: b_based_on, nodes: b_nodes }).await).await.build_id.to_string();
    let created = estate.until_node("mesh1.rpc.2", "mesh1.rpc.2 ready", SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::ReadyForTraffic).cloned()).await;
    assert_eq!(exe_of(&estate, "mesh1.rpc.2").await, next.executable, "the created node runs the executable its request named");
    let row = record_of(&estate, "mesh1", created.node_id.as_str());
    assert_eq!(row["record"]["executable"]["sha256"], json!(next.sha256), "its row records it: {row}");
    let read = fabric_nodes(&estate).await;
    assert_eq!(read["mesh1.rpc.2"].executable.as_ref(), Some(&next), "fabric.nodes.get returns it from the row");
    assert_eq!(read["mesh1.rpc.2"].tags, ["zone_a".to_string()].into(), "the node carries the tags its entry named, from its claim");
    assert_eq!(read["mesh1.rpc.1"].executable.as_ref(), Some(&rpc), "and every other node's");

    // A, from its earlier read: refused, nothing accepted, mesh1.rpc.2 untouched.
    let carrier = estate.carrier().await;
    let (reason, detail) = rejected("a request based on the Build before B's", carrier.create(Change::Reconcile { based_on: a_based_on.clone(), nodes: a_request }).await);
    assert_eq!(reason, "based-on-not-current", "{detail}");
    assert!(detail.contains(&a_based_on.to_string()) && detail.contains(&b), "the detail names both Builds: {detail}");
    assert_eq!(estate.accepted_build_id().await, b, "nothing is accepted");

    // A re-reads.
    let (based_on, request) = read_request(&estate).await;
    assert_eq!(based_on.to_string(), b);
    assert!(request.iter().any(|(p, _)| p.to_string() == "mesh1.rpc.2"), "the read holds what B added");
    let (reason, detail) = rejected("a request listing no node", carrier.create(Change::Reconcile { based_on: based_on.clone(), nodes: vec![] }).await);
    assert_eq!(reason, "no-node-listed", "{detail}");
    assert!(detail.contains("fabric.delete"), "the refusal names fabric.delete: {detail}");
    let (reason, detail) = rejected("a created path naming no executable", carrier.create(Change::Reconcile { based_on: based_on.clone(), nodes: naming(request.clone(), "mesh1.rpc.3", None) }).await);
    assert_eq!(reason, "executable-missing", "{detail}");
    assert!(detail.contains("mesh1.rpc.3"), "{detail}");
    assert_eq!(estate.accepted_build_id().await, b, "nothing is accepted");
    assert!(estate.node_value_opt("mesh1.rpc.3").await.is_none(), "nothing runs at mesh1.rpc.3");

    followed(&estate, estate.submit_change(Change::Reconcile { based_on, nodes: request }).await).await;
    assert_eq!(estate.node_value("mesh1.rpc.2").await["node_id"], json!(created.node_id), "the re-read request changes nothing");
    estate.shutdown().await;
}

/// A two-mesh fabric of the faulted node-admin with the role kinds bound, each mesh two node-admins, a
/// broker, two gateways and a compute node, ready for traffic.
async fn role_fabric(test: &str) -> Estate {
    let sha = candidate_sha();
    let estate = Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &binding_set_with_roles(&sha), &sha, &["broker", "gateway", "compute"], &[]).await.expect("the faulted-admin binding set with the role kinds is accepted");
    let mesh = |m: &str| MeshDesired::of(m, [(NodeKind::NodeAdmin, 2), (NodeKind::Broker, 1), (NodeKind::Gateway, 2), (NodeKind::Compute, 1)]);
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await;
    let want: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| ["admin.1", "admin.2", "broker.1", "gateway.1", "gateway.2", "compute.1"].map(|n| format!("{m}.{n}"))).collect();
    estate.settles_to_named(&want, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    estate
}

/// The request that names `next` for every gateway and leaves every other entry as read.
async fn gateways_on(estate: &Estate, next: &Binding) -> (rdm_core::BuildId, Vec<(PathName, PathConfig)>) {
    let (based_on, mut request) = read_request(estate).await;
    for (p, c) in request.iter_mut().filter(|(p, _)| p.kind == NodeKind::Gateway) {
        assert!(c.executable.is_some(), "{p}'s row records its executable");
        c.executable = Some(next.clone());
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
        assert_eq!(n.status, rdm_client::NodeStatus::ReadyForTraffic, "{name}");
        let touched: Vec<String> = steps.iter().filter(|r| r.operation.ends_with(&format!(":{name}"))).map(|r| format!("{} {}", r.operation, r.step)).collect();
        assert!(touched.is_empty(), "{name}: no operation of the Build acts on it: {touched:?}");
    }
}

/// CONTRACT (proposal executable-bindings-at-create.md rule 2: "A live node whose row already records its
/// entry's config and executable is not deleted, created or restarted because another node changes. A
/// request that changes only the gateways' executables redeploys the gateways and leaves every broker,
/// compute and admin running as it was"; rule 5: "a type's non-primary members before its primary"; Q154:
/// the Build is Complete only once the fabric is ready-for-traffic again from it): a two-mesh fabric's request changes
/// only its gateways' executables. Every gateway comes back at its `path.name` as a new node.id and
/// incarnation running the new executable, deleted once and created once; in each mesh its non-primary
/// gateway before its primary. Every broker, compute node and node-admin is the same birth, and no
/// operation of the Build acts on it. At `Complete` the fabric-primary holds the fabric ready-for-traffic
/// under the Build. Must NOT happen: an untouched node stopped, restarted or re-created, or a gateway left on
/// its old executable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_changing_only_the_gateways_executables_replaces_every_gateway_and_nothing_else() {
    let estate = role_fabric("a_request_changing_only_the_gateways_executables_replaces_every_gateway_and_nothing_else").await;
    let before = fabric_nodes(&estate).await;
    let gateway = before["mesh1.gateway.1"].executable.clone().expect("a gateway's row records its executable");
    let next = another_build(&estate, &gateway, "next");
    let (based_on, request) = gateways_on(&estate, &next).await;
    let build = followed(&estate, estate.submit_change(Change::Reconcile { based_on, nodes: request }).await).await.build_id.to_string();
    let (fp, status, accepted) = fabric_at_its_primary(&estate).await;
    assert_eq!((status, accepted.as_deref()), (ScopeStatus::ReadyForTraffic, Some(build.as_str())), "Complete only once the fabric is ready-for-traffic again under it ({fp})");

    let steps = receipts(&estate, &build).await;
    for (name, v) in before.iter().filter(|(_, v)| v.kind == NodeKind::Gateway) {
        let now = estate.until_node(name, &format!("{name} ready as a new node"), SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::ReadyForTraffic && n.node_id != v.node_id).cloned()).await;
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
/// gateway's create (the successor ReadyForTraffic, its receipt not written) and killed. The next attempt
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

    let stream = estate.submit_change(Change::Reconcile { based_on, nodes: request }).await;
    let build = stream.build_id().to_string();
    let held = door.wait_held("gateway-created").await;
    assert_eq!((s(&held["hit"]["build_id"]), s(&held["hit"]["operation"])), (build.clone(), format!("node.create:{first}")), "{p} is held at {first}'s create Complete: {held}");
    let successor = estate.until_node(&first, &format!("{first}'s successor ready"), SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::ReadyForTraffic && n.node_id != before[&first].node_id).cloned()).await;
    let left: Vec<String> = before.values().filter(|v| v.kind == NodeKind::Gateway && v.name.to_string() != first && v.mesh == "mesh2").map(|v| v.name.to_string()).collect();
    assert_eq!(left.len(), 1);
    assert_eq!(estate.node_value(&left[0]).await["node_id"], json!(before[&left[0]].node_id), "halfway: mesh2's other gateway is not replaced yet");
    estate.kill_node(&p).await;

    followed(&estate, stream).await;
    let steps = receipts(&estate, &build).await;
    for (name, v) in before.iter().filter(|(_, v)| v.kind == NodeKind::Gateway) {
        estate.until_node(name, &format!("{name} ready as a new node"), SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::ReadyForTraffic && n.node_id != v.node_id).map(|_| ())).await;
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
/// it is created, ReadyForTraffic and mesh-primary ... before the original is replaced, and stays"; rule 1:
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
    let estate = Estate::bootstrap_external(owner("a_one_admin_meshs_second_admin_is_accepted_first_and_kept_when_its_executor_is_killed"), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
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

    let stream = estate.submit_change(Change::Reconcile { based_on: based_on.clone(), nodes: naming(request, "mesh2.admin.1", Some(&next)) }).await;
    let build = stream.build_id().to_string();
    let accepted = build_log::build_value(&estate.carrier().await, &build).await;
    assert!(accepted["topology"]["meshes"]["mesh2"]["nodes"].as_array().is_some_and(|n| n.iter().any(|p| p == "mesh2.admin.2")), "the added admin is in the accepted topology from acceptance: {}", accepted["topology"]);
    door.wait_held("added-admin-created").await;
    let added = estate.until_node("mesh2.admin.2", "mesh2.admin.2 ready", SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::ReadyForTraffic).cloned()).await;
    assert_eq!(estate.node_value("mesh2.admin.1").await["node_id"], json!(before["mesh2.admin.1"].node_id), "the original is not replaced before its second admin is created");
    estate.kill_node("mesh2.admin.1").await;

    followed(&estate, stream).await;
    estate.until_node("mesh2.admin.1", "mesh2.admin.1 on the new executable", SETTLE + crate::node_recovery_due(), |n| n.filter(|n| n.status == rdm_client::NodeStatus::ReadyForTraffic && n.node_id != before["mesh2.admin.1"].node_id && n.executable.as_ref() == Some(&next)).map(|_| ())).await;
    assert_eq!(exe_of(&estate, "mesh2.admin.1").await, next.executable, "the original's path runs the new executable");
    let read = fabric_nodes(&estate).await;
    assert_eq!(read["mesh2.admin.2"].node_id, added.node_id, "the added admin is kept: the same node");
    assert_eq!(read["mesh2.admin.2"].executable.as_ref(), Some(&next), "fabric.nodes.get lists the added admin on the new executable");
    let receipts = receipts(&estate, &build).await;
    assert!(receipts_of(&receipts, "node.delete:mesh2.admin.2", "NodeDeleted").is_empty(), "the added admin is never deleted");

    // The next request, built from the read, lists the added admin and leaves it as it is.
    let (current, again) = read_request(&estate).await;
    assert!(again.iter().any(|(p, c)| p.to_string() == "mesh2.admin.2" && c.executable.as_ref() == Some(&next)), "the read lists the node RDM created");
    let (reason, detail) = rejected("a request without the added admin, under the stale based_on", estate.carrier().await.create(Change::Reconcile { based_on, nodes: stale }).await);
    assert_eq!(reason, "based-on-not-current", "{detail}");
    followed(&estate, estate.submit_change(Change::Reconcile { based_on: current, nodes: again }).await).await;
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
/// mesh2's primary, "the other ReadyForTraffic mesh-primaries" (node-primary-update.md), and the new
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
    let of = |name: &str| request.iter().find(|(p, _)| p.to_string() == name).and_then(|(_, c)| c.executable.clone()).expect("the row records its executable");
    let (admin, rpc) = (of("mesh1.admin.1"), of("mesh1.rpc.1"));
    let mut grown = request.clone();
    for (p, b) in [("mesh2.admin.1", &admin), ("mesh2.admin.2", &admin), ("mesh2.rpc.1", &rpc), ("mesh3.admin.1", &admin), ("mesh3.rpc.1", &rpc)] {
        grown = naming(grown, p, Some(b));
    }
    followed(&estate, estate.submit_change(Change::Reconcile { based_on, nodes: grown }).await).await;
    estate.settles_to(&[("mesh1", 2, 1), ("mesh2", 2, 1), ("mesh3", 1, 1)], SETTLE).await;
    for m in ["mesh2", "mesh3"] {
        let view = estate.mesh_view(m).await;
        assert!(view.id.is_some() && view.status == ScopeStatus::ReadyForTraffic, "{m} is created with its first node-admin: {view:?}");
    }

    let mesh3_id = estate.mesh_view("mesh3").await.id.expect("mesh3 carries its id");
    // mesh3, a mesh that does not hold the fabric-primary, left out: its mesh.delete, its row Dead. Its
    // final primary stays alive until its own node.delete (mesh-delete.md step 7).
    let (fp, _, _) = fabric_at_its_primary(&estate).await;
    assert!(fp.starts_with("mesh1."), "the fabric-primary is mesh1's: {fp}");
    let (based_on, request) = read_request(&estate).await;
    let without3: Vec<_> = request.into_iter().filter(|(p, _)| p.mesh != "mesh3").collect();
    followed(&estate, estate.submit_change(Change::Reconcile { based_on, nodes: without3 }).await).await;
    rdm_client::Fabrics::new(estate.carrier().await)
        .fact("mesh3 deleted", SETTLE, |f| f.meshes.iter().any(|m| m.name == "mesh3" && m.status == ScopeStatus::Dead))
        .await
        .unwrap_or_else(|end| panic!("{end}"));

    // mesh1, the fabric-primary's own mesh, left out: the fabric-primary moves first, inside mesh1's
    // mesh.delete, to the other ReadyForTraffic mesh-primary, mesh2's (node-primary-update.md: "the lowest
    // NodeId among the other ReadyForTraffic mesh-primaries"), never to the Dead mesh3's final primary.
    let mesh2_admin = estate.mesh_node_views("mesh2").await.into_iter().find(|n| n.kind == NodeKind::NodeAdmin && n.status == rdm_client::NodeStatus::ReadyForTraffic).map(|n| n.name).expect("a ready mesh2 admin");
    estate.enter(&mesh2_admin).await;
    let (based_on, request) = read_request(&estate).await;
    let without1: Vec<_> = request.into_iter().filter(|(p, _)| p.mesh != "mesh1").collect();
    let done = followed(&estate, estate.submit_change(Change::Reconcile { based_on, nodes: without1 }).await).await;
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
