//! Node investigation and node recovery of a lost mesh-primary, run by the fabric-primary
//! (docs/architecture/lifecycles/node-investigation.md, docs/architecture/lifecycles/node-recovery.md;
//! Q335: a mesh-primary is a node, one investigation and one recovery), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2803-detect-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans, feature `i143-2803-detect`).
//!
//! The estate: meshes settled through a Build. The peer mesh is the one whose admins do not hold the
//! fabric-primary role. The election names the lost mesh-primary's successor at the staleness floor; the
//! fabric-primary runs node investigation on the lost birth without the half-floor wait; `reached` is
//! node.mesh.join, any other verdict node recovery at its path.name, the successor its executor, and with
//! no successor the same with none named (node-recovery.md step 2).

use rdm_core::PathName;
use rdm_test::estate::location;
use rdm_client::{NodeView, ScopeStatus};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;


fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2803-detect".into(),
        subfeature: "peer-mesh-investigation".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2803-detect/process").join(cell),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

fn at_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// The views as the JSON the cell records.
fn values(views: &[NodeView]) -> Vec<Value> {
    views.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect()
}

/// The fabric's lifecycle state as `fabric.get` words it (`pending`, `ready`).
fn status_word(status: ScopeStatus) -> String {
    s(&serde_json::to_value(status).expect("a status serializes"))
}

/// Whether a checklist's `missing` names the birth at `at` whose node id is `node_id`.
fn names_missing(missing: &str, at: &PathName, node_id: &str) -> bool {
    missing.contains(&format!("node_id={node_id}")) || missing.contains(&format!("path={:?}, name={:?}", at.path, at.name))
}

/// A settled estate and the peer mesh the fault will fall on.
struct Fixture {
    estate: Estate,
    fabric_primary: PathName,
    fabric_primary_id: String,
    lost: String,
    before: Vec<Value>,
}

async fn fixture(cell: &str) -> Fixture {
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = [("mesh1", 2, 3), ("mesh2", 2, 3)];
    estate.resize_shape("fabric1", &shape).await;
    let before = values(&estate.settled_views(&shape).await);
    let fabric_primary_row = before.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("one fabric primary");
    let (fabric_primary, fabric_primary_id) = (location(&fabric_primary_row), s(&fabric_primary_row["node_id"]));
    let lost = if fabric_primary.path.starts_with("mesh1.") { "mesh2" } else { "mesh1" }.to_string();
    // Control goes through the fabric primary's advertised API, which no fault here touches.
    estate.enter(&fabric_primary).await;
    // The fault falls on an established fabric, so the cells that assert the fabric stays ready start
    // from ready.
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    // The fault falls on a mesh whose status is published: the fabric is ready at the
    // fabric-primary's apply of the mesh's ready, the moment the mesh's primary sends it (states.md), so
    // the fixture waits for that send and its first repeat in the primary's own spans.
    let primary = before.iter().find(|n| n["mesh"] == lost.as_str() && n["type"] == "node-admin" && n["is_primary"] == true).map(|n| s(&n["node_id"])).expect("the lost mesh has a primary node-admin");
    wait_for("the lost mesh's primary has sent its ready status and repeated it", Duration::from_secs(60), || {
        let spans = estate.spans();
        let sends = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| attr(sp, "scope") == format!("mesh:{lost}") && attr(sp, "node") == primary && attr(sp, "status") == "ready").count();
        async move { (sends >= 2).then_some(()) }
    })
    .await;
    Fixture { estate, fabric_primary, fabric_primary_id, lost, before }
}

impl Fixture {
    /// The runtimes of the settled nodes at `path`, by location: a runtime's data dir is named by its
    /// birth's node id.
    fn runtimes(&self, path: &str) -> Vec<(PathName, u32)> {
        let at: BTreeMap<String, PathName> = self.before.iter().map(|n| (s(&n["node_id"]), location(n))).filter(|(_, l)| l.path == path).collect();
        let mut out: Vec<(PathName, u32)> = self.estate.live_runtimes().into_iter().filter_map(|(p, pid)| at.get(p.file_name().unwrap().to_string_lossy().as_ref()).map(|l| (l.clone(), pid))).collect();
        // The bootstrap admin is the mesh1 admin.1 and has no provider deployment.json.
        if path == "mesh1.node-admin" {
            if let Some(pid) = self.estate.bootstrap_pid() {
                out.push((Estate::path("mesh1.node-admin", "1"), pid));
            }
        }
        out
    }

    /// The node id settled at `at`.
    fn id_of(&self, at: &PathName) -> String {
        self.before.iter().find(|n| location(n) == *at).map(|n| s(&n["node_id"])).unwrap_or_else(|| panic!("{at:?} is settled"))
    }

    /// The node ids of `mesh`'s node-admins, settled and as the view holds them now.
    async fn admin_ids(&self, mesh: &str) -> std::collections::BTreeSet<String> {
        let now = self.estate.node_values().await;
        self.before.iter().chain(now.iter()).filter(|n| n["mesh"] == mesh && n["type"] == "node-admin").map(|n| s(&n["node_id"])).collect()
    }
}

/// The changed_at of the first MeshStatus `ready` of `mesh` sent before `before_ms`.
fn mesh_status_change_before(spans: &[Value], mesh: &str, before_ms: u64) -> Option<String> {
    let scope = format!("mesh:{mesh}");
    named(spans, "rdm.mesh.fabric.update.via-status-send")
        .into_iter()
        .filter(|sp| attr(sp, "scope") == scope && attr(sp, "status") == "ready" && at_ns(sp) / 1_000_000 < before_ms)
        .min_by_key(|sp| at_ns(sp))
        .map(|sp| attr(sp, "changed_at_rafka_ms"))
}

/// The lifecycle MeshStatus sends of `mesh` at or after `from_ms`, by (sender, changed_at): how many, and when the first.
fn mesh_sends_after(spans: &[Value], mesh: &str, from_ms: u64) -> BTreeMap<(String, String), (usize, u64)> {
    let scope = format!("mesh:{mesh}");
    let mut out: BTreeMap<(String, String), (usize, u64)> = BTreeMap::new();
    for sp in named(spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| attr(sp, "scope") == scope && at_ns(sp) / 1_000_000 >= from_ms) {
        let at = at_ns(sp) / 1_000_000;
        let e = out.entry((attr(sp, "node"), attr(sp, "changed_at_rafka_ms"))).or_insert((0, at));
        e.0 += 1;
        e.1 = e.1.min(at);
    }
    out
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

fn result(dir: &std::path::Path, v: Value) {
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&v).unwrap()).unwrap();
}

/// The birth `node_id` as the fabric's shared store root holds it: its row's status, and the node id the
/// path.name `node`'s name is held by (`None`: released).
fn stored_birth(root: &std::path::Path, node: &PathName, node_id: &str) -> (Option<String>, Option<String>) {
    let record = rdm_admin::store_log::read_record(&root.join(rdm_admin::store_root::NODES_FILE), "node-record", Some(node_id));
    let status = record.as_ref().map(|r| s(&r["record"]["status"]));
    // The name is read from the record (i152 s5: no name claim): the node id whose live record holds the pair.
    let name = record.filter(|r| r["record"]["path"] == node.path.as_str() && r["record"]["name"] == node.name.as_str() && r["record"]["status"] != "deleted").map(|r| s(&r["record"]["node_id"]));
    (status, name)
}

/// The ready span of the start whose trace is `run`.
fn ready_of<'a>(run: &[&'a Value], trace: &str) -> &'a Value {
    run.iter().copied().find(|sp| sp["name"] == "rdm.mesh.node.update.via-ready").unwrap_or_else(|| panic!("the start ends ready: trace {trace}"))
}

/// The pid of the runtime of the birth `node_id`: a runtime's data dir is named by its birth's node id.
fn runtime_of(estate: &Estate, node_id: &str) -> u32 {
    estate.live_runtimes().into_iter().find(|(p, _)| p.file_name().is_some_and(|f| f.to_string_lossy() == node_id)).map(|(_, pid)| pid).unwrap_or_else(|| panic!("the runtime of {node_id} is running"))
}

/// The names of the spans of the second ladder Q335 removed: none is emitted.
const REMOVED_SPANS: [&str; 5] = ["rdm.node_admin.mesh.update.via-probe", "rdm.node_admin.mesh.update.via-probe-verdict", "rdm.node_admin.mesh.update.via-unheard", "rdm.node_admin.mesh.create.via-recovery", "rdm.node_admin.mesh.update.via-recovered"];

/// CONTRACT: docs/architecture/lifecycles/node-recovery.md step 2
/// "the fabric-primary runs it, without the half-floor wait, only when the mesh's aggregate stays
/// unheard past the staleness floor: no successor took the role (Q335)."
/// CONTRACT: docs/architecture/lifecycles/node-recovery.md
/// "(A mesh none of whose node-admins can be reached is this page's case with no successor named: the
/// fabric-primary investigates the lost mesh-primary and re-creates it at its path.name, Q335.)"
/// CONTRACT (Q335; states.md: "**Fabric:** `pending` -> `ready`."): a peer mesh has one node-admin, its
/// mesh-primary, and it is killed (exact SIGKILL) while the mesh's nodes live. The election names no
/// successor. The fabric-primary's primary watch runs node investigation on the lost birth, reads its
/// verdict (the runtime exited) and submits its node recovery; with no node-admin left in the mesh the
/// fabric-primary executes it, and the node-admin is re-created at its path.name with a new node.id, the
/// mesh's primary again. Must NOT happen: a probe round, a mesh recovery of its own, a `degraded` status,
/// or the mesh given a node-admin anywhere but at the lost birth's path.name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_mesh_primary_with_no_successor_is_investigated_and_re_created_by_the_fabric_primary() {
    let cell = "a_lost_mesh_primary_with_no_successor_is_investigated_and_re_created_by_the_fabric_primary";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = [("mesh1", 2, 1), ("mesh2", 1, 2)];
    let built = estate.resize_shape("fabric1", &shape).await;
    let accepted = built.build_id.to_string();
    let before = values(&estate.settled_views(&shape).await);
    let fp_row = before.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("one fabric-primary");
    let (fp, fp_id) = (location(&fp_row), s(&fp_row["node_id"]));
    assert_eq!(fp_row["mesh"], "mesh1", "the fabric-primary is mesh1's: {fp_row}");
    let lost = Estate::path("mesh2.node-admin", "1");
    let lost_row = before.iter().find(|n| location(n) == lost).cloned().expect("mesh2's node-admin");
    let (lost_id, lost_inc) = (s(&lost_row["node_id"]), s(&lost_row["incarnation_id"]));
    assert_eq!(lost_row["is_primary"], true, "mesh2's one node-admin is its primary: {lost_row}");
    let mesh2_id = estate.mesh_view("mesh2").await.id.expect("mesh2 has its id").to_string();
    estate.enter(&fp).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let attempt_before = estate.attempt_of_build(&accepted).await;
    let (floor, gossip) = (rdm_transport::topology::staleness_floor(), rdm_transport::topology::gossip_interval());
    let fault_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.kill_pid(runtime_of(&estate, &lost_id));
    let recreated = wait_for(&format!("{lost:?} re-created at its path.name, ready and mesh2's primary"), floor * 4 + Duration::from_secs(60), || {
        let (estate, lost, lost_id) = (&estate, lost.clone(), lost_id.clone());
        async move { estate.node_values().await.into_iter().find(|n| location(n) == lost && s(&n["node_id"]) != lost_id && n["status"] == "ready" && n["is_primary"] == true) }
    })
    .await;
    wait_for("the fabric is ready", floor + gossip * 2, || async { estate.fabric_view_at(&fp).await.filter(|f| status_word(f.status) == "ready").map(|_| ()) }).await;
    let mesh2_after = estate.mesh_view("mesh2").await;
    let attempt_after = estate.attempt_of_build(&accepted).await;
    let stop_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.stop().await;
    let spans = estate.spans();
    let story = |sp: &&Value| at_ns(sp) >= fault_ns && at_ns(sp) < stop_ns;
    let looked: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-investigation").into_iter().filter(story).filter(|sp| rdm_test::estate::of_node(sp, &fp, &fp_id) && attr(sp, "suspect_node_id") == lost_id).collect();
    let look = looked.first().copied().unwrap_or_else(|| panic!("the fabric-primary ran node investigation on {lost:?}'s lost birth"));
    assert_eq!(attr(look, "suspect_incarnation"), lost_inc, "the exact lost birth: {look}");
    assert_ne!(attr(look, "finding"), "reached", "the killed birth is not reached: {look}");
    let submitted: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(story).filter(|sp| attr(sp, "node_id") == lost_id && attr(sp, "submitter") == fp_id).collect();
    assert!(!submitted.is_empty(), "the fabric-primary submitted the node recovery of {lost:?} on the verdict");
    assert!(submitted.iter().all(|sp| at_ns(sp) >= at_ns(look)), "the recovery follows the investigation's verdict");
    estate.record_trace_url(&s(&look["trace_id"]));
    for name in REMOVED_SPANS {
        assert!(named(&spans, name).is_empty(), "no span of the removed second ladder: {name}");
    }
    let degraded: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| attr(sp, "status") == "degraded").collect();
    assert!(degraded.is_empty(), "no degraded status is sent: {degraded:#?}");
    assert_eq!(mesh2_after.id.map(|id| id.to_string()), Some(mesh2_id.clone()), "mesh2 keeps its MeshId");
    assert_eq!(attempt_after, attempt_before, "the recovery is the fabric-primary's own workflow, never a Build attempt (Q342): {attempt_before} -> {attempt_after}");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "lost": lost, "lost_node_id": lost_id, "recreated_node_id": recreated["node_id"], "finding": attr(look, "finding"), "trace_id": look["trace_id"], "mesh_id": mesh2_id}));
}

/// Cut every UDP path between the nodes of `mine` and `theirs`; `None` (the estate stopped, named) when
/// the host cannot and the run does not require it.
async fn cut_between(estate: &mut Estate, nodes: &[Value], mine: &[PathName], theirs: &[PathName]) -> Option<Partition> {
    match Partition::start(&udp_ports(nodes, mine), &udp_ports(nodes, theirs)) {
        Ok(p) => Some(p),
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot cut UDP between the meshes: {why}");
            estate.stop().await;
            None
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// CONTRACT: docs/architecture/lifecycles/node-recovery.md step 1
/// "Each node's pruner marks a member `PendingReconnect` once it is unheard past the staleness floor.
/// Nothing is sent; the mark comes from the digests not arriving. ... Silence is not death."
/// CONTRACT (Q325 under Q335: heard again is node investigation's `reached`): every UDP path between the
/// fabric-primary's mesh and the peer mesh is cut while every process runs, and the cut heals before the
/// staleness floor. The fabric-primary never marks the peer mesh's primary silent: nothing investigates
/// it, nothing submits its recovery, and nothing deletes it. The same birth is ready and its mesh's primary
/// in every view, its row live and its name held by its node id on the store root, and the fabric is
/// ready. Must NOT happen: an investigation or a recovery of that birth, its TerminateRuntime, its row
/// written deleted, its name released, its `NodeDeleted`, or a node.create at its path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_heard_again_before_the_floor_is_never_investigated_and_nothing_deletes_it() {
    let cell = "a_mesh_primary_heard_again_before_the_floor_is_never_investigated_and_nothing_deletes_it";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = fixture(cell).await;
    let (fp, fp_id, lost) = (f.fabric_primary.clone(), f.fabric_primary_id.clone(), f.lost.clone());
    let fp_mesh = if lost == "mesh1" { "mesh2" } else { "mesh1" };
    let first_row = f.before.iter().find(|n| n["mesh"] == lost.as_str() && n["type"] == "node-admin" && n["is_primary"] == true).cloned().expect("the peer mesh's primary");
    let (first, first_id, first_inc) = (location(&first_row), s(&first_row["node_id"]), s(&first_row["incarnation_id"]));
    let mine: Vec<PathName> = f.before.iter().filter(|n| n["mesh"] == fp_mesh).map(location).collect();
    let theirs: Vec<PathName> = f.before.iter().filter(|n| n["mesh"] == lost.as_str()).map(location).collect();
    let root = f.estate.fabric_store_root();
    let intact = || {
        let (status, name) = stored_birth(&root, &first, &first_id);
        assert!(status.as_deref().is_some_and(|st| st != "deleted"), "{first:?}'s row stays live on the store root: {status:?}");
        assert_eq!(name.as_deref(), Some(first_id.as_str()), "{first:?}'s name stays held by its node id on the store root");
    };
    intact();
    let (floor, gossip) = (rdm_transport::topology::staleness_floor(), rdm_transport::topology::gossip_interval());
    let before = f.before.clone();
    let Some(cut) = cut_between(&mut f.estate, &before, &mine, &theirs).await else { return };
    let cut_ns = now_ns();
    // Healed well inside the floor: the last receipt before the cut is at most a backbone interval old.
    tokio::time::sleep(floor / 3).await;
    drop(cut);
    let healed_ns = now_ns();
    for at in [&fp, &first] {
        f.estate
            .until_view_at(at, &format!("{at:?}'s view holds {first:?} as the same birth, ready and {lost}'s primary"), floor * 2 + gossip * 4, |v| {
                Estate::values(v).iter().any(|n| location(n) == first && s(&n["node_id"]) == first_id && s(&n["incarnation_id"]) == first_inc && n["is_primary"] == true && n["status"] == "ready").then_some(())
            })
            .await;
    }
    wait_for("the fabric is ready", floor + gossip * 2, || async { f.estate.fabric_view_at(&fp).await.filter(|v| status_word(v.status) == "ready").map(|_| ()) }).await;
    // Two floors more after the heal: a mark, an investigation or a recovery would have shown by now.
    tokio::time::sleep(floor * 2).await;
    intact();
    let stop_at = now_ns();
    f.estate.stop().await;
    let spans = f.estate.spans();
    let story = |sp: &&Value| at_ns(sp) < stop_at && at_ns(sp) >= cut_ns;
    let looked: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-investigation").into_iter().filter(story).filter(|sp| attr(sp, "suspect_node_id") == first_id).collect();
    assert!(looked.is_empty(), "nothing investigated {first:?}: it was never marked silent: {looked:#?}");
    let submitted: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(story).filter(|sp| attr(sp, "node_id") == first_id).collect();
    assert!(submitted.is_empty(), "nothing submitted {first:?}'s recovery: {submitted:#?}");
    let terminated: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(story).filter(|sp| rdm_test::estate::at_location(sp, &first) && attr(sp, "step") == "TerminateRuntime").collect();
    assert!(terminated.is_empty(), "{first:?}'s runtime was never terminated: {terminated:#?}");
    let marks: Vec<&Value> = spans
        .iter()
        .filter(story)
        .filter(|sp| sp["name"].as_str().is_some_and(|n| n.ends_with(".via-deleted-birth") || n.ends_with(".via-node-deleted") || n.ends_with(".via-node-deleting") || n == "rdm.node_admin.node.delete.via-node-row"))
        .filter(|sp| sp["attributes"].to_string().contains(&first_id) || sp["attributes"].to_string().contains(&first_inc))
        .collect();
    assert!(marks.is_empty(), "nothing deleted or marked {first:?}'s birth: {marks:#?}");
    let creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().filter(story).filter(|sp| rdm_test::estate::names_location(sp, "subject", &first) && attr(sp, "moment") == "create").collect();
    assert!(creates.is_empty(), "no node.create wrote {first:?}'s row: {creates:#?}");
    for name in REMOVED_SPANS {
        assert!(named(&spans, name).is_empty(), "no span of the removed second ladder: {name}");
    }
    eprintln!("{fp:?} ({fp_id}): cut {} ms, {first:?} never marked", (healed_ns - cut_ns) / 1_000_000);
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": lost, "primary": first, "cut_ms": (healed_ns - cut_ns) / 1_000_000}));
}

/// CONTRACT (node-mesh-join.md Wire: "`primary` is the commander's held mesh-primary record of the node's
/// mesh, which a commanded node-admin takes by the supersedes rule before it runs its election: not named,
/// it drops its primary role and its own record, then joins; named, it refuses the command by name ... and
/// joins no one"; Q335: "once they hear each other, both run the one election over the same ids and
/// primary records and get the same answer; the one it does not name removes its primary role, its own
/// held primary record included, and rejoins"): the peer mesh's primary is cut from every other node past
/// the staleness floor while it runs. Its mesh's other node-admin, the successor its election names,
/// publishes its aggregate on the backbone naming itself, and the cut heals then, inside the successor's
/// ladder on the lost birth. The two node-admins hear each other and the one election decides, whichever
/// way: a partition case, held to what the standing rules do with it (inbox 0030). Every node-admin
/// commanded back runs its election over the commander's record; not named, it drops its primary role
/// before node.mesh.join; named, it refuses by name. Exactly one mesh-primary of the mesh, one of the two
/// same births, is left in every view. Nothing recovers, terminates or deletes either, and the
/// fabric-primary does not investigate the lost birth. Must NOT happen: two mesh-primaries of the mesh in
/// a view after the heal, a rejoin before its drop, a named admin's rejoin, a recovery or a
/// TerminateRuntime of either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_mesh_primary_heard_again_and_its_successor_settle_on_one_mesh_primary_each_commanded_one_dropping_its_role_before_it_rejoins() {
    let cell = "a_lost_mesh_primary_heard_again_and_its_successor_settle_on_one_mesh_primary_each_commanded_one_dropping_its_role_before_it_rejoins";
    let mut f = fixture(cell).await;
    let (fp, fp_id, lost) = (f.fabric_primary.clone(), f.fabric_primary_id.clone(), f.lost.clone());
    let first_row = f.before.iter().find(|n| n["mesh"] == lost.as_str() && n["type"] == "node-admin" && n["is_primary"] == true).cloned().expect("the peer mesh's primary");
    let (first, first_id) = (location(&first_row), s(&first_row["node_id"]));
    let successor_row = f.before.iter().find(|n| n["mesh"] == lost.as_str() && n["type"] == "node-admin" && n["node_id"] != first_row["node_id"]).cloned().expect("the peer mesh's other node-admin");
    let (successor, successor_id) = (location(&successor_row), s(&successor_row["node_id"]));
    let births: Vec<(PathName, String, String)> = [&first_row, &successor_row].iter().map(|r| (location(r), s(&r["node_id"]), s(&r["incarnation_id"]))).collect();
    let others: Vec<PathName> = f.before.iter().map(location).filter(|l| *l != first).collect();
    let (floor, gossip) = (rdm_transport::topology::staleness_floor(), rdm_transport::topology::gossip_interval());
    let before = f.before.clone();
    let Some(cut) = cut_between(&mut f.estate, &before, &[first.clone()], &others).await else { return };
    let cut_ns = now_ns();
    // The successor's first act: its mesh's aggregate on the backbone naming it mesh-primary. The cut heals
    // then, before its ladder on the lost birth answers.
    let first_aggregate = wait_for(&format!("{successor:?} publishes {lost}'s aggregate naming itself"), floor * 4 + Duration::from_secs(30), || async {
        named(&f.estate.spans(), "rdm.mesh.backbone.update.via-primary-aggregate").into_iter().find(|sp| at_ns(sp) >= cut_ns && attr(sp, "mesh") == lost && attr(sp, "publisher").contains(&successor_id)).cloned()
    })
    .await;
    drop(cut);
    let healed_ns = now_ns();
    // Every view converges on both births, the same births, and one mesh-primary of the mesh: the one
    // election's, whichever it names. The views are read together until they agree: a view read while the
    // election's answer is still reaching it can name the other birth for a moment.
    let named_by_view: Vec<String> = wait_for(&format!("every view holds {first:?} and {successor:?} as the same births and the same one primary of {lost}"), floor * 3 + gossip * 4, || async {
        let mut named = Vec::new();
        for at in [&fp, &first, &successor] {
            let v = Estate::values(&f.estate.node_views_at(at).await?);
            let same = births.iter().all(|(l, id, inc)| v.iter().any(|n| location(n) == *l && s(&n["node_id"]) == *id && s(&n["incarnation_id"]) == *inc));
            let primaries: Vec<String> = v.iter().filter(|n| n["mesh"] == lost.as_str() && n["type"] == "node-admin" && n["is_primary"] == true).map(|n| s(&n["node_id"])).collect();
            if !(same && primaries.len() == 1 && births.iter().any(|(_, id, _)| *id == primaries[0])) {
                return None;
            }
            named.push(primaries[0].clone());
        }
        named.windows(2).all(|w| w[0] == w[1]).then_some(named)
    })
    .await;
    let winner_id = named_by_view[0].clone();
    let loser = births.iter().find(|(_, id, _)| *id != winner_id).map(|(l, _, _)| l.clone()).expect("the one the election does not name");
    let winner = births.iter().find(|(_, id, _)| *id == winner_id).map(|(l, _, _)| l.clone()).expect("the one the election names");
    let stop_at = now_ns();
    f.estate.stop().await;
    let spans = f.estate.spans();
    let story = |sp: &&Value| at_ns(sp) < stop_at && at_ns(sp) >= cut_ns;
    let after_heal = |sp: &&Value| at_ns(sp) < stop_at && at_ns(sp) >= healed_ns;
    let truthy = |sp: &Value, k: &str| sp["attributes"][k] == true || sp["attributes"][k] == "true";
    // Every commanded rejoin of either admin after the heal: its election over the commander's record did
    // not name it, and it dropped its primary role before node.mesh.join.
    let elections: Vec<&Value> = named(&spans, "rdm.mesh.election.resolve.via-commanded-join").into_iter().filter(after_heal).filter(|sp| births.iter().any(|(_, id, _)| attr(sp, "node_id") == *id)).collect();
    let starts: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-start").into_iter().filter(after_heal).filter(|sp| births.iter().any(|(_, id, _)| attr(sp, "node_id") == *id)).collect();
    // The race may settle before any command reaches either (the old primary takes the successor's record
    // from gossip and yields): then no rejoin follows, and the views alone hold the one election's answer.
    eprintln!("{loser:?}, not named: {} commanded rejoin(s) after the heal", starts.len());
    let mut flame = Vec::new();
    for start in &starts {
        let (id, trace) = (attr(start, "node_id"), s(&start["trace_id"]));
        let run: Vec<&Value> = spans.iter().filter(|sp| s(&sp["trace_id"]) == trace).collect();
        let election = elections.iter().copied().filter(|sp| attr(sp, "node_id") == id && at_ns(sp) <= at_ns(start)).max_by_key(|sp| at_ns(sp)).unwrap_or_else(|| panic!("{id} runs its election over the commander's record before its start: {elections:#?}"));
        assert!(!truthy(election, "named"), "{id}'s election before its rejoin does not name it: {election:#?}");
        let dropped = run.iter().copied().find(|sp| sp["name"] == "rdm.mesh.primary.remove.via-commanded-join").unwrap_or_else(|| panic!("{id} drops its primary role in its commanded start: trace {trace}"));
        let joined = run.iter().copied().filter(|sp| sp["name"] == "rdm.mesh.node.update.via-join-admitted").min_by_key(|sp| at_ns(sp));
        assert!(joined.is_none_or(|j| at_ns(dropped) <= at_ns(j)), "{id}'s drop precedes its node.mesh.join: {dropped:#?} {joined:#?}");
        flame.push(format!("{id}: election {} drop {} trace {trace}", s(&election["span_id"]), s(&dropped["span_id"])));
    }
    // A named admin refuses by name: no rejoin of it follows its named election in the same command.
    let refusals: Vec<&Value> = elections.iter().copied().filter(|sp| truthy(sp, "named")).collect();
    for sp in &refusals {
        let id = attr(sp, "node_id");
        assert!(!starts.iter().any(|st| attr(st, "node_id") == id && s(&st["trace_id"]) == s(&sp["trace_id"])), "{id}, named, starts no rejoin on that command: {sp:#?}");
    }
    // Reached: nothing recovers or terminates either, and the fabric-primary does not look at the lost birth.
    for (l, id, _) in &births {
        let submitted: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(story).filter(|sp| attr(sp, "node_id") == *id).collect();
        assert!(submitted.is_empty(), "nothing recovers {l:?}: {submitted:#?}");
        let terminated: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(story).filter(|sp| rdm_test::estate::at_location(sp, l) && attr(sp, "step") == "TerminateRuntime").collect();
        assert!(terminated.is_empty(), "{l:?}'s runtime was never terminated: {terminated:#?}");
    }
    let fp_looks: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-investigation").into_iter().filter(story).filter(|sp| rdm_test::estate::of_node(sp, &fp, &fp_id) && attr(sp, "suspect_node_id") == first_id).collect();
    assert!(fp_looks.is_empty(), "the fabric-primary does not investigate {first:?}: {fp_looks:#?}");
    let refusal = refusals.first().map(|sp| format!("{} (trace {})", s(&sp["span_id"]), s(&sp["trace_id"]))).unwrap_or_else(|| "none".into());
    eprintln!(
        "one-election cell: {first:?} ({first_id}) cut {} ms, healed after {successor:?}'s first aggregate (trace {}); the one primary {winner:?}; refusal {refusal}; rejoins {flame:?}",
        (healed_ns - cut_ns) / 1_000_000,
        s(&first_aggregate["trace_id"]),
    );
    if let Some(start) = starts.first() {
        f.estate.record_trace_url(&s(&start["trace_id"]));
    }
}

/// CONTRACT (R-S3, same status, fresh proof): a peer mesh loses its primary node-admin (exact SIGKILL)
/// while its other node-admin and its nodes live. The survivor is ELECTED primary; nothing is created. It
/// adopts the mesh's existing status without inventing a transition and re-sends the checklist's down op to
/// its planned births. The killed primary is still a planned birth of the accepted Build, so the
/// elected primary's checklist names it as missing (a named hold, no timer) and republishes nothing.
/// The Build restarts the killed birth; when every planned birth has checked in the checklist completes
/// and the adopted fact is republished: the same status, the OLD changed_at, five identical sends,
/// and the mesh primary's Mesh declaration to the fabric primary (the fresh checklist-complete report)
/// follows the completed checklist. What must NOT happen: silence after the election with no named
/// hold, a republish before the checklist completes, a new changed_at.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elected_primary_holds_its_checklist_for_the_killed_primary_then_republishes_the_old_status() {
    let cell = "elected_primary_holds_its_checklist_for_the_killed_primary_then_republishes_the_old_status";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = fixture(cell).await;
    let primary = f.before.iter().find(|n| n["mesh"] == f.lost.as_str() && n["type"] == "node-admin" && n["is_primary"] == true).map(|n| location(&n)).expect("the peer mesh has a primary node-admin");
    let survivor = f.before.iter().filter(|n| n["mesh"] == f.lost.as_str() && n["type"] == "node-admin").map(|n| location(&n)).find(|n| *n != primary).expect("the peer mesh has a second node-admin");
    let fault_ms = now_ms();
    let (_, pid) = f.runtimes(&format!("{}.node-admin", f.lost)).into_iter().find(|(n, _)| *n == primary).unwrap_or_else(|| panic!("{primary:?} is running"));
    f.estate.kill_pid(pid);
    if primary == Estate::path("mesh1.node-admin", "1") {
        f.estate.kill_bootstrap();
    }
    let (lost, sv, killed, killed_id) = (f.lost.clone(), survivor.clone(), primary.clone(), f.id_of(&primary));
    wait_for("the elected primary names the killed primary as missing from its checklist", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let held = named(&spans, "rdm.node_admin.mesh.update.via-checklist-held").into_iter().any(|sp| rdm_test::estate::at_location(sp, &sv) && attr(sp, "mesh") == lost && names_missing(&attr(sp, "missing"), &killed, &killed_id) && at_ns(sp) / 1_000_000 >= fault_ms);
        async move { held.then_some(()) }
    })
    .await;
    // The report is its own declaration, owed once the checklist completed and sent on the declarer's
    // cadence: it follows the checklist, and the republish does not wait for it. The estate is stopped
    // only once the fabric primary of the moment has it, or the stop outruns the declaration.
    let lost_admin_ids = f.admin_ids(&f.lost).await;
    wait_for("a checklist of the mesh completes after the fault, its adopted status is republished and the mesh primary's checklist-complete report reaches the fabric primary", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let complete = named(&spans, "rdm.node_admin.mesh.update.via-checklist-complete").into_iter().filter(|sp| attr(sp, "mesh") == lost && at_ns(sp) / 1_000_000 >= fault_ms).map(|sp| at_ns(sp) / 1_000_000).min();
        let sent = !mesh_sends_after(&spans, &lost, fault_ms).is_empty();
        let reported = complete.is_some_and(|c| {
            named(&spans, "rdm.node_admin.status.update.via-declaration")
                .into_iter()
                .any(|sp| attr(&sp, "op") == "declare-mesh-state" && lost_admin_ids.contains(&attr(&sp, "sender")) && matches!(attr(&sp, "outcome").as_str(), "applied" | "already-applied") && at_ns(&sp) / 1_000_000 >= c)
        });
        async move { (sent && reported).then_some(()) }
    })
    .await;
    let (fp, fp_id) = (f.fabric_primary.clone(), f.fabric_primary_id.clone());
    f.estate.stop().await;
    let spans = f.estate.spans();
    let before = mesh_status_change_before(&spans, &f.lost, fault_ms).expect("the mesh published its status before the fault");
    let sends = mesh_sends_after(&spans, &f.lost, fault_ms);
    for ((sender, changed_at), _) in &sends {
        assert!(lost_admin_ids.contains(sender), "the mesh's status is authored by its own primary, not {sender}");
        assert_eq!(*changed_at, before, "the republish keeps the old changed_at: the lifecycle state did not change ({sender})");
    }
    let first_send_ms = sends.values().map(|(_, first)| *first).min().expect("the adopted status was republished");
    let after_fault = |name: &str, node: Option<&PathName>| -> Vec<u64> {
        named(&spans, name).into_iter().filter(|sp| attr(sp, "mesh") == f.lost && node.is_none_or(|n| rdm_test::estate::at_location(sp, n)) && at_ns(sp) / 1_000_000 >= fault_ms).map(|sp| at_ns(sp) / 1_000_000).collect()
    };
    let held = after_fault("rdm.node_admin.mesh.update.via-checklist-held", Some(&survivor));
    let down = after_fault("rdm.node_admin.mesh.update.via-state-down", Some(&survivor));
    let complete = after_fault("rdm.node_admin.mesh.update.via-checklist-complete", None);
    let (held_ms, complete_ms) = (*held.iter().min().expect("the elected primary held its checklist"), *complete.iter().min().expect("a checklist completed"));
    assert!(!down.is_empty(), "the elected primary re-sent the checklist's down op to its planned births");
    assert!(held_ms <= complete_ms && complete_ms <= first_send_ms, "hold ({held_ms}) -> checklist complete ({complete_ms}) -> republish ({first_send_ms}), in that order: nothing is republished while a planned birth is missing");
    let republisher = sends.keys().map(|(sender, _)| sender.clone()).next().unwrap();
    let reports: Vec<u64> = named(&spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|sp| attr(sp, "op") == "declare-mesh-state" && lost_admin_ids.contains(&attr(sp, "sender")) && matches!(attr(sp, "outcome").as_str(), "applied" | "already-applied"))
        .map(|sp| at_ns(sp) / 1_000_000)
        .collect();
    assert!(reports.iter().any(|at| *at >= complete_ms), "the fabric primary of the moment received a checklist-complete report from the mesh's primary after the checklist completed: {reports:?} vs {complete_ms}");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "killed_primary": primary, "elected": survivor, "republisher": republisher, "changed_at": before, "held_ms": held_ms, "complete_ms": complete_ms, "first_send_ms": first_send_ms}));
}
