//! i143.e4.s10 detection acceptance (rafka-v2 #2803; docs/architecture/lifecycles/mesh-investigation.md),
//! PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2803-detect-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans, feature `i143-2803-detect`).
//!
//! The estate: two meshes of two node-admins and three rpc nodes, settled through a Build. The
//! peer mesh is the one whose admins do not hold the fabric-primary role. Time is the elapsed time since the
//! fabric primary's last direct backbone receipt from the mesh, on the investigation's rungs
//! (`Rungs::configured`, from the staleness floor `RDM_STALENESS_MS`): probe 1, the silent mark,
//! probe 2 and the decision at 20 s, 30 s, 40 s and 60 s at the 30 s default floor.

use rdm_client::{NodeView, ScopeStatus};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::typed::fabric_view_of;
use rdm_test::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

/// The investigation's rungs as the estate runs them (the nodes read the same cadence environment):
/// a cell waits on these, never a fixed number of milliseconds.
fn rungs() -> rdm_admin::investigate::Rungs {
    rdm_admin::investigate::Rungs::configured()
}

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

/// The fabric's lifecycle state as `fabric.get` words it (`ready`, `degraded`, ...).
fn status_word(status: ScopeStatus) -> String {
    s(&serde_json::to_value(status).expect("a status serializes"))
}

fn births(nodes: &[Value]) -> BTreeMap<String, String> {
    nodes.iter().map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect()
}

fn signal(pid: u32, sig: &str) {
    assert!(Command::new("kill").args([sig, &pid.to_string()]).status().unwrap().success(), "kill {sig} {pid}");
}

/// A settled estate and the peer mesh the fault will fall on.
struct Fixture {
    estate: Estate,
    accepted: String,
    attempt_before: u64,
    fabric_primary: String,
    lost: String,
    lost_mesh_id: String,
    before: Vec<Value>,
}

async fn fixture(cell: &str) -> Fixture {
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = [("mesh1", 2, 3), ("mesh2", 2, 3)];
    let built = estate.reconcile_shape("fabric1", &shape).await;
    let accepted = built.build_id.to_string();
    let before = values(&estate.settled_views(&shape).await);
    let fabric_primary = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let lost = if fabric_primary.starts_with("mesh1.") { "mesh2" } else { "mesh1" }.to_string();
    let lost_mesh_id = estate.mesh_view(&lost).await.id.expect("the lost mesh has its id").to_string();
    let attempt_before = estate.attempt_of_build(&accepted).await;
    // Control goes through the fabric primary's advertised API, which no fault here touches.
    estate.enter(&Estate::path(&fabric_primary)).await;
    // The fault falls on an established fabric, so the cells that assert the fabric stays ready start
    // from ready.
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    // The fault falls on a mesh whose status is published: the fabric is ready at the
    // fabric-primary's apply of the mesh's ready, the moment the mesh's primary sends it (states.md), so
    // the fixture waits for that send and its first repeat in the primary's own spans.
    let primary = before.iter().find(|n| n["mesh"] == lost.as_str() && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("the lost mesh has a primary node-admin");
    wait_for("the lost mesh's primary has sent its ready status and repeated it", Duration::from_secs(60), || {
        let spans = estate.spans();
        let sends = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| attr(sp, "scope") == format!("mesh:{lost}") && attr(sp, "node") == primary && attr(sp, "status") == "ready").count();
        async move { (sends >= 2).then_some(()) }
    })
    .await;
    Fixture { estate, accepted, attempt_before, fabric_primary, lost, lost_mesh_id, before }
}

impl Fixture {
    /// The runtimes of the peer mesh whose path starts with `prefix`, by node name.
    fn runtimes(&self, prefix: &str) -> Vec<(String, u32)> {
        let mut out: Vec<(String, u32)> = self
            .estate
            .live_runtimes()
            .into_iter()
            .filter_map(|(p, pid)| {
                let name = p.file_name().unwrap().to_string_lossy().to_string();
                name.starts_with(prefix).then(|| (name.split('-').next().unwrap_or(&name).to_string(), pid))
            })
            .collect();
        // The bootstrap admin is the mesh1 admin.1 and has no provider deployment.json.
        if prefix.starts_with("mesh1.admin") {
            if let Some(pid) = self.estate.bootstrap_pid() {
                out.push(("mesh1.admin.1".into(), pid));
            }
        }
        out
    }

    fn lost_admins(&self) -> Vec<(String, u32)> {
        self.runtimes(&format!("{}.admin.", self.lost))
    }

    fn lost_members(&self) -> Vec<(String, u32)> {
        self.runtimes(&format!("{}.rpc.", self.lost))
    }

    /// The unix nanoseconds of the last backbone receipt of the peer mesh the fabric primary
    /// names in its first `via-unheard` span, as `at` (unix ms).
    fn unheard_since_ms(&self, spans: &[Value]) -> Option<u64> {
        named(spans, "rdm.node_admin.mesh.update.via-unheard").into_iter().filter(|sp| attr(sp, "mesh") == self.lost && attr(sp, "node") == self.fabric_primary).filter_map(|sp| attr(sp, "at").parse::<u64>().ok()).min()
    }

    /// Stop the estate; the instant (unix ns) the cell's story ended, taken before the shutdown began.
    /// The shutdown stops the peer mesh's members one by one, and a mesh it leaves unheard opens a new
    /// unheard interval at the fabric primary (mesh-investigation.md step 4: "Direct backbone receipt
    /// from mesh-x at any point cancels the investigation and resets the unheard interval"): what that
    /// makes the fabric do is not the investigation the cell is about.
    async fn stop(&mut self) -> u64 {
        let end = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.estate.stop().await;
        end
    }

    /// The probes of the lost mesh the fabric primary made before `end` ([`Self::stop`]).
    fn probes_before<'a>(&self, spans: &'a [Value], end: u64) -> Vec<&'a Value> {
        self.probes(spans).into_iter().filter(|p| at_ns(p) < end).collect()
    }

    fn probes<'a>(&self, spans: &'a [Value]) -> Vec<&'a Value> {
        let mut p: Vec<&Value> = named(spans, "rdm.node_admin.mesh.update.via-probe").into_iter().filter(|sp| attr(sp, "mesh") == self.lost && attr(sp, "node") == self.fabric_primary).collect();
        p.sort_by_key(|sp| at_ns(sp));
        p
    }

    fn verdicts<'a>(&self, spans: &'a [Value]) -> Vec<&'a Value> {
        let mut p: Vec<&Value> = named(spans, "rdm.node_admin.mesh.update.via-probe-verdict").into_iter().filter(|sp| attr(sp, "mesh") == self.lost && attr(sp, "node") == self.fabric_primary).collect();
        p.sort_by_key(|sp| at_ns(sp));
        p
    }

    /// The node.create of a node-admin into the lost mesh the fabric primary opened on a verdict.
    fn admin_creates<'a>(&self, spans: &'a [Value]) -> Vec<&'a Value> {
        named(spans, "rdm.node_admin.mesh.create.via-recovery").into_iter().filter(|sp| attr(sp, "mesh") == self.lost && attr(sp, "node") == self.fabric_primary).collect()
    }

    async fn fabric_status(&self) -> String {
        status_word(self.estate.fabric_view().await.status)
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
    for sp in named(spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| attr(sp, "scope") == scope && attr(sp, "status") != "degraded" && at_ns(sp) / 1_000_000 >= from_ms) {
        let at = at_ns(sp) / 1_000_000;
        let e = out.entry((attr(sp, "node"), attr(sp, "changed_at_rafka_ms"))).or_insert((0, at));
        e.0 += 1;
        e.1 = e.1.min(at);
    }
    out
}

/// When the fabric left `degraded` on a completed checklist, in unix ms: the holder that decided the
/// node.create of the mesh's node-admin clears it on the new mesh-primary's report; a successor
/// holder that adopted `degraded` clears it when every mesh primary has reported.
fn degraded_cleared_at(spans: &[Value], mesh: &str) -> Vec<u64> {
    let mut at: Vec<u64> = named(spans, "rdm.node_admin.mesh.update.via-recovered").into_iter().filter(|sp| attr(sp, "mesh") == mesh).map(|sp| at_ns(sp) / 1_000_000).collect();
    at.extend(named(spans, "rdm.node_admin.fabric.update.via-checklist-complete").into_iter().filter(|sp| attr(sp, "degraded_cleared") == "true").map(|sp| at_ns(sp) / 1_000_000));
    at.sort();
    at
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

fn result(dir: &std::path::Path, v: Value) {
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&v).unwrap()).unwrap();
}

/// CONTRACT (#2803 detection, ruling 2026-10-08): a peer mesh loses both node-admins (exact SIGKILL)
/// while its three rpc nodes live. The fabric primary hears nothing of the mesh on the backbone.
/// At probe 1's rung (20 s unheard at the default floor) it sends probe 1 to each node-admin, the
/// exact mesh-primary first: direct, then a Ping to the members and, through the member that
/// answered, a Forward carrying `ProbeNodeState`; the member cannot reach its own node-admin and
/// answers `connection-lost`. At the mark (30 s) the mesh is marked silent (nothing is sent), at
/// probe 2's rung (40 s) probe 2 prefers another member and answers the same. At the decision (60 s)
/// the latest probe being `connection-lost` is the verdict: the fabric primary authors
/// FabricStatus `degraded` and runs node.create of a node-admin into the mesh, under the same Build and
/// the existing MeshId; the fabric returns to ready when the mesh has its primary again. What must NOT
/// happen: a node-admin created before the decision's rung, a new MeshId or Build id, a surviving member created again, a degraded fabric
/// before the verdict.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_mesh_without_its_admins_gets_a_node_admin_only_on_a_connection_lost_verdict_after_two_probes() {
    let cell = "a_peer_mesh_without_its_admins_gets_a_node_admin_only_on_a_connection_lost_verdict_after_two_probes";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = fixture(cell).await;
    let lost_births: BTreeMap<String, String> = births(&f.before).into_iter().filter(|(n, _)| n.starts_with(&format!("{}.admin.", f.lost))).collect();
    let survivors: Vec<String> = (1..=3).map(|i| format!("{}.rpc.{i}", f.lost)).collect();
    let before_births = births(&f.before);

    let admins = f.lost_admins();
    assert_eq!(admins.len(), 2, "both admins of {} are running: {admins:?}", f.lost);
    let fault_ms = now_ms();
    for (_, pid) in &admins {
        f.estate.kill_pid(*pid);
    }
    if f.lost == "mesh1" {
        f.estate.kill_bootstrap();
    }

    let (accepted, attempt_before, lost_births_w) = (f.accepted.clone(), f.attempt_before, lost_births.clone());
    let statuses = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(String, u64)>::new()));
    let sampler = {
        let (peers, entry, fabric_id, statuses) = (f.estate.peers().await, f.estate.entry().await, f.estate.fabric_id.clone(), statuses.clone());
        tokio::spawn(async move {
            loop {
                if let Some(view) = fabric_view_of(&peers, &entry, &fabric_id).await {
                    let st = status_word(view.status);
                    let mut seen = statuses.lock().unwrap();
                    if seen.last().map(|(l, _)| l != &st).unwrap_or(true) {
                        seen.push((st, now_ms()));
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    wait_for("the accepted Build is complete past the fault and a node-admin is created for the lost ones", Duration::from_secs(300), || {
        let estate = &f.estate;
        let (accepted, lost_births) = (accepted.clone(), lost_births_w.clone());
        async move {
            let b = estate.carrier().await.build(&rdm_client::BuildId::parse(&accepted).unwrap()).await.expect("build.get [id] of the accepted Build");
            let later = b.phase == rdm_core::build::BuildPhase::Complete && u64::from(b.attempt) > attempt_before;
            let created = births(&estate.node_values().await).iter().filter(|(n, i)| lost_births.get(*n).is_some_and(|old| old != *i)).count() >= 1;
            (later && created).then_some(b)
        }
    })
    .await;
    let after = values(&f.estate.settles_to(&[("mesh1", 2, 3), ("mesh2", 2, 3)], Duration::from_secs(90)).await);
    wait_for("the fabric is ready again", Duration::from_secs(30), || async { (f.fabric_status().await == "ready").then_some(()) }).await;
    // The sampler polls on its own cadence: stop it only once it has recorded the return to ready.
    wait_for("the sampler records the return to ready", Duration::from_secs(10), || {
        let seen = statuses.lock().unwrap().last().is_some_and(|(st, _)| st == "ready");
        async move { seen.then_some(()) }
    })
    .await;
    sampler.abort();
    let statuses = statuses.lock().unwrap().clone();
    // The return to ready is five sends a second apart, from the fabric primary for as long as it
    // holds the role: the mesh, its node-admin created, can take the role (the election is the lowest ready NodeId), and
    // a publisher that loses the role ends the sends still to come.
    let degraded_ms = statuses.iter().find(|(st, _)| st == "degraded").map(|(_, at)| *at).unwrap_or(0);
    let fp_for_sends = f.fabric_primary.clone();
    let role_lost_or_five = |spans: &[Value]| {
        let sent = named(spans, "rdm.mesh.fabric.update.via-status-send")
            .into_iter()
            .filter(|sp| attr(sp, "node") == fp_for_sends && attr(sp, "scope").starts_with("fabric:") && attr(sp, "status") == "ready" && at_ns(sp) / 1_000_000 > degraded_ms)
            .count();
        let any_sent = named(spans, "rdm.mesh.fabric.update.via-status-send").into_iter().any(|sp| attr(sp, "scope").starts_with("fabric:") && attr(sp, "status") == "ready" && at_ns(sp) / 1_000_000 > degraded_ms);
        let role_lost = any_sent && named(spans, "rdm.mesh.fabric.update.via-status-publisher").into_iter().any(|sp| attr(sp, "node") == fp_for_sends && attr(sp, "role") == "stop" && at_ns(sp) / 1_000_000 > degraded_ms);
        (sent, role_lost)
    };
    wait_for("the return to ready is sent five times or the role moves", Duration::from_secs(30), || {
        let (sent, role_lost) = role_lost_or_five(&f.estate.spans());
        async move { (sent >= 5 || role_lost).then_some(()) }
    })
    .await;
    let role_moved = role_lost_or_five(&f.estate.spans()).1;
    assert_eq!(f.estate.accepted_build_id().await, f.accepted, "no new Build: the current build_id is unchanged");
    assert_eq!(f.estate.mesh_view(&f.lost).await.id.expect("the lost mesh still has its id").to_string(), f.lost_mesh_id, "{} keeps its own MeshId", f.lost);
    let new_births = births(&after);
    for m in &survivors {
        assert_eq!(new_births[m], before_births[m], "{m} answered the probes and is never created again");
    }

    let fp = f.fabric_primary.clone();
    let story_end = f.stop().await;
    let spans = f.estate.spans();
    let last_heard_ms = f.unheard_since_ms(&spans).unwrap_or_else(|| panic!("{fp} tracked {} as unheard (via-unheard names the last backbone receipt)", f.lost));
    let unheard = |sp: &Value| Duration::from_millis((at_ns(sp) / 1_000_000).saturating_sub(last_heard_ms));

    let probes = f.probes_before(&spans, story_end);
    assert_eq!(probes.len(), 2, "two probes, no more: {probes:#?}");
    for (i, p) in probes.iter().enumerate() {
        assert_eq!(attr(p, "probe"), (i + 1).to_string(), "probes are numbered in order");
        // A member may still hold a pooled connection to the killed node-admin: a probe over it
        // commits and loses its reply (`Indeterminate`, unreachable). Its next dial fails at the dial,
        // which is the edge-lost answer. Probe 2 is the one that decides.
        let stale_pool = i == 0 && attr(p, "outcome") == "unreachable" && attr(p, "detail").contains("Indeterminate");
        assert!(attr(p, "outcome") == "connection-lost" || stale_pool, "the member cannot reach its own node-admin: {p:#?}");
        assert!(attr(p, "member").split(',').all(|m| survivors.contains(&m.to_string())) && survivors.contains(&attr(p, "carrier")), "members of {} were asked and one carried it: {p:#?}", f.lost);
        // iroh's local view of each node-admin asked is a diagnostic on the probe span (R-CK1:
        // observation stays, separate from the decision); nothing decides on it.
        for asked in attr(p, "target").split(',').filter_map(|t| t.split('=').next()) {
            for field in ["iroh_known_addrs", "iroh_active_addrs"] {
                assert!(attr(p, field).split(';').any(|v| v.starts_with(&format!("{asked}="))), "the probe span carries iroh's view of {asked} in {field}: {p:#?}");
            }
        }
    }
    assert!(unheard(probes[0]) >= rungs().probe1, "probe 1 fires at {:?} unheard, not before: {:?}", rungs().probe1, unheard(probes[0]));
    assert!(unheard(probes[1]) >= rungs().probe2, "probe 2 fires at {:?} unheard, not before: {:?}", rungs().probe2, unheard(probes[1]));
    assert_ne!(attr(probes[0], "carrier"), attr(probes[1], "carrier"), "probe 2 prefers another carrier");

    let verdicts = f.verdicts(&spans);
    let verdict = verdicts.iter().find(|v| attr(v, "outcome") == "connection-lost").unwrap_or_else(|| panic!("the verdict is connection-lost: {verdicts:#?}"));
    assert!(unheard(verdict) >= rungs().decide, "the decision is at {:?} unheard, not before: {:?}", rungs().decide, unheard(verdict));
    let creates = f.admin_creates(&spans);
    assert_eq!(creates.len(), 1, "one node.create of a node-admin: {creates:#?}");
    assert!(at_ns(creates[0]) >= at_ns(verdict), "the verdict is recorded before the node.create opens");

    // R-D1: until the mesh is heard again the fabric primary holds it as a mesh. The ladder's decision
    // releases the first recovering node-admin only: one attempt, for mesh node-admin 1. Every other node of the
    // mesh is reconciled once the mesh is heard again (by its new mesh primary's own sweep or the drift pass).
    // The window ends when the fabric primary hears the mesh again, or when it loses the role to the mesh's new
    // mesh (its drift pass is then no one's but the new primary's). Heard again is its first direct backbone receipt
    // of the mesh after the verdict (the snapshot installed off the backbone), the instant the R-D1 target reads
    // (`Ladder::target` over `DigestView::mesh_unheard`); the ladder's own heard-again verdict comes on its next
    // checklist, after it.
    let heard_again = named(&spans, "rdm.mesh.topology.update.via-snapshot-installed")
        .into_iter()
        .filter(|sp| attr(sp, "mesh") == f.lost && attr(sp, "node") == fp && attr(sp, "channel") == "backbone" && at_ns(sp) >= at_ns(verdict))
        .map(at_ns)
        .chain(named(&spans, "rdm.node_admin.mesh.update.via-probe-verdict").into_iter().filter(|sp| attr(sp, "mesh") == f.lost && attr(sp, "node") == fp && attr(sp, "outcome") == "heard-again").map(at_ns))
        .chain(named(&spans, "rdm.mesh.fabric.update.via-status-publisher").into_iter().filter(|sp| attr(sp, "node") == fp && attr(sp, "role") == "stop" && at_ns(sp) >= at_ns(verdict)).map(at_ns))
        .min()
        .unwrap_or_else(|| panic!("{fp} heard {} again after its node-admin was created, or gave up the role", f.lost));
    let drifts: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-connection-lost").into_iter().filter(|sp| attr(sp, "primary") == fp && attr(sp, "mesh") == f.lost).collect();
    let while_unheard: Vec<&&Value> = drifts.iter().filter(|sp| at_ns(sp) >= at_ns(verdict) && at_ns(sp) < heard_again).collect();
    assert_eq!(while_unheard.len(), 1, "one attempt while {} is held unheard, for its first recovering node-admin: {while_unheard:#?}", f.lost);
    assert!(attr(while_unheard[0], "node") == format!("{}.admin.1", f.lost) && attr(while_unheard[0], "action") == "steps" && attr(while_unheard[0], "reason") == "steps", "the verdict runs node.create of the first node-admin in path order, a single step of the same Build: {:#?}", while_unheard[0]);
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().all(|sp| !attr(sp, "scope").starts_with(&format!("{}.", f.lost))), "the verdict is no proven drift: nothing of {} is repaired from an exited verdict", f.lost);

    // FabricStatus: never degraded before the verdict; degraded from it; ready again after.
    let degraded_at = statuses.iter().find(|(st, _)| st == "degraded").map(|(_, at)| *at).unwrap_or_else(|| panic!("the fabric primary showed degraded: {statuses:?}"));
    assert!(degraded_at >= at_ns(verdict) / 1_000_000 - 200, "degraded is authored at the verdict, not before: {statuses:?}");
    assert!(statuses.last().is_some_and(|(st, _)| st == "ready"), "the fabric is ready again: {statuses:?}");

    // The mark sends nothing and the fabric primary never authors the peer mesh's status; the
    // fabric's two changes are five identical sends each.
    // The marks of the story: the estate shutdown that follows it (`Fixture::stop`) stops the members of the
    // mesh one by one and leaves it unheard, which opens a new interval and a new mark at the fabric primary.
    let marks: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-unheard").into_iter().filter(|sp| attr(sp, "node") == fp && attr(sp, "mesh") == f.lost && attr(sp, "mark") == "true" && at_ns(sp) < story_end).collect();
    assert_eq!(marks.len(), 1, "the silent mark is made once before the story ends: {marks:#?}");
    assert!(unheard(marks[0]) >= rungs().mark, "the mark is at {:?} unheard, not before: {:?}", rungs().mark, unheard(marks[0]));
    // A probe in flight never holds the ladder (R-CK1: a delayed tick never moves a threshold):
    // the mark is stepped at its rung, before probe 2's, however long probe 1 runs.
    assert!(unheard(marks[0]) < rungs().probe2, "the mark is stepped at its rung, before probe 2's at {:?}, not when probe 1 ends: {:?}", rungs().probe2, unheard(marks[0]));
    let sends = named(&spans, "rdm.mesh.fabric.update.via-status-send");
    let authored_for_peer_before_verdict: Vec<&&Value> = sends.iter().filter(|sp| attr(sp, "node") == fp && attr(sp, "scope") == format!("mesh:{}", f.lost) && at_ns(sp) < at_ns(verdict)).collect();
    assert!(authored_for_peer_before_verdict.is_empty(), "the fabric primary sends no MeshStatus for {} during probing: {authored_for_peer_before_verdict:#?}", f.lost);
    let authored_for_peer: Vec<&&Value> = sends.iter().filter(|sp| attr(sp, "node") == fp && attr(sp, "scope") == format!("mesh:{}", f.lost)).collect();
    assert!(!authored_for_peer.is_empty() && authored_for_peer.iter().all(|sp| attr(sp, "status") == "degraded"), "the fabric primary authors {}'s MeshStatus degraded at the verdict and no lifecycle status: {authored_for_peer:#?}", f.lost);
    // Degraded is the fabric primary's alone. The return to ready is the primary's: the fabric
    // primary's while it holds the role, else the admin that took it (the election is the lowest ready NodeId).
    let sent_by = |status: &str, from_ms: u64, only_fp: bool| -> BTreeMap<(String, String), usize> {
        let mut by_change: BTreeMap<(String, String), usize> = BTreeMap::new();
        for sp in sends.iter().filter(|sp| attr(sp, "scope").starts_with("fabric:") && attr(sp, "status") == status && at_ns(sp) / 1_000_000 >= from_ms && (!only_fp || attr(sp, "node") == fp)) {
            *by_change.entry((attr(sp, "node"), attr(sp, "changed_at_rafka_ms"))).or_default() += 1;
        }
        by_change
    };
    let degraded_sent = sent_by("degraded", last_heard_ms, true);
    assert!(!degraded_sent.is_empty() && degraded_sent.values().all(|n| *n == 5), "degraded is one change sent five identical times: {degraded_sent:?}");
    let ready_sent = sent_by("ready", degraded_ms, !role_moved);
    assert!(!ready_sent.is_empty(), "the return to ready was sent: {ready_sent:?}");
    if role_moved {
        assert!(ready_sent.values().all(|n| (1..=5).contains(n)), "one change, sent while its author held the primary role: {ready_sent:?}");
    } else {
        assert!(ready_sent.values().all(|n| *n == 5), "ready is one change sent five identical times: {ready_sent:?}");
    }
    // Same status, fresh proof (R-S3): the new mesh primary adopts the mesh's existing status and does
    // not stay silent. Once established it re-sends the SAME fact (the old changed_at, the lifecycle
    // state did not change) five times, and the fabric stays degraded until that checklist-complete report.
    let created_at = at_ns(creates[0]) / 1_000_000;
    let status_before = mesh_status_change_before(&spans, &f.lost, fault_ms).unwrap_or_else(|| panic!("{} published its MeshStatus before the fault", f.lost));
    let republished = mesh_sends_after(&spans, &f.lost, created_at);
    assert!(!republished.is_empty(), "the new primary of {} sends the adopted MeshStatus once established (a new via-status-send for mesh:{}): none", f.lost, f.lost);
    for ((sender, changed_at), (n, _)) in &republished {
        assert!(sender.starts_with(&format!("{}.admin.", f.lost)), "the mesh's status is authored by its own primary, not {sender}");
        assert_eq!(*changed_at, status_before, "the lifecycle state did not change: the republish keeps the old changed_at ({sender}, {n} sends)");
    }
    assert!(republished.values().any(|(n, _)| *n == 5), "the republished fact is five identical sends (R-S1): {republished:?}");
    let first_republish_ms = republished.values().map(|(_, first)| *first).min().unwrap();
    let checklist_complete_ms = named(&spans, "rdm.node_admin.mesh.update.via-checklist-complete")
        .into_iter()
        .filter(|sp| attr(sp, "mesh") == f.lost && at_ns(sp) / 1_000_000 >= created_at)
        .map(|sp| at_ns(sp) / 1_000_000)
        .min()
        .expect("the new primary's checklist completed");
    assert!(first_republish_ms >= checklist_complete_ms, "the adopted status is republished only after the checklist completed: {first_republish_ms} vs {checklist_complete_ms}");
    let cleared = degraded_cleared_at(&spans, &f.lost);
    assert!(!cleared.is_empty() && cleared.iter().all(|at| *at >= checklist_complete_ms), "degraded clears only after the new primary's checklist completed, never on a ready primary of a newer birth alone: cleared at {cleared:?}, checklist complete at {checklist_complete_ms}");
    result(
        &dir,
        json!({
            "cell": cell, "fabric_id": f.estate.fabric_id, "mesh": f.lost, "mesh_id": f.lost_mesh_id, "accepted_build_id": f.accepted,
            "fabric_primary": fp, "fault_unix_ms": fault_ms, "last_heard_unix_ms": last_heard_ms,
            "probe_unheard_ms": probes.iter().map(|p| unheard(p).as_millis() as u64).collect::<Vec<_>>(), "verdict_unheard_ms": unheard(verdict).as_millis() as u64,
            "fabric_statuses": statuses, "provider": f.estate.owner.provider,
        }),
    );
}

/// CONTRACT (#2803 detection): a node-admin that is ALIVE but unheard by the fabric primary is not
/// given a new one. Every UDP path between the fabric primary's mesh and the peer mesh's node-admins is cut
/// (netfault; the fabric primary still reaches the peer mesh's rpc nodes). The fabric primary
/// pings a member, forwards `ProbeNodeState` through it, and the node-admin answers: probe 1 is
/// `admin-alive`, the investigation stops, no node-admin is created, the fabric is never degraded and no
/// second probe is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_alive_but_unheard_admin_answers_the_probe_and_the_investigation_stops() {
    let cell = "an_alive_but_unheard_admin_answers_the_probe_and_the_investigation_stops";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = fixture(cell).await;
    let side = |m: &str| -> Vec<String> { f.before.iter().filter(|n| n["mesh"] == m && n["kind"] == "node_admin").map(|n| s(&n["name"])).collect() };
    let (mine, theirs) = (side(if f.lost == "mesh1" { "mesh2" } else { "mesh1" }), side(&f.lost));
    let cut = match Partition::start(&udp_ports(&f.before, &mine), &udp_ports(&f.before, &theirs)) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot cut UDP between node-admins: {why}");
            f.estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let cut_ms = now_ms();
    let first = wait_for("probe 1 answered", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let found = f.probes(&spans).first().map(|p| (*p).clone());
        async move { found }
    })
    .await;
    assert_eq!(attr(&first, "outcome"), "admin-alive", "the node-admin answered the carried probe: {first:#?}");
    // The decision's elapsed time on from probe 1 (past probe 2 and the decision), the investigation is over.
    tokio::time::sleep(rungs().decide).await;
    let fabric = f.fabric_status().await;
    drop(cut);
    let fp = f.fabric_primary.clone();
    let story_end = f.stop().await;
    let spans = f.estate.spans();
    let probes = f.probes_before(&spans, story_end);
    assert_eq!(probes.len(), 1, "an answering node-admin stops the investigation after probe 1: {probes:#?}");
    assert!(f.admin_creates(&spans).is_empty(), "no node.create for a node-admin that answers");
    assert_eq!(fabric, "ready", "the fabric is never degraded for an admin that is alive");
    let verdicts = f.verdicts(&spans);
    assert!(verdicts.iter().any(|v| attr(v, "outcome") == "admin-alive"), "the verdict names admin-alive: {verdicts:#?}");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "cut_unix_ms": cut_ms, "probe": first["attributes"]}));
}

/// CONTRACT (#2803 detection; mesh-investigation.md step 4: "Direct backbone receipt from mesh-x at
/// any point cancels the investigation and resets the unheard interval"): the peer mesh is heard
/// again between the probes and the investigation is cancelled. Both node-admins of the peer mesh
/// are frozen (SIGSTOP: no backbone word, no exit) until after probe 1's rung and before their own
/// view passes the staleness floor, then continued: the fabric primary hears the mesh on the
/// backbone again before probe 2's rung, records the cancel as the verdict `heard-again`, sends no
/// second probe and creates nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_mesh_heard_again_between_probes_cancels_the_investigation() {
    let cell = "peer_mesh_heard_again_between_probes_cancels_the_investigation";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = fixture(cell).await;
    let admins = f.lost_admins();
    assert_eq!(admins.len(), 2, "{admins:?}");
    for (_, pid) in &admins {
        signal(*pid, "-STOP");
    }
    // When the admins are continued, from the cadence the estate runs:
    // - after probe 1's rung: the fabric primary's t was already up to one backbone interval old at
    //   the freeze, so probe 1 is asked by `probe1` after it;
    // - before the frozen admins' own view passes the staleness floor (`mark`, the shared staleness
    //   mark of step 2): each last heard a peer up to one gossip interval before the freeze, and a
    //   node that hears no other member past the floor goes on unheard after it is continued, which a
    //   resume at the mark raced;
    // so halfway between `probe1` and `mark` less one gossip interval. Heard again at most one
    //   backbone interval after the resume, the fabric primary's t is then below probe 2's rung.
    let (r, gossip, backbone) = (rungs(), rdm_core::cadence::gossip_interval(), rdm_core::cadence::backbone_gossip_interval());
    let resume = (r.probe1 + r.mark - gossip) / 2;
    assert!(r.probe1 < resume && resume + gossip < r.mark && backbone + resume + backbone < r.probe2, "the cadence leaves no resume strictly between probe 1 and the cut-off with probe 2 still ahead: {r:?}, gossip {gossip:?}, backbone {backbone:?}");
    tokio::time::sleep(resume).await;
    for (_, pid) in &admins {
        signal(*pid, "-CONT");
    }
    let verdict = wait_for("the cancel is recorded", Duration::from_secs(90), || {
        let spans = f.estate.spans();
        let found = f.verdicts(&spans).into_iter().find(|v| attr(v, "outcome") == "heard-again").cloned();
        async move { found }
    })
    .await;
    tokio::time::sleep(rungs().decide).await;
    let fabric = f.fabric_status().await;
    let fp = f.fabric_primary.clone();
    let story_end = f.stop().await;
    let spans = f.estate.spans();
    assert_eq!(f.probes_before(&spans, story_end).len(), 1, "a mesh heard again is not probed a second time: {:#?}", f.probes(&spans));
    assert!(f.admin_creates(&spans).is_empty(), "no node.create for a mesh that is heard again");
    assert_eq!(fabric, "ready");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "probes": f.probes(&spans).iter().map(|p| p["attributes"].clone()).collect::<Vec<_>>(), "verdict": verdict["attributes"]}));
}

/// CONTRACT (mesh-investigation.md step 5, "No member answers"; Q111; R-DG1): every runtime of the peer
/// mesh (admins and rpc nodes) is frozen (SIGSTOP; the host has no root to cut the mesh's UDP, the frozen
/// processes answer nothing and are not exited): both probes find no member that answers the Ping and the
/// verdict at the decision's rung is `unreachable`. The fabric-primary runs node investigation on the
/// mesh's node-admins, none is reached, and it creates the mesh's node-admin in process on that verdict;
/// the fabric is marked degraded. Must NOT happen: a node.create before the decision, a third probe, or
/// another mesh tickling the peer mesh's node-admins beside the investigation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_mesh_whose_members_all_stay_silent_gets_its_node_admin_created_on_the_investigation_verdict() {
    let cell = "a_peer_mesh_whose_members_all_stay_silent_gets_its_node_admin_created_on_the_investigation_verdict";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = fixture(cell).await;
    let all: Vec<(String, u32)> = f.lost_admins().into_iter().chain(f.lost_members()).collect();
    assert_eq!(all.len(), 5, "every runtime of {}: {all:?}", f.lost);

    for (_, pid) in &all {
        signal(*pid, "-STOP");
    }
    let verdict = wait_for("the verdict is recorded", Duration::from_secs(180), || {
        let spans = f.estate.spans();
        let found = f.verdicts(&spans).into_iter().find(|v| attr(v, "outcome") == "unreachable").cloned();
        async move { found }
    })
    .await;
    let created = wait_for("the fabric-primary creates the mesh's node-admin on the investigation's verdict", Duration::from_secs(60), || {
        let spans = f.estate.spans();
        let found = f.admin_creates(&spans).first().map(|sp| (*sp).clone());
        async move { found }
    })
    .await;
    let mut degraded = false;
    for _ in 0..60 {
        if f.fabric_status().await == "degraded" {
            degraded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    for (_, pid) in &all {
        signal(*pid, "-CONT");
    }
    let fp = f.fabric_primary.clone();
    f.stop().await;
    let spans = f.estate.spans();
    // The investigation the verdict closed made two probes (mesh-investigation.md: probe 1 "At 20 s",
    // probe 2 "At 40 s", the decision "At 60 s").
    let probes: Vec<&Value> = f.probes_before(&spans, at_ns(&verdict) + 1);
    assert_eq!(probes.len(), 2, "two probes before the verdict: {probes:#?}");
    for p in &probes {
        assert_eq!(attr(p, "outcome"), "unreachable", "{p:#?}");
    }
    assert!(at_ns(&created) >= at_ns(&verdict), "no node.create before the decision: {created:#?}");
    assert!(degraded, "the fabric is marked degraded on the verdict (R-DG1)");
    // One investigation reaches the peer mesh's node-admins: no other mesh's primary tickles them on a
    // per-member clock beside it (node-identity-and-state.md §7.3, R-CK1).
    let tickled: Vec<_> = named(&spans, "rdm.node_admin.node.update.via-true-offline")
        .into_iter()
        .filter(|sp| attr(sp, "path").starts_with(&format!("{}.admin.", f.lost)) && !attr(sp, "observer").starts_with(&format!("{}.", f.lost)))
        .collect();
    assert!(tickled.is_empty(), "no other mesh tickles {}'s node-admins beside the investigation: {tickled:#?}", f.lost);
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "verdict": verdict["attributes"], "created": created["attributes"]}));
}

/// CONTRACT (#2803 detection): a probe that finds the node-admin cut off from its members and a later
/// probe that finds it answering do not add up to a node.create: the latest probe decides. Both
/// node-admins of the peer mesh are frozen (SIGSTOP: every member's dial to them fails, so probe 1 is
/// `connection-lost`) while every UDP path between the fabric primary's mesh and the peer mesh's
/// node-admins is cut (netfault; the mesh is unheard). After probe 1 the node-admins are continued:
/// probe 2 reaches them through a member and they answer, `admin-alive`. No node.create, no degraded
/// fabric, no third probe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admin_that_answers_probe_two_stops_the_investigation_over_probe_ones_connection_lost() {
    let cell = "an_admin_that_answers_probe_two_stops_the_investigation_over_probe_ones_connection_lost";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = fixture(cell).await;
    let side = |m: &str| -> Vec<String> { f.before.iter().filter(|n| n["mesh"] == m && n["kind"] == "node_admin").map(|n| s(&n["name"])).collect() };
    let (mine, theirs) = (side(if f.lost == "mesh1" { "mesh2" } else { "mesh1" }), side(&f.lost));
    let cut = match Partition::start(&udp_ports(&f.before, &mine), &udp_ports(&f.before, &theirs)) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot cut UDP between node-admins: {why}");
            f.estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let admins = f.lost_admins();
    for (_, pid) in &admins {
        signal(*pid, "-STOP");
    }
    let first = wait_for("probe 1 is made", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let found = f.probes(&spans).first().map(|p| (*p).clone());
        async move { found }
    })
    .await;
    for (_, pid) in &admins {
        signal(*pid, "-CONT");
    }
    let second = wait_for("probe 2 is made", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let found = f.probes(&spans).get(1).map(|p| (*p).clone());
        async move { found }
    })
    .await;
    tokio::time::sleep(rungs().decide).await;
    let fabric = f.fabric_status().await;
    drop(cut);
    let fp = f.fabric_primary.clone();
    let story_end = f.stop().await;
    let spans = f.estate.spans();
    // A frozen node-admin keeps its sockets, so a member's pooled connection to it closes only when the
    // connection idles out, a staleness floor after the admin's last packet (the endpoint's idle
    // timeout): at the freeze, up to one backbone interval after the fabric-primary last heard it.
    // Probe 1 is made at two thirds of the floor unheard and reaches the members once its direct
    // contact has had its budget (two backbone intervals, `direct_budget`). Landing inside the floor
    // whatever the freeze's moment, the member still holds the pooled connection, the carried probe
    // commits over it and loses its reply (`unreachable`, `Indeterminate`); landing past the floor
    // plus one interval, the member's dial to the frozen admin fails (`carrier-edge-lost`). In
    // between (the test cadence: 2 s + 2 x 0.5 s at a 3 s floor) the freeze's moment decides, and
    // either is the member's true answer.
    let backbone = rdm_core::cadence::backbone_gossip_interval();
    let carried_at = rungs().probe1 + backbone * 2;
    let floor = rdm_core::cadence::staleness_floor();
    let held = attr(&first, "outcome") == "unreachable" && attr(&first, "detail").contains("Indeterminate");
    let lost = attr(&first, "outcome") == "carrier-edge-lost";
    if carried_at < floor {
        assert!(held, "a member still holds its pooled connection to the frozen node-admin at probe 1 (it lands at {carried_at:?}, inside the {floor:?} floor): {first:#?}");
    } else if carried_at >= floor + backbone {
        assert!(lost, "the member's pooled connection idled out before probe 1 (it lands at {carried_at:?}, past the {floor:?} floor and one {backbone:?} interval): {first:#?}");
    } else {
        assert!(held || lost, "probe 1 lands at {carried_at:?}, within one {backbone:?} interval of the {floor:?} floor: the member answers over its pooled connection (unreachable, Indeterminate) or its dial fails (carrier-edge-lost), nothing else: {first:#?}");
    }
    assert_eq!(attr(&second, "outcome"), "admin-alive", "continued, they answer the second probe: {second:#?}");
    assert_eq!(f.probes_before(&spans, story_end).len(), 2, "no third probe: {:#?}", f.probes(&spans));
    assert!(f.admin_creates(&spans).is_empty(), "the latest probe decides: no node.create");
    assert!(f.verdicts(&spans).iter().any(|v| attr(v, "outcome") == "admin-alive"));
    assert_eq!(fabric, "ready");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "probe_1": first["attributes"], "probe_2": second["attributes"]}));
}

/// CONTRACT (#2803 detection): two peer meshes that lose their node-admins at the same time are two
/// investigations. The estate has three meshes of two node-admins and two rpc nodes; the node-admins
/// of the two meshes the fabric primary does not belong to are killed together. Each mesh is
/// probed twice by its own members, each decided `connection-lost` and given its node-admin once, under its own
/// MeshId; neither is probed through the other's members.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_peer_meshes_whose_admins_are_lost_together_are_two_investigations_each_creating_its_node_admin() {
    let cell = "two_peer_meshes_whose_admins_are_lost_together_are_two_investigations_each_creating_its_node_admin";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = [("mesh1", 2, 2), ("mesh2", 2, 2), ("mesh3", 2, 2)];
    estate.reconcile_shape("fabric1", &shape).await;
    let before = values(&estate.settled_views(&shape).await);
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let fp = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let fp_mesh = fp.split('.').next().unwrap().to_string();
    let lost: Vec<String> = ["mesh1", "mesh2", "mesh3"].iter().filter(|m| **m != fp_mesh).map(|m| m.to_string()).collect();
    let mut mesh_ids = BTreeMap::new();
    for m in &lost {
        mesh_ids.insert(m.clone(), estate.mesh_view(m).await.id.expect("the mesh has its id").to_string());
    }
    estate.enter(&Estate::path(&fp)).await;
    let old = births(&before);
    for (path, pid) in estate.live_runtimes() {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if lost.iter().any(|m| name.starts_with(&format!("{m}.admin."))) {
            estate.kill_pid(pid);
        }
    }
    if lost.iter().any(|m| m == "mesh1") {
        estate.kill_bootstrap();
    }
    wait_for("both meshes have a node-admin created", Duration::from_secs(300), || {
        let (estate, old, lost) = (&estate, old.clone(), lost.clone());
        async move {
            let now = births(&estate.node_values().await);
            lost.iter().all(|m| (1..=2).any(|i| { let n = format!("{m}.admin.{i}"); now.get(&n).is_some_and(|inc| old.get(&n) != Some(inc)) })).then_some(())
        }
    })
    .await;
    let after = estate.settles_to(&shape, Duration::from_secs(120)).await;
    for m in &lost {
        assert_eq!(estate.mesh_view(m).await.id.expect("the mesh still has its id").to_string(), mesh_ids[m], "{m} keeps its own MeshId");
    }
    let _ = after;
    estate.stop().await;
    let spans = estate.spans();
    for m in &lost {
        // The fabric primary that decided the node.create probed the mesh: the role can move to a
        // created admin while the other mesh is still unheard, and its ladder starts over from the role.
        // A fabric primary that loses the role takes its ladder with it, so the next holder decides
        // again for a mesh it has not yet heard: a mesh gets its node-admin at least once, from any one
        // fabric primary at most once.
        let creates: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.create.via-recovery").into_iter().filter(|sp| attr(sp, "mesh") == *m).collect();
        assert!(!creates.is_empty(), "{m}: a node-admin created: {creates:#?}");
        for r in &creates {
            assert_eq!(creates.iter().filter(|o| attr(o, "node") == attr(r, "node")).count(), 1, "{m}: one node.create per fabric primary: {creates:#?}");
        }
        let first = creates.iter().min_by_key(|sp| at_ns(sp)).unwrap();
        let decider = attr(first, "node");
        let mut probes: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-probe").into_iter().filter(|sp| attr(sp, "node") == decider && attr(sp, "mesh") == *m && at_ns(sp) <= at_ns(first)).collect();
        probes.sort_by_key(|sp| attr(sp, "probe"));
        assert_eq!(probes.len(), 2, "{m}: two probes: {probes:#?}");
        for (i, p) in probes.iter().enumerate() {
            // A member may still hold a pooled connection to the killed node-admin: probe 1 over it
            // commits and loses its reply (`Indeterminate`, unreachable). The node.create is decided only
            // on the LATEST probe being connection-lost, so probe 2 must be.
            let stale_pool = i == 0 && attr(p, "outcome") == "unreachable" && attr(p, "detail").contains("Indeterminate");
            assert!(attr(p, "outcome") == "connection-lost" || stale_pool, "{m}: {p:#?}");
            assert!(i == 0 || attr(p, "outcome") == "connection-lost", "{m}: the latest probe decides the node.create: {p:#?}");
            assert!(attr(p, "carrier").starts_with(&format!("{m}.rpc.")), "{m} is probed through its own members only: {p:#?}");
        }
    }
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "meshes": lost, "mesh_ids": mesh_ids}));
}

/// CONTRACT (R-S3, same status, fresh proof): a peer mesh loses its primary node-admin (exact SIGKILL)
/// while its other node-admin and its rpc nodes live. The survivor is ELECTED primary; nothing is created. It
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
    let primary = f.before.iter().find(|n| n["mesh"] == f.lost.as_str() && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("the peer mesh has a primary node-admin");
    let survivor = f.before.iter().filter(|n| n["mesh"] == f.lost.as_str() && n["kind"] == "node_admin").map(|n| s(&n["name"])).find(|n| *n != primary).expect("the peer mesh has a second node-admin");
    let fault_ms = now_ms();
    let (_, pid) = f.runtimes(&format!("{}.admin.", f.lost)).into_iter().find(|(n, _)| *n == primary).unwrap_or_else(|| panic!("{primary} is running"));
    f.estate.kill_pid(pid);
    if primary == "mesh1.admin.1" {
        f.estate.kill_bootstrap();
    }
    let (lost, sv, killed) = (f.lost.clone(), survivor.clone(), primary.clone());
    wait_for("the elected primary names the killed primary as missing from its checklist", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let held = named(&spans, "rdm.node_admin.mesh.update.via-checklist-held").into_iter().any(|sp| attr(sp, "node") == sv && attr(sp, "mesh") == lost && attr(sp, "missing").contains(&killed) && at_ns(sp) / 1_000_000 >= fault_ms);
        async move { held.then_some(()) }
    })
    .await;
    // The report is its own declaration, owed once the checklist completed and sent on the declarer's
    // cadence: it follows the checklist, and the republish does not wait for it. The estate is stopped
    // only once the fabric primary of the moment has it, or the stop outruns the declaration.
    wait_for("a checklist of the mesh completes after the fault, its adopted status is republished and the mesh primary's checklist-complete report reaches the fabric primary", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let complete = named(&spans, "rdm.node_admin.mesh.update.via-checklist-complete").into_iter().filter(|sp| attr(sp, "mesh") == lost && at_ns(sp) / 1_000_000 >= fault_ms).map(|sp| at_ns(sp) / 1_000_000).min();
        let sent = !mesh_sends_after(&spans, &lost, fault_ms).is_empty();
        let reported = complete.is_some_and(|c| {
            named(&spans, "rdm.node_admin.status.update.via-declaration")
                .into_iter()
                .any(|sp| attr(&sp, "op") == "declare-mesh-state" && attr(&sp, "sender").starts_with(&format!("{lost}.admin.")) && matches!(attr(&sp, "outcome").as_str(), "applied" | "already-applied") && at_ns(&sp) / 1_000_000 >= c)
        });
        async move { (sent && reported).then_some(()) }
    })
    .await;
    let fp = f.fabric_primary.clone();
    f.estate.stop().await;
    let spans = f.estate.spans();
    let before = mesh_status_change_before(&spans, &f.lost, fault_ms).expect("the mesh published its status before the fault");
    let sends = mesh_sends_after(&spans, &f.lost, fault_ms);
    for ((sender, changed_at), _) in &sends {
        assert!(sender.starts_with(&format!("{}.admin.", f.lost)), "the mesh's status is authored by its own primary, not {sender}");
        assert_eq!(*changed_at, before, "the republish keeps the old changed_at: the lifecycle state did not change ({sender})");
    }
    let first_send_ms = sends.values().map(|(_, first)| *first).min().expect("the adopted status was republished");
    let after_fault = |name: &str, node: Option<&str>| -> Vec<u64> {
        named(&spans, name).into_iter().filter(|sp| attr(sp, "mesh") == f.lost && node.is_none_or(|n| attr(sp, "node") == n) && at_ns(sp) / 1_000_000 >= fault_ms).map(|sp| at_ns(sp) / 1_000_000).collect()
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
        .filter(|sp| attr(sp, "op") == "declare-mesh-state" && attr(sp, "sender").starts_with(&format!("{}.admin.", f.lost)) && matches!(attr(sp, "outcome").as_str(), "applied" | "already-applied"))
        .map(|sp| at_ns(sp) / 1_000_000)
        .collect();
    assert!(reports.iter().any(|at| *at >= complete_ms), "the fabric primary of the moment received a checklist-complete report from the mesh's primary after the checklist completed: {reports:?} vs {complete_ms}");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "killed_primary": primary, "elected": survivor, "republisher": republisher, "changed_at": before, "held_ms": held_ms, "complete_ms": complete_ms, "first_send_ms": first_send_ms}));
}

/// CONTRACT (R-S3, same status, fresh proof; node-recovery.md: any verdict but `reached`, `os-stopped`
/// included, is recovered at once, "Nothing holds"; Q188): a peer mesh loses both node-admins (exact
/// SIGKILL) while its rpc nodes live, and one of those rpc nodes is frozen (SIGSTOP) so it never checks
/// in. The mesh is given a node-admin under the same Build and MeshId; its new primary's checklist names
/// the frozen planned birth as missing (a named hold, no timer), republishes nothing, and the fabric
/// stays `degraded`. The frozen member is investigated `os-stopped` and re-created at its path.name; the
/// re-created birth checks in, the checklist completes, the adopted status is republished and only then
/// does the fabric clear degraded. Continued afterwards, the old birth is a deleted birth: its
/// mesh-primary reports it. What must NOT happen: degraded cleared on the ready newer-birth primary
/// alone, a republish while a planned birth is missing, the old birth taken back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_node_admin_whose_planned_member_has_not_checked_in_keeps_degraded_until_it_does() {
    let cell = "a_joined_node_admin_whose_planned_member_has_not_checked_in_keeps_degraded_until_it_does";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = fixture(cell).await;
    let frozen = format!("{}.rpc.1", f.lost);
    let frozen_id = f.before.iter().find(|n| s(&n["name"]) == frozen).map(|n| s(&n["node_id"])).unwrap_or_else(|| panic!("{frozen} in the view"));
    let (_, frozen_pid) = f.lost_members().into_iter().find(|(n, _)| *n == frozen).unwrap_or_else(|| panic!("{frozen} is running"));
    let admins = f.lost_admins();
    assert_eq!(admins.len(), 2, "both admins of {} are running: {admins:?}", f.lost);
    for (_, pid) in &admins {
        f.estate.kill_pid(*pid);
    }
    if f.lost == "mesh1" {
        f.estate.kill_bootstrap();
    }
    // Frozen after the admins are gone: a member frozen while its parent dies is killed by the
    // kernel (SIGHUP to a stopped, newly orphaned process group) and would exit, not stay silent.
    signal(frozen_pid, "-STOP");
    let lost = f.lost.clone();
    wait_for("the new primary names the frozen planned birth as missing", Duration::from_secs(240), || {
        let spans = f.estate.spans();
        let held = named(&spans, "rdm.node_admin.mesh.update.via-checklist-held").into_iter().any(|sp| attr(sp, "mesh") == lost && attr(sp, "missing").contains(&frozen));
        async move { held.then_some(()) }
    })
    .await;
    // The frozen member is recovered at once: a newer birth at its path.name, ready.
    wait_for("the frozen member is re-created at its path.name and ready", Duration::from_secs(240), || {
        let (estate, frozen, frozen_id) = (&f.estate, frozen.clone(), frozen_id.clone());
        async move {
            let nodes = estate.node_values().await;
            nodes.iter().any(|n| s(&n["name"]) == frozen && n["status"] == "ready" && s(&n["node_id"]) != frozen_id).then_some(())
        }
    })
    .await;
    wait_for("the fabric is ready again", Duration::from_secs(120), || async { (f.fabric_status().await == "ready").then_some(()) }).await;
    // Continued, the old birth comes back as a deleted birth: its mesh-primary reports it.
    signal(frozen_pid, "-CONT");
    wait_for("the continued old birth is reported as a deleted birth", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let reported = named(&spans, "rdm.node_admin.node.update.via-returned-reported").into_iter().any(|sp| attr(sp, "node_id") == frozen_id);
        async move { reported.then_some(()) }
    })
    .await;
    let fp = f.fabric_primary.clone();
    f.estate.stop().await;
    let spans = f.estate.spans();
    let created_ms = admin_created_at(&spans, &f.lost);
    let rejoined = named(&spans, "rdm.mesh.topology.update.from-node-started").into_iter().filter(|sp| attr(sp, "member") == frozen && attr(sp, "node_id") != frozen_id).map(|sp| at_ns(sp) / 1_000_000).min().expect("the re-created birth started");
    let complete = named(&spans, "rdm.node_admin.mesh.update.via-checklist-complete").into_iter().filter(|sp| attr(sp, "mesh") == f.lost && at_ns(sp) / 1_000_000 >= created_ms).map(|sp| at_ns(sp) / 1_000_000).min().expect("the new primary's checklist completed");
    assert!(complete >= rejoined, "the checklist completes only after the re-created birth checks in: {complete} vs {rejoined}");
    let republished: Vec<u64> = mesh_sends_after(&spans, &f.lost, created_ms).values().map(|(_, first)| *first).collect();
    assert!(!republished.is_empty() && republished.iter().all(|at| *at >= complete), "nothing is republished while a planned birth is missing: {republished:?} vs complete at {complete}");
    let cleared = degraded_cleared_at(&spans, &f.lost);
    assert!(!cleared.is_empty() && cleared.iter().all(|at| *at >= complete), "degraded clears only on the checklist-complete report: {cleared:?} vs {complete}");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "frozen": frozen, "frozen_node_id": frozen_id, "rejoined_ms": rejoined, "complete_ms": complete}));
}

/// The instant after which the new primary's sends of `mesh` count: the first node.create of a node-admin
/// into the mesh (`via-recovery`; `u64::MAX` before it).
fn admin_created_at(spans: &[Value], mesh: &str) -> u64 {
    named(spans, "rdm.node_admin.mesh.create.via-recovery").into_iter().filter(|sp| attr(sp, "mesh") == mesh).map(|sp| at_ns(sp) / 1_000_000).min().unwrap_or(u64::MAX)
}
