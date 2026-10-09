//! i143.e4.s10 detection acceptance (rafka-v2 #2803; fabric-node-lifecycle-events-ops-gossip.md
//! "Mesh Recovery" -> "Detection: the peer-mesh investigation"), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2803-detect-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans, feature `i143-2803-detect`).
//!
//! The estate: two meshes of two node-admins and three rpc nodes, settled through a Build. The
//! peer mesh is the one whose admins do not hold the fabric seat. Time is counted in backbone
//! rounds (`RDM_BACKBONE_INTERVAL_MS`): probe 1 at 10 rounds unheard, the silent mark at 15,
//! probe 2 at 20, the decision at 30 (20 s, 30 s, 40 s, 60 s at the 2 s default).

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

const ROUND_MS: u64 = 500;

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

fn names(meshes: &[(&str, u32, u32)]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (m, a, r) in meshes {
        out.extend((1..=*a).map(|i| format!("{m}.admin.{i}")));
        out.extend((1..=*r).map(|i| format!("{m}.rpc.{i}")));
    }
    out
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
    let shape = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 3},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 3},
    ]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    let accepted = s(&a["build_id"]);
    estate.await_attempt(&accepted, Estate::attempt_of(&a), Duration::from_secs(120)).await;
    let before = estate.settled(&names(&[("mesh1", 2, 3), ("mesh2", 2, 3)]), Duration::from_secs(30)).await;
    let fabric_primary = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let lost = if fabric_primary.starts_with("mesh1.") { "mesh2" } else { "mesh1" }.to_string();
    let (_, lost_view) = estate.get(&format!("/api/meshes/{lost}")).await;
    let (_, b0) = estate.get(&format!("/api/builds?id={accepted}")).await;
    // Control goes through the fabric primary's advertised API, which no fault here touches.
    estate.admin = before.iter().find(|n| s(&n["name"]) == fabric_primary).map(|n| s(&n["admin_api_base"])).filter(|b| !b.is_empty()).expect("the fabric primary advertises its control API");
    Fixture { estate, accepted, attempt_before: b0["attempt"].as_u64().unwrap_or(0), fabric_primary, lost, lost_mesh_id: s(&lost_view["id"]), before }
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

    fn rebirths<'a>(&self, spans: &'a [Value]) -> Vec<&'a Value> {
        named(spans, "rdm.node_admin.mesh.create.via-rebirth").into_iter().filter(|sp| attr(sp, "mesh") == self.lost && attr(sp, "node") == self.fabric_primary).collect()
    }

    async fn fabric_status(&self) -> String {
        let (_, f) = self.estate.get("/api/fabric").await;
        s(&f["status"])
    }
}

/// The changed_at of the first MeshStatus `ready-for-traffic` of `mesh` sent before `before_ms`.
fn mesh_status_change_before(spans: &[Value], mesh: &str, before_ms: u64) -> Option<String> {
    let scope = format!("mesh:{mesh}");
    named(spans, "rdm.mesh.fabric.update.via-status-send")
        .into_iter()
        .filter(|sp| attr(sp, "scope") == scope && attr(sp, "status") == "ready-for-traffic" && at_ns(sp) / 1_000_000 < before_ms)
        .min_by_key(|sp| at_ns(sp))
        .map(|sp| attr(sp, "changed_at_rafka_ms"))
}

/// The MeshStatus sends of `mesh` at or after `from_ms`, by (sender, changed_at): how many, and when the first.
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

/// When the fabric left `degraded` on a completed round, in unix ms: the holder that decided the
/// rebirth records the recovery of the mesh on its report; a successor holder that adopted
/// `degraded` clears it when every mesh primary has reported.
fn degraded_cleared_at(spans: &[Value], mesh: &str) -> Vec<u64> {
    let mut at: Vec<u64> = named(spans, "rdm.node_admin.mesh.update.via-recovered").into_iter().filter(|sp| attr(sp, "mesh") == mesh).map(|sp| at_ns(sp) / 1_000_000).collect();
    at.extend(named(spans, "rdm.node_admin.fabric.update.via-round-complete").into_iter().filter(|sp| attr(sp, "degraded_cleared") == "true").map(|sp| at_ns(sp) / 1_000_000));
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
/// At 10 rounds unheard it sends probe 1: a Ping to the members, then, through the member that
/// answered, a Forward carrying `ProbeNodeState` to the mesh's node-admin; the member cannot reach
/// its own node-admin and answers `carrier-edge-lost`. At 15 rounds the mesh is marked silent
/// (nothing is sent), at 20 rounds probe 2 prefers another member and answers the same. At 30
/// rounds the latest probe being `carrier-edge-lost` is the verdict: the fabric primary authors
/// FabricStatus `degraded` and the mesh is reborn under the same Build and the existing MeshId; the
/// fabric returns to ready when the mesh has its primary again. What must NOT happen: a rebirth
/// before 30 rounds, a new MeshId or Build id, a surviving member created again, a degraded fabric
/// before the verdict.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_mesh_without_admin_is_reborn_only_after_two_carrier_edge_lost_probes() {
    let cell = "peer_mesh_without_admin_is_reborn_only_after_two_carrier_edge_lost_probes";
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
        let (base, http, statuses) = (f.estate.admin.clone(), reqwest::Client::new(), statuses.clone());
        tokio::spawn(async move {
            loop {
                if let Ok(r) = http.get(format!("{base}/api/fabric")).send().await {
                    let v: Value = r.json().await.unwrap_or(Value::Null);
                    let st = s(&v["status"]);
                    let mut seen = statuses.lock().unwrap();
                    if seen.last().map(|(l, _)| l != &st).unwrap_or(true) {
                        seen.push((st, now_ms()));
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    wait_for("the accepted Build is complete past the fault and the lost admins are reborn", Duration::from_secs(300), || {
        let estate = &f.estate;
        let (accepted, lost_births) = (accepted.clone(), lost_births_w.clone());
        async move {
            let (_, b) = estate.get(&format!("/api/builds?id={accepted}")).await;
            let later = b["state"] == "complete" && b["attempt"].as_u64().unwrap_or(0) > attempt_before;
            let reborn = births(&estate.nodes().await).iter().filter(|(n, i)| lost_births.get(*n).is_some_and(|old| old != *i)).count() >= 1;
            (later && reborn).then_some(b)
        }
    })
    .await;
    let after = f.estate.settled(&names(&[("mesh1", 2, 3), ("mesh2", 2, 3)]), Duration::from_secs(90)).await;
    wait_for("the fabric is ready again", Duration::from_secs(30), || async { (f.fabric_status().await == "ready-for-traffic").then_some(()) }).await;
    // The sampler polls on its own cadence: stop it only once it has recorded the return to ready.
    wait_for("the sampler records the return to ready", Duration::from_secs(10), || {
        let seen = statuses.lock().unwrap().last().is_some_and(|(st, _)| st == "ready-for-traffic");
        async move { seen.then_some(()) }
    })
    .await;
    sampler.abort();
    let statuses = statuses.lock().unwrap().clone();
    // The return to ready is five sends a second apart, from the fabric primary for as long as it
    // holds the seat: the reborn mesh can take the seat (the election is the lowest ready NodeId), and
    // a publisher that loses the role ends the sends still to come.
    let degraded_ms = statuses.iter().find(|(st, _)| st == "degraded").map(|(_, at)| *at).unwrap_or(0);
    let fp_for_sends = f.fabric_primary.clone();
    let seat_lost_or_five = |spans: &[Value]| {
        let sent = named(spans, "rdm.mesh.fabric.update.via-status-send")
            .into_iter()
            .filter(|sp| attr(sp, "node") == fp_for_sends && attr(sp, "scope").starts_with("fabric:") && attr(sp, "status") == "ready-for-traffic" && at_ns(sp) / 1_000_000 > degraded_ms)
            .count();
        let any_sent = named(spans, "rdm.mesh.fabric.update.via-status-send").into_iter().any(|sp| attr(sp, "scope").starts_with("fabric:") && attr(sp, "status") == "ready-for-traffic" && at_ns(sp) / 1_000_000 > degraded_ms);
        let seat_lost = any_sent && named(spans, "rdm.mesh.fabric.update.via-status-publisher").into_iter().any(|sp| attr(sp, "node") == fp_for_sends && attr(sp, "role") == "stop" && at_ns(sp) / 1_000_000 > degraded_ms);
        (sent, seat_lost)
    };
    wait_for("the return to ready is sent five times or the seat moves", Duration::from_secs(30), || {
        let (sent, seat_lost) = seat_lost_or_five(&f.estate.spans());
        async move { (sent >= 5 || seat_lost).then_some(()) }
    })
    .await;
    let seat_moved = seat_lost_or_five(&f.estate.spans()).1;
    let (_, fabric_after) = f.estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["build_id"]), f.accepted, "no new Build: Fabric.build_id is unchanged");
    let (_, lost_after) = f.estate.get(&format!("/api/meshes/{}", f.lost)).await;
    assert_eq!(s(&lost_after["id"]), f.lost_mesh_id, "{} is reborn under its own MeshId", f.lost);
    let new_births = births(&after);
    for m in &survivors {
        assert_eq!(new_births[m], before_births[m], "{m} answered the probes and is never created again");
    }

    let fp = f.fabric_primary.clone();
    let fabric_admin = f.estate.admin.clone();
    f.estate.admin = fabric_admin;
    f.estate.stop().await;
    let spans = f.estate.spans();
    let last_heard_ms = f.unheard_since_ms(&spans).unwrap_or_else(|| panic!("{fp} tracked {} as unheard (via-unheard names the last backbone receipt)", f.lost));
    let round = |sp: &Value| (at_ns(sp) / 1_000_000).saturating_sub(last_heard_ms) / ROUND_MS;

    let probes = f.probes(&spans);
    assert_eq!(probes.len(), 2, "two probes, no more: {probes:#?}");
    for (i, p) in probes.iter().enumerate() {
        assert_eq!(attr(p, "probe"), (i + 1).to_string(), "probes are numbered in order");
        // A member may still hold a pooled connection to the killed node-admin: a probe over it
        // commits and loses its reply (`Indeterminate`, unreachable). Its next dial fails at the dial,
        // which is the edge-lost answer. Probe 2 is the one that decides.
        let stale_pool = i == 0 && attr(p, "outcome") == "unreachable" && attr(p, "detail") == "Indeterminate";
        assert!(attr(p, "outcome") == "carrier-edge-lost" || stale_pool, "the member cannot reach its own node-admin: {p:#?}");
        assert!(attr(p, "member").split(',').all(|m| survivors.contains(&m.to_string())) && survivors.contains(&attr(p, "carrier")), "members of {} were asked and one carried it: {p:#?}", f.lost);
    }
    assert!(round(probes[0]) >= 10, "probe 1 fires at 10 rounds unheard, not before: round {}", round(probes[0]));
    assert!(round(probes[1]) >= 20, "probe 2 fires at 20 rounds unheard, not before: round {}", round(probes[1]));
    assert_ne!(attr(probes[0], "carrier"), attr(probes[1], "carrier"), "probe 2 prefers another carrier");

    let verdicts = f.verdicts(&spans);
    let rebirth_verdict = verdicts.iter().find(|v| attr(v, "outcome") == "carrier-edge-lost").unwrap_or_else(|| panic!("the verdict is carrier-edge-lost: {verdicts:#?}"));
    assert!(round(rebirth_verdict) >= 30, "the decision is at 30 rounds, not before: round {}", round(rebirth_verdict));
    let rebirths = f.rebirths(&spans);
    assert_eq!(rebirths.len(), 1, "one rebirth: {rebirths:#?}");
    assert!(at_ns(rebirths[0]) >= at_ns(rebirth_verdict), "the verdict is recorded before the rebirth opens");

    // FabricStatus: never degraded before the verdict; degraded from it; ready again after.
    let degraded_at = statuses.iter().find(|(st, _)| st == "degraded").map(|(_, at)| *at).unwrap_or_else(|| panic!("the fabric primary showed degraded: {statuses:?}"));
    assert!(degraded_at >= at_ns(rebirth_verdict) / 1_000_000 - 200, "degraded is authored at the verdict, not before: {statuses:?}");
    assert!(statuses.last().is_some_and(|(st, _)| st == "ready-for-traffic"), "the fabric is ready again: {statuses:?}");

    // The mark sends nothing and the fabric primary never authors the peer mesh's status; the
    // fabric's two changes are five identical sends each.
    let marks: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-unheard").into_iter().filter(|sp| attr(sp, "node") == fp && attr(sp, "mesh") == f.lost && attr(sp, "mark") == "true").collect();
    assert_eq!(marks.len(), 1, "the silent mark is made once: {marks:#?}");
    assert!(round(marks[0]) >= 15, "the mark is at 15 rounds, not before: {}", round(marks[0]));
    let sends = named(&spans, "rdm.mesh.fabric.update.via-status-send");
    let authored_for_peer: Vec<&&Value> = sends.iter().filter(|sp| attr(sp, "node") == fp && attr(sp, "scope") == format!("mesh:{}", f.lost)).collect();
    assert!(authored_for_peer.is_empty(), "the fabric primary never authors {}'s MeshStatus: {authored_for_peer:#?}", f.lost);
    // Degraded is the fabric primary's alone. The return to ready is the seat holder's: the fabric
    // primary's while it holds the seat, else the admin that took it (the election is the lowest ready NodeId).
    let sent_by = |status: &str, from_ms: u64, only_fp: bool| -> BTreeMap<(String, String), usize> {
        let mut by_change: BTreeMap<(String, String), usize> = BTreeMap::new();
        for sp in sends.iter().filter(|sp| attr(sp, "scope").starts_with("fabric:") && attr(sp, "status") == status && at_ns(sp) / 1_000_000 >= from_ms && (!only_fp || attr(sp, "node") == fp)) {
            *by_change.entry((attr(sp, "node"), attr(sp, "changed_at_rafka_ms"))).or_default() += 1;
        }
        by_change
    };
    let degraded_sent = sent_by("degraded", last_heard_ms, true);
    assert!(!degraded_sent.is_empty() && degraded_sent.values().all(|n| *n == 5), "degraded is one change sent five identical times: {degraded_sent:?}");
    let ready_sent = sent_by("ready-for-traffic", degraded_ms, !seat_moved);
    assert!(!ready_sent.is_empty(), "the return to ready was sent: {ready_sent:?}");
    if seat_moved {
        assert!(ready_sent.values().all(|n| (1..=5).contains(n)), "one change, sent while its author held the seat: {ready_sent:?}");
    } else {
        assert!(ready_sent.values().all(|n| *n == 5), "ready is one change sent five identical times: {ready_sent:?}");
    }
    // Same status, fresh proof (R-S3): the reborn primary adopts the mesh's existing status and does
    // not stay silent. After the rebirth it re-sends the SAME fact (the old changed_at, the lifecycle
    // state did not change) five times, and the fabric stays degraded until that round-complete report.
    let reborn_at = at_ns(rebirths[0]) / 1_000_000;
    let status_before = mesh_status_change_before(&spans, &f.lost, fault_ms).unwrap_or_else(|| panic!("{} published its MeshStatus before the fault", f.lost));
    let republished = mesh_sends_after(&spans, &f.lost, reborn_at);
    assert!(!republished.is_empty(), "the reborn primary of {} sends the adopted MeshStatus after the rebirth (a new via-status-send for mesh:{}): none", f.lost, f.lost);
    for ((sender, changed_at), (n, _)) in &republished {
        assert!(sender.starts_with(&format!("{}.admin.", f.lost)), "the mesh's status is authored by its own primary, not {sender}");
        assert_eq!(*changed_at, status_before, "the lifecycle state did not change: the republish keeps the old changed_at ({sender}, {n} sends)");
    }
    assert!(republished.values().any(|(n, _)| *n == 5), "the republished fact is five identical sends (R-S1): {republished:?}");
    let first_republish_ms = republished.values().map(|(_, first)| *first).min().unwrap();
    let round_complete_ms = named(&spans, "rdm.node_admin.mesh.update.via-round-complete")
        .into_iter()
        .filter(|sp| attr(sp, "mesh") == f.lost && at_ns(sp) / 1_000_000 >= reborn_at)
        .map(|sp| at_ns(sp) / 1_000_000)
        .min()
        .expect("the reborn primary's round completed");
    assert!(first_republish_ms >= round_complete_ms, "the adopted status is republished only after the round completed: {first_republish_ms} vs {round_complete_ms}");
    let recovered = degraded_cleared_at(&spans, &f.lost);
    assert!(!recovered.is_empty() && recovered.iter().all(|at| *at >= round_complete_ms), "degraded clears only after the reborn primary's round completed, never on a ready primary of a newer birth alone: recovered at {recovered:?}, round complete at {round_complete_ms}");
    result(
        &dir,
        json!({
            "cell": cell, "fabric_id": f.estate.fabric_id, "mesh": f.lost, "mesh_id": f.lost_mesh_id, "accepted_build_id": f.accepted,
            "fabric_primary": fp, "fault_unix_ms": fault_ms, "last_heard_unix_ms": last_heard_ms,
            "probe_rounds": probes.iter().map(|p| round(p)).collect::<Vec<_>>(), "verdict_round": round(rebirth_verdict),
            "fabric_statuses": statuses, "provider": f.estate.owner.provider,
        }),
    );
}

/// CONTRACT (#2803 detection): a node-admin that is ALIVE but unheard by the fabric primary is not
/// reborn. Every UDP path between the fabric primary's mesh and the peer mesh's node-admins is cut
/// (netfault; the fabric primary still reaches the peer mesh's rpc nodes). The fabric primary
/// pings a member, forwards `ProbeNodeState` through it, and the node-admin answers: probe 1 is
/// `admin-alive`, the investigation stops, nothing is reborn, the fabric is never degraded and no
/// second probe is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alive_but_unheard_admin_answers_the_probe_and_is_not_reborn() {
    let cell = "alive_but_unheard_admin_answers_the_probe_and_is_not_reborn";
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
    // 40 rounds on (past probe 2 and the decision), the investigation is over.
    tokio::time::sleep(Duration::from_millis(ROUND_MS * 40)).await;
    let fabric = f.fabric_status().await;
    drop(cut);
    let fp = f.fabric_primary.clone();
    f.estate.stop().await;
    let spans = f.estate.spans();
    let probes = f.probes(&spans);
    assert_eq!(probes.len(), 1, "an answering node-admin stops the investigation after probe 1: {probes:#?}");
    assert!(f.rebirths(&spans).is_empty(), "no rebirth of a node-admin that answers");
    assert_eq!(fabric, "ready-for-traffic", "the fabric is never degraded for an admin that is alive");
    let verdicts = f.verdicts(&spans);
    assert!(verdicts.iter().any(|v| attr(v, "outcome") == "admin-alive"), "the verdict names admin-alive: {verdicts:#?}");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "cut_unix_ms": cut_ms, "probe": first["attributes"]}));
}

/// CONTRACT (#2803 detection): the peer mesh is heard again between the probes and the
/// investigation is cancelled. Both node-admins of the peer mesh are frozen (SIGSTOP: no backbone
/// word, no exit) for 11 rounds, then continued before probe 2: the fabric primary hears the
/// mesh on the backbone again, records the cancel as the verdict `heard-again`, sends no second
/// probe and rebirths nothing.
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
    // Probe 1 is due at 10 rounds unheard and takes as long as the dial to a frozen node-admin
    // does; the mesh is continued at 11 rounds, between probe 1 and probe 2 (20 rounds).
    tokio::time::sleep(Duration::from_millis(ROUND_MS * 11)).await;
    for (_, pid) in &admins {
        signal(*pid, "-CONT");
    }
    let verdict = wait_for("the cancel is recorded", Duration::from_secs(90), || {
        let spans = f.estate.spans();
        let found = f.verdicts(&spans).into_iter().find(|v| attr(v, "outcome") == "heard-again").cloned();
        async move { found }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(ROUND_MS * 40)).await;
    let fabric = f.fabric_status().await;
    let fp = f.fabric_primary.clone();
    f.estate.stop().await;
    let spans = f.estate.spans();
    assert_eq!(f.probes(&spans).len(), 1, "a mesh heard again is not probed a second time: {:#?}", f.probes(&spans));
    assert!(f.rebirths(&spans).is_empty(), "no rebirth of a mesh that is heard again");
    assert_eq!(fabric, "ready-for-traffic");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "probes": f.probes(&spans).iter().map(|p| p["attributes"].clone()).collect::<Vec<_>>(), "verdict": verdict["attributes"]}));
}

/// CONTRACT (#2803 detection): no member answers, so nothing is reborn. Every runtime of the peer
/// mesh (admins and rpc nodes) is frozen (SIGSTOP; the host has no root to cut the mesh's UDP, the
/// frozen processes answer nothing and are not exited): both probes find no member that answers
/// the Ping, the verdict at 30 rounds is `unreachable`, and the fabric primary holds. No rebirth,
/// no degraded fabric. After the mesh is continued it is heard again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_mesh_whose_members_all_stay_silent_is_held_not_reborn() {
    let cell = "peer_mesh_whose_members_all_stay_silent_is_held_not_reborn";
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
    tokio::time::sleep(Duration::from_millis(ROUND_MS * 20)).await;
    let fabric = f.fabric_status().await;
    for (_, pid) in &all {
        signal(*pid, "-CONT");
    }
    let fp = f.fabric_primary.clone();
    f.estate.stop().await;
    let spans = f.estate.spans();
    let probes = f.probes(&spans);
    assert_eq!(probes.len(), 2, "two probes: {probes:#?}");
    for p in &probes {
        assert_eq!(attr(p, "outcome"), "unreachable", "{p:#?}");
    }
    assert!(f.rebirths(&spans).is_empty(), "timeout or unreachable alone grants no rebirth");
    assert_eq!(fabric, "ready-for-traffic", "no degraded fabric on a hold");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "verdict": verdict["attributes"]}));
}

/// CONTRACT (#2803 detection): a probe that finds the node-admin cut off from its members and a later
/// probe that finds it answering do not add up to a rebirth: the latest probe decides. Both
/// node-admins of the peer mesh are frozen (SIGSTOP: every member's dial to them fails, so probe 1 is
/// `carrier-edge-lost`) while every UDP path between the fabric primary's mesh and the peer mesh's
/// node-admins is cut (netfault; the mesh is unheard). After probe 1 the node-admins are continued:
/// probe 2 reaches them through a member and they answer, `admin-alive`. No rebirth, no degraded
/// fabric, no third probe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admin_that_answers_probe_two_is_not_reborn_for_probe_ones_edge_lost() {
    let cell = "an_admin_that_answers_probe_two_is_not_reborn_for_probe_ones_edge_lost";
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
    tokio::time::sleep(Duration::from_millis(ROUND_MS * 40)).await;
    let fabric = f.fabric_status().await;
    drop(cut);
    let fp = f.fabric_primary.clone();
    f.estate.stop().await;
    let spans = f.estate.spans();
    assert_eq!(attr(&first, "outcome"), "carrier-edge-lost", "frozen node-admins cannot be reached by their members: {first:#?}");
    assert_eq!(attr(&second, "outcome"), "admin-alive", "continued, they answer the second probe: {second:#?}");
    assert_eq!(f.probes(&spans).len(), 2, "no third probe");
    assert!(f.rebirths(&spans).is_empty(), "the latest probe decides: no rebirth");
    assert!(f.verdicts(&spans).iter().any(|v| attr(v, "outcome") == "admin-alive"));
    assert_eq!(fabric, "ready-for-traffic");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "probe_1": first["attributes"], "probe_2": second["attributes"]}));
}

/// CONTRACT (#2803 detection): two peer meshes that lose their node-admins at the same time are two
/// investigations. The estate has three meshes of two node-admins and two rpc nodes; the node-admins
/// of the two meshes the fabric primary does not belong to are killed together. Each mesh is
/// probed twice by its own members, each decided `carrier-edge-lost` and reborn once, under its own
/// MeshId; neither is probed through the other's members.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_peer_meshes_lost_together_are_investigated_and_reborn_independently() {
    let cell = "two_peer_meshes_lost_together_are_investigated_and_reborn_independently";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 2},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 2},
        {"name": "mesh3", "node_admin": 2, "rpc_node": 2},
    ]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    let accepted = s(&a["build_id"]);
    estate.await_attempt(&accepted, Estate::attempt_of(&a), Duration::from_secs(180)).await;
    let want = names(&[("mesh1", 2, 2), ("mesh2", 2, 2), ("mesh3", 2, 2)]);
    let before = estate.settled(&want, Duration::from_secs(60)).await;
    let fp = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let fp_mesh = fp.split('.').next().unwrap().to_string();
    let lost: Vec<String> = ["mesh1", "mesh2", "mesh3"].iter().filter(|m| **m != fp_mesh).map(|m| m.to_string()).collect();
    let mut mesh_ids = BTreeMap::new();
    for m in &lost {
        let (_, v) = estate.get(&format!("/api/meshes/{m}")).await;
        mesh_ids.insert(m.clone(), s(&v["id"]));
    }
    estate.admin = before.iter().find(|n| s(&n["name"]) == fp).map(|n| s(&n["admin_api_base"])).filter(|b| !b.is_empty()).expect("the fabric primary advertises its control API");
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
    wait_for("both meshes are reborn", Duration::from_secs(300), || {
        let (estate, old, lost) = (&estate, old.clone(), lost.clone());
        async move {
            let now = births(&estate.nodes().await);
            lost.iter().all(|m| (1..=2).any(|i| { let n = format!("{m}.admin.{i}"); now.get(&n).is_some_and(|inc| old.get(&n) != Some(inc)) })).then_some(())
        }
    })
    .await;
    let after = estate.settled(&want, Duration::from_secs(120)).await;
    for m in &lost {
        let (_, v) = estate.get(&format!("/api/meshes/{m}")).await;
        assert_eq!(s(&v["id"]), mesh_ids[m], "{m} is reborn under its own MeshId");
    }
    let _ = after;
    let fabric_admin = estate.admin.clone();
    estate.admin = fabric_admin;
    estate.stop().await;
    let spans = estate.spans();
    for m in &lost {
        let mut probes: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-probe").into_iter().filter(|sp| attr(sp, "node") == fp && attr(sp, "mesh") == *m).collect();
        probes.sort_by_key(|sp| attr(sp, "probe"));
        assert_eq!(probes.len(), 2, "{m}: two probes: {probes:#?}");
        for (i, p) in probes.iter().enumerate() {
            // A member may still hold a pooled connection to the killed node-admin: probe 1 over it
            // commits and loses its reply (`Indeterminate`, unreachable). The rebirth is decided only
            // on the LATEST probe being carrier-edge-lost, so probe 2 must be.
            let stale_pool = i == 0 && attr(p, "outcome") == "unreachable" && attr(p, "detail") == "Indeterminate";
            assert!(attr(p, "outcome") == "carrier-edge-lost" || stale_pool, "{m}: {p:#?}");
            assert!(i == 0 || attr(p, "outcome") == "carrier-edge-lost", "{m}: the latest probe decides the rebirth: {p:#?}");
            assert!(attr(p, "carrier").starts_with(&format!("{m}.rpc.")), "{m} is probed through its own members only: {p:#?}");
        }
        let rebirths: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.create.via-rebirth").into_iter().filter(|sp| attr(sp, "node") == fp && attr(sp, "mesh") == *m).collect();
        assert_eq!(rebirths.len(), 1, "{m}: reborn once: {rebirths:#?}");
    }
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "meshes": lost, "mesh_ids": mesh_ids}));
}

/// CONTRACT (R-S3, same status, fresh proof): a peer mesh loses its primary node-admin (exact SIGKILL)
/// while its other node-admin and its rpc nodes live. The survivor is ELECTED primary, not reborn. It
/// adopts the mesh's existing status without inventing a transition and re-sends the round's down op to
/// its planned births. The killed primary is still a planned birth of the accepted Build, so the
/// elected primary's checklist names it as missing (a named hold, no timer) and republishes nothing.
/// The Build restarts the killed birth; when every planned birth has checked in the round completes
/// and the adopted fact is republished: the same status, the OLD changed_at, five identical sends,
/// and the mesh primary's Mesh declaration to the fabric primary (the fresh round-complete report)
/// follows the completed round. What must NOT happen: silence after the election with no named
/// hold, a republish before the round completes, a new changed_at.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elected_primary_holds_its_round_for_the_killed_primary_then_republishes_the_old_status() {
    let cell = "elected_primary_holds_its_round_for_the_killed_primary_then_republishes_the_old_status";
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
    wait_for("the elected primary names the killed primary as missing from its round", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let held = named(&spans, "rdm.node_admin.mesh.update.via-round-held").into_iter().any(|sp| attr(sp, "node") == sv && attr(sp, "mesh") == lost && attr(sp, "missing").contains(&killed) && at_ns(sp) / 1_000_000 >= fault_ms);
        async move { held.then_some(()) }
    })
    .await;
    // The report is its own declaration, owed once the round completed and sent on the declarer's
    // cadence: it follows the round, and the republish does not wait for it. The estate is stopped
    // only once the fabric primary of the moment has it, or the stop outruns the declaration.
    wait_for("a round of the mesh completes after the fault, its adopted status is republished and the mesh primary's round-complete report reaches the fabric primary", Duration::from_secs(120), || {
        let spans = f.estate.spans();
        let complete = named(&spans, "rdm.node_admin.mesh.update.via-round-complete").into_iter().filter(|sp| attr(sp, "mesh") == lost && at_ns(sp) / 1_000_000 >= fault_ms).map(|sp| at_ns(sp) / 1_000_000).min();
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
    let held = after_fault("rdm.node_admin.mesh.update.via-round-held", Some(&survivor));
    let down = after_fault("rdm.node_admin.mesh.update.via-round-down", Some(&survivor));
    let complete = after_fault("rdm.node_admin.mesh.update.via-round-complete", None);
    let (held_ms, complete_ms) = (*held.iter().min().expect("the elected primary held its round"), *complete.iter().min().expect("a round completed"));
    assert!(!down.is_empty(), "the elected primary re-sent the round's down op to its planned births");
    assert!(held_ms <= complete_ms && complete_ms <= first_send_ms, "hold ({held_ms}) -> checklist complete ({complete_ms}) -> republish ({first_send_ms}), in that order: nothing is republished while a planned birth is missing");
    let republisher = sends.keys().map(|(sender, _)| sender.clone()).next().unwrap();
    let reports: Vec<u64> = named(&spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|sp| attr(sp, "op") == "declare-mesh-state" && attr(sp, "sender").starts_with(&format!("{}.admin.", f.lost)) && matches!(attr(sp, "outcome").as_str(), "applied" | "already-applied"))
        .map(|sp| at_ns(sp) / 1_000_000)
        .collect();
    assert!(reports.iter().any(|at| *at >= complete_ms), "the fabric primary of the moment received a round-complete report from the mesh's primary after the round completed: {reports:?} vs {complete_ms}");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "killed_primary": primary, "elected": survivor, "republisher": republisher, "changed_at": before, "held_ms": held_ms, "complete_ms": complete_ms, "first_send_ms": first_send_ms}));
}

/// CONTRACT (R-S3, same status, fresh proof): a peer mesh loses both node-admins (exact SIGKILL) while
/// its rpc nodes live, and one of those rpc nodes is frozen (SIGSTOP) so it never checks in. The mesh is
/// reborn under the same Build and MeshId; its reborn primary is ready in the fabric's view, a ready
/// primary of a newer birth. That is not the proof: the reborn primary's checklist names the frozen
/// planned birth as missing (a named hold, no timer), republishes nothing, and the fabric stays
/// `degraded`. After the member is continued it checks in, the round completes, the adopted status is
/// republished and only then does the fabric clear degraded. What must NOT happen: degraded cleared on
/// the ready newer-birth primary alone, a republish while a planned birth is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reborn_mesh_with_a_planned_member_that_has_not_checked_in_holds_degraded_until_it_does() {
    let cell = "reborn_mesh_with_a_planned_member_that_has_not_checked_in_holds_degraded_until_it_does";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut f = fixture(cell).await;
    let frozen = format!("{}.rpc.1", f.lost);
    let lost_births: BTreeMap<String, String> = births(&f.before).into_iter().filter(|(n, _)| n.starts_with(&format!("{}.admin.", f.lost))).collect();
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
    wait_for("the reborn primary names the frozen planned birth as missing", Duration::from_secs(240), || {
        let spans = f.estate.spans();
        let held = named(&spans, "rdm.node_admin.mesh.update.via-round-held").into_iter().any(|sp| attr(sp, "mesh") == lost && attr(sp, "missing").contains(&frozen));
        async move { held.then_some(()) }
    })
    .await;
    // The stale-ready condition: the fabric's view holds a ready primary of the mesh from a newer birth.
    wait_for("the mesh shows a ready primary of a newer birth", Duration::from_secs(120), || {
        let (estate, lost_births, lost) = (&f.estate, lost_births.clone(), lost.clone());
        async move {
            let nodes = estate.nodes().await;
            nodes.iter().any(|n| n["mesh"] == lost.as_str() && n["kind"] == "node_admin" && n["is_primary"] == true && n["status"] == "ready-for-traffic" && lost_births.get(&s(&n["name"])).is_some_and(|old| *old != s(&n["incarnation_id"]))).then_some(())
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(ROUND_MS * 10)).await;
    let held_status = f.fabric_status().await;
    let held_spans = f.estate.spans();
    let sent_while_held = mesh_sends_after(&held_spans, &f.lost, rebirth_start(&held_spans, &f.lost));
    let recovered_while_held = named(&held_spans, "rdm.node_admin.mesh.update.via-recovered").into_iter().any(|sp| attr(sp, "mesh") == f.lost);
    let cont_ms = now_ms();
    signal(frozen_pid, "-CONT");
    wait_for("the fabric is ready again", Duration::from_secs(120), || async { (f.fabric_status().await == "ready-for-traffic").then_some(()) }).await;
    let fp = f.fabric_primary.clone();
    f.estate.stop().await;
    let spans = f.estate.spans();
    assert_eq!(held_status, "degraded", "a ready primary of a newer birth does not clear degraded while a planned birth ({frozen}) has not checked in");
    assert!(!recovered_while_held, "the recovery is not recorded while the round is held");
    assert!(sent_while_held.is_empty(), "nothing is republished while a planned birth is missing: {sent_while_held:?}");
    let republished: Vec<u64> = mesh_sends_after(&spans, &f.lost, rebirth_start(&spans, &f.lost)).values().map(|(_, first)| *first).collect();
    assert!(!republished.is_empty() && republished.iter().all(|at| *at >= cont_ms), "the adopted status is republished only after {frozen} checks in: {republished:?} vs continued at {cont_ms}");
    let reborn_ms = rebirth_start(&spans, &f.lost);
    let complete = named(&spans, "rdm.node_admin.mesh.update.via-round-complete").into_iter().filter(|sp| attr(sp, "mesh") == f.lost && at_ns(sp) / 1_000_000 >= reborn_ms).map(|sp| at_ns(sp) / 1_000_000).min().expect("the reborn primary's round completed");
    assert!(complete >= cont_ms, "the checklist completes only after the frozen member checks in: {complete} vs {cont_ms}");
    let recovered = degraded_cleared_at(&spans, &f.lost);
    assert!(!recovered.is_empty() && recovered.iter().all(|at| *at >= complete), "degraded clears only on the round-complete report: {recovered:?} vs {complete}");
    result(&dir, json!({"cell": cell, "fabric_primary": fp, "mesh": f.lost, "frozen": frozen, "continued_ms": cont_ms, "complete_ms": complete}));
}

/// The instant after which a rebirth's sends of `mesh` count: the first `via-rebirth` of the mesh (0 before it).
fn rebirth_start(spans: &[Value], mesh: &str) -> u64 {
    named(spans, "rdm.node_admin.mesh.create.via-rebirth").into_iter().filter(|sp| attr(sp, "mesh") == mesh).map(|sp| at_ns(sp) / 1_000_000).min().unwrap_or(u64::MAX)
}
