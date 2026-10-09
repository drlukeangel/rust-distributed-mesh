//! i143.e6.s14 acceptance (rafka-v2 #2902, hardened 2026-10-07), CHAOS-PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2902-chaos-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans land under it, feature `i143-2902`, test the
//! cell's name) at the test cadence (staleness 3 s, gossip 500 ms).
//!
//! The estate: two meshes of two node-admins and two rpc nodes each, settled through a Build. All
//! UDP between the two meshes' processes is dropped (`iptables`, `crate::netfault::Partition`); no
//! process is cut from a member of its own mesh. The held-member repair of `gossip.md` §6 is read
//! from the `rdm.mesh.connection.update.via-refeed` spans (`reason = stale-held-member`) and from
//! the views of every admin.

use rafka_test_scenario::elections::{advertised_fabric_primaries, advertised_primaries, seats_as_expected};
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CELL: &str = "partitioned_meshes_rejoin_with_neighbors_preserve_live_births";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2902".into(),
        subfeature: "held-member-repair".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: CELL.into(),
    }
}

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2902/chaos-process").join(CELL),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

fn num(sp: &Value, k: &str) -> u64 {
    attr(sp, k).parse().unwrap_or(0)
}

fn alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

fn live(nodes: &[Value]) -> Vec<Value> {
    nodes.iter().filter(|n| !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))).cloned().collect()
}

/// A repair attempt on a held member whose coverage was stale, after `since`.
fn attempts(spans: &[Value], since: u64) -> Vec<&Value> {
    let mut v: Vec<&Value> = named(spans, "rdm.mesh.connection.update.via-refeed").into_iter().filter(|sp| at(sp) >= since && attr(sp, "reason") == "stale-held-member").collect();
    v.sort_by_key(|sp| at(sp));
    v
}

fn fallbacks(spans: &[Value], since: u64) -> Vec<&Value> {
    named(spans, "rdm.mesh.connection.update.via-refeed").into_iter().filter(|sp| at(sp) >= since && attr(sp, "reason") == "no-neighbours-fallback").collect()
}

/// Attempts per (observer, peer node).
fn per_pair(att: &[&Value]) -> BTreeMap<(String, String), usize> {
    let mut m = BTreeMap::new();
    for sp in att {
        *m.entry((attr(sp, "node"), attr(sp, "peer_node"))).or_default() += 1;
    }
    m
}

/// Every (admin, admin of the other mesh) pair has been attempted at least `n` times in `att`.
fn every_cross_admin_pair_attempted(att: &[&Value], admins: &[(String, String)], n: usize) -> bool {
    let m = per_pair(att);
    admins.iter().all(|(a, am)| admins.iter().filter(|(_, bm)| bm != am).all(|(b, _)| m.get(&(a.clone(), b.clone())).copied().unwrap_or(0) >= n))
}

fn attempt_json(sp: &Value) -> Value {
    json!({
        "start_unix_nano": at(sp), "node": attr(sp, "node"), "channel": attr(sp, "channel"), "peer": attr(sp, "peer"), "peer_node": attr(sp, "peer_node"),
        "peer_node_id": attr(sp, "peer_node_id"), "coverage_age_ms": num(sp, "coverage_age_ms"), "joined": attr(sp, "joined"),
        "trace_id": sp["trace_id"], "span_id": sp["span_id"],
    })
}

/// Spans that mark a death, a tombstone, a departure, a rebirth or a provider terminate.
fn lifecycle_marks(spans: &[Value], from: u64, to: u64) -> Vec<String> {
    spans
        .iter()
        .filter(|sp| at(sp) >= from && at(sp) <= to)
        .map(|sp| s(&sp["name"]))
        .filter(|n| {
            n.contains(".node.delete.")
                || n.contains(".deployment.delete.")
                || n.contains(".deployment.update.")
                || n.contains("proven-drift")
                || n.contains("membership.remove.")
                || n.contains("membership.reject.via-departed-birth")
                || n.contains("runtime.update.via-adopt")
                || n.contains("node.create.via-deployment")
        })
        .collect()
}

/// Every pre-cut member is live and ready in the view of `base`.
fn holds_all(view: &[Value], everyone: &[String]) -> bool {
    let view = live(view);
    everyone.iter().all(|name| view.iter().any(|n| n["name"] == name.as_str() && n["status"] == "ready-for-traffic"))
}

async fn drop_cut(partition: Partition) -> u64 {
    drop(partition);
    now_ns()
}

/// CONTRACT (#2902, gossip.md §6): two meshes that stop hearing each other, each keeping its own
/// neighbours, hear each other again. While every UDP path between the meshes is dropped (no
/// process is cut from a member of its own mesh), each node-admin hands each peer-mesh node-admin
/// to its backbone channel again once per repair window, naming the peer, the coverage age and
/// the join result, with the neighbours it still has: no mesh channel falls back to the
/// no-neighbour refeed, and no own-mesh member goes silent. What must NOT happen: a hot loop on a
/// peer (more attempts than windows), a healthy or own-mesh member re-fed, any member seen
/// `dead`, any death, tombstone, departure, rebirth or provider terminate (every process lives
/// on with its NodeId, incarnation and pid), or a retired birth named by a later attempt. After
/// the cut is dropped every admin holds every pre-cut member ready, the same seats and the same
/// fabric primary, and every node hears the peer mesh again. A rpc node then retired through a
/// Build leaves the held set (its departure is the one lifecycle effect of the run, for exactly
/// that birth) and a second cut, whose attempts on the peer mesh's node-admins complete whole
/// repair windows, never names its NodeId. One non-primary node-admin of the peer mesh is then retired the same way (that mesh keeps its primary admin) and a third cut shows no attempt naming its NodeId or name while attempts on the remaining peer admins continue once per window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_meshes_rejoin_with_neighbors_preserve_live_births() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let window_ms: u64 = std::env::var("RDM_STALENESS_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(3000);
    let window = Duration::from_millis(window_ms);

    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [
            {"name": "mesh1", "node_admin": 2, "rpc_node": 2},
            {"name": "mesh2", "node_admin": 2, "rpc_node": 2},
        ]}))
        .await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let nodes = estate.settled_shape(&[("mesh1", 2, 2), ("mesh2", 2, 2)], Duration::from_secs(60)).await;
    // Every admin of both meshes holds all eight members ready before anything is cut.
    let everyone: Vec<String> = nodes.iter().map(|n| s(&n["name"])).collect();
    let admins: Vec<(String, String)> = nodes.iter().filter(|n| n["kind"] == "node_admin").map(|n| (s(&n["name"]), s(&n["mesh"]))).collect();
    let bases: Vec<(String, String)> = nodes.iter().filter(|n| n["kind"] == "node_admin").map(|n| (s(&n["name"]), s(&n["admin_api_base"]))).collect();
    assert_eq!(admins.len(), 4, "{nodes:#?}");
    for (name, base) in &bases {
        wait_for(&format!("{name} holds every member ready before the cut"), Duration::from_secs(60), || async { holds_all(&estate.nodes_at(base).await, &everyone).then_some(()) }).await;
    }
    let nodes = estate.nodes().await;
    let mesh_of = |n: &str| nodes.iter().find(|x| x["name"] == n).map(|x| s(&x["mesh"])).unwrap();
    let side = |m: &str| -> Vec<String> { nodes.iter().filter(|n| n["mesh"] == m).map(|n| s(&n["name"])).collect() };
    let (mesh1, mesh2) = (side("mesh1"), side("mesh2"));
    let mut pids: BTreeMap<String, u64> = BTreeMap::new();
    for n in &nodes {
        // The bootstrap admin is adopted on day 0 and has no provider deployment record.
        let name = s(&n["name"]);
        let pid = if name == "mesh1.admin.1" { estate.bootstrap_pid().expect("the bootstrap admin runs") as u64 } else { estate.pid_of(&name).await };
        pids.insert(name, pid);
    }
    let before: BTreeMap<String, (String, String)> = nodes.iter().map(|n| (s(&n["name"]), (s(&n["node_id"]), s(&n["incarnation_id"])))).collect();
    let fabric_primary_before = advertised_fabric_primaries(&nodes);

    // Control: before the cut nothing is stale, so no member is re-fed.
    let t_start = now_ns();
    let healthy_attempts = attempts(&estate.spans(), 0).len();
    assert_eq!(healthy_attempts, 0, "a healthy fabric re-feeds no held member: {:?}", attempts(&estate.spans(), 0).iter().map(|sp| attempt_json(sp)).collect::<Vec<_>>());

    // ---- cut 1: every UDP path between the meshes
    let cut = match Partition::start(&udp_ports(&nodes, &mesh1), &udp_ports(&nodes, &mesh2)) {
        Ok(p) => p,
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: this host cannot drop traffic between the meshes: {why}"),
    };
    let cut_at = now_ns();
    let statuses_seen: Arc<Mutex<BTreeMap<String, BTreeSet<String>>>> = Arc::new(Mutex::new(BTreeMap::new()));
    // Hold the cut until each admin has re-fed each peer-mesh admin twice: a whole repair window
    // completed after the first attempt.
    let held = wait_for("every admin re-feeds every peer-mesh admin twice", window * 6 + Duration::from_secs(40), || {
        let seen = statuses_seen.clone();
        let bases = bases.clone();
        let admins = admins.clone();
        let estate = &estate;
        async move {
            for (name, base) in &bases {
                for n in estate.nodes_at(base).await {
                    seen.lock().unwrap().entry(format!("{name} sees {}", s(&n["name"]))).or_default().insert(s(&n["status"]));
                }
            }
            let spans = estate.spans();
            let att = attempts(&spans, cut_at);
            every_cross_admin_pair_attempted(&att, &admins, 2).then_some(now_ns())
        }
    })
    .await;
    let spans = estate.spans();
    let att1: Vec<&Value> = attempts(&spans, cut_at);
    let held_for_ns = held - cut_at;
    // What was attempted: only peer-mesh admins, on the backbone, from an admin of the other mesh,
    // stale for at least the window, each with a named result.
    for sp in &att1 {
        let (obs, peer) = (attr(sp, "node"), attr(sp, "peer_node"));
        assert!(admins.iter().any(|(n, _)| *n == obs), "an admin re-feeds: {sp}");
        assert!(admins.iter().any(|(n, _)| *n == peer) && mesh_of(&obs) != mesh_of(&peer), "only a peer mesh's node-admin is re-fed across the cut: {sp}");
        assert!(num(sp, "coverage_age_ms") >= window_ms, "a member is re-fed only after its coverage was stale for the window: {sp}");
        assert!(["true", "false"].contains(&attr(sp, "joined").as_str()), "the join result is named: {sp}");
        assert!(!attr(sp, "peer_node_id").is_empty() && !attr(sp, "peer").is_empty(), "the attempt names its peer: {sp}");
        assert!(!attr(sp, "channel").starts_with("mesh:"), "no own-mesh member is stale, so no mesh channel re-feeds: {sp}");
    }
    // Bounded: per (observer, peer) at most one attempt per window over the time the cut held.
    let mut gaps_ms: Vec<u64> = Vec::new();
    for ((obs, peer), n) in per_pair(&att1) {
        let first_to_last: Vec<u64> = att1.iter().filter(|sp| attr(sp, "node") == obs && attr(sp, "peer_node") == peer).map(|sp| at(sp)).collect();
        let cap = held_for_ns / (window.as_nanos() as u64) + 1;
        assert!((n as u64) <= cap, "{obs} -> {peer}: {n} attempts in {} ms exceeds one per {window_ms} ms window", held_for_ns / 1_000_000);
        gaps_ms.extend(first_to_last.windows(2).map(|w| (w[1] - w[0]) / 1_000_000));
    }
    // Neighbours kept: nothing fell back to the no-neighbour refeed, no own-mesh member went
    // silent, and each admin still holds its own-mesh admin ready.
    let fb = fallbacks(&spans, cut_at);
    assert!(fb.is_empty(), "no channel was left without a neighbour: {:?}", fb.iter().map(|sp| (attr(sp, "node"), attr(sp, "channel"))).collect::<Vec<_>>());
    let silent: Vec<(String, String)> = named(&spans, "rdm.mesh.membership.update.via-mesh-silent").into_iter().filter(|sp| at(sp) >= cut_at).map(|sp| (attr(sp, "node"), attr(sp, "mesh"))).collect();
    for (obs, m) in &silent {
        assert!(*m != mesh_of(obs), "{obs} lost its own mesh {m}: {silent:?}");
    }
    for (name, base) in &bases {
        let view = estate.nodes_at(base).await;
        let own: Vec<String> = side(&mesh_of(name));
        assert!(holds_all(&view, &own), "{name} still holds its own mesh ready during the cut: {view:#?}");
    }
    // Ordinary nodes hold the peer mesh as topology (R-G2): nothing they hear is liveness, so no
    // rpc node ever marks a mesh silent, in the cut or before it.
    let ordinary: BTreeSet<String> = nodes.iter().filter(|n| n["kind"] == "rpc_node").map(|n| s(&n["name"])).collect();
    let ordinary_silent: Vec<&(String, String)> = silent.iter().filter(|(o, _)| ordinary.contains(o)).collect();
    assert!(ordinary_silent.is_empty(), "an ordinary node never marks a mesh silent for want of forwarded frames: {ordinary_silent:?}");
    // Interrupted coverage: the peer mesh is unheard by every admin.
    let unheard: BTreeSet<String> = silent.iter().map(|(o, _)| o.clone()).collect();
    for (name, _) in &admins {
        assert!(unheard.contains(name), "{name} stopped hearing the peer mesh: {silent:?}");
    }
    // R-G1 (gossip.md §3.3): the absence of a differential is never a refresh. While the peer mesh
    // is unheard, no primary puts any forwarded frame about it into its mesh: no timer full, no
    // delta, and no admin's coverage of the peer mesh is renewed by a forwarded word.
    let forwarded: Vec<(String, String)> = ["rdm.mesh.membership.update.via-forwarded-full", "rdm.mesh.membership.update.via-delta"]
        .iter()
        .flat_map(|n| named(&spans, n))
        .filter(|sp| at(sp) >= cut_at && at(sp) <= held)
        .map(|sp| (s(&sp["name"]), attr(sp, "node")))
        .collect();
    assert!(forwarded.is_empty(), "no forwarded full or delta refreshed the unheard peer mesh during the cut: {forwarded:?}");
    // No death in any view.
    let seen_cut1 = statuses_seen.lock().unwrap().clone();
    let dead: Vec<&String> = seen_cut1.iter().filter(|(_, st)| st.contains("dead")).map(|(k, _)| k).collect();
    assert!(dead.is_empty(), "no admin ever held a member dead during the cut: {dead:?}");
    let marks_cut1 = lifecycle_marks(&spans, cut_at, held);
    assert!(marks_cut1.is_empty(), "no death, tombstone, departure, rebirth or terminate during the cut: {marks_cut1:?}");

    // ---- heal
    let healed_at = drop_cut(cut).await;
    let mut views = Vec::new();
    for (name, base) in &bases {
        wait_for(&format!("{name} holds every pre-cut member ready after the cut is dropped"), Duration::from_secs(60), || async { holds_all(&estate.nodes_at(base).await, &everyone).then_some(()) }).await;
        let view = wait_for(&format!("{name} settles on the computed seats"), Duration::from_secs(30), || async {
            let v = live(&estate.nodes_at(base).await);
            (holds_all(&v, &everyone) && seats_as_expected(&v).is_ok()).then_some(v)
        })
        .await;
        views.push((name.clone(), view));
    }
    for (name, v) in &views[1..] {
        assert_eq!(advertised_primaries(v), advertised_primaries(&views[0].1), "{name} and {} advertise the same seats", views[0].0);
        assert_eq!(advertised_fabric_primaries(v), advertised_fabric_primaries(&views[0].1), "{name} and {} name the same fabric primary", views[0].0);
    }
    assert_eq!(advertised_fabric_primaries(&views[0].1), fabric_primary_before, "the fabric primary that lived through the cut keeps the seat (R-A2: a lower NodeId never displaces a living holder)");
    // Every node-admin hears the peer mesh again on the backbone: its membership watcher reports
    // the mesh learned. An ordinary node held it as topology through the cut and reports nothing.
    let learned: BTreeSet<(String, String)> = wait_for("every node-admin reports the peer mesh learned again", Duration::from_secs(30), || async {
        let spans = estate.spans();
        let learned: BTreeSet<(String, String)> = named(&spans, "rdm.mesh.membership.update.via-mesh-learned").into_iter().filter(|sp| at(sp) >= healed_at).map(|sp| (attr(sp, "node"), attr(sp, "mesh"))).collect();
        admins.iter().map(|(n, _)| n).all(|n| learned.contains(&(n.clone(), (if mesh1.contains(n) { "mesh2" } else { "mesh1" }).to_string()))).then_some(learned)
    })
    .await;
    let spans = estate.spans();
    // Every birth is the one that was cut: same NodeId, same incarnation, same process.
    let after = estate.nodes().await;
    for n in &after {
        let name = s(&n["name"]);
        assert_eq!(before.get(&name), Some(&(s(&n["node_id"]), s(&n["incarnation_id"]))), "{name} is the same birth after the heal");
        assert!(alive(pids[&name]), "{name} (pid {}) lived through the cut", pids[&name]);
    }
    let marks_heal = lifecycle_marks(&spans, cut_at, now_ns());
    assert!(marks_heal.is_empty(), "no death, tombstone, departure, rebirth or terminate through cut and heal: {marks_heal:?}");
    assert_eq!(advertised_fabric_primaries(&after).len(), 1);
    let _ = (&fabric_primary_before, t_start);

    // ---- retirement: a rpc node is retired through a Build (a legal removal, never an admin)
    let victim = after.iter().find(|n| n["kind"] == "rpc_node" && n["mesh"] == "mesh2" && n["is_primary"] == false).cloned().expect("a non-primary rpc node of mesh2");
    let (victim_name, victim_id) = (s(&victim["name"]), s(&victim["node_id"]));
    let retire_at = now_ns();
    let (status, d) = estate.delete(&format!("/api/nodes/{victim_name}")).await;
    assert_eq!(status, 202, "{d}");
    estate.await_build(d["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let remaining: Vec<String> = everyone.iter().filter(|n| **n != victim_name).cloned().collect();
    for (name, base) in &bases {
        wait_for(&format!("{name} no longer lists the retired {victim_name}"), Duration::from_secs(60), || async {
            let v = estate.nodes_at(base).await;
            (!v.iter().any(|n| n["node_id"] == victim_id.as_str()) && holds_all(&v, &remaining)).then_some(())
        })
        .await;
    }
    let retired_pid = pids[&victim_name];
    wait_for("the retired runtime has exited", Duration::from_secs(30), || async { (!alive(retired_pid)).then_some(()) }).await;
    let spans = estate.spans();
    let retire_marks: Vec<String> = lifecycle_marks(&spans, retire_at, now_ns());
    assert!(retire_marks.iter().any(|n| n.contains("node.delete")), "the retirement is the run's one lifecycle effect, and it fired: {retire_marks:?}");

    // ---- cut 2: the retired birth is held by no one; the peer mesh's admins are the control
    let nodes2 = estate.nodes().await;
    let (m1, m2) = (
        nodes2.iter().filter(|n| n["mesh"] == "mesh1").map(|n| s(&n["name"])).collect::<Vec<_>>(),
        nodes2.iter().filter(|n| n["mesh"] == "mesh2").map(|n| s(&n["name"])).collect::<Vec<_>>(),
    );
    assert!(!m2.contains(&victim_name) && !m1.contains(&victim_name), "the retired {victim_name} ({victim_id}) is absent from the control admin's view: {:?}", nodes2.iter().filter(|n| n["node_id"] == victim_id.as_str() || n["name"] == victim_name.as_str()).collect::<Vec<_>>());
    let cut2 = Partition::start(&udp_ports(&nodes2, &m1), &udp_ports(&nodes2, &m2)).unwrap_or_else(|why| panic!("RDM_REQUIRE_NETFAULT: {why}"));
    let cut2_at = now_ns();
    wait_for("a whole repair window completes on every peer-mesh admin pair after the retirement", window * 6 + Duration::from_secs(40), || async {
        every_cross_admin_pair_attempted(&attempts(&estate.spans(), cut2_at), &admins, 2).then_some(())
    })
    .await;
    let spans = estate.spans();
    let att2 = attempts(&spans, retire_at);
    let named_victim: Vec<Value> = att2.iter().filter(|sp| attr(sp, "peer_node_id") == victim_id || attr(sp, "peer_node") == victim_name).map(|sp| attempt_json(sp)).collect();
    assert!(named_victim.is_empty(), "the retired birth {victim_name} ({victim_id}) is never a repair target after its retirement: {named_victim:?}");
    let control2: Vec<Value> = attempts(&spans, cut2_at).iter().map(|sp| attempt_json(sp)).collect();
    assert!(fallbacks(&spans, cut2_at).is_empty(), "no channel was left without a neighbour in the second cut");
    let healed2_at = drop_cut(cut2).await;
    for (name, base) in &bases {
        wait_for(&format!("{name} holds every surviving member ready after the second cut"), Duration::from_secs(60), || async { holds_all(&estate.nodes_at(base).await, &remaining).then_some(()) }).await;
    }
    let spans = estate.spans();
    let marks2 = lifecycle_marks(&spans, cut2_at, healed2_at);
    assert!(marks2.is_empty(), "no death, tombstone, departure, rebirth or terminate during the second cut: {marks2:?}");

    // ---- retirement of a node-admin: one non-primary admin of the peer mesh, a Build removal
    // (that mesh keeps its primary admin; never the fabric primary)
    let nodes3 = estate.nodes().await;
    let admin_victim = nodes3.iter().find(|n| n["kind"] == "node_admin" && n["mesh"] == "mesh2" && n["is_primary"] == false && n["is_fabric_primary"] != true).cloned().expect("a non-primary admin of mesh2");
    let (av_name, av_id) = (s(&admin_victim["name"]), s(&admin_victim["node_id"]));
    let av_pid = pids[&av_name];
    let survivors: Vec<(String, String)> = admins.iter().filter(|(n, _)| *n != av_name).cloned().collect();
    let bases_left: Vec<(String, String)> = bases.iter().filter(|(n, _)| *n != av_name).cloned().collect();
    let remaining2: Vec<String> = remaining.iter().filter(|n| **n != av_name).cloned().collect();
    assert!(survivors.iter().filter(|(_, m)| m == "mesh2").count() == 1, "mesh2 keeps its other admin");
    let retire2_at = now_ns();
    let (status, d) = estate.delete(&format!("/api/nodes/{av_name}")).await;
    assert_eq!(status, 202, "{d}");
    estate.await_build(d["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    for (name, base) in &bases_left {
        wait_for(&format!("{name} no longer lists the retired admin {av_name}"), Duration::from_secs(60), || async {
            let v = estate.nodes_at(base).await;
            (!v.iter().any(|n| n["node_id"] == av_id.as_str()) && holds_all(&v, &remaining2)).then_some(())
        })
        .await;
    }
    wait_for("the retired admin's runtime has exited", Duration::from_secs(30), || async { (!alive(av_pid)).then_some(()) }).await;
    let retire2_marks = lifecycle_marks(&estate.spans(), retire2_at, now_ns());
    assert!(retire2_marks.iter().any(|n| n.contains("node.delete")), "the admin's retirement fired: {retire2_marks:?}");
    let nodes4 = estate.nodes().await;
    let (a1, a2) = (
        nodes4.iter().filter(|n| n["mesh"] == "mesh1").map(|n| s(&n["name"])).collect::<Vec<_>>(),
        nodes4.iter().filter(|n| n["mesh"] == "mesh2").map(|n| s(&n["name"])).collect::<Vec<_>>(),
    );
    assert!(!a2.contains(&av_name));
    let cut3 = Partition::start(&udp_ports(&nodes4, &a1), &udp_ports(&nodes4, &a2)).unwrap_or_else(|why| panic!("RDM_REQUIRE_NETFAULT: {why}"));
    let cut3_at = now_ns();
    wait_for("attempts on the remaining peer admins continue, a whole window each", window * 6 + Duration::from_secs(40), || async {
        every_cross_admin_pair_attempted(&attempts(&estate.spans(), cut3_at), &survivors, 2).then_some(())
    })
    .await;
    let spans = estate.spans();
    let named_admin: Vec<Value> = attempts(&spans, retire2_at).iter().filter(|sp| attr(sp, "peer_node_id") == av_id || attr(sp, "peer_node") == av_name).map(|sp| attempt_json(sp)).collect();
    assert!(named_admin.is_empty(), "the retired admin {av_name} ({av_id}) is never a repair target after its departure: {named_admin:?}");
    let att3: Vec<Value> = attempts(&spans, cut3_at).iter().map(|sp| attempt_json(sp)).collect();
    assert!(att3.iter().all(|a| a["peer_node"] != av_name.as_str()));
    // The lone admin of mesh2 has no own-mesh admin on the backbone, so the cut leaves that one
    // channel without a neighbour: the edge fallback fires there and nowhere else.
    let lone = survivors.iter().find(|(_, m)| m == "mesh2").unwrap().0.clone();
    let fb3: Vec<(String, String)> = fallbacks(&spans, cut3_at).iter().map(|sp| (attr(sp, "node"), attr(sp, "channel"))).collect();
    assert!(fb3.iter().all(|(n, c)| *n == lone && c == "backbone"), "only the lone admin's backbone falls back: {fb3:?}");
    let healed3_at = drop_cut(cut3).await;
    for (name, base) in &bases_left {
        wait_for(&format!("{name} holds every surviving member ready after the third cut"), Duration::from_secs(60), || async { holds_all(&estate.nodes_at(base).await, &remaining2).then_some(()) }).await;
    }
    let marks3 = lifecycle_marks(&estate.spans(), cut3_at, healed3_at);
    assert!(marks3.is_empty(), "no death, tombstone, departure, rebirth or terminate during the third cut: {marks3:?}");
    let admin_retirement = json!({
        "name": av_name, "node_id": av_id, "retired_at_unix_nano": retire2_at, "marks": retire2_marks, "exited_pid": av_pid,
        "cut_3_at_unix_nano": cut3_at, "control_attempts": att3, "attempts_naming_the_retired_admin": named_admin.len(), "no_neighbours_fallbacks": fb3, "lifecycle_marks_during_cut_3": marks3,
    });
    let spans = estate.spans();

    let result = json!({
        "cell": CELL,
        "window_ms": window_ms,
        "estate": { "members": everyone, "admins": admins.iter().map(|(n, m)| json!({"name": n, "mesh": m})).collect::<Vec<_>>() },
        "cut_1": {
            "cut_at_unix_nano": cut_at, "held_ms": held_for_ns / 1_000_000, "healed_at_unix_nano": healed_at,
            "attempts": att1.iter().map(|sp| attempt_json(sp)).collect::<Vec<_>>(),
            "attempts_per_pair": per_pair(&att1).iter().map(|((o, p), n)| json!({"node": o, "peer_node": p, "attempts": n})).collect::<Vec<_>>(),
            "gaps_ms": gaps_ms,
            "no_neighbours_fallbacks": fb.len(),
            "mesh_silent_observations": silent.iter().map(|(o, m)| json!({"node": o, "mesh": m})).collect::<Vec<_>>(),
            "statuses_seen": seen_cut1.iter().map(|(k, v)| json!({"view": k, "statuses": v})).collect::<Vec<_>>(),
            "lifecycle_marks": marks_cut1,
        },
        "heal": {
            "seats": views.iter().map(|(n, v)| json!({"view": n, "primaries": advertised_primaries(v).iter().map(|(c, p)| json!({"mesh": c.0, "kind": c.1, "primary": p})).collect::<Vec<_>>(), "fabric_primary": advertised_fabric_primaries(v)})).collect::<Vec<_>>(),
            "learned_after_heal": learned.iter().map(|(n, m)| json!({"node": n, "mesh": m})).collect::<Vec<_>>(),
            "same_births": before.iter().map(|(n, (id, inc))| json!({"name": n, "node_id": id, "incarnation_id": inc, "pid": pids[n]})).collect::<Vec<_>>(),
            "lifecycle_marks_through_heal": marks_heal,
        },
        "retirement": { "name": victim_name, "node_id": victim_id, "retired_at_unix_nano": retire_at, "marks": retire_marks, "exited_pid": retired_pid },
        "cut_2": { "cut_at_unix_nano": cut2_at, "control_attempts": control2, "attempts_naming_the_retired_birth": named_victim.len(), "lifecycle_marks": marks2 },
        "admin_retirement": admin_retirement,
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
}
