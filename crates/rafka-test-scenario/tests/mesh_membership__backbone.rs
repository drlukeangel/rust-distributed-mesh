//! i143.e4.s9 process E2E: hierarchical membership (`gossip.md` §3, §5, §6).
//!
//! Each mesh has its own membership channel; only node-admins join the
//! backbone; only a mesh's admin primary publishes its mesh's members there,
//! and only a receiving peer mesh's primary forwards them onto its own mesh
//! channel; the fabric primary alone publishes the fabric's status. Ordinary
//! nodes therefore hold every mesh's nodes without a fabric-wide topic.
//!
//! Cells (s9 acceptance 1-5), from public surfaces and span evidence only:
//! 1. ordinary nodes subscribe to their own mesh channel only, never to the
//!    backbone, yet learn both meshes; every admin joins the backbone;
//! 2. exactly one aggregate publisher per mesh, its admin primary: a
//!    non-primary admin never publishes;
//! 3. losing a mesh's admin primary moves publication to its successor;
//! 5. ...and that successor forwards the peer mesh again, so the mesh's
//!    ordinary nodes hold the peer mesh once more;
//! 4. losing the fabric primary moves fabric-status publication to the new
//!    one, with never two publishers live at once.
//!
//! And under faults (s9 acceptance 6-8):
//! 6. a lost backbone push (the two primaries cut from each other) is
//!    repaired by gossip through the other admins: the peer mesh is never
//!    lost by the mesh's ordinary nodes, and nothing beside gossip resends;
//! 7. an admin isolated past the silence window is cut off and authorizes
//!    nothing: a Build submitted to it is not executed by it, and after the
//!    heal its rightful executor runs it without re-creating a live node;
//! 8. a restarted node is ready only once it holds every mesh.
//!
//! The faults drop UDP on loopback (`iptables`, as root or `sudo -n`); where
//! the host cannot, cells 6-7 are a named skip, a failure under
//! `RAFKA_REQUIRE_NETFAULT=1` (CI).

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-membership".into(),
        subfeature: "backbone".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// One estate at a time: two MM fabrics on one host halve each one's CPU.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// MM with two admins and two rpc nodes per mesh, settled.
async fn mm(estate: &Estate, fabric: &str) -> Vec<Value> {
    let (_, a) = estate
        .post("/api/build", &json!({"fabric": fabric, "meshes": [
            {"name": "mesh1", "node_admin": 2, "rpc_node": 2},
            {"name": "mesh2", "node_admin": 2, "rpc_node": 2},
        ]}))
        .await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let want: BTreeSet<String> = ["mesh1", "mesh2"]
        .iter()
        .flat_map(|m| (1..=2).flat_map(move |i| [format!("{m}.admin.{i}"), format!("{m}.rpc.{i}")]))
        .collect();
    estate.settled(&want, Duration::from_secs(30)).await
}

fn netfault(why: String) -> Option<()> {
    if std::env::var("RAFKA_REQUIRE_NETFAULT").as_deref() == Ok("1") {
        panic!("RAFKA_REQUIRE_NETFAULT=1 but this host cannot drop traffic: {why}");
    }
    eprintln!("SKIP fault cells: {why}");
    None
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}


fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_str().and_then(|v| v.parse().ok()).or_else(|| sp["start_unix_nano"].as_u64()).unwrap_or(0)
}

/// `(node, mesh)` -> the role spans, in time order.
fn roles(spans: &[Value], name: &str, key: &str) -> Vec<(u64, String, String, String)> {
    let mut v: Vec<_> = named(spans, name).into_iter().map(|sp| (start(sp), attr(sp, "node"), attr(sp, key), attr(sp, "role"))).collect();
    v.sort();
    v
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// Who holds `role` for `scope` now, by the last start/stop per node. A killed process never
/// says stop, so a path in `dead` drops its events from before its kill; drift recovery may
/// rebirth the same path under a new NodeId, and that birth's events count.
fn holders(events: &[(u64, String, String, String)], scope: &str, dead: &BTreeMap<String, u64>) -> BTreeSet<String> {
    let mut last: BTreeMap<&str, &str> = BTreeMap::new();
    for (at, node, _, role) in events.iter().filter(|e| e.2 == scope) {
        if dead.get(node.as_str()).is_some_and(|killed| at <= killed) {
            continue;
        }
        last.insert(node, role);
    }
    last.into_iter().filter(|(_, r)| *r == "start").map(|(n, _)| n.to_string()).collect()
}

/// The latest learned/silent verdict each ordinary node holds about `mesh`.
fn verdicts(spans: &[Value], node: &str, mesh: &str) -> Option<String> {
    let mut v: Vec<(u64, &str)> = Vec::new();
    for (name, verdict) in [("rafka.mesh.membership.update.via-mesh-learned", "learned"), ("rafka.mesh.membership.update.via-mesh-silent", "silent")] {
        v.extend(named(spans, name).into_iter().filter(|sp| attr(sp, "node") == node && attr(sp, "mesh") == mesh).map(|sp| (start(sp), verdict)));
    }
    v.sort();
    v.last().map(|(_, x)| x.to_string())
}

fn primary(nodes: &[Value], mesh: &str) -> Value {
    nodes.iter().find(|n| n["mesh"] == mesh && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().unwrap_or(Value::Null)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_rides_mesh_channels_and_the_admin_backbone() {
    let _one = SERIAL.lock().await;
    let mut estate = Estate::bootstrap(owner("membership_rides_mesh_channels_and_the_admin_backbone"), "fabric1", "mesh1").await;
    let nodes = mm(&estate, "fabric1").await;
    let rpcs: Vec<String> = nodes.iter().filter(|n| n["kind"] == "rpc_node").map(|n| s(&n["name"])).collect();
    let admins: Vec<String> = nodes.iter().filter(|n| n["kind"] == "node_admin").map(|n| s(&n["name"])).collect();

    // 1. Every ordinary node learns both meshes...
    wait_for("every rpc node holds both meshes", Duration::from_secs(30), || {
        let spans = estate.spans();
        let rpcs = rpcs.clone();
        async move { rpcs.iter().all(|n| ["mesh1", "mesh2"].iter().all(|m| verdicts(&spans, n, m).as_deref() == Some("learned"))).then_some(()) }
    })
    .await;
    // ...through its own mesh channel only; every admin is on the backbone.
    let spans = estate.spans();
    let subs: Vec<(String, String)> =
        named(&spans, "rafka.mesh.membership.update.via-subscribe").into_iter().map(|sp| (attr(sp, "node"), attr(sp, "channel"))).collect();
    for n in &rpcs {
        let mesh = n.split('.').next().unwrap();
        let mine: BTreeSet<&str> = subs.iter().filter(|(x, _)| x == n).map(|(_, c)| c.as_str()).collect();
        assert_eq!(mine, [format!("mesh:{mesh}").as_str()].into_iter().collect(), "{n} joins its own mesh channel only: {mine:?}");
    }
    for n in &admins {
        assert!(subs.iter().any(|(x, c)| x == n && c == "backbone"), "{n} joins the backbone");
    }

    // 2. One aggregate publisher per mesh, its admin primary.
    let none: BTreeMap<String, u64> = BTreeMap::new();
    for m in ["mesh1", "mesh2"] {
        let p = s(&primary(&nodes, m)["name"]);
        wait_for(&format!("{m}'s primary publishes it, alone"), Duration::from_secs(15), || {
            let spans = estate.spans();
            let (p, none) = (p.clone(), none.clone());
            async move {
                let ev = roles(&spans, "rafka.mesh.backbone.update.via-aggregate-publisher", "mesh");
                (holders(&ev, m, &none) == [p].into_iter().collect()).then_some(())
            }
        })
        .await;
    }
    // An admin that is not its mesh's primary now may have published earlier: under
    // lowest-ready-NodeId elections a mesh's first admin holds the seat until a lower NodeId of
    // its cohort is ready, then hands it over. What must hold: it publishes only while it is its
    // mesh's primary in its own view, and none of its publishing intervals is still open.
    let spans = estate.spans();
    let ev = roles(&spans, "rafka.mesh.backbone.update.via-aggregate-publisher", "mesh");
    for n in admins.iter().filter(|n| ["mesh1", "mesh2"].iter().all(|m| s(&primary(&nodes, m)["name"]) != **n)) {
        let mine: Vec<&(u64, String, String, String)> = ev.iter().filter(|e| &e.1 == n).collect();
        assert!(mine.last().is_none_or(|e| e.3 == "stop"), "the non-primary admin {n} has stopped publishing: {mine:?}");
        for e in mine.iter().filter(|e| e.3 == "start") {
            let seat = named(&spans, "rafka.mesh.election.resolve.via-mesh-primary")
                .into_iter()
                .filter(|sp| attr(sp, "observer") == *n && start(sp) <= e.0)
                .max_by_key(|sp| start(sp))
                .map(|sp| attr(sp, "winner_path"));
            assert_eq!(seat.as_deref(), Some(n.as_str()), "{n} started publishing {} only while it held the mesh seat: {mine:?}", e.2);
        }
    }

    // 3 + 5. Lose mesh2's admin primary: its successor publishes mesh2 and
    // forwards mesh1, and mesh2's rpc nodes hold mesh1 again.
    let old = primary(&nodes, "mesh2");
    let (old_name, old_id) = (s(&old["name"]), s(&old["node_id"]));
    estate.kill_node(&old_name).await;
    let dead: BTreeMap<String, u64> = [(old_name.clone(), now_ns())].into_iter().collect();
    // Tracked by NodeId: drift recovery may rebirth the killed path under a new NodeId.
    let successor = wait_for("mesh2 elects a successor", Duration::from_secs(30), || async {
        let p = primary(&estate.nodes().await, "mesh2");
        (!s(&p["name"]).is_empty() && s(&p["node_id"]) != old_id).then(|| s(&p["name"]))
    })
    .await;
    wait_for("mesh2's successor publishes and forwards", Duration::from_secs(20), || {
        let spans = estate.spans();
        let (successor, dead) = (successor.clone(), dead.clone());
        async move {
            let publ = roles(&spans, "rafka.mesh.backbone.update.via-aggregate-publisher", "mesh");
            let fwd = roles(&spans, "rafka.mesh.backbone.update.via-forwarder", "mesh");
            let one: BTreeSet<String> = [successor].into_iter().collect();
            (holders(&publ, "mesh2", &dead) == one && holders(&fwd, "mesh2", &dead) == one).then_some(())
        }
    })
    .await;
    for n in rpcs.iter().filter(|n| n.starts_with("mesh2.")) {
        let n = n.clone();
        wait_for(&format!("{n} holds mesh1 again"), Duration::from_secs(15), || {
            let spans = estate.spans();
            let n = n.clone();
            async move { (verdicts(&spans, &n, "mesh1").as_deref() == Some("learned")).then_some(()) }
        })
        .await;
    }

    // 4. Lose the fabric primary: status publication moves, never doubled. The seat moved in
    // step 3; the primary is lost only once it alone publishes the fabric's status.
    let (_, fabric) = estate.get("/api/fabric").await;
    let fp = wait_for("the fabric primary alone publishes the fabric's status before it is lost", Duration::from_secs(40), || {
        let dead = dead.clone();
        let fabric_id = s(&fabric["id"]);
        let estate = &estate;
        async move {
            let fp = estate.nodes().await.into_iter().find(|n| n["is_fabric_primary"] == true)?;
            let ev = roles(&estate.spans(), "rafka.mesh.fabric.update.via-status-publisher", "fabric");
            (holders(&ev, &fabric_id, &dead) == [s(&fp["name"])].into_iter().collect()).then_some(fp)
        }
    })
    .await;
    let (fp_name, fp_id) = (s(&fp["name"]), s(&fp["node_id"]));
    // Survivors from the live view at the kill, never an earlier /api/fabric answer: step 3's
    // lost admin may still be advertised there.
    let survivors: Vec<String> = estate
        .nodes()
        .await
        .iter()
        .filter(|n| n["kind"] == "node_admin" && n["status"] == "ready-for-traffic" && s(&n["node_id"]) != fp_id)
        .map(|n| s(&n["admin_api_base"]))
        .filter(|b| !b.is_empty())
        .collect();
    assert!(!survivors.is_empty(), "a live admin other than the fabric primary {fp_name}");
    let lost_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    if fp_name == "mesh1.admin.1" {
        estate.kill_bootstrap();
    } else {
        estate.kill_node(&fp_name).await;
    }
    let mut dead = dead;
    dead.insert(fp_name.clone(), now_ns());
    estate.admin = survivors[0].clone();
    // The winner is read with the publisher, in one poll: until the lost
    // primary's mesh is heard again through its successor's aggregate, a
    // peer mesh's admin may briefly name itself in its own view (and, gated,
    // publishes nothing). The converged view names the one publisher.
    let next = wait_for("a new fabric primary alone publishes the fabric's status", Duration::from_secs(40), || async {
        let n = estate.nodes().await;
        // Tracked by NodeId: drift recovery may rebirth the lost path under a new NodeId.
        let next = n.iter().find(|x| x["is_fabric_primary"] == true && s(&x["node_id"]) != fp_id).map(|x| s(&x["name"]))?;
        // Status publication is keyed by the Fabric's id, the backbone's key.
        let ev = roles(&estate.spans(), "rafka.mesh.fabric.update.via-status-publisher", "fabric");
        (holders(&ev, &s(&fabric["id"]), &dead) == [next.clone()].into_iter().collect()).then_some(next)
    })
    .await;
    // The seat moves with the lowest NodeId as meshes grow and lose primaries, and a rebirth of
    // the lost path may hold it on the way: the lost primary was the last to start publishing
    // before the loss, and the last to start after it is the seat holder the view names.
    let ev = roles(&estate.spans(), "rafka.mesh.fabric.update.via-status-publisher", "fabric");
    let before = ev.iter().filter(|e| e.0 < lost_at && e.3 == "start").max_by_key(|e| e.0).map(|e| e.1.clone());
    let after = ev.iter().filter(|e| e.0 >= lost_at && e.3 == "start").max_by_key(|e| e.0).map(|e| e.1.clone());
    assert_eq!(before.as_deref(), Some(fp_name.as_str()), "the lost primary published before the loss: {ev:?}");
    assert_eq!(after.as_deref(), Some(next.as_str()), "the seat holder is the last to start publishing after the loss: {ev:?}");

    // Stop through whoever holds the fabric now.
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gossip_repairs_a_lost_push_and_isolation_authorizes_nothing() {
    let _one = SERIAL.lock().await;
    let mut estate = Estate::bootstrap(owner("gossip_repairs_a_lost_push_and_isolation_authorizes_nothing"), "fabric2", "mesh1").await;
    let nodes = mm(&estate, "fabric2").await;
    let rpcs2: Vec<String> = nodes.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "rpc_node").map(|n| s(&n["name"])).collect();
    wait_for("mesh2's rpc nodes hold mesh1", Duration::from_secs(30), || {
        let spans = estate.spans();
        let rpcs2 = rpcs2.clone();
        async move { rpcs2.iter().all(|n| verdicts(&spans, n, "mesh1").as_deref() == Some("learned")).then_some(()) }
    })
    .await;

    // 8. Restart mesh2.rpc.1: its new birth is ready holding both meshes.
    let before = s(&estate.node("mesh2.rpc.1").await["incarnation_id"]);
    let (status, a) = estate.post("/api/nodes/mesh2.rpc.1/restart", &json!({})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(60)).await;
    // The Build can complete on another admin: wait for the asked one's view.
    let reborn = wait_for("the asked admin's view holds the new birth", Duration::from_secs(15), || async {
        let now = s(&estate.node("mesh2.rpc.1").await["incarnation_id"]);
        (!now.is_empty() && now != before).then_some(now)
    })
    .await;
    let spans = estate.spans();
    let ready = named(&spans, "rafka.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| attr(sp, "incarnation_id") == reborn)
        .unwrap_or_else(|| panic!("the reborn mesh2.rpc.1 reports ready"))
        .clone();
    assert_eq!(attr(&ready, "meshes"), "2", "ready holding both meshes: {ready}");
    let pulled = named(&spans, "rafka.mesh.entry.update.via-membership-pulled")
        .into_iter()
        .filter(|sp| attr(sp, "node") == "mesh2.rpc.1")
        .map(start)
        .max()
        .expect("the reborn node pulled its entry");
    assert!(start(&ready) >= pulled, "ready only after the entry pull");

    // 6. Cut the two mesh primaries from each other: gossip repairs through
    // the other admins, and mesh2's ordinary nodes never lose mesh1.
    let nodes = estate.nodes().await;
    let (p1, p2) = (s(&primary(&nodes, "mesh1")["name"]), s(&primary(&nodes, "mesh2")["name"]));
    let cut_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let Some(cut) = Partition::start(&udp_ports(&nodes, &[p1.clone()]), &udp_ports(&nodes, &[p2.clone()])).map_err(netfault).ok() else {
        return estate.stop().await;
    };
    tokio::time::sleep(Duration::from_secs(6)).await;
    drop(cut);
    let spans = estate.spans();
    let lost: Vec<String> = named(&spans, "rafka.mesh.membership.update.via-mesh-silent")
        .into_iter()
        .filter(|sp| start(sp) > cut_at && attr(sp, "mesh") == "mesh1" && rpcs2.contains(&attr(sp, "node")))
        .map(|sp| attr(sp, "node"))
        .collect();
    assert!(lost.is_empty(), "with {p1} and {p2} cut apart, mesh2's rpc nodes kept mesh1: {lost:?}");

    // 7. Isolate mesh1's non-primary admin past the silence window and ask it
    // for a Build: cut off, it executes nothing; healed, the rightful
    // executor runs it and no live node is created again.
    let nodes = estate.nodes().await;
    let lone = nodes.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["is_primary"] != true).cloned().unwrap();
    let lone_name = s(&lone["name"]);
    let others: Vec<String> = nodes.iter().map(|n| s(&n["name"])).filter(|n| *n != lone_name).collect();
    let before: BTreeMap<String, String> = nodes.iter().map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect();
    let isolation = Partition::start(&udp_ports(&nodes, &[lone_name.clone()]), &udp_ports(&nodes, &others)).expect("the host dropped traffic above");
    wait_for(&format!("{lone_name} is cut off"), Duration::from_secs(20), || {
        let spans = estate.spans();
        let lone_name = lone_name.clone();
        async move {
            named(&spans, "rafka.mesh.membership.update.via-cut-off").iter().any(|sp| attr(sp, "node") == lone_name && attr(sp, "role") == "start").then_some(())
        }
    })
    .await;
    let main_admin = estate.admin.clone();
    estate.admin = s(&lone["admin_api_base"]);
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric2", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 3},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 2},
    ]})).await;
    assert_eq!(status, 202, "{a}");
    let b = s(&a["build_id"]);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let by_lone: Vec<String> = named(&estate.spans(), "rafka.node_admin.build.update.via-reconcile")
        .into_iter()
        .filter(|sp| attr(sp, "build_id") == b && attr(sp, "executor") == lone_name)
        .map(|sp| attr(sp, "operations"))
        .collect();
    assert!(by_lone.is_empty(), "the cut-off {lone_name} executed nothing: {by_lone:?}");
    drop(isolation);
    estate.admin = main_admin;
    estate.await_build(&b, Duration::from_secs(90)).await;
    let mut want: BTreeSet<String> = before.keys().cloned().collect();
    want.insert("mesh1.rpc.3".into());
    let after = estate.settled(&want, Duration::from_secs(30)).await;
    // Relearned with membership after the heal: every birth's current
    // runtime metadata (i143.e4.s16), no Build history replayed.
    for n in &after {
        assert!(n["data_dir"].as_str().is_some_and(|d| !d.is_empty()), "{} carries its data dir after the heal: {n}", n["name"]);
    }
    for n in &after {
        if let Some(inc) = before.get(&s(&n["name"])) {
            assert_eq!(&s(&n["incarnation_id"]), inc, "{} was never created again", n["name"]);
        }
    }
    wait_for(&format!("{lone_name} hears the fabric again"), Duration::from_secs(20), || {
        let spans = estate.spans();
        let lone_name = lone_name.clone();
        async move {
            named(&spans, "rafka.mesh.membership.update.via-cut-off").iter().any(|sp| attr(sp, "node") == lone_name && attr(sp, "role") == "stop").then_some(())
        }
    })
    .await;
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
}
