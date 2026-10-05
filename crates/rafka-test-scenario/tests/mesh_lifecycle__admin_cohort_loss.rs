//! i143.e4.s10 process E2E: node-admin cohort loss on the hierarchical
//! membership topology (rafka-v2 #2803).
//!
//! A mesh loses its node-admin cohort; the fabric primary reseeds it under the
//! same mesh identity without replacing the mesh's surviving members, and
//! control returns to the mesh once its recovered admin is eligible. From
//! public surfaces only (control API, deployment records for fault
//! injection), with OTLP evidence:
//!
//! 1. a secondary mesh loses its admin primary: the sibling admin succeeds it,
//!    the fabric primary does not move, the mesh's backbone publication moves
//!    to the successor, and the lost path comes back as a new birth that does
//!    not displace the incumbent;
//! 2. a secondary mesh loses every admin: the fabric primary recreates the
//!    cohort under the same mesh id, every surviving rpc birth is untouched,
//!    one admin primary is elected, backbone publication resumes, and the
//!    mesh's next member work runs under its recovered primary;
//! 3. the fabric-primary mesh loses every admin: the peer mesh's primary takes
//!    the fabric, recreates the cohort under the same mesh id, and the fabric
//!    returns to the recovered mesh only once its admin is eligible, never two
//!    fabric primaries;
//! 5. a partition is not death: an isolated cohort that still answers
//!    directly keeps its paths, and the heal converges to one primary per mesh
//!    and one fabric primary;
//! 6. an active Build survives the loss of its executor's cohort under the
//!    same id, naming the previous executor;
//! 7. a recreated admin is born full: subscribed and entry-pulled before it is
//!    ready, and holds no authority (primary, fabric primary, publisher)
//!    before it is ready.
//!
//! Cell 4 (whole fabric-primary mesh loss) is `mesh_lifecycle__mesh_recover`,
//! which runs on this topology.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "admin-cohort-loss".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// One estate at a time: two MM fabrics on one host halve each one's CPU.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn desired(fabric: &str, rpc2: u32) -> Value {
    json!({"fabric": fabric, "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 2},
        {"name": "mesh2", "node_admin": 2, "rpc_node": rpc2},
    ]})
}

fn names(rpc2: u32) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2", "mesh2.admin.1", "mesh2.admin.2"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    out.extend((1..=rpc2).map(|i| format!("mesh2.rpc.{i}")));
    out
}

async fn mm(estate: &Estate, fabric: &str) -> Vec<Value> {
    let (status, a) = estate.post("/api/build", &desired(fabric, 2)).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    estate.settled(&names(2), Duration::from_secs(30)).await
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

async fn get(base: &str, path: &str) -> Option<Value> {
    let r = reqwest::Client::new().get(format!("{base}{path}")).timeout(Duration::from_secs(2)).send().await.ok()?;
    r.status().is_success().then_some(())?;
    r.json().await.ok()
}

fn kill(node: &Value) {
    let d: Value = serde_json::from_slice(&std::fs::read(format!("{}/deployment.json", s(&node["data_dir"]))).unwrap()).unwrap();
    let _ = Command::new("kill").args(["-9", &d["pid"].to_string()]).status();
}

fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_str().and_then(|v| v.parse().ok()).or_else(|| sp["start_unix_nano"].as_u64()).unwrap_or(0)
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// name -> incarnation.
fn births(nodes: &[Value]) -> BTreeMap<String, String> {
    nodes.iter().map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect()
}

fn primary(nodes: &[Value], mesh: &str) -> Option<String> {
    nodes.iter().find(|n| n["mesh"] == mesh && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"]))
}

fn fabric_primaries(nodes: &[Value]) -> Vec<String> {
    nodes.iter().filter(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).collect()
}

/// `(start, node, scope, role)` for a publisher role span, in time order.
fn roles(spans: &[Value], name: &str, key: &str) -> Vec<(u64, String, String, String)> {
    let mut v: Vec<_> = named(spans, name).into_iter().map(|sp| (start(sp), attr(sp, "node"), attr(sp, key), attr(sp, "role"))).collect();
    v.sort();
    v
}

/// Who holds the role for `scope` now, by each node's last start/stop.
fn holders(events: &[(u64, String, String, String)], scope: &str, dead: &BTreeSet<String>) -> BTreeSet<String> {
    let mut last: BTreeMap<&str, &str> = BTreeMap::new();
    for (_, node, _, role) in events.iter().filter(|e| e.2 == scope) {
        last.insert(node, role);
    }
    last.into_iter().filter(|(n, r)| *r == "start" && !dead.contains(*n)).map(|(n, _)| n.to_string()).collect()
}

/// Cell 7: the birth `incarnation` of `node` was born full and held no
/// authority before it was ready.
fn born_full(spans: &[Value], node: &str, incarnation: &str) {
    let ready = named(spans, "rafka.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| attr(sp, "node") == node && attr(sp, "incarnation_id") == incarnation)
        .unwrap_or_else(|| panic!("{node} ({incarnation}) reports ready"));
    let ready_at = start(ready);
    // Its own process: the spans of the file the ready span came from share its pid.
    let birth: Vec<&Value> = spans.iter().filter(|sp| sp["_file"] == ready["_file"]).collect();
    for channel in ["backbone", "mesh:"] {
        assert!(
            birth.iter().any(|sp| sp["name"] == "rafka.mesh.membership.update.via-subscribe" && attr(sp, "channel").starts_with(channel) && start(sp) <= ready_at),
            "{node} subscribed to {channel} before ready"
        );
    }
    assert!(
        birth.iter().any(|sp| sp["name"] == "rafka.mesh.entry.update.via-membership-pulled" && start(sp) <= ready_at),
        "{node} pulled its entry before ready"
    );
    let early = |name: &str, key: &str| {
        named(spans, name).into_iter().filter(|sp| attr(sp, key) == node && start(sp) < ready_at && sp["_file"] == ready["_file"]).count()
    };
    assert_eq!(early("rafka.mesh.backbone.update.via-aggregate-publisher", "node"), 0, "{node} published nothing before ready");
    assert_eq!(early("rafka.mesh.fabric.update.via-status-publisher", "node"), 0, "{node} published no status before ready");
    let named_primary_early = named(spans, "rafka.mesh.election.resolve.via-recompute")
        .into_iter()
        .chain(named(spans, "rafka.mesh.election.resolve.via-fabric-recompute"))
        .filter(|sp| attr(sp, "primary") == node && start(sp) < ready_at)
        .filter(|sp| start(sp) > start(birth.iter().min_by_key(|x| start(x)).unwrap()))
        .count();
    assert_eq!(named_primary_early, 0, "no admin named {node} primary before it was ready");
}

/// Cells 1, 2 and 7 on one fabric.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_secondary_mesh_loses_its_admins_and_is_reseeded() {
    let _one = SERIAL.lock().await;
    let mut estate = Estate::bootstrap(owner("a_secondary_mesh_loses_its_admins_and_is_reseeded"), "fabric1", "mesh1").await;
    let before = mm(&estate, "fabric1").await;
    let (_, mesh2) = estate.get("/api/meshes/mesh2").await;
    let mesh2_id = s(&mesh2["id"]);

    // 1. Lose mesh2's admin primary.
    let incumbent = primary(&before, "mesh2").expect("mesh2 has an admin primary");
    let sibling = if incumbent == "mesh2.admin.1" { "mesh2.admin.2" } else { "mesh2.admin.1" }.to_string();
    let lost_at = now_ns();
    kill(before.iter().find(|n| n["name"] == incumbent.as_str()).unwrap());
    wait_for(&format!("{sibling} succeeds {incumbent}"), Duration::from_secs(30), || async {
        let n = estate.nodes().await;
        (primary(&n, "mesh2").as_deref() == Some(sibling.as_str())).then_some(())
    })
    .await;
    let nodes = estate.nodes().await;
    assert_eq!(fabric_primaries(&nodes), vec!["mesh1.admin.1".to_string()], "the fabric primary did not move");
    let dead: BTreeSet<String> = [incumbent.clone()].into();
    wait_for("mesh2's backbone publication moved to the successor", Duration::from_secs(20), || {
        let spans = estate.spans();
        let (dead, sibling) = (dead.clone(), sibling.clone());
        async move {
            let ev = roles(&spans, "rafka.mesh.backbone.update.via-aggregate-publisher", "mesh");
            (holders(&ev, "mesh2", &dead) == [sibling].into()).then_some(())
        }
    })
    .await;
    assert!(
        !named(&estate.spans(), "rafka.mesh.election.resolve.via-fabric-recompute").iter().any(|sp| start(sp) > lost_at),
        "no admin's fabric primary changed"
    );
    // The lost path comes back as a new birth; the incumbent keeps primacy.
    let (status, a) = estate.post("/api/build", &desired("fabric1", 2)).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let after1 = estate.settled(&names(2), Duration::from_secs(30)).await;
    assert_ne!(births(&after1)[&incumbent], births(&before)[&incumbent], "{incumbent} is a new birth");
    assert_eq!(primary(&after1, "mesh2").as_deref(), Some(sibling.as_str()), "the new birth does not displace the incumbent");

    // 2. Lose every mesh2 admin; mesh2's rpc nodes keep running.
    let rpc2: BTreeMap<String, String> = births(&after1).into_iter().filter(|(n, _)| n.starts_with("mesh2.rpc.")).collect();
    let admins2_before: BTreeMap<String, String> = births(&after1).into_iter().filter(|(n, _)| n.starts_with("mesh2.admin.")).collect();
    for n in after1.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin") {
        kill(n);
    }
    let lost2_at = now_ns();
    // A Build reconciles against the view: submit once the fabric primary's
    // view no longer holds a mesh2 admin primary.
    wait_for("the fabric primary's view lost mesh2's admin primary", Duration::from_secs(30), || async {
        primary(&estate.nodes().await, "mesh2").is_none().then_some(())
    })
    .await;
    let (status, a) = estate.post("/api/build", &desired("fabric1", 2)).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let after2 = wait_for("mesh2's cohort is back with one admin primary", Duration::from_secs(60), || async {
        let n = estate.settled(&names(2), Duration::from_secs(30)).await;
        primary(&n, "mesh2").is_some().then_some(n)
    })
    .await;
    let (_, mesh2) = estate.get("/api/meshes/mesh2").await;
    assert_eq!(s(&mesh2["id"]), mesh2_id, "mesh2 reseeded as itself");
    let b2 = births(&after2);
    for (name, inc) in &rpc2 {
        assert_eq!(&b2[name], inc, "{name} was not replaced or restarted");
    }
    for (name, inc) in &admins2_before {
        assert_ne!(&b2[name], inc, "{name} is a new birth");
    }
    let p2 = primary(&after2, "mesh2").unwrap();
    assert_eq!(after2.iter().filter(|n| n["mesh"] == "mesh2" && n["is_primary"] == true && n["kind"] == "node_admin").count(), 1);
    assert_eq!(fabric_primaries(&after2), vec!["mesh1.admin.1".to_string()], "one fabric primary");
    wait_for("mesh2's backbone publication resumed under its recovered primary", Duration::from_secs(20), || {
        let spans = estate.spans();
        let p2 = p2.clone();
        async move {
            let ev: Vec<_> = roles(&spans, "rafka.mesh.backbone.update.via-aggregate-publisher", "mesh").into_iter().filter(|e| e.0 > lost2_at).collect();
            (holders(&ev, "mesh2", &BTreeSet::new()) == [p2].into()).then_some(())
        }
    })
    .await;
    // The mesh's next member work runs under its recovered primary.
    let (status, a) = estate.post("/api/nodes/spawn", &json!({"mesh": "mesh2", "kind": "rpc_node"})).await;
    assert_eq!(status, 202, "{a}");
    let grow = s(&a["build_id"]);
    estate.await_build(&grow, Duration::from_secs(120)).await;
    let spans = estate.spans();
    let executors: BTreeSet<String> = named(&spans, "rafka.node_admin.build.update.via-reconcile")
        .into_iter()
        .filter(|sp| attr(sp, "build_id") == grow && attr(sp, "operations").contains("create-node:mesh2.rpc.3"))
        .map(|sp| attr(sp, "executor"))
        .collect();
    assert_eq!(executors, [p2.clone()].into(), "mesh2's member work ran under its recovered primary");

    // 7. Every recreated admin was born full.
    let nodes = estate.nodes().await;
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin") {
        born_full(&spans, &s(&n["name"]), &s(&n["incarnation_id"]));
    }
    estate.stop().await;
}

/// Cells 3 and 7.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fabric_primary_mesh_loses_its_admins_and_control_returns_once_eligible() {
    let _one = SERIAL.lock().await;
    let mut estate = Estate::bootstrap(owner("the_fabric_primary_mesh_loses_its_admins_and_control_returns_once_eligible"), "fabric2", "mesh1").await;
    let before = mm(&estate, "fabric2").await;
    let (_, mesh1) = estate.get("/api/meshes/mesh1").await;
    let mesh1_id = s(&mesh1["id"]);
    let (_, fabric) = estate.get("/api/fabric").await;
    let advertised: Vec<String> = fabric["meshes"].as_array().unwrap().iter().map(|m| s(&m["admin_api_base"])).collect();
    let rpc1: BTreeMap<String, String> = births(&before).into_iter().filter(|(n, _)| n.starts_with("mesh1.rpc.")).collect();
    let admins1: BTreeMap<String, String> = births(&before).into_iter().filter(|(n, _)| n.starts_with("mesh1.admin.")).collect();

    // Lose every mesh1 admin (the bootstrap one included); mesh1's rpc nodes run on.
    for n in before.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["name"] != "mesh1.admin.1") {
        kill(n);
    }
    estate.kill_bootstrap();

    // Control through the advertised topology only: mesh2's primary holds the fabric.
    let (holder, base) = wait_for("mesh2's admin primary holds the fabric", Duration::from_secs(60), || {
        let advertised = advertised.clone();
        async move {
            for b in &advertised {
                let Some(f) = get(b, "/api/fabric").await else { continue };
                let (p, pb) = (s(&f["fabric_primary"]), s(&f["admin_api_base"]));
                if p.starts_with("mesh2.admin.") {
                    if let Some(own) = get(&pb, "/api/fabric").await {
                        if s(&own["fabric_primary"]) == p {
                            return Some((p, pb));
                        }
                    }
                }
            }
            None
        }
    })
    .await;
    estate.admin = base;
    let nodes = estate.nodes().await;
    assert_eq!(primary(&nodes, "mesh2").as_deref(), Some(holder.as_str()), "the fabric primary is mesh2's admin primary");

    // It reseeds mesh1's cohort under mesh1's identity.
    let (status, a) = estate.post("/api/build", &desired("fabric2", 2)).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let after = wait_for("the fabric returns to mesh1's recovered admin primary", Duration::from_secs(90), || async {
        let n = estate.settled(&names(2), Duration::from_secs(30)).await;
        let fp = fabric_primaries(&n);
        (fp.len() == 1 && fp[0].starts_with("mesh1.admin.") && primary(&n, "mesh1").as_deref() == Some(fp[0].as_str())).then_some(n)
    })
    .await;
    let (_, mesh1) = estate.get("/api/meshes/mesh1").await;
    assert_eq!(s(&mesh1["id"]), mesh1_id, "mesh1 reseeded as itself");
    let b = births(&after);
    for (name, inc) in &rpc1 {
        assert_eq!(&b[name], inc, "{name} was not replaced or restarted");
    }
    for (name, inc) in &admins1 {
        assert_ne!(&b[name], inc, "{name} is a new birth");
    }

    // The fabric returned only to an eligible admin: no admin named a
    // recovered mesh1 admin fabric primary before that birth was ready.
    let spans = estate.spans();
    for n in after.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin") {
        born_full(&spans, &s(&n["name"]), &s(&n["incarnation_id"]));
    }
    // ...and never two: each admin's own view names one fabric primary.
    for n in after.iter().filter(|n| n["kind"] == "node_admin") {
        let own = get(&s(&n["admin_api_base"]), "/api/nodes").await.unwrap_or(Value::Null);
        let fp: Vec<String> = own["nodes"].as_array().map(|a| fabric_primaries(a)).unwrap_or_default();
        assert_eq!(fp.len(), 1, "{} sees one fabric primary: {fp:?}", n["name"]);
    }
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
}

/// Cells 5 and 6.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partition_is_not_death_and_an_active_build_survives_its_cohort() {
    let _one = SERIAL.lock().await;
    let mut estate = Estate::bootstrap(owner("a_partition_is_not_death_and_an_active_build_survives_its_cohort"), "fabric3", "mesh1").await;
    let before = mm(&estate, "fabric3").await;

    // 5. Isolate mesh2's admin cohort past the silence window, with no Build
    // active (an isolated side executes nothing), so every view outside it
    // holds those admins as silent. Heal, and submit at once: the views still
    // hold them silent while gossip rejoins, but they answer directly again.
    // The recovery authority's direct-answer fence keeps their paths: no
    // second runtime at a live admin path.
    let admins2: Vec<String> = before.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin").map(|n| s(&n["name"])).collect();
    let others: Vec<String> = before.iter().map(|n| s(&n["name"])).filter(|n| !admins2.contains(n)).collect();
    let incarnations = births(&before);
    match Partition::start(&udp_ports(&before, &admins2), &udp_ports(&before, &others)) {
        Ok(cut) => {
            wait_for("the fabric primary holds mesh2's admins as silent", Duration::from_secs(30), || async {
                let n = estate.nodes().await;
                admins2.iter().all(|a| n.iter().any(|x| x["name"] == a.as_str() && x["status"] == "dead")).then_some(())
            })
            .await;
            let healed_at = now_ns();
            drop(cut);
            let (status, a) = estate.post("/api/build", &desired("fabric3", 2)).await;
            assert_eq!(status, 202, "{a}");
            let during = s(&a["build_id"]);
            estate.await_build(&during, Duration::from_secs(120)).await;
            let mesh2_admin_runtimes = estate
                .live_runtimes()
                .into_iter()
                .filter(|(d, _)| d.file_name().unwrap().to_string_lossy().starts_with("mesh2.admin."))
                .count();
            assert_eq!(mesh2_admin_runtimes, 2, "no second runtime at a live admin path");
            let spans = estate.spans();
            for a in &admins2 {
                assert!(
                    named(&spans, "rafka.node_admin.deployment.delete.via-fence")
                        .iter()
                        .any(|sp| attr(sp, "node") == *a && attr(sp, "outcome") == "answers" && start(sp) > healed_at),
                    "the recovery authority asked {a} directly and it answered"
                );
            }
            let heal_from = std::time::Instant::now();
            let healed = wait_for("the heal converges: one admin primary per mesh and one fabric primary", Duration::from_secs(150), || async {
                let n = estate.nodes().await;
                let all_ready = n.len() == names(2).len() && n.iter().all(|x| x["status"] == "ready-for-traffic");
                (all_ready && fabric_primaries(&n).len() == 1 && primary(&n, "mesh1").is_some() && primary(&n, "mesh2").is_some()).then_some(n)
            })
            .await;
            eprintln!("HEAL converged {:?} after the Build", heal_from.elapsed());
            for a in &admins2 {
                assert_eq!(births(&healed)[a], incarnations[a], "{a} kept its path: partition is not death");
            }
            let spans = estate.spans();
            let ev = roles(&spans, "rafka.mesh.backbone.update.via-aggregate-publisher", "mesh");
            assert_eq!(holders(&ev, "mesh2", &BTreeSet::new()).len(), 1, "one mesh2 backbone publisher: {ev:?}");
            let st = roles(&spans, "rafka.mesh.fabric.update.via-status-publisher", "fabric");
            assert_eq!(holders(&st, "fabric3", &BTreeSet::new()).len(), 1, "one fabric status publisher: {st:?}");
        }
        Err(why) => {
            if std::env::var("RAFKA_REQUIRE_NETFAULT").as_deref() == Ok("1") {
                panic!("RAFKA_REQUIRE_NETFAULT=1 but this host cannot drop traffic: {why}");
            }
            eprintln!("SKIP cell 5: {why}");
        }
    }

    // 6. Grow mesh2 (its admin primary executes), and lose mesh2's admin
    // cohort while that attempt is open: the fabric primary continues the
    // same Build, naming the previous executor, and runs only what is left.
    let nodes = estate.nodes().await;
    let p2 = primary(&nodes, "mesh2").expect("mesh2 has an admin primary");
    let (status, a) = estate.post("/api/build", &desired("fabric3", 4)).await;
    assert_eq!(status, 202, "{a}");
    let grow = s(&a["build_id"]);
    wait_for(&format!("{p2} holds the grow's attempt"), Duration::from_secs(30), || async {
        let v = estate.get(&format!("/api/builds?id={grow}")).await.1;
        (v["state"] == "running" && v["executor"] == p2.as_str()).then_some(())
    })
    .await;
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin") {
        kill(n);
    }
    let done = estate.await_build(&grow, Duration::from_secs(180)).await;
    assert!(done["attempt"].as_u64().unwrap() > 1, "a successor continued the Build: {done:#}");
    let spans = estate.spans();
    let took_over = named(&spans, "rafka.node_admin.build.update.via-reconcile")
        .into_iter()
        .any(|sp| attr(sp, "build_id") == grow && attr(sp, "previous_executor") == p2 && attr(sp, "executor") != p2);
    assert!(took_over, "a surviving admin continued {grow} naming {p2} as the previous executor");
    let after = estate.settled(&names(4), Duration::from_secs(60)).await;
    for name in ["mesh2.rpc.3", "mesh2.rpc.4"] {
        let created = named(&spans, "rafka.node_admin.node.create.via-build")
            .into_iter()
            .filter(|sp| attr(sp, "build_id") == grow && attr(sp, "node") == name)
            .filter(|sp| named(&spans, "rafka.node_admin.deployment.update.via-step").iter().any(|st| {
                attr(st, "build_id") == grow && attr(st, "node") == name && attr(st, "step") == "DeployRuntime" && attr(st, "outcome") == "complete" && attr(st, "attempt") == attr(sp, "attempt")
            }))
            .count();
        assert!(created <= 1, "{name} was deployed once: {created}");
    }
    assert_eq!(after.iter().filter(|n| s(&n["name"]).starts_with("mesh2.rpc.")).count(), 4);
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
}
