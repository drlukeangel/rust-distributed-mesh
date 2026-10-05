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

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-membership".into(),
        subfeature: "backbone".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "membership_rides_mesh_channels_and_the_admin_backbone".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
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

/// `(node, mesh)` -> the role spans, in time order.
fn roles(spans: &[Value], name: &str, key: &str) -> Vec<(u64, String, String, String)> {
    let mut v: Vec<_> = named(spans, name).into_iter().map(|sp| (start(sp), attr(sp, "node"), attr(sp, key), attr(sp, "role"))).collect();
    v.sort();
    v
}

/// Who holds `role` for `scope` now, by the last start/stop per node.
fn holders(events: &[(u64, String, String, String)], scope: &str, dead: &BTreeSet<String>) -> BTreeSet<String> {
    let mut last: BTreeMap<&str, &str> = BTreeMap::new();
    for (_, node, _, role) in events.iter().filter(|e| e.2 == scope) {
        last.insert(node, role);
    }
    last.into_iter().filter(|(n, r)| *r == "start" && !dead.contains(*n)).map(|(n, _)| n.to_string()).collect()
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
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (_, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [
            {"name": "mesh1", "node_admin": 2, "rpc_node": 2},
            {"name": "mesh2", "node_admin": 2, "rpc_node": 2},
        ]}))
        .await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let want: BTreeSet<String> = ["mesh1", "mesh2"]
        .iter()
        .flat_map(|m| (1..=2).flat_map(move |i| [format!("{m}.admin.{i}"), format!("{m}.rpc.{i}")]))
        .collect();
    let nodes = estate.settled(&want, Duration::from_secs(30)).await;
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
    let none = BTreeSet::new();
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
    let ev = roles(&estate.spans(), "rafka.mesh.backbone.update.via-aggregate-publisher", "mesh");
    for n in admins.iter().filter(|n| ["mesh1", "mesh2"].iter().all(|m| s(&primary(&nodes, m)["name"]) != **n)) {
        assert!(!ev.iter().any(|e| &e.1 == n), "the non-primary admin {n} never publishes: {ev:?}");
    }

    // 3 + 5. Lose mesh2's admin primary: its successor publishes mesh2 and
    // forwards mesh1, and mesh2's rpc nodes hold mesh1 again.
    let old = primary(&nodes, "mesh2");
    let old_name = s(&old["name"]);
    kill(&old);
    let dead: BTreeSet<String> = [old_name.clone()].into_iter().collect();
    let successor = wait_for("mesh2 elects a successor", Duration::from_secs(30), || async {
        let p = s(&primary(&estate.nodes().await, "mesh2")["name"]);
        (!p.is_empty() && p != old_name).then_some(p)
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

    // 4. Lose the fabric primary: status publication moves, never doubled.
    let nodes = estate.nodes().await;
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric primary");
    let fp_name = s(&fp["name"]);
    let (_, fabric) = estate.get("/api/fabric").await;
    let survivors: Vec<String> = fabric["meshes"].as_array().unwrap().iter().map(|m| s(&m["admin_api_base"])).filter(|b| *b != s(&fp["admin_api_base"])).collect();
    if fp_name == "mesh1.admin.1" {
        estate.kill_bootstrap();
    } else {
        kill(&fp);
    }
    let mut dead = dead;
    dead.insert(fp_name.clone());
    estate.admin = survivors[0].clone();
    let next = wait_for("a new fabric primary", Duration::from_secs(40), || async {
        let n = estate.nodes().await;
        n.iter().find(|x| x["is_fabric_primary"] == true && x["name"] != fp_name.as_str()).map(|x| s(&x["name"]))
    })
    .await;
    wait_for("the new fabric primary alone publishes the fabric's status", Duration::from_secs(20), || {
        let spans = estate.spans();
        let (next, dead) = (next.clone(), dead.clone());
        async move {
            let ev = roles(&spans, "rafka.mesh.fabric.update.via-status-publisher", "fabric");
            (holders(&ev, "fabric1", &dead) == [next].into_iter().collect()).then_some(())
        }
    })
    .await;
    let ev = roles(&estate.spans(), "rafka.mesh.fabric.update.via-status-publisher", "fabric");
    let starts: Vec<&(u64, String, String, String)> = ev.iter().filter(|e| e.3 == "start").collect();
    assert_eq!(starts.len(), 2, "one publisher before the loss, one after: {ev:?}");

    // Stop through whoever holds the fabric now.
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
}
