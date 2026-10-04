//! i143.e4.s5 process E2E: fabric-primary election and the advertised
//! control endpoint (PRD §1.14, §1.16, §11).
//!
//! An MM fabric loses its fabric-primary mesh (every mesh1 process killed).
//! From public surfaces only:
//! - before the loss, `GET /api/fabric` names one fabric primary, an admin
//!   primary of its own mesh, and advertises every mesh's control API;
//! - after it, the test finds control again through those advertised
//!   endpoints alone (no hidden map): the surviving mesh's admin answers, its
//!   fabric view names one new fabric primary (mesh2's admin primary) and
//!   advertises that admin's control API;
//! - control has moved: through that API the test grows mesh2, removes a
//!   node the lost admin had launched, and finally shuts the fabric down; no
//!   runtime of the estate is left running.
//!
//! Evidence: the new fabric primary's election is announced as
//! `rafka.mesh.election.resolve.via-fabric-recompute` (previous: the lost
//! one) by a surviving admin.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::process::Command;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "fabric-primary".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "losing_the_fabric_primary_mesh_moves_control".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

async fn get(base: &str, path: &str) -> Option<Value> {
    let r = reqwest::Client::new().get(format!("{base}{path}")).timeout(Duration::from_secs(2)).send().await.ok()?;
    r.status().is_success().then_some(())?;
    r.json().await.ok()
}

fn kill(data_dir: &str) {
    let d: Value = serde_json::from_slice(&std::fs::read(format!("{data_dir}/deployment.json")).unwrap()).unwrap();
    let pid = d["pid"].as_u64().unwrap();
    assert!(Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success(), "kill -9 {pid}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_fabric_primary_mesh_moves_control() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let desired = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 3},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 3},
    ]});
    let (status, accepted) = estate.post("/api/build", &desired).await;
    assert_eq!(status, 202, "{accepted}");
    estate.await_build(accepted["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;

    // The advertised topology: one fabric primary, every mesh's control API.
    let (_, fabric) = estate.get("/api/fabric").await;
    let old_primary = s(&fabric["fabric_primary"]);
    assert_eq!(old_primary, "mesh1.admin.1", "{fabric:#}");
    assert_eq!(s(&fabric["admin_api_base"]), estate.admin, "the fabric advertises its primary's control API");
    let advertised: Vec<String> = fabric["meshes"].as_array().unwrap().iter().map(|m| s(&m["admin_api_base"])).collect();
    assert!(advertised.iter().all(|b| b.starts_with("http://")), "every mesh advertises its control API: {fabric:#}");

    // Lose mesh1: every one of its processes, the bootstrap admin included.
    let nodes = estate.nodes().await;
    let mesh2_launched_by_lost_admin = "mesh2.rpc.3";
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["name"] != "mesh1.admin.1") {
        kill(n["data_dir"].as_str().unwrap());
    }
    estate.kill_bootstrap();

    // Find control again through the advertised endpoints alone: ask each
    // advertised admin for the fabric, and take the primary it advertises
    // once that primary's own control API answers and agrees.
    let control = wait_for("an advertised admin leads to a live fabric primary", Duration::from_secs(60), || {
        let advertised = advertised.clone();
        let old = old_primary.clone();
        async move {
            for base in &advertised {
                let Some(f) = get(base, "/api/fabric").await else { continue };
                let (primary, primary_base) = (s(&f["fabric_primary"]), s(&f["admin_api_base"]));
                if primary.is_empty() || primary == old {
                    continue;
                }
                let Some(own) = get(&primary_base, "/api/fabric").await else { continue };
                if s(&own["fabric_primary"]) == primary && s(&own["admin_api_base"]) == primary_base {
                    return Some((primary, primary_base, own));
                }
            }
            None
        }
    })
    .await;
    let (new_primary, new_base, fabric) = control;
    estate.admin = new_base.clone();
    // The surviving mesh's view settles: every mesh2 member ready again.
    // Losing half the fabric costs gossip a failure-detection window (the
    // dead peers' connections time out) before mesh2's members are heard.
    wait_for("mesh2 settles in the new primary's view", Duration::from_secs(90), || async {
        let nodes = estate.nodes().await;
        let mesh2: Vec<&Value> = nodes.iter().filter(|n| n["mesh"] == "mesh2").collect();
        (mesh2.len() == 5 && mesh2.iter().all(|n| n["status"] == "ready-for-traffic")).then_some(())
    })
    .await;
    let nodes = estate.nodes().await;
    let holder = nodes.iter().find(|n| n["name"] == new_primary.as_str()).unwrap();
    assert_eq!(holder["is_primary"], true, "the fabric primary is its own mesh's admin primary");
    assert_eq!(nodes.iter().filter(|n| n["is_fabric_primary"] == true).count(), 1, "exactly one fabric primary");
    let mesh2 = fabric["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh2").unwrap();
    assert_eq!(s(&mesh2["admin_api_base"]), new_base, "mesh2 advertises its live owning admin");

    // Control moved: grow mesh2, and remove a node the lost admin launched.
    let (status, a) = estate.post("/api/nodes/spawn", &json!({"mesh": "mesh2", "kind": "rpc_node"})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    assert_eq!(estate.node("mesh2.rpc.4").await["status"], "ready-for-traffic");
    let (status, a) = estate.delete(&format!("/api/nodes/{mesh2_launched_by_lost_admin}")).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    assert!(
        !estate.nodes().await.iter().any(|n| n["name"] == mesh2_launched_by_lost_admin && n["status"] != "dead"),
        "the adopted node was retired"
    );

    // And the whole fabric stops from the new primary.
    estate.stop().await;
    assert_eq!(estate.live_runtimes(), vec![], "no runtime of the estate is left running");
    let spans = estate.spans();
    assert!(
        named(&spans, "rafka.mesh.election.resolve.via-fabric-recompute").iter().any(|sp| {
            let a = &sp["attributes"];
            // The lost mesh's admins fall silent milliseconds apart: the
            // previous holder is whichever mesh1 admin went silent last.
            a["primary"] == new_primary.as_str()
                && a["previous"].as_str().is_some_and(|p| p.starts_with("mesh1.admin."))
                && a["observer"].as_str().is_some_and(|o| o.starts_with("mesh2."))
        }),
        "a surviving admin announced the new fabric primary, succeeding the lost mesh's"
    );
}

/// A member that is unheard (frozen: its process runs but says nothing) is
/// dead in the view, so a reconcile recreates its path. The new birth fences
/// the old one first: a path never has two live runtimes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_birth_fences_an_unheard_predecessor_at_its_path() {
    let mut estate = Estate::bootstrap(
        Owner { subfeature: "path-fence".into(), rung: "MN".into(), test: "a_new_birth_fences_an_unheard_predecessor".into(), ..owner() },
        "fabric1",
        "mesh1",
    )
    .await;
    let desired = json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 2}]});
    let (_, a) = estate.post("/api/build", &desired).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let frozen = estate.node("mesh1.rpc.2").await;
    let dir = s(&frozen["data_dir"]);
    let pid: u64 = serde_json::from_slice::<Value>(&std::fs::read(format!("{dir}/deployment.json")).unwrap()).unwrap()["pid"].as_u64().unwrap();
    assert!(Command::new("kill").args(["-STOP", &pid.to_string()]).status().unwrap().success());
    wait_for("the frozen member is dead in the view", Duration::from_secs(30), || async {
        (estate.node("mesh1.rpc.2").await["status"] == "dead").then_some(())
    })
    .await;

    let (_, a) = estate.post("/api/build", &desired).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let now = estate.node("mesh1.rpc.2").await;
    assert_eq!(now["status"], "ready-for-traffic");
    assert_ne!(now["incarnation_id"], frozen["incarnation_id"], "a new birth holds the path");
    let at_path: Vec<_> = estate.live_runtimes().into_iter().filter(|(d, _)| d.file_name().unwrap().to_string_lossy().starts_with("mesh1.rpc.2-")).collect();
    assert_eq!(at_path.len(), 1, "one live runtime at the path: {at_path:?}");
    assert_ne!(u64::from(at_path[0].1), pid, "the frozen predecessor was stopped");

    estate.stop().await;
    let spans = estate.spans();
    assert!(
        named(&spans, "rafka.node_admin.deployment.delete.via-fence")
            .iter()
            .any(|sp| sp["attributes"]["node"] == "mesh1.rpc.2" && sp["attributes"]["outcome"] == "terminated"),
        "the fence is in the evidence"
    );
}
