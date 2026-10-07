//! i143.e4.s5 process E2E: fabric-primary election and the advertised
//! control endpoint (PRD §1.14, §1.16, §11).
//!
//! The fabric primary is the mesh primary with the lowest NodeId (i143.e4.s14).
//!
//! An MM fabric loses its fabric-primary mesh (every process of the mesh
//! that holds the fabric killed). From public surfaces only:
//! - before the loss, `GET /api/fabric` names one fabric primary, the one
//!   computed from the advertised NodeIds, and advertises every mesh's
//!   control API;
//! - after it, the test finds control again through those advertised
//!   endpoints alone (no hidden map): the surviving mesh's admin answers, its
//!   fabric view names one new fabric primary (the surviving mesh's primary)
//!   and advertises that admin's control API;
//! - control has moved: through that API the test grows the surviving mesh,
//!   removes one of its nodes, and finally shuts the fabric down; no runtime
//!   of the estate is left running.
//!
//! A three-mesh fabric with shuffled names elects the lowest mesh-primary
//! NodeId; every mesh primary reports the same winner; losing the winner
//! moves the seat to the next-lowest mesh primary.
//!
//! Evidence: the new fabric primary's election is announced as
//! `rafka.mesh.election.resolve.via-fabric-recompute` (`election_level =
//! fabric_primary`, the winner's NodeId, path and mesh) by a surviving mesh
//! primary.

use rafka_test_scenario::elections::{expected_fabric_primary, seats_as_expected};
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

fn kill(pid: u64) {
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
    // The Build may complete on another admin: this view hears every birth first.
    estate.settled_shape(&[("mesh1", 2, 3), ("mesh2", 2, 3)], Duration::from_secs(30)).await;

    // The advertised topology: one fabric primary, every mesh's control API.
    let (_, fabric) = estate.get("/api/fabric").await;
    let old_primary = s(&fabric["fabric_primary"]);
    let nodes = estate.nodes().await;
    assert_eq!(Some(old_primary.clone()), expected_fabric_primary(&nodes), "the lowest mesh-primary NodeId: {nodes:#?}");
    let old_base = s(&nodes.iter().find(|n| n["name"] == old_primary.as_str()).unwrap()["admin_api_base"]);
    assert_eq!(s(&fabric["admin_api_base"]), old_base, "the fabric advertises its primary's control API");
    let lost = old_primary.split('.').next().unwrap().to_string();
    let survivor = if lost == "mesh1" { "mesh2" } else { "mesh1" }.to_string();
    let advertised: Vec<String> = fabric["meshes"].as_array().unwrap().iter().map(|m| s(&m["admin_api_base"])).collect();
    assert!(advertised.iter().all(|b| b.starts_with("http://")), "every mesh advertises its control API: {fabric:#}");

    // Lose the fabric primary's mesh: every one of its processes.
    let removed = format!("{survivor}.rpc.3");
    // Every pid first: an admin's view goes with it.
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == lost.as_str() && n["name"] != "mesh1.admin.1") {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    for pid in pids {
        kill(pid);
    }
    if lost == "mesh1" {
        estate.kill_bootstrap();
    }

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
    // The surviving mesh's view settles: every one of its members ready again.
    // Losing half the fabric costs gossip a failure-detection window (the
    // dead peers' connections time out) before mesh2's members are heard.
    wait_for("the surviving mesh settles in the new primary's view", Duration::from_secs(90), || async {
        let nodes = estate.nodes().await;
        let alive: Vec<&Value> = nodes.iter().filter(|n| n["mesh"] == survivor.as_str()).collect();
        (alive.len() == 5 && alive.iter().all(|n| n["status"] == "ready-for-traffic")).then_some(())
    })
    .await;
    let nodes = estate.nodes().await;
    let holder = nodes.iter().find(|n| n["name"] == new_primary.as_str()).unwrap().clone();
    assert_eq!(holder["is_primary"], true, "the fabric primary is its own mesh's admin primary");
    assert_eq!(nodes.iter().filter(|n| n["is_fabric_primary"] == true).count(), 1, "exactly one fabric primary");
    assert_eq!(new_primary.split('.').next(), Some(survivor.as_str()), "the surviving mesh's primary holds the fabric");
    let held = fabric["meshes"].as_array().unwrap().iter().find(|m| m["name"] == survivor.as_str()).unwrap();
    assert_eq!(s(&held["admin_api_base"]), new_base, "the surviving mesh advertises its live owning admin");

    // Control moved: grow the surviving mesh, and remove one of its nodes.
    let (status, a) = estate.post("/api/nodes/spawn", &json!({"mesh": survivor, "kind": "rpc_node"})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    assert_eq!(estate.node(&format!("{survivor}.rpc.4")).await["status"], "ready-for-traffic");
    let (status, a) = estate.delete(&format!("/api/nodes/{removed}")).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    assert!(!estate.nodes().await.iter().any(|n| n["name"] == removed.as_str() && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))), "the node was retired");

    // And the whole fabric stops from the new primary.
    estate.stop().await;
    assert_eq!(estate.live_runtimes(), vec![], "no runtime of the estate is left running");
    let spans = estate.spans();
    assert!(
        named(&spans, "rafka.mesh.election.resolve.via-fabric-recompute").iter().any(|sp| {
            let a = &sp["attributes"];
            // The lost mesh's admins fall silent milliseconds apart: the
            // previous holder is whichever mesh1 admin went silent last.
            a["election_level"] == "fabric_primary"
                && a["winner_path"] == new_primary.as_str()
                && a["winner_node_id"] == holder["node_id"]
                && a["winner_mesh"] == survivor.as_str()
                && a["election_key"] == "node_id_crockford"
                && a["previous"].as_str().is_some_and(|p| p.starts_with(&format!("{lost}.admin.")))
                && a["observer"] == new_primary.as_str()
        }),
        "a surviving admin announced the new fabric primary, succeeding the lost mesh's"
    );
}

/// A member that is unheard (frozen: its process runs but says nothing) is dead in the view, but
/// silence is never death proof (Luke 2026-10-05): the desired Build resubmitted holds it. No new
/// birth takes the path, nothing is terminated, and the fence names the hold (`held-running`).
/// Must NOT happen: a replacement birth at the path, or `via-fence outcome=terminated`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_member_whose_runtime_still_runs_is_held_not_replaced() {
    let mut estate = Estate::bootstrap(
        Owner { subfeature: "path-fence".into(), rung: "MN".into(), test: "a_silent_member_whose_runtime_still_runs_is_held".into(), ..owner() },
        "fabric1",
        "mesh1",
    )
    .await;
    let desired = json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 2}]});
    let (_, a) = estate.post("/api/build", &desired).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let frozen = wait_for("this admin hears mesh1.rpc.2", Duration::from_secs(30), || async {
        estate.nodes().await.into_iter().find(|n| n["name"] == "mesh1.rpc.2")
    })
    .await;
    let pid = estate.pid_of("mesh1.rpc.2").await;
    assert!(Command::new("kill").args(["-STOP", &pid.to_string()]).status().unwrap().success());
    wait_for("the frozen member is dead in the view", Duration::from_secs(30), || async {
        (estate.node_opt("mesh1.rpc.2").await?["status"].as_str().is_some_and(|s| s == "dead" || s == "pending-reconnect")).then_some(())
    })
    .await;

    let (_, a) = estate.post("/api/build", &desired).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let at_path: Vec<_> = estate.live_runtimes().into_iter().filter(|(d, _)| d.file_name().unwrap().to_string_lossy().starts_with("mesh1.rpc.2-")).collect();
    assert_eq!(at_path.len(), 1, "one runtime at the path, the frozen one: {at_path:?}");
    assert_eq!(u64::from(at_path[0].1), pid, "the frozen predecessor is held, never terminated");
    assert_eq!(estate.node("mesh1.rpc.2").await["incarnation_id"], frozen["incarnation_id"], "no new birth at the path");

    assert!(Command::new("kill").args(["-CONT", &pid.to_string()]).status().unwrap().success());
    estate.stop().await;
    let spans = estate.spans();
    let fences: Vec<&Value> = named(&spans, "rafka.node_admin.deployment.delete.via-fence").into_iter().filter(|sp| sp["attributes"]["node"] == "mesh1.rpc.2").collect();
    assert!(fences.iter().any(|sp| sp["attributes"]["outcome"] == "held-running"), "the hold is in the evidence: {fences:?}");
    assert!(!fences.iter().any(|sp| sp["attributes"]["outcome"] == "terminated"), "nothing was terminated on silence: {fences:?}");
}

/// The positive arm: a member whose exact runtime the provider inspects as `Exited` is
/// canonically dead, so the same desired topology restores the count with a new birth at the
/// lowest free path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_whose_runtime_exited_is_replaced() {
    let mut estate = Estate::bootstrap(
        Owner { subfeature: "path-fence".into(), rung: "MN".into(), test: "a_member_whose_runtime_exited_is_replaced".into(), ..owner() },
        "fabric1",
        "mesh1",
    )
    .await;
    let desired = json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 2}]});
    let (_, a) = estate.post("/api/build", &desired).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let gone = wait_for("this admin hears mesh1.rpc.2", Duration::from_secs(30), || async {
        estate.nodes().await.into_iter().find(|n| n["name"] == "mesh1.rpc.2")
    })
    .await;
    kill(estate.pid_of("mesh1.rpc.2").await);
    wait_for("a new birth restores two ready rpc nodes", Duration::from_secs(60), || async {
        let nodes = estate.nodes().await;
        let ready: Vec<&Value> = nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["status"] == "ready-for-traffic").collect();
        (ready.len() == 2 && !ready.iter().any(|n| n["incarnation_id"] == gone["incarnation_id"])).then_some(())
    })
    .await;
    estate.stop().await;
}

/// Three meshes with shuffled names: the fabric primary is the lowest
/// mesh-primary NodeId whatever the names; every mesh primary resolves the
/// same winner; losing it moves the seat to the next-lowest mesh primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_mesh_primaries_elect_the_lowest_node_id_and_fail_over_to_the_next() {
    let mut estate = Estate::bootstrap(
        Owner { subfeature: "fabric-primary".into(), rung: "MMM".into(), test: "three_mesh_primaries_elect_the_lowest_node_id".into(), ..owner() },
        "fabric1",
        "mesh1",
    )
    .await;
    let desired = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh3", "node_admin": 2, "rpc_node": 1},
        {"name": "mesh1", "node_admin": 2, "rpc_node": 1},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 1},
    ]});
    let (status, a) = estate.post("/api/build", &desired).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let all_ready = |nodes: &[Value]| nodes.len() == 9 && nodes.iter().all(|n| n["status"] == "ready-for-traffic");
    let nodes = wait_for("three meshes settle on the computed seats", Duration::from_secs(30), || async {
        let nodes = estate.nodes().await;
        (all_ready(&nodes) && seats_as_expected(&nodes).is_ok()).then_some(nodes)
    })
    .await;
    let winner = expected_fabric_primary(&nodes).unwrap();
    let winner_node = nodes.iter().find(|n| n["name"] == winner.as_str()).unwrap().clone();
    // Every mesh primary's own view advertises the same winner.
    let mesh_primaries: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "node_admin" && n["is_primary"] == true).cloned().collect();
    assert_eq!(mesh_primaries.len(), 3);
    for mp in &mesh_primaries {
        let view = wait_for(&format!("{} agrees", mp["name"]), Duration::from_secs(30), || async {
            let v = estate.nodes_at(&s(&mp["admin_api_base"])).await;
            (all_ready(&v) && seats_as_expected(&v).is_ok()).then_some(v)
        })
        .await;
        assert_eq!(expected_fabric_primary(&view).as_deref(), Some(winner.as_str()), "{} resolves the same fabric primary", mp["name"]);
    }

    // Lose the winner: its mesh elects its other admin; the fabric recomputes.
    let survivor_base = s(&mesh_primaries.iter().find(|m| m["name"] != winner.as_str()).unwrap()["admin_api_base"]);
    if winner == "mesh1.admin.1" {
        estate.kill_bootstrap();
    } else {
        kill(estate.pid_of(&winner).await);
    }
    estate.admin = survivor_base.clone();
    // The killed winner is tracked by its NodeId, never its path: drift recovery reborns the
    // exited path at once under a NEW NodeId (the dead incarnation is gone or `dead`), and that
    // rebirth may even take the seat back by NodeId order. What moved is the seat away from the
    // killed NodeId.
    let winner_id = s(&winner_node["node_id"]);
    let after = wait_for("the fabric seat moves off the killed winner's NodeId", Duration::from_secs(60), || async {
        let v = estate.nodes_at(&survivor_base).await;
        let fp = expected_fabric_primary(&v);
        let fp_id = fp.as_deref().and_then(|name| v.iter().find(|n| n["name"] == name)).map(|n| s(&n["node_id"]));
        let killed_gone = !v.iter().any(|n| s(&n["node_id"]) == winner_id && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect")));
        (killed_gone && fp_id.is_some() && fp_id.as_deref() != Some(winner_id.as_str()) && seats_as_expected(&v).is_ok()).then_some(v)
    })
    .await;
    let next = expected_fabric_primary(&after).unwrap();
    let next_id = s(&after.iter().find(|n| n["name"] == next.as_str()).unwrap()["node_id"]);
    let others: Vec<String> = after
        .iter()
        .filter(|n| n["kind"] == "node_admin" && n["is_primary"] == true && n["name"] != next.as_str())
        .map(|n| s(&n["node_id"]))
        .collect();
    assert!(others.iter().all(|o| *o > next_id), "the next-lowest mesh primary: {next_id} < {others:?}");

    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
    let spans = estate.spans();
    // Each mesh primary that observed the first resolution reported the same winner.
    let first: Vec<&Value> = named(&spans, "rafka.mesh.election.resolve.via-fabric-recompute")
        .into_iter()
        .filter(|sp| sp["attributes"]["winner_node_id"] == winner_node["node_id"])
        .collect();
    let observers: std::collections::BTreeSet<String> = first.iter().map(|sp| s(&sp["attributes"]["observer"])).collect();
    assert!(observers.len() >= 2, "more than one mesh primary resolved the same winner: {observers:?}");
    assert!(first.iter().all(|sp| sp["attributes"]["election_level"] == "fabric_primary" && sp["attributes"]["election_key"] == "node_id_crockford"));
    assert!(
        named(&spans, "rafka.mesh.election.resolve.via-fabric-recompute").iter().any(|sp| {
            let a = &sp["attributes"];
            a["winner_node_id"] == next_id.as_str() && a["previous_node_id"] == winner_node["node_id"]
        }),
        "the failover to the next-lowest mesh primary was announced"
    );
}
