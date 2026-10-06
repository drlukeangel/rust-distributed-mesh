//! i143.e4.s14 process E2E: a lower-NodeId admin born after a Build
//! completed hydrates, wins, and manages that Build's births (rafka-v2#2840
//! acceptance 3–4; the RED of trace `eb8f18449f2d1479`).
//!
//! Build A grows mesh1 and completes; its history is forgotten. Admins are
//! then born one at a time until one draws a NodeId lower than the incumbent
//! admin's (NodeIds are random, so a birth wins with the incumbent's share of the
//! id space; a loser is removed again). The incumbent is drawn first from the
//! upper 70% of the space (an estate whose bootstrap id is lower is stopped and
//! bootstrapped again), so each birth wins at least 30% of the time. That late
//! admin:
//! - holds the current desired topology and every held birth's runtime
//!   before it commits Ready;
//! - is elected (lowest ready NodeId: node-admin cohort, mesh, fabric);
//! - restarts and retires births A launched, through its own control API,
//!   from their published runtimes, with A's receipts nowhere.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "late-authority".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// One topology request, accepted by whichever admin holds the fabric now (the estate follows
/// the seat), run to convergence.
async fn build(estate: &Estate, method: &str, path: &str, body: Value) -> String {
    let (status, a) = match method {
        "POST" => estate.post(path, &body).await,
        _ => estate.delete(path).await,
    };
    assert_eq!(status, 202, "{method} {path}: {a}");
    let id = s(&a["build_id"]);
    // Every admin holds the fabric's Build facts: any of them can be asked.
    estate.await_build(&id, Duration::from_secs(120)).await;
    id
}

/// The share of the 60-bit id space below `id` (canonical Crockford, most significant first),
/// read from its first two characters.
fn share_below(id: &str) -> f64 {
    const ALPHABET: &str = "0123456789abcdefghjkmnpqrstvwxyz";
    let digit = |c: char| ALPHABET.find(c).unwrap_or(0) as f64;
    let mut chars = id.chars();
    let (a, b) = (chars.next().map_or(0.0, digit), chars.next().map_or(0.0, digit));
    (a * 32.0 + b) / 1024.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births() {
    // An incumbent near the bottom of the id space can never be beaten by a later birth: draw it
    // from the upper 70%.
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    for _ in 0..5 {
        let me = wait_for("the bootstrap admin lists itself", Duration::from_secs(30), || async {
            estate.nodes().await.into_iter().find(|n| n["name"] == "mesh1.admin.1" && !n["node_id"].as_str().unwrap_or_default().is_empty())
        })
        .await;
        if share_below(&s(&me["node_id"])) >= 0.3 {
            break;
        }
        estate.stop().await;
        estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    }
    let base = estate.admin.clone();
    // Build A: three rpc nodes, launched by the bootstrap admin. The accepted Build is never
    // history: it is forgotten only once a later Build holds the pointer.
    let a = build(&estate, "POST", "/api/build", json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 3}]})).await;
    let nodes = estate.settled_shape(&[("mesh1", 1, 3)], Duration::from_secs(30)).await;
    let incumbent = nodes.iter().find(|n| n["name"] == "mesh1.admin.1").cloned().unwrap();
    let a_births: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "rpc_node").cloned().collect();
    let (status, v) = estate.delete_raw(&format!("/api/builds?id={a}")).await;
    assert_eq!(status, 409, "the accepted Build is never history: {v}");

    // Births until one draws a lower NodeId than the incumbent's.
    let mut late = None;
    for _ in 0..30 {
        build(&estate, "POST", "/api/nodes/spawn", json!({"mesh": "mesh1", "kind": "node_admin"})).await;
        let nodes = estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
        let born = nodes.iter().find(|n| n["kind"] == "node_admin" && n["name"] != "mesh1.admin.1").cloned().unwrap();
        if s(&born["node_id"]) < s(&incumbent["node_id"]) {
            late = Some(born);
            break;
        }
        build(&estate, "DELETE", &format!("/api/nodes/{}", s(&born["name"])), Value::Null).await;
        estate.settled_shape(&[("mesh1", 1, 3)], Duration::from_secs(30)).await;
    }
    let late = late.unwrap_or_else(|| {
        panic!(
            "none of 30 births drew a NodeId below the incumbent {} (it holds {:.0}% of the space below it)",
            s(&incumbent["node_id"]),
            share_below(&s(&incumbent["node_id"])) * 100.0
        )
    });
    let late_name = s(&late["name"]);
    let late_base = s(&late["admin_api_base"]);

    // It holds every seat: the lowest ready NodeId of the cohort, the mesh
    // and the fabric (one mesh).
    estate.admin = late_base.clone();
    wait_for("the late admin holds the cohort, the mesh and the fabric", Duration::from_secs(30), || async {
        let n = estate.node(&late_name).await;
        (n["is_primary"] == true && n["is_fabric_primary"] == true).then_some(())
    })
    .await;
    assert_eq!(estate.node("mesh1.admin.1").await["is_primary"], false, "the incumbent does not keep the seat");
    // A later Build holds the pointer now: A is history on the admin that ran it, and leaves; the
    // late admin never held it.
    let (status, v) = estate.delete_at(&base, &format!("/api/builds?id={a}")).await;
    assert_eq!(status, 204, "{v}");
    assert_eq!(estate.get(&format!("/api/builds?id={a}")).await.0, 404, "A's history is nowhere");

    // It restarts and retires A's births.
    let (restarted, retired) = (a_births[0].clone(), a_births[1].clone());
    build(&estate, "POST", &format!("/api/nodes/{}/restart", s(&restarted["name"])), Value::Null).await;
    let after = wait_for("the restarted birth is ready under a new incarnation", Duration::from_secs(30), || async {
        let n = estate.node(&s(&restarted["name"])).await;
        (n["status"] == "ready-for-traffic" && n["incarnation_id"] != restarted["incarnation_id"]).then_some(n)
    })
    .await;
    assert_eq!(after["node_id"], restarted["node_id"], "a restart keeps the NodeId");
    build(&estate, "DELETE", &format!("/api/nodes/{}", s(&retired["name"])), Value::Null).await;
    wait_for("the retired birth leaves the view", Duration::from_secs(30), || async {
        estate.nodes().await.iter().all(|n| n["name"] != retired["name"] || n["incarnation_id"] != retired["incarnation_id"]).then_some(())
    })
    .await;

    estate.stop().await;
    let spans = estate.spans();
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap();
    // Before Ready: Fabric.build_id (taken from the fabric control topic) and every held birth's
    // runtime.
    let ready = named(&spans, "rafka.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == late_name.as_str() && sp["attributes"]["incarnation_id"] == late["incarnation_id"])
        .cloned()
        .expect("the late admin is ready");
    assert!(
        named(&spans, "rafka.node_admin.fabric.update.via-build-accepted")
            .into_iter()
            .any(|sp| sp["attributes"]["node"] == late_name.as_str() && sp["attributes"]["via"].as_str().is_some_and(|v| v != "day-0" && v != "accepted") && at(sp) < at(&ready)),
        "it held Fabric.build_id before Ready"
    );
    let held = ready["attributes"]["runtime_facts_held"].as_str().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    assert!(held >= 4, "it held the incumbent's and A's three births' runtimes before Ready: {held}");
    // Elected by the lowest NodeId, at every level.
    assert!(named(&spans, "rafka.mesh.election.resolve.via-recompute")
        .iter()
        .any(|sp| sp["attributes"]["kind"] == "node_admin" && sp["attributes"]["winner_node_id"] == late["node_id"] && sp["attributes"]["election_key"] == "node_id_crockford"));
    assert!(named(&spans, "rafka.mesh.election.resolve.via-fabric-recompute").iter().any(|sp| sp["attributes"]["winner_node_id"] == late["node_id"]));
    // It managed A's births from their published runtimes.
    for birth in [&restarted, &retired] {
        let adopted = named(&spans, "rafka.node_admin.runtime.update.via-adopt")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == birth["name"] && sp["attributes"]["incarnation_id"] == birth["incarnation_id"])
            .unwrap_or_else(|| panic!("{} adopted by the late admin", birth["name"]));
        assert_eq!(adopted["attributes"]["adopter"], late_name.as_str());
        assert_eq!(adopted["attributes"]["source"], "self-published-membership");
    }
}
