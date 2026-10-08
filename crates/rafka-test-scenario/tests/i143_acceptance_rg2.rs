//! i143 R-G2 acceptance (Luke 2026-10-08), CHAOS-PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-rg2-chaos-process`, which exports `I143_ACCEPTANCE_DIR`
//! and whose command sets `RDM_ARTIFACTS_DIR` at the test cadence (staleness 3 s, gossip 500 ms).
//!
//! The estate: two meshes of two node-admins and two rpc nodes each, settled through a Build.
//!
//! CONTRACT: forwarded topology carries topology, never liveness. An ordinary node (an rpc node)
//! holds a peer mesh's members as topology: across 60 s with nothing changing, and across a cut of
//! every UDP path between the meshes, it never marks a peer mesh silent. A node-admin judges a
//! peer mesh from the backbone alone: it marks the mesh silent in the cut and learns it again
//! after the heal.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CELL: &str = "ordinary_nodes_hold_peer_mesh_topology_through_quiet_and_cut";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-rg2".into(),
        subfeature: "forwarded-topology".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: CELL.into(),
    }
}

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/rg2/chaos-process").join(CELL),
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

/// Who marked which mesh silent after `since`.
fn silent_since(spans: &[Value], since: u64) -> Vec<(String, String)> {
    named(spans, "rdm.mesh.membership.update.via-mesh-silent").into_iter().filter(|sp| at(sp) >= since).map(|sp| (attr(sp, "node"), attr(sp, "mesh"))).collect()
}

/// CONTRACT: see the module docs. No rpc node marks any mesh silent in 60 s of nothing changing
/// nor in a cut that outlasts the forwarded staleness floor; every node-admin marks the peer mesh
/// silent in the cut and learns it again after the heal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_nodes_hold_peer_mesh_topology_through_quiet_and_cut() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
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
    let ordinary: BTreeSet<String> = nodes.iter().filter(|n| n["kind"] == "rpc_node").map(|n| s(&n["name"])).collect();
    let admins: BTreeSet<String> = nodes.iter().filter(|n| n["kind"] == "node_admin").map(|n| s(&n["name"])).collect();
    assert_eq!((ordinary.len(), admins.len()), (4, 4), "{nodes:#?}");
    let side = |m: &str| -> Vec<String> { nodes.iter().filter(|n| n["mesh"] == m).map(|n| s(&n["name"])).collect() };
    let (mesh1, mesh2) = (side("mesh1"), side("mesh2"));

    // Quiet: every node holds both meshes, then 60 s pass with nothing changing.
    wait_for("every node has learned both meshes", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let learned: BTreeSet<(String, String)> = named(&spans, "rdm.mesh.membership.update.via-mesh-learned").into_iter().map(|sp| (attr(sp, "node"), attr(sp, "mesh"))).collect();
        nodes.iter().all(|n| ["mesh1", "mesh2"].iter().all(|m| learned.contains(&(s(&n["name"]), m.to_string())))).then_some(())
    })
    .await;
    let quiet_from = now_ns();
    tokio::time::sleep(Duration::from_secs(60)).await;
    let quiet = silent_since(&estate.spans(), quiet_from);
    assert!(quiet.is_empty(), "no node marks any mesh silent in 60 s of nothing changing: {quiet:?}");

    // Cut: every UDP path between the meshes, held past the forwarded staleness floor.
    let cut = Partition::start(&udp_ports(&nodes, &mesh1), &udp_ports(&nodes, &mesh2)).unwrap_or_else(|why| panic!("RDM_REQUIRE_NETFAULT: {why}"));
    let cut_at = now_ns();
    let admin_silent = wait_for("every node-admin marks the peer mesh silent", Duration::from_secs(60), || async {
        let silent = silent_since(&estate.spans(), cut_at);
        admins.iter().all(|a| silent.iter().any(|(n, _)| n == a)).then_some(silent)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let during_cut = silent_since(&estate.spans(), cut_at);
    let ordinary_during: Vec<&(String, String)> = during_cut.iter().filter(|(n, _)| ordinary.contains(n)).collect();
    assert!(ordinary_during.is_empty(), "an ordinary node holds the peer mesh as topology through the cut: {ordinary_during:?}");
    drop(cut);
    let healed_at = now_ns();
    wait_for("every node-admin learns the peer mesh again", Duration::from_secs(60), || async {
        let spans = estate.spans();
        let learned: BTreeSet<String> = named(&spans, "rdm.mesh.membership.update.via-mesh-learned").into_iter().filter(|sp| at(sp) >= healed_at).map(|sp| attr(sp, "node")).collect();
        admins.iter().all(|a| learned.contains(a)).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let after = silent_since(&estate.spans(), healed_at);
    let ordinary_after: Vec<&(String, String)> = after.iter().filter(|(n, _)| ordinary.contains(n)).collect();
    assert!(ordinary_after.is_empty(), "an ordinary node never marks a mesh silent after the cut: {ordinary_after:?}");

    let result = json!({
        "cell": CELL, "quiet_from_unix_nano": quiet_from, "quiet_silent": quiet.len(), "cut_at_unix_nano": cut_at, "healed_at_unix_nano": healed_at,
        "admins_silent_in_cut": admin_silent.iter().map(|(n, m)| json!({"node": n, "mesh": m})).collect::<Vec<_>>(),
        "ordinary_nodes": ordinary, "ordinary_silent_in_cut": ordinary_during.len(), "ordinary_silent_after_heal": ordinary_after.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
}
