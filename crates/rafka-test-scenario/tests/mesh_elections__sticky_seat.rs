//! Ruling R-A2 process cells: the fabric-primary seat stays in its mesh until that mesh is proven
//! unable to hold it, and a silent holder is looked at, never replaced for being silent.
//!
//! Each cell runs a two-mesh fabric of two node-admins and three rpc nodes per mesh. A cell that
//! kills the fabric primary is permitted by R-P1; no other cell kills a fabric-primary node-admin.

use rafka_test_scenario::estate::{iroh_observation_shape, named, wait_for, Estate, Owner};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::process::Command;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "sticky-seat".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

fn names() -> BTreeSet<String> {
    ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=3).map(move |i| format!("{m}.rpc.{i}")))).collect()
}

async fn fabric(cell: &str) -> (Estate, Vec<Value>) {
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 3},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 3},
    ]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    estate.await_attempt(&s(&a["build_id"]), Estate::attempt_of(&a), Duration::from_secs(120)).await;
    let nodes = estate.settled(&names(), Duration::from_secs(30)).await;
    (estate, nodes)
}

fn node<'a>(nodes: &'a [Value], name: &str) -> &'a Value {
    nodes.iter().find(|n| n["name"] == name).unwrap_or_else(|| panic!("{name} is not in the view"))
}

fn fabric_primary(nodes: &[Value]) -> Option<String> {
    nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"]))
}

async fn kill(estate: &mut Estate, name: &str) {
    if name == "mesh1.admin.1" {
        estate.kill_bootstrap();
    } else {
        let pid = estate.pid_of(name).await;
        assert!(Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success(), "kill -9 {pid}");
    }
}

/// CONTRACT: the fabric primary dies while another admin of its mesh survives. The seat goes to
/// that admin, the lowest Ready NodeId of the SAME mesh, whatever the other mesh's primary's id
/// is, and the new holder says so (`rdm.mesh.seat.update.via-announce`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_fabric_primary_hands_the_seat_to_the_surviving_admin_of_its_own_mesh() {
    let (mut estate, nodes) = fabric("a_dead_fabric_primary_hands_the_seat_to_the_surviving_admin_of_its_own_mesh").await;
    let old = fabric_primary(&nodes).expect("one fabric primary");
    let mesh = old.split('.').next().unwrap().to_string();
    let heir = if old.ends_with(".1") { format!("{mesh}.admin.2") } else { format!("{mesh}.admin.1") };
    let other_mesh = if mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let other_primary = nodes.iter().find(|n| n["mesh"] == other_mesh && n["is_primary"] == true).map(|n| (s(&n["name"]), s(&n["node_id"]))).unwrap();
    let heir_id = s(&node(&nodes, &heir)["node_id"]);
    let discriminating = other_primary.1 < heir_id;
    estate.admin = s(&node(&nodes, &heir)["admin_api_base"]);
    kill(&mut estate, &old).await;
    let seen = wait_for("the surviving admin of the fabric primary's mesh holds the seat", Duration::from_secs(90), || async {
        let ns = estate.nodes().await;
        (fabric_primary(&ns).as_deref() == Some(heir.as_str())).then_some(ns)
    })
    .await;
    // The seat stays with the heir: the other mesh's primary does not take it back from it.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let later = estate.nodes().await;
    estate.stop().await;
    assert_eq!(fabric_primary(&later).as_deref(), Some(heir.as_str()), "the heir keeps the seat: {later:#?}");
    assert_eq!(seen.iter().filter(|n| n["is_fabric_primary"] == true).count(), 1);
    let spans = estate.spans();
    assert!(
        named(&spans, "rdm.mesh.seat.update.via-announce").iter().any(|sp| attr(sp, "node") == heir && attr(sp, "seat") == "fabric-primary" && attr(sp, "holder").contains(&heir_id)),
        "the heir announced the fabric-primary seat (the other mesh's primary id is {} the heir's: {})",
        if discriminating { "below" } else { "above" },
        other_primary.1
    );
}

/// CONTRACT: the primary of the mesh that does not hold the fabric seat dies; its mesh's other
/// admin takes the mesh-primary seat and announces it, and an admin of the OTHER mesh holds the
/// record (it reached the backbone).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_change_reaches_the_other_mesh_over_the_backbone() {
    let (mut estate, nodes) = fabric("a_mesh_primary_change_reaches_the_other_mesh_over_the_backbone").await;
    let fp = fabric_primary(&nodes).expect("one fabric primary");
    let mesh = if fp.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let old = nodes.iter().find(|n| n["mesh"] == mesh && n["is_primary"] == true).map(|n| s(&n["name"])).unwrap();
    let heir = if old.ends_with(".1") { format!("{mesh}.admin.2") } else { format!("{mesh}.admin.1") };
    let heir_id = s(&node(&nodes, &heir)["node_id"]);
    kill(&mut estate, &old).await;
    wait_for("the surviving admin is its mesh's primary", Duration::from_secs(90), || async {
        let ns = estate.nodes().await;
        ns.iter().any(|n| n["name"] == heir.as_str() && n["is_primary"] == true).then_some(())
    })
    .await;
    let watchers: Vec<String> = nodes.iter().filter(|n| n["kind"] == "node_admin" && !n["mesh"].as_str().unwrap_or("").eq(mesh) && n["name"] != old.as_str()).map(|n| s(&n["name"])).collect();
    let heard = wait_for("an admin of the other mesh heard the new mesh primary", Duration::from_secs(60), || {
        let spans = estate.spans();
        let found = named(&spans, "rdm.mesh.seat.update.via-announcement").into_iter().find(|sp| attr(sp, "seat") == "mesh-primary" && attr(sp, "holder").contains(&heir_id) && watchers.contains(&attr(sp, "node"))).cloned();
        async move { found }
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    assert!(named(&spans, "rdm.mesh.seat.update.via-announce").iter().any(|sp| attr(sp, "node") == heir && attr(sp, "seat") == "mesh-primary"), "the new mesh primary announced itself");
    assert_eq!(attr(&heard, "outcome"), "held", "{heard:#?}");
}

/// CONTRACT: every UDP path between the fabric primary's mesh and the other mesh is cut while
/// all processes run. The other mesh's primary says the fabric primary's exact birth is silent (a
/// Concern), the incumbent mesh's admins look at it, and the seat does not move on either side. The
/// cut is healed and the seat is still where it was. The Concern and the investigation carry
/// iroh's local view of the holder (A1), in its shape and read as nothing more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_off_fabric_primary_that_runs_is_looked_at_and_keeps_the_seat() {
    let (mut estate, nodes) = fabric("a_cut_off_fabric_primary_that_runs_is_looked_at_and_keeps_the_seat").await;
    let fp = fabric_primary(&nodes).expect("one fabric primary");
    let fp_node = node(&nodes, &fp).clone();
    let (mine, theirs): (Vec<String>, Vec<String>) = {
        let side = |incumbent: bool| nodes.iter().filter(|n| n["kind"] == "node_admin" && (n["mesh"] == fp_node["mesh"]) == incumbent).map(|n| s(&n["name"])).collect::<Vec<_>>();
        (side(true), side(false))
    };
    let other_admin = nodes.iter().find(|n| n["kind"] == "node_admin" && n["mesh"] != fp_node["mesh"]).unwrap().clone();
    let cut = match Partition::start(&udp_ports(&nodes, &mine), &udp_ports(&nodes, &theirs)) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot cut UDP between node-admins: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let fp_id = s(&fp_node["node_id"]);
    let concern = wait_for("the other mesh's primary says the fabric primary's birth is silent", Duration::from_secs(120), || {
        let spans = estate.spans();
        let found = named(&spans, "rdm.mesh.seat.update.via-concern").into_iter().find(|sp| attr(sp, "holder_node_id") == fp_id).cloned();
        async move { found }
    })
    .await;
    let looked = wait_for("an admin looked at the exact silent birth", Duration::from_secs(60), || {
        let spans = estate.spans();
        let found = named(&spans, "rdm.node_admin.seat.update.via-investigation").into_iter().find(|sp| attr(sp, "suspect_node_id") == fp_id).cloned();
        async move { found }
    })
    .await;
    // Both sides still name the same holder while the cut stands.
    let from_other = estate.nodes_at(&s(&other_admin["admin_api_base"])).await;
    let from_incumbent = estate.nodes_at(&s(&fp_node["admin_api_base"])).await;
    drop(cut);
    tokio::time::sleep(Duration::from_secs(8)).await;
    let healed = estate.nodes().await;
    estate.stop().await;
    for (side, ns) in [("the other mesh", &from_other), ("the incumbent mesh", &from_incumbent), ("after the heal", &healed)] {
        assert_eq!(fabric_primary(ns).as_deref(), Some(fp.as_str()), "{side} names the unchanged holder: {ns:#?}");
    }
    assert_eq!(attr(&concern, "seat"), "fabric-primary");
    assert_ne!(attr(&looked, "finding"), "exit-proven", "a running birth is not a loss: {looked:#?}");
    // A1: the Concern and the investigation carry iroh's local view of the holder.
    for sp in [&concern, &looked] {
        iroh_observation_shape(sp).unwrap_or_else(|why| panic!("{why}"));
    }
}
