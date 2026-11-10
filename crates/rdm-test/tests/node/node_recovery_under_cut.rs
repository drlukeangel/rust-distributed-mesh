//! node-recovery.md "Execution": "The mesh-primary runs the recovery itself and records each step on
//! the node record ... It is never a Build: no build.create, no Build record, nothing claimed" (Q342),
//! and "Detection and the result": the mesh-primary runs one loop for its own members on every gossip
//! interval.
//!
//! CONTRACT: in a two-mesh estate (two node-admins and two nodes each, the fabric-primary mesh1.node-admin.1)
//! every UDP path between mesh2's primary and mesh1's nodes is dropped (mesh2's other nodes still reach both
//! sides, so no mesh is unheard), and the provider kills mesh2.node.1's runtime. Mesh2's primary recovers
//! that birth while the cut holds, its own workflow: one recovery submit of the birth by mesh2's primary
//! completes before the cut is dropped, and mesh2.node.1 is ready under a new node id. What must NOT
//! happen: the recovery waiting on the fabric-primary it cannot reach, a recovery by any other admin,
//! or a second recovery of the birth. Nor is mesh2's primary investigated or commanded back through
//! node.mesh.join: a call to it spent across the cut marks it at the fabric-primary, which investigates
//! a peer mesh's primary only when that mesh's aggregate stays unheard past the staleness floor
//! (node-recovery.md step 2, Q335; rdm#295: a reached, Ready primary was re-marked, investigated and
//! commanded back after each spent call).

use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use rdm_test::netfault::{udp_ports, Partition};
use serde_json::Value;
use rdm_core::PathName;

fn location(node: &Value) -> PathName { rdm_test::estate::location(&node) }
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-recovery".into(),
        subfeature: "recovery-under-cut".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}
fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}
fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(u64::MAX)
}
fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// Every submit of `node_id`'s recovery a mesh2 admin made since `since`, in start order.
fn submits_of(spans: &[Value], node_id: &str, incarnation: &str, since: u64) -> Vec<Value> {
    let mut v: Vec<Value> = named(spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| at(sp) >= since && s(&sp["attributes"]["node_id"]) == node_id && s(&sp["attributes"]["incarnation_id"]) == incarnation).cloned().collect();
    v.sort_by_key(at);
    v
}

fn completed(sp: &Value) -> bool {
    s(&sp["attributes"]["outcome"]).starts_with("complete")
}

// @feature: node-lifecycle
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_cut_from_the_fabric_primary_recovers_its_killed_node_while_the_cut_holds() {
    let test = "a_mesh_primary_cut_from_the_fabric_primary_recovers_its_killed_node_while_the_cut_holds";
    let shape = [("mesh1", 2, 2), ("mesh2", 2, 2)];
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"], &[]).await.expect("the faulted-admin binding set is accepted");
    estate.resize_shape("fabric1", &shape).await;
    estate.settles_to(&shape, Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let nodes = estate.node_values().await;
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary");
    assert_eq!(rdm_test::estate::mesh_of(&fp), "mesh1", "the fabric-primary has explicit mesh1 metadata");
    let primary_view = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).expect("mesh2's primary");
    let mesh2_primary = location(primary_view);
    let primary_id = s(&primary_view["node_id"]);
    let victim = PathName::new("mesh2.node", "1").unwrap();
    let victim_view = nodes.iter().find(|n| location(n) == victim).expect("fixture victim");
    let victim_id = s(&victim_view["node_id"]);
    let victim_incarnation = s(&victim_view["incarnation"]);
    assert!(!victim_incarnation.is_empty(), "the victim has an incarnation");
    let mesh1: Vec<PathName> = nodes.iter().filter(|n| rdm_test::estate::mesh_of(&n) == "mesh1").map(location).collect();
    let floor = rdm_core::cadence::staleness_floor();

    let cut = match Partition::start(&udp_ports(&nodes, &[mesh2_primary.clone()]), &udp_ports(&nodes, &mesh1)) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot drop {mesh2_primary:?}'s UDP to mesh1: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    estate.kill_node(&victim).await;
    let killed_at = now_ns();
    wait_for(&format!("{victim:?}'s recovery by {mesh2_primary:?} completes while it is cut from mesh1"), floor * 4 + Duration::from_secs(120), || async {
        submits_of(&estate.spans(), &victim_id, &victim_incarnation, killed_at).iter().any(completed).then_some(())
    })
    .await;
    let done_at = now_ns();
    drop(cut);
    // The fabric-primary's view, healed, holds the recovered birth.
    let fp = location(fp);
    let recovered = wait_for(&format!("{victim:?} is ready under a new node id"), Duration::from_secs(60), || async {
        let v = estate.node_values_at(&fp).await;
        v.iter().find(|n| location(n) == victim && n["state"] == "ready" && s(&n["node_id"]) != victim_id).map(|n| s(&n["node_id"]))
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();

    let submits = submits_of(&spans, &victim_id, &victim_incarnation, killed_at);
    let done: Vec<&Value> = submits.iter().filter(|sp| completed(sp)).collect();
    assert_eq!(done.len(), 1, "one recovery of {victim:?} completed: {submits:#?}");
    assert!(end(done[0]) <= done_at, "the recovery completed while the cut held: {}", done[0]);
    assert!(submits.iter().all(|sp| s(&sp["attributes"]["submitter"]) == primary_id), "every recovery of {victim:?} is {mesh2_primary:?}'s: {submits:#?}");
    // mesh2's aggregate is heard throughout (its other nodes reach both sides): its primary is never
    // investigated and never commanded back, whatever spent calls marked it.
    let looks: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-investigation").into_iter().filter(|sp| s(&sp["attributes"]["suspect_node_id"]) == primary_id).collect();
    assert!(looks.is_empty(), "{mesh2_primary:?}, its mesh heard, is never investigated: {looks:#?}");
    let joins: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-mesh-join-commanded").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == primary_id).collect();
    assert!(joins.is_empty(), "{mesh2_primary:?}, Ready on its mesh, is never commanded back through node.mesh.join: {joins:#?}");
    eprintln!("the recovery of {victim:?} by {mesh2_primary:?} completed {} ms after the kill, the cut holding; {victim:?} is now {recovered}", end(done[0]).saturating_sub(killed_at) / 1_000_000);
    estate.record_trace_url(&s(&done[0]["trace_id"]));
}
