//! Q209 (node-recovery.md "Detection and the verdict": "The mesh-primary runs one loop for its own members
//! on every gossip interval, never on a timer of its own. After a retryable recovery-submit failure, that
//! exact birth becomes eligible again after a backoff doubling from the gossip interval to the staleness
//! floor."; "Execution": "The mesh-primary reports the recovery up as `build.create { Change(Recover) }`",
//! and a mesh the fabric-primary cannot reach "is recovered once the cut heals (Q118)"; lead decision
//! D22, 2026-11-02: a mesh-primary behind a cut fails its recovery by name and recovers after the heal).
//!
//! CONTRACT: in a two-mesh estate (two node-admins and two rpc nodes each, the fabric-primary mesh1.node-admin.1)
//! every UDP path between mesh2's primary and mesh1's nodes is dropped (mesh2's other nodes still reach both
//! sides, so no mesh is unheard), and the provider kills mesh2.node.1's runtime. Mesh2's primary's submits of
//! that birth's recovery fail while the cut holds, each by name: its outcome names the report of the birth's
//! drift to the fabric-primary that did not go through; each failed submit records its backoff (the gossip
//! interval, doubling, at most the staleness floor) and the next submit of the birth starts no earlier than
//! that backoff after the failed one ended. The cut is dropped: the next submit starts within the staleness
//! floor and a tick of the last failed one, and completes: one new birth at mesh2.node.1 under a
//! new node id. What must NOT happen: a failed submit made again on every tick, a backoff past the
//! staleness floor, a recovery that completes while the cut holds, or the heal answered later than the
//! floor and a tick after the last failed submit.

use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use rdm_test::netfault::{udp_ports, Partition};
use serde_json::Value;
use rdm_core::PathName;

fn location(node: &Value) -> PathName { serde_json::from_value(node["name"].clone()).expect("separate path and name fields") }
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-recovery".into(),
        subfeature: "submit-backoff".into(),
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
    let mut v: Vec<Value> = named(spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| at(sp) >= since && s(&sp["attributes"]["node_id"]) == node_id && s(&sp["attributes"]["incarnation_id"]) == incarnation).cloned().collect();
    v.sort_by_key(at);
    v
}

fn completed(sp: &Value) -> bool {
    s(&sp["attributes"]["outcome"]).starts_with("complete")
}

// @feature: node-lifecycle
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "https://github.com/drlukeangel/rust-distributed-mesh/issues/295: a reached mesh-primary commanded back through node.mesh.join, then the killed node read reached, delays the recovery past its bound; i152 s2 (rdm#296) rewrites recovery and moves this cell"]
async fn a_mesh_primary_cut_from_the_fabric_primary_recovers_its_killed_node_within_its_backoff_of_the_heal() {
    let test = "a_mesh_primary_cut_from_the_fabric_primary_recovers_its_killed_node_within_its_backoff_of_the_heal";
    let shape = [("mesh1", 2, 2), ("mesh2", 2, 2)];
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"], &[]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let nodes = estate.node_values().await;
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary");
    assert_eq!(fp["mesh"], "mesh1", "the fabric-primary has explicit mesh1 metadata");
    let fp = location(fp);
    let primary_view = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).expect("mesh2's primary");
    let mesh2_primary = location(primary_view);
    let primary_id = s(&primary_view["node_id"]);
    let victim = PathName::new("mesh2.node", "1").unwrap();
    let victim_view = nodes.iter().find(|n| location(n) == victim).expect("fixture victim");
    let victim_id = s(&victim_view["node_id"]);
    let victim_incarnation = s(&victim_view["incarnation_id"]);
    assert!(!victim_incarnation.is_empty(), "the victim has an incarnation");
    let mesh1: Vec<PathName> = nodes.iter().filter(|n| n["mesh"] == "mesh1").map(location).collect();
    let floor = rdm_transport::topology::staleness_floor();
    let gossip = rdm_transport::topology::gossip_interval();

    let cut = match Partition::start(&udp_ports(&nodes, &[mesh2_primary.clone()]), &udp_ports(&nodes, &mesh1)) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot drop {mesh2_primary:?}'s UDP to mesh1: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let cut_at = now_ns();
    estate.kill_node(&victim).await;
    let killed_at = now_ns();
    // Held until five submits of the birth have failed, so the backoff has doubled to the floor (the gossip
    // interval, doubling: 1, 2, 4, then the floor).
    wait_for(&format!("five submits of {victim:?}'s recovery fail while {mesh2_primary:?} is cut from mesh1"), floor * 12 + Duration::from_secs(120), || async {
        let subs = submits_of(&estate.spans(), &victim_id, &victim_incarnation, killed_at);
        assert!(!subs.iter().any(completed), "no recovery of {victim:?} completes while the cut holds: {subs:#?}");
        (subs.len() >= 5).then_some(())
    })
    .await;
    drop(cut);
    let healed_at = now_ns();
    wait_for(&format!("{victim:?}'s recovery completes after the heal"), floor * 2 + Duration::from_secs(90), || async { submits_of(&estate.spans(), &victim_id, &victim_incarnation, killed_at).iter().any(completed).then_some(()) }).await;
    let recovered = wait_for(&format!("{victim:?} is ready under a new node id"), Duration::from_secs(60), || async {
        let v = estate.node_values_at(&fp).await;
        v.iter().find(|n| location(n) == victim && n["status"] == "ready" && s(&n["node_id"]) != victim_id).map(|n| s(&n["node_id"]))
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();

    let submits = submits_of(&spans, &victim_id, &victim_incarnation, killed_at);
    let ms = |t: u64| t.saturating_sub(cut_at) / 1_000_000;
    let line = |sp: &Value| format!("{} +{} ms..+{} ms {} backoff_ms={}", s(&sp["attributes"]["submitter"]), ms(at(sp)), ms(end(sp)), s(&sp["attributes"]["outcome"]), s(&sp["attributes"]["backoff_ms"]));
    eprintln!("Q209: cut at 0, killed +{} ms, healed +{} ms; the submits of {victim:?}:\n  {}", ms(killed_at), ms(healed_at), submits.iter().map(line).collect::<Vec<_>>().join("\n  "));
    let done: Vec<&Value> = submits.iter().filter(|sp| completed(sp)).collect();
    assert_eq!(done.len(), 1, "one submit of {victim:?} completed: {done:#?}");
    assert!(end(done[0]) >= healed_at, "no recovery of {victim:?} completed while the cut held: {}", line(done[0]));
    assert!(submits.iter().all(|sp| s(&sp["attributes"]["submitter"]) == primary_id), "every submit is {mesh2_primary:?}'s, mesh2's primary throughout: {:#?}", submits.iter().map(line).collect::<Vec<_>>());
    // Each failed submit is paced by its backoff, capped at the floor.
    let failed: Vec<&Value> = submits.iter().take_while(|sp| !completed(sp)).collect();
    assert!(failed.len() >= 5, "five submits failed under the cut: {failed:#?}");
    // Each fails by name: the report of the birth's drift to the fabric-primary did not go through.
    for sp in &failed {
        let outcome = s(&sp["attributes"]["outcome"]);
        assert!(outcome.contains(&victim_id) && outcome.contains("drift to"), "a failed submit under the cut names the report of {victim:?}'s drift that did not go through: {}", line(sp));
    }
    let backoff = |sp: &Value| -> u64 { s(&sp["attributes"]["backoff_ms"]).parse().unwrap_or_else(|_| panic!("a failed submit of {victim:?} records its backoff: {}", line(sp))) };
    for sp in &failed {
        assert!(Duration::from_millis(backoff(sp)) <= floor, "the backoff is at most the staleness floor ({floor:?}): {}", line(sp));
    }
    for pair in submits.windows(2) {
        let gap_ms = at(&pair[1]).saturating_sub(end(&pair[0])) / 1_000_000;
        assert!(gap_ms + 2 >= backoff(&pair[0]), "{victim:?} was submitted again {gap_ms} ms after a failed submit: {} then {}", line(&pair[0]), line(&pair[1]));
    }
    // After the heal: the completed submit was in flight at the heal, or starts within the staleness floor
    // (the capped backoff) and a tick of the last failed submit's end.
    let last = failed.last().expect("a failed submit");
    if at(done[0]) >= healed_at {
        let bound = end(last) + (floor + gossip + Duration::from_millis(250)).as_nanos() as u64;
        assert!(at(done[0]) <= bound, "the recovery after the heal starts within the staleness floor ({floor:?}) and a tick of the last failed submit: {} after {}", line(done[0]), line(last));
    }
    eprintln!("Q209: the completed submit started {} ms after the heal ({} ms after the last failed submit ended), ran {} ms; {victim:?} is now {recovered}", at(done[0]) as i64 / 1_000_000 - healed_at as i64 / 1_000_000, at(done[0]).saturating_sub(end(last)) / 1_000_000, end(done[0]).saturating_sub(at(done[0])) / 1_000_000);
    estate.record_trace_url(&s(&done[0]["trace_id"]));
}
