//! i143.e4.s14 process E2E: the canonical cohort election (PRD §11;
//! rafka-v2 `docs/architecture/node-lifecycle-elections.md`).
//!
//! Every seat is computed: a cohort's primary is its ready member with the
//! lowest NodeId, a mesh's primary is its node-admin cohort's, and the fabric
//! primary is the lowest-NodeId mesh primary. From public surfaces only (each
//! admin's `GET /api/nodes`), the test computes the expected winner from the
//! advertised NodeIds and statuses of the same view and compares; it never
//! asserts that an incumbent stays. Through the matrix:
//! - day 0: the one bootstrap admin holds the node-admin cohort, the mesh and
//!   the fabric, by the same function (its own evidence says so);
//! - settled, restart of a non-primary, grow, shrink: the seats are the
//!   computed ones on every sample (no flap away from the computed winner);
//! - restart of the primary: it keeps its NodeId, and once ready it holds the
//!   seat again;
//! - kill the primary (SIGKILL): the next-lowest ready NodeId succeeds; the
//!   recreated path is a new NodeId and wins or not by it;
//! - legal removal of the primary: the next-lowest succeeds;
//! - partition: a transient split is permitted; heal: every admin's view
//!   advertises the same computed seats.
//!
//! Evidence: each successor is announced by a
//! `rdm.mesh.election.resolve.via-recompute` span (`election_level =
//! node_type`, `winner_node_id`, `previous_node_id`, `election_key =
//! node_id_crockford`); the mesh primary by `via-mesh-primary`.
//!
//! The partition drops UDP between the two sides' endpoint ports on loopback
//! (`iptables`, as root or through `sudo -n`). Where that is unavailable the
//! partition case is a named skip; `RAFKA_REQUIRE_NETFAULT=1` (CI) makes it a
//! failure.

use rafka_test_scenario::elections::{advertised_primaries, expected_fabric_primary, expected_primaries, seats_as_expected, Cohort};
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn owner(test: &str, rung: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "cohort-election".into(),
        rung: rung.into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn cohort(mesh: &str, kind: &str) -> Cohort {
    (mesh.into(), kind.into())
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn id_of(nodes: &[Value], name: &str) -> String {
    nodes.iter().find(|n| n["name"] == name).map(|n| s(&n["node_id"])).unwrap_or_else(|| panic!("no node {name}"))
}

/// The one primary of `c` in `nodes`, if exactly one.
fn primary_of(nodes: &[Value], c: &Cohort) -> Option<String> {
    match advertised_primaries(nodes).get(c).map(Vec::as_slice) {
        Some([one]) => Some(one.clone()),
        _ => None,
    }
}

async fn build(estate: &Estate, meshes: &[(&str, u32, u32)]) {
    let desired = json!({
        "fabric": "fabric1",
        "meshes": meshes.iter().map(|(m, a, r)| json!({"name": m, "node_admin": a, "rpc_node": r})).collect::<Vec<_>>(),
    });
    let (status, accepted) = estate.post("/api/build", &desired).await;
    assert_eq!(status, 202, "{accepted}");
    estate.await_build(accepted["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
}

async fn accepted_build(estate: &Estate, (status, v): (u16, Value)) {
    assert_eq!(status, 202, "{v}");
    estate.await_build(v["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
}

/// The view without the rows of births proven dead: drift recovery restores a killed birth's
/// count at a free path, so the dead birth's own row is not replaced at its path.
fn live(nodes: &[Value]) -> Vec<Value> {
    nodes.iter().filter(|n| !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))).cloned().collect()
}

/// Every live node is ready and every seat is the computed one.
fn settled(nodes: &[Value]) -> bool {
    let nodes = live(nodes);
    !nodes.is_empty() && nodes.iter().all(|n| n["status"] == "ready-for-traffic") && seats_as_expected(&nodes).is_ok()
}

/// Wait until `base`'s view is settled; return its live rows. A timeout names the last view.
async fn settle(estate: &Estate, base: &str, label: &str) -> Vec<Value> {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let nodes = estate.nodes_at(base).await;
        if settled(&nodes) {
            return live(&nodes);
        }
        if Instant::now() > until {
            let seats = seats_as_expected(&live(&nodes)).err().unwrap_or_default();
            panic!("{label}: the view did not settle on the computed seats in 30s ({seats}): {nodes:#?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// For `hold`, every sample of `base`'s view advertises the computed seats
/// of that same sample (never two, never none, never another).
async fn steady(estate: &Estate, base: &str, label: &str, hold: Duration) {
    let until = Instant::now() + hold;
    while Instant::now() < until {
        let nodes = live(&estate.nodes_at(base).await);
        if nodes.iter().all(|n| n["status"] == "ready-for-traffic") {
            if let Err(e) = seats_as_expected(&nodes) {
                panic!("{label}: {e}: {nodes:#?}");
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The successor to `gone_id` in `c`: the lowest NodeId among the cohort's other ready members
/// in `before` (the view just before the kill), announced by the mesh as the winner after
/// `gone_id`. Decided from the pre-kill view and the announcement span, never from a polled
/// view: drift recovery rebirths the path within seconds, and a view polled after that already
/// shows the reborn NodeId (a different birth) in the seat.
async fn successor(estate: &Estate, base: &str, before: &[Value], c: &Cohort, gone_id: &str) -> (String, String) {
    let (name, id) = before
        .iter()
        .filter(|n| n["mesh"] == c.0.as_str() && n["kind"] == c.1.as_str() && n["status"] == "ready-for-traffic" && n["node_id"] != gone_id)
        .map(|n| (s(&n["name"]), s(&n["node_id"])))
        .min_by(|a, b| a.1.cmp(&b.1))
        .unwrap_or_else(|| panic!("no other ready member of {c:?} to succeed {gone_id}: {before:#?}"));
    // Silence is local receipt age, and gossip may still hand on the gone birth's last cached
    // digests for one cache window after it died: the successor is announced within the staleness
    // floor after that.
    let silence_bound = rafka_mesh_transport::membership::staleness_floor() * 2 + rafka_mesh_transport::membership::backbone_gossip_interval() * 2 + Duration::from_secs(10);
    wait_for(&format!("{name} ({id}) announced as the successor to {gone_id}"), silence_bound, || async {
        announced(&estate.spans(), c, &id, gone_id).then_some(())
    })
    .await;
    // Another admin may announce first: `base`'s own view holds the killed birth dead before
    // anything is read from it.
    wait_for(&format!("{base} no longer hears {gone_id}"), rafka_mesh_transport::membership::staleness_floor() + Duration::from_secs(30), || async {
        (!estate.nodes_at(base).await.iter().any(|n| n["node_id"] == gone_id && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect")))).then_some(())
    })
    .await;
    (name, id)
}

/// The node-type election span that announced `winner` for `c` after `previous` (NodeIds).
fn announced(spans: &[Value], c: &Cohort, winner: &str, previous: &str) -> bool {
    named(spans, "rdm.mesh.election.resolve.via-recompute").iter().any(|s| {
        let a = &s["attributes"];
        a["election_level"] == "node_type"
            && a["election_key"] == "node_id_crockford"
            && a["mesh"] == c.0.as_str()
            && a["kind"] == c.1.as_str()
            && a["winner_node_id"] == winner
            && a["previous_node_id"] == previous
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_mn_cohort_elects_the_lowest_ready_node_id_through_the_matrix() {
    let mut estate = Estate::bootstrap(owner("every_mn_cohort_elects_the_lowest_ready_node_id", "MN"), "fabric1", "mesh1").await;
    let base1 = estate.admin.clone();
    let (rpc, admin) = (cohort("mesh1", "rpc_node"), cohort("mesh1", "node_admin"));
    let hold = Duration::from_secs(4);

    // day 0: one admin, every seat its own.
    let day0 = settle(&estate, &base1, "day 0").await;
    let boot = day0.iter().find(|n| n["kind"] == "node_admin").map(|n| (s(&n["name"]), s(&n["node_id"]))).unwrap();
    assert_eq!(expected_fabric_primary(&day0).as_deref(), Some(boot.0.as_str()));

    // settled MN -> the computed seats
    build(&estate, &[("mesh1", 2, 3)]).await;
    estate.settled_shape(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    let nodes = settle(&estate, &base1, "MN").await;
    let rpc_p = primary_of(&nodes, &rpc).unwrap();
    assert_eq!(advertised_fabric(&nodes), expected_fabric_primary(&nodes), "the fabric primary is the lowest-NodeId mesh primary");

    // restart a non-primary -> it keeps its NodeId; the seats stay computed
    let other_rpc: Vec<String> = nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["is_primary"] == false).map(|n| s(&n["name"])).collect();
    let before = id_of(&nodes, &other_rpc[1]);
    accepted_build(&estate, estate.post(&format!("/api/nodes/{}/restart", other_rpc[1]), &json!({})).await).await;
    let nodes = settle(&estate, &base1, "restart non-primary").await;
    assert_eq!(id_of(&nodes, &other_rpc[1]), before, "restart keeps the NodeId");
    steady(&estate, &base1, "restart non-primary", hold).await;

    // restart the primary -> it keeps its NodeId and, once ready, holds the seat again
    let rpc_p_id = id_of(&nodes, &rpc_p);
    accepted_build(&estate, estate.post(&format!("/api/nodes/{rpc_p}/restart"), &json!({})).await).await;
    let nodes = settle(&estate, &base1, "restart primary").await;
    assert_eq!(id_of(&nodes, &rpc_p), rpc_p_id, "restart keeps the NodeId");
    assert_eq!(primary_of(&nodes, &rpc).as_deref(), Some(rpc_p.as_str()), "the restarted lowest NodeId retakes its seat: {nodes:#?}");

    // grow, then shrink back -> every sample the computed seats (a newcomer may win)
    build(&estate, &[("mesh1", 3, 5)]).await;
    settle(&estate, &base1, "grow").await;
    steady(&estate, &base1, "grow", hold).await;
    build(&estate, &[("mesh1", 2, 3)]).await;
    settle(&estate, &base1, "shrink").await;
    steady(&estate, &base1, "shrink", hold).await;

    // kill the current primary -> the next-lowest succeeds; drift recovery restores the count with
    // a new NodeId at a free path
    let nodes = estate.nodes().await;
    let killed = primary_of(&nodes, &rpc).unwrap();
    let killed_id = id_of(&nodes, &killed);
    estate.kill_node(&killed).await;
    let (_succ, succ_id) = successor(&estate, &base1, &nodes, &rpc, &killed_id).await;
    build(&estate, &[("mesh1", 2, 3)]).await;
    // The rebirth is drift recovery's, once the killed runtime is proven exited: wait for the
    // count, then for the seats.
    wait_for("drift recovery restores three live rpc nodes", rafka_mesh_transport::membership::staleness_floor() * 2 + Duration::from_secs(30), || async {
        let live_rpc = live(&estate.nodes_at(&base1).await).iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node" && n["status"] == "ready-for-traffic").count();
        (live_rpc == 3).then_some(())
    })
    .await;
    let nodes = settle(&estate, &base1, "the cohort is back at its desired count").await;
    estate.artifact("settled-after-kill.json", &json!({"killed": killed, "killed_id": killed_id, "live": nodes, "all": estate.nodes().await}));
    assert!(!nodes.iter().any(|n| n["node_id"] == killed_id.as_str()), "the killed birth {killed_id} is not live again: {nodes:#?}");
    assert_eq!(nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node").count(), 3, "three live rpc nodes: {nodes:#?}");
    steady(&estate, &base1, "the cohort is back at its desired count", hold).await;

    // legal removal of the primary -> the next-lowest succeeds
    let removed = primary_of(&nodes, &rpc).unwrap();
    let removed_id = id_of(&nodes, &removed);
    accepted_build(&estate, estate.delete(&format!("/api/nodes/{removed}")).await).await;
    let nodes = wait_for("the view drops the removed primary", Duration::from_secs(30), || async {
        let nodes = estate.nodes().await;
        (!nodes.iter().any(|n| n["name"] == removed.as_str() && n["status"] == "ready-for-traffic") && primary_of(&nodes, &rpc).is_some()).then_some(nodes)
    })
    .await;
    let succ2 = primary_of(&nodes, &rpc).unwrap();
    assert_eq!(expected_primaries(&nodes).get(&rpc), Some(&succ2), "the next-lowest succeeds: {nodes:#?}");
    let succ2_id = id_of(&nodes, &succ2);
    build(&estate, &[("mesh1", 2, 3)]).await;
    settle(&estate, &base1, "after removal").await;
    steady(&estate, &base1, "after removal", hold).await;

    // partition -> transient split permitted; heal -> the computed seats in every view
    let nodes = estate.nodes().await;
    let admin_p = primary_of(&nodes, &admin).unwrap();
    let admin2 = nodes.iter().find(|n| n["kind"] == "node_admin" && n["name"] != admin_p.as_str()).unwrap();
    let (admin2_name, base2) = (s(&admin2["name"]), s(&admin2["admin_api_base"]));
    let base_p = s(&nodes.iter().find(|n| n["name"] == admin_p.as_str()).unwrap()["admin_api_base"]);
    let rpc_now = primary_of(&nodes, &rpc).unwrap();
    let lone_rpc = nodes.iter().find(|n| n["kind"] == "rpc_node" && n["name"] != rpc_now.as_str()).map(|n| s(&n["name"])).unwrap();
    let side_a = vec![admin_p.clone(), lone_rpc.clone()];
    let side_b: Vec<String> = nodes.iter().map(|n| s(&n["name"])).filter(|n| !side_a.contains(n)).collect();
    match Partition::start(&udp_ports(&nodes, &side_a), &udp_ports(&nodes, &side_b)) {
        Err(why) if std::env::var("RAFKA_REQUIRE_NETFAULT").as_deref() == Ok("1") => {
            panic!("RAFKA_REQUIRE_NETFAULT=1 but this host cannot partition: {why}")
        }
        Err(why) => eprintln!("SKIP partition case: {why}"),
        Ok(partition) => {
            // Each side elects from what it can hear: the split is visible.
            wait_for("side A elects its own rpc primary", Duration::from_secs(30), || async {
                (primary_of(&estate.nodes_at(&base_p).await, &rpc).as_deref() == Some(lone_rpc.as_str())).then_some(())
            })
            .await;
            wait_for("side B elects its own admin primary", Duration::from_secs(30), || async {
                (primary_of(&estate.nodes_at(&base2).await, &admin).as_deref() == Some(admin2_name.as_str())).then_some(())
            })
            .await;
            for base in [&base_p, &base2] {
                for (c, p) in advertised_primaries(&estate.nodes_at(base).await) {
                    assert!(p.len() <= 1, "{base}: a view never names two primaries of {c:?}: {p:?}");
                }
            }
            drop(partition);
            let mut healed = Vec::new();
            for base in [&base_p, &base2] {
                healed.push(settle(&estate, base, &format!("{base} heals")).await);
            }
            assert_eq!(advertised_primaries(&healed[0]), advertised_primaries(&healed[1]), "healed views agree");
            steady(&estate, &base2, "healed", hold).await;
        }
    }

    estate.artifact("nodes.json", &json!(estate.nodes().await));
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
    let spans = estate.spans();
    // Day 0: the bootstrap admin resolved itself at all three levels, observing itself.
    let own = |name: &str, level: &str| {
        named(&spans, name).iter().any(|sp| {
            let a = &sp["attributes"];
            a["election_level"] == level && a["observer"] == boot.0.as_str() && a["winner_node_id"] == boot.1.as_str() && a["previous_node_id"] == ""
        })
    };
    assert!(own("rdm.mesh.election.resolve.via-recompute", "node_type"), "day 0: node-admin cohort");
    assert!(own("rdm.mesh.election.resolve.via-mesh-primary", "mesh_primary"), "day 0: mesh primary");
    assert!(own("rdm.mesh.election.resolve.via-fabric-recompute", "fabric_primary"), "day 0: fabric primary");
    assert!(announced(&spans, &rpc, &succ_id, &killed_id), "the successor to the killed primary was announced");
    assert!(announced(&spans, &rpc, &succ2_id, &removed_id), "the successor to the removed primary was announced");
    // The restarted primary's NodeId left the seat while it was not ready and took it back.
    let back = named(&spans, "rdm.mesh.election.resolve.via-recompute").iter().any(|sp| {
        let a = &sp["attributes"];
        a["mesh"] == "mesh1" && a["kind"] == "rpc_node" && a["winner_node_id"] == rpc_p_id.as_str() && a["previous_node_id"].as_str().is_some_and(|p| !p.is_empty() && p != rpc_p_id)
    });
    assert!(back, "the restarted primary's return to its seat was announced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_meshs_admin_cohort_elects_its_lowest_node_id() {
    let mut estate = Estate::bootstrap(owner("a_second_meshs_admin_cohort_elects", "MM"), "fabric1", "mesh1").await;
    let base1 = estate.admin.clone();
    let admin2 = cohort("mesh2", "node_admin");
    let hold = Duration::from_secs(4);

    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    // A peer mesh's members reach this admin through its primary's backbone
    // publication: the view settles within a publication round or two.
    let nodes = wait_for("the MM view settles with four cohort primaries", Duration::from_secs(15), || async {
        let nodes = estate.nodes().await;
        (settled(&nodes) && advertised_primaries(&nodes).len() == 4).then_some(nodes)
    })
    .await;
    let p = primary_of(&nodes, &admin2).unwrap();
    let p_id = id_of(&nodes, &p);
    estate.artifact("settled-before-kill.json", &json!({"killed": p, "killed_id": p_id, "nodes": nodes}));

    // kill the mesh2 admin primary -> the next-lowest succeeds; the fabric seat is recomputed
    estate.kill_node(&p).await;
    let (succ, succ_id) = successor(&estate, &base1, &nodes, &admin2, &p_id).await;
    // The successor was announced; the repair of the same Build rebirths the killed path within
    // seconds, and that birth may draw a lower NodeId and take the seat in turn. The view is read
    // once the killed birth is dead in it; whoever holds the seat then is the computed one.
    let nodes = wait_for("the view holds the killed birth dead", Duration::from_secs(30), || async {
        let nodes = estate.nodes().await;
        (!nodes.iter().any(|n| n["node_id"] == p_id.as_str() && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))) && primary_of(&nodes, &admin2).is_some()).then_some(nodes)
    })
    .await;
    estate.artifact("successor-after-kill.json", &json!({"successor": succ, "successor_id": succ_id, "nodes": nodes}));
    assert_eq!(advertised_fabric(&nodes), expected_fabric_primary(&nodes));
    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    settle(&estate, &base1, "mesh2's killed admin is back").await;
    steady(&estate, &base1, "mesh2's killed admin is back", hold).await;

    // legal removal of the mesh2 admin primary -> the next-lowest succeeds
    let nodes = estate.nodes().await;
    let removed = primary_of(&nodes, &admin2).unwrap();
    let removed_id = id_of(&nodes, &removed);
    accepted_build(&estate, estate.delete(&format!("/api/nodes/{removed}")).await).await;
    let nodes = wait_for("the view drops the removed admin primary", Duration::from_secs(30), || async {
        let nodes = estate.nodes().await;
        (!nodes.iter().any(|n| n["name"] == removed.as_str() && n["status"] == "ready-for-traffic") && primary_of(&nodes, &admin2).is_some()).then_some(nodes)
    })
    .await;
    let succ2 = primary_of(&nodes, &admin2).unwrap();
    assert_eq!(expected_primaries(&nodes).get(&admin2), Some(&succ2));
    let succ2_id = id_of(&nodes, &succ2);
    assert_eq!(advertised_fabric(&nodes), expected_fabric_primary(&nodes));
    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    settle(&estate, &base1, "mesh2 after removal").await;
    steady(&estate, &base1, "mesh2 after removal", hold).await;

    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
    let spans = estate.spans();
    assert!(announced(&spans, &admin2, &succ_id, &p_id), "the successor to the killed admin primary was announced");
    assert!(announced(&spans, &admin2, &succ2_id, &removed_id), "the successor to the removed admin primary was announced");
    // The mesh primary is the node-admin cohort's winner, announced as such.
    assert!(
        named(&spans, "rdm.mesh.election.resolve.via-mesh-primary").iter().any(|sp| {
            let a = &sp["attributes"];
            a["mesh"] == "mesh2" && a["source_kind"] == "node_admin" && a["winner_node_id"] == succ2_id.as_str()
        }),
        "mesh2's primary follows its node-admin cohort"
    );
}

fn advertised_fabric(nodes: &[Value]) -> Option<String> {
    let fp = rafka_test_scenario::elections::advertised_fabric_primaries(nodes);
    (fp.len() == 1).then(|| fp[0].clone())
}
