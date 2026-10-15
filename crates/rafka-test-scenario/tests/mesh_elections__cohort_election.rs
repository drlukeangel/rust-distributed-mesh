//! i143.e4.s14 process E2E: the canonical cohort election (PRD §11;
//! `docs/architecture/fabric-node-lifecycle-elections.md`).
//!
//! Every seat, each cohort's (rpc, node-admin: the mesh primary) and the
//! fabric seat, follows elections §5: "a living holder keeps its seat; a
//! lower-NodeId candidate that later becomes Ready never displaces it"; "a
//! holder that yields or whose exact birth is proven gone hands the seat to
//! the lowest ReadyForTraffic NodeId of its own mesh"; "a restarted node is a
//! candidate for a later vacancy and never displaces the holder". A holder a
//! Build stops, restarts or deletes yields first by node.primary.update to the
//! cohort's lowest ReadyForTraffic NodeId with the holder excluded (proposal
//! node-primary-update-fabric-mesh-and-node-type.md Part C1 steps 1-2). From
//! public surfaces only (each admin's `node.get`), every sample advertises
//! exactly one holder per cohort and a living holder keeps its seat. Through
//! the matrix:
//! - day 0: the one bootstrap admin holds the node-admin cohort, the mesh and
//!   the fabric, by the same function (its own evidence says so);
//! - settled, restart of a non-primary, grow, shrink: one holder per cohort on
//!   every sample, and a living holder keeps its seat (a newcomer never wins it);
//! - restart of the primary: node.primary.update moves its seat to the
//!   next-lowest ready NodeId first; the restarted node keeps its NodeId and
//!   never takes the seat back;
//! - kill the primary (SIGKILL): the next-lowest ready NodeId succeeds; the
//!   recreated path is a new NodeId and never displaces it;
//! - legal removal of the primary: the next-lowest succeeds;
//! - UDP cut between two sides of the mesh: neither side elects a primary of its own for a holder
//!   it cannot hear (a silent holder keeps its seat); heal: every admin's view advertises the same
//!   seats.
//!
//! Evidence: each successor is announced by a
//! `rdm.mesh.election.resolve.via-recompute` span (`election_level =
//! node_type`, `winner_node_id`, `previous_node_id`, `election_key =
//! node_id_crockford`); the mesh primary by `via-mesh-primary`.
//!
//! The partition drops UDP between the two sides' endpoint ports on loopback
//! (`nft`, as root or through `sudo -n`). Where that is unavailable the
//! partition case is a named skip; `RDM_REQUIRE_NETFAULT=1` (CI) makes it a
//! failure.

use rdm_client::{CallEnd, LaunchKind as NodeKind, NodeStatus, PathName, WorkflowStream};
use rafka_test_scenario::elections::{advertised_primaries, expected_primaries, seats_as_expected, seats_kept, Cohort};
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
    estate.reconcile_shape("fabric1", meshes).await;
}

/// The workflow `call` was accepted by the fabric-primary: await its reply stream to `Complete`.
async fn complete(what: &str, call: Result<WorkflowStream, CallEnd>) {
    let mut stream = call.unwrap_or_else(|end| panic!("the fabric-primary refused {what}: {end}"));
    Estate::complete(&mut stream).await;
}

fn path(name: &str) -> PathName {
    name.parse().unwrap_or_else(|e| panic!("{name} is not a path.name: {e:?}"))
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

/// Wait until the view of the node-admin `admin` is settled; return its live rows. A timeout names the last view.
async fn settle(estate: &Estate, admin: &PathName, label: &str) -> Vec<Value> {
    estate
        .until_view_at(admin, &format!("{label}: the view of {admin} settles on the computed seats"), Duration::from_secs(30), |views| {
            let nodes = Estate::values(views);
            settled(&nodes).then(|| live(&nodes))
        })
        .await
}

/// For `hold`, every sample of the view of the node-admin `admin` advertises one holder per cohort
/// (never two, never none), and every holder the first sample advertised that still lives holds its
/// seat on every later sample (elections §5: a newcomer, a lower NodeId or a restarted node never
/// displaces a living holder).
async fn steady(estate: &Estate, admin: &PathName, label: &str, hold: Duration) {
    let until = Instant::now() + hold;
    let mut first: Option<Vec<Value>> = None;
    while Instant::now() < until {
        let nodes = live(&estate.node_values_at(&admin.to_string()).await);
        if nodes.iter().all(|n| n["status"] == "ready-for-traffic") {
            if let Err(e) = seats_as_expected(&nodes) {
                panic!("{label}: {e}: {nodes:#?}");
            }
            match &first {
                None => first = Some(nodes),
                Some(before) => {
                    if let Err(e) = seats_kept(before, &nodes) {
                        panic!("{label}: {e}: before {before:#?} after {nodes:#?}");
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The lowest ready NodeId of `c` in `nodes` other than `holder` (a NodeId): the successor the
/// cohort election names with the holder excluded (proposal Part C1 step 2).
fn next_lowest(nodes: &[Value], c: &Cohort, holder: &str) -> (String, String) {
    nodes
        .iter()
        .filter(|n| n["mesh"] == c.0.as_str() && n["kind"] == c.1.as_str() && n["status"] == "ready-for-traffic" && n["node_id"] != holder)
        .map(|n| (s(&n["name"]), s(&n["node_id"])))
        .min_by(|a, b| a.1.cmp(&b.1))
        .unwrap_or_else(|| panic!("no other ready member of {c:?} than {holder}: {nodes:#?}"))
}

/// The "primary updated" announcement of the update `operation_suffix` (a Build operation's key, the
/// end of the update's operation identity) that committed `c`'s seat to the NodeId `successor`
/// (proposal Part C1 step 7).
fn primary_updated(spans: &[Value], c: &Cohort, operation_suffix: &str, successor: &str) -> bool {
    let seat = format!("{}-primary={}:{successor}@", c.1, c.0);
    named(spans, "rdm.mesh.seat.update.via-announce-primary-updated").iter().any(|sp| {
        let a = &sp["attributes"];
        a["operation"].as_str().is_some_and(|o| o.ends_with(operation_suffix)) && a["seats"].as_str().is_some_and(|v| v.contains(&seat))
    })
}

/// The successor to `gone_id` in `c`: the lowest NodeId among the cohort's other ready members
/// in `before` (the view just before the kill), announced by the mesh as the winner after
/// `gone_id`. Decided from the pre-kill view and the announcement span, never from a polled
/// view: drift recovery rebirths the path within seconds, and a view polled after that already
/// shows the reborn NodeId (a different birth) in the seat.
async fn successor(estate: &Estate, admin: &PathName, before: &[Value], c: &Cohort, gone_id: &str) -> (String, String) {
    let (name, id) = before
        .iter()
        .filter(|n| n["mesh"] == c.0.as_str() && n["kind"] == c.1.as_str() && n["status"] == "ready-for-traffic" && n["node_id"] != gone_id)
        .map(|n| (s(&n["name"]), s(&n["node_id"])))
        .min_by(|a, b| a.1.cmp(&b.1))
        .unwrap_or_else(|| panic!("no other ready member of {c:?} to succeed {gone_id}: {before:#?}"));
    // The seat vacates only on the provider's exit proof, which node recovery takes at the gone
    // birth's TRUE OFFLINE mark (elections §4.1, §5; `node_recovery_due`). The announcement names the
    // winner it elected and the birth it replaced; the election's span is the only place that fact
    // lives (a view may already show the birth drift recovery brings back in the seat's cohort), so the
    // span is awaited.
    wait_for(&format!("{name} ({id}) announced as the successor to {gone_id}"), crate::node_recovery_due(), || async {
        announced(&estate.spans(), c, &id, gone_id).then_some(())
    })
    .await;
    // Another admin may announce first: `admin`'s own view holds the killed birth dead before
    // anything is read from it.
    estate
        .until_view_at(admin, &format!("{admin} no longer hears {gone_id}"), rafka_mesh_transport::membership::staleness_floor() + Duration::from_secs(30), |views| {
            (!views.iter().any(|n| n.node_id.to_string() == gone_id && !matches!(n.status, NodeStatus::Dead | NodeStatus::PendingReconnect))).then_some(())
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
    let admin1 = estate.entry().await;
    let (rpc, admin) = (cohort("mesh1", "rpc_node"), cohort("mesh1", "node_admin"));
    let hold = Duration::from_secs(4);

    // day 0: one admin, every seat its own.
    let day0 = settle(&estate, &admin1, "day 0").await;
    let boot = day0.iter().find(|n| n["kind"] == "node_admin").map(|n| (s(&n["name"]), s(&n["node_id"]))).unwrap();
    assert_eq!(advertised_fabric(&day0).as_deref(), Some(boot.0.as_str()), "day 0: the one admin holds the fabric seat");

    // settled MN -> the computed seats
    build(&estate, &[("mesh1", 2, 3)]).await;
    estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    let nodes = settle(&estate, &admin1, "MN").await;
    let rpc_p = primary_of(&nodes, &rpc).unwrap();
    assert_eq!(advertised_fabric(&nodes).as_deref(), Some(boot.0.as_str()), "the Day-0 admin keeps the fabric seat whatever NodeId its mesh's second admin drew (R-A2)");

    // The initial fabric refuses a node restart until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // restart a non-primary -> it keeps its NodeId; the seats stay computed
    let other_rpc: Vec<String> = nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["is_primary"] == false).map(|n| s(&n["name"])).collect();
    let before = id_of(&nodes, &other_rpc[1]);
    complete(&format!("node.restart of {}", other_rpc[1]), estate.nodes_obj().await.restart(&path(&other_rpc[1])).await).await;
    // The Build completed on the executor; the asked admin's view holds the reborn node once its
    // Ready has reached it (one gossip round): wait for the shape, as after the Build above.
    estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    let nodes = settle(&estate, &admin1, "restart non-primary").await;
    assert_eq!(id_of(&nodes, &other_rpc[1]), before, "restart keeps the NodeId");
    steady(&estate, &admin1, "restart non-primary", hold).await;

    // restart the primary -> node.primary.update moves its seat to the next-lowest ready NodeId
    // first (C1 step 1: the holder is commanded "as a step of the Build that ... restarts" it); the
    // restarted node keeps its NodeId and never takes the seat back (elections §5)
    let rpc_p_id = id_of(&nodes, &rpc_p);
    let (succ_r, succ_r_id) = next_lowest(&nodes, &rpc, &rpc_p_id);
    complete(&format!("node.restart of {rpc_p}"), estate.nodes_obj().await.restart(&path(&rpc_p)).await).await;
    let nodes = settle(&estate, &admin1, "restart primary").await;
    assert_eq!(id_of(&nodes, &rpc_p), rpc_p_id, "restart keeps the NodeId");
    assert_eq!(primary_of(&nodes, &rpc).as_deref(), Some(succ_r.as_str()), "node.primary.update moved the seat of {rpc_p} to {succ_r} before the restart, and the restarted node never displaces it: {nodes:#?}");
    steady(&estate, &admin1, "restart primary", hold).await;

    // grow, then shrink back -> one holder per cohort on every sample (a newcomer never displaces the holder)
    build(&estate, &[("mesh1", 3, 5)]).await;
    settle(&estate, &admin1, "grow").await;
    steady(&estate, &admin1, "grow", hold).await;
    build(&estate, &[("mesh1", 2, 3)]).await;
    settle(&estate, &admin1, "shrink").await;
    steady(&estate, &admin1, "shrink", hold).await;

    // kill the current primary -> the next-lowest succeeds; drift recovery restores the count with
    // a new NodeId at a free path
    let nodes = estate.node_values().await;
    let killed = primary_of(&nodes, &rpc).unwrap();
    let killed_id = id_of(&nodes, &killed);
    // Until this kill the holder the restart's node.primary.update seated lives; after it the vacancy is
    // filled by the ordinary rule, which may seat the restarted node (elections §5: "a restarted node is a
    // candidate for a later vacancy").
    let holder_lived_until = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.kill_node(&killed).await;
    let (_succ, succ_id) = successor(&estate, &admin1, &nodes, &rpc, &killed_id).await;
    build(&estate, &[("mesh1", 2, 3)]).await;
    // The rebirth is drift recovery's, once the killed runtime is proven exited: wait for the
    // count, then for the seats.
    estate
        .until_view_at(&admin1, "drift recovery restores three live rpc nodes", rafka_mesh_transport::membership::staleness_floor() * 2 + Duration::from_secs(30), |views| {
            (views.iter().filter(|n| n.mesh == "mesh1" && n.kind == NodeKind::RpcNode && n.status == NodeStatus::ReadyForTraffic).count() == 3).then_some(())
        })
        .await;
    let nodes = settle(&estate, &admin1, "the cohort is back at its desired count").await;
    estate.artifact("settled-after-kill.json", &json!({"killed": killed, "killed_id": killed_id, "live": nodes, "all": estate.node_values().await}));
    assert!(!nodes.iter().any(|n| n["node_id"] == killed_id.as_str()), "the killed birth {killed_id} is not live again: {nodes:#?}");
    assert_eq!(nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node").count(), 3, "three live rpc nodes: {nodes:#?}");
    steady(&estate, &admin1, "the cohort is back at its desired count", hold).await;

    // legal removal of the primary -> the next-lowest succeeds
    let removed = primary_of(&nodes, &rpc).unwrap();
    let removed_id = id_of(&nodes, &removed);
    complete(&format!("node.delete of {removed}"), estate.nodes_obj().await.delete(&path(&removed)).await).await;
    let nodes = estate
        .until_view("the view drops the removed primary", Duration::from_secs(30), |views| {
            let nodes = Estate::values(views);
            (!nodes.iter().any(|n| n["name"] == removed.as_str() && n["status"] == "ready-for-traffic") && primary_of(&nodes, &rpc).is_some()).then_some(nodes)
        })
        .await;
    let succ2 = primary_of(&nodes, &rpc).unwrap();
    assert_eq!(expected_primaries(&nodes).get(&rpc), Some(&succ2), "the next-lowest succeeds: {nodes:#?}");
    let succ2_id = id_of(&nodes, &succ2);
    build(&estate, &[("mesh1", 2, 3)]).await;
    settle(&estate, &admin1, "after removal").await;
    steady(&estate, &admin1, "after removal", hold).await;

    // partition -> transient split permitted; heal -> the computed seats in every view
    let nodes = estate.node_values().await;
    let admin_p = primary_of(&nodes, &admin).unwrap();
    let admin2_name = s(&nodes.iter().find(|n| n["kind"] == "node_admin" && n["name"] != admin_p.as_str()).unwrap()["name"]);
    let (admin2, admin_pp) = (path(&admin2_name), path(&admin_p));
    let rpc_now = primary_of(&nodes, &rpc).unwrap();
    let lone_rpc = nodes.iter().find(|n| n["kind"] == "rpc_node" && n["name"] != rpc_now.as_str()).map(|n| s(&n["name"])).unwrap();
    let side_a = vec![admin_p.clone(), lone_rpc.clone()];
    let side_b: Vec<String> = nodes.iter().map(|n| s(&n["name"])).filter(|n| !side_a.contains(n)).collect();
    match Partition::start(&udp_ports(&nodes, &side_a), &udp_ports(&nodes, &side_b)) {
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() == Ok("1") => {
            panic!("RDM_REQUIRE_NETFAULT=1 but this host cannot partition: {why}")
        }
        Err(why) => eprintln!("SKIP partition case: {why}"),
        Ok(partition) => {
            // Side A stops hearing the rpc holder, and elects no rpc primary of its own: silence and an
            // observer-inferred Dead are not proof, the holder keeps the seat (elections §5).
            let a_view = estate
                .until_view_at(&admin_pp, &format!("side A no longer hears the rpc holder {rpc_now}"), rafka_mesh_transport::membership::staleness_floor() + Duration::from_secs(30), |views| {
                    let nodes = Estate::values(views);
                    nodes.iter().any(|n| n["name"] == rpc_now.as_str() && n["status"] != "ready-for-traffic").then_some(nodes)
                })
                .await;
            assert_eq!(primary_of(&a_view, &rpc).as_deref(), Some(rpc_now.as_str()), "a cut-off rpc holder keeps its seat: side A names no rpc primary of its own: {a_view:#?}");
            // Side B has now heard nothing from the admin primary for longer than the staleness
            // floor, and it elects no admin primary of its own: a silent holder keeps its seat, and
            // silence is not proof of exit (R-A2).
            let b_view = estate.node_values_at(&admin2_name).await;
            assert_ne!(primary_of(&b_view, &admin).as_deref(), Some(admin2_name.as_str()), "a cut-off holder keeps the admin seat: side B names no admin primary of its own: {b_view:#?}");
            for at in [&admin_pp, &admin2] {
                for (c, p) in advertised_primaries(&estate.node_values_at(&at.to_string()).await) {
                    assert!(p.len() <= 1, "{at}: a view never names two primaries of {c:?}: {p:?}");
                }
            }
            drop(partition);
            // A view cut off from a side is already "settled" on the seats it computes from the
            // members it hears; it has healed only once it holds every pre-cut member as live
            // and ready. Wait for that fact (the same member set in both views), then for the
            // computed seats over it.
            let everyone: Vec<String> = nodes.iter().map(|n| s(&n["name"])).collect();
            let mut healed = Vec::new();
            for at in [&admin_pp, &admin2] {
                estate
                    .until_view_at(at, &format!("{at} holds every pre-cut member ready"), Duration::from_secs(30), |views| {
                        let view = live(&Estate::values(views));
                        everyone.iter().all(|name| view.iter().any(|n| n["name"] == name.as_str() && n["status"] == "ready-for-traffic")).then_some(())
                    })
                    .await;
                healed.push(settle(&estate, at, &format!("{at} heals")).await);
            }
            assert_eq!(advertised_primaries(&healed[0]), advertised_primaries(&healed[1]), "healed views agree");
            steady(&estate, &admin2, "healed", hold).await;
        }
    }

    estate.artifact("nodes.json", &json!(estate.node_values().await));
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
    // The restart's node.primary.update moved the rpc seat to the next-lowest ready NodeId, and
    // the mesh-primary announced it (C1 step 7).
    assert!(primary_updated(&spans, &rpc, &format!("node.restart:{rpc_p}"), &succ_r_id), "the mesh-primary announced the rpc seat updated to {succ_r} ({succ_r_id}) before the restart of {rpc_p}");
    // The restarted node never took its seat back while the holder lived.
    let back = named(&spans, "rdm.mesh.election.resolve.via-recompute").iter().any(|sp| {
        let a = &sp["attributes"];
        sp["start_unix_nano"].as_u64().unwrap_or(0) < holder_lived_until && a["mesh"] == "mesh1" && a["kind"] == "rpc_node" && a["winner_node_id"] == rpc_p_id.as_str() && a["previous_node_id"] == succ_r_id.as_str()
    });
    assert!(!back, "the restarted {rpc_p} displaced the holder {succ_r}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_meshs_admin_cohort_elects_its_lowest_node_id() {
    let mut estate = Estate::bootstrap(owner("a_second_meshs_admin_cohort_elects", "MM"), "fabric1", "mesh1").await;
    let admin1 = estate.entry().await;
    let admin2 = cohort("mesh2", "node_admin");
    let hold = Duration::from_secs(4);

    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    // A peer mesh's members reach this admin through its primary's backbone
    // publication: the view settles within a publication round or two.
    let nodes = estate
        .until_view("the MM view settles with four cohort primaries", Duration::from_secs(15), |views| {
            let nodes = Estate::values(views);
            (settled(&nodes) && advertised_primaries(&nodes).len() == 4).then_some(nodes)
        })
        .await;
    let p = primary_of(&nodes, &admin2).unwrap();
    let p_id = id_of(&nodes, &p);
    estate.artifact("settled-before-kill.json", &json!({"killed": p, "killed_id": p_id, "nodes": nodes}));

    // kill the mesh2 admin primary -> the next-lowest succeeds; the fabric seat is recomputed
    estate.kill_node(&p).await;
    let (succ, succ_id) = successor(&estate, &admin1, &nodes, &admin2, &p_id).await;
    // The successor was announced; the repair of the same Build rebirths the killed path within
    // seconds, and that birth never displaces the successor whatever NodeId it drew (elections §5).
    // The view is read once the killed birth is dead in it.
    let nodes = estate
        .until_view("the view holds the killed birth dead", Duration::from_secs(30), |views| {
            let nodes = Estate::values(views);
            (!nodes.iter().any(|n| n["node_id"] == p_id.as_str() && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))) && primary_of(&nodes, &admin2).is_some()).then_some(nodes)
        })
        .await;
    assert_eq!(primary_of(&nodes, &admin2).as_deref(), Some(succ.as_str()), "the announced successor keeps mesh2's admin seat over the birth drift recovery brings back at the killed path: {nodes:#?}");
    estate.artifact("successor-after-kill.json", &json!({"successor": succ, "successor_id": succ_id, "nodes": nodes}));
    assert_eq!(advertised_fabric(&nodes).as_deref(), Some("mesh1.admin.1"), "the fabric seat stays with the Day-0 admin of mesh1 while mesh2 loses and regains admins (R-A2)");
    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    settle(&estate, &admin1, "mesh2's killed admin is back").await;
    steady(&estate, &admin1, "mesh2's killed admin is back", hold).await;

    // The initial fabric refuses the removal of a node until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // legal removal of the mesh2 admin primary -> the next-lowest succeeds
    let nodes = estate.node_values().await;
    let removed = primary_of(&nodes, &admin2).unwrap();
    let removed_id = id_of(&nodes, &removed);
    complete(&format!("node.delete of {removed}"), estate.nodes_obj().await.delete(&path(&removed)).await).await;
    // The removed primary keeps its seat while it drains and leaves: a seat moves only by an
    // explicit transfer or a proven loss, never by its holder's status (R-EL1). The view is
    // read once the seat has left the removed birth.
    let nodes = estate
        .until_view("the removed admin primary's seat moves to another admin of its mesh", Duration::from_secs(30), |views| {
            let nodes = Estate::values(views);
            (!nodes.iter().any(|n| n["name"] == removed.as_str() && n["status"] == "ready-for-traffic") && primary_of(&nodes, &admin2).is_some_and(|p| p != removed)).then_some(nodes)
        })
        .await;
    let succ2 = primary_of(&nodes, &admin2).unwrap();
    assert_eq!(expected_primaries(&nodes).get(&admin2), Some(&succ2));
    let succ2_id = id_of(&nodes, &succ2);
    assert_eq!(advertised_fabric(&nodes).as_deref(), Some("mesh1.admin.1"), "the fabric seat stays with the Day-0 admin of mesh1 while mesh2 loses and regains admins (R-A2)");
    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    settle(&estate, &admin1, "mesh2 after removal").await;
    steady(&estate, &admin1, "mesh2 after removal", hold).await;

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
