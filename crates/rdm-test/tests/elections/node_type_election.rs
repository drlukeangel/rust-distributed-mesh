//! i143.e4.s14 process E2E: the canonical cohort election (PRD §11;
//! `docs/architecture/fabric-node-lifecycle-elections.md`).
//!
//! Every primary, each cohort's (rpc, node-admin: the mesh primary) and the
//! fabric-primary, follows elections §5: "a heard primary stays primary; a
//! lower-NodeId candidate that later becomes Ready never displaces it";
//! silence past the staleness floor supersedes it; "restart preserves NodeId;
//! a restarted node is a candidate for a later vacancy and never displaces the
//! primary". A holder a
//! Build stops, restarts or deletes yields first by node.primary.update to the
//! cohort's lowest Ready NodeId with the holder excluded (proposal
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
//! - UDP cut between two sides of the mesh (Q111, Q122; one host, one store): the side with an eligible
//!   Ready successor supersedes the primary it no longer hears at the floor; each side
//!   re-creates only births node investigation did not reach; heal: every pre-cut path.name holds one
//!   live Ready birth and every admin's view advertises the same seats. No side's outcome is
//!   pinned.
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
use rdm_test::elections::{advertised_primaries, expected_primaries, primaries_as_expected, primaries_kept, NodeType};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::netfault::{udp_ports, Partition};
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

fn node_type_of(mesh: &str, kind: &str) -> NodeType {
    (mesh.into(), kind.into())
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn id_of(nodes: &[Value], name: &str) -> String {
    nodes.iter().find(|n| n["name"] == name).map(|n| s(&n["node_id"])).unwrap_or_else(|| panic!("no node {name}"))
}

/// The one primary of `c` in `nodes`, if exactly one.
fn primary_of(nodes: &[Value], c: &NodeType) -> Option<String> {
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
    !nodes.is_empty() && nodes.iter().all(|n| n["status"] == "ready") && primaries_as_expected(&nodes).is_ok()
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
        if nodes.iter().all(|n| n["status"] == "ready") {
            if let Err(e) = primaries_as_expected(&nodes) {
                panic!("{label}: {e}: {nodes:#?}");
            }
            match &first {
                None => first = Some(nodes),
                Some(before) => {
                    if let Err(e) = primaries_kept(before, &nodes) {
                        panic!("{label}: {e}: before {before:#?} after {nodes:#?}");
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The lowest ready NodeId of `c` in `nodes` other than `holder` (a NodeId): the successor the
/// node-type election names with the holder excluded (proposal Part C1 step 2).
fn next_lowest(nodes: &[Value], c: &NodeType, holder: &str) -> (String, String) {
    nodes
        .iter()
        .filter(|n| n["mesh"] == c.0.as_str() && n["kind"] == c.1.as_str() && n["status"] == "ready" && n["node_id"] != holder)
        .map(|n| (s(&n["name"]), s(&n["node_id"])))
        .min_by(|a, b| a.1.cmp(&b.1))
        .unwrap_or_else(|| panic!("no other ready member of {c:?} than {holder}: {nodes:#?}"))
}

/// When the successor, the NodeId `successor`, installed itself as `c`'s primary by the update
/// `operation_suffix` (a Build operation's key, the end of the update's operation identity) and answered
/// node.primary.updated (lifecycles/node-primary-update.md step 4: its `via-primary-updated` span's
/// start), if it did.
fn primary_updated(spans: &[Value], c: &NodeType, operation_suffix: &str, successor: &str) -> Option<u64> {
    let entry = format!("{} ", c.1.replace('_', "-"));
    named(spans, "rdm.node_admin.primary.update.via-primary-updated")
        .iter()
        .filter(|sp| {
            let a = &sp["attributes"];
            a["operation"].as_str().is_some_and(|o| o.ends_with(operation_suffix)) && a["primaries"].as_str().is_some_and(|v| v.starts_with(&entry) && v.contains(&format!("-> {successor}/")))
        })
        .filter_map(|sp| sp["start_unix_nano"].as_u64())
        .min()
}

/// The successor to `gone_id` in `c`: the lowest NodeId among the node type's other ready members
/// in `before` (the view just before the kill), announced by the mesh as the winner after
/// `gone_id`. Decided from the pre-kill view and the announcement span, never from a polled
/// view: drift recovery rebirths the path within seconds, and a view polled after that already
/// shows the re-born NodeId (a different birth) in the seat.
async fn successor(estate: &Estate, admin: &PathName, before: &[Value], c: &NodeType, gone_id: &str) -> (String, String) {
    let (name, id) = before
        .iter()
        .filter(|n| n["mesh"] == c.0.as_str() && n["kind"] == c.1.as_str() && n["status"] == "ready" && n["node_id"] != gone_id)
        .map(|n| (s(&n["name"]), s(&n["node_id"])))
        .min_by(|a, b| a.1.cmp(&b.1))
        .unwrap_or_else(|| panic!("no other ready member of {c:?} to succeed {gone_id}: {before:#?}"));
    // The seat vacates only on the provider's exited verdict, which node recovery takes at the gone
    // birth's TRUE OFFLINE mark (elections §4.1, §5; `node_recovery_due`). The announcement names the
    // winner it elected and the birth it replaced; the election's span is the only place that fact
    // lives (a view may already show the birth drift recovery brings back in the seat's node type), so the
    // span is awaited.
    wait_for(&format!("{name} ({id}) announced as the successor to {gone_id}"), crate::node_recovery_due(), || async {
        announced(&estate.spans(), c, &id, gone_id).then_some(())
    })
    .await;
    // Another admin may announce first: `admin`'s own view holds the killed birth dead before
    // anything is read from it.
    estate
        .until_view_at(admin, &format!("{admin} no longer hears {gone_id}"), rdm_transport::topology::staleness_floor() + Duration::from_secs(30), |views| {
            (!views.iter().any(|n| n.node_id.to_string() == gone_id && !matches!(n.status, NodeStatus::Dead | NodeStatus::PendingReconnect))).then_some(())
        })
        .await;
    (name, id)
}

/// The node-type election span that announced `winner` for `c` after `previous` (NodeIds).
fn announced(spans: &[Value], c: &NodeType, winner: &str, previous: &str) -> bool {
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
async fn every_mn_node_type_elects_the_lowest_ready_node_id_through_the_matrix() {
    let mut estate = Estate::bootstrap(owner("every_mn_node_type_elects_the_lowest_ready_node_id", "MN"), "fabric1", "mesh1").await;
    let admin1 = estate.entry().await;
    let (rpc, admin) = (node_type_of("mesh1", "rpc_node"), node_type_of("mesh1", "node_admin"));
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

    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // stop then start a non-primary -> it keeps its NodeId; the seats stay computed
    let other_rpc: Vec<String> = nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["is_primary"] == false).map(|n| s(&n["name"])).collect();
    let before = id_of(&nodes, &other_rpc[1]);
    complete(&format!("node.stop of {}", other_rpc[1]), estate.nodes_obj().await.stop(&path(&other_rpc[1])).await).await;
    estate.until_stopped_at_the_fabric_primary(&path(&other_rpc[1])).await;
    complete(&format!("node.start of {}", other_rpc[1]), estate.nodes_obj().await.start(&path(&other_rpc[1])).await).await;
    // The Build completed on the executor; the asked admin's view holds the re-born node once its
    // Ready has reached it (one gossip interval): wait for the shape, as after the Build above.
    estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
    let nodes = settle(&estate, &admin1, "restart non-primary").await;
    assert_eq!(id_of(&nodes, &other_rpc[1]), before, "restart keeps the NodeId");
    steady(&estate, &admin1, "restart non-primary", hold).await;

    // stop then start the primary -> node.primary.update moves its seat to the next-lowest ready NodeId
    // first (C1 step 1: the holder is commanded "as a step of the Build that stops" it); the
    // started node keeps its NodeId and never takes the seat back (elections §5)
    let rpc_p_id = id_of(&nodes, &rpc_p);
    let (succ_r, succ_r_id) = next_lowest(&nodes, &rpc, &rpc_p_id);
    complete(&format!("node.stop of {rpc_p}"), estate.nodes_obj().await.stop(&path(&rpc_p)).await).await;
    estate.until_stopped_at_the_fabric_primary(&path(&rpc_p)).await;
    complete(&format!("node.start of {rpc_p}"), estate.nodes_obj().await.start(&path(&rpc_p)).await).await;
    let nodes = settle(&estate, &admin1, "restart primary").await;
    assert_eq!(id_of(&nodes, &rpc_p), rpc_p_id, "restart keeps the NodeId");
    assert_eq!(primary_of(&nodes, &rpc).as_deref(), Some(succ_r.as_str()), "node.primary.update moved the seat of {rpc_p} to {succ_r} before the stop, and the started node never displaces it: {nodes:#?}");
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
    // The rebirth is drift recovery's, once the killed runtime is proven exited: wait for the
    // count, then for the seats.
    estate
        .until_view_at(&admin1, "drift recovery restores three live rpc nodes", rdm_transport::topology::staleness_floor() * 2 + Duration::from_secs(30), |views| {
            (views.iter().filter(|n| n.mesh == "mesh1" && n.kind == NodeKind::RpcNode && n.status == NodeStatus::Ready).count() == 3).then_some(())
        })
        .await;
    let nodes = settle(&estate, &admin1, "the node type is back at its desired count").await;
    estate.artifact("settled-after-kill.json", &json!({"killed": killed, "killed_id": killed_id, "live": nodes, "all": estate.node_values().await}));
    assert!(!nodes.iter().any(|n| n["node_id"] == killed_id.as_str()), "the killed birth {killed_id} is not live again: {nodes:#?}");
    assert_eq!(nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node").count(), 3, "three live rpc nodes: {nodes:#?}");
    steady(&estate, &admin1, "the node type is back at its desired count", hold).await;

    // legal removal of the primary -> the next-lowest succeeds
    let removed = primary_of(&nodes, &rpc).unwrap();
    let removed_id = id_of(&nodes, &removed);
    complete(&format!("node.delete of {removed}"), estate.nodes_obj().await.delete(&path(&removed)).await).await;
    let nodes = estate
        .until_view("the view drops the removed primary", Duration::from_secs(30), |views| {
            let nodes = Estate::values(views);
            (!nodes.iter().any(|n| n["name"] == removed.as_str() && n["status"] == "ready") && primary_of(&nodes, &rpc).is_some()).then_some(nodes)
        })
        .await;
    let succ2 = primary_of(&nodes, &rpc).unwrap();
    assert_eq!(expected_primaries(&nodes).get(&rpc), Some(&succ2), "the next-lowest succeeds: {nodes:#?}");
    let succ2_id = id_of(&nodes, &succ2);
    build(&estate, &[("mesh1", 2, 3)]).await;
    settle(&estate, &admin1, "after removal").await;
    steady(&estate, &admin1, "after removal", hold).await;

    // The partition (Q111, Q122): one host, one store; no side's outcome is pinned, each is read from the
    // trace. Side A holds the fabric-primary (the mesh's primary too) and one rpc node; side B the rest.
    let nodes = estate.node_values().await;
    let admin_p = primary_of(&nodes, &admin).unwrap();
    let admin2_name = s(&nodes.iter().find(|n| n["kind"] == "node_admin" && n["name"] != admin_p.as_str()).unwrap()["name"]);
    let (admin2, admin_pp) = (path(&admin2_name), path(&admin_p));
    let rpc_now = primary_of(&nodes, &rpc).unwrap();
    let lone_rpc = nodes.iter().find(|n| n["kind"] == "rpc_node" && n["name"] != rpc_now.as_str()).map(|n| s(&n["name"])).unwrap();
    let side_a = vec![admin_p.clone(), lone_rpc.clone()];
    let side_b: Vec<String> = nodes.iter().map(|n| s(&n["name"])).filter(|n| !side_a.contains(n)).collect();
    let everyone: Vec<String> = nodes.iter().map(|n| s(&n["name"])).collect();
    let floor = rdm_transport::topology::staleness_floor();
    match Partition::start(&udp_ports(&nodes, &side_a), &udp_ports(&nodes, &side_b)) {
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() == Ok("1") => {
            panic!("RDM_REQUIRE_NETFAULT=1 but this host cannot partition: {why}")
        }
        Err(why) => eprintln!("SKIP partition case: {why}"),
        Ok(partition) => {
            let cut_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
            // Side A has an eligible Ready successor to the rpc primary it no longer hears: at the
            // floor the election supersedes the stale primary (elections §4.1, §5).
            let superseded = wait_for(&format!("side A supersedes the unheard rpc primary {rpc_now} with {lone_rpc}"), floor * 3 + Duration::from_secs(30), || {
                let spans = estate.spans();
                let found = named(&spans, "rdm.mesh.election.resolve.via-recompute")
                    .into_iter()
                    .find(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) >= cut_ns && sp["attributes"]["observer"] == admin_p.as_str() && sp["attributes"]["election_level"] == "node_type" && sp["attributes"]["kind"] == "rpc_node" && sp["attributes"]["winner_path"] == lone_rpc.as_str())
                    .cloned();
                async move { found }
            })
            .await;
            estate.record_trace_url(superseded["trace_id"].as_str().unwrap_or(""));
            // The cut stands for a few floors: the investigations and recoveries of both sides run.
            tokio::time::sleep(floor * 4).await;
            drop(partition);
            // Healed: every pre-cut path.name holds one live Ready birth (a re-created one carries
            // a new node id), and the views converge. Whichever pre-cut admin answers is read first; its view
            // lists the births re-created during the cut, which the estate dials from then on.
            let converged = |views: &[rdm_client::NodeView]| {
                let view = live(&Estate::values(views));
                let one_each = everyone.iter().all(|name| view.iter().filter(|n| n["name"] == name.as_str()).count() == 1 && view.iter().any(|n| n["name"] == name.as_str() && n["status"] == "ready"));
                (one_each && primaries_as_expected(&view).is_ok()).then_some(view)
            };
            // Both pre-cut admin paths are asked on every pass: an answer teaches the estate the births the
            // other path holds now (a re-created admin is dialled where a view names it).
            let deadline = tokio::time::Instant::now() + floor * 12 + Duration::from_secs(120);
            let healed = loop {
                let mut last = Vec::new();
                let mut views = Vec::new();
                for at in [&admin_pp, &admin2] {
                    match estate.node_views_at(at).await {
                        Some(v) => {
                            let ns = live(&Estate::values(&v));
                            last.push(format!("{at}: {:?} seats={:?}", ns.iter().map(|n| format!("{}={}/{} p={} f={}", s(&n["name"]), s(&n["status"]), s(&n["node_id"]), n["is_primary"], n["is_fabric_primary"])).collect::<Vec<_>>(), primaries_as_expected(&ns)));
                            views.push(converged(&v));
                        }
                        None => {
                            last.push(format!("{at}: no answer"));
                            views.push(None);
                        }
                    }
                }
                if let [Some(a), Some(b)] = &views[..] {
                    break vec![a.clone(), b.clone()];
                }
                assert!(tokio::time::Instant::now() < deadline, "both pre-cut admin paths answer with a converged view after the heal; last: {last:#?}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            };
            assert_eq!(advertised_primaries(&healed[0]), advertised_primaries(&healed[1]), "healed views agree");
            let spans = estate.spans();
            let after = |sp: &&Value| sp["start_unix_nano"].as_u64().unwrap_or(0) >= cut_ns;
            // Within every view an observer computed during and after the cut: at most one primary per
            // (mesh, node type).
            for sp in named(&spans, "rdm.mesh.election.resolve.via-recompute").into_iter().filter(after) {
                let winner = s(&sp["attributes"]["winner_path"]);
                assert!(!winner.contains(','), "one primary per node type in every view: {sp}");
            }
            // Recovery follows node investigation's verdict: nothing was re-created on `reached`.
            for sp in named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(after) {
                assert_ne!(s(&sp["attributes"]["verdict"]), "reached", "a reached birth is commanded back, never re-created: {sp}");
            }
            for sp in named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(after) {
                assert_ne!(s(&sp["attributes"]["verdict"]), "reached", "fabric recovery re-creates only a birth node investigation did not reach: {sp}");
            }
            steady(&estate, &admin_pp, "healed", hold).await;
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
    assert!(own("rdm.mesh.election.resolve.via-recompute", "node_type"), "day 0: node-admin type");
    assert!(own("rdm.mesh.election.resolve.via-mesh-primary", "mesh_primary"), "day 0: mesh primary");
    assert!(own("rdm.mesh.election.resolve.via-fabric-recompute", "fabric_primary"), "day 0: fabric primary");
    assert!(announced(&spans, &rpc, &succ_id, &killed_id), "the successor to the killed primary was announced");
    assert!(announced(&spans, &rpc, &succ2_id, &removed_id), "the successor to the removed primary was announced");
    // The restart's node.primary.update moved the rpc-node primary to the next-lowest ready NodeId,
    // which installed itself and answered (node-primary-update.md step 4).
    let seat_moved_at = primary_updated(&spans, &rpc, &format!("node.stop:{rpc_p}"), &succ_r_id)
        .unwrap_or_else(|| panic!("{succ_r} ({succ_r_id}) installed itself as the rpc-node primary before the restart of {rpc_p}"));
    // The restarted node never took its seat back while the holder lived: from the restart's seat move
    // until the kill. Before that move the cohort's first election (no holder record) seats the lowest
    // ready NodeId as each member of the forming MN becomes ready, which may move the seat from the
    // restart's successor to the node restarted later; that is the formation's election, not a
    // displacement (elections §5).
    let back = named(&spans, "rdm.mesh.election.resolve.via-recompute").iter().any(|sp| {
        let a = &sp["attributes"];
        let at = sp["start_unix_nano"].as_u64().unwrap_or(0);
        (seat_moved_at..holder_lived_until).contains(&at) && a["mesh"] == "mesh1" && a["kind"] == "rpc_node" && a["winner_node_id"] == rpc_p_id.as_str() && a["previous_node_id"] == succ_r_id.as_str()
    });
    assert!(!back, "the restarted {rpc_p} displaced the holder {succ_r}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_meshs_node_admins_elect_their_lowest_node_id() {
    let mut estate = Estate::bootstrap(owner("a_second_meshs_node_admins_elect", "MM"), "fabric1", "mesh1").await;
    let admin1 = estate.entry().await;
    let admin2 = node_type_of("mesh2", "node_admin");
    let hold = Duration::from_secs(4);

    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // A peer mesh's members reach this admin through its primary's backbone
    // publication: the view settles within a publication or two.
    let nodes = estate
        .until_view("the MM view settles with four node-type primaries", Duration::from_secs(15), |views| {
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
    estate
        .until_view_at(&admin1, "drift recovery restores two live mesh2 admins", rdm_transport::topology::staleness_floor() * 2 + Duration::from_secs(30), |views| {
            (views.iter().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::Ready).count() == 2).then_some(())
        })
        .await;
    settle(&estate, &admin1, "mesh2's killed admin is back").await;
    steady(&estate, &admin1, "mesh2's killed admin is back", hold).await;

    // The initial fabric refuses the removal of a node until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // legal removal of the mesh2 admin primary -> the next-lowest succeeds
    let nodes = estate.node_values().await;
    let removed = primary_of(&nodes, &admin2).unwrap();
    let removed_id = id_of(&nodes, &removed);
    complete(&format!("node.delete of {removed}"), estate.nodes_obj().await.delete(&path(&removed)).await).await;
    // The removed primary keeps its seat while it deletes its connections and leaves: a seat moves only by an
    // explicit transfer or a proven loss, never by its holder's status (R-EL1). The view is
    // read once the seat has left the removed birth.
    let nodes = estate
        .until_view("the removed admin primary's seat moves to another admin of its mesh", Duration::from_secs(30), |views| {
            let nodes = Estate::values(views);
            (!nodes.iter().any(|n| n["name"] == removed.as_str() && n["status"] == "ready") && primary_of(&nodes, &admin2).is_some_and(|p| p != removed)).then_some(nodes)
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
    // The mesh primary is the node-admin type's winner, announced as such.
    assert!(
        named(&spans, "rdm.mesh.election.resolve.via-mesh-primary").iter().any(|sp| {
            let a = &sp["attributes"];
            a["mesh"] == "mesh2" && a["source_kind"] == "node_admin" && a["winner_node_id"] == succ2_id.as_str()
        }),
        "mesh2's primary follows its node-admin type"
    );
}

fn advertised_fabric(nodes: &[Value]) -> Option<String> {
    let fp = rdm_test::elections::advertised_fabric_primaries(nodes);
    (fp.len() == 1).then(|| fp[0].clone())
}
