//! Ruling R-A2 process cells: the fabric-primary seat stays in its mesh until that mesh is proven
//! unable to hold it, and a silent holder is looked at, never replaced for being silent.
//!
//! Each cell runs a two-mesh fabric of two node-admins and three rpc nodes per mesh. A cell that
//! kills the fabric primary is permitted by R-P1; no other cell kills a fabric-primary node-admin.

use rafka_test_scenario::estate::{descends_from, iroh_observation_shape, named, wait_for, Estate, Owner};
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

/// A numeric span attribute (exported as a string).
fn num(sp: &Value, k: &str) -> Option<i64> {
    attr(sp, k).parse().ok()
}

fn names() -> BTreeSet<String> {
    ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=3).map(move |i| format!("{m}.rpc.{i}")))).collect()
}

async fn fabric(cell: &str) -> (Estate, Vec<Value>) {
    fabric_skewing(cell, None).await
}

/// The OS-clock skew one ordinary node of a cell runs under: one hour ahead.
const OS_CLOCK_SKEW_MS: i64 = 3_600_000;

/// [`fabric`], with the OS clock of the ordinary node `skewed` (if any) an hour ahead from its start.
async fn fabric_skewing(cell: &str, skewed: Option<&str>) -> (Estate, Vec<Value>) {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    if let Some(node) = skewed {
        let dir = rafka_node_rpc_testkit::os_clock_skew_dir(&estate.root);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(node), OS_CLOCK_SKEW_MS.to_string()).unwrap();
    }
    let shape = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 3},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 3},
    ]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    estate.await_attempt(&s(&a["build_id"]), Estate::attempt_of(&a), Duration::from_secs(120)).await;
    let nodes = estate.settled(&names(), Duration::from_secs(30)).await;
    // The initial fabric refuses a node replace until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
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
    // One ordinary node of each mesh runs on an OS clock an hour ahead: rafka-time is adopted from
    // the authority and no stamp of any node reads the OS clock.
    let (mut estate, nodes) = fabric_skewing("a_dead_fabric_primary_hands_the_seat_to_the_surviving_admin_of_its_own_mesh", Some("mesh2.rpc.1")).await;
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
    let spans: Vec<Value> = estate.spans();
    assert!(
        named(&spans, "rdm.mesh.seat.update.via-announce").iter().any(|sp| attr(sp, "node") == heir && attr(sp, "seat") == "fabric-primary" && attr(sp, "holder").contains(&heir_id)),
        "the heir announced the fabric-primary seat (the other mesh's primary id is {} the heir's: {})",
        if discriminating { "below" } else { "above" },
        other_primary.1
    );

    // rafka-time is one lineage across the fabric.
    let adopted = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted");
    // The Day-0 root adopted its own OS clock, once, and says why.
    let own: Vec<&&Value> = adopted.iter().filter(|sp| attr(sp, "source") == "own-clock").collect();
    assert_eq!(own.len(), 1, "exactly one own-clock adoption in the fabric: {own:?}");
    assert_eq!((attr(own[0], "node").as_str(), attr(own[0], "reason").as_str()), ("mesh1.admin.1", "day0-root"));
    // Every other node adopted from the answer to the join it made, served by the admin that deployed it.
    let joined: BTreeSet<String> = adopted.iter().filter(|sp| attr(sp, "source") == "pull" && attr(sp, "via") == "join").map(|sp| attr(sp, "node")).collect();
    let expected: BTreeSet<String> = names().into_iter().filter(|n| n != "mesh1.admin.1").collect();
    assert!(expected.is_subset(&joined), "every node but the root adopted at its join; missing {:?}", expected.difference(&joined).collect::<Vec<_>>());
    // The skewed node ran an hour ahead (its boot span says so), and every heartbeat of every node
    // carries rafka-time less the host clock: none is off by the skew, and the nodes stay within a
    // second of one another.
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment");
    assert!(
        boot.iter().any(|sp| attr(sp, "node") == "mesh2.rpc.1" && num(sp, "os_clock_skew_ms") == Some(OS_CLOCK_SKEW_MS)),
        "mesh2.rpc.1 ran on an OS clock {OS_CLOCK_SKEW_MS} ms ahead"
    );
    let beats = named(&spans, "rdm.mesh.node.update.via-heartbeat");
    let skew_of = |sp: &&Value| num(sp, "clock_skew_ms").unwrap_or(i64::MAX);
    assert!(beats.iter().any(|sp| attr(sp, "node") == "mesh2.rpc.1"), "mesh2.rpc.1 published heartbeats");
    let (lo, hi) = (beats.iter().map(skew_of).min().unwrap(), beats.iter().map(skew_of).max().unwrap());
    assert!(lo > -1_000 && hi < 1_000, "every node's rafka-time stands within 1 s of the host clock (the skewed node's OS clock was {OS_CLOCK_SKEW_MS} ms ahead): {lo}..{hi} ms over {} heartbeats", beats.len());
    // The fabric seat moved to the heir: the other mesh's primary pulled once from it and adopted
    // its time under the mesh-primary rule.
    let moved: Vec<&Value> = named(&spans, "rdm.mesh.entry.resolve.via-fabric-seat-moved").into_iter().filter(|sp| attr(sp, "node") == other_primary.0).collect();
    assert_eq!(moved.len(), 1, "the other mesh's primary pulled once when the fabric seat moved: {moved:?}");
    assert_eq!((attr(moved[0], "outcome").as_str(), attr(moved[0], "to").as_str()), ("adopted", heir_id.as_str()), "{:?}", moved[0]);
    let child = adopted.iter().find(|a| rafka_test_scenario::estate::descends_from(&spans, a, moved[0])).expect("the adoption is a child of the pull");
    assert_eq!(
        (attr(child, "source").as_str(), attr(child, "via").as_str(), attr(child, "seat").as_str(), attr(child, "served_by").as_str(), attr(child, "node").as_str()),
        ("pull", "get-topology", "fabric-primary", heir.as_str(), other_primary.0.as_str())
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

fn at_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// CONTRACT (the approved proposal docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md
/// Part C1, Luke yes 2026-10-12: "One writer, one row, one point of no return"; Q1: "the one row of C1
/// commits both seats to NA2 at their next epochs"; step 8: "Only then does the commanding Build
/// continue ... The successor is now the holder's primary"): the fabric-primary P is replaced through
/// the control API (a Build attempt the rectifier executes; never a signal). P holds both seats and is
/// its mesh's row writer, so it moves them itself before the replace runs:
/// 1. P says "primary updating" (`via-announce-primary-updating`, sent on the backbone and its mesh
///    channel), cuts off the writes of both seats (`via-writes-cut-off`, the fabric seat included) and
///    writes the ONE committed row (`seat.update.via-seat-row-written` at P: holder P, successor N, both
///    seats at their next epochs).
/// 2. N, P's same-mesh successor, installs from the row and answers node.primary.updated
///    (`seat.update.via-primary-updated` at N, not a replay, under P's update); then P says "primary
///    updated" (`via-announce-primary-updated`).
/// 3. P's drive ends at the row (`seat-lost`; `build.reject.via-fenced` naming N), and P decides no claim
///    after it. N claims the attempt the update preceded and no later one, and runs the replace; no step
///    on P runs before the row.
/// Must NOT happen: a view showing two fabric-primaries; an observer resolving P's mesh or the fabric
/// with no primary between the replace and the fabric's ready-for-traffic; N writing a seat row or
/// saying "primary updated"; a birth made twice; an attempt with two winners. The fabric is ready again
/// under the same Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts() {
    let cell = "a_planned_hand_over_moves_both_seats_in_the_old_holders_one_row_before_the_new_one_acts";
    let (mut estate, nodes) = fabric(cell).await;
    let p = fabric_primary(&nodes).expect("one fabric primary");
    let p_node = node(&nodes, &p).clone();
    let p_id = s(&p_node["node_id"]);
    let mesh = s(&p_node["mesh"]);
    // The heir is the other admin of P's mesh; control stays on it.
    let n_name = if p.ends_with(".1") { format!("{mesh}.admin.2") } else { format!("{mesh}.admin.1") };
    let n_id = s(&node(&nodes, &n_name)["node_id"]);
    let n_base = s(&node(&nodes, &n_name)["admin_api_base"]);
    estate.admin = n_base.clone();
    let (_, before_build) = estate.get("/api/fabric").await;
    let replaced_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let (status, r) = estate.post(&format!("/api/nodes/{p}/replace"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    let build = s(&r["build_id"]);
    // Sampled while the hand-over runs: no admin's view ever shows two fabric primaries.
    let mut worst = 0usize;
    let until = std::time::Instant::now() + Duration::from_secs(120);
    let mut done = false;
    while std::time::Instant::now() < until && !done {
        let r = estate.nodes_at(&n_base).await;
        let held = r.iter().filter(|x| x["is_fabric_primary"] == true).count();
        worst = worst.max(held);
        done = r.iter().any(|x| x["name"] == p.as_str() && x["incarnation_id"] != p_node["incarnation_id"] && x["status"] == "ready-for-traffic") && held == 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(done, "the replaced admin's path is ready again under a new birth");
    assert!(worst <= 1, "a view showed {worst} fabric primaries");
    // The fabric is ready-for-traffic once its planned births are all ready and the state-sync,
    // state-commit and open-traffic rounds have run under the new fabric-primary.
    let after = wait_for("the fabric is ready-for-traffic under the new fabric-primary", Duration::from_secs(60), || async {
        let f = estate.get("/api/fabric").await.1;
        (f["status"] == "ready-for-traffic").then_some(f)
    })
    .await;
    let ready_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.stop().await;
    let spans = estate.spans();
    let one = |name: &str, f: &dyn Fn(&Value) -> bool| -> Value {
        let found: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| f(sp)).collect();
        assert_eq!(found.len(), 1, "one {name}: {found:#?}");
        found[0].clone()
    };
    // C1 steps 2-5 at P: the pre-check named N, "primary updating", the cut-off, the ONE row.
    let updating = one("rdm.node_admin.seat.update.via-primary-updating", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build);
    assert_eq!((attr(&updating, "seat").as_str(), attr(&updating, "successor").as_str(), attr(&updating, "outcome").as_str()), ("mesh-primary+fabric-primary", n_name.as_str(), "updated"), "P moved both its seats to N: {updating:#?}");
    let said_updating = one("rdm.mesh.seat.update.via-announce-primary-updating", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build);
    assert_eq!((attr(&said_updating, "backbone").as_str(), attr(&said_updating, "mesh_channel").as_str()), ("sent", "sent"), "{said_updating:#?}");
    let cut_off = one("rdm.node_admin.seat.update.via-writes-cut-off", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build);
    assert_eq!(attr(&cut_off, "fabric_seat"), "true", "the fabric seat's writes were cut off too: {cut_off:#?}");
    let row = one("rdm.node_admin.seat.update.via-seat-row-written", &|sp| attr(sp, "build_id") == build);
    assert_eq!(attr(&row, "node"), p, "the holder P, its mesh's row writer, wrote the row: {row:#?}");
    assert!(attr(&row, "holder").contains(&p_id) && attr(&row, "successor").contains(&n_id), "the row moves P's seats to N: {row:#?}");
    assert_eq!((num(&row, "mesh_epoch").is_some(), num(&row, "fabric_epoch").is_some()), (true, true), "both seats moved in the one row: {row:#?}");
    let row_at = at_ns(&row);
    // C1 step 6: N installs from the row and answers node.primary.updated, under P's update call.
    let installed = one("rdm.node_admin.seat.update.via-primary-updated", &|sp| attr(sp, "node") == n_name && attr(sp, "build_id") == build);
    assert_eq!((attr(&installed, "replay").as_str(), attr(&installed, "outcome").as_str()), ("false", "held"), "N installed P's row: {installed:#?}");
    assert_eq!((attr(&installed, "mesh_epoch"), attr(&installed, "fabric_epoch")), (attr(&row, "mesh_epoch"), attr(&row, "fabric_epoch")), "N installed the row's epochs: {installed:#?}");
    assert!(descends_from(&spans, &installed, &updating), "N installs under P's node.primary.update call: {installed:#?}");
    // C1 step 7: P says "primary updated" once N answered. The successor neither says it nor writes a row.
    let said_updated = one("rdm.mesh.seat.update.via-announce-primary-updated", &|sp| attr(sp, "node") == p && attr(sp, "build_id") == build);
    assert_eq!((attr(&said_updated, "backbone").as_str(), attr(&said_updated, "mesh_channel").as_str()), ("sent", "sent"), "{said_updated:#?}");
    let by_n: Vec<&Value> = named(&spans, "rdm.mesh.seat.update.via-announce-primary-updated").into_iter().filter(|sp| attr(sp, "node") == n_name).collect();
    assert!(by_n.is_empty(), "the successor said primary updated: {by_n:#?}");
    let n_rows: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-seat-row-written").into_iter().filter(|sp| attr(sp, "node") == n_name).collect();
    assert!(n_rows.is_empty(), "the successor wrote a seat row: {n_rows:#?}");
    // The order of the sequence (C1 steps 3-7).
    assert!(at_ns(&said_updating) <= at_ns(&cut_off) && sp_end(&cut_off) <= row_at, "P said primary updating, then cut off its writes, then wrote the row");
    assert!(sp_end(&row) <= at_ns(&installed), "N installed only after P's row");
    assert!(sp_end(&installed) <= at_ns(&said_updated), "P said primary updated only after N answered");
    // Step 8: the replace runs after the row, from the attempt the update preceded: no step on P before it.
    let transfer_attempt = num(&row, "attempt").unwrap();
    let p_steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "build_id") == build && attr(sp, "node") == p && num(sp, "attempt").is_some_and(|a| a >= transfer_attempt)).collect();
    assert!(!p_steps.is_empty(), "the replace's steps on P ran from attempt {transfer_attempt}");
    assert!(p_steps.iter().all(|sp| at_ns(sp) >= sp_end(&row)), "no step on P ran before the row: {p_steps:#?}");
    // No seatless window: from the replace until the fabric is ready again (the fabric's own
    // shutdown after that is not this cell's story), no observer resolves P's mesh or the fabric
    // with nobody.
    let vacant: Vec<&Value> = named(&spans, "rdm.mesh.election.resolve.via-mesh-primary")
        .into_iter()
        .filter(|sp| attr(sp, "mesh") == mesh && attr(sp, "winner_path").is_empty() && attr(sp, "previous_node_id") != "" && (replaced_at..ready_at).contains(&at_ns(sp)))
        .chain(named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute").into_iter().filter(|sp| attr(sp, "winner_path").is_empty() && attr(sp, "previous_node_id") != "" && (replaced_at..ready_at).contains(&at_ns(sp))))
        .collect();
    assert!(vacant.is_empty(), "a seat was vacant during the hand-over: {vacant:#?}");
    // P was the fabric-primary driving the Build: its drive ends at its row (seat-lost), its committed
    // seat check naming N (build_drive.rs `may_act`). The replace came in by REST and the seats moved
    // before any claim (build_drive.rs `seats_first`), so P dispatched nothing to N: N claims the
    // attempt the update preceded itself, and no later attempt is claimed.
    let p_drive: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-drive").into_iter().filter(|sp| attr(sp, "node") == p && attr(sp, "build_id") == build).collect();
    assert!(p_drive.iter().any(|sp| attr(sp, "outcome") == "seat-lost"), "P's drive ends at its seat row: {p_drive:#?}");
    let p_refused: Vec<&Value> = named(&spans, "rdm.node_admin.build.reject.via-fenced").into_iter().filter(|sp| attr(sp, "node") == p && attr(sp, "build_id") == build).collect();
    assert!(!p_refused.is_empty() && p_refused.iter().all(|sp| attr(sp, "to") == n_name), "P's drive lost the seat at the committed record naming N: {p_refused:#?}");
    let moved: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-seat-moved").into_iter().filter(|sp| attr(sp, "build_id") == build).collect();
    assert!(moved.iter().all(|sp| attr(sp, "to") == n_name), "every SeatMoved of the Build names N: {moved:#?}");
    let n_claimed: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| attr(sp, "node") == n_name && attr(sp, "build_id") == build && attr(sp, "outcome") == "won" && num(sp, "attempt") == Some(transfer_attempt)).collect();
    assert_eq!(n_claimed.len(), 1, "N claimed the attempt the update preceded: {:#?}", named(&spans, "rdm.node_admin.build.update.via-claim-decision"));
    // A planned move leaves no fault anywhere in the run (the fault span of a handler is a root of its
    // own trace, so the whole run is read).
    let faults = named(&spans, "rdm.node_rpc.handler.reject.via-internal-rpc-failure");
    assert!(faults.is_empty(), "a planned seat move left a handler fault: {faults:#?}");
    let later_claims: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| attr(sp, "build_id") == build && attr(sp, "outcome") == "won" && num(sp, "attempt").is_some_and(|a| a > transfer_attempt)).collect();
    assert!(later_claims.is_empty(), "N ran the attempt the update preceded; no later attempt was claimed: {later_claims:#?}");
    // The Build facts were handed on when P's record moved.
    let hand_over: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-seat-hand-over").into_iter().filter(|sp| attr(sp, "node") == p).collect();
    assert!(hand_over.iter().any(|sp| sp["attributes"]["facts"].as_str().and_then(|f| f.parse::<u64>().ok()).is_some()), "the hand-over span names the facts handed on: {hand_over:#?}");
    // No claim decided by P after its row.
    let late: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| attr(sp, "node") == p && at_ns(sp) > row_at && matches!(attr(sp, "outcome").as_str(), "won" | "lost" | "not-open")).collect();
    assert!(late.is_empty(), "P decided a claim after its row: {late:#?}");
    // One winner per attempt; no birth made twice.
    let mut won: std::collections::BTreeMap<(String, String), std::collections::BTreeSet<String>> = Default::default();
    let mut made: std::collections::BTreeMap<(String, String, String), u32> = Default::default();
    for sp in &spans {
        match sp["name"].as_str() {
            Some("rdm.node_admin.build.update.via-claim-decision") if attr(sp, "outcome") == "won" => {
                won.entry((attr(sp, "build_id"), attr(sp, "attempt"))).or_default().insert(attr(sp, "executor"));
            }
            Some("rdm.node_admin.node.create.via-build") => *made.entry((attr(sp, "build_id"), attr(sp, "attempt"), attr(sp, "node"))).or_default() += 1,
            _ => {}
        }
    }
    assert!(won.values().all(|w| w.len() == 1), "conflicting winners: {won:#?}");
    assert!(made.values().all(|c| *c == 1), "a birth made twice: {made:#?}");
    assert_eq!(after["status"], "ready-for-traffic", "the fabric is ready again: {after}");
    estate.record_trace_url(row["trace_id"].as_str().unwrap_or(""));
    let _ = before_build;
}

/// CONTRACT (the approved proposal Part C1 step 5: "The writer writes the one seat row at the next
/// revision(s) naming the successor. This is the commit. From this write the holder is no longer
/// primary"): the same hand-over while the fabric-primary P is deciding claims of a running Build
/// (mesh2 grows by two rpc nodes as P is replaced). P writes its ONE row under its Build-log decision
/// lock (`seat.update.via-seat-row-written` at P), before N installs it (`seat.update.via-primary-updated`
/// at N): a claim P won committed wholly before the row, and P wins none after it. One winner per
/// attempt, no birth twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_in_flight_commits_before_the_old_holders_seat_row_and_none_is_decided_after() {
    let cell = "a_claim_in_flight_commits_before_the_old_holders_seat_row_and_none_is_decided_after";
    let (mut estate, nodes) = fabric(cell).await;
    let p = fabric_primary(&nodes).expect("one fabric primary");
    let mesh = s(&node(&nodes, &p)["mesh"]);
    let p_birth = node(&nodes, &p)["incarnation_id"].clone();
    let n_name = if p.ends_with(".1") { format!("{mesh}.admin.2") } else { format!("{mesh}.admin.1") };
    estate.admin = s(&node(&nodes, &n_name)["admin_api_base"]);
    let grow = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 3},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 5},
    ]});
    let (status, g) = estate.post("/api/build", &grow).await;
    assert_eq!(status, 202, "{g}");
    let (status, r) = estate.post(&format!("/api/nodes/{p}/replace"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    let all: BTreeSet<String> = names().into_iter().chain(["mesh2.rpc.4".to_string(), "mesh2.rpc.5".to_string()]).collect();
    // The replace is part of the story: the fabric is stopped only once P's path is ready again under
    // a new birth, so the fabric's own shutdown never races the seat transfer.
    wait_for("the grown fabric is settled under the new holder and P's path is a new birth", Duration::from_secs(180), || async {
        let ns = estate.nodes().await;
        let replaced = ns.iter().any(|x| x["name"] == p.as_str() && x["incarnation_id"] != p_birth && x["status"] == "ready-for-traffic");
        (replaced && all.iter().all(|n| ns.iter().any(|x| x["name"] == n.as_str() && x["status"] == "ready-for-traffic")) && ns.iter().filter(|x| x["is_fabric_primary"] == true).count() == 1).then_some(())
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    // C1 step 5: P's ONE row is the commit, written under its Build-log decision lock
    // (fabric_handover.rs `update_primary_in`), before N installs it.
    let replace = s(&r["build_id"]);
    let row: Vec<&Value> = named(&spans, "rdm.node_admin.seat.update.via-seat-row-written").into_iter().filter(|sp| attr(sp, "build_id") == replace).collect();
    assert_eq!(row.len(), 1, "P wrote one seat row for the replace: {row:#?}");
    assert_eq!(attr(row[0], "node"), p, "the holder P wrote the row: {:#?}", row[0]);
    let installed = named(&spans, "rdm.node_admin.seat.update.via-primary-updated").into_iter().filter(|sp| attr(sp, "build_id") == replace && attr(sp, "node") == n_name).map(at_ns).min().expect("N installed the committed row");
    assert!(sp_end(row[0]) <= installed, "P's row was written before N installed it");
    // A decision P won committed wholly before the row moved its record; none after it.
    let row_at = at_ns(row[0]);
    let mine: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| attr(sp, "node") == p && at_ns(sp) < sp_end(row[0]) + 3_000_000_000 && attr(sp, "outcome") == "won").collect();
    for sp in &mine {
        assert!(sp_end(sp) <= row_at, "P won a claim that did not commit before its seat row: {sp:#?}");
    }
    let mut won: std::collections::BTreeMap<(String, String), BTreeSet<String>> = Default::default();
    for sp in named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|sp| attr(sp, "outcome") == "won") {
        won.entry((attr(sp, "build_id"), attr(sp, "attempt"))).or_default().insert(attr(sp, "executor"));
    }
    assert!(won.values().all(|w| w.len() == 1), "conflicting winners: {won:#?}");
    estate.record_trace_url(row[0]["trace_id"].as_str().unwrap_or(""));
}

fn sp_end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}
