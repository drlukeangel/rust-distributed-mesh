//! Rafka-time discipline process cell (i143.e12.s21): a node whose rafka-time is put off its
//! authority's is brought back to it by the heartbeats the authority already publishes.
//!
//! The cell runs a two-mesh fabric of two node-admins and two rpc nodes per mesh. Three rpc nodes
//! carry a scenario-recorded knob (`rafka_node_rpc_testkit::rafka_time_knob_dir`), applied by the
//! testkit executable once the node has adopted its authority's time:
//!
//! - `mesh2.rpc.1` runs its monotonic time 2 % slow;
//! - `mesh1.rpc.1` re-adopts a rafka-time 1.5 s ahead of the authority's;
//! - `mesh2.rpc.2` runs on an OS clock an hour ahead (the clock the binaries never read).

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-runtime".into(),
        subfeature: "rafka-time-discipline".into(),
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

fn num(sp: &Value, k: &str) -> Option<i64> {
    attr(sp, k).parse().ok()
}

fn names() -> BTreeSet<String> {
    ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=2).map(move |i| format!("{m}.rpc.{i}")))).collect()
}

const OS_CLOCK_SKEW_MS: i64 = 3_600_000;
const AHEAD_MS: i64 = 1_500;
const DRIFT_PPM: i64 = -20_000;

/// The heartbeats of `node` as `(start, clock_skew_ms)` in start order.
fn skews(spans: &[Value], node: &str) -> Vec<(u64, i64)> {
    let mut v: Vec<(u64, i64)> = named(spans, "rdm.mesh.node.update.via-heartbeat").into_iter().filter(|sp| attr(sp, "node") == node).filter_map(|sp| Some((sp["start_unix_nano"].as_u64()?, num(sp, "clock_skew_ms")?))).collect();
    v.sort();
    v
}

/// CONTRACT: a node whose rafka-time runs slow is stepped forward to its mesh primary's stamp on
/// the heartbeats and stays within a fraction of a second of the authority for as long as it runs;
/// a node whose rafka-time stands 1.5 s ahead is told by a whole window of its mesh primary's
/// stamps that it is ahead, sheds the excess at the maximum slew rate (its rafka-time minus the
/// host clock falls, never rises), and does not step back; a mesh primary of the other mesh
/// decides on its fabric-primary's aggregate; and a node on an OS clock an hour ahead is moved by
/// none of it. Every one of these is a span of the node it happened to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_off_its_authoritys_time_is_brought_back_by_the_heartbeats() {
    let mut estate = Estate::bootstrap(owner("a_node_off_its_authoritys_time_is_brought_back_by_the_heartbeats"), "fabric1", "mesh1").await;
    let write = |dir: std::path::PathBuf, node: &str, text: String| {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(node), text).unwrap();
    };
    write(rafka_node_rpc_testkit::rafka_time_knob_dir(&estate.root), "mesh2.rpc.1", format!("drift_ppm={DRIFT_PPM}\n"));
    write(rafka_node_rpc_testkit::rafka_time_knob_dir(&estate.root), "mesh1.rpc.1", format!("ahead_ms={AHEAD_MS}\n"));
    write(rafka_node_rpc_testkit::os_clock_skew_dir(&estate.root), "mesh2.rpc.2", OS_CLOCK_SKEW_MS.to_string());
    let shape = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 2},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 2},
    ]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    estate.await_attempt(&s(&a["build_id"]), Estate::attempt_of(&a), Duration::from_secs(120)).await;
    let nodes = estate.settled(&names(), Duration::from_secs(30)).await;
    let mesh2_primary = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("mesh2 has a primary");
    let fabric_primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");

    // The ahead node is told, by its mesh primary's stamps, that it is ahead; the slow node is stepped.
    let slewing = wait_for("the ahead node shed its excess", Duration::from_secs(90), || {
        let spans = estate.spans();
        let found = named(&spans, "rdm.mesh.entry.update.via-rafka-time-observed").into_iter().find(|sp| attr(sp, "node") == "mesh1.rpc.1" && attr(sp, "outcome") == "slew-started").cloned();
        async move { found }
    })
    .await;
    // Then forty more seconds of heartbeats: undisciplined, the slow node would stand 800 ms behind.
    tokio::time::sleep(Duration::from_secs(40)).await;
    estate.stop().await;
    let spans = estate.spans();

    // The knobs were applied, by the node they name.
    let knobs = named(&spans, "rdm.mesh.node.update.via-rafka-time-knob");
    for (node, ahead, drift) in [("mesh2.rpc.1", 0, DRIFT_PPM), ("mesh1.rpc.1", AHEAD_MS, 0)] {
        assert!(knobs.iter().any(|sp| attr(sp, "node") == node && num(sp, "ahead_ms") == Some(ahead) && num(sp, "drift_ppm") == Some(drift)), "{node} ran under its knob: {knobs:?}");
    }

    // The ahead node: one window of stamps behind by more than the allowance, a shed at the maximum
    // rate of the excess, and its offset to the host clock only falls from there.
    assert_eq!((attr(&slewing, "source").as_str(), num(&slewing, "slew_ppm")), ("mesh-primary-digest", Some(500)), "{slewing}");
    let excess = num(&slewing, "excess_ms").unwrap();
    assert!((800..=1_500).contains(&excess), "the excess is the 1.5 s offset less the 500 ms allowance, less the delivery delay: {excess}");
    let started = slewing["start_unix_nano"].as_u64().unwrap();
    let ahead = skews(&spans, "mesh1.rpc.1");
    let after: Vec<&(u64, i64)> = ahead.iter().filter(|(t, _)| *t > started).collect();
    assert!(after.len() >= 4, "heartbeats after the shed began: {ahead:?}");
    let (first, last) = (after.first().unwrap().1, after.last().unwrap().1);
    assert!(first > 1_000, "the node stood ahead: {first} ms");
    assert!(first - last >= 8, "the clock shed time while slewing: {first} -> {last}");
    assert!(after.windows(2).all(|w| w[1].1 <= w[0].1 + 3), "the ahead node's offset never rises: {after:?}");

    // The slow node is stepped on its mesh primary's heartbeats and stays near the authority.
    let steps: Vec<&Value> = named(&spans, "rdm.mesh.entry.update.via-rafka-time-observed").into_iter().filter(|sp| attr(sp, "node") == "mesh2.rpc.1" && attr(sp, "outcome") == "stepped-forward").collect();
    assert!(steps.len() >= 5, "the slow node was stepped forward repeatedly: {}", steps.len());
    assert!(steps.iter().all(|sp| attr(sp, "source") == "mesh-primary-digest"), "a member observes only its mesh primary's digest");
    let slow = skews(&spans, "mesh2.rpc.1");
    let authority = skews(&spans, &fabric_primary);
    assert!(slow.len() >= 8 && !authority.is_empty());
    let lowest = slow.iter().map(|(_, k)| *k).min().unwrap();
    assert!(lowest > -300, "a clock 2 % slow would stand 800 ms behind after 40 s; the lowest offset to the host clock is {lowest} ms");

    // A mesh primary of the other mesh decided on its fabric-primary's Members aggregate; the
    // fabric-primary decided nothing from any follower.
    let decisions = |node: &str| -> Vec<&Value> { named(&spans, "rdm.mesh.entry.update.via-rafka-time-observed").into_iter().filter(|sp| attr(sp, "node") == node).collect() };
    let by_primary = decisions(&mesh2_primary);
    if mesh2_primary != fabric_primary {
        assert!(!by_primary.is_empty() && by_primary.iter().all(|sp| attr(sp, "source") == "fabric-primary-members"), "{mesh2_primary} observed its fabric-primary's aggregate: {by_primary:?}");
    }
    assert!(decisions(&fabric_primary).is_empty(), "the fabric-primary observed no follower");
    for sp in named(&spans, "rdm.mesh.entry.update.via-rafka-time-observed") {
        assert!(["mesh-primary-digest", "fabric-primary-members"].contains(&attr(sp, "source").as_str()), "{sp}");
    }

    // The node on the OS clock an hour ahead: its rafka-time never read it.
    let os = skews(&spans, "mesh2.rpc.2");
    assert!(!os.is_empty() && os.iter().all(|(_, k)| k.abs() < 1_000), "mesh2.rpc.2 stood within 1 s of the host clock on an OS clock an hour ahead: {os:?}");
}
