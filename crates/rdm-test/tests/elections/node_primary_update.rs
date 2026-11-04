//! node.primary.update and node.fabric-primary.update as the operation (lifecycles/node-primary-update.md)
//! and fabric-recovery.md's playbook: a same-mesh successor becomes both primaries on its direct answer and
//! the record follows; a lost fabric-primary is succeeded within its mesh while a node-admin of it lives;
//! a mesh-primary change reaches the other mesh; with no node-admin of its mesh available to the backbone
//! the fabric-primary is taken over at the decision step and handed back after the heal.
//!
//! Each cell runs a two-mesh fabric of two node-admins and three nodes per mesh. A cell that
//! kills the fabric primary is permitted by R-P1; no other cell kills a fabric-primary node-admin.

use rdm_core::PathName;
use rdm_test::estate::location;
use rdm_test::estate::{descends_from, iroh_observation_shape, named, wait_for, Estate, Owner};
use rdm_test::netfault::{udp_ports, Partition};
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "node-primary-update".into(),
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

fn names() -> BTreeSet<PathName> {
    ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| Estate::path(&format!("{m}.node-admin"), &i.to_string())).chain((1..=3).map(move |i| Estate::path(&format!("{m}.node"), &i.to_string())))).collect()
}

async fn fabric(cell: &str) -> (Estate, Vec<Value>) {
    fabric_skewing(cell, None).await
}

/// The OS-clock skew one ordinary node of a cell runs under: one hour ahead.
const OS_CLOCK_SKEW_MS: i64 = 3_600_000;

/// [`fabric`], with the OS clock of the ordinary node `skewed` (if any) an hour ahead from its start.
async fn fabric_skewing(cell: &str, skewed: Option<&rdm_core::PathName>) -> (Estate, Vec<Value>) {
    born(Estate::bootstrap(owner(cell), "fabric1", "mesh1").await, skewed).await
}

async fn born(estate: Estate, skewed: Option<&rdm_core::PathName>) -> (Estate, Vec<Value>) {
    if let Some(node) = skewed {
        let dir = rdm_test::os_clock_skew_dir(&estate.root);
        let file = rdm_test::location_knob_file(&dir, node);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, OS_CLOCK_SKEW_MS.to_string()).unwrap();
    }
    estate.resize_shape("fabric1", &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    let nodes = Estate::values(&estate.settles_to_named(&names(), Duration::from_secs(30)).await);
    // The initial fabric refuses a node replace until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    (estate, nodes)
}

fn node<'a>(nodes: &'a [Value], name: &PathName) -> &'a Value {
    nodes.iter().find(|n| location(n) == *name).unwrap_or_else(|| panic!("{name:?} is not in the view"))
}

/// The sibling admin of `admin` in its mesh of two: `1` and `2` stand for each other.
fn sibling(admin: &PathName) -> PathName {
    admin.with_name(if admin.name == "1" { "2" } else { "1" }).expect("a sibling admin's location")
}

/// The mesh a location belongs to: its path's first segment.
fn mesh_of(at: &PathName) -> String {
    at.path.split('.').next().unwrap_or_default().to_string()
}

fn fabric_primary(nodes: &[Value]) -> Option<PathName> {
    nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| location(&n))
}

async fn kill(estate: &mut Estate, name: &PathName) {
    if *name == Estate::path("mesh1.node-admin", "1") {
        estate.kill_bootstrap();
    } else {
        estate.kill_node(name).await;
    }
}

/// CONTRACT (fabric-recovery.md "The loss of the fabric-primary while a node-admin lives";
/// lifecycles/node-primary-update.md: "The mesh-primary and the fabric-primary move together: a same-mesh
/// successor becomes both"): the fabric-primary is stopped and its process killed while another admin of
/// its mesh survives. That admin, the heir, the lowest Ready NodeId of the SAME mesh, becomes
/// fabric-primary whatever the other mesh's primary's id is, and stays it; rafka-time is one lineage
/// across the fabric, and the other mesh's primary pulled once from the heir when the fabric-primary moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_fabric_primary_is_succeeded_by_the_surviving_admin_of_its_own_mesh() {
    // One ordinary node of each mesh runs on an OS clock an hour ahead: rafka-time is adopted from
    // the primaries and no stamp of any node reads the OS clock.
    let (mut estate, nodes) = fabric_skewing("a_lost_fabric_primary_is_succeeded_by_the_surviving_admin_of_its_own_mesh", Some(&rdm_core::PathName::new("mesh2.node", "1").unwrap())).await;
    let old = fabric_primary(&nodes).expect("one fabric primary");
    let mesh = mesh_of(&old);
    let heir = sibling(&old);
    let other_mesh = if mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let other_primary = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == other_mesh && n["type"] == "node-admin" && n["is_primary"] == true).map(|n| (location(&n), s(&n["node_id"]))).unwrap();
    let heir_id = s(&node(&nodes, &heir)["node_id"]);
    let discriminating = other_primary.1 < heir_id;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter(&heir).await;
    let mut stop = estate.nodes_obj().await.stop(&old).await.expect("node.stop of old");
    // A stop is a node command, never a Build (Q342): its stream ends with the stop's own completion.
    let frames = Estate::frames_of(&mut stop).await;
    assert_eq!(frames.last(), Some(&rdm_client::Frame::Complete), "the stop completes: {frames:?}");
    kill(&mut estate, &old).await;
    let seen = estate
        .until_view("the surviving admin of the fabric-primary's mesh is fabric-primary", Duration::from_secs(90), |views| {
            let ns = Estate::values(views);
            (fabric_primary(&ns).as_ref() == Some(&heir)).then_some(ns)
        })
        .await;
    // The heir stays fabric-primary: the other mesh's primary does not take it from it.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let later = estate.node_values().await;
    estate.stop().await;
    assert_eq!(fabric_primary(&later).as_ref(), Some(&heir), "the heir stays fabric-primary: {later:#?}");
    assert_eq!(seen.iter().filter(|n| n["is_fabric_primary"] == true).count(), 1);
    let spans: Vec<Value> = estate.spans();
    assert!(
        named(&spans, "rdm.node_admin.primary.update.via-primary-updated").iter().any(|sp| rdm_test::estate::of_node(sp, &heir, &heir_id) && attr(sp, "outcome") == "held" && attr(sp, "primaries").contains("fabric ") && attr(sp, "primaries").contains(&format!("-> {heir_id}/")))
            || named(&spans, "rdm.mesh.primary.update.via-announcement").iter().any(|sp| rdm_test::estate::of_node(sp, &heir, &heir_id) && attr(sp, "role") == "fabric-primary" && attr(sp, "primary").contains(&heir_id)),
        "the heir installed the fabric-primary from the update's call or from its own election (the other mesh's primary id is {} the heir's: {})",
        if discriminating { "below" } else { "above" },
        other_primary.1
    );

    // rafka-time is one lineage across the fabric.
    let adopted = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted");
    // The Day-0 root adopted its own OS clock, once, and says why.
    let own: Vec<&&Value> = adopted.iter().filter(|sp| attr(sp, "source") == "own-clock").collect();
    assert_eq!(own.len(), 1, "exactly one own-clock adoption in the fabric: {own:?}");
    let root_id = s(&node(&nodes, &Estate::path("mesh1.node-admin", "1"))["node_id"]);
    assert_eq!((attr(own[0], "node").as_str(), attr(own[0], "reason").as_str()), (root_id.as_str(), "day0-root"));
    // Every other node adopted from the answer to the join it made, served by the admin that deployed it.
    let joined: BTreeSet<String> = adopted.iter().filter(|sp| attr(sp, "source") == "pull" && attr(sp, "via") == "join").map(|sp| attr(sp, "node")).collect();
    // Adoption spans name the node by its node id.
    let expected: BTreeSet<String> = names().into_iter().filter(|n| *n != Estate::path("mesh1.node-admin", "1")).map(|n| s(&node(&nodes, &n)["node_id"])).collect();
    assert!(expected.is_subset(&joined), "every node but the root adopted at its join; missing {:?}", expected.difference(&joined).collect::<Vec<_>>());
    // The skewed node ran an hour ahead (its boot span says so), and every heartbeat of every node
    // carries rafka-time less the host clock: none is off by the skew, and the nodes stay within a
    // second of one another.
    let boot = named(&spans, "rdm.mesh.node.create.via-deployment");
    assert!(
        boot.iter().any(|sp| rdm_test::estate::at_location(sp, &Estate::path("mesh2.node", "1")) && num(sp, "os_clock_skew_ms") == Some(OS_CLOCK_SKEW_MS)),
        "mesh2.node.1 ran on an OS clock {OS_CLOCK_SKEW_MS} ms ahead"
    );
    let beats = named(&spans, "rdm.mesh.node.update.via-heartbeat");
    let skew_of = |sp: &&Value| num(sp, "clock_skew_ms").unwrap_or(i64::MAX);
    assert!(beats.iter().any(|sp| rdm_test::estate::at_location(sp, &Estate::path("mesh2.node", "1"))), "mesh2.node.1 published heartbeats");
    let (lo, hi) = (beats.iter().map(skew_of).min().unwrap(), beats.iter().map(skew_of).max().unwrap());
    assert!(lo > -1_000 && hi < 1_000, "every node's rafka-time stands within 1 s of the host clock (the skewed node's OS clock was {OS_CLOCK_SKEW_MS} ms ahead): {lo}..{hi} ms over {} heartbeats", beats.len());
    // The fabric-primary moved to the heir: the other mesh's primary pulled once from it and adopted
    // its time under the mesh-primary rule.
    let moved: Vec<&Value> = named(&spans, "rdm.mesh.entry.resolve.via-fabric-primary-changed").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &other_primary.0)).collect();
    assert_eq!(moved.len(), 1, "the other mesh's primary pulled once when the fabric-primary moved: {moved:?}");
    assert_eq!((attr(moved[0], "outcome").as_str(), attr(moved[0], "to").as_str()), ("adopted", heir_id.as_str()), "{:?}", moved[0]);
    let child = adopted.iter().find(|a| rdm_test::estate::descends_from(&spans, a, moved[0])).expect("the adoption is a child of the pull");
    assert_eq!(
        (attr(child, "source").as_str(), attr(child, "via").as_str(), attr(child, "kind").as_str(), attr(child, "served_by").as_str(), attr(child, "node").as_str()),
        ("pull", "get-topology", "fabric-primary", heir_id.as_str(), other_primary.1.as_str())
    );
}

/// CONTRACT (node-config-update.md step 1: "The caller sends `node.config.update` to the
/// fabric-primary. It writes the map to the node in its atlas and sends the command to the
/// node's mesh-primary"; query.md §1: "Node rows and property maps are not copied into an atlas to
/// serve `node.get` or acceptance ... Properties are read separately at the subject node ID"): a
/// caller's `node.config.update` of a node of the other mesh, sent to the fabric-primary, ends
/// `node.config.updated`, and the fabric-primary's `mesh.nodes.get` answers the node's config at once,
/// from the node's own record. The fabric-primary is then stopped and its process killed; the
/// surviving admin of its mesh, the new fabric-primary, takes every mesh into its atlas's scope as it
/// becomes the fabric-primary and answers the same config from the same record. Spans: the old
/// fabric-primary's `rdm.node_admin.node.update.via-fabric-primary` (`config-updated`, sent to the
/// node's mesh-primary) and the successor's `rdm.node_admin.fabric.update.via-atlas-scope` (the other
/// mesh taken). Must NOT happen: the read answering the config the node had before, at once or after
/// the hand-over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_config_update_is_read_back_from_the_fabric_primary_at_once_and_after_a_hand_over() {
    let (mut estate, nodes) = fabric("a_config_update_is_read_back_from_the_fabric_primary_at_once_and_after_a_hand_over").await;
    let old = fabric_primary(&nodes).expect("one fabric primary");
    let mesh = mesh_of(&old);
    let successor = sibling(&old);
    let other_mesh = if mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let subject_name = Estate::path(&format!("{other_mesh}.node"), "1");
    let subject = node(&nodes, &subject_name).clone();
    let node_id = rdm_core::NodeId::parse(&s(&subject["node_id"])).expect("a node id");
    let incarnation = rdm_core::IncarnationId::parse(&s(&subject["incarnation"])).expect("a live birth");
    let fp_id = rdm_core::NodeId::parse(&s(&node(&nodes, &old)["node_id"])).expect("the fabric-primary's id");
    let told: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node_id.clone()), node_id: node_id.clone(), incarnation: incarnation.clone() };
    let carrier = estate.carrier().await;
    let reply = rdm_client::NodeRpc::new(carrier.client()).config_update(&fp_id, &rdm_transport::RouteChoice::Direct, &birth, told.clone(), &rdm_transport::CallOptions::default()).await.expect("node.config.update answered");
    assert!(matches!(&reply, rdm_core::status::StatusReply::ConfigUpdated { node_id: n, incarnation: i, config, .. } if *n == node_id && *i == incarnation && *config == told), "the fabric-primary sent it down and the node applied it, the config as written carried back: {reply:?}");
    let read = |views: Vec<rdm_client::NodeView>| views.into_iter().find(|v| v.location() == subject_name).unwrap_or_else(|| panic!("{subject_name:?} is answered"));
    let at_once = read(estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", [other_mesh])).await.expect("node.get [mesh] at the fabric-primary"));
    assert_eq!(at_once.config, told, "the fabric-primary answers the config at once, from the node's own record");
    // The fabric-primary's process is killed below: its evidence is read once its span reached it
    // (the exporter writes in batches), so the kill never decides what the cell can read.
    let subject_for = subject_name.clone();
    let fp_for = fp_id.to_string();
    wait_for("the fabric-primary's node.config.update span in its evidence", Duration::from_secs(10), || {
        let spans = estate.spans();
        let found = named(&spans, "rdm.node_admin.node.update.via-fabric-primary").into_iter().any(|sp| attr(sp, "receiver_node_id") == fp_for && rdm_test::estate::names_location(sp, "subject", &subject_for));
        async move { found.then_some(()) }
    })
    .await;

    // The hand-over: the fabric-primary is stopped and killed; the surviving admin of its mesh becomes the fabric-primary.
    estate.enter(&successor).await;
    let mut stop = estate.nodes_obj().await.stop(&old).await.expect("node.stop of the fabric-primary");
    // A stop is a node command, never a Build (Q342): its stream ends with the stop's own completion.
    let frames = Estate::frames_of(&mut stop).await;
    assert_eq!(frames.last(), Some(&rdm_client::Frame::Complete), "the stop completes: {frames:?}");
    kill(&mut estate, &old).await;
    estate
        .until_view("the surviving admin of the fabric-primary's mesh is fabric-primary", Duration::from_secs(90), |views| {
            let ns = Estate::values(views);
            (fabric_primary(&ns).as_ref() == Some(&successor)).then_some(())
        })
        .await;
    let after = read(estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", [other_mesh])).await.expect("node.get [mesh] at the new fabric-primary"));
    estate.stop().await;
    assert_eq!(after.config, told, "the new fabric-primary answers the config from the node's own record after the hand-over");

    let spans: Vec<Value> = estate.spans();
    // The receiving fabric-primary is named by its node id on this span.
    let sent: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-fabric-primary").into_iter().filter(|sp| attr(sp, "receiver_node_id") == fp_id.to_string() && rdm_test::estate::names_location(sp, "subject", &subject_name)).collect();
    assert_eq!(sent.len(), 1, "one node.config.update at the fabric-primary: {sent:?}");
    // The config is on the node's own record, read at the subject node id (query.md); no atlas
    // copy of it is written.
    assert_eq!(attr(sent[0], "outcome"), "config-updated", "{:?}", sent[0]);
    let scoped: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-atlas-scope").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &successor)).collect();
    assert!(scoped.iter().any(|sp| attr(sp, "taken").split(',').any(|m| m == other_mesh)), "the successor took the other mesh into its atlas's scope as it became the fabric-primary: {scoped:?}");
    println!("trace {}: rdm.node_admin.node.update.via-fabric-primary node={old:?} subject={subject_name:?} outcome=config-updated route={}", s(&sent[0]["trace_id"]), attr(sent[0], "route"));
}

/// CONTRACT (lifecycles/node-primary-update.md step 4: the successor's next `Digest` carries the change,
/// its normal announcement and the only one): the primary of the mesh that is not the fabric-primary's
/// dies; its mesh's other admin becomes its mesh-primary by the election, and an admin of the OTHER
/// mesh names it in its view (the change reached the backbone).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_change_reaches_the_other_mesh_over_the_backbone() {
    let (mut estate, nodes) = fabric("a_mesh_primary_change_reaches_the_other_mesh_over_the_backbone").await;
    let fp = fabric_primary(&nodes).expect("one fabric primary");
    let mesh = if mesh_of(&fp) == "mesh1" { "mesh2" } else { "mesh1" };
    let old = nodes.iter().find(|n| rdm_test::estate::mesh_of(&n) == mesh && n["type"] == "node-admin" && n["is_primary"] == true).map(|n| location(&n)).unwrap();
    let heir = sibling(&old);
    let heir_id = s(&node(&nodes, &heir)["node_id"]);
    kill(&mut estate, &old).await;
    estate.until_view("the surviving admin is its mesh's primary", Duration::from_secs(90), |views| views.iter().any(|n| n.location() == heir && n.is_primary).then_some(())).await;
    let watcher = nodes.iter().find(|n| n["type"] == "node-admin" && rdm_test::estate::mesh_of(&n) != mesh).map(|n| location(&n)).expect("an admin of the other mesh");
    let heard = estate
        .until_view_at(&watcher, &format!("{watcher:?} names {heir:?} {mesh}'s primary"), Duration::from_secs(60), |views| {
            let ns = Estate::values(views);
            ns.iter().any(|n| location(n) == heir && n["node_id"] == heir_id.as_str() && n["is_primary"] == true).then_some(ns)
        })
        .await;
    estate.stop().await;
    let primaries: Vec<PathName> = heard.iter().filter(|n| rdm_test::estate::mesh_of(&n) == mesh && n["type"] == "node-admin" && n["is_primary"] == true).map(|n| location(&n)).collect();
    assert_eq!(primaries, vec![heir.clone()], "{watcher:?} names one {mesh} primary, {heir:?}: {heard:#?}");
}

/// CONTRACT: docs/architecture/lifecycles/fabric-recovery.md step 2
/// "*Outside its mesh*: a mesh-primary hears nothing of that mesh once its mesh topology stops, so it runs
/// [node investigation](node-investigation.md) on the fabric-primary's birth."
/// CONTRACT: docs/architecture/lifecycles/node-recovery.md step 2
/// "For an unheard mesh-primary the fabric-primary runs it, without the half-floor wait, after the
/// election has named the new mesh-primary"
/// CONTRACT (Q335 item 4; fabric-recovery.md step 2, "When the mesh has no node-admin left"; Q153: a
/// runtime the executor can inspect running but cannot reach is unreachable from it, not lost): every UDP
/// path between every node of the fabric-primary's mesh and every node of the other mesh is cut while all
/// processes run, past the staleness floor. Each side investigates the other's primary and finds it
/// unreached; each finds the other mesh's second node-admin running, so that mesh has a node-admin left:
/// the fabric-primary executes no recovery of mesh2's primary and mesh2's primary runs no fabric recovery
/// from the backbone. Nothing is terminated, deleted or re-created. Healed, both sides name the same
/// births ready at every path, and the node at the fabric-primary's path.name fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_cut_from_the_backbone_with_a_node_admin_left_on_each_side_is_recovered_by_neither_side_and_converges_unchanged() {
    let (mut estate, nodes) = fabric("a_mesh_cut_from_the_backbone_with_a_node_admin_left_on_each_side_is_recovered_by_neither_side_and_converges_unchanged").await;
    let fp = fabric_primary(&nodes).expect("one fabric primary");
    let fp_node = node(&nodes, &fp).clone();
    let (mine, theirs): (Vec<PathName>, Vec<PathName>) = {
        let side = |incumbent: bool| nodes.iter().filter(|n| (rdm_test::estate::mesh_of(&n) == rdm_test::estate::mesh_of(&fp_node)) == incumbent).map(|n| location(&n)).collect::<Vec<_>>();
        (side(true), side(false))
    };
    let other_primary_row = nodes.iter().find(|n| n["type"] == "node-admin" && rdm_test::estate::mesh_of(&n) != rdm_test::estate::mesh_of(&fp_node) && n["is_primary"] == true).cloned().expect("the other mesh's primary");
    let (other_primary, other_primary_id) = (location(&other_primary_row), s(&other_primary_row["node_id"]));
    let floor = rdm_transport::topology::staleness_floor();
    let fp_id = s(&fp_node["node_id"]);
    let cut = match Partition::start(&udp_ports(&nodes, &mine), &udp_ports(&nodes, &theirs)) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot cut UDP between node-admins: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let cut_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    // Past the floor the fabric-primary investigates mesh2's primary and finds mesh2's second node-admin
    // left: it executes nothing.
    let left = wait_for("the fabric-primary finds a node-admin of mesh2 left and executes nothing", floor * 4 + Duration::from_secs(30), || {
        let spans = estate.spans();
        let found = named(&spans, "rdm.node_admin.node.resolve.via-node-admin-left").into_iter().find(|sp| at_ns(sp) >= cut_ns && attr(sp, "lost_node_id") == other_primary_id && attr(sp, "observer") == fp_id).cloned();
        async move { found }
    })
    .await;
    // A floor more under the cut: anything executed across it would have shown by now.
    tokio::time::sleep(floor).await;
    drop(cut);
    // Both pre-cut admin paths are asked on every pass, until each answers with a ready birth at every
    // pre-cut path.name and names the node at fp's path.name fabric-primary: an answer teaches the estate
    // the births re-created during the cut, which it dials from then on.
    let everyone: Vec<PathName> = nodes.iter().map(|n| location(&n)).collect();
    let deadline = tokio::time::Instant::now() + floor * 12 + Duration::from_secs(120);
    loop {
        let mut healed = 0;
        let mut last = Vec::new();
        for at in [&fp, &other_primary] {
            match estate.node_views_at(at).await {
                Some(v) => {
                    let ns = Estate::values(&v);
                    let all = everyone.iter().all(|name| ns.iter().any(|n| location(n) == *name && n["state"] == "ready"));
                    if all && fabric_primary(&ns).as_ref() == Some(&fp) {
                        healed += 1;
                    }
                    last.push(format!("{at:?}: fabric-primary {:?}, ready-at-every-path {all}", fabric_primary(&ns)));
                }
                None => last.push(format!("{at:?}: no answer")),
            }
        }
        if healed == 2 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "both pre-cut admins name the node at {fp:?} fabric-primary after the heal: {last:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    estate.stop().await;
    let spans = estate.spans();
    // Neither side recovered the other across the cut: no fabric recovery run, no node recovery of either
    // primary, nothing terminated.
    let ran: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| at_ns(sp) >= cut_ns).collect();
    assert!(ran.is_empty(), "no fabric recovery ran: the fabric-primary's mesh had a node-admin left: {ran:#?}");
    let recovered: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| at_ns(sp) >= cut_ns && (attr(sp, "node_id") == other_primary_id || attr(sp, "node_id") == fp_id)).collect();
    assert!(recovered.is_empty(), "no recovery of either primary was opened: {recovered:#?}");
    let terminated: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| at_ns(sp) >= cut_ns && attr(sp, "step") == "TerminateRuntime").collect();
    assert!(terminated.is_empty(), "nothing was terminated: {terminated:#?}");
    let recomputes = named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute");
    assert!(!recomputes.iter().any(|sp| at_ns(sp) >= cut_ns && rdm_test::estate::names_location(sp, "winner", &other_primary)), "{other_primary:?} was never elected fabric-primary");
    estate.record_trace_url(&s(&left["trace_id"]));
}

fn at_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

