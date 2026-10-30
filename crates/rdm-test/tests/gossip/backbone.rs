//! i143.e4.s9 process E2E: hierarchical topology (`gossip.md` §3, §5, §6).
//!
//! Each mesh has its own topology channel; only node-admins join the
//! backbone; only a mesh's admin primary publishes its mesh's members there,
//! and only a receiving peer mesh's primary forwards them onto its own mesh
//! channel; the fabric primary alone publishes the fabric's status. Ordinary
//! nodes therefore hold every mesh's nodes without a fabric-wide topic.
//!
//! Cells (s9 acceptance 1-5), from public surfaces and span evidence only:
//! 1. ordinary nodes subscribe to their own mesh channel only, never to the
//!    backbone, yet learn both meshes; every admin joins the backbone;
//! 2. exactly one mesh topology publisher per mesh, its admin primary: a
//!    non-primary admin never publishes;
//! 3. losing a mesh's admin primary moves publication to its successor;
//! 5. ...and that successor forwards the peer mesh again, so the mesh's
//!    ordinary nodes hold the peer mesh once more;
//! 4. losing the fabric primary moves fabric-status publication to the new
//!    one, with never two publishers live at once.
//!
//! And under faults (s9 acceptance 6-8):
//! 6. a lost backbone push (the two primaries cut from each other) is
//!    repaired by gossip through the other admins: the peer mesh is never
//!    lost by the mesh's ordinary nodes, and nothing beside gossip resends;
//! 7. an admin isolated past the silence window refuses a Build submitted through it; while it is cut
//!    every view holds at most one primary per (mesh, node type), and a recovery it or anyone opens
//!    follows node investigation's verdict, which side's births survive not pinned (Q4, Q111,
//!    fabric-recovery.md step 2); after the heal every pre-cut path.name holds one live birth, the same in
//!    every admin's view, and a Build submitted at the fabric-primary then completes;
//! 8. a node stopped and started is ready only once it holds every mesh.
//!
//! The faults drop UDP on loopback (`nft`, as root or `sudo -n`); where
//! the host cannot, cells 6-7 are a named skip, a failure under
//! `RDM_REQUIRE_NETFAULT=1` (CI).

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, PathName};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::netfault::{udp_ports, Partition};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-topology".into(),
        subfeature: "backbone".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// One estate at a time: two MM fabrics on one host halve each one's CPU.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The fabric `fabric` with `mesh1` of two admins and `rpc1` rpc nodes, and `mesh2` of two of each.
fn shape(fabric: &str, rpc1: u32) -> FabricDesired {
    FabricDesired { fabric: fabric.into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, rpc1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] }
}

/// MM with two admins and two rpc nodes per mesh, settled.
async fn mm(estate: &Estate, fabric: &str) -> Vec<Value> {
    estate.reconcile(&shape(fabric, 2)).await;
    let want: BTreeSet<String> = ["mesh1", "mesh2"]
        .iter()
        .flat_map(|m| (1..=2).flat_map(move |i| [format!("{m}.admin.{i}"), format!("{m}.rpc.{i}")]))
        .collect();
    Estate::values(&estate.settles_to_named(&want, Duration::from_secs(30)).await)
}

fn netfault(why: String) -> Option<()> {
    if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() == Ok("1") {
        panic!("RDM_REQUIRE_NETFAULT=1 but this host cannot drop traffic: {why}");
    }
    eprintln!("SKIP fault cells: {why}");
    None
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}


fn attr(sp: &Value, k: &str) -> String {
    s(&sp["attributes"][k])
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_str().and_then(|v| v.parse().ok()).or_else(|| sp["start_unix_nano"].as_u64()).unwrap_or(0)
}

/// `(node, mesh)` -> the role spans, in time order.
fn roles(spans: &[Value], name: &str, key: &str) -> Vec<(u64, String, String, String)> {
    let mut v: Vec<_> = named(spans, name).into_iter().map(|sp| (start(sp), attr(sp, "node"), attr(sp, key), attr(sp, "role"))).collect();
    v.sort();
    v
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// Who holds `role` for `scope` now, by the last start/stop per node. A killed process never
/// says stop, so a path in `dead` drops its events from before its kill; drift recovery may
/// rebirth the same path under a new NodeId, and that birth's events count.
fn role_nodes(events: &[(u64, String, String, String)], scope: &str, dead: &BTreeMap<String, u64>) -> BTreeSet<String> {
    let mut last: BTreeMap<&str, &str> = BTreeMap::new();
    for (at, node, _, role) in events.iter().filter(|e| e.2 == scope) {
        if dead.get(node.as_str()).is_some_and(|killed| at <= killed) {
            continue;
        }
        last.insert(node, role);
    }
    last.into_iter().filter(|(_, r)| *r == "start").map(|(n, _)| n.to_string()).collect()
}

/// The latest learned/silent verdict each ordinary node holds about `mesh`.
fn verdicts(spans: &[Value], node: &str, mesh: &str) -> Option<String> {
    let mut v: Vec<(u64, &str)> = Vec::new();
    for (name, verdict) in [("rdm.mesh.topology.update.via-mesh-learned", "learned"), ("rdm.mesh.topology.update.via-mesh-silent", "silent")] {
        v.extend(named(spans, name).into_iter().filter(|sp| attr(sp, "node") == node && attr(sp, "mesh") == mesh).map(|sp| (start(sp), verdict)));
    }
    v.sort();
    v.last().map(|(_, x)| x.to_string())
}

fn primary(nodes: &[Value], mesh: &str) -> Value {
    nodes.iter().find(|n| n["mesh"] == mesh && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().unwrap_or(Value::Null)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_rides_mesh_channels_and_the_admin_backbone() {
    let _one = SERIAL.lock().await;
    let mut estate = Estate::bootstrap(owner("topology_rides_mesh_channels_and_the_admin_backbone"), "fabric1", "mesh1").await;
    let nodes = mm(&estate, "fabric1").await;
    let rpcs: Vec<String> = nodes.iter().filter(|n| n["kind"] == "rpc_node").map(|n| s(&n["name"])).collect();
    let admins: Vec<String> = nodes.iter().filter(|n| n["kind"] == "node_admin").map(|n| s(&n["name"])).collect();

    // 1. Every ordinary node learns both meshes...
    wait_for("every rpc node holds both meshes", Duration::from_secs(30), || {
        let spans = estate.spans();
        let rpcs = rpcs.clone();
        async move { rpcs.iter().all(|n| ["mesh1", "mesh2"].iter().all(|m| verdicts(&spans, n, m).as_deref() == Some("learned"))).then_some(()) }
    })
    .await;
    // ...through its own mesh channel only; every admin is on the backbone.
    let spans = estate.spans();
    let subs: Vec<(String, String)> =
        named(&spans, "rdm.mesh.topology.update.via-subscribe").into_iter().map(|sp| (attr(sp, "node"), attr(sp, "channel"))).collect();
    for n in &rpcs {
        let mesh = n.split('.').next().unwrap();
        let mine: BTreeSet<&str> = subs.iter().filter(|(x, _)| x == n).map(|(_, c)| c.as_str()).collect();
        assert_eq!(mine, [format!("mesh:{mesh}").as_str()].into_iter().collect(), "{n} joins its own mesh channel only: {mine:?}");
    }
    for n in &admins {
        assert!(subs.iter().any(|(x, c)| x == n && c == "backbone"), "{n} joins the backbone");
    }

    // 2. One mesh topology publisher per mesh, its admin primary.
    let none: BTreeMap<String, u64> = BTreeMap::new();
    for m in ["mesh1", "mesh2"] {
        let p = s(&primary(&nodes, m)["name"]);
        wait_for(&format!("{m}'s primary publishes it, alone"), Duration::from_secs(15), || {
            let spans = estate.spans();
            let (p, none) = (p.clone(), none.clone());
            async move {
                let ev = roles(&spans, "rdm.mesh.backbone.update.via-aggregate-publisher", "mesh");
                (role_nodes(&ev, m, &none) == [p].into_iter().collect()).then_some(())
            }
        })
        .await;
    }
    // An admin that is not its mesh's primary now may have published earlier: under
    // lowest-ready-NodeId elections a mesh's first admin holds the role until a lower NodeId of
    // its cohort is ready, then hands it over. What must hold: it publishes only while it is its
    // mesh's primary in its own view, and none of its publishing intervals is still open.
    let spans = estate.spans();
    let ev = roles(&spans, "rdm.mesh.backbone.update.via-aggregate-publisher", "mesh");
    for n in admins.iter().filter(|n| ["mesh1", "mesh2"].iter().all(|m| s(&primary(&nodes, m)["name"]) != **n)) {
        let mine: Vec<&(u64, String, String, String)> = ev.iter().filter(|e| &e.1 == n).collect();
        assert!(mine.last().is_none_or(|e| e.3 == "stop"), "the non-primary admin {n} has stopped publishing: {mine:?}");
        for e in mine.iter().filter(|e| e.3 == "start") {
            let role = named(&spans, "rdm.mesh.election.resolve.via-mesh-primary")
                .into_iter()
                .filter(|sp| attr(sp, "observer") == *n && start(sp) <= e.0)
                .max_by_key(|sp| start(sp))
                .map(|sp| attr(sp, "winner_path"));
            assert_eq!(role.as_deref(), Some(n.as_str()), "{n} started publishing {} only while it held the mesh-primary role: {mine:?}", e.2);
        }
    }

    // 3 + 5. Lose mesh2's admin primary: its successor publishes mesh2 and
    // forwards mesh1, and mesh2's rpc nodes hold mesh1 again.
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let old = primary(&nodes, "mesh2");
    let (old_name, old_id) = (s(&old["name"]), s(&old["node_id"]));
    estate.kill_node(&old_name).await;
    let dead: BTreeMap<String, u64> = [(old_name.clone(), now_ns())].into_iter().collect();
    // Tracked by NodeId: node recovery may create the killed path again under a new NodeId.
    let successor = estate
        .until_view("mesh2 elects a successor", rdm_transport::topology::staleness_floor() * 2 + rdm_transport::topology::backbone_gossip_interval() * 2 + Duration::from_secs(30), |views| {
            let p = primary(&Estate::values(views), "mesh2");
            (!s(&p["name"]).is_empty() && s(&p["node_id"]) != old_id).then(|| s(&p["name"]))
        })
        .await;
    wait_for("mesh2's successor publishes and forwards", Duration::from_secs(20), || {
        let spans = estate.spans();
        let (successor, dead) = (successor.clone(), dead.clone());
        async move {
            let publ = roles(&spans, "rdm.mesh.backbone.update.via-aggregate-publisher", "mesh");
            let fwd = roles(&spans, "rdm.mesh.backbone.update.via-forwarder", "mesh");
            let one: BTreeSet<String> = [successor].into_iter().collect();
            (role_nodes(&publ, "mesh2", &dead) == one && role_nodes(&fwd, "mesh2", &dead) == one).then_some(())
        }
    })
    .await;
    for n in rpcs.iter().filter(|n| n.starts_with("mesh2.")) {
        let n = n.clone();
        wait_for(&format!("{n} holds mesh1 again"), Duration::from_secs(15), || {
            let spans = estate.spans();
            let n = n.clone();
            async move { (verdicts(&spans, &n, "mesh1").as_deref() == Some("learned")).then_some(()) }
        })
        .await;
    }

    // The killed primary is recovered at its path by the fabric-primary (node-recovery.md step 3, Q111),
    // an attempt that holds the fabric's lane until it completes: step 4 starts once it has.
    estate
        .until_view(&format!("{old_name} is recovered as a new node"), rdm_transport::topology::staleness_floor() * 4 + Duration::from_secs(60), |views| {
            Estate::values(views).iter().any(|n| s(&n["name"]) == old_name && s(&n["node_id"]) != old_id && n["status"] == "ready").then_some(())
        })
        .await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    // 4. Lose the fabric primary: status publication moves, never doubled. The role moved in
    // step 3; the primary is lost only once it alone publishes the fabric's status.
    let fabric_id = estate.fabric_view().await.id.to_string();
    let fp = estate
        .until_view("the fabric primary alone publishes the fabric's status before it is lost", Duration::from_secs(40), |views| {
            let fp = Estate::values(views).into_iter().find(|n| n["is_fabric_primary"] == true)?;
            let ev = roles(&estate.spans(), "rdm.mesh.fabric.update.via-status-publisher", "fabric");
            (role_nodes(&ev, &fabric_id, &dead) == [s(&fp["name"])].into_iter().collect()).then_some(fp)
        })
        .await;
    let (fp_name, fp_id) = (s(&fp["name"]), s(&fp["node_id"]));
    // Survivors from the live view at the kill, never an earlier fabric.get answer: step 3's
    // lost admin may still be advertised there.
    let survivors: Vec<String> = estate
        .node_values()
        .await
        .iter()
        .filter(|n| n["kind"] == "node_admin" && n["status"] == "ready" && s(&n["node_id"]) != fp_id)
        .map(|n| s(&n["name"]))
        .collect();
    assert!(!survivors.is_empty(), "a live admin other than the fabric primary {fp_name}");
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter(&Estate::path(&survivors[0])).await;
    let lost_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let mut stop = estate.nodes_obj().await.stop(&fp_name.parse().unwrap()).await.expect("node.stop of fp_name");
    let accepted = stop.accepted().clone();
    if matches!(Estate::frames_of(&mut stop).await.last(), Some(rdm_client::Frame::FabricPrimaryMoved { .. })) {
        estate.resume(&accepted.build_id, accepted.attempt).await;
    }
    if fp_name == "mesh1.admin.1" {
        estate.kill_bootstrap();
    } else {
        estate.kill_node(&fp_name).await;
    }
    let mut dead = dead;
    dead.insert(fp_name.clone(), now_ns());
    // The winner is read with the publisher, in one poll: until the lost
    // primary's mesh is heard again through its successor's mesh topology, a
    // peer mesh's admin may briefly name itself in its own view (and, gated,
    // publishes nothing). The converged view names the one publisher.
    let next = estate
        .until_view("a new fabric primary alone publishes the fabric's status", rdm_transport::topology::staleness_floor() * 2 + rdm_transport::topology::backbone_gossip_interval() * 2 + Duration::from_secs(30), |views| {
            let n = Estate::values(views);
            // Tracked by NodeId: node recovery may create the lost path again under a new NodeId.
            let next = n.iter().find(|x| x["is_fabric_primary"] == true && s(&x["node_id"]) != fp_id).map(|x| s(&x["name"]))?;
            // Status publication is keyed by the Fabric's id, the backbone's key.
            let ev = roles(&estate.spans(), "rdm.mesh.fabric.update.via-status-publisher", "fabric");
            (role_nodes(&ev, &fabric_id, &dead) == [next.clone()].into_iter().collect()).then_some(next)
        })
        .await;
    // The role moves with the lowest NodeId as meshes grow and lose primaries, and a rebirth of
    // the lost path may hold it on the way: the lost primary was the last to start publishing
    // before the loss, and the last to start after it is the primary the view names.
    let ev = roles(&estate.spans(), "rdm.mesh.fabric.update.via-status-publisher", "fabric");
    let before = ev.iter().filter(|e| e.0 < lost_at && e.3 == "start").max_by_key(|e| e.0).map(|e| e.1.clone());
    let after = ev.iter().filter(|e| e.0 >= lost_at && e.3 == "start").max_by_key(|e| e.0).map(|e| e.1.clone());
    assert_eq!(before.as_deref(), Some(fp_name.as_str()), "the lost primary published before the loss: {ev:?}");
    assert_eq!(after.as_deref(), Some(next.as_str()), "the primary is the last to start publishing after the loss: {ev:?}");

    // The stop follows the fabric-primary role itself.
    estate.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gossip_repairs_a_lost_push_and_an_isolated_admin_refuses_builds_and_heals_to_one_birth_per_path() {
    let _one = SERIAL.lock().await;
    let mut estate = Estate::bootstrap(owner("gossip_repairs_a_lost_push_and_an_isolated_admin_refuses_builds_and_heals_to_one_birth_per_path"), "fabric2", "mesh1").await;
    let nodes = mm(&estate, "fabric2").await;
    let rpcs2: Vec<String> = nodes.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "rpc_node").map(|n| s(&n["name"])).collect();
    wait_for("mesh2's rpc nodes hold mesh1", Duration::from_secs(30), || {
        let spans = estate.spans();
        let rpcs2 = rpcs2.clone();
        async move { rpcs2.iter().all(|n| verdicts(&spans, n, "mesh1").as_deref() == Some("learned")).then_some(()) }
    })
    .await;

    // 8. Stop then start mesh2.rpc.1: it rejoins as the same birth (node-stop.md "Kept"; node-start.md)
    // and is ready again holding both meshes.
    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let before = s(&estate.node_value("mesh2.rpc.1").await["incarnation_id"]);
    let ready_before = named(&estate.spans(), "rdm.mesh.node.update.via-ready").into_iter().filter(|sp| attr(sp, "incarnation_id") == before).count();
    estate.stop_then_start(&"mesh2.rpc.1".parse().unwrap()).await;
    // The Build can complete on another admin: wait for the asked one's view, and for the node's own
    // second ready.
    let restarted = estate
        .until_node("mesh2.rpc.1", "the asked admin's view holds the restarted birth ready", Duration::from_secs(15), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            let ready_now = named(&estate.spans(), "rdm.mesh.node.update.via-ready").into_iter().filter(|sp| attr(sp, "incarnation_id") == before).count();
            (n["status"] == "ready" && s(&n["incarnation_id"]) == before && ready_now > ready_before).then_some(s(&n["incarnation_id"]))
        })
        .await;
    assert_eq!(restarted, before, "a stop then a start keep the birth");
    let spans = estate.spans();
    let ready = named(&spans, "rdm.mesh.node.update.via-ready")
        .into_iter()
        .filter(|sp| attr(sp, "incarnation_id") == restarted)
        .max_by_key(|sp| start(sp))
        .unwrap_or_else(|| panic!("the restarted mesh2.rpc.1 reports ready"))
        .clone();
    assert_eq!(attr(&ready, "meshes"), "2", "ready holding both meshes: {ready}");
    let pulled = named(&spans, "rdm.mesh.entry.update.via-topology-pulled")
        .into_iter()
        .filter(|sp| attr(sp, "node") == "mesh2.rpc.1")
        .map(start)
        .max()
        .expect("the restarted node pulled its entry");
    assert!(start(&ready) >= pulled, "ready only after the entry pull");

    // 6. Cut the two mesh primaries from each other: gossip repairs through
    // the other admins, and mesh2's ordinary nodes never lose mesh1.
    let nodes = estate.node_values().await;
    let (p1, p2) = (s(&primary(&nodes, "mesh1")["name"]), s(&primary(&nodes, "mesh2")["name"]));
    let cut_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let Some(cut) = Partition::start(&udp_ports(&nodes, &[p1.clone()]), &udp_ports(&nodes, &[p2.clone()])).map_err(netfault).ok() else {
        estate.stop().await;
        return;
    };
    tokio::time::sleep(Duration::from_secs(6)).await;
    drop(cut);
    let spans = estate.spans();
    let lost: Vec<String> = named(&spans, "rdm.mesh.topology.update.via-mesh-silent")
        .into_iter()
        .filter(|sp| start(sp) > cut_at && attr(sp, "mesh") == "mesh1" && rpcs2.contains(&attr(sp, "node")))
        .map(|sp| attr(sp, "node"))
        .collect();
    assert!(lost.is_empty(), "with {p1} and {p2} cut apart, mesh2's rpc nodes kept mesh1: {lost:?}");

    // 7. Isolate mesh1's non-primary admin past the silence window and ask it for a Build: it refuses it.
    // Isolated, it cannot tell its own cut from the fabric-primary's loss: it may run fabric recovery, whose
    // node investigation decides (fabric-recovery.md step 2-3, Q111), and which side's births survive is not
    // pinned (Q4). Every view holds at most one primary per (mesh, node type); after the heal every pre-cut
    // path.name holds one live birth, the same in every admin's view, and a Build submitted at the
    // fabric-primary then completes.
    let nodes = estate.node_values().await;
    let lone = nodes.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["is_primary"] != true).cloned().unwrap();
    let lone_name = s(&lone["name"]);
    let others: Vec<String> = nodes.iter().map(|n| s(&n["name"])).filter(|n| *n != lone_name).collect();
    let pre_cut: BTreeSet<String> = nodes.iter().map(|n| s(&n["name"])).collect();
    let admins: Vec<PathName> = nodes.iter().filter(|n| n["kind"] == "node_admin").map(|n| s(&n["name"]).parse().unwrap()).collect();
    let one_primary = |view: &[Value]| -> Result<(), String> {
        let mut seen: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
        for n in view.iter().filter(|n| n["is_primary"] == true && n["status"] == "ready") {
            seen.entry((s(&n["mesh"]), s(&n["kind"]))).or_default().push(s(&n["name"]));
        }
        let fps: Vec<String> = view.iter().filter(|n| n["is_fabric_primary"] == true && n["status"] == "ready").map(|n| s(&n["name"])).collect();
        match seen.iter().find(|(_, v)| v.len() > 1) {
            Some((k, v)) => Err(format!("two primaries of {k:?}: {v:?}")),
            None if fps.len() > 1 => Err(format!("two fabric-primaries: {fps:?}")),
            None => Ok(()),
        }
    };
    let fp_name = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).unwrap_or_else(|| panic!("the pre-cut view names a fabric-primary: {nodes:?}"))["name"]);
    let cut_at = now_ns();
    let isolation = Partition::start(&udp_ports(&nodes, &[lone_name.clone()]), &udp_ports(&nodes, &others)).expect("the host dropped traffic above");
    // The isolated admin's own pruner marks the fabric-primary stale, and its election of mesh1 then names
    // no primary: only from then does its view name no fabric-primary. Another member's stale mark comes
    // first and is not this: a refusal before it names the fabric-primary the admin still holds (build.md:
    // `not-fabric-primary`, naming the fabric-primary in its own view), and the carrier follows it there.
    wait_for(&format!("{lone_name} marks the fabric-primary {fp_name} stale and its view names no primary of mesh1"), rdm_transport::topology::staleness_floor() + Duration::from_secs(20), || {
        let spans = estate.spans();
        let (lone_name, fp_name) = (lone_name.clone(), fp_name.clone());
        async move {
            // The two spans come in either order (the election may read the mark before the stale span closes).
            let marked = named(&spans, "rdm.mesh.topology.update.via-member-stale").iter().any(|sp| attr(sp, "node") == lone_name && attr(sp, "member") == fp_name && start(sp) >= cut_at);
            let elected = named(&spans, "rdm.mesh.election.resolve.via-mesh-primary")
                .into_iter()
                .filter(|sp| attr(sp, "observer") == lone_name && attr(sp, "mesh") == "mesh1" && start(sp) >= cut_at)
                .max_by_key(|sp| start(sp))
                .map(|sp| attr(sp, "winner_path"));
            (marked && elected.is_some_and(|w| w.is_empty())).then_some(())
        }
    })
    .await;
    // build.create at the isolated admin is refused: while the fabric-primary's record names a holder it
    // does not hear, its view names no fabric-primary (Q122, fabric-node-lifecycle-elections.md: "no
    // node-admin is elected"), so it accepts nothing and names nobody.
    // The carrier follows a refusal that names a fabric-primary (its view may still name the one it no
    // longer hears): what is asserted is the isolated admin's own refusal, never where the call ended.
    let lone_path: PathName = lone_name.parse().unwrap();
    let asked_at = now_ns();
    let request = estate.shape_change(&shape("fabric2", 3)).await;
    let _ = estate.carrier_at(&lone_path).await.create(request).await;
    wait_for(&format!("the isolated {lone_name} refuses build.create as not the fabric-primary"), Duration::from_secs(15), || {
        let spans = estate.spans();
        let lone_name = lone_name.clone();
        async move { named(&spans, "rdm.node_admin.build.reject.via-not-fabric-primary").into_iter().any(|sp| attr(sp, "node") == lone_name && start(sp) >= asked_at).then_some(()) }
    })
    .await;
    let accepted_by_lone: Vec<String> = named(&estate.spans(), "rdm.node_admin.build.create.via-node-rpc").into_iter().filter(|sp| attr(sp, "node") == lone_name && start(sp) >= asked_at).map(|sp| attr(sp, "build_id")).collect();
    assert!(accepted_by_lone.is_empty(), "the isolated {lone_name} accepted no Build: {accepted_by_lone:?}");
    // Held past the decision: every view an admin answers holds at most one primary per cohort.
    let until = std::time::Instant::now() + rdm_transport::topology::staleness_floor() * 2;
    while std::time::Instant::now() < until {
        for admin in &admins {
            if let Some(view) = estate.node_views_at(admin).await {
                let view: Vec<Value> = view.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect();
                one_primary(&view).unwrap_or_else(|e| panic!("during the cut, {admin}'s view: {e}"));
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    drop(isolation);
    // The estate was entered at the fabric-primary's first birth, which the isolated admin's fabric
    // recovery may have re-created: it is entered again through a node-admin the cut never touched.
    let entry: PathName = "mesh2.admin.1".parse().unwrap();
    estate.enter(&entry).await;
    // Healed: every pre-cut path.name holds exactly one live Ready birth, the same in every
    // admin's view (a new NodeId where its recovery re-created it). The untouched admin is read first:
    // its view names the birth a re-created admin holds now, which is how that admin is reached.
    let in_order: Vec<PathName> = std::iter::once(entry.clone()).chain(admins.iter().filter(|a| **a != entry).cloned()).collect();
    let converged = wait_for("every admin holds one live birth per pre-cut path, the same in each", Duration::from_secs(180), || async {
        let mut want: Option<BTreeMap<String, String>> = None;
        for admin in &in_order {
            let view: Vec<Value> = estate.node_views_at(admin).await?.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect();
            one_primary(&view).ok()?;
            let mut at = BTreeMap::new();
            for p in &pre_cut {
                let live: Vec<&Value> = view.iter().filter(|n| n["name"] == p.as_str() && n["status"] == "ready").collect();
                if live.len() != 1 {
                    return None;
                }
                at.insert(p.clone(), s(&live[0]["node_id"]));
            }
            if want.as_ref().is_some_and(|w| *w != at) {
                return None;
            }
            want = Some(at);
        }
        want
    })
    .await;
    // A recovery through cut and heal follows node investigation's verdict, never `reached`.
    for sp in named(&estate.spans(), "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| start(sp) >= cut_at) {
        assert_ne!(attr(sp, "verdict"), "reached", "a recovery follows node investigation's verdict, never `reached`: {sp}");
    }
    // fabric-recovery.md (Q111): a lost fabric-primary the investigation reached is commanded back, never
    // re-created: a fabric recovery whose verdict is `reached` submits no re-create in its run.
    let spans = estate.spans();
    for run in named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| attr(sp, "verdict") == "reached") {
        let recreated: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| sp["trace_id"] == run["trace_id"]).collect();
        assert!(recreated.is_empty(), "fabric recovery reached the lost fabric-primary and re-created it anyway: {run} {recreated:#?}");
    }
    // The Build submitted at the fabric-primary the healed fabric names completes.
    let healed = estate.node_values().await;
    let fp_path: PathName = s(&healed.iter().find(|n| n["is_fabric_primary"] == true).unwrap_or_else(|| panic!("the healed fabric names a fabric-primary: {healed:?}"))["name"]).parse().unwrap();
    let at_fp = estate.carrier_at(&fp_path).await;
    let mut grow = at_fp.create(estate.shape_change_at(&at_fp, &shape("fabric2", 3)).await).await.unwrap_or_else(|e| panic!("build.create at the fabric-primary {fp_path} was refused: {e}"));
    Estate::until_complete(&mut grow).await;
    let mut want: BTreeSet<String> = pre_cut.clone();
    want.insert("mesh1.rpc.3".into());
    let after = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(30)).await);
    // Relearned with the topology view after the heal: every birth's current
    // runtime metadata (i143.e4.s16), no Build history replayed.
    for n in &after {
        assert!(n["data_dir"].as_str().is_some_and(|d| !d.is_empty()), "{} carries its data dir after the heal: {n}", n["name"]);
    }
    let _ = converged;
    estate.stop().await;
}
