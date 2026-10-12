//! i143.e12.s53 functional: the fabric-primary publishes a deleted mesh's aggregate from its last
//! admin's stop on (lifecycles/mesh-delete.md step 7: "A separate node.delete deletes the
//! mesh-primary. The fabric-primary is its executor, proves its exit and, as acting primary of a mesh
//! that has no primary, writes its row deleted; from that admin's stop on it publishes the deleted
//! mesh's aggregate"; gossip.md "Mesh delete": "it publishes the deleted mesh's own Members aggregate
//! on the backbone, publisher itself, `digests` empty, the stopped admin in `in_flight` as NodeStopped
//! on the ordinary anti-entropy cadence, until a start hands publishing back to the admin or a delete
//! moves the birth to `deleted` as NodeDeleted"). Only for a mesh its view holds deleted: a live mesh
//! with no primary goes unheard (R-RX4, mesh-investigation.md), and a mesh created again under the
//! deleted one's name is a new mesh with its own primary.

use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::{learn_addresses, Backbone, Frame, Membership, StatusEvent};
use rafka_mesh_transport::snapshot::SourceSnapshot;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const HELD: &str = "a_peer_holds_a_deleted_meshs_last_admin_stopped_then_deleted_from_the_fabric_primarys_aggregate";
const LIVE: &str = "the_fabric_primary_executing_a_live_primaryless_meshs_stop_and_delete_publishes_no_aggregate_of_it";
const AGAIN: &str = "a_mesh_created_again_under_the_deleted_meshs_name_ends_the_deleted_meshs_aggregate";

fn evidence_dir(cell: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-s53").join(cell)
}

fn collect_spans(dir: &std::path::Path) -> Vec<Value> {
    let mine = format!(".{}-", std::process::id());
    let mut spans = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.file_name().is_some_and(|n| n.to_string_lossy().ends_with(".spans.jsonl") && n.to_string_lossy().contains(&mine)) {
            for line in std::fs::read_to_string(&p).unwrap().lines() {
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    spans.push(v);
                }
            }
        }
    }
    spans
}

fn attr_u(sp: &Value, k: &str) -> u64 {
    sp["attributes"][k].as_u64().or_else(|| sp["attributes"][k].as_str().and_then(|v| v.parse().ok())).unwrap_or(u64::MAX)
}

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn digest(name: &str, port: u16) -> MeshDigest {
    MeshDigest {
        fabric_id: fabric(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(format!("key-{name}")),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::ReadyForTraffic,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 1_000,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        config: Default::default(),
        load: None,
        gossip: None,
    }
}

fn op(kind: &str, d: &MeshDigest) -> LifecycleOp {
    LifecycleOp {
        build_id: "b-s53".into(),
        attempt: 1,
        operation: format!("{kind}:{}", d.node.name),
        node_id: d.node.node_id.clone(),
        incarnation: d.node.incarnation.clone(),
        name: d.node.name.clone(),
        event_at_rafka_ms: 2_000,
    }
}

struct Node {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, _router: router, gossip }
}

fn addr_of(ep: &iroh::Endpoint) -> iroh::EndpointAddr {
    let a = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    iroh::EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.port())))
}

fn view(meshes: &[(&str, bool)]) -> BTreeMap<String, bool> {
    meshes.iter().map(|(m, deleted)| (m.to_string(), *deleted)).collect()
}

fn keys(ops: &[LifecycleOp]) -> Vec<(String, u32, String)> {
    ops.iter().map(LifecycleOp::key).collect()
}

/// The source of `mesh` a peer mesh primary holds and forwards into its own mesh.
fn held_of(m: &Membership, own: &MeshDigest, mesh: &str) -> Option<SourceSnapshot> {
    m.held_topology(own).into_iter().find(|s| s.mesh == mesh)
}

/// The fabric-primary's anti-entropy round, as the node-admin runs it, until `done` holds at the
/// peer; every round publishes the deleted mesh.
async fn rounds_until(fp_bb: &Backbone, fp_m: &Membership, what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        fp_bb.publish(fp_m, fp_bb.own_members(fp_m)).await;
        assert_eq!(fp_bb.publish_deleted_meshes(fp_m).await, vec!["mesh3".to_string()], "every round publishes the deleted mesh3");
        if done() {
            return;
        }
        assert!(Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// CONTRACT: mesh3's last admin, mesh3.admin.1, still its primary after mesh.deleted, published
/// mesh3's aggregate with itself the only member, and the fabric-primary mesh1.admin.1 and a peer
/// mesh primary mesh4.admin.1 hold it. The fabric-primary's view holds mesh3 deleted. The admin
/// stops publishing, and the fabric-primary, as executor, stops it (NodeStopped from the matching
/// stopped reply) and then deletes it (NodeDeleted from the exit proof). From the stop on it
/// publishes mesh3's own aggregate every round: publisher itself, no digests, the stop in
/// `in_flight`, stamped with that aggregate's version; the peer holds mesh3 from it with
/// mesh3.admin.1 parked, and a later round of the same version ends no stop. After the delete the
/// aggregate carries the birth in `deleted` and the peer holds it departed, at that version, round
/// after round. Must NOT happen: the peer holding nothing of mesh3 after the admin's own last
/// aggregate (main after the R-RX4 cut, stem trace 8ffa0cdd41888f6cd5054095b8a2133d).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_holds_a_deleted_meshs_last_admin_stopped_then_deleted_from_the_fabric_primarys_aggregate() {
    if crate::own_process::delegated(module_path!(), HELD) {
        return;
    }
    let dir = evidence_dir(HELD);
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-deleted-mesh-aggregate");

    let (n3, fp_n, peer_n) = (node().await, node().await, node().await);
    let seed = addr_of(&fp_n.ep);
    for n in [&n3, &peer_n] {
        learn_addresses(&n.ep, &[seed.clone()]).unwrap();
    }
    let fab = fabric();
    let clock = rafka_mesh_transport::clock::os_clock;
    let admin3 = digest("mesh3.admin.1", 43_001);
    let fp_own = digest("mesh1.admin.1", 41_001);
    let peer_own = digest("mesh4.admin.1", 44_001);
    let m3 = Membership::join(&n3.gossip, &n3.ep, &fab, "mesh3", &MeshId::mint(), "mesh3.admin.1", clock(), vec![]).await.unwrap();
    let fp_m = Membership::join(&fp_n.gossip, &fp_n.ep, &fab, "mesh1", &MeshId::mint(), "mesh1.admin.1", clock(), vec![]).await.unwrap();
    let peer_m = Membership::join(&peer_n.gossip, &peer_n.ep, &fab, "mesh4", &MeshId::mint(), "mesh4.admin.1", clock(), vec![]).await.unwrap();
    let bb3 = Backbone::join(&n3.gossip, &n3.ep, &m3, "mesh3", "mesh3.admin.1", admin3.node.incarnation.clone(), vec![seed.clone()]).await.unwrap();
    let fp_bb = Backbone::join(&fp_n.gossip, &fp_n.ep, &fp_m, "mesh1", "mesh1.admin.1", fp_own.node.incarnation.clone(), vec![]).await.unwrap();
    let peer_bb = Backbone::join(&peer_n.gossip, &peer_n.ep, &peer_m, "mesh4", "mesh4.admin.1", peer_own.node.incarnation.clone(), vec![seed.clone()]).await.unwrap();
    for bb in [&bb3, &fp_bb, &peer_bb] {
        bb.set_mesh_primary(true);
    }

    // mesh3's last admin publishes itself alone until the fabric-primary and the peer hold mesh3.
    assert!(m3.book.record(admin3.clone()));
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        bb3.publish(&m3, bb3.own_members(&m3)).await;
        if fp_bb.held_source("mesh3").is_some() && peer_bb.held_source("mesh3").is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "the backbone never carried mesh3's aggregate to the fabric-primary and the peer");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(peer_bb.held_source("mesh3").unwrap().0.node, "mesh3.admin.1", "the peer holds mesh3 from its last admin");

    // The last admin stops: it publishes nothing more. The fabric-primary's view holds mesh3 deleted.
    drop(bb3);
    n3.ep.close().await;
    fp_bb.set_fabric_primary(true);
    fp_bb.set_view_meshes(view(&[("mesh1", false), ("mesh3", true), ("mesh4", false)]));

    // The stop, executed by the fabric-primary from the matching stopped reply.
    let stop = op("stop-node", &admin3);
    let (publisher, stamped) = fp_bb.publish_status_event(StatusEvent::Stopped, &stop).await;
    assert_eq!(publisher.node, "mesh1.admin.1");
    let fp_publisher = format!("mesh1.admin.1@{}", fp_own.node.incarnation.0);
    rounds_until(&fp_bb, &fp_m, "the peer never held mesh3 from the fabric-primary with the stopped admin in in_flight", || {
        peer_bb.held_source("mesh3").is_some_and(|(p, _)| p.node == "mesh1.admin.1")
    })
    .await;
    let at_stop = held_of(&peer_m, &peer_own, "mesh3").expect("the peer holds mesh3");
    assert!(at_stop.digests.is_empty(), "the deleted mesh's aggregate has no digests: {at_stop:?}");
    assert_eq!(keys(&at_stop.in_flight), keys(std::slice::from_ref(&stop)), "the stopped admin is in in_flight: {at_stop:?}");
    assert!(at_stop.departed.is_empty(), "{at_stop:?}");
    assert_eq!(at_stop.topology_version, stamped, "the NodeStopped is stamped with the version of the deleted mesh's aggregate that carries it");
    let parked = |m: &Membership| m.book.parked().iter().any(|d| d.node.node_id == admin3.node.node_id);
    assert!(parked(&peer_m), "the peer holds mesh3.admin.1 parked");
    // A later round of the same version ends no stop.
    for _ in 0..3 {
        assert_eq!(fp_bb.publish_deleted_meshes(&fp_m).await, vec!["mesh3".to_string()]);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(peer_bb.held_source("mesh3").map(|(_, v)| v), Some(stamped), "a quiet round republishes the same version");
    assert!(parked(&peer_m), "a republish of the same version ends no stop");

    // The delete: the provider's exit proof, NodeDeleted on the executor's own mesh channel, and the
    // departure taken by its own view, as the node-admin runs it.
    let deleted = op("retire-node", &admin3);
    let of_deleted_mesh = fp_bb.publishes_deleted_mesh_of(&deleted);
    assert!(of_deleted_mesh, "the fabric-primary publishes the birth it stopped");
    fp_m.publish_lifecycle(&Frame::NodeDeleted { op: deleted.clone(), forwarded_by: None }).await.unwrap();
    fp_bb.executed_delete(&deleted, of_deleted_mesh);
    rounds_until(&fp_bb, &fp_m, "the peer never held mesh3's last admin deleted from the fabric-primary's aggregate", || {
        held_of(&peer_m, &peer_own, "mesh3").is_some_and(|s| !s.departed.is_empty())
    })
    .await;
    let at_delete = held_of(&peer_m, &peer_own, "mesh3").expect("the peer holds mesh3");
    assert!(at_delete.digests.is_empty() && at_delete.in_flight.is_empty(), "{at_delete:?}");
    assert_eq!(keys(&at_delete.departed), keys(std::slice::from_ref(&deleted)), "the delete moves the birth to deleted: {at_delete:?}");
    assert!(peer_m.book.is_departed(admin3.node.node_id.as_str()), "the peer holds mesh3.admin.1 departed");
    // Held round after round, at that version.
    for _ in 0..3 {
        assert_eq!(fp_bb.publish_deleted_meshes(&fp_m).await, vec!["mesh3".to_string()]);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let held_at = peer_bb.held_source("mesh3").expect("the peer still holds mesh3");
    assert_eq!((held_at.0.node.as_str(), held_at.1), ("mesh1.admin.1", at_delete.topology_version), "the peer holds the deleted mesh at that version, round after round");

    fp_bb.set_fabric_primary(false);
    for bb in [fp_bb, peer_bb] {
        drop(bb);
    }
    for n in [&fp_n, &peer_n] {
        n.ep.close().await;
    }
    drop(telemetry);
    let spans = collect_spans(&dir);
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let event = spans
        .iter()
        .find(|s| s["name"] == "rdm.mesh.membership.update.via-status-event" && s["attributes"]["subject"] == "mesh3.admin.1")
        .unwrap_or_else(|| panic!("the stop the fabric-primary executed is spanned"));
    assert_eq!(event["attributes"]["role"], "deleted-mesh-publisher", "{event}");
    assert_eq!(attr_u(event, "topology_version"), stamped, "{event}");
    let published: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.backbone.update.via-deleted-mesh" && s["attributes"]["mesh"] == "mesh3").collect();
    let stopped = published.iter().find(|s| attr_u(s, "digests") == 0 && attr_u(s, "in_flight") == 1 && attr_u(s, "departed") == 0).unwrap_or_else(|| panic!("the stopped aggregate is spanned: {published:#?}"));
    let departed = published.iter().find(|s| attr_u(s, "digests") == 0 && attr_u(s, "in_flight") == 0 && attr_u(s, "departed") == 1).unwrap_or_else(|| panic!("the deleted aggregate is spanned: {published:#?}"));
    assert_eq!(published.len(), 2, "one span per version: the stop, then the delete: {published:#?}");
    let installed: Vec<&Value> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.mesh.membership.update.via-snapshot-installed" && s["attributes"]["node"] == "mesh4.admin.1" && s["attributes"]["mesh"] == "mesh3" && s["attributes"]["publisher"] == fp_publisher.as_str())
        .collect();
    let held_stop = installed.iter().find(|s| attr_u(s, "members") == 0 && attr_u(s, "in_flight") == 1).unwrap_or_else(|| panic!("the peer installed the stopped aggregate: {installed:#?}"));
    let held_delete = installed.iter().find(|s| attr_u(s, "members") == 0 && attr_u(s, "departed") == 1).unwrap_or_else(|| panic!("the peer installed the deleted aggregate: {installed:#?}"));
    let parked_span = spans
        .iter()
        .find(|s| s["name"] == "rdm.mesh.membership.update.via-member-state" && s["attributes"]["node"] == "mesh4.admin.1" && s["attributes"]["member"] == "mesh3.admin.1" && (s["attributes"]["parked"] == true || s["attributes"]["parked"] == "true"))
        .unwrap_or_else(|| panic!("the peer names mesh3.admin.1 parked"));
    println!(
        "{HELD}: status event trace {} (v{stamped}); published stopped trace {} deleted trace {}; peer installed stopped trace {} deleted trace {}; parked trace {}",
        event["trace_id"], stopped["trace_id"], departed["trace_id"], held_stop["trace_id"], held_delete["trace_id"], parked_span["trace_id"]
    );
}

/// CONTRACT (R-RX4: a live mesh with no primary goes unheard; mesh-investigation.md: "The
/// fabric-primary never publishes a mesh"): mesh2's primary, mesh2.admin.1, published mesh2 (itself
/// and mesh2.rpc.1), and the fabric-primary and a peer hold it. mesh2's primary is lost; the
/// fabric-primary's view names mesh2 live. As executor it stops mesh2.rpc.1 and deletes
/// mesh2.admin.1 through the same entry points the node-admin calls. Its rounds publish no
/// aggregate of mesh2, the stop is stamped by the executor role, and the peer holds mesh2 from its
/// own primary only. Must NOT happen: the fabric-primary publishing a live mesh because it executed
/// its stop or delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fabric_primary_executing_a_live_primaryless_meshs_stop_and_delete_publishes_no_aggregate_of_it() {
    if crate::own_process::delegated(module_path!(), LIVE) {
        return;
    }
    let dir = evidence_dir(LIVE);
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-live-mesh-no-aggregate");

    let (n2, fp_n, peer_n) = (node().await, node().await, node().await);
    let seed = addr_of(&fp_n.ep);
    for n in [&n2, &peer_n] {
        learn_addresses(&n.ep, &[seed.clone()]).unwrap();
    }
    let fab = fabric();
    let clock = rafka_mesh_transport::clock::os_clock;
    let (admin2, rpc) = (digest("mesh2.admin.1", 42_001), digest("mesh2.rpc.1", 42_101));
    let fp_own = digest("mesh1.admin.1", 41_001);
    let m2 = Membership::join(&n2.gossip, &n2.ep, &fab, "mesh2", &MeshId::mint(), "mesh2.admin.1", clock(), vec![]).await.unwrap();
    let fp_m = Membership::join(&fp_n.gossip, &fp_n.ep, &fab, "mesh1", &MeshId::mint(), "mesh1.admin.1", clock(), vec![]).await.unwrap();
    let peer_m = Membership::join(&peer_n.gossip, &peer_n.ep, &fab, "mesh4", &MeshId::mint(), "mesh4.admin.1", clock(), vec![]).await.unwrap();
    let bb2 = Backbone::join(&n2.gossip, &n2.ep, &m2, "mesh2", "mesh2.admin.1", admin2.node.incarnation.clone(), vec![seed.clone()]).await.unwrap();
    let fp_bb = Backbone::join(&fp_n.gossip, &fp_n.ep, &fp_m, "mesh1", "mesh1.admin.1", fp_own.node.incarnation.clone(), vec![]).await.unwrap();
    let peer_bb = Backbone::join(&peer_n.gossip, &peer_n.ep, &peer_m, "mesh4", "mesh4.admin.1", IncarnationId::mint(), vec![seed.clone()]).await.unwrap();
    for bb in [&bb2, &fp_bb, &peer_bb] {
        bb.set_mesh_primary(true);
    }
    for d in [&admin2, &rpc] {
        assert!(m2.book.record(d.clone()));
    }
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        bb2.publish(&m2, bb2.own_members(&m2)).await;
        if fp_bb.held_source("mesh2").is_some() && peer_bb.held_source("mesh2").is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "the backbone never carried mesh2's aggregate to the fabric-primary and the peer");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // mesh2's primary is lost. The fabric-primary's view names mesh2 live, with no primary.
    drop(bb2);
    n2.ep.close().await;
    fp_bb.set_fabric_primary(true);
    fp_bb.set_view_meshes(view(&[("mesh1", false), ("mesh2", false), ("mesh4", false)]));
    let (_, stamped) = fp_bb.publish_status_event(StatusEvent::Stopped, &op("stop-node", &rpc)).await;
    let deleted = op("retire-node", &admin2);
    let of_deleted_mesh = fp_bb.publishes_deleted_mesh_of(&deleted);
    assert!(!of_deleted_mesh, "a live mesh is not deleted");
    fp_m.publish_lifecycle(&Frame::NodeDeleted { op: deleted.clone(), forwarded_by: None }).await.unwrap();
    fp_bb.executed_delete(&deleted, of_deleted_mesh);
    for _ in 0..5 {
        fp_bb.publish(&fp_m, fp_bb.own_members(&fp_m)).await;
        assert!(fp_bb.publish_deleted_meshes(&fp_m).await.is_empty(), "the fabric-primary publishes no aggregate of the live mesh2");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(peer_bb.held_source("mesh2").map(|(p, _)| p.node), Some("mesh2.admin.1".to_string()), "the peer holds mesh2 from its own primary only");

    fp_bb.set_fabric_primary(false);
    for bb in [fp_bb, peer_bb] {
        drop(bb);
    }
    for n in [&fp_n, &peer_n] {
        n.ep.close().await;
    }
    drop(telemetry);
    let spans = collect_spans(&dir);
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let event = spans.iter().find(|s| s["name"] == "rdm.mesh.membership.update.via-status-event" && s["attributes"]["subject"] == "mesh2.rpc.1").expect("the stop is spanned");
    assert_eq!(event["attributes"]["role"], "executor", "a live mesh's stop is stamped by the executor role: {event}");
    assert_eq!(attr_u(event, "topology_version"), stamped);
    for name in ["rdm.mesh.backbone.update.via-deleted-mesh", "rdm.mesh.backbone.reject.via-mesh-live"] {
        let found: Vec<&Value> = spans.iter().filter(|s| s["name"] == name).collect();
        assert!(found.is_empty(), "{name}: nothing of the live mesh2 is published: {found:#?}");
    }
    let from_fp: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-snapshot-installed" && s["attributes"]["mesh"] == "mesh2" && s["attributes"]["publisher"].as_str().is_some_and(|p| p.starts_with("mesh1.admin.1@"))).collect();
    assert!(from_fp.is_empty(), "no node installed a mesh2 aggregate from the fabric-primary: {from_fp:#?}");
    println!("{LIVE}: executor status event trace {} (v{stamped}); no aggregate of mesh2 published by the fabric-primary", event["trace_id"]);
}

/// CONTRACT (lifecycles/mesh-delete.md "Identity": "a mesh created again under the same name claims
/// its names over the deleted mesh and node rows"; one mesh, one publisher on the backbone, gossip.md
/// §3.1 `Members`): the fabric-primary publishes deleted mesh3's aggregate from its last admin's
/// stop. Then its view names mesh3 live: a mesh created again under that name, with its own primary.
/// The deleted mesh's aggregate ends there, by name, once; and it does not return when the view
/// names no mesh3 again. Must NOT happen: the fabric-primary publishing a second `mesh3` aggregate
/// beside the new mesh's primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_created_again_under_the_deleted_meshs_name_ends_the_deleted_meshs_aggregate() {
    if crate::own_process::delegated(module_path!(), AGAIN) {
        return;
    }
    let dir = evidence_dir(AGAIN);
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-deleted-mesh-created-again");

    let fp_n = node().await;
    let fab = fabric();
    let fp_own = digest("mesh1.admin.1", 41_001);
    let fp_m = Membership::join(&fp_n.gossip, &fp_n.ep, &fab, "mesh1", &MeshId::mint(), "mesh1.admin.1", rafka_mesh_transport::clock::os_clock(), vec![]).await.unwrap();
    let fp_bb = Backbone::join(&fp_n.gossip, &fp_n.ep, &fp_m, "mesh1", "mesh1.admin.1", fp_own.node.incarnation.clone(), vec![]).await.unwrap();
    fp_bb.set_mesh_primary(true);
    fp_bb.set_fabric_primary(true);
    let admin3 = digest("mesh3.admin.1", 43_001);
    assert!(fp_m.book.record(admin3.clone()));
    fp_bb.set_view_meshes(view(&[("mesh1", false), ("mesh3", true)]));
    fp_bb.publish_status_event(StatusEvent::Stopped, &op("stop-node", &admin3)).await;
    assert_eq!(fp_bb.publish_deleted_meshes(&fp_m).await, vec!["mesh3".to_string()], "the deleted mesh3 is published from its last admin's stop");

    // mesh3 is created again: the view names it live.
    fp_bb.set_view_meshes(view(&[("mesh1", false), ("mesh3", false)]));
    assert!(fp_bb.publish_deleted_meshes(&fp_m).await.is_empty(), "a live mesh3 ends the deleted mesh's aggregate");
    assert!(fp_bb.publish_deleted_meshes(&fp_m).await.is_empty());
    fp_bb.set_view_meshes(view(&[("mesh1", false)]));
    assert!(fp_bb.publish_deleted_meshes(&fp_m).await.is_empty(), "the deleted mesh's aggregate does not return");

    fp_bb.set_fabric_primary(false);
    drop(fp_bb);
    fp_n.ep.close().await;
    drop(telemetry);
    let spans = collect_spans(&dir);
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let ended: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.backbone.reject.via-mesh-live" && s["attributes"]["mesh"] == "mesh3").collect();
    assert_eq!(ended.len(), 1, "the end is said once, by name: {ended:#?}");
    assert_eq!(attr_u(ended[0], "in_flight"), 1, "{}", ended[0]);
    println!("{AGAIN}: ended trace {}", ended[0]["trace_id"]);
}
