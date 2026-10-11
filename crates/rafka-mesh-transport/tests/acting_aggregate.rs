//! i143.e12.s41 functional: the fabric-primary as acting primary of a mesh with no primary
//! (gossip.md §3: "for a mesh with no primary, the fabric-primary publishes that mesh's own aggregate
//! as its acting primary"; §3.1 catalog, `Members`: "`digests`: as held"). A living mesh keeps the
//! members the fabric-primary holds of it, so a recovering admin that reads the fabric-primary's
//! topology finds a local node of its own mesh to call (mesh-recovery.md steps 3-4). Only the dead
//! mesh of R-NS1 (gossip.md "Mesh delete": its last admin stopped, "`digests` empty") is published
//! with none: the births the fabric-primary stopped or deleted are listed in `in_flight` and
//! `departed`, never among the digests.

use iroh::protocol::Router;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::{learn_addresses, Backbone, Membership};
use rafka_mesh_transport::snapshot::SourceSnapshot;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;

const CELL: &str = "the_acting_aggregate_carries_the_held_digests_of_a_living_mesh_and_none_of_a_dead_one";

fn evidence_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-s41").join(CELL)
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
        build_id: "b-s41".into(),
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

fn names(s: &SourceSnapshot) -> Vec<String> {
    let mut v: Vec<String> = s.digests.iter().map(|d| d.node.name.to_string()).collect();
    v.sort();
    v
}

/// CONTRACT: mesh2 (its primary mesh2.admin.1 and two rpc members) and mesh3 (only its last admin,
/// mesh3.admin.1, after mesh.deleted) each published their aggregate, and the fabric-primary
/// mesh1.admin.1 and a peer mesh primary mesh4.admin.1 hold both from the backbone. Neither source
/// publishes again. As executor the fabric-primary deleted mesh2.admin.1 (NodeDeleted from its exit
/// proof) and stopped mesh3.admin.1 (NodeStopped from its stopped reply); then, as acting primary,
/// it publishes both meshes. mesh2 is a living mesh with no primary: its aggregate carries the two
/// rpc members it holds, mesh2.admin.1 in `departed` and not among the digests, and that is what the
/// fabric-primary's own topology answer (GetTopology) and the peer's hold of mesh2 serve. mesh3 is
/// R-NS1's dead mesh: no digests, mesh3.admin.1 in `in_flight` as NodeStopped. The acting span
/// names each mesh's digest count. Must NOT happen: a living mesh's held members replaced by an
/// empty aggregate (the recovering admin then installs 0 members and has no local node to call).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_acting_aggregate_carries_the_held_digests_of_a_living_mesh_and_none_of_a_dead_one() {
    if crate::own_process::delegated(module_path!(), CELL) {
        return;
    }
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-acting-aggregate");

    let (n2, n3, fp_n, peer_n) = (node().await, node().await, node().await, node().await);
    let seed = addr_of(&fp_n.ep);
    for n in [&n2, &n3, &peer_n] {
        learn_addresses(&n.ep, &[seed.clone()]).unwrap();
    }
    let fab = fabric();
    let clock = rafka_mesh_transport::clock::os_clock;
    let m2 = Membership::join(&n2.gossip, &n2.ep, &fab, "mesh2", &MeshId::mint(), "mesh2.admin.1", clock(), vec![]).await.unwrap();
    let m3 = Membership::join(&n3.gossip, &n3.ep, &fab, "mesh3", &MeshId::mint(), "mesh3.admin.1", clock(), vec![]).await.unwrap();
    let fp_m = Membership::join(&fp_n.gossip, &fp_n.ep, &fab, "mesh1", &MeshId::mint(), "mesh1.admin.1", clock(), vec![]).await.unwrap();
    let peer_m = Membership::join(&peer_n.gossip, &peer_n.ep, &fab, "mesh4", &MeshId::mint(), "mesh4.admin.1", clock(), vec![]).await.unwrap();
    let bb2 = Backbone::join(&n2.gossip, &n2.ep, &m2, "mesh2", "mesh2.admin.1", IncarnationId::mint(), vec![seed.clone()]).await.unwrap();
    let bb3 = Backbone::join(&n3.gossip, &n3.ep, &m3, "mesh3", "mesh3.admin.1", IncarnationId::mint(), vec![seed.clone()]).await.unwrap();
    let fp_own = digest("mesh1.admin.1", 41_001);
    let fp_bb = Backbone::join(&fp_n.gossip, &fp_n.ep, &fp_m, "mesh1", "mesh1.admin.1", fp_own.node.incarnation.clone(), vec![]).await.unwrap();
    let peer_own = digest("mesh4.admin.1", 44_001);
    let peer_bb = Backbone::join(&peer_n.gossip, &peer_n.ep, &peer_m, "mesh4", "mesh4.admin.1", peer_own.node.incarnation.clone(), vec![seed.clone()]).await.unwrap();
    for bb in [&bb2, &bb3, &fp_bb, &peer_bb] {
        bb.set_mesh_primary(true);
    }

    // mesh2: its primary and two rpc members; mesh3: its last admin alone.
    let admin2 = digest("mesh2.admin.1", 42_001);
    let rpc = [digest("mesh2.rpc.1", 42_101), digest("mesh2.rpc.2", 42_102)];
    for d in std::iter::once(&admin2).chain(rpc.iter()) {
        assert!(m2.book.record(d.clone()));
    }
    let admin3 = digest("mesh3.admin.1", 43_001);
    assert!(m3.book.record(admin3.clone()));

    // Both sources publish until the fabric-primary and the peer hold both meshes.
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    loop {
        bb2.publish(&m2, bb2.own_members(&m2)).await;
        bb3.publish(&m3, bb3.own_members(&m3)).await;
        if ["mesh2", "mesh3"].iter().all(|m| fp_bb.held_source(m).is_some() && peer_bb.held_source(m).is_some()) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the backbone never carried both sources to the fabric-primary and the peer");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let held2 = fp_m.held_topology(&fp_own).into_iter().find(|s| s.mesh == "mesh2").expect("the fabric-primary holds mesh2");
    assert_eq!(names(&held2), vec!["mesh2.admin.1", "mesh2.rpc.1", "mesh2.rpc.2"], "the fabric-primary holds mesh2's last aggregate");
    let (source_publisher, source_version) = fp_bb.held_source("mesh2").unwrap();

    // The fabric-primary, as executor: mesh2.admin.1 deleted on its exit proof, mesh3.admin.1 stopped.
    let deleted = op("delete-node", &admin2);
    assert!(fp_m.book.depart(deleted.clone()));
    fp_m.book.executed_delete(&deleted);
    let stopped = op("stop-node", &admin3);
    assert!(fp_m.book.stopped(stopped.clone()));
    fp_m.book.executed_stop(&stopped);

    // As the fabric primary, the acting primary of both meshes (neither has a primary).
    fp_bb.set_fabric_primary(true);
    let mut published = fp_bb.publish_for_primaryless(&fp_m, |_| None).await;
    published.sort();
    assert_eq!(published, vec!["mesh2".to_string(), "mesh3".to_string()], "both meshes are published by their acting primary");

    // What the fabric-primary's own topology answer serves of each.
    let served = fp_m.held_topology(&fp_own);
    let living = served.iter().find(|s| s.mesh == "mesh2").expect("the fabric-primary serves mesh2");
    assert_eq!(living.publisher.node, "mesh1.admin.1", "the acting primary is mesh2's publisher of record");
    assert_eq!(names(living), vec!["mesh2.rpc.1", "mesh2.rpc.2"], "a living mesh with no primary keeps the members held of it (was {source_publisher} v{source_version}): {living:?}");
    assert_eq!(living.departed.iter().map(|o| o.key()).collect::<Vec<_>>(), vec![deleted.key()], "the deleted primary is departed, not a digest");
    let dead = served.iter().find(|s| s.mesh == "mesh3").expect("the fabric-primary serves mesh3");
    assert!(dead.digests.is_empty(), "R-NS1: a dead mesh's acting aggregate has no digests: {dead:?}");
    assert_eq!(dead.in_flight.iter().map(|o| o.key()).collect::<Vec<_>>(), vec![stopped.key()], "its stopped last admin is in in_flight");

    // A peer mesh primary holds mesh2 as the acting primary published it.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let at_peer = loop {
        if peer_bb.held_source("mesh2").is_some_and(|(p, _)| p.node == "mesh1.admin.1") {
            break peer_m.held_topology(&peer_own).into_iter().find(|s| s.mesh == "mesh2").expect("the peer holds mesh2");
        }
        assert!(std::time::Instant::now() < deadline, "the peer never held mesh2 from its acting primary");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(names(&at_peer), vec!["mesh2.rpc.1", "mesh2.rpc.2"], "the peer holds the living mesh's members from the acting aggregate: {at_peer:?}");

    for bb in [bb2, bb3, fp_bb, peer_bb] {
        drop(bb);
    }
    for n in [&n2, &n3, &fp_n, &peer_n] {
        n.ep.close().await;
    }
    drop(telemetry);
    let spans = collect_spans(&dir);
    let acting: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.backbone.update.via-acting-primary").collect();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    for (mesh, want) in [("mesh2", 2u64), ("mesh3", 0u64)] {
        let span = acting.iter().find(|s| s["attributes"]["mesh"] == mesh).unwrap_or_else(|| panic!("the acting publish of {mesh} is spanned: {acting:#?}"));
        let digests = span["attributes"]["digests"].as_u64().or_else(|| span["attributes"]["digests"].as_str().and_then(|s| s.parse().ok()));
        assert_eq!(digests, Some(want), "the acting span of {mesh} names its digest count: {span}");
        println!("{CELL}: {mesh} acting span trace {} digests {want}", span["trace_id"]);
    }
}
