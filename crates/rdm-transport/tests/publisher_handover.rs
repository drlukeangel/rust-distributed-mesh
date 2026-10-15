//! i143.e12.s53 (rafka-v2 PRD row e12.s53), R-RX3: a gossip frame received out of order, with an
//! earlier version than the one the receiver holds for its source, is ignored, once, in the gossip
//! framework's receive path, over real gossip.
//!
//! The design (docs/plans/rulings/i143-rulings-2026-10-12-rx-authz.md, R-RX3, Luke
//! 2026-10-12): "One check in the gossip receive path, for every frame kind. A frame whose order key
//! is older than ... the newest the receiver holds for the same source is ignored. Order keys already
//! on the wire, so no new field: Members and MembersDelta: `published_at_rafka_ms` then
//! `topology_version`, per mesh; Digest: `digest_seq`, per birth; lifecycle facts:
//! `topology_version`." R-RX2: "One mesh has one publisher on the backbone: its mesh-primary ... No
//! cross-publisher ordering rule and no seat-epoch field are added." R-RX1: no receiver refuses an
//! input because of who sent it.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, Seat, SeatHolder};
use rdm_transport::membership::{learn_addresses, Backbone, Frame, Membership, StatusEvent};
use rdm_transport::snapshot::PublisherId;
use serde_json::Value;
use std::time::Duration;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
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
        emitted_at_rafka_ms: 1,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        config: Default::default(),
    }
}

fn seat_of(d: &MeshDigest, revision: u64) -> SeatHolder {
    SeatHolder { mesh: d.node.name.mesh.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision }
}

struct Node {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, _router: router, gossip }
}

fn addr_of(ep: &iroh::Endpoint) -> iroh::EndpointAddr {
    let a = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    iroh::EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.port())))
}

/// Wait, bounded, for `what` to hold: gossip delivers on its own time.
async fn until(what: &str, mut holds: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !holds() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn op_of(d: &MeshDigest, operation: &str) -> LifecycleOp {
    LifecycleOp { build_id: "81ey2zm6h456".into(), attempt: 1, operation: format!("{operation}:{}", d.node.name), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1 }
}

fn with_seq(d: &MeshDigest, digest_seq: u64) -> MeshDigest {
    MeshDigest { digest_seq, ..d.clone() }
}

/// CONTRACT (R-RX3; R-RX2; R-RX1): through real gossip on mesh1's channel, the ordinary member
/// mesh1.rpc.1 holds mesh1's projection from its primary mesh1.admin.1, then from mesh1.admin.2 once
/// the seat moved (its full, then its delta). Four frames, each published before what the member
/// holds of its source and each sent once, arrive after it:
/// - mesh1.admin.1's full of mesh1, stamped (`published_at_rafka_ms`) before the successor's, at a
///   higher `topology_version` than the successor holds;
/// - mesh1.admin.1's delta of mesh1, stamped the same;
/// - mesh1.admin.2's NodeStopped of mesh1.rpc.4 at a `topology_version` below its NodeDeleting of
///   mesh1.rpc.3, which the member already holds;
/// - mesh1.rpc.2's heartbeat at a `digest_seq` below the one the member holds.
/// The receive path ignores each by name (`rdm.mesh.membership.reject.via-earlier-version`), naming
/// the channel, the frame, its source, the key held and the key offered. The member keeps the
/// successor's projection at its version, holds no stop of mesh1.rpc.4, holds mesh1.rpc.2 at the
/// newer heartbeat, and marks no source desynchronized. Must NOT happen: a frame refused for who
/// published it (`via-predecessor-publisher`, `via-unseated-publisher`), an unknown-epoch repair, the
/// member switched back to the predecessor's projection, a late stop held.
#[test]
fn a_frame_published_before_what_the_receiver_holds_of_its_source_is_ignored_by_the_receive_path() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-s53-receive-order").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (n1, n2, nm) = (node().await, node().await, node().await);
        let a1 = addr_of(&n1.ep);
        for n in [&n2, &nm] {
            learn_addresses(&n.ep, &[a1.clone()]).unwrap();
        }
        let mesh_id = MeshId::mint();
        let clock = rdm_transport::clock::os_clock;
        let (p1_d, p2_d, rpc1, rpc2, rpc3, rpc4) = (digest("mesh1.admin.1", 41_001), digest("mesh1.admin.2", 41_002), digest("mesh1.rpc.1", 41_011), digest("mesh1.rpc.2", 41_012), digest("mesh1.rpc.3", 41_013), digest("mesh1.rpc.4", 41_014));
        let m1 = Membership::join(&n1.gossip, &n1.ep, &fabric(), "mesh1", &mesh_id, "mesh1.admin.1", clock(), vec![]).await.unwrap();
        let m2 = Membership::join(&n2.gossip, &n2.ep, &fabric(), "mesh1", &mesh_id, "mesh1.admin.2", clock(), vec![a1.clone()]).await.unwrap();
        let m = Membership::join(&nm.gossip, &nm.ep, &fabric(), "mesh1", &mesh_id, "mesh1.rpc.1", clock(), vec![a1.clone()]).await.unwrap();
        let bb1 = Backbone::join(&n1.gossip, &n1.ep, &m1, "mesh1", "mesh1.admin.1", p1_d.node.incarnation.clone(), vec![]).await.unwrap();
        let bb2 = Backbone::join(&n2.gossip, &n2.ep, &m2, "mesh1", "mesh1.admin.2", p2_d.node.incarnation.clone(), vec![a1.clone()]).await.unwrap();
        m2.wait_for_neighbour().await;
        m.wait_for_neighbour().await;
        let (pub1, pub2) = (PublisherId { node: "mesh1.admin.1".into(), incarnation: p1_d.node.incarnation.clone() }, PublisherId { node: "mesh1.admin.2".into(), incarnation: p2_d.node.incarnation.clone() });
        let held_at_member = || m.held_source_version("mesh1");
        let seq_at_member = |d: &MeshDigest| m.book.get(d.node.node_id.as_str()).map(|(h, _)| h.digest_seq);
        for book in [&m1.book, &m2.book] {
            for d in [&p1_d, &p2_d, &rpc1] {
                book.record(d.clone());
            }
        }

        // mesh1.admin.1 is seated and publishes: the member holds its projection.
        bb1.announce_seat(Seat::MeshPrimary, seat_of(&p1_d, 1)).await;
        bb1.set_mesh_primary(true);
        bb1.publish(&m1, bb1.own_members(&m1)).await;
        until("the member to hold mesh1.admin.1's projection", || held_at_member().is_some_and(|(p, _)| p == pub1)).await;
        let (_, pred_version) = held_at_member().unwrap();
        // The instant mesh1.admin.1's late frames are stamped with: before the successor publishes.
        let pred_stamp = m1.clock().now_rafka_ms();
        tokio::time::sleep(Duration::from_millis(5)).await;

        // The seat moves to mesh1.admin.2: its full, then its delta.
        bb1.set_mesh_primary(false);
        bb2.announce_seat(Seat::MeshPrimary, seat_of(&p2_d, 2)).await;
        bb2.set_mesh_primary(true);
        bb2.publish(&m2, bb2.own_members(&m2)).await;
        until("the member to install the successor's full", || held_at_member().is_some_and(|(p, _)| p == pub2)).await;
        let (_, v1) = held_at_member().unwrap();
        m2.book.record(rpc2.clone());
        bb2.publish(&m2, bb2.own_members(&m2)).await;
        until("the member to apply the successor's delta", || held_at_member().is_some_and(|(p, v)| p == pub2 && v > v1)).await;
        let (_, v2) = held_at_member().unwrap();

        // mesh1.admin.2 executes mesh1.rpc.3's delete: its NodeDeleting at the version it moved to.
        let deleting = op_of(&rpc3, "delete-node");
        let (_, deleting_version) = bb2.publish_status_event(StatusEvent::Deleting, &deleting).await;
        until("the member to hold mesh1.rpc.3's NodeDeleting", || m.book.in_flight().contains(&deleting)).await;
        // mesh1.rpc.2's heartbeat at digest_seq 7.
        m2.forward(&Frame::Digest { digest: with_seq(&rpc2, 7) }).await.unwrap();
        until("the member to hold mesh1.rpc.2 at digest_seq 7", || seq_at_member(&rpc2) == Some(7)).await;

        // The late frames, each published before what the member holds of its source.
        let late_full = Frame::Members {
            mesh: "mesh1".into(),
            publisher: pub1.clone(),
            forwarded_by: Some("mesh1.admin.1".into()),
            topology_version: v2 + 7,
            published_at_rafka_ms: pred_stamp,
            snapshot_id: 9_001,
            chunk_index: 0,
            chunk_count: 1,
            digests: vec![p1_d.clone(), rpc1.clone()],
            in_flight: vec![],
            departed: vec![],
        };
        let late_delta = Frame::MembersDelta { mesh: "mesh1".into(), source_publisher: pub1.clone(), base_version: pred_version, topology_version: pred_version + 1, published_at_rafka_ms: pred_stamp, changed: vec![], removed: vec![rpc1.node.node_id.to_string()], in_flight: vec![], departed: vec![] };
        let late_stop = StatusEvent::Stopped.frame(op_of(&rpc4, "stop-node"), pub2.clone(), deleting_version - 1);
        let late_heartbeat = Frame::Digest { digest: with_seq(&rpc2, 3) };
        // The cell's story starts here: an ignore before this instant is gossip's own reordering during setup
        // (a resend of a version the member already holds under a later stamp), not one of the late frames.
        let late_from = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
        m1.forward(&late_full).await.unwrap();
        m1.forward(&late_delta).await.unwrap();
        m2.forward(&late_stop).await.unwrap();
        m2.forward(&late_heartbeat).await.unwrap();
        // A newer heartbeat from each sender, after its late frames: once the member holds it, the late
        // frames ahead of it on the same channel were received.
        m1.forward(&Frame::Digest { digest: with_seq(&p1_d, 5) }).await.unwrap();
        m2.forward(&Frame::Digest { digest: with_seq(&rpc2, 9) }).await.unwrap();
        until("the member to hold each sender's newer heartbeat", || seq_at_member(&p1_d) == Some(5) && seq_at_member(&rpc2) == Some(9)).await;

        assert_eq!(held_at_member(), Some((pub2.clone(), v2)), "the member keeps the successor's projection at its version");
        assert!(!m.book.in_flight().iter().any(|op| op.node_id == rpc4.node.node_id), "the late stop is not held: {:?}", m.book.in_flight());
        assert!(m.desynced_sources().is_empty(), "no source is marked desynchronized: {:?}", m.desynced_sources());

        drop((bb1, bb2));
        for n in [&n1, &n2, &nm] {
            n.ep.close().await;
        }
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let at = |name: &str, node: &str| -> Vec<&Value> { spans.iter().filter(|s| s["name"] == name && attr(s, "node") == node).collect() };
        let ignored: Vec<&Value> = at("rdm.mesh.membership.reject.via-earlier-version", "mesh1.rpc.1").into_iter().filter(|s| s["start_unix_nano"].as_u64().is_some_and(|t| t >= late_from)).collect();
        let mut said: Vec<(String, String, String, String, String)> = ignored.iter().map(|s| (attr(s, "channel"), attr(s, "frame"), attr(s, "source"), attr(s, "held"), attr(s, "offered"))).collect();
        let birth = |d: &MeshDigest| format!("{}/{}", d.node.node_id, d.node.incarnation.as_str());
        let mut expected = vec![
            ("mesh:mesh1".to_string(), "members".to_string(), "mesh1".to_string(), String::new(), format!("published_at_rafka_ms={pred_stamp} topology_version={}", v2 + 7)),
            ("mesh:mesh1".to_string(), "members-delta".to_string(), "mesh1".to_string(), String::new(), format!("published_at_rafka_ms={pred_stamp} topology_version={}", pred_version + 1)),
            ("mesh:mesh1".to_string(), "node-stopped".to_string(), format!("{pub2} mesh1"), format!("topology_version={deleting_version}"), format!("topology_version={}", deleting_version - 1)),
            ("mesh:mesh1".to_string(), "digest".to_string(), birth(&rpc2), "digest_seq=7".to_string(), "digest_seq=3".to_string()),
        ];
        // The successor's delta stamp is the member's held key of mesh1: read it from the span itself.
        let held_stamp = ignored.iter().find(|s| attr(s, "frame") == "members").map(|s| attr(s, "held")).unwrap_or_default();
        for e in expected.iter_mut().take(2) {
            e.3 = held_stamp.clone();
        }
        // Two senders: the receive order across them is gossip's own.
        said.sort();
        expected.sort();
        assert_eq!(said, expected, "each late frame is ignored by the receive path, by name: {ignored:#?}");
        let held_published_at: u64 = held_stamp.strip_prefix("published_at_rafka_ms=").and_then(|r| r.split(' ').next()).and_then(|n| n.parse().ok()).unwrap_or(0);
        assert!(held_published_at > pred_stamp && held_stamp.ends_with(&format!(" topology_version={v2}")), "the held key of mesh1 is the successor's, stamped after the late frames: {held_stamp}");
        for refusal in ["rdm.mesh.membership.reject.via-predecessor-publisher", "rdm.mesh.membership.reject.via-unseated-publisher"] {
            assert!(spans.iter().all(|s| s["name"] != refusal), "no frame is refused for who published it: {refusal}");
        }
        let mut lines = String::new();
        for s in &ignored {
            lines.push_str(&format!("ignored {} {} frame={} source={} held={} offered={}\n", s["trace_id"].as_str().unwrap_or(""), s["name"].as_str().unwrap_or(""), attr(s, "frame"), attr(s, "source"), attr(s, "held"), attr(s, "offered")));
        }
        std::fs::write(dir.join("trace-ids.txt"), lines).unwrap();
    });
}
