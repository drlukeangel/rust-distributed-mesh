//! i143.e12.s53 (rafka-v2 PRD row e12.s53), R-RX3: a gossip frame received out of order, with an
//! earlier version than the one the receiver holds for its source, is ignored, once, in the gossip
//! framework's receive path, over real gossip.
//!
//! The design (docs/plans/rulings/i143-rulings-2026-10-12-rx-authz.md, R-RX3, Luke
//! 2026-10-12): "One check in the gossip receive path, for every frame kind. A frame whose order key
//! is older than ... the newest the receiver holds for the same source is ignored. Order keys already
//! on the wire, so no new field: TopologyFull and TopologyDelta: `published_at_rafka_ms` then
//! `topology_version`, per mesh; Digest: `digest_seq`, per birth; lifecycle facts:
//! `topology_version`." R-RX2: "One mesh has one publisher on the backbone: its mesh-primary ... No
//! cross-publisher ordering rule and no primary-revision field are added." R-RX1: no receiver refuses an
//! input because of who sent it.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId, Primary, PrimaryRole};
use rdm_gossip::location::learn_addresses;
use rdm_gossip::backbone::Backbone;
use rdm_gossip::frame::Frame;
use rdm_gossip::topology::TopologyView;
use rdm_gossip::status::StatusEvent;
use rdm_gossip::aggregate::PublisherId;
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

fn digest(name: rdm_core::PathName, mesh: &str, r#type: &str, port: u16) -> MeshDigest {
    let node_id = NodeId::mint();
    MeshDigest { 
        fabric_id: fabric(),
        node: MeshNode { mesh: mesh.to_owned(), r#type: r#type.to_string(),
            node_id: node_id.clone(),
            path: name.path.clone(), name: name.name.clone(),
            endpoint_id: EndpointId(format!("key-{node_id}")),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
        },
        status: NodeStatus::Ready,
        digest_seq: 1,
        emitted_at_rafka_ms: 1,
        primary: Vec::new(),
    }
}

fn primary_of(d: &MeshDigest, revision: u64) -> Primary {
    Primary { mesh: d.node.mesh.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision }
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
    LifecycleOp { r#type: d.node.r#type.clone(), mesh: d.node.mesh.clone(), build_id: "81ey2zm6h456".into(), operation: rdm_core::OperationId::new(operation, rdm_core::OperationSubject::Birth { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), path: d.node.path.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }
}

/// What `view`'s topology read serves of `mesh`, as the chunks a reader takes (gossip.md §3.3: a node
/// holds its own mesh's projection only by a read, Q387).
fn read_of(view: &TopologyView, me: &MeshDigest, mesh: &str) -> Vec<rdm_gossip::aggregate::Chunk> {
    let served = view.held_topology(me);
    let s = served.iter().find(|s| s.mesh == mesh).expect("the read holds the mesh");
    vec![rdm_gossip::aggregate::Chunk { mesh: s.mesh.clone(), mesh_id: None, author: None, topology_version: s.topology_version, time_ms: 1, chunk_index: 0, chunk_count: 1, digests: s.full().digests(), runtimes: Vec::new() }]
}

fn with_seq(d: &MeshDigest, digest_seq: u64) -> MeshDigest {
    MeshDigest { digest_seq, ..d.clone() }
}

/// CONTRACT (R-RX3; R-RX2; R-RX1): through real gossip on mesh1's channel, the ordinary member
/// mesh1.node.1 holds mesh1's projection from its primary mesh1.node-admin.1, then from mesh1.node-admin.2 once
/// the role moved (its full, then its delta). Four frames, each published before what the member
/// holds of its source and each sent once, arrive after it:
/// - mesh1.node-admin.1's full of mesh1, stamped (`published_at_rafka_ms`) before the successor's, at a
///   higher `topology_version` than the successor holds;
/// - mesh1.node-admin.1's delta of mesh1, stamped the same;
/// - mesh1.node-admin.2's NodeStopped of mesh1.node.4 at a `topology_version` below its NodeDeleting of
///   mesh1.node.3, which the member already holds;
/// - mesh1.node.2's heartbeat at a `digest_seq` below the one the member holds.
/// The receive path ignores each by name (`via-stale` for the lifecycle projection revision,
/// `via-stale` for the other keyed families), naming
/// the channel, the frame, its source, the key held and the key offered. The member keeps the
/// successor's projection at its version, holds no stop of mesh1.node.4, holds mesh1.node.2 at the
/// newer heartbeat, and marks no source desynchronized. Must NOT happen: a frame refused for who
/// published it (`via-predecessor-publisher`, `via-non-primary-publisher`), an unknown-revision repair, the
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
        let (p1_d, p2_d, rpc1, rpc2, rpc3, rpc4) = (digest(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 41_001), digest(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", 41_002), digest(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 41_011), digest(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", 41_012), digest(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", 41_013), digest(rdm_core::PathName::new("mesh1.node", "4").unwrap(), "mesh1", "node", 41_014));
        let m1 = rdm_gossip::topology::TopologyView::new(&fabric(), "mesh1", &mesh_id, p1_d.node.node_id.as_str(), clock(), Default::default());
        let m1_channels = rdm_gossip::channel::Channels::new(&n1.gossip, &n1.ep, &m1).unwrap();
        m1_channels.join(rdm_gossip::channel::ChannelId::mesh(m1.fabric_id(), m1.mesh_id()), vec![]).await.unwrap();
        let m2 = rdm_gossip::topology::TopologyView::new(&fabric(), "mesh1", &mesh_id, p2_d.node.node_id.as_str(), clock(), Default::default());
        let m2_channels = rdm_gossip::channel::Channels::new(&n2.gossip, &n2.ep, &m2).unwrap();
        m2_channels.join(rdm_gossip::channel::ChannelId::mesh(m2.fabric_id(), m2.mesh_id()), vec![a1.clone()]).await.unwrap();
        let m = rdm_gossip::topology::TopologyView::new(&fabric(), "mesh1", &mesh_id, rpc1.node.node_id.as_str(), clock(), Default::default());
        let m_channels = rdm_gossip::channel::Channels::new(&nm.gossip, &nm.ep, &m).unwrap();
        m_channels.join(rdm_gossip::channel::ChannelId::mesh(m.fabric_id(), m.mesh_id()), vec![a1.clone()]).await.unwrap();
        m1_channels.set_publisher(&rdm_core::PublisherId { incarnation: p1_d.node.incarnation.clone(), node_id: p1_d.node.node_id.clone() });
        m1_channels.join(rdm_gossip::channel::ChannelId::backbone(m1.fabric_id()), vec![]).await.unwrap();
        let bb1 = m1_channels.backbone().expect("the backbone is joined");
        m2_channels.set_publisher(&rdm_core::PublisherId { incarnation: p2_d.node.incarnation.clone(), node_id: p2_d.node.node_id.clone() });
        m2_channels.join(rdm_gossip::channel::ChannelId::backbone(m2.fabric_id()), vec![a1.clone()]).await.unwrap();
        let bb2 = m2_channels.backbone().expect("the backbone is joined");
        m2_channels.wait_for_neighbour().await;
        m_channels.wait_for_neighbour().await;
        // The predecessor publishes once below, and the successor must hold that publication: its
        // backbone link to the predecessor is up first, as this file's forwarder cell waits for its own.
        // `wait_for_neighbour` is the mesh channel's alone (rdm#390: gate 68da21540 published at +26 ms,
        // the backbone neighbour up at +44 ms, and the one frame reached no one).
        let mut linked = n2.gossip.subscribe(rdm_gossip::channel::ChannelId::backbone(m2.fabric_id()).topic(), vec![n1.ep.id()]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(20), linked.joined()).await.expect("the successor's backbone link to the predecessor is up").unwrap();
        let (pub1, pub2) =(PublisherId { incarnation: p1_d.node.incarnation.clone(), node_id: p1_d.node.node_id.clone() }, PublisherId { incarnation: p2_d.node.incarnation.clone(), node_id: p2_d.node.node_id.clone() });
        let held_at_member = || m.held_source_version("mesh1");
        let seq_at_member = |d: &MeshDigest| m.digest_view.get(d.node.node_id.as_str()).map(|(h, _)| h.digest_seq);
        for digest_view in [&m1.digest_view, &m2.digest_view] {
            for d in [&p1_d, &p2_d, &rpc1] {
                digest_view.record(d.clone());
            }
        }

        // mesh1.node-admin.1 is primary and publishes: the member holds its projection.
        bb1.hold_primary(PrimaryRole::MeshPrimary, primary_of(&p1_d, 1));
        bb1.set_mesh_primary(true);
        bb1.publish(&m1, bb1.own_members(&m1)).await;
        // Its own mesh reaches the member by a read (Q387: nothing of mesh1 on mesh1's channel).
        let read = read_of(&m1, &p1_d, "mesh1");
        let served = read[0].topology_version;
        for c in read {
            let _ = m_channels.take_read_chunk(c, "mesh1");
        }
        // A read names no author: the member holds the version it served.
        until("the member to hold mesh1.node-admin.1's projection", || held_at_member() == Some((None, served))).await;
        let (_, pred_version) = held_at_member().unwrap();
        // The successor holds that version of its mesh from the backbone before it takes the role, so it
        // continues the mesh's version from it (Q391). Gate 32b4231c7: mesh1.node-admin.2 took the role
        // holding none and published v1 again; the late delta at the version then held was not earlier.
        until("mesh1.node-admin.2 to hold mesh1's aggregate from the backbone", || bb2.held_source("mesh1").is_some_and(|(_, v)| v == pred_version)).await;
        // The instant mesh1.node-admin.1's late frames are stamped with: before the successor publishes.
        let pred_stamp = m1.clock().now_rafka_ms();
        tokio::time::sleep(Duration::from_millis(5)).await;

        // The role moves to mesh1.node-admin.2: its full, then its delta.
        bb1.set_mesh_primary(false);
        bb2.hold_primary(PrimaryRole::MeshPrimary, primary_of(&p2_d, 2));
        bb2.set_mesh_primary(true);
        bb2.publish(&m2, bb2.own_members(&m2)).await;
        let read = read_of(&m2, &p2_d, "mesh1");
        let served = read[0].topology_version;
        for c in read {
            let _ = m_channels.take_read_chunk(c, "mesh1");
        }
        assert!(served > pred_version, "the successor continues the mesh's version (Q391): v{served} after v{pred_version}");
        until("the member to install the successor's full", || held_at_member() == Some((None, served))).await;
        let (_, v1) = held_at_member().unwrap();
        m2.digest_view.record(rpc2.clone());
        bb2.publish(&m2, bb2.own_members(&m2)).await;
        for c in read_of(&m2, &p2_d, "mesh1") {
            let _ = m_channels.take_read_chunk(c, "mesh1");
        }
        until("the member to take the successor's next version", || held_at_member().is_some_and(|(_, v)| v > v1)).await;
        let (_, v2) = held_at_member().unwrap();

        // mesh1.node-admin.2 executes mesh1.node.3's delete: its NodeDeleting at the version it moved to.
        let deleting = op_of(&rpc3, "delete-node");
        let (_, deleting_version) = bb2.publish_status_event(StatusEvent::Deleting, &deleting).await;
        until("the member to hold mesh1.node.3's NodeDeleting", || m.digest_view.in_flight().contains(&deleting)).await;
        // mesh1.node.2's heartbeat at digest_seq 7.
        m2_channels.forward(&Frame::digest(&with_seq(&rpc2, 7))).await.unwrap();
        until("the member to hold mesh1.node.2 at digest_seq 7", || seq_at_member(&rpc2) == Some(7)).await;

        // The late frames, each published before what the member holds of its source.
        let late_full = Frame::aggregate("mesh1", None, &rdm_gossip::envelope::Birth::publisher(&fabric(), &pub1), v2 + 7, pred_stamp, 0, 1, vec![p1_d.clone(), rpc1.clone()]);
        let late_delta = Frame::TopologyDelta { mesh: "mesh1".into(), mesh_id: None, base_version: pred_version, chunk_index: 0, chunk_count: 1, added: vec![], changed: vec![], removed: vec![rdm_core::framing::Birth { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation.clone() }]}
            .authored_delta(&rdm_gossip::envelope::Birth::publisher(&fabric(), &pub1), pred_version + 1, pred_stamp);
        let late_stop = StatusEvent::Stopped.frame(op_of(&rpc4, "stop-node"), deleting_version - 1);
        let late_heartbeat = Frame::digest(&with_seq(&rpc2, 3));
        // The cell's story starts here: an ignore before this instant is gossip's own reordering during setup
        // (a resend of a version the member already holds under a later stamp), not one of the late frames.
        let late_from = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
        m1_channels.forward(&late_full).await.unwrap();
        m1_channels.forward(&late_delta).await.unwrap();
        m2_channels.forward(&late_stop.clone().authored(&rdm_gossip::envelope::Birth::publisher(&fabric(), &pub2), 0, 1)).await.unwrap();
        m2_channels.forward(&late_heartbeat).await.unwrap();
        // A newer heartbeat from each sender, after its late frames: once the member holds it, the late
        // frames ahead of it on the same channel were received.
        m1_channels.forward(&Frame::digest(&with_seq(&p1_d, 5))).await.unwrap();
        m2_channels.forward(&Frame::digest(&with_seq(&rpc2, 9))).await.unwrap();
        until("the member to hold each sender's newer heartbeat", || seq_at_member(&p1_d) == Some(5) && seq_at_member(&rpc2) == Some(9)).await;

        assert_eq!(held_at_member(), Some((None, v2)), "the member keeps the successor's projection at its version");
        assert!(!m.digest_view.in_flight().iter().any(|op| op.node_id == rpc4.node.node_id), "the late stop is not held: {:?}", m.digest_view.in_flight());
        assert!(m.desynced_sources().is_empty(), "no source is marked desynchronized: {:?}", m.desynced_sources());

        drop((bb1, bb2));
        for n in [&n1, &n2, &nm] {
            n.ep.close().await;
        }
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let at = |name: &str, node: &str| -> Vec<&Value> { spans.iter().filter(|s| s["name"] == name && attr(s, "node") == node).collect() };
        let ignored: Vec<&Value> = at("rdm.mesh.topology.reject.via-stale", rpc1.node.node_id.as_str()).into_iter()
            .filter(|s| s["start_unix_nano"].as_u64().is_some_and(|t| t >= late_from)).collect();
        let fields = |s: &Value| (attr(s, "channel"), attr(s, "frame"), attr(s, "subject"), attr(s, "held_key"), attr(s, "frame_key"), s["name"].as_str().unwrap_or_default().to_string());
        let mut said: Vec<_> = ignored.iter().map(|s| fields(s)).collect();
        let birth = |d: &MeshDigest| format!("{}/{}", d.node.node_id, d.node.incarnation.as_str());
        let mut expected = vec![
            ("mesh:mesh1".to_string(), "node-stopped".to_string(), "mesh1".to_string(), format!("topology_version={deleting_version}"), format!("topology_version={}", deleting_version - 1), "rdm.mesh.topology.reject.via-stale".to_string()),
            ("mesh:mesh1".to_string(), "digest".to_string(), birth(&rpc2), "digest_seq=7".to_string(), "digest_seq=3".to_string(), "rdm.mesh.topology.reject.via-stale".to_string()),
            // The predecessor's late delta is below the version the member holds of mesh1: the receive path
            // ignores it first, whoever sent it (Q389).
            ("mesh:mesh1".to_string(), "mesh-topology-delta".to_string(), "mesh1".to_string(), format!("topology_version={v2}"), format!("topology_version={}", pred_version + 1), "rdm.mesh.topology.reject.via-stale".to_string()),
        ];
        // Two senders: the receive order across them is gossip's own.
        said.sort();
        expected.sort();
        assert_eq!(said, expected, "each late lifecycle frame and heartbeat is ignored by the receive path, by name: {ignored:#?}");
        // The predecessor's late aggregate of mesh1, above the held version, is off the backbone: refused by
        // name (gossip.md §3.3, Q387: an aggregate travels on the backbone only; a member's view comes by a read).
        let off: Vec<(String, String)> = at("rdm.mesh.topology.reject.via-members-on-wrong-channel", rpc1.node.node_id.as_str()).into_iter()
            .filter(|s| s["start_unix_nano"].as_u64().is_some_and(|t| t >= late_from) && attr(s, "publisher") == pub1.to_string())
            .map(|s| (attr(s, "mesh"), attr(s, "channel"))).collect();
        let mut off = off; off.sort(); off.dedup();
        assert_eq!(off, vec![("mesh1".to_string(), "mesh-channel".to_string())], "the late aggregate of mesh1 is refused off the backbone");
        let _ = (pred_stamp, pred_version);
        for refusal in ["rdm.mesh.topology.reject.via-predecessor-publisher", "rdm.mesh.topology.reject.via-non-primary-publisher"] {
            assert!(spans.iter().all(|s| s["name"] != refusal), "no frame is refused for who published it: {refusal}");
        }
        let mut lines = String::new();
        for s in &ignored {
            lines.push_str(&format!("ignored {} {} frame={} source={} held={} offered={}\n", s["trace_id"].as_str().unwrap_or(""), s["name"].as_str().unwrap_or(""), attr(s, "frame"), fields(s).2, fields(s).3, fields(s).4));
        }
        std::fs::write(dir.join("trace-ids.txt"), lines).unwrap();
    });
}

/// A supplied author survives an empty publication and its receipt on the backbone. A same-name member
/// in the forwarder's view supplies no author identity. On the mesh channel the differential names its
/// forwarder, its envelope's source, never the peer mesh's mesh-primary (Q389): a neighbour replay and
/// the next differential name the forwarder; a topology read, whose reply names no author, installs the
/// version.
#[test]
fn an_empty_publication_retains_its_supplied_author_birth_everywhere() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-publisher-birth").unwrap();
    capture.run(4, async {
        let (source, forwarder, member, reader) = (node().await, node().await, node().await, node().await);
        let (source_addr, forwarder_addr) = (addr_of(&source.ep), addr_of(&forwarder.ep));
        learn_addresses(&forwarder.ep, &[source_addr.clone()]).unwrap();
        learn_addresses(&member.ep, &[forwarder_addr.clone()]).unwrap();
        let (source_mesh, own_mesh) = (MeshId::mint(), MeshId::mint());
        let source_birth = digest(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", 42001);
        let forwarder_birth = digest(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 42002);
        let author = PublisherId::from(&source_birth.node);
        let src = rdm_gossip::topology::TopologyView::new(&fabric(), "mesh2", &source_mesh, source_birth.node.node_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let src_channels = rdm_gossip::channel::Channels::new(&source.gossip, &source.ep, &src).unwrap();
        src_channels.join(rdm_gossip::channel::ChannelId::mesh(src.fabric_id(), src.mesh_id()), vec![]).await.unwrap();
        let fwd = rdm_gossip::topology::TopologyView::new(&fabric(), "mesh1", &own_mesh, forwarder_birth.node.node_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let fwd_channels = rdm_gossip::channel::Channels::new(&forwarder.gossip, &forwarder.ep, &fwd).unwrap();
        fwd_channels.join(rdm_gossip::channel::ChannelId::mesh(fwd.fabric_id(), fwd.mesh_id()), vec![]).await.unwrap();
        // Deliberately a different exact birth at the author's descriptive path.
        let impostor = digest(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", 42003);
        assert_ne!(impostor.node.node_id, author.node_id);
        fwd.digest_view.record(impostor);
        src_channels.set_publisher(&author.clone());
        src_channels.join(rdm_gossip::channel::ChannelId::backbone(src.fabric_id()), vec![]).await.unwrap();
        let source_bb = src_channels.backbone().expect("the backbone is joined");
        fwd_channels.set_publisher(&PublisherId::from(&forwarder_birth.node));
        fwd_channels.join(rdm_gossip::channel::ChannelId::backbone(fwd.fabric_id()), vec![source_addr]).await.unwrap();
        let fwd_bb = fwd_channels.backbone().expect("the backbone is joined");
        source_bb.set_mesh_primary(true);
        fwd_bb.set_mesh_primary(true);
        let mut linked = source.gossip.subscribe(rdm_gossip::channel::ChannelId::backbone(&fabric()).topic(), vec![forwarder.ep.id()]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(20), linked.joined()).await.unwrap().unwrap();
        assert!(source_bb.publish(&src, vec![]).await);
        until("the forwarder to install the empty source", || fwd_bb.held_source("mesh2").is_some()).await;
        let (held_author, version) = fwd_bb.held_source("mesh2").unwrap();
        assert_eq!(held_author, Some(author.clone()), "receipt keeps the supplied author, not the same-name member");

        // This neighbour joins after the empty source installed: its baseline is the replayed differential from 0.
        let own = digest(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 42004);
        let mem = rdm_gossip::topology::TopologyView::new(&fabric(), "mesh1", &own_mesh, own.node.node_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let mem_channels = rdm_gossip::channel::Channels::new(&member.gossip, &member.ep, &mem).unwrap();
        mem_channels.join(rdm_gossip::channel::ChannelId::mesh(mem.fabric_id(), mem.mesh_id()), vec![forwarder_addr]).await.unwrap();
        until("the new neighbour to apply the replayed differential", || mem.held_source_version("mesh2").is_some()).await;
        let sent_by = PublisherId::from(&forwarder_birth.node);
        assert_eq!(mem.held_source_version("mesh2"), Some((Some(sent_by.clone()), version)), "the replayed differential names its forwarder");
        let served = mem.held_topology(&own).into_iter().find(|s| s.mesh == "mesh2").unwrap();
        assert_eq!(served.topology_version, version);
        assert!(served.digests.is_empty(), "the author is carried even without an author digest");
        let reader_id = NodeId::mint();
        let read = rdm_gossip::topology::TopologyView::new(&fabric(), "mesh1", &own_mesh, reader_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let read_channels = rdm_gossip::channel::Channels::new(&reader.gossip, &reader.ep, &read).unwrap();
        read_channels.join(rdm_gossip::channel::ChannelId::mesh(read.fabric_id(), read.mesh_id()), vec![]).await.unwrap();
        let chunk = rdm_gossip::aggregate::Chunk { mesh: served.mesh, mesh_id: None, author: None, topology_version: served.topology_version, time_ms: 1, chunk_index: 0, chunk_count: 1, digests: served.digests, runtimes: Vec::new() };
        assert!(matches!(read_channels.take_read_chunk(chunk, "mesh1"), rdm_gossip::aggregate::Taken::Written(_)));
        assert_eq!(read.held_source_version("mesh2"), Some((None, version)), "a topology read installs the version; its reply names no author");

        // A subsequent material change goes in through the delta path, from the same forwarder.
        source_bb.publish(&src, vec![digest(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", 42005)]).await;
        until("the member to apply the next source version", || mem.held_source_version("mesh2").is_some_and(|(_, v)| v > version)).await;
        assert_eq!(mem.held_source_version("mesh2").unwrap().0, Some(sent_by));
        for n in [&source, &forwarder, &member, &reader] { n.ep.close().await; }
    });
}

/// One birth's backbone publications at an unchanged material version carry the mesh's version as
/// their `sequence` and its id `rdm.mesh.members/<mesh>/<version>/<chunk>` (Q389): distinct
/// publications by their `time`, the author their `source`; nothing of its own mesh goes on its own
/// mesh channel, whose nodes publish themselves (gossip.md §2, Q387).
#[test]
fn one_births_publications_at_one_version_share_its_id_and_put_nothing_of_its_own_mesh_on_its_channel() {
    use futures_lite::StreamExt;
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-aggregate-id").unwrap();
    capture.run(4, async {
        let (source, probe) = (node().await, node().await);
        learn_addresses(&probe.ep, &[addr_of(&source.ep)]).unwrap();
        learn_addresses(&source.ep, &[addr_of(&probe.ep)]).unwrap();
        let mesh = MeshId::mint();
        let node_id = NodeId::mint();
        let author = PublisherId { node_id, incarnation: IncarnationId::mint() };
        let topology_view = rdm_gossip::topology::TopologyView::new(&fabric(), "mesh1", &mesh, author.node_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let channels = rdm_gossip::channel::Channels::new(&source.gossip, &source.ep, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![]).await.unwrap();
        channels.set_publisher(&author.clone());
        channels.join(rdm_gossip::channel::ChannelId::backbone(topology_view.fabric_id()), vec![]).await.unwrap();
        let backbone = channels.backbone().expect("the backbone is joined");
        backbone.set_mesh_primary(true);
        let mut local = probe.gossip.subscribe(rdm_gossip::channel::ChannelId::mesh(&fabric(), &mesh).topic(), vec![source.ep.id()]).await.unwrap();
        let mut remote = probe.gossip.subscribe(rdm_gossip::channel::ChannelId::backbone(&fabric()).topic(), vec![source.ep.id()]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), local.joined()).await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(5), remote.joined()).await.unwrap().unwrap();
        let (_local_sender, mut local) = local.split(); let (_remote_sender, mut remote) = remote.split();
        let mut times = Vec::new();
        let mut on_mesh = 0;
        for _ in 0..2 {
            // A publication is its author's at one Rafka-time: two in one millisecond are one.
            tokio::time::sleep(Duration::from_millis(3)).await;
            assert!(backbone.publish(&topology_view, vec![]).await);
            let full = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    tokio::select! {
                        event = local.next() => if let Some(Ok(iroh_gossip::api::Event::Received(message))) = event {
                            if matches!(Frame::decode(&message.content).unwrap().data(), Frame::TopologyFull { .. }) { on_mesh += 1; }
                        },
                        event = remote.next() => if let Some(Ok(iroh_gossip::api::Event::Received(message))) = event {
                            let frame = Frame::decode(&message.content).unwrap();
                            if matches!(frame.data(), Frame::TopologyFull { .. }) { break frame; }
                        },
                    }
                }
            }).await.unwrap();
            let context = full.context().unwrap();
            assert_eq!(context.source, rdm_gossip::envelope::Birth::publisher(&fabric(), &author));
            assert_eq!(context.sequence.as_deref(), Some("00000000000000000001"));
            assert_eq!(context.id, "rdm.mesh.members/mesh1/1/0", "the id names the mesh, its version and the chunk");
            times.push(context.time_ms);
        }
        assert!(times[0] < times[1], "two publications at one version, each at its own time: {times:?}");
        // The backbone's two fulls were received; a full of mesh1 on mesh1's channel would have been too.
        let _ = tokio::time::timeout(Duration::from_millis(500), async {
            while let Some(Ok(event)) = local.next().await {
                if let iroh_gossip::api::Event::Received(message) = event {
                    if matches!(Frame::decode(&message.content).unwrap().data(), Frame::TopologyFull { .. }) { on_mesh += 1; }
                }
            }
        }).await;
        assert_eq!(on_mesh, 0, "nothing of mesh1 on mesh1's channel");
        source.ep.close().await; probe.ep.close().await;
    });
}
