//! Position 10 has one receive effect: refuse it, with no status, fact or role installation.
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, PrimaryRole, Primary};
use rdm_transport::topology::{backbone_channel, learn_addresses, mesh_channel, Backbone, Frame, TopologyView};
use std::time::Duration;

async fn endpoint() -> (iroh::Endpoint, iroh_gossip::net::Gossip, iroh::protocol::Router) {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    (ep, gossip, router)
}

/// CONTRACT (Q23, gossip.md §3.1): position 10 is reserved on every channel. Its only effect is
/// a named refusal; it installs neither a projection nor a fact, carries nothing and decides nothing.
#[test]
fn receiver_refuses_position_ten_and_changes_no_status_or_primary() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-concern-reserved").unwrap();
    capture.run(2, async {
        let (send_ep, gossip, _send_router) = endpoint().await;
        let (recv_ep, recv_gossip, _recv_router) = endpoint().await;
        let socket = send_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let addr = iroh::EndpointAddr::new(send_ep.id()).with_ip_addr(socket);
        learn_addresses(&recv_ep, &[addr.clone()]).unwrap();
        learn_addresses(&send_ep, &[iroh::EndpointAddr::new(recv_ep.id()).with_ip_addr(recv_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap())]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let mut mesh_channel = gossip.subscribe(mesh_channel(&fabric, &mesh), vec![]).await.unwrap();
        let mut backbone_channel = gossip.subscribe(backbone_channel(&fabric), vec![]).await.unwrap();
        let receiver = TopologyView::join(&recv_gossip, &recv_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![addr.clone()], &[]).await.unwrap();
        let _backbone = Backbone::join(&recv_gossip, &recv_ep, &receiver, "mesh1", rdm_core::PublisherId { node: "mesh1.admin.1".to_string(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }, vec![addr]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), mesh_channel.joined()).await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(5), backbone_channel.joined()).await.unwrap().unwrap();
        let (mesh_sender, _mesh_rx) = mesh_channel.split();
        let (backbone_sender, _backbone_rx) = backbone_channel.split();
        let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
        let digest = MeshDigest { held_hash: Default::default(), fabric_id: fabric.clone(), node: MeshNode { node_id: node_id.clone(), name: "mesh1.admin.2".parse().unwrap(), endpoint_id: EndpointId(send_ep.id().to_string()), transport_addr: socket, incarnation: incarnation.clone(), supersedes: None, runtime: None }, status: MemberStatus::Ready, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: Some(mesh), in_flight: None, load: None, gossip: None, is_primary: None, is_fabric_primary: None };
        receiver.digest_view.record(digest);
        assert!(receiver.digest_view.started(&node_id, &incarnation, &"mesh1.admin.2".parse().unwrap()));
        let primary = Primary { mesh: "mesh1".into(), node_id: node_id.clone(), incarnation: incarnation.clone(), revision: 1 };
        receiver.primaries().take(PrimaryRole::FabricPrimary, &primary);
        receiver.primaries().take(PrimaryRole::MeshPrimary, &primary);
        receiver.learn_statuses(&[Frame::MeshStatus { mesh: "mesh1".into(), status: "ready".into(), publisher: "mesh1.admin.2".into(), forwarded_by: None, changed_at_rafka_ms: 1 }]);
        let held = receiver.digest_view.get(node_id.as_str()).unwrap().0;
        let status = receiver.mesh_status("mesh1");
        let fabric_status = receiver.fabric_status();
        let roles = receiver.primaries().subscribe();
        let births = receiver.digest_view.birth_changes();
        let overlays = receiver.digest_view.overlay_changes();
        let before = (*roles.borrow(), *births.borrow(), *overlays.borrow());
        let frame = Frame::Concern { role: PrimaryRole::FabricPrimary, node_id: node_id.clone(), incarnation, observer: "mesh2.admin.1".into() };
        assert_eq!(frame.clone().authored(&crate::gossip_author(&fabric), 0, 1).context().unwrap().kind, "rdm.mesh.concern", "reserved position 10 has its named type");
        for (n, sender) in [&mesh_sender, &backbone_sender].into_iter().enumerate() {
            sender.broadcast(frame.clone().authored(&crate::gossip_author(&fabric), 0, 1).encode().into()).await.unwrap();
            // A following frame on this same channel proves processing finished, without changing
            // the concerned mesh's status or relying on an arbitrary delay.
            let revision = n as u64 + 10;
            sender.broadcast(Frame::MeshStatus { mesh: "barrier".into(), status: "ready".into(), publisher: "mesh1.admin.2".into(), forwarded_by: None, changed_at_rafka_ms: revision }.authored(&crate::gossip_author(&fabric), 0, 1).encode().into()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while receiver.mesh_status("barrier").is_none_or(|s| s.changed_at_rafka_ms < revision) { tokio::time::sleep(Duration::from_millis(10)).await; }
            }).await.expect("the following frame was applied on this channel");
        }
        assert_eq!(receiver.digest_view.get(node_id.as_str()).unwrap().0, held);
        assert_eq!(receiver.mesh_status("mesh1"), status);
        assert_eq!(receiver.fabric_status(), fabric_status);
        assert_eq!(receiver.primaries().fabric(), Some(primary.clone()));
        assert_eq!(receiver.primaries().mesh("mesh1"), Some(primary));
        assert_eq!((*roles.borrow(), *births.borrow(), *overlays.borrow()), before);
        assert!(!receiver.digest_view.is_deleted(node_id.as_str()));
        assert!(receiver.digest_view.in_flight().is_empty());
        assert!(receiver.digest_view.routable(node_id.as_str()));
        send_ep.close().await; recv_ep.close().await;
    });
    let spans = capture.spans().unwrap();
    for s in spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.reject.via-reserved") { println!("{}", serde_json::to_string(s).unwrap()); }
    for via in ["mesh-channel", "backbone"] {
        assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.reject.via-reserved" && s["attributes"]["position"] == "10" && s["attributes"]["frame"] == "concern" && s["attributes"]["via"] == via).count(), 1, "each channel refuses the reserved frame by name");
    }
    assert!(!spans.iter().any(|s| matches!(s["name"].as_str(), Some("rdm.mesh.primary.update.via-concern" | "rdm.node_admin.primary.update.via-concern-answer"))));
}
