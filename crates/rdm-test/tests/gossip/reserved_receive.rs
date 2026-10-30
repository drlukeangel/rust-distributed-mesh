//! Reserved positions are refused before any fact, status or role path runs.
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, PrimaryRole, Primary, LifecycleOp};
use rdm_transport::topology::{backbone_channel, learn_addresses, mesh_channel, Backbone, Frame, TopologyView};
use std::time::Duration;

async fn endpoint() -> (iroh::Endpoint, iroh_gossip::net::Gossip, iroh::protocol::Router) {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    (ep, gossip, router)
}

#[derive(Clone, Copy)]
enum Kind { ConnectionsDeleting, ConnectionsDeleted, Leaving, Left, FabricPrimary, Restarting }
impl Kind {
    fn type_name(self) -> &'static str { match self { Self::ConnectionsDeleting => "rdm.node.connections.deleting", Self::ConnectionsDeleted => "rdm.node.connections.deleted", Self::Leaving => "rdm.node.leaving", Self::Left => "rdm.node.left", Self::FabricPrimary => "rdm.fabric.primary.new", Self::Restarting => "rdm.node.restarting" } }
    fn named(self) -> (&'static str, u8) { match self { Self::ConnectionsDeleting => ("node-connections-deleting", 11), Self::ConnectionsDeleted => ("node-connections-deleted", 14), Self::Leaving => ("node-leaving", 12), Self::Left => ("node-left", 13), Self::FabricPrimary => ("new-fabric-primary", 15), Self::Restarting => ("node-restarting", 7) } }
}
fn reserved(kind: Kind) {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, &format!("rdm-reserved-{}",kind.named().0)).unwrap();
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
        let op = LifecycleOp { build_id: "reserved-receive".into(), attempt: 1, operation: "node.delete:mesh1.admin.2".into(), node_id: node_id.clone(), incarnation: incarnation.clone(), name: "mesh1.admin.2".parse().unwrap(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        let unknown = iroh::SecretKey::generate().public();
        let frame = match kind {
            Kind::ConnectionsDeleting => Frame::NodeConnectionsDeleting { op, forwarded_by: None },
            Kind::ConnectionsDeleted => Frame::NodeConnectionsDeleted { op, forwarded_by: None },
            Kind::Leaving => Frame::NodeLeaving { op, forwarded_by: None },
            Kind::Left => Frame::NodeLeft { op, forwarded_by: None },
            Kind::Restarting => Frame::NodeRestarting { op: LifecycleOp { operation: "node.restart:mesh1.admin.2".into(), ..op }, forwarded_by: None, publisher: { let a = crate::gossip_author(&fabric); rdm_core::PublisherId { node: "mesh1.admin.1".into(), incarnation: a.incarnation, node_id: a.node } }, topology_version: 2 },
            Kind::FabricPrimary => Frame::NewFabricPrimary { primary: Primary { mesh: "mesh2".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 99 }, endpoint_id: unknown.to_string(), transport_addr: "127.0.0.1:12009".parse().unwrap(), admin_api_base: Some("http://127.0.0.1:9".into()) },
        };
        assert_eq!(frame.clone().authored(&crate::gossip_author(&fabric), 0, 1).context().unwrap().kind, kind.type_name(), "the reserved frame travels as its reserved type");
        for (n, sender) in [&mesh_sender, &backbone_sender].into_iter().enumerate() {
            sender.broadcast_neighbors(frame.clone().authored(&crate::gossip_author(&fabric), 0, 1).encode().into()).await.unwrap();
            // A following frame on this same channel proves processing finished, without changing
            // the concerned mesh's status or relying on an arbitrary delay.
            let revision = n as u64 + 10;
            sender.broadcast_neighbors(Frame::MeshStatus { mesh: "barrier".into(), status: "ready".into(), publisher: "mesh1.admin.2".into(), forwarded_by: None, changed_at_rafka_ms: revision }.authored(&crate::gossip_author(&fabric), 0, 1).encode().into()).await.unwrap();
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
        assert!(receiver.location_view().addrs(unknown).is_empty(), "a reserved address declaration installs no contact");
        assert!(!receiver.digest_view.is_deleted(node_id.as_str()));
        assert!(receiver.digest_view.in_flight().is_empty());
        assert!(receiver.digest_view.routable(node_id.as_str()));
        send_ep.close().await; recv_ep.close().await;
    });
    let spans = capture.spans().unwrap();
    for s in spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.reject.via-reserved") { println!("{}", serde_json::to_string(s).unwrap()); }
    for via in ["mesh-channel", "backbone"] {
        assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.reject.via-reserved" && s["attributes"]["position"] == kind.named().1.to_string() && s["attributes"]["frame"] == kind.named().0 && s["attributes"]["via"] == via).count(), 1, "each channel refuses the reserved frame by name");
    }
    assert!(!spans.iter().any(|s| s["name"] == "rdm.mesh.topology.update.via-lifecycle-frame"), "reserved positions emit no ordinary apply span");
}

/// CONTRACT: reserved position 11 installs no fact.
#[test] fn reserved_connections_deleting() { reserved(Kind::ConnectionsDeleting); }
/// CONTRACT: reserved position 14 installs no completion.
#[test] fn reserved_connections_deleted() { reserved(Kind::ConnectionsDeleted); }
/// CONTRACT: reserved position 12 changes no lifecycle or routing state.
#[test] fn reserved_leaving() { reserved(Kind::Leaving); }
/// CONTRACT: reserved position 13 establishes no departure, exit or vacancy.
#[test] fn reserved_left() { reserved(Kind::Left); }
/// CONTRACT: reserved position 15 installs no role or endpoint.
#[test] fn reserved_fabric_primary() { reserved(Kind::FabricPrimary); }
/// CONTRACT (Q183, Luke 2026-10-26: node.restart and its NodeRestarting notice are removed): reserved
/// position 7 holds no birth, opens no overlay and changes no routing.
#[test] fn reserved_node_restarting() { reserved(Kind::Restarting); }
