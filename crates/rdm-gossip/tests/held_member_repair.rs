//! Held-member repair (gossip.md §6) reaches every member a node holds, heard or not.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, PublisherId};
use rdm_gossip::channel::{ChannelId, Channels};
use rdm_gossip::topology::TopologyView;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(30);

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

fn birth(fabric: &FabricId, n: &Node, name: &str) -> MeshDigest {
    let port = n.ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap().port();
    MeshDigest {
        fabric_id: fabric.clone(),
        node: MeshNode {
            mesh: "mesh1".to_owned(),
            r#type: "node".to_string(),
            node_id: NodeId::mint(),
            path: "mesh1.node".into(),
            name: name.into(),
            endpoint_id: EndpointId(n.ep.id().to_string()),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        is_primary: None,
        is_fabric_primary: None,
    }
}

/// CONTRACT (gossip.md §6: "member is still held/current + EndpointId/address are known + this
/// member/remote coverage has gone stale for the repair window -> bounded join/refeed attempt to that
/// held member"): a node holds a member only from its mesh-primary's projection and has never heard
/// it. The member's launcher, the one peer it knew, is gone (the launch's seed is the launcher alone,
/// Q382), so it has no neighbour and names no one who can hear it. The holder has a neighbour, as a new
/// mesh-primary does, so no zero-neighbour fallback runs. Past the repair window the holder's repair
/// joins the member, the two are neighbours, and the member's digest is heard. Must NOT happen: a held
/// member its holder knows the address of left unreached because no digest of it was ever heard (the
/// 30 s `WaitForNodeReady` "no digest" of the new mesh-primary).
#[test]
fn a_member_held_only_from_the_projection_and_never_heard_is_repaired() {
    if crate::own_process::delegated(module_path!(), "a_member_held_only_from_the_projection_and_never_heard_is_repaired") {
        return;
    }
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "held-member-repair").unwrap();
    capture.run(4, async {
        let (holder, neighbour, member) = (node().await, node().await, node().await);
        let fabric = FabricId::parse("fab000000001").unwrap();
        let mesh_id = MeshId::mint();

        let holder_view = TopologyView::new(&fabric, "mesh1", &mesh_id, "mesh1.node-admin.1", rdm_transport::clock::os_clock(), Default::default());
        let holder_channels = Channels::new(&holder.gossip, &holder.ep, &holder_view).unwrap();
        holder_channels.join(ChannelId::mesh(&fabric, &mesh_id), vec![]).await.unwrap();

        // The holder's neighbour: it is not alone on the channel.
        let neighbour_view = TopologyView::new(&fabric, "mesh1", &mesh_id, "mesh1.node.1", rdm_transport::clock::os_clock(), Default::default());
        let neighbour_channels = Channels::new(&neighbour.gossip, &neighbour.ep, &neighbour_view).unwrap();
        let holder_port = holder.ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap().port();
        let holder_addr = iroh::EndpointAddr::new(holder.ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], holder_port)));
        neighbour_channels.join(ChannelId::mesh(&fabric, &mesh_id), vec![holder_addr]).await.unwrap();
        tokio::time::timeout(BOUND, holder_channels.wait_for_neighbour()).await.expect("the holder has its neighbour");

        let held = birth(&fabric, &member, "4");
        let member_view = TopologyView::new(&fabric, "mesh1", &mesh_id, held.node.node_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let member_channels = Channels::new(&member.gossip, &member.ep, &member_view).unwrap();
        member_channels.set_author(&PublisherId { node_id: held.node.node_id.clone(), incarnation: held.node.incarnation.clone() }, None);
        member_channels.join(ChannelId::mesh(&fabric, &mesh_id), vec![]).await.unwrap();

        holder_view.digest_view.hold_projected_at(&held, Instant::now());
        assert_eq!(member_channels.neighbours(), 0, "the member knows of no one to dial");

        let deadline = Instant::now() + BOUND;
        loop {
            member_channels.publish(&held).await.unwrap();
            if holder_view.digest_view.get(held.node.node_id.as_str()).is_some() {
                break;
            }
            assert!(Instant::now() < deadline, "the holder's repair never reached the member it holds only from the projection: neighbours {} and {}", holder_channels.neighbours(), member_channels.neighbours());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(member_channels.neighbours() >= 1, "the member the repair joined has a neighbour");
        assert!(holder_view.digest_view.projected_unheard_at(Instant::now()).is_empty(), "heard: it is held by its own digest now");

        let spans = capture.spans().unwrap();
        let repairs: Vec<_> = spans
            .iter()
            .filter(|s| s["name"] == "rdm.mesh.connection.update.via-refeed" && s["attributes"]["reason"] == "stale-held-member" && s["attributes"]["node"] == "mesh1.node-admin.1" && s["attributes"]["peer_node_id"] == held.node.node_id.as_str())
            .collect();
        assert_eq!(repairs.len(), 1, "one bounded attempt reached it: {repairs:#?}");
        drop(neighbour_channels);
    });
}
