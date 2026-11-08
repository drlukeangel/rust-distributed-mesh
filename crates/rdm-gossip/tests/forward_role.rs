//! Forwarding into a Mesh is done by that Mesh's current primary alone (gossip.md §2 rules: "only
//! receiving peer mesh primaries forward remote mesh topologies locally").
//!
//! CONTRACT: a node-admin of mesh1 that is not its primary hears a peer mesh's topology on the
//! backbone and holds each new version of it in its one copy, and puts nothing of it onto mesh1's
//! channel: no forwarded full, no forwarded delta, and a member reached only through it holds no
//! version of that source. The same admin given the role puts nothing into mesh1 at the takeover
//! (gossip.md §3.3, Q409: a takeover tops up nothing); at the peer mesh's next move it forwards the
//! delta its full wrote, from the version its copy held. The member, given nothing of the peer mesh
//! by any primary, holds no base: it desynchronizes the source, and its top-up read of its primary
//! holds what the primary's copy holds. The channel carried nothing before because the admin held no
//! role.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_gossip::location::learn_addresses;
use rdm_gossip::backbone::Backbone;
use rdm_gossip::topology::TopologyView;
use serde_json::Value;
use std::time::Duration;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn member(ordinal: u32, node_id: &NodeId, birth: &IncarnationId, seq: u64, status: NodeStatus) -> MeshDigest {
    MeshDigest { 
        fabric_id: fabric(),
        node: MeshNode { mesh: "mesh2".into(), r#type: "node".into(),
            node_id: node_id.clone(),
            path: "mesh2.node".into(), name: (ordinal.to_string()).to_string(),
            endpoint_id: EndpointId(format!("key{ordinal}")),
            transport_addr: format!("127.0.0.1:{}", 41_000 + ordinal).parse().unwrap(),
            incarnation: birth.clone(),
            supersedes: None,
        },
        status,
        digest_seq: seq,
        emitted_at_rafka_ms: 1_000 + seq,
        primary: Vec::new(),
    }
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

fn attr(sp: &Value, k: &str) -> String {
    sp["attributes"][k].as_str().unwrap_or_default().to_string()
}

#[test]
fn an_admin_that_is_not_its_meshs_primary_forwards_nothing_into_it() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-forward-role").expect("the cell's evidence file can be created");
    capture.run(4, async {
        // mesh2's primary publishes on the backbone; mesh1.node-admin.2 (no role) hears it there; mesh1.node.1
        // is on mesh1's channel through mesh1.node-admin.2 alone.
        let (src, admin, rpc) = (node().await, node().await, node().await);
        let (src_addr, admin_addr) = (addr_of(&src.ep), addr_of(&admin.ep));
        learn_addresses(&admin.ep, &[src_addr.clone()]).unwrap();
        learn_addresses(&rpc.ep, &[admin_addr.clone()]).unwrap();
        let fab = fabric();
        let (mesh1, mesh2) = (MeshId::mint(), MeshId::mint());
        let clock = rdm_transport::clock::os_clock;
        let src_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh2", &mesh2, "mesh2.node-admin.1", clock(), Default::default());
        let src_m_channels = rdm_gossip::channel::Channels::new(&src.gossip, &src.ep, &src_m).unwrap();
        src_m_channels.join(rdm_gossip::channel::ChannelId::mesh(src_m.fabric_id(), src_m.mesh_id()), vec![]).await.unwrap();
        let admin_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh1, "mesh1.node-admin.2", clock(), Default::default());
        let admin_m_channels = rdm_gossip::channel::Channels::new(&admin.gossip, &admin.ep, &admin_m).unwrap();
        admin_m_channels.join(rdm_gossip::channel::ChannelId::mesh(admin_m.fabric_id(), admin_m.mesh_id()), vec![]).await.unwrap();
        let rpc_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh1, "mesh1.node.1", clock(), Default::default());
        let rpc_m_channels = rdm_gossip::channel::Channels::new(&rpc.gossip, &rpc.ep, &rpc_m).unwrap();
        rpc_m_channels.join(rdm_gossip::channel::ChannelId::mesh(rpc_m.fabric_id(), rpc_m.mesh_id()), vec![admin_addr.clone()]).await.unwrap();
        src_m_channels.set_publisher(&rdm_core::PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() });
        src_m_channels.join(rdm_gossip::channel::ChannelId::backbone(src_m.fabric_id()), vec![]).await.unwrap();
        let src_bb = src_m_channels.backbone().expect("the backbone is joined");
        admin_m_channels.set_publisher(&rdm_core::PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() });
        admin_m_channels.join(rdm_gossip::channel::ChannelId::backbone(admin_m.fabric_id()), vec![src_addr.clone()]).await.unwrap();
        let admin_bb = admin_m_channels.backbone().expect("the backbone is joined");
        src_bb.set_mesh_primary(true);
        assert!(!admin_bb.is_mesh_primary(), "mesh1.node-admin.2 holds no role");
        rpc_m_channels.wait_for_neighbour().await;

        let ids: Vec<(NodeId, IncarnationId)> = (0..3).map(|_| (NodeId::mint(), IncarnationId::mint())).collect();
        let at = |seq: u64, moved: bool| -> Vec<MeshDigest> {
            ids.iter().enumerate().map(|(k, (id, b))| member(k as u32 + 1, id, b, seq, if moved && k == 0 { NodeStatus::ConnectionsDeleting } else { NodeStatus::Ready })).collect()
        };
        // Publications until the non-primary admin holds the source, then a change until it holds the next
        // version: it hears both on the backbone.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut seq = 0;
        let v1 = loop {
            seq += 1;
            src_bb.publish(&src_m, at(seq, false)).await;
            if let Some((_, v)) = admin_bb.held_source("mesh2") {
                break v;
            }
            assert!(std::time::Instant::now() < deadline, "mesh1.node-admin.2 never held mesh2 from the backbone");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let v2 = loop {
            seq += 1;
            src_bb.publish(&src_m, at(seq, true)).await;
            if let Some((_, v)) = admin_bb.held_source("mesh2").filter(|(_, v)| *v > v1) {
                break v;
            }
            assert!(std::time::Instant::now() < deadline, "mesh1.node-admin.2 never held mesh2's next version");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        // Its own publications publish nothing (no role).
        assert!(!admin_bb.publish(&admin_m, vec![]).await, "an admin with no role publishes no mesh topology");

        // The same admin given the role puts nothing into mesh1 at the takeover: a republication of the
        // version its copy holds moves nothing (Q409). The spans below are the fact: its first forward
        // is mesh2's next move, v2 to v3, and none comes before the role.
        admin_bb.set_mesh_primary(true);
        seq += 1;
        src_bb.publish(&src_m, at(seq, true)).await;
        admin_bb.publish(&admin_m, vec![]).await;
        // mesh2 moves: the new primary forwards the delta its full wrote, from v2, the version its copy
        // held. The member holds no base: it desynchronizes mesh2.
        let v3 = loop {
            seq += 1;
            src_bb.publish(&src_m, at(seq, false)).await;
            admin_bb.publish(&admin_m, vec![]).await;
            if rpc_m.desynced_sources().iter().any(|(m, _)| m == "mesh2") {
                break admin_bb.held_source("mesh2").map(|(_, v)| v).expect("the primary's copy of mesh2");
            }
            assert!(std::time::Instant::now() < deadline, "the member never took mesh2's next move once mesh1.node-admin.2 held the role");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert!(v3 > v2, "mesh2 moved past v{v2}: v{v3}");
        // Its top-up: a topology read of its primary (op 0x1E), which serves mesh2 from its copy.
        let served = admin_m.held_topology_of(&at(seq, false)[0], Some("mesh2"));
        for s in &served {
            let chunk = rdm_gossip::aggregate::Chunk { mesh: s.mesh.clone(), mesh_id: s.mesh_id.clone(), author: None, topology_version: s.topology_version, time_ms: 1, chunk_index: 0, chunk_count: 1, digests: s.digests.clone(), runtimes: Vec::new() };
            let _ = rpc_m_channels.take_read_chunk(chunk, "mesh1");
        }
        assert_eq!(rpc_m.held_source_version("mesh2").map(|(_, v)| v), Some(v3), "the member's read holds what its primary's copy holds");
        assert!(rpc_m.desynced_sources().is_empty(), "the read resumed the source");

        for n in [&src, &admin, &rpc] {
            n.ep.close().await;
        }
        let all = capture.spans().expect("the cell's spans reached its evidence file");
        let role: Vec<&Value> = all.iter().filter(|s| s["name"] == "rdm.mesh.backbone.update.via-forwarder" && attr(s, "node") == "mesh1.node-admin.2" && attr(s, "role") == "start").collect();
        assert_eq!(role.len(), 1, "the forwarder role started once, when the role was given");
        let started = role[0]["start_unix_nano"].as_u64().unwrap();
        let forwards: Vec<(u64, String)> = all
            .iter()
            .filter(|s| s["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.topology.update.via-forwarded-")) && attr(s, "node") == "mesh1.node-admin.2")
            .map(|s| (s["start_unix_nano"].as_u64().unwrap(), s["name"].as_str().unwrap().to_string()))
            .collect();
        assert!(!forwards.is_empty(), "the mesh-primary forwarded");
        assert!(forwards.iter().all(|(t, _)| *t >= started), "nothing was forwarded before the role: {forwards:?}");
        let first: Vec<(String, String)> = all
            .iter()
            .filter(|s| s["name"] == "rdm.mesh.topology.update.via-forwarded-delta" && attr(s, "node") == "mesh1.node-admin.2" && attr(s, "mesh") == "mesh2")
            .map(|s| (attr(s, "base_version"), attr(s, "topology_version")))
            .collect();
        assert_eq!(first.first(), Some(&(v2.to_string(), v3.to_string())), "the new primary's first differential of mesh2 is from the version its copy held: {first:?}");
        eprintln!("forward-role: trace of the role start {} span {}; forwards {:?}", role[0]["trace_id"], role[0]["span_id"], forwards.iter().map(|(_, n)| n).collect::<Vec<_>>());
    });
}
