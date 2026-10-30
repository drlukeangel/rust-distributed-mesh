//! Forwarding into a Mesh is done by that Mesh's current primary alone (gossip.md §2 rules: "only
//! receiving peer mesh primaries forward remote mesh topologies locally").
//!
//! CONTRACT: a node-admin of mesh1 that is not its primary hears a peer mesh's topology on the
//! backbone and holds each new version of it, and puts nothing of it onto mesh1's channel: no
//! forwarded full, no forwarded delta, and a member reached only through it holds no version of
//! that source. The same admin given the role forwards at its next publication, and the member installs
//! the source: the channel carried nothing before because the admin held no role.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::{learn_addresses, Backbone, TopologyView};
use serde_json::Value;
use std::time::Duration;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn member(ordinal: u32, node_id: &NodeId, birth: &IncarnationId, seq: u64, status: MemberStatus) -> MeshDigest {
    MeshDigest { held_hash: Default::default(),
        fabric_id: fabric(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: format!("mesh2.rpc.{ordinal}").parse().unwrap(),
            endpoint_id: EndpointId(format!("key{ordinal}")),
            transport_addr: format!("127.0.0.1:{}", 41_000 + ordinal).parse().unwrap(),
            incarnation: birth.clone(),
            supersedes: None,
            runtime: None,
        },
        status,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: seq,
        emitted_at_rafka_ms: 1_000 + seq,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
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
        // mesh2's primary publishes on the backbone; mesh1.admin.2 (no role) hears it there; mesh1.rpc.1
        // is on mesh1's channel through mesh1.admin.2 alone.
        let (src, admin, rpc) = (node().await, node().await, node().await);
        let (src_addr, admin_addr) = (addr_of(&src.ep), addr_of(&admin.ep));
        learn_addresses(&admin.ep, &[src_addr.clone()]).unwrap();
        learn_addresses(&rpc.ep, &[admin_addr.clone()]).unwrap();
        let fab = fabric();
        let (mesh1, mesh2) = (MeshId::mint(), MeshId::mint());
        let clock = rdm_transport::clock::os_clock;
        let src_m = TopologyView::join(&src.gossip, &src.ep, &fab, "mesh2", &mesh2, "mesh2.admin.1", clock(), vec![], &[]).await.unwrap();
        let admin_m = TopologyView::join(&admin.gossip, &admin.ep, &fab, "mesh1", &mesh1, "mesh1.admin.2", clock(), vec![], &[]).await.unwrap();
        let rpc_m = TopologyView::join(&rpc.gossip, &rpc.ep, &fab, "mesh1", &mesh1, "mesh1.rpc.1", clock(), vec![admin_addr.clone()], &[]).await.unwrap();
        let src_bb = Backbone::join(&src.gossip, &src.ep, &src_m, "mesh2", rdm_core::PublisherId { node: "mesh2.admin.1".to_string(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }, vec![]).await.unwrap();
        let admin_bb = Backbone::join(&admin.gossip, &admin.ep, &admin_m, "mesh1", rdm_core::PublisherId { node: "mesh1.admin.2".to_string(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }, vec![src_addr.clone()]).await.unwrap();
        src_bb.set_mesh_primary(true);
        assert!(!admin_bb.is_mesh_primary(), "mesh1.admin.2 holds no role");
        rpc_m.wait_for_neighbour().await;

        let ids: Vec<(NodeId, IncarnationId)> = (0..3).map(|_| (NodeId::mint(), IncarnationId::mint())).collect();
        let at = |seq: u64, moved: bool| -> Vec<MeshDigest> {
            ids.iter().enumerate().map(|(k, (id, b))| member(k as u32 + 1, id, b, seq, if moved && k == 0 { MemberStatus::ConnectionsDeleting } else { MemberStatus::Ready })).collect()
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
            assert!(std::time::Instant::now() < deadline, "mesh1.admin.2 never held mesh2 from the backbone");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let v2 = loop {
            seq += 1;
            src_bb.publish(&src_m, at(seq, true)).await;
            if let Some((_, v)) = admin_bb.held_source("mesh2").filter(|(_, v)| *v > v1) {
                break v;
            }
            assert!(std::time::Instant::now() < deadline, "mesh1.admin.2 never held mesh2's next version");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        // Its own publications publish nothing (no role), and its mesh holds nothing of mesh2.
        for _ in 0..5 {
            assert!(!admin_bb.publish(&admin_m, vec![]).await, "an admin with no role publishes no mesh topology");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(rpc_m.held_source_version("mesh2"), None, "a member reached only through a non-primary admin holds no version of mesh2 (it holds v{v2})");

        // The same admin given the role forwards at its next publication: the member installs mesh2.
        admin_bb.set_mesh_primary(true);
        let installed = loop {
            seq += 1;
            src_bb.publish(&src_m, at(seq, true)).await;
            admin_bb.publish(&admin_m, vec![]).await;
            if let Some((_, v)) = rpc_m.held_source_version("mesh2") {
                break v;
            }
            assert!(std::time::Instant::now() < deadline, "the member never installed mesh2 once mesh1.admin.2 held the role");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(installed, v2);

        for n in [&src, &admin, &rpc] {
            n.ep.close().await;
        }
        let all = capture.spans().expect("the cell's spans reached its evidence file");
        let role: Vec<&Value> = all.iter().filter(|s| s["name"] == "rdm.mesh.backbone.update.via-forwarder" && attr(s, "node") == "mesh1.admin.2" && attr(s, "role") == "start").collect();
        assert_eq!(role.len(), 1, "the forwarder role started once, when the role was given");
        let started = role[0]["start_unix_nano"].as_u64().unwrap();
        let forwards: Vec<(u64, String)> = all
            .iter()
            .filter(|s| s["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.topology.update.via-forwarded-")) && attr(s, "node") == "mesh1.admin.2")
            .map(|s| (s["start_unix_nano"].as_u64().unwrap(), s["name"].as_str().unwrap().to_string()))
            .collect();
        assert!(!forwards.is_empty(), "the mesh-primary forwarded");
        assert!(forwards.iter().all(|(t, _)| *t >= started), "nothing was forwarded before the role: {forwards:?}");
        eprintln!("forward-role: trace of the role start {} span {}; forwards {:?}", role[0]["trace_id"], role[0]["span_id"], forwards.iter().map(|(_, n)| n).collect::<Vec<_>>());
    });
}
