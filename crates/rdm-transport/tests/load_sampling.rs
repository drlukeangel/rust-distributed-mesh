//! Each published digest carries the process's CPU and RAM against the host or budget ceilings,
//! and the backbone's `MeshTopology` snapshot retains those loads while the forwarding primary's
//! cross-mesh projection omits them.
//!
//! CONTRACT: `docs/architecture/gossip.md:151` "the complete Mesh with loads: what every node-admin
//! holds of every Mesh for routing and control decisions" and `docs/architecture/gossip.md:152`
//! "transforms the held remote projection from `base_version` into `topology_version`: births added
//! or changed, identities removed, overlay and deleted changes, loads omitted."
//! `crates/rdm-transport/src/load.rs:4-6` "The ceilings are the host's cores and memory unless the
//! process was given a budget (`RDM_CPU_BUDGET_MILLICORES`, `RDM_RAM_BUDGET_BYTES`)."

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, NodeLoad, PublisherId};
use rdm_transport::topology::{learn_addresses, Backbone, TopologyView};
use serde_json::Value;
use std::time::{Duration, Instant};

fn digest(name: &str, ep: &iroh::Endpoint) -> MeshDigest {
    let port = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap().port();
    MeshDigest {
        held_hash: Default::default(),
        fabric_id: FabricId::parse("fab000000001").unwrap(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(ep.id().to_string()),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::Ready,
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

struct Node {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(
        iroh::SecretKey::generate(),
        "127.0.0.1:0".parse().unwrap(),
        vec![iroh_gossip::ALPN.to_vec()],
        transport,
    )
    .await
    .unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, _router: router, gossip }
}

fn addr_of(ep: &iroh::Endpoint) -> iroh::EndpointAddr {
    let a = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    iroh::EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], a.port())))
}

/// CONTRACT: `docs/architecture/gossip.md:151` "the complete Mesh with loads: what every node-admin
/// holds of every Mesh for routing and control decisions" and `docs/architecture/gossip.md:574`
/// "Loads are omitted from the ordinary-node cross-Mesh projection, not from the protocol. They
/// remain on the backbone."
/// When `RDM_CPU_BUDGET_MILLICORES` and `RDM_RAM_BUDGET_BYTES` are set in the process environment,
/// `TopologyView::publish` stamps the sampled `NodeLoad` with those budget ceilings and positive
/// resident memory, the backbone `MeshTopology` snapshot retains that load for the receiving
/// node-admin, and the forwarded cross-mesh projection omits it for an ordinary member.
#[test]
fn published_digest_carries_process_load_with_configured_budgets_to_backbone_and_omits_it_for_remote_members() {
    if crate::own_process::delegated_with_env(
        module_path!(),
        "published_digest_carries_process_load_with_configured_budgets_to_backbone_and_omits_it_for_remote_members",
        &[
            ("RDM_CPU_BUDGET_MILLICORES", Some(std::ffi::OsStr::new("3500"))),
            ("RDM_RAM_BUDGET_BYTES", Some(std::ffi::OsStr::new("536870912"))),
        ],
    ) {
        return;
    }
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-transport-load-sampling")
        .expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (src_node, fwd_node, mem_node) = (node().await, node().await, node().await);
        let (src_addr, fwd_addr) = (addr_of(&src_node.ep), addr_of(&fwd_node.ep));
        learn_addresses(&fwd_node.ep, &[src_addr.clone()]).unwrap();
        learn_addresses(&mem_node.ep, &[fwd_addr.clone()]).unwrap();

        let fab = FabricId::parse("fab000000001").unwrap();
        let (mesh1_id, mesh2_id) = (MeshId::mint(), MeshId::mint());
        let src_m = TopologyView::join(&src_node.gossip, &src_node.ep, &fab, "mesh2", &mesh2_id, "mesh2.admin.1", rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        let fwd_m = TopologyView::join(&fwd_node.gossip, &fwd_node.ep, &fab, "mesh1", &mesh1_id, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        let mem_m = TopologyView::join(&mem_node.gossip, &mem_node.ep, &fab, "mesh1", &mesh1_id, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![fwd_addr.clone()], &[]).await.unwrap();

        let src_digest = digest("mesh2.admin.1", &src_node.ep);
        let src_pub = PublisherId {
            node: "mesh2.admin.1".to_string(),
            incarnation: src_digest.node.incarnation.clone(),
            node_id: src_digest.node.node_id.clone(),
        };
        let fwd_digest = digest("mesh1.admin.1", &fwd_node.ep);
        let fwd_pub = PublisherId {
            node: "mesh1.admin.1".to_string(),
            incarnation: fwd_digest.node.incarnation.clone(),
            node_id: fwd_digest.node.node_id.clone(),
        };

        let src_bb = Backbone::join(&src_node.gossip, &src_node.ep, &src_m, "mesh2", src_pub, vec![]).await.unwrap();
        let fwd_bb = Backbone::join(&fwd_node.gossip, &fwd_node.ep, &fwd_m, "mesh1", fwd_pub, vec![src_addr]).await.unwrap();
        src_bb.set_mesh_primary(true);
        fwd_bb.set_mesh_primary(true);

        src_m.publish(&src_digest).await.unwrap();
        let own_held = src_m
            .digest_view
            .get(src_digest.node.node_id.as_str())
            .expect("the publisher holds its own digest")
            .0;
        let sampled: NodeLoad = own_held.load.expect("published digest carries sampled NodeLoad");
        assert_eq!(sampled.cpu_budget_millicores, 3500, "{sampled:?}");
        assert_eq!(sampled.ram_budget_bytes, 536_870_912, "{sampled:?}");
        assert!(sampled.ram_used_bytes > 0, "{sampled:?}");

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let members = src_m.digest_view.current(src_m.digest_view.staleness_floor());
            src_bb.publish(&src_m, members).await;
            fwd_bb.forward_fulls().await;
            if fwd_bb.held_source("mesh2").is_some() && mem_m.held_source_version("mesh2").is_some() {
                break;
            }
            assert!(Instant::now() < deadline, "timed out waiting for backbone and member snapshots");
        }

        let admin_seen = fwd_m
            .digest_view
            .get(src_digest.node.node_id.as_str())
            .expect("receiving node-admin holds the remote member from the backbone")
            .0;
        let admin_load = admin_seen.load.expect("backbone MeshTopology retains the member's NodeLoad");
        assert_eq!(admin_load.cpu_budget_millicores, 3500, "{admin_load:?}");
        assert_eq!(admin_load.ram_budget_bytes, 536_870_912, "{admin_load:?}");
        assert_eq!(admin_load.ram_used_bytes, sampled.ram_used_bytes, "{admin_load:?}");

        let member_seen = mem_m
            .digest_view
            .get(src_digest.node.node_id.as_str())
            .expect("ordinary member holds the remote member from the forwarded snapshot")
            .0;
        assert!(member_seen.load.is_none(), "forwarded cross-mesh projection omits loads: {member_seen:?}");

        drop(src_bb);
        drop(fwd_bb);
        for n in [&src_node, &fwd_node, &mem_node] {
            n.ep.close().await;
        }

        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let installs: Vec<&Value> = spans
            .iter()
            .filter(|s| s["name"] == "rdm.mesh.topology.update.via-snapshot-installed")
            .collect();
        assert!(
            installs.iter().any(|s| s["attributes"]["node"] == "mesh1.admin.1" && s["attributes"]["channel"] == "backbone"),
            "backbone snapshot install span recorded on mesh1.admin.1: {installs:?}"
        );
        assert!(
            installs.iter().any(|s| s["attributes"]["node"] == "mesh1.rpc.1" && s["attributes"]["channel"] == "mesh-channel"),
            "forwarded snapshot install span recorded on mesh1.rpc.1: {installs:?}"
        );
    });
}
