//! Each process samples its own CPU and RAM against the host or budget ceilings, in memory, for its
//! own `node.get`; no load rides gossip: neither its Digest nor the backbone's `TopologyFull` nor the
//! forwarding primary's differential carries one (Q394).
//!
//! CONTRACT: `docs/architecture/lifecycles/node-identity-and-state.md` §2.5 "Load and the gossip
//! counters ... are in-memory diagnostics of the node itself: never gossiped (Q394), never written to
//! the node record, storage or durable history; `node.get` serves them only for the node that answers
//! it." `crates/rdm-transport/src/load.rs:4-6` "The ceilings are the host's cores and memory unless the
//! process was given a budget (`RDM_CPU_BUDGET_MILLICORES`, `RDM_RAM_BUDGET_BYTES`)."

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId, NodeLoad, PublisherId};
use rdm_gossip::location::learn_addresses;
use rdm_gossip::backbone::Backbone;
use rdm_gossip::topology::TopologyView;
use serde_json::Value;
use std::time::{Duration, Instant};

fn digest(name: rdm_core::PathName, mesh: &str, r#type: &str, ep: &iroh::Endpoint) -> MeshDigest {
    let port = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap().port();
    MeshDigest {
        fabric_id: FabricId::parse("fab000000001").unwrap(),
        node: MeshNode { mesh: mesh.to_owned(), r#type: r#type.to_string(),
            node_id: NodeId::mint(),
            path: name.path.clone(), name: name.name.clone(),
            endpoint_id: EndpointId(ep.id().to_string()),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
        },
        status: NodeStatus::Ready,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
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

/// CONTRACT (node-identity-and-state.md §2.5, Q394): when `RDM_CPU_BUDGET_MILLICORES` and
/// `RDM_RAM_BUDGET_BYTES` are set in the process environment, the node's own sample (`own_sample`, what
/// its `node.get` serves) carries those budget ceilings and positive resident memory; the node's
/// Digest, the backbone `TopologyFull` that reaches the receiving node-admin and the forwarded
/// differential that reaches an ordinary member deliver the node without any load. Must NOT happen:
/// a remote node's load held from gossip.
#[test]
fn a_node_samples_its_own_load_with_configured_budgets_and_gossip_delivers_it_without_load() {
    if crate::own_process::delegated_with_env(
        module_path!(),
        "a_node_samples_its_own_load_with_configured_budgets_and_gossip_delivers_it_without_load",
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
        let src_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh2", &mesh2_id, "mesh2.node-admin.1", rdm_transport::clock::os_clock(), Default::default());
        let src_m_channels = rdm_gossip::channel::Channels::new(&src_node.gossip, &src_node.ep, &src_m).unwrap();
        src_m_channels.join(rdm_gossip::channel::ChannelId::mesh(src_m.fabric_id(), src_m.mesh_id()), vec![]).await.unwrap();
        let fwd_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh1_id, "mesh1.node-admin.1", rdm_transport::clock::os_clock(), Default::default());
        let fwd_m_channels = rdm_gossip::channel::Channels::new(&fwd_node.gossip, &fwd_node.ep, &fwd_m).unwrap();
        fwd_m_channels.join(rdm_gossip::channel::ChannelId::mesh(fwd_m.fabric_id(), fwd_m.mesh_id()), vec![]).await.unwrap();
        let mem_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &mesh1_id, "mesh1.node.1", rdm_transport::clock::os_clock(), Default::default());
        let mem_m_channels = rdm_gossip::channel::Channels::new(&mem_node.gossip, &mem_node.ep, &mem_m).unwrap();
        mem_m_channels.join(rdm_gossip::channel::ChannelId::mesh(mem_m.fabric_id(), mem_m.mesh_id()), vec![fwd_addr.clone()]).await.unwrap();

        let src_digest = digest(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", &src_node.ep);
        let src_pub = PublisherId {
            incarnation: src_digest.node.incarnation.clone(),
            node_id: src_digest.node.node_id.clone(),
        };
        let fwd_digest = digest(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", &fwd_node.ep);
        let fwd_pub = PublisherId {
            incarnation: fwd_digest.node.incarnation.clone(),
            node_id: fwd_digest.node.node_id.clone(),
        };

        src_m_channels.set_publisher(&src_pub);
        src_m_channels.join(rdm_gossip::channel::ChannelId::backbone(src_m.fabric_id()), vec![]).await.unwrap();
        let src_bb = src_m_channels.backbone().expect("the backbone is joined");
        fwd_m_channels.set_publisher(&fwd_pub);
        fwd_m_channels.join(rdm_gossip::channel::ChannelId::backbone(fwd_m.fabric_id()), vec![src_addr]).await.unwrap();
        let fwd_bb = fwd_m_channels.backbone().expect("the backbone is joined");
        // The forwarder sends each differential once: the member is its neighbour before mesh2 is published.
        fwd_m_channels.wait_for_neighbour().await;
        mem_m_channels.wait_for_neighbour().await;
        src_bb.set_mesh_primary(true);
        fwd_bb.set_mesh_primary(true);

        src_m_channels.publish(&src_digest).await.unwrap();
        assert!(src_m.digest_view.get(src_digest.node.node_id.as_str()).is_some(), "the publisher holds its own digest");
        let (sampled, _): (NodeLoad, _) = src_m_channels.own_sample();
        assert_eq!(sampled.cpu_budget_millicores, 3500, "{sampled:?}");
        assert_eq!(sampled.ram_budget_bytes, 536_870_912, "{sampled:?}");
        assert!(sampled.ram_used_bytes > 0, "{sampled:?}");

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let members = src_m.digest_view.current(src_m.digest_view.staleness_floor());
            src_bb.publish(&src_m, members).await;
            if fwd_bb.held_source("mesh2").is_some() && mem_m.held_source_version("mesh2").is_some() {
                break;
            }
            assert!(Instant::now() < deadline, "timed out waiting for backbone and member aggregates");
        }

        // The node reaches both with its identity and status, and no load: a held digest has no place
        // for one, and the frames that carried it have none (gossip.md §3.1).
        let admin_seen = fwd_m
            .digest_view
            .get(src_digest.node.node_id.as_str())
            .expect("receiving node-admin holds the remote member from the backbone")
            .0;
        assert_eq!((admin_seen.node.incarnation.clone(), admin_seen.status), (src_digest.node.incarnation.clone(), NodeStatus::Ready), "{admin_seen:?}");
        let member_seen = mem_m
            .digest_view
            .get(src_digest.node.node_id.as_str())
            .expect("ordinary member holds the remote member from the forwarded differential")
            .0;
        assert_eq!((member_seen.node.incarnation.clone(), member_seen.status), (src_digest.node.incarnation.clone(), NodeStatus::Ready), "{member_seen:?}");

        drop(src_bb);
        drop(fwd_bb);
        for n in [&src_node, &fwd_node, &mem_node] {
            n.ep.close().await;
        }

        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let installs: Vec<&Value> = spans
            .iter()
            .filter(|s| s["name"] == "rdm.mesh.topology.update.via-aggregate-installed")
            .collect();
        assert!(
            installs.iter().any(|s| s["attributes"]["node"] == "mesh1.node-admin.1" && s["attributes"]["channel"] == "backbone"),
            "backbone aggregate install span recorded on mesh1.node-admin.1: {installs:?}"
        );
        let applied: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-delta").collect();
        assert!(
            applied.iter().any(|s| s["attributes"]["node"] == "mesh1.node.1" && s["attributes"]["mesh"] == "mesh2" && s["attributes"]["base_version"] == "0"),
            "the forwarded differential of mesh2, from 0, applied on mesh1.node.1: {applied:?}"
        );
    });
}
