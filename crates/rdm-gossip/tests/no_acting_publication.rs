//! i143.e12.s53 functional (R-RX4): nobody publishes a live mesh on its behalf. Only a live mesh's
//! own mesh-primary publishes that mesh on the backbone; a live mesh with no primary goes unheard,
//! and its new mesh-primary publishes once it is established. The fabric-primary executes the stops
//! and deletes of a live peer mesh's nodes like any executor, and none of it makes it that mesh's
//! publisher: its publication has no mesh topology of that mesh to put out, so nothing wakes it.
//! (A deleted mesh's topology from its last admin's stop on is `deleted_mesh_aggregate`'s.)

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_gossip::backbone::Backbone;
use rdm_gossip::topology::TopologyView;
use rdm_gossip::status::StatusEvent;
use serde_json::Value;
use std::time::{Duration, Instant};

const CELL: &str = "the_fabric_primary_executing_a_peer_meshs_stop_and_delete_publishes_nothing_for_that_mesh";

/// The anti-entropy interval the fabric-primary's publication waits on: the wake the cell reads is the
/// publication returning at this deadline and no earlier.
const ANTI_ENTROPY: Duration = Duration::from_millis(1500);

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
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
            runtime: None,
        },
        status: NodeStatus::Ready,
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 1_000,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

fn op(kind: &str, d: &MeshDigest) -> LifecycleOp {
    LifecycleOp { r#type: d.node.r#type.clone(), mesh: d.node.mesh.clone(),
        build_id: "b-s53".into(),
        operation: rdm_core::OperationId::new(kind, rdm_core::OperationSubject::Birth { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        node_id: d.node.node_id.clone(),
        incarnation: d.node.incarnation.clone(),
        path: d.node.path.clone(), name: d.node.name.clone(),
        event_at_rafka_ms: 2_000,
        traceparent: None,
        tracestate: None,
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

/// CONTRACT (R-RX4: "Only a mesh's own mesh-primary publishes that mesh on the backbone. The
/// fabric-primary never publishes a mesh as its acting primary"): the fabric-primary mesh1.node-admin.1,
/// its view naming mesh2 live, holds mesh2's last admin and one rpc member, and runs its publication
/// publication as the node-admin does (publish its own mesh, then wait for the publication's wake with a 1.5 s
/// anti-entropy deadline).
/// While the publication waits, it executes the stop of mesh2.node.1 (NodeStopped) and the delete of
/// mesh2.node-admin.1 (the departure its view takes). Neither is a change of mesh1's own projection, so
/// the publication returns at its deadline and no earlier; the NodeStopped is stamped by the executor
/// role with the version the fabric-primary holds, never an acting counter of mesh2; and no span
/// names the fabric-primary an acting primary. Must NOT happen: the executed stop or delete waking
/// the publication to publish a mesh topology of mesh2, whose primary is gone.
#[test]
fn the_fabric_primary_executing_a_peer_meshs_stop_and_delete_publishes_nothing_for_that_mesh() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-no-acting-publication").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let fp_n = node().await;
        let fab = fabric();
        let fp_own = digest(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 41_001);
        let fp_m = rdm_gossip::topology::TopologyView::new(&fab, "mesh1", &MeshId::mint(), fp_own.node.node_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let fp_m_channels = rdm_gossip::channel::Channels::new(&fp_n.gossip, &fp_n.ep, &fp_m).unwrap();
        fp_m_channels.join(rdm_gossip::channel::ChannelId::mesh(fp_m.fabric_id(), fp_m.mesh_id()), vec![]).await.unwrap();
        fp_m_channels.set_publisher(&rdm_core::PublisherId { incarnation: fp_own.node.incarnation.clone(), node_id: fp_own.node.node_id.clone() });
        fp_m_channels.join(rdm_gossip::channel::ChannelId::backbone(fp_m.fabric_id()), vec![]).await.unwrap();
        let fp_bb = fp_m_channels.backbone().expect("the backbone is joined");
        fp_bb.set_mesh_primary(true);
        // The view a node-admin hands its backbone at each install: it names mesh2 live, for the
        // accepted Build names it (rafka-node-admin-core admin.rs `project_decided_at`); a live mesh
        // with no primary goes unheard (R-RX4).
            fp_bb.set_fabric_primary(true);

        let admin2 = digest(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", 42_001);
        let rpc = digest(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", 42_101);
        for d in [&admin2, &rpc] {
            assert!(fp_m.digest_view.record(d.clone()));
        }
        // The first publication has published; the next waits.
        fp_bb.publish(&fp_m, fp_bb.own_members(&fp_m)).await;

        let publication = {
            let (bb, m) = (fp_bb.clone(), fp_m.clone());
            tokio::spawn(async move {
                let started = Instant::now();
                bb.publication_wake(&m, tokio::time::Instant::now() + ANTI_ENTROPY).await;
                started.elapsed()
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;

        // mesh2's primary is gone. The fabric-primary, as executor, stops mesh2.node.1 and deletes mesh2.node-admin.1.
        let (publisher, stamped) = fp_bb.publish_status_event(StatusEvent::Stopped, &op("stop-node", &rpc)).await;
        assert!(fp_m.digest_view.depart(op("delete-node", &admin2)), "the exited verdict's departure is taken");
        let woke_after = publication.await.unwrap();

        fp_bb.set_fabric_primary(false);
        drop(fp_bb);
        fp_n.ep.close().await;
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();

        assert!(woke_after >= ANTI_ENTROPY, "the publication returned after {woke_after:?}, before its {ANTI_ENTROPY:?} deadline: the executed stop or delete of mesh2 woke it to publish for mesh2");
        let event = spans
            .iter()
            .find(|s| s["name"] == "rdm.mesh.topology.update.via-status-event" && s["attributes"]["subject_path"] == "mesh2.node" && s["attributes"]["subject_name"] == "1")
            .unwrap_or_else(|| panic!("the stop mesh1.node-admin.1 executed is spanned: {spans:#?}"));
        println!("{CELL}: status event trace {} role {} topology_version {}", event["trace_id"], event["attributes"]["role"], event["attributes"]["topology_version"]);
        assert_eq!(event["attributes"]["role"], "executor", "a stop of a peer mesh's node is stamped by the executor, not as that mesh's publisher: {event}");
        let published = spans
            .iter()
            .find(|s| s["name"] == "rdm.mesh.backbone.update.via-topology-version" && s["attributes"]["mesh"] == "mesh1")
            .and_then(|s| s["attributes"]["topology_version"].as_u64().or_else(|| s["attributes"]["topology_version"].as_str().and_then(|v| v.parse().ok())))
            .expect("the fabric-primary's own mesh was published at a version");
        assert_eq!(stamped, published, "stamped with the version the fabric-primary holds of its own mesh, no counter of mesh2");
        assert_eq!(publisher.node_id, fp_own.node.node_id, "the executor stamps the event under its own node id");
        for name in ["rdm.mesh.backbone.update.via-acting-primary", "rdm.mesh.backbone.reject.via-mesh-has-primary"] {
            let found: Vec<&Value> = spans.iter().filter(|s| s["name"] == name).collect();
            assert!(found.is_empty(), "{name}: the fabric-primary is no mesh's acting primary: {found:#?}");
        }
    });
}
