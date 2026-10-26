//! Live receive-path cells for operation facts: exact identity, independent of unrelated traffic.

use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::{learn_addresses, Frame, TopologyView};
use serde_json::Value;
use std::time::Duration;

async fn endpoint() -> (iroh::Endpoint, iroh_gossip::net::Gossip, iroh::protocol::Router) {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    (ep, gossip, router)
}

fn node_deleted_story(repeat: bool) -> (Vec<Value>, LifecycleOp) {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let service = if repeat { "rdm-node-deleted-repeat" } else { "rdm-node-deleted-after-unrelated" };
    let capture = rdm_transport::EvidenceCapture::start(&dir, service).unwrap();
    let op = capture.run(2, async {
        let (sender_ep, sender_gossip, _sender_router) = endpoint().await;
        let (receiver_ep, receiver_gossip, _receiver_router) = endpoint().await;
        let socket = sender_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let address = iroh::EndpointAddr::new(sender_ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], socket.port())));
        learn_addresses(&receiver_ep, &[address.clone()]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let sender = TopologyView::join(&sender_gossip, &sender_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        crate::declare_gossip_probe(&sender);
        let receiver = TopologyView::join(&receiver_gossip, &receiver_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![address], &[]).await.unwrap();
        let op = LifecycleOp { build_id: "deletion-build".into(), attempt: 1, operation: "node.delete:mesh1.rpc.2".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "mesh1.rpc.2".parse().unwrap(), event_at_rafka_ms: 1 };
        assert!(receiver.digest_view.record(MeshDigest { held_hash: Default::default(),
            fabric_id: fabric.clone(), node: MeshNode { node_id: op.node_id.clone(), name: op.name.clone(), endpoint_id: EndpointId(sender_ep.id().to_string()), transport_addr: socket, incarnation: op.incarnation.clone(), supersedes: None, runtime: None },
            status: MemberStatus::ReadyForTraffic, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1,
            data_dir: None, mesh_id: Some(mesh.clone()), in_flight: None, config: Default::default(), load: None, gossip: None, is_primary: None, is_fabric_primary: None
        }), "the exact birth is initially held");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.connection.update.via-neighbour-up" && s["attributes"]["node"] == "mesh1.rpc.1") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the receiver joins the live mesh");
        crate::forward_gossip_probe(&sender, &Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10_000 }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.topology.update.via-mesh-status" && s["attributes"]["taken"] == "true") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("unrelated newer traffic is applied first");
        crate::forward_gossip_probe(&sender, &Frame::NodeDeleted { op: op.clone(), forwarded_by: None }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.topology.update.via-node-deleted" && s["attributes"]["applied"] == "true") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the terminal fact is applied after unrelated newer traffic");
        assert!(receiver.digest_view.get(op.node_id.as_str()).is_none());
        assert!(receiver.digest_view.is_deleted(op.node_id.as_str()));
        assert_eq!(receiver.digest_view.deleted(), vec![op.clone()]);
        if repeat {
            let changes = receiver.digest_view.birth_changes();
            let before = *changes.borrow();
            // Different forwarding metadata reaches RDM past the transport's byte dedup;
            // the operation and exact birth remain identical.
            crate::forward_gossip_probe(&sender, &Frame::NodeDeleted { op: op.clone(), forwarded_by: Some("mesh2.admin.1".into()) }).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while capture.spans().unwrap().iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-node-deleted").count() < 2 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.expect("RDM receives the repeat");
            assert_eq!(*changes.borrow(), before, "the repeated fact emits no birth-change notification");
            assert_eq!(receiver.digest_view.deleted(), vec![op.clone()], "the terminal fact is unchanged");
        }
        sender_ep.close().await;
        receiver_ep.close().await;
        op
    });
    let spans = capture.spans().unwrap();
    for sp in spans.iter().filter(|s| s["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.topology."))) {
        println!("{}", serde_json::to_string(sp).unwrap());
    }
    (spans, op)
}

/// CONTRACT: a NodeDeleted terminal fact stands after an unrelated status at a later timestamp;
/// its ordinary apply span proves the held birth was removed, without any stale refusal.
#[test]
fn node_deleted_stands_after_unrelated_newer_traffic() {
    let (spans, _) = node_deleted_story(false);
    let apply: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.remove.via-node-deleted").collect();
    assert_eq!(apply.len(), 1);
    assert_eq!(apply[0]["attributes"]["was_held"], "true");
    let status = spans.iter().find(|s| s["name"] == "rdm.mesh.topology.update.via-mesh-status").unwrap();
    assert!(status["end_unix_nano"].as_u64().unwrap() <= apply[0]["start_unix_nano"].as_u64().unwrap());
    assert!(!spans.iter().any(|s| s["name"] == "rdm.mesh.topology.reject.via-stale" && s["attributes"]["frame"] == "node-deleted"));
}

/// CONTRACT: a NodeDeleted repeat matches kind, operation and exact birth, emits via-duplicate,
/// and produces no additional removal or birth notification; forwarding metadata is not identity.
#[test]
fn node_deleted_repeat_is_ignored_by_its_operation_and_exact_birth() {
    let (spans, op) = node_deleted_story(true);
    assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.remove.via-node-deleted").count(), 1);
    let identity = format!("node-deleted {} {} {} {}/{}", op.build_id, op.attempt, op.operation, op.node_id, op.incarnation);
    assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-duplicate"
        && s["attributes"]["frame"] == "node-deleted" && s["attributes"]["identity"] == identity
        && s["attributes"]["outcome"] == "ignored").count(), 1, "the framework must name the ignored exact operation fact");
}

