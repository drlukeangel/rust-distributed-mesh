//! Live context refusals precede opaque data, and cannot alter installed state.
use rdm_core::{FabricId, MeshId};
use rdm_transport::topology::{Frame, TopologyView, mesh_channel, learn_addresses};
use std::time::Duration;

#[test]
fn reserved_unknown_and_malformed_contexts_are_named_before_data_and_mutate_nothing() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-envelope-context-refusals").unwrap();
    capture.run(2, async {
        let (send_ep, gossip, _send_router) = crate::receive_rules::endpoint().await;
        let (recv_ep, recv_gossip, _recv_router) = crate::receive_rules::endpoint().await;
        let addr = iroh::EndpointAddr::new(send_ep.id()).with_ip_addr(send_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap());
        learn_addresses(&recv_ep, &[addr.clone()]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let mut topic = gossip.subscribe(mesh_channel(&fabric, &mesh), vec![]).await.unwrap();
        let receiver = TopologyView::join(&recv_gossip, &recv_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![addr], &[]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), topic.joined()).await.unwrap().unwrap();
        let (sender, _rx) = topic.split();
        let author = crate::gossip_author(&fabric);
        let status = |scope: &str, revision| Frame::MeshStatus { mesh: scope.into(), status: "ready".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: revision }.authored(&author, 2, revision);
        sender.broadcast_neighbors(status("held", 1).encode().into()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async { while receiver.mesh_status("held").is_none() { tokio::time::sleep(Duration::from_millis(10)).await; } }).await.unwrap();
        let held = receiver.mesh_status("held");
        let roles = receiver.primaries().subscribe(); let births = receiver.digest_view.birth_changes();
        let before = (*roles.borrow(), *births.borrow());
        let mut cases = Vec::new();
        for kind in ["rdm.mesh.concern", "rdm.node.connections.deleting", "rdm.node.leaving", "rdm.node.left", "rdm.node.connections.deleted", "rdm.fabric.primary.new", "rdm.unknown"] {
            let (mut attrs, _): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&status("held", 2).encode()).unwrap();
            attrs.retain(|(n, _)| n != "sequence"); attrs.iter_mut().find(|(n, _)| n == "type").unwrap().1 = kind.into();
            cases.push((rdm_transport::wire::encode(&(attrs, vec![255u8])).unwrap(), if kind == "rdm.unknown" { "rdm.mesh.topology.reject.via-unknown-type" } else { "rdm.mesh.topology.reject.via-reserved" }));
        }
        for (name, value) in [("type", ""), ("source", "rdm://mesh1.admin.1"), ("source", "rdm://000000000001/0123456789ab/yrz49j303yh4"), ("sequence", "2.2"), ("specversion", "0.3"), ("subject", "mesh/different/status"), ("time", "invalid")] {
            let (mut attrs, body): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&status("held", 2).encode()).unwrap();
            attrs.iter_mut().find(|(n, _)| n == name).unwrap().1 = value.into();
            cases.push((rdm_transport::wire::encode(&(attrs, body)).unwrap(), "rdm.mesh.topology.reject.via-undecodable-frame"));
        }
        let (mut attrs, body): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&status("held", 2).encode()).unwrap();
        attrs.push(attrs[0].clone()); cases.push((rdm_transport::wire::encode(&(attrs, body)).unwrap(), "rdm.mesh.topology.reject.via-undecodable-frame"));
        for (index, (bytes, outcome)) in cases.into_iter().enumerate() {
            let count = capture.spans().unwrap().iter().filter(|s| s["name"] == outcome).count();
            sender.broadcast_neighbors(bytes.into()).await.unwrap();
            sender.broadcast_neighbors(status("barrier", index as u64 + 10).encode().into()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async { while receiver.mesh_status("barrier").is_none_or(|s| s.changed_at_rafka_ms < index as u64 + 10) { tokio::time::sleep(Duration::from_millis(10)).await; } }).await.unwrap();
            assert_eq!(capture.spans().unwrap().iter().filter(|s| s["name"] == outcome).count(), count + 1, "the context has its named refusal");
            assert_eq!(receiver.mesh_status("held"), held);
            assert_eq!((*roles.borrow(), *births.borrow()), before);
        }
        send_ep.close().await; recv_ep.close().await;
    });
}
