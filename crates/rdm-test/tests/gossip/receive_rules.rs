//! A live mesh-channel probe of the replacement-state receive contract.

use rdm_core::{FabricId, MeshId};
use rdm_transport::membership::{learn_addresses, Frame, Membership};
use std::time::Duration;

async fn endpoint() -> (iroh::Endpoint, iroh_gossip::net::Gossip, iroh::protocol::Router) {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    (ep, gossip, router)
}

/// CONTRACT: after a receiver holds a MeshStatus revision, another frame of the same declared
/// subject, publisher and revision with different status content is refused as inconsistent.
/// It cannot be silently classified as a duplicate. Both frames travel over a live mesh channel.
#[test]
fn an_unkeyed_mesh_status_with_equal_revision_and_different_content_is_refused_inconsistent() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-gossip-conformance").unwrap();
    capture.run(2, async {
        let (sender_ep, sender_gossip, _sender_router) = endpoint().await;
        let (receiver_ep, receiver_gossip, _receiver_router) = endpoint().await;
        let socket = sender_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let address = iroh::EndpointAddr::new(sender_ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], socket.port())));
        learn_addresses(&receiver_ep, &[address.clone()]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let sender = Membership::join(&sender_gossip, &sender_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![]).await.unwrap();
        let receiver = Membership::join(&receiver_gossip, &receiver_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![address]).await.unwrap();
        let frame = |status: &str| Frame::MeshStatus { mesh: "mesh1".into(), status: status.into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 100 };
        // Wait for the actual neighbour event, without sending or retrying the subject frame.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.connection.update.via-neighbour-up" && s["attributes"]["node"] == "mesh1.rpc.1") { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the receiver joins the live mesh");
        sender.forward(&frame("ready-for-traffic")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while receiver.mesh_status("mesh1").is_none() { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.expect("the receiver holds the initial status");
        sender.forward(&frame("deleting")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let spans = capture.spans().unwrap();
                if spans.iter().any(|s| s["attributes"]["status"] == "deleting" || s["name"] == "rdm.mesh.membership.reject.via-inconsistent") { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the conflicting frame reaches the receiver");
        assert_eq!(receiver.mesh_status("mesh1").unwrap().status, "ready-for-traffic");
        sender_ep.close().await;
        receiver_ep.close().await;
    });
    let spans = capture.spans().unwrap();
    for span in spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-mesh-status" || s["name"] == "rdm.mesh.membership.reject.via-inconsistent") {
        println!("{}", serde_json::to_string(span).unwrap());
    }
    assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.membership.reject.via-inconsistent"), "equal revision with different content must be refused by name, never silently treated as a duplicate; evidence {}", capture.path().display());
}
