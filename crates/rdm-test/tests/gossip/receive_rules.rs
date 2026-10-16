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
    assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.membership.reject.via-inconsistent" && s["attributes"]["frame"] == "mesh-status" && s["attributes"]["subject"] == "mesh1 mesh1.admin.1" && s["attributes"]["revision"] == "100"), "equal revision with different content must be refused by name, never silently treated as a duplicate; evidence {}", capture.path().display());
}

/// Deliver one more MeshStatus after revision 100 over a live channel, preserving a snapshot of
/// the held projection and counting its actual receipt before checking the outcome span.
fn mesh_status_receipt(service: &str, status: &str, revision: u64) -> Vec<serde_json::Value> {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, service).unwrap();
    capture.run(2, async {
        let (sender_ep, sender_gossip, _sender_router) = endpoint().await;
        let (receiver_ep, receiver_gossip, _receiver_router) = endpoint().await;
        let socket = sender_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let address = iroh::EndpointAddr::new(sender_ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], socket.port())));
        learn_addresses(&receiver_ep, &[address.clone()]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let sender = Membership::join(&sender_gossip, &sender_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![]).await.unwrap();
        let receiver = Membership::join(&receiver_gossip, &receiver_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![address]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.connection.update.via-neighbour-up" && s["attributes"]["node"] == "mesh1.rpc.1") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the receiver joins the live mesh");
        let initial = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 100 };
        sender.forward(&initial).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while receiver.mesh_status("mesh1").is_none() { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.expect("the receiver holds the initial status");
        let changes = receiver.book.statuses.mesh_changes();
        let changes_before = *changes.borrow();
        // A forwarded copy has different wire bytes, so iroh-gossip's content dedup does not hide
        // the receipt from RDM. Its declared identity and status content remain unchanged.
        let offered = Frame::MeshStatus { mesh: "mesh1".into(), status: status.into(), publisher: "mesh1.admin.1".into(), forwarded_by: Some("mesh2.admin.1".into()), changed_at_rafka_ms: revision };
        sender.forward(&offered).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while capture.spans().unwrap().iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-mesh-status").count() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("RDM receives the offered status exactly once");
        assert_eq!(*changes.borrow(), changes_before, "a refused or duplicate status emits no projection-change notification");
        let held = receiver.mesh_status("mesh1").unwrap();
        assert_eq!((held.status.as_str(), held.publisher.as_str(), held.changed_at_rafka_ms), ("ready-for-traffic", "mesh1.admin.1", 100), "the held projection does not move");
        sender_ep.close().await;
        receiver_ep.close().await;
    });
    let spans = capture.spans().unwrap();
    for span in spans.iter().filter(|s| s["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.membership."))) {
        println!("{}", serde_json::to_string(span).unwrap());
    }
    spans
}

/// CONTRACT: an older MeshStatus of the held declared subject is refused with via-stale,
/// including both ordering keys, and cannot roll back the held projection.
#[test]
fn an_older_mesh_status_is_refused_with_a_named_stale_span() {
    let spans = mesh_status_receipt("rdm-mesh-status-stale", "deleting", 99);
    assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.membership.reject.via-stale"
        && s["attributes"]["frame"] == "mesh-status" && s["attributes"]["subject"] == "mesh1 mesh1.admin.1"
        && s["attributes"]["held_key"] == "changed_at_rafka_ms=100" && s["attributes"]["frame_key"] == "changed_at_rafka_ms=99"), "the stale refusal must name the frame, subject and both keys");
}

/// CONTRACT: an identical MeshStatus identity/content received as a forwarded copy is ignored
/// with via-duplicate naming its identity; forwarding metadata cannot create another status fact.
#[test]
fn an_identical_mesh_status_is_ignored_with_a_named_duplicate_span() {
    let spans = mesh_status_receipt("rdm-mesh-status-duplicate", "ready-for-traffic", 100);
    assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.membership.update.via-duplicate"
        && s["attributes"]["frame"] == "mesh-status" && s["attributes"]["identity"] == "mesh1 mesh1.admin.1 changed_at_rafka_ms=100"
        && s["attributes"]["outcome"] == "ignored"), "a duplicate must name the frame and identity and record ignored");
}
