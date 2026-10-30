//! Live cells for lifecycle operation identity alongside the carried projection revision.
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, PublisherId};
use rdm_transport::topology::{learn_addresses, Frame, TopologyView, StatusEvent};
use std::time::Duration;

async fn endpoint() -> (iroh::Endpoint, iroh_gossip::net::Gossip, iroh::protocol::Router) {
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], iroh::endpoint::QuicTransportConfig::builder().build()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    (ep, gossip, router)
}
#[derive(Clone, Copy)]
enum Story { Control, Repeat, Completed, Stale, Successor }
fn story(kind: StatusEvent, scenario: Story) {
    let suffix = match scenario { Story::Control => "control", Story::Repeat => "repeat", Story::Completed => "completed", Story::Stale => "stale", Story::Successor => "successor" };
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, &format!("rdm-{}-{suffix}", kind.name())).unwrap();
    let (op, publisher, successor) = capture.run(2, async {
        let (send_ep, send_gossip, _send_router) = endpoint().await;
        let (recv_ep, recv_gossip, _recv_router) = endpoint().await;
        let socket = send_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let addr = iroh::EndpointAddr::new(send_ep.id()).with_ip_addr(socket);
        learn_addresses(&recv_ep, &[addr.clone()]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let sender = TopologyView::join(&send_gossip, &send_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        let receiver = TopologyView::join(&recv_gossip, &recv_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![addr], &[]).await.unwrap();
        let command = match kind { StatusEvent::Deleting => "delete", StatusEvent::Stopped => "stop" };
        let op = LifecycleOp { build_id: "lifecycle-build".into(), attempt: 1, operation: format!("node.{command}:mesh1.rpc.2"), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "mesh1.rpc.2".parse().unwrap(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        // The publisher that takes the role after it: another birth, which re-publishes the operation its
        // predecessor published under the same identity (a resumed attempt replays its receipted step).
        let successor = PublisherId { node: "mesh1.admin.2".into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        sender.set_author(&publisher);
        receiver.digest_view.record(MeshDigest { held_hash: Default::default(), fabric_id: fabric, node: MeshNode { node_id: op.node_id.clone(), name: op.name.clone(), endpoint_id: EndpointId(send_ep.id().to_string()), transport_addr: socket, incarnation: op.incarnation.clone(), supersedes: None, runtime: None }, status: MemberStatus::Ready, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: Some(mesh), in_flight: None, load: None, gossip: None, is_primary: None, is_fabric_primary: None });
        assert!(receiver.digest_view.started(&op.node_id, &op.incarnation, &op.name));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.connection.update.via-neighbour-up") { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.unwrap();
        crate::forward_gossip_probe(&sender, &Frame::MeshStatus { mesh: "mesh1".into(), status: "ready".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10_000 }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async { while receiver.mesh_status("mesh1").is_none() { tokio::time::sleep(Duration::from_millis(10)).await; } }).await.unwrap();
        assert!(receiver.digest_view.routable(op.node_id.as_str()));
        if matches!(scenario, Story::Completed) {
            match kind {
                StatusEvent::Deleting => { assert!(receiver.digest_view.deleting(op.clone())); receiver.digest_view.deletions_from_facts(&std::collections::BTreeMap::from([(op.key(), "attempt-converged")]), &Default::default()); }
                StatusEvent::Stopped => {
                    assert!(receiver.digest_view.stopped(op.clone()));
                    let mut start = op.clone(); start.operation = "node.start:mesh1.rpc.2".into();
                    assert!(receiver.digest_view.joined(&start)); assert!(receiver.digest_view.started(&op.node_id, &op.incarnation, &op.name));
                }
            }
            assert!(receiver.digest_view.routable(op.node_id.as_str()));
        }
        let frame = |op: LifecycleOp, version| kind.frame(op, publisher.clone(), version);
        let successor_frame = |op: LifecycleOp, version| kind.frame(op, successor.clone(), version);
        if matches!(scenario, Story::Stale) {
            let mut other = op.clone(); other.node_id = NodeId::mint(); other.incarnation = IncarnationId::mint(); other.name = "mesh1.rpc.3".parse().unwrap(); other.operation = format!("node.{command}:mesh1.rpc.3");
            let other_id = other.node_id.to_string();
            crate::forward_gossip_probe(&sender, &frame(other, 20)).await.unwrap();
            // An open overlay is visible before the same receive finishes its birth notification.
            // Capture the baseline only after that operation's enclosing receipt has closed.
            tokio::time::timeout(Duration::from_secs(5), async {
                while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.topology.update.via-lifecycle-receipt" && s["attributes"]["node_id"] == other_id) { tokio::time::sleep(Duration::from_millis(10)).await; }
            }).await.unwrap();
        }
        let births = receiver.digest_view.birth_changes(); let overlays = receiver.digest_view.overlay_changes();
        let held_before = receiver.digest_view.get(op.node_id.as_str()).map(|(d, _)| d);
        let completed_counts = (*births.borrow(), *overlays.borrow());
        let receipts = if matches!(scenario, Story::Control) { 1 } else { 2 };
        for n in 1..=receipts {
            let mut offered = op.clone(); if n == 2 { offered.event_at_rafka_ms = 2; }
            if n == 2 && matches!(scenario, Story::Successor) {
                sender.set_author(&successor);
                crate::forward_gossip_probe(&sender, &successor_frame(offered, 10)).await.unwrap();
            } else {
                crate::forward_gossip_probe(&sender, &frame(offered, if n == 2 && matches!(scenario, Story::Stale) { 21 } else { 10 })).await.unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while capture.spans().unwrap().iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-lifecycle-receipt" && s["attributes"]["node_id"] == op.node_id.to_string()).count() < n { tokio::time::sleep(Duration::from_millis(10)).await; }
            }).await.expect("the lifecycle receipt finished classification and application");
        }
        if matches!(scenario, Story::Stale) {
            // Operation identity was already seen at the refused v10. Its repeat at v21 runs no
            // effect, while that independent projection key still advances the publisher floor.
            let mut third = op.clone(); third.node_id = NodeId::mint(); third.name = "mesh1.rpc.4".parse().unwrap(); third.operation = format!("node.{command}:mesh1.rpc.4");
            crate::forward_gossip_probe(&sender, &frame(third.clone(), 20)).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.topology.update.via-lifecycle-receipt" && s["attributes"]["node_id"] == third.node_id.to_string()) { tokio::time::sleep(Duration::from_millis(10)).await; }
            }).await.unwrap();
            assert!(!receiver.digest_view.in_flight().iter().any(|o| o.node_id == third.node_id), "a repeat's newer carried revision still orders later projection frames");
        }
        if matches!(scenario, Story::Successor) {
            // The successor's re-entry ran no effect, and its carried revision still advanced its own
            // floor: its later frame below that revision is refused as stale and never applied.
            let mut below = op.clone(); below.node_id = NodeId::mint(); below.name = "mesh1.rpc.5".parse().unwrap(); below.operation = format!("node.{command}:mesh1.rpc.5");
            crate::forward_gossip_probe(&sender, &successor_frame(below.clone(), 9)).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.topology.update.via-lifecycle-receipt" && s["attributes"]["node_id"] == below.node_id.to_string()) { tokio::time::sleep(Duration::from_millis(10)).await; }
            }).await.unwrap();
            assert!(!receiver.digest_view.in_flight().iter().any(|o| o.node_id == below.node_id), "the successor's frame below its recorded revision applies nothing");
        }
        if matches!(scenario, Story::Completed | Story::Stale) {
            assert!(!receiver.digest_view.in_flight().iter().any(|o| o.node_id == op.node_id), "no old operation reopens");
            assert_eq!(receiver.digest_view.get(op.node_id.as_str()).map(|(d, _)| d), held_before);
            assert_eq!((*births.borrow(), *overlays.borrow()), completed_counts);
            assert!(receiver.digest_view.routable(op.node_id.as_str()));
        } else {
            assert_eq!(receiver.digest_view.in_flight(), vec![op.clone()], "a repeat cannot replace the held operation evidence");
            assert!(!receiver.digest_view.routable(op.node_id.as_str()), "routing reads the overlay");
            if kind != StatusEvent::Stopped { assert_eq!(receiver.digest_view.get(op.node_id.as_str()).map(|(d, _)| d), held_before, "deleting does not rewrite the held digest"); }
            assert_eq!(*overlays.borrow(), completed_counts.1 + 1, "the overlay changes once");
        }
        send_ep.close().await; recv_ep.close().await;
        (op, publisher, successor)
    });
    let spans = capture.spans().unwrap();
    for s in spans.iter().filter(|s| s["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.topology."))) { println!("{}", serde_json::to_string(s).unwrap()); }
    let apply_name = match kind { StatusEvent::Deleting => "rdm.mesh.topology.update.via-node-deleting", StatusEvent::Stopped => "rdm.mesh.topology.remove.via-node-stopped" };
    assert_eq!(spans.iter().filter(|s| s["name"] == apply_name && s["attributes"]["node_id"] == op.node_id.to_string()).count(), usize::from(!matches!(scenario, Story::Stale)), "exactly one ordinary apply, including the locally applied operation before completion; no apply of a version-refused fact");
    if !matches!(scenario, Story::Control) {
        let identity = format!("{} {} {} {}/{}", op.build_id, op.attempt, op.operation, op.node_id, op.incarnation);
        assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.topology.update.via-duplicate" && s["attributes"]["frame"] == kind.name() && s["attributes"]["identity"] == identity && s["attributes"]["outcome"] == "ignored"));
    }
    if matches!(scenario, Story::Stale) {
        assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.topology.reject.via-stale" && s["attributes"]["frame"] == kind.name() && s["attributes"]["held_key"] == "topology_version=21" && s["attributes"]["frame_key"] == "topology_version=20"));
        assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.topology.reject.via-stale" && s["attributes"]["frame"] == kind.name() && s["attributes"]["subject"] == format!("{publisher} mesh1") && s["attributes"]["held_key"] == "topology_version=20" && s["attributes"]["frame_key"] == "topology_version=10"));
    }
    if matches!(scenario, Story::Successor) {
        assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.topology.reject.via-stale" && s["attributes"]["frame"] == kind.name() && s["attributes"]["subject"] == format!("{successor} mesh1") && s["attributes"]["held_key"] == "topology_version=10" && s["attributes"]["frame_key"] == "topology_version=9"), "the successor's duplicate recorded its carried revision as its own floor");
    }
}
macro_rules! cells {
    ($first:ident, $repeat:ident, $completed:ident, $stale:ident, $kind:ident) => {
        /// CONTRACT: a first fact applies; routing alone reads the deleting overlay.
        #[test] fn $first() { story(StatusEvent::$kind, Story::Control); }
        /// CONTRACT: a repeated operation identity emits via-duplicate and changes nothing twice.
        #[test] fn $repeat() { story(StatusEvent::$kind, Story::Repeat); }
        /// CONTRACT: held completion evidence prevents a delayed operation from reopening the birth.
        #[test] fn $completed() { story(StatusEvent::$kind, Story::Completed); }
        /// CONTRACT: version refusal is named and still records the operation identity as seen.
        #[test] fn $stale() { story(StatusEvent::$kind, Story::Stale); }
    };
}
/// CONTRACT (gossip.md receive rules, lifecycle facts: identity is the kind and the complete operation
/// identity, never the publisher; "a repeat runs no effect twice"; the carried `topology_version` is data
/// under its own per-publisher floor, "even for a repeated fact"): the publisher that took the role
/// re-publishes an operation its predecessor published, under the same identity. The receiver applies
/// it once (the predecessor's copy), names the successor's copy `via-duplicate`, and records the
/// successor's carried revision as that publisher's floor. What must NOT happen: a second apply, the
/// held operation evidence rewritten, or the successor's revision left unrecorded.
macro_rules! successor_cells {
    ($($name:ident: $kind:ident),*) => { $( #[test] fn $name() { story(StatusEvent::$kind, Story::Successor); } )* };
}
successor_cells!(deleting_reentered_by_a_successor_applies_once: Deleting, stopped_reentered_by_a_successor_applies_once: Stopped);
cells!(deleting_first_fact_changes_routing, deleting_repeat_changes_nothing, deleting_after_completion_reopens_nothing, deleting_stale_revision_records_identity, Deleting);
cells!(stopped_first_fact_changes_routing, stopped_repeat_changes_nothing, stopped_after_later_start_reopens_nothing, stopped_stale_revision_records_identity, Stopped);
