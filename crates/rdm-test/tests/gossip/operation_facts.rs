//! Live receive-path cells for operation facts: exact identity, independent of unrelated traffic.

use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::membership::{learn_addresses, Frame, Membership};
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
        let sender = Membership::join(&sender_gossip, &sender_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![]).await.unwrap();
        let receiver = Membership::join(&receiver_gossip, &receiver_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![address]).await.unwrap();
        let op = LifecycleOp { build_id: "deletion-build".into(), attempt: 1, operation: "node.delete:mesh1.rpc.2".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "mesh1.rpc.2".parse().unwrap(), event_at_rafka_ms: 1 };
        assert!(receiver.book.record(MeshDigest {
            fabric_id: fabric.clone(), node: MeshNode { node_id: op.node_id.clone(), name: op.name.clone(), endpoint_id: EndpointId(sender_ep.id().to_string()), transport_addr: socket, incarnation: op.incarnation.clone(), supersedes: None, runtime: None },
            status: MemberStatus::ReadyForTraffic, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1,
            data_dir: None, mesh_id: Some(mesh.clone()), in_flight: None, config: Default::default(), load: None, gossip: None,
        }), "the exact birth is initially held");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.connection.update.via-neighbour-up" && s["attributes"]["node"] == "mesh1.rpc.1") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the receiver joins the live mesh");
        sender.forward(&Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10_000 }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.membership.update.via-mesh-status" && s["attributes"]["taken"] == "true") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("unrelated newer traffic is applied first");
        sender.forward(&Frame::NodeDeleted { op: op.clone(), forwarded_by: None }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.membership.update.via-node-deleted" && s["attributes"]["applied"] == "true") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the terminal fact is applied after unrelated newer traffic");
        assert!(receiver.book.get(op.node_id.as_str()).is_none());
        assert!(receiver.book.is_departed(op.node_id.as_str()));
        assert_eq!(receiver.book.departed(), vec![op.clone()]);
        if repeat {
            let changes = receiver.book.birth_changes();
            let before = *changes.borrow();
            // Different forwarding metadata reaches RDM past the transport's byte dedup;
            // the operation and exact birth remain identical.
            sender.forward(&Frame::NodeDeleted { op: op.clone(), forwarded_by: Some("mesh2.admin.1".into()) }).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while capture.spans().unwrap().iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-node-deleted").count() < 2 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.expect("RDM receives the repeat");
            assert_eq!(*changes.borrow(), before, "the repeated fact emits no birth-change notification");
            assert_eq!(receiver.book.departed(), vec![op.clone()], "the terminal fact is unchanged");
        }
        sender_ep.close().await;
        receiver_ep.close().await;
        op
    });
    let spans = capture.spans().unwrap();
    for sp in spans.iter().filter(|s| s["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.membership."))) {
        println!("{}", serde_json::to_string(sp).unwrap());
    }
    (spans, op)
}

/// CONTRACT: a NodeDeleted terminal fact stands after an unrelated status at a later timestamp;
/// its ordinary apply span proves the held birth was removed, without any stale refusal.
#[test]
fn node_deleted_stands_after_unrelated_newer_traffic() {
    let (spans, _) = node_deleted_story(false);
    let apply: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.remove.via-node-deleted").collect();
    assert_eq!(apply.len(), 1);
    assert_eq!(apply[0]["attributes"]["was_held"], "true");
    let status = spans.iter().find(|s| s["name"] == "rdm.mesh.membership.update.via-mesh-status").unwrap();
    assert!(status["end_unix_nano"].as_u64().unwrap() <= apply[0]["start_unix_nano"].as_u64().unwrap());
    assert!(!spans.iter().any(|s| s["name"] == "rdm.mesh.membership.reject.via-stale" && s["attributes"]["frame"] == "node-deleted"));
}

/// CONTRACT: a NodeDeleted repeat matches kind, operation and exact birth, emits via-duplicate,
/// and produces no additional removal or birth notification; forwarding metadata is not identity.
#[test]
fn node_deleted_repeat_is_ignored_by_its_operation_and_exact_birth() {
    let (spans, op) = node_deleted_story(true);
    assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.remove.via-node-deleted").count(), 1);
    let identity = format!("node-deleted {} {} {} {}/{}", op.build_id, op.attempt, op.operation, op.node_id, op.incarnation);
    assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-duplicate"
        && s["attributes"]["frame"] == "node-deleted" && s["attributes"]["identity"] == identity
        && s["attributes"]["outcome"] == "ignored").count(), 1, "the framework must name the ignored exact operation fact");
}

#[derive(Clone, Copy)]
enum HookStory { AfterUnrelated, Repeat, PastFabric }

fn round_hook_story(kind: &'static str, story: HookStory) {
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    let suffix = match story { HookStory::AfterUnrelated => "control", HookStory::Repeat => "repeat", HookStory::PastFabric => "past-fabric" };
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, &format!("rdm-{kind}-{suffix}")).unwrap();
    let carried = Arc::new(AtomicUsize::new(0));
    let count = carried.clone();
    capture.run(2, async {
        let (sender_ep, sender_gossip, _sender_router) = endpoint().await;
        let (receiver_ep, receiver_gossip, _receiver_router) = endpoint().await;
        let socket = sender_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let address = iroh::EndpointAddr::new(sender_ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], socket.port())));
        learn_addresses(&receiver_ep, &[address.clone()]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let sender = Membership::join(&sender_gossip, &sender_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![]).await.unwrap();
        let receiver = Membership::join(&receiver_gossip, &receiver_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![address]).await.unwrap();
        receiver.set_completion_carrier(Arc::new(move |_| { count.fetch_add(1, Ordering::SeqCst); }));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.connection.update.via-neighbour-up" && s["attributes"]["node"] == "mesh1.rpc.1") { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.unwrap();
        let status = if matches!(story, HookStory::PastFabric) {
            Frame::FabricStatus { fabric: fabric.clone(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10_000 }
        } else {
            Frame::MeshStatus { mesh: "mesh2".into(), status: "ready-for-traffic".into(), publisher: "mesh2.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10_000 }
        };
        sender.forward(&status).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| (s["name"] == "rdm.mesh.membership.update.via-mesh-status" || s["name"] == "rdm.mesh.membership.update.via-fabric-status") && s["attributes"]["taken"] == "true") { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.unwrap();
        let changes = receiver.book.birth_changes();
        let before = *changes.borrow();
        let hook = rdm_core::RoundHook { fabric_id: fabric.clone(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), build_id: "held-round-build".into(), attempt: 1, operation: format!("{}:{fabric}", if kind.starts_with("state-") { "commit-state" } else { "open-traffic" }), publisher: "mesh1.rpc.2".into(), event_at_rafka_ms: 1 };
        let frame = |time| {
            let mut hook = hook.clone();
            hook.event_at_rafka_ms = time;
            match kind {
                "state-committing" => Frame::StateCommitting { hook, forwarded_by: None },
                "state-committed" => Frame::StateCommitted { hook, forwarded_by: None },
                "traffic-opening" => Frame::TrafficOpening { hook, forwarded_by: None },
                _ => Frame::TrafficOpened { hook, forwarded_by: None },
            }
        };
        for receipt in 1..=if matches!(story, HookStory::Repeat) { 2 } else { 1 } {
            // A changed evidence timestamp passes transport byte dedup without changing operation identity.
            sender.forward(&frame(receipt)).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while capture.spans().unwrap().iter().filter(|s| {
                    s["name"] == "rdm.mesh.membership.update.via-round-frame" && s["attributes"]["kind"] == kind
                    || (s["name"] == "rdm.mesh.membership.update.via-duplicate" || s["name"] == "rdm.mesh.membership.update.via-round-reconciled") && s["attributes"]["frame"] == kind
                }).count() < receipt as usize { tokio::time::sleep(Duration::from_millis(10)).await; }
            }).await.expect("the hook reaches the RDM receiver");
        }
        assert_eq!(*changes.borrow(), before, "a hook never changes a birth's lifecycle");
        sender_ep.close().await;
        receiver_ep.close().await;
    });
    let spans = capture.spans().unwrap();
    for sp in spans.iter().filter(|s| s["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.membership."))) { println!("{}", serde_json::to_string(sp).unwrap()); }
    let applies = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-round-frame" && s["attributes"]["kind"] == kind).count();
    let completion = kind == "state-committed" || kind == "traffic-opened";
    match story {
        HookStory::AfterUnrelated => {
            assert_eq!(applies, 1, "the ordinary apply span controls that unrelated traffic did not discard the hook");
            assert_eq!(carried.load(Ordering::SeqCst), usize::from(completion));
        }
        HookStory::Repeat => {
            assert_eq!(applies, 1, "a repeat has no second apply effect");
            assert_eq!(carried.load(Ordering::SeqCst), usize::from(completion), "a repeat is never carried twice");
            assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-duplicate" && s["attributes"]["frame"] == kind && s["attributes"]["outcome"] == "ignored").count(), 1);
        }
        HookStory::PastFabric => {
            assert_eq!(applies, 0, "a fabric past the round reconciles the delayed hook without applying it");
            assert_eq!(carried.load(Ordering::SeqCst), 0);
        }
    }
}

macro_rules! round_cells {
    ($control:ident, $repeat:ident, $past:ident, $kind:literal) => {
        /// CONTRACT: unrelated newer traffic does not supersede this operation fact.
        #[test] fn $control() { round_hook_story($kind, HookStory::AfterUnrelated); }
        /// CONTRACT: the full operation identity is applied once, with a named duplicate on repeat.
        #[test] fn $repeat() { round_hook_story($kind, HookStory::Repeat); }
        /// CONTRACT: a hook after the fabric moved past its round reopens and carries nothing.
        #[test] fn $past() { round_hook_story($kind, HookStory::PastFabric); }
    };
}
round_cells!(state_committing_after_unrelated_traffic_applies, state_committing_repeat_does_nothing, state_committing_after_fabric_round_reopens_nothing, "state-committing");
round_cells!(state_committed_after_unrelated_traffic_applies, state_committed_repeat_does_nothing, state_committed_after_fabric_round_reopens_nothing, "state-committed");
round_cells!(traffic_opening_after_unrelated_traffic_applies, traffic_opening_repeat_does_nothing, traffic_opening_after_fabric_round_reopens_nothing, "traffic-opening");
round_cells!(traffic_opened_after_unrelated_traffic_applies, traffic_opened_repeat_does_nothing, traffic_opened_after_fabric_round_reopens_nothing, "traffic-opened");
