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
        let sender = Membership::join(&sender_gossip, &sender_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        crate::declare_gossip_probe(&sender);
        let receiver = Membership::join(&receiver_gossip, &receiver_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![address], &[]).await.unwrap();
        let op = LifecycleOp { build_id: "deletion-build".into(), attempt: 1, operation: "node.delete:mesh1.rpc.2".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "mesh1.rpc.2".parse().unwrap(), event_at_rafka_ms: 1 };
        assert!(receiver.book.record(MeshDigest { held_hash: Default::default(),
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
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.membership.update.via-mesh-status" && s["attributes"]["taken"] == "true") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("unrelated newer traffic is applied first");
        crate::forward_gossip_probe(&sender, &Frame::NodeDeleted { op: op.clone(), forwarded_by: None }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.membership.update.via-node-deleted" && s["attributes"]["applied"] == "true") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the terminal fact is applied after unrelated newer traffic");
        assert!(receiver.book.get(op.node_id.as_str()).is_none());
        assert!(receiver.book.is_deleted(op.node_id.as_str()));
        assert_eq!(receiver.book.deleted(), vec![op.clone()]);
        if repeat {
            let changes = receiver.book.birth_changes();
            let before = *changes.borrow();
            // Different forwarding metadata reaches RDM past the transport's byte dedup;
            // the operation and exact birth remain identical.
            crate::forward_gossip_probe(&sender, &Frame::NodeDeleted { op: op.clone(), forwarded_by: Some("mesh2.admin.1".into()) }).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while capture.spans().unwrap().iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-node-deleted").count() < 2 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.expect("RDM receives the repeat");
            assert_eq!(*changes.borrow(), before, "the repeated fact emits no birth-change notification");
            assert_eq!(receiver.book.deleted(), vec![op.clone()], "the terminal fact is unchanged");
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
enum HookStory { AfterUnrelated, Repeat, ForwardedRepeat, Closed, AdminAuthor }

fn round_hook_story(kind: &'static str, story: HookStory) {
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    use rdm_admin::fabric_state::{RoundBook, RoundKey, RoundKind};
    use rdm_transport::membership::Backbone;
    let suffix = match story { HookStory::AfterUnrelated => "control", HookStory::Repeat => "repeat", HookStory::ForwardedRepeat => "forwarded-repeat", HookStory::Closed => "closed", HookStory::AdminAuthor => "admin-author" };
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, &format!("rdm-{kind}-{suffix}")).unwrap();
    let carried = Arc::new(AtomicUsize::new(0));
    let called = Arc::new(AtomicUsize::new(0));
    let (count, calls) = (carried.clone(), called.clone());
    let hook = capture.run(2, async {
        let (sender_ep, sender_gossip, _sender_router) = endpoint().await;
        let (receiver_ep, receiver_gossip, _receiver_router) = endpoint().await;
        let (peer_ep, peer_gossip, _peer_router) = endpoint().await;
        let address = |ep: &iroh::Endpoint| {
            let socket = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
            iroh::EndpointAddr::new(ep.id()).with_ip_addr(std::net::SocketAddr::from(([127, 0, 0, 1], socket.port())))
        };
        learn_addresses(&receiver_ep, &[address(&sender_ep), address(&peer_ep)]).unwrap();
        learn_addresses(&peer_ep, &[address(&receiver_ep)]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let sender = Membership::join(&sender_gossip, &sender_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.2", rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        crate::declare_gossip_probe(&sender);
        let receiver = Membership::join(&receiver_gossip, &receiver_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![address(&sender_ep)], &[]).await.unwrap();
        let peer = Membership::join(&peer_gossip, &peer_ep, &fabric, "mesh2", &MeshId::mint(), "mesh2.admin.1", rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        let _peer_backbone = Backbone::join(&peer_gossip, &peer_ep, &peer, "mesh2", rdm_core::PublisherId { node: "mesh2.admin.1".to_string(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }, vec![]).await.unwrap();
        let backbone = Backbone::join(&receiver_gossip, &receiver_ep, &receiver, "mesh1", rdm_core::PublisherId { node: "mesh1.admin.1".to_string(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }, vec![address(&peer_ep)]).await.unwrap();
        let key = RoundKey { kind: if kind.starts_with("state-") { RoundKind::StateCommit } else { RoundKind::OpenTraffic }, fabric_id: fabric.clone(), build_id: "held-round-build".into(), attempt: 1 };
        let (subject_id, subject_incarnation) = (NodeId::mint(), IncarnationId::mint());
        sender.set_author(&rdm_core::PublisherId { node: "mesh1.rpc.2".into(), node_id: subject_id.clone(), incarnation: subject_incarnation.clone() });
        let book = Arc::new(RoundBook::default());
        assert!(book.open(&key));
        book.expect(&key, std::collections::BTreeMap::from([(subject_id.clone(), rdm_admin::fabric_state::Expected { incarnation: subject_incarnation.clone(), name: "mesh1.rpc.2".into() })]), Vec::new());
        if matches!(story, HookStory::Closed) { book.close(&key); }
        let checklist_changes = book.subscribe();
        let checklist_before = *checklist_changes.borrow();
        let held_book = book.clone();
        receiver.set_completion_carrier(Arc::new(move |f| {
            calls.fetch_add(1, Ordering::SeqCst);
            held_book.carry_check_in(f, |f| {
                count.fetch_add(1, Ordering::SeqCst);
                let (backbone, f) = (backbone.clone(), f.clone());
                tokio::spawn(async move { backbone.carry_completion(f).await });
            })
        }));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let spans = capture.spans().unwrap();
                let up = |channel| spans.iter().any(|s| s["name"] == "rdm.mesh.connection.update.via-neighbour-up" && s["attributes"]["channel"] == channel);
                if up("mesh:mesh1") && up("backbone") { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("live mesh and backbone neighbours");
        crate::forward_gossip_probe(&sender, &Frame::MeshStatus { mesh: "mesh3".into(), status: "ready-for-traffic".into(), publisher: "mesh3.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10_000 }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while receiver.mesh_status("mesh3").is_none() { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.unwrap();
        let changes = receiver.book.birth_changes();
        let before = *changes.borrow();
        let completion = kind == "state-committed" || kind == "traffic-opened";
        let publisher = if !completion || matches!(story, HookStory::AdminAuthor) { "mesh1.admin.2" } else { "mesh1.rpc.2" };
        let hook = rdm_core::RoundHook { fabric_id: fabric.clone(), node_id: subject_id, incarnation: subject_incarnation, build_id: key.build_id.clone(), attempt: key.attempt, operation: key.kind.operation(&fabric), publisher: publisher.into(), event_at_rafka_ms: 1 };
        for receipt in 1..=if matches!(story, HookStory::Repeat | HookStory::ForwardedRepeat) { 2 } else { 1 } {
            let mut offered = hook.clone();
            // Evidence time and forwarding metadata change the wire bytes, never the identity.
            offered.event_at_rafka_ms = receipt;
            let forwarded_by = matches!(story, HookStory::ForwardedRepeat).then(|| format!("mesh{receipt}.admin.3"));
            let f = match kind {
                "state-committing" => Frame::StateCommitting { hook: offered, forwarded_by },
                "state-committed" => Frame::StateCommitted { hook: offered, forwarded_by },
                "traffic-opening" => Frame::TrafficOpening { hook: offered, forwarded_by },
                _ => Frame::TrafficOpened { hook: offered, forwarded_by },
            };
            crate::forward_gossip_probe(&sender, &f).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while capture.spans().unwrap().iter().filter(|s| {
                    s["name"] == "rdm.mesh.membership.update.via-round-frame" && s["attributes"]["kind"] == kind && s["attributes"]["node"] == "mesh1.admin.1"
                    || s["name"] == "rdm.mesh.membership.update.via-duplicate" && s["attributes"]["frame"] == kind
                }).count() < receipt as usize { tokio::time::sleep(Duration::from_millis(10)).await; }
            }).await.expect("the hook reaches the RDM receiver");
        }
        if completion && !matches!(story, HookStory::Closed | HookStory::AdminAuthor) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.mesh.membership.update.via-round-frame" && s["attributes"]["kind"] == kind && s["attributes"]["via"] == "backbone" && s["attributes"]["node"] == "mesh2.admin.1") { tokio::time::sleep(Duration::from_millis(10)).await; }
            }).await.expect("the ordinary check-in reaches the live backbone peer");
        }
        assert_eq!(*changes.borrow(), before, "hooks never change birth lifecycle");
        assert_eq!(*checklist_changes.borrow(), checklist_before, "gossip never notifies or changes the directed checklist");
        assert!(!book.complete(&key), "even completion gossip cannot check in the planned birth");
        assert!(if matches!(story, HookStory::Closed) { !book.is_open(&key) } else { book.is_open(&key) }, "gossip never changes the round book");
        sender_ep.close().await;
        receiver_ep.close().await;
        peer_ep.close().await;
        hook
    });
    let spans = capture.spans().unwrap();
    for sp in spans.iter().filter(|s| s["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.membership."))) { println!("{}", serde_json::to_string(sp).unwrap()); }
    let applies: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-round-frame" && s["attributes"]["kind"] == kind && s["attributes"]["node"] == "mesh1.admin.1").collect();
    let completion = kind == "state-committed" || kind == "traffic-opened";
    let expected = usize::from(completion && !matches!(story, HookStory::Closed | HookStory::AdminAuthor));
    assert_eq!(applies.len(), 1, "each identity has exactly one ordinary apply span");
    assert_eq!(carried.load(Ordering::SeqCst), expected, "only an ordinary check-in of an open round is carried once");
    assert_eq!(called.load(Ordering::SeqCst), usize::from(completion), "a duplicate never invokes the carrier");
    assert_eq!(applies[0]["attributes"]["carried"], if !completion { "not-a-check-in" } else if matches!(story, HookStory::Closed) { "closed-round" } else if matches!(story, HookStory::AdminAuthor) { "node-admin" } else { "carried" });
    let backbone_receipts = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-round-frame" && s["attributes"]["kind"] == kind && s["attributes"]["via"] == "backbone" && s["attributes"]["node"] == "mesh2.admin.1").count();
    assert_eq!(backbone_receipts, expected, "the real backbone receives exactly the carried fact");
    if matches!(story, HookStory::Repeat | HookStory::ForwardedRepeat) {
        let identity = format!("{kind} {} {} {} {} {}/{}", hook.fabric_id, hook.build_id, hook.attempt, hook.operation, hook.node_id, hook.incarnation);
        assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-duplicate" && s["attributes"]["frame"] == kind && s["attributes"]["identity"] == identity && s["attributes"]["outcome"] == "ignored").count(), 1);
    }
}

macro_rules! round_cells {
    ($control:ident, $repeat:ident, $kind:literal) => {
        /// CONTRACT: an unrelated newer frame does not supersede a first hook's span or open-round carry.
        #[test] fn $control() { round_hook_story($kind, HookStory::AfterUnrelated); }
        /// CONTRACT: a repeat by exact operation identity emits via-duplicate with no second apply or carrier invocation.
        #[test] fn $repeat() { round_hook_story($kind, HookStory::Repeat); }
    };
}
round_cells!(state_committing_after_unrelated_traffic_applies, state_committing_repeat_does_nothing, "state-committing");
round_cells!(state_committed_after_unrelated_traffic_applies, state_committed_repeat_does_nothing, "state-committed");
round_cells!(traffic_opening_after_unrelated_traffic_applies, traffic_opening_repeat_does_nothing, "traffic-opening");
round_cells!(traffic_opened_after_unrelated_traffic_applies, traffic_opened_repeat_does_nothing, "traffic-opened");

/// CONTRACT: forwarded ordinary check-ins of open rounds reach the backbone once; metadata never changes identity.
#[test] fn forwarded_state_check_in_is_carried_once() { round_hook_story("state-committed", HookStory::ForwardedRepeat); }
/// CONTRACT: both round kinds use the same open-round carrier after forwarding.
#[test] fn forwarded_traffic_check_in_is_carried_once() { round_hook_story("traffic-opened", HookStory::ForwardedRepeat); }
/// CONTRACT: a first hook of a closed round spans carried=closed-round and is never carried or counted as a check-in.
#[test] fn closed_state_round_check_in_is_not_carried() { round_hook_story("state-committed", HookStory::Closed); }
/// CONTRACT: closed open-traffic rounds have the same named outcome.
#[test] fn closed_traffic_round_check_in_is_not_carried() { round_hook_story("traffic-opened", HookStory::Closed); }
/// CONTRACT: a node-admin author publishes on the backbone itself and is never carried by another admin.
#[test] fn admin_state_check_in_is_not_carried() { round_hook_story("state-committed", HookStory::AdminAuthor); }
/// CONTRACT: the author rule also applies to traffic-opened.
#[test] fn admin_traffic_check_in_is_not_carried() { round_hook_story("traffic-opened", HookStory::AdminAuthor); }
