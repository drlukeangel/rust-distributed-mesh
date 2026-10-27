//! Digest replacement: exact birth, digest_seq, and the heartbeat's wire content (never status).
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::{learn_addresses, mesh_channel, Frame, TopologyView};
use std::time::Duration;

async fn endpoint() -> (iroh::Endpoint, iroh_gossip::net::Gossip, iroh::protocol::Router) {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    (ep, gossip, router)
}

async fn send(sender: &iroh_gossip::api::GossipSender, receiver: &TopologyView, digest: MeshDigest, serial: u64) {
    // Neighbor delivery passes identical heartbeat bytes to RDM without waiting for swarm dedup
    // retention. A following status on the same channel proves receipt has finished.
    sender.broadcast_neighbors(Frame::Digest { digest }.authored(&crate::gossip_author(receiver.fabric_id()), 0, 1).encode().into()).await.unwrap();
    sender.broadcast_neighbors(Frame::MeshStatus { mesh: "barrier".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.2".into(), forwarded_by: None, changed_at_rafka_ms: serial }.authored(&crate::gossip_author(receiver.fabric_id()), 0, 1).encode().into()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while receiver.mesh_status("barrier").is_none_or(|s| s.changed_at_rafka_ms < serial) { tokio::time::sleep(Duration::from_millis(10)).await; }
    }).await.expect("the following frame was applied on this channel");
}

#[derive(Clone, Copy)]
enum Story { Stale, Duplicate, Inconsistent, Newer, NewBirth, ReadFloor }
fn story(story: Story) {
    let suffix = match story { Story::Stale => "stale", Story::Duplicate => "duplicate", Story::Inconsistent => "inconsistent", Story::Newer => "newer", Story::NewBirth => "new-birth", Story::ReadFloor => "read-floor" };
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, &format!("rdm-digest-{suffix}")).unwrap();
    let (node_id, incarnation) = capture.run(2, async {
        let (sender_ep, gossip, _sender_router) = endpoint().await;
        let (receiver_ep, receiver_gossip, _receiver_router) = endpoint().await;
        let socket = sender_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let address = iroh::EndpointAddr::new(sender_ep.id()).with_ip_addr(socket);
        learn_addresses(&receiver_ep, &[address.clone()]).unwrap();
        learn_addresses(&sender_ep, &[iroh::EndpointAddr::new(receiver_ep.id()).with_ip_addr(receiver_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap())]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let mut topic = gossip.subscribe(mesh_channel(&fabric, &mesh), vec![]).await.unwrap();
        let receiver = TopologyView::join(&receiver_gossip, &receiver_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![address], &[]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), topic.joined()).await.unwrap().unwrap();
        let (sender, _rx) = topic.split();
        let key = iroh::SecretKey::generate().public();
        let other_key = iroh::SecretKey::generate().public();
        let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
        let digest = MeshDigest { held_hash: Default::default(), fabric_id: fabric, node: MeshNode { node_id: node_id.clone(), name: "mesh1.rpc.2".parse().unwrap(), endpoint_id: EndpointId(key.to_string()), transport_addr: "127.0.0.1:12001".parse().unwrap(), incarnation: incarnation.clone(), supersedes: None, runtime: None }, status: MemberStatus::ReadyForTraffic, node_state: Default::default(), admin_api_base: None, digest_seq: 100, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: Some(mesh), in_flight: None, load: None, gossip: None, is_primary: None, is_fabric_primary: None };
        if matches!(story, Story::ReadFloor) {
            let read = rdm_transport::snapshot::Chunk {
                mesh: "mesh1".into(), publisher: rdm_transport::snapshot::PublisherId { node: "mesh1.admin.2".into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() },
                forwarded_by: None, topology_version: 5, snapshot_id: 1, chunk_index: 0, chunk_count: 1,
                digests: vec![digest.clone()], in_flight: vec![], deleted: vec![],
            };
            assert!(matches!(receiver.take_read_chunk(read, "mesh1"), rdm_transport::snapshot::Taken::Installed(_)), "the real topology-read path installs the held digest");
        } else {
            send(&sender, &receiver, digest.clone(), 1).await;
        }
        assert!(receiver.digest_view.started(&node_id, &incarnation, &digest.node.name));
        let before = receiver.digest_view.get(node_id.as_str()).unwrap().0;
        let changes = receiver.digest_view.digest_changes();
        let births = receiver.digest_view.birth_changes();
        let overlays = receiver.digest_view.overlay_changes();
        let counts = (*changes.borrow(), *births.borrow(), *overlays.borrow());
        let mut offered = digest.clone();
        match story {
            Story::Stale | Story::ReadFloor => { offered.digest_seq = 90; offered.data_dir = Some("obsolete=yes".into()); }
            Story::Duplicate => {
                offered.status = MemberStatus::Leaving; // absent from the wire; the held status is Ready.
                assert_eq!(Frame::Digest { digest: offered.clone() }.authored(&crate::gossip_author(&digest.fabric_id), 0, 1).encode(), Frame::Digest { digest: digest.clone() }.authored(&crate::gossip_author(&digest.fabric_id), 0, 1).encode());
                receiver.location_view().forget(key);
            }
            Story::Inconsistent => { offered.node.endpoint_id = EndpointId(other_key.to_string()); }
            Story::Newer => { offered.digest_seq = 101; offered.node.transport_addr = "127.0.0.1:12002".parse().unwrap(); offered.data_dir = Some("new=yes".into()); }
            Story::NewBirth => { offered.digest_seq = 1; offered.node.incarnation = IncarnationId::mint(); offered.node.supersedes = Some(incarnation.clone()); }
        }
        send(&sender, &receiver, offered.clone(), 2).await;
        if matches!(story, Story::ReadFloor) {
            let mut even_older = offered.clone(); even_older.digest_seq = 80;
            send(&sender, &receiver, even_older, 3).await;
        }
        if matches!(story, Story::Newer | Story::NewBirth) {
            let held = receiver.digest_view.get(node_id.as_str()).unwrap().0;
            assert_eq!(held.digest_seq, offered.digest_seq);
            assert_eq!(held.node, offered.node);
            assert_eq!(*changes.borrow(), counts.0 + 1);
            if matches!(story, Story::Newer) { assert_eq!(held.status, MemberStatus::ReadyForTraffic); }
        } else {
            assert_eq!(receiver.digest_view.get(node_id.as_str()).unwrap().0, before, "refused or duplicate heartbeat changes no projection or held status");
            assert_eq!((*changes.borrow(), *births.borrow(), *overlays.borrow()), counts);
        }
        assert!(receiver.digest_view.in_flight().is_empty());
        assert!(!receiver.digest_view.is_deleted(node_id.as_str()));
        if matches!(story, Story::Duplicate) { assert!(receiver.location_view().addrs(key).is_empty(), "a duplicate cannot fill a forgotten address"); }
        if matches!(story, Story::Inconsistent) { assert!(receiver.location_view().addrs(other_key).is_empty(), "an inconsistent digest cannot install its offered endpoint"); }
        sender_ep.close().await; receiver_ep.close().await;
        (node_id, incarnation)
    });
    let spans = capture.spans().unwrap();
    for s in spans.iter().filter(|s| s["attributes"]["frame"] == "digest" || matches!(s["name"].as_str(), Some("rdm.mesh.topology.update.via-digest" | "rdm.mesh.topology.update.via-digest-receipt"))) { println!("{}", serde_json::to_string(s).unwrap()); }
    let apply_count = match story { Story::Newer | Story::NewBirth => 2, Story::ReadFloor => 0, _ => 1 };
    assert_eq!(spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-digest" && s["attributes"]["node_id"] == node_id.to_string()).count(), apply_count);
    {
        let receipt = spans.iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-digest-receipt").last().unwrap();
        let accepted = matches!(story, Story::Newer | Story::NewBirth);
        assert_eq!(receipt["attributes"]["addresses_registered"], if accepted { "1" } else { "0" });
        assert_eq!(receipt["attributes"]["applied"], if accepted { "true" } else { "false" });
    }
    if matches!(story, Story::ReadFloor) {
        assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.topology.reject.via-stale" && s["attributes"]["held_key"] == "digest_seq=100" && s["attributes"]["frame_key"] == "digest_seq=80"), "a refused frame never becomes the held floor; both refusals must name the installed 100");
    }
    let subject = format!("{node_id}/{incarnation}");
    let outcome = match story { Story::Stale | Story::ReadFloor => "rdm.mesh.topology.reject.via-stale", Story::Duplicate => "rdm.mesh.topology.update.via-duplicate", Story::Inconsistent => "rdm.mesh.topology.reject.via-inconsistent", _ => return };
    let refusal = spans.iter().find(|s| s["name"] == outcome && s["attributes"]["frame"] == "digest").expect("the receive framework names the digest outcome");
    if matches!(story, Story::Duplicate) {
        assert_eq!(refusal["attributes"]["identity"], format!("{subject} digest_seq=100"));
        assert_eq!(refusal["attributes"]["outcome"], "ignored");
    } else {
        assert_eq!(refusal["attributes"]["subject"], subject);
        if matches!(story, Story::Inconsistent) { assert_eq!(refusal["attributes"]["revision"], "100"); }
        else { assert_eq!(refusal["attributes"]["held_key"], "digest_seq=100"); assert_eq!(refusal["attributes"]["frame_key"], "digest_seq=90"); }
    }
}

/// CONTRACT: a lower sequence of the same birth is named stale, never a rollback.
#[test] fn receiver_digest_newer_then_older_never_regresses() { story(Story::Stale); }
/// CONTRACT: the identical heartbeat, despite the independent held status, is ignored by name.
#[test] fn receiver_digest_repeat_is_duplicate_and_changes_nothing() { story(Story::Duplicate); }
/// CONTRACT: equal revision with different wire content is inconsistent, never address evidence.
#[test] fn receiver_digest_equal_sequence_with_different_content_is_inconsistent() { story(Story::Inconsistent); }
/// CONTRACT: a higher sequence installs and emits the ordinary apply span.
#[test] fn receiver_digest_higher_sequence_installs_the_projection() { story(Story::Newer); }
/// CONTRACT: a new exact birth starts its own sequence and does not inherit its predecessor's floor.
#[test] fn receiver_digest_new_incarnation_starts_its_own_sequence() { story(Story::NewBirth); }
/// CONTRACT: the held sequence installed by a topology read is also the floor for a direct heartbeat.
#[test] fn receiver_digest_below_a_read_installed_sequence_is_stale() { story(Story::ReadFloor); }
