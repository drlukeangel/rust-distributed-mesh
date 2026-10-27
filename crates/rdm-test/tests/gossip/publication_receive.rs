//! Snapshot/delta receiver contracts, through the live channel and real topology-read door.
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::{learn_addresses, mesh_channel, backbone_channel, Backbone, Frame, TopologyView};
use rdm_transport::snapshot::{Chunk, PublisherId, Taken};
use std::time::Duration;
async fn endpoint() -> (iroh::Endpoint, iroh_gossip::net::Gossip, iroh::protocol::Router) {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    (ep, gossip, router)
}

async fn send(sender: &iroh_gossip::api::GossipSender, receiver: &TopologyView, frame: Frame, serial: u64) {
    // A following status on the same channel proves receipt has finished.
    sender.broadcast_neighbors(frame.authored(&crate::gossip_author(receiver.fabric_id()), 0, 1).encode().into()).await.unwrap();
    sender.broadcast_neighbors(Frame::MeshStatus { mesh: "barrier".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.2".into(), forwarded_by: None, changed_at_rafka_ms: serial }.authored(&crate::gossip_author(receiver.fabric_id()), 0, 1).encode().into()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while receiver.mesh_status("barrier").is_none_or(|s| s.changed_at_rafka_ms < serial) { tokio::time::sleep(Duration::from_millis(10)).await; }
    }).await.expect("the following frame was applied on this channel");
}



#[derive(Clone, Copy)] enum Story { Stale, ReadFloor, Duplicate, Inconsistent, Chunks, PendingConflict, Gap, DeltaApply, DeltaRepeat, DeltaInconsistent, DeltaStale, LowerStamp, ReadRepair, CrossReadRefresh, CrossReadConflict, CrossDeltaBase, NewPublication, CrossRepeat, TwoForwarders }
fn story(story: Story) {
    let suffix = match story { Story::Stale => "stale", Story::ReadFloor => "read-floor", Story::Duplicate => "duplicate", Story::Inconsistent => "inconsistent", Story::Chunks => "chunks", Story::PendingConflict => "pending-conflict", Story::Gap => "gap", Story::DeltaApply => "delta-apply", Story::DeltaRepeat => "delta-repeat", Story::DeltaInconsistent => "delta-inconsistent", Story::DeltaStale => "delta-stale", Story::LowerStamp => "lower-stamp", Story::ReadRepair => "read-repair", Story::CrossReadRefresh => "cross-read-refresh", Story::CrossReadConflict => "cross-read-conflict", Story::CrossDeltaBase => "cross-delta-base", Story::NewPublication => "new-publication", Story::CrossRepeat => "cross-repeat", Story::TwoForwarders => "two-forwarders" };
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance"); std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, &format!("rdm-publication-{suffix}")).unwrap();
    capture.run(2, async {
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


        let mut role = None;
        let mut backbone_sender = None;
        if matches!(story, Story::CrossReadRefresh | Story::CrossReadConflict | Story::CrossDeltaBase | Story::NewPublication | Story::CrossRepeat) {
            let address = iroh::EndpointAddr::new(sender_ep.id()).with_ip_addr(sender_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap());
            let mut backbone_channel = gossip.subscribe(backbone_channel(&fabric), vec![]).await.unwrap();
            role = Some(Backbone::join(&receiver_gossip, &receiver_ep, &receiver, "mesh1", rdm_core::PublisherId { node: "mesh1.admin.1".to_string(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }, vec![address]).await.unwrap());
            tokio::time::timeout(Duration::from_secs(5), backbone_channel.joined()).await.unwrap().unwrap();
            backbone_sender = Some(backbone_channel.split().0);
        }
        let publisher = PublisherId { node: "mesh2.admin.1".into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        let id = NodeId::mint();
        let digest = MeshDigest { held_hash: Default::default(), fabric_id: fabric, node: MeshNode { node_id: id.clone(), name: "mesh2.rpc.1".parse().unwrap(), endpoint_id: EndpointId(iroh::SecretKey::generate().public().to_string()), transport_addr: "127.0.0.1:12001".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, runtime: None }, status: MemberStatus::ReadyForTraffic, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: Some(MeshId::mint()), in_flight: None, config: Default::default(), load: None, gossip: None, is_primary: None, is_fabric_primary: None };
        let snapshot = |version, publication, index, count, digests: Vec<MeshDigest>, stamp| Frame::MeshTopology { mesh: "mesh2".into(), publisher: publisher.clone(), forwarded_by: Some("mesh1.admin.2".into()), topology_version: version, published_at_rafka_ms: stamp, snapshot_id: publication, chunk_index: index, chunk_count: count, digests, in_flight: vec![], deleted: vec![] };
        let read = |digests| Chunk { mesh: "mesh2".into(), publisher: publisher.clone(), forwarded_by: None, topology_version: 10, snapshot_id: 3, chunk_index: 0, chunk_count: 1, digests, in_flight: vec![], departed: vec![] };
        let delta = |base, version, changed: Vec<MeshDigest>, stamp| Frame::MeshTopologyDelta { mesh: "mesh2".into(), source_publisher: publisher.clone(), base_version: base, topology_version: version, published_at_rafka_ms: stamp, changed, removed: vec![], in_flight: vec![], deleted: vec![] };
        let baseline = snapshot(10, 1, 0, 1, vec![digest.clone()], 100);
        if matches!(story, Story::ReadFloor | Story::DeltaStale | Story::CrossReadRefresh | Story::CrossReadConflict | Story::CrossDeltaBase | Story::NewPublication) { assert!(matches!(receiver.take_read_chunk(read(vec![digest.clone()]), "mesh1"), Taken::Installed(_))); }
        else { send(&sender, &receiver, baseline.clone(), 1).await; }
        assert_eq!(receiver.held_source_version("mesh2"), Some((publisher.clone(), 10)));
        let heard_before = receiver.digest_view.get(id.as_str()).unwrap().1;
        let before = receiver.digest_view.all().into_iter().find(|d| d.node.node_id == id).unwrap();
        let mut changed = digest.clone(); changed.config.insert("version".into(), "eleven".into());
        let mut expected = 10;
        match story {
            Story::CrossReadRefresh | Story::CrossReadConflict | Story::CrossDeltaBase | Story::NewPublication | Story::CrossRepeat => {
                let mut full = snapshot(if matches!(story, Story::CrossDeltaBase) { 11 } else { 10 }, if matches!(story, Story::CrossRepeat) { 1 } else { 2 }, 0, 1, vec![if matches!(story, Story::CrossReadRefresh | Story::NewPublication | Story::CrossRepeat) { let mut current = digest.clone(); if matches!(story, Story::NewPublication) { current.digest_seq += 1; } current } else { changed.clone() }], if matches!(story, Story::CrossRepeat) { 100 } else { 101 });
                if let Frame::MeshTopology { forwarded_by, .. } = &mut full { *forwarded_by = None; }
                send(backbone_sender.as_ref().unwrap(), &receiver, full, 2).await;
                if matches!(story, Story::CrossRepeat) { assert_eq!(receiver.digest_view.get(id.as_str()).unwrap().1, heard_before, "a publication already installed on another channel is not heard again"); }
                if matches!(story, Story::NewPublication) {
                    assert!(receiver.digest_view.get(id.as_str()).unwrap().1 > heard_before, "a fresh publication at an unchanged material version carries new hearing evidence");
                }
                if matches!(story, Story::CrossDeltaBase) {
                    send(&sender, &receiver, delta(11, 12, vec![changed.clone()], 102), 3).await;
                    assert!(receiver.desynced_sources().is_empty(), "a delta whose base is the version already installed on the backbone applies without repair");
                    expected=12;
                }
            },
            Story::Stale | Story::ReadFloor => send(&sender, &receiver, snapshot(9, 2, 0, 1, vec![], 200), 2).await,
            Story::Duplicate => send(&sender, &receiver, baseline, 2).await,
            Story::Inconsistent => send(&sender, &receiver, snapshot(10, 1, 0, 1, vec![changed.clone()], 100), 2).await,
            Story::LowerStamp => { send(&sender, &receiver, snapshot(11, 2, 0, 1, vec![changed.clone()], 1), 2).await; expected = 11; },
            Story::Chunks | Story::PendingConflict => {
                let mut second = digest.clone(); second.node.node_id = NodeId::mint(); second.node.name = "mesh2.rpc.2".parse().unwrap();
                let first = snapshot(11, 2, 0, 2, vec![changed.clone()], 101);
                let last = snapshot(11, 2, 1, 2, vec![second.clone()], 101);
                if matches!(story, Story::Chunks) {
                    send(&sender, &receiver, last.clone(), 2).await;
                    send(&sender, &receiver, last.clone(), 3).await;
                } else {
                    send(&sender, &receiver, first.clone(), 2).await;
                    let mut wrong = changed.clone(); wrong.config.insert("contradiction".into(), "yes".into());
                    send(&sender, &receiver, snapshot(11, 2, 0, 2, vec![wrong], 101), 3).await;
                }
                assert_eq!(receiver.held_source_version("mesh2"), Some((publisher.clone(), 10)), "unfinished publication never advances the installed floor");
                assert_eq!(receiver.digest_view.all().into_iter().find(|d| d.node.node_id == id), Some(before.clone()), "no partial projection install");
                assert!(!receiver.digest_view.all().iter().any(|d| d.node.node_id == second.node.node_id));
                send(&sender, &receiver, if matches!(story, Story::Chunks) { first } else { last }, 4).await;
                assert!(receiver.digest_view.all().iter().any(|d| d.node.node_id == second.node.node_id));
                expected = 11;
            },
            Story::Gap | Story::ReadRepair => {
                send(&sender, &receiver, delta(20, 21, vec![changed.clone()], if matches!(story, Story::Gap) { 1 } else { 101 }), 2).await;
                assert_eq!(receiver.held_source_version("mesh2"), Some((publisher.clone(), 10)));
                assert!(receiver.desynced_sources().iter().any(|(m, _)| m=="mesh2"), "a base ahead requests repair, even with an earlier publication timestamp");
                if matches!(story, Story::ReadRepair) {
                    send(&sender, &receiver, baseline, 3).await;
                    assert!(!receiver.desynced_sources().is_empty(), "repeated gossip does nothing, including no repair completion");
                    // A gossip publication is half assembled at this same version. Its snapshot
                    // ordinal belongs to the author, whereas the read's chunks belong to its answer.
                    send(&sender, &receiver, snapshot(10, 99, 0, 2, vec![digest.clone()], 100), 4).await;
                    assert!(matches!(receiver.take_read_chunk(read(vec![digest.clone()]), "mesh1"), Taken::Installed(_)), "the directed repair read installs the held version despite unfinished gossip and resumes the source");
                    assert!(receiver.desynced_sources().is_empty());
                    send(&sender, &receiver, delta(10, 11, vec![changed.clone()], 102), 5).await; expected=11;
                }
            },
            Story::DeltaApply | Story::DeltaRepeat | Story::DeltaInconsistent => {
                let frame = delta(10, 11, vec![changed.clone()], 101);
                send(&sender, &receiver, frame.clone(), 2).await;
                if matches!(story, Story::DeltaRepeat) { send(&sender, &receiver, frame, 3).await; }
                if matches!(story, Story::DeltaInconsistent) { let mut wrong=changed.clone();wrong.config.insert("contradiction".into(), "yes".into());send(&sender, &receiver, delta(10, 11, vec![wrong], 101), 3).await; }
                expected=11;
            },
            Story::TwoForwarders => {
                let owner_b = rdm_transport::envelope::Birth { fabric: receiver.fabric_id().clone(), node: NodeId::mint(), incarnation: IncarnationId::mint() };
                send(&sender, &receiver, baseline.clone().authored(&owner_b, 0, 100), 2).await;
                let installs = || capture.spans().unwrap().iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-snapshot-installed").count();
                assert_eq!(installs(), 2, "a second forwarder's counter 1 is a distinct publication install");
                let repeated = |by: &str| { let mut f = baseline.clone(); if let Frame::MeshTopology { forwarded_by, .. } = &mut f { *forwarded_by = Some(by.into()); } f };
                send(&sender, &receiver, repeated("mesh1.admin.4"), 3).await;
                assert_eq!(installs(), 2, "the first owner's resend is still a duplicate after the other owner's publication");
                send(&sender, &receiver, snapshot(10, 2, 0, 1, vec![digest.clone()], 101), 4).await;
                assert_eq!(installs(), 3, "the first owner's new counter is a new publication install");
                send(&sender, &receiver, repeated("mesh1.admin.5"), 5).await;
                assert_eq!(installs(), 3, "an older exact publication id stays seen after a newer counter");
                assert_eq!(receiver.digest_view.get(id.as_str()).unwrap().1, heard_before, "a peer mesh projection on this mesh channel supplies no liveness");
            },
            Story::DeltaStale => send(&sender, &receiver, delta(8, 9, vec![], 200), 2).await,
        }
        assert_eq!(receiver.held_source_version("mesh2"), Some((publisher, expected)));
        let held=receiver.digest_view.all().into_iter().find(|d| d.node.node_id == id).unwrap();
        assert_eq!(held.config, if expected>=11 { changed.config } else { before.config }, "only a newer complete publication or applicable delta replaces content");
        drop(role);
        sender_ep.close().await; receiver_ep.close().await;
    });
    let spans=capture.spans().unwrap();
    for s in spans.iter().filter(|s| s["name"].as_str().is_some_and(|n| n.starts_with("rdm.mesh.topology."))) { println!("{}",serde_json::to_string(s).unwrap()); }
    let outcome=match story { Story::Stale | Story::ReadFloor | Story::DeltaStale => "rdm.mesh.topology.reject.via-stale", Story::Duplicate | Story::DeltaRepeat | Story::Chunks | Story::CrossRepeat => "rdm.mesh.topology.update.via-duplicate", Story::Inconsistent | Story::DeltaInconsistent | Story::PendingConflict | Story::CrossReadConflict => "rdm.mesh.topology.reject.via-inconsistent", Story::Gap | Story::ReadRepair => "rdm.mesh.topology.update.via-gap", _=>"" };
    if !outcome.is_empty() {
        let named=spans.iter().find(|s| s["name"]==outcome).expect("the framework names the publication outcome");
        if matches!(story, Story::Gap | Story::ReadRepair) { assert_eq!(named["attributes"]["repair_requested"], "true"); }
        else if matches!(story, Story::Duplicate | Story::DeltaRepeat | Story::Chunks | Story::CrossRepeat) { assert_eq!(named["attributes"]["outcome"], "ignored"); }
    }
    let snapshots=if matches!(story, Story::TwoForwarders) { 3 } else if matches!(story, Story::Chunks | Story::PendingConflict | Story::LowerStamp | Story::ReadRepair | Story::CrossDeltaBase | Story::CrossReadRefresh | Story::NewPublication) { 2 } else { 1 };
    assert_eq!(spans.iter().filter(|s|s["name"]=="rdm.mesh.topology.update.via-snapshot-installed").count(),snapshots);
    let deltas=usize::from(matches!(story, Story::DeltaApply | Story::DeltaRepeat | Story::DeltaInconsistent | Story::ReadRepair | Story::CrossDeltaBase));
    assert_eq!(spans.iter().filter(|s|s["name"]=="rdm.mesh.topology.update.via-delta").count(),deltas);
}

/// CONTRACT: Stale follows the installed source version and publication identity, with no partial install.
#[test] fn publication_stale() { story(Story::Stale); }

/// CONTRACT: ReadFloor follows the installed source version and publication identity, with no partial install.
#[test] fn publication_readfloor() { story(Story::ReadFloor); }

/// CONTRACT: Duplicate follows the installed source version and publication identity, with no partial install.
#[test] fn publication_duplicate() { story(Story::Duplicate); }

/// CONTRACT: Inconsistent follows the installed source version and publication identity, with no partial install.
#[test] fn publication_inconsistent() { story(Story::Inconsistent); }

/// CONTRACT: Chunks follows the installed source version and publication identity, with no partial install.
#[test] fn publication_chunks() { story(Story::Chunks); }

/// CONTRACT: PendingConflict follows the installed source version and publication identity, with no partial install.
#[test] fn publication_pendingconflict() { story(Story::PendingConflict); }

/// CONTRACT: Gap follows the installed source version and publication identity, with no partial install.
#[test] fn publication_gap() { story(Story::Gap); }

/// CONTRACT: DeltaApply follows the installed source version and publication identity, with no partial install.
#[test] fn publication_deltaapply() { story(Story::DeltaApply); }

/// CONTRACT: DeltaRepeat follows the installed source version and publication identity, with no partial install.
#[test] fn publication_deltarepeat() { story(Story::DeltaRepeat); }

/// CONTRACT: DeltaInconsistent follows the installed source version and publication identity, with no partial install.
#[test] fn publication_deltainconsistent() { story(Story::DeltaInconsistent); }

/// CONTRACT: DeltaStale follows the installed source version and publication identity, with no partial install.
#[test] fn publication_deltastale() { story(Story::DeltaStale); }

/// CONTRACT: LowerStamp follows the installed source version and publication identity, with no partial install.
#[test] fn publication_lowerstamp() { story(Story::LowerStamp); }

/// CONTRACT: ReadRepair follows the installed source version and publication identity, with no partial install.
#[test] fn publication_readrepair() { story(Story::ReadRepair); }

/// CONTRACT: a first new gossip publication at a read-installed version preserves the material image and is heard.
#[test] fn publication_cross_read_refresh() { story(Story::CrossReadRefresh); }
/// CONTRACT: equal conflicting content on the backbone cannot override a read-installed projection.
#[test] fn publication_cross_read_conflict() { story(Story::CrossReadConflict); }
/// CONTRACT: a delta uses the actual installed version, including an install from the other channel.
#[test] fn publication_cross_channel_delta_base() { story(Story::CrossDeltaBase); }

/// CONTRACT: a new publication at an unchanged topology version carries fresh hearing evidence.
#[test] fn publication_fresh_publication_at_unchanged_version_is_heard() { story(Story::NewPublication); }

/// CONTRACT: a publication forwarded onto another channel retains its identity and runs no effect twice.
#[test] fn publication_repeat_on_another_channel_is_duplicate() { story(Story::CrossRepeat); }

/// Forwarding births namespace equal counters; exact older resends remain duplicates.
#[test] fn distinct_forwarders_at_one_material_version_install_once_each() { story(Story::TwoForwarders); }
