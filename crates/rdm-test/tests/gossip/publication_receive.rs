//! Aggregate/delta receiver contracts, through the live channels and real `TopologyService`: an
//! aggregate on the backbone (the one channel it travels on, Q387), a differential on the mesh channel.
use rdm_core::{EndpointId, FabricId, IncarnationId, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_gossip::location::learn_addresses;
use rdm_gossip::frame::Frame;
use rdm_gossip::topology::TopologyView;
use rdm_gossip::aggregate::{Chunk, PublisherId, Taken};
use rdm_gossip::envelope::Birth;
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
    sender.broadcast_neighbors(Frame::MeshStatus { mesh: "barrier".into(), status: "ready".into(), publisher: "mesh1.node-admin.2".into(), forwarded_by: None, changed_at_rafka_ms: serial }.authored(&crate::gossip_author(receiver.fabric_id()), 0, 1).encode().into()).await.unwrap();
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
        let mut subscription = gossip.subscribe(rdm_gossip::channel::ChannelId::mesh(&fabric, &mesh).topic(), vec![]).await.unwrap();
        let receiver = rdm_gossip::topology::TopologyView::new(&fabric, "mesh1", &mesh, "mesh1.node-admin.1", rdm_transport::clock::os_clock(), Default::default());
        let receiver_channels = rdm_gossip::channel::Channels::new(&receiver_gossip, &receiver_ep, &receiver).unwrap();
        receiver_channels.join(rdm_gossip::channel::ChannelId::mesh(receiver.fabric_id(), receiver.mesh_id()), vec![address]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), subscription.joined()).await.unwrap().unwrap();
        let (sender, _rx) = subscription.split();


        // The receiver, a node-admin, hears mesh2's aggregate on the backbone.
        let address = iroh::EndpointAddr::new(sender_ep.id()).with_ip_addr(sender_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap());
        let mut backbone_channel = gossip.subscribe(rdm_gossip::channel::ChannelId::backbone(&fabric).topic(), vec![]).await.unwrap();
        receiver_channels.set_publisher(&rdm_core::PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() });
        receiver_channels.join(rdm_gossip::channel::ChannelId::backbone(&fabric), vec![address]).await.unwrap();
        let role = Some(receiver_channels.backbone().expect("the backbone is joined"));
        tokio::time::timeout(Duration::from_secs(5), backbone_channel.joined()).await.unwrap().unwrap();
        let backbone = backbone_channel.split().0;
        let publisher = PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        let author = Birth::publisher(&fabric, &publisher);
        // The version this node holds of mesh2, whichever channel or read wrote it.
        let installed = || receiver.held_copy("mesh2").map(|(v, _)| v);
        let id = NodeId::mint();
        let digest = MeshDigest { fabric_id: fabric, node: MeshNode { mesh: "mesh2".into(), r#type: "node".into(), node_id: id.clone(), path: "mesh2.node".into(), name: "1".into(), endpoint_id: EndpointId(iroh::SecretKey::generate().public().to_string()), transport_addr: "127.0.0.1:12001".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, }, status: NodeStatus::Ready, digest_seq: 1, emitted_at_rafka_ms: 1, primary: Vec::new(), };
        // An aggregate of mesh2 by its mesh-primary: (mesh, version, author) names its chunk set and its
        // `time` the publication (Q389).
        let aggregate = |version, index, count, digests: Vec<MeshDigest>, stamp| Frame::aggregate("mesh2", None, &author, version, stamp, index, count, digests);
        let read = |digests| Chunk { mesh: "mesh2".into(), mesh_id: None, author: None, topology_version: 10, time_ms: 3, chunk_index: 0, chunk_count: 1, digests, runtimes: Vec::new() };
        // A differential of mesh2 whose nodes' details moved: each is an added entry (Q387). Its author is
        // the envelope's source and its version the envelope's sequence.
        let delta = |base, version, added: Vec<MeshDigest>, stamp| Frame::TopologyDelta { mesh: "mesh2".into(), mesh_id: None, base_version: base, chunk_index: 0, chunk_count: 1, added: added.iter().map(rdm_core::wire::NodeEntry::of).collect(), changed: vec![], removed: vec![]}
            .authored_delta(&rdm_gossip::envelope::Birth::publisher(receiver.fabric_id(), &publisher), version, stamp);
        let baseline = aggregate(10, 0, 1, vec![digest.clone()], 100);
        if matches!(story, Story::ReadFloor | Story::DeltaStale | Story::CrossReadRefresh | Story::CrossReadConflict | Story::CrossDeltaBase | Story::NewPublication) { assert!(matches!(receiver_channels.take_read_chunk(read(vec![digest.clone()]), "mesh1"), Taken::Written(_))); }
        else { send(&backbone, &receiver, baseline.clone(), 1).await; }
        assert_eq!(installed(), Some(10));
        // How long the node had gone unheard at one instant: asked again at that instant, the view answers
        // the same age only if nothing heard the node since.
        let heard_at = std::time::Instant::now();
        let heard_before = receiver.digest_view.get_at(id.as_str(), heard_at).unwrap().1;
        let before = receiver.digest_view.all().into_iter().find(|d| d.node.node_id == id).unwrap();
        // The content a later version changes: the node's transport address (version eleven's).
        let mut changed = digest.clone(); changed.node.transport_addr = "127.0.0.1:12011".parse().unwrap();
        let mut expected = 10;
        match story {
            Story::CrossReadRefresh | Story::CrossReadConflict | Story::CrossDeltaBase | Story::NewPublication | Story::CrossRepeat => {
                if matches!(story, Story::CrossRepeat) {
                    // The publication installed from the backbone, then a read of the same version: the
                    // read never replaces the author's publication, whose exact repeat is not heard again.
                    assert!(matches!(receiver_channels.take_read_chunk(read(vec![digest.clone()]), "mesh1"), Taken::Written(_)));
                }
                let full = aggregate(if matches!(story, Story::CrossDeltaBase) { 11 } else { 10 }, 0, 1, vec![if matches!(story, Story::CrossReadRefresh | Story::NewPublication | Story::CrossRepeat) { digest.clone() } else { changed.clone() }], if matches!(story, Story::CrossRepeat) { 100 } else { 101 });
                send(&backbone, &receiver, full, 2).await;
                if matches!(story, Story::CrossRepeat) { assert_eq!(receiver.digest_view.get_at(id.as_str(), heard_at).unwrap().1, heard_before, "a publication already installed on another channel is not heard again"); }
                if matches!(story, Story::NewPublication) {
                    assert!(receiver.digest_view.get(id.as_str()).unwrap().1 > heard_before, "a fresh publication at an unchanged material version carries new hearing evidence");
                }
                if matches!(story, Story::CrossDeltaBase) {
                    send(&sender, &receiver, delta(11, 12, vec![changed.clone()], 102), 3).await;
                    assert!(receiver.desynced_sources().is_empty(), "a delta whose base is the version already installed on the backbone applies without repair");
                    expected=12;
                }
            },
            Story::Stale | Story::ReadFloor => send(&backbone, &receiver, aggregate(9, 0, 1, vec![], 200), 2).await,
            Story::Duplicate => send(&backbone, &receiver, baseline, 2).await,
            Story::Inconsistent => send(&backbone, &receiver, aggregate(10, 0, 1, vec![changed.clone()], 100), 2).await,
            Story::LowerStamp => { send(&backbone, &receiver, aggregate(11, 0, 1, vec![changed.clone()], 1), 2).await; expected = 11; },
            Story::Chunks | Story::PendingConflict => {
                let mut second = digest.clone(); second.node.node_id = NodeId::mint(); second.node.path = "mesh2.node".into(); second.node.name = "2".into();
                let first = aggregate(11, 0, 2, vec![changed.clone()], 101);
                let last = aggregate(11, 1, 2, vec![second.clone()], 101);
                if matches!(story, Story::Chunks) {
                    send(&backbone, &receiver, last.clone(), 2).await;
                    send(&backbone, &receiver, last.clone(), 3).await;
                } else {
                    send(&backbone, &receiver, first.clone(), 2).await;
                    let mut wrong = changed.clone(); wrong.node.transport_addr = "127.0.0.1:12099".parse().unwrap();
                    send(&backbone, &receiver, aggregate(11, 0, 2, vec![wrong], 101), 3).await;
                }
                assert_eq!(installed(), Some(10), "unfinished publication never advances the held floor");
                assert_eq!(receiver.digest_view.all().into_iter().find(|d| d.node.node_id == id), Some(before.clone()), "no partial projection install");
                assert!(!receiver.digest_view.all().iter().any(|d| d.node.node_id == second.node.node_id));
                send(&backbone, &receiver, if matches!(story, Story::Chunks) { first } else { last }, 4).await;
                assert!(receiver.digest_view.all().iter().any(|d| d.node.node_id == second.node.node_id));
                expected = 11;
            },
            Story::Gap | Story::ReadRepair => {
                send(&sender, &receiver, delta(20, 21, vec![changed.clone()], if matches!(story, Story::Gap) { 1 } else { 101 }), 2).await;
                assert_eq!(installed(), Some(10));
                assert!(receiver.desynced_sources().iter().any(|(m, _)| m=="mesh2"), "a base ahead requests repair, even with an earlier publication timestamp");
                if matches!(story, Story::ReadRepair) {
                    send(&backbone, &receiver, baseline, 3).await;
                    assert!(!receiver.desynced_sources().is_empty(), "repeated gossip does nothing, including no repair completion");
                    // A later publication of the author is half assembled at this same version. Its chunk
                    // set is the author's (mesh, version, author), whereas the read's chunks belong to its answer.
                    send(&backbone, &receiver, aggregate(10, 0, 2, vec![digest.clone()], 150), 4).await;
                    assert!(matches!(receiver_channels.take_read_chunk(read(vec![digest.clone()]), "mesh1"), Taken::Written(_)), "the directed repair read installs the held version despite unfinished gossip and resumes the source");
                    assert!(receiver.desynced_sources().is_empty());
                    send(&sender, &receiver, delta(10, 11, vec![changed.clone()], 102), 5).await; expected=11;
                }
            },
            Story::DeltaApply | Story::DeltaRepeat | Story::DeltaInconsistent => {
                let frame = delta(10, 11, vec![changed.clone()], 101);
                send(&sender, &receiver, frame.clone(), 2).await;
                if matches!(story, Story::DeltaRepeat) { send(&sender, &receiver, frame, 3).await; }
                if matches!(story, Story::DeltaInconsistent) { let mut wrong=changed.clone();wrong.node.transport_addr = "127.0.0.1:12099".parse().unwrap();send(&sender, &receiver, delta(10, 11, vec![wrong], 101), 3).await; }
                expected=11;
            },
            Story::TwoForwarders => {
                // Two authors at one version (an old mesh-primary's aggregate and its successor's): the mesh's
                // version orders its topology, whichever mesh-primary sent it (Q389), so both list the same
                // nodes. Each chunk set is its (mesh, version, author), kept apart; the second author's
                // complete set is its own publication; each author's exact repeat is its own duplicate.
                let author_b = Birth { fabric: receiver.fabric_id().clone(), node: NodeId::mint(), incarnation: IncarnationId::mint() };
                let of_b = |index, count, digests: Vec<MeshDigest>| Frame::aggregate("mesh2", None, &author_b, 11, 120, index, count, digests);
                let installs = || capture.spans().unwrap().iter().filter(|s| s["name"] == "rdm.mesh.topology.update.via-full").count();
                let mut second = digest.clone(); second.node.node_id = NodeId::mint(); second.node.name = "2".into();
                // The first author's chunk 0 of 2 and the second author's chunk 1 of 2 at one version and
                // time: neither completes the other's set.
                send(&backbone, &receiver, aggregate(11, 0, 2, vec![changed.clone()], 120), 2).await;
                send(&backbone, &receiver, of_b(1, 2, vec![second.clone()]), 3).await;
                assert_eq!(installs(), 1, "two authors' chunks at one version are two sets: neither installs");
                assert_eq!(installed(), Some(10), "an unfinished set never advances the installed version");
                send(&backbone, &receiver, aggregate(11, 1, 2, vec![second.clone()], 120), 4).await;
                assert_eq!(installs(), 2, "the first author's set completes and installs");
                send(&backbone, &receiver, of_b(0, 2, vec![changed.clone()]), 5).await;
                assert_eq!(installs(), 3, "the second author's set, the same list at that version, completes and installs as its own publication");
                send(&backbone, &receiver, of_b(0, 2, vec![changed.clone()]), 6).await;
                assert_eq!(installs(), 3, "its exact repeat is a duplicate");
                assert_eq!(role.as_ref().unwrap().held_source("mesh2").map(|(p, v)| (p.map(|p| p.node_id), v)), Some((Some(author_b.node.clone()), 11)), "the installed set names its author");
                expected = 11;
            },
            Story::DeltaStale => send(&sender, &receiver, delta(8, 9, vec![], 200), 2).await,
        }
        assert_eq!(installed(), Some(expected));
        let held=receiver.digest_view.all().into_iter().find(|d| d.node.node_id == id).unwrap();
        assert_eq!(held.node.transport_addr, if expected>=11 { changed.node.transport_addr } else { before.node.transport_addr }, "only a newer complete publication or applicable delta replaces content");
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
    let aggregates=if matches!(story, Story::TwoForwarders) { 3 } else if matches!(story, Story::Chunks | Story::PendingConflict | Story::LowerStamp | Story::ReadRepair | Story::CrossDeltaBase | Story::CrossReadRefresh | Story::NewPublication | Story::CrossRepeat) { 2 } else { 1 };
    assert_eq!(spans.iter().filter(|s|s["name"]=="rdm.mesh.topology.update.via-full").count(),aggregates);
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
