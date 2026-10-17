//! Replacement seats are classified against installed records, atomically for a multi-seat row.
use rdm_core::{FabricId, MeshId, NodeId, IncarnationId, Seat, SeatHolder, NodeKind, LifecycleOp};
use rdm_core::seat::transfer::CommittedSeat;
use rdm_transport::membership::{learn_addresses, mesh_topic, Frame, Membership};
use std::time::Duration;

async fn endpoint() -> (iroh::Endpoint, iroh_gossip::net::Gossip, iroh::protocol::Router) {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    (ep, gossip, router)
}

async fn send(sender: &iroh_gossip::api::GossipSender, receiver: &Membership, frame: Frame, serial: u64) {
    // A following status on the same channel proves receipt has finished.
    sender.broadcast_neighbors(frame.encode().into()).await.unwrap();
    sender.broadcast_neighbors(Frame::MeshStatus { mesh: "barrier".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.2".into(), forwarded_by: None, changed_at_rafka_ms: serial }.encode().into()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while receiver.mesh_status("barrier").is_none_or(|s| s.changed_at_rafka_ms < serial) { tokio::time::sleep(Duration::from_millis(10)).await; }
    }).await.expect("the following frame was applied on this channel");
}


#[derive(Clone, Copy)] enum Kind { Seated, Updating, Updated }
impl Kind {
    fn name(self) -> &'static str { match self { Self::Seated => "seated", Self::Updating => "primary-updating", Self::Updated => "primary-updated" } }
    fn frame(self, op: &LifecycleOp, seats: Vec<CommittedSeat>) -> Frame {
        match self { Self::Seated => Frame::Seated { seat: seats[0].seat.kind(), holder: seats[0].record() }, Self::Updating => Frame::PrimaryUpdating { op: op.clone(), seats }, Self::Updated => Frame::PrimaryUpdated { op: op.clone(), seats } }
    }
    fn span(self) -> &'static str { match self { Self::Seated => "rdm.mesh.seat.update.via-announcement", Self::Updating => "rdm.mesh.seat.update.via-primary-updating", Self::Updated => "rdm.mesh.seat.update.via-primary-updated" } }
}
#[derive(Clone, Copy)] enum Story { Stale, Duplicate, Inconsistent, Newer, ReadFloor, Atomic, Stages, Scope }
fn story(kind: Kind, story: Story) {
    let suffix = match story { Story::Stale => "stale", Story::Duplicate => "duplicate", Story::Inconsistent => "inconsistent", Story::Newer => "newer", Story::ReadFloor => "read-floor", Story::Atomic => "atomic", Story::Stages => "stages", Story::Scope => "scope" };
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, &format!("rdm-{}-{suffix}", kind.name())).unwrap();
    capture.run(2, async {
        let (sender_ep, gossip, _sender_router) = endpoint().await;
        let (receiver_ep, receiver_gossip, _receiver_router) = endpoint().await;
        let socket = sender_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let address = iroh::EndpointAddr::new(sender_ep.id()).with_ip_addr(socket);
        learn_addresses(&receiver_ep, &[address.clone()]).unwrap();
        learn_addresses(&sender_ep, &[iroh::EndpointAddr::new(receiver_ep.id()).with_ip_addr(receiver_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap())]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let mut topic = gossip.subscribe(mesh_topic(&fabric, &mesh), vec![]).await.unwrap();
        let receiver = Membership::join(&receiver_gossip, &receiver_ep, &fabric, "mesh1", &mesh, "mesh1.admin.1", rdm_transport::clock::os_clock(), vec![address]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), topic.joined()).await.unwrap().unwrap();
        let (sender, _rx) = topic.split();

        let holder = SeatHolder { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 10 };
        let op = LifecycleOp { build_id: "seat-replacement".into(), attempt: 1, operation: "node.primary.update:mesh1.rpc.1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "mesh1.rpc.1".parse().unwrap(), event_at_rafka_ms: 1 };
        let seat = Seat::Cohort(NodeKind::RpcNode);
        let first = CommittedSeat::of(seat, &holder);
        if matches!(story, Story::ReadFloor) { receiver.learn_seats(&[(seat, holder.clone(), false)], "topology-read"); }
        else { send(&sender, &receiver, kind.frame(&op, vec![first.clone()]), 1).await; }
        let changes = receiver.seats().subscribe();
        let before = *changes.borrow();
        let mut offered = first.clone();
        match story {
            Story::Stale | Story::ReadFloor => offered.revision = 9,
            Story::Duplicate => {},
            Story::Inconsistent => { offered.holder.incarnation = IncarnationId::mint(); },
            Story::Newer => { offered.revision = 11; offered.holder.incarnation = IncarnationId::mint(); },
            Story::Atomic => {
                receiver.learn_seats(&[(Seat::FabricPrimary, SeatHolder { revision: 20, ..holder.clone() }, false)], "topology-read");
                let count = *changes.borrow();
                offered.revision = 11;
                send(&sender, &receiver, kind.frame(&op, vec![offered, CommittedSeat::of(Seat::FabricPrimary, &SeatHolder { revision: 19, ..holder.clone() })]), 2).await;
                assert_eq!(*changes.borrow(), count, "a refused multi-seat row installs no subset");
                assert_eq!(receiver.seats().fabric().unwrap().revision, 20);
                assert_eq!(receiver.seats().cohort("mesh1", NodeKind::RpcNode).map(|h| h.revision), if matches!(kind, Kind::Updating) { None } else { Some(10) });
                // The refused row must not advance the rpc cohort's receive floor either.
                send(&sender, &receiver, kind.frame(&op, vec![CommittedSeat { revision: 11, ..first.clone() }]), 3).await;
                return;
            },
            Story::Stages => {
                send(&sender, &receiver, Kind::Updated.frame(&op, vec![first.clone()]), 2).await;
                let count = *changes.borrow();
                send(&sender, &receiver, Kind::Updating.frame(&op, vec![first]), 3).await;
                assert_eq!(*changes.borrow(), count);
                assert_eq!(receiver.seats().cohort("mesh1", NodeKind::RpcNode), Some(holder));
                return;
            },
            Story::Scope => {
                for (i, (other, mesh)) in [(Seat::MeshPrimary, "mesh1"), (Seat::Cohort(NodeKind::Broker), "mesh1"), (seat, "mesh2"), (Seat::FabricPrimary, "mesh1")].into_iter().enumerate() {
                    send(&sender, &receiver, kind.frame(&op, vec![CommittedSeat::of(other, &SeatHolder { mesh: mesh.into(), revision: 1, ..holder.clone() })]), 2 + i as u64).await;
                }
                assert_eq!(receiver.seats().mesh("mesh1").unwrap().revision, 1);
                assert_eq!(receiver.seats().cohort("mesh1", NodeKind::Broker).unwrap().revision, 1);
                assert_eq!(receiver.seats().cohort("mesh2", NodeKind::RpcNode).unwrap().revision, 1);
                assert_eq!(receiver.seats().fabric().unwrap().revision, 1);
                return;
            }
        }
        send(&sender, &receiver, kind.frame(&op, vec![offered.clone()]), 2).await;
        let advances = matches!(story, Story::Newer) && !matches!(kind, Kind::Updating);
        assert_eq!(*changes.borrow(), before + u64::from(advances));
        let expected = if advances { Some(offered.record()) } else if matches!(kind, Kind::Updating) && !matches!(story, Story::ReadFloor) { None } else { Some(holder) };
        assert_eq!(receiver.seats().cohort("mesh1", NodeKind::RpcNode), expected, "only an admitted settled replacement moves a seat");
        sender_ep.close().await; receiver_ep.close().await;
    });
    let spans = capture.spans().unwrap();
    for s in spans.iter().filter(|s| s["attributes"]["frame"] == kind.name() || s["name"] == kind.span()) { println!("{}", serde_json::to_string(s).unwrap()); }
    let outcome = match story { Story::Stale | Story::ReadFloor | Story::Atomic | Story::Stages => "rdm.mesh.membership.reject.via-stale", Story::Duplicate => "rdm.mesh.membership.update.via-duplicate", Story::Inconsistent => "rdm.mesh.membership.reject.via-inconsistent", _ => "" };
    if !outcome.is_empty() {
        let refusal = spans.iter().find(|s| s["name"] == outcome && s["attributes"]["frame"] == kind.name()).expect("the receive framework names the outcome");
        if matches!(story, Story::Duplicate) { assert_eq!(refusal["attributes"]["outcome"], "ignored"); assert!(refusal["attributes"]["identity"].as_str().unwrap().contains("revision=10")); }
        else { assert!(refusal["attributes"]["subject"].as_str().unwrap().contains("primary")); }
    }
    let expected = match story { Story::ReadFloor => 0, Story::Newer | Story::Atomic => 2, Story::Scope => 5, _ => 1 };
    assert_eq!(spans.iter().filter(|s| s["name"] == kind.span()).count(), expected, "only applied frames emit the ordinary span");
}

/// CONTRACT: Seated stale is classified by the complete cohort and committed revision.
#[test] fn seated_stale() { story(Kind::Seated, Story::Stale); }

/// CONTRACT: Seated duplicate is classified by the complete cohort and committed revision.
#[test] fn seated_duplicate() { story(Kind::Seated, Story::Duplicate); }

/// CONTRACT: Seated inconsistent is classified by the complete cohort and committed revision.
#[test] fn seated_inconsistent() { story(Kind::Seated, Story::Inconsistent); }

/// CONTRACT: Seated newer is classified by the complete cohort and committed revision.
#[test] fn seated_newer() { story(Kind::Seated, Story::Newer); }

/// CONTRACT: Seated readfloor is classified by the complete cohort and committed revision.
#[test] fn seated_readfloor() { story(Kind::Seated, Story::ReadFloor); }

/// CONTRACT: Seated scope is classified by the complete cohort and committed revision.
#[test] fn seated_scope() { story(Kind::Seated, Story::Scope); }

/// CONTRACT: Updating stale is classified by the complete cohort and committed revision.
#[test] fn updating_stale() { story(Kind::Updating, Story::Stale); }

/// CONTRACT: Updating duplicate is classified by the complete cohort and committed revision.
#[test] fn updating_duplicate() { story(Kind::Updating, Story::Duplicate); }

/// CONTRACT: Updating inconsistent is classified by the complete cohort and committed revision.
#[test] fn updating_inconsistent() { story(Kind::Updating, Story::Inconsistent); }

/// CONTRACT: Updating newer is classified by the complete cohort and committed revision.
#[test] fn updating_newer() { story(Kind::Updating, Story::Newer); }

/// CONTRACT: Updating readfloor is classified by the complete cohort and committed revision.
#[test] fn updating_readfloor() { story(Kind::Updating, Story::ReadFloor); }

/// CONTRACT: Updating atomic is classified by the complete cohort and committed revision.
#[test] fn updating_atomic() { story(Kind::Updating, Story::Atomic); }

/// CONTRACT: Updated stale is classified by the complete cohort and committed revision.
#[test] fn updated_stale() { story(Kind::Updated, Story::Stale); }

/// CONTRACT: Updated duplicate is classified by the complete cohort and committed revision.
#[test] fn updated_duplicate() { story(Kind::Updated, Story::Duplicate); }

/// CONTRACT: Updated inconsistent is classified by the complete cohort and committed revision.
#[test] fn updated_inconsistent() { story(Kind::Updated, Story::Inconsistent); }

/// CONTRACT: Updated newer is classified by the complete cohort and committed revision.
#[test] fn updated_newer() { story(Kind::Updated, Story::Newer); }

/// CONTRACT: Updated readfloor is classified by the complete cohort and committed revision.
#[test] fn updated_readfloor() { story(Kind::Updated, Story::ReadFloor); }

/// CONTRACT: Updated atomic is classified by the complete cohort and committed revision.
#[test] fn updated_atomic() { story(Kind::Updated, Story::Atomic); }

/// CONTRACT: updated at one revision follows updating; a delayed updating never replaces it.
#[test] fn updating_after_updated_is_stale() { story(Kind::Updating, Story::Stages); }
