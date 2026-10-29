//! i143.e6.s13 acceptance (rafka-v2 #2901, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2901-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span the cell's
//! own threads emitted, captured by its `EvidenceCapture`) there.
//!
//! CONTRACT: a status change is sent at once and then once per second, five sends in all, every
//! send carrying the one instant of the change; a status that did not change sends nothing; only
//! the node holding the primary role authors a status frame; a forwarding primary keeps the
//! original publisher; and no status travels inside `MeshTopology`.

use rdm_core::{FabricId, IncarnationId, MeshId};
use rdm_transport::snapshot::PublisherId;
use rdm_transport::topology::{
    forward_of, Backbone, Frame, TopologyView, StatusView, StatusPublisher, StatusScope, STATUS_EVERY, STATUS_SENDS,
};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn ts(f: &Frame) -> u64 {
    match f.data() {
        Frame::MeshStatus { changed_at_rafka_ms, .. } | Frame::FabricStatus { changed_at_rafka_ms, .. } => *changed_at_rafka_ms,
        other => panic!("not a status frame: {other:?}"),
    }
}

fn publisher_of(f: &Frame) -> &str {
    match f.data() {
        Frame::MeshStatus { publisher, .. } | Frame::FabricStatus { publisher, .. } => publisher,
        other => panic!("not a status frame: {other:?}"),
    }
}

fn status_of(f: &Frame) -> &str {
    match f.data() {
        Frame::MeshStatus { status, .. } | Frame::FabricStatus { status, .. } => status,
        other => panic!("not a status frame: {other:?}"),
    }
}

/// Every send a publisher makes over `span`, stepping a fake clock in `step` increments from
/// `from`: (offset in ms from `base`, frame).
fn sweep(p: &mut StatusPublisher, base: Instant, from: Duration, span: Duration, step: Duration) -> Vec<(u64, Frame)> {
    let mut out = Vec::new();
    let mut t = from;
    while t <= from + span {
        if let Some(f) = p.due(base + t) {
            out.push((t.as_millis() as u64, f));
        }
        t += step;
    }
    out
}

#[test]
fn status_publisher_reinforces_changed_state_five_times_then_stops() {
    let cell = "status_publisher_reinforces_changed_state_five_times_then_stops";
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-acceptance").expect("the cell's evidence file can be created");
    capture.run(2, async {
        let fabric = FabricId::mint();
        let (me, peer) = ("mesh1.admin.1", "mesh2.admin.1");
        let base = Instant::now();
        let ms = Duration::from_millis;

        // A changed mesh status: the frame at once, then exactly four repeats one second apart, then
        // nothing however long the clock runs; every send is the same fact at the same instant.
        // The mesh's first state is a heard Pending with no instant (Q272): stamped here, once.
        let first_state = rdm_transport::topology::StatusFact { status: "pending".into(), changed_at_rafka_ms: 0 };
        let mut mesh = StatusPublisher::new(me, StatusScope::Mesh("mesh1".into()));
        mesh.set_role(true);
        let first = mesh.observe("pending", Some(first_state.clone()), true, base, 1_000).expect("a change is sent at once");
        let repeats = sweep(&mut mesh, base, ms(0), Duration::from_secs(120), ms(100));
        let offsets: Vec<u64> = repeats.iter().map(|(o, _)| *o).collect();
        assert_eq!(STATUS_SENDS, 5);
        assert_eq!(STATUS_EVERY, Duration::from_secs(1));
        assert_eq!(offsets, vec![1000, 2000, 3000, 4000], "four repeats, one per second after the first send, then nothing for two minutes");
        let sends: Vec<&Frame> = std::iter::once(&first).chain(repeats.iter().map(|(_, f)| f)).collect();
        assert_eq!(sends.len(), 5, "five sends in all");
        for f in &sends {
            assert_eq!(ts(f), 1_000, "all five sends carry the instant of the change");
            assert_eq!(publisher_of(f), me, "authored by the publisher itself");
            assert_eq!(status_of(f), "pending");
            assert!(matches!(f, Frame::MeshStatus { mesh, forwarded_by: None, .. } if mesh == "mesh1"));
        }
        assert_eq!(crate::gossip_wire::authored(&sends[0]).encode(), crate::gossip_wire::authored(&sends[4]).encode(), "the fifth send is the first, byte for byte");

        // The same status observed again, however often, sends nothing.
        let mut unchanged_sends = 0;
        for k in 0..600u64 {
            if mesh.observe("pending", None, true, base + ms(130_000 + k * 100), 99_999).is_some() {
                unchanged_sends += 1;
            }
            if mesh.due(base + ms(130_000 + k * 100)).is_some() {
                unchanged_sends += 1;
            }
        }
        assert_eq!(unchanged_sends, 0, "no change means no status traffic");

        // A new change mid-reinforcement replaces the old one: its own five sends, the old one's rest dropped.
        let mut fabric_pub = StatusPublisher::new(me, StatusScope::Fabric(fabric.clone()));
        fabric_pub.set_role(true);
        let a = fabric_pub.observe("pending", None, true, base, 10).unwrap();
        let a2 = fabric_pub.due(base + ms(1000)).unwrap();
        let b = fabric_pub.observe("ready", None, true, base + ms(1500), 20).unwrap();
        let rest = sweep(&mut fabric_pub, base, ms(1500), Duration::from_secs(30), ms(100));
        assert_eq!((ts(&a), ts(&a2), ts(&b)), (10, 10, 20));
        assert_eq!(rest.len(), 4, "the new change is repeated four times after its first send");
        assert!(rest.iter().all(|(_, f)| ts(f) == 20 && status_of(f) == "ready"), "no repeat of the replaced change follows");

        // Only the role holder authors: without the role nothing is sent; losing the role ends the sends still to come.
        let mut not_primary = StatusPublisher::new(peer, StatusScope::Mesh("mesh2".into()));
        assert!(not_primary.observe("pending", Some(first_state.clone()), true, base, 1).is_none(), "a node that is not the primary authors nothing");
        not_primary.set_role(true);
        assert!(not_primary.observe("pending", Some(first_state.clone()), true, base, 2).is_some());
        not_primary.set_role(false);
        assert!(sweep(&mut not_primary, base, ms(0), Duration::from_secs(10), ms(100)).is_empty(), "a primary that lost the role sends no more");
        // A successor that hears the status it observes adopts it: no transition is invented, so nothing
        // is sent until its checklist completes; then the adopted fact is sent, the old instant, five sends.
        let mut successor = StatusPublisher::new(me, StatusScope::Mesh("mesh2".into()));
        successor.set_role(true);
        let heard = rdm_transport::topology::StatusFact { status: "ready".into(), changed_at_rafka_ms: 2 };
        assert!(successor.observe("ready", Some(heard.clone()), false, base, 77).is_none(), "an adopted status is not sent before the checklist completes");
        assert!(successor.awaiting_checklist(), "the adopted status waits for its checklist");
        assert!(sweep(&mut successor, base, ms(0), Duration::from_secs(10), ms(100)).is_empty(), "no timer republishes an adopted status");
        assert!(successor.observe("ready", Some(heard.clone()), false, base + ms(11_000), 78).is_none(), "an incomplete checklist still sends nothing");
        let again = successor.observe("ready", Some(heard.clone()), true, base + ms(12_000), 79).expect("the completed checklist republishes the adopted status");
        assert_eq!((ts(&again), status_of(&again), publisher_of(&again)), (2, "ready", me), "the old instant, the same status, under the new publisher");
        assert!(!successor.awaiting_checklist());
        let rest = sweep(&mut successor, base, ms(12_000), Duration::from_secs(10), ms(100));
        assert_eq!(rest.len(), 4, "the republished fact is five identical sends: {rest:?}");
        assert!(rest.iter().all(|(_, f)| ts(f) == 2));
        // A forward change while the checklist is open sends nothing; it goes out, stamped then, when
        // the checklist completes (Q272).
        let mut moved_on = StatusPublisher::new(me, StatusScope::Mesh("mesh2".into()));
        moved_on.set_role(true);
        assert!(moved_on.observe("ready", Some(heard.clone()), false, base, 80).is_none());
        assert!(moved_on.observe("leaving", Some(heard.clone()), false, base, 81).is_none(), "nothing is sent while the checklist is open");
        let changed = moved_on.observe("leaving", Some(heard), true, base, 82).expect("the forward change goes out when the checklist completes");
        assert_eq!((ts(&changed), status_of(&changed)), (82, "leaving"));
        assert!(!moved_on.awaiting_checklist());

        // Forwarding: a peer mesh's primary keeps the original publisher and instant, names itself
        // only in forwarded_by, never forwards its own mesh's status, and forwards only what an author sent.
        let fwd = forward_of(first.clone(), peer, "mesh2").expect("a peer mesh's status is forwarded");
        assert!(matches!(&fwd, Frame::MeshStatus { mesh, publisher, forwarded_by: Some(by), changed_at_rafka_ms: 1_000, status } if mesh == "mesh1" && publisher == me && by == peer && status == "pending"));
        assert!(forward_of(first.clone(), me, "mesh1").is_none(), "a primary does not forward its own mesh's status");
        assert!(forward_of(fwd.clone(), "mesh3.admin.1", "mesh3").is_none(), "a forwarded copy is not forwarded again");
        let fab = fabric_pub.observe("connections-deleting", None, true, base + ms(200_000), 30).unwrap();
        let fab_fwd = forward_of(fab.clone(), peer, "mesh2").unwrap();
        assert!(matches!(&fab_fwd, Frame::FabricStatus { publisher, forwarded_by: Some(by), changed_at_rafka_ms: 30, .. } if publisher == me && by == peer));
        assert!(forward_of(fab, me, "mesh1").is_none(), "the fabric primary does not forward its own status");

        // A holder keeps a status until the next change, refuses an older one, and replays the
        // original authorship: a replayed status is never restated as the replayer's own.
        // Held as gossip delivers it: under its author's context (a status held without one is not
        // replayed, Q272).
        let status_view = StatusView::default();
        let fwd = crate::gossip_wire::authored(&fwd);
        assert!(status_view.take(&fwd, &fabric));
        assert!(!status_view.take(&fwd, &fabric), "the same frame again is not taken twice");
        let older = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready".into(), publisher: me.into(), forwarded_by: None, changed_at_rafka_ms: 500 };
        assert!(!status_view.take(&older, &fabric), "an older status is refused");
        assert_eq!(status_view.mesh("mesh1").unwrap().status, "pending");
        let replay = status_view.held_frames(&fabric, peer);
        assert_eq!(replay.len(), 1);
        assert!(matches!(replay[0].data(), Frame::MeshStatus { publisher, forwarded_by: Some(by), changed_at_rafka_ms: 1_000, .. } if publisher == me && by == peer));
        let own = status_view.held_frames(&fabric, me);
        assert!(matches!(own[0].data(), Frame::MeshStatus { forwarded_by: None, .. }), "the author's own replay names no forwarder");
        // A republish keeps the old instant: under a NEW publisher it is a different frame from the one
        // held and is taken (the view holds the later publisher's authorship); the very same publisher
        // with the same instant is a repeat and is not.
        let republish = |publisher: &str| Frame::MeshStatus { mesh: "mesh1".into(), status: "pending".into(), publisher: publisher.into(), forwarded_by: None, changed_at_rafka_ms: 1_000 };
        assert!(status_view.take(&republish(peer), &fabric), "a new publisher with the old changed_at is taken");
        assert_eq!(status_view.mesh("mesh1").unwrap().publisher, peer);
        assert!(!status_view.take(&republish(peer), &fabric), "the same publisher with the same changed_at is a repeat");

        // Nothing rides inside MeshTopology: its fields are digests and overlays only.
        let members = Frame::MeshTopology {
            mesh: "mesh1".into(),
            publisher: PublisherId { node: me.into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() },
            forwarded_by: None,
            topology_version: 1,
            published_at_rafka_ms: 1,
            snapshot_id: 1,
            chunk_index: 0,
            chunk_count: 1,
            digests: vec![],
            in_flight: vec![],
            deleted: vec![],
        };
        // The frame is positional postcard: this pattern names every field the frame carries, and
        // none of them is a status.
        let Ok(Frame::MeshTopology { mesh: _, publisher: _, forwarded_by: _, topology_version: _, published_at_rafka_ms: _, snapshot_id: _, chunk_index: _, chunk_count: _, digests: _, in_flight: _, deleted: _ }) = Frame::decode(&crate::gossip_wire::authored(&members).encode()).map(Frame::into_data) else {
            panic!("MeshTopology round-trips as MeshTopology, carrying no status field");
        };

        // The real Backbone: the publisher role is the span `via-status-publisher`, and a status the
        // primary announces is held under its own authorship at one instant.
        let (endpoint, gossip) = {
            let transport = iroh::endpoint::QuicTransportConfig::builder().build();
            let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
            let g = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
            (ep, g)
        };
        let topology_view = TopologyView::join(&gossip, &endpoint, &fabric, "mesh1", &MeshId::mint(), me, rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        let author = rdm_core::PublisherId { node: me.to_string(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        let backbone = Backbone::join(&gossip, &endpoint, &topology_view, "mesh1", author.clone(), vec![]).await.unwrap();
        backbone.announce_statuses("pending", "pending", true, true).await;
        assert!(topology_view.mesh_status("mesh1").is_none() && topology_view.fabric_status().is_none(), "a node holding no role publishes no status");
        let holder = rdm_core::Primary { mesh: "mesh1".into(), node_id: author.node_id.clone(), incarnation: author.incarnation.clone(), revision: 2 };
        topology_view.primaries().take(rdm_core::Seat::MeshPrimary, &holder);
        topology_view.primaries().take(rdm_core::Seat::FabricPrimary, &holder);
        backbone.set_mesh_primary(true);
        backbone.set_fabric_primary(true);
        // The mesh's first state, as a hand-off or the Day-0 self-apply holds it (Q272): Pending with no
        // instant, stamped by the publisher; ready is then the projection's forward change.
        let pending = Frame::MeshStatus { mesh: "mesh1".into(), status: "pending".into(), publisher: me.into(), forwarded_by: None, changed_at_rafka_ms: 0 };
        assert!(topology_view.digest_view.statuses.take(&pending, &fabric));
        backbone.announce_statuses("ready", "ready", true, true).await;
        assert_eq!(topology_view.mesh_status("mesh1").map(|s| s.status), Some("pending".to_string()), "the first state is sent first");
        backbone.announce_statuses("ready", "ready", true, true).await;
        let (m, f) = (topology_view.mesh_status("mesh1").expect("the mesh status is held"), topology_view.fabric_status().expect("the fabric status is held"));
        assert_eq!((m.publisher.as_str(), m.status.as_str()), (me, "ready"));
        assert_eq!((f.publisher.as_str(), f.status.as_str()), (me, "ready"));
        // The real status sender runs on the real clock: its five sends are the spans below.
        tokio::time::sleep(Duration::from_millis(5_500)).await;
        backbone.announce_statuses("ready", "ready", true, true).await;
        assert_eq!(topology_view.mesh_status("mesh1").unwrap().changed_at_rafka_ms, m.changed_at_rafka_ms, "an unchanged status is not published again");
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        backbone.set_mesh_primary(false);
        backbone.set_fabric_primary(false);
        endpoint.close().await;
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
        for span in spans.iter().filter(|s| s["name"] == "rdm.mesh.fabric.update.via-status-send" && s["attributes"]["node"] == me) {
            assert_eq!(span["attributes"]["committed_revision"], "2", "every reinforcement retains the publisher's held committed row revision");
        }
        let roles: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.fabric.update.via-status-publisher" && s["attributes"]["node"] == me).collect();
        let role_of = |r: &str| roles.iter().filter(|s| s["attributes"]["role"] == r).count();
        assert_eq!((role_of("start"), role_of("stop")), (1, 1), "the fabric publisher role started and stopped once: {roles:?}");

        // The five sends, proven at the SENDER by its own span for each send: one `via-status-send`
        // per send, the first at once and the rest one second apart, every one carrying the SAME
        // changed_at_rafka_ms, and none after the fifth nor for the unchanged announce nor before the role.
        let sends_of = |scope: String| -> Vec<(u64, u64)> {
            let mut v: Vec<(u64, u64)> = spans
                .iter()
                .filter(|s| s["name"] == "rdm.mesh.fabric.update.via-status-send" && s["attributes"]["node"] == me && s["attributes"]["scope"] == scope.as_str())
                .map(|s| (s["start_unix_nano"].as_u64().unwrap(), s["attributes"]["changed_at_rafka_ms"].as_str().unwrap().parse::<u64>().unwrap()))
                .collect();
            v.sort();
            v
        };
        let mut sender_proof = serde_json::Map::new();
        for (label, scope, held_at) in [("mesh", "mesh:mesh1".to_string(), m.changed_at_rafka_ms), ("fabric", format!("fabric:{fabric}"), f.changed_at_rafka_ms)] {
            // The mesh's first state, Pending, went out once before ready replaced it (Q272).
            let all = sends_of(scope.clone());
            let first_state = usize::from(label == "mesh");
            assert_eq!(all.len(), 5 + first_state, "{label}: five send spans for one change (and the mesh's first state once), none for the unchanged announce, none before the role: {all:?}");
            let sends: Vec<_> = all.into_iter().filter(|(_, at)| *at == held_at).collect();
            assert_eq!(sends.len(), 5, "{label}: five sends of the held change: {sends:?}");
            assert!(sends.iter().all(|(_, at)| *at == held_at), "{label}: all five sends carry the one changed_at_rafka_ms {held_at}: {sends:?}");
            let offsets_ms: Vec<u64> = sends.iter().map(|(t, _)| (t - sends[0].0) / 1_000_000).collect();
            for (k, o) in offsets_ms.iter().enumerate() {
                assert!((k as u64 * 1_000 - (k as u64).min(1) * 100..k as u64 * 1_000 + 500).contains(o), "{label}: send {k} is due {k} s after the first: {offsets_ms:?}");
            }
            sender_proof.insert(label.into(), json!({ "scope": scope, "send_offsets_ms": offsets_ms, "changed_at_rafka_ms": held_at }));
        }

        let result = json!({
            "cell": cell,
            "sender_send_spans": sender_proof,
            "clock": "fake: Instant offsets from one base, stepped by the cell; no sleeping",
            "sends_per_change": STATUS_SENDS,
            "send_offsets_ms": std::iter::once(0u64).chain(offsets.iter().copied()).collect::<Vec<_>>(),
            "send_changed_at_rafka_ms": sends.iter().map(|f| ts(f)).collect::<Vec<_>>(),
            "send_publishers": sends.iter().map(|f| publisher_of(f)).collect::<Vec<_>>(),
            "sends_after_fifth_over_two_minutes": 0,
            "unchanged_observations": 600,
            "unchanged_sends": unchanged_sends,
            "replaced_change_repeats_after_replacement": rest.len(),
            "forwarded": { "publisher": publisher_of(&fwd), "forwarded_by": peer, "changed_at_rafka_ms": ts(&fwd) },
            "members_frame_fields": ["mesh", "publisher", "forwarded_by", "topology_version", "published_at_rafka_ms", "snapshot_id", "chunk_index", "chunk_count", "digests", "in_flight", "deleted"],
            "held_mesh_status": { "publisher": m.publisher, "status": m.status, "changed_at_rafka_ms": m.changed_at_rafka_ms },
            "held_fabric_status": { "publisher": f.publisher, "status": f.status, "changed_at_rafka_ms": f.changed_at_rafka_ms },
            "publisher_role_spans": roles.iter().map(|s| json!({ "role": s["attributes"]["role"], "trace_id": s["trace_id"], "span_id": s["span_id"] })).collect::<Vec<_>>(),
            "spans_read": spans.len(),
        });
        std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    });
}

/// CONTRACT (Q272; fabric-primary.md §7: a takeover "adopts that exact state" and invents no
/// transition; mesh-investigation.md step 7): a mesh's publisher sends only a state it holds. Holding
/// nothing and hearing nothing it is silent, whatever its projection says; a heard state with an
/// instant is adopted exactly and sent, that instant, when its checklist completes, whatever the
/// projection says; a heard Pending with no instant is the mesh's first state, stamped once and sent
/// at once; a projection behind the held state never replaces it. Must NOT happen: a successor
/// authoring ready at its own instant before it is handed the mesh's state (the 2805 cell's red at
/// 4d6bb4e95: ready sent 150 ms before the hand-off of ready at the older instant).
#[test]
fn a_mesh_status_publisher_sends_only_a_state_it_holds() {
    let base = Instant::now();
    let fact = |status: &str, at: u64| rdm_transport::topology::StatusFact { status: status.into(), changed_at_rafka_ms: at };
    let publisher = || {
        let mut p = StatusPublisher::new("mesh2.admin.1", StatusScope::Mesh("mesh2".into()));
        p.set_role(true);
        p
    };

    let mut silent = publisher();
    assert!(silent.observe("ready", None, true, base, 100).is_none(), "nothing held, nothing heard: nothing sent, the projection notwithstanding");
    assert!(silent.due(base + Duration::from_secs(5)).is_none());

    let mut successor = publisher();
    assert!(successor.observe("pending", Some(fact("ready", 40)), false, base, 100).is_none(), "ready at 40 heard, pending projected, the checklist open: nothing sent");
    let sent = successor.observe("pending", Some(fact("ready", 40)), true, base, 101).expect("the checklist complete sends the state held");
    assert_eq!((status_of(&sent), ts(&sent)), ("ready", 40), "the adopted state at its own instant, not the projection's");
    assert!(successor.observe("pending", None, true, base, 102).is_none(), "a projection behind the held state never replaces it");

    let mut first = publisher();
    let stamped = first.observe("ready", Some(fact("pending", 0)), false, base, 200).expect("a mesh's first state is sent at once");
    assert_eq!((status_of(&stamped), ts(&stamped)), ("pending", 200), "Pending with no instant is stamped here");
    let forward = first.observe("ready", Some(fact("pending", 0)), false, base, 201).expect("the projection's forward change follows");
    assert_eq!((status_of(&forward), ts(&forward)), ("ready", 201));
    assert!(first.observe("ready", Some(fact("pending", 0)), false, base, 202).is_none(), "the first state is stamped once");

    // Q144: Pending at an instant heard, ready projected: nothing while the checklist is open, then
    // ready stamped at the checklist's completion.
    let mut guard = publisher();
    assert!(guard.observe("ready", Some(fact("pending", 30)), false, base, 300).is_none());
    let ready = guard.observe("ready", Some(fact("pending", 30)), true, base, 301).expect("ready goes out once the checklist completes");
    assert_eq!((status_of(&ready), ts(&ready)), ("ready", 301));

    let mut wordless = publisher();
    assert!(wordless.observe("ready", Some(fact("degraded", 50)), true, base, 400).is_none(), "only the four lifecycle words are adopted");
}
