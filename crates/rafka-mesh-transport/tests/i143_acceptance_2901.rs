//! i143.e6.s13 acceptance (rafka-v2 #2901, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2901-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span this process
//! emitted, captured in-process by the evidence exporter) there.
//!
//! CONTRACT: a status change is sent at once and then once per second, five sends in all, every
//! send carrying the one instant of the change; a status that did not change sends nothing; only
//! the node holding the primary role authors a status frame; a forwarding primary keeps the
//! original publisher; and no status travels inside `Members`.

use rafka_mesh_entity::{FabricId, MeshId};
use rafka_mesh_transport::membership::{
    forward_of, Backbone, Frame, Membership, StatusBook, StatusPublisher, StatusScope, STATUS_EVERY, STATUS_SENDS,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2901/unit").join(cell),
    }
}

fn collect_spans(dir: &std::path::Path) -> Vec<Value> {
    let mut spans = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        let mine = format!(".{}-", std::process::id());
        if p.file_name().is_some_and(|n| n.to_string_lossy().ends_with(".spans.jsonl") && n.to_string_lossy().contains(&mine)) {
            for line in std::fs::read_to_string(&p).unwrap().lines() {
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    spans.push(v);
                }
            }
        }
    }
    spans
}

fn ts(f: &Frame) -> u64 {
    match f {
        Frame::MeshStatus { published_at_rafka_ms, .. } | Frame::FabricStatus { published_at_rafka_ms, .. } => *published_at_rafka_ms,
        other => panic!("not a status frame: {other:?}"),
    }
}

fn publisher_of(f: &Frame) -> &str {
    match f {
        Frame::MeshStatus { publisher, .. } | Frame::FabricStatus { publisher, .. } => publisher,
        other => panic!("not a status frame: {other:?}"),
    }
}

fn status_of(f: &Frame) -> &str {
    match f {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_publisher_reinforces_changed_state_five_times_then_stops() {
    let cell = "status_publisher_reinforces_changed_state_five_times_then_stops";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RAFKA_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-transport-acceptance");

    let fabric = FabricId::mint();
    let (me, peer) = ("mesh1.admin.1", "mesh2.admin.1");
    let base = Instant::now();
    let ms = Duration::from_millis;

    // A changed mesh status: the frame at once, then exactly four repeats one second apart, then
    // nothing however long the clock runs; every send is the same fact at the same instant.
    let mut mesh = StatusPublisher::new(me, StatusScope::Mesh("mesh1".into()));
    mesh.set_role(true);
    let first = mesh.observe("pending", None, base, 1_000).expect("a change is sent at once");
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
    assert_eq!(sends[0].encode(), sends[4].encode(), "the fifth send is the first, byte for byte");

    // The same status observed again, however often, sends nothing.
    let mut unchanged_sends = 0;
    for k in 0..600u64 {
        if mesh.observe("pending", None, base + ms(130_000 + k * 100), 99_999).is_some() {
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
    let a = fabric_pub.observe("pending", None, base, 10).unwrap();
    let a2 = fabric_pub.due(base + ms(1000)).unwrap();
    let b = fabric_pub.observe("ready-for-traffic", None, base + ms(1500), 20).unwrap();
    let rest = sweep(&mut fabric_pub, base, ms(1500), Duration::from_secs(30), ms(100));
    assert_eq!((ts(&a), ts(&a2), ts(&b)), (10, 10, 20));
    assert_eq!(rest.len(), 4, "the new change is repeated four times after its first send");
    assert!(rest.iter().all(|(_, f)| ts(f) == 20 && status_of(f) == "ready-for-traffic"), "no repeat of the replaced change follows");

    // Only the role holder authors: without the role nothing is sent; losing the role ends the sends still to come.
    let mut not_primary = StatusPublisher::new(peer, StatusScope::Mesh("mesh2".into()));
    assert!(not_primary.observe("ready-for-traffic", None, base, 1).is_none(), "a node that is not the primary authors nothing");
    not_primary.set_role(true);
    assert!(not_primary.observe("ready-for-traffic", None, base, 2).is_some());
    not_primary.set_role(false);
    assert!(sweep(&mut not_primary, base, ms(0), Duration::from_secs(10), ms(100)).is_empty(), "a primary that lost the role sends no more");
    // A successor that hears the status it observes adopts it and authors nothing.
    let mut successor = StatusPublisher::new(me, StatusScope::Mesh("mesh2".into()));
    successor.set_role(true);
    let heard = rafka_mesh_transport::membership::StatusFact { status: "ready-for-traffic".into(), published_at_rafka_ms: 2 };
    assert!(successor.observe("ready-for-traffic", Some(heard), base, 77).is_none(), "the status the previous publisher published is not published again");
    assert!(sweep(&mut successor, base, ms(0), Duration::from_secs(10), ms(100)).is_empty());

    // Forwarding: a peer mesh's primary keeps the original publisher and instant, names itself
    // only in forwarded_by, never forwards its own mesh's status, and forwards only what an author sent.
    let fwd = forward_of(first.clone(), peer, "mesh2").expect("a peer mesh's status is forwarded");
    assert!(matches!(&fwd, Frame::MeshStatus { mesh, publisher, forwarded_by: Some(by), published_at_rafka_ms: 1_000, status } if mesh == "mesh1" && publisher == me && by == peer && status == "pending"));
    assert!(forward_of(first.clone(), me, "mesh1").is_none(), "a primary does not forward its own mesh's status");
    assert!(forward_of(fwd.clone(), "mesh3.admin.1", "mesh3").is_none(), "a forwarded copy is not forwarded again");
    let fab = fabric_pub.observe("draining", None, base + ms(200_000), 30).unwrap();
    let fab_fwd = forward_of(fab.clone(), peer, "mesh2").unwrap();
    assert!(matches!(&fab_fwd, Frame::FabricStatus { publisher, forwarded_by: Some(by), published_at_rafka_ms: 30, .. } if publisher == me && by == peer));
    assert!(forward_of(fab, me, "mesh1").is_none(), "the fabric primary does not forward its own status");

    // A holder keeps a status until the next change, refuses an older one, and replays the
    // original authorship: a replayed status is never restated as the replayer's own.
    let book = StatusBook::default();
    assert!(book.take(&fwd, &fabric));
    assert!(!book.take(&fwd, &fabric), "the same frame again is not taken twice");
    let older = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: me.into(), forwarded_by: None, published_at_rafka_ms: 500 };
    assert!(!book.take(&older, &fabric), "an older status is refused");
    assert_eq!(book.mesh("mesh1").unwrap().status, "pending");
    let replay = book.held_frames(&fabric, peer);
    assert_eq!(replay.len(), 1);
    assert!(matches!(&replay[0], Frame::MeshStatus { publisher, forwarded_by: Some(by), published_at_rafka_ms: 1_000, .. } if publisher == me && by == peer));
    let own = book.held_frames(&fabric, me);
    assert!(matches!(&own[0], Frame::MeshStatus { forwarded_by: None, .. }), "the author's own replay names no forwarder");

    // Nothing rides inside Members: its fields are digests and overlays only.
    let members = Frame::Members { mesh: "mesh1".into(), publisher: me.into(), forwarded_by: None, sent_unix_ms: 1, digests: vec![], in_flight: vec![], departed: vec![] };
    let encoded: Value = serde_json::from_slice(&members.encode()).unwrap();
    let mut keys: Vec<&str> = encoded.as_object().unwrap().keys().map(|k| k.as_str()).collect();
    keys.sort();
    assert_eq!(keys, vec!["departed", "digests", "forwarded_by", "frame", "in_flight", "mesh", "publisher", "sent_unix_ms"], "Members carries no status field");

    // The real Backbone: the publisher role is the span `via-status-publisher`, and a status the
    // primary announces is held under its own authorship at one instant.
    let (endpoint, gossip) = {
        let transport = iroh::endpoint::QuicTransportConfig::builder().build();
        let ep = rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
        let g = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        (ep, g)
    };
    let membership = Membership::join(&gossip, &endpoint, &fabric, "mesh1", &MeshId::mint(), me, vec![]).await.unwrap();
    let backbone = Backbone::join(&gossip, &endpoint, &membership, "mesh1", me, vec![]).await.unwrap();
    backbone.announce_statuses("pending", "pending").await;
    assert!(membership.mesh_status("mesh1").is_none() && membership.fabric_status().is_none(), "a node holding no role publishes no status");
    backbone.set_mesh_primary(true);
    backbone.set_fabric_primary(true);
    backbone.announce_statuses("ready-for-traffic", "ready-for-traffic").await;
    let (m, f) = (membership.mesh_status("mesh1").expect("the mesh status is held"), membership.fabric_status().expect("the fabric status is held"));
    assert_eq!((m.publisher.as_str(), m.status.as_str()), (me, "ready-for-traffic"));
    assert_eq!((f.publisher.as_str(), f.status.as_str()), (me, "ready-for-traffic"));
    backbone.announce_statuses("ready-for-traffic", "ready-for-traffic").await;
    assert_eq!(membership.mesh_status("mesh1").unwrap().published_at_rafka_ms, m.published_at_rafka_ms, "an unchanged status is not published again");
    backbone.set_mesh_primary(false);
    backbone.set_fabric_primary(false);
    endpoint.close().await;
    drop(telemetry);
    let spans = collect_spans(&dir);
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let roles: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.fabric.update.via-status-publisher" && s["attributes"]["node"] == me).collect();
    let role_of = |r: &str| roles.iter().filter(|s| s["attributes"]["role"] == r).count();
    assert_eq!((role_of("start"), role_of("stop")), (1, 1), "the fabric publisher role started and stopped once: {roles:?}");

    let result = json!({
        "cell": cell,
        "clock": "fake: Instant offsets from one base, stepped by the cell; no sleeping",
        "sends_per_change": STATUS_SENDS,
        "send_offsets_ms": std::iter::once(0u64).chain(offsets.iter().copied()).collect::<Vec<_>>(),
        "send_published_at_rafka_ms": sends.iter().map(|f| ts(f)).collect::<Vec<_>>(),
        "send_publishers": sends.iter().map(|f| publisher_of(f)).collect::<Vec<_>>(),
        "sends_after_fifth_over_two_minutes": 0,
        "unchanged_observations": 600,
        "unchanged_sends": unchanged_sends,
        "replaced_change_repeats_after_replacement": rest.len(),
        "forwarded": { "publisher": publisher_of(&fwd), "forwarded_by": peer, "published_at_rafka_ms": ts(&fwd) },
        "members_frame_keys": keys,
        "held_mesh_status": { "publisher": m.publisher, "status": m.status, "published_at_rafka_ms": m.published_at_rafka_ms },
        "held_fabric_status": { "publisher": f.publisher, "status": f.status, "published_at_rafka_ms": f.published_at_rafka_ms },
        "publisher_role_spans": roles.iter().map(|s| json!({ "role": s["attributes"]["role"], "trace_id": s["trace_id"], "span_id": s["span_id"] })).collect::<Vec<_>>(),
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
