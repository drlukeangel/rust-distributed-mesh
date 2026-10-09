//! i143.e6.s11 acceptance (rafka-v2 #2899, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2899-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span this process
//! emitted, captured in-process by the evidence exporter) there.
//!
//! The receiver under proof, `DigestBook`, lives in `rafka-mesh-transport`; the digest frame it
//! orders lives here, so this crate dev-depends on the transport.
//!
//! CONTRACT: two heartbeats of one birth are ordered by `digest_seq` alone. However the supplied
//! clock behaves (frozen, repeating, stepping back, or jumping far ahead), a higher `digest_seq`
//! is taken and an equal or lower one is not, so an older heartbeat never overrides a newer one
//! and a newer one is never mistaken for an older one. A new incarnation along the lineage is
//! taken though its `digest_seq` starts again at 1, and a late digest of the incarnation it
//! replaced is not. A real `Membership` given a frozen clock stamps one frozen `emitted_at_rafka_ms`
//! and a strictly increasing `digest_seq` from 1 on every heartbeat it publishes.

use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::clock::Clock;
use rafka_mesh_transport::membership::{DigestBook, Membership};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// A Rafka-time source the cell drives by hand: it reads whatever the cell last set.
#[derive(Debug, Default)]
struct HandClock(AtomicU64);

impl Clock for HandClock {
    fn now_rafka_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

const CELL: &str = "digest_receiver_orders_same_birth_by_sequence_with_stalled_clock";

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2899/unit").join(CELL),
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

struct Birth {
    fabric: FabricId,
    node_id: NodeId,
    incarnation: IncarnationId,
    supersedes: Option<IncarnationId>,
}

impl Birth {
    fn first(fabric: &FabricId, node_id: &NodeId) -> Self {
        Self { fabric: fabric.clone(), node_id: node_id.clone(), incarnation: IncarnationId::mint(), supersedes: None }
    }

    fn successor(&self) -> Self {
        Self { fabric: self.fabric.clone(), node_id: self.node_id.clone(), incarnation: IncarnationId::mint(), supersedes: Some(self.incarnation.clone()) }
    }

    fn digest(&self, seq: u64, at_rafka_ms: u64, status: MemberStatus) -> MeshDigest {
        MeshDigest {
            fabric_id: self.fabric.clone(),
            node: MeshNode {
                node_id: self.node_id.clone(),
                name: "mesh1.rpc.1".parse().unwrap(),
                endpoint_id: EndpointId("k".into()),
                transport_addr: "127.0.0.1:7000".parse().unwrap(),
                incarnation: self.incarnation.clone(),
                supersedes: self.supersedes.clone(),
                runtime: None,
            },
            status,
            admin_api_base: None,
            digest_seq: seq,
            emitted_at_rafka_ms: at_rafka_ms,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            extra: Default::default(),
        }
    }
}

fn held(book: &DigestBook, node_id: &NodeId) -> (u64, MemberStatus, u64) {
    let (d, _) = book.get(node_id.as_str()).expect("the birth is held");
    (d.digest_seq, d.status, d.emitted_at_rafka_ms)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn digest_receiver_orders_same_birth_by_sequence_with_stalled_clock() {
    if crate::own_process::delegated(module_path!(), "digest_receiver_orders_same_birth_by_sequence_with_stalled_clock") {
        return;
    }
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RDM_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-mesh-entity-acceptance");

    let fabric = FabricId::mint();
    let now = Instant::now();
    let mut observations: Vec<Value> = Vec::new();
    let mut note = |label: &str, seq: u64, at: u64, status: MemberStatus, taken: bool, book: &DigestBook, node: &NodeId| {
        let (hs, hst, hat) = held(book, node);
        observations.push(json!({ "offered": label, "digest_seq": seq, "emitted_at_rafka_ms": at, "status": format!("{status:?}"), "taken": taken, "held": { "digest_seq": hs, "status": format!("{hst:?}"), "emitted_at_rafka_ms": hat } }));
    };

    // The mesh channel: a frozen clock, a repeated stamp, a step backwards and a jump far ahead
    // all leave the order to `digest_seq`.
    let book = DigestBook::default();
    let node_id = NodeId::mint();
    let b1 = Birth::first(&fabric, &node_id);
    for (label, seq, at, status, want) in [
        ("first heartbeat", 1u64, 1_000u64, MemberStatus::Pending, true),
        ("clock frozen: same stamp, next seq", 2, 1_000, MemberStatus::ReadyForTraffic, true),
        ("clock stepped back: lower stamp, next seq", 3, 900, MemberStatus::Draining, true),
        ("a repeat of the held heartbeat", 3, 900, MemberStatus::Draining, false),
        ("an older heartbeat stamped far ahead", 2, 99_999_999, MemberStatus::ReadyForTraffic, false),
        ("the oldest heartbeat stamped far ahead", 1, 99_999_999, MemberStatus::Pending, false),
        ("a newer heartbeat stamped at zero", 4, 0, MemberStatus::Leaving, true),
    ] {
        let d = b1.digest(seq, at, status);
        let taken = book.record_at(d, now);
        assert_eq!(taken, want, "{label}: seq {seq} at {at}");
        note(label, seq, at, status, taken, &book, &node_id);
    }
    assert_eq!(held(&book, &node_id), (4, MemberStatus::Leaving, 0), "the newest heartbeat by sequence is held whatever its stamp");

    // The forwarded path: a copy of the held heartbeat keeps the member heard (equal sequence is
    // taken), a lower sequence is refused whatever its stamp, a higher one is taken.
    let fbook = DigestBook::default();
    let fnode = NodeId::mint();
    let f1 = Birth::first(&fabric, &fnode);
    for (label, seq, at, status, want) in [
        ("forwarded first", 5u64, 2_000u64, MemberStatus::ReadyForTraffic, true),
        ("forwarded copy of the held heartbeat", 5, 2_000, MemberStatus::ReadyForTraffic, true),
        ("forwarded lower sequence stamped far ahead", 4, 99_999_999, MemberStatus::Pending, false),
        ("forwarded higher sequence, same stamp", 6, 2_000, MemberStatus::Draining, true),
    ] {
        let taken = fbook.record_forwarded_at(f1.digest(seq, at, status), now);
        assert_eq!(taken, want, "{label}");
        note(label, seq, at, status, taken, &fbook, &fnode);
    }
    assert_eq!(held(&fbook, &fnode), (6, MemberStatus::Draining, 2_000));

    // A new incarnation has its own sequence: it starts again at 1 and is taken along its
    // lineage whatever its stamp; a late digest of the incarnation it replaced is not taken.
    let b2 = b1.successor();
    for (label, seq, at, status, want, new_inc) in [
        ("successor incarnation, sequence starts at 1, lower stamp", 1u64, 10u64, MemberStatus::Pending, true, true),
        ("successor's next heartbeat", 2, 10, MemberStatus::ReadyForTraffic, true, true),
        ("a late digest of the replaced incarnation with a high sequence and a far stamp", 500, 99_999_999, MemberStatus::Draining, false, false),
    ] {
        let d = if new_inc { b2.digest(seq, at, status) } else { b1.digest(seq, at, status) };
        let taken = book.record_at(d, now);
        assert_eq!(taken, want, "{label}");
        note(label, seq, at, status, taken, &book, &node_id);
    }
    let (_, _) = book.get(node_id.as_str()).unwrap();
    assert_eq!(held(&book, &node_id), (2, MemberStatus::ReadyForTraffic, 10));
    assert_eq!(book.get(node_id.as_str()).unwrap().0.node.incarnation, b2.incarnation);

    // The sender, composed with a supplied clock: a real Membership stamps every heartbeat it
    // publishes with the next `digest_seq` (from 1) and the clock's reading, and its own book
    // takes each one however the clock behaves (frozen, stepped back, jumped far ahead).
    let (endpoint, gossip) = {
        let transport = iroh::endpoint::QuicTransportConfig::builder().build();
        let ep = rafka_node_rpc::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
        let g = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        (ep, g)
    };
    let hand = Arc::new(HandClock::default());
    let me = "mesh1.rpc.1";
    let membership = Membership::join(&gossip, &endpoint, &fabric, "mesh1", &MeshId::mint(), me, hand.clone(), vec![]).await.unwrap();
    let sender_id = NodeId::mint();
    let sender = Birth::first(&fabric, &sender_id);
    let mut stamped: Vec<Value> = Vec::new();
    for (label, clock_ms, status) in [
        ("clock frozen at 5000", 5_000u64, MemberStatus::Pending),
        ("clock still frozen", 5_000, MemberStatus::ReadyForTraffic),
        ("clock stepped back", 4_000, MemberStatus::Draining),
        ("clock jumped far ahead", 99_999_999, MemberStatus::ReadyForTraffic),
        ("clock back at zero", 0, MemberStatus::Leaving),
    ] {
        hand.0.store(clock_ms, Ordering::SeqCst);
        membership.publish(&sender.digest(0, 0, status)).await.unwrap();
        let (held, _) = membership.book.get(sender_id.as_str()).expect("the sender's own book holds its heartbeat");
        let n = stamped.len() as u64 + 1;
        assert_eq!((held.digest_seq, held.emitted_at_rafka_ms, held.status), (n, clock_ms, status), "{label}: stamped with the next sequence and the clock's reading, and taken");
        stamped.push(json!({ "clock": label, "digest_seq": held.digest_seq, "emitted_at_rafka_ms": held.emitted_at_rafka_ms, "status": format!("{:?}", held.status) }));
    }
    endpoint.close().await;

    drop(telemetry);
    let spans = collect_spans(&dir);
    let subscribed: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-subscribe" && s["attributes"]["node"] == me).collect();
    assert!(!subscribed.is_empty(), "the real membership emitted its subscribe span: {} spans read", spans.len());
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let result = json!({
        "cell": CELL,
        "ordering": "digest_seq alone within one incarnation; a successor incarnation along the lineage restarts at 1",
        "observations": observations,
        "sender_with_supplied_clock": stamped,
        "subscribe_spans": subscribed.iter().map(|s| json!({ "name": s["name"], "trace_id": s["trace_id"], "span_id": s["span_id"] })).collect::<Vec<_>>(),
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
