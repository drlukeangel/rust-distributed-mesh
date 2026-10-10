//! Fixed bytes for the fabric round hook frames on the membership channels (R-W1: postcard,
//! positional, append-only; gossip.md, "Fabric round gossip hooks"). The literals lock the
//! discriminants and field order independently of codec round trips; they are derived from the
//! postcard rules by hand. Never regenerate an expectation from the Rust serializer to make a
//! schema change pass.

use rafka_mesh_entity::{FabricId, IncarnationId, NodeId, RoundHook};
use rafka_mesh_transport::membership::{forward_of, Frame};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn hook(operation: &str, publisher: &str) -> RoundHook {
    RoundHook {
        fabric_id: FabricId::parse("04raj09p3zp7").unwrap(),
        node_id: NodeId::parse("0123456789ab").unwrap(),
        incarnation: IncarnationId("birth".into()),
        build_id: "bld_1".into(),
        attempt: 1,
        operation: operation.into(),
        publisher: publisher.into(),
        event_at_rafka_ms: 300,
    }
}

// fabric_id "04raj09p3zp7", node_id "0123456789ab", incarnation "birth", build_id "bld_1", attempt 1.
const SUBJECT: &str = "0c 303472616a30397033 7a7037  0c 303132333435363738396162  05 6269727468  05 626c645f31  01";
// operation: length 25, the verb and colon, the fabric id.
const COMMIT: &str = "19 636f6d6d69742d73746174653a 303472616a303970337a7037";
const OPEN: &str = "19 6f70656e2d747261666669633a 303472616a303970337a7037";
// publisher "mesh1.admin.1", event_at_rafka_ms 300 (varint ac 02).
const PUBLISHER: &str = "0d 6d657368312e61646d696e2e31  ac02";
// forwarded_by: None, then Some("mesh2.admin.1").
const NONE: &str = "00";
const SOME: &str = "01 0d 6d657368322e61646d696e2e31";

/// CONTRACT: `StateCommitting` is variant 14, `StateCommitted` 15, `TrafficOpening` 16 and `TrafficOpened`
/// 17 of the membership frame, appended after `NodeDrained` (13); each is the fabric id, the subject node id,
/// its incarnation, the build, the attempt, the operation, the publisher, the Rafka-time instant, then the
/// forwarding primary. What must NOT happen: a field moved, a variant inserted before the end, or a
/// self-describing encoding.
#[test]
fn fabric_round_hook_frames_match_the_frozen_wire_schema() {
    let fixtures = [
        (Frame::StateCommitting { hook: hook("commit-state:04raj09p3zp7", "mesh1.admin.1"), forwarded_by: None }, format!("0e {SUBJECT} {COMMIT} {PUBLISHER} {NONE}")),
        (Frame::StateCommitted { hook: hook("commit-state:04raj09p3zp7", "mesh1.admin.1"), forwarded_by: Some("mesh2.admin.1".into()) }, format!("0f {SUBJECT} {COMMIT} {PUBLISHER} {SOME}")),
        (Frame::TrafficOpening { hook: hook("open-traffic:04raj09p3zp7", "mesh1.admin.1"), forwarded_by: None }, format!("10 {SUBJECT} {OPEN} {PUBLISHER} {NONE}")),
        (Frame::TrafficOpened { hook: hook("open-traffic:04raj09p3zp7", "mesh1.admin.1"), forwarded_by: Some("mesh2.admin.1".into()) }, format!("11 {SUBJECT} {OPEN} {PUBLISHER} {SOME}")),
    ];
    for (frame, hex) in fixtures {
        assert_eq!(frame.encode(), bytes(&hex), "{frame:?}");
        assert_eq!(Frame::decode(&bytes(&hex)).unwrap().encode(), bytes(&hex), "{frame:?} decodes and re-encodes to the same bytes");
    }
    // The frames that existed keep their positions.
    assert_eq!(Frame::NodeDrained { op: rafka_mesh_entity::LifecycleOp {
        build_id: "bld_1".into(), attempt: 1, operation: "drain-node:mesh1.rpc.1".into(), node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId("birth".into()), name: "mesh1.rpc.1".parse().unwrap(), event_at_rafka_ms: 300 },
        forwarded_by: None }.encode()[0], 13);
}

/// CONTRACT: a peer mesh's primary forwards a round hook onto its own channel, the authored fields
/// unchanged and itself named in `forwarded_by`; the author's own mesh hears the original and
/// forwards nothing. Receipt of a forwarded frame carries no liveness: only `forwarded_by` differs.
#[test]
fn a_peer_mesh_primary_forwards_round_hooks_unchanged_and_the_authors_mesh_does_not() {
    for (original, kind) in [
        (Frame::StateCommitting { hook: hook("commit-state:04raj09p3zp7", "mesh1.admin.1"), forwarded_by: None }, 14u8),
        (Frame::StateCommitted { hook: hook("commit-state:04raj09p3zp7", "mesh1.rpc.1"), forwarded_by: Some("mesh1.admin.1".into()) }, 15),
        (Frame::TrafficOpening { hook: hook("open-traffic:04raj09p3zp7", "mesh1.admin.1"), forwarded_by: None }, 16),
        (Frame::TrafficOpened { hook: hook("open-traffic:04raj09p3zp7", "mesh1.rpc.1"), forwarded_by: Some("mesh1.admin.1".into()) }, 17),
    ] {
        let forwarded = forward_of(original.clone(), "mesh2.admin.1", "mesh2").expect("a peer mesh forwards it");
        assert_eq!(forwarded.encode()[0], kind);
        let (want, got, by) = match (&original, forwarded) {
            (Frame::StateCommitting { hook: w, .. }, Frame::StateCommitting { hook: g, forwarded_by })
            | (Frame::StateCommitted { hook: w, .. }, Frame::StateCommitted { hook: g, forwarded_by })
            | (Frame::TrafficOpening { hook: w, .. }, Frame::TrafficOpening { hook: g, forwarded_by })
            | (Frame::TrafficOpened { hook: w, .. }, Frame::TrafficOpened { hook: g, forwarded_by }) => (w.clone(), g, forwarded_by),
            other => panic!("{other:?}"),
        };
        assert_eq!(got, want, "the authored fields are preserved");
        assert_eq!(by.as_deref(), Some("mesh2.admin.1"));
        assert!(forward_of(original, "mesh1.admin.2", "mesh1").is_none(), "the author's own mesh hears the original");
    }
}
