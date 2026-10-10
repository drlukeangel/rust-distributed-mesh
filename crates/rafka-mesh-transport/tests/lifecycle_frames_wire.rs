//! Fixed bytes for the node command and completion frames on the membership channels (R-W1:
//! postcard, positional, append-only). The literals lock the discriminants and field order
//! independently of codec round trips; they are derived from the postcard rules by hand. Never
//! regenerate an expectation from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::{IncarnationId, LifecycleOp, MeshId, NodeId};
use rafka_mesh_transport::membership::{forward_of, Frame};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

const OPERATION: &str = "shutdown-mesh:1123456789ab";

fn mesh() -> MeshId {
    MeshId::parse("1123456789ab").unwrap()
}

fn op(operation: &str) -> LifecycleOp {
    LifecycleOp {
        build_id: "bld_1".into(),
        attempt: 1,
        operation: operation.into(),
        node_id: NodeId::parse("0123456789ab").unwrap(),
        incarnation: IncarnationId("birth".into()),
        name: "mesh1.rpc.1".parse().unwrap(),
        event_at_rafka_ms: 300,
    }
}

/// CONTRACT: `NodeDraining` is variant 10, `NodeLeaving` 11 and `NodeLeft` 12 and `NodeDrained` 13 of the membership
/// frame, appended after `Concern` (9); each is the lifecycle op (build, attempt, operation,
/// node id, incarnation, path.name, event instant) then the forwarding primary. What must NOT
/// happen: a field moved, a variant inserted before the end, or a self-describing encoding.
#[test]
fn node_command_frames_match_the_frozen_wire_schema() {
    let fixtures = [
        (Frame::NodeDraining { op: op("drain-node:mesh1.rpc.1"), forwarded_by: None }, "0a 05626c645f310116647261696e2d6e6f64653a6d657368312e7270632e310c3031323334353637383961620562697274680b6d657368312e7270632e31ac02 00"),
        (Frame::NodeLeaving { op: op("stop-node:mesh1.rpc.1"), forwarded_by: Some("mesh2.admin.1".into()) }, "0b 05626c645f31011573746f702d6e6f64653a6d657368312e7270632e310c3031323334353637383961620562697274680b6d657368312e7270632e31ac02 010d6d657368322e61646d696e2e31"),
        (Frame::NodeLeft { op: op("stop-node:mesh1.rpc.1"), forwarded_by: None }, "0c 05626c645f31011573746f702d6e6f64653a6d657368312e7270632e310c3031323334353637383961620562697274680b6d657368312e7270632e31ac02 00"),
        (Frame::NodeDrained { op: op("drain-node:mesh1.rpc.1"), forwarded_by: None }, "0d 05626c645f310116647261696e2d6e6f64653a6d657368312e7270632e310c3031323334353637383961620562697274680b6d657368312e7270632e31ac02 00"),
    ];
    for (frame, hex) in fixtures {
        assert_eq!(frame.encode(), bytes(&hex), "{frame:?}");
        assert_eq!(Frame::decode(&bytes(&hex)).unwrap().encode(), bytes(&hex), "{frame:?} decodes and re-encodes to the same bytes");
    }
    // The frames that existed keep their positions.
    assert_eq!(Frame::NodeRestarting { op: op("restart-node:mesh1.rpc.1"), forwarded_by: None }.encode()[0], 5);
}

/// CONTRACT: a peer mesh's primary forwards a command or completion frame onto its own channel,
/// preserving the op and naming itself; the frame's own mesh hears the original and forwards
/// nothing.
#[test]
fn a_peer_mesh_primary_forwards_node_command_frames_and_the_own_mesh_does_not() {
    for original in [
        Frame::NodeDraining { op: op("drain-node:mesh1.rpc.1"), forwarded_by: None },
        Frame::NodeLeaving { op: op("stop-node:mesh1.rpc.1"), forwarded_by: None },
        Frame::NodeLeft { op: op("stop-node:mesh1.rpc.1"), forwarded_by: Some("mesh1.admin.1".into()) },
        Frame::NodeDrained { op: op("drain-node:mesh1.rpc.1"), forwarded_by: Some("mesh1.admin.1".into()) },
    ] {
        let forwarded = forward_of(original.clone(), "mesh2.admin.1", "mesh2").expect("a peer mesh forwards it");
        let (want_op, kind) = match &original {
            Frame::NodeDraining { op, .. } => (op.clone(), 10u8),
            Frame::NodeLeaving { op, .. } => (op.clone(), 11),
            Frame::NodeLeft { op, .. } => (op.clone(), 12),
            Frame::NodeDrained { op, .. } => (op.clone(), 13),
            _ => unreachable!(),
        };
        assert_eq!(forwarded.encode()[0], kind);
        match forwarded {
            Frame::NodeDraining { op, forwarded_by } | Frame::NodeLeaving { op, forwarded_by } | Frame::NodeLeft { op, forwarded_by } | Frame::NodeDrained { op, forwarded_by } => {
                assert_eq!(op, want_op, "the op is preserved");
                assert_eq!(forwarded_by.as_deref(), Some("mesh2.admin.1"));
            }
            other => panic!("{other:?}"),
        }
        assert!(forward_of(original, "mesh1.admin.1", "mesh1").is_none(), "the frame's own mesh hears the original");
    }
}

/// CONTRACT: `MeshLeaving` is variant 14, `MeshLeave` 15 and `MeshLeft` 16, appended after
/// `NodeDrained` (13); each is the mesh id, build, attempt, operation, its own fields, the
/// publisher, the event instant and the forwarding primary, in that order.
#[test]
fn mesh_leave_frames_match_the_frozen_wire_schema() {
    let fixtures = [
        (Frame::MeshLeaving { mesh_id: mesh(), build_id: "bld_1".into(), attempt: 1, operation: OPERATION.into(), publisher: "mesh1.admin.1".into(), event_at_rafka_ms: 300, forwarded_by: None }, "0e0c31313233343536373839616205626c645f31011a73687574646f776e2d6d6573683a3131323334353637383961620d6d657368312e61646d696e2e31ac0200"),
        (Frame::MeshLeave { mesh_id: mesh(), build_id: "bld_1".into(), attempt: 1, operation: OPERATION.into(), final_node_id: NodeId::parse("0123456789ab").unwrap(), final_incarnation: IncarnationId("birth".into()), receipt_manifest: "bld_1/1/other-members-exited".into(), publisher: "mesh2.admin.1".into(), event_at_rafka_ms: 300, forwarded_by: Some("mesh3.admin.1".into()) }, "0f0c31313233343536373839616205626c645f31011a73687574646f776e2d6d6573683a3131323334353637383961620c3031323334353637383961620562697274681c626c645f312f312f6f746865722d6d656d626572732d6578697465640d6d657368322e61646d696e2e31ac02010d6d657368332e61646d696e2e31"),
        (Frame::MeshLeft { mesh_id: mesh(), build_id: "bld_1".into(), attempt: 1, operation: OPERATION.into(), receipt_manifest: "bld_1/1/all-members-exited".into(), publisher: "mesh1.admin.1".into(), event_at_rafka_ms: 300, forwarded_by: None }, "100c31313233343536373839616205626c645f31011a73687574646f776e2d6d6573683a3131323334353637383961621a626c645f312f312f616c6c2d6d656d626572732d6578697465640d6d657368312e61646d696e2e31ac0200"),
    ];
    for (frame, hex) in fixtures {
        assert_eq!(frame.encode(), bytes(&hex), "{frame:?}");
        assert_eq!(Frame::decode(&bytes(&hex)).unwrap().encode(), bytes(&hex), "{frame:?} decodes and re-encodes to the same bytes");
    }
    assert_eq!(Frame::NodeDrained { op: op("drain-node:mesh1.rpc.1"), forwarded_by: None }.encode()[0], 13);
}

/// CONTRACT: a peer mesh's primary forwards a mesh frame its author sent onto its own channel,
/// preserving every authored field and naming itself; the author's own mesh hears the original and
/// forwards nothing, and a frame already forwarded is never forwarded again.
#[test]
fn a_peer_mesh_primary_forwards_mesh_frames_once_and_the_authors_mesh_does_not() {
    let authored = [
        Frame::MeshLeaving { mesh_id: mesh(), build_id: "bld_1".into(), attempt: 1, operation: OPERATION.into(), publisher: "mesh1.admin.1".into(), event_at_rafka_ms: 300, forwarded_by: None },
        Frame::MeshLeave { mesh_id: mesh(), build_id: "bld_1".into(), attempt: 1, operation: OPERATION.into(), final_node_id: NodeId::parse("0123456789ab").unwrap(), final_incarnation: IncarnationId("birth".into()), receipt_manifest: "m".into(), publisher: "mesh1.admin.1".into(), event_at_rafka_ms: 300, forwarded_by: None },
        Frame::MeshLeft { mesh_id: mesh(), build_id: "bld_1".into(), attempt: 1, operation: OPERATION.into(), receipt_manifest: "m".into(), publisher: "mesh1.admin.1".into(), event_at_rafka_ms: 300, forwarded_by: None },
    ];
    for original in authored {
        let forwarded = forward_of(original.clone(), "mesh2.admin.1", "mesh2").expect("a peer mesh forwards it");
        let (kind, a, b) = match (&original, &forwarded) {
            (Frame::MeshLeaving { publisher: p, .. }, Frame::MeshLeaving { publisher, forwarded_by, .. }) => (14u8, p.clone(), (publisher.clone(), forwarded_by.clone())),
            (Frame::MeshLeave { publisher: p, .. }, Frame::MeshLeave { publisher, forwarded_by, .. }) => (15, p.clone(), (publisher.clone(), forwarded_by.clone())),
            (Frame::MeshLeft { publisher: p, .. }, Frame::MeshLeft { publisher, forwarded_by, .. }) => (16, p.clone(), (publisher.clone(), forwarded_by.clone())),
            other => panic!("{other:?}"),
        };
        assert_eq!(forwarded.encode()[0], kind);
        assert_eq!(b, (a, Some("mesh2.admin.1".to_string())), "the author is preserved, the forwarder named");
        assert!(forward_of(original.clone(), "mesh1.admin.2", "mesh1").is_none(), "the author's own mesh hears the original");
        assert!(forward_of(forwarded, "mesh3.admin.1", "mesh3").is_none(), "a forwarded frame is not forwarded again");
    }
}
