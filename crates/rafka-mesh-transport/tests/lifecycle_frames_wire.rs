//! Fixed bytes for the node command and completion frames on the membership channels (R-W1:
//! postcard, positional, append-only). The literals lock the discriminants and field order
//! independently of codec round trips; they are derived from the postcard rules by hand. Never
//! regenerate an expectation from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::{IncarnationId, LifecycleOp, NodeId};
use rafka_mesh_transport::membership::{forward_of, Frame};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
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

/// CONTRACT: `NodeDraining` is variant 10, `NodeLeaving` 11 and `NodeLeft` 12 of the membership
/// frame, appended after `Concern` (9); each is the lifecycle op (build, attempt, operation,
/// node id, incarnation, path.name, event instant) then the forwarding primary. What must NOT
/// happen: a field moved, a variant inserted before the end, or a self-describing encoding.
#[test]
fn node_command_frames_match_the_frozen_wire_schema() {
    let fixtures = [
        (Frame::NodeDraining { op: op("drain-node:mesh1.rpc.1"), forwarded_by: None }, "0a 05626c645f310116647261696e2d6e6f64653a6d657368312e7270632e310c3031323334353637383961620562697274680b6d657368312e7270632e31ac02 00"),
        (Frame::NodeLeaving { op: op("stop-node:mesh1.rpc.1"), forwarded_by: Some("mesh2.admin.1".into()) }, "0b 05626c645f31011573746f702d6e6f64653a6d657368312e7270632e310c3031323334353637383961620562697274680b6d657368312e7270632e31ac02 010d6d657368322e61646d696e2e31"),
        (Frame::NodeLeft { op: op("stop-node:mesh1.rpc.1"), forwarded_by: None }, "0c 05626c645f31011573746f702d6e6f64653a6d657368312e7270632e310c3031323334353637383961620562697274680b6d657368312e7270632e31ac02 00"),
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
    ] {
        let forwarded = forward_of(original.clone(), "mesh2.admin.1", "mesh2").expect("a peer mesh forwards it");
        let (want_op, kind) = match &original {
            Frame::NodeDraining { op, .. } => (op.clone(), 10u8),
            Frame::NodeLeaving { op, .. } => (op.clone(), 11),
            Frame::NodeLeft { op, .. } => (op.clone(), 12),
            _ => unreachable!(),
        };
        assert_eq!(forwarded.encode()[0], kind);
        match forwarded {
            Frame::NodeDraining { op, forwarded_by } | Frame::NodeLeaving { op, forwarded_by } | Frame::NodeLeft { op, forwarded_by } => {
                assert_eq!(op, want_op, "the op is preserved");
                assert_eq!(forwarded_by.as_deref(), Some("mesh2.admin.1"));
            }
            other => panic!("{other:?}"),
        }
        assert!(forward_of(original, "mesh1.admin.1", "mesh1").is_none(), "the frame's own mesh hears the original");
    }
}
