//! Fixed bytes for the node command and completion frames on the membership channels (R-W1:
//! postcard, positional, append-only). The literals lock the discriminants and field order
//! independently of codec round trips; they are derived from the postcard rules by hand. Never
//! regenerate an expectation from the Rust serializer to make a schema change pass.

use rdm_core::{IncarnationId, LifecycleOp, MeshId, NodeId, PublisherId};
use rdm_transport::membership::{forward_of, Frame};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

const OPERATION: &str = "shutdown-mesh:1123456789ab";

fn mesh() -> MeshId {
    MeshId::parse("1123456789ab").unwrap()
}

/// The publisher a status event names: mesh1's primary, birth `p-birth`.
fn publisher() -> PublisherId {
    PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("p-birth")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() }
}

fn op(operation: &str) -> LifecycleOp {
    LifecycleOp {
        build_id: "66h8qrqvaajk".into(),
        attempt: 1,
        operation: operation.into(),
        node_id: NodeId::parse("0123456789ab").unwrap(),
        incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap(),
        name: "mesh1.rpc.1".parse().unwrap(),
        event_at_rafka_ms: 300,
    }
}

/// CONTRACT: `NodeConnectionsDeleting` is variant 10, `NodeLeaving` 11 and `NodeLeft` 12 and `NodeConnectionsDeleted` 13 of the membership
/// frame, appended after `Concern` (9); each is the lifecycle op (build, attempt, operation,
/// node id, incarnation, path.name, event instant) then the forwarding primary. What must NOT
/// happen: a field moved, a variant inserted before the end, or a self-describing encoding.
#[test]
fn node_command_frames_match_the_frozen_wire_schema() {
    let fixtures = [
        (Frame::NodeConnectionsDeleting { op: op("drain-node:mesh1.rpc.1"), forwarded_by: None }, "0a 0c363668387172717661616a6b0116647261696e2d6e6f64653a6d657368312e7270632e310c3031323334353637383961620c3977637072666777767468790b6d657368312e7270632e31ac02 00"),
        (Frame::NodeLeaving { op: op("stop-node:mesh1.rpc.1"), forwarded_by: Some("mesh2.admin.1".into()) }, "0b 0c363668387172717661616a6b011573746f702d6e6f64653a6d657368312e7270632e310c3031323334353637383961620c3977637072666777767468790b6d657368312e7270632e31ac02 010d6d657368322e61646d696e2e31"),
        (Frame::NodeLeft { op: op("stop-node:mesh1.rpc.1"), forwarded_by: None }, "0c 0c363668387172717661616a6b011573746f702d6e6f64653a6d657368312e7270632e310c3031323334353637383961620c3977637072666777767468790b6d657368312e7270632e31ac02 00"),
        (Frame::NodeConnectionsDeleted { op: op("drain-node:mesh1.rpc.1"), forwarded_by: None }, "0d 0c363668387172717661616a6b0116647261696e2d6e6f64653a6d657368312e7270632e310c3031323334353637383961620c3977637072666777767468790b6d657368312e7270632e31ac02 00"),
    ];
    for (frame, hex) in fixtures {
        assert_eq!(frame.encode(), bytes(&hex), "{frame:?}");
        assert_eq!(Frame::decode(&bytes(&hex)).unwrap().encode(), bytes(&hex), "{frame:?} decodes and re-encodes to the same bytes");
    }
    // The frames that existed keep their positions.
    assert_eq!(Frame::NodeRestarting { op: op("restart-node:mesh1.rpc.1"), forwarded_by: None, publisher: publisher(), topology_version: 1 }.encode()[0], 5);
}

/// CONTRACT: a peer mesh's primary forwards a command or completion frame onto its own channel,
/// preserving the op and naming itself; the frame's own mesh hears the original and forwards
/// nothing. `NodeLeaving` and `NodeLeft` are reserved positions (gossip.md frame catalog): never forwarded.
#[test]
fn a_peer_mesh_primary_forwards_node_command_frames_and_the_own_mesh_does_not() {
    for original in [
        Frame::NodeConnectionsDeleting { op: op("drain-node:mesh1.rpc.1"), forwarded_by: None },
        Frame::NodeConnectionsDeleted { op: op("drain-node:mesh1.rpc.1"), forwarded_by: Some("mesh1.admin.1".into()) },
    ] {
        let forwarded = forward_of(original.clone(), "mesh2.admin.1", "mesh2").expect("a peer mesh forwards it");
        let (want_op, kind) = match &original {
            Frame::NodeConnectionsDeleting { op, .. } => (op.clone(), 10u8),
            Frame::NodeConnectionsDeleted { op, .. } => (op.clone(), 13),
            _ => unreachable!(),
        };
        assert_eq!(forwarded.encode()[0], kind);
        match forwarded {
            Frame::NodeConnectionsDeleting { op, forwarded_by } | Frame::NodeConnectionsDeleted { op, forwarded_by } => {
                assert_eq!(op, want_op, "the op is preserved");
                assert_eq!(forwarded_by.as_deref(), Some("mesh2.admin.1"));
            }
            other => panic!("{other:?}"),
        }
        assert!(forward_of(original, "mesh1.admin.1", "mesh1").is_none(), "the frame's own mesh hears the original");
    }
    let left = Frame::NodeLeft { op: op("stop-node:mesh1.rpc.1"), forwarded_by: Some("mesh1.admin.1".into()) };
    assert!(forward_of(left, "mesh2.admin.1", "mesh2").is_none(), "NodeLeft is refused and never forwarded");
    let leaving = Frame::NodeLeaving { op: op("stop-node:mesh1.rpc.1"), forwarded_by: None };
    assert!(forward_of(leaving, "mesh2.admin.1", "mesh2").is_none(), "NodeLeaving is refused and never forwarded");
}

/// CONTRACT: `MeshLeaving` is variant 14, `MeshLeave` 15 and `MeshLeft` 16, appended after
/// `NodeConnectionsDeleted` (13); each is the mesh id, build, attempt, operation, its own fields, the
/// publisher, the event instant and the forwarding primary, in that order.

/// CONTRACT: a peer mesh's primary forwards a mesh frame its author sent onto its own channel,
/// preserving every authored field and naming itself; the author's own mesh hears the original and
/// forwards nothing, and a frame already forwarded is never forwarded again.

/// CONTRACT (node-status-heartbeat-connections.md R-HB1, "The version field": "Each status fact
/// carries its subject's `topology_version` ... a named coordinated schema cut of the lifecycle
/// frames"): the three status events keep their positions, `NodeDeleting` variant 3,
/// `NodeRestarting` 5 and `NodeStopped` 22 (appended after `TrafficOpened`, 21). Each is the
/// lifecycle op (build, attempt, operation, node id, incarnation, path.name, event instant), then the
/// forwarding primary, then the publisher's exact identity (node, incarnation) and the
/// `topology_version` at which its subject changed, appended after `forwarded_by`. A peer mesh's
/// primary forwards each onto its own channel, preserving the op, the publisher and the version;
/// the frame's own mesh hears the original. What must NOT happen: a field moved, a variant
/// inserted before the end, or a forward that drops the publisher or the version.
#[test]
fn status_events_match_the_frozen_wire_schema_and_are_forwarded_with_their_publisher_and_version() {
    let fixtures = [
        (
            Frame::NodeDeleting { op: op("retire-node:mesh1.rpc.1"), forwarded_by: None, publisher: publisher(), topology_version: 1 },
            "03 0c363668387172717661616a6b01177265746972652d6e6f64653a6d657368312e7270632e310c3031323334353637383961620c3977637072666777767468790b6d657368312e7270632e31ac02 00 0d6d657368312e61646d696e2e310c793073657939677277367933 0c 303132333435363738396162 01",
        ),
        (
            Frame::NodeRestarting { op: op("restart-node:mesh1.rpc.1"), forwarded_by: Some("mesh2.admin.1".into()), publisher: publisher(), topology_version: 300 },
            "05 0c363668387172717661616a6b0118726573746172742d6e6f64653a6d657368312e7270632e310c3031323334353637383961620c3977637072666777767468790b6d657368312e7270632e31ac02 010d6d657368322e61646d696e2e31 0d6d657368312e61646d696e2e310c793073657939677277367933 0c 303132333435363738396162 ac02",
        ),
        (
            Frame::NodeStopped { op: op("stop-node:mesh1.rpc.1"), forwarded_by: None, publisher: publisher(), topology_version: 300 },
            "13 0c363668387172717661616a6b011573746f702d6e6f64653a6d657368312e7270632e310c3031323334353637383961620c3977637072666777767468790b6d657368312e7270632e31ac02 00 0d6d657368312e61646d696e2e310c793073657939677277367933 0c 303132333435363738396162 ac02",
        ),
    ];
    for (frame, hex) in fixtures {
        assert_eq!(frame.encode(), bytes(hex), "{frame:?}");
        assert_eq!(Frame::decode(&bytes(hex)).unwrap().encode(), bytes(hex), "{frame:?} decodes and re-encodes to the same bytes");
        let kind = frame.encode()[0];
        let forwarded = forward_of(frame.clone(), "mesh3.admin.1", "mesh3").expect("a peer mesh forwards it");
        assert_eq!(forwarded.encode()[0], kind);
        match (&frame, &forwarded) {
            (Frame::NodeDeleting { op: a, publisher: pa, topology_version: va, .. }, Frame::NodeDeleting { op: b, forwarded_by, publisher: pb, topology_version: vb })
            | (Frame::NodeRestarting { op: a, publisher: pa, topology_version: va, .. }, Frame::NodeRestarting { op: b, forwarded_by, publisher: pb, topology_version: vb })
            | (Frame::NodeStopped { op: a, publisher: pa, topology_version: va, .. }, Frame::NodeStopped { op: b, forwarded_by, publisher: pb, topology_version: vb }) => {
                assert_eq!((a, pa, va), (b, pb, vb), "the op, the publisher and the version are preserved");
                assert_eq!(forwarded_by.as_deref(), Some("mesh3.admin.1"));
            }
            other => panic!("{other:?}"),
        }
        assert!(forward_of(frame, "mesh1.admin.1", "mesh1").is_none(), "the frame's own mesh hears the original");
    }
}
