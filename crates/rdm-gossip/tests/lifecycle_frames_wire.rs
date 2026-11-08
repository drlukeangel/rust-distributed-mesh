//! Fixed bytes for the node command and completion frames on the topology channels (R-W1:
//! postcard, positional, append-only). The literals lock the type names and data field order
//! independently of codec round trips; they are derived from the postcard rules by hand. Never
//! regenerate an expectation from the Rust serializer to make a schema change pass.

use crate::gossip_wire::{WireGolden, golden, assert_roundtrip};

use rdm_core::{IncarnationId, LifecycleOp, MeshId, NodeId, PublisherId};
use rdm_gossip::frame::{forward_of, Frame};

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
    PublisherId { incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("p-birth")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() }
}

fn op(operation: &str) -> LifecycleOp {
    LifecycleOp { r#type: "node".into(), mesh: "mesh1".into(),
        build_id: "66h8qrqvaajk".into(),
        operation: rdm_core::OperationId::new(operation, rdm_core::OperationSubject::Birth { node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap() }),
        node_id: NodeId::parse("0123456789ab").unwrap(),
        incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap(),
        path: "mesh1.node".into(), name: "1".into(),
        event_at_rafka_ms: 300,
        traceparent: None,
        tracestate: None,
    }
}

/// CONTRACT: `NodeConnectionsDeleting` is variant 10, `NodeLeaving` 11 and `NodeLeft` 12 and `NodeConnectionsDeleted` 13 of the topology
/// frame, appended after `Concern` (9); each is the lifecycle op (build, operation,
/// node id, incarnation, path.name, owning mesh, event instant) then the forwarding primary. What must NOT
/// happen: a field moved, a variant inserted before the end, or a self-describing encoding.
#[test]
fn node_command_frames_match_the_frozen_wire_schema() {
    let fixtures = [
        (Frame::NodeDeleted { op: op("node.delete"), forwarded_by: None }, "04 04 6e6f6465 0c363668387172717661616a6b 0b 6e6f64652e64656c657465 01 01 0c303132333435363738396162 0c397763707266677776746879 00 00 0c303132333435363738396162 0c397763707266677776746879 0a 6d657368312e6e6f6465 01 31 05 6d65736831 ac02 00"),
        (Frame::NodeConnectionsDeleting { op: op("node.connections.delete"), forwarded_by: None }, "0a 04 6e6f6465 0c363668387172717661616a6b 17 6e6f64652e636f6e6e656374696f6e732e64656c657465 01 01 0c303132333435363738396162 0c397763707266677776746879 00 00 0c303132333435363738396162 0c397763707266677776746879 0a 6d657368312e6e6f6465 01 31 05 6d65736831 ac02 00"),
        (Frame::NodeLeaving { op: op("node.stop"), forwarded_by: Some("mesh2.node-admin.1".into()) }, "0b 04 6e6f6465 0c363668387172717661616a6b 09 6e6f64652e73746f70 01 01 0c303132333435363738396162 0c397763707266677776746879 00 00 0c303132333435363738396162 0c397763707266677776746879 0a 6d657368312e6e6f6465 01 31 05 6d65736831 ac02 01 12 6d657368322e6e6f64652d61646d696e2e31"),
        (Frame::NodeLeft { op: op("node.stop"), forwarded_by: None }, "0c 04 6e6f6465 0c363668387172717661616a6b 09 6e6f64652e73746f70 01 01 0c303132333435363738396162 0c397763707266677776746879 00 00 0c303132333435363738396162 0c397763707266677776746879 0a 6d657368312e6e6f6465 01 31 05 6d65736831 ac02 00"),
        (Frame::NodeConnectionsDeleted { op: op("node.connections.delete"), forwarded_by: None }, "0d 04 6e6f6465 0c363668387172717661616a6b 17 6e6f64652e636f6e6e656374696f6e732e64656c657465 01 01 0c303132333435363738396162 0c397763707266677776746879 00 00 0c303132333435363738396162 0c397763707266677776746879 0a 6d657368312e6e6f6465 01 31 05 6d65736831 ac02 00"),
        // Reserved since Q183 (node.restart removed): its bytes stand at variant 5, never sent.
        (
            Frame::NodeRestarting { op: op("node.restart"), forwarded_by: Some("mesh2.node-admin.1".into()), publisher: publisher(), topology_version: 300 },
            "05 04 6e6f6465 0c363668387172717661616a6b 0c 6e6f64652e72657374617274 01 01 0c303132333435363738396162 0c397763707266677776746879 00 00 0c303132333435363738396162 0c397763707266677776746879 0a 6d657368312e6e6f6465 01 31 05 6d65736831 ac02 01 12 6d657368322e6e6f64652d61646d696e2e31 0c793073657939677277367933 0c 303132333435363738396162 ac02",
        ),
    ];
    for (frame, hex) in fixtures {
        let expected = golden(&frame, &bytes(&hex));
        assert_eq!(frame.golden_encode(), expected, "{frame:?}");
        assert_roundtrip(&frame, &expected);
    }
    // The frames that existed keep their positions.
    assert_eq!(Frame::NodeRestarting { op: op("node.restart"), forwarded_by: None, publisher: publisher(), topology_version: 1 }.type_position(), 5);
}

/// CONTRACT (gossip.md frame catalog: the reserved positions, "None is published. Each type is refused
/// before data decode as via-reserved"): a peer mesh's primary forwards a lifecycle fact it heard on the
/// backbone onto its own channel, preserving the op and naming itself; the fact's own mesh hears the
/// original and forwards nothing. No reserved frame (`NodeConnectionsDeleting`, `NodeConnectionsDeleted`,
/// `NodeLeaving`, `NodeLeft`, `NodeRestarting`) is ever forwarded. Must NOT happen: a reserved frame
/// forwarded into a mesh (code audit 2026-11-08, gossip and core, N7).
#[test]
fn a_peer_mesh_primary_forwards_a_lifecycle_fact_and_no_reserved_frame() {
    let original = Frame::NodeStopped { op: op("node.stop"), forwarded_by: None, topology_version: 3 };
    let forwarded = forward_of(original.clone(), "mesh2.node-admin.1", "mesh2").expect("a peer mesh forwards it");
    match forwarded {
        Frame::NodeStopped { op: carried, forwarded_by, topology_version } => {
            assert_eq!((carried, topology_version), (op("node.stop"), 3), "the op and its version are preserved");
            assert_eq!(forwarded_by.as_deref(), Some("mesh2.node-admin.1"));
        }
        other => panic!("{other:?}"),
    }
    assert!(forward_of(original, "mesh1.node-admin.1", "mesh1").is_none(), "the frame's own mesh hears the original");
    let reserved = [
        ("NodeConnectionsDeleting", Frame::NodeConnectionsDeleting { op: op("node.connections.delete"), forwarded_by: None }),
        ("NodeConnectionsDeleted", Frame::NodeConnectionsDeleted { op: op("node.connections.delete"), forwarded_by: Some("mesh1.node-admin.1".into()) }),
        ("NodeLeft", Frame::NodeLeft { op: op("node.stop"), forwarded_by: Some("mesh1.node-admin.1".into()) }),
        ("NodeLeaving", Frame::NodeLeaving { op: op("node.stop"), forwarded_by: None }),
        ("NodeRestarting", Frame::NodeRestarting { op: op("node.restart"), forwarded_by: None, publisher: publisher(), topology_version: 1 }),
    ];
    for (name, frame) in reserved {
        assert!(forward_of(frame, "mesh2.node-admin.1", "mesh2").is_none(), "{name} is reserved and never forwarded");
    }
}

/// CONTRACT: `MeshLeaving` is variant 14, `MeshLeave` 15 and `MeshLeft` 16, appended after
/// `NodeConnectionsDeleted` (13); each is the mesh id, build, operation, its own fields, the
/// publisher, the event instant and the forwarding primary, in that order.

/// CONTRACT: a peer mesh's primary forwards a mesh frame its author sent onto its own channel,
/// preserving every authored field and naming itself; the author's own mesh hears the original and
/// forwards nothing, and a frame already forwarded is never forwarded again.

/// CONTRACT (node-status-heartbeat-connections.md R-HB1, "The version field": "Each status fact
/// carries its subject's `topology_version` ... a named coordinated schema cut of the lifecycle
/// frames"): the two status events keep their positions, `NodeDeleting` variant 3 and `NodeStopped`
/// 22 (appended after the reserved position 21); `NodeRestarting` 5 is reserved (Q183). Each is the
/// lifecycle op (build, operation, node id, incarnation, path.name, owning mesh, event instant), then the
/// forwarding primary, then the `topology_version` of the subject's mesh at which its subject changed;
/// the publishing mesh-primary is the envelope's `source`, never repeated in the body. A peer mesh's
/// primary forwards each onto its own channel, preserving the op and the version; the frame's own mesh
/// hears the original. What must NOT happen: a field moved, a variant inserted before the end, a body
/// repeating its publisher, or a forward that drops the version.
#[test]
fn status_events_match_the_frozen_wire_schema_and_are_forwarded_with_their_version() {
    let fixtures = [
        (
            Frame::NodeDeleting { op: op("node.delete"), forwarded_by: None, topology_version: 1 },
            "03 04 6e6f6465 0c363668387172717661616a6b 0b 6e6f64652e64656c657465 01 01 0c303132333435363738396162 0c397763707266677776746879 00 00 0c303132333435363738396162 0c397763707266677776746879 0a 6d657368312e6e6f6465 01 31 05 6d65736831 ac02 00 01",
        ),
        (
            Frame::NodeStopped { op: op("node.stop"), forwarded_by: None, topology_version: 300 },
            "13 04 6e6f6465 0c363668387172717661616a6b 09 6e6f64652e73746f70 01 01 0c303132333435363738396162 0c397763707266677776746879 00 00 0c303132333435363738396162 0c397763707266677776746879 0a 6d657368312e6e6f6465 01 31 05 6d65736831 ac02 00 ac02",
        ),
    ];
    for (frame, hex) in fixtures {
        let expected = golden(&frame, &bytes(hex));
        assert_eq!(frame.golden_encode(), expected, "{frame:?}");
        assert_roundtrip(&frame, &expected);
        let kind = frame.type_position();
        let forwarded = forward_of(frame.clone(), "mesh3.node-admin.1", "mesh3").expect("a peer mesh forwards it");
        assert_eq!(forwarded.type_position(), kind);
        match (&frame, &forwarded) {
            (Frame::NodeDeleting { op: a, topology_version: va, .. }, Frame::NodeDeleting { op: b, forwarded_by, topology_version: vb })
            | (Frame::NodeStopped { op: a, topology_version: va, .. }, Frame::NodeStopped { op: b, forwarded_by, topology_version: vb }) => {
                assert_eq!((a, va), (b, vb), "the op and the version are preserved");
                assert_eq!(forwarded_by.as_deref(), Some("mesh3.node-admin.1"));
            }
            other => panic!("{other:?}"),
        }
        assert!(forward_of(frame, "mesh1.node-admin.1", "mesh1").is_none(), "the frame's own mesh hears the original");
    }
}
