//! Fixed bytes for the `0x23` read family (Q285). These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::filter::{Entity, Filter};
use rdm_core::outcome::MalformedKind;
use rdm_core::protocol::{DecodeFailure, NodeProtocol};
use rdm_core::read::{Read, ReadReply, ReadRequest};
use rdm_core::streaming::{FrameKind, StreamingProtocol};
use rdm_core::{IncarnationId, NodeId};

fn node() -> NodeId {
    NodeId::parse("0123456789ab").unwrap()
}

fn birth() -> IncarnationId {
    IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap()
}

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

/// One atlas frame as `fabric.atlas.get` streams it (Q439, Q285): the fabric with fabric.get's fields
/// and its current Build, its meshes empty; one mesh with mesh.get's fields, its nodes empty; and one
/// node row with a stop accepted on it.
fn atlas_frame() -> ReadReply {
    use rdm_core::views::{AtlasBuildView, AtlasFabricView, AtlasMeshView, AtlasNodeView, FabricView, MeshView, NodeStatus, ProviderKind, ScopeStatus};
    let birth_node = rdm_core::MeshNode { mesh: "mesh1".into(), r#type: "node".into(), node_id: node(), path: "mesh1.node".into(), name: "1".into(), endpoint_id: rdm_core::EndpointId("k".into()), transport_addr: "127.0.0.1:80".parse().unwrap(), incarnation: birth(), supersedes: None };
    let row = AtlasNodeView {
        node: rdm_core::wire::ReadNode::of_node(&birth_node, rdm_core::model::NodeStatus::Ready, Vec::new(), 7, Some(&rdm_core::NodeRuntime { runtime: None, data_dir: Some("/d".into()) })),
        stopped: false,
        accepted: Some(rdm_core::OperationId::node("node.stop", &node())),
        observed: Some(NodeStatus::Ready),
    };
    let mesh = MeshView { renewals: None, id: Some(rdm_core::MeshId::parse("1123456789ab").unwrap()), name: "mesh1".into(), status: ScopeStatus::Ready, primary_admin: Some(node()), nodes: Vec::new() };
    let fabric = FabricView { next_expiry: None, topology_storage_provider: None, roots: None, ca: None, previous_topology_storage_providers: Vec::new(), id: rdm_core::FabricId::parse("2123456789ab").unwrap(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process, fabric_primary: Some(node()), meshes: vec![mesh.clone()], build_id: Some("b".into()) };
    ReadReply::Atlas {
        fabric: AtlasFabricView::of(fabric, Some(AtlasBuildView { build_id: "b".into(), phase: rdm_core::build::BuildPhase::Complete }), Vec::new()),
        mesh: Some(AtlasMeshView::of(mesh, Vec::new())),
        nodes: vec![row],
    }
}

#[test]
fn requests_match_the_frozen_four_variant_wire_schema() {
    for (q, hex) in [
        (ReadRequest::Get { entity: Entity::Nodes, filter: Filter::new() }, "00 02 00"),
        (ReadRequest::Get { entity: Entity::Fabrics, filter: Filter::new().eq("mesh", ["m"]) }, "00 00 01 04 6d657368 01 01 6d"),
        (ReadRequest::GetConnections { node: node(), incarnation: birth(), filter: Filter::new() }, "01 0c 303132333435363738396162 0c 397763707266677776746879 00"),
        // fabric.atlas.get (Q439), appended last: no field
        (ReadRequest::GetAtlas, "02"),
        // mesh.atlas.get (Q439), appended last: filter (pairs len 1: "name" | values len 1: "mesh2")
        (ReadRequest::GetMeshAtlas { filter: Filter::new().eq("name", ["mesh2"]) }, "03 01 04 6e616d65 01 05 6d65736832"),
    ] {
        assert_eq!(Read::encode_request(&q).unwrap(), bytes(hex), "{q:?}");
        assert_eq!(Read::decode_request(&bytes(hex)).unwrap(), q);
    }
    assert_eq!(Read::REQUEST_VARIANTS, 4);
    assert_eq!(Read::decode_request(&[4]), Err(DecodeFailure::UnknownVariant));
}

#[test]
fn replies_match_the_frozen_sixteen_variant_wire_schema() {
    let mut variants = std::collections::BTreeSet::new();
    use ReadReply::*;
    let fixtures = [
        (Started { read: "r".into() }, "00 01 72", FrameKind::Started),
        (Nodes { nodes: Vec::new() }, "01 00", FrameKind::Data),
        (Meshes { meshes: Vec::new() }, "02 00", FrameKind::Data),
        (Fabrics { fabrics: Vec::new() }, "03 00", FrameKind::Data),
        (End { rows: 3, frames: 2 }, "04 03 02", FrameKind::Terminal),
        (Connections { node: "n".into(), complete: true, connections: Vec::new() }, "0e 01 6e 01 00", FrameKind::Data),
        // an atlas frame (Q439, Q285), appended last: the fabric's fabric.get fields (id | name | status
        // Ready | provider Process | fabric_primary Some | build_id Some("b") | next_expiry None |
        // topology_storage_provider None | previous [] | roots None | ca None) | build Some(build_id "b" |
        // phase Complete (2)) | meshes (len 0) | mesh Some: mesh.get's fields (id Some | name | status
        // Ready | primary_admin Some | renewals None) | nodes (len 0) | nodes (len 1): the node row (node_id
        // | incarnation | type | path | name | endpoint_id "k" | transport_addr V4 127.0.0.1:80 | supersedes
        // None | status Ready | primary [] | digest_seq 7 | runtime None | data_dir Some("/d")) | stopped
        // false | accepted Some(action "node.stop" | subject Some(Node (0) node_id) | step None | occurrence
        // None) | observed Some(Ready)
        (atlas_frame(), concat!(
            "0f",
            "0c323132333435363738396162", "0766616272696331", "01", "00", "010c303132333435363738396162", "010162", "00", "00", "00", "00", "00",
            "01", "0162", "02",
            "00",
            "01", "010c313132333435363738396162", "056d65736831", "01", "010c303132333435363738396162", "00",
            "00",
            "01",
            "0c303132333435363738396162", "0c397763707266677776746879", "046e6f6465", "0a6d657368312e6e6f6465", "0131", "016b", "007f00000150", "00", "01", "00", "07", "00", "01022f64",
            "00",
            "01", "096e6f64652e73746f70", "01", "00", "0c303132333435363738396162", "00", "00",
            "0101",
        ), FrameKind::Data),
    ];
    for (r, hex, kind) in fixtures {
        variants.insert(Read::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<Read>(&r);
        assert_eq!(Read::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Read::decode_reply(&bytes(hex)).unwrap(), r);
        assert_eq!(Read::frame_kind(&r), kind, "{r:?}");
    }
    let refusals = [
        (RejectedFilter { reason: "f".into() }, "05 01 66"),
        (ReadOverBound { read: "r".into(), row: "w".into(), bytes: 5, max: 4 }, "06 01 72 01 77 05 04"),
        (RejectedSourceFailed { source: "s".into(), op: "o".into(), cause: "c".into() }, "07 01 73 01 6f 01 63"),
        (NotReady { reason: "n".into() }, "08 01 6e"),
        (PeerUnresolved { reason: "p".into() }, "09 01 70"),
        (Busy { reason: "b".into() }, "0a 01 62"),
        (ConnectionsDeleting { reason: "d".into() }, "0b 01 64"),
        (Malformed { kind: MalformedKind::Corrupt }, "0c 02"),
        (Unauthorized { reason: "u".into() }, "0d 01 75"),
    ];
    for (r, hex) in refusals {
        variants.insert(Read::encode_reply(&r).unwrap()[0]);
        crate::outcome_codes::check::<Read>(&r);
        assert_eq!(Read::encode_reply(&r).unwrap(), bytes(hex), "{r:?}");
        assert_eq!(Read::decode_reply(&bytes(hex)).unwrap(), r);
        assert!(matches!(Read::frame_kind(&r), FrameKind::Refusal(_)), "{r:?}");
    }
    assert_eq!(Read::REPLY_VARIANTS, 16);
    assert_eq!(Read::decode_reply(&[16]), Err(DecodeFailure::UnknownVariant));
    assert_eq!(variants.len(), Read::REPLY_VARIANTS as usize);
    crate::outcome_codes::check_malformed::<Read>();
}
