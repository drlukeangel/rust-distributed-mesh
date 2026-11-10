//! R-W1 (Luke 2026-10-08), UNIT layer: run by `scripts/i143-acceptance-gate.sh i143-rw1-unit`, which
//! exports `I143_ACCEPTANCE_DIR`; the cell leaves its `result.json` there. A pure model cell:
//! no estate, no spans.
//!
//! CONTRACT: every internal mesh gossip frame is postcard. Every `Frame` variant of the
//! topology channels encodes, decodes to the value it was made from, and re-encodes to the same
//! bytes; a chunk the packers produce encodes under `MAX_MESSAGE_BYTES`; a frame this build does not
//! read is refused with a named reason, a JSON frame included.

use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId, PathName};
use rdm_transport::chunking::MAX_MESSAGE_BYTES;
use rdm_gossip::frame::Frame;
use rdm_gossip::aggregate::{chunks_of, Full, PublisherId};
use serde_json::{json, Value};
use std::path::PathBuf;

const CELL: &str = "every_gossip_frame_encodes_and_decodes_through_postcard_under_the_ceiling";

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/rw1/unit").join(CELL),
    }
}

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn digest(ordinal: u32, full: bool) -> MeshDigest {
    MeshDigest { 
        fabric_id: fabric(),
        node: MeshNode { mesh: "mesh1".into(), r#type: "node".into(),
            node_id: NodeId::mint(),
            path: "mesh1.node".into(), name: (ordinal.to_string()).to_string(),
            endpoint_id: EndpointId(format!("key{ordinal}")),
            transport_addr: format!("127.0.0.1:{}", 41_000 + ordinal).parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: full.then(IncarnationId::mint),
        },
        status: NodeStatus::Ready,
        digest_seq: 3 + ordinal as u64,
        emitted_at_rafka_ms: 1_000 + ordinal as u64,
        primary: Vec::new(),
    }
}

/// A node's Digest at pending: its first into its mesh, at its accepted join (gossip.md §3.1 `Digest`).
fn pending(ordinal: u32, full: bool) -> MeshDigest {
    MeshDigest { status: NodeStatus::Pending, ..digest(ordinal, full) }
}

/// `d` listed at `status`.
fn with_status(d: MeshDigest, status: NodeStatus) -> MeshDigest {
    MeshDigest { status, ..d }
}

/// A node-type primary that is also the fabric-primary, each role at its revision (Q392).
fn primary(ordinal: u32) -> MeshDigest {
    let mut d = digest(ordinal, false);
    d.set_primary(Some(4));
    d.set_fabric_primary(Some(300));
    d
}

fn op(action: &str, name: PathName, mesh: &str, r#type: &str) -> LifecycleOp {
    let node_id = NodeId::mint();
    LifecycleOp { r#type: r#type.to_string(), mesh: mesh.into(), build_id: "3qcfk1kq47m9".into(), operation: rdm_core::OperationId::node(action, &node_id), node_id, incarnation: IncarnationId::mint(), path: name.path.clone(), name: name.name.clone(), event_at_rafka_ms: 7, traceparent: None, tracestate: None }
}

fn publisher() -> PublisherId {
    let node_id = NodeId::mint();
    PublisherId { incarnation: IncarnationId::mint(), node_id }
}

/// One explicit executor birth shared by these independent wire-shape samples.
fn executor() -> rdm_core::framing::Birth {
    rdm_core::framing::Birth { node_id: NodeId::parse("100000000001").unwrap(), incarnation: IncarnationId::parse("100000000002").unwrap() }
}

/// Every `Frame` variant, each Digest shape (a first birth, a lineage, primary roles).
fn frames() -> Vec<Frame> {
    let p = publisher();
    vec![
        Frame::digest(&pending(1, false)),
        Frame::digest(&pending(2, true)),
        Frame::digest(&primary(3)),
        // One list of nodes, each with its status: a node under a delete and a deleted node say so (Q390).
        Frame::TopologyFull {
            mesh: "mesh2".into(),
            mesh_id: Some(MeshId::mint()),
            chunk_index: 1,
            chunk_count: 3,
            nodes: [digest(1, false), with_status(digest(2, true), NodeStatus::Deleting), with_status(digest(3, true), NodeStatus::Deleted), primary(5)].iter().map(rdm_core::wire::NodeEntry::of).collect(),
        }.authored_aggregate(&crate::gossip_wire::author(), 9, 11),
        Frame::TopologyFull {
            mesh: "mesh2".into(),
            mesh_id: None,
            chunk_index: 0,
            chunk_count: 1,
            nodes: vec![],
        },
        Frame::TopologyDelta {
            mesh: "mesh2".into(),
            mesh_id: Some(MeshId::mint()),
            base_version: 8,
            chunk_index: 0,
            chunk_count: 1,
            added: vec![rdm_core::wire::NodeEntry::of(&digest(4, true))],
            changed: vec![rdm_core::wire::NodeStatusEntry::of(&digest(5, true))],
            removed: vec![rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }],
        }
        .authored_delta(&crate::gossip_wire::author(), 9, 12),
        Frame::NodeDeleting { op: op("node.delete", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: None, topology_version: 10 },
        Frame::NodeDeleted { op: op("node.delete", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: Some(executor().node_id.to_string()) },
        Frame::NodeRestarting { op: op("node.restart", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: None, publisher: p.clone(), topology_version: 11 },
        Frame::NodeStopped { op: op("node.stop", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: Some(executor().node_id.to_string()), topology_version: 12 },
        Frame::NodeConnectionsDeleting { op: op("node.connection.delete", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: None },
        Frame::NodeLeaving { op: op("node.stop", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: Some(executor().node_id.to_string()) },
        Frame::NodeLeft { op: op("node.stop", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: Some(NodeId::parse("200000000001").unwrap().to_string()) },
        Frame::NodeConnectionsDeleted { op: op("node.connection.delete", PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node"), forwarded_by: None },
        Frame::MeshStatus { mesh: "mesh2".into(), mesh_id: rdm_core::fixture_mesh_id("mesh2"), status: "ready".into(), publisher: NodeId::parse("200000000001").unwrap().to_string(), forwarded_by: None, changed_at_rafka_ms: 77 },
        Frame::FabricStatus { fabric: fabric(), status: "ready".into(), publisher: executor().node_id.to_string(), forwarded_by: Some(NodeId::parse("200000000001").unwrap().to_string()), changed_at_rafka_ms: 78 },
        Frame::Concern { role: rdm_core::PrimaryRole::FabricPrimary, node_id: rdm_core::NodeId::mint(), incarnation: IncarnationId::mint(), observer: NodeId::parse("200000000001").unwrap().to_string() },
        Frame::NewFabricPrimary { primary: rdm_core::Primary { mesh: "mesh2".into(), node_id: rdm_core::NodeId::mint(), incarnation: IncarnationId::mint(), revision: 4 }, endpoint_id: "key2".into(), transport_addr: "127.0.0.1:41002".parse().unwrap() },
    ]
}

#[test]
fn every_gossip_frame_encodes_and_decodes_through_postcard_under_the_ceiling() {
    let mut result = serde_json::Map::new();

    // ---- every Frame variant: decode(encode(f)) is f, and re-encodes to the same bytes ----
    let all_frames = frames();
    for f in &all_frames {
        let event = crate::gossip_wire::authored(f);
        let bytes = event.encode();
        crate::gossip_wire::assert_roundtrip(f, &bytes);
        assert!(bytes.len() <= MAX_MESSAGE_BYTES, "{f:?} fits one complete gossip message");
    }
    result.insert("frame_variants_round_tripped".into(), json!(all_frames.len()));

    let members: Vec<MeshDigest> = (1..=150).map(|k| if k % 2 == 0 { primary(k) } else { digest(k, k % 3 == 0) }).collect();
    let full = Full::new(members.clone());
    let chunks = chunks_of(&full, |nodes, chunk_index, chunk_count| Frame::TopologyFull {
        mesh: "mesh1".into(),
        mesh_id: None,
        chunk_index,
        chunk_count,
        nodes: nodes.iter().map(rdm_core::wire::NodeEntry::of).collect(),
    }.authored_aggregate(&crate::gossip_wire::author(), 5, 1));
    assert!(chunks.len() > 2);
    let mut heard = 0;
    for c in &chunks {
        let bytes = c.encode();
        assert!(bytes.len() <= MAX_MESSAGE_BYTES, "a TopologyFull chunk of {} bytes fits the ceiling", bytes.len());
        match Frame::decode(&bytes).unwrap().into_data() {
            Frame::TopologyFull { nodes, .. } => heard += nodes.len(),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(heard, members.len(), "the chunks carry every member");
    result.insert("nodes_aggregate".into(), json!({ "members": members.len(), "chunks": chunks.len(), "largest_bytes": chunks.iter().map(|c| c.encode().len()).max(), "ceiling": MAX_MESSAGE_BYTES }));

    // ---- sizes: one Digest's data, postcard against the JSON-shaped MeshDigest; the envelope that
    // carries either is the same CloudEvents metadata, measured beside them ----
    let one = digest(1, true);
    let every = Frame::digest(&one);
    let data_bytes = crate::gossip_wire::data(&every).len();
    let message_bytes = crate::gossip_wire::authored(&every).encode().len();
    let json_bytes = serde_json::to_vec(&one).unwrap().len();
    assert!(data_bytes < json_bytes, "postcard {data_bytes} B against JSON {json_bytes} B");
    result.insert("digest_bytes".into(), json!({ "postcard_data": data_bytes, "complete_message": message_bytes, "json_digest": json_bytes }));

    // ---- a frame this build does not read is refused by name ----
    let garbage = Frame::decode(&[0xff, 0xff, 0xff, 0xff]).unwrap_err().to_string();
    assert!(garbage.contains("postcard decode of 4 bytes as"), "{garbage}");
    let json_frame = Frame::decode(br#"{"frame":"digest","digest":{}}"#).unwrap_err().to_string();
    assert!(json_frame.contains("postcard decode"), "a JSON frame is refused, not read: {json_frame}");
    let mut padded = crate::gossip_wire::authored(&all_frames[5]).encode();
    padded.push(0);
    let trailing = Frame::decode(&padded).unwrap_err().to_string();
    assert!(trailing.contains("1 bytes left after the frame"), "{trailing}");
    result.insert("refusals".into(), json!({ "garbage": garbage, "json_frame": json_frame, "trailing_bytes": trailing }));

    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&Value::Object(result)).unwrap()).unwrap();
}

/// All 19 kinds declare their literal standard type; reserved kinds remain named refusals.
#[test]
fn every_kind_declares_its_ruled_type_string() {
    let cases = frames();
    let expected = ["rdm.mesh.digest", "rdm.mesh.digest", "rdm.mesh.digest", "rdm.mesh.members", "rdm.mesh.members", "rdm.mesh.members.delta", "rdm.node.deleting", "rdm.node.deleted", "rdm.node.restarting", "rdm.node.stopped", "rdm.node.connections.deleting", "rdm.node.leaving", "rdm.node.left", "rdm.node.connections.deleted", "rdm.mesh.status", "rdm.fabric.status", "rdm.mesh.concern", "rdm.fabric.primary.new"];
    assert_eq!(cases.len(), expected.len());
    for (frame, kind) in cases.iter().zip(expected) {
        let bytes = crate::gossip_wire::authored(frame).encode();
        let (attrs, _): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&bytes).unwrap();
        assert_eq!(attrs.iter().find(|(n, _)| n == "type").unwrap().1, kind);
        crate::gossip_wire::assert_roundtrip(frame, &bytes);
        assert!(bytes.len() <= 4032);
    }
    // 19 catalog types; the two reserved since Q171 and the two since Q174 are never sent;
    // rdm.node.restarting, reserved since Q183, is encoded here only to prove its refusal.
    assert_eq!(expected.into_iter().collect::<std::collections::BTreeSet<_>>().len(), 15);
}
