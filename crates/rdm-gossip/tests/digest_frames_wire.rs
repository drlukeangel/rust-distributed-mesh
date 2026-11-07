//! Fixed bytes for a node's Digest and its mesh-primary's projection on the topology channels
//! (gossip.md §3.1: the `Digest` carries the node's status, the primary roles it holds and, on its
//! first publication, its details; the envelope names its birth, sequence and time; `TopologyFull` and
//! `TopologyDelta` carry the mesh's id beside its name and each node with its status). Postcard,
//! positional, append-only. The literals are derived from the postcard rules by hand. Never
//! regenerate an expectation from the Rust serializer to make a schema change pass.

use crate::gossip_wire::{WireGolden, golden};

use rdm_core::{IncarnationId, model::NodeStatus, MeshDigest, MeshId, PrimaryRole, PublisherId, RoleRevision};
use rdm_gossip::frame::Frame;

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

/// mesh1.node.1's digest as JSON carries it (REST, files), with the status `status`.
fn digest(status: &str) -> MeshDigest {
    serde_json::from_str(&format!(
        r#"{{"fabric_id":"1123456789ab","node":{{"mesh":"mesh1","type":"node","node_id":"0123456789ab","path":"mesh1.node","name":"1","endpoint_id":"k","transport_addr":"127.0.0.1:80","incarnation":"yrz49j303yh4","supersedes":null}},"status":"{status}","digest_seq":7,"emitted_at_rafka_ms":9}}"#
    ))
    .unwrap()
}

/// The Digest's body: the status (the record's `NodeStatus` position: `Ready` = 1,
/// `ConnectionsDeleting` = 2), the primary roles (a vector: empty is one `00`), then the details
/// (`Option`: absent `00`; present `01`, then type, path, name, endpoint id, transport address (variant
/// V4, four address bytes, port) and supersedes). Nothing else: the fabric, node id, incarnation,
/// sequence and time are the envelope's.
fn wire_digest_body(status: &str, details: bool) -> String {
    let details = if details { "01 04 6e6f6465 0a 6d657368312e6e6f6465 01 31 01 6b 00 7f000001 50 00" } else { "00" };
    format!("{status} 00 {details}")
}

/// A projection entry's wire shape: fabric id, node (mesh, type (a string), node id, path, name,
/// endpoint id, transport address, incarnation, supersedes), the status, the primary roles, then
/// digest_seq and emitted_at. No runtime, control API, data dir, mesh id, in-flight count, load or
/// gossip counters (Q387, Q394).
fn wire_entry(status: &str) -> String {
    format!("0c 313132333435363738396162 \
    05 6d65736831 04 6e6f6465 0c 303132333435363738396162 0a 6d657368312e6e6f6465 01 31 01 6b 00 7f000001 50 0c 79727a34396a333033796834 00 \
    {status} 00 07 09")
}

/// The mesh id the projection frames name beside the mesh: `Some` (`01`), then the id's twelve characters.
const MESH_ID: &str = "2123456789ab";
const WIRE_MESH_ID: &str = "01 0c 323132333435363738396162";

/// CONTRACT (gossip.md §3.1 frame catalog, Q387, Q392): a `Digest` (variant 0) is the node's status
/// at position 0, the primary roles it holds at position 1 and its details at position 2, present on
/// its first publication and absent on a heartbeat: a connections-deleting Digest and a ready one
/// differ in that one byte. The envelope's `source`, `sequence` and `time` are the birth, its
/// `digest_seq` and its emitted time, and a decoded Digest names its birth from them. Must NOT
/// happen: a Digest body repeating its birth, sequence or time, or carrying runtime, data dir,
/// control API, mesh id, load or gossip counters; a field moved.
#[test]
fn a_digest_carries_its_status_its_primary_roles_and_its_details_on_its_first_publication() {
    for (status, byte) in [("connections-deleting", "02"), ("ready", "01")] {
        let frame = Frame::digest(&digest(status), true);
        let first = format!("00 {}", wire_digest_body(byte, true));
        assert_eq!(frame.golden_encode(), golden(&frame, &bytes(&first)), "a {status} Digest with its details");
    }
    let heartbeat = Frame::digest(&digest("ready"), false);
    let heartbeat_golden = golden(&heartbeat, &bytes("00 01 00 00"));
    assert_eq!(heartbeat.golden_encode(), heartbeat_golden, "a heartbeat: the status, no roles, no details");
    let context = heartbeat.context().expect("a Digest is authored");
    assert_eq!((context.source.fabric.as_str(), context.source.node.as_str(), context.source.incarnation.as_str()), ("1123456789ab", "0123456789ab", "yrz49j303yh4"), "the envelope's source is the exact birth");
    assert_eq!((context.digest_seq(), context.time_ms), (Some(7), 9), "its sequence is the digest_seq and its time the emitted time");

    // A node-type primary at revision 4 that is also the fabric-primary at revision 300: the roles in
    // their wire order (`FabricPrimary` = 1, `NodePrimary` = 2 with its type), each with its revision
    // (300 = varint ac 02).
    let mut primary = digest("ready");
    primary.set_primary(Some(4));
    primary.set_fabric_primary(Some(300));
    let roles = Frame::digest(&primary, false);
    let roles_golden = golden(&roles, &bytes("00 01 02 01 ac02 02 04 6e6f6465 04 00"));
    assert_eq!(roles.golden_encode(), roles_golden, "a Digest with its primary roles");
    let back = Frame::decode(&roles_golden).expect("the Digest decodes");
    let Frame::Digest { status, primary: carried, details } = back.data() else { panic!("not a Digest") };
    assert_eq!((*status, carried.clone(), details.is_none()), (NodeStatus::Ready, vec![RoleRevision { role: PrimaryRole::FabricPrimary, revision: 300 }, RoleRevision { role: PrimaryRole::NodePrimary("node".into()), revision: 4 }], true));
    let held = digest("ready").node;
    let heard = back.digest_of("mesh1", |b| (b.node == held.node_id && b.incarnation == held.incarnation).then(|| held.clone())).expect("a heartbeat of a held birth names it");
    assert_eq!(heard, primary, "the Digest, its envelope and the details held are the node's digest");
    assert!(back.digest_of("mesh1", |_| None).is_none(), "a heartbeat of a birth whose details are not held names nothing");
    let first = Frame::decode(&Frame::digest(&digest("ready"), true).golden_encode()).unwrap();
    assert_eq!(first.digest_of("mesh1", |_| None), Some(digest("ready")), "a first publication names its birth from its details");
}

/// CONTRACT (gossip.md §3.1, lead note 0001): a projection entry, in `TopologyFull` (variant 1) and
/// `TopologyDelta` (variant 2), is the node with its status and primary roles, and decodes to that
/// status; each frame names the mesh's id beside its name (a recreated mesh of the same name has
/// another). Must NOT happen: an entry without its status, an entry carrying runtime, control API,
/// data dir, mesh id, in-flight count, load or gossip counters, or a frame without its mesh's id.
#[test]
fn a_projection_entry_carries_the_nodes_status_and_its_frame_the_meshs_id() {
    let publisher = PublisherId { incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("p")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() };
    let mesh_id = MeshId::parse(MESH_ID).unwrap();
    let members = Frame::TopologyFull {
        mesh: "mesh1".into(),
        mesh_id: Some(mesh_id.clone()),
        publisher: publisher.clone(),
        forwarded_by: None,
        topology_version: 3,
        published_at_rafka_ms: 5,
        snapshot_id: 9,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![digest("ready")],
        in_flight: vec![],
        deleted: vec![],
    };
    let topology_full_hex = format!("01 05 6d65736831 {WIRE_MESH_ID} 0c 79727a64396a333034326266 0c 303132333435363738396162 00 03 05 09 00 01 01 {} 00 00", wire_entry("01"));
    let topology_full_golden = golden(&members, &bytes(&topology_full_hex));
    assert_eq!(members.golden_encode(), topology_full_golden, "TopologyFull: the mesh's id, one projection entry");
    let Frame::TopologyFull { mesh_id: carried, digests, .. } = Frame::decode(&topology_full_golden).expect("the TopologyFull frame decodes").into_data() else { panic!("not TopologyFull") };
    assert_eq!(carried, Some(mesh_id.clone()), "the frame names the mesh's id");
    assert_eq!(digests.iter().map(|d| d.status).collect::<Vec<_>>(), vec![NodeStatus::Ready], "the entry's status is its node's");

    let delta = Frame::TopologyDelta {
        mesh: "mesh1".into(),
        mesh_id: Some(mesh_id.clone()),
        source_publisher: publisher,
        base_version: 2,
        topology_version: 3,
        published_at_rafka_ms: 5,
        changed: vec![digest("ready")],
        removed: vec![],
        in_flight: vec![],
        deleted: vec![],
    };
    let delta_hex = format!("02 05 6d65736831 {WIRE_MESH_ID} 0c 79727a64396a333034326266 0c 303132333435363738396162 02 03 05 01 {} 00 00 00", wire_entry("01"));
    let delta_golden = golden(&delta, &bytes(&delta_hex));
    assert_eq!(delta.golden_encode(), delta_golden, "TopologyDelta: the mesh's id, one changed projection entry");
    let Frame::TopologyDelta { mesh_id: carried, changed, .. } = Frame::decode(&delta_golden).expect("the TopologyDelta frame decodes").into_data() else { panic!("not TopologyDelta") };
    assert_eq!(carried, Some(mesh_id), "the frame names the mesh's id");
    assert_eq!(changed.iter().map(|d| d.status).collect::<Vec<_>>(), vec![NodeStatus::Ready], "the entry's status is its node's");
}

/// The complete gossip message is the standard context list followed by opaque postcard data.
#[test]
fn a_gossip_message_has_standard_context_and_an_opaque_postcard_body() {
    let frame = Frame::digest(&digest("ready"), true);
    let author = rdm_gossip::envelope::Birth { fabric: digest("ready").fabric_id, node: digest("ready").node.node_id, incarnation: digest("ready").node.incarnation };
    let encoded = frame.authored(&author, 0, 9).encode();
    let (attributes, body): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&encoded).expect("CloudEvents binary metadata and opaque data");
    assert_eq!(attributes.iter().find(|(name, _)| name == "specversion").map(|(_, v)| v.as_str()), Some("1.0"));
    assert_eq!(attributes.iter().find(|(name, _)| name == "type").map(|(_, v)| v.as_str()), Some("rdm.mesh.digest"));
    assert!(!body.is_empty());
    assert!(encoded.len() <= rdm_gossip::frame::MAX_FRAME);
}

/// The largest fixed byte golden includes the complete metadata and both vector prefixes.
/// Its long endpoint id deliberately brings the complete message near the transport ceiling.
#[test]
fn the_largest_complete_message_golden_fits_max_frame() {
    let mut d = digest("ready");
    d.node.endpoint_id = rdm_core::EndpointId("x".repeat(3600));
    let frame = Frame::digest(&d, true);
    // The status (`Ready` = 1), no roles, the details through the name; the endpoint id string has a
    // two-byte postcard length (3600 = 0x90 0x1c), then the address and supersedes.
    let mut data = bytes("00 01 00 01 04 6e6f6465 0a 6d657368312e6e6f6465 01 31 901c");
    data.extend(std::iter::repeat_n(b'x', 3600));
    data.extend(bytes("00 7f000001 50 00"));
    let expected = golden(&frame, &data);
    let actual = frame.golden_encode();
    assert_eq!(actual, expected, "the complete near-ceiling golden is independent of the serializer");
    assert!(expected.len() <= rdm_gossip::frame::MAX_FRAME);
    assert!(expected.len() > 3900);
    crate::gossip_wire::assert_roundtrip(&frame, &expected);
    println!("largest complete message golden: {} B; MAX_FRAME {} B", expected.len(), rdm_gossip::frame::MAX_FRAME);
}

/// A body that fits by itself can overflow once its metadata and both prefixes are counted.
#[test]
fn members_chunks_are_sized_against_the_complete_message() {
    let mut first = digest("ready"); first.node.endpoint_id = rdm_core::EndpointId("x".repeat(1800));
    let mut second = first.clone(); second.node.node_id = rdm_core::NodeId::parse("1123456789ab").unwrap(); second.node.path = "mesh1.node".into(); second.node.name = "2".into();
    let author = crate::gossip_wire::author();
    let publisher = PublisherId { incarnation: author.incarnation.clone(), node_id: author.node.clone() };
    let shape = |digests, in_flight, deleted, chunk_index, chunk_count| Frame::TopologyFull { mesh: "mesh1".into(), mesh_id: None, publisher: publisher.clone(), forwarded_by: None, topology_version: 1, published_at_rafka_ms: 1, snapshot_id: 1, chunk_index, chunk_count, digests, in_flight, deleted }.authored(&author, 0, 1);
    let complete = shape(vec![first.clone(), second.clone()], vec![], vec![], 0, 1).encode();
    let (_, body): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&complete).unwrap();
    assert!(body.len() < 4032);
    assert!(complete.len() > 4032, "metadata makes this otherwise fitting body too large");
    let chunks = rdm_gossip::snapshot::chunks_of(&rdm_gossip::snapshot::Full::new(vec![first, second], vec![], vec![]), shape);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks.iter().map(|f| match f.data() { Frame::TopologyFull { digests, .. } => digests.len(), _ => unreachable!() }).sum::<usize>(), 2);
    assert!(chunks.iter().all(|f| f.encode().len() <= 4032));
}

/// Optional tracing pairs move the complete envelope, never its opaque data or identity.
/// Hand prefix: ten pairs, traceparent name length 11, value length 55; tracestate name 10.
#[test]
fn the_traced_envelope_wire_matches_the_hand_golden_and_complete_ceiling() {
    const PARENT: &str = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
    const STATE: &str = "rojo=00f067aa0ba902b7";
    let mut d = digest("ready");
    d.node.endpoint_id = rdm_core::EndpointId("x".repeat(3500));
    let frame = Frame::digest(&d, true);
    // 3500 = postcard unsigned varint 0xac 0x1b. All other domain positions stand.
    let mut data = bytes("00 01 00 01 04 6e6f6465 0a 6d657368312e6e6f6465 01 31 ac1b");
    data.extend(std::iter::repeat_n(b'x', 3500));
    data.extend(bytes("00 7f000001 50 00"));
    let expected = crate::gossip_wire::golden_with_trace(&frame, &data, Some(PARENT), Some(STATE));
    let mut traced = crate::gossip_wire::authored(&frame);
    let Frame::Authored { context, .. } = &mut traced else { unreachable!() };
    context.traceparent = Some(PARENT.into()); context.tracestate = Some(STATE.into());
    assert_eq!(traced.encode(), expected);
    assert_eq!(expected[0], 10, "the complete metadata count is written by hand");
    assert!(expected.len() <= rdm_gossip::frame::MAX_FRAME);
    assert!(expected.len() > 3900);
    assert_eq!(Frame::decode(&expected).unwrap().encode(), expected);
    println!("traced complete message golden: {} B / {} B", expected.len(), rdm_gossip::frame::MAX_FRAME);
}

/// The full permitted trace state rides every chunk; packing counts its exact metadata bytes.
#[test]
fn members_chunks_include_the_maximum_trace_context_in_their_complete_size() {
    let author = crate::gossip_wire::author();
    let publisher = PublisherId { incarnation: author.incarnation.clone(), node_id: author.node.clone() };
    let state = format!("a={},b={}", "x".repeat(256), "y".repeat(251));
    assert_eq!(state.len(), rdm_core::context::MAX_TRACESTATE_BYTES);
    let mut d = digest("ready"); d.node.endpoint_id = rdm_core::EndpointId("x".repeat(1600));
    let mut other = d.clone(); other.node.node_id = rdm_core::NodeId::mint(); other.node.path = "mesh1.node".into(); other.node.name = "2".into();
    let shape = |digests, in_flight, deleted, chunk_index, chunk_count| {
        let mut f = Frame::TopologyFull { mesh: "mesh1".into(), mesh_id: None, publisher: publisher.clone(), forwarded_by: None, topology_version: 1, published_at_rafka_ms: 1, snapshot_id: 1, chunk_index, chunk_count, digests, in_flight, deleted: deleted }.authored(&author, 0, 1);
        let Frame::Authored { context, .. } = &mut f else { unreachable!() };
        context.traceparent = Some("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".into());
        context.tracestate = Some(state.clone()); f
    };
    let full = rdm_gossip::snapshot::Full::new(vec![d, other], vec![], vec![]);
    let oversized = shape(full.digests(), vec![], vec![], 0, 1).encode();
    assert!(oversized.len() > rdm_gossip::frame::MAX_FRAME);
    let chunks = rdm_gossip::snapshot::chunks_of(&full, shape);
    assert_eq!(chunks.len(), 2);
    assert!(chunks.iter().all(|f| f.encode().len() <= rdm_gossip::frame::MAX_FRAME));
    assert!(chunks.iter().all(|f| f.context().unwrap().tracestate.as_ref() == Some(&state)));
}
