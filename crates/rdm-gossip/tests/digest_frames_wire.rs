//! Fixed bytes for a node's Digest and its mesh-primary's projection on the topology channels
//! (gossip.md §3.1: every `Digest` carries the node's status, the primary roles it holds and its
//! details; the envelope names its birth, sequence and time; `TopologyFull` and
//! `TopologyDelta` carry the mesh's id beside its name and each node with its status). Postcard,
//! positional, append-only. The literals are derived from the postcard rules by hand. Never
//! regenerate an expectation from the Rust serializer to make a schema change pass.

use crate::gossip_wire::{WireGolden, golden};

use rdm_core::{IncarnationId, model::NodeStatus, MeshDigest, MeshId, PrimaryRole, PublisherId, RoleRevision};
use rdm_core::wire::NodeEntry;
use rdm_gossip::envelope::Birth;
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

/// mesh1.node.1's details on the wire: type, path, name, endpoint id, transport address (variant V4,
/// four address bytes, port) and supersedes (`Option`: absent `00`).
const WIRE_DETAILS: &str = "04 6e6f6465 0a 6d657368312e6e6f6465 01 31 01 6b 00 7f000001 50 00";

/// The Digest's body: the status (the record's `NodeStatus` position: `Ready` = 1,
/// `ConnectionsDeleting` = 2), the primary roles (a vector: empty is one `00`), then the details,
/// every time. Nothing else: the fabric, node id, incarnation, sequence and time are the envelope's.
fn wire_digest_body(status: &str) -> String {
    format!("{status} 00 {WIRE_DETAILS}")
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

/// An aggregate's node entry (`NodeEntry`, Q390): node id, incarnation, type (a string), path, name,
/// endpoint id, transport address (variant V4, four address bytes, port), the status, then the
/// primary roles. Nothing else: no fabric id, mesh, lineage, digest sequence or emitted time (the
/// envelope and the aggregate name the fabric, the mesh and the version), no runtime, control API,
/// data dir, load or gossip counters.
fn wire_node(status: &str, primary: &str) -> String {
    format!("0c 303132333435363738396162 0c 79727a34396a333033796834 04 6e6f6465 0a 6d657368312e6e6f6465 01 31 01 6b 00 7f000001 50 {status} {primary}")
}

/// The mesh id the projection frames name beside the mesh: `Some` (`01`), then the id's twelve characters.
const MESH_ID: &str = "2123456789ab";
const WIRE_MESH_ID: &str = "01 0c 323132333435363738396162";

/// CONTRACT (gossip.md §3.1 frame catalog, Q387, Q392, Q405 A): a `Digest` (variant 0) is the node's
/// status at position 0, the primary roles it holds at position 1 and its details at position 2, on
/// every Digest: a connections-deleting Digest and a ready one differ in their status byte alone. The
/// envelope's `source`, `sequence` and `time` are the birth, its `digest_seq` and its emitted time, and
/// a decoded Digest names its birth from them and its details, whatever the receiver holds. Must NOT
/// happen: a Digest without its details, a Digest body repeating its birth, sequence or time, or
/// carrying runtime, data dir, control API, mesh id, load or gossip counters; a field moved.
#[test]
fn every_digest_carries_its_status_its_primary_roles_and_its_details() {
    for (status, byte) in [("connections-deleting", "02"), ("ready", "01")] {
        let frame = Frame::digest(&digest(status));
        let body = format!("00 {}", wire_digest_body(byte));
        assert_eq!(frame.golden_encode(), golden(&frame, &bytes(&body)), "a {status} Digest with its details");
    }
    let heartbeat = Frame::digest(&digest("ready"));
    let context = heartbeat.context().expect("a Digest is authored");
    assert_eq!((context.source.fabric.as_str(), context.source.node.as_str(), context.source.incarnation.as_str()), ("1123456789ab", "0123456789ab", "yrz49j303yh4"), "the envelope's source is the exact birth");
    assert_eq!((context.digest_seq(), context.time_ms), (Some(7), 9), "its sequence is the digest_seq and its time the emitted time");

    // A node-type primary at revision 4 that is also the fabric-primary at revision 300: the roles in
    // their wire order (`FabricPrimary` = 1, `NodePrimary` = 2 with its type), each with its revision
    // (300 = varint ac 02), then the details.
    let mut primary = digest("ready");
    primary.set_primary(Some(4));
    primary.set_fabric_primary(Some(300));
    let roles = Frame::digest(&primary);
    let roles_golden = golden(&roles, &bytes(&format!("00 01 02 01 ac02 02 04 6e6f6465 04 {WIRE_DETAILS}")));
    assert_eq!(roles.golden_encode(), roles_golden, "a Digest with its primary roles and its details");
    let back = Frame::decode(&roles_golden).expect("the Digest decodes");
    let Frame::Digest { status, primary: carried, details } = back.data() else { panic!("not a Digest") };
    assert_eq!((*status, carried.clone(), details.clone()), (NodeStatus::Ready, vec![RoleRevision { role: PrimaryRole::FabricPrimary, revision: 300 }, RoleRevision { role: PrimaryRole::NodePrimary("node".into()), revision: 4 }], rdm_core::wire::NodeDetails::of(&digest("ready").node)));
    assert_eq!(back.digest_of("mesh1"), Some(primary), "the Digest and its envelope are the node's digest");
}

/// CONTRACT (gossip.md §3.1 `TopologyFull`, Q387, Q389, Q390): the aggregate (variant 1) is the mesh,
/// the mesh's id, the chunk's index and count, then its one list of nodes, each with its identity,
/// endpoints, status and primary roles. Its author, version and time are the envelope's alone: `source`
/// the mesh-primary's exact birth, `sequence` the mesh's version, `time` the publication's, `id`
/// `rdm.mesh.members/<mesh>/<version>/<chunk>`, `subject` `mesh/<mesh>`. It decodes to the same nodes
/// and context. Must NOT happen: a body repeating its publisher, forwarder, version, time or a
/// publication counter; an `in_flight` or `deleted` list; an entry carrying a fabric id, lineage,
/// digest sequence, emitted time, runtime, control API, data dir, load or gossip counters.
#[test]
fn an_aggregate_is_its_mesh_its_chunk_and_one_list_of_nodes_under_its_authors_envelope() {
    let author = Birth::publisher(&digest("ready").fabric_id, &PublisherId { incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("p")).unwrap(), node_id: rdm_core::NodeId::parse("2123456789ab").unwrap() });
    let mesh_id = MeshId::parse(MESH_ID).unwrap();
    let mut node = digest("ready");
    node.set_primary(Some(4));
    let members = Frame::aggregate("mesh1", Some(mesh_id.clone()), &author, 3, 5, 0, 1, vec![node.clone()]);
    // One node, ready (`01`), holding the node-type primary role of `node` at revision 4 (`01 02 04
    // 6e6f6465 04`: one role, `NodePrimary` = 2, its type, the revision).
    let hex = format!("01 05 6d65736831 {WIRE_MESH_ID} 00 01 01 {}", wire_node("01", "01 02 04 6e6f6465 04"));
    let topology_full_golden = golden(&members, &bytes(&hex));
    assert_eq!(members.golden_encode(), topology_full_golden, "TopologyFull: the mesh, its id, the chunk, one node");
    let back = Frame::decode(&topology_full_golden).expect("the TopologyFull frame decodes");
    let context = back.context().expect("a decoded aggregate carries its envelope");
    assert_eq!(context.source, author, "the envelope's source is the mesh-primary that authored it");
    assert_eq!((context.version(), context.sequence.as_deref(), context.time_ms), (Some(3), Some("00000000000000000003"), 5), "its sequence is the mesh's version, its time the publication's");
    assert_eq!((context.id.as_str(), context.subject.as_str(), context.kind.as_str()), ("rdm.mesh.members/mesh1/3/0", "mesh/mesh1", "rdm.mesh.members"));
    assert_eq!(back.aggregate_version(), Some(3));
    let Frame::TopologyFull { mesh, mesh_id: carried, chunk_index, chunk_count, nodes } = back.into_data() else { panic!("not TopologyFull") };
    assert_eq!((mesh.as_str(), carried, chunk_index, chunk_count), ("mesh1", Some(mesh_id.clone()), 0, 1), "the frame names the mesh, its id and the chunk");
    assert_eq!(nodes, vec![NodeEntry::of(&node)], "the one node, as listed");
    assert_eq!(nodes[0].primary, vec![RoleRevision { role: PrimaryRole::NodePrimary("node".into()), revision: 4 }], "the entry carries the primary roles the node holds");
    assert_eq!(nodes[0].digest(&node.fabric_id, "mesh1"), MeshDigest { digest_seq: 0, emitted_at_rafka_ms: 0, ..node.clone() }, "a receiver holds the entry as the node, under the aggregate's fabric and mesh");
    // A stopped, a deleting and a deleted node are each one entry with its status (Q390): `Stopped` (12),
    // `Deleting` (13) and `Deleted` (7) at their record positions, never a list of operations beside the nodes.
    for (status, byte) in [(NodeStatus::Stopped, "0c"), (NodeStatus::Deleting, "0d"), (NodeStatus::Deleted, "07")] {
        let listed = Frame::aggregate("mesh1", Some(mesh_id.clone()), &author, 4, 6, 0, 1, vec![MeshDigest { status, ..digest("ready") }]);
        let golden_listed = golden(&listed, &bytes(&format!("01 05 6d65736831 {WIRE_MESH_ID} 00 01 01 {}", wire_node(byte, "00"))));
        assert_eq!(listed.golden_encode(), golden_listed, "a {status:?} node is one entry with its status");
        let Frame::TopologyFull { nodes, .. } = Frame::decode(&golden_listed).unwrap().into_data() else { panic!("not TopologyFull") };
        assert_eq!(nodes.iter().map(|n| n.status).collect::<Vec<_>>(), vec![status]);
    }
    // A body that names a version the envelope does not is refused: the envelope alone carries it.
    let Frame::Authored { mut context, frame } = members.clone() else { unreachable!() };
    context.sequence = Some("00000000000000000004".into());
    let tampered = Frame::Authored { context, frame };
    assert!(Frame::decode(&tampered.encode()).is_err(), "an envelope whose sequence disagrees with its id is refused");
}

/// CONTRACT (gossip.md §3.1, lead note 0001): a projection entry in `TopologyDelta` (variant 2) is the
/// node with its status and primary roles, and decodes to that status; the frame names the mesh's id
/// beside its name. Must NOT happen: an entry without its status, an entry carrying runtime, control API,
/// data dir, mesh id, in-flight count, load or gossip counters, or a frame without its mesh's id.
#[test]
fn a_projection_entry_carries_the_nodes_status_and_its_frame_the_meshs_id() {
    let publisher = PublisherId { incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("p")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() };
    let mesh_id = MeshId::parse(MESH_ID).unwrap();
    // The differential (Q387, Q389): the mesh, its id, the base version, the chunk's index and count,
    // then `added` (each node entry: node id, incarnation, type, path, name, endpoint id, transport
    // address, status, primary roles), `changed`, `removed`, `in_flight` and `deleted`. The forwarder
    // is the envelope's `source` and the peer mesh's version its `sequence`; the body repeats neither.
    let _ = publisher;
    let delta = Frame::TopologyDelta {
        mesh: "mesh1".into(),
        mesh_id: Some(mesh_id.clone()),
        base_version: 2,
        chunk_index: 0,
        chunk_count: 1,
        added: vec![rdm_core::wire::NodeEntry::of(&digest("ready"))],
        changed: vec![],
        removed: vec![],
        in_flight: vec![],
        deleted: vec![],
    }
    .authored_delta(&crate::gossip_wire::author(), 3, 5);
    let delta_hex = format!("02 05 6d65736831 {WIRE_MESH_ID} 02 00 01 01 0c 303132333435363738396162 0c 79727a34396a333033796834 04 6e6f6465 0a 6d657368312e6e6f6465 01 31 01 6b 00 7f000001 50 01 00 00 00 00 00");
    let delta_golden = golden(&delta, &bytes(&delta_hex));
    assert_eq!(delta.golden_encode(), delta_golden, "TopologyDelta: the mesh's id, the base, one chunk of one, one added node entry");
    let back = Frame::decode(&delta_golden).expect("the TopologyDelta frame decodes");
    assert_eq!((back.delta_version(), back.context().map(|c| c.source.clone())), (Some(3), Some(crate::gossip_wire::author())), "the version and the forwarder are the envelope's");
    let Frame::TopologyDelta { mesh_id: carried, added, .. } = back.into_data() else { panic!("not TopologyDelta") };
    assert_eq!(carried, Some(mesh_id), "the frame names the mesh's id");
    assert_eq!(added.iter().map(|e| e.status).collect::<Vec<_>>(), vec![NodeStatus::Ready], "the entry's status is its node's");
}

/// The complete gossip message is the standard context list followed by opaque postcard data.
#[test]
fn a_gossip_message_has_standard_context_and_an_opaque_postcard_body() {
    let frame = Frame::digest(&digest("ready"));
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
    let frame = Frame::digest(&d);
    // The status (`Ready` = 1), no roles, the details through the name; the endpoint id string has a
    // two-byte postcard length (3600 = 0x90 0x1c), then the address and supersedes.
    let mut data = bytes("00 01 00 04 6e6f6465 0a 6d657368312e6e6f6465 01 31 901c");
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
    let mut first = digest("ready"); first.node.endpoint_id = rdm_core::EndpointId("x".repeat(1900));
    let mut second = first.clone(); second.node.node_id = rdm_core::NodeId::parse("1123456789ab").unwrap(); second.node.path = "mesh1.node".into(); second.node.name = "2".into();
    let author = crate::gossip_wire::author();
    let shape = |nodes, chunk_index, chunk_count| Frame::aggregate("mesh1", None, &author, 1, 1, chunk_index, chunk_count, nodes);
    let complete = shape(vec![first.clone(), second.clone()], 0, 1).encode();
    let (_, body): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&complete).unwrap();
    assert!(body.len() < 4032);
    assert!(complete.len() > 4032, "metadata makes this otherwise fitting body too large");
    let chunks = rdm_gossip::snapshot::chunks_of(&rdm_gossip::snapshot::Full::new(vec![first, second], vec![], vec![]), shape);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks.iter().map(|f| match f.data() { Frame::TopologyFull { nodes, .. } => nodes.len(), _ => unreachable!() }).sum::<usize>(), 2);
    // Chunk k of n: the envelope id names it, every chunk under the one author and version.
    let ids: Vec<String> = chunks.iter().map(|f| f.context().unwrap().id.clone()).collect();
    assert_eq!(ids, vec!["rdm.mesh.members/mesh1/1/0".to_string(), "rdm.mesh.members/mesh1/1/1".to_string()]);
    assert!(chunks.iter().all(|f| f.context().unwrap().source == author && matches!(f.data(), Frame::TopologyFull { chunk_count: 2, .. })));
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
    let frame = Frame::digest(&d);
    // 3500 = postcard unsigned varint 0xac 0x1b. All other domain positions stand.
    let mut data = bytes("00 01 00 04 6e6f6465 0a 6d657368312e6e6f6465 01 31 ac1b");
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
    let state = format!("a={},b={}", "x".repeat(256), "y".repeat(251));
    assert_eq!(state.len(), rdm_core::context::MAX_TRACESTATE_BYTES);
    let mut d = digest("ready"); d.node.endpoint_id = rdm_core::EndpointId("x".repeat(1600));
    let mut other = d.clone(); other.node.node_id = rdm_core::NodeId::mint(); other.node.path = "mesh1.node".into(); other.node.name = "2".into();
    let shape = |nodes, chunk_index, chunk_count| {
        let mut f = Frame::aggregate("mesh1", None, &author, 1, 1, chunk_index, chunk_count, nodes);
        let Frame::Authored { context, .. } = &mut f else { unreachable!() };
        context.traceparent = Some("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".into());
        context.tracestate = Some(state.clone()); f
    };
    let full = rdm_gossip::snapshot::Full::new(vec![d, other], vec![], vec![]);
    let oversized = shape(full.digests(), 0, 1).encode();
    assert!(oversized.len() > rdm_gossip::frame::MAX_FRAME);
    let chunks = rdm_gossip::snapshot::chunks_of(&full, shape);
    assert_eq!(chunks.len(), 2);
    assert!(chunks.iter().all(|f| f.encode().len() <= rdm_gossip::frame::MAX_FRAME));
    assert!(chunks.iter().all(|f| f.context().unwrap().tracestate.as_ref() == Some(&state)));
}

/// `n` characters of the path grammar's segments, each 64 characters, dot-joined: `None` when the
/// `n`th character would be a dot (no path ends in one).
fn path_of(n: usize) -> Option<String> {
    let path: String = std::iter::repeat_n(format!("{}.", "p".repeat(64)), n / 65 + 1).collect::<String>().chars().take(n).collect();
    (!path.ends_with('.')).then_some(path)
}

/// The largest details a Digest carries, at a path of `path`: a 64-character type (its grammar's
/// bound, `status::type_refusal`), a 64-character name (`PathName`'s), a 64-character endpoint id, a V6
/// transport address, a superseded incarnation, both primary roles at the largest revision, the
/// largest digest sequence and a time in the last year of four digits; and the largest trace context
/// its envelope carries (a traceparent and a tracestate of `MAX_TRACESTATE_BYTES`).
fn largest(path: &str) -> Frame {
    let mut d = digest("connections-deleting");
    d.node.r#type = "t".repeat(64);
    d.node.path = path.to_string();
    d.node.name = "n".repeat(64);
    d.node.endpoint_id = rdm_core::EndpointId("f".repeat(64));
    d.node.transport_addr = "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535".parse().unwrap();
    d.node.supersedes = Some(IncarnationId::parse(&rdm_core::fixture_crockford60("s")).unwrap());
    d.set_primary(Some(u64::MAX));
    d.set_fabric_primary(Some(u64::MAX));
    (d.digest_seq, d.emitted_at_rafka_ms) = (u64::MAX, 253_402_300_799_999);
    let mut frame = Frame::digest(&d);
    let Frame::Authored { context, .. } = &mut frame else { panic!("a Digest is authored") };
    context.traceparent = Some(format!("00-{}-{}-01", "a".repeat(32), "b".repeat(16)));
    let state = format!("a={},b={}", "v".repeat(254), "w".repeat(253));
    assert_eq!(state.len(), rdm_core::context::MAX_TRACESTATE_BYTES);
    context.tracestate = Some(state);
    frame
}

/// CONTRACT (gossip.md §3.1 `Digest`; lead 0007 item 5): a Digest carrying the largest details, at a
/// path of four 64-character segments, and the largest trace context its envelope carries, is one
/// gossip message: its complete encoding is within `MAX_FRAME` and decodes to the same details. The
/// cell prints the longest path at which that Digest still fits. What must NOT happen: such a Digest
/// split or refused for its size. The ceiling the cell names: `PathName` bounds each segment, never
/// the number of segments, so node.create admits a path whose Digest can never be sent.
#[test]
fn a_digest_carrying_the_largest_details_fits_one_gossip_message() {
    use rdm_gossip::frame::MAX_FRAME;
    let path = path_of(4 * 64 + 3).expect("four segments");
    assert_eq!(path.split('.').map(str::len).collect::<Vec<_>>(), vec![64; 4]);
    let frame = largest(&path);
    let bytes = frame.encode();
    assert!(bytes.len() <= MAX_FRAME, "the largest Digest at a four-segment path is {} bytes, over MAX_FRAME {MAX_FRAME}", bytes.len());
    let Frame::Digest { details: back, primary, .. } = Frame::decode(&bytes).expect("the Digest decodes").into_data() else { panic!("not a Digest") };
    let Frame::Digest { details: sent, .. } = frame.data() else { unreachable!() };
    assert_eq!((&back, primary.len()), (sent, 2), "the details and both roles travel whole");
    let longest = (path.len()..MAX_FRAME).filter_map(path_of).take_while(|p| largest(p).encode().len() <= MAX_FRAME).last().map_or(0, |p| p.len());
    eprintln!("the largest Digest at a four-segment path ({} characters) is {} of {MAX_FRAME} bytes; the longest path at which it still fits is {longest} characters", path.len(), bytes.len());
}
