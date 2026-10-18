//! Fixed bytes for a member's heartbeat and its mesh-primary's member projection on the membership
//! channels (gossip.md §3.1: the `Digest` carries "`node_state`" and "no status: a member's status
//! comes only from its mesh-primary"; `Members` and `MembersDelta` carry the mesh-primary's own-mesh
//! member projection, each entry its digest and its status). Postcard, positional, append-only. The
//! literals are derived from the postcard rules by hand. Never regenerate an expectation from the Rust
//! serializer to make a schema change pass.

use crate::gossip_wire::{WireGolden, golden};

use rdm_core::{IncarnationId, MemberStatus, MeshDigest, PublisherId};
use rdm_transport::membership::Frame;

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

/// mesh1.rpc.1's digest as JSON carries it (REST, files): no liveness field, so `node_state` takes its
/// default (`Joining`), and the status `status`.
fn digest(status: &str) -> MeshDigest {
    serde_json::from_str(&format!(
        r#"{{"fabric_id":"1123456789ab","node":{{"node_id":"0123456789ab","name":"mesh1.rpc.1","endpoint_id":"k","transport_addr":"127.0.0.1:80","incarnation":"yrz49j303yh4","supersedes":null}},"status":"{status}","admin_api_base":null,"digest_seq":7,"emitted_at_rafka_ms":9}}"#
    ))
    .unwrap()
}

/// The digest's wire shape: fabric id, node (node id, path.name, endpoint id, transport address
/// (variant V4, four address bytes, port), incarnation, supersedes, runtime), then `node_state` at
/// position 2 (`Joining` = 0), admin API base, digest_seq, emitted_at, data dir, mesh id, in flight,
/// config, load, gossip.
const WIRE_DIGEST: &str = "0c 313132333435363738396162 \
    0c 303132333435363738396162 0b 6d657368312e7270632e31 01 6b 00 7f000001 50 0c 79727a34396a333033796834 00 00 \
    00 \
    00 07 09 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00";

/// CONTRACT (gossip.md §3.1 frame catalog): a heartbeat (`Digest`, variant 0) is the digest's wire
/// shape with the liveness `node_state` at position 2 and no status: a ConnectionsDeleting digest and a
/// ready-for-traffic one encode alike. A member projection entry, in `Members` (variant 1) and
/// `MembersDelta` (variant 2), is the digest's wire shape then its status (`ReadyForTraffic` = 1), and
/// decodes to that status. What must NOT happen: a status on the heartbeat, a projection entry without
/// its status, or a field moved.
#[test]
fn a_heartbeat_carries_no_status_and_a_projection_entry_carries_its_digest_then_its_status() {
    let heartbeat = format!("00 {WIRE_DIGEST}");
    for status in ["connections-deleting", "ready-for-traffic"] {
        let frame = Frame::Digest { digest: digest(status) };
        assert_eq!(frame.golden_encode(), golden(&frame, &bytes(&heartbeat)), "a {status} heartbeat");
    }

    let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("p")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() };
    let members = Frame::Members {
        mesh: "mesh1".into(),
        publisher: publisher.clone(),
        forwarded_by: None,
        topology_version: 3,
        published_at_rafka_ms: 5,
        snapshot_id: 9,
        chunk_index: 0,
        chunk_count: 1,
        digests: vec![digest("ready-for-traffic")],
        in_flight: vec![],
        departed: vec![],
    };
    let members_hex = format!("01 05 6d65736831 0d 6d657368312e61646d696e2e31 0c 79727a64396a333034326266 0c 303132333435363738396162 00 03 05 09 00 01 01 {WIRE_DIGEST} 01 00 00");
    let members_golden = golden(&members, &bytes(&members_hex));
    assert_eq!(members.golden_encode(), members_golden, "Members: one projection entry, digest then status");
    let Frame::Members { digests, .. } = Frame::decode(&members_golden).expect("the Members frame decodes").into_data() else { panic!("not Members") };
    assert_eq!(digests.iter().map(|d| d.status).collect::<Vec<_>>(), vec![MemberStatus::ReadyForTraffic], "the entry's status is the projection's");

    let delta = Frame::MembersDelta {
        mesh: "mesh1".into(),
        source_publisher: publisher,
        base_version: 2,
        topology_version: 3,
        published_at_rafka_ms: 5,
        changed: vec![digest("ready-for-traffic")],
        removed: vec![],
        in_flight: vec![],
        departed: vec![],
    };
    let delta_hex = format!("02 05 6d65736831 0d 6d657368312e61646d696e2e31 0c 79727a64396a333034326266 0c 303132333435363738396162 02 03 05 01 {WIRE_DIGEST} 01 00 00 00");
    let delta_golden = golden(&delta, &bytes(&delta_hex));
    assert_eq!(delta.golden_encode(), delta_golden, "MembersDelta: one changed projection entry, digest then status");
    let Frame::MembersDelta { changed, .. } = Frame::decode(&delta_golden).expect("the MembersDelta frame decodes").into_data() else { panic!("not MembersDelta") };
    assert_eq!(changed.iter().map(|d| d.status).collect::<Vec<_>>(), vec![MemberStatus::ReadyForTraffic], "the entry's status is the projection's");
}

/// The complete gossip message is the standard context list followed by opaque postcard data.
#[test]
fn a_gossip_message_has_standard_context_and_an_opaque_postcard_body() {
    let frame = Frame::Digest { digest: digest("ready-for-traffic") };
    let author = rdm_transport::envelope::Birth { fabric: digest("ready-for-traffic").fabric_id, node: digest("ready-for-traffic").node.node_id, incarnation: digest("ready-for-traffic").node.incarnation };
    let encoded = frame.authored(&author, 0, 9).encode();
    let (attributes, body): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&encoded).expect("CloudEvents binary metadata and opaque data");
    assert_eq!(attributes.iter().find(|(name, _)| name == "specversion").map(|(_, v)| v.as_str()), Some("1.0"));
    assert_eq!(attributes.iter().find(|(name, _)| name == "type").map(|(_, v)| v.as_str()), Some("rdm.mesh.digest"));
    assert!(!body.is_empty());
    assert!(encoded.len() <= rdm_transport::membership::MAX_FRAME);
}

/// The largest fixed byte golden includes the complete metadata and both vector prefixes.
/// Its long API address deliberately brings the complete message near the transport ceiling.
#[test]
fn the_largest_complete_message_golden_fits_max_frame() {
    let mut d = digest("ready-for-traffic");
    d.admin_api_base = Some("x".repeat(3600));
    let frame = Frame::Digest { digest: d };
    // The existing literal digest prefix through node_state; Some API string has a two-byte
    // postcard length (3600 = 0x90 0x1c), then sequence, stamp and all remaining empty fields.
    let mut data = bytes("00 0c313132333435363738396162 0c303132333435363738396162 0b6d657368312e7270632e31 016b 007f00000150 0c79727a34396a333033796834 0000 00 01 901c");
    data.extend(std::iter::repeat_n(b'x', 3600));
    data.extend(bytes("07 09 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00"));
    let expected = golden(&frame, &data);
    let actual = frame.golden_encode();
    assert_eq!(actual, expected, "the complete near-ceiling golden is independent of the serializer");
    assert!(expected.len() <= rdm_transport::membership::MAX_FRAME);
    assert!(expected.len() > 3900);
    crate::gossip_wire::assert_roundtrip(&frame, &expected);
    println!("largest complete message golden: {} B; MAX_FRAME {} B", expected.len(), rdm_transport::membership::MAX_FRAME);
}

/// A body that fits by itself can overflow once its metadata and both prefixes are counted.
#[test]
fn members_chunks_are_sized_against_the_complete_message() {
    let mut first = digest("ready-for-traffic"); first.admin_api_base = Some("x".repeat(1800));
    let mut second = first.clone(); second.node.node_id = rdm_core::NodeId::parse("1123456789ab").unwrap(); second.node.name = "mesh1.rpc.2".parse().unwrap();
    let author = crate::gossip_wire::author();
    let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: author.incarnation.clone(), node_id: author.node.clone() };
    let shape = |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members { mesh: "mesh1".into(), publisher: publisher.clone(), forwarded_by: None, topology_version: 1, published_at_rafka_ms: 1, snapshot_id: 1, chunk_index, chunk_count, digests, in_flight, departed }.authored(&author, 0, 1);
    let complete = shape(vec![first.clone(), second.clone()], vec![], vec![], 0, 1).encode();
    let (_, body): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&complete).unwrap();
    assert!(body.len() < 4032);
    assert!(complete.len() > 4032, "metadata makes this otherwise fitting body too large");
    let chunks = rdm_transport::snapshot::chunks_of(&rdm_transport::snapshot::Full::new(vec![first, second], vec![], vec![]), shape);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks.iter().map(|f| match f.data() { Frame::Members { digests, .. } => digests.len(), _ => unreachable!() }).sum::<usize>(), 2);
    assert!(chunks.iter().all(|f| f.encode().len() <= 4032));
}

/// Optional tracing pairs move the complete envelope, never its opaque data or identity.
/// Hand prefix: ten pairs, traceparent name length 11, value length 55; tracestate name 10.
#[test]
fn the_traced_envelope_reseal_matches_the_hand_golden_and_complete_ceiling() {
    const PARENT: &str = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
    const STATE: &str = "rojo=00f067aa0ba902b7";
    let mut d = digest("ready-for-traffic");
    d.admin_api_base = Some("x".repeat(3500));
    let frame = Frame::Digest { digest: d };
    // 3500 = postcard unsigned varint 0xac 0x1b. All other domain positions stand.
    let mut data = bytes("00 0c313132333435363738396162 0c303132333435363738396162 0b6d657368312e7270632e31 016b 007f00000150 0c79727a34396a333033796834 0000 00 01 ac1b");
    data.extend(std::iter::repeat_n(b'x', 3500));
    data.extend(bytes("07 09 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00"));
    let expected = crate::gossip_wire::golden_with_trace(&frame, &data, Some(PARENT), Some(STATE));
    let mut traced = crate::gossip_wire::authored(&frame);
    let Frame::Authored { context, .. } = &mut traced else { unreachable!() };
    context.traceparent = Some(PARENT.into()); context.tracestate = Some(STATE.into());
    assert_eq!(traced.encode(), expected);
    assert_eq!(expected[0], 10, "the complete metadata count is hand-sealed");
    assert!(expected.len() <= rdm_transport::membership::MAX_FRAME);
    assert!(expected.len() > 3900);
    assert_eq!(Frame::decode(&expected).unwrap().encode(), expected);
    println!("traced complete message golden: {} B / {} B", expected.len(), rdm_transport::membership::MAX_FRAME);
}

/// The full permitted trace state rides every chunk; packing counts its exact metadata bytes.
#[test]
fn members_chunks_include_the_maximum_trace_context_in_their_complete_size() {
    let author = crate::gossip_wire::author();
    let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: author.incarnation.clone(), node_id: author.node.clone() };
    let state = format!("a={},b={}", "x".repeat(256), "y".repeat(251));
    assert_eq!(state.len(), rdm_core::context::MAX_TRACESTATE_BYTES);
    let mut d = digest("ready-for-traffic"); d.admin_api_base = Some("x".repeat(1600));
    let mut other = d.clone(); other.node.node_id = rdm_core::NodeId::mint(); other.node.name = "mesh1.rpc.2".parse().unwrap();
    let shape = |digests, in_flight, departed, chunk_index, chunk_count| {
        let mut f = Frame::Members { mesh: "mesh1".into(), publisher: publisher.clone(), forwarded_by: None, topology_version: 1, published_at_rafka_ms: 1, snapshot_id: 1, chunk_index, chunk_count, digests, in_flight, departed }.authored(&author, 0, 1);
        let Frame::Authored { context, .. } = &mut f else { unreachable!() };
        context.traceparent = Some("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".into());
        context.tracestate = Some(state.clone()); f
    };
    let full = rdm_transport::snapshot::Full::new(vec![d, other], vec![], vec![]);
    let oversized = shape(full.digests(), vec![], vec![], 0, 1).encode();
    assert!(oversized.len() > rdm_transport::membership::MAX_FRAME);
    let chunks = rdm_transport::snapshot::chunks_of(&full, shape);
    assert_eq!(chunks.len(), 2);
    assert!(chunks.iter().all(|f| f.encode().len() <= rdm_transport::membership::MAX_FRAME));
    assert!(chunks.iter().all(|f| f.context().unwrap().tracestate.as_ref() == Some(&state)));
}
