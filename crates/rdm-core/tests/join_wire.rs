//! Fixed bytes for the `0x1D` join family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::wire::WireDigest;
use rdm_core::join::{Join, JoinReply, JoinRequest};
use rdm_core::{IncarnationId, NodeId};
use rdm_core::outcome::MalformedKind;
use rdm_core::protocol::NodeProtocol;

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn digest() -> rdm_core::MeshDigest {
    use rdm_core::*;
    MeshDigest { held_hash: Default::default(),
        fabric_id: FabricId::mint(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            endpoint_id: EndpointId("k".into()),
            transport_addr: "127.0.0.1:34567".parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

/// The request is variant 0 followed by the digest in its positional wire shape: a digest with
/// every optional field absent still decodes to itself (no `skip_serializing_if` shifts a field).
#[test]
fn a_join_request_and_its_digest_round_trip_through_postcard_with_every_field_present() {
    let d = digest();
    let q = JoinRequest::JoinNode { types: Vec::new(), digest: WireDigest::from(&d), listeners: Vec::new() };
    let bytes = Join::encode_request(&q).unwrap();
    assert_eq!(bytes[0], 0x00, "variant 0");
    assert!(bytes.len() <= Join::MAX_REQUEST_FRAME_BYTES);
    let JoinRequest::JoinNode { digest: back, .. } = Join::decode_request(&bytes).unwrap();
    assert_eq!(rdm_core::MeshDigest::from(back), d);
}

#[test]
fn replies_match_the_frozen_eleven_variant_wire_schema() {
    use JoinReply::*;
    let fixtures = [
        (Joined { answer: vec![1] }, "000101"),
        (JoinMismatch { field: "a".into(), deployed: "b".into(), reported: "c".into() }, "010161016201 63"),
        (NotPrimary { primary: Some("m".into()) }, "0201016d"),
        (NotPrimary { primary: None }, "0200"),
        (PeerUnresolved { reason: "p".into() }, "030170"),
        (NotReady { reason: "n".into() }, "04016e"),
        (Busy { reason: "b".into() }, "050162"),
        (ConnectionsDeleting { reason: "d".into() }, "060164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0702"),
        (Unauthorized { reason: "u".into() }, "080175"),
        (DeploymentAbandoned { build_id: "b".into(), attempt: 2, node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap() }, "09016202 0c 303132333435363738396162 0c79727a34396a333033796834"),
        (CertRefused { name: "n".into(), detail: "d".into() }, "0a016e0164"),
    ];
    for (reply, hex) in fixtures {
        let expected = bytes(&hex.replace(' ', ""));
        assert_eq!(Join::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Join::decode_reply(&expected).unwrap(), reply);
    }
}

/// The request ends with the listeners the node bound (Q151), after the type declaration: their
/// count, then each name and its address (`V4`, four octets, the port as a u16 varint).
#[test]
fn a_join_request_carries_the_listeners_the_node_bound_after_its_types() {
    let d = digest();
    let listeners = vec![("kafka".to_string(), "127.0.0.1:9092".parse().unwrap())];
    let q = JoinRequest::JoinNode { types: Vec::new(), digest: WireDigest::from(&d), listeners: listeners.clone() };
    let wire = Join::encode_request(&q).unwrap();
    assert_eq!(&wire[wire.len() - 15..], bytes("00 01 056b61666b61 00 7f000001 8447".replace(' ', "").as_str()).as_slice(), "the empty types list, then one listener: kafka at 127.0.0.1:9092");
    let JoinRequest::JoinNode { listeners: back, .. } = Join::decode_request(&wire).unwrap();
    assert_eq!(back, listeners);
}

/// The listeners round-trip in order, none and three alike (Q151): an empty list is one `00` count byte;
/// three named listeners come back with their names and addresses.
#[test]
fn a_join_request_round_trips_no_listeners_and_three() {
    let d = digest();
    let none = Join::encode_request(&JoinRequest::JoinNode { types: Vec::new(), digest: WireDigest::from(&d), listeners: Vec::new() }).unwrap();
    assert_eq!(none.last(), Some(&0u8), "no listeners is a zero count");
    let JoinRequest::JoinNode { listeners: back, .. } = Join::decode_request(&none).unwrap();
    assert!(back.is_empty());
    let three: Vec<(String, std::net::SocketAddr)> = [("kafka", "10.0.0.7:41001"), ("rest", "10.0.0.7:41002"), ("proxy", "10.0.0.7:41003")].iter().map(|(n, a)| (n.to_string(), a.parse().unwrap())).collect();
    let wire = Join::encode_request(&JoinRequest::JoinNode { types: Vec::new(), digest: WireDigest::from(&d), listeners: three.clone() }).unwrap();
    let JoinRequest::JoinNode { listeners: back, .. } = Join::decode_request(&wire).unwrap();
    assert_eq!(back, three, "three listeners come back as sent");
}

/// The digest's wire shape ends `load`, `gossip`, the held hash, then the primary revision and the
/// fabric-primary revision (gossip.md "A primary change on gossip"), every field present: `gossip`
/// absent is one `00` byte after the `00` of an absent `load`; present it is `01` then `heard`,
/// `neighbours` (u32 varints) and `frames_sent`, `frames_received` (u64 varints). A revision absent is
/// one `00`; present it is `01` then the revision (a u64 varint). A field added to the struct moves
/// these bytes again.
#[test]
fn a_join_keeps_gossip_positions_then_appends_the_held_hash_and_type_declaration() {
    use rdm_core::GossipStats;
    let mut d = digest();
    let none = Join::encode_request(&JoinRequest::JoinNode { types: Vec::new(), digest: WireDigest::from(&d), listeners: Vec::new() }).unwrap();
    assert_eq!(&none[none.len() - 38..none.len() - 36], bytes("0000").as_slice(), "load absent, gossip absent");
    d.gossip = Some(GossipStats { heard: 2, neighbours: 3, frames_sent: 300, frames_received: 5 });
    let some = Join::encode_request(&JoinRequest::JoinNode { types: Vec::new(), digest: WireDigest::from(&d), listeners: Vec::new() }).unwrap();
    assert_eq!(&some[some.len() - 43..some.len() - 36], bytes("00010203ac0205").as_slice());
    let JoinRequest::JoinNode { digest: back, .. } = Join::decode_request(&some).unwrap();
    assert_eq!(rdm_core::MeshDigest::from(back).gossip, d.gossip);
    assert_eq!(&some[some.len()-36..], &[0;36], "held hash is 32 fixed bytes, both revisions absent, then the empty types list and the empty listeners list");
    d.is_primary = Some(4);
    d.is_fabric_primary = Some(300);
    let primary = Join::encode_request(&JoinRequest::JoinNode { types: Vec::new(), digest: WireDigest::from(&d), listeners: Vec::new() }).unwrap();
    assert_eq!(&primary[primary.len() - 7..], bytes("010401ac020000").as_slice(), "the primary revision, the fabric-primary revision, then the empty types list and the empty listeners list");
    let JoinRequest::JoinNode { digest: back, .. } = Join::decode_request(&primary).unwrap();
    let back = rdm_core::MeshDigest::from(back);
    assert_eq!((back.is_primary, back.is_fabric_primary), (Some(4), Some(300)));
}
