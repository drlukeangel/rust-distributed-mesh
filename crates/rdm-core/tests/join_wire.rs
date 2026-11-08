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
    MeshDigest { 
        fabric_id: FabricId::mint(),
        node: MeshNode { mesh: "mesh1".into(), r#type: "node".into(),
            node_id: NodeId::mint(),
            path: "mesh1.node".into(), name: "1".into(),
            endpoint_id: EndpointId("k".into()),
            transport_addr: "127.0.0.1:34567".parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
        },
        status: rdm_core::model::NodeStatus::Pending,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        primary: Vec::new(),
    }
}

/// The request is variant 0 followed by the digest in its positional wire shape: a digest with
/// every optional field absent still decodes to itself (no `skip_serializing_if` shifts a field).
#[test]
fn a_join_request_and_its_digest_encode_and_decode_through_postcard_with_every_field_present() {
    let d = digest();
    let q = JoinRequest::JoinNode { digest: WireDigest::from(&d), runtime: None, data_dir: None, listeners: Vec::new() };
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
        (NotPrimary { primary: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()) }, "02010c303132333435363738396162"),
        (NotPrimary { primary: None }, "0200"),
        (PeerUnresolved { reason: "p".into() }, "030170"),
        (NotReady { reason: "n".into() }, "04016e"),
        (Busy { reason: "b".into() }, "050162"),
        (ConnectionsDeleting { reason: "d".into() }, "060164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0702"),
        (Unauthorized { reason: "u".into() }, "080175"),
        (DeploymentAbandoned { build_id: "b".into(), node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap() }, "090162 0c 303132333435363738396162 0c79727a34396a333033796834"),
        (CertRefused { name: "n".into(), detail: "d".into() }, "0a016e0164"),
    ];
    for (reply, hex) in fixtures {
        let expected = bytes(&hex.replace(' ', ""));
        assert_eq!(Join::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Join::decode_reply(&expected).unwrap(), reply);
    }
}

/// The request ends with the listeners the node bound (Q151), after its digest: their count, then
/// each name and its address (`V4`, four octets, the port as a u16 varint).
#[test]
fn a_join_request_carries_the_listeners_the_node_bound_after_its_digest() {
    let d = digest();
    let listeners = vec![("kafka".to_string(), "127.0.0.1:9092".parse().unwrap())];
    let q = JoinRequest::JoinNode { digest: WireDigest::from(&d), runtime: None, data_dir: None, listeners: listeners.clone() };
    let wire = Join::encode_request(&q).unwrap();
    assert_eq!(&wire[wire.len() - 15..], bytes("00 01 056b61666b61 00 7f000001 8447".replace(' ', "").as_str()).as_slice(), "the absent data dir, then one listener: kafka at 127.0.0.1:9092");
    let JoinRequest::JoinNode { listeners: back, .. } = Join::decode_request(&wire).unwrap();
    assert_eq!(back, listeners);
}

/// The listeners round-trip in order, none and three alike (Q151): an empty list is one `00` count byte;
/// three named listeners come back with their names and addresses.
#[test]
fn a_join_request_encodes_and_decodes_with_no_listeners_and_three() {
    let d = digest();
    let none = Join::encode_request(&JoinRequest::JoinNode { digest: WireDigest::from(&d), runtime: None, data_dir: None, listeners: Vec::new() }).unwrap();
    assert_eq!(none.last(), Some(&0u8), "no listeners is a zero count");
    let JoinRequest::JoinNode { listeners: back, .. } = Join::decode_request(&none).unwrap();
    assert!(back.is_empty());
    let three: Vec<(String, std::net::SocketAddr)> = [("kafka", "10.0.0.7:41001"), ("rest", "10.0.0.7:41002"), ("proxy", "10.0.0.7:41003")].iter().map(|(n, a)| (n.to_string(), a.parse().unwrap())).collect();
    let wire = Join::encode_request(&JoinRequest::JoinNode { digest: WireDigest::from(&d), runtime: None, data_dir: None, listeners: three.clone() }).unwrap();
    let JoinRequest::JoinNode { listeners: back, .. } = Join::decode_request(&wire).unwrap();
    assert_eq!(back, three, "three listeners come back as sent");
}

/// The digest's wire shape ends with the primary roles the birth holds (Q392: a count, then each
/// role's position, a node type's name after `NodePrimary`, and its revision as a u64 varint), its
/// `digest_seq` and its emitted time; the join then carries the birth's runtime and data dir (each
/// `00` absent, `01` then the value present) and its listeners. No load or gossip counter rides the
/// join (Q394). A field added to the struct moves these bytes again.
#[test]
fn a_join_carries_the_primary_roles_then_the_runtime_the_data_dir_and_the_listeners() {
    let mut d = digest();
    let none = Join::encode_request(&JoinRequest::JoinNode { digest: WireDigest::from(&d), runtime: None, data_dir: None, listeners: Vec::new() }).unwrap();
    assert_eq!(&none[none.len() - 6..], &[0; 6], "no primary role, digest_seq 0, emitted 0, runtime absent, data dir absent, no listeners");
    d.set_primary(Some(4));
    d.set_fabric_primary(Some(300));
    let primary = Join::encode_request(&JoinRequest::JoinNode { digest: WireDigest::from(&d), runtime: None, data_dir: Some("/d".into()), listeners: Vec::new() }).unwrap();
    assert_eq!(&primary[primary.len() - 19..], bytes("02 01ac02 02046e6f646504 00 00 00 01022f64 00".replace(' ', "").as_str()).as_slice(), "the fabric-primary at 300 and the node's own type primary at 4, then digest_seq, emitted, the absent runtime, the data dir and the empty listeners list");
    let JoinRequest::JoinNode { digest: back, data_dir, .. } = Join::decode_request(&primary).unwrap();
    let back = rdm_core::MeshDigest::from(back);
    assert_eq!((back.is_primary(), back.is_fabric_primary(), data_dir.as_deref()), (Some(4), Some(300), Some("/d")));
}
