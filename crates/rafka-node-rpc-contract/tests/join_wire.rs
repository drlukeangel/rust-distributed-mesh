//! Fixed bytes for the `0x1D` join family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::wire::WireDigest;
use rafka_node_rpc_contract::join::{Join, JoinReply, JoinRequest};
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc_contract::outcome::MalformedKind;
use rafka_node_rpc_contract::protocol::NodeProtocol;

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn digest() -> rafka_mesh_entity::MeshDigest {
    use rafka_mesh_entity::*;
    MeshDigest {
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
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        extra: Default::default(),
        load: None,
        gossip: None,
    }
}

/// The request is variant 0 followed by the digest in its positional wire shape: a digest with
/// every optional field absent still decodes to itself (no `skip_serializing_if` shifts a field).
#[test]
fn a_join_request_and_its_digest_round_trip_through_postcard_with_every_field_present() {
    let d = digest();
    let q = JoinRequest::JoinNode { digest: WireDigest::from(&d) };
    let bytes = Join::encode_request(&q).unwrap();
    assert_eq!(bytes[0], 0x00, "variant 0");
    assert!(bytes.len() <= Join::MAX_REQUEST_FRAME_BYTES);
    let JoinRequest::JoinNode { digest: back } = Join::decode_request(&bytes).unwrap();
    assert_eq!(rafka_mesh_entity::MeshDigest::from(back), d);
}

#[test]
fn replies_match_the_frozen_eleven_variant_wire_schema() {
    use JoinReply::*;
    let fixtures = [
        (Joined { answer: vec![1] }, "000101"),
        (JoinMismatch { field: "a".into(), deployed: "b".into(), reported: "c".into() }, "010161016201 63"),
        (NotAuthority { primary: Some("m".into()) }, "0201016d"),
        (NotAuthority { primary: None }, "0200"),
        (PeerUnresolved { reason: "p".into() }, "030170"),
        (NotReady { reason: "n".into() }, "04016e"),
        (Busy { reason: "b".into() }, "050162"),
        (Draining { reason: "d".into() }, "060164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0702"),
        (Unauthorized { reason: "u".into() }, "080175"),
        (DeploymentAbandoned { build_id: "b".into(), attempt: 2, node_id: NodeId::parse("0123456789ab").unwrap(), incarnation: IncarnationId("i".into()) }, "09016202 0c 303132333435363738396162 0169"),
        (CertRefused { name: "n".into(), detail: "d".into() }, "0a016e0164"),
    ];
    for (reply, hex) in fixtures {
        let expected = bytes(&hex.replace(' ', ""));
        assert_eq!(Join::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Join::decode_reply(&expected).unwrap(), reply);
    }
}

/// The digest's wire shape ends `load`, then `gossip`, every field present: `gossip` absent is one
/// `00` byte after the `00` of an absent `load`; present it is `01` then `heard`, `neighbours`
/// (u32 varints) and `frames_sent`, `frames_received` (u64 varints). A field added to the struct
/// moves these bytes again.
#[test]
fn a_digest_wire_shape_ends_with_the_gossip_stats_positional_bytes() {
    use rafka_mesh_entity::GossipStats;
    let mut d = digest();
    let none = Join::encode_request(&JoinRequest::JoinNode { digest: WireDigest::from(&d) }).unwrap();
    assert_eq!(&none[none.len() - 2..], bytes("0000").as_slice(), "load absent, gossip absent");
    d.gossip = Some(GossipStats { heard: 2, neighbours: 3, frames_sent: 300, frames_received: 5 });
    let some = Join::encode_request(&JoinRequest::JoinNode { digest: WireDigest::from(&d) }).unwrap();
    assert_eq!(&some[some.len() - 7..], bytes("00010203ac0205").as_slice());
    let JoinRequest::JoinNode { digest: back } = Join::decode_request(&some).unwrap();
    assert_eq!(rafka_mesh_entity::MeshDigest::from(back).gossip, d.gossip);
}
