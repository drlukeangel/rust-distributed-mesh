//! Fixed bytes for the `0x1D` join family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_node_rpc_contract::join::{Join, JoinReply, JoinRequest};
use rafka_node_rpc_contract::outcome::MalformedKind;
use rafka_node_rpc_contract::protocol::NodeProtocol;

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn the_request_matches_the_frozen_one_variant_wire_schema() {
    let q = JoinRequest::JoinNode { digest: vec![0xAB, 0xCD] };
    let expected = bytes("0002abcd");
    assert_eq!(Join::encode_request(&q).unwrap(), expected);
    assert_eq!(Join::decode_request(&expected).unwrap(), q);
}

#[test]
fn replies_match_the_frozen_nine_variant_wire_schema() {
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
    ];
    for (reply, hex) in fixtures {
        let expected = bytes(&hex.replace(' ', ""));
        assert_eq!(Join::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Join::decode_reply(&expected).unwrap(), reply);
    }
}
