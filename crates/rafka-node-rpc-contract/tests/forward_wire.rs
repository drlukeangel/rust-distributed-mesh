//! Fixed bytes for the `0x1A` forward family. These literals lock discriminants and positional
//! fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::NodeId;
use rafka_node_rpc_contract::forward::{Forward, ForwardReply, ForwardRequest};
use rafka_node_rpc_contract::outcome::MalformedKind;
use rafka_node_rpc_contract::protocol::NodeProtocol;

fn bytes(hex: &str) -> Vec<u8> {
    hex.replace(' ', "").as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

/// The ASCII of `0123456789ab`, a NodeId on the wire as a length-prefixed string.
const NODE: &str = "0123456789ab";
const NODE_HEX: &str = "30 31 32 33 34 35 36 37 38 39 61 62";

#[test]
fn the_request_matches_the_frozen_one_variant_wire_schema() {
    assert_eq!(Forward::REQUEST_VARIANTS, 1);
    // variant 0 | target (len 12, ascii) | inner_op 0x5e | inner (len 2: 01 02) | remaining_ms 1000 (varint e8 07)
    let q = ForwardRequest::Forward { target: NodeId::parse(NODE).unwrap(), inner_op: 0x5e, inner: vec![1, 2], remaining_ms: 1000 };
    let expected = bytes(&format!("00 0c {NODE_HEX} 5e 02 01 02 e8 07"));
    assert_eq!(Forward::encode_request(&q).unwrap(), expected);
    assert_eq!(Forward::decode_request(&expected).unwrap(), q);
}

#[test]
fn replies_match_the_frozen_fourteen_variant_wire_schema() {
    assert_eq!(Forward::REPLY_VARIANTS, 14);
    use ForwardReply::*;
    let fixtures = [
        (Relayed { inner: vec![1] }, "00 01 01".to_string()),
        (InnerNotSent { reason: "r".into() }, "01 01 72".into()),
        (InnerUnserved { op: 7 }, "02 07".into()),
        (InnerRejectedStale { target_node_id: NodeId::parse(NODE).unwrap() }, format!("03 0c {NODE_HEX}")),
        (InnerIndeterminate { reason: "r".into() }, "04 01 72".into()),
        (NotForwardable { op: 1 }, "05 01".into()),
        (PeerUnresolved { reason: "p".into() }, "06 01 70".into()),
        (NotReady { reason: "n".into() }, "07 01 6e".into()),
        (Busy { reason: "b".into() }, "08 01 62".into()),
        (Draining { reason: "d".into() }, "09 01 64".into()),
        (Malformed { kind: MalformedKind::Corrupt }, "0a 02".into()),
        (Unauthorized { reason: "u".into() }, "0b 01 75".into()),
        (CarrierEdgeLost { reason: "e".into() }, "0c 01 65".into()),
        (NoInnerBudget { remaining_ms: 5, reserve_ms: 100 }, "0d 05 64".into()),
    ];
    assert_eq!(fixtures.len() as u32, Forward::REPLY_VARIANTS);
    for (reply, hex) in fixtures {
        let expected = bytes(&hex);
        assert_eq!(Forward::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Forward::decode_reply(&expected).unwrap(), reply);
    }
}
