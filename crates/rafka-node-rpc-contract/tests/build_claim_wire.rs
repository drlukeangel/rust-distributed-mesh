//! Fixed bytes for the `0x1C` build-claim family. These literals lock discriminants and
//! positional fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc_contract::build_claim::{BuildClaim, BuildClaimReply, BuildClaimRequest};
use rafka_node_rpc_contract::context::CallContext;
use rafka_node_rpc_contract::outcome::MalformedKind;
use rafka_node_rpc_contract::protocol::NodeProtocol;

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn the_request_matches_the_frozen_one_variant_wire_schema() {
    let q = BuildClaimRequest::ClaimAttempt {
        build_id: "bld-x".into(),
        attempt: 3,
        executor_node_id: NodeId::parse("0123456789ab").unwrap(),
        executor_incarnation: IncarnationId("birth".into()),
    };
    let expected = bytes("00") .into_iter().chain(bytes("05626c642d78")).chain(bytes("03")).chain(bytes("0c303132333435363738396162")).chain(bytes("056269727468")).collect::<Vec<u8>>();
    assert_eq!(BuildClaim::encode_request(&q).unwrap(), expected);
    assert_eq!(BuildClaim::decode_request(&expected).unwrap(), q);
}

#[test]
fn replies_match_the_frozen_ten_variant_wire_schema() {
    use BuildClaimReply::*;
    let fixtures = [
        (Won { context: CallContext::default() }, "0000000000"),
        (Lost { holder: "a".into() }, "010161"),
        (NotOpen { next: Some(7) }, "020107"),
        (NotOpen { next: None }, "0200"),
        (NotFabricPrimary { fabric_primary: Some("m".into()) }, "0301016d"),
        (PeerUnresolved { reason: "p".into() }, "040170"),
        (NotReady { reason: "n".into() }, "05016e"),
        (Busy { reason: "b".into() }, "060162"),
        (Draining { reason: "d".into() }, "070164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0802"),
        (Unauthorized { reason: "u".into() }, "090175"),
    ];
    for (reply, hex) in fixtures {
        let expected = bytes(hex);
        assert_eq!(BuildClaim::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(BuildClaim::decode_reply(&expected).unwrap(), reply);
    }
}
