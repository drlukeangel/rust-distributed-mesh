//! Fixed bytes for the `0x1C` build-claim family. These literals lock discriminants and
//! positional fields independently of codec round trips; enums are append-only.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::{IncarnationId, NodeId};
use rdm_core::build_claim::{BuildClaim, BuildClaimReply, BuildClaimRequest};
use rdm_core::context::CallContext;
use rdm_core::outcome::MalformedKind;
use rdm_core::protocol::NodeProtocol;

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn the_request_matches_the_frozen_one_variant_wire_schema() {
    let q = BuildClaimRequest::ClaimAttempt {
        build_id: "b7swg3jj790z".into(),
        attempt: 3,
        executor_node_id: NodeId::parse("0123456789ab").unwrap(),
        executor_incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap(),
        mesh: None,
    };
    let expected = bytes("00") .into_iter().chain(bytes("0c6237737767336a6a3739307a")).chain(bytes("03")).chain(bytes("0c303132333435363738396162")).chain(bytes("0c397763707266677776746879")).chain(bytes("00")).collect::<Vec<u8>>();
    assert_eq!(BuildClaim::encode_request(&q).unwrap(), expected);
    assert_eq!(BuildClaim::decode_request(&expected).unwrap(), q);
    let drift = BuildClaimRequest::ClaimAttempt {
        build_id: "b7swg3jj790z".into(),
        attempt: 1,
        executor_node_id: NodeId::parse("0123456789ab").unwrap(),
        executor_incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap(),
        mesh: Some("m".into()),
    };
    let expected = bytes("00") .into_iter().chain(bytes("0c6237737767336a6a3739307a")).chain(bytes("01")).chain(bytes("0c303132333435363738396162")).chain(bytes("0c397763707266677776746879")).chain(bytes("01016d")).collect::<Vec<u8>>();
    assert_eq!(BuildClaim::encode_request(&drift).unwrap(), expected);
    assert_eq!(BuildClaim::decode_request(&expected).unwrap(), drift);
}

#[test]
fn replies_match_the_frozen_eleven_variant_wire_schema() {
    use BuildClaimReply::*;
    let fixtures = [
        (Won { context: CallContext::default() }, "0000000000"),
        (Lost { holder: "a".into() }, "010161"),
        (NotOpen { next: Some(7) }, "020107"),
        (NotOpen { next: None }, "0200"),
        (NotClaimant { mesh: Some("x".into()), fabric_primary: Some("m".into()), mesh_primary: None }, "0301017801016d00"),
        (PeerUnresolved { reason: "p".into() }, "040170"),
        (NotReady { reason: "n".into() }, "05016e"),
        (Busy { reason: "b".into() }, "060162"),
        (ConnectionsDeleting { reason: "d".into() }, "070164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0802"),
        (Unauthorized { reason: "u".into() }, "090175"),
        (BuildInProgress { current_build_id: "c".into(), mesh: "x".into(), held_by: Some("h".into()) }, "0a01630178010168"),
        (BuildInProgress { current_build_id: "c".into(), mesh: "x".into(), held_by: None }, "0a0163017800"),
    ];
    for (reply, hex) in fixtures {
        let expected = bytes(hex);
        assert_eq!(BuildClaim::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(BuildClaim::decode_reply(&expected).unwrap(), reply);
    }
}
