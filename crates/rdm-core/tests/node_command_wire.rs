//! The node command op `0x22` (i144 R22; Luke Q7): its request and every reply frame in frozen
//! postcard bytes, its catalog shape (server-streaming, never forwarded), and Status 0x1B still
//! single-reply and forwarded.

use rdm_core::catalog::{CatalogBuilder, CatalogEntry, OpOwner, SealError, Shape};
use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
use rdm_core::outcome::MalformedKind;
use rdm_core::protocol::NodeProtocol;
use rdm_core::status::{ConnectionScope, ConnectionsReceipt, Status};
use rdm_core::{ConnectionId, IncarnationId, NodeId};

fn node() -> NodeId { NodeId::parse("0123456789ab").unwrap() }
fn birth() -> IncarnationId { IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap() }
fn connection() -> ConnectionId { ConnectionId::parse("3123456789ab").unwrap() }

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

/// CONTRACT (Luke Q7): the one request of op 0x22, `DeleteConnections`, is variant 0 in frozen bytes:
/// node id, incarnation, then the scope (`All` 0, `Connections` 1 with its ids).
#[test]
fn the_request_matches_the_frozen_one_variant_wire_schema() {
    assert_eq!(NodeCommand::REQUEST_VARIANTS, 1);
    for (req, hex) in [
        (NodeCommandRequest::DeleteConnections { node_id: node(), incarnation: birth(), scope: ConnectionScope::All }, "000c3031323334353637383961620c39776370726667777674687900"),
        (NodeCommandRequest::DeleteConnections { node_id: node(), incarnation: birth(), scope: ConnectionScope::Connections(vec![connection()]) }, "000c3031323334353637383961620c39776370726667777674687901010c333132333435363738396162"),
    ] {
        let expected = bytes(hex);
        assert_eq!(NodeCommand::encode_request(&req).unwrap(), expected, "{req:?}");
        assert_eq!(NodeCommand::decode_request(&expected).unwrap(), req);
    }
}

/// CONTRACT (Luke Q7): every reply of op 0x22 in frozen bytes, in its position: `CommandAccepted` 0,
/// `ConnectionsDeleting` 1, `ConnectionsDeleted` 2 (its receipt absent for a list of ids, present for
/// `All`), then the refusals.
#[test]
fn replies_match_the_frozen_eleven_variant_wire_schema() {
    assert_eq!(NodeCommand::REPLY_VARIANTS, 11);
    use NodeCommandReply::*;
    for (reply, hex) in [
        (CommandAccepted { node_id: node(), incarnation: birth() }, "000c3031323334353637383961620c397763707266677776746879"),
        (ConnectionsDeleting { node_id: node(), incarnation: birth() }, "010c3031323334353637383961620c397763707266677776746879"),
        (ConnectionsDeleted { closed: vec![connection()], not_open: vec![], held: vec![], receipt: None }, "02010c333132333435363738396162000000"),
        (ConnectionsDeleted { closed: vec![connection()], not_open: vec![], held: vec![], receipt: Some(ConnectionsReceipt::Established { in_flight: 0, closed: vec![connection()] }) }, "02010c3331323334353637383961620000010000010c333132333435363738396162"),
        (ConnectionsDeleted { closed: vec![], not_open: vec![], held: vec![connection()], receipt: Some(ConnectionsReceipt::Deadline { last_in_flight: 1, closed: vec![], held: vec![connection()] }) }, "020000010c333132333435363738396162010101 00010c333132333435363738396162".replace(' ', "").as_str()),
        (RejectedConnectionScope { reason: "r".into() }, "030172"),
        (RejectedStaleIncarnation { held: birth() }, "040c397763707266677776746879"),
        (NotReady { reason: "r".into() }, "050172"),
        (PeerUnresolved { reason: "r".into() }, "060172"),
        (Busy { reason: "r".into() }, "070172"),
        (RejectedConnectionsDeleting { reason: "r".into() }, "080172"),
        (Unauthorized { reason: "r".into() }, "0a0172"),
    ] {
        let expected = bytes(hex);
        assert_eq!(NodeCommand::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(NodeCommand::decode_reply(&expected).unwrap(), reply);
    }
    let malformed = Malformed { kind: MalformedKind::Corrupt };
    let encoded = NodeCommand::encode_reply(&malformed).unwrap();
    assert_eq!(encoded[0], 0x09, "Malformed is position 9: {encoded:?}");
    assert_eq!(NodeCommand::decode_reply(&encoded).unwrap(), malformed);
}

/// CONTRACT (Luke Q7: "NodeCommand seals non-forwardable and streaming; Status still seals
/// forwardable"; the seal refuses a forwardable stream, `SealError::ForwardableStream`): op 0x22's
/// catalog entry is server-streaming and not forwardable and seals beside Status 0x1B, which stays a
/// single reply and forwarded; the same op marked forwardable is refused at the seal.
#[test]
fn node_command_seals_streaming_and_never_forwarded_beside_a_forwarded_status() {
    let command = CatalogEntry::canonical::<NodeCommand>(OpOwner::Product("rdm".into()), Shape::ServerStreaming);
    let status = CatalogEntry::canonical::<Status>(OpOwner::Product("rdm".into()), Shape::Unary);
    assert_eq!((command.op, command.shape, command.forwardable), (0x22, Shape::ServerStreaming, false), "{command:?}");
    assert_eq!((status.op, status.shape, status.forwardable), (0x1B, Shape::Unary, true), "{status:?}");
    CatalogBuilder::new().serve(command.clone()).serve(status).seal().expect("NodeCommand seals beside Status");
    let forwarded = CatalogEntry { forwardable: true, ..command };
    let refused = CatalogBuilder::new().serve(forwarded).seal().expect_err("a forwarded stream is refused at the seal");
    assert!(refused.iter().any(|e| matches!(e, SealError::ForwardableStream { op: 0x22, .. })), "{refused:?}");
}
