//! Fixed bytes for the seat frames on the membership channels (R-A2; R-W1: postcard, positional,
//! append-only). These literals lock the type names and the data field order independently of codec
//! round trips. Never regenerate an expectation from the Rust serializer to make a schema change
//! pass: a changed byte here is a moved wire format and needs a rebirth.

use crate::gossip_wire::{WireGolden, golden, assert_roundtrip};

use rdm_core::{IncarnationId, NodeId, Seat, SeatHolder};
use rdm_transport::membership::{Frame, SeatBook, SeatTaken};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn node() -> NodeId {
    NodeId::parse("04raj09p3zp7").unwrap()
}

fn holder(revision: u64) -> SeatHolder {
    SeatHolder { mesh: "m".into(), node_id: node(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), revision }
}

/// CONTRACT: `Seated` is variant 8 and `Concern` variant 9 of the membership frame, appended after
/// `FabricStatus` (7); a seat is its own enum (`MeshPrimary` 0, `FabricPrimary` 1); a holder is its
/// mesh, NodeId, incarnation and revision, in that order. What must NOT happen: a field moved, a
/// variant inserted before the end, or a self-describing encoding.
#[test]
fn seat_frames_match_the_frozen_wire_schema() {
    // 08 = Seated; 01 = FabricPrimary; 01 6d = "m"; 0c + 12 ASCII bytes = the NodeId; 0c <fixture("i")> = the incarnation; 02 = revision.
    let node_hex = "0c 303472616a303970337a7037";
    let fixtures = [
        (Frame::MeshStatus { mesh: "m".into(), status: "s".into(), publisher: "p".into(), forwarded_by: None, changed_at_rafka_ms: 1 }, "06 016d 0173 0170 00 01".to_string()),
        (Frame::FabricStatus { fabric: rdm_core::FabricId::parse("04raj09p3zp7").unwrap(), status: "s".into(), publisher: "p".into(), forwarded_by: None, changed_at_rafka_ms: 1 }, "07 0c303472616a303970337a7037 0173 0170 00 01".to_string()),
        (Frame::Seated { seat: Seat::FabricPrimary, holder: holder(2) }, format!("08 01 016d {node_hex} 0c79727a34396a333033796834 02")),
        (Frame::Seated { seat: Seat::MeshPrimary, holder: holder(300) }, format!("08 00 016d {node_hex} 0c79727a34396a333033796834 ac02")),
        (Frame::Concern { seat: Seat::FabricPrimary, node_id: node(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), observer: "o".into() }, format!("09 01 {node_hex} 0c79727a34396a333033796834 016f")),
    ];
    for (frame, hex) in fixtures {
        let expected = golden(&frame, &bytes(&hex));
        assert_eq!(frame.golden_encode(), expected, "{frame:?}");
        assert_roundtrip(&frame, &expected);
    }
    // The frames that existed keep their positions.
    assert_eq!(Frame::FabricStatus { fabric: rdm_core::FabricId::parse("04raj09p3zp7").unwrap(), status: "s".into(), publisher: "p".into(), forwarded_by: None, changed_at_rafka_ms: 1 }.type_position(), 7);
    // A seat this build does not know is refused by name, never read as a prefix.
    let mut malformed = golden(&Frame::Seated { seat: Seat::FabricPrimary, holder: holder(2) }, &bytes("08 01 016d 0c303472616a303970337a7037 0c79727a34396a333033796834 02"));
    let (attrs, mut body): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&malformed).unwrap();
    body[0] = 3; malformed = rdm_transport::wire::encode(&(attrs, body)).unwrap();
    let e = Frame::decode(&malformed).unwrap_err().to_string();
    assert!(e.contains("postcard decode"), "{e}");
}

/// CONTRACT: a seat's record is replaced only by one that supersedes it, a repeat changes nothing,
/// and a primary replays every record it holds to a neighbour that came up.
#[test]
fn a_seat_book_holds_the_superseding_record_and_replays_what_it_holds() {
    let book = SeatBook::default();
    assert_eq!(book.take(Seat::FabricPrimary, &holder(1)), SeatTaken::Held { previous: None });
    assert_eq!(book.take(Seat::FabricPrimary, &holder(1)), SeatTaken::Same);
    assert_eq!(book.take(Seat::FabricPrimary, &holder(3)), SeatTaken::Held { previous: Some(holder(1)) });
    assert_eq!(book.take(Seat::FabricPrimary, &holder(2)), SeatTaken::Refused { held: holder(3) }, "an older revision never replaces");
    assert_eq!(book.take(Seat::MeshPrimary, &holder(1)), SeatTaken::Held { previous: None }, "a mesh's seat is its own record");
    assert_eq!(book.fabric(), Some(holder(3)));
    assert_eq!(book.mesh("m"), Some(holder(1)));
    let replay: Vec<Vec<u8>> = book.held_frames().iter().map(|f| f.golden_encode()).collect();
    assert_eq!(replay, vec![Frame::Seated { seat: Seat::MeshPrimary, holder: holder(1) }.golden_encode(), Frame::Seated { seat: Seat::FabricPrimary, holder: holder(3) }.golden_encode()]);
}

/// CONTRACT: `NewFabricPrimary` is variant 14, appended after `NodeConnectionsDeleted` (13), a reserved position (R-G3): the committed
/// holder (mesh, NodeId, incarnation, revision), the endpoint key, the bound QUIC UDP address
/// (socket-address variant 0 is IPv4: four octets then the port as a varint) and the optional HTTP
/// API base. The two addresses are separate fields: neither is derived from the other.
#[test]
fn new_fabric_primary_matches_the_frozen_wire_schema() {
    let node_hex = "0c 303472616a303970337a7037";
    let frame = Frame::NewFabricPrimary { holder: holder(8), endpoint_id: "e".into(), transport_addr: "127.0.0.1:54474".parse().unwrap(), admin_api_base: Some("h".into()) };
    // 0e = variant 14; holder 016d {node} 0c79727a34396a333033796834 08; 0165 = "e"; 00 7f000001 caa903 = 127.0.0.1:54474; 01 0168 = Some("h").
    let hex = format!("0e 016d {node_hex} 0c79727a34396a333033796834 08 0165 00 7f000001 caa903 01 0168");
    let expected = golden(&frame, &bytes(&hex));
    assert_eq!(frame.golden_encode(), expected);
    assert_roundtrip(&frame, &expected);
    let none = Frame::NewFabricPrimary { holder: holder(8), endpoint_id: "e".into(), transport_addr: "127.0.0.1:80".parse().unwrap(), admin_api_base: None };
    assert_eq!(none.golden_encode(), golden(&none, &bytes(&format!("0e 016d {node_hex} 0c79727a34396a333033796834 08 0165 00 7f000001 50 00"))));
}


fn op() -> rdm_core::LifecycleOp {
    rdm_core::LifecycleOp { build_id: "b".into(), attempt: 1, operation: "o".into(), node_id: node(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), name: "m.admin.1".parse().unwrap(), event_at_rafka_ms: 1 }
}

/// Both seats of mesh `m`, committed to the birth `m`/`node()`/`i` at their revisions.
fn both(mesh_revision: u64, fabric_revision: u64) -> Vec<rdm_core::seat::transfer::CommittedSeat> {
    use rdm_core::seat::transfer::{CommittedSeat, HandoverBirth, Seat as TransferSeat};
    let birth = HandoverBirth { mesh: "m".into(), node_id: node(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap() };
    vec![CommittedSeat { seat: TransferSeat::MeshPrimary { mesh: "m".into() }, holder: birth.clone(), revision: mesh_revision }, CommittedSeat { seat: TransferSeat::FabricPrimary, holder: birth, revision: fabric_revision }]
}

fn primary_updated(mesh_revision: u64, fabric_revision: u64) -> Frame {
    Frame::PrimaryUpdated { op: op(), seats: both(mesh_revision, fabric_revision) }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md Part C3 edit 6:
/// "two announcements on the mesh channel and the backbone, 'primary updating' before the row and
/// 'primary updated' after `node.primary.updated`, each naming operation, cohort, both exact births
/// and the epochs"): `PrimaryUpdated` is variant 23, where the retired `SeatedTogether` stood, the next
/// after `NodeStopped` (22): the update's lifecycle op (its subject is the holder the seat moved from)
/// and every written seat (its cohort, the successor's exact birth, the committed epoch). What must
/// NOT happen: a field moved, a variant inserted before the end.
#[test]
fn primary_updated_matches_the_frozen_wire_schema() {
    let node_hex = "0c 303472616a303970337a7037";
    // 17 = variant 23; op: 0162 01 016f {node} 0c79727a34396a333033796834 09"m.admin.1" 01; seats: 02 | MeshPrimary=1 { 016d }
    // birth 016d {node} 0c79727a34396a333033796834 epoch 04 | FabricPrimary=0 birth 016d {node} 0c79727a34396a333033796834 epoch 09.
    let hex = format!("14 0162 01 016f {node_hex} 0c79727a34396a333033796834 09 6d2e61646d696e2e31 01  02 01 016d 016d {node_hex} 0c79727a34396a333033796834 04 00 016d {node_hex} 0c79727a34396a333033796834 09");
    let frame = primary_updated(4, 9);
    let expected = golden(&frame, &bytes(&hex));
    assert_eq!(frame.golden_encode(), expected);
    assert_roundtrip(&frame, &expected);
}

/// CONTRACT (the same edit): `PrimaryUpdating` is variant 24, the next after `PrimaryUpdated` (23),
/// with the same fields: the op naming the current holder and every seat that is updating, each at
/// the epoch it would take, naming the successor's exact birth. What must NOT happen: a field moved,
/// a variant inserted before the end.
#[test]
fn primary_updating_matches_the_frozen_wire_schema() {
    let node_hex = "0c 303472616a303970337a7037";
    let frame = Frame::PrimaryUpdating { op: op(), seats: both(5, 9) };
    let hex = format!("15 0162 01 016f {node_hex} 0c79727a34396a333033796834 09 6d2e61646d696e2e31 01  02 01 016d 016d {node_hex} 0c79727a34396a333033796834 05 00 016d {node_hex} 0c79727a34396a333033796834 09");
    let expected = golden(&frame, &bytes(&hex));
    assert_eq!(frame.golden_encode(), expected);
    assert_roundtrip(&frame, &expected);
    assert_eq!(primary_updated(1, 1).type_position(), 20, "PrimaryUpdated keeps its position");
}

/// CONTRACT (proposal Part C2, Q1: "the one row of C1 commits both seats to NA2 at their next epochs,
/// which makes NA2 mesh-primary and fabric-primary at the same boundary"): the seats one update
/// committed are held together under one lock, so no reader sees one moved and the other not; a stale
/// update moves neither; and a primary replays them as the one `PrimaryUpdated` while both are still
/// the records it holds (two `Seated` frames would re-open the window between the two installs at a
/// neighbour that came up).
#[test]
fn a_seat_book_holds_seats_together_and_replays_them_together() {
    let book = SeatBook::default();
    book.take(Seat::MeshPrimary, &holder(3));
    book.take(Seat::FabricPrimary, &holder(8));
    assert!(book.take_together(&op(), &both(4, 9)).iter().all(|t| matches!(t, SeatTaken::Held { .. })));
    assert_eq!((book.mesh("m"), book.fabric()), (Some(holder(4)), Some(holder(9))));
    assert_eq!(book.held_frames().iter().map(|f| f.golden_encode()).collect::<Vec<_>>(), vec![primary_updated(4, 9).golden_encode()], "both seats replay as the one update");
    // A later record of one seat supersedes the update: the records replay one by one again.
    book.take(Seat::FabricPrimary, &holder(10));
    assert_eq!(book.held_frames().iter().map(|f| f.golden_encode()).collect::<Vec<_>>(), vec![Frame::Seated { seat: Seat::MeshPrimary, holder: holder(4) }.golden_encode(), Frame::Seated { seat: Seat::FabricPrimary, holder: holder(10) }.golden_encode()]);
    // An older update moves neither seat.
    assert!(book.take_together(&op(), &both(2, 2)).iter().all(|t| matches!(t, SeatTaken::Refused { .. })));
    assert_eq!((book.mesh("m"), book.fabric()), (Some(holder(4)), Some(holder(10))));
}
