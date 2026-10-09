//! Fixed bytes for the seat frames on the membership channels (R-A2; R-W1: postcard, positional,
//! append-only). These literals lock the discriminants and the field order independently of codec
//! round trips. Never regenerate an expectation from the Rust serializer to make a schema change
//! pass: a changed byte here is a moved wire format and needs a rebirth.

use rafka_mesh_entity::{IncarnationId, NodeId, Seat, SeatHolder};
use rafka_mesh_transport::membership::{Frame, SeatBook, SeatTaken};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn node() -> NodeId {
    NodeId::parse("04raj09p3zp7").unwrap()
}

fn holder(epoch: u64) -> SeatHolder {
    SeatHolder { mesh: "m".into(), node_id: node(), incarnation: IncarnationId("i".into()), epoch }
}

/// CONTRACT: `Seated` is variant 8 and `Concern` variant 9 of the membership frame, appended after
/// `FabricStatus` (7); a seat is its own enum (`MeshPrimary` 0, `FabricPrimary` 1); a holder is its
/// mesh, NodeId, incarnation and epoch, in that order. What must NOT happen: a field moved, a
/// variant inserted before the end, or a self-describing encoding.
#[test]
fn seat_frames_match_the_frozen_wire_schema() {
    // 08 = Seated; 01 = FabricPrimary; 01 6d = "m"; 0c + 12 ASCII bytes = the NodeId; 01 69 = "i"; 02 = epoch.
    let node_hex = "0c 303472616a303970337a7037";
    let fixtures = [
        (Frame::Seated { seat: Seat::FabricPrimary, holder: holder(2) }, format!("08 01 016d {node_hex} 0169 02")),
        (Frame::Seated { seat: Seat::MeshPrimary, holder: holder(300) }, format!("08 00 016d {node_hex} 0169 ac02")),
        (Frame::Concern { seat: Seat::FabricPrimary, node_id: node(), incarnation: IncarnationId("i".into()), observer: "o".into() }, format!("09 01 {node_hex} 0169 016f")),
    ];
    for (frame, hex) in fixtures {
        assert_eq!(frame.encode(), bytes(&hex), "{frame:?}");
        assert_eq!(Frame::decode(&bytes(&hex)).unwrap().encode(), bytes(&hex), "{frame:?} decodes and re-encodes to the same bytes");
    }
    // The frames that existed keep their positions.
    assert_eq!(Frame::FabricStatus { fabric: rafka_mesh_entity::FabricId::parse("04raj09p3zp7").unwrap(), status: "s".into(), publisher: "p".into(), forwarded_by: None, changed_at_rafka_ms: 1 }.encode()[0], 7);
    // A seat this build does not know is refused by name, never read as a prefix.
    let e = Frame::decode(&bytes("08 02 016d 0c 303472616a303970337a7037 0169 02")).unwrap_err().to_string();
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
    assert_eq!(book.take(Seat::FabricPrimary, &holder(2)), SeatTaken::Refused { held: holder(3) }, "an older epoch never replaces");
    assert_eq!(book.take(Seat::MeshPrimary, &holder(1)), SeatTaken::Held { previous: None }, "a mesh's seat is its own record");
    assert_eq!(book.fabric(), Some(holder(3)));
    assert_eq!(book.mesh("m"), Some(holder(1)));
    let replay: Vec<Vec<u8>> = book.held_frames().iter().map(Frame::encode).collect();
    assert_eq!(replay, vec![Frame::Seated { seat: Seat::MeshPrimary, holder: holder(1) }.encode(), Frame::Seated { seat: Seat::FabricPrimary, holder: holder(3) }.encode()]);
}
