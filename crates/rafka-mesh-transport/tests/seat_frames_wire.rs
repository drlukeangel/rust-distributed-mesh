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
    SeatHolder { mesh: "m".into(), node_id: node(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap(), epoch }
}

/// CONTRACT: `Seated` is variant 8 and `Concern` variant 9 of the membership frame, appended after
/// `FabricStatus` (7); a seat is its own enum (`MeshPrimary` 0, `FabricPrimary` 1); a holder is its
/// mesh, NodeId, incarnation and epoch, in that order. What must NOT happen: a field moved, a
/// variant inserted before the end, or a self-describing encoding.
#[test]
fn seat_frames_match_the_frozen_wire_schema() {
    // 08 = Seated; 01 = FabricPrimary; 01 6d = "m"; 0c + 12 ASCII bytes = the NodeId; 0c <fixture("i")> = the incarnation; 02 = epoch.
    let node_hex = "0c 303472616a303970337a7037";
    let fixtures = [
        (Frame::Seated { seat: Seat::FabricPrimary, holder: holder(2) }, format!("08 01 016d {node_hex} 0c79727a34396a333033796834 02")),
        (Frame::Seated { seat: Seat::MeshPrimary, holder: holder(300) }, format!("08 00 016d {node_hex} 0c79727a34396a333033796834 ac02")),
        (Frame::Concern { seat: Seat::FabricPrimary, node_id: node(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap(), observer: "o".into() }, format!("09 01 {node_hex} 0c79727a34396a333033796834 016f")),
    ];
    for (frame, hex) in fixtures {
        assert_eq!(frame.encode(), bytes(&hex), "{frame:?}");
        assert_eq!(Frame::decode(&bytes(&hex)).unwrap().encode(), bytes(&hex), "{frame:?} decodes and re-encodes to the same bytes");
    }
    // The frames that existed keep their positions.
    assert_eq!(Frame::FabricStatus { fabric: rafka_mesh_entity::FabricId::parse("04raj09p3zp7").unwrap(), status: "s".into(), publisher: "p".into(), forwarded_by: None, changed_at_rafka_ms: 1 }.encode()[0], 7);
    // A seat this build does not know is refused by name, never read as a prefix.
    let e = Frame::decode(&bytes("08 02 016d 0c 303472616a303970337a7037 0c79727a34396a333033796834 02")).unwrap_err().to_string();
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

/// CONTRACT: `NewFabricPrimary` is variant 17, appended after `MeshLeft` (16): the committed
/// holder (mesh, NodeId, incarnation, epoch), the endpoint key, the bound QUIC UDP address
/// (socket-address variant 0 is IPv4: four octets then the port as a varint) and the optional HTTP
/// API base. The two addresses are separate fields: neither is derived from the other.
#[test]
fn new_fabric_primary_matches_the_frozen_wire_schema() {
    let node_hex = "0c 303472616a303970337a7037";
    let frame = Frame::NewFabricPrimary { holder: holder(8), endpoint_id: "e".into(), transport_addr: "127.0.0.1:54474".parse().unwrap(), admin_api_base: Some("h".into()) };
    // 11 = variant 17; holder 016d {node} 0c79727a34396a333033796834 08; 0165 = "e"; 00 7f000001 caa903 = 127.0.0.1:54474; 01 0168 = Some("h").
    let hex = format!("11 016d {node_hex} 0c79727a34396a333033796834 08 0165 00 7f000001 caa903 01 0168");
    assert_eq!(frame.encode(), bytes(&hex));
    assert_eq!(Frame::decode(&bytes(&hex)).unwrap().encode(), bytes(&hex));
    let none = Frame::NewFabricPrimary { holder: holder(8), endpoint_id: "e".into(), transport_addr: "127.0.0.1:80".parse().unwrap(), admin_api_base: None };
    assert_eq!(none.encode(), bytes(&format!("11 016d {node_hex} 0c79727a34396a333033796834 08 0165 00 7f000001 50 00")));
    assert_eq!(Frame::MeshLeft { mesh_id: rafka_mesh_entity::MeshId::parse("04raj09p3zp7").unwrap(), build_id: "b".into(), attempt: 1, operation: "o".into(), receipt_manifest: "m".into(), publisher: "p".into(), event_at_rafka_ms: 1, forwarded_by: None }.encode()[0], 16);
}

/// CONTRACT: the contacts an announcement carries attach to the held fabric holder's exact birth
/// and epoch only; a delayed announcement of an earlier holder attaches to nothing.
#[test]
fn a_new_fabric_primarys_contacts_attach_only_to_the_holder_the_book_holds() {
    use rafka_mesh_transport::membership::FabricContacts;
    let book = SeatBook::default();
    let contacts = |p: u16| FabricContacts { endpoint_id: "e".into(), transport_addr: format!("127.0.0.1:{p}").parse().unwrap(), admin_api_base: Some(format!("http://127.0.0.1:{}", p + 1)) };
    assert!(!book.attach_fabric_contacts(&holder(8), contacts(1)), "no record held: nothing to attach to");
    book.take(Seat::FabricPrimary, &holder(8));
    assert!(book.attach_fabric_contacts(&holder(8), contacts(10)));
    assert_eq!(book.fabric_contacts(), Some(contacts(10)));
    assert!(!book.attach_fabric_contacts(&holder(7), contacts(20)), "a delayed announcement of an earlier epoch attaches to nothing");
    book.take(Seat::FabricPrimary, &holder(9));
    assert_eq!(book.fabric_contacts(), None, "the contacts belonged to the epoch-8 holder, not the one that replaced it");
}

fn op() -> rafka_mesh_entity::LifecycleOp {
    rafka_mesh_entity::LifecycleOp { build_id: "b".into(), attempt: 1, operation: "o".into(), node_id: node(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap(), name: "m.admin.1".parse().unwrap(), event_at_rafka_ms: 1 }
}

/// Both seats of mesh `m`, committed to the birth `m`/`node()`/`i` at their epochs.
fn both(mesh_epoch: u64, fabric_epoch: u64) -> Vec<rafka_mesh_entity::seat::transfer::CommittedSeat> {
    use rafka_mesh_entity::seat::transfer::{CommittedSeat, HandoverBirth, Seat as TransferSeat};
    let birth = HandoverBirth { mesh: "m".into(), node_id: node(), incarnation: IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("i")).unwrap() };
    vec![CommittedSeat { seat: TransferSeat::MeshPrimary { mesh: "m".into() }, holder: birth.clone(), epoch: mesh_epoch }, CommittedSeat { seat: TransferSeat::FabricPrimary, holder: birth, epoch: fabric_epoch }]
}

fn primary_updated(mesh_epoch: u64, fabric_epoch: u64) -> Frame {
    Frame::PrimaryUpdated { op: op(), seats: both(mesh_epoch, fabric_epoch) }
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md Part C3 edit 6:
/// "two announcements on the mesh channel and the backbone, 'primary updating' before the row and
/// 'primary updated' after `node.primary.updated`, each naming operation, cohort, both exact births
/// and the epochs"): `PrimaryUpdated` is variant 23, where the retired `SeatedTogether` stood, the next
/// after `NodeStopped` (22): the update's lifecycle op (its subject is the holder the seat moved from)
/// and every committed seat (its cohort, the successor's exact birth, the committed epoch). What must
/// NOT happen: a field moved, a variant inserted before the end.
#[test]
fn primary_updated_matches_the_frozen_wire_schema() {
    let node_hex = "0c 303472616a303970337a7037";
    // 17 = variant 23; op: 0162 01 016f {node} 0c79727a34396a333033796834 09"m.admin.1" 01; seats: 02 | MeshPrimary=1 { 016d }
    // birth 016d {node} 0c79727a34396a333033796834 epoch 04 | FabricPrimary=0 birth 016d {node} 0c79727a34396a333033796834 epoch 09.
    let hex = format!("17 0162 01 016f {node_hex} 0c79727a34396a333033796834 09 6d2e61646d696e2e31 01  02 01 016d 016d {node_hex} 0c79727a34396a333033796834 04 00 016d {node_hex} 0c79727a34396a333033796834 09");
    assert_eq!(primary_updated(4, 9).encode(), bytes(&hex));
    assert_eq!(Frame::decode(&bytes(&hex)).unwrap().encode(), bytes(&hex));
}

/// CONTRACT (the same edit): `PrimaryUpdating` is variant 24, the next after `PrimaryUpdated` (23),
/// with the same fields: the op naming the current holder and every seat that is updating, each at
/// the epoch it would take, naming the successor's exact birth. What must NOT happen: a field moved,
/// a variant inserted before the end.
#[test]
fn primary_updating_matches_the_frozen_wire_schema() {
    let node_hex = "0c 303472616a303970337a7037";
    let frame = Frame::PrimaryUpdating { op: op(), seats: both(5, 9) };
    let hex = format!("18 0162 01 016f {node_hex} 0c79727a34396a333033796834 09 6d2e61646d696e2e31 01  02 01 016d 016d {node_hex} 0c79727a34396a333033796834 05 00 016d {node_hex} 0c79727a34396a333033796834 09");
    assert_eq!(frame.encode(), bytes(&hex));
    assert_eq!(Frame::decode(&bytes(&hex)).unwrap().encode(), bytes(&hex));
    assert_eq!(primary_updated(1, 1).encode()[0], 23, "PrimaryUpdated keeps its position");
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
    assert!(book.take_together(&op(), &both(4, 9), false).iter().all(|t| matches!(t, SeatTaken::Held { .. })));
    assert_eq!((book.mesh("m"), book.fabric()), (Some(holder(4)), Some(holder(9))));
    assert_eq!(book.held_frames().iter().map(Frame::encode).collect::<Vec<_>>(), vec![primary_updated(4, 9).encode()], "both seats replay as the one update");
    // A later record of one seat supersedes the update: the records replay one by one again.
    book.take(Seat::FabricPrimary, &holder(10));
    assert_eq!(book.held_frames().iter().map(Frame::encode).collect::<Vec<_>>(), vec![Frame::Seated { seat: Seat::MeshPrimary, holder: holder(4) }.encode(), Frame::Seated { seat: Seat::FabricPrimary, holder: holder(10) }.encode()]);
    // An older update moves neither seat.
    assert!(book.take_together(&op(), &both(2, 2), false).iter().all(|t| matches!(t, SeatTaken::Refused { .. })));
    assert_eq!((book.mesh("m"), book.fabric()), (Some(holder(4)), Some(holder(10))));
}
