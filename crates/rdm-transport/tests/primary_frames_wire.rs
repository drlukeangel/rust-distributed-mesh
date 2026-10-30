//! Fixed bytes for the primary frames on the topology channels (R-A2; R-W1: postcard, positional,
//! append-only), and the view that holds the primary records. These literals lock the type names and the data field order independently of codec
//! round trips. Never regenerate an expectation from the Rust serializer to make a schema change
//! pass: a changed byte here is a moved wire format and needs a rebirth.

use crate::gossip_wire::{WireGolden, golden, assert_roundtrip};

use rdm_core::{IncarnationId, NodeId, Primary, PrimaryRole};
use rdm_transport::topology::{Frame, PrimaryView, RoleTaken};

fn bytes(hex: &str) -> Vec<u8> {
    let hex: String = hex.split_whitespace().collect();
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

fn node() -> NodeId {
    NodeId::parse("04raj09p3zp7").unwrap()
}

fn primary(revision: u64) -> Primary {
    Primary { mesh: "m".into(), node_id: node(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), revision }
}

/// CONTRACT: `Concern` is the reserved type after `FabricStatus` (7) (its literal data golden keeps its
/// historical enum byte 09); a role is its own enum (`MeshPrimary` 0, `FabricPrimary` 1); a primary is
/// its mesh, NodeId, incarnation and revision, in that order. What must NOT happen: a field moved, a
/// variant inserted before the end, or a self-describing encoding.
#[test]
fn primary_frames_match_the_frozen_wire_schema() {
    let node_hex = "0c 303472616a303970337a7037";
    let fixtures = [
        (Frame::MeshStatus { mesh: "m".into(), status: "s".into(), publisher: "p".into(), forwarded_by: None, changed_at_rafka_ms: 1 }, "06 016d 0173 0170 00 01".to_string()),
        (Frame::FabricStatus { fabric: rdm_core::FabricId::parse("04raj09p3zp7").unwrap(), status: "s".into(), publisher: "p".into(), forwarded_by: None, changed_at_rafka_ms: 1 }, "07 0c303472616a303970337a7037 0173 0170 00 01".to_string()),
        (Frame::Concern { role: PrimaryRole::FabricPrimary, node_id: node(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap(), observer: "o".into() }, format!("09 01 {node_hex} 0c79727a34396a333033796834 016f")),
    ];
    for (frame, hex) in fixtures {
        let expected = golden(&frame, &bytes(&hex));
        assert_eq!(frame.golden_encode(), expected, "{frame:?}");
        assert_roundtrip(&frame, &expected);
    }
    // The frames that existed keep their positions.
    assert_eq!(Frame::FabricStatus { fabric: rdm_core::FabricId::parse("04raj09p3zp7").unwrap(), status: "s".into(), publisher: "p".into(), forwarded_by: None, changed_at_rafka_ms: 1 }.type_position(), 7);
}

/// CONTRACT: a primary record is replaced only by one that supersedes it, and a repeat changes nothing.
#[test]
fn the_view_holds_the_superseding_primary_record() {
    let primary_view = PrimaryView::default();
    assert_eq!(primary_view.take(PrimaryRole::FabricPrimary, &primary(1)), RoleTaken::Held { previous: None });
    assert_eq!(primary_view.take(PrimaryRole::FabricPrimary, &primary(1)), RoleTaken::Same);
    assert_eq!(primary_view.take(PrimaryRole::FabricPrimary, &primary(3)), RoleTaken::Held { previous: Some(primary(1)) });
    assert_eq!(primary_view.take(PrimaryRole::FabricPrimary, &primary(2)), RoleTaken::Refused { held: primary(3) }, "an older revision never replaces");
    assert_eq!(primary_view.take(PrimaryRole::MeshPrimary, &primary(1)), RoleTaken::Held { previous: None }, "a mesh's primary is its own record");
    assert_eq!(primary_view.fabric(), Some(primary(3)));
    assert_eq!(primary_view.mesh("m"), Some(primary(1)));
}

/// CONTRACT: `NewFabricPrimary` is variant 14, appended after `NodeConnectionsDeleted` (13), a reserved position (R-G3): the committed
/// primary (mesh, NodeId, incarnation, revision), the endpoint key, the bound QUIC UDP address
/// (socket-address variant 0 is IPv4: four octets then the port as a varint) and the optional HTTP
/// API base. The two addresses are separate fields: neither is derived from the other.
#[test]
fn new_fabric_primary_matches_the_frozen_wire_schema() {
    let node_hex = "0c 303472616a303970337a7037";
    let frame = Frame::NewFabricPrimary { primary: primary(8), endpoint_id: "e".into(), transport_addr: "127.0.0.1:54474".parse().unwrap(), admin_api_base: Some("h".into()) };
    // 0e = variant 14; primary 016d {node} 0c79727a34396a333033796834 08; 0165 = "e"; 00 7f000001 caa903 = 127.0.0.1:54474; 01 0168 = Some("h").
    let hex = format!("0e 016d {node_hex} 0c79727a34396a333033796834 08 0165 00 7f000001 caa903 01 0168");
    let expected = golden(&frame, &bytes(&hex));
    assert_eq!(frame.golden_encode(), expected);
    assert_roundtrip(&frame, &expected);
    let none = Frame::NewFabricPrimary { primary: primary(8), endpoint_id: "e".into(), transport_addr: "127.0.0.1:80".parse().unwrap(), admin_api_base: None };
    assert_eq!(none.golden_encode(), golden(&none, &bytes(&format!("0e 016d {node_hex} 0c79727a34396a333033796834 08 0165 00 7f000001 50 00"))));
}


/// Both primaries of mesh `m`, the birth `m`/`node()`/`i` at their revisions.
fn both(mesh_revision: u64, fabric_revision: u64) -> Vec<(PrimaryRole, rdm_core::Primary)> {
    vec![(PrimaryRole::MeshPrimary, primary(mesh_revision)), (PrimaryRole::FabricPrimary, primary(fabric_revision))]
}

/// CONTRACT (lifecycles/node-primary-update.md "Both primaries": "the successor becomes both"): the
/// records one update names are held together under one lock, so no reader sees one moved and the
/// other not; and a stale update moves neither.
#[test]
fn the_view_holds_an_updates_primaries_together() {
    let primary_view = PrimaryView::default();
    primary_view.take(PrimaryRole::MeshPrimary, &primary(3));
    primary_view.take(PrimaryRole::FabricPrimary, &primary(8));
    assert!(primary_view.take_together(&both(4, 9)).iter().all(|t| matches!(t, RoleTaken::Held { .. })));
    assert_eq!((primary_view.mesh("m"), primary_view.fabric()), (Some(primary(4)), Some(primary(9))));
    primary_view.take(PrimaryRole::FabricPrimary, &primary(10));
    // An older update moves neither role.
    assert!(primary_view.take_together(&both(2, 2)).iter().all(|t| matches!(t, RoleTaken::Refused { .. })));
    assert_eq!((primary_view.mesh("m"), primary_view.fabric()), (Some(primary(4)), Some(primary(10))));
}
