//! i143.e4.s2: the generic connection model — held projection, incarnation fencing and the
//! effective route (`rafka_mesh_entity::connections`).

use rafka_mesh_entity::connections::{
    proxy_invalid, resolve, select_carrier, ApplyRefusal, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState,
    ConnectionsHeld, DirectRecovery, EffectiveRoute, NodeConnection, INVALID_CARRIER_EDGE_LOST, INVALID_CARRIER_SUPERSEDED,
    INVALID_DESTINATION_SUPERSEDED,
};
use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind, PathName};

fn path(kind: NodeKind, ordinal: u32) -> PathName {
    PathName { mesh: "mesh1".into(), kind, ordinal }
}

fn end(kind: NodeKind, ordinal: u32, incarnation: Option<&str>) -> ConnectionEnd {
    ConnectionEnd { name: path(kind, ordinal), node_id: NodeId::mint(), incarnation: incarnation.map(|i| IncarnationId(i.into())) }
}

fn direct(source: ConnectionEnd, destination: ConnectionEnd, state: ConnectionState, at: u64) -> NodeConnection {
    let recovery = (state == ConnectionState::Failed).then_some(DirectRecovery { recovery_epoch: 1, attempt_ordinal: 1 });
    NodeConnection { source, destination, kind: ConnectionKind::Direct, state, carrier: None, recovery, reason: None, logged_at_ms: at }
}

fn proxy(source: ConnectionEnd, carrier: ConnectionEnd, destination: ConnectionEnd, at: u64) -> NodeConnection {
    NodeConnection {
        source,
        destination,
        kind: ConnectionKind::Proxy,
        state: ConnectionState::Connected,
        carrier: Some(carrier),
        recovery: None,
        reason: None,
        logged_at_ms: at,
    }
}

const RPC: CarrierPolicy = CarrierPolicy::Forwardable { carrier_kind: NodeKind::RpcNode };

/// own = rpc.1, carrier = rpc.2, destination = rpc.3; the carrier's edge to the destination
/// names destination process `d1` and carrier process `c1`.
fn estate() -> (ConnectionsHeld, ConnectionEnd, ConnectionEnd, ConnectionEnd) {
    let (own, carrier, dest) = (end(NodeKind::RpcNode, 1, Some("o1")), end(NodeKind::RpcNode, 2, Some("c1")), end(NodeKind::RpcNode, 3, Some("d1")));
    let mut held = ConnectionsHeld::new();
    held.set_own_source(own.name.clone());
    held.mark_complete();
    held.apply(direct(carrier.clone(), dest.clone(), ConnectionState::Connected, 10)).unwrap();
    (held, own, carrier, dest)
}

#[test]
fn a_disconnected_or_failed_direct_never_stays_resident_and_an_older_connect_never_revives_it() {
    let (mut held, own, _carrier, dest) = estate();
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 20)).unwrap();
    assert_eq!(held.active_direct(&own.name, &dest.name), Some(Some(&direct(own.clone(), dest.clone(), ConnectionState::Connected, 20))));

    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Failed, 30)).unwrap();
    assert_eq!(held.active_direct(&own.name, &dest.name), Some(None), "a Failed entry removes the edge");
    assert_eq!(
        held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 25)),
        Err(ApplyRefusal::Stale { held: 61, offered: 50 }),
        "an older connect delivered after the failure is refused"
    );
    assert_eq!(held.active_direct(&own.name, &dest.name), Some(None));

    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 40)).unwrap();
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Disconnected, 50)).unwrap();
    assert_eq!(held.active_direct(&own.name, &dest.name), Some(None), "a Disconnected entry removes the edge");
    assert_eq!(held.active_len(), 1, "only the carrier's active edge is resident");
    assert_eq!(held.keys(), 2, "each key keeps its stamp, resident or not");
}

#[test]
fn at_one_instant_a_drop_wins_over_a_connect_whichever_arrives_first() {
    let (mut held, own, _carrier, dest) = estate();
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Disconnected, 20)).unwrap();
    assert!(held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 20)).is_err());
    assert_eq!(held.active_direct(&own.name, &dest.name), Some(None));
}

#[test]
fn the_route_prefers_a_valid_proxy_then_an_active_direct_then_none() {
    let (mut held, own, carrier, dest) = estate();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::NoActiveRoute);

    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 20)).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: true });

    let p = proxy(own.clone(), carrier.clone(), dest.clone(), 30);
    held.apply(p.clone()).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::ViaPeer { carrier: carrier.name.clone(), proxy: p });

    assert_eq!(
        resolve(&held, &own.name, &dest.name, CarrierPolicy::NotForwardable).route,
        EffectiveRoute::Direct { known: true },
        "a protocol that is not forwardable never takes the Proxy"
    );
}

#[test]
fn an_incomplete_projection_answers_a_direct_dial_it_cannot_vouch_for() {
    let own = path(NodeKind::RpcNode, 1);
    let held = ConnectionsHeld::new();
    assert_eq!(resolve(&held, &own, &path(NodeKind::RpcNode, 3), RPC).route, EffectiveRoute::Direct { known: false });
    assert!(select_carrier(&held, &own, &path(NodeKind::RpcNode, 3), RPC).unwrap_err().contains("not complete"));
}

#[test]
fn a_stale_destination_incarnation_is_fenced() {
    let (mut held, own, carrier, dest) = estate();
    let p = proxy(own.clone(), carrier.clone(), dest.clone(), 30);
    held.apply(p.clone()).unwrap();
    // The destination restarts: the carrier's edge now names process d2.
    let restarted = ConnectionEnd { incarnation: Some(IncarnationId("d2".into())), ..dest.clone() };
    held.apply(direct(carrier.clone(), restarted, ConnectionState::Connected, 40)).unwrap();
    assert_eq!(proxy_invalid(&held, &p), Some(INVALID_DESTINATION_SUPERSEDED));
    let r = resolve(&held, &own.name, &dest.name, RPC);
    assert_eq!(r.route, EffectiveRoute::NoActiveRoute);
    assert_eq!(r.retire, Some((p, INVALID_DESTINATION_SUPERSEDED)), "the superseded Proxy is handed back for retirement");
}

#[test]
fn a_stale_carrier_incarnation_is_fenced() {
    let (mut held, own, carrier, dest) = estate();
    let p = proxy(own.clone(), carrier.clone(), dest.clone(), 30);
    held.apply(p.clone()).unwrap();
    let restarted = ConnectionEnd { incarnation: Some(IncarnationId("c2".into())), ..carrier.clone() };
    held.apply(direct(restarted, dest.clone(), ConnectionState::Connected, 40)).unwrap();
    assert_eq!(proxy_invalid(&held, &p), Some(INVALID_CARRIER_SUPERSEDED));
}

#[test]
fn a_proxy_whose_carrier_lost_its_edge_is_invalid() {
    let (mut held, own, carrier, dest) = estate();
    let p = proxy(own.clone(), carrier.clone(), dest.clone(), 30);
    held.apply(p.clone()).unwrap();
    held.apply(direct(carrier.clone(), dest.clone(), ConnectionState::Disconnected, 40)).unwrap();
    assert_eq!(proxy_invalid(&held, &p), Some(INVALID_CARRIER_EDGE_LOST));
}

#[test]
fn a_valid_proxy_through_a_carrier_kind_the_protocol_forbids_is_skipped_not_retired() {
    let (own, dest) = (end(NodeKind::RpcNode, 1, Some("o1")), end(NodeKind::RpcNode, 3, Some("d1")));
    let admin = end(NodeKind::NodeAdmin, 1, Some("a1"));
    let mut held = ConnectionsHeld::new();
    held.set_own_source(own.name.clone());
    held.mark_complete();
    held.apply(direct(admin.clone(), dest.clone(), ConnectionState::Connected, 10)).unwrap();
    held.apply(proxy(own.clone(), admin, dest.clone(), 20)).unwrap();
    let r = resolve(&held, &own.name, &dest.name, RPC);
    assert_eq!(r.route, EffectiveRoute::NoActiveRoute);
    assert_eq!(r.retire, None);
}

#[test]
fn another_nodes_proxy_is_never_held() {
    let (mut held, _own, carrier, dest) = estate();
    let other = end(NodeKind::RpcNode, 4, Some("x1"));
    held.apply(proxy(other.clone(), carrier, dest.clone(), 30)).unwrap();
    assert_eq!(resolve(&held, &other.name, &dest.name, RPC).route, EffectiveRoute::NoActiveRoute);
}

#[test]
fn malformed_entries_are_refused_by_name() {
    let (mut held, own, carrier, dest) = estate();
    let mut failed_proxy = proxy(own.clone(), carrier, dest.clone(), 30);
    failed_proxy.state = ConnectionState::Failed;
    assert!(matches!(held.apply(failed_proxy), Err(ApplyRefusal::Shape(s)) if s.contains("Failed is a Direct state only")));
    let mut no_recovery = direct(own, dest, ConnectionState::Failed, 40);
    no_recovery.recovery = None;
    assert!(matches!(held.apply(no_recovery), Err(ApplyRefusal::Shape(s)) if s.contains("recovery")));
}

#[test]
fn a_carrier_is_chosen_from_edges_naming_the_current_destination_process_spread_by_source() {
    let dest = end(NodeKind::RpcNode, 9, Some("d2"));
    let mut held = ConnectionsHeld::new();
    held.mark_complete();
    for (ordinal, dest_incarnation, at) in [(2, "d2", 20), (3, "d2", 21), (4, "d1", 5)] {
        let d = ConnectionEnd { incarnation: Some(IncarnationId(dest_incarnation.into())), ..dest.clone() };
        held.apply(direct(end(NodeKind::RpcNode, ordinal, Some("c")), d, ConnectionState::Connected, at)).unwrap();
    }
    held.apply(direct(end(NodeKind::RpcNode, 5, None), dest.clone(), ConnectionState::Connected, 30)).unwrap();
    let own = path(NodeKind::RpcNode, 1);
    let a = select_carrier(&held, &own, &dest.name, RPC).unwrap().unwrap();
    assert!([2, 3].contains(&a.carrier.ordinal), "a carrier on the current destination process: {}", a.carrier);
    assert_eq!(select_carrier(&held, &own, &dest.name, RPC).unwrap().unwrap(), a, "the choice is deterministic");
    assert!(select_carrier(&held, &own, &dest.name, CarrierPolicy::NotForwardable).is_err());
    let lonely = path(NodeKind::RpcNode, 8);
    assert_eq!(select_carrier(&held, &own, &lonely, RPC), Ok(None));
}
