//! i143.e4.s2: the generic connection model — held projection, incarnation targeting and the
//! effective route (`rdm_core::connections`).

use rdm_core::connections::{
    carrier_order, proxy_invalid, resolve, ApplyRefusal, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState,
    ConnectionsHeld, DirectRecovery, EffectiveRoute, NodeConnection, StaticIncarnations, INVALID_CARRIER_SUPERSEDED,
    INVALID_DESTINATION_SUPERSEDED,
};
use rdm_core::{IncarnationId, NodeId, NodeKind, PathName};
use std::sync::Arc;

fn path(kind: NodeKind, ordinal: u32) -> PathName {
    PathName::new("mesh1", kind, ordinal)
}

fn end(kind: NodeKind, ordinal: u32, incarnation: Option<&str>) -> ConnectionEnd {
    ConnectionEnd { name: path(kind, ordinal), node_id: NodeId::mint(), incarnation: incarnation.map(|i| IncarnationId::parse(&rdm_core::fixture_crockford60(i)).unwrap()) }
}

fn direct(source: ConnectionEnd, destination: ConnectionEnd, state: ConnectionState, at: u64) -> NodeConnection {
    let recovery = (state == ConnectionState::Failed).then_some(DirectRecovery { recovery_round: 1, attempt_ordinal: 1 });
    NodeConnection { id: rdm_core::ConnectionId::mint(), source, destination, kind: ConnectionKind::Direct, alpn: rdm_core::connections::ALPN.to_vec(), state, carrier: None, recovery, reason: None, logged_at_ms: at }
}

fn proxy(source: ConnectionEnd, carrier: ConnectionEnd, destination: ConnectionEnd, at: u64) -> NodeConnection {
    NodeConnection {
        id: rdm_core::ConnectionId::mint(),
        source,
        destination,
        kind: ConnectionKind::Proxy,
        alpn: rdm_core::connections::ALPN.to_vec(),
        state: ConnectionState::Connected,
        carrier: Some(carrier),
        recovery: None,
        reason: None,
        logged_at_ms: at,
    }
}

const RPC: CarrierPolicy = CarrierPolicy::Proxy { carrier_kind: NodeKind::RpcNode };

/// own = rpc.1, carrier = rpc.2, destination = rpc.3; the carrier's connection to the destination
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
    let connected = direct(own.clone(), dest.clone(), ConnectionState::Connected, 20);
    held.apply(connected.clone()).unwrap();
    assert_eq!(held.active_direct(&own.name, &dest.name), Some(Some(&connected)));

    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Failed, 30)).unwrap();
    assert_eq!(held.active_direct(&own.name, &dest.name), Some(None), "a Failed entry removes the connection");
    assert_eq!(
        held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 25)),
        Err(ApplyRefusal::Stale { held: 61, offered: 50 }),
        "an older connect delivered after the failure is refused"
    );
    assert_eq!(held.active_direct(&own.name, &dest.name), Some(None));

    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 40)).unwrap();
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Disconnected, 50)).unwrap();
    assert_eq!(held.active_direct(&own.name, &dest.name), Some(None), "a Disconnected entry removes the connection");
    assert_eq!(held.active_len(), 1, "only the carrier's active connection is resident");
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
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false });

    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 20)).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: true });

    let p = proxy(own.clone(), carrier.clone(), dest.clone(), 30);
    held.apply(p.clone()).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::ViaPeer { carrier: carrier.name.clone(), proxy: p });

    assert_eq!(
        resolve(&held, &own.name, &dest.name, CarrierPolicy::NoProxy).route,
        EffectiveRoute::Direct { known: true },
        "a protocol that does not proxy never takes the Proxy"
    );
}

/// CONTRACT: a complete holder that has never observed a Direct fact toward the destination
/// dials it (connections.md section 7); once a Direct Failed fact stands, the reconnect series owns
/// the retry (section 9.1) and no route is offered.
#[test]
fn a_complete_projection_with_no_direct_fact_dials_and_a_standing_failed_fact_does_not() {
    let (mut held, own, _carrier, dest) = estate();
    assert!(!held.holds_direct_fact(&own.name, &dest.name));
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false });
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Failed, 20)).unwrap();
    assert!(held.holds_direct_fact(&own.name, &dest.name));
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::NoActiveRoute);
}

/// CONTRACT (connections.md section 9.1: a reconnect series exists only when the latest Direct is
/// Failed; section 1.2: only Failed is this source's failure evidence): a Direct this node dropped
/// (`node.connection.delete`'s Disconnected, as node.stop writes) leaves no series
/// to retry it, so the next new call dials the destination again; a Failed one after it waits for its
/// series. Must NOT happen: a dropped Direct answering NoActiveRoute to every later call.
#[test]
fn a_dropped_direct_is_dialled_again_and_only_a_failed_one_waits_for_its_series() {
    let (mut held, own, _carrier, dest) = estate();
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 20)).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: true });
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Disconnected, 30)).unwrap();
    assert!(held.holds_direct_fact(&own.name, &dest.name));
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false }, "a drop is no failure: nothing else dials it again");
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Failed, 40)).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::NoActiveRoute, "the series owns a Failed Direct's retry");
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 50)).unwrap();
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Disconnected, 60)).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false }, "a later drop supersedes the series' Failed");
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Refused, 70)).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false }, "an acceptor's Refused is no failure of this source");
}

#[test]
fn an_incomplete_projection_answers_a_direct_dial_it_cannot_vouch_for() {
    let own = path(NodeKind::RpcNode, 1);
    let held = ConnectionsHeld::new();
    assert_eq!(resolve(&held, &own, &path(NodeKind::RpcNode, 3), RPC).route, EffectiveRoute::Direct { known: false });
}

/// The topology view names `own`, `carrier` and `dest` as `estate()` recorded them.
fn members(held: &mut ConnectionsHeld, ends: &[&ConnectionEnd]) -> Arc<StaticIncarnations> {
    let m = Arc::new(StaticIncarnations::default());
    for e in ends {
        m.set(e.node_id.clone(), e.name.clone(), e.incarnation.clone().unwrap());
    }
    held.set_topology_view(m.clone());
    m
}

#[test]
fn a_stale_destination_incarnation_is_superseded() {
    let (mut held, own, carrier, dest) = estate();
    let m = members(&mut held, &[&carrier, &dest]);
    let p = proxy(own.clone(), carrier.clone(), dest.clone(), 30);
    held.apply(p.clone()).unwrap();
    assert_eq!(proxy_invalid(&held, &p), None, "the topology view holds the birth the Proxy recorded");
    // The destination restarts: the topology view now holds process d2 at its path.
    m.set(dest.node_id.clone(), dest.name.clone(), IncarnationId::parse(&rdm_core::fixture_crockford60("d2")).unwrap());
    assert_eq!(proxy_invalid(&held, &p), Some(INVALID_DESTINATION_SUPERSEDED));
    let r = resolve(&held, &own.name, &dest.name, RPC);
    assert_eq!(r.route, EffectiveRoute::Direct { known: false });
    assert_eq!(r.delete, Some((p, INVALID_DESTINATION_SUPERSEDED)), "the superseded Proxy is handed back for delete");
}

#[test]
fn a_stale_carrier_incarnation_is_superseded() {
    let (mut held, own, carrier, dest) = estate();
    let m = members(&mut held, &[&carrier, &dest]);
    let p = proxy(own.clone(), carrier.clone(), dest.clone(), 30);
    held.apply(p.clone()).unwrap();
    m.set(carrier.node_id.clone(), carrier.name.clone(), IncarnationId::parse(&rdm_core::fixture_crockford60("c2")).unwrap());
    assert_eq!(proxy_invalid(&held, &p), Some(INVALID_CARRIER_SUPERSEDED));
}

/// CONTRACT: the carrier's own Direct connection is the carrier's fact (connections.md section 8). A
/// source whose held graph does not hold it still keeps its Proxy valid; a Disconnected connection it
/// does hold changes nothing either, because the carried call, not this read, answers
/// `connection-lost`.
#[test]
fn a_proxy_is_valid_without_the_carriers_connection_in_the_sources_held_graph() {
    let (own, carrier, dest) = (end(NodeKind::RpcNode, 1, Some("o1")), end(NodeKind::RpcNode, 2, Some("c1")), end(NodeKind::RpcNode, 3, Some("d1")));
    let mut held = ConnectionsHeld::new();
    held.set_own_source(own.name.clone());
    held.mark_complete();
    members(&mut held, &[&carrier, &dest]);
    let p = proxy(own.clone(), carrier.clone(), dest.clone(), 30);
    held.apply(p.clone()).unwrap();
    assert_eq!(proxy_invalid(&held, &p), None);
    assert!(matches!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::ViaPeer { .. }));
    held.apply(direct(carrier.clone(), dest.clone(), ConnectionState::Disconnected, 40)).unwrap();
    assert_eq!(proxy_invalid(&held, &p), None);
}

/// CONTRACT: a Direct fact naming a destination process the topology view no longer holds is not the
/// current connection: routing dials instead of trusting it, and it deletes no Proxy.
#[test]
fn a_direct_fact_naming_a_superseded_destination_is_not_current() {
    let (mut held, own, carrier, dest) = estate();
    let m = members(&mut held, &[&carrier, &dest]);
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 20)).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: true });
    assert_eq!(held.active_directs_to(&dest.name).unwrap().len(), 2);
    m.set(dest.node_id.clone(), dest.name.clone(), IncarnationId::parse(&rdm_core::fixture_crockford60("d2")).unwrap());
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false }, "the fact names d1, the topology view holds d2");
    assert!(held.active_directs_to(&dest.name).unwrap().is_empty(), "no connection names the current destination");
}

/// CONTRACT (connections.md §9.1: "A reconnect series exists when the latest Direct record for this
/// source and destination is Failed for the current destination incarnation"; §5.1: "A latest Failed
/// Direct is the one state a reconnect series stands on"; §13 I: "Stale evidence is never used for a new
/// process birth"): a latest Direct Failed naming a destination process the topology view no longer
/// holds (another birth of the node, or a node that left its path) stands no series, so a new call
/// dials the current birth. Must NOT happen: `NoActiveRoute` from a Failed fact about a gone birth.
#[test]
fn a_failed_direct_naming_a_superseded_destination_stands_no_series_and_the_call_dials() {
    let (mut held, own, carrier, dest) = estate();
    let m = members(&mut held, &[&carrier, &dest]);
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Failed, 20)).unwrap();
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::NoActiveRoute, "the series of the current birth owns the retry");
    m.set(dest.node_id.clone(), dest.name.clone(), IncarnationId::parse(&rdm_core::fixture_crockford60("d2")).unwrap());
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false }, "the Failed fact names d1, the topology view holds d2");
    m.set(dest.node_id.clone(), dest.name.clone(), dest.incarnation.clone().unwrap());
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::NoActiveRoute);
    m.depart(dest.node_id.clone(), dest.name.clone());
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false }, "the node the Failed fact names left its path");
}

/// CONTRACT: a destination that deleted is no current birth: the Direct fact toward it
/// is not the current connection and a Proxy to it is targeted by name.
#[test]
fn a_direct_fact_and_a_proxy_naming_a_deleted_destination_are_not_current() {
    let (mut held, own, carrier, dest) = estate();
    let m = members(&mut held, &[&carrier, &dest]);
    held.apply(direct(own.clone(), dest.clone(), ConnectionState::Connected, 20)).unwrap();
    let p = proxy(own.clone(), carrier.clone(), dest.clone(), 30);
    held.apply(p.clone()).unwrap();
    assert_eq!(held.active_len(), 2);
    m.depart(dest.node_id.clone(), dest.name.clone());
    assert_eq!(held.active_len(), 0, "no Direct fact names a current destination");
    assert_eq!(proxy_invalid(&held, &p), Some(INVALID_DESTINATION_SUPERSEDED));
    assert_eq!(resolve(&held, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false });
}

#[test]
fn a_valid_proxy_through_a_carrier_kind_the_protocol_forbids_is_skipped_not_disconnected() {
    let (own, dest) = (end(NodeKind::RpcNode, 1, Some("o1")), end(NodeKind::RpcNode, 3, Some("d1")));
    let admin = end(NodeKind::NodeAdmin, 1, Some("a1"));
    let mut held = ConnectionsHeld::new();
    held.set_own_source(own.name.clone());
    held.mark_complete();
    held.apply(direct(admin.clone(), dest.clone(), ConnectionState::Connected, 10)).unwrap();
    held.apply(proxy(own.clone(), admin, dest.clone(), 20)).unwrap();
    let r = resolve(&held, &own.name, &dest.name, RPC);
    assert_eq!(r.route, EffectiveRoute::Direct { known: false });
    assert_eq!(r.delete, None);
}

#[test]
fn another_nodes_proxy_is_never_held() {
    let (mut held, _own, carrier, dest) = estate();
    let other = end(NodeKind::RpcNode, 4, Some("x1"));
    held.apply(proxy(other.clone(), carrier, dest.clone(), 30)).unwrap();
    assert_eq!(resolve(&held, &other.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: false });
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

/// CONTRACT (connections.md §6, Q227): the carriers a source asks are the live nodes of its mesh, from
/// the topology it holds, of a kind its protocol lets carry, itself and the destination left out, in
/// path order from `spread(source, destination, destination incarnation) % len`, wrapping; no
/// connection fact is read. The order is the same at every call, and two sources spread over the
/// carriers. A protocol with proxy false asks none. Must NOT happen: a carrier of another mesh,
/// of a kind the protocol does not let carry, the source itself or the destination asked.
#[test]
fn carriers_are_asked_from_the_topology_in_path_order_spread_by_source() {
    let dest = path(NodeKind::RpcNode, 9);
    let incarnation = IncarnationId::parse(&rdm_core::fixture_crockford60("d2")).unwrap();
    let own = path(NodeKind::RpcNode, 1);
    let topology = vec![path(NodeKind::RpcNode, 4), path(NodeKind::RpcNode, 2), own.clone(), dest.clone(), path(NodeKind::RpcNode, 3), PathName::new("mesh2", NodeKind::RpcNode, 5), path(NodeKind::NodeAdmin, 1)];
    let order = carrier_order(&own, &dest, &incarnation, true, &[NodeKind::RpcNode], topology.clone());
    let in_path_order = [path(NodeKind::RpcNode, 2), path(NodeKind::RpcNode, 3), path(NodeKind::RpcNode, 4)];
    let at = (rdm_core::connections::spread(&own.to_string(), &dest.to_string(), incarnation.as_str()) % 3) as usize;
    let expected: Vec<PathName> = (0..3).map(|k| in_path_order[(at + k) % 3].clone()).collect();
    assert_eq!(order, expected, "the mesh's carriers in path order from the spread, wrapping");
    assert_eq!(carrier_order(&own, &dest, &incarnation, true, &[NodeKind::RpcNode], topology.clone()), order, "the order is deterministic");
    let firsts: std::collections::BTreeSet<PathName> = (1..=8u32)
        .map(|o| carrier_order(&PathName::new("mesh1", NodeKind::RpcNode, 100 + o), &dest, &incarnation, true, &[NodeKind::RpcNode], topology.clone())[0].clone())
        .collect();
    assert!(firsts.len() > 1, "sources spread over the carriers: {firsts:?}");
    assert!(carrier_order(&own, &dest, &incarnation, false, &[NodeKind::RpcNode], topology.clone()).is_empty(), "a protocol with proxy false asks no carrier");
    assert!(carrier_order(&own, &dest, &incarnation, true, &[NodeKind::Gateway], topology).is_empty(), "no node of a kind the protocol lets carry");
}
