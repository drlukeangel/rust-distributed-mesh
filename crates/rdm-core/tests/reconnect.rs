//! A source's Direct failure rows and the ordered Proxy delete (`rdm_core::reconnect`; connections.md
//! §7, §10; Q348: nothing schedules a dial from a Failed row).

use rdm_core::connections::{resolve, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rdm_core::reconnect::{first_failure, owed_direct_restored, DIRECT_RESTORED};
use rdm_core::{IncarnationId, NodeId};

fn rpc() -> CarrierPolicy {
    CarrierPolicy::Proxy { carrier_type: "node".into() }
}

fn end(ordinal: u32, incarnation: &str) -> ConnectionEnd {
    ConnectionEnd { mesh: "mesh1".into(), r#type: "node".into(), path: format!("{}.{}", "mesh1", "node"), name: (ordinal).to_string(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::parse(&rdm_core::fixture_crockford60(incarnation)).unwrap()) }
}

fn held_for(own: &ConnectionEnd) -> ConnectionsHeld {
    let mut h = ConnectionsHeld::new();
    h.set_own_source(own.node_id.clone());
    h.mark_complete();
    h
}

fn connected(source: &ConnectionEnd, destination: &ConnectionEnd, at: u64) -> NodeConnection {
    NodeConnection {
        id: rdm_core::ConnectionId::mint(),
        source: source.clone(),
        destination: destination.clone(),
        kind: ConnectionKind::Direct,
        alpn: rdm_core::connections::ALPN.to_vec(),
        state: ConnectionState::Connected,
        carrier: None,
        recovery: None,
        reason: None,
        logged_at_ms: at,
    }
}

/// own (rpc.1) failed to reach dest (rpc.3, process d1): the first Failed row of round 1, at 100.
fn failed_once(own: &ConnectionEnd, dest: &ConnectionEnd) -> NodeConnection {
    first_failure(own.clone(), dest.clone(), None, "connect refused", 100)
}

#[test]
fn a_new_incident_starts_a_new_round_at_ordinal_one() {
    let (own, dest) = (end(1, "o1"), end(3, "d1"));
    let latest = failed_once(&own, &dest);
    assert_eq!(latest.recovery.map(|r| (r.recovery_round, r.attempt_ordinal)), Some((1, 1)), "the first incident is round 1");
    let again = first_failure(own.clone(), dest.clone(), Some(&latest), "connect refused", 9000);
    assert_eq!(again.recovery.map(|r| (r.recovery_round, r.attempt_ordinal)), Some((2, 1)));
}

#[test]
fn cutback_to_direct_happens_only_after_the_proxy_disconnected_write_is_applied_and_a_failed_write_is_owed_again() {
    let (own, carrier, dest) = (end(1, "o1"), end(2, "c1"), end(3, "d1"));
    let mut h = held_for(&own);
    h.apply(connected(&carrier, &dest, 10)).unwrap();
    let proxy = NodeConnection {
        id: rdm_core::ConnectionId::mint(),
        source: own.clone(),
        destination: dest.clone(),
        kind: ConnectionKind::Proxy,
        alpn: rdm_core::connections::ALPN.to_vec(),
        state: ConnectionState::Connected,
        carrier: Some(carrier.clone()),
        recovery: None,
        reason: None,
        logged_at_ms: 20,
    };
    h.apply(proxy).unwrap();
    assert!(owed_direct_restored(&h, 25).is_empty(), "no Direct yet: nothing owed");

    h.apply(connected(&own, &dest, 30)).unwrap();
    assert!(matches!(resolve(&h, &own.node_id, &dest.node_id, rpc()).route, EffectiveRoute::ViaPeer { .. }), "the Proxy stays effective until deleted");
    let owed = owed_direct_restored(&h, 31);
    assert_eq!(owed.len(), 1);
    assert_eq!((owed[0].state, owed[0].reason.as_deref()), (ConnectionState::Disconnected, Some(DIRECT_RESTORED)));

    // The write fails (never applied): the delete is owed again, and the Proxy still routes.
    assert_eq!(owed_direct_restored(&h, 40).len(), 1);
    assert!(matches!(resolve(&h, &own.node_id, &dest.node_id, rpc()).route, EffectiveRoute::ViaPeer { .. }));

    // The write lands: the Proxy is deleted and new invocations go Direct.
    h.apply(owed_direct_restored(&h, 50).remove(0)).unwrap();
    assert!(owed_direct_restored(&h, 51).is_empty());
    assert_eq!(resolve(&h, &own.node_id, &dest.node_id, rpc()).route, EffectiveRoute::Direct { known: true });
}

/// CONTRACT (node-connections-admit.md Row and Never, Q212; connections.md: an active connection is removed
/// only when the latest Direct entry becomes Disconnected or Failed, never Refused): a known dialer's
/// `Refused` fact, newer than its live `Connected` connection on the same key, is held and never removes
/// that connection.
#[test]
fn a_known_dialers_refused_fact_cuts_no_live_connection() {
    let acceptor = end(2, "a1");
    let dialer = end(1, "d1");
    let mut held = held_for(&acceptor);
    let live = connected(&dialer, &acceptor, 100);
    held.apply(live.clone()).unwrap();
    assert_eq!(held.active_direct(&dialer.node_id, &acceptor.node_id).flatten().map(|c| c.id.clone()), Some(live.id.clone()), "the live connection is held");
    let refused = NodeConnection { id: rdm_core::ConnectionId::mint(), state: ConnectionState::Refused, reason: Some("expired".into()), logged_at_ms: 200, ..live.clone() };
    held.apply(refused).unwrap();
    assert_eq!(held.active_direct(&dialer.node_id, &acceptor.node_id).flatten().map(|c| c.id.clone()), Some(live.id), "a Refused fact cuts no live connection");
}
