//! i143.e4.s3: the source-owned reconnect reconciler and ordered Proxy retirement
//! (`rafka_mesh_entity::reconnect`).

use rafka_mesh_entity::connections::{resolve, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rafka_mesh_entity::reconnect::{
    deterministic_jitter, due, first_failure, next_failure, owed_retirements, reconnect_backoff, reconnect_plan, DIRECT_RESTORED,
};
use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind, PathName};

const RPC: CarrierPolicy = CarrierPolicy::Forwardable { carrier_kind: NodeKind::RpcNode };

fn end(ordinal: u32, incarnation: &str) -> ConnectionEnd {
    ConnectionEnd { name: PathName { mesh: "mesh1".into(), kind: NodeKind::RpcNode, ordinal, old: false }, node_id: NodeId::mint(), incarnation: Some(IncarnationId(incarnation.into())) }
}

fn held_for(own: &ConnectionEnd) -> ConnectionsHeld {
    let mut h = ConnectionsHeld::new();
    h.set_own_source(own.name.clone());
    h.mark_complete();
    h
}

fn connected(source: &ConnectionEnd, destination: &ConnectionEnd, at: u64) -> NodeConnection {
    NodeConnection {
        source: source.clone(),
        destination: destination.clone(),
        kind: ConnectionKind::Direct,
        state: ConnectionState::Connected,
        carrier: None,
        recovery: None,
        reason: None,
        logged_at_ms: at,
    }
}

/// own (rpc.1) failed to reach dest (rpc.3, process d1) three times in epoch 1, the last at 1000.
fn failing_series(own: &ConnectionEnd, dest: &ConnectionEnd) -> NodeConnection {
    let f1 = first_failure(own.clone(), dest.clone(), None, "connect refused", 100);
    let mut item = reconnect_plan(&single(own, &f1), |_| None).remove(0);
    let f2 = next_failure(&item, "connect refused", 400);
    let mut h = single(own, &f1);
    h.apply(f2.clone()).unwrap();
    item = reconnect_plan(&h, |_| None).remove(0);
    next_failure(&item, "connect refused", 1000)
}

fn single(own: &ConnectionEnd, row: &NodeConnection) -> ConnectionsHeld {
    let mut h = held_for(own);
    h.apply(row.clone()).unwrap();
    h
}

#[test]
fn after_a_restart_the_backoff_is_reconstructed_from_the_latest_failed_row_not_reset() {
    let (own, dest) = (end(1, "o1"), end(3, "d1"));
    let latest = failing_series(&own, &dest);
    assert_eq!(latest.recovery.map(|r| (r.recovery_epoch, r.attempt_ordinal)), Some((1, 3)), "one epoch, ordinal counted from 1");

    let before = reconnect_plan(&single(&own, &latest), |_| Some(IncarnationId("d1".into())));
    // A restart: a fresh projection handed the same latest row.
    let after = reconnect_plan(&single(&own, &latest), |_| Some(IncarnationId("d1".into())));
    assert_eq!(before, after);
    let want = 1000 + reconnect_backoff(3) + deterministic_jitter(&own.name, &dest.name, 3);
    assert_eq!(after[0].due_ms, want);
    assert!(reconnect_backoff(3) > reconnect_backoff(1), "backed off, not reset to the first interval");
    assert!(due(&after, want - 1).is_empty(), "a restart before the due time never attempts early");
    assert_eq!(due(&after, want).len(), 1);
}

#[test]
fn a_new_incident_starts_a_new_epoch_at_ordinal_one() {
    let (own, dest) = (end(1, "o1"), end(3, "d1"));
    let latest = failing_series(&own, &dest);
    let again = first_failure(own.clone(), dest.clone(), Some(&latest), "connect refused", 9000);
    assert_eq!(again.recovery.map(|r| (r.recovery_epoch, r.attempt_ordinal)), Some((2, 1)));
}

#[test]
fn a_series_for_a_superseded_destination_process_is_not_owed() {
    let (own, dest) = (end(1, "o1"), end(3, "d1"));
    let latest = failing_series(&own, &dest);
    let h = single(&own, &latest);
    assert!(reconnect_plan(&h, |_| Some(IncarnationId("d2".into()))).is_empty());
    assert_eq!(reconnect_plan(&h, |_| None).len(), 1, "an unknown current process keeps the series");
}

#[test]
fn a_direct_connected_ends_the_series() {
    let (own, dest) = (end(1, "o1"), end(3, "d1"));
    let latest = failing_series(&own, &dest);
    let mut h = single(&own, &latest);
    h.apply(connected(&own, &dest, 2000)).unwrap();
    assert!(reconnect_plan(&h, |_| None).is_empty());
}

#[test]
fn cutback_to_direct_happens_only_after_the_proxy_retirement_is_applied_and_a_failed_write_is_owed_again() {
    let (own, carrier, dest) = (end(1, "o1"), end(2, "c1"), end(3, "d1"));
    let mut h = held_for(&own);
    h.apply(connected(&carrier, &dest, 10)).unwrap();
    let proxy = NodeConnection {
        source: own.clone(),
        destination: dest.clone(),
        kind: ConnectionKind::Proxy,
        state: ConnectionState::Connected,
        carrier: Some(carrier.clone()),
        recovery: None,
        reason: None,
        logged_at_ms: 20,
    };
    h.apply(proxy).unwrap();
    assert!(owed_retirements(&h, 25).is_empty(), "no Direct yet: nothing owed");

    h.apply(connected(&own, &dest, 30)).unwrap();
    assert!(matches!(resolve(&h, &own.name, &dest.name, RPC).route, EffectiveRoute::ViaPeer { .. }), "the Proxy stays effective until retired");
    let owed = owed_retirements(&h, 31);
    assert_eq!(owed.len(), 1);
    assert_eq!((owed[0].state, owed[0].reason.as_deref()), (ConnectionState::Disconnected, Some(DIRECT_RESTORED)));

    // The write fails (never applied): the retirement is owed again, and the Proxy still routes.
    assert_eq!(owed_retirements(&h, 40).len(), 1);
    assert!(matches!(resolve(&h, &own.name, &dest.name, RPC).route, EffectiveRoute::ViaPeer { .. }));

    // The write lands: the Proxy is retired and new invocations go Direct.
    h.apply(owed_retirements(&h, 50).remove(0)).unwrap();
    assert!(owed_retirements(&h, 51).is_empty());
    assert_eq!(resolve(&h, &own.name, &dest.name, RPC).route, EffectiveRoute::Direct { known: true });
}

#[test]
fn route_resolution_never_creates_or_moves_a_reconnect() {
    let (own, dest) = (end(1, "o1"), end(3, "d1"));
    let latest = failing_series(&own, &dest);
    let h = single(&own, &latest);
    let plan = reconnect_plan(&h, |_| None);
    for _ in 0..100 {
        assert_eq!(resolve(&h, &own.name, &dest.name, RPC).route, EffectiveRoute::NoActiveRoute);
        assert_eq!(resolve(&h, &own.name, &end(7, "x").name, RPC).route, EffectiveRoute::Direct { known: false });
    }
    assert_eq!(reconnect_plan(&h, |_| None), plan, "traffic changes no reconnect obligation");
}
