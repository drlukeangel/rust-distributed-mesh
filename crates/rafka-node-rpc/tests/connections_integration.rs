//! i143.e6.s5 functional: the runtime is the glue between the held connections projection and
//! the routed call. `NodeRpcClient::call_connected` resolves the route over the projection it is
//! handed (connections.md §5), executes exactly that route, and hands back the route and the
//! resolution's own Proxy verdict; it persists no Proxy state, selects no carrier of its own and
//! schedules no reconnect. The cells are the story's three acceptance lines: an active Proxy is
//! reused without a fresh direct dial ladder; an `Indeterminate` alone never invalidates a Proxy;
//! a Direct Connected creates the retirement obligation, and new calls cut back only after the
//! retirement is durable (here: applied to the projection, as a landed row would be).

mod common;
use common::*;
use rafka_mesh_entity::connections::{CarrierPolicy, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute};
use rafka_mesh_entity::reconnect::owed_retirements;
use rafka_mesh_entity::{NodeKind, PathName};
use rafka_node_rpc::{Budget, CallOptions, ConnectedCall, PoolKey, RouteLeg};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::sync::atomic::Ordering;
use std::time::Duration;

const POLICY: CarrierPolicy = CarrierPolicy::Forwardable { carrier_kind: NodeKind::RpcNode };

async fn connected(rig: &Rig, held: &ConnectionsHeld, own: &PathName, target: &Node, payload: &[u8]) -> ConnectedCall<ProbeReply> {
    rig.origin
        .call_connected::<Probe>(
            held,
            own,
            &target.resolved.name,
            &target.resolved.node_id,
            POLICY,
            &probe(payload),
            &CallOptions { budget: Budget::Overall(Duration::from_millis(1500)), ..Default::default() },
        )
        .await
}

/// The pooled connections of the origin, by the node they reach.
fn pooled_to(rig: &Rig, node: &Node) -> Vec<PoolKey> {
    rig.origin.pooled().into_iter().filter(|k| k.peer == node.resolved.endpoint_id).collect()
}

/// A projection in which the origin's Direct to B failed and its Proxy to B through P is active.
fn proxied(rig: &Rig, own: &PathName) -> ConnectionsHeld {
    let me = own_end(own);
    let mut held = held_for(own);
    held.apply(edge(me.clone(), end(&rig.b), ConnectionKind::Direct, ConnectionState::Failed, None, 10)).unwrap();
    held.apply(edge(end(&rig.p), end(&rig.b), ConnectionKind::Direct, ConnectionState::Connected, None, 11)).unwrap();
    held.apply(edge(me, end(&rig.b), ConnectionKind::Proxy, ConnectionState::Connected, Some(end(&rig.p)), 12)).unwrap();
    held
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_active_proxy_is_reused_without_a_fresh_direct_dial_ladder() {
    let r = rig().await;
    let own: PathName = "mesh1.admin.1".parse().unwrap();
    let held = proxied(&r, &own);
    let first = connected(&r, &held, &own, &r.b, b"first").await;
    assert_eq!(served_by(&first.outcome).map(|s| s.0).as_deref(), Some("mesh1.rpc.1"), "{:?}", first.outcome);
    assert!(matches!(first.route, EffectiveRoute::ViaPeer { .. }), "{:?}", first.route);
    assert_eq!(first.leg, RouteLeg::ViaPeer { carrier: "mesh1.rpc.3".into() });
    assert!(first.retire.is_none(), "a valid Proxy is reused, not retired");
    let ev = first.evidence.expect("one leg ran");
    assert_eq!(ev.node_id, r.p.resolved.node_id, "the one leg the origin ran went to the carrier");
    assert!(!ev.reused, "the first call dialled the carrier");
    assert!(!pooled_to(&r, &r.p).is_empty(), "the carrier's connection is pooled");
    assert!(pooled_to(&r, &r.b).is_empty(), "no direct dial to B was paid: the Proxy was used as recorded");
    assert!(r.origin.dialing().is_empty(), "and none is in flight");
    // The next new invocation reuses the Proxy and the pooled carrier connection: no ladder, no dial.
    let second = connected(&r, &held, &own, &r.b, b"second").await;
    assert_eq!(served_by(&second.outcome).map(|s| s.0).as_deref(), Some("mesh1.rpc.1"), "{:?}", second.outcome);
    assert_eq!(second.leg, RouteLeg::ViaPeer { carrier: "mesh1.rpc.3".into() });
    assert!(second.evidence.expect("one leg ran").reused, "the carrier connection came from the pool");
    assert!(pooled_to(&r, &r.b).is_empty(), "still no direct connection to B");
    assert_eq!((r.b.handled.load(Ordering::SeqCst), r.c.handled.load(Ordering::SeqCst)), (2, 0), "B served both, through P; C was never touched");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_indeterminate_alone_never_invalidates_a_proxy() {
    let r = rig().await;
    let own: PathName = "mesh1.admin.1".parse().unwrap();
    let held = proxied(&r, &own);
    // B commits the carried request and never answers within the budget: Indeterminate, over the Proxy.
    let held_call = connected(&r, &held, &own, &r.b, b"hold").await;
    assert!(matches!(held_call.outcome, RpcOutcome::Indeterminate(_)), "{:?}", held_call.outcome);
    assert_eq!(held_call.leg, RouteLeg::ViaPeer { carrier: "mesh1.rpc.3".into() });
    assert!(held_call.evidence.expect("one leg ran").committed);
    assert!(held_call.retire.is_none(), "a post-commit loss is not Proxy-health evidence (connections.md §8)");
    // The projection is the seam's read-only input: the Proxy is still valid, nothing is owed, and
    // the next new invocation still goes over it.
    assert!(owed_retirements(&held, 1_000).is_empty(), "no retirement is owed after an Indeterminate");
    let next = connected(&r, &held, &own, &r.b, b"after").await;
    assert_eq!(served_by(&next.outcome).map(|s| s.0).as_deref(), Some("mesh1.rpc.1"), "{:?}", next.outcome);
    assert!(matches!(next.route, EffectiveRoute::ViaPeer { .. }));
    assert!(next.retire.is_none());
    assert_eq!(r.c.handled.load(Ordering::SeqCst), 0, "nothing was replayed anywhere else");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_direct_connected_creates_the_retirement_obligation_and_new_calls_cut_back_only_after_durable_retirement() {
    let r = rig().await;
    let own: PathName = "mesh1.admin.1".parse().unwrap();
    let me = own_end(&own);
    let mut held = proxied(&r, &own);
    // The direct connection to B returns (whichever path opened it): Direct Connected beside the
    // active Proxy. The obligation exists at once; the Proxy stays effective until it is retired.
    held.apply(edge(me.clone(), end(&r.b), ConnectionKind::Direct, ConnectionState::Connected, None, 20)).unwrap();
    let owed = owed_retirements(&held, 21);
    assert_eq!(owed.len(), 1, "one retirement is owed: {owed:?}");
    assert_eq!((owed[0].kind, owed[0].state, owed[0].reason.as_deref()), (ConnectionKind::Proxy, ConnectionState::Disconnected, Some("direct-restored")));
    let still = connected(&r, &held, &own, &r.b, b"still-proxied").await;
    assert_eq!(still.leg, RouteLeg::ViaPeer { carrier: "mesh1.rpc.3".into() }, "new calls stay on the proven Proxy until the retirement lands");
    assert!(matches!(still.route, EffectiveRoute::ViaPeer { .. }));
    assert!(still.retire.is_none(), "the seam retires nothing itself: the obligation is the caller's");
    assert!(pooled_to(&r, &r.b).is_empty(), "no direct leg was run on the strength of the row alone");
    // The retirement lands durably (the owed row applied, as the writer's ACK would apply it): the
    // next new invocation is Direct.
    held.apply(owed.into_iter().next().unwrap()).unwrap();
    assert!(owed_retirements(&held, 22).is_empty(), "nothing is owed once the retirement landed");
    let cut = connected(&r, &held, &own, &r.b, b"direct").await;
    assert_eq!(served_by(&cut.outcome).map(|s| s.0).as_deref(), Some("mesh1.rpc.1"), "{:?}", cut.outcome);
    assert_eq!(cut.leg, RouteLeg::Direct);
    assert_eq!(cut.route, EffectiveRoute::Direct { known: true });
    assert_eq!(cut.evidence.expect("one leg ran").node_id, r.b.resolved.node_id);
    assert!(!pooled_to(&r, &r.b).is_empty(), "the direct connection to B is pooled now");
    assert_eq!(r.c.handled.load(Ordering::SeqCst), 0);
}
