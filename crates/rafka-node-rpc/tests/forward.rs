//! i143.e6.s4: generic one-hop carried execution — the carrier makes exactly one direct inner
//! call, a non-forwardable family is refused by type, and certainty composes across the hop.

use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, CarrierEdges, HandlerFault, NodeRpcClient, NodeTarget, ResolvedNode, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::ping::{Ping, PingRequest};
use rafka_node_rpc_contract::forward::{Forward, ForwardReply, ForwardRequest};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::rig::*;


#[tokio::test]
async fn a_carried_call_reaches_the_target_once_and_the_target_sees_the_carrier() {
    let r = rig().await;
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    let reply = out.reply().unwrap_or_else(|| panic!("a reply through the carrier: {out:?}")).value().clone();
    assert_eq!(reply, ProbeReply::Probed { payload: b"ping".to_vec(), caller: r.carrier.key.public().to_string() });
    assert_eq!(r.handled.load(Ordering::SeqCst), 1, "exactly one inner call");
    let _ = &r.target.router;
}

#[tokio::test]
async fn a_non_forwardable_family_is_refused_by_type_at_the_origin_and_at_the_carrier() {
    let r = rig().await;
    let echo = PingRequest::Ping { payload: b"x".to_vec() };
    let (out, _) = r.origin.call_via::<Ping>(&carrier_of(&r), &r.target.resolved.node_id, &echo, &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::NotForwardable { op: Ping::OP }), "{out:?}");

    // A forward built by hand for a non-forwardable op: the carrier refuses it by type.
    let forward = ForwardRequest::Forward {
        target: r.target.resolved.node_id.clone(),
        inner_op: Ping::OP,
        inner: Ping::encode_request(&echo).unwrap(),
        remaining_ms: 5_000,
    };
    let (out, _) = r.origin.call::<Forward>(&carrier_of(&r), &forward, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|x| x.value().clone()), Some(ForwardReply::NotForwardable { op: Ping::OP }));
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_carrier_that_is_gone_before_commit_is_not_sent() {
    let r = rig().await;
    r.carrier.router.shutdown().await.unwrap();
    let opts = CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_secs(2)), ..CallOptions::default() };
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &opts).await;
    assert!(out.proves_not_dispatched(), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_inner_call_applied_whose_outer_reply_is_lost_is_indeterminate() {
    let r = rig().await;
    let target = r.target.resolved.node_id.clone();
    let carrier = carrier_of(&r);
    let call = async { r.origin.call_via::<Probe>(&carrier, &target, &probe(b"hold"), &CallOptions::default()).await };
    let kill = async {
        r.applied.notified().await;
        r.carrier.router.shutdown().await.unwrap();
    };
    let ((out, _), ()) = tokio::join!(call, kill);
    assert!(matches!(&out, RpcOutcome::Indeterminate(_)), "the inner call applied, the outer reply was lost: {out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_target_the_carrier_cannot_resolve_is_not_sent() {
    let r = rig().await;
    let stranger = NodeId::mint();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &stranger, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Carried(_))), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_inner_reply_lost_at_the_target_is_indeterminate_through_the_carrier() {
    let r = rig().await;
    let target = r.target.resolved.node_id.clone();
    let carrier = carrier_of(&r);
    let call = async { r.origin.call_via::<Probe>(&carrier, &target, &probe(b"hold"), &CallOptions::default()).await };
    let kill = async {
        r.applied.notified().await;
        r.target.router.shutdown().await.unwrap();
    };
    let ((out, _), ()) = tokio::join!(call, kill);
    assert!(
        matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::Carried(_))),
        "the carrier could not learn the inner outcome: {out:?}"
    );
}

/// A carrier's own account of its Direct edge to the final target.
struct EdgeFact(Option<String>);

#[async_trait::async_trait]
impl CarrierEdges for EdgeFact {
    async fn edge_not_active(&self, _target: &NodeId) -> Option<String> {
        self.0.clone()
    }
}

/// A carrier whose inner dial to the final target fails while its own latest Direct fact toward
/// it is not Active answers the typed `CarrierEdgeLost`, naming the fact; the origin sees
/// `NotSent(CarrierEdgeLost)` and the target never handled the call.
///
/// CONTRACT: the third Proxy validity condition (connections.md section 8) is learned from the
/// carried call: no rewording of an inner NotSent stands in for it.
#[tokio::test]
async fn a_carrier_whose_own_edge_to_the_target_is_not_active_refuses_carrier_edge_lost() {
    let r = rig_with(Some(Arc::new(EdgeFact(Some("Direct Failed (dial failed)".into())))), true).await;
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::CarrierEdgeLost("Direct Failed (dial failed)".into())), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

/// The same dial failure with the carrier holding no non-Active edge fact stays the carrier's
/// plain `InnerNotSent`: the refusal is the edge fact, not the failure.
///
/// CONTRACT: a carrier that cannot name its own non-Active edge never claims `carrier-edge-lost`.
#[tokio::test]
async fn a_carrier_with_no_non_active_edge_fact_keeps_the_plain_inner_not_sent() {
    let r = rig_with(Some(Arc::new(EdgeFact(None))), true).await;
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Carried(_))), "{out:?}");
}

/// The source of a Proxy `origin -> carrier -> target` and the held projection that records it.
fn proxied_held(r: &Rig) -> (rafka_mesh_entity::connections::ConnectionsHeld, rafka_mesh_entity::PathName, rafka_mesh_entity::connections::NodeConnection) {
    use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
    let own: rafka_mesh_entity::PathName = "mesh1.rpc.1".parse().unwrap();
    let end = |n: &ResolvedNode| ConnectionEnd { name: n.name.clone(), node_id: n.node_id.clone(), incarnation: Some(n.incarnation.clone()) };
    let proxy = NodeConnection {
        source: ConnectionEnd { name: own.clone(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) },
        destination: end(&r.target.resolved),
        kind: ConnectionKind::Proxy,
        state: ConnectionState::Connected,
        carrier: Some(end(&r.carrier.resolved)),
        recovery: None,
        reason: None,
        logged_at_ms: 10,
    };
    let mut held = ConnectionsHeld::new();
    held.set_own_source(own.clone());
    held.mark_complete();
    held.apply(proxy.clone()).unwrap();
    (held, own, proxy)
}

/// A call over a Proxy whose carrier answers `CarrierEdgeLost` hands the Proxy back for
/// retirement with the structural reason `carrier-edge-lost`, and the seam writes nothing; a
/// plain `InnerNotSent` retires nothing.
///
/// CONTRACT: the source learns the third Proxy validity condition from the carried call itself
/// (connections.md section 8) and from nothing else.
#[tokio::test]
async fn a_carried_call_answering_carrier_edge_lost_hands_the_proxy_back_for_retirement() {
    use rafka_mesh_entity::connections::{resolve, CarrierPolicy, INVALID_CARRIER_EDGE_LOST};
    let policy = CarrierPolicy::Forwardable { carrier_kind: rafka_mesh_entity::NodeKind::RpcNode };
    for (edge, retired) in [(Some("Direct Failed".to_string()), true), (None, false)] {
        let r = rig_with(Some(Arc::new(EdgeFact(edge))), true).await;
        let (held, own, proxy) = proxied_held(&r);
        let resolution = resolve(&held, &own, &r.target.resolved.name, policy);
        let call = r.origin.call_resolved::<Probe>(resolution, &own, &r.target.resolved.name, &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
        assert!(matches!(call.outcome, RpcOutcome::NotSent(_)), "{:?}", call.outcome);
        assert_eq!(call.retire, retired.then_some((proxy, INVALID_CARRIER_EDGE_LOST)));
    }
}

/// The origin's whole budget equals the carrier's default call budget (10 s) and the final target
/// is unreachable: the carrier's inner dial runs to its own deadline.
///
/// CONTRACT: the carrier's inner call is bounded by what the origin has left, so the carrier's
/// `CarrierEdgeLost` arrives inside the origin's budget and the origin records the named
/// `NotSent(CarrierEdgeLost)`, never `Indeterminate(ReplyDeadline)`. Wall: the 10 s is the
/// carrier's inner dial running to its bound (rdm.node_rpc.request.update.via-call, outcome and
/// reason); it is the cell's subject, not setup.
#[tokio::test]
async fn an_unreachable_target_under_the_origins_default_budget_is_carrier_edge_lost_never_indeterminate() {
    let r = rig_with(Some(Arc::new(EdgeFact(Some("Direct Failed (dial failed)".into())))), false).await;
    r.target.router.shutdown().await.unwrap();
    let opts = CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_secs(10)), ..CallOptions::default() };
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &opts).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::CarrierEdgeLost("Direct Failed (dial failed)".into())), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}


/// A caller budget far under the carrier's default still gets the named reason.
///
/// CONTRACT: the carrier's inner call is bounded by the origin's remaining budget less the reply
/// reserve, so an unreachable target under a 500 ms budget answers `CarrierEdgeLost` inside it.
#[tokio::test]
async fn an_unreachable_target_under_a_500ms_budget_is_carrier_edge_lost_inside_it() {
    let _trace = std::env::var("FWD_TRACE").ok().map(|_| tracing::subscriber::set_default(tracing_subscriber::fmt().with_test_writer().with_env_filter(tracing_subscriber::EnvFilter::new(std::env::var("FWD_TRACE").unwrap())).with_target(true).with_ansi(false).finish()));
    let r = rig_with(Some(Arc::new(EdgeFact(Some("Direct Failed (dial failed)".into())))), false).await;
    r.target.router.shutdown().await.unwrap();
    let started = std::time::Instant::now();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &overall(Duration::from_millis(500))).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::CarrierEdgeLost(_))), "{out:?}");
    assert!(started.elapsed() < Duration::from_millis(500), "the named answer arrived inside the budget: {:?}", started.elapsed());
}

/// A carrier whose durable write of its own Direct fact is slow: reading the fact back waits 400 ms, naming
/// it from the dial that just failed does not.
struct SlowFactWrite;

#[async_trait::async_trait]
impl CarrierEdges for SlowFactWrite {
    async fn edge_not_active(&self, _target: &NodeId) -> Option<String> {
        tokio::time::sleep(Duration::from_millis(400)).await;
        Some("Direct failed (read back)".into())
    }
    async fn edge_after_dial(&self, _node: &ResolvedNode, reason: &str) -> Option<String> {
        Some(format!("Direct failed ({reason})"))
    }
}

/// CONTRACT: the carrier's own failed dial is its Direct fact, so `CarrierEdgeLost` is answered from the dial's
/// outcome and not from reading the fact back after its durable write: with a fact write that takes 400 ms the
/// named answer still reaches the origin inside a 1 s budget, and it names the dial's own reason.
#[tokio::test]
async fn a_slow_direct_fact_write_is_not_on_the_carriers_reply_path() {
    let _trace = std::env::var("FWD_TRACE").ok().map(|_| tracing::subscriber::set_default(tracing_subscriber::fmt().with_test_writer().with_env_filter(tracing_subscriber::EnvFilter::new(std::env::var("FWD_TRACE").unwrap())).with_target(true).with_ansi(false).finish()));
    let r = rig_with(Some(Arc::new(SlowFactWrite)), false).await;
    r.target.router.shutdown().await.unwrap();
    let started = std::time::Instant::now();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &overall(Duration::from_millis(1000))).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::CarrierEdgeLost(e) if e.starts_with("Direct failed (") && !e.contains("read back"))), "{out:?}");
    assert!(started.elapsed() < Duration::from_millis(1000), "the named answer arrived inside the budget: {:?}", started.elapsed());
}

/// A carrier that records the budget each forward carries and answers without an inner call.
async fn spy_carrier() -> (NodeRpcClient, NodeTarget, Arc<std::sync::Mutex<Vec<u64>>>, Node) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let s = seen.clone();
    let birth = birth();
    let server = ServerBuilder::new()
        .ledger(test_ledger())
        .serve::<Forward, _, _>(OpOwner::Core, move |_peer, req: ForwardRequest| {
            let s = s.clone();
            async move {
                let ForwardRequest::Forward { remaining_ms, .. } = req;
                s.lock().unwrap().push(remaining_ms);
                Ok::<_, HandlerFault>(ForwardReply::InnerNotSent { reason: "spy".into() })
            }
        })
        .seal(served(&birth))
        .unwrap();
    let node = start(server, SecretKey::generate(), "mesh1.rpc.2", birth).await;
    let origin = client_with(&[&node.resolved]).await;
    let target = NodeTarget::ExactNode(node.resolved.node_id.clone());
    (origin, target, seen, node)
}

/// CONTRACT: the frame carries what the origin has left, never the default: an overall budget
/// forwards deadline minus now, a split budget forwards its reply budget.
#[tokio::test]
async fn the_forward_frame_carries_the_budget_that_remains_never_the_default() {
    let (origin, carrier, seen, _node) = spy_carrier().await;
    let stranger = NodeId::mint();
    let split = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(10), reply: Duration::from_millis(700) }, ..CallOptions::default() };
    for opts in [overall(Duration::from_millis(2000)), split] {
        let _ = origin.call_via::<Probe>(&carrier, &stranger, &probe(b"x"), &opts).await;
    }
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!((1_800..=2_000).contains(&seen[0]), "overall: deadline minus now: {seen:?}");
    assert_eq!(seen[1], 700, "split: the reply budget, not the 10 s send bound or the default: {seen:?}");
}

/// CONTRACT: a forward naming Forward is refused `NotForwardable` and a draining carrier refuses
/// a forward `Draining`, each before any inner call.
#[tokio::test]
async fn a_nested_forward_and_a_draining_carrier_are_still_refused_by_name() {
    let r = rig().await;
    let nested = ForwardRequest::Forward { target: r.target.resolved.node_id.clone(), inner_op: Forward::OP, inner: Vec::new(), remaining_ms: 5_000 };
    let (out, _) = r.origin.call::<Forward>(&carrier_of(&r), &nested, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|x| x.value().clone()), Some(ForwardReply::NotForwardable { op: Forward::OP }));
    r.carrier_server.drain();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Carried(why) if why.contains("draining"))), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}
