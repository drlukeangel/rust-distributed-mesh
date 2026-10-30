//! i143.e6.s4: generic one-hop carried execution — the carrier makes exactly one direct inner
//! call, a non-forwardable family is refused by type, and certainty composes across the hop.

use iroh::SecretKey;
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::{CallOptions, CarrierEdges, HandlerFault, NodeRpcClient, NodeTarget, ResolvedNode, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::ping::{Ping, PingRequest};
use rdm_core::forward::{Forward, ForwardReply, ForwardRequest};
use rdm_core::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
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
        owed: None,
    };
    let (out, _) = r.origin.call::<Forward>(&carrier_of(&r), &forward, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|x| x.value().clone()), Some(ForwardReply::NotForwardable { op: Ping::OP }));
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_carrier_that_is_gone_before_commit_is_not_sent() {
    let r = rig().await;
    r.carrier.router.shutdown().await.unwrap();
    let opts = CallOptions { budget: rdm_transport::Budget::Overall(Duration::from_secs(2)), ..CallOptions::default() };
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &opts).await;
    assert!(out.proves_not_dispatched(), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

/// CONTRACT: the inner call applied at the target, then the carrier exited. The origin's Forward is
/// owed to the carrier until the carrier's exit is proven in the origin's view; the proof ends it
/// `Indeterminate(ReceiverGone)`, never a lost stream or the origin's budget.
#[tokio::test]
async fn a_carrier_whose_exit_is_proven_after_the_inner_call_applied_leaves_the_origin_indeterminate_receiver_gone() {
    let r = rig().await;
    let target = r.target.resolved.node_id.clone();
    let carrier = carrier_of(&r);
    let call = async { r.origin.call_via::<Probe>(&carrier, &target, &probe(b"hold"), &CallOptions::default()).await };
    let kill = async {
        r.applied.notified().await;
        r.carrier.router.shutdown().await.unwrap();
        r.origin_view.remove_gone(&r.carrier.resolved.node_id);
    };
    let ((out, _), ()) = tokio::join!(call, kill);
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::ReceiverGone(_))), "the carrier's exited verdict ends the Forward: {out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_target_the_carrier_cannot_resolve_is_not_sent() {
    let r = rig().await;
    let stranger = NodeId::mint();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &stranger, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Resolve(rdm_core::outcome::ResolveFailure::Unknown))), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

/// CONTRACT: the inner call applied at the target, then the target exited. The carrier's inner call
/// is owed to the target until the target's exit is proven in the carrier's view; the proof ends it
/// `Indeterminate(ReceiverGone)`, and the carrier preserves that typed reason at the origin.
#[tokio::test]
async fn a_target_whose_exit_is_proven_after_it_applied_answers_the_origin_carried_receiver_gone() {
    if crate::own_process::delegated(module_path!(), "a_target_whose_exit_is_proven_after_it_applied_answers_the_origin_carried_receiver_gone") { return; }
    use tracing::Instrument;
    let _telemetry = rdm_transport::init_evidence_telemetry("rdm-outcome-carried");
    async {
    let r = rig().await;
    let target = r.target.resolved.node_id.clone();
    let carrier = carrier_of(&r);
    let call = async { r.origin.call_via::<Probe>(&carrier, &target, &probe(b"hold"), &CallOptions::default()).await };
    let kill = async {
        r.applied.notified().await;
        r.target.router.shutdown().await.unwrap();
        r.carrier_view.remove_gone(&target);
    };
    let ((out, _), ()) = tokio::join!(call, kill);
    assert!(
        matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::ReceiverGone(_))),
        "the carrier learned the target's exited verdict and carried it: {out:?}"
    );
    assert_eq!(r.handled.load(Ordering::SeqCst), 1);
    let code = match &out { RpcOutcome::Indeterminate(i) => i.code(), other => panic!("expected indeterminate: {other:?}") };
    tracing::Span::current().record("code", code.name());
    tracing::Span::current().record("cause", "receiver-gone");
    tracing::info_span!(target: "rdm_test", "rdm.test.outcome.update.via-carried-cause", code = code.name(), cause = "receiver-gone", certainty = "Indeterminate")
        .in_scope(|| tracing::info!("the origin rebuilt the inner typed reason"));
    assert_eq!(code, rdm_core::outcome::Code::Unavailable, "Q40: proven receiver-gone is unavailable, including through a carrier");
    }.instrument(tracing::info_span!(target: "rdm_test", "rdm.test.outcome.serve.via-carried-exit", code = tracing::field::Empty, cause = tracing::field::Empty)).await;
    rdm_transport::flush_before_exit();

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
/// it is not Active answers the typed `ConnectionLost`, naming the fact; the origin sees
/// `NotSent(ConnectionLost)` and the target never handled the call.
///
/// CONTRACT: the third Proxy validity condition (connections.md section 8) is learned from the
/// carried call: no rewording of an inner NotSent stands in for it. The refusal comes directly from
/// the carrier's failed dial (the envelope's Forward): its inner request is never resent.
#[tokio::test]
async fn a_carrier_whose_own_direct_to_the_target_is_not_active_refuses_connection_lost() {
    let r = rig_with(Some(Arc::new(EdgeFact(Some("Direct Failed (dial failed)".into())))), true).await;
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::ConnectionLost("Direct Failed (dial failed)".into())), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
    assert_eq!(r.carrier_client.delivery().stats().resends.load(Ordering::SeqCst), 0, "the failed dial is the carrier's answer, never resent");
}

/// The same dial failure with the carrier holding no non-Active edge fact stays the carrier's
/// plain `InnerNotSent`: the refusal is the edge fact, not the failure.
///
/// CONTRACT: a carrier that cannot name its own non-Active edge never claims `connection-lost`;
/// its plain `InnerNotSent` comes directly from its failed dial, the inner request never resent.
#[tokio::test]
async fn a_carrier_with_no_non_active_edge_fact_keeps_the_plain_inner_not_sent() {
    let r = rig_with(Some(Arc::new(EdgeFact(None))), true).await;
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Connection(_))), "{out:?}");
    assert_eq!(r.carrier_client.delivery().stats().resends.load(Ordering::SeqCst), 0, "the failed dial is the carrier's answer, never resent");
}

/// The source of a Proxy `origin -> carrier -> target` and the held projection that records it.
fn proxied_held(r: &Rig) -> (rdm_core::connections::ConnectionsHeld, rdm_core::PathName, rdm_core::connections::NodeConnection) {
    use rdm_core::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
    let own: rdm_core::PathName = "mesh1.rpc.1".parse().unwrap();
    let end = |n: &ResolvedNode| ConnectionEnd { name: n.name.clone(), node_id: n.node_id.clone(), incarnation: Some(n.incarnation.clone()) };
    let proxy = NodeConnection {
        id: rdm_core::ConnectionId::mint(),
        source: ConnectionEnd { name: own.clone(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) },
        destination: end(&r.target.resolved),
        kind: ConnectionKind::Proxy,
        alpn: rdm_core::connections::ALPN.to_vec(),
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

/// A call over a Proxy whose carrier answers `ConnectionLost` hands the Proxy back for
/// delete with the structural reason `connection-lost`, and the seam writes nothing; a
/// plain `InnerNotSent` deletes nothing.
///
/// CONTRACT: the source learns the third Proxy validity condition from the carried call itself
/// (connections.md section 8) and from nothing else.
#[tokio::test]
async fn a_carried_call_answering_connection_lost_hands_the_proxy_back_to_be_disconnected() {
    use rdm_core::connections::{resolve, CarrierPolicy, INVALID_CONNECTION_LOST};
    let policy = CarrierPolicy::Proxy { carrier_kind: rdm_core::NodeKind::RpcNode };
    for (edge, deleted) in [(Some("Direct Failed".to_string()), true), (None, false)] {
        let r = rig_with(Some(Arc::new(EdgeFact(edge))), true).await;
        let (held, own, proxy) = proxied_held(&r);
        let resolution = resolve(&held, &own, &r.target.resolved.name, policy);
        let call = r.origin.call_resolved::<Probe>(resolution, &own, &r.target.resolved.name, &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
        assert!(matches!(call.outcome, RpcOutcome::NotSent(_)), "{:?}", call.outcome);
        assert_eq!(call.delete, deleted.then_some((proxy, INVALID_CONNECTION_LOST)));
    }
}

/// The origin's whole budget equals the carrier's default call budget (10 s) and the final target
/// is unreachable: the carrier's inner dial ends at its handshake's bound or its own deadline,
/// whichever is first.
///
/// CONTRACT: the carrier's inner call is bounded by what the origin has left, so the carrier's
/// `ConnectionLost` arrives inside the origin's budget and the origin records the named
/// `NotSent(ConnectionLost)`, never `Indeterminate(ReplyDeadline)`. Wall: 3 s at the gate's
/// cadence, the carrier's handshake to the silent target timing out at the endpoint's idle timeout
/// (the staleness floor, `RDM_STALENESS_MS`); the failed dial is the carrier's answer
/// (rdm.node_rpc.request.update.via-call, outcome and reason). It is the cell's subject, not setup.
#[tokio::test]
async fn an_unreachable_target_under_the_origins_default_budget_is_connection_lost_never_indeterminate() {
    let r = rig_with(Some(Arc::new(EdgeFact(Some("Direct Failed (dial failed)".into())))), false).await;
    r.target.router.shutdown().await.unwrap();
    let opts = CallOptions { budget: rdm_transport::Budget::Overall(Duration::from_secs(10)), ..CallOptions::default() };
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &opts).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::ConnectionLost("Direct Failed (dial failed)".into())), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}


/// A caller budget far under the carrier's default still gets the named reason.
///
/// CONTRACT: the carrier's inner call is bounded by the origin's remaining budget less the reply
/// reserve, so an unreachable target under a 500 ms budget answers `ConnectionLost` inside it.
///
/// The cell runs on one thread, as a `#[tokio::test]` does, under an evidence capture, and prints from
/// its trace where the origin's 500 ms went: the remaining budget the origin measured at its write, the
/// carrier's `serve.via-carried-inner` span (the inner call bounded by that less the reserve), and the
/// origin's call end.
#[test]
fn an_unreachable_target_under_a_500ms_budget_is_connection_lost_inside_it() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-forward-500ms-budget").expect("the cell's evidence file can be created");
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("the cell's runtime");
    let (trace, out, elapsed) = rt.block_on(async {
        let root = tracing::info_span!("rdm.test.forward.reject.via-unreachable-target-under-500ms");
        let trace = crate::delivery::trace_of(&root);
        let (out, elapsed) = tracing::Instrument::instrument(
            async {
                let r = rig_with(Some(Arc::new(EdgeFact(Some("Direct Failed (dial failed)".into())))), false).await;
                r.target.router.shutdown().await.unwrap();
                let started = std::time::Instant::now();
                let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &overall(Duration::from_millis(500))).await;
                (out, started.elapsed())
            },
            root,
        )
        .await;
        (trace, out, elapsed)
    });
    drop(rt);
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let in_trace = |name: &str| -> Vec<&serde_json::Value> { spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace.as_str()).collect() };
    let nano = |v: &serde_json::Value| v.as_u64().unwrap_or(0);
    let forward_op = Forward::OP.to_string();
    let call = in_trace("rdm.node_rpc.request.update.via-call").into_iter().find(|c| c["attributes"]["op"].as_str() == Some(forward_op.as_str()));
    let t0 = call.map(|c| nano(&c["start_unix_nano"])).unwrap_or(0);
    let at = |t: u64| (i128::from(t) - i128::from(t0)) / 1_000_000;
    let measured = call
        .and_then(|c| c["events"].as_array().and_then(|es| es.iter().find(|e| e["attributes"]["step"] == "remaining-measured").cloned()))
        .map(|e| format!("{} ms remaining at {} ms", e["attributes"]["remaining_ms"].as_str().unwrap_or("?"), at(nano(&e["time_unix_nano"]))));
    let inner: Vec<String> = in_trace("rdm.node_rpc.request.serve.via-carried-inner")
        .iter()
        .map(|s| format!("{}..{} ms ({})", at(nano(&s["start_unix_nano"])), at(nano(&s["end_unix_nano"])), s["attributes"]["outcome"].as_str().unwrap_or("?")))
        .collect();
    let ended = call.map(|c| format!("{} ms ({})", at(nano(&c["end_unix_nano"])), c["attributes"]["outcome"].as_str().unwrap_or("?")));
    let timeline = format!(
        "from the origin's call start: {measured:?} measured at the write; the carrier's inner call {inner:?}; the origin's call ended {ended:?}; reserve {} ms; flame {}",
        rdm_core::forward::FORWARD_REPLY_RESERVE.as_millis(),
        crate::delivery::flame(&trace)
    );
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::ConnectionLost(_))), "{out:?}; {timeline}");
    assert!(elapsed < Duration::from_millis(500), "the named answer arrived inside the budget: {elapsed:?}; {timeline}");
    println!("forward under a 500 ms budget: {elapsed:?}; {timeline}");
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

/// CONTRACT: the carrier's own failed dial is its Direct fact, so `ConnectionLost` is answered from the dial's
/// outcome and not from reading the fact back after its durable write: with a fact write that takes 400 ms the
/// named answer still reaches the origin inside a 1 s budget, and it names the dial's own reason.
#[tokio::test]
async fn a_slow_direct_fact_write_is_not_on_the_carriers_reply_path() {
    let _trace = std::env::var("FWD_TRACE").ok().map(|_| tracing::subscriber::set_default(tracing_subscriber::fmt().with_test_writer().with_env_filter(tracing_subscriber::EnvFilter::new(std::env::var("FWD_TRACE").unwrap())).with_target(true).with_ansi(false).finish()));
    let r = rig_with(Some(Arc::new(SlowFactWrite)), false).await;
    r.target.router.shutdown().await.unwrap();
    let started = std::time::Instant::now();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &overall(Duration::from_millis(1000))).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::ConnectionLost(e) if e.starts_with("Direct failed (") && !e.contains("read back"))), "{out:?}");
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
                Ok::<_, HandlerFault>(ForwardReply::InnerNotSent { reason: "spy".into(), cause: rdm_core::forward::NotSentCause::Connection })
            }
        })
        .build(served(&birth))
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
    let split = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(10), reply: Duration::from_millis(700) }, ..CallOptions::default() };
    for opts in [overall(Duration::from_millis(2000)), split] {
        let _ = origin.call_via::<Probe>(&carrier, &stranger, &probe(b"x"), &opts).await;
    }
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!((1_800..=2_000).contains(&seen[0]), "overall: deadline minus now: {seen:?}");
    assert_eq!(seen[1], 700, "split: the reply budget, not the 10 s send bound or the default: {seen:?}");
}

/// CONTRACT: a forward naming Forward is refused `NotForwardable` and a connections-deleting carrier refuses
/// a forward `ConnectionsDeleting`, each before any inner call.
#[tokio::test]
async fn a_nested_forward_and_a_carrier_deleting_connections_are_still_refused_by_name() {
    let r = rig().await;
    let nested = ForwardRequest::Forward { target: r.target.resolved.node_id.clone(), inner_op: Forward::OP, inner: Vec::new(), remaining_ms: 5_000, owed: None };
    let (out, _) = r.origin.call::<Forward>(&carrier_of(&r), &nested, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|x| x.value().clone()), Some(ForwardReply::NotForwardable { op: Forward::OP }));
    r.carrier_server.begin_connections_delete();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Carried(why) if why.contains("connections-deleting"))), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

/// Controlled typed inner outcomes traverse a real Forward codec and stream. All explanation
/// strings are identical. The genuine inner receiver exit is exercised by the live cell above.
#[tokio::test]
async fn every_inner_indeterminate_cause_crosses_a_carrier_with_its_code() {
    use rdm_core::forward::IndeterminateCause;
    use rdm_core::outcome::Code;
    for (cause, code) in [(IndeterminateCause::ReceiverGone, Code::Unavailable), (IndeterminateCause::ReplyDeadline, Code::DeadlineExceeded), (IndeterminateCause::ReplyLost, Code::Unknown)] {
        let birth = birth();
        let server = ServerBuilder::new().ledger(test_ledger())
            .serve::<Forward, _, _>(OpOwner::Core, move |_peer, _req: ForwardRequest| async move {
                let typed_inner = cause.into_reason("same explanation".into());
                Ok::<_, HandlerFault>(ForwardReply::InnerIndeterminate { cause: IndeterminateCause::from_reason(&typed_inner), reason: "same explanation".into() })
            }).build(served(&birth)).unwrap();
        let carrier = start(server, SecretKey::generate(), "mesh1.rpc.2", birth).await;
        let origin = client_with(&[&carrier.resolved]).await;
        let target = NodeTarget::ExactNode(carrier.resolved.node_id.clone());
        let (out, _) = origin.call_via::<Probe>(&target, &NodeId::mint(), &probe(b"x"), &CallOptions::default()).await;
        match out {
            RpcOutcome::Indeterminate(i) => {
                assert_eq!(i.code(), code);
                assert_eq!(IndeterminateCause::from_reason(i.reason()), cause);
            }
            other => panic!("typed cause {cause:?}: {other:?}"),
        }
        carrier.router.shutdown().await.unwrap();
    }
}
