//! i146 e3 Q52: an Owed call's resends re-resolve their route per attempt (node-rpc.md §43, §45).
//!
//! Real endpoints on 127.0.0.1: the forward rig's carrier and receiver, and an origin on a shortened
//! schedule (the publish cadence 100 ms, the floor 800 ms, no jitter) whose own view of the receiver
//! names a transport identity nothing answers to, so every direct attempt sends nothing. The owner's
//! carrier policy names the rig's carrier; the receiver's counter proves what reached it.

use crate::rig::*;
use iroh::SecretKey;
use rdm_core::framing::Birth;
use rdm_core::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::route::RouteChoice;
use rdm_transport::{Budget, CallOptions, Delivery, NodeRpcClient, NodeTarget, Pacing, RoutePolicy, StaticResolver};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const FLOOR: Duration = Duration::from_millis(800);

/// An origin on the shortened schedule whose view names the carrier and the receiver, the receiver
/// under a transport identity nothing answers to: its direct dial fails at the handshake.
async fn misdialing_origin(r: &Rig) -> (NodeRpcClient, Arc<StaticResolver>) {
    let view = Arc::new(StaticResolver::new());
    view.insert(r.carrier.resolved.clone());
    let mut receiver = r.target.resolved.clone();
    receiver.endpoint_id = SecretKey::generate().public();
    view.insert(receiver);
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let pacing = Pacing { cadence: Duration::from_millis(100), floor: FLOOR, jitter: false };
    let client = NodeRpcClient::new(ep, view.clone()).with_delivery(Delivery::with_pacing(Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() }, pacing));
    (client, view)
}

/// An Owed call whose owner's carrier policy answers `policy` per attempt.
fn owed_via(policy: impl Fn(u32) -> RouteChoice + Send + Sync + 'static) -> CallOptions {
    CallOptions { budget: Budget::Owed, route: Some(RoutePolicy(Arc::new(policy))), ..CallOptions::default() }
}

fn via_carrier(r: &Rig) -> RouteChoice {
    RouteChoice::ViaPeer { carrier: r.carrier.resolved.node_id.clone(), path: r.carrier.resolved.name.clone() }
}

/// Wait until the origin has resent its request `n` times, or fail naming the wait.
async fn resent(origin: &NodeRpcClient, n: u64) {
    let until = tokio::time::Instant::now() + FLOOR * 8;
    while origin.delivery().stats().resends.load(Ordering::SeqCst) < n {
        assert!(tokio::time::Instant::now() < until, "the origin resent fewer than {n} times");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// CONTRACT (i146 Q52: "direct first, and once an attempt sends nothing, its later resends take the
/// route the projection now answers, ViaPeer(carrier), every attempt still targeting the same holder"):
/// one Owed call whose direct attempt sends nothing (the origin's view of its receiver misdials) is
/// carried on a later attempt through the carrier its owner's policy names, and the receiver answers
/// it: the call ends with the receiver's reply, its handler run once and seeing the carrier. Must NOT
/// happen: the call ending `NotSent` on the failed direct dial, or the policy asked before an attempt
/// went direct.
#[tokio::test]
async fn an_owed_call_whose_direct_attempt_sends_nothing_is_carried_and_answered_inside_one_call() {
    let r = rig().await;
    let (origin, _) = misdialing_origin(&r).await;
    let asked = Arc::new(Mutex::new(Vec::new()));
    let (seen, carrier) = (asked.clone(), via_carrier(&r));
    let opts = owed_via(move |attempt| {
        seen.lock().unwrap().push(attempt);
        carrier.clone()
    });
    let (receiver, request) = (NodeTarget::ExactNode(r.target.resolved.node_id.clone()), probe(b"ping"));
    let call = origin.call::<Probe>(&receiver, &request, &opts);
    let (out, _) = tokio::time::timeout(FLOOR * 8, call).await.expect("the owed call is answered through the carrier");
    let reply = out.reply().unwrap_or_else(|| panic!("the receiver's reply: {out:?}")).value().clone();
    assert_eq!(reply, ProbeReply::Probed { payload: b"ping".to_vec(), caller: r.carrier.key.public().to_string() }, "the receiver answered the carried attempt");
    assert_eq!(r.handled.load(Ordering::SeqCst), 1, "the receiver's handler ran once");
    let asked = asked.lock().unwrap().clone();
    assert!(!asked.is_empty() && !asked.contains(&1), "the policy is asked only after the first attempt went direct and sent nothing: {asked:?}");
}

/// CONTRACT (i146 Q52: "A deadline or a carrier failure never ends the call"): one Owed call whose
/// direct attempts send nothing and whose carrier cannot reach the receiver (the carrier's own view
/// misdials) is not ended by the carrier's refusals: it is resent, every other attempt carried, and
/// once the origin's view of the receiver is corrected a later direct attempt is answered, the
/// receiver's handler run once. Must NOT happen: the call ending on a carrier's refusal.
#[tokio::test]
async fn an_owed_call_whose_carrier_cannot_reach_the_receiver_is_not_ended_by_the_carriers_refusal() {
    let r = rig_with(None, true).await;
    let (origin, view) = misdialing_origin(&r).await;
    let carrier = via_carrier(&r);
    let opts = owed_via(move |attempt| if attempt % 2 == 0 { carrier.clone() } else { RouteChoice::Direct });
    let (o, t) = (origin.clone(), NodeTarget::ExactNode(r.target.resolved.node_id.clone()));
    let call = tokio::spawn(async move { o.call::<Probe>(&t, &probe(b"ping"), &opts).await.0 });
    resent(&origin, 4).await;
    assert!(!call.is_finished(), "the carrier's refusals do not end the owed call");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0, "nothing reached the receiver yet");
    view.insert(r.target.resolved.clone());
    let out = tokio::time::timeout(FLOOR * 8, call).await.expect("answered once the receiver can be reached").unwrap();
    assert!(out.reply().is_some(), "the receiver's reply: {out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 1, "the receiver's handler ran once");
}

/// CONTRACT (i146 Q52: "The obligation ends on the holder's answer or proven exit"): one Owed call
/// carried through a carrier that cannot reach its receiver ends with the receiver gone once the
/// receiver is removed from the origin's view, nothing delivered. Must NOT happen: the call outliving
/// its receiver's removal.
#[tokio::test]
async fn an_owed_carried_call_ends_on_its_receivers_removal() {
    let r = rig_with(None, true).await;
    let (origin, view) = misdialing_origin(&r).await;
    let carrier = via_carrier(&r);
    let opts = owed_via(move |_| carrier.clone());
    let (o, t) = (origin.clone(), NodeTarget::ExactNode(r.target.resolved.node_id.clone()));
    let call = tokio::spawn(async move { o.call::<Probe>(&t, &probe(b"ping"), &opts).await.0 });
    resent(&origin, 3).await;
    assert!(!call.is_finished(), "silence and the carrier's refusals end nothing");
    view.remove_gone(&r.target.resolved.node_id);
    let out = tokio::time::timeout(FLOOR * 4, call).await.expect("ended on the receiver's removal").unwrap();
    let gone = matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::ReceiverGone(_))) || matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Resolve(_)));
    assert!(gone && out.code() == rdm_core::outcome::Code::Unavailable, "the call ends with its receiver gone, `UNAVAILABLE`: {out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0, "nothing was delivered");
}

/// CONTRACT (i146 Q52: "its later resends take the route the projection now answers, ViaPeer(carrier),
/// every attempt still targeting the same holder"): one Owed call whose carrier answered an earlier
/// attempt with its failure to reach the receiver is carried by that same carrier once it can reach
/// the receiver: each carried attempt is a Forward invocation of its own toward the carrier, the inner
/// call keeping the request's identity, and the receiver answers it, its handler run once. Must NOT
/// happen: the carrier acking the later attempt as a repeat of the one it answered, and the call
/// holding on that ack with nothing delivered.
#[tokio::test]
async fn an_owed_call_is_carried_again_by_the_carrier_that_answered_its_earlier_attempt_with_a_failure() {
    let r = rig_with(None, true).await;
    let (origin, _) = misdialing_origin(&r).await;
    let carrier = via_carrier(&r);
    let opts = owed_via(move |_| carrier.clone());
    let (o, t) = (origin.clone(), NodeTarget::ExactNode(r.target.resolved.node_id.clone()));
    let call = tokio::spawn(async move { o.call::<Probe>(&t, &probe(b"ping"), &opts).await.0 });
    let carrier_stats = r.carrier_server.delivery().clone();
    let until = tokio::time::Instant::now() + FLOOR * 8;
    while carrier_stats.stats().requests_new.load(Ordering::SeqCst) < 2 {
        assert!(tokio::time::Instant::now() < until, "the carrier was asked fewer than twice");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!call.is_finished(), "the carrier's failures end nothing");
    r.carrier_view.insert(r.target.resolved.clone());
    let out = tokio::time::timeout(FLOOR * 8, call).await.expect("answered through the same carrier").unwrap();
    let reply = out.reply().unwrap_or_else(|| panic!("the receiver's reply: {out:?}")).value().clone();
    assert_eq!(reply, ProbeReply::Probed { payload: b"ping".to_vec(), caller: r.carrier.key.public().to_string() }, "the receiver answered the carried attempt");
    assert_eq!(r.handled.load(Ordering::SeqCst), 1, "the receiver's handler ran once");
    assert_eq!(carrier_stats.stats().duplicates_acked.load(Ordering::SeqCst), 0, "no carried attempt reached the carrier as a repeat");
}
