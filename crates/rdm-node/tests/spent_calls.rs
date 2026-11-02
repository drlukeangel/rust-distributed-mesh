//! `rdm_node::spent_calls`'s cells (i151.s2; node-rpc.md §43, Q311, Q322): a node's call whose
//! attempts are spent is reported to the mesh-primary of its receiver's mesh as `ReportCallFailed`,
//! and a report whose own attempts are spent is dropped by name, reporting nothing.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::catalog::OpOwner;
use rdm_core::ping::{Ping, PingRequest};
use rdm_core::status::{ReportedBirth, Status, StatusReply, StatusRequest};
use rdm_core::{IncarnationId, NodeId, PathName};
use rdm_node::spent_calls::{ReportEnd, ReportToPrimary, SpentCall};
use rdm_transport::{CallFailedObserver, CallFailedReason, CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, Resend, ResolvedNode, ServedBirth, ServerBuilder, SpentCallsSlot, TimeLimit};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A birth of mesh1 at `name`, reached at `ep`.
fn birth_at(ep: &iroh::Endpoint, name: &str) -> ResolvedNode {
    ResolvedNode {
        mesh: "mesh1".into(),
        kind: rdm_core::NodeKind::RpcNode,
        node_id: NodeId::mint(),
        name: PathName::new("mesh1.rpc", name).unwrap(),
        endpoint_id: ep.secret_key().public(),
        transport_addr: ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap(),
        incarnation: IncarnationId::mint(),
    }
}

/// Every spent call this process's clients tell, then handed to the report.
struct Told {
    calls: Mutex<Vec<SpentCall>>,
    report: ReportToPrimary,
}

impl CallFailedObserver for Told {
    fn call_failed(&self, receiver: Option<&ResolvedNode>, reason: CallFailedReason) {
        if let Some(r) = receiver {
            self.calls.lock().unwrap().push(SpentCall::of(r, &reason));
        }
        self.report.call_failed(receiver, reason);
    }
}

/// CONTRACT (i151.s2; node-rpc.md §43, Q311: "a call whose attempts end unanswered starts node
/// investigation on that birth (... any other caller by reporting the birth to it)"; Q322:
/// `ReportCallFailed` carries sender, receiver and reason only): a node's call to a birth of mesh1
/// whose attempts are spent is told once, and reported to the exact birth the view names mesh1's
/// mesh-primary, naming the calling birth, the exact receiver and the call's end; the
/// mesh-primary's `Applied` ends it. Then, with the role naming a birth that answers nothing, the
/// report's own attempts are spent: it ends `Dropped`, by name, and no spent call is told for it.
/// Must NOT happen: a report of the report, or a report to anyone but the primary above the birth.
// @feature: node-lifecycle
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nodes_spent_call_is_reported_to_its_mesh_primary_and_a_spent_report_is_dropped() {
    // The mesh-primary of mesh1: serves Status, keeps every request, answers `Applied`.
    let primary_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let primary = ResolvedNode { kind: rdm_core::NodeKind::NodeAdmin, name: PathName::new("mesh1.admin", "1").unwrap(), ..birth_at(&primary_ep, "1") };
    let (heard_tx, mut heard) = tokio::sync::mpsc::unbounded_channel::<StatusRequest>();
    let served = ServerBuilder::new()
        .serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
            let heard_tx = heard_tx.clone();
            async move {
                let _ = heard_tx.send(req);
                Ok(StatusReply::Applied)
            }
        })
        .build(ServedBirth { node_id: primary.node_id.to_string(), incarnation: primary.incarnation.to_string() })
        .unwrap();
    let _router = Router::builder(primary_ep.clone()).accept(rdm_transport::ALPN, served).spawn();
    // The receiver: a birth whose key no process at its address holds, so every attempt's handshake
    // fails before its commit cut.
    let receiver = ResolvedNode { endpoint_id: SecretKey::generate().public(), ..birth_at(&primary_ep, "2") };
    // The calling node, its resolver naming both and the primary record of mesh1.
    let resolver = Arc::new(LiveNodeResolver::default());
    resolver.apply(primary.clone(), None);
    resolver.apply(receiver.clone(), None);
    resolver.set_primary(Some("mesh1".into()), Some(primary.node_id.clone()));
    let me = (NodeId::mint(), IncarnationId::mint(), PathName::new("mesh1.rpc", "3").unwrap());
    let caller_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let slot = SpentCallsSlot::default();
    let client = NodeRpcClient::new(caller_ep, resolver.clone()).with_spent_calls(slot.clone());
    let report = ReportToPrimary::new(resolver.clone(), &client, me.0.clone(), me.1.clone(), me.2.clone());
    let told = Arc::new(Told { calls: Mutex::default(), report: report.clone() });
    let _ = slot.set(told.clone());

    // One attempt, the caller's (Q315): spent at once.
    let opts = CallOptions { resend: Resend { attempts: 1, time_limit: TimeLimit(Duration::from_secs(2)) }, ..CallOptions::default() };
    let bound = Duration::from_secs(60);
    let out = tokio::time::timeout(bound, client.call::<Ping>(&NodeTarget::ExactNode(receiver.node_id.clone()), &PingRequest::Ping { payload: b"p".to_vec() }, &opts)).await.expect("the call to the receiver ends").0;
    assert_eq!(out.name(), "NotSent", "nothing reached the receiver");
    let got = tokio::time::timeout(Duration::from_secs(30), heard.recv()).await.expect("the report reaches the mesh-primary").unwrap();
    let StatusRequest::ReportCallFailed { sender, receiver: named, reason } = got else { panic!("a report of the spent call: {got:?}") };
    assert_eq!(sender, ReportedBirth { node_id: me.0.clone(), incarnation: me.1.clone(), name: me.2.clone() }, "the calling birth");
    assert_eq!(named, ReportedBirth { node_id: receiver.node_id.clone(), incarnation: receiver.incarnation.clone(), name: receiver.name.clone() }, "the exact receiver");
    assert!(reason.starts_with("NotSent("), "the call's end by name: {reason}");
    {
        let calls = told.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "one spent call, told once");
        assert_eq!(calls[0].receiver.node_id, receiver.node_id);
    }
    let call = told.calls.lock().unwrap()[0].clone();
    assert_eq!(tokio::time::timeout(bound, report.send(&call)).await.expect("the report ends"), ReportEnd::Answered(StatusReply::Applied), "the mesh-primary's answer ends the report");
    assert!(matches!(tokio::time::timeout(bound, heard.recv()).await.expect("heard"), Some(StatusRequest::ReportCallFailed { .. })), "the report sent again by the cell");

    // The role now names a birth that answers nothing: the report's own attempts are spent.
    let dead = ResolvedNode { kind: rdm_core::NodeKind::NodeAdmin, name: PathName::new("mesh1.admin", "2").unwrap(), endpoint_id: SecretKey::generate().public(), ..birth_at(&primary_ep, "1") };
    resolver.apply(dead.clone(), None);
    resolver.set_primary(Some("mesh1".into()), Some(dead.node_id.clone()));
    let before = told.calls.lock().unwrap().len();
    match tokio::time::timeout(bound, report.send(&call)).await.expect("the spent report ends") {
        ReportEnd::Dropped(why) => assert!(why.contains("NotSent"), "dropped by name: {why}"),
        other => panic!("a report whose attempts are spent is dropped: {other:?}"),
    }
    assert_eq!(told.calls.lock().unwrap().len(), before, "nothing reports a report");
    assert!(heard.try_recv().is_err(), "the mesh-primary that left the role heard nothing more");
}

/// CONTRACT (i151.s2; node-recovery.md step 2: an unheard mesh-primary is the fabric-primary's,
/// found by gossip and the election): a spent call to the mesh-primary of the caller's mesh, or to the
/// fabric-primary, sends no report: `NotSent`, naming why. Must NOT happen: a report to the birth the
/// call could not reach.
// @feature: node-lifecycle
#[tokio::test]
async fn a_spent_call_to_a_primary_sends_no_report() {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let mesh_primary = birth_at(&ep, "1");
    let fabric_primary = ResolvedNode { mesh: "mesh0".into(), ..birth_at(&ep, "9") };
    let resolver = Arc::new(LiveNodeResolver::default());
    resolver.apply(mesh_primary.clone(), None);
    resolver.apply(fabric_primary.clone(), None);
    resolver.set_primary(Some("mesh1".into()), Some(mesh_primary.node_id.clone()));
    resolver.set_primary(None, Some(fabric_primary.node_id.clone()));
    let client = NodeRpcClient::new(ep.clone(), resolver.clone());
    let report = ReportToPrimary::new(resolver, &client, NodeId::mint(), IncarnationId::mint(), PathName::new("mesh1.rpc", "3").unwrap());
    for receiver in [mesh_primary, fabric_primary] {
        let call = SpentCall { receiver: receiver.clone(), reason: "NotSent(Deadline)".into() };
        assert!(matches!(report.send(&call).await, ReportEnd::NotSent(ref why) if why.contains(receiver.node_id.as_str())), "{}", receiver.node_id);
    }
}
