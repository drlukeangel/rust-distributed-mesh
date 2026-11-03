//! `rdm_node::spent_calls`'s cells (i151.s2; node-rpc.md §43, Q311, Q322, Q338): a node's call whose
//! attempts are spent is reported to the primary above its receiver as `ReportCallFailed` (its mesh's
//! mesh-primary, or the fabric-primary for that mesh-primary), and a report whose own attempts are
//! spent is dropped by name, reporting nothing.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::catalog::OpOwner;
use rdm_core::ping::{Ping, PingRequest};
use rdm_core::status::{ReportedBirth, Status, StatusReply, StatusRequest};
use rdm_core::{IncarnationId, NodeId, PathName};
use rdm_node::spent_calls::{ReportEnd, ReportToPrimary, SpentCall};
use rdm_transport::{CallEnd, CallEndObserver, CallEndsSlot, CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, Resend, ResolvedNode, ServedBirth, ServerBuilder, TimeLimit};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A birth of mesh1 at `name`, reached at `ep`.
fn birth_at(ep: &iroh::Endpoint, name: &str) -> ResolvedNode {
    ResolvedNode {
        mesh: "mesh1".into(),
        r#type: "node".into(),
        node_id: NodeId::mint(),
        name: PathName::new("mesh1.node", name).unwrap(),
        endpoint_id: ep.secret_key().public(),
        transport_addr: ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap(),
        incarnation: IncarnationId::mint(),
    }
}

/// Every spent call this process's clients tell, and every birth that answered one, then handed to the
/// report.
struct Told {
    calls: Mutex<Vec<SpentCall>>,
    answered: Mutex<Vec<NodeId>>,
    report: ReportToPrimary,
}

impl CallEndObserver for Told {
    fn call_ended(&self, receiver: Option<&ResolvedNode>, end: CallEnd) {
        match (&end, receiver) {
            (CallEnd::Spent(reason), Some(r)) => self.calls.lock().unwrap().push(SpentCall::of(r, reason)),
            (CallEnd::Answered, Some(r)) => self.answered.lock().unwrap().push(r.node_id.clone()),
            _ => {}
        }
        self.report.call_ended(receiver, end);
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
    let primary = ResolvedNode { r#type: "node-admin".into(), name: PathName::new("mesh1.node-admin", "1").unwrap(), ..birth_at(&primary_ep, "1") };
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
    let me = (NodeId::mint(), IncarnationId::mint(), PathName::new("mesh1.node", "3").unwrap());
    let caller_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let slot = CallEndsSlot::default();
    let client = NodeRpcClient::new(caller_ep, resolver.clone()).with_call_ends(slot.clone());
    let report = ReportToPrimary::new(resolver.clone(), &client, me.0.clone(), me.1.clone(), me.2.clone());
    let told = Arc::new(Told { calls: Mutex::default(), answered: Mutex::default(), report: report.clone() });
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
    let dead = ResolvedNode { r#type: "node-admin".into(), name: PathName::new("mesh1.node-admin", "2").unwrap(), endpoint_id: SecretKey::generate().public(), ..birth_at(&primary_ep, "1") };
    resolver.apply(dead.clone(), None);
    resolver.set_primary(Some("mesh1".into()), Some(dead.node_id.clone()));
    let before = told.calls.lock().unwrap().len();
    match tokio::time::timeout(bound, report.send(&call)).await.expect("the spent report ends") {
        ReportEnd::Dropped(why) => assert!(why.contains("NotSent"), "dropped by name: {why}"),
        other => panic!("a report whose attempts are spent is dropped: {other:?}"),
    }
    assert_eq!(told.calls.lock().unwrap().len(), before, "nothing reports a report");
    assert!(heard.try_recv().is_err(), "the mesh-primary that left the role heard nothing more");
    // A call the mesh-primary's birth answers is told answered, and reports nothing.
    let (heard_before, spent_before) = (told.answered.lock().unwrap().len(), told.calls.lock().unwrap().len());
    let out = tokio::time::timeout(bound, client.call::<Status>(&NodeTarget::ExactNode(primary.node_id.clone()), &StatusRequest::GetFabric, &CallOptions::default())).await.expect("the call ends").0;
    assert_eq!(out.name(), "Reply", "the mesh-primary answered");
    assert_eq!(told.answered.lock().unwrap()[heard_before..], [primary.node_id.clone()], "an answered call is told once, naming the birth that answered");
    assert_eq!(told.calls.lock().unwrap().len(), spent_before, "an answered call is no spent call");
}

/// CONTRACT (i151.s2, Q338; node-investigation.md, Caller: "the fabric-primary for a mesh-primary";
/// node-recovery.md step 3: the fabric-primary "has no primary above it"): a member's spent call to
/// the mesh-primary of its own mesh is reported to the exact birth its view names the fabric-primary,
/// naming the calling member and the mesh-primary; a spent call to the fabric-primary is reported to
/// nobody (`NotSent`, naming why), and so is one to a mesh-primary while the view names no
/// fabric-primary. Must NOT happen: a report to the birth the call could not reach, or any report of
/// a spent call to the fabric-primary.
// @feature: node-lifecycle
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spent_call_to_a_mesh_primary_is_reported_to_the_fabric_primary_and_one_to_the_fabric_primary_to_nobody() {
    // The fabric-primary, of mesh0: serves Status, keeps every request, answers `Applied`.
    let fp_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let fabric_primary = ResolvedNode { mesh: "mesh0".into(), r#type: "node-admin".into(), name: PathName::new("mesh0.node-admin", "1").unwrap(), ..birth_at(&fp_ep, "9") };
    let (heard_tx, mut heard) = tokio::sync::mpsc::unbounded_channel::<StatusRequest>();
    let served = ServerBuilder::new()
        .serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
            let heard_tx = heard_tx.clone();
            async move {
                let _ = heard_tx.send(req);
                Ok(StatusReply::Applied)
            }
        })
        .build(ServedBirth { node_id: fabric_primary.node_id.to_string(), incarnation: fabric_primary.incarnation.to_string() })
        .unwrap();
    let _router = Router::builder(fp_ep.clone()).accept(rdm_transport::ALPN, served).spawn();
    // mesh1's mesh-primary: a birth whose key no process at its address holds.
    let mesh_primary = ResolvedNode { r#type: "node-admin".into(), name: PathName::new("mesh1.node-admin", "1").unwrap(), endpoint_id: SecretKey::generate().public(), ..birth_at(&fp_ep, "1") };
    let resolver = Arc::new(LiveNodeResolver::default());
    resolver.apply(mesh_primary.clone(), None);
    resolver.set_primary(Some("mesh1".into()), Some(mesh_primary.node_id.clone()));
    let me = (NodeId::mint(), IncarnationId::mint(), PathName::new("mesh1.node", "3").unwrap());
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep.clone(), resolver.clone());
    let report = ReportToPrimary::new(resolver.clone(), &client, me.0.clone(), me.1.clone(), me.2.clone());
    let to_mesh_primary = SpentCall { receiver: mesh_primary.clone(), reason: "Indeterminate(ReplyDeadline)".into() };
    assert!(matches!(report.send(&to_mesh_primary).await, ReportEnd::NotSent(ref why) if why.contains("no fabric-primary")), "no fabric-primary named: nobody to report to");
    resolver.apply(fabric_primary.clone(), None);
    resolver.set_primary(None, Some(fabric_primary.node_id.clone()));
    let bound = Duration::from_secs(60);
    assert_eq!(tokio::time::timeout(bound, report.send(&to_mesh_primary)).await.expect("the report ends"), ReportEnd::Answered(StatusReply::Applied), "the fabric-primary's answer ends the report");
    let got = tokio::time::timeout(Duration::from_secs(30), heard.recv()).await.expect("the report reaches the fabric-primary").unwrap();
    let StatusRequest::ReportCallFailed { sender, receiver, reason } = got else { panic!("a report of the spent call: {got:?}") };
    assert_eq!(sender, ReportedBirth { node_id: me.0.clone(), incarnation: me.1.clone(), name: me.2.clone() }, "the calling member");
    assert_eq!(receiver, ReportedBirth { node_id: mesh_primary.node_id.clone(), incarnation: mesh_primary.incarnation.clone(), name: mesh_primary.name.clone() }, "the exact mesh-primary");
    assert_eq!(reason, "Indeterminate(ReplyDeadline)");
    let to_fabric_primary = SpentCall { receiver: fabric_primary.clone(), reason: "NotSent(Deadline)".into() };
    assert!(matches!(report.send(&to_fabric_primary).await, ReportEnd::NotSent(ref why) if why.contains(fabric_primary.node_id.as_str())), "nothing is above the fabric-primary");
    assert!(heard.try_recv().is_err(), "the fabric-primary heard no report of itself");
    ep.close().await;
}
