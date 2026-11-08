//! The typed caller's node command at the primary above a node (Q413; node-connections-delete.md
//! "Wire", node-primary-update.md step 6), over real Node RPC against primaries that answer from a
//! script: one call per command at a node. A `Busy` is sent again under the call's `Send`, and a
//! `PrimaryUpdated { node_id }` sends the same command once to the mesh-primary it names
//! (`rdm_client::NodeRpc::primary_command`, behind `NodeRpc::connections_delete` and `Nodes`'s node
//! commands).

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
use rdm_core::status::ConnectionScope;
use rdm_core::{ConnectionId, IncarnationId, NodeId};
use rdm_transport::stream::{NotStarted, ReplySink};
use rdm_transport::{CallOptions, NodeRpcClient, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How a scripted primary answers one call.
#[derive(Clone)]
enum Answer {
    /// `Busy`, the call's one frame.
    Busy,
    /// `PrimaryUpdated` naming this node, the call's one frame.
    PrimaryUpdated(NodeId),
    /// `CommandAccepted`, then `ConnectionsDeleted` naming `not_open`.
    Runs,
}

/// A primary serving NodeCommand 0x22 from its script, recording every request it was sent.
struct Primary {
    node_id: NodeId,
    resolved: ResolvedNode,
    sent: Arc<Mutex<Vec<NodeCommandRequest>>>,
    _router: Router,
}

/// A primary that answers each call with the next of `script` (the last again once it is spent).
async fn primary(script: Vec<Answer>) -> Primary {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let sent: Arc<Mutex<Vec<NodeCommandRequest>>> = Default::default();
    let script = Arc::new(Mutex::new(VecDeque::from(script)));
    let (me, at, held) = (node_id.clone(), incarnation.clone(), sent.clone());
    let server = ServerBuilder::new()
        .serve_stream::<NodeCommand, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req: NodeCommandRequest, sink: ReplySink<NodeCommand, NotStarted>| {
            let (script, held, me, at) = (script.clone(), held.clone(), me.clone(), at.clone());
            async move {
                held.lock().unwrap().push(req.clone());
                let answer = {
                    let mut s = script.lock().unwrap();
                    if s.len() > 1 { s.pop_front() } else { s.front().cloned() }
                }
                .expect("a scripted answer");
                Ok(match answer {
                    Answer::Busy => rdm_core::reply::node_command::busy(format!("{me} runs another command or step on the node")),
                    Answer::PrimaryUpdated(to) => rdm_core::reply::node_command::primary_updated(to),
                    Answer::Runs => {
                        let NodeCommandRequest::DeleteConnections { scope: ConnectionScope::Connections(ids), .. } = req else { panic!("the cells send a named delete: {req:?}") };
                        if sink.started(NodeCommandReply::CommandAccepted { node_id: me, incarnation: at }).await.is_err() {
                            return Ok(rdm_core::reply::node_command::not_ready("the caller left".into()));
                        }
                        NodeCommandReply::ConnectionsDeleted { closed: Vec::new(), not_open: ids, held: Vec::new(), receipt: None }
                    }
                })
            }
        })
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .expect("the catalog is built");
    let router = Router::builder(ep.clone()).accept(rdm_transport::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { mesh: "mesh1".into(), r#type: "node-admin".into(), node_id: node_id.clone(), path: "mesh1.node-admin".into(), name: node_id.to_string(), endpoint_id: ep.id(), transport_addr: addr, incarnation };
    Primary { node_id, resolved, sent, _router: router }
}

/// A caller's client resolving every primary.
async fn caller(primaries: &[&Primary]) -> (Arc<NodeRpcClient>, iroh::Endpoint) {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    for p in primaries {
        resolver.insert(p.resolved.clone());
    }
    (Arc::new(NodeRpcClient::new(ep.clone(), resolver)), ep)
}

/// The caller's `node.connection.delete { Connections([never]) }` of one birth, sent to `to`, bounded.
async fn delete(client: &NodeRpcClient, to: &NodeId, never: &ConnectionId) -> (Result<NodeCommandReply, rdm_client::CallEnd>, NodeCommandRequest) {
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(NodeId::mint()), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let scope = ConnectionScope::Connections(vec![never.clone()]);
    let req = NodeCommandRequest::DeleteConnections { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), scope: scope.clone() };
    let out = tokio::time::timeout(Duration::from_secs(30), rdm_client::NodeRpc::new(client).connections_delete(to, &birth, &scope, &CallOptions::default())).await.expect("the call ends within its bound");
    (out, req)
}

// @feature: node-lifecycle
/// CONTRACT (Q413; node-connections-delete.md "Wire": "`Busy { reason }` while the primary runs another
/// command or step on the node, which the caller's `Send` sends again"): a primary answering `Busy` twice
/// is sent the same command again under the call's `Send`, and the third attempt runs it: the call ends
/// with the command's terminal. Must NOT happen: the `Busy` ending the call while attempts are left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_busy_primary_is_sent_the_command_again_until_it_runs_it() {
    let p = primary(vec![Answer::Busy, Answer::Busy, Answer::Runs]).await;
    let (client, _ep) = caller(&[&p]).await;
    let never = ConnectionId::mint();
    let (out, req) = delete(&client, &p.node_id, &never).await;
    assert!(matches!(&out, Ok(NodeCommandReply::ConnectionsDeleted { not_open, .. }) if not_open == &vec![never.clone()]), "the third attempt ran the command: {out:?}");
    let sent = p.sent.lock().unwrap().clone();
    assert_eq!(sent, vec![req.clone(), req.clone(), req], "the same command, sent three times: twice refused Busy, then run");
}

// @feature: node-lifecycle
/// CONTRACT (Q413: "After the attempts are spent, the call ends `Rejected` with the busy reason"): a
/// primary answering `Busy` to every attempt is sent the command `Send::default()`'s attempts times, and
/// the call ends `Rejected { reason: "busy" }`, naming the primary's reason. Must NOT happen: a fourth
/// send, or an end that is not by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_primary_busy_past_the_calls_attempts_ends_it_rejected_busy() {
    let p = primary(vec![Answer::Busy]).await;
    let (client, _ep) = caller(&[&p]).await;
    let (out, _) = delete(&client, &p.node_id, &ConnectionId::mint()).await;
    assert!(matches!(&out, Err(rdm_client::CallEnd::Rejected { reason, detail }) if reason == "busy" && detail.contains("runs another command or step on the node")), "its Send spent, the call ends Rejected busy with the primary's reason: {out:?}");
    assert_eq!(p.sent.lock().unwrap().len(), usize::from(rdm_transport::Send::default().attempts), "one send per attempt of the call's Send");
}

// @feature: node-lifecycle
/// CONTRACT (Q413; node-primary-update.md step 6: "the primary above answers the caller's call
/// `PrimaryUpdated { node_id }` and the caller sends the command to that mesh-primary itself"): the
/// primary above answers `PrimaryUpdated` naming B; the caller sends the same request once to B, which
/// runs it, and the call ends with B's terminal. Must NOT happen: the answer ending the call, the
/// command sent again to the old primary, or a request to B other than the caller's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_primary_updated_answer_sends_the_same_command_once_to_the_new_mesh_primary() {
    let b = primary(vec![Answer::Runs]).await;
    let a = primary(vec![Answer::PrimaryUpdated(b.node_id.clone())]).await;
    let (client, _ep) = caller(&[&a, &b]).await;
    let never = ConnectionId::mint();
    let (out, req) = delete(&client, &a.node_id, &never).await;
    assert!(matches!(&out, Ok(NodeCommandReply::ConnectionsDeleted { not_open, .. }) if not_open == &vec![never.clone()]), "the new mesh-primary ran the command: {out:?}");
    assert_eq!(a.sent.lock().unwrap().clone(), vec![req.clone()], "the old primary was sent the command once");
    assert_eq!(b.sent.lock().unwrap().clone(), vec![req], "the new mesh-primary was sent the same command once");
}

// @feature: node-lifecycle
/// CONTRACT (Q413: "A second `PrimaryUpdated` from that mesh-primary is an error by name, never a loop"):
/// A answers `PrimaryUpdated` naming B, and B answers `PrimaryUpdated` naming C: the call ends
/// `Rejected { reason: "primary-updated-again" }` and C is never sent it. Must NOT happen: a third
/// call, to C or anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_primary_updated_ends_the_call_by_name_never_a_loop() {
    let c = primary(vec![Answer::Runs]).await;
    let b = primary(vec![Answer::PrimaryUpdated(c.node_id.clone())]).await;
    let a = primary(vec![Answer::PrimaryUpdated(b.node_id.clone())]).await;
    let (client, _ep) = caller(&[&a, &b, &c]).await;
    let (out, _) = delete(&client, &a.node_id, &ConnectionId::mint()).await;
    assert!(matches!(&out, Err(rdm_client::CallEnd::Rejected { reason, .. }) if reason == "primary-updated-again"), "the second PrimaryUpdated ends the call by name: {out:?}");
    assert_eq!((a.sent.lock().unwrap().len(), b.sent.lock().unwrap().len(), c.sent.lock().unwrap().len()), (1, 1, 0), "one call to A, one to B, none to C");
}
