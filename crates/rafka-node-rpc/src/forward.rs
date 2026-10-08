//! Generic one-hop carried execution (node-rpc.md §36.1; i143.e6.s4).
//!
//! The carrier side: a node that serves [`Forward`] makes exactly one direct inner call to the
//! exact final target for a protocol it was told to carry, and only when that protocol is
//! forwardable. It never forwards again (its inner call goes through the ordinary direct client)
//! and hands the inner outcome back verbatim.
//!
//! The origin side: [`NodeRpcClient::call_via`] executes a route's `ViaPeer` choice. It never
//! selects a carrier; the connections route projection does.

use crate::client::{CallEvidence, CallOptions, Decode, NodeRpcClient};
use crate::resolve::NodeTarget;
use crate::server::{PeerContext, ServerBuilder};
use rafka_mesh_entity::NodeId;
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::forward::{Forward, ForwardReply, ForwardRequest};
use rafka_node_rpc_contract::outcome::{carried, NotSentReason, PreCommit, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::sync::Arc;

/// The carrier's account of its own Direct edges (connections.md §8): the one fact a carrier
/// owns about a forward's final target.
#[async_trait::async_trait]
pub trait CarrierEdges: Send + Sync {
    /// Why this node's own latest Direct edge to the exact node `target` is not Active, or `None`
    /// when it is Active or this node holds no Direct fact toward it. A fact the inner call itself
    /// observed is held only once its durable write lands, so an implementation answers after the
    /// observations already handed to it have landed.
    async fn edge_not_active(&self, target: &NodeId) -> Option<String>;
}

impl ServerBuilder {
    /// Carry protocol `P` for others: a forward naming its op is executed only when `P` is
    /// forwardable. A protocol that is not forwardable is never carried, whatever is declared.
    pub fn carry<P: NodeProtocol>(mut self) -> Self {
        if P::FORWARDABLE {
            self.carried.insert(P::OP, P::MAX_REPLY_FRAME_BYTES);
        }
        self
    }

    /// Serve [`Forward`]: each forward is one direct inner call through `client`.
    pub fn serve_forward(self, client: Arc<NodeRpcClient>) -> Self {
        self.serve_forward_with(client, None)
    }

    /// [`Self::serve_forward`] for a carrier that records its own Direct edges: a forward whose
    /// inner call ends `NotSent` at the dial while `edges` reports this carrier's own edge to the
    /// final target not Active is answered [`ForwardReply::CarrierEdgeLost`] (connections.md §8),
    /// so the origin learns the third Proxy validity condition from the carried call itself.
    pub fn serve_forward_with_edges(self, client: Arc<NodeRpcClient>, edges: Arc<dyn CarrierEdges>) -> Self {
        self.serve_forward_with(client, Some(edges))
    }

    fn serve_forward_with(mut self, client: Arc<NodeRpcClient>, edges: Option<Arc<dyn CarrierEdges>>) -> Self {
        let table = Arc::new(std::sync::OnceLock::new());
        self.carried_table = Some(table.clone());
        self.serve::<Forward, _, _>(OpOwner::Core, move |peer: PeerContext, req: ForwardRequest| {
            let (client, table, edges) = (client.clone(), table.clone(), edges.clone());
            async move { Ok(carry_once(&client, &table, edges.as_deref(), &peer, req).await) }
        })
    }
}

async fn carry_once(
    client: &NodeRpcClient,
    table: &std::sync::OnceLock<std::collections::HashMap<u8, usize>>,
    edges: Option<&dyn CarrierEdges>,
    peer: &PeerContext,
    req: ForwardRequest,
) -> ForwardReply {
    let ForwardRequest::Forward { target, inner_op, inner, .. } = req;
    let Some(&max_reply) = table.get().and_then(|t| t.get(&inner_op)) else {
        tracing::info_span!(
            "rdm.node_rpc.request.reject.via-not-forwardable",
            inner_op,
            target = %target,
            caller = %peer.endpoint_id
        )
        .in_scope(|| tracing::info!("the inner protocol is not forwardable through this carrier"));
        return ForwardReply::NotForwardable { op: inner_op };
    };
    let node_id = target.clone();
    // The hop is a child span of the origin's trace; the inner call carries the origin's
    // context unchanged, so the target sees the origin's caller_system and causal parent.
    let span = tracing::info_span!(
        "rdm.node_rpc.request.serve.via-carried-inner",
        inner_op,
        target = %target,
        caller = %peer.endpoint_id,
        caller_system = peer.context.caller_system.as_deref().unwrap_or(""),
        outcome = tracing::field::Empty
    );
    if let Some(tp) = peer.context.traceparent.as_deref() {
        rafka_mesh_telemetry::set_remote_parent(&span, tp, peer.context.tracestate.as_deref());
    }
    let opts = CallOptions { context: Some(peer.context.clone()), ..CallOptions::default() };
    let (out, _evidence) = client
        .invoke_raw::<Vec<u8>, _>(&NodeTarget::ExactNode(node_id), inner_op, inner, max_reply, &opts, |d| match d {
            Decode::Committed(c, bytes) => c.relayed(bytes),
            Decode::Early(e, bytes) => e.relayed(bytes),
        })
        .await;
    span.record("outcome", out.name());
    span.in_scope(|| tracing::info!("one direct inner call"));
    match out {
        RpcOutcome::Reply(r) => ForwardReply::Relayed { inner: r.into_value() },
        RpcOutcome::NotSent(n) => {
            // Only a dial that ended in this carrier's own Direct fact speaks for the edge.
            let at_the_dial = matches!(n.reason(), NotSentReason::Connection(_) | NotSentReason::Deadline);
            let edge = match edges.filter(|_| at_the_dial) {
                Some(e) => e.edge_not_active(&target).await,
                None => None,
            };
            match edge {
                Some(reason) => {
                    tracing::info_span!(
                        "rdm.node_rpc.request.reject.via-carrier-edge-lost",
                        inner_op,
                        target = %target,
                        caller = %peer.endpoint_id,
                        edge = %reason
                    )
                    .in_scope(|| tracing::info!("the carrier's own Direct edge to the final target is not Active"));
                    ForwardReply::CarrierEdgeLost { reason }
                }
                None => ForwardReply::InnerNotSent { reason: format!("{:?}", n.reason()) },
            }
        }
        RpcOutcome::Unserved(u) => ForwardReply::InnerUnserved { op: u.op() },
        RpcOutcome::RejectedStale(r) => match NodeId::parse(r.target_node_id()) {
            Ok(target_node_id) => ForwardReply::InnerRejectedStale { target_node_id },
            Err(e) => ForwardReply::InnerIndeterminate { reason: format!("the stale target {:?} is not a NodeId: {e}", r.target_node_id()) },
        },
        RpcOutcome::Indeterminate(i) => ForwardReply::InnerIndeterminate { reason: format!("{:?}", i.reason()) },
    }
}

impl NodeRpcClient {
    /// Invoke protocol `P` on `target` through `carrier`, the `ViaPeer` route's exact carrier.
    /// A protocol that is not forwardable is refused here, before anything is sent.
    pub async fn call_via<P: NodeProtocol>(
        &self,
        carrier: &NodeTarget,
        target: &NodeId,
        req: &P::Request,
        opts: &CallOptions,
    ) -> (RpcOutcome<P::Reply>, Option<CallEvidence>) {
        let pre = PreCommit::begin(P::OP);
        if !P::FORWARDABLE {
            return (pre.not_sent(NotSentReason::NotForwardable { op: P::OP }), None);
        }
        let inner = match P::encode_request(req) {
            Ok(b) => b,
            Err(e) => return (pre.not_sent(NotSentReason::Connection(format!("request does not encode: {}", e.0))), None),
        };
        let forward = ForwardRequest::Forward {
            target: target.clone(),
            inner_op: P::OP,
            inner,
        };
        let (outer, evidence) = self.call::<Forward>(carrier, &forward, opts).await;
        (carried::<P>(outer), evidence)
    }
}
