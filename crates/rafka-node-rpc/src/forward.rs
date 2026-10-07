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
use rafka_node_rpc_contract::catalog::TagOwner;
use rafka_node_rpc_contract::forward::{Forward, ForwardReply, ForwardRequest};
use rafka_node_rpc_contract::outcome::{carried, MalformedKind, NotSentReason, PreCommit, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::sync::Arc;

impl ServerBuilder {
    /// Carry protocol `P` for others: a forward naming its tag is executed only when `P` is
    /// forwardable. A protocol that is not forwardable is never carried, whatever is declared.
    pub fn carry<P: NodeProtocol>(mut self) -> Self {
        if P::FORWARDABLE {
            self.carried.insert(P::TAG, P::MAX_REPLY_FRAME_BYTES);
        }
        self
    }

    /// Serve [`Forward`]: each forward is one direct inner call through `client`.
    pub fn serve_forward(mut self, client: Arc<NodeRpcClient>) -> Self {
        let table = Arc::new(std::sync::OnceLock::new());
        self.carried_table = Some(table.clone());
        self.serve::<Forward, _, _>(TagOwner::Core, move |peer: PeerContext, req: ForwardRequest| {
            let (client, table) = (client.clone(), table.clone());
            async move { Ok(carry_once(&client, &table, &peer, req).await) }
        })
    }
}

async fn carry_once(
    client: &NodeRpcClient,
    table: &std::sync::OnceLock<std::collections::HashMap<u8, usize>>,
    peer: &PeerContext,
    req: ForwardRequest,
) -> ForwardReply {
    let ForwardRequest::Forward { target, inner_tag, inner, .. } = req;
    let Some(&max_reply) = table.get().and_then(|t| t.get(&inner_tag)) else {
        tracing::info_span!(
            "rafka.node_rpc.request.reject.via-not-forwardable",
            inner_tag,
            target = %target,
            caller = %peer.endpoint_id
        )
        .in_scope(|| tracing::info!("the inner protocol is not forwardable through this carrier"));
        return ForwardReply::NotForwardable { tag: inner_tag };
    };
    let Ok(node_id) = NodeId::parse(&target) else {
        return ForwardReply::Malformed { kind: MalformedKind::Corrupt };
    };
    // The hop is a child span of the origin's trace; the inner call carries the origin's
    // context unchanged, so the target sees the origin's caller_system and causal parent.
    let span = tracing::info_span!(
        "rafka.node_rpc.request.serve.via-carried-inner",
        inner_tag,
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
        .invoke_raw::<Vec<u8>, _>(&NodeTarget::ExactNode(node_id), inner_tag, inner, max_reply, &opts, |d| match d {
            Decode::Committed(c, bytes) => c.relayed(bytes),
            Decode::Early(e, bytes) => e.relayed(bytes),
        })
        .await;
    span.record("outcome", out.name());
    span.in_scope(|| tracing::info!("one direct inner call"));
    match out {
        RpcOutcome::Reply(r) => ForwardReply::Relayed { inner: r.into_value() },
        RpcOutcome::NotSent(n) => ForwardReply::InnerNotSent { reason: format!("{:?}", n.reason()) },
        RpcOutcome::Unserved(u) => ForwardReply::InnerUnserved { tag: u.tag() },
        RpcOutcome::RejectedStale(r) => ForwardReply::InnerRejectedStale { target_node_id: r.target_node_id().to_string() },
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
        let pre = PreCommit::begin(P::TAG);
        if !P::FORWARDABLE {
            return (pre.not_sent(NotSentReason::NotForwardable { tag: P::TAG }), None);
        }
        let inner = match P::encode_request(req) {
            Ok(b) => b,
            Err(e) => return (pre.not_sent(NotSentReason::Connection(format!("request does not encode: {}", e.0))), None),
        };
        let forward = ForwardRequest::Forward {
            target: target.as_str().to_string(),
            inner_tag: P::TAG,
            inner,
        };
        let (outer, evidence) = self.call::<Forward>(carrier, &forward, opts).await;
        (carried::<P>(outer), evidence)
    }
}
