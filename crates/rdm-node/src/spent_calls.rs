//! A call whose attempts are spent is reported to the primary above its receiver (node-rpc.md §43;
//! Q311, Q322, Q338): one rule for every caller. The primary above is the mesh-primary of the
//! receiver's mesh, or the fabric-primary when the receiver is that mesh-primary
//! (node-investigation.md, Caller); it marks the birth as a command never sent marks it
//! (node-recovery.md step 1). Every other caller sends it `ReportCallFailed { sender, receiver, reason }`
//! on Status, to the exact birth its view names in that role now, with `rdm_transport::Send::default()`. The report
//! is sent on a client that tells no spent call: a report whose own attempts are spent is dropped by
//! name, never sent on to the role's next birth, and nothing reports a report. Nothing is above the
//! fabric-primary: a spent call to it is reported to nobody.

use rdm_core::status::{ReportedBirth, Status, StatusReply, StatusRequest};
use rdm_core::{IncarnationId, NodeId, PathName};
use rdm_transport::{CallEnd, CallEndObserver, CallFailedReason, CallOptions, LiveNodeResolver, NodeResolver, NodeRpcClient, NodeTarget, ResolvedNode};
use std::sync::Arc;

/// One call whose attempts are spent, as the client tells it (node-rpc.md §43): the birth it was
/// addressed to, and its end by name with its reason (`NotSent(..)` or `Indeterminate(..)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpentCall {
    /// The birth the call was addressed to, as the resolver named it.
    pub receiver: ResolvedNode,
    /// The call's stored end: `NotSent(<reason>)` or `Indeterminate(<reason>)`.
    pub reason: String,
}

impl SpentCall {
    /// The spent call to `receiver` that ended `reason`.
    pub fn of(receiver: &ResolvedNode, reason: &CallFailedReason) -> Self {
        let reason = match reason {
            CallFailedReason::NotSent(r) => format!("NotSent({r:?})"),
            CallFailedReason::Indeterminate(r) => format!("Indeterminate({r:?})"),
        };
        Self { receiver: receiver.clone(), reason }
    }
}

/// How one `ReportCallFailed` ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportEnd {
    /// No report was sent, and why: the receiver is the fabric-primary (nothing is above it), or this
    /// node's view names no birth in the role above the receiver.
    NotSent(String),
    /// The primary above answered it: its reply (`Applied`, or a refusal by name).
    Answered(StatusReply),
    /// The report's own call ended with no answer: it is dropped, and nothing reports it.
    Dropped(String),
}

impl ReportEnd {
    /// The end's name, as the report's span carries it.
    pub fn name(&self) -> &'static str {
        match self {
            Self::NotSent(_) => "not-sent",
            Self::Answered(_) => "answered",
            Self::Dropped(_) => "dropped",
        }
    }
}

/// This process's report of its spent calls to the primary above each receiver: its own birth, the
/// resolver that names the primaries, and a client that tells no spent call.
#[derive(Clone)]
pub struct ReportToPrimary {
    resolver: Arc<LiveNodeResolver>,
    client: Arc<NodeRpcClient>,
    node_id: NodeId,
    incarnation: IncarnationId,
    path: String,
    /// The name, beside its path.
    name: String,
}

impl ReportToPrimary {
    /// The `(path, name)` lookup index key of path and name.
    fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl ReportToPrimary {
    /// The reports of the birth `(node_id, incarnation)`, launched at `name`, sent on `client` (its
    /// pool and delivery shared, telling no spent call and no `on_call_failed`) to the primaries
    /// `resolver` names.
    pub fn new(resolver: Arc<LiveNodeResolver>, client: &NodeRpcClient, node_id: NodeId, incarnation: IncarnationId, name: PathName) -> Self {
        Self { resolver, client: Arc::new(client.for_reports()), node_id, incarnation, path: name.path.clone(), name: name.name.clone()}
    }

    /// This birth as a report names it: its `(path, name)` the one the resolver holds for it now (a
    /// renamed node reports under its current name), else the one it was launched at.
    pub fn sender(&self) -> ReportedBirth {
        let at = self.resolver.resolve(&NodeTarget::ExactNode(self.node_id.clone())).ok().filter(|n| n.incarnation == self.incarnation).map_or_else(|| self.location(), |n| n.location());
        ReportedBirth { node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), path: at.path, name: at.name }
    }

    /// The exact birth this node's view names now as the primary above `receiver`
    /// (node-investigation.md, Caller): its mesh's mesh-primary, or the fabric-primary for that
    /// mesh-primary (Q338); or why no report goes: nothing is above the fabric-primary, and a role this
    /// resolver names no birth for has nobody to report to.
    pub fn above(&self, receiver: &ResolvedNode) -> Result<ResolvedNode, String> {
        let fabric_primary = self.resolver.resolve(&NodeTarget::Primary { mesh: None });
        if fabric_primary.as_ref().is_ok_and(|p| p.node_id == receiver.node_id) {
            return Err(format!("{} is the fabric-primary: nothing is above it", receiver.node_id));
        }
        match self.resolver.resolve(&NodeTarget::Primary { mesh: Some(receiver.mesh.clone()) }) {
            Ok(p) if p.node_id == receiver.node_id => fabric_primary.map_err(|f| format!("{} is the mesh-primary of {}: this node's view names no fabric-primary to report it to ({f:?})", receiver.node_id, receiver.mesh)),
            Ok(p) => Ok(p),
            Err(f) => Err(format!("this node's view names no mesh-primary of {} to report {} to ({f:?})", receiver.mesh, receiver.node_id)),
        }
    }

    /// Report `call` to the primary above its receiver and read the answer: one call,
    /// `rdm_transport::Send::default()`, to the exact birth the view names in that role now ([`Self::above`]).
    /// Spanned `rdm.mesh.node.update.via-call-failed-report` under the current span, its
    /// `primary_above` the birth reported to.
    pub async fn send(&self, call: &SpentCall) -> ReportEnd {
        let sender = self.sender();
        let span = tracing::info_span!(
            "rdm.mesh.node.update.via-call-failed-report",
            node_path = %sender.path, node_name = %sender.name,
            node_id = %sender.node_id,
            receiver_path = %call.receiver.path, receiver_name = %call.receiver.name,
            receiver_node_id = %call.receiver.node_id,
            receiver_incarnation = %call.receiver.incarnation.as_str(),
            reason = %call.reason,
            primary_above = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let end = tracing::Instrument::instrument(self.send_in(sender, call), span.clone()).await;
        span.record("outcome", end.name());
        match &end {
            ReportEnd::NotSent(why) | ReportEnd::Dropped(why) => span.record("detail", why.as_str()),
            ReportEnd::Answered(reply) => span.record("detail", reply.name()),
        };
        span.in_scope(|| match &end {
            ReportEnd::Dropped(_) => tracing::info!("the report of a spent call ended with no answer: it is dropped, and nothing reports it"),
            ReportEnd::NotSent(_) => tracing::info!("a call whose attempts are spent: no primary above its receiver to report it to"),
            ReportEnd::Answered(_) => tracing::info!("a call whose attempts are spent, reported to the primary above its receiver"),
        });
        end
    }

    async fn send_in(&self, sender: ReportedBirth, call: &SpentCall) -> ReportEnd {
        // The primary above the view names now, by its exact birth: a report whose attempts are spent is
        // dropped, never sent on to the role's next birth (the next one hears of the birth by its own
        // calls, or by gossip).
        let primary = match self.above(&call.receiver) {
            Ok(p) => p,
            Err(why) => return ReportEnd::NotSent(why),
        };
        tracing::Span::current().record("primary_above", primary.node_id.to_string().as_str());
        let target = NodeTarget::ExactNode(primary.node_id.clone());
        let at = call.receiver.location();
        let receiver = ReportedBirth { node_id: call.receiver.node_id.clone(), incarnation: call.receiver.incarnation.clone(), path: at.path, name: at.name };
        let req = StatusRequest::report_call_failed(sender, receiver, &call.reason);
        let opts = CallOptions { send: rdm_transport::Send::default(), ..CallOptions::default() };
        match self.client.call::<Status>(&target, &req, &opts).await.0 {
            rdm_core::outcome::RpcOutcome::Reply(r) => ReportEnd::Answered(r.into_value()),
            other => ReportEnd::Dropped(format!("the report's own call ended {} ({})", other.name(), other.detail_name())),
        }
    }
}

impl CallEndObserver for ReportToPrimary {
    /// A spent call is reported on its own task; an answered call reports nothing (the primary above
    /// ends a mark on its own calls' answers, Q340), nor does a call that named no birth (an endpoint,
    /// a receiver never resolved).
    fn call_ended(&self, receiver: Option<&ResolvedNode>, end: CallEnd) {
        let (Some(receiver), CallEnd::Spent(reason)) = (receiver, end) else { return };
        let (me, call) = (self.clone(), SpentCall::of(receiver, &reason));
        let span = tracing::Span::current();
        tokio::spawn(tracing::Instrument::instrument(async move { me.send(&call).await; }, span));
    }
}
