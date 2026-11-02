//! A call whose attempts are spent is reported to the primary above its receiver (node-rpc.md §43;
//! Q311, Q322): one rule for every caller. The mesh-primary of the receiver's mesh marks the birth as a
//! command never sent marks it (node-recovery.md step 1); every other caller sends it
//! `ReportCallFailed { sender, receiver, reason }` on Status, to the exact birth its view names that
//! mesh's mesh-primary now, with `Resend::default()`. The report is sent on a client that tells no spent
//! call: a report whose own attempts are spent is dropped by name, never sent on to the role's next
//! birth, and nothing reports a report.

use rdm_core::status::{ReportedBirth, Status, StatusReply, StatusRequest};
use rdm_core::{IncarnationId, NodeId, PathName};
use rdm_transport::{CallFailedObserver, CallFailedReason, CallOptions, LiveNodeResolver, NodeResolver, NodeRpcClient, NodeTarget, Resend, ResolvedNode};
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
    /// No report was sent, and why: the receiver is a primary (its silence is the fabric-primary's,
    /// by gossip and the election, node-recovery.md step 2) or its mesh names no mesh-primary here.
    NotSent(String),
    /// The mesh-primary answered it: its reply (`Applied`, or a refusal by name).
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
    name: PathName,
}

impl ReportToPrimary {
    /// The reports of the birth `(node_id, incarnation)`, launched at `name`, sent on `client` (its
    /// pool and delivery shared, telling no spent call and no `on_call_failed`) to the primaries
    /// `resolver` names.
    pub fn new(resolver: Arc<LiveNodeResolver>, client: &NodeRpcClient, node_id: NodeId, incarnation: IncarnationId, name: PathName) -> Self {
        Self { resolver, client: Arc::new(client.for_reports()), node_id, incarnation, name }
    }

    /// This birth as a report names it: its `(path, name)` the one the resolver holds for it now (a
    /// renamed node reports under its current name), else the one it was launched at.
    pub fn sender(&self) -> ReportedBirth {
        let name = self.resolver.resolve(&NodeTarget::ExactNode(self.node_id.clone())).ok().filter(|n| n.incarnation == self.incarnation).map_or_else(|| self.name.clone(), |n| n.name);
        ReportedBirth { node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), name }
    }

    /// The mesh whose mesh-primary is the primary above `receiver`, or why no report goes: a receiver
    /// that is a primary itself (the fabric-primary, or its mesh's mesh-primary) has its silence found
    /// by gossip and the election (node-recovery.md step 2), and a mesh this resolver names no
    /// mesh-primary for has nobody to report to.
    pub fn above(&self, receiver: &ResolvedNode) -> Result<String, String> {
        if self.resolver.resolve(&NodeTarget::Primary { mesh: None }).is_ok_and(|p| p.node_id == receiver.node_id) {
            return Err(format!("{} is the fabric-primary: its silence is fabric recovery's", receiver.node_id));
        }
        match self.resolver.resolve(&NodeTarget::Primary { mesh: Some(receiver.mesh.clone()) }) {
            Ok(p) if p.node_id == receiver.node_id => Err(format!("{} is the mesh-primary of {}: its silence is the fabric-primary's, by gossip and the election", receiver.node_id, receiver.mesh)),
            Ok(_) => Ok(receiver.mesh.clone()),
            Err(f) => Err(format!("this node's view names no mesh-primary of {} to report {} to ({f:?})", receiver.mesh, receiver.node_id)),
        }
    }

    /// Report `call` to the primary above its receiver and read the answer: one call,
    /// `Resend::default()`, to the exact birth the view names that mesh's mesh-primary now. Spanned
    /// `rdm.mesh.node.update.via-call-failed-report` under the current span.
    pub async fn send(&self, call: &SpentCall) -> ReportEnd {
        let sender = self.sender();
        let span = tracing::info_span!(
            "rdm.mesh.node.update.via-call-failed-report",
            node_path = %sender.name.path, node_name = %sender.name.name,
            node_id = %sender.node_id,
            receiver_path = %call.receiver.name.path, receiver_name = %call.receiver.name.name,
            receiver_node_id = %call.receiver.node_id,
            receiver_incarnation = %call.receiver.incarnation.as_str(),
            reason = %call.reason,
            mesh_primary = tracing::field::Empty,
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
            _ => tracing::info!("a call whose attempts are spent, reported to the primary above its receiver"),
        });
        end
    }

    async fn send_in(&self, sender: ReportedBirth, call: &SpentCall) -> ReportEnd {
        let mesh = match self.above(&call.receiver) {
            Ok(mesh) => mesh,
            Err(why) => return ReportEnd::NotSent(why),
        };
        // The mesh-primary the view names now, by its exact birth: a report whose attempts are spent is
        // dropped, never sent on to the role's next birth (the next one hears of the birth by its own
        // calls, or by gossip).
        let primary = match self.resolver.resolve(&NodeTarget::Primary { mesh: Some(mesh.clone()) }) {
            Ok(p) => p,
            Err(f) => return ReportEnd::NotSent(format!("this node's view names no mesh-primary of {mesh} to report {} to ({f:?})", call.receiver.node_id)),
        };
        tracing::Span::current().record("mesh_primary", primary.node_id.to_string().as_str());
        let target = NodeTarget::ExactNode(primary.node_id.clone());
        let receiver = ReportedBirth { node_id: call.receiver.node_id.clone(), incarnation: call.receiver.incarnation.clone(), name: call.receiver.name.clone() };
        let req = StatusRequest::report_call_failed(sender, receiver, &call.reason);
        let opts = CallOptions { resend: Resend::default(), ..CallOptions::default() };
        match self.client.call::<Status>(&target, &req, &opts).await.0 {
            rdm_core::outcome::RpcOutcome::Reply(r) => ReportEnd::Answered(r.into_value()),
            other => ReportEnd::Dropped(format!("the report's own call ended {} ({})", other.name(), other.detail_name())),
        }
    }
}

impl CallFailedObserver for ReportToPrimary {
    /// A call that named no birth (an endpoint, a receiver never resolved) reports nothing.
    fn call_failed(&self, receiver: Option<&ResolvedNode>, reason: CallFailedReason) {
        let Some(receiver) = receiver else { return };
        let (me, call) = (self.clone(), SpentCall::of(receiver, &reason));
        let span = tracing::Span::current();
        tokio::spawn(tracing::Instrument::instrument(async move { me.send(&call).await; }, span));
    }
}
