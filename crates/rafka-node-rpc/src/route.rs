//! The routing composition seam (node-rpc.md §36–§37; i143.e6.s6): an exact target that a
//! domain already selected, reached over the route connections chose, executed exactly.
//!
//! ```text
//! domain selector -> ExactNode(target)
//!                 -> connections::resolve -> Direct | ViaPeer(carrier) | NoActiveRoute
//!                 -> Node RPC executes exactly that
//! ```
//!
//! This layer never chooses a replica, an owner, a fallback target or another destination:
//! the target it is handed is the target it executes against. `ViaPeer` carries one invocation
//! to that same target through the named carrier, which can neither substitute a target nor
//! forward again. `NoActiveRoute` starts no leg at all: nothing is resolved, dialled or sent,
//! and the caller gets the named `NotSent` outcome. A `NotSent`, `Unserved`, `RejectedStale` or
//! `Indeterminate` is handed back as is; nothing here retries, re-routes or replays.

use crate::client::{CallEvidence, CallOptions, NodeRpcClient};
use crate::resolve::NodeTarget;
use rafka_mesh_entity::connections::{resolve, CarrierPolicy, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rafka_mesh_entity::{NodeId, PathName};
use rafka_node_rpc_contract::outcome::{NotSentReason, PreCommit, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use tracing::Instrument;

/// What connections chose for reaching the exact target: the execution-relevant part of an
/// [`EffectiveRoute`]. The Proxy record a `ViaPeer` was chosen by stays with the caller, which
/// retires it; the seam needs only the carrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteChoice {
    Direct,
    /// The exact carrier process connections chose: its logical id is dialled, never its path's
    /// current holder, so a carrier replaced after resolution is refused rather than substituted.
    ViaPeer { carrier: NodeId, path: PathName },
    NoActiveRoute,
}

impl RouteChoice {
    pub fn token(&self) -> &'static str {
        match self {
            RouteChoice::Direct => "direct",
            RouteChoice::ViaPeer { .. } => "via-peer",
            RouteChoice::NoActiveRoute => "no-active-route",
        }
    }
}

impl From<&EffectiveRoute> for RouteChoice {
    fn from(r: &EffectiveRoute) -> Self {
        match r {
            EffectiveRoute::Direct { .. } => RouteChoice::Direct,
            EffectiveRoute::ViaPeer { carrier, proxy } => RouteChoice::ViaPeer {
                carrier: proxy.carrier.as_ref().map(|c| c.node_id.clone()).expect("a valid Proxy names its carrier"),
                path: carrier.clone(),
            },
            EffectiveRoute::NoActiveRoute => RouteChoice::NoActiveRoute,
        }
    }
}

/// Which leg the composition executed, as evidence for the caller and the spans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteLeg {
    Direct,
    ViaPeer { carrier: String },
    /// No leg: nothing was resolved, dialled or sent.
    None,
}

impl RouteLeg {
    pub fn token(&self) -> &'static str {
        match self {
            RouteLeg::Direct => "direct",
            RouteLeg::ViaPeer { .. } => "via-peer",
            RouteLeg::None => "no-active-route",
        }
    }
}

impl NodeRpcClient {
    /// Invoke protocol `P` on the exact `target` over `route`, and nothing else.
    pub async fn call_routed<P: NodeProtocol>(
        &self,
        target: &NodeId,
        route: &RouteChoice,
        req: &P::Request,
        opts: &CallOptions,
    ) -> (RpcOutcome<P::Reply>, Option<CallEvidence>, RouteLeg) {
        let span = tracing::info_span!(
            "rdm.node_rpc.route.resolve.via-connections",
            protocol = P::NAME,
            target = %target,
            route = route.token(),
            carrier = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let (out, evidence, leg) = match route {
            RouteChoice::Direct => {
                let (out, ev) = self.call::<P>(&NodeTarget::ExactNode(target.clone()), req, opts).await;
                (out, ev, RouteLeg::Direct)
            }
            RouteChoice::ViaPeer { carrier, path } => {
                span.record("carrier", path.to_string().as_str());
                let (out, ev) = self.call_via::<P>(&NodeTarget::ExactNode(carrier.clone()), target, req, opts).await;
                (out, ev, RouteLeg::ViaPeer { carrier: path.to_string() })
            }
            RouteChoice::NoActiveRoute => (PreCommit::begin(P::OP).not_sent(NotSentReason::NoActiveRoute), None, RouteLeg::None),
        };
        span.record("outcome", out.name());
        span.in_scope(|| tracing::info!(leg = leg.token(), "one exact target, one route, executed as chosen"));
        (out, evidence, leg)
    }
}

/// What a call over the held connections projection answered, beside what the resolution found
/// on the way: the route it took, and the own Proxy found invalid and why, which the caller
/// retires. The seam writes nothing (i143.e6.s5).
#[derive(Debug)]
pub struct ConnectedCall<R> {
    pub outcome: RpcOutcome<R>,
    pub evidence: Option<CallEvidence>,
    pub leg: RouteLeg,
    pub route: EffectiveRoute,
    pub retire: Option<(NodeConnection, &'static str)>,
}

impl NodeRpcClient {
    /// Invoke `P` on the exact `target`, whose path is `destination`, over the route the held
    /// connections projection chooses for `own` under `policy` (connections.md §5): a valid own
    /// Proxy is reused with no direct dial, else an active Direct, else nothing is sent. The
    /// outcome is handed back as is: an `Indeterminate` says nothing about the route and retires
    /// no Proxy; only the resolution's own verdict on the Proxy is returned, for the caller.
    pub async fn call_connected<P: NodeProtocol>(
        &self,
        held: &ConnectionsHeld,
        own: &PathName,
        destination: &PathName,
        target: &NodeId,
        policy: CarrierPolicy,
        req: &P::Request,
        opts: &CallOptions,
    ) -> ConnectedCall<P::Reply> {
        let resolution = resolve(held, own, destination, policy);
        let span = tracing::info_span!(
            "rdm.node_rpc.route.resolve.via-held-projection",
            protocol = P::NAME,
            own = %own,
            destination = %destination,
            route = resolution.route.token(),
            retire = tracing::field::Empty,
        );
        if let Some((_, why)) = &resolution.retire {
            span.record("retire", *why);
        }
        let choice = RouteChoice::from(&resolution.route);
        let (outcome, evidence, leg) = self.call_routed::<P>(target, &choice, req, opts).instrument(span).await;
        ConnectedCall { outcome, evidence, leg, route: resolution.route, retire: resolution.retire }
    }
}
