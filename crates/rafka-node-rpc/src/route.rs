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
use rafka_mesh_entity::connections::EffectiveRoute;
use rafka_mesh_entity::{NodeId, PathName};
use rafka_node_rpc_contract::outcome::{NotSentReason, PreCommit, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;

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
            "rafka.node_rpc.route.resolve.via-connections",
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
