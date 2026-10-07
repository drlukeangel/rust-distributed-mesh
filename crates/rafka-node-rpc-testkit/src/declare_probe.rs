//! The declare probe: a testkit-only oracle on op `0x72` that makes the executing rpc node
//! declare its own lifecycle state to an authority over the real `Status` protocol and hands the
//! typed reply back.
//!
//! The sender of a declaration is the authenticated peer, never a field of the request, so the
//! probe binary cannot declare on a node's behalf: it asks the node to. Two testkit-only
//! overrides exist so a scenario can prove the refusals: a `node_id` that is not the node's
//! (`RejectedNotAuthority: sender-not-subject`) and an `incarnation` that is not its birth's
//! (`RejectedStaleBirth`). Testkit range only ([`TESTKIT_OPS`]); never a product binary.
//!
//! [`TESTKIT_OPS`]: rafka_node_rpc_contract::catalog::TESTKIT_OPS

use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::{NodeId, PathName};
use rafka_node_rpc::{CallOptions, NodeTarget, PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_node_rpc_contract::status::{NodeState, Status, StatusReply, StatusRequest};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};

pub struct DeclareProbe;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeclareTarget {
    Exact(String),
    Path(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeclareRequest {
    /// Declare `state` for this node's own birth to `to`; the overrides are testkit-only.
    Declare { to: DeclareTarget, state: NodeState, node_id: Option<String>, incarnation: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeclareReply {
    /// The authority answered: the Node RPC outcome name and, for a Reply, the typed status reply.
    Answered { outcome: String, reply: Option<StatusReply>, reason: Option<String> },
    BadTarget { reason: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for DeclareProbe {
    const OP: u8 = 0x72;
    const NAME: &'static str = "declare-probe";
    const MAX_REQUEST_FRAME_BYTES: usize = 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 2048;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 8;

    type Request = DeclareRequest;
    type Reply = DeclareReply;

    fn classify_reply(reply: &DeclareReply) -> ReplyKind {
        match reply {
            DeclareReply::Answered { .. } => ReplyKind::Success,
            DeclareReply::BadTarget { .. } => ReplyKind::ProtocolRefusal,
            DeclareReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            DeclareReply::NotReady { .. } => ReplyKind::NotReady,
            DeclareReply::Busy { .. } => ReplyKind::Busy,
            DeclareReply::Draining { .. } => ReplyKind::Draining,
            DeclareReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            DeclareReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> DeclareReply {
        DeclareReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> DeclareReply {
        DeclareReply::NotReady { reason }
    }
    fn busy(reason: String) -> DeclareReply {
        DeclareReply::Busy { reason }
    }
    fn draining(reason: String) -> DeclareReply {
        DeclareReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> DeclareReply {
        DeclareReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> DeclareReply {
        DeclareReply::Unauthorized { reason }
    }
}

/// Serve the probe: the node declares through its own process client, filled in `client` once
/// the node runs.
pub fn serve(b: ServerBuilder, client: Arc<OnceLock<crate::node_rpc::ProcessNodeRpc>>, launch: &Launch) -> ServerBuilder {
    let (own_id, own_inc) = (launch.node_id.to_string(), launch.incarnation.0.clone());
    b.serve::<DeclareProbe, _, _>(OpOwner::Testkit, move |_peer: PeerContext, req: DeclareRequest| {
        let (client, own_id, own_inc) = (client.clone(), own_id.clone(), own_inc.clone());
        async move {
            let DeclareRequest::Declare { to, state, node_id, incarnation } = req;
            let Some(rpc) = client.get() else { return Ok(DeclareReply::NotReady { reason: "the node's client is not up yet".into() }) };
            let target = match &to {
                DeclareTarget::Exact(id) => NodeId::parse(id).map(NodeTarget::ExactNode).map_err(|e| format!("exact {id:?}: {e}")),
                DeclareTarget::Path(p) => p.parse::<PathName>().map(NodeTarget::CurrentPath).map_err(|e| format!("path {p:?}: {e}")),
            };
            let target = match target {
                Ok(t) => t,
                Err(reason) => return Ok(DeclareReply::BadTarget { reason }),
            };
            let node_id = match NodeId::parse(&node_id.unwrap_or(own_id)) {
                Ok(id) => id,
                Err(e) => return Ok(DeclareReply::BadTarget { reason: format!("node id: {e}") }),
            };
            let incarnation = rafka_mesh_entity::IncarnationId(incarnation.unwrap_or(own_inc));
            let declare = StatusRequest::DeclareNodeState { node_id, incarnation, state };
            let (out, _) = rpc.client.call::<Status>(&target, &declare, &CallOptions::default()).await;
            let (reply, reason) = match &out {
                RpcOutcome::Reply(r) => (Some(r.value().clone()), None),
                RpcOutcome::NotSent(n) => (None, Some(format!("{:?}", n.reason()))),
                RpcOutcome::Indeterminate(i) => (None, Some(format!("{:?}", i.reason()))),
                RpcOutcome::Unserved(u) => (None, Some(format!("{u:?}"))),
                RpcOutcome::RejectedStale(r) => (None, Some(format!("stale target {}", r.target_node_id()))),
            };
            tracing::info_span!("rafka.node_rpc.declare_probe.serve.via-request", state = ?state, outcome = out.name(), reply = reply.as_ref().map(StatusReply::name).unwrap_or(""))
                .in_scope(|| tracing::info!("the node declared its own state"));
            Ok(DeclareReply::Answered { outcome: out.name().to_string(), reply, reason })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reply_variant_encodes_and_round_trips() {
        let all = [
            DeclareReply::Answered { outcome: "Reply".into(), reply: Some(StatusReply::Applied), reason: None },
            DeclareReply::BadTarget { reason: "r".into() },
            DeclareReply::PeerUnresolved { reason: "r".into() },
            DeclareReply::NotReady { reason: "r".into() },
            DeclareReply::Busy { reason: "r".into() },
            DeclareReply::Draining { reason: "r".into() },
            DeclareReply::Malformed { kind: MalformedKind::Corrupt },
            DeclareReply::Unauthorized { reason: "r".into() },
        ];
        assert_eq!(all.len() as u32, DeclareProbe::REPLY_VARIANTS);
        for r in all {
            assert_eq!(DeclareProbe::decode_reply(&DeclareProbe::encode_reply(&r).unwrap()).unwrap(), r);
        }
        let req = DeclareRequest::Declare { to: DeclareTarget::Path("mesh1.admin.1".into()), state: NodeState::ReadyForTraffic, node_id: None, incarnation: None };
        assert_eq!(DeclareProbe::decode_request(&DeclareProbe::encode_request(&req).unwrap()).unwrap(), req);
    }
}
