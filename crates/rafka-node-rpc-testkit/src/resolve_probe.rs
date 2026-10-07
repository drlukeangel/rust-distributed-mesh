//! The resolve probe: a testkit-only oracle on op `0x71` that answers what
//! the executing node's own live resolver says about a target.
//!
//! The probe binary resolves from an admin's `/api/nodes` view, which is
//! current-only: it can never say `Gone`. This protocol asks a real node,
//! whose `LiveNodeResolver` is fed by its own membership, so a scenario proves
//! `Found` / `Gone` / `Unknown` where they are decided. Testkit range only
//! ([`TESTKIT_OPS`]); never a product binary.
//!
//! [`TESTKIT_OPS`]: rafka_node_rpc_contract::catalog::TESTKIT_OPS

use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::{NodeId, PathName};
use rafka_node_rpc::{LiveNodeResolver, NodeResolver, NodeTarget, PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind, ResolveFailure};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct ResolveProbe;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeTarget {
    Exact(String),
    Path(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolveRequest {
    Resolve { target: ProbeTarget },
}

/// Who answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnsweredBy {
    pub node_id: String,
    pub node: String,
    pub incarnation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolveReply {
    Found { by: AnsweredBy, node_id: String, name: String, incarnation_id: String },
    Gone { by: AnsweredBy },
    Unknown { by: AnsweredBy },
    Unavailable { by: AnsweredBy },
    /// The target does not parse.
    BadTarget { by: AnsweredBy, reason: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl ResolveReply {
    pub fn resolution(&self) -> &'static str {
        match self {
            Self::Found { .. } => "found",
            Self::Gone { .. } => "gone",
            Self::Unknown { .. } => "unknown",
            Self::Unavailable { .. } => "unavailable",
            Self::BadTarget { .. } => "bad-target",
            _ => "refused",
        }
    }
}

impl NodeProtocol for ResolveProbe {
    const OP: u8 = 0x71;
    const NAME: &'static str = "resolve-probe";
    const MAX_REQUEST_FRAME_BYTES: usize = 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 2048;
    const FORWARDABLE: bool = true;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 11;

    type Request = ResolveRequest;
    type Reply = ResolveReply;


    fn classify_reply(reply: &ResolveReply) -> ReplyKind {
        match reply {
            ResolveReply::Found { .. } | ResolveReply::Gone { .. } | ResolveReply::Unknown { .. } | ResolveReply::Unavailable { .. } => ReplyKind::Success,
            ResolveReply::BadTarget { .. } => ReplyKind::ProtocolRefusal,
            ResolveReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            ResolveReply::NotReady { .. } => ReplyKind::NotReady,
            ResolveReply::Busy { .. } => ReplyKind::Busy,
            ResolveReply::Draining { .. } => ReplyKind::Draining,
            ResolveReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            ResolveReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }

    fn peer_unresolved(reason: String) -> ResolveReply {
        ResolveReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> ResolveReply {
        ResolveReply::NotReady { reason }
    }
    fn busy(reason: String) -> ResolveReply {
        ResolveReply::Busy { reason }
    }
    fn draining(reason: String) -> ResolveReply {
        ResolveReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> ResolveReply {
        ResolveReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> ResolveReply {
        ResolveReply::Unauthorized { reason }
    }
}

/// Serve the probe on this node's own live `resolver`.
pub fn serve(b: ServerBuilder, resolver: Arc<LiveNodeResolver>, launch: &Launch) -> ServerBuilder {
    let by = AnsweredBy { node_id: launch.node_id.to_string(), node: launch.name.to_string(), incarnation_id: launch.incarnation.to_string() };
    b.serve::<ResolveProbe, _, _>(OpOwner::Testkit, move |_peer: PeerContext, req: ResolveRequest| {
        let (by, resolver) = (by.clone(), resolver.clone());
        async move {
            let ResolveRequest::Resolve { target, .. } = req;
            let target = match &target {
                ProbeTarget::Exact(id) => NodeId::parse(id).map(NodeTarget::ExactNode).map_err(|e| format!("exact {id:?}: {e}")),
                ProbeTarget::Path(p) => p.parse::<PathName>().map(NodeTarget::CurrentPath).map_err(|e| format!("path {p:?}: {e}")),
            };
            let reply = match target {
                Err(reason) => ResolveReply::BadTarget { by, reason },
                Ok(t) => match resolver.resolve(&t) {
                    Ok(n) => ResolveReply::Found { by, node_id: n.node_id.to_string(), name: n.name.to_string(), incarnation_id: n.incarnation.to_string() },
                    Err(ResolveFailure::Gone) => ResolveReply::Gone { by },
                    Err(ResolveFailure::Unknown) => ResolveReply::Unknown { by },
                    Err(ResolveFailure::Unavailable) => ResolveReply::Unavailable { by },
                },
            };
            tracing::info_span!("rdm.node_rpc.resolve_probe.serve.via-request", node = %reply_by(&reply), resolution = reply.resolution())
                .in_scope(|| tracing::info!("the node's own resolver answered"));
            Ok(reply)
        }
    })
}

fn reply_by(r: &ResolveReply) -> String {
    match r {
        ResolveReply::Found { by, .. }
        | ResolveReply::Gone { by }
        | ResolveReply::Unknown { by }
        | ResolveReply::Unavailable { by }
        | ResolveReply::BadTarget { by, .. } => by.node.clone(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reply_variant_encodes_and_round_trips() {
        let by = AnsweredBy { node_id: "n".into(), node: "mesh1.rpc.1".into(), incarnation_id: "i".into() };
        let all = [
            ResolveReply::Found { by: by.clone(), node_id: "x".into(), name: "mesh1.rpc.2".into(), incarnation_id: "j".into() },
            ResolveReply::Gone { by: by.clone() },
            ResolveReply::Unknown { by: by.clone() },
            ResolveReply::Unavailable { by: by.clone() },
            ResolveReply::BadTarget { by, reason: "r".into() },
            ResolveReply::PeerUnresolved { reason: "r".into() },
            ResolveReply::NotReady { reason: "r".into() },
            ResolveReply::Busy { reason: "r".into() },
            ResolveReply::Draining { reason: "r".into() },
            ResolveReply::Malformed { kind: MalformedKind::Corrupt },
            ResolveReply::Unauthorized { reason: "r".into() },
        ];
        assert_eq!(all.len() as u32, ResolveProbe::REPLY_VARIANTS);
        for r in all {
            assert_eq!(ResolveProbe::decode_reply(&ResolveProbe::encode_reply(&r).unwrap()).unwrap(), r);
        }
        let req = ResolveRequest::Resolve { target: ProbeTarget::Path("mesh1.rpc.1".into()) };
        assert_eq!(ResolveProbe::decode_request(&ResolveProbe::encode_request(&req).unwrap()).unwrap(), req);
    }
}
