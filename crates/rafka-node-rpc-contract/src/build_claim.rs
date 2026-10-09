//! The Build attempt claim on op `0x1C`, RDM's second control family (node-rpc.md §9;
//! node-rpc-envelope.md tag table; i143 R-A1, R-T4, R-X1).
//!
//! An executor never decides its own claim of a Build attempt. The claim is an insert-and-fail
//! on the FABRIC-PRIMARY's Build log, and the executor runs the attempt only on [`BuildClaimReply::Won`].
//! One request, `ClaimAttempt`, names the Build, the attempt and the executor's exact birth; the
//! receiver takes the sender from the authenticated peer, never from the request.
//!
//! ```text
//! Won                  the attempt is this executor's; the reply carries the attempt's context
//! Lost { holder }      another executor holds the attempt
//! NotOpen { next }     the attempt is not the Build's next (or the Build is complete): nothing ran
//! NotFabricPrimary     the receiver is not the fabric-primary; it names the one it sees
//! ```
//!
//! The attempt's [`CallContext`] (traceparent, tracestate, allowlisted baggage, caller_system) is
//! the fabric-primary's local record of where the attempt came from. It rides this reply only;
//! it is never a Build fact. The family is not forwardable: a claim is decided by the
//! fabric-primary itself or not at all.

use crate::context::CallContext;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use rafka_mesh_entity::{IncarnationId, NodeId};
use serde::{Deserialize, Serialize};

/// The BuildClaim protocol: an executor asks the fabric primary for the right to run one attempt of
/// a Build.
pub struct BuildClaim;

/// A BuildClaim call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildClaimRequest {
    /// The executor asks to take attempt `attempt` of Build `build_id`.
    ClaimAttempt {
        /// The Build.
        build_id: String,
        /// The attempt claimed.
        attempt: u32,
        /// The claiming executor's node id.
        executor_node_id: NodeId,
        /// The claiming executor's incarnation.
        executor_incarnation: IncarnationId,
    },
}

impl BuildClaimRequest {
    /// The request's operation name as it appears in spans and replies.
    pub fn op(&self) -> &'static str {
        match self {
            Self::ClaimAttempt { .. } => "claim-attempt",
        }
    }
}

/// The fabric primary's answer to a claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildClaimReply {
    /// The attempt is the executor's. `context` is the attempt's observability context.
    Won {
        /// The attempt's observability context.
        context: CallContext,
    },
    /// The attempt is held by `holder` (a node-admin's path.name).
    Lost {
        /// The node-admin holding the attempt.
        holder: String,
    },
    /// The attempt is not the Build's next, or the Build is complete; `next` is the attempt that
    /// is open, when one is.
    NotOpen {
        /// The open attempt, when there is one.
        next: Option<u32>,
    },
    /// The receiver is not the fabric-primary; `fabric_primary` is the one it sees, if any.
    NotFabricPrimary {
        /// The fabric primary the receiver sees, when it sees one.
        fabric_primary: Option<String>,
    },
    /// The peer the call needed could not be resolved.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The receiver is not ready to serve.
    NotReady {
        /// Why the receiver is not ready.
        reason: String,
    },
    /// The receiver is at its admission bound.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// The receiver is draining and takes no new work.
    Draining {
        /// Why it refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
}

impl BuildClaimReply {
    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Won { .. } => "won",
            Self::Lost { .. } => "lost",
            Self::NotOpen { .. } => "not-open",
            Self::NotFabricPrimary { .. } => "not-fabric-primary",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::NotReady { .. } => "not-ready",
            Self::Busy { .. } => "busy",
            Self::Draining { .. } => "draining",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
        }
    }
}

impl NodeProtocol for BuildClaim {
    const OP: u8 = 0x1C;
    const NAME: &'static str = "build-claim";
    /// A Build id, an attempt, a node id and an incarnation id.
    const MAX_REQUEST_FRAME_BYTES: usize = 512;
    /// A reply carries at most one `CallContext`: a traceparent, a tracestate (512 bytes) and
    /// baggage (8192 bytes), with room for the enum tag and the string lengths.
    const MAX_REPLY_FRAME_BYTES: usize = 9216;
    /// The fabric-primary decides its own claims; a carried claim would be decided by no one.
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 10;

    type Request = BuildClaimRequest;
    type Reply = BuildClaimReply;

    fn classify_reply(reply: &BuildClaimReply) -> ReplyKind {
        match reply {
            BuildClaimReply::Won { .. } => ReplyKind::Success,
            BuildClaimReply::Lost { .. } | BuildClaimReply::NotOpen { .. } | BuildClaimReply::NotFabricPrimary { .. } => ReplyKind::ProtocolRefusal,
            BuildClaimReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            BuildClaimReply::NotReady { .. } => ReplyKind::NotReady,
            BuildClaimReply::Busy { .. } => ReplyKind::Busy,
            BuildClaimReply::Draining { .. } => ReplyKind::Draining,
            BuildClaimReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            BuildClaimReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> BuildClaimReply {
        BuildClaimReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> BuildClaimReply {
        BuildClaimReply::NotReady { reason }
    }
    fn busy(reason: String) -> BuildClaimReply {
        BuildClaimReply::Busy { reason }
    }
    fn draining(reason: String) -> BuildClaimReply {
        BuildClaimReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> BuildClaimReply {
        BuildClaimReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> BuildClaimReply {
        BuildClaimReply::Unauthorized { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reply_variant_encodes_and_classifies_the_same_after_a_round_trip() {
        let all = [
            (BuildClaimReply::Won { context: CallContext { caller_system: Some("rdm".into()), traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()), tracestate: None, baggage: None } }, ReplyKind::Success),
            (BuildClaimReply::Lost { holder: "mesh1.admin.1".into() }, ReplyKind::ProtocolRefusal),
            (BuildClaimReply::NotOpen { next: Some(7) }, ReplyKind::ProtocolRefusal),
            (BuildClaimReply::NotFabricPrimary { fabric_primary: Some("mesh1.admin.1".into()) }, ReplyKind::ProtocolRefusal),
            (BuildClaim::peer_unresolved("p".into()), ReplyKind::PeerUnresolved),
            (BuildClaim::not_ready("n".into()), ReplyKind::NotReady),
            (BuildClaim::busy("b".into()), ReplyKind::Busy),
            (BuildClaim::draining("d".into()), ReplyKind::Draining),
            (BuildClaim::malformed(MalformedKind::Corrupt), ReplyKind::Malformed(MalformedKind::Corrupt)),
            (BuildClaim::unauthorized("u".into()), ReplyKind::Unauthorized),
        ];
        assert_eq!(all.len() as u32, BuildClaim::REPLY_VARIANTS);
        for (r, kind) in all {
            let back = BuildClaim::decode_reply(&BuildClaim::encode_reply(&r).unwrap()).unwrap();
            assert_eq!(back, r);
            assert_eq!(BuildClaim::classify_reply(&back), kind);
        }
        let q = BuildClaimRequest::ClaimAttempt { build_id: "bld-x".into(), attempt: 3, executor_node_id: NodeId::mint(), executor_incarnation: IncarnationId::mint() };
        assert_eq!(BuildClaim::decode_request(&BuildClaim::encode_request(&q).unwrap()).unwrap(), q);
        assert!(BuildClaim::encode_request(&q).unwrap().len() <= BuildClaim::MAX_REQUEST_FRAME_BYTES);
    }

    #[test]
    fn a_reply_variant_past_the_declared_count_is_an_unknown_variant() {
        use crate::protocol::DecodeFailure;
        assert_eq!(BuildClaim::decode_reply(&[BuildClaim::REPLY_VARIANTS as u8]), Err(DecodeFailure::UnknownVariant));
        assert_eq!(BuildClaim::decode_request(&[BuildClaim::REQUEST_VARIANTS as u8]), Err(DecodeFailure::UnknownVariant));
    }

    #[test]
    fn the_largest_context_fits_the_declared_reply_ceiling() {
        let ctx = CallContext {
            caller_system: Some("x".repeat(crate::context::MAX_CALLER_SYSTEM_BYTES)),
            traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
            tracestate: Some(format!("k={}", "v".repeat(crate::context::MAX_TRACESTATE_BYTES - 2))),
            baggage: Some(format!("k={}", "v".repeat(crate::context::MAX_BAGGAGE_BYTES - 2))),
        };
        let bytes = BuildClaim::encode_reply(&BuildClaimReply::Won { context: ctx }).unwrap();
        assert!(bytes.len() <= BuildClaim::MAX_REPLY_FRAME_BYTES, "{} bytes", bytes.len());
    }
}
