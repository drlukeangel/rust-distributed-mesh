//! The Build attempt claim on op `0x1C`, RDM's second control family (node-rpc.md §9;
//! node-rpc-envelope.md tag table; i143 R-A1, R-T4, R-X1).
//!
//! An executor never decides its own claim of a Build attempt. The claim is decided by its
//! claimant (R-BI7): the fabric-primary for a topology Build's attempt (the fabric lane), the
//! mesh-primary of the mesh for that mesh's drift attempt (its lane); the key is
//! `(build_id, mesh, attempt)`, decided by insert-and-fail on the mesh's rows, and the executor
//! runs the attempt only on [`BuildClaimReply::Won`]. One request, `ClaimAttempt`, names the Build,
//! the lane, the attempt and the executor's exact birth; the receiver takes the sender from the
//! authenticated peer, never from the request.
//!
//! ```text
//! Won                  the attempt is this executor's; the reply carries the attempt's context
//! Lost { holder }      another executor holds the attempt
//! NotOpen { next }     the attempt is not the Build's next (or the Build is complete): nothing ran
//! NotClaimant          the receiver may not claim on that mesh; it names who may: the
//!                      fabric-primary (a topology attempt) and the mesh's mesh-primary (a drift attempt)
//! BuildInProgress      another attempt holds the mesh the attempt acts in: the Build, the mesh, the holder
//! ```
//!
//! The attempt's [`CallContext`] (traceparent, tracestate, allowlisted baggage, caller_system) is
//! the fabric-primary's local record of where the attempt came from. It rides this reply only;
//! it is never a Build fact. The family is not forwardable: a claim is decided by the
//! fabric-primary itself or not at all.

use crate::context::CallContext;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::{IncarnationId, NodeId};
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
        /// The lane the attempt is numbered in: `None` the fabric lane (a topology Build's
        /// attempts), `Some(mesh)` that mesh's drift lane.
        mesh: Option<String>,
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
    /// The receiver may not claim on `mesh`: a topology attempt is claimed by the fabric-primary,
    /// a drift attempt by its mesh's mesh-primary. Both are named as the receiver sees them.
    NotClaimant {
        /// The mesh the attempt acts in, when the receiver's view names one.
        mesh: Option<String>,
        /// The fabric primary the receiver sees, when it sees one.
        fabric_primary: Option<String>,
        /// The mesh's mesh-primary the receiver sees, when it sees one.
        mesh_primary: Option<String>,
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
    /// The receiver is connections-deleting and takes no new work.
    ConnectionsDeleting {
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
    /// Another attempt holds the mesh the attempt acts in (one attempt in flight per mesh, R-BI7):
    /// nothing is claimed.
    BuildInProgress {
        /// The Build of the attempt holding the mesh.
        current_build_id: String,
        /// The mesh.
        mesh: String,
        /// The admin that claimed the attempt holding it, when it was recorded.
        held_by: Option<String>,
    },
}

impl BuildClaimReply {
    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Won { .. } => "won",
            Self::Lost { .. } => "lost",
            Self::NotOpen { .. } => "not-open",
            Self::NotClaimant { .. } => "not-claimant",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::NotReady { .. } => "not-ready",
            Self::Busy { .. } => "busy",
            Self::ConnectionsDeleting { .. } => "connections-deleting",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::BuildInProgress { .. } => "build-in-progress",
        }
    }
}

impl NodeProtocol for BuildClaim {
    const OP: u8 = 0x1C;
    const NAME: &'static str = "build-claim";
    /// A Build id, an attempt, a node id, an incarnation id and a mesh name.
    const MAX_REQUEST_FRAME_BYTES: usize = 512;
    /// A reply carries at most one `CallContext`: a traceparent, a tracestate (512 bytes) and
    /// baggage (8192 bytes), with room for the enum tag and the string lengths.
    const MAX_REPLY_FRAME_BYTES: usize = 9216;
    /// The claimant decides its own claims; a carried claim would be decided by no one.
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 11;

    type Request = BuildClaimRequest;
    type Reply = BuildClaimReply;

    fn classify_reply(reply: &BuildClaimReply) -> ReplyKind {
        match reply {
            BuildClaimReply::Won { .. } => ReplyKind::Success,
            BuildClaimReply::Lost { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            BuildClaimReply::NotOpen { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildClaimReply::NotClaimant { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildClaimReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            BuildClaimReply::NotReady { .. } => ReplyKind::NotReady,
            BuildClaimReply::Busy { .. } => ReplyKind::Busy,
            BuildClaimReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            BuildClaimReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            BuildClaimReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            BuildClaimReply::BuildInProgress { .. } => ReplyKind::new(crate::outcome::Code::Unavailable),
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
    fn connections_deleting(reason: String) -> BuildClaimReply {
        BuildClaimReply::ConnectionsDeleting { reason }
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
            (BuildClaimReply::Lost { holder: "mesh1.admin.1".into() }, ReplyKind::new(crate::outcome::Code::Aborted)),
            (BuildClaimReply::NotOpen { next: Some(7) }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (BuildClaimReply::NotClaimant { mesh: Some("mesh2".into()), fabric_primary: Some("mesh1.admin.1".into()), mesh_primary: Some("mesh2.admin.1".into()) }, ReplyKind::new(crate::outcome::Code::FailedPrecondition)),
            (BuildClaim::peer_unresolved("p".into()), ReplyKind::PeerUnresolved),
            (BuildClaim::not_ready("n".into()), ReplyKind::NotReady),
            (BuildClaim::busy("b".into()), ReplyKind::Busy),
            (BuildClaim::connections_deleting("d".into()), ReplyKind::ConnectionsDeleting),
            (BuildClaim::malformed(MalformedKind::Corrupt), ReplyKind::Malformed(MalformedKind::Corrupt)),
            (BuildClaim::unauthorized("u".into()), ReplyKind::Unauthorized),
            (BuildClaimReply::BuildInProgress { current_build_id: "b7swg3jj790z".into(), mesh: "mesh2".into(), held_by: Some("mesh1.admin.1".into()) }, ReplyKind::new(crate::outcome::Code::Unavailable)),
        ];
        assert_eq!(all.len() as u32, BuildClaim::REPLY_VARIANTS);
        for (r, kind) in all {
            let back = BuildClaim::decode_reply(&BuildClaim::encode_reply(&r).unwrap()).unwrap();
            assert_eq!(back, r);
            assert_eq!(BuildClaim::classify_reply(&back), kind);
        }
        let q = BuildClaimRequest::ClaimAttempt { build_id: "b7swg3jj790z".into(), attempt: 3, executor_node_id: NodeId::mint(), executor_incarnation: IncarnationId::mint(), mesh: Some("mesh2".into()) };
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
