//! Generic one-hop carried execution (node-rpc.md §36.1; i143.e6.s4) on core op `0x1A`.
//!
//! The origin asks a carrier to make exactly one direct inner call to an exact final target. The
//! request carries the target, the inner protocol op and the opaque inner request bytes; no origin
//! identity rides in it, because the target authenticates the carrier, never a copied origin.
//!
//! The carrier verifies the inner protocol is forwardable, makes one direct inner call, never
//! forwards again, and hands back the inner outcome without inventing domain semantics. The
//! forward protocol itself is never forwardable.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::NodeId;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The Forward protocol: a carrier makes one inner call to an exact final target for a caller that
/// cannot reach it directly.
pub struct Forward;

/// The largest inner request or reply a carrier hands across.
pub(crate) const MAX_CARRIED_BYTES: usize = 1024 * 1024;

/// What the carrier keeps back from the origin's remaining budget so its reply still reaches the
/// origin inside that budget: the request's transit from origin to carrier (the origin measures
/// before it writes), the carrier's work after its inner call ends (edge lookup, reply encoding),
/// the reply's write and transit back, and the origin's read and decode. It is a bounded
/// transport and serialization allowance, not a policy margin.
pub const FORWARD_REPLY_RESERVE: Duration = Duration::from_millis(100);

/// A Forward call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardRequest {
    /// Make `inner_op` on `target` and hand its reply back.
    ///
    /// Position 0 on the wire.
    Forward {
        /// The exact final target's logical NodeId.
        target: NodeId,
        /// The inner call's op.
        inner_op: u8,
        /// The inner call's request frame, as encoded for `inner_op`.
        inner: Vec<u8>,
        /// The origin's remaining time budget for this call, in whole milliseconds, measured
        /// immediately before this frame is written to the carrier (after the carrier was
        /// resolved and dialed). For an overall budget it is deadline minus now; for a split
        /// budget it is the reply budget, which starts at the commit that follows the write. The
        /// carrier bounds its one inner call by this minus [`FORWARD_REPLY_RESERVE`].
        remaining_ms: u64,
        /// An Owed request's own delivery identity, which its carried attempt keeps toward the
        /// target under this frame's sender (node-rpc.md §43; i146 Q52). `None`: the inner call
        /// keeps this frame's identity.
        owed: Option<OwedDelivery>,
    },
}

/// The delivery identity an Owed request keeps on every leg: each carried attempt is a Forward
/// invocation of its own toward the carrier, and the inner call is the request's, so a carrier that
/// answered one attempt carries the next and the target's duplicate window suppresses any repeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwedDelivery {
    /// The request's `delivery_seq` in its origin birth's window.
    pub delivery_seq: u64,
    /// The origin's `forget_below` toward the target.
    pub forget_below: u64,
}

/// The typed reason an inner invocation has no reply; the explanation is a separate field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndeterminateCause {
    /// The receiving birth is proven exited or removed.
    ///
    /// Position 0 on the wire.
    ReceiverGone,
    /// The owner's reply bound lapsed after commit.
    ///
    /// Position 1 on the wire.
    ReplyDeadline,
    /// The reply was lost after commit.
    ///
    /// Position 2 on the wire.
    ReplyLost,
    /// A reset after commit; its numeric code is evidence.
    ///
    /// Position 3 on the wire.
    Reset(u64),
    /// The caller cannot decode this reply variant.
    ///
    /// Position 4 on the wire.
    UnsupportedReplyVariant,
    /// The reply bytes are corrupt.
    ///
    /// Position 5 on the wire.
    CorruptReply,
    /// The response violated its framing or ordering contract.
    ///
    /// Position 6 on the wire.
    ProtocolViolation,
}

/// The typed reason an inner invocation did not reach its commit cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotSentCause {
    /// The resolver's typed result.
    ///
    /// Position 0 on the wire.
    Resolve(crate::outcome::ResolveFailure),
    /// Dial or stream open failed before commit.
    ///
    /// Position 1 on the wire.
    Connection,
    /// The permit was not available before the owner's deadline.
    ///
    /// Position 2 on the wire.
    StreamBudget,
    /// The owner's bound lapsed before commit.
    ///
    /// Position 3 on the wire.
    Deadline,
    /// The unfinished request was cut.
    ///
    /// Position 4 on the wire.
    FrameNotSent,
    /// A carrier's own refusal proved no dispatch.
    ///
    /// Position 5 on the wire.
    Carried,
    /// The carrier's own edge was lost.
    ///
    /// Position 6 on the wire.
    ConnectionLost,
    /// The remaining budget did not exceed the reserve.
    ///
    /// Position 7 on the wire.
    CarrierNoBudget {
        /// The origin's remaining milliseconds.
        remaining_ms: u64,
        /// The reply reserve in milliseconds.
        reserve_ms: u64,
    },
    /// The requested op cannot be forwarded.
    ///
    /// Position 8 on the wire.
    NotForwardable {
        /// The inner op.
        op: u8,
    },
    /// No active route was selected.
    ///
    /// Position 9 on the wire.
    NoActiveRoute,
    /// The source admits no new connection.
    ///
    /// Position 10 on the wire.
    AdmissionBlocked,
}

/// A carrier's answer to a Forward call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardReply {
    /// The inner call reached the target, which answered: its reply payload, verbatim.
    ///
    /// Position 0 on the wire.
    Forwarded {
        /// The inner reply payload.
        inner: Vec<u8>,
    },
    /// The carrier proved the inner call never committed at the target.
    ///
    /// Position 1 on the wire.
    InnerNotSent {
        /// Why the inner call never committed.
        reason: String,
        /// The original typed cause, independent of the explanation.
        cause: NotSentCause,
    },
    /// The target does not serve the inner op.
    ///
    /// Position 2 on the wire.
    InnerUnserved {
        /// The op the target does not serve.
        op: u8,
    },
    /// The carrier's resolver stopped naming the birth the inner call was dialled to, before the
    /// inner request went: never dispatched (no receiver refuses a call for the node id it names).
    ///
    /// Position 3 on the wire.
    InnerRejectedStale {
        /// The node the inner call addressed.
        target_node_id: NodeId,
    },
    /// The inner call committed at the target and its outcome is unknown.
    ///
    /// Position 4 on the wire.
    InnerIndeterminate {
        /// Why the outcome is unknown.
        reason: String,
        /// The original typed cause, independent of the explanation.
        cause: IndeterminateCause,
    },
    /// The inner protocol is not forwardable through this carrier.
    ///
    /// Position 5 on the wire.
    NotForwardable {
        /// The inner op that is not forwardable.
        op: u8,
    },
    /// The peer the call needed could not be resolved.
    ///
    /// Position 6 on the wire.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The carrier is not ready to serve.
    ///
    /// Position 7 on the wire.
    NotReady {
        /// Why the carrier is not ready.
        reason: String,
    },
    /// The carrier is at its admission bound.
    ///
    /// Position 8 on the wire.
    Busy {
        /// Which bound the carrier is at.
        reason: String,
    },
    /// The carrier is connections-deleting and takes no new work.
    ///
    /// Position 9 on the wire.
    ConnectionsDeleting {
        /// Why the carrier refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 10 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    ///
    /// Position 11 on the wire.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
    /// The carrier made no inner call it could prove arrived: its own Direct edge to the final
    /// target is not Active (connections.md §8). `reason` names the carrier's latest Direct fact
    /// toward the target. The origin deletes its Proxy through this carrier with the structural
    /// reason `connection-lost`.
    ///
    /// Position 12 on the wire.
    ConnectionLost {
        /// The carrier's latest Direct fact toward the target.
        reason: String,
    },
    /// The origin's remaining budget does not exceed [`FORWARD_REPLY_RESERVE`], so the carrier
    /// made no inner call. Both are whole milliseconds.
    ///
    /// Position 13 on the wire.
    NoInnerBudget {
        /// The origin's remaining budget.
        remaining_ms: u64,
        /// The carrier's reply reserve.
        reserve_ms: u64,
    },
}

impl IndeterminateCause {
    /// Capture the typed cause at the carrier before it formats the explanation.
    pub fn from_reason(reason: &crate::outcome::IndeterminateReason) -> Self {
        use crate::outcome::IndeterminateReason as R;
        match reason {
            R::ReceiverGone(_) => Self::ReceiverGone,
            R::ReplyDeadline => Self::ReplyDeadline,
            R::ReplyLost(_) => Self::ReplyLost,
            R::Reset(n) => Self::Reset(*n),
            R::UnsupportedReplyVariant => Self::UnsupportedReplyVariant,
            R::CorruptReply => Self::CorruptReply,
            R::ProtocolViolation(_) => Self::ProtocolViolation,
        }
    }
    /// Rebuild the exact typed reason at the origin; the text has no deciding role.
    pub fn into_reason(self, explanation: String) -> crate::outcome::IndeterminateReason {
        use crate::outcome::IndeterminateReason as R;
        match self {
            Self::ReceiverGone => R::ReceiverGone(explanation),
            Self::ReplyDeadline => R::ReplyDeadline,
            Self::ReplyLost => R::ReplyLost(explanation),
            Self::Reset(n) => R::Reset(n),
            Self::UnsupportedReplyVariant => R::UnsupportedReplyVariant,
            Self::CorruptReply => R::CorruptReply,
            Self::ProtocolViolation => R::ProtocolViolation(explanation),
        }
    }
    /// The standard code carried by this cause.
    pub fn code(self) -> crate::outcome::Code { self.into_reason(String::new()).code() }
}

impl NotSentCause {
    /// Capture the typed pre-commit reason, retaining its structured fields.
    pub fn from_reason(reason: &crate::outcome::NotSentReason) -> Self {
        use crate::outcome::NotSentReason as R;
        match reason {
            R::Resolve(why) => Self::Resolve(*why),
            R::Connection(_) => Self::Connection,
            R::StreamBudget => Self::StreamBudget,
            R::Deadline => Self::Deadline,
            R::FrameNotSent => Self::FrameNotSent,
            R::Carried(_) => Self::Carried,
            R::ConnectionLost(_) => Self::ConnectionLost,
            R::CarrierNoBudget { remaining_ms, reserve_ms } => Self::CarrierNoBudget { remaining_ms: *remaining_ms, reserve_ms: *reserve_ms },
            R::NotForwardable { op } => Self::NotForwardable { op: *op },
            R::NoActiveRoute => Self::NoActiveRoute,
            R::AdmissionBlocked(_) => Self::AdmissionBlocked,
        }
    }
    /// Rebuild the exact pre-commit reason without interpreting the text.
    pub fn into_reason(self, explanation: String) -> crate::outcome::NotSentReason {
        use crate::outcome::NotSentReason as R;
        match self {
            Self::Resolve(why) => R::Resolve(why),
            Self::Connection => R::Connection(explanation),
            Self::StreamBudget => R::StreamBudget,
            Self::Deadline => R::Deadline,
            Self::FrameNotSent => R::FrameNotSent,
            Self::Carried => R::Carried(explanation),
            Self::ConnectionLost => R::ConnectionLost(explanation),
            Self::CarrierNoBudget { remaining_ms, reserve_ms } => R::CarrierNoBudget { remaining_ms, reserve_ms },
            Self::NotForwardable { op } => R::NotForwardable { op },
            Self::NoActiveRoute => R::NoActiveRoute,
            Self::AdmissionBlocked => R::AdmissionBlocked(explanation),
        }
    }
    /// The standard code carried by this cause.
    pub fn code(self) -> crate::outcome::Code { self.into_reason(String::new()).code() }
}

impl ForwardReply {
    /// Code of the carrier's own typed answer; the final protocol classifies a Forwarded body.
    pub fn code(&self) -> crate::outcome::Code {
        use crate::outcome::{Code, MalformedKind};
        match self {
            Self::Forwarded { .. } => Code::Ok,
            Self::InnerNotSent { cause, .. } => cause.code(),
            Self::InnerIndeterminate { cause, .. } => cause.code(),
            Self::InnerUnserved { .. } => Code::Unimplemented,
            Self::InnerRejectedStale { .. } => Code::Aborted,
            Self::NotForwardable { .. } => Code::FailedPrecondition,
            Self::ConnectionLost { .. } | Self::NoInnerBudget { .. } | Self::NotReady { .. } | Self::Busy { .. } | Self::ConnectionsDeleting { .. } => Code::Unavailable,
            Self::PeerUnresolved { .. } => Code::NotFound,
            Self::Malformed { kind: MalformedKind::TooLarge | MalformedKind::TypesOverCap } => Code::ResourceExhausted,
            Self::Malformed { kind: MalformedKind::UnknownVariant } => Code::InvalidArgument,
            Self::Malformed { kind: MalformedKind::Corrupt } => Code::DataLoss,
            Self::Unauthorized { .. } => Code::PermissionDenied,
        }
    }
}

impl NodeProtocol for Forward {
    const OP: u8 = 0x1A;
    const NAME: &'static str = "forward";
    const MAX_REQUEST_FRAME_BYTES: usize = MAX_CARRIED_BYTES + 256;
    const MAX_REPLY_FRAME_BYTES: usize = MAX_CARRIED_BYTES + 256;
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 14;

    type Request = ForwardRequest;
    type Reply = ForwardReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            ForwardReply::Forwarded { .. } => "forwarded",
            ForwardReply::InnerNotSent { .. } => "inner-not-sent",
            ForwardReply::InnerUnserved { .. } => "inner-unserved",
            ForwardReply::InnerRejectedStale { .. } => "inner-rejected-stale",
            ForwardReply::InnerIndeterminate { .. } => "inner-indeterminate",
            ForwardReply::NotForwardable { .. } => "not-forwardable",
            ForwardReply::PeerUnresolved { .. } => "peer-unresolved",
            ForwardReply::NotReady { .. } => "not-ready",
            ForwardReply::Busy { .. } => "busy",
            ForwardReply::ConnectionsDeleting { .. } => "connections-deleting",
            ForwardReply::Malformed { .. } => "malformed",
            ForwardReply::Unauthorized { .. } => "unauthorized",
            ForwardReply::ConnectionLost { .. } => "connection-lost",
            ForwardReply::NoInnerBudget { .. } => "no-inner-budget",
        }
    }

    fn classify_reply(reply: &ForwardReply) -> ReplyKind {
        match reply {
            ForwardReply::Forwarded { .. } => ReplyKind::Success,
            ForwardReply::InnerNotSent { .. } => ReplyKind::new(reply.code()),
            ForwardReply::InnerUnserved { .. } => ReplyKind::new(reply.code()),
            ForwardReply::InnerRejectedStale { .. } => ReplyKind::new(reply.code()),
            ForwardReply::InnerIndeterminate { .. } => ReplyKind::new(reply.code()),
            ForwardReply::NotForwardable { .. } => ReplyKind::new(reply.code()),
            ForwardReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            ForwardReply::NotReady { .. } => ReplyKind::NotReady,
            ForwardReply::Busy { .. } => ReplyKind::Busy,
            ForwardReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            ForwardReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            ForwardReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            ForwardReply::ConnectionLost { .. } => ReplyKind::new(reply.code()),
            ForwardReply::NoInnerBudget { .. } => ReplyKind::new(reply.code()),
        }
    }

    fn peer_unresolved(reason: String) -> ForwardReply {
        ForwardReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> ForwardReply {
        ForwardReply::NotReady { reason }
    }
    fn busy(reason: String) -> ForwardReply {
        ForwardReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> ForwardReply {
        ForwardReply::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> ForwardReply {
        ForwardReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> ForwardReply {
        ForwardReply::Unauthorized { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_forward_protocol_is_never_itself_forwardable() {
        assert!(!Forward::FORWARDABLE);
        assert_eq!(Forward::OP, 0x1A);
    }

    #[test]
    fn every_reply_variant_round_trips() {
        let replies = [
            ForwardReply::Forwarded { inner: vec![1] },
            ForwardReply::InnerNotSent { reason: "r".into(), cause: NotSentCause::Connection },
            ForwardReply::InnerUnserved { op: 7 },
            ForwardReply::InnerRejectedStale { target_node_id: NodeId::mint() },
            ForwardReply::InnerIndeterminate { reason: "r".into(), cause: IndeterminateCause::ReplyLost },
            ForwardReply::NotForwardable { op: 0x01 },
            ForwardReply::PeerUnresolved { reason: "p".into() },
            ForwardReply::NotReady { reason: "n".into() },
            ForwardReply::Busy { reason: "b".into() },
            ForwardReply::ConnectionsDeleting { reason: "d".into() },
            ForwardReply::Malformed { kind: MalformedKind::Corrupt },
            ForwardReply::Unauthorized { reason: "u".into() },
            ForwardReply::ConnectionLost { reason: "e".into() },
            ForwardReply::NoInnerBudget { remaining_ms: 5, reserve_ms: 100 },
        ];
        assert_eq!(replies.len() as u32, Forward::REPLY_VARIANTS);
        for r in replies {
            assert_eq!(Forward::decode_reply(&Forward::encode_reply(&r).unwrap()).unwrap(), r);
        }
    }
}
