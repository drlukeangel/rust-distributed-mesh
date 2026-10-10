//! The fabric-primary handover on op `0x21` (node-rpc-envelope.md "Fabric-primary-handover, op
//! `0x21`, rdm"; lifecycles/fabric-primary-handover.md; R-ST2).
//!
//! The incumbent fabric-primary calls [`FabricPrimaryHandoverRequest::TakeFabricPrimary`] on the
//! exact successor birth; the successor durably commits the seat at `expected_epoch + 1`,
//! publishes new-fabric-primary and calls [`FabricPrimaryHandoverRequest::FabricPrimaryTaken`] on
//! the incumbent. `Applied` acknowledges a call; only the matched `FabricPrimaryTaken` confirms
//! the transfer. Both calls are unary and not forwardable; only node-admins serve the family.
//!
//! The doc's reply enum holds nine variants. The family sits on the framework's six outcomes, so
//! `PeerUnresolved`, `Busy`, `Draining` and `Unauthorized` are appended after the doc's nine;
//! positions 0..=8 are the doc's order.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use rafka_mesh_entity::{IncarnationId, NodeId};
use serde::{Deserialize, Serialize};

/// The fabric-primary handover protocol.
pub struct FabricPrimaryHandover;

/// An exact birth named by a handover: its mesh, node and incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HandoverBirth {
    /// The mesh the birth belongs to.
    pub mesh: String,
    /// The birth's node.
    pub node_id: NodeId,
    /// The birth's incarnation.
    pub incarnation: IncarnationId,
}

/// The two directed calls of a handover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FabricPrimaryHandoverRequest {
    /// Incumbent to successor: take the fabric seat at `expected_epoch + 1`.
    TakeFabricPrimary {
        /// The fabric.
        fabric: String,
        /// The current fabric-primary.
        incumbent: HandoverBirth,
        /// The selected outside mesh-primary.
        successor: HandoverBirth,
        /// The incumbent's seat epoch.
        expected_epoch: u64,
        /// The handover's operation identity.
        operation: String,
    },
    /// Successor to incumbent: the transfer is committed at `committed_epoch`.
    FabricPrimaryTaken {
        /// The fabric.
        fabric: String,
        /// The former fabric-primary.
        incumbent: HandoverBirth,
        /// The new fabric-primary.
        successor: HandoverBirth,
        /// The incumbent's seat epoch the command named.
        expected_epoch: u64,
        /// The epoch the successor committed.
        committed_epoch: u64,
        /// The handover's operation identity.
        operation: String,
    },
}

impl FabricPrimaryHandoverRequest {
    /// The request's name as it appears in spans and replies.
    pub fn op(&self) -> &'static str {
        match self {
            Self::TakeFabricPrimary { .. } => "take-fabric-primary",
            Self::FabricPrimaryTaken { .. } => "fabric-primary-taken",
        }
    }
}

/// The answer to a handover call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FabricPrimaryHandoverReply {
    /// The call was accepted.
    Applied,
    /// An identical call was accepted before; its recorded result stands.
    AlreadyApplied,
    /// The command's epoch is not the seat's.
    RejectedStaleEpoch {
        /// The epoch the call named.
        expected: u64,
        /// The epoch the receiver holds.
        current: u64,
    },
    /// A birth the call names is not the receiver's or the caller's exact birth.
    RejectedWrongBirth {
        /// Which birth: `incumbent`, `successor` or `sender`.
        role: String,
    },
    /// The caller does not hold the authority the call needs.
    RejectedNotAuthority,
    /// The successor is not an eligible Ready mesh-primary outside the incumbent's mesh.
    RejectedNotEligible {
        /// Why it is not.
        reason: String,
    },
    /// The operation identity names a different payload, or the fabric is not the receiver's.
    RejectedConflict {
        /// The operation in conflict.
        operation: String,
    },
    /// The receiver cannot decide yet.
    NotReady {
        /// What it waits for.
        reason: String,
    },
    /// The request frame was malformed.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The peer the call needed could not be resolved.
    PeerUnresolved {
        /// Why.
        reason: String,
    },
    /// The receiver is at its admission bound.
    Busy {
        /// Which bound.
        reason: String,
    },
    /// The receiver is draining and takes no new work.
    Draining {
        /// Why.
        reason: String,
    },
    /// The caller is not allowed this call.
    Unauthorized {
        /// Why.
        reason: String,
    },
}

impl FabricPrimaryHandoverReply {
    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::AlreadyApplied => "already-applied",
            Self::RejectedStaleEpoch { .. } => "rejected-stale-epoch",
            Self::RejectedWrongBirth { .. } => "rejected-wrong-birth",
            Self::RejectedNotAuthority => "rejected-not-authority",
            Self::RejectedNotEligible { .. } => "rejected-not-eligible",
            Self::RejectedConflict { .. } => "rejected-conflict",
            Self::NotReady { .. } => "not-ready",
            Self::Malformed { .. } => "malformed",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::Busy { .. } => "busy",
            Self::Draining { .. } => "draining",
            Self::Unauthorized { .. } => "unauthorized",
        }
    }
}

impl NodeProtocol for FabricPrimaryHandover {
    const OP: u8 = 0x21;
    const NAME: &'static str = "fabric-primary-handover";
    /// A fabric name, two exact births, two epochs and an operation identity.
    const MAX_REQUEST_FRAME_BYTES: usize = 2048;
    /// One reply: an enum tag and at most one reason string.
    const MAX_REPLY_FRAME_BYTES: usize = 1024;
    /// A handover is decided by the two exact births themselves; a carried call is decided by no one.
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 2;
    const REPLY_VARIANTS: u32 = 13;

    type Request = FabricPrimaryHandoverRequest;
    type Reply = FabricPrimaryHandoverReply;

    fn classify_reply(reply: &FabricPrimaryHandoverReply) -> ReplyKind {
        use FabricPrimaryHandoverReply::*;
        match reply {
            Applied | AlreadyApplied => ReplyKind::Success,
            RejectedStaleEpoch { .. } | RejectedWrongBirth { .. } | RejectedNotAuthority | RejectedNotEligible { .. } | RejectedConflict { .. } => ReplyKind::ProtocolRefusal,
            NotReady { .. } => ReplyKind::NotReady,
            Malformed { kind } => ReplyKind::Malformed(*kind),
            PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            Busy { .. } => ReplyKind::Busy,
            Draining { .. } => ReplyKind::Draining,
            Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> FabricPrimaryHandoverReply {
        FabricPrimaryHandoverReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> FabricPrimaryHandoverReply {
        FabricPrimaryHandoverReply::NotReady { reason }
    }
    fn busy(reason: String) -> FabricPrimaryHandoverReply {
        FabricPrimaryHandoverReply::Busy { reason }
    }
    fn draining(reason: String) -> FabricPrimaryHandoverReply {
        FabricPrimaryHandoverReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> FabricPrimaryHandoverReply {
        FabricPrimaryHandoverReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> FabricPrimaryHandoverReply {
        FabricPrimaryHandoverReply::Unauthorized { reason }
    }
}
