//! The fabric-primary handover on op `0x21` (node-rpc-envelope.md "Fabric-primary-handover, op
//! `0x21`, rdm"; lifecycles/fabric-primary-handover.md; R-ST2).
//!
//! The incumbent fabric-primary calls [`FabricPrimaryHandoverRequest::TakeFabricPrimary`] on the
//! exact successor birth; the successor durably commits the seat at `expected_epoch + 1`,
//! publishes new-fabric-primary and calls [`FabricPrimaryHandoverRequest::FabricPrimaryTaken`] on
//! the incumbent. `Applied` acknowledges a call; only the matched `FabricPrimaryTaken` confirms
//! the transfer. Both calls are unary and not forwardable; only node-admins serve the family.
//!
//! A Build's executor N calls [`FabricPrimaryHandoverRequest::TakeSeats`] on the exact seat holder
//! P it is about to drain, inside P's mesh ("TakeSeats: Build-driven seat transfer"): P commits one
//! durable row holding every seat it transfers at each seat's next epoch and answers
//! [`FabricPrimaryHandoverReply::SeatsCommitted`]; a seat epoch that is not P's is
//! [`FabricPrimaryHandoverReply::RejectedStaleSeat`]. A seat is the existing
//! [`rafka_mesh_entity::Seat`] (the mesh is the holder birth's).
//!
//! The doc's reply enum holds nine variants. The family sits on the framework's six outcomes, so
//! `PeerUnresolved`, `Busy`, `Draining` and `Unauthorized` are appended after the doc's nine;
//! positions 0..=8 are the doc's order, and the seat transfer's two replies follow at 13 and 14.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use rafka_mesh_entity::{CommittedSeat, IncarnationId, NodeId, Seat};
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

/// A seat a [`FabricPrimaryHandoverRequest::TakeSeats`] moves, with the epoch the caller holds for it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeatEpoch {
    /// The seat.
    pub seat: Seat,
    /// The seat's current epoch as the caller holds it.
    pub expected_epoch: u64,
}

/// The directed calls of a handover and of a Build-driven seat transfer.
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
    /// A Build's executor to the seat holder it is about to drain: commit every seat named to the
    /// executor at its next epoch.
    TakeSeats {
        /// The fabric.
        fabric: String,
        /// The exact birth that holds the seats.
        holder: HandoverBirth,
        /// The executor of the SeatTransfer step: the admin the election seats once the holder's
        /// birth is gone.
        successor: HandoverBirth,
        /// Every seat the holder holds, each with its current epoch.
        seats: Vec<SeatEpoch>,
        /// The Build whose attempt the executor won.
        build_id: String,
        /// The attempt the executor won.
        attempt: u32,
        /// The drain, stop, restart or replace operation the transfer precedes.
        operation: String,
    },
}

impl FabricPrimaryHandoverRequest {
    /// The request's name as it appears in spans and replies.
    pub fn op(&self) -> &'static str {
        match self {
            Self::TakeFabricPrimary { .. } => "take-fabric-primary",
            Self::FabricPrimaryTaken { .. } => "fabric-primary-taken",
            Self::TakeSeats { .. } => "take-seats",
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
    /// The seat holder committed the transfer: every seat it moved, at its committed epoch.
    SeatsCommitted {
        /// The committed seats.
        seats: Vec<CommittedSeat>,
    },
    /// A seat's epoch is not the one the call named.
    RejectedStaleSeat {
        /// The seat.
        seat: Seat,
        /// The epoch the call named.
        expected: u64,
        /// The epoch the holder holds.
        current: u64,
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
            Self::SeatsCommitted { .. } => "seats-committed",
            Self::RejectedStaleSeat { .. } => "rejected-stale-seat",
        }
    }
}

impl NodeProtocol for FabricPrimaryHandover {
    const OP: u8 = 0x21;
    const NAME: &'static str = "fabric-primary-handover";
    /// A fabric name, two exact births, at most two seat epochs (or two epochs), a Build id and an
    /// operation identity.
    const MAX_REQUEST_FRAME_BYTES: usize = 2048;
    /// One reply: an enum tag and at most one reason string, or two committed seats (a seat, an
    /// exact birth and an epoch each).
    const MAX_REPLY_FRAME_BYTES: usize = 1024;
    /// A handover is decided by the two exact births themselves; a carried call is decided by no one.
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 3;
    const REPLY_VARIANTS: u32 = 15;

    type Request = FabricPrimaryHandoverRequest;
    type Reply = FabricPrimaryHandoverReply;

    fn classify_reply(reply: &FabricPrimaryHandoverReply) -> ReplyKind {
        use FabricPrimaryHandoverReply::*;
        match reply {
            Applied | AlreadyApplied | SeatsCommitted { .. } => ReplyKind::Success,
            RejectedStaleEpoch { .. } | RejectedWrongBirth { .. } | RejectedNotAuthority | RejectedNotEligible { .. } | RejectedConflict { .. } | RejectedStaleSeat { .. } => ReplyKind::ProtocolRefusal,
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
