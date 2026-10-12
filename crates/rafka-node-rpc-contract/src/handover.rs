//! The fabric-primary handover on op `0x21` (node-rpc-envelope.md "Fabric-primary-handover, op
//! `0x21`, rdm"; lifecycles/fabric-primary-handover.md; R-ST2).
//!
//! The incumbent fabric-primary calls [`FabricPrimaryHandoverRequest::TakeFabricPrimary`] on the
//! exact successor birth; the successor durably commits the seat at `expected_epoch + 1`,
//! publishes new-fabric-primary and calls [`FabricPrimaryHandoverRequest::FabricPrimaryTaken`] on
//! the incumbent. `Applied` acknowledges a call; only the matched `FabricPrimaryTaken` confirms
//! the transfer. Both calls are unary and not forwardable; only node-admins serve the family.
//!
//! node.primary.update (R-MT1, Luke 2026-10-12): commands only go down. The fabric-primary calls
//! [`FabricPrimaryHandoverRequest::UpdatePrimary`] on the exact primary NA1 of a (mesh, node type)
//! cohort; NA1 computes its successor NA2 in its cohort and calls
//! [`FabricPrimaryHandoverRequest::TakeSeats`] (node.primary.update) on NA2; NA2 writes its own seat
//! row at each seat's next epoch and answers [`FabricPrimaryHandoverReply::SeatsCommitted`]
//! (node.primary.updated), which NA1 hands back up as its own answer. A seat epoch that is not the
//! one NA2 holds for NA1 is [`FabricPrimaryHandoverReply::RejectedStaleSeat`]. [`Seat`],
//! [`CommittedSeat`] and [`HandoverBirth`] are the doc's shapes (`rafka_mesh_entity::seat::transfer`,
//! which the `SeatedTogether` gossip frame carries too).
//!
//! The doc's reply enum holds nine variants. The family sits on the framework's six outcomes, so
//! `PeerUnresolved`, `Busy`, `Draining` and `Unauthorized` are appended after the doc's nine;
//! positions 0..=8 are the doc's order, and the seat transfer's two replies follow at 13 and 14.
//! A seat epoch at `u64::MAX` that a call would advance is [`FabricPrimaryHandoverReply::RejectedEpochOverflow`],
//! at 15; a seat the holder holds that a TakeSeats leaves out is [`FabricPrimaryHandoverReply::RejectedSeatOmitted`],
//! at 16; a store or a read the receiver needed that failed is
//! [`FabricPrimaryHandoverReply::RejectedSourceFailed`], at 17; a record the receiver needed that was
//! never written where it looked is [`FabricPrimaryHandoverReply::RejectedRecordAbsent`], at 18.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
pub use rafka_mesh_entity::seat::transfer::{CommittedSeat, HandoverBirth, Seat};
use serde::{Deserialize, Serialize};

/// The fabric-primary handover protocol.
pub struct FabricPrimaryHandover;

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
    /// node.primary.update, the current primary NA1 to the successor NA2 it computed (R-MT1): take
    /// every seat named at its next epoch, writing the seat row as the cohort's new writer.
    TakeSeats {
        /// The fabric.
        fabric: String,
        /// The exact birth that holds the seats: the caller, NA1.
        holder: HandoverBirth,
        /// The successor NA1 computed in its cohort: the receiver, NA2.
        successor: HandoverBirth,
        /// Every seat that moves, each with the epoch NA1 holds it at.
        seats: Vec<SeatEpoch>,
        /// The Build whose stop, delete, restart, replace or connections delete moves the seats.
        build_id: String,
        /// That Build's attempt.
        attempt: u32,
        /// The operation the update precedes.
        operation: String,
    },
    /// node.primary.update commanded from above (R-MT1: "only a FP should issue commands to a MP"):
    /// the fabric-primary to the exact primary `holder` of a cohort. The holder computes its
    /// successor, moves the seats named by [`Self::TakeSeats`] and answers the successor's
    /// `SeatsCommitted`.
    UpdatePrimary {
        /// The fabric.
        fabric: String,
        /// The exact birth that holds the seats: the receiver.
        holder: HandoverBirth,
        /// Every seat that moves, each with the epoch the commander holds it at.
        seats: Vec<SeatEpoch>,
        /// The Build whose stop, delete, restart, replace or connections delete moves the seats.
        build_id: String,
        /// That Build's attempt.
        attempt: u32,
        /// The operation the update precedes.
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
            Self::UpdatePrimary { .. } => "update-primary",
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
    /// node.primary.updated: the successor committed the seats, every one at its committed epoch.
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
    /// A seat's epoch cannot advance: it is at `u64::MAX`, so no transfer can commit at its next
    /// epoch (node-rpc-envelope.md: "Epoch overflow cannot commit and is refused by name with a
    /// typed refusal"). Nothing is written.
    RejectedEpochOverflow {
        /// The seat whose epoch cannot advance.
        seat: Seat,
        /// The epoch it is at.
        epoch: u64,
    },
    /// A TakeSeats leaves out a seat the holder holds: every seat P holds moves in the one row, so a
    /// call that does not name one cannot commit (node-rpc-envelope.md, TakeSeats: the row holds "every
    /// seat P transfers"). Nothing is written.
    RejectedSeatOmitted {
        /// The seat the call does not name.
        seat: Seat,
        /// The epoch the holder holds it at.
        current: u64,
    },
    /// What the receiver had to read or write to decide or commit the call failed: its `fabric.storage`
    /// refused a put or could not be read, or a Build log it reads could not be read. A runtime fact of
    /// the receiver, never a readiness answer; nothing the call would have done was done. A record
    /// that was never written is not a failed read: it is [`Self::RejectedRecordAbsent`]. The fields are
    /// the Status arm's, within the same budgets ([`crate::status::source_failed_fields`]).
    RejectedSourceFailed {
        /// What failed, named with its owner and row.
        source: String,
        /// The operation on it that failed, with its subject.
        op: String,
        /// Why it failed: the store's or the peer's own error.
        cause: String,
    },
    /// A record the receiver needed to decide the call was never written where it looked: the
    /// admin holds no such record, so what the call names cannot be compared with it (CLAUDE.md
    /// error rule: "'Never written' and 'could not be read' are different errors"). Not a failed read
    /// ([`Self::RejectedSourceFailed`]) and never a readiness answer; nothing the call would have done
    /// was done. Within budgets ([`FabricPrimaryHandoverReply::record_absent`]).
    RejectedRecordAbsent {
        /// The record never written, named with its subject: `the fabric-primary seat record of
        /// fabric1`. Within [`MAX_RECORD_ABSENT_RECORD_BYTES`].
        record: String,
        /// The admin whose records hold no such record (its path.name). Within
        /// [`MAX_RECORD_ABSENT_HOLDER_BYTES`].
        holder: String,
    },
}

/// The most bytes of the `holder` a `RejectedRecordAbsent` names: a path.name, as a refusal names a
/// node ([`crate::status::MAX_REFUSAL_NODE_BYTES`]).
pub const MAX_RECORD_ABSENT_HOLDER_BYTES: usize = crate::status::MAX_REFUSAL_NODE_BYTES;
/// The most bytes of the `record` a `RejectedRecordAbsent` names: what is left of the 1024-byte reply
/// bound after the variant (one byte), the holder at its budget and a two-byte length prefix on each
/// string.
pub const MAX_RECORD_ABSENT_RECORD_BYTES: usize = 1024 - 1 - (2 + MAX_RECORD_ABSENT_HOLDER_BYTES) - 2;

impl FabricPrimaryHandoverReply {
    /// `RejectedSourceFailed` naming what failed (`source`), the operation on it (`op`) and why
    /// (`cause`), each within its budget so the reply always fits [`FabricPrimaryHandover`]'s reply
    /// bound.
    pub fn source_failed(source: &str, op: &str, cause: &str) -> Self {
        let (source, op, cause) = crate::status::source_failed_fields(source, op, cause);
        Self::RejectedSourceFailed { source, op, cause }
    }

    /// `RejectedRecordAbsent` naming the record never written (`record`) and the admin whose records
    /// hold no such record (`holder`), each within its budget so the reply always fits
    /// [`FabricPrimaryHandover`]'s reply bound.
    pub fn record_absent(record: &str, holder: &str) -> Self {
        Self::RejectedRecordAbsent { record: crate::status::within(record, MAX_RECORD_ABSENT_RECORD_BYTES), holder: crate::status::within(holder, MAX_RECORD_ABSENT_HOLDER_BYTES) }
    }

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
            Self::RejectedEpochOverflow { .. } => "rejected-epoch-overflow",
            Self::RejectedSeatOmitted { .. } => "rejected-seat-omitted",
            Self::RejectedSourceFailed { .. } => "rejected-source-failed",
            Self::RejectedRecordAbsent { .. } => "rejected-record-absent",
        }
    }
}

impl NodeProtocol for FabricPrimaryHandover {
    const OP: u8 = 0x21;
    const NAME: &'static str = "fabric-primary-handover";
    /// A fabric name, two exact births, at most two seat epochs (or two epochs), a Build id and an
    /// operation identity.
    const MAX_REQUEST_FRAME_BYTES: usize = 2048;
    /// One reply: an enum tag and at most one reason string, two committed seats (a seat, an exact
    /// birth and an epoch each), a source failure's three fields or an absent record's two, within
    /// their budgets.
    const MAX_REPLY_FRAME_BYTES: usize = 1024;
    /// A handover is decided by the two exact births themselves; a carried call is decided by no one.
    const FORWARDABLE: bool = false;
    /// A successor keeps serving TakeSeats after its seat row, and a former holder UpdatePrimary,
    /// through a drain: a lost reply's identical retry is answered from the row.
    const SERVED_WHILE_DRAINING: bool = true;
    const REQUEST_VARIANTS: u32 = 4;
    const REPLY_VARIANTS: u32 = 19;

    type Request = FabricPrimaryHandoverRequest;
    type Reply = FabricPrimaryHandoverReply;

    fn classify_reply(reply: &FabricPrimaryHandoverReply) -> ReplyKind {
        use FabricPrimaryHandoverReply::*;
        match reply {
            Applied | AlreadyApplied | SeatsCommitted { .. } => ReplyKind::Success,
            RejectedStaleEpoch { .. } | RejectedWrongBirth { .. } | RejectedNotAuthority | RejectedNotEligible { .. } | RejectedConflict { .. } | RejectedStaleSeat { .. } | RejectedEpochOverflow { .. } | RejectedSeatOmitted { .. } | RejectedSourceFailed { .. } | RejectedRecordAbsent { .. } => ReplyKind::ProtocolRefusal,
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
