//! node.primary.update and node.fabric-primary.update on op `0x21` (the approved proposal
//! docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md, Part C3 edit 5 and
//! Part D: "0x21 carries the primitive's `PrimaryUpdate` / `PrimaryUpdated` for both seats; the
//! incumbent writes the row").
//!
//! Commands only go down. The commander calls [`NodePrimaryUpdateRequest::UpdatePrimary`] on the
//! holder of a seat (the fabric-primary on a mesh-primary), and on the mesh-primary of a cohort that is
//! not node-admin (the fabric-primary's command for a mesh's node goes through its mesh-primary). For
//! such a cohort the mesh-primary is commander and row writer: it writes the one row, then calls
//! [`NodePrimaryUpdateRequest::UpdatePrimaryFromRow`] on the holder carrying the row's content, and the
//! holder makes the call on the successor below. The holder, its cohort's row writer,
//! pre-checks its successor, writes the ONE committed row for one or both seats, then calls
//! [`NodePrimaryUpdateRequest::PrimaryUpdate`] on the successor carrying the row's content: every seat
//! with its cohort, both exact births (never a name) and the committed revision. The successor installs
//! from the row and answers [`NodePrimaryUpdateReply::PrimaryUpdated`] (node.primary.updated), which the
//! holder hands back up as its own answer. The fabric seat is a second entry in `seats`, cohort
//! `fabric`. An identical repeat replays the committed result; a call naming the operation with
//! another payload is [`NodePrimaryUpdateReply::RejectedConflict`]; before the initial fabric first
//! reaches ready-for-traffic a call is [`NodePrimaryUpdateReply::RejectedFabricNotReady`]. No receiver
//! refuses a call for the birth it names (R-UC1a, R-RX5): there is no wrong-birth refusal, and
//! eligibility is the holder's pre-check before the row.
//!
//! [`Seat`], [`CommittedSeat`] and [`HandoverBirth`] are the doc's shapes
//! (`crate::seat::transfer`, which the "primary updating" and "primary updated" gossip
//! frames carry too). The family's outcomes sit on the framework's six: `NotReady`, `Malformed`,
//! `PeerUnresolved`, `Busy`, `ConnectionsDeleting` and `Unauthorized` follow the family's own replies.

use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
pub use crate::seat::transfer::{CommittedSeat, HandoverBirth, Seat};
use serde::{Deserialize, Serialize};

/// The node-primary-update protocol: node.primary.update and node.fabric-primary.update.
pub struct NodePrimaryUpdate;

/// A seat the commander names in [`NodePrimaryUpdateRequest::UpdatePrimary`], with the revision it holds
/// the seat at.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeatRevision {
    /// The seat.
    pub seat: Seat,
    /// The seat's current revision as the caller holds it.
    pub expected_revision: u64,
}

/// One seat the committed row moves, as [`NodePrimaryUpdateRequest::PrimaryUpdate`] carries it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeatUpdate {
    /// The seat's cohort: the mesh-primary seat of a mesh, or the fabric seat.
    pub seat: Seat,
    /// The exact birth that held the seat and wrote the row.
    pub holder: HandoverBirth,
    /// The exact birth the row commits the seat to.
    pub successor: HandoverBirth,
    /// The revision the row commits.
    pub revision: u64,
}

impl SeatUpdate {
    /// The committed seat it names.
    pub fn committed(&self) -> CommittedSeat {
        CommittedSeat { seat: self.seat.clone(), holder: self.successor.clone(), revision: self.revision }
    }
}

/// The directed calls of node.primary.update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodePrimaryUpdateRequest {
    /// node.primary.update, the holder to the successor its pre-check named, after its one committed
    /// row: install the seats the row commits and answer node.primary.updated.
    PrimaryUpdate {
        /// Every seat the row moves (one, or both when the holder held both, Q1).
        seats: Vec<SeatUpdate>,
        /// The Build whose stop, delete, restart, replace or node.connections.delete the update precedes.
        build_id: String,
        /// That Build's attempt.
        attempt: u32,
        /// The update's operation identity: the row's key.
        operation: String,
    },
    /// node.primary.update commanded from above: the commander to the holder of the seats named.
    /// The holder judges it by the seats it holds and their revisions, never by the birth it names.
    UpdatePrimary {
        /// The fabric.
        fabric: String,
        /// The exact birth the commander holds as the seats' holder.
        holder: HandoverBirth,
        /// Every seat that moves, each with the revision the commander holds it at.
        seats: Vec<SeatRevision>,
        /// The Build whose stop, delete, restart, replace or node.connections.delete moves the seats.
        build_id: String,
        /// That Build's attempt.
        attempt: u32,
        /// The update's operation identity.
        operation: String,
    },
    /// node.primary.update of a cohort that is not node-admin, commanded by its mesh-primary, the
    /// cohort's commander and row writer, after its one committed row and carrying the row's content
    /// (the approved proposal Part C1: "its command to the holder B1 comes after the row and carries
    /// it; B1 then makes the call of step 6"): the holder calls [`Self::PrimaryUpdate`] with these
    /// seats on the successor they name and answers with the successor's node.primary.updated.
    UpdatePrimaryFromRow {
        /// The fabric.
        fabric: String,
        /// The seat the row moves, its holder and successor births and its committed revision.
        seats: Vec<SeatUpdate>,
        /// The Build whose stop, delete, restart, replace or node.connections.delete the update precedes.
        build_id: String,
        /// That Build's attempt.
        attempt: u32,
        /// The update's operation identity: the row's key.
        operation: String,
    },
}

impl NodePrimaryUpdateRequest {
    /// The request's name as it appears in spans and replies.
    pub fn op(&self) -> &'static str {
        match self {
            Self::PrimaryUpdate { .. } => "primary-update",
            Self::UpdatePrimary { .. } => "update-primary",
            Self::UpdatePrimaryFromRow { .. } => "update-primary-from-row",
        }
    }
}

/// The answer to a node.primary.update call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodePrimaryUpdateReply {
    /// node.primary.updated: the successor installed the committed row, every seat at its revision.
    PrimaryUpdated {
        /// The committed seats: each seat, its committed holder's exact birth and its revision.
        revisions: Vec<CommittedSeat>,
    },
    /// The operation's committed row names another payload: the row decides.
    RejectedConflict {
        /// The operation in conflict.
        operation: String,
    },
    /// The initial fabric has not first reached ready-for-traffic (R-PB1): no seat moves.
    RejectedFabricNotReady {
        /// The workflow refused and the fabric's state.
        reason: String,
    },
    /// No successor (proposal Q7): `cannot transfer: no secondary of type <node_type> in mesh <mesh>`,
    /// or the fabric seat's `cannot transfer fabric seat: ...` naming both searches. Nothing changed.
    RejectedNoSuccessor {
        /// The refusal's exact text.
        reason: String,
    },
    /// The receiver holds no seat the command names.
    RejectedNotAuthority {
        /// What the receiver holds instead.
        reason: String,
    },
    /// A seat's revision is not the one the command named: the holder's pre-check of the call's revision.
    RejectedStaleSeat {
        /// The seat.
        seat: Seat,
        /// The revision the call named.
        expected: u64,
        /// The revision the holder holds.
        current: u64,
    },
    /// A seat's revision cannot advance: it is at `u64::MAX`, so no row can commit at its next revision
    /// (node-rpc-envelope.md: "Revision overflow cannot commit and is refused by name with a typed
    /// refusal"). Nothing is written.
    RejectedRevisionOverflow {
        /// The seat whose revision cannot advance.
        seat: Seat,
        /// The revision it is at.
        revision: u64,
    },
    /// What the receiver had to read or write to decide or commit the call failed: its `fabric.storage`
    /// refused a put or could not be read. A runtime fact of the receiver, never a readiness answer.
    /// The fields are the Status arm's, within the same budgets ([`crate::status::source_failed_fields`]).
    RejectedSourceFailed {
        /// What failed, named with its owner and row.
        source: String,
        /// The operation on it that failed, with its subject.
        op: String,
        /// Why it failed: the store's or the peer's own error.
        cause: String,
    },
    /// A record the receiver needed was never written where it looked (CLAUDE.md error rule: "'Never
    /// written' and 'could not be read' are different errors"): a call whose committed row the store
    /// root does not hold. Within budgets ([`NodePrimaryUpdateReply::record_absent`]).
    RejectedRecordAbsent {
        /// The record never written, named with its subject. Within [`MAX_RECORD_ABSENT_RECORD_BYTES`].
        record: String,
        /// The admin whose store holds no such record (its path.name). Within
        /// [`MAX_RECORD_ABSENT_HOLDER_BYTES`].
        holder: String,
    },
    /// The holder wrote its row, and its call on the successor did not answer node.primary.updated:
    /// the seat is the successor's from the row, and the update is incomplete until a later attempt
    /// obtains the successor's answer (proposal Part C1 failure contract).
    Incomplete {
        /// The call, the successor and how it ended.
        reason: String,
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
    /// The receiver is connections-deleting and takes no new work.
    ConnectionsDeleting {
        /// Why.
        reason: String,
    },
    /// The caller is not allowed this call.
    Unauthorized {
        /// Why.
        reason: String,
    },
    /// The commanded `node.primary.update` ended failed after the committed row (Q10, R-PU2, R-BI4):
    /// the Build carrier's own terminal (`Failed { step, reason }`), recording the step, the new
    /// holder's node id and incarnation, the seats the row moved with their revisions, who the
    /// ordinary rule seated, and the provider's proof that the new holder's process is gone.
    Failed {
        /// The step that failed (`primary-update`). Within [`MAX_FAILED_STEP_BYTES`].
        step: String,
        /// Why the step failed. Within [`MAX_FAILED_REASON_BYTES`].
        reason: String,
    },
}

/// The most bytes of the `holder` a `RejectedRecordAbsent` names: a path.name, as a refusal names a
/// node ([`crate::status::MAX_REFUSAL_NODE_BYTES`]).
pub const MAX_RECORD_ABSENT_HOLDER_BYTES: usize = crate::status::MAX_REFUSAL_NODE_BYTES;
/// The most bytes of the `record` a `RejectedRecordAbsent` names: what is left of the 1024-byte reply
/// bound after the variant (one byte), the holder at its budget and a two-byte length prefix on each
/// string.
pub const MAX_RECORD_ABSENT_RECORD_BYTES: usize = 1024 - 1 - (2 + MAX_RECORD_ABSENT_HOLDER_BYTES) - 2;

/// The most bytes of the `step` a `Failed` reply names.
pub const MAX_FAILED_STEP_BYTES: usize = 128;
/// The most bytes of the `reason` a `Failed` reply names: what is left of the 1024-byte reply bound
/// after the variant (one byte), `step` at its budget and a two-byte length prefix on each string.
pub const MAX_FAILED_REASON_BYTES: usize = 1024 - 1 - (2 + MAX_FAILED_STEP_BYTES) - 2;

/// The most bytes a refusal's reason holds: what is left of the 1024-byte reply bound after the
/// variant (one byte) and a two-byte length prefix.
pub const MAX_REASON_BYTES: usize = 1024 - 1 - 2;

impl NodePrimaryUpdateReply {
    /// `RejectedNoSuccessor` with `reason` within its budget.
    pub fn no_successor(reason: &str) -> Self {
        Self::RejectedNoSuccessor { reason: crate::status::within(reason, MAX_REASON_BYTES) }
    }

    /// `Incomplete` with `reason` within its budget.
    pub fn incomplete(reason: &str) -> Self {
        Self::Incomplete { reason: crate::status::within(reason, MAX_REASON_BYTES) }
    }

    /// `Failed` with `step` and `reason` within their budgets so the reply always fits
    /// [`NodePrimaryUpdate`]'s reply bound.
    pub fn failed(step: &str, reason: &str) -> Self {
        Self::Failed { step: crate::status::within(step, MAX_FAILED_STEP_BYTES), reason: crate::status::within(reason, MAX_FAILED_REASON_BYTES) }
    }

    /// `RejectedNotAuthority` with `reason` within its budget.
    pub fn not_authority(reason: &str) -> Self {
        Self::RejectedNotAuthority { reason: crate::status::within(reason, MAX_REASON_BYTES) }
    }

    /// `RejectedFabricNotReady` with `reason` within its budget.
    pub fn fabric_not_ready(reason: &str) -> Self {
        Self::RejectedFabricNotReady { reason: crate::status::within(reason, MAX_REASON_BYTES) }
    }

    /// `RejectedSourceFailed` naming what failed (`source`), the operation on it (`op`) and why
    /// (`cause`), each within its budget so the reply always fits [`NodePrimaryUpdate`]'s reply
    /// bound.
    pub fn source_failed(source: &str, op: &str, cause: &str) -> Self {
        let (source, op, cause) = crate::status::source_failed_fields(source, op, cause);
        Self::RejectedSourceFailed { source, op, cause }
    }

    /// `RejectedRecordAbsent` naming the record never written (`record`) and the admin whose records
    /// hold no such record (`holder`), each within its budget so the reply always fits
    /// [`NodePrimaryUpdate`]'s reply bound.
    pub fn record_absent(record: &str, holder: &str) -> Self {
        Self::RejectedRecordAbsent { record: crate::status::within(record, MAX_RECORD_ABSENT_RECORD_BYTES), holder: crate::status::within(holder, MAX_RECORD_ABSENT_HOLDER_BYTES) }
    }

    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::PrimaryUpdated { .. } => "primary-updated",
            Self::RejectedConflict { .. } => "rejected-conflict",
            Self::RejectedFabricNotReady { .. } => "rejected-fabric-not-ready",
            Self::RejectedNoSuccessor { .. } => "rejected-no-successor",
            Self::RejectedNotAuthority { .. } => "rejected-not-authority",
            Self::RejectedStaleSeat { .. } => "rejected-stale-seat",
            Self::RejectedRevisionOverflow { .. } => "rejected-revision-overflow",
            Self::RejectedSourceFailed { .. } => "rejected-source-failed",
            Self::RejectedRecordAbsent { .. } => "rejected-record-absent",
            Self::Incomplete { .. } => "incomplete",
            Self::NotReady { .. } => "not-ready",
            Self::Malformed { .. } => "malformed",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::Busy { .. } => "busy",
            Self::ConnectionsDeleting { .. } => "connections-deleting",
            Self::Unauthorized { .. } => "unauthorized",
            Self::Failed { .. } => "failed",
        }
    }
}

impl NodeProtocol for NodePrimaryUpdate {
    const OP: u8 = 0x21;
    const NAME: &'static str = "node-primary-update";
    /// Two seats, each with two exact births and a revision, a fabric name, a Build id and an operation
    /// identity.
    const MAX_REQUEST_FRAME_BYTES: usize = 2048;
    /// One reply: an enum tag and at most one reason string, two committed seats (a seat, an exact
    /// birth and a revision each), a source failure's three fields or an absent record's two, within
    /// their budgets.
    const MAX_REPLY_FRAME_BYTES: usize = 1024;
    /// An update is decided by the receiver's own seats and the committed row; a carried call is
    /// decided by no one.
    const FORWARDABLE: bool = false;
    /// A successor keeps serving PrimaryUpdate after the row, and a former holder UpdatePrimary or
    /// UpdatePrimaryFromRow, while it refuses new calls (node.connections.delete): a lost reply's
    /// identical retry is answered from the row.
    const SERVED_WHILE_CONNECTIONS_DELETING: bool = true;
    const REQUEST_VARIANTS: u32 = 3;
    const REPLY_VARIANTS: u32 = 17;

    type Request = NodePrimaryUpdateRequest;
    type Reply = NodePrimaryUpdateReply;

    fn classify_reply(reply: &NodePrimaryUpdateReply) -> ReplyKind {
        match reply {
            NodePrimaryUpdateReply::PrimaryUpdated { .. } => ReplyKind::Success,
            NodePrimaryUpdateReply::RejectedConflict { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            NodePrimaryUpdateReply::RejectedFabricNotReady { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodePrimaryUpdateReply::RejectedNoSuccessor { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodePrimaryUpdateReply::RejectedNotAuthority { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodePrimaryUpdateReply::RejectedStaleSeat { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            NodePrimaryUpdateReply::RejectedRevisionOverflow { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodePrimaryUpdateReply::RejectedSourceFailed { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodePrimaryUpdateReply::RejectedRecordAbsent { .. } => ReplyKind::new(crate::outcome::Code::NotFound),
            NodePrimaryUpdateReply::Incomplete { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodePrimaryUpdateReply::NotReady { .. } => ReplyKind::NotReady,
            NodePrimaryUpdateReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            NodePrimaryUpdateReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            NodePrimaryUpdateReply::Busy { .. } => ReplyKind::Busy,
            NodePrimaryUpdateReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            NodePrimaryUpdateReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            NodePrimaryUpdateReply::Failed { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
        }
    }

    fn peer_unresolved(reason: String) -> NodePrimaryUpdateReply {
        NodePrimaryUpdateReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> NodePrimaryUpdateReply {
        NodePrimaryUpdateReply::NotReady { reason }
    }
    fn busy(reason: String) -> NodePrimaryUpdateReply {
        NodePrimaryUpdateReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> NodePrimaryUpdateReply {
        NodePrimaryUpdateReply::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> NodePrimaryUpdateReply {
        NodePrimaryUpdateReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> NodePrimaryUpdateReply {
        NodePrimaryUpdateReply::Unauthorized { reason }
    }
}
