//! node.primary.update and node.fabric-primary.update on op `0x21` (node-rpc-envelope.md
//! "PrimaryUpdate, op `0x21`, rdm"; lifecycles/node-primary-update.md).
//!
//! Who is primary is the election's result. The primary, commanded from above with
//! [`NodePrimaryUpdateRequest::UpdatePrimary`], computes its successor by the election over its own view
//! with itself excluded and calls [`NodePrimaryUpdateRequest::PrimaryUpdate`] on the successor's exact
//! birth, carrying the election's result: one entry per primary that moves, `(node_type, primary,
//! successor, revision)`, the fabric-primary a second entry of node type `fabric`. The successor
//! installs it in its own view and answers [`NodePrimaryUpdateReply::PrimaryUpdated`]
//! (node.primary.updated); the primary hands that answer up to its commander. The receiver refuses
//! only a conflicting payload for the same operation and a fabric not first ready; it never
//! refuses a call for the birth it names (R-UC1a, R-RX5). No row and no frame of its own record the
//! change: the successor's digest carries it and the mesh-primary records it on the node's row. The
//! family's outcomes sit on the framework's six: `NotReady`, `Malformed`, `PeerUnresolved`, `Busy`,
//! `ConnectionsDeleting` and `Unauthorized`.

use crate::framing::Birth;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
pub use crate::seat::transfer::{HandoverBirth, Seat};
use serde::{Deserialize, Serialize};

/// The node-primary-update protocol: node.primary.update and node.fabric-primary.update.
pub struct NodePrimaryUpdate;

/// The node type of the fabric-primary's entry in [`NodePrimaryUpdateRequest::PrimaryUpdate`]'s
/// `primaries` (node-rpc-envelope.md: "the fabric-primary is a second entry, node type `fabric`").
pub const FABRIC: &str = "fabric";

/// The directed calls of node.primary.update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodePrimaryUpdateRequest {
    /// node.primary.update, the primary to its successor's exact birth: the election's result the
    /// successor installs, one entry or two, each `(node_type, primary, successor, revision)`, the
    /// revision the next one.
    ///
    /// Position 0 on the wire.
    PrimaryUpdate {
        /// Every primary that moves: its node type (`fabric` for the fabric-primary), the primary's
        /// exact birth, the successor's exact birth and the revision the successor takes.
        primaries: Vec<(String, Birth, Birth, u64)>,
        /// The Build whose stop, delete, replace or node.connection.delete the update precedes.
        build_id: String,
        /// The run of that Build: the first attempt of the series a submission opened.
        run: u32,
        /// The update's operation identity, `node.primary.update:<build_id>:<run>:<op>`.
        operation: String,
    },
    /// node.primary.update commanded from above, on the primary of `node_type` in the receiver's mesh,
    /// or on the mesh-primary for another type's primary of its mesh: the primary runs the sequence.
    ///
    /// Position 1 on the wire.
    UpdatePrimary {
        /// The node type whose primary moves.
        node_type: String,
        /// The Build whose stop, delete, replace or node.connection.delete the update precedes.
        build_id: String,
        /// The run of that Build.
        run: u32,
        /// The update's operation identity.
        operation: String,
        /// The successor's exact birth, named by the mesh-primary's pre-check (node-primary-update.md step
        /// 2) when it commands another type's primary; none to a node-admin, which computes the election
        /// itself (Q129).
        successor: Option<Birth>,
    },
}

impl NodePrimaryUpdateRequest {
    /// The request's name as it appears in spans and replies.
    pub fn op(&self) -> &'static str {
        match self {
            Self::PrimaryUpdate { .. } => "primary-update",
            Self::UpdatePrimary { .. } => "update-primary",
        }
    }
}

/// The answer to a node.primary.update call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodePrimaryUpdateReply {
    /// node.primary.updated: the successor installed the election's result, every entry at its revision.
    ///
    /// Position 0 on the wire.
    PrimaryUpdated {
        /// The entries installed, as [`NodePrimaryUpdateRequest::PrimaryUpdate`] carries them.
        revisions: Vec<(String, Birth, Birth, u64)>,
    },
    /// The operation was answered for another payload.
    ///
    /// Position 1 on the wire.
    RejectedConflictingPayload {
        /// The operation in conflict.
        operation: String,
    },
    /// Reserved position (Q171): never sent.
    ///
    /// Position 2 on the wire.
    Reserved2 {
        /// Never read.
        reason: String,
    },
    /// The receiver is not the primary the call needs: a command on a node that is not the primary of
    /// the node type it names, a take-over proposal naming another winner.
    ///
    /// Position 3 on the wire.
    RejectedNotPrimary {
        /// What the receiver holds instead.
        reason: String,
    },
    /// A take-over at a revision the recorded primary no longer holds.
    ///
    /// Position 4 on the wire.
    RejectedStaleRevision {
        /// The node type whose revision moved (`fabric`).
        node_type: String,
        /// The revision the call named.
        expected: u64,
        /// The revision the receiver holds.
        current: u64,
    },
    /// The receiver cannot decide yet.
    ///
    /// Position 5 on the wire.
    NotReady {
        /// What it waits for.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 6 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The peer the call needed could not be resolved.
    ///
    /// Position 7 on the wire.
    PeerUnresolved {
        /// Why.
        reason: String,
    },
    /// The receiver is at its admission bound.
    ///
    /// Position 8 on the wire.
    Busy {
        /// Which bound.
        reason: String,
    },
    /// The receiver is connections-deleting and takes no new work.
    ///
    /// Position 9 on the wire.
    ConnectionsDeleting {
        /// Why.
        reason: String,
    },
    /// The caller is not allowed this call.
    ///
    /// Position 10 on the wire.
    Unauthorized {
        /// Why.
        reason: String,
    },
    /// The commanded update ended failed (R-BI4): the Build carrier's own terminal, recording the step
    /// and why (no successor; the successor unheard after the update, with the investigation's verdict).
    ///
    /// Position 11 on the wire.
    Failed {
        /// The step that failed (`primary-update`). Within [`MAX_FAILED_STEP_BYTES`].
        step: String,
        /// Why the step failed. Within [`MAX_FAILED_REASON_BYTES`].
        reason: String,
        /// The standard code captured where the failure occurred.
        code: crate::outcome::Code,
    },
    /// The take-over is applied already, to the successor it names.
    ///
    /// Position 12 on the wire.
    AlreadyApplied {
        /// The fabric-primary's entry, at its revision.
        revisions: Vec<(String, Birth, Birth, u64)>,
    },
}

/// The most bytes of the `step` a `Failed` reply names.
pub const MAX_FAILED_STEP_BYTES: usize = 128;
/// The most bytes of the `reason` a `Failed` reply names: what is left of the 1024-byte reply bound
/// after the variant (one byte), `step` at its budget and a two-byte length prefix on each string.
pub const MAX_FAILED_REASON_BYTES: usize = 1024 - 1 - 1 - (2 + MAX_FAILED_STEP_BYTES) - 2;

/// The most bytes a refusal's reason holds: what is left of the 1024-byte reply bound after the
/// variant (one byte) and a two-byte length prefix.
pub const MAX_REASON_BYTES: usize = 1024 - 1 - 2;

impl NodePrimaryUpdateReply {
    /// `Failed` with `step` and `reason` within their budgets so the reply always fits
    /// [`NodePrimaryUpdate`]'s reply bound.
    pub fn failed(step: &str, reason: &str, code: crate::outcome::Code) -> Self {
        Self::Failed { step: crate::status::within(step, MAX_FAILED_STEP_BYTES), reason: crate::status::within(reason, MAX_FAILED_REASON_BYTES), code }
    }

    /// `RejectedNotPrimary` with `reason` within its budget.
    pub fn not_primary(reason: &str) -> Self {
        Self::RejectedNotPrimary { reason: crate::status::within(reason, MAX_REASON_BYTES) }
    }


    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::PrimaryUpdated { .. } => "primary-updated",
            Self::RejectedConflictingPayload { .. } => "rejected-conflicting-payload",
            Self::Reserved2 { .. } => "reserved-2",
            Self::RejectedNotPrimary { .. } => "rejected-not-primary",
            Self::RejectedStaleRevision { .. } => "rejected-stale-revision",
            Self::NotReady { .. } => "not-ready",
            Self::Malformed { .. } => "malformed",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::Busy { .. } => "busy",
            Self::ConnectionsDeleting { .. } => "connections-deleting",
            Self::Unauthorized { .. } => "unauthorized",
            Self::Failed { .. } => "failed",
            Self::AlreadyApplied { .. } => "already-applied",
        }
    }
}

impl NodeProtocol for NodePrimaryUpdate {
    const OP: u8 = 0x21;
    const NAME: &'static str = "node-primary-update";
    /// Two entries, each a node type, two exact births and a revision, a Build id, its run and an
    /// operation identity.
    const MAX_REQUEST_FRAME_BYTES: usize = 2048;
    /// One reply: an enum tag and at most one reason string, or two entries, within their budgets.
    const MAX_REPLY_FRAME_BYTES: usize = 1024;
    /// An update is decided by the receiver's own view; a carried call is decided by no one.
    const FORWARDABLE: bool = false;
    /// A successor keeps serving PrimaryUpdate, and a primary UpdatePrimary, while it refuses new
    /// calls (node.connection.delete): a lost reply's identical retry is answered as the first was.
    const SERVED_WHILE_CONNECTIONS_DELETING: bool = true;
    const REQUEST_VARIANTS: u32 = 2;
    const REPLY_VARIANTS: u32 = 13;

    type Request = NodePrimaryUpdateRequest;
    type Reply = NodePrimaryUpdateReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            NodePrimaryUpdateReply::PrimaryUpdated { .. } => "primary-updated",
            NodePrimaryUpdateReply::RejectedConflictingPayload { .. } => "rejected-conflicting-payload",
            NodePrimaryUpdateReply::Reserved2 { .. } => "reserved-2",
            NodePrimaryUpdateReply::RejectedNotPrimary { .. } => "rejected-not-primary",
            NodePrimaryUpdateReply::RejectedStaleRevision { .. } => "rejected-stale-revision",
            NodePrimaryUpdateReply::NotReady { .. } => "not-ready",
            NodePrimaryUpdateReply::Malformed { .. } => "malformed",
            NodePrimaryUpdateReply::PeerUnresolved { .. } => "peer-unresolved",
            NodePrimaryUpdateReply::Busy { .. } => "busy",
            NodePrimaryUpdateReply::ConnectionsDeleting { .. } => "connections-deleting",
            NodePrimaryUpdateReply::Unauthorized { .. } => "unauthorized",
            NodePrimaryUpdateReply::Failed { .. } => "failed",
            NodePrimaryUpdateReply::AlreadyApplied { .. } => "already-applied",
        }
    }

    fn classify_reply(reply: &NodePrimaryUpdateReply) -> ReplyKind {
        match reply {
            NodePrimaryUpdateReply::PrimaryUpdated { .. } => ReplyKind::Success,
            NodePrimaryUpdateReply::RejectedConflictingPayload { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            NodePrimaryUpdateReply::Reserved2 { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodePrimaryUpdateReply::RejectedNotPrimary { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            NodePrimaryUpdateReply::RejectedStaleRevision { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            NodePrimaryUpdateReply::NotReady { .. } => ReplyKind::NotReady,
            NodePrimaryUpdateReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            NodePrimaryUpdateReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            NodePrimaryUpdateReply::Busy { .. } => ReplyKind::Busy,
            NodePrimaryUpdateReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            NodePrimaryUpdateReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            NodePrimaryUpdateReply::Failed { code, .. } => ReplyKind::new(*code),
            NodePrimaryUpdateReply::AlreadyApplied { .. } => ReplyKind::new(crate::outcome::Code::AlreadyExists),
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
